#include "l2flow/ingress/raw_writer_lease.h"

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <span>
#include <string>

#include <sys/stat.h>
#include <unistd.h>

namespace ingress = l2flow::ingress;

namespace {

struct TestContext final {
    void Expect(bool condition, const std::string& description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    int failures = 0;
};

l2flow::common::Identity128 MakeAttempt(
    std::uint8_t seed) {
    l2flow::common::Identity128 result{};
    for (std::size_t index = 0U;
         index < result.size();
         ++index) {
        result[index] =
            static_cast<std::byte>(
                static_cast<std::uint8_t>(
                    seed + index));
    }
    return result;
}

bool CreateCompleteAttemptTemporary(
    int directory_fd,
    const l2flow::common::Identity128& attempt,
    std::uint32_t source_stream_id,
    std::uint32_t capture_date) {
    ingress::RawWriterLeaseMarkerV1 marker;
    marker.source_stream_id = source_stream_id;
    marker.capture_date = capture_date;
    std::array<std::byte, ingress::kRawWriterLeaseMarkerBytes>
        wire{};
    if (!ingress::EncodeRawWriterLeaseMarkerV1(
            marker, &wire)) {
        return false;
    }
    const std::string name =
        ingress::
            RawWriterLeaseAttemptTemporaryFilenameV1(
                attempt);
    const int fd = ::openat(
        directory_fd,
        name.c_str(),
        O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW |
            O_CLOEXEC,
        0600);
    if (fd < 0) {
        return false;
    }
    std::size_t completed = 0U;
    while (completed < wire.size()) {
        const ssize_t result = ::pwrite(
            fd,
            wire.data() + completed,
            wire.size() - completed,
            static_cast<off_t>(completed));
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            static_cast<void>(::close(fd));
            return false;
        }
        completed += static_cast<std::size_t>(result);
    }
    return ::close(fd) == 0;
}

class TemporaryDirectory final {
public:
    TemporaryDirectory() {
        std::array<char, 64U> path{};
        const std::string pattern =
            "/tmp/l2flow-raw-lease-XXXXXX";
        std::copy(pattern.begin(), pattern.end(), path.begin());
        char* const created = ::mkdtemp(path.data());
        if (created != nullptr) {
            path_ = created;
            fd_ = ::open(
                path_.c_str(),
                O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        }
    }

    ~TemporaryDirectory() {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
        if (!path_.empty()) {
            static_cast<void>(
                ::unlinkat(
                    AT_FDCWD,
                    (path_ + "/" +
                     ingress::kRawWriterLeaseFilename).c_str(),
                    0));
            static_cast<void>(
                ::unlinkat(
                    AT_FDCWD,
                    (path_ + "/" +
                     ingress::kRawWriterLeaseTemporaryFilename).c_str(),
                    0));
            static_cast<void>(::rmdir(path_.c_str()));
        }
    }

    [[nodiscard]] int fd() const noexcept {
        return fd_;
    }

private:
    std::string path_;
    int fd_ = -1;
};

}  // namespace

int main() {
    TestContext test;

    ingress::RawWriterLeaseMarkerV1 marker;
    marker.source_stream_id = 1001U;
    marker.capture_date = 20260718U;
    std::array<std::byte, ingress::kRawWriterLeaseMarkerBytes>
        wire{};
    test.Expect(
        ingress::EncodeRawWriterLeaseMarkerV1(
            marker, &wire),
        "lease marker encodes");
    test.Expect(
        std::equal(
            ingress::kRawWriterLeaseMagic.begin(),
            ingress::kRawWriterLeaseMagic.end(),
            wire.begin()),
        "lease marker has the frozen magic");
    ingress::RawWriterLeaseMarkerV1 decoded;
    test.Expect(
        ingress::DecodeRawWriterLeaseMarkerV1(
            wire, &decoded) &&
            decoded.source_stream_id == 1001U &&
            decoded.capture_date == 20260718U,
        "lease marker round-trips");
    wire[ingress::raw_writer_lease_offset::kReserved] =
        std::byte{1U};
    test.Expect(
        !ingress::DecodeRawWriterLeaseMarkerV1(
            wire, &decoded),
        "nonzero reserved lease bytes are rejected");

    TemporaryDirectory directory;
    test.Expect(
        directory.fd() >= 0,
        "private test directory opens");
    std::string error;
    std::unique_ptr<ingress::RawWriterLease> first =
        ingress::AcquireRawWriterLeaseAt(
            directory.fd(), 1001U, 20260718U, &error);
    test.Expect(first != nullptr, "fresh lease is published and held");
    if (first != nullptr) {
        struct stat status {};
        test.Expect(
            ::fstat(first->descriptor(), &status) == 0 &&
                S_ISREG(status.st_mode) &&
                status.st_nlink == 1 &&
                (status.st_mode & 0777U) == 0600U,
            "lease inode has the required type/mode/link count");
    }

    std::unique_ptr<ingress::RawWriterLease> concurrent =
        ingress::AcquireRawWriterLeaseAt(
            directory.fd(), 1001U, 20260718U, &error);
    test.Expect(
        concurrent == nullptr,
        "a second cooperating writer cannot acquire the lease");
    first.reset();

    std::unique_ptr<ingress::RawWriterLease> reopened =
        ingress::AcquireRawWriterLeaseAt(
            directory.fd(), 1001U, 20260718U, &error);
    test.Expect(
        reopened != nullptr,
        "the persistent typed lease can be reacquired");
    reopened.reset();

    const auto current_attempt = MakeAttempt(0x20U);
    const auto conflicting_attempt = MakeAttempt(0x40U);
    const std::string conflicting_name =
        ingress::
            RawWriterLeaseAttemptTemporaryFilenameV1(
                conflicting_attempt);
    const int conflicting_fd = ::openat(
        directory.fd(),
        conflicting_name.c_str(),
        O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW |
            O_CLOEXEC,
        0600);
    test.Expect(
        conflicting_fd >= 0,
        "conflicting attempt temporary is created after earlier inventories");
    if (conflicting_fd >= 0) {
        static_cast<void>(::close(conflicting_fd));
    }
    std::unique_ptr<ingress::RawWriterLease>
        hidden_conflict =
            ingress::AcquireRawWriterLeaseAtV1(
                directory.fd(),
                1001U,
                20260718U,
                current_attempt,
                &error);
    test.Expect(
        hidden_conflict == nullptr,
        "independent directory enumeration detects a later conflicting attempt tmp");
    static_cast<void>(::unlinkat(
        directory.fd(),
        conflicting_name.c_str(),
        0));

    std::unique_ptr<ingress::RawWriterLease> wrong_namespace =
        ingress::AcquireRawWriterLeaseAt(
            directory.fd(), 2001U, 20260718U, &error);
    test.Expect(
        wrong_namespace == nullptr,
        "a persisted lease cannot be reused by another namespace");

    TemporaryDirectory adoption_directory;
    const auto adoption_attempt = MakeAttempt(0x60U);
    test.Expect(
        adoption_directory.fd() >= 0 &&
            CreateCompleteAttemptTemporary(
                adoption_directory.fd(),
                adoption_attempt,
                2002U,
                20260718U),
        "complete attempt-derived lease tmp is prepared");
    auto adopted = ingress::AcquireRawWriterLeaseAtV1(
        adoption_directory.fd(),
        2002U,
        20260718U,
        adoption_attempt,
        &error);
    struct stat adopted_final {};
    const std::string adoption_name =
        ingress::
            RawWriterLeaseAttemptTemporaryFilenameV1(
                adoption_attempt);
    struct stat adopted_tmp {};
    test.Expect(
        adopted != nullptr &&
            ::fstatat(
                adoption_directory.fd(),
                ingress::kRawWriterLeaseFilename,
                &adopted_final,
                AT_SYMLINK_NOFOLLOW) == 0 &&
            ::fstatat(
                adoption_directory.fd(),
                adoption_name.c_str(),
                &adopted_tmp,
                AT_SYMLINK_NOFOLLOW) != 0 &&
            errno == ENOENT,
        "complete attempt-derived tmp is synchronized and adopted as the fixed final lease");

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Phase 2 Raw writer-lease tests failed\n";
        return 1;
    }
    std::cout << "Phase 2 Raw writer-lease tests passed\n";
    return 0;
}
