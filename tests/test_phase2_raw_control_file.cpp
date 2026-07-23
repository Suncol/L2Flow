#include "l2flow/ingress/raw_control_file.h"
#include "l2flow/ingress/raw_writer_lease.h"

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <thread>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace ingress = l2flow::ingress;

namespace {

int failures = 0;

void Expect(bool condition, std::string_view message) {
    if (!condition) {
        ++failures;
        std::cerr << "FAIL: " << message << '\n';
    }
}

template <std::size_t Size>
std::array<std::byte, Size> Pattern(std::uint8_t seed) {
    std::array<std::byte, Size> value{};
    for (std::size_t index = 0U; index < Size; ++index) {
        value[index] = static_cast<std::byte>(
            seed + static_cast<std::uint8_t>(index));
    }
    return value;
}

class TemporaryDirectory final {
public:
    TemporaryDirectory() {
        std::array<char, 64U> path{};
        const std::string pattern =
            "/tmp/l2flow-control-file-XXXXXX";
        std::copy(
            pattern.begin(),
            pattern.end(),
            path.begin());
        char* created = ::mkdtemp(path.data());
        if (created == nullptr) {
            throw std::runtime_error("mkdtemp failed");
        }
        path_ = created;
        if (::chmod(path_.c_str(), 0700) != 0) {
            throw std::runtime_error("chmod failed");
        }
        fd_ = ::open(
            path_.c_str(),
            O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (fd_ < 0) {
            throw std::runtime_error("open directory failed");
        }
    }

    ~TemporaryDirectory() {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] int fd() const noexcept {
        return fd_;
    }

private:
    std::string path_;
    int fd_ = -1;
};

ingress::RawControlSnapshot MakeSnapshot(
    std::uint8_t writer_seed,
    std::uint64_t append) {
    ingress::RawControlSnapshot snapshot;
    snapshot.writer_instance =
        Pattern<16U>(writer_seed);
    snapshot.stream_day_id =
        Pattern<16U>(0x20U);
    snapshot.source_stream_id = 1001U;
    snapshot.capture_date = 20260718U;
    snapshot.segment_sequence = 1U;
    snapshot.append_global_wal_pos = append;
    snapshot.append_segment_offset = append;
    snapshot.append_ingress_sequence =
        append == 4096U ? 0U : 1U;
    snapshot.durable_global_wal_pos = 4096U;
    snapshot.durable_segment_offset = 4096U;
    snapshot.heartbeat_monotonic_ns = 123U;
    return snapshot;
}

void TestPublicationReplacementAndStaleMapping() {
    TemporaryDirectory directory;
    std::string error;
    std::unique_ptr<ingress::RawWriterLease> lease =
        ingress::AcquireRawWriterLeaseAt(
            directory.fd(),
            1001U,
            20260718U,
            &error);
    Expect(lease != nullptr, "writer lease acquired");
    if (lease == nullptr) {
        return;
    }

    const auto first_snapshot =
        MakeSnapshot(0x40U, 4096U);
    std::unique_ptr<ingress::RawControlFileWriter> first =
        ingress::CreateRawControlFile(
            *lease, first_snapshot, &error);
    Expect(first != nullptr, "first control inode publishes");
    std::unique_ptr<ingress::RawControlFileReader> old_reader =
        ingress::OpenRawControlFile(
            lease->directory_descriptor(),
            1001U,
            20260718U,
            &error);
    Expect(old_reader != nullptr, "reader attaches to first inode");
    if (first == nullptr || old_reader == nullptr) {
        return;
    }

    ingress::RawControlSnapshot progressed =
        first_snapshot;
    progressed.append_global_wal_pos = 4224U;
    progressed.append_segment_offset = 4224U;
    progressed.append_ingress_sequence = 1U;
    Expect(
        first->Publish(progressed),
        "writer publishes coherent progress");
    ingress::RawControlSnapshot observed;
    Expect(
        old_reader->Read(&observed) &&
            observed.append_global_wal_pos == 4224U,
        "mapped reader observes progress");

    void* const writable_mapping = ::mmap(
        nullptr,
        ingress::kRawControlPageBytes,
        PROT_READ | PROT_WRITE,
        MAP_SHARED,
        first->descriptor(),
        0);
    Expect(
        writable_mapping != MAP_FAILED,
        "control contention fixture maps the writer page");
    if (writable_mapping != MAP_FAILED) {
        auto* const page = static_cast<ingress::RawControlPageV1*>(
            writable_mapping);
        const std::uint64_t even =
            page->generation.load(std::memory_order_acquire);
        page->generation.store(even + 1U, std::memory_order_release);
        std::thread release_writer([page, even]() {
            std::this_thread::sleep_for(std::chrono::milliseconds{2});
            page->generation.store(even + 2U, std::memory_order_release);
        });
        std::uint64_t contended_generation = 0U;
        const bool contended_read = old_reader->Read(
            &observed, &contended_generation);
        release_writer.join();
        Expect(
            contended_read && contended_generation == even + 2U &&
                observed.append_global_wal_pos == 4224U,
            "mapped reader retries a transient odd control generation");
        static_cast<void>(::munmap(
            writable_mapping, ingress::kRawControlPageBytes));
    }

    ingress::RawControlSnapshot regressed = progressed;
    regressed.append_global_wal_pos = 4096U;
    regressed.append_segment_offset = 4096U;
    regressed.append_ingress_sequence = 0U;
    Expect(
        !first->Publish(regressed),
        "writer rejects append cursor regression");
    regressed = progressed;
    regressed.durable_global_wal_pos = 4095U;
    Expect(
        !first->Publish(regressed),
        "writer rejects durable cursor regression");
    regressed = progressed;
    regressed.heartbeat_monotonic_ns = 122U;
    Expect(
        !first->Publish(regressed),
        "writer rejects heartbeat regression");
    regressed = progressed;
    regressed.segment_sequence = 3U;
    Expect(
        !first->Publish(regressed),
        "writer rejects a skipped segment");

    ingress::RawControlSnapshot rotated = progressed;
    rotated.segment_sequence = 2U;
    rotated.append_global_wal_pos = 8320U;
    rotated.append_ingress_sequence = 2U;
    rotated.append_segment_offset = 4096U;
    rotated.durable_global_wal_pos = 8320U;
    rotated.durable_ingress_sequence = 1U;
    rotated.durable_segment_offset = 4096U;
    rotated.heartbeat_monotonic_ns = 124U;
    Expect(
        first->Publish(rotated),
        "writer accepts the next segment with monotonic stream cursors");
    ingress::RawControlSnapshot stale_segment = rotated;
    stale_segment.segment_sequence = 1U;
    Expect(
        !first->Publish(stale_segment),
        "writer rejects segment-sequence regression");
    ingress::RawControlSnapshot fatal = rotated;
    fatal.fatal_state = 7U;
    fatal.heartbeat_monotonic_ns = 125U;
    Expect(
        first->Publish(fatal),
        "writer publishes the first fatal state");
    fatal.fatal_state = 0U;
    fatal.heartbeat_monotonic_ns = 126U;
    Expect(
        !first->Publish(fatal),
        "writer never clears a published fatal state");

    const auto second_snapshot =
        MakeSnapshot(0x60U, 4096U);
    std::unique_ptr<ingress::RawControlFileWriter> second =
        ingress::CreateRawControlFile(
            *lease, second_snapshot, &error);
    Expect(second != nullptr, "replacement control inode publishes");
    Expect(
        !old_reader->PathStillNamesMapping(),
        "old mapping detects pathname replacement");
    Expect(
        old_reader->Read(&observed) &&
            observed.writer_instance ==
                first_snapshot.writer_instance,
        "old mapping remains safely stale without truncation");

    std::unique_ptr<ingress::RawControlFileReader> current =
        ingress::OpenRawControlFile(
            lease->directory_descriptor(),
            1001U,
            20260718U,
            &error);
    Expect(
        current != nullptr &&
            current->Read(&observed) &&
            observed.writer_instance ==
                second_snapshot.writer_instance &&
            current->PathStillNamesMapping(),
        "reattach observes the new writer instance");

    struct stat status {};
    Expect(
        ::fstatat(
            lease->directory_descriptor(),
            ingress::kRawControlFilename,
            &status,
            AT_SYMLINK_NOFOLLOW) == 0 &&
            S_ISREG(status.st_mode) &&
            (status.st_mode & 0777) == 0600 &&
            status.st_nlink == 1 &&
            status.st_size == 4096,
        "published control inode has exact secure metadata");
}

void TestIdentityAndTemporaryCollisionFailClosed() {
    TemporaryDirectory directory;
    std::string error;
    auto lease = ingress::AcquireRawWriterLeaseAt(
        directory.fd(), 1001U, 20260718U, &error);
    Expect(lease != nullptr, "collision fixture lease acquired");
    if (lease == nullptr) {
        return;
    }

    ingress::RawControlSnapshot wrong =
        MakeSnapshot(0x40U, 4096U);
    wrong.source_stream_id = 2002U;
    Expect(
        ingress::CreateRawControlFile(
            *lease, wrong, &error) == nullptr,
        "namespace mismatch is rejected before mutation");

    const ingress::RawControlSnapshot valid =
        MakeSnapshot(0x40U, 4096U);
    const std::string temporary =
        ".control.page.404142434445464748494a4b4c4d4e4f"
        ".raw-control.tmp";
    const int collision = ::openat(
        lease->directory_descriptor(),
        temporary.c_str(),
        O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
        0600);
    Expect(collision >= 0, "typed tmp collision fixture created");
    if (collision >= 0) {
        static_cast<void>(::close(collision));
    }
    Expect(
        ingress::CreateRawControlFile(
            *lease, valid, &error) == nullptr,
        "existing typed tmp fails closed without random loser");
}

}  // namespace

int main() {
    TestPublicationReplacementAndStaleMapping();
    TestIdentityAndTemporaryCollisionFailClosed();
    if (failures != 0) {
        std::cerr << failures
                  << " Phase 2 Raw control-file tests failed\n";
        return 1;
    }
    std::cout << "Phase 2 Raw control-file tests passed\n";
    return 0;
}
