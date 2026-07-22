#include "l2flow/canonical/source_frontier_posix_v1.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace canonical = l2flow::canonical;

namespace {

struct TestContext final {
    int failures = 0;

    void Expect(bool condition, std::string_view message) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << message << '\n';
        }
    }
};

class TestDirectory final {
public:
    TestDirectory() {
        std::array<char, 64U> pattern{};
        constexpr std::string_view prefix =
            "/tmp/l2flow-source-frontier-posix-XXXXXX";
        std::copy(prefix.begin(), prefix.end(), pattern.begin());
        char* const created = ::mkdtemp(pattern.data());
        if (created == nullptr) {
            return;
        }
        path_ = created;
        descriptor_ = ::open(
            path_.c_str(),
            O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    }

    ~TestDirectory() {
        if (descriptor_ >= 0) {
            constexpr std::array<std::string_view, 5U> names{
                "frontier.link",
                "frontier.page",
                "invalid.page",
                "short.page",
                "wide-mode.page"};
            for (const std::string_view name : names) {
                static_cast<void>(::unlinkat(
                    descriptor_, name.data(), 0));
            }
            static_cast<void>(::close(descriptor_));
        }
        if (!path_.empty()) {
            static_cast<void>(::rmdir(path_.c_str()));
        }
    }

    TestDirectory(const TestDirectory&) = delete;
    TestDirectory& operator=(const TestDirectory&) = delete;

    [[nodiscard]] int descriptor() const noexcept {
        return descriptor_;
    }

private:
    std::string path_{};
    int descriptor_ = -1;
};

template <std::size_t Size>
std::array<std::byte, Size> Pattern(std::uint8_t seed) {
    std::array<std::byte, Size> result{};
    for (std::size_t index = 0U; index < Size; ++index) {
        result[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(seed + index));
    }
    return result;
}

canonical::SourceFrontierConfigV1 Config() {
    canonical::SourceFrontierConfigV1 result{};
    result.source_stream_id = 1001U;
    result.capture_date = 20260723U;
    result.stream_day_id = Pattern<16U>(0x10U);
    result.clock_epoch.algorithm = 1U;
    result.clock_epoch.digest = Pattern<32U>(0x20U);
    result.clock_epoch.label = 17U;
    result.writer_instance = Pattern<16U>(0x40U);
    result.generation = 9U;
    result.initial_state = canonical::SourceStateV1::kRecovering;
    return result;
}

canonical::SourceFrontierPosixFileOptionsV1 Options(
    int directory_descriptor,
    std::string_view name) {
    canonical::SourceFrontierPosixFileOptionsV1 result{};
    result.directory_fd = directory_descriptor;
    result.file_name = name;
    result.expected_owner_uid = static_cast<std::uint32_t>(::geteuid());
    return result;
}

void CloseOnce(int descriptor) {
    if (descriptor >= 0) {
        static_cast<void>(::close(descriptor));
    }
}

void CheckMappingAndIdentity(
    TestContext* context,
    TestDirectory* directory,
    std::unique_ptr<canonical::SourceFrontierPosixMappingV1>* owner,
    std::unique_ptr<canonical::SourceFrontierPosixMappingV1>* attacher) {
    const canonical::SourceFrontierConfigV1 config = Config();
    const auto options = Options(directory->descriptor(), "frontier.page");
    const auto create = canonical::SourceFrontierPosixMappingV1::Create(
        options, config, owner);
    context->Expect(
        static_cast<bool>(create) && *owner != nullptr,
        "owner creates and initializes one shared page");
    if (*owner == nullptr) {
        return;
    }

    struct stat status {};
    context->Expect(
        ::fstatat(
            directory->descriptor(),
            "frontier.page",
            &status,
            AT_SYMLINK_NOFOLLOW) == 0 &&
            S_ISREG(status.st_mode) &&
            status.st_size == static_cast<off_t>(
                canonical::kSourceFrontierPageBytesV1) &&
            (status.st_mode & 07777U) == 0600U &&
            status.st_nlink == 1,
        "created backing inode has exact type, size, mode and link count");

    const canonical::SourceFrontierIdentityV1 identity =
        canonical::SourceFrontierIdentityFromConfigV1(config);
    const auto attach = canonical::SourceFrontierPosixMappingV1::Attach(
        options, identity, attacher);
    context->Expect(
        static_cast<bool>(attach) && *attacher != nullptr,
        "attacher validates identity and maps the existing inode");
    if (*attacher == nullptr) {
        return;
    }

    context->Expect(
        canonical::PublishSourceStateV1(
            (*owner)->page(),
            config.writer_instance,
            config.generation,
            canonical::SourceStateV1::kHealthy,
            0U) == canonical::SourceFrontierErrorV1::kNone,
        "owner publishes through its mapping");
    canonical::SourceFrontierV1 observed{};
    context->Expect(
        canonical::ReadSourceFrontierV1(
            *(*attacher)->page(), &observed) ==
                canonical::SourceFrontierErrorV1::kNone &&
            observed.source_state == canonical::SourceStateV1::kHealthy &&
            observed.writer_instance == config.writer_instance,
        "a distinct MAP_SHARED attachment immediately observes publication");

    canonical::SourceFrontierIdentityV1 wrong_identity = identity;
    wrong_identity.generation += 1U;
    std::unique_ptr<canonical::SourceFrontierPosixMappingV1> rejected;
    const auto mismatch = canonical::SourceFrontierPosixMappingV1::Attach(
        options, wrong_identity, &rejected);
    context->Expect(
        mismatch.error ==
                canonical::SourceFrontierPosixErrorV1::kIdentityMismatch &&
            rejected == nullptr,
        "attacher rejects a valid page from another writer generation");
}

void CheckRoleLeases(
    TestContext* context,
    canonical::SourceFrontierPosixMappingV1* first,
    canonical::SourceFrontierPosixMappingV1* second) {
    if (first == nullptr || second == nullptr) {
        return;
    }
    std::unique_ptr<canonical::SourceFrontierPosixRoleLeaseV1> producer;
    std::unique_ptr<canonical::SourceFrontierPosixRoleLeaseV1> processor;
    context->Expect(
        static_cast<bool>(first->AcquireRole(
            canonical::SourceFrontierWriterRoleV1::kProducer,
            &producer)),
        "first producer acquires its OFD role byte");
    context->Expect(
        static_cast<bool>(second->AcquireRole(
            canonical::SourceFrontierWriterRoleV1::kProcessor,
            &processor)),
        "processor role is independent from producer role");

    std::unique_ptr<canonical::SourceFrontierPosixRoleLeaseV1>
        conflicting_producer;
    const auto producer_conflict = second->AcquireRole(
        canonical::SourceFrontierWriterRoleV1::kProducer,
        &conflicting_producer);
    context->Expect(
        producer_conflict.error ==
                canonical::SourceFrontierPosixErrorV1::kRoleConflict &&
            (producer_conflict.system_errno == EACCES ||
             producer_conflict.system_errno == EAGAIN) &&
            conflicting_producer == nullptr,
        "a second producer mapping cannot enter the producer role");

    std::unique_ptr<canonical::SourceFrontierPosixRoleLeaseV1>
        conflicting_processor;
    const auto processor_conflict = first->AcquireRole(
        canonical::SourceFrontierWriterRoleV1::kProcessor,
        &conflicting_processor);
    context->Expect(
        processor_conflict.error ==
                canonical::SourceFrontierPosixErrorV1::kRoleConflict &&
            conflicting_processor == nullptr,
        "a second processor mapping cannot enter the processor role");

    producer.reset();
    context->Expect(
        static_cast<bool>(second->AcquireRole(
            canonical::SourceFrontierWriterRoleV1::kProducer,
            &conflicting_producer)),
        "producer lease destruction releases only the producer role byte");
    processor.reset();
    context->Expect(
        static_cast<bool>(first->AcquireRole(
            canonical::SourceFrontierWriterRoleV1::kProcessor,
            &conflicting_processor)),
        "processor lease destruction releases the processor role byte");
}

void CheckUnsafeArtifacts(
    TestContext* context,
    TestDirectory* directory) {
    const canonical::SourceFrontierIdentityV1 identity =
        canonical::SourceFrontierIdentityFromConfigV1(Config());

    int descriptor = ::openat(
        directory->descriptor(),
        "short.page",
        O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
        0600);
    context->Expect(descriptor >= 0, "short test file is created");
    if (descriptor >= 0) {
        context->Expect(
            ::ftruncate(descriptor, 17) == 0,
            "short test file is truncated");
    }
    CloseOnce(descriptor);
    std::unique_ptr<canonical::SourceFrontierPosixMappingV1> mapping;
    const auto short_result =
        canonical::SourceFrontierPosixMappingV1::Attach(
            Options(directory->descriptor(), "short.page"),
            identity,
            &mapping);
    context->Expect(
        short_result.error ==
                canonical::SourceFrontierPosixErrorV1::kWrongFileSize &&
            mapping == nullptr,
        "attacher rejects any backing file that is not exactly 4096 bytes");

    context->Expect(
        ::symlinkat(
            "frontier.page",
            directory->descriptor(),
            "frontier.link") == 0,
        "symlink test artifact is created");
    const auto symlink_result =
        canonical::SourceFrontierPosixMappingV1::Attach(
            Options(directory->descriptor(), "frontier.link"),
            identity,
            &mapping);
    context->Expect(
        symlink_result.error ==
                canonical::SourceFrontierPosixErrorV1::kSymlinkRejected &&
            symlink_result.system_errno == ELOOP && mapping == nullptr,
        "O_NOFOLLOW rejects a symbolic-link frontier path");

    descriptor = ::openat(
        directory->descriptor(),
        "invalid.page",
        O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
        0600);
    context->Expect(descriptor >= 0, "invalid page test file is created");
    if (descriptor >= 0) {
        context->Expect(
            ::ftruncate(
                descriptor,
                static_cast<off_t>(
                    canonical::kSourceFrontierPageBytesV1)) == 0,
            "invalid page has the required physical size");
    }
    CloseOnce(descriptor);
    const auto invalid_result =
        canonical::SourceFrontierPosixMappingV1::Attach(
            Options(directory->descriptor(), "invalid.page"),
            identity,
            &mapping);
    context->Expect(
        invalid_result.error ==
                canonical::SourceFrontierPosixErrorV1::kSourceFrontierFailure &&
            invalid_result.frontier_error ==
                canonical::SourceFrontierErrorV1::kInvalidPage &&
            mapping == nullptr,
        "physical shape alone cannot bypass SourceFrontier page validation");

    descriptor = ::openat(
        directory->descriptor(),
        "wide-mode.page",
        O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
        0640);
    context->Expect(descriptor >= 0, "mode test file is created");
    if (descriptor >= 0) {
        context->Expect(
            ::fchmod(descriptor, 0640) == 0,
            "mode test file is changed to the intended unsafe mode");
        context->Expect(
            ::ftruncate(
                descriptor,
                static_cast<off_t>(
                    canonical::kSourceFrontierPageBytesV1)) == 0,
            "mode test file has the required physical size");
    }
    CloseOnce(descriptor);
    const auto mode_result =
        canonical::SourceFrontierPosixMappingV1::Attach(
            Options(directory->descriptor(), "wide-mode.page"),
            identity,
            &mapping);
    context->Expect(
        mode_result.error ==
                canonical::SourceFrontierPosixErrorV1::kWrongFileMode &&
            mapping == nullptr,
        "attacher rejects a group-readable frontier page");
}

}  // namespace

int main() {
    TestContext context;
    TestDirectory directory;
    context.Expect(
        directory.descriptor() >= 0,
        "private test directory opens");
    if (directory.descriptor() < 0) {
        return 1;
    }

    std::unique_ptr<canonical::SourceFrontierPosixMappingV1> owner;
    std::unique_ptr<canonical::SourceFrontierPosixMappingV1> attacher;
    CheckMappingAndIdentity(&context, &directory, &owner, &attacher);
    CheckRoleLeases(&context, owner.get(), attacher.get());
    CheckUnsafeArtifacts(&context, &directory);

    if (context.failures != 0) {
        std::cerr << "source frontier POSIX tests failed with "
                  << context.failures << " failure(s)\n";
        return 1;
    }
    std::cout << "source frontier POSIX tests passed\n";
    return 0;
}
