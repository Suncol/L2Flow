#include "l2flow/ingress/raw_reserve_state_posix.h"

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <system_error>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace ingress = l2flow::ingress;

namespace {

struct TestContext final {
    void Expect(bool condition, const std::string& message) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << message << '\n';
        }
    }

    void ExpectPosixError(
        ingress::RawReserveStatePosixError actual,
        ingress::RawReserveStatePosixError expected,
        const std::string& message) {
        if (actual != expected) {
            ++failures;
            std::cerr
                << "FAIL: " << message << " (expected "
                << ingress::RawReserveStatePosixErrorName(
                       expected)
                << ", got "
                << ingress::RawReserveStatePosixErrorName(
                       actual)
                << ")\n";
        }
    }

    int failures = 0;
};

class TempDirectory final {
public:
    TempDirectory() {
        char pattern[] = "/tmp/l2flow-reserve-state-XXXXXX";
        char* const created = ::mkdtemp(pattern);
        if (created == nullptr) {
            return;
        }
        path_ = created;
        fd_ = ::open(
            path_.c_str(),
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    }

    ~TempDirectory() {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
        if (!path_.empty()) {
            std::error_code ignored;
            static_cast<void>(
                std::filesystem::remove_all(path_, ignored));
        }
    }

    TempDirectory(const TempDirectory&) = delete;
    TempDirectory& operator=(const TempDirectory&) = delete;

    [[nodiscard]] bool valid() const noexcept {
        return fd_ >= 0;
    }
    [[nodiscard]] int fd() const noexcept {
        return fd_;
    }
    [[nodiscard]] const std::string& path() const noexcept {
        return path_;
    }

private:
    std::string path_{};
    int fd_ = -1;
};

template <std::size_t Size>
std::array<std::byte, Size> Pattern(std::uint8_t first) {
    std::array<std::byte, Size> result{};
    for (std::size_t index = 0U; index < Size; ++index) {
        result[index] = static_cast<std::byte>(
            static_cast<unsigned int>(first) +
            static_cast<unsigned int>(index));
    }
    return result;
}

ingress::ReserveCoordinatorHeaderV1 MakeHeader() {
    ingress::ReserveCoordinatorHeaderV1 header{};
    header.reserve_state_uuid = Pattern<16U>(0x10U);
    header.quota_identity_sha256 = Pattern<32U>(0x20U);
    header.mount_identity_sha256 = Pattern<32U>(0x40U);
    header.device_id = 0x0102030405060708ULL;
    header.declared_releasable_bytes = 1ULL << 30U;
    header.allocation_quantum_bytes = 4096U;
    header.declared_inode_reserve_count = 1000U;
    header.byte_probe_version = 1U;
    header.inode_probe_version = 1U;
    header.inode_inventory_sha256 = Pattern<32U>(0x60U);
    header.safe_stop_catalog_sha256 = Pattern<32U>(0x80U);
    return header;
}

ingress::ReserveStateEntryV1 MakeActiveEntry() {
    ingress::ReserveStateEntryV1 entry{};
    entry.source_stream_id = 7U;
    entry.capture_date = 20260719U;
    entry.stream_day_id = Pattern<16U>(0x90U);
    entry.registry_status =
        ingress::ReserveRegistryStatusV1::kActive;
    entry.recovery_origin =
        ingress::ReserveRecoveryOriginV1::kFreshInit;
    entry.recovery_intent =
        ingress::ReserveRecoveryIntentV1::kResumeConnect;
    entry.writer_instance = Pattern<16U>(0xa0U);
    entry.executor_or_recovery_attempt = Pattern<16U>(0xb0U);
    entry.safe_stop_template_id =
        0x0102030405060708ULL;
    return entry;
}

ingress::ReserveStateSlotV1 MakeProvisionedSlot(
    const ingress::ReserveCoordinatorHeaderV1& header,
    std::uint64_t generation) {
    ingress::ReserveStateSlotV1 slot{};
    slot.coordinator_state =
        ingress::ReserveCoordinatorPhaseV1::kProvisioned;
    slot.generation = generation;
    slot.reserve_state_uuid = header.reserve_state_uuid;
    slot.entry_count = 1U;
    slot.entries[0U] = MakeActiveEntry();
    return slot;
}

ingress::ReserveCoordinatorStateV1 MakeBootstrap() {
    ingress::ReserveCoordinatorStateV1 state{};
    state.header = MakeHeader();
    state.slots[0U] =
        MakeProvisionedSlot(state.header, 1U);
    state.slots[1U] = state.slots[0U];
    state.selected_slot = 0U;
    return state;
}

bool WriteAll(
    int fd,
    std::span<const std::byte> bytes,
    std::uint64_t offset = 0U) {
    std::size_t completed = 0U;
    while (completed < bytes.size()) {
        const ssize_t result = ::pwrite(
            fd,
            bytes.data() + completed,
            bytes.size() - completed,
            static_cast<off_t>(
                offset +
                static_cast<std::uint64_t>(completed)));
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (result == 0) {
            return false;
        }
        completed += static_cast<std::size_t>(result);
    }
    return true;
}

bool ReadAll(
    int fd,
    std::span<std::byte> bytes) {
    std::size_t completed = 0U;
    while (completed < bytes.size()) {
        const ssize_t result = ::pread(
            fd,
            bytes.data() + completed,
            bytes.size() - completed,
            static_cast<off_t>(completed));
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (result == 0) {
            return false;
        }
        completed += static_cast<std::size_t>(result);
    }
    return true;
}

bool WriteNamedWire(
    int directory_fd,
    const char* name,
    const ingress::ReserveStateV1FileWire& wire) {
    const int fd = ::openat(
        directory_fd,
        name,
        O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
        0600);
    if (fd < 0) {
        return false;
    }
    const bool result =
        ::fchmod(fd, 0600) == 0 &&
        WriteAll(fd, wire) &&
        ::fsync(fd) == 0;
    static_cast<void>(::close(fd));
    return result;
}

bool ReadFixedWire(
    int directory_fd,
    ingress::ReserveStateV1FileWire* wire) {
    const int fd = ::openat(
        directory_fd,
        ingress::kRawReserveStateFilename,
        O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0) {
        return false;
    }
    const bool result = ReadAll(fd, *wire);
    static_cast<void>(::close(fd));
    return result;
}

std::unique_ptr<ingress::RawReserveStateFileV1>
PublishBootstrap(
    TestContext* test,
    TempDirectory* directory) {
    ingress::RawReserveStatePosixError failure =
        ingress::RawReserveStatePosixError::kNone;
    ingress::ReserveStateV1Error codec =
        ingress::ReserveStateV1Error::kNone;
    std::string error;
    auto state = ingress::PublishFreshRawReserveStateAtV1(
        directory->fd(),
        MakeBootstrap(),
        &failure,
        &codec,
        &error);
    test->Expect(
        state != nullptr,
        "fresh bootstrap publishes: " + error);
    test->ExpectPosixError(
        failure,
        ingress::RawReserveStatePosixError::kNone,
        "fresh bootstrap POSIX status");
    test->Expect(
        codec == ingress::ReserveStateV1Error::kNone,
        "fresh bootstrap codec status");
    return state;
}

void TestFreshAttachAndAdoption(TestContext* test) {
    TempDirectory directory;
    test->Expect(directory.valid(), "temporary root opens");
    if (!directory.valid()) {
        return;
    }

    const auto bootstrap = MakeBootstrap();
    ingress::ReserveStateV1FileWire intended{};
    test->Expect(
        ingress::EncodeReserveCoordinatorStateV1(
            bootstrap, &intended) ==
            ingress::ReserveStateV1Error::kNone,
        "bootstrap encodes for temporary adoption");
    test->Expect(
        WriteNamedWire(
            directory.fd(),
            ingress::kRawReserveStateTemporaryFilename,
            intended),
        "complete typed temporary fixture writes");

    ingress::RawReserveStatePosixError failure =
        ingress::RawReserveStatePosixError::kNone;
    ingress::ReserveStateV1Error codec =
        ingress::ReserveStateV1Error::kNone;
    std::string error;
    auto published =
        ingress::PublishFreshRawReserveStateAtV1(
            directory.fd(),
            bootstrap,
            &failure,
            &codec,
            &error);
    test->Expect(
        published != nullptr,
        "complete byte-identical temporary is adopted: " +
            error);
    test->Expect(
        ::faccessat(
            directory.fd(),
            ingress::kRawReserveStateTemporaryFilename,
            F_OK,
            AT_SYMLINK_NOFOLLOW) != 0 &&
            errno == ENOENT,
        "typed temporary name disappears after adoption");
    test->Expect(
        published != nullptr &&
            published->state().selected_slot == 0U &&
            published->state().slots[0U].generation == 1U,
        "published bootstrap selects slot zero");

    published.reset();
    auto attached = ingress::AttachRawReserveStateAtV1(
        directory.fd(), &failure, &codec, &error);
    test->Expect(
        attached != nullptr,
        "published state securely attaches: " + error);
    test->Expect(
        attached != nullptr &&
            attached->state() == bootstrap,
        "attach returns exact bootstrap model");
    attached.reset();

    auto idempotent =
        ingress::PublishFreshRawReserveStateAtV1(
            directory.fd(),
            bootstrap,
            &failure,
            &codec,
            &error);
    test->Expect(
        idempotent != nullptr,
        "byte-identical already-published bootstrap is idempotent: " +
            error);
}

void TestTransitionsAndGeneration(TestContext* test) {
    TempDirectory directory;
    auto state = PublishBootstrap(test, &directory);
    if (state == nullptr) {
        return;
    }

    ingress::ReserveStateV1Error codec =
        ingress::ReserveStateV1Error::kNone;
    std::string error;
    auto next = state->state().slots[state->state().selected_slot];
    next.generation = 2U;
    test->ExpectPosixError(
        state->PublishNext(next, &codec, &error),
        ingress::RawReserveStatePosixError::kNone,
        "generation two writes non-selected slot: " + error);
    test->Expect(
        state->state().selected_slot == 1U &&
            state->state().slots[1U].generation == 2U,
        "generation two selects slot one");

    ingress::ReserveStateV1FileWire generation_two{};
    test->Expect(
        ReadFixedWire(directory.fd(), &generation_two),
        "generation two wire reads");
    test->ExpectPosixError(
        state->PublishNext(next, &codec, &error),
        ingress::RawReserveStatePosixError::kNone,
        "exact same-generation retry is idempotent");
    ingress::ReserveStateV1FileWire after_retry{};
    test->Expect(
        ReadFixedWire(directory.fd(), &after_retry) &&
            after_retry == generation_two,
        "idempotent retry does not rewrite either slot");

    auto changed_same_generation = next;
    changed_same_generation.entries[0U].writer_instance =
        Pattern<16U>(0xc0U);
    test->ExpectPosixError(
        state->PublishNext(
            changed_same_generation, &codec, &error),
        ingress::RawReserveStatePosixError::
            kInvalidTransition,
        "non-identical same generation is rejected");

    auto skipped = next;
    skipped.generation = 4U;
    test->ExpectPosixError(
        state->PublishNext(skipped, &codec, &error),
        ingress::RawReserveStatePosixError::
            kInvalidTransition,
        "generation skip is rejected");

    auto invalid_logical_transition = next;
    invalid_logical_transition.generation = 3U;
    invalid_logical_transition.entries[0U].writer_instance =
        Pattern<16U>(0xd0U);
    test->ExpectPosixError(
        state->PublishNext(
            invalid_logical_transition, &codec, &error),
        ingress::RawReserveStatePosixError::
            kInvalidTransition,
        "codec transition rules reject an unsupported ACTIVE mutation");
    ingress::ReserveStateV1FileWire after_invalid{};
    test->Expect(
        ReadFixedWire(directory.fd(), &after_invalid) &&
            after_invalid == generation_two,
        "rejected transitions leave exact state bytes unchanged");

    auto generation_three = next;
    generation_three.generation = 3U;
    test->ExpectPosixError(
        state->PublishNext(
            generation_three, &codec, &error),
        ingress::RawReserveStatePosixError::kNone,
        "generation three writes the other slot: " + error);
    test->Expect(
        state->state().selected_slot == 0U &&
            state->state().slots[0U].generation == 3U,
        "generation three selects slot zero");

    state.reset();
    ingress::RawReserveStatePosixError failure =
        ingress::RawReserveStatePosixError::kNone;
    auto attached = ingress::AttachRawReserveStateAtV1(
        directory.fd(), &failure, &codec, &error);
    test->Expect(
        attached != nullptr &&
            attached->state().selected_slot == 0U &&
            attached->state().slots[0U].generation == 3U &&
            attached->state().slots[1U].generation == 2U,
        "reattach selects exact highest consecutive generation");
}

void TestCorruptionIsFatal(TestContext* test) {
    TempDirectory directory;
    auto state = PublishBootstrap(test, &directory);
    if (state == nullptr) {
        return;
    }
    auto next = state->state().slots[0U];
    next.generation = 2U;
    std::string error;
    test->ExpectPosixError(
        state->PublishNext(next, nullptr, &error),
        ingress::RawReserveStatePosixError::kNone,
        "corruption fixture reaches generation two");
    state.reset();

    const int fd = ::openat(
        directory.fd(),
        ingress::kRawReserveStateFilename,
        O_RDWR | O_NOFOLLOW | O_CLOEXEC);
    test->Expect(fd >= 0, "corruption fixture opens state");
    if (fd >= 0) {
        std::byte byte{};
        const off_t offset = static_cast<off_t>(
            ingress::kReserveStateV1HeaderBytes + 300U);
        test->Expect(
            ::pread(fd, &byte, 1U, offset) == 1,
            "inactive slot byte reads");
        byte ^= std::byte{0x01U};
        test->Expect(
            ::pwrite(fd, &byte, 1U, offset) == 1 &&
                ::fsync(fd) == 0,
            "inactive slot is torn");
        static_cast<void>(::close(fd));
    }

    ingress::RawReserveStatePosixError failure =
        ingress::RawReserveStatePosixError::kNone;
    ingress::ReserveStateV1Error codec =
        ingress::ReserveStateV1Error::kNone;
    auto attached = ingress::AttachRawReserveStateAtV1(
        directory.fd(), &failure, &codec, &error);
    test->Expect(
        attached == nullptr,
        "one corrupt inactive slot forbids fallback");
    test->ExpectPosixError(
        failure,
        ingress::RawReserveStatePosixError::kStateCorruption,
        "one corrupt inactive slot is classified fatal");
    test->Expect(
        codec ==
            ingress::ReserveStateV1Error::
                kSlotCorruptionFatal,
        "codec collapses slot corruption to fatal");
}

void TestUnsafeObjectsFailClosed(TestContext* test) {
    {
        TempDirectory directory;
        test->Expect(
            ::symlinkat(
                "/dev/null",
                directory.fd(),
                ingress::kRawReserveStateFilename) == 0,
            "symlink state fixture creates");
        ingress::RawReserveStatePosixError failure =
            ingress::RawReserveStatePosixError::kNone;
        std::string error;
        auto attached = ingress::AttachRawReserveStateAtV1(
            directory.fd(), &failure, nullptr, &error);
        test->Expect(
            attached == nullptr,
            "state symlink is not followed");
        test->ExpectPosixError(
            failure,
            ingress::RawReserveStatePosixError::kUnsafeFile,
            "state symlink is unsafe");
    }
    {
        TempDirectory directory;
        auto state = PublishBootstrap(test, &directory);
        state.reset();
        test->Expect(
            ::linkat(
                directory.fd(),
                ingress::kRawReserveStateFilename,
                directory.fd(),
                "reserve.state.extra",
                0) == 0,
            "hardlink state fixture creates");
        ingress::RawReserveStatePosixError failure =
            ingress::RawReserveStatePosixError::kNone;
        std::string error;
        auto attached = ingress::AttachRawReserveStateAtV1(
            directory.fd(), &failure, nullptr, &error);
        test->Expect(
            attached == nullptr,
            "multiply-linked state is rejected");
        test->ExpectPosixError(
            failure,
            ingress::RawReserveStatePosixError::kUnsafeFile,
            "hardlink count is unsafe");
    }
    {
        TempDirectory directory;
        auto state = PublishBootstrap(test, &directory);
        state.reset();
        test->Expect(
            ::fchmodat(
                directory.fd(),
                ingress::kRawReserveStateFilename,
                0644,
                0) == 0,
            "unsafe state mode fixture changes");
        ingress::RawReserveStatePosixError failure =
            ingress::RawReserveStatePosixError::kNone;
        std::string error;
        auto attached = ingress::AttachRawReserveStateAtV1(
            directory.fd(), &failure, nullptr, &error);
        test->Expect(
            attached == nullptr,
            "non-0600 state is rejected");
        test->ExpectPosixError(
            failure,
            ingress::RawReserveStatePosixError::kUnsafeFile,
            "non-0600 state is unsafe");
    }
    {
        TempDirectory directory;
        test->Expect(
            ::fchmod(directory.fd(), 0777) == 0,
            "unsafe root mode fixture changes");
        ingress::RawReserveStatePosixError failure =
            ingress::RawReserveStatePosixError::kNone;
        std::string error;
        auto attached = ingress::AttachRawReserveStateAtV1(
            directory.fd(), &failure, nullptr, &error);
        test->Expect(
            attached == nullptr,
            "group/world-writable root is rejected");
        test->ExpectPosixError(
            failure,
            ingress::RawReserveStatePosixError::
                kUnsafeDirectory,
            "unsafe root classification");
    }
}

void TestTemporaryAndFinalConflict(TestContext* test) {
    TempDirectory directory;
    auto state = PublishBootstrap(test, &directory);
    state.reset();

    ingress::ReserveStateV1FileWire wire{};
    test->Expect(
        ingress::EncodeReserveCoordinatorStateV1(
            MakeBootstrap(), &wire) ==
            ingress::ReserveStateV1Error::kNone &&
            WriteNamedWire(
                directory.fd(),
                ingress::kRawReserveStateTemporaryFilename,
                wire),
        "coexisting typed temporary fixture writes");

    ingress::RawReserveStatePosixError failure =
        ingress::RawReserveStatePosixError::kNone;
    std::string error;
    auto retry = ingress::PublishFreshRawReserveStateAtV1(
        directory.fd(),
        MakeBootstrap(),
        &failure,
        nullptr,
        &error);
    test->Expect(
        retry == nullptr,
        "final plus typed temporary fails closed");
    test->ExpectPosixError(
        failure,
        ingress::RawReserveStatePosixError::
            kAmbiguousTemporary,
        "final plus temporary is ambiguous");
}

void TestPartialTemporaryFailsClosed(TestContext* test) {
    TempDirectory directory;
    const int fd = ::openat(
        directory.fd(),
        ingress::kRawReserveStateTemporaryFilename,
        O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
        0600);
    const std::array<std::byte, 128U> partial{};
    test->Expect(
        fd >= 0 &&
            WriteAll(fd, partial) &&
            ::fsync(fd) == 0,
        "partial typed temporary fixture writes");
    if (fd >= 0) {
        static_cast<void>(::close(fd));
    }

    ingress::RawReserveStatePosixError failure =
        ingress::RawReserveStatePosixError::kNone;
    std::string error;
    auto published =
        ingress::PublishFreshRawReserveStateAtV1(
            directory.fd(),
            MakeBootstrap(),
            &failure,
            nullptr,
            &error);
    test->Expect(
        published == nullptr,
        "partial typed temporary is never completed by guessing");
    test->Expect(
        failure ==
                ingress::RawReserveStatePosixError::kUnsafeFile ||
            failure ==
                ingress::RawReserveStatePosixError::
                    kAmbiguousTemporary,
        "partial typed temporary fails closed");
    struct stat status {};
    test->Expect(
        ::fstatat(
            directory.fd(),
            ingress::kRawReserveStateTemporaryFilename,
            &status,
            AT_SYMLINK_NOFOLLOW) == 0 &&
            status.st_size ==
                static_cast<off_t>(partial.size()),
        "partial typed temporary is retained for recovery evidence");
}

void TestGenerationOverflowAndPoison(TestContext* test) {
    {
        TempDirectory directory;
        ingress::ReserveCoordinatorStateV1 maximum{};
        maximum.header = MakeHeader();
        maximum.slots[0U] = MakeProvisionedSlot(
            maximum.header,
            std::numeric_limits<std::uint64_t>::max());
        maximum.slots[1U] = MakeProvisionedSlot(
            maximum.header,
            std::numeric_limits<std::uint64_t>::max() - 1U);
        maximum.selected_slot = 0U;
        ingress::ReserveStateV1FileWire wire{};
        test->Expect(
            ingress::EncodeReserveCoordinatorStateV1(
                maximum, &wire) ==
                ingress::ReserveStateV1Error::kNone &&
                WriteNamedWire(
                    directory.fd(),
                    ingress::kRawReserveStateFilename,
                    wire),
            "maximum-generation fixture writes");
        ingress::RawReserveStatePosixError failure =
            ingress::RawReserveStatePosixError::kNone;
        std::string error;
        auto state = ingress::AttachRawReserveStateAtV1(
            directory.fd(), &failure, nullptr, &error);
        test->Expect(
            state != nullptr,
            "maximum valid generation attaches: " + error);
        if (state != nullptr) {
            auto wrapped = state->state().slots[0U];
            wrapped.generation = 0U;
            test->ExpectPosixError(
                state->PublishNext(
                    wrapped, nullptr, &error),
                ingress::RawReserveStatePosixError::
                    kGenerationOverflow,
                "maximum generation refuses a wrapped successor");
            auto nonzero = state->state().slots[0U];
            test->ExpectPosixError(
                state->PublishNext(
                    nonzero, nullptr, &error),
                ingress::RawReserveStatePosixError::kNone,
                "exact maximum-generation retry stays idempotent");
        }
    }
    {
        TempDirectory directory;
        auto state = PublishBootstrap(test, &directory);
        if (state == nullptr) {
            return;
        }
        auto next = state->state().slots[0U];
        next.generation = 2U;
        const int descriptor = state->descriptor();
        test->Expect(
            ::close(descriptor) == 0,
            "write-failure fixture closes retained descriptor");
        std::string error;
        const auto first =
            state->PublishNext(next, nullptr, &error);
        test->Expect(
            first ==
                    ingress::RawReserveStatePosixError::
                        kReadbackFailure ||
                first ==
                    ingress::RawReserveStatePosixError::
                        kUnsafeFile,
            "descriptor loss fails before mutation");
        test->Expect(
            state->poisoned(),
            "uncertain retained descriptor failure poisons handle");
        test->ExpectPosixError(
            state->PublishNext(next, nullptr, &error),
            ingress::RawReserveStatePosixError::kPoisoned,
            "poisoned handle refuses further transitions");
    }
}

void TestReloadIsMonotonic(TestContext* test) {
    {
        TempDirectory directory;
        auto stale = PublishBootstrap(test, &directory);
        ingress::RawReserveStatePosixError failure =
            ingress::RawReserveStatePosixError::kNone;
        std::string error;
        auto publisher =
            ingress::AttachRawReserveStateAtV1(
                directory.fd(),
                &failure,
                nullptr,
                &error);
        if (stale == nullptr || publisher == nullptr) {
            test->Expect(
                false,
                "monotonic reload fixtures attach");
            return;
        }
        ingress::ReserveStateV1FileWire
            generation_one{};
        test->Expect(
            ReadFixedWire(
                directory.fd(),
                &generation_one),
            "generation-one rollback image is saved");
        auto generation_two =
            publisher->state()
                .slots[
                    publisher->state()
                        .selected_slot];
        generation_two.generation = 2U;
        test->ExpectPosixError(
            publisher->PublishNext(
                generation_two, nullptr, &error),
            ingress::RawReserveStatePosixError::kNone,
            "independent handle publishes a newer generation");
        test->ExpectPosixError(
            stale->Reload(nullptr, &error),
            ingress::RawReserveStatePosixError::kNone,
            "reload accepts a strictly newer valid generation");
        test->Expect(
            stale->state()
                    .slots[
                        stale->state()
                            .selected_slot]
                    .generation == 2U,
            "newer reload refreshes the selected generation");
        test->Expect(
            WriteAll(
                stale->descriptor(),
                generation_one) &&
                ::fsync(stale->descriptor()) == 0,
            "older valid image is restored into the retained inode");
        test->ExpectPosixError(
            stale->Reload(nullptr, &error),
            ingress::RawReserveStatePosixError::
                kStateCorruption,
            "reload rejects a valid generation rollback");
        test->Expect(
            stale->poisoned(),
            "generation rollback poisons the retained handle");
    }

    {
        TempDirectory directory;
        auto state = PublishBootstrap(test, &directory);
        if (state == nullptr) {
            return;
        }
        ingress::ReserveCoordinatorStateV1 changed =
            state->state();
        changed.header.mount_identity_sha256 =
            Pattern<32U>(0xe0U);
        ingress::ReserveStateV1FileWire changed_wire{};
        test->Expect(
            ingress::EncodeReserveCoordinatorStateV1(
                changed, &changed_wire) ==
                    ingress::ReserveStateV1Error::kNone &&
                WriteAll(
                    state->descriptor(),
                    changed_wire) &&
                ::fsync(state->descriptor()) == 0,
            "same-generation different valid image is installed");
        std::string error;
        test->ExpectPosixError(
            state->Reload(nullptr, &error),
            ingress::RawReserveStatePosixError::
                kStateCorruption,
            "reload rejects different content at the cached generation");
        test->Expect(
            state->poisoned(),
            "same-generation substitution poisons the handle");
    }
}

}  // namespace

int main() {
    TestContext test;
    TestFreshAttachAndAdoption(&test);
    TestTransitionsAndGeneration(&test);
    TestCorruptionIsFatal(&test);
    TestUnsafeObjectsFailClosed(&test);
    TestTemporaryAndFinalConflict(&test);
    TestPartialTemporaryFailsClosed(&test);
    TestGenerationOverflowAndPoison(&test);
    TestReloadIsMonotonic(&test);
    if (test.failures != 0) {
        std::cerr
            << test.failures
            << " raw reserve state POSIX test(s) failed\n";
        return EXIT_FAILURE;
    }
    std::cout
        << "raw reserve state POSIX tests passed\n";
    return EXIT_SUCCESS;
}
