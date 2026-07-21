#include "l2flow/ingress/raw_reserve_coordinator_gate.h"

#include "l2flow/common/crc32c.h"
#include "l2flow/common/sha256.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <type_traits>

#include <sys/stat.h>
#include <unistd.h>

namespace common = l2flow::common;
namespace ingress = l2flow::ingress;

namespace {

template <typename Type>
concept ExposesRawDescriptor =
    requires(const Type& value) {
        value.descriptor();
    };

static_assert(
    !std::is_copy_constructible_v<
        ingress::RawReserveGenerationActionGateV1>);
static_assert(
    !std::is_move_constructible_v<
        ingress::RawReserveGenerationActionGateV1>);
static_assert(
    !std::is_constructible_v<
        ingress::RawReserveCoordinatorTransitionGuardV1,
        ingress::RawReserveGenerationActionGateV1&&>);
static_assert(
    !std::is_constructible_v<
        ingress::RawReserveCoordinatorTransitionGuardV1,
        int>);
static_assert(
    !ExposesRawDescriptor<
        ingress::RawReserveGenerationActionGateV1>);
static_assert(
    !ExposesRawDescriptor<
        ingress::RawReserveCoordinatorTransitionGuardV1>);

struct TestContext final {
    void Expect(
        bool condition,
        const std::string& description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    int failures = 0;
};

class TemporaryDirectory final {
public:
    TemporaryDirectory() {
        std::array<char, 72U> path{};
        const std::string pattern =
            "/tmp/l2flow-reserve-coordinator-gate-XXXXXX";
        std::copy(
            pattern.begin(), pattern.end(), path.begin());
        char* const created = ::mkdtemp(path.data());
        if (created != nullptr) {
            path_ = created;
            static_cast<void>(
                ::chmod(path_.c_str(), 0700U));
            fd_ = ::open(
                path_.c_str(),
                O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                    O_CLOEXEC);
        }
    }

    ~TemporaryDirectory() {
        if (fd_ >= 0) {
            const std::array<const char*, 4U> names{
                ingress::
                    kRawReserveCoordinatorLeaseFilename,
                ingress::
                    kRawReserveCoordinatorLeaseTemporaryFilename,
                "lease-old",
                "lease-hardlink"};
            for (const char* name : names) {
                static_cast<void>(
                    ::unlinkat(fd_, name, 0));
            }
            static_cast<void>(::close(fd_));
        }
        if (!path_.empty()) {
            static_cast<void>(::rmdir(path_.c_str()));
        }
    }

    TemporaryDirectory(const TemporaryDirectory&) = delete;
    TemporaryDirectory& operator=(
        const TemporaryDirectory&) = delete;

    [[nodiscard]] int fd() const noexcept {
        return fd_;
    }

    [[nodiscard]] const std::string& path() const noexcept {
        return path_;
    }

private:
    std::string path_;
    int fd_ = -1;
};

template <std::size_t Size>
[[nodiscard]] std::array<std::byte, Size> Pattern(
    std::uint8_t first) {
    std::array<std::byte, Size> result{};
    for (std::size_t index = 0U; index < Size; ++index) {
        result[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                first + static_cast<std::uint8_t>(index)));
    }
    return result;
}

[[nodiscard]] ingress::
    RawReserveCoordinatorLeaseMarkerV1
MakeMarker(
    int directory_fd,
    std::uint8_t identity_seed = 1U) {
    struct stat status {};
    static_cast<void>(::fstat(directory_fd, &status));
    ingress::RawReserveCoordinatorLeaseMarkerV1 marker{};
    marker.coordinator_identity =
        Pattern<16U>(identity_seed);
    marker.device_id =
        static_cast<std::uint64_t>(status.st_dev);
    marker.quota_identity_sha256 =
        Pattern<32U>(
            static_cast<std::uint8_t>(
                0x20U + identity_seed));
    marker.mount_identity_sha256 =
        Pattern<32U>(
            static_cast<std::uint8_t>(
                0x40U + identity_seed));
    return marker;
}

[[nodiscard]] ingress::RawReserveGenerationActionTokenV1
MakeToken(
    const ingress::RawReserveCoordinatorLeaseMarkerV1&
        marker) {
    ingress::RawReserveGenerationActionTokenV1 token{};
    static_cast<void>(marker);
    token.reserve_state_uuid = Pattern<16U>(0x51U);
    token.state_generation = 17U;
    token.writer_instance_id = Pattern<16U>(0x71U);
    token.recovery_attempt_id = Pattern<16U>(0x91U);
    token.finalization_cycle_id =
        Pattern<16U>(0xb1U);
    return token;
}

[[nodiscard]] bool PwriteAll(
    int fd,
    std::span<const std::byte> bytes) {
    std::size_t completed = 0U;
    while (completed < bytes.size()) {
        const ssize_t result = ::pwrite(
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

[[nodiscard]] bool WriteFileAt(
    int directory_fd,
    const char* name,
    std::span<const std::byte> bytes,
    mode_t mode = 0600U) {
    const int fd = ::openat(
        directory_fd,
        name,
        O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW |
            O_CLOEXEC,
        mode);
    if (fd < 0) {
        return false;
    }
    const bool okay =
        ::fchmod(fd, mode) == 0 &&
        PwriteAll(fd, bytes) &&
        ::fsync(fd) == 0;
    static_cast<void>(::close(fd));
    return okay;
}

void StoreU32(
    std::uint32_t value,
    std::span<std::byte> output,
    std::size_t offset) {
    for (std::size_t index = 0U; index < 4U; ++index) {
        output[offset + index] = static_cast<std::byte>(
            (value >>
             static_cast<unsigned int>(index * 8U)) &
            0xffU);
    }
}

[[nodiscard]] bool IndependentExclusiveOfdLock(
    int root_fd) {
#if defined(F_OFD_SETLK)
    const int fd = ::openat(
        root_fd,
        ingress::kRawReserveCoordinatorLeaseFilename,
        O_RDWR | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
    if (fd < 0) {
        return false;
    }
    struct flock lock {};
    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = static_cast<off_t>(
        ingress::kRawReserveGenerationGateOffset);
    lock.l_len = static_cast<off_t>(
        ingress::kRawReserveGenerationGateLength);
    lock.l_pid = 0;
    const bool acquired =
        ::fcntl(fd, F_OFD_SETLK, &lock) == 0;
    static_cast<void>(::close(fd));
    return acquired;
#else
    static_cast<void>(root_fd);
    return false;
#endif
}

void TestCodecAndGolden(TestContext& test) {
    ingress::RawReserveCoordinatorLeaseMarkerV1 marker{};
    marker.coordinator_identity = Pattern<16U>(0x01U);
    marker.device_id = UINT64_C(0x0102030405060708);
    marker.quota_identity_sha256 = Pattern<32U>(0x20U);
    marker.mount_identity_sha256 = Pattern<32U>(0x40U);

    ingress::RawReserveCoordinatorLeaseMarkerWireV1 wire{};
    test.Expect(
        ingress::EncodeRawReserveCoordinatorLeaseMarkerV1(
            marker, &wire),
        "coordinator lease marker encodes");
    test.Expect(
        std::equal(
            ingress::kRawReserveCoordinatorLeaseMagic.begin(),
            ingress::kRawReserveCoordinatorLeaseMagic.end(),
            wire.begin()),
        "coordinator lease marker uses frozen magic");
    test.Expect(
        wire[
            ingress::raw_reserve_coordinator_lease_offset::
                kDeviceId] == std::byte{0x08U} &&
            wire[
                ingress::raw_reserve_coordinator_lease_offset::
                    kDeviceId +
                7U] == std::byte{0x01U},
        "coordinator lease device id is explicit little-endian");

    ingress::RawReserveCoordinatorLeaseMarkerV1 decoded{};
    test.Expect(
        ingress::DecodeRawReserveCoordinatorLeaseMarkerV1(
            wire, &decoded) &&
            decoded.coordinator_identity ==
                marker.coordinator_identity &&
            decoded.device_id == marker.device_id &&
            decoded.quota_identity_sha256 ==
                marker.quota_identity_sha256 &&
            decoded.mount_identity_sha256 ==
                marker.mount_identity_sha256 &&
            decoded.crc32c != 0U,
        "coordinator lease marker round-trips with CRC");

    const std::string golden =
        common::Sha256Hex(common::ComputeSha256(wire));
    if (golden !=
        ingress::
            kRawReserveCoordinatorLeaseGoldenSha256Hex) {
        std::cerr << "coordinator lease golden SHA-256: "
                  << golden << '\n';
    }
    test.Expect(
        golden ==
            ingress::
                kRawReserveCoordinatorLeaseGoldenSha256Hex,
        "coordinator lease golden bytes remain frozen");

    auto corrupt = wire;
    corrupt[
        ingress::raw_reserve_coordinator_lease_offset::
            kReserved1] = std::byte{1U};
    StoreU32(
        0U,
        corrupt,
        ingress::raw_reserve_coordinator_lease_offset::
            kCrc32c);
    StoreU32(
        common::ComputeCrc32c(corrupt),
        corrupt,
        ingress::raw_reserve_coordinator_lease_offset::
            kCrc32c);
    test.Expect(
        !ingress::DecodeRawReserveCoordinatorLeaseMarkerV1(
            corrupt, &decoded),
        "nonzero reserved bytes are rejected even with a valid CRC");

    corrupt = wire;
    corrupt[
        ingress::raw_reserve_coordinator_lease_offset::
            kCrc32c] ^= std::byte{1U};
    test.Expect(
        !ingress::DecodeRawReserveCoordinatorLeaseMarkerV1(
            corrupt, &decoded),
        "corrupt coordinator lease CRC is rejected");
}

void TestLeaseAndGenerationGate(TestContext& test) {
    TemporaryDirectory directory;
    test.Expect(
        directory.fd() >= 0,
        "private Raw-root test directory opens");
    const auto marker = MakeMarker(directory.fd());
    ingress::RawReserveCoordinatorGateError failure =
        ingress::RawReserveCoordinatorGateError::kNone;
    std::string error;
    auto coordinator =
        ingress::AcquireRawReserveCoordinatorLeaseAtV1(
            directory.fd(),
            marker,
            &failure,
            &error);
    test.Expect(
        coordinator != nullptr,
        "fresh fixed coordinator lease publishes and locks");
    if (coordinator == nullptr) {
        return;
    }
    struct stat lease_status {};
    test.Expect(
        ::fstat(
            coordinator->descriptor(),
            &lease_status) == 0 &&
            S_ISREG(lease_status.st_mode) &&
            (lease_status.st_mode & 0777U) == 0600U &&
            lease_status.st_nlink == 1,
        "fixed coordinator lease is regular 0600 nlink=1");

    auto second =
        ingress::AcquireRawReserveCoordinatorLeaseAtV1(
            directory.fd(),
            marker,
            &failure,
            &error);
    test.Expect(
        second == nullptr &&
            failure ==
                ingress::RawReserveCoordinatorGateError::
                    kCoordinatorBusy,
        "second coordinator cannot acquire the fixed flock");

    const auto token = MakeToken(marker);
    auto first_action =
        ingress::AcquireRawReserveGenerationActionGateAtV1(
            directory.fd(),
            coordinator->anchor(),
            token,
            &failure,
            &error);
    test.Expect(
        first_action != nullptr &&
            token.reserve_state_uuid !=
                marker.coordinator_identity,
        "generation action binds the current reserve UUID independently of the immutable lease identity");
    if (first_action == nullptr) {
        return;
    }
    test.Expect(
        first_action->token() == token,
        "independent generation action retains every causal token identity");

    auto second_action =
        ingress::AcquireRawReserveGenerationActionGateAtV1(
            directory.fd(),
            coordinator->anchor(),
            token,
            &failure,
            &error);
    test.Expect(
        second_action != nullptr,
        "independent shared OFD generation gates coexist");
    test.Expect(
        !IndependentExclusiveOfdLock(directory.fd()),
        "an independent exclusive OFD cannot bypass shared gates");

    ingress::RawReserveGenerationObservationV1 observation{};
    observation.reserve_state_uuid =
        token.reserve_state_uuid;
    observation.state_generation =
        token.state_generation;
    observation.writer_instance_id =
        token.writer_instance_id;
    observation.recovery_attempt_id =
        token.recovery_attempt_id;
    observation.finalization_cycle_id =
        token.finalization_cycle_id;
    test.Expect(
        ingress::ValidateRawReserveGenerationObservationV1(
            *first_action, observation, &error),
        "latest decoded state exactly validates under the gate");
    ++observation.state_generation;
    test.Expect(
        !ingress::ValidateRawReserveGenerationObservationV1(
            *first_action, observation, &error),
        "a newer durable generation revokes the action token");
    --observation.state_generation;
    observation.writer_instance_id[0U] ^= std::byte{1U};
    test.Expect(
        !ingress::ValidateRawReserveGenerationObservationV1(
            *first_action, observation, &error),
        "writer identity changes revoke the action token");
    observation.writer_instance_id =
        token.writer_instance_id;
    observation.recovery_attempt_id[0U] ^=
        std::byte{1U};
    test.Expect(
        !ingress::ValidateRawReserveGenerationObservationV1(
            *first_action, observation, &error),
        "recovery identity changes revoke the action token");
    observation.recovery_attempt_id =
        token.recovery_attempt_id;
    observation.finalization_cycle_id[0U] ^=
        std::byte{1U};
    test.Expect(
        !ingress::ValidateRawReserveGenerationObservationV1(
            *first_action, observation, &error),
        "cycle identity changes revoke the action token");

    ingress::ReserveCoordinatorStateV1 latest{};
    latest.header.reserve_state_uuid =
        token.reserve_state_uuid;
    latest.selected_slot = 0U;
    latest.slots[0U].reserve_state_uuid =
        token.reserve_state_uuid;
    latest.slots[0U].generation =
        token.state_generation;
    latest.slots[0U].entry_count = 1U;
    latest.slots[0U].finalization_cycle_id =
        token.finalization_cycle_id;
    latest.slots[0U].entries[0U].writer_instance =
        token.writer_instance_id;
    latest.slots[0U]
        .entries[0U]
        .executor_or_recovery_attempt =
        token.recovery_attempt_id;
    test.Expect(
        ingress::ValidateRawReserveGenerationStateV1(
            *first_action, latest, 0U, &error),
        "latest decoded reserve state bridges directly to exact gate validation");
    latest.slots[0U].generation =
        token.state_generation + 1U;
    test.Expect(
        !ingress::ValidateRawReserveGenerationStateV1(
            *first_action, latest, 0U, &error),
        "decoded-state bridge rejects a transitioned generation");

    first_action.reset();
    second_action.reset();
    test.Expect(
        IndependentExclusiveOfdLock(directory.fd()),
        "closing action descriptors releases shared OFD locks");

    auto transition =
        ingress::
            AcquireRawReserveCoordinatorTransitionGuardAtV1(
                directory.fd(),
                coordinator->anchor(),
                &failure,
                &error);
    test.Expect(
        transition != nullptr,
        "coordinator transition independently opens and acquires an exclusive OFD gate");
    if (transition != nullptr) {
        auto blocked_action =
            ingress::
                AcquireRawReserveGenerationActionGateAtV1(
                    directory.fd(),
                    coordinator->anchor(),
                    token,
                    &failure,
                    &error);
        test.Expect(
            blocked_action == nullptr &&
                failure ==
                    ingress::
                        RawReserveCoordinatorGateError::
                            kGenerationGateBusy,
            "exclusive transition gate rejects new shared actions");
    }
    transition.reset();
    auto after_release =
        ingress::AcquireRawReserveGenerationActionGateAtV1(
            directory.fd(),
            coordinator->anchor(),
            token,
            &failure,
            &error);
    test.Expect(
        after_release != nullptr,
        "closing transition descriptor releases exclusive OFD lock");
    after_release.reset();
    coordinator.reset();
    auto restarted =
        ingress::AcquireRawReserveCoordinatorLeaseAtV1(
            directory.fd(),
            marker,
            &failure,
            &error);
    test.Expect(
        restarted != nullptr,
        "closing coordinator descriptor releases the fixed flock");
}

void TestNameReplacementIsRejected(TestContext& test) {
    TemporaryDirectory directory;
    const auto marker = MakeMarker(directory.fd());
    ingress::RawReserveCoordinatorGateError failure{};
    std::string error;
    auto coordinator =
        ingress::AcquireRawReserveCoordinatorLeaseAtV1(
            directory.fd(),
            marker,
            &failure,
            &error);
    test.Expect(
        coordinator != nullptr,
        "replacement fixture acquires original lease");
    if (coordinator == nullptr) {
        return;
    }
    ingress::RawReserveCoordinatorLeaseMarkerWireV1 wire{};
    static_cast<void>(
        ingress::EncodeRawReserveCoordinatorLeaseMarkerV1(
            marker, &wire));
    const bool replaced =
        ::renameat(
            directory.fd(),
            ingress::kRawReserveCoordinatorLeaseFilename,
            directory.fd(),
            "lease-old") == 0 &&
        WriteFileAt(
            directory.fd(),
            ingress::kRawReserveCoordinatorLeaseFilename,
            wire);
    test.Expect(
        replaced,
        "replacement fixture installs byte-identical new inode");
    if (!replaced) {
        return;
    }
    const auto token = MakeToken(marker);
    auto gate =
        ingress::AcquireRawReserveGenerationActionGateAtV1(
            directory.fd(),
            coordinator->anchor(),
            token,
            &failure,
            &error);
    test.Expect(
        gate == nullptr &&
            failure ==
                ingress::RawReserveCoordinatorGateError::
                    kIdentityChanged,
        "name replacement is rejected despite byte-identical marker");
}

void TestUnsafeLeaseFixtures(TestContext& test) {
    {
        TemporaryDirectory directory;
        const auto marker = MakeMarker(directory.fd());
        ingress::RawReserveCoordinatorGateError failure{};
        std::string error;
        auto lease =
            ingress::AcquireRawReserveCoordinatorLeaseAtV1(
                directory.fd(),
                marker,
                &failure,
                &error);
        lease.reset();
        const int link_result =
            ::linkat(
                directory.fd(),
                ingress::
                    kRawReserveCoordinatorLeaseFilename,
                directory.fd(),
                "lease-hardlink",
                0);
        test.Expect(
            link_result == 0,
            "hard-link fixture is created");
        auto rejected =
            ingress::AcquireRawReserveCoordinatorLeaseAtV1(
                directory.fd(),
                marker,
                &failure,
                &error);
        test.Expect(
            rejected == nullptr &&
                failure ==
                    ingress::
                        RawReserveCoordinatorGateError::
                            kUnsafeLease,
            "hard-linked coordinator lease is rejected");
    }
    {
        TemporaryDirectory directory;
        const auto marker = MakeMarker(directory.fd());
        ingress::RawReserveCoordinatorGateError failure{};
        std::string error;
        auto lease =
            ingress::AcquireRawReserveCoordinatorLeaseAtV1(
                directory.fd(),
                marker,
                &failure,
                &error);
        lease.reset();
        const int chmod_result =
            ::fchmodat(
                directory.fd(),
                ingress::
                    kRawReserveCoordinatorLeaseFilename,
                0640U,
                0);
        test.Expect(
            chmod_result == 0,
            "wrong-mode fixture is created");
        auto rejected =
            ingress::AcquireRawReserveCoordinatorLeaseAtV1(
                directory.fd(),
                marker,
                &failure,
                &error);
        test.Expect(
            rejected == nullptr &&
                failure ==
                    ingress::
                        RawReserveCoordinatorGateError::
                            kUnsafeLease,
            "wrong coordinator lease mode is rejected");
    }
    {
        TemporaryDirectory directory;
        const auto marker = MakeMarker(directory.fd());
        auto other_marker = marker;
        other_marker.coordinator_identity =
            Pattern<16U>(0xe0U);
        ingress::RawReserveCoordinatorLeaseMarkerWireV1 wire{};
        static_cast<void>(
            ingress::EncodeRawReserveCoordinatorLeaseMarkerV1(
                other_marker, &wire));
        static_cast<void>(
            WriteFileAt(
                directory.fd(),
                ingress::
                    kRawReserveCoordinatorLeaseFilename,
                wire));
        ingress::RawReserveCoordinatorGateError failure{};
        std::string error;
        auto rejected =
            ingress::AcquireRawReserveCoordinatorLeaseAtV1(
                directory.fd(),
                marker,
                &failure,
                &error);
        test.Expect(
            rejected == nullptr &&
                failure ==
                    ingress::
                        RawReserveCoordinatorGateError::
                            kMarkerMismatch,
            "coordinator lease marker identity mismatch is rejected");
    }
    {
        TemporaryDirectory directory;
        const auto marker = MakeMarker(directory.fd());
        ingress::RawReserveCoordinatorLeaseMarkerWireV1 wire{};
        static_cast<void>(
            ingress::EncodeRawReserveCoordinatorLeaseMarkerV1(
                marker, &wire));
        wire[
            ingress::raw_reserve_coordinator_lease_offset::
                kCrc32c] ^= std::byte{1U};
        static_cast<void>(
            WriteFileAt(
                directory.fd(),
                ingress::
                    kRawReserveCoordinatorLeaseFilename,
                wire));
        ingress::RawReserveCoordinatorGateError failure{};
        std::string error;
        auto rejected =
            ingress::AcquireRawReserveCoordinatorLeaseAtV1(
                directory.fd(),
                marker,
                &failure,
                &error);
        test.Expect(
            rejected == nullptr &&
                failure ==
                    ingress::
                        RawReserveCoordinatorGateError::
                            kUnsafeLease,
            "coordinator lease CRC corruption is rejected");
    }
    {
        TemporaryDirectory directory;
        const auto marker = MakeMarker(directory.fd());
        static_cast<void>(
            ::fchmod(directory.fd(), 0770U));
        ingress::RawReserveCoordinatorGateError failure{};
        std::string error;
        auto rejected =
            ingress::AcquireRawReserveCoordinatorLeaseAtV1(
                directory.fd(),
                marker,
                &failure,
                &error);
        test.Expect(
            rejected == nullptr &&
                failure ==
                    ingress::
                        RawReserveCoordinatorGateError::
                            kUnsafeRoot,
            "group-writable Raw root is rejected");
    }
}

void TestTypedTemporaryStates(TestContext& test) {
    {
        TemporaryDirectory directory;
        const auto marker = MakeMarker(directory.fd());
        ingress::RawReserveCoordinatorLeaseMarkerWireV1 wire{};
        static_cast<void>(
            ingress::EncodeRawReserveCoordinatorLeaseMarkerV1(
                marker, &wire));
        test.Expect(
            WriteFileAt(
                directory.fd(),
                ingress::
                    kRawReserveCoordinatorLeaseTemporaryFilename,
                wire),
            "complete typed coordinator lease temporary is created");
        ingress::RawReserveCoordinatorGateError failure{};
        std::string error;
        auto adopted =
            ingress::AcquireRawReserveCoordinatorLeaseAtV1(
                directory.fd(),
                marker,
                &failure,
                &error);
        struct stat final_status {};
        struct stat temporary_status {};
        test.Expect(
            adopted != nullptr &&
                ::fstatat(
                    directory.fd(),
                    ingress::
                        kRawReserveCoordinatorLeaseFilename,
                    &final_status,
                    AT_SYMLINK_NOFOLLOW) == 0 &&
                ::fstatat(
                    directory.fd(),
                    ingress::
                        kRawReserveCoordinatorLeaseTemporaryFilename,
                    &temporary_status,
                    AT_SYMLINK_NOFOLLOW) != 0 &&
                errno == ENOENT,
            "complete typed temporary is synced and adopted with NOREPLACE");
    }
    {
        TemporaryDirectory directory;
        const auto marker = MakeMarker(directory.fd());
        ingress::RawReserveCoordinatorGateError failure{};
        std::string error;
        auto published =
            ingress::AcquireRawReserveCoordinatorLeaseAtV1(
                directory.fd(),
                marker,
                &failure,
                &error);
        published.reset();
        ingress::RawReserveCoordinatorLeaseMarkerWireV1 wire{};
        static_cast<void>(
            ingress::EncodeRawReserveCoordinatorLeaseMarkerV1(
                marker, &wire));
        static_cast<void>(
            WriteFileAt(
                directory.fd(),
                ingress::
                    kRawReserveCoordinatorLeaseTemporaryFilename,
                wire));
        auto ambiguous =
            ingress::AcquireRawReserveCoordinatorLeaseAtV1(
                directory.fd(),
                marker,
                &failure,
                &error);
        test.Expect(
            ambiguous == nullptr &&
                failure ==
                    ingress::
                        RawReserveCoordinatorGateError::
                            kAmbiguousTemporary,
            "fixed lease plus typed temporary fails closed");
    }
    {
        TemporaryDirectory directory;
        const auto marker = MakeMarker(directory.fd());
        const std::array<std::byte, 1U> partial{
            std::byte{'L'}};
        static_cast<void>(
            WriteFileAt(
                directory.fd(),
                ingress::
                    kRawReserveCoordinatorLeaseTemporaryFilename,
                partial));
        ingress::RawReserveCoordinatorGateError failure{};
        std::string error;
        auto rejected =
            ingress::AcquireRawReserveCoordinatorLeaseAtV1(
                directory.fd(),
                marker,
                &failure,
                &error);
        test.Expect(
            rejected == nullptr &&
                failure ==
                    ingress::
                        RawReserveCoordinatorGateError::
                            kUnsafeLease,
            "partial typed coordinator lease temporary is rejected");
    }
}

}  // namespace

int main() {
    TestContext test;
    TestCodecAndGolden(test);
    TestLeaseAndGenerationGate(test);
    TestNameReplacementIsRejected(test);
    TestUnsafeLeaseFixtures(test);
    TestTypedTemporaryStates(test);

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Phase 2 reserve coordinator-gate tests failed\n";
        return 1;
    }
    std::cout
        << "Phase 2 reserve coordinator-gate tests passed\n";
    return 0;
}
