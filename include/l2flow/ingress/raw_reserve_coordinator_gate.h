#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/ingress/reserve_state_v1.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>

namespace l2flow::ingress {

// RawReserveCoordinatorLeaseMarkerV1 is an L2Flow-private, explicitly
// little-endian disk format. It is never persisted through a C++ object
// representation.
inline constexpr std::uint16_t
    kRawReserveCoordinatorLeaseVersion = 1U;
inline constexpr std::size_t
    kRawReserveCoordinatorLeaseMarkerBytes = 256U;
inline constexpr std::array<std::byte, 8U>
    kRawReserveCoordinatorLeaseMagic{
        std::byte{'L'}, std::byte{'2'}, std::byte{'R'},
        std::byte{'C'}, std::byte{'L'}, std::byte{'1'},
        std::byte{0}, std::byte{0}};

inline constexpr char kRawReserveCoordinatorLeaseFilename[] =
    "reserve.coordinator.lease";
inline constexpr char
    kRawReserveCoordinatorLeaseTemporaryFilename[] =
        ".reserve.coordinator.lease.raw-reserve-coordinator-lease-v1.tmp";

// The generation gate is a byte-range lock, not stored payload. Keeping its
// range outside the CRC field makes diagnostic reads unambiguous even though
// Linux record locks do not modify file bytes.
inline constexpr std::uint64_t
    kRawReserveGenerationGateOffset = 240U;
inline constexpr std::uint64_t
    kRawReserveGenerationGateLength = 1U;

namespace raw_reserve_coordinator_lease_offset {
inline constexpr std::size_t kMagic = 0U;
inline constexpr std::size_t kVersion = 8U;
inline constexpr std::size_t kMarkerSize = 10U;
inline constexpr std::size_t kReserved0 = 12U;
inline constexpr std::size_t kCoordinatorIdentity = 16U;
inline constexpr std::size_t kDeviceId = 32U;
inline constexpr std::size_t kQuotaIdentitySha256 = 40U;
inline constexpr std::size_t kMountIdentitySha256 = 72U;
inline constexpr std::size_t kReserved1 = 104U;
inline constexpr std::size_t kCrc32c = 252U;
}  // namespace raw_reserve_coordinator_lease_offset

// Golden domain:
//   coordinator_identity[i] = i + 1
//   device_id = 0x0102030405060708
//   quota_identity_sha256[i] = 0x20 + i
//   mount_identity_sha256[i] = 0x40 + i
// and every reserved byte is zero. The digest covers the exact 256 encoded
// bytes, including the little-endian CRC-32C at offset 252.
inline constexpr std::string_view
    kRawReserveCoordinatorLeaseGoldenSha256Hex =
        "4f062c5b60636415c153e76eb0e4690f"
        "1387f6b47f334b537c86b319d896dec1";

using RawReserveCoordinatorLeaseDigestV1 =
    l2flow::common::Sha256Digest;

struct RawReserveCoordinatorLeaseMarkerV1 final {
    // Immutable identity of the fixed coordinator lease inode. It is not a
    // reserve-state UUID: reserve.state receives a fresh UUID on every
    // offline reprovision while this lease inode and marker remain fixed.
    l2flow::common::Identity128 coordinator_identity{};
    std::uint64_t device_id = 0U;
    RawReserveCoordinatorLeaseDigestV1
        quota_identity_sha256{};
    RawReserveCoordinatorLeaseDigestV1
        mount_identity_sha256{};
    std::uint32_t crc32c = 0U;

    friend bool operator==(
        const RawReserveCoordinatorLeaseMarkerV1&,
        const RawReserveCoordinatorLeaseMarkerV1&) = default;
};

using RawReserveCoordinatorLeaseMarkerWireV1 =
    std::array<
        std::byte,
        kRawReserveCoordinatorLeaseMarkerBytes>;

[[nodiscard]] bool EncodeRawReserveCoordinatorLeaseMarkerV1(
    const RawReserveCoordinatorLeaseMarkerV1& marker,
    RawReserveCoordinatorLeaseMarkerWireV1* wire) noexcept;

[[nodiscard]] bool DecodeRawReserveCoordinatorLeaseMarkerV1(
    std::span<const std::byte> wire,
    RawReserveCoordinatorLeaseMarkerV1* marker) noexcept;

// A trusted name-to-inode anchor carried across independent openat calls.
// Gates compare both the marker and this inode identity, so replacement with
// byte-identical content is still rejected.
struct RawReserveCoordinatorLeaseAnchorV1 final {
    RawReserveCoordinatorLeaseMarkerV1 marker{};
    std::uint64_t filesystem_device = 0U;
    std::uint64_t inode = 0U;

    friend bool operator==(
        const RawReserveCoordinatorLeaseAnchorV1&,
        const RawReserveCoordinatorLeaseAnchorV1&) = default;
};

enum class RawReserveCoordinatorGateError : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kUnsafeRoot,
    kNotFound,
    kAmbiguousTemporary,
    kUnsafeLease,
    kMarkerMismatch,
    kWriteFailure,
    kSyncFailure,
    kPublishConflict,
    kCoordinatorBusy,
    kGenerationGateBusy,
    kOfdLocksUnsupported,
    kLockFailure,
    kIdentityChanged,
    kAllocationFailure,
};

[[nodiscard]] std::string_view
RawReserveCoordinatorGateErrorName(
    RawReserveCoordinatorGateError error) noexcept;

class RawReserveCoordinatorLeaseV1 final {
public:
    ~RawReserveCoordinatorLeaseV1();

    RawReserveCoordinatorLeaseV1(
        const RawReserveCoordinatorLeaseV1&) = delete;
    RawReserveCoordinatorLeaseV1& operator=(
        const RawReserveCoordinatorLeaseV1&) = delete;
    RawReserveCoordinatorLeaseV1(
        RawReserveCoordinatorLeaseV1&&) = delete;
    RawReserveCoordinatorLeaseV1& operator=(
        RawReserveCoordinatorLeaseV1&&) = delete;

    [[nodiscard]] int descriptor() const noexcept {
        return lease_fd_;
    }
    [[nodiscard]] int root_directory_descriptor()
        const noexcept {
        return root_directory_fd_;
    }
    [[nodiscard]] const
        RawReserveCoordinatorLeaseAnchorV1&
    anchor() const noexcept {
        return anchor_;
    }

private:
    friend std::unique_ptr<RawReserveCoordinatorLeaseV1>
    AcquireRawReserveCoordinatorLeaseAtV1(
        int,
        const RawReserveCoordinatorLeaseMarkerV1&,
        RawReserveCoordinatorGateError*,
        std::string*) noexcept;

    RawReserveCoordinatorLeaseV1(
        int root_directory_fd,
        int lease_fd,
        RawReserveCoordinatorLeaseAnchorV1 anchor) noexcept;

    int root_directory_fd_ = -1;
    int lease_fd_ = -1;
    RawReserveCoordinatorLeaseAnchorV1 anchor_{};
};

// Creates or attaches the fixed coordinator lease and retains both the
// private Raw-root directory and an exclusive nonblocking flock. Creation is
// marker write -> fsync(tmp) -> flock(tmp) -> RENAME_NOREPLACE ->
// fsync(root) -> name/inode/readback. Existing leases are securely opened,
// validated, locked, fsynced, and revalidated.
[[nodiscard]] std::unique_ptr<RawReserveCoordinatorLeaseV1>
AcquireRawReserveCoordinatorLeaseAtV1(
    int retained_raw_root_fd,
    const RawReserveCoordinatorLeaseMarkerV1& expected_marker,
    RawReserveCoordinatorGateError* failure = nullptr,
    std::string* error = nullptr) noexcept;

struct RawReserveGenerationActionTokenV1 final {
    l2flow::common::Identity128 reserve_state_uuid{};
    std::uint64_t state_generation = 0U;
    l2flow::common::Identity128 writer_instance_id{};
    l2flow::common::Identity128 recovery_attempt_id{};
    // Zero is the canonical value before a finalization cycle exists.
    l2flow::common::Identity128 finalization_cycle_id{};

    friend bool operator==(
        const RawReserveGenerationActionTokenV1&,
        const RawReserveGenerationActionTokenV1&) = default;
};

// The upper state layer constructs this only after decoding the latest
// durable selected state while the shared gate is held. Keeping it distinct
// from the issued token prevents a call site from accidentally "validating"
// a token against itself.
struct RawReserveGenerationObservationV1 final {
    l2flow::common::Identity128 reserve_state_uuid{};
    std::uint64_t state_generation = 0U;
    l2flow::common::Identity128 writer_instance_id{};
    l2flow::common::Identity128 recovery_attempt_id{};
    l2flow::common::Identity128 finalization_cycle_id{};
};

// The action gate owns a fresh open file description and a shared Linux OFD
// lock. Keep this object alive across the durable state-generation check,
// syscall admission, concrete syscall, and returned-object identity capture.
class RawReserveGenerationActionGateV1 final {
public:
    ~RawReserveGenerationActionGateV1();

    RawReserveGenerationActionGateV1(
        const RawReserveGenerationActionGateV1&) = delete;
    RawReserveGenerationActionGateV1& operator=(
        const RawReserveGenerationActionGateV1&) = delete;
    RawReserveGenerationActionGateV1(
        RawReserveGenerationActionGateV1&&) = delete;
    RawReserveGenerationActionGateV1& operator=(
        RawReserveGenerationActionGateV1&&) = delete;

    [[nodiscard]] const RawReserveGenerationActionTokenV1&
    token() const noexcept {
        return token_;
    }

private:
    friend std::unique_ptr<RawReserveGenerationActionGateV1>
    AcquireRawReserveGenerationActionGateAtV1(
        int,
        const RawReserveCoordinatorLeaseAnchorV1&,
        const RawReserveGenerationActionTokenV1&,
        RawReserveCoordinatorGateError*,
        std::string*) noexcept;
    friend std::unique_ptr<RawReserveGenerationActionGateV1>
    AcquireUnboundRawReserveGenerationActionGateAtV1(
        int,
        const RawReserveCoordinatorLeaseAnchorV1&,
        RawReserveCoordinatorGateError*,
        std::string*) noexcept;
    friend bool BindRawReserveGenerationActionGateTokenV1(
        RawReserveGenerationActionGateV1&,
        const RawReserveGenerationActionTokenV1&,
        std::string*) noexcept;

    RawReserveGenerationActionGateV1(
        int lease_fd,
        l2flow::common::Identity128 reserve_state_uuid,
        RawReserveGenerationActionTokenV1 token) noexcept;

    int lease_fd_ = -1;
    l2flow::common::Identity128 reserve_state_uuid_{};
    RawReserveGenerationActionTokenV1 token_{};
};

// Acquires the independent shared OFD lock without reading coordinator state.
// The caller must decode the durable state after this returns, derive its
// exact token, and bind that token once before admitting a syscall. This
// avoids racing an unprotected state read with an exclusive transition.
[[nodiscard]] std::unique_ptr<RawReserveGenerationActionGateV1>
AcquireUnboundRawReserveGenerationActionGateAtV1(
    int retained_raw_root_fd,
    const RawReserveCoordinatorLeaseAnchorV1& expected_lease,
    RawReserveCoordinatorGateError* failure = nullptr,
    std::string* error = nullptr) noexcept;

[[nodiscard]] bool
BindRawReserveGenerationActionGateTokenV1(
    RawReserveGenerationActionGateV1& gate,
    const RawReserveGenerationActionTokenV1& token,
    std::string* error = nullptr) noexcept;

[[nodiscard]] std::unique_ptr<RawReserveGenerationActionGateV1>
AcquireRawReserveGenerationActionGateAtV1(
    int retained_raw_root_fd,
    const RawReserveCoordinatorLeaseAnchorV1& expected_lease,
    const RawReserveGenerationActionTokenV1& token,
    RawReserveCoordinatorGateError* failure = nullptr,
    std::string* error = nullptr) noexcept;

// Must be called after decoding the latest durable state and immediately
// before every admitted syscall, while gate remains alive. All causal fields
// compare exactly, including an all-zero pre-finalization cycle identity.
[[nodiscard]] bool
ValidateRawReserveGenerationObservationV1(
    const RawReserveGenerationActionGateV1& gate,
    const RawReserveGenerationObservationV1& observation,
    std::string* error = nullptr) noexcept;

// Convenience bridge for RawReserveStateFileV1::state(). The caller supplies
// the route's selected entry index from its already-decoded state snapshot;
// this function extracts and exactly checks the selected slot generation,
// header/slot UUID, writer, recovery-or-executor, and cycle identities.
[[nodiscard]] bool ValidateRawReserveGenerationStateV1(
    const RawReserveGenerationActionGateV1& gate,
    const ReserveCoordinatorStateV1& latest_decoded_state,
    std::size_t entry_index,
    std::string* error = nullptr) noexcept;

// This deliberately distinct type can only be created from a new openat of
// the fixed lease. It cannot be constructed from, or converted out of, a
// shared action gate's open file description. It holds an exclusive blocking
// F_OFD_SETLKW lock for one coordinator state transition.
class RawReserveCoordinatorTransitionGuardV1 final {
public:
    ~RawReserveCoordinatorTransitionGuardV1();

    RawReserveCoordinatorTransitionGuardV1(
        const RawReserveCoordinatorTransitionGuardV1&) =
        delete;
    RawReserveCoordinatorTransitionGuardV1& operator=(
        const RawReserveCoordinatorTransitionGuardV1&) =
        delete;
    RawReserveCoordinatorTransitionGuardV1(
        RawReserveCoordinatorTransitionGuardV1&&) = delete;
    RawReserveCoordinatorTransitionGuardV1& operator=(
        RawReserveCoordinatorTransitionGuardV1&&) = delete;

private:
    friend std::unique_ptr<
        RawReserveCoordinatorTransitionGuardV1>
    AcquireRawReserveCoordinatorTransitionGuardAtV1(
        int,
        const RawReserveCoordinatorLeaseAnchorV1&,
        RawReserveCoordinatorGateError*,
        std::string*) noexcept;

    explicit RawReserveCoordinatorTransitionGuardV1(
        int lease_fd) noexcept;

    int lease_fd_ = -1;
};

[[nodiscard]] std::unique_ptr<
    RawReserveCoordinatorTransitionGuardV1>
AcquireRawReserveCoordinatorTransitionGuardAtV1(
    int retained_raw_root_fd,
    const RawReserveCoordinatorLeaseAnchorV1& expected_lease,
    RawReserveCoordinatorGateError* failure = nullptr,
    std::string* error = nullptr) noexcept;

}  // namespace l2flow::ingress
