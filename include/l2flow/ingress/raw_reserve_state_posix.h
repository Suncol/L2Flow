#pragma once

#include "l2flow/ingress/reserve_state_v1.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::ingress {

// ReserveCoordinatorStateV1 has one fixed name in the retained Raw root.
// The typed temporary is an implementation-private publication candidate and
// is never a valid attached state name.
inline constexpr char kRawReserveStateFilename[] = "reserve.state";
inline constexpr char kRawReserveStateTemporaryFilename[] =
    ".reserve.state.reserve-state-v1.tmp";

enum class RawReserveStatePosixError : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kUnsafeDirectory,
    kNotFound,
    kUnsafeFile,
    kAmbiguousTemporary,
    kCodecFailure,
    kStateCorruption,
    kWriteFailure,
    kSyncFailure,
    kPublishConflict,
    kReadbackFailure,
    kInvalidTransition,
    kGenerationOverflow,
    kAllocationFailure,
    kPoisoned,
};

[[nodiscard]] std::string_view RawReserveStatePosixErrorName(
    RawReserveStatePosixError error) noexcept;

// A successful attach retains both the root directory descriptor and the
// O_RDWR state descriptor. Mutations always use this retained state
// descriptor; the fixed pathname is used only to prove name-to-inode
// continuity. This layer does not acquire the coordinator lease: its caller
// must already hold that root's exclusive coordinator lease for every
// publication or transition.
class RawReserveStateFileV1 final {
public:
    ~RawReserveStateFileV1();

    RawReserveStateFileV1(
        const RawReserveStateFileV1&) = delete;
    RawReserveStateFileV1& operator=(
        const RawReserveStateFileV1&) = delete;
    RawReserveStateFileV1(
        RawReserveStateFileV1&&) = delete;
    RawReserveStateFileV1& operator=(
        RawReserveStateFileV1&&) = delete;

    [[nodiscard]] int descriptor() const noexcept {
        return state_fd_;
    }
    [[nodiscard]] int directory_descriptor() const noexcept {
        return directory_fd_;
    }
    [[nodiscard]] const ReserveCoordinatorStateV1& state()
        const noexcept {
        return state_;
    }
    [[nodiscard]] bool poisoned() const noexcept {
        return poisoned_;
    }

    // Re-reads and validates the complete two-slot image through the retained
    // descriptor and fixed name. Unlike PublishNext(), this accepts a newer
    // fully valid selected generation and refreshes the cached image. The
    // coordinator must hold the appropriate independent OFD generation gate
    // for the entire reload/check/action window.
    [[nodiscard]] RawReserveStatePosixError Reload(
        ReserveStateV1Error* codec_error = nullptr,
        std::string* error = nullptr) noexcept;

    // Publishes next into the non-selected slot. An exact repeat of the
    // already-selected logical state is an idempotent success. Every new
    // transition must be generation+1 and pass the codec's transition
    // validator before any write. Once a write may have reached the inode,
    // any failure poisons this handle and requires a fresh fail-closed attach.
    [[nodiscard]] RawReserveStatePosixError PublishNext(
        const ReserveStateSlotV1& next,
        ReserveStateV1Error* codec_error = nullptr,
        std::string* error = nullptr) noexcept;

private:
    friend std::unique_ptr<RawReserveStateFileV1>
    AttachRawReserveStateAtV1(
        int,
        RawReserveStatePosixError*,
        ReserveStateV1Error*,
        std::string*) noexcept;
    friend std::unique_ptr<RawReserveStateFileV1>
    PublishFreshRawReserveStateAtV1(
        int,
        const ReserveCoordinatorStateV1&,
        RawReserveStatePosixError*,
        ReserveStateV1Error*,
        std::string*) noexcept;

    RawReserveStateFileV1(
        int directory_fd,
        int state_fd,
        ReserveCoordinatorStateV1 state,
        ReserveStateV1FileWire wire) noexcept;

    int directory_fd_ = -1;
    int state_fd_ = -1;
    ReserveCoordinatorStateV1 state_{};
    ReserveStateV1FileWire wire_{};
    bool poisoned_ = false;
};

// Securely attaches the fixed state. Both slots are decoded on every attach;
// any invalid inactive slot is fatal and is never treated as a fallback.
// The coordinator must acquire and validate its fixed lease before calling.
[[nodiscard]] std::unique_ptr<RawReserveStateFileV1>
AttachRawReserveStateAtV1(
    int retained_root_directory_fd,
    RawReserveStatePosixError* failure = nullptr,
    ReserveStateV1Error* codec_error = nullptr,
    std::string* error = nullptr) noexcept;

// Publishes the unique generation=1 PROVISIONED bootstrap through the
// deterministic typed temporary, fsync(state), RENAME_NOREPLACE,
// fsync(root), then same-fd readback and full decode. A byte-identical
// already-published bootstrap is an idempotent success. A complete
// byte-identical temporary may be adopted when the fixed name is absent.
//
// failure receives the POSIX-layer failure because the nullptr return alone
// cannot distinguish an idempotent attach failure from publication failure.
// Initial provision/reprovision must hold the external coordinator lease for
// the complete candidate transaction.
[[nodiscard]] std::unique_ptr<RawReserveStateFileV1>
PublishFreshRawReserveStateAtV1(
    int retained_root_directory_fd,
    const ReserveCoordinatorStateV1& bootstrap,
    RawReserveStatePosixError* failure = nullptr,
    ReserveStateV1Error* codec_error = nullptr,
    std::string* error = nullptr) noexcept;

}  // namespace l2flow::ingress
