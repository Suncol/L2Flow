#pragma once

#include "l2flow/route/production_route_posix_store_v1.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::route {

inline constexpr std::string_view kProductionRouteOwnerLockFilenameV1 =
    "production-route-v1.live.lock";
inline constexpr std::string_view kProductionRouteOwnerMetadataFilenameV1 =
    "production-route-v1.live-v1.bin";
inline constexpr std::string_view
    kProductionRouteOwnerMetadataTemporaryFilenameV1 =
        ".production-route-v1.live-v1.tmp";

enum class ProductionRouteOwnerLeaseErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kUnsupported,
    kUnsafeDirectory,
    kOpenFailure,
    kUnsafeFile,
    kHeldByAnotherOwner,
    kLockFailure,
    kEncodeFailure,
    kWriteFailure,
    kFileSyncFailure,
    kDirectorySyncFailure,
    kReadFailure,
    kInvalidLease,
    kManifestMismatch,
    kOwnerMissing,
    kRouteNotActive,
    kRouteChanged,
    kAllocationFailure,
    kFreshArtifactPresent,
};

[[nodiscard]] std::string_view ProductionRouteOwnerLeaseErrorNameV1(
    ProductionRouteOwnerLeaseErrorV1 error) noexcept;

// Holds one process-associated POSIX record write lock.  Adjacent durable
// metadata binds its owner PID, Linux boot ID, /proc start ticks and lock
// inode to the exact encoded Active manifest.  Kernel release on process
// death makes a stale Active route fail the live-reader gate even when no
// destructor or signal handler ran.  Because closing any descriptor for this
// inode releases the process's record locks, Acquire fences the directory
// identity before opening it and owner code must not open this lock name by
// another path.  Acquire also proves pidfd_open support before creating owner
// files, because cross-process readers require a pinned process identity.
class ProductionRouteOwnerLeaseV1 final {
public:
    ~ProductionRouteOwnerLeaseV1();

    ProductionRouteOwnerLeaseV1(
        const ProductionRouteOwnerLeaseV1&) = delete;
    ProductionRouteOwnerLeaseV1& operator=(
        const ProductionRouteOwnerLeaseV1&) = delete;
    ProductionRouteOwnerLeaseV1(
        ProductionRouteOwnerLeaseV1&&) = delete;
    ProductionRouteOwnerLeaseV1& operator=(
        ProductionRouteOwnerLeaseV1&&) = delete;

    [[nodiscard]] static ProductionRouteOwnerLeaseErrorV1 Acquire(
        int retained_directory_fd,
        const ProductionRouteManifestV1& active_manifest,
        std::unique_ptr<ProductionRouteOwnerLeaseV1>* output,
        std::string* diagnostic = nullptr) noexcept;

    // Strict production fresh acquisition.  The caller must already hold the
    // route publication gate.  Every fixed owner lock/metadata artifact must
    // be absent; this call never adopts, removes, or overwrites an old one.
    [[nodiscard]] static ProductionRouteOwnerLeaseErrorV1 AcquireFresh(
        int retained_directory_fd,
        const ProductionRouteManifestV1& active_manifest,
        std::unique_ptr<ProductionRouteOwnerLeaseV1>* output,
        std::string* diagnostic = nullptr) noexcept;

    [[nodiscard]] bool held() const noexcept {
        return descriptor_ >= 0;
    }

private:
    [[nodiscard]] static ProductionRouteOwnerLeaseErrorV1 AcquireImpl(
        int retained_directory_fd,
        const ProductionRouteManifestV1& active_manifest,
        bool fresh_only,
        std::unique_ptr<ProductionRouteOwnerLeaseV1>* output,
        std::string* diagnostic) noexcept;

    ProductionRouteOwnerLeaseV1(
        int descriptor,
        std::uint64_t device,
        std::uint64_t inode) noexcept
        : descriptor_(descriptor), device_(device), inode_(inode) {}

    int descriptor_ = -1;
    std::uint64_t device_ = 0U;
    std::uint64_t inode_ = 0U;
};

struct LiveProductionRouteReadResultV1;

// Cross-process consumer capability.  Validate immediately before and after
// a factor/input batch; a failed post-check invalidates that batch and every
// derived object tied to this route generation.  Same-process consumers use
// the composition root's directly injected history/capability instead:
// POSIX F_GETLK intentionally does not report the calling process's own lock.
class LiveProductionRouteGuardV1 final {
public:
    ~LiveProductionRouteGuardV1();

    LiveProductionRouteGuardV1(
        const LiveProductionRouteGuardV1&) = delete;
    LiveProductionRouteGuardV1& operator=(
        const LiveProductionRouteGuardV1&) = delete;
    LiveProductionRouteGuardV1(
        LiveProductionRouteGuardV1&&) = delete;
    LiveProductionRouteGuardV1& operator=(
        LiveProductionRouteGuardV1&&) = delete;

    [[nodiscard]] ProductionRouteOwnerLeaseErrorV1 Validate(
        std::string* diagnostic = nullptr) const noexcept;

private:
    class Impl;
    friend struct LiveProductionRouteReadResultV1;
    friend LiveProductionRouteReadResultV1
    ReadLiveProductionRouteV1At(
        int, std::uint64_t, std::string*) noexcept;

    explicit LiveProductionRouteGuardV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

struct LiveProductionRouteReadResultV1 final {
    ProductionRouteOwnerLeaseErrorV1 error =
        ProductionRouteOwnerLeaseErrorV1::kNone;
    ProductionRouteReadResultV1 route{};
    std::unique_ptr<LiveProductionRouteGuardV1> guard;

    [[nodiscard]] bool authoritative() const noexcept {
        return error == ProductionRouteOwnerLeaseErrorV1::kNone &&
               route.authoritative() && guard != nullptr;
    }
};

// Returns an Active route only when exact route/metadata reads, F_GETLK owner
// PID, pidfd liveness, Linux boot ID and /proc start ticks all agree. Authority
// is point-in-time, as with every liveness observation; callers must revalidate
// before committing work across a long-running boundary.
[[nodiscard]] LiveProductionRouteReadResultV1
ReadLiveProductionRouteV1At(
    int retained_directory_fd,
    std::uint64_t minimum_generation = 0U,
    std::string* diagnostic = nullptr) noexcept;

}  // namespace l2flow::route
