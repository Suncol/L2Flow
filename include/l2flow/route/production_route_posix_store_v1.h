#pragma once

#include "l2flow/route/production_route_v1.h"

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::route {

inline constexpr std::string_view kProductionRouteFilenameV1 =
    "production-route-v1.bin";
inline constexpr std::string_view kProductionRouteTemporaryFilenameV1 =
    ".production-route-v1.tmp";

enum class ProductionRouteStoreErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kUnsafeDirectory,
    kLockFailure,
    kRouteNotFound,
    kUnsafeRouteFile,
    kReadFailure,
    kManifestInvalid,
    kRouteFatal,
    kGenerationRegression,
    kGenerationConflict,
    kPreviousGenerationMismatch,
    kTemporaryConflict,
    kTemporaryCreateFailure,
    kWriteFailure,
    kFileSyncFailure,
    kRenameFailure,
    kDirectorySyncFailure,
    kReadbackFailure,
    kInjectedFailure,
    kAllocationFailure,
    kFreshArtifactPresent,
    kFreshPreparationFailure,
};

[[nodiscard]] std::string_view ProductionRouteStoreErrorNameV1(
    ProductionRouteStoreErrorV1 error) noexcept;

enum class ProductionRoutePublishDispositionV1 : std::uint8_t {
    kNone = 0U,
    kPublishedNew,
    kAdoptedCompleteTemporary,
    kAcceptedExisting,
};

enum class ProductionRouteStoreOperationV1 : std::uint8_t {
    kAfterLock = 0U,
    kAfterTemporaryOpen,
    kAfterWrite,
    kAfterFileSync,
    kBeforeRename,
    kAfterRename,
    kBeforeDirectorySync,
    kAfterDirectorySync,
    kBeforeReadback,
};

using ProductionRouteStoreOperationHookV1 = bool (*)(
    void* context,
    ProductionRouteStoreOperationV1 operation) noexcept;

struct ProductionRouteStoreOptionsV1 final {
    ProductionRouteStoreOperationHookV1 operation_hook = nullptr;
    void* operation_hook_context = nullptr;
    // Bounds both the in-process publication gate and the cross-process
    // directory lease acquisition.  A caller may retry kLockFailure; route
    // publication never waits indefinitely for another publisher.
    std::chrono::nanoseconds lock_timeout =
        std::chrono::seconds{1};
};

struct ProductionRoutePublishResultV1 final {
    ProductionRouteStoreErrorV1 error =
        ProductionRouteStoreErrorV1::kNone;
    ProductionRouteManifestErrorV1 manifest_error =
        ProductionRouteManifestErrorV1::kNone;
    ProductionRoutePublishDispositionV1 disposition =
        ProductionRoutePublishDispositionV1::kNone;
    std::uint64_t generation = 0U;
    ProductionRouteStateV1 state = ProductionRouteStateV1::kInvalid;
    l2flow::common::Sha256Digest encoded_sha256{};
    bool file_synced = false;
    bool renamed = false;
    bool directory_synced = false;

    [[nodiscard]] bool ok() const noexcept {
        return error == ProductionRouteStoreErrorV1::kNone;
    }

    [[nodiscard]] bool authoritative() const noexcept {
        return ok() && state == ProductionRouteStateV1::kActive;
    }
};

// The caller supplies a retained descriptor for a dedicated owner-only 0700
// directory.  Publication is serialized with flock and uses:
//
//   O_EXCL 0600 temp -> complete pwrite -> fsync(temp)
//       -> atomic rename over current -> fsync(retained parent) -> readback.
//
// Exact retries are idempotent, including the crash windows containing a
// complete temporary, a strict-prefix partial write, or an already-renamed
// final.  A partial temporary is repaired only when its bytes are an exact
// prefix of the requested encoding.  Conflicting temp bytes, malformed
// current bytes, a generation regression, or a predecessor mismatch fail
// closed and are never overwritten.
[[nodiscard]] ProductionRoutePublishResultV1
PublishProductionRouteManifestV1At(
    int retained_directory_fd,
    const ProductionRouteManifestV1& manifest,
    const ProductionRouteStoreOptionsV1& options = {},
    std::string* diagnostic = nullptr) noexcept;

// Invoked while both the in-process publication gate and the cross-process
// directory flock are held, after proving that neither route current nor
// route temporary exists.  The hook must not call a route-store API.  It is
// intended to publish the matching live-owner evidence before Active becomes
// visible; false aborts without creating a route temporary.
using ProductionRouteFreshPreparationHookV1 = bool (*)(
    void* context) noexcept;

// Strict first publication for production mode=fresh.  It accepts only an
// Active generation 1 manifest with previous_generation 0, rejects any route
// current/temporary artifact, prepares live-owner evidence under the same
// cross-process lock, then publishes Active.  Unlike the generic API above,
// it never adopts a temporary, accepts an existing route, or installs a
// successor.
[[nodiscard]] ProductionRoutePublishResultV1
PublishFreshProductionRouteManifestV1At(
    int retained_directory_fd,
    const ProductionRouteManifestV1& manifest,
    ProductionRouteFreshPreparationHookV1 preparation_hook,
    void* preparation_hook_context,
    const ProductionRouteStoreOptionsV1& options = {},
    std::string* diagnostic = nullptr) noexcept;

struct ProductionRouteReadResultV1 final {
    ProductionRouteStoreErrorV1 error =
        ProductionRouteStoreErrorV1::kNone;
    ProductionRouteManifestErrorV1 manifest_error =
        ProductionRouteManifestErrorV1::kNone;
    std::unique_ptr<const ProductionRouteManifestV1> manifest;
    l2flow::common::Sha256Digest encoded_sha256{};

    [[nodiscard]] bool ok() const noexcept {
        return error == ProductionRouteStoreErrorV1::kNone;
    }

    [[nodiscard]] bool authoritative() const noexcept {
        return ok() && manifest != nullptr && manifest->authoritative();
    }
};

// Loads the current route for audit.  authoritative() here means only that
// the persisted manifest is structurally Active; it is not a live-owner
// capability. Cross-process data consumers must use
// ReadLiveProductionRouteV1At(). A valid kFatal route is returned with
// kRouteFatal and a populated manifest for diagnosis.
[[nodiscard]] ProductionRouteReadResultV1
ReadProductionRouteManifestV1At(
    int retained_directory_fd,
    std::uint64_t minimum_generation = 0U,
    std::string* diagnostic = nullptr) noexcept;

}  // namespace l2flow::route
