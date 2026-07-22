#pragma once

#include "l2flow/route/production_route_owner_lease_v1.h"

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

namespace l2flow::route {

enum class ProductionRouteControllerErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidRetainedDirectory,
    kInvalidActiveManifest,
    kActiveNotPublished,
    kFatalAlreadyRequested,
    kInvalidFatalReason,
    kIdentityGenerationFailure,
    kAllocationFailure,
    kOwnerLeaseFailure,
    kStoreFailure,
    kPathIdentityMismatch,
};

[[nodiscard]] std::string_view ProductionRouteControllerErrorNameV1(
    ProductionRouteControllerErrorV1 error) noexcept;

enum class ProductionRouteControllerPolicyV1 : std::uint8_t {
    // Preserves the generic successor/idempotent store behavior.
    kCompatible = 0U,
    // Requires a pristine route/owner namespace and generation 1/previous 0.
    kFreshOnly,
};

// Production-only publication fence.  PublishActive safely reopens both
// absolute paths under the fresh publication lock and compares their device
// and inode identities with these retained descriptors before owner evidence
// can be created.  The canonical descriptor is duplicated by the controller;
// the route descriptor is the constructor's first argument.
struct ProductionRoutePathIdentityGuardV1 final {
    int retained_canonical_directory_fd = -1;
    std::string route_directory_path;
    std::string canonical_directory_path;
};

struct ProductionRouteControllerResultV1 final {
    ProductionRouteControllerErrorV1 error =
        ProductionRouteControllerErrorV1::kNone;
    ProductionRoutePublishResultV1 publish_result{};
    ProductionRouteOwnerLeaseErrorV1 owner_lease_error =
        ProductionRouteOwnerLeaseErrorV1::kNone;
    // errno-style detail for descriptor or secure-identity failures.
    int system_error_number = 0;
    // True means this controller had already completed the exact operation;
    // no filesystem publication was attempted by this call.
    bool already_complete = false;

    [[nodiscard]] bool ok() const noexcept {
        return error == ProductionRouteControllerErrorV1::kNone &&
               publish_result.ok() &&
               publish_result.disposition !=
                   ProductionRoutePublishDispositionV1::kNone;
    }

    [[nodiscard]] bool authoritative() const noexcept {
        return ok() && publish_result.authoritative();
    }
};

// Process-local lifecycle owner for one immutable active route and, at most,
// one fatal successor.  The constructor duplicates retained_directory_fd with
// CLOEXEC, so the caller may close its descriptor after construction.
//
// PublishActive publishes only the constructor-supplied active manifest.  A
// kFreshOnly controller atomically proves the route and owner artifacts absent
// before publishing new owner evidence and Active.  Once publication succeeds,
// exact repeat calls are answered from controller state.  If that controller's
// fresh publication reports that rename completed but a later commit check
// failed, a retry may converge through the compatible store only while the
// exact owner lease acquired by that fresh attempt is still held.  Compatible
// convergence accepts only the exact encoded Active manifest; a current that
// predates that fresh attempt, or whose bytes conflict, is never adopted.
// PublishFatal is first-reason-wins: it creates generation+1 with the active
// generation as predecessor and a cryptographically generated new route
// identity.  The candidate is retained before publication so retries through
// uncertain POSIX commit windows always use exactly the same bytes.
class ProductionRouteControllerV1 final {
public:
    ProductionRouteControllerV1(
        int retained_directory_fd,
        ProductionRouteManifestV1 active_manifest);
    ProductionRouteControllerV1(
        int retained_directory_fd,
        ProductionRouteManifestV1 active_manifest,
        ProductionRouteControllerPolicyV1 policy,
        ProductionRoutePathIdentityGuardV1 path_identity_guard);
    ~ProductionRouteControllerV1();

    ProductionRouteControllerV1(
        const ProductionRouteControllerV1&) = delete;
    ProductionRouteControllerV1& operator=(
        const ProductionRouteControllerV1&) = delete;
    ProductionRouteControllerV1(
        ProductionRouteControllerV1&&) = delete;
    ProductionRouteControllerV1& operator=(
        ProductionRouteControllerV1&&) = delete;

    [[nodiscard]] ProductionRouteControllerResultV1 PublishActive(
        const ProductionRouteStoreOptionsV1& options = {},
        std::string* diagnostic = nullptr) noexcept;

    [[nodiscard]] ProductionRouteControllerResultV1 PublishFatal(
        std::uint32_t reason_code,
        const ProductionRouteStoreOptionsV1& options = {},
        std::string* diagnostic = nullptr) noexcept;

    // The active manifest is immutable after construction.  This reference
    // is safe for concurrent reads and remains valid until this controller is
    // destroyed; callers must not retain it beyond that lifetime.
    [[nodiscard]] const ProductionRouteManifestV1& active_manifest()
        const noexcept {
        return active_manifest_;
    }

private:
    ProductionRouteManifestV1 active_manifest_{};
    ProductionRouteControllerPolicyV1 policy_ =
        ProductionRouteControllerPolicyV1::kCompatible;
    std::string route_directory_path_;
    std::string canonical_directory_path_;
    int retained_directory_error_number_ = 0;
    int retained_directory_fd_ = -1;
    int retained_canonical_directory_error_number_ = 0;
    int retained_canonical_directory_fd_ = -1;
    std::mutex mutex_;
    bool active_complete_ = false;
    bool fatal_complete_ = false;
    std::optional<ProductionRouteManifestV1> fatal_manifest_;
    ProductionRouteControllerResultV1 active_completed_result_{};
    ProductionRouteControllerResultV1 fatal_completed_result_{};
    std::unique_ptr<ProductionRouteOwnerLeaseV1> owner_lease_;
    ProductionRouteOwnerLeaseV1* fresh_renamed_owner_lease_ = nullptr;
};

}  // namespace l2flow::route
