#include "l2flow/route/production_route_controller_v1.h"

#include "l2flow/common/identity128.h"

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace l2flow::route {
namespace {

constexpr std::size_t kFreshIdentityAttemptLimit = 8U;

void SetDiagnostic(
    std::string* diagnostic,
    std::string_view message) noexcept {
    if (diagnostic == nullptr) {
        return;
    }
    try {
        diagnostic->assign(message.data(), message.size());
    } catch (...) {
    }
}

[[nodiscard]] int DuplicateDescriptorNoIntr(
    int descriptor,
    int* error_number) noexcept {
    if (descriptor < 0) {
        if (error_number != nullptr) {
            *error_number = EBADF;
        }
        return -1;
    }
    for (;;) {
        const int duplicate = ::fcntl(descriptor, F_DUPFD_CLOEXEC, 0);
        if (duplicate >= 0) {
            if (error_number != nullptr) {
                *error_number = 0;
            }
            return duplicate;
        }
        if (errno != EINTR) {
            if (error_number != nullptr) {
                *error_number = errno;
            }
            return -1;
        }
    }
}

[[nodiscard]] int OpenDirectoryNoIntr(
    const std::string& path,
    int* error_number) noexcept {
    if (path.empty() || path.front() != '/' ||
        path.find('\0') != std::string::npos) {
        if (error_number != nullptr) {
            *error_number = EINVAL;
        }
        return -1;
    }
    for (;;) {
        const int descriptor = ::open(
            path.c_str(),
            O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW |
                O_NONBLOCK);
        if (descriptor >= 0) {
            if (error_number != nullptr) {
                *error_number = 0;
            }
            return descriptor;
        }
        if (errno != EINTR) {
            if (error_number != nullptr) {
                *error_number = errno;
            }
            return -1;
        }
    }
}

[[nodiscard]] bool PathMatchesRetainedDirectory(
    int retained_directory_fd,
    const std::string& path,
    int* error_number) noexcept {
    if (retained_directory_fd < 0) {
        if (error_number != nullptr) {
            *error_number = EBADF;
        }
        return false;
    }
    int open_error = 0;
    const int reopened = OpenDirectoryNoIntr(path, &open_error);
    if (reopened < 0) {
        if (error_number != nullptr) {
            *error_number = open_error;
        }
        return false;
    }
    struct stat retained_status {};
    struct stat reopened_status {};
    errno = 0;
    const bool matches =
        ::fstat(retained_directory_fd, &retained_status) == 0 &&
        ::fstat(reopened, &reopened_status) == 0 &&
        S_ISDIR(retained_status.st_mode) &&
        S_ISDIR(reopened_status.st_mode) &&
        retained_status.st_uid == ::geteuid() &&
        reopened_status.st_uid == ::geteuid() &&
        (retained_status.st_mode & 07777U) == 0700U &&
        (reopened_status.st_mode & 07777U) == 0700U &&
        retained_status.st_dev == reopened_status.st_dev &&
        retained_status.st_ino == reopened_status.st_ino;
    const int stat_error = errno;
    static_cast<void>(::close(reopened));
    if (error_number != nullptr) {
        *error_number = matches
            ? 0
            : stat_error != 0 ? stat_error : ESTALE;
    }
    return matches;
}

[[nodiscard]] ProductionRouteControllerResultV1 ControllerFailure(
    ProductionRouteControllerErrorV1 error,
    std::string* diagnostic,
    std::string_view message) noexcept {
    ProductionRouteControllerResultV1 result;
    result.error = error;
    result.publish_result.error =
        ProductionRouteStoreErrorV1::kInvalidArgument;
    SetDiagnostic(diagnostic, message);
    return result;
}

[[nodiscard]] ProductionRouteControllerResultV1 StoreResult(
    ProductionRoutePublishResultV1 publish_result) noexcept {
    ProductionRouteControllerResultV1 result;
    result.publish_result = publish_result;
    if (!publish_result.ok()) {
        result.error = ProductionRouteControllerErrorV1::kStoreFailure;
    }
    return result;
}

struct FreshOwnerPreparation final {
    int retained_directory_fd = -1;
    const ProductionRouteManifestV1* manifest = nullptr;
    std::unique_ptr<ProductionRouteOwnerLeaseV1>* owner_lease = nullptr;
    int retained_canonical_directory_fd = -1;
    const std::string* route_directory_path = nullptr;
    const std::string* canonical_directory_path = nullptr;
    std::string* diagnostic = nullptr;
    int path_identity_error_number = 0;
    bool path_identity_failed = false;
    ProductionRouteOwnerLeaseErrorV1 error =
        ProductionRouteOwnerLeaseErrorV1::kNone;
};

[[nodiscard]] bool PrepareFreshOwner(void* context) noexcept {
    auto* const preparation =
        static_cast<FreshOwnerPreparation*>(context);
    if (preparation == nullptr || preparation->manifest == nullptr ||
        preparation->owner_lease == nullptr ||
        preparation->route_directory_path == nullptr ||
        preparation->canonical_directory_path == nullptr) {
        return false;
    }
    if (!PathMatchesRetainedDirectory(
            preparation->retained_directory_fd,
            *preparation->route_directory_path,
            &preparation->path_identity_error_number) ||
        !PathMatchesRetainedDirectory(
            preparation->retained_canonical_directory_fd,
            *preparation->canonical_directory_path,
            &preparation->path_identity_error_number)) {
        preparation->path_identity_failed = true;
        return false;
    }
    preparation->error = ProductionRouteOwnerLeaseV1::AcquireFresh(
        preparation->retained_directory_fd,
        *preparation->manifest,
        preparation->owner_lease,
        preparation->diagnostic);
    return preparation->error ==
               ProductionRouteOwnerLeaseErrorV1::kNone &&
           *preparation->owner_lease != nullptr &&
           (*preparation->owner_lease)->held();
}

}  // namespace

std::string_view ProductionRouteControllerErrorNameV1(
    ProductionRouteControllerErrorV1 error) noexcept {
    switch (error) {
        case ProductionRouteControllerErrorV1::kNone:
            return "none";
        case ProductionRouteControllerErrorV1::kInvalidRetainedDirectory:
            return "invalid_retained_directory";
        case ProductionRouteControllerErrorV1::kInvalidActiveManifest:
            return "invalid_active_manifest";
        case ProductionRouteControllerErrorV1::kActiveNotPublished:
            return "active_not_published";
        case ProductionRouteControllerErrorV1::kFatalAlreadyRequested:
            return "fatal_already_requested";
        case ProductionRouteControllerErrorV1::kInvalidFatalReason:
            return "invalid_fatal_reason";
        case ProductionRouteControllerErrorV1::kIdentityGenerationFailure:
            return "identity_generation_failure";
        case ProductionRouteControllerErrorV1::kAllocationFailure:
            return "allocation_failure";
        case ProductionRouteControllerErrorV1::kOwnerLeaseFailure:
            return "owner_lease_failure";
        case ProductionRouteControllerErrorV1::kStoreFailure:
            return "store_failure";
        case ProductionRouteControllerErrorV1::kPathIdentityMismatch:
            return "path_identity_mismatch";
    }
    return "unknown";
}

ProductionRouteControllerV1::ProductionRouteControllerV1(
    int retained_directory_fd,
    ProductionRouteManifestV1 active_manifest)
    : ProductionRouteControllerV1(
          retained_directory_fd,
          std::move(active_manifest),
          ProductionRouteControllerPolicyV1::kCompatible,
          ProductionRoutePathIdentityGuardV1{}) {}

ProductionRouteControllerV1::ProductionRouteControllerV1(
    int retained_directory_fd,
    ProductionRouteManifestV1 active_manifest,
    ProductionRouteControllerPolicyV1 policy,
    ProductionRoutePathIdentityGuardV1 path_identity_guard)
    : active_manifest_(std::move(active_manifest)),
      policy_(policy),
      route_directory_path_(
          std::move(path_identity_guard.route_directory_path)),
      canonical_directory_path_(
          std::move(path_identity_guard.canonical_directory_path)),
      retained_directory_fd_(DuplicateDescriptorNoIntr(
          retained_directory_fd,
          &retained_directory_error_number_)) {
    if (policy_ == ProductionRouteControllerPolicyV1::kFreshOnly) {
        retained_canonical_directory_fd_ = DuplicateDescriptorNoIntr(
            path_identity_guard.retained_canonical_directory_fd,
            &retained_canonical_directory_error_number_);
    }
}

ProductionRouteControllerV1::~ProductionRouteControllerV1() {
    if (retained_directory_fd_ >= 0) {
        // close(2) must not be retried after EINTR because the descriptor may
        // already have been released and reused by another thread.
        static_cast<void>(::close(retained_directory_fd_));
    }
    if (retained_canonical_directory_fd_ >= 0) {
        static_cast<void>(::close(retained_canonical_directory_fd_));
    }
}

ProductionRouteControllerResultV1
ProductionRouteControllerV1::PublishActive(
    const ProductionRouteStoreOptionsV1& options,
    std::string* diagnostic) noexcept {
    const std::lock_guard<std::mutex> lock(mutex_);
    if (fatal_manifest_.has_value()) {
        return ControllerFailure(
            ProductionRouteControllerErrorV1::kFatalAlreadyRequested,
            diagnostic,
            "fatal route publication has already been requested");
    }
    if (active_complete_) {
        ProductionRouteControllerResultV1 result =
            active_completed_result_;
        result.already_complete = true;
        SetDiagnostic(
            diagnostic,
            "active route publication was already completed");
        return result;
    }
    if (retained_directory_fd_ < 0) {
        ProductionRouteControllerResultV1 result = ControllerFailure(
            ProductionRouteControllerErrorV1::kInvalidRetainedDirectory,
            diagnostic,
            "retained route directory descriptor could not be duplicated");
        result.system_error_number =
            retained_directory_error_number_;
        return result;
    }

    ProductionRouteManifestErrorV1 validation =
        ValidateProductionRouteManifestV1(active_manifest_);
    if (validation == ProductionRouteManifestErrorV1::kNone &&
        active_manifest_.state != ProductionRouteStateV1::kActive) {
        validation = ProductionRouteManifestErrorV1::kInvalidState;
    }
    if (validation == ProductionRouteManifestErrorV1::kNone &&
        policy_ == ProductionRouteControllerPolicyV1::kFreshOnly &&
        (active_manifest_.generation != 1U ||
         active_manifest_.previous_generation != 0U)) {
        validation = ProductionRouteManifestErrorV1::kInvalidGeneration;
    }
    if (validation == ProductionRouteManifestErrorV1::kNone &&
        policy_ != ProductionRouteControllerPolicyV1::kCompatible &&
        policy_ != ProductionRouteControllerPolicyV1::kFreshOnly) {
        validation = ProductionRouteManifestErrorV1::kInvalidState;
    }
    if (validation != ProductionRouteManifestErrorV1::kNone) {
        ProductionRouteControllerResultV1 result = ControllerFailure(
            ProductionRouteControllerErrorV1::kInvalidActiveManifest,
            diagnostic,
            "controller active route manifest is invalid");
        result.publish_result.error =
            ProductionRouteStoreErrorV1::kManifestInvalid;
        result.publish_result.manifest_error = validation;
        result.publish_result.generation = active_manifest_.generation;
        result.publish_result.state = active_manifest_.state;
        return result;
    }

    if (policy_ == ProductionRouteControllerPolicyV1::kFreshOnly) {
        if (fresh_renamed_owner_lease_ != nullptr) {
            if (owner_lease_ == nullptr ||
                owner_lease_.get() != fresh_renamed_owner_lease_ ||
                !owner_lease_->held()) {
                ProductionRouteControllerResultV1 result =
                    ControllerFailure(
                        ProductionRouteControllerErrorV1::
                            kOwnerLeaseFailure,
                        diagnostic,
                        "fresh route owner lease was lost before active "
                        "publication convergence");
                result.owner_lease_error =
                    ProductionRouteOwnerLeaseErrorV1::kInvalidLease;
                return result;
            }

            ProductionRouteControllerResultV1 result = StoreResult(
                PublishProductionRouteManifestV1At(
                    retained_directory_fd_, active_manifest_, options,
                    diagnostic));
            if (result.ok()) {
                active_complete_ = true;
                active_completed_result_ = result;
                fresh_renamed_owner_lease_ = nullptr;
            }
            return result;
        }

        FreshOwnerPreparation preparation{
            retained_directory_fd_, &active_manifest_, &owner_lease_,
            retained_canonical_directory_fd_, &route_directory_path_,
            &canonical_directory_path_, diagnostic};
        ProductionRouteControllerResultV1 result = StoreResult(
            PublishFreshProductionRouteManifestV1At(
                retained_directory_fd_, active_manifest_,
                &PrepareFreshOwner, &preparation, options,
                diagnostic));
        if (preparation.path_identity_failed) {
            result.error = ProductionRouteControllerErrorV1::
                kPathIdentityMismatch;
            result.system_error_number =
                preparation.path_identity_error_number != 0
                ? preparation.path_identity_error_number
                : retained_canonical_directory_error_number_;
            SetDiagnostic(
                diagnostic,
                "production route/canonical pathname identity changed");
        } else if (preparation.error !=
            ProductionRouteOwnerLeaseErrorV1::kNone) {
            result.error =
                ProductionRouteControllerErrorV1::kOwnerLeaseFailure;
            result.owner_lease_error = preparation.error;
        }
        if (!result.ok() && result.publish_result.renamed &&
            owner_lease_ != nullptr && owner_lease_->held()) {
            fresh_renamed_owner_lease_ = owner_lease_.get();
        }
        if (result.ok()) {
            active_complete_ = true;
            active_completed_result_ = result;
            fresh_renamed_owner_lease_ = nullptr;
        }
        return result;
    }

    if (owner_lease_ == nullptr) {
        const ProductionRouteOwnerLeaseErrorV1 lease_error =
            ProductionRouteOwnerLeaseV1::Acquire(
                retained_directory_fd_,
                active_manifest_,
                &owner_lease_,
                diagnostic);
        if (lease_error != ProductionRouteOwnerLeaseErrorV1::kNone ||
            owner_lease_ == nullptr || !owner_lease_->held()) {
            ProductionRouteControllerResultV1 result = ControllerFailure(
                ProductionRouteControllerErrorV1::kOwnerLeaseFailure,
                diagnostic,
                "active route owner lease could not be acquired");
            result.owner_lease_error = lease_error;
            return result;
        }
    }

    ProductionRouteControllerResultV1 result = StoreResult(
        PublishProductionRouteManifestV1At(
            retained_directory_fd_, active_manifest_, options,
            diagnostic));
    if (result.ok()) {
        active_complete_ = true;
        active_completed_result_ = result;
    }
    return result;
}

ProductionRouteControllerResultV1
ProductionRouteControllerV1::PublishFatal(
    std::uint32_t reason_code,
    const ProductionRouteStoreOptionsV1& options,
    std::string* diagnostic) noexcept {
    const std::lock_guard<std::mutex> lock(mutex_);
    if (!active_complete_) {
        return ControllerFailure(
            ProductionRouteControllerErrorV1::kActiveNotPublished,
            diagnostic,
            "active route publication has not completed");
    }
    if (reason_code == 0U) {
        return ControllerFailure(
            ProductionRouteControllerErrorV1::kInvalidFatalReason,
            diagnostic,
            "fatal route reason code must be nonzero");
    }
    if (fatal_manifest_.has_value() &&
        fatal_manifest_->fatal_reason_code != reason_code) {
        return ControllerFailure(
            ProductionRouteControllerErrorV1::kFatalAlreadyRequested,
            diagnostic,
            "a different first fatal route reason is already latched");
    }
    if (fatal_complete_) {
        ProductionRouteControllerResultV1 result =
            fatal_completed_result_;
        result.already_complete = true;
        SetDiagnostic(
            diagnostic,
            "fatal route publication was already completed");
        return result;
    }

    if (!fatal_manifest_.has_value()) {
        ProductionRouteManifestV1 candidate;
        try {
            candidate = active_manifest_;
        } catch (...) {
            return ControllerFailure(
                ProductionRouteControllerErrorV1::kAllocationFailure,
                diagnostic,
                "fatal route candidate allocation failed");
        }
        candidate.state = ProductionRouteStateV1::kFatal;
        candidate.previous_generation = active_manifest_.generation;
        candidate.generation = active_manifest_.generation + 1U;
        candidate.fatal_reason_code = reason_code;

        bool fresh_identity = false;
        int identity_error = 0;
        for (std::size_t attempt = 0U;
             attempt < kFreshIdentityAttemptLimit;
             ++attempt) {
            l2flow::common::Identity128 identity{};
            if (!l2flow::common::GenerateIdentity128(
                    &identity, &identity_error)) {
                ProductionRouteControllerResultV1 result =
                    ControllerFailure(
                        ProductionRouteControllerErrorV1::
                            kIdentityGenerationFailure,
                        diagnostic,
                        "secure fatal route identity generation failed");
                result.system_error_number = identity_error;
                return result;
            }
            if (identity != active_manifest_.route_instance) {
                candidate.route_instance = identity;
                fresh_identity = true;
                break;
            }
        }
        if (!fresh_identity) {
            ProductionRouteControllerResultV1 result =
                ControllerFailure(
                    ProductionRouteControllerErrorV1::
                        kIdentityGenerationFailure,
                    diagnostic,
                    "secure generator repeatedly returned the active route identity");
            result.system_error_number = EEXIST;
            return result;
        }
        try {
            fatal_manifest_.emplace(std::move(candidate));
        } catch (...) {
            return ControllerFailure(
                ProductionRouteControllerErrorV1::kAllocationFailure,
                diagnostic,
                "fatal route candidate allocation failed");
        }
    }

    ProductionRouteControllerResultV1 result = StoreResult(
        PublishProductionRouteManifestV1At(
            retained_directory_fd_, *fatal_manifest_, options,
            diagnostic));
    if (result.ok()) {
        fatal_complete_ = true;
        fatal_completed_result_ = result;
    }
    return result;
}

}  // namespace l2flow::route
