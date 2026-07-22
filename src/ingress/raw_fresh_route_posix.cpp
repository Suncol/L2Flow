#include "l2flow/ingress/raw_fresh_route_posix.h"

#include "l2flow/common/identity128.h"
#include "l2flow/ingress/raw_namespace.h"
#include "l2flow/ingress/raw_writer_lease.h"

#include <string>
#include <utility>

#include <sys/stat.h>

namespace l2flow::ingress {
namespace {

void SetError(
    std::string* error,
    std::string_view message) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        error->assign(message);
    } catch (...) {
    }
}

[[nodiscard]] bool ValidInput(
    std::string_view stream_slug,
    const RawReserveFreshScaffoldingV1& registration,
    const RawWalWriterConfig& writer,
    const RawPosixWalStreamBackendOptionsV1&
        backend) noexcept {
    if (stream_slug.empty() ||
        registration.key.route.source_stream_id == 0U ||
        !IsCanonicalRawStreamRouteV1(
            registration.key.route.source_stream_id,
            stream_slug) ||
        registration.key.route.capture_date == 0U ||
        common::IsZeroIdentity(
            registration.key.stream_day_id) ||
        common::IsZeroIdentity(
            registration.key.recovery_attempt_id) ||
        common::IsZeroIdentity(
            registration.writer_instance) ||
        registration.recovery_intent !=
            ReserveRecoveryIntentV1::kResumeConnect ||
        writer.writer_instance !=
            registration.writer_instance ||
        backend.writer_instance !=
            registration.writer_instance ||
        writer.headers_already_persisted ||
        writer.initialization_mode !=
            RawWalInitializationMode::kFreshJournal) {
        return false;
    }
    SegmentHeaderV1 segment{};
    DurableJournalHeaderV1 journal{};
    return DecodeSegmentHeaderV1(
               writer.segment_header_wire,
               &segment) == RawV1Error::kNone &&
           DecodeDurableJournalHeaderV1(
               writer.journal_header_wire,
               &journal) == RawV1Error::kNone &&
           segment.source_stream_id ==
               registration.key.route.source_stream_id &&
           segment.capture_date ==
               registration.key.route.capture_date &&
           segment.stream_day_id ==
               registration.key.stream_day_id &&
           segment.segment_sequence == 1U &&
           segment.segment_base_wal_pos == 0U &&
           segment.first_ingress_sequence == 1U &&
           journal.source_stream_id ==
               segment.source_stream_id &&
           journal.capture_date == segment.capture_date &&
           journal.stream_day_id == segment.stream_day_id &&
           journal.raw_schema_sha256 ==
               segment.raw_schema_sha256;
}

[[nodiscard]] bool SameDirectory(
    int left_fd,
    int right_fd) noexcept {
    struct stat left {};
    struct stat right {};
    return left_fd >= 0 && right_fd >= 0 &&
           ::fstat(left_fd, &left) == 0 &&
           ::fstat(right_fd, &right) == 0 &&
           S_ISDIR(left.st_mode) && S_ISDIR(right.st_mode) &&
           left.st_dev == right.st_dev &&
           left.st_ino == right.st_ino;
}

[[nodiscard]] bool ExactFreshScaffoldingRegistration(
    RawReserveRegistryCoordinatorV1& coordinator,
    const RawReserveFreshScaffoldingV1& registration,
    const RawReserveAuthorizedActionV1& action) noexcept {
    try {
        const ReserveCoordinatorStateV1 state = coordinator.state();
        if (state.selected_slot >= state.slots.size()) {
            return false;
        }
        const ReserveStateSlotV1& slot =
            state.slots[state.selected_slot];
        if (slot.coordinator_state !=
                ReserveCoordinatorPhaseV1::kProvisioned ||
            slot.entry_count > slot.entries.size() ||
            slot.generation != action.token().state_generation ||
            slot.reserve_state_uuid != action.token().reserve_state_uuid ||
            action.key() != registration.key ||
            action.required_status() !=
                ReserveRegistryStatusV1::kScaffolding ||
            action.recovery_intent() != registration.recovery_intent ||
            action.token().writer_instance_id !=
                registration.writer_instance ||
            action.token().recovery_attempt_id !=
                registration.key.recovery_attempt_id) {
            return false;
        }
        std::size_t matches = 0U;
        for (std::size_t index = 0U;
             index < static_cast<std::size_t>(slot.entry_count);
             ++index) {
            const ReserveStateEntryV1& entry = slot.entries[index];
            if (entry.source_stream_id !=
                    registration.key.route.source_stream_id ||
                entry.capture_date !=
                    registration.key.route.capture_date) {
                continue;
            }
            ++matches;
            if (entry.stream_day_id != registration.key.stream_day_id ||
                entry.executor_or_recovery_attempt !=
                    registration.key.recovery_attempt_id ||
                entry.registry_status !=
                    ReserveRegistryStatusV1::kScaffolding ||
                entry.recovery_origin !=
                    ReserveRecoveryOriginV1::kFreshInit ||
                entry.recovery_intent !=
                    ReserveRecoveryIntentV1::kResumeConnect ||
                entry.recovery_intent != registration.recovery_intent ||
                entry.writer_instance != registration.writer_instance ||
                entry.grant_bytes !=
                    registration.scaffolding_allocation_cap ||
                entry.safe_stop_template_id !=
                    registration.safe_stop_template_id) {
                return false;
            }
        }
        return matches == 1U;
    } catch (...) {
        return false;
    }
}

}  // namespace

std::string_view RawFreshRoutePosixFailureV1Name(
    RawFreshRoutePosixFailureV1 failure) noexcept {
    switch (failure) {
        case RawFreshRoutePosixFailureV1::kNone:
            return "none";
        case RawFreshRoutePosixFailureV1::kInvalidInput:
            return "invalid_input";
        case RawFreshRoutePosixFailureV1::
            kScaffoldingAuthorization:
            return "scaffolding_authorization";
        case RawFreshRoutePosixFailureV1::
            kStreamDirectoryScaffold:
            return "stream_directory_scaffold";
        case RawFreshRoutePosixFailureV1::
            kMaintenanceDirectoryScaffold:
            return "maintenance_directory_scaffold";
        case RawFreshRoutePosixFailureV1::kWriterLease:
            return "writer_lease";
        case RawFreshRoutePosixFailureV1::kJournalAnchor:
            return "journal_anchor";
        case RawFreshRoutePosixFailureV1::
            kInitPublication:
            return "init_publication";
        case RawFreshRoutePosixFailureV1::kActiveStream:
            return "active_stream";
        case RawFreshRoutePosixFailureV1::
            kAllocationFailure:
            return "allocation_failure";
    }
    return "unknown";
}

namespace {

RawFreshRoutePosixResultV1
CompleteRegisteredFreshRawRouteImpl(
    int retained_raw_root_fd,
    const std::string* raw_root,
    std::string_view stream_slug,
    const RawReserveFreshScaffoldingV1& registration,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawWalWriterConfig logical_writer_config,
    RawPosixWalStreamBackendOptionsV1 backend_options,
    RawSegmentArtifactOptionsV1 artifact_options,
    RawWalStreamLimitsV1 stream_limits,
    std::string* error) noexcept {
    RawFreshRoutePosixResultV1 result{};
    SetError(error, {});
    const bool retained_authority = retained_raw_root_fd >= 0;
    if ((retained_authority && raw_root != nullptr) ||
        (!retained_authority &&
         (raw_root == nullptr || raw_root->empty())) ||
        (retained_authority &&
         !SameDirectory(
             retained_raw_root_fd,
             coordinator.raw_root_descriptor())) ||
        !ValidInput(
            stream_slug,
            registration,
            logical_writer_config,
            backend_options)) {
        result.failure =
            RawFreshRoutePosixFailureV1::kInvalidInput;
        SetError(
            error,
            "invalid registered fresh Raw route input");
        return result;
    }
    std::string owned_stream_slug;
    try {
        owned_stream_slug.assign(stream_slug);
    } catch (...) {
        result.failure =
            RawFreshRoutePosixFailureV1::
                kAllocationFailure;
        SetError(
            error,
            "cannot allocate canonical fresh Raw stream slug");
        return result;
    }

    auto scaffolding_action = coordinator.AcquireAction(
        registration.key,
        ReserveRegistryStatusV1::kScaffolding,
        &result.coordinator_failure,
        error);
    if (scaffolding_action == nullptr ||
        scaffolding_action->recovery_intent() !=
            ReserveRecoveryIntentV1::kResumeConnect ||
        scaffolding_action->token().writer_instance_id !=
            registration.writer_instance ||
        !scaffolding_action->ValidateLatest(nullptr) ||
        !ExactFreshScaffoldingRegistration(
            coordinator, registration, *scaffolding_action)) {
        result.failure =
            RawFreshRoutePosixFailureV1::
                kScaffoldingAuthorization;
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "fresh route lacks exact SCAFFOLDING writer authorization");
        }
        return result;
    }

    RawFreshStateAuthorizationV1 authorization{};
    authorization.source_stream_id =
        registration.key.route.source_stream_id;
    authorization.capture_date =
        registration.key.route.capture_date;
    authorization.stream_day_id =
        registration.key.stream_day_id;
    authorization.recovery_attempt =
        registration.key.recovery_attempt_id;
    authorization.registry_stage =
        RawFreshRegistryStageV1::kScaffolding;
    authorization.durable_state_generation =
        scaffolding_action->token().state_generation;

    std::unique_ptr<RawStreamDirectory> stream_directory;
    if (retained_authority) {
        stream_directory =
            OpenOrCreateAuthorizedFreshRawStreamDirectoryAt(
                retained_raw_root_fd,
                registration.key.route.source_stream_id,
                registration.key.route.capture_date,
                owned_stream_slug,
                authorization,
                *scaffolding_action,
                error);
    } else {
        stream_directory =
            OpenOrCreateAuthorizedFreshRawStreamDirectory(
                *raw_root,
                registration.key.route.source_stream_id,
                registration.key.route.capture_date,
                owned_stream_slug,
                authorization,
                *scaffolding_action,
                error);
    }
    if (stream_directory == nullptr) {
        result.failure =
            RawFreshRoutePosixFailureV1::
                kStreamDirectoryScaffold;
        return result;
    }
    result.stream_directory_durable = true;

    if (!CreateOrAdoptAuthorizedFreshRawMaintenanceDirectoryV1(
            *stream_directory,
            authorization,
            *scaffolding_action,
            error)) {
        result.failure =
            RawFreshRoutePosixFailureV1::
                kMaintenanceDirectoryScaffold;
        return result;
    }
    result.maintenance_directory_durable = true;

    std::unique_ptr<RawWriterLease> lease =
        AcquireRawWriterLeaseAtV1(
            stream_directory->descriptor(),
            registration.key.route.source_stream_id,
            registration.key.route.capture_date,
            registration.key.recovery_attempt_id,
            error);
    if (lease == nullptr) {
        result.failure =
            RawFreshRoutePosixFailureV1::kWriterLease;
        return result;
    }
    result.writer_lease_durable = true;

    std::unique_ptr<RawFreshJournalAnchor> anchor =
        PublishFreshRawJournalAnchor(
            *lease,
            logical_writer_config.journal_header_wire,
            authorization,
            *scaffolding_action,
            error);
    if (anchor == nullptr) {
        result.failure =
            RawFreshRoutePosixFailureV1::kJournalAnchor;
        return result;
    }
    result.journal_anchor_durable = true;

    scaffolding_action.reset();
    result.init_publication_attempted = true;
    result.coordinator_failure = coordinator.PublishInit(
        registration.key, error);
    if (result.coordinator_failure !=
        RawReserveCoordinatorErrorV1::kNone) {
        result.failure =
            RawFreshRoutePosixFailureV1::
                kInitPublication;
        return result;
    }

    result.active = CreateFreshActiveRawPosixStreamV1(
        std::move(lease),
        std::move(anchor),
        std::move(logical_writer_config),
        std::move(backend_options),
        std::move(artifact_options),
        std::move(stream_limits),
        coordinator,
        registration.key,
        stream_slug,
        error);
    if (!result.active.ok()) {
        result.failure =
            RawFreshRoutePosixFailureV1::kActiveStream;
        result.coordinator_failure =
            result.active.coordinator_failure;
        return result;
    }
    return result;
}

}  // namespace

RawFreshRoutePosixResultV1
CompleteRegisteredFreshRawRouteV1(
    const std::string& raw_root,
    std::string_view stream_slug,
    const RawReserveFreshScaffoldingV1& registration,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawWalWriterConfig logical_writer_config,
    RawPosixWalStreamBackendOptionsV1 backend_options,
    RawSegmentArtifactOptionsV1 artifact_options,
    RawWalStreamLimitsV1 stream_limits,
    std::string* error) noexcept {
    return CompleteRegisteredFreshRawRouteImpl(
        -1,
        &raw_root,
        stream_slug,
        registration,
        coordinator,
        std::move(logical_writer_config),
        std::move(backend_options),
        std::move(artifact_options),
        std::move(stream_limits),
        error);
}

RawFreshRoutePosixResultV1
CompleteRegisteredFreshRawRouteAtV1(
    int retained_raw_root_fd,
    std::string_view stream_slug,
    const RawReserveFreshScaffoldingV1& registration,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawWalWriterConfig logical_writer_config,
    RawPosixWalStreamBackendOptionsV1 backend_options,
    RawSegmentArtifactOptionsV1 artifact_options,
    RawWalStreamLimitsV1 stream_limits,
    std::string* error) noexcept {
    return CompleteRegisteredFreshRawRouteImpl(
        retained_raw_root_fd,
        nullptr,
        stream_slug,
        registration,
        coordinator,
        std::move(logical_writer_config),
        std::move(backend_options),
        std::move(artifact_options),
        std::move(stream_limits),
        error);
}

}  // namespace l2flow::ingress
