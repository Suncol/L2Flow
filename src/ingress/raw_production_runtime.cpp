#include "l2flow/ingress/raw_production_runtime.h"

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/ingress/raw_clean_stop_gate.h"

#include <cerrno>
#include <cstddef>
#include <limits>
#include <memory>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

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

[[nodiscard]] bool HasNul(
    std::string_view value) noexcept {
    return value.find('\0') != std::string_view::npos;
}

[[nodiscard]] bool ValidRawAppPreflight(
    const RawIngressAppConfigV1& config,
    const std::shared_ptr<l2flow::sdk::SdkFactory>&
        sdk_factory,
    const std::unique_ptr<CaptureClock>& clock,
    const RawIngressAppOptionsV1& options) noexcept {
    try {
        if (sdk_factory == nullptr || clock == nullptr ||
            config.connect_generation == 0U ||
            !ValidateRawIngressConfig(
                 config.stable)
                 .empty() ||
            !ValidateRawIngressRuntimeState(
                 config.stable, config.recovered)
                 .empty() ||
            config.endpoint == nullptr ||
            config.endpoint->ingress_kind() !=
                config.stable.kind ||
            config.endpoint->contract_sha256() !=
                config.stable.endpoint_contract_sha256 ||
            config.endpoint
                ->resolved_server_address()
                .empty() ||
            HasNul(
                config.endpoint
                    ->resolved_server_address()) ||
            config.endpoint->message_encoding() ==
                datayes::mdl::MDLEID_UNDEFINED ||
            config.credential_token.empty() ||
            config.credential_token.size() >
                4096U ||
            HasNul(config.credential_token) ||
            options.callback_quiesce_timeout <=
                std::chrono::milliseconds::zero() ||
            options.emergency_writer_pause_timeout <=
                std::chrono::milliseconds::zero() ||
            options.observer_maximum_lag_bytes == 0U ||
            options.observer_heartbeat_timeout_ns ==
                0U ||
            options
                    .observer_final_catch_up_timeout_ns ==
                0U ||
            HasNul(
                options.sdk_log_runtime_prefix)) {
            return false;
        }
        static_cast<void>(
            l2flow::sdk::GetIngressSpec(
                config.stable.kind));
        return true;
    } catch (...) {
        return false;
    }
}

[[nodiscard]] bool SameCursor(
    const RawIngressRuntimeCursor& runtime,
    const RawWalCursor& wal,
    std::uint32_t segment_sequence) noexcept {
    return runtime.segment_sequence ==
               segment_sequence &&
           runtime.global_wal_pos ==
               wal.global_wal_pos &&
           runtime.ingress_sequence ==
               wal.ingress_sequence &&
           runtime.segment_offset ==
               wal.segment_offset;
}

[[nodiscard]] bool SameOpenRuntimeSnapshot(
    const RawIngressRuntimeState& runtime,
    const RawWalWriterSnapshot& snapshot,
    std::uint32_t segment_sequence) noexcept {
    return snapshot.initialized &&
           !snapshot.sealed &&
           !snapshot.closed &&
           !snapshot.fatal &&
           SameCursor(
               runtime.append,
               snapshot.append,
               segment_sequence) &&
           SameCursor(
               runtime.durable,
               snapshot.durable,
               runtime.durable.segment_sequence);
}

[[nodiscard]] bool ValidFreshRuntimePreflightImpl(
    std::string_view raw_root,
    std::string_view stream_slug,
    const RawReserveFreshScaffoldingV1& registration,
    const RawWalWriterConfig& logical_writer_config,
    const RawPosixWalStreamBackendOptionsV1&
        backend_options,
    const RawSegmentArtifactOptionsV1&
        artifact_options,
    const RawWalStreamLimitsV1& stream_limits,
    const RawIngressAppConfigV1& config,
    const std::shared_ptr<l2flow::sdk::SdkFactory>&
        sdk_factory,
    const std::unique_ptr<CaptureClock>& clock,
    const RawLiveTailPosixLimitsV1&
        live_tail_limits,
    const RawIngressAppOptionsV1& options) {
    if (raw_root.empty() || stream_slug.empty() ||
        !ValidRawAppPreflight(
            config, sdk_factory, clock, options) ||
        config.stable.raw_root != raw_root ||
        backend_options.maximum_manifest_bytes == 0U ||
        live_tail_limits.max_segments == 0U ||
        live_tail_limits.max_segments >
            kRawLiveTailPosixAbsoluteMaxSegments ||
        live_tail_limits.max_segment_bytes <
            backend_options.segment_preallocation_bytes ||
        live_tail_limits.max_journal_markers == 0U ||
        live_tail_limits
                .max_control_reattach_attempts ==
            0U ||
        stream_limits.segment_target_bytes !=
            config.stable.segment_target_bytes ||
        stream_limits.segment_max_age_ns !=
            static_cast<std::uint64_t>(
                config.stable
                    .segment_max_age_seconds) *
                UINT64_C(1'000'000'000) ||
        backend_options.segment_preallocation_bytes <
            stream_limits.segment_target_bytes ||
        artifact_options.maximum_segment_bytes <
            backend_options.segment_preallocation_bytes ||
        artifact_options.sample_record_interval !=
            config.stable
                .sparse_index_every_records ||
        artifact_options.sample_raw_bytes_interval !=
            config.stable.sparse_index_every_bytes) {
        return false;
    }

    RawRecordLayoutV1 maximum_record{};
    if (config.stable.max_message_bytes <
            kVendorMessageHeadBytes ||
        ComputeRawRecordLayoutV1(
            config.stable.max_message_bytes -
                kVendorMessageHeadBytes,
            &maximum_record) !=
            RawV1Error::kNone ||
        stream_limits.maximum_record_bytes !=
            maximum_record.record_size) {
        return false;
    }

    SegmentHeaderV1 segment{};
    if (DecodeSegmentHeaderV1(
            logical_writer_config
                .segment_header_wire,
            &segment) != RawV1Error::kNone ||
        segment.source_stream_id !=
            registration.key.route
                .source_stream_id ||
        segment.capture_date !=
            registration.key.route.capture_date ||
        segment.stream_day_id !=
            registration.key.stream_day_id ||
        segment.segment_sequence != 1U ||
        segment.segment_base_wal_pos != 0U ||
        segment.first_ingress_sequence != 1U ||
        segment.raw_schema_sha256 !=
            artifact_options
                .expected_raw_schema_sha256 ||
        logical_writer_config.writer_instance !=
            registration.writer_instance ||
        backend_options.writer_instance !=
            registration.writer_instance) {
        return false;
    }

    const RawIngressRuntimeState& runtime =
        config.recovered;
    if (runtime.source_stream_id !=
            segment.source_stream_id ||
        runtime.capture_date !=
            segment.capture_date ||
        runtime.stream_day_id !=
            segment.stream_day_id ||
        runtime.writer_instance !=
            registration.writer_instance ||
        runtime.current_segment_sequence != 1U ||
        runtime.recovered_next_ingress_sequence !=
            1U ||
        runtime.append.segment_sequence != 1U ||
        runtime.append.global_wal_pos !=
            kRawV1SegmentHeaderBytes ||
        runtime.append.ingress_sequence != 0U ||
        runtime.append.segment_offset !=
            kRawV1SegmentHeaderBytes ||
        runtime.durable.segment_sequence != 1U ||
        runtime.durable.global_wal_pos !=
            kRawV1SegmentHeaderBytes ||
        runtime.durable.ingress_sequence != 0U ||
        runtime.durable.segment_offset !=
            kRawV1SegmentHeaderBytes ||
        runtime.clock_epoch_algorithm_version !=
            segment.clock_epoch_algorithm ||
        runtime.clock_epoch_digest !=
            segment.clock_epoch_digest ||
        runtime.clock_epoch_label !=
            segment.clock_epoch_label) {
        return false;
    }

    try {
        common::Sha256Digest endpoint_digest{};
        common::Sha256Digest config_digest{};
        common::Sha256Digest raw_schema_digest{};
        std::string parse_error;
        const std::string stable_config_hash =
            RawIngressConfigSha256(config.stable);
        return common::ParseSha256Hex(
                   config.stable
                       .endpoint_contract_sha256,
                   &endpoint_digest,
                   &parse_error) &&
               common::ParseSha256Hex(
                   stable_config_hash,
                   &config_digest,
                   &parse_error) &&
               common::ParseSha256Hex(
                   config.stable.raw_schema_sha256,
                   &raw_schema_digest,
                   &parse_error) &&
               segment.endpoint_contract_sha256 ==
                   endpoint_digest &&
               segment.config_sha256 ==
                   config_digest &&
               segment.raw_schema_sha256 ==
                   raw_schema_digest;
    } catch (...) {
        return false;
    }
}

[[nodiscard]] bool ValidFreshRuntimePreflight(
    std::string_view raw_root,
    std::string_view stream_slug,
    const RawReserveFreshScaffoldingV1& registration,
    const RawWalWriterConfig& logical_writer_config,
    const RawPosixWalStreamBackendOptionsV1&
        backend_options,
    const RawSegmentArtifactOptionsV1&
        artifact_options,
    const RawWalStreamLimitsV1& stream_limits,
    const RawIngressAppConfigV1& config,
    const std::shared_ptr<l2flow::sdk::SdkFactory>&
        sdk_factory,
    const std::unique_ptr<CaptureClock>& clock,
    const RawLiveTailPosixLimitsV1&
        live_tail_limits,
    const RawIngressAppOptionsV1&
        options) noexcept {
    try {
        return ValidFreshRuntimePreflightImpl(
            raw_root,
            stream_slug,
            registration,
            logical_writer_config,
            backend_options,
            artifact_options,
            stream_limits,
            config,
            sdk_factory,
            clock,
            live_tail_limits,
            options);
    } catch (...) {
        return false;
    }
}

void CopyFreshOutcome(
    const RawFreshRoutePosixResultV1& fresh,
    RawProductionRuntimeBuildResultV1*
        output) noexcept {
    if (output == nullptr) {
        return;
    }
    output->fresh_route_failure = fresh.failure;
    output->fresh_active_failure =
        fresh.active.failure;
    output->fresh_active_publication_state =
        fresh.active.publication_state;
    output->fresh_init_publication_attempted =
        fresh.init_publication_attempted;
}

[[nodiscard]] bool SameRuntimeIdentity(
    const RawIngressRuntimeState& runtime,
    const RawWalSinkIdentityV1& sink,
    const RawLiveTailAttachV1& attach) noexcept {
    return runtime.writer_instance ==
               sink.writer_instance &&
           runtime.writer_instance ==
               attach.writer_instance &&
           runtime.stream_day_id ==
               sink.stream_day_id &&
           runtime.stream_day_id ==
               attach.stream_day_id &&
           runtime.source_stream_id ==
               sink.source_stream_id &&
           runtime.source_stream_id ==
               attach.source_stream_id &&
           runtime.capture_date ==
               sink.capture_date &&
           runtime.capture_date ==
               attach.capture_date &&
           runtime.current_segment_sequence ==
               sink.segment_sequence &&
           runtime.current_segment_sequence ==
               attach.segment_sequence &&
           runtime.recovered_next_ingress_sequence ==
               attach.next_ingress_sequence &&
           runtime.append.global_wal_pos ==
               attach.global_wal_pos &&
           runtime.append.segment_offset ==
               attach.segment_offset;
}

[[nodiscard]] bool SameLiveAttach(
    const RawLiveTailAttachV1& left,
    const RawLiveTailAttachV1& right) noexcept {
    return left.writer_instance == right.writer_instance &&
           left.stream_day_id == right.stream_day_id &&
           left.source_stream_id == right.source_stream_id &&
           left.capture_date == right.capture_date &&
           left.segment_sequence == right.segment_sequence &&
           left.global_wal_pos == right.global_wal_pos &&
           left.segment_offset == right.segment_offset &&
           left.next_ingress_sequence ==
               right.next_ingress_sequence;
}

[[nodiscard]] bool ControlMatchesAttach(
    const RawControlSnapshot& control,
    const RawLiveTailAttachV1& attach) noexcept {
    return control.fatal_state == 0U &&
           control.writer_instance == attach.writer_instance &&
           control.stream_day_id == attach.stream_day_id &&
           control.source_stream_id == attach.source_stream_id &&
           control.capture_date == attach.capture_date &&
           control.segment_sequence == attach.segment_sequence &&
           control.append_global_wal_pos == attach.global_wal_pos &&
           control.append_segment_offset == attach.segment_offset &&
           control.append_ingress_sequence !=
               std::numeric_limits<std::uint64_t>::max() &&
           control.append_ingress_sequence + 1U ==
               attach.next_ingress_sequence &&
           control.append_global_wal_pos ==
               control.durable_global_wal_pos &&
           control.append_ingress_sequence ==
               control.durable_ingress_sequence &&
           control.append_segment_offset ==
               control.durable_segment_offset &&
           control.segment_sequence != 0U &&
           control.append_segment_offset >=
               kRawV1SegmentHeaderBytes &&
           control.append_global_wal_pos >=
               control.append_segment_offset;
}

[[nodiscard]] RawProductionRuntimeFailureV1
ValidateActiveWriter(
    const RawReserveAuthorizedActionV1& action,
    RawReserveRegistryCoordinatorV1& coordinator,
    const RawIngressRuntimeState& runtime,
    const RawReserveMutationTargetProviderV1&
        target_provider) noexcept {
    if (action.required_status() !=
            ReserveRegistryStatusV1::kActive ||
        action.recovery_intent() !=
            ReserveRecoveryIntentV1::kResumeConnect ||
        action.key().route.source_stream_id !=
            runtime.source_stream_id ||
        action.key().route.capture_date !=
            runtime.capture_date ||
        action.key().stream_day_id !=
            runtime.stream_day_id ||
        !action.ValidateLatest(nullptr)) {
        return RawProductionRuntimeFailureV1::
            kActiveActionMismatch;
    }
    if (action.target() == nullptr ||
        !ValidateRawReserveMutationTargetProviderV1(
            target_provider,
            *action.target())) {
        return RawProductionRuntimeFailureV1::
            kTargetMismatch;
    }
    try {
        const ReserveCoordinatorStateV1 state =
            coordinator.state();
        if (state.selected_slot >=
            state.slots.size()) {
            return RawProductionRuntimeFailureV1::
                kActiveActionMismatch;
        }
        const ReserveStateSlotV1& slot =
            state.slots[state.selected_slot];
        if (slot.coordinator_state !=
                ReserveCoordinatorPhaseV1::kProvisioned ||
            slot.entry_count > slot.entries.size()) {
            return RawProductionRuntimeFailureV1::
                kActiveActionMismatch;
        }
        for (std::size_t index = 0U;
             index <
             static_cast<std::size_t>(slot.entry_count);
             ++index) {
            const ReserveStateEntryV1& entry =
                slot.entries[index];
            if (entry.source_stream_id ==
                    action.key().route
                        .source_stream_id &&
                entry.capture_date ==
                    action.key().route.capture_date &&
                entry.stream_day_id ==
                    action.key().stream_day_id &&
                entry.executor_or_recovery_attempt ==
                    action.key()
                        .recovery_attempt_id &&
                entry.registry_status ==
                    ReserveRegistryStatusV1::kActive) {
                return entry.writer_instance ==
                               runtime.writer_instance
                           ? RawProductionRuntimeFailureV1::
                                 kNone
                           : RawProductionRuntimeFailureV1::
                                 kWriterMismatch;
            }
        }
    } catch (...) {
    }
    return RawProductionRuntimeFailureV1::
        kActiveActionMismatch;
}

}  // namespace

RawProductionRuntimeV1::RawProductionRuntimeV1(
    std::unique_ptr<RawLiveTailPosixSource>
        live_tail_source,
    std::unique_ptr<RawIngressApp> app,
    RawLiveTailAttachV1 initial_live_attach) noexcept
    : live_tail_source_(
          std::move(live_tail_source)),
      app_(std::move(app)),
      initial_live_attach_(initial_live_attach) {}

RawProductionRuntimeV1::~RawProductionRuntimeV1() =
    default;

bool RawProductionRuntimeV1::Initialize(
    std::string* error) noexcept {
    return app_->Initialize(error);
}

bool RawProductionRuntimeV1::Stop(
    std::string* error) noexcept {
    return app_->Stop(error);
}

RawProductionReplaySnapshotResultV1
RawProductionRuntimeV1::PrepareAuthoritativeReplay(
    RawProductionReplaySnapshotLimitsV1 limits) noexcept {
    RawProductionReplaySnapshotResultV1 result{};
    if (live_tail_source_ == nullptr || app_ == nullptr ||
        app_->state() != RawIngressAppState::kConstructed ||
        app_->tail_consumer_mode() !=
            RawIngressTailConsumerModeV1::
                kExternalAuthoritativeConsumer ||
        authoritative_replay_prepared_ ||
        authoritative_tail_attached_) {
        result.error =
            RawProductionReplaySnapshotErrorV1::kInvalidState;
        return result;
    }
    if (limits.max_segments == 0U ||
        limits.max_segments >
            kRawLiveTailPosixAbsoluteMaxSegments ||
        limits.max_segment_bytes < kRawV1SegmentHeaderBytes ||
        limits.max_total_bytes < kRawV1SegmentHeaderBytes ||
        limits.max_segment_bytes >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max()) ||
        limits.max_total_bytes >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max())) {
        result.error =
            RawProductionReplaySnapshotErrorV1::kInvalidLimits;
        return result;
    }

    try {
        RawControlSnapshot first{};
        std::uint64_t first_generation = 0U;
        result.error_number = live_tail_source_->ReadControl(
            &first, &first_generation);
        if (result.error_number != 0) {
            result.error =
                RawProductionReplaySnapshotErrorV1::kControlRead;
            return result;
        }
        if (first_generation == 0U ||
            !ControlMatchesAttach(first, initial_live_attach_) ||
            first.segment_sequence > limits.max_segments) {
            result.error =
                RawProductionReplaySnapshotErrorV1::kControlInvalid;
            return result;
        }

        result.snapshot.control = first;
        result.snapshot.control_generation = first_generation;
        result.snapshot.live_attach = initial_live_attach_;
        result.snapshot.scans.reserve(
            static_cast<std::size_t>(first.segment_sequence));

        std::uint64_t total_bytes = 0U;
        std::uint64_t expected_segment_base = 0U;
        std::uint64_t expected_next_ingress_sequence = 1U;
        for (std::uint32_t sequence = 1U;
             sequence <= first.segment_sequence;
             ++sequence) {
            result.segment_sequence = sequence;
            RawLiveSegmentInfo info{};
            result.error_number =
                live_tail_source_->InspectSegment(sequence, &info);
            if (result.error_number != 0) {
                result.error =
                    RawProductionReplaySnapshotErrorV1::kSegmentInspect;
                return result;
            }
            const bool current = sequence == first.segment_sequence;
            if (info.header.source_stream_id !=
                    first.source_stream_id ||
                info.header.capture_date != first.capture_date ||
                info.header.stream_day_id != first.stream_day_id ||
                info.header.segment_sequence != sequence ||
                info.header.segment_base_wal_pos !=
                    expected_segment_base ||
                info.header.first_ingress_sequence !=
                    expected_next_ingress_sequence ||
                (!current && !info.sealed) ||
                (current && info.sealed)) {
                result.error =
                    RawProductionReplaySnapshotErrorV1::kSegmentChain;
                return result;
            }

            const std::uint64_t end_offset = current
                ? first.append_segment_offset
                : info.visible_end_offset;
            if (end_offset < kRawV1SegmentHeaderBytes ||
                end_offset > info.visible_end_offset ||
                end_offset > limits.max_segment_bytes ||
                end_offset > limits.max_total_bytes ||
                total_bytes >
                    limits.max_total_bytes - end_offset ||
                expected_segment_base >
                    std::numeric_limits<std::uint64_t>::max() -
                        end_offset) {
                result.error =
                    RawProductionReplaySnapshotErrorV1::kSegmentLimit;
                return result;
            }
            total_bytes += end_offset;
            auto bytes =
                std::make_shared<std::vector<std::byte>>(
                    static_cast<std::size_t>(end_offset));
            std::size_t offset = 0U;
            while (offset < bytes->size()) {
                const RawLiveReadResult read =
                    live_tail_source_->ReadSegmentSome(
                        sequence,
                        static_cast<std::uint64_t>(offset),
                        std::span<std::byte>(*bytes).subspan(offset));
                if (read.error_number != 0 ||
                    read.bytes_read == 0U ||
                    read.bytes_read > bytes->size() - offset) {
                    result.error =
                        RawProductionReplaySnapshotErrorV1::kSegmentRead;
                    result.error_number =
                        read.error_number == 0
                            ? ENODATA
                            : read.error_number;
                    return result;
                }
                offset += read.bytes_read;
            }

            RawSegmentScanResult scan = ScanRawSegmentV1(
                std::shared_ptr<const std::vector<std::byte>>(bytes),
                end_offset);
            if (!scan.ok()) {
                result.error =
                    RawProductionReplaySnapshotErrorV1::kSegmentScan;
                result.reader_error = scan.error;
                result.codec_error = scan.codec_error;
                return result;
            }
            if (scan.segment.source_stream_id !=
                    info.header.source_stream_id ||
                scan.segment.capture_date != info.header.capture_date ||
                scan.segment.stream_day_id != info.header.stream_day_id ||
                scan.segment.segment_sequence != sequence ||
                scan.segment.segment_base_wal_pos !=
                    expected_segment_base ||
                scan.segment.first_ingress_sequence !=
                    expected_next_ingress_sequence ||
                scan.validated_end_offset != end_offset ||
                scan.validated_end_wal_pos !=
                    expected_segment_base + end_offset) {
                result.error =
                    RawProductionReplaySnapshotErrorV1::kSegmentChain;
                return result;
            }
            if (!scan.records.empty()) {
                const std::uint64_t last =
                    scan.records.back().header().ingress_sequence;
                if (last ==
                    std::numeric_limits<std::uint64_t>::max()) {
                    result.error =
                        RawProductionReplaySnapshotErrorV1::kSegmentChain;
                    return result;
                }
                expected_next_ingress_sequence = last + 1U;
            }
            expected_segment_base = scan.validated_end_wal_pos;
            result.snapshot.scans.push_back(std::move(scan));
        }

        if (expected_segment_base != first.append_global_wal_pos ||
            expected_next_ingress_sequence !=
                first.append_ingress_sequence + 1U) {
            result.error =
                RawProductionReplaySnapshotErrorV1::kSegmentChain;
            return result;
        }

        RawControlSnapshot second{};
        std::uint64_t second_generation = 0U;
        result.error_number = live_tail_source_->ReadControl(
            &second, &second_generation);
        if (result.error_number != 0) {
            result.error =
                RawProductionReplaySnapshotErrorV1::kControlRead;
            return result;
        }
        if (second_generation != first_generation || second != first) {
            result.error =
                RawProductionReplaySnapshotErrorV1::kControlChanged;
            return result;
        }

        authoritative_live_attach_ = result.snapshot.live_attach;
        authoritative_replay_prepared_ = true;
        result.error =
            RawProductionReplaySnapshotErrorV1::kNone;
        result.error_number = 0;
        result.segment_sequence = 0U;
        return result;
    } catch (const std::bad_alloc&) {
        result.error =
            RawProductionReplaySnapshotErrorV1::kAllocationFailure;
        return result;
    } catch (...) {
        result.error =
            RawProductionReplaySnapshotErrorV1::kSegmentRead;
        result.error_number = EIO;
        return result;
    }
}

RawLiveTailError RawProductionRuntimeV1::AttachAuthoritativeTail(
    const RawLiveTailAttachV1& attach,
    std::unique_ptr<RawLiveTail>* output) noexcept {
    if (output == nullptr) {
        return RawLiveTailError::kInvalidAttach;
    }
    output->reset();
    if (live_tail_source_ == nullptr || app_ == nullptr ||
        app_->state() != RawIngressAppState::kConstructed ||
        app_->tail_consumer_mode() !=
            RawIngressTailConsumerModeV1::
                kExternalAuthoritativeConsumer ||
        !authoritative_replay_prepared_ ||
        authoritative_tail_attached_ ||
        !SameLiveAttach(attach, authoritative_live_attach_)) {
        return RawLiveTailError::kInvalidAttach;
    }
    const RawLiveTailError attach_error = RawLiveTail::Attach(
        live_tail_source_.get(), attach, output);
    if (attach_error == RawLiveTailError::kNone &&
        *output != nullptr) {
        authoritative_tail_attached_ = true;
    }
    return attach_error;
}

bool RawProductionRuntimeV1::InstallExternalTailConsumer(
    RawIngressExternalTailConsumerV1* consumer,
    std::string* error) noexcept {
    if (app_ == nullptr || !authoritative_tail_attached_) {
        SetError(error,
            "authoritative Raw tail must be attached before consumer installation");
        return false;
    }
    return app_->InstallExternalTailConsumer(consumer, error);
}

RawProductionControlSampleV1
RawProductionRuntimeV1::SampleControlFresh() const noexcept {
    RawProductionControlSampleV1 result{};
    if (live_tail_source_ == nullptr) {
        result.error_number = EINVAL;
        return result;
    }
    result.error_number = live_tail_source_->ReadControl(
        &result.snapshot, &result.generation);
    return result;
}

RawIngressAppState RawProductionRuntimeV1::app_state()
    const noexcept {
    return app_ == nullptr
        ? RawIngressAppState::kStopped
        : app_->state();
}

bool RawProductionRuntimeV1::app_fatal() const noexcept {
    return app_ == nullptr || app_->fatal();
}

std::uint64_t RawProductionRuntimeV1::connect_generation()
    const noexcept {
    return app_ == nullptr ? 0U : app_->connect_generation();
}

RawProductionReadinessSampleV1
RawProductionRuntimeV1::SampleReadiness(
    std::uint64_t now_monotonic_ns) const noexcept {
    RawProductionReadinessSampleV1 result{};
    if (live_tail_source_ == nullptr || app_ == nullptr) {
        result.control_error = EINVAL;
        return result;
    }

    RawControlSnapshot snapshot{};
    std::uint64_t generation = 0U;
    result.control_error =
        live_tail_source_->ReadControl(
            &snapshot, &generation);
    if (result.control_error != 0) {
        return result;
    }

    result.sampled_control = snapshot;
    result.control_generation = generation;
    result.gate =
        app_->EvaluateReadiness(
            snapshot, now_monotonic_ns);
    result.sampled = true;
    return result;
}

RawProductionRuntimeBuildResultV1
RawExistingRouteProductionRuntimeFactoryV1::
Assess() noexcept {
    RawProductionRuntimeBuildResultV1 result{};
    result.blocker =
        RawProductionRuntimeBlockerV1::
            kExplicitActivationInputsRequired;
    return result;
}

RawProductionRuntimeBuildResultV1
RawExistingRouteProductionRuntimeFactoryV1::
ActivateFreshRegistered(
    const std::string& raw_root,
    std::string_view stream_slug,
    const RawReserveFreshScaffoldingV1& registration,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawWalWriterConfig logical_writer_config,
    RawPosixWalStreamBackendOptionsV1 backend_options,
    RawSegmentArtifactOptionsV1 artifact_options,
    RawWalStreamLimitsV1 stream_limits,
    RawIngressAppConfigV1 config,
    std::shared_ptr<l2flow::sdk::SdkFactory>
        sdk_factory,
    std::unique_ptr<CaptureClock> clock,
    RawLiveTailPosixLimitsV1 live_tail_limits,
    RawIngressAppOptionsV1 options,
    RawIngressLifecycleObserver*
        lifecycle_observer,
    std::string* error) noexcept {
    RawProductionRuntimeBuildResultV1 result{};
    SetError(error, {});
    if (!ValidFreshRuntimePreflight(
            raw_root,
            stream_slug,
            registration,
            logical_writer_config,
            backend_options,
            artifact_options,
            stream_limits,
            config,
            sdk_factory,
            clock,
            live_tail_limits,
            options)) {
        result.failure =
            RawProductionRuntimeFailureV1::
                kInvalidInput;
        SetError(
            error,
            "invalid fresh Raw production runtime input");
        return result;
    }

    const std::size_t maximum_manifest_bytes =
        backend_options.maximum_manifest_bytes;
    RawFreshRoutePosixResultV1 fresh =
        CompleteRegisteredFreshRawRouteV1(
            raw_root,
            stream_slug,
            registration,
            coordinator,
            std::move(logical_writer_config),
            std::move(backend_options),
            std::move(artifact_options),
            std::move(stream_limits),
            error);
    CopyFreshOutcome(fresh, &result);
    result.coordinator_error =
        fresh.coordinator_failure;
    if (!fresh.ok()) {
        result.failure =
            RawProductionRuntimeFailureV1::
                kFreshRouteActivation;
        result.fail_stop_required =
            fresh.requires_fail_stop();
        return result;
    }

    // From this point the registry is durably ACTIVE. Every failure consumes
    // the sole local sink and therefore requires restart recovery/takeover.
    std::unique_ptr<RawActiveBoundWalSinkV1> sink =
        std::move(fresh.active.sink);
    if (sink == nullptr ||
        !sink->active_binding_validated() ||
        sink->authorization_key() != registration.key ||
        sink->authorized_writer_instance() !=
            registration.writer_instance) {
        result.failure =
            RawProductionRuntimeFailureV1::
                kFreshActiveIdentityMismatch;
        result.fail_stop_required = true;
        SetError(
            error,
            "fresh ACTIVE sink capability does not match its registration");
        return result;
    }

    const RawWalSinkIdentityV1 identity =
        sink->identity();
    const RawWalWriterSnapshot snapshot =
        sink->Snapshot();
    if (identity.writer_instance !=
            registration.writer_instance ||
        identity.stream_day_id !=
            registration.key.stream_day_id ||
        identity.source_stream_id !=
            registration.key.route
                .source_stream_id ||
        identity.capture_date !=
            registration.key.route.capture_date ||
        identity.segment_sequence != 1U ||
        identity.segment_base_wal_pos != 0U ||
        identity.first_ingress_sequence != 1U ||
        !snapshot.initialized || snapshot.sealed ||
        snapshot.closed || snapshot.fatal ||
        snapshot.append != snapshot.durable ||
        snapshot.append.global_wal_pos !=
            kRawV1SegmentHeaderBytes ||
        snapshot.append.ingress_sequence != 0U ||
        snapshot.append.segment_offset !=
            kRawV1SegmentHeaderBytes ||
        !SameCursor(
            config.recovered.append,
            snapshot.append,
            identity.segment_sequence) ||
        !SameCursor(
            config.recovered.durable,
            snapshot.durable,
            identity.segment_sequence)) {
        result.failure =
            RawProductionRuntimeFailureV1::
                kFreshActiveIdentityMismatch;
        result.fail_stop_required = true;
        SetError(
            error,
            "fresh ACTIVE sink does not expose the initialized header-only boundary");
        return result;
    }

    auto active_action =
        coordinator.AcquireActionForExistingRoute(
            registration.key,
            ReserveRegistryStatusV1::kActive,
            stream_slug,
            &result.coordinator_error,
            error);
    if (active_action == nullptr) {
        result.failure =
            RawProductionRuntimeFailureV1::
                kActiveActionMismatch;
        result.fail_stop_required = true;
        return result;
    }

    RawLiveTailPosixAttachGateV1 source_gate{};
    source_gate.writer_instance =
        identity.writer_instance;
    source_gate.stream_day_id =
        identity.stream_day_id;
    source_gate.recovered_durable.segment_sequence =
        identity.segment_sequence;
    source_gate.recovered_durable.global_wal_pos =
        snapshot.durable.global_wal_pos;
    source_gate.recovered_durable.ingress_sequence =
        snapshot.durable.ingress_sequence;
    source_gate.recovered_durable.segment_offset =
        snapshot.durable.segment_offset;
    source_gate.recovered_durable.marker_flags = 0U;

    std::unique_ptr<RawLiveTailPosixSource>
        live_tail_source =
            OpenRawLiveTailPosixSource(
                sink
                    ->RawReserveMutationTargetDirectoryDescriptorV1(),
                identity.source_stream_id,
                identity.capture_date,
                source_gate,
                live_tail_limits,
                error);
    if (live_tail_source == nullptr) {
        result.failure =
            RawProductionRuntimeFailureV1::
                kLiveTailSourceOpen;
        result.fail_stop_required = true;
        return result;
    }

    RawLiveTailAttachV1 live_tail_attach{};
    live_tail_attach.writer_instance =
        identity.writer_instance;
    live_tail_attach.stream_day_id =
        identity.stream_day_id;
    live_tail_attach.source_stream_id =
        identity.source_stream_id;
    live_tail_attach.capture_date =
        identity.capture_date;
    live_tail_attach.segment_sequence =
        identity.segment_sequence;
    live_tail_attach.global_wal_pos =
        snapshot.append.global_wal_pos;
    live_tail_attach.segment_offset =
        snapshot.append.segment_offset;
    live_tail_attach.next_ingress_sequence =
        snapshot.append.ingress_sequence + 1U;

    std::unique_ptr<RawPosixCleanStopGateV1>
        clean_stop_gate =
            CreateRawPosixCleanStopGateV1(
                *sink,
                coordinator,
                registration.key,
                stream_slug,
                maximum_manifest_bytes,
                error);
    if (clean_stop_gate == nullptr) {
        result.failure =
            RawProductionRuntimeFailureV1::
                kCleanStopGateConstruction;
        result.fail_stop_required = true;
        return result;
    }

    result = AdoptAlreadyActive(
        std::move(active_action),
        coordinator,
        std::move(config),
        std::move(sdk_factory),
        std::move(clock),
        std::move(sink),
        std::move(live_tail_source),
        live_tail_attach,
        std::move(clean_stop_gate),
        std::move(options),
        lifecycle_observer);
    CopyFreshOutcome(fresh, &result);
    if (!result.ok()) {
        result.fail_stop_required = true;
        SetError(
            error,
            "fresh ACTIVE resources could not be adopted by the production runtime");
    }
    return result;
}

RawProductionRuntimeBuildResultV1
RawExistingRouteProductionRuntimeFactoryV1::
ActivateRecoveredClosed(
    std::unique_ptr<
        RawRecoveredClosedPosixStreamV1>&&
        supplied_stream,
    const BuiltRecoveryMaintenanceReportV1& report,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawReserveRegistryEntryKeyV1 key,
    std::string_view stream_slug,
    RawIngressAppConfigV1 config,
    std::shared_ptr<l2flow::sdk::SdkFactory>
        sdk_factory,
    std::unique_ptr<CaptureClock> clock,
    std::unique_ptr<RawLiveTailPosixSource>
        live_tail_source,
    RawLiveTailAttachV1 live_tail_attach,
    std::unique_ptr<RawIngressCleanStopGateV1>
        clean_stop_gate,
    RawIngressAppOptionsV1 options,
    RawIngressLifecycleObserver*
        lifecycle_observer) noexcept {
    RawProductionRuntimeBuildResultV1 result{};
    auto recovering_stream = std::move(supplied_stream);
    if (recovering_stream == nullptr ||
        stream_slug.empty() ||
        live_tail_source == nullptr ||
        clean_stop_gate == nullptr ||
        !ValidRawAppPreflight(
            config,
            sdk_factory,
            clock,
            options)) {
        result.failure =
            RawProductionRuntimeFailureV1::
                kInvalidInput;
        return result;
    }

    const RawWalSinkIdentityV1 recovering_identity =
        recovering_stream->identity();
    const RawWalWriterSnapshot recovering_snapshot =
        recovering_stream->Snapshot();
    if (!SameOpenRuntimeSnapshot(
            config.recovered,
            recovering_snapshot,
            recovering_identity.segment_sequence) ||
        recovering_stream->authorization_key() != key ||
        recovering_stream
                ->authorized_writer_instance() !=
            config.recovered.writer_instance ||
        recovering_stream->active_binding_validated() ||
        report.model().recovery_attempt_id !=
            key.recovery_attempt_id ||
        report.model().namespace_identity
                .source_stream_id !=
            key.route.source_stream_id ||
        report.model().namespace_identity.capture_date !=
            key.route.capture_date ||
        report.model().namespace_identity.stream_day_id !=
            key.stream_day_id ||
        report.writer_instance() !=
            config.recovered.writer_instance ||
        report.control_cursor().segment_sequence !=
            config.recovered.current_segment_sequence ||
        report.control_cursor().global_wal_pos !=
            config.recovered.durable.global_wal_pos ||
        report.control_cursor().ingress_sequence !=
            config.recovered.durable.ingress_sequence ||
        report.control_cursor().segment_offset !=
            config.recovered.durable.segment_offset ||
        !SameRuntimeIdentity(
            config.recovered,
            recovering_identity,
            live_tail_attach)) {
        result.failure =
            RawProductionRuntimeFailureV1::
                kInvalidInput;
        return result;
    }

    // Validate the read-only live-tail attach before publishing the recovery
    // report or changing RECOVERING to ACTIVE. A second attach is still
    // required after ACTIVE promotion so a race cannot reuse this snapshot.
    std::unique_ptr<RawLiveTail> attach_probe;
    result.live_tail_error =
        RawLiveTail::Attach(
            live_tail_source.get(),
            live_tail_attach,
            &attach_probe);
    if (result.live_tail_error !=
            RawLiveTailError::kNone ||
        attach_probe == nullptr) {
        result.failure =
            RawProductionRuntimeFailureV1::
                kLiveTailAttach;
        return result;
    }
    attach_probe.reset();

    RawReserveCoordinatorErrorV1 action_failure =
        RawReserveCoordinatorErrorV1::kNone;
    auto report_action =
        coordinator.AcquireActionForExistingRoute(
            key,
            ReserveRegistryStatusV1::kRecovering,
            stream_slug,
            &action_failure,
            nullptr);
    if (report_action == nullptr) {
        result.failure =
            RawProductionRuntimeFailureV1::
                kRecoveryReportPublication;
        result.coordinator_error = action_failure;
        return result;
    }
    std::string diagnostic;
    RecoveryMaintenanceReportPublishResultV1
        publication =
            PublishRecoveryMaintenanceReportV1(
                recovering_stream->lease(),
                std::move(report_action),
                report,
                &diagnostic);
    if (!publication.ok() ||
        publication.activation_receipt == nullptr) {
        result.failure =
            RawProductionRuntimeFailureV1::
                kRecoveryReportPublication;
        result.fail_stop_required = true;
        result.recovery_report_error =
            publication.error;
        return result;
    }

    auto active_receipt =
        coordinator.PublishRecoveredActive(
            std::move(publication.activation_receipt),
            &result.coordinator_error,
            &diagnostic);
    if (active_receipt == nullptr) {
        result.failure =
            RawProductionRuntimeFailureV1::
                kCoordinatorActivation;
        result.fail_stop_required = true;
        return result;
    }
    std::unique_ptr<RawActiveBoundWalSinkV1>
        active_sink =
            PromoteRecoveredClosedRawPosixStreamToActiveV1(
                std::move(recovering_stream),
                std::move(active_receipt),
                &diagnostic);
    if (active_sink == nullptr ||
        !active_sink->active_binding_validated()) {
        result.failure =
            RawProductionRuntimeFailureV1::
                kWalActivationPromotion;
        result.fail_stop_required = true;
        return result;
    }

    auto active_action =
        coordinator.AcquireActionForExistingRoute(
            key,
            ReserveRegistryStatusV1::kActive,
            stream_slug,
            &result.coordinator_error,
            &diagnostic);
    if (active_action == nullptr) {
        result.failure =
            RawProductionRuntimeFailureV1::
                kActiveActionMismatch;
        result.fail_stop_required = true;
        return result;
    }
    return AdoptAlreadyActive(
        std::move(active_action),
        coordinator,
        std::move(config),
        std::move(sdk_factory),
        std::move(clock),
        std::move(active_sink),
        std::move(live_tail_source),
        live_tail_attach,
        std::move(clean_stop_gate),
        std::move(options),
        lifecycle_observer);
}

RawProductionRuntimeBuildResultV1
RawExistingRouteProductionRuntimeFactoryV1::
AdoptAlreadyActive(
    std::unique_ptr<RawReserveAuthorizedActionV1>
        active_action,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawIngressAppConfigV1 config,
    std::shared_ptr<l2flow::sdk::SdkFactory>
        sdk_factory,
    std::unique_ptr<CaptureClock> clock,
    std::unique_ptr<RawActiveBoundWalSinkV1>
        prepared_sink,
    std::unique_ptr<RawLiveTailPosixSource>
        live_tail_source,
    RawLiveTailAttachV1 live_tail_attach,
    std::unique_ptr<RawIngressCleanStopGateV1>
        clean_stop_gate,
    RawIngressAppOptionsV1 options,
    RawIngressLifecycleObserver*
        lifecycle_observer) noexcept {
    RawProductionRuntimeBuildResultV1 result{};
    result.fail_stop_required =
        active_action != nullptr ||
        prepared_sink != nullptr;
    if (active_action == nullptr ||
        prepared_sink == nullptr ||
        live_tail_source == nullptr ||
        clean_stop_gate == nullptr ||
        !ValidRawAppPreflight(
            config,
            sdk_factory,
            clock,
            options)) {
        result.failure =
            RawProductionRuntimeFailureV1::
                kInvalidInput;
        return result;
    }
    const RawWalSinkIdentityV1 sink_identity =
        prepared_sink->identity();
    const RawWalWriterSnapshot sink_snapshot =
        prepared_sink->Snapshot();
    if (!SameRuntimeIdentity(
            config.recovered,
            sink_identity,
            live_tail_attach) ||
        !SameOpenRuntimeSnapshot(
            config.recovered,
            sink_snapshot,
            sink_identity.segment_sequence) ||
        prepared_sink->authorization_key() !=
            active_action->key() ||
        prepared_sink
                ->authorized_writer_instance() !=
            config.recovered.writer_instance ||
        !prepared_sink->active_binding_validated()) {
        result.failure =
            RawProductionRuntimeFailureV1::
                kInvalidInput;
        return result;
    }
    result.failure = ValidateActiveWriter(
        *active_action,
        coordinator,
        config.recovered,
        *prepared_sink);
    if (result.failure !=
        RawProductionRuntimeFailureV1::kNone) {
        return result;
    }

    std::unique_ptr<RawLiveTail> live_tail;
    if (options.tail_consumer_mode ==
        RawIngressTailConsumerModeV1::
            kPhase2CompatibilityObserver) {
        result.live_tail_error =
            RawLiveTail::Attach(
                live_tail_source.get(),
                live_tail_attach,
                &live_tail);
        if (result.live_tail_error !=
                RawLiveTailError::kNone ||
            live_tail == nullptr) {
            result.failure =
                RawProductionRuntimeFailureV1::
                    kLiveTailAttach;
            return result;
        }
    } else if (options.tail_consumer_mode !=
               RawIngressTailConsumerModeV1::
                   kExternalAuthoritativeConsumer) {
        result.failure =
            RawProductionRuntimeFailureV1::kInvalidInput;
        return result;
    }

    try {
        auto app = std::make_unique<RawIngressApp>(
            std::move(config),
            std::move(sdk_factory),
            std::move(clock),
            std::move(prepared_sink),
            std::move(live_tail),
            std::move(clean_stop_gate),
            std::move(options),
            lifecycle_observer);
        result.runtime =
            std::unique_ptr<RawProductionRuntimeV1>(
                new RawProductionRuntimeV1(
                    std::move(live_tail_source),
                    std::move(app),
                    live_tail_attach));
    } catch (...) {
        result.failure =
            RawProductionRuntimeFailureV1::
                kAllocationFailure;
        return result;
    }
    result.fail_stop_required = false;
    return result;
}

}  // namespace l2flow::ingress
