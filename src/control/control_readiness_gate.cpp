#include "l2flow/control/control_readiness_gate.h"

#include "l2flow/common/identity128.h"
#include "l2flow/control/raw_frontier_v1.h"
#include "l2flow/ingress/raw_schema.h"

#include <algorithm>
#include <cstddef>

namespace l2flow::control {
namespace {

bool DigestIsZero(
    const l2flow::common::Sha256Digest& digest) noexcept {
    return std::all_of(
        digest.begin(), digest.end(), [](std::byte value) {
            return value == std::byte{0};
        });
}

bool CalendarProofValid(
    const MarketSilenceProofV1& proof,
    const ControlDecoderSnapshotV1& decoder,
    const ControlReadinessGateConfigV1& config,
    std::uint64_t now_realtime_ns) noexcept {
    return proof.schema_version ==
               kMarketSilenceProofSchemaVersionV1 &&
           proof.source_stream_id == decoder.source_stream_id &&
           proof.capture_date == decoder.capture_date &&
           proof.stream_day_id == decoder.stream_day_id &&
           !DigestIsZero(config.approved_calendar_sha256) &&
           proof.calendar_sha256 ==
               config.approved_calendar_sha256 &&
           proof.valid_from_realtime_ns != 0U &&
           proof.valid_until_realtime_ns >
               proof.valid_from_realtime_ns &&
           proof.evaluated_realtime_ns >=
               proof.valid_from_realtime_ns &&
           proof.evaluated_realtime_ns <
               proof.valid_until_realtime_ns &&
           now_realtime_ns >= proof.evaluated_realtime_ns &&
           now_realtime_ns < proof.valid_until_realtime_ns &&
           proof.required_market_records_not_expected;
}

}  // namespace

ControlReadinessResultV1 EvaluateControlReadinessV1(
    const ControlReadinessGateConfigV1& config,
    const ControlDecoderSnapshotV1& decoder,
    const ControlReadinessRuntimeV1& runtime,
    const l2flow::ingress::RawControlSnapshot& sampled_raw,
    const std::optional<MarketSilenceProofV1>& calendar_proof,
    std::uint64_t now_monotonic_ns) noexcept {
    ControlReadinessResultV1 result;
    result.connection_epoch = decoder.connection_epoch;
    result.subscription_epoch = decoder.subscription_epoch;
    result.sampled_append_wal_pos =
        sampled_raw.append_global_wal_pos;
    result.decoder_processed_wal_pos =
        runtime.processed_wal_pos;

    if (config.maximum_decoder_lag_bytes == 0U ||
        config.maximum_durability_lag_bytes == 0U ||
        config.heartbeat_timeout_ns == 0U ||
        l2flow::common::IsZeroIdentity(runtime.writer_instance)) {
        result.reason = ControlReadinessReasonV1::kInvalidConfig;
        return result;
    }
    if (sampled_raw.writer_instance != runtime.writer_instance) {
        result.reason =
            ControlReadinessReasonV1::kWriterInstanceMismatch;
        return result;
    }
    if (sampled_raw.source_stream_id != decoder.source_stream_id ||
        sampled_raw.capture_date != decoder.capture_date ||
        sampled_raw.stream_day_id != decoder.stream_day_id) {
        result.reason = ControlReadinessReasonV1::kNamespaceMismatch;
        return result;
    }
    if (!RawFrontierCursorShapeValidV1(sampled_raw)) {
        result.reason = ControlReadinessReasonV1::kRawFrontierInvalid;
        return result;
    }
    if ((runtime.processed_wal_pos %
         l2flow::ingress::kRawV1RecordAlignment) != 0U ||
        runtime.processed_wal_pos <
            decoder.processed_record_end_wal_pos ||
        runtime.processed_ingress_sequence !=
            decoder.processed_ingress_sequence) {
        result.reason =
            ControlReadinessReasonV1::kDecoderCursorInvalid;
        return result;
    }
    if (sampled_raw.fatal_state != 0U) {
        result.reason = ControlReadinessReasonV1::kWriterFatal;
        return result;
    }
    if (!runtime.capture_pipeline_healthy) {
        result.reason =
            ControlReadinessReasonV1::kCapturePipelineUnhealthy;
        return result;
    }
    if (sampled_raw.heartbeat_monotonic_ns == 0U) {
        result.reason =
            ControlReadinessReasonV1::kWriterHeartbeatMissing;
        return result;
    }
    if (now_monotonic_ns < sampled_raw.heartbeat_monotonic_ns) {
        result.reason = ControlReadinessReasonV1::
            kWriterHeartbeatClockRegression;
        return result;
    }
    result.writer_heartbeat_age_ns =
        now_monotonic_ns - sampled_raw.heartbeat_monotonic_ns;
    if (result.writer_heartbeat_age_ns > config.heartbeat_timeout_ns) {
        result.reason =
            ControlReadinessReasonV1::kWriterHeartbeatTimedOut;
        return result;
    }
    if (runtime.decoder_heartbeat_monotonic_ns == 0U) {
        result.reason =
            ControlReadinessReasonV1::kDecoderHeartbeatMissing;
        return result;
    }
    if (now_monotonic_ns <
        runtime.decoder_heartbeat_monotonic_ns) {
        result.reason = ControlReadinessReasonV1::
            kDecoderHeartbeatClockRegression;
        return result;
    }
    result.decoder_heartbeat_age_ns =
        now_monotonic_ns -
        runtime.decoder_heartbeat_monotonic_ns;
    if (result.decoder_heartbeat_age_ns >
        config.heartbeat_timeout_ns) {
        result.reason =
            ControlReadinessReasonV1::kDecoderHeartbeatTimedOut;
        return result;
    }
    if (!runtime.decoder_healthy) {
        result.reason = ControlReadinessReasonV1::kDecoderUnhealthy;
        return result;
    }
    if (runtime.connect_generation == 0U ||
        runtime.generation_first_ingress_sequence == 0U ||
        runtime.generation_first_ingress_sequence >
            decoder.next_ingress_sequence) {
        result.reason =
            ControlReadinessReasonV1::kLiveGenerationInvalid;
        return result;
    }
    if (runtime.successful_logon_connect_generation !=
            runtime.connect_generation ||
        runtime.successful_logon_ingress_sequence <
            runtime.generation_first_ingress_sequence ||
        runtime.successful_logon_ingress_sequence >
            decoder.processed_ingress_sequence) {
        result.reason = ControlReadinessReasonV1::
            kCurrentGenerationLogonMissing;
        return result;
    }
    if (decoder.poisoned) {
        result.reason = ControlReadinessReasonV1::kControlPoisoned;
        return result;
    }
    if (!decoder.control_ready) {
        result.reason =
            ControlReadinessReasonV1::kControlEvidenceIncomplete;
        return result;
    }
    if (!decoder.decoder_evidence_ready) {
        if (!calendar_proof.has_value()) {
            result.reason =
                ControlReadinessReasonV1::kMarketEvidenceIncomplete;
            return result;
        }
        if (!CalendarProofValid(
                *calendar_proof,
                decoder,
                config,
                runtime.sampled_realtime_ns)) {
            result.reason =
                ControlReadinessReasonV1::kCalendarProofInvalid;
            return result;
        }
        result.calendar_exception_used = true;
    }

    result.durability_lag_bytes =
        sampled_raw.append_global_wal_pos -
        sampled_raw.durable_global_wal_pos;
    if (result.durability_lag_bytes >
        config.maximum_durability_lag_bytes) {
        result.reason =
            ControlReadinessReasonV1::kDurabilityLagExceeded;
        return result;
    }
    const bool raw_wal_ahead =
        sampled_raw.append_global_wal_pos >
        runtime.processed_wal_pos;
    const bool raw_sequence_ahead =
        sampled_raw.append_ingress_sequence >
        runtime.processed_ingress_sequence;
    const bool decoder_wal_ahead =
        runtime.processed_wal_pos >
        sampled_raw.append_global_wal_pos;
    const bool decoder_sequence_ahead =
        runtime.processed_ingress_sequence >
        sampled_raw.append_ingress_sequence;
    const bool raw_advance_valid =
        !decoder_wal_ahead && !decoder_sequence_ahead &&
        RawCursorAdvancePlausibleV1(
            runtime.processed_wal_pos,
            runtime.processed_ingress_sequence,
            sampled_raw.append_global_wal_pos,
            sampled_raw.append_ingress_sequence);
    const bool decoder_advance_valid =
        !raw_wal_ahead && !raw_sequence_ahead &&
        RawCursorAdvancePlausibleV1(
            sampled_raw.append_global_wal_pos,
            sampled_raw.append_ingress_sequence,
            runtime.processed_wal_pos,
            runtime.processed_ingress_sequence);
    // Raw is sampled before the worker state mutex is acquired. The same
    // writer/decoder may therefore advance in that interval. A plausible
    // decoder advance proves that it processed at least the sampled boundary;
    // crossed or arithmetically impossible cursors remain fail-closed.
    if (!raw_advance_valid && !decoder_advance_valid) {
        result.reason =
            ControlReadinessReasonV1::kDecoderCursorInvalid;
        return result;
    }
    if (raw_wal_ahead || raw_sequence_ahead) {
        result.decoder_lag_bytes =
            sampled_raw.append_global_wal_pos -
            runtime.processed_wal_pos;
        result.reason =
            result.decoder_lag_bytes >
                    config.maximum_decoder_lag_bytes
                ? ControlReadinessReasonV1::kDecoderLagExceeded
                : ControlReadinessReasonV1::kDecoderNotCaughtUp;
        return result;
    }

    result.reason = ControlReadinessReasonV1::kReady;
    result.ready = true;
    return result;
}

}  // namespace l2flow::control
