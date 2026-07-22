#include "l2flow/canonical/canonical_control_adapter_v1.h"

#include "l2flow/control/quality_flags_v1.h"

namespace l2flow::canonical {
namespace {

CanonicalControlTypeV1 ConvertType(
    l2flow::control::ControlTypeV1 type) noexcept {
    using Input = l2flow::control::ControlTypeV1;
    switch (type) {
        case Input::kConnecting:
            return CanonicalControlTypeV1::kConnecting;
        case Input::kConnectError:
            return CanonicalControlTypeV1::kConnectError;
        case Input::kDisconnected:
            return CanonicalControlTypeV1::kDisconnected;
        case Input::kLogonSuccess:
            return CanonicalControlTypeV1::kLogonSuccess;
        case Input::kLogonFailure:
            return CanonicalControlTypeV1::kLogonFailure;
        case Input::kSubscriptionAccepted:
            return CanonicalControlTypeV1::kSubscriptionAccepted;
        case Input::kSubscriptionRejected:
            return CanonicalControlTypeV1::kSubscriptionRejected;
        case Input::kServiceStatus:
            return CanonicalControlTypeV1::kServiceStatus;
        case Input::kSessionStatus:
            return CanonicalControlTypeV1::kSessionStatus;
        case Input::kDecodeError:
            return CanonicalControlTypeV1::kDecodeError;
    }
    return CanonicalControlTypeV1::kUnknown;
}

std::uint16_t ConvertFlags(std::uint32_t flags) noexcept {
    std::uint16_t result = 0U;
    if ((flags & l2flow::control::
             kControlRecordResponseManifestHashPresent) != 0U) {
        result |= CanonicalControlFlagBitV1(
            CanonicalControlFlagV1::kResponseManifestHashPresent);
    }
    if ((flags & l2flow::control::kControlRecordAddressHashPresent) != 0U) {
        result |= CanonicalControlFlagBitV1(
            CanonicalControlFlagV1::kAddressHashPresent);
    }
    if ((flags & l2flow::control::kControlRecordErrorTextHashPresent) != 0U) {
        result |= CanonicalControlFlagBitV1(
            CanonicalControlFlagV1::kErrorTextHashPresent);
    }
    if ((flags & l2flow::control::kControlRecordRequiredFailure) != 0U) {
        result |= CanonicalControlFlagBitV1(
            CanonicalControlFlagV1::kRequiredFailure);
    }
    if ((flags & l2flow::control::kControlRecordOptionalFailure) != 0U) {
        result |= CanonicalControlFlagBitV1(
            CanonicalControlFlagV1::kOptionalFailure);
    }
    return result;
}

}  // namespace

CanonicalControlAdapterErrorV1 MakeCanonicalControlRecordV1(
    const CanonicalRawContextV1& raw,
    const l2flow::control::ControlRecordV1& control,
    std::int64_t recv_realtime_ns,
    std::int64_t recv_monotonic_ns,
    std::uint64_t shard_event_id,
    CanonicalControlRecordV1* output) noexcept {
    if (output == nullptr) {
        return CanonicalControlAdapterErrorV1::kNullOutput;
    }
    if (raw.capture_date == 0U || raw.trade_date == 0U ||
        raw.source_stream_id == 0U ||
        l2flow::common::IsZeroIdentity(raw.stream_day_id) ||
        l2flow::common::IsZeroIdentity(raw.source_writer_instance) ||
        raw.source_generation == 0U ||
        raw.origin_ingress_sequence == 0U ||
        raw.origin_wal_end_pos == 0U ||
        !ClockEpochIdentityV1Valid(raw.clock_epoch) ||
        recv_realtime_ns < 0 || recv_monotonic_ns < 0 ||
        shard_event_id == 0U ||
        (raw.upstream_quality_flags &
         ~kCanonicalQualityFlagsMaskV1) != 0U) {
        return CanonicalControlAdapterErrorV1::kInvalidContext;
    }
    if (l2flow::control::ValidateControlRecordV1(control) !=
        l2flow::control::ControlRecordV1Error::kNone) {
        return CanonicalControlAdapterErrorV1::kInvalidControlRecord;
    }
    if (control.capture_date != raw.capture_date ||
        control.source_stream_id != raw.source_stream_id ||
        control.stream_day_id != raw.stream_day_id ||
        control.origin_ingress_sequence != raw.origin_ingress_sequence ||
        control.origin_record_end_wal_pos != raw.origin_wal_end_pos ||
        control.connection_epoch != raw.authoritative_connection_epoch) {
        return CanonicalControlAdapterErrorV1::kContextMismatch;
    }
    if ((control.flags &
         ~l2flow::control::kControlRecordFlagsMask) != 0U) {
        return CanonicalControlAdapterErrorV1::kUnsupportedControlFlags;
    }

    CanonicalControlRecordV1 record{};
    record.header.event_type = CanonicalEventTypeV1::kControl;
    record.header.record_size = static_cast<std::uint32_t>(
        kCanonicalControlRecordBytesV1);
    record.header.source_stream_id = raw.source_stream_id;
    record.header.connection_epoch = control.connection_epoch;
    record.header.trade_date = raw.trade_date;
    record.header.quality_flags =
        raw.upstream_quality_flags | control.quality_flags;
    record.header.shard_event_id = shard_event_id;
    record.header.origin_ingress_sequence = raw.origin_ingress_sequence;
    record.header.origin_wal_end_pos = raw.origin_wal_end_pos;
    record.header.recv_realtime_ns = recv_realtime_ns;
    record.header.recv_monotonic_ns = recv_monotonic_ns;
    record.header.origin_service_version = control.vendor_service_version;
    record.header.origin_message_id = control.vendor_message_id;
    record.header.origin_service_id = control.vendor_service_id;
    record.header.sub_index = 0U;

    record.payload.response_manifest_sha256 =
        control.response_manifest_sha256;
    record.payload.address_sha256 = control.address_sha256;
    record.payload.error_text_sha256 = control.error_text_sha256;
    // Phase-3 decode errors carry a precise decoder code in a distinct
    // field, while all other control types carry a vendor return/error code.
    // Canonical V1 projects the mutually exclusive meanings into this union.
    record.payload.return_or_error_code =
        control.control_type == l2flow::control::ControlTypeV1::kDecodeError
        ? static_cast<std::uint32_t>(control.decode_error)
        : control.return_or_error_code;
    record.payload.connection_epoch = control.connection_epoch;
    record.payload.subscription_epoch = control.subscription_epoch;
    record.payload.required_count = control.required_count;
    record.payload.required_ok_count = control.required_ok_count;
    record.payload.required_failed_count = control.required_failed_count;
    record.payload.optional_count = control.optional_count;
    record.payload.optional_ok_count = control.optional_ok_count;
    record.payload.optional_failed_count = control.optional_failed_count;
    record.payload.response_entry_count = control.response_entry_count;
    record.payload.control_type = ConvertType(control.control_type);
    record.payload.flags = ConvertFlags(control.flags);

    if (ValidateCanonicalControlRecordV1(record) !=
        CanonicalValidationErrorV1::kNone) {
        return CanonicalControlAdapterErrorV1::
            kCanonicalValidationFailure;
    }
    *output = record;
    return CanonicalControlAdapterErrorV1::kNone;
}

std::string_view CanonicalControlAdapterErrorNameV1(
    CanonicalControlAdapterErrorV1 error) noexcept {
    switch (error) {
        case CanonicalControlAdapterErrorV1::kNone:
            return "none";
        case CanonicalControlAdapterErrorV1::kNullOutput:
            return "null_output";
        case CanonicalControlAdapterErrorV1::kInvalidContext:
            return "invalid_context";
        case CanonicalControlAdapterErrorV1::kInvalidControlRecord:
            return "invalid_control_record";
        case CanonicalControlAdapterErrorV1::kContextMismatch:
            return "context_mismatch";
        case CanonicalControlAdapterErrorV1::kUnsupportedControlFlags:
            return "unsupported_control_flags";
        case CanonicalControlAdapterErrorV1::
                kCanonicalValidationFailure:
            return "canonical_validation_failure";
    }
    return "unknown";
}

}  // namespace l2flow::canonical
