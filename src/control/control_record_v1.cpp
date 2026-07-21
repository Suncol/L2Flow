#include "l2flow/control/control_record_v1.h"

#include "l2flow/common/crc32c.h"
#include "l2flow/control/quality_flags_v1.h"
#include "l2flow/ingress/raw_v1.h"

#include "mdl_api_msg.h"
#include "mdl_api_types.h"
#include "mdl_sys_msg.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>

namespace l2flow::control {
namespace {

namespace api = datayes::mdl::mdl_api_msg;
namespace sys = datayes::mdl::mdl_sys_msg;

static_assert(datayes::mdl::MDLSID_MDL_API == 1U);
static_assert(datayes::mdl::MDLSID_MDL_SYS == 2U);
static_assert(api::MDLVID_MDL_API == 101U);
static_assert(sys::MDLVID_MDL_SYS == 101U);
static_assert(datayes::mdl::MDLEC_OK == 0U);

void StoreU16(
    std::uint16_t value,
    std::span<std::byte> output,
    std::size_t offset) noexcept {
    output[offset] =
        static_cast<std::byte>(value & 0xffU);
    output[offset + 1U] =
        static_cast<std::byte>((value >> 8U) & 0xffU);
}

void StoreU32(
    std::uint32_t value,
    std::span<std::byte> output,
    std::size_t offset) noexcept {
    for (std::size_t index = 0U; index < 4U; ++index) {
        output[offset + index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
}

void StoreU64(
    std::uint64_t value,
    std::span<std::byte> output,
    std::size_t offset) noexcept {
    for (std::size_t index = 0U; index < 8U; ++index) {
        output[offset + index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
}

std::uint16_t LoadU16(
    std::span<const std::byte> input,
    std::size_t offset) noexcept {
    return static_cast<std::uint16_t>(
        std::to_integer<std::uint16_t>(input[offset]) |
        static_cast<std::uint16_t>(
            std::to_integer<std::uint16_t>(
                input[offset + 1U])
            << 8U));
}

std::uint32_t LoadU32(
    std::span<const std::byte> input,
    std::size_t offset) noexcept {
    std::uint32_t value = 0U;
    for (std::size_t index = 0U; index < 4U; ++index) {
        value |= std::to_integer<std::uint32_t>(
                     input[offset + index])
                 << (index * 8U);
    }
    return value;
}

std::uint64_t LoadU64(
    std::span<const std::byte> input,
    std::size_t offset) noexcept {
    std::uint64_t value = 0U;
    for (std::size_t index = 0U; index < 8U; ++index) {
        value |= std::to_integer<std::uint64_t>(
                     input[offset + index])
                 << (index * 8U);
    }
    return value;
}

bool IsKnownControlType(ControlTypeV1 type) noexcept {
    switch (type) {
        case ControlTypeV1::kConnecting:
        case ControlTypeV1::kConnectError:
        case ControlTypeV1::kDisconnected:
        case ControlTypeV1::kLogonSuccess:
        case ControlTypeV1::kLogonFailure:
        case ControlTypeV1::kSubscriptionAccepted:
        case ControlTypeV1::kSubscriptionRejected:
        case ControlTypeV1::kServiceStatus:
        case ControlTypeV1::kSessionStatus:
        case ControlTypeV1::kDecodeError:
            return true;
    }
    return false;
}

bool CountsValid(
    std::uint32_t total,
    std::uint32_t ok,
    std::uint32_t failed) noexcept {
    return ok <= total && failed <= total - ok;
}

void StoreDigest(
    const ControlDigestV1& digest,
    std::span<std::byte> output,
    std::size_t offset) noexcept {
    std::copy(
        digest.begin(), digest.end(), output.begin() + offset);
}

ControlDigestV1 LoadDigest(
    std::span<const std::byte> input,
    std::size_t offset) noexcept {
    ControlDigestV1 digest{};
    std::copy_n(
        input.begin() + offset,
        digest.size(),
        digest.begin());
    return digest;
}

bool IsZeroRange(
    std::span<const std::byte> input,
    std::size_t offset,
    std::size_t size) noexcept {
    return std::all_of(
        input.begin() + offset,
        input.begin() + offset + size,
        [](std::byte value) { return value == std::byte{0}; });
}

bool DigestIsZero(const ControlDigestV1& digest) noexcept {
    return std::all_of(
        digest.begin(), digest.end(), [](std::byte value) {
            return value == std::byte{0};
        });
}

bool DecodeErrorIdentityValid(
    const ControlRecordV1& record) noexcept {
    std::uint16_t first_error = 0U;
    std::uint16_t last_error = 0U;
    std::uint16_t supported_version = 0U;
    if (record.vendor_service_id ==
        datayes::mdl::MDLSID_MDL_API) {
        const bool recognized_message =
            record.vendor_message_id ==
                api::MDLMID_MDL_API_ConnectingEvent ||
            record.vendor_message_id ==
                api::MDLMID_MDL_API_ConnectErrorEvent ||
            record.vendor_message_id ==
                api::MDLMID_MDL_API_DisconnectedEvent;
        if (!recognized_message) {
            return false;
        }
        first_error = 0x0103U;
        last_error = 0x0106U;
        supported_version = api::MDLVID_MDL_API;
    } else {
        if (record.vendor_service_id !=
            datayes::mdl::MDLSID_MDL_SYS) {
            return false;
        }
        const bool recognized_message =
            record.vendor_message_id ==
                sys::MDLMID_MDL_SYS_LogonResponse ||
            record.vendor_message_id ==
                sys::MDLMID_MDL_SYS_SubscribeResponse ||
            record.vendor_message_id ==
                sys::MDLMID_MDL_SYS_ServiceStatus ||
            record.vendor_message_id ==
                sys::MDLMID_MDL_SYS_SessionStatus;
        if (!recognized_message) {
            return false;
        }
        first_error = 0x0203U;
        last_error = 0x0208U;
        supported_version = sys::MDLVID_MDL_SYS;
    }
    if (record.decode_error < first_error ||
        record.decode_error > last_error) {
        return false;
    }
    const std::uint16_t local_error =
        static_cast<std::uint16_t>(record.decode_error & 0x00ffU);
    const std::uint64_t required_quality =
        local_error == 3U
            ? QualityBit(QualityFlagV1::kSchemaUnknown)
            : (local_error == 4U
                   ? QualityBit(QualityFlagV1::kDecodeTruncated)
                   : QualityBit(
                         QualityFlagV1::kDecodeOffsetInvalid));
    const bool version_reachable =
        local_error == 3U
            ? record.vendor_service_version != supported_version
            : record.vendor_service_version == supported_version;
    return version_reachable &&
           (record.quality_flags & required_quality) != 0U &&
           (record.flags &
            kControlRecordNoncanonicalEmptyOffset) == 0U;
}

bool TypeIdentityAndPayloadValid(
    const ControlRecordV1& record,
    bool response_present,
    bool address_present,
    bool error_present) noexcept {
    const bool no_response =
        !response_present && record.response_entry_count == 0U;
    const bool no_address_or_error =
        !address_present && !error_present;
    switch (record.control_type) {
        case ControlTypeV1::kConnecting:
            return record.vendor_service_id ==
                       datayes::mdl::MDLSID_MDL_API &&
                   record.vendor_service_version ==
                       api::MDLVID_MDL_API &&
                   record.vendor_message_id ==
                       api::MDLMID_MDL_API_ConnectingEvent &&
                   record.return_or_error_code == 0U &&
                   no_response && address_present && !error_present;
        case ControlTypeV1::kConnectError:
            return record.vendor_service_id ==
                       datayes::mdl::MDLSID_MDL_API &&
                   record.vendor_service_version ==
                       api::MDLVID_MDL_API &&
                   record.vendor_message_id ==
                       api::MDLMID_MDL_API_ConnectErrorEvent &&
                   record.return_or_error_code == 0U &&
                   no_response && address_present && error_present;
        case ControlTypeV1::kDisconnected:
            return record.vendor_service_id ==
                       datayes::mdl::MDLSID_MDL_API &&
                   record.vendor_service_version ==
                       api::MDLVID_MDL_API &&
                   record.vendor_message_id ==
                       api::MDLMID_MDL_API_DisconnectedEvent &&
                   record.return_or_error_code == 0U &&
                   no_response && address_present && error_present;
        case ControlTypeV1::kLogonSuccess:
            return record.vendor_service_id ==
                       datayes::mdl::MDLSID_MDL_SYS &&
                   record.vendor_service_version ==
                       sys::MDLVID_MDL_SYS &&
                   record.vendor_message_id ==
                       sys::MDLMID_MDL_SYS_LogonResponse &&
                   record.return_or_error_code ==
                       static_cast<std::uint32_t>(
                           datayes::mdl::MDLEC_OK) &&
                   response_present && no_address_or_error &&
                   record.response_entry_count <= 1'000'000U;
        case ControlTypeV1::kLogonFailure:
            return record.vendor_service_id ==
                       datayes::mdl::MDLSID_MDL_SYS &&
                   record.vendor_service_version ==
                       sys::MDLVID_MDL_SYS &&
                   record.vendor_message_id ==
                       sys::MDLMID_MDL_SYS_LogonResponse &&
                   record.return_or_error_code !=
                       static_cast<std::uint32_t>(
                           datayes::mdl::MDLEC_OK) &&
                   response_present && no_address_or_error &&
                   record.response_entry_count <= 1'000'000U;
        case ControlTypeV1::kSubscriptionAccepted:
        case ControlTypeV1::kSubscriptionRejected:
            return record.vendor_service_id ==
                       datayes::mdl::MDLSID_MDL_SYS &&
                   record.vendor_service_version ==
                       sys::MDLVID_MDL_SYS &&
                   record.vendor_message_id ==
                       sys::MDLMID_MDL_SYS_SubscribeResponse &&
                   record.return_or_error_code == 0U &&
                   response_present && no_address_or_error &&
                   record.response_entry_count <= 1'000'000U &&
                   (record.control_type !=
                        ControlTypeV1::kSubscriptionRejected ||
                    record.response_entry_count != 0U);
        case ControlTypeV1::kServiceStatus:
            return record.vendor_service_id ==
                       datayes::mdl::MDLSID_MDL_SYS &&
                   record.vendor_service_version ==
                       sys::MDLVID_MDL_SYS &&
                   record.vendor_message_id ==
                       sys::MDLMID_MDL_SYS_ServiceStatus &&
                   !response_present && no_address_or_error &&
                   record.response_entry_count <= 4096U;
        case ControlTypeV1::kSessionStatus:
            return record.vendor_service_id ==
                       datayes::mdl::MDLSID_MDL_SYS &&
                   record.vendor_service_version ==
                       sys::MDLVID_MDL_SYS &&
                   record.vendor_message_id ==
                       sys::MDLMID_MDL_SYS_SessionStatus &&
                   record.return_or_error_code == 0U &&
                   !response_present && no_address_or_error &&
                   record.response_entry_count <= 4096U;
        case ControlTypeV1::kDecodeError:
            return DecodeErrorIdentityValid(record) &&
                   record.return_or_error_code == 0U &&
                   no_response && no_address_or_error;
    }
    return false;
}

bool QualityFlagsReachable(const ControlRecordV1& record) noexcept {
    constexpr std::uint64_t kDecodeMask =
        QualityBit(QualityFlagV1::kSchemaUnknown) |
        QualityBit(QualityFlagV1::kDecodeTruncated) |
        QualityBit(QualityFlagV1::kDecodeOffsetInvalid);
    constexpr std::uint64_t kAllowed =
        QualityBit(QualityFlagV1::kSubscriptionChanged) |
        QualityBit(QualityFlagV1::kUnauthorized) |
        QualityBit(QualityFlagV1::kNoncanonicalEmptyOffset) |
        kDecodeMask |
        QualityBit(QualityFlagV1::kSessionUnknown) |
        QualityBit(QualityFlagV1::kSourceDisconnected) |
        QualityBit(QualityFlagV1::kConnectionSwitched) |
        QualityBit(QualityFlagV1::kRawRecoveredAppendOnly);
    const bool session_unknown =
        (record.quality_flags &
         QualityBit(QualityFlagV1::kSessionUnknown)) != 0U;
    const bool source_disconnected =
        (record.quality_flags &
         QualityBit(QualityFlagV1::kSourceDisconnected)) != 0U;
    const bool unauthorized =
        (record.quality_flags &
         QualityBit(QualityFlagV1::kUnauthorized)) != 0U;
    const bool decode_quality =
        (record.quality_flags & kDecodeMask) != 0U;
    const bool event_requires_session_unknown =
        record.control_type == ControlTypeV1::kConnecting ||
        record.control_type == ControlTypeV1::kConnectError ||
        record.control_type == ControlTypeV1::kDisconnected ||
        record.control_type == ControlTypeV1::kLogonFailure ||
        record.control_type == ControlTypeV1::kDecodeError;
    const bool event_requires_source_disconnected =
        record.control_type == ControlTypeV1::kConnectError ||
        record.control_type == ControlTypeV1::kDisconnected;
    return (record.quality_flags & ~kAllowed) == 0U &&
           (!source_disconnected || session_unknown) &&
           (!unauthorized || session_unknown) &&
           (!decode_quality || session_unknown) &&
           (!event_requires_session_unknown || session_unknown) &&
           (!event_requires_source_disconnected ||
            decode_quality ||
            source_disconnected) &&
           (record.control_type != ControlTypeV1::kLogonSuccess ||
            session_unknown == decode_quality);
}

}  // namespace

std::string_view ControlRecordV1ErrorName(
    ControlRecordV1Error error) noexcept {
    switch (error) {
        case ControlRecordV1Error::kNone:
            return "none";
        case ControlRecordV1Error::kNullOutput:
            return "null_output";
        case ControlRecordV1Error::kInvalidWireSize:
            return "invalid_wire_size";
        case ControlRecordV1Error::kInvalidMagic:
            return "invalid_magic";
        case ControlRecordV1Error::kUnsupportedVersion:
            return "unsupported_version";
        case ControlRecordV1Error::kInvalidHeaderSize:
            return "invalid_header_size";
        case ControlRecordV1Error::kInvalidRecordSize:
            return "invalid_record_size";
        case ControlRecordV1Error::kUnknownFlags:
            return "unknown_flags";
        case ControlRecordV1Error::kInvalidControlType:
            return "invalid_control_type";
        case ControlRecordV1Error::kInvalidIdentity:
            return "invalid_identity";
        case ControlRecordV1Error::kInvalidOriginCursor:
            return "invalid_origin_cursor";
        case ControlRecordV1Error::kInvalidCounts:
            return "invalid_counts";
        case ControlRecordV1Error::kInvalidDecodeError:
            return "invalid_decode_error";
        case ControlRecordV1Error::kInconsistentFields:
            return "inconsistent_fields";
        case ControlRecordV1Error::kNonzeroReserved:
            return "nonzero_reserved";
        case ControlRecordV1Error::kCrcMismatch:
            return "crc_mismatch";
    }
    return "unknown";
}

ControlRecordV1Error ValidateControlRecordV1(
    const ControlRecordV1& record) noexcept {
    if ((record.flags & ~kControlRecordFlagsMask) != 0U) {
        return ControlRecordV1Error::kUnknownFlags;
    }
    if (!IsKnownControlType(record.control_type)) {
        return ControlRecordV1Error::kInvalidControlType;
    }
    if (record.source_stream_id == 0U ||
        record.capture_date == 0U ||
        l2flow::common::IsZeroIdentity(record.stream_day_id) ||
        (record.vendor_service_id != 1U &&
         record.vendor_service_id != 2U) ||
        record.vendor_message_id == 0U) {
        return ControlRecordV1Error::kInvalidIdentity;
    }
    if (record.control_type != ControlTypeV1::kDecodeError &&
        record.vendor_service_version == 0U) {
        return ControlRecordV1Error::kInvalidIdentity;
    }
    if (record.origin_ingress_sequence == 0U ||
        record.origin_record_end_wal_pos <
            l2flow::ingress::kRawV1SegmentHeaderBytes ||
        (record.origin_record_end_wal_pos %
         l2flow::ingress::kRawV1RecordAlignment) != 0U) {
        return ControlRecordV1Error::kInvalidOriginCursor;
    }
    if (!CountsValid(
            record.required_count,
            record.required_ok_count,
            record.required_failed_count) ||
        !CountsValid(
            record.optional_count,
            record.optional_ok_count,
            record.optional_failed_count) ||
        record.required_count == 0U ||
        record.required_count > 64U ||
        record.optional_count > 64U - record.required_count) {
        return ControlRecordV1Error::kInvalidCounts;
    }
    if ((record.control_type == ControlTypeV1::kDecodeError) !=
        (record.decode_error != 0U)) {
        return ControlRecordV1Error::kInvalidDecodeError;
    }
    const bool response_present =
        (record.flags &
         kControlRecordResponseManifestHashPresent) != 0U;
    const bool address_present =
        (record.flags & kControlRecordAddressHashPresent) != 0U;
    const bool error_present =
        (record.flags & kControlRecordErrorTextHashPresent) != 0U;
    if (!TypeIdentityAndPayloadValid(
            record,
            response_present,
            address_present,
            error_present) ||
        !QualityFlagsReachable(record) ||
        DigestIsZero(record.control_state_sha256) ||
        response_present ==
            DigestIsZero(record.response_manifest_sha256) ||
        address_present == DigestIsZero(record.address_sha256) ||
        error_present == DigestIsZero(record.error_text_sha256) ||
        (((record.flags & kControlRecordRequiredFailure) != 0U) !=
         (record.required_failed_count != 0U)) ||
        (((record.flags & kControlRecordOptionalFailure) != 0U) !=
         (record.optional_failed_count != 0U)) ||
        ((record.flags &
          kControlRecordNoncanonicalEmptyOffset) != 0U &&
         (record.quality_flags &
          QualityBit(QualityFlagV1::kNoncanonicalEmptyOffset)) ==
             0U)) {
        return ControlRecordV1Error::kInconsistentFields;
    }
    return ControlRecordV1Error::kNone;
}

ControlRecordV1Error EncodeControlRecordV1(
    const ControlRecordV1& record,
    ControlRecordWireV1* wire) noexcept {
    if (wire == nullptr) {
        return ControlRecordV1Error::kNullOutput;
    }
    const ControlRecordV1Error validation =
        ValidateControlRecordV1(record);
    if (validation != ControlRecordV1Error::kNone) {
        return validation;
    }

    ControlRecordWireV1 encoded{};
    const std::span<std::byte> bytes(encoded);
    StoreU32(
        kControlRecordV1Magic,
        bytes,
        control_record_v1_offset::kMagic);
    StoreU16(
        kControlRecordV1Version,
        bytes,
        control_record_v1_offset::kVersion);
    StoreU16(
        kControlRecordV1HeaderBytes,
        bytes,
        control_record_v1_offset::kHeaderSize);
    StoreU32(
        static_cast<std::uint32_t>(kControlRecordV1Bytes),
        bytes,
        control_record_v1_offset::kRecordSize);
    StoreU32(record.flags, bytes, control_record_v1_offset::kFlags);
    StoreU16(
        static_cast<std::uint16_t>(record.control_type),
        bytes,
        control_record_v1_offset::kControlType);
    StoreU16(
        record.decode_error,
        bytes,
        control_record_v1_offset::kDecodeError);
    StoreU32(
        record.source_stream_id,
        bytes,
        control_record_v1_offset::kSourceStreamId);
    StoreU32(
        record.capture_date,
        bytes,
        control_record_v1_offset::kCaptureDate);
    std::copy(
        record.stream_day_id.begin(),
        record.stream_day_id.end(),
        bytes.begin() + control_record_v1_offset::kStreamDayId);
    bytes[control_record_v1_offset::kVendorServiceId] =
        static_cast<std::byte>(record.vendor_service_id);
    StoreU16(
        record.vendor_service_version,
        bytes,
        control_record_v1_offset::kVendorServiceVersion);
    StoreU16(
        record.vendor_message_id,
        bytes,
        control_record_v1_offset::kVendorMessageId);
    StoreU32(
        record.return_or_error_code,
        bytes,
        control_record_v1_offset::kReturnOrErrorCode);
    StoreU32(
        record.connection_epoch,
        bytes,
        control_record_v1_offset::kConnectionEpoch);
    StoreU32(
        record.subscription_epoch,
        bytes,
        control_record_v1_offset::kSubscriptionEpoch);
    StoreU64(
        record.origin_ingress_sequence,
        bytes,
        control_record_v1_offset::kOriginIngressSequence);
    StoreU64(
        record.origin_record_end_wal_pos,
        bytes,
        control_record_v1_offset::kOriginRecordEndWalPos);
    StoreU64(
        record.quality_flags,
        bytes,
        control_record_v1_offset::kQualityFlags);
    StoreU32(
        record.required_count,
        bytes,
        control_record_v1_offset::kRequiredCount);
    StoreU32(
        record.required_ok_count,
        bytes,
        control_record_v1_offset::kRequiredOkCount);
    StoreU32(
        record.required_failed_count,
        bytes,
        control_record_v1_offset::kRequiredFailedCount);
    StoreU32(
        record.optional_count,
        bytes,
        control_record_v1_offset::kOptionalCount);
    StoreU32(
        record.optional_ok_count,
        bytes,
        control_record_v1_offset::kOptionalOkCount);
    StoreU32(
        record.optional_failed_count,
        bytes,
        control_record_v1_offset::kOptionalFailedCount);
    StoreU32(
        record.response_entry_count,
        bytes,
        control_record_v1_offset::kResponseEntryCount);
    StoreDigest(
        record.response_manifest_sha256,
        bytes,
        control_record_v1_offset::kResponseManifestSha256);
    StoreDigest(
        record.address_sha256,
        bytes,
        control_record_v1_offset::kAddressSha256);
    StoreDigest(
        record.error_text_sha256,
        bytes,
        control_record_v1_offset::kErrorTextSha256);
    StoreDigest(
        record.control_state_sha256,
        bytes,
        control_record_v1_offset::kControlStateSha256);
    const std::uint32_t crc =
        l2flow::common::ComputeCrc32c(bytes);
    StoreU32(
        crc, bytes, control_record_v1_offset::kRecordCrc32c);
    *wire = encoded;
    return ControlRecordV1Error::kNone;
}

ControlRecordV1Error DecodeControlRecordV1(
    std::span<const std::byte> wire,
    ControlRecordV1* record) noexcept {
    if (record == nullptr) {
        return ControlRecordV1Error::kNullOutput;
    }
    if (wire.size() != kControlRecordV1Bytes) {
        return ControlRecordV1Error::kInvalidWireSize;
    }
    if (LoadU32(wire, control_record_v1_offset::kMagic) !=
        kControlRecordV1Magic) {
        return ControlRecordV1Error::kInvalidMagic;
    }
    if (LoadU16(wire, control_record_v1_offset::kVersion) !=
        kControlRecordV1Version) {
        return ControlRecordV1Error::kUnsupportedVersion;
    }
    if (LoadU16(wire, control_record_v1_offset::kHeaderSize) !=
        kControlRecordV1HeaderBytes) {
        return ControlRecordV1Error::kInvalidHeaderSize;
    }
    if (LoadU32(wire, control_record_v1_offset::kRecordSize) !=
        kControlRecordV1Bytes) {
        return ControlRecordV1Error::kInvalidRecordSize;
    }
    if (!IsZeroRange(wire, control_record_v1_offset::kReserved0, 1U) ||
        !IsZeroRange(wire, control_record_v1_offset::kReserved1, 2U) ||
        !IsZeroRange(wire, control_record_v1_offset::kReserved2, 4U) ||
        !IsZeroRange(wire, control_record_v1_offset::kReservedTail, 4U)) {
        return ControlRecordV1Error::kNonzeroReserved;
    }

    ControlRecordWireV1 crc_input{};
    std::copy(wire.begin(), wire.end(), crc_input.begin());
    const std::uint32_t stored_crc =
        LoadU32(wire, control_record_v1_offset::kRecordCrc32c);
    StoreU32(
        0U,
        crc_input,
        control_record_v1_offset::kRecordCrc32c);
    if (l2flow::common::ComputeCrc32c(crc_input) != stored_crc) {
        return ControlRecordV1Error::kCrcMismatch;
    }

    ControlRecordV1 decoded{};
    decoded.flags = LoadU32(wire, control_record_v1_offset::kFlags);
    decoded.control_type = static_cast<ControlTypeV1>(
        LoadU16(wire, control_record_v1_offset::kControlType));
    decoded.decode_error =
        LoadU16(wire, control_record_v1_offset::kDecodeError);
    decoded.source_stream_id =
        LoadU32(wire, control_record_v1_offset::kSourceStreamId);
    decoded.capture_date =
        LoadU32(wire, control_record_v1_offset::kCaptureDate);
    std::copy_n(
        wire.begin() + control_record_v1_offset::kStreamDayId,
        decoded.stream_day_id.size(),
        decoded.stream_day_id.begin());
    decoded.vendor_service_id =
        std::to_integer<std::uint8_t>(
            wire[control_record_v1_offset::kVendorServiceId]);
    decoded.vendor_service_version = LoadU16(
        wire, control_record_v1_offset::kVendorServiceVersion);
    decoded.vendor_message_id =
        LoadU16(wire, control_record_v1_offset::kVendorMessageId);
    decoded.return_or_error_code = LoadU32(
        wire, control_record_v1_offset::kReturnOrErrorCode);
    decoded.connection_epoch =
        LoadU32(wire, control_record_v1_offset::kConnectionEpoch);
    decoded.subscription_epoch =
        LoadU32(wire, control_record_v1_offset::kSubscriptionEpoch);
    decoded.origin_ingress_sequence = LoadU64(
        wire, control_record_v1_offset::kOriginIngressSequence);
    decoded.origin_record_end_wal_pos = LoadU64(
        wire, control_record_v1_offset::kOriginRecordEndWalPos);
    decoded.quality_flags =
        LoadU64(wire, control_record_v1_offset::kQualityFlags);
    decoded.required_count =
        LoadU32(wire, control_record_v1_offset::kRequiredCount);
    decoded.required_ok_count =
        LoadU32(wire, control_record_v1_offset::kRequiredOkCount);
    decoded.required_failed_count = LoadU32(
        wire, control_record_v1_offset::kRequiredFailedCount);
    decoded.optional_count =
        LoadU32(wire, control_record_v1_offset::kOptionalCount);
    decoded.optional_ok_count =
        LoadU32(wire, control_record_v1_offset::kOptionalOkCount);
    decoded.optional_failed_count = LoadU32(
        wire, control_record_v1_offset::kOptionalFailedCount);
    decoded.response_entry_count = LoadU32(
        wire, control_record_v1_offset::kResponseEntryCount);
    decoded.response_manifest_sha256 = LoadDigest(
        wire, control_record_v1_offset::kResponseManifestSha256);
    decoded.address_sha256 =
        LoadDigest(wire, control_record_v1_offset::kAddressSha256);
    decoded.error_text_sha256 = LoadDigest(
        wire, control_record_v1_offset::kErrorTextSha256);
    decoded.control_state_sha256 = LoadDigest(
        wire, control_record_v1_offset::kControlStateSha256);
    decoded.record_crc32c = stored_crc;

    const ControlRecordV1Error validation =
        ValidateControlRecordV1(decoded);
    if (validation != ControlRecordV1Error::kNone) {
        return validation;
    }
    *record = decoded;
    return ControlRecordV1Error::kNone;
}

}  // namespace l2flow::control
