#include "l2flow/control/api_decoder.h"

#include "l2flow/control/control_hash.h"
#include "l2flow/control/quality_flags_v1.h"

#include "mdl_api_msg.h"

#include <cstddef>
#include <cstdint>
#include <span>

namespace l2flow::control {
namespace {

namespace api = datayes::mdl::mdl_api_msg;

constexpr std::size_t kStringBytes = 6U;

static_assert(api::MDLVID_MDL_API == 101U);
static_assert(sizeof(api::ConnectingEvent) == 6U);
static_assert(offsetof(api::ConnectingEvent, Address) == 0U);
static_assert(sizeof(api::ConnectErrorEvent) == 12U);
static_assert(offsetof(api::ConnectErrorEvent, ErrorMessage) == 0U);
static_assert(offsetof(api::ConnectErrorEvent, Address) == 6U);
static_assert(sizeof(api::DisconnectedEvent) == 12U);
static_assert(offsetof(api::DisconnectedEvent, ErrorMessage) == 0U);
static_assert(offsetof(api::DisconnectedEvent, Address) == 6U);

ApiControlDecodeErrorV1 Translate(
    CheckedBodyErrorV1 error) noexcept {
    switch (error) {
        case CheckedBodyErrorV1::kNone:
            return ApiControlDecodeErrorV1::kNone;
        case CheckedBodyErrorV1::kTruncated:
            return ApiControlDecodeErrorV1::kTruncated;
        case CheckedBodyErrorV1::kRangeOverlap:
            return ApiControlDecodeErrorV1::kRangeOverlap;
        case CheckedBodyErrorV1::kResourceExhausted:
            return ApiControlDecodeErrorV1::kResourceExhausted;
        case CheckedBodyErrorV1::kArithmeticOverflow:
        case CheckedBodyErrorV1::kOffsetInvalid:
        case CheckedBodyErrorV1::kCountExceeded:
            return ApiControlDecodeErrorV1::kOffsetInvalid;
    }
    return ApiControlDecodeErrorV1::kOffsetInvalid;
}

ApiControlDecodeErrorV1 ReadText(
    CheckedBodyViewV1* view,
    std::size_t descriptor_offset,
    std::size_t fixed_bytes,
    std::string_view domain,
    l2flow::common::Sha256Digest* digest) noexcept {
    std::span<const std::byte> text;
    const CheckedBodyErrorV1 error = view->ReadString(
        descriptor_offset, fixed_bytes, &text);
    if (error != CheckedBodyErrorV1::kNone) {
        return Translate(error);
    }
    *digest = HashControlTextV1(domain, text);
    return ApiControlDecodeErrorV1::kNone;
}

}  // namespace

ApiControlDecodeErrorV1 DecodeApiControlV1(
    std::uint16_t service_version,
    std::uint16_t message_id,
    std::span<const std::byte> body,
    DecodedApiControlV1* output) noexcept {
    if (output == nullptr) {
        return ApiControlDecodeErrorV1::kNullOutput;
    }
    const bool recognized =
        message_id == api::MDLMID_MDL_API_ConnectingEvent ||
        message_id == api::MDLMID_MDL_API_ConnectErrorEvent ||
        message_id == api::MDLMID_MDL_API_DisconnectedEvent;
    if (!recognized) {
        return ApiControlDecodeErrorV1::kUnsupportedMessage;
    }
    if (service_version != api::MDLVID_MDL_API) {
        return ApiControlDecodeErrorV1::
            kUnsupportedServiceVersion;
    }

    DecodedApiControlV1 decoded{};
    CheckedBodyViewV1 view(body);
    CheckedBodyErrorV1 fixed_error = CheckedBodyErrorV1::kNone;
    ApiControlDecodeErrorV1 text_error =
        ApiControlDecodeErrorV1::kNone;
    switch (message_id) {
        case api::MDLMID_MDL_API_ConnectingEvent:
            decoded.control_type = ControlTypeV1::kConnecting;
            fixed_error = view.RequireFixed(
                sizeof(api::ConnectingEvent));
            if (fixed_error != CheckedBodyErrorV1::kNone) {
                return Translate(fixed_error);
            }
            text_error = ReadText(
                &view,
                offsetof(api::ConnectingEvent, Address),
                sizeof(api::ConnectingEvent),
                "l2flow-control-api-address-v1",
                &decoded.address_sha256);
            if (text_error != ApiControlDecodeErrorV1::kNone) {
                return text_error;
            }
            decoded.address_present = true;
            break;
        case api::MDLMID_MDL_API_ConnectErrorEvent:
            decoded.control_type = ControlTypeV1::kConnectError;
            fixed_error = view.RequireFixed(
                sizeof(api::ConnectErrorEvent));
            if (fixed_error != CheckedBodyErrorV1::kNone) {
                return Translate(fixed_error);
            }
            text_error = ReadText(
                &view,
                offsetof(api::ConnectErrorEvent, ErrorMessage),
                sizeof(api::ConnectErrorEvent),
                "l2flow-control-api-error-text-v1",
                &decoded.error_text_sha256);
            if (text_error != ApiControlDecodeErrorV1::kNone) {
                return text_error;
            }
            text_error = ReadText(
                &view,
                offsetof(api::ConnectErrorEvent, Address),
                sizeof(api::ConnectErrorEvent),
                "l2flow-control-api-address-v1",
                &decoded.address_sha256);
            if (text_error != ApiControlDecodeErrorV1::kNone) {
                return text_error;
            }
            decoded.address_present = true;
            decoded.error_text_present = true;
            break;
        case api::MDLMID_MDL_API_DisconnectedEvent:
            decoded.control_type = ControlTypeV1::kDisconnected;
            fixed_error = view.RequireFixed(
                sizeof(api::DisconnectedEvent));
            if (fixed_error != CheckedBodyErrorV1::kNone) {
                return Translate(fixed_error);
            }
            text_error = ReadText(
                &view,
                offsetof(api::DisconnectedEvent, ErrorMessage),
                sizeof(api::DisconnectedEvent),
                "l2flow-control-api-error-text-v1",
                &decoded.error_text_sha256);
            if (text_error != ApiControlDecodeErrorV1::kNone) {
                return text_error;
            }
            text_error = ReadText(
                &view,
                offsetof(api::DisconnectedEvent, Address),
                sizeof(api::DisconnectedEvent),
                "l2flow-control-api-address-v1",
                &decoded.address_sha256);
            if (text_error != ApiControlDecodeErrorV1::kNone) {
                return text_error;
            }
            decoded.address_present = true;
            decoded.error_text_present = true;
            break;
        default:
            return ApiControlDecodeErrorV1::kUnsupportedMessage;
    }

    decoded.noncanonical_empty_offset =
        view.noncanonical_empty_offset();
    *output = decoded;
    return ApiControlDecodeErrorV1::kNone;
}

std::uint64_t ApiDecodeQualityFlagsV1(
    ApiControlDecodeErrorV1 error) noexcept {
    switch (error) {
        case ApiControlDecodeErrorV1::kNone:
        case ApiControlDecodeErrorV1::kUnsupportedMessage:
            return 0U;
        case ApiControlDecodeErrorV1::kUnsupportedServiceVersion:
            return QualityBit(QualityFlagV1::kSchemaUnknown);
        case ApiControlDecodeErrorV1::kTruncated:
            return QualityBit(QualityFlagV1::kDecodeTruncated);
        case ApiControlDecodeErrorV1::kNullOutput:
        case ApiControlDecodeErrorV1::kOffsetInvalid:
        case ApiControlDecodeErrorV1::kRangeOverlap:
        case ApiControlDecodeErrorV1::kResourceExhausted:
            return QualityBit(QualityFlagV1::kDecodeOffsetInvalid);
    }
    return QualityBit(QualityFlagV1::kDecodeOffsetInvalid);
}

}  // namespace l2flow::control
