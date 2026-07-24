#include "l2flow/realtime/owned_ingress_message_v1.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>
#include <utility>

namespace l2flow::realtime {
namespace {

[[nodiscard]] bool IsZeroIdentity(
    const l2flow::common::Identity128& identity) noexcept {
    return std::all_of(
        identity.begin(), identity.end(),
        [](std::byte value) { return value == std::byte{0U}; });
}

}  // namespace

std::string_view OwnedIngressKeyErrorNameV1(
    OwnedIngressKeyErrorV1 error) noexcept {
    switch (error) {
        case OwnedIngressKeyErrorV1::kNone:
            return "none";
        case OwnedIngressKeyErrorV1::kNullOutput:
            return "null_output";
        case OwnedIngressKeyErrorV1::kUnsupported:
            return "unsupported";
        case OwnedIngressKeyErrorV1::kForbiddenCombinedTick:
            return "forbidden_combined_tick";
    }
    return "unknown";
}

OwnedIngressKeyErrorV1 ClassifyOwnedIngressMessageKeyV1(
    const l2flow::sdk::MessageKey& key,
    OwnedIngressSourceV1* output) noexcept {
    if (output == nullptr) {
        return OwnedIngressKeyErrorV1::kNullOutput;
    }
    if (key == kForbiddenShenzhenCombinedTickKeyV1) {
        return OwnedIngressKeyErrorV1::kForbiddenCombinedTick;
    }

    OwnedIngressSourceV1 source =
        OwnedIngressSourceV1::kShanghaiSnapshot;
    if (key == kRequiredOwnedIngressMessageKeysV1[0U]) {
        source = OwnedIngressSourceV1::kShanghaiSnapshot;
    } else if (key == kRequiredOwnedIngressMessageKeysV1[1U]) {
        source = OwnedIngressSourceV1::kShanghaiTick;
    } else if (key == kRequiredOwnedIngressMessageKeysV1[2U]) {
        source = OwnedIngressSourceV1::kShenzhenSnapshot;
    } else if (key == kRequiredOwnedIngressMessageKeysV1[3U] ||
               key == kRequiredOwnedIngressMessageKeysV1[4U]) {
        source = OwnedIngressSourceV1::kShenzhenTick;
    } else {
        return OwnedIngressKeyErrorV1::kUnsupported;
    }

    *output = source;
    return OwnedIngressKeyErrorV1::kNone;
}

std::string_view OwnedIngressCreateErrorNameV1(
    OwnedIngressCreateErrorV1 error) noexcept {
    switch (error) {
        case OwnedIngressCreateErrorV1::kNone:
            return "none";
        case OwnedIngressCreateErrorV1::kNullOutput:
            return "null_output";
        case OwnedIngressCreateErrorV1::kInvalidMetadata:
            return "invalid_metadata";
        case OwnedIngressCreateErrorV1::kInvalidMaximumMessageBytes:
            return "invalid_maximum_message_bytes";
        case OwnedIngressCreateErrorV1::kNullMessage:
            return "null_message";
        case OwnedIngressCreateErrorV1::kSdkAccess:
            return "sdk_access";
        case OwnedIngressCreateErrorV1::kNullHead:
            return "null_head";
        case OwnedIngressCreateErrorV1::kWrongHeadSize:
            return "wrong_head_size";
        case OwnedIngressCreateErrorV1::kMessageSmallerThanHead:
            return "message_smaller_than_head";
        case OwnedIngressCreateErrorV1::kMessageTooLarge:
            return "message_too_large";
        case OwnedIngressCreateErrorV1::kUnexpectedMessageEncoding:
            return "unexpected_message_encoding";
        case OwnedIngressCreateErrorV1::kUnsupportedMessage:
            return "unsupported_message";
        case OwnedIngressCreateErrorV1::kForbiddenCombinedTick:
            return "forbidden_combined_tick";
        case OwnedIngressCreateErrorV1::kNullBody:
            return "null_body";
        case OwnedIngressCreateErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

OwnedIngressMessageV1::OwnedIngressMessageV1(
    OwnedIngressMetadataV1 metadata) noexcept
    : metadata_(metadata) {}

OwnedIngressCreateErrorV1 OwnedIngressMessageV1::Create(
    const datayes::mdl::MDLMessage* message,
    const OwnedIngressMetadataV1& metadata,
    std::uint32_t maximum_message_bytes,
    std::shared_ptr<const OwnedIngressMessageV1>* output) noexcept {
    if (output == nullptr) {
        return OwnedIngressCreateErrorV1::kNullOutput;
    }
    output->reset();

    if (IsZeroIdentity(metadata.run_id) ||
        metadata.global_ingress_sequence == 0U ||
        metadata.source_sequence == 0U ||
        metadata.global_ingress_sequence ==
            std::numeric_limits<std::uint64_t>::max() ||
        metadata.source_sequence ==
            std::numeric_limits<std::uint64_t>::max()) {
        return OwnedIngressCreateErrorV1::kInvalidMetadata;
    }
    if (maximum_message_bytes < l2flow::sdk::kVendorHeadBytes) {
        return OwnedIngressCreateErrorV1::kInvalidMaximumMessageBytes;
    }
    if (message == nullptr) {
        return OwnedIngressCreateErrorV1::kNullMessage;
    }

    const datayes::mdl::MDLMessageHead* vendor_head = nullptr;
    try {
        vendor_head = message->GetHead();
        if (vendor_head == nullptr) {
            return OwnedIngressCreateErrorV1::kNullHead;
        }
    } catch (...) {
        return OwnedIngressCreateErrorV1::kSdkAccess;
    }

    std::shared_ptr<OwnedIngressMessageV1> candidate;
    try {
        candidate = std::shared_ptr<OwnedIngressMessageV1>(
            new OwnedIngressMessageV1(metadata));
    } catch (...) {
        return OwnedIngressCreateErrorV1::kResourceExhausted;
    }
    std::memcpy(
        candidate->vendor_head_bytes_.data(),
        vendor_head,
        candidate->vendor_head_bytes_.size());

    const l2flow::sdk::VendorHeadView head(
        candidate->vendor_head_bytes_);
    if (head.head_size() != l2flow::sdk::kVendorHeadBytes) {
        return OwnedIngressCreateErrorV1::kWrongHeadSize;
    }
    if (head.message_size() <
        static_cast<std::uint32_t>(head.head_size())) {
        return OwnedIngressCreateErrorV1::kMessageSmallerThanHead;
    }
    if (head.message_size() > maximum_message_bytes) {
        return OwnedIngressCreateErrorV1::kMessageTooLarge;
    }
    if (head.message_encoding() !=
        static_cast<std::uint8_t>(datayes::mdl::MDLEID_BINARY)) {
        return OwnedIngressCreateErrorV1::kUnexpectedMessageEncoding;
    }

    const l2flow::sdk::MessageKey key{
        head.service_id(), head.service_version(), head.message_id()};
    OwnedIngressSourceV1 source{};
    const OwnedIngressKeyErrorV1 key_error =
        ClassifyOwnedIngressMessageKeyV1(key, &source);
    if (key_error ==
        OwnedIngressKeyErrorV1::kForbiddenCombinedTick) {
        return OwnedIngressCreateErrorV1::kForbiddenCombinedTick;
    }
    if (key_error != OwnedIngressKeyErrorV1::kNone) {
        return OwnedIngressCreateErrorV1::kUnsupportedMessage;
    }
    candidate->key_ = key;
    candidate->source_ = source;

    const std::size_t body_size =
        static_cast<std::size_t>(head.message_size()) -
        l2flow::sdk::kVendorHeadBytes;
    const char* body_pointer = nullptr;
    if (body_size != 0U) {
        try {
            body_pointer = message->GetBody();
        } catch (...) {
            return OwnedIngressCreateErrorV1::kSdkAccess;
        }
        if (body_pointer == nullptr) {
            return OwnedIngressCreateErrorV1::kNullBody;
        }
    }

    try {
        candidate->body_.resize(body_size);
        if (body_size != 0U) {
            std::memcpy(
                candidate->body_.data(),
                body_pointer,
                candidate->body_.size());
        }
        *output = std::move(candidate);
    } catch (const std::bad_alloc&) {
        return OwnedIngressCreateErrorV1::kResourceExhausted;
    } catch (...) {
        return OwnedIngressCreateErrorV1::kResourceExhausted;
    }
    return OwnedIngressCreateErrorV1::kNone;
}

}  // namespace l2flow::realtime
