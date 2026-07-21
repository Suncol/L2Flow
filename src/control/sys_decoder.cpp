#include "l2flow/control/sys_decoder.h"

#include "l2flow/common/sha256.h"
#include "l2flow/control/checked_body_view.h"
#include "l2flow/control/quality_flags_v1.h"

#include "mdl_sys_msg.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <span>
#include <tuple>

namespace l2flow::control {
namespace {

namespace sys = datayes::mdl::mdl_sys_msg;

constexpr std::size_t kServiceItemBytes = 16U;
constexpr std::size_t kMessageItemBytes = 8U;
constexpr std::size_t kMaximumServices = 4096U;
constexpr std::size_t kMaximumMessages = 1'000'000U;

static_assert(sys::MDLVID_MDL_SYS == 101U);
static_assert(sizeof(sys::LogonResponse) == 24U);
static_assert(offsetof(sys::LogonResponse, UserName) == 0U);
static_assert(offsetof(sys::LogonResponse, Password) == 6U);
static_assert(offsetof(sys::LogonResponse, Services) == 12U);
static_assert(offsetof(sys::LogonResponse, ReturnCode) == 20U);
static_assert(sizeof(sys::LogonResponse::ServicesItem) == 16U);
static_assert(
    offsetof(sys::LogonResponse::ServicesItem, ServiceID) == 0U);
static_assert(
    offsetof(sys::LogonResponse::ServicesItem, ServiceVersion) == 4U);
static_assert(
    offsetof(sys::LogonResponse::ServicesItem, Messages) == 8U);
static_assert(
    sizeof(sys::LogonResponse::ServicesItem::MessagesItem) == 8U);
static_assert(
    offsetof(
        sys::LogonResponse::ServicesItem::MessagesItem,
        MessageID) == 0U);
static_assert(
    offsetof(
        sys::LogonResponse::ServicesItem::MessagesItem,
        MessageStatus) == 4U);
static_assert(sizeof(sys::SubscribeResponse) == 8U);
static_assert(offsetof(sys::SubscribeResponse, Services) == 0U);
static_assert(sizeof(sys::SubscribeResponse::ServicesItem) == 16U);
static_assert(
    sizeof(sys::SubscribeResponse::ServicesItem::MessagesItem) == 8U);
static_assert(sizeof(sys::ServiceStatus) == 60U);
static_assert(offsetof(sys::ServiceStatus, Version) == 0U);
static_assert(offsetof(sys::ServiceStatus, Services) == 44U);
static_assert(sizeof(sys::ServiceStatus::ServicesItem) == 12U);
static_assert(sizeof(sys::SessionStatus) == 8U);
static_assert(offsetof(sys::SessionStatus, Clients) == 0U);
static_assert(sizeof(sys::SessionStatus::ClientsItem) == 36U);
static_assert(
    offsetof(sys::SessionStatus::ClientsItem, Address) == 4U);
static_assert(
    offsetof(sys::SessionStatus::ClientsItem, SubscriptionList) == 30U);

SysControlDecodeErrorV1 Translate(
    CheckedBodyErrorV1 error) noexcept {
    switch (error) {
        case CheckedBodyErrorV1::kNone:
            return SysControlDecodeErrorV1::kNone;
        case CheckedBodyErrorV1::kTruncated:
            return SysControlDecodeErrorV1::kTruncated;
        case CheckedBodyErrorV1::kRangeOverlap:
            return SysControlDecodeErrorV1::kRangeOverlap;
        case CheckedBodyErrorV1::kCountExceeded:
            return SysControlDecodeErrorV1::kCountExceeded;
        case CheckedBodyErrorV1::kResourceExhausted:
            return SysControlDecodeErrorV1::kResourceExhausted;
        case CheckedBodyErrorV1::kArithmeticOverflow:
        case CheckedBodyErrorV1::kOffsetInvalid:
            return SysControlDecodeErrorV1::kOffsetInvalid;
    }
    return SysControlDecodeErrorV1::kOffsetInvalid;
}

bool CheckedAdd(
    std::size_t left,
    std::size_t right,
    std::size_t* output) noexcept {
    if (output == nullptr ||
        left > std::numeric_limits<std::size_t>::max() - right) {
        return false;
    }
    *output = left + right;
    return true;
}

bool SameKey(
    const SubscriptionStatusV1& left,
    const SubscriptionStatusV1& right) noexcept {
    return std::tie(
               left.service_id,
               left.service_version,
               left.message_id) ==
           std::tie(
               right.service_id,
               right.service_version,
               right.message_id);
}

bool StatusLess(
    const SubscriptionStatusV1& left,
    const SubscriptionStatusV1& right) noexcept {
    return std::tie(
               left.service_id,
               left.service_version,
               left.message_id,
               left.status) <
           std::tie(
               right.service_id,
               right.service_version,
               right.message_id,
               right.status);
}

bool StatusKeyLess(
    const SubscriptionStatusV1& left,
    const SubscriptionStatusV1& right) noexcept {
    return std::tie(
               left.service_id,
               left.service_version,
               left.message_id) <
           std::tie(
               right.service_id,
               right.service_version,
               right.message_id);
}

bool UpdateU32(
    l2flow::common::Sha256Hasher* hasher,
    std::uint32_t value) noexcept {
    std::array<std::byte, 4U> bytes{};
    for (std::size_t index = 0U; index < bytes.size(); ++index) {
        bytes[index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
    return hasher->Update(bytes);
}

SysControlDecodeErrorV1 DecodeStatuses(
    CheckedBodyViewV1* view,
    std::size_t services_descriptor,
    std::size_t fixed_bytes,
    std::vector<SubscriptionStatusV1>* statuses) noexcept {
    CheckedBodyRangeV1 services;
    CheckedBodyErrorV1 body_error = view->ReadList(
        services_descriptor,
        kServiceItemBytes,
        fixed_bytes,
        kMaximumServices,
        &services);
    if (body_error != CheckedBodyErrorV1::kNone) {
        return Translate(body_error);
    }
    if (services.count >
        std::numeric_limits<std::size_t>::max() /
            services.item_bytes) {
        return SysControlDecodeErrorV1::kOffsetInvalid;
    }
    std::size_t services_end = 0U;
    if (!CheckedAdd(
            services.start,
            services.count * services.item_bytes,
            &services_end)) {
        return SysControlDecodeErrorV1::kOffsetInvalid;
    }

    std::size_t aggregate_messages = 0U;
    try {
        statuses->clear();
        statuses->reserve(std::min(
            kMaximumMessages,
            view->body().size() / kMessageItemBytes));
    } catch (const std::bad_alloc&) {
        return SysControlDecodeErrorV1::kResourceExhausted;
    }

    for (std::size_t service_index = 0U;
         service_index < services.count;
         ++service_index) {
        const std::size_t service_offset =
            services.start + service_index * services.item_bytes;
        std::uint32_t service_id = 0U;
        std::uint32_t service_version = 0U;
        body_error = view->ReadU32(service_offset, &service_id);
        if (body_error != CheckedBodyErrorV1::kNone) {
            return Translate(body_error);
        }
        body_error = view->ReadU32(
            service_offset + 4U, &service_version);
        if (body_error != CheckedBodyErrorV1::kNone) {
            return Translate(body_error);
        }

        CheckedBodyRangeV1 messages;
        body_error = view->ReadList(
            service_offset + 8U,
            kMessageItemBytes,
            services_end,
            kMaximumMessages,
            &messages);
        if (body_error != CheckedBodyErrorV1::kNone) {
            return Translate(body_error);
        }
        if (aggregate_messages > kMaximumMessages ||
            messages.count >
                kMaximumMessages - aggregate_messages) {
            return SysControlDecodeErrorV1::kCountExceeded;
        }
        aggregate_messages += messages.count;
        try {
            for (std::size_t message_index = 0U;
                 message_index < messages.count;
                 ++message_index) {
                const std::size_t message_offset =
                    messages.start +
                    message_index * messages.item_bytes;
                SubscriptionStatusV1 status{};
                status.service_id = service_id;
                status.service_version = service_version;
                body_error = view->ReadU32(
                    message_offset, &status.message_id);
                if (body_error != CheckedBodyErrorV1::kNone) {
                    return Translate(body_error);
                }
                body_error = view->ReadU32(
                    message_offset + 4U, &status.status);
                if (body_error != CheckedBodyErrorV1::kNone) {
                    return Translate(body_error);
                }
                statuses->push_back(status);
            }
        } catch (const std::bad_alloc&) {
            return SysControlDecodeErrorV1::kResourceExhausted;
        }
    }

    std::sort(statuses->begin(), statuses->end(), StatusLess);
    for (std::size_t index = 1U; index < statuses->size(); ++index) {
        if (SameKey((*statuses)[index - 1U], (*statuses)[index])) {
            return SysControlDecodeErrorV1::
                kDuplicateSubscriptionKey;
        }
    }
    return SysControlDecodeErrorV1::kNone;
}

SysControlDecodeErrorV1 DecodeLogon(
    std::span<const std::byte> body,
    DecodedSysControlV1* decoded) noexcept {
    CheckedBodyViewV1 view(body);
    CheckedBodyErrorV1 body_error =
        view.RequireFixed(sizeof(sys::LogonResponse));
    if (body_error != CheckedBodyErrorV1::kNone) {
        return Translate(body_error);
    }
    std::span<const std::byte> ignored;
    body_error = view.ReadString(
        offsetof(sys::LogonResponse, UserName),
        sizeof(sys::LogonResponse),
        &ignored);
    if (body_error != CheckedBodyErrorV1::kNone) {
        return Translate(body_error);
    }
    body_error = view.ReadString(
        offsetof(sys::LogonResponse, Password),
        sizeof(sys::LogonResponse),
        &ignored);
    if (body_error != CheckedBodyErrorV1::kNone) {
        return Translate(body_error);
    }
    const SysControlDecodeErrorV1 status_error = DecodeStatuses(
        &view,
        offsetof(sys::LogonResponse, Services),
        sizeof(sys::LogonResponse),
        &decoded->subscription_statuses);
    if (status_error != SysControlDecodeErrorV1::kNone) {
        return status_error;
    }
    body_error = view.ReadU32(
        offsetof(sys::LogonResponse, ReturnCode),
        &decoded->return_or_error_code);
    if (body_error != CheckedBodyErrorV1::kNone) {
        return Translate(body_error);
    }
    decoded->control_type =
        decoded->return_or_error_code ==
                static_cast<std::uint32_t>(datayes::mdl::MDLEC_OK)
            ? ControlTypeV1::kLogonSuccess
            : ControlTypeV1::kLogonFailure;
    decoded->response_entry_count = static_cast<std::uint32_t>(
        decoded->subscription_statuses.size());
    decoded->response_manifest_sha256 =
        ComputeSubscriptionResponseManifestSha256V1(
            decoded->subscription_statuses);
    decoded->response_manifest_present = true;
    decoded->noncanonical_empty_offset =
        view.noncanonical_empty_offset();
    return SysControlDecodeErrorV1::kNone;
}

SysControlDecodeErrorV1 DecodeSubscribe(
    std::span<const std::byte> body,
    DecodedSysControlV1* decoded) noexcept {
    CheckedBodyViewV1 view(body);
    const CheckedBodyErrorV1 fixed_error =
        view.RequireFixed(sizeof(sys::SubscribeResponse));
    if (fixed_error != CheckedBodyErrorV1::kNone) {
        return Translate(fixed_error);
    }
    const SysControlDecodeErrorV1 status_error = DecodeStatuses(
        &view,
        offsetof(sys::SubscribeResponse, Services),
        sizeof(sys::SubscribeResponse),
        &decoded->subscription_statuses);
    if (status_error != SysControlDecodeErrorV1::kNone) {
        return status_error;
    }
    const bool accepted = std::all_of(
        decoded->subscription_statuses.begin(),
        decoded->subscription_statuses.end(),
        [](const SubscriptionStatusV1& status) {
            return status.status ==
                   static_cast<std::uint32_t>(
                       datayes::mdl::MDLEC_OK);
        });
    decoded->control_type =
        accepted ? ControlTypeV1::kSubscriptionAccepted
                 : ControlTypeV1::kSubscriptionRejected;
    decoded->response_entry_count = static_cast<std::uint32_t>(
        decoded->subscription_statuses.size());
    decoded->response_manifest_sha256 =
        ComputeSubscriptionResponseManifestSha256V1(
            decoded->subscription_statuses);
    decoded->response_manifest_present = true;
    decoded->noncanonical_empty_offset =
        view.noncanonical_empty_offset();
    return SysControlDecodeErrorV1::kNone;
}

SysControlDecodeErrorV1 DecodeServiceStatus(
    std::span<const std::byte> body,
    DecodedSysControlV1* decoded) noexcept {
    CheckedBodyViewV1 view(body);
    CheckedBodyErrorV1 body_error =
        view.RequireFixed(sizeof(sys::ServiceStatus));
    if (body_error != CheckedBodyErrorV1::kNone) {
        return Translate(body_error);
    }
    body_error = view.ReadU32(
        offsetof(sys::ServiceStatus, Version),
        &decoded->return_or_error_code);
    if (body_error != CheckedBodyErrorV1::kNone) {
        return Translate(body_error);
    }
    CheckedBodyRangeV1 services;
    body_error = view.ReadList(
        offsetof(sys::ServiceStatus, Services),
        sizeof(sys::ServiceStatus::ServicesItem),
        sizeof(sys::ServiceStatus),
        kMaximumServices,
        &services);
    if (body_error != CheckedBodyErrorV1::kNone) {
        return Translate(body_error);
    }
    decoded->control_type = ControlTypeV1::kServiceStatus;
    decoded->response_entry_count =
        static_cast<std::uint32_t>(services.count);
    decoded->noncanonical_empty_offset =
        view.noncanonical_empty_offset();
    return SysControlDecodeErrorV1::kNone;
}

SysControlDecodeErrorV1 DecodeSessionStatus(
    std::span<const std::byte> body,
    DecodedSysControlV1* decoded) noexcept {
    CheckedBodyViewV1 view(body);
    CheckedBodyErrorV1 body_error =
        view.RequireFixed(sizeof(sys::SessionStatus));
    if (body_error != CheckedBodyErrorV1::kNone) {
        return Translate(body_error);
    }
    CheckedBodyRangeV1 clients;
    body_error = view.ReadList(
        offsetof(sys::SessionStatus, Clients),
        sizeof(sys::SessionStatus::ClientsItem),
        sizeof(sys::SessionStatus),
        kMaximumServices,
        &clients);
    if (body_error != CheckedBodyErrorV1::kNone) {
        return Translate(body_error);
    }
    std::size_t clients_end = 0U;
    if (clients.count >
            std::numeric_limits<std::size_t>::max() /
                clients.item_bytes ||
        !CheckedAdd(
            clients.start,
            clients.count * clients.item_bytes,
            &clients_end)) {
        return SysControlDecodeErrorV1::kOffsetInvalid;
    }
    for (std::size_t index = 0U; index < clients.count; ++index) {
        const std::size_t item =
            clients.start + index * clients.item_bytes;
        std::span<const std::byte> ignored;
        body_error = view.ReadString(
            item + offsetof(sys::SessionStatus::ClientsItem, Address),
            clients_end,
            &ignored);
        if (body_error != CheckedBodyErrorV1::kNone) {
            return Translate(body_error);
        }
        body_error = view.ReadString(
            item + offsetof(
                       sys::SessionStatus::ClientsItem,
                       SubscriptionList),
            clients_end,
            &ignored);
        if (body_error != CheckedBodyErrorV1::kNone) {
            return Translate(body_error);
        }
    }
    decoded->control_type = ControlTypeV1::kSessionStatus;
    decoded->response_entry_count =
        static_cast<std::uint32_t>(clients.count);
    decoded->noncanonical_empty_offset =
        view.noncanonical_empty_offset();
    return SysControlDecodeErrorV1::kNone;
}

}  // namespace

l2flow::common::Sha256Digest
ComputeSubscriptionResponseManifestSha256V1(
    std::span<const SubscriptionStatusV1> sorted_statuses) noexcept {
    constexpr std::string_view kDomain =
        "L2FLOW_PHASE3_SUBSCRIPTION_RESPONSE_MANIFEST_V1";
    const std::span<const char> domain_chars(
        kDomain.data(), kDomain.size());
    static constexpr std::array<std::byte, 1U> separator{
        std::byte{0}};
    l2flow::common::Sha256Hasher hasher;
    l2flow::common::Sha256Digest digest{};
    if (!hasher.Update(std::as_bytes(domain_chars)) ||
        !hasher.Update(separator) ||
        sorted_statuses.size() >
            std::numeric_limits<std::uint32_t>::max() ||
        !UpdateU32(
            &hasher,
            static_cast<std::uint32_t>(sorted_statuses.size()))) {
        return digest;
    }
    for (std::size_t index = 1U;
         index < sorted_statuses.size();
         ++index) {
        if (!StatusKeyLess(
                sorted_statuses[index - 1U],
                sorted_statuses[index])) {
            return {};
        }
    }
    for (const SubscriptionStatusV1& status : sorted_statuses) {
        if (!UpdateU32(&hasher, status.service_id) ||
            !UpdateU32(&hasher, status.service_version) ||
            !UpdateU32(&hasher, status.message_id) ||
            !UpdateU32(&hasher, status.status)) {
            return {};
        }
    }
    if (!hasher.Finalize(&digest)) {
        return {};
    }
    return digest;
}

SysControlDecodeErrorV1 DecodeSysControlV1(
    std::uint16_t service_version,
    std::uint16_t message_id,
    std::span<const std::byte> body,
    DecodedSysControlV1* output) noexcept {
    if (output == nullptr) {
        return SysControlDecodeErrorV1::kNullOutput;
    }
    const bool recognized =
        message_id == sys::MDLMID_MDL_SYS_LogonResponse ||
        message_id == sys::MDLMID_MDL_SYS_SubscribeResponse ||
        message_id == sys::MDLMID_MDL_SYS_ServiceStatus ||
        message_id == sys::MDLMID_MDL_SYS_SessionStatus;
    if (!recognized) {
        return SysControlDecodeErrorV1::kUnsupportedMessage;
    }
    if (service_version != sys::MDLVID_MDL_SYS) {
        return SysControlDecodeErrorV1::
            kUnsupportedServiceVersion;
    }

    try {
        DecodedSysControlV1 decoded{};
        SysControlDecodeErrorV1 result =
            SysControlDecodeErrorV1::kUnsupportedMessage;
        switch (message_id) {
            case sys::MDLMID_MDL_SYS_LogonResponse:
                result = DecodeLogon(body, &decoded);
                break;
            case sys::MDLMID_MDL_SYS_SubscribeResponse:
                result = DecodeSubscribe(body, &decoded);
                break;
            case sys::MDLMID_MDL_SYS_ServiceStatus:
                result = DecodeServiceStatus(body, &decoded);
                break;
            case sys::MDLMID_MDL_SYS_SessionStatus:
                result = DecodeSessionStatus(body, &decoded);
                break;
            default:
                break;
        }
        if (result == SysControlDecodeErrorV1::kNone) {
            *output = std::move(decoded);
        }
        return result;
    } catch (const std::bad_alloc&) {
        return SysControlDecodeErrorV1::kResourceExhausted;
    } catch (...) {
        return SysControlDecodeErrorV1::kResourceExhausted;
    }
}

std::uint64_t SysDecodeQualityFlagsV1(
    SysControlDecodeErrorV1 error) noexcept {
    switch (error) {
        case SysControlDecodeErrorV1::kNone:
        case SysControlDecodeErrorV1::kUnsupportedMessage:
            return 0U;
        case SysControlDecodeErrorV1::kUnsupportedServiceVersion:
            return QualityBit(QualityFlagV1::kSchemaUnknown);
        case SysControlDecodeErrorV1::kTruncated:
            return QualityBit(QualityFlagV1::kDecodeTruncated);
        case SysControlDecodeErrorV1::kNullOutput:
        case SysControlDecodeErrorV1::kOffsetInvalid:
        case SysControlDecodeErrorV1::kRangeOverlap:
        case SysControlDecodeErrorV1::kCountExceeded:
        case SysControlDecodeErrorV1::kDuplicateSubscriptionKey:
        case SysControlDecodeErrorV1::kResourceExhausted:
            return QualityBit(QualityFlagV1::kDecodeOffsetInvalid);
    }
    return QualityBit(QualityFlagV1::kDecodeOffsetInvalid);
}

}  // namespace l2flow::control
