#include "l2flow/sdk/ingress_config.h"

#include "l2flow/common/sha256.h"
#include "l2flow/ingress/capture_meta.h"
#include "l2flow/sdk/vendor_head_view.h"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string_view>

namespace l2flow::sdk {
namespace {

bool IsGregorianDate(std::uint32_t value) {
    if (value < 10000101U ||
        value > 99991231U) {
        return false;
    }
    const std::uint32_t year = value / 10000U;
    const std::uint32_t month = (value / 100U) % 100U;
    const std::uint32_t day = value % 100U;
    if (month == 0U || month > 12U) {
        return false;
    }
    static constexpr std::uint32_t days[] = {
        31U, 28U, 31U, 30U, 31U, 30U,
        31U, 31U, 30U, 31U, 30U, 31U,
    };
    std::uint32_t maximum = days[month - 1U];
    const bool leap =
        year % 4U == 0U && (year % 100U != 0U || year % 400U == 0U);
    if (month == 2U && leap) {
        maximum = 29U;
    }
    return day != 0U && day <= maximum;
}

bool IsLowerHexSha256(std::string_view text) {
    return text.size() == 64U &&
           std::all_of(text.begin(), text.end(), [](unsigned char value) {
               return (value >= static_cast<unsigned char>('0') &&
                       value <= static_cast<unsigned char>('9')) ||
                      (value >= static_cast<unsigned char>('a') &&
                       value <= static_cast<unsigned char>('f'));
           });
}

bool IsSupportedMessageEncoding(
    datayes::mdl::MDLMessageEncoding encoding) noexcept {
    switch (encoding) {
    case datayes::mdl::MDLEID_BINARY:
    case datayes::mdl::MDLEID_FAST:
    case datayes::mdl::MDLEID_JSON:
    case datayes::mdl::MDLEID_PROTOBUF:
    case datayes::mdl::MDLEID_CSV:
    case datayes::mdl::MDLEID_MKTDATA:
    case datayes::mdl::MDLEID_MKTPRO:
    case datayes::mdl::MDLEID_PACKAGE:
    case datayes::mdl::MDLEID_DEFLATE:
    case datayes::mdl::MDLEID_DEFLATE_PROTOBUF:
        return true;
    case datayes::mdl::MDLEID_UNDEFINED:
        return false;
    }
    return false;
}

bool ContainsNul(std::string_view value) noexcept {
    return value.find('\0') != std::string_view::npos;
}

void AppendJsonString(std::ostringstream& output, std::string_view value) {
    static constexpr char hex[] = "0123456789abcdef";
    output << '"';
    for (const unsigned char character : value) {
        switch (character) {
        case '"':
            output << "\\\"";
            break;
        case '\\':
            output << "\\\\";
            break;
        case '\b':
            output << "\\b";
            break;
        case '\f':
            output << "\\f";
            break;
        case '\n':
            output << "\\n";
            break;
        case '\r':
            output << "\\r";
            break;
        case '\t':
            output << "\\t";
            break;
        default:
            if (character < 0x20U) {
                output << "\\u00"
                       << hex[(character >> 4U) & 0x0fU]
                       << hex[character & 0x0fU];
            } else {
                output << static_cast<char>(character);
            }
            break;
        }
    }
    output << '"';
}

void AppendMessageList(std::ostringstream& output,
                       const std::vector<MessageKey>& messages) {
    output << '[';
    for (std::size_t index = 0U; index < messages.size(); ++index) {
        if (index != 0U) {
            output << ',';
        }
        const MessageKey& key = messages[index];
        output << "{\"message_id\":" << key.message_id
               << ",\"service_id\":" << static_cast<unsigned>(key.service_id)
               << ",\"service_version\":" << key.service_version << '}';
    }
    output << ']';
}

}  // namespace

IngressConfig DefaultIngressConfig(IngressKind kind) {
    IngressConfig config;
    config.kind = kind;
    const IngressSpec& spec = GetIngressSpec(kind);
    config.work_threads = spec.default_work_threads;
    config.io_threads = spec.default_io_threads;
    return config;
}

std::string ValidateIngressConfig(const IngressConfig& config) {
    const std::string manifest_error = ValidateIngressSpecs();
    if (!manifest_error.empty()) {
        return "invalid built-in subscription manifest: " + manifest_error;
    }
    try {
        static_cast<void>(GetIngressSpec(config.kind));
    } catch (...) {
        return "unknown ingress kind";
    }
    if (config.endpoint.name.empty() ||
        config.endpoint.resolved_server_address.empty()) {
        return "endpoint contract name and resolved address are required";
    }
    if (ContainsNul(config.endpoint.name) ||
        ContainsNul(config.endpoint.resolved_server_address)) {
        return "endpoint contract text must not contain NUL";
    }
    if (!IsLowerHexSha256(config.endpoint.contract_sha256)) {
        return "endpoint contract hash must be 64 lowercase hexadecimal digits";
    }
    if (!config.endpoint.send_mac_auth.has_value()) {
        return "send_mac_auth must be supplied by the endpoint contract";
    }
    if (!IsSupportedMessageEncoding(
            config.endpoint.message_encoding)) {
        return "message encoding must be an approved explicit enum value";
    }
    if (config.credential_name.empty() ||
        config.credential_name == "." ||
        config.credential_name == ".." ||
        config.credential_name.find('/') != std::string::npos ||
        ContainsNul(config.credential_name) ||
        config.token.empty() ||
        ContainsNul(config.token)) {
        return "credential name and non-empty runtime token are required";
    }
    if (config.sdk_log_prefix.empty() ||
        config.shadow_capture_path.empty() ||
        config.metrics_textfile_path.empty()) {
        return "SDK log, shadow capture, and metrics paths are required";
    }
    if (ContainsNul(config.sdk_log_prefix) ||
        ContainsNul(config.shadow_capture_path) ||
        ContainsNul(config.metrics_textfile_path)) {
        return "SDK log, shadow, and metrics paths must not contain NUL";
    }
    if (!std::filesystem::path(
            config.metrics_textfile_path).is_absolute()) {
        return "metrics textfile path must be absolute";
    }
    const std::filesystem::path metrics_path =
        std::filesystem::path(
            config.metrics_textfile_path).lexically_normal();
    const std::filesystem::path shadow_path =
        std::filesystem::path(
            config.shadow_capture_path).lexically_normal();
    const std::filesystem::path sdk_log_path =
        std::filesystem::path(
            config.sdk_log_prefix).lexically_normal();
    if (metrics_path == shadow_path ||
        metrics_path == sdk_log_path ||
        shadow_path == sdk_log_path) {
        return "SDK log, shadow, and metrics outputs must be distinct";
    }
    if (config.work_threads <= 0 || config.io_threads <= 0) {
        return "SDK thread counts must be positive";
    }
    if (config.heartbeat_interval_seconds == 0U ||
        config.heartbeat_timeout_seconds <=
            config.heartbeat_interval_seconds) {
        return "heartbeat timeout must exceed a non-zero interval";
    }
    if (config.max_message_bytes < kVendorHeadBytes) {
        return "max_message_bytes is smaller than the vendor head";
    }

    constexpr std::uint64_t commit_bytes = sizeof(std::uint32_t);
    constexpr std::uint64_t metadata_bytes =
        sizeof(l2flow::ingress::CaptureMetaV1);
    const std::uint64_t message_bytes = config.max_message_bytes;
    if (metadata_bytes >
        std::numeric_limits<std::uint64_t>::max() - message_bytes ||
        metadata_bytes + message_bytes >
            std::numeric_limits<std::uint64_t>::max() - commit_bytes) {
        return "maximum ring entry size overflows uint64";
    }
    const std::uint64_t maximum_entry =
        metadata_bytes + message_bytes + commit_bytes;
    if (maximum_entry >
        std::numeric_limits<std::uint32_t>::max()) {
        return "maximum ring entry exceeds the uint32 commit length";
    }
    if (config.ring_capacity_bytes < maximum_entry) {
        return "ring capacity cannot hold one maximum-size entry";
    }
    if (!IsGregorianDate(config.capture_date)) {
        return "capture_date must be a valid Gregorian YYYYMMDD date";
    }
    if (config.first_ingress_sequence == 0U ||
        config.first_ingress_sequence ==
            std::numeric_limits<std::uint64_t>::max()) {
        return "first ingress sequence must be below the exhaustion sentinel";
    }
    return {};
}

std::string CanonicalIngressConfig(const IngressConfig& config) {
    const IngressSpec& spec = GetIngressSpec(config.kind);
    std::ostringstream output;
    output << "{\"capture_date\":" << config.capture_date
           << ",\"credential_name\":";
    AppendJsonString(output, config.credential_name);
    output << ",\"endpoint\":{\"contract_sha256\":";
    AppendJsonString(output, config.endpoint.contract_sha256);
    output << ",\"merge_message\":"
           << (config.endpoint.merge_message ? "true" : "false")
           << ",\"message_encoding\":"
           << static_cast<int>(config.endpoint.message_encoding)
           << ",\"name\":";
    AppendJsonString(output, config.endpoint.name);
    output << ",\"resolved_server_address\":";
    AppendJsonString(output, config.endpoint.resolved_server_address);
    output << ",\"send_mac_auth\":";
    if (config.endpoint.send_mac_auth.has_value()) {
        output << (*config.endpoint.send_mac_auth ? "true" : "false");
    } else {
        output << "null";
    }
    output << ",\"server_select\":"
           << (config.endpoint.server_select ? "true" : "false")
           << "},\"first_ingress_sequence\":"
           << config.first_ingress_sequence
           << ",\"heartbeat_interval_seconds\":"
           << config.heartbeat_interval_seconds
           << ",\"heartbeat_timeout_seconds\":"
           << config.heartbeat_timeout_seconds
           << ",\"include_optional_index\":"
           << (config.include_optional_index ? "true" : "false")
           << ",\"io_threads\":" << config.io_threads
           << ",\"max_message_bytes\":" << config.max_message_bytes
           << ",\"metrics_textfile_path\":";
    AppendJsonString(output, config.metrics_textfile_path);
    output << ",\"ring_capacity_bytes\":" << config.ring_capacity_bytes
           << ",\"sdk_log_prefix\":";
    AppendJsonString(output, config.sdk_log_prefix);
    output << ",\"service_name\":";
    AppendJsonString(output, spec.service_name);
    output << ",\"shadow_capture_path\":";
    AppendJsonString(output, config.shadow_capture_path);
    output << ",\"source_stream_id\":" << spec.source_stream_id
           << ",\"subscriptions\":{\"forbidden\":";
    AppendMessageList(output, spec.forbidden);
    output << ",\"optional\":";
    AppendMessageList(output, spec.optional);
    output << ",\"required\":";
    AppendMessageList(output, spec.required);
    output << "},\"work_threads\":" << config.work_threads << '}';
    return output.str();
}

std::string IngressConfigSha256(const IngressConfig& config) {
    const std::string canonical = CanonicalIngressConfig(config);
    return l2flow::common::Sha256Hex(
        l2flow::common::ComputeSha256(canonical));
}

std::optional<std::uint64_t> RecommendedRingCapacity(
    std::uint64_t peak_bytes_per_second,
    std::uint64_t stall_seconds,
    std::uint64_t minimum_bytes) noexcept {
    if (peak_bytes_per_second == 0U || stall_seconds == 0U ||
        minimum_bytes == 0U) {
        return std::nullopt;
    }
    constexpr std::uint64_t maximum =
        std::numeric_limits<std::uint64_t>::max();
    if (peak_bytes_per_second > maximum / stall_seconds) {
        return std::nullopt;
    }
    const std::uint64_t base = peak_bytes_per_second * stall_seconds;
    const std::uint64_t rounded_half =
        base / 2U + (base % 2U == 0U ? 0U : 1U);
    if (base > maximum - rounded_half) {
        return std::nullopt;
    }
    const std::uint64_t required = base + rounded_half;
    return std::max(minimum_bytes, required);
}

}  // namespace l2flow::sdk
