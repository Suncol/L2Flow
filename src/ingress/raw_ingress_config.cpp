#include "l2flow/ingress/raw_ingress_config.h"

#include "l2flow/ingress/capture_meta.h"
#include "l2flow/ingress/raw_schema.h"
#include "l2flow/ingress/raw_v1.h"

#include <algorithm>
#include <charconv>
#include <filesystem>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <vector>

namespace l2flow::ingress {
namespace {

constexpr std::size_t kMaximumConfigTextBytes = 4096U;
constexpr std::size_t kMaximumReserveDomainBytes = 128U;
constexpr std::size_t kMaximumUnixSocketPathBytes = 107U;

bool IsGregorianDate(std::uint32_t value) noexcept {
    if (value < 10000101U || value > 99991231U) {
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
        year % 4U == 0U &&
        (year % 100U != 0U || year % 400U == 0U);
    if (month == 2U && leap) {
        maximum = 29U;
    }
    return day != 0U && day <= maximum;
}

bool IsLowerHexSha256(std::string_view text) noexcept {
    return text.size() == 64U &&
           std::all_of(
               text.begin(), text.end(),
               [](unsigned char value) noexcept {
                   return (value >= static_cast<unsigned char>('0') &&
                           value <= static_cast<unsigned char>('9')) ||
                          (value >= static_cast<unsigned char>('a') &&
                           value <= static_cast<unsigned char>('f'));
               });
}

bool IsContinuationByte(unsigned char value) noexcept {
    return (value & 0xc0U) == 0x80U;
}

bool IsValidUtf8(std::string_view text) noexcept {
    std::size_t index = 0U;
    while (index < text.size()) {
        const unsigned char first =
            static_cast<unsigned char>(text[index]);
        if (first <= 0x7fU) {
            ++index;
            continue;
        }
        if (first >= 0xc2U && first <= 0xdfU) {
            if (index + 1U >= text.size() ||
                !IsContinuationByte(
                    static_cast<unsigned char>(text[index + 1U]))) {
                return false;
            }
            index += 2U;
            continue;
        }
        if (first >= 0xe0U && first <= 0xefU) {
            if (index + 2U >= text.size()) {
                return false;
            }
            const unsigned char second =
                static_cast<unsigned char>(text[index + 1U]);
            const unsigned char third =
                static_cast<unsigned char>(text[index + 2U]);
            if (!IsContinuationByte(second) ||
                !IsContinuationByte(third) ||
                (first == 0xe0U && second < 0xa0U) ||
                (first == 0xedU && second >= 0xa0U)) {
                return false;
            }
            index += 3U;
            continue;
        }
        if (first >= 0xf0U && first <= 0xf4U) {
            if (index + 3U >= text.size()) {
                return false;
            }
            const unsigned char second =
                static_cast<unsigned char>(text[index + 1U]);
            const unsigned char third =
                static_cast<unsigned char>(text[index + 2U]);
            const unsigned char fourth =
                static_cast<unsigned char>(text[index + 3U]);
            if (!IsContinuationByte(second) ||
                !IsContinuationByte(third) ||
                !IsContinuationByte(fourth) ||
                (first == 0xf0U && second < 0x90U) ||
                (first == 0xf4U && second >= 0x90U)) {
                return false;
            }
            index += 4U;
            continue;
        }
        return false;
    }
    return true;
}

bool IsSafeConfigText(std::string_view value,
                      std::size_t maximum_bytes) noexcept {
    if (value.empty() || value.size() > maximum_bytes ||
        !IsValidUtf8(value)) {
        return false;
    }
    return std::none_of(
        value.begin(), value.end(),
        [](unsigned char character) noexcept {
            return character < 0x20U || character == 0x7fU;
        });
}

std::string ValidateAbsoluteNormalizedPath(
    std::string_view text,
    std::size_t maximum_bytes,
    std::string_view label) {
    if (!IsSafeConfigText(text, maximum_bytes)) {
        return std::string(label) +
               " must be non-empty bounded UTF-8 without control bytes";
    }
    const std::filesystem::path path{std::string(text)};
    if (!path.is_absolute()) {
        return std::string(label) + " must be absolute";
    }
    const std::string normalized =
        path.lexically_normal().generic_string();
    if (normalized != text || normalized == "/") {
        return std::string(label) +
               " must be a normalized non-root path";
    }
    return {};
}

bool IsSafeComponent(std::string_view value) noexcept {
    if (!IsSafeConfigText(value, kMaximumReserveDomainBytes) ||
        value == "." || value == "..") {
        return false;
    }
    return std::all_of(
        value.begin(), value.end(),
        [](unsigned char character) noexcept {
            return (character >= static_cast<unsigned char>('a') &&
                    character <= static_cast<unsigned char>('z')) ||
                   (character >= static_cast<unsigned char>('A') &&
                    character <= static_cast<unsigned char>('Z')) ||
                   (character >= static_cast<unsigned char>('0') &&
                    character <= static_cast<unsigned char>('9')) ||
                   character == static_cast<unsigned char>('.') ||
                   character == static_cast<unsigned char>('_') ||
                   character == static_cast<unsigned char>('-');
        });
}

bool IsZeroDigest(
    const l2flow::common::Sha256Digest& digest) noexcept {
    return std::all_of(
        digest.begin(), digest.end(),
        [](std::byte value) noexcept {
            return value == std::byte{0};
        });
}

std::uint64_t DigestLabel(
    const l2flow::common::Sha256Digest& digest) noexcept {
    std::uint64_t label = 0U;
    for (std::size_t index = 0U; index < 8U; ++index) {
        label =
            (label << 8U) |
            std::to_integer<std::uint64_t>(digest[index]);
    }
    return label;
}

bool MaximumRawRecordBytes(
    std::uint32_t max_message_bytes,
    std::uint64_t* result) noexcept {
    if (result == nullptr ||
        max_message_bytes < kVendorMessageHeadBytes) {
        return false;
    }
    constexpr std::uint64_t header = kRawV1RecordHeaderBytes;
    constexpr std::uint64_t trailer = kRawV1RecordTrailerBytes;
    const std::uint64_t payload = max_message_bytes;
    if (payload >
        std::numeric_limits<std::uint64_t>::max() - header) {
        return false;
    }
    const std::uint64_t before_padding = header + payload;
    const std::uint64_t remainder =
        before_padding % kRawV1RecordAlignment;
    const std::uint64_t padding =
        remainder == 0U ? 0U : kRawV1RecordAlignment - remainder;
    if (before_padding >
            std::numeric_limits<std::uint64_t>::max() - padding ||
        before_padding + padding >
            std::numeric_limits<std::uint64_t>::max() - trailer) {
        return false;
    }
    const std::uint64_t total =
        before_padding + padding + trailer;
    if (total > std::numeric_limits<std::uint32_t>::max()) {
        return false;
    }
    *result = total;
    return true;
}

template <typename Integer>
void AppendInteger(std::string* output, Integer value) {
    static_assert(std::is_integral_v<Integer>);
    char bytes[32]{};
    const auto converted =
        std::to_chars(std::begin(bytes), std::end(bytes), value);
    if (converted.ec != std::errc{}) {
        throw std::runtime_error("integer canonicalization failed");
    }
    output->append(bytes, converted.ptr);
}

void AppendJsonString(std::string* output, std::string_view value) {
    static constexpr char hex[] = "0123456789abcdef";
    output->push_back('"');
    for (const unsigned char character : value) {
        switch (character) {
        case '"':
            output->append("\\\"");
            break;
        case '\\':
            output->append("\\\\");
            break;
        case '\b':
            output->append("\\b");
            break;
        case '\f':
            output->append("\\f");
            break;
        case '\n':
            output->append("\\n");
            break;
        case '\r':
            output->append("\\r");
            break;
        case '\t':
            output->append("\\t");
            break;
        default:
            if (character < 0x20U) {
                output->append("\\u00");
                output->push_back(hex[(character >> 4U) & 0x0fU]);
                output->push_back(hex[character & 0x0fU]);
            } else {
                output->push_back(static_cast<char>(character));
            }
            break;
        }
    }
    output->push_back('"');
}

void AppendBool(std::string* output, bool value) {
    output->append(value ? "true" : "false");
}

using MessageSortKey =
    std::tuple<std::uint8_t, std::uint16_t, std::uint16_t>;

MessageSortKey SortKey(
    const l2flow::sdk::MessageKey& key) noexcept {
    return {
        key.service_id,
        key.service_version,
        key.message_id,
    };
}

void AppendMessages(
    std::string* output,
    const std::vector<l2flow::sdk::MessageKey>& input) {
    std::vector<l2flow::sdk::MessageKey> messages = input;
    std::sort(
        messages.begin(), messages.end(),
        [](const l2flow::sdk::MessageKey& left,
           const l2flow::sdk::MessageKey& right) noexcept {
            return SortKey(left) < SortKey(right);
        });
    output->push_back('[');
    for (std::size_t index = 0U; index < messages.size(); ++index) {
        if (index != 0U) {
            output->push_back(',');
        }
        const l2flow::sdk::MessageKey& key = messages[index];
        output->append("{\"message_id\":");
        AppendInteger(output, key.message_id);
        output->append(",\"service_id\":");
        AppendInteger(output, key.service_id);
        output->append(",\"service_version\":");
        AppendInteger(output, key.service_version);
        output->push_back('}');
    }
    output->push_back(']');
}

}  // namespace

RawIngressConfig DefaultRawIngressConfig(
    l2flow::sdk::IngressKind kind) {
    RawIngressConfig config;
    config.kind = kind;
    const l2flow::sdk::IngressSpec& spec =
        l2flow::sdk::GetIngressSpec(kind);
    config.work_threads = spec.default_work_threads;
    config.io_threads = spec.default_io_threads;
    config.raw_schema_sha256 =
        std::string(kFrozenRawSchemaSha256Hex);
    return config;
}

std::string ValidateRawIngressConfig(
    const RawIngressConfig& config) {
    const std::string manifest_error =
        l2flow::sdk::ValidateIngressSpecs();
    if (!manifest_error.empty()) {
        return "invalid built-in subscription manifest: " +
               manifest_error;
    }
    try {
        static_cast<void>(
            l2flow::sdk::GetIngressSpec(config.kind));
    } catch (...) {
        return "unknown ingress kind";
    }
    if (!IsLowerHexSha256(
            config.endpoint_contract_sha256)) {
        return "endpoint contract hash must be 64 lowercase hexadecimal digits";
    }
    if (!IsSafeComponent(config.credential_name)) {
        return "credential name must be a safe non-empty component";
    }
    if (!IsSafeConfigText(
            config.sdk_log_prefix,
            kMaximumConfigTextBytes)) {
        return "SDK log prefix must be bounded UTF-8 without control bytes";
    }
    std::string path_error = ValidateAbsoluteNormalizedPath(
        config.metrics_textfile_path,
        kMaximumConfigTextBytes,
        "metrics textfile path");
    if (!path_error.empty()) {
        return path_error;
    }
    if (config.work_threads <= 0 ||
        config.io_threads <= 0) {
        return "SDK thread counts must be positive";
    }
    if (config.heartbeat_interval_seconds == 0U ||
        config.heartbeat_timeout_seconds <=
            config.heartbeat_interval_seconds) {
        return "heartbeat timeout must exceed a non-zero interval";
    }

    std::uint64_t maximum_raw_record = 0U;
    if (!MaximumRawRecordBytes(
            config.max_message_bytes,
            &maximum_raw_record)) {
        return "max_message_bytes cannot be represented by Raw V1";
    }
    constexpr std::uint64_t ring_metadata_bytes =
        sizeof(CaptureMetaV1);
    constexpr std::uint64_t ring_commit_bytes =
        sizeof(std::uint32_t);
    const std::uint64_t maximum_ring_entry =
        ring_metadata_bytes +
        static_cast<std::uint64_t>(config.max_message_bytes) +
        ring_commit_bytes;
    if (maximum_ring_entry >
        std::numeric_limits<std::uint32_t>::max()) {
        return "maximum ring entry exceeds the uint32 commit length";
    }
    if (config.ring_capacity_bytes < maximum_ring_entry ||
        config.ring_capacity_bytes >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max())) {
        return "ring capacity cannot be represented or hold one maximum entry";
    }
    if (config.ring_stall_budget_seconds == 0U) {
        return "ring stall budget must be non-zero";
    }

    path_error = ValidateAbsoluteNormalizedPath(
        config.raw_root,
        kMaximumConfigTextBytes,
        "Raw root");
    if (!path_error.empty()) {
        return path_error;
    }
    if (config.raw_format_version !=
        kRawV1FormatVersion) {
        return "only Raw format V1 is implemented";
    }
    if (config.raw_schema_sha256 !=
        kFrozenRawSchemaSha256Hex) {
        return "Raw schema hash does not match the frozen Raw V1 schema";
    }
    if (maximum_raw_record >
            std::numeric_limits<std::uint64_t>::max() -
                kRawV1SegmentHeaderBytes ||
        config.segment_target_bytes <
            kRawV1SegmentHeaderBytes + maximum_raw_record ||
        config.segment_target_bytes >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::int64_t>::max())) {
        return "segment target cannot hold one maximum Raw record in signed file offsets";
    }
    if (config.segment_max_age_seconds == 0U) {
        return "segment maximum age must be non-zero";
    }
    const std::uint64_t segment_data_capacity =
        config.segment_target_bytes -
        kRawV1SegmentHeaderBytes;
    if (config.sync_interval_milliseconds == 0U ||
        config.sync_bytes == 0U ||
        config.sync_bytes > segment_data_capacity) {
        return "sync interval/bytes must be non-zero and fit segment data capacity";
    }
    if (config.sparse_index_every_records == 0U ||
        config.sparse_index_every_bytes == 0U ||
        config.sparse_index_every_bytes >
            segment_data_capacity) {
        return "sparse-index thresholds must be non-zero and fit segment data capacity";
    }

    if (!IsSafeComponent(config.reserve_domain_id)) {
        return "reserve domain id must be a safe non-empty component";
    }
    path_error = ValidateAbsoluteNormalizedPath(
        config.reserve_coordinator_socket,
        kMaximumUnixSocketPathBytes,
        "reserve coordinator socket");
    if (!path_error.empty()) {
        return path_error;
    }
    if (config.reserve_ack_timeout_milliseconds == 0U) {
        return "reserve ACK timeout must be non-zero";
    }
    if (config.emergency_reserve_bytes == 0U) {
        return "shared emergency reserve bytes must be non-zero";
    }
    if (config.clock_epoch_algorithm_version != 1U) {
        return "only clock epoch algorithm V1 is implemented";
    }
    if (!IsSafeConfigText(
            config.canonical_clock_source_config,
            kMaximumConfigTextBytes)) {
        return "canonical clock source config must be bounded UTF-8 without control bytes";
    }
    return {};
}

std::string ValidateRawIngressProcessInputs(
    const RawIngressProcessInputs& inputs) {
    const std::string path_error =
        ValidateAbsoluteNormalizedPath(
            inputs.endpoint_contract_path,
            kMaximumConfigTextBytes,
            "endpoint contract path");
    if (!path_error.empty()) {
        return path_error;
    }
    if (inputs.credential_token.empty() ||
        inputs.credential_token.find('\0') !=
            std::string::npos) {
        return "non-empty runtime credential token without NUL is required";
    }
    if (!IsGregorianDate(
            inputs.configured_capture_date)) {
        return "configured capture date must be a valid Gregorian YYYYMMDD date";
    }
    return {};
}

std::string ValidateRawIngressRuntimeState(
    const RawIngressConfig& config,
    const RawIngressRuntimeState& state) {
    const std::string config_error =
        ValidateRawIngressConfig(config);
    if (!config_error.empty()) {
        return "stable config is invalid: " + config_error;
    }
    const l2flow::sdk::IngressSpec& spec =
        l2flow::sdk::GetIngressSpec(config.kind);
    if (state.source_stream_id != spec.source_stream_id) {
        return "runtime source stream does not match stable config";
    }
    if (!IsGregorianDate(state.capture_date)) {
        return "runtime capture date must be a valid Gregorian YYYYMMDD date";
    }
    if (l2flow::common::IsZeroIdentity(
            state.stream_day_id) ||
        l2flow::common::IsZeroIdentity(
            state.writer_instance)) {
        return "runtime stream-day and writer identities must be non-zero";
    }
    if (state.current_segment_sequence == 0U ||
        state.append.segment_sequence !=
            state.current_segment_sequence ||
        state.durable.segment_sequence == 0U ||
        state.durable.segment_sequence >
            state.append.segment_sequence) {
        return "runtime segment sequence state is inconsistent";
    }
    if (state.append.segment_offset <
            kRawV1SegmentHeaderBytes ||
        state.durable.segment_offset <
            kRawV1SegmentHeaderBytes ||
        state.append.global_wal_pos <
            state.append.segment_offset ||
        state.durable.global_wal_pos <
            state.durable.segment_offset ||
        state.durable.global_wal_pos >
            state.append.global_wal_pos ||
        state.durable.ingress_sequence >
            state.append.ingress_sequence ||
        (state.durable.segment_sequence ==
             state.append.segment_sequence &&
         state.durable.segment_offset >
             state.append.segment_offset)) {
        return "runtime durable cursor exceeds append cursor";
    }
    if (state.durable.segment_sequence ==
            state.append.segment_sequence &&
        state.durable.global_wal_pos -
                state.durable.segment_offset !=
            state.append.global_wal_pos -
                state.append.segment_offset) {
        return "runtime cursors disagree on current segment base";
    }
    if (state.durable.global_wal_pos ==
            state.append.global_wal_pos &&
        (state.durable.segment_sequence !=
             state.append.segment_sequence ||
         state.durable.segment_offset !=
             state.append.segment_offset ||
         state.durable.ingress_sequence !=
             state.append.ingress_sequence)) {
        return "equal runtime WAL positions must have identical cursors";
    }
    if (state.append.ingress_sequence ==
            std::numeric_limits<std::uint64_t>::max() ||
        state.recovered_next_ingress_sequence !=
            state.append.ingress_sequence + 1U) {
        return "recovered next ingress sequence must follow append ingress";
    }
    if (state.clock_epoch_algorithm_version !=
            config.clock_epoch_algorithm_version ||
        IsZeroDigest(state.clock_epoch_digest) ||
        state.clock_epoch_label !=
            DigestLabel(state.clock_epoch_digest)) {
        return "runtime clock epoch identity is inconsistent";
    }
    return {};
}

std::string ValidateRawIngressResumeConnectInputs(
    const RawIngressConfig& config,
    const RawIngressProcessInputs& inputs,
    const RawIngressRuntimeState& state) {
    const std::string process_error =
        ValidateRawIngressProcessInputs(inputs);
    if (!process_error.empty()) {
        return "process inputs are invalid: " +
               process_error;
    }
    const std::string runtime_error =
        ValidateRawIngressRuntimeState(config, state);
    if (!runtime_error.empty()) {
        return "runtime state is invalid: " +
               runtime_error;
    }
    if (inputs.configured_capture_date !=
        state.capture_date) {
        return "RESUME_CONNECT runtime date does not match the configured capture date";
    }
    return {};
}

std::string CanonicalRawIngressConfig(
    const RawIngressConfig& config) {
    const std::string validation_error =
        ValidateRawIngressConfig(config);
    if (!validation_error.empty()) {
        throw std::invalid_argument(validation_error);
    }
    const l2flow::sdk::IngressSpec& spec =
        l2flow::sdk::GetIngressSpec(config.kind);

    std::string output;
    output.reserve(2048U);
    output.append("{\"clock\":{\"algorithm_version\":");
    AppendInteger(
        &output, config.clock_epoch_algorithm_version);
    output.append(",\"source_config\":");
    AppendJsonString(
        &output, config.canonical_clock_source_config);
    output.append("},\"config_schema\":\"l2flow.raw-ingress.stable.v1\"");
    output.append(",\"credential_name\":");
    AppendJsonString(&output, config.credential_name);
    output.append(",\"endpoint_contract_sha256\":");
    AppendJsonString(
        &output, config.endpoint_contract_sha256);
    output.append(",\"heartbeat\":{\"interval_seconds\":");
    AppendInteger(
        &output, config.heartbeat_interval_seconds);
    output.append(",\"timeout_seconds\":");
    AppendInteger(
        &output, config.heartbeat_timeout_seconds);
    output.append("},\"index\":{\"every_bytes\":");
    AppendInteger(
        &output, config.sparse_index_every_bytes);
    output.append(",\"every_records\":");
    AppendInteger(
        &output, config.sparse_index_every_records);
    output.append("},\"metrics_textfile_path\":");
    AppendJsonString(
        &output, config.metrics_textfile_path);
    output.append(",\"raw\":{\"format_version\":");
    AppendInteger(&output, config.raw_format_version);
    output.append(",\"root\":");
    AppendJsonString(&output, config.raw_root);
    output.append(",\"schema_sha256\":");
    AppendJsonString(&output, config.raw_schema_sha256);
    output.append("},\"reserve\":{\"ack_timeout_ms\":");
    AppendInteger(
        &output, config.reserve_ack_timeout_milliseconds);
    output.append(",\"coordinator_socket\":");
    AppendJsonString(
        &output, config.reserve_coordinator_socket);
    output.append(",\"domain_id\":");
    AppendJsonString(&output, config.reserve_domain_id);
    output.append(",\"emergency_reserve_bytes\":");
    AppendInteger(
        &output, config.emergency_reserve_bytes);
    output.append("},\"ring\":{\"capacity_bytes\":");
    AppendInteger(
        &output, config.ring_capacity_bytes);
    output.append(",\"max_message_bytes\":");
    AppendInteger(
        &output, config.max_message_bytes);
    output.append(",\"stall_budget_seconds\":");
    AppendInteger(
        &output, config.ring_stall_budget_seconds);
    output.append("},\"sdk\":{\"io_threads\":");
    AppendInteger(&output, config.io_threads);
    output.append(",\"log_prefix\":");
    AppendJsonString(&output, config.sdk_log_prefix);
    output.append(",\"work_threads\":");
    AppendInteger(&output, config.work_threads);
    output.append("},\"segment\":{\"max_age_seconds\":");
    AppendInteger(
        &output, config.segment_max_age_seconds);
    output.append(",\"target_bytes\":");
    AppendInteger(
        &output, config.segment_target_bytes);
    output.append("},\"service_name\":");
    AppendJsonString(&output, spec.service_name);
    output.append(",\"source_stream_id\":");
    AppendInteger(&output, spec.source_stream_id);
    output.append(",\"subscriptions\":{\"forbidden\":");
    AppendMessages(&output, spec.forbidden);
    output.append(",\"include_optional\":");
    AppendBool(
        &output, config.include_optional_index);
    output.append(",\"optional\":");
    AppendMessages(&output, spec.optional);
    output.append(",\"required\":");
    AppendMessages(&output, spec.required);
    output.append("},\"sync\":{\"bytes\":");
    AppendInteger(&output, config.sync_bytes);
    output.append(",\"interval_ms\":");
    AppendInteger(
        &output, config.sync_interval_milliseconds);
    output.append("}}");
    return output;
}

std::string RawIngressConfigSha256(
    const RawIngressConfig& config) {
    const std::string canonical =
        CanonicalRawIngressConfig(config);
    return l2flow::common::Sha256Hex(
        l2flow::common::ComputeSha256(canonical));
}

}  // namespace l2flow::ingress
