#include "l2flow/ingress/run_manifest_v1.h"

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <new>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace l2flow::ingress {
namespace {

inline constexpr std::size_t kMaximumShortStringBytes = 4096U;
inline constexpr std::size_t kMaximumLocatorBytes = 1024U;

struct EncodedSizeExceeded final {};

template <std::size_t Size>
[[nodiscard]] bool IsZero(
    const std::array<std::byte, Size>& value) noexcept {
    return std::all_of(
        value.begin(),
        value.end(),
        [](std::byte byte) {
            return byte == std::byte{0};
        });
}

[[nodiscard]] bool IsGregorianDate(
    std::uint32_t value) noexcept {
    if (value < 10000101U || value > 99991231U) {
        return false;
    }
    const std::uint32_t year = value / 10000U;
    const std::uint32_t month =
        (value / 100U) % 100U;
    const std::uint32_t day = value % 100U;
    if (month == 0U || month > 12U || day == 0U) {
        return false;
    }
    constexpr std::array<std::uint8_t, 12U> days{
        31U, 28U, 31U, 30U, 31U, 30U,
        31U, 31U, 30U, 31U, 30U, 31U};
    std::uint32_t maximum = days[month - 1U];
    const bool leap =
        year % 4U == 0U &&
        (year % 100U != 0U || year % 400U == 0U);
    if (month == 2U && leap) {
        maximum = 29U;
    }
    return day <= maximum;
}

[[nodiscard]] bool IsValidUtf8(
    std::string_view text) noexcept {
    std::size_t index = 0U;
    while (index < text.size()) {
        const std::uint8_t first =
            static_cast<std::uint8_t>(
                static_cast<unsigned char>(text[index]));
        if (first <= 0x7fU) {
            ++index;
            continue;
        }
        std::size_t continuation_count = 0U;
        std::uint8_t second_min = 0x80U;
        std::uint8_t second_max = 0xbfU;
        if (first >= 0xc2U && first <= 0xdfU) {
            continuation_count = 1U;
        } else if (first == 0xe0U) {
            continuation_count = 2U;
            second_min = 0xa0U;
        } else if (first >= 0xe1U && first <= 0xecU) {
            continuation_count = 2U;
        } else if (first == 0xedU) {
            continuation_count = 2U;
            second_max = 0x9fU;
        } else if (first >= 0xeeU && first <= 0xefU) {
            continuation_count = 2U;
        } else if (first == 0xf0U) {
            continuation_count = 3U;
            second_min = 0x90U;
        } else if (first >= 0xf1U && first <= 0xf3U) {
            continuation_count = 3U;
        } else if (first == 0xf4U) {
            continuation_count = 3U;
            second_max = 0x8fU;
        } else {
            return false;
        }
        if (continuation_count >
            text.size() - index - 1U) {
            return false;
        }
        const std::uint8_t second =
            static_cast<std::uint8_t>(
                static_cast<unsigned char>(
                    text[index + 1U]));
        if (second < second_min || second > second_max) {
            return false;
        }
        for (std::size_t offset = 2U;
             offset <= continuation_count;
             ++offset) {
            const std::uint8_t continuation =
                static_cast<std::uint8_t>(
                    static_cast<unsigned char>(
                        text[index + offset]));
            if (continuation < 0x80U ||
                continuation > 0xbfU) {
                return false;
            }
        }
        index += continuation_count + 1U;
    }
    return true;
}

[[nodiscard]] bool ValidString(
    std::string_view text,
    std::size_t maximum = kMaximumShortStringBytes,
    bool allow_empty = false) noexcept {
    return (allow_empty || !text.empty()) &&
           text.size() <= maximum &&
           text.find('\0') == std::string_view::npos &&
           IsValidUtf8(text);
}

[[nodiscard]] bool NonzeroOptionalDigest(
    const std::optional<RawV1Digest>& value) noexcept {
    return !value.has_value() || !IsZero(*value);
}

[[nodiscard]] bool SameNamespace(
    const RunManifestRawInputV1& left,
    const RunManifestRawInputV1& right) noexcept {
    return left.source_stream_id == right.source_stream_id &&
           left.capture_date == right.capture_date &&
           left.stream_day_id == right.stream_day_id;
}

[[nodiscard]] std::uint64_t ClockEpochLabel(
    const RawV1Digest& digest) noexcept {
    std::uint64_t value = 0U;
    for (std::size_t index = 0U; index < 8U; ++index) {
        value |= static_cast<std::uint64_t>(
                     std::to_integer<std::uint8_t>(
                         digest[index]))
                 << (8U * index);
    }
    return value;
}

[[nodiscard]] bool MatchesFinalizationReportLocator(
    std::string_view locator,
    const RawV1Identity& cycle) noexcept {
    constexpr std::string_view prefix =
        "maintenance/finalization-";
    constexpr std::string_view suffix = ".json";
    constexpr std::string_view hex =
        "0123456789abcdef";
    if (locator.size() !=
            prefix.size() + (cycle.size() * 2U) +
                suffix.size() ||
        !locator.starts_with(prefix) ||
        !locator.ends_with(suffix)) {
        return false;
    }
    std::size_t cursor = prefix.size();
    for (const std::byte byte : cycle) {
        const std::uint8_t octet =
            std::to_integer<std::uint8_t>(byte);
        if (locator[cursor] !=
                hex[(octet >> 4U) & 0x0fU] ||
            locator[cursor + 1U] !=
                hex[octet & 0x0fU]) {
            return false;
        }
        cursor += 2U;
    }
    return true;
}

[[nodiscard]] bool ValidateReserveFinalization(
    const RunManifestReserveFinalizationV1& value,
    const DurableMarkerV1& marker) noexcept {
    const bool have_identity =
        !IsZero(value.reserve_state_uuid) ||
        !IsZero(value.finalization_cycle_id) ||
        !IsZero(value.immutable_grant_sha256);
    const bool have_reference =
        value.maintenance_report_locator.has_value() ||
        value.maintenance_report_sha256.has_value() ||
        value.finalization_archive_locator.has_value() ||
        value.finalization_archive_manifest_sha256.has_value() ||
        !value.continuation_segment_sequences.empty();
    if (!have_identity && !have_reference) {
        return true;
    }
    if (IsZero(value.reserve_state_uuid) ||
        IsZero(value.finalization_cycle_id) ||
        IsZero(value.immutable_grant_sha256) ||
        !value.maintenance_report_locator.has_value() ||
        !value.maintenance_report_sha256.has_value() ||
        IsZero(*value.maintenance_report_sha256) ||
        !ValidString(
            *value.maintenance_report_locator,
            kMaximumLocatorBytes) ||
        !MatchesFinalizationReportLocator(
            *value.maintenance_report_locator,
            value.finalization_cycle_id)) {
        return false;
    }
    if (value.finalization_archive_locator.has_value() !=
            value.finalization_archive_manifest_sha256
                .has_value() ||
        (value.finalization_archive_locator.has_value() &&
         (!ValidString(
              *value.finalization_archive_locator,
              kMaximumLocatorBytes) ||
          IsZero(
              *value
                   .finalization_archive_manifest_sha256)))) {
        return false;
    }
    if (value.continuation_segment_sequences.size() >
        kRunManifestV1MaximumContinuationSegments) {
        return false;
    }
    if (!value.continuation_segment_sequences.empty() &&
        (value.continuation_segment_sequences.front() == 0U ||
         value.continuation_segment_sequences.front() !=
             marker.segment_sequence)) {
        return false;
    }
    return marker.marker_flags == kRawV1SegmentSealed;
}

[[nodiscard]] RunManifestV1Error ValidateMarker(
    const RunManifestRawInputV1& input) noexcept {
    DurableMarkerV1 decoded{};
    RawV1DurableMarkerWire canonical{};
    if (DecodeDurableMarkerV1(
            input.durable_marker_bytes,
            &decoded) != RawV1Error::kNone ||
        EncodeDurableMarkerV1(
            decoded, &canonical) != RawV1Error::kNone ||
        canonical != input.durable_marker_bytes) {
        return RunManifestV1Error::kInvalidMarker;
    }
    if (decoded.source_stream_id !=
            input.source_stream_id ||
        decoded.segment_sequence !=
            input.durable_marker.segment_sequence ||
        decoded.durable_global_wal_pos !=
            input.durable_marker
                .durable_global_wal_pos ||
        decoded.durable_ingress_sequence !=
            input.durable_marker
                .durable_ingress_sequence ||
        decoded.durable_segment_offset !=
            input.durable_marker
                .durable_segment_offset ||
        decoded.marker_flags !=
            input.durable_marker.marker_flags ||
        decoded.marker_crc32c !=
            input.durable_marker.marker_crc32c) {
        return RunManifestV1Error::kMarkerMismatch;
    }
    if (decoded.segment_sequence == 0U ||
        decoded.durable_segment_offset <
            kRawV1SegmentHeaderBytes ||
        decoded.durable_global_wal_pos <
            decoded.durable_segment_offset ||
        (decoded.marker_flags != 0U &&
         decoded.marker_flags !=
             kRawV1SegmentSealed)) {
        return RunManifestV1Error::kInvalidMarker;
    }
    return RunManifestV1Error::kNone;
}

[[nodiscard]] RunManifestV1Error ValidateInput(
    const RunManifestRawInputV1& input) noexcept {
    if (input.source_stream_id == 0U ||
        IsZero(input.stream_day_id)) {
        return RunManifestV1Error::kInvalidIdentity;
    }
    if (!IsGregorianDate(input.capture_date)) {
        return RunManifestV1Error::kInvalidDate;
    }
    if (IsZero(input.durable_journal_header_sha256)) {
        return RunManifestV1Error::kInvalidDigest;
    }
    if (input.durability_policy !=
            RunManifestDurabilityPolicyV1::
                kDurableOnly &&
        input.durability_policy !=
            RunManifestDurabilityPolicyV1::
                kIncludesRecoveredAppendOnly) {
        return RunManifestV1Error::
            kInvalidDurabilityPolicy;
    }
    const RunManifestV1Error marker_error =
        ValidateMarker(input);
    if (marker_error != RunManifestV1Error::kNone) {
        return marker_error;
    }
    if (input.range.has_value()) {
        const RunManifestRawRangeV1& range = *input.range;
        if (range.first_record_start_wal_pos >=
                range.last_record_end_wal_pos ||
            range.first_ingress_sequence == 0U ||
            range.first_ingress_sequence >
                range.last_ingress_sequence ||
            (input.durability_policy ==
                 RunManifestDurabilityPolicyV1::
                     kDurableOnly &&
             (range.last_record_end_wal_pos >
                  input.durable_marker
                      .durable_global_wal_pos ||
              range.last_ingress_sequence >
                  input.durable_marker
                      .durable_ingress_sequence))) {
            return RunManifestV1Error::kInvalidRange;
        }
    }
    if (input.durability_policy ==
            RunManifestDurabilityPolicyV1::
                kDurableOnly) {
        if (input.append_only_reason.has_value()) {
            return RunManifestV1Error::
                kInvalidDurabilityPolicy;
        }
    } else if (!input.range.has_value() ||
               !input.append_only_reason.has_value() ||
               !ValidString(
                   *input.append_only_reason) ||
               input.range->last_record_end_wal_pos <=
                   input.durable_marker
                       .durable_global_wal_pos ||
               input.range->last_ingress_sequence <=
                   input.durable_marker
                       .durable_ingress_sequence) {
        return RunManifestV1Error::
            kInvalidDurabilityPolicy;
    }

    if (input.segment_sha256.empty() ||
        input.segment_sha256.size() >
            kRunManifestV1MaximumSegmentsPerInput ||
        std::any_of(
            input.segment_sha256.begin(),
            input.segment_sha256.end(),
            [](const RawV1Digest& digest) {
                return IsZero(digest);
            })) {
        return RunManifestV1Error::kInvalidSegmentSet;
    }
    if (input.clock_epoch_transitions.size() >
        kRunManifestV1MaximumClockTransitionsPerInput) {
        return RunManifestV1Error::
            kInvalidClockTransitions;
    }
    std::uint64_t previous_transition = 0U;
    bool have_transition = false;
    for (const RunManifestClockTransitionV1& transition :
         input.clock_epoch_transitions) {
        if (transition.algorithm == 0U ||
            IsZero(transition.digest) ||
            (have_transition &&
             transition.record_start_wal_pos <=
                 previous_transition)) {
            return RunManifestV1Error::
                kInvalidClockTransitions;
        }
        have_transition = true;
        previous_transition =
            transition.record_start_wal_pos;
    }
    if (input.range.has_value()) {
        if (input.clock_epoch_transitions.empty() ||
            input.clock_epoch_transitions.front()
                    .record_start_wal_pos !=
                input.range
                    ->first_record_start_wal_pos ||
            input.clock_epoch_transitions.back()
                    .record_start_wal_pos >=
                input.range->last_record_end_wal_pos) {
            return RunManifestV1Error::
                kInvalidClockTransitions;
        }
    } else if (!input.clock_epoch_transitions.empty()) {
        return RunManifestV1Error::
            kInvalidClockTransitions;
    }

    const bool any_synthetic =
        input.synthetic_schema.has_value() ||
        input.parent_run_id.has_value() ||
        input.parent_raw_identity_sha256.has_value() ||
        input.fault_rule_sha256.has_value() ||
        input.fault_seed.has_value();
    if (!input.synthetic) {
        if (any_synthetic) {
            return RunManifestV1Error::
                kInvalidSyntheticProvenance;
        }
    } else if (!input.synthetic_schema.has_value() ||
               !ValidString(*input.synthetic_schema) ||
               !input.parent_run_id.has_value() ||
               IsZero(*input.parent_run_id) ||
               !input.parent_raw_identity_sha256
                    .has_value() ||
               IsZero(
                   *input.parent_raw_identity_sha256) ||
               !input.fault_rule_sha256.has_value() ||
               IsZero(*input.fault_rule_sha256) ||
               !input.fault_seed.has_value()) {
        return RunManifestV1Error::
            kInvalidSyntheticProvenance;
    }
    if (!ValidateReserveFinalization(
            input.reserve_finalization,
            input.durable_marker)) {
        return RunManifestV1Error::
            kInvalidReserveFinalization;
    }
    return RunManifestV1Error::kNone;
}

void EnsureAppend(
    const std::string& output,
    std::size_t byte_count) {
    if (output.size() >
            kRunManifestV1MaximumBytes ||
        byte_count >
            kRunManifestV1MaximumBytes -
                output.size()) {
        throw EncodedSizeExceeded{};
    }
}

void AppendLiteral(
    std::string* output,
    std::string_view literal) {
    EnsureAppend(*output, literal.size());
    output->append(literal);
}

void AppendU32(
    std::string* output,
    std::uint32_t value) {
    std::array<char, 10U> bytes{};
    const auto converted =
        std::to_chars(
            bytes.data(),
            bytes.data() + bytes.size(),
            value);
    if (converted.ec != std::errc{}) {
        throw std::bad_alloc();
    }
    EnsureAppend(
        *output,
        static_cast<std::size_t>(
            converted.ptr - bytes.data()));
    output->append(bytes.data(), converted.ptr);
}

void AppendU64String(
    std::string* output,
    std::uint64_t value) {
    std::array<char, 20U> bytes{};
    const auto converted =
        std::to_chars(
            bytes.data(),
            bytes.data() + bytes.size(),
            value);
    if (converted.ec != std::errc{}) {
        throw std::bad_alloc();
    }
    EnsureAppend(
        *output,
        static_cast<std::size_t>(
            converted.ptr - bytes.data()) +
            2U);
    output->push_back('"');
    output->append(bytes.data(), converted.ptr);
    output->push_back('"');
}

template <std::size_t Size>
void AppendHex(
    std::string* output,
    const std::array<std::byte, Size>& value) {
    static constexpr std::string_view hex =
        "0123456789abcdef";
    EnsureAppend(*output, 2U + (2U * Size));
    output->push_back('"');
    for (const std::byte byte : value) {
        const std::uint8_t octet =
            std::to_integer<std::uint8_t>(byte);
        output->push_back(
            hex[(octet >> 4U) & 0x0fU]);
        output->push_back(hex[octet & 0x0fU]);
    }
    output->push_back('"');
}

void AppendString(
    std::string* output,
    std::string_view value) {
    static constexpr std::string_view hex =
        "0123456789abcdef";
    std::size_t encoded_size = 2U;
    for (const char character : value) {
        const std::uint8_t byte =
            static_cast<std::uint8_t>(
                static_cast<unsigned char>(
                    character));
        if (byte == 0x08U || byte == 0x09U ||
            byte == 0x0aU || byte == 0x0cU ||
            byte == 0x0dU || byte == 0x22U ||
            byte == 0x5cU) {
            encoded_size += 2U;
        } else if (byte < 0x20U) {
            encoded_size += 6U;
        } else {
            ++encoded_size;
        }
    }
    EnsureAppend(*output, encoded_size);
    output->push_back('"');
    for (const char character : value) {
        const std::uint8_t byte =
            static_cast<std::uint8_t>(
                static_cast<unsigned char>(
                    character));
        switch (byte) {
            case 0x08U:
                AppendLiteral(output, "\\b");
                break;
            case 0x09U:
                AppendLiteral(output, "\\t");
                break;
            case 0x0aU:
                AppendLiteral(output, "\\n");
                break;
            case 0x0cU:
                AppendLiteral(output, "\\f");
                break;
            case 0x0dU:
                AppendLiteral(output, "\\r");
                break;
            case 0x22U:
                AppendLiteral(output, "\\\"");
                break;
            case 0x5cU:
                AppendLiteral(output, "\\\\");
                break;
            default:
                if (byte < 0x20U) {
                    AppendLiteral(output, "\\u00");
                    output->push_back(
                        hex[(byte >> 4U) & 0x0fU]);
                    output->push_back(hex[byte & 0x0fU]);
                } else {
                    output->push_back(character);
                }
                break;
        }
    }
    output->push_back('"');
}

void AppendNullableString(
    std::string* output,
    const std::optional<std::string>& value) {
    if (value.has_value()) {
        AppendString(output, *value);
    } else {
        AppendLiteral(output, "null");
    }
}

template <std::size_t Size>
void AppendOptionalHex(
    std::string* output,
    const std::optional<
        std::array<std::byte, Size>>& value) {
    if (value.has_value()) {
        AppendHex(output, *value);
    } else {
        AppendLiteral(output, "null");
    }
}

void EncodeBuild(
    const RunManifestBuildV1& value,
    std::string* output) {
    AppendLiteral(
        output,
        "{\"build_manifest_sha256\":");
    AppendHex(output, value.build_manifest_sha256);
    AppendLiteral(output, ",\"compiler\":");
    AppendString(output, value.compiler);
    AppendLiteral(output, ",\"cxx_flags\":");
    AppendString(output, value.cxx_flags);
    AppendLiteral(
        output, ",\"dependency_lock_hash\":");
    AppendHex(output, value.dependency_lock_sha256);
    AppendLiteral(output, ",\"python_version\":");
    AppendNullableString(output, value.python_version);
    AppendLiteral(output, ",\"source_revision\":");
    AppendNullableString(output, value.source_revision);
    AppendLiteral(
        output, ",\"source_revision_status\":");
    AppendString(
        output,
        value.source_revision_status ==
                RunManifestSourceRevisionStatusV1::
                    kAvailable
            ? "available"
            : "unavailable");
    AppendLiteral(output, "}");
}

void EncodeConfiguration(
    const RunManifestConfigurationV1& value,
    std::string* output) {
    AppendLiteral(
        output, "{\"canonical_schema_hash\":");
    AppendOptionalHex(
        output, value.canonical_schema_sha256);
    AppendLiteral(output, ",\"config_sha256\":");
    AppendHex(output, value.config_sha256);
    AppendLiteral(output, ",\"dtype_hash\":");
    AppendOptionalHex(output, value.dtype_sha256);
    AppendLiteral(
        output, ",\"endpoint_contract_hash\":");
    AppendHex(
        output, value.endpoint_contract_sha256);
    AppendLiteral(output, ",\"raw_schema_hash\":");
    AppendHex(output, value.raw_schema_sha256);
    AppendLiteral(output, ",\"registry_hash\":");
    AppendHex(output, value.registry_sha256);
    AppendLiteral(
        output, ",\"registry_version\":");
    AppendString(output, value.registry_version);
    AppendLiteral(output, ",\"shard_count\":");
    AppendU32(output, value.shard_count);
    AppendLiteral(output, "}");
}

void EncodeMarker(
    const DurableMarkerV1& value,
    std::string* output) {
    AppendLiteral(output, "{\"flags\":");
    AppendU32(output, value.marker_flags);
    AppendLiteral(output, ",\"global\":");
    AppendU64String(
        output, value.durable_global_wal_pos);
    AppendLiteral(output, ",\"ingress\":");
    AppendU64String(
        output, value.durable_ingress_sequence);
    AppendLiteral(output, ",\"offset\":");
    AppendU64String(
        output, value.durable_segment_offset);
    AppendLiteral(output, ",\"segment\":");
    AppendU32(output, value.segment_sequence);
    AppendLiteral(output, "}");
}

void EncodeReserve(
    const RunManifestReserveFinalizationV1& value,
    std::string* output) {
    AppendLiteral(
        output,
        "{\"continuation_segment_sequences\":[");
    for (std::size_t index = 0U;
         index <
             value.continuation_segment_sequences
                 .size();
         ++index) {
        if (index != 0U) {
            AppendLiteral(output, ",");
        }
        AppendU32(
            output,
            value.continuation_segment_sequences[
                index]);
    }
    AppendLiteral(
        output, "],\"finalization_archive_locator\":");
    AppendNullableString(
        output, value.finalization_archive_locator);
    AppendLiteral(
        output,
        ",\"finalization_archive_manifest_sha256\":");
    AppendOptionalHex(
        output,
        value.finalization_archive_manifest_sha256);
    AppendLiteral(
        output, ",\"finalization_cycle_id\":");
    if (IsZero(value.finalization_cycle_id)) {
        AppendLiteral(output, "null");
    } else {
        AppendHex(output, value.finalization_cycle_id);
    }
    AppendLiteral(
        output, ",\"immutable_grant_sha256\":");
    if (IsZero(value.immutable_grant_sha256)) {
        AppendLiteral(output, "null");
    } else {
        AppendHex(
            output, value.immutable_grant_sha256);
    }
    AppendLiteral(
        output, ",\"maintenance_report_locator\":");
    AppendNullableString(
        output, value.maintenance_report_locator);
    AppendLiteral(
        output, ",\"maintenance_report_sha256\":");
    AppendOptionalHex(
        output, value.maintenance_report_sha256);
    AppendLiteral(
        output, ",\"reserve_state_uuid\":");
    if (IsZero(value.reserve_state_uuid)) {
        AppendLiteral(output, "null");
    } else {
        AppendHex(output, value.reserve_state_uuid);
    }
    AppendLiteral(output, "}");
}

void EncodeRawInput(
    const RunManifestRawInputV1& value,
    std::string* output) {
    AppendLiteral(output, "{\"append_only_reason\":");
    AppendNullableString(output, value.append_only_reason);
    AppendLiteral(output, ",\"capture_date\":");
    AppendU32(output, value.capture_date);
    AppendLiteral(
        output, ",\"clock_epoch_transitions\":[");
    for (std::size_t index = 0U;
         index < value.clock_epoch_transitions.size();
         ++index) {
        if (index != 0U) {
            AppendLiteral(output, ",");
        }
        const RunManifestClockTransitionV1&
            transition =
                value.clock_epoch_transitions[index];
        AppendLiteral(output, "{\"algorithm\":");
        AppendU32(output, transition.algorithm);
        AppendLiteral(output, ",\"digest\":");
        AppendHex(output, transition.digest);
        AppendLiteral(
            output, ",\"record_start_wal_pos\":");
        AppendU64String(
            output,
            transition.record_start_wal_pos);
        AppendLiteral(output, "}");
    }
    AppendLiteral(output, "],\"durability_policy\":");
    AppendString(
        output,
        value.durability_policy ==
                RunManifestDurabilityPolicyV1::
                    kDurableOnly
            ? "durable_only"
            : "includes_recovered_append_only");
    AppendLiteral(
        output,
        ",\"durable_journal_header_sha256\":");
    AppendHex(
        output,
        value.durable_journal_header_sha256);
    AppendLiteral(output, ",\"durable_marker\":");
    EncodeMarker(value.durable_marker, output);
    AppendLiteral(output, ",\"fault_rule_hash\":");
    AppendOptionalHex(output, value.fault_rule_sha256);
    AppendLiteral(output, ",\"fault_seed\":");
    if (value.fault_seed.has_value()) {
        AppendU64String(output, *value.fault_seed);
    } else {
        AppendLiteral(output, "null");
    }
    AppendLiteral(
        output, ",\"first_ingress_sequence\":");
    if (value.range.has_value()) {
        AppendU64String(
            output,
            value.range->first_ingress_sequence);
    } else {
        AppendLiteral(output, "null");
    }
    AppendLiteral(
        output, ",\"first_record_start_wal_pos\":");
    if (value.range.has_value()) {
        AppendU64String(
            output,
            value.range
                ->first_record_start_wal_pos);
    } else {
        AppendLiteral(output, "null");
    }
    AppendLiteral(
        output, ",\"last_ingress_sequence\":");
    if (value.range.has_value()) {
        AppendU64String(
            output,
            value.range->last_ingress_sequence);
    } else {
        AppendLiteral(output, "null");
    }
    AppendLiteral(
        output, ",\"last_record_end_wal_pos\":");
    if (value.range.has_value()) {
        AppendU64String(
            output,
            value.range->last_record_end_wal_pos);
    } else {
        AppendLiteral(output, "null");
    }
    AppendLiteral(output, ",\"parent_raw_identity_hash\":");
    AppendOptionalHex(
        output, value.parent_raw_identity_sha256);
    AppendLiteral(output, ",\"parent_run_id\":");
    AppendOptionalHex(output, value.parent_run_id);
    AppendLiteral(output, ",\"reserve_finalization\":");
    EncodeReserve(value.reserve_finalization, output);
    AppendLiteral(output, ",\"segment_sha256\":[");
    for (std::size_t index = 0U;
         index < value.segment_sha256.size();
         ++index) {
        if (index != 0U) {
            AppendLiteral(output, ",");
        }
        AppendHex(output, value.segment_sha256[index]);
    }
    AppendLiteral(output, "],\"source_stream_id\":");
    AppendU32(output, value.source_stream_id);
    AppendLiteral(output, ",\"stream_day_id\":");
    AppendHex(output, value.stream_day_id);
    AppendLiteral(output, ",\"synthetic\":");
    AppendLiteral(
        output, value.synthetic ? "true" : "false");
    AppendLiteral(output, ",\"synthetic_schema\":");
    AppendNullableString(
        output, value.synthetic_schema);
    AppendLiteral(output, "}");
}

void EncodeFactor(
    const std::optional<RunManifestFactorV1>& value,
    std::string* output) {
    if (!value.has_value()) {
        AppendLiteral(output, "null");
        return;
    }
    AppendLiteral(output, "{\"factor_code_hash\":");
    AppendHex(output, value->factor_code_sha256);
    AppendLiteral(
        output, ",\"factor_config_hash\":");
    AppendHex(output, value->factor_config_sha256);
    AppendLiteral(output, ",\"factor_group\":");
    AppendString(output, value->factor_group);
    AppendLiteral(
        output, ",\"state_schema_hash\":");
    AppendHex(output, value->state_schema_sha256);
    AppendLiteral(output, "}");
}

void EncodeFaultInjection(
    const RunManifestFaultInjectionV1& value,
    std::string* output) {
    AppendLiteral(output, "{\"enabled\":");
    AppendLiteral(
        output, value.enabled ? "true" : "false");
    AppendLiteral(output, ",\"rules\":[");
    for (std::size_t index = 0U;
         index < value.rules.size();
         ++index) {
        if (index != 0U) {
            AppendLiteral(output, ",");
        }
        AppendLiteral(
            output, "{\"canonical_rule\":");
        AppendString(
            output, value.rules[index].canonical_rule);
        AppendLiteral(output, ",\"rule_sha256\":");
        AppendHex(
            output, value.rules[index].rule_sha256);
        AppendLiteral(output, "}");
    }
    AppendLiteral(output, "],\"seed\":");
    if (value.seed.has_value()) {
        AppendU64String(output, *value.seed);
    } else {
        AppendLiteral(output, "null");
    }
    AppendLiteral(output, "}");
}

void EncodeVendor(
    const RunManifestVendorV1& value,
    std::string* output) {
    AppendLiteral(output, "{\"elf_build_id\":");
    AppendString(output, value.elf_build_id);
    AppendLiteral(output, ",\"libmdl_api_sha256\":");
    AppendHex(output, value.libmdl_api_sha256);
    AppendLiteral(output, ",\"sdk_archive_sha256\":");
    AppendHex(output, value.sdk_archive_sha256);
    AppendLiteral(output, ",\"sdk_version\":");
    AppendU32(output, value.sdk_version);
    AppendLiteral(output, "}");
}

}  // namespace

std::string_view RunManifestV1ErrorName(
    RunManifestV1Error error) noexcept {
    switch (error) {
        case RunManifestV1Error::kNone:
            return "none";
        case RunManifestV1Error::kNullOutput:
            return "null_output";
        case RunManifestV1Error::kUnsupportedSchema:
            return "unsupported_schema";
        case RunManifestV1Error::kInvalidMode:
            return "invalid_mode";
        case RunManifestV1Error::kInvalidDate:
            return "invalid_date";
        case RunManifestV1Error::kInvalidIdentity:
            return "invalid_identity";
        case RunManifestV1Error::kInvalidDigest:
            return "invalid_digest";
        case RunManifestV1Error::kInvalidString:
            return "invalid_string";
        case RunManifestV1Error::kInvalidSourceRevision:
            return "invalid_source_revision";
        case RunManifestV1Error::kInvalidConfiguration:
            return "invalid_configuration";
        case RunManifestV1Error::kInvalidInputCount:
            return "invalid_input_count";
        case RunManifestV1Error::kDuplicateNamespace:
            return "duplicate_namespace";
        case RunManifestV1Error::
            kInvalidDurabilityPolicy:
            return "invalid_durability_policy";
        case RunManifestV1Error::kInvalidRange:
            return "invalid_range";
        case RunManifestV1Error::kInvalidMarker:
            return "invalid_marker";
        case RunManifestV1Error::kMarkerMismatch:
            return "marker_mismatch";
        case RunManifestV1Error::kInvalidSegmentSet:
            return "invalid_segment_set";
        case RunManifestV1Error::
            kInvalidClockTransitions:
            return "invalid_clock_transitions";
        case RunManifestV1Error::
            kInvalidSyntheticProvenance:
            return "invalid_synthetic_provenance";
        case RunManifestV1Error::
            kInvalidReserveFinalization:
            return "invalid_reserve_finalization";
        case RunManifestV1Error::kInvalidFactor:
            return "invalid_factor";
        case RunManifestV1Error::
            kInvalidFaultInjection:
            return "invalid_fault_injection";
        case RunManifestV1Error::
            kEncodedSizeExceeded:
            return "encoded_size_exceeded";
        case RunManifestV1Error::kAllocationFailure:
            return "allocation_failure";
    }
    return "unknown";
}

RunManifestV1Error ValidateRunManifestV1(
    const RunManifestV1& manifest) noexcept {
    if (manifest.schema_version !=
        kRunManifestV1SchemaVersion) {
        return RunManifestV1Error::
            kUnsupportedSchema;
    }
    if (manifest.mode != RunManifestModeV1::kLive &&
        manifest.mode != RunManifestModeV1::kReplay) {
        return RunManifestV1Error::kInvalidMode;
    }
    if (!IsGregorianDate(manifest.capture_date) ||
        (manifest.trade_date.has_value() &&
         !IsGregorianDate(*manifest.trade_date))) {
        return RunManifestV1Error::kInvalidDate;
    }
    if (IsZero(manifest.run_id) ||
        IsZero(manifest.host_uuid) ||
        IsZero(manifest.linux_boot_id)) {
        return RunManifestV1Error::kInvalidIdentity;
    }
    if (manifest.clock_epoch_algorithm == 0U ||
        IsZero(manifest.clock_epoch_digest) ||
        IsZero(manifest.vendor.sdk_archive_sha256) ||
        IsZero(manifest.vendor.libmdl_api_sha256) ||
        IsZero(
            manifest.build.build_manifest_sha256) ||
        IsZero(
            manifest.build.dependency_lock_sha256)) {
        return RunManifestV1Error::kInvalidDigest;
    }
    if (manifest.clock_epoch_label !=
        ClockEpochLabel(manifest.clock_epoch_digest)) {
        return RunManifestV1Error::kInvalidIdentity;
    }
    if (manifest.vendor.sdk_version == 0U) {
        return RunManifestV1Error::
            kInvalidConfiguration;
    }
    if (!ValidString(manifest.vendor.elf_build_id) ||
        !ValidString(manifest.build.compiler) ||
        !ValidString(
            manifest.build.cxx_flags,
            kMaximumShortStringBytes,
            true) ||
        (manifest.build.python_version.has_value() &&
         !ValidString(
             *manifest.build.python_version)) ||
        !ValidString(
            manifest.configuration.registry_version)) {
        return RunManifestV1Error::kInvalidString;
    }
    if ((manifest.build.source_revision_status ==
             RunManifestSourceRevisionStatusV1::
                 kAvailable &&
         (!manifest.build.source_revision.has_value() ||
          !ValidString(
              *manifest.build.source_revision))) ||
        (manifest.build.source_revision_status ==
             RunManifestSourceRevisionStatusV1::
                 kUnavailable &&
         manifest.build.source_revision.has_value()) ||
        (manifest.build.source_revision_status !=
             RunManifestSourceRevisionStatusV1::
                 kAvailable &&
         manifest.build.source_revision_status !=
             RunManifestSourceRevisionStatusV1::
                 kUnavailable)) {
        return RunManifestV1Error::
            kInvalidSourceRevision;
    }
    const RunManifestConfigurationV1& configuration =
        manifest.configuration;
    if (IsZero(configuration.config_sha256) ||
        IsZero(
            configuration.endpoint_contract_sha256) ||
        IsZero(configuration.registry_sha256) ||
        IsZero(configuration.raw_schema_sha256) ||
        !NonzeroOptionalDigest(
            configuration.canonical_schema_sha256) ||
        !NonzeroOptionalDigest(
            configuration.dtype_sha256) ||
        configuration.canonical_schema_sha256
                .has_value() !=
            configuration.dtype_sha256.has_value() ||
        (configuration.canonical_schema_sha256
                 .has_value() &&
         configuration.shard_count == 0U) ||
        (!configuration.canonical_schema_sha256
                 .has_value() &&
         configuration.shard_count != 0U)) {
        return RunManifestV1Error::
            kInvalidConfiguration;
    }
    if (manifest.raw_inputs.empty() ||
        manifest.raw_inputs.size() >
            kRunManifestV1MaximumRawInputs) {
        return RunManifestV1Error::kInvalidInputCount;
    }
    for (std::size_t index = 0U;
         index < manifest.raw_inputs.size();
         ++index) {
        const RunManifestRawInputV1& input =
            manifest.raw_inputs[index];
        const RunManifestV1Error input_error =
            ValidateInput(input);
        if (input_error != RunManifestV1Error::kNone) {
            return input_error;
        }
        if (manifest.mode == RunManifestModeV1::kLive &&
            input.capture_date != manifest.capture_date) {
            return RunManifestV1Error::kInvalidDate;
        }
        if (manifest.mode == RunManifestModeV1::kLive &&
            std::any_of(
                input.clock_epoch_transitions.begin(),
                input.clock_epoch_transitions.end(),
                [&manifest](
                    const RunManifestClockTransitionV1&
                        transition) {
                    return transition.algorithm !=
                               manifest
                                   .clock_epoch_algorithm ||
                           transition.digest !=
                               manifest.clock_epoch_digest;
                })) {
            return RunManifestV1Error::
                kInvalidClockTransitions;
        }
        for (std::size_t earlier = 0U;
             earlier < index;
             ++earlier) {
            if (SameNamespace(
                    input,
                    manifest.raw_inputs[earlier])) {
                return RunManifestV1Error::
                    kDuplicateNamespace;
            }
        }
    }
    if (manifest.factor.has_value()) {
        if (!manifest.trade_date.has_value() ||
            !configuration.canonical_schema_sha256
                 .has_value() ||
            !configuration.dtype_sha256.has_value() ||
            configuration.shard_count == 0U ||
            !ValidString(
                manifest.factor->factor_group) ||
            IsZero(
                manifest.factor->factor_code_sha256) ||
            IsZero(
                manifest.factor->factor_config_sha256) ||
            IsZero(
                manifest.factor->state_schema_sha256)) {
            return RunManifestV1Error::kInvalidFactor;
        }
    }
    const RunManifestFaultInjectionV1& fault =
        manifest.fault_injection;
    if ((!fault.enabled &&
         (fault.seed.has_value() || !fault.rules.empty())) ||
        (fault.enabled &&
         (!fault.seed.has_value() ||
          fault.rules.empty())) ||
        fault.rules.size() >
            kRunManifestV1MaximumFaultRules) {
        return RunManifestV1Error::
            kInvalidFaultInjection;
    }
    for (std::size_t index = 0U;
         index < fault.rules.size();
         ++index) {
        const RunManifestFaultRuleV1& rule =
            fault.rules[index];
        if (IsZero(rule.rule_sha256) ||
            !ValidString(rule.canonical_rule) ||
            rule.rule_sha256 !=
                l2flow::common::ComputeSha256(
                    std::string_view{
                        rule.canonical_rule})) {
            return RunManifestV1Error::
                kInvalidFaultInjection;
        }
        for (std::size_t earlier = 0U;
             earlier < index;
             ++earlier) {
            if (fault.rules[earlier].rule_sha256 ==
                rule.rule_sha256) {
                return RunManifestV1Error::
                    kInvalidFaultInjection;
            }
        }
    }
    for (const RunManifestRawInputV1& input :
         manifest.raw_inputs) {
        if (!input.synthetic) {
            continue;
        }
        if (manifest.mode != RunManifestModeV1::kReplay ||
            !input.parent_run_id.has_value() ||
            *input.parent_run_id == manifest.run_id ||
            !fault.enabled ||
            input.fault_seed != fault.seed ||
            std::none_of(
                fault.rules.begin(),
                fault.rules.end(),
                [&input](
                    const RunManifestFaultRuleV1&
                        rule) {
                    return input.fault_rule_sha256 ==
                        rule.rule_sha256;
                })) {
            return RunManifestV1Error::
                kInvalidSyntheticProvenance;
        }
    }
    return RunManifestV1Error::kNone;
}

RunManifestV1Error EncodeRunManifestV1Jcs(
    const RunManifestV1& manifest,
    std::string* output) noexcept {
    if (output == nullptr) {
        return RunManifestV1Error::kNullOutput;
    }
    const RunManifestV1Error validation =
        ValidateRunManifestV1(manifest);
    if (validation != RunManifestV1Error::kNone) {
        return validation;
    }
    try {
        std::string candidate;
        candidate.reserve(8192U);
        AppendLiteral(&candidate, "{\"build\":");
        EncodeBuild(manifest.build, &candidate);
        AppendLiteral(&candidate, ",\"capture_date\":");
        AppendU32(&candidate, manifest.capture_date);
        AppendLiteral(
            &candidate, ",\"clock_epoch_algorithm\":");
        AppendU32(
            &candidate,
            manifest.clock_epoch_algorithm);
        AppendLiteral(
            &candidate, ",\"clock_epoch_digest\":");
        AppendHex(
            &candidate, manifest.clock_epoch_digest);
        AppendLiteral(
            &candidate, ",\"clock_epoch_label\":");
        AppendU64String(
            &candidate, manifest.clock_epoch_label);
        AppendLiteral(&candidate, ",\"configuration\":");
        EncodeConfiguration(
            manifest.configuration, &candidate);
        AppendLiteral(&candidate, ",\"factor\":");
        EncodeFactor(manifest.factor, &candidate);
        AppendLiteral(
            &candidate, ",\"fault_injection\":");
        EncodeFaultInjection(
            manifest.fault_injection, &candidate);
        AppendLiteral(&candidate, ",\"host_uuid\":");
        AppendHex(&candidate, manifest.host_uuid);
        AppendLiteral(
            &candidate, ",\"inputs\":{\"raw_streams\":[");
        for (std::size_t index = 0U;
             index < manifest.raw_inputs.size();
             ++index) {
            if (index != 0U) {
                AppendLiteral(&candidate, ",");
            }
            EncodeRawInput(
                manifest.raw_inputs[index],
                &candidate);
        }
        AppendLiteral(&candidate, "]}");
        AppendLiteral(
            &candidate, ",\"linux_boot_id\":");
        AppendHex(&candidate, manifest.linux_boot_id);
        AppendLiteral(
            &candidate,
            ",\"manifest_schema_version\":");
        AppendU32(
            &candidate, manifest.schema_version);
        AppendLiteral(&candidate, ",\"mode\":");
        AppendString(
            &candidate,
            manifest.mode == RunManifestModeV1::kLive
                ? "live"
                : "replay");
        AppendLiteral(&candidate, ",\"run_id\":");
        AppendHex(&candidate, manifest.run_id);
        AppendLiteral(&candidate, ",\"trade_date\":");
        if (manifest.trade_date.has_value()) {
            AppendU32(
                &candidate, *manifest.trade_date);
        } else {
            AppendLiteral(&candidate, "null");
        }
        AppendLiteral(&candidate, ",\"vendor\":");
        EncodeVendor(manifest.vendor, &candidate);
        AppendLiteral(&candidate, "}");
        *output = std::move(candidate);
        return RunManifestV1Error::kNone;
    } catch (const EncodedSizeExceeded&) {
        return RunManifestV1Error::
            kEncodedSizeExceeded;
    } catch (...) {
        return RunManifestV1Error::kAllocationFailure;
    }
}

BuiltRunManifestV1::BuiltRunManifestV1(
    RunManifestV1 model,
    std::string canonical_jcs,
    RawV1Digest sha256) noexcept
    : model_(std::move(model)),
      canonical_jcs_(std::move(canonical_jcs)),
      sha256_(sha256) {}

RunManifestV1Error BuildRunManifestV1(
    RunManifestV1 model,
    std::unique_ptr<BuiltRunManifestV1>* output) noexcept {
    if (output == nullptr) {
        return RunManifestV1Error::kNullOutput;
    }
    try {
        std::string canonical;
        const RunManifestV1Error error =
            EncodeRunManifestV1Jcs(model, &canonical);
        if (error != RunManifestV1Error::kNone) {
            return error;
        }
        const RawV1Digest digest =
            l2flow::common::ComputeSha256(
                std::span<const std::byte>(
                    reinterpret_cast<const std::byte*>(
                        canonical.data()),
                    canonical.size()));
        std::unique_ptr<BuiltRunManifestV1> candidate(
            new BuiltRunManifestV1(
                std::move(model),
                std::move(canonical),
                digest));
        *output = std::move(candidate);
        return RunManifestV1Error::kNone;
    } catch (...) {
        return RunManifestV1Error::kAllocationFailure;
    }
}

}  // namespace l2flow::ingress
