#include "l2flow/ingress/raw_manifest_v1.h"

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <span>
#include <string>
#include <string_view>
#include <system_error>

namespace l2flow::ingress {
namespace {

template <std::size_t Size>
[[nodiscard]] bool IsZero(
    const std::array<std::byte, Size>& value) noexcept {
    return std::all_of(
        value.begin(),
        value.end(),
        [](std::byte byte) { return byte == std::byte{0}; });
}

[[nodiscard]] bool SameNamespace(
    const RawManifestNamespaceV1& left,
    const RawManifestNamespaceV1& right) noexcept {
    return left.capture_date == right.capture_date &&
           left.source_stream_id == right.source_stream_id &&
           left.stream_day_id == right.stream_day_id;
}

[[nodiscard]] bool HasExactContinuationLocators(
    const RawManifestSegmentEntryV1& entry) noexcept {
    if (!entry.maintenance_report_locator.has_value() ||
        !entry.archive_locator.has_value()) {
        return false;
    }
    try {
        const std::string reserve_uuid =
            l2flow::common::Identity128Hex(
                entry.reserve_state_uuid);
        const std::string cycle_id =
            l2flow::common::Identity128Hex(
                entry.finalization_cycle_id);
        return *entry.maintenance_report_locator ==
                   "maintenance/finalization-" +
                       cycle_id + ".json" &&
               *entry.archive_locator ==
                   "reserve-audit/finalization-" +
                       reserve_uuid + "-" +
                       cycle_id + "/";
    } catch (...) {
        return false;
    }
}

[[nodiscard]] bool IsValidNamespace(
    const RawManifestNamespaceV1& value) noexcept {
    return value.capture_date != 0U &&
           value.source_stream_id != 0U &&
           !l2flow::common::IsZeroIdentity(value.stream_day_id);
}

[[nodiscard]] bool AddChecked(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* result) noexcept {
    if (result == nullptr ||
        right > std::numeric_limits<std::uint64_t>::max() - left) {
        return false;
    }
    *result = left + right;
    return true;
}

void AppendLiteral(std::string* output, std::string_view text) {
    output->append(text);
}

void AppendU32(std::string* output, std::uint32_t value) {
    std::array<char, 10U> buffer{};
    const auto conversion =
        std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
    if (conversion.ec != std::errc{}) {
        throw std::bad_alloc();
    }
    output->append(buffer.data(), conversion.ptr);
}

void AppendU64String(std::string* output, std::uint64_t value) {
    std::array<char, 20U> buffer{};
    const auto conversion =
        std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
    if (conversion.ec != std::errc{}) {
        throw std::bad_alloc();
    }
    output->push_back('"');
    output->append(buffer.data(), conversion.ptr);
    output->push_back('"');
}

void AppendNullableU64String(
    std::string* output,
    const std::optional<std::uint64_t>& value) {
    if (!value.has_value()) {
        AppendLiteral(output, "null");
        return;
    }
    AppendU64String(output, *value);
}

template <std::size_t Size>
void AppendHex(
    std::string* output,
    const std::array<std::byte, Size>& value) {
    static constexpr std::string_view kHex = "0123456789abcdef";
    output->push_back('"');
    for (const std::byte byte : value) {
        const std::uint8_t octet =
            std::to_integer<std::uint8_t>(byte);
        output->push_back(kHex[(octet >> 4U) & 0x0fU]);
        output->push_back(kHex[octet & 0x0fU]);
    }
    output->push_back('"');
}

template <std::size_t Size>
void AppendNullableHex(
    std::string* output,
    const std::array<std::byte, Size>& value) {
    if (IsZero(value)) {
        AppendLiteral(output, "null");
        return;
    }
    AppendHex(output, value);
}

[[nodiscard]] bool IsContinuation(
    const RawManifestSegmentEntryV1& entry) noexcept {
    return (entry.segment_flags &
            kRawV1FinalizationContinuation) != 0U;
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

        if (continuation_count > text.size() - index - 1U) {
            return false;
        }
        const std::uint8_t second =
            static_cast<std::uint8_t>(
                static_cast<unsigned char>(text[index + 1U]));
        if (second < second_min || second > second_max) {
            return false;
        }
        for (std::size_t offset = 2U;
             offset <= continuation_count;
             ++offset) {
            const std::uint8_t continuation =
                static_cast<std::uint8_t>(
                    static_cast<unsigned char>(text[index + offset]));
            if (continuation < 0x80U || continuation > 0xbfU) {
                return false;
            }
        }
        index += continuation_count + 1U;
    }
    return true;
}

void AppendEscapedString(
    std::string* output,
    std::string_view text) {
    static constexpr std::string_view kHex = "0123456789abcdef";
    output->push_back('"');
    for (const char character : text) {
        const std::uint8_t value =
            static_cast<std::uint8_t>(
                static_cast<unsigned char>(character));
        switch (value) {
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
            if (value < 0x20U) {
                AppendLiteral(output, "\\u00");
                output->push_back(kHex[(value >> 4U) & 0x0fU]);
                output->push_back(kHex[value & 0x0fU]);
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
    if (!value.has_value()) {
        AppendLiteral(output, "null");
        return;
    }
    AppendEscapedString(output, *value);
}

void EncodeNamespace(
    const RawManifestNamespaceV1& value,
    std::string* output) {
    AppendLiteral(output, "{\"capture_date\":");
    AppendU32(output, value.capture_date);
    AppendLiteral(output, ",\"source_stream_id\":");
    AppendU32(output, value.source_stream_id);
    AppendLiteral(output, ",\"stream_day_id\":");
    AppendHex(output, value.stream_day_id);
    output->push_back('}');
}

void EncodeEntry(
    const RawManifestSegmentEntryV1& entry,
    std::string* output) {
    // Keys are emitted in UTF-16/JCS order. Every V1 key is ASCII.
    AppendLiteral(output, "{\"accepted_marker_bytes\":");
    AppendHex(output, entry.accepted_marker_bytes);
    AppendLiteral(output, ",\"accepted_marker_sha256\":");
    AppendHex(output, entry.accepted_marker_sha256);
    AppendLiteral(output, ",\"actual_first_ingress_sequence\":");
    AppendNullableU64String(
        output, entry.actual_first_ingress_sequence);
    AppendLiteral(output, ",\"actual_last_ingress_sequence\":");
    AppendNullableU64String(
        output, entry.actual_last_ingress_sequence);
    AppendLiteral(output, ",\"archive_locator\":");
    AppendNullableString(output, entry.archive_locator);
    AppendLiteral(output, ",\"build_manifest_sha256\":");
    AppendHex(output, entry.build_manifest_sha256);
    AppendLiteral(output, ",\"clock_epoch_algorithm\":");
    AppendU32(output, entry.clock_epoch_algorithm);
    AppendLiteral(output, ",\"clock_epoch_digest\":");
    AppendHex(output, entry.clock_epoch_digest);
    AppendLiteral(output, ",\"clock_epoch_label\":");
    AppendU64String(output, entry.clock_epoch_label);
    AppendLiteral(output, ",\"config_sha256\":");
    AppendHex(output, entry.config_sha256);
    AppendLiteral(output, ",\"endpoint_contract_sha256\":");
    AppendHex(output, entry.endpoint_contract_sha256);
    AppendLiteral(output, ",\"finalization_cycle_id\":");
    AppendNullableHex(output, entry.finalization_cycle_id);
    AppendLiteral(output, ",\"host_uuid\":");
    AppendHex(output, entry.host_uuid);
    AppendLiteral(output, ",\"immutable_grant_sha256\":");
    AppendNullableHex(output, entry.immutable_grant_sha256);
    AppendLiteral(output, ",\"libmdl_api_sha256\":");
    AppendHex(output, entry.libmdl_api_sha256);
    AppendLiteral(output, ",\"linux_boot_id\":");
    AppendHex(output, entry.linux_boot_id);
    AppendLiteral(output, ",\"maintenance_report_locator\":");
    AppendNullableString(output, entry.maintenance_report_locator);
    AppendLiteral(output, ",\"namespace\":");
    EncodeNamespace(entry.namespace_identity, output);
    AppendLiteral(
        output, ",\"next_expected_first_ingress_sequence\":");
    AppendU64String(
        output, entry.next_expected_first_ingress_sequence);
    AppendLiteral(output, ",\"raw_schema_sha256\":");
    AppendHex(output, entry.raw_schema_sha256);
    AppendLiteral(output, ",\"record_count\":");
    AppendU64String(output, entry.record_count);
    AppendLiteral(output, ",\"reserve_state_uuid\":");
    AppendNullableHex(output, entry.reserve_state_uuid);
    AppendLiteral(output, ",\"sdk_archive_sha256\":");
    AppendHex(output, entry.sdk_archive_sha256);
    AppendLiteral(output, ",\"segment_base_wal_pos\":");
    AppendU64String(output, entry.segment_base_wal_pos);
    AppendLiteral(output, ",\"segment_flags\":");
    AppendU32(output, entry.segment_flags);
    AppendLiteral(output, ",\"segment_logical_length\":");
    AppendU64String(output, entry.segment_logical_length);
    AppendLiteral(output, ",\"segment_sequence\":");
    AppendU32(output, entry.segment_sequence);
    AppendLiteral(output, ",\"segment_sha256\":");
    AppendHex(output, entry.segment_sha256);
    AppendLiteral(output, ",\"state\":");
    if (entry.state == RawManifestSegmentStateV1::kClosed) {
        AppendLiteral(output, "\"closed\"}");
    } else {
        AppendLiteral(output, "\"open\"}");
    }
}

void EncodeEntryArray(
    std::span<const RawManifestSegmentEntryV1> entries,
    std::string* output) {
    output->push_back('[');
    bool first = true;
    for (const RawManifestSegmentEntryV1& entry : entries) {
        if (!first) {
            output->push_back(',');
        }
        first = false;
        EncodeEntry(entry, output);
    }
    output->push_back(']');
}

void EncodeFrontier(
    const RawManifestV1& manifest,
    std::size_t closed_count,
    std::string* output) {
    AppendLiteral(output, "{\"closed_entries\":");
    EncodeEntryArray(
        std::span<const RawManifestSegmentEntryV1>(
            manifest.closed_entries.data(), closed_count),
        output);
    AppendLiteral(output, ",\"closed_entry_count\":");
    AppendU64String(
        output, static_cast<std::uint64_t>(closed_count));
    AppendLiteral(output, ",\"namespace\":");
    EncodeNamespace(manifest.namespace_identity, output);
    AppendLiteral(output, ",\"schema_version\":");
    AppendU32(output, manifest.schema_version);
    output->push_back('}');
}

[[nodiscard]] RawManifestV1Error ValidateEntryIntrinsic(
    const RawManifestSegmentEntryV1& entry,
    RawManifestSegmentStateV1 required_state) noexcept {
    if (!IsValidNamespace(entry.namespace_identity)) {
        return RawManifestV1Error::kInvalidNamespace;
    }
    if (entry.segment_sequence == 0U) {
        return RawManifestV1Error::kSegmentOrderViolation;
    }
    if (entry.state != RawManifestSegmentStateV1::kOpen &&
        entry.state != RawManifestSegmentStateV1::kClosed) {
        return RawManifestV1Error::kInvalidEntryState;
    }
    if (entry.state != required_state) {
        return RawManifestV1Error::kInvalidEntryState;
    }
    if ((entry.segment_flags & ~kRawV1SegmentFlagsMask) != 0U) {
        return RawManifestV1Error::kUnknownSegmentFlags;
    }
    if (entry.segment_logical_length <
        static_cast<std::uint64_t>(kRawV1SegmentHeaderBytes)) {
        return RawManifestV1Error::kInvalidRecordRange;
    }
    if (IsZero(entry.segment_sha256) ||
        IsZero(entry.accepted_marker_sha256) ||
        IsZero(entry.clock_epoch_digest) ||
        IsZero(entry.sdk_archive_sha256) ||
        IsZero(entry.libmdl_api_sha256) ||
        IsZero(entry.endpoint_contract_sha256) ||
        IsZero(entry.config_sha256) ||
        IsZero(entry.raw_schema_sha256) ||
        IsZero(entry.build_manifest_sha256)) {
        return RawManifestV1Error::kInvalidDigest;
    }
    if (l2flow::common::IsZeroIdentity(entry.host_uuid) ||
        l2flow::common::IsZeroIdentity(entry.linux_boot_id) ||
        entry.clock_epoch_algorithm == 0U ||
        entry.next_expected_first_ingress_sequence == 0U) {
        return RawManifestV1Error::kInvalidIdentity;
    }

    const bool has_first =
        entry.actual_first_ingress_sequence.has_value();
    const bool has_last =
        entry.actual_last_ingress_sequence.has_value();
    if (entry.record_count == 0U) {
        if (has_first || has_last) {
            return RawManifestV1Error::kInvalidRecordRange;
        }
    } else {
        if (!has_first || !has_last) {
            return RawManifestV1Error::kInvalidRecordRange;
        }
        const std::uint64_t first =
            *entry.actual_first_ingress_sequence;
        const std::uint64_t last =
            *entry.actual_last_ingress_sequence;
        if (first == 0U || first > last ||
            first != entry.next_expected_first_ingress_sequence ||
            last - first != entry.record_count - 1U) {
            return RawManifestV1Error::kInvalidRecordRange;
        }
    }

    DurableMarkerV1 marker{};
    if (DecodeDurableMarkerV1(
            entry.accepted_marker_bytes, &marker) !=
        RawV1Error::kNone) {
        return RawManifestV1Error::kInvalidMarker;
    }
    if (ComputeAcceptedMarkerSha256(
            entry.accepted_marker_bytes) !=
        entry.accepted_marker_sha256) {
        return RawManifestV1Error::kMarkerHashMismatch;
    }
    std::uint64_t expected_global = 0U;
    if (!AddChecked(
            entry.segment_base_wal_pos,
            entry.segment_logical_length,
            &expected_global)) {
        return RawManifestV1Error::kLengthOverflow;
    }
    if (marker.source_stream_id !=
            entry.namespace_identity.source_stream_id ||
        marker.segment_sequence != entry.segment_sequence ||
        marker.durable_segment_offset !=
            entry.segment_logical_length ||
        marker.durable_global_wal_pos != expected_global) {
        return RawManifestV1Error::kMarkerMismatch;
    }
    const bool sealed =
        (marker.marker_flags & kRawV1SegmentSealed) != 0U;
    if (sealed !=
        (required_state == RawManifestSegmentStateV1::kClosed)) {
        return RawManifestV1Error::kMarkerMismatch;
    }
    if (entry.record_count != 0U &&
        marker.durable_ingress_sequence !=
            *entry.actual_last_ingress_sequence) {
        return RawManifestV1Error::kMarkerMismatch;
    }

    const bool continuation = IsContinuation(entry);
    const bool reserve_zero =
        l2flow::common::IsZeroIdentity(entry.reserve_state_uuid);
    const bool cycle_zero =
        l2flow::common::IsZeroIdentity(entry.finalization_cycle_id);
    const bool grant_zero =
        IsZero(entry.immutable_grant_sha256);
    const bool has_report =
        entry.maintenance_report_locator.has_value();
    const bool has_archive = entry.archive_locator.has_value();
    if (continuation) {
        if (reserve_zero || cycle_zero || grant_zero ||
            !has_report || !has_archive ||
            entry.maintenance_report_locator->empty() ||
            entry.archive_locator->empty()) {
            return RawManifestV1Error::kInvalidFinalization;
        }
        if (!IsValidUtf8(*entry.maintenance_report_locator) ||
            !IsValidUtf8(*entry.archive_locator)) {
            return RawManifestV1Error::kInvalidUtf8;
        }
        if (!HasExactContinuationLocators(entry)) {
            return RawManifestV1Error::kInvalidFinalization;
        }
    } else if (!reserve_zero || !cycle_zero || !grant_zero ||
               has_report || has_archive) {
        return RawManifestV1Error::kInvalidFinalization;
    }

    return RawManifestV1Error::kNone;
}

[[nodiscard]] RawManifestV1Error ValidateEntryChain(
    const RawManifestV1& manifest) noexcept {
    std::uint32_t expected_segment_sequence = 1U;
    std::uint64_t expected_base = 0U;
    std::uint64_t expected_ingress = 1U;
    std::uint64_t last_durable_ingress = 0U;

    const auto validate_one =
        [&](const RawManifestSegmentEntryV1& entry,
            RawManifestSegmentStateV1 required_state)
        -> RawManifestV1Error {
        RawManifestV1Error result =
            ValidateEntryIntrinsic(entry, required_state);
        if (result != RawManifestV1Error::kNone) {
            return result;
        }
        if (!SameNamespace(
                manifest.namespace_identity,
                entry.namespace_identity)) {
            return RawManifestV1Error::kInvalidNamespace;
        }
        if (entry.segment_sequence != expected_segment_sequence) {
            return RawManifestV1Error::kSegmentOrderViolation;
        }
        if (entry.segment_base_wal_pos != expected_base) {
            return RawManifestV1Error::kWalDiscontinuity;
        }
        if (entry.next_expected_first_ingress_sequence !=
            expected_ingress) {
            return RawManifestV1Error::kIngressDiscontinuity;
        }

        DurableMarkerV1 marker{};
        if (DecodeDurableMarkerV1(
                entry.accepted_marker_bytes, &marker) !=
            RawV1Error::kNone) {
            return RawManifestV1Error::kInvalidMarker;
        }
        if (entry.record_count == 0U) {
            if (marker.durable_ingress_sequence !=
                last_durable_ingress) {
                return RawManifestV1Error::kIngressDiscontinuity;
            }
        } else {
            const std::uint64_t last =
                *entry.actual_last_ingress_sequence;
            if (last == std::numeric_limits<std::uint64_t>::max()) {
                return RawManifestV1Error::kLengthOverflow;
            }
            last_durable_ingress = last;
            expected_ingress = last + 1U;
        }

        if (!AddChecked(
                expected_base,
                entry.segment_logical_length,
                &expected_base)) {
            return RawManifestV1Error::kLengthOverflow;
        }
        if (expected_segment_sequence ==
            std::numeric_limits<std::uint32_t>::max()) {
            return RawManifestV1Error::kLengthOverflow;
        }
        ++expected_segment_sequence;
        return RawManifestV1Error::kNone;
    };

    for (const RawManifestSegmentEntryV1& entry :
         manifest.closed_entries) {
        const RawManifestV1Error result =
            validate_one(
                entry, RawManifestSegmentStateV1::kClosed);
        if (result != RawManifestV1Error::kNone) {
            return result;
        }
    }
    if (manifest.open_entry.has_value()) {
        return validate_one(
            *manifest.open_entry,
            RawManifestSegmentStateV1::kOpen);
    }
    return RawManifestV1Error::kNone;
}

[[nodiscard]] RawManifestV1Error ComputePrefixForCount(
    const RawManifestV1& manifest,
    std::size_t count,
    RawV1Digest* digest,
    std::string* canonical_frontier) noexcept {
    if (digest == nullptr ||
        count > manifest.closed_entries.size()) {
        return RawManifestV1Error::kNullOutput;
    }
    try {
        std::string encoded;
        EncodeFrontier(manifest, count, &encoded);
        const RawV1Digest computed =
            l2flow::common::ComputeSha256(
                std::string_view(encoded));
        if (canonical_frontier != nullptr) {
            *canonical_frontier = encoded;
        }
        *digest = computed;
        return RawManifestV1Error::kNone;
    } catch (const std::bad_alloc&) {
        return RawManifestV1Error::kAllocationFailure;
    } catch (...) {
        return RawManifestV1Error::kAllocationFailure;
    }
}

[[nodiscard]] RawManifestV1Error ValidateCore(
    const RawManifestV1& manifest,
    bool validate_stored_prefix) noexcept {
    if (manifest.schema_version !=
        kRawManifestV1SchemaVersion) {
        return RawManifestV1Error::kUnsupportedSchemaVersion;
    }
    if (!IsValidNamespace(manifest.namespace_identity)) {
        return RawManifestV1Error::kInvalidNamespace;
    }
    if (manifest.closed_entries.size() >
        static_cast<std::size_t>(
            std::numeric_limits<std::uint64_t>::max())) {
        return RawManifestV1Error::kLengthOverflow;
    }
    if (manifest.closed_entry_count !=
        static_cast<std::uint64_t>(
            manifest.closed_entries.size())) {
        return RawManifestV1Error::kClosedEntryCountMismatch;
    }
    const RawManifestV1Error chain_result =
        ValidateEntryChain(manifest);
    if (chain_result != RawManifestV1Error::kNone) {
        return chain_result;
    }
    if (!validate_stored_prefix) {
        return RawManifestV1Error::kNone;
    }
    RawV1Digest computed{};
    const RawManifestV1Error prefix_result =
        ComputePrefixForCount(
            manifest,
            manifest.closed_entries.size(),
            &computed,
            nullptr);
    if (prefix_result != RawManifestV1Error::kNone) {
        return prefix_result;
    }
    if (computed != manifest.closed_prefix_sha256) {
        return RawManifestV1Error::kClosedPrefixMismatch;
    }
    return RawManifestV1Error::kNone;
}

[[nodiscard]] bool SameImmutableSegmentIdentity(
    const RawManifestSegmentEntryV1& left,
    const RawManifestSegmentEntryV1& right) noexcept {
    return SameNamespace(
               left.namespace_identity,
               right.namespace_identity) &&
           left.segment_sequence == right.segment_sequence &&
           left.segment_flags == right.segment_flags &&
           left.segment_base_wal_pos == right.segment_base_wal_pos &&
           left.next_expected_first_ingress_sequence ==
               right.next_expected_first_ingress_sequence &&
           left.host_uuid == right.host_uuid &&
           left.linux_boot_id == right.linux_boot_id &&
           left.clock_epoch_algorithm ==
               right.clock_epoch_algorithm &&
           left.clock_epoch_digest == right.clock_epoch_digest &&
           left.clock_epoch_label == right.clock_epoch_label &&
           left.sdk_archive_sha256 == right.sdk_archive_sha256 &&
           left.libmdl_api_sha256 == right.libmdl_api_sha256 &&
           left.endpoint_contract_sha256 ==
               right.endpoint_contract_sha256 &&
           left.config_sha256 == right.config_sha256 &&
           left.raw_schema_sha256 == right.raw_schema_sha256 &&
           left.build_manifest_sha256 ==
               right.build_manifest_sha256 &&
           left.reserve_state_uuid == right.reserve_state_uuid &&
           left.finalization_cycle_id ==
               right.finalization_cycle_id &&
           left.immutable_grant_sha256 ==
               right.immutable_grant_sha256 &&
           left.maintenance_report_locator ==
               right.maintenance_report_locator &&
           left.archive_locator == right.archive_locator;
}

[[nodiscard]] bool IsNonRegressingOpenSuccessor(
    const RawManifestSegmentEntryV1& previous,
    const RawManifestSegmentEntryV1& current) noexcept {
    if (current.record_count < previous.record_count ||
        current.segment_logical_length <
            previous.segment_logical_length) {
        return false;
    }
    if (current.record_count == previous.record_count) {
        if (current.segment_logical_length !=
                previous.segment_logical_length ||
            current.segment_sha256 != previous.segment_sha256 ||
            current.actual_first_ingress_sequence !=
                previous.actual_first_ingress_sequence ||
            current.actual_last_ingress_sequence !=
                previous.actual_last_ingress_sequence) {
            return false;
        }
    } else if (current.segment_logical_length ==
               previous.segment_logical_length) {
        return false;
    }

    DurableMarkerV1 previous_marker{};
    DurableMarkerV1 current_marker{};
    if (DecodeDurableMarkerV1(
            previous.accepted_marker_bytes,
            &previous_marker) != RawV1Error::kNone ||
        DecodeDurableMarkerV1(
            current.accepted_marker_bytes,
            &current_marker) != RawV1Error::kNone) {
        return false;
    }
    if (current_marker.durable_global_wal_pos <
            previous_marker.durable_global_wal_pos ||
        current_marker.durable_segment_offset <
            previous_marker.durable_segment_offset ||
        current_marker.durable_ingress_sequence <
            previous_marker.durable_ingress_sequence) {
        return false;
    }
    if (current.state == RawManifestSegmentStateV1::kOpen &&
        current.record_count == previous.record_count &&
        (current.accepted_marker_bytes !=
             previous.accepted_marker_bytes ||
         current.accepted_marker_sha256 !=
             previous.accepted_marker_sha256)) {
        return false;
    }
    return true;
}

[[nodiscard]] RawManifestV1Error CompareClosedPrefixEntries(
    const RawManifestV1& current,
    const RawManifestV1& previous) noexcept {
    try {
        for (std::size_t index = 0U;
             index < previous.closed_entries.size();
             ++index) {
            std::string current_entry;
            std::string previous_entry;
            EncodeEntry(current.closed_entries[index], &current_entry);
            EncodeEntry(previous.closed_entries[index], &previous_entry);
            if (current_entry != previous_entry) {
                return RawManifestV1Error::kNotAppendOnly;
            }
        }
        return RawManifestV1Error::kNone;
    } catch (const std::bad_alloc&) {
        return RawManifestV1Error::kAllocationFailure;
    } catch (...) {
        return RawManifestV1Error::kAllocationFailure;
    }
}

}  // namespace

std::string_view RawManifestV1ErrorName(
    RawManifestV1Error error) noexcept {
    switch (error) {
    case RawManifestV1Error::kNone:
        return "none";
    case RawManifestV1Error::kNullOutput:
        return "null_output";
    case RawManifestV1Error::kUnsupportedSchemaVersion:
        return "unsupported_schema_version";
    case RawManifestV1Error::kInvalidNamespace:
        return "invalid_namespace";
    case RawManifestV1Error::kClosedEntryCountMismatch:
        return "closed_entry_count_mismatch";
    case RawManifestV1Error::kClosedPrefixMismatch:
        return "closed_prefix_mismatch";
    case RawManifestV1Error::kInvalidEntryState:
        return "invalid_entry_state";
    case RawManifestV1Error::kUnknownSegmentFlags:
        return "unknown_segment_flags";
    case RawManifestV1Error::kInvalidIdentity:
        return "invalid_identity";
    case RawManifestV1Error::kInvalidDigest:
        return "invalid_digest";
    case RawManifestV1Error::kInvalidMarker:
        return "invalid_marker";
    case RawManifestV1Error::kMarkerHashMismatch:
        return "marker_hash_mismatch";
    case RawManifestV1Error::kMarkerMismatch:
        return "marker_mismatch";
    case RawManifestV1Error::kInvalidRecordRange:
        return "invalid_record_range";
    case RawManifestV1Error::kSegmentOrderViolation:
        return "segment_order_violation";
    case RawManifestV1Error::kWalDiscontinuity:
        return "wal_discontinuity";
    case RawManifestV1Error::kIngressDiscontinuity:
        return "ingress_discontinuity";
    case RawManifestV1Error::kInvalidFinalization:
        return "invalid_finalization";
    case RawManifestV1Error::kInvalidUtf8:
        return "invalid_utf8";
    case RawManifestV1Error::kGenerationRegression:
        return "generation_regression";
    case RawManifestV1Error::kNotAppendOnly:
        return "not_append_only";
    case RawManifestV1Error::kLengthOverflow:
        return "length_overflow";
    case RawManifestV1Error::kAllocationFailure:
        return "allocation_failure";
    }
    return "unknown";
}

RawV1Digest ComputeAcceptedMarkerSha256(
    std::span<const std::byte> marker_bytes) noexcept {
    return l2flow::common::ComputeSha256(marker_bytes);
}

RawManifestV1Error ComputeClosedPrefix(
    const RawManifestV1& manifest,
    RawV1Digest* digest,
    std::string* canonical_frontier) noexcept {
    if (digest == nullptr) {
        return RawManifestV1Error::kNullOutput;
    }
    const RawManifestV1Error validation =
        ValidateCore(manifest, false);
    if (validation != RawManifestV1Error::kNone) {
        return validation;
    }
    return ComputePrefixForCount(
        manifest,
        manifest.closed_entries.size(),
        digest,
        canonical_frontier);
}

RawManifestV1Error ValidateManifestModel(
    const RawManifestV1& manifest,
    const RawManifestV1* previous) noexcept {
    RawManifestV1Error result =
        ValidateCore(manifest, true);
    if (result != RawManifestV1Error::kNone || previous == nullptr) {
        return result;
    }
    result = ValidateCore(*previous, true);
    if (result != RawManifestV1Error::kNone) {
        return result;
    }
    if (!SameNamespace(
            manifest.namespace_identity,
            previous->namespace_identity)) {
        return RawManifestV1Error::kNotAppendOnly;
    }
    if (manifest.manifest_generation <=
        previous->manifest_generation) {
        return RawManifestV1Error::kGenerationRegression;
    }
    if (manifest.closed_entries.size() <
        previous->closed_entries.size()) {
        return RawManifestV1Error::kNotAppendOnly;
    }
    result = CompareClosedPrefixEntries(manifest, *previous);
    if (result != RawManifestV1Error::kNone) {
        return result;
    }

    RawV1Digest recomputed_previous_frontier{};
    result = ComputePrefixForCount(
        manifest,
        previous->closed_entries.size(),
        &recomputed_previous_frontier,
        nullptr);
    if (result != RawManifestV1Error::kNone) {
        return result;
    }
    if (recomputed_previous_frontier !=
        previous->closed_prefix_sha256) {
        return RawManifestV1Error::kNotAppendOnly;
    }

    if (previous->open_entry.has_value()) {
        const RawManifestSegmentEntryV1* successor = nullptr;
        if (manifest.closed_entries.size() >
            previous->closed_entries.size()) {
            successor = &manifest.closed_entries[
                previous->closed_entries.size()];
        } else if (manifest.open_entry.has_value()) {
            successor = &*manifest.open_entry;
        } else {
            return RawManifestV1Error::kNotAppendOnly;
        }
        if (!SameImmutableSegmentIdentity(
                *previous->open_entry, *successor) ||
            !IsNonRegressingOpenSuccessor(
                *previous->open_entry, *successor)) {
            return RawManifestV1Error::kNotAppendOnly;
        }
    }
    return RawManifestV1Error::kNone;
}

RawManifestV1Error EncodeRawManifestJcs(
    const RawManifestV1& manifest,
    std::string* output) noexcept {
    if (output == nullptr) {
        return RawManifestV1Error::kNullOutput;
    }
    const RawManifestV1Error validation =
        ValidateManifestModel(manifest);
    if (validation != RawManifestV1Error::kNone) {
        return validation;
    }
    try {
        std::string encoded;
        AppendLiteral(&encoded, "{\"closed_entries\":");
        EncodeEntryArray(manifest.closed_entries, &encoded);
        AppendLiteral(&encoded, ",\"closed_entry_count\":");
        AppendU64String(&encoded, manifest.closed_entry_count);
        AppendLiteral(&encoded, ",\"closed_prefix_sha256\":");
        AppendHex(&encoded, manifest.closed_prefix_sha256);
        AppendLiteral(&encoded, ",\"manifest_generation\":");
        AppendU64String(&encoded, manifest.manifest_generation);
        AppendLiteral(&encoded, ",\"namespace\":");
        EncodeNamespace(manifest.namespace_identity, &encoded);
        AppendLiteral(&encoded, ",\"open_entry\":");
        if (manifest.open_entry.has_value()) {
            EncodeEntry(*manifest.open_entry, &encoded);
        } else {
            AppendLiteral(&encoded, "null");
        }
        AppendLiteral(&encoded, ",\"schema_version\":");
        AppendU32(&encoded, manifest.schema_version);
        encoded.push_back('}');
        *output = std::move(encoded);
        return RawManifestV1Error::kNone;
    } catch (const std::bad_alloc&) {
        return RawManifestV1Error::kAllocationFailure;
    } catch (...) {
        return RawManifestV1Error::kAllocationFailure;
    }
}

RawManifestV1Error
ComputeRawManifestOpenEntryCommitmentV1(
    const RawManifestV1& manifest,
    RawV1Digest* digest,
    std::string* canonical_open_entry) noexcept {
    if (digest == nullptr) {
        return RawManifestV1Error::kNullOutput;
    }
    const RawManifestV1Error validation =
        ValidateManifestModel(manifest);
    if (validation != RawManifestV1Error::kNone) {
        return validation;
    }
    if (!manifest.open_entry.has_value()) {
        return RawManifestV1Error::kInvalidEntryState;
    }
    try {
        std::string entry_jcs;
        EncodeEntry(
            *manifest.open_entry, &entry_jcs);
        l2flow::common::Sha256Hasher hasher;
        const auto domain = std::as_bytes(
            std::span{
                kRawManifestOpenEntryCommitmentDomainV1
                    .data(),
                kRawManifestOpenEntryCommitmentDomainV1
                    .size()});
        const std::array<std::byte, 1U> separator{
            std::byte{0}};
        const auto entry_bytes = std::as_bytes(
            std::span{
                entry_jcs.data(),
                entry_jcs.size()});
        RawV1Digest candidate{};
        if (!hasher.Update(domain) ||
            !hasher.Update(separator) ||
            !hasher.Update(entry_bytes) ||
            !hasher.Finalize(&candidate)) {
            return RawManifestV1Error::kLengthOverflow;
        }
        *digest = candidate;
        if (canonical_open_entry != nullptr) {
            canonical_open_entry->swap(entry_jcs);
        }
        return RawManifestV1Error::kNone;
    } catch (const std::bad_alloc&) {
        return RawManifestV1Error::kAllocationFailure;
    } catch (...) {
        return RawManifestV1Error::kAllocationFailure;
    }
}

}  // namespace l2flow::ingress
