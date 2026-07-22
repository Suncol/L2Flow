#include "l2flow/factor/factor_watermark_v1.h"

#include "l2flow/control/raw_frontier_v1.h"

#include <algorithm>
#include <array>
#include <limits>
#include <new>
#include <tuple>

namespace l2flow::factor {
namespace {

[[nodiscard]] bool FamilyValid(
    l2flow::canonical::CanonicalEventTypeV1 family) noexcept {
    using l2flow::canonical::CanonicalEventTypeV1;
    switch (family) {
        case CanonicalEventTypeV1::kSnapshot:
        case CanonicalEventTypeV1::kTick:
        case CanonicalEventTypeV1::kQuality:
        case CanonicalEventTypeV1::kControl:
            return true;
        case CanonicalEventTypeV1::kUnknown:
            return false;
    }
    return false;
}

[[nodiscard]] bool EntryValid(
    const FactorInputWatermarkEntryV1& entry) noexcept {
    if (entry.key.source_stream_id == 0U ||
        entry.key.origin_capture_date == 0U ||
        l2flow::common::IsZeroIdentity(entry.key.origin_stream_day_id) ||
        !FamilyValid(entry.key.family) ||
        !l2flow::canonical::ClockEpochIdentityV1Valid(entry.clock_epoch) ||
        (entry.input_quality_flags &
         ~l2flow::canonical::kCanonicalQualityFlagsMaskV1) != 0U) {
        return false;
    }
    if ((entry.canonical_cursor == 0U) !=
        (entry.max_consumed_origin_wal_end_pos == 0U)) {
        return false;
    }
    return true;
}

void AppendU16Le(std::uint16_t value, std::vector<std::byte>* output) {
    output->push_back(static_cast<std::byte>(value & 0xffU));
    output->push_back(static_cast<std::byte>((value >> 8U) & 0xffU));
}

void AppendU32Le(std::uint32_t value, std::vector<std::byte>* output) {
    for (std::size_t index = 0U; index < 4U; ++index) {
        output->push_back(static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU));
    }
}

void AppendU64Le(std::uint64_t value, std::vector<std::byte>* output) {
    for (std::size_t index = 0U; index < 8U; ++index) {
        output->push_back(static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU));
    }
}

[[nodiscard]] bool AuthorityValid(
    const l2flow::ingress::RawControlSnapshot& authority) noexcept {
    return authority.source_stream_id != 0U && authority.capture_date != 0U &&
           !l2flow::common::IsZeroIdentity(authority.stream_day_id) &&
           !l2flow::common::IsZeroIdentity(authority.writer_instance) &&
           authority.fatal_state == 0U &&
           l2flow::control::RawFrontierCursorShapeValidV1(authority);
}

[[nodiscard]] bool SameAuthorityNamespace(
    const l2flow::ingress::RawControlSnapshot& left,
    const l2flow::ingress::RawControlSnapshot& right) noexcept {
    return left.source_stream_id == right.source_stream_id &&
           left.capture_date == right.capture_date &&
           left.stream_day_id == right.stream_day_id;
}

[[nodiscard]] bool AuthorityNamespaceLess(
    const l2flow::ingress::RawControlSnapshot& left,
    const l2flow::ingress::RawControlSnapshot& right) noexcept {
    if (left.source_stream_id != right.source_stream_id) {
        return left.source_stream_id < right.source_stream_id;
    }
    if (left.capture_date != right.capture_date) {
        return left.capture_date < right.capture_date;
    }
    return left.stream_day_id < right.stream_day_id;
}

[[nodiscard]] bool AuthorityNamespaceLessThanKey(
    const l2flow::ingress::RawControlSnapshot& authority,
    const FactorInputWatermarkKeyV1& key) noexcept {
    if (authority.source_stream_id != key.source_stream_id) {
        return authority.source_stream_id < key.source_stream_id;
    }
    if (authority.capture_date != key.origin_capture_date) {
        return authority.capture_date < key.origin_capture_date;
    }
    return authority.stream_day_id < key.origin_stream_day_id;
}

[[nodiscard]] bool KeyNamespaceLessThanAuthority(
    const FactorInputWatermarkKeyV1& key,
    const l2flow::ingress::RawControlSnapshot& authority) noexcept {
    if (key.source_stream_id != authority.source_stream_id) {
        return key.source_stream_id < authority.source_stream_id;
    }
    if (key.origin_capture_date != authority.capture_date) {
        return key.origin_capture_date < authority.capture_date;
    }
    return key.origin_stream_day_id < authority.stream_day_id;
}

}  // namespace

std::string_view FactorWatermarkErrorNameV1(
    FactorWatermarkErrorV1 error) noexcept {
    switch (error) {
        case FactorWatermarkErrorV1::kNone:
            return "none";
        case FactorWatermarkErrorV1::kNullOutput:
            return "null_output";
        case FactorWatermarkErrorV1::kInvalidSetId:
            return "invalid_set_id";
        case FactorWatermarkErrorV1::kInvalidTradeDate:
            return "invalid_trade_date";
        case FactorWatermarkErrorV1::kInvalidEntryCount:
            return "invalid_entry_count";
        case FactorWatermarkErrorV1::kInvalidEntry:
            return "invalid_entry";
        case FactorWatermarkErrorV1::kDuplicateKey:
            return "duplicate_key";
        case FactorWatermarkErrorV1::kNotCanonical:
            return "not_canonical";
        case FactorWatermarkErrorV1::kHashMismatch:
            return "hash_mismatch";
        case FactorWatermarkErrorV1::kSizeOverflow:
            return "size_overflow";
        case FactorWatermarkErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "invalid_watermark_error";
}

bool FactorInputWatermarkKeyLessV1(
    const FactorInputWatermarkKeyV1& left,
    const FactorInputWatermarkKeyV1& right) noexcept {
    if (left.source_stream_id != right.source_stream_id) {
        return left.source_stream_id < right.source_stream_id;
    }
    if (left.origin_capture_date != right.origin_capture_date) {
        return left.origin_capture_date < right.origin_capture_date;
    }
    if (left.origin_stream_day_id != right.origin_stream_day_id) {
        return left.origin_stream_day_id < right.origin_stream_day_id;
    }
    if (left.family != right.family) {
        return static_cast<std::uint16_t>(left.family) <
               static_cast<std::uint16_t>(right.family);
    }
    return left.shard_id < right.shard_id;
}

bool FactorInputWatermarkKeyEqualV1(
    const FactorInputWatermarkKeyV1& left,
    const FactorInputWatermarkKeyV1& right) noexcept {
    return !FactorInputWatermarkKeyLessV1(left, right) &&
           !FactorInputWatermarkKeyLessV1(right, left);
}

bool FactorInputWatermarkEntryExactEqualV1(
    const FactorInputWatermarkEntryV1& left,
    const FactorInputWatermarkEntryV1& right) noexcept {
    return FactorInputWatermarkKeyEqualV1(left.key, right.key) &&
           left.canonical_cursor == right.canonical_cursor &&
           left.max_consumed_origin_wal_end_pos ==
               right.max_consumed_origin_wal_end_pos &&
           left.observed_raw_durable_wal_pos ==
               right.observed_raw_durable_wal_pos &&
           left.clock_epoch.algorithm == right.clock_epoch.algorithm &&
           left.clock_epoch.digest == right.clock_epoch.digest &&
           left.clock_epoch.label == right.clock_epoch.label &&
           left.input_quality_flags == right.input_quality_flags;
}

bool FactorInputWatermarkSetExactEqualV1(
    const FactorInputWatermarkSetV1& left,
    const FactorInputWatermarkSetV1& right) noexcept {
    if (left.watermark_set_id != right.watermark_set_id ||
        left.trade_date != right.trade_date ||
        left.input_identity_sha256 != right.input_identity_sha256 ||
        left.entries.size() != right.entries.size()) {
        return false;
    }
    for (std::size_t index = 0U; index < left.entries.size(); ++index) {
        if (!FactorInputWatermarkEntryExactEqualV1(
                left.entries[index], right.entries[index])) {
            return false;
        }
    }
    return true;
}

FactorWatermarkErrorV1 EncodeFactorInputIdentityV1(
    const FactorInputWatermarkSetV1& value,
    std::vector<std::byte>* output) noexcept {
    if (output == nullptr) {
        return FactorWatermarkErrorV1::kNullOutput;
    }
    if (value.trade_date == 0U) {
        return FactorWatermarkErrorV1::kInvalidTradeDate;
    }
    if (value.entries.empty() ||
        value.entries.size() > kFactorWatermarkMaximumEntriesV1 ||
        value.entries.size() >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max())) {
        return FactorWatermarkErrorV1::kInvalidEntryCount;
    }
    constexpr std::size_t kEntryWireBytes = 90U;
    const std::size_t prefix_bytes =
        kFactorInputIdentityDomainV1.size() + 1U + 8U;
    if (value.entries.size() >
        (std::numeric_limits<std::size_t>::max() - prefix_bytes) /
            kEntryWireBytes) {
        return FactorWatermarkErrorV1::kSizeOverflow;
    }
    for (std::size_t index = 0U; index < value.entries.size(); ++index) {
        if (!EntryValid(value.entries[index])) {
            return FactorWatermarkErrorV1::kInvalidEntry;
        }
        if (index != 0U) {
            if (!FactorInputWatermarkKeyLessV1(
                    value.entries[index - 1U].key,
                    value.entries[index].key)) {
                return FactorInputWatermarkKeyEqualV1(
                           value.entries[index - 1U].key,
                           value.entries[index].key)
                           ? FactorWatermarkErrorV1::kDuplicateKey
                           : FactorWatermarkErrorV1::kNotCanonical;
            }
        }
    }
    try {
        std::vector<std::byte> candidate;
        candidate.reserve(
            prefix_bytes + value.entries.size() * kEntryWireBytes);
        for (const char character : kFactorInputIdentityDomainV1) {
            candidate.push_back(static_cast<std::byte>(character));
        }
        candidate.push_back(std::byte{0U});
        AppendU32Le(value.trade_date, &candidate);
        AppendU32Le(
            static_cast<std::uint32_t>(value.entries.size()), &candidate);
        for (const FactorInputWatermarkEntryV1& entry : value.entries) {
            AppendU32Le(entry.key.source_stream_id, &candidate);
            AppendU32Le(entry.key.origin_capture_date, &candidate);
            candidate.insert(
                candidate.end(),
                entry.key.origin_stream_day_id.begin(),
                entry.key.origin_stream_day_id.end());
            AppendU16Le(
                static_cast<std::uint16_t>(entry.key.family), &candidate);
            AppendU32Le(entry.key.shard_id, &candidate);
            AppendU64Le(entry.canonical_cursor, &candidate);
            AppendU64Le(
                entry.max_consumed_origin_wal_end_pos, &candidate);
            AppendU32Le(entry.clock_epoch.algorithm, &candidate);
            candidate.insert(
                candidate.end(),
                entry.clock_epoch.digest.begin(),
                entry.clock_epoch.digest.end());
            AppendU64Le(entry.input_quality_flags, &candidate);
        }
        *output = std::move(candidate);
    } catch (const std::bad_alloc&) {
        return FactorWatermarkErrorV1::kResourceExhausted;
    } catch (...) {
        return FactorWatermarkErrorV1::kResourceExhausted;
    }
    return FactorWatermarkErrorV1::kNone;
}

FactorWatermarkErrorV1 BuildFactorInputWatermarkSetV1(
    std::uint64_t watermark_set_id,
    std::uint32_t trade_date,
    std::span<const FactorInputWatermarkEntryV1> entries,
    FactorInputWatermarkSetV1* output) noexcept {
    if (output == nullptr) {
        return FactorWatermarkErrorV1::kNullOutput;
    }
    if (watermark_set_id == 0U) {
        return FactorWatermarkErrorV1::kInvalidSetId;
    }
    if (trade_date == 0U) {
        return FactorWatermarkErrorV1::kInvalidTradeDate;
    }
    if (entries.empty() || entries.size() > kFactorWatermarkMaximumEntriesV1) {
        return FactorWatermarkErrorV1::kInvalidEntryCount;
    }
    try {
        FactorInputWatermarkSetV1 candidate{};
        candidate.watermark_set_id = watermark_set_id;
        candidate.trade_date = trade_date;
        candidate.entries.assign(entries.begin(), entries.end());
        for (const FactorInputWatermarkEntryV1& entry : candidate.entries) {
            if (!EntryValid(entry)) {
                return FactorWatermarkErrorV1::kInvalidEntry;
            }
        }
        std::sort(
            candidate.entries.begin(),
            candidate.entries.end(),
            [](const FactorInputWatermarkEntryV1& left,
               const FactorInputWatermarkEntryV1& right) {
                return FactorInputWatermarkKeyLessV1(left.key, right.key);
            });
        for (std::size_t index = 1U; index < candidate.entries.size(); ++index) {
            if (FactorInputWatermarkKeyEqualV1(
                    candidate.entries[index - 1U].key,
                    candidate.entries[index].key)) {
                return FactorWatermarkErrorV1::kDuplicateKey;
            }
        }
        std::vector<std::byte> identity_wire;
        const FactorWatermarkErrorV1 encode_error =
            EncodeFactorInputIdentityV1(candidate, &identity_wire);
        if (encode_error != FactorWatermarkErrorV1::kNone) {
            return encode_error;
        }
        candidate.input_identity_sha256 =
            l2flow::common::ComputeSha256(identity_wire);
        *output = std::move(candidate);
    } catch (const std::bad_alloc&) {
        return FactorWatermarkErrorV1::kResourceExhausted;
    } catch (...) {
        return FactorWatermarkErrorV1::kResourceExhausted;
    }
    return FactorWatermarkErrorV1::kNone;
}

FactorWatermarkErrorV1 ValidateFactorInputWatermarkSetV1(
    const FactorInputWatermarkSetV1& value) noexcept {
    if (value.watermark_set_id == 0U) {
        return FactorWatermarkErrorV1::kInvalidSetId;
    }
    std::vector<std::byte> wire;
    const FactorWatermarkErrorV1 error =
        EncodeFactorInputIdentityV1(value, &wire);
    if (error != FactorWatermarkErrorV1::kNone) {
        return error;
    }
    return l2flow::common::ComputeSha256(wire) ==
                   value.input_identity_sha256
               ? FactorWatermarkErrorV1::kNone
               : FactorWatermarkErrorV1::kHashMismatch;
}

std::string_view FactorDurabilityBarrierErrorNameV1(
    FactorDurabilityBarrierErrorV1 error) noexcept {
    switch (error) {
        case FactorDurabilityBarrierErrorV1::kNone:
            return "none";
        case FactorDurabilityBarrierErrorV1::kInvalidWatermark:
            return "invalid_watermark";
        case FactorDurabilityBarrierErrorV1::kInvalidAuthority:
            return "invalid_authority";
        case FactorDurabilityBarrierErrorV1::kDuplicateAuthority:
            return "duplicate_authority";
        case FactorDurabilityBarrierErrorV1::kMissingAuthority:
            return "missing_authority";
        case FactorDurabilityBarrierErrorV1::kNamespaceMismatch:
            return "namespace_mismatch";
        case FactorDurabilityBarrierErrorV1::kDurabilityLag:
            return "durability_lag";
        case FactorDurabilityBarrierErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "invalid_durability_barrier_error";
}

FactorDurabilityBarrierResultV1 CheckFactorDurabilityBarrierV1(
    const FactorInputWatermarkSetV1& watermark,
    std::span<const l2flow::ingress::RawControlSnapshot> authorities) noexcept {
    FactorDurabilityBarrierResultV1 result{};
    if (ValidateFactorInputWatermarkSetV1(watermark) !=
        FactorWatermarkErrorV1::kNone) {
        result.error = FactorDurabilityBarrierErrorV1::kInvalidWatermark;
        return result;
    }
    if (authorities.size() > kFactorWatermarkMaximumEntriesV1) {
        result.error = FactorDurabilityBarrierErrorV1::kInvalidAuthority;
        return result;
    }
    std::vector<const l2flow::ingress::RawControlSnapshot*>
        sorted_authorities;
    try {
        sorted_authorities.reserve(authorities.size());
        for (const auto& authority : authorities) {
            if (!AuthorityValid(authority)) {
                result.error =
                    FactorDurabilityBarrierErrorV1::kInvalidAuthority;
                return result;
            }
            sorted_authorities.push_back(&authority);
        }
        std::sort(
            sorted_authorities.begin(),
            sorted_authorities.end(),
            [](const l2flow::ingress::RawControlSnapshot* left,
               const l2flow::ingress::RawControlSnapshot* right) noexcept {
                return AuthorityNamespaceLess(*left, *right);
            });
    } catch (const std::bad_alloc&) {
        result.error = FactorDurabilityBarrierErrorV1::kResourceExhausted;
        return result;
    } catch (...) {
        result.error = FactorDurabilityBarrierErrorV1::kResourceExhausted;
        return result;
    }
    for (std::size_t index = 1U; index < sorted_authorities.size(); ++index) {
        if (SameAuthorityNamespace(
                *sorted_authorities[index - 1U],
                *sorted_authorities[index])) {
            result.error =
                FactorDurabilityBarrierErrorV1::kDuplicateAuthority;
            return result;
        }
    }
    std::size_t authority_index = 0U;
    for (std::size_t entry_index = 0U;
         entry_index < watermark.entries.size(); ++entry_index) {
        const FactorInputWatermarkEntryV1& entry =
            watermark.entries[entry_index];
        while (authority_index < sorted_authorities.size() &&
               AuthorityNamespaceLessThanKey(
                   *sorted_authorities[authority_index], entry.key)) {
            ++authority_index;
        }
        if (authority_index == sorted_authorities.size() ||
            KeyNamespaceLessThanAuthority(
                entry.key, *sorted_authorities[authority_index])) {
            result.error = FactorDurabilityBarrierErrorV1::kMissingAuthority;
            result.entry_index = entry_index;
            result.required_wal_end_pos =
                entry.max_consumed_origin_wal_end_pos;
            return result;
        }
        const l2flow::ingress::RawControlSnapshot* matched =
            sorted_authorities[authority_index];
        if (entry.max_consumed_origin_wal_end_pos >
            matched->durable_global_wal_pos) {
            result.error = FactorDurabilityBarrierErrorV1::kDurabilityLag;
            result.entry_index = entry_index;
            result.required_wal_end_pos =
                entry.max_consumed_origin_wal_end_pos;
            result.durable_wal_pos = matched->durable_global_wal_pos;
            return result;
        }
    }
    return result;
}

}  // namespace l2flow::factor
