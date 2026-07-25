#include "l2flow/market/instrument_registry.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <span>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace l2flow::market {
namespace {

enum class ByteOrderV1 : std::uint8_t {
    kLess = 0U,
    kEqual,
    kGreater,
};

ByteOrderV1 CompareBytes(
    std::span<const std::byte> left,
    std::span<const std::byte> right) noexcept {
    const std::size_t common_size =
        std::min(left.size(), right.size());
    for (std::size_t index = 0U; index < common_size; ++index) {
        const std::uint8_t left_byte =
            std::to_integer<std::uint8_t>(left[index]);
        const std::uint8_t right_byte =
            std::to_integer<std::uint8_t>(right[index]);
        if (left_byte < right_byte) {
            return ByteOrderV1::kLess;
        }
        if (left_byte > right_byte) {
            return ByteOrderV1::kGreater;
        }
    }
    if (left.size() < right.size()) {
        return ByteOrderV1::kLess;
    }
    if (left.size() > right.size()) {
        return ByteOrderV1::kGreater;
    }
    return ByteOrderV1::kEqual;
}

std::uint8_t MarketCode(MarketV1 value) noexcept {
    return static_cast<std::uint8_t>(value);
}

bool MarketValid(MarketV1 value) noexcept {
    switch (value) {
        case MarketV1::kShanghai:
        case MarketV1::kShenzhen:
            return true;
        case MarketV1::kUnknown:
            return false;
    }
    return false;
}

bool QuantityUnitValid(QuantityUnitV1 value) noexcept {
    switch (value) {
        case QuantityUnitV1::kUnknown:
        case QuantityUnitV1::kShare:
        case QuantityUnitV1::kFundUnit:
        case QuantityUnitV1::kLot:
        case QuantityUnitV1::kBondPiece:
        case QuantityUnitV1::kIndexUnit:
            return true;
    }
    return false;
}

bool SecurityTypeValid(SecurityTypeV1 value) noexcept {
    switch (value) {
        case SecurityTypeV1::kUnknown:
        case SecurityTypeV1::kEquity:
        case SecurityTypeV1::kFund:
        case SecurityTypeV1::kBond:
        case SecurityTypeV1::kConvertibleBond:
        case SecurityTypeV1::kIndex:
        case SecurityTypeV1::kWarrant:
        case SecurityTypeV1::kOption:
            return true;
    }
    return false;
}

bool AssetScopeValid(AssetScopeV1 value) noexcept {
    switch (value) {
        case AssetScopeV1::kUnknown:
        case AssetScopeV1::kDocumentedCore:
        case AssetScopeV1::kOutsideDocumentedCore:
            return true;
    }
    return false;
}

ByteOrderV1 CompareKey(
    const InstrumentKeyV1& left,
    const InstrumentKeyViewV1& right) noexcept {
    const std::uint8_t left_market = MarketCode(left.market);
    const std::uint8_t right_market = MarketCode(right.market);
    if (left_market < right_market) {
        return ByteOrderV1::kLess;
    }
    if (left_market > right_market) {
        return ByteOrderV1::kGreater;
    }
    const ByteOrderV1 source_order = CompareBytes(
        left.security_id_source, right.security_id_source);
    if (source_order != ByteOrderV1::kEqual) {
        return source_order;
    }
    return CompareBytes(left.security_id, right.security_id);
}

ByteOrderV1 CompareKey(
    const InstrumentKeyV1& left,
    const InstrumentKeyV1& right) noexcept {
    return CompareKey(
        left,
        InstrumentKeyViewV1{
            right.market,
            right.security_id_source,
            right.security_id});
}

bool KeyLess(
    const InstrumentRegistryEntryV1& left,
    const InstrumentRegistryEntryV1& right) noexcept {
    return CompareKey(left.key, right.key) == ByteOrderV1::kLess;
}

bool SpanShapeValid(std::span<const std::byte> value) noexcept {
    return value.empty() || value.data() != nullptr;
}

bool UpdateBytes(
    l2flow::common::Sha256Hasher* hasher,
    std::span<const std::byte> value) noexcept {
    return hasher != nullptr && hasher->Update(value);
}

bool UpdateU8(
    l2flow::common::Sha256Hasher* hasher,
    std::uint8_t value) noexcept {
    const std::array<std::byte, 1U> bytes{
        static_cast<std::byte>(value)};
    return UpdateBytes(hasher, bytes);
}

bool UpdateU32(
    l2flow::common::Sha256Hasher* hasher,
    std::uint32_t value) noexcept {
    std::array<std::byte, 4U> bytes{};
    for (std::size_t index = 0U; index < bytes.size(); ++index) {
        bytes[index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
    return UpdateBytes(hasher, bytes);
}

bool UpdateU64(
    l2flow::common::Sha256Hasher* hasher,
    std::uint64_t value) noexcept {
    std::array<std::byte, 8U> bytes{};
    for (std::size_t index = 0U; index < bytes.size(); ++index) {
        bytes[index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
    return UpdateBytes(hasher, bytes);
}

bool UpdateSize(
    l2flow::common::Sha256Hasher* hasher,
    std::size_t value) noexcept {
    if constexpr (
        sizeof(std::size_t) > sizeof(std::uint64_t)) {
        if (value > static_cast<std::size_t>(
                        std::numeric_limits<std::uint64_t>::max())) {
            return false;
        }
    }
    return UpdateU64(hasher, static_cast<std::uint64_t>(value));
}

bool ComputeRegistrySha256(
    std::uint64_t registry_version,
    std::span<const InstrumentRegistryEntryV1> entries,
    l2flow::common::Sha256Digest* digest) noexcept {
    if (digest == nullptr) {
        return false;
    }
    const std::span<const char> domain(
        kInstrumentRegistrySha256DomainV1.data(),
        kInstrumentRegistrySha256DomainV1.size());
    static constexpr std::array<std::byte, 1U> separator{
        std::byte{0U}};

    l2flow::common::Sha256Hasher hasher;
    if (!hasher.Update(std::as_bytes(domain)) ||
        !hasher.Update(separator) ||
        !UpdateU64(&hasher, registry_version) ||
        !UpdateSize(&hasher, entries.size())) {
        return false;
    }
    for (const InstrumentRegistryEntryV1& entry : entries) {
        if (!UpdateU8(&hasher, MarketCode(entry.key.market)) ||
            !UpdateSize(
                &hasher, entry.key.security_id_source.size()) ||
            !UpdateBytes(
                &hasher, entry.key.security_id_source) ||
            !UpdateSize(&hasher, entry.key.security_id.size()) ||
            !UpdateBytes(&hasher, entry.key.security_id) ||
            !UpdateU32(&hasher, entry.instrument_id) ||
            !UpdateU8(
                &hasher,
                static_cast<std::uint8_t>(entry.quantity_unit)) ||
            !UpdateU8(
                &hasher,
                static_cast<std::uint8_t>(entry.security_type)) ||
            !UpdateU8(
                &hasher,
                static_cast<std::uint8_t>(entry.asset_scope))) {
            return false;
        }
    }
    return hasher.Finalize(digest);
}

InstrumentRegistryLookupResultV1 KnownResult(
    const InstrumentRegistryEntryV1* entry,
    std::size_t registry_ordinal) noexcept {
    InstrumentRegistryLookupResultV1 result{};
    if (entry == nullptr || entry->instrument_id == 0U ||
        registry_ordinal ==
            std::numeric_limits<std::size_t>::max()) {
        return result;
    }
    result.error = InstrumentRegistryLookupErrorV1::kNone;
    result.instrument_id = entry->instrument_id;
    result.registry_ordinal = registry_ordinal;
    result.quantity_unit = entry->quantity_unit;
    result.security_type = entry->security_type;
    result.asset_scope = entry->asset_scope;
    result.entry = entry;
    return result;
}

}  // namespace

InstrumentRegistryV1::InstrumentRegistryV1(
    std::uint64_t registry_version,
    std::vector<InstrumentRegistryEntryV1> entries,
    std::vector<IdIndexEntryV1> id_index,
    std::vector<std::size_t> entry_index_to_registry_ordinal,
    l2flow::common::Sha256Digest registry_sha256) noexcept
    : registry_version_(registry_version),
      entries_(std::move(entries)),
      id_index_(std::move(id_index)),
      entry_index_to_registry_ordinal_(
          std::move(entry_index_to_registry_ordinal)),
      registry_sha256_(registry_sha256) {}

InstrumentRegistryCreateErrorV1 InstrumentRegistryV1::Create(
    std::uint64_t registry_version,
    std::span<const InstrumentRegistryEntryV1> entries,
    std::unique_ptr<InstrumentRegistryV1>* output) noexcept {
    if (output == nullptr) {
        return InstrumentRegistryCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (registry_version == 0U) {
        return InstrumentRegistryCreateErrorV1::kZeroRegistryVersion;
    }

    try {
        std::vector<InstrumentRegistryEntryV1> sorted_entries(
            entries.begin(), entries.end());
        for (const InstrumentRegistryEntryV1& entry : sorted_entries) {
            if (!MarketValid(entry.key.market)) {
                return InstrumentRegistryCreateErrorV1::kInvalidMarket;
            }
            if (entry.key.security_id.empty()) {
                return InstrumentRegistryCreateErrorV1::kEmptySecurityId;
            }
            if (!QuantityUnitValid(entry.quantity_unit)) {
                return InstrumentRegistryCreateErrorV1::
                    kInvalidQuantityUnit;
            }
            if (!SecurityTypeValid(entry.security_type)) {
                return InstrumentRegistryCreateErrorV1::
                    kInvalidSecurityType;
            }
            if (!AssetScopeValid(entry.asset_scope)) {
                return InstrumentRegistryCreateErrorV1::
                    kInvalidAssetScope;
            }
            if (entry.instrument_id == 0U) {
                return InstrumentRegistryCreateErrorV1::
                    kZeroInstrumentId;
            }
        }

        std::sort(
            sorted_entries.begin(), sorted_entries.end(), KeyLess);
        for (std::size_t index = 1U;
             index < sorted_entries.size();
             ++index) {
            if (CompareKey(
                    sorted_entries[index - 1U].key,
                    sorted_entries[index].key) ==
                ByteOrderV1::kEqual) {
                return InstrumentRegistryCreateErrorV1::kDuplicateKey;
            }
        }

        std::vector<IdIndexEntryV1> id_index;
        id_index.reserve(sorted_entries.size());
        for (std::size_t index = 0U;
             index < sorted_entries.size();
             ++index) {
            id_index.push_back(IdIndexEntryV1{
                sorted_entries[index].instrument_id, index});
        }
        std::sort(
            id_index.begin(),
            id_index.end(),
            [](const IdIndexEntryV1& left,
               const IdIndexEntryV1& right) noexcept {
                return left.instrument_id < right.instrument_id;
            });
        for (std::size_t index = 1U;
             index < id_index.size();
             ++index) {
            if (id_index[index - 1U].instrument_id ==
                id_index[index].instrument_id) {
                return InstrumentRegistryCreateErrorV1::
                    kDuplicateInstrumentId;
            }
        }

        std::vector<std::size_t> entry_index_to_registry_ordinal(
            sorted_entries.size(),
            std::numeric_limits<std::size_t>::max());
        for (std::size_t registry_ordinal = 0U;
             registry_ordinal < id_index.size();
             ++registry_ordinal) {
            const std::size_t entry_index =
                id_index[registry_ordinal].entry_index;
            if (entry_index >=
                    entry_index_to_registry_ordinal.size()) {
                return InstrumentRegistryCreateErrorV1::
                    kUnexpectedFailure;
            }
            entry_index_to_registry_ordinal[entry_index] =
                registry_ordinal;
        }

        l2flow::common::Sha256Digest registry_sha256{};
        if (!ComputeRegistrySha256(
                registry_version,
                sorted_entries,
                &registry_sha256)) {
            return InstrumentRegistryCreateErrorV1::kHashFailure;
        }
        output->reset(new InstrumentRegistryV1(
            registry_version,
            std::move(sorted_entries),
            std::move(id_index),
            std::move(entry_index_to_registry_ordinal),
            registry_sha256));
        return InstrumentRegistryCreateErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return InstrumentRegistryCreateErrorV1::kResourceExhausted;
    } catch (const std::length_error&) {
        return InstrumentRegistryCreateErrorV1::kResourceExhausted;
    } catch (...) {
        return InstrumentRegistryCreateErrorV1::kUnexpectedFailure;
    }
}

InstrumentRegistryLookupResultV1 InstrumentRegistryV1::Lookup(
    const InstrumentKeyViewV1& key) const noexcept {
    InstrumentRegistryLookupResultV1 result{};
    if (!MarketValid(key.market)) {
        result.error =
            InstrumentRegistryLookupErrorV1::kInvalidMarket;
        return result;
    }
    if (!SpanShapeValid(key.security_id_source) ||
        !SpanShapeValid(key.security_id)) {
        result.error =
            InstrumentRegistryLookupErrorV1::kInvalidByteSpan;
        return result;
    }
    if (key.security_id.empty()) {
        result.error =
            InstrumentRegistryLookupErrorV1::kEmptySecurityId;
        return result;
    }

    std::size_t first = 0U;
    std::size_t count = entries_.size();
    while (count != 0U) {
        const std::size_t step = count / 2U;
        const std::size_t middle = first + step;
        const ByteOrderV1 order =
            CompareKey(entries_[middle].key, key);
        if (order == ByteOrderV1::kLess) {
            first = middle + 1U;
            count -= step + 1U;
        } else {
            count = step;
        }
    }
    if (first == entries_.size() ||
        CompareKey(entries_[first].key, key) !=
            ByteOrderV1::kEqual) {
        return result;
    }
    if (first >= entry_index_to_registry_ordinal_.size()) {
        return result;
    }
    return KnownResult(
        &entries_[first],
        entry_index_to_registry_ordinal_[first]);
}

InstrumentRegistryLookupResultV1 InstrumentRegistryV1::Lookup(
    const InstrumentKeyV1& key) const noexcept {
    return Lookup(InstrumentKeyViewV1{
        key.market, key.security_id_source, key.security_id});
}

InstrumentRegistryLookupResultV1 InstrumentRegistryV1::Lookup(
    MarketV1 market,
    std::string_view security_id_source,
    std::string_view security_id) const noexcept {
    const std::span<const char> source_chars(
        security_id_source.data(), security_id_source.size());
    const std::span<const char> id_chars(
        security_id.data(), security_id.size());
    return Lookup(InstrumentKeyViewV1{
        market,
        std::as_bytes(source_chars),
        std::as_bytes(id_chars)});
}

InstrumentRegistryLookupResultV1 InstrumentRegistryV1::LookupById(
    std::uint32_t instrument_id) const noexcept {
    InstrumentRegistryLookupResultV1 result{};
    if (instrument_id == 0U) {
        result.error =
            InstrumentRegistryLookupErrorV1::kZeroInstrumentId;
        return result;
    }

    std::size_t first = 0U;
    std::size_t count = id_index_.size();
    while (count != 0U) {
        const std::size_t step = count / 2U;
        const std::size_t middle = first + step;
        if (id_index_[middle].instrument_id < instrument_id) {
            first = middle + 1U;
            count -= step + 1U;
        } else {
            count = step;
        }
    }
    if (first == id_index_.size() ||
        id_index_[first].instrument_id != instrument_id) {
        return result;
    }
    const std::size_t entry_index = id_index_[first].entry_index;
    if (entry_index >= entries_.size()) {
        return result;
    }
    return KnownResult(&entries_[entry_index], first);
}

}  // namespace l2flow::market
