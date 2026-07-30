#include "l2flow/market/daily_instrument_catalog_v2.h"

#include "l2flow/market/mainland_a_share_filter_v1.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <limits>
#include <new>
#include <span>
#include <stdexcept>
#include <utility>

namespace l2flow::market {
namespace {

enum class ByteOrder : std::uint8_t {
    kLess = 0U,
    kEqual,
    kGreater,
};

[[nodiscard]] ByteOrder CompareBytes(
    std::span<const std::byte> left,
    std::span<const std::byte> right) noexcept {
    const std::size_t common = std::min(left.size(), right.size());
    for (std::size_t index = 0U; index < common; ++index) {
        const std::uint8_t lhs =
            std::to_integer<std::uint8_t>(left[index]);
        const std::uint8_t rhs =
            std::to_integer<std::uint8_t>(right[index]);
        if (lhs < rhs) {
            return ByteOrder::kLess;
        }
        if (lhs > rhs) {
            return ByteOrder::kGreater;
        }
    }
    if (left.size() < right.size()) {
        return ByteOrder::kLess;
    }
    if (left.size() > right.size()) {
        return ByteOrder::kGreater;
    }
    return ByteOrder::kEqual;
}

[[nodiscard]] ByteOrder CompareKey(
    const InstrumentKeyV1& left,
    const InstrumentKeyViewV1& right) noexcept {
    const auto left_market = static_cast<std::uint8_t>(left.market);
    const auto right_market = static_cast<std::uint8_t>(right.market);
    if (left_market < right_market) {
        return ByteOrder::kLess;
    }
    if (left_market > right_market) {
        return ByteOrder::kGreater;
    }
    const ByteOrder source = CompareBytes(
        left.security_id_source, right.security_id_source);
    return source == ByteOrder::kEqual
               ? CompareBytes(left.security_id, right.security_id)
               : source;
}

[[nodiscard]] ByteOrder CompareKey(
    const InstrumentKeyV1& left,
    const InstrumentKeyV1& right) noexcept {
    return CompareKey(
        left,
        InstrumentKeyViewV1{
            right.market,
            right.security_id_source,
            right.security_id});
}

[[nodiscard]] bool KeyLess(
    const DailyInstrumentSourceEntryV2& left,
    const DailyInstrumentSourceEntryV2& right) noexcept {
    return CompareKey(left.key, right.key) == ByteOrder::kLess;
}

[[nodiscard]] bool SpanShapeValid(
    std::span<const std::byte> value) noexcept {
    return value.empty() || value.data() != nullptr;
}

[[nodiscard]] bool PrintableAscii(
    std::span<const std::byte> value) noexcept {
    return std::all_of(
        value.begin(),
        value.end(),
        [](std::byte byte) noexcept {
            const std::uint8_t character =
                std::to_integer<std::uint8_t>(byte);
            return character >= 0x20U && character <= 0x7eU;
        });
}

[[nodiscard]] bool RealtimeExactKeyShapeValid(
    const InstrumentKeyV1& key) noexcept {
    if (!SpanShapeValid(key.security_id_source) ||
        !SpanShapeValid(key.security_id) ||
        key.security_id.empty() ||
        !PrintableAscii(key.security_id)) {
        return false;
    }
    if (key.market == MarketV1::kShanghai) {
        return key.security_id_source.empty();
    }
    return key.market == MarketV1::kShenzhen &&
           !key.security_id_source.empty() &&
           PrintableAscii(key.security_id_source);
}

[[nodiscard]] bool MarketValid(MarketV1 market) noexcept {
    return market == MarketV1::kShanghai ||
           market == MarketV1::kShenzhen;
}

[[nodiscard]] bool MetadataValid(
    const InstrumentMetadataV2& metadata) noexcept {
    const auto quantity = static_cast<std::uint8_t>(
        metadata.quantity_unit);
    const auto security = static_cast<std::uint8_t>(
        metadata.security_type);
    const auto scope = static_cast<std::uint8_t>(
        metadata.asset_scope);
    return quantity <=
               static_cast<std::uint8_t>(QuantityUnitV1::kIndexUnit) &&
           security <=
               static_cast<std::uint8_t>(SecurityTypeV1::kOption) &&
           scope <= static_cast<std::uint8_t>(
                        AssetScopeV1::kOutsideDocumentedCore);
}

[[nodiscard]] bool MetadataEqual(
    const DailyInstrumentSourceEntryV2& left,
    const DailyInstrumentSourceEntryV2& right) noexcept {
    return left.metadata.quantity_unit ==
               right.metadata.quantity_unit &&
           left.metadata.security_type ==
               right.metadata.security_type &&
           left.metadata.asset_scope == right.metadata.asset_scope &&
           left.external_instrument_id == right.external_instrument_id;
}

[[nodiscard]] bool ValidTradeDate(std::uint32_t value) noexcept {
    const int year = static_cast<int>(value / 10'000U);
    const unsigned int month = (value / 100U) % 100U;
    const unsigned int day = value % 100U;
    return std::chrono::year_month_day{
               std::chrono::year(year),
               std::chrono::month(month),
               std::chrono::day(day)}
        .ok();
}

[[nodiscard]] std::uint8_t MarketScopeBit(MarketV1 market) noexcept {
    return market == MarketV1::kShanghai
               ? kDailyCatalogShanghaiScopeV2
               : market == MarketV1::kShenzhen
                     ? kDailyCatalogShenzhenScopeV2
                     : 0U;
}

[[nodiscard]] MainlandExchangeV1 ExchangeForMarket(
    MarketV1 market) noexcept {
    return market == MarketV1::kShanghai
               ? MainlandExchangeV1::kShanghai
               : market == MarketV1::kShenzhen
                     ? MainlandExchangeV1::kShenzhen
                     : MainlandExchangeV1::kUnknown;
}

[[nodiscard]] bool UpdateBytes(
    l2flow::common::Sha256Hasher* hasher,
    std::span<const std::byte> bytes) noexcept {
    return hasher != nullptr && hasher->Update(bytes);
}

[[nodiscard]] bool UpdateU8(
    l2flow::common::Sha256Hasher* hasher,
    std::uint8_t value) noexcept {
    const std::array<std::byte, 1U> bytes{static_cast<std::byte>(value)};
    return UpdateBytes(hasher, bytes);
}

[[nodiscard]] bool UpdateU32(
    l2flow::common::Sha256Hasher* hasher,
    std::uint32_t value) noexcept {
    std::array<std::byte, 4U> bytes{};
    for (std::size_t index = 0U; index < bytes.size(); ++index) {
        bytes[index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
    return UpdateBytes(hasher, bytes);
}

[[nodiscard]] bool UpdateU64(
    l2flow::common::Sha256Hasher* hasher,
    std::uint64_t value) noexcept {
    std::array<std::byte, 8U> bytes{};
    for (std::size_t index = 0U; index < bytes.size(); ++index) {
        bytes[index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
    return UpdateBytes(hasher, bytes);
}

[[nodiscard]] bool UpdateSizedBytes(
    l2flow::common::Sha256Hasher* hasher,
    std::span<const std::byte> bytes) noexcept {
    return UpdateU64(hasher, static_cast<std::uint64_t>(bytes.size())) &&
           UpdateBytes(hasher, bytes);
}

[[nodiscard]] bool ComputeDigest(
    const DailyInstrumentCatalogConfigV2& config,
    std::span<const DailyInstrumentCatalogEntryV2> entries,
    l2flow::common::Sha256Digest* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    const std::span<const char> domain(
        kDailyInstrumentCatalogDigestDomainV2.data(),
        kDailyInstrumentCatalogDigestDomainV2.size());
    constexpr std::array<std::byte, 1U> separator{std::byte{0U}};
    l2flow::common::Sha256Hasher hasher;
    if (!hasher.Update(std::as_bytes(domain)) ||
        !hasher.Update(separator) ||
        !UpdateU32(&hasher, config.trade_date) ||
        !UpdateU64(&hasher, config.catalog_version) ||
        !UpdateU8(&hasher, config.market_scope) ||
        !UpdateU8(&hasher, config.coverage_complete ? 1U : 0U) ||
        !UpdateU64(
            &hasher, static_cast<std::uint64_t>(entries.size()))) {
        return false;
    }
    for (const DailyInstrumentCatalogEntryV2& entry : entries) {
        if (!UpdateU8(
                &hasher,
                static_cast<std::uint8_t>(entry.key.market)) ||
            !UpdateSizedBytes(
                &hasher, entry.key.security_id_source) ||
            !UpdateSizedBytes(&hasher, entry.key.security_id) ||
            !UpdateU32(&hasher, entry.instrument_id) ||
            !UpdateU8(
                &hasher,
                static_cast<std::uint8_t>(
                    entry.metadata.quantity_unit)) ||
            !UpdateU8(
                &hasher,
                static_cast<std::uint8_t>(
                    entry.metadata.security_type)) ||
            !UpdateU8(
                &hasher,
                static_cast<std::uint8_t>(
                    entry.metadata.asset_scope)) ||
            !UpdateSizedBytes(
                &hasher, entry.external_instrument_id)) {
            return false;
        }
    }
    return hasher.Finalize(output);
}

}  // namespace

std::string_view DailyInstrumentCatalogCreateErrorNameV2(
    DailyInstrumentCatalogCreateErrorV2 error) noexcept {
    switch (error) {
        case DailyInstrumentCatalogCreateErrorV2::kNone:
            return "none";
        case DailyInstrumentCatalogCreateErrorV2::kNullOutput:
            return "null_output";
        case DailyInstrumentCatalogCreateErrorV2::kInvalidConfiguration:
            return "invalid_configuration";
        case DailyInstrumentCatalogCreateErrorV2::kInvalidMarket:
            return "invalid_market";
        case DailyInstrumentCatalogCreateErrorV2::kInvalidKey:
            return "invalid_key";
        case DailyInstrumentCatalogCreateErrorV2::kInvalidMetadata:
            return "invalid_metadata";
        case DailyInstrumentCatalogCreateErrorV2::
            kMarketOutsideDeclaredScope:
            return "market_outside_declared_scope";
        case DailyInstrumentCatalogCreateErrorV2::
            kDuplicateMetadataConflict:
            return "duplicate_metadata_conflict";
        case DailyInstrumentCatalogCreateErrorV2::kEmptyCatalog:
            return "empty_catalog";
        case DailyInstrumentCatalogCreateErrorV2::kTooManyInstruments:
            return "too_many_instruments";
        case DailyInstrumentCatalogCreateErrorV2::kHashFailure:
            return "hash_failure";
        case DailyInstrumentCatalogCreateErrorV2::kResourceExhausted:
            return "resource_exhausted";
        case DailyInstrumentCatalogCreateErrorV2::kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

std::string_view DailyInstrumentCatalogLookupErrorNameV2(
    DailyInstrumentCatalogLookupErrorV2 error) noexcept {
    switch (error) {
        case DailyInstrumentCatalogLookupErrorV2::kNone:
            return "none";
        case DailyInstrumentCatalogLookupErrorV2::kInvalidKey:
            return "invalid_key";
        case DailyInstrumentCatalogLookupErrorV2::kUnknownInstrument:
            return "unknown_instrument";
        case DailyInstrumentCatalogLookupErrorV2::kInvalidInstrumentId:
            return "invalid_instrument_id";
    }
    return "unknown";
}

DailyInstrumentCatalogV2::DailyInstrumentCatalogV2(
    DailyInstrumentCatalogConfigV2 config,
    std::vector<DailyInstrumentCatalogEntryV2> entries,
    std::size_t filtered_non_a_share_count,
    l2flow::common::Sha256Digest digest) noexcept
    : config_(config),
      entries_(std::move(entries)),
      filtered_non_a_share_count_(filtered_non_a_share_count),
      catalog_digest_(digest) {}

DailyInstrumentCatalogCreateErrorV2 DailyInstrumentCatalogV2::Create(
    DailyInstrumentCatalogConfigV2 config,
    std::span<const DailyInstrumentSourceEntryV2> source_entries,
    std::unique_ptr<DailyInstrumentCatalogV2>* output) noexcept {
    if (output == nullptr) {
        return DailyInstrumentCatalogCreateErrorV2::kNullOutput;
    }
    output->reset();
    if (!ValidTradeDate(config.trade_date) ||
        config.catalog_version == 0U ||
        config.session_epoch == 0U ||
        config.market_scope == 0U ||
        (config.market_scope & ~kDailyCatalogMainlandScopeV2) != 0U ||
        !config.coverage_complete) {
        return DailyInstrumentCatalogCreateErrorV2::
            kInvalidConfiguration;
    }
    if (source_entries.size() >
        static_cast<std::size_t>(
            std::numeric_limits<std::uint32_t>::max())) {
        return DailyInstrumentCatalogCreateErrorV2::
            kTooManyInstruments;
    }

    try {
        std::vector<DailyInstrumentSourceEntryV2> filtered;
        filtered.reserve(source_entries.size());
        std::size_t filtered_non_a_share_count = 0U;
        for (const DailyInstrumentSourceEntryV2& source : source_entries) {
            if (!MarketValid(source.key.market)) {
                return DailyInstrumentCatalogCreateErrorV2::kInvalidMarket;
            }
            if ((config.market_scope &
                 MarketScopeBit(source.key.market)) == 0U) {
                return DailyInstrumentCatalogCreateErrorV2::
                    kMarketOutsideDeclaredScope;
            }
            if (!RealtimeExactKeyShapeValid(source.key)) {
                return DailyInstrumentCatalogCreateErrorV2::kInvalidKey;
            }
            if (!MetadataValid(source.metadata)) {
                return DailyInstrumentCatalogCreateErrorV2::
                    kInvalidMetadata;
            }
            if (!IsMainlandAShareSecurityIdV1(
                    ExchangeForMarket(source.key.market),
                    source.key.security_id)) {
                ++filtered_non_a_share_count;
                continue;
            }
            filtered.push_back(source);
        }

        std::sort(filtered.begin(), filtered.end(), KeyLess);
        std::vector<DailyInstrumentCatalogEntryV2> entries;
        entries.reserve(filtered.size());
        for (std::size_t index = 0U; index < filtered.size(); ++index) {
            if (index != 0U &&
                CompareKey(
                    filtered[index - 1U].key,
                    filtered[index].key) == ByteOrder::kEqual) {
                if (!MetadataEqual(
                        filtered[index - 1U], filtered[index])) {
                    return DailyInstrumentCatalogCreateErrorV2::
                        kDuplicateMetadataConflict;
                }
                continue;
            }
            if (entries.size() >=
                static_cast<std::size_t>(
                    std::numeric_limits<std::uint32_t>::max())) {
                return DailyInstrumentCatalogCreateErrorV2::
                    kTooManyInstruments;
            }
            const std::size_t ordinal = entries.size();
            DailyInstrumentCatalogEntryV2 entry{};
            entry.instrument_id =
                static_cast<std::uint32_t>(ordinal + 1U);
            entry.ordinal = ordinal;
            entry.key = filtered[index].key;
            entry.metadata = filtered[index].metadata;
            entry.external_instrument_id =
                filtered[index].external_instrument_id;
            entries.push_back(std::move(entry));
        }
        if (entries.empty()) {
            return DailyInstrumentCatalogCreateErrorV2::kEmptyCatalog;
        }

        l2flow::common::Sha256Digest digest{};
        if (!ComputeDigest(config, entries, &digest)) {
            return DailyInstrumentCatalogCreateErrorV2::kHashFailure;
        }
        output->reset(new DailyInstrumentCatalogV2(
            config,
            std::move(entries),
            filtered_non_a_share_count,
            digest));
        return DailyInstrumentCatalogCreateErrorV2::kNone;
    } catch (const std::bad_alloc&) {
        return DailyInstrumentCatalogCreateErrorV2::kResourceExhausted;
    } catch (const std::length_error&) {
        return DailyInstrumentCatalogCreateErrorV2::kResourceExhausted;
    } catch (...) {
        return DailyInstrumentCatalogCreateErrorV2::kUnexpectedFailure;
    }
}

DailyInstrumentCatalogLookupResultV2 DailyInstrumentCatalogV2::Lookup(
    const InstrumentKeyViewV1& key) const noexcept {
    DailyInstrumentCatalogLookupResultV2 result{};
    if (!MarketValid(key.market) ||
        !SpanShapeValid(key.security_id_source) ||
        !SpanShapeValid(key.security_id) ||
        key.security_id.empty()) {
        result.error = DailyInstrumentCatalogLookupErrorV2::kInvalidKey;
        return result;
    }
    std::size_t first = 0U;
    std::size_t count = entries_.size();
    while (count != 0U) {
        const std::size_t step = count / 2U;
        const std::size_t middle = first + step;
        if (CompareKey(entries_[middle].key, key) == ByteOrder::kLess) {
            first = middle + 1U;
            count -= step + 1U;
        } else {
            count = step;
        }
    }
    if (first == entries_.size() ||
        CompareKey(entries_[first].key, key) != ByteOrder::kEqual) {
        return result;
    }
    result.error = DailyInstrumentCatalogLookupErrorV2::kNone;
    result.entry = &entries_[first];
    return result;
}

DailyInstrumentCatalogLookupResultV2 DailyInstrumentCatalogV2::Lookup(
    const InstrumentKeyV1& key) const noexcept {
    return Lookup(InstrumentKeyViewV1{
        key.market, key.security_id_source, key.security_id});
}

DailyInstrumentCatalogLookupResultV2
DailyInstrumentCatalogV2::LookupById(
    std::uint32_t instrument_id) const noexcept {
    DailyInstrumentCatalogLookupResultV2 result{};
    if (instrument_id == 0U) {
        result.error =
            DailyInstrumentCatalogLookupErrorV2::kInvalidInstrumentId;
        return result;
    }
    const DailyInstrumentCatalogEntryV2* entry = EntryAt(
        static_cast<std::size_t>(instrument_id - 1U));
    if (entry == nullptr || entry->instrument_id != instrument_id) {
        return result;
    }
    result.error = DailyInstrumentCatalogLookupErrorV2::kNone;
    result.entry = entry;
    return result;
}

const DailyInstrumentCatalogEntryV2* DailyInstrumentCatalogV2::EntryAt(
    std::size_t ordinal) const noexcept {
    return ordinal < entries_.size() ? &entries_[ordinal] : nullptr;
}

}  // namespace l2flow::market
