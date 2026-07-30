#pragma once

#include "l2flow/common/sha256.h"
#include "l2flow/market/instrument_identity_v2.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

namespace l2flow::market {

inline constexpr std::uint8_t kDailyCatalogShanghaiScopeV2 = 1U << 0U;
inline constexpr std::uint8_t kDailyCatalogShenzhenScopeV2 = 1U << 1U;
inline constexpr std::uint8_t kDailyCatalogMainlandScopeV2 =
    kDailyCatalogShanghaiScopeV2 | kDailyCatalogShenzhenScopeV2;

struct InstrumentMetadataV2 final {
    QuantityUnitV1 quantity_unit = QuantityUnitV1::kUnknown;
    SecurityTypeV1 security_type = SecurityTypeV1::kUnknown;
    AssetScopeV1 asset_scope = AssetScopeV1::kUnknown;
};

// One source row before canonical sorting. Instrument IDs supplied by a
// reference-data source are deliberately not accepted here: the session ID is
// assigned only after exact-key sorting and is always ordinal + 1.
struct DailyInstrumentSourceEntryV2 final {
    InstrumentKeyV1 key{};
    InstrumentMetadataV2 metadata{};
    // Optional opaque source-system identity. It participates in the catalog
    // digest but is never used as a runtime lookup fallback.
    std::vector<std::byte> external_instrument_id;
};

struct DailyInstrumentCatalogConfigV2 final {
    std::uint32_t trade_date = 0U;
    std::uint64_t catalog_version = 0U;
    std::uint64_t session_epoch = 0U;
    std::uint8_t market_scope = kDailyCatalogMainlandScopeV2;
    bool coverage_complete = false;
};

struct DailyInstrumentCatalogEntryV2 final {
    std::uint32_t instrument_id = 0U;
    std::size_t ordinal = 0U;
    InstrumentKeyV1 key{};
    InstrumentMetadataV2 metadata{};
    std::vector<std::byte> external_instrument_id;
};

enum class DailyInstrumentCatalogCreateErrorV2 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kInvalidMarket,
    kInvalidKey,
    kInvalidMetadata,
    kMarketOutsideDeclaredScope,
    kDuplicateMetadataConflict,
    kEmptyCatalog,
    kTooManyInstruments,
    kHashFailure,
    kResourceExhausted,
    kUnexpectedFailure,
};

enum class DailyInstrumentCatalogLookupErrorV2 : std::uint8_t {
    kNone = 0U,
    kInvalidKey,
    kUnknownInstrument,
    kInvalidInstrumentId,
};

[[nodiscard]] std::string_view DailyInstrumentCatalogCreateErrorNameV2(
    DailyInstrumentCatalogCreateErrorV2 error) noexcept;
[[nodiscard]] std::string_view DailyInstrumentCatalogLookupErrorNameV2(
    DailyInstrumentCatalogLookupErrorV2 error) noexcept;

struct DailyInstrumentCatalogLookupResultV2 final {
    DailyInstrumentCatalogLookupErrorV2 error =
        DailyInstrumentCatalogLookupErrorV2::kUnknownInstrument;
    const DailyInstrumentCatalogEntryV2* entry = nullptr;

    [[nodiscard]] bool known() const noexcept {
        return error == DailyInstrumentCatalogLookupErrorV2::kNone &&
               entry != nullptr && entry->instrument_id != 0U &&
               entry->instrument_id ==
                   static_cast<std::uint32_t>(entry->ordinal + 1U);
    }
};

inline constexpr std::string_view kDailyInstrumentCatalogDigestDomainV2 =
    "L2FLOW_DAILY_INSTRUMENT_CATALOG_V2";

// Immutable session catalog. Create validates the declared date/scope,
// filters non-A-share rows with the same classifier used by realtime
// admission, rejects key shapes that callback extraction cannot produce,
// sorts by the exact opaque key, coalesces byte-identical duplicates, assigns
// dense IDs, and computes a canonical SHA-256 digest.
// Create also freezes a bounded flat-hash side index for normal exact-key
// lookup. The canonical sorted table remains the identity source and the
// correctness fallback if a usable bounded hash distribution cannot be
// constructed.
// Every const lookup is allocation-free and lock-free.
class DailyInstrumentCatalogV2 final {
public:
    DailyInstrumentCatalogV2(const DailyInstrumentCatalogV2&) = delete;
    DailyInstrumentCatalogV2& operator=(
        const DailyInstrumentCatalogV2&) = delete;
    DailyInstrumentCatalogV2(DailyInstrumentCatalogV2&&) = delete;
    DailyInstrumentCatalogV2& operator=(DailyInstrumentCatalogV2&&) = delete;
    ~DailyInstrumentCatalogV2();

    [[nodiscard]] static DailyInstrumentCatalogCreateErrorV2 Create(
        DailyInstrumentCatalogConfigV2 config,
        std::span<const DailyInstrumentSourceEntryV2> source_entries,
        std::unique_ptr<DailyInstrumentCatalogV2>* output) noexcept;

    [[nodiscard]] DailyInstrumentCatalogLookupResultV2 Lookup(
        const InstrumentKeyViewV1& key) const noexcept;
    [[nodiscard]] DailyInstrumentCatalogLookupResultV2 Lookup(
        const InstrumentKeyV1& key) const noexcept;
    [[nodiscard]] DailyInstrumentCatalogLookupResultV2 LookupById(
        std::uint32_t instrument_id) const noexcept;
    [[nodiscard]] const DailyInstrumentCatalogEntryV2* EntryAt(
        std::size_t ordinal) const noexcept;

    [[nodiscard]] std::uint32_t trade_date() const noexcept {
        return config_.trade_date;
    }
    [[nodiscard]] std::uint64_t catalog_version() const noexcept {
        return config_.catalog_version;
    }
    [[nodiscard]] std::uint64_t session_epoch() const noexcept {
        return config_.session_epoch;
    }
    [[nodiscard]] std::uint8_t market_scope() const noexcept {
        return config_.market_scope;
    }
    [[nodiscard]] bool coverage_complete() const noexcept {
        return config_.coverage_complete;
    }
    [[nodiscard]] const l2flow::common::Sha256Digest& catalog_digest()
        const noexcept {
        return catalog_digest_;
    }
    [[nodiscard]] std::size_t instrument_count() const noexcept {
        return entries_.size();
    }
    [[nodiscard]] std::size_t filtered_non_a_share_count() const noexcept {
        return filtered_non_a_share_count_;
    }
    [[nodiscard]] std::span<const DailyInstrumentCatalogEntryV2> entries()
        const noexcept {
        return entries_;
    }

private:
    class LookupIndex;

    DailyInstrumentCatalogV2(
        DailyInstrumentCatalogConfigV2 config,
        std::vector<DailyInstrumentCatalogEntryV2> entries,
        std::size_t filtered_non_a_share_count,
        l2flow::common::Sha256Digest digest,
        std::unique_ptr<const LookupIndex> lookup_index) noexcept;

    DailyInstrumentCatalogConfigV2 config_{};
    std::vector<DailyInstrumentCatalogEntryV2> entries_;
    std::size_t filtered_non_a_share_count_ = 0U;
    l2flow::common::Sha256Digest catalog_digest_{};
    std::unique_ptr<const LookupIndex> lookup_index_;
};

}  // namespace l2flow::market
