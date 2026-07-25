#pragma once

#include "l2flow/common/sha256.h"
#include "l2flow/market/market_types_v1.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

namespace l2flow::market {

// The registry treats identifiers as opaque bytes.  It does not trim,
// case-fold, transcode, or infer a market from either identifier component.
struct InstrumentKeyV1 final {
    MarketV1 market = MarketV1::kUnknown;
    std::vector<std::byte> security_id_source;
    std::vector<std::byte> security_id;
};

struct InstrumentKeyViewV1 final {
    MarketV1 market = MarketV1::kUnknown;
    std::span<const std::byte> security_id_source;
    std::span<const std::byte> security_id;
};

struct InstrumentRegistryEntryV1 final {
    // IDs are assigned by registry/reference data.  They are never derived
    // from std::hash or from the identifier bytes at runtime.
    std::uint32_t instrument_id = 0U;
    InstrumentKeyV1 key{};
    QuantityUnitV1 quantity_unit = QuantityUnitV1::kUnknown;
    SecurityTypeV1 security_type = SecurityTypeV1::kUnknown;
    AssetScopeV1 asset_scope = AssetScopeV1::kUnknown;
};

enum class InstrumentRegistryCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kZeroRegistryVersion,
    kInvalidMarket,
    kEmptySecurityId,
    kInvalidQuantityUnit,
    kInvalidSecurityType,
    kInvalidAssetScope,
    kZeroInstrumentId,
    kDuplicateKey,
    kDuplicateInstrumentId,
    kHashFailure,
    kResourceExhausted,
    kUnexpectedFailure,
};

enum class InstrumentRegistryLookupErrorV1 : std::uint8_t {
    kNone = 0U,
    kUnknownInstrument,
    kInvalidMarket,
    kEmptySecurityId,
    kInvalidByteSpan,
    kZeroInstrumentId,
};

// A failed or unknown lookup always retains the zero/unknown defaults.  The
// entry pointer is valid for the immutable registry's lifetime only when
// known() is true.
struct InstrumentRegistryLookupResultV1 final {
    InstrumentRegistryLookupErrorV1 error =
        InstrumentRegistryLookupErrorV1::kUnknownInstrument;
    std::uint32_t instrument_id = 0U;
    // Stable zero-based position in the registry's instrument_id ordering.
    // It is deliberately independent of entries(), whose public canonical
    // order remains the exact byte-key order used by the registry digest.
    std::size_t registry_ordinal =
        std::numeric_limits<std::size_t>::max();
    QuantityUnitV1 quantity_unit = QuantityUnitV1::kUnknown;
    SecurityTypeV1 security_type = SecurityTypeV1::kUnknown;
    AssetScopeV1 asset_scope = AssetScopeV1::kUnknown;
    const InstrumentRegistryEntryV1* entry = nullptr;

    [[nodiscard]] bool known() const noexcept {
        return error == InstrumentRegistryLookupErrorV1::kNone &&
               instrument_id != 0U &&
               registry_ordinal !=
                   std::numeric_limits<std::size_t>::max() &&
               entry != nullptr;
    }
};

inline constexpr std::string_view
    kInstrumentRegistrySha256DomainV1 =
        "L2FLOW_PHASE4_INSTRUMENT_REGISTRY_V1";

// Immutable, canonical registry.  Create() validates and owns a copy of every
// entry, sorts the copy by the exact byte key, and publishes the object only
// after its canonical SHA-256 has been finalized.  Consequently concurrent
// const lookups require no lock.
//
// Registry SHA-256 preimage (all integers unsigned little-endian):
//
//   ASCII(kInstrumentRegistrySha256DomainV1) || 0x00
//   || u64(registry_version) || u64(entry_count)
//   || for each entry in (market, security_id_source, security_id) byte order:
//        u8(market)
//        || u64(security_id_source.size) || security_id_source bytes
//        || u64(security_id.size) || security_id bytes
//        || u32(instrument_id)
//        || u8(quantity_unit) || u8(security_type) || u8(asset_scope)
//
// Both variable byte strings are length-prefixed.  Input order therefore does
// not affect the digest, while every key and metadata field does.
class InstrumentRegistryV1 final {
public:
    InstrumentRegistryV1(const InstrumentRegistryV1&) = delete;
    InstrumentRegistryV1& operator=(const InstrumentRegistryV1&) = delete;
    InstrumentRegistryV1(InstrumentRegistryV1&&) = delete;
    InstrumentRegistryV1& operator=(InstrumentRegistryV1&&) = delete;
    ~InstrumentRegistryV1() = default;

    [[nodiscard]] static InstrumentRegistryCreateErrorV1 Create(
        std::uint64_t registry_version,
        std::span<const InstrumentRegistryEntryV1> entries,
        std::unique_ptr<InstrumentRegistryV1>* output) noexcept;

    [[nodiscard]] InstrumentRegistryLookupResultV1 Lookup(
        const InstrumentKeyViewV1& key) const noexcept;
    [[nodiscard]] InstrumentRegistryLookupResultV1 Lookup(
        const InstrumentKeyV1& key) const noexcept;
    [[nodiscard]] InstrumentRegistryLookupResultV1 Lookup(
        MarketV1 market,
        std::string_view security_id_source,
        std::string_view security_id) const noexcept;
    [[nodiscard]] InstrumentRegistryLookupResultV1 LookupById(
        std::uint32_t instrument_id) const noexcept;

    [[nodiscard]] std::uint64_t registry_version() const noexcept {
        return registry_version_;
    }
    [[nodiscard]] const l2flow::common::Sha256Digest&
    registry_sha256() const noexcept {
        return registry_sha256_;
    }
    [[nodiscard]] std::span<const InstrumentRegistryEntryV1>
    entries() const noexcept {
        return entries_;
    }
    [[nodiscard]] std::size_t size() const noexcept {
        return entries_.size();
    }
    [[nodiscard]] bool empty() const noexcept {
        return entries_.empty();
    }

private:
    struct IdIndexEntryV1 final {
        std::uint32_t instrument_id = 0U;
        std::size_t entry_index = 0U;
    };

    InstrumentRegistryV1(
        std::uint64_t registry_version,
        std::vector<InstrumentRegistryEntryV1> entries,
        std::vector<IdIndexEntryV1> id_index,
        std::vector<std::size_t> entry_index_to_registry_ordinal,
        l2flow::common::Sha256Digest registry_sha256) noexcept;

    std::uint64_t registry_version_ = 0U;
    std::vector<InstrumentRegistryEntryV1> entries_;
    std::vector<IdIndexEntryV1> id_index_;
    std::vector<std::size_t> entry_index_to_registry_ordinal_;
    l2flow::common::Sha256Digest registry_sha256_{};
};

}  // namespace l2flow::market
