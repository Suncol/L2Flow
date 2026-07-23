#pragma once

#include "l2flow/canonical/sequence_guard_v1.h"
#include "l2flow/market/market_types_v1.h"

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace l2flow::canonical {

// The exchange business-sequence domain is distinct from Raw ingress order
// and from the vendor-head SequenceID domain.
struct MarketBusinessSequenceIdentityV1 final {
    bool present = false;
    SequenceScopeKindV1 kind =
        SequenceScopeKindV1::kShanghaiChannel;
    std::uint32_t channel = 0U;
    std::uint64_t sequence = 0U;
    bool sequence_valid = false;
};

[[nodiscard]] MarketBusinessSequenceIdentityV1
MarketBusinessSequenceIdentityOfV1(
    const l2flow::market::ShanghaiSnapshotV1&) noexcept;
[[nodiscard]] MarketBusinessSequenceIdentityV1
MarketBusinessSequenceIdentityOfV1(
    const l2flow::market::ShanghaiTickV1& event) noexcept;
[[nodiscard]] MarketBusinessSequenceIdentityV1
MarketBusinessSequenceIdentityOfV1(
    const l2flow::market::ShenzhenSnapshotV1&) noexcept;
[[nodiscard]] MarketBusinessSequenceIdentityV1
MarketBusinessSequenceIdentityOfV1(
    const l2flow::market::ShenzhenOrderV1& event) noexcept;
[[nodiscard]] MarketBusinessSequenceIdentityV1
MarketBusinessSequenceIdentityOfV1(
    const l2flow::market::ShenzhenTransactionV1& event) noexcept;
[[nodiscard]] MarketBusinessSequenceIdentityV1
MarketBusinessSequenceIdentityOfV1(
    const l2flow::market::DecodedMarketEventV1& event) noexcept;

enum class MarketSequenceEvidenceErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kNoBusinessSequence,
    kValueTooLarge,
    kResourceExhausted,
    kUnexpectedFailure,
};

[[nodiscard]] std::string_view MarketSequenceEvidenceErrorNameV1(
    MarketSequenceEvidenceErrorV1 error) noexcept;

// Within the caller-owned
// (capture_date, source_stream_id, stream_day_id, ServiceID, MessageID)
// scope, SequenceID is the key. Evidence is exactly ServiceVersion,
// MessageEncoding, LocalTime and the complete body. HeadSize/MessageSize
// framing and local receive/ingress metadata are excluded.
[[nodiscard]] MarketSequenceEvidenceErrorV1
BuildVendorSequenceEvidenceV1(
    const l2flow::market::MarketMessageViewV1& message,
    std::vector<std::byte>* output) noexcept;

// Exchange evidence deliberately excludes any phase inherited from prior SH
// status state. Call this on the kDeferred decoder result before attribution.
[[nodiscard]] MarketSequenceEvidenceErrorV1
BuildExchangeSequenceEvidenceV1(
    const l2flow::market::ShanghaiTickV1& event,
    std::vector<std::byte>* output) noexcept;
[[nodiscard]] MarketSequenceEvidenceErrorV1
BuildExchangeSequenceEvidenceV1(
    const l2flow::market::ShenzhenOrderV1& event,
    std::vector<std::byte>* output) noexcept;
[[nodiscard]] MarketSequenceEvidenceErrorV1
BuildExchangeSequenceEvidenceV1(
    const l2flow::market::ShenzhenTransactionV1& event,
    std::vector<std::byte>* output) noexcept;
[[nodiscard]] MarketSequenceEvidenceErrorV1
BuildExchangeSequenceEvidenceV1(
    const l2flow::market::DecodedMarketEventV1& event,
    std::vector<std::byte>* output) noexcept;

}  // namespace l2flow::canonical
