#pragma once

#include "l2flow/canonical/canonical_schema_v1.h"
#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/ingress/raw_control_page.h"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace l2flow::factor {

inline constexpr std::size_t kFactorWatermarkMaximumEntriesV1 = 65'536U;
inline constexpr std::string_view kFactorInputIdentityDomainV1 =
    "l2flow.factor.input-identity.v1";

struct FactorInputWatermarkKeyV1 final {
    std::uint32_t source_stream_id = 0U;
    std::uint32_t origin_capture_date = 0U;
    l2flow::common::Identity128 origin_stream_day_id{};
    // Frozen wire uses CanonicalEventTypeV1's uint16 numeric value.
    l2flow::canonical::CanonicalEventTypeV1 family =
        l2flow::canonical::CanonicalEventTypeV1::kUnknown;
    std::uint32_t shard_id = 0U;
};

struct FactorInputWatermarkEntryV1 final {
    FactorInputWatermarkKeyV1 key{};
    // Exclusive next-record cursor; consumed interval ends immediately before
    // this value.
    std::uint64_t canonical_cursor = 0U;
    std::uint64_t max_consumed_origin_wal_end_pos = 0U;
    // Observation metadata only.  It is never used as durability authority
    // and is deliberately excluded from input_identity_sha256.
    std::uint64_t observed_raw_durable_wal_pos = 0U;
    l2flow::canonical::ClockEpochIdentityV1 clock_epoch{};
    std::uint64_t input_quality_flags = 0U;
};

struct FactorInputWatermarkSetV1 final {
    // Unique only within the current run/table namespace.
    std::uint64_t watermark_set_id = 0U;
    std::uint32_t trade_date = 0U;
    std::vector<FactorInputWatermarkEntryV1> entries;
    l2flow::common::Sha256Digest input_identity_sha256{};
};

enum class FactorWatermarkErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidSetId,
    kInvalidTradeDate,
    kInvalidEntryCount,
    kInvalidEntry,
    kDuplicateKey,
    kNotCanonical,
    kHashMismatch,
    kSizeOverflow,
    kResourceExhausted,
};

[[nodiscard]] std::string_view FactorWatermarkErrorNameV1(
    FactorWatermarkErrorV1 error) noexcept;

[[nodiscard]] bool FactorInputWatermarkKeyLessV1(
    const FactorInputWatermarkKeyV1& left,
    const FactorInputWatermarkKeyV1& right) noexcept;

[[nodiscard]] bool FactorInputWatermarkKeyEqualV1(
    const FactorInputWatermarkKeyV1& left,
    const FactorInputWatermarkKeyV1& right) noexcept;

[[nodiscard]] bool FactorInputWatermarkEntryExactEqualV1(
    const FactorInputWatermarkEntryV1& left,
    const FactorInputWatermarkEntryV1& right) noexcept;

[[nodiscard]] bool FactorInputWatermarkSetExactEqualV1(
    const FactorInputWatermarkSetV1& left,
    const FactorInputWatermarkSetV1& right) noexcept;

// Canonicalizes entries by exact key and computes the frozen identity below.
// Caller order has no effect on the result.
[[nodiscard]] FactorWatermarkErrorV1 BuildFactorInputWatermarkSetV1(
    std::uint64_t watermark_set_id,
    std::uint32_t trade_date,
    std::span<const FactorInputWatermarkEntryV1> entries,
    FactorInputWatermarkSetV1* output) noexcept;

[[nodiscard]] FactorWatermarkErrorV1 ValidateFactorInputWatermarkSetV1(
    const FactorInputWatermarkSetV1& value) noexcept;

// Frozen SHA-256 preimage, also returned by this encoder for Python/golden
// parity.  Every integer is unsigned little-endian:
//
//   "l2flow.factor.input-identity.v1\0"
//   || trade_date:u32 || entry_count:u32
//   || for each entry sorted by exact key:
//        source_stream_id:u32
//        origin_capture_date:u32
//        origin_stream_day_id:16 raw bytes
//        family:u16 (CanonicalEventTypeV1 numeric value)
//        shard_id:u32
//        canonical_cursor:u64 (exclusive next)
//        max_consumed_origin_wal_end_pos:u64
//        clock_epoch_algorithm:u32
//        clock_epoch_digest:32 raw bytes
//        input_quality_flags:u64
//
// Excluded by contract: clock label, watermark_set_id and
// observed_raw_durable_wal_pos.
[[nodiscard]] FactorWatermarkErrorV1 EncodeFactorInputIdentityV1(
    const FactorInputWatermarkSetV1& value,
    std::vector<std::byte>* output) noexcept;

enum class FactorDurabilityBarrierErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidWatermark,
    kInvalidAuthority,
    kDuplicateAuthority,
    kMissingAuthority,
    kNamespaceMismatch,
    kDurabilityLag,
    kResourceExhausted,
};

struct FactorDurabilityBarrierResultV1 final {
    FactorDurabilityBarrierErrorV1 error =
        FactorDurabilityBarrierErrorV1::kNone;
    std::size_t entry_index = 0U;
    std::uint64_t required_wal_end_pos = 0U;
    std::uint64_t durable_wal_pos = 0U;

    [[nodiscard]] bool durable() const noexcept {
        return error == FactorDurabilityBarrierErrorV1::kNone;
    }
};

[[nodiscard]] std::string_view FactorDurabilityBarrierErrorNameV1(
    FactorDurabilityBarrierErrorV1 error) noexcept;

// Each RawControlSnapshot is current external durability authority for its
// exact (capture_date, source_stream_id, stream_day_id) namespace.  The
// watermark's observed position is intentionally ignored.
[[nodiscard]] FactorDurabilityBarrierResultV1
CheckFactorDurabilityBarrierV1(
    const FactorInputWatermarkSetV1& watermark,
    std::span<const l2flow::ingress::RawControlSnapshot> authorities) noexcept;

}  // namespace l2flow::factor
