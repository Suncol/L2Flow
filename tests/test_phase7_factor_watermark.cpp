#include "l2flow/factor/latest_factor_v1.h"
#include "l2flow/factor/watermark_table_v1.h"

#include "l2flow/common/sha256.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

namespace factor = l2flow::factor;
namespace canonical = l2flow::canonical;
namespace common = l2flow::common;
namespace ingress = l2flow::ingress;

namespace {

struct TestContext final {
    int failures = 0;

    void Expect(bool condition, std::string_view message) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << message << '\n';
        }
    }
};

template <std::size_t Size>
std::array<std::byte, Size> Pattern(std::uint8_t seed) {
    std::array<std::byte, Size> value{};
    for (std::size_t index = 0U; index < Size; ++index) {
        value[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(seed + index));
    }
    return value;
}

factor::FactorInputWatermarkEntryV1 Entry(
    std::uint32_t source,
    canonical::CanonicalEventTypeV1 family,
    std::uint32_t shard,
    std::uint64_t cursor,
    std::uint64_t wal,
    std::uint64_t observed,
    std::uint8_t seed) {
    factor::FactorInputWatermarkEntryV1 value{};
    value.key.source_stream_id = source;
    value.key.origin_capture_date = 20260722U;
    value.key.origin_stream_day_id = Pattern<16U>(seed);
    value.key.family = family;
    value.key.shard_id = shard;
    value.canonical_cursor = cursor;
    value.max_consumed_origin_wal_end_pos = wal;
    value.observed_raw_durable_wal_pos = observed;
    value.clock_epoch.algorithm = 1U;
    value.clock_epoch.digest = Pattern<32U>(0x40U);
    value.clock_epoch.label = seed;
    value.input_quality_flags = 0U;
    return value;
}

ingress::RawControlSnapshot Authority(
    const factor::FactorInputWatermarkEntryV1& entry,
    std::uint64_t durable) {
    ingress::RawControlSnapshot value{};
    value.writer_instance = Pattern<16U>(0xa0U);
    value.stream_day_id = entry.key.origin_stream_day_id;
    value.source_stream_id = entry.key.source_stream_id;
    value.capture_date = entry.key.origin_capture_date;
    value.segment_sequence = 1U;
    value.append_global_wal_pos = durable + 4096U;
    value.append_ingress_sequence = entry.canonical_cursor + 10U;
    value.append_segment_offset = value.append_global_wal_pos;
    value.durable_global_wal_pos = durable;
    value.durable_ingress_sequence = entry.canonical_cursor;
    value.durable_segment_offset = durable;
    return value;
}

factor::LatestFactorValueV1 Latest(
    const factor::FactorInputWatermarkSetV1& watermark) {
    factor::LatestFactorValueV1 value{};
    value.factor_id_sha256 = Pattern<32U>(0x11U);
    value.factor_version_sha256 = Pattern<32U>(0x22U);
    value.instrument_id = 7U;
    value.asof_ns = 1000;
    value.value = 1.25;
    value.valid = true;
    value.watermark_set_id = watermark.watermark_set_id;
    value.input_identity_sha256 = watermark.input_identity_sha256;
    value.clock_epoch = watermark.entries.front().clock_epoch;
    return value;
}

void CheckIdentityAndOrdering(TestContext* test) {
    std::array<factor::FactorInputWatermarkEntryV1, 2U> first_entries{
        Entry(2002U, canonical::CanonicalEventTypeV1::kTick,
              7U, 90U, 9000U, 9100U, 0x20U),
        Entry(1001U, canonical::CanonicalEventTypeV1::kSnapshot,
              7U, 20U, 2000U, 2100U, 0x10U)};
    factor::FactorInputWatermarkSetV1 first{};
    test->Expect(
        factor::BuildFactorInputWatermarkSetV1(
            1U, 20260722U, first_entries, &first) ==
            factor::FactorWatermarkErrorV1::kNone,
        "first watermark builds");
    test->Expect(
        first.entries[0].key.source_stream_id == 1001U &&
            first.entries[1].key.source_stream_id == 2002U,
        "watermark entries sort by exact key, independent of caller order");

    std::array<factor::FactorInputWatermarkEntryV1, 2U> second_entries{
        first_entries[1], first_entries[0]};
    second_entries[0].observed_raw_durable_wal_pos = 999999U;
    second_entries[1].observed_raw_durable_wal_pos = 888888U;
    second_entries[0].clock_epoch.label = 999U;
    second_entries[1].clock_epoch.label = 888U;
    factor::FactorInputWatermarkSetV1 second{};
    test->Expect(
        factor::BuildFactorInputWatermarkSetV1(
            2U, 20260722U, second_entries, &second) ==
            factor::FactorWatermarkErrorV1::kNone,
        "reordered/late-observation watermark builds");
    test->Expect(
        first.input_identity_sha256 == second.input_identity_sha256,
        "observed durable, clock label, set id and caller order are excluded from identity");

    std::vector<std::byte> first_wire;
    std::vector<std::byte> second_wire;
    test->Expect(
        factor::EncodeFactorInputIdentityV1(first, &first_wire) ==
                factor::FactorWatermarkErrorV1::kNone &&
            factor::EncodeFactorInputIdentityV1(second, &second_wire) ==
                factor::FactorWatermarkErrorV1::kNone &&
            first_wire == second_wire,
        "frozen identity wire is deterministic");
    test->Expect(
        first_wire.size() ==
            factor::kFactorInputIdentityDomainV1.size() + 1U + 8U +
                2U * 90U,
        "identity wire has exact documented field width");

    auto changed_entries = first_entries;
    changed_entries[0].canonical_cursor += 1U;
    factor::FactorInputWatermarkSetV1 changed{};
    test->Expect(
        factor::BuildFactorInputWatermarkSetV1(
            3U, 20260722U, changed_entries, &changed) ==
                factor::FactorWatermarkErrorV1::kNone &&
            changed.input_identity_sha256 != first.input_identity_sha256,
        "exclusive canonical cursor participates in identity");

    auto duplicate_entries = first_entries;
    duplicate_entries[1].key = duplicate_entries[0].key;
    test->Expect(
        factor::BuildFactorInputWatermarkSetV1(
            4U, 20260722U, duplicate_entries, &changed) ==
            factor::FactorWatermarkErrorV1::kDuplicateKey,
        "duplicate exact input key fails closed");

    // Filled after the implementation's frozen wire was independently
    // inspected; this guards accidental field/order/domain changes.
    test->Expect(
        common::Sha256Hex(first.input_identity_sha256) ==
            "61f141cb342195ea17e31ce2dc6ac884abb3e7d1150e3191aecd5533dd376726",
        "input identity SHA-256 golden");
}

void CheckDurabilityBarrier(TestContext* test) {
    std::array<factor::FactorInputWatermarkEntryV1, 2U> entries{
        Entry(1001U, canonical::CanonicalEventTypeV1::kSnapshot,
              3U, 20U, 8192U, 999999U, 0x10U),
        Entry(2002U, canonical::CanonicalEventTypeV1::kTick,
              3U, 90U, 16384U, 999999U, 0x20U)};
    factor::FactorInputWatermarkSetV1 watermark{};
    test->Expect(
        factor::BuildFactorInputWatermarkSetV1(
            11U, 20260722U, entries, &watermark) ==
            factor::FactorWatermarkErrorV1::kNone,
        "barrier watermark builds");
    std::array<ingress::RawControlSnapshot, 2U> authorities{
        Authority(watermark.entries[0], 12288U),
        Authority(watermark.entries[1], 16376U)};
    auto result = factor::CheckFactorDurabilityBarrierV1(
        watermark, authorities);
    test->Expect(
        result.error ==
                factor::FactorDurabilityBarrierErrorV1::kDurabilityLag &&
            result.required_wal_end_pos == 16384U &&
            result.durable_wal_pos == 16376U,
        "each exact source authority is checked independently; observed metadata is ignored");
    authorities[1] = Authority(watermark.entries[1], 16384U);
    result = factor::CheckFactorDurabilityBarrierV1(watermark, authorities);
    test->Expect(result.durable(), "all exact namespaces crossing barrier pass");
    result = factor::CheckFactorDurabilityBarrierV1(
        watermark, std::span(authorities).first(1U));
    test->Expect(
        result.error ==
            factor::FactorDurabilityBarrierErrorV1::kMissingAuthority,
        "a missing source authority blocks the full set");
    authorities[1] = authorities[0];
    result = factor::CheckFactorDurabilityBarrierV1(watermark, authorities);
    test->Expect(
        result.error ==
            factor::FactorDurabilityBarrierErrorV1::kDuplicateAuthority,
        "duplicate authority namespace is ambiguous and rejected");

    authorities = {
        Authority(watermark.entries[0], 12288U),
        Authority(watermark.entries[1], 16384U)};
    authorities[1].durable_segment_offset += 8U;
    result = factor::CheckFactorDurabilityBarrierV1(watermark, authorities);
    test->Expect(
        result.error ==
            factor::FactorDurabilityBarrierErrorV1::kInvalidAuthority,
        "malformed Raw cursor coordinates cannot act as durability authority");
}

void CheckLargeDurabilityBarrier(TestContext* test) {
    constexpr std::size_t kNamespaceCount = 4096U;
    std::vector<factor::FactorInputWatermarkEntryV1> entries;
    std::vector<ingress::RawControlSnapshot> authorities;
    entries.reserve(kNamespaceCount * 2U);
    authorities.reserve(kNamespaceCount);
    for (std::size_t index = 0U; index < kNamespaceCount; ++index) {
        const std::uint32_t source =
            100'000U + static_cast<std::uint32_t>(index);
        const std::uint8_t seed = static_cast<std::uint8_t>(index);
        auto snapshot = Entry(
            source, canonical::CanonicalEventTypeV1::kSnapshot,
            1U, 20U + index, 8192U, 12288U, seed);
        auto tick = snapshot;
        tick.key.family = canonical::CanonicalEventTypeV1::kTick;
        tick.key.shard_id = 2U;
        entries.push_back(snapshot);
        entries.push_back(tick);
        authorities.push_back(Authority(snapshot, 12288U));
    }
    factor::FactorInputWatermarkSetV1 watermark{};
    test->Expect(
        factor::BuildFactorInputWatermarkSetV1(
            12U, 20260722U, entries, &watermark) ==
            factor::FactorWatermarkErrorV1::kNone,
        "large multi-family watermark builds");
    std::reverse(authorities.begin(), authorities.end());
    auto result = factor::CheckFactorDurabilityBarrierV1(
        watermark, authorities);
    test->Expect(
        result.durable(),
        "large reverse-ordered authority set merges by exact namespace");
    std::reverse(authorities.begin(), authorities.end());
    result = factor::CheckFactorDurabilityBarrierV1(
        watermark, authorities);
    test->Expect(
        result.durable(),
        "authority caller order does not affect the durability result");
}

void CheckAppendOnlyTable(TestContext* test) {
    const std::array entries{
        Entry(1002U, canonical::CanonicalEventTypeV1::kTick,
              1U, 10U, 1000U, 1200U, 0x12U)};
    factor::FactorInputWatermarkSetV1 watermark{};
    test->Expect(
        factor::BuildFactorInputWatermarkSetV1(
            77U, 20260722U, entries, &watermark) ==
            factor::FactorWatermarkErrorV1::kNone,
        "table watermark builds");
    factor::WatermarkTableV1 table;
    factor::LatestFactorSlotV1 latest_slot{};
    factor::LatestFactorValueV1 latest = Latest(watermark);
    auto bound_publish = factor::PublishLatestFactorWithWatermarkV1(
        table, &latest_slot, latest);
    test->Expect(
        bound_publish.watermark_error ==
                factor::WatermarkTableErrorV1::kNotFound &&
            factor::ReadLatestFactorV1(latest_slot, &latest) ==
                factor::LatestFactorErrorV1::kEmpty,
        "checked latest publication rejects an orphan watermark ID");
    auto appended = table.Append(watermark);
    test->Expect(
        appended.ok() && appended.disposition ==
            factor::WatermarkTableAppendDispositionV1::kAppended,
        "new watermark ID appends");
    appended = table.Append(watermark);
    test->Expect(
        appended.ok() && appended.disposition ==
            factor::WatermarkTableAppendDispositionV1::kAlreadyPresent &&
            table.size() == 1U,
        "exact reappend is idempotent without another table row");
    factor::FactorInputWatermarkSetV1 conflict = watermark;
    conflict.entries[0].observed_raw_durable_wal_pos += 1U;
    appended = table.Append(conflict);
    test->Expect(
        appended.error == factor::WatermarkTableErrorV1::kIdConflict,
        "same run-local ID cannot name a different full map");

    std::shared_ptr<const factor::FactorInputWatermarkSetV1> resolved;
    test->Expect(
        table.Resolve(77U, &resolved) ==
                factor::WatermarkTableErrorV1::kNone &&
            resolved != nullptr &&
            factor::FactorInputWatermarkSetExactEqualV1(
                *resolved, watermark),
        "append-only table resolves full watermark map");
    latest = Latest(watermark);
    resolved.reset();
    test->Expect(
        factor::ResolveLatestFactorWatermarkV1(
            table, latest, &resolved) ==
                factor::WatermarkTableErrorV1::kNone &&
            resolved != nullptr,
        "latest-factor watermark reference resolves with matching stable identity");
    bound_publish = factor::PublishLatestFactorWithWatermarkV1(
        table, &latest_slot, latest);
    test->Expect(
        bound_publish.ok() &&
            bound_publish.publish.disposition ==
                factor::LatestFactorPublishDispositionV1::kPublished,
        "checked latest publication requires and uses a resolvable watermark");
    latest.input_identity_sha256[0] ^= std::byte{0xffU};
    test->Expect(
        factor::ResolveLatestFactorWatermarkV1(
            table, latest, &resolved) ==
            factor::WatermarkTableErrorV1::kIdentityMismatch,
        "table ID cannot resolve a conflicting latest input identity");
    factor::LatestFactorSlotV1 mismatch_slot{};
    bound_publish = factor::PublishLatestFactorWithWatermarkV1(
        table, &mismatch_slot, latest);
    factor::LatestFactorValueV1 empty{};
    test->Expect(
        bound_publish.watermark_error ==
                factor::WatermarkTableErrorV1::kIdentityMismatch &&
            factor::ReadLatestFactorV1(mismatch_slot, &empty) ==
                factor::LatestFactorErrorV1::kEmpty,
        "checked latest publication rejects a mismatched input identity");
}

}  // namespace

int main() {
    TestContext test;
    CheckIdentityAndOrdering(&test);
    CheckDurabilityBarrier(&test);
    CheckLargeDurabilityBarrier(&test);
    CheckAppendOnlyTable(&test);
    if (test.failures != 0) {
        std::cerr << test.failures << " Phase 7 watermark checks failed\n";
        return 1;
    }
    std::cout << "Phase 7 watermark checks passed\n";
    return 0;
}
