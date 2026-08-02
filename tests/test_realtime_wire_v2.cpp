#include "l2flow/ipc/realtime_wire_v2.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <string_view>

namespace {

namespace ipc = l2flow::ipc;

static_assert(ipc::kRealtimeWireMajorV2 == 2U);
static_assert(ipc::kRealtimeWireMinorV2 == 4U);
static_assert(
    ipc::kRealtimeShmMagicV2 ==
    std::array<std::uint8_t, 8U>{
        'L', '2', 'F', 'S', 'H', 'M', '2', '\0'});
static_assert(
    ipc::kRealtimeControlMagicV2 ==
    std::array<std::uint8_t, 8U>{
        'L', '2', 'F', 'C', 'T', 'L', '2', '\0'});

static_assert(sizeof(ipc::RealtimeWireHeaderV2) == 4096U);
static_assert(alignof(ipc::RealtimeWireHeaderV2) == 4096U);
static_assert(
    offsetof(ipc::RealtimeWireHeaderV2, status_publish_tag) %
        alignof(std::uint64_t) ==
    0U);
static_assert(
    offsetof(ipc::RealtimeWireHeaderV2, catalog_generation) %
        alignof(std::uint64_t) ==
    0U);
static_assert(
    offsetof(ipc::RealtimeWireHeaderV2, data_state_generation) %
        alignof(std::uint64_t) ==
    0U);
static_assert(
    offsetof(ipc::RealtimeWireHeaderV2, catalog_digest) %
        alignof(std::uint64_t) ==
    0U);
static_assert(
    offsetof(ipc::RealtimeWireHeaderV2, accepted_sequence) %
        alignof(std::uint64_t) ==
    0U);
static_assert(
    offsetof(ipc::RealtimeWireHeaderV2, applied_sequence) %
        alignof(std::uint64_t) ==
    0U);
static_assert(
    offsetof(ipc::RealtimeWireHeaderV2, bound_count) %
        alignof(std::uint32_t) ==
    0U);
static_assert(
    offsetof(
        ipc::RealtimeWireHeaderV2,
        kline_coverage_start_unix_ns) == 264U);

static_assert(sizeof(ipc::RealtimeWireInstrumentV2) == 128U);
static_assert(alignof(ipc::RealtimeWireInstrumentV2) == 64U);
static_assert(
    offsetof(ipc::RealtimeWireInstrumentV2, publish_tag) == 0U);
static_assert(
    offsetof(ipc::RealtimeWireInstrumentV2, instrument_id) == 8U);
static_assert(
    offsetof(ipc::RealtimeWireInstrumentV2, ordinal) == 12U);
static_assert(
    offsetof(ipc::RealtimeWireInstrumentV2, binding_state) == 16U);
static_assert(
    offsetof(ipc::RealtimeWireInstrumentV2, availability_flags) ==
    20U);
static_assert(
    offsetof(
        ipc::RealtimeWireInstrumentV2,
        security_id_source_offset) == 32U);
static_assert(
    offsetof(
        ipc::RealtimeWireInstrumentV2,
        first_ingress_sequence) == 56U);
static_assert(
    offsetof(ipc::RealtimeWireInstrumentV2, reserved) == 72U);

static_assert(sizeof(ipc::RealtimeWireCommonRecordV2) == 128U);
static_assert(
    offsetof(ipc::RealtimeWireCommonRecordV2, ordinal) == 12U);
static_assert(sizeof(ipc::RealtimeWireSnapshotPayloadV2) == 3104U);
static_assert(sizeof(ipc::RealtimeWireTickPayloadV2) == 336U);
static_assert(sizeof(ipc::RealtimeWireKLinePayloadV2) == 192U);
static_assert(
    offsetof(ipc::RealtimeWireKLinePayloadV2, coverage_flags) ==
    20U);
static_assert(sizeof(ipc::RealtimeWireSnapshotSlotV2) == 4096U);
static_assert(sizeof(ipc::RealtimeWireTickSlotV2) == 512U);
static_assert(sizeof(ipc::RealtimeWireKLineSlotV2) == 256U);
static_assert(sizeof(ipc::RealtimeControlRequestV2) == 40U);
static_assert(sizeof(ipc::RealtimeControlResponseV2) == 64U);

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

bool TestDefaultHeaderSemantics() {
    const ipc::RealtimeWireHeaderV2 header{};
    bool ok = true;
    ok &= Expect(
        header.capacity ==
            ipc::kRealtimeDefaultInstrumentCapacityV2,
        "default physical capacity is 65536");
    ok &= Expect(
        header.catalog_scope ==
            static_cast<std::uint32_t>(
                ipc::RealtimeCatalogScopeV2::
                    kDeclaredDailyAShare),
        "default catalog scope is DECLARED_DAILY_A_SHARE");
    ok &= Expect(
        header.coverage_complete == 1U,
        "default header requires declared complete catalog coverage");
    ok &= Expect(
        header.catalog_generation == 0U &&
            header.data_state_generation == 0U &&
            header.bound_count == 0U &&
            header.available_count == 0U &&
            header.snapshot_available_count == 0U &&
            header.tick_available_count == 0U &&
            header.factor_eligible_count == 0U,
        "uninitialized header generations and counts are empty");
    ok &= Expect(
        !ipc::RealtimeWireHeaderStatusValidV2(header),
        "uninitialized header is not a publishable V2.2 session");
    auto initialized = header;
    initialized.trade_date = 20260730U;
    initialized.catalog_trade_date = initialized.trade_date;
    initialized.catalog_version = 17U;
    initialized.catalog_generation = 1U;
    initialized.bound_count = initialized.capacity;
    ok &= Expect(
        ipc::RealtimeWireHeaderStatusValidV2(initialized),
        "frozen full catalog status satisfies V2.2 invariants");
    ok &= Expect(
        ipc::RealtimeStatusPublishTagStableV2(
            header.status_publish_tag) &&
            !ipc::RealtimeStatusPublishTagStableV2(1U) &&
            ipc::RealtimeStatusPublishTagStableV2(2U),
        "status publish tag uses even stable and odd write states");
    return ok;
}

bool TestCanonicalUnboundRow() {
    const ipc::RealtimeWireInstrumentV2 row{};
    const auto bytes = std::bit_cast<
        std::array<std::byte, sizeof(row)>>(row);
    bool ok = true;
    ok &= Expect(
        std::all_of(
            bytes.begin(),
            bytes.end(),
            [](std::byte value) noexcept {
                return value == std::byte{0U};
            }),
        "value-initialized UNBOUND row is all-zero bytes");
    ok &= Expect(
        ipc::RealtimeWireInstrumentStateValidV2(
            row, ipc::kRealtimeDefaultInstrumentCapacityV2),
        "canonical all-zero UNBOUND row is valid");

    ipc::RealtimeWireInstrumentV2 corrupt_unbound{};
    corrupt_unbound.security_id_offset = 1U;
    ok &= Expect(
        !ipc::RealtimeWireInstrumentStateValidV2(
            corrupt_unbound,
            ipc::kRealtimeDefaultInstrumentCapacityV2),
        "UNBOUND row rejects hidden nonzero identity state");
    return ok;
}

bool TestInstrumentStateInvariants() {
    ipc::RealtimeWireInstrumentV2 row{};
    row.publish_tag = 2U;
    row.instrument_id = 1U;
    row.ordinal = 0U;
    row.binding_state = static_cast<std::uint32_t>(
        ipc::RealtimeInstrumentBindingStateV2::kBoundNoData);
    row.market = 1U;
    row.security_id_source_length = 3U;
    row.security_id_length = 6U;

    bool ok = true;
    ok &= Expect(
        ipc::RealtimeWireInstrumentStateValidV2(
            row, ipc::kRealtimeDefaultInstrumentCapacityV2),
        "BOUND_NO_DATA row has identity but no availability");

    row.binding_state = static_cast<std::uint32_t>(
        ipc::RealtimeInstrumentBindingStateV2::kAvailable);
    row.availability_flags =
        ipc::kRealtimeInstrumentHasSnapshotV2 |
        ipc::kRealtimeInstrumentFactorEligibleV2;
    row.first_ingress_sequence = 7U;
    row.last_ingress_sequence = 11U;
    ok &= Expect(
        ipc::RealtimeWireInstrumentStateValidV2(
            row, ipc::kRealtimeDefaultInstrumentCapacityV2),
        "AVAILABLE row accepts ordered ingress bounds");

    row.availability_flags =
        ipc::kRealtimeInstrumentFactorEligibleV2;
    ok &= Expect(
        !ipc::RealtimeWireInstrumentStateValidV2(
            row, ipc::kRealtimeDefaultInstrumentCapacityV2),
        "factor eligibility requires snapshot availability");

    row.availability_flags =
        ipc::kRealtimeInstrumentHasSnapshotV2;
    row.last_ingress_sequence = 6U;
    ok &= Expect(
        !ipc::RealtimeWireInstrumentStateValidV2(
            row, ipc::kRealtimeDefaultInstrumentCapacityV2),
        "last ingress cannot precede first ingress");

    row.last_ingress_sequence = 11U;
    row.instrument_id = 2U;
    ok &= Expect(
        !ipc::RealtimeWireInstrumentStateValidV2(
            row, ipc::kRealtimeDefaultInstrumentCapacityV2),
        "instrument_id must equal ordinal plus one");
    return ok;
}

bool TestCountInvariants() {
    bool ok = true;
    ok &= Expect(
        ipc::RealtimeWireCountsValidV2(
            65'536U, 50'200U, 18'420U, 7'210U, 16'500U,
            6'990U),
        "representative count hierarchy is valid");
    ok &= Expect(
        !ipc::RealtimeWireCountsValidV2(
            65'536U, 65'537U, 1U, 1U, 1U, 1U),
        "bound_count cannot exceed capacity");
    ok &= Expect(
        !ipc::RealtimeWireCountsValidV2(
            65'536U, 10U, 11U, 1U, 1U, 1U),
        "available_count cannot exceed bound_count");
    ok &= Expect(
        !ipc::RealtimeWireCountsValidV2(
            65'536U, 10U, 5U, 6U, 1U, 1U),
        "snapshot count cannot exceed available_count");
    ok &= Expect(
        !ipc::RealtimeWireCountsValidV2(
            65'536U, 10U, 5U, 1U, 6U, 1U),
        "tick count cannot exceed available_count");
    ok &= Expect(
        !ipc::RealtimeWireCountsValidV2(
            65'536U, 10U, 5U, 2U, 3U, 3U),
        "factor eligible count cannot exceed snapshot count");
    return ok;
}

bool TestProcessingSequenceInvariants() {
    bool ok = true;
    std::uint64_t processing_lag_records = 99U;
    ok &= Expect(
        ipc::RealtimeWireProcessingSequencesValidV2(
            10'000'000U, 9'950'000U) &&
            ipc::RealtimeWireProcessingLagRecordsV2(
                10'000'000U,
                9'950'000U,
                &processing_lag_records) &&
            processing_lag_records == 50'000U,
        "processing lag is the exact accepted-minus-applied distance");

    processing_lag_records = 99U;
    ok &= Expect(
        !ipc::RealtimeWireProcessingSequencesValidV2(9U, 10U) &&
            !ipc::RealtimeWireProcessingLagRecordsV2(
                9U, 10U, &processing_lag_records) &&
            processing_lag_records == 99U,
        "applied cannot exceed accepted and failed lag leaves output intact");

    processing_lag_records = 99U;
    ok &= Expect(
        ipc::RealtimeWireProcessingLagRecordsV2(
            12U, 12U, &processing_lag_records) &&
            processing_lag_records == 0U,
        "a fully processed accepted prefix has zero lag");
    ok &= Expect(
        !ipc::RealtimeWireProcessingLagRecordsV2(12U, 12U, nullptr),
        "lag helpers reject null output");
    ok &= Expect(
        !ipc::RealtimeWireProcessingSequencesValidV2(
            std::numeric_limits<std::uint64_t>::max(), 0U),
        "UINT64_MAX remains the reserved unpublished sequence sentinel");
    return ok;
}

bool TestKLineCoverageFlagInvariants() {
    bool ok = true;
    ok &= Expect(
        ipc::RealtimeWireKLineCoverageFlagsValidV2(0U) &&
            ipc::RealtimeWireKLineCoverageFlagsValidV2(
                ipc::kRealtimeWireKLineProcessStartPartialV2) &&
            ipc::RealtimeWireKLineCoverageFlagsValidV2(
                ipc::kRealtimeWireKLineProcessStartPartialV2 |
                ipc::kRealtimeWireKLineNaturalWindowLeftTruncatedV2),
        "canonical KLine coverage flags are accepted");
    ok &= Expect(
        !ipc::RealtimeWireKLineCoverageFlagsValidV2(
            ipc::kRealtimeWireKLineNaturalWindowLeftTruncatedV2),
        "left-truncated KLine coverage requires process-start partial");
    ok &= Expect(
        !ipc::RealtimeWireKLineCoverageFlagsValidV2(1U << 2U),
        "unknown KLine coverage flag is rejected");
    return ok;
}

}  // namespace

int main() {
    bool ok = true;
    ok &= TestDefaultHeaderSemantics();
    ok &= TestCanonicalUnboundRow();
    ok &= TestInstrumentStateInvariants();
    ok &= TestCountInvariants();
    ok &= TestProcessingSequenceInvariants();
    ok &= TestKLineCoverageFlagInvariants();
    if (!ok) {
        return 1;
    }
    std::cout << "realtime Wire V2 tests passed\n";
    return 0;
}
