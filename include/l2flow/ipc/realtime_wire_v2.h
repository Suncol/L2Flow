#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

namespace l2flow::ipc {

inline constexpr std::array<std::uint8_t, 8U> kRealtimeShmMagicV2{
    'L', '2', 'F', 'S', 'H', 'M', '2', '\0'};
inline constexpr std::array<std::uint8_t, 8U> kRealtimeControlMagicV2{
    'L', '2', 'F', 'C', 'T', 'L', '2', '\0'};
inline constexpr std::uint16_t kRealtimeWireMajorV2 = 2U;
inline constexpr std::uint16_t kRealtimeWireMinorV2 = 2U;
inline constexpr std::uint32_t kRealtimeLittleEndianMarkerV2 =
    0x01020304U;
inline constexpr std::uint32_t kRealtimeDefaultInstrumentCapacityV2 =
    65'536U;
inline constexpr std::size_t kRealtimeWireRegionCountV2 = 9U;
inline constexpr std::size_t kRealtimeSnapshotSlotBytesV2 = 4096U;
inline constexpr std::size_t kRealtimeTickSlotBytesV2 = 512U;
inline constexpr std::size_t kRealtimeKLineSlotBytesV2 = 256U;
inline constexpr std::size_t kRealtimeKLineTableCountV2 = 2U;

enum class RealtimeServerStateV2 : std::uint32_t {
    kInitializing = 1U,
    kActive = 2U,
    kDraining = 3U,
    kStoppedClean = 4U,
    kFailed = 5U,
};

enum RealtimeHeaderFlagV2 : std::uint32_t {
    kRealtimeHeaderCoverageLostV2 = 1U << 0U,
    kRealtimeHeaderKLineEnabledV2 = 1U << 1U,
};

// V2.2 exposes the immutable, declared daily Shanghai+Shenzhen A-share
// catalog. It does not claim that every exchange security is subscribed or
// that every catalog instrument has produced data.
enum class RealtimeCatalogScopeV2 : std::uint32_t {
    kDeclaredDailyAShare = 2U,
};

enum class RealtimeInstrumentBindingStateV2 : std::uint32_t {
    // Zero remains the value-initialized prepublication representation.
    // A V2.2 ACTIVE mapping has no UNBOUND tail: every capacity row is
    // prepublished as kBoundNoData or kAvailable.
    kUnbound = 0U,
    kBinding = 1U,
    kBoundNoData = 2U,
    kAvailable = 3U,
};

enum RealtimeInstrumentAvailabilityFlagV2 : std::uint32_t {
    kRealtimeInstrumentHasSnapshotV2 = 1U << 0U,
    kRealtimeInstrumentHasTickV2 = 1U << 1U,
    kRealtimeInstrumentHasKLineV2 = 1U << 2U,
    kRealtimeInstrumentFactorEligibleV2 = 1U << 3U,
};

enum class RealtimeSelectionScopeV2 : std::uint32_t {
    kCatalogAll = 1U,
    kAvailableAny = 2U,
    kSnapshotAvailable = 3U,
    kTickAvailable = 4U,
    kFactorEligible = 5U,
    // Source-compatibility aliases. Wire V2.2 documentation and new code use
    // the catalog/availability names.
    kBound = kCatalogAll,
    kObservedAny = kAvailableAny,
};

enum RealtimeWireTickProjectionFlagV2 : std::uint32_t {
    kRealtimeWireTickRawTypeOmittedV2 = 1U << 0U,
    kRealtimeWireTickRawTickFlagOmittedV2 = 1U << 1U,
};

enum class RealtimeRegionKindV2 : std::uint32_t {
    kInstrumentRows = 1U,
    // A capacity-sized arena reserved before the service becomes active.
    // Instrument-row offsets are relative to this region.
    kInstrumentKeyArena = 2U,
    kKLineWindows = 3U,
    kLatestSnapshots = 4U,
    kLatestTicks = 5U,
    kLatestKLines = 6U,
    kTickRingSlots = 7U,
    kReserved8 = 8U,
    kReserved9 = 9U,
};

enum class RealtimeControlOpcodeV2 : std::uint16_t {
    kGetSession = 1U,
};

enum class RealtimeControlStatusV2 : std::uint16_t {
    kOk = 0U,
    kInvalidRequest = 1U,
    kUnsupportedVersion = 2U,
    kUnavailable = 3U,
};

// Digest words are represented as integers solely so a mutable catalog
// digest can be copied through lock-free uint64 atomic_ref operations. The
// in-memory bytes of the four words are the exact 32 digest bytes.
struct alignas(8) RealtimeWireDigest256V2 final {
    std::array<std::uint64_t, 4U> words{};
};
static_assert(sizeof(RealtimeWireDigest256V2) == 32U);
static_assert(alignof(RealtimeWireDigest256V2) == 8U);
static_assert(std::is_standard_layout_v<RealtimeWireDigest256V2>);

// Every protocol structure is fixed-width and little-endian. No C++ enum,
// bool, pointer, string, variant, span, size_t, or atomic object is embedded
// in shared memory.
struct RealtimeWireRegionDescriptorV2 final {
    std::uint32_t kind = 0U;
    std::uint16_t schema_major = 0U;
    std::uint16_t schema_minor = 0U;
    std::uint64_t offset = 0U;
    std::uint64_t length = 0U;
    std::uint64_t element_stride = 0U;
    std::uint64_t element_count = 0U;
    std::uint64_t capacity = 0U;
    std::uint32_t alignment = 0U;
    std::uint32_t flags = 0U;
    std::uint64_t reserved = 0U;
};
static_assert(sizeof(RealtimeWireRegionDescriptorV2) == 64U);
static_assert(std::is_standard_layout_v<RealtimeWireRegionDescriptorV2>);

// In V2.2 catalog_generation is the immutable value 1 because the complete
// capacity-sized identity table is published before ACTIVE.
// data_state_generation changes when availability or factor eligibility
// changes. Writers serialize updates covered by status_publish_tag:
//
//   even stable tag -> odd tag -> atomic field updates -> next even tag
//
// Readers acquire-copy the covered fields and accept them only when the same
// even tag is observed at both ends. Every covered mutable scalar, including
// the four catalog-digest words, is accessed through atomic_ref; the seqcount
// does not make non-atomic concurrent accesses valid.
struct alignas(4096) RealtimeWireHeaderV2 final {
    std::array<std::uint8_t, 8U> magic{};
    std::uint16_t abi_major = 0U;
    std::uint16_t abi_minor = 0U;
    std::uint32_t header_bytes = 0U;
    std::uint32_t endian_marker = 0U;
    // Independently accessed through atomic_ref.
    std::uint32_t server_state = 0U;
    std::uint64_t total_mapping_bytes = 0U;

    // Immutable session identity.
    std::array<std::uint8_t, 16U> run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
    // Independently accessed through atomic_ref.
    std::uint32_t flags = 0U;

    // Immutable layout and catalog semantics.
    std::uint32_t capacity = kRealtimeDefaultInstrumentCapacityV2;
    std::uint32_t window_count = 0U;
    std::uint32_t catalog_scope =
        static_cast<std::uint32_t>(
            RealtimeCatalogScopeV2::kDeclaredDailyAShare);
    std::uint32_t coverage_complete = 1U;
    std::uint32_t catalog_trade_date = 0U;
    std::uint32_t reserved_catalog = 0U;
    std::uint64_t catalog_version = 0U;
    RealtimeWireDigest256V2 layout_digest{};

    // Coherent live catalog/data-state status. All fields from this tag
    // through applied_sequence are accessed through atomic_ref.
    std::uint64_t status_publish_tag = 0U;
    std::uint64_t catalog_generation = 0U;
    std::uint64_t data_state_generation = 0U;
    RealtimeWireDigest256V2 catalog_digest{};
    std::uint32_t bound_count = 0U;
    std::uint32_t available_count = 0U;
    std::uint32_t snapshot_available_count = 0U;
    std::uint32_t tick_available_count = 0U;
    std::uint32_t factor_eligible_count = 0U;
    std::uint32_t reserved_count = 0U;
    std::uint64_t accepted_sequence = 0U;
    std::uint64_t applied_sequence = 0U;

    // Independently release-published live/stream state retained from the
    // previous wire generation.
    std::uint64_t heartbeat_monotonic_ns = 0U;
    std::uint64_t tick_highest_published_sequence = 0U;
    std::uint64_t tick_contiguous_published_sequence = 0U;
    std::uint64_t kline_generation = 0U;
    std::uint64_t published_records = 0U;
    std::uint64_t reserved_scalar = 0U;

    std::uint32_t region_count = 0U;
    std::uint32_t region_descriptor_bytes = 0U;
    std::array<RealtimeWireRegionDescriptorV2,
               kRealtimeWireRegionCountV2>
        regions{};
    std::array<std::uint8_t, 3240U> reserved{};
};
static_assert(sizeof(RealtimeWireHeaderV2) == 4096U);
static_assert(alignof(RealtimeWireHeaderV2) == 4096U);
static_assert(offsetof(RealtimeWireHeaderV2, server_state) == 20U);
static_assert(offsetof(RealtimeWireHeaderV2, total_mapping_bytes) == 24U);
static_assert(offsetof(RealtimeWireHeaderV2, session_epoch) == 48U);
static_assert(offsetof(RealtimeWireHeaderV2, flags) == 60U);
static_assert(offsetof(RealtimeWireHeaderV2, capacity) == 64U);
static_assert(
    offsetof(RealtimeWireHeaderV2, catalog_trade_date) == 80U);
static_assert(offsetof(RealtimeWireHeaderV2, catalog_version) == 88U);
static_assert(offsetof(RealtimeWireHeaderV2, layout_digest) == 96U);
static_assert(
    offsetof(RealtimeWireHeaderV2, status_publish_tag) == 128U);
static_assert(
    offsetof(RealtimeWireHeaderV2, catalog_generation) == 136U);
static_assert(
    offsetof(RealtimeWireHeaderV2, data_state_generation) == 144U);
static_assert(offsetof(RealtimeWireHeaderV2, catalog_digest) == 152U);
static_assert(offsetof(RealtimeWireHeaderV2, bound_count) == 184U);
static_assert(offsetof(RealtimeWireHeaderV2, accepted_sequence) == 208U);
static_assert(
    offsetof(RealtimeWireHeaderV2, applied_sequence) == 216U);
static_assert(
    offsetof(RealtimeWireHeaderV2, heartbeat_monotonic_ns) == 224U);
static_assert(offsetof(RealtimeWireHeaderV2, regions) == 280U);
static_assert(offsetof(RealtimeWireHeaderV2, reserved) == 856U);

// One physical ordinal slot. Instrument identity and key bytes are published
// once and never changed or rebound within a session. Availability flags and
// ingress bounds may advance under the row publish_tag. Before prepublication
// a physical row is all-zero bytes; no such row is valid after ACTIVE.
struct alignas(64) RealtimeWireInstrumentV2 final {
    std::uint64_t publish_tag = 0U;
    std::uint32_t instrument_id = 0U;
    std::uint32_t ordinal = 0U;
    std::uint32_t binding_state = 0U;
    std::uint32_t availability_flags = 0U;
    std::uint8_t market = 0U;
    std::uint8_t quantity_unit = 0U;
    std::uint8_t security_type = 0U;
    std::uint8_t asset_scope = 0U;
    std::uint32_t reserved0 = 0U;
    std::uint64_t security_id_source_offset = 0U;
    std::uint64_t security_id_offset = 0U;
    std::uint32_t security_id_source_length = 0U;
    std::uint32_t security_id_length = 0U;
    std::uint64_t first_ingress_sequence = 0U;
    std::uint64_t last_ingress_sequence = 0U;
    std::array<std::uint64_t, 7U> reserved{};
};
static_assert(sizeof(RealtimeWireInstrumentV2) == 128U);
static_assert(alignof(RealtimeWireInstrumentV2) == 64U);
static_assert(std::is_standard_layout_v<RealtimeWireInstrumentV2>);
static_assert(offsetof(RealtimeWireInstrumentV2, publish_tag) == 0U);
static_assert(offsetof(RealtimeWireInstrumentV2, instrument_id) == 8U);
static_assert(offsetof(RealtimeWireInstrumentV2, ordinal) == 12U);
static_assert(offsetof(RealtimeWireInstrumentV2, binding_state) == 16U);
static_assert(
    offsetof(RealtimeWireInstrumentV2, availability_flags) == 20U);
static_assert(offsetof(RealtimeWireInstrumentV2, market) == 24U);
static_assert(
    offsetof(RealtimeWireInstrumentV2, security_id_source_offset) ==
    32U);
static_assert(
    offsetof(RealtimeWireInstrumentV2, security_id_offset) == 40U);
static_assert(
    offsetof(RealtimeWireInstrumentV2, first_ingress_sequence) == 56U);
static_assert(
    offsetof(RealtimeWireInstrumentV2, last_ingress_sequence) == 64U);
static_assert(offsetof(RealtimeWireInstrumentV2, reserved) == 72U);

struct RealtimeWireKLineWindowV2 final {
    std::uint32_t window_id = 0U;
    std::uint32_t reserved = 0U;
    std::uint64_t duration_ns = 0U;
};
static_assert(sizeof(RealtimeWireKLineWindowV2) == 16U);

struct RealtimeWireDecimalV2 final {
    std::int64_t raw = 0;
    std::int64_t normalized_p6 = 0;
    std::uint8_t scale = 0U;
    std::uint8_t valid = 0U;
    std::uint8_t is_null = 0U;
    std::array<std::uint8_t, 5U> reserved{};
};
static_assert(sizeof(RealtimeWireDecimalV2) == 24U);

struct RealtimeWireQuantityV2 final {
    std::int64_t raw = 0;
    std::uint8_t scale = 0U;
    std::uint8_t valid = 0U;
    std::uint8_t is_null = 0U;
    std::array<std::uint8_t, 5U> reserved{};
};
static_assert(sizeof(RealtimeWireQuantityV2) == 16U);

// The payload layout is intentionally unchanged. The semantic field at
// offset 12 is the frozen daily-catalog ordinal (instrument_id - 1).
struct RealtimeWireCommonRecordV2 final {
    std::uint32_t record_schema_version = 2U;
    std::uint32_t record_bytes = 0U;
    std::uint32_t instrument_id = 0U;
    std::uint32_t ordinal = 0U;
    std::uint64_t source_sequence = 0U;
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t tick_stream_sequence = 0U;
    std::uint64_t vendor_sequence_id = 0U;
    std::int64_t event_time_unix_ns = 0;
    std::int64_t recv_realtime_ns = 0;
    std::int64_t recv_monotonic_ns = 0;
    std::uint64_t exchange_time_ns_since_midnight = 0U;
    std::uint64_t quality_flags = 0U;
    std::uint64_t market_notices = 0U;
    std::uint32_t source_stream_id = 0U;
    std::uint32_t trade_date = 0U;
    std::uint32_t vendor_local_time_raw = 0U;
    std::uint32_t reserved0 = 0U;
    std::uint8_t source_slot = 0U;
    std::uint8_t event_kind = 0U;
    std::uint8_t market = 0U;
    std::uint8_t quantity_unit = 0U;
    std::uint8_t security_type = 0U;
    std::uint8_t asset_scope = 0U;
    std::array<std::uint8_t, 10U> reserved{};
};
static_assert(sizeof(RealtimeWireCommonRecordV2) == 128U);
static_assert(offsetof(RealtimeWireCommonRecordV2, ordinal) == 12U);
static_assert(
    offsetof(RealtimeWireCommonRecordV2, ingress_sequence) == 24U);

struct RealtimeWireBookLevelV2 final {
    RealtimeWireDecimalV2 price{};
    RealtimeWireQuantityV2 quantity{};
    std::uint32_t order_count = 0U;
    std::uint8_t order_count_valid = 0U;
    std::array<std::uint8_t, 3U> reserved{};
};
static_assert(sizeof(RealtimeWireBookLevelV2) == 48U);

struct RealtimeWireQueueHeaderV2 final {
    std::uint32_t total_order_count = 0U;
    std::uint32_t actual_revealed_count = 0U;
    std::uint32_t retained_count = 0U;
    std::uint32_t reserved = 0U;
};
static_assert(sizeof(RealtimeWireQueueHeaderV2) == 16U);

struct RealtimeWireSnapshotPayloadV2 final {
    RealtimeWireCommonRecordV2 common{};
    std::int64_t trade_count = 0;
    std::int32_t image_status = 0;
    std::uint32_t channel = 0U;
    RealtimeWireDecimalV2 pre_close_price{};
    RealtimeWireDecimalV2 open_price{};
    RealtimeWireDecimalV2 high_price{};
    RealtimeWireDecimalV2 low_price{};
    RealtimeWireDecimalV2 last_price{};
    RealtimeWireDecimalV2 close_price{};
    RealtimeWireQuantityV2 trade_volume{};
    RealtimeWireDecimalV2 turnover{};
    RealtimeWireQuantityV2 total_bid_quantity{};
    RealtimeWireDecimalV2 weighted_average_bid_price{};
    RealtimeWireQuantityV2 total_ask_quantity{};
    RealtimeWireDecimalV2 weighted_average_ask_price{};
    RealtimeWireDecimalV2 high_limit_price{};
    RealtimeWireDecimalV2 low_limit_price{};
    RealtimeWireDecimalV2 iopv{};
    RealtimeWireQuantityV2 open_interest{};
    std::uint32_t actual_bid_depth = 0U;
    std::uint32_t actual_ask_depth = 0U;
    std::uint32_t retained_bid_depth = 0U;
    std::uint32_t retained_ask_depth = 0U;
    std::array<RealtimeWireBookLevelV2, 10U> bids{};
    std::array<RealtimeWireBookLevelV2, 10U> asks{};
    RealtimeWireQueueHeaderV2 bid1_queue{};
    RealtimeWireQueueHeaderV2 ask1_queue{};
    std::array<RealtimeWireQuantityV2, 50U> bid1_queue_quantities{};
    std::array<RealtimeWireQuantityV2, 50U> ask1_queue_quantities{};
};
static_assert(sizeof(RealtimeWireSnapshotPayloadV2) == 3104U);
static_assert(
    sizeof(RealtimeWireSnapshotPayloadV2) <=
    kRealtimeSnapshotSlotBytesV2 - 64U);

struct RealtimeWireTickPayloadV2 final {
    RealtimeWireCommonRecordV2 common{};
    std::uint32_t validity_bitmap = 0U;
    std::uint32_t projection_flags = 0U;
    std::int64_t channel = 0;
    std::int64_t native_event_sequence = 0;
    std::int32_t source_raw_code_1 = 0;
    std::int32_t source_raw_code_2 = 0;
    std::uint8_t action = 0U;
    std::uint8_t side = 0U;
    std::uint8_t order_type = 0U;
    std::uint8_t aggressor = 0U;
    std::uint8_t phase = 0U;
    std::uint8_t raw_type_length = 0U;
    std::uint8_t raw_tick_flag_length = 0U;
    std::uint8_t reserved0 = 0U;
    RealtimeWireDecimalV2 price{};
    RealtimeWireQuantityV2 quantity{};
    RealtimeWireDecimalV2 trade_amount{};
    RealtimeWireQuantityV2 matched_quantity{};
    std::int64_t primary_order_id = 0;
    std::int64_t buy_order_id = 0;
    std::int64_t sell_order_id = 0;
    std::array<std::uint8_t, 32U> raw_type{};
    std::array<std::uint8_t, 32U> raw_tick_flag{};
};
static_assert(sizeof(RealtimeWireTickPayloadV2) == 336U);
static_assert(
    offsetof(RealtimeWireTickPayloadV2, projection_flags) == 132U);
static_assert(
    sizeof(RealtimeWireTickPayloadV2) <=
    kRealtimeTickSlotBytesV2 - 64U);

struct RealtimeWireKLinePayloadV2 final {
    std::uint64_t generation = 0U;
    std::uint32_t trade_date = 0U;
    std::uint32_t instrument_id = 0U;
    std::uint32_t window_id = 0U;
    std::uint32_t reserved0 = 0U;
    std::uint64_t window_duration_ns = 0U;
    std::uint64_t window_start_ns_since_midnight = 0U;
    std::uint64_t window_end_ns_since_midnight = 0U;
    std::int64_t window_start_unix_ns = 0;
    std::int64_t window_end_unix_ns = 0;
    std::int64_t open_price_p6 = 0;
    std::int64_t high_price_p6 = 0;
    std::int64_t low_price_p6 = 0;
    std::int64_t close_price_p6 = 0;
    std::uint64_t volume_raw = 0U;
    std::uint64_t trade_count = 0U;
    std::uint64_t revision = 0U;
    std::uint64_t first_event_time_ns_since_midnight = 0U;
    std::uint64_t first_event_sequence = 0U;
    std::uint64_t first_source_sequence = 0U;
    std::uint64_t first_ingress_sequence = 0U;
    std::uint64_t last_event_time_ns_since_midnight = 0U;
    std::uint64_t last_event_sequence = 0U;
    std::uint64_t last_source_sequence = 0U;
    std::uint64_t last_ingress_sequence = 0U;
    std::uint8_t volume_scale = 0U;
    std::uint8_t quantity_unit = 0U;
    std::uint8_t present = 0U;
    std::array<std::uint8_t, 5U> reserved{};
};
static_assert(sizeof(RealtimeWireKLinePayloadV2) == 192U);

struct alignas(64) RealtimeWireSnapshotSlotV2 final {
    std::uint64_t publish_tag = 0U;
    std::array<std::uint8_t, 56U> reserved{};
    std::array<std::uint64_t, 504U> payload_words{};
};
static_assert(sizeof(RealtimeWireSnapshotSlotV2) == 4096U);

struct alignas(64) RealtimeWireTickSlotV2 final {
    std::uint64_t publish_tag = 0U;
    std::array<std::uint8_t, 56U> reserved{};
    std::array<std::uint64_t, 56U> payload_words{};
};
static_assert(sizeof(RealtimeWireTickSlotV2) == 512U);

struct alignas(64) RealtimeWireKLineSlotV2 final {
    std::uint64_t publish_tag = 0U;
    std::array<std::uint8_t, 56U> reserved{};
    std::array<std::uint64_t, 24U> payload_words{};
};
static_assert(sizeof(RealtimeWireKLineSlotV2) == 256U);

struct RealtimeControlRequestV2 final {
    std::array<std::uint8_t, 8U> magic{};
    std::uint16_t protocol_major = 0U;
    std::uint16_t protocol_minor = 0U;
    std::uint16_t opcode = 0U;
    std::uint16_t reserved0 = 0U;
    std::uint32_t message_bytes = 0U;
    std::uint32_t flags = 0U;
    std::uint64_t request_id = 0U;
    std::uint64_t reserved1 = 0U;
};
static_assert(sizeof(RealtimeControlRequestV2) == 40U);

struct RealtimeControlResponseV2 final {
    std::array<std::uint8_t, 8U> magic{};
    std::uint16_t protocol_major = 0U;
    std::uint16_t protocol_minor = 0U;
    std::uint16_t status = 0U;
    std::uint16_t flags = 0U;
    std::uint32_t message_bytes = 0U;
    std::uint32_t reserved0 = 0U;
    std::uint64_t request_id = 0U;
    std::uint64_t session_epoch = 0U;
    std::uint64_t total_mapping_bytes = 0U;
    std::array<std::uint64_t, 2U> reserved{};
};
static_assert(sizeof(RealtimeControlResponseV2) == 64U);

[[nodiscard]] constexpr bool RealtimeStatusPublishTagStableV2(
    std::uint64_t publish_tag) noexcept {
    return (publish_tag & 1U) == 0U;
}

[[nodiscard]] constexpr bool RealtimeWireCountsValidV2(
    std::uint32_t capacity,
    std::uint32_t bound_count,
    std::uint32_t available_count,
    std::uint32_t snapshot_available_count,
    std::uint32_t tick_available_count,
    std::uint32_t factor_eligible_count) noexcept {
    return bound_count <= capacity &&
           available_count <= bound_count &&
           snapshot_available_count <= available_count &&
           tick_available_count <= available_count &&
           factor_eligible_count <= snapshot_available_count;
}

[[nodiscard]] constexpr bool RealtimeWireProcessingSequencesValidV2(
    std::uint64_t accepted_sequence,
    std::uint64_t applied_sequence) noexcept {
    return accepted_sequence !=
               std::numeric_limits<std::uint64_t>::max() &&
           applied_sequence <= accepted_sequence;
}

// On failure output is left untouched.
[[nodiscard]] constexpr bool RealtimeWireProcessingLagRecordsV2(
    std::uint64_t accepted_sequence,
    std::uint64_t applied_sequence,
    std::uint64_t* output) noexcept {
    if (output == nullptr ||
        !RealtimeWireProcessingSequencesValidV2(
            accepted_sequence, applied_sequence)) {
        return false;
    }
    *output = accepted_sequence - applied_sequence;
    return true;
}

[[nodiscard]] constexpr bool RealtimeWireHeaderStatusValidV2(
    const RealtimeWireHeaderV2& header) noexcept {
    return header.catalog_scope ==
               static_cast<std::uint32_t>(
                   RealtimeCatalogScopeV2::kDeclaredDailyAShare) &&
           header.coverage_complete == 1U &&
           header.catalog_trade_date == header.trade_date &&
           header.catalog_trade_date != 0U &&
           header.reserved_catalog == 0U &&
           header.catalog_version != 0U &&
           header.catalog_generation == 1U &&
           header.bound_count == header.capacity &&
           header.reserved_count == 0U &&
           RealtimeWireCountsValidV2(
               header.capacity,
               header.bound_count,
               header.available_count,
               header.snapshot_available_count,
               header.tick_available_count,
               header.factor_eligible_count) &&
           RealtimeWireProcessingSequencesValidV2(
               header.accepted_sequence,
               header.applied_sequence);
}

[[nodiscard]] constexpr bool RealtimeWireAvailabilityFlagsValidV2(
    std::uint32_t flags) noexcept {
    constexpr std::uint32_t known =
        kRealtimeInstrumentHasSnapshotV2 |
        kRealtimeInstrumentHasTickV2 |
        kRealtimeInstrumentHasKLineV2 |
        kRealtimeInstrumentFactorEligibleV2;
    return (flags & ~known) == 0U &&
           ((flags & kRealtimeInstrumentFactorEligibleV2) == 0U ||
            (flags & kRealtimeInstrumentHasSnapshotV2) != 0U);
}

[[nodiscard]] constexpr bool RealtimeWireInstrumentIdentityValidV2(
    const RealtimeWireInstrumentV2& row,
    std::uint32_t capacity) noexcept {
    return row.instrument_id != 0U &&
           row.ordinal < capacity &&
           row.ordinal != std::numeric_limits<std::uint32_t>::max() &&
           row.instrument_id == row.ordinal + 1U &&
           row.security_id_length != 0U;
}

[[nodiscard]] constexpr bool RealtimeWireInstrumentStateValidV2(
    const RealtimeWireInstrumentV2& row,
    std::uint32_t capacity) noexcept {
    if (!RealtimeWireAvailabilityFlagsValidV2(
            row.availability_flags)) {
        return false;
    }
    const auto state =
        static_cast<RealtimeInstrumentBindingStateV2>(
            row.binding_state);
    if (state == RealtimeInstrumentBindingStateV2::kUnbound) {
        return row.publish_tag == 0U &&
               row.instrument_id == 0U && row.ordinal == 0U &&
               row.availability_flags == 0U && row.market == 0U &&
               row.quantity_unit == 0U &&
               row.security_type == 0U && row.asset_scope == 0U &&
               row.reserved0 == 0U &&
               row.security_id_source_offset == 0U &&
               row.security_id_offset == 0U &&
               row.security_id_source_length == 0U &&
               row.security_id_length == 0U &&
               row.first_ingress_sequence == 0U &&
               row.last_ingress_sequence == 0U &&
               std::all_of(
                   row.reserved.begin(),
                   row.reserved.end(),
                   [](std::uint64_t value) constexpr noexcept {
                       return value == 0U;
                   });
    }
    if (state == RealtimeInstrumentBindingStateV2::kBinding) {
        // Readers deliberately do not interpret an in-progress binding.
        return true;
    }
    if (!RealtimeWireInstrumentIdentityValidV2(row, capacity) ||
        row.market == 0U || row.reserved0 != 0U ||
        !std::all_of(
            row.reserved.begin(),
            row.reserved.end(),
            [](std::uint64_t value) constexpr noexcept {
                return value == 0U;
            })) {
        return false;
    }
    if (state == RealtimeInstrumentBindingStateV2::kBoundNoData) {
        return row.availability_flags == 0U &&
               row.first_ingress_sequence == 0U &&
               row.last_ingress_sequence == 0U;
    }
    if (state != RealtimeInstrumentBindingStateV2::kAvailable) {
        return false;
    }
    constexpr std::uint32_t data_flags =
        kRealtimeInstrumentHasSnapshotV2 |
        kRealtimeInstrumentHasTickV2 |
        kRealtimeInstrumentHasKLineV2;
    return (row.availability_flags & data_flags) != 0U &&
           row.first_ingress_sequence != 0U &&
           row.last_ingress_sequence >=
               row.first_ingress_sequence;
}

static_assert(
    std::atomic_ref<std::uint32_t>::is_always_lock_free,
    "the realtime V2 wire ABI requires lock-free 32-bit atomic_ref");
static_assert(
    std::atomic_ref<std::uint64_t>::is_always_lock_free,
    "the realtime V2 wire ABI requires lock-free 64-bit atomic_ref");

}  // namespace l2flow::ipc
