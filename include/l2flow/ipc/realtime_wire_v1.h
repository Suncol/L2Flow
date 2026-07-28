#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace l2flow::ipc {

inline constexpr std::array<std::uint8_t, 8U> kRealtimeShmMagicV1{
    'L', '2', 'F', 'S', 'H', 'M', '1', '\0'};
inline constexpr std::array<std::uint8_t, 8U> kRealtimeControlMagicV1{
    'L', '2', 'F', 'C', 'T', 'L', '1', '\0'};
inline constexpr std::uint16_t kRealtimeWireMajorV1 = 1U;
// Minor 1 assigns the formerly-reserved tick word at offset 132 to explicit
// projection flags. A 1.0 reader must reject 1.1 during handshake rather than
// silently interpret an omitted raw field as a genuine empty value.
inline constexpr std::uint16_t kRealtimeWireMinorV1 = 1U;
inline constexpr std::uint32_t kRealtimeLittleEndianMarkerV1 =
    0x01020304U;
inline constexpr std::size_t kRealtimeWireRegionCountV1 = 9U;
inline constexpr std::size_t kRealtimeSnapshotSlotBytesV1 = 4096U;
inline constexpr std::size_t kRealtimeTickSlotBytesV1 = 512U;
inline constexpr std::size_t kRealtimeKLineSlotBytesV1 = 256U;
// KLine publication fills the inactive table and then release-publishes the
// completed generation. Readers select one table by generation parity, so a
// normal concurrent refresh never exposes a partially updated generation.
inline constexpr std::size_t kRealtimeKLineTableCountV1 = 2U;

enum class RealtimeServerStateV1 : std::uint32_t {
    kInitializing = 1U,
    kActive = 2U,
    kDraining = 3U,
    kStoppedClean = 4U,
    kFailed = 5U,
};

enum RealtimeHeaderFlagV1 : std::uint32_t {
    kRealtimeHeaderCoverageLostV1 = 1U << 0U,
    kRealtimeHeaderKLineEnabledV1 = 1U << 1U,
};

// The fixed tick payload keeps only 32 inline bytes for each Shanghai raw
// text field. Oversized retained Store strings are represented by an empty
// inline value plus one of these explicit flags; the tick record itself is
// still published to latest, ring, and history.
enum RealtimeWireTickProjectionFlagV1 : std::uint32_t {
    kRealtimeWireTickRawTypeOmittedV1 = 1U << 0U,
    kRealtimeWireTickRawTickFlagOmittedV1 = 1U << 1U,
};

enum class RealtimeRegionKindV1 : std::uint32_t {
    kInstrumentRows = 1U,
    kInstrumentKeyBlob = 2U,
    kKLineWindows = 3U,
    kLatestSnapshots = 4U,
    kLatestTicks = 5U,
    kLatestKLines = 6U,
    kTickRingSlots = 7U,
    kReserved8 = 8U,
    kReserved9 = 9U,
};

enum class RealtimeControlOpcodeV1 : std::uint16_t {
    kGetSession = 1U,
};

enum class RealtimeControlStatusV1 : std::uint16_t {
    kOk = 0U,
    kInvalidRequest = 1U,
    kUnsupportedVersion = 2U,
    kUnavailable = 3U,
};

// Every protocol structure is fixed-width and little-endian. The native
// reader validates the endian marker before interpreting any field. No C++
// enum, bool, pointer, string, variant, span, size_t, or atomic object is
// embedded in shared memory.
struct RealtimeWireRegionDescriptorV1 final {
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
static_assert(sizeof(RealtimeWireRegionDescriptorV1) == 64U);
static_assert(std::is_standard_layout_v<RealtimeWireRegionDescriptorV1>);

struct alignas(4096) RealtimeWireHeaderV1 final {
    std::array<std::uint8_t, 8U> magic{};
    std::uint16_t abi_major = 0U;
    std::uint16_t abi_minor = 0U;
    std::uint32_t header_bytes = 0U;
    std::uint32_t endian_marker = 0U;
    // Accessed through atomic_ref by the publisher and native reader.
    std::uint32_t server_state = 0U;
    std::uint64_t total_mapping_bytes = 0U;
    std::array<std::uint8_t, 16U> run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
    // Accessed through atomic_ref.
    std::uint32_t flags = 0U;
    std::uint64_t registry_version = 0U;
    std::array<std::uint8_t, 32U> registry_sha256{};
    std::uint32_t instrument_count = 0U;
    std::uint32_t window_count = 0U;
    // The following six words are independently published with release
    // stores and acquired by the native reader.
    std::uint64_t heartbeat_monotonic_ns = 0U;
    // Greatest admission-assigned tick sequence observed at the applied
    // publisher. It may lead the contiguous prefix during worker reordering.
    std::uint64_t tick_highest_published_sequence = 0U;
    std::uint64_t tick_contiguous_published_sequence = 0U;
    std::uint64_t kline_generation = 0U;
    std::uint64_t published_records = 0U;
    std::uint64_t reserved_scalar = 0U;
    std::uint32_t region_count = 0U;
    std::uint32_t region_descriptor_bytes = 0U;
    std::array<std::uint8_t, 32U> reserved_schema_identity{};
    std::array<RealtimeWireRegionDescriptorV1,
               kRealtimeWireRegionCountV1>
        regions{};
    std::array<std::uint8_t, 3320U> reserved{};
};
static_assert(sizeof(RealtimeWireHeaderV1) == 4096U);
static_assert(alignof(RealtimeWireHeaderV1) == 4096U);
static_assert(offsetof(
                  RealtimeWireHeaderV1,
                  tick_highest_published_sequence) %
                  alignof(std::uint64_t) ==
              0U);

struct RealtimeWireInstrumentV1 final {
    std::uint32_t instrument_id = 0U;
    std::uint8_t market = 0U;
    std::uint8_t quantity_unit = 0U;
    std::uint8_t security_type = 0U;
    std::uint8_t asset_scope = 0U;
    std::uint64_t security_id_source_offset = 0U;
    std::uint32_t security_id_source_length = 0U;
    std::uint32_t reserved0 = 0U;
    std::uint64_t security_id_offset = 0U;
    std::uint32_t security_id_length = 0U;
    std::uint32_t reserved1 = 0U;
    std::array<std::uint64_t, 3U> reserved{};
};
static_assert(sizeof(RealtimeWireInstrumentV1) == 64U);

struct RealtimeWireKLineWindowV1 final {
    std::uint32_t window_id = 0U;
    std::uint32_t reserved = 0U;
    std::uint64_t duration_ns = 0U;
};
static_assert(sizeof(RealtimeWireKLineWindowV1) == 16U);

struct RealtimeWireDecimalV1 final {
    std::int64_t raw = 0;
    std::int64_t normalized_p6 = 0;
    std::uint8_t scale = 0U;
    std::uint8_t valid = 0U;
    std::uint8_t is_null = 0U;
    std::array<std::uint8_t, 5U> reserved{};
};
static_assert(sizeof(RealtimeWireDecimalV1) == 24U);

struct RealtimeWireQuantityV1 final {
    std::int64_t raw = 0;
    std::uint8_t scale = 0U;
    std::uint8_t valid = 0U;
    std::uint8_t is_null = 0U;
    std::array<std::uint8_t, 5U> reserved{};
};
static_assert(sizeof(RealtimeWireQuantityV1) == 16U);

struct RealtimeWireCommonRecordV1 final {
    std::uint32_t record_schema_version = 1U;
    std::uint32_t record_bytes = 0U;
    std::uint32_t instrument_id = 0U;
    std::uint32_t registry_ordinal = 0U;
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
static_assert(sizeof(RealtimeWireCommonRecordV1) == 128U);

struct RealtimeWireBookLevelV1 final {
    RealtimeWireDecimalV1 price{};
    RealtimeWireQuantityV1 quantity{};
    std::uint32_t order_count = 0U;
    std::uint8_t order_count_valid = 0U;
    std::array<std::uint8_t, 3U> reserved{};
};
static_assert(sizeof(RealtimeWireBookLevelV1) == 48U);

struct RealtimeWireQueueHeaderV1 final {
    std::uint32_t total_order_count = 0U;
    std::uint32_t actual_revealed_count = 0U;
    std::uint32_t retained_count = 0U;
    std::uint32_t reserved = 0U;
};
static_assert(sizeof(RealtimeWireQueueHeaderV1) == 16U);

struct RealtimeWireSnapshotPayloadV1 final {
    RealtimeWireCommonRecordV1 common{};
    std::int64_t trade_count = 0;
    std::int32_t image_status = 0;
    std::uint32_t channel = 0U;
    RealtimeWireDecimalV1 pre_close_price{};
    RealtimeWireDecimalV1 open_price{};
    RealtimeWireDecimalV1 high_price{};
    RealtimeWireDecimalV1 low_price{};
    RealtimeWireDecimalV1 last_price{};
    RealtimeWireDecimalV1 close_price{};
    RealtimeWireQuantityV1 trade_volume{};
    RealtimeWireDecimalV1 turnover{};
    RealtimeWireQuantityV1 total_bid_quantity{};
    RealtimeWireDecimalV1 weighted_average_bid_price{};
    RealtimeWireQuantityV1 total_ask_quantity{};
    RealtimeWireDecimalV1 weighted_average_ask_price{};
    RealtimeWireDecimalV1 high_limit_price{};
    RealtimeWireDecimalV1 low_limit_price{};
    RealtimeWireDecimalV1 iopv{};
    RealtimeWireQuantityV1 open_interest{};
    std::uint32_t actual_bid_depth = 0U;
    std::uint32_t actual_ask_depth = 0U;
    std::uint32_t retained_bid_depth = 0U;
    std::uint32_t retained_ask_depth = 0U;
    std::array<RealtimeWireBookLevelV1, 10U> bids{};
    std::array<RealtimeWireBookLevelV1, 10U> asks{};
    RealtimeWireQueueHeaderV1 bid1_queue{};
    RealtimeWireQueueHeaderV1 ask1_queue{};
    std::array<RealtimeWireQuantityV1, 50U> bid1_queue_quantities{};
    std::array<RealtimeWireQuantityV1, 50U> ask1_queue_quantities{};
};
static_assert(sizeof(RealtimeWireSnapshotPayloadV1) == 3104U);
static_assert(
    sizeof(RealtimeWireSnapshotPayloadV1) <=
    kRealtimeSnapshotSlotBytesV1 - 64U);

struct RealtimeWireTickPayloadV1 final {
    RealtimeWireCommonRecordV1 common{};
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
    RealtimeWireDecimalV1 price{};
    RealtimeWireQuantityV1 quantity{};
    RealtimeWireDecimalV1 trade_amount{};
    RealtimeWireQuantityV1 matched_quantity{};
    std::int64_t primary_order_id = 0;
    std::int64_t buy_order_id = 0;
    std::int64_t sell_order_id = 0;
    std::array<std::uint8_t, 32U> raw_type{};
    std::array<std::uint8_t, 32U> raw_tick_flag{};
};
static_assert(sizeof(RealtimeWireTickPayloadV1) == 336U);
static_assert(
    offsetof(RealtimeWireTickPayloadV1, projection_flags) == 132U);
static_assert(
    sizeof(RealtimeWireTickPayloadV1) <=
    kRealtimeTickSlotBytesV1 - 64U);

struct RealtimeWireKLinePayloadV1 final {
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
static_assert(sizeof(RealtimeWireKLinePayloadV1) == 192U);

struct alignas(64) RealtimeWireSnapshotSlotV1 final {
    std::uint64_t publish_tag = 0U;
    std::array<std::uint8_t, 56U> reserved{};
    std::array<std::uint64_t, 504U> payload_words{};
};
static_assert(sizeof(RealtimeWireSnapshotSlotV1) == 4096U);

struct alignas(64) RealtimeWireTickSlotV1 final {
    std::uint64_t publish_tag = 0U;
    std::array<std::uint8_t, 56U> reserved{};
    std::array<std::uint64_t, 56U> payload_words{};
};
static_assert(sizeof(RealtimeWireTickSlotV1) == 512U);

struct alignas(64) RealtimeWireKLineSlotV1 final {
    std::uint64_t publish_tag = 0U;
    std::array<std::uint8_t, 56U> reserved{};
    std::array<std::uint64_t, 24U> payload_words{};
};
static_assert(sizeof(RealtimeWireKLineSlotV1) == 256U);

struct RealtimeControlRequestV1 final {
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
static_assert(sizeof(RealtimeControlRequestV1) == 40U);

struct RealtimeControlResponseV1 final {
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
static_assert(sizeof(RealtimeControlResponseV1) == 64U);

static_assert(
    std::atomic_ref<std::uint64_t>::is_always_lock_free,
    "the realtime wire ABI requires lock-free 64-bit atomic_ref");

}  // namespace l2flow::ipc
