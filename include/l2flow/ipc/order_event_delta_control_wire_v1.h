#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace l2flow::ipc {

inline constexpr std::array<std::uint8_t, 8U>
    kOrderEventDeltaControlMagicV1{
        'L', '2', 'F', 'E', 'C', 'T', '1', '\0'};
inline constexpr std::uint16_t
    kOrderEventDeltaControlProtocolMajorV1 = 1U;
inline constexpr std::uint16_t
    kOrderEventDeltaControlProtocolMinorV1 = 0U;

enum class OrderEventDeltaControlOpcodeV1 : std::uint16_t {
    kGetSession = 1U,
};

enum class OrderEventDeltaControlStatusV1 : std::uint16_t {
    kOk = 0U,
    kInvalidRequest = 1U,
    kUnsupportedVersion = 2U,
    kUnavailable = 3U,
    kSourceSessionMismatch = 4U,
    kInternalError = 5U,
};

// V1 is a native little-endian, fixed-width Linux ABI. Requests carry the
// mandatory source-session expectation. This prevents a client which has
// retained a stale socket path from accidentally attaching to another source
// run, epoch, or trading day.
struct OrderEventDeltaControlGetSessionRequestV1 final {
    std::array<std::uint8_t, 8U> magic{};
    std::uint16_t protocol_major = 0U;
    std::uint16_t protocol_minor = 0U;
    std::uint16_t opcode = 0U;
    std::uint16_t flags = 0U;
    std::uint32_t message_bytes = 0U;
    std::uint32_t reserved0 = 0U;
    std::uint64_t request_id = 0U;
    std::array<std::uint8_t, 16U> expected_source_run_id{};
    std::uint64_t expected_source_session_epoch = 0U;
    std::uint32_t expected_source_trade_date = 0U;
    std::uint32_t reserved1 = 0U;
    std::array<std::uint64_t, 2U> reserved{};
};
static_assert(
    sizeof(OrderEventDeltaControlGetSessionRequestV1) == 80U);
static_assert(
    alignof(OrderEventDeltaControlGetSessionRequestV1) == 8U);
static_assert(std::is_standard_layout_v<
              OrderEventDeltaControlGetSessionRequestV1>);
static_assert(std::is_trivially_copyable_v<
              OrderEventDeltaControlGetSessionRequestV1>);
static_assert(offsetof(
                  OrderEventDeltaControlGetSessionRequestV1,
                  request_id) == 24U);
static_assert(offsetof(
                  OrderEventDeltaControlGetSessionRequestV1,
                  expected_source_run_id) == 32U);
static_assert(offsetof(
                  OrderEventDeltaControlGetSessionRequestV1,
                  expected_source_session_epoch) == 48U);
static_assert(offsetof(
                  OrderEventDeltaControlGetSessionRequestV1,
                  reserved) == 64U);

// A successful response is accompanied by exactly one SCM_RIGHTS descriptor.
// The descriptor is O_RDONLY and names the event ring described below.
//
// Mutable values are a point-in-time control snapshot. source cursor is read
// before event cursor, matching the ring's release/acquire publication
// contract. A client must still read the live header after receiving the fd.
struct OrderEventDeltaControlGetSessionResponseV1 final {
    std::array<std::uint8_t, 8U> magic{};
    std::uint16_t protocol_major = 0U;
    std::uint16_t protocol_minor = 0U;
    std::uint16_t status = 0U;
    std::uint16_t flags = 0U;
    std::uint32_t message_bytes = 0U;
    std::uint32_t reserved0 = 0U;
    std::uint64_t request_id = 0U;

    std::array<std::uint8_t, 16U> source_run_id{};
    std::uint64_t source_session_epoch = 0U;
    std::uint32_t source_trade_date = 0U;
    std::uint32_t event_producer_state = 0U;

    std::array<std::uint8_t, 16U> event_run_id{};
    std::uint64_t event_session_epoch = 0U;
    std::uint32_t event_trade_date = 0U;
    std::uint32_t event_header_flags = 0U;
    std::uint64_t event_ring_capacity = 0U;
    std::uint64_t event_total_mapping_bytes = 0U;
    std::uint64_t event_published_sequence = 0U;
    std::uint64_t source_tick_consumed_sequence = 0U;
    std::uint64_t heartbeat_monotonic_ns = 0U;
    std::uint64_t producer_started_monotonic_ns = 0U;
    std::array<std::uint64_t, 6U> reserved{};
};
static_assert(
    sizeof(OrderEventDeltaControlGetSessionResponseV1) == 192U);
static_assert(
    alignof(OrderEventDeltaControlGetSessionResponseV1) == 8U);
static_assert(std::is_standard_layout_v<
              OrderEventDeltaControlGetSessionResponseV1>);
static_assert(std::is_trivially_copyable_v<
              OrderEventDeltaControlGetSessionResponseV1>);
static_assert(offsetof(
                  OrderEventDeltaControlGetSessionResponseV1,
                  request_id) == 24U);
static_assert(offsetof(
                  OrderEventDeltaControlGetSessionResponseV1,
                  source_run_id) == 32U);
static_assert(offsetof(
                  OrderEventDeltaControlGetSessionResponseV1,
                  event_producer_state) == 60U);
static_assert(offsetof(
                  OrderEventDeltaControlGetSessionResponseV1,
                  event_run_id) == 64U);
static_assert(offsetof(
                  OrderEventDeltaControlGetSessionResponseV1,
                  event_ring_capacity) == 96U);
static_assert(offsetof(
                  OrderEventDeltaControlGetSessionResponseV1,
                  event_published_sequence) == 112U);
static_assert(offsetof(
                  OrderEventDeltaControlGetSessionResponseV1,
                  reserved) == 144U);

}  // namespace l2flow::ipc
