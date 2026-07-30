#pragma once

#include "l2flow/ipc/order_event_delta_ring_v1.h"
#include "l2flow/ipc/order_event_wire_adapter_v2.h"
#include "l2flow/ipc/realtime_wire_v2.h"
#include "l2flow/market/shanghai_order_event_aggregator_v1.h"
#include "l2flow/market/shenzhen_order_event_projector_v1.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

namespace l2flow::ipc {

struct OrderEventLiveAggregationConfigV1 final {
    std::size_t maximum_shanghai_order_states = 0U;
    std::size_t maximum_shenzhen_order_states = 0U;
};

enum class OrderEventLiveSourceClassV1 : std::uint8_t {
    kNonTarget = 0U,
    kShanghai,
    kShenzhen,
};

struct OrderEventLiveConsumeResultV1 final {
    std::uint64_t source_tick_sequence = 0U;
    std::uint64_t published_event_sequence = 0U;
    std::size_t derived_event_count = 0U;
    OrderEventLiveSourceClassV1 source_class =
        OrderEventLiveSourceClassV1::kNonTarget;
};

enum class OrderEventLiveCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kProducerNotPristine,
    kCoreCreateFailed,
    kResourceExhausted,
};

enum class OrderEventLiveConsumeErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kSourceTickGap,
    kInvalidSourceEnvelope,
    kWireProjectionFailed,
    kCoreAggregationFailed,
    kDerivedProjectionFailed,
    kDeltaPublicationFailed,
    kResourceExhausted,
    kFailed,
};

[[nodiscard]] std::string_view
OrderEventLiveCreateErrorNameV1(
    OrderEventLiveCreateErrorV1 error) noexcept;
[[nodiscard]] std::string_view
OrderEventLiveConsumeErrorNameV1(
    OrderEventLiveConsumeErrorV1 error) noexcept;

// Serial, in-process bridge from the existing dense mixed global tick ring to
// the revisioned order-event delta ring. It owns one Shanghai core and one
// Shenzhen core, but not the output producer. The producer must outlive this
// object.
//
// Every global source tick must be submitted exactly once and in dense
// tick_stream_sequence order, including records which are not one of the
// supported Shanghai/Shenzhen target kinds. A valid non-target record is
// committed as an empty source-tick batch. Target records are strictly
// projected, reduced, flattened, then committed with one PublishSourceTick
// call. The producer advances its source cursor only after every event caused
// by that tick is release-published.
//
// The delta-ring capacity is therefore a deployment invariant: it must be at
// least the maximum number of derived rows any single source tick can emit.
// In particular, a Shanghai end-status tick can emit its status row plus one
// final order revision for every affected instrument order. An oversized
// batch is fatal and is never split across source-tick commits.
//
// A source gap, malformed target, core error, derived-row error, allocation
// failure, or publication error permanently fails both this engine and the
// producer. V1 intentionally has no skip, reset, catch-up, WAL, or recovery
// transition.
class OrderEventLiveAggregationEngineV1 final {
public:
    OrderEventLiveAggregationEngineV1(
        const OrderEventLiveAggregationEngineV1&) = delete;
    OrderEventLiveAggregationEngineV1& operator=(
        const OrderEventLiveAggregationEngineV1&) = delete;
    OrderEventLiveAggregationEngineV1(
        OrderEventLiveAggregationEngineV1&&) = delete;
    OrderEventLiveAggregationEngineV1& operator=(
        OrderEventLiveAggregationEngineV1&&) = delete;
    ~OrderEventLiveAggregationEngineV1();

    [[nodiscard]] static OrderEventLiveCreateErrorV1 Create(
        OrderEventLiveAggregationConfigV1 config,
        OrderEventDeltaRingProducerV1* producer,
        std::unique_ptr<OrderEventLiveAggregationEngineV1>* output)
        noexcept;

    [[nodiscard]] OrderEventLiveConsumeErrorV1 Consume(
        const RealtimeWireTickPayloadV2& source_tick,
        OrderEventLiveConsumeResultV1* output) noexcept;

    [[nodiscard]] bool failed() const noexcept;
    [[nodiscard]] std::uint64_t consumed_source_tick_sequence()
        const noexcept;
    [[nodiscard]] std::size_t shanghai_order_count() const noexcept;
    [[nodiscard]] std::size_t shenzhen_order_count() const noexcept;

    // Diagnostic values describe the most recent failing stage. They are not
    // retry authorization; every non-kNone fatal Consume result fail-closes.
    [[nodiscard]] WireOrderEventProjectionResultV2
    last_wire_projection_result() const noexcept;
    [[nodiscard]] market::ShanghaiOrderAggregatorConsumeErrorV1
    last_shanghai_error() const noexcept;
    [[nodiscard]] market::ShenzhenOrderProjectorConsumeErrorV1
    last_shenzhen_error() const noexcept;
    [[nodiscard]] OrderEventDeltaPublishErrorV1
    last_publish_error() const noexcept;

private:
    class Impl;
    explicit OrderEventLiveAggregationEngineV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::ipc
