#pragma once

#include "l2flow/ipc/order_event_live_aggregation_engine_v1.h"
#include "l2flow/ipc/realtime_wire_v2.h"

#include <cstdint>
#include <string>

namespace l2flow::apps {

// A value snapshot keeps failure formatting independent from the failed
// engine lifetime. The last_* names are intentional: only the outer consume
// error identifies the failing stage; diagnostics from other stages may
// describe the most recent earlier call.
struct OrderEventFailureDiagnosticV1 final {
    ipc::OrderEventLiveConsumeErrorV1 consume_error =
        ipc::OrderEventLiveConsumeErrorV1::kNone;
    ipc::WireOrderEventProjectionResultV2 last_wire_projection =
        ipc::WireOrderEventProjectionResultV2::kProjected;
    market::ShanghaiOrderAggregatorConsumeErrorV1
        last_shanghai_core =
            market::ShanghaiOrderAggregatorConsumeErrorV1::kNone;
    market::ShenzhenOrderProjectorConsumeErrorV1
        last_shenzhen_core =
            market::ShenzhenOrderProjectorConsumeErrorV1::kNone;
    ipc::OrderEventDeltaPublishErrorV1 last_publish =
        ipc::OrderEventDeltaPublishErrorV1::kNone;
    std::uint64_t consume_result_source_tick = 0U;
    std::uint64_t engine_last_consumed_source_tick = 0U;
    bool engine_failed = false;
};

[[nodiscard]] inline OrderEventFailureDiagnosticV1
CaptureOrderEventFailureDiagnosticV1(
    ipc::OrderEventLiveConsumeErrorV1 consume_error,
    const ipc::OrderEventLiveConsumeResultV1& consume_result,
    const ipc::OrderEventLiveAggregationEngineV1& engine) noexcept {
    return {
        .consume_error = consume_error,
        .last_wire_projection =
            engine.last_wire_projection_result(),
        .last_shanghai_core = engine.last_shanghai_error(),
        .last_shenzhen_core = engine.last_shenzhen_error(),
        .last_publish = engine.last_publish_error(),
        .consume_result_source_tick =
            consume_result.source_tick_sequence,
        .engine_last_consumed_source_tick =
            engine.consumed_source_tick_sequence(),
        .engine_failed = engine.failed(),
    };
}

[[nodiscard]] inline std::string FormatOrderEventFailureDiagnosticV1(
    const OrderEventFailureDiagnosticV1& diagnostic,
    const ipc::RealtimeWireTickPayloadV2& record,
    std::uint64_t expected_source_tick) {
    const auto& common = record.common;
    std::string result =
        "live_aggregation_failed expected_source_tick=" +
        std::to_string(expected_source_tick);
    result += " record_tick_stream_sequence=" +
              std::to_string(common.tick_stream_sequence);
    result += " record_source_sequence=" +
              std::to_string(common.source_sequence);
    result += " record_ingress_sequence=" +
              std::to_string(common.ingress_sequence);
    result += " record_source_stream_id=" +
              std::to_string(common.source_stream_id);
    result += " record_market=" +
              std::to_string(static_cast<unsigned int>(common.market));
    result += " record_event_kind=" +
              std::to_string(
                  static_cast<unsigned int>(common.event_kind));
    result += " record_source_slot=" +
              std::to_string(
                  static_cast<unsigned int>(common.source_slot));
    result += " record_trade_date=" +
              std::to_string(common.trade_date);
    result += " record_instrument_id=" +
              std::to_string(common.instrument_id);
    result += " record_channel=" + std::to_string(record.channel);
    result += " record_native_event_sequence=" +
              std::to_string(record.native_event_sequence);
    result += " record_source_raw_code_1=" +
              std::to_string(record.source_raw_code_1);
    result += " record_source_raw_code_2=" +
              std::to_string(record.source_raw_code_2);
    result += " consume_error=" +
              std::string(ipc::OrderEventLiveConsumeErrorNameV1(
                  diagnostic.consume_error));
    result += " consume_result_source_tick=" +
              std::to_string(diagnostic.consume_result_source_tick);
    result += " engine_last_consumed_source_tick=" +
              std::to_string(
                  diagnostic.engine_last_consumed_source_tick);
    result += diagnostic.engine_failed
                  ? " engine_failed=true"
                  : " engine_failed=false";
    result += " last_wire_projection=" +
              std::string(ipc::WireOrderEventProjectionResultNameV2(
                  diagnostic.last_wire_projection));
    result += " last_shanghai_core=" +
              std::string(
                  market::ShanghaiOrderAggregatorConsumeErrorNameV1(
                      diagnostic.last_shanghai_core));
    result += " last_shenzhen_core=" +
              std::string(
                  market::ShenzhenOrderProjectorConsumeErrorNameV1(
                      diagnostic.last_shenzhen_core));
    result += " last_publish=" +
              std::string(ipc::OrderEventDeltaPublishErrorNameV1(
                  diagnostic.last_publish));
    return result;
}

}  // namespace l2flow::apps
