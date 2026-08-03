#include "../apps/mdl_order_event_aggregator_cli_v1.h"
#include "../apps/order_event_failure_diagnostic_v1.h"

#include <array>
#include <cstdint>
#include <iostream>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace {

namespace app = l2flow::apps;
namespace ipc = l2flow::ipc;

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

constexpr std::array<std::string_view, 22U> kValidArguments{
    "--source-socket",
    "/run/user/1000/l2flow-source.sock",
    "--event-socket",
    "/run/user/1000/l2flow-events.sock",
    "--session-epoch",
    "42",
    "--trade-date",
    "20260730",
    "--shanghai-state-capacity",
    "2000000",
    "--shenzhen-state-capacity",
    "3000000",
    "--event-ring-capacity",
    "262144",
    "--event-maximum-mapping-bytes",
    "268435456",
    "--read-batch-records",
    "4096",
    "--poll-ms",
    "2",
    "--timeout-ms",
    "1500",
};

void TestValidParse(bool* ok) {
    app::OrderEventAggregatorOptionsV1 options{};
    std::string error;
    *ok &= Expect(
        app::ParseOrderEventAggregatorArgumentsV1(
            kValidArguments, &options, &error) ==
                app::OrderEventAggregatorParseResultV1::kOk &&
            error.empty() &&
            options.source_control_socket ==
                "/run/user/1000/l2flow-source.sock" &&
            options.event_control_socket ==
                "/run/user/1000/l2flow-events.sock" &&
            options.session_epoch == 42U &&
            options.trade_date == 20260730U &&
            options.maximum_shanghai_order_states == 2'000'000U &&
            options.maximum_shenzhen_order_states == 3'000'000U &&
            options.event_ring_capacity == 262'144U &&
            options.event_maximum_mapping_bytes == 268'435'456U &&
            options.read_batch_records == 4'096U &&
            options.poll_interval_ms == 2U &&
            options.control_timeout_ms == 1'500U &&
            options.temporal_coverage ==
                ipc::OrderEventDeltaTemporalCoverageV1::
                    kFromMarketOpen,
        "valid CLI projects every required option exactly");
}

void TestProcessStartAndZeroPoll(bool* ok) {
    std::vector<std::string_view> arguments(
        kValidArguments.begin(), kValidArguments.end());
    arguments[19U] = "0";
    arguments.push_back("--temporal-coverage");
    arguments.push_back("process-start");
    arguments.push_back("--cpu-set");
    arguments.push_back("8-9,12");
    arguments.push_back("--parent-pid");
    arguments.push_back("1234");
    app::OrderEventAggregatorOptionsV1 options{};
    std::string error;
    *ok &= Expect(
        app::ParseOrderEventAggregatorArgumentsV1(
            arguments, &options, &error) ==
                app::OrderEventAggregatorParseResultV1::kOk &&
            error.empty() && options.poll_interval_ms == 0U &&
            options.temporal_coverage ==
                ipc::OrderEventDeltaTemporalCoverageV1::
                    kFromProcessStart &&
            options.cpu_set == "8-9,12" &&
            options.managed_parent_pid == 1234U,
        "process-start permits yield polling and strict Event affinity");
}

void TestHelpIsPure(bool* ok) {
    constexpr std::array<std::string_view, 1U> arguments{
        "--help"};
    app::OrderEventAggregatorOptionsV1 options{};
    options.session_epoch = 99U;
    std::string error{"old"};
    *ok &= Expect(
        app::ParseOrderEventAggregatorArgumentsV1(
            arguments, &options, &error) ==
                app::OrderEventAggregatorParseResultV1::kHelp &&
            options.session_epoch == 0U && error.empty() &&
            app::OrderEventAggregatorHelpV1().find(
                "--source-socket") != std::string_view::npos &&
            app::OrderEventAggregatorHelpV1().find(
                "skip, overrun catch-up, WAL, recovery, Parquet") !=
                std::string_view::npos,
        "--help has no filesystem or process side effect");
}

void TestInvalidInputs(bool* ok) {
    {
        auto arguments = kValidArguments;
        arguments[1U] = "relative.sock";
        app::OrderEventAggregatorOptionsV1 options{};
        std::string error;
        *ok &= Expect(
            app::ParseOrderEventAggregatorArgumentsV1(
                arguments, &options, &error) ==
                    app::OrderEventAggregatorParseResultV1::kError &&
                !error.empty(),
            "relative source socket is rejected");
    }
    {
        auto arguments = kValidArguments;
        arguments[7U] = "20260229";
        app::OrderEventAggregatorOptionsV1 options{};
        std::string error;
        *ok &= Expect(
            app::ParseOrderEventAggregatorArgumentsV1(
                arguments, &options, &error) ==
                    app::OrderEventAggregatorParseResultV1::kError &&
                !error.empty(),
            "invalid Gregorian trade date is rejected");
    }
    {
        auto arguments = kValidArguments;
        arguments[15U] = "4096";
        app::OrderEventAggregatorOptionsV1 options{};
        std::string error;
        *ok &= Expect(
            app::ParseOrderEventAggregatorArgumentsV1(
                arguments, &options, &error) ==
                    app::OrderEventAggregatorParseResultV1::kError &&
                error.find("smaller") != std::string::npos,
            "mapping ceiling smaller than ring layout is rejected");
    }
    {
        auto arguments = kValidArguments;
        arguments[13U] = "1";
        arguments[15U] = "4480";
        app::OrderEventAggregatorOptionsV1 options{};
        std::string error;
        *ok &= Expect(
            app::ParseOrderEventAggregatorArgumentsV1(
                arguments, &options, &error) ==
                    app::OrderEventAggregatorParseResultV1::kError,
            "mapping ceiling includes required page alignment");
    }
    {
        auto arguments = kValidArguments;
        arguments[19U] = "1001";
        app::OrderEventAggregatorOptionsV1 options{};
        std::string error;
        *ok &= Expect(
            app::ParseOrderEventAggregatorArgumentsV1(
                arguments, &options, &error) ==
                    app::OrderEventAggregatorParseResultV1::kError,
            "idle poll interval above one second is rejected");
    }
    {
        std::vector<std::string_view> arguments(
            kValidArguments.begin(), kValidArguments.end());
        arguments.push_back("--temporal-coverage");
        arguments.push_back("market-open-ish");
        app::OrderEventAggregatorOptionsV1 options{};
        std::string error;
        *ok &= Expect(
            app::ParseOrderEventAggregatorArgumentsV1(
                arguments, &options, &error) ==
                    app::OrderEventAggregatorParseResultV1::kError &&
                error.find("from-open") != std::string::npos,
            "unknown temporal coverage mode is rejected");
    }
    {
        std::vector<std::string_view> arguments(
            kValidArguments.begin(), kValidArguments.end());
        arguments.push_back("--cpu-set");
        arguments.push_back("8, 9");
        app::OrderEventAggregatorOptionsV1 options{};
        std::string error;
        *ok &= Expect(
            app::ParseOrderEventAggregatorArgumentsV1(
                arguments, &options, &error) ==
                    app::OrderEventAggregatorParseResultV1::kError &&
                error.find("invalid_syntax") != std::string::npos,
            "Event CPU set uses the strict no-whitespace grammar");
    }
    {
        std::vector<std::string_view> arguments(
            kValidArguments.begin(), kValidArguments.end());
        arguments.push_back("--parent-pid");
        arguments.push_back("0");
        app::OrderEventAggregatorOptionsV1 options{};
        std::string error;
        *ok &= Expect(
            app::ParseOrderEventAggregatorArgumentsV1(
                arguments, &options, &error) ==
                    app::OrderEventAggregatorParseResultV1::kError &&
                error.find("parent-pid") != std::string::npos,
            "managed parent PID must be a positive pid_t");
    }
    {
        constexpr std::array<std::string_view, 2U> arguments{
            "--unknown", "1"};
        app::OrderEventAggregatorOptionsV1 options{};
        std::string error;
        *ok &= Expect(
            app::ParseOrderEventAggregatorArgumentsV1(
                arguments, &options, &error) ==
                    app::OrderEventAggregatorParseResultV1::kError &&
                error.find("unknown") != std::string::npos,
            "unknown option is rejected");
    }
    {
        constexpr std::array<std::string_view, 4U> arguments{
            "--source-socket",
            "/tmp/source.sock",
            "--source-socket",
            "/tmp/source2.sock"};
        app::OrderEventAggregatorOptionsV1 options{};
        std::string error;
        *ok &= Expect(
            app::ParseOrderEventAggregatorArgumentsV1(
                arguments, &options, &error) ==
                    app::OrderEventAggregatorParseResultV1::kError &&
                error.find("duplicate") != std::string::npos,
            "duplicate option is rejected");
    }
}

void TestFailureDiagnostic(bool* ok) {
    app::OrderEventFailureDiagnosticV1 diagnostic{};
    diagnostic.consume_error =
        ipc::OrderEventLiveConsumeErrorV1::kCoreAggregationFailed;
    diagnostic.last_shenzhen_core =
        l2flow::market::ShenzhenOrderProjectorConsumeErrorV1::
            kOutOfOrderInput;
    diagnostic.consume_result_source_tick = 0U;
    diagnostic.engine_last_consumed_source_tick = 4U;
    diagnostic.engine_failed = true;

    ipc::RealtimeWireTickPayloadV2 record{};
    record.common.tick_stream_sequence = 5U;
    record.common.source_sequence = 101U;
    record.common.ingress_sequence = 105U;
    record.common.source_stream_id = 9U;
    record.common.market = 2U;
    record.common.event_kind = 5U;
    record.common.source_slot = 3U;
    record.common.trade_date = 20260730U;
    record.common.instrument_id = 42U;
    record.channel = 2012;
    record.native_event_sequence = 49'309'108;
    record.source_raw_code_1 = 33;
    record.source_raw_code_2 = 36;

    const std::string text =
        app::FormatOrderEventFailureDiagnosticV1(
            diagnostic, record, 5U);
    *ok &= Expect(
        text.find("expected_source_tick=5") != std::string::npos &&
            text.find("record_tick_stream_sequence=5") !=
                std::string::npos &&
            text.find("record_channel=2012") != std::string::npos &&
            text.find("record_native_event_sequence=49309108") !=
                std::string::npos &&
            text.find("consume_error=core_aggregation_failed") !=
                std::string::npos &&
            text.find("last_shenzhen_core=out_of_order_input") !=
                std::string::npos &&
            text.find("engine_last_consumed_source_tick=4") !=
                std::string::npos &&
            text.find("engine_failed=true") != std::string::npos,
        "fatal diagnostics retain exact outer, core, native, and source "
        "anchors");
}

}  // namespace

int main() {
    bool ok = true;
    TestValidParse(&ok);
    TestProcessStartAndZeroPoll(&ok);
    TestHelpIsPure(&ok);
    TestInvalidInputs(&ok);
    TestFailureDiagnostic(&ok);
    if (!ok) {
        return 1;
    }
    std::cout << "order event aggregator CLI tests passed\n";
    return 0;
}
