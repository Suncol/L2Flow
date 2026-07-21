#include "l2flow/common/sha256.h"
#include "l2flow/ingress/capture_clock.h"
#include "l2flow/ingress/capture_meta.h"
#include "l2flow/ingress/capture_metrics.h"
#include "l2flow/ingress/clock_epoch.h"
#include "l2flow/sdk/ingress_config.h"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <string>
#include <string_view>

#include <unistd.h>

namespace common = l2flow::common;
namespace ingress = l2flow::ingress;
namespace sdk = l2flow::sdk;

namespace {

struct TestContext {
    void Expect(bool condition, const std::string& description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }
    int failures = 0;
};

class TemporaryClockInputs final {
public:
    TemporaryClockInputs() {
        const auto ticks =
            std::chrono::steady_clock::now()
                .time_since_epoch()
                .count();
        directory_ =
            std::filesystem::temp_directory_path() /
            ("l2flow-clock-inputs-" +
             std::to_string(::getpid()) + "-" +
             std::to_string(ticks));
        std::filesystem::create_directories(directory_);
    }

    ~TemporaryClockInputs() {
        std::error_code ignored;
        std::filesystem::remove_all(directory_, ignored);
    }

    TemporaryClockInputs(const TemporaryClockInputs&) = delete;
    TemporaryClockInputs& operator=(
        const TemporaryClockInputs&) = delete;

    [[nodiscard]] std::filesystem::path Path(
        std::string_view name) const {
        return directory_ / std::string(name);
    }

    [[nodiscard]] bool Write(
        std::string_view name,
        std::string_view contents) const {
        std::ofstream output(
            Path(name),
            std::ios::binary | std::ios::trunc);
        output.write(
            contents.data(),
            static_cast<std::streamsize>(contents.size()));
        output.flush();
        return static_cast<bool>(output);
    }

private:
    std::filesystem::path directory_;
};

sdk::IngressConfig ValidConfig() {
    sdk::IngressConfig config =
        sdk::DefaultIngressConfig(sdk::IngressKind::SzTick);
    config.endpoint.name = "sz_tick_prod";
    config.endpoint.resolved_server_address = "tcp://feed.example:1234";
    config.endpoint.contract_sha256 = std::string(64U, 'a');
    config.endpoint.message_encoding = datayes::mdl::MDLEID_MKTPRO;
    config.endpoint.merge_message = true;
    config.endpoint.send_mac_auth = false;
    config.endpoint.server_select = false;
    config.credential_name = "mdl_sz_tick_token";
    config.token = "top-secret-runtime-token";
    config.sdk_log_prefix = "/var/log/mdl/sz-tick";
    config.shadow_capture_path = "/data/mdl/shadow/sz-tick.capture";
    config.metrics_textfile_path =
        "/run/prometheus-textfile/mdl-sz-tick.prom";
    config.capture_date = 20260717U;
    return config;
}

void CheckRingCapacityMath(TestContext* test) {
    const auto lower = sdk::RecommendedRingCapacity(71'582'788U, 5U);
    const auto upper = sdk::RecommendedRingCapacity(71'582'789U, 5U);
    test->Expect(lower.has_value() &&
                     *lower == sdk::kDefaultMinimumRingBytes,
                 "capacity stays at 512 MiB through the exact lower boundary");
    test->Expect(upper.has_value() && *upper == 536'870'918U,
                 "capacity uses exact ceil(3*rate*stall/2) above boundary");
    test->Expect(!sdk::RecommendedRingCapacity(0U, 5U).has_value() &&
                     !sdk::RecommendedRingCapacity(1U, 0U).has_value(),
                 "zero rate or stall is rejected");
    test->Expect(
        !sdk::RecommendedRingCapacity(
             std::numeric_limits<std::uint64_t>::max(), 2U)
             .has_value(),
        "rate-times-stall overflow is rejected");

    const std::uint64_t largest_base_with_fitting_three_halves =
        (std::numeric_limits<std::uint64_t>::max() / 3U) * 2U;
    const auto large = sdk::RecommendedRingCapacity(
        largest_base_with_fitting_three_halves, 1U, 1U);
    test->Expect(large.has_value(),
                 "checked formula accepts a large result that still fits");
}

void CheckConfigValidationAndIdentity(TestContext* test) {
    sdk::IngressConfig config = ValidConfig();
    test->Expect(sdk::ValidateIngressConfig(config).empty(),
                 "complete endpoint-derived ingress config validates");
    const std::string canonical = sdk::CanonicalIngressConfig(config);
    test->Expect(canonical.find(config.token) == std::string::npos,
                 "canonical config never includes credential bytes");
    test->Expect(canonical.find("\"message_id\":33") != std::string::npos &&
                     canonical.find("\"message_id\":36") !=
                         std::string::npos,
                 "canonical config fixes both SZ tick subscriptions");
    test->Expect(canonical.find("\"message_id\":53") != std::string::npos,
                 "canonical config audits the forbidden 6.53 contract");
    test->Expect(sdk::IngressConfigSha256(config) ==
                     common::Sha256Hex(common::ComputeSha256(canonical)),
                 "config SHA-256 is exactly the canonical byte hash");

    sdk::IngressConfig changed_secret = config;
    changed_secret.token = "different-secret";
    test->Expect(sdk::IngressConfigSha256(changed_secret) ==
                     sdk::IngressConfigSha256(config),
                 "secret rotation does not disclose or perturb config identity");
    sdk::IngressConfig changed_contract = config;
    changed_contract.endpoint.send_mac_auth = true;
    test->Expect(sdk::IngressConfigSha256(changed_contract) !=
                     sdk::IngressConfigSha256(config),
                 "endpoint-contract behavior changes config identity");

    sdk::IngressConfig invalid = config;
    invalid.endpoint.send_mac_auth.reset();
    test->Expect(!sdk::ValidateIngressConfig(invalid).empty(),
                 "implicit MAC-auth default is rejected");
    invalid = config;
    invalid.endpoint.message_encoding = datayes::mdl::MDLEID_UNDEFINED;
    test->Expect(!sdk::ValidateIngressConfig(invalid).empty(),
                 "implicit message encoding is rejected");
    invalid.endpoint.message_encoding =
        static_cast<datayes::mdl::MDLMessageEncoding>(8);
    test->Expect(!sdk::ValidateIngressConfig(invalid).empty(),
                 "an integer outside the frozen encoding enum is rejected");
    invalid = config;
    invalid.heartbeat_timeout_seconds =
        invalid.heartbeat_interval_seconds;
    test->Expect(!sdk::ValidateIngressConfig(invalid).empty(),
                 "heartbeat timeout must exceed interval");
    invalid = config;
    invalid.capture_date = 20260229U;
    test->Expect(!sdk::ValidateIngressConfig(invalid).empty(),
                 "invalid Gregorian capture date is rejected");
    invalid = config;
    invalid.capture_date = 100001231U;
    test->Expect(!sdk::ValidateIngressConfig(invalid).empty(),
                 "a year above the four-digit YYYY range is rejected");
    invalid = config;
    invalid.capture_date = 20280229U;
    test->Expect(sdk::ValidateIngressConfig(invalid).empty(),
                 "valid Gregorian leap day is accepted");
    invalid = config;
    invalid.endpoint.resolved_server_address =
        std::string("feed\0address", 12U);
    test->Expect(!sdk::ValidateIngressConfig(invalid).empty(),
                 "SDK C-string fields reject embedded NUL bytes");
    invalid = config;
    invalid.credential_name = "../token";
    test->Expect(!sdk::ValidateIngressConfig(invalid).empty(),
                 "credential name is exactly one path component");
    invalid = config;
    invalid.token = std::string("secret\0suffix", 13U);
    test->Expect(!sdk::ValidateIngressConfig(invalid).empty(),
                 "runtime credential bytes cannot contain NUL");
    invalid = config;
    invalid.metrics_textfile_path = "relative.prom";
    test->Expect(!sdk::ValidateIngressConfig(invalid).empty(),
                 "metrics textfile path must be absolute");
    invalid = config;
    invalid.metrics_textfile_path =
        invalid.shadow_capture_path;
    test->Expect(
        !sdk::ValidateIngressConfig(invalid).empty(),
        "metrics and shadow output paths must be distinct");
    invalid = config;
    invalid.ring_capacity_bytes =
        sizeof(ingress::CaptureMetaV1) + invalid.max_message_bytes;
    test->Expect(!sdk::ValidateIngressConfig(invalid).empty(),
                 "ring must also fit its commit length");
    invalid = config;
    invalid.max_message_bytes =
        std::numeric_limits<std::uint32_t>::max();
    invalid.ring_capacity_bytes =
        std::numeric_limits<std::uint64_t>::max();
    test->Expect(!sdk::ValidateIngressConfig(invalid).empty(),
                 "an entry larger than uint32 commit length is rejected");
    invalid = config;
    invalid.first_ingress_sequence =
        std::numeric_limits<std::uint64_t>::max();
    test->Expect(!sdk::ValidateIngressConfig(invalid).empty(),
                 "the sequence exhaustion sentinel cannot be configured");
}

void CheckClockEpoch(TestContext* test) {
    const ingress::ClockEpochInputs inputs{
        "host-uuid", "boot-uuid", "CLOCK_MONOTONIC_RAW"};
    const ingress::ClockEpoch first = ingress::ComputeClockEpoch(inputs);
    const ingress::ClockEpoch repeated = ingress::ComputeClockEpoch(inputs);
    test->Expect(first.value == repeated.value &&
                     first.digest == repeated.digest &&
                     first.value != 0U,
                 "clock epoch is deterministic across four service processes");

    ingress::ClockEpochInputs changed = inputs;
    changed.linux_boot_id = "next-boot";
    test->Expect(ingress::ComputeClockEpoch(changed).digest != first.digest,
                 "host reboot changes clock epoch");
    changed = inputs;
    changed.clock_source_config = "CLOCK_MONOTONIC";
    test->Expect(ingress::ComputeClockEpoch(changed).digest != first.digest,
                 "clock source configuration changes clock epoch");

    // Length prefixes make concatenation collisions impossible.
    const ingress::ClockEpochInputs left{"ab", "c", "d"};
    const ingress::ClockEpochInputs right{"a", "bc", "d"};
    test->Expect(ingress::ComputeClockEpoch(left).digest !=
                     ingress::ComputeClockEpoch(right).digest,
                 "clock epoch fields are unambiguously length-prefixed");
}

void CheckClockEpochFileInputs(TestContext* test) {
    TemporaryClockInputs temporary;
    const std::filesystem::path host =
        temporary.Path("host_uuid");
    const std::filesystem::path boot =
        temporary.Path("boot_id");
    test->Expect(
        temporary.Write("host_uuid", "host-id\n") &&
            temporary.Write("boot_id", "boot-id\r\n"),
        "clock identity fixtures are written");

    ingress::ClockEpochInputs inputs;
    std::string error;
    test->Expect(
        ingress::ReadClockEpochInputs(
            host.string(),
            boot.string(),
            "CLOCK_MONOTONIC_RAW",
            &inputs,
            &error) &&
            inputs.host_uuid == "host-id" &&
            inputs.linux_boot_id == "boot-id",
        "clock identity input strips only trailing line endings");

    test->Expect(
        temporary.Write(
            "host_uuid", std::string(4096U, 'h')) &&
            ingress::ReadClockEpochInputs(
                host.string(),
                boot.string(),
                "CLOCK_MONOTONIC_RAW",
                &inputs,
                &error),
        "the exact 4096-byte identity boundary is accepted");
    test->Expect(
        temporary.Write(
            "host_uuid", std::string(4097U, 'h')) &&
            !ingress::ReadClockEpochInputs(
                host.string(),
                boot.string(),
                "CLOCK_MONOTONIC_RAW",
                &inputs,
                &error),
        "clock identity reads stop and reject at 4097 bytes");

    const std::string embedded_nul("host\0id", 7U);
    test->Expect(
        temporary.Write("host_uuid", embedded_nul) &&
            !ingress::ReadClockEpochInputs(
                host.string(),
                boot.string(),
                "CLOCK_MONOTONIC_RAW",
                &inputs,
                &error),
        "clock identity files containing NUL are rejected");
    test->Expect(
        temporary.Write("host_uuid", "host-id") &&
            !ingress::ReadClockEpochInputs(
                host.string(),
                boot.string(),
                std::string("clock\0source", 12U),
                &inputs,
                &error) &&
            !ingress::ReadClockEpochInputs(
                host.string(),
                boot.string(),
                "CLOCK_MONOTONIC_RAW",
                nullptr,
                &error),
        "invalid clock source text and null output fail closed");
}

void CheckClockAndMetrics(TestContext* test) {
    ingress::LinuxCaptureClock clock;
    const std::uint64_t monotonic = clock.MonotonicRawNanoseconds();
    const std::uint64_t realtime = clock.RealtimeNanoseconds();
    test->Expect(monotonic != 0U && realtime != 0U,
                 "Linux capture clocks return nanoseconds");

    ingress::CaptureMetrics metrics;
    metrics.IncrementCallbackInvocations();
    metrics.IncrementCaptured(23U, 9U);
    metrics.IncrementInvalid(
        ingress::InvalidMessageReason::MessageTooLarge);
    metrics.RecordLatencyNanoseconds(0U);
    metrics.RecordLatencyNanoseconds(1U);
    metrics.RecordLatencyNanoseconds(2U);
    metrics.RecordLatencyNanoseconds(
        std::numeric_limits<std::uint64_t>::max());
    const ingress::CaptureMetricsSnapshot snapshot = metrics.Snapshot();
    test->Expect(snapshot.callback_invocations == 1U &&
                     snapshot.captured_records == 1U &&
                     snapshot.captured_vendor_bytes == 23U &&
                     snapshot.captured_ingress_sequence == 9U,
                 "capture metrics snapshot preserves exact counters");
    test->Expect(
        snapshot.invalid_messages[static_cast<std::size_t>(
            ingress::InvalidMessageReason::MessageTooLarge)] == 1U,
        "invalid-message metric is reason-specific");
    test->Expect(snapshot.latency_ns.front() == 2U &&
                     snapshot.latency_ns[1U] == 1U &&
                     snapshot.latency_ns.back() == 1U,
                 "latency histogram uses exact floor-log2 buckets");

    const std::string rendered = ingress::RenderPrometheusMetrics(
        "mdl-ingress-sz-tick", 2002U, snapshot, 67U, 1024U, false);
    test->Expect(
        rendered.find("mdl_captured_ingress_sequence") !=
                std::string::npos &&
            rendered.find("source_stream_id=\"2002\"") !=
                std::string::npos &&
            rendered.find(
                "mdl_ingress_ring_utilization_ratio"
                "{service=\"mdl-ingress-sz-tick\","
                "source_stream_id=\"2002\"} "
                "0.0654296875") != std::string::npos,
        "Prometheus rendering includes stream identity, cursor, and "
        "ring utilization");
    const std::string escaped =
        ingress::RenderPrometheusMetrics(
            "service\"\\\nname",
            1U,
            snapshot,
            0U,
            1U,
            false);
    test->Expect(
        escaped.find(
            "service=\"service\\\"\\\\\\nname\"") !=
            std::string::npos,
        "Prometheus label values escape quotes, backslashes, and newlines");
}

}  // namespace

int main() {
    TestContext test;
    CheckRingCapacityMath(&test);
    CheckConfigValidationAndIdentity(&test);
    CheckClockEpoch(&test);
    CheckClockEpochFileInputs(&test);
    CheckClockAndMetrics(&test);
    if (test.failures != 0) {
        std::cerr << test.failures << " phase-1 config/clock test(s) failed\n";
        return 1;
    }
    std::cout << "phase-1 config/clock checks passed\n";
    return 0;
}
