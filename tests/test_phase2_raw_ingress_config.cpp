#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/ingress/raw_ingress_config.h"
#include "l2flow/ingress/raw_schema.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

namespace common = l2flow::common;
namespace ingress = l2flow::ingress;
namespace sdk = l2flow::sdk;

struct TestContext final {
    void Expect(bool condition, std::string_view message) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << message << '\n';
        }
    }

    int failures = 0;
};

ingress::RawIngressConfig ValidConfig(
    sdk::IngressKind kind = sdk::IngressKind::SzTick) {
    ingress::RawIngressConfig config =
        ingress::DefaultRawIngressConfig(kind);
    config.endpoint_contract_sha256 =
        std::string(64U, 'a');
    config.credential_name = "mdl_sz_tick_token";
    config.sdk_log_prefix = "/var/log/l2flow/sz-tick";
    config.metrics_textfile_path =
        "/run/l2flow/metrics/sz-tick.prom";
    config.include_optional_index = true;
    config.raw_root = "/data/mdl/raw";
    config.reserve_domain_id = "raw-nvme0-project-mdl";
    config.reserve_coordinator_socket =
        "/run/l2flow/raw-reserve-coordinator.sock";
    config.canonical_clock_source_config =
        "CLOCK_MONOTONIC_RAW+CLOCK_REALTIME";
    return config;
}

template <std::size_t Size>
std::array<std::byte, Size> Pattern(std::uint8_t first) {
    std::array<std::byte, Size> result{};
    for (std::size_t index = 0U; index < result.size(); ++index) {
        result[index] =
            static_cast<std::byte>(
                static_cast<std::uint8_t>(
                    first + static_cast<std::uint8_t>(index)));
    }
    return result;
}

std::uint64_t DigestLabel(
    const common::Sha256Digest& digest) {
    std::uint64_t result = 0U;
    for (std::size_t index = 0U; index < 8U; ++index) {
        result =
            (result << 8U) |
            std::to_integer<std::uint64_t>(digest[index]);
    }
    return result;
}

ingress::RawIngressRuntimeState ValidRuntime(
    const ingress::RawIngressConfig& config) {
    const sdk::IngressSpec& spec =
        sdk::GetIngressSpec(config.kind);
    ingress::RawIngressRuntimeState state;
    state.source_stream_id = spec.source_stream_id;
    state.capture_date = 20260718U;
    state.stream_day_id = Pattern<16U>(0x10U);
    state.recovered_next_ingress_sequence = 43U;
    state.append = {
        .segment_sequence = 3U,
        .global_wal_pos = 9000U,
        .ingress_sequence = 42U,
        .segment_offset = 5000U,
    };
    state.durable = {
        .segment_sequence = 3U,
        .global_wal_pos = 8096U,
        .ingress_sequence = 40U,
        .segment_offset = 4000U + 96U,
    };
    state.writer_instance = Pattern<16U>(0x30U);
    state.current_segment_sequence = 3U;
    state.clock_epoch_algorithm_version =
        config.clock_epoch_algorithm_version;
    state.clock_epoch_digest = Pattern<32U>(0x50U);
    state.clock_epoch_label =
        DigestLabel(state.clock_epoch_digest);
    return state;
}

void CheckDefaultsAreStartingPoints(TestContext* test) {
    const ingress::RawIngressConfig config =
        ingress::DefaultRawIngressConfig(
            sdk::IngressKind::ShSnapshot);
    test->Expect(
        config.segment_target_bytes ==
                4ULL * 1024ULL * 1024ULL * 1024ULL &&
            config.segment_max_age_seconds == 300U &&
            config.sync_interval_milliseconds == 10U &&
            config.sync_bytes == 4ULL * 1024ULL * 1024ULL &&
            config.sparse_index_every_records == 4096U &&
            config.sparse_index_every_bytes ==
                4ULL * 1024ULL * 1024ULL,
        "defaults preserve the documented operator starting points");
    test->Expect(
        config.raw_schema_sha256 ==
            ingress::kFrozenRawSchemaSha256Hex,
        "default config pins the exact implemented Raw V1 schema");
    test->Expect(
        !ingress::ValidateRawIngressConfig(config).empty(),
        "defaults remain invalid until deployment identities and paths are supplied");

    ingress::RawIngressConfig tuned = ValidConfig();
    tuned.segment_target_bytes =
        2ULL * 1024ULL * 1024ULL * 1024ULL;
    tuned.segment_max_age_seconds = 120U;
    test->Expect(
        ingress::ValidateRawIngressConfig(tuned).empty(),
        "4 GiB and five minutes are tunable defaults, not hard-coded facts");
}

void CheckCanonicalGolden(TestContext* test) {
    const ingress::RawIngressConfig config = ValidConfig();
    const std::string canonical =
        ingress::CanonicalRawIngressConfig(config);
    const std::string expected =
        "{\"clock\":{\"algorithm_version\":1,\"source_config\":"
        "\"CLOCK_MONOTONIC_RAW+CLOCK_REALTIME\"},\"config_schema\":"
        "\"l2flow.raw-ingress.stable.v1\",\"credential_name\":"
        "\"mdl_sz_tick_token\",\"endpoint_contract_sha256\":"
        "\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\","
        "\"heartbeat\":{\"interval_seconds\":10,\"timeout_seconds\":30},"
        "\"index\":{\"every_bytes\":4194304,\"every_records\":4096},"
        "\"metrics_textfile_path\":\"/run/l2flow/metrics/sz-tick.prom\","
        "\"raw\":{\"format_version\":1,\"root\":\"/data/mdl/raw\","
        "\"schema_sha256\":"
        "\"cc8c858ac3b7ba2424a6bda87c961574fb7f8b73e9135391dd316672c85789c3\"},"
        "\"reserve\":{\"ack_timeout_ms\":5000,\"coordinator_socket\":"
        "\"/run/l2flow/raw-reserve-coordinator.sock\",\"domain_id\":"
        "\"raw-nvme0-project-mdl\",\"emergency_reserve_bytes\":53687091200},"
        "\"ring\":{\"capacity_bytes\":536870912,\"max_message_bytes\":16777216,"
        "\"stall_budget_seconds\":5},\"sdk\":{\"io_threads\":1,\"log_prefix\":"
        "\"/var/log/l2flow/sz-tick\",\"work_threads\":4},\"segment\":"
        "{\"max_age_seconds\":300,\"target_bytes\":4294967296},"
        "\"service_name\":\"mdl-ingress-sz-tick\",\"source_stream_id\":2002,"
        "\"subscriptions\":{\"forbidden\":[{\"message_id\":53,\"service_id\":6,"
        "\"service_version\":101}],\"include_optional\":true,\"optional\":[],"
        "\"required\":[{\"message_id\":33,\"service_id\":6,\"service_version\":101},"
        "{\"message_id\":36,\"service_id\":6,\"service_version\":101}]},"
        "\"sync\":{\"bytes\":4194304,\"interval_ms\":10}}";
    test->Expect(
        canonical == expected,
        "canonical stable config bytes match the frozen typed golden");

    const std::string digest =
        ingress::RawIngressConfigSha256(config);
    test->Expect(
        digest ==
            "b6688356e6f5f3c7db3ad2df27968a60e5afe6b9b1e389b33524de7f36f79040",
        "stable config SHA-256 matches its golden");
    test->Expect(
        digest == common::Sha256Hex(
            common::ComputeSha256(canonical)),
        "config SHA-256 covers exactly the canonical bytes");
}

void CheckHashDomainSeparation(TestContext* test) {
    const ingress::RawIngressConfig config = ValidConfig();
    const std::string canonical =
        ingress::CanonicalRawIngressConfig(config);
    const std::string digest =
        ingress::RawIngressConfigSha256(config);

    ingress::RawIngressProcessInputs first_process{
        .endpoint_contract_path =
            "/etc/mdl/endpoint-contracts/sz_tick_prod.json",
        .credential_token = "first-super-secret-token",
        .configured_capture_date = 20260718U,
    };
    ingress::RawIngressProcessInputs second_process = first_process;
    second_process.endpoint_contract_path =
        "/secure/contracts/same-bytes.json";
    second_process.credential_token =
        "second-different-secret-token";
    second_process.configured_capture_date = 20260719U;
    test->Expect(
        ingress::ValidateRawIngressProcessInputs(first_process).empty() &&
            ingress::ValidateRawIngressProcessInputs(second_process).empty(),
        "separate process-only endpoint path and token inputs validate");
    test->Expect(
        ingress::RawIngressConfigSha256(config) == digest &&
            canonical.find(first_process.credential_token) ==
                std::string::npos &&
            canonical.find(second_process.credential_token) ==
                std::string::npos &&
            canonical.find("configured_capture_date") ==
                std::string::npos,
        "runtime token, contract lookup path, and configured date cannot enter stable hash bytes");

    ingress::RawIngressRuntimeState runtime =
        ValidRuntime(config);
    const std::string first_stream_day =
        common::Identity128Hex(runtime.stream_day_id);
    runtime.capture_date = 20260719U;
    runtime.stream_day_id = Pattern<16U>(0x70U);
    runtime.recovered_next_ingress_sequence = 1001U;
    runtime.append.ingress_sequence = 1000U;
    runtime.durable.ingress_sequence = 999U;
    runtime.writer_instance = Pattern<16U>(0x90U);
    runtime.current_segment_sequence = 9U;
    runtime.append.segment_sequence = 9U;
    runtime.durable.segment_sequence = 8U;
    test->Expect(
        ingress::RawIngressConfigSha256(config) == digest &&
            canonical.find(first_stream_day) ==
                std::string::npos &&
            canonical.find("stream_day_id") ==
                std::string::npos &&
            canonical.find("first_ingress_sequence") ==
                std::string::npos &&
            canonical.find("append") ==
                std::string::npos &&
            canonical.find("durable") ==
                std::string::npos &&
            canonical.find("writer_instance") ==
                std::string::npos &&
            canonical.find("shadow_capture_path") ==
                std::string::npos,
        "recovered/date/cursor/writer/shadow state is outside stable config identity");
}

void CheckEveryStableGroupAffectsIdentity(TestContext* test) {
    const ingress::RawIngressConfig baseline = ValidConfig();
    const std::string expected =
        ingress::RawIngressConfigSha256(baseline);
    std::vector<std::pair<std::string, ingress::RawIngressConfig>>
        variants;
    auto add = [&variants](
                   std::string name,
                   ingress::RawIngressConfig value) {
        variants.emplace_back(std::move(name), std::move(value));
    };

    ingress::RawIngressConfig value = baseline;
    value.endpoint_contract_sha256 = std::string(64U, 'b');
    add("endpoint contract", value);
    value = baseline;
    value.include_optional_index = false;
    add("subscription selection", value);
    value = baseline;
    ++value.ring_capacity_bytes;
    add("ring", value);
    value = baseline;
    value.raw_root = "/data/mdl/raw-second";
    add("Raw root", value);
    value = baseline;
    --value.segment_target_bytes;
    add("segment threshold", value);
    value = baseline;
    ++value.sync_interval_milliseconds;
    add("sync threshold", value);
    value = baseline;
    ++value.sparse_index_every_records;
    add("index threshold", value);
    value = baseline;
    value.reserve_domain_id = "raw-nvme1-project-mdl";
    add("reserve identity", value);
    value = baseline;
    value.canonical_clock_source_config =
        "CLOCK_MONOTONIC_RAW+CLOCK_TAI";
    add("clock config", value);

    for (const auto& [name, variant] : variants) {
        test->Expect(
            ingress::ValidateRawIngressConfig(variant).empty(),
            name + " variant remains valid");
        test->Expect(
            ingress::RawIngressConfigSha256(variant) !=
                expected,
            name + " changes stable identity");
    }
}

void CheckValidationBoundaries(TestContext* test) {
    const ingress::RawIngressConfig baseline = ValidConfig();
    auto rejects = [test, &baseline](
                       std::string_view name,
                       auto mutate) {
        ingress::RawIngressConfig value = baseline;
        mutate(&value);
        test->Expect(
            !ingress::ValidateRawIngressConfig(value).empty(),
            std::string(name) + " is rejected");
        bool threw = false;
        try {
            static_cast<void>(
                ingress::CanonicalRawIngressConfig(value));
        } catch (const std::invalid_argument&) {
            threw = true;
        }
        test->Expect(
            threw,
            std::string(name) +
                " cannot be hashed as a valid config");
    };

    rejects("unknown ingress kind", [](auto* value) {
        value->kind = static_cast<sdk::IngressKind>(255U);
    });
    rejects("uppercase endpoint hash", [](auto* value) {
        value->endpoint_contract_sha256 = std::string(64U, 'A');
    });
    rejects("unsafe credential name", [](auto* value) {
        value->credential_name = "../token";
    });
    rejects("control byte in SDK log prefix", [](auto* value) {
        value->sdk_log_prefix = "bad\nlog";
    });
    rejects("relative metrics path", [](auto* value) {
        value->metrics_textfile_path = "metrics.prom";
    });
    rejects("unnormalized metrics path", [](auto* value) {
        value->metrics_textfile_path =
            "/run/l2flow/../metrics.prom";
    });
    rejects("zero SDK thread count", [](auto* value) {
        value->work_threads = 0;
    });
    rejects("heartbeat inversion", [](auto* value) {
        value->heartbeat_timeout_seconds =
            value->heartbeat_interval_seconds;
    });
    rejects("message shorter than vendor head", [](auto* value) {
        value->max_message_bytes = 22U;
    });
    rejects("unrepresentable Raw record", [](auto* value) {
        value->max_message_bytes =
            std::numeric_limits<std::uint32_t>::max();
    });
    rejects("ring cannot hold maximum entry", [](auto* value) {
        value->ring_capacity_bytes = 100U;
    });
    rejects("zero ring stall budget", [](auto* value) {
        value->ring_stall_budget_seconds = 0U;
    });
    rejects("relative Raw root", [](auto* value) {
        value->raw_root = "data/raw";
    });
    rejects("unsupported Raw format", [](auto* value) {
        value->raw_format_version = 2U;
    });
    rejects("wrong Raw schema hash", [](auto* value) {
        value->raw_schema_sha256 = std::string(64U, '0');
    });
    rejects("segment cannot hold maximum record", [](auto* value) {
        value->segment_target_bytes = 4096U;
    });
    rejects("segment beyond signed file offsets", [](auto* value) {
        value->segment_target_bytes =
            static_cast<std::uint64_t>(
                std::numeric_limits<std::int64_t>::max()) +
            1U;
    });
    rejects("zero segment age", [](auto* value) {
        value->segment_max_age_seconds = 0U;
    });
    rejects("zero sync interval", [](auto* value) {
        value->sync_interval_milliseconds = 0U;
    });
    rejects("sync bytes exceed segment", [](auto* value) {
        value->sync_bytes = value->segment_target_bytes;
    });
    rejects("zero sparse record interval", [](auto* value) {
        value->sparse_index_every_records = 0U;
    });
    rejects("sparse bytes exceed segment", [](auto* value) {
        value->sparse_index_every_bytes =
            value->segment_target_bytes;
    });
    rejects("unsafe reserve domain", [](auto* value) {
        value->reserve_domain_id = "raw/domain";
    });
    rejects("relative reserve socket", [](auto* value) {
        value->reserve_coordinator_socket = "reserve.sock";
    });
    rejects("zero reserve ACK timeout", [](auto* value) {
        value->reserve_ack_timeout_milliseconds = 0U;
    });
    rejects("zero emergency reserve", [](auto* value) {
        value->emergency_reserve_bytes = 0U;
    });
    rejects("unsupported clock algorithm", [](auto* value) {
        value->clock_epoch_algorithm_version = 2U;
    });
    rejects("invalid UTF-8 clock config", [](auto* value) {
        value->canonical_clock_source_config =
            std::string("\xc0\xaf", 2U);
    });

    ingress::RawIngressProcessInputs inputs{
        .endpoint_contract_path = "relative.json",
        .credential_token = "secret",
        .configured_capture_date = 20260719U,
    };
    test->Expect(
        !ingress::ValidateRawIngressProcessInputs(inputs).empty(),
        "relative contract lookup path is rejected");
    inputs.endpoint_contract_path =
        "/etc/mdl/endpoint-contract.json";
    inputs.credential_token.clear();
    test->Expect(
        !ingress::ValidateRawIngressProcessInputs(inputs).empty(),
        "empty credential token is rejected without exposing it");
    inputs.credential_token = "secret";
    inputs.configured_capture_date = 20260230U;
    test->Expect(
        !ingress::ValidateRawIngressProcessInputs(inputs).empty(),
        "invalid configured capture date is rejected before route discovery");
}

void CheckRuntimeValidation(TestContext* test) {
    const ingress::RawIngressConfig config = ValidConfig();
    const ingress::RawIngressRuntimeState baseline =
        ValidRuntime(config);
    test->Expect(
        ingress::ValidateRawIngressRuntimeState(
            config, baseline).empty(),
        "coherent recovered runtime state validates separately");

    auto rejects = [test, &config, &baseline](
                       std::string_view name,
                       auto mutate) {
        ingress::RawIngressRuntimeState value = baseline;
        mutate(&value);
        test->Expect(
            !ingress::ValidateRawIngressRuntimeState(
                 config, value).empty(),
            std::string(name) + " runtime state is rejected");
    };
    rejects("wrong stream", [](auto* value) {
        ++value->source_stream_id;
    });
    rejects("invalid capture date", [](auto* value) {
        value->capture_date = 20260230U;
    });
    rejects("zero stream day", [](auto* value) {
        value->stream_day_id = {};
    });
    rejects("wrong current segment", [](auto* value) {
        ++value->current_segment_sequence;
    });
    rejects("durable beyond append", [](auto* value) {
        value->durable.global_wal_pos =
            value->append.global_wal_pos + 1U;
    });
    rejects("cursor segment-base disagreement", [](auto* value) {
        ++value->durable.global_wal_pos;
    });
    rejects("equal WAL position with different ingress", [](auto* value) {
        value->durable.global_wal_pos =
            value->append.global_wal_pos;
        value->durable.segment_offset =
            value->append.segment_offset;
    });
    rejects("wrong recovered next ingress", [](auto* value) {
        ++value->recovered_next_ingress_sequence;
    });
    rejects("zero clock digest", [](auto* value) {
        value->clock_epoch_digest = {};
        value->clock_epoch_label = 0U;
    });
    rejects("wrong clock label", [](auto* value) {
        ++value->clock_epoch_label;
    });
}

void CheckResumeConnectInputGate(TestContext* test) {
    const ingress::RawIngressConfig config = ValidConfig();
    const ingress::RawIngressRuntimeState runtime =
        ValidRuntime(config);
    ingress::RawIngressProcessInputs inputs{
        .endpoint_contract_path =
            "/etc/mdl/endpoint-contracts/sz_tick_prod.json",
        .credential_token = "runtime-secret",
        .configured_capture_date = runtime.capture_date,
    };
    test->Expect(
        ingress::ValidateRawIngressResumeConnectInputs(
            config, inputs, runtime).empty(),
        "matching configured date and recovered runtime pass the RESUME_CONNECT input gate");

    inputs.configured_capture_date = 20260719U;
    test->Expect(
        !ingress::ValidateRawIngressResumeConnectInputs(
             config, inputs, runtime).empty(),
        "an old-date runtime cannot enter RESUME_CONNECT through a different configured date");
}

}  // namespace

int main() {
    TestContext test;
    CheckDefaultsAreStartingPoints(&test);
    CheckCanonicalGolden(&test);
    CheckHashDomainSeparation(&test);
    CheckEveryStableGroupAffectsIdentity(&test);
    CheckValidationBoundaries(&test);
    CheckRuntimeValidation(&test);
    CheckResumeConnectInputGate(&test);
    if (test.failures != 0) {
        std::cerr << test.failures << " checks failed\n";
        return 1;
    }
    std::cout << "phase2 raw ingress config tests passed\n";
    return 0;
}
