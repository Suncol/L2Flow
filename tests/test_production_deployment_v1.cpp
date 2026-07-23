#include "l2flow/apps/production_deployment_v1.h"

#include "l2flow/common/sha256.h"

#include <array>
#include <cstddef>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace apps = l2flow::apps;
namespace common = l2flow::common;

namespace {

struct TestContext final {
    int failures = 0;

    void Check(bool condition, std::string_view expression, int line) {
        if (!condition) {
            ++failures;
            std::cerr << "line " << line << ": " << expression
                      << " failed\n";
        }
    }
};

#define CHECK(test, expression) \
    (test)->Check((expression), #expression, __LINE__)

[[nodiscard]] std::string ReadExampleManifest() {
    std::ifstream input(
        L2FLOW_PRODUCTION_EXAMPLE_MANIFEST_PATH,
        std::ios::binary);
    return std::string(
        std::istreambuf_iterator<char>(input),
        std::istreambuf_iterator<char>());
}

[[nodiscard]] apps::ProductionDeploymentLoadResultV1 Parse(
    std::string_view bytes) {
    return apps::ParseProductionDeploymentManifestV1(
        bytes, common::ComputeSha256(bytes));
}

[[nodiscard]] std::string ReplaceOnce(
    std::string value,
    std::string_view before,
    std::string_view after) {
    const std::size_t offset = value.find(before);
    if (offset != std::string::npos) {
        value.replace(offset, before.size(), after);
    }
    return value;
}

void TestValidManifest(TestContext* test, const std::string& manifest) {
    CHECK(test, !manifest.empty());
    CHECK(test, manifest.find("REPLACE_BEFORE_PRODUCTION") !=
                    std::string::npos);
    const apps::ProductionDeploymentLoadResultV1 result = Parse(manifest);
    CHECK(test, result.ok());
    if (result.ok()) {
        CHECK(test, result.deployment.sources.size() == 4U);
        CHECK(test,
              result.deployment.sdk_library_path ==
                  "/REPLACE_BEFORE_PRODUCTION/sdk/libmdl_api.so");
        CHECK(test, !result.deployment.raw_include_optional_index);
        CHECK(test,
              result.deployment
                      .canonical_snapshot_capacity_records_per_sink ==
                  200000U);
        CHECK(test,
              result.deployment.canonical_tick_capacity_records_per_sink ==
                  1200000U);
        CHECK(test,
              result.deployment
                      .canonical_quality_capacity_records_per_sink ==
                  15000000U);
        CHECK(test,
              result.deployment
                      .canonical_control_capacity_records_per_sink ==
                  1024U);
        CHECK(test,
              result.deployment.history_maximum_records_per_query == 65536U);
        CHECK(test,
              result.deployment.history_maximum_records_per_shard ==
                  1200000U);
    }
}

void TestExactFields(TestContext* test, const std::string& manifest) {
    const std::string unknown = manifest + "unknown.field\t1\n";
    CHECK(test,
          Parse(unknown).error ==
              apps::ProductionDeploymentErrorV1::kUnknownField);

    const std::string retired_sdk_pin =
        manifest + "sdk_library_sha256\t" + std::string(64U, '1') + "\n";
    CHECK(test,
          Parse(retired_sdk_pin).error ==
              apps::ProductionDeploymentErrorV1::kUnknownField);

    const std::string retired_archive =
        manifest + "sdk_archive_path\t/unused/sdk.tar.gz\n";
    CHECK(test,
          Parse(retired_archive).error ==
              apps::ProductionDeploymentErrorV1::kUnknownField);

    const std::string duplicate = manifest + "mode\tfresh\n";
    CHECK(test,
          Parse(duplicate).error ==
              apps::ProductionDeploymentErrorV1::kDuplicateField);

    const std::string missing = ReplaceOnce(
        manifest,
        "credential_name\tREPLACE_BEFORE_PRODUCTION_MDL_CREDENTIAL\n",
        "");
    CHECK(test, missing != manifest);
    CHECK(test,
          Parse(missing).error ==
              apps::ProductionDeploymentErrorV1::kMissingField);

    const common::Sha256Digest wrong_pin = common::ComputeSha256(
        std::string_view("a different nonempty deployment"));
    CHECK(test,
          apps::ParseProductionDeploymentManifestV1(manifest, wrong_pin)
                  .error ==
              apps::ProductionDeploymentErrorV1::kDigestMismatch);

    const std::string optional = ReplaceOnce(
        manifest,
        "raw.include_optional_index\tfalse\n",
        "raw.include_optional_index\ttrue\n");
    CHECK(test, optional != manifest);
    CHECK(test,
          Parse(optional).error ==
              apps::ProductionDeploymentErrorV1::kInvalidValue);

    const std::string false_clock = ReplaceOnce(
        manifest,
        "clock_source_config\tCLOCK_REALTIME+CLOCK_MONOTONIC_RAW:V1\n",
        "clock_source_config\tCLOCK_TAI:V1\n");
    CHECK(test, false_clock != manifest);
    CHECK(test,
          Parse(false_clock).error ==
              apps::ProductionDeploymentErrorV1::kInvalidValue);

    const std::string successor_generation = ReplaceOnce(
        manifest, "route_generation\t1\n", "route_generation\t2\n");
    CHECK(test, successor_generation != manifest);
    CHECK(test,
          Parse(successor_generation).error ==
              apps::ProductionDeploymentErrorV1::kInvalidValue);

    const std::string nonzero_predecessor = ReplaceOnce(
        manifest,
        "route_previous_generation\t0\n",
        "route_previous_generation\t1\n");
    CHECK(test, nonzero_predecessor != manifest);
    CHECK(test,
          Parse(nonzero_predecessor).error ==
              apps::ProductionDeploymentErrorV1::kInvalidValue);

    const std::string maximum_size =
        std::to_string(std::numeric_limits<std::size_t>::max());
    const std::string invalid_queue = ReplaceOnce(
        manifest,
        "history.queue_capacity\t65536\n",
        "history.queue_capacity\t" + maximum_size + "\n");
    CHECK(test, invalid_queue != manifest);
    CHECK(test,
          Parse(invalid_queue).error ==
              apps::ProductionDeploymentErrorV1::kInvalidValue);

    const std::string invalid_query = ReplaceOnce(
        manifest,
        "history.maximum_records_per_query\t65536\n",
        "history.maximum_records_per_query\t" + maximum_size + "\n");
    CHECK(test, invalid_query != manifest);
    CHECK(test,
          Parse(invalid_query).error ==
              apps::ProductionDeploymentErrorV1::kInvalidValue);

    const std::string excessive_live_segments = ReplaceOnce(
        manifest,
        "raw.live.max_segments\t8\n",
        "raw.live.max_segments\t100001\n");
    CHECK(test, excessive_live_segments != manifest);
    CHECK(test,
          Parse(excessive_live_segments).error ==
              apps::ProductionDeploymentErrorV1::kInvalidValue);

    const std::array<std::string_view, 4U> canonical_capacity_fields{{
        "canonical.snapshot_capacity_records_per_sink",
        "canonical.tick_capacity_records_per_sink",
        "canonical.quality_capacity_records_per_sink",
        "canonical.control_capacity_records_per_sink",
    }};
    for (std::string_view field : canonical_capacity_fields) {
        const std::string prefix = std::string(field) + "\t";
        const std::size_t begin = manifest.find(prefix);
        CHECK(test, begin != std::string::npos);
        if (begin == std::string::npos) {
            continue;
        }
        const std::size_t end = manifest.find('\n', begin);
        CHECK(test, end != std::string::npos);
        if (end == std::string::npos) {
            continue;
        }
        const std::string excessive_canonical_capacity = ReplaceOnce(
            manifest,
            std::string_view(manifest).substr(begin, end + 1U - begin),
            prefix + "18446744073709551615\n");
        CHECK(test, excessive_canonical_capacity != manifest);
        CHECK(test,
              Parse(excessive_canonical_capacity).error ==
                  apps::ProductionDeploymentErrorV1::kInvalidValue);
    }

    const std::string retired_unified_capacity =
        manifest + "canonical.capacity_records_per_sink\t200000\n";
    CHECK(test,
          Parse(retired_unified_capacity).error ==
              apps::ProductionDeploymentErrorV1::kUnknownField);

    const std::string sparse_index_bytes_too_infrequent = ReplaceOnce(
        manifest,
        "raw.sparse_index_every_bytes\t4194304\n",
        "raw.sparse_index_every_bytes\t4194305\n");
    CHECK(test, sparse_index_bytes_too_infrequent != manifest);
    CHECK(test,
          Parse(sparse_index_bytes_too_infrequent).error ==
              apps::ProductionDeploymentErrorV1::kInvalidValue);

    const std::string sparse_index_records_too_infrequent = ReplaceOnce(
        manifest,
        "raw.sparse_index_every_records\t4096\n",
        "raw.sparse_index_every_records\t4097\n");
    CHECK(test, sparse_index_records_too_infrequent != manifest);
    CHECK(test,
          Parse(sparse_index_records_too_infrequent).error ==
              apps::ProductionDeploymentErrorV1::kInvalidValue);
}

void TestCli(TestContext* test) {
    const std::string_view help_argument = "--help";
    const auto help = apps::ParseProductionRouterArgumentsV1(
        std::span<const std::string_view>(&help_argument, 1U));
    CHECK(test, help.ok);
    CHECK(test, help.arguments.show_help);

    const std::string digest(64U, '1');
    const std::vector<std::string_view> valid{
        "--deployment-dir",
        "/run/l2flow/deployment",
        "--manifest-sha256",
        digest,
        "--fast-plane-shadow",
        "--check",
    };
    const auto parsed = apps::ParseProductionRouterArgumentsV1(valid);
    CHECK(test, parsed.ok);
    CHECK(test, parsed.arguments.check_only);
    CHECK(test, parsed.arguments.fast_plane_shadow);
    CHECK(test,
          parsed.arguments.deployment_directory ==
              "/run/l2flow/deployment");

    const std::vector<std::string_view> bounded{
        "--deployment-dir",
        "/run/l2flow/deployment",
        "--manifest-sha256",
        digest,
        "--run-seconds",
        "300",
        "--evidence-json",
        "/run/l2flow/deployment/formal-router-evidence.json",
    };
    const auto bounded_parsed =
        apps::ParseProductionRouterArgumentsV1(bounded);
    CHECK(test, bounded_parsed.ok);
    CHECK(test, bounded_parsed.arguments.run_seconds == 300U);
    CHECK(test,
          bounded_parsed.arguments.evidence_json ==
              "/run/l2flow/deployment/formal-router-evidence.json");

    const std::vector<std::string_view> missing_evidence{
        "--deployment-dir",
        "/run/l2flow/deployment",
        "--manifest-sha256",
        digest,
        "--run-seconds",
        "300",
    };
    CHECK(test,
          !apps::ParseProductionRouterArgumentsV1(missing_evidence).ok);

    const std::vector<std::string_view> zero_seconds{
        "--deployment-dir",
        "/run/l2flow/deployment",
        "--manifest-sha256",
        digest,
        "--run-seconds",
        "0",
        "--evidence-json",
        "/run/l2flow/deployment/formal-router-evidence.json",
    };
    CHECK(test,
          !apps::ParseProductionRouterArgumentsV1(zero_seconds).ok);

    const std::vector<std::string_view> check_and_run{
        "--deployment-dir",
        "/run/l2flow/deployment",
        "--manifest-sha256",
        digest,
        "--check",
        "--run-seconds",
        "300",
        "--evidence-json",
        "/run/l2flow/deployment/formal-router-evidence.json",
    };
    CHECK(test,
          !apps::ParseProductionRouterArgumentsV1(check_and_run).ok);

    const std::vector<std::string_view> duplicate{
        "--deployment-dir",
        "/run/l2flow/deployment",
        "--deployment-dir",
        "/run/l2flow/other",
        "--manifest-sha256",
        digest,
    };
    CHECK(test,
          !apps::ParseProductionRouterArgumentsV1(duplicate).ok);

    const std::vector<std::string_view> missing_digest{
        "--deployment-dir",
        "/run/l2flow/deployment",
    };
    CHECK(test,
          !apps::ParseProductionRouterArgumentsV1(missing_digest).ok);

    const std::string usage = apps::ProductionRouterUsageV1();
    CHECK(test,
          usage.find("configs/production-v1.example.tsv") !=
              std::string::npos);
    CHECK(test, usage.find("not deployment sizing") != std::string::npos);
    CHECK(test, usage.find("--fast-plane-shadow") != std::string::npos);
    CHECK(test, usage.find("sha256sum production-v1.tsv") !=
                    std::string::npos);
}

}  // namespace

int main() {
    TestContext test;
    const std::string manifest = ReadExampleManifest();
    TestValidManifest(&test, manifest);
    TestExactFields(&test, manifest);
    TestCli(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " production deployment test(s) failed\n";
        return 1;
    }
    std::cout << "production deployment tests passed\n";
    return 0;
}
