#include "l2flow/apps/production_deployment_v1.h"

#include "l2flow/common/sha256.h"

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
        CHECK(test, !result.deployment.raw_include_optional_index);
        CHECK(test,
              result.deployment.history_maximum_records_per_query == 65536U);
    }
}

void TestExactFields(TestContext* test, const std::string& manifest) {
    const std::string unknown = manifest + "unknown.field\t1\n";
    CHECK(test,
          Parse(unknown).error ==
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

    const std::string excessive_canonical_capacity = ReplaceOnce(
        manifest,
        "canonical.capacity_records_per_sink\t16777216\n",
        "canonical.capacity_records_per_sink\t18446744073709551615\n");
    CHECK(test, excessive_canonical_capacity != manifest);
    CHECK(test,
          Parse(excessive_canonical_capacity).error ==
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
        "--check",
    };
    const auto parsed = apps::ParseProductionRouterArgumentsV1(valid);
    CHECK(test, parsed.ok);
    CHECK(test, parsed.arguments.check_only);
    CHECK(test,
          parsed.arguments.deployment_directory ==
              "/run/l2flow/deployment");

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
