#include "l2flow/apps/production_service_v1.h"

#include "l2flow/canonical/canonical_schema_v1.h"
#include "l2flow/route/production_route_v1.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

namespace apps = l2flow::apps;
namespace canonical = l2flow::canonical;
namespace common = l2flow::common;
namespace market = l2flow::market;
namespace route = l2flow::route;
namespace runtime = l2flow::runtime;
namespace sdk = l2flow::sdk;

namespace {

struct TestContext final {
    void Expect(bool condition, std::string_view message) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << message << '\n';
        }
    }
    int failures = 0;
};

template <std::size_t Size>
std::array<std::byte, Size> Pattern(std::uint8_t seed) {
    std::array<std::byte, Size> result{};
    for (std::size_t index = 0U; index < result.size(); ++index) {
        result[index] = static_cast<std::byte>(seed + index);
    }
    return result;
}

std::vector<std::byte> Bytes(std::string_view text) {
    const std::span<const char> chars(text.data(), text.size());
    const auto bytes = std::as_bytes(chars);
    return {bytes.begin(), bytes.end()};
}

class TemporaryDirectory final {
public:
    TemporaryDirectory() {
        std::array<char, 64U> pattern{};
        const char* const value = "/tmp/l2flow-production-service-XXXXXX";
        std::memcpy(pattern.data(), value, std::strlen(value) + 1U);
        char* const created = ::mkdtemp(pattern.data());
        if (created != nullptr) {
            path_ = created;
            descriptor_ = ::open(
                path_.c_str(),
                O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
        }
    }
    ~TemporaryDirectory() {
        if (descriptor_ >= 0) {
            static_cast<void>(::close(descriptor_));
        }
        if (!path_.empty()) {
            static_cast<void>(::rmdir(path_.c_str()));
        }
    }
    [[nodiscard]] bool ok() const noexcept { return descriptor_ >= 0; }
    [[nodiscard]] int descriptor() const noexcept { return descriptor_; }
    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
    int descriptor_ = -1;
};

struct CaptureCounters final {
    std::atomic<std::uint32_t> starts{0U};
    std::atomic<std::uint32_t> stops{0U};
};

class DummyCapture final : public apps::ProductionCaptureRuntimeV1 {
public:
    explicit DummyCapture(
        std::shared_ptr<CaptureCounters> counters,
        apps::ProductionCaptureBindingV1 binding = {})
        : counters_(std::move(counters)), binding_(binding) {}
    [[nodiscard]] const apps::ProductionCaptureBindingV1& binding()
        const noexcept override {
        return binding_;
    }
    bool Start(std::string*) noexcept override {
        counters_->starts.fetch_add(1U, std::memory_order_relaxed);
        return true;
    }
    bool Stop(std::string*) noexcept override {
        counters_->stops.fetch_add(1U, std::memory_order_relaxed);
        return true;
    }

private:
    std::shared_ptr<CaptureCounters> counters_;
    const apps::ProductionCaptureBindingV1 binding_{};
};

class DummyLifetime final : public apps::ProductionSourceLifetimeV1 {};

std::unique_ptr<market::InstrumentRegistryV1> MakeRegistry(
    std::uint64_t version) {
    market::InstrumentRegistryEntryV1 entry{};
    entry.instrument_id = 1U;
    entry.key.market = market::MarketV1::kShanghai;
    entry.key.security_id = Bytes("600000");
    entry.quantity_unit = market::QuantityUnitV1::kShare;
    entry.security_type = market::SecurityTypeV1::kEquity;
    entry.asset_scope = market::AssetScopeV1::kDocumentedCore;
    std::unique_ptr<market::InstrumentRegistryV1> result;
    if (market::InstrumentRegistryV1::Create(
            version, std::span<const market::InstrumentRegistryEntryV1>(
                         &entry, 1U),
            &result) != market::InstrumentRegistryCreateErrorV1::kNone) {
        return nullptr;
    }
    return result;
}

std::unique_ptr<market::InstrumentHistoryRuntimeV1> MakeHistory(
    bool exact_source_set) {
    market::InstrumentHistoryRuntimeConfigV1 config{};
    config.source_stream_ids = route::kProductionRouteSourceStreamIdsV1;
    if (!exact_source_set) {
        config.source_stream_ids[3] = 2999U;
    }
    config.physical_worker_count = 2U;
    config.queue_capacity = 8U;
    config.maximum_inflight_per_source = 64U;
    config.chunk_record_capacity = 8U;
    config.maximum_records_per_logical_shard = 1024U;
    config.maximum_instruments_per_logical_shard = 64U;
    config.maximum_owned_payload_bytes_per_logical_shard = 1024U * 1024U;
    std::unique_ptr<market::InstrumentHistoryRuntimeV1> result;
    if (market::InstrumentHistoryRuntimeV1::Create(config, &result) !=
        market::InstrumentHistoryCreateErrorV1::kNone) {
        return nullptr;
    }
    return result;
}

route::ProductionRouteManifestV1 MakeManifest(
    const market::InstrumentRegistryV1& registry,
    const std::filesystem::path& root) {
    route::ProductionRouteManifestV1 manifest{};
    manifest.state = route::ProductionRouteStateV1::kActive;
    manifest.generation = 1U;
    manifest.route_instance = Pattern<16U>(0x10U);
    manifest.trade_date = 20260723U;
    manifest.registry_version = registry.registry_version();
    manifest.registry_sha256 = registry.registry_sha256();
    manifest.schema_sha256 = canonical::CanonicalSchemaDescriptorSha256V1();
    manifest.build_sha256 = Pattern<32U>(0x20U);
    manifest.config_sha256 = Pattern<32U>(0x30U);
    for (std::size_t index = 0U; index < manifest.sources.size(); ++index) {
        auto& source = manifest.sources[index];
        source.source_stream_id = route::kProductionRouteSourceStreamIdsV1[index];
        source.capture_date = 20260723U;
        source.stream_day_id = Pattern<16U>(
            static_cast<std::uint8_t>(0x40U + index));
        source.writer_instance = Pattern<16U>(
            static_cast<std::uint8_t>(0x50U + index));
        source.source_generation = 1U + index;
        source.canonical_generation = 11U + index;
        canonical::ClockEpochIdentityV1 clock{};
        clock.algorithm = 1U;
        clock.digest = Pattern<32U>(
            static_cast<std::uint8_t>(0x60U + index));
        clock.label = 1U;
        source.clock_epoch_identity_sha256 =
            route::ComputeProductionRouteClockEpochIdentitySha256V1(clock);
    }
    manifest.endpoints.canonical = (root / "canonical").string();
    manifest.endpoints.history = (root / "history-in-process-only").string();
    manifest.endpoints.state = (root / "state").string();
    manifest.endpoints.factor = (root / "factor").string();
    return manifest;
}

struct Prepared final {
    apps::ProductionServiceInputsV1 inputs{};
    std::shared_ptr<CaptureCounters> counters =
        std::make_shared<CaptureCounters>();
};

Prepared MakePrepared(
    TestContext* test,
    TemporaryDirectory* directory,
    bool exact_history = true,
    bool matching_registry = true) {
    Prepared result{};
    result.inputs.registry = MakeRegistry(7U);
    result.inputs.history = MakeHistory(exact_history);
    test->Expect(
        result.inputs.registry != nullptr && result.inputs.history != nullptr,
        "typed registry/history fixtures create");
    if (result.inputs.registry == nullptr || result.inputs.history == nullptr) {
        return result;
    }
    auto manifest = MakeManifest(*result.inputs.registry, directory->path());
    if (!matching_registry) {
        manifest.registry_sha256[0] ^= std::byte{0x01U};
    }
    test->Expect(
        route::ValidateProductionRouteManifestV1(manifest) ==
            route::ProductionRouteManifestErrorV1::kNone,
        "preflight route fixture is independently valid");
    result.inputs.route_controller =
        std::make_unique<route::ProductionRouteControllerV1>(
            directory->descriptor(), std::move(manifest));
    for (std::size_t index = 0U; index < result.inputs.captures.size(); ++index) {
        result.inputs.source_lifetimes[index] =
            std::make_unique<DummyLifetime>();
        result.inputs.captures[index] =
            std::make_unique<DummyCapture>(result.counters);
    }
    return result;
}

void TestConfigurationAndDependencyOrder(TestContext* test) {
    apps::ProductionServiceConfigV1 config{};
    config.activation_timeout = std::chrono::nanoseconds::zero();
    std::unique_ptr<apps::ProductionServiceV1> service;
    const auto invalid = apps::ProductionServiceV1::Create(
        config, {}, &service);
    test->Expect(
        invalid == apps::ProductionServiceCreateErrorV1::kInvalidConfiguration &&
            service == nullptr,
        "invalid lifecycle deadlines fail before dependency ownership changes");

    config = {};
    const auto missing = apps::ProductionServiceV1::Create(
        config, {}, &service);
    test->Expect(
        missing == apps::ProductionServiceCreateErrorV1::kNullRegistry,
        "dependency preflight is closed and deterministic");
    test->Expect(
        apps::OwnRawProductionCaptureRuntimeV1(nullptr) == nullptr,
        "null Raw runtime cannot be disguised as a capture dependency");
}

void TestCaptureBindingPredicate(TestContext* test) {
    canonical::SourceFrontierPageV1 page{};
    canonical::SourceFrontierPageV1 foreign_page{};
    runtime::ProductionSourcePipelineConfigV1 pipeline{};
    pipeline.source_slot = 3U;
    pipeline.ingress_kind = sdk::IngressKind::SzTick;
    pipeline.source_generation = 77U;

    route::ProductionRouteSourceV1 source{};
    source.source_stream_id =
        sdk::GetIngressSpec(pipeline.ingress_kind).source_stream_id;
    source.capture_date = 20260723U;
    source.stream_day_id = Pattern<16U>(0x11U);
    source.writer_instance = Pattern<16U>(0x31U);
    source.source_generation = pipeline.source_generation;

    canonical::SourceFrontierV1 frontier{};
    frontier.source_stream_id = source.source_stream_id;
    frontier.capture_date = source.capture_date;
    frontier.stream_day_id = source.stream_day_id;
    frontier.writer_instance = source.writer_instance;
    frontier.generation = source.source_generation;

    apps::ProductionCaptureBindingV1 binding{};
    binding.source_slot = pipeline.source_slot;
    binding.ingress_kind = pipeline.ingress_kind;
    binding.source_stream_id = source.source_stream_id;
    binding.capture_date = source.capture_date;
    binding.stream_day_id = source.stream_day_id;
    binding.writer_instance = source.writer_instance;
    binding.source_generation = source.source_generation;
    binding.source_frontier = &page;

    const auto matches = [&](const apps::ProductionCaptureBindingV1& value) {
        return apps::ProductionCaptureBindingMatchesV1(
            value, 3U, pipeline, source, frontier, &page);
    };
    test->Expect(matches(binding), "exact capture binding is accepted");

    auto changed = binding;
    changed.source_slot = 2U;
    test->Expect(!matches(changed), "capture slot mismatch is rejected");
    changed = binding;
    changed.ingress_kind = sdk::IngressKind::SzSnapshot;
    test->Expect(!matches(changed), "capture kind mismatch is rejected");
    changed = binding;
    ++changed.source_stream_id;
    test->Expect(!matches(changed), "capture source mismatch is rejected");
    changed = binding;
    ++changed.capture_date;
    test->Expect(!matches(changed), "capture date mismatch is rejected");
    changed = binding;
    changed.stream_day_id[0U] ^= std::byte{0x01U};
    test->Expect(!matches(changed), "capture stream-day mismatch is rejected");
    changed = binding;
    changed.writer_instance[0U] ^= std::byte{0x01U};
    test->Expect(!matches(changed), "capture writer mismatch is rejected");
    changed = binding;
    ++changed.source_generation;
    test->Expect(!matches(changed), "capture generation mismatch is rejected");
    changed = binding;
    changed.source_frontier = nullptr;
    test->Expect(!matches(changed), "null capture frontier is rejected");
    changed = binding;
    changed.source_frontier = &foreign_page;
    test->Expect(
        !matches(changed), "different capture frontier page is rejected");

    auto wrong_pipeline = pipeline;
    wrong_pipeline.source_slot = 2U;
    test->Expect(
        !apps::ProductionCaptureBindingMatchesV1(
            binding, 3U, wrong_pipeline, source, frontier, &page),
        "pipeline slot cannot be decoupled from the route position");
}

void TestNoConnectBeforeCompleteTopology(TestContext* test) {
    TemporaryDirectory directory;
    test->Expect(directory.ok(), "route directory opens");
    if (!directory.ok()) {
        return;
    }
    Prepared prepared = MakePrepared(test, &directory);
    std::unique_ptr<apps::ProductionServiceV1> service;
    const auto error = apps::ProductionServiceV1::Create(
        {}, std::move(prepared.inputs), &service);
    test->Expect(
        error == apps::ProductionServiceCreateErrorV1::kNullPipeline &&
            service == nullptr &&
            prepared.counters->starts.load(std::memory_order_relaxed) == 0U &&
            prepared.counters->stops.load(std::memory_order_relaxed) == 0U,
        "all four concrete pipelines must validate before any capture Start");
}

void TestIdentityFailuresPrecedeRuntimeStart(TestContext* test) {
    {
        TemporaryDirectory directory;
        Prepared prepared = MakePrepared(test, &directory, true, false);
        std::unique_ptr<apps::ProductionServiceV1> service;
        const auto error = apps::ProductionServiceV1::Create(
            {}, std::move(prepared.inputs), &service);
        if (error != apps::ProductionServiceCreateErrorV1::
                         kRegistryIdentityMismatch) {
            std::cerr << "registry mismatch returned "
                      << apps::ProductionServiceCreateErrorNameV1(error)
                      << '\n';
        }
        test->Expect(
            error ==
                    apps::ProductionServiceCreateErrorV1::
                        kRegistryIdentityMismatch &&
                prepared.counters->starts.load(std::memory_order_relaxed) == 0U,
            "route/registry identity mismatch fails before SDK lifecycle");
    }
    {
        TemporaryDirectory directory;
        Prepared prepared = MakePrepared(test, &directory, false, true);
        std::unique_ptr<apps::ProductionServiceV1> service;
        const auto error = apps::ProductionServiceV1::Create(
            {}, std::move(prepared.inputs), &service);
        if (error != apps::ProductionServiceCreateErrorV1::
                         kHistorySourceSetMismatch) {
            std::cerr << "history mismatch returned "
                      << apps::ProductionServiceCreateErrorNameV1(error)
                      << '\n';
        }
        test->Expect(
            error ==
                    apps::ProductionServiceCreateErrorV1::
                        kHistorySourceSetMismatch &&
                prepared.counters->starts.load(std::memory_order_relaxed) == 0U,
            "noncanonical four-source history set fails before SDK lifecycle");
    }
}

}  // namespace

int main() {
    TestContext test;
    TestConfigurationAndDependencyOrder(&test);
    TestCaptureBindingPredicate(&test);
    TestNoConnectBeforeCompleteTopology(&test);
    TestIdentityFailuresPrecedeRuntimeStart(&test);
    if (test.failures != 0) {
        std::cerr << test.failures << " production service test(s) failed\n";
        return 1;
    }
    std::cout << "production service preflight tests passed\n";
    return 0;
}
