#include "l2flow/route/production_route_posix_store_v1.h"
#include "l2flow/route/production_route_controller_v1.h"
#include "l2flow/route/production_route_v1.h"
#include "l2flow/runtime/production_aggregate_runtime_v1.h"

#include "l2flow/common/sha256.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <limits>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace route = l2flow::route;
namespace runtime = l2flow::runtime;
namespace sdk = l2flow::sdk;

namespace {

struct TestContext final {
    int failures = 0;

    void Check(
        bool condition,
        std::string_view expression,
        int line) {
        if (!condition) {
            ++failures;
            std::cerr << "line " << line << ": " << expression
                      << " failed\n";
        }
    }
};

#define CHECK(test, expression) \
    (test)->Check((expression), #expression, __LINE__)

template <std::size_t Size>
std::array<std::byte, Size> Filled(std::uint8_t seed) {
    std::array<std::byte, Size> output{};
    for (std::size_t index = 0U; index < output.size(); ++index) {
        output[index] = std::byte{
            static_cast<unsigned char>(seed +
                                       static_cast<std::uint8_t>(index))};
    }
    return output;
}

l2flow::canonical::ClockEpochIdentityV1 SourceClock(
    std::size_t index) {
    l2flow::canonical::ClockEpochIdentityV1 identity{};
    identity.algorithm = static_cast<std::uint32_t>(1U + index);
    identity.digest = Filled<32U>(
        static_cast<std::uint8_t>(0x81U + index));
    identity.label = 100U + index;
    return identity;
}

route::ProductionRouteManifestV1 MakeManifest(
    std::uint64_t generation = 1U,
    std::uint64_t previous_generation = 0U) {
    route::ProductionRouteManifestV1 manifest;
    manifest.state = route::ProductionRouteStateV1::kActive;
    manifest.generation = generation;
    manifest.previous_generation = previous_generation;
    manifest.route_instance = Filled<16U>(0x11U);
    manifest.trade_date = 20260723U;
    manifest.registry_version = 7U;
    manifest.registry_sha256 = Filled<32U>(0x21U);
    manifest.schema_sha256 =
        l2flow::canonical::CanonicalSchemaDescriptorSha256V1();
    manifest.build_sha256 = Filled<32U>(0x41U);
    manifest.config_sha256 = Filled<32U>(0x51U);
    for (std::size_t index = 0U; index < manifest.sources.size(); ++index) {
        route::ProductionRouteSourceV1& source = manifest.sources[index];
        source.source_stream_id =
            route::kProductionRouteSourceStreamIdsV1[index];
        source.capture_date = manifest.trade_date;
        source.stream_day_id = Filled<16U>(
            static_cast<std::uint8_t>(0x61U + index));
        source.writer_instance = Filled<16U>(
            static_cast<std::uint8_t>(0x71U + index));
        source.source_generation = 20U + index;
        source.canonical_generation = 40U + index;
        source.durable_ingress_sequence = 1000U + index;
        source.durable_global_wal_pos = 9000U + (index * 8U);
        source.clock_epoch_identity_sha256 =
            route::ComputeProductionRouteClockEpochIdentitySha256V1(
                SourceClock(index));
    }
    manifest.endpoints.canonical = "/run/l2flow/canonical-v7";
    manifest.endpoints.history = "/run/l2flow/history-v7.sock";
    manifest.endpoints.state = "/dev/shm/l2flow-state-v7";
    manifest.endpoints.factor = "/run/l2flow/factor-v7.sock";
    return manifest;
}

runtime::ProductionAggregateRouteBindingV1 MakeBinding(
    const route::ProductionRouteManifestV1& manifest) {
    constexpr std::array<sdk::IngressKind, 4U> kinds{{
        sdk::IngressKind::ShSnapshot,
        sdk::IngressKind::ShTick,
        sdk::IngressKind::SzSnapshot,
        sdk::IngressKind::SzTick}};
    runtime::ProductionAggregateRouteBindingV1 binding{};
    for (std::size_t index = 0U; index < manifest.sources.size(); ++index) {
        const auto& source = manifest.sources[index];
        auto& config = binding.source_configs[index];
        config.source_slot = static_cast<std::uint8_t>(index);
        config.ingress_kind = kinds[index];
        config.trade_date = manifest.trade_date;
        config.source_generation = source.source_generation;
        config.canonical_generation = source.canonical_generation;

        auto& frontier = binding.source_frontiers[index];
        frontier.source_stream_id = source.source_stream_id;
        frontier.capture_date = source.capture_date;
        frontier.stream_day_id = source.stream_day_id;
        frontier.writer_instance = source.writer_instance;
        frontier.generation = source.source_generation;
        frontier.clock_epoch = SourceClock(index);
        frontier.source_state =
            l2flow::canonical::SourceStateV1::kHealthy;
        binding.registry_identities[index].version =
            manifest.registry_version;
        binding.registry_identities[index].sha256 =
            manifest.registry_sha256;
    }
    return binding;
}

void RecomputeWireDigest(std::vector<std::byte>* encoded) {
    static constexpr char domain_bytes[] =
        "l2flow.production-route.v1\0";
    constexpr std::string_view domain{
        domain_bytes, sizeof(domain_bytes) - 1U};
    constexpr std::size_t digest_bytes = 32U;
    l2flow::common::Sha256Hasher hasher;
    const auto* const domain_span =
        reinterpret_cast<const std::byte*>(domain.data());
    static_cast<void>(hasher.Update(std::span<const std::byte>(
        domain_span, domain.size())));
    static_cast<void>(hasher.Update(std::span<const std::byte>(
        encoded->data(), encoded->size() - digest_bytes)));
    l2flow::common::Sha256Digest digest{};
    static_cast<void>(hasher.Finalize(&digest));
    std::copy(
        digest.begin(), digest.end(),
        encoded->end() - static_cast<std::ptrdiff_t>(digest.size()));
}

class TemporaryDirectory final {
public:
    TemporaryDirectory() {
        std::array<char, 64U> pattern{};
        constexpr std::string_view prefix =
            "/tmp/l2flow-production-route-test-XXXXXX";
        std::copy(prefix.begin(), prefix.end(), pattern.begin());
        char* const created = ::mkdtemp(pattern.data());
        if (created != nullptr) {
            path_ = created;
            static_cast<void>(::chmod(path_.c_str(), 0700));
            fd_ = ::open(
                path_.c_str(),
                O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
        }
    }

    ~TemporaryDirectory() {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
        if (!path_.empty()) {
            std::error_code ignored;
            std::filesystem::remove_all(path_, ignored);
        }
    }

    TemporaryDirectory(const TemporaryDirectory&) = delete;
    TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

    [[nodiscard]] bool valid() const noexcept {
        return fd_ >= 0;
    }
    [[nodiscard]] int fd() const noexcept {
        return fd_;
    }
    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
    int fd_ = -1;
};

struct Fault final {
    route::ProductionRouteStoreOperationV1 operation =
        route::ProductionRouteStoreOperationV1::kAfterLock;
    bool fired = false;

    static bool Hook(
        void* context,
        route::ProductionRouteStoreOperationV1 operation) noexcept {
        auto* const fault = static_cast<Fault*>(context);
        if (!fault->fired && operation == fault->operation) {
            fault->fired = true;
            return false;
        }
        return true;
    }
};

struct BlockingRouteHook final {
    std::mutex mutex;
    std::condition_variable condition;
    bool entered = false;
    bool released = false;

    static bool Hook(
        void* context,
        route::ProductionRouteStoreOperationV1 operation) noexcept {
        auto* state = static_cast<BlockingRouteHook*>(context);
        if (state == nullptr || operation !=
                route::ProductionRouteStoreOperationV1::kAfterLock) {
            return true;
        }
        std::unique_lock<std::mutex> lock(state->mutex);
        state->entered = true;
        state->condition.notify_all();
        state->condition.wait(lock, [state]() { return state->released; });
        return true;
    }
};

void TestValidationAndCodec(TestContext* test) {
    const route::ProductionRouteManifestV1 manifest = MakeManifest();
    CHECK(test, route::ValidateProductionRouteManifestV1(manifest) ==
                    route::ProductionRouteManifestErrorV1::kNone);

    std::vector<std::byte> first;
    std::vector<std::byte> second;
    CHECK(test, route::EncodeProductionRouteManifestV1(manifest, &first) ==
                    route::ProductionRouteManifestErrorV1::kNone);
    CHECK(test, route::EncodeProductionRouteManifestV1(manifest, &second) ==
                    route::ProductionRouteManifestErrorV1::kNone);
    CHECK(test, first == second);
    CHECK(test, !first.empty());

    route::ProductionRouteManifestV1 decoded;
    CHECK(test, route::DecodeProductionRouteManifestV1(first, &decoded) ==
                    route::ProductionRouteManifestErrorV1::kNone);
    CHECK(test, decoded == manifest);

    route::ProductionRouteManifestV1 cross_date = manifest;
    cross_date.sources[0U].capture_date = 20260722U;
    CHECK(test, route::ValidateProductionRouteManifestV1(cross_date) ==
                    route::ProductionRouteManifestErrorV1::kNone);
    std::vector<std::byte> cross_date_encoded;
    CHECK(test, route::EncodeProductionRouteManifestV1(
                    cross_date, &cross_date_encoded) ==
                    route::ProductionRouteManifestErrorV1::kNone);
    CHECK(test, route::DecodeProductionRouteManifestV1(
                    cross_date_encoded, &decoded) ==
                    route::ProductionRouteManifestErrorV1::kNone);
    CHECK(test, decoded == cross_date);

    std::vector<std::byte> corrupt = first;
    corrupt[40U] ^= std::byte{0x01U};
    CHECK(test, route::DecodeProductionRouteManifestV1(corrupt, &decoded) ==
                    route::ProductionRouteManifestErrorV1::kDigestMismatch);

    std::vector<std::byte> reserved = first;
    reserved[10U] = std::byte{1U};
    RecomputeWireDigest(&reserved);
    CHECK(test, route::DecodeProductionRouteManifestV1(reserved, &decoded) ==
                    route::ProductionRouteManifestErrorV1::
                        kInvalidReservedField);

    std::vector<std::byte> bad_magic = first;
    bad_magic[0U] = std::byte{'X'};
    RecomputeWireDigest(&bad_magic);
    CHECK(test, route::DecodeProductionRouteManifestV1(bad_magic, &decoded) ==
                    route::ProductionRouteManifestErrorV1::kInvalidMagic);

    CHECK(test, route::DecodeProductionRouteManifestV1(
                    std::span<const std::byte>(first).first(first.size() - 1U),
                    &decoded) ==
                    route::ProductionRouteManifestErrorV1::kDigestMismatch);

    route::ProductionRouteManifestV1 invalid = manifest;
    invalid.state = route::ProductionRouteStateV1::kInvalid;
    CHECK(test, route::ValidateProductionRouteManifestV1(invalid) ==
                    route::ProductionRouteManifestErrorV1::kInvalidState);
    invalid = manifest;
    invalid.fatal_reason_code = 9U;
    CHECK(test, route::ValidateProductionRouteManifestV1(invalid) ==
                    route::ProductionRouteManifestErrorV1::
                        kInvalidFatalReason);
    invalid = manifest;
    invalid.state = route::ProductionRouteStateV1::kFatal;
    CHECK(test, route::ValidateProductionRouteManifestV1(invalid) ==
                    route::ProductionRouteManifestErrorV1::
                        kInvalidFatalReason);
    invalid.fatal_reason_code = 3U;
    CHECK(test, route::ValidateProductionRouteManifestV1(invalid) ==
                    route::ProductionRouteManifestErrorV1::kNone);
    invalid = manifest;
    std::swap(invalid.sources[0U], invalid.sources[1U]);
    CHECK(test, route::ValidateProductionRouteManifestV1(invalid) ==
                    route::ProductionRouteManifestErrorV1::kInvalidSourceSet);
    invalid = manifest;
    invalid.sources[0U].writer_instance = {};
    CHECK(test, route::ValidateProductionRouteManifestV1(invalid) ==
                    route::ProductionRouteManifestErrorV1::kInvalidSource);
    invalid = manifest;
    invalid.sources[0U].canonical_generation = 0U;
    CHECK(test, route::ValidateProductionRouteManifestV1(invalid) ==
                    route::ProductionRouteManifestErrorV1::kInvalidSource);
    invalid = manifest;
    invalid.sources[2U].capture_date = 20260230U;
    CHECK(test, route::ValidateProductionRouteManifestV1(invalid) ==
                    route::ProductionRouteManifestErrorV1::kInvalidSource);
    invalid = manifest;
    invalid.endpoints.factor = invalid.endpoints.history;
    CHECK(test, route::ValidateProductionRouteManifestV1(invalid) ==
                    route::ProductionRouteManifestErrorV1::kDuplicateEndpoint);
    invalid = manifest;
    invalid.endpoints.factor = "relative.sock";
    CHECK(test, route::ValidateProductionRouteManifestV1(invalid) ==
                    route::ProductionRouteManifestErrorV1::kInvalidEndpoint);
    invalid = manifest;
    invalid.endpoints.history.clear();
    invalid.endpoints.state.clear();
    invalid.endpoints.factor.clear();
    CHECK(test, route::ValidateProductionRouteManifestV1(invalid) ==
                    route::ProductionRouteManifestErrorV1::kNone);
    invalid.endpoints.canonical.clear();
    CHECK(test, route::ValidateProductionRouteManifestV1(invalid) ==
                    route::ProductionRouteManifestErrorV1::kInvalidEndpoint);
    invalid = manifest;
    invalid.schema_sha256 = {};
    CHECK(test, route::ValidateProductionRouteManifestV1(invalid) ==
                    route::ProductionRouteManifestErrorV1::kInvalidDigest);

    invalid = manifest;
    invalid.generation = std::numeric_limits<std::uint64_t>::max();
    invalid.previous_generation = invalid.generation - 1U;
    CHECK(test, route::ValidateProductionRouteManifestV1(invalid) ==
                    route::ProductionRouteManifestErrorV1::
                        kInvalidGeneration);
    invalid.state = route::ProductionRouteStateV1::kFatal;
    invalid.fatal_reason_code = 9U;
    CHECK(test, route::ValidateProductionRouteManifestV1(invalid) ==
                    route::ProductionRouteManifestErrorV1::kNone);
}

void TestClockEpochIdentityHash(TestContext* test) {
    l2flow::canonical::ClockEpochIdentityV1 identity{};
    identity.algorithm = 0x01020304U;
    identity.digest = Filled<32U>(0x20U);
    identity.label = 0x1122334455667788ULL;

    const l2flow::common::Sha256Digest expected{{
        std::byte{0x6cU}, std::byte{0xf5U}, std::byte{0x29U},
        std::byte{0x80U}, std::byte{0x24U}, std::byte{0x9dU},
        std::byte{0x8bU}, std::byte{0xcfU}, std::byte{0x85U},
        std::byte{0x0cU}, std::byte{0xe2U}, std::byte{0xd2U},
        std::byte{0x67U}, std::byte{0xfbU}, std::byte{0x78U},
        std::byte{0x62U}, std::byte{0x71U}, std::byte{0xf3U},
        std::byte{0x89U}, std::byte{0x86U}, std::byte{0x06U},
        std::byte{0xafU}, std::byte{0x69U}, std::byte{0x42U},
        std::byte{0x8dU}, std::byte{0x70U}, std::byte{0x42U},
        std::byte{0xbfU}, std::byte{0xdaU}, std::byte{0xc6U},
        std::byte{0x7fU}, std::byte{0x4bU}}};
    const l2flow::common::Sha256Digest baseline =
        route::ComputeProductionRouteClockEpochIdentitySha256V1(identity);
    CHECK(test, baseline == expected);

    auto changed = identity;
    changed.label ^= std::numeric_limits<std::uint64_t>::max();
    CHECK(test, changed == identity);
    CHECK(test,
          route::ComputeProductionRouteClockEpochIdentitySha256V1(changed) ==
              baseline);

    changed = identity;
    ++changed.algorithm;
    CHECK(test,
          route::ComputeProductionRouteClockEpochIdentitySha256V1(changed) !=
              baseline);
    for (const std::size_t index : {0U, 15U, 31U}) {
        changed = identity;
        changed.digest[index] ^= std::byte{0x80U};
        CHECK(test,
              route::ComputeProductionRouteClockEpochIdentitySha256V1(
                  changed) != baseline);
    }
}

void TestControllerLifecycle(TestContext* test) {
    TemporaryDirectory directory;
    CHECK(test, directory.valid());
    if (!directory.valid()) {
        return;
    }

    const route::ProductionRouteManifestV1 active = MakeManifest();
    CHECK(test, !route::ProductionRouteControllerResultV1{}.ok());
    route::ProductionRouteControllerV1 invalid_controller(-1, active);
    const route::ProductionRouteControllerResultV1 invalid_directory =
        invalid_controller.PublishActive();
    CHECK(test, invalid_directory.error ==
                    route::ProductionRouteControllerErrorV1::
                        kInvalidRetainedDirectory);
    CHECK(test, invalid_directory.system_error_number == EBADF);

    const int passed_directory_fd = ::dup(directory.fd());
    CHECK(test, passed_directory_fd >= 0);
    if (passed_directory_fd < 0) {
        return;
    }
    route::ProductionRouteControllerV1 controller(
        passed_directory_fd, active);
    CHECK(test, ::close(passed_directory_fd) == 0);
    CHECK(test, controller.active_manifest() == active);

    route::ProductionRouteControllerResultV1 result =
        controller.PublishFatal(7001U);
    CHECK(test, result.error ==
                    route::ProductionRouteControllerErrorV1::
                        kActiveNotPublished);

    route::ProductionRouteControllerResultV1 first_active;
    route::ProductionRouteControllerResultV1 second_active;
    std::thread active_left([&]() {
        first_active = controller.PublishActive();
    });
    std::thread active_right([&]() {
        second_active = controller.PublishActive();
    });
    active_left.join();
    active_right.join();
    CHECK(test, first_active.ok());
    CHECK(test, second_active.ok());
    CHECK(test, first_active.already_complete !=
                    second_active.already_complete);

    result = controller.PublishFatal(0U);
    CHECK(test, result.error ==
                    route::ProductionRouteControllerErrorV1::
                        kInvalidFatalReason);

    route::ProductionRouteControllerResultV1 first_fatal;
    route::ProductionRouteControllerResultV1 second_fatal;
    std::thread fatal_left([&]() {
        first_fatal = controller.PublishFatal(7001U);
    });
    std::thread fatal_right([&]() {
        second_fatal = controller.PublishFatal(7001U);
    });
    fatal_left.join();
    fatal_right.join();
    CHECK(test, first_fatal.ok());
    CHECK(test, second_fatal.ok());
    CHECK(test, !first_fatal.authoritative());
    CHECK(test, !second_fatal.authoritative());
    CHECK(test, first_fatal.already_complete !=
                    second_fatal.already_complete);

    const route::ProductionRouteReadResultV1 read =
        route::ReadProductionRouteManifestV1At(directory.fd());
    CHECK(test, read.error ==
                    route::ProductionRouteStoreErrorV1::kRouteFatal);
    CHECK(test, read.manifest != nullptr);
    if (read.manifest != nullptr) {
        CHECK(test, read.manifest->state ==
                        route::ProductionRouteStateV1::kFatal);
        CHECK(test, read.manifest->generation == 2U);
        CHECK(test, read.manifest->previous_generation == 1U);
        CHECK(test, read.manifest->fatal_reason_code == 7001U);
        CHECK(test, read.manifest->route_instance != active.route_instance);
    }

    result = controller.PublishFatal(7001U);
    CHECK(test, result.ok());
    CHECK(test, result.already_complete);
    result = controller.PublishFatal(7002U);
    CHECK(test, result.error ==
                    route::ProductionRouteControllerErrorV1::
                        kFatalAlreadyRequested);
    result = controller.PublishActive();
    CHECK(test, result.error ==
                    route::ProductionRouteControllerErrorV1::
                        kFatalAlreadyRequested);
    CHECK(test, controller.active_manifest() == active);
}

void TestAggregateRouteBinding(TestContext* test) {
    const route::ProductionRouteManifestV1 manifest = MakeManifest();
    const runtime::ProductionAggregateRouteBindingV1 binding =
        MakeBinding(manifest);
    CHECK(test, runtime::ValidateProductionAggregateRouteBindingV1(
                    manifest, binding) ==
                    runtime::ProductionAggregateRouteBindingErrorV1::kNone);

    auto changed = binding;
    changed.source_frontiers[2U].writer_instance = Filled<16U>(0xe1U);
    CHECK(test, runtime::ValidateProductionAggregateRouteBindingV1(
                    manifest, changed) ==
                    runtime::ProductionAggregateRouteBindingErrorV1::
                        kSourceIdentityMismatch);

    changed = binding;
    ++changed.source_configs[0U].source_generation;
    CHECK(test, runtime::ValidateProductionAggregateRouteBindingV1(
                    manifest, changed) ==
                    runtime::ProductionAggregateRouteBindingErrorV1::
                        kSourceIdentityMismatch);

    changed = binding;
    ++changed.source_configs[0U].canonical_generation;
    CHECK(test, runtime::ValidateProductionAggregateRouteBindingV1(
                    manifest, changed) ==
                    runtime::ProductionAggregateRouteBindingErrorV1::
                        kSourceIdentityMismatch);

    // Binding validation is not a READY decision.  Disconnected is a valid,
    // non-fatal identity state; the caller's control gate decides readiness.
    changed = binding;
    changed.source_frontiers[1U].source_state =
        l2flow::canonical::SourceStateV1::kDisconnected;
    CHECK(test, runtime::ValidateProductionAggregateRouteBindingV1(
                    manifest, changed) ==
                    runtime::ProductionAggregateRouteBindingErrorV1::kNone);

    changed = binding;
    ++changed.registry_identities[3U].version;
    CHECK(test, runtime::ValidateProductionAggregateRouteBindingV1(
                    manifest, changed) ==
                    runtime::ProductionAggregateRouteBindingErrorV1::
                        kRegistryIdentityMismatch);

    route::ProductionRouteManifestV1 changed_manifest = manifest;
    changed_manifest.schema_sha256[7U] ^= std::byte{0x01U};
    CHECK(test, runtime::ValidateProductionAggregateRouteBindingV1(
                    changed_manifest, binding) ==
                    runtime::ProductionAggregateRouteBindingErrorV1::
                        kSchemaIdentityMismatch);

    changed = binding;
    changed.source_frontiers[0U].clock_epoch.label ^= 0xffffU;
    CHECK(test, runtime::ValidateProductionAggregateRouteBindingV1(
                    manifest, changed) ==
                    runtime::ProductionAggregateRouteBindingErrorV1::kNone);

    changed = binding;
    changed.source_frontiers[0U].clock_epoch.digest[17U] ^=
        std::byte{0x40U};
    CHECK(test, runtime::ValidateProductionAggregateRouteBindingV1(
                    manifest, changed) ==
                    runtime::ProductionAggregateRouteBindingErrorV1::
                        kClockIdentityMismatch);

    changed = binding;
    changed.source_frontiers[1U].source_state =
        l2flow::canonical::SourceStateV1::kFatal;
    CHECK(test, runtime::ValidateProductionAggregateRouteBindingV1(
                    manifest, changed) ==
                    runtime::ProductionAggregateRouteBindingErrorV1::
                        kSourceFatal);

    changed = binding;
    changed.history_frontiers[3U].fatal = true;
    CHECK(test, runtime::ValidateProductionAggregateRouteBindingV1(
                    manifest, changed) ==
                    runtime::ProductionAggregateRouteBindingErrorV1::
                        kHistoryFatal);

    changed = binding;
    changed.source_configs[0U].trade_date = manifest.trade_date - 1U;
    CHECK(test, runtime::ValidateProductionAggregateRouteBindingV1(
                    manifest, changed) ==
                    runtime::ProductionAggregateRouteBindingErrorV1::
                        kTradeDateMismatch);

    route::ProductionRouteManifestV1 invalid = manifest;
    invalid.state = route::ProductionRouteStateV1::kInvalid;
    CHECK(test, runtime::ValidateProductionAggregateRouteBindingV1(
                    invalid, binding) ==
                    runtime::ProductionAggregateRouteBindingErrorV1::
                        kInvalidManifest);
}

runtime::ProductionSourceActivationEvidenceV1 MakeActivationEvidence(
    const route::ProductionRouteManifestV1& manifest,
    std::size_t index) {
    runtime::ProductionSourceActivationEvidenceV1 evidence{};
    const auto& source = manifest.sources[index];
    evidence.control.source_stream_id = source.source_stream_id;
    evidence.control.capture_date = source.capture_date;
    evidence.control.stream_day_id = source.stream_day_id;
    evidence.control.processed_ingress_sequence =
        source.durable_ingress_sequence;
    evidence.control.processed_record_end_wal_pos =
        source.durable_global_wal_pos;
    evidence.control.control_ready = true;
    evidence.control.decoder_evidence_ready = true;

    evidence.raw_control.generation = 2U;
    auto& raw = evidence.raw_control.snapshot;
    raw.writer_instance = source.writer_instance;
    raw.stream_day_id = source.stream_day_id;
    raw.source_stream_id = source.source_stream_id;
    raw.capture_date = source.capture_date;
    raw.segment_sequence = 1U;
    raw.append_ingress_sequence = source.durable_ingress_sequence;
    raw.append_global_wal_pos = source.durable_global_wal_pos;
    raw.durable_ingress_sequence = source.durable_ingress_sequence;
    raw.durable_global_wal_pos = source.durable_global_wal_pos;
    raw.append_segment_offset = source.durable_global_wal_pos;
    raw.durable_segment_offset = source.durable_global_wal_pos;
    raw.heartbeat_monotonic_ns = 99U;

    auto& frontier = evidence.source_frontier;
    frontier.source_stream_id = source.source_stream_id;
    frontier.capture_date = source.capture_date;
    frontier.stream_day_id = source.stream_day_id;
    frontier.writer_instance = source.writer_instance;
    frontier.generation = source.source_generation;
    frontier.clock_epoch = SourceClock(index);
    frontier.processed_ingress_sequence =
        source.durable_ingress_sequence;
    frontier.processed_global_wal_pos =
        source.durable_global_wal_pos;
    frontier.source_state = l2flow::canonical::SourceStateV1::kHealthy;

    evidence.history_barrier.source_slot =
        static_cast<std::uint8_t>(index);
    evidence.history_barrier.valid = true;
    return evidence;
}

runtime::ProductionSourcePipelineConfigV1 MakeSourceConfig(
    const route::ProductionRouteManifestV1& manifest,
    std::size_t index) {
    constexpr std::array<sdk::IngressKind, 4U> kinds{{
        sdk::IngressKind::ShSnapshot,
        sdk::IngressKind::ShTick,
        sdk::IngressKind::SzSnapshot,
        sdk::IngressKind::SzTick}};
    runtime::ProductionSourcePipelineConfigV1 config{};
    config.source_slot = static_cast<std::uint8_t>(index);
    config.ingress_kind = kinds[index];
    config.trade_date = manifest.trade_date;
    config.source_generation =
        manifest.sources[index].source_generation;
    config.canonical_generation =
        manifest.sources[index].canonical_generation;
    return config;
}

void TestMinimumActivationGate(TestContext* test) {
    const route::ProductionRouteManifestV1 manifest = MakeManifest();
    const std::size_t index = 2U;
    const auto config = MakeSourceConfig(manifest, index);
    const auto baseline = MakeActivationEvidence(manifest, index);
    const auto evaluate = [&](const auto& evidence) {
        return runtime::EvaluateProductionSourceActivationGateV1(
            manifest.sources[index],
            config,
            evidence,
            l2flow::market::InstrumentHistoryBarrierWaitErrorV1::kNone);
    };

    CHECK(test, evaluate(baseline).ready());

    auto changed = baseline;
    changed.source_frontier.source_state =
        l2flow::canonical::SourceStateV1::kDisconnected;
    auto result = evaluate(changed);
    CHECK(test, result.error == runtime::
                    ProductionSourceActivationGateErrorV1::
                        kSourceNotHealthy &&
                !result.fail_stop);

    changed = baseline;
    changed.control.control_ready = false;
    result = evaluate(changed);
    CHECK(test, result.error == runtime::
                    ProductionSourceActivationGateErrorV1::
                        kControlEvidenceIncomplete &&
                !result.fail_stop);

    changed = baseline;
    changed.raw_control.error =
        l2flow::ingress::RawLiveTailError::kControlUnavailable;
    result = evaluate(changed);
    CHECK(test, result.error == runtime::
                    ProductionSourceActivationGateErrorV1::
                        kRawControlUnavailable &&
                !result.fail_stop);

    changed.raw_control.error =
        l2flow::ingress::RawLiveTailError::kInstanceChanged;
    result = evaluate(changed);
    CHECK(test, result.error == runtime::
                    ProductionSourceActivationGateErrorV1::
                        kRawControlInvalid &&
                result.fail_stop);

    changed = baseline;
    changed.control.control_ready = false;
    changed.raw_control.snapshot.fatal_state = 7U;
    result = evaluate(changed);
    CHECK(test, result.error == runtime::
                    ProductionSourceActivationGateErrorV1::
                        kRawControlFatal &&
                result.fail_stop);

    changed = baseline;
    changed.raw_control.error =
        l2flow::ingress::RawLiveTailError::kControlUnavailable;
    changed.pipeline.history_frontier.fatal = true;
    result = evaluate(changed);
    CHECK(test, result.error == runtime::
                    ProductionSourceActivationGateErrorV1::kHistoryFatal &&
                result.fail_stop);

    changed = baseline;
    changed.raw_control.snapshot.heartbeat_monotonic_ns = 0U;
    result = evaluate(changed);
    CHECK(test, result.error == runtime::
                    ProductionSourceActivationGateErrorV1::
                        kRawHeartbeatMissing &&
                !result.fail_stop);

    changed = baseline;
    changed.source_frontier_read_error =
        l2flow::canonical::SourceFrontierErrorV1::kBusy;
    result = evaluate(changed);
    CHECK(test, result.error == runtime::
                    ProductionSourceActivationGateErrorV1::
                        kSourceFrontierBusy &&
                !result.fail_stop);

    changed = baseline;
    changed.pipeline.history_pending = true;
    result = evaluate(changed);
    CHECK(test, result.error == runtime::
                    ProductionSourceActivationGateErrorV1::
                        kHistoryAdmissionPending &&
                !result.fail_stop);

    changed = baseline;
    changed.pipeline.history_draining = true;
    result = evaluate(changed);
    CHECK(test, result.source_ended &&
                result.error == runtime::
                    ProductionSourceActivationGateErrorV1::
                        kHistoryDrainPending);

    result = runtime::EvaluateProductionSourceActivationGateV1(
        manifest.sources[index],
        config,
        baseline,
        l2flow::market::InstrumentHistoryBarrierWaitErrorV1::kTimeout);
    CHECK(test, result.error == runtime::
                    ProductionSourceActivationGateErrorV1::
                        kHistoryBarrierPending &&
                !result.fail_stop);

    changed = baseline;
    --changed.raw_control.snapshot.durable_ingress_sequence;
    result = evaluate(changed);
    CHECK(test, result.error == runtime::
                    ProductionSourceActivationGateErrorV1::
                        kRawDurabilityAnchorPending &&
                !result.fail_stop);

    changed = baseline;
    --changed.control.processed_ingress_sequence;
    --changed.source_frontier.processed_ingress_sequence;
    changed.control.processed_record_end_wal_pos -= 8U;
    changed.source_frontier.processed_global_wal_pos -= 8U;
    result = evaluate(changed);
    CHECK(test, result.error == runtime::
                    ProductionSourceActivationGateErrorV1::
                        kCanonicalProcessedAnchorPending &&
                !result.fail_stop);

    changed = baseline;
    changed.source_frontier.clock_epoch.label ^= 0xffffffffU;
    CHECK(test, evaluate(changed).ready());
    changed.source_frontier.clock_epoch.digest[3U] ^= std::byte{0x01U};
    result = evaluate(changed);
    CHECK(test, result.error == runtime::
                    ProductionSourceActivationGateErrorV1::
                        kClockIdentityMismatch &&
                result.fail_stop);

    auto pre_record_route = manifest.sources[index];
    pre_record_route.durable_ingress_sequence = 0U;
    pre_record_route.durable_global_wal_pos = 0U;
    CHECK(test, runtime::EvaluateProductionSourceActivationGateV1(
                    pre_record_route,
                    config,
                    baseline,
                    l2flow::market::
                        InstrumentHistoryBarrierWaitErrorV1::kNone)
                    .ready());
}

void TestControllerFatalRetryIdentity(TestContext* test) {
    TemporaryDirectory directory;
    CHECK(test, directory.valid());
    if (!directory.valid()) {
        return;
    }

    route::ProductionRouteControllerV1 controller(
        directory.fd(), MakeManifest());
    CHECK(test, controller.PublishActive().ok());

    Fault fault;
    fault.operation =
        route::ProductionRouteStoreOperationV1::kAfterRename;
    route::ProductionRouteStoreOptionsV1 options;
    options.operation_hook = &Fault::Hook;
    options.operation_hook_context = &fault;
    const route::ProductionRouteControllerResultV1 uncertain =
        controller.PublishFatal(8101U, options);
    CHECK(test, !uncertain.ok());
    CHECK(test, uncertain.error ==
                    route::ProductionRouteControllerErrorV1::kStoreFailure);
    CHECK(test, uncertain.publish_result.renamed);
    CHECK(test, fault.fired);

    const route::ProductionRouteReadResultV1 before_retry =
        route::ReadProductionRouteManifestV1At(directory.fd());
    CHECK(test, before_retry.error ==
                    route::ProductionRouteStoreErrorV1::kRouteFatal);
    CHECK(test, before_retry.manifest != nullptr);

    const route::ProductionRouteControllerResultV1 conflicting =
        controller.PublishFatal(8102U);
    CHECK(test, conflicting.error ==
                    route::ProductionRouteControllerErrorV1::
                        kFatalAlreadyRequested);

    const route::ProductionRouteControllerResultV1 retry =
        controller.PublishFatal(8101U);
    CHECK(test, retry.ok());
    CHECK(test, !retry.already_complete);
    CHECK(test, retry.publish_result.disposition ==
                    route::ProductionRoutePublishDispositionV1::
                        kAcceptedExisting);

    const route::ProductionRouteReadResultV1 after_retry =
        route::ReadProductionRouteManifestV1At(directory.fd());
    CHECK(test, after_retry.error ==
                    route::ProductionRouteStoreErrorV1::kRouteFatal);
    CHECK(test, after_retry.manifest != nullptr);
    if (before_retry.manifest != nullptr &&
        after_retry.manifest != nullptr) {
        CHECK(test, before_retry.manifest->route_instance ==
                        after_retry.manifest->route_instance);
        CHECK(test, *before_retry.manifest == *after_retry.manifest);
    }
}

void TestStoreLifecycle(TestContext* test) {
    TemporaryDirectory directory;
    CHECK(test, directory.valid());
    if (!directory.valid()) {
        return;
    }

    const route::ProductionRouteManifestV1 active = MakeManifest();
    route::ProductionRoutePublishResultV1 published =
        route::PublishProductionRouteManifestV1At(directory.fd(), active);
    CHECK(test, published.ok());
    CHECK(test, published.authoritative());
    CHECK(test, published.disposition ==
                    route::ProductionRoutePublishDispositionV1::kPublishedNew);
    CHECK(test, published.file_synced);
    CHECK(test, published.renamed);
    CHECK(test, published.directory_synced);

    route::ProductionRouteReadResultV1 read =
        route::ReadProductionRouteManifestV1At(directory.fd());
    CHECK(test, read.ok());
    CHECK(test, read.authoritative());
    CHECK(test, read.manifest != nullptr && *read.manifest == active);

    published = route::PublishProductionRouteManifestV1At(
        directory.fd(), active);
    CHECK(test, published.ok());
    CHECK(test, published.disposition ==
                    route::ProductionRoutePublishDispositionV1::
                        kAcceptedExisting);

    route::ProductionRouteManifestV1 conflict = active;
    conflict.endpoints.factor = "/run/l2flow/factor-conflict.sock";
    published = route::PublishProductionRouteManifestV1At(
        directory.fd(), conflict);
    CHECK(test, published.error ==
                    route::ProductionRouteStoreErrorV1::kGenerationConflict);

    route::ProductionRouteManifestV1 reused_identity =
        MakeManifest(2U, 1U);
    published = route::PublishProductionRouteManifestV1At(
        directory.fd(), reused_identity);
    CHECK(test, published.error ==
                    route::ProductionRouteStoreErrorV1::kGenerationConflict);

    route::ProductionRouteManifestV1 fatal = MakeManifest(2U, 1U);
    fatal.state = route::ProductionRouteStateV1::kFatal;
    fatal.fatal_reason_code = 7001U;
    fatal.route_instance = Filled<16U>(0x12U);
    published = route::PublishProductionRouteManifestV1At(
        directory.fd(), fatal);
    CHECK(test, published.ok());
    CHECK(test, !published.authoritative());

    read = route::ReadProductionRouteManifestV1At(directory.fd());
    CHECK(test, read.error ==
                    route::ProductionRouteStoreErrorV1::kRouteFatal);
    CHECK(test, !read.authoritative());
    CHECK(test, read.manifest != nullptr && *read.manifest == fatal);

    published = route::PublishProductionRouteManifestV1At(
        directory.fd(), active);
    CHECK(test, published.error ==
                    route::ProductionRouteStoreErrorV1::kGenerationRegression);

    route::ProductionRouteManifestV1 wrong_previous =
        MakeManifest(3U, 1U);
    wrong_previous.route_instance = Filled<16U>(0x13U);
    published = route::PublishProductionRouteManifestV1At(
        directory.fd(), wrong_previous);
    CHECK(test, published.error ==
                    route::ProductionRouteStoreErrorV1::
                        kPreviousGenerationMismatch);

    route::ProductionRouteManifestV1 recovered = MakeManifest(3U, 2U);
    recovered.route_instance = Filled<16U>(0x14U);
    recovered.endpoints.canonical = "/run/l2flow/canonical-v8";
    recovered.endpoints.history = "/run/l2flow/history-v8.sock";
    recovered.endpoints.state = "/dev/shm/l2flow-state-v8";
    recovered.endpoints.factor = "/run/l2flow/factor-v8.sock";
    published = route::PublishProductionRouteManifestV1At(
        directory.fd(), recovered);
    CHECK(test, published.ok());
    CHECK(test, published.authoritative());

    read = route::ReadProductionRouteManifestV1At(directory.fd(), 4U);
    CHECK(test, read.error ==
                    route::ProductionRouteStoreErrorV1::kGenerationRegression);
    CHECK(test, read.manifest == nullptr);
}

void TestFaultWindows(TestContext* test) {
    constexpr std::array<route::ProductionRouteStoreOperationV1, 9U>
        operations{{
            route::ProductionRouteStoreOperationV1::kAfterLock,
            route::ProductionRouteStoreOperationV1::kAfterTemporaryOpen,
            route::ProductionRouteStoreOperationV1::kAfterWrite,
            route::ProductionRouteStoreOperationV1::kAfterFileSync,
            route::ProductionRouteStoreOperationV1::kBeforeRename,
            route::ProductionRouteStoreOperationV1::kAfterRename,
            route::ProductionRouteStoreOperationV1::kBeforeDirectorySync,
            route::ProductionRouteStoreOperationV1::kAfterDirectorySync,
            route::ProductionRouteStoreOperationV1::kBeforeReadback}};

    for (const route::ProductionRouteStoreOperationV1 operation : operations) {
        TemporaryDirectory directory;
        CHECK(test, directory.valid());
        if (!directory.valid()) {
            continue;
        }
        const route::ProductionRouteManifestV1 manifest = MakeManifest();
        Fault fault;
        fault.operation = operation;
        route::ProductionRouteStoreOptionsV1 options;
        options.operation_hook = &Fault::Hook;
        options.operation_hook_context = &fault;
        const route::ProductionRoutePublishResultV1 injected =
            route::PublishProductionRouteManifestV1At(
                directory.fd(), manifest, options);
        CHECK(test, !injected.ok());
        CHECK(test, fault.fired);

        const route::ProductionRoutePublishResultV1 retry =
            route::PublishProductionRouteManifestV1At(
                directory.fd(), manifest);
        CHECK(test, retry.ok());
        const route::ProductionRouteReadResultV1 read =
            route::ReadProductionRouteManifestV1At(directory.fd());
        CHECK(test, read.authoritative());
        CHECK(test, read.manifest != nullptr &&
                        *read.manifest == manifest);
    }
}

void TestConcurrentIdempotentPublication(TestContext* test) {
    TemporaryDirectory directory;
    CHECK(test, directory.valid());
    if (!directory.valid()) {
        return;
    }
    CHECK(test, route::PublishProductionRouteManifestV1At(
                    directory.fd(), MakeManifest()).ok());
    route::ProductionRouteManifestV1 successor = MakeManifest(2U, 1U);
    successor.route_instance = Filled<16U>(0xa1U);
    successor.endpoints.canonical = "/run/l2flow/canonical-v9";
    successor.endpoints.history = "/run/l2flow/history-v9.sock";
    successor.endpoints.state = "/dev/shm/l2flow-state-v9";
    successor.endpoints.factor = "/run/l2flow/factor-v9.sock";

    route::ProductionRoutePublishResultV1 left;
    route::ProductionRoutePublishResultV1 right;
    std::thread first([&]() {
        left = route::PublishProductionRouteManifestV1At(
            directory.fd(), successor);
    });
    std::thread second([&]() {
        right = route::PublishProductionRouteManifestV1At(
            directory.fd(), successor);
    });
    first.join();
    second.join();
    CHECK(test, left.ok());
    CHECK(test, right.ok());
    CHECK(test,
          (left.disposition ==
               route::ProductionRoutePublishDispositionV1::kPublishedNew &&
           right.disposition ==
               route::ProductionRoutePublishDispositionV1::
                   kAcceptedExisting) ||
              (right.disposition ==
                   route::ProductionRoutePublishDispositionV1::kPublishedNew &&
               left.disposition ==
                   route::ProductionRoutePublishDispositionV1::
                       kAcceptedExisting));
    const route::ProductionRouteReadResultV1 read =
        route::ReadProductionRouteManifestV1At(directory.fd());
    CHECK(test, read.authoritative());
    CHECK(test, read.manifest != nullptr && *read.manifest == successor);
}

void TestPartialTemporaryRecoveryAndBoundedLock(TestContext* test) {
    TemporaryDirectory partial;
    CHECK(test, partial.valid());
    if (!partial.valid()) {
        return;
    }
    const route::ProductionRouteManifestV1 manifest = MakeManifest();
    std::vector<std::byte> encoded;
    CHECK(test, route::EncodeProductionRouteManifestV1(manifest, &encoded) ==
                    route::ProductionRouteManifestErrorV1::kNone);
    const int temporary = ::openat(
        partial.fd(), route::kProductionRouteTemporaryFilenameV1.data(),
        O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
        0600);
    CHECK(test, temporary >= 0);
    if (temporary >= 0 && !encoded.empty()) {
        const std::size_t prefix = encoded.size() / 2U;
        CHECK(test, ::pwrite(temporary, encoded.data(), prefix, 0) ==
                        static_cast<ssize_t>(prefix));
        CHECK(test, ::fsync(temporary) == 0);
        CHECK(test, ::close(temporary) == 0);
        CHECK(test, ::fsync(partial.fd()) == 0);
    }
    const route::ProductionRoutePublishResultV1 recovered =
        route::PublishProductionRouteManifestV1At(
            partial.fd(), manifest);
    CHECK(test, recovered.ok());
    CHECK(test, route::ReadProductionRouteManifestV1At(partial.fd())
                    .authoritative());

    TemporaryDirectory contended;
    CHECK(test, contended.valid());
    if (!contended.valid()) {
        return;
    }
    BlockingRouteHook blocker;
    route::ProductionRouteStoreOptionsV1 blocking_options;
    blocking_options.operation_hook = &BlockingRouteHook::Hook;
    blocking_options.operation_hook_context = &blocker;
    route::ProductionRoutePublishResultV1 first;
    std::thread owner([&]() {
        first = route::PublishProductionRouteManifestV1At(
            contended.fd(), manifest, blocking_options);
    });
    {
        std::unique_lock<std::mutex> lock(blocker.mutex);
        blocker.condition.wait(
            lock, [&blocker]() { return blocker.entered; });
    }
    route::ProductionRouteStoreOptionsV1 bounded_options;
    bounded_options.lock_timeout = std::chrono::milliseconds{1};
    const route::ProductionRoutePublishResultV1 blocked =
        route::PublishProductionRouteManifestV1At(
            contended.fd(), manifest, bounded_options);
    CHECK(test, blocked.error ==
                    route::ProductionRouteStoreErrorV1::kLockFailure);
    {
        std::lock_guard<std::mutex> lock(blocker.mutex);
        blocker.released = true;
    }
    blocker.condition.notify_all();
    owner.join();
    CHECK(test, first.ok());
}

void TestUnsafeAndCorruptFiles(TestContext* test) {
    TemporaryDirectory unsafe;
    CHECK(test, unsafe.valid());
    if (!unsafe.valid()) {
        return;
    }
    CHECK(test, ::chmod(unsafe.path().c_str(), 0755) == 0);
    const route::ProductionRoutePublishResultV1 rejected =
        route::PublishProductionRouteManifestV1At(
            unsafe.fd(), MakeManifest());
    CHECK(test, rejected.error ==
                    route::ProductionRouteStoreErrorV1::kUnsafeDirectory);

    TemporaryDirectory corrupt;
    CHECK(test, corrupt.valid());
    if (!corrupt.valid()) {
        return;
    }
    CHECK(test, route::PublishProductionRouteManifestV1At(
                    corrupt.fd(), MakeManifest()).ok());
    const int writable = ::openat(
        corrupt.fd(), route::kProductionRouteFilenameV1.data(),
        O_WRONLY | O_CLOEXEC | O_NOFOLLOW);
    CHECK(test, writable >= 0);
    if (writable >= 0) {
        const std::byte corrupt_byte{0xffU};
        CHECK(test, ::pwrite(writable, &corrupt_byte, 1U, 40) == 1);
        CHECK(test, ::fsync(writable) == 0);
        CHECK(test, ::close(writable) == 0);
        CHECK(test, ::fsync(corrupt.fd()) == 0);
    }
    const route::ProductionRouteReadResultV1 read =
        route::ReadProductionRouteManifestV1At(corrupt.fd());
    CHECK(test, read.error ==
                    route::ProductionRouteStoreErrorV1::kManifestInvalid);
    CHECK(test, !read.authoritative());
}

}  // namespace

int main() {
    TestContext test;
    TestValidationAndCodec(&test);
    TestClockEpochIdentityHash(&test);
    TestControllerLifecycle(&test);
    TestAggregateRouteBinding(&test);
    TestMinimumActivationGate(&test);
    TestControllerFatalRetryIdentity(&test);
    TestStoreLifecycle(&test);
    TestFaultWindows(&test);
    TestConcurrentIdempotentPublication(&test);
    TestPartialTemporaryRecoveryAndBoundedLock(&test);
    TestUnsafeAndCorruptFiles(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " production route V1 checks failed\n";
        return 1;
    }
    std::cout << "production route V1 checks passed\n";
    return 0;
}
