#include "l2flow/common/sha256.h"
#include "l2flow/ingress/raw_production_runtime.h"
#include "l2flow/ingress/raw_schema.h"
#include "l2flow/ingress/raw_v1.h"
#include "l2flow/ingress/raw_wal_stream_posix.h"

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace ingress = l2flow::ingress;
namespace canonical = l2flow::canonical;
namespace sdk = l2flow::sdk;
namespace mdl = datayes::mdl;

namespace {

struct TestContext final {
    void Expect(
        bool condition,
        std::string_view description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description
                      << '\n';
        }
    }

    int failures = 0;
};

template <std::size_t Size>
std::array<std::byte, Size> Pattern(
    std::uint8_t seed) {
    std::array<std::byte, Size> output{};
    for (std::size_t index = 0U;
         index < output.size();
         ++index) {
        output[index] =
            static_cast<std::byte>(
                static_cast<std::uint8_t>(
                    seed +
                    static_cast<std::uint8_t>(
                        index)));
    }
    return output;
}

std::uint64_t DigestLabel(
    const l2flow::common::Sha256Digest&
        digest) noexcept {
    std::uint64_t output = 0U;
    for (std::size_t index = 0U;
         index < 8U;
         ++index) {
        output =
            (output << 8U) |
            std::to_integer<std::uint64_t>(
                digest[index]);
    }
    return output;
}

class TemporaryDirectory final {
public:
    TemporaryDirectory() {
        std::array<char, 64U> pattern{};
        constexpr char prefix[] =
            "/tmp/l2flow-fresh-runtime-XXXXXX";
        static_assert(
            sizeof(prefix) <=
            std::tuple_size_v<
                decltype(pattern)>);
        std::memcpy(
            pattern.data(), prefix, sizeof(prefix));
        char* const created =
            ::mkdtemp(pattern.data());
        if (created == nullptr) {
            return;
        }
        path_ = created;
        do {
            descriptor_ = ::open(
                path_.c_str(),
                O_RDONLY | O_DIRECTORY |
                    O_NOFOLLOW | O_CLOEXEC |
                    O_NOATIME);
        } while (descriptor_ < 0 &&
                 errno == EINTR);
    }

    ~TemporaryDirectory() {
        if (descriptor_ >= 0) {
            static_cast<void>(
                ::close(descriptor_));
        }
        if (!path_.empty()) {
            std::error_code ignored;
            static_cast<void>(
                std::filesystem::remove_all(
                    path_, ignored));
        }
    }

    TemporaryDirectory(
        const TemporaryDirectory&) = delete;
    TemporaryDirectory& operator=(
        const TemporaryDirectory&) = delete;

    [[nodiscard]] bool ok() const noexcept {
        return descriptor_ >= 0;
    }
    [[nodiscard]] int descriptor()
        const noexcept {
        return descriptor_;
    }
    [[nodiscard]] const std::string& path()
        const noexcept {
        return path_;
    }

private:
    std::string path_;
    int descriptor_ = -1;
};

class RootPathReplacement final {
public:
    explicit RootPathReplacement(std::string original)
        : original_(std::move(original)),
          retained_(original_ + ".retained") {
        struct stat status {};
        if (::lstat(retained_.c_str(), &status) == 0 ||
            errno != ENOENT ||
            ::rename(original_.c_str(), retained_.c_str()) != 0) {
            return;
        }
        if (::mkdir(original_.c_str(), 0700) != 0) {
            static_cast<void>(
                ::rename(retained_.c_str(), original_.c_str()));
            return;
        }
        active_ = true;
    }

    ~RootPathReplacement() {
        if (!active_) {
            return;
        }
        std::error_code ignored;
        static_cast<void>(
            std::filesystem::remove_all(original_, ignored));
        static_cast<void>(
            ::rename(retained_.c_str(), original_.c_str()));
    }

    RootPathReplacement(const RootPathReplacement&) = delete;
    RootPathReplacement& operator=(const RootPathReplacement&) = delete;

    [[nodiscard]] bool ok() const noexcept { return active_; }
    [[nodiscard]] const std::string& retained_path() const noexcept {
        return retained_;
    }

private:
    std::string original_;
    std::string retained_;
    bool active_ = false;
};

ingress::ReserveCoordinatorStateV1
MakeCoordinatorBootstrap(int root_fd) {
    struct stat status {};
    if (::fstat(root_fd, &status) != 0) {
        return {};
    }
    ingress::ReserveCoordinatorStateV1 state{};
    state.header.reserve_state_uuid =
        Pattern<16U>(0x11U);
    state.header.quota_identity_sha256 =
        Pattern<32U>(0x31U);
    state.header.mount_identity_sha256 =
        Pattern<32U>(0x51U);
    state.header.device_id =
        static_cast<std::uint64_t>(
            status.st_dev);
    state.header.declared_releasable_bytes =
        UINT64_C(1) << 20U;
    state.header.allocation_quantum_bytes =
        4096U;
    state.header.declared_inode_reserve_count =
        64U;
    state.header.byte_probe_version = 1U;
    state.header.inode_probe_version = 1U;
    state.header.inode_inventory_sha256 =
        Pattern<32U>(0x71U);
    state.header.safe_stop_catalog_sha256 =
        Pattern<32U>(0x91U);

    ingress::ReserveStateSlotV1 slot{};
    slot.coordinator_state =
        ingress::ReserveCoordinatorPhaseV1::
            kProvisioned;
    slot.generation = 1U;
    slot.reserve_state_uuid =
        state.header.reserve_state_uuid;
    state.slots[0U] = slot;
    state.slots[1U] = slot;
    state.selected_slot = 0U;
    return state;
}

ingress::RawReserveCoordinatorLeaseMarkerV1
MakeCoordinatorMarker(
    const ingress::ReserveCoordinatorStateV1&
        state) {
    ingress::RawReserveCoordinatorLeaseMarkerV1
        marker{};
    marker.coordinator_identity =
        Pattern<16U>(0x05U);
    marker.device_id = state.header.device_id;
    marker.quota_identity_sha256 =
        state.header.quota_identity_sha256;
    marker.mount_identity_sha256 =
        state.header.mount_identity_sha256;
    return marker;
}

class FixedCaptureClock final
    : public ingress::CaptureClock {
public:
    std::uint64_t MonotonicRawNanoseconds()
        override {
        return monotonic_++;
    }
    std::uint64_t RealtimeNanoseconds()
        override {
        return realtime_++;
    }

private:
    std::uint64_t monotonic_ = 100U;
    std::uint64_t realtime_ = 200U;
};

struct SdkCounters final {
    std::uint64_t factory_create = 0U;
    std::uint64_t connect = 0U;
    std::uint64_t manager_release = 0U;
};

class FakeSubscriber final
    : public sdk::SdkSubscriber {
public:
    explicit FakeSubscriber(
        std::shared_ptr<SdkCounters> counters)
        : counters_(std::move(counters)) {}

    void SetServerAddress(
        std::string_view) override {}
    void SetUserName(
        std::string_view) override {}
    void SetHeartbeatInterval(
        std::uint32_t) override {}
    void SetHeartbeatTimeout(
        std::uint32_t) override {}
    void SetMessageEncoding(
        mdl::MDLMessageEncoding) override {}
    void EnableMergeMessage(bool) override {}
    void SetSendMacAuth(bool) override {}
    void EnableServerSelect(bool) override {}
    void AddSubscription(
        const sdk::MessageKey&) override {}
    std::string Connect() override {
        ++counters_->connect;
        return {};
    }
    bool Release(
        std::string*) noexcept override {
        return true;
    }

private:
    std::shared_ptr<SdkCounters> counters_;
};

class FakeManager final : public sdk::SdkManager {
public:
    explicit FakeManager(
        std::shared_ptr<SdkCounters> counters)
        : counters_(std::move(counters)) {}

    void EnableLog(
        std::string_view, bool) override {}
    std::unique_ptr<sdk::SdkSubscriber>
    CreateSubscriber(
        mdl::MessageHandlerBase*,
        bool multithread) override {
        if (multithread) {
            return nullptr;
        }
        return std::make_unique<FakeSubscriber>(
            counters_);
    }
    void Shutdown() override {}
    bool Release(
        std::string*) noexcept override {
        ++counters_->manager_release;
        return true;
    }

private:
    std::shared_ptr<SdkCounters> counters_;
};

class FakeFactory final : public sdk::SdkFactory {
public:
    explicit FakeFactory(
        std::shared_ptr<SdkCounters> counters)
        : counters_(std::move(counters)) {}

    std::unique_ptr<sdk::SdkManager> Create(
        int, int) override {
        ++counters_->factory_create;
        return std::make_unique<FakeManager>(
            counters_);
    }

private:
    std::shared_ptr<SdkCounters> counters_;
};

struct BackendClock final {
    static std::uint64_t Realtime(
        void* context) noexcept {
        auto* const clock =
            static_cast<BackendClock*>(context);
        return ++clock->realtime;
    }
    static std::uint64_t Monotonic(
        void* context) noexcept {
        auto* const clock =
            static_cast<BackendClock*>(context);
        return clock->fail_monotonic
                   ? 0U
                   : ++clock->monotonic;
    }
    static std::uint64_t Stream(
        void* context) noexcept {
        auto* const clock =
            static_cast<BackendClock*>(context);
        return ++clock->stream;
    }

    std::uint64_t realtime =
        UINT64_C(1'721'234'567'000'000'000);
    std::uint64_t monotonic = 1000U;
    std::uint64_t stream = 2000U;
    bool fail_monotonic = false;
};

struct FreshRuntimeFixture final {
    [[nodiscard]] bool Initialize(
        bool fail_backend_monotonic = false) {
        if (!root.ok()) {
            error = "temporary Raw root unavailable";
            return false;
        }
        clocks.fail_monotonic =
            fail_backend_monotonic;

        config.stable =
            ingress::DefaultRawIngressConfig(
                sdk::IngressKind::SzTick);
        constexpr std::string_view endpoint_bytes =
            "{\"schema_version\":1,"
            "\"ingress_kind\":\"sz-tick\","
            "\"name\":\"sz-tick-runtime-test\","
            "\"resolved_server_address\":\"127.0.0.1:12345\","
            "\"message_encoding\":1,"
            "\"merge_message\":false,"
            "\"send_mac_auth\":false,"
            "\"server_select\":false}";
        config.stable.endpoint_contract_sha256 =
            l2flow::common::Sha256Hex(
                l2flow::common::ComputeSha256(
                    endpoint_bytes));
        config.stable.credential_name =
            "mdl-token";
        config.stable.sdk_log_prefix =
            "/tmp/l2flow-sdk";
        config.stable.metrics_textfile_path =
            "/tmp/l2flow-fresh-runtime.prom";
        config.stable.max_message_bytes =
            ingress::kVendorMessageHeadBytes +
            13U;
        config.stable.ring_capacity_bytes =
            4096U;
        config.stable.raw_root = root.path();
        if (ingress::ComputeRawRecordLayoutV1(
                13U, &maximum_record) !=
            ingress::RawV1Error::kNone) {
            error = "maximum Raw record layout failed";
            return false;
        }
        config.stable.segment_target_bytes =
            ingress::kRawV1SegmentHeaderBytes +
            maximum_record.record_size;
        config.stable.segment_max_age_seconds =
            1U;
        config.stable.sync_bytes = 1U;
        config.stable.sparse_index_every_records =
            1U;
        config.stable.sparse_index_every_bytes =
            1U;
        config.stable.reserve_domain_id =
            "raw-runtime-test";
        config.stable.reserve_coordinator_socket =
            "/tmp/l2flow-fresh-runtime.sock";
        config.stable.emergency_reserve_bytes =
            4096U;
        config.stable
                .canonical_clock_source_config =
            "CLOCK_MONOTONIC_RAW+CLOCK_REALTIME";

        config.endpoint =
            sdk::VerifyEndpointContractBytes(
                endpoint_bytes,
                config.stable
                    .endpoint_contract_sha256,
                config.stable.kind,
                &error);
        if (config.endpoint == nullptr) {
            return false;
        }
        config.credential_token = "secret";
        config.connect_generation = 1U;

        const sdk::IngressSpec& spec =
            sdk::GetIngressSpec(
                config.stable.kind);
        registration.key.route.source_stream_id =
            spec.source_stream_id;
        registration.key.route.capture_date =
            20260718U;
        registration.key.stream_day_id =
            Pattern<16U>(0x10U);
        registration.key.recovery_attempt_id =
            Pattern<16U>(0x20U);
        registration.writer_instance =
            Pattern<16U>(0x30U);
        registration.recovery_intent =
            ingress::ReserveRecoveryIntentV1::
                kResumeConnect;
        registration.scaffolding_allocation_cap =
            UINT64_C(1) << 20U;
        registration.safe_stop_template_id = 7U;

        config.recovered.source_stream_id =
            spec.source_stream_id;
        config.recovered.capture_date =
            registration.key.route.capture_date;
        config.recovered.stream_day_id =
            registration.key.stream_day_id;
        config.recovered
                .recovered_next_ingress_sequence =
            1U;
        config.recovered.append = {
            .segment_sequence = 1U,
            .global_wal_pos =
                ingress::kRawV1SegmentHeaderBytes,
            .ingress_sequence = 0U,
            .segment_offset =
                ingress::kRawV1SegmentHeaderBytes,
        };
        config.recovered.durable =
            config.recovered.append;
        config.recovered.writer_instance =
            registration.writer_instance;
        config.recovered
                .current_segment_sequence =
            1U;
        config.recovered
                .clock_epoch_algorithm_version =
            config.stable
                .clock_epoch_algorithm_version;
        config.recovered.clock_epoch_digest =
            Pattern<32U>(0x40U);
        config.recovered.clock_epoch_label =
            DigestLabel(
                config.recovered
                    .clock_epoch_digest);

        segment.source_stream_id =
            spec.source_stream_id;
        segment.capture_date =
            registration.key.route.capture_date;
        segment.stream_day_id =
            registration.key.stream_day_id;
        segment.segment_sequence = 1U;
        segment.segment_base_wal_pos = 0U;
        segment.first_ingress_sequence = 1U;
        segment.created_realtime_ns =
            UINT64_C(1'721'234'567'000'000'000);
        segment.created_monotonic_ns = 900U;
        segment.host_uuid =
            Pattern<16U>(0x50U);
        segment.linux_boot_id =
            Pattern<16U>(0x60U);
        segment.clock_epoch_algorithm =
            config.recovered
                .clock_epoch_algorithm_version;
        segment.clock_epoch_digest =
            config.recovered.clock_epoch_digest;
        segment.clock_epoch_label =
            config.recovered.clock_epoch_label;
        segment.sdk_archive_sha256 =
            Pattern<32U>(0x70U);
        segment.libmdl_api_sha256 =
            Pattern<32U>(0x80U);
        segment.build_manifest_sha256 =
            Pattern<32U>(0x90U);

        l2flow::common::Sha256Digest digest{};
        if (!l2flow::common::ParseSha256Hex(
                config.stable
                    .endpoint_contract_sha256,
                &digest,
                &error)) {
            return false;
        }
        segment.endpoint_contract_sha256 =
            digest;
        if (!l2flow::common::ParseSha256Hex(
                ingress::RawIngressConfigSha256(
                    config.stable),
                &digest,
                &error)) {
            return false;
        }
        segment.config_sha256 = digest;
        if (!l2flow::common::ParseSha256Hex(
                config.stable.raw_schema_sha256,
                &digest,
                &error)) {
            return false;
        }
        segment.raw_schema_sha256 = digest;

        ingress::DurableJournalHeaderV1 journal{};
        journal.capture_date =
            segment.capture_date;
        journal.source_stream_id =
            segment.source_stream_id;
        journal.stream_day_id =
            segment.stream_day_id;
        journal.raw_schema_sha256 =
            segment.raw_schema_sha256;
        journal.created_host_uuid =
            segment.host_uuid;
        journal.created_linux_boot_id =
            segment.linux_boot_id;
        journal.created_clock_epoch_algorithm =
            segment.clock_epoch_algorithm;
        journal.created_clock_epoch_digest =
            segment.clock_epoch_digest;
        journal.created_clock_epoch_label =
            segment.clock_epoch_label;
        if (ingress::EncodeSegmentHeaderV1(
                segment,
                &writer_config
                     .segment_header_wire) !=
                ingress::RawV1Error::kNone ||
            ingress::EncodeDurableJournalHeaderV1(
                journal,
                &writer_config
                     .journal_header_wire) !=
                ingress::RawV1Error::kNone) {
            error = "fresh Raw headers cannot encode";
            return false;
        }
        writer_config.source_stream_id =
            segment.source_stream_id;
        writer_config.capture_date =
            segment.capture_date;
        writer_config.segment_sequence = 1U;
        writer_config.segment_base_wal_pos = 0U;
        writer_config.first_ingress_sequence = 1U;
        writer_config
                .initial_durable_ingress_sequence =
            0U;
        writer_config.writer_instance =
            registration.writer_instance;
        writer_config.initialization_mode =
            ingress::RawWalInitializationMode::
                kFreshJournal;
        writer_config.headers_already_persisted =
            false;

        backend_options.writer_instance =
            registration.writer_instance;
        backend_options
                .segment_preallocation_bytes =
            config.stable.segment_target_bytes;
        backend_options.maximum_manifest_bytes =
            1024U * 1024U;
        backend_options.realtime_now =
            &BackendClock::Realtime;
        backend_options.realtime_clock_context =
            &clocks;
        backend_options.monotonic_now =
            &BackendClock::Monotonic;
        backend_options.monotonic_clock_context =
            &clocks;

        artifact_options
                .expected_raw_schema_sha256 =
            segment.raw_schema_sha256;
        artifact_options.sample_record_interval =
            config.stable
                .sparse_index_every_records;
        artifact_options
                .sample_raw_bytes_interval =
            config.stable
                .sparse_index_every_bytes;
        artifact_options.maximum_segment_bytes =
            config.stable.segment_target_bytes;

        stream_limits.segment_target_bytes =
            config.stable.segment_target_bytes;
        stream_limits.segment_max_age_ns =
            UINT64_C(1'000'000'000);
        stream_limits.maximum_record_bytes =
            maximum_record.record_size;
        stream_limits.monotonic_now =
            &BackendClock::Stream;
        stream_limits.monotonic_clock_context =
            &clocks;

        const auto bootstrap =
            MakeCoordinatorBootstrap(
                root.descriptor());
        ingress::RawReserveCoordinatorErrorV1
            coordinator_failure =
                ingress::
                    RawReserveCoordinatorErrorV1::
                        kNone;
        coordinator =
            ingress::
                PublishFreshRawReserveRegistryCoordinatorAtV1(
                    root.descriptor(),
                    MakeCoordinatorMarker(
                        bootstrap),
                    bootstrap,
                    &coordinator_failure,
                    &error);
        return coordinator != nullptr &&
               coordinator->RegisterFreshScaffolding(
                   registration,
                   &error) ==
                   ingress::
                       RawReserveCoordinatorErrorV1::
                           kNone;
    }

    TemporaryDirectory root;
    ingress::RawIngressAppConfigV1 config{};
    ingress::RawReserveFreshScaffoldingV1
        registration{};
    ingress::SegmentHeaderV1 segment{};
    ingress::RawWalWriterConfig writer_config{};
    ingress::RawPosixWalStreamBackendOptionsV1
        backend_options{};
    ingress::RawSegmentArtifactOptionsV1
        artifact_options{};
    ingress::RawWalStreamLimitsV1 stream_limits{};
    ingress::RawRecordLayoutV1 maximum_record{};
    BackendClock clocks{};
    std::shared_ptr<
        ingress::RawReserveRegistryCoordinatorV1>
        coordinator;
    std::string error;
};

void TestFreshRegisteredRuntime(
    TestContext* test) {
    FreshRuntimeFixture fixture;
    test->Expect(
        fixture.Initialize(),
        "fresh production fixture is valid and durably registered");
    if (fixture.coordinator == nullptr) {
        return;
    }
    auto counters =
        std::make_shared<SdkCounters>();
    auto factory =
        std::make_shared<FakeFactory>(counters);

    canonical::SourceFrontierPageV1 source_frontier{};
    canonical::SourceFrontierConfigV1 frontier_config{};
    frontier_config.source_stream_id =
        fixture.config.recovered.source_stream_id;
    frontier_config.capture_date = fixture.config.recovered.capture_date;
    frontier_config.stream_day_id = fixture.config.recovered.stream_day_id;
    frontier_config.clock_epoch.algorithm =
        fixture.config.recovered.clock_epoch_algorithm_version;
    frontier_config.clock_epoch.digest =
        fixture.config.recovered.clock_epoch_digest;
    frontier_config.clock_epoch.label =
        fixture.config.recovered.clock_epoch_label;
    frontier_config.writer_instance = fixture.registration.writer_instance;
    frontier_config.generation = 37U;
    frontier_config.initial_global_wal_pos =
        ingress::kRawV1SegmentHeaderBytes;
    frontier_config.initial_state = canonical::SourceStateV1::kHealthy;
    test->Expect(
        canonical::InitializeSourceFrontierPageV1(
            frontier_config, &source_frontier) ==
            canonical::SourceFrontierErrorV1::kNone,
        "fresh production SourceFrontier fixture initializes");
    ingress::RawIngressAppOptionsV1 app_options{};
    app_options.source_frontier = &source_frontier;
    app_options.frontier_writer_instance =
        fixture.registration.writer_instance;
    app_options.frontier_generation = frontier_config.generation;

    ingress::RawProductionRuntimeBuildResultV1
        result =
            ingress::
                RawExistingRouteProductionRuntimeFactoryV1::
                    ActivateFreshRegistered(
                        fixture.root.path(),
                        "sz-tick",
                        fixture.registration,
                        *fixture.coordinator,
                        fixture.writer_config,
                        fixture.backend_options,
                        fixture.artifact_options,
                        fixture.stream_limits,
                        fixture.config,
                        factory,
                        std::make_unique<
                            FixedCaptureClock>(),
                        {},
                        app_options,
                        nullptr,
                        &fixture.error);
    test->Expect(
        result.ok() &&
            !result.requires_fail_stop() &&
            result.fresh_route_failure ==
                ingress::
                    RawFreshRoutePosixFailureV1::
                        kNone &&
            result.fresh_active_failure ==
                ingress::
                    RawFreshActivePosixStreamFailureV1::
                        kNone &&
            result.fresh_active_publication_state ==
                ingress::
                    RawFreshActivePublicationStateV1::
                        kActiveBound &&
            result.fresh_init_publication_attempted,
        "fresh factory returns one complete runtime only after exact ACTIVE publication");
    if (!result.ok()) {
        std::cerr
            << "fresh runtime construction: "
            << fixture.error << '\n';
        return;
    }

    const auto& binding = result.runtime->capture_binding();
    test->Expect(
        binding.source_slot == 3U &&
            binding.ingress_kind == sdk::IngressKind::SzTick &&
            binding.source_stream_id ==
                fixture.config.recovered.source_stream_id &&
            binding.capture_date == fixture.config.recovered.capture_date &&
            binding.stream_day_id == fixture.config.recovered.stream_day_id &&
            binding.writer_instance == fixture.registration.writer_instance &&
            binding.source_generation == frontier_config.generation &&
            binding.source_frontier == &source_frontier,
        "fresh Raw runtime exposes its exact immutable production binding");

    test->Expect(
        result.runtime->HasFreshPipelineLiveTail(),
        "fresh runtime retains an independent source-order pipeline tail");
    std::unique_ptr<ingress::RawLiveTail> pipeline_tail =
        result.runtime->TakeFreshPipelineLiveTail();
    test->Expect(
        pipeline_tail != nullptr &&
            !result.runtime->HasFreshPipelineLiveTail() &&
            result.runtime->TakeFreshPipelineLiveTail() == nullptr,
        "fresh pipeline tail is transferred exactly once");
    if (pipeline_tail != nullptr) {
        const ingress::RawLiveControlSampleV1 sample =
            pipeline_tail->SampleControlFresh();
        test->Expect(
            sample.ok() &&
                pipeline_tail->source_stream_id() ==
                    fixture.registration.key.route.source_stream_id &&
                pipeline_tail->capture_date() ==
                    fixture.registration.key.route.capture_date &&
                pipeline_tail->writer_instance() ==
                    fixture.registration.writer_instance &&
                pipeline_tail->next_ingress_sequence() == 1U,
            "transferred pipeline tail retains its independent authenticated POSIX source");
    }
    pipeline_tail.reset();

    ingress::RawReserveCoordinatorErrorV1
        action_error =
            ingress::RawReserveCoordinatorErrorV1::
                kNone;
    auto active =
        fixture.coordinator
            ->AcquireActionForExistingRoute(
                fixture.registration.key,
                ingress::
                    ReserveRegistryStatusV1::
                        kActive,
                "sz-tick",
                &action_error,
                &fixture.error);
    test->Expect(
        active != nullptr,
        "runtime construction retains the exact durable ACTIVE route");
    active.reset();

    test->Expect(
        result.runtime->Initialize(
            &fixture.error) &&
            counters->factory_create == 1U &&
            counters->connect == 1U,
        "capture/live-tail workers start before the real runtime calls SDK Connect");
    const ingress::RawProductionReadinessSampleV1
        readiness =
            result.runtime->SampleReadiness(
                UINT64_C(10'000));
    test->Expect(
        readiness.ok() &&
            readiness.control_generation != 0U &&
            readiness.sampled_control.writer_instance ==
                fixture.registration.writer_instance &&
            readiness.sampled_control.stream_day_id ==
                fixture.registration.key.stream_day_id &&
            readiness.gate.connect_generation ==
                fixture.config.connect_generation &&
            !readiness.gate.ready,
        "production runtime returns one fresh coherent control/readiness sample without caching READY");
    test->Expect(
        result.runtime->Stop(
            &fixture.error) &&
            counters->manager_release == 1U,
        "empty fresh runtime completes sealed evidence, clean-stop certificate, and SDK release");
    auto removed =
        fixture.coordinator
            ->AcquireActionForExistingRoute(
                fixture.registration.key,
                ingress::
                    ReserveRegistryStatusV1::
                        kActive,
                "sz-tick",
                &action_error,
                nullptr);
    test->Expect(
        removed == nullptr,
        "clean stop consumes the sink-owned lease gate and unregisters ACTIVE");
}

void TestFreshFailureRequiresFailStop(
    TestContext* test) {
    FreshRuntimeFixture fixture;
    test->Expect(
        fixture.Initialize(true),
        "post-INIT failure fixture is durably registered");
    if (fixture.coordinator == nullptr) {
        return;
    }
    auto counters =
        std::make_shared<SdkCounters>();
    ingress::RawProductionRuntimeBuildResultV1
        result =
            ingress::
                RawExistingRouteProductionRuntimeFactoryV1::
                    ActivateFreshRegistered(
                        fixture.root.path(),
                        "sz-tick",
                        fixture.registration,
                        *fixture.coordinator,
                        fixture.writer_config,
                        fixture.backend_options,
                        fixture.artifact_options,
                        fixture.stream_limits,
                        fixture.config,
                        std::make_shared<FakeFactory>(
                            counters),
                        std::make_unique<
                            FixedCaptureClock>(),
                        {},
                        {},
                        nullptr,
                        &fixture.error);
    test->Expect(
        !result.ok() &&
            result.failure ==
                ingress::
                    RawProductionRuntimeFailureV1::
                        kFreshRouteActivation &&
            result.fresh_route_failure ==
                ingress::
                    RawFreshRoutePosixFailureV1::
                        kActiveStream &&
            result.fresh_init_publication_attempted &&
            result.fresh_active_publication_state ==
                ingress::
                    RawFreshActivePublicationStateV1::
                        kNotPublished &&
            result.requires_fail_stop() &&
            counters->factory_create == 0U,
        "failure after durable INIT is explicitly non-retryable even before ACTIVE publication");
}

void TestFreshRetainedRootSurvivesPathReplacement(
    TestContext* test) {
    FreshRuntimeFixture fixture;
    test->Expect(
        fixture.Initialize(),
        "retained-root fixture is durably registered");
    if (fixture.coordinator == nullptr) {
        return;
    }

    RootPathReplacement replacement(fixture.root.path());
    test->Expect(
        replacement.ok(),
        "Raw root pathname is renamed and replaced after preflight");
    if (!replacement.ok()) {
        return;
    }

    int replacement_fd = -1;
    do {
        replacement_fd = ::open(
            fixture.root.path().c_str(),
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    } while (replacement_fd < 0 && errno == EINTR);
    test->Expect(
        replacement_fd >= 0,
        "replacement Raw root can be opened independently");
    if (replacement_fd < 0) {
        return;
    }

    auto rejected = ingress::
        RawExistingRouteProductionRuntimeFactoryV1::
            ActivateFreshRegisteredAt(
                replacement_fd,
                fixture.root.path(),
                "sz-tick",
                fixture.registration,
                *fixture.coordinator,
                fixture.writer_config,
                fixture.backend_options,
                fixture.artifact_options,
                fixture.stream_limits,
                fixture.config,
                std::make_shared<FakeFactory>(
                    std::make_shared<SdkCounters>()),
                std::make_unique<FixedCaptureClock>(),
                {},
                {},
                nullptr,
                &fixture.error);
    static_cast<void>(::close(replacement_fd));
    test->Expect(
        !rejected.ok() &&
            rejected.failure == ingress::
                RawProductionRuntimeFailureV1::kInvalidInput &&
            !rejected.fresh_init_publication_attempted &&
            !rejected.requires_fail_stop(),
        "retained-root activation rejects a different root inode before mutation");

    auto counters = std::make_shared<SdkCounters>();
    auto result = ingress::
        RawExistingRouteProductionRuntimeFactoryV1::
            ActivateFreshRegisteredAt(
                fixture.root.descriptor(),
                fixture.root.path(),
                "sz-tick",
                fixture.registration,
                *fixture.coordinator,
                fixture.writer_config,
                fixture.backend_options,
                fixture.artifact_options,
                fixture.stream_limits,
                fixture.config,
                std::make_shared<FakeFactory>(counters),
                std::make_unique<FixedCaptureClock>(),
                {},
                {},
                nullptr,
                &fixture.error);
    test->Expect(
        result.ok(),
        "retained-root activation succeeds after configured pathname replacement");
    if (!result.ok()) {
        std::cerr << "retained-root activation: "
                  << fixture.error << '\n';
        return;
    }

    const std::filesystem::path relative =
        "capture_date=20260718/stream=2002-sz-tick/segment-00000001.raw";
    test->Expect(
        std::filesystem::exists(
            std::filesystem::path(replacement.retained_path()) / relative) &&
            !std::filesystem::exists(
                std::filesystem::path(fixture.root.path()) / relative),
        "all fresh Raw artifacts are created below the retained inode, not the replacement pathname");

    std::unique_ptr<ingress::RawLiveTail> pipeline_tail =
        result.runtime->TakeFreshPipelineLiveTail();
    pipeline_tail.reset();
    test->Expect(
        result.runtime->Initialize(&fixture.error) &&
            result.runtime->Stop(&fixture.error) &&
            counters->connect == 1U &&
            counters->manager_release == 1U,
        "retained-root runtime initializes and clean-stops without reopening the replaced pathname");
}

void TestFreshPreflightRejectsBeforeMutation(
    TestContext* test) {
    FreshRuntimeFixture fixture;
    test->Expect(
        fixture.Initialize(),
        "preflight rejection fixture is durably registered");
    if (fixture.coordinator == nullptr) {
        return;
    }
    ingress::RawReserveFreshScaffoldingV1 wrong_cap =
        fixture.registration;
    ++wrong_cap.scaffolding_allocation_cap;
    auto cap_result = ingress::
        RawExistingRouteProductionRuntimeFactoryV1::
            ActivateFreshRegisteredAt(
                fixture.root.descriptor(),
                fixture.root.path(),
                "sz-tick",
                wrong_cap,
                *fixture.coordinator,
                fixture.writer_config,
                fixture.backend_options,
                fixture.artifact_options,
                fixture.stream_limits,
                fixture.config,
                std::make_shared<FakeFactory>(
                    std::make_shared<SdkCounters>()),
                std::make_unique<FixedCaptureClock>(),
                {},
                {},
                nullptr,
                &fixture.error);
    ingress::RawReserveFreshScaffoldingV1 wrong_template =
        fixture.registration;
    ++wrong_template.safe_stop_template_id;
    auto template_result = ingress::
        RawExistingRouteProductionRuntimeFactoryV1::
            ActivateFreshRegisteredAt(
                fixture.root.descriptor(),
                fixture.root.path(),
                "sz-tick",
                wrong_template,
                *fixture.coordinator,
                fixture.writer_config,
                fixture.backend_options,
                fixture.artifact_options,
                fixture.stream_limits,
                fixture.config,
                std::make_shared<FakeFactory>(
                    std::make_shared<SdkCounters>()),
                std::make_unique<FixedCaptureClock>(),
                {},
                {},
                nullptr,
                &fixture.error);
    test->Expect(
        !cap_result.ok() &&
            cap_result.failure == ingress::
                RawProductionRuntimeFailureV1::kFreshRouteActivation &&
            cap_result.fresh_route_failure == ingress::
                RawFreshRoutePosixFailureV1::
                    kScaffoldingAuthorization &&
            !cap_result.fresh_init_publication_attempted &&
            !cap_result.requires_fail_stop(),
        "fresh activation rejects a registration whose allocation cap differs from the durable SCAFFOLDING entry");
    test->Expect(
        !template_result.ok() &&
            template_result.failure == ingress::
                RawProductionRuntimeFailureV1::kFreshRouteActivation &&
            template_result.fresh_route_failure == ingress::
                RawFreshRoutePosixFailureV1::
                    kScaffoldingAuthorization &&
            !template_result.fresh_init_publication_attempted &&
            !template_result.requires_fail_stop(),
        "fresh activation rejects a registration whose safe-stop template differs from the durable SCAFFOLDING entry");
    ingress::RawIngressAppConfigV1 foreign =
        fixture.config;
    foreign.recovered.writer_instance[0U] ^=
        std::byte{0x7fU};
    auto counters =
        std::make_shared<SdkCounters>();
    auto result =
        ingress::
            RawExistingRouteProductionRuntimeFactoryV1::
                ActivateFreshRegistered(
                    fixture.root.path(),
                    "sz-tick",
                    fixture.registration,
                    *fixture.coordinator,
                    fixture.writer_config,
                    fixture.backend_options,
                    fixture.artifact_options,
                    fixture.stream_limits,
                    std::move(foreign),
                    std::make_shared<FakeFactory>(
                        counters),
                    std::make_unique<
                        FixedCaptureClock>(),
                    {},
                    {},
                    nullptr,
                    &fixture.error);
    ingress::RawReserveCoordinatorErrorV1
        action_error =
            ingress::RawReserveCoordinatorErrorV1::
                kNone;
    auto scaffolding =
        fixture.coordinator->AcquireAction(
            fixture.registration.key,
            ingress::ReserveRegistryStatusV1::
                kScaffolding,
            &action_error,
            nullptr);
    ingress::RawIngressAppOptionsV1
        invalid_options{};
    invalid_options.emergency_writer_pause_timeout =
        std::chrono::milliseconds::zero();
    auto pause_counters =
        std::make_shared<SdkCounters>();
    auto pause_result =
        ingress::
            RawExistingRouteProductionRuntimeFactoryV1::
                ActivateFreshRegistered(
                    fixture.root.path(),
                    "sz-tick",
                    fixture.registration,
                    *fixture.coordinator,
                    fixture.writer_config,
                    fixture.backend_options,
                    fixture.artifact_options,
                    fixture.stream_limits,
                    fixture.config,
                    std::make_shared<FakeFactory>(
                        pause_counters),
                    std::make_unique<
                        FixedCaptureClock>(),
                    {},
                    invalid_options,
                    nullptr,
                    &fixture.error);
    test->Expect(
        !result.ok() &&
            result.failure ==
                ingress::
                    RawProductionRuntimeFailureV1::
                        kInvalidInput &&
            !result.fresh_init_publication_attempted &&
            !result.requires_fail_stop() &&
            scaffolding != nullptr &&
            counters->factory_create == 0U,
        "foreign runtime identity is rejected while the route remains retryable SCAFFOLDING");
    test->Expect(
        !pause_result.ok() &&
            pause_result.failure ==
                ingress::
                    RawProductionRuntimeFailureV1::
                        kInvalidInput &&
            !pause_result
                 .fresh_init_publication_attempted &&
            !pause_result.requires_fail_stop() &&
            scaffolding != nullptr &&
            scaffolding->ValidateLatest(nullptr) &&
            pause_counters->factory_create == 0U,
        "zero emergency pause timeout is rejected before any fresh namespace or registry mutation");
}

}  // namespace

int main() {
    TestContext test;
    static_assert(
        !std::is_default_constructible_v<
            ingress::
                RawReserveActiveActivationReceiptV1>);
    static_assert(
        !std::is_copy_constructible_v<
            ingress::
                RawReserveActiveActivationReceiptV1>);
    static_assert(
        !std::is_move_constructible_v<
            ingress::
                RawReserveActiveActivationReceiptV1>);
    static_assert(
        !std::is_constructible_v<
            ingress::
                RawReserveActiveActivationReceiptV1,
            ingress::RawReserveRegistryEntryKeyV1,
            l2flow::common::Identity128,
            ingress::
                RawReserveGenerationActionTokenV1,
            ingress::
                RawReserveMutationTargetAnchorV1>);
    static_assert(
        std::is_base_of_v<
            ingress::RawWalSink,
            ingress::
                RawRecoveredClosedPosixStreamV1>);
    static_assert(
        std::is_base_of_v<
            ingress::
                RawReserveMutationTargetProviderV1,
            ingress::
                RawRecoveredClosedPosixStreamV1>);

    std::unique_ptr<
        ingress::RawRecoveredClosedPosixStreamV1>
        absent_recovering_stream;
    std::string promotion_error;
    auto absent_promotion =
        ingress::
            PromoteRecoveredClosedRawPosixStreamToActiveV1(
                std::move(absent_recovering_stream),
                nullptr,
                &promotion_error);
    if (absent_promotion != nullptr ||
        promotion_error.empty()) {
        std::cerr
            << "null activation receipt bypassed Raw ACTIVE promotion\n";
        return 1;
    }

    auto assessment =
        ingress::
            RawExistingRouteProductionRuntimeFactoryV1::
                Assess();
    if (assessment.ok() ||
        assessment.runtime != nullptr ||
        assessment.failure !=
            ingress::RawProductionRuntimeFailureV1::
                kNone ||
        assessment.blocker !=
            ingress::RawProductionRuntimeBlockerV1::
                kExplicitActivationInputsRequired) {
        std::cerr
            << "production assessment crossed a missing typed report barrier\n";
        return 1;
    }
    TestFreshRegisteredRuntime(&test);
    TestFreshFailureRequiresFailStop(&test);
    TestFreshRetainedRootSurvivesPathReplacement(
        &test);
    TestFreshPreflightRejectsBeforeMutation(
        &test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Phase 2 Raw production runtime assertion(s) failed\n";
        return 1;
    }
    std::cout
        << "Phase 2 Raw production runtime gate tests passed\n";
    return 0;
}
