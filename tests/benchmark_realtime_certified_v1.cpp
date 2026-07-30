#include "l2flow/ipc/realtime_certified_reader_v1.h"
#include "l2flow/ipc/realtime_certified_service_v1.h"
#include "l2flow/ipc/realtime_wire_projection_v2.h"
#include "l2flow/market/daily_instrument_catalog_v2.h"
#include "l2flow/market/instrument_runtime_state_v2.h"
#include "l2flow/market/realtime_latest_read_model_v1.h"
#include "l2flow/runtime/realtime_pipeline_v1.h"
#include "l2flow/sdk/market_message_catalog_v1.h"
#include "l2flow/sdk/vendor_head_view.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <time.h>
#include <unistd.h>

namespace common = l2flow::common;
namespace ipc = l2flow::ipc;
namespace market = l2flow::market;
namespace mdl = datayes::mdl;
namespace runtime = l2flow::runtime;
namespace sdk = l2flow::sdk;

namespace {

using namespace std::chrono_literals;

constexpr std::size_t kWarmupSamples = 256U;
constexpr std::size_t kMeasuredSamples = 2'048U;
constexpr std::size_t kMaximumIngressRecords = 16'384U;
constexpr std::size_t kGapWarmupTrials = 1U;
constexpr std::size_t kGapMeasuredTrials = 5U;
constexpr std::array<std::uint64_t, 4U> kGapSuffixes{
    1U, 32U, 256U, 1'024U};

[[nodiscard]] std::uint64_t MonotonicNowNs() noexcept {
    timespec value{};
    if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0 ||
        value.tv_sec < 0 || value.tv_nsec < 0 ||
        value.tv_nsec >= 1'000'000'000L) {
        return 0U;
    }
    const std::uint64_t seconds =
        static_cast<std::uint64_t>(value.tv_sec);
    constexpr std::uint64_t kNanosecondsPerSecond =
        1'000'000'000ULL;
    if (seconds >
        (std::numeric_limits<std::uint64_t>::max() -
         static_cast<std::uint64_t>(value.tv_nsec)) /
            kNanosecondsPerSecond) {
        return 0U;
    }
    return seconds * kNanosecondsPerSecond +
           static_cast<std::uint64_t>(value.tv_nsec);
}

template <typename Predicate>
[[nodiscard]] bool WaitUntil(
    Predicate predicate,
    std::chrono::seconds timeout = 5s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::yield();
    }
    return predicate();
}

[[nodiscard]] bool Fail(
    std::string_view stage,
    std::string_view detail) {
    std::cerr << "CERTIFIED_BENCH_ERROR"
              << " stage=" << stage
              << " detail=" << detail << '\n';
    return false;
}

class WireWriter final {
public:
    explicit WireWriter(std::size_t bytes)
        : bytes_(bytes, std::byte{0}) {}

    void StoreU16(std::size_t offset, std::uint16_t value) {
        StoreUnsigned(offset, value);
    }
    void StoreU32(std::size_t offset, std::uint32_t value) {
        StoreUnsigned(offset, value);
    }
    void StoreU64(std::size_t offset, std::uint64_t value) {
        StoreUnsigned(offset, value);
    }
    void StoreString(std::size_t descriptor, std::string_view value) {
        const std::size_t start = bytes_.size();
        StoreU16(
            descriptor,
            static_cast<std::uint16_t>(value.size()));
        StoreU32(
            descriptor + sizeof(std::uint16_t),
            static_cast<std::uint32_t>(start - descriptor));
        const auto encoded =
            std::as_bytes(std::span(value.data(), value.size()));
        bytes_.insert(bytes_.end(), encoded.begin(), encoded.end());
    }
    [[nodiscard]] std::vector<std::byte> Take() && {
        return std::move(bytes_);
    }

private:
    template <typename Unsigned>
    void StoreUnsigned(std::size_t offset, Unsigned value) {
        static_assert(std::is_unsigned_v<Unsigned>);
        for (std::size_t index = 0U; index < sizeof(Unsigned);
             ++index) {
            bytes_.at(offset + index) = static_cast<std::byte>(
                (value >>
                 static_cast<unsigned int>(index * 8U)) &
                static_cast<Unsigned>(0xffU));
        }
    }

    std::vector<std::byte> bytes_;
};

[[nodiscard]] std::vector<std::byte> ShanghaiTradeBody(
    std::uint64_t native_sequence,
    std::uint64_t quantity) {
    WireWriter writer(70U);
    writer.StoreU64(0U, native_sequence);
    writer.StoreU32(8U, 7U);
    writer.StoreU32(18U, 93'000'125U);
    writer.StoreU64(28U, 11'001U);
    writer.StoreU64(36U, 22'002U);
    writer.StoreU32(44U, 12'345U);
    writer.StoreU64(48U, quantity);
    writer.StoreU64(56U, 506'145U);
    writer.StoreString(12U, "600007");
    writer.StoreString(22U, "T");
    writer.StoreString(64U, "B");
    return std::move(writer).Take();
}

class FakeMessage final : public mdl::MDLMessage {
public:
    explicit FakeMessage(std::vector<std::byte> body)
        : body_(std::move(body)) {
        std::memset(&head_, 0, sizeof(head_));
        head_.HeadSize =
            static_cast<std::uint8_t>(sdk::kVendorHeadBytes);
        head_.MessageSize = static_cast<std::uint32_t>(
            sdk::kVendorHeadBytes + body_.size());
        head_.MessageEncoding =
            static_cast<std::uint8_t>(mdl::MDLEID_BINARY);
        head_.ServiceID = 4U;
        head_.ServiceVersion = 101U;
        head_.MessageID = 24U;
        head_.LocalTime.m_Value = 93'000'000U;
        head_.SequenceID = 99U;
    }

    void AddRef() override {}
    int ReleaseRef() override { return 1; }
    mdl::MDLMessageHead* GetHead() const override {
        return const_cast<mdl::MDLMessageHead*>(&head_);
    }
    char* GetBody() const override {
        return reinterpret_cast<char*>(
            const_cast<std::byte*>(body_.data()));
    }
    mdl::MDLMessage* _Copy() const override { return nullptr; }

private:
    mdl::MDLMessageHead head_{};
    std::vector<std::byte> body_;
};

struct TimedFastPublication final {
    std::atomic<std::uint64_t> recv_monotonic_ns{0U};
    std::atomic<std::uint64_t> visible_monotonic_ns{0U};
    std::atomic<std::int64_t> native_sequence{0};
    std::atomic<std::uint8_t> ready{0U};
};

struct FastPublicationTiming final {
    std::uint64_t recv_monotonic_ns = 0U;
    std::uint64_t visible_monotonic_ns = 0U;
    std::int64_t native_sequence = 0;
};

// This is the instrumented first sink in both compositions. In the enabled
// composition RealtimeCertifiedMarketServiceV1 must call it before doing any
// certification handoff, so its timestamp is the FAST-first publication
// boundary rather than the later CERTIFIED boundary.
class TimedFastSink final
    : public market::RealtimeAppliedRecordSinkV1 {
public:
    explicit TimedFastSink(std::size_t maximum_ingress)
        : maximum_ingress_(maximum_ingress),
          publications_(
              std::make_unique<TimedFastPublication[]>(
                  maximum_ingress + 1U)) {}

    [[nodiscard]] bool PublishApplied(
        std::size_t ordinal,
        const market::RealtimeHistoryRecordV1& record)
        noexcept override {
        if (!market::IsTickEventKindV1(record.kind())) {
            return true;
        }
        const std::uint64_t ingress = record.ingress_sequence();
        if (ingress == 0U || ingress > maximum_ingress_) {
            return false;
        }
        ipc::RealtimeWireTickPayloadV2 payload{};
        if (!ipc::ProjectRealtimeWireTickPayloadV2(
                record, ordinal, &payload)) {
            return false;
        }
        const std::uint64_t recv =
            record.recv_monotonic_ns() > 0
                ? static_cast<std::uint64_t>(
                      record.recv_monotonic_ns())
                : 0U;
        if (recv == 0U) {
            return false;
        }
        latest_native_sequence_.store(
            payload.native_event_sequence,
            std::memory_order_relaxed);
        latest_ingress_sequence_.store(
            ingress, std::memory_order_release);
        const std::uint64_t visible = MonotonicNowNs();
        if (visible < recv) {
            return false;
        }
        TimedFastPublication& publication =
            publications_[static_cast<std::size_t>(ingress)];
        publication.recv_monotonic_ns.store(
            recv, std::memory_order_relaxed);
        publication.visible_monotonic_ns.store(
            visible, std::memory_order_relaxed);
        publication.native_sequence.store(
            payload.native_event_sequence,
            std::memory_order_relaxed);
        publication.ready.store(1U, std::memory_order_release);
        return true;
    }

    void MarkCoverageLost() noexcept override {
        coverage_lost_.store(true, std::memory_order_release);
    }

    [[nodiscard]] bool ReadTiming(
        std::uint64_t ingress,
        FastPublicationTiming* output) const noexcept {
        if (output == nullptr || ingress == 0U ||
            ingress > maximum_ingress_) {
            return false;
        }
        const TimedFastPublication& publication =
            publications_[static_cast<std::size_t>(ingress)];
        if (publication.ready.load(std::memory_order_acquire) == 0U) {
            return false;
        }
        output->recv_monotonic_ns =
            publication.recv_monotonic_ns.load(
                std::memory_order_relaxed);
        output->visible_monotonic_ns =
            publication.visible_monotonic_ns.load(
                std::memory_order_relaxed);
        output->native_sequence =
            publication.native_sequence.load(
                std::memory_order_relaxed);
        return output->recv_monotonic_ns != 0U &&
               output->visible_monotonic_ns >=
                   output->recv_monotonic_ns;
    }

    [[nodiscard]] bool ReadLatest(
        std::uint64_t* ingress,
        std::int64_t* native_sequence) const noexcept {
        if (ingress == nullptr || native_sequence == nullptr) {
            return false;
        }
        const std::uint64_t observed =
            latest_ingress_sequence_.load(std::memory_order_acquire);
        if (observed == 0U) {
            return false;
        }
        *ingress = observed;
        *native_sequence =
            latest_native_sequence_.load(std::memory_order_relaxed);
        return true;
    }

    [[nodiscard]] bool coverage_lost() const noexcept {
        return coverage_lost_.load(std::memory_order_acquire);
    }

private:
    const std::size_t maximum_ingress_;
    std::unique_ptr<TimedFastPublication[]> publications_;
    std::atomic<std::uint64_t> latest_ingress_sequence_{0U};
    std::atomic<std::int64_t> latest_native_sequence_{0};
    std::atomic<bool> coverage_lost_{false};
};

struct Fixture final {
    std::shared_ptr<const market::DailyInstrumentCatalogV2> catalog;
    std::unique_ptr<market::InstrumentRuntimeStateV2> runtime_state;
};

[[nodiscard]] bool BuildFixture(Fixture* output) {
    if (output == nullptr) {
        return false;
    }
    market::DailyInstrumentSourceEntryV2 source{};
    source.key.market = market::MarketV1::kShanghai;
    constexpr std::string_view security_id = "600007";
    const auto bytes =
        std::as_bytes(
            std::span(security_id.data(), security_id.size()));
    source.key.security_id.assign(bytes.begin(), bytes.end());
    source.metadata.quantity_unit =
        market::QuantityUnitV1::kShare;
    source.metadata.security_type =
        market::SecurityTypeV1::kEquity;
    source.metadata.asset_scope =
        market::AssetScopeV1::kDocumentedCore;

    market::DailyInstrumentCatalogConfigV2 config{};
    config.trade_date = 20260730U;
    config.catalog_version = 1U;
    config.session_epoch = 1U;
    config.market_scope = market::kDailyCatalogMainlandScopeV2;
    config.coverage_complete = true;
    std::unique_ptr<market::DailyInstrumentCatalogV2> catalog;
    if (market::DailyInstrumentCatalogV2::Create(
            config, std::span(&source, 1U), &catalog) !=
            market::DailyInstrumentCatalogCreateErrorV2::kNone ||
        catalog == nullptr) {
        return false;
    }
    output->catalog =
        std::shared_ptr<const market::DailyInstrumentCatalogV2>(
            std::move(catalog));
    return market::InstrumentRuntimeStateV2::Create(
               *output->catalog, &output->runtime_state) ==
               market::InstrumentRuntimeStateErrorV2::kNone;
}

[[nodiscard]] std::filesystem::path CreateTemporaryDirectory() {
    const std::uint64_t clock = MonotonicNowNs();
    const std::filesystem::path base =
        std::filesystem::temp_directory_path();
    for (std::uint32_t attempt = 0U; attempt < 100U; ++attempt) {
        const std::filesystem::path candidate =
            base /
            ("l2flow-certified-benchmark-" +
             std::to_string(static_cast<long long>(::getpid())) +
             "-" + std::to_string(clock) + "-" +
             std::to_string(attempt));
        std::error_code error;
        if (std::filesystem::create_directory(candidate, error)) {
            return candidate;
        }
    }
    return {};
}

class BenchmarkHarness final {
public:
    BenchmarkHarness(const BenchmarkHarness&) = delete;
    BenchmarkHarness& operator=(const BenchmarkHarness&) = delete;
    ~BenchmarkHarness() {
        Stop();
        reader_.reset();
        service_.reset();
        pipeline_.reset();
        if (!temporary_directory_.empty()) {
            std::error_code error;
            static_cast<void>(
                std::filesystem::remove(
                    temporary_directory_, error));
        }
    }

    [[nodiscard]] static bool Create(
        bool recovery_enabled,
        std::unique_ptr<BenchmarkHarness>* output) {
        if (output == nullptr) {
            return false;
        }
        output->reset();
        auto result = std::unique_ptr<BenchmarkHarness>(
            new BenchmarkHarness(recovery_enabled));
        if (!result->Initialize()) {
            return false;
        }
        *output = std::move(result);
        return true;
    }

    [[nodiscard]] runtime::RealtimePipelineV1* pipeline() const noexcept {
        return pipeline_.get();
    }
    [[nodiscard]] const std::shared_ptr<TimedFastSink>& fast() const
        noexcept {
        return fast_;
    }
    [[nodiscard]] const std::shared_ptr<
        ipc::RealtimeCertifiedMarketServiceV1>&
    service() const noexcept {
        return service_;
    }
    [[nodiscard]] ipc::RealtimeCertifiedReaderV1* reader() const
        noexcept {
        return reader_.get();
    }

    void Stop() noexcept {
        if (stopped_) {
            return;
        }
        if (service_ != nullptr) {
            service_->MarkDraining();
        }
        if (pipeline_ != nullptr) {
            pipeline_->StopAndDrain();
        }
        if (service_ != nullptr) {
            service_->MarkStoppedClean();
            service_->StopControl();
        }
        stopped_ = true;
    }

private:
    explicit BenchmarkHarness(bool recovery_enabled)
        : recovery_enabled_(recovery_enabled),
          fast_(std::make_shared<TimedFastSink>(
              kMaximumIngressRecords)) {}

    [[nodiscard]] bool Initialize() {
        if (!BuildFixture(&fixture_) || fast_ == nullptr) {
            return Fail("fixture", "create_failed");
        }

        common::Identity128 run_id{};
        run_id[0U] = std::byte{0x51};
        run_id[15U] = recovery_enabled_
                          ? std::byte{0xa5}
                          : std::byte{0x5a};

        if (recovery_enabled_) {
            temporary_directory_ = CreateTemporaryDirectory();
            if (temporary_directory_.empty()) {
                return Fail("temporary_directory", "create_failed");
            }
            ipc::RealtimeCertifiedServiceConfigV1 service_config{};
            service_config.run_id = run_id;
            service_config.session_epoch = 1U;
            service_config.trade_date = 20260730U;
            service_config.daily_catalog = fixture_.catalog;
            service_config.fast_sink = fast_;
            service_config.certified_tick_ring_capacity = 16'384U;
            service_config.channel_capacity = 8U;
            service_config.handoff_queue_capacity = 32'768U;
            service_config.maximum_pending_entries = 8'192U;
            service_config.maximum_pending_entries_per_channel =
                4'096U;
            service_config.certified_duplicate_retention_entries =
                8'192U;
            service_config.maximum_reorder_span = 8'192U;
            service_config.maximum_mapping_bytes =
                16U * 1024U * 1024U;
            service_config.maximum_order_states = 4'096U;
            service_config.maximum_derived_events = 65'536U;
            service_config.control_socket_path =
                temporary_directory_ / "certified.sock";
            int system_error = 0;
            const auto create_error =
                ipc::RealtimeCertifiedMarketServiceV1::Create(
                    service_config, &service_, &system_error);
            if (create_error !=
                    ipc::RealtimeCertifiedServiceCreateErrorV1::
                        kNone ||
                service_ == nullptr) {
                std::cerr
                    << "CERTIFIED_BENCH_ERROR"
                    << " stage=certified_service_create"
                    << " error="
                    << ipc::
                           RealtimeCertifiedServiceCreateErrorNameV1(
                               create_error)
                    << " errno=" << system_error << '\n';
                return false;
            }
            if (!service_->Start(&system_error)) {
                std::cerr
                    << "CERTIFIED_BENCH_ERROR"
                    << " stage=certified_service_start"
                    << " errno=" << system_error << '\n';
                return false;
            }

            int descriptor = -1;
            if (!service_->DuplicateReadOnlyDescriptorForTest(
                    &descriptor) ||
                descriptor < 0) {
                return Fail(
                    "certified_reader_descriptor",
                    "duplicate_failed");
            }
            ipc::RealtimeCertifiedExpectedSessionV1 expected{};
            std::memcpy(
                expected.run_id.data(),
                run_id.data(),
                run_id.size());
            expected.session_epoch = 1U;
            expected.trade_date = 20260730U;
            const auto reader_error =
                ipc::RealtimeCertifiedReaderV1::
                    OpenDescriptorForTest(
                        descriptor,
                        expected,
                        &reader_,
                        &system_error);
            static_cast<void>(::close(descriptor));
            if (reader_error !=
                    ipc::RealtimeCertifiedReaderOpenErrorV1::kNone ||
                reader_ == nullptr) {
                std::cerr
                    << "CERTIFIED_BENCH_ERROR"
                    << " stage=certified_reader_open"
                    << " error="
                    << ipc::
                           RealtimeCertifiedReaderOpenErrorNameV1(
                               reader_error)
                    << " errno=" << system_error << '\n';
                return false;
            }
        }

        runtime::RealtimePipelineConfigV1 pipeline_config{};
        pipeline_config.run_id = run_id;
        pipeline_config.trade_date = 20260730U;
        pipeline_config.daily_catalog = fixture_.catalog;
        pipeline_config.runtime_state = fixture_.runtime_state.get();
        pipeline_config.source_stream_ids =
            {1001U, 1002U, 2001U, 2002U};
        pipeline_config.maximum_sdk_message_bytes = 4'096U;
        pipeline_config.decoder_queue_capacity_per_source = 2'048U;
        pipeline_config.completion_tracker_capacity = 32'768U;
        pipeline_config.tick_ring_capacity = 32'768U;
        pipeline_config.store_worker_count = 1U;
        pipeline_config.store_queue_capacity_per_source_worker =
            2'048U;
        pipeline_config.intraday_store.segment_target_bytes =
            64U * 1024U;
        pipeline_config.intraday_store.maximum_session_records =
            kMaximumIngressRecords;
        pipeline_config.intraday_store
            .maximum_session_accounted_bytes =
            64U * 1024U * 1024U;
        pipeline_config.intraday_store.maximum_records_per_batch =
            512U;
        pipeline_config.intraday_store.coverage_from_open = true;
        if (recovery_enabled_) {
            pipeline_config.applied_record_sink = service_;
            pipeline_config.native_sequence_observation_sink =
                service_;
        } else {
            pipeline_config.applied_record_sink = fast_;
        }
        pipeline_config.sdk.enabled = false;

        std::string detail;
        const auto pipeline_error = runtime::RealtimePipelineV1::Create(
            pipeline_config, &pipeline_, &detail);
        if (pipeline_error !=
                runtime::RealtimePipelineCreateErrorV1::kNone ||
            pipeline_ == nullptr) {
            std::cerr
                << "CERTIFIED_BENCH_ERROR"
                << " stage=pipeline_create"
                << " error="
                << runtime::RealtimePipelineCreateErrorNameV1(
                       pipeline_error)
                << " detail=" << detail << '\n';
            return false;
        }
        return true;
    }

    const bool recovery_enabled_;
    bool stopped_ = false;
    Fixture fixture_{};
    std::shared_ptr<TimedFastSink> fast_;
    std::shared_ptr<ipc::RealtimeCertifiedMarketServiceV1> service_;
    std::unique_ptr<ipc::RealtimeCertifiedReaderV1> reader_;
    std::unique_ptr<runtime::RealtimePipelineV1> pipeline_;
    std::filesystem::path temporary_directory_;
};

struct LatencySummary final {
    std::uint64_t minimum_ns = 0U;
    std::uint64_t mean_ns = 0U;
    std::uint64_t p50_ns = 0U;
    std::uint64_t p95_ns = 0U;
    std::uint64_t p99_ns = 0U;
    std::uint64_t maximum_ns = 0U;
};

[[nodiscard]] std::uint64_t NearestRank(
    const std::vector<std::uint64_t>& sorted,
    std::uint64_t numerator,
    std::uint64_t denominator) noexcept {
    if (sorted.empty() || denominator == 0U ||
        numerator == 0U || numerator > denominator) {
        return 0U;
    }
    const std::uint64_t count =
        static_cast<std::uint64_t>(sorted.size());
    const std::uint64_t rank =
        (count / denominator) * numerator +
        ((count % denominator) * numerator + denominator - 1U) /
            denominator;
    const std::uint64_t one_based =
        std::max<std::uint64_t>(1U, rank);
    return sorted[static_cast<std::size_t>(
        std::min(count, one_based) - 1U)];
}

[[nodiscard]] LatencySummary Summarize(
    std::vector<std::uint64_t> values) {
    LatencySummary result{};
    if (values.empty()) {
        return result;
    }
    std::sort(values.begin(), values.end());
    long double total = 0.0L;
    for (const std::uint64_t value : values) {
        total += static_cast<long double>(value);
    }
    result.minimum_ns = values.front();
    result.mean_ns = static_cast<std::uint64_t>(
        total / static_cast<long double>(values.size()));
    result.p50_ns = NearestRank(values, 50U, 100U);
    result.p95_ns = NearestRank(values, 95U, 100U);
    result.p99_ns = NearestRank(values, 99U, 100U);
    result.maximum_ns = values.back();
    return result;
}

void PrintLatency(
    std::string_view phase,
    std::string_view recovery,
    std::string_view metric,
    const std::vector<std::uint64_t>& values) {
    const LatencySummary summary = Summarize(values);
    std::cout
        << "CERTIFIED_BENCH"
        << " kind=latency"
        << " phase=" << phase
        << " recovery=" << recovery
        << " metric=" << metric
        << " unit=ns"
        << " samples=" << values.size()
        << " estimator=nearest_rank"
        << " min=" << summary.minimum_ns
        << " mean=" << summary.mean_ns
        << " p50=" << summary.p50_ns
        << " p95=" << summary.p95_ns
        << " p99=" << summary.p99_ns
        << " max=" << summary.maximum_ns << '\n';
}

[[nodiscard]] std::string SignedDifference(
    std::uint64_t enabled,
    std::uint64_t disabled) {
    if (enabled >= disabled) {
        return std::to_string(enabled - disabled);
    }
    return "-" + std::to_string(disabled - enabled);
}

void PrintIncrement(
    std::string_view metric,
    const std::vector<std::uint64_t>& disabled,
    const std::vector<std::uint64_t>& enabled) {
    const LatencySummary off = Summarize(disabled);
    const LatencySummary on = Summarize(enabled);
    std::cout
        << "CERTIFIED_BENCH"
        << " kind=increment"
        << " phase=fast"
        << " metric=" << metric
        << " unit=ns"
        << " disabled_samples=" << disabled.size()
        << " enabled_samples=" << enabled.size()
        << " disabled_p50=" << off.p50_ns
        << " enabled_p50=" << on.p50_ns
        << " delta_p50="
        << SignedDifference(on.p50_ns, off.p50_ns)
        << " disabled_p95=" << off.p95_ns
        << " enabled_p95=" << on.p95_ns
        << " delta_p95="
        << SignedDifference(on.p95_ns, off.p95_ns)
        << " disabled_p99=" << off.p99_ns
        << " enabled_p99=" << on.p99_ns
        << " delta_p99="
        << SignedDifference(on.p99_ns, off.p99_ns)
        << '\n';
}

[[nodiscard]] bool ReadPipelineFastLatest(
    runtime::RealtimePipelineV1* pipeline,
    std::int64_t expected_native_sequence,
    std::uint64_t* call_duration_ns) {
    if (pipeline == nullptr || call_duration_ns == nullptr) {
        return false;
    }
    market::RealtimeLatestRecordViewV1 latest{};
    const std::uint64_t begin = MonotonicNowNs();
    const auto error =
        pipeline->GetLatestTick(1U, &latest);
    const std::uint64_t end = MonotonicNowNs();
    if (begin == 0U || end < begin ||
        error != market::RealtimeLatestQueryErrorV1::kNone ||
        !latest.available() || latest.record == nullptr) {
        return false;
    }
    ipc::RealtimeWireTickPayloadV2 payload{};
    if (!ipc::ProjectRealtimeWireTickPayloadV2(
            *latest.record, 0U, &payload) ||
        payload.native_event_sequence != expected_native_sequence) {
        return false;
    }
    *call_duration_ns = end - begin;
    return true;
}

struct FastBenchmarkResult final {
    std::vector<std::uint64_t> callback_call_ns;
    std::vector<std::uint64_t> callback_origin_to_fast_ns;
    std::vector<std::uint64_t> recv_to_fast_ns;
    std::vector<std::uint64_t> fast_latest_read_call_ns;
};

[[nodiscard]] bool InjectTrade(
    BenchmarkHarness* harness,
    std::uint64_t native_sequence,
    std::uint64_t quantity,
    std::uint64_t* origin_ns,
    std::uint64_t* callback_return_ns,
    runtime::RealtimePipelineIngressResultV1* result) {
    if (harness == nullptr || harness->pipeline() == nullptr ||
        origin_ns == nullptr || callback_return_ns == nullptr ||
        result == nullptr) {
        return false;
    }
    FakeMessage message(
        ShanghaiTradeBody(native_sequence, quantity));
    *origin_ns = MonotonicNowNs();
    *result =
        harness->pipeline()->InjectSdkMessageForTest(&message);
    *callback_return_ns = MonotonicNowNs();
    return *origin_ns != 0U &&
           *callback_return_ns >= *origin_ns &&
           result->accepted();
}

[[nodiscard]] bool RunFastBenchmark(
    bool recovery_enabled,
    FastBenchmarkResult* output) {
    if (output == nullptr) {
        return false;
    }
    *output = {};
    output->callback_call_ns.reserve(kMeasuredSamples);
    output->callback_origin_to_fast_ns.reserve(kMeasuredSamples);
    output->recv_to_fast_ns.reserve(kMeasuredSamples);
    output->fast_latest_read_call_ns.reserve(kMeasuredSamples);

    std::unique_ptr<BenchmarkHarness> harness;
    if (!BenchmarkHarness::Create(recovery_enabled, &harness) ||
        harness == nullptr) {
        return false;
    }
    const std::size_t total = kWarmupSamples + kMeasuredSamples;
    for (std::size_t index = 0U; index < total; ++index) {
        const std::uint64_t native_sequence =
            static_cast<std::uint64_t>(index) + 1U;
        std::uint64_t origin = 0U;
        std::uint64_t returned = 0U;
        runtime::RealtimePipelineIngressResultV1 ingress{};
        if (!InjectTrade(
                harness.get(),
                native_sequence,
                1'000U + native_sequence % 997U,
                &origin,
                &returned,
                &ingress)) {
            return Fail("fast_inject", "rejected");
        }
        FastPublicationTiming timing{};
        if (!WaitUntil([&] {
                return harness->fast()->ReadTiming(
                    ingress.global_ingress_sequence, &timing);
            }) ||
            timing.native_sequence !=
                static_cast<std::int64_t>(native_sequence) ||
            timing.recv_monotonic_ns < origin ||
            timing.visible_monotonic_ns <
                timing.recv_monotonic_ns) {
            return Fail("fast_publication", "correlation_failed");
        }
        std::uint64_t read_call = 0U;
        if (!ReadPipelineFastLatest(
                harness->pipeline(),
                static_cast<std::int64_t>(native_sequence),
                &read_call)) {
            return Fail("fast_latest", "read_failed");
        }
        if (index >= kWarmupSamples) {
            output->callback_call_ns.push_back(returned - origin);
            output->callback_origin_to_fast_ns.push_back(
                timing.visible_monotonic_ns - origin);
            output->recv_to_fast_ns.push_back(
                timing.visible_monotonic_ns -
                timing.recv_monotonic_ns);
            output->fast_latest_read_call_ns.push_back(read_call);
        }
    }

    if (harness->fast()->coverage_lost() ||
        harness->pipeline()->fatal()) {
        return Fail("fast_health", "coverage_or_pipeline_fatal");
    }
    if (recovery_enabled) {
        if (!WaitUntil([&] {
                const auto snapshot = harness->service()->Snapshot();
                return snapshot.wire_snapshot_consistent &&
                       snapshot.canonical_apply_frontier ==
                           static_cast<std::uint64_t>(total) &&
                       snapshot.state ==
                           ipc::RealtimeCertifiedStateV1::kContiguous;
            })) {
            return Fail(
                "fast_enabled_certified_drain",
                "frontier_not_contiguous");
        }
        const auto snapshot = harness->service()->Snapshot();
        if (snapshot.globally_frozen_resource ||
            snapshot.dropped_handoffs != 0U) {
            return Fail(
                "fast_enabled_certified_health",
                "handoff_loss_or_freeze");
        }
    }
    harness->Stop();
    return true;
}

void PrintFastBenchmark(
    std::string_view recovery,
    const FastBenchmarkResult& result) {
    PrintLatency(
        "fast",
        recovery,
        "callback_call",
        result.callback_call_ns);
    PrintLatency(
        "fast",
        recovery,
        "callback_origin_to_fast_first_visible",
        result.callback_origin_to_fast_ns);
    PrintLatency(
        "fast",
        recovery,
        "wire_recv_to_fast_first_visible",
        result.recv_to_fast_ns);
    PrintLatency(
        "fast",
        recovery,
        "inprocess_fast_latest_read_call",
        result.fast_latest_read_call_ns);
}

struct CertifiedNormalResult final {
    std::vector<std::uint64_t> callback_call_ns;
    std::vector<std::uint64_t> callback_origin_to_fast_ns;
    std::vector<std::uint64_t> recv_to_fast_ns;
    std::vector<std::uint64_t> recv_to_writer_stamp_ns;
    std::vector<std::uint64_t> callback_origin_to_reader_ns;
    std::vector<std::uint64_t> recv_to_reader_ns;
    std::vector<std::uint64_t> writer_stamp_to_reader_ns;
    std::vector<std::uint64_t> read_latest_call_ns;
};

[[nodiscard]] bool WaitCertifiedLatest(
    ipc::RealtimeCertifiedReaderV1* reader,
    std::int64_t expected_native_sequence,
    ipc::RealtimeCertifiedTickEnvelopeV1* output,
    std::uint64_t* visible_ns,
    std::uint64_t* successful_call_ns) {
    if (reader == nullptr || output == nullptr ||
        visible_ns == nullptr || successful_call_ns == nullptr) {
        return false;
    }
    const std::uint64_t start = MonotonicNowNs();
    if (start == 0U) {
        return false;
    }
    constexpr std::uint64_t kTimeoutNs = 5'000'000'000ULL;
    for (;;) {
        ipc::RealtimeCertifiedTickEnvelopeV1 candidate{};
        const std::uint64_t begin = MonotonicNowNs();
        const auto result = reader->ReadLatest(0U, &candidate);
        const std::uint64_t end = MonotonicNowNs();
        if (begin == 0U || end < begin) {
            return false;
        }
        if (result == ipc::RealtimeCertifiedReadResultV1::kOk) {
            if (candidate.payload.native_event_sequence >
                expected_native_sequence) {
                return false;
            }
            if (candidate.payload.native_event_sequence ==
                expected_native_sequence) {
                *output = candidate;
                *visible_ns = end;
                *successful_call_ns = end - begin;
                return true;
            }
        } else if (
            result != ipc::RealtimeCertifiedReadResultV1::kNoData &&
            result !=
                ipc::RealtimeCertifiedReadResultV1::kInconsistent) {
            return false;
        }
        if (end - start >= kTimeoutNs) {
            return false;
        }
    }
}

[[nodiscard]] bool RunCertifiedNormalBenchmark(
    CertifiedNormalResult* output) {
    if (output == nullptr) {
        return false;
    }
    *output = {};
    output->callback_call_ns.reserve(kMeasuredSamples);
    output->callback_origin_to_fast_ns.reserve(kMeasuredSamples);
    output->recv_to_fast_ns.reserve(kMeasuredSamples);
    output->recv_to_writer_stamp_ns.reserve(kMeasuredSamples);
    output->callback_origin_to_reader_ns.reserve(kMeasuredSamples);
    output->recv_to_reader_ns.reserve(kMeasuredSamples);
    output->writer_stamp_to_reader_ns.reserve(kMeasuredSamples);
    output->read_latest_call_ns.reserve(kMeasuredSamples);

    std::unique_ptr<BenchmarkHarness> harness;
    if (!BenchmarkHarness::Create(true, &harness) ||
        harness == nullptr || harness->reader() == nullptr) {
        return false;
    }
    const std::size_t total = kWarmupSamples + kMeasuredSamples;
    for (std::size_t index = 0U; index < total; ++index) {
        const std::uint64_t native_sequence =
            static_cast<std::uint64_t>(index) + 1U;
        std::uint64_t origin = 0U;
        std::uint64_t returned = 0U;
        runtime::RealtimePipelineIngressResultV1 ingress{};
        if (!InjectTrade(
                harness.get(),
                native_sequence,
                2'000U + native_sequence % 991U,
                &origin,
                &returned,
                &ingress)) {
            return Fail("certified_normal_inject", "rejected");
        }
        FastPublicationTiming fast{};
        if (!WaitUntil([&] {
                return harness->fast()->ReadTiming(
                    ingress.global_ingress_sequence, &fast);
            })) {
            return Fail(
                "certified_normal_fast", "publication_timeout");
        }
        ipc::RealtimeCertifiedTickEnvelopeV1 envelope{};
        std::uint64_t visible = 0U;
        std::uint64_t read_call = 0U;
        if (!WaitCertifiedLatest(
                harness->reader(),
                static_cast<std::int64_t>(native_sequence),
                &envelope,
                &visible,
                &read_call) ||
            envelope.payload.common.recv_monotonic_ns <= 0 ||
            static_cast<std::uint64_t>(
                envelope.payload.common.recv_monotonic_ns) !=
                fast.recv_monotonic_ns ||
            envelope.certified_monotonic_ns <
                fast.recv_monotonic_ns ||
            visible < envelope.certified_monotonic_ns) {
            return Fail(
                "certified_normal_reader", "correlation_failed");
        }
        if (index >= kWarmupSamples) {
            output->callback_call_ns.push_back(returned - origin);
            output->callback_origin_to_fast_ns.push_back(
                fast.visible_monotonic_ns - origin);
            output->recv_to_fast_ns.push_back(
                fast.visible_monotonic_ns -
                fast.recv_monotonic_ns);
            output->recv_to_writer_stamp_ns.push_back(
                envelope.certified_monotonic_ns -
                fast.recv_monotonic_ns);
            output->callback_origin_to_reader_ns.push_back(
                visible - origin);
            output->recv_to_reader_ns.push_back(
                visible - fast.recv_monotonic_ns);
            output->writer_stamp_to_reader_ns.push_back(
                visible - envelope.certified_monotonic_ns);
            output->read_latest_call_ns.push_back(read_call);
        }
    }
    const auto snapshot = harness->service()->Snapshot();
    if (harness->pipeline()->fatal() ||
        harness->fast()->coverage_lost() ||
        snapshot.globally_frozen_resource ||
        snapshot.dropped_handoffs != 0U) {
        return Fail("certified_normal_health", "unhealthy");
    }
    harness->Stop();
    return true;
}

void PrintCertifiedNormal(
    const CertifiedNormalResult& result) {
    PrintLatency(
        "certified_normal",
        "enabled",
        "callback_call",
        result.callback_call_ns);
    PrintLatency(
        "certified_normal",
        "enabled",
        "callback_origin_to_fast_first_visible",
        result.callback_origin_to_fast_ns);
    PrintLatency(
        "certified_normal",
        "enabled",
        "wire_recv_to_fast_first_visible",
        result.recv_to_fast_ns);
    PrintLatency(
        "certified_normal",
        "enabled",
        "wire_recv_to_certified_writer_stamp",
        result.recv_to_writer_stamp_ns);
    PrintLatency(
        "certified_normal",
        "enabled",
        "callback_origin_to_certified_reader_visible",
        result.callback_origin_to_reader_ns);
    PrintLatency(
        "certified_normal",
        "enabled",
        "wire_recv_to_certified_reader_visible",
        result.recv_to_reader_ns);
    PrintLatency(
        "certified_normal",
        "enabled",
        "certified_writer_stamp_to_reader_visible",
        result.writer_stamp_to_reader_ns);
    PrintLatency(
        "certified_normal",
        "enabled",
        "certified_read_latest_successful_call",
        result.read_latest_call_ns);
}

struct GapSuffixResult final {
    std::uint64_t suffix = 0U;
    std::vector<std::uint64_t> repair_callback_call_ns;
    std::vector<std::uint64_t> catchup_total_ns;
    std::vector<std::uint64_t> catchup_ticks_per_second;
    bool fast_latest_available_during_gap = true;
    bool certified_last_good_available_during_gap = true;
};

[[nodiscard]] bool WaitGapPrepared(
    BenchmarkHarness* harness,
    std::uint64_t total_ingress,
    std::uint64_t prior_canonical_frontier) {
    if (harness == nullptr || harness->service() == nullptr) {
        return false;
    }
    return WaitUntil([&] {
        const auto snapshot = harness->service()->Snapshot();
        return snapshot.wire_snapshot_consistent &&
               snapshot.enqueued_observations >= total_ingress &&
               snapshot.enqueued_applied_records >= total_ingress &&
               snapshot.processed_handoffs >= total_ingress * 2U &&
               snapshot.canonical_apply_frontier ==
                   prior_canonical_frontier &&
               snapshot.state ==
                   ipc::RealtimeCertifiedStateV1::kGapOpen;
    });
}

[[nodiscard]] bool RunGapRecoveryBenchmark(
    std::array<GapSuffixResult, kGapSuffixes.size()>* output) {
    if (output == nullptr) {
        return false;
    }
    std::unique_ptr<BenchmarkHarness> harness;
    if (!BenchmarkHarness::Create(true, &harness) ||
        harness == nullptr || harness->reader() == nullptr) {
        return false;
    }

    std::uint64_t next_native = 1U;
    std::uint64_t total_ingress = 0U;
    std::uint64_t origin = 0U;
    std::uint64_t returned = 0U;
    runtime::RealtimePipelineIngressResultV1 ingress{};
    if (!InjectTrade(
            harness.get(),
            next_native,
            3'000U,
            &origin,
            &returned,
            &ingress)) {
        return Fail("gap_anchor_inject", "rejected");
    }
    total_ingress = ingress.global_ingress_sequence;
    ipc::RealtimeCertifiedTickEnvelopeV1 anchor{};
    std::uint64_t anchor_visible = 0U;
    std::uint64_t anchor_read_call = 0U;
    if (!WaitCertifiedLatest(
            harness->reader(),
            1,
            &anchor,
            &anchor_visible,
            &anchor_read_call)) {
        return Fail("gap_anchor_certified", "not_visible");
    }
    std::uint64_t canonical_frontier = 1U;
    ++next_native;

    for (std::size_t suffix_index = 0U;
         suffix_index < kGapSuffixes.size();
         ++suffix_index) {
        GapSuffixResult& result = (*output)[suffix_index];
        result = {};
        result.suffix = kGapSuffixes[suffix_index];
        result.repair_callback_call_ns.reserve(kGapMeasuredTrials);
        result.catchup_total_ns.reserve(kGapMeasuredTrials);
        result.catchup_ticks_per_second.reserve(
            kGapMeasuredTrials);

        const std::size_t trials =
            kGapWarmupTrials + kGapMeasuredTrials;
        for (std::size_t trial = 0U; trial < trials; ++trial) {
            const std::uint64_t missing = next_native;
            const std::uint64_t prior_native = missing - 1U;
            const std::uint64_t future_last =
                missing + result.suffix;
            for (std::uint64_t native = missing + 1U;
                 native <= future_last;
                 ++native) {
                if (!InjectTrade(
                        harness.get(),
                        native,
                        4'000U + native % 983U,
                        &origin,
                        &returned,
                        &ingress)) {
                    return Fail(
                        "gap_suffix_inject", "rejected");
                }
                total_ingress =
                    ingress.global_ingress_sequence;
            }

            FastPublicationTiming future_fast{};
            if (!WaitUntil([&] {
                    return harness->fast()->ReadTiming(
                        total_ingress, &future_fast);
                }) ||
                future_fast.native_sequence !=
                    static_cast<std::int64_t>(future_last) ||
                !WaitGapPrepared(
                    harness.get(),
                    total_ingress,
                    canonical_frontier)) {
                return Fail("gap_prepare", "not_quiescent");
            }

            std::uint64_t latest_fast_ingress = 0U;
            std::int64_t latest_fast_native = 0;
            std::uint64_t fast_read_call = 0U;
            const bool fast_sink_available =
                harness->fast()->ReadLatest(
                    &latest_fast_ingress,
                    &latest_fast_native) &&
                latest_fast_ingress == total_ingress &&
                latest_fast_native ==
                    static_cast<std::int64_t>(future_last);
            const bool pipeline_fast_available =
                ReadPipelineFastLatest(
                    harness->pipeline(),
                    static_cast<std::int64_t>(future_last),
                    &fast_read_call);
            result.fast_latest_available_during_gap =
                result.fast_latest_available_during_gap &&
                fast_sink_available && pipeline_fast_available;

            ipc::RealtimeCertifiedTickEnvelopeV1 last_good{};
            const bool certified_last_good =
                harness->reader()->ReadLatest(0U, &last_good) ==
                    ipc::RealtimeCertifiedReadResultV1::kOk &&
                last_good.canonical_apply_sequence ==
                    canonical_frontier &&
                last_good.payload.native_event_sequence ==
                    static_cast<std::int64_t>(prior_native);
            result.certified_last_good_available_during_gap =
                result.certified_last_good_available_during_gap &&
                certified_last_good;
            if (!fast_sink_available || !pipeline_fast_available ||
                !certified_last_good) {
                return Fail(
                    "gap_availability", "invariant_failed");
            }

            FakeMessage repair_message(
                ShanghaiTradeBody(
                    missing, 5'000U + missing % 977U));
            const std::uint64_t repair_origin = MonotonicNowNs();
            const auto repair_ingress =
                harness->pipeline()->InjectSdkMessageForTest(
                    &repair_message);
            const std::uint64_t repair_return = MonotonicNowNs();
            if (repair_origin == 0U ||
                repair_return < repair_origin ||
                !repair_ingress.accepted()) {
                return Fail("gap_repair_inject", "rejected");
            }
            total_ingress =
                repair_ingress.global_ingress_sequence;
            const std::uint64_t expected_frontier =
                canonical_frontier + result.suffix + 1U;
            ipc::RealtimeCertifiedTickEnvelopeV1 repaired{};
            std::uint64_t catchup_visible = 0U;
            std::uint64_t catchup_read_call = 0U;
            if (!WaitCertifiedLatest(
                    harness->reader(),
                    static_cast<std::int64_t>(future_last),
                    &repaired,
                    &catchup_visible,
                    &catchup_read_call) ||
                repaired.canonical_apply_sequence !=
                    expected_frontier ||
                catchup_visible < repair_origin) {
                return Fail("gap_catchup", "reader_timeout");
            }
            if (!WaitUntil([&] {
                    ipc::RealtimeCertifiedStatusSnapshotV1 status{};
                    return harness->reader()->ReadStatus(&status) ==
                               ipc::RealtimeCertifiedReadResultV1::
                                   kOk &&
                           status.canonical_apply_frontier ==
                               expected_frontier &&
                           status.state ==
                               ipc::RealtimeCertifiedStateV1::
                                   kContiguous;
                })) {
                return Fail(
                    "gap_catchup_state", "not_contiguous");
            }

            if (trial >= kGapWarmupTrials) {
                const std::uint64_t total_ns =
                    catchup_visible - repair_origin;
                if (total_ns == 0U) {
                    return Fail("gap_catchup", "zero_duration");
                }
                result.repair_callback_call_ns.push_back(
                    repair_return - repair_origin);
                result.catchup_total_ns.push_back(total_ns);
                const long double rate =
                    static_cast<long double>(
                        result.suffix + 1U) *
                    1'000'000'000.0L /
                    static_cast<long double>(total_ns);
                result.catchup_ticks_per_second.push_back(
                    static_cast<std::uint64_t>(rate));
            }
            canonical_frontier = expected_frontier;
            next_native = future_last + 1U;
        }
    }

    const auto snapshot = harness->service()->Snapshot();
    if (harness->pipeline()->fatal() ||
        harness->fast()->coverage_lost() ||
        snapshot.globally_frozen_resource ||
        snapshot.dropped_handoffs != 0U ||
        snapshot.gap_recovered_count <
            kGapSuffixes.size() *
                (kGapWarmupTrials + kGapMeasuredTrials)) {
        return Fail("gap_final_health", "unhealthy");
    }
    harness->Stop();
    return true;
}

void PrintGapRecovery(
    const std::array<GapSuffixResult, kGapSuffixes.size()>& results) {
    for (const GapSuffixResult& result : results) {
        const LatencySummary callback =
            Summarize(result.repair_callback_call_ns);
        const LatencySummary catchup =
            Summarize(result.catchup_total_ns);
        const LatencySummary rate =
            Summarize(result.catchup_ticks_per_second);
        std::cout
            << "CERTIFIED_BENCH"
            << " kind=gap"
            << " phase=certified_recovery"
            << " recovery=enabled"
            << " suffix=" << result.suffix
            << " catchup_records=" << result.suffix + 1U
            << " warmup_trials=" << kGapWarmupTrials
            << " measured_trials=" << kGapMeasuredTrials
            << " estimator=nearest_rank"
            << " repair_callback_p50_ns=" << callback.p50_ns
            << " repair_callback_p95_ns=" << callback.p95_ns
            << " repair_callback_p99_ns=" << callback.p99_ns
            << " catchup_total_p50_ns=" << catchup.p50_ns
            << " catchup_total_p95_ns=" << catchup.p95_ns
            << " catchup_total_p99_ns=" << catchup.p99_ns
            << " throughput_p50_ticks_per_s=" << rate.p50_ns
            << " throughput_p95_ticks_per_s=" << rate.p95_ns
            << " throughput_p99_ticks_per_s=" << rate.p99_ns
            << " fast_latest_available_during_gap="
            << (result.fast_latest_available_during_gap ? 1 : 0)
            << " certified_last_good_available_during_gap="
            << (result.certified_last_good_available_during_gap
                    ? 1
                    : 0)
            << '\n';
    }
}

enum class BenchmarkMode : std::uint8_t {
    kAll = 0U,
    kFastDisabled,
    kFastEnabled,
    kFastComparison,
    kCertifiedNormal,
    kGapRecovery,
};

[[nodiscard]] bool ParseMode(
    int argc,
    char** argv,
    BenchmarkMode* output) {
    if (output == nullptr) {
        return false;
    }
    if (argc == 1) {
        *output = BenchmarkMode::kAll;
        return true;
    }
    if (argc != 2) {
        return false;
    }
    const std::string_view argument(argv[1]);
    if (argument == "--all") {
        *output = BenchmarkMode::kAll;
    } else if (argument == "--fast-disabled") {
        *output = BenchmarkMode::kFastDisabled;
    } else if (argument == "--fast-enabled") {
        *output = BenchmarkMode::kFastEnabled;
    } else if (argument == "--fast-comparison") {
        *output = BenchmarkMode::kFastComparison;
    } else if (argument == "--certified-normal") {
        *output = BenchmarkMode::kCertifiedNormal;
    } else if (argument == "--gap-recovery") {
        *output = BenchmarkMode::kGapRecovery;
    } else {
        return false;
    }
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    BenchmarkMode mode = BenchmarkMode::kAll;
    if (!ParseMode(argc, argv, &mode)) {
        std::cerr
            << "usage: " << argv[0]
            << " [--all|--fast-disabled|--fast-enabled"
               "|--fast-comparison|--certified-normal"
               "|--gap-recovery]\n";
        return 2;
    }

    std::cout
        << "CERTIFIED_BENCH"
        << " kind=meta"
        << " schema=1"
        << " workload=shanghai_trade_single_instrument_closed_loop"
        << " callback_path=InjectSdkMessageForTest"
        << " warmup_samples=" << kWarmupSamples
        << " measured_samples=" << kMeasuredSamples
        << " quantile=nearest_rank"
        << " clock=CLOCK_MONOTONIC"
        << '\n';

    FastBenchmarkResult disabled{};
    FastBenchmarkResult enabled{};
    const bool run_disabled =
        mode == BenchmarkMode::kAll ||
        mode == BenchmarkMode::kFastDisabled ||
        mode == BenchmarkMode::kFastComparison;
    const bool run_enabled =
        mode == BenchmarkMode::kAll ||
        mode == BenchmarkMode::kFastEnabled ||
        mode == BenchmarkMode::kFastComparison;
    if (run_disabled) {
        if (!RunFastBenchmark(false, &disabled)) {
            return 1;
        }
        PrintFastBenchmark("disabled", disabled);
    }
    if (run_enabled) {
        if (!RunFastBenchmark(true, &enabled)) {
            return 1;
        }
        PrintFastBenchmark("enabled", enabled);
    }
    if (run_disabled && run_enabled) {
        PrintIncrement(
            "callback_call",
            disabled.callback_call_ns,
            enabled.callback_call_ns);
        PrintIncrement(
            "callback_origin_to_fast_first_visible",
            disabled.callback_origin_to_fast_ns,
            enabled.callback_origin_to_fast_ns);
        PrintIncrement(
            "wire_recv_to_fast_first_visible",
            disabled.recv_to_fast_ns,
            enabled.recv_to_fast_ns);
        PrintIncrement(
            "inprocess_fast_latest_read_call",
            disabled.fast_latest_read_call_ns,
            enabled.fast_latest_read_call_ns);
    }

    if (mode == BenchmarkMode::kAll ||
        mode == BenchmarkMode::kCertifiedNormal) {
        CertifiedNormalResult normal{};
        if (!RunCertifiedNormalBenchmark(&normal)) {
            return 1;
        }
        PrintCertifiedNormal(normal);
    }

    if (mode == BenchmarkMode::kAll ||
        mode == BenchmarkMode::kGapRecovery) {
        std::array<GapSuffixResult, kGapSuffixes.size()> gaps{};
        if (!RunGapRecoveryBenchmark(&gaps)) {
            return 1;
        }
        PrintGapRecovery(gaps);
    }
    return 0;
}
