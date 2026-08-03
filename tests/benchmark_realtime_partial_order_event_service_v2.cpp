#include "l2flow/ipc/partial_order_event_reader_v2.h"
#include "l2flow/ipc/realtime_partial_order_event_service_v2.h"
#include "l2flow/ipc/realtime_wire_projection_v2.h"
#include "l2flow/market/daily_instrument_catalog_v2.h"
#include "l2flow/market/instrument_runtime_state_v2.h"
#include "l2flow/runtime/realtime_pipeline_v1.h"
#include "l2flow/sdk/vendor_head_view.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
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

#include <sched.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

namespace ipc = l2flow::ipc;
namespace market = l2flow::market;
namespace mdl = datayes::mdl;
namespace realtime = l2flow::realtime;
namespace runtime = l2flow::runtime;
namespace sdk = l2flow::sdk;

namespace {

using namespace std::chrono_literals;

constexpr std::uint32_t kTradeDate = 20260803U;
constexpr std::uint32_t kChannel = 12U;
constexpr std::size_t kWarmupSamples = 128U;
constexpr std::size_t kMeasuredSamples = 1'024U;
constexpr std::size_t kMaximumIngressRecords = 32'768U;
constexpr std::uint64_t kNormalHandoffQueueCapacity = 32'768U;
constexpr std::uint64_t kMaximumPendingEntries = 8'192U;
constexpr std::uint64_t kMaximumPendingEntriesPerChannel = 8'192U;
constexpr std::uint64_t kDuplicateRetentionEntries = 8'192U;
constexpr std::uint64_t kMaximumReorderSpan = 16'384U;
constexpr std::uint32_t kChannelCapacity = 8U;
constexpr std::size_t kMaximumOrderProjectorStates = 8'192U;
constexpr std::size_t kMaximumDerivedEvents = 16'384U;
constexpr std::uint64_t kEventJournalCapacity = 16'384U;
constexpr std::uint64_t kOrderStateCapacity = 32'768U;
constexpr std::uint64_t kMaximumMappingBytes =
    64ULL * 1024ULL * 1024ULL;
constexpr std::uint64_t kLazyCommitChunkBytes =
    1ULL * 1024ULL * 1024ULL;
constexpr std::size_t kDecoderQueueCapacityPerSource = 4'096U;
constexpr std::size_t kCompletionTrackerCapacity = 16'384U;
constexpr std::size_t kTickRingCapacity = 16'384U;
constexpr std::size_t kStoreQueueCapacityPerSourceWorker = 4'096U;
constexpr std::size_t kRealtimeHistoryRecordCapacity = 16'384U;
constexpr std::uint64_t kRealtimeHistoryAccountedByteCapacity =
    128ULL * 1024ULL * 1024ULL;
constexpr std::uint64_t kPressureQueueCapacity = 64U;
constexpr std::size_t kGapWarmupTrials = 1U;
constexpr std::size_t kGapMeasuredTrials = 3U;
constexpr std::array<std::uint64_t, 4U> kGapSuffixes{
    1U, 32U, 256U, 1'024U};
constexpr std::chrono::nanoseconds kGapHold = 1ms;

struct CpuAffinitySnapshot final {
    bool available = false;
    std::string allowed_cpus = "unavailable";
    int system_error = 0;
};

[[nodiscard]] CpuAffinitySnapshot ReadCpuAffinity() {
    CpuAffinitySnapshot result{};
    cpu_set_t set{};
    CPU_ZERO(&set);
    if (::sched_getaffinity(0, sizeof(set), &set) != 0) {
        result.system_error = errno;
        return result;
    }
    result.available = true;
    result.allowed_cpus.clear();
    bool first_range = true;
    int cpu = 0;
    while (cpu < CPU_SETSIZE) {
        if (CPU_ISSET(cpu, &set) == 0) {
            ++cpu;
            continue;
        }
        const int start = cpu;
        while (cpu + 1 < CPU_SETSIZE &&
               CPU_ISSET(cpu + 1, &set) != 0) {
            ++cpu;
        }
        const int end = cpu;
        if (!first_range) {
            result.allowed_cpus.push_back(',');
        }
        result.allowed_cpus += std::to_string(start);
        if (end != start) {
            result.allowed_cpus.push_back('-');
            result.allowed_cpus += std::to_string(end);
        }
        first_range = false;
        ++cpu;
    }
    if (result.allowed_cpus.empty()) {
        result.allowed_cpus = "empty";
    }
    return result;
}

struct ResourceUsageSnapshot final {
    bool available = false;
    std::uint64_t user_cpu_ns = 0U;
    std::uint64_t system_cpu_ns = 0U;
    std::uint64_t minor_faults = 0U;
    std::uint64_t major_faults = 0U;
    std::uint64_t process_max_rss_kib = 0U;
    int system_error = 0;
};

[[nodiscard]] bool TimevalToNanoseconds(
    const timeval& value,
    std::uint64_t* output) noexcept {
    if (output == nullptr || value.tv_sec < 0 || value.tv_usec < 0 ||
        value.tv_usec >= 1'000'000L) {
        return false;
    }
    constexpr std::uint64_t kNanosecondsPerSecond =
        1'000'000'000ULL;
    constexpr std::uint64_t kNanosecondsPerMicrosecond = 1'000ULL;
    const std::uint64_t seconds =
        static_cast<std::uint64_t>(value.tv_sec);
    const std::uint64_t subsecond =
        static_cast<std::uint64_t>(value.tv_usec) *
        kNanosecondsPerMicrosecond;
    if (seconds >
        (std::numeric_limits<std::uint64_t>::max() - subsecond) /
            kNanosecondsPerSecond) {
        return false;
    }
    *output = seconds * kNanosecondsPerSecond + subsecond;
    return true;
}

[[nodiscard]] ResourceUsageSnapshot ReadResourceUsage() noexcept {
    ResourceUsageSnapshot result{};
    rusage usage{};
    if (::getrusage(RUSAGE_SELF, &usage) != 0) {
        result.system_error = errno;
        return result;
    }
    if (usage.ru_minflt < 0 || usage.ru_majflt < 0 ||
        usage.ru_maxrss < 0 ||
        !TimevalToNanoseconds(usage.ru_utime, &result.user_cpu_ns) ||
        !TimevalToNanoseconds(
            usage.ru_stime, &result.system_cpu_ns)) {
        result.system_error = EDOM;
        return result;
    }
    result.available = true;
    result.minor_faults =
        static_cast<std::uint64_t>(usage.ru_minflt);
    result.major_faults =
        static_cast<std::uint64_t>(usage.ru_majflt);
    // Linux reports ru_maxrss in KiB. It is a process lifetime peak and
    // cannot be subtracted into a meaningful per-phase delta.
    result.process_max_rss_kib =
        static_cast<std::uint64_t>(usage.ru_maxrss);
    return result;
}

[[nodiscard]] std::uint64_t NonnegativeDifference(
    std::uint64_t after,
    std::uint64_t before) noexcept {
    return after >= before ? after - before : 0U;
}

void PrintResourceUsage(
    std::string_view phase,
    const ResourceUsageSnapshot& before,
    const ResourceUsageSnapshot& after) {
    std::cout
        << "PARTIAL_EVENT_BENCH"
        << " kind=resource_usage"
        << " phase=" << phase
        << " available="
        << (before.available && after.available ? 1 : 0);
    if (!before.available || !after.available) {
        std::cout
            << " before_errno=" << before.system_error
            << " after_errno=" << after.system_error << '\n';
        return;
    }
    std::cout
        << " measurement_scope=phase_including_fixture_lifecycle"
        << " user_cpu_delta_ns="
        << NonnegativeDifference(
               after.user_cpu_ns, before.user_cpu_ns)
        << " system_cpu_delta_ns="
        << NonnegativeDifference(
               after.system_cpu_ns, before.system_cpu_ns)
        << " minor_faults_delta="
        << NonnegativeDifference(
               after.minor_faults, before.minor_faults)
        << " major_faults_delta="
        << NonnegativeDifference(
               after.major_faults, before.major_faults)
        << " process_max_rss_kib=" << after.process_max_rss_kib
        << " max_rss_scope=process_peak_not_phase_delta"
        << '\n';
}

[[nodiscard]] std::uint64_t MonotonicNowNs() noexcept {
    timespec value{};
    if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0 ||
        value.tv_sec < 0 || value.tv_nsec < 0 ||
        value.tv_nsec >= 1'000'000'000L) {
        return 0U;
    }
    constexpr std::uint64_t kNanosecondsPerSecond =
        1'000'000'000ULL;
    const std::uint64_t seconds =
        static_cast<std::uint64_t>(value.tv_sec);
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
    std::cerr << "PARTIAL_EVENT_BENCH_ERROR"
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

[[nodiscard]] std::vector<std::byte> ShenzhenOrderBody(
    std::uint32_t channel,
    std::uint64_t native_sequence) {
    WireWriter writer(58U);
    writer.StoreU32(0U, channel);
    writer.StoreU64(4U, native_sequence);
    writer.StoreU64(30U, 123'456U);
    writer.StoreU64(38U, 201U);
    writer.StoreU32(46U, 49U);
    writer.StoreU32(50U, 93'000'124U);
    writer.StoreU32(54U, 50U);
    writer.StoreString(12U, "010");
    writer.StoreString(18U, "000001");
    writer.StoreString(24U, "102 ");
    return std::move(writer).Take();
}

class FakeMessage final : public mdl::MDLMessage {
public:
    FakeMessage(
        std::uint32_t channel,
        std::uint64_t native_sequence)
        : body_(ShenzhenOrderBody(channel, native_sequence)) {
        std::memset(&head_, 0, sizeof(head_));
        head_.HeadSize =
            static_cast<std::uint8_t>(sdk::kVendorHeadBytes);
        head_.MessageSize = static_cast<std::uint32_t>(
            sdk::kVendorHeadBytes + body_.size());
        head_.MessageEncoding =
            static_cast<std::uint8_t>(mdl::MDLEID_BINARY);
        head_.ServiceID = 6U;
        head_.ServiceVersion = 101U;
        head_.MessageID = 33U;
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
        const std::uint64_t visible = MonotonicNowNs();
        if (recv == 0U || visible < recv) {
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
        count_.fetch_add(1U, std::memory_order_release);
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

    [[nodiscard]] std::uint64_t count() const noexcept {
        return count_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool coverage_lost() const noexcept {
        return coverage_lost_.load(std::memory_order_acquire);
    }

private:
    const std::size_t maximum_ingress_;
    std::unique_ptr<TimedFastPublication[]> publications_;
    std::atomic<std::uint64_t> count_{0U};
    std::atomic<bool> coverage_lost_{false};
};

[[nodiscard]] std::vector<std::byte> OpaqueBytes(
    std::string_view value) {
    const auto bytes =
        std::as_bytes(std::span(value.data(), value.size()));
    return {bytes.begin(), bytes.end()};
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
    }

    [[nodiscard]] static bool Create(
        bool event_enabled,
        std::uint64_t handoff_queue_capacity,
        std::unique_ptr<BenchmarkHarness>* output) {
        if (output == nullptr) {
            return false;
        }
        output->reset();
        auto result = std::unique_ptr<BenchmarkHarness>(
            new BenchmarkHarness(event_enabled));
        if (!result->Initialize(handoff_queue_capacity)) {
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
        ipc::RealtimePartialOrderEventServiceV2>& service() const
        noexcept {
        return service_;
    }
    [[nodiscard]] ipc::PartialOrderEventReaderV2* reader() const
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
            service_->Stop();
        }
        stopped_ = true;
    }

private:
    explicit BenchmarkHarness(bool event_enabled)
        : event_enabled_(event_enabled),
          fast_(std::make_shared<TimedFastSink>(
              kMaximumIngressRecords)) {}

    [[nodiscard]] bool Initialize(
        std::uint64_t handoff_queue_capacity) {
        market::DailyInstrumentSourceEntryV2 entry{};
        entry.key.market = market::MarketV1::kShenzhen;
        entry.key.security_id_source = OpaqueBytes("102 ");
        entry.key.security_id = OpaqueBytes("000001");
        entry.metadata.quantity_unit = market::QuantityUnitV1::kShare;
        entry.metadata.security_type = market::SecurityTypeV1::kEquity;
        entry.metadata.asset_scope =
            market::AssetScopeV1::kDocumentedCore;

        market::DailyInstrumentCatalogConfigV2 catalog_config{};
        catalog_config.trade_date = kTradeDate;
        catalog_config.catalog_version = 1U;
        catalog_config.session_epoch = 1U;
        catalog_config.market_scope =
            market::kDailyCatalogMainlandScopeV2;
        catalog_config.coverage_complete = true;
        std::unique_ptr<market::DailyInstrumentCatalogV2> catalog;
        if (market::DailyInstrumentCatalogV2::Create(
                catalog_config,
                std::span(&entry, 1U),
                &catalog) !=
                market::DailyInstrumentCatalogCreateErrorV2::kNone ||
            catalog == nullptr) {
            return Fail("catalog_create", "failed");
        }
        catalog_ =
            std::shared_ptr<const market::DailyInstrumentCatalogV2>(
                std::move(catalog));
        if (market::InstrumentRuntimeStateV2::Create(
                *catalog_, &runtime_state_) !=
                market::InstrumentRuntimeStateErrorV2::kNone ||
            runtime_state_ == nullptr) {
            return Fail("runtime_state_create", "failed");
        }

        l2flow::common::Identity128 run_id{};
        run_id[0U] = std::byte{0x52};
        run_id[15U] = event_enabled_
                          ? std::byte{0x25}
                          : std::byte{0x52};
        int system_error = 0;
        if (event_enabled_) {
            ipc::RealtimePartialOrderEventServiceConfigV2
                service_config{};
            service_config.run_id = run_id;
            service_config.session_epoch = 1U;
            service_config.trade_date = kTradeDate;
            service_config.coverage_start_unix_ns = 1U;
            service_config.fast_sink = fast_;
            service_config.channel_capacity = kChannelCapacity;
            service_config.handoff_queue_capacity =
                handoff_queue_capacity;
            service_config.maximum_pending_entries =
                kMaximumPendingEntries;
            service_config.maximum_pending_entries_per_channel =
                kMaximumPendingEntriesPerChannel;
            service_config.duplicate_retention_entries =
                kDuplicateRetentionEntries;
            service_config.maximum_reorder_span =
                kMaximumReorderSpan;
            service_config.discovery_horizon = 1ms;
            service_config.maximum_shanghai_order_states =
                kMaximumOrderProjectorStates;
            service_config.maximum_shenzhen_order_states =
                kMaximumOrderProjectorStates;
            service_config.maximum_derived_events =
                kMaximumDerivedEvents;
            service_config.event_journal_capacity =
                kEventJournalCapacity;
            service_config.order_state_capacity = kOrderStateCapacity;
            service_config.maximum_order_state_updates_per_commit = 3U;
            service_config.maximum_mapping_bytes = kMaximumMappingBytes;
            service_config.lazy_commit_chunk_bytes =
                kLazyCommitChunkBytes;
            const auto create_error =
                ipc::RealtimePartialOrderEventServiceV2::Create(
                    service_config, &service_, &system_error);
            if (create_error !=
                    ipc::RealtimePartialOrderEventServiceCreateErrorV2::
                        kNone ||
                service_ == nullptr) {
                std::cerr
                    << "PARTIAL_EVENT_BENCH_ERROR"
                    << " stage=service_create"
                    << " error="
                    << ipc::
                           RealtimePartialOrderEventServiceCreateErrorNameV2(
                               create_error)
                    << " errno=" << system_error << '\n';
                return false;
            }
            if (!service_->StartWorker(&system_error)) {
                std::cerr
                    << "PARTIAL_EVENT_BENCH_ERROR"
                    << " stage=service_start"
                    << " errno=" << system_error << '\n';
                return false;
            }

            int descriptor = -1;
            if (!service_->DuplicateReadOnlyDescriptor(
                    &descriptor, &system_error)) {
                return Fail("descriptor_duplicate", "failed");
            }
            const auto session = service_->session();
            ipc::PartialOrderEventExpectedSessionV2 expected{};
            expected.run_id = session.run_id;
            expected.session_epoch = session.session_epoch;
            expected.trade_date = session.trade_date;
            expected.publication_generation =
                session.publication_generation;
            expected.correction_epoch = session.correction_epoch;
            const auto reader_error =
                ipc::PartialOrderEventReaderV2::OpenDescriptor(
                    descriptor, expected, &reader_, &system_error);
            static_cast<void>(::close(descriptor));
            if (reader_error !=
                    ipc::PartialOrderEventReaderOpenErrorV2::kNone ||
                reader_ == nullptr) {
                std::cerr
                    << "PARTIAL_EVENT_BENCH_ERROR"
                    << " stage=reader_open"
                    << " error="
                    << ipc::PartialOrderEventReaderOpenErrorNameV2(
                           reader_error)
                    << " errno=" << system_error << '\n';
                return false;
            }
        }

        runtime::RealtimePipelineConfigV1 pipeline_config{};
        pipeline_config.run_id = run_id;
        pipeline_config.trade_date = kTradeDate;
        pipeline_config.daily_catalog = catalog_;
        pipeline_config.runtime_state = runtime_state_.get();
        pipeline_config.source_stream_ids = {
            1001U, 1002U, 2001U, 2002U};
        pipeline_config.maximum_sdk_message_bytes = 4096U;
        pipeline_config.decoder_queue_capacity_per_source =
            kDecoderQueueCapacityPerSource;
        pipeline_config.completion_tracker_capacity =
            kCompletionTrackerCapacity;
        pipeline_config.tick_ring_capacity = kTickRingCapacity;
        pipeline_config.store_worker_count = 1U;
        pipeline_config.store_queue_capacity_per_source_worker =
            kStoreQueueCapacityPerSourceWorker;
        pipeline_config.intraday_store.segment_target_bytes =
            1U * 1024U * 1024U;
        pipeline_config.intraday_store.maximum_session_records =
            kRealtimeHistoryRecordCapacity;
        pipeline_config.intraday_store.maximum_session_accounted_bytes =
            kRealtimeHistoryAccountedByteCapacity;
        pipeline_config.intraday_store.maximum_records_per_batch =
            4'096U;
        pipeline_config.intraday_store.coverage_from_open = false;
        if (event_enabled_) {
            pipeline_config.applied_record_sink = service_;
            pipeline_config.native_sequence_observation_sink = service_;
        } else {
            pipeline_config.applied_record_sink = fast_;
            pipeline_config.native_sequence_observation_sink.reset();
        }
        pipeline_config.sdk.enabled = false;
        std::string detail;
        const auto pipeline_error =
            runtime::RealtimePipelineV1::Create(
                pipeline_config, &pipeline_, &detail);
        if (pipeline_error !=
                runtime::RealtimePipelineCreateErrorV1::kNone ||
            pipeline_ == nullptr) {
            std::cerr
                << "PARTIAL_EVENT_BENCH_ERROR"
                << " stage=pipeline_create"
                << " error="
                << runtime::RealtimePipelineCreateErrorNameV1(
                       pipeline_error)
                << " detail=" << detail << '\n';
            return false;
        }
        return true;
    }

    const bool event_enabled_;
    std::shared_ptr<const market::DailyInstrumentCatalogV2> catalog_;
    std::unique_ptr<market::InstrumentRuntimeStateV2> runtime_state_;
    std::shared_ptr<TimedFastSink> fast_;
    std::shared_ptr<ipc::RealtimePartialOrderEventServiceV2> service_;
    std::unique_ptr<runtime::RealtimePipelineV1> pipeline_;
    std::unique_ptr<ipc::PartialOrderEventReaderV2> reader_;
    bool stopped_ = false;
};

struct InjectTiming final {
    std::uint64_t origin_ns = 0U;
    std::uint64_t return_ns = 0U;
    runtime::RealtimePipelineIngressResultV1 ingress{};
};

[[nodiscard]] bool InjectOrder(
    BenchmarkHarness* harness,
    std::uint32_t channel,
    std::uint64_t native_sequence,
    InjectTiming* output) {
    if (harness == nullptr || harness->pipeline() == nullptr ||
        output == nullptr) {
        return false;
    }
    FakeMessage message(channel, native_sequence);
    output->origin_ns = MonotonicNowNs();
    output->ingress =
        harness->pipeline()->InjectSdkMessageForTest(&message);
    output->return_ns = MonotonicNowNs();
    return output->origin_ns != 0U &&
           output->return_ns >= output->origin_ns &&
           output->ingress.accepted();
}

[[nodiscard]] bool WaitFast(
    const BenchmarkHarness& harness,
    const InjectTiming& injection,
    std::uint64_t expected_native_sequence,
    FastPublicationTiming* output) {
    if (output == nullptr) {
        return false;
    }
    return WaitUntil([&] {
               return harness.fast()->ReadTiming(
                   injection.ingress.global_ingress_sequence,
                   output);
           }) &&
           output->native_sequence ==
               static_cast<std::int64_t>(expected_native_sequence) &&
           output->recv_monotonic_ns >= injection.origin_ns &&
           output->visible_monotonic_ns >=
               output->recv_monotonic_ns;
}

[[nodiscard]] bool WaitCanonicalEvent(
    ipc::PartialOrderEventReaderV2* reader,
    std::uint64_t expected_derived_event_sequence,
    std::uint64_t expected_canonical_sequence,
    std::uint64_t expected_native_sequence,
    ipc::PartialOrderEventEnvelopeV2* output,
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
        ipc::PartialOrderEventEnvelopeV2 candidate{};
        const std::uint64_t begin = MonotonicNowNs();
        const auto result = reader->ReadEvent(
            expected_derived_event_sequence, &candidate);
        const std::uint64_t end = MonotonicNowNs();
        if (begin == 0U || end < begin || end < start) {
            return false;
        }
        if (result == ipc::PartialOrderEventReadResultV2::kOk) {
            if (candidate.canonical_apply_sequence !=
                    expected_canonical_sequence ||
                candidate.event.derived_event_sequence !=
                    expected_derived_event_sequence ||
                candidate.event.native_event_sequence !=
                    static_cast<std::int64_t>(
                        expected_native_sequence)) {
                return false;
            }
            *output = candidate;
            *visible_ns = end;
            *successful_call_ns = end - begin;
            return true;
        }
        if (result !=
                ipc::PartialOrderEventReadResultV2::kNotYetPublished &&
            result != ipc::PartialOrderEventReadResultV2::kInconsistent) {
            return false;
        }
        if (end - start >= kTimeoutNs) {
            return false;
        }
    }
}

struct LatencySummary final {
    std::uint64_t minimum_ns = 0U;
    std::uint64_t mean_ns = 0U;
    std::uint64_t p50_ns = 0U;
    std::uint64_t p95_ns = 0U;
    std::uint64_t p99_ns = 0U;
    std::uint64_t maximum_ns = 0U;
};

struct ThroughputSummary final {
    std::uint64_t minimum_records_per_second = 0U;
    std::uint64_t p05_records_per_second = 0U;
    std::uint64_t p50_records_per_second = 0U;
    std::size_t trial_count = 0U;
};

[[nodiscard]] std::uint64_t NearestRank(
    const std::vector<std::uint64_t>& sorted,
    std::uint64_t numerator,
    std::uint64_t denominator) noexcept {
    if (sorted.empty() || denominator == 0U || numerator == 0U ||
        numerator > denominator) {
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

[[nodiscard]] ThroughputSummary SummarizeThroughput(
    std::vector<std::uint64_t> values) {
    ThroughputSummary result{};
    result.trial_count = values.size();
    if (values.empty()) {
        return result;
    }
    std::sort(values.begin(), values.end());
    result.minimum_records_per_second = values.front();
    result.p05_records_per_second = NearestRank(values, 5U, 100U);
    result.p50_records_per_second = NearestRank(values, 50U, 100U);
    return result;
}

void PrintLatency(
    std::string_view phase,
    std::string_view metric,
    const std::vector<std::uint64_t>& values) {
    const LatencySummary summary = Summarize(values);
    std::cout
        << "PARTIAL_EVENT_BENCH"
        << " kind=latency"
        << " phase=" << phase
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

struct FastPathResult final {
    std::vector<std::uint64_t> callback_call_ns;
    std::vector<std::uint64_t> callback_to_fast_ns;
    std::vector<std::uint64_t> recv_to_fast_ns;
    std::uint64_t measured_elapsed_ns = 0U;
    ipc::RealtimePartialOrderEventServiceSnapshotV2 event_snapshot{};
};

[[nodiscard]] bool RunFastPathBenchmark(
    bool event_enabled,
    FastPathResult* output) {
    if (output == nullptr) {
        return false;
    }
    *output = {};
    output->callback_call_ns.reserve(kMeasuredSamples);
    output->callback_to_fast_ns.reserve(kMeasuredSamples);
    output->recv_to_fast_ns.reserve(kMeasuredSamples);

    std::unique_ptr<BenchmarkHarness> harness;
    if (!BenchmarkHarness::Create(
            event_enabled, kNormalHandoffQueueCapacity, &harness) ||
        harness == nullptr) {
        return false;
    }
    const std::size_t total = kWarmupSamples + kMeasuredSamples;
    std::uint64_t measured_begin = 0U;
    std::uint64_t measured_end = 0U;
    for (std::size_t index = 0U; index < total; ++index) {
        const std::uint64_t native_sequence =
            static_cast<std::uint64_t>(index) + 1U;
        InjectTiming injection{};
        if (!InjectOrder(
                harness.get(), kChannel, native_sequence, &injection)) {
            return Fail("fast_ab_inject", "rejected");
        }
        FastPublicationTiming fast{};
        if (!WaitFast(*harness, injection, native_sequence, &fast)) {
            return Fail("fast_ab_publication", "correlation_failed");
        }
        if (index >= kWarmupSamples) {
            if (index == kWarmupSamples) {
                measured_begin = injection.origin_ns;
            }
            measured_end = fast.visible_monotonic_ns;
            output->callback_call_ns.push_back(
                injection.return_ns - injection.origin_ns);
            output->callback_to_fast_ns.push_back(
                fast.visible_monotonic_ns - injection.origin_ns);
            output->recv_to_fast_ns.push_back(
                fast.visible_monotonic_ns -
                fast.recv_monotonic_ns);
        }
    }
    if (measured_begin == 0U || measured_end <= measured_begin ||
        harness->pipeline()->fatal() ||
        harness->fast()->coverage_lost()) {
        return Fail("fast_ab_health", "unhealthy");
    }
    output->measured_elapsed_ns = measured_end - measured_begin;
    if (event_enabled) {
        const std::uint64_t expected_frontier =
            static_cast<std::uint64_t>(total);
        if (!WaitUntil([&] {
                const auto snapshot = harness->service()->Snapshot();
                return snapshot.canonical_apply_frontier ==
                           expected_frontier &&
                       snapshot.published_event_frontier ==
                           expected_frontier &&
                       snapshot.pending_entries == 0U;
            })) {
            return Fail("fast_ab_event_drain", "timeout");
        }
        output->event_snapshot = harness->service()->Snapshot();
        if (output->event_snapshot.globally_frozen ||
            output->event_snapshot.dropped_handoffs != 0U) {
            return Fail("fast_ab_event_health", "unhealthy");
        }
    }
    harness->Stop();
    return true;
}

void PrintFastPath(
    std::string_view composition,
    const FastPathResult& result) {
    PrintLatency(
        composition,
        "synthetic_callback_call",
        result.callback_call_ns);
    PrintLatency(
        composition,
        "synthetic_callback_origin_to_fast_first_visible",
        result.callback_to_fast_ns);
    PrintLatency(
        composition,
        "pipeline_recv_to_fast_first_visible",
        result.recv_to_fast_ns);
    const long double rate =
        static_cast<long double>(kMeasuredSamples) *
        1'000'000'000.0L /
        static_cast<long double>(result.measured_elapsed_ns);
    std::cout
        << "PARTIAL_EVENT_BENCH"
        << " kind=throughput"
        << " phase=" << composition
        << " closure_boundary=fast_first_visible"
        << " records=" << kMeasuredSamples
        << " elapsed_ns=" << result.measured_elapsed_ns
        << " records_per_second="
        << static_cast<std::uint64_t>(rate)
        << " open_loop=0"
        << " sustained=0"
        << " sla_eligible=0"
        << " validates_400k_60s=0"
        << " event_queue_high_water="
        << result.event_snapshot.handoff_queue_high_water
        << " event_pending_high_water="
        << result.event_snapshot.reorder_high_water
        << " event_dropped_handoffs="
        << result.event_snapshot.dropped_handoffs
        << " event_globally_frozen="
        << (result.event_snapshot.globally_frozen ? 1 : 0)
        << '\n';
}

[[nodiscard]] std::string SignedDifference(
    std::uint64_t enabled,
    std::uint64_t baseline) {
    if (enabled >= baseline) {
        return std::to_string(enabled - baseline);
    }
    return "-" + std::to_string(baseline - enabled);
}

void PrintFastP99AbMetric(
    std::string_view metric,
    const std::vector<std::uint64_t>& fast_only,
    const std::vector<std::uint64_t>& fast_plus_event) {
    const std::uint64_t baseline = Summarize(fast_only).p99_ns;
    const std::uint64_t enabled = Summarize(fast_plus_event).p99_ns;
    std::cout
        << "PARTIAL_EVENT_BENCH"
        << " kind=fast_ab"
        << " phase=fast_visibility_closed_loop"
        << " closure_boundary=fast_first_visible"
        << " metric=" << metric
        << " quantile=p99"
        << " unit=ns"
        << " fast_only=" << baseline
        << " fast_plus_event=" << enabled
        << " absolute_delta="
        << SignedDifference(enabled, baseline)
        << " percentage_delta_available="
        << (baseline != 0U ? 1 : 0);
    if (baseline != 0U) {
        const long double percentage =
            (static_cast<long double>(enabled) -
             static_cast<long double>(baseline)) *
            100.0L / static_cast<long double>(baseline);
        std::cout << " percentage_delta="
                  << static_cast<double>(percentage);
    }
    std::cout
        << " hard_threshold=none"
        << " sla_eligible=0"
        << '\n';
}

struct NormalResult final {
    std::vector<std::uint64_t> callback_call_ns;
    std::vector<std::uint64_t> callback_to_fast_ns;
    std::vector<std::uint64_t> recv_to_fast_ns;
    std::vector<std::uint64_t> callback_to_event_ns;
    std::vector<std::uint64_t> recv_to_event_ns;
    std::vector<std::uint64_t> fast_to_event_ns;
    std::vector<std::uint64_t> event_read_call_ns;
    std::uint64_t measured_elapsed_ns = 0U;
    std::uint64_t observed_source_to_canonical_lag_high_water = 0U;
    std::uint64_t observed_source_to_event_lag_high_water = 0U;
    std::uint64_t observed_queue_backlog_high_water = 0U;
    std::uint64_t observed_pending_high_water = 0U;
    ipc::RealtimePartialOrderEventServiceSnapshotV2 snapshot{};
};

[[nodiscard]] bool RunNormalBenchmark(NormalResult* output) {
    if (output == nullptr) {
        return false;
    }
    *output = {};
    output->callback_call_ns.reserve(kMeasuredSamples);
    output->callback_to_fast_ns.reserve(kMeasuredSamples);
    output->recv_to_fast_ns.reserve(kMeasuredSamples);
    output->callback_to_event_ns.reserve(kMeasuredSamples);
    output->recv_to_event_ns.reserve(kMeasuredSamples);
    output->fast_to_event_ns.reserve(kMeasuredSamples);
    output->event_read_call_ns.reserve(kMeasuredSamples);

    std::unique_ptr<BenchmarkHarness> harness;
    if (!BenchmarkHarness::Create(
            true, kNormalHandoffQueueCapacity, &harness) ||
        harness == nullptr) {
        return false;
    }
    const std::size_t total = kWarmupSamples + kMeasuredSamples;
    std::uint64_t measured_begin = 0U;
    std::uint64_t measured_end = 0U;
    for (std::size_t index = 0U; index < total; ++index) {
        const std::uint64_t native_sequence =
            static_cast<std::uint64_t>(index) + 1U;
        InjectTiming injection{};
        if (!InjectOrder(
                harness.get(), kChannel, native_sequence, &injection)) {
            return Fail("normal_inject", "rejected");
        }
        FastPublicationTiming fast{};
        if (!WaitFast(
                *harness, injection, native_sequence, &fast)) {
            return Fail("normal_fast", "correlation_failed");
        }
        const auto pre_event_snapshot = harness->service()->Snapshot();
        output->observed_source_to_canonical_lag_high_water = std::max(
            output->observed_source_to_canonical_lag_high_water,
            NonnegativeDifference(
                pre_event_snapshot.captured_source_frontier,
                pre_event_snapshot.canonical_apply_frontier));
        output->observed_source_to_event_lag_high_water = std::max(
            output->observed_source_to_event_lag_high_water,
            NonnegativeDifference(
                pre_event_snapshot.captured_source_frontier,
                pre_event_snapshot.published_event_frontier));
        output->observed_queue_backlog_high_water = std::max(
            output->observed_queue_backlog_high_water,
            pre_event_snapshot.handoff_queue_depth);
        output->observed_pending_high_water = std::max(
            output->observed_pending_high_water,
            pre_event_snapshot.pending_entries);
        ipc::PartialOrderEventEnvelopeV2 event{};
        std::uint64_t event_visible = 0U;
        std::uint64_t read_call = 0U;
        if (!WaitCanonicalEvent(
                harness->reader(),
                native_sequence,
                native_sequence,
                native_sequence,
                &event,
                &event_visible,
                &read_call) ||
            event.event.ingress_sequence !=
                injection.ingress.global_ingress_sequence ||
            event.event.recv_monotonic_ns <= 0 ||
            static_cast<std::uint64_t>(
                event.event.recv_monotonic_ns) !=
                fast.recv_monotonic_ns ||
            event_visible < fast.visible_monotonic_ns) {
            return Fail("normal_event", "correlation_failed");
        }
        if (index >= kWarmupSamples) {
            if (index == kWarmupSamples) {
                measured_begin = injection.origin_ns;
            }
            measured_end = event_visible;
            output->callback_call_ns.push_back(
                injection.return_ns - injection.origin_ns);
            output->callback_to_fast_ns.push_back(
                fast.visible_monotonic_ns - injection.origin_ns);
            output->recv_to_fast_ns.push_back(
                fast.visible_monotonic_ns -
                fast.recv_monotonic_ns);
            output->callback_to_event_ns.push_back(
                event_visible - injection.origin_ns);
            output->recv_to_event_ns.push_back(
                event_visible - fast.recv_monotonic_ns);
            output->fast_to_event_ns.push_back(
                event_visible - fast.visible_monotonic_ns);
            output->event_read_call_ns.push_back(read_call);
        }
    }
    if (measured_begin == 0U || measured_end <= measured_begin) {
        return Fail("normal_clock", "invalid_interval");
    }
    output->measured_elapsed_ns = measured_end - measured_begin;
    output->snapshot = harness->service()->Snapshot();
    const auto expected_frontier = static_cast<std::uint64_t>(total);
    if (harness->pipeline()->fatal() ||
        harness->fast()->coverage_lost() ||
        output->snapshot.globally_frozen ||
        output->snapshot.dropped_handoffs != 0U ||
        output->snapshot.canonical_apply_frontier != expected_frontier ||
        output->snapshot.published_event_frontier != expected_frontier ||
        output->snapshot.captured_source_frontier != expected_frontier ||
        output->snapshot.pending_entries != 0U) {
        return Fail("normal_health", "unhealthy");
    }
    harness->Stop();
    return true;
}

void PrintNormal(const NormalResult& result) {
    PrintLatency(
        "normal",
        "synthetic_callback_call",
        result.callback_call_ns);
    PrintLatency(
        "normal",
        "synthetic_callback_origin_to_fast_first_visible",
        result.callback_to_fast_ns);
    PrintLatency(
        "normal",
        "pipeline_recv_to_fast_first_visible",
        result.recv_to_fast_ns);
    PrintLatency(
        "normal",
        "synthetic_callback_origin_to_canonical_event_reader_visible_upper_bound",
        result.callback_to_event_ns);
    PrintLatency(
        "normal",
        "pipeline_recv_to_canonical_event_reader_visible_upper_bound",
        result.recv_to_event_ns);
    PrintLatency(
        "normal",
        "fast_first_visible_to_canonical_event_reader_visible_upper_bound",
        result.fast_to_event_ns);
    PrintLatency(
        "normal",
        "event_read_successful_call",
        result.event_read_call_ns);
    const long double rate =
        static_cast<long double>(kMeasuredSamples) *
        1'000'000'000.0L /
        static_cast<long double>(result.measured_elapsed_ns);
    std::cout
        << "PARTIAL_EVENT_BENCH"
        << " kind=throughput"
        << " phase=normal_closed_loop"
        << " records=" << kMeasuredSamples
        << " elapsed_ns=" << result.measured_elapsed_ns
        << " records_per_second="
        << static_cast<std::uint64_t>(rate)
        << " open_loop=0"
        << " sustained=0"
        << " sla_eligible=0"
        << " validates_400k_60s=0"
        << " queue_high_water="
        << result.snapshot.handoff_queue_high_water
        << " pending_high_water="
        << result.snapshot.reorder_high_water
        << " dropped_handoffs="
        << result.snapshot.dropped_handoffs
        << " globally_frozen="
        << (result.snapshot.globally_frozen ? 1 : 0)
        << '\n';

    const std::uint64_t source_to_canonical_lag =
        NonnegativeDifference(
            result.snapshot.captured_source_frontier,
            result.snapshot.canonical_apply_frontier);
    const std::uint64_t source_to_event_lag =
        NonnegativeDifference(
            result.snapshot.captured_source_frontier,
            result.snapshot.published_event_frontier);
    std::cout
        << "PARTIAL_EVENT_BENCH"
        << " kind=progress_lag"
        << " phase=normal_closed_loop"
        << " source_counter_domain=process_local_captured_source"
        << " lag_high_water_sampling=after_fast_before_event_read"
        << " workload_one_event_per_canonical_input=1"
        << " captured_source_frontier="
        << result.snapshot.captured_source_frontier
        << " canonical_apply_frontier="
        << result.snapshot.canonical_apply_frontier
        << " published_event_frontier="
        << result.snapshot.published_event_frontier
        << " source_to_canonical_lag_records="
        << source_to_canonical_lag
        << " source_to_event_lag_records=" << source_to_event_lag
        << " observed_source_to_canonical_lag_high_water_records="
        << result.observed_source_to_canonical_lag_high_water
        << " observed_source_to_event_lag_high_water_records="
        << result.observed_source_to_event_lag_high_water
        << " handoff_queue_backlog_current="
        << result.snapshot.handoff_queue_depth
        << " observed_handoff_queue_backlog_high_water="
        << result.observed_queue_backlog_high_water
        << " exact_handoff_queue_reservation_high_water="
        << result.snapshot.handoff_queue_high_water
        << " reorder_pending_current="
        << result.snapshot.pending_entries
        << " observed_reorder_pending_high_water="
        << result.observed_pending_high_water
        << " exact_reorder_pending_high_water="
        << result.snapshot.reorder_high_water
        << " enqueued_handoffs="
        << result.snapshot.enqueued_handoffs
        << " processed_handoffs="
        << result.snapshot.processed_handoffs
        << '\n';
}

struct GapResult final {
    std::uint64_t suffix = 0U;
    std::vector<std::uint64_t> repair_callback_ns;
    std::vector<std::uint64_t> reorder_dwell_before_repair_ns;
    std::vector<std::uint64_t> oldest_record_total_dwell_ns;
    std::vector<std::uint64_t> catchup_total_ns;
    std::vector<std::uint64_t> catchup_records_per_second;
    std::uint64_t queue_high_water = 0U;
    std::uint64_t pending_high_water = 0U;
    std::uint64_t dropped_handoffs = 0U;
    bool globally_frozen = false;
    bool fast_available_during_gap = true;
    bool event_last_good_available_during_gap = true;
};

[[nodiscard]] bool RunGapBenchmark(
    std::array<GapResult, kGapSuffixes.size()>* output) {
    if (output == nullptr) {
        return false;
    }
    std::unique_ptr<BenchmarkHarness> harness;
    if (!BenchmarkHarness::Create(
            true, kNormalHandoffQueueCapacity, &harness) ||
        harness == nullptr) {
        return false;
    }

    InjectTiming anchor_injection{};
    if (!InjectOrder(
            harness.get(), kChannel, 1U, &anchor_injection)) {
        return Fail("gap_anchor_inject", "rejected");
    }
    FastPublicationTiming anchor_fast{};
    ipc::PartialOrderEventEnvelopeV2 anchor_event{};
    std::uint64_t anchor_visible = 0U;
    std::uint64_t anchor_read_call = 0U;
    if (!WaitFast(*harness, anchor_injection, 1U, &anchor_fast) ||
        !WaitCanonicalEvent(
            harness->reader(),
            1U,
            1U,
            1U,
            &anchor_event,
            &anchor_visible,
            &anchor_read_call)) {
        return Fail("gap_anchor", "not_visible");
    }

    std::uint64_t next_native = 2U;
    std::uint64_t canonical_frontier = 1U;
    std::uint64_t event_frontier = 1U;
    for (std::size_t suffix_index = 0U;
         suffix_index < kGapSuffixes.size();
         ++suffix_index) {
        GapResult& result = (*output)[suffix_index];
        result = {};
        result.suffix = kGapSuffixes[suffix_index];
        result.repair_callback_ns.reserve(kGapMeasuredTrials);
        result.reorder_dwell_before_repair_ns.reserve(
            kGapMeasuredTrials);
        result.oldest_record_total_dwell_ns.reserve(
            kGapMeasuredTrials);
        result.catchup_total_ns.reserve(kGapMeasuredTrials);
        result.catchup_records_per_second.reserve(
            kGapMeasuredTrials);

        const std::size_t trials =
            kGapWarmupTrials + kGapMeasuredTrials;
        for (std::size_t trial = 0U; trial < trials; ++trial) {
            const std::uint64_t missing = next_native;
            const std::uint64_t future_last =
                missing + result.suffix;
            InjectTiming first_future{};
            InjectTiming last_future{};
            for (std::uint64_t sequence = missing + 1U;
                 sequence <= future_last;
                 ++sequence) {
                InjectTiming injection{};
                if (!InjectOrder(
                        harness.get(),
                        kChannel,
                        sequence,
                        &injection)) {
                    return Fail("gap_suffix_inject", "rejected");
                }
                if (sequence == missing + 1U) {
                    first_future = injection;
                }
                last_future = injection;
            }

            FastPublicationTiming first_fast{};
            FastPublicationTiming last_fast{};
            if (!WaitFast(
                    *harness,
                    first_future,
                    missing + 1U,
                    &first_fast) ||
                !WaitFast(
                    *harness,
                    last_future,
                    future_last,
                    &last_fast) ||
                !WaitUntil([&] {
                    const auto snapshot =
                        harness->service()->Snapshot();
                    return snapshot.canonical_apply_frontier ==
                               canonical_frontier &&
                           snapshot.pending_entries == result.suffix &&
                           snapshot.gap_channel_count == 1U;
                })) {
                return Fail("gap_prepare", "not_pending");
            }

            ipc::PartialOrderEventStatusSnapshotV2 status{};
            ipc::PartialOrderEventEnvelopeV2 last_good{};
            const bool last_good_available =
                harness->reader()->ReadStatus(&status) ==
                    ipc::PartialOrderEventReadResultV2::kOk &&
                status.cut.canonical_apply_frontier ==
                    canonical_frontier &&
                status.cut.event_published_frontier == event_frontier &&
                status.ordering_quality ==
                    ipc::PartialOrderEventOrderingQualityV2::
                        kBoundedReorderedPartial &&
                harness->reader()->ReadEvent(
                    event_frontier, &last_good) ==
                    ipc::PartialOrderEventReadResultV2::kOk &&
                last_good.canonical_apply_sequence ==
                    canonical_frontier;
            result.event_last_good_available_during_gap =
                result.event_last_good_available_during_gap &&
                last_good_available;
            result.fast_available_during_gap =
                result.fast_available_during_gap &&
                last_fast.native_sequence ==
                    static_cast<std::int64_t>(future_last);
            if (!last_good_available ||
                last_fast.native_sequence !=
                    static_cast<std::int64_t>(future_last)) {
                return Fail("gap_availability", "invariant_failed");
            }

            const auto prepared_snapshot =
                harness->service()->Snapshot();
            result.queue_high_water = std::max(
                result.queue_high_water,
                prepared_snapshot.handoff_queue_high_water);
            result.pending_high_water = std::max(
                result.pending_high_water,
                prepared_snapshot.reorder_high_water);
            std::this_thread::sleep_for(kGapHold);

            InjectTiming repair{};
            if (!InjectOrder(
                    harness.get(), kChannel, missing, &repair)) {
                return Fail("gap_repair_inject", "rejected");
            }
            const std::uint64_t expected_frontier =
                canonical_frontier + result.suffix + 1U;
            const std::uint64_t expected_event_frontier =
                event_frontier + result.suffix + 1U;
            ipc::PartialOrderEventEnvelopeV2 repaired{};
            std::uint64_t catchup_visible = 0U;
            std::uint64_t catchup_read_call = 0U;
            if (!WaitCanonicalEvent(
                    harness->reader(),
                    expected_event_frontier,
                    expected_frontier,
                    future_last,
                    &repaired,
                    &catchup_visible,
                    &catchup_read_call) ||
                !WaitUntil([&] {
                    const auto snapshot =
                        harness->service()->Snapshot();
                    return snapshot.canonical_apply_frontier ==
                               expected_frontier &&
                           snapshot.pending_entries == 0U &&
                           snapshot.gap_channel_count == 0U &&
                           !snapshot.globally_frozen;
                })) {
                return Fail("gap_catchup", "not_repaired");
            }

            if (trial >= kGapWarmupTrials) {
                if (repair.origin_ns <
                        first_fast.recv_monotonic_ns ||
                    catchup_visible <= repair.origin_ns ||
                    catchup_visible <
                        first_fast.recv_monotonic_ns) {
                    return Fail("gap_clock", "invalid_interval");
                }
                const std::uint64_t catchup_ns =
                    catchup_visible - repair.origin_ns;
                result.repair_callback_ns.push_back(
                    repair.return_ns - repair.origin_ns);
                result.reorder_dwell_before_repair_ns.push_back(
                    repair.origin_ns -
                    first_fast.recv_monotonic_ns);
                result.oldest_record_total_dwell_ns.push_back(
                    catchup_visible -
                    first_fast.recv_monotonic_ns);
                result.catchup_total_ns.push_back(catchup_ns);
                const long double rate =
                    static_cast<long double>(result.suffix + 1U) *
                    1'000'000'000.0L /
                    static_cast<long double>(catchup_ns);
                result.catchup_records_per_second.push_back(
                    static_cast<std::uint64_t>(rate));
            }
            canonical_frontier = expected_frontier;
            event_frontier = expected_event_frontier;
            next_native = future_last + 1U;
        }
        const auto snapshot = harness->service()->Snapshot();
        result.queue_high_water = std::max(
            result.queue_high_water,
            snapshot.handoff_queue_high_water);
        result.pending_high_water = std::max(
            result.pending_high_water,
            snapshot.reorder_high_water);
        result.dropped_handoffs = snapshot.dropped_handoffs;
        result.globally_frozen = snapshot.globally_frozen;
    }

    const auto snapshot = harness->service()->Snapshot();
    if (harness->pipeline()->fatal() ||
        harness->fast()->coverage_lost() ||
        snapshot.globally_frozen ||
        snapshot.dropped_handoffs != 0U ||
        snapshot.canonical_apply_frontier != canonical_frontier ||
        snapshot.published_event_frontier != event_frontier) {
        return Fail("gap_final_health", "unhealthy");
    }
    harness->Stop();
    return true;
}

void PrintGap(
    const std::array<GapResult, kGapSuffixes.size()>& results) {
    for (const GapResult& result : results) {
        const LatencySummary callback =
            Summarize(result.repair_callback_ns);
        const LatencySummary before_repair =
            Summarize(result.reorder_dwell_before_repair_ns);
        const LatencySummary total_dwell =
            Summarize(result.oldest_record_total_dwell_ns);
        const LatencySummary catchup =
            Summarize(result.catchup_total_ns);
        const ThroughputSummary throughput =
            SummarizeThroughput(result.catchup_records_per_second);
        std::cout
            << "PARTIAL_EVENT_BENCH"
            << " kind=reorder"
            << " phase=gap_repair"
            << " suffix=" << result.suffix
            << " catchup_records=" << result.suffix + 1U
            << " configured_hold_ns=" << kGapHold.count()
            << " warmup_trials=" << kGapWarmupTrials
            << " measured_trials=" << kGapMeasuredTrials
            << " estimator=nearest_rank"
            << " repair_callback_p50_ns=" << callback.p50_ns
            << " repair_callback_p95_ns=" << callback.p95_ns
            << " repair_callback_p99_ns=" << callback.p99_ns
            << " reorder_dwell_before_repair_p50_ns="
            << before_repair.p50_ns
            << " reorder_dwell_before_repair_p95_ns="
            << before_repair.p95_ns
            << " reorder_dwell_before_repair_p99_ns="
            << before_repair.p99_ns
            << " oldest_record_total_dwell_p50_ns="
            << total_dwell.p50_ns
            << " oldest_record_total_dwell_p95_ns="
            << total_dwell.p95_ns
            << " oldest_record_total_dwell_p99_ns="
            << total_dwell.p99_ns
            << " catchup_total_p50_ns=" << catchup.p50_ns
            << " catchup_total_p95_ns=" << catchup.p95_ns
            << " catchup_total_p99_ns=" << catchup.p99_ns
            << " throughput_min_records_per_s="
            << throughput.minimum_records_per_second
            << " throughput_p05_records_per_s="
            << throughput.p05_records_per_second
            << " throughput_p50_records_per_s="
            << throughput.p50_records_per_second
            << " throughput_trial_count=" << throughput.trial_count
            << " queue_high_water=" << result.queue_high_water
            << " pending_high_water=" << result.pending_high_water
            << " dropped_handoffs=" << result.dropped_handoffs
            << " globally_frozen="
            << (result.globally_frozen ? 1 : 0)
            << " fast_available_during_gap="
            << (result.fast_available_during_gap ? 1 : 0)
            << " event_last_good_available_during_gap="
            << (result.event_last_good_available_during_gap ? 1 : 0)
            << '\n';
    }
}

struct PressureResult final {
    std::uint64_t queue_capacity = 0U;
    std::uint64_t queue_high_water = 0U;
    std::uint64_t pending_high_water = 0U;
    std::uint64_t dropped_handoffs = 0U;
    bool globally_frozen = false;
    bool fast_available_after_freeze = false;
    bool event_history_last_good_available = false;
    bool order_state_last_good_available = false;
};

[[nodiscard]] bool RunPressureBenchmark(PressureResult* output) {
    if (output == nullptr) {
        return false;
    }
    *output = {};
    output->queue_capacity = kPressureQueueCapacity;
    std::unique_ptr<BenchmarkHarness> harness;
    if (!BenchmarkHarness::Create(
            true, kPressureQueueCapacity, &harness) ||
        harness == nullptr) {
        return false;
    }

    InjectTiming anchor_injection{};
    FastPublicationTiming anchor_fast{};
    ipc::PartialOrderEventEnvelopeV2 anchor_event{};
    std::uint64_t anchor_visible = 0U;
    std::uint64_t anchor_read_call = 0U;
    if (!InjectOrder(
            harness.get(), kChannel, 1U, &anchor_injection) ||
        !WaitFast(*harness, anchor_injection, 1U, &anchor_fast) ||
        !WaitCanonicalEvent(
            harness->reader(),
            1U,
            1U,
            1U,
            &anchor_event,
            &anchor_visible,
            &anchor_read_call)) {
        return Fail("pressure_anchor", "not_visible");
    }

    harness->service()->SetWorkerPausedForTest(true);
    if (!WaitUntil([&] {
            return harness->service()->WorkerPauseReachedForTest();
        })) {
        return Fail("pressure_pause", "not_reached");
    }
    realtime::NativeSequenceObservationV1 observation{};
    observation.descriptor.domain.market =
        realtime::NativeSequenceMarketV1::kShenzhen;
    observation.descriptor.domain.channel = 13U;
    observation.message_key = sdk::MessageKey{6U, 101U, 33U};
    observation.record_class =
        realtime::NativeSequenceRecoveryRecordClassV1::kFiltered;
    for (std::uint64_t sequence = 1U;
         sequence <= kPressureQueueCapacity + 1U;
         ++sequence) {
        observation.descriptor.sequence = sequence;
        harness->service()->ObserveNativeSequence(observation);
    }
    const auto pressured = harness->service()->Snapshot();
    if (!pressured.globally_frozen ||
        pressured.dropped_handoffs == 0U ||
        pressured.handoff_queue_depth != kPressureQueueCapacity ||
        pressured.handoff_queue_high_water != kPressureQueueCapacity) {
        return Fail("pressure_overflow", "invariant_failed");
    }
    harness->service()->SetWorkerPausedForTest(false);
    if (!WaitUntil([&] {
            return harness->service()->Snapshot().handoff_queue_depth ==
                0U;
        })) {
        return Fail("pressure_drain", "timeout");
    }

    InjectTiming after_freeze{};
    FastPublicationTiming after_freeze_fast{};
    if (!InjectOrder(
            harness.get(), kChannel, 2U, &after_freeze) ||
        !WaitFast(*harness, after_freeze, 2U, &after_freeze_fast)) {
        return Fail("pressure_fast_after_freeze", "not_available");
    }

    ipc::PartialOrderEventStatusSnapshotV2 status{};
    ipc::PartialOrderEventEnvelopeV2 last_good_event{};
    ipc::PartialOrderEventOrderStateV2 last_good_state{};
    ipc::PartialOrderEventOrderKeyV2 key{};
    key.market =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHENZHEN_V1;
    key.instrument_id = 1U;
    key.channel = static_cast<std::int64_t>(kChannel);
    key.order_id = 1;
    if (!WaitUntil([&] {
            return harness->reader()->ReadStatus(&status) ==
                       ipc::PartialOrderEventReadResultV2::kOk &&
                   status.cut.state ==
                       ipc::PartialOrderEventServiceStateV2::
                           kFrozenResource;
        })) {
        return Fail("pressure_status", "not_published");
    }
    output->event_history_last_good_available =
        harness->reader()->ReadEvent(1U, &last_good_event) ==
            ipc::PartialOrderEventReadResultV2::kOk &&
        last_good_event.canonical_apply_sequence == 1U &&
        status.cut.canonical_apply_frontier == 1U &&
        status.cut.event_published_frontier == 1U;
    output->order_state_last_good_available =
        harness->reader()->FindOrderState(
            key, &last_good_state) ==
            ipc::PartialOrderEventReadResultV2::kOk &&
        last_good_state.canonical_apply_sequence == 1U &&
        last_good_state.order_revision.native_event_sequence == 1;
    output->fast_available_after_freeze =
        harness->fast()->count() == 2U &&
        after_freeze_fast.native_sequence == 2 &&
        !harness->pipeline()->fatal() &&
        !harness->fast()->coverage_lost();
    const auto final_snapshot = harness->service()->Snapshot();
    output->queue_high_water =
        final_snapshot.handoff_queue_high_water;
    output->pending_high_water = final_snapshot.reorder_high_water;
    output->dropped_handoffs = final_snapshot.dropped_handoffs;
    output->globally_frozen = final_snapshot.globally_frozen;
    if (!output->fast_available_after_freeze ||
        !output->event_history_last_good_available ||
        !output->order_state_last_good_available ||
        !output->globally_frozen || output->dropped_handoffs == 0U) {
        return Fail("pressure_final_health", "invariant_failed");
    }
    harness->Stop();
    return true;
}

void PrintPressure(const PressureResult& result) {
    std::cout
        << "PARTIAL_EVENT_BENCH"
        << " kind=fault_isolation"
        << " phase=event_handoff_queue_overflow"
        << " queue_capacity=" << result.queue_capacity
        << " queue_high_water=" << result.queue_high_water
        << " pending_high_water=" << result.pending_high_water
        << " dropped_handoffs=" << result.dropped_handoffs
        << " globally_frozen="
        << (result.globally_frozen ? 1 : 0)
        << " fast_available_after_freeze="
        << (result.fast_available_after_freeze ? 1 : 0)
        << " event_history_last_good_available="
        << (result.event_history_last_good_available ? 1 : 0)
        << " order_state_last_good_available="
        << (result.order_state_last_good_available ? 1 : 0)
        << '\n';
}

enum class BenchmarkMode : std::uint8_t {
    kAll = 0U,
    kNormal,
    kGap,
    kPressure,
    kSelfTest,
};

[[nodiscard]] bool RunStatisticsSelfTest() {
    const ThroughputSummary summary =
        SummarizeThroughput({300U, 100U, 200U});
    if (summary.minimum_records_per_second != 100U ||
        summary.p05_records_per_second != 100U ||
        summary.p50_records_per_second != 200U ||
        summary.trial_count != 3U) {
        std::cerr
            << "throughput summary regression: min="
            << summary.minimum_records_per_second
            << " p05=" << summary.p05_records_per_second
            << " p50=" << summary.p50_records_per_second
            << " trial_count=" << summary.trial_count << '\n';
        return false;
    }
    std::cout << "PASS: partial Event throughput statistics v2\n";
    return true;
}

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
    if (argc != 2 || argv[1] == nullptr) {
        return false;
    }
    const std::string_view argument(argv[1]);
    if (argument == "--all") {
        *output = BenchmarkMode::kAll;
    } else if (argument == "--normal") {
        *output = BenchmarkMode::kNormal;
    } else if (argument == "--gap") {
        *output = BenchmarkMode::kGap;
    } else if (argument == "--pressure") {
        *output = BenchmarkMode::kPressure;
    } else if (argument == "--self-test") {
        *output = BenchmarkMode::kSelfTest;
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
            << " [--all|--normal|--gap|--pressure|--self-test]\n";
        return 2;
    }
    if (mode == BenchmarkMode::kSelfTest) {
        return RunStatisticsSelfTest() ? 0 : 1;
    }

    const CpuAffinitySnapshot affinity = ReadCpuAffinity();
    std::cout
        << "PARTIAL_EVENT_BENCH"
        << " kind=meta"
        << " schema=2"
        << " workload=synthetic_shenzhen_order_single_instrument"
        << " callback_path=InjectSdkMessageForTest"
        << " sdk_enabled=0"
        << " feeder_connected=0"
        << " reader_visibility=polling_upper_bound"
        << " temporal_coverage=PROCESS_START"
        << " ordering_quality=BOUNDED_REORDERED_PARTIAL"
        << " native_completeness_proven=0"
        << " authoritative_ordered_marker=0"
        << " writer_ack=0"
        << " manifest_barrier=0"
        << " fast_ab_available=1"
        << " fast_ab_scope=homogeneous_fast_visibility_closed_loop_p99"
        << " fast_ab_closure_boundary=fast_first_visible"
        << " fast_ab_event_may_backlog_during_measurement=1"
        << " fast_ab_event_drain=verified_after_measurement"
        << " normal_workload=one_record_in_flight_closed_loop"
        << " normal_closed_loop_open_loop=0"
        << " normal_closed_loop_sustained=0"
        << " normal_closed_loop_sla_eligible=0"
        << " validates_400k_60s=0"
        << " warmup_samples=" << kWarmupSamples
        << " measured_samples=" << kMeasuredSamples
        << " event_projector_history_capacity="
        << kMaximumDerivedEvents
        << " partial_event_history_capacity="
        << kEventJournalCapacity
        << " event_journal_capacity=" << kEventJournalCapacity
        << " order_state_capacity=" << kOrderStateCapacity
        << " shanghai_order_projector_state_capacity="
        << kMaximumOrderProjectorStates
        << " shenzhen_order_projector_state_capacity="
        << kMaximumOrderProjectorStates
        << " handoff_queue_capacity_normal="
        << kNormalHandoffQueueCapacity
        << " handoff_queue_capacity_pressure="
        << kPressureQueueCapacity
        << " maximum_pending_entries=" << kMaximumPendingEntries
        << " maximum_pending_entries_per_channel="
        << kMaximumPendingEntriesPerChannel
        << " duplicate_retention_entries="
        << kDuplicateRetentionEntries
        << " maximum_reorder_span=" << kMaximumReorderSpan
        << " realtime_history_record_capacity="
        << kRealtimeHistoryRecordCapacity
        << " realtime_history_accounted_byte_capacity="
        << kRealtimeHistoryAccountedByteCapacity
        << " tick_ring_capacity=" << kTickRingCapacity
        << " maximum_mapping_bytes=" << kMaximumMappingBytes
        << " affinity_available="
        << (affinity.available ? 1 : 0)
        << " allowed_cpu_affinity=" << affinity.allowed_cpus
        << " affinity_errno=" << affinity.system_error
        << " quantile=nearest_rank"
        << " clock=CLOCK_MONOTONIC"
        << " clock_api=clock_gettime"
        << " rusage_api=getrusage_RUSAGE_SELF"
        << " rusage_cpu_faults_scope=phase_delta"
        << " rusage_max_rss_unit=KiB"
        << " rusage_max_rss_scope=process_peak_not_phase_delta"
        << '\n';

    if (mode == BenchmarkMode::kAll ||
        mode == BenchmarkMode::kNormal) {
        FastPathResult fast_only{};
        const ResourceUsageSnapshot fast_only_before =
            ReadResourceUsage();
        if (!RunFastPathBenchmark(false, &fast_only)) {
            return 1;
        }
        const ResourceUsageSnapshot fast_only_after =
            ReadResourceUsage();
        PrintFastPath("fast_ab_fast_only", fast_only);
        PrintResourceUsage(
            "fast_ab_fast_only", fast_only_before, fast_only_after);

        FastPathResult fast_plus_event{};
        const ResourceUsageSnapshot fast_plus_event_before =
            ReadResourceUsage();
        if (!RunFastPathBenchmark(true, &fast_plus_event)) {
            return 1;
        }
        const ResourceUsageSnapshot fast_plus_event_after =
            ReadResourceUsage();
        PrintFastPath("fast_ab_fast_plus_event", fast_plus_event);
        PrintResourceUsage(
            "fast_ab_fast_plus_event",
            fast_plus_event_before,
            fast_plus_event_after);

        NormalResult result{};
        const ResourceUsageSnapshot event_before = ReadResourceUsage();
        if (!RunNormalBenchmark(&result)) {
            return 1;
        }
        const ResourceUsageSnapshot event_after = ReadResourceUsage();
        PrintNormal(result);
        PrintResourceUsage(
            "normal_fast_plus_event", event_before, event_after);
        PrintFastP99AbMetric(
            "synthetic_callback_call",
            fast_only.callback_call_ns,
            fast_plus_event.callback_call_ns);
        PrintFastP99AbMetric(
            "synthetic_callback_origin_to_fast_first_visible",
            fast_only.callback_to_fast_ns,
            fast_plus_event.callback_to_fast_ns);
        PrintFastP99AbMetric(
            "pipeline_recv_to_fast_first_visible",
            fast_only.recv_to_fast_ns,
            fast_plus_event.recv_to_fast_ns);
    }
    if (mode == BenchmarkMode::kAll ||
        mode == BenchmarkMode::kGap) {
        std::array<GapResult, kGapSuffixes.size()> results{};
        const ResourceUsageSnapshot before = ReadResourceUsage();
        if (!RunGapBenchmark(&results)) {
            return 1;
        }
        const ResourceUsageSnapshot after = ReadResourceUsage();
        PrintGap(results);
        PrintResourceUsage("gap_repair", before, after);
    }
    if (mode == BenchmarkMode::kAll ||
        mode == BenchmarkMode::kPressure) {
        PressureResult result{};
        const ResourceUsageSnapshot before = ReadResourceUsage();
        if (!RunPressureBenchmark(&result)) {
            return 1;
        }
        const ResourceUsageSnapshot after = ReadResourceUsage();
        PrintPressure(result);
        PrintResourceUsage("queue_pressure", before, after);
    }
    return 0;
}
