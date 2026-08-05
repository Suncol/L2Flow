#include "callback_polars_bridge_v1.h"

#include "l2flow/common/linux_thread_affinity_v1.h"
#include "l2flow/ipc/instrument_data_service_v3.h"
#include "l2flow/market/daily_instrument_catalog_v2.h"
#include "l2flow/runtime/fast_tick_pipeline_v1.h"
#include "l2flow/sdk/market_message_catalog_v1.h"
#include "l2flow/sdk/vendor_head_view.h"

#include "mdl_api.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace {

namespace market = l2flow::market;
namespace mdl = datayes::mdl;
namespace ipc = l2flow::ipc;
namespace runtime = l2flow::runtime;
namespace sdk = l2flow::sdk;

constexpr std::uint32_t kTradeDate = 20260805U;
constexpr std::uint64_t kNanosecondsPerSecond = 1'000'000'000U;
constexpr std::uint64_t kRealtimeBaseNs =
    1'785'859'200'000'000'000U;

[[nodiscard]] std::uint64_t MonotonicNowNs() noexcept {
    const auto count = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    return count <= 0 ? 0U : static_cast<std::uint64_t>(count);
}

[[nodiscard]] std::uint64_t RealtimeNowNs() noexcept {
    const auto count = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    return count <= 0 ? kRealtimeBaseNs : static_cast<std::uint64_t>(count);
}

void WriteDetail(
    char* output,
    std::size_t capacity,
    std::string_view value) noexcept {
    if (output == nullptr || capacity == 0U) {
        return;
    }
    const std::size_t count = std::min(capacity - 1U, value.size());
    if (count != 0U) {
        std::memcpy(output, value.data(), count);
    }
    output[count] = '\0';
}

class WireWriter final {
public:
    explicit WireWriter(std::size_t fixed_bytes)
        : bytes_(fixed_bytes, std::byte{0U}) {}

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
        StoreU16(descriptor, static_cast<std::uint16_t>(value.size()));
        StoreU32(
            descriptor + sizeof(std::uint16_t),
            value.empty()
                ? 0U
                : static_cast<std::uint32_t>(start - descriptor));
        const auto characters =
            std::span<const char>(value.data(), value.size());
        const auto bytes = std::as_bytes(characters);
        bytes_.insert(bytes_.end(), bytes.begin(), bytes.end());
    }
    [[nodiscard]] std::vector<std::byte> Take() && {
        return std::move(bytes_);
    }

private:
    template <typename Unsigned>
    void StoreUnsigned(std::size_t offset, Unsigned value) {
        static_assert(std::is_unsigned_v<Unsigned>);
        for (std::size_t index = 0U; index < sizeof(Unsigned); ++index) {
            bytes_.at(offset + index) = static_cast<std::byte>(
                (value >> static_cast<unsigned int>(index * 8U)) &
                static_cast<Unsigned>(0xffU));
        }
    }

    std::vector<std::byte> bytes_;
};

[[nodiscard]] std::vector<std::byte> ShanghaiTradeBody(
    std::string_view security_id) {
    WireWriter writer(70U);
    writer.StoreU64(0U, 1U);
    writer.StoreU32(8U, 7U);
    writer.StoreU32(18U, 93'000'125U);
    writer.StoreU64(28U, 11'001U);
    writer.StoreU64(36U, 22'002U);
    writer.StoreU32(44U, 12'345U);
    writer.StoreU64(48U, 41U);
    writer.StoreU64(56U, 506'145U);
    writer.StoreString(12U, security_id);
    writer.StoreString(22U, "T");
    writer.StoreString(64U, "B");
    return std::move(writer).Take();
}

[[nodiscard]] std::vector<std::byte> ShenzhenTransactionBody(
    std::string_view security_id) {
    WireWriter writer(70U);
    writer.StoreU32(0U, 12U);
    writer.StoreU64(4U, 1U);
    writer.StoreU64(18U, 201U);
    writer.StoreU64(26U, 301U);
    writer.StoreU64(46U, 123'456U);
    writer.StoreU64(54U, 33U);
    writer.StoreU32(62U, 70U);
    writer.StoreU32(66U, 93'000'124U);
    writer.StoreString(12U, "010");
    writer.StoreString(34U, security_id);
    writer.StoreString(40U, "102 ");
    return std::move(writer).Take();
}

class FakeMessage final : public mdl::MDLMessage {
public:
    FakeMessage(
        sdk::MessageKey key,
        std::vector<std::byte> body,
        std::uint32_t instrument_id,
        bool shanghai)
        : body_(std::move(body)),
          instrument_id_(instrument_id),
          shanghai_(shanghai) {
        head_.HeadSize =
            static_cast<std::uint8_t>(sdk::kVendorHeadBytes);
        head_.MessageSize = static_cast<std::uint32_t>(
            sdk::kVendorHeadBytes + body_.size());
        head_.MessageEncoding =
            static_cast<std::uint8_t>(mdl::MDLEID_BINARY);
        head_.ServiceID = key.service_id;
        head_.ServiceVersion = key.service_version;
        head_.MessageID = key.message_id;
        head_.LocalTime.m_Value = 93'000'000U;
        head_.SequenceID = 1U;
    }

    void AddRef() override {}
    int ReleaseRef() override { return 1; }
    mdl::MDLMessageHead* GetHead() const override {
        return const_cast<mdl::MDLMessageHead*>(&head_);
    }
    char* GetBody() const override {
        return body_.empty()
                   ? nullptr
                   : reinterpret_cast<char*>(
                         const_cast<std::byte*>(body_.data()));
    }
    mdl::MDLMessage* _Copy() const override { return nullptr; }

    void Prepare(std::uint64_t business_sequence) noexcept {
        StoreU64(shanghai_ ? 0U : 4U, business_sequence);
        const std::uint64_t order_base =
            business_sequence <=
                    (std::numeric_limits<std::uint64_t>::max() - 3U) / 2U
                ? business_sequence * 2U + 1U
                : business_sequence;
        if (shanghai_) {
            StoreU64(28U, order_base);
            StoreU64(36U, order_base + 1U);
        } else {
            StoreU64(18U, order_base);
            StoreU64(26U, order_base + 1U);
        }
        head_.SequenceID = business_sequence;
    }

    [[nodiscard]] std::uint32_t instrument_id() const noexcept {
        return instrument_id_;
    }
    [[nodiscard]] bool shanghai() const noexcept { return shanghai_; }

private:
    void StoreU64(std::size_t offset, std::uint64_t value) noexcept {
        for (std::size_t index = 0U; index < sizeof(value); ++index) {
            body_[offset + index] = static_cast<std::byte>(
                (value >> static_cast<unsigned int>(index * 8U)) & 0xffU);
        }
    }

    mutable mdl::MDLMessageHead head_{};
    std::vector<std::byte> body_;
    std::uint32_t instrument_id_ = 0U;
    bool shanghai_ = false;
};

[[nodiscard]] std::vector<std::byte> Bytes(std::string_view value) {
    const auto characters =
        std::span<const char>(value.data(), value.size());
    const auto bytes = std::as_bytes(characters);
    return std::vector<std::byte>(bytes.begin(), bytes.end());
}

[[nodiscard]] std::string SixDigit(std::uint32_t value) {
    std::array<char, 7U> text{};
    const int written = std::snprintf(
        text.data(), text.size(), "%06u", value);
    if (written != 6) {
        throw std::bad_alloc();
    }
    return std::string(text.data(), 6U);
}

[[nodiscard]] l2flow::common::Identity128 SessionId() noexcept {
    l2flow::common::Identity128 result{};
    result[0U] = std::byte{0xc4U};
    result[15U] = std::byte{0x8dU};
    return result;
}

[[nodiscard]] std::shared_ptr<const market::DailyInstrumentCatalogV2>
BuildCatalog(std::uint32_t instrument_count, std::string* detail) {
    const market::InstrumentMetadataV2 metadata{
        market::QuantityUnitV1::kShare,
        market::SecurityTypeV1::kEquity,
        market::AssetScopeV1::kDocumentedCore};
    std::vector<market::DailyInstrumentSourceEntryV2> entries;
    entries.reserve(instrument_count);
    const std::uint32_t shanghai_count = instrument_count / 2U;
    const std::uint32_t shenzhen_count =
        instrument_count - shanghai_count;
    for (std::uint32_t index = 0U; index < shanghai_count; ++index) {
        market::DailyInstrumentSourceEntryV2 entry{};
        entry.key.market = market::MarketV1::kShanghai;
        entry.key.security_id = Bytes(SixDigit(600'000U + index));
        entry.metadata = metadata;
        entries.push_back(std::move(entry));
    }
    for (std::uint32_t index = 0U; index < shenzhen_count; ++index) {
        market::DailyInstrumentSourceEntryV2 entry{};
        entry.key.market = market::MarketV1::kShenzhen;
        entry.key.security_id_source = Bytes("102 ");
        entry.key.security_id = Bytes(SixDigit(1U + index));
        entry.metadata = metadata;
        entries.push_back(std::move(entry));
    }
    market::DailyInstrumentCatalogConfigV2 config{};
    config.trade_date = kTradeDate;
    config.catalog_version = 1U;
    config.session_epoch = 1U;
    config.coverage_complete = true;
    std::unique_ptr<market::DailyInstrumentCatalogV2> catalog;
    const auto error = market::DailyInstrumentCatalogV2::Create(
        config, entries, &catalog);
    if (error != market::DailyInstrumentCatalogCreateErrorV2::kNone ||
        catalog == nullptr) {
        if (detail != nullptr) {
            *detail = "daily instrument catalog creation failed";
        }
        return nullptr;
    }
    return std::shared_ptr<const market::DailyInstrumentCatalogV2>(
        std::move(catalog));
}

[[nodiscard]] bool AddWouldOverflow(
    std::uint64_t lhs,
    std::uint64_t rhs) noexcept {
    return lhs > std::numeric_limits<std::uint64_t>::max() - rhs;
}

[[nodiscard]] runtime::FastTickPipelineConfigV1 PipelineConfig(
    std::shared_ptr<const market::DailyInstrumentCatalogV2> catalog,
    std::uint64_t maximum_messages,
    std::uint32_t worker_count,
    std::uint32_t queue_capacity,
    std::uint32_t maximum_change_batch,
    std::span<const std::size_t> plane_cpus) {
    const std::size_t instrument_count = catalog->instrument_count();
    const std::size_t per_instrument = static_cast<std::size_t>(
        maximum_messages / instrument_count + 2U);
    const std::size_t maximum_events =
        per_instrument <=
                (std::numeric_limits<std::size_t>::max() - 1024U) / 2U
            ? per_instrument * 2U + 1024U
            : std::numeric_limits<std::size_t>::max();

    runtime::FastTickPipelineConfigV1 result{};
    result.run_id = SessionId();
    result.trade_date = kTradeDate;
    result.daily_catalog = std::move(catalog);
    result.source_stream_ids = {101U, 202U};
    result.maximum_sdk_message_bytes = 512U;
    result.raw_tick_queue_capacity_per_source_worker = queue_capacity;
    result.raw_tick_batch_budget = 256U;
    result.prewarm_message_bytes = 128U;
    result.prewarm_message_count_per_source_worker =
        std::min<std::size_t>(queue_capacity, 65'536U);
    result.enforce_receive_trade_date = false;

    result.planes.fast.session_id = SessionId();
    result.planes.fast.trade_date = kTradeDate;
    result.planes.fast.instrument_count = instrument_count;
    result.planes.fast.worker_count = worker_count;
    result.planes.fast.maximum_session_records =
        maximum_messages + instrument_count;
    result.planes.fast.records_per_chunk = 1024U;
    result.planes.fast.maximum_records_per_read = maximum_change_batch;
    result.planes.fast.coverage_from_open = true;

    result.planes.event.session_id = SessionId();
    result.planes.event.trade_date = kTradeDate;
    result.planes.event.instrument_count = instrument_count;
    result.planes.event.worker_count = worker_count;
    result.planes.event.maximum_order_states_per_instrument = 4096U;
    result.planes.event.maximum_inputs_per_instrument = per_instrument;
    result.planes.event.maximum_events_per_instrument = maximum_events;
    result.planes.event.input_block_records = 256U;
    result.planes.event.event_block_records = 1024U;
    result.planes.event.cdc_range_chunk_records = 4096U;
    result.planes.event.maximum_change_records_per_instrument =
        maximum_events;
    result.planes.event.maximum_changes_per_read = maximum_change_batch;
    result.planes.event.repair_replay_record_budget = 4096U;

    result.planes.kline.session_id = SessionId();
    result.planes.kline.trade_date = kTradeDate;
    result.planes.kline.instrument_count = instrument_count;
    result.planes.kline.worker_count = worker_count;
    result.planes.kline.windows = {{1U, kNanosecondsPerSecond}};
    result.planes.kline.maximum_trades_per_instrument = per_instrument;
    result.planes.kline.maximum_bars_per_instrument = 16U;
    result.planes.kline.stable_block_bars = 16U;
    result.planes.kline.cdc_range_chunk_bars = 16U;
    result.planes.kline.maximum_change_records_per_instrument =
        maximum_events;
    result.planes.kline.maximum_changes_per_read = maximum_change_batch;
    result.planes.kline.repair_replay_record_budget = 4096U;

    result.planes.fast.tick_routes.resize(instrument_count);
    result.planes.event.event_routes.resize(instrument_count);
    result.planes.kline.kline_routes.resize(instrument_count);
    for (std::size_t ordinal = 0U; ordinal < instrument_count; ++ordinal) {
        const auto worker = static_cast<std::uint32_t>(
            ordinal % static_cast<std::size_t>(worker_count));
        result.planes.fast.tick_routes[ordinal] = worker;
        result.planes.event.event_routes[ordinal] = worker;
        result.planes.kline.kline_routes[ordinal] = worker;
    }
    result.planes.event_queue_capacity_per_tick_worker = queue_capacity;
    result.planes.kline_queue_capacity_per_tick_worker = queue_capacity;
    result.planes.live_batch_budget = 256U;
    result.planes.affinity.enforce = true;
    result.planes.affinity.tick_workers.resize(worker_count);
    result.planes.affinity.event_workers.resize(worker_count);
    result.planes.affinity.kline_workers.resize(worker_count);
    for (std::size_t worker = 0U; worker < worker_count; ++worker) {
        static_cast<void>(
            result.planes.affinity.tick_workers[worker].Add(
                plane_cpus[worker]));
        static_cast<void>(
            result.planes.affinity.event_workers[worker].Add(
                plane_cpus[worker_count + worker]));
        static_cast<void>(
            result.planes.affinity.kline_workers[worker].Add(
                plane_cpus[worker_count * 2U + worker]));
    }
    return result;
}

class BenchmarkHandle final {
public:
    BenchmarkHandle(const BenchmarkHandle&) = delete;
    BenchmarkHandle& operator=(const BenchmarkHandle&) = delete;

    ~BenchmarkHandle() {
        if (producer_.joinable()) {
            producer_.join();
        }
        if (pipeline_ != nullptr) {
            pipeline_->StopAndDrain();
        }
        if (restore_caller_affinity_) {
            static_cast<void>(
                l2flow::common::ApplyCurrentLinuxThreadAffinityExactV1(
                    original_caller_affinity_));
        }
    }

    [[nodiscard]] static std::unique_ptr<BenchmarkHandle> Create(
        std::uint64_t maximum_messages,
        std::uint32_t instrument_count,
        std::uint32_t worker_count,
        std::uint32_t queue_capacity,
        std::uint32_t maximum_change_batch,
        std::string* detail) {
        if (maximum_messages == 0U || instrument_count < 2U ||
            instrument_count % 2U != 0U || worker_count == 0U ||
            worker_count > instrument_count || queue_capacity == 0U ||
            maximum_change_batch == 0U ||
            maximum_messages >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max()) ||
            AddWouldOverflow(maximum_messages, instrument_count)) {
            if (detail != nullptr) {
                *detail = "invalid callback/Polars benchmark configuration";
            }
            return nullptr;
        }
        l2flow::common::LinuxCpuSetV1 original_affinity{};
        int affinity_system_error = 0;
        if (l2flow::common::ReadCurrentLinuxThreadAffinityV1(
                &original_affinity, &affinity_system_error) !=
            l2flow::common::LinuxThreadAffinityErrorV1::kNone) {
            if (detail != nullptr) {
                *detail = "cannot read benchmark thread affinity";
            }
            return nullptr;
        }
        std::vector<std::size_t> available_cpus;
        for (std::size_t cpu = 0U;
             cpu < l2flow::common::kLinuxCpuSetMaximumCpuCountV1;
             ++cpu) {
            if (original_affinity.contains(cpu)) {
                available_cpus.push_back(cpu);
            }
        }
        const std::size_t plane_cpu_count =
            static_cast<std::size_t>(worker_count) * 3U;
        if (available_cpus.size() < plane_cpu_count + 2U) {
            if (detail != nullptr) {
                *detail = "benchmark affinity needs three exclusive plane "
                    "CPUs per worker plus two callback/client CPUs";
            }
            return nullptr;
        }
        l2flow::common::LinuxCpuSetV1 reserved_affinity{};
        for (std::size_t index = plane_cpu_count;
             index < available_cpus.size(); ++index) {
            static_cast<void>(
                reserved_affinity.Add(available_cpus[index]));
        }
        auto catalog = BuildCatalog(instrument_count, detail);
        if (catalog == nullptr) {
            return nullptr;
        }
        auto result = std::unique_ptr<BenchmarkHandle>(
            new BenchmarkHandle(
                maximum_messages,
                maximum_change_batch,
                std::move(catalog)));
        result->original_caller_affinity_ = original_affinity;
        result->reserved_affinity_ = reserved_affinity;
        runtime::FastTickPipelineConfigV1 config = PipelineConfig(
            result->catalog_,
            maximum_messages,
            worker_count,
            queue_capacity,
            maximum_change_batch,
            std::span<const std::size_t>(
                available_cpus.data(), plane_cpu_count));
        if (l2flow::common::ApplyCurrentLinuxThreadAffinityExactV1(
                reserved_affinity,
                nullptr,
                &affinity_system_error) !=
            l2flow::common::LinuxThreadAffinityErrorV1::kNone) {
            if (detail != nullptr) {
                *detail = "cannot reserve benchmark callback/client CPUs";
            }
            return nullptr;
        }
        result->restore_caller_affinity_ = true;
        const auto error = runtime::FastTickPipelineV1::Create(
            std::move(config), &result->pipeline_, detail);
        if (error != runtime::FastTickPipelineCreateErrorV1::kNone ||
            result->pipeline_ == nullptr) {
            if (detail != nullptr && detail->empty()) {
                *detail = std::string("pipeline creation failed: ") +
                    std::string(
                        runtime::FastTickPipelineCreateErrorNameV1(error));
            }
            return nullptr;
        }
        ipc::InstrumentDataServiceConfigV3 service_config{};
        service_config.maximum_fast_rows_per_read = maximum_change_batch;
        service_config.maximum_event_changes_per_read =
            maximum_change_batch;
        service_config.maximum_kline_changes_per_read =
            maximum_change_batch;
        service_config.maximum_instruments_per_batch = instrument_count;
        const auto service_error = ipc::InstrumentDataServiceV3::Create(
            service_config,
            &result->pipeline_->planes(),
            &result->service_);
        if (service_error != ipc::InstrumentDataServiceErrorV3::kNone ||
            result->service_ == nullptr) {
            if (detail != nullptr) {
                *detail = std::string("V3 instrument service creation failed: ") +
                    std::string(
                        ipc::InstrumentDataServiceErrorNameV3(
                            service_error));
            }
            return nullptr;
        }
        result->BuildMessages();
        return result;
    }

    [[nodiscard]] int Start(
        std::uint64_t target_messages_per_second,
        std::uint64_t message_count) noexcept {
        if (target_messages_per_second == 0U || message_count == 0U ||
            message_count > maximum_messages_) {
            return L2FLOW_BENCHMARK_INVALID_ARGUMENT_V1;
        }
        std::lock_guard<std::mutex> lock(start_mutex_);
        if (producer_running_.load(std::memory_order_acquire) ||
            producer_started_) {
            return L2FLOW_BENCHMARK_ALREADY_RUNNING_V1;
        }
        producer_started_ = true;
        producer_running_.store(true, std::memory_order_release);
        scheduled_messages_.store(message_count, std::memory_order_release);
        try {
            producer_ = std::thread([this,
                                     target_messages_per_second,
                                     message_count] {
                Producer(target_messages_per_second, message_count);
            });
        } catch (...) {
            producer_started_ = false;
            producer_running_.store(false, std::memory_order_release);
            scheduled_messages_.store(0U, std::memory_order_release);
            return L2FLOW_BENCHMARK_THREAD_FAILED_V1;
        }
        return L2FLOW_BENCHMARK_OK_V1;
    }

    void Snapshot(L2FlowBenchmarkSnapshotV1* output) const noexcept {
        *output = {};
        output->scheduled_messages = scheduled_messages_.load(
            std::memory_order_acquire);
        output->attempted_messages = attempted_messages_.load(
            std::memory_order_acquire);
        output->accepted_messages = accepted_messages_.load(
            std::memory_order_acquire);
        output->ingress_errors = ingress_errors_.load(
            std::memory_order_acquire);
        output->raw_tick_queue_full_errors =
            raw_tick_queue_full_errors_.load(std::memory_order_acquire);
        output->owned_message_rejected_errors =
            owned_message_rejected_errors_.load(
                std::memory_order_acquire);
        output->other_ingress_errors = other_ingress_errors_.load(
            std::memory_order_acquire);
        output->first_callback_start_ns = first_callback_start_ns_.load(
            std::memory_order_acquire);
        output->last_callback_end_ns = last_callback_end_ns_.load(
            std::memory_order_acquire);
        output->producer_end_ns = producer_end_ns_.load(
            std::memory_order_acquire);
        output->producer_done = producer_done_.load(
            std::memory_order_acquire) ? 1U : 0U;
        output->producer_running = producer_running_.load(
            std::memory_order_acquire) ? 1U : 0U;
        if (pipeline_ == nullptr) {
            return;
        }
        const runtime::FastTickPipelineSnapshotV1 snapshot =
            pipeline_->Snapshot();
        output->accepted_shanghai = snapshot.accepted_by_source[0U];
        output->accepted_shenzhen = snapshot.accepted_by_source[1U];
        output->decoded_messages = snapshot.decoded_messages;
        output->decode_failures = snapshot.decode_failures;
        output->rejected_messages = snapshot.rejected_messages;
        output->pipeline_fatal = snapshot.fatal ? 1U : 0U;
        output->fast_append_failures =
            snapshot.planes.fast_append_failures;
        output->fast_unrecoverable_drops =
            snapshot.planes.fast_unrecoverable_drops;
        output->event_queue_failures =
            snapshot.planes.event_queue_failures;
        output->kline_queue_failures =
            snapshot.planes.kline_queue_failures;
        output->fast_applied = snapshot.planes.fast_applied;
        output->event_applied = snapshot.planes.event_applied;
        output->kline_applied = snapshot.planes.kline_applied;
        output->event_rebuild_attempts =
            snapshot.planes.event_rebuild_attempts;
        output->kline_rebuild_attempts =
            snapshot.planes.kline_rebuild_attempts;

        for (const auto& entry : catalog_->entries()) {
            market::FastTickInstrumentStatusV1 fast{};
            if (pipeline_->planes().fast_store().Status(
                    entry.instrument_id, &fast) !=
                    market::FastTickStoreQueryErrorV1::kNone) {
                ++output->incomplete_fast_instruments;
            } else {
                output->fast_stable_rows += fast.published_tail;
                if (!fast.coverage_complete) {
                    ++output->incomplete_fast_instruments;
                }
            }
            market::EventStableSnapshotV1 events{};
            if (pipeline_->planes().event_history().AcquireStable(
                    entry.instrument_id, &events) !=
                    market::OrderedEventHistoryErrorV1::kNone ||
                events.root == nullptr) {
                ++output->event_non_live_instruments;
            } else {
                output->event_stable_rows += events.root->row_count();
                if (events.repair_state !=
                    market::EventRepairStateV1::kLive) {
                    ++output->event_non_live_instruments;
                }
            }
            if (pipeline_->planes().kline_history().RepairState(
                    entry.instrument_id) !=
                market::EventRepairStateV1::kLive) {
                ++output->kline_non_live_instruments;
            }
        }
    }

    [[nodiscard]] std::uint32_t instrument_count() const noexcept {
        return static_cast<std::uint32_t>(producer_order_.size());
    }

    [[nodiscard]] std::uint32_t InstrumentId(
        std::uint32_t slot) const noexcept {
        if (slot >= producer_order_.size()) {
            return 0U;
        }
        return messages_[producer_order_[slot]]->instrument_id();
    }

    [[nodiscard]] int ReadChanges(
        std::uint32_t instrument_id,
        std::uint64_t next_change_sequence,
        L2FlowBenchmarkEventRowV1* output,
        std::size_t output_capacity,
        std::size_t* written,
        std::uint64_t* next_change_sequence_out) noexcept {
        if (instrument_id == 0U || next_change_sequence == 0U ||
            output == nullptr || output_capacity == 0U ||
            output_capacity > change_scratch_.size() ||
            written == nullptr || next_change_sequence_out == nullptr) {
            return L2FLOW_BENCHMARK_INVALID_ARGUMENT_V1;
        }
        *written = 0U;
        *next_change_sequence_out = next_change_sequence;
        std::lock_guard<std::mutex> lock(read_mutex_);
        ipc::EventChangeCursorWireV3 cursor{};
        cursor.session_id = SessionId();
        cursor.instrument_id = instrument_id;
        cursor.next_change_sequence = next_change_sequence;
        std::size_t native_written = 0U;
        const auto error = service_->ReadEventChanges(
            &cursor,
            std::span<market::EventMutationV1>(
                change_scratch_.data(), output_capacity),
            &native_written);
        if (error != ipc::InstrumentDataServiceErrorV3::kNone) {
            return error ==
                           ipc::InstrumentDataServiceErrorV3::kCursorMismatch
                       ? L2FLOW_BENCHMARK_CURSOR_MISMATCH_V1
                       : L2FLOW_BENCHMARK_READ_FAILED_V1;
        }
        for (std::size_t index = 0U; index < native_written; ++index) {
            const market::EventMutationV1& mutation =
                change_scratch_[index];
            if (mutation.kind != market::EventMutationKindV1::kInsert) {
                return L2FLOW_BENCHMARK_UNSUPPORTED_MUTATION_V1;
            }
            if (!CopyRow(
                    mutation.row,
                    mutation.change_sequence,
                    static_cast<std::uint8_t>(mutation.kind),
                    &output[index])) {
                return L2FLOW_BENCHMARK_READ_FAILED_V1;
            }
        }
        *written = native_written;
        *next_change_sequence_out = cursor.next_change_sequence;
        return L2FLOW_BENCHMARK_OK_V1;
    }

    [[nodiscard]] int StableSize(
        std::uint32_t instrument_id,
        std::uint64_t* row_count,
        std::uint64_t* included_change_sequence,
        std::uint32_t* repair_state) const noexcept {
        if (instrument_id == 0U || row_count == nullptr ||
            included_change_sequence == nullptr ||
            repair_state == nullptr) {
            return L2FLOW_BENCHMARK_INVALID_ARGUMENT_V1;
        }
        ipc::EventStableViewV3 snapshot{};
        if (service_->AcquireEventStable(instrument_id, &snapshot) !=
                ipc::InstrumentDataServiceErrorV3::kNone ||
            snapshot.root == nullptr) {
            return L2FLOW_BENCHMARK_READ_FAILED_V1;
        }
        *row_count = snapshot.root->row_count();
        *included_change_sequence =
            snapshot.root->included_change_sequence();
        *repair_state = snapshot.status.repair_state;
        return L2FLOW_BENCHMARK_OK_V1;
    }

    [[nodiscard]] int CopyStable(
        std::uint32_t instrument_id,
        L2FlowBenchmarkEventRowV1* output,
        std::size_t output_capacity,
        std::size_t* written,
        std::uint64_t* included_change_sequence,
        std::uint32_t* repair_state) const noexcept {
        if (instrument_id == 0U || output == nullptr || written == nullptr ||
            included_change_sequence == nullptr ||
            repair_state == nullptr) {
            return L2FLOW_BENCHMARK_INVALID_ARGUMENT_V1;
        }
        *written = 0U;
        ipc::EventStableViewV3 snapshot{};
        if (service_->AcquireEventStable(instrument_id, &snapshot) !=
                ipc::InstrumentDataServiceErrorV3::kNone ||
            snapshot.root == nullptr) {
            return L2FLOW_BENCHMARK_READ_FAILED_V1;
        }
        if (snapshot.root->row_count() > output_capacity) {
            return L2FLOW_BENCHMARK_BUFFER_TOO_SMALL_V1;
        }
        std::vector<market::OrderedDerivedEventV1> rows;
        if (!snapshot.root->CopyRows(&rows) ||
            rows.size() > output_capacity) {
            return L2FLOW_BENCHMARK_READ_FAILED_V1;
        }
        for (std::size_t index = 0U; index < rows.size(); ++index) {
            if (!CopyRow(rows[index], 0U, 0xffU, &output[index])) {
                return L2FLOW_BENCHMARK_READ_FAILED_V1;
            }
        }
        *written = rows.size();
        *included_change_sequence =
            snapshot.root->included_change_sequence();
        *repair_state = snapshot.status.repair_state;
        return L2FLOW_BENCHMARK_OK_V1;
    }

private:
    BenchmarkHandle(
        std::uint64_t maximum_messages,
        std::uint32_t maximum_change_batch,
        std::shared_ptr<const market::DailyInstrumentCatalogV2> catalog)
        : maximum_messages_(maximum_messages),
          catalog_(std::move(catalog)),
          callback_start_by_arrival_(
              std::make_unique<std::atomic<std::uint64_t>[]>(
                  static_cast<std::size_t>(maximum_messages + 1U))),
          change_scratch_(maximum_change_batch) {
        for (std::uint64_t index = 0U; index <= maximum_messages_; ++index) {
            callback_start_by_arrival_[static_cast<std::size_t>(index)].store(
                0U, std::memory_order_relaxed);
        }
    }

    void BuildMessages() {
        messages_.reserve(catalog_->instrument_count());
        std::vector<std::size_t> shanghai;
        std::vector<std::size_t> shenzhen;
        for (const auto& entry : catalog_->entries()) {
            const std::string security_id(
                reinterpret_cast<const char*>(entry.key.security_id.data()),
                entry.key.security_id.size());
            const bool is_shanghai =
                entry.key.market == market::MarketV1::kShanghai;
            const sdk::MessageKey key = is_shanghai
                ? sdk::kProductionMessageKeysV1[0U]
                : sdk::kProductionMessageKeysV1[2U];
            auto body = is_shanghai
                ? ShanghaiTradeBody(security_id)
                : ShenzhenTransactionBody(security_id);
            const std::size_t message_index = messages_.size();
            messages_.push_back(std::make_unique<FakeMessage>(
                key,
                std::move(body),
                entry.instrument_id,
                is_shanghai));
            (is_shanghai ? shanghai : shenzhen).push_back(message_index);
        }
        const std::size_t slots = std::max(shanghai.size(), shenzhen.size());
        producer_order_.reserve(messages_.size());
        for (std::size_t index = 0U; index < slots; ++index) {
            if (index < shanghai.size()) {
                producer_order_.push_back(shanghai[index]);
            }
            if (index < shenzhen.size()) {
                producer_order_.push_back(shenzhen[index]);
            }
        }
    }

    static void PaceUntil(
        std::chrono::steady_clock::time_point deadline) noexcept {
        constexpr auto sleep_margin = std::chrono::microseconds(30);
        for (;;) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                return;
            }
            if (deadline - now > std::chrono::microseconds(100)) {
                std::this_thread::sleep_until(deadline - sleep_margin);
            }
        }
    }

    void Producer(
        std::uint64_t target_messages_per_second,
        std::uint64_t message_count) noexcept {
        const auto schedule_start =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(20);
        std::uint64_t accepted = 0U;
        for (std::uint64_t index = 0U; index < message_count; ++index) {
            const std::uint64_t whole_seconds =
                index / target_messages_per_second;
            const std::uint64_t remainder =
                index % target_messages_per_second;
            const std::uint64_t offset_ns =
                whole_seconds * kNanosecondsPerSecond +
                (remainder * kNanosecondsPerSecond) /
                    target_messages_per_second;
            const auto deadline = schedule_start +
                std::chrono::nanoseconds(offset_ns);

            FakeMessage& message = *messages_[producer_order_[
                static_cast<std::size_t>(
                    index % producer_order_.size())]];
            message.Prepare(index + 1U);
            PaceUntil(deadline);

            const std::uint64_t callback_start = MonotonicNowNs();
            if (index == 0U) {
                first_callback_start_ns_.store(
                    callback_start, std::memory_order_release);
            }
            const std::uint64_t expected_arrival = accepted + 1U;
            callback_start_by_arrival_[
                static_cast<std::size_t>(expected_arrival)].store(
                    callback_start, std::memory_order_release);
            const auto result = pipeline_->IngestForTest(
                &message, RealtimeNowNs(), callback_start);
            attempted_messages_.fetch_add(1U, std::memory_order_release);
            const std::uint64_t callback_end = MonotonicNowNs();
            last_callback_end_ns_.store(
                callback_end, std::memory_order_release);
            if (result.accepted()) {
                if (result.arrival_id != expected_arrival) {
                    ingress_errors_.fetch_add(
                        1U, std::memory_order_release);
                    other_ingress_errors_.fetch_add(
                        1U, std::memory_order_release);
                } else {
                    accepted = result.arrival_id;
                    accepted_messages_.store(
                        accepted, std::memory_order_release);
                }
            } else {
                ingress_errors_.fetch_add(1U, std::memory_order_release);
                if (result.error ==
                    runtime::FastTickPipelineIngressErrorV1::
                        kRawTickQueueFull) {
                    raw_tick_queue_full_errors_.fetch_add(
                        1U, std::memory_order_release);
                } else if (result.error ==
                           runtime::FastTickPipelineIngressErrorV1::
                               kOwnedMessageRejected) {
                    owned_message_rejected_errors_.fetch_add(
                        1U, std::memory_order_release);
                } else {
                    other_ingress_errors_.fetch_add(
                        1U, std::memory_order_release);
                }
            }
        }
        producer_end_ns_.store(MonotonicNowNs(), std::memory_order_release);
        producer_running_.store(false, std::memory_order_release);
        producer_done_.store(true, std::memory_order_release);
    }

    [[nodiscard]] bool CopyRow(
        const market::OrderedDerivedEventV1& row,
        std::uint64_t change_sequence,
        std::uint8_t mutation_kind,
        L2FlowBenchmarkEventRowV1* output) const noexcept {
        if (output == nullptr || row.source_arrival_id == 0U ||
            row.source_arrival_id > maximum_messages_) {
            return false;
        }
        const std::uint64_t callback_start =
            callback_start_by_arrival_[
                static_cast<std::size_t>(row.source_arrival_id)].load(
                    std::memory_order_acquire);
        if (callback_start == 0U) {
            return false;
        }
        *output = {};
        output->change_sequence = change_sequence;
        output->callback_start_ns = callback_start;
        output->source_arrival_id = row.source_arrival_id;
        output->business_sequence = row.order_key.business_sequence;
        output->affected_order_id = row.order_key.affected_order_id;
        output->instrument_id = row.uid.instrument_id;
        output->channel = row.order_key.channel;
        output->source_event_ordinal = row.order_key.source_event_ordinal;
        output->derived_event_ordinal = row.order_key.derived_event_ordinal;
        output->occurrence = row.uid.occurrence;
        output->event_kind = static_cast<std::uint8_t>(row.uid.kind);
        output->mutation_kind = mutation_kind;
        constexpr std::uint32_t price_valid = 1U << 0U;
        constexpr std::uint32_t quantity_valid = 1U << 1U;
        constexpr std::uint32_t amount_valid = 1U << 2U;
        constexpr std::uint32_t event_time_valid = 1U << 3U;
        constexpr std::uint32_t receive_time_valid = 1U << 4U;
        if (const auto* trade =
                std::get_if<market::ShanghaiTradeEventV1>(&row.payload);
            trade != nullptr) {
            output->price_p6 = trade->price_p6;
            output->quantity = trade->quantity;
            output->trade_amount_p6 = trade->trade_amount_p6;
            output->buy_order_id = trade->buy_order_id;
            output->sell_order_id = trade->sell_order_id;
            output->recv_realtime_ns =
                trade->source_anchor.recv_realtime_ns;
            output->recv_monotonic_ns =
                trade->source_anchor.recv_monotonic_ns;
            output->event_time_ns_since_midnight =
                trade->source_anchor.event_time_ns_since_midnight;
            output->source_quality_flags = trade->source_quality_flags;
            output->source_market_notices =
                trade->source_market_notices;
            output->payload_validity = price_valid | quantity_valid |
                receive_time_valid;
            if (trade->trade_amount_valid) {
                output->payload_validity |= amount_valid;
            }
            if (trade->source_anchor.event_time_valid) {
                output->payload_validity |= event_time_valid;
            }
        } else if (const auto* shenzhen_trade =
                       std::get_if<market::ShenzhenTradeEventV1>(
                           &row.payload);
                   shenzhen_trade != nullptr) {
            output->price_p6 = shenzhen_trade->price_p6;
            output->quantity = shenzhen_trade->quantity;
            output->trade_amount_p6 = shenzhen_trade->amount_p6;
            output->buy_order_id = shenzhen_trade->buy_order_id;
            output->sell_order_id = shenzhen_trade->sell_order_id;
            output->recv_realtime_ns =
                shenzhen_trade->source_anchor.recv_realtime_ns;
            output->recv_monotonic_ns =
                shenzhen_trade->source_anchor.recv_monotonic_ns;
            output->event_time_ns_since_midnight =
                shenzhen_trade->source_anchor.event_time_ns_since_midnight;
            output->source_quality_flags =
                shenzhen_trade->source_quality_flags;
            output->source_market_notices =
                shenzhen_trade->source_market_notices;
            output->event_quality_flags = shenzhen_trade->quality_flags;
            output->payload_validity = price_valid | quantity_valid |
                receive_time_valid;
            if (shenzhen_trade->amount_valid) {
                output->payload_validity |= amount_valid;
            }
            if (shenzhen_trade->source_anchor.event_time_valid) {
                output->payload_validity |= event_time_valid;
            }
        }
        return true;
    }

    const std::uint64_t maximum_messages_ = 0U;
    std::shared_ptr<const market::DailyInstrumentCatalogV2> catalog_;
    std::unique_ptr<runtime::FastTickPipelineV1> pipeline_;
    std::unique_ptr<ipc::InstrumentDataServiceV3> service_;
    std::vector<std::unique_ptr<FakeMessage>> messages_;
    std::vector<std::size_t> producer_order_;
    std::unique_ptr<std::atomic<std::uint64_t>[]>
        callback_start_by_arrival_;
    std::vector<market::EventMutationV1> change_scratch_;
    mutable std::mutex read_mutex_;
    std::mutex start_mutex_;
    std::thread producer_;
    bool producer_started_ = false;
    std::atomic<bool> producer_running_{false};
    std::atomic<bool> producer_done_{false};
    std::atomic<std::uint64_t> scheduled_messages_{0U};
    std::atomic<std::uint64_t> attempted_messages_{0U};
    std::atomic<std::uint64_t> accepted_messages_{0U};
    std::atomic<std::uint64_t> ingress_errors_{0U};
    std::atomic<std::uint64_t> raw_tick_queue_full_errors_{0U};
    std::atomic<std::uint64_t> owned_message_rejected_errors_{0U};
    std::atomic<std::uint64_t> other_ingress_errors_{0U};
    std::atomic<std::uint64_t> first_callback_start_ns_{0U};
    std::atomic<std::uint64_t> last_callback_end_ns_{0U};
    std::atomic<std::uint64_t> producer_end_ns_{0U};
    l2flow::common::LinuxCpuSetV1 original_caller_affinity_{};
    l2flow::common::LinuxCpuSetV1 reserved_affinity_{};
    bool restore_caller_affinity_ = false;
};

[[nodiscard]] BenchmarkHandle* Handle(void* value) noexcept {
    return static_cast<BenchmarkHandle*>(value);
}

[[nodiscard]] const BenchmarkHandle* Handle(const void* value) noexcept {
    return static_cast<const BenchmarkHandle*>(value);
}

}  // namespace

extern "C" void* l2flow_benchmark_create_v1(
    std::uint64_t maximum_messages,
    std::uint32_t instrument_count,
    std::uint32_t worker_count,
    std::uint32_t queue_capacity,
    std::uint32_t maximum_change_batch,
    char* detail,
    std::size_t detail_capacity) {
    WriteDetail(detail, detail_capacity, "");
    try {
        std::string error;
        auto result = BenchmarkHandle::Create(
            maximum_messages,
            instrument_count,
            worker_count,
            queue_capacity,
            maximum_change_batch,
            &error);
        if (result == nullptr) {
            WriteDetail(detail, detail_capacity, error);
            return nullptr;
        }
        return result.release();
    } catch (const std::exception& error) {
        WriteDetail(detail, detail_capacity, error.what());
        return nullptr;
    } catch (...) {
        WriteDetail(detail, detail_capacity, "unknown benchmark creation error");
        return nullptr;
    }
}

extern "C" int l2flow_benchmark_start_v1(
    void* handle,
    std::uint64_t target_messages_per_second,
    std::uint64_t message_count) {
    return handle == nullptr
        ? L2FLOW_BENCHMARK_INVALID_ARGUMENT_V1
        : Handle(handle)->Start(target_messages_per_second, message_count);
}

extern "C" int l2flow_benchmark_snapshot_v1(
    const void* handle,
    L2FlowBenchmarkSnapshotV1* output) {
    if (handle == nullptr || output == nullptr) {
        return L2FLOW_BENCHMARK_INVALID_ARGUMENT_V1;
    }
    Handle(handle)->Snapshot(output);
    return L2FLOW_BENCHMARK_OK_V1;
}

extern "C" std::uint32_t l2flow_benchmark_instrument_count_v1(
    const void* handle) {
    return handle == nullptr ? 0U : Handle(handle)->instrument_count();
}

extern "C" std::uint32_t l2flow_benchmark_instrument_id_v1(
    const void* handle,
    std::uint32_t producer_slot) {
    return handle == nullptr
        ? 0U
        : Handle(handle)->InstrumentId(producer_slot);
}

extern "C" int l2flow_benchmark_read_event_changes_v1(
    void* handle,
    std::uint32_t instrument_id,
    std::uint64_t next_change_sequence,
    L2FlowBenchmarkEventRowV1* output,
    std::size_t output_capacity,
    std::size_t* written,
    std::uint64_t* next_change_sequence_out) {
    return handle == nullptr
        ? L2FLOW_BENCHMARK_INVALID_ARGUMENT_V1
        : Handle(handle)->ReadChanges(
              instrument_id,
              next_change_sequence,
              output,
              output_capacity,
              written,
              next_change_sequence_out);
}

extern "C" int l2flow_benchmark_event_stable_size_v1(
    const void* handle,
    std::uint32_t instrument_id,
    std::uint64_t* row_count,
    std::uint64_t* included_change_sequence,
    std::uint32_t* repair_state) {
    return handle == nullptr
        ? L2FLOW_BENCHMARK_INVALID_ARGUMENT_V1
        : Handle(handle)->StableSize(
              instrument_id,
              row_count,
              included_change_sequence,
              repair_state);
}

extern "C" int l2flow_benchmark_copy_event_stable_v1(
    const void* handle,
    std::uint32_t instrument_id,
    L2FlowBenchmarkEventRowV1* output,
    std::size_t output_capacity,
    std::size_t* written,
    std::uint64_t* included_change_sequence,
    std::uint32_t* repair_state) {
    return handle == nullptr
        ? L2FLOW_BENCHMARK_INVALID_ARGUMENT_V1
        : Handle(handle)->CopyStable(
              instrument_id,
              output,
              output_capacity,
              written,
              included_change_sequence,
              repair_state);
}

extern "C" std::uint64_t l2flow_benchmark_monotonic_now_ns_v1() {
    return MonotonicNowNs();
}

extern "C" std::size_t l2flow_benchmark_event_row_size_v1() {
    return sizeof(L2FlowBenchmarkEventRowV1);
}

extern "C" std::size_t l2flow_benchmark_snapshot_size_v1() {
    return sizeof(L2FlowBenchmarkSnapshotV1);
}

extern "C" void l2flow_benchmark_destroy_v1(void* handle) {
    delete Handle(handle);
}
