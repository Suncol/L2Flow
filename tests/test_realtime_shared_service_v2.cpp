#include "l2flow/ipc/instrument_raw_event_history_v2.h"
#include "l2flow/ipc/instrument_derived_event_history_c_v1.h"
#include "l2flow/ipc/instrument_derived_event_history_v1.h"
#include "l2flow/ipc/realtime_history_wire_v2.h"
#include "l2flow/ipc/realtime_instrument_tick_delta_wire_v2.h"
#include "l2flow/ipc/realtime_certified_service_v1.h"
#include "l2flow/ipc/realtime_shared_service_v2.h"
#include "l2flow/ipc/realtime_shm_reader_c_v2.h"
#include "l2flow/ipc/realtime_wire_v2.h"
#include "l2flow/market/instrument_runtime_state_v2.h"
#include "l2flow/market/realtime_history_v1.h"
#include "l2flow/runtime/realtime_pipeline_v1.h"
#include "l2flow/sdk/market_message_catalog_v1.h"
#include "l2flow/sdk/vendor_head_view.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <poll.h>
#include <sched.h>
#include <sstream>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#include <spawn.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

extern char** environ;

namespace {

namespace common = l2flow::common;
namespace ipc = l2flow::ipc;
namespace market = l2flow::market;
namespace realtime = l2flow::realtime;
namespace runtime = l2flow::runtime;
namespace sdk = l2flow::sdk;
namespace mdl = datayes::mdl;

constexpr std::uint32_t kTradeDate = 20260729U;
constexpr std::uint64_t kSessionEpoch = 29U;
constexpr std::array<std::uint32_t, 4U> kSourceStreamIds{
    11U, 12U, 13U, 14U};
constexpr std::uint32_t kWindowId = 60U;
constexpr std::uint64_t kWindowDurationNs =
    60U * market::kKLineNanosecondsPerSecondV1;

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

std::vector<std::byte> Bytes(std::string_view text) {
    return {
        std::as_bytes(std::span(text)).begin(),
        std::as_bytes(std::span(text)).end()};
}

common::Identity128 RunId(std::uint8_t first) {
    common::Identity128 result{};
    result[0U] = static_cast<std::byte>(first);
    result[15U] = std::byte{0xa5U};
    return result;
}

class PipelineCleanup final {
public:
    explicit PipelineCleanup(
        std::unique_ptr<runtime::RealtimePipelineV1>* pipeline)
        : pipeline_(pipeline) {}

    PipelineCleanup(const PipelineCleanup&) = delete;
    PipelineCleanup& operator=(const PipelineCleanup&) = delete;

    ~PipelineCleanup() {
        if (pipeline_ != nullptr && *pipeline_ != nullptr) {
            (*pipeline_)->StopAndDrain();
        }
    }

private:
    std::unique_ptr<runtime::RealtimePipelineV1>* pipeline_ = nullptr;
};

class PipelineWireWriter final {
public:
    explicit PipelineWireWriter(std::size_t fixed_bytes)
        : bytes_(fixed_bytes, std::byte{0U}) {}

    void StoreU32(std::size_t offset, std::uint32_t value) {
        StoreUnsigned(offset, value);
    }

    void StoreU64(std::size_t offset, std::uint64_t value) {
        StoreUnsigned(offset, value);
    }

    void StoreString(
        std::size_t descriptor,
        std::string_view value) {
        const std::size_t start = bytes_.size();
        StoreUnsigned(
            descriptor,
            static_cast<std::uint16_t>(value.size()));
        StoreU32(
            descriptor + sizeof(std::uint16_t),
            value.empty()
                ? 0U
                : static_cast<std::uint32_t>(
                      start - descriptor));
        const auto encoded =
            std::as_bytes(std::span(value));
        bytes_.insert(
            bytes_.end(), encoded.begin(), encoded.end());
    }

    [[nodiscard]] std::vector<std::byte> Take() && {
        return std::move(bytes_);
    }

private:
    template <typename Unsigned>
    void StoreUnsigned(std::size_t offset, Unsigned value) {
        static_assert(std::is_unsigned_v<Unsigned>);
        for (std::size_t index = 0U;
             index < sizeof(Unsigned);
             ++index) {
            bytes_.at(offset + index) =
                static_cast<std::byte>(
                    (value >>
                     static_cast<unsigned int>(index * 8U)) &
                    static_cast<Unsigned>(0xffU));
        }
    }

    std::vector<std::byte> bytes_;
};

std::vector<std::byte> PipelineShenzhenSnapshotBody(
    std::string_view security_id = "000001") {
    PipelineWireWriter writer(224U);
    writer.StoreU32(0U, 93'000'123U);
    writer.StoreU32(4U, 12U);
    writer.StoreU64(32U, 12'000'000U);
    writer.StoreU64(40U, 1U);
    writer.StoreU64(48U, 100U);
    writer.StoreU64(56U, 1'234'560U);
    writer.StoreU64(64U, 12'345'600U);
    writer.StoreString(8U, "010");
    writer.StoreString(14U, security_id);
    writer.StoreString(20U, "102 ");
    writer.StoreString(26U, "T");
    return std::move(writer).Take();
}

std::vector<std::byte> PipelineShanghaiSnapshotBody(
    std::string_view security_id = "600001") {
    PipelineWireWriter writer(248U);
    writer.StoreU32(0U, 93'000'123U);
    writer.StoreU32(30U, 12'345U);
    writer.StoreString(4U, security_id);
    writer.StoreString(38U, "TRADE");
    return std::move(writer).Take();
}

std::vector<std::byte> PipelineShanghaiTickBody(
    std::string_view security_id = "600001",
    std::uint64_t business_index = 1U,
    std::uint64_t quantity = 41U) {
    PipelineWireWriter writer(70U);
    writer.StoreU64(0U, business_index);
    writer.StoreU32(8U, 7U);
    writer.StoreU32(18U, 93'000'125U);
    writer.StoreU64(28U, 11'001U);
    writer.StoreU64(36U, 22'002U);
    writer.StoreU32(44U, 12'345U);
    writer.StoreU64(48U, quantity);
    writer.StoreU64(56U, 12'345U * quantity);
    writer.StoreString(12U, security_id);
    writer.StoreString(22U, "T");
    writer.StoreString(64U, "B");
    return std::move(writer).Take();
}

std::vector<std::byte> PipelineShanghaiAddBody(
    std::string_view security_id,
    std::uint64_t business_index,
    std::uint64_t published_quantity,
    std::uint64_t matched_quantity) {
    PipelineWireWriter writer(70U);
    writer.StoreU64(0U, business_index);
    writer.StoreU32(8U, 7U);
    writer.StoreU32(18U, 93'000'125U);
    writer.StoreU64(28U, 11'001U);
    writer.StoreU64(36U, 0U);
    writer.StoreU32(44U, 12'345U);
    writer.StoreU64(48U, published_quantity);
    writer.StoreU64(56U, matched_quantity * 1'000U);
    writer.StoreString(12U, security_id);
    writer.StoreString(22U, "A");
    writer.StoreString(64U, "B");
    return std::move(writer).Take();
}

std::vector<std::byte> PipelineShanghaiStatusBody(
    std::string_view security_id,
    std::uint64_t business_index,
    std::string_view phase) {
    PipelineWireWriter writer(70U);
    writer.StoreU64(0U, business_index);
    writer.StoreU32(8U, 7U);
    writer.StoreU32(18U, 93'000'125U);
    writer.StoreString(12U, security_id);
    writer.StoreString(22U, "S");
    writer.StoreString(64U, phase);
    return std::move(writer).Take();
}

std::vector<std::byte> PipelineShenzhenOrderBody(
    std::string_view security_id = "000001",
    std::uint64_t application_sequence = 1U) {
    PipelineWireWriter writer(58U);
    writer.StoreU32(0U, 12U);
    writer.StoreU64(4U, application_sequence);
    writer.StoreU64(30U, 123'456U);
    writer.StoreU64(38U, 201U);
    writer.StoreU32(46U, 49U);
    writer.StoreU32(50U, 93'000'124U);
    writer.StoreU32(54U, 50U);
    writer.StoreString(12U, "010");
    writer.StoreString(18U, security_id);
    writer.StoreString(24U, "102 ");
    return std::move(writer).Take();
}

std::vector<std::byte> PipelineShenzhenTransactionBody(
    std::string_view security_id = "000001",
    std::uint64_t application_sequence = 2U,
    std::uint64_t bid_application_sequence = 1U) {
    PipelineWireWriter writer(70U);
    writer.StoreU32(0U, 12U);
    writer.StoreU64(4U, application_sequence);
    writer.StoreU64(18U, bid_application_sequence);
    writer.StoreU64(26U, 0U);
    writer.StoreU64(46U, 123'456U);
    writer.StoreU64(54U, 33U);
    writer.StoreU32(62U, 70U);
    writer.StoreU32(66U, 93'000'124U);
    writer.StoreString(12U, "010");
    writer.StoreString(34U, security_id);
    writer.StoreString(40U, "102 ");
    return std::move(writer).Take();
}

[[nodiscard]] std::string SixDigitSecurityId(
    std::uint32_t value) {
    if (value == 0U || value > 999'999U) {
        return {};
    }
    std::string result(6U, '0');
    for (std::size_t offset = 0U; offset < result.size(); ++offset) {
        const std::size_t index = result.size() - offset - 1U;
        result[index] =
            static_cast<char>('0' + static_cast<char>(value % 10U));
        value /= 10U;
    }
    return result;
}

// Enumerates the documented Shenzhen A-share ranges in exact byte-sort
// order. The synthetic performance fixtures therefore exercise the same
// classifier and immutable-catalog contract as production.
[[nodiscard]] std::string ShenzhenAShareSecurityId(
    std::uint32_t ordinal) {
    constexpr std::uint32_t kFirstRangeCount = 999U;
    constexpr std::uint32_t kSecondRangeCount = 3'800U;
    constexpr std::uint32_t kMaximumOrdinal = 14'599U;
    if (ordinal == 0U || ordinal > kMaximumOrdinal) {
        return {};
    }
    std::uint32_t numeric = 0U;
    if (ordinal <= kFirstRangeCount) {
        numeric = ordinal;
    } else if (
        ordinal <= kFirstRangeCount + kSecondRangeCount) {
        numeric = 1'200U + ordinal - kFirstRangeCount - 1U;
    } else {
        numeric =
            300'000U + ordinal -
            kFirstRangeCount - kSecondRangeCount - 1U;
    }
    return SixDigitSecurityId(numeric);
}

class FakeSdkMessage final : public mdl::MDLMessage {
public:
    FakeSdkMessage(
        sdk::MessageKey key,
        std::vector<std::byte> body)
        : body_(std::move(body)) {
        std::memset(&head_, 0, sizeof(head_));
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
        head_.SequenceID = 777U;
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
                         const_cast<std::byte*>(
                             body_.data()));
    }
    mdl::MDLMessage* _Copy() const override { return nullptr; }

    [[nodiscard]] bool StoreBodyU64(
        std::size_t offset,
        std::uint64_t value) noexcept {
        if (offset > body_.size() ||
            body_.size() - offset < sizeof(value)) {
            return false;
        }
        for (std::size_t index = 0U;
             index < sizeof(value);
             ++index) {
            body_[offset + index] = static_cast<std::byte>(
                (value >>
                 static_cast<unsigned int>(index * 8U)) &
                0xffU);
        }
        return true;
    }

private:
    mdl::MDLMessageHead head_{};
    std::vector<std::byte> body_;
};

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

class LatencySdkState final {
public:
    void Install(mdl::MessageHandlerBase* handler) noexcept {
        handler_.store(handler, std::memory_order_release);
    }

    [[nodiscard]] mdl::MessageHandlerBase* handler() const noexcept {
        return handler_.load(std::memory_order_acquire);
    }

    void Shutdown() noexcept {
        handler_.store(nullptr, std::memory_order_release);
    }

private:
    std::atomic<mdl::MessageHandlerBase*> handler_{nullptr};
};

class LatencySdkSubscriber final : public sdk::SdkSubscriber {
public:
    void SetServerAddress(std::string_view address) override {
        server_valid_ = !address.empty();
    }
    void SetUserName(std::string_view user_name) override {
        user_valid_ = !user_name.empty();
    }
    void SetHeartbeatInterval(std::uint32_t seconds) override {
        heartbeat_valid_ = seconds != 0U;
    }
    void SetHeartbeatTimeout(std::uint32_t seconds) override {
        timeout_valid_ = seconds != 0U;
    }
    void SetMessageEncoding(mdl::MDLMessageEncoding encoding) override {
        encoding_valid_ = encoding == mdl::MDLEID_BINARY;
    }
    void EnableMergeMessage(bool enable) override {
        merge_valid_ = !enable;
    }
    void SetSendMacAuth(bool) override {}
    void EnableServerSelect(bool) override {}
    void AddSubscription(const sdk::MessageKey&) override {
        ++subscription_count_;
    }

    [[nodiscard]] std::string Connect() override {
        return server_valid_ && user_valid_ && heartbeat_valid_ &&
                       timeout_valid_ && encoding_valid_ && merge_valid_ &&
                       subscription_count_ != 0U
                   ? std::string{}
                   : "invalid latency SDK fixture";
    }

    [[nodiscard]] bool Release(std::string* error) noexcept override {
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

private:
    std::size_t subscription_count_ = 0U;
    bool server_valid_ = false;
    bool user_valid_ = false;
    bool heartbeat_valid_ = false;
    bool timeout_valid_ = false;
    bool encoding_valid_ = false;
    bool merge_valid_ = false;
};

class LatencySdkManager final : public sdk::SdkManager {
public:
    explicit LatencySdkManager(
        std::shared_ptr<LatencySdkState> state)
        : state_(std::move(state)) {}

    void EnableLog(std::string_view, bool) override {}

    [[nodiscard]] std::unique_ptr<sdk::SdkSubscriber> CreateSubscriber(
        mdl::MessageHandlerBase* handler,
        bool multithread_callback) override {
        if (handler == nullptr || multithread_callback) {
            return nullptr;
        }
        state_->Install(handler);
        return std::make_unique<LatencySdkSubscriber>();
    }

    void Shutdown() override { state_->Shutdown(); }

    [[nodiscard]] bool Release(std::string* error) noexcept override {
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

private:
    std::shared_ptr<LatencySdkState> state_;
};

class LatencySdkFactory final : public sdk::SdkFactory {
public:
    explicit LatencySdkFactory(
        std::shared_ptr<LatencySdkState> state)
        : state_(std::move(state)) {}

    [[nodiscard]] std::unique_ptr<sdk::SdkManager> Create(
        int work_threads,
        int io_threads) override {
        if (work_threads <= 0 || io_threads <= 0) {
            return nullptr;
        }
        return std::make_unique<LatencySdkManager>(state_);
    }

private:
    std::shared_ptr<LatencySdkState> state_;
};

class UniqueFd final {
public:
    UniqueFd() = default;
    explicit UniqueFd(int fd) noexcept : fd_(fd) {}
    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;
    UniqueFd(UniqueFd&& other) noexcept : fd_(other.fd_) {
        other.fd_ = -1;
    }
    UniqueFd& operator=(UniqueFd&& other) noexcept {
        if (this != &other) {
            Reset(other.fd_);
            other.fd_ = -1;
        }
        return *this;
    }
    ~UniqueFd() { Reset(); }

    [[nodiscard]] int get() const noexcept { return fd_; }

    void Reset(int fd = -1) noexcept {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
        fd_ = fd;
    }

private:
    int fd_ = -1;
};

class ReaderHandle final {
public:
    ReaderHandle() = default;
    ReaderHandle(const ReaderHandle&) = delete;
    ReaderHandle& operator=(const ReaderHandle&) = delete;
    ~ReaderHandle() { Reset(); }

    [[nodiscard]] l2flow_shm_reader_v2* get() const noexcept {
        return reader_;
    }

    [[nodiscard]] l2flow_shm_reader_v2** output() noexcept {
        Reset();
        return &reader_;
    }

    void Reset() noexcept {
        l2flow_shm_reader_close_v2(reader_);
        reader_ = nullptr;
    }

private:
    l2flow_shm_reader_v2* reader_ = nullptr;
};

struct TimedPublicationRecord final {
    std::atomic<std::uint64_t> recv_monotonic_ns{0U};
    std::atomic<std::uint64_t> begin_monotonic_ns{0U};
    std::atomic<std::uint64_t> end_monotonic_ns{0U};
    std::atomic<std::uint8_t> success{0U};
};

class TimedAppliedSink final
    : public market::RealtimeAppliedRecordSinkV1 {
public:
    TimedAppliedSink(
        std::shared_ptr<ipc::RealtimeSharedMarketServiceV2> service,
        std::size_t maximum_sequence)
        : service_(std::move(service)),
          maximum_sequence_(maximum_sequence),
          records_(
              std::make_unique<TimedPublicationRecord[]>(
                  maximum_sequence + 1U)) {}

    [[nodiscard]] bool PublishApplied(
        std::size_t ordinal,
        const market::RealtimeHistoryRecordV1& record)
        noexcept override {
        const std::uint64_t sequence = record.ingress_sequence();
        if (service_ == nullptr || sequence == 0U ||
            sequence > maximum_sequence_) {
            if (service_ != nullptr) {
                service_->MarkCoverageLost();
            }
            return false;
        }
        TimedPublicationRecord& timing =
            records_[static_cast<std::size_t>(sequence)];
        timing.recv_monotonic_ns.store(
            record.recv_monotonic_ns() < 0
                ? 0U
                : static_cast<std::uint64_t>(
                      record.recv_monotonic_ns()),
            std::memory_order_relaxed);
        const std::uint64_t begin = MonotonicNowNs();
        timing.begin_monotonic_ns.store(
            begin, std::memory_order_relaxed);
        const bool published =
            begin != 0U &&
            service_->PublishApplied(ordinal, record);
        const std::uint64_t end = MonotonicNowNs();
        timing.success.store(
            published && end != 0U ? 1U : 0U,
            std::memory_order_relaxed);
        timing.end_monotonic_ns.store(
            end, std::memory_order_release);
        return published && end != 0U;
    }

    void MarkCoverageLost() noexcept override {
        if (service_ != nullptr) {
            service_->MarkCoverageLost();
        }
    }

    [[nodiscard]] bool Read(
        std::uint64_t sequence,
        std::uint64_t* recv,
        std::uint64_t* begin,
        std::uint64_t* end) const noexcept {
        if (sequence == 0U || sequence > maximum_sequence_ ||
            recv == nullptr || begin == nullptr || end == nullptr) {
            return false;
        }
        const TimedPublicationRecord& timing =
            records_[static_cast<std::size_t>(sequence)];
        const std::uint64_t observed_end =
            timing.end_monotonic_ns.load(std::memory_order_acquire);
        const std::uint64_t observed_recv =
            timing.recv_monotonic_ns.load(std::memory_order_relaxed);
        const std::uint64_t observed_begin =
            timing.begin_monotonic_ns.load(std::memory_order_relaxed);
        if (observed_end == 0U || observed_recv == 0U ||
            observed_begin < observed_recv ||
            observed_end < observed_begin ||
            timing.success.load(std::memory_order_relaxed) == 0U) {
            return false;
        }
        *recv = observed_recv;
        *begin = observed_begin;
        *end = observed_end;
        return true;
    }

private:
    std::shared_ptr<ipc::RealtimeSharedMarketServiceV2> service_;
    const std::size_t maximum_sequence_;
    std::unique_ptr<TimedPublicationRecord[]> records_;
};

class HistoryStageCollector final
    : public ipc::RealtimeHistoryPageStageObserverV2 {
public:
    void ObserveHistoryPageStageTiming(
        const ipc::RealtimeHistoryPageStageTimingV2& timing)
        noexcept override {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            timings_.push_back(timing);
        } catch (...) {
            failed_.store(true, std::memory_order_release);
        }
    }

    [[nodiscard]] std::vector<
        ipc::RealtimeHistoryPageStageTimingV2>
    Take() {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<ipc::RealtimeHistoryPageStageTimingV2> result;
        result.swap(timings_);
        return result;
    }

    void Clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        timings_.clear();
    }

    [[nodiscard]] bool failed() const noexcept {
        return failed_.load(std::memory_order_acquire);
    }

private:
    std::mutex mutex_;
    std::vector<ipc::RealtimeHistoryPageStageTimingV2> timings_;
    std::atomic<bool> failed_{false};
};

struct LatencySummary final {
    std::uint64_t minimum_ns = 0U;
    std::uint64_t maximum_ns = 0U;
    std::uint64_t mean_ns = 0U;
    std::uint64_t p50_ns = 0U;
    std::uint64_t p90_ns = 0U;
    std::uint64_t p95_ns = 0U;
    std::uint64_t p99_ns = 0U;
    std::uint64_t p999_ns = 0U;
};

[[nodiscard]] std::uint64_t ExactQuantile(
    const std::vector<std::uint64_t>& sorted,
    std::uint64_t numerator,
    std::uint64_t denominator) noexcept {
    if (sorted.empty() || denominator == 0U) {
        return 0U;
    }
    const std::uint64_t count =
        static_cast<std::uint64_t>(sorted.size());
    const std::uint64_t rank =
        (count / denominator) * numerator +
        ((count % denominator) * numerator + denominator - 1U) /
            denominator;
    const std::uint64_t one_based = std::max<std::uint64_t>(1U, rank);
    return sorted[static_cast<std::size_t>(
        std::min(count, one_based) - 1U)];
}

[[nodiscard]] LatencySummary SummarizeLatency(
    std::vector<std::uint64_t> values) {
    LatencySummary result{};
    if (values.empty()) {
        return result;
    }
    std::sort(values.begin(), values.end());
    long double sum = 0.0L;
    for (const std::uint64_t value : values) {
        sum += static_cast<long double>(value);
    }
    result.minimum_ns = values.front();
    result.maximum_ns = values.back();
    result.mean_ns = static_cast<std::uint64_t>(
        sum / static_cast<long double>(values.size()));
    result.p50_ns = ExactQuantile(values, 50U, 100U);
    result.p90_ns = ExactQuantile(values, 90U, 100U);
    result.p95_ns = ExactQuantile(values, 95U, 100U);
    result.p99_ns = ExactQuantile(values, 99U, 100U);
    result.p999_ns = ExactQuantile(values, 999U, 1000U);
    return result;
}

void PrintLatency(
    std::string_view name,
    const std::vector<std::uint64_t>& values) {
    const LatencySummary summary = SummarizeLatency(values);
    std::cout
        << "LATENCY name=" << name
        << " samples=" << values.size()
        << " min_ns=" << summary.minimum_ns
        << " mean_ns=" << summary.mean_ns
        << " p50_ns=" << summary.p50_ns
        << " p90_ns=" << summary.p90_ns
        << " p95_ns=" << summary.p95_ns
        << " p99_ns=" << summary.p99_ns
        << " p999_ns=" << summary.p999_ns
        << " max_ns=" << summary.maximum_ns << '\n';
}

void PrintStageLatency(
    std::string_view name,
    const runtime::RealtimeLatencyDistributionV1& distribution) {
    std::cout
        << "STAGE name=" << name
        << " samples=" << distribution.samples
        << " invalid=" << distribution.invalid_samples
        << " below_range=" << distribution.below_histogram_range
        << " above_range=" << distribution.above_histogram_range
        << " min_ns=" << distribution.minimum_ns
        << " mean_ns=" << distribution.mean_ns
        << " p50_ns=" << distribution.p50.estimate_ns
        << " p90_ns=" << distribution.p90.estimate_ns
        << " p95_ns=" << distribution.p95.estimate_ns
        << " p99_ns=" << distribution.p99.estimate_ns
        << " p999_ns=" << distribution.p999.estimate_ns
        << " max_ns=" << distribution.maximum_ns
        << " bucket_width_ns="
        << distribution.histogram_bucket_width_ns << '\n';
}

[[nodiscard]] constexpr std::string_view DecoderLaneName(
    std::size_t source) noexcept {
    constexpr std::array<std::string_view, 4U> names{
        "sh_snapshot",
        "sh_ngts_tick",
        "sz_snapshot",
        "sz_tick",
    };
    return source < names.size() ? names[source] : "invalid";
}

[[nodiscard]] std::string CpuAffinityText() {
    cpu_set_t set{};
    if (::sched_getaffinity(0, sizeof(set), &set) != 0) {
        return "unavailable";
    }
    std::ostringstream output;
    std::size_t count = 0U;
    bool first = true;
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
        if (!CPU_ISSET(cpu, &set)) {
            continue;
        }
        if (!first) {
            output << ',';
        }
        output << cpu;
        first = false;
        ++count;
    }
    output << ";count=" << count;
    return output.str();
}

class ScopedTempDirectory final {
public:
    ScopedTempDirectory() {
        char pattern[] = "/tmp/l2flow-ipc-v2-XXXXXX";
        char* const created = ::mkdtemp(pattern);
        if (created != nullptr && ::chmod(created, 0700) == 0) {
            path_ = created;
        } else if (created != nullptr) {
            static_cast<void>(::rmdir(created));
        }
    }

    ScopedTempDirectory(const ScopedTempDirectory&) = delete;
    ScopedTempDirectory& operator=(const ScopedTempDirectory&) =
        delete;

    ~ScopedTempDirectory() {
        if (!path_.empty()) {
            std::error_code error;
            static_cast<void>(
                std::filesystem::remove_all(path_, error));
        }
    }

    [[nodiscard]] bool valid() const noexcept {
        return !path_.empty();
    }

    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

class ProtocolChannel final {
public:
    explicit ProtocolChannel(int fd) noexcept : fd_(fd) {}

    ProtocolChannel(const ProtocolChannel&) = delete;
    ProtocolChannel& operator=(const ProtocolChannel&) = delete;

    [[nodiscard]] bool SendLine(std::string_view line) noexcept {
        std::string message;
        try {
            message.assign(line);
            message.push_back('\n');
        } catch (...) {
            return false;
        }
        std::size_t offset = 0U;
        while (offset < message.size()) {
            const ssize_t written = ::send(
                fd_.get(),
                message.data() + offset,
                message.size() - offset,
                MSG_NOSIGNAL);
            if (written > 0) {
                offset += static_cast<std::size_t>(written);
            } else if (written < 0 && errno == EINTR) {
                continue;
            } else {
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] bool ReadLine(
        std::chrono::milliseconds timeout,
        std::string* output) noexcept {
        if (output == nullptr || timeout.count() <= 0) {
            return false;
        }
        const auto deadline =
            std::chrono::steady_clock::now() + timeout;
        for (;;) {
            const std::size_t newline = buffer_.find('\n');
            if (newline != std::string::npos) {
                try {
                    *output = buffer_.substr(0U, newline);
                    buffer_.erase(0U, newline + 1U);
                } catch (...) {
                    return false;
                }
                return true;
            }
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                return false;
            }
            const auto remaining =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    deadline - now);
            pollfd descriptor{};
            descriptor.fd = fd_.get();
            descriptor.events = POLLIN;
            const int wait_ms = static_cast<int>(std::max<std::int64_t>(
                1,
                std::min<std::int64_t>(
                    remaining.count(),
                    std::numeric_limits<int>::max())));
            const int polled = ::poll(&descriptor, 1U, wait_ms);
            if (polled < 0 && errno == EINTR) {
                continue;
            }
            if (polled <= 0 ||
                (descriptor.revents &
                 (POLLERR | POLLNVAL)) != 0 ||
                (descriptor.revents & POLLIN) == 0) {
                return false;
            }
            std::array<char, 4096U> chunk{};
            const ssize_t received =
                ::recv(fd_.get(), chunk.data(), chunk.size(), 0);
            if (received > 0) {
                try {
                    buffer_.append(
                        chunk.data(),
                        static_cast<std::size_t>(received));
                } catch (...) {
                    return false;
                }
                if (buffer_.size() > 64U * 1024U) {
                    return false;
                }
            } else if (received < 0 && errno == EINTR) {
                continue;
            } else {
                return false;
            }
        }
    }

    [[nodiscard]] int fd() const noexcept { return fd_.get(); }

private:
    UniqueFd fd_;
    std::string buffer_;
};

[[nodiscard]] bool ParseLineUnsignedField(
    std::string_view line,
    std::string_view name,
    std::uint64_t* output) noexcept {
    if (output == nullptr || name.empty() ||
        name.find_first_of(" =\t\r\n") != std::string_view::npos) {
        return false;
    }
    std::size_t position = 0U;
    while (position < line.size()) {
        const std::size_t end = line.find(' ', position);
        const std::string_view item = line.substr(
            position,
            end == std::string_view::npos
                ? line.size() - position
                : end - position);
        if (item.size() > name.size() &&
            item.starts_with(name) &&
            item[name.size()] == '=') {
            const std::string_view value =
                item.substr(name.size() + 1U);
            if (value.empty()) {
                return false;
            }
            std::uint64_t parsed = 0U;
            for (const char character : value) {
                if (character < '0' || character > '9') {
                    return false;
                }
                const std::uint64_t digit =
                    static_cast<std::uint64_t>(character - '0');
                if (parsed >
                    (std::numeric_limits<std::uint64_t>::max() -
                     digit) /
                        10U) {
                    return false;
                }
                parsed = parsed * 10U + digit;
            }
            *output = parsed;
            return true;
        }
        if (end == std::string_view::npos) {
            break;
        }
        position = end + 1U;
    }
    return false;
}

void PrintHistoryPageStages(
    std::string_view workload,
    const std::vector<
        ipc::RealtimeHistoryPageStageTimingV2>& timings) {
    std::vector<std::uint64_t> cursor;
    std::vector<std::uint64_t> layout;
    std::vector<std::uint64_t> memfd_prepare;
    std::vector<std::uint64_t> projection;
    std::vector<std::uint64_t> memfd_finalize;
    std::vector<std::uint64_t> build;
    std::vector<std::uint64_t> token;
    std::vector<std::uint64_t> send;
    cursor.reserve(timings.size());
    layout.reserve(timings.size());
    memfd_prepare.reserve(timings.size());
    projection.reserve(timings.size());
    memfd_finalize.reserve(timings.size());
    build.reserve(timings.size());
    token.reserve(timings.size());
    send.reserve(timings.size());
    std::uint64_t records = 0U;
    std::uint64_t bytes = 0U;
    std::uint64_t clock_failures = 0U;
    for (const auto& timing : timings) {
        cursor.push_back(timing.cursor_read_ns);
        layout.push_back(timing.classify_layout_ns);
        memfd_prepare.push_back(timing.memfd_prepare_ns);
        projection.push_back(timing.projection_ns);
        memfd_finalize.push_back(timing.memfd_finalize_ns);
        build.push_back(timing.build_total_ns);
        token.push_back(timing.token_ns);
        send.push_back(timing.send_ns);
        records += timing.record_count;
        bytes += timing.page_mapping_bytes;
        clock_failures += timing.clock_read_failures;
    }
    const std::string prefix =
        "history_page_" + std::string(workload) + "_";
    PrintLatency(prefix + "cursor_read", cursor);
    PrintLatency(prefix + "classify_layout", layout);
    PrintLatency(prefix + "memfd_prepare", memfd_prepare);
    PrintLatency(prefix + "projection", projection);
    PrintLatency(prefix + "memfd_finalize", memfd_finalize);
    PrintLatency(prefix + "build_total", build);
    PrintLatency(prefix + "token", token);
    PrintLatency(prefix + "send", send);
    std::cout
        << "HISTORY_PAGE_STAGE_TOTAL workload=" << workload
        << " pages=" << timings.size()
        << " records=" << records
        << " mapping_bytes=" << bytes
        << " clock_failures=" << clock_failures << '\n';
}

struct PythonHistoryCommandResult final {
    std::uint64_t published_ns = 0U;
    std::uint64_t first_open_start_ns = 0U;
    std::uint64_t first_open_return_ns = 0U;
    std::uint64_t first_cursor_open_start_ns = 0U;
    std::uint64_t first_cursor_open_return_ns = 0U;
    std::uint64_t first_scan_start_ns = 0U;
    std::uint64_t first_complete_ns = 0U;
    std::vector<std::uint64_t> session_open_ns;
    std::vector<std::uint64_t> cursor_open_ns;
    std::vector<std::uint64_t> scan_ns;
    std::vector<std::uint64_t> open_return_to_complete_ns;
    std::vector<std::uint64_t> checkpoint_access_ns;
    std::vector<std::uint64_t> transaction_begin_ns;
    std::vector<std::uint64_t> atomic_commit_ns;
    std::vector<std::uint64_t> factor_update_ns;
    std::vector<std::uint64_t> factor_column_ns;
    std::vector<std::uint64_t> factor_math_ns;
    std::vector<std::uint64_t> consume_nonfactor_ns;
    std::vector<std::uint64_t> worker_page_read_ns;
    std::vector<std::uint64_t> worker_pipeline_ns;
    std::vector<std::uint64_t>
        selected_column_tuple_materialize_ns;
    std::vector<std::uint64_t>
        summed_ring_publish_to_validated_ready_ns;
    std::vector<std::uint64_t> parent_complete_consumption_ns;
};

[[nodiscard]] bool RunPythonHistoryCommand(
    ProtocolChannel* channel,
    std::string_view command,
    std::string_view sample_tag,
    std::size_t repeats,
    std::uint64_t expected_records,
    PythonHistoryCommandResult* output) {
    if (channel == nullptr || command.empty() ||
        sample_tag.empty() || repeats == 0U || output == nullptr ||
        !channel->SendLine(command)) {
        return false;
    }
    PythonHistoryCommandResult result{};
    std::string line;
    for (std::size_t sample = 0U; sample < repeats; ++sample) {
        if (!channel->ReadLine(
                std::chrono::seconds(120), &line) ||
            !line.starts_with(sample_tag)) {
            std::cerr << "unexpected history probe line: "
                      << line << '\n';
            return false;
        }
        std::uint64_t record_count = 0U;
        std::uint64_t published = 0U;
        std::uint64_t open_start = 0U;
        std::uint64_t open_return = 0U;
        std::uint64_t scan_start = 0U;
        std::uint64_t complete = 0U;
        if (!ParseLineUnsignedField(
                line, "record_count", &record_count) ||
            record_count != expected_records ||
            !ParseLineUnsignedField(
                line,
                "history_published_monotonic_ns",
                &published)) {
            std::cerr << "invalid history probe sample: "
                      << line << '\n';
            return false;
        }
        if (sample_tag == "HISTORY_SAMPLE") {
            std::uint64_t open_latency = 0U;
            std::uint64_t scan_latency = 0U;
            std::uint64_t open_to_complete = 0U;
            if (!ParseLineUnsignedField(
                    line, "open_call_start_ns", &open_start) ||
                !ParseLineUnsignedField(
                    line, "open_return_ns", &open_return) ||
                !ParseLineUnsignedField(
                    line, "scan_start_ns", &scan_start) ||
                !ParseLineUnsignedField(
                    line,
                    "eof_columns_complete_ns",
                    &complete) ||
                !ParseLineUnsignedField(
                    line, "python_open_ns", &open_latency) ||
                !ParseLineUnsignedField(
                    line, "complete_scan_ns", &scan_latency) ||
                !ParseLineUnsignedField(
                    line,
                    "open_return_to_eof_columns_complete_ns",
                    &open_to_complete)) {
                return false;
            }
            result.cursor_open_ns.push_back(open_latency);
            result.scan_ns.push_back(scan_latency);
            result.open_return_to_complete_ns.push_back(
                open_to_complete);
        } else if (sample_tag == "DELTA_SAMPLE") {
            std::uint64_t session_open = 0U;
            std::uint64_t cursor_open = 0U;
            std::uint64_t scan_latency = 0U;
            std::uint64_t checkpoint_access = 0U;
            std::uint64_t cursor_open_start = 0U;
            std::uint64_t cursor_open_return = 0U;
            std::uint64_t open_to_complete = 0U;
            if (!ParseLineUnsignedField(
                    line,
                    "session_open_call_start_ns",
                    &open_start) ||
                !ParseLineUnsignedField(
                    line,
                    "session_open_return_ns",
                    &open_return) ||
                !ParseLineUnsignedField(
                    line,
                    "cursor_open_call_start_ns",
                    &cursor_open_start) ||
                !ParseLineUnsignedField(
                    line,
                    "cursor_open_return_ns",
                    &cursor_open_return) ||
                !ParseLineUnsignedField(
                    line, "scan_start_ns", &scan_start) ||
                !ParseLineUnsignedField(
                    line, "checkpoint_return_ns", &complete) ||
                !ParseLineUnsignedField(
                    line, "session_open_ns", &session_open) ||
                !ParseLineUnsignedField(
                    line, "cursor_open_ns", &cursor_open) ||
                !ParseLineUnsignedField(
                    line, "scan_to_eof_ns", &scan_latency) ||
                !ParseLineUnsignedField(
                    line,
                    "checkpoint_access_ns",
                    &checkpoint_access) ||
                !ParseLineUnsignedField(
                    line,
                    "cursor_open_return_to_checkpoint_ns",
                    &open_to_complete)) {
                return false;
            }
            result.session_open_ns.push_back(session_open);
            result.cursor_open_ns.push_back(cursor_open);
            result.scan_ns.push_back(scan_latency);
            result.open_return_to_complete_ns.push_back(
                open_to_complete);
            result.checkpoint_access_ns.push_back(
                checkpoint_access);
            if (cursor_open_start < open_return ||
                cursor_open_return < cursor_open_start ||
                scan_start < cursor_open_return) {
                return false;
            }
            if (sample == 0U) {
                result.first_cursor_open_start_ns =
                    cursor_open_start;
                result.first_cursor_open_return_ns =
                    cursor_open_return;
            }
        } else if (sample_tag == "WORKER_DELTA_SAMPLE") {
            std::uint64_t cursor_open_start = 0U;
            std::uint64_t cursor_open_return = 0U;
            std::uint64_t worker_page_read = 0U;
            std::uint64_t worker_pipeline = 0U;
            std::uint64_t materialize = 0U;
            std::uint64_t summed_publish_to_ready = 0U;
            std::uint64_t parent_complete_consumption = 0U;
            std::uint64_t worker_pipeline_begin = 0U;
            std::uint64_t worker_publish_return = 0U;
            std::uint64_t parent_complete_ready = 0U;
            std::uint64_t checkpoint_access = 0U;
            std::uint64_t open_to_complete = 0U;
            std::uint64_t main_pid = 0U;
            std::uint64_t worker_pid = 0U;
            if (!ParseLineUnsignedField(
                    line, "open_call_start_ns", &open_start) ||
                !ParseLineUnsignedField(
                    line, "open_return_ns", &open_return) ||
                !ParseLineUnsignedField(
                    line,
                    "cursor_open_call_start_ns",
                    &cursor_open_start) ||
                !ParseLineUnsignedField(
                    line,
                    "cursor_open_return_ns",
                    &cursor_open_return) ||
                !ParseLineUnsignedField(
                    line, "scan_start_ns", &scan_start) ||
                !ParseLineUnsignedField(
                    line, "checkpoint_return_ns", &complete) ||
                !ParseLineUnsignedField(
                    line,
                    "worker_page_read_ns",
                    &worker_page_read) ||
                !ParseLineUnsignedField(
                    line,
                    "worker_pipeline_ns",
                    &worker_pipeline) ||
                !ParseLineUnsignedField(
                    line,
                    "selected_column_tuple_materialize_ns",
                    &materialize) ||
                !ParseLineUnsignedField(
                    line,
                    "summed_ring_publish_to_validated_ready_ns",
                    &summed_publish_to_ready) ||
                !ParseLineUnsignedField(
                    line,
                    "parent_complete_consumption_ns",
                    &parent_complete_consumption) ||
                !ParseLineUnsignedField(
                    line,
                    "worker_pipeline_begin_ns",
                    &worker_pipeline_begin) ||
                !ParseLineUnsignedField(
                    line,
                    "worker_complete_ring_publish_return_ns",
                    &worker_publish_return) ||
                !ParseLineUnsignedField(
                    line,
                    "parent_complete_ready_ns",
                    &parent_complete_ready) ||
                !ParseLineUnsignedField(
                    line,
                    "checkpoint_access_ns",
                    &checkpoint_access) ||
                !ParseLineUnsignedField(
                    line,
                    "cursor_open_return_to_checkpoint_ns",
                    &open_to_complete) ||
                !ParseLineUnsignedField(
                    line, "main_pid", &main_pid) ||
                !ParseLineUnsignedField(
                    line, "worker_pid", &worker_pid)) {
                return false;
            }
            if (main_pid == 0U || worker_pid == 0U ||
                main_pid == worker_pid ||
                cursor_open_start != open_start ||
                cursor_open_return != open_return ||
                scan_start < cursor_open_return ||
                open_return < open_start ||
                complete < scan_start ||
                open_to_complete != complete - open_return ||
                parent_complete_consumption !=
                    complete - scan_start ||
                worker_pipeline_begin < open_start ||
                worker_publish_return < worker_pipeline_begin ||
                parent_complete_ready < worker_publish_return ||
                complete < parent_complete_ready ||
                worker_page_read > worker_pipeline ||
                worker_pipeline !=
                    worker_publish_return -
                        worker_pipeline_begin ||
                materialize > parent_complete_consumption) {
                return false;
            }
            result.cursor_open_ns.push_back(
                open_return - open_start);
            result.scan_ns.push_back(
                complete - scan_start);
            result.open_return_to_complete_ns.push_back(
                open_to_complete);
            result.checkpoint_access_ns.push_back(
                checkpoint_access);
            result.worker_page_read_ns.push_back(
                worker_page_read);
            result.worker_pipeline_ns.push_back(
                worker_pipeline);
            result.selected_column_tuple_materialize_ns.push_back(
                materialize);
            result.summed_ring_publish_to_validated_ready_ns
                .push_back(
                summed_publish_to_ready);
            result.parent_complete_consumption_ns.push_back(
                parent_complete_consumption);
            if (sample == 0U) {
                result.first_cursor_open_start_ns =
                    cursor_open_start;
                result.first_cursor_open_return_ns =
                    cursor_open_return;
            }
        } else if (sample_tag == "ROLLING_SAMPLE") {
            std::uint64_t session_open = 0U;
            std::uint64_t cursor_open = 0U;
            std::uint64_t cursor_open_start = 0U;
            std::uint64_t cursor_open_return = 0U;
            std::uint64_t begin_start = 0U;
            std::uint64_t begin_return = 0U;
            std::uint64_t consume_latency = 0U;
            std::uint64_t commit_start = 0U;
            std::uint64_t begin_latency = 0U;
            std::uint64_t commit_latency = 0U;
            std::uint64_t factor_update = 0U;
            std::uint64_t factor_column = 0U;
            std::uint64_t factor_math = 0U;
            std::uint64_t consume_nonfactor = 0U;
            std::uint64_t cursor_to_commit = 0U;
            if (!ParseLineUnsignedField(
                    line,
                    "session_open_call_start_ns",
                    &open_start) ||
                !ParseLineUnsignedField(
                    line, "session_open_return_ns", &open_return) ||
                !ParseLineUnsignedField(
                    line,
                    "cursor_open_call_start_ns",
                    &cursor_open_start) ||
                !ParseLineUnsignedField(
                    line,
                    "cursor_open_return_ns",
                    &cursor_open_return) ||
                !ParseLineUnsignedField(
                    line,
                    "transaction_begin_start_ns",
                    &begin_start) ||
                !ParseLineUnsignedField(
                    line,
                    "transaction_begin_return_ns",
                    &begin_return) ||
                !ParseLineUnsignedField(
                    line, "consume_start_ns", &scan_start) ||
                !ParseLineUnsignedField(
                    line, "commit_start_ns", &commit_start) ||
                !ParseLineUnsignedField(
                    line, "commit_return_ns", &complete) ||
                !ParseLineUnsignedField(
                    line, "session_open_ns", &session_open) ||
                !ParseLineUnsignedField(
                    line, "cursor_open_ns", &cursor_open) ||
                !ParseLineUnsignedField(
                    line,
                    "transaction_begin_ns",
                    &begin_latency) ||
                !ParseLineUnsignedField(
                    line,
                    "consume_to_eof_ns",
                    &consume_latency) ||
                !ParseLineUnsignedField(
                    line, "atomic_commit_ns", &commit_latency) ||
                !ParseLineUnsignedField(
                    line, "factor_update_ns", &factor_update) ||
                !ParseLineUnsignedField(
                    line, "factor_column_ns", &factor_column) ||
                !ParseLineUnsignedField(
                    line, "factor_math_ns", &factor_math) ||
                !ParseLineUnsignedField(
                    line,
                    "consume_nonfactor_ns",
                    &consume_nonfactor) ||
                !ParseLineUnsignedField(
                    line,
                    "cursor_open_return_to_commit_ns",
                    &cursor_to_commit)) {
                return false;
            }
            result.session_open_ns.push_back(session_open);
            result.cursor_open_ns.push_back(cursor_open);
            result.scan_ns.push_back(consume_latency);
            result.open_return_to_complete_ns.push_back(
                cursor_to_commit);
            result.transaction_begin_ns.push_back(begin_latency);
            result.atomic_commit_ns.push_back(commit_latency);
            result.factor_update_ns.push_back(factor_update);
            result.factor_column_ns.push_back(factor_column);
            result.factor_math_ns.push_back(factor_math);
            result.consume_nonfactor_ns.push_back(
                consume_nonfactor);
            if (cursor_open_start < open_return ||
                cursor_open_return < cursor_open_start ||
                begin_start < cursor_open_return ||
                begin_return < begin_start ||
                scan_start < begin_return ||
                commit_start < scan_start ||
                complete < commit_start ||
                factor_column > factor_update ||
                factor_math > factor_update ||
                factor_update > consume_latency ||
                consume_nonfactor > consume_latency) {
                return false;
            }
            if (sample == 0U) {
                result.first_cursor_open_start_ns =
                    cursor_open_start;
                result.first_cursor_open_return_ns =
                    cursor_open_return;
            }
        } else {
            return false;
        }
        if (published == 0U || open_start < published ||
            open_return < open_start || scan_start < open_return ||
            complete < scan_start ||
            (result.published_ns != 0U &&
             result.published_ns != published)) {
            std::cerr << "non-monotonic history probe sample: "
                      << line << '\n';
            return false;
        }
        result.published_ns = published;
        if (sample == 0U) {
            result.first_open_start_ns = open_start;
            result.first_open_return_ns = open_return;
            result.first_scan_start_ns = scan_start;
            result.first_complete_ns = complete;
        }
        std::cout << "PYTHON_" << line << '\n';
    }
    if (!channel->ReadLine(std::chrono::seconds(30), &line) ||
        !line.starts_with("DONE ")) {
        std::cerr << "missing history probe DONE: " << line << '\n';
        return false;
    }
    std::cout << "PYTHON_" << line << '\n';
    *output = std::move(result);
    return true;
}

class PythonLatencyProcess final {
public:
    PythonLatencyProcess() = default;
    PythonLatencyProcess(const PythonLatencyProcess&) = delete;
    PythonLatencyProcess& operator=(const PythonLatencyProcess&) =
        delete;

    ~PythonLatencyProcess() {
        if (pid_ > 0) {
            static_cast<void>(::kill(pid_, SIGKILL));
            int status = 0;
            while (::waitpid(pid_, &status, 0) < 0 &&
                   errno == EINTR) {
            }
        }
    }

    void Adopt(pid_t pid, std::unique_ptr<ProtocolChannel> channel) {
        pid_ = pid;
        channel_ = std::move(channel);
    }

    [[nodiscard]] ProtocolChannel* channel() const noexcept {
        return channel_.get();
    }

    [[nodiscard]] bool Wait(std::chrono::seconds timeout) noexcept {
        if (pid_ <= 0) {
            return false;
        }
        const auto deadline =
            std::chrono::steady_clock::now() + timeout;
        int status = 0;
        for (;;) {
            const pid_t result = ::waitpid(pid_, &status, WNOHANG);
            if (result == pid_) {
                pid_ = -1;
                return WIFEXITED(status) && WEXITSTATUS(status) == 0;
            }
            if (result < 0 && errno != EINTR) {
                return false;
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

private:
    pid_t pid_ = -1;
    std::unique_ptr<ProtocolChannel> channel_;
};

struct SessionTransfer final {
    ipc::RealtimeControlResponseV2 response{};
    UniqueFd fd;
};

SessionTransfer RequestSession(
    const std::filesystem::path& socket_path,
    std::uint16_t opcode =
        static_cast<std::uint16_t>(
            ipc::RealtimeControlOpcodeV2::kGetSession)) {
    SessionTransfer result{};
    UniqueFd socket_fd(::socket(
        AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0));
    if (socket_fd.get() < 0) {
        return result;
    }
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    const std::string path = socket_path.string();
    if (path.size() >= sizeof(address.sun_path)) {
        return result;
    }
    std::memcpy(
        address.sun_path, path.c_str(), path.size() + 1U);
    if (::connect(
            socket_fd.get(),
            reinterpret_cast<const sockaddr*>(&address),
            static_cast<socklen_t>(
                offsetof(sockaddr_un, sun_path) +
                path.size() + 1U)) != 0) {
        return result;
    }
    ipc::RealtimeControlRequestV2 request{};
    request.magic = ipc::kRealtimeControlMagicV2;
    request.protocol_major = ipc::kRealtimeWireMajorV2;
    request.protocol_minor = ipc::kRealtimeWireMinorV2;
    request.opcode = opcode;
    request.message_bytes =
        static_cast<std::uint32_t>(sizeof(request));
    request.request_id = 0x1020304050607080ULL;
    if (::send(
            socket_fd.get(),
            &request,
            sizeof(request),
            MSG_NOSIGNAL) !=
        static_cast<ssize_t>(sizeof(request))) {
        return result;
    }

    iovec vector{};
    vector.iov_base = &result.response;
    vector.iov_len = sizeof(result.response);
    std::array<std::byte, CMSG_SPACE(sizeof(int))> control{};
    msghdr message{};
    message.msg_iov = &vector;
    message.msg_iovlen = 1U;
    message.msg_control = control.data();
    message.msg_controllen = control.size();
    const ssize_t received =
        ::recvmsg(socket_fd.get(), &message, MSG_CMSG_CLOEXEC);
    if (received !=
        static_cast<ssize_t>(sizeof(result.response))) {
        return result;
    }
    for (cmsghdr* header = CMSG_FIRSTHDR(&message);
         header != nullptr;
         header = CMSG_NXTHDR(&message, header)) {
        if (header->cmsg_level == SOL_SOCKET &&
            header->cmsg_type == SCM_RIGHTS &&
            header->cmsg_len == CMSG_LEN(sizeof(int))) {
            int received_fd = -1;
            std::memcpy(
                &received_fd,
                CMSG_DATA(header),
                sizeof(received_fd));
            result.fd.Reset(received_fd);
            break;
        }
    }
    return result;
}

[[nodiscard]] UniqueFd ConnectControlSocket(
    const std::filesystem::path& socket_path) {
    UniqueFd socket_fd(::socket(
        AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0));
    if (socket_fd.get() < 0) {
        return {};
    }
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    const std::string path = socket_path.string();
    if (path.size() >= sizeof(address.sun_path)) {
        return {};
    }
    std::memcpy(
        address.sun_path, path.c_str(), path.size() + 1U);
    if (::connect(
            socket_fd.get(),
            reinterpret_cast<const sockaddr*>(&address),
            static_cast<socklen_t>(
                offsetof(sockaddr_un, sun_path) +
                path.size() + 1U)) != 0) {
        return {};
    }
    return socket_fd;
}

template <typename Message>
[[nodiscard]] bool SendObject(int socket_fd, const Message& message) {
    return socket_fd >= 0 &&
           ::send(
               socket_fd,
               &message,
               sizeof(message),
               MSG_NOSIGNAL) ==
               static_cast<ssize_t>(sizeof(message));
}

template <typename Message>
[[nodiscard]] bool ReceiveObjectWithoutDescriptor(
    int socket_fd,
    Message* output) {
    if (socket_fd < 0 || output == nullptr) {
        return false;
    }
    *output = Message{};
    iovec vector{};
    vector.iov_base = output;
    vector.iov_len = sizeof(*output);
    std::array<std::byte, CMSG_SPACE(sizeof(int))> control{};
    msghdr message{};
    message.msg_iov = &vector;
    message.msg_iovlen = 1U;
    message.msg_control = control.data();
    message.msg_controllen = control.size();
    const ssize_t received =
        ::recvmsg(socket_fd, &message, MSG_CMSG_CLOEXEC);
    return received == static_cast<ssize_t>(sizeof(*output)) &&
           (message.msg_flags & (MSG_TRUNC | MSG_CTRUNC)) == 0 &&
           CMSG_FIRSTHDR(&message) == nullptr;
}

[[nodiscard]] bool RequestHistoryOpen(
    const std::filesystem::path& socket_path,
    std::uint64_t expected_generation,
    ipc::RealtimeHistoryOpenResponseV2* output) {
    if (output == nullptr) {
        return false;
    }
    UniqueFd channel = ConnectControlSocket(socket_path);
    ipc::RealtimeHistoryOpenRequestV2 request{};
    request.magic = ipc::kRealtimeControlMagicV2;
    request.protocol_major = ipc::kRealtimeWireMajorV2;
    request.protocol_minor = ipc::kRealtimeWireMinorV2;
    request.opcode = static_cast<std::uint16_t>(
        ipc::RealtimeHistoryControlOpcodeV2::kOpenHistory);
    request.message_bytes = sizeof(request);
    request.request_id = 0x5041525448495354ULL;
    request.instrument_id = 1U;
    request.requested_page_records = 1U;
    request.expected_generation = expected_generation;
    return channel.get() >= 0 && SendObject(channel.get(), request) &&
           ReceiveObjectWithoutDescriptor(channel.get(), output);
}

[[nodiscard]] bool RequestDeltaOpen(
    const std::filesystem::path& socket_path,
    std::uint64_t expected_generation,
    ipc::RealtimeInstrumentTickDeltaOpenSessionResponseV2* output) {
    if (output == nullptr) {
        return false;
    }
    UniqueFd channel = ConnectControlSocket(socket_path);
    ipc::RealtimeInstrumentTickDeltaOpenSessionRequestV2 request{};
    request.magic = ipc::kRealtimeControlMagicV2;
    request.protocol_major = ipc::kRealtimeWireMajorV2;
    request.protocol_minor = ipc::kRealtimeWireMinorV2;
    request.opcode = static_cast<std::uint16_t>(
        ipc::RealtimeInstrumentTickDeltaControlOpcodeV2::
            kOpenDeltaSession);
    request.message_bytes = sizeof(request);
    request.request_id = 0x5041525444454c54ULL;
    request.expected_generation = expected_generation;
    return channel.get() >= 0 && SendObject(channel.get(), request) &&
           ReceiveObjectWithoutDescriptor(channel.get(), output);
}

[[nodiscard]] bool CheckCanonicalHistoryReadErrorFrame(
    const std::filesystem::path& socket_path,
    std::uint64_t generation) {
    UniqueFd channel = ConnectControlSocket(socket_path);
    ipc::RealtimeHistoryOpenRequestV2 open{};
    open.magic = ipc::kRealtimeControlMagicV2;
    open.protocol_major = ipc::kRealtimeWireMajorV2;
    open.protocol_minor = ipc::kRealtimeWireMinorV2;
    open.opcode = static_cast<std::uint16_t>(
        ipc::RealtimeHistoryControlOpcodeV2::kOpenHistory);
    open.message_bytes = sizeof(open);
    open.request_id = 0x484953544f50454eULL;
    open.instrument_id = 1U;
    open.requested_page_records = 1U;
    open.expected_generation = generation;
    ipc::RealtimeHistoryOpenResponseV2 opened{};
    if (!SendObject(channel.get(), open) ||
        !ReceiveObjectWithoutDescriptor(channel.get(), &opened) ||
        opened.status != static_cast<std::uint16_t>(
            ipc::RealtimeHistoryControlStatusV2::kOk) ||
        opened.initial_read_token == 0U) {
        return false;
    }

    ipc::RealtimeHistoryReadRequestV2 malformed{};
    malformed.magic = ipc::kRealtimeControlMagicV2;
    malformed.protocol_major = ipc::kRealtimeWireMajorV2;
    malformed.protocol_minor = ipc::kRealtimeWireMinorV2;
    malformed.opcode = static_cast<std::uint16_t>(
        ipc::RealtimeHistoryControlOpcodeV2::kReadHistory);
    malformed.message_bytes = sizeof(malformed);
    malformed.request_id = 0x4849535452454144ULL;
    malformed.expected_page_index = 0U;
    malformed.read_token = 0U;
    ipc::RealtimeHistoryReadResponseV2 rejected{};
    return SendObject(channel.get(), malformed) &&
           ReceiveObjectWithoutDescriptor(channel.get(), &rejected) &&
           rejected.status == static_cast<std::uint16_t>(
               ipc::RealtimeHistoryControlStatusV2::kInvalidRequest) &&
           rejected.flags == 0U && rejected.record_count == 0U &&
           rejected.request_id == malformed.request_id &&
           rejected.page_mapping_bytes == 0U &&
           rejected.page_index == 0U &&
           rejected.generation == 0U &&
           rejected.next_read_token == 0U;
}

[[nodiscard]] bool CheckCanonicalDeltaReadErrorFrame(
    const std::filesystem::path& socket_path,
    std::uint64_t generation) {
    UniqueFd channel = ConnectControlSocket(socket_path);
    ipc::RealtimeInstrumentTickDeltaOpenSessionRequestV2 open_session{};
    open_session.magic = ipc::kRealtimeControlMagicV2;
    open_session.protocol_major = ipc::kRealtimeWireMajorV2;
    open_session.protocol_minor = ipc::kRealtimeWireMinorV2;
    open_session.opcode = static_cast<std::uint16_t>(
        ipc::RealtimeInstrumentTickDeltaControlOpcodeV2::
            kOpenDeltaSession);
    open_session.message_bytes = sizeof(open_session);
    open_session.request_id = 0x44454c5441534553ULL;
    open_session.expected_generation = generation;
    ipc::RealtimeInstrumentTickDeltaOpenSessionResponseV2
        session{};
    if (!SendObject(channel.get(), open_session) ||
        !ReceiveObjectWithoutDescriptor(channel.get(), &session) ||
        session.status != static_cast<std::uint16_t>(
            ipc::RealtimeInstrumentTickDeltaControlStatusV2::kOk) ||
        session.delta_session_token == 0U) {
        return false;
    }

    ipc::RealtimeInstrumentTickDeltaOpenInstrumentRequestV2
        open_instrument{};
    open_instrument.magic = ipc::kRealtimeControlMagicV2;
    open_instrument.protocol_major = ipc::kRealtimeWireMajorV2;
    open_instrument.protocol_minor = ipc::kRealtimeWireMinorV2;
    open_instrument.opcode = static_cast<std::uint16_t>(
        ipc::RealtimeInstrumentTickDeltaControlOpcodeV2::
            kOpenInstrumentDelta);
    open_instrument.message_bytes = sizeof(open_instrument);
    open_instrument.request_id = 0x44454c5441494e53ULL;
    open_instrument.instrument_id = 1U;
    open_instrument.requested_page_records = 1U;
    open_instrument.base_kind = static_cast<std::uint32_t>(
        ipc::RealtimeInstrumentTickDeltaBaseKindV2::kOrigin);
    open_instrument.delta_session_token =
        session.delta_session_token;
    ipc::RealtimeInstrumentTickDeltaOpenInstrumentResponseV2
        opened{};
    if (!SendObject(channel.get(), open_instrument) ||
        !ReceiveObjectWithoutDescriptor(channel.get(), &opened) ||
        opened.status != static_cast<std::uint16_t>(
            ipc::RealtimeInstrumentTickDeltaControlStatusV2::kOk) ||
        opened.initial_read_token == 0U) {
        return false;
    }

    ipc::RealtimeInstrumentTickDeltaReadRequestV2 malformed{};
    malformed.magic = ipc::kRealtimeControlMagicV2;
    malformed.protocol_major = ipc::kRealtimeWireMajorV2;
    malformed.protocol_minor = ipc::kRealtimeWireMinorV2;
    malformed.opcode = static_cast<std::uint16_t>(
        ipc::RealtimeInstrumentTickDeltaControlOpcodeV2::
            kReadInstrumentDelta);
    malformed.message_bytes = sizeof(malformed);
    malformed.request_id = 0x44454c5441524541ULL;
    malformed.expected_page_index = 0U;
    malformed.read_token = 0U;
    ipc::RealtimeInstrumentTickDeltaReadResponseV2 rejected{};
    return SendObject(channel.get(), malformed) &&
           ReceiveObjectWithoutDescriptor(channel.get(), &rejected) &&
           rejected.status == static_cast<std::uint16_t>(
               ipc::RealtimeInstrumentTickDeltaControlStatusV2::
                   kInvalidRequest) &&
           rejected.flags == 0U && rejected.record_count == 0U &&
           rejected.request_id == malformed.request_id &&
           rejected.page_mapping_bytes == 0U &&
           rejected.page_index == 0U &&
           rejected.target_generation == 0U &&
           rejected.next_read_token == 0U;
}

[[nodiscard]] bool SpawnPythonLatencyProbe(
    const std::filesystem::path& socket_path,
    std::uint32_t instrument_id,
    std::size_t active_count,
    std::size_t warmup,
    std::size_t measured,
    PythonLatencyProcess* output) {
#if defined(L2FLOW_V2_PYTHON_PROBE_EXECUTABLE) && \
    defined(L2FLOW_V2_PYTHON_LATENCY_SCRIPT) && \
    defined(L2FLOW_V2_PYTHON_SOURCE) && \
    defined(L2FLOW_V2_PYTHON_READER_LIBRARY)
    if (output == nullptr || instrument_id == 0U ||
        active_count == 0U ||
        warmup == 0U || measured == 0U) {
        return false;
    }
    std::array<int, 2U> sockets{-1, -1};
    if (::socketpair(
            AF_UNIX,
            SOCK_STREAM | SOCK_CLOEXEC,
            0,
            sockets.data()) != 0) {
        return false;
    }
    UniqueFd parent_socket(sockets[0U]);
    UniqueFd child_socket(sockets[1U]);
    std::array<std::string, 10U> arguments{{
        L2FLOW_V2_PYTHON_PROBE_EXECUTABLE,
        "-B",
        L2FLOW_V2_PYTHON_LATENCY_SCRIPT,
        socket_path.string(),
        L2FLOW_V2_PYTHON_READER_LIBRARY,
        L2FLOW_V2_PYTHON_SOURCE,
        std::to_string(instrument_id),
        std::to_string(active_count),
        std::to_string(warmup),
        std::to_string(measured),
    }};
    std::array<char*, 11U> argv{};
    for (std::size_t index = 0U; index < arguments.size(); ++index) {
        argv[index] = arguments[index].data();
    }

    posix_spawn_file_actions_t actions{};
    if (::posix_spawn_file_actions_init(&actions) != 0) {
        return false;
    }
    bool actions_valid = true;
    actions_valid =
        actions_valid &&
        ::posix_spawn_file_actions_adddup2(
            &actions, child_socket.get(), STDIN_FILENO) == 0;
    actions_valid =
        actions_valid &&
        ::posix_spawn_file_actions_adddup2(
            &actions, child_socket.get(), STDOUT_FILENO) == 0;
    actions_valid =
        actions_valid &&
        ::posix_spawn_file_actions_addclose(
            &actions, parent_socket.get()) == 0;
    if (child_socket.get() != STDIN_FILENO &&
        child_socket.get() != STDOUT_FILENO) {
        actions_valid =
            actions_valid &&
            ::posix_spawn_file_actions_addclose(
                &actions, child_socket.get()) == 0;
    }
    pid_t child = -1;
    const int spawn_error =
        actions_valid
            ? ::posix_spawn(
                  &child,
                  argv[0U],
                  &actions,
                  nullptr,
                  argv.data(),
                  environ)
            : EINVAL;
    static_cast<void>(
        ::posix_spawn_file_actions_destroy(&actions));
    if (spawn_error != 0 || child <= 0) {
        return false;
    }
    child_socket.Reset();
    const int channel_fd = ::dup(parent_socket.get());
    if (channel_fd < 0) {
        static_cast<void>(::kill(child, SIGKILL));
        int status = 0;
        while (::waitpid(child, &status, 0) < 0 &&
               errno == EINTR) {
        }
        return false;
    }
    output->Adopt(
        child,
        std::make_unique<ProtocolChannel>(
            channel_fd));
    return true;
#else
    static_cast<void>(socket_path);
    static_cast<void>(instrument_id);
    static_cast<void>(active_count);
    static_cast<void>(warmup);
    static_cast<void>(measured);
    static_cast<void>(output);
    return false;
#endif
}

[[nodiscard]] bool SpawnPythonHistoryLatencyProbe(
    const std::filesystem::path& socket_path,
    PythonLatencyProcess* output) {
#if defined(L2FLOW_V2_PYTHON_PROBE_EXECUTABLE) && \
    defined(L2FLOW_V2_PYTHON_HISTORY_LATENCY_SCRIPT) && \
    defined(L2FLOW_V2_PYTHON_SOURCE) && \
    defined(L2FLOW_V2_PYTHON_READER_LIBRARY)
    if (output == nullptr) {
        return false;
    }
    std::array<int, 2U> sockets{-1, -1};
    if (::socketpair(
            AF_UNIX,
            SOCK_STREAM | SOCK_CLOEXEC,
            0,
            sockets.data()) != 0) {
        return false;
    }
    UniqueFd parent_socket(sockets[0U]);
    UniqueFd child_socket(sockets[1U]);
    std::array<std::string, 6U> arguments{{
        L2FLOW_V2_PYTHON_PROBE_EXECUTABLE,
        "-B",
        L2FLOW_V2_PYTHON_HISTORY_LATENCY_SCRIPT,
        socket_path.string(),
        L2FLOW_V2_PYTHON_READER_LIBRARY,
        L2FLOW_V2_PYTHON_SOURCE,
    }};
    std::array<char*, 7U> argv{};
    for (std::size_t index = 0U; index < arguments.size(); ++index) {
        argv[index] = arguments[index].data();
    }

    posix_spawn_file_actions_t actions{};
    if (::posix_spawn_file_actions_init(&actions) != 0) {
        return false;
    }
    bool actions_valid = true;
    actions_valid =
        actions_valid &&
        ::posix_spawn_file_actions_adddup2(
            &actions, child_socket.get(), STDIN_FILENO) == 0;
    actions_valid =
        actions_valid &&
        ::posix_spawn_file_actions_adddup2(
            &actions, child_socket.get(), STDOUT_FILENO) == 0;
    actions_valid =
        actions_valid &&
        ::posix_spawn_file_actions_addclose(
            &actions, parent_socket.get()) == 0;
    if (child_socket.get() != STDIN_FILENO &&
        child_socket.get() != STDOUT_FILENO) {
        actions_valid =
            actions_valid &&
            ::posix_spawn_file_actions_addclose(
                &actions, child_socket.get()) == 0;
    }
    pid_t child = -1;
    const int spawn_error =
        actions_valid
            ? ::posix_spawn(
                  &child,
                  argv[0U],
                  &actions,
                  nullptr,
                  argv.data(),
                  environ)
            : EINVAL;
    static_cast<void>(
        ::posix_spawn_file_actions_destroy(&actions));
    if (spawn_error != 0 || child <= 0) {
        return false;
    }
    child_socket.Reset();
    const int channel_fd = ::dup(parent_socket.get());
    if (channel_fd < 0) {
        static_cast<void>(::kill(child, SIGKILL));
        int status = 0;
        while (::waitpid(child, &status, 0) < 0 &&
               errno == EINTR) {
        }
        return false;
    }
    output->Adopt(
        child,
        std::make_unique<ProtocolChannel>(channel_fd));
    return true;
#else
    static_cast<void>(socket_path);
    static_cast<void>(output);
    return false;
#endif
}

market::InstrumentKeyV1 Key(
    std::string_view source,
    std::string_view security_id) {
    market::InstrumentKeyV1 result{};
    result.market = market::MarketV1::kShanghai;
    result.security_id_source = Bytes(source);
    result.security_id = Bytes(security_id);
    return result;
}

market::InstrumentRuntimeMetadataV2 Metadata() {
    market::InstrumentRuntimeMetadataV2 result{};
    result.quantity_unit = market::QuantityUnitV1::kShare;
    result.security_type = market::SecurityTypeV1::kEquity;
    result.asset_scope =
        market::AssetScopeV1::kDocumentedCore;
    return result;
}

struct DailyRuntimeFixture final {
    std::shared_ptr<const market::DailyInstrumentCatalogV2> catalog;
    std::unique_ptr<market::InstrumentRuntimeStateV2> runtime_state;

    [[nodiscard]] explicit operator bool() const noexcept {
        return catalog != nullptr && runtime_state != nullptr;
    }
};

[[nodiscard]] DailyRuntimeFixture MakeDailyFixture(
    std::span<const market::InstrumentKeyV1> keys,
    std::uint64_t epoch) {
    DailyRuntimeFixture result{};
    if (keys.empty() || epoch == 0U) {
        return result;
    }
    std::vector<market::DailyInstrumentSourceEntryV2> source;
    try {
        source.reserve(keys.size());
        for (const market::InstrumentKeyV1& key : keys) {
            market::DailyInstrumentSourceEntryV2 entry{};
            entry.key = key;
            entry.metadata = Metadata();
            source.push_back(std::move(entry));
        }
    } catch (...) {
        return result;
    }
    market::DailyInstrumentCatalogConfigV2 config{};
    config.trade_date = kTradeDate;
    config.catalog_version = epoch;
    config.session_epoch = epoch;
    config.market_scope = market::kDailyCatalogMainlandScopeV2;
    config.coverage_complete = true;
    std::unique_ptr<market::DailyInstrumentCatalogV2> catalog;
    if (market::DailyInstrumentCatalogV2::Create(
            config, source, &catalog) !=
            market::DailyInstrumentCatalogCreateErrorV2::kNone ||
        catalog == nullptr) {
        return result;
    }
    result.catalog =
        std::shared_ptr<const market::DailyInstrumentCatalogV2>(
            std::move(catalog));
    if (market::InstrumentRuntimeStateV2::Create(
            *result.catalog, &result.runtime_state) !=
        market::InstrumentRuntimeStateErrorV2::kNone) {
        result = {};
    }
    return result;
}

[[nodiscard]] DailyRuntimeFixture MakeManualDailyFixture(
    std::size_t capacity,
    std::uint64_t epoch) {
    std::vector<market::InstrumentKeyV1> keys;
    try {
        keys.reserve(capacity);
        for (std::size_t index = 0U; index < capacity; ++index) {
            keys.push_back(Key(
                "",
                SixDigitSecurityId(
                    static_cast<std::uint32_t>(
                        600'001U + index))));
        }
    } catch (...) {
        return {};
    }
    return MakeDailyFixture(keys, epoch);
}

[[nodiscard]] DailyRuntimeFixture MakePipelineDailyFixture(
    std::uint64_t epoch) {
    market::InstrumentKeyV1 key{};
    key.market = market::MarketV1::kShenzhen;
    key.security_id_source = Bytes("102 ");
    key.security_id = Bytes("000001");
    return MakeDailyFixture(std::span(&key, 1U), epoch);
}

[[nodiscard]] DailyRuntimeFixture MakeBenchmarkDailyFixture(
    std::size_t shenzhen_instrument_count,
    std::uint64_t epoch,
    bool include_shanghai_tick_instrument) {
    if (shenzhen_instrument_count > 14'599U) {
        return {};
    }
    std::vector<market::InstrumentKeyV1> keys;
    try {
        keys.reserve(
            shenzhen_instrument_count +
            (include_shanghai_tick_instrument ? 1U : 0U));
        if (include_shanghai_tick_instrument) {
            keys.push_back(Key("", "600001"));
        }
        for (std::size_t index = 1U;
             index <= shenzhen_instrument_count;
             ++index) {
            const std::string security_id =
                ShenzhenAShareSecurityId(
                    static_cast<std::uint32_t>(index));
            if (security_id.empty()) {
                return {};
            }
            market::InstrumentKeyV1 key{};
            key.market = market::MarketV1::kShenzhen;
            key.security_id_source = Bytes("102 ");
            key.security_id = Bytes(security_id);
            keys.push_back(std::move(key));
        }
    } catch (...) {
        return {};
    }
    return MakeDailyFixture(keys, epoch);
}

[[nodiscard]] DailyRuntimeFixture MakeHistoryBenchmarkDailyFixture(
    std::size_t shenzhen_instrument_count,
    std::size_t shanghai_instrument_count,
    std::uint64_t epoch) {
    if (shenzhen_instrument_count > 14'599U ||
        shanghai_instrument_count == 0U ||
        shanghai_instrument_count > 99'999U) {
        return {};
    }
    std::vector<market::InstrumentKeyV1> keys;
    try {
        keys.reserve(
            shenzhen_instrument_count + shanghai_instrument_count);
        for (std::size_t index = 0U;
             index < shanghai_instrument_count;
             ++index) {
            keys.push_back(Key(
                "",
                SixDigitSecurityId(
                    static_cast<std::uint32_t>(600'001U + index))));
        }
        for (std::size_t index = 1U;
             index <= shenzhen_instrument_count;
             ++index) {
            const std::string security_id =
                ShenzhenAShareSecurityId(
                    static_cast<std::uint32_t>(index));
            if (security_id.empty()) {
                return {};
            }
            market::InstrumentKeyV1 key{};
            key.market = market::MarketV1::kShenzhen;
            key.security_id_source = Bytes("102 ");
            key.security_id = Bytes(security_id);
            keys.push_back(std::move(key));
        }
    } catch (...) {
        return {};
    }
    return MakeDailyFixture(keys, epoch);
}

[[nodiscard]] DailyRuntimeFixture MakeThroughputBenchmarkDailyFixture(
    std::size_t instruments_per_market,
    std::uint64_t epoch) {
    if (instruments_per_market == 0U ||
        instruments_per_market > 14'599U) {
        return {};
    }
    return MakeHistoryBenchmarkDailyFixture(
        instruments_per_market,
        instruments_per_market,
        epoch);
}

void FillCommon(
    market::DecodedMarketCommonV1* common_record,
    market::MarketEventKindV1 kind,
    std::uint8_t source_slot,
    std::uint64_t source_sequence,
    std::uint64_t ingress_sequence,
    std::uint32_t instrument_id,
    std::size_t ordinal) {
    *common_record = {};
    common_record->kind = kind;
    common_record->market = market::MarketV1::kShanghai;
    common_record->origin.source_stream_id =
        kSourceStreamIds[source_slot];
    common_record->origin.trade_date = kTradeDate;
    common_record->origin.source_sequence = source_sequence;
    common_record->origin.vendor_sequence_id =
        10'000U + ingress_sequence;
    common_record->origin.recv_realtime_ns =
        static_cast<std::int64_t>(20'000U + ingress_sequence);
    common_record->origin.recv_monotonic_ns =
        static_cast<std::int64_t>(30'000U + ingress_sequence);
    common_record->instrument_id = instrument_id;
    common_record->ordinal = ordinal;
    common_record->quantity_unit =
        market::QuantityUnitV1::kShare;
    common_record->security_type =
        market::SecurityTypeV1::kEquity;
    common_record->asset_scope =
        market::AssetScopeV1::kDocumentedCore;
}

std::optional<market::RealtimeHistoryEventInputV1> SnapshotInput(
    std::uint64_t source_sequence,
    std::uint64_t ingress_sequence) {
    market::ShanghaiSnapshotV1 snapshot{};
    FillCommon(
        &snapshot.common,
        market::MarketEventKindV1::kShanghaiSnapshot,
        0U,
        source_sequence,
        ingress_sequence,
        1U,
        0U);
    snapshot.last_price.valid = true;
    snapshot.last_price.raw = 1'234'000;
    snapshot.last_price.normalized_p6 = 1'234'000;
    snapshot.last_price.scale = 6U;
    snapshot.trade_volume.valid = true;
    snapshot.trade_volume.raw =
        static_cast<std::int64_t>(100U + ingress_sequence);
    market::DecodedMarketEventV1 event(std::move(snapshot));
    return market::RealtimeHistoryEventInputV1::Create(
        0U, ingress_sequence, std::move(event), 0U);
}

std::optional<market::RealtimeHistoryEventInputV1> TickInput(
    std::uint64_t source_sequence,
    std::uint64_t ingress_sequence,
    std::uint64_t tick_stream_sequence) {
    market::ShanghaiTickV1 tick{};
    FillCommon(
        &tick.common,
        market::MarketEventKindV1::kShanghaiTick,
        1U,
        source_sequence,
        ingress_sequence,
        1U,
        0U);
    tick.common.exchange_time.raw_hhmmssmmm = 93000000U;
    tick.common.exchange_time.nanoseconds_since_midnight =
        34'200'000'000'000ULL + ingress_sequence;
    tick.common.exchange_time.unix_nanoseconds =
        1'785'254'400'000'000'000LL +
        static_cast<std::int64_t>(
            tick.common.exchange_time
                .nanoseconds_since_midnight);
    tick.common.exchange_time.valid = true;
    tick.common.exchange_time.unix_nanoseconds_valid = true;
    tick.business_index =
        static_cast<std::int64_t>(500U + source_sequence);
    tick.channel = 9;
    tick.fields.action = market::TickActionV1::kTrade;
    tick.fields.side = market::SideV1::kUnknown;
    tick.fields.order_type = market::OrderTypeV1::kUnknown;
    tick.fields.aggressor = market::AggressorV1::kBuy;
    tick.fields.phase = market::TradingPhaseV1::kContinuous;
    tick.fields.buy_order_id = 11'001;
    tick.fields.sell_order_id = 22'002;
    tick.fields.price.valid = true;
    tick.fields.price.raw = 1'235;
    tick.fields.price.normalized_p6 = 1'235'000;
    tick.fields.price.scale = 3U;
    tick.fields.quantity.valid = true;
    tick.fields.quantity.raw = 101;
    tick.fields.quantity.scale = 0U;
    tick.fields.trade_amount.valid = true;
    tick.fields.trade_amount.raw = 124'735;
    tick.fields.trade_amount.normalized_p6 = 124'735'000;
    tick.fields.trade_amount.scale = 3U;
    tick.fields.validity_bitmap =
        market::kTickPriceValidV1 |
        market::kTickQuantityValidV1 |
        market::kTickTradeAmountValidV1 |
        market::kTickExchangeTimeValidV1 |
        market::kTickBuyOrderIdValidV1 |
        market::kTickSellOrderIdValidV1 |
        market::kTickAggressorValidV1 |
        market::kTickPhaseValidV1;
    tick.raw_type = "T";
    tick.raw_tick_flag = "B";
    market::DecodedMarketEventV1 event(std::move(tick));
    return market::RealtimeHistoryEventInputV1::Create(
        1U,
        ingress_sequence,
        std::move(event),
        tick_stream_sequence);
}

std::optional<market::RealtimeHistoryEventInputV1> AddInput(
    std::uint64_t source_sequence,
    std::uint64_t ingress_sequence,
    std::uint64_t tick_stream_sequence) {
    market::ShanghaiTickV1 tick{};
    FillCommon(
        &tick.common,
        market::MarketEventKindV1::kShanghaiTick,
        1U,
        source_sequence,
        ingress_sequence,
        1U,
        0U);
    tick.common.exchange_time.raw_hhmmssmmm = 93000000U;
    tick.common.exchange_time.nanoseconds_since_midnight =
        34'200'000'000'000ULL + ingress_sequence;
    tick.common.exchange_time.unix_nanoseconds =
        1'785'254'400'000'000'000LL +
        static_cast<std::int64_t>(
            tick.common.exchange_time
                .nanoseconds_since_midnight);
    tick.common.exchange_time.valid = true;
    tick.common.exchange_time.unix_nanoseconds_valid = true;
    tick.business_index =
        static_cast<std::int64_t>(500U + source_sequence);
    tick.channel = 9;
    tick.fields.action = market::TickActionV1::kAdd;
    tick.fields.side = market::SideV1::kBuy;
    tick.fields.order_type = market::OrderTypeV1::kUnknown;
    tick.fields.aggressor = market::AggressorV1::kUnknown;
    tick.fields.phase = market::TradingPhaseV1::kContinuous;
    tick.fields.primary_order_id = 11'001;
    tick.fields.buy_order_id = 11'001;
    tick.fields.price.valid = true;
    tick.fields.price.raw = 1'236;
    tick.fields.price.normalized_p6 = 1'236'000;
    tick.fields.price.scale = 3U;
    tick.fields.quantity.valid = true;
    tick.fields.quantity.raw = 50;
    tick.fields.quantity.scale = 0U;
    // For an A message the decoder retains source TradeMoney in its p3
    // decimal field and separately projects the documented matched quantity.
    tick.fields.trade_amount.valid = false;
    tick.fields.trade_amount.raw = 101'000;
    tick.fields.trade_amount.scale = 3U;
    tick.fields.matched_quantity.valid = true;
    tick.fields.matched_quantity.raw = 101;
    tick.fields.matched_quantity.scale = 0U;
    tick.fields.validity_bitmap =
        market::kTickPriceValidV1 |
        market::kTickQuantityValidV1 |
        market::kTickMatchedQuantityValidV1 |
        market::kTickPrimaryOrderIdValidV1 |
        market::kTickSideValidV1 |
        market::kTickPhaseValidV1 |
        market::kTickExchangeTimeValidV1;
    tick.raw_type = "A";
    tick.raw_tick_flag = "B";
    market::DecodedMarketEventV1 event(std::move(tick));
    return market::RealtimeHistoryEventInputV1::Create(
        1U,
        ingress_sequence,
        std::move(event),
        tick_stream_sequence);
}

bool Submit(
    market::RealtimeHistoryRuntimeV1* runtime,
    std::optional<market::RealtimeHistoryEventInputV1> input) {
    return runtime != nullptr && input.has_value() &&
           runtime->TrySubmit(std::move(*input)) ==
               market::RealtimeHistorySubmitErrorV1::kNone;
}

template <typename Predicate>
bool WaitUntil(Predicate predicate) {
    const auto deadline =
        std::chrono::steady_clock::now() +
        std::chrono::seconds(3);
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::yield();
    }
    return predicate();
}

bool RunPythonKnownIdProbe(
    const std::filesystem::path& socket_path) {
#if defined(L2FLOW_V2_PYTHON_PROBE_EXECUTABLE) && \
    defined(L2FLOW_V2_PYTHON_PROBE_SCRIPT) && \
    defined(L2FLOW_V2_PYTHON_SOURCE) && \
    defined(L2FLOW_V2_PYTHON_READER_LIBRARY)
    std::array<std::string, 8U> arguments{{
        L2FLOW_V2_PYTHON_PROBE_EXECUTABLE,
        "-B",
        L2FLOW_V2_PYTHON_PROBE_SCRIPT,
        socket_path.string(),
        L2FLOW_V2_PYTHON_READER_LIBRARY,
        L2FLOW_V2_PYTHON_SOURCE,
        "1",
        "10000",
    }};
    std::array<char*, 9U> argv{};
    for (std::size_t index = 0U; index < arguments.size(); ++index) {
        argv[index] = arguments[index].data();
    }
    pid_t child = -1;
    const int spawn_error = ::posix_spawn(
        &child,
        argv[0U],
        nullptr,
        nullptr,
        argv.data(),
        environ);
    if (!Expect(
            spawn_error == 0 && child > 0,
            "spawn real Python Wire V2 hot-read probe")) {
        return false;
    }

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    int status = 0;
    for (;;) {
        const pid_t result = ::waitpid(child, &status, WNOHANG);
        if (result == child) {
            return Expect(
                WIFEXITED(status) && WEXITSTATUS(status) == 0,
                "real Python Wire V2 hot-read probe passes");
        }
        if (result < 0 && errno != EINTR) {
            return Expect(false, "wait for Python Wire V2 probe");
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            static_cast<void>(::kill(child, SIGKILL));
            do {
                errno = 0;
            } while (::waitpid(child, &status, 0) < 0 &&
                     errno == EINTR);
            return Expect(false, "Python Wire V2 probe timed out");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
#else
    static_cast<void>(socket_path);
    return true;
#endif
}

bool RunPythonDerivedHistorySmoke(
    const std::filesystem::path& socket_path,
    std::uint64_t generation) {
#if defined(L2FLOW_V2_PYTHON_PROBE_EXECUTABLE) && \
    defined(L2FLOW_V2_PYTHON_DERIVED_HISTORY_SCRIPT) && \
    defined(L2FLOW_V2_PYTHON_SOURCE) && \
    defined(L2FLOW_V2_PYTHON_READER_LIBRARY)
    std::array<std::string, 8U> arguments{{
        L2FLOW_V2_PYTHON_PROBE_EXECUTABLE,
        "-B",
        L2FLOW_V2_PYTHON_DERIVED_HISTORY_SCRIPT,
        socket_path.string(),
        L2FLOW_V2_PYTHON_READER_LIBRARY,
        L2FLOW_V2_PYTHON_SOURCE,
        "1",
        std::to_string(generation),
    }};
    std::array<char*, 9U> argv{};
    for (std::size_t index = 0U; index < arguments.size(); ++index) {
        argv[index] = arguments[index].data();
    }
    pid_t child = -1;
    const int spawn_error = ::posix_spawn(
        &child,
        argv[0U],
        nullptr,
        nullptr,
        argv.data(),
        environ);
    if (!Expect(
            spawn_error == 0 && child > 0,
            "spawn real Python derived-history probe")) {
        return false;
    }

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    int status = 0;
    for (;;) {
        const pid_t result = ::waitpid(child, &status, WNOHANG);
        if (result == child) {
            return Expect(
                WIFEXITED(status) && WEXITSTATUS(status) == 0,
                "real Python derived-history probe passes");
        }
        if (result < 0 && errno != EINTR) {
            return Expect(
                false, "wait for Python derived-history probe");
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            static_cast<void>(::kill(child, SIGKILL));
            do {
                errno = 0;
            } while (::waitpid(child, &status, 0) < 0 &&
                     errno == EINTR);
            return Expect(
                false, "Python derived-history probe timed out");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
#else
    static_cast<void>(socket_path);
    static_cast<void>(generation);
    return true;
#endif
}

bool RunPythonHistoryDeltaSmoke(
    const std::filesystem::path& socket_path,
    std::uint64_t generation,
    std::uint64_t history_records,
    bool include_delta) {
#if defined(L2FLOW_V2_PYTHON_PROBE_EXECUTABLE) && \
    defined(L2FLOW_V2_PYTHON_HISTORY_LATENCY_SCRIPT) && \
    defined(L2FLOW_V2_PYTHON_SOURCE) && \
    defined(L2FLOW_V2_PYTHON_READER_LIBRARY)
    PythonLatencyProcess stream_python;
    const bool spawned =
        SpawnPythonHistoryLatencyProbe(
            socket_path, &stream_python) &&
        stream_python.channel() != nullptr;
    if (!Expect(
            spawned,
            "spawn native Wire V2 history/delta Python smoke probe")) {
        return false;
    }
    ProtocolChannel* const stream_protocol =
        stream_python.channel();
    std::string stream_line;
    if (!Expect(
            stream_protocol->ReadLine(
                std::chrono::seconds(30), &stream_line) &&
                stream_line.starts_with("READY "),
            "Python history/delta smoke probe opens Wire V2")) {
        return false;
    }
    PythonHistoryCommandResult history_result{};
    PythonHistoryCommandResult delta_result{};
    PythonHistoryCommandResult worker_delta_result{};
    const std::string history_command =
        "HISTORY 1 " + std::to_string(generation) +
        " price 1 " + std::to_string(history_records);
    if (!Expect(
            RunPythonHistoryCommand(
                stream_protocol,
                history_command,
                "HISTORY_SAMPLE",
                1U,
                history_records,
                &history_result),
            history_records == 0U
                ? "Python reaches explicit EOF for bound no-data history"
                : "Python reaches explicit EOF for three-record history")) {
        return false;
    }
    const std::string delta_command =
        "DELTA_ORIGIN 1 " + std::to_string(generation) +
        " price 1 1";
    if (include_delta &&
        !Expect(
            RunPythonHistoryCommand(
                stream_protocol,
                delta_command,
                "DELTA_SAMPLE",
                1U,
                1U,
                &delta_result) &&
                delta_result.checkpoint_access_ns.size() == 1U,
            "Python returns the tick checkpoint only after delta EOF")) {
        return false;
    }
    const std::string worker_delta_command =
        "WORKER_DELTA_ORIGIN 1 " + std::to_string(generation) +
        " price 1 1";
    if (include_delta &&
        !Expect(
            RunPythonHistoryCommand(
                stream_protocol,
                worker_delta_command,
                "WORKER_DELTA_SAMPLE",
                1U,
                1U,
                &worker_delta_result) &&
                worker_delta_result.worker_page_read_ns.size() ==
                    1U &&
                worker_delta_result.worker_pipeline_ns.size() ==
                    1U &&
                worker_delta_result
                        .selected_column_tuple_materialize_ns
                        .size() ==
                    1U,
            "isolated worker consumes delta and returns fixed-ring result")) {
        return false;
    }
    if (!Expect(
            stream_protocol->SendLine("QUIT") &&
                stream_protocol->ReadLine(
                    std::chrono::seconds(30), &stream_line) &&
                stream_line.starts_with("BYE ") &&
                stream_python.Wait(std::chrono::seconds(30)),
            "Python history/delta smoke probe exits cleanly")) {
        return false;
    }
    return true;
#else
    static_cast<void>(socket_path);
    static_cast<void>(generation);
    static_cast<void>(history_records);
    static_cast<void>(include_delta);
    return true;
#endif
}

bool ReadNativeRawEventHistory(
    const std::filesystem::path& socket_path,
    const l2flow_shm_session_info_v2& expected_session,
    std::uint64_t expected_generation,
    const ipc::InstrumentRawEventHistoryCheckpointV2*
        base_checkpoint,
    std::span<const std::uint64_t> expected_ingress_sequences,
    std::span<const std::uint64_t> expected_tick_sequences,
    ipc::InstrumentRawEventHistoryCheckpointV2* checkpoint_output,
    std::string_view label,
    std::uint8_t expected_source_slot = 1U,
    std::uint8_t expected_event_kind = 2U,
    std::uint8_t expected_market = 1U) {
    if (checkpoint_output == nullptr ||
        expected_ingress_sequences.size() !=
            expected_tick_sequences.size()) {
        return false;
    }
    ipc::InstrumentRawEventHistorySessionV2 session;
    const auto open_error =
        ipc::InstrumentRawEventHistorySessionV2::Open(
            socket_path.c_str(),
            expected_session,
            expected_generation,
            3'000U,
            &session);
    bool ok = Expect(
        open_error ==
                ipc::InstrumentRawEventHistoryErrorV2::kNone &&
            session.is_open(),
        std::string(label) + " opens pinned native session");
    if (!ok) {
        std::cerr
            << label << " session error="
            << ipc::InstrumentRawEventHistoryErrorNameV2(
                   open_error)
            << '\n';
        return false;
    }
    ipc::InstrumentRawEventHistoryEndpointV2 target{};
    ok &= Expect(
        session.Target(&target) ==
                ipc::InstrumentRawEventHistoryErrorV2::kNone &&
            target.generation == expected_generation &&
            target.session_epoch == expected_session.session_epoch &&
            target.trade_date == expected_session.trade_date,
        std::string(label) + " exposes the pinned target identity");

    ipc::InstrumentRawEventHistoryCursorV2 cursor;
    const auto cursor_error =
        base_checkpoint == nullptr
            ? session.OpenFull(1U, 1U, &cursor)
            : session.OpenUpdate(
                  1U, 1U, *base_checkpoint, &cursor);
    ok &= Expect(
        cursor_error ==
                ipc::InstrumentRawEventHistoryErrorV2::kNone &&
            cursor.is_open(),
        std::string(label) + " opens one raw-event cursor");
    if (cursor_error !=
            ipc::InstrumentRawEventHistoryErrorV2::kNone ||
        !cursor.is_open()) {
        std::cerr
            << label << " cursor error="
            << ipc::InstrumentRawEventHistoryErrorNameV2(
                   cursor_error)
            << '\n';
        return false;
    }

    ipc::InstrumentRawEventHistoryMetadataV2 metadata{};
    ok &= Expect(
        cursor.Metadata(&metadata) ==
                ipc::InstrumentRawEventHistoryErrorV2::kNone &&
            metadata.base_kind ==
                (base_checkpoint == nullptr
                     ? L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_ORIGIN_V2
                     : L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_CHECKPOINT_V2) &&
            metadata.target_checkpoint.instrument_id == 1U &&
            metadata.target_checkpoint.generation.generation ==
                expected_generation &&
            metadata.delta_event_record_count ==
                expected_ingress_sequences.size(),
        std::string(label) +
            " returns exact raw-event delta metadata");
    ipc::InstrumentRawEventHistoryCheckpointV2 unverified{};
    ok &= Expect(
        cursor.VerifiedCheckpoint(&unverified) ==
            ipc::InstrumentRawEventHistoryErrorV2::kNotReady,
        std::string(label) +
            " withholds checkpoint before explicit EOF");

    std::vector<std::uint64_t> ingress_sequences;
    std::vector<std::uint64_t> tick_sequences;
    bool observed_eof = false;
    while (!observed_eof && ok) {
        ipc::InstrumentRawEventHistoryPageViewV2 page;
        const auto read_error = cursor.ReadPage(&page);
        ok &= Expect(
            read_error ==
                ipc::InstrumentRawEventHistoryErrorV2::kNone,
            std::string(label) + " reads one immutable native page");
        if (read_error !=
            ipc::InstrumentRawEventHistoryErrorV2::kNone) {
            std::cerr
                << label << " read error="
                << ipc::InstrumentRawEventHistoryErrorNameV2(
                       read_error)
                << '\n';
            break;
        }
        if (page.eof()) {
            observed_eof = true;
            ok &= Expect(
                page.records().empty() &&
                    page.mapping_bytes() == 0U &&
                    page.cumulative_record_count() ==
                        expected_ingress_sequences.size(),
                std::string(label) +
                    " reaches reconciled explicit EOF");
            continue;
        }
        const auto records = page.records();
        ok &= Expect(
            records.size() == 1U &&
                page.mapping_bytes() ==
                    ipc::
                        kRealtimeInstrumentTickDeltaPageHeaderBytesV2 +
                        sizeof(ipc::RealtimeWireTickPayloadV2),
            std::string(label) +
                " exposes one borrowed normalized Wire tick");
        for (const ipc::RealtimeWireTickPayloadV2& record :
             records) {
            ingress_sequences.push_back(
                record.common.ingress_sequence);
            tick_sequences.push_back(
                record.common.tick_stream_sequence);
            ok &= record.common.instrument_id == 1U &&
                  record.common.source_slot == expected_source_slot &&
                  record.common.event_kind == expected_event_kind &&
                  record.common.market == expected_market;
        }
    }
    ok &= Expect(
        observed_eof &&
            std::equal(
                ingress_sequences.begin(),
                ingress_sequences.end(),
                expected_ingress_sequences.begin(),
                expected_ingress_sequences.end()) &&
            std::equal(
                tick_sequences.begin(),
                tick_sequences.end(),
                expected_tick_sequences.begin(),
                expected_tick_sequences.end()),
        std::string(label) +
            " preserves exact raw ingress/tick order");
    ok &= Expect(
        cursor.VerifiedCheckpoint(checkpoint_output) ==
                ipc::InstrumentRawEventHistoryErrorV2::kNone &&
            checkpoint_output->generation.generation ==
                expected_generation &&
            checkpoint_output->instrument_event_record_count ==
                (base_checkpoint == nullptr
                     ? expected_ingress_sequences.size()
                     : base_checkpoint
                               ->instrument_event_record_count +
                           expected_ingress_sequences.size()),
        std::string(label) +
            " releases checkpoint only after EOF reconciliation");
    return ok;
}

bool CheckHistoryExpectedDailyCatalogIdentity(
    const std::filesystem::path& socket_path,
    const l2flow_shm_session_info_v2& expected_session,
    std::uint64_t expected_generation) {
    bool ok = true;
    const auto expect_invalid =
        [&](auto mutate, std::string_view label) {
            l2flow_shm_session_info_v2 candidate =
                expected_session;
            mutate(candidate);
            ipc::InstrumentRawEventHistorySessionV2 session;
            ok &= Expect(
                ipc::InstrumentRawEventHistorySessionV2::Open(
                    socket_path.c_str(),
                    candidate,
                    expected_generation,
                    3'000U,
                    &session) ==
                        ipc::InstrumentRawEventHistoryErrorV2::
                            kInvalidArgument &&
                    !session.is_open(),
                label);
        };
    expect_invalid(
        [](l2flow_shm_session_info_v2& value) {
            value.catalog_scope = 1U;
        },
        "history rejects legacy catalog scope before connect");
    expect_invalid(
        [](l2flow_shm_session_info_v2& value) {
            value.coverage_complete = 0U;
        },
        "history rejects incomplete daily catalog before connect");
    expect_invalid(
        [](l2flow_shm_session_info_v2& value) {
            value.catalog_generation = 2U;
        },
        "history rejects mutable catalog generation before connect");
    expect_invalid(
        [](l2flow_shm_session_info_v2& value) {
            --value.bound_count;
        },
        "history rejects partial daily catalog before connect");
    expect_invalid(
        [](l2flow_shm_session_info_v2& value) {
            ++value.catalog_trade_date;
        },
        "history rejects mismatched catalog trade date before connect");
    expect_invalid(
        [](l2flow_shm_session_info_v2& value) {
            value.catalog_version = 0U;
        },
        "history rejects zero catalog version before connect");
    expect_invalid(
        [](l2flow_shm_session_info_v2& value) {
            std::fill(
                std::begin(value.catalog_digest),
                std::end(value.catalog_digest),
                0U);
        },
        "history rejects zero catalog digest before connect");

    l2flow_shm_session_info_v2 different_catalog =
        expected_session;
    different_catalog.catalog_digest[0U] ^= 0xffU;
    ipc::InstrumentRawEventHistorySessionV2 session;
    ok &= Expect(
        ipc::InstrumentRawEventHistorySessionV2::Open(
            socket_path.c_str(),
            different_catalog,
            expected_generation,
            3'000U,
            &session) ==
                ipc::InstrumentRawEventHistoryErrorV2::
                    kProtocolError &&
            !session.is_open(),
        "history rejects same session tuple with another catalog digest");
    return ok;
}

bool DrainDerivedEventHistory(
    ipc::InstrumentDerivedEventHistorySessionV1* session,
    std::vector<ipc::InstrumentDerivedEventV1>* events,
    ipc::InstrumentDerivedEventCheckpointV1* checkpoint,
    std::string_view label) {
    if (session == nullptr || events == nullptr ||
        checkpoint == nullptr) {
        return false;
    }
    events->clear();
    bool ok = true;
    bool eof = false;
    while (!eof && ok) {
        ipc::InstrumentDerivedEventHistoryPageV1 page;
        const auto error = session->ReadPage(&page);
        ok &= Expect(
            error ==
                ipc::InstrumentDerivedEventHistoryErrorV1::kNone,
            std::string(label) + " reads one derived page");
        if (error !=
            ipc::InstrumentDerivedEventHistoryErrorV1::kNone) {
            std::cerr
                << label << " derived read error="
                << ipc::InstrumentDerivedEventHistoryErrorNameV1(
                       error)
                << " raw_error="
                << ipc::InstrumentRawEventHistoryErrorNameV2(
                       session->last_raw_error())
                << '\n';
            break;
        }
        eof = page.eof;
        events->insert(
            events->end(),
            std::make_move_iterator(page.events.begin()),
            std::make_move_iterator(page.events.end()));
    }
    ok &= Expect(
        eof &&
            session->VerifiedCheckpoint(checkpoint) ==
                ipc::InstrumentDerivedEventHistoryErrorV1::kNone,
        std::string(label) +
            " releases checkpoint only after explicit EOF");
    return ok;
}

std::size_t CountOpenFileDescriptors() {
    DIR* const directory = ::opendir("/proc/self/fd");
    if (directory == nullptr) {
        return std::numeric_limits<std::size_t>::max();
    }
    std::size_t count = 0U;
    while (dirent* const entry = ::readdir(directory)) {
        if (std::strcmp(entry->d_name, ".") != 0 &&
            std::strcmp(entry->d_name, "..") != 0) {
            ++count;
        }
    }
    static_cast<void>(::closedir(directory));
    return count;
}

bool TestMalformedHistoryResponseClosesReceivedDescriptor() {
    ScopedTempDirectory temporary;
    if (!Expect(
            temporary.valid(),
            "create malformed-history-response fixture")) {
        return false;
    }
    const std::filesystem::path socket_path =
        temporary.path() / "malformed-history.sock";
    UniqueFd listener(
        ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0));
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    const std::string path = socket_path.string();
    if (listener.get() < 0 ||
        path.size() >= sizeof(address.sun_path)) {
        return false;
    }
    std::memcpy(
        address.sun_path, path.c_str(), path.size() + 1U);
    if (::bind(
            listener.get(),
            reinterpret_cast<const sockaddr*>(&address),
            static_cast<socklen_t>(
                offsetof(sockaddr_un, sun_path) +
                path.size() + 1U)) != 0 ||
        ::listen(listener.get(), 1) != 0) {
        return false;
    }
    const std::size_t before = CountOpenFileDescriptors();
    std::atomic<bool> server_ok{false};
    std::thread server([&] {
        UniqueFd client(::accept4(
            listener.get(), nullptr, nullptr, SOCK_CLOEXEC));
        std::array<std::byte, 64U> request{};
        if (client.get() < 0 ||
            ::recv(
                client.get(),
                request.data(),
                request.size(),
                0) <= 0) {
            return;
        }
        UniqueFd transferred(::open("/dev/null", O_RDONLY | O_CLOEXEC));
        if (transferred.get() < 0) {
            return;
        }
        std::array<std::byte, 8U> short_response{};
        iovec vector{};
        vector.iov_base = short_response.data();
        vector.iov_len = short_response.size();
        std::array<std::byte, CMSG_SPACE(sizeof(int))> control{};
        msghdr message{};
        message.msg_iov = &vector;
        message.msg_iovlen = 1U;
        message.msg_control = control.data();
        message.msg_controllen = control.size();
        cmsghdr* const header = CMSG_FIRSTHDR(&message);
        if (header == nullptr) {
            return;
        }
        header->cmsg_level = SOL_SOCKET;
        header->cmsg_type = SCM_RIGHTS;
        header->cmsg_len = CMSG_LEN(sizeof(int));
        const int descriptor = transferred.get();
        std::memcpy(
            CMSG_DATA(header), &descriptor, sizeof(descriptor));
        server_ok.store(
            ::sendmsg(client.get(), &message, MSG_NOSIGNAL) ==
                static_cast<ssize_t>(short_response.size()),
            std::memory_order_release);
    });

    l2flow_shm_session_info_v2 expected{};
    expected.run_id[0U] = 1U;
    expected.catalog_digest[0U] = 1U;
    expected.session_epoch = 1U;
    expected.trade_date = kTradeDate;
    expected.capacity = 1U;
    expected.catalog_scope =
        static_cast<std::uint32_t>(
            L2FLOW_CATALOG_DECLARED_DAILY_A_SHARE_V2);
    expected.coverage_complete = 1U;
    expected.catalog_generation = 1U;
    expected.bound_count = expected.capacity;
    expected.catalog_trade_date = expected.trade_date;
    expected.catalog_version = 1U;
    l2flow_instrument_raw_event_history_session_v2* session =
        nullptr;
    const int error =
        l2flow_instrument_raw_event_history_session_open_v2(
            socket_path.c_str(),
            &expected,
            0U,
            3'000U,
            &session);
    server.join();
    listener.Reset();
    static_cast<void>(::unlink(socket_path.c_str()));
    const std::size_t after = CountOpenFileDescriptors();
    l2flow_instrument_raw_event_history_session_close_v2(session);
    return Expect(
        server_ok.load(std::memory_order_acquire) &&
            error ==
                L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_PROTOCOL_ERROR_V2 &&
            session == nullptr &&
            before != std::numeric_limits<std::size_t>::max() &&
            before > 0U &&
            after == before - 1U,
        "short history response is protocol-fatal and closes every received SCM_RIGHTS fd");
}

bool TestLivePartialSemantics() {
    ScopedTempDirectory temporary;
    constexpr std::uint64_t partial_epoch = kSessionEpoch + 100U;
    DailyRuntimeFixture fixture =
        MakeManualDailyFixture(1U, partial_epoch);
    if (!Expect(temporary.valid(), "create partial temp directory") ||
        !Expect(static_cast<bool>(fixture), "create partial daily catalog")) {
        return false;
    }

    const std::filesystem::path socket_path =
        temporary.path() / "live-partial.sock";
    ipc::RealtimeSharedServiceConfigV2 config{};
    config.run_id = RunId(0x61U);
    config.session_epoch = partial_epoch;
    config.trade_date = kTradeDate;
    config.daily_catalog = fixture.catalog;
    config.coverage_from_open = false;
    config.startup_prefix_recovered = false;
    config.full_day_kline_valid = false;
    config.full_day_factor_valid = false;
    config.certified_prefix_valid = false;
    config.tick_ring_capacity = 4U;
    config.key_arena_bytes = 128U;
    config.maximum_mapping_bytes = 16U * 1024U * 1024U;
    config.control_socket_path = socket_path;

    std::shared_ptr<ipc::RealtimeSharedMarketServiceV2> service;
    int system_error = 0;
    bool ok = Expect(
        ipc::RealtimeSharedMarketServiceV2::Create(
            config, &service, &system_error) ==
                ipc::RealtimeSharedServiceCreateErrorV2::kNone &&
            service != nullptr && system_error == 0,
        "create LIVE_PARTIAL service");
    if (service == nullptr) {
        return false;
    }

    ok &= Expect(
        !service->Start(&system_error) && system_error == EINVAL,
        "ACTIVE start rejects a service without from-open coverage");

    market::IntradayInstrumentStoreConfigV1 store_config{};
    store_config.segment_target_bytes =
        market::kIntradayInstrumentStoreMinimumSegmentBytesV1;
    store_config.maximum_session_records = 4U;
    store_config.maximum_session_accounted_bytes = 1U << 20U;
    store_config.maximum_records_per_batch = 4U;
    store_config.coverage_from_open = false;
    std::unique_ptr<market::IntradayInstrumentStoreV1> startup_store;
    ok &= Expect(
        market::IntradayInstrumentStoreV1::Create(
            store_config,
            1U,
            kSourceStreamIds,
            fixture.runtime_state.get(),
            &startup_store) ==
                market::IntradayInstrumentStoreCreateErrorV1::kNone &&
            startup_store != nullptr,
        "create direct Store record fixture while service is INITIALIZING");
    market::InstrumentRouteTokenV1 route{};
    std::optional<market::RealtimeHistoryEventInputV1> startup_input =
        SnapshotInput(1U, 1U);
    const market::RealtimeHistoryRecordV1* startup_record = nullptr;
    ok &= Expect(
        startup_store != nullptr && startup_input.has_value() &&
            startup_store->ResolveRouteToken(0U, 1U, &route) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            startup_store->Append(
                0U,
                route,
                std::move(*startup_input),
                &startup_record) ==
                market::IntradayInstrumentStoreAppendErrorV1::kNone &&
            startup_record != nullptr,
        "materialize an exact startup snapshot record");
    if (startup_record == nullptr) {
        service->MarkFailed();
        service->StopControl();
        return false;
    }
    ok &= Expect(
        service->PublishApplied(0U, *startup_record) &&
            service->PublishProcessingProgress({1U, 1U}),
        "INITIALIZING accepts exact latest and processing-progress publication");
    if (service->failed()) {
        service->StopControl();
        return false;
    }

    system_error = 0;
    ok &= Expect(
        service->StartLivePartial(&system_error) && system_error == 0,
        "LIVE_PARTIAL exposes the initialized startup prefix");

    SessionTransfer transfer = RequestSession(socket_path);
    ok &= Expect(
        transfer.response.status == static_cast<std::uint16_t>(
            ipc::RealtimeControlStatusV2::kOk) &&
            transfer.fd.get() >= 0,
        "LIVE_PARTIAL exposes GET_SESSION and its latest-value mapping");
    ReaderHandle reader;
    if (transfer.fd.get() >= 0) {
        ok &= Expect(
            l2flow_shm_reader_open_fd_v2(
                transfer.fd.get(), reader.output()) ==
                    L2FLOW_SHM_READER_OK_V2 &&
                reader.get() != nullptr,
            "native reader accepts the V2.3 LIVE_PARTIAL state");
    }
    transfer.fd.Reset();
    if (reader.get() == nullptr) {
        service->MarkFailed();
        service->StopControl();
        return false;
    }

    l2flow_shm_session_info_v2 session{};
    ok &= Expect(
        l2flow_shm_reader_session_v2(reader.get(), &session) ==
                L2FLOW_SHM_READER_OK_V2 &&
            session.server_state == static_cast<std::uint32_t>(
                ipc::RealtimeServerStateV2::kLivePartial) &&
            session.flags == 0U &&
            session.coverage_complete == 1U &&
            session.bound_count == session.capacity &&
            session.available_count == 1U &&
            session.snapshot_available_count == 1U &&
            session.tick_available_count == 0U &&
            session.factor_eligible_count == 1U &&
            session.accepted_sequence == 1U &&
            session.applied_sequence == 1U &&
            session.processing_lag_records == 0U,
        "preview exposes the exact pre-start latest/progress prefix without a strong coverage claim");

    constexpr std::uint32_t instrument_id = 1U;
    ipc::RealtimeWireSnapshotPayloadV2 snapshot{};
    std::uint8_t latest_status = 0xffU;
    ok &= Expect(
        l2flow_shm_reader_latest_snapshots_v2(
            reader.get(),
            &instrument_id,
            1U,
            &snapshot,
            sizeof(snapshot),
            &latest_status) == L2FLOW_SHM_READER_OK_V2 &&
            latest_status == L2FLOW_LATEST_AVAILABLE_V2 &&
            snapshot.common.instrument_id == instrument_id &&
            snapshot.common.ordinal == 0U &&
            snapshot.common.source_stream_id == kSourceStreamIds[0U] &&
            snapshot.common.source_sequence == 1U &&
            snapshot.common.ingress_sequence == 1U &&
            snapshot.last_price.valid == 1U &&
            snapshot.last_price.is_null == 0U &&
            snapshot.last_price.raw == 1'234'000 &&
            snapshot.last_price.normalized_p6 == 1'234'000 &&
            snapshot.trade_volume.valid == 1U &&
            snapshot.trade_volume.raw == 101,
        "latest API preserves the exact record published before LIVE_PARTIAL exposure");
    ok &= Expect(
        !service->MarkCertifiedPrefixValid(),
        "LIVE_PARTIAL cannot manufacture a certified from-open prefix");

    const auto history_unavailable = [&socket_path] {
        UniqueFd channel = ConnectControlSocket(socket_path);
        ipc::RealtimeHistoryOpenRequestV2 request{};
        request.magic = ipc::kRealtimeControlMagicV2;
        request.protocol_major = ipc::kRealtimeWireMajorV2;
        request.protocol_minor = ipc::kRealtimeWireMinorV2;
        request.opcode = static_cast<std::uint16_t>(
            ipc::RealtimeHistoryControlOpcodeV2::kOpenHistory);
        request.message_bytes = sizeof(request);
        request.request_id = 0x5041525448495354ULL;
        request.instrument_id = 1U;
        request.requested_page_records = 1U;
        ipc::RealtimeHistoryOpenResponseV2 response{};
        return channel.get() >= 0 && SendObject(channel.get(), request) &&
               ReceiveObjectWithoutDescriptor(channel.get(), &response) &&
               response.status == static_cast<std::uint16_t>(
                   ipc::RealtimeHistoryControlStatusV2::kUnavailable) &&
               response.initial_read_token == 0U;
    };
    const auto delta_unavailable = [&socket_path] {
        UniqueFd channel = ConnectControlSocket(socket_path);
        ipc::RealtimeInstrumentTickDeltaOpenSessionRequestV2 request{};
        request.magic = ipc::kRealtimeControlMagicV2;
        request.protocol_major = ipc::kRealtimeWireMajorV2;
        request.protocol_minor = ipc::kRealtimeWireMinorV2;
        request.opcode = static_cast<std::uint16_t>(
            ipc::RealtimeInstrumentTickDeltaControlOpcodeV2::
                kOpenDeltaSession);
        request.message_bytes = sizeof(request);
        request.request_id = 0x5041525444454c54ULL;
        ipc::RealtimeInstrumentTickDeltaOpenSessionResponseV2 response{};
        return channel.get() >= 0 && SendObject(channel.get(), request) &&
               ReceiveObjectWithoutDescriptor(channel.get(), &response) &&
               response.status == static_cast<std::uint16_t>(
                   ipc::RealtimeInstrumentTickDeltaControlStatusV2::
                       kUnavailable) &&
               response.delta_session_token == 0U;
    };
    ok &= Expect(
        history_unavailable(),
        "History OPEN is unavailable before from-open promotion");
    ok &= Expect(
        delta_unavailable(),
        "tick-delta OPEN is unavailable before from-open promotion");

    service->MarkDraining();
    ok &= Expect(
        service->MarkStoppedClean(0U),
        "zero-tick LIVE_PARTIAL session can stop cleanly without claiming coverage loss");
    session = {};
    ok &= Expect(
        l2flow_shm_reader_session_v2(reader.get(), &session) ==
                L2FLOW_SHM_READER_OK_V2 &&
            session.server_state == static_cast<std::uint32_t>(
                ipc::RealtimeServerStateV2::kStoppedClean) &&
            session.flags == 0U,
        "partial origin remains explicit after DRAINING and STOPPED_CLEAN");
    ok &= Expect(
        history_unavailable() && delta_unavailable(),
        "stopped partial session still refuses complete History and delta semantics");
    service->StopControl();
    return ok && Expect(!service->failed(), "LIVE_PARTIAL lifecycle is nonfatal");
}

bool TestStandaloneLivePartialProcessStartHistory() {
    ScopedTempDirectory temporary;
    constexpr std::uint64_t partial_epoch = kSessionEpoch + 101U;
    DailyRuntimeFixture fixture =
        MakePipelineDailyFixture(partial_epoch);
    if (!Expect(
            temporary.valid() && static_cast<bool>(fixture),
            "create standalone partial History fixture")) {
        return false;
    }

    const common::Identity128 run_id = RunId(0x62U);
    ipc::RealtimeSharedServiceConfigV2 base_config{};
    base_config.run_id = run_id;
    base_config.session_epoch = partial_epoch;
    base_config.trade_date = kTradeDate;
    base_config.daily_catalog = fixture.catalog;
    base_config.coverage_from_open = false;
    base_config.tick_ring_capacity = 16U;
    base_config.key_arena_bytes = 128U;
    base_config.maximum_mapping_bytes = 16U * 1024U * 1024U;

    ipc::RealtimeSharedServiceConfigV2 preview_config = base_config;
    const std::filesystem::path preview_socket =
        temporary.path() / "preview-partial.sock";
    preview_config.control_socket_path = preview_socket;
    std::shared_ptr<ipc::RealtimeSharedMarketServiceV2> preview_service;
    int system_error = 0;
    bool ok = Expect(
        ipc::RealtimeSharedMarketServiceV2::Create(
            preview_config, &preview_service, &system_error) ==
                ipc::RealtimeSharedServiceCreateErrorV2::kNone &&
            preview_service != nullptr && system_error == 0 &&
            preview_service->StartLivePartial(&system_error),
        "start default latest-only LIVE_PARTIAL preview");
    if (preview_service == nullptr) {
        return false;
    }

    ipc::RealtimeSharedServiceConfigV2 standalone_config =
        base_config;
    const std::filesystem::path standalone_socket =
        temporary.path() / "standalone-partial.sock";
    standalone_config.control_socket_path = standalone_socket;
    std::shared_ptr<ipc::RealtimeSharedMarketServiceV2>
        standalone_service;
    system_error = 0;
    ok &= Expect(
        ipc::RealtimeSharedMarketServiceV2::Create(
            standalone_config,
            &standalone_service,
            &system_error) ==
                ipc::RealtimeSharedServiceCreateErrorV2::kNone &&
            standalone_service != nullptr && system_error == 0 &&
            standalone_service
                ->StartLivePartialWithProcessStartHistory(
                    &system_error),
        "start standalone LIVE_PARTIAL with process-start History");
    if (standalone_service == nullptr || !ok) {
        preview_service->MarkFailed();
        preview_service->StopControl();
        if (standalone_service != nullptr) {
            standalone_service->MarkFailed();
            standalone_service->StopControl();
        }
        return false;
    }

    ipc::RealtimeHistoryOpenResponseV2 history_response{};
    ipc::RealtimeInstrumentTickDeltaOpenSessionResponseV2
        delta_response{};
    ok &= Expect(
        RequestHistoryOpen(
            standalone_socket, 0U, &history_response) &&
            history_response.status ==
                static_cast<std::uint16_t>(
                    ipc::RealtimeHistoryControlStatusV2::
                        kUnavailable) &&
            RequestDeltaOpen(
                standalone_socket, 0U, &delta_response) &&
            delta_response.status ==
                static_cast<std::uint16_t>(
                    ipc::RealtimeInstrumentTickDeltaControlStatusV2::
                        kUnavailable),
        "opt-in partial queries remain unavailable before generation one");

    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    PipelineCleanup pipeline_cleanup(&pipeline);
    runtime::RealtimePipelineConfigV1 pipeline_config{};
    pipeline_config.run_id = run_id;
    pipeline_config.trade_date = kTradeDate;
    pipeline_config.daily_catalog = fixture.catalog;
    pipeline_config.runtime_state = fixture.runtime_state.get();
    pipeline_config.source_stream_ids = kSourceStreamIds;
    pipeline_config.maximum_sdk_message_bytes = 4'096U;
    pipeline_config.decoder_queue_capacity_per_source = 8U;
    pipeline_config.completion_tracker_capacity = 16U;
    pipeline_config.tick_ring_capacity = 16U;
    pipeline_config.store_worker_count = 1U;
    pipeline_config.store_queue_capacity_per_source_worker = 16U;
    pipeline_config.intraday_store.segment_target_bytes =
        market::kIntradayInstrumentStoreMinimumSegmentBytesV1;
    pipeline_config.intraday_store.maximum_session_records = 16U;
    pipeline_config.intraday_store.maximum_session_accounted_bytes =
        1U << 20U;
    pipeline_config.intraday_store.maximum_records_per_batch = 16U;
    pipeline_config.intraday_store.coverage_from_open = false;
    pipeline_config.applied_record_sink = standalone_service;
    pipeline_config.processing_progress_sink = standalone_service;
    pipeline_config.store_generation_sink = standalone_service;
    pipeline_config.sdk.enabled = false;

    std::string detail;
    const runtime::RealtimePipelineCreateErrorV1 pipeline_error =
        runtime::RealtimePipelineV1::Create(
            pipeline_config, &pipeline, &detail);
    ok &= Expect(
        pipeline_error ==
                runtime::RealtimePipelineCreateErrorV1::kNone &&
            pipeline != nullptr,
        "create process-start partial Pipeline: " + detail);
    if (pipeline == nullptr) {
        preview_service->MarkFailed();
        standalone_service->MarkFailed();
        preview_service->StopControl();
        standalone_service->StopControl();
        return false;
    }

    FakeSdkMessage snapshot(
        sdk::kProductionMessageKeysV1[2U],
        PipelineShenzhenSnapshotBody());
    FakeSdkMessage order(
        sdk::kProductionMessageKeysV1[3U],
        PipelineShenzhenOrderBody());
    const runtime::RealtimePipelineIngressResultV1 snapshot_ingress =
        pipeline->InjectSdkMessageForTest(&snapshot);
    const runtime::RealtimePipelineIngressResultV1 order_ingress =
        pipeline->InjectSdkMessageForTest(&order);
    ok &= Expect(
        snapshot_ingress.accepted() && order_ingress.accepted() &&
            snapshot_ingress.global_ingress_sequence == 1U &&
            order_ingress.global_ingress_sequence == 2U,
        "admit one snapshot and one tick into partial generation one");

    const runtime::RealtimePipelineCutResultV1 first_cut =
        pipeline->CutAndPublishGeneration(std::chrono::seconds(3));
    ok &= Expect(
        first_cut.published() &&
            first_cut.store_generation != nullptr &&
            first_cut.store_generation->watermark().generation == 1U,
        "periodic partial cut publishes generation one");
    bool preview_prefix_published = false;
    if (first_cut.store_generation != nullptr) {
        market::IntradayInstrumentScanOptionsV1 options{};
        options.ingress_sequence_begin_inclusive = 1U;
        options.ingress_sequence_end_exclusive = 3U;
        options.maximum_records = 2U;
        options.direction =
            market::IntradayInstrumentScanDirectionV1::kOldestFirst;
        std::unique_ptr<market::IntradayInstrumentCursorV1> cursor;
        std::array<const market::RealtimeHistoryRecordV1*, 2U>
            records{};
        std::size_t written = 0U;
        preview_prefix_published =
            first_cut.store_generation->OpenInstrumentCursor(
                1U, options, &cursor) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            cursor != nullptr &&
            cursor->ReadBatch(records, &written) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            written == records.size();
        for (std::size_t index = 0U;
             preview_prefix_published && index < written;
             ++index) {
            preview_prefix_published =
                records[index] != nullptr &&
                preview_service->PublishApplied(
                    0U, *records[index]);
        }
        preview_prefix_published =
            preview_prefix_published &&
            preview_service->PublishProcessingProgress({2U, 2U}) &&
            preview_service->PublishStoreGeneration(
                first_cut.store_generation);
    }
    ok &= Expect(
        preview_prefix_published,
        "publish a coherent partial prefix and generation into the default preview");

    history_response = {};
    delta_response = {};
    ok &= Expect(
        RequestHistoryOpen(preview_socket, 1U, &history_response) &&
            history_response.status ==
                static_cast<std::uint16_t>(
                    ipc::RealtimeHistoryControlStatusV2::
                        kUnavailable) &&
            RequestDeltaOpen(preview_socket, 1U, &delta_response) &&
            delta_response.status ==
                static_cast<std::uint16_t>(
                    ipc::RealtimeInstrumentTickDeltaControlStatusV2::
                        kUnavailable),
        "online-style preview rejects History/delta even with a valid partial generation");

    history_response = {};
    delta_response = {};
    const std::array<std::uint64_t, 4U> first_counts{{
        0U, 0U, 1U, 1U}};
    ok &= Expect(
        RequestHistoryOpen(
            standalone_socket, 1U, &history_response) &&
            history_response.status ==
                static_cast<std::uint16_t>(
                    ipc::RealtimeHistoryControlStatusV2::kOk) &&
            history_response.initial_read_token != 0U &&
            ipc::RealtimeHistoryGenerationInfoCanonicalV2(
                history_response.generation) &&
            history_response.generation.endpoint.flags ==
                ipc::kRealtimeGenerationRecordCoverageCompleteV2 &&
            history_response.generation
                    .instrument_source_record_counts ==
                first_counts &&
            history_response.generation.instrument_record_count == 2U &&
            history_response.generation.snapshot_record_count == 1U &&
            history_response.generation.tick_record_count == 1U &&
            RequestDeltaOpen(
                standalone_socket, 1U, &delta_response) &&
            delta_response.status ==
                static_cast<std::uint16_t>(
                    ipc::RealtimeInstrumentTickDeltaControlStatusV2::
                        kOk) &&
            delta_response.delta_session_token != 0U &&
            delta_response.target_generation.flags ==
                ipc::kRealtimeGenerationRecordCoverageCompleteV2,
        "standalone partial exposes complete process-start History and delta without from-open coverage");

    SessionTransfer transfer = RequestSession(standalone_socket);
    ReaderHandle reader;
    ok &= Expect(
        transfer.fd.get() >= 0 &&
            l2flow_shm_reader_open_fd_v2(
                transfer.fd.get(), reader.output()) ==
                L2FLOW_SHM_READER_OK_V2 &&
            reader.get() != nullptr,
        "open standalone partial reader identity");
    transfer.fd.Reset();
    l2flow_shm_session_info_v2 session{};
    ok &= Expect(
        reader.get() != nullptr &&
            l2flow_shm_reader_session_v2(reader.get(), &session) ==
                L2FLOW_SHM_READER_OK_V2 &&
            session.server_state ==
                static_cast<std::uint32_t>(
                    ipc::RealtimeServerStateV2::kLivePartial) &&
            session.flags == 0U && session.window_count == 0U &&
            !standalone_service->MarkCertifiedPrefixValid(),
        "standalone partial retains LIVE_PARTIAL and no strong/KLine/CERTIFIED flags");

    ok &= RunPythonHistoryDeltaSmoke(
        standalone_socket, 1U, 2U, true);
    ipc::InstrumentRawEventHistoryCheckpointV2 first_checkpoint{};
    const std::array<std::uint64_t, 1U> first_ingress{{2U}};
    const std::array<std::uint64_t, 1U> first_ticks{{1U}};
    ok &= ReadNativeRawEventHistory(
        standalone_socket,
        session,
        1U,
        nullptr,
        first_ingress,
        first_ticks,
        &first_checkpoint,
        "partial native origin",
        3U,
        4U,
        2U);

    preview_service->MarkDraining();
    ok &= Expect(
        preview_service->MarkStoppedClean(1U),
        "default partial preview stops cleanly after hidden generation publication");
    history_response = {};
    delta_response = {};
    ok &= Expect(
        RequestHistoryOpen(preview_socket, 1U, &history_response) &&
            history_response.status ==
                static_cast<std::uint16_t>(
                    ipc::RealtimeHistoryControlStatusV2::
                        kUnavailable) &&
            RequestDeltaOpen(preview_socket, 1U, &delta_response) &&
            delta_response.status ==
                static_cast<std::uint16_t>(
                    ipc::RealtimeInstrumentTickDeltaControlStatusV2::
                        kUnavailable),
        "default preview remains isolated after DRAINING/STOPPED_CLEAN");
    preview_service->StopControl();

    FakeSdkMessage transaction(
        sdk::kProductionMessageKeysV1[4U],
        PipelineShenzhenTransactionBody());
    const runtime::RealtimePipelineIngressResultV1 transaction_ingress =
        pipeline->InjectSdkMessageForTest(&transaction);
    ok &= Expect(
        transaction_ingress.accepted() &&
            transaction_ingress.global_ingress_sequence == 3U,
        "admit one tick into the next partial generation");
    const runtime::RealtimePipelineCutResultV1 second_cut =
        pipeline->CutAndPublishGeneration(std::chrono::seconds(3));
    ok &= Expect(
        second_cut.published() &&
            second_cut.store_generation != nullptr &&
            second_cut.store_generation->watermark().generation == 2U,
        "periodic partial cut publishes generation two");

    history_response = {};
    const std::array<std::uint64_t, 4U> second_counts{{
        0U, 0U, 1U, 2U}};
    ok &= Expect(
        RequestHistoryOpen(
            standalone_socket, 2U, &history_response) &&
            history_response.status ==
                static_cast<std::uint16_t>(
                    ipc::RealtimeHistoryControlStatusV2::kOk) &&
            history_response.generation.endpoint.flags ==
                ipc::kRealtimeGenerationRecordCoverageCompleteV2 &&
            history_response.generation
                    .instrument_source_record_counts ==
                second_counts &&
            history_response.generation.instrument_record_count == 3U &&
            history_response.generation.snapshot_record_count == 1U &&
            history_response.generation.tick_record_count == 2U,
        "generation two History retains the complete process-start prefix");
    ok &= RunPythonHistoryDeltaSmoke(
        standalone_socket, 2U, 3U, false);

    ipc::InstrumentRawEventHistoryCheckpointV2 second_checkpoint{};
    const std::array<std::uint64_t, 1U> second_ingress{{3U}};
    const std::array<std::uint64_t, 1U> second_ticks{{2U}};
    ok &= ReadNativeRawEventHistory(
        standalone_socket,
        session,
        2U,
        &first_checkpoint,
        second_ingress,
        second_ticks,
        &second_checkpoint,
        "partial native generation delta",
        3U,
        5U,
        2U);
    ok &= Expect(
        second_checkpoint.generation.flags ==
            ipc::kRealtimeGenerationRecordCoverageCompleteV2 &&
            second_checkpoint.instrument_event_record_count == 2U,
        "partial generation checkpoint remains complete but explicitly not from-open");

    history_response = {};
    delta_response = {};
    ok &= Expect(
        RequestHistoryOpen(
            standalone_socket, 1U, &history_response) &&
            history_response.status ==
                static_cast<std::uint16_t>(
                    ipc::RealtimeHistoryControlStatusV2::
                        kGenerationChanged) &&
            RequestDeltaOpen(
                standalone_socket, 1U, &delta_response) &&
            delta_response.status ==
                static_cast<std::uint16_t>(
                    ipc::RealtimeInstrumentTickDeltaControlStatusV2::
                        kCheckpointMismatch),
        "new partial opens pin only the current immutable generation");

    standalone_service->MarkDraining();
    history_response = {};
    delta_response = {};
    ok &= Expect(
        RequestHistoryOpen(
            standalone_socket, 2U, &history_response) &&
            history_response.status ==
                static_cast<std::uint16_t>(
                    ipc::RealtimeHistoryControlStatusV2::kOk) &&
            RequestDeltaOpen(
                standalone_socket, 2U, &delta_response) &&
            delta_response.status ==
                static_cast<std::uint16_t>(
                    ipc::RealtimeInstrumentTickDeltaControlStatusV2::
                        kOk),
        "process-start generation queries remain available while draining");
    pipeline->StopAndDrain();
    ok &= Expect(
        standalone_service->MarkStoppedClean(2U),
        "standalone partial stops with its exact contiguous tick prefix");
    history_response = {};
    delta_response = {};
    ok &= Expect(
        RequestHistoryOpen(
            standalone_socket, 2U, &history_response) &&
            history_response.status ==
                static_cast<std::uint16_t>(
                    ipc::RealtimeHistoryControlStatusV2::kOk) &&
            RequestDeltaOpen(
                standalone_socket, 2U, &delta_response) &&
            delta_response.status ==
                static_cast<std::uint16_t>(
                    ipc::RealtimeInstrumentTickDeltaControlStatusV2::
                        kOk),
        "stopped standalone partial retains its last complete process-start generation");
    standalone_service->StopControl();
    return ok && !pipeline->fatal() &&
           !preview_service->failed() &&
           !standalone_service->failed();
}

bool TestServiceEndToEnd() {
    ScopedTempDirectory temporary;
    DailyRuntimeFixture fixture =
        MakeManualDailyFixture(4U, kSessionEpoch);
    if (!Expect(temporary.valid(), "create secure temp directory") ||
        !Expect(static_cast<bool>(fixture), "create daily catalog")) {
        return false;
    }
    const std::filesystem::path socket_path =
        temporary.path() / "market.sock";
    const common::Identity128 run_id = RunId(0x29U);

    ipc::RealtimeSharedServiceConfigV2 service_config{};
    service_config.run_id = run_id;
    service_config.session_epoch = kSessionEpoch;
    service_config.trade_date = kTradeDate;
    service_config.daily_catalog = fixture.catalog;
    service_config.coverage_from_open = true;
    service_config.kline_windows = {{
        kWindowId,
        kWindowDurationNs,
    }};
    service_config.tick_ring_capacity = 4U;
    service_config.key_arena_bytes = 128U;
    service_config.maximum_mapping_bytes = 16U * 1024U * 1024U;
    service_config.control_socket_path = socket_path;
    std::shared_ptr<ipc::RealtimeSharedMarketServiceV2> service;
    int system_error = 0;
    const ipc::RealtimeSharedServiceCreateErrorV2 create_error =
        ipc::RealtimeSharedMarketServiceV2::Create(
            service_config, &service, &system_error);
    bool ok = Expect(
        create_error ==
                ipc::RealtimeSharedServiceCreateErrorV2::kNone &&
            service != nullptr && system_error == 0,
        "create Wire V2 service");
    if (!ok) {
        std::cerr
            << "create error="
            << ipc::RealtimeSharedServiceCreateErrorNameV2(
                   create_error)
            << " errno=" << system_error << '\n';
        return false;
    }
    ok &= Expect(
        service->Start(&system_error) && system_error == 0,
        "start service with prepublished daily catalog");

    SessionTransfer invalid =
        RequestSession(socket_path, 2U);
    ok &= Expect(
        invalid.response.status ==
                static_cast<std::uint16_t>(
                    ipc::RealtimeControlStatusV2::kInvalidRequest) &&
            invalid.fd.get() < 0,
        "malformed history OPEN using the GET_SESSION shape is rejected");

    SessionTransfer transfer = RequestSession(socket_path);
    ok &= Expect(
        transfer.fd.get() >= 0 &&
            transfer.response.magic ==
                ipc::kRealtimeControlMagicV2 &&
            transfer.response.status ==
                static_cast<std::uint16_t>(
                    ipc::RealtimeControlStatusV2::kOk) &&
            transfer.response.session_epoch == kSessionEpoch &&
            transfer.response.total_mapping_bytes ==
                service->mapping_bytes(),
        "GET_SESSION transfers the sealed read-only memfd");
    if (transfer.fd.get() < 0) {
        service->MarkFailed();
        service->StopControl();
        return false;
    }
    const int descriptor_flags =
        ::fcntl(transfer.fd.get(), F_GETFL);
    ok &= Expect(
        descriptor_flags >= 0 &&
            (descriptor_flags & O_ACCMODE) == O_RDONLY,
        "transferred memfd is read-only");

    ReaderHandle reader;
    ok &= Expect(
        l2flow_shm_reader_open_fd_v2(
            transfer.fd.get(), reader.output()) ==
                L2FLOW_SHM_READER_OK_V2 &&
            reader.get() != nullptr,
        "open Writer V2 mapping with Reader V2");
    if (reader.get() == nullptr) {
        service->MarkFailed();
        service->StopControl();
        return false;
    }
    transfer.fd.Reset();

    l2flow_shm_session_info_v2 session{};
    ok &= Expect(
        l2flow_shm_reader_session_v2(
            reader.get(), &session) ==
                L2FLOW_SHM_READER_OK_V2 &&
            session.server_state ==
                static_cast<std::uint32_t>(
                    ipc::RealtimeServerStateV2::kActive) &&
            session.capacity == 4U && session.bound_count == 4U &&
            session.available_count == 0U &&
            session.catalog_scope ==
                static_cast<std::uint32_t>(
                    ipc::RealtimeCatalogScopeV2::
                        kDeclaredDailyAShare) &&
            session.coverage_complete == 1U &&
            session.catalog_trade_date == kTradeDate &&
            session.catalog_version == kSessionEpoch,
        "complete daily catalog mapping is ACTIVE before ingress");

    const std::uint64_t used_after_binding =
        service->key_arena_used_bytes();
    ok &= Expect(
        used_after_binding != 0U,
        "startup prepublication consumes exact opaque key bytes once");

    market::RealtimeHistoryRuntimeConfigV1 runtime_config{};
    runtime_config.source_stream_ids = kSourceStreamIds;
    runtime_config.worker_count = 1U;
    runtime_config.queue_capacity_per_source_worker = 16U;
    runtime_config.runtime_state = fixture.runtime_state.get();
    runtime_config.intraday_store.segment_target_bytes =
        market::kIntradayInstrumentStoreMinimumSegmentBytesV1;
    runtime_config.intraday_store.maximum_session_records = 16U;
    runtime_config.intraday_store.maximum_session_accounted_bytes =
        1U << 20U;
    runtime_config.intraday_store.maximum_records_per_batch = 16U;
    runtime_config.intraday_store.coverage_from_open = true;
    runtime_config.kline.trade_date = kTradeDate;
    runtime_config.kline.windows = {{
        kWindowId,
        kWindowDurationNs,
    }};
    runtime_config.applied_record_sink = service;
    std::unique_ptr<market::RealtimeHistoryRuntimeV1> runtime;
    ok &= Expect(
        market::RealtimeHistoryRuntimeV1::Create(
            runtime_config, &runtime) ==
                market::RealtimeHistoryCreateErrorV1::kNone &&
            runtime != nullptr,
        "create real history runtime with Writer V2 sink");
    if (runtime == nullptr) {
        service->MarkFailed();
        service->StopControl();
        return false;
    }

    std::shared_ptr<
        const market::DailyInstrumentCatalogSnapshotV2>
        catalog;
    ok &= Expect(
        fixture.runtime_state->AcquireSnapshot(&catalog) ==
                market::InstrumentRuntimeStateErrorV2::kNone &&
            catalog != nullptr,
        "capture bound no-data catalog snapshot");
    const std::array<market::RealtimeSourceWatermarkV1, 4U>
        empty_sources{{
            {11U, 1U},
            {12U, 1U},
            {13U, 1U},
            {14U, 1U},
        }};
    market::RealtimeHistoryWatermarkV1 empty_watermark{};
    ok &= Expect(
        market::BuildRealtimeHistoryWatermarkV1(
            run_id,
            1U,
            kTradeDate,
            1U,
            40'000U,
            catalog,
            realtime::ProcessingProgressV2{},
            empty_sources,
            &empty_watermark) ==
            market::RealtimeHistoryWatermarkErrorV1::kNone,
        "build bound no-data generation watermark");
    ok &= Expect(
        runtime->BeginGeneration(empty_watermark) ==
            market::RealtimeHistoryGenerationErrorV1::kNone,
        "begin bound no-data generation");
    for (std::uint8_t source = 0U; source < 4U; ++source) {
        ok &= Expect(
            runtime->SealSource(source, 1U) ==
                market::RealtimeHistoryGenerationErrorV1::kNone,
            "seal bound no-data generation source");
    }
    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>
        empty_store_generation;
    std::shared_ptr<const market::RealtimeKLineGenerationV1>
        empty_kline_generation;
    ok &= Expect(
        runtime->WaitForGeneration(
            1U,
            std::chrono::seconds(3),
            &empty_store_generation,
            &empty_kline_generation) ==
                market::RealtimeHistoryGenerationErrorV1::kNone &&
            empty_store_generation != nullptr &&
            empty_kline_generation != nullptr,
        "build bound no-data immutable generation");
    const bool empty_kline_published =
        empty_kline_generation != nullptr &&
        service->PublishKLineGeneration(*empty_kline_generation);
    const bool empty_store_published =
        empty_kline_published &&
        service->PublishStoreGeneration(empty_store_generation);
    ok &= Expect(
        empty_kline_published && empty_store_published,
        std::string("publish bound no-data generation kline=") +
            (empty_kline_published ? "1" : "0") +
            " store=" + (empty_store_published ? "1" : "0"));
    ok &= RunPythonHistoryDeltaSmoke(
        socket_path, 1U, 0U, false);

    ok &= Expect(
        Submit(runtime.get(), SnapshotInput(1U, 1U)) &&
            Submit(runtime.get(), SnapshotInput(2U, 2U)) &&
            Submit(runtime.get(), TickInput(1U, 3U, 1U)),
        "submit snapshot, repeated snapshot, and tick");
    ok &= Expect(
        WaitUntil([&] {
            std::shared_ptr<
                const market::DailyInstrumentCatalogSnapshotV2>
                snapshot;
            return fixture.runtime_state->AcquireSnapshot(&snapshot) ==
                       market::InstrumentRuntimeStateErrorV2::
                           kNone &&
                   snapshot != nullptr &&
                   snapshot->available_count() == 1U &&
                   snapshot->snapshot_available_count() == 1U &&
                   snapshot->tick_available_count() == 1U &&
                   snapshot->factor_eligible_count() == 1U;
        }),
        "history applies availability to the directory");

    ok &= Expect(
        fixture.runtime_state->AcquireSnapshot(&catalog) ==
                market::InstrumentRuntimeStateErrorV2::kNone &&
            catalog != nullptr,
        "capture generation catalog snapshot");
    const std::array<market::RealtimeSourceWatermarkV1, 4U>
        sources{{
            {11U, 3U},
            {12U, 2U},
            {13U, 1U},
            {14U, 1U},
        }};
    market::RealtimeHistoryWatermarkV1 watermark{};
    ok &= Expect(
        market::BuildRealtimeHistoryWatermarkV1(
            run_id,
            2U,
            kTradeDate,
            4U,
            50'000U,
            catalog,
            realtime::ProcessingProgressV2{3U, 3U},
            sources,
            &watermark) ==
            market::RealtimeHistoryWatermarkErrorV1::kNone,
        "build frozen daily-catalog generation watermark");
    ok &= Expect(
        runtime->BeginGeneration(watermark) ==
            market::RealtimeHistoryGenerationErrorV1::kNone,
        "begin KLine generation");
    for (std::uint8_t source = 0U; source < 4U; ++source) {
        ok &= Expect(
            runtime->SealSource(source, 2U) ==
                market::RealtimeHistoryGenerationErrorV1::kNone,
            "seal generation source");
    }
    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>
        store_generation;
    std::shared_ptr<const market::RealtimeKLineGenerationV1>
        kline_generation;
    ok &= Expect(
        runtime->WaitForGeneration(
            2U,
            std::chrono::seconds(3),
            &store_generation,
            &kline_generation) ==
                market::RealtimeHistoryGenerationErrorV1::kNone &&
            store_generation != nullptr &&
            kline_generation != nullptr,
        "build immutable Store/KLine generation");
    ok &= Expect(
        kline_generation != nullptr &&
            service->PublishKLineGeneration(*kline_generation),
        "publish KLine table before generation pointer");
    ok &= Expect(
        service->PublishStoreGeneration(store_generation),
        "publish exact immutable Store generation for V2 history");
    ok &= Expect(
        CheckCanonicalHistoryReadErrorFrame(socket_path, 2U),
        "history READ error frame zeros every success-only field");
    ok &= Expect(
        CheckCanonicalDeltaReadErrorFrame(socket_path, 2U),
        "delta READ error frame zeros every success-only field");
    ok &= RunPythonHistoryDeltaSmoke(
        socket_path, 2U, 3U, true);

    ok &= Expect(
        l2flow_shm_reader_session_v2(
            reader.get(), &session) ==
            L2FLOW_SHM_READER_OK_V2,
        "refresh shared-memory identity before native history");
    ok &= CheckHistoryExpectedDailyCatalogIdentity(
        socket_path, session, 2U);
    ipc::InstrumentRawEventHistoryCheckpointV2 first_checkpoint{};
    const std::array<std::uint64_t, 1U> first_ingress{{3U}};
    const std::array<std::uint64_t, 1U> first_ticks{{1U}};
    ok &= ReadNativeRawEventHistory(
        socket_path,
        session,
        2U,
        nullptr,
        first_ingress,
        first_ticks,
        &first_checkpoint,
        "native origin");

    l2flow_instrument_derived_event_history_session_v1*
        derived_c_raw = nullptr;
    ok &= Expect(
        l2flow_instrument_derived_event_history_session_open_v1(
            socket_path.c_str(),
            &session,
            1U,
            L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1,
            100U,
            3'000U,
            &derived_c_raw) ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_OK_V1 &&
            derived_c_raw != nullptr,
        "derived C ABI opens one instrument-bound stateful session");
    const auto derived_c_deleter = [](
                                       l2flow_instrument_derived_event_history_session_v1*
                                           value) noexcept {
        l2flow_instrument_derived_event_history_session_close_v1(
            value);
    };
    std::unique_ptr<
        l2flow_instrument_derived_event_history_session_v1,
        decltype(derived_c_deleter)>
        derived_c_history(derived_c_raw, derived_c_deleter);
    l2flow_instrument_derived_event_checkpoint_v1
        first_derived_c_checkpoint{};
    if (derived_c_history != nullptr) {
        ok &= Expect(
            l2flow_instrument_derived_event_history_begin_full_v1(
                derived_c_history.get(), 2U, 1U) ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_OK_V1,
            "derived C ABI begins retained full replay");
        std::array<l2flow_instrument_derived_event_row_v1, 1U>
            undersized_rows{};
        std::size_t derived_c_count = 0U;
        std::uint32_t derived_c_eof = 0U;
        ok &= Expect(
            l2flow_instrument_derived_event_history_read_v1(
                derived_c_history.get(),
                undersized_rows.data(),
                undersized_rows.size(),
                &derived_c_count,
                &derived_c_eof) ==
                    L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_BUFFER_TOO_SMALL_V1 &&
                derived_c_count == 2U && derived_c_eof == 0U,
            "derived C ABI reports exact required capacity without "
            "advancing the page");
        std::array<l2flow_instrument_derived_event_row_v1, 2U>
            derived_c_rows{};
        ok &= Expect(
            l2flow_instrument_derived_event_history_read_v1(
                derived_c_history.get(),
                derived_c_rows.data(),
                derived_c_rows.size(),
                &derived_c_count,
                &derived_c_eof) ==
                    L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_OK_V1 &&
                derived_c_count == derived_c_rows.size() &&
                derived_c_eof == 0U,
            "derived C ABI retries the same full-replay page");
        bool c_trade_seen = false;
        bool c_order_seen = false;
        for (const auto& row : derived_c_rows) {
            c_trade_seen =
                c_trade_seen ||
                (row.record_schema_version == 1U &&
                 row.record_bytes == sizeof(row) &&
                 row.event_kind ==
                     L2FLOW_INSTRUMENT_DERIVED_EVENT_TRADE_V1 &&
                 row.quantity == 101);
            c_order_seen =
                c_order_seen ||
                (row.record_schema_version == 1U &&
                 row.record_bytes == sizeof(row) &&
                 row.event_kind ==
                     L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_REVISION_V1 &&
                 row.order_id == 11'001 &&
                 row.original_quantity_valid == 1U &&
                 row.original_quantity == 101 &&
                 row.apply_to_book == 0U);
        }
        ok &= Expect(
            c_trade_seen && c_order_seen,
            "derived C ABI flattens the full T source event and "
            "synthetic order revision");
        ok &= Expect(
            l2flow_instrument_derived_event_history_verified_checkpoint_v1(
                derived_c_history.get(),
                &first_derived_c_checkpoint) ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_INVALID_STATE_V1,
            "derived C ABI withholds checkpoint before explicit EOF");
        derived_c_count = std::numeric_limits<std::size_t>::max();
        derived_c_eof = 0U;
        ok &= Expect(
            l2flow_instrument_derived_event_history_read_v1(
                derived_c_history.get(),
                nullptr,
                0U,
                &derived_c_count,
                &derived_c_eof) ==
                    L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_OK_V1 &&
                derived_c_count == 0U && derived_c_eof == 1U &&
                l2flow_instrument_derived_event_history_verified_checkpoint_v1(
                    derived_c_history.get(),
                    &first_derived_c_checkpoint) ==
                    L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_OK_V1 &&
                first_derived_c_checkpoint
                        .derived_event_sequence_exclusive ==
                    3U &&
                first_derived_c_checkpoint.order_state_count == 1U,
            "derived C ABI releases its checkpoint only after EOF");
    }

    std::unique_ptr<
        ipc::InstrumentDerivedEventHistorySessionV1>
        derived_history;
    ipc::InstrumentDerivedEventHistoryConfigV1 derived_config{};
    derived_config.control_socket_path = socket_path.string();
    derived_config.expected_session = session;
    derived_config.instrument_id = 1U;
    derived_config.market = market::MarketV1::kShanghai;
    derived_config.maximum_order_states = 100U;
    derived_config.timeout_ms = 3'000U;
    ok &= Expect(
        ipc::InstrumentDerivedEventHistorySessionV1::Create(
            std::move(derived_config), &derived_history) ==
                ipc::InstrumentDerivedEventHistoryErrorV1::kNone &&
            derived_history != nullptr &&
            derived_history->BeginFull(2U, 1U) ==
                ipc::InstrumentDerivedEventHistoryErrorV1::kNone,
        "derived history opens full replay on the first generation");
    ipc::InstrumentDerivedEventCheckpointV1
        unverified_derived_checkpoint{};
    ok &= Expect(
        derived_history != nullptr &&
            derived_history->VerifiedCheckpoint(
                &unverified_derived_checkpoint) ==
                ipc::InstrumentDerivedEventHistoryErrorV1::
                    kInvalidState,
        "derived history withholds checkpoint before explicit EOF");
    std::vector<ipc::InstrumentDerivedEventV1>
        first_derived_events;
    ipc::InstrumentDerivedEventCheckpointV1
        first_derived_checkpoint{};
    if (derived_history != nullptr) {
        ok &= DrainDerivedEventHistory(
            derived_history.get(),
            &first_derived_events,
            &first_derived_checkpoint,
            "derived full T");
    }
    const market::ShanghaiTradeEventV1* derived_trade =
        nullptr;
    const market::ShanghaiOrderRevisionEventV1*
        first_order_revision = nullptr;
    for (const auto& event : first_derived_events) {
        if (const auto* trade =
                std::get_if<market::ShanghaiTradeEventV1>(
                    &event.payload)) {
            derived_trade = trade;
        }
        if (const auto* order =
                std::get_if<
                    market::ShanghaiOrderRevisionEventV1>(
                    &event.payload)) {
            first_order_revision = order;
        }
    }
    ok &= Expect(
        first_derived_events.size() == 2U &&
            derived_trade != nullptr &&
            derived_trade->buy_order_id == 11'001 &&
            derived_trade->quantity == 101 &&
            first_order_revision != nullptr &&
            first_order_revision->order.order_source ==
                market::ShanghaiOrderSourceV1::
                    kReconstructedFromTrades &&
            first_order_revision->order.original_quantity == 101 &&
            first_order_revision->order.original_quantity_status ==
                market::ShanghaiOriginalQuantityStatusV1::
                    kLowerBound &&
            first_order_revision->order.price_p6 == 1'235'000 &&
            first_order_revision->order.price_source ==
                market::ShanghaiOrderPriceSourceV1::
                    kBuyMaximumExecution &&
            !first_order_revision->order.apply_to_book,
        "full T emits trade plus provisional synthetic lower-bound order");

    ok &= Expect(
        Submit(runtime.get(), AddInput(2U, 4U, 2U)),
        "append matching A for native rolling update");
    ipc::RealtimeWireTickPayloadV2 rolling_latest{};
    std::uint8_t rolling_status = 0xffU;
    constexpr std::uint32_t rolling_instrument_id = 1U;
    ok &= Expect(
        WaitUntil([&] {
            rolling_status = 0xffU;
            return l2flow_shm_reader_latest_ticks_v2(
                       reader.get(),
                       &rolling_instrument_id,
                       1U,
                       &rolling_latest,
                       sizeof(rolling_latest),
                       &rolling_status) ==
                       L2FLOW_SHM_READER_OK_V2 &&
                   rolling_status ==
                       L2FLOW_LATEST_AVAILABLE_V2 &&
                   rolling_latest.common.tick_stream_sequence ==
                       2U;
        }),
        "rolling-update tick reaches Store and Wire latest");
    ok &= Expect(
        fixture.runtime_state->AcquireSnapshot(&catalog) ==
                market::InstrumentRuntimeStateErrorV2::kNone &&
            catalog != nullptr,
        "capture catalog for native rolling generation");
    const std::array<market::RealtimeSourceWatermarkV1, 4U>
        rolling_sources{{
            {11U, 3U},
            {12U, 3U},
            {13U, 1U},
            {14U, 1U},
        }};
    market::RealtimeHistoryWatermarkV1 rolling_watermark{};
    ok &= Expect(
        market::BuildRealtimeHistoryWatermarkV1(
            run_id,
            3U,
            kTradeDate,
            5U,
            60'000U,
            catalog,
            realtime::ProcessingProgressV2{4U, 4U},
            rolling_sources,
            &rolling_watermark) ==
            market::RealtimeHistoryWatermarkErrorV1::kNone,
        "build next immutable generation for native rolling update");
    ok &= Expect(
        runtime->BeginGeneration(rolling_watermark) ==
            market::RealtimeHistoryGenerationErrorV1::kNone,
        "begin native rolling generation");
    for (std::uint8_t source = 0U; source < 4U; ++source) {
        ok &= Expect(
            runtime->SealSource(source, 3U) ==
                market::RealtimeHistoryGenerationErrorV1::kNone,
            "seal native rolling generation source");
    }
    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>
        rolling_store_generation;
    std::shared_ptr<const market::RealtimeKLineGenerationV1>
        rolling_kline_generation;
    ok &= Expect(
        runtime->WaitForGeneration(
            3U,
            std::chrono::seconds(3),
            &rolling_store_generation,
            &rolling_kline_generation) ==
                market::RealtimeHistoryGenerationErrorV1::kNone &&
            rolling_store_generation != nullptr &&
            rolling_kline_generation != nullptr &&
            service->PublishStoreGeneration(
                rolling_store_generation),
        "publish next Store generation for native rolling update");
    ok &= Expect(
        l2flow_shm_reader_session_v2(
            reader.get(), &session) ==
            L2FLOW_SHM_READER_OK_V2,
        "refresh identity for native rolling target");
    ipc::InstrumentRawEventHistoryCheckpointV2
        second_checkpoint{};
    const std::array<std::uint64_t, 1U> second_ingress{{4U}};
    const std::array<std::uint64_t, 1U> second_ticks{{2U}};
    ok &= ReadNativeRawEventHistory(
        socket_path,
        session,
        3U,
        &first_checkpoint,
        second_ingress,
        second_ticks,
        &second_checkpoint,
        "native rolling suffix");
    std::vector<ipc::InstrumentDerivedEventV1>
        second_derived_events;
    ipc::InstrumentDerivedEventCheckpointV1
        second_derived_checkpoint{};
    if (derived_history != nullptr) {
        ok &= Expect(
            derived_history->BeginUpdate(
                first_derived_checkpoint, 3U, 1U) ==
                ipc::InstrumentDerivedEventHistoryErrorV1::kNone,
            "derived rolling update starts from exact full checkpoint");
        ok &= DrainDerivedEventHistory(
            derived_history.get(),
            &second_derived_events,
            &second_derived_checkpoint,
            "derived rolling A");
    }
    const market::ShanghaiOrderRevisionEventV1*
        second_order_revision = nullptr;
    for (const auto& event : second_derived_events) {
        if (const auto* order =
                std::get_if<
                    market::ShanghaiOrderRevisionEventV1>(
                    &event.payload)) {
            second_order_revision = order;
        }
    }
    ok &= Expect(
        second_derived_events.size() == 1U &&
            second_order_revision != nullptr &&
            second_order_revision->operation ==
                market::ShanghaiOrderDeltaOperationV1::kUpdate &&
            second_order_revision->order.revision == 2U &&
            second_order_revision->order.order_source ==
                market::ShanghaiOrderSourceV1::kSourceAdd &&
            second_order_revision->order.original_quantity == 151 &&
            second_order_revision->order.original_quantity_status ==
                market::ShanghaiOriginalQuantityStatusV1::kExact &&
            second_order_revision->order
                    .observed_pre_add_trade_quantity == 101 &&
            second_order_revision->order.source_matched_quantity == 101 &&
            second_order_revision->order.published_quantity == 50 &&
            second_order_revision->order.remaining_quantity == 50 &&
            second_order_revision->order.price_p6 == 1'236'000 &&
            second_order_revision->order.price_source ==
                market::ShanghaiOrderPriceSourceV1::kSourceAdd &&
            second_order_revision->order.apply_to_book &&
            second_derived_checkpoint
                    .derived_event_sequence_exclusive ==
                first_derived_checkpoint
                        .derived_event_sequence_exclusive +
                    1U,
        "rolling A revises T-only order to exact A-backed quantity/state");
    if (derived_c_history != nullptr) {
        ok &= Expect(
            l2flow_instrument_derived_event_history_begin_update_v1(
                derived_c_history.get(),
                &first_derived_c_checkpoint,
                3U,
                1U) ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_OK_V1,
            "derived C ABI begins rolling update from its exact checkpoint");
        std::array<l2flow_instrument_derived_event_row_v1, 1U>
            derived_c_update_rows{};
        std::size_t derived_c_update_count = 0U;
        std::uint32_t derived_c_update_eof = 0U;
        ok &= Expect(
            l2flow_instrument_derived_event_history_read_v1(
                derived_c_history.get(),
                derived_c_update_rows.data(),
                derived_c_update_rows.size(),
                &derived_c_update_count,
                &derived_c_update_eof) ==
                    L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_OK_V1 &&
                derived_c_update_count == 1U &&
                derived_c_update_eof == 0U &&
                derived_c_update_rows[0].event_kind ==
                    L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_REVISION_V1 &&
                derived_c_update_rows[0].operation ==
                    static_cast<std::uint8_t>(
                        market::ShanghaiOrderDeltaOperationV1::kUpdate) &&
                derived_c_update_rows[0].revision == 2U &&
                derived_c_update_rows[0].original_quantity_valid == 1U &&
                derived_c_update_rows[0].original_quantity == 151 &&
                derived_c_update_rows[0]
                        .observed_pre_add_trade_quantity ==
                    101 &&
                derived_c_update_rows[0]
                        .source_matched_quantity_valid ==
                    1U &&
                derived_c_update_rows[0].source_matched_quantity ==
                    101 &&
                derived_c_update_rows[0].remaining_quantity_valid == 1U &&
                derived_c_update_rows[0].remaining_quantity == 50 &&
                derived_c_update_rows[0].apply_to_book == 1U,
            "derived C ABI preserves the native T-to-A exact revision");
        derived_c_update_count =
            std::numeric_limits<std::size_t>::max();
        derived_c_update_eof = 0U;
        l2flow_instrument_derived_event_checkpoint_v1
            second_derived_c_checkpoint{};
        ok &= Expect(
            l2flow_instrument_derived_event_history_read_v1(
                derived_c_history.get(),
                nullptr,
                0U,
                &derived_c_update_count,
                &derived_c_update_eof) ==
                    L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_OK_V1 &&
                derived_c_update_count == 0U &&
                derived_c_update_eof == 1U &&
                l2flow_instrument_derived_event_history_verified_checkpoint_v1(
                    derived_c_history.get(),
                    &second_derived_c_checkpoint) ==
                    L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_OK_V1 &&
                second_derived_c_checkpoint
                        .derived_event_sequence_exclusive ==
                    first_derived_c_checkpoint
                            .derived_event_sequence_exclusive +
                        1U,
            "derived C ABI verifies the rolling suffix after EOF");
        std::size_t derived_c_finalize_count = 0U;
        ok &= Expect(
            l2flow_instrument_derived_event_history_finalize_v1(
                derived_c_history.get(),
                nullptr,
                0U,
                &derived_c_finalize_count) ==
                    L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_BUFFER_TOO_SMALL_V1 &&
                derived_c_finalize_count == 1U,
            "derived C ABI finalization reports its required capacity "
            "without losing the revision");
        std::array<l2flow_instrument_derived_event_row_v1, 1U>
            derived_c_finalize_rows{};
        ok &= Expect(
            l2flow_instrument_derived_event_history_finalize_v1(
                derived_c_history.get(),
                derived_c_finalize_rows.data(),
                derived_c_finalize_rows.size(),
                &derived_c_finalize_count) ==
                    L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_OK_V1 &&
                derived_c_finalize_count == 1U &&
                derived_c_finalize_rows[0].event_kind ==
                    L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_REVISION_V1 &&
                derived_c_finalize_rows[0].operation ==
                    static_cast<std::uint8_t>(
                        market::ShanghaiOrderDeltaOperationV1::kFinalize) &&
                derived_c_finalize_rows[0].revision == 3U &&
                derived_c_finalize_rows[0].remaining_quantity_valid ==
                    1U &&
                derived_c_finalize_rows[0].remaining_quantity == 50 &&
                (derived_c_finalize_rows[0].quality_flags &
                 market::ShanghaiOrderQualityBitV1(
                     market::ShanghaiOrderQualityFlagV1::
                         kEndedWithObservedBalance)) != 0U,
            "derived C ABI retains explicit clean-boundary finalization "
            "across buffer retry");
    }
    std::vector<ipc::InstrumentDerivedEventV1>
        empty_derived_events;
    ipc::InstrumentDerivedEventCheckpointV1
        empty_derived_checkpoint{};
    if (derived_history != nullptr) {
        ok &= Expect(
            derived_history->BeginUpdate(
                second_derived_checkpoint, 3U, 1U) ==
                ipc::InstrumentDerivedEventHistoryErrorV1::kNone,
            "derived empty update accepts latest exact checkpoint");
        ok &= DrainDerivedEventHistory(
            derived_history.get(),
            &empty_derived_events,
            &empty_derived_checkpoint,
            "derived empty update");
    }
    ok &= Expect(
        empty_derived_events.empty() &&
            empty_derived_checkpoint
                    .derived_event_sequence_exclusive ==
                second_derived_checkpoint
                    .derived_event_sequence_exclusive &&
            std::memcmp(
                &empty_derived_checkpoint.raw_checkpoint,
                &second_derived_checkpoint.raw_checkpoint,
                sizeof(
                    empty_derived_checkpoint.raw_checkpoint)) == 0,
        "empty derived update advances no derived sequence/state");
    ok &= RunPythonDerivedHistorySmoke(socket_path, 3U);
    ipc::InstrumentRawEventHistoryCheckpointV2 empty_checkpoint{};
    const std::array<std::uint64_t, 0U> empty_sequences{};
    ok &= ReadNativeRawEventHistory(
        socket_path,
        session,
        3U,
        &second_checkpoint,
        empty_sequences,
        empty_sequences,
        &empty_checkpoint,
        "native empty suffix");
    ok &= Expect(
        std::memcmp(
            &empty_checkpoint,
            &second_checkpoint,
            sizeof(empty_checkpoint)) == 0,
        "empty rolling suffix preserves the exact verified checkpoint");
    {
        ipc::InstrumentRawEventHistorySessionV2 abandoned_session;
        ipc::InstrumentRawEventHistoryCursorV2 abandoned_cursor;
        ipc::InstrumentRawEventHistoryEndpointV2 abandoned_target{};
        ok &= Expect(
            ipc::InstrumentRawEventHistorySessionV2::Open(
                socket_path.c_str(),
                session,
                3U,
                3'000U,
                &abandoned_session) ==
                    ipc::InstrumentRawEventHistoryErrorV2::kNone &&
                abandoned_session.OpenFull(
                    1U, 1U, &abandoned_cursor) ==
                    ipc::InstrumentRawEventHistoryErrorV2::kNone &&
                abandoned_session.is_open(),
            "native early-close fixture opens an active cursor");
        abandoned_cursor.Reset();
        ok &= Expect(
            !abandoned_session.is_open() &&
                abandoned_session.Target(&abandoned_target) ==
                    ipc::InstrumentRawEventHistoryErrorV2::kClosed,
            "closing before EOF truthfully fail-closes the native session");
    }

    ok &= Expect(
        service->PublishProcessingProgress({10U, 9U}) &&
            service->PublishProcessingProgress({9U, 7U}),
        "progress publication merges concurrent-stale pairs monotonically");
    ok &= Expect(
        l2flow_shm_reader_session_v2(
            reader.get(), &session) ==
                L2FLOW_SHM_READER_OK_V2 &&
            session.bound_count == 4U &&
            session.available_count == 1U &&
            session.snapshot_available_count == 1U &&
            session.tick_available_count == 1U &&
            session.factor_eligible_count == 1U &&
            session.accepted_sequence == 10U &&
            session.applied_sequence == 9U &&
            session.processing_lag_records == 1U &&
            session.kline_generation == 2U &&
            service->key_arena_used_bytes() ==
                used_after_binding,
        "counts/progress satisfy one coherent header envelope");
    ok &= Expect(
        session.factor_eligible_count <=
                session.snapshot_available_count &&
            session.snapshot_available_count <=
                session.available_count &&
            session.available_count <= session.bound_count &&
            session.bound_count <= session.capacity &&
            session.tick_available_count <=
                session.available_count &&
            session.applied_sequence <=
                session.accepted_sequence,
        "wire count and processing inequalities hold");

    constexpr std::uint32_t instrument_id = 1U;
    ipc::RealtimeWireSnapshotPayloadV2 latest_snapshot{};
    std::uint8_t item_status = 0xffU;
    ok &= Expect(
        l2flow_shm_reader_latest_snapshots_v2(
            reader.get(),
            &instrument_id,
            1U,
            &latest_snapshot,
            sizeof(latest_snapshot),
            &item_status) == L2FLOW_SHM_READER_OK_V2 &&
            item_status == L2FLOW_LATEST_AVAILABLE_V2 &&
            latest_snapshot.common.ingress_sequence == 2U &&
            latest_snapshot.common.ordinal == 0U,
        "repeated existing-ID publish is direct ordinal latest");
    constexpr std::size_t hot_read_iterations = 100'000U;
    const auto hot_read_begin = std::chrono::steady_clock::now();
    bool hot_reads_valid = true;
    for (std::size_t iteration = 0U;
         iteration < hot_read_iterations;
         ++iteration) {
        item_status = 0xffU;
        if (l2flow_shm_reader_latest_snapshots_v2(
                reader.get(),
                &instrument_id,
                1U,
                &latest_snapshot,
                sizeof(latest_snapshot),
                &item_status) != L2FLOW_SHM_READER_OK_V2 ||
            item_status != L2FLOW_LATEST_AVAILABLE_V2 ||
            latest_snapshot.common.instrument_id != instrument_id ||
            latest_snapshot.common.ingress_sequence != 2U) {
            hot_reads_valid = false;
            break;
        }
    }
    const auto hot_read_elapsed =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - hot_read_begin)
            .count();
    ok &= Expect(
        hot_reads_valid && hot_read_elapsed > 0 &&
            service->key_arena_used_bytes() == used_after_binding,
        "100k known-ID reads neither resolve nor mutate the catalog");
    if (hot_read_elapsed > 0) {
        std::cout
            << "known-id latest snapshot mean_ns="
            << hot_read_elapsed /
                   static_cast<std::int64_t>(hot_read_iterations)
            << " iterations=" << hot_read_iterations << '\n';
    }
    ok &= RunPythonKnownIdProbe(socket_path);
    ipc::RealtimeWireKLinePayloadV2 latest_kline{};
    constexpr std::uint32_t window_id = kWindowId;
    ok &= Expect(
        l2flow_shm_reader_latest_klines_v2(
            reader.get(),
            &instrument_id,
            &window_id,
            1U,
            &latest_kline,
            sizeof(latest_kline),
            &item_status) == L2FLOW_SHM_READER_OK_V2 &&
            item_status == L2FLOW_LATEST_AVAILABLE_V2 &&
            latest_kline.generation == 2U &&
            latest_kline.instrument_id == 1U &&
            latest_kline.present == 1U,
        "Reader observes published KLine from active table");
    const std::uint64_t catalog_generation =
        session.catalog_generation;
    std::array<std::uint8_t, 32U> catalog_digest{};
    std::memcpy(
        catalog_digest.data(),
        session.catalog_digest,
        catalog_digest.size());

    service->MarkDraining();
    ok &= Expect(
        Submit(runtime.get(), TickInput(3U, 5U, 3U)),
        "DRAINING still accepts an existing-ID publication");
    ipc::RealtimeWireTickPayloadV2 latest_tick{};
    ok &= Expect(
        WaitUntil([&] {
            item_status = 0xffU;
            return l2flow_shm_reader_latest_ticks_v2(
                       reader.get(),
                       &instrument_id,
                       1U,
                       &latest_tick,
                       sizeof(latest_tick),
                       &item_status) ==
                       L2FLOW_SHM_READER_OK_V2 &&
                   item_status == L2FLOW_LATEST_AVAILABLE_V2 &&
                   latest_tick.common.tick_stream_sequence == 3U;
        }),
        "DRAINING tick reaches latest and contiguous ring");
    ok &= Expect(
        service->PublishProcessingProgress({12U, 12U}),
        "DRAINING accepts progress publication");
    ok &= Expect(
        l2flow_shm_reader_session_v2(
            reader.get(), &session) ==
                L2FLOW_SHM_READER_OK_V2 &&
            session.server_state ==
                static_cast<std::uint32_t>(
                    ipc::RealtimeServerStateV2::kDraining) &&
            session.catalog_generation == catalog_generation &&
            std::memcmp(
                session.catalog_digest,
                catalog_digest.data(),
                catalog_digest.size()) == 0 &&
            session.bound_count == 4U &&
            session.available_count == 1U &&
            session.snapshot_available_count == 1U &&
            session.tick_available_count == 1U &&
            session.factor_eligible_count == 1U &&
            session.accepted_sequence == 12U &&
            session.applied_sequence == 12U,
        "repeated existing-ID traffic leaves catalog/key/counts unchanged");

    runtime->StopAndDrain();
    ok &= Expect(
        service->MarkStoppedClean(3U),
        "STOPPED_CLEAN requires exact highest and contiguous tick watermarks");
    ok &= Expect(
        l2flow_shm_reader_session_v2(
            reader.get(), &session) ==
                L2FLOW_SHM_READER_OK_V2 &&
            session.server_state ==
                static_cast<std::uint32_t>(
                    ipc::RealtimeServerStateV2::kStoppedClean) &&
            session.tick_highest_published_sequence == 3U &&
            session.tick_contiguous_published_sequence == 3U,
        "clean terminal mapping preserves exact ring watermark");
    service->StopControl();
    return ok && !service->failed();
}

bool TestPromotionExposureGate() {
    ScopedTempDirectory temporary;
    DailyRuntimeFixture fixture =
        MakeManualDailyFixture(1U, kSessionEpoch + 300U);
    if (!Expect(temporary.valid(), "create promotion-gate temp directory") ||
        !Expect(static_cast<bool>(fixture),
                "create promotion-gate daily catalog")) {
        return false;
    }
    const std::filesystem::path socket_path =
        temporary.path() / "promoted-fast.sock";
    const auto exposure_gate =
        std::make_shared<std::atomic<bool>>(false);
    ipc::RealtimeSharedServiceConfigV2 config{};
    config.run_id = RunId(0x6fU);
    config.session_epoch = kSessionEpoch + 300U;
    config.trade_date = kTradeDate;
    config.daily_catalog = fixture.catalog;
    config.coverage_from_open = true;
    config.startup_prefix_recovered = true;
    config.full_day_factor_valid = true;
    config.tick_ring_capacity = 4U;
    config.key_arena_bytes = 128U;
    config.maximum_mapping_bytes = 16U * 1024U * 1024U;
    config.control_socket_path = socket_path;
    config.control_exposure_gate = exposure_gate;

    std::shared_ptr<ipc::RealtimeSharedMarketServiceV2> service;
    int system_error = 0;
    bool ok = Expect(
        ipc::RealtimeSharedMarketServiceV2::Create(
            config, &service, &system_error) ==
                ipc::RealtimeSharedServiceCreateErrorV2::kNone &&
            service != nullptr,
        "create recovered FAST behind a shared exposure gate");
    if (service == nullptr) {
        return false;
    }
    ok &= Expect(
        service->PrepareCertifiedPrefixValidBeforeStart() &&
            service->Start(&system_error),
        "prepare certified capability before starting gated FAST control");
    const SessionTransfer hidden = RequestSession(socket_path);
    ok &= Expect(
        hidden.fd.get() < 0,
        "false promotion gate transfers no FAST descriptor");

    exposure_gate->store(true, std::memory_order_release);
    SessionTransfer visible = RequestSession(socket_path);
    ipc::RealtimeWireHeaderV2 header{};
    const ssize_t header_bytes =
        visible.fd.get() < 0
            ? -1
            : ::pread(
                  visible.fd.get(), &header, sizeof(header), 0);
    ok &= Expect(
        visible.fd.get() >= 0 &&
            header_bytes == static_cast<ssize_t>(sizeof(header)) &&
            header.server_state == static_cast<std::uint32_t>(
                ipc::RealtimeServerStateV2::kActive) &&
            (header.flags &
             ipc::kRealtimeHeaderCertifiedPrefixValidV2) != 0U,
        "first obtainable FAST descriptor is ACTIVE with certified prefix");
    service->MarkDraining();
    ok &= Expect(
        service->MarkStoppedClean(0U),
        "gated FAST service stops cleanly at the empty Tick frontier");
    service->StopControl();
    return ok;
}

bool TestProcessingAdmissionPublishesWireLatest() {
    ScopedTempDirectory temporary;
    DailyRuntimeFixture fixture = MakePipelineDailyFixture(33U);
    if (!Expect(
            temporary.valid() && static_cast<bool>(fixture),
            "create processing-to-Wire integration fixture")) {
        return false;
    }

    const common::Identity128 run_id = RunId(0x33U);
    const std::filesystem::path socket_path =
        temporary.path() / "processing-latest.sock";

    ipc::RealtimeSharedServiceConfigV2 service_config{};
    service_config.run_id = run_id;
    service_config.session_epoch = 33U;
    service_config.trade_date = kTradeDate;
    service_config.daily_catalog = fixture.catalog;
    service_config.coverage_from_open = true;
    service_config.tick_ring_capacity = 16U;
    service_config.key_arena_bytes = 128U;
    service_config.maximum_mapping_bytes =
        16U * 1024U * 1024U;
    service_config.control_socket_path = socket_path;

    std::shared_ptr<ipc::RealtimeSharedMarketServiceV2> service;
    int system_error = 0;
    const ipc::RealtimeSharedServiceCreateErrorV2 service_error =
        ipc::RealtimeSharedMarketServiceV2::Create(
            service_config, &service, &system_error);
    bool ok = Expect(
        service_error ==
                ipc::RealtimeSharedServiceCreateErrorV2::kNone &&
            service != nullptr && system_error == 0,
        "create processing-to-Wire V2 service");
    if (!ok) {
        std::cerr
            << "create error="
            << ipc::RealtimeSharedServiceCreateErrorNameV2(
                   service_error)
            << " errno=" << system_error << '\n';
        return false;
    }
    ok &= Expect(
        service->Start(&system_error) && system_error == 0,
        "start processing-to-Wire V2 control plane");
    if (!ok) {
        service->StopControl();
        return false;
    }

    SessionTransfer transfer = RequestSession(socket_path);
    ReaderHandle reader;
    ok &= Expect(
        transfer.fd.get() >= 0 &&
            l2flow_shm_reader_open_fd_v2(
                transfer.fd.get(), reader.output()) ==
                L2FLOW_SHM_READER_OK_V2 &&
            reader.get() != nullptr,
        "open real C Reader after the daily catalog is prepublished");
    if (reader.get() == nullptr) {
        service->MarkFailed();
        service->StopControl();
        return false;
    }
    transfer.fd.Reset();

    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    PipelineCleanup pipeline_cleanup(&pipeline);
    runtime::RealtimePipelineConfigV1 pipeline_config{};
    pipeline_config.run_id = run_id;
    pipeline_config.trade_date = kTradeDate;
    pipeline_config.daily_catalog = fixture.catalog;
    pipeline_config.runtime_state = fixture.runtime_state.get();
    pipeline_config.source_stream_ids = kSourceStreamIds;
    pipeline_config.maximum_sdk_message_bytes = 4096U;
    // This integration deliberately offers six callbacks back-to-back on
    // one source. Direct source-local admission is fail-closed when that
    // source FIFO fills, so size the test lane for the offered burst.
    pipeline_config.decoder_queue_capacity_per_source = 8U;
    pipeline_config.completion_tracker_capacity = 16U;
    pipeline_config.tick_ring_capacity = 16U;
    pipeline_config.store_worker_count = 1U;
    pipeline_config.store_queue_capacity_per_source_worker = 16U;
    pipeline_config.intraday_store.segment_target_bytes =
        market::kIntradayInstrumentStoreMinimumSegmentBytesV1;
    pipeline_config.intraday_store.maximum_session_records = 16U;
    pipeline_config.intraday_store.maximum_session_accounted_bytes =
        1U << 20U;
    pipeline_config.intraday_store.maximum_records_per_batch = 16U;
    pipeline_config.intraday_store.coverage_from_open = true;
    pipeline_config.applied_record_sink = service;
    pipeline_config.processing_progress_sink = service;
    pipeline_config.store_generation_sink = service;
    pipeline_config.sdk.enabled = false;

    std::string pipeline_detail;
    const runtime::RealtimePipelineCreateErrorV1 pipeline_error =
        runtime::RealtimePipelineV1::Create(
            pipeline_config, &pipeline, &pipeline_detail);
    ok &= Expect(
        pipeline_error ==
                runtime::RealtimePipelineCreateErrorV1::kNone &&
            pipeline != nullptr,
        "create Pipeline with service as all three projections: " +
            pipeline_detail);
    if (pipeline_error !=
            runtime::RealtimePipelineCreateErrorV1::kNone ||
        pipeline == nullptr) {
        service->MarkFailed();
        service->StopControl();
        return false;
    }

    FakeSdkMessage snapshot(
        sdk::kProductionMessageKeysV1[2U],
        PipelineShenzhenSnapshotBody());
    constexpr std::uint64_t kCaptureCount = 6U;
    const runtime::RealtimePipelineIngressResultV1 first_ingress =
        pipeline->InjectSdkMessageForTest(&snapshot);
    ok &= Expect(
        first_ingress.accepted() &&
            first_ingress.global_ingress_sequence == 1U,
        "first capture is admitted directly to its decoder lane");
    for (std::uint64_t sequence = 2U;
         sequence <= kCaptureCount;
         ++sequence) {
        const runtime::RealtimePipelineIngressResultV1 ingress =
            pipeline->InjectSdkMessageForTest(&snapshot);
        ok &= Expect(
            ingress.accepted() &&
                ingress.global_ingress_sequence == sequence,
            "subsequent callback preserves dense direct admission");
    }

    l2flow_shm_session_info_v2 session{};
    ipc::RealtimeWireSnapshotPayloadV2 latest{};
    std::uint8_t latest_status = 0xffU;
    constexpr std::uint32_t instrument_id = 1U;
    const bool visible = WaitUntil([&] {
        latest_status = 0xffU;
        return l2flow_shm_reader_session_v2(
                   reader.get(), &session) ==
                   L2FLOW_SHM_READER_OK_V2 &&
               session.server_state ==
                   static_cast<std::uint32_t>(
                       ipc::RealtimeServerStateV2::kActive) &&
               session.catalog_scope ==
                   static_cast<std::uint32_t>(
                       ipc::RealtimeCatalogScopeV2::
                           kDeclaredDailyAShare) &&
               session.coverage_complete == 1U &&
               session.catalog_trade_date == kTradeDate &&
               session.catalog_version == 33U &&
               session.catalog_generation == 1U &&
               session.bound_count == 1U &&
               session.available_count == 1U &&
               session.snapshot_available_count == 1U &&
               session.accepted_sequence == kCaptureCount &&
               session.applied_sequence == kCaptureCount &&
               session.processing_lag_records == 0U &&
               l2flow_shm_reader_latest_snapshots_v2(
                   reader.get(),
                   &instrument_id,
                   1U,
                   &latest,
                   sizeof(latest),
                   &latest_status) ==
                   L2FLOW_SHM_READER_OK_V2 &&
               latest_status == L2FLOW_LATEST_AVAILABLE_V2 &&
               latest.common.instrument_id == instrument_id &&
               latest.common.ordinal == 0U &&
               latest.common.ingress_sequence ==
                   kCaptureCount &&
               latest.last_price.valid == 1U &&
               latest.last_price.normalized_p6 == 12'345'600;
    });
    ok &= Expect(
        visible,
        "real C Reader sees the full accepted/applied capture prefix and "
        "latest record");

    if (visible) {
        ok &= RunPythonKnownIdProbe(socket_path);
        l2flow_shm_session_info_v2 after_python{};
        ok &= Expect(
            l2flow_shm_reader_session_v2(
                reader.get(), &after_python) ==
                    L2FLOW_SHM_READER_OK_V2 &&
                after_python.accepted_sequence ==
                    kCaptureCount &&
                after_python.applied_sequence ==
                    kCaptureCount &&
                after_python.processing_lag_records == 0U,
            "Python hot reads consume the applied processing prefix");
    }

    const runtime::RealtimePipelineSnapshotV1 state =
        pipeline->Snapshot();
    ok &= Expect(
        state.processing_progress.accepted_sequence == kCaptureCount &&
            state.processing_progress.applied_sequence == kCaptureCount &&
            state.processing_progress.processing_lag_records() == 0U &&
            !state.fatal,
        "Pipeline preserves accepted/applied ordering through publication");

    pipeline->StopAndDrain();
    service->MarkDraining();
    ok &= Expect(
        service->MarkStoppedClean(0U),
        "processing-to-Wire integration service stops with no tick gap");
    service->StopControl();
    return ok && !pipeline->fatal() && !service->failed();
}

bool TestKeyArenaExhaustionIsFatal() {
    ScopedTempDirectory temporary;
    DailyRuntimeFixture fixture = MakeManualDailyFixture(2U, 30U);
    if (!Expect(
            temporary.valid() && static_cast<bool>(fixture),
            "create exhaustion fixture")) {
        return false;
    }
    ipc::RealtimeSharedServiceConfigV2 config{};
    config.run_id = RunId(0x30U);
    config.session_epoch = 30U;
    config.trade_date = kTradeDate;
    config.daily_catalog = fixture.catalog;
    config.coverage_from_open = true;
    config.tick_ring_capacity = 2U;
    config.key_arena_bytes = 2U;
    config.maximum_mapping_bytes = 8U * 1024U * 1024U;
    config.control_socket_path =
        temporary.path() / "exhaust.sock";
    std::shared_ptr<ipc::RealtimeSharedMarketServiceV2> service;
    const bool ok = Expect(
        ipc::RealtimeSharedMarketServiceV2::Create(
            config, &service) ==
                ipc::RealtimeSharedServiceCreateErrorV2::
                    kCatalogMismatch &&
            service == nullptr,
        "daily catalog key arena exhaustion fails before ACTIVE");
    return ok;
}

bool TestStoppedCleanRejectsMismatchedWatermark() {
    ScopedTempDirectory temporary;
    DailyRuntimeFixture fixture = MakeManualDailyFixture(1U, 31U);
    if (!Expect(
            temporary.valid() && static_cast<bool>(fixture),
            "create terminal-watermark fixture")) {
        return false;
    }
    ipc::RealtimeSharedServiceConfigV2 config{};
    config.run_id = RunId(0x31U);
    config.session_epoch = 31U;
    config.trade_date = kTradeDate;
    config.daily_catalog = fixture.catalog;
    config.coverage_from_open = true;
    config.tick_ring_capacity = 2U;
    config.key_arena_bytes = 16U;
    config.maximum_mapping_bytes = 8U * 1024U * 1024U;
    config.control_socket_path =
        temporary.path() / "terminal.sock";
    std::shared_ptr<ipc::RealtimeSharedMarketServiceV2> service;
    bool ok = Expect(
        ipc::RealtimeSharedMarketServiceV2::Create(
            config, &service) ==
                ipc::RealtimeSharedServiceCreateErrorV2::kNone &&
            service != nullptr && service->Start(),
        "start terminal-watermark fixture");
    if (!ok) {
        return false;
    }
    service->MarkDraining();
    ok &= Expect(
        !service->MarkStoppedClean(1U) && service->failed(),
        "STOPPED_CLEAN rejects a non-exact admitted tick watermark");
    service->StopControl();
    return ok;
}

bool TestTickRingRejectsUnclosedGapOverwrite() {
    ScopedTempDirectory temporary;
    DailyRuntimeFixture fixture = MakeManualDailyFixture(1U, 32U);
    if (!Expect(
            temporary.valid() && static_cast<bool>(fixture),
            "create tick-gap fixture")) {
        return false;
    }
    ipc::RealtimeSharedServiceConfigV2 config{};
    config.run_id = RunId(0x32U);
    config.session_epoch = 32U;
    config.trade_date = kTradeDate;
    config.daily_catalog = fixture.catalog;
    config.coverage_from_open = true;
    config.tick_ring_capacity = 2U;
    config.key_arena_bytes = 32U;
    config.maximum_mapping_bytes = 8U * 1024U * 1024U;
    config.control_socket_path =
        temporary.path() / "tick-gap.sock";
    std::shared_ptr<ipc::RealtimeSharedMarketServiceV2> service;
    bool ok = Expect(
        ipc::RealtimeSharedMarketServiceV2::Create(
            config, &service) ==
                ipc::RealtimeSharedServiceCreateErrorV2::kNone &&
            service != nullptr && service->Start(),
        "start tick-gap fixture");
    if (!ok) {
        return false;
    }

    market::RealtimeHistoryRuntimeConfigV1 runtime_config{};
    runtime_config.source_stream_ids = kSourceStreamIds;
    runtime_config.worker_count = 1U;
    runtime_config.queue_capacity_per_source_worker = 4U;
    runtime_config.runtime_state = fixture.runtime_state.get();
    runtime_config.intraday_store.segment_target_bytes =
        market::kIntradayInstrumentStoreMinimumSegmentBytesV1;
    runtime_config.intraday_store.maximum_session_records = 4U;
    runtime_config.intraday_store.maximum_session_accounted_bytes =
        1U << 20U;
    runtime_config.intraday_store.maximum_records_per_batch = 4U;
    runtime_config.intraday_store.coverage_from_open = true;
    runtime_config.applied_record_sink = service;
    std::unique_ptr<market::RealtimeHistoryRuntimeV1> runtime;
    ok &= Expect(
        market::RealtimeHistoryRuntimeV1::Create(
            runtime_config, &runtime) ==
                market::RealtimeHistoryCreateErrorV1::kNone &&
            runtime != nullptr,
        "create tick-gap history runtime");
    if (runtime == nullptr) {
        service->MarkFailed();
        service->StopControl();
        return false;
    }

    ok &= Expect(
        Submit(runtime.get(), TickInput(1U, 2U, 2U)) &&
            WaitUntil([&] {
                std::shared_ptr<
                    const market::DailyInstrumentCatalogSnapshotV2>
                    snapshot;
                return fixture.runtime_state->AcquireSnapshot(&snapshot) ==
                           market::InstrumentRuntimeStateErrorV2::
                               kNone &&
                       snapshot != nullptr &&
                       snapshot->tick_available_count() == 1U &&
                       !service->failed();
            }),
        "publish tick two while tick one is absent");
    ok &= Expect(
        Submit(runtime.get(), TickInput(2U, 3U, 3U)) &&
            WaitUntil([&] { return service->failed(); }),
        "tick three fails closed before a two-slot ring can hide tick one");

    runtime->StopAndDrain();
    service->StopControl();
    return ok && service->failed();
}

bool RunLatencyBenchmark(bool measure_stage_latency) {
    constexpr std::size_t kCapacity = 12'000U;
    constexpr std::size_t kActiveInstrumentCount = kCapacity;
    constexpr std::size_t kFillBatchSize = 512U;
    constexpr std::size_t kWarmupSamples = 1'000U;
    constexpr std::size_t kMeasuredSamples = 10'000U;
    constexpr std::size_t kBurstSamples = 3'000U;
    constexpr std::size_t kMaximumSequence = 100'000U;
    constexpr std::size_t kQueueCapacity = 4'096U;
    constexpr std::size_t kClosedLoopAppliedBarrierRecords = 512U;
    constexpr std::uint32_t kStoreWorkerCount = 4U;
    constexpr std::uint64_t kTickRingCapacity = 32'768U;
    static_assert(
        kActiveInstrumentCount +
                2U * (kWarmupSamples + kMeasuredSamples) +
                kBurstSamples <=
            kMaximumSequence);

    const char* const python_cpu =
        std::getenv("L2FLOW_PYTHON_CPU");
    std::cout
        << "ENV capacity=" << kCapacity
        << " worker_count=" << kStoreWorkerCount
        << " decoder_queue_per_source=" << kQueueCapacity
        << " store_queue_per_source_worker=" << kQueueCapacity
        << " tick_ring_capacity=" << kTickRingCapacity
        << " active_instrument_count=" << kActiveInstrumentCount
        << " fill_batch_size=" << kFillBatchSize
        << " closed_loop_applied_barrier_records="
        << kClosedLoopAppliedBarrierRecords
        << " barrier_scope=between_samples_not_reader_gate"
        << " burst_records=" << kBurstSamples
        << " burst_scope=within_default_queue_capacity"
        << " stage_latency="
        << (measure_stage_latency ? 1 : 0)
        << " stage_scope=whole_run_including_setup"
        << " clock=CLOCK_MONOTONIC"
        << " affinity=" << CpuAffinityText()
        << " python_cpu="
        << (python_cpu == nullptr ? "unset" : python_cpu)
        << '\n';

    ScopedTempDirectory temporary;
    DailyRuntimeFixture fixture =
        MakeBenchmarkDailyFixture(kCapacity, 47U, false);
    if (!Expect(
            temporary.valid() && static_cast<bool>(fixture),
            "create latency benchmark daily catalog")) {
        return false;
    }
    const common::Identity128 run_id = RunId(0x47U);
    const std::filesystem::path socket_path =
        temporary.path() / "latency.sock";

    ipc::RealtimeSharedServiceConfigV2 service_config{};
    service_config.run_id = run_id;
    service_config.session_epoch = 47U;
    service_config.trade_date = kTradeDate;
    service_config.daily_catalog = fixture.catalog;
    service_config.coverage_from_open = true;
    service_config.tick_ring_capacity = kTickRingCapacity;
    service_config.control_socket_path = socket_path;
    std::shared_ptr<ipc::RealtimeSharedMarketServiceV2> service;
    int system_error = 0;
    const ipc::RealtimeSharedServiceCreateErrorV2 service_error =
        ipc::RealtimeSharedMarketServiceV2::Create(
            service_config, &service, &system_error);
    if (!Expect(
            service_error ==
                    ipc::RealtimeSharedServiceCreateErrorV2::kNone &&
                service != nullptr && system_error == 0,
            "create latency Wire V2 service")) {
        std::cerr
            << "latency service create error="
            << ipc::RealtimeSharedServiceCreateErrorNameV2(
                   service_error)
            << " errno=" << system_error << '\n';
        return false;
    }
    if (!Expect(
            service->Start(&system_error) && system_error == 0,
            "start latency Wire V2 service")) {
        service->StopControl();
        return false;
    }
    std::cout
        << "MAPPING total_bytes=" << service->mapping_bytes()
        << " key_arena_used_bytes="
        << service->key_arena_used_bytes() << '\n';

    auto timed_sink = std::make_shared<TimedAppliedSink>(
        service, kMaximumSequence);
    auto sdk_state = std::make_shared<LatencySdkState>();
    auto sdk_factory =
        std::make_shared<LatencySdkFactory>(sdk_state);
    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    PipelineCleanup pipeline_cleanup(&pipeline);
    runtime::RealtimePipelineConfigV1 pipeline_config{};
    pipeline_config.run_id = run_id;
    pipeline_config.trade_date = kTradeDate;
    pipeline_config.daily_catalog = fixture.catalog;
    pipeline_config.runtime_state = fixture.runtime_state.get();
    pipeline_config.source_stream_ids = kSourceStreamIds;
    pipeline_config.maximum_sdk_message_bytes = 4'096U;
    pipeline_config.decoder_queue_capacity_per_source =
        kQueueCapacity;
    pipeline_config.tick_ring_capacity = kTickRingCapacity;
    pipeline_config.store_worker_count = kStoreWorkerCount;
    pipeline_config.store_queue_capacity_per_source_worker =
        kQueueCapacity;
    pipeline_config.intraday_store.segment_target_bytes =
        market::kIntradayInstrumentStoreMinimumSegmentBytesV1;
    pipeline_config.intraday_store.maximum_session_records =
        kMaximumSequence;
    pipeline_config.intraday_store.maximum_session_accounted_bytes =
        1ULL * 1024ULL * 1024ULL * 1024ULL;
    pipeline_config.intraday_store.maximum_records_per_batch =
        kQueueCapacity;
    pipeline_config.intraday_store.coverage_from_open = true;
    pipeline_config.applied_record_sink = timed_sink;
    pipeline_config.processing_progress_sink = service;
    pipeline_config.store_generation_sink = service;
    pipeline_config.measure_stage_latency =
        measure_stage_latency;
    pipeline_config.sdk.enabled = true;
    pipeline_config.sdk.server_address = "latency.invalid";
    pipeline_config.sdk.user_name = "latency-test";

    std::string pipeline_detail;
    const runtime::RealtimePipelineCreateErrorV1 pipeline_error =
        runtime::RealtimePipelineV1::CreateForTest(
            pipeline_config,
            sdk_factory,
            &pipeline,
            &pipeline_detail);
    if (!Expect(
            pipeline_error ==
                    runtime::RealtimePipelineCreateErrorV1::kNone &&
                pipeline != nullptr,
            "create full latency Pipeline: " + pipeline_detail)) {
        service->MarkFailed();
        service->StopControl();
        return false;
    }
    mdl::MessageHandlerBase* const handler = sdk_state->handler();
    if (!Expect(handler != nullptr, "latency SDK handler installed")) {
        service->MarkFailed();
        service->StopControl();
        return false;
    }
    FakeSdkMessage snapshot(
        sdk::kProductionMessageKeysV1[2U],
        PipelineShenzhenSnapshotBody());

    SessionTransfer transfer = RequestSession(socket_path);
    ReaderHandle reader;
    if (!Expect(
            transfer.fd.get() >= 0 &&
                l2flow_shm_reader_open_fd_v2(
                    transfer.fd.get(), reader.output()) ==
                    L2FLOW_SHM_READER_OK_V2 &&
                reader.get() != nullptr,
            "open benchmark C Reader before working-set fill")) {
        service->MarkFailed();
        service->StopControl();
        return false;
    }
    transfer.fd.Reset();

    auto wait_publication =
        [&](std::uint64_t sequence,
            std::uint64_t* recv,
            std::uint64_t* begin,
            std::uint64_t* end) {
            return WaitUntil([&] {
                return timed_sink->Read(
                    sequence, recv, begin, end);
            });
        };

    auto wait_pipeline_prefix =
        [&](std::uint64_t sequence) {
            const auto deadline =
                std::chrono::steady_clock::now() +
                std::chrono::seconds(30);
            for (;;) {
                const runtime::RealtimePipelineSnapshotV1 state =
                    pipeline->Snapshot();
                if (state.fatal || pipeline->fatal()) {
                    return false;
                }
                if (state.accepted_messages == sequence &&
                    state.processing_progress.accepted_sequence ==
                        sequence &&
                    state.processing_progress.applied_sequence ==
                        sequence) {
                    return true;
                }
                if (state.accepted_messages > sequence ||
                    state.processing_progress.accepted_sequence >
                        sequence ||
                    state.processing_progress.applied_sequence >
                        sequence ||
                    std::chrono::steady_clock::now() >= deadline) {
                    return false;
                }
                std::this_thread::yield();
            }
        };

    const std::uint64_t fill_start = MonotonicNowNs();
    std::uint64_t fill_accepted = 0U;
    std::uint64_t fill_visible = 0U;
    for (std::size_t first = 1U;
         first <= kActiveInstrumentCount;
         first += kFillBatchSize) {
        const std::size_t last = std::min(
            kActiveInstrumentCount,
            first + kFillBatchSize - 1U);
        for (std::size_t instrument = first;
             instrument <= last;
             ++instrument) {
            const std::string security_id = ShenzhenAShareSecurityId(
                static_cast<std::uint32_t>(instrument));
            if (!Expect(
                    security_id.size() == 6U,
                    "format working-set SecurityID")) {
                return false;
            }
            FakeSdkMessage fill_snapshot(
                sdk::kProductionMessageKeysV1[2U],
                PipelineShenzhenSnapshotBody(security_id));
            handler->OnMessage(nullptr, &fill_snapshot);
        }
        if (last == kActiveInstrumentCount) {
            fill_accepted = MonotonicNowNs();
        }
        if (!Expect(
                wait_pipeline_prefix(
                    static_cast<std::uint64_t>(last)),
                "working-set batch reaches applied prefix")) {
            return false;
        }
        if (last == kActiveInstrumentCount) {
            fill_visible = MonotonicNowNs();
        }
    }
    if (!Expect(
            fill_start != 0U && fill_accepted >= fill_start &&
                fill_visible >= fill_accepted,
            "measure working-set fill boundaries")) {
        return false;
    }

    std::shared_ptr<
        const market::DailyInstrumentCatalogSnapshotV2>
        fill_catalog;
    if (!Expect(
            fixture.runtime_state->AcquireSnapshot(&fill_catalog) ==
                    market::InstrumentRuntimeStateErrorV2::kNone &&
                fill_catalog != nullptr &&
                fill_catalog->bound_count() ==
                    kActiveInstrumentCount &&
                fill_catalog->available_count() ==
                    kActiveInstrumentCount &&
                fill_catalog->snapshot_available_count() ==
                    kActiveInstrumentCount &&
                fill_catalog->tick_available_count() == 0U &&
                fill_catalog->factor_eligible_count() ==
                    kActiveInstrumentCount,
            "working-set catalog has all daily instruments available")) {
        return false;
    }

    l2flow_shm_session_info_v2 fill_session{};
    bool session_matches_fill = false;
    const auto session_deadline =
        std::chrono::steady_clock::now() +
        std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < session_deadline) {
        const int session_error =
            l2flow_shm_reader_session_v2(
                reader.get(), &fill_session);
        if (session_error ==
                L2FLOW_SHM_READER_INCONSISTENT_READ_V2) {
            std::this_thread::yield();
            continue;
        }
        if (session_error != L2FLOW_SHM_READER_OK_V2) {
            break;
        }
        session_matches_fill =
            fill_session.catalog_generation == 1U &&
            fill_session.catalog_trade_date == kTradeDate &&
            fill_session.catalog_version == 47U &&
            fill_session.accepted_sequence ==
                kActiveInstrumentCount &&
            fill_session.applied_sequence ==
                kActiveInstrumentCount &&
            fill_session.processing_lag_records == 0U &&
            fill_session.bound_count ==
                kActiveInstrumentCount &&
            fill_session.available_count ==
                kActiveInstrumentCount &&
            fill_session.snapshot_available_count ==
                kActiveInstrumentCount &&
            fill_session.tick_available_count == 0U &&
            fill_session.factor_eligible_count ==
                kActiveInstrumentCount;
        if (session_matches_fill) {
            break;
        }
        std::this_thread::yield();
    }
    if (!Expect(
            session_matches_fill,
            "Wire session exposes the complete daily-catalog fill")) {
        return false;
    }

    constexpr std::array<std::uint32_t, 2U> kBoundaryIds{
        1U, static_cast<std::uint32_t>(kActiveInstrumentCount)};
    std::array<ipc::RealtimeWireSnapshotPayloadV2, 2U>
        boundary_latest{};
    std::array<std::uint8_t, 2U> boundary_status{};
    const int boundary_error =
        l2flow_shm_reader_latest_snapshots_v2(
            reader.get(),
            kBoundaryIds.data(),
            kBoundaryIds.size(),
            boundary_latest.data(),
            sizeof(boundary_latest[0U]),
            boundary_status.data());
    if (!Expect(
            boundary_error == L2FLOW_SHM_READER_OK_V2 &&
                boundary_status[0U] ==
                    L2FLOW_LATEST_AVAILABLE_V2 &&
                boundary_status[1U] ==
                    L2FLOW_LATEST_AVAILABLE_V2 &&
                boundary_latest[0U].common.instrument_id == 1U &&
                boundary_latest[0U].common.ingress_sequence == 1U &&
                boundary_latest[1U].common.instrument_id ==
                    kActiveInstrumentCount &&
                boundary_latest[1U].common.ingress_sequence ==
                    kActiveInstrumentCount,
            "C Reader verifies first and last daily IDs")) {
        return false;
    }

    std::cout
        << "FILL active_count=" << kActiveInstrumentCount
        << " batch_size=" << kFillBatchSize
        << " batches="
        << (kActiveInstrumentCount + kFillBatchSize - 1U) /
               kFillBatchSize
        << " first_sequence=1"
        << " last_sequence=" << kActiveInstrumentCount
        << " start_to_accepted_ns="
        << fill_accepted - fill_start
        << " accepted_to_visible_ns="
        << fill_visible - fill_accepted
        << " start_to_visible_ns="
        << fill_visible - fill_start
        << " mapping_bytes=" << service->mapping_bytes()
        << " key_arena_used_bytes="
        << service->key_arena_used_bytes() << '\n';

    std::uint64_t next_sequence =
        static_cast<std::uint64_t>(kActiveInstrumentCount) + 1U;
    std::vector<std::uint64_t> c_callback_duration;
    std::vector<std::uint64_t> c_strict_callback_to_consumer;
    std::vector<std::uint64_t> c_wire_recv_to_consumer;
    std::vector<std::uint64_t> c_successful_read_call;
    std::vector<std::uint64_t> c_strict_callback_to_ipc_begin;
    std::vector<std::uint64_t> c_strict_callback_to_ipc_return;
    std::vector<std::uint64_t> c_wire_recv_to_ipc_begin;
    std::vector<std::uint64_t> c_wire_recv_to_ipc_return;
    std::vector<std::uint64_t> c_ipc_call;
    std::uint64_t c_poll_count = 0U;
    std::uint64_t c_inconsistent_retries = 0U;
    std::uint64_t c_max_polls = 0U;
    for (std::size_t index = 0U;
         index < kWarmupSamples + kMeasuredSamples;
         ++index, ++next_sequence) {
        const std::uint64_t expected = next_sequence;
        const std::uint64_t strict_origin = MonotonicNowNs();
        handler->OnMessage(nullptr, &snapshot);
        const std::uint64_t callback_return = MonotonicNowNs();
        if (!Expect(
                strict_origin != 0U &&
                    callback_return >= strict_origin,
                "measure C callback invocation")) {
            return false;
        }

        ipc::RealtimeWireSnapshotPayloadV2 latest{};
        std::uint8_t status = 0xffU;
        std::uint64_t successful_call_ns = 0U;
        std::uint64_t seen_ns = 0U;
        std::uint64_t polls = 0U;
        std::uint64_t inconsistent_retries = 0U;
        const std::uint64_t deadline =
            MonotonicNowNs() + 3'000'000'000ULL;
        for (;;) {
            status = 0xffU;
            const std::uint64_t poll_begin = MonotonicNowNs();
            constexpr std::uint32_t instrument_id = 1U;
            const int actual_error =
                l2flow_shm_reader_latest_snapshots_v2(
                    reader.get(),
                    &instrument_id,
                    1U,
                    &latest,
                    sizeof(latest),
                    &status);
            const std::uint64_t poll_end = MonotonicNowNs();
            ++polls;
            if (poll_begin == 0U || poll_end < poll_begin) {
                return Expect(false, "C latest poll failed");
            }
            if (actual_error ==
                L2FLOW_SHM_READER_INCONSISTENT_READ_V2) {
                ++inconsistent_retries;
                if (poll_end >= deadline) {
                    return Expect(
                        false,
                        "C latest inconsistent-read retry timed out");
                }
                continue;
            }
            if (actual_error != L2FLOW_SHM_READER_OK_V2) {
                std::cerr
                    << "C latest error=" << actual_error << '\n';
                return Expect(false, "C latest poll failed");
            }
            if (status == L2FLOW_LATEST_AVAILABLE_V2) {
                if (latest.common.ingress_sequence > expected) {
                    return Expect(
                        false,
                        "C latest skipped a closed-loop sequence");
                }
                if (latest.common.ingress_sequence == expected) {
                    successful_call_ns = poll_end - poll_begin;
                    seen_ns = poll_end;
                    break;
                }
            }
            if (poll_end >= deadline) {
                return Expect(
                    false,
                    "C latest closed-loop observation timed out");
            }
        }
        std::uint64_t recv = 0U;
        std::uint64_t ipc_begin = 0U;
        std::uint64_t ipc_end = 0U;
        if (!Expect(
                wait_publication(
                    expected, &recv, &ipc_begin, &ipc_end) &&
                    latest.common.recv_monotonic_ns > 0 &&
                    recv == static_cast<std::uint64_t>(
                                latest.common.recv_monotonic_ns) &&
                    recv >= strict_origin &&
                    seen_ns >= recv &&
                    ipc_begin >= strict_origin &&
                    ipc_end >= ipc_begin,
                "correlate C observation with timed IPC publication")) {
            return false;
        }
        if (index >= kWarmupSamples) {
            c_callback_duration.push_back(
                callback_return - strict_origin);
            c_strict_callback_to_consumer.push_back(
                seen_ns - strict_origin);
            c_wire_recv_to_consumer.push_back(seen_ns - recv);
            c_successful_read_call.push_back(successful_call_ns);
            c_strict_callback_to_ipc_begin.push_back(
                ipc_begin - strict_origin);
            c_strict_callback_to_ipc_return.push_back(
                ipc_end - strict_origin);
            c_wire_recv_to_ipc_begin.push_back(ipc_begin - recv);
            c_wire_recv_to_ipc_return.push_back(ipc_end - recv);
            c_ipc_call.push_back(ipc_end - ipc_begin);
            c_poll_count += polls;
            c_inconsistent_retries += inconsistent_retries;
            c_max_polls = std::max(c_max_polls, polls);
        }
        const std::size_t completed_samples = index + 1U;
        if ((completed_samples %
                 kClosedLoopAppliedBarrierRecords ==
             0U) ||
            completed_samples ==
                kWarmupSamples + kMeasuredSamples) {
            if (!Expect(
                    wait_pipeline_prefix(expected),
                    "C closed-loop applied barrier")) {
                return false;
            }
        }
    }
    PrintLatency("c_callback_call_duration", c_callback_duration);
    PrintLatency(
        "c_strict_callback_origin_to_latest_return",
        c_strict_callback_to_consumer);
    PrintLatency(
        "c_wire_recv_monotonic_to_latest_return",
        c_wire_recv_to_consumer);
    PrintLatency(
        "c_successful_latest_call",
        c_successful_read_call);
    PrintLatency(
        "c_strict_callback_origin_to_ipc_begin",
        c_strict_callback_to_ipc_begin);
    PrintLatency(
        "c_strict_callback_origin_to_ipc_return",
        c_strict_callback_to_ipc_return);
    PrintLatency(
        "c_wire_recv_monotonic_to_ipc_begin",
        c_wire_recv_to_ipc_begin);
    PrintLatency(
        "c_wire_recv_monotonic_to_ipc_return",
        c_wire_recv_to_ipc_return);
    PrintLatency("c_ipc_publish_call", c_ipc_call);
    std::cout
        << "POLL consumer=c samples=" << kMeasuredSamples
        << " total_polls=" << c_poll_count
        << " inconsistent_retries=" << c_inconsistent_retries
        << " mean_polls="
        << std::fixed << std::setprecision(3)
        << static_cast<long double>(c_poll_count) /
               static_cast<long double>(kMeasuredSamples)
        << " max_polls=" << c_max_polls << std::defaultfloat
        << '\n';

    PythonLatencyProcess python;
    if (!Expect(
            SpawnPythonLatencyProbe(
                socket_path,
                1U,
                kActiveInstrumentCount,
                kWarmupSamples,
                kMeasuredSamples,
                &python) &&
                python.channel() != nullptr,
            "spawn Python latency benchmark probe")) {
        return false;
    }
    ProtocolChannel* const protocol = python.channel();
    std::string line;
    for (;;) {
        if (!Expect(
                protocol->ReadLine(
                    std::chrono::seconds(30), &line),
                "read Python HOT/READY line")) {
            return false;
        }
        if (line.starts_with("ENV ") ||
            line.starts_with("CONNECT ") ||
            line.starts_with("HEAD_PUBLIC ") ||
            line.starts_with("TAIL_PUBLIC ") ||
            line.starts_with("PYTHON_NATIVE_WRAPPER ") ||
            line.starts_with("PUBLIC_STALENESS_DISABLED ") ||
            line.starts_with("WORKING_SET ") ||
            line.starts_with("DISTINCT_BATCH ")) {
            std::cout << "PYTHON_" << line << '\n';
            continue;
        }
        if (line.starts_with("READY")) {
            std::cout << "PYTHON_" << line << '\n';
            break;
        }
        std::cerr << "unexpected Python line: " << line << '\n';
        return false;
    }

    std::vector<std::uint64_t> python_callback_duration;
    std::vector<std::uint64_t>
        python_strict_callback_to_consumer;
    std::vector<std::uint64_t> python_wire_recv_to_consumer;
    std::vector<std::uint64_t>
        python_strict_callback_to_ipc_begin;
    std::vector<std::uint64_t>
        python_strict_callback_to_ipc_return;
    std::vector<std::uint64_t>
        python_wire_recv_to_ipc_begin;
    std::vector<std::uint64_t>
        python_wire_recv_to_ipc_return;
    std::vector<std::uint64_t> python_ipc_call;
    std::uint64_t python_poll_count = 0U;
    std::uint64_t python_inconsistent_retries = 0U;
    std::uint64_t python_max_polls = 0U;
    for (std::size_t index = 0U;
         index < kWarmupSamples + kMeasuredSamples;
         ++index, ++next_sequence) {
        const std::uint64_t expected = next_sequence;
        if (!Expect(
                protocol->SendLine(
                    "EXPECT " + std::to_string(expected)),
                "send Python EXPECT")) {
            return false;
        }
        if (!Expect(
                protocol->ReadLine(
                    std::chrono::seconds(5), &line),
                "read Python ARMED")) {
            return false;
        }
        std::istringstream armed(line);
        std::string armed_tag;
        std::uint64_t armed_sequence = 0U;
        armed >> armed_tag >> armed_sequence;
        if (!Expect(
                armed && armed_tag == "ARMED" &&
                    armed_sequence == expected,
                "validate Python ARMED")) {
            std::cerr << "protocol line: " << line << '\n';
            return false;
        }

        const std::uint64_t strict_origin = MonotonicNowNs();
        handler->OnMessage(nullptr, &snapshot);
        const std::uint64_t callback_return = MonotonicNowNs();
        if (!Expect(
                strict_origin != 0U &&
                    callback_return >= strict_origin,
                "measure Python-phase callback invocation")) {
            return false;
        }
        if (!Expect(
                protocol->ReadLine(
                    std::chrono::seconds(5), &line),
                "read Python SEEN")) {
            return false;
        }
        std::istringstream seen(line);
        std::string seen_tag;
        std::uint64_t seen_sequence = 0U;
        std::uint64_t seen_ns = 0U;
        std::uint64_t wire_recv_ns = 0U;
        std::uint64_t polls = 0U;
        std::uint64_t inconsistent_retries = 0U;
        seen >> seen_tag >> seen_sequence >> seen_ns >>
            wire_recv_ns >> polls >> inconsistent_retries;
        if (!Expect(
                seen && seen_tag == "SEEN" &&
                    seen_sequence == expected &&
                    seen_ns >= wire_recv_ns &&
                    wire_recv_ns >= strict_origin &&
                    polls != 0U,
                "validate Python SEEN")) {
            std::cerr << "protocol line: " << line << '\n';
            return false;
        }
        std::uint64_t recv = 0U;
        std::uint64_t ipc_begin = 0U;
        std::uint64_t ipc_end = 0U;
        if (!Expect(
                wait_publication(
                    expected, &recv, &ipc_begin, &ipc_end) &&
                    recv == wire_recv_ns &&
                    ipc_begin >= strict_origin &&
                    ipc_end >= ipc_begin,
                "correlate Python observation with IPC publication")) {
            return false;
        }
        if (index >= kWarmupSamples) {
            python_callback_duration.push_back(
                callback_return - strict_origin);
            python_strict_callback_to_consumer.push_back(
                seen_ns - strict_origin);
            python_wire_recv_to_consumer.push_back(
                seen_ns - wire_recv_ns);
            python_strict_callback_to_ipc_begin.push_back(
                ipc_begin - strict_origin);
            python_strict_callback_to_ipc_return.push_back(
                ipc_end - strict_origin);
            python_wire_recv_to_ipc_begin.push_back(
                ipc_begin - wire_recv_ns);
            python_wire_recv_to_ipc_return.push_back(
                ipc_end - wire_recv_ns);
            python_ipc_call.push_back(ipc_end - ipc_begin);
            python_poll_count += polls;
            python_inconsistent_retries +=
                inconsistent_retries;
            python_max_polls =
                std::max(python_max_polls, polls);
        }
        const std::size_t completed_samples = index + 1U;
        if ((completed_samples %
                 kClosedLoopAppliedBarrierRecords ==
             0U) ||
            completed_samples ==
                kWarmupSamples + kMeasuredSamples) {
            if (!Expect(
                    wait_pipeline_prefix(expected),
                    "Python closed-loop applied barrier")) {
                return false;
            }
        }
    }
    PrintLatency(
        "python_callback_call_duration",
        python_callback_duration);
    PrintLatency(
        "python_strict_callback_origin_to_latest_return",
        python_strict_callback_to_consumer);
    PrintLatency(
        "python_wire_recv_monotonic_to_latest_return",
        python_wire_recv_to_consumer);
    PrintLatency(
        "python_strict_callback_origin_to_ipc_begin",
        python_strict_callback_to_ipc_begin);
    PrintLatency(
        "python_strict_callback_origin_to_ipc_return",
        python_strict_callback_to_ipc_return);
    PrintLatency(
        "python_wire_recv_monotonic_to_ipc_begin",
        python_wire_recv_to_ipc_begin);
    PrintLatency(
        "python_wire_recv_monotonic_to_ipc_return",
        python_wire_recv_to_ipc_return);
    PrintLatency("python_ipc_publish_call", python_ipc_call);
    std::cout
        << "POLL consumer=python samples=" << kMeasuredSamples
        << " total_polls=" << python_poll_count
        << " inconsistent_retries="
        << python_inconsistent_retries
        << " mean_polls="
        << std::fixed << std::setprecision(3)
        << static_cast<long double>(python_poll_count) /
               static_cast<long double>(kMeasuredSamples)
        << " max_polls=" << python_max_polls
        << std::defaultfloat << '\n';

    const std::uint64_t burst_first = next_sequence;
    if (!Expect(
            protocol->SendLine(
                "BURST " + std::to_string(burst_first) + " " +
                std::to_string(kBurstSamples)),
            "send Python BURST")) {
        return false;
    }
    if (!Expect(
            protocol->ReadLine(
                std::chrono::seconds(5), &line),
            "read Python BURST_ARMED")) {
        return false;
    }
    std::istringstream burst_armed(line);
    std::string burst_armed_tag;
    std::uint64_t armed_first = 0U;
    std::size_t armed_count = 0U;
    burst_armed >> burst_armed_tag >> armed_first >> armed_count;
    if (!Expect(
            burst_armed && burst_armed_tag == "BURST_ARMED" &&
                armed_first == burst_first &&
                armed_count == kBurstSamples,
            "validate Python BURST_ARMED")) {
        std::cerr << "protocol line: " << line << '\n';
        return false;
    }
    std::vector<std::uint64_t> burst_strict_origins;
    burst_strict_origins.reserve(kBurstSamples);
    const std::uint64_t burst_start = MonotonicNowNs();
    for (std::size_t index = 0U;
         index < kBurstSamples;
         ++index) {
        const std::uint64_t strict_origin = MonotonicNowNs();
        if (strict_origin == 0U) {
            return Expect(
                false,
                "capture strict burst callback origin");
        }
        burst_strict_origins.push_back(strict_origin);
        handler->OnMessage(nullptr, &snapshot);
    }
    const std::uint64_t burst_end = MonotonicNowNs();
    const std::uint64_t burst_last =
        burst_first + kBurstSamples - 1U;
    next_sequence = burst_last + 1U;
    const runtime::RealtimePipelineSnapshotV1 after_burst_admission =
        pipeline->Snapshot();
    if (!Expect(
            burst_start != 0U && burst_end > burst_start,
            "measure burst producer duration")) {
        return false;
    }
    if (!Expect(
            protocol->ReadLine(
                std::chrono::seconds(30), &line) &&
                line.starts_with("BURST_RESULT "),
            "read Python BURST_RESULT")) {
        std::cerr << "protocol line: " << line << '\n';
        return false;
    }
    std::cout << "PYTHON_" << line << '\n';
    if (!Expect(
            protocol->ReadLine(
                std::chrono::seconds(5), &line) &&
                line == "DONE",
            "read Python DONE")) {
        return false;
    }
    if (!Expect(
            python.Wait(std::chrono::seconds(10)),
            "Python latency benchmark exits cleanly")) {
        return false;
    }

    std::uint64_t last_recv = 0U;
    std::uint64_t last_begin = 0U;
    std::uint64_t last_end = 0U;
    if (!Expect(
            wait_publication(
                burst_last,
                &last_recv,
                &last_begin,
                &last_end),
            "burst reaches the IPC sink")) {
        return false;
    }
    std::vector<std::uint64_t>
        burst_strict_callback_to_ipc_begin;
    std::vector<std::uint64_t>
        burst_strict_callback_to_ipc_return;
    std::vector<std::uint64_t>
        burst_wire_recv_to_ipc_begin;
    std::vector<std::uint64_t>
        burst_wire_recv_to_ipc_return;
    std::vector<std::uint64_t> burst_ipc_call;
    burst_strict_callback_to_ipc_begin.reserve(kBurstSamples);
    burst_strict_callback_to_ipc_return.reserve(kBurstSamples);
    burst_wire_recv_to_ipc_begin.reserve(kBurstSamples);
    burst_wire_recv_to_ipc_return.reserve(kBurstSamples);
    burst_ipc_call.reserve(kBurstSamples);
    for (std::uint64_t sequence = burst_first;
         sequence <= burst_last;
         ++sequence) {
        std::uint64_t recv = 0U;
        std::uint64_t ipc_begin = 0U;
        std::uint64_t ipc_end = 0U;
        if (!timed_sink->Read(
                sequence, &recv, &ipc_begin, &ipc_end)) {
            return Expect(
                false,
                "burst publication timing is incomplete");
        }
        const std::size_t origin_index =
            static_cast<std::size_t>(sequence - burst_first);
        const std::uint64_t strict_origin =
            burst_strict_origins[origin_index];
        if (!Expect(
                recv >= strict_origin &&
                    ipc_begin >= recv && ipc_end >= ipc_begin,
                "correlate strict burst callback origin")) {
            return false;
        }
        burst_strict_callback_to_ipc_begin.push_back(
            ipc_begin - strict_origin);
        burst_strict_callback_to_ipc_return.push_back(
            ipc_end - strict_origin);
        burst_wire_recv_to_ipc_begin.push_back(ipc_begin - recv);
        burst_wire_recv_to_ipc_return.push_back(ipc_end - recv);
        burst_ipc_call.push_back(ipc_end - ipc_begin);
    }
    const std::uint64_t burst_duration = burst_end - burst_start;
    const long double burst_rate =
        static_cast<long double>(kBurstSamples) *
        1'000'000'000.0L /
        static_cast<long double>(burst_duration);
    std::cout
        << "BURST produced=" << kBurstSamples
        << " first_sequence=" << burst_first
        << " last_sequence=" << burst_last
        << " producer_duration_ns=" << burst_duration
        << " producer_records_per_second="
        << std::fixed << std::setprecision(3) << burst_rate
        << std::defaultfloat << '\n';
    std::cout
        << "PROGRESS phase=after_burst_admission accepted="
        << after_burst_admission.processing_progress.accepted_sequence
        << " applied="
        << after_burst_admission.processing_progress.applied_sequence
        << " accepted_minus_applied="
        << after_burst_admission.processing_progress
               .processing_lag_records()
        << '\n';
    for (std::size_t source = 0U;
         source < market::kRealtimeHistorySourceCountV1;
         ++source) {
        const runtime::RealtimeDecoderQueueSnapshotV1& queue =
            after_burst_admission.decoder_queues[source];
        std::cout
            << "DECODER_QUEUE phase=after_burst_admission lane="
            << DecoderLaneName(source)
            << " message_depth=" << queue.message_depth
            << " message_high_water=" << queue.message_high_water
            << " full_count=" << queue.full_count
            << '\n';
    }
    PrintLatency(
        "burst_strict_callback_origin_to_ipc_begin",
        burst_strict_callback_to_ipc_begin);
    PrintLatency(
        "burst_strict_callback_origin_to_ipc_return",
        burst_strict_callback_to_ipc_return);
    PrintLatency(
        "burst_wire_recv_monotonic_to_ipc_begin",
        burst_wire_recv_to_ipc_begin);
    PrintLatency(
        "burst_wire_recv_monotonic_to_ipc_return",
        burst_wire_recv_to_ipc_return);
    PrintLatency("burst_ipc_publish_call", burst_ipc_call);

    pipeline->StopAndDrain();
    const runtime::RealtimePipelineStageLatencySnapshotV1 stages =
        pipeline->LatencySnapshot();
    if (measure_stage_latency) {
        PrintStageLatency(
            "callback_entry_to_success",
            stages.callback_entry_to_success);
        PrintStageLatency(
            "callback_entry_to_store_append",
            stages.callback_entry_to_append_complete);
        PrintStageLatency(
            "callback_entry_to_inprocess_latest",
            stages.callback_entry_to_inprocess_latest_read);
        PrintStageLatency("store_append_call", stages.append_call);
        for (std::size_t source = 0U;
             source < market::kRealtimeHistorySourceCountV1;
             ++source) {
            const std::string lane{DecoderLaneName(source)};
            PrintStageLatency(
                "callback_to_decoder_publish{" + lane + "}",
                stages.callback_to_decoder_publish[source]);
            PrintStageLatency(
                "decoder_queue_dwell{" + lane + "}",
                stages.decoder_queue_dwell[source]);
            PrintStageLatency(
                "decode_duration{" + lane + "}",
                stages.decode_duration[source]);
            PrintStageLatency(
                "decode_to_history_submit{" + lane + "}",
                stages.decode_to_history_submit[source]);
            PrintStageLatency(
                "decode_to_applied{" + lane + "}",
                stages.decode_to_applied[source]);
        }
        PrintStageLatency(
            "callback_to_store_applied",
            stages.callback_to_store_applied);
        PrintStageLatency(
            "callback_to_ipc_visible",
            stages.callback_to_ipc_visible);
    }
    std::cout
        << "STAGE_COUNTS enabled=" << (stages.enabled ? 1 : 0)
        << " stage_scope=whole_run_including_setup"
        << " callback_samples="
        << stages.callback_entry_to_success.samples
        << " append_samples="
        << stages.callback_entry_to_append_complete.samples
        << '\n';

    const runtime::RealtimePipelineSnapshotV1 final_state =
        pipeline->Snapshot();
    std::cout
        << "PROGRESS phase=final accepted="
        << final_state.processing_progress.accepted_sequence
        << " applied="
        << final_state.processing_progress.applied_sequence
        << " accepted_minus_applied="
        << final_state.processing_progress.processing_lag_records()
        << '\n';
    for (std::size_t source = 0U;
         source < market::kRealtimeHistorySourceCountV1;
         ++source) {
        const runtime::RealtimeDecoderQueueSnapshotV1& queue =
            final_state.decoder_queues[source];
        std::cout
            << "DECODER_QUEUE phase=final lane="
            << DecoderLaneName(source)
            << " message_depth=" << queue.message_depth
            << " message_high_water=" << queue.message_high_water
            << " full_count=" << queue.full_count
            << '\n';
    }
    const auto metric_attempts = [](
        const runtime::RealtimeLatencyDistributionV1& distribution) {
        return distribution.samples + distribution.invalid_samples;
    };
    std::uint64_t decoder_publish_attempts = 0U;
    std::uint64_t queue_dwell_attempts = 0U;
    std::uint64_t decode_attempts = 0U;
    std::uint64_t history_submit_attempts = 0U;
    std::uint64_t decode_to_applied_attempts = 0U;
    for (std::size_t source = 0U;
         source < market::kRealtimeHistorySourceCountV1;
         ++source) {
        decoder_publish_attempts += metric_attempts(
            stages.callback_to_decoder_publish[source]);
        queue_dwell_attempts += metric_attempts(
            stages.decoder_queue_dwell[source]);
        decode_attempts += metric_attempts(
            stages.decode_duration[source]);
        history_submit_attempts += metric_attempts(
            stages.decode_to_history_submit[source]);
        decode_to_applied_attempts += metric_attempts(
            stages.decode_to_applied[source]);
    }
    const bool stage_metric_counts_match =
        !measure_stage_latency ||
        (decoder_publish_attempts == burst_last &&
         queue_dwell_attempts == burst_last &&
         decode_attempts == burst_last &&
         history_submit_attempts == burst_last &&
         decode_to_applied_attempts == burst_last &&
         metric_attempts(stages.callback_to_store_applied) ==
             burst_last &&
         metric_attempts(stages.callback_to_ipc_visible) ==
             burst_last);
    bool ok = Expect(
        stages.enabled == measure_stage_latency &&
            stage_metric_counts_match &&
            !pipeline->fatal() &&
            !final_state.fatal &&
            final_state.accepted_messages == burst_last &&
            final_state.processing_progress.accepted_sequence ==
                burst_last &&
            final_state.processing_progress.applied_sequence ==
                burst_last &&
            !service->failed(),
        "latency benchmark retains the complete prefix and stage metrics");

    l2flow_shm_session_info_v2 final_session{};
    bool wire_matches_final_prefix = false;
    const auto final_session_deadline =
        std::chrono::steady_clock::now() +
        std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() <
           final_session_deadline) {
        const int session_error =
            l2flow_shm_reader_session_v2(
                reader.get(), &final_session);
        if (session_error ==
                L2FLOW_SHM_READER_INCONSISTENT_READ_V2) {
            std::this_thread::yield();
            continue;
        }
        if (session_error != L2FLOW_SHM_READER_OK_V2) {
            break;
        }
        wire_matches_final_prefix =
            final_session.server_state ==
                static_cast<std::uint32_t>(
                    ipc::RealtimeServerStateV2::kActive) &&
            final_session.catalog_scope ==
                static_cast<std::uint32_t>(
                    ipc::RealtimeCatalogScopeV2::
                        kDeclaredDailyAShare) &&
            final_session.coverage_complete == 1U &&
            final_session.capacity == kCapacity &&
            final_session.catalog_generation == 1U &&
            final_session.catalog_trade_date == kTradeDate &&
            final_session.catalog_version == 47U &&
            final_session.bound_count ==
                kActiveInstrumentCount &&
            final_session.available_count ==
                kActiveInstrumentCount &&
            final_session.snapshot_available_count ==
                kActiveInstrumentCount &&
            final_session.tick_available_count == 0U &&
            final_session.factor_eligible_count ==
                kActiveInstrumentCount &&
            final_session.accepted_sequence == burst_last &&
            final_session.applied_sequence == burst_last &&
            final_session.processing_lag_records == 0U;
        if (wire_matches_final_prefix) {
            break;
        }
        std::this_thread::yield();
    }
    ok &= Expect(
        wire_matches_final_prefix,
        "Wire session publishes the complete benchmark prefix");

    service->MarkDraining();
    ok &= Expect(
        service->MarkStoppedClean(0U),
        "latency benchmark stops with no tick gap");
    service->StopControl();
    return ok;
}

enum class StartupBenchmarkScenarioV1 : std::uint8_t {
    kFromOpen = 0U,
    kLivePartialNoRecovery,
};

[[nodiscard]] std::string_view StartupBenchmarkScenarioNameV1(
    StartupBenchmarkScenarioV1 scenario) noexcept {
    switch (scenario) {
        case StartupBenchmarkScenarioV1::kFromOpen:
            return "from_open";
        case StartupBenchmarkScenarioV1::kLivePartialNoRecovery:
            return "live_partial_no_recovery";
    }
    return "unknown";
}

[[nodiscard]] bool StartupBenchmarkCoverageFromOpenV1(
    StartupBenchmarkScenarioV1 scenario) noexcept {
    return scenario == StartupBenchmarkScenarioV1::kFromOpen;
}

[[nodiscard]] bool StartStartupBenchmarkServiceV1(
    const std::shared_ptr<ipc::RealtimeSharedMarketServiceV2>& service,
    StartupBenchmarkScenarioV1 scenario,
    int* system_error) noexcept {
    if (service == nullptr) {
        return false;
    }
    if (scenario == StartupBenchmarkScenarioV1::kFromOpen) {
        return service->Start(system_error);
    }
    return service->StartLivePartialWithProcessStartHistory(
        system_error);
}

enum class ThroughputWorkloadV1 : std::uint8_t {
    kSingleInstrument = 0U,
    kFiveTupleUniform,
    kFourSourceBalanced,
    kHotShenzhenTickSource,
};

enum class ThroughputSinkV1 : std::uint8_t {
    kFast = 0U,
    kFastAndCertified,
};

struct ThroughputBenchmarkConfigV1 final {
    std::uint64_t target_rate = 0U;
    std::chrono::milliseconds duration{0};
    std::size_t instruments_per_market = 0U;
    std::uint32_t store_worker_count = 0U;
    std::uint32_t parallel_decoder_worker_count = 0U;
    bool parallel_decoder_idle_inline_enabled = true;
    std::size_t decoder_queue_capacity_per_source = 0U;
    std::size_t store_queue_capacity_per_source_worker = 0U;
    std::uint32_t segment_kib = 0U;
    ThroughputWorkloadV1 workload =
        ThroughputWorkloadV1::kSingleInstrument;
    ThroughputSinkV1 sink = ThroughputSinkV1::kFast;
    StartupBenchmarkScenarioV1 scenario =
        StartupBenchmarkScenarioV1::kFromOpen;
    std::chrono::milliseconds generation_interval{0};
};

[[nodiscard]] std::string_view ThroughputWorkloadNameV1(
    ThroughputWorkloadV1 workload) noexcept {
    switch (workload) {
        case ThroughputWorkloadV1::kSingleInstrument:
            return "single_instrument";
        case ThroughputWorkloadV1::kFiveTupleUniform:
            return "five_tuple_uniform";
        case ThroughputWorkloadV1::kFourSourceBalanced:
            return "four_source_balanced";
        case ThroughputWorkloadV1::kHotShenzhenTickSource:
            return "hot_shenzhen_tick_source";
    }
    return "unknown";
}

[[nodiscard]] std::string_view ThroughputSinkNameV1(
    ThroughputSinkV1 sink) noexcept {
    switch (sink) {
        case ThroughputSinkV1::kFast:
            return "fast";
        case ThroughputSinkV1::kFastAndCertified:
            return "fast_certified";
    }
    return "unknown";
}

template <typename Config>
[[nodiscard]] bool SetParallelDecoderWorkerCountV1(
    Config* config,
    std::uint32_t worker_count) noexcept {
    if (config == nullptr) {
        return false;
    }
    if constexpr (requires(Config& value) {
                      value.parallel_decoder_worker_count = worker_count;
                  }) {
        config->parallel_decoder_worker_count = worker_count;
        return true;
    }
    return worker_count == 0U;
}

[[nodiscard]] std::size_t
EffectiveParallelDecoderFarmActivationDepthV1(
    std::size_t configured,
    std::size_t queue_capacity) noexcept {
    if (configured == 0U || queue_capacity == 0U) {
        return configured;
    }
    const std::size_t capacity_limited =
        queue_capacity -
        std::max<std::size_t>(1U, queue_capacity / 4U);
    return std::min(configured, capacity_limited);
}

struct ThroughputInstrumentMessagesV1 final {
    std::unique_ptr<FakeSdkMessage> shanghai_snapshot;
    std::unique_ptr<FakeSdkMessage> shanghai_tick;
    std::unique_ptr<FakeSdkMessage> shenzhen_snapshot;
    std::unique_ptr<FakeSdkMessage> shenzhen_order;
    std::unique_ptr<FakeSdkMessage> shenzhen_transaction;
    std::uint64_t last_shenzhen_order_sequence = 0U;
};

[[nodiscard]] bool MakeThroughputMessagesV1(
    std::size_t instruments_per_market,
    std::vector<ThroughputInstrumentMessagesV1>* output) {
    if (output == nullptr || instruments_per_market == 0U) {
        return false;
    }
    output->clear();
    try {
        output->reserve(instruments_per_market);
        for (std::size_t index = 0U;
             index < instruments_per_market;
             ++index) {
            const std::string shanghai_id = SixDigitSecurityId(
                static_cast<std::uint32_t>(600'001U + index));
            const std::string shenzhen_id = ShenzhenAShareSecurityId(
                static_cast<std::uint32_t>(index + 1U));
            if (shanghai_id.empty() || shenzhen_id.empty()) {
                output->clear();
                return false;
            }
            ThroughputInstrumentMessagesV1 messages{};
            messages.shanghai_snapshot =
                std::make_unique<FakeSdkMessage>(
                    sdk::kProductionMessageKeysV1[0U],
                    PipelineShanghaiSnapshotBody(shanghai_id));
            messages.shanghai_tick =
                std::make_unique<FakeSdkMessage>(
                    sdk::kProductionMessageKeysV1[1U],
                    PipelineShanghaiTickBody(shanghai_id));
            messages.shenzhen_snapshot =
                std::make_unique<FakeSdkMessage>(
                    sdk::kProductionMessageKeysV1[2U],
                    PipelineShenzhenSnapshotBody(shenzhen_id));
            messages.shenzhen_order =
                std::make_unique<FakeSdkMessage>(
                    sdk::kProductionMessageKeysV1[3U],
                    PipelineShenzhenOrderBody(shenzhen_id));
            messages.shenzhen_transaction =
                std::make_unique<FakeSdkMessage>(
                    sdk::kProductionMessageKeysV1[4U],
                    PipelineShenzhenTransactionBody(shenzhen_id));
            output->push_back(std::move(messages));
        }
    } catch (...) {
        output->clear();
        return false;
    }
    return output->size() == instruments_per_market;
}

[[nodiscard]] std::size_t ThroughputTupleIndexV1(
    ThroughputWorkloadV1 workload,
    std::uint64_t callback_index) noexcept {
    switch (workload) {
        case ThroughputWorkloadV1::kSingleInstrument:
            return 2U;
        case ThroughputWorkloadV1::kFiveTupleUniform:
            return static_cast<std::size_t>(callback_index % 5U);
        case ThroughputWorkloadV1::kFourSourceBalanced: {
            constexpr std::array<std::size_t, 8U> kCycle{
                0U, 1U, 2U, 3U, 0U, 1U, 2U, 4U};
            return kCycle[static_cast<std::size_t>(
                callback_index % kCycle.size())];
        }
        case ThroughputWorkloadV1::kHotShenzhenTickSource:
            return callback_index % 2U == 0U ? 3U : 4U;
    }
    return sdk::kProductionMessageCountV1;
}

[[nodiscard]] std::size_t ThroughputCycleSizeV1(
    ThroughputWorkloadV1 workload) noexcept {
    switch (workload) {
        case ThroughputWorkloadV1::kSingleInstrument:
            return 1U;
        case ThroughputWorkloadV1::kFiveTupleUniform:
            return 5U;
        case ThroughputWorkloadV1::kFourSourceBalanced:
            return 8U;
        case ThroughputWorkloadV1::kHotShenzhenTickSource:
            return 2U;
    }
    return 1U;
}

struct ThroughputHistoryValidationV1 final {
    std::uint64_t scan_elapsed_ns = 0U;
    std::uint64_t scanned_records = 0U;
    std::uint64_t unique_ingress_sequences = 0U;
    std::uint64_t duplicate_ingress_sequences = 0U;
    std::uint64_t out_of_range_ingress_sequences = 0U;
    std::uint64_t invalid_source_slots = 0U;
    std::array<std::uint64_t, 4U> scanned_source_records{};
    std::uint16_t history_control_status =
        std::numeric_limits<std::uint16_t>::max();
    std::uint32_t endpoint_flags = 0U;
    bool cursor_opened = false;
    bool explicit_eof = false;
    bool endpoint_valid = false;
    bool generation_valid = false;
    bool source_counts_valid = false;
    bool passed = false;
};

[[nodiscard]] ThroughputHistoryValidationV1
ValidateThroughputHistoryGenerationV1(
    const std::filesystem::path& socket_path,
    const std::shared_ptr<const market::
            IntradayInstrumentStoreGenerationV1>& generation,
    StartupBenchmarkScenarioV1 scenario,
    std::uint64_t planned,
    const std::array<std::uint64_t, 4U>& expected_source_records) {
    ThroughputHistoryValidationV1 result{};
    const std::uint64_t begin_ns = MonotonicNowNs();
    if (generation == nullptr || planned == 0U || begin_ns == 0U ||
        planned >= std::numeric_limits<std::uint64_t>::max()) {
        return result;
    }

    const bool coverage_from_open =
        StartupBenchmarkCoverageFromOpenV1(scenario);
    const market::RealtimeHistoryWatermarkV1& watermark =
        generation->watermark();
    result.generation_valid =
        generation->record_count() == planned &&
        generation->coverage_from_open() == coverage_from_open &&
        watermark.generation != 0U &&
        watermark.ingress_sequence_exclusive == planned + 1U &&
        watermark.processing_progress.valid() &&
        watermark.processing_progress.accepted_sequence == planned &&
        watermark.processing_progress.applied_sequence == planned;
    for (std::size_t source = 0U;
         source < expected_source_records.size();
         ++source) {
        result.generation_valid =
            result.generation_valid &&
            watermark.sources[source].sequence_exclusive ==
                expected_source_records[source] + 1U;
    }

    ipc::RealtimeHistoryOpenResponseV2 history{};
    if (RequestHistoryOpen(
            socket_path, watermark.generation, &history)) {
        result.history_control_status = history.status;
        result.endpoint_flags = history.generation.endpoint.flags;
        const std::uint32_t expected_flags =
            ipc::kRealtimeGenerationRecordCoverageCompleteV2 |
            (coverage_from_open
                 ? ipc::kRealtimeGenerationCoverageFromOpenV2
                 : 0U);
        result.endpoint_valid =
            history.status == static_cast<std::uint16_t>(
                                  ipc::RealtimeHistoryControlStatusV2::
                                      kOk) &&
            ipc::RealtimeHistoryGenerationInfoCanonicalV2(
                history.generation) &&
            history.generation.endpoint.generation ==
                watermark.generation &&
            history.generation.endpoint.flags == expected_flags &&
            history.generation.endpoint.ingress_sequence_exclusive ==
                planned + 1U;
        for (std::size_t source = 0U;
             source < expected_source_records.size();
             ++source) {
            result.endpoint_valid =
                result.endpoint_valid &&
                history.generation.endpoint
                        .source_sequence_exclusive[source] ==
                    expected_source_records[source] + 1U;
        }
    }

    market::IntradayInstrumentScanOptionsV1 options{};
    options.ingress_sequence_begin_inclusive = 1U;
    options.ingress_sequence_end_exclusive = planned + 1U;
    options.maximum_records = planned;
    options.direction =
        market::IntradayInstrumentScanDirectionV1::kOldestFirst;
    std::unique_ptr<market::IntradayUniverseCursorV1> cursor;
    result.cursor_opened =
        generation->OpenUniverseCursor(options, &cursor) ==
            market::IntradayInstrumentStoreQueryErrorV1::kNone &&
        cursor != nullptr;
    if (result.cursor_opened) {
        try {
            std::vector<std::uint8_t> seen(
                static_cast<std::size_t>(planned + 1U), 0U);
            std::vector<const market::RealtimeHistoryRecordV1*> batch(
                65'536U, nullptr);
            for (;;) {
                std::size_t written = 0U;
                const auto read_error = cursor->ReadBatch(
                    batch, &written);
                if (read_error !=
                    market::IntradayInstrumentStoreQueryErrorV1::kNone) {
                    break;
                }
                if (written == 0U) {
                    result.explicit_eof = cursor->done();
                    break;
                }
                for (std::size_t index = 0U;
                     index < written;
                     ++index) {
                    const market::RealtimeHistoryRecordV1* const record =
                        batch[index];
                    ++result.scanned_records;
                    if (record == nullptr ||
                        record->ingress_sequence() == 0U ||
                        record->ingress_sequence() > planned) {
                        ++result.out_of_range_ingress_sequences;
                        continue;
                    }
                    const std::size_t ingress =
                        static_cast<std::size_t>(
                            record->ingress_sequence());
                    if (seen[ingress] != 0U) {
                        ++result.duplicate_ingress_sequences;
                    } else {
                        seen[ingress] = 1U;
                        ++result.unique_ingress_sequences;
                    }
                    const std::size_t source = record->source_slot();
                    if (source >= result.scanned_source_records.size()) {
                        ++result.invalid_source_slots;
                    } else {
                        ++result.scanned_source_records[source];
                    }
                }
            }
        } catch (...) {
            result.explicit_eof = false;
        }
    }

    result.source_counts_valid =
        result.scanned_source_records == expected_source_records;
    result.passed =
        result.generation_valid && result.endpoint_valid &&
        result.cursor_opened && result.explicit_eof &&
        result.scanned_records == planned &&
        result.unique_ingress_sequences == planned &&
        result.duplicate_ingress_sequences == 0U &&
        result.out_of_range_ingress_sequences == 0U &&
        result.invalid_source_slots == 0U &&
        result.source_counts_valid;
    const std::uint64_t end_ns = MonotonicNowNs();
    if (end_ns >= begin_ns) {
        result.scan_elapsed_ns = end_ns - begin_ns;
    }
    return result;
}

bool RunThroughputProfileBenchmark(
    const ThroughputBenchmarkConfigV1& benchmark) {
    constexpr std::uint64_t kTickRingCapacity = 262'144U;
    constexpr std::uint64_t kCertifiedQueueCapacity = 4'194'304U;
    constexpr std::uint64_t kMaximumTargetRate = 1'000'000U;
    constexpr std::uint64_t kMaximumDurationMs = 10'000U;
    constexpr std::uint64_t kMaximumPlannedCallbacks = 5'000'000U;
    constexpr std::uint64_t kMaximumAccountedBytes =
        32ULL * 1024ULL * 1024ULL * 1024ULL;
    const std::uint64_t duration_ms =
        static_cast<std::uint64_t>(benchmark.duration.count());
    const bool factor_generation_enabled =
        benchmark.scenario == StartupBenchmarkScenarioV1::kFromOpen;
    if (benchmark.target_rate == 0U ||
        benchmark.target_rate > kMaximumTargetRate ||
        duration_ms == 0U || duration_ms > kMaximumDurationMs ||
        benchmark.instruments_per_market == 0U ||
        benchmark.instruments_per_market > 14'599U ||
        benchmark.store_worker_count == 0U ||
        benchmark.decoder_queue_capacity_per_source < 2U ||
        benchmark.store_queue_capacity_per_source_worker < 2U ||
        benchmark.segment_kib < 64U ||
        benchmark.generation_interval.count() < 0 ||
        (benchmark.scenario ==
             StartupBenchmarkScenarioV1::kLivePartialNoRecovery &&
         benchmark.sink != ThroughputSinkV1::kFast)) {
        std::cerr << "invalid throughput profile arguments\n";
        return false;
    }
    if (benchmark.target_rate >
        std::numeric_limits<std::uint64_t>::max() / duration_ms) {
        std::cerr << "throughput callback count overflow\n";
        return false;
    }
    const std::uint64_t planned =
        benchmark.target_rate * duration_ms / 1'000U;
    if (planned == 0U || planned > kMaximumPlannedCallbacks) {
        std::cerr << "throughput profile callback count out of range\n";
        return false;
    }
    if (benchmark.decoder_queue_capacity_per_source >
        std::numeric_limits<std::uint64_t>::max() - planned) {
        std::cerr << "throughput retained-record bound overflow\n";
        return false;
    }
    const std::uint64_t segment_bytes =
        static_cast<std::uint64_t>(benchmark.segment_kib) * 1'024U;
    if (segment_bytes >
        std::numeric_limits<std::uint32_t>::max()) {
        std::cerr << "throughput segment size is not representable\n";
        return false;
    }

    std::cout
        << "THROUGHPUT_ENV target_rps=" << benchmark.target_rate
        << " duration_ms=" << duration_ms
        << " planned_callbacks=" << planned
        << " callback_contract=serialized"
        << " scenario="
        << StartupBenchmarkScenarioNameV1(benchmark.scenario)
        << " server_state="
        << (benchmark.scenario ==
                    StartupBenchmarkScenarioV1::kFromOpen
                ? "ACTIVE"
                : "LIVE_PARTIAL")
        << " coverage_from_open="
        << (StartupBenchmarkCoverageFromOpenV1(benchmark.scenario)
                ? 1
                : 0)
        << " online_recovery=0"
        << " factor_generation_enabled="
        << (factor_generation_enabled ? 1 : 0)
        << " generation_interval_ms="
        << benchmark.generation_interval.count()
        << " native_sequence_base="
        << (benchmark.scenario ==
                    StartupBenchmarkScenarioV1::kFromOpen
                ? 0U
                : 10'000'000U)
        << " workload=" << ThroughputWorkloadNameV1(benchmark.workload)
        << " instruments_per_market="
        << benchmark.instruments_per_market
        << " production_tuple_count="
        << sdk::kProductionMessageCountV1
        << " parallel_decoder_workers="
        << benchmark.parallel_decoder_worker_count
        << " parallel_idle_inline="
        << (benchmark.parallel_decoder_idle_inline_enabled ? 1 : 0)
        << " parallel_farm_activation_configured="
        << runtime::RealtimePipelineConfigV1{}
               .parallel_decoder_farm_activation_queue_depth
        << " parallel_farm_activation_effective="
        << EffectiveParallelDecoderFarmActivationDepthV1(
               runtime::RealtimePipelineConfigV1{}
                   .parallel_decoder_farm_activation_queue_depth,
               benchmark.decoder_queue_capacity_per_source)
        << " decoder_queue_capacity_per_source="
        << benchmark.decoder_queue_capacity_per_source
        << " store_queue_capacity_per_source_worker="
        << benchmark.store_queue_capacity_per_source_worker
        << " store_worker_count=" << benchmark.store_worker_count
        << " store_segment_kib=" << benchmark.segment_kib
        << " tick_ring_capacity=" << kTickRingCapacity
        << " sink=" << ThroughputSinkNameV1(benchmark.sink)
        << " pacing=absolute_deadline_one_based_no_batch_wait"
        << " clock=CLOCK_MONOTONIC"
        << " affinity=" << CpuAffinityText() << '\n';

    ScopedTempDirectory temporary;
    DailyRuntimeFixture fixture = MakeThroughputBenchmarkDailyFixture(
        benchmark.instruments_per_market, 82U);
    std::vector<ThroughputInstrumentMessagesV1> messages;
    if (!Expect(
            temporary.valid() && static_cast<bool>(fixture) &&
                MakeThroughputMessagesV1(
                    benchmark.instruments_per_market, &messages),
            "create throughput profile fixture")) {
        return false;
    }
    const common::Identity128 run_id = RunId(0x82U);
    const std::filesystem::path socket_path =
        temporary.path() / "throughput-profile-fast.sock";

    ipc::RealtimeSharedServiceConfigV2 service_config{};
    service_config.run_id = run_id;
    service_config.session_epoch = 82U;
    service_config.trade_date = kTradeDate;
    service_config.daily_catalog = fixture.catalog;
    service_config.coverage_from_open =
        StartupBenchmarkCoverageFromOpenV1(benchmark.scenario);
    service_config.tick_ring_capacity = kTickRingCapacity;
    service_config.maximum_history_readers = 4U;
    service_config.maximum_history_page_records = 65'536U;
    service_config.control_socket_path = socket_path;
    std::shared_ptr<ipc::RealtimeSharedMarketServiceV2> service;
    int system_error = 0;
    const auto service_error =
        ipc::RealtimeSharedMarketServiceV2::Create(
            service_config, &service, &system_error);
    if (service_error !=
            ipc::RealtimeSharedServiceCreateErrorV2::kNone ||
        service == nullptr || system_error != 0) {
        std::cerr
            << "throughput profile FAST create error="
            << static_cast<unsigned int>(service_error)
            << " system_error=" << system_error << '\n';
    }
    if (!Expect(
            service_error ==
                    ipc::RealtimeSharedServiceCreateErrorV2::kNone &&
                service != nullptr && system_error == 0,
            "create throughput profile FAST service") ||
        !Expect(
            StartStartupBenchmarkServiceV1(
                service, benchmark.scenario, &system_error) &&
                system_error == 0,
            "start throughput profile FAST service")) {
        if (service != nullptr) {
            service->StopControl();
        }
        return false;
    }

    std::shared_ptr<ipc::RealtimeCertifiedMarketServiceV1>
        certified_service;
    if (benchmark.sink == ThroughputSinkV1::kFastAndCertified) {
        ipc::RealtimeCertifiedServiceConfigV1 certified_config{};
        certified_config.run_id = run_id;
        certified_config.session_epoch = 82U;
        certified_config.trade_date = kTradeDate;
        certified_config.daily_catalog = fixture.catalog;
        certified_config.fast_sink = service;
        certified_config.certified_tick_ring_capacity =
            kTickRingCapacity;
        certified_config.handoff_queue_capacity =
            kCertifiedQueueCapacity;
        certified_config.maximum_mapping_bytes =
            2ULL * 1024ULL * 1024ULL * 1024ULL;
        if (planned >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max() / 4U)) {
            service->MarkFailed();
            service->StopControl();
            return false;
        }
        certified_config.maximum_order_states =
            static_cast<std::size_t>(planned);
        certified_config.maximum_derived_events =
            static_cast<std::size_t>(planned * 4U);
        certified_config.control_socket_path =
            temporary.path() / "throughput-profile-certified.sock";
        const auto certified_error =
            ipc::RealtimeCertifiedMarketServiceV1::Create(
                std::move(certified_config),
                &certified_service,
                &system_error);
        if (!Expect(
                certified_error ==
                        ipc::RealtimeCertifiedServiceCreateErrorV1::
                            kNone &&
                    certified_service != nullptr &&
                    certified_service->Start(&system_error),
                "create/start throughput CERTIFIED service")) {
            if (certified_service != nullptr) {
                certified_service->StopControl();
            }
            service->MarkFailed();
            service->StopControl();
            return false;
        }
    }

    auto sdk_state = std::make_shared<LatencySdkState>();
    auto sdk_factory =
        std::make_shared<LatencySdkFactory>(sdk_state);
    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    PipelineCleanup pipeline_cleanup(&pipeline);
    runtime::RealtimePipelineConfigV1 pipeline_config{};
    pipeline_config.run_id = run_id;
    pipeline_config.trade_date = kTradeDate;
    pipeline_config.daily_catalog = fixture.catalog;
    pipeline_config.runtime_state = fixture.runtime_state.get();
    pipeline_config.source_stream_ids = kSourceStreamIds;
    pipeline_config.maximum_sdk_message_bytes = 4'096U;
    pipeline_config.decoder_queue_capacity_per_source =
        benchmark.decoder_queue_capacity_per_source;
    pipeline_config.tick_ring_capacity = kTickRingCapacity;
    pipeline_config.store_worker_count =
        benchmark.store_worker_count;
    pipeline_config.store_queue_capacity_per_source_worker =
        benchmark.store_queue_capacity_per_source_worker;
    pipeline_config.intraday_store.segment_target_bytes =
        static_cast<std::uint32_t>(segment_bytes);
    pipeline_config.intraday_store.maximum_session_records =
        planned + benchmark.decoder_queue_capacity_per_source;
    pipeline_config.intraday_store.maximum_session_accounted_bytes =
        kMaximumAccountedBytes;
    pipeline_config.intraday_store.maximum_records_per_batch =
        65'536U;
    pipeline_config.intraday_store.coverage_from_open =
        StartupBenchmarkCoverageFromOpenV1(benchmark.scenario);
    pipeline_config.factor_generation_enabled =
        factor_generation_enabled;
    pipeline_config.applied_record_sink =
        certified_service != nullptr
            ? std::static_pointer_cast<
                  market::RealtimeAppliedRecordSinkV1>(
                  certified_service)
            : std::static_pointer_cast<
                  market::RealtimeAppliedRecordSinkV1>(service);
    if (certified_service != nullptr) {
        pipeline_config.native_sequence_observation_sink =
            certified_service;
    }
    pipeline_config.processing_progress_sink = service;
    pipeline_config.store_generation_sink = service;
    pipeline_config.sdk.enabled = true;
    pipeline_config.sdk.server_address = "throughput.invalid";
    pipeline_config.sdk.user_name = "throughput";
    pipeline_config.parallel_decoder_idle_inline_enabled =
        benchmark.parallel_decoder_idle_inline_enabled;
    if (!SetParallelDecoderWorkerCountV1(
            &pipeline_config,
            benchmark.parallel_decoder_worker_count)) {
        std::cerr
            << "parallel decoder worker configuration is unavailable\n";
        if (certified_service != nullptr) {
            certified_service->StopControl();
        }
        service->MarkFailed();
        service->StopControl();
        return false;
    }

    std::string pipeline_detail;
    const auto pipeline_error =
        runtime::RealtimePipelineV1::CreateForTest(
            pipeline_config,
            sdk_factory,
            &pipeline,
            &pipeline_detail);
    if (!Expect(
            pipeline_error ==
                    runtime::RealtimePipelineCreateErrorV1::kNone &&
                pipeline != nullptr,
            "create throughput profile Pipeline: " + pipeline_detail)) {
        if (certified_service != nullptr) {
            certified_service->StopControl();
        }
        service->MarkFailed();
        service->StopControl();
        return false;
    }
    mdl::MessageHandlerBase* const handler = sdk_state->handler();
    if (!Expect(
            handler != nullptr,
            "throughput profile SDK callback installed")) {
        if (certified_service != nullptr) {
            certified_service->StopControl();
        }
        service->MarkFailed();
        service->StopControl();
        return false;
    }

    std::array<std::uint64_t, 5U> offered_by_tuple{};
    std::array<std::uint64_t, 4U> backlog_at_quarter{};
    const std::uint64_t native_sequence_base =
        benchmark.scenario ==
                StartupBenchmarkScenarioV1::kFromOpen
            ? 0U
            : 10'000'000U;
    std::uint64_t shanghai_business_sequence = native_sequence_base;
    std::uint64_t shenzhen_native_sequence = native_sequence_base;
    const std::size_t cycle_size =
        ThroughputCycleSizeV1(benchmark.workload);

    std::mutex generation_mutex;
    std::condition_variable generation_cv;
    bool stop_generation_thread = false;
    std::atomic<std::uint64_t> periodic_generation_cuts{0U};
    std::atomic<bool> periodic_generation_failed{false};
    std::atomic<std::uint32_t> periodic_cut_error{0U};
    std::atomic<std::uint32_t> periodic_generation_error{0U};
    std::thread generation_thread;
    if (benchmark.generation_interval.count() > 0) {
        try {
            generation_thread = std::thread(
                [&]() noexcept {
                    std::unique_lock<std::mutex> lock(
                        generation_mutex);
                    auto deadline =
                        std::chrono::steady_clock::now() +
                        benchmark.generation_interval;
                    while (!generation_cv.wait_until(
                        lock,
                        deadline,
                        [&]() noexcept {
                            return stop_generation_thread;
                        })) {
                        lock.unlock();
                        const runtime::RealtimePipelineCutResultV1 cut =
                            pipeline->CutAndPublishGeneration(
                                std::chrono::seconds(60));
                        const bool factor_contract_valid =
                            cut.factor_generation_enabled ==
                                factor_generation_enabled &&
                            (factor_generation_enabled
                                 ? cut.factor_generation != nullptr
                                 : cut.factor_generation == nullptr);
                        if (!cut.published() ||
                            !factor_contract_valid) {
                            periodic_cut_error.store(
                                static_cast<std::uint32_t>(cut.error),
                                std::memory_order_release);
                            periodic_generation_error.store(
                                static_cast<std::uint32_t>(
                                    cut.generation_error),
                                std::memory_order_release);
                            periodic_generation_failed.store(
                                true, std::memory_order_release);
                            return;
                        }
                        periodic_generation_cuts.fetch_add(
                            1U, std::memory_order_relaxed);
                        lock.lock();
                        deadline =
                            std::chrono::steady_clock::now() +
                            benchmark.generation_interval;
                    }
                });
        } catch (...) {
            std::cerr
                << "throughput generation thread creation failed\n";
            if (certified_service != nullptr) {
                certified_service->StopControl();
            }
            service->MarkFailed();
            service->StopControl();
            return false;
        }
    }
    const auto stop_periodic_generations = [&]() noexcept {
        {
            std::lock_guard<std::mutex> lock(generation_mutex);
            stop_generation_thread = true;
        }
        generation_cv.notify_all();
        if (generation_thread.joinable()) {
            generation_thread.join();
        }
    };

    const std::uint64_t start_ns = MonotonicNowNs();
    if (!Expect(start_ns != 0U, "start throughput profile clock")) {
        stop_periodic_generations();
        service->MarkFailed();
        service->StopControl();
        return false;
    }
    std::uint64_t invoked = 0U;
    bool message_patch_failed = false;
    bool fatal_during_offer = false;
    std::vector<bool> covered_store_workers(
        benchmark.store_worker_count, false);
    for (std::size_t instrument = 0U;
         instrument < benchmark.instruments_per_market;
         ++instrument) {
        if (benchmark.workload !=
            ThroughputWorkloadV1::kHotShenzhenTickSource) {
            covered_store_workers[
                instrument % benchmark.store_worker_count] = true;
        }
        if (benchmark.workload !=
            ThroughputWorkloadV1::kSingleInstrument) {
            covered_store_workers[
                (benchmark.instruments_per_market + instrument) %
                benchmark.store_worker_count] = true;
        }
    }
    if (benchmark.workload ==
        ThroughputWorkloadV1::kSingleInstrument) {
        covered_store_workers[
            benchmark.instruments_per_market %
            benchmark.store_worker_count] = true;
    }
    const std::size_t covered_store_worker_count =
        static_cast<std::size_t>(std::count(
            covered_store_workers.begin(),
            covered_store_workers.end(),
            true));
    std::size_t next_quarter = 0U;
    for (std::uint64_t index = 0U; index < planned; ++index) {
        const std::size_t tuple =
            ThroughputTupleIndexV1(benchmark.workload, index);
        const std::size_t instrument_index =
            benchmark.workload ==
                    ThroughputWorkloadV1::kSingleInstrument
                ? 0U
                : static_cast<std::size_t>(
                      (index / cycle_size) % messages.size());
        ThroughputInstrumentMessagesV1& instrument =
            messages[instrument_index];
        FakeSdkMessage* message = nullptr;
        if (tuple == 0U) {
            message = instrument.shanghai_snapshot.get();
        } else if (tuple == 1U) {
            ++shanghai_business_sequence;
            message = instrument.shanghai_tick.get();
            message_patch_failed =
                !message->StoreBodyU64(
                    0U, shanghai_business_sequence) ||
                !message->StoreBodyU64(
                    28U,
                    shanghai_business_sequence * 2U) ||
                !message->StoreBodyU64(
                    36U,
                    shanghai_business_sequence * 2U + 1U);
        } else if (tuple == 2U) {
            message = instrument.shenzhen_snapshot.get();
        } else if (tuple == 3U) {
            ++shenzhen_native_sequence;
            message = instrument.shenzhen_order.get();
            instrument.last_shenzhen_order_sequence =
                shenzhen_native_sequence;
            message_patch_failed = !message->StoreBodyU64(
                4U, shenzhen_native_sequence);
        } else if (tuple == 4U) {
            ++shenzhen_native_sequence;
            message = instrument.shenzhen_transaction.get();
            const std::uint64_t bid_sequence =
                instrument.last_shenzhen_order_sequence == 0U
                    ? shenzhen_native_sequence - 1U
                    : instrument.last_shenzhen_order_sequence;
            message_patch_failed =
                !message->StoreBodyU64(
                    4U, shenzhen_native_sequence) ||
                !message->StoreBodyU64(18U, bid_sequence);
        } else {
            message_patch_failed = true;
        }
        if (message == nullptr || message_patch_failed) {
            break;
        }

        // Pace N callback offers over N complete rate intervals.  Scheduling
        // callback zero at start_ns would cover only N - 1 intervals while
        // the reported rate divides N by the elapsed time.
        const std::uint64_t deadline_ns =
            start_ns + (index + 1U) * 1'000'000'000ULL /
                           benchmark.target_rate;
        for (;;) {
            const std::uint64_t now_ns = MonotonicNowNs();
            if (now_ns == 0U || now_ns >= deadline_ns) {
                break;
            }
            const std::uint64_t remaining_ns = deadline_ns - now_ns;
            if (remaining_ns > 100'000U) {
                std::this_thread::sleep_for(std::chrono::nanoseconds(
                    remaining_ns - 50'000U));
            } else {
                std::this_thread::yield();
            }
        }
        handler->OnMessage(nullptr, message);
        ++invoked;
        ++offered_by_tuple[tuple];
        while (next_quarter < backlog_at_quarter.size() &&
               invoked * 4U >=
                   planned *
                       static_cast<std::uint64_t>(next_quarter + 1U)) {
            const auto sample = pipeline->Snapshot();
            backlog_at_quarter[next_quarter] =
                sample.accepted_messages >=
                        sample.processing_progress.applied_sequence
                    ? sample.accepted_messages -
                          sample.processing_progress.applied_sequence
                    : 0U;
            ++next_quarter;
        }
        if ((invoked & 255U) == 0U && pipeline->fatal()) {
            fatal_during_offer = true;
            break;
        }
    }
    fatal_during_offer = fatal_during_offer || pipeline->fatal();
    const std::uint64_t offer_complete_ns = MonotonicNowNs();
    const runtime::RealtimePipelineSnapshotV1 before_drain =
        pipeline->Snapshot();
    stop_periodic_generations();
    const std::uint64_t applied_before_drain =
        before_drain.processing_progress.applied_sequence;
    const std::uint64_t backlog_before_drain =
        before_drain.accepted_messages >= applied_before_drain
            ? before_drain.accepted_messages - applied_before_drain
            : 0U;
    if (certified_service != nullptr) {
        certified_service->MarkDraining();
    }
    const std::uint64_t drain_start_ns = MonotonicNowNs();
    const runtime::RealtimePipelineCutResultV1 final_cut =
        pipeline->StopAndPublishFinalGeneration(
            std::chrono::seconds(60));
    const std::uint64_t drain_complete_ns = MonotonicNowNs();
    const runtime::RealtimePipelineSnapshotV1 final =
        pipeline->Snapshot();

    bool certified_idle = true;
    ipc::RealtimeCertifiedServiceSnapshotV1 certified_snapshot{};
    if (certified_service != nullptr) {
        certified_idle = certified_service->WaitUntilIdleForTest(
            std::chrono::seconds(60));
        certified_service->MarkStoppedClean();
        certified_snapshot = certified_service->Snapshot();
    }

    const std::array<std::uint64_t, 4U> expected_source_records{{
        offered_by_tuple[0U],
        offered_by_tuple[1U],
        offered_by_tuple[2U],
        offered_by_tuple[3U] + offered_by_tuple[4U],
    }};
    const ThroughputHistoryValidationV1 history_validation =
        ValidateThroughputHistoryGenerationV1(
            socket_path,
            final_cut.store_generation,
            benchmark.scenario,
            planned,
            expected_source_records);

    std::size_t decoder_depth_before_drain = 0U;
    std::size_t decoder_high_water_max = 0U;
    std::uint64_t decoder_full_count = 0U;
    for (const auto& queue : final.decoder_queues) {
        decoder_high_water_max = std::max(
            decoder_high_water_max, queue.message_high_water);
        decoder_full_count += queue.full_count;
    }
    for (const auto& queue : before_drain.decoder_queues) {
        decoder_depth_before_drain += queue.message_depth;
    }
    std::uint64_t parallel_parsed = 0U;
    std::uint64_t parallel_parse_failures = 0U;
    std::size_t parallel_issue_high_water_max = 0U;
    std::size_t parallel_active_worker_count = 0U;
    std::ostringstream parallel_worker_parsed_csv;
    for (std::size_t index = 0U;
         index < final.parallel_decoder.worker_count;
         ++index) {
        const auto& worker = final.parallel_decoder.workers[index];
        if (index != 0U) {
            parallel_worker_parsed_csv << ',';
        }
        parallel_worker_parsed_csv << worker.parsed_messages;
        if (worker.parsed_messages != 0U) {
            ++parallel_active_worker_count;
        }
        parallel_parsed += worker.parsed_messages;
        parallel_parse_failures += worker.parse_failures;
        parallel_issue_high_water_max = std::max(
            parallel_issue_high_water_max,
            worker.issue_high_water);
    }
    std::uint64_t parallel_dispatched = 0U;
    std::uint64_t parallel_inline = 0U;
    std::uint64_t parallel_farm = 0U;
    std::uint64_t parallel_completed = 0U;
    std::uint64_t parallel_committed = 0U;
    std::uint64_t parallel_history_batch_calls = 0U;
    std::uint64_t parallel_history_batched_messages = 0U;
    std::size_t parallel_history_batch_max = 0U;
    std::uint64_t parallel_discarded = 0U;
    std::size_t parallel_farm_outstanding = 0U;
    std::uint64_t parallel_committed_before_drain = 0U;
    std::uint64_t parallel_publish_failures = 0U;
    std::uint64_t parallel_lease_waits = 0U;
    std::uint64_t parallel_reorder_wait_max_ns = 0U;
    std::size_t parallel_completion_high_water_max = 0U;
    for (const auto& source : final.parallel_decoder.sources) {
        parallel_dispatched += source.dispatched_messages;
        parallel_inline += source.inline_messages;
        parallel_farm += source.farm_messages;
        parallel_completed += source.completed_messages;
        parallel_committed += source.committed_messages;
        parallel_history_batch_calls += source.history_batch_calls;
        parallel_history_batched_messages +=
            source.history_batched_messages;
        parallel_history_batch_max = std::max(
            parallel_history_batch_max,
            source.history_batch_max);
        parallel_discarded += source.discarded_messages;
        parallel_farm_outstanding += source.farm_outstanding;
        parallel_publish_failures +=
            source.completion_publish_failures;
        parallel_lease_waits += source.lease_wait_count;
        parallel_reorder_wait_max_ns = std::max(
            parallel_reorder_wait_max_ns,
            source.reorder_wait_max_ns);
        parallel_completion_high_water_max = std::max(
            parallel_completion_high_water_max,
            source.completion_high_water);
    }
    for (const auto& source :
         before_drain.parallel_decoder.sources) {
        parallel_committed_before_drain +=
            source.committed_messages;
    }

    const std::uint64_t producer_elapsed_ns =
        offer_complete_ns >= start_ns
            ? offer_complete_ns - start_ns
            : 0U;
    const std::uint64_t final_drain_and_cut_elapsed_ns =
        drain_complete_ns >= drain_start_ns
            ? drain_complete_ns - drain_start_ns
            : 0U;
    const std::uint64_t history_ready_elapsed_ns =
        drain_complete_ns >= start_ns
            ? drain_complete_ns - start_ns
            : 0U;
    const double achieved_offered_rps =
        producer_elapsed_ns == 0U
            ? 0.0
            : static_cast<double>(invoked) * 1'000'000'000.0 /
                  static_cast<double>(producer_elapsed_ns);
    const double history_ready_rps =
        history_ready_elapsed_ns == 0U
            ? 0.0
            : static_cast<double>(planned) * 1'000'000'000.0 /
                  static_cast<double>(history_ready_elapsed_ns);
    const std::uint64_t published_periodic_generations =
        periodic_generation_cuts.load(std::memory_order_acquire);
    const bool periodic_failed =
        periodic_generation_failed.load(std::memory_order_acquire);
    const bool generation_sequence_valid =
        final_cut.published() &&
        final_cut.factor_generation_enabled ==
            factor_generation_enabled &&
        (factor_generation_enabled
             ? final_cut.factor_generation != nullptr
             : final_cut.factor_generation == nullptr) &&
        final_cut.store_generation != nullptr &&
        final_cut.store_generation->watermark().generation ==
            published_periodic_generations + 1U &&
        (benchmark.generation_interval.count() == 0 ||
         duration_ms < static_cast<std::uint64_t>(
                           benchmark.generation_interval.count()) ||
         published_periodic_generations != 0U);
    const bool certified_healthy =
        certified_service == nullptr ||
        (certified_idle &&
         !certified_snapshot.globally_frozen_resource &&
         certified_snapshot.frozen_channel_count == 0U &&
         certified_snapshot.dropped_handoffs == 0U &&
         certified_snapshot.processed_handoffs ==
             certified_snapshot.enqueued_observations +
                 certified_snapshot.enqueued_applied_records);
    const bool complete_prefix =
        !final.fatal && !periodic_failed && generation_sequence_valid &&
        invoked == planned && final.accepted_messages == planned &&
        final.rejected_messages == 0U && final.post_cut_messages == 0U &&
        final.decoded_messages == planned &&
        final.processing_progress.applied_sequence == planned &&
        final.store.appended_records == planned &&
        final.store.failed_appends == 0U &&
        !final.store.coverage_lost && !service->failed() &&
        history_validation.passed && certified_healthy &&
        ((benchmark.parallel_decoder_worker_count == 0U &&
          !final.parallel_decoder.enabled) ||
         (final.parallel_decoder.enabled &&
          final.parallel_decoder.worker_count ==
              benchmark.parallel_decoder_worker_count &&
          parallel_dispatched == planned &&
          parallel_inline + parallel_farm ==
              parallel_dispatched &&
          parallel_parsed == parallel_farm &&
          parallel_completed == planned &&
          parallel_committed == planned &&
          parallel_discarded == 0U &&
          parallel_farm_outstanding == 0U &&
          parallel_parse_failures == 0U &&
          parallel_publish_failures == 0U));
    const std::uint64_t steady_state_backlog_budget = std::max(
        std::uint64_t{1'024U},
        (benchmark.target_rate + 999U) / 1'000U);
    const bool steady_state_met =
        backlog_before_drain <= steady_state_backlog_budget &&
        backlog_at_quarter[3U] <=
            backlog_at_quarter[1U] + steady_state_backlog_budget;
    const bool target_met =
        invoked == planned && complete_prefix &&
        achieved_offered_rps >=
            static_cast<double>(benchmark.target_rate) * 0.98 &&
        decoder_full_count == 0U && !message_patch_failed &&
        steady_state_met;
    bool stopped_clean = false;
    service->MarkDraining();
    if (complete_prefix) {
        stopped_clean = service->MarkStoppedClean(
            final.tick_stream_sequence);
    } else {
        service->MarkFailed();
    }
    if (certified_service != nullptr) {
        certified_service->StopControl();
    }
    service->StopControl();

    std::ostringstream achieved_text;
    achieved_text << std::fixed << std::setprecision(3)
                  << achieved_offered_rps;
    std::ostringstream history_ready_text;
    history_ready_text << std::fixed << std::setprecision(3)
                       << history_ready_rps;
    std::cout
        << "THROUGHPUT_RESULT target_rps=" << benchmark.target_rate
        << " duration_ms=" << duration_ms
        << " scenario="
        << StartupBenchmarkScenarioNameV1(benchmark.scenario)
        << " server_state="
        << (benchmark.scenario ==
                    StartupBenchmarkScenarioV1::kFromOpen
                ? "ACTIVE"
                : "LIVE_PARTIAL")
        << " coverage_from_open="
        << (StartupBenchmarkCoverageFromOpenV1(benchmark.scenario)
                ? 1
                : 0)
        << " online_recovery=0"
        << " factor_generation_enabled="
        << (factor_generation_enabled ? 1 : 0)
        << " workload=" << ThroughputWorkloadNameV1(benchmark.workload)
        << " sink=" << ThroughputSinkNameV1(benchmark.sink)
        << " instruments_per_market="
        << benchmark.instruments_per_market
        << " parallel_decoder_workers="
        << benchmark.parallel_decoder_worker_count
        << " decoder_queue_capacity_per_source="
        << benchmark.decoder_queue_capacity_per_source
        << " store_queue_capacity_per_source_worker="
        << benchmark.store_queue_capacity_per_source_worker
        << " store_segment_kib=" << benchmark.segment_kib
        << " parallel_idle_inline="
        << (final.parallel_decoder.idle_inline_enabled ? 1 : 0)
        << " store_worker_count=" << benchmark.store_worker_count
        << " covered_store_worker_count="
        << covered_store_worker_count
        << " planned_callbacks=" << planned
        << " invoked_callbacks=" << invoked
        << " producer_elapsed_ns=" << producer_elapsed_ns
        << " achieved_offered_rps=" << achieved_text.str()
        << " history_ready_elapsed_ns="
        << history_ready_elapsed_ns
        << " history_ready_rps=" << history_ready_text.str()
        << " target_met=" << (target_met ? 1 : 0)
        << " process_survived=1"
        << " fatal_during_offer=" << (fatal_during_offer ? 1 : 0)
        << " fatal_final=" << (final.fatal ? 1 : 0)
        << " message_patch_failed="
        << (message_patch_failed ? 1 : 0)
        << " accepting_before_drain="
        << (before_drain.accepting ? 1 : 0)
        << " accepted=" << final.accepted_messages
        << " rejected=" << final.rejected_messages
        << " post_cut=" << final.post_cut_messages
        << " decoded=" << final.decoded_messages
        << " decoded_before_drain="
        << before_drain.decoded_messages
        << " applied_before_drain=" << applied_before_drain
        << " backlog_before_drain=" << backlog_before_drain
        << " steady_state_backlog_budget="
        << steady_state_backlog_budget
        << " steady_state_met=" << (steady_state_met ? 1 : 0)
        << " applied="
        << final.processing_progress.applied_sequence
        << " store_appended=" << final.store.appended_records
        << " store_appended_before_drain="
        << before_drain.store.appended_records
        << " store_failed_appends=" << final.store.failed_appends
        << " store_coverage_lost="
        << (final.store.coverage_lost ? 1 : 0)
        << " decoder_depth_before_drain="
        << decoder_depth_before_drain
        << " decoder_high_water_max="
        << decoder_high_water_max
        << " decoder_full_count=" << decoder_full_count
        << " parallel_enabled="
        << (final.parallel_decoder.enabled ? 1 : 0)
        << " parallel_parsed=" << parallel_parsed
        << " parallel_worker_parsed_csv="
        << parallel_worker_parsed_csv.str()
        << " parallel_active_worker_count="
        << parallel_active_worker_count
        << " parallel_slots_per_source_worker="
        << final.parallel_decoder.slots_per_source_worker
        << " parallel_issue_capacity_per_worker="
        << market::kRealtimeHistorySourceCountV1 *
               final.parallel_decoder.slots_per_source_worker
        << " parallel_completion_capacity_per_source="
        << static_cast<std::size_t>(
               final.parallel_decoder.worker_count) *
               final.parallel_decoder.slots_per_source_worker
        << " parallel_parse_failures="
        << parallel_parse_failures
        << " parallel_dispatched=" << parallel_dispatched
        << " parallel_inline=" << parallel_inline
        << " parallel_farm=" << parallel_farm
        << " parallel_completed=" << parallel_completed
        << " parallel_committed=" << parallel_committed
        << " parallel_history_batch_calls="
        << parallel_history_batch_calls
        << " parallel_history_batched_messages="
        << parallel_history_batched_messages
        << " parallel_history_batch_max="
        << parallel_history_batch_max
        << " parallel_committed_before_drain="
        << parallel_committed_before_drain
        << " parallel_discarded=" << parallel_discarded
        << " parallel_farm_outstanding="
        << parallel_farm_outstanding
        << " parallel_completion_publish_failures="
        << parallel_publish_failures
        << " parallel_issue_high_water_max="
        << parallel_issue_high_water_max
        << " parallel_completion_high_water_max="
        << parallel_completion_high_water_max
        << " parallel_lease_waits=" << parallel_lease_waits
        << " parallel_reorder_wait_max_ns="
        << parallel_reorder_wait_max_ns
        << " backlog_q25=" << backlog_at_quarter[0U]
        << " backlog_q50=" << backlog_at_quarter[1U]
        << " backlog_q75=" << backlog_at_quarter[2U]
        << " backlog_q100=" << backlog_at_quarter[3U]
        << " tuple0_offered=" << offered_by_tuple[0U]
        << " tuple1_offered=" << offered_by_tuple[1U]
        << " tuple2_offered=" << offered_by_tuple[2U]
        << " tuple3_offered=" << offered_by_tuple[3U]
        << " tuple4_offered=" << offered_by_tuple[4U]
        << " generation_interval_ms="
        << benchmark.generation_interval.count()
        << " periodic_generation_cuts="
        << published_periodic_generations
        << " periodic_generation_failed="
        << (periodic_failed ? 1 : 0)
        << " periodic_cut_error="
        << periodic_cut_error.load(std::memory_order_acquire)
        << " periodic_generation_error="
        << periodic_generation_error.load(std::memory_order_acquire)
        << " final_cut_published="
        << (final_cut.published() ? 1 : 0)
        << " final_cut_error="
        << static_cast<std::uint32_t>(final_cut.error)
        << " final_generation_error="
        << static_cast<std::uint32_t>(final_cut.generation_error)
        << " final_generation="
        << (final_cut.store_generation == nullptr
                ? 0U
                : final_cut.store_generation->watermark().generation)
        << " final_factor_generation_present="
        << (final_cut.factor_generation != nullptr ? 1 : 0)
        << " generation_sequence_valid="
        << (generation_sequence_valid ? 1 : 0)
        << " history_control_status="
        << history_validation.history_control_status
        << " history_endpoint_flags="
        << history_validation.endpoint_flags
        << " history_endpoint_valid="
        << (history_validation.endpoint_valid ? 1 : 0)
        << " history_generation_valid="
        << (history_validation.generation_valid ? 1 : 0)
        << " history_scan_cursor_opened="
        << (history_validation.cursor_opened ? 1 : 0)
        << " history_scan_explicit_eof="
        << (history_validation.explicit_eof ? 1 : 0)
        << " history_scan_records="
        << history_validation.scanned_records
        << " history_unique_ingress="
        << history_validation.unique_ingress_sequences
        << " history_duplicate_ingress="
        << history_validation.duplicate_ingress_sequences
        << " history_out_of_range_ingress="
        << history_validation.out_of_range_ingress_sequences
        << " history_invalid_source_slots="
        << history_validation.invalid_source_slots
        << " history_source0_records="
        << history_validation.scanned_source_records[0U]
        << " history_source1_records="
        << history_validation.scanned_source_records[1U]
        << " history_source2_records="
        << history_validation.scanned_source_records[2U]
        << " history_source3_records="
        << history_validation.scanned_source_records[3U]
        << " history_source_counts_valid="
        << (history_validation.source_counts_valid ? 1 : 0)
        << " history_integrity_validation_elapsed_ns="
        << history_validation.scan_elapsed_ns
        << " history_lossless="
        << (history_validation.passed ? 1 : 0)
        << " last_decode_error="
        << static_cast<unsigned int>(final.last_decode_error)
        << " service_failed=" << (service->failed() ? 1 : 0)
        << " complete_prefix=" << (complete_prefix ? 1 : 0)
        << " stopped_clean=" << (stopped_clean ? 1 : 0)
        << " final_drain_and_cut_elapsed_ns="
        << final_drain_and_cut_elapsed_ns
        << " certified_idle=" << (certified_idle ? 1 : 0)
        << " certified_healthy=" << (certified_healthy ? 1 : 0)
        << " certified_enqueued_observations="
        << certified_snapshot.enqueued_observations
        << " certified_enqueued_applied="
        << certified_snapshot.enqueued_applied_records
        << " certified_processed="
        << certified_snapshot.processed_handoffs
        << " certified_dropped="
        << certified_snapshot.dropped_handoffs
        << " certified_frozen_channels="
        << certified_snapshot.frozen_channel_count
        << " certified_global_frozen="
        << (certified_snapshot.globally_frozen_resource ? 1 : 0)
        << '\n';
    return target_met && complete_prefix && stopped_clean;
}

bool RunThroughputStabilityBenchmark(
    std::uint64_t target_rate,
    std::chrono::milliseconds duration) {
    constexpr std::size_t kQueueCapacity = 4'096U;
    constexpr std::uint64_t kTickRingCapacity = 262'144U;
    constexpr std::uint32_t kStoreWorkerCount = 4U;
    constexpr std::uint64_t kMaximumTargetRate = 1'000'000U;
    constexpr std::uint64_t kMaximumDurationMs = 10'000U;
    const std::uint64_t duration_ms =
        static_cast<std::uint64_t>(duration.count());
    if (target_rate == 0U || target_rate > kMaximumTargetRate ||
        duration_ms == 0U || duration_ms > kMaximumDurationMs) {
        std::cerr << "invalid throughput benchmark arguments\n";
        return false;
    }
    const std::uint64_t planned =
        target_rate * duration_ms / 1'000U;
    if (planned == 0U) {
        std::cerr << "throughput benchmark plans no callbacks\n";
        return false;
    }

    std::cout
        << "THROUGHPUT_ENV target_rps=" << target_rate
        << " duration_ms=" << duration_ms
        << " planned_callbacks=" << planned
        << " callback_contract=serialized"
        << " decoder_queue_capacity_per_source=" << kQueueCapacity
        << " store_queue_capacity_per_source_worker=" << kQueueCapacity
        << " store_worker_count=" << kStoreWorkerCount
        << " tick_ring_capacity=" << kTickRingCapacity
        << " pacing=absolute_deadline_one_based_no_batch_wait"
        << " clock=CLOCK_MONOTONIC"
        << " affinity=" << CpuAffinityText() << '\n';

    ScopedTempDirectory temporary;
    DailyRuntimeFixture fixture = MakePipelineDailyFixture(81U);
    if (!Expect(
            temporary.valid() && static_cast<bool>(fixture),
            "create throughput fixture")) {
        return false;
    }
    const common::Identity128 run_id = RunId(0x81U);
    const std::filesystem::path socket_path =
        temporary.path() / "throughput-stability.sock";

    ipc::RealtimeSharedServiceConfigV2 service_config{};
    service_config.run_id = run_id;
    service_config.session_epoch = 81U;
    service_config.trade_date = kTradeDate;
    service_config.daily_catalog = fixture.catalog;
    service_config.coverage_from_open = true;
    service_config.tick_ring_capacity = kTickRingCapacity;
    service_config.maximum_history_readers = 4U;
    service_config.maximum_history_page_records = kQueueCapacity;
    service_config.control_socket_path = socket_path;
    std::shared_ptr<ipc::RealtimeSharedMarketServiceV2> service;
    int system_error = 0;
    const auto service_error =
        ipc::RealtimeSharedMarketServiceV2::Create(
            service_config, &service, &system_error);
    if (!Expect(
            service_error ==
                    ipc::RealtimeSharedServiceCreateErrorV2::kNone &&
                service != nullptr && system_error == 0,
            "create throughput Wire V2 service") ||
        !Expect(
            service->Start(&system_error) && system_error == 0,
            "start throughput Wire V2 service")) {
        if (service != nullptr) {
            service->StopControl();
        }
        return false;
    }

    auto sdk_state = std::make_shared<LatencySdkState>();
    auto sdk_factory =
        std::make_shared<LatencySdkFactory>(sdk_state);
    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    PipelineCleanup pipeline_cleanup(&pipeline);
    runtime::RealtimePipelineConfigV1 pipeline_config{};
    pipeline_config.run_id = run_id;
    pipeline_config.trade_date = kTradeDate;
    pipeline_config.daily_catalog = fixture.catalog;
    pipeline_config.runtime_state = fixture.runtime_state.get();
    pipeline_config.source_stream_ids = kSourceStreamIds;
    pipeline_config.maximum_sdk_message_bytes = 4'096U;
    pipeline_config.decoder_queue_capacity_per_source =
        kQueueCapacity;
    pipeline_config.tick_ring_capacity = kTickRingCapacity;
    pipeline_config.store_worker_count = kStoreWorkerCount;
    pipeline_config.store_queue_capacity_per_source_worker =
        kQueueCapacity;
    pipeline_config.intraday_store.segment_target_bytes =
        market::kIntradayInstrumentStoreMinimumSegmentBytesV1;
    pipeline_config.intraday_store.maximum_session_records =
        planned + kQueueCapacity;
    pipeline_config.intraday_store.maximum_session_accounted_bytes =
        4ULL * 1024ULL * 1024ULL * 1024ULL;
    pipeline_config.intraday_store.maximum_records_per_batch =
        kQueueCapacity;
    pipeline_config.intraday_store.coverage_from_open = true;
    pipeline_config.applied_record_sink = service;
    pipeline_config.processing_progress_sink = service;
    pipeline_config.store_generation_sink = service;
    pipeline_config.sdk.enabled = true;
    pipeline_config.sdk.server_address = "throughput.invalid";
    pipeline_config.sdk.user_name = "throughput";

    std::string pipeline_detail;
    const auto pipeline_error =
        runtime::RealtimePipelineV1::CreateForTest(
            pipeline_config,
            sdk_factory,
            &pipeline,
            &pipeline_detail);
    if (!Expect(
            pipeline_error ==
                    runtime::RealtimePipelineCreateErrorV1::kNone &&
                pipeline != nullptr,
            "create throughput Pipeline: " + pipeline_detail)) {
        service->MarkFailed();
        service->StopControl();
        return false;
    }
    mdl::MessageHandlerBase* const handler = sdk_state->handler();
    if (!Expect(
            handler != nullptr,
            "throughput SDK callback installed")) {
        service->MarkFailed();
        service->StopControl();
        return false;
    }

    FakeSdkMessage message(
        sdk::kProductionMessageKeysV1[2U],
        PipelineShenzhenSnapshotBody());
    const std::uint64_t start_ns = MonotonicNowNs();
    if (!Expect(start_ns != 0U, "start throughput clock")) {
        service->MarkFailed();
        service->StopControl();
        return false;
    }
    std::uint64_t invoked = 0U;
    bool fatal_during_offer = false;
    for (std::uint64_t index = 0U; index < planned; ++index) {
        const std::uint64_t deadline_ns =
            start_ns +
            (index + 1U) * 1'000'000'000ULL / target_rate;
        for (;;) {
            const std::uint64_t now_ns = MonotonicNowNs();
            if (now_ns == 0U || now_ns >= deadline_ns) {
                break;
            }
            const std::uint64_t remaining_ns = deadline_ns - now_ns;
            if (remaining_ns > 100'000U) {
                std::this_thread::sleep_for(std::chrono::nanoseconds(
                    remaining_ns - 50'000U));
            } else {
                std::this_thread::yield();
            }
        }
        handler->OnMessage(nullptr, &message);
        ++invoked;
        if ((invoked & 255U) == 0U && pipeline->fatal()) {
            fatal_during_offer = true;
            break;
        }
    }
    fatal_during_offer = fatal_during_offer || pipeline->fatal();
    const std::uint64_t offer_complete_ns = MonotonicNowNs();
    const runtime::RealtimePipelineSnapshotV1 before_drain =
        pipeline->Snapshot();
    const std::uint64_t drain_start_ns = MonotonicNowNs();
    pipeline->StopAndDrain();
    const std::uint64_t drain_complete_ns = MonotonicNowNs();
    const runtime::RealtimePipelineSnapshotV1 final =
        pipeline->Snapshot();

    std::size_t decoder_depth_before_drain = 0U;
    std::size_t decoder_high_water_max = 0U;
    std::uint64_t decoder_full_count = 0U;
    for (const auto& queue : final.decoder_queues) {
        decoder_high_water_max = std::max(
            decoder_high_water_max, queue.message_high_water);
        decoder_full_count += queue.full_count;
    }
    for (const auto& queue : before_drain.decoder_queues) {
        decoder_depth_before_drain += queue.message_depth;
    }

    const std::uint64_t producer_elapsed_ns =
        offer_complete_ns >= start_ns
            ? offer_complete_ns - start_ns
            : 0U;
    const std::uint64_t drain_elapsed_ns =
        drain_complete_ns >= drain_start_ns
            ? drain_complete_ns - drain_start_ns
            : 0U;
    const double achieved_offered_rps =
        producer_elapsed_ns == 0U
            ? 0.0
            : static_cast<double>(invoked) * 1'000'000'000.0 /
                  static_cast<double>(producer_elapsed_ns);
    const bool complete_prefix =
        !final.fatal && final.accepted_messages == planned &&
        final.decoded_messages == planned &&
        final.processing_progress.applied_sequence == planned &&
        final.store.appended_records == planned &&
        final.store.failed_appends == 0U && !service->failed();
    const bool target_met =
        invoked == planned && complete_prefix &&
        achieved_offered_rps >=
            static_cast<double>(target_rate) * 0.98;
    bool stopped_clean = false;
    service->MarkDraining();
    if (complete_prefix) {
        stopped_clean = service->MarkStoppedClean(
            final.tick_stream_sequence);
    } else {
        service->MarkFailed();
    }
    service->StopControl();

    std::ostringstream achieved_text;
    achieved_text << std::fixed << std::setprecision(3)
                  << achieved_offered_rps;
    std::cout
        << "THROUGHPUT_RESULT target_rps=" << target_rate
        << " duration_ms=" << duration_ms
        << " planned_callbacks=" << planned
        << " invoked_callbacks=" << invoked
        << " producer_elapsed_ns=" << producer_elapsed_ns
        << " achieved_offered_rps=" << achieved_text.str()
        << " target_met=" << (target_met ? 1 : 0)
        << " process_survived=1"
        << " fatal_during_offer="
        << (fatal_during_offer ? 1 : 0)
        << " fatal_final=" << (final.fatal ? 1 : 0)
        << " accepting_before_drain="
        << (before_drain.accepting ? 1 : 0)
        << " accepted=" << final.accepted_messages
        << " rejected=" << final.rejected_messages
        << " post_cut=" << final.post_cut_messages
        << " decoded=" << final.decoded_messages
        << " applied="
        << final.processing_progress.applied_sequence
        << " store_appended=" << final.store.appended_records
        << " store_failed_appends=" << final.store.failed_appends
        << " decoder_depth_before_drain="
        << decoder_depth_before_drain
        << " decoder_high_water_max="
        << decoder_high_water_max
        << " decoder_full_count=" << decoder_full_count
        << " last_decode_error="
        << static_cast<unsigned int>(final.last_decode_error)
        << " service_failed=" << (service->failed() ? 1 : 0)
        << " complete_prefix=" << (complete_prefix ? 1 : 0)
        << " stopped_clean=" << (stopped_clean ? 1 : 0)
        << " drain_elapsed_ns=" << drain_elapsed_ns << '\n';
    return target_met && complete_prefix && stopped_clean;
}

bool RunHistoryLatencyBenchmark(
    std::uint32_t parallel_decoder_worker_count,
    bool callback_polars_only = false,
    StartupBenchmarkScenarioV1 scenario =
        StartupBenchmarkScenarioV1::kFromOpen) {
    constexpr std::size_t kShanghaiInstrumentCount = 3U;
    constexpr std::size_t kSnapshotFillCount = 11'997U;
    constexpr std::size_t kCapacity =
        kSnapshotFillCount + kShanghaiInstrumentCount;
    constexpr std::size_t kBoundInstrumentCount = kCapacity;
    constexpr std::size_t kQueueCapacity = 8'192U;
    constexpr std::size_t kFillBatch = 512U;
    constexpr std::size_t kMaximumSequence = 250'000U;
    constexpr std::uint64_t kTickRingCapacity = 262'144U;
    constexpr std::uint32_t kStoreWorkerCount = 4U;
    // Exact-key sorting assigns the sole Shanghai entry ID 1, followed by
    // the other Shanghai entries and the Shenzhen range.
    constexpr std::uint32_t kPureTickInstrument = 1U;
    constexpr std::uint32_t kDerivedInstrument = 2U;
    constexpr std::uint32_t kRawBatchInstrument = 3U;
    constexpr std::uint32_t kMixedInstrument = 4U;
    constexpr std::size_t kRawPolarsRecords = 4'096U;
    constexpr std::size_t kRawPolarsRepeats = 20U;
    constexpr std::size_t kPriceRepeats = 20U;
    constexpr std::size_t kAllColumnRepeats = 10U;
    const bool factor_generation_enabled =
        scenario == StartupBenchmarkScenarioV1::kFromOpen;

    std::cout
        << "HISTORY_ENV capacity=" << kCapacity
        << " scenario=" << StartupBenchmarkScenarioNameV1(scenario)
        << " server_state="
        << (scenario == StartupBenchmarkScenarioV1::kFromOpen
                ? "ACTIVE"
                : "LIVE_PARTIAL")
        << " coverage_from_open="
        << (StartupBenchmarkCoverageFromOpenV1(scenario) ? 1 : 0)
        << " online_recovery=0"
        << " factor_generation_enabled="
        << (factor_generation_enabled ? 1 : 0)
        << " generation_visibility=forced_cut"
        << " bound_instruments=" << kBoundInstrumentCount
        << " snapshot_fill=" << kSnapshotFillCount
        << " worker_count=" << kStoreWorkerCount
        << " tick_ring_capacity=" << kTickRingCapacity
        << " requested_page_records=4096"
        << " price_repeats=" << kPriceRepeats
        << " all_column_repeats=" << kAllColumnRepeats
        << " raw_polars_records=" << kRawPolarsRecords
        << " raw_polars_repeats=" << kRawPolarsRepeats
        << " parallel_decoder_workers="
        << parallel_decoder_worker_count
        << " parallel_decoder_farm_activation_queue_depth="
        << runtime::RealtimePipelineConfigV1{}
               .parallel_decoder_farm_activation_queue_depth
        << " parallel_decoder_farm_activation_effective_depth="
        << EffectiveParallelDecoderFarmActivationDepthV1(
               runtime::RealtimePipelineConfigV1{}
                   .parallel_decoder_farm_activation_queue_depth,
               kQueueCapacity)
        << " clock=CLOCK_MONOTONIC"
        << " affinity=" << CpuAffinityText() << '\n';

    ScopedTempDirectory temporary;
    DailyRuntimeFixture fixture = MakeHistoryBenchmarkDailyFixture(
        kSnapshotFillCount, kShanghaiInstrumentCount, 67U);
    if (!Expect(
            temporary.valid() && static_cast<bool>(fixture),
            "create history latency fixture")) {
        return false;
    }
    const common::Identity128 run_id = RunId(0x67U);
    const std::filesystem::path socket_path =
        temporary.path() / "history-latency.sock";
    HistoryStageCollector history_stages;

    ipc::RealtimeSharedServiceConfigV2 service_config{};
    service_config.run_id = run_id;
    service_config.session_epoch = 67U;
    service_config.trade_date = kTradeDate;
    service_config.daily_catalog = fixture.catalog;
    service_config.coverage_from_open =
        StartupBenchmarkCoverageFromOpenV1(scenario);
    service_config.tick_ring_capacity = kTickRingCapacity;
    service_config.maximum_history_readers = 8U;
    service_config.maximum_history_page_records = 4'096U;
    service_config.history_stage_observer = &history_stages;
    service_config.control_socket_path = socket_path;
    std::shared_ptr<ipc::RealtimeSharedMarketServiceV2> service;
    int system_error = 0;
    const auto service_error =
        ipc::RealtimeSharedMarketServiceV2::Create(
            service_config, &service, &system_error);
    if (service_error !=
            ipc::RealtimeSharedServiceCreateErrorV2::kNone ||
        service == nullptr || system_error != 0) {
        std::cerr
            << "history service create error="
            << static_cast<unsigned int>(service_error)
            << " system_error=" << system_error << '\n';
    }
    if (!Expect(
            service_error ==
                    ipc::RealtimeSharedServiceCreateErrorV2::kNone &&
                service != nullptr && system_error == 0,
            "create history Wire V2 service") ||
        !Expect(
            StartStartupBenchmarkServiceV1(
                service, scenario, &system_error) &&
                system_error == 0,
            "start history Wire V2 service")) {
        if (service != nullptr) {
            service->StopControl();
        }
        return false;
    }

    auto timed_applied = std::make_shared<TimedAppliedSink>(
        service, kMaximumSequence);
    auto sdk_state = std::make_shared<LatencySdkState>();
    auto sdk_factory =
        std::make_shared<LatencySdkFactory>(sdk_state);
    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    PipelineCleanup pipeline_cleanup(&pipeline);
    runtime::RealtimePipelineConfigV1 pipeline_config{};
    pipeline_config.run_id = run_id;
    pipeline_config.trade_date = kTradeDate;
    pipeline_config.daily_catalog = fixture.catalog;
    pipeline_config.runtime_state = fixture.runtime_state.get();
    pipeline_config.source_stream_ids = kSourceStreamIds;
    pipeline_config.maximum_sdk_message_bytes = 4'096U;
    pipeline_config.decoder_queue_capacity_per_source =
        kQueueCapacity;
    pipeline_config.tick_ring_capacity = kTickRingCapacity;
    pipeline_config.store_worker_count = kStoreWorkerCount;
    pipeline_config.store_queue_capacity_per_source_worker =
        kQueueCapacity;
    pipeline_config.intraday_store.segment_target_bytes =
        market::kIntradayInstrumentStoreMinimumSegmentBytesV1;
    pipeline_config.intraday_store.maximum_session_records =
        kMaximumSequence;
    pipeline_config.intraday_store.maximum_session_accounted_bytes =
        2ULL * 1024ULL * 1024ULL * 1024ULL;
    pipeline_config.intraday_store.maximum_records_per_batch =
        kQueueCapacity;
    pipeline_config.intraday_store.coverage_from_open =
        StartupBenchmarkCoverageFromOpenV1(scenario);
    pipeline_config.factor_generation_enabled =
        factor_generation_enabled;
    pipeline_config.applied_record_sink = timed_applied;
    pipeline_config.processing_progress_sink = service;
    pipeline_config.store_generation_sink = service;
    pipeline_config.sdk.enabled = true;
    pipeline_config.sdk.server_address = "history-latency.invalid";
    pipeline_config.sdk.user_name = "history-latency";
    pipeline_config.parallel_decoder_idle_inline_enabled = true;
    if (!SetParallelDecoderWorkerCountV1(
            &pipeline_config,
            parallel_decoder_worker_count)) {
        std::cerr
            << "parallel decoder worker configuration is unavailable\n";
        service->MarkFailed();
        service->StopControl();
        return false;
    }

    std::string pipeline_detail;
    const auto pipeline_error =
        runtime::RealtimePipelineV1::CreateForTest(
            pipeline_config,
            sdk_factory,
            &pipeline,
            &pipeline_detail);
    if (!Expect(
            pipeline_error ==
                    runtime::RealtimePipelineCreateErrorV1::kNone &&
                pipeline != nullptr,
            "create history latency Pipeline: " +
                pipeline_detail)) {
        service->MarkFailed();
        service->StopControl();
        return false;
    }
    const auto cut_matches_factor_contract =
        [factor_generation_enabled](
            const runtime::RealtimePipelineCutResultV1& cut) noexcept {
            return cut.factor_generation_enabled ==
                       factor_generation_enabled &&
                   (factor_generation_enabled
                        ? cut.factor_generation != nullptr
                        : cut.factor_generation == nullptr);
        };
    mdl::MessageHandlerBase* const handler = sdk_state->handler();
    if (!Expect(
            handler != nullptr,
            "history latency SDK callback installed")) {
        service->MarkFailed();
        service->StopControl();
        return false;
    }

    auto wait_prefix =
        [&](std::uint64_t sequence) {
            const auto deadline =
                std::chrono::steady_clock::now() +
                std::chrono::seconds(60);
            while (std::chrono::steady_clock::now() < deadline) {
                const runtime::RealtimePipelineSnapshotV1 state =
                    pipeline->Snapshot();
                if (state.fatal || pipeline->fatal()) {
                    return false;
                }
                if (state.accepted_messages == sequence &&
                    state.processing_progress.accepted_sequence ==
                        sequence &&
                    state.processing_progress.applied_sequence ==
                        sequence) {
                    return true;
                }
                std::this_thread::yield();
            }
            return false;
        };

    std::uint64_t next_sequence = 1U;
    for (std::uint32_t first = 1U;
         first <= kSnapshotFillCount;
         first += static_cast<std::uint32_t>(kFillBatch)) {
        const std::uint32_t last =
            std::min<std::uint32_t>(
                static_cast<std::uint32_t>(kSnapshotFillCount),
                first + static_cast<std::uint32_t>(kFillBatch) - 1U);
        for (std::uint32_t id = first; id <= last; ++id) {
            FakeSdkMessage snapshot(
                sdk::kProductionMessageKeysV1[2U],
                PipelineShenzhenSnapshotBody(
                    ShenzhenAShareSecurityId(id)));
            handler->OnMessage(nullptr, &snapshot);
            ++next_sequence;
        }
        if (!Expect(
                wait_prefix(next_sequence - 1U),
                "fill daily Shenzhen snapshot working set")) {
            service->MarkFailed();
            service->StopControl();
            return false;
        }
    }
    std::cout
        << "HISTORY_FILL records=" << (next_sequence - 1U)
        << " bound_count=" << kSnapshotFillCount << '\n';

    FakeSdkMessage shanghai_tick(
        sdk::kProductionMessageKeysV1[1U],
        PipelineShanghaiTickBody());
    FakeSdkMessage shenzhen_order(
        sdk::kProductionMessageKeysV1[3U],
        PipelineShenzhenOrderBody());

    struct CallbackBoundary final {
        std::uint64_t sequence = 0U;
        std::uint64_t callback_start_ns = 0U;
        std::uint64_t callback_return_ns = 0U;
        std::uint64_t wire_recv_ns = 0U;
        std::uint64_t ipc_begin_ns = 0U;
        std::uint64_t ipc_return_ns = 0U;
        std::uint64_t applied_observed_ns = 0U;
    };
    auto append_until =
        [&](FakeSdkMessage* message,
            std::uint64_t* current_count,
            std::uint64_t target_count,
            CallbackBoundary* boundary) {
            if (message == nullptr || current_count == nullptr ||
                boundary == nullptr ||
                target_count <= *current_count) {
                return false;
            }
            while (*current_count + 1U < target_count) {
                handler->OnMessage(nullptr, message);
                ++*current_count;
                ++next_sequence;
                if ((*current_count % kFillBatch) == 0U &&
                    !wait_prefix(next_sequence - 1U)) {
                    const auto state = pipeline->Snapshot();
                    std::cerr
                        << "append wait failed target_count="
                        << target_count
                        << " current_count=" << *current_count
                        << " expected_sequence="
                        << (next_sequence - 1U)
                        << " accepted=" << state.accepted_messages
                        << " decoded=" << state.decoded_messages
                        << " rejected=" << state.rejected_messages
                        << " applied="
                        << state.processing_progress.applied_sequence
                        << " decode_error="
                        << static_cast<unsigned int>(
                               state.last_decode_error)
                        << " store_records="
                        << state.store.appended_records
                        << " store_bytes="
                        << state.store.accounted_record_bytes
                        << " store_failed="
                        << state.store.failed_appends
                        << " store_coverage_lost="
                        << state.store.coverage_lost
                        << " fatal=" << state.fatal << '\n';
                    return false;
                }
            }
            if (next_sequence > 1U &&
                !wait_prefix(next_sequence - 1U)) {
                const auto state = pipeline->Snapshot();
                std::cerr
                    << "append pre-final wait failed target_count="
                    << target_count
                    << " current_count=" << *current_count
                    << " expected_sequence="
                    << (next_sequence - 1U)
                    << " accepted=" << state.accepted_messages
                    << " applied="
                    << state.processing_progress.applied_sequence
                    << " fatal=" << state.fatal << '\n';
                return false;
            }
            boundary->sequence = next_sequence;
            boundary->callback_start_ns = MonotonicNowNs();
            handler->OnMessage(nullptr, message);
            boundary->callback_return_ns = MonotonicNowNs();
            ++*current_count;
            ++next_sequence;
            const auto deadline =
                std::chrono::steady_clock::now() +
                std::chrono::seconds(30);
            while (std::chrono::steady_clock::now() < deadline) {
                if (timed_applied->Read(
                        boundary->sequence,
                        &boundary->wire_recv_ns,
                        &boundary->ipc_begin_ns,
                        &boundary->ipc_return_ns)) {
                    break;
                }
                std::this_thread::yield();
            }
            const auto applied_deadline =
                std::chrono::steady_clock::now() +
                std::chrono::seconds(30);
            while (std::chrono::steady_clock::now() <
                   applied_deadline) {
                const runtime::RealtimePipelineSnapshotV1 state =
                    pipeline->Snapshot();
                if (state.fatal || pipeline->fatal()) {
                    break;
                }
                if (state.processing_progress.applied_sequence >=
                    boundary->sequence) {
                    boundary->applied_observed_ns =
                        MonotonicNowNs();
                    break;
                }
                std::this_thread::yield();
            }
            return boundary->callback_start_ns != 0U &&
                   boundary->callback_return_ns >=
                       boundary->callback_start_ns &&
                   boundary->wire_recv_ns >=
                       boundary->callback_start_ns &&
                   boundary->ipc_begin_ns >=
                       boundary->wire_recv_ns &&
                   boundary->ipc_return_ns >=
                       boundary->ipc_begin_ns &&
                   boundary->applied_observed_ns >=
                       boundary->ipc_return_ns;
        };

    PythonLatencyProcess python;
    ProtocolChannel* protocol = nullptr;
    auto ensure_python = [&] {
        if (protocol != nullptr) {
            return true;
        }
        if (!SpawnPythonHistoryLatencyProbe(
                socket_path, &python) ||
            python.channel() == nullptr) {
            return false;
        }
        protocol = python.channel();
        std::string line;
        if (!protocol->ReadLine(
                std::chrono::seconds(30), &line) ||
            !line.starts_with("READY ")) {
            std::cerr << "history probe startup: " << line << '\n';
            return false;
        }
        std::cout << "PYTHON_" << line << '\n';
        return true;
    };
    if (!Expect(
            ensure_python(),
            "prestart Python history probe before measured t0")) {
        return false;
    }

    auto print_python_distribution =
        [](std::string_view workload,
           const PythonHistoryCommandResult& result,
           std::uint64_t records) {
            const std::string prefix =
                std::string(workload) + "_";
            if (!result.session_open_ns.empty()) {
                PrintLatency(
                    prefix + "python_session_open",
                    result.session_open_ns);
            }
            PrintLatency(
                prefix + "python_cursor_open",
                result.cursor_open_ns);
            PrintLatency(
                prefix + "python_scan_to_explicit_eof",
                result.scan_ns);
            PrintLatency(
                prefix +
                    "python_open_return_to_complete_consumption",
                result.open_return_to_complete_ns);
            if (!result.checkpoint_access_ns.empty()) {
                PrintLatency(
                    prefix +
                        "verified_checkpoint_property_access",
                    result.checkpoint_access_ns);
            }
            if (!result.transaction_begin_ns.empty()) {
                PrintLatency(
                    prefix + "python_transaction_begin",
                    result.transaction_begin_ns);
                PrintLatency(
                    prefix + "python_factor_update",
                    result.factor_update_ns);
                PrintLatency(
                    prefix + "python_factor_column_materialization",
                    result.factor_column_ns);
                PrintLatency(
                    prefix + "python_factor_arithmetic",
                    result.factor_math_ns);
                PrintLatency(
                    prefix + "python_consume_nonfactor",
                    result.consume_nonfactor_ns);
                PrintLatency(
                    prefix + "python_atomic_commit",
                    result.atomic_commit_ns);
            }
            const LatencySummary scan =
                SummarizeLatency(result.scan_ns);
            const long double records_per_second =
                scan.mean_ns == 0U
                    ? 0.0L
                    : static_cast<long double>(records) *
                          1'000'000'000.0L /
                          static_cast<long double>(scan.mean_ns);
            std::cout
                << "PYTHON_THROUGHPUT workload=" << workload
                << " records_per_scan=" << records
                << " mean_records_per_second=" << std::fixed
                << std::setprecision(3) << records_per_second
                << std::defaultfloat << '\n';
        };

    auto run_python_lines =
        [&](const std::string& command,
            std::string_view sample_prefix,
            std::size_t sample_count,
            std::vector<std::string>* samples) {
            if (samples == nullptr ||
                !protocol->SendLine(command)) {
                return false;
            }
            samples->clear();
            samples->reserve(sample_count);
            std::string response;
            for (std::size_t index = 0U;
                 index < sample_count;
                 ++index) {
                if (!protocol->ReadLine(
                        std::chrono::seconds(120), &response) ||
                    !response.starts_with(sample_prefix)) {
                    std::cerr << "Python Polars sample: "
                              << response << '\n';
                    return false;
                }
                std::cout << "PYTHON_" << response << '\n';
                samples->push_back(response);
            }
            if (!protocol->ReadLine(
                    std::chrono::seconds(120), &response) ||
                !response.starts_with("DONE ")) {
                std::cerr << "Python Polars DONE: "
                          << response << '\n';
                return false;
            }
            std::cout << "PYTHON_" << response << '\n';
            return true;
        };

    std::vector<std::string> polars_lines;
    if (!run_python_lines(
            "PREPARE_DERIVED_POLARS " +
                std::to_string(kDerivedInstrument),
            "DERIVED_POLARS_PREPARED ",
            1U,
            &polars_lines)) {
        return false;
    }
    const runtime::RealtimePipelineCutResultV1 polars_baseline_cut =
        pipeline->CutAndPublishGeneration(std::chrono::seconds(60));
    if (!Expect(
            polars_baseline_cut.published() &&
                polars_baseline_cut.store_generation != nullptr &&
                cut_matches_factor_contract(polars_baseline_cut),
            "publish empty raw-Polars baseline generation")) {
        return false;
    }
    const std::uint64_t polars_baseline_generation =
        polars_baseline_cut.store_generation->watermark().generation;
    if (!run_python_lines(
            "RAW_POLARS_BASELINE " +
                std::to_string(kRawBatchInstrument) + " " +
                std::to_string(polars_baseline_generation),
            "RAW_POLARS_BASELINE ",
            1U,
            &polars_lines)) {
        return false;
    }

    struct PolarsIngressBoundary final {
        std::uint64_t first_sequence = 0U;
        std::uint64_t first_call_start_ns = 0U;
        std::uint64_t first_call_return_ns = 0U;
        std::uint64_t last_sequence = 0U;
        std::uint64_t last_call_start_ns = 0U;
        std::uint64_t last_call_return_ns = 0U;
        std::uint64_t first_callback_entry_ns = 0U;
        std::uint64_t last_callback_entry_ns = 0U;
    };
    auto fill_applied_callback_entries =
        [&](PolarsIngressBoundary* boundary) {
            if (boundary == nullptr) {
                return false;
            }
            std::uint64_t ignored_begin = 0U;
            std::uint64_t ignored_return = 0U;
            return timed_applied->Read(
                       boundary->first_sequence,
                       &boundary->first_callback_entry_ns,
                       &ignored_begin,
                       &ignored_return) &&
                   timed_applied->Read(
                       boundary->last_sequence,
                       &boundary->last_callback_entry_ns,
                       &ignored_begin,
                       &ignored_return);
        };

    PolarsIngressBoundary raw_polars_boundary{};
    const std::uint64_t polars_native_sequence_base =
        scenario == StartupBenchmarkScenarioV1::kFromOpen
            ? 0U
            : 10'000'000U;
    for (std::size_t index = 0U;
         index < kRawPolarsRecords;
         ++index) {
        FakeSdkMessage message(
            sdk::kProductionMessageKeysV1[1U],
            PipelineShanghaiTickBody(
                "600003",
                polars_native_sequence_base +
                    static_cast<std::uint64_t>(index + 1U),
                1U));
        const std::uint64_t sequence = next_sequence;
        const std::uint64_t call_start = MonotonicNowNs();
        handler->OnMessage(nullptr, &message);
        const std::uint64_t call_return = MonotonicNowNs();
        ++next_sequence;
        if (index == 0U) {
            raw_polars_boundary.first_sequence = sequence;
            raw_polars_boundary.first_call_start_ns = call_start;
            raw_polars_boundary.first_call_return_ns = call_return;
        }
        if (index + 1U == kRawPolarsRecords) {
            raw_polars_boundary.last_sequence = sequence;
            raw_polars_boundary.last_call_start_ns = call_start;
            raw_polars_boundary.last_call_return_ns = call_return;
        }
    }
    if (!Expect(
            wait_prefix(next_sequence - 1U) &&
                fill_applied_callback_entries(&raw_polars_boundary),
            "raw Polars batch reaches the applied prefix")) {
        return false;
    }
    const runtime::RealtimePipelineCutResultV1 raw_polars_cut =
        pipeline->CutAndPublishGeneration(std::chrono::seconds(60));
    if (!Expect(
            raw_polars_cut.published() &&
                raw_polars_cut.store_generation != nullptr &&
                cut_matches_factor_contract(raw_polars_cut),
            "publish raw Polars batch generation")) {
        return false;
    }
    const std::uint64_t raw_polars_generation =
        raw_polars_cut.store_generation->watermark().generation;
    ipc::RealtimeHistoryOpenResponseV2 raw_history_response{};
    const std::uint32_t expected_generation_flags =
        ipc::kRealtimeGenerationRecordCoverageCompleteV2 |
        (StartupBenchmarkCoverageFromOpenV1(scenario)
             ? ipc::kRealtimeGenerationCoverageFromOpenV2
             : 0U);
    if (!Expect(
            RequestHistoryOpen(
                socket_path,
                raw_polars_generation,
                &raw_history_response) &&
                raw_history_response.status ==
                    static_cast<std::uint16_t>(
                        ipc::RealtimeHistoryControlStatusV2::kOk) &&
                raw_history_response.generation.endpoint.flags ==
                    expected_generation_flags,
            "raw Polars generation preserves startup coverage flags")) {
        return false;
    }
    if (!run_python_lines(
            "RAW_POLARS_UPDATE " +
                std::to_string(kRawBatchInstrument) + " " +
                std::to_string(raw_polars_generation) + " " +
                std::to_string(kRawPolarsRepeats) + " " +
                std::to_string(kRawPolarsRecords) + " " +
                std::to_string(raw_polars_boundary.first_sequence) + " " +
                std::to_string(raw_polars_boundary.last_sequence),
            "RAW_POLARS_SAMPLE ",
            kRawPolarsRepeats,
            &polars_lines)) {
        return false;
    }
    std::uint64_t raw_first_entry_ns = 0U;
    std::uint64_t raw_last_entry_ns = 0U;
    std::uint64_t raw_polars_ready_ns = 0U;
    if (!Expect(
            !polars_lines.empty() &&
                ParseLineUnsignedField(
                    polars_lines.front(),
                    "first_callback_entry_ns",
                    &raw_first_entry_ns) &&
                ParseLineUnsignedField(
                    polars_lines.front(),
                    "last_callback_entry_ns",
                    &raw_last_entry_ns) &&
                ParseLineUnsignedField(
                    polars_lines.front(),
                    "polars_ready_ns",
                    &raw_polars_ready_ns) &&
                raw_first_entry_ns ==
                    raw_polars_boundary.first_callback_entry_ns &&
                raw_last_entry_ns ==
                    raw_polars_boundary.last_callback_entry_ns &&
                raw_polars_boundary.first_call_start_ns <=
                    raw_first_entry_ns &&
                raw_first_entry_ns <=
                    raw_polars_boundary.first_call_return_ns &&
                raw_polars_boundary.last_call_start_ns <=
                    raw_last_entry_ns &&
                raw_last_entry_ns <=
                    raw_polars_boundary.last_call_return_ns &&
                raw_polars_ready_ns >= raw_last_entry_ns,
            "raw Polars callback boundaries reconcile")) {
        return false;
    }
    std::cout
        << "POLARS_BOUNDARY workload=raw_batch_4096_all_columns"
        << " scenario=" << StartupBenchmarkScenarioNameV1(scenario)
        << " generation=" << raw_polars_generation
        << " records=" << kRawPolarsRecords
        << " columns=55"
        << " first_ingress_sequence="
        << raw_polars_boundary.first_sequence
        << " last_ingress_sequence="
        << raw_polars_boundary.last_sequence
        << " first_caller_before_callback_ns="
        << raw_polars_boundary.first_call_start_ns
        << " last_caller_before_callback_ns="
        << raw_polars_boundary.last_call_start_ns
        << " first_callback_entry_ns=" << raw_first_entry_ns
        << " last_callback_entry_ns=" << raw_last_entry_ns
        << " polars_ready_ns=" << raw_polars_ready_ns
        << " strict_first_callback_to_polars_ns="
        << (raw_polars_ready_ns -
            raw_polars_boundary.first_call_start_ns)
        << " strict_last_callback_to_polars_ns="
        << (raw_polars_ready_ns -
            raw_polars_boundary.last_call_start_ns)
        << '\n';

    PolarsIngressBoundary derived_boundary{};
    const std::array<std::vector<std::byte>, 4U> derived_bodies{{
        PipelineShanghaiStatusBody(
            "600002",
            polars_native_sequence_base + 4'096U,
            "TRADE"),
        PipelineShanghaiTickBody(
            "600002",
            polars_native_sequence_base + 4'097U,
            101U),
        PipelineShanghaiAddBody(
            "600002",
            polars_native_sequence_base + 4'098U,
            50U,
            101U),
        PipelineShanghaiTickBody(
            "600002",
            polars_native_sequence_base + 4'099U,
            50U),
    }};
    for (std::size_t index = 0U;
         index < derived_bodies.size();
         ++index) {
        FakeSdkMessage message(
            sdk::kProductionMessageKeysV1[1U],
            derived_bodies[index]);
        const std::uint64_t sequence = next_sequence;
        const std::uint64_t call_start = MonotonicNowNs();
        handler->OnMessage(nullptr, &message);
        const std::uint64_t call_return = MonotonicNowNs();
        ++next_sequence;
        if (index == 1U) {
            derived_boundary.first_sequence = sequence;
            derived_boundary.first_call_start_ns = call_start;
            derived_boundary.first_call_return_ns = call_return;
        }
        if (index + 1U == derived_bodies.size()) {
            derived_boundary.last_sequence = sequence;
            derived_boundary.last_call_start_ns = call_start;
            derived_boundary.last_call_return_ns = call_return;
        }
    }
    if (!Expect(
            wait_prefix(next_sequence - 1U) &&
                fill_applied_callback_entries(&derived_boundary),
            "derived lifecycle reaches the applied prefix")) {
        return false;
    }
    const runtime::RealtimePipelineCutResultV1 derived_polars_cut =
        pipeline->CutAndPublishGeneration(std::chrono::seconds(60));
    if (!Expect(
            derived_polars_cut.published() &&
                derived_polars_cut.store_generation != nullptr &&
                cut_matches_factor_contract(derived_polars_cut),
            "publish derived Polars lifecycle generation")) {
        return false;
    }
    const std::uint64_t derived_polars_generation =
        derived_polars_cut.store_generation->watermark().generation;
    if (!run_python_lines(
            "DERIVED_POLARS " +
                std::to_string(kDerivedInstrument) + " " +
                std::to_string(derived_polars_generation) +
                " 5 11001",
            "DERIVED_POLARS_SAMPLE ",
            1U,
            &polars_lines)) {
        return false;
    }
    std::uint64_t derived_first_entry_ns = 0U;
    std::uint64_t derived_last_entry_ns = 0U;
    std::uint64_t derived_polars_ready_ns = 0U;
    if (!Expect(
            polars_lines.size() == 1U &&
                ParseLineUnsignedField(
                    polars_lines.front(),
                    "first_callback_entry_ns",
                    &derived_first_entry_ns) &&
                ParseLineUnsignedField(
                    polars_lines.front(),
                    "last_callback_entry_ns",
                    &derived_last_entry_ns) &&
                ParseLineUnsignedField(
                    polars_lines.front(),
                    "polars_ready_ns",
                    &derived_polars_ready_ns) &&
                derived_first_entry_ns ==
                    derived_boundary.first_callback_entry_ns &&
                derived_last_entry_ns ==
                    derived_boundary.last_callback_entry_ns &&
                derived_boundary.first_call_start_ns <=
                    derived_first_entry_ns &&
                derived_first_entry_ns <=
                    derived_boundary.first_call_return_ns &&
                derived_boundary.last_call_start_ns <=
                    derived_last_entry_ns &&
                derived_last_entry_ns <=
                    derived_boundary.last_call_return_ns &&
                derived_polars_ready_ns >=
                    derived_last_entry_ns,
            "derived Polars callback boundaries reconcile")) {
        return false;
    }
    std::cout
        << "POLARS_BOUNDARY workload=derived_complete_order_lifecycle"
        << " scenario=" << StartupBenchmarkScenarioNameV1(scenario)
        << " generation=" << derived_polars_generation
        << " raw_records=4 derived_events=6"
        << " order_sequence_events=5 order_id=11001"
        << " final_revision=3 final_remaining_quantity=0"
        << " first_caller_before_callback_ns="
        << derived_boundary.first_call_start_ns
        << " last_caller_before_callback_ns="
        << derived_boundary.last_call_start_ns
        << " first_callback_entry_ns=" << derived_first_entry_ns
        << " last_callback_entry_ns=" << derived_last_entry_ns
        << " polars_ready_ns=" << derived_polars_ready_ns
        << " strict_first_callback_to_polars_ns="
        << (derived_polars_ready_ns -
            derived_boundary.first_call_start_ns)
        << " strict_last_callback_to_polars_ns="
        << (derived_polars_ready_ns -
            derived_boundary.last_call_start_ns)
        << '\n';

    if (callback_polars_only) {
        std::string response;
        if (!protocol->SendLine("QUIT") ||
            !protocol->ReadLine(
                std::chrono::seconds(30), &response) ||
            !response.starts_with("BYE ") ||
            !python.Wait(std::chrono::seconds(30))) {
            std::cerr
                << "callback-to-Polars probe shutdown: "
                << response << '\n';
            return false;
        }
        std::cout << "PYTHON_" << response << '\n';

        pipeline->StopAndDrain();
        const auto final = pipeline->Snapshot();
        std::uint64_t parallel_inline_messages = 0U;
        std::uint64_t parallel_farm_messages = 0U;
        std::size_t parallel_active_workers = 0U;
        for (const auto& source : final.parallel_decoder.sources) {
            parallel_inline_messages += source.inline_messages;
            parallel_farm_messages += source.farm_messages;
        }
        for (std::size_t worker = 0U;
             worker < final.parallel_decoder.worker_count;
             ++worker) {
            if (final.parallel_decoder.workers[worker].parsed_messages !=
                0U) {
                ++parallel_active_workers;
            }
        }
        std::cout
            << "CALLBACK_POLARS_TOPOLOGY enabled="
            << (final.parallel_decoder.enabled ? 1 : 0)
            << " scenario="
            << StartupBenchmarkScenarioNameV1(scenario)
            << " coverage_from_open="
            << (StartupBenchmarkCoverageFromOpenV1(scenario) ? 1 : 0)
            << " online_recovery=0"
            << " factor_generation_enabled="
            << (factor_generation_enabled ? 1 : 0)
            << " final_factor_generation_present="
            << (derived_polars_cut.factor_generation != nullptr ? 1 : 0)
            << " configured_workers="
            << parallel_decoder_worker_count
            << " reported_workers="
            << final.parallel_decoder.worker_count
            << " inline_messages=" << parallel_inline_messages
            << " farm_messages=" << parallel_farm_messages
            << " active_parse_workers=" << parallel_active_workers
            << " activation_configured_depth="
            << pipeline_config
                   .parallel_decoder_farm_activation_queue_depth
            << " activation_effective_depth="
            << EffectiveParallelDecoderFarmActivationDepthV1(
                   pipeline_config
                       .parallel_decoder_farm_activation_queue_depth,
                   pipeline_config.decoder_queue_capacity_per_source)
            << '\n';
        const bool complete =
            !pipeline->fatal() && !final.fatal &&
            final.accepted_messages == next_sequence - 1U &&
            final.rejected_messages == 0U &&
            final.post_cut_messages == 0U &&
            final.decoded_messages == next_sequence - 1U &&
            final.processing_progress.applied_sequence ==
                next_sequence - 1U &&
            final.store.appended_records == next_sequence - 1U &&
            final.store.failed_appends == 0U &&
            !final.store.coverage_lost &&
            derived_polars_cut.store_generation != nullptr &&
            derived_polars_cut.store_generation->record_count() ==
                next_sequence - 1U &&
            derived_polars_cut.store_generation->coverage_from_open() ==
                StartupBenchmarkCoverageFromOpenV1(scenario) &&
            !service->failed() && !history_stages.failed();
        service->MarkDraining();
        const bool stopped =
            service->MarkStoppedClean(final.tick_stream_sequence);
        service->StopControl();
        return Expect(
                   complete,
                   "callback-to-Polars benchmark retains its complete "
                   "accepted/applied prefix") &&
               Expect(
                   stopped,
                   "callback-to-Polars benchmark stops cleanly");
    }
    history_stages.Clear();

    auto cut_and_run_history =
        [&](std::string_view workload,
            std::uint32_t instrument_id,
            std::uint64_t expected_records,
            const CallbackBoundary& boundary) {
            const auto cut_start = MonotonicNowNs();
            const runtime::RealtimePipelineCutResultV1 cut =
                pipeline->CutAndPublishGeneration(
                    std::chrono::seconds(60));
            const auto cut_return = MonotonicNowNs();
            if (!Expect(
                    cut.published() &&
                        cut.store_generation != nullptr &&
                        cut_matches_factor_contract(cut),
                    "publish history generation for " +
                        std::string(workload))) {
                return false;
            }
            const std::uint64_t generation =
                cut.store_generation->watermark().generation;
            PythonHistoryCommandResult price{};
            history_stages.Clear();
            const std::string price_command =
                "HISTORY " + std::to_string(instrument_id) + " " +
                std::to_string(generation) + " price " +
                std::to_string(kPriceRepeats) + " " +
                std::to_string(expected_records);
            if (!RunPythonHistoryCommand(
                    protocol,
                    price_command,
                    "HISTORY_SAMPLE",
                    kPriceRepeats,
                    expected_records,
                    &price)) {
                return false;
            }
            PrintHistoryPageStages(
                std::string(workload) + "_price",
                history_stages.Take());
            print_python_distribution(
                std::string(workload) + "_price",
                price,
                expected_records);
            if (!Expect(
                    price.published_ns >=
                            boundary.applied_observed_ns &&
                        price.first_open_start_ns >=
                            price.published_ns &&
                        price.first_complete_ns >=
                            price.first_open_return_ns &&
                        boundary.applied_observed_ns >=
                            boundary.ipc_return_ns &&
                        boundary.ipc_return_ns >=
                            boundary.callback_start_ns,
                    "history t0..t4 timestamps are monotonic")) {
                return false;
            }
            std::cout
                << "HISTORY_BOUNDARY workload=" << workload
                << " generation=" << generation
                << " records=" << expected_records
                << " t0_callback_start_ns="
                << boundary.callback_start_ns
                << " t0_kind=caller_side_before_OnMessage"
                << " callback_return_ns="
                << boundary.callback_return_ns
                << " t1_applied_observed_ns="
                << boundary.applied_observed_ns
                << " t1_kind=applied_observation_upper_bound"
                << " store_ipc_visible_ns="
                << boundary.ipc_return_ns
                << " cut_call_start_ns=" << cut_start
                << " t2_generation_published_ns="
                << price.published_ns
                << " t2_kind=pre_release_publication_mark"
                << " cut_call_return_ns=" << cut_return
                << " t3_python_open_return_ns="
                << price.first_open_return_ns
                << " t4_eof_columns_complete_ns="
                << price.first_complete_ns
                << " realtime_processing_ns="
                << (boundary.applied_observed_ns -
                    boundary.callback_start_ns)
                << " realtime_ipc_visibility_ns="
                << (boundary.ipc_return_ns -
                    boundary.callback_start_ns)
                << " generation_publish_wait_ns="
                << (price.published_ns -
                    boundary.applied_observed_ns)
                << " publication_to_open_call_ns="
                << (price.first_open_start_ns -
                    price.published_ns)
                << " publication_to_open_return_ns="
                << (price.first_open_return_ns -
                    price.published_ns)
                << " first_complete_scan_ns="
                << (price.first_complete_ns -
                    price.first_scan_start_ns)
                << " open_return_to_complete_consumption_ns="
                << (price.first_complete_ns -
                    price.first_open_return_ns)
                << " end_to_end_complete_ns="
                << (price.first_complete_ns -
                    boundary.callback_start_ns)
                << '\n';

            PythonHistoryCommandResult all{};
            history_stages.Clear();
            const std::string all_command =
                "HISTORY " + std::to_string(instrument_id) + " " +
                std::to_string(generation) + " all " +
                std::to_string(kAllColumnRepeats) + " " +
                std::to_string(expected_records);
            if (!RunPythonHistoryCommand(
                    protocol,
                    all_command,
                    "HISTORY_SAMPLE",
                    kAllColumnRepeats,
                    expected_records,
                    &all)) {
                return false;
            }
            PrintHistoryPageStages(
                std::string(workload) + "_all",
                history_stages.Take());
            print_python_distribution(
                std::string(workload) + "_all",
                all,
                expected_records);
            return !history_stages.failed();
        };

    std::uint64_t pure_tick_count = 0U;
    CallbackBoundary pure_1k{};
    if (!Expect(
            append_until(
                &shanghai_tick,
                &pure_tick_count,
                1'000U,
                &pure_1k),
            "append pure tick history to 1k") ||
        !cut_and_run_history(
            "pure_tick_1k",
            kPureTickInstrument,
            1'000U,
            pure_1k)) {
        return false;
    }
    CallbackBoundary pure_10k{};
    if (!Expect(
            append_until(
                &shanghai_tick,
                &pure_tick_count,
                10'000U,
                &pure_10k),
            "append pure tick history to 10k") ||
        !cut_and_run_history(
            "pure_tick_10k",
            kPureTickInstrument,
            10'000U,
            pure_10k)) {
        return false;
    }
    CallbackBoundary pure_65536{};
    if (!Expect(
            append_until(
                &shanghai_tick,
                &pure_tick_count,
                65'536U,
                &pure_65536),
            "append pure tick history to 65,536") ||
        !cut_and_run_history(
            "pure_tick_65536",
            kPureTickInstrument,
            65'536U,
            pure_65536)) {
        return false;
    }

    const std::uint64_t generation_65536 =
        pipeline->Snapshot().last_published_generation;
    PythonHistoryCommandResult delta_origin{};
    history_stages.Clear();
    if (!RunPythonHistoryCommand(
            protocol,
            "DELTA_ORIGIN " +
                std::to_string(kPureTickInstrument) + " " +
                std::to_string(generation_65536) + " price " +
                std::to_string(kPriceRepeats) + " 65536",
            "DELTA_SAMPLE",
            kPriceRepeats,
            65'536U,
            &delta_origin)) {
        return false;
    }
    PrintHistoryPageStages(
        "delta_origin_65536_price",
        history_stages.Take());
    print_python_distribution(
        "delta_origin_65536_price",
        delta_origin,
        65'536U);
    PythonHistoryCommandResult worker_delta_origin{};
    history_stages.Clear();
    if (!RunPythonHistoryCommand(
            protocol,
            "WORKER_DELTA_ORIGIN " +
                std::to_string(kPureTickInstrument) + " " +
                std::to_string(generation_65536) + " price " +
                std::to_string(kPriceRepeats) + " 65536",
            "WORKER_DELTA_SAMPLE",
            kPriceRepeats,
            65'536U,
            &worker_delta_origin)) {
        return false;
    }
    PrintHistoryPageStages(
        "worker_delta_origin_65536_price",
        history_stages.Take());
    print_python_distribution(
        "worker_delta_origin_65536_price",
        worker_delta_origin,
        65'536U);
    PrintLatency(
        "worker_delta_origin_65536_wire_page_read",
        worker_delta_origin.worker_page_read_ns);
    PrintLatency(
        "worker_delta_origin_65536_pipeline",
        worker_delta_origin.worker_pipeline_ns);
    PrintLatency(
        "worker_delta_origin_65536_selected_column_tuple_materialize",
        worker_delta_origin.selected_column_tuple_materialize_ns);
    PrintLatency(
        "worker_delta_origin_65536_summed_ring_publish_to_validated_ready",
        worker_delta_origin
            .summed_ring_publish_to_validated_ready_ns);
    PrintLatency(
        "worker_delta_origin_65536_parent_complete_consumption",
        worker_delta_origin.parent_complete_consumption_ns);

    auto cut_without_history =
        [&](std::uint64_t target_tick_count,
            CallbackBoundary* boundary,
            std::uint64_t* generation) {
            if (!append_until(
                    &shanghai_tick,
                    &pure_tick_count,
                    target_tick_count,
                    boundary)) {
                return false;
            }
            const auto cut = pipeline->CutAndPublishGeneration(
                std::chrono::seconds(60));
            if (!cut.published() ||
                cut.store_generation == nullptr ||
                !cut_matches_factor_contract(cut)) {
                const auto state = pipeline->Snapshot();
                std::cerr
                    << "delta cut failed target_tick_count="
                    << target_tick_count
                    << " cut_error="
                    << runtime::RealtimePipelineCutErrorNameV1(
                           cut.error)
                    << " generation_error="
                    << static_cast<unsigned int>(
                           cut.generation_error)
                    << " accepted=" << state.accepted_messages
                    << " applied="
                    << state.processing_progress.applied_sequence
                    << " last_started="
                    << state.last_started_generation
                    << " last_published="
                    << state.last_published_generation
                    << " service_failed=" << service->failed()
                    << " fatal=" << state.fatal << '\n';
                return false;
            }
            *generation =
                cut.store_generation->watermark().generation;
            return true;
        };
    CallbackBoundary delta_price_boundary{};
    std::uint64_t delta_price_generation = 0U;
    if (!Expect(
            cut_without_history(
                69'632U,
                &delta_price_boundary,
                &delta_price_generation),
            "publish 4,096-record verified delta target")) {
        return false;
    }
    PythonHistoryCommandResult delta_price{};
    history_stages.Clear();
    if (!RunPythonHistoryCommand(
            protocol,
            "DELTA_FROM_VERIFIED " +
                std::to_string(kPureTickInstrument) + " " +
                std::to_string(delta_price_generation) +
                " price " + std::to_string(kPriceRepeats) +
                " 4096",
            "DELTA_SAMPLE",
            kPriceRepeats,
            4'096U,
            &delta_price)) {
        return false;
    }
    PrintHistoryPageStages(
        "delta_verified_4096_price",
        history_stages.Take());
    print_python_distribution(
        "delta_verified_4096_price",
        delta_price,
        4'096U);
    if (!Expect(
            delta_price.published_ns >=
                    delta_price_boundary.applied_observed_ns &&
                delta_price.first_open_start_ns >=
                    delta_price.published_ns &&
                delta_price.first_complete_ns >=
                    delta_price.first_cursor_open_return_ns &&
                delta_price_boundary.applied_observed_ns >=
                    delta_price_boundary.ipc_return_ns &&
                delta_price_boundary.ipc_return_ns >=
                    delta_price_boundary.callback_start_ns,
            "direct delta callback-to-consumption times are monotonic")) {
        return false;
    }
    std::cout
        << "DELTA_BOUNDARY workload=verified_4096_price"
        << " generation=" << delta_price_generation
        << " realtime_processing_ns="
        << (delta_price_boundary.applied_observed_ns -
            delta_price_boundary.callback_start_ns)
        << " realtime_ipc_visibility_ns="
        << (delta_price_boundary.ipc_return_ns -
            delta_price_boundary.callback_start_ns)
        << " generation_publish_wait_ns="
        << (delta_price.published_ns -
            delta_price_boundary.applied_observed_ns)
        << " t3_delta_session_open_return_ns="
        << delta_price.first_open_return_ns
        << " t3_delta_cursor_open_return_ns="
        << delta_price.first_cursor_open_return_ns
        << " publication_to_verified_checkpoint_ns="
        << (delta_price.first_complete_ns -
            delta_price.published_ns)
        << " cursor_open_return_to_verified_checkpoint_ns="
        << (delta_price.first_complete_ns -
            delta_price.first_cursor_open_return_ns)
        << " end_to_end_verified_checkpoint_ns="
        << (delta_price.first_complete_ns -
            delta_price_boundary.callback_start_ns)
        << '\n';

    // Advance the independent worker checkpoint to the direct benchmark's
    // generation.  This single unreported synchronization scan lets the
    // next generation measure worker callback-to-complete latency without
    // including the preceding direct benchmark.
    PythonHistoryCommandResult worker_delta_sync{};
    history_stages.Clear();
    if (!RunPythonHistoryCommand(
            protocol,
            "WORKER_DELTA_FROM_VERIFIED " +
                std::to_string(kPureTickInstrument) + " " +
                std::to_string(delta_price_generation) +
                " validate 1 4096",
            "WORKER_DELTA_SAMPLE",
            1U,
            4'096U,
            &worker_delta_sync) ||
        history_stages.failed()) {
        return false;
    }
    history_stages.Clear();

    CallbackBoundary delta_all_boundary{};
    std::uint64_t delta_all_generation = 0U;
    if (!Expect(
            cut_without_history(
                73'728U,
                &delta_all_boundary,
                &delta_all_generation),
            "publish second 4,096-record delta target")) {
        return false;
    }
    PythonHistoryCommandResult worker_delta_price{};
    history_stages.Clear();
    if (!RunPythonHistoryCommand(
            protocol,
            "WORKER_DELTA_FROM_VERIFIED " +
                std::to_string(kPureTickInstrument) + " " +
                std::to_string(delta_all_generation) +
                " price " + std::to_string(kPriceRepeats) +
                " 4096",
            "WORKER_DELTA_SAMPLE",
            kPriceRepeats,
            4'096U,
            &worker_delta_price)) {
        return false;
    }
    PrintHistoryPageStages(
        "worker_delta_verified_4096_price",
        history_stages.Take());
    print_python_distribution(
        "worker_delta_verified_4096_price",
        worker_delta_price,
        4'096U);
    PrintLatency(
        "worker_delta_verified_4096_wire_page_read",
        worker_delta_price.worker_page_read_ns);
    PrintLatency(
        "worker_delta_verified_4096_pipeline",
        worker_delta_price.worker_pipeline_ns);
    PrintLatency(
        "worker_delta_verified_4096_selected_column_tuple_materialize",
        worker_delta_price.selected_column_tuple_materialize_ns);
    PrintLatency(
        "worker_delta_verified_4096_summed_ring_publish_to_validated_ready",
        worker_delta_price
            .summed_ring_publish_to_validated_ready_ns);
    PrintLatency(
        "worker_delta_verified_4096_parent_complete_consumption",
        worker_delta_price.parent_complete_consumption_ns);
    if (!Expect(
            worker_delta_price.published_ns >=
                    delta_all_boundary.applied_observed_ns &&
                worker_delta_price.first_open_start_ns >=
                    worker_delta_price.published_ns &&
                worker_delta_price.first_complete_ns >=
                    worker_delta_price.first_open_return_ns &&
                delta_all_boundary.applied_observed_ns >=
                    delta_all_boundary.ipc_return_ns &&
                delta_all_boundary.ipc_return_ns >=
                    delta_all_boundary.callback_start_ns,
            "worker delta callback-to-consumption times are monotonic")) {
        return false;
    }
    std::cout
        << "WORKER_DELTA_BOUNDARY workload=verified_4096_price"
        << " generation=" << delta_all_generation
        << " worker_page_read_ns="
        << worker_delta_price.worker_page_read_ns.front()
        << " worker_pipeline_ns="
        << worker_delta_price.worker_pipeline_ns.front()
        << " parent_selected_column_tuple_materialize_ns="
        << worker_delta_price
               .selected_column_tuple_materialize_ns.front()
        << " summed_ring_publish_to_validated_ready_ns="
        << worker_delta_price
               .summed_ring_publish_to_validated_ready_ns.front()
        << " parent_complete_consumption_ns="
        << worker_delta_price
               .parent_complete_consumption_ns.front()
        << " generation_publish_wait_ns="
        << (worker_delta_price.published_ns -
            delta_all_boundary.applied_observed_ns)
        << " publication_to_open_call_ns="
        << (worker_delta_price.first_open_start_ns -
            worker_delta_price.published_ns)
        << " publication_to_complete_ns="
        << (worker_delta_price.first_complete_ns -
            worker_delta_price.published_ns)
        << " callback_to_complete_ns="
        << (worker_delta_price.first_complete_ns -
            delta_all_boundary.callback_start_ns)
        << '\n';

    PythonHistoryCommandResult delta_all{};
    history_stages.Clear();
    if (!RunPythonHistoryCommand(
            protocol,
            "DELTA_FROM_VERIFIED " +
                std::to_string(kPureTickInstrument) + " " +
                std::to_string(delta_all_generation) +
                " all " + std::to_string(kAllColumnRepeats) +
                " 4096",
            "DELTA_SAMPLE",
            kAllColumnRepeats,
            4'096U,
            &delta_all)) {
        return false;
    }
    PrintHistoryPageStages(
        "delta_verified_4096_all",
        history_stages.Take());
    print_python_distribution(
        "delta_verified_4096_all",
        delta_all,
        4'096U);

    CallbackBoundary delta_validate_boundary{};
    std::uint64_t delta_validate_generation = 0U;
    if (!Expect(
            cut_without_history(
                77'824U,
                &delta_validate_boundary,
                &delta_validate_generation),
            "publish 4,096-record validation-only delta target")) {
        return false;
    }
    PythonHistoryCommandResult delta_validate{};
    history_stages.Clear();
    if (!RunPythonHistoryCommand(
            protocol,
            "DELTA_FROM_VERIFIED " +
                std::to_string(kPureTickInstrument) + " " +
                std::to_string(delta_validate_generation) +
                " validate " + std::to_string(kPriceRepeats) +
                " 4096",
            "DELTA_SAMPLE",
            kPriceRepeats,
            4'096U,
            &delta_validate)) {
        return false;
    }
    PrintHistoryPageStages(
        "delta_verified_4096_validate",
        history_stages.Take());
    print_python_distribution(
        "delta_verified_4096_validate",
        delta_validate,
        4'096U);

    constexpr std::uint64_t kRollingWindowRecords = 4'096U;
    PythonHistoryCommandResult rolling_origin{};
    history_stages.Clear();
    if (!RunPythonHistoryCommand(
            protocol,
            "ROLLING_ORIGIN " +
                std::to_string(kPureTickInstrument) + " " +
                std::to_string(delta_validate_generation) +
                " price " + std::to_string(kPriceRepeats) +
                " 77824 " +
                std::to_string(kRollingWindowRecords),
            "ROLLING_SAMPLE",
            kPriceRepeats,
            77'824U,
            &rolling_origin)) {
        return false;
    }
    PrintHistoryPageStages(
        "rolling_origin_77824_price",
        history_stages.Take());
    print_python_distribution(
        "rolling_origin_77824_price",
        rolling_origin,
        77'824U);

    CallbackBoundary rolling_boundary{};
    std::uint64_t rolling_generation = 0U;
    if (!Expect(
            cut_without_history(
                81'920U,
                &rolling_boundary,
                &rolling_generation),
            "publish 4,096-record rolling delta target")) {
        return false;
    }
    PythonHistoryCommandResult rolling_delta{};
    history_stages.Clear();
    if (!RunPythonHistoryCommand(
            protocol,
            "ROLLING_FROM_VERIFIED " +
                std::to_string(kPureTickInstrument) + " " +
                std::to_string(rolling_generation) +
                " price " + std::to_string(kPriceRepeats) +
                " 4096 " +
                std::to_string(kRollingWindowRecords),
            "ROLLING_SAMPLE",
            kPriceRepeats,
            4'096U,
            &rolling_delta)) {
        return false;
    }
    PrintHistoryPageStages(
        "rolling_verified_4096_price",
        history_stages.Take());
    print_python_distribution(
        "rolling_verified_4096_price",
        rolling_delta,
        4'096U);
    std::cout
        << "ROLLING_BOUNDARY workload=verified_4096_price"
        << " generation=" << rolling_generation
        << " realtime_processing_ns="
        << (rolling_boundary.applied_observed_ns -
            rolling_boundary.callback_start_ns)
        << " realtime_ipc_visibility_ns="
        << (rolling_boundary.ipc_return_ns -
            rolling_boundary.callback_start_ns)
        << " generation_publish_wait_ns="
        << (rolling_delta.published_ns -
            rolling_boundary.applied_observed_ns)
        << " t3_delta_session_open_return_ns="
        << rolling_delta.first_open_return_ns
        << " t3_delta_cursor_open_return_ns="
        << rolling_delta.first_cursor_open_return_ns
        << " publication_to_atomic_commit_ns="
        << (rolling_delta.first_complete_ns -
            rolling_delta.published_ns)
        << " cursor_open_return_to_atomic_commit_ns="
        << (rolling_delta.first_complete_ns -
            rolling_delta.first_cursor_open_return_ns)
        << " callback_to_atomic_commit_ns="
        << (rolling_delta.first_complete_ns -
            rolling_boundary.callback_start_ns)
        << '\n';

    std::uint64_t mixed_tick_count = 0U;
    CallbackBoundary mixed_boundary{};
    if (!Expect(
            append_until(
                &shenzhen_order,
                &mixed_tick_count,
                65'535U,
                &mixed_boundary),
            "append mixed snapshot/tick history")) {
        return false;
    }
    const auto mixed_cut = pipeline->CutAndPublishGeneration(
        std::chrono::seconds(60));
    if (!Expect(
            mixed_cut.published() &&
                mixed_cut.store_generation != nullptr &&
                cut_matches_factor_contract(mixed_cut),
            "publish mixed history generation")) {
        return false;
    }
    const std::uint64_t mixed_generation =
        mixed_cut.store_generation->watermark().generation;
    PythonHistoryCommandResult mixed_price{};
    history_stages.Clear();
    if (!RunPythonHistoryCommand(
            protocol,
            "HISTORY " + std::to_string(kMixedInstrument) + " " +
                std::to_string(mixed_generation) + " price " +
                std::to_string(kPriceRepeats) + " 65536",
            "HISTORY_SAMPLE",
            kPriceRepeats,
            65'536U,
            &mixed_price)) {
        return false;
    }
    PrintHistoryPageStages(
        "mixed_65536_price",
        history_stages.Take());
    print_python_distribution(
        "mixed_65536_price",
        mixed_price,
        65'536U);
    std::cout
        << "HISTORY_BOUNDARY workload=mixed_65536"
        << " generation=" << mixed_generation
        << " records=65536 snapshots=1 ticks=65535"
        << " realtime_processing_ns="
        << (mixed_boundary.applied_observed_ns -
            mixed_boundary.callback_start_ns)
        << " realtime_ipc_visibility_ns="
        << (mixed_boundary.ipc_return_ns -
            mixed_boundary.callback_start_ns)
        << " generation_publish_wait_ns="
        << (mixed_price.published_ns -
            mixed_boundary.applied_observed_ns)
        << " publication_to_open_return_ns="
        << (mixed_price.first_open_return_ns -
            mixed_price.published_ns)
        << " complete_scan_ns="
        << (mixed_price.first_complete_ns -
            mixed_price.first_scan_start_ns)
        << " open_return_to_complete_consumption_ns="
        << (mixed_price.first_complete_ns -
            mixed_price.first_open_return_ns)
        << " end_to_end_complete_ns="
        << (mixed_price.first_complete_ns -
            mixed_boundary.callback_start_ns)
        << '\n';
    PythonHistoryCommandResult mixed_all{};
    history_stages.Clear();
    if (!RunPythonHistoryCommand(
            protocol,
            "HISTORY " + std::to_string(kMixedInstrument) + " " +
                std::to_string(mixed_generation) + " all " +
                std::to_string(kAllColumnRepeats) + " 65536",
            "HISTORY_SAMPLE",
            kAllColumnRepeats,
            65'536U,
            &mixed_all)) {
        return false;
    }
    PrintHistoryPageStages(
        "mixed_65536_all",
        history_stages.Take());
    print_python_distribution(
        "mixed_65536_all",
        mixed_all,
        65'536U);

    struct LatestSeriesResult final {
        std::vector<std::uint64_t> strict_callback_to_python_ns;
        std::vector<std::uint64_t> wire_recv_to_python_ns;
        std::vector<std::uint64_t> callback_call_ns;
        std::uint64_t polls = 0U;
        std::uint64_t inconsistent_retries = 0U;
    };
    FakeSdkMessage live_snapshot(
        sdk::kProductionMessageKeysV1[2U],
        PipelineShenzhenSnapshotBody("000001"));
    auto run_latest_series =
        [&](std::string_view workload,
            std::size_t count,
            LatestSeriesResult* output) {
            if (count == 0U || output == nullptr) {
                return false;
            }
            const std::uint64_t first = next_sequence;
            if (!protocol->SendLine(
                    "LATEST_SERIES " +
                    std::to_string(kMixedInstrument) + " " +
                    std::to_string(first) + " " +
                    std::to_string(count))) {
                return false;
            }
            std::string response;
            if (!protocol->ReadLine(
                    std::chrono::seconds(30), &response) ||
                !response.starts_with("LATEST_ARMED ")) {
                std::cerr << "latest series arm: " << response
                          << '\n';
                return false;
            }
            std::cout << "PYTHON_" << response << '\n';
            LatestSeriesResult result{};
            result.strict_callback_to_python_ns.reserve(count);
            result.wire_recv_to_python_ns.reserve(count);
            result.callback_call_ns.reserve(count);
            for (std::size_t index = 0U; index < count; ++index) {
                const std::uint64_t expected = next_sequence;
                const std::uint64_t start = MonotonicNowNs();
                handler->OnMessage(nullptr, &live_snapshot);
                const std::uint64_t returned = MonotonicNowNs();
                ++next_sequence;
                if (!protocol->ReadLine(
                        std::chrono::seconds(30), &response) ||
                    !response.starts_with("LATEST_SAMPLE ")) {
                    std::cerr << "latest sample: " << response
                              << '\n';
                    return false;
                }
                std::uint64_t reported_expected = 0U;
                std::uint64_t seen = 0U;
                std::uint64_t recv = 0U;
                std::uint64_t polls = 0U;
                std::uint64_t inconsistent = 0U;
                if (!ParseLineUnsignedField(
                        response,
                        "expected",
                        &reported_expected) ||
                    reported_expected != expected ||
                    !ParseLineUnsignedField(
                        response, "seen_ns", &seen) ||
                    !ParseLineUnsignedField(
                        response,
                        "wire_recv_monotonic_ns",
                        &recv) ||
                    !ParseLineUnsignedField(
                        response, "polls", &polls) ||
                    !ParseLineUnsignedField(
                        response,
                        "inconsistent_retries",
                        &inconsistent) ||
                    start == 0U || returned < start ||
                    recv < start || seen < recv) {
                    std::cerr << "invalid latest sample: "
                              << response << '\n';
                    return false;
                }
                result.strict_callback_to_python_ns.push_back(
                    seen - start);
                result.wire_recv_to_python_ns.push_back(
                    seen - recv);
                result.callback_call_ns.push_back(returned - start);
                result.polls += polls;
                result.inconsistent_retries += inconsistent;
            }
            if (!protocol->ReadLine(
                    std::chrono::seconds(30), &response) ||
                !response.starts_with("DONE ")) {
                std::cerr << "latest series DONE: " << response
                          << '\n';
                return false;
            }
            std::cout << "PYTHON_" << response << '\n';
            PrintLatency(
                std::string(workload) +
                    "_strict_callback_to_python_latest",
                result.strict_callback_to_python_ns);
            PrintLatency(
                std::string(workload) +
                    "_wire_recv_to_python_latest",
                result.wire_recv_to_python_ns);
            PrintLatency(
                std::string(workload) + "_callback_call",
                result.callback_call_ns);
            std::cout
                << "LATEST_SERIES_TOTAL workload=" << workload
                << " samples=" << count
                << " total_polls=" << result.polls
                << " inconsistent_retries="
                << result.inconsistent_retries << '\n';
            *output = std::move(result);
            return true;
        };

    constexpr std::size_t kInterferenceSamples = 1'000U;
    LatestSeriesResult latest_baseline{};
    if (!run_latest_series(
            "latest_baseline",
            kInterferenceSamples,
            &latest_baseline)) {
        return false;
    }

    std::string line;
    history_stages.Clear();
    std::uint64_t worker_loop_pid = 0U;
    std::uint64_t worker_loop_start_scans = 0U;
    std::uint64_t worker_loop_start_records = 0U;
    if (!protocol->SendLine(
            "START_HISTORY_LOOP " +
            std::to_string(kPureTickInstrument) + " " +
            std::to_string(mixed_generation) +
            " all 81920")) {
        return false;
    }
    if (!protocol->ReadLine(
            std::chrono::seconds(120), &line) ||
        !line.starts_with("HISTORY_LOOP_STARTED ")) {
        std::cerr << "history loop start: " << line << '\n';
        return false;
    }
    std::cout << "PYTHON_" << line << '\n';
    LatestSeriesResult latest_with_same_process_scan{};
    if (!run_latest_series(
            "latest_with_same_process_history_scan",
            kInterferenceSamples,
            &latest_with_same_process_scan)) {
        return false;
    }
    if (!protocol->SendLine("STOP_HISTORY_LOOP") ||
        !protocol->ReadLine(
            std::chrono::seconds(120), &line) ||
        !line.starts_with("HISTORY_LOOP_STOPPED ")) {
        std::cerr << "history loop stop: " << line << '\n';
        return false;
    }
    std::cout << "PYTHON_" << line << '\n';
    PrintHistoryPageStages(
        "same_process_concurrent_scan_loop",
        history_stages.Take());

    history_stages.Clear();
    if (!protocol->SendLine(
            "START_WORKER_DELTA_LOOP " +
            std::to_string(kPureTickInstrument) + " " +
            std::to_string(mixed_generation) +
            " 81920") ||
        !protocol->ReadLine(
            std::chrono::seconds(120), &line) ||
        !line.starts_with("WORKER_DELTA_LOOP_STARTED ") ||
        !ParseLineUnsignedField(
            line, "worker_pid", &worker_loop_pid) ||
        !ParseLineUnsignedField(
            line, "scans", &worker_loop_start_scans) ||
        !ParseLineUnsignedField(
            line, "records", &worker_loop_start_records) ||
        worker_loop_pid == 0U ||
        worker_loop_start_scans != 1U ||
        worker_loop_start_records != 81'920U) {
        std::cerr << "isolated worker delta loop start: "
                  << line << '\n';
        return false;
    }
    std::cout << "PYTHON_" << line << '\n';
    LatestSeriesResult latest_with_worker_scan{};
    if (!run_latest_series(
            "latest_with_isolated_worker_delta_tuple_materialization",
            kInterferenceSamples,
            &latest_with_worker_scan)) {
        return false;
    }
    std::uint64_t worker_loop_stop_pid = 0U;
    std::uint64_t worker_loop_stop_scans = 0U;
    std::uint64_t worker_loop_stop_records = 0U;
    if (!protocol->SendLine("STOP_WORKER_DELTA_LOOP") ||
        !protocol->ReadLine(
            std::chrono::seconds(120), &line) ||
        !line.starts_with("WORKER_DELTA_LOOP_STOPPED ") ||
        !ParseLineUnsignedField(
            line, "worker_pid", &worker_loop_stop_pid) ||
        !ParseLineUnsignedField(
            line, "scans", &worker_loop_stop_scans) ||
        !ParseLineUnsignedField(
            line, "records", &worker_loop_stop_records) ||
        worker_loop_stop_pid != worker_loop_pid ||
        worker_loop_stop_scans < 2U ||
        worker_loop_stop_scans >
            std::numeric_limits<std::uint64_t>::max() /
                81'920U ||
        worker_loop_stop_records !=
            worker_loop_stop_scans * 81'920U) {
        std::cerr << "isolated worker delta loop stop: "
                  << line << '\n';
        return false;
    }
    std::cout << "PYTHON_" << line << '\n';
    PrintHistoryPageStages(
        "isolated_worker_delta_tuple_materialization_loop",
        history_stages.Take());
    LatestSeriesResult latest_baseline_after{};
    if (!run_latest_series(
            "latest_baseline_after_worker_scan",
            kInterferenceSamples,
            &latest_baseline_after)) {
        return false;
    }

    const LatencySummary baseline_before = SummarizeLatency(
        latest_baseline.strict_callback_to_python_ns);
    const LatencySummary baseline_after = SummarizeLatency(
        latest_baseline_after.strict_callback_to_python_ns);
    LatencySummary baseline{};
    baseline.p50_ns = std::max(
        baseline_before.p50_ns, baseline_after.p50_ns);
    baseline.p95_ns = std::max(
        baseline_before.p95_ns, baseline_after.p95_ns);
    baseline.p99_ns = std::max(
        baseline_before.p99_ns, baseline_after.p99_ns);
    const LatencySummary with_same_process_scan = SummarizeLatency(
        latest_with_same_process_scan
            .strict_callback_to_python_ns);
    const LatencySummary with_worker_scan = SummarizeLatency(
        latest_with_worker_scan
            .strict_callback_to_python_ns);
    auto ratio = [](std::uint64_t numerator,
                    std::uint64_t denominator) {
        return denominator == 0U
                   ? 0.0L
                   : static_cast<long double>(numerator) /
                         static_cast<long double>(denominator);
    };
    std::cout
        << "HISTORY_SCAN_INTERFERENCE mode=same_python_process"
        << " samples="
        << kInterferenceSamples
        << " baseline_p50_ns=" << baseline.p50_ns
        << " scan_p50_ns=" << with_same_process_scan.p50_ns
        << " p50_ratio=" << std::fixed << std::setprecision(3)
        << ratio(
               with_same_process_scan.p50_ns,
               baseline.p50_ns)
        << " baseline_p95_ns=" << baseline.p95_ns
        << " scan_p95_ns=" << with_same_process_scan.p95_ns
        << " p95_ratio="
        << ratio(
               with_same_process_scan.p95_ns,
               baseline.p95_ns)
        << " baseline_p99_ns=" << baseline.p99_ns
        << " scan_p99_ns=" << with_same_process_scan.p99_ns
        << " p99_ratio="
        << ratio(
               with_same_process_scan.p99_ns,
               baseline.p99_ns)
        << std::defaultfloat << '\n';
    std::cout
        << "HISTORY_SCAN_INTERFERENCE"
        << " mode=isolated_worker_fixed_result_ring_tuple_materialization"
        << " samples=" << kInterferenceSamples
        << " baseline_p50_ns=" << baseline.p50_ns
        << " scan_p50_ns=" << with_worker_scan.p50_ns
        << " p50_ratio=" << std::fixed << std::setprecision(3)
        << ratio(with_worker_scan.p50_ns, baseline.p50_ns)
        << " baseline_p95_ns=" << baseline.p95_ns
        << " scan_p95_ns=" << with_worker_scan.p95_ns
        << " p95_ratio="
        << ratio(with_worker_scan.p95_ns, baseline.p95_ns)
        << " baseline_p99_ns=" << baseline.p99_ns
        << " scan_p99_ns=" << with_worker_scan.p99_ns
        << " p99_ratio="
        << ratio(with_worker_scan.p99_ns, baseline.p99_ns)
        << std::defaultfloat << '\n';
    auto saturating_add = [](std::uint64_t value,
                             std::uint64_t increment) {
        return value >
                       std::numeric_limits<std::uint64_t>::max() -
                           increment
                   ? std::numeric_limits<std::uint64_t>::max()
                   : value + increment;
    };
    auto saturating_double = [](std::uint64_t value) {
        return value >
                       std::numeric_limits<std::uint64_t>::max() /
                           2U
                   ? std::numeric_limits<std::uint64_t>::max()
                   : value * 2U;
    };
    const std::uint64_t worker_p95_limit = std::max(
        saturating_double(baseline.p95_ns),
        saturating_add(baseline.p95_ns, 250'000U));
    const std::uint64_t worker_p99_limit = std::max(
        saturating_double(baseline.p99_ns),
        saturating_add(baseline.p99_ns, 500'000U));
    if (!Expect(
            baseline.p95_ns > 0U && baseline.p99_ns > 0U &&
                with_worker_scan.p95_ns <= worker_p95_limit &&
                with_worker_scan.p99_ns <= worker_p99_limit,
            "isolated worker scan preserves latest p95/p99 bounds")) {
        std::cerr
            << "worker latest limits p95_limit_ns="
            << worker_p95_limit
            << " actual_p95_ns=" << with_worker_scan.p95_ns
            << " p99_limit_ns=" << worker_p99_limit
            << " actual_p99_ns=" << with_worker_scan.p99_ns
            << '\n';
        return false;
    }
    if (!Expect(
            wait_prefix(next_sequence - 1U),
            "concurrent latest samples reach the applied prefix")) {
        return false;
    }

    if (!protocol->SendLine("QUIT")) {
        return false;
    }
    if (!protocol->ReadLine(
            std::chrono::seconds(30), &line) ||
        !line.starts_with("BYE ") ||
        !python.Wait(std::chrono::seconds(30))) {
        std::cerr << "history probe shutdown: " << line << '\n';
        return false;
    }
    std::cout << "PYTHON_" << line << '\n';

    pipeline->StopAndDrain();
    const auto final = pipeline->Snapshot();
    const bool ok = Expect(
        !pipeline->fatal() && !final.fatal &&
            final.accepted_messages == next_sequence - 1U &&
            final.processing_progress.applied_sequence ==
                next_sequence - 1U &&
            !service->failed() && !history_stages.failed(),
        "history benchmark retains its complete accepted/applied prefix");
    service->MarkDraining();
    const bool stopped =
        service->MarkStoppedClean(final.tick_stream_sequence);
    service->StopControl();
    return ok &&
           Expect(stopped, "history benchmark stops cleanly");
}

}  // namespace

int main(int argc, char** argv) {
    auto parse_unsigned = [](
                              std::string_view text,
                              std::uint64_t* output) {
        if (output == nullptr || text.empty()) {
            return false;
        }
        std::uint64_t value = 0U;
        const auto result = std::from_chars(
            text.data(), text.data() + text.size(), value);
        if (result.ec != std::errc{} ||
            result.ptr != text.data() + text.size()) {
            return false;
        }
        *output = value;
        return true;
    };
    auto parse_scenario = [](
                              std::string_view text,
                              StartupBenchmarkScenarioV1* output) {
        if (output == nullptr) {
            return false;
        }
        if (text == "from_open") {
            *output = StartupBenchmarkScenarioV1::kFromOpen;
            return true;
        }
        if (text == "live_partial_no_recovery") {
            *output =
                StartupBenchmarkScenarioV1::kLivePartialNoRecovery;
            return true;
        }
        return false;
    };
    if ((argc == 13 || argc == 14 || argc == 15) &&
        std::string_view(argv[1]) ==
            "--throughput-profile-benchmark") {
        std::array<std::uint64_t, 8U> numeric{};
        for (std::size_t index = 0U;
             index < numeric.size();
             ++index) {
            if (!parse_unsigned(argv[index + 2U], &numeric[index])) {
                std::cerr
                    << "invalid throughput profile numeric argument\n";
                return 2;
            }
        }
        if (numeric[1U] > static_cast<std::uint64_t>(
                std::numeric_limits<
                    std::chrono::milliseconds::rep>::max()) ||
            numeric[2U] >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max()) ||
            numeric[3U] >
                std::numeric_limits<std::uint32_t>::max() ||
            numeric[4U] >
                std::numeric_limits<std::uint32_t>::max() ||
            numeric[5U] > 1U ||
            numeric[6U] >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max()) ||
            numeric[7U] >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max())) {
            std::cerr << "throughput profile argument out of range\n";
            return 2;
        }
        std::uint64_t segment_kib = 0U;
        if (!parse_unsigned(argv[10], &segment_kib) ||
            segment_kib >
                std::numeric_limits<std::uint32_t>::max()) {
            std::cerr << "invalid throughput segment argument\n";
            return 2;
        }
        ThroughputWorkloadV1 workload{};
        const std::string_view workload_text(argv[11]);
        if (workload_text == "single_instrument") {
            workload = ThroughputWorkloadV1::kSingleInstrument;
        } else if (workload_text == "five_tuple_uniform") {
            workload = ThroughputWorkloadV1::kFiveTupleUniform;
        } else if (workload_text == "four_source_balanced") {
            workload = ThroughputWorkloadV1::kFourSourceBalanced;
        } else if (workload_text == "hot_shenzhen_tick_source") {
            workload =
                ThroughputWorkloadV1::kHotShenzhenTickSource;
        } else {
            std::cerr << "invalid throughput workload\n";
            return 2;
        }
        ThroughputSinkV1 sink{};
        const std::string_view sink_text(argv[12]);
        if (sink_text == "fast") {
            sink = ThroughputSinkV1::kFast;
        } else if (sink_text == "fast_certified") {
            sink = ThroughputSinkV1::kFastAndCertified;
        } else {
            std::cerr << "invalid throughput sink\n";
            return 2;
        }
        ThroughputBenchmarkConfigV1 config{};
        config.target_rate = numeric[0U];
        config.duration = std::chrono::milliseconds(
            static_cast<std::chrono::milliseconds::rep>(numeric[1U]));
        config.instruments_per_market =
            static_cast<std::size_t>(numeric[2U]);
        config.store_worker_count =
            static_cast<std::uint32_t>(numeric[3U]);
        config.parallel_decoder_worker_count =
            static_cast<std::uint32_t>(numeric[4U]);
        config.parallel_decoder_idle_inline_enabled =
            numeric[5U] == 1U;
        config.decoder_queue_capacity_per_source =
            static_cast<std::size_t>(numeric[6U]);
        config.store_queue_capacity_per_source_worker =
            static_cast<std::size_t>(numeric[7U]);
        config.segment_kib =
            static_cast<std::uint32_t>(segment_kib);
        config.workload = workload;
        config.sink = sink;
        if (argc >= 14 &&
            !parse_scenario(argv[13], &config.scenario)) {
            std::cerr << "invalid throughput startup scenario\n";
            return 2;
        }
        if (argc == 15) {
            std::uint64_t generation_interval_ms = 0U;
            if (!parse_unsigned(
                    argv[14], &generation_interval_ms) ||
                generation_interval_ms == 0U ||
                generation_interval_ms > 60'000U ||
                generation_interval_ms >
                    static_cast<std::uint64_t>(
                        std::numeric_limits<std::chrono::
                                milliseconds::rep>::max())) {
                std::cerr
                    << "invalid throughput generation interval\n";
                return 2;
            }
            config.generation_interval =
                std::chrono::milliseconds(
                    static_cast<std::chrono::milliseconds::rep>(
                        generation_interval_ms));
        }
        return RunThroughputProfileBenchmark(config) ? 0 : 1;
    }
    if ((argc == 3 || argc == 4) &&
        std::string_view(argv[1]) ==
            "--history-latency-benchmark-workers") {
        std::uint64_t worker_count = 0U;
        if (!parse_unsigned(argv[2], &worker_count) ||
            worker_count >
                std::numeric_limits<std::uint32_t>::max()) {
            std::cerr << "invalid parallel decoder worker count\n";
            return 2;
        }
        StartupBenchmarkScenarioV1 scenario =
            StartupBenchmarkScenarioV1::kFromOpen;
        if (argc == 4 && !parse_scenario(argv[3], &scenario)) {
            std::cerr << "invalid history startup scenario\n";
            return 2;
        }
        return RunHistoryLatencyBenchmark(
                   static_cast<std::uint32_t>(worker_count),
                   false,
                   scenario)
                   ? 0
                   : 1;
    }
    if ((argc == 3 || argc == 4) &&
        std::string_view(argv[1]) ==
            "--callback-polars-latency-benchmark-workers") {
        std::uint64_t worker_count = 0U;
        if (!parse_unsigned(argv[2], &worker_count) ||
            worker_count >
                std::numeric_limits<std::uint32_t>::max()) {
            std::cerr << "invalid parallel decoder worker count\n";
            return 2;
        }
        StartupBenchmarkScenarioV1 scenario =
            StartupBenchmarkScenarioV1::kFromOpen;
        if (argc == 4 && !parse_scenario(argv[3], &scenario)) {
            std::cerr
                << "invalid callback-to-Polars startup scenario\n";
            return 2;
        }
        return RunHistoryLatencyBenchmark(
                   static_cast<std::uint32_t>(worker_count),
                   true,
                   scenario)
                   ? 0
                   : 1;
    }
    if (argc == 4 &&
        std::string_view(argv[1]) ==
            "--throughput-stability-benchmark") {
        std::uint64_t target_rate = 0U;
        std::uint64_t duration_ms = 0U;
        const std::string_view rate_text(argv[2]);
        const std::string_view duration_text(argv[3]);
        const auto rate_result = std::from_chars(
            rate_text.data(),
            rate_text.data() + rate_text.size(),
            target_rate);
        const auto duration_result = std::from_chars(
            duration_text.data(),
            duration_text.data() + duration_text.size(),
            duration_ms);
        if (rate_result.ec != std::errc{} ||
            rate_result.ptr != rate_text.data() + rate_text.size() ||
            duration_result.ec != std::errc{} ||
            duration_result.ptr !=
                duration_text.data() + duration_text.size() ||
            duration_ms > static_cast<std::uint64_t>(
                              std::numeric_limits<
                                  std::chrono::milliseconds::rep>::max())) {
            std::cerr << "invalid throughput benchmark arguments\n";
            return 2;
        }
        return RunThroughputStabilityBenchmark(
                   target_rate,
                   std::chrono::milliseconds(
                       static_cast<
                           std::chrono::milliseconds::rep>(
                           duration_ms)))
                   ? 0
                   : 1;
    }
    if (argc == 2) {
        const std::string_view mode(argv[1]);
        if (mode == "--latency-benchmark") {
            return RunLatencyBenchmark(false) ? 0 : 1;
        }
        if (mode == "--latency-benchmark-stages") {
            return RunLatencyBenchmark(true) ? 0 : 1;
        }
        if (mode == "--history-latency-benchmark") {
            return RunHistoryLatencyBenchmark(0U) ? 0 : 1;
        }
    }
    if (argc != 1) {
        std::cerr
            << "usage: " << argv[0]
            << " [--latency-benchmark"
               "|--latency-benchmark-stages"
               "|--history-latency-benchmark"
               "|--history-latency-benchmark-workers WORKERS "
               "[SCENARIO]"
               "|--callback-polars-latency-benchmark-workers "
               "WORKERS [SCENARIO]"
               "|--throughput-stability-benchmark RATE DURATION_MS"
               "|--throughput-profile-benchmark RATE DURATION_MS "
               "INSTRUMENTS_PER_MARKET STORE_WORKERS "
               "PARALLEL_DECODER_WORKERS IDLE_INLINE "
               "DECODER_QUEUE STORE_QUEUE "
               "SEGMENT_KIB WORKLOAD SINK [SCENARIO "
               "GENERATION_INTERVAL_MS]]\n"
               "  SCENARIO: from_open|live_partial_no_recovery\n";
        return 2;
    }
    if (!TestMalformedHistoryResponseClosesReceivedDescriptor() ||
        !TestLivePartialSemantics() ||
        !TestStandaloneLivePartialProcessStartHistory() ||
        !TestPromotionExposureGate() ||
        !TestServiceEndToEnd() ||
        !TestProcessingAdmissionPublishesWireLatest() ||
        !TestKeyArenaExhaustionIsFatal() ||
        !TestStoppedCleanRejectsMismatchedWatermark() ||
        !TestTickRingRejectsUnclosedGapOverwrite()) {
        return 1;
    }
    std::cout << "realtime shared service V2 tests passed\n";
    return 0;
}
