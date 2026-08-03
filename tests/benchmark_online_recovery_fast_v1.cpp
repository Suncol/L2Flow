#include "l2flow/ipc/realtime_shared_service_v2.h"
#include "l2flow/ipc/realtime_shm_reader_c_v2.h"
#include "l2flow/ipc/realtime_wire_v2.h"
#include "l2flow/market/daily_instrument_catalog_v2.h"
#include "l2flow/market/instrument_runtime_state_v2.h"
#include "l2flow/recovery/online_recovery_v1.h"
#include "l2flow/recovery/startup_replay_v1.h"
#include "l2flow/runtime/realtime_pipeline_v1.h"
#include "l2flow/sdk/market_message_catalog_v1.h"

#include "mdl_api.h"

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
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <poll.h>
#include <span>
#include <spawn.h>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

extern char** environ;

namespace {

namespace common = l2flow::common;
namespace ipc = l2flow::ipc;
namespace market = l2flow::market;
namespace mdl = datayes::mdl;
namespace realtime = l2flow::realtime;
namespace recovery = l2flow::recovery;
namespace runtime = l2flow::runtime;
namespace sdk = l2flow::sdk;

using namespace std::chrono_literals;

constexpr std::uint32_t kTradeDate = 20260731U;
constexpr std::array<std::uint32_t, 4U> kSourceStreamIds{
    1001U, 1002U, 2001U, 2002U};
constexpr std::size_t kPipelineQueueCapacity = 8'192U;
constexpr std::size_t kCompletionCapacity = 65'536U;
constexpr std::uint64_t kTickRingCapacity = 65'536U;
constexpr std::size_t kPureReadsPerSample = 8U;

enum class BenchmarkMode : std::uint8_t {
    kOrdinary = 0U,
    kParked,
    kActive,
};

struct Options final {
    BenchmarkMode mode = BenchmarkMode::kActive;
    std::size_t warmup_samples = 128U;
    std::size_t measured_samples = 2'048U;
    std::size_t replay_records = 50'000U;
    std::size_t throughput_rate = 0U;
    std::size_t throughput_duration_ms = 0U;
    std::uint32_t parallel_decoder_workers = 0U;
    bool mode_supplied = false;
    bool polars = false;

    [[nodiscard]] bool throughput_enabled() const noexcept {
        return throughput_rate != 0U || throughput_duration_ms != 0U;
    }

    [[nodiscard]] std::size_t planned_live_records() const noexcept {
        return throughput_enabled()
                   ? throughput_rate * throughput_duration_ms / 1'000U
                   : warmup_samples + measured_samples;
    }
};

[[nodiscard]] std::string_view ModeName(BenchmarkMode mode) noexcept {
    switch (mode) {
        case BenchmarkMode::kOrdinary:
            return "ordinary";
        case BenchmarkMode::kParked:
            return "parked";
        case BenchmarkMode::kActive:
            return "active";
    }
    return "invalid";
}

[[nodiscard]] bool ParseSize(
    std::string_view text,
    std::size_t maximum,
    std::size_t* output) noexcept {
    if (text.empty() || output == nullptr) {
        return false;
    }
    std::uint64_t value = 0U;
    const auto parsed = std::from_chars(
        text.data(), text.data() + text.size(), value);
    if (parsed.ec != std::errc{} ||
        parsed.ptr != text.data() + text.size() || value > maximum ||
        value > std::numeric_limits<std::size_t>::max()) {
        return false;
    }
    *output = static_cast<std::size_t>(value);
    return true;
}

[[nodiscard]] bool ParseOptions(
    int argc,
    char** argv,
    Options* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    Options parsed{};
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument == "--mode") {
            if (index + 1 >= argc) {
                return false;
            }
            const std::string_view value(argv[++index]);
            if (value == "ordinary") {
                parsed.mode = BenchmarkMode::kOrdinary;
            } else if (value == "parked") {
                parsed.mode = BenchmarkMode::kParked;
            } else if (value == "active") {
                parsed.mode = BenchmarkMode::kActive;
            } else {
                return false;
            }
            parsed.mode_supplied = true;
            continue;
        }
        if (argument == "--warmup-samples" ||
            argument == "--samples" ||
            argument == "--replay-records" ||
            argument == "--throughput-rate" ||
            argument == "--throughput-duration-ms") {
            if (index + 1 >= argc) {
                return false;
            }
            std::size_t value = 0U;
            constexpr std::size_t kMaximumRecordCount = 5'000'000U;
            if (!ParseSize(
                    argv[++index], kMaximumRecordCount, &value)) {
                return false;
            }
            if (argument == "--warmup-samples") {
                parsed.warmup_samples = value;
            } else if (argument == "--samples") {
                parsed.measured_samples = value;
            } else if (argument == "--replay-records") {
                parsed.replay_records = value;
            } else if (argument == "--throughput-rate") {
                parsed.throughput_rate = value;
            } else {
                parsed.throughput_duration_ms = value;
            }
            continue;
        }
        if (argument == "--parallel-decoder-workers") {
            if (index + 1 >= argc) {
                return false;
            }
            std::size_t value = 0U;
            if (!ParseSize(
                    argv[++index],
                    runtime::kRealtimeParallelDecoderMaximumWorkersV1,
                    &value)) {
                return false;
            }
            parsed.parallel_decoder_workers =
                static_cast<std::uint32_t>(value);
            continue;
        }
        if (argument == "--polars") {
            parsed.polars = true;
            continue;
        }
        return false;
    }
    const bool throughput_pair =
        (parsed.throughput_rate == 0U) ==
        (parsed.throughput_duration_ms == 0U);
    const bool throughput_valid =
        !parsed.throughput_enabled() ||
        (parsed.mode == BenchmarkMode::kActive &&
         parsed.throughput_rate != 0U &&
         parsed.throughput_duration_ms != 0U &&
         parsed.throughput_rate <= 1'000'000U &&
         parsed.throughput_duration_ms <= 10'000U &&
         parsed.throughput_rate <=
             std::numeric_limits<std::size_t>::max() /
                 parsed.throughput_duration_ms &&
         parsed.planned_live_records() != 0U &&
         parsed.planned_live_records() <= 5'000'000U);
    if (!parsed.mode_supplied || !throughput_pair || !throughput_valid ||
        (parsed.polars &&
         (parsed.mode != BenchmarkMode::kActive ||
          parsed.throughput_enabled())) ||
        (!parsed.throughput_enabled() && parsed.measured_samples == 0U) ||
        parsed.warmup_samples >
            std::numeric_limits<std::size_t>::max() -
                parsed.measured_samples ||
        parsed.warmup_samples + parsed.measured_samples > 1'000'000U) {
        return false;
    }
    if (parsed.mode == BenchmarkMode::kOrdinary) {
        parsed.replay_records = 0U;
    }
    *output = parsed;
    return true;
}

[[nodiscard]] std::uint64_t MonotonicNowNs() noexcept {
    timespec value{};
    if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0 ||
        value.tv_sec < 0 || value.tv_nsec < 0 ||
        value.tv_nsec >= 1'000'000'000L) {
        return 0U;
    }
    const std::uint64_t seconds =
        static_cast<std::uint64_t>(value.tv_sec);
    constexpr std::uint64_t kNanosecondsPerSecond = 1'000'000'000ULL;
    if (seconds >
        (std::numeric_limits<std::uint64_t>::max() -
         static_cast<std::uint64_t>(value.tv_nsec)) /
            kNanosecondsPerSecond) {
        return 0U;
    }
    return seconds * kNanosecondsPerSecond +
           static_cast<std::uint64_t>(value.tv_nsec);
}

[[nodiscard]] bool Fail(
    std::string_view stage,
    std::string_view detail) {
    std::cerr << "ONLINE_RECOVERY_FAST_ERROR"
              << " stage=" << stage
              << " detail=" << detail << '\n';
    return false;
}

class ScopedDirectory final {
public:
    ScopedDirectory() {
        std::string pattern =
            (std::filesystem::temp_directory_path() /
             "l2flow-online-recovery-bench-XXXXXX")
                .string();
        std::vector<char> writable(pattern.begin(), pattern.end());
        writable.push_back('\0');
        char* const created = ::mkdtemp(writable.data());
        if (created != nullptr) {
            path_ = created;
        }
    }

    ScopedDirectory(const ScopedDirectory&) = delete;
    ScopedDirectory& operator=(const ScopedDirectory&) = delete;

    ~ScopedDirectory() {
        if (!path_.empty()) {
            std::error_code error;
            static_cast<void>(std::filesystem::remove_all(path_, error));
        }
    }

    [[nodiscard]] bool valid() const noexcept { return !path_.empty(); }
    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

class WireWriter final {
public:
    explicit WireWriter(std::size_t fixed_bytes)
        : bytes_(fixed_bytes, std::byte{0U}) {}

    void StoreU32(std::size_t offset, std::uint32_t value) {
        StoreUnsigned(offset, value);
    }

    void StoreU64(std::size_t offset, std::uint64_t value) {
        StoreUnsigned(offset, value);
    }

    void StoreString(std::size_t descriptor, std::string_view value) {
        const std::size_t start = bytes_.size();
        StoreUnsigned(
            descriptor, static_cast<std::uint16_t>(value.size()));
        StoreU32(
            descriptor + sizeof(std::uint16_t),
            value.empty()
                ? 0U
                : static_cast<std::uint32_t>(start - descriptor));
        const auto encoded = std::as_bytes(std::span(value));
        bytes_.insert(bytes_.end(), encoded.begin(), encoded.end());
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

[[nodiscard]] std::vector<std::byte> TransactionBody(
    std::uint64_t application_sequence) {
    WireWriter writer(70U);
    writer.StoreU32(0U, 12U);
    writer.StoreU64(4U, application_sequence);
    writer.StoreU64(
        18U,
        application_sequence == 0U ? 0U : application_sequence - 1U);
    writer.StoreU64(26U, 0U);
    writer.StoreU64(46U, 100'000U);
    writer.StoreU64(54U, 100U);
    writer.StoreU32(62U, 70U);
    writer.StoreU32(66U, 93'000'010U);
    writer.StoreString(12U, "011");
    writer.StoreString(34U, "000001");
    writer.StoreString(40U, "102 ");
    return std::move(writer).Take();
}

class MutableTransactionMessage final : public mdl::MDLMessage {
public:
    MutableTransactionMessage() : body_(TransactionBody(1U)) {
        std::memset(&head_, 0, sizeof(head_));
        head_.HeadSize = static_cast<std::uint8_t>(sdk::kVendorHeadBytes);
        head_.MessageSize = static_cast<std::uint32_t>(
            sdk::kVendorHeadBytes + body_.size());
        head_.MessageEncoding =
            static_cast<std::uint8_t>(mdl::MDLEID_BINARY);
        head_.ServiceID = 6U;
        head_.ServiceVersion = 101U;
        head_.MessageID = 36U;
        head_.LocalTime.m_Value = 93'000'000U;
        SetSequence(1U);
    }

    void SetSequence(std::uint64_t sequence) noexcept {
        head_.SequenceID = sequence;
        StoreBodyU64(4U, sequence);
        StoreBodyU64(18U, sequence == 0U ? 0U : sequence - 1U);
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
    void StoreBodyU64(std::size_t offset, std::uint64_t value) noexcept {
        for (std::size_t index = 0U; index < sizeof(value); ++index) {
            body_[offset + index] = static_cast<std::byte>(
                (value >> static_cast<unsigned int>(index * 8U)) & 0xffU);
        }
    }

    mdl::MDLMessageHead head_{};
    std::vector<std::byte> body_;
};

[[nodiscard]] bool WriteCsvFixture(
    const std::filesystem::path& directory,
    std::size_t replay_records) {
    std::error_code error;
    if (!std::filesystem::create_directory(directory, error) || error) {
        return false;
    }
    std::ofstream orders(
        directory / "mdl_6_33_0.csv", std::ios::binary);
    std::ofstream transactions(
        directory / "mdl_6_36_0.csv", std::ios::binary);
    if (!orders || !transactions) {
        return false;
    }
    orders
        << "ChannelNo,ApplSeqNum,MDStreamID,SecurityID,"
           "SecurityIDSource,Price,OrderQty,Side,TransactTime,OrdType,"
           "LocalTime,SeqNo\n";
    transactions
        << "ChannelNo,ApplSeqNum,MDStreamID,BidApplSeqNum,"
           "OfferApplSeqNum,SecurityID,SecurityIDSource,LastPx,LastQty,"
           "ExecType,TransactTime,LocalTime,SeqNo\n";
    for (std::size_t index = 0U; index < replay_records; ++index) {
        const std::uint64_t sequence =
            static_cast<std::uint64_t>(index) + 1U;
        transactions
            << "12," << sequence << ",011,"
            << (sequence == 1U ? 0U : sequence - 1U)
            << ",0,000001,102,10.0000,100,F,09:30:00.009,"
               "09:30:00.010,"
            << sequence << '\n';
    }
    orders.flush();
    transactions.flush();
    return static_cast<bool>(orders) && static_cast<bool>(transactions);
}

class GatedCsvReplaySource final : public recovery::StartupReplaySourceV1 {
public:
    explicit GatedCsvReplaySource(
        std::shared_ptr<recovery::StartupReplaySourceV1> delegate)
        : delegate_(std::move(delegate)) {}

    recovery::StartupReplayResultV1 Replay(
        recovery::StartupReplaySinkV1& sink) noexcept override {
        if (delegate_ == nullptr) {
            recovery::StartupReplayResultV1 result{};
            result.error = recovery::StartupReplayErrorV1::kInvalidConfiguration;
            result.detail = "benchmark replay delegate is null";
            return result;
        }
        ForwardingSink forwarding(*this, sink);
        return delegate_->Replay(forwarding);
    }

    [[nodiscard]] bool WaitForFences(
        std::chrono::seconds timeout) noexcept {
        try {
            std::unique_lock<std::mutex> lock(mutex_);
            return condition_.wait_for(lock, timeout, [this] {
                return fences_ready_ || fences_failed_;
            }) && fences_ready_ && !fences_failed_;
        } catch (...) {
            return false;
        }
    }

    void Release() noexcept {
        try {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                released_ = true;
            }
            condition_.notify_all();
        } catch (...) {
        }
    }

    void Cancel() noexcept {
        cancelled_.store(true, std::memory_order_release);
        Release();
    }

    [[nodiscard]] std::uint64_t published() const noexcept {
        return published_.load(std::memory_order_acquire);
    }

private:
    class ForwardingSink final : public recovery::StartupReplaySinkV1 {
    public:
        ForwardingSink(
            GatedCsvReplaySource& owner,
            recovery::StartupReplaySinkV1& target)
            : owner_(owner), target_(target) {}

        [[nodiscard]] bool CaptureTupleFence(
            const sdk::MessageKey& key,
            std::string* detail) noexcept override {
            return owner_.ForwardFence(key, target_, detail);
        }

        [[nodiscard]] bool CooperativeCheckpoint(
            std::string* detail) noexcept override {
            return target_.CooperativeCheckpoint(detail);
        }

        [[nodiscard]] bool Publish(
            const recovery::StartupReplayPublicationV1& publication,
            std::string* detail) noexcept override {
            if (owner_.cancelled_.load(std::memory_order_acquire)) {
                if (detail != nullptr) {
                    *detail = "benchmark replay cancelled";
                }
                return false;
            }
            if (!target_.Publish(publication, detail)) {
                return false;
            }
            static_cast<void>(owner_.published_.fetch_add(
                1U, std::memory_order_release));
            return true;
        }

    private:
        GatedCsvReplaySource& owner_;
        recovery::StartupReplaySinkV1& target_;
    };

    [[nodiscard]] bool ForwardFence(
        const sdk::MessageKey& key,
        recovery::StartupReplaySinkV1& target,
        std::string* detail) noexcept {
        try {
            const auto position = std::find(
                sdk::kProductionMessageKeysV1.begin(),
                sdk::kProductionMessageKeysV1.end(),
                key);
            if (position == sdk::kProductionMessageKeysV1.end()) {
                if (detail != nullptr) {
                    *detail = "benchmark delegate emitted an unknown fence";
                }
                return false;
            }
            const std::size_t ordinal = static_cast<std::size_t>(
                position - sdk::kProductionMessageKeysV1.begin());
            if (fence_seen_[ordinal] ||
                !target.CaptureTupleFence(key, detail)) {
                SignalFenceFailure();
                return false;
            }
            fence_seen_[ordinal] = true;
            ++delegate_fence_count_;
            constexpr std::size_t kDelegateFenceCount = 2U;
            if (delegate_fence_count_ != kDelegateFenceCount) {
                return true;
            }
            for (std::size_t index = 0U;
                 index < fence_seen_.size();
                 ++index) {
                if (fence_seen_[index]) {
                    continue;
                }
                if (!target.CaptureTupleFence(
                        sdk::kProductionMessageKeysV1[index], detail)) {
                    SignalFenceFailure();
                    return false;
                }
                fence_seen_[index] = true;
            }
            {
                std::unique_lock<std::mutex> lock(mutex_);
                fences_ready_ = true;
                condition_.notify_all();
                condition_.wait(lock, [this] {
                    return released_ ||
                           cancelled_.load(std::memory_order_acquire);
                });
            }
            if (cancelled_.load(std::memory_order_acquire)) {
                if (detail != nullptr) {
                    *detail = "benchmark replay cancelled at fence";
                }
                return false;
            }
            return true;
        } catch (...) {
            SignalFenceFailure();
            if (detail != nullptr) {
                try {
                    *detail = "benchmark fence gate failed";
                } catch (...) {
                }
            }
            return false;
        }
    }

    void SignalFenceFailure() noexcept {
        try {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                fences_failed_ = true;
            }
            condition_.notify_all();
        } catch (...) {
        }
    }

    std::shared_ptr<recovery::StartupReplaySourceV1> delegate_;
    mutable std::mutex mutex_;
    std::condition_variable condition_;
    std::array<bool, sdk::kProductionMessageCountV1> fence_seen_{};
    std::size_t delegate_fence_count_ = 0U;
    bool fences_ready_ = false;
    bool fences_failed_ = false;
    bool released_ = false;
    std::atomic<bool> cancelled_{false};
    std::atomic<std::uint64_t> published_{0U};
};

class BenchmarkSdkState final {
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

class BenchmarkSdkSubscriber final : public sdk::SdkSubscriber {
public:
    void SetServerAddress(std::string_view value) override {
        configured_ = configured_ && !value.empty();
    }
    void SetUserName(std::string_view value) override {
        configured_ = configured_ && !value.empty();
    }
    void SetHeartbeatInterval(std::uint32_t value) override {
        configured_ = configured_ && value != 0U;
    }
    void SetHeartbeatTimeout(std::uint32_t value) override {
        configured_ = configured_ && value != 0U;
    }
    void SetMessageEncoding(mdl::MDLMessageEncoding value) override {
        configured_ = configured_ && value == mdl::MDLEID_BINARY;
    }
    void EnableMergeMessage(bool value) override {
        configured_ = configured_ && !value;
    }
    void SetSendMacAuth(bool) override {}
    void EnableServerSelect(bool) override {}
    void AddSubscription(const sdk::MessageKey&) override {
        ++subscriptions_;
    }

    [[nodiscard]] std::string Connect() override {
        return configured_ && subscriptions_ != 0U
                   ? std::string{}
                   : "invalid benchmark SDK configuration";
    }

    [[nodiscard]] bool Release(std::string* error) noexcept override {
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

private:
    std::size_t subscriptions_ = 0U;
    bool configured_ = true;
};

class BenchmarkSdkManager final : public sdk::SdkManager {
public:
    explicit BenchmarkSdkManager(
        std::shared_ptr<BenchmarkSdkState> state)
        : state_(std::move(state)) {}

    void EnableLog(std::string_view, bool) override {}

    [[nodiscard]] std::unique_ptr<sdk::SdkSubscriber> CreateSubscriber(
        mdl::MessageHandlerBase* handler,
        bool multithread_callback) override {
        if (handler == nullptr || multithread_callback) {
            return nullptr;
        }
        state_->Install(handler);
        return std::make_unique<BenchmarkSdkSubscriber>();
    }

    void Shutdown() override { state_->Shutdown(); }

    [[nodiscard]] bool Release(std::string* error) noexcept override {
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

private:
    std::shared_ptr<BenchmarkSdkState> state_;
};

class BenchmarkSdkFactory final : public sdk::SdkFactory {
public:
    explicit BenchmarkSdkFactory(
        std::shared_ptr<BenchmarkSdkState> state)
        : state_(std::move(state)) {}

    [[nodiscard]] std::unique_ptr<sdk::SdkManager> Create(
        int work_threads,
        int io_threads) override {
        if (work_threads <= 0 || io_threads != 1) {
            return nullptr;
        }
        return std::make_unique<BenchmarkSdkManager>(state_);
    }

private:
    std::shared_ptr<BenchmarkSdkState> state_;
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

[[nodiscard]] UniqueFd RequestSession(
    const std::filesystem::path& socket_path) noexcept {
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
    std::memcpy(address.sun_path, path.c_str(), path.size() + 1U);
    if (::connect(
            socket_fd.get(),
            reinterpret_cast<const sockaddr*>(&address),
            static_cast<socklen_t>(
                offsetof(sockaddr_un, sun_path) + path.size() + 1U)) != 0) {
        return {};
    }
    ipc::RealtimeControlRequestV2 request{};
    request.magic = ipc::kRealtimeControlMagicV2;
    request.protocol_major = ipc::kRealtimeWireMajorV2;
    request.protocol_minor = ipc::kRealtimeWireMinorV2;
    request.opcode = static_cast<std::uint16_t>(
        ipc::RealtimeControlOpcodeV2::kGetSession);
    request.message_bytes = static_cast<std::uint32_t>(sizeof(request));
    request.request_id = 0x4f4e4c494e454241ULL;
    if (::send(
            socket_fd.get(),
            &request,
            sizeof(request),
            MSG_NOSIGNAL) != static_cast<ssize_t>(sizeof(request))) {
        return {};
    }

    ipc::RealtimeControlResponseV2 response{};
    iovec vector{};
    vector.iov_base = &response;
    vector.iov_len = sizeof(response);
    std::array<std::byte, CMSG_SPACE(sizeof(int))> control{};
    msghdr message{};
    message.msg_iov = &vector;
    message.msg_iovlen = 1U;
    message.msg_control = control.data();
    message.msg_controllen = control.size();
    const ssize_t received =
        ::recvmsg(socket_fd.get(), &message, MSG_CMSG_CLOEXEC);
    if (received != static_cast<ssize_t>(sizeof(response)) ||
        response.status != static_cast<std::uint16_t>(
            ipc::RealtimeControlStatusV2::kOk)) {
        return {};
    }
    for (cmsghdr* header = CMSG_FIRSTHDR(&message);
         header != nullptr;
         header = CMSG_NXTHDR(&message, header)) {
        if (header->cmsg_level == SOL_SOCKET &&
            header->cmsg_type == SCM_RIGHTS &&
            header->cmsg_len == CMSG_LEN(sizeof(int))) {
            int received_fd = -1;
            std::memcpy(
                &received_fd, CMSG_DATA(header), sizeof(received_fd));
            return UniqueFd(received_fd);
        }
    }
    return {};
}

[[nodiscard]] common::Identity128 RunId(std::uint8_t first) noexcept {
    common::Identity128 result{};
    result[0U] = static_cast<std::byte>(first);
    result[15U] = std::byte{0xa5U};
    return result;
}

struct DailyFixture final {
    std::shared_ptr<const market::DailyInstrumentCatalogV2> catalog;
    std::unique_ptr<market::InstrumentRuntimeStateV2> preview_state;
    std::unique_ptr<market::InstrumentRuntimeStateV2> shadow_state;
};

[[nodiscard]] bool MakeDailyFixture(DailyFixture* output) {
    if (output == nullptr) {
        return false;
    }
    *output = {};
    market::DailyInstrumentSourceEntryV2 instrument{};
    instrument.key.market = market::MarketV1::kShenzhen;
    instrument.key.security_id_source = {
        std::byte{'1'}, std::byte{'0'},
        std::byte{'2'}, std::byte{' '}};
    instrument.key.security_id = {
        std::byte{'0'}, std::byte{'0'}, std::byte{'0'},
        std::byte{'0'}, std::byte{'0'}, std::byte{'1'}};
    instrument.metadata = market::InstrumentMetadataV2{
        market::QuantityUnitV1::kShare,
        market::SecurityTypeV1::kEquity,
        market::AssetScopeV1::kDocumentedCore};
    market::DailyInstrumentCatalogConfigV2 config{};
    config.trade_date = kTradeDate;
    config.catalog_version = 1U;
    config.session_epoch = 1U;
    config.market_scope = market::kDailyCatalogMainlandScopeV2;
    config.coverage_complete = true;
    std::unique_ptr<market::DailyInstrumentCatalogV2> catalog;
    if (market::DailyInstrumentCatalogV2::Create(
            config,
            std::span(&instrument, 1U),
            &catalog) != market::DailyInstrumentCatalogCreateErrorV2::kNone ||
        catalog == nullptr) {
        return false;
    }
    output->catalog =
        std::shared_ptr<const market::DailyInstrumentCatalogV2>(
            std::move(catalog));
    return market::InstrumentRuntimeStateV2::Create(
               *output->catalog, &output->preview_state) ==
               market::InstrumentRuntimeStateErrorV2::kNone &&
           output->preview_state != nullptr &&
           market::InstrumentRuntimeStateV2::Create(
               *output->catalog, &output->shadow_state) ==
               market::InstrumentRuntimeStateErrorV2::kNone &&
           output->shadow_state != nullptr;
}

[[nodiscard]] runtime::RealtimePipelineConfigV1 PipelineConfig(
    const common::Identity128& run_id,
    const std::shared_ptr<const market::DailyInstrumentCatalogV2>& catalog,
    market::InstrumentRuntimeStateV2* runtime_state,
    std::uint64_t maximum_records,
    bool coverage_from_open,
    std::uint32_t parallel_decoder_workers) {
    runtime::RealtimePipelineConfigV1 config{};
    config.run_id = run_id;
    config.trade_date = kTradeDate;
    config.daily_catalog = catalog;
    config.runtime_state = runtime_state;
    config.source_stream_ids = kSourceStreamIds;
    config.maximum_sdk_message_bytes = 4096U;
    config.decoder_queue_capacity_per_source = kPipelineQueueCapacity;
    config.parallel_decoder_worker_count = parallel_decoder_workers;
    config.completion_tracker_capacity = kCompletionCapacity;
    config.tick_ring_capacity = kCompletionCapacity;
    config.store_worker_count = 4U;
    config.store_queue_capacity_per_source_worker =
        kPipelineQueueCapacity;
    config.intraday_store.segment_target_bytes =
        market::kIntradayInstrumentStoreMinimumSegmentBytesV1;
    config.intraday_store.maximum_session_records = maximum_records;
    config.intraday_store.maximum_session_accounted_bytes =
        32ULL * 1024ULL * 1024ULL * 1024ULL;
    config.intraday_store.maximum_records_per_batch = 1'024U;
    config.intraday_store.coverage_from_open = coverage_from_open;
    config.enforce_receive_trade_date = false;
    return config;
}

[[nodiscard]] ipc::RealtimeSharedServiceConfigV2 ServiceConfig(
    const common::Identity128& run_id,
    const std::shared_ptr<const market::DailyInstrumentCatalogV2>& catalog,
    const std::filesystem::path& socket,
    bool coverage_from_open) {
    ipc::RealtimeSharedServiceConfigV2 config{};
    config.run_id = run_id;
    config.session_epoch = 1U;
    config.trade_date = kTradeDate;
    config.daily_catalog = catalog;
    config.coverage_from_open = coverage_from_open;
    config.startup_prefix_recovered = coverage_from_open;
    config.full_day_factor_valid = coverage_from_open;
    config.tick_ring_capacity = kTickRingCapacity;
    config.key_arena_bytes = 4U * 1024U;
    config.maximum_mapping_bytes = 256ULL * 1024ULL * 1024ULL;
    config.control_socket_path = socket;
    return config;
}

struct LatencySummary final {
    std::uint64_t minimum_ns = 0U;
    std::uint64_t mean_ns = 0U;
    std::uint64_t p50_ns = 0U;
    std::uint64_t p95_ns = 0U;
    std::uint64_t p99_ns = 0U;
    std::uint64_t p999_ns = 0U;
    std::uint64_t maximum_ns = 0U;
};

[[nodiscard]] std::uint64_t NearestRank(
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
    return sorted[static_cast<std::size_t>(
        std::min(count, std::max<std::uint64_t>(1U, rank)) - 1U)];
}

[[nodiscard]] LatencySummary Summarize(
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
    result.mean_ns = static_cast<std::uint64_t>(
        sum / static_cast<long double>(values.size()));
    result.p50_ns = NearestRank(values, 50U, 100U);
    result.p95_ns = NearestRank(values, 95U, 100U);
    result.p99_ns = NearestRank(values, 99U, 100U);
    result.p999_ns = NearestRank(values, 999U, 1'000U);
    result.maximum_ns = values.back();
    return result;
}

void PrintLatency(
    BenchmarkMode mode,
    std::string_view metric,
    const std::vector<std::uint64_t>& values) {
    const LatencySummary summary = Summarize(values);
    std::cout
        << "ONLINE_RECOVERY_FAST_LATENCY"
        << " mode=" << ModeName(mode)
        << " metric=" << metric
        << " unit=ns"
        << " samples=" << values.size()
        << " estimator=nearest_rank"
        << " min=" << summary.minimum_ns
        << " mean=" << summary.mean_ns
        << " p50=" << summary.p50_ns
        << " p95=" << summary.p95_ns
        << " p99=" << summary.p99_ns
        << " p999=" << summary.p999_ns
        << " max=" << summary.maximum_ns << '\n';
}

struct LatencyResult final {
    std::vector<std::uint64_t> callback_call_ns;
    std::vector<std::uint64_t> callback_to_preview_ns;
    std::vector<std::uint64_t> recv_to_preview_ns;
    std::vector<std::uint64_t> pure_latest_read_ns;
    std::uint64_t poll_calls = 0U;
    std::uint64_t inconsistent_reads = 0U;
    std::uint64_t promotion_overlap_samples = 0U;
    std::uint64_t first_measured_caller_before_callback_ns = 0U;
    std::uint64_t last_measured_caller_before_callback_ns = 0U;
};

struct ThroughputResult final {
    std::uint64_t planned_callbacks = 0U;
    std::uint64_t invoked_callbacks = 0U;
    std::uint64_t start_ns = 0U;
    std::uint64_t offer_complete_ns = 0U;
    std::uint64_t promotion_overlap_callbacks = 0U;
    std::size_t sampled_quarters = 0U;
    std::array<std::uint64_t, 4U> preview_backlog_at_quarter{};
    std::array<std::uint64_t, 4U> journal_uncommitted_at_quarter{};
    std::array<std::uint64_t, 4U> journal_queue_at_quarter{};
    runtime::RealtimePipelineSnapshotV1 preview_before_finish{};
    recovery::LiveJournalSnapshotV1 journal_before_finish{};
    bool fatal_during_offer = false;
};

struct RecoveryResult final {
    recovery::OnlineRecoveryBoundaryV1 boundary{};
    recovery::OnlineRecoverySnapshotV1 promotion_snapshot{};
    runtime::RealtimePipelineCutResultV1 promotion_cut{};
    recovery::OnlineRecoveryPumpResultV1 terminal_pump{};
    std::uint64_t release_ns = 0U;
    std::uint64_t promotion_ns = 0U;
    bool recovered_service_started = false;
    bool mark_promoted = false;
};

class BenchmarkHarness final {
public:
    explicit BenchmarkHarness(Options options)
        : options_(options) {}

    BenchmarkHarness(const BenchmarkHarness&) = delete;
    BenchmarkHarness& operator=(const BenchmarkHarness&) = delete;

    ~BenchmarkHarness() { Cleanup(); }

    [[nodiscard]] bool Initialize() {
        if (!temporary_.valid() || !MakeDailyFixture(&daily_)) {
            return Fail("fixture", "daily fixture creation failed");
        }
        const std::size_t total_live = options_.planned_live_records();
        if (options_.replay_records >
            std::numeric_limits<std::uint64_t>::max() - total_live - 1U) {
            return Fail("configuration", "record bound overflow");
        }
        maximum_records_ = static_cast<std::uint64_t>(
            options_.replay_records + total_live + 1U);

        if (options_.mode != BenchmarkMode::kOrdinary &&
            !InitializeRecoverySide()) {
            return false;
        }
        if (!InitializePreview()) {
            return false;
        }
        if (options_.mode != BenchmarkMode::kOrdinary &&
            !InitializeHandoff()) {
            return false;
        }

        int system_error = 0;
        if (!preview_service_->StartLivePartial(&system_error) ||
            system_error != 0) {
            return Fail("preview_start", "LIVE_PARTIAL start failed");
        }
        UniqueFd transfer = RequestSession(
            preview_service_->control_socket_path());
        if (transfer.get() < 0 ||
            l2flow_shm_reader_open_fd_v2(
                transfer.get(), reader_.output()) !=
                L2FLOW_SHM_READER_OK_V2 ||
            reader_.get() == nullptr) {
            return Fail("reader_open", "preview reader open failed");
        }
        return true;
    }

    [[nodiscard]] bool Measure(LatencyResult* output) {
        if (output == nullptr || preview_pipeline_ == nullptr ||
            reader_.get() == nullptr || sdk_state_ == nullptr) {
            return false;
        }
        *output = {};
        output->callback_call_ns.reserve(options_.measured_samples);
        output->callback_to_preview_ns.reserve(options_.measured_samples);
        output->recv_to_preview_ns.reserve(options_.measured_samples);
        output->pure_latest_read_ns.reserve(
            options_.measured_samples * kPureReadsPerSample);

        if (options_.mode == BenchmarkMode::kActive &&
            !ReleaseRecovery()) {
            return false;
        }

        mdl::MessageHandlerBase* const handler = sdk_state_->handler();
        if (handler == nullptr) {
            return Fail("sdk_handler", "benchmark SDK handler is absent");
        }
        MutableTransactionMessage live_message;
        const std::size_t total =
            options_.warmup_samples + options_.measured_samples;
        for (std::size_t index = 0U; index < total; ++index) {
            const std::uint64_t ingress_sequence =
                static_cast<std::uint64_t>(index) + 1U;
            const std::uint64_t vendor_sequence =
                static_cast<std::uint64_t>(options_.replay_records) +
                ingress_sequence;
            live_message.SetSequence(vendor_sequence);
            const bool measured = index >= options_.warmup_samples;
            if (measured &&
                options_.mode == BenchmarkMode::kActive &&
                !promotion_done_.load(std::memory_order_acquire)) {
                ++output->promotion_overlap_samples;
            }
            const std::uint64_t origin = MonotonicNowNs();
            handler->OnMessage(nullptr, &live_message);
            const std::uint64_t callback_return = MonotonicNowNs();
            if (origin == 0U || callback_return < origin ||
                !preview_pipeline_->LiveStatus().healthy()) {
                return Fail("callback", "preview callback failed");
            }

            ipc::RealtimeWireTickPayloadV2 latest{};
            std::uint8_t status = 0xffU;
            std::uint64_t visible = 0U;
            const std::uint64_t deadline =
                callback_return + 5'000'000'000ULL;
            for (;;) {
                const std::uint64_t begin = MonotonicNowNs();
                constexpr std::uint32_t kInstrumentId = 1U;
                const int error = l2flow_shm_reader_latest_ticks_v2(
                    reader_.get(),
                    &kInstrumentId,
                    1U,
                    &latest,
                    sizeof(latest),
                    &status);
                const std::uint64_t end = MonotonicNowNs();
                ++output->poll_calls;
                if (begin == 0U || end < begin) {
                    return Fail("latest_clock", "latest read clock failed");
                }
                if (error == L2FLOW_SHM_READER_INCONSISTENT_READ_V2) {
                    ++output->inconsistent_reads;
                    if (end >= deadline) {
                        return Fail("latest", "inconsistent read timed out");
                    }
                    continue;
                }
                if (error != L2FLOW_SHM_READER_OK_V2) {
                    return Fail("latest", "preview latest read failed");
                }
                if (status == L2FLOW_LATEST_AVAILABLE_V2 &&
                    latest.common.ingress_sequence > ingress_sequence) {
                    return Fail("latest", "preview skipped an ingress sequence");
                }
                if (status == L2FLOW_LATEST_AVAILABLE_V2 &&
                    latest.common.ingress_sequence == ingress_sequence) {
                    visible = end;
                    break;
                }
                if (end >= deadline) {
                    return Fail("latest", "preview visibility timed out");
                }
                std::this_thread::yield();
            }
            if (latest.common.vendor_sequence_id != vendor_sequence ||
                latest.common.recv_monotonic_ns <= 0 ||
                static_cast<std::uint64_t>(
                    latest.common.recv_monotonic_ns) < origin ||
                visible < static_cast<std::uint64_t>(
                    latest.common.recv_monotonic_ns)) {
                return Fail("correlation", "preview payload correlation failed");
            }
            if (measured) {
                if (output->first_measured_caller_before_callback_ns == 0U) {
                    output->first_measured_caller_before_callback_ns =
                        origin;
                }
                output->last_measured_caller_before_callback_ns = origin;
                output->callback_call_ns.push_back(
                    callback_return - origin);
                output->callback_to_preview_ns.push_back(visible - origin);
                output->recv_to_preview_ns.push_back(
                    visible - static_cast<std::uint64_t>(
                                  latest.common.recv_monotonic_ns));
            }

            const std::uint64_t pure_read_deadline =
                MonotonicNowNs() + 5'000'000'000ULL;
            std::size_t completed_pure_reads = 0U;
            while (completed_pure_reads < kPureReadsPerSample) {
                status = 0xffU;
                const std::uint64_t begin = MonotonicNowNs();
                constexpr std::uint32_t kInstrumentId = 1U;
                const int error = l2flow_shm_reader_latest_ticks_v2(
                    reader_.get(),
                    &kInstrumentId,
                    1U,
                    &latest,
                    sizeof(latest),
                    &status);
                const std::uint64_t end = MonotonicNowNs();
                if (begin == 0U || end < begin) {
                    return Fail("pure_latest_clock", "clock failed");
                }
                if (error == L2FLOW_SHM_READER_INCONSISTENT_READ_V2) {
                    ++output->inconsistent_reads;
                    if (end >= pure_read_deadline) {
                        return Fail(
                            "pure_latest",
                            "inconsistent read retry timed out");
                    }
                    std::this_thread::yield();
                    continue;
                }
                if (error != L2FLOW_SHM_READER_OK_V2 ||
                    status != L2FLOW_LATEST_AVAILABLE_V2 ||
                    latest.common.ingress_sequence != ingress_sequence) {
                    return Fail("pure_latest", "stable latest read failed");
                }
                if (measured) {
                    output->pure_latest_read_ns.push_back(end - begin);
                }
                ++completed_pure_reads;
            }
        }
        return true;
    }

    [[nodiscard]] bool MeasureThroughput(ThroughputResult* output) {
        if (output == nullptr || !options_.throughput_enabled() ||
            options_.mode != BenchmarkMode::kActive ||
            preview_pipeline_ == nullptr || sdk_state_ == nullptr ||
            live_journal_ == nullptr) {
            return false;
        }
        *output = {};
        output->planned_callbacks = static_cast<std::uint64_t>(
            options_.planned_live_records());
        if (!ReleaseRecovery()) {
            return false;
        }
        mdl::MessageHandlerBase* const handler = sdk_state_->handler();
        if (handler == nullptr) {
            return Fail("sdk_handler", "benchmark SDK handler is absent");
        }

        MutableTransactionMessage live_message;
        output->start_ns = MonotonicNowNs();
        if (output->start_ns == 0U) {
            return Fail("throughput_clock", "offer clock failed");
        }
        std::size_t next_quarter = 0U;
        for (std::uint64_t index = 0U;
             index < output->planned_callbacks;
             ++index) {
            const std::uint64_t deadline_ns =
                output->start_ns +
                (index + 1U) * 1'000'000'000ULL /
                    static_cast<std::uint64_t>(
                        options_.throughput_rate);
            for (;;) {
                const std::uint64_t now_ns = MonotonicNowNs();
                if (now_ns == 0U || now_ns >= deadline_ns) {
                    break;
                }
                const std::uint64_t remaining_ns = deadline_ns - now_ns;
                if (remaining_ns > 100'000U) {
                    std::this_thread::sleep_for(
                        std::chrono::nanoseconds(
                            remaining_ns - 50'000U));
                } else {
                    std::this_thread::yield();
                }
            }

            const std::uint64_t ingress_sequence = index + 1U;
            live_message.SetSequence(
                static_cast<std::uint64_t>(options_.replay_records) +
                ingress_sequence);
            if (!promotion_done_.load(std::memory_order_acquire)) {
                ++output->promotion_overlap_callbacks;
            }
            handler->OnMessage(nullptr, &live_message);
            ++output->invoked_callbacks;

            while (next_quarter < 4U &&
                   output->invoked_callbacks * 4U >=
                       output->planned_callbacks *
                           static_cast<std::uint64_t>(next_quarter + 1U)) {
                const runtime::RealtimePipelineSnapshotV1 preview =
                    preview_pipeline_->Snapshot();
                const recovery::LiveJournalSnapshotV1 journal =
                    live_journal_->Snapshot();
                output->preview_backlog_at_quarter[next_quarter] =
                    preview.accepted_messages >=
                            preview.processing_progress.applied_sequence
                        ? preview.accepted_messages -
                              preview.processing_progress.applied_sequence
                        : 0U;
                output->journal_uncommitted_at_quarter[next_quarter] =
                    journal.accepted_serial >= journal.committed_serial
                        ? journal.accepted_serial -
                              journal.committed_serial
                        : 0U;
                output->journal_queue_at_quarter[next_quarter] =
                    static_cast<std::uint64_t>(journal.queue_depth);
                ++next_quarter;
            }
            if ((output->invoked_callbacks & 255U) == 0U) {
                const auto live = preview_pipeline_->LiveStatus();
                const auto journal = live_journal_->Snapshot();
                if (!live.healthy() || !journal.healthy()) {
                    output->fatal_during_offer = true;
                    break;
                }
            }
        }
        output->offer_complete_ns = MonotonicNowNs();
        output->sampled_quarters = next_quarter;
        output->preview_before_finish = preview_pipeline_->Snapshot();
        output->journal_before_finish = live_journal_->Snapshot();
        output->fatal_during_offer =
            output->fatal_during_offer ||
            output->preview_before_finish.fatal ||
            !output->journal_before_finish.healthy();
        return output->offer_complete_ns >= output->start_ns;
    }

    [[nodiscard]] bool Finish(
        std::uint64_t* history_records,
        bool* history_complete) {
        if (history_records == nullptr || history_complete == nullptr) {
            return false;
        }
        *history_records = 0U;
        *history_complete = false;
        if (options_.mode == BenchmarkMode::kOrdinary) {
            const runtime::RealtimePipelineCutResultV1 cut =
                preview_pipeline_->CutAndPublishGeneration(60s);
            if (!cut.published() || cut.store_generation == nullptr) {
                return Fail("ordinary_cut", "preview generation failed");
            }
            final_generation_ =
                cut.store_generation->watermark().generation;
            *history_records = cut.store_generation->record_count();
            *history_complete = ValidateHistory(
                *cut.store_generation,
                static_cast<std::uint64_t>(
                    options_.warmup_samples + options_.measured_samples));
            return *history_complete;
        }

        if (!recovery_released_ && !ReleaseRecovery()) {
            return false;
        }
        if (!WaitForPromotion(120s)) {
            return Fail("promotion_wait", "recovery promotion timed out");
        }
        if (!recovery_result_.boundary.ready() ||
            !recovery_result_.promotion_cut.published() ||
            !recovery_result_.mark_promoted ||
            !recovery_result_.recovered_service_started) {
            return Fail("promotion", "recovery promotion failed");
        }
        if (live_journal_ == nullptr || !live_journal_->StopAndFlush()) {
            return Fail("journal_stop", "journal flush failed");
        }
        journal_stopped_ = true;
        if (recovery_thread_.joinable()) {
            recovery_thread_.join();
        }
        if (!recovery_thread_done_.load(std::memory_order_acquire) ||
            recovery_result_.terminal_pump.disposition !=
                recovery::OnlineRecoveryPumpDispositionV1::kEnd) {
            return Fail("tail", "journal tail did not reach End");
        }
        const runtime::RealtimePipelineCutResultV1 final_cut =
            shadow_pipeline_->CutAndPublishGeneration(60s);
        if (!final_cut.published() || final_cut.store_generation == nullptr) {
            return Fail("final_cut", "recovered generation failed");
        }
        final_generation_ =
            final_cut.store_generation->watermark().generation;
        const std::uint64_t expected =
            static_cast<std::uint64_t>(options_.replay_records) +
            static_cast<std::uint64_t>(
                options_.warmup_samples + options_.measured_samples);
        *history_records = final_cut.store_generation->record_count();
        *history_complete =
            *history_records == expected &&
            ValidateHistory(*final_cut.store_generation, expected);
        return *history_complete;
    }

    [[nodiscard]] const RecoveryResult& recovery_result() const noexcept {
        return recovery_result_;
    }

    [[nodiscard]] recovery::OnlineRecoverySnapshotV1
    final_recovery_snapshot() const noexcept {
        return handoff_ == nullptr
                   ? recovery::OnlineRecoverySnapshotV1{}
                   : handoff_->Snapshot();
    }

    [[nodiscard]] recovery::LiveJournalSnapshotV1
    journal_snapshot() const noexcept {
        return live_journal_ == nullptr
                   ? recovery::LiveJournalSnapshotV1{}
                   : live_journal_->Snapshot();
    }

    [[nodiscard]] runtime::RealtimePipelineSnapshotV1
    preview_snapshot() const noexcept {
        return preview_pipeline_ == nullptr
                   ? runtime::RealtimePipelineSnapshotV1{}
                   : preview_pipeline_->Snapshot();
    }

    [[nodiscard]] std::uint64_t csv_published() const noexcept {
        return replay_source_ == nullptr ? 0U : replay_source_->published();
    }

    [[nodiscard]] std::uint64_t final_generation() const noexcept {
        return final_generation_;
    }

    [[nodiscard]] std::filesystem::path recovered_socket_path() const {
        return recovered_service_ == nullptr
                   ? std::filesystem::path{}
                   : recovered_service_->control_socket_path();
    }

private:
    [[nodiscard]] bool InitializeRecoverySide() {
        const std::filesystem::path csv_directory =
            temporary_.path() / "csv";
        if (!WriteCsvFixture(csv_directory, options_.replay_records)) {
            return Fail("csv_fixture", "CSV fixture write failed");
        }
        recovery::StartupReplayConfigV1 replay_config{};
        replay_config.directory = csv_directory;
        replay_config.enabled_messages =
            recovery::StartupReplayMessageSetV1::kShenzhenTransaction;
        auto csv_source = std::make_shared<
            recovery::MdlCsvStartupReplaySourceV1>(
                std::move(replay_config));
        replay_source_ = std::make_shared<GatedCsvReplaySource>(
            std::move(csv_source));

        recovery::LiveJournalConfigV1 journal_config{};
        journal_config.directory = temporary_.path() / "journal";
        journal_config.run_id = RunId(0x31U);
        journal_config.trade_date = kTradeDate;
        journal_config.maximum_message_bytes = 4096U;
        journal_config.segment_maximum_bytes =
            256ULL * 1024ULL * 1024ULL;
        journal_config.maximum_total_bytes =
            32ULL * 1024ULL * 1024ULL * 1024ULL;
        journal_config.queue_capacity_records = 65'536U;
        journal_config.sync_batch_records = 256U;
        journal_config.sync_interval = 2ms;
        if (recovery::MdlLiveJournalV1::Create(
                journal_config, &live_journal_) !=
                recovery::LiveJournalErrorV1::kNone ||
            live_journal_ == nullptr) {
            return Fail("journal_create", "live journal creation failed");
        }

        const common::Identity128 recovered_run_id = RunId(0x42U);
        ipc::RealtimeSharedServiceConfigV2 service_config = ServiceConfig(
            recovered_run_id,
            daily_.catalog,
            temporary_.path() / "recovered.sock",
            true);
        int system_error = 0;
        const auto service_error =
            ipc::RealtimeSharedMarketServiceV2::Create(
                std::move(service_config),
                &recovered_service_,
                &system_error);
        if (service_error !=
                ipc::RealtimeSharedServiceCreateErrorV2::kNone ||
            recovered_service_ == nullptr || system_error != 0) {
            std::cerr
                << "recovered service error="
                << ipc::RealtimeSharedServiceCreateErrorNameV2(
                       service_error)
                << " errno=" << system_error << '\n';
            return Fail("recovered_service", "service creation failed");
        }

        runtime::RealtimePipelineConfigV1 shadow_config = PipelineConfig(
            recovered_run_id,
            daily_.catalog,
            daily_.shadow_state.get(),
            maximum_records_,
            true,
            options_.parallel_decoder_workers);
        shadow_config.sdk.enabled = false;
        shadow_config.external_ingress_enabled = true;
        shadow_config.applied_record_sink = recovered_service_;
        shadow_config.processing_progress_sink = recovered_service_;
        shadow_config.store_generation_sink = recovered_service_;
        std::string detail;
        if (runtime::RealtimePipelineV1::Create(
                std::move(shadow_config),
                &shadow_pipeline_,
                &detail) !=
                runtime::RealtimePipelineCreateErrorV1::kNone ||
            shadow_pipeline_ == nullptr) {
            std::cerr << "shadow detail=" << detail << '\n';
            return Fail("shadow_pipeline", "pipeline creation failed");
        }
        return true;
    }

    [[nodiscard]] bool InitializePreview() {
        const common::Identity128 preview_run_id = RunId(0x21U);
        ipc::RealtimeSharedServiceConfigV2 service_config = ServiceConfig(
            preview_run_id,
            daily_.catalog,
            temporary_.path() / "preview.sock",
            false);
        int system_error = 0;
        const auto service_error =
            ipc::RealtimeSharedMarketServiceV2::Create(
                std::move(service_config),
                &preview_service_,
                &system_error);
        if (service_error !=
                ipc::RealtimeSharedServiceCreateErrorV2::kNone ||
            preview_service_ == nullptr || system_error != 0) {
            std::cerr
                << "preview service error="
                << ipc::RealtimeSharedServiceCreateErrorNameV2(
                       service_error)
                << " errno=" << system_error << '\n';
            return Fail("preview_service", "service creation failed");
        }

        sdk_state_ = std::make_shared<BenchmarkSdkState>();
        runtime::RealtimePipelineConfigV1 preview_config = PipelineConfig(
            preview_run_id,
            daily_.catalog,
            daily_.preview_state.get(),
            maximum_records_,
            false,
            options_.parallel_decoder_workers);
        preview_config.sdk.enabled = true;
        preview_config.sdk.server_address = "benchmark.invalid";
        preview_config.sdk.user_name = "online-recovery-benchmark";
        preview_config.sdk.message_encoding = mdl::MDLEID_BINARY;
        preview_config.sdk.merge_message = false;
        preview_config.applied_record_sink = preview_service_;
        preview_config.processing_progress_sink = preview_service_;
        preview_config.store_generation_sink = preview_service_;
        if (live_journal_ != nullptr) {
            preview_config.live_ingress_capture_sink = live_journal_;
        }
        std::string detail;
        if (runtime::RealtimePipelineV1::CreateForTest(
                std::move(preview_config),
                std::make_shared<BenchmarkSdkFactory>(sdk_state_),
                &preview_pipeline_,
                &detail) !=
                runtime::RealtimePipelineCreateErrorV1::kNone ||
            preview_pipeline_ == nullptr) {
            std::cerr << "preview detail=" << detail << '\n';
            return Fail("preview_pipeline", "pipeline creation failed");
        }
        return true;
    }

    [[nodiscard]] bool InitializeHandoff() {
        recovery::OnlineRecoveryConfigV1 config{};
        config.live_journal = live_journal_;
        config.csv_replay_source = replay_source_;
        config.shadow_pipeline = shadow_pipeline_.get();
        config.trade_date = kTradeDate;
        config.source_stream_ids = kSourceStreamIds;
        config.maximum_message_bytes = 4096U;
        config.overlap_retention_per_tuple = std::max<std::size_t>(
            1U, options_.replay_records);
        config.warmup_timeout = 10min;
        config.per_record_admission_timeout = 30s;
        config.preview_live_status = [this] {
            return preview_pipeline_->LiveStatus();
        };
        std::string detail;
        if (recovery::OnlineRecoveryHandoffV1::Create(
                std::move(config),
                &handoff_,
                &detail) != recovery::OnlineRecoveryErrorV1::kNone ||
            handoff_ == nullptr) {
            std::cerr << "handoff detail=" << detail << '\n';
            return Fail("handoff", "online handoff creation failed");
        }
        recovery_thread_ = std::thread([this] { RecoveryMain(); });
        if (!replay_source_->WaitForFences(30s)) {
            return Fail("fence", "CSV tuple fences were not captured");
        }
        return true;
    }

    void RecoveryMain() noexcept {
        recovery_result_.boundary =
            handoff_->RecoverToPromotionBoundary();
        if (!recovery_result_.boundary.ready()) {
            recovery_result_.promotion_ns = MonotonicNowNs();
            promotion_done_.store(true, std::memory_order_release);
            recovery_thread_done_.store(true, std::memory_order_release);
            return;
        }
        recovery_result_.promotion_cut =
            shadow_pipeline_->CutAndPublishGeneration(60s);
        int system_error = 0;
        recovery_result_.recovered_service_started =
            recovery_result_.promotion_cut.published() &&
            recovered_service_->Start(&system_error) && system_error == 0;
        const std::uint64_t promotion_time = MonotonicNowNs();
        recovery_result_.mark_promoted =
            recovery_result_.recovered_service_started &&
            promotion_time != 0U &&
            handoff_->MarkPromoted(promotion_time);
        recovery_result_.promotion_snapshot = handoff_->Snapshot();
        recovery_result_.promotion_ns = promotion_time;
        promotion_done_.store(true, std::memory_order_release);
        if (!recovery_result_.mark_promoted) {
            recovery_thread_done_.store(true, std::memory_order_release);
            return;
        }
        for (;;) {
            recovery_result_.terminal_pump = handoff_->PumpNext(
                std::chrono::steady_clock::now() + 100ms);
            if (recovery_result_.terminal_pump.disposition ==
                    recovery::OnlineRecoveryPumpDispositionV1::kRecord ||
                recovery_result_.terminal_pump.disposition ==
                    recovery::OnlineRecoveryPumpDispositionV1::kIdle) {
                continue;
            }
            break;
        }
        recovery_thread_done_.store(true, std::memory_order_release);
    }

    [[nodiscard]] bool ReleaseRecovery() noexcept {
        if (replay_source_ == nullptr || recovery_released_) {
            return replay_source_ != nullptr;
        }
        recovery_result_.release_ns = MonotonicNowNs();
        if (recovery_result_.release_ns == 0U) {
            return Fail("recovery_clock", "release clock failed");
        }
        recovery_released_ = true;
        replay_source_->Release();
        return true;
    }

    [[nodiscard]] bool WaitForPromotion(
        std::chrono::seconds timeout) const noexcept {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (promotion_done_.load(std::memory_order_acquire)) {
                return true;
            }
            std::this_thread::yield();
        }
        return promotion_done_.load(std::memory_order_acquire);
    }

    [[nodiscard]] static bool ValidateHistory(
        const market::IntradayInstrumentStoreGenerationV1& generation,
        std::uint64_t expected_records) {
        if (generation.record_count() != expected_records ||
            generation.watermark().ingress_sequence_exclusive !=
                expected_records + 1U) {
            return false;
        }
        std::unique_ptr<market::IntradayInstrumentCursorV1> cursor;
        if (generation.OpenInstrumentCursor(1U, {}, &cursor) !=
                market::IntradayInstrumentStoreQueryErrorV1::kNone ||
            cursor == nullptr) {
            return false;
        }
        std::array<const market::RealtimeHistoryRecordV1*, 512U> records{};
        std::uint64_t next = 1U;
        for (;;) {
            std::size_t written = 0U;
            if (cursor->ReadBatch(records, &written) !=
                market::IntradayInstrumentStoreQueryErrorV1::kNone) {
                return false;
            }
            if (written == 0U) {
                break;
            }
            for (std::size_t index = 0U; index < written; ++index) {
                const market::RealtimeHistoryRecordV1* const record =
                    records[index];
                if (record == nullptr ||
                    record->ingress_sequence() != next) {
                    return false;
                }
                const auto* transaction =
                    market::StoredMarketEventGetV1<
                        market::ShenzhenTransactionV1>(record->event());
                if (transaction == nullptr ||
                    transaction->application_sequence !=
                        static_cast<std::int64_t>(next)) {
                    return false;
                }
                ++next;
            }
        }
        return next == expected_records + 1U && cursor->done();
    }

    void Cleanup() noexcept {
        if (replay_source_ != nullptr) {
            replay_source_->Cancel();
        }
        if (live_journal_ != nullptr && !journal_stopped_) {
            static_cast<void>(live_journal_->StopAndFlush());
            journal_stopped_ = true;
        }
        if (recovery_thread_.joinable()) {
            recovery_thread_.join();
        }
        reader_.Reset();
        if (preview_pipeline_ != nullptr) {
            preview_pipeline_->StopAndDrain();
        }
        if (shadow_pipeline_ != nullptr) {
            shadow_pipeline_->StopAndDrain();
        }
        if (preview_service_ != nullptr) {
            preview_service_->StopControl();
        }
        if (recovered_service_ != nullptr) {
            recovered_service_->StopControl();
        }
    }

    Options options_{};
    ScopedDirectory temporary_{};
    DailyFixture daily_{};
    std::uint64_t maximum_records_ = 0U;
    std::uint64_t final_generation_ = 0U;
    std::shared_ptr<recovery::MdlLiveJournalV1> live_journal_;
    std::shared_ptr<ipc::RealtimeSharedMarketServiceV2> recovered_service_;
    std::unique_ptr<runtime::RealtimePipelineV1> shadow_pipeline_;
    std::shared_ptr<ipc::RealtimeSharedMarketServiceV2> preview_service_;
    std::shared_ptr<BenchmarkSdkState> sdk_state_;
    std::unique_ptr<runtime::RealtimePipelineV1> preview_pipeline_;
    std::shared_ptr<GatedCsvReplaySource> replay_source_;
    std::unique_ptr<recovery::OnlineRecoveryHandoffV1> handoff_;
    std::thread recovery_thread_;
    ReaderHandle reader_;
    RecoveryResult recovery_result_{};
    std::atomic<bool> promotion_done_{false};
    std::atomic<bool> recovery_thread_done_{false};
    bool recovery_released_ = false;
    bool journal_stopped_ = false;
};

struct RecoveredPolarsResult final {
    std::uint64_t generation = 0U;
    std::uint64_t records = 0U;
    std::uint64_t columns = 0U;
    std::uint64_t batches = 0U;
    std::uint64_t history_published_ns = 0U;
    std::uint64_t open_return_ns = 0U;
    std::uint64_t ready_ns = 0U;
    std::uint64_t probe_elapsed_ns = 0U;
    std::uint64_t dataframe_estimated_bytes = 0U;
};

[[nodiscard]] bool ParseUnsignedField(
    std::string_view line,
    std::string_view key,
    std::uint64_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    const std::string needle = " " + std::string(key) + "=";
    const std::size_t position = line.find(needle);
    if (position == std::string_view::npos) {
        return false;
    }
    const std::size_t begin = position + needle.size();
    const std::size_t end = line.find(' ', begin);
    const std::string_view value = line.substr(
        begin,
        end == std::string_view::npos ? line.size() - begin : end - begin);
    std::uint64_t parsed = 0U;
    const auto result = std::from_chars(
        value.data(), value.data() + value.size(), parsed);
    if (result.ec != std::errc{} ||
        result.ptr != value.data() + value.size()) {
        return false;
    }
    *output = parsed;
    return true;
}

[[nodiscard]] bool RunRecoveredPolarsProbe(
    const std::filesystem::path& socket_path,
    std::uint64_t generation,
    std::uint64_t expected_records,
    RecoveredPolarsResult* output) {
#if defined(L2FLOW_ONLINE_RECOVERY_PYTHON_EXECUTABLE) && \
    defined(L2FLOW_ONLINE_RECOVERY_POLARS_SCRIPT) && \
    defined(L2FLOW_ONLINE_RECOVERY_PYTHON_SOURCE) && \
    defined(L2FLOW_ONLINE_RECOVERY_READER_LIBRARY)
    if (output == nullptr || socket_path.empty() || generation == 0U ||
        expected_records == 0U) {
        return false;
    }
    *output = {};
    std::array<int, 2U> descriptors{-1, -1};
    if (::pipe2(descriptors.data(), O_CLOEXEC) != 0) {
        return false;
    }
    UniqueFd read_end(descriptors[0U]);
    UniqueFd write_end(descriptors[1U]);
    std::array<std::string, 9U> arguments{{
        L2FLOW_ONLINE_RECOVERY_PYTHON_EXECUTABLE,
        "-B",
        L2FLOW_ONLINE_RECOVERY_POLARS_SCRIPT,
        socket_path.string(),
        L2FLOW_ONLINE_RECOVERY_READER_LIBRARY,
        L2FLOW_ONLINE_RECOVERY_PYTHON_SOURCE,
        std::to_string(generation),
        std::to_string(expected_records),
        "1",
    }};
    std::array<char*, 10U> argv{};
    for (std::size_t index = 0U; index < arguments.size(); ++index) {
        argv[index] = arguments[index].data();
    }
    posix_spawn_file_actions_t actions{};
    if (::posix_spawn_file_actions_init(&actions) != 0) {
        return false;
    }
    bool actions_valid =
        ::posix_spawn_file_actions_adddup2(
            &actions, write_end.get(), STDOUT_FILENO) == 0 &&
        ::posix_spawn_file_actions_adddup2(
            &actions, write_end.get(), STDERR_FILENO) == 0 &&
        ::posix_spawn_file_actions_addclose(
            &actions, read_end.get()) == 0;
    if (write_end.get() != STDOUT_FILENO &&
        write_end.get() != STDERR_FILENO) {
        actions_valid = actions_valid &&
            ::posix_spawn_file_actions_addclose(
                &actions, write_end.get()) == 0;
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
    static_cast<void>(::posix_spawn_file_actions_destroy(&actions));
    if (spawn_error != 0 || child <= 0) {
        return false;
    }
    write_end.Reset();
    std::string child_output;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(180);
    bool timed_out = false;
    for (;;) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            timed_out = true;
            break;
        }
        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - now);
        pollfd descriptor{};
        descriptor.fd = read_end.get();
        descriptor.events = POLLIN | POLLHUP;
        const int poll_result = ::poll(
            &descriptor,
            1U,
            static_cast<int>(std::min<std::int64_t>(
                remaining.count(), 1'000)));
        if (poll_result < 0) {
            if (errno == EINTR) {
                continue;
            }
            timed_out = true;
            break;
        }
        if (poll_result == 0) {
            continue;
        }
        std::array<char, 4'096U> buffer{};
        const ssize_t read_count =
            ::read(read_end.get(), buffer.data(), buffer.size());
        if (read_count == 0) {
            break;
        }
        if (read_count < 0) {
            if (errno == EINTR) {
                continue;
            }
            timed_out = true;
            break;
        }
        child_output.append(
            buffer.data(), static_cast<std::size_t>(read_count));
        if (child_output.size() > 64U * 1024U) {
            timed_out = true;
            break;
        }
    }
    if (timed_out) {
        static_cast<void>(::kill(child, SIGKILL));
    }
    int status = 0;
    while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
    }
    std::cout << child_output;
    if (timed_out || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        return false;
    }
    std::string_view result_line;
    std::size_t begin = 0U;
    constexpr std::string_view kPrefix =
        "ONLINE_RECOVERY_POLARS_RESULT ";
    while (begin < child_output.size()) {
        const std::size_t end = child_output.find('\n', begin);
        const std::string_view line(
            child_output.data() + begin,
            (end == std::string::npos ? child_output.size() : end) -
                begin);
        if (line.starts_with(kPrefix)) {
            if (!result_line.empty()) {
                return false;
            }
            result_line = line;
        }
        if (end == std::string::npos) {
            break;
        }
        begin = end + 1U;
    }
    return !result_line.empty() &&
           ParseUnsignedField(result_line, "generation", &output->generation) &&
           ParseUnsignedField(result_line, "records", &output->records) &&
           ParseUnsignedField(result_line, "columns", &output->columns) &&
           ParseUnsignedField(result_line, "batches", &output->batches) &&
           ParseUnsignedField(
               result_line,
               "history_published_monotonic_ns",
               &output->history_published_ns) &&
           ParseUnsignedField(
               result_line, "open_return_ns", &output->open_return_ns) &&
           ParseUnsignedField(
               result_line, "polars_ready_ns", &output->ready_ns) &&
           ParseUnsignedField(
               result_line,
               "probe_elapsed_ns",
               &output->probe_elapsed_ns) &&
           ParseUnsignedField(
               result_line,
               "dataframe_estimated_bytes",
               &output->dataframe_estimated_bytes) &&
           output->generation == generation &&
           output->records == expected_records && output->columns != 0U &&
           output->batches != 0U &&
           output->history_published_ns <= output->open_return_ns &&
           output->open_return_ns <= output->ready_ns;
#else
    static_cast<void>(socket_path);
    static_cast<void>(generation);
    static_cast<void>(expected_records);
    static_cast<void>(output);
    return false;
#endif
}

[[nodiscard]] bool RunThroughputBenchmark(const Options& options) {
    const std::uint64_t planned = static_cast<std::uint64_t>(
        options.planned_live_records());
    std::cout
        << "ONLINE_RECOVERY_THROUGHPUT_ENV"
        << " mode=" << ModeName(options.mode)
        << " target_rps=" << options.throughput_rate
        << " duration_ms=" << options.throughput_duration_ms
        << " planned_callbacks=" << planned
        << " replay_records=" << options.replay_records
        << " parallel_decoder_workers="
        << options.parallel_decoder_workers
        << " polars=" << (options.polars ? 1 : 0)
        << " callback_contract=serialized"
        << " pacing=absolute_deadline_one_based_no_batch_wait"
        << " preview_state=LIVE_PARTIAL"
        << " recovered_state=ACTIVE_after_promotion"
        << " live_journal_queue_capacity=65536"
        << " live_journal_segment_bytes=268435456"
        << " live_journal_maximum_bytes=34359738368"
        << " clock=CLOCK_MONOTONIC\n";

    BenchmarkHarness harness(options);
    if (!harness.Initialize()) {
        return false;
    }
    ThroughputResult throughput{};
    if (!harness.MeasureThroughput(&throughput)) {
        return false;
    }
    const std::uint64_t producer_elapsed_ns =
        throughput.offer_complete_ns - throughput.start_ns;
    const long double achieved_offered_rps =
        producer_elapsed_ns == 0U
            ? 0.0L
            : static_cast<long double>(throughput.invoked_callbacks) *
                  1'000'000'000.0L /
                  static_cast<long double>(producer_elapsed_ns);
    const std::uint64_t backlog_budget = std::max<std::uint64_t>(
        1'024U,
        (static_cast<std::uint64_t>(options.throughput_rate) + 999U) /
            1'000U);
    const bool steady_state_met =
        throughput.sampled_quarters == 4U &&
        throughput.preview_backlog_at_quarter[3U] <= backlog_budget &&
        throughput.preview_backlog_at_quarter[3U] <=
            throughput.preview_backlog_at_quarter[1U] +
                backlog_budget &&
        throughput.journal_queue_at_quarter[3U] <=
            throughput.journal_queue_at_quarter[1U] + backlog_budget &&
        throughput.journal_uncommitted_at_quarter[3U] <=
            throughput.journal_uncommitted_at_quarter[1U] +
                backlog_budget;
    const bool offer_target_met =
        throughput.invoked_callbacks == planned &&
        achieved_offered_rps >=
            static_cast<long double>(options.throughput_rate) * 0.98L &&
        !throughput.fatal_during_offer &&
        throughput.preview_before_finish.accepted_messages == planned &&
        throughput.preview_before_finish.rejected_messages == 0U &&
        throughput.preview_before_finish.post_cut_messages == 0U &&
        throughput.journal_before_finish.healthy() &&
        throughput.journal_before_finish.accepted_serial == planned &&
        steady_state_met;

    runtime::RealtimePipelineSnapshotV1 preview_final =
        throughput.preview_before_finish;
    if (offer_target_met) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline) {
            preview_final = harness.preview_snapshot();
            if (preview_final.fatal ||
                (preview_final.decoded_messages == planned &&
                 preview_final.processing_progress.applied_sequence ==
                     planned &&
                 preview_final.store.appended_records == planned)) {
                break;
            }
            std::this_thread::yield();
        }
    }

    std::uint64_t history_records = 0U;
    bool history_complete = false;
    bool finish_complete = false;
    if (offer_target_met && !preview_final.fatal &&
        preview_final.decoded_messages == planned &&
        preview_final.processing_progress.applied_sequence == planned &&
        preview_final.store.appended_records == planned) {
        finish_complete = harness.Finish(
            &history_records, &history_complete);
    }
    const std::uint64_t history_ready_ns = MonotonicNowNs();
    const std::uint64_t history_ready_elapsed_ns =
        history_ready_ns >= throughput.start_ns
            ? history_ready_ns - throughput.start_ns
            : 0U;
    const long double recovered_history_ready_live_rps =
        !history_complete || history_ready_elapsed_ns == 0U
            ? 0.0L
            : static_cast<long double>(planned) * 1'000'000'000.0L /
                  static_cast<long double>(history_ready_elapsed_ns);
    const RecoveryResult& recovered = harness.recovery_result();
    const recovery::OnlineRecoverySnapshotV1 final_recovery =
        harness.final_recovery_snapshot();
    const recovery::LiveJournalSnapshotV1 journal_final =
        harness.journal_snapshot();
    const std::uint64_t recovery_duration_ns =
        recovered.release_ns != 0U &&
                recovered.promotion_ns >= recovered.release_ns
            ? recovered.promotion_ns - recovered.release_ns
            : 0U;
    const bool exact_preview_prefix =
        !preview_final.fatal &&
        preview_final.accepted_messages == planned &&
        preview_final.rejected_messages == 0U &&
        preview_final.post_cut_messages == 0U &&
        preview_final.decoded_messages == planned &&
        preview_final.processing_progress.applied_sequence == planned &&
        preview_final.store.appended_records == planned &&
        preview_final.store.failed_appends == 0U &&
        !preview_final.store.coverage_lost;
    const bool exact_journal =
        journal_final.healthy() &&
        journal_final.accepted_serial == planned &&
        journal_final.committed_serial == planned &&
        journal_final.queue_depth == 0U;
    const bool scenario_pass =
        offer_target_met && exact_preview_prefix && exact_journal &&
        finish_complete && history_complete &&
        throughput.promotion_overlap_callbacks != 0U &&
        recovered.boundary.ready() && recovered.mark_promoted &&
        recovered.recovered_service_started;

    std::cout
        << "ONLINE_RECOVERY_THROUGHPUT_RESULT"
        << " mode=" << ModeName(options.mode)
        << " target_rps=" << options.throughput_rate
        << " duration_ms=" << options.throughput_duration_ms
        << " planned_callbacks=" << planned
        << " invoked_callbacks=" << throughput.invoked_callbacks
        << " producer_elapsed_ns=" << producer_elapsed_ns
        << " achieved_offered_rps="
        << static_cast<double>(achieved_offered_rps)
        << " offer_target_met=" << (offer_target_met ? 1 : 0)
        << " scenario_pass=" << (scenario_pass ? 1 : 0)
        << " process_survived=1"
        << " fatal_during_offer="
        << (throughput.fatal_during_offer ? 1 : 0)
        << " steady_state_met=" << (steady_state_met ? 1 : 0)
        << " sampled_quarters=" << throughput.sampled_quarters
        << " backlog_budget=" << backlog_budget
        << " promotion_overlap_callbacks="
        << throughput.promotion_overlap_callbacks
        << " preview_accepted=" << preview_final.accepted_messages
        << " preview_rejected=" << preview_final.rejected_messages
        << " preview_post_cut=" << preview_final.post_cut_messages
        << " preview_decoded=" << preview_final.decoded_messages
        << " preview_applied="
        << preview_final.processing_progress.applied_sequence
        << " preview_store_appended="
        << preview_final.store.appended_records
        << " preview_store_failed="
        << preview_final.store.failed_appends
        << " preview_fatal=" << (preview_final.fatal ? 1 : 0)
        << " preview_backlog_q25="
        << throughput.preview_backlog_at_quarter[0U]
        << " preview_backlog_q50="
        << throughput.preview_backlog_at_quarter[1U]
        << " preview_backlog_q75="
        << throughput.preview_backlog_at_quarter[2U]
        << " preview_backlog_q100="
        << throughput.preview_backlog_at_quarter[3U]
        << " journal_accepted=" << journal_final.accepted_serial
        << " journal_committed=" << journal_final.committed_serial
        << " journal_queue_depth=" << journal_final.queue_depth
        << " journal_queue_high_water=" << journal_final.queue_high_water
        << " journal_queue_q25="
        << throughput.journal_queue_at_quarter[0U]
        << " journal_queue_q50="
        << throughput.journal_queue_at_quarter[1U]
        << " journal_queue_q75="
        << throughput.journal_queue_at_quarter[2U]
        << " journal_queue_q100="
        << throughput.journal_queue_at_quarter[3U]
        << " journal_uncommitted_q25="
        << throughput.journal_uncommitted_at_quarter[0U]
        << " journal_uncommitted_q50="
        << throughput.journal_uncommitted_at_quarter[1U]
        << " journal_uncommitted_q75="
        << throughput.journal_uncommitted_at_quarter[2U]
        << " journal_uncommitted_q100="
        << throughput.journal_uncommitted_at_quarter[3U]
        << " journal_records_read="
        << final_recovery.journal_records_read
        << " journal_suffix_publications="
        << final_recovery.journal_suffix_publications
        << " csv_published=" << harness.csv_published()
        << " recovery_duration_ns=" << recovery_duration_ns
        << " history_ready_elapsed_ns="
        << history_ready_elapsed_ns
        << " recovered_history_ready_live_rps="
        << static_cast<double>(recovered_history_ready_live_rps)
        << " history_records=" << history_records
        << " history_expected="
        << options.replay_records + planned
        << " history_complete=" << (history_complete ? 1 : 0)
        << " exact_preview_prefix="
        << (exact_preview_prefix ? 1 : 0)
        << " exact_journal=" << (exact_journal ? 1 : 0)
        << " promotion_ready="
        << (recovered.boundary.ready() ? 1 : 0)
        << " mark_promoted=" << (recovered.mark_promoted ? 1 : 0)
        << " recovered_service_started="
        << (recovered.recovered_service_started ? 1 : 0)
        << '\n';
    return scenario_pass;
}

[[nodiscard]] bool RunBenchmark(const Options& options) {
    std::cout
        << "ONLINE_RECOVERY_FAST_ENV"
        << " mode=" << ModeName(options.mode)
        << " warmup_samples=" << options.warmup_samples
        << " measured_samples=" << options.measured_samples
        << " pure_reads_per_sample=" << kPureReadsPerSample
        << " replay_records=" << options.replay_records
        << " parallel_decoder_workers="
        << options.parallel_decoder_workers
        << " polars=" << (options.polars ? 1 : 0)
        << " replay_source=mdl_csv_shenzhen_transaction"
        << " reader=wire_v2_c_latest_tick"
        << " clock=CLOCK_MONOTONIC"
        << " ctest_registered=0\n";

    BenchmarkHarness harness(options);
    if (!harness.Initialize()) {
        return false;
    }
    LatencyResult latency{};
    if (!harness.Measure(&latency)) {
        return false;
    }
    std::uint64_t history_records = 0U;
    bool history_complete = false;
    if (!harness.Finish(&history_records, &history_complete)) {
        return false;
    }

    bool polars_complete = !options.polars;
    RecoveredPolarsResult polars{};
    if (options.polars) {
        const std::uint64_t expected_records =
            static_cast<std::uint64_t>(options.replay_records) +
            static_cast<std::uint64_t>(
                options.warmup_samples + options.measured_samples);
        polars_complete =
            latency.first_measured_caller_before_callback_ns != 0U &&
            latency.last_measured_caller_before_callback_ns >=
                latency.first_measured_caller_before_callback_ns &&
            RunRecoveredPolarsProbe(
                harness.recovered_socket_path(),
                harness.final_generation(),
                expected_records,
                &polars) &&
            latency.last_measured_caller_before_callback_ns <=
                polars.history_published_ns &&
            polars.history_published_ns <= polars.ready_ns;
        if (!polars_complete) {
            return Fail(
                "recovered_polars",
                "recovered History Polars probe failed");
        }
        std::cout
            << "ONLINE_RECOVERY_POLARS_BOUNDARY"
            << " mode=" << ModeName(options.mode)
            << " replay_records=" << options.replay_records
            << " measured_records=" << options.measured_samples
            << " generation=" << polars.generation
            << " records=" << polars.records
            << " columns=" << polars.columns
            << " batches=" << polars.batches
            << " first_measured_caller_before_callback_ns="
            << latency.first_measured_caller_before_callback_ns
            << " last_measured_caller_before_callback_ns="
            << latency.last_measured_caller_before_callback_ns
            << " history_published_monotonic_ns="
            << polars.history_published_ns
            << " polars_ready_ns=" << polars.ready_ns
            << " strict_first_callback_to_polars_ns="
            << polars.ready_ns -
                   latency.first_measured_caller_before_callback_ns
            << " strict_last_callback_to_polars_ns="
            << polars.ready_ns -
                   latency.last_measured_caller_before_callback_ns
            << " publication_to_polars_ns="
            << polars.ready_ns - polars.history_published_ns
            << " probe_elapsed_ns=" << polars.probe_elapsed_ns
            << " dataframe_estimated_bytes="
            << polars.dataframe_estimated_bytes
            << '\n';
    }

    PrintLatency(options.mode, "callback_call", latency.callback_call_ns);
    PrintLatency(
        options.mode,
        "callback_origin_to_preview_latest",
        latency.callback_to_preview_ns);
    PrintLatency(
        options.mode,
        "wire_recv_to_preview_latest",
        latency.recv_to_preview_ns);
    PrintLatency(
        options.mode,
        "pure_latest_read_call",
        latency.pure_latest_read_ns);

    const RecoveryResult& recovered = harness.recovery_result();
    const recovery::OnlineRecoverySnapshotV1 final_recovery =
        harness.final_recovery_snapshot();
    const recovery::LiveJournalSnapshotV1 journal =
        harness.journal_snapshot();
    const std::uint64_t recovery_duration =
        recovered.release_ns != 0U &&
                recovered.promotion_ns >= recovered.release_ns
            ? recovered.promotion_ns - recovered.release_ns
            : 0U;
    const std::uint64_t promotion_records =
        recovered.promotion_cut.store_generation == nullptr
            ? 0U
            : recovered.promotion_cut.store_generation->record_count();
    const std::uint64_t recovery_records_per_second =
        recovery_duration == 0U
            ? 0U
            : static_cast<std::uint64_t>(
                  (static_cast<long double>(promotion_records) *
                   1'000'000'000.0L) /
                  static_cast<long double>(recovery_duration));

    std::cout
        << "ONLINE_RECOVERY_FAST_RESULT"
        << " mode=" << ModeName(options.mode)
        << " success=1"
        << " measured_samples=" << options.measured_samples
        << " parallel_decoder_workers="
        << options.parallel_decoder_workers
        << " pure_latest_samples=" << latency.pure_latest_read_ns.size()
        << " poll_calls=" << latency.poll_calls
        << " inconsistent_reads=" << latency.inconsistent_reads
        << " promotion_overlap_samples="
        << latency.promotion_overlap_samples
        << " recovery_duration_ns=" << recovery_duration
        << " recovery_records=" << promotion_records
        << " recovery_records_per_second="
        << recovery_records_per_second
        << " csv_published=" << harness.csv_published()
        << " journal_accepted=" << journal.accepted_serial
        << " journal_committed=" << journal.committed_serial
        << " journal_queue_high_water=" << journal.queue_high_water
        << " journal_records_read="
        << final_recovery.journal_records_read
        << " journal_overlap_digests="
        << final_recovery.journal_overlap_digests
        << " journal_suffix_digests_skipped="
        << final_recovery.journal_live_suffix_digests_skipped
        << " journal_suffix_publications="
        << final_recovery.journal_suffix_publications
        << " replay_throttle_events="
        << final_recovery.replay_throttle_events
        << " replay_pause_events="
        << final_recovery.replay_pause_events
        << " csv_parser_checkpoint_events="
        << final_recovery.csv_parser_checkpoint_events
        << " preview_pause_events="
        << final_recovery.preview_pause_events
        << " shadow_pause_events="
        << final_recovery.shadow_pause_events
        << " cooperative_yield_events="
        << final_recovery.cooperative_yield_events
        << " history_records=" << history_records
        << " history_expected="
        << options.replay_records + options.warmup_samples +
               options.measured_samples
        << " history_complete=" << (history_complete ? 1 : 0)
        << " polars_complete=" << (polars_complete ? 1 : 0)
        << " promotion_ready="
        << (options.mode == BenchmarkMode::kOrdinary ||
                    recovered.boundary.ready()
                ? 1
                : 0)
        << '\n';
    const bool exact_latency_sample_count =
        latency.callback_call_ns.size() == options.measured_samples &&
        latency.callback_to_preview_ns.size() ==
            options.measured_samples &&
        latency.recv_to_preview_ns.size() == options.measured_samples &&
        latency.pure_latest_read_ns.size() ==
            options.measured_samples * kPureReadsPerSample;
    if (!exact_latency_sample_count) {
        std::cerr << "latency sample cardinality mismatch\n";
    }
    return history_complete && exact_latency_sample_count &&
           polars_complete;
}

}  // namespace

int main(int argc, char** argv) {
    Options options{};
    if (!ParseOptions(argc, argv, &options)) {
        std::cerr
            << "usage: benchmark_online_recovery_fast_v1 --mode "
               "ordinary|parked|active [--warmup-samples N] [--samples N] "
               "[--replay-records N] [--parallel-decoder-workers N] "
               "[--throughput-rate N --throughput-duration-ms N]\n";
        return 2;
    }
    return (options.throughput_enabled()
                ? RunThroughputBenchmark(options)
                : RunBenchmark(options))
               ? 0
               : 1;
}
