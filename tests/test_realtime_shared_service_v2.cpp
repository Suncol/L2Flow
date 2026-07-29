#include "l2flow/ipc/realtime_shared_service_v2.h"
#include "l2flow/ipc/realtime_shm_reader_c_v2.h"
#include "l2flow/ipc/realtime_wire_v2.h"
#include "l2flow/market/observed_instrument_directory_v2.h"
#include "l2flow/market/realtime_history_v1.h"
#include "l2flow/runtime/realtime_pipeline_v1.h"
#include "l2flow/sdk/market_message_catalog_v1.h"
#include "l2flow/sdk/vendor_head_view.h"

#include <array>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <signal.h>
#include <spawn.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/wait.h>
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

class JournalSyncBlocker final {
public:
    ~JournalSyncBlocker() { Release(); }

    JournalSyncBlocker(const JournalSyncBlocker&) = delete;
    JournalSyncBlocker& operator=(const JournalSyncBlocker&) = delete;
    JournalSyncBlocker() = default;

    static void BeforeSync(void* context) noexcept {
        auto* const blocker =
            static_cast<JournalSyncBlocker*>(context);
        if (blocker == nullptr) {
            return;
        }
        try {
            std::unique_lock<std::mutex> lock(blocker->mutex_);
            blocker->entered_ = true;
            blocker->condition_.notify_all();
            blocker->condition_.wait(lock, [blocker] {
                return blocker->released_;
            });
        } catch (...) {
            blocker->Release();
        }
    }

    [[nodiscard]] bool WaitUntilEntered(
        std::chrono::nanoseconds timeout) {
        try {
            std::unique_lock<std::mutex> lock(mutex_);
            return condition_.wait_for(lock, timeout, [this] {
                return entered_;
            });
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
            condition_.notify_all();
        }
    }

private:
    std::mutex mutex_;
    std::condition_variable condition_;
    bool entered_ = false;
    bool released_ = false;
};

class PipelineJournalCleanup final {
public:
    PipelineJournalCleanup(
        JournalSyncBlocker* blocker,
        std::unique_ptr<runtime::RealtimePipelineV1>* pipeline)
        : blocker_(blocker), pipeline_(pipeline) {}

    PipelineJournalCleanup(const PipelineJournalCleanup&) = delete;
    PipelineJournalCleanup& operator=(
        const PipelineJournalCleanup&) = delete;

    ~PipelineJournalCleanup() {
        if (blocker_ != nullptr) {
            blocker_->Release();
        }
        if (pipeline_ != nullptr && *pipeline_ != nullptr) {
            (*pipeline_)->StopAndDrain();
        }
    }

private:
    JournalSyncBlocker* blocker_ = nullptr;
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

std::vector<std::byte> PipelineShenzhenSnapshotBody() {
    PipelineWireWriter writer(224U);
    writer.StoreU32(0U, 93'000'123U);
    writer.StoreU32(4U, 12U);
    writer.StoreU64(32U, 12'000'000U);
    writer.StoreU64(40U, 1U);
    writer.StoreU64(48U, 100U);
    writer.StoreU64(56U, 1'234'560U);
    writer.StoreU64(64U, 12'345'600U);
    writer.StoreString(8U, "010");
    writer.StoreString(14U, "000001");
    writer.StoreString(20U, "102 ");
    writer.StoreString(26U, "T");
    return std::move(writer).Take();
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

private:
    mdl::MDLMessageHead head_{};
    std::vector<std::byte> body_;
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

std::unique_ptr<market::ObservedInstrumentDirectoryV2>
MakeDirectory(std::size_t capacity, std::uint64_t epoch) {
    market::ObservedInstrumentDirectoryConfigV2 config{};
    config.capacity = capacity;
    config.session_epoch = epoch;
    std::unique_ptr<market::ObservedInstrumentDirectoryV2> result;
    return market::ObservedInstrumentDirectoryV2::Create(
               config, &result) ==
               market::ObservedInstrumentDirectoryErrorV2::kNone
           ? std::move(result)
           : nullptr;
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

market::ObservedInstrumentMetadataV2 Metadata() {
    market::ObservedInstrumentMetadataV2 result{};
    result.quantity_unit = market::QuantityUnitV1::kShare;
    result.security_type = market::SecurityTypeV1::kEquity;
    result.asset_scope =
        market::AssetScopeV1::kDocumentedCore;
    return result;
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
    tick.fields.side = market::SideV1::kBuy;
    tick.fields.order_type = market::OrderTypeV1::kLimit;
    tick.fields.price.valid = true;
    tick.fields.price.raw = 1'235'000;
    tick.fields.price.normalized_p6 = 1'235'000;
    tick.fields.price.scale = 6U;
    tick.fields.quantity.valid = true;
    tick.fields.quantity.raw = 101;
    tick.fields.quantity.scale = 0U;
    tick.fields.validity_bitmap =
        market::kTickPriceValidV1 |
        market::kTickQuantityValidV1 |
        market::kTickExchangeTimeValidV1;
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

bool TestServiceEndToEnd() {
    ScopedTempDirectory temporary;
    auto directory = MakeDirectory(4U, kSessionEpoch);
    if (!Expect(temporary.valid(), "create secure temp directory") ||
        !Expect(directory != nullptr, "create observed directory")) {
        return false;
    }
    const std::filesystem::path socket_path =
        temporary.path() / "market.sock";
    const common::Identity128 run_id = RunId(0x29U);

    ipc::RealtimeSharedServiceConfigV2 service_config{};
    service_config.run_id = run_id;
    service_config.session_epoch = kSessionEpoch;
    service_config.trade_date = kTradeDate;
    service_config.directory = directory.get();
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
        "start service with empty observed catalog");

    SessionTransfer invalid =
        RequestSession(socket_path, 2U);
    ok &= Expect(
        invalid.response.status ==
                static_cast<std::uint16_t>(
                    ipc::RealtimeControlStatusV2::kInvalidRequest) &&
            invalid.fd.get() < 0,
        "control plane exposes only GET_SESSION");

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
            session.capacity == 4U && session.bound_count == 0U &&
            session.available_count == 0U &&
            session.catalog_scope ==
                static_cast<std::uint32_t>(
                    ipc::RealtimeCatalogScopeV2::kObservedOnly) &&
            session.coverage_complete == 0U,
        "bound=0 mapping is immediately ACTIVE and observed-only");

    market::InstrumentKeyV1 key = Key("101", "600001");
    market::ObservedInstrumentBindResultV2 binding{};
    ok &= Expect(
        directory->BindOrGet(
            key, Metadata(), 1U, &binding) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            binding.newly_bound &&
            service->PublishObservedInstrumentBinding(binding),
        "release-publish first dynamic binding");
    const std::uint64_t used_after_binding =
        service->key_arena_used_bytes();
    ok &= Expect(
        used_after_binding == 9U,
        "binding consumes exact opaque key bytes");

    market::ObservedInstrumentBindResultV2 repeated_binding{};
    ok &= Expect(
        directory->BindOrGet(
            key, Metadata(), 2U, &repeated_binding) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            !repeated_binding.newly_bound &&
            repeated_binding.catalog_generation ==
                binding.catalog_generation &&
            repeated_binding.catalog_digest ==
                binding.catalog_digest &&
            service->key_arena_used_bytes() ==
                used_after_binding,
        "repeated BindOrGet does not republish catalog or key arena");

    market::RealtimeHistoryRuntimeConfigV1 runtime_config{};
    runtime_config.source_stream_ids = kSourceStreamIds;
    runtime_config.worker_count = 1U;
    runtime_config.queue_capacity_per_source_worker = 16U;
    runtime_config.directory = directory.get();
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

    ok &= Expect(
        Submit(runtime.get(), SnapshotInput(1U, 1U)) &&
            Submit(runtime.get(), SnapshotInput(2U, 2U)) &&
            Submit(runtime.get(), TickInput(1U, 3U, 1U)),
        "submit snapshot, repeated snapshot, and tick");
    ok &= Expect(
        WaitUntil([&] {
            std::shared_ptr<
                const market::ObservedInstrumentCatalogSnapshotV2>
                snapshot;
            return directory->AcquireSnapshot(&snapshot) ==
                       market::ObservedInstrumentDirectoryErrorV2::
                           kNone &&
                   snapshot != nullptr &&
                   snapshot->available_count() == 1U &&
                   snapshot->snapshot_available_count() == 1U &&
                   snapshot->tick_available_count() == 1U &&
                   snapshot->factor_eligible_count() == 1U;
        }),
        "history applies availability to the directory");

    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2>
        catalog;
    ok &= Expect(
        directory->AcquireSnapshot(&catalog) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
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
            1U,
            kTradeDate,
            4U,
            50'000U,
            catalog,
            realtime::ProcessingProgressV2{3U, 3U, 3U},
            sources,
            &watermark) ==
            market::RealtimeHistoryWatermarkErrorV1::kNone,
        "build observed-universe generation watermark");
    ok &= Expect(
        runtime->BeginGeneration(watermark) ==
            market::RealtimeHistoryGenerationErrorV1::kNone,
        "begin KLine generation");
    for (std::uint8_t source = 0U; source < 4U; ++source) {
        ok &= Expect(
            runtime->SealSource(source, 1U) ==
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
            1U,
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
        service->PublishProcessingProgress({10U, 7U, 9U}) &&
            service->PublishProcessingProgress({9U, 8U, 7U}),
        "progress publication merges concurrent-stale pairs monotonically");
    ok &= Expect(
        l2flow_shm_reader_session_v2(
            reader.get(), &session) ==
                L2FLOW_SHM_READER_OK_V2 &&
            session.bound_count == 1U &&
            session.available_count == 1U &&
            session.snapshot_available_count == 1U &&
            session.tick_available_count == 1U &&
            session.factor_eligible_count == 1U &&
            session.accepted_sequence == 10U &&
            session.durable_sequence == 8U &&
            session.applied_sequence == 9U &&
            session.processing_lag_records == 1U &&
            session.durability_lag_records == 2U &&
            session.kline_generation == 1U &&
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
                session.accepted_sequence &&
            session.durable_sequence <=
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
            latest_kline.generation == 1U &&
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
        Submit(runtime.get(), TickInput(2U, 4U, 2U)),
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
                   latest_tick.common.tick_stream_sequence == 2U;
        }),
        "DRAINING tick reaches latest and contiguous ring");
    ok &= Expect(
        service->PublishProcessingProgress({12U, 11U, 12U}),
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
            session.bound_count == 1U &&
            session.available_count == 1U &&
            session.snapshot_available_count == 1U &&
            session.tick_available_count == 1U &&
            session.factor_eligible_count == 1U &&
            session.accepted_sequence == 12U &&
            session.durable_sequence == 11U &&
            session.applied_sequence == 12U,
        "repeated existing-ID traffic leaves catalog/key/counts unchanged");

    runtime->StopAndDrain();
    ok &= Expect(
        service->MarkStoppedClean(2U),
        "STOPPED_CLEAN requires exact highest and contiguous tick watermarks");
    ok &= Expect(
        l2flow_shm_reader_session_v2(
            reader.get(), &session) ==
                L2FLOW_SHM_READER_OK_V2 &&
            session.server_state ==
                static_cast<std::uint32_t>(
                    ipc::RealtimeServerStateV2::kStoppedClean) &&
            session.tick_highest_published_sequence == 2U &&
            session.tick_contiguous_published_sequence == 2U,
        "clean terminal mapping preserves exact ring watermark");
    service->StopControl();
    return ok && !service->failed();
}

bool TestBlockedJournalDoesNotGateWireLatest() {
    ScopedTempDirectory temporary;
    auto directory = MakeDirectory(4U, 33U);
    if (!Expect(
            temporary.valid() && directory != nullptr,
            "create async-Journal integration fixture")) {
        return false;
    }

    const common::Identity128 run_id = RunId(0x33U);
    const std::filesystem::path socket_path =
        temporary.path() / "async-journal.sock";
    const std::filesystem::path journal_path =
        temporary.path() / "async-capture.journal";

    ipc::RealtimeSharedServiceConfigV2 service_config{};
    service_config.run_id = run_id;
    service_config.session_epoch = 33U;
    service_config.trade_date = kTradeDate;
    service_config.directory = directory.get();
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
        "create async-Journal Wire V2 service");
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
        "start async-Journal Wire V2 control plane");
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
        "open real C Reader before the first instrument is observed");
    if (reader.get() == nullptr) {
        service->MarkFailed();
        service->StopControl();
        return false;
    }
    transfer.fd.Reset();

    JournalSyncBlocker sync_blocker;
    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    // This guard is destroyed before both objects: every return first
    // releases a possibly blocked writer and then joins the Pipeline while
    // the hook context remains alive.
    PipelineJournalCleanup pipeline_cleanup(
        &sync_blocker, &pipeline);
    runtime::RealtimePipelineConfigV1 pipeline_config{};
    pipeline_config.run_id = run_id;
    pipeline_config.trade_date = kTradeDate;
    pipeline_config.directory = directory.get();
    pipeline_config.source_stream_ids = kSourceStreamIds;
    pipeline_config.maximum_sdk_message_bytes = 4096U;
    pipeline_config.processing_queue_capacity = 16U;
    pipeline_config.decoder_queue_capacity_per_source = 1U;
    pipeline_config.store_worker_count = 1U;
    pipeline_config.store_queue_capacity_per_source_worker = 16U;
    pipeline_config.intraday_store.segment_target_bytes =
        market::kIntradayInstrumentStoreMinimumSegmentBytesV1;
    pipeline_config.intraday_store.maximum_session_records = 16U;
    pipeline_config.intraday_store.maximum_session_accounted_bytes =
        1U << 20U;
    pipeline_config.intraday_store.maximum_records_per_batch = 16U;
    pipeline_config.intraday_store.coverage_from_open = true;
    pipeline_config.journal.path = journal_path.string();
    pipeline_config.journal.queue_capacity = 8U;
    pipeline_config.journal.max_batch_records = 1U;
    pipeline_config.journal.max_batch_delay =
        std::chrono::microseconds(50);
    pipeline_config.journal.before_sync_for_test =
        &JournalSyncBlocker::BeforeSync;
    pipeline_config.journal.before_sync_context_for_test =
        &sync_blocker;
    pipeline_config.applied_record_sink = service;
    pipeline_config.instrument_binding_sink = service;
    pipeline_config.processing_progress_sink = service;
    pipeline_config.sdk.enabled = false;

    ok &= Expect(
        !std::filesystem::exists(journal_path),
        "mandatory Journal path is fresh before Pipeline creation");
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
    constexpr std::uint64_t kBlockedCaptureCount = 6U;
    const runtime::RealtimePipelineIngressResultV1 first_ingress =
        pipeline->InjectSdkMessageForTest(&snapshot);
    const bool writer_is_blocked =
        sync_blocker.WaitUntilEntered(std::chrono::seconds(3));
    ok &= Expect(
        first_ingress.accepted() &&
            first_ingress.global_ingress_sequence == 1U &&
            writer_is_blocked,
        "capture is admitted and writer blocks immediately before fdatasync");
    for (std::uint64_t sequence = 2U;
         sequence <= kBlockedCaptureCount;
         ++sequence) {
        const runtime::RealtimePipelineIngressResultV1 ingress =
            pipeline->InjectSdkMessageForTest(&snapshot);
        ok &= Expect(
            ingress.accepted() &&
                ingress.global_ingress_sequence == sequence,
            "subsequent callback returns while the Journal writer remains "
            "blocked");
    }

    l2flow_shm_session_info_v2 blocked_session{};
    ipc::RealtimeWireSnapshotPayloadV2 latest{};
    std::uint8_t latest_status = 0xffU;
    constexpr std::uint32_t instrument_id = 1U;
    const bool visible_while_not_durable = WaitUntil([&] {
        latest_status = 0xffU;
        return l2flow_shm_reader_session_v2(
                   reader.get(), &blocked_session) ==
                   L2FLOW_SHM_READER_OK_V2 &&
               blocked_session.server_state ==
                   static_cast<std::uint32_t>(
                       ipc::RealtimeServerStateV2::kActive) &&
               blocked_session.catalog_scope ==
                   static_cast<std::uint32_t>(
                       ipc::RealtimeCatalogScopeV2::kObservedOnly) &&
               blocked_session.coverage_complete == 0U &&
               blocked_session.bound_count == 1U &&
               blocked_session.available_count == 1U &&
               blocked_session.snapshot_available_count == 1U &&
               blocked_session.accepted_sequence ==
                   kBlockedCaptureCount &&
               blocked_session.applied_sequence ==
                   kBlockedCaptureCount &&
               blocked_session.durable_sequence == 0U &&
               blocked_session.processing_lag_records == 0U &&
               blocked_session.durability_lag_records ==
                   kBlockedCaptureCount &&
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
                   kBlockedCaptureCount &&
               latest.last_price.valid == 1U &&
               latest.last_price.normalized_p6 == 12'345'600;
    });
    ok &= Expect(
        visible_while_not_durable,
        "real C Reader sees the full accepted/applied capture prefix and "
        "latest while durable=0");

    if (visible_while_not_durable) {
        ok &= RunPythonKnownIdProbe(socket_path);
        l2flow_shm_session_info_v2 after_python{};
        ok &= Expect(
            l2flow_shm_reader_session_v2(
                reader.get(), &after_python) ==
                    L2FLOW_SHM_READER_OK_V2 &&
                after_python.accepted_sequence ==
                    kBlockedCaptureCount &&
                after_python.applied_sequence ==
                    kBlockedCaptureCount &&
                after_python.durable_sequence == 0U,
            "Python hot reads complete while Journal sync remains blocked");
    }

    sync_blocker.Release();
    const bool durable_after_release = WaitUntil([&] {
        l2flow_shm_session_info_v2 session{};
        const runtime::RealtimePipelineSnapshotV1 state =
            pipeline->Snapshot();
        return l2flow_shm_reader_session_v2(
                   reader.get(), &session) ==
                   L2FLOW_SHM_READER_OK_V2 &&
               session.accepted_sequence == kBlockedCaptureCount &&
               session.applied_sequence == kBlockedCaptureCount &&
               session.durable_sequence == kBlockedCaptureCount &&
               session.processing_lag_records == 0U &&
               session.durability_lag_records == 0U &&
               state.processing_progress.accepted_sequence ==
                   kBlockedCaptureCount &&
               state.processing_progress.applied_sequence ==
                   kBlockedCaptureCount &&
               state.processing_progress.durable_sequence ==
                   kBlockedCaptureCount &&
               !state.fatal;
    });
    ok &= Expect(
        durable_after_release,
        "durable sequence advances independently after releasing fdatasync");

    pipeline->StopAndDrain();
    service->MarkDraining();
    ok &= Expect(
        service->MarkStoppedClean(0U),
        "async-Journal integration service stops with no tick gap");
    service->StopControl();
    return ok && !pipeline->fatal() && !service->failed();
}

bool TestKeyArenaExhaustionIsFatal() {
    ScopedTempDirectory temporary;
    auto directory = MakeDirectory(2U, 30U);
    if (!Expect(
            temporary.valid() && directory != nullptr,
            "create exhaustion fixture")) {
        return false;
    }
    ipc::RealtimeSharedServiceConfigV2 config{};
    config.run_id = RunId(0x30U);
    config.session_epoch = 30U;
    config.trade_date = kTradeDate;
    config.directory = directory.get();
    config.tick_ring_capacity = 2U;
    config.key_arena_bytes = 2U;
    config.maximum_mapping_bytes = 8U * 1024U * 1024U;
    config.control_socket_path =
        temporary.path() / "exhaust.sock";
    std::shared_ptr<ipc::RealtimeSharedMarketServiceV2> service;
    bool ok = Expect(
        ipc::RealtimeSharedMarketServiceV2::Create(
            config, &service) ==
                ipc::RealtimeSharedServiceCreateErrorV2::kNone &&
            service != nullptr,
        "create fixed tiny key arena");
    if (!ok) {
        return false;
    }
    market::ObservedInstrumentBindResultV2 first{};
    market::InstrumentKeyV1 first_key = Key("A", "1");
    ok &= Expect(
        directory->BindOrGet(
            first_key, Metadata(), 1U, &first) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            service->PublishObservedInstrumentBinding(first) &&
            service->key_arena_used_bytes() == 2U,
        "fill fixed key arena exactly");
    market::ObservedInstrumentBindResultV2 second{};
    market::InstrumentKeyV1 second_key = Key("", "22");
    ok &= Expect(
        directory->BindOrGet(
            second_key, Metadata(), 2U, &second) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            !service->PublishObservedInstrumentBinding(second) &&
            service->failed() &&
            service->key_arena_used_bytes() == 2U,
        "key arena exhaustion fails session without rollover");
    service->StopControl();
    return ok;
}

bool TestStoppedCleanRejectsMismatchedWatermark() {
    ScopedTempDirectory temporary;
    auto directory = MakeDirectory(1U, 31U);
    if (!Expect(
            temporary.valid() && directory != nullptr,
            "create terminal-watermark fixture")) {
        return false;
    }
    ipc::RealtimeSharedServiceConfigV2 config{};
    config.run_id = RunId(0x31U);
    config.session_epoch = 31U;
    config.trade_date = kTradeDate;
    config.directory = directory.get();
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
    auto directory = MakeDirectory(1U, 32U);
    if (!Expect(
            temporary.valid() && directory != nullptr,
            "create tick-gap fixture")) {
        return false;
    }
    ipc::RealtimeSharedServiceConfigV2 config{};
    config.run_id = RunId(0x32U);
    config.session_epoch = 32U;
    config.trade_date = kTradeDate;
    config.directory = directory.get();
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

    market::ObservedInstrumentBindResultV2 binding{};
    market::InstrumentKeyV1 key = Key("101", "600001");
    ok &= Expect(
        directory->BindOrGet(
            key, Metadata(), 1U, &binding) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            service->PublishObservedInstrumentBinding(binding),
        "bind tick-gap instrument");

    market::RealtimeHistoryRuntimeConfigV1 runtime_config{};
    runtime_config.source_stream_ids = kSourceStreamIds;
    runtime_config.worker_count = 1U;
    runtime_config.queue_capacity_per_source_worker = 4U;
    runtime_config.directory = directory.get();
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
                    const market::ObservedInstrumentCatalogSnapshotV2>
                    snapshot;
                return directory->AcquireSnapshot(&snapshot) ==
                           market::ObservedInstrumentDirectoryErrorV2::
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

}  // namespace

int main() {
    if (!TestServiceEndToEnd() ||
        !TestBlockedJournalDoesNotGateWireLatest() ||
        !TestKeyArenaExhaustionIsFatal() ||
        !TestStoppedCleanRejectsMismatchedWatermark() ||
        !TestTickRingRejectsUnclosedGapOverwrite()) {
        return 1;
    }
    std::cout << "realtime shared service V2 tests passed\n";
    return 0;
}
