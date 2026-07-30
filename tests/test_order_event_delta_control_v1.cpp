#include "l2flow/ipc/order_event_delta_control_v1.h"

#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>

#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {

namespace ipc = l2flow::ipc;

constexpr std::uint32_t kTradeDate = 20260730U;
constexpr std::uint64_t kSourceSessionEpoch = 71U;

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAILED: " << message << '\n';
    }
    return condition;
}

l2flow::common::Identity128 Identity(std::uint8_t seed) {
    l2flow::common::Identity128 result{};
    for (std::size_t index = 0U; index < result.size(); ++index) {
        result[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                seed + static_cast<std::uint8_t>(index)));
    }
    return result;
}

ipc::OrderEventDeltaSourceSessionV1 SourceSession() {
    ipc::OrderEventDeltaSourceSessionV1 result{};
    result.run_id = Identity(90U);
    result.session_epoch = kSourceSessionEpoch;
    result.trade_date = kTradeDate;
    return result;
}

ipc::OrderEventDeltaRingConfigV1 RingConfig(
    std::uint8_t seed = 10U) {
    ipc::OrderEventDeltaRingConfigV1 result{};
    result.run_id = Identity(seed);
    result.session_epoch = 19U;
    result.trade_date = kTradeDate;
    result.ring_capacity = 8U;
    result.maximum_mapping_bytes = 16ULL * 1024ULL * 1024ULL;
    result.producer_started_monotonic_ns = 1'234U;
    return result;
}

ipc::OrderEventDeltaPayloadV1 OrderEvent(
    std::uint64_t source_tick_sequence) {
    ipc::OrderEventDeltaPayloadV1 result{};
    result.record_schema_version =
        ipc::kOrderEventDeltaPayloadSchemaV1;
    result.record_bytes = sizeof(result);
    result.trade_date = kTradeDate;
    result.instrument_id = 1U;
    result.channel = 1;
    result.market =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1;
    result.event_kind =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_REVISION_V1;
    result.native_event_sequence =
        static_cast<std::int64_t>(source_tick_sequence);
    result.source_sequence = source_tick_sequence;
    result.ingress_sequence = source_tick_sequence;
    result.tick_stream_sequence = source_tick_sequence;
    result.vendor_sequence_id = source_tick_sequence;
    result.order_id = 100;
    result.revision = 1U;
    result.original_quantity = 500;
    result.original_quantity_valid = 1U;
    result.remaining_quantity = 500;
    result.remaining_quantity_valid = 1U;
    result.side = 1U;
    return result;
}

std::unique_ptr<ipc::OrderEventDeltaRingProducerV1> MakeProducer(
    bool* ok,
    std::uint8_t seed = 10U) {
    std::unique_ptr<ipc::OrderEventDeltaRingProducerV1> result;
    int system_error = -1;
    const auto error =
        ipc::OrderEventDeltaRingProducerV1::Create(
            RingConfig(seed), &result, &system_error);
    *ok &= Expect(
        error == ipc::OrderEventDeltaRingCreateErrorV1::kNone &&
            result != nullptr && system_error == 0,
        "create event ring");
    return result;
}

ipc::OrderEventDeltaControlServerConfigV1 ServerConfig(
    const std::filesystem::path& socket_path,
    const ipc::OrderEventDeltaRingProducerV1* producer) {
    ipc::OrderEventDeltaControlServerConfigV1 result{};
    result.source_session = SourceSession();
    result.event_ring = producer;
    result.control_socket_path = socket_path;
    result.request_timeout = std::chrono::milliseconds(250);
    return result;
}

ipc::OrderEventDeltaControlClientConfigV1 ClientConfig(
    const std::filesystem::path& socket_path) {
    ipc::OrderEventDeltaControlClientConfigV1 result{};
    result.control_socket_path = socket_path;
    result.expected_source_session = SourceSession();
    result.timeout = std::chrono::milliseconds(500);
    return result;
}

std::filesystem::path MakePrivateDirectory(bool* ok) {
    std::array<char, 64U> pattern{};
    const char source[] = "/tmp/l2flow-event-control-XXXXXX";
    static_assert(sizeof(source) <= pattern.size());
    std::memcpy(pattern.data(), source, sizeof(source));
    char* const path = ::mkdtemp(pattern.data());
    *ok &= Expect(path != nullptr, "create private test directory");
    return path == nullptr ? std::filesystem::path{}
                           : std::filesystem::path(path);
}

void CleanupDirectory(const std::filesystem::path& path) {
    if (!path.empty()) {
        static_cast<void>(::rmdir(path.c_str()));
    }
}

int ChildClient(const char* socket_path) {
    if (socket_path == nullptr) {
        return 20;
    }
    ipc::OrderEventDeltaControlSnapshotV1 probe{};
    int system_error = -1;
    if (ipc::OrderEventDeltaControlProbeV1(
            ClientConfig(socket_path), &probe, &system_error) !=
            ipc::OrderEventDeltaControlClientErrorV1::kNone ||
        system_error != 0 ||
        probe.source_session != SourceSession() ||
        probe.event_producer_state !=
            static_cast<std::uint32_t>(
                ipc::OrderEventDeltaProducerStateV1::kActive)) {
        return 21;
    }

    ipc::OrderEventDeltaControlSnapshotV1 connected{};
    std::unique_ptr<ipc::OrderEventDeltaRingReaderV1> reader;
    if (ipc::OrderEventDeltaControlConnectV1(
            ClientConfig(socket_path),
            &connected,
            &reader,
            &system_error) !=
            ipc::OrderEventDeltaControlClientErrorV1::kNone ||
        reader == nullptr || system_error != 0 ||
        connected.event_session != reader->session()) {
        return 22;
    }
    std::array<ipc::OrderEventDeltaPayloadV1, 2U> rows{};
    ipc::OrderEventDeltaReadResultV1 read{};
    if (reader->Read(1U, rows, &read) !=
            ipc::OrderEventDeltaReadErrorV1::kNone ||
        read.written != 1U || read.next_sequence != 2U ||
        rows[0U].derived_event_sequence != 1U ||
        rows[0U].tick_stream_sequence != 1U) {
        return 23;
    }
    return 0;
}

bool SpawnExecClient(const std::filesystem::path& socket_path) {
    const pid_t child = ::fork();
    if (child < 0) {
        return false;
    }
    if (child == 0) {
        ::execl(
            "/proc/self/exe",
            "/proc/self/exe",
            "--child",
            socket_path.c_str(),
            static_cast<char*>(nullptr));
        ::_exit(127);
    }
    int status = 0;
    pid_t waited = -1;
    do {
        waited = ::waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    return waited == child && WIFEXITED(status) &&
           WEXITSTATUS(status) == 0;
}

void TestWireAbi(bool* ok) {
    *ok &= Expect(
        sizeof(
            ipc::OrderEventDeltaControlGetSessionRequestV1) ==
                80U &&
            sizeof(
                ipc::OrderEventDeltaControlGetSessionResponseV1) ==
                192U,
        "fixed-width control wire sizes");
}

void TestPathAndStartValidation(
    const std::filesystem::path& directory,
    bool* ok) {
    auto producer = MakeProducer(ok, 20U);
    if (producer == nullptr) {
        return;
    }
    {
        auto config =
            ServerConfig("relative.sock", producer.get());
        std::unique_ptr<ipc::OrderEventDeltaControlServerV1> server;
        *ok &= Expect(
            ipc::OrderEventDeltaControlServerV1::Create(
                config, &server) ==
                    ipc::
                        OrderEventDeltaControlServerCreateErrorV1::
                            kInvalidConfiguration &&
                server == nullptr,
            "relative socket path rejected");
    }
    {
        const std::filesystem::path existing =
            directory / "existing.sock";
        const int descriptor = ::open(
            existing.c_str(),
            O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC,
            0600);
        *ok &= Expect(
            descriptor >= 0, "create pre-existing path");
        if (descriptor >= 0) {
            static_cast<void>(::close(descriptor));
        }
        std::unique_ptr<ipc::OrderEventDeltaControlServerV1> server;
        *ok &= Expect(
            ipc::OrderEventDeltaControlServerV1::Create(
                ServerConfig(existing, producer.get()),
                &server) ==
                    ipc::
                        OrderEventDeltaControlServerCreateErrorV1::
                            kSocketPathExists &&
                server == nullptr,
            "server refuses to unlink pre-existing path");
        struct stat retained {};
        *ok &= Expect(
            ::lstat(existing.c_str(), &retained) == 0 &&
                S_ISREG(retained.st_mode),
            "pre-existing path retained");
        static_cast<void>(::unlink(existing.c_str()));
    }
    {
        const std::filesystem::path socket_path =
            directory / "not-ready.sock";
        std::unique_ptr<ipc::OrderEventDeltaControlServerV1> server;
        int create_system_error = -1;
        const auto create_error =
            ipc::OrderEventDeltaControlServerV1::Create(
                ServerConfig(socket_path, producer.get()),
                &server,
                &create_system_error);
        if (create_error !=
            ipc::OrderEventDeltaControlServerCreateErrorV1::kNone) {
            std::cerr << "create error="
                      << ipc::
                             OrderEventDeltaControlServerCreateErrorNameV1(
                                 create_error)
                      << " errno=" << create_system_error << '\n';
        }
        *ok &= Expect(
            create_error ==
                    ipc::
                        OrderEventDeltaControlServerCreateErrorV1::
                            kNone &&
                server != nullptr,
            "create server before ACTIVE gate test");
        *ok &= Expect(
            producer->BeginDraining(),
            "transition ring to draining");
        int system_error = 0;
        *ok &= Expect(
            server != nullptr &&
                !server->Start(&system_error) &&
                !server->ready(),
            "READY fails after ring leaves ACTIVE");
        server.reset();
        struct stat removed {};
        *ok &= Expect(
            ::lstat(socket_path.c_str(), &removed) != 0 &&
                errno == ENOENT,
            "owned socket removed after failed start");
    }
}

void TestLiveServer(
    const std::filesystem::path& directory,
    bool* ok) {
    auto producer = MakeProducer(ok, 30U);
    if (producer == nullptr) {
        return;
    }
    const auto event = OrderEvent(1U);
    *ok &= Expect(
        producer->PublishSourceTick(
            1U, std::span(&event, 1U)) ==
            ipc::OrderEventDeltaPublishErrorV1::kNone,
        "publish one event before client attach");
    *ok &= Expect(
        producer->UpdateHeartbeat(9'876U),
        "publish event ring heartbeat");

    const std::filesystem::path socket_path =
        directory / "live.sock";
    std::unique_ptr<ipc::OrderEventDeltaControlServerV1> server;
    int system_error = -1;
    const auto create_error =
        ipc::OrderEventDeltaControlServerV1::Create(
            ServerConfig(socket_path, producer.get()),
            &server,
            &system_error);
    if (create_error !=
        ipc::OrderEventDeltaControlServerCreateErrorV1::kNone) {
        std::cerr << "live create error="
                  << ipc::
                         OrderEventDeltaControlServerCreateErrorNameV1(
                             create_error)
                  << " errno=" << system_error << '\n';
    }
    *ok &= Expect(
        create_error ==
                ipc::OrderEventDeltaControlServerCreateErrorV1::
                    kNone &&
            server != nullptr && system_error == 0,
        "create live control server");
    if (server == nullptr) {
        return;
    }
    *ok &= Expect(
        server->Start(&system_error) && server->ready() &&
            system_error == 0,
        "start ACTIVE control server");

    ipc::OrderEventDeltaControlSnapshotV1 snapshot{};
    *ok &= Expect(
        ipc::OrderEventDeltaControlProbeV1(
            ClientConfig(socket_path),
            &snapshot,
            &system_error) ==
                ipc::OrderEventDeltaControlClientErrorV1::kNone &&
            system_error == 0 &&
            snapshot.source_session == SourceSession() &&
            snapshot.event_session == producer->session() &&
            snapshot.event_published_sequence == 1U &&
            snapshot.source_tick_consumed_sequence == 1U &&
            snapshot.heartbeat_monotonic_ns == 9'876U &&
            snapshot.producer_started_monotonic_ns == 1'234U &&
            snapshot.event_producer_state ==
                static_cast<std::uint32_t>(
                    ipc::OrderEventDeltaProducerStateV1::kActive) &&
            snapshot.event_header_flags == 0U,
        "lightweight Probe validates full ACTIVE snapshot");

    int descriptor = -1;
    snapshot = {};
    *ok &= Expect(
        ipc::OrderEventDeltaControlGetSessionV1(
            ClientConfig(socket_path),
            &snapshot,
            &descriptor,
            &system_error) ==
                ipc::OrderEventDeltaControlClientErrorV1::kNone &&
            descriptor >= 0 && system_error == 0 &&
            (fcntl(descriptor, F_GETFL) & O_ACCMODE) == O_RDONLY,
        "GET_SESSION transfers an O_RDONLY descriptor");
    if (descriptor >= 0) {
        static_cast<void>(::close(descriptor));
    }

    std::unique_ptr<ipc::OrderEventDeltaRingReaderV1> reader;
    snapshot = {};
    *ok &= Expect(
        ipc::OrderEventDeltaControlConnectV1(
            ClientConfig(socket_path),
            &snapshot,
            &reader,
            &system_error) ==
                ipc::OrderEventDeltaControlClientErrorV1::kNone &&
            reader != nullptr && system_error == 0 &&
            reader->session() == producer->session(),
        "Connect returns a fully validated ring reader");
    if (reader != nullptr) {
        std::array<ipc::OrderEventDeltaPayloadV1, 2U> rows{};
        ipc::OrderEventDeltaReadResultV1 read{};
        *ok &= Expect(
            reader->Read(1U, rows, &read) ==
                    ipc::OrderEventDeltaReadErrorV1::kNone &&
                read.written == 1U && read.next_sequence == 2U &&
                rows[0U].order_id == 100,
            "connected reader consumes live event");
    }

    auto wrong = ClientConfig(socket_path);
    ++wrong.expected_source_session.session_epoch;
    snapshot = {};
    *ok &= Expect(
        ipc::OrderEventDeltaControlProbeV1(
            wrong, &snapshot, &system_error) ==
                ipc::OrderEventDeltaControlClientErrorV1::
                    kSourceSessionMismatch &&
            snapshot ==
                ipc::OrderEventDeltaControlSnapshotV1{},
        "client and server reject stale source session");

    *ok &= Expect(
        SpawnExecClient(socket_path),
        "exec child receives fd and reads ring cross-process");

    *ok &= Expect(
        producer->BeginDraining(),
        "live ring enters draining");
    snapshot = {};
    *ok &= Expect(
        ipc::OrderEventDeltaControlProbeV1(
            ClientConfig(socket_path),
            &snapshot,
            &system_error) ==
            ipc::OrderEventDeltaControlClientErrorV1::kUnavailable,
        "GET_SESSION transfers no READY fd outside ACTIVE");

    server->Stop();
    *ok &= Expect(!server->ready(), "server stops serially");
    struct stat removed {};
    *ok &= Expect(
        ::lstat(socket_path.c_str(), &removed) != 0 &&
            errno == ENOENT,
        "Stop removes only the server-owned socket");
}

void TestBoundedClientTimeout(
    const std::filesystem::path& directory,
    bool* ok) {
    const std::filesystem::path socket_path =
        directory / "stall.sock";
    int listener =
        ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    *ok &= Expect(listener >= 0, "create stalling listener");
    if (listener < 0) {
        return;
    }
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    const std::string path = socket_path.string();
    std::memcpy(
        address.sun_path, path.c_str(), path.size() + 1U);
    *ok &= Expect(
        ::bind(
            listener,
            reinterpret_cast<const sockaddr*>(&address),
            static_cast<socklen_t>(
                offsetof(sockaddr_un, sun_path) +
                path.size() + 1U)) == 0 &&
            ::chmod(path.c_str(), 0600) == 0 &&
            ::listen(listener, 1) == 0,
        "bind stalling same-UID listener");

    std::thread stalled([listener] {
        const int client =
            ::accept(listener, nullptr, nullptr);
        if (client >= 0) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(150));
            static_cast<void>(::close(client));
        }
    });
    auto config = ClientConfig(socket_path);
    config.timeout = std::chrono::milliseconds(30);
    ipc::OrderEventDeltaControlSnapshotV1 snapshot{};
    int system_error = 0;
    const auto started = std::chrono::steady_clock::now();
    const auto error = ipc::OrderEventDeltaControlProbeV1(
        config, &snapshot, &system_error);
    const auto elapsed = std::chrono::steady_clock::now() - started;
    *ok &= Expect(
        error ==
                ipc::OrderEventDeltaControlClientErrorV1::kTimeout &&
            system_error == ETIMEDOUT &&
            elapsed < std::chrono::seconds(1),
        "client timeout bounds the complete handshake");
    stalled.join();
    static_cast<void>(::close(listener));
    static_cast<void>(::unlink(socket_path.c_str()));
}

}  // namespace

int main(int argc, char** argv) {
    if (argc == 3 && std::string_view(argv[1]) == "--child") {
        return ChildClient(argv[2]);
    }

    bool ok = true;
    TestWireAbi(&ok);
    const std::filesystem::path directory =
        MakePrivateDirectory(&ok);
    if (!directory.empty()) {
        TestPathAndStartValidation(directory, &ok);
        TestLiveServer(directory, &ok);
        TestBoundedClientTimeout(directory, &ok);
        CleanupDirectory(directory);
    }
    if (!ok) {
        return 1;
    }
    std::cout
        << "order event delta control V1 tests passed\n";
    return 0;
}
