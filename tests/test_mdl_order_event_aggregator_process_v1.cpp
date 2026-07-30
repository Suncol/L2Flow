#include "l2flow/ipc/order_event_delta_control_v1.h"
#include "l2flow/ipc/realtime_shared_service_v2.h"
#include "l2flow/market/observed_instrument_directory_v2.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <poll.h>
#include <spawn.h>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

namespace {

namespace common = l2flow::common;
namespace ipc = l2flow::ipc;
namespace market = l2flow::market;

constexpr std::uint64_t kEpoch = 42U;
constexpr std::uint32_t kTradeDate = 20260730U;

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

common::Identity128 RunId(std::uint8_t seed = 0x42U) {
    common::Identity128 result{};
    result[0U] = static_cast<std::byte>(seed);
    result[15U] = std::byte{0xa5U};
    return result;
}

class ScopedTempDirectory final {
public:
    ScopedTempDirectory() {
        char pattern[] = "/tmp/l2flow-event-app-XXXXXX";
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

class ChildProcess final {
public:
    ChildProcess() noexcept = default;
    ChildProcess(const ChildProcess&) = delete;
    ChildProcess& operator=(const ChildProcess&) = delete;
    ~ChildProcess() {
        if (pid_ > 0) {
            static_cast<void>(::kill(pid_, SIGKILL));
            int status = 0;
            while (::waitpid(pid_, &status, 0) < 0 &&
                   errno == EINTR) {
            }
        }
        if (output_fd_ >= 0) {
            static_cast<void>(::close(output_fd_));
        }
    }

    [[nodiscard]] bool Spawn(
        const std::filesystem::path& executable,
        const std::filesystem::path& source_socket,
        const std::filesystem::path& event_socket) {
        std::vector<std::string> arguments{
            executable.string(),
            "--source-socket",
            source_socket.string(),
            "--event-socket",
            event_socket.string(),
            "--session-epoch",
            std::to_string(kEpoch),
            "--trade-date",
            std::to_string(kTradeDate),
            "--shanghai-state-capacity",
            "16",
            "--shenzhen-state-capacity",
            "16",
            "--event-ring-capacity",
            "16",
            "--event-maximum-mapping-bytes",
            "1048576",
            "--read-batch-records",
            "4",
            "--poll-ms",
            "1",
            "--timeout-ms",
            "1000",
        };
        std::vector<char*> child_argv;
        child_argv.reserve(arguments.size() + 1U);
        for (std::string& argument : arguments) {
            child_argv.push_back(argument.data());
        }
        child_argv.push_back(nullptr);

        int pipe_descriptors[2]{-1, -1};
        if (::pipe2(pipe_descriptors, O_CLOEXEC) != 0) {
            return false;
        }
        posix_spawn_file_actions_t actions{};
        if (::posix_spawn_file_actions_init(&actions) != 0) {
            static_cast<void>(::close(pipe_descriptors[0U]));
            static_cast<void>(::close(pipe_descriptors[1U]));
            return false;
        }
        const bool actions_ok =
            ::posix_spawn_file_actions_adddup2(
                &actions, pipe_descriptors[1U], STDOUT_FILENO) ==
                0 &&
            ::posix_spawn_file_actions_adddup2(
                &actions, pipe_descriptors[1U], STDERR_FILENO) ==
                0 &&
            ::posix_spawn_file_actions_addclose(
                &actions, pipe_descriptors[0U]) == 0 &&
            ::posix_spawn_file_actions_addclose(
                &actions, pipe_descriptors[1U]) == 0;
        pid_t child = -1;
        const int spawn_error =
            actions_ok
                ? ::posix_spawn(
                      &child,
                      child_argv[0U],
                      &actions,
                      nullptr,
                      child_argv.data(),
                      environ)
                : EINVAL;
        static_cast<void>(
            ::posix_spawn_file_actions_destroy(&actions));
        static_cast<void>(::close(pipe_descriptors[1U]));
        if (spawn_error != 0 || child <= 0) {
            static_cast<void>(::close(pipe_descriptors[0U]));
            return false;
        }
        pid_ = child;
        output_fd_ = pipe_descriptors[0U];
        return true;
    }

    [[nodiscard]] bool ReadReady(
        std::chrono::milliseconds timeout,
        std::string* transcript) {
        const auto deadline =
            std::chrono::steady_clock::now() + timeout;
        std::string buffer;
        for (;;) {
            const std::size_t newline = buffer.find('\n');
            if (newline != std::string::npos) {
                const std::string line = buffer.substr(0U, newline);
                buffer.erase(0U, newline + 1U);
                if (transcript != nullptr) {
                    transcript->append(line);
                    transcript->push_back('\n');
                }
                if (line.starts_with("READY ")) {
                    return true;
                }
            }
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                return false;
            }
            const auto remaining =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    deadline - now);
            pollfd descriptor{};
            descriptor.fd = output_fd_;
            descriptor.events = POLLIN;
            const int waited = ::poll(
                &descriptor,
                1U,
                static_cast<int>(std::max<std::int64_t>(
                    1,
                    std::min<std::int64_t>(
                        remaining.count(), 1000))));
            if (waited < 0 && errno == EINTR) {
                continue;
            }
            if (waited <= 0 ||
                (descriptor.revents & POLLIN) == 0) {
                return false;
            }
            char bytes[1024];
            const ssize_t read_bytes =
                ::read(output_fd_, bytes, sizeof(bytes));
            if (read_bytes <= 0) {
                return false;
            }
            buffer.append(
                bytes, static_cast<std::size_t>(read_bytes));
        }
    }

    [[nodiscard]] bool Wait(
        std::chrono::milliseconds timeout,
        int* exit_code) {
        const auto deadline =
            std::chrono::steady_clock::now() + timeout;
        for (;;) {
            int status = 0;
            const pid_t result = ::waitpid(pid_, &status, WNOHANG);
            if (result == pid_) {
                pid_ = -1;
                if (exit_code != nullptr) {
                    *exit_code =
                        WIFEXITED(status) ? WEXITSTATUS(status) : -1;
                }
                return WIFEXITED(status);
            }
            if (result < 0 && errno != EINTR) {
                return false;
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                return false;
            }
            std::this_thread::sleep_for(
                std::chrono::milliseconds(1));
        }
    }

    [[nodiscard]] bool Signal(int signal_number) const noexcept {
        return pid_ > 0 && ::kill(pid_, signal_number) == 0;
    }

private:
    pid_t pid_ = -1;
    int output_fd_ = -1;
};

std::unique_ptr<market::ObservedInstrumentDirectoryV2>
MakeDirectory() {
    market::ObservedInstrumentDirectoryConfigV2 config{};
    config.capacity = 1U;
    config.session_epoch = kEpoch;
    std::unique_ptr<market::ObservedInstrumentDirectoryV2> result;
    return market::ObservedInstrumentDirectoryV2::Create(
               config, &result) ==
               market::ObservedInstrumentDirectoryErrorV2::kNone
           ? std::move(result)
           : nullptr;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: process-test <aggregator-executable>\n";
        return 2;
    }
    bool ok = true;
    ScopedTempDirectory temporary;
    auto directory = MakeDirectory();
    if (!Expect(
            temporary.valid() && directory != nullptr,
            "create process fixture")) {
        return 1;
    }
    const std::filesystem::path source_socket =
        temporary.path() / "source.sock";
    const std::filesystem::path event_socket =
        temporary.path() / "events.sock";

    ipc::RealtimeSharedServiceConfigV2 config{};
    config.run_id = RunId();
    config.session_epoch = kEpoch;
    config.trade_date = kTradeDate;
    config.directory = directory.get();
    config.tick_ring_capacity = 8U;
    config.key_arena_bytes = 64U;
    config.maximum_mapping_bytes = 8U * 1024U * 1024U;
    config.control_socket_path = source_socket;
    std::shared_ptr<ipc::RealtimeSharedMarketServiceV2> service;
    int system_error = 0;
    const auto create_error =
        ipc::RealtimeSharedMarketServiceV2::Create(
            config, &service, &system_error);
    const bool started =
        create_error ==
                ipc::RealtimeSharedServiceCreateErrorV2::kNone &&
            service != nullptr &&
            service->Start(&system_error);
    ok &= Expect(
        started,
        "start empty Wire V2 source");
    if (!ok) {
        std::cerr
            << "create_error="
            << ipc::RealtimeSharedServiceCreateErrorNameV2(
                   create_error)
            << " system_error=" << system_error << '\n';
        return 1;
    }

    ChildProcess child;
    std::string transcript;
    ok &= Expect(
        child.Spawn(argv[1], source_socket, event_socket) &&
            child.ReadReady(std::chrono::seconds(5), &transcript),
        "standalone aggregator reaches READY after source cut zero");
    if (!ok) {
        std::cerr << transcript;
        service->MarkFailed();
        service->StopControl();
        return 1;
    }

    ipc::OrderEventDeltaControlClientConfigV1 client_config{};
    client_config.control_socket_path = event_socket;
    client_config.expected_source_session.run_id = RunId();
    client_config.expected_source_session.session_epoch = kEpoch;
    client_config.expected_source_session.trade_date = kTradeDate;
    client_config.timeout = std::chrono::seconds(1);
    ipc::OrderEventDeltaControlSnapshotV1 snapshot{};
    std::unique_ptr<ipc::OrderEventDeltaRingReaderV1> event_reader;
    ok &= Expect(
        ipc::OrderEventDeltaControlConnectV1(
            client_config, &snapshot, &event_reader) ==
                ipc::OrderEventDeltaControlClientErrorV1::kNone &&
            event_reader != nullptr &&
            snapshot.source_tick_consumed_sequence == 0U &&
            snapshot.event_published_sequence == 0U,
        "READY event control transfers the validated read-only ring");

    service->MarkDraining();
    ok &= Expect(
        service->MarkStoppedClean(0U),
        "empty source reaches exact STOPPED_CLEAN watermark");
    int exit_code = -1;
    ok &= Expect(
        child.Wait(std::chrono::seconds(5), &exit_code) &&
            exit_code == 0 &&
            event_reader != nullptr &&
            event_reader->state() ==
                ipc::OrderEventDeltaProducerStateV1::kStoppedClean,
        "source clean stop drains aggregator and clean-stops event ring");
    service->StopControl();

    auto second_directory = MakeDirectory();
    const std::filesystem::path second_source_socket =
        temporary.path() / "source-signal.sock";
    const std::filesystem::path second_event_socket =
        temporary.path() / "events-signal.sock";
    ipc::RealtimeSharedServiceConfigV2 second_config = config;
    second_config.run_id = RunId(0x43U);
    second_config.directory = second_directory.get();
    second_config.control_socket_path = second_source_socket;
    std::shared_ptr<ipc::RealtimeSharedMarketServiceV2>
        second_service;
    system_error = 0;
    const auto second_create_error =
        ipc::RealtimeSharedMarketServiceV2::Create(
            second_config, &second_service, &system_error);
    ok &= Expect(
        second_directory != nullptr &&
            second_create_error ==
                ipc::RealtimeSharedServiceCreateErrorV2::kNone &&
            second_service != nullptr &&
            second_service->Start(&system_error),
        "start second active source for signal test");
    if (!ok) {
        if (second_service != nullptr) {
            second_service->MarkFailed();
            second_service->StopControl();
        }
        return 1;
    }

    ChildProcess interrupted_child;
    transcript.clear();
    ok &= Expect(
        interrupted_child.Spawn(
            argv[1], second_source_socket, second_event_socket) &&
            interrupted_child.ReadReady(
                std::chrono::seconds(5), &transcript),
        "second aggregator reaches READY against active source");
    ipc::OrderEventDeltaControlClientConfigV1
        interrupted_client_config{};
    interrupted_client_config.control_socket_path =
        second_event_socket;
    interrupted_client_config.expected_source_session.run_id =
        RunId(0x43U);
    interrupted_client_config.expected_source_session.session_epoch =
        kEpoch;
    interrupted_client_config.expected_source_session.trade_date =
        kTradeDate;
    interrupted_client_config.timeout = std::chrono::seconds(1);
    ipc::OrderEventDeltaControlSnapshotV1 interrupted_snapshot{};
    std::unique_ptr<ipc::OrderEventDeltaRingReaderV1>
        interrupted_reader;
    ok &= Expect(
        ipc::OrderEventDeltaControlConnectV1(
            interrupted_client_config,
            &interrupted_snapshot,
            &interrupted_reader) ==
                ipc::OrderEventDeltaControlClientErrorV1::kNone &&
            interrupted_reader != nullptr,
        "attach event reader before interrupting active source");
    int interrupted_exit_code = 0;
    ok &= Expect(
        interrupted_child.Signal(SIGTERM) &&
            interrupted_child.Wait(
                std::chrono::seconds(5),
                &interrupted_exit_code) &&
            interrupted_exit_code != 0 &&
            interrupted_reader != nullptr &&
            interrupted_reader->state() ==
                ipc::OrderEventDeltaProducerStateV1::kFailed,
        "signal while source ACTIVE fail-closes event ring");
    second_service->MarkFailed();
    second_service->StopControl();
    return ok ? 0 : 1;
}
