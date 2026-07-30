#include "mdl_order_event_aggregator_cli_v1.h"

#include "l2flow/ipc/order_event_delta_control_v1.h"
#include "l2flow/ipc/order_event_delta_ring_v1.h"
#include "l2flow/ipc/order_event_live_aggregation_engine_v1.h"
#include "l2flow/ipc/realtime_shm_reader_c_v2.h"
#include "l2flow/ipc/realtime_wire_v2.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <poll.h>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/random.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

namespace {

namespace app = l2flow::apps;
namespace ipc = l2flow::ipc;

volatile std::sig_atomic_t g_stop_requested = 0;

extern "C" void StopSignalHandler(int) noexcept {
    g_stop_requested = 1;
}

class UniqueFd final {
public:
    UniqueFd() noexcept = default;
    explicit UniqueFd(int descriptor) noexcept
        : descriptor_(descriptor) {}
    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;
    UniqueFd(UniqueFd&& other) noexcept
        : descriptor_(std::exchange(other.descriptor_, -1)) {}
    UniqueFd& operator=(UniqueFd&& other) noexcept {
        if (this != &other) {
            Reset(std::exchange(other.descriptor_, -1));
        }
        return *this;
    }
    ~UniqueFd() {
        Reset();
    }

    [[nodiscard]] int get() const noexcept {
        return descriptor_;
    }
    [[nodiscard]] int Release() noexcept {
        return std::exchange(descriptor_, -1);
    }
    void Reset(int descriptor = -1) noexcept {
        if (descriptor_ >= 0) {
            static_cast<void>(::close(descriptor_));
        }
        descriptor_ = descriptor;
    }

private:
    int descriptor_ = -1;
};

struct ShmReaderCloser final {
    void operator()(l2flow_shm_reader_v2* reader) const noexcept {
        l2flow_shm_reader_close_v2(reader);
    }
};
using ShmReaderPtr =
    std::unique_ptr<l2flow_shm_reader_v2, ShmReaderCloser>;

class EventFailureGuard final {
public:
    explicit EventFailureGuard(
        ipc::OrderEventDeltaRingProducerV1* producer) noexcept
        : producer_(producer) {}
    EventFailureGuard(const EventFailureGuard&) = delete;
    EventFailureGuard& operator=(const EventFailureGuard&) = delete;
    ~EventFailureGuard() {
        if (armed_ && producer_ != nullptr) {
            producer_->MarkFailed();
        }
    }
    void Release() noexcept {
        armed_ = false;
    }

private:
    ipc::OrderEventDeltaRingProducerV1* producer_ = nullptr;
    bool armed_ = true;
};

enum class SourceAttachError : std::uint8_t {
    kNone = 0U,
    kInterrupted,
    kSocket,
    kPeerCredentials,
    kTimeout,
    kTransport,
    kProtocol,
    kUnavailable,
    kDescriptor,
    kReader,
    kIdentity,
};

[[nodiscard]] std::string_view SourceAttachErrorName(
    SourceAttachError error) noexcept {
    switch (error) {
        case SourceAttachError::kNone:
            return "none";
        case SourceAttachError::kInterrupted:
            return "interrupted";
        case SourceAttachError::kSocket:
            return "socket";
        case SourceAttachError::kPeerCredentials:
            return "peer_credentials";
        case SourceAttachError::kTimeout:
            return "timeout";
        case SourceAttachError::kTransport:
            return "transport";
        case SourceAttachError::kProtocol:
            return "protocol";
        case SourceAttachError::kUnavailable:
            return "unavailable";
        case SourceAttachError::kDescriptor:
            return "descriptor";
        case SourceAttachError::kReader:
            return "reader";
        case SourceAttachError::kIdentity:
            return "identity";
    }
    return "unknown";
}

template <typename Value, std::size_t Size>
[[nodiscard]] bool AllZero(
    const std::array<Value, Size>& values) noexcept {
    return std::all_of(
        values.begin(), values.end(), [](Value value) noexcept {
            return value == Value{};
        });
}

[[nodiscard]] bool AnyNonzero(
    const std::uint8_t* values,
    std::size_t size) noexcept {
    return values != nullptr &&
           std::any_of(
               values,
               values + size,
               [](std::uint8_t value) noexcept {
                   return value != 0U;
               });
}

[[nodiscard]] std::uint64_t MonotonicNanoseconds() noexcept {
    timespec value{};
    if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0 ||
        value.tv_sec < 0 || value.tv_nsec < 0) {
        return 0U;
    }
    const auto seconds =
        static_cast<std::uint64_t>(value.tv_sec);
    const auto nanoseconds =
        static_cast<std::uint64_t>(value.tv_nsec);
    if (seconds >
        (std::numeric_limits<std::uint64_t>::max() -
         nanoseconds) /
            1'000'000'000ULL) {
        return 0U;
    }
    return seconds * 1'000'000'000ULL + nanoseconds;
}

[[nodiscard]] bool InstallSignalHandlers() noexcept {
    struct sigaction action {};
    action.sa_handler = StopSignalHandler;
    if (::sigemptyset(&action.sa_mask) != 0) {
        return false;
    }
    action.sa_flags = 0;
    return ::sigaction(SIGINT, &action, nullptr) == 0 &&
           ::sigaction(SIGTERM, &action, nullptr) == 0;
}

using IoDeadline = std::chrono::steady_clock::time_point;

[[nodiscard]] bool WaitSocket(
    int descriptor,
    short events,
    IoDeadline deadline,
    int* system_error) noexcept {
    for (;;) {
        if (g_stop_requested != 0) {
            if (system_error != nullptr) {
                *system_error = ECANCELED;
            }
            return false;
        }
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            if (system_error != nullptr) {
                *system_error = ETIMEDOUT;
            }
            return false;
        }
        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - now);
        const auto rounded =
            remaining +
            (remaining < deadline - now
                 ? std::chrono::milliseconds(1)
                 : std::chrono::milliseconds(0));
        const auto bounded = std::min<std::int64_t>(
            rounded.count(),
            std::numeric_limits<int>::max());
        pollfd item{};
        item.fd = descriptor;
        item.events = events;
        const int result =
            ::poll(&item, 1U, static_cast<int>(bounded));
        if (result > 0) {
            if ((item.revents & events) != 0) {
                return true;
            }
            if ((item.revents & (POLLERR | POLLHUP | POLLNVAL)) !=
                0) {
                if (system_error != nullptr) {
                    *system_error = EIO;
                }
                return false;
            }
            continue;
        }
        if (result == 0) {
            if (system_error != nullptr) {
                *system_error = ETIMEDOUT;
            }
            return false;
        }
        if (errno != EINTR) {
            if (system_error != nullptr) {
                *system_error = errno;
            }
            return false;
        }
    }
}

[[nodiscard]] SourceAttachError ConnectSourceControl(
    const std::filesystem::path& path,
    IoDeadline deadline,
    UniqueFd* output,
    int* system_error) {
    if (output == nullptr) {
        return SourceAttachError::kSocket;
    }
    output->Reset();
    const std::string path_string = path.string();
    if (path_string.empty() ||
        path_string.size() >= sizeof(sockaddr_un::sun_path)) {
        return SourceAttachError::kSocket;
    }
    UniqueFd socket(::socket(
        AF_UNIX,
        SOCK_SEQPACKET | SOCK_CLOEXEC | SOCK_NONBLOCK,
        0));
    if (socket.get() < 0) {
        if (system_error != nullptr) {
            *system_error = errno;
        }
        return SourceAttachError::kSocket;
    }
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::memcpy(
        address.sun_path,
        path_string.c_str(),
        path_string.size() + 1U);
    const socklen_t address_bytes = static_cast<socklen_t>(
        offsetof(sockaddr_un, sun_path) +
        path_string.size() + 1U);
    int result = -1;
    do {
        result = ::connect(
            socket.get(),
            reinterpret_cast<const sockaddr*>(&address),
            address_bytes);
    } while (result != 0 && errno == EINTR &&
             g_stop_requested == 0);
    if (result != 0 &&
        errno != EINPROGRESS && errno != EAGAIN) {
        if (system_error != nullptr) {
            *system_error = errno;
        }
        return g_stop_requested != 0
                   ? SourceAttachError::kInterrupted
                   : SourceAttachError::kSocket;
    }
    if (result != 0) {
        if (!WaitSocket(
                socket.get(), POLLOUT, deadline, system_error)) {
            return g_stop_requested != 0
                       ? SourceAttachError::kInterrupted
                       : (system_error != nullptr &&
                                  *system_error == ETIMEDOUT
                              ? SourceAttachError::kTimeout
                              : SourceAttachError::kSocket);
        }
        int socket_error = 0;
        socklen_t error_bytes =
            static_cast<socklen_t>(sizeof(socket_error));
        if (::getsockopt(
                socket.get(),
                SOL_SOCKET,
                SO_ERROR,
                &socket_error,
                &error_bytes) != 0 ||
            error_bytes != sizeof(socket_error) ||
            socket_error != 0) {
            if (system_error != nullptr) {
                *system_error =
                    socket_error != 0 ? socket_error : errno;
            }
            return SourceAttachError::kSocket;
        }
    }

    ucred credentials{};
    socklen_t credential_bytes =
        static_cast<socklen_t>(sizeof(credentials));
    if (::getsockopt(
            socket.get(),
            SOL_SOCKET,
            SO_PEERCRED,
            &credentials,
            &credential_bytes) != 0 ||
        credential_bytes != sizeof(credentials) ||
        credentials.uid != ::geteuid()) {
        if (system_error != nullptr) {
            *system_error = errno != 0 ? errno : EACCES;
        }
        return SourceAttachError::kPeerCredentials;
    }
    *output = std::move(socket);
    return SourceAttachError::kNone;
}

[[nodiscard]] SourceAttachError SendSourceRequest(
    int descriptor,
    const ipc::RealtimeControlRequestV2& request,
    IoDeadline deadline,
    int* system_error) noexcept {
    for (;;) {
        const ssize_t sent = ::send(
            descriptor,
            &request,
            sizeof(request),
            MSG_NOSIGNAL);
        if (sent == static_cast<ssize_t>(sizeof(request))) {
            return SourceAttachError::kNone;
        }
        if (sent >= 0) {
            if (system_error != nullptr) {
                *system_error = EMSGSIZE;
            }
            return SourceAttachError::kTransport;
        }
        if (errno == EINTR) {
            if (g_stop_requested != 0) {
                return SourceAttachError::kInterrupted;
            }
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            if (system_error != nullptr) {
                *system_error = errno;
            }
            return SourceAttachError::kTransport;
        }
        if (!WaitSocket(
                descriptor, POLLOUT, deadline, system_error)) {
            return g_stop_requested != 0
                       ? SourceAttachError::kInterrupted
                       : (system_error != nullptr &&
                                  *system_error == ETIMEDOUT
                              ? SourceAttachError::kTimeout
                              : SourceAttachError::kTransport);
        }
    }
}

struct SourceResponse final {
    ipc::RealtimeControlResponseV2 response{};
    UniqueFd descriptor;
    std::size_t descriptor_count = 0U;
};

[[nodiscard]] SourceAttachError ReceiveSourceResponse(
    int descriptor,
    IoDeadline deadline,
    SourceResponse* output,
    int* system_error) noexcept {
    if (output == nullptr) {
        return SourceAttachError::kProtocol;
    }
    *output = {};
    for (;;) {
        std::array<std::byte, CMSG_SPACE(sizeof(int) * 4U)>
            control{};
        iovec vector{};
        vector.iov_base = &output->response;
        vector.iov_len = sizeof(output->response);
        msghdr message{};
        message.msg_iov = &vector;
        message.msg_iovlen = 1U;
        message.msg_control = control.data();
        message.msg_controllen = control.size();
        const ssize_t received =
            ::recvmsg(
                descriptor,
                &message,
#ifdef MSG_CMSG_CLOEXEC
                MSG_CMSG_CLOEXEC
#else
                0
#endif
            );
        if (received < 0 &&
            (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (!WaitSocket(
                    descriptor, POLLIN, deadline, system_error)) {
                return g_stop_requested != 0
                           ? SourceAttachError::kInterrupted
                           : (system_error != nullptr &&
                                      *system_error == ETIMEDOUT
                                  ? SourceAttachError::kTimeout
                                  : SourceAttachError::kTransport);
            }
            continue;
        }
        if (received < 0 && errno == EINTR) {
            if (g_stop_requested != 0) {
                return SourceAttachError::kInterrupted;
            }
            continue;
        }
        if (received < 0) {
            if (system_error != nullptr) {
                *system_error = errno;
            }
            return SourceAttachError::kTransport;
        }

        bool ancillary_valid = true;
        for (cmsghdr* header = CMSG_FIRSTHDR(&message);
             header != nullptr;
             header = CMSG_NXTHDR(&message, header)) {
            if (header->cmsg_level != SOL_SOCKET ||
                header->cmsg_type != SCM_RIGHTS ||
                header->cmsg_len < CMSG_LEN(sizeof(int))) {
                ancillary_valid = false;
                continue;
            }
            const std::size_t payload_bytes =
                header->cmsg_len - CMSG_LEN(0U);
            if (payload_bytes % sizeof(int) != 0U) {
                ancillary_valid = false;
                continue;
            }
            const std::size_t count =
                payload_bytes / sizeof(int);
            const auto* bytes =
                reinterpret_cast<const std::byte*>(
                    CMSG_DATA(header));
            for (std::size_t index = 0U; index < count;
                 ++index) {
                int received_descriptor = -1;
                std::memcpy(
                    &received_descriptor,
                    bytes + index * sizeof(int),
                    sizeof(received_descriptor));
                ++output->descriptor_count;
                if (output->descriptor.get() < 0) {
                    output->descriptor.Reset(
                        received_descriptor);
                } else if (received_descriptor >= 0) {
                    static_cast<void>(
                        ::close(received_descriptor));
                }
            }
        }
        if (received !=
                static_cast<ssize_t>(sizeof(output->response)) ||
            (message.msg_flags & (MSG_TRUNC | MSG_CTRUNC)) != 0 ||
            !ancillary_valid ||
            output->descriptor_count > 1U) {
            output->descriptor.Reset();
            return SourceAttachError::kProtocol;
        }
#ifndef MSG_CMSG_CLOEXEC
        if (output->descriptor.get() >= 0) {
            const int flags =
                ::fcntl(output->descriptor.get(), F_GETFD);
            if (flags < 0 ||
                ::fcntl(
                    output->descriptor.get(),
                    F_SETFD,
                    flags | FD_CLOEXEC) != 0) {
                if (system_error != nullptr) {
                    *system_error = errno;
                }
                output->descriptor.Reset();
                return SourceAttachError::kDescriptor;
            }
        }
#endif
        return SourceAttachError::kNone;
    }
}

[[nodiscard]] bool SourceResponseEnvelopeValid(
    const ipc::RealtimeControlResponseV2& response,
    std::uint64_t request_id) noexcept {
    return response.magic == ipc::kRealtimeControlMagicV2 &&
           response.protocol_major == ipc::kRealtimeWireMajorV2 &&
           response.protocol_minor == ipc::kRealtimeWireMinorV2 &&
           response.flags == 0U &&
           response.message_bytes == sizeof(response) &&
           response.reserved0 == 0U &&
           response.request_id == request_id &&
           AllZero(response.reserved);
}

[[nodiscard]] SourceAttachError AttachSource(
    const app::OrderEventAggregatorOptionsV1& options,
    ShmReaderPtr* output_reader,
    l2flow_shm_session_info_v2* output_session,
    int* system_error,
    int* reader_error) {
    if (output_reader == nullptr || output_session == nullptr) {
        return SourceAttachError::kReader;
    }
    output_reader->reset();
    *output_session = {};
    if (system_error != nullptr) {
        *system_error = 0;
    }
    if (reader_error != nullptr) {
        *reader_error = L2FLOW_SHM_READER_OK_V2;
    }
    const IoDeadline deadline =
        std::chrono::steady_clock::now() +
        std::chrono::milliseconds(options.control_timeout_ms);
    UniqueFd channel;
    SourceAttachError error = ConnectSourceControl(
        options.source_control_socket,
        deadline,
        &channel,
        system_error);
    if (error != SourceAttachError::kNone) {
        return error;
    }

    ipc::RealtimeControlRequestV2 request{};
    request.magic = ipc::kRealtimeControlMagicV2;
    request.protocol_major = ipc::kRealtimeWireMajorV2;
    request.protocol_minor = ipc::kRealtimeWireMinorV2;
    request.opcode = static_cast<std::uint16_t>(
        ipc::RealtimeControlOpcodeV2::kGetSession);
    request.message_bytes =
        static_cast<std::uint32_t>(sizeof(request));
    request.request_id =
        MonotonicNanoseconds() ^
        (static_cast<std::uint64_t>(::getpid()) << 32U);
    if (request.request_id == 0U) {
        request.request_id = 1U;
    }
    error = SendSourceRequest(
        channel.get(), request, deadline, system_error);
    if (error != SourceAttachError::kNone) {
        return error;
    }
    SourceResponse response{};
    error = ReceiveSourceResponse(
        channel.get(), deadline, &response, system_error);
    if (error != SourceAttachError::kNone) {
        return error;
    }
    channel.Reset();
    if (!SourceResponseEnvelopeValid(
            response.response, request.request_id)) {
        return SourceAttachError::kProtocol;
    }
    const auto status =
        static_cast<ipc::RealtimeControlStatusV2>(
            response.response.status);
    if (status != ipc::RealtimeControlStatusV2::kOk) {
        if (response.descriptor_count != 0U) {
            return SourceAttachError::kProtocol;
        }
        return status ==
                       ipc::RealtimeControlStatusV2::kUnavailable
                   ? SourceAttachError::kUnavailable
                   : SourceAttachError::kProtocol;
    }
    if (response.descriptor_count != 1U ||
        response.descriptor.get() < 0 ||
        response.response.session_epoch !=
            options.session_epoch ||
        response.response.total_mapping_bytes <
            sizeof(ipc::RealtimeWireHeaderV2)) {
        return response.response.session_epoch !=
                       options.session_epoch
                   ? SourceAttachError::kIdentity
                   : SourceAttachError::kProtocol;
    }
    const int access =
        ::fcntl(response.descriptor.get(), F_GETFL);
    struct stat descriptor_status {};
    if (access < 0 ||
        (access & O_ACCMODE) != O_RDONLY ||
        ::fstat(
            response.descriptor.get(),
            &descriptor_status) != 0 ||
        descriptor_status.st_size < 0 ||
        static_cast<std::uint64_t>(
            descriptor_status.st_size) !=
            response.response.total_mapping_bytes) {
        if (system_error != nullptr) {
            *system_error = errno;
        }
        return SourceAttachError::kDescriptor;
    }

    l2flow_shm_reader_v2* raw_reader = nullptr;
    const int open_error = l2flow_shm_reader_open_fd_v2(
        response.descriptor.get(), &raw_reader);
    response.descriptor.Reset();
    if (reader_error != nullptr) {
        *reader_error = open_error;
    }
    if (open_error != L2FLOW_SHM_READER_OK_V2 ||
        raw_reader == nullptr) {
        return SourceAttachError::kReader;
    }
    ShmReaderPtr reader(raw_reader);
    l2flow_shm_session_info_v2 session{};
    const int session_error =
        l2flow_shm_reader_session_v2(reader.get(), &session);
    if (reader_error != nullptr) {
        *reader_error = session_error;
    }
    if (session_error != L2FLOW_SHM_READER_OK_V2) {
        return SourceAttachError::kReader;
    }
    if (!AnyNonzero(session.run_id, sizeof(session.run_id)) ||
        !AnyNonzero(
            session.catalog_digest,
            sizeof(session.catalog_digest)) ||
        session.session_epoch != options.session_epoch ||
        session.trade_date != options.trade_date ||
        session.catalog_generation != 1U ||
        session.catalog_trade_date != options.trade_date ||
        session.catalog_version == 0U ||
        session.capacity == 0U ||
        session.tick_ring_capacity == 0U ||
        session.coverage_complete != 1U ||
        session.bound_count != session.capacity ||
        session.reserved_catalog != 0U ||
        session.catalog_scope !=
            static_cast<std::uint32_t>(
                ipc::RealtimeCatalogScopeV2::
                    kDeclaredDailyAShare)) {
        return SourceAttachError::kIdentity;
    }
    *output_session = session;
    *output_reader = std::move(reader);
    return SourceAttachError::kNone;
}

[[nodiscard]] bool SameSourceIdentity(
    const l2flow_shm_session_info_v2& left,
    const l2flow_shm_session_info_v2& right) noexcept {
    return std::memcmp(
               left.run_id,
               right.run_id,
               sizeof(left.run_id)) == 0 &&
           left.session_epoch == right.session_epoch &&
           left.trade_date == right.trade_date &&
           left.catalog_generation ==
               right.catalog_generation &&
           left.catalog_trade_date == right.catalog_trade_date &&
           left.catalog_version == right.catalog_version &&
           std::memcmp(
               left.catalog_digest,
               right.catalog_digest,
               sizeof(left.catalog_digest)) == 0 &&
           left.tick_ring_capacity == right.tick_ring_capacity &&
           left.capacity == right.capacity &&
           left.bound_count == right.bound_count &&
           left.catalog_scope == right.catalog_scope &&
           left.coverage_complete == right.coverage_complete;
}

[[nodiscard]] bool SourceStateReadable(
    std::uint32_t state) noexcept {
    return state ==
               static_cast<std::uint32_t>(
                   ipc::RealtimeServerStateV2::kActive) ||
           state ==
               static_cast<std::uint32_t>(
                   ipc::RealtimeServerStateV2::kDraining) ||
           state ==
               static_cast<std::uint32_t>(
                   ipc::RealtimeServerStateV2::kStoppedClean);
}

[[nodiscard]] bool SourceHealthUsable(
    const l2flow_shm_health_v2& health,
    std::uint64_t expected_epoch) noexcept {
    return health.session_epoch == expected_epoch &&
           (health.flags &
            ipc::kRealtimeHeaderCoverageLostV2) == 0U &&
           SourceStateReadable(health.server_state);
}

[[nodiscard]] bool GenerateRunId(
    l2flow::common::Identity128* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    output->fill(std::byte{0});
    auto* bytes = reinterpret_cast<std::byte*>(output->data());
    std::size_t written = 0U;
    while (written < output->size()) {
        const ssize_t result = ::getrandom(
            bytes + written, output->size() - written, 0U);
        if (result > 0) {
            written += static_cast<std::size_t>(result);
            continue;
        }
        if (result < 0 && errno == EINTR) {
            if (g_stop_requested != 0) {
                return false;
            }
            continue;
        }
        return false;
    }
    return std::any_of(
        output->begin(),
        output->end(),
        [](std::byte value) noexcept {
            return value != std::byte{0};
        });
}

[[nodiscard]] ipc::OrderEventDeltaSourceSessionV1
SourceSession(
    const l2flow_shm_session_info_v2& source) noexcept {
    ipc::OrderEventDeltaSourceSessionV1 result{};
    for (std::size_t index = 0U; index < result.run_id.size();
         ++index) {
        result.run_id[index] =
            static_cast<std::byte>(source.run_id[index]);
    }
    for (std::size_t index = 0U;
         index < result.catalog_digest.size(); ++index) {
        result.catalog_digest[index] =
            static_cast<std::byte>(
                source.catalog_digest[index]);
    }
    result.session_epoch = source.session_epoch;
    result.catalog_generation = source.catalog_generation;
    result.catalog_version = source.catalog_version;
    result.trade_date = source.trade_date;
    result.catalog_trade_date = source.catalog_trade_date;
    result.capacity = source.capacity;
    result.bound_count = source.bound_count;
    result.catalog_scope = source.catalog_scope;
    result.coverage_complete = source.coverage_complete;
    return result;
}

[[nodiscard]] bool MaybeHeartbeat(
    ipc::OrderEventDeltaRingProducerV1* producer,
    std::uint64_t* next_heartbeat_ns,
    std::string* error) {
    if (producer == nullptr || next_heartbeat_ns == nullptr ||
        error == nullptr) {
        return false;
    }
    const std::uint64_t now = MonotonicNanoseconds();
    if (now == 0U) {
        *error = "clock_gettime(CLOCK_MONOTONIC) failed";
        return false;
    }
    if (*next_heartbeat_ns != 0U &&
        now < *next_heartbeat_ns) {
        return true;
    }
    if (!producer->UpdateHeartbeat(now)) {
        *error = "event-ring heartbeat update failed";
        return false;
    }
    constexpr std::uint64_t interval = 1'000'000'000ULL;
    *next_heartbeat_ns =
        now <= std::numeric_limits<std::uint64_t>::max() - interval
            ? now + interval
            : std::numeric_limits<std::uint64_t>::max();
    return true;
}

enum class BatchResult : std::uint8_t {
    kData = 0U,
    kEmpty,
    kFailed,
};

[[nodiscard]] BatchResult ReadAndAggregate(
    const l2flow_shm_reader_v2* source_reader,
    ipc::OrderEventLiveAggregationEngineV1* engine,
    ipc::OrderEventDeltaRingProducerV1* producer,
    std::vector<ipc::RealtimeWireTickPayloadV2>* buffer,
    std::uint64_t* expected_sequence,
    std::size_t maximum_records,
    bool enforce_initial_event_retention,
    std::uint64_t event_ring_capacity,
    std::string* error) {
    if (source_reader == nullptr || engine == nullptr ||
        producer == nullptr || buffer == nullptr ||
        expected_sequence == nullptr || maximum_records == 0U ||
        maximum_records > buffer->size() || error == nullptr) {
        if (error != nullptr) {
            *error = "invalid internal read arguments";
        }
        return BatchResult::kFailed;
    }
    std::size_t written = 0U;
    std::uint64_t next = *expected_sequence;
    std::uint64_t observed = 0U;
    const int read_error = l2flow_shm_reader_ticks_v2(
        source_reader,
        *expected_sequence,
        buffer->data(),
        sizeof(ipc::RealtimeWireTickPayloadV2),
        maximum_records,
        &written,
        &next,
        &observed);
    if (read_error != L2FLOW_SHM_READER_OK_V2) {
        *error =
            "source tick read failed: code=" +
            std::to_string(read_error) +
            " expected=" + std::to_string(*expected_sequence) +
            " observed=" + std::to_string(observed);
        return BatchResult::kFailed;
    }
    if (written == 0U) {
        if (next != *expected_sequence || observed != 0U) {
            *error = "empty source read returned a noncanonical cursor";
            return BatchResult::kFailed;
        }
        return BatchResult::kEmpty;
    }
    if (written > maximum_records ||
        *expected_sequence >
            std::numeric_limits<std::uint64_t>::max() - written ||
        next !=
            *expected_sequence +
                static_cast<std::uint64_t>(written) ||
        observed != 0U) {
        *error = "source tick read returned a non-dense batch";
        return BatchResult::kFailed;
    }
    for (std::size_t index = 0U; index < written; ++index) {
        ipc::OrderEventLiveConsumeResultV1 result{};
        const auto consume_error =
            engine->Consume((*buffer)[index], &result);
        const std::uint64_t expected_tick =
            *expected_sequence +
            static_cast<std::uint64_t>(index);
        if (consume_error !=
                ipc::OrderEventLiveConsumeErrorV1::kNone ||
            result.source_tick_sequence != expected_tick) {
            *error =
                "live aggregation failed at source tick " +
                std::to_string(expected_tick) + ": " +
                std::string(
                    ipc::OrderEventLiveConsumeErrorNameV1(
                        consume_error));
            return BatchResult::kFailed;
        }
    }
    *expected_sequence = next;
    if (enforce_initial_event_retention &&
        producer->published_event_sequence() >
            event_ring_capacity) {
        *error =
            "startup event prefix exceeded event ring capacity "
            "before READY";
        return BatchResult::kFailed;
    }
    return BatchResult::kData;
}

[[nodiscard]] bool ProcessStartupCut(
    const l2flow_shm_reader_v2* source_reader,
    ipc::OrderEventLiveAggregationEngineV1* engine,
    ipc::OrderEventDeltaRingProducerV1* producer,
    std::vector<ipc::RealtimeWireTickPayloadV2>* buffer,
    std::uint64_t cut,
    std::uint64_t event_ring_capacity,
    std::uint64_t* expected_sequence,
    std::uint64_t* next_heartbeat_ns,
    std::string* error) {
    if (cut == std::numeric_limits<std::uint64_t>::max()) {
        *error = "source startup cut exhausted uint64 sequence space";
        return false;
    }
    while (*expected_sequence <= cut) {
        if (g_stop_requested != 0) {
            *error = "interrupted while processing the startup cut";
            return false;
        }
        const std::uint64_t remaining =
            cut - *expected_sequence + 1U;
        const std::size_t maximum_records =
            remaining < buffer->size()
                ? static_cast<std::size_t>(remaining)
                : buffer->size();
        const BatchResult result = ReadAndAggregate(
            source_reader,
            engine,
            producer,
            buffer,
            expected_sequence,
            maximum_records,
            true,
            event_ring_capacity,
            error);
        if (result != BatchResult::kData) {
            if (result == BatchResult::kEmpty) {
                *error =
                    "source contiguous startup cut was not readable";
            }
            return false;
        }
        if (!MaybeHeartbeat(
                producer, next_heartbeat_ns, error)) {
            return false;
        }
    }
    return *expected_sequence == cut + 1U;
}

enum class LiveLoopResult : std::uint8_t {
    kStoppedClean = 0U,
    kFailed,
    kInterrupted,
};

[[nodiscard]] LiveLoopResult RunLiveLoop(
    const app::OrderEventAggregatorOptionsV1& options,
    const l2flow_shm_session_info_v2& initial_source,
    const l2flow_shm_reader_v2* source_reader,
    ipc::OrderEventLiveAggregationEngineV1* engine,
    ipc::OrderEventDeltaRingProducerV1* producer,
    std::vector<ipc::RealtimeWireTickPayloadV2>* buffer,
    std::uint64_t* expected_sequence,
    std::uint64_t* next_heartbeat_ns,
    std::string* error) {
    for (;;) {
        if (g_stop_requested != 0) {
            *error = "termination signal received";
            return LiveLoopResult::kInterrupted;
        }
        l2flow_shm_health_v2 health{};
        const int health_error =
            l2flow_shm_reader_health_v2(source_reader, &health);
        if (health_error != L2FLOW_SHM_READER_OK_V2 ||
            !SourceHealthUsable(
                health, initial_source.session_epoch)) {
            *error =
                "source health became unavailable: code=" +
                std::to_string(health_error) +
                " state=" + std::to_string(health.server_state) +
                " flags=" + std::to_string(health.flags);
            return LiveLoopResult::kFailed;
        }

        const BatchResult batch = ReadAndAggregate(
            source_reader,
            engine,
            producer,
            buffer,
            expected_sequence,
            buffer->size(),
            false,
            options.event_ring_capacity,
            error);
        if (batch == BatchResult::kFailed) {
            return LiveLoopResult::kFailed;
        }
        if (!MaybeHeartbeat(
                producer, next_heartbeat_ns, error)) {
            return LiveLoopResult::kFailed;
        }
        if (batch == BatchResult::kData) {
            continue;
        }

        if (health.server_state ==
            static_cast<std::uint32_t>(
                ipc::RealtimeServerStateV2::kStoppedClean)) {
            l2flow_shm_session_info_v2 final_source{};
            const int session_error =
                l2flow_shm_reader_session_v2(
                    source_reader, &final_source);
            if (session_error != L2FLOW_SHM_READER_OK_V2 ||
                !SameSourceIdentity(
                    initial_source, final_source) ||
                final_source.server_state !=
                    static_cast<std::uint32_t>(
                        ipc::RealtimeServerStateV2::
                            kStoppedClean) ||
                (final_source.flags &
                 ipc::kRealtimeHeaderCoverageLostV2) != 0U ||
                final_source.tick_contiguous_published_sequence !=
                    final_source.tick_highest_published_sequence ||
                final_source.tick_contiguous_published_sequence ==
                    std::numeric_limits<std::uint64_t>::max()) {
                *error =
                    "source clean-stop snapshot was not canonical";
                return LiveLoopResult::kFailed;
            }
            const std::uint64_t final_next =
                final_source.tick_contiguous_published_sequence +
                1U;
            if (*expected_sequence == final_next) {
                return LiveLoopResult::kStoppedClean;
            }
            if (*expected_sequence < final_next) {
                continue;
            }
            *error =
                "event source cursor advanced beyond clean source "
                "watermark";
            return LiveLoopResult::kFailed;
        }
        std::this_thread::sleep_for(
            std::chrono::milliseconds(
                options.poll_interval_ms));
    }
}

[[nodiscard]] int Fail(
    ipc::OrderEventDeltaControlServerV1* control,
    ipc::OrderEventDeltaRingProducerV1* producer,
    std::string_view message) {
    if (producer != nullptr) {
        producer->MarkFailed();
    }
    if (control != nullptr) {
        control->Stop();
    }
    std::cerr << "mdl-order-event-aggregator: " << message
              << '\n';
    return 1;
}

[[nodiscard]] int Run(
    const app::OrderEventAggregatorOptionsV1& options) {
    if (!InstallSignalHandlers()) {
        std::cerr
            << "mdl-order-event-aggregator: failed to install "
               "signal handlers\n";
        return 1;
    }

    ShmReaderPtr source_reader;
    l2flow_shm_session_info_v2 initial_source{};
    int system_error = 0;
    int reader_error = L2FLOW_SHM_READER_OK_V2;
    const SourceAttachError attach_error = AttachSource(
        options,
        &source_reader,
        &initial_source,
        &system_error,
        &reader_error);
    if (attach_error != SourceAttachError::kNone) {
        std::cerr
            << "mdl-order-event-aggregator: source attach failed: "
            << SourceAttachErrorName(attach_error)
            << " system_error=" << system_error
            << " reader_error=" << reader_error << '\n';
        return 1;
    }
    if (!SourceStateReadable(initial_source.server_state) ||
        (initial_source.flags &
         ipc::kRealtimeHeaderCoverageLostV2) != 0U ||
        initial_source.tick_contiguous_published_sequence >
            initial_source.tick_highest_published_sequence) {
        std::cerr
            << "mdl-order-event-aggregator: source was not "
               "coverage-healthy at attach\n";
        return 1;
    }
    const std::uint64_t attach_cut =
        initial_source.tick_contiguous_published_sequence;

    l2flow::common::Identity128 event_run_id{};
    const std::uint64_t started_ns = MonotonicNanoseconds();
    if (!GenerateRunId(&event_run_id) || started_ns == 0U) {
        std::cerr
            << "mdl-order-event-aggregator: failed to create "
               "event session identity\n";
        return 1;
    }

    ipc::OrderEventDeltaRingConfigV1 ring_config{};
    ring_config.run_id = event_run_id;
    // The random event run ID makes an aggregator restart a distinct event
    // session even while it remains pinned to this source epoch.
    ring_config.session_epoch = options.session_epoch;
    ring_config.trade_date = options.trade_date;
    ring_config.ring_capacity = options.event_ring_capacity;
    ring_config.maximum_mapping_bytes =
        options.event_maximum_mapping_bytes;
    ring_config.producer_started_monotonic_ns = started_ns;
    std::unique_ptr<ipc::OrderEventDeltaRingProducerV1> producer;
    const auto ring_error =
        ipc::OrderEventDeltaRingProducerV1::Create(
            ring_config, &producer, &system_error);
    if (ring_error !=
            ipc::OrderEventDeltaRingCreateErrorV1::kNone ||
        producer == nullptr) {
        std::cerr
            << "mdl-order-event-aggregator: event ring create "
               "failed: "
            << ipc::OrderEventDeltaRingCreateErrorNameV1(
                   ring_error)
            << " system_error=" << system_error << '\n';
        return 1;
    }
    std::unique_ptr<ipc::OrderEventDeltaControlServerV1>
        control;
    EventFailureGuard failure_guard(producer.get());

    std::unique_ptr<ipc::OrderEventLiveAggregationEngineV1>
        engine;
    const auto engine_error =
        ipc::OrderEventLiveAggregationEngineV1::Create(
            {.maximum_shanghai_order_states =
                 options.maximum_shanghai_order_states,
             .maximum_shenzhen_order_states =
                 options.maximum_shenzhen_order_states},
            producer.get(),
            &engine);
    if (engine_error !=
            ipc::OrderEventLiveCreateErrorV1::kNone ||
        engine == nullptr) {
        return Fail(
            nullptr,
            producer.get(),
            std::string("live engine create failed: ") +
                std::string(
                    ipc::OrderEventLiveCreateErrorNameV1(
                        engine_error)));
    }

    ipc::OrderEventDeltaControlServerConfigV1 control_config{};
    control_config.source_session =
        SourceSession(initial_source);
    control_config.event_ring = producer.get();
    control_config.control_socket_path =
        options.event_control_socket;
    control_config.request_timeout =
        std::chrono::milliseconds(options.control_timeout_ms);
    const auto control_error =
        ipc::OrderEventDeltaControlServerV1::Create(
            control_config, &control, &system_error);
    if (control_error !=
            ipc::OrderEventDeltaControlServerCreateErrorV1::
                kNone ||
        control == nullptr) {
        return Fail(
            nullptr,
            producer.get(),
            std::string("event control create failed: ") +
                std::string(
                    ipc::OrderEventDeltaControlServerCreateErrorNameV1(
                        control_error)) +
                " system_error=" +
                std::to_string(system_error));
    }

    std::vector<ipc::RealtimeWireTickPayloadV2> buffer;
    try {
        buffer.resize(options.read_batch_records);
    } catch (...) {
        return Fail(
            control.get(),
            producer.get(),
            "source batch buffer allocation failed");
    }
    std::uint64_t expected_sequence = 1U;
    std::uint64_t next_heartbeat_ns = 0U;
    std::string error;
    if (!MaybeHeartbeat(
            producer.get(), &next_heartbeat_ns, &error) ||
        !ProcessStartupCut(
            source_reader.get(),
            engine.get(),
            producer.get(),
            &buffer,
            attach_cut,
            options.event_ring_capacity,
            &expected_sequence,
            &next_heartbeat_ns,
            &error)) {
        return Fail(control.get(), producer.get(), error);
    }

    l2flow_shm_session_info_v2 pre_ready_source{};
    const int pre_ready_error =
        l2flow_shm_reader_session_v2(
            source_reader.get(), &pre_ready_source);
    if (pre_ready_error != L2FLOW_SHM_READER_OK_V2 ||
        !SameSourceIdentity(
            initial_source, pre_ready_source) ||
        expected_sequence != attach_cut + 1U ||
        engine->consumed_source_tick_sequence() != attach_cut ||
        producer->consumed_source_tick_sequence() != attach_cut ||
        !SourceStateReadable(pre_ready_source.server_state) ||
        (pre_ready_source.flags &
         ipc::kRealtimeHeaderCoverageLostV2) != 0U ||
        pre_ready_source.tick_contiguous_published_sequence >
            pre_ready_source.tick_highest_published_sequence) {
        return Fail(
            control.get(),
            producer.get(),
            "source became unavailable before event READY");
    }
    const std::uint64_t oldest_retained =
        pre_ready_source.tick_contiguous_published_sequence >=
                pre_ready_source.tick_ring_capacity
            ? pre_ready_source.tick_contiguous_published_sequence -
                  pre_ready_source.tick_ring_capacity +
                  1U
            : 1U;
    if (expected_sequence < oldest_retained ||
        producer->published_event_sequence() >
            options.event_ring_capacity) {
        return Fail(
            control.get(),
            producer.get(),
            "startup prefix was overwritten before event READY");
    }
    if (g_stop_requested != 0) {
        return Fail(
            control.get(),
            producer.get(),
            "termination signal received before event READY");
    }

    if (!control->Start(&system_error) || !control->ready()) {
        return Fail(
            control.get(),
            producer.get(),
            std::string("event control READY failed: system_error=") +
                std::to_string(system_error));
    }
    std::cout
        << "READY source_epoch=" << options.session_epoch
        << " trade_date=" << options.trade_date
        << " source_cut=" << attach_cut
        << " event_prefix="
        << producer->published_event_sequence()
        << " event_socket="
        << options.event_control_socket.string() << '\n'
        << std::flush;

    const LiveLoopResult loop_result = RunLiveLoop(
        options,
        initial_source,
        source_reader.get(),
        engine.get(),
        producer.get(),
        &buffer,
        &expected_sequence,
        &next_heartbeat_ns,
        &error);
    if (loop_result != LiveLoopResult::kStoppedClean) {
        return Fail(control.get(), producer.get(), error);
    }

    if (!MaybeHeartbeat(
            producer.get(), &next_heartbeat_ns, &error) ||
        !producer->BeginDraining()) {
        return Fail(
            control.get(),
            producer.get(),
            error.empty()
                ? "event ring failed to begin clean draining"
                : error);
    }
    // Once DRAINING is visible, the control server refuses every new
    // GET_SESSION. Existing read-only mappings can observe the final state.
    control->Stop();
    if (!producer->StopClean()) {
        return Fail(
            nullptr,
            producer.get(),
            "event ring failed clean stop");
    }
    failure_guard.Release();
    std::cout
        << "STOPPED_CLEAN source_tick="
        << producer->consumed_source_tick_sequence()
        << " event_sequence="
        << producer->published_event_sequence() << '\n'
        << std::flush;
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        std::vector<std::string_view> arguments;
        arguments.reserve(
            argc > 1 ? static_cast<std::size_t>(argc - 1) : 0U);
        for (int index = 1; index < argc; ++index) {
            arguments.emplace_back(argv[index]);
        }
        app::OrderEventAggregatorOptionsV1 options{};
        std::string error;
        const auto parse =
            app::ParseOrderEventAggregatorArgumentsV1(
                arguments, &options, &error);
        if (parse ==
            app::OrderEventAggregatorParseResultV1::kHelp) {
            std::cout << app::OrderEventAggregatorHelpV1();
            return 0;
        }
        if (parse !=
            app::OrderEventAggregatorParseResultV1::kOk) {
            std::cerr
                << "mdl-order-event-aggregator: " << error
                << "\n\n"
                << app::OrderEventAggregatorHelpV1();
            return 2;
        }
        return Run(options);
    } catch (const std::exception& exception) {
        std::cerr
            << "mdl-order-event-aggregator: unexpected exception: "
            << exception.what() << '\n';
        return 1;
    } catch (...) {
        std::cerr
            << "mdl-order-event-aggregator: unexpected failure\n";
        return 1;
    }
}
