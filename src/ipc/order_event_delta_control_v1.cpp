#include "l2flow/ipc/order_event_delta_control_v1.h"
#include "l2flow/ipc/realtime_wire_v2.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <span>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

#include <fcntl.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#ifndef F_SEAL_FUTURE_WRITE
#define F_SEAL_FUTURE_WRITE 0x0010
#endif

namespace l2flow::ipc {
namespace {

constexpr std::int64_t kMaximumTimeoutMilliseconds = 60'000;
constexpr int kListenBacklog = 16;
constexpr std::size_t kMaximumReceivedDescriptors = 4U;

template <typename T>
[[nodiscard]] std::atomic_ref<T> Atomic(const T& value) noexcept {
    return std::atomic_ref<T>(const_cast<T&>(value));
}

void SetSystemError(int* output, int value) noexcept {
    if (output != nullptr) {
        *output = value;
    }
}

void CloseDescriptor(int* descriptor) noexcept {
    if (descriptor != nullptr && *descriptor >= 0) {
        static_cast<void>(::close(*descriptor));
        *descriptor = -1;
    }
}

[[nodiscard]] bool IdentityNonzero(
    const common::Identity128& identity) noexcept {
    return std::any_of(
        identity.begin(), identity.end(), [](std::byte value) {
            return value != std::byte{0};
        });
}

[[nodiscard]] common::Identity128 IdentityFromBytes(
    const std::array<std::uint8_t, 16U>& bytes) noexcept {
    common::Identity128 result{};
    std::memcpy(result.data(), bytes.data(), result.size());
    return result;
}

void CopyIdentity(
    const common::Identity128& source,
    std::array<std::uint8_t, 16U>* output) noexcept {
    if (output != nullptr) {
        std::memcpy(output->data(), source.data(), output->size());
    }
}

[[nodiscard]] common::Sha256Digest DigestFromBytes(
    const std::array<std::uint8_t, 32U>& bytes) noexcept {
    common::Sha256Digest result{};
    std::memcpy(result.data(), bytes.data(), result.size());
    return result;
}

void CopyDigest(
    const common::Sha256Digest& source,
    std::array<std::uint8_t, 32U>* output) noexcept {
    if (output != nullptr) {
        std::memcpy(output->data(), source.data(), output->size());
    }
}

template <typename Value>
[[nodiscard]] bool ObjectBytesZero(const Value& value) noexcept {
    static_assert(std::is_trivially_copyable_v<Value>);
    const auto bytes = std::as_bytes(std::span(&value, 1U));
    return std::all_of(
        bytes.begin(), bytes.end(), [](std::byte byte) {
            return byte == std::byte{0U};
        });
}

[[nodiscard]] bool KnownTemporalCoverage(
    OrderEventDeltaTemporalCoverageV1 value) noexcept {
    return value ==
               OrderEventDeltaTemporalCoverageV1::kFromMarketOpen ||
           value == OrderEventDeltaTemporalCoverageV1::
                        kFromProcessStart;
}

[[nodiscard]] bool KnownStreamQuality(
    OrderEventDeltaStreamQualityV1 value) noexcept {
    return value == OrderEventDeltaStreamQualityV1::
                        kLocalTickStreamContiguous;
}

[[nodiscard]] bool SourceSessionValid(
    const OrderEventDeltaSourceSessionV1& session) noexcept {
    return IdentityNonzero(session.run_id) &&
           !ObjectBytesZero(session.catalog_digest) &&
           session.session_epoch != 0U &&
           session.catalog_generation == 1U &&
           session.catalog_version != 0U &&
           session.trade_date != 0U &&
           session.catalog_trade_date == session.trade_date &&
           session.capacity != 0U &&
           session.bound_count == session.capacity &&
           session.catalog_scope ==
               static_cast<std::uint32_t>(
                   RealtimeCatalogScopeV2::kDeclaredDailyAShare) &&
           session.coverage_complete == 1U &&
           KnownTemporalCoverage(session.temporal_coverage) &&
           KnownStreamQuality(session.stream_quality);
}

[[nodiscard]] OrderEventDeltaSourceSessionV1
SourceSessionFromWire(
    const OrderEventDeltaSourceSessionWireV1& wire) noexcept {
    OrderEventDeltaSourceSessionV1 result{};
    result.run_id = IdentityFromBytes(wire.run_id);
    result.catalog_digest = DigestFromBytes(wire.catalog_digest);
    result.session_epoch = wire.session_epoch;
    result.catalog_generation = wire.catalog_generation;
    result.catalog_version = wire.catalog_version;
    result.trade_date = wire.trade_date;
    result.catalog_trade_date = wire.catalog_trade_date;
    result.capacity = wire.capacity;
    result.bound_count = wire.bound_count;
    result.catalog_scope = wire.catalog_scope;
    result.coverage_complete = wire.coverage_complete;
    return result;
}

void SourceSessionToWire(
    const OrderEventDeltaSourceSessionV1& session,
    OrderEventDeltaSourceSessionWireV1* wire) noexcept {
    if (wire == nullptr) {
        return;
    }
    CopyIdentity(session.run_id, &wire->run_id);
    CopyDigest(session.catalog_digest, &wire->catalog_digest);
    wire->session_epoch = session.session_epoch;
    wire->catalog_generation = session.catalog_generation;
    wire->catalog_version = session.catalog_version;
    wire->trade_date = session.trade_date;
    wire->catalog_trade_date = session.catalog_trade_date;
    wire->capacity = session.capacity;
    wire->bound_count = session.bound_count;
    wire->catalog_scope = session.catalog_scope;
    wire->coverage_complete = session.coverage_complete;
}

[[nodiscard]] bool EventSessionValid(
    const OrderEventDeltaSessionV1& session) noexcept {
    if (!IdentityNonzero(session.run_id) ||
        session.session_epoch == 0U || session.trade_date == 0U ||
        session.ring_capacity == 0U ||
        !KnownTemporalCoverage(session.temporal_coverage) ||
        !KnownStreamQuality(session.stream_quality) ||
        session.ring_capacity >
            (std::numeric_limits<std::uint64_t>::max() -
             sizeof(OrderEventDeltaHeaderV1)) /
                sizeof(OrderEventDeltaSlotV1)) {
        return false;
    }
    constexpr std::uint64_t page_mask = 4096U - 1U;
    const std::uint64_t logical_bytes =
        sizeof(OrderEventDeltaHeaderV1) +
        session.ring_capacity * sizeof(OrderEventDeltaSlotV1);
    if (logical_bytes >
        std::numeric_limits<std::uint64_t>::max() - page_mask) {
        return false;
    }
    const std::uint64_t expected_mapping_bytes =
        (logical_bytes + page_mask) & ~page_mask;
    return session.total_mapping_bytes == expected_mapping_bytes &&
           session.total_mapping_bytes <=
               static_cast<std::uint64_t>(
                   std::numeric_limits<std::size_t>::max()) &&
           session.total_mapping_bytes <=
               static_cast<std::uint64_t>(
                   std::numeric_limits<off_t>::max());
}

[[nodiscard]] bool TimeoutValid(
    std::chrono::milliseconds timeout) noexcept {
    return timeout.count() > 0 &&
           timeout.count() <= kMaximumTimeoutMilliseconds;
}

[[nodiscard]] bool KnownProducerState(
    std::uint32_t state) noexcept {
    return state >= static_cast<std::uint32_t>(
                        OrderEventDeltaProducerStateV1::kInitializing) &&
           state <= static_cast<std::uint32_t>(
                        OrderEventDeltaProducerStateV1::kFailed);
}

[[nodiscard]] bool KnownHeaderFlags(std::uint32_t flags) noexcept {
    return (flags & ~kOrderEventDeltaCoverageLostV1) == 0U;
}

[[nodiscard]] bool AbsoluteSocketPath(
    const std::filesystem::path& path,
    std::string* native) noexcept {
    if (native == nullptr) {
        return false;
    }
    try {
        if (!path.is_absolute() || path.filename().empty() ||
            path.filename() == "." || path.filename() == "..") {
            return false;
        }
        *native = path.string();
        sockaddr_un address{};
        return !native->empty() &&
               native->size() < sizeof(address.sun_path);
    } catch (...) {
        return false;
    }
}

using SteadyClock = std::chrono::steady_clock;

struct IoDeadline final {
    SteadyClock::time_point value{};
};

[[nodiscard]] IoDeadline MakeDeadline(
    std::chrono::milliseconds timeout) noexcept {
    return IoDeadline{SteadyClock::now() + timeout};
}

[[nodiscard]] int RemainingMilliseconds(
    const IoDeadline& deadline) noexcept {
    const auto now = SteadyClock::now();
    if (now >= deadline.value) {
        return 0;
    }
    const auto remaining = deadline.value - now;
    const auto milliseconds =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            remaining);
    const auto rounded =
        milliseconds < remaining ? milliseconds.count() + 1
                                 : milliseconds.count();
    return static_cast<int>(std::clamp<std::int64_t>(
        rounded, 1, std::numeric_limits<int>::max()));
}

enum class WaitResult {
    kReady,
    kTimeout,
    kFailed,
};

[[nodiscard]] WaitResult WaitForDescriptor(
    int descriptor,
    short events,
    const IoDeadline& deadline,
    int* system_error_number) noexcept {
    if (descriptor < 0) {
        SetSystemError(system_error_number, EBADF);
        return WaitResult::kFailed;
    }
    for (;;) {
        const int timeout = RemainingMilliseconds(deadline);
        if (timeout == 0) {
            SetSystemError(system_error_number, ETIMEDOUT);
            return WaitResult::kTimeout;
        }
        pollfd item{};
        item.fd = descriptor;
        item.events = events;
        const int result = ::poll(&item, 1U, timeout);
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result == 0) {
            SetSystemError(system_error_number, ETIMEDOUT);
            return WaitResult::kTimeout;
        }
        if (result < 0) {
            SetSystemError(system_error_number, errno);
            return WaitResult::kFailed;
        }
        if ((item.revents & events) != 0) {
            return WaitResult::kReady;
        }
        if ((item.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
            SetSystemError(system_error_number, ECONNRESET);
            return WaitResult::kFailed;
        }
    }
}

enum class IoResult {
    kOk,
    kTimeout,
    kFailed,
    kProtocol,
};

[[nodiscard]] IoResult SendPacket(
    int socket_fd,
    const void* data,
    std::size_t bytes,
    int attached_descriptor,
    const IoDeadline& deadline,
    int* system_error_number) noexcept {
    if (socket_fd < 0 || data == nullptr || bytes == 0U) {
        SetSystemError(system_error_number, EINVAL);
        return IoResult::kFailed;
    }
    iovec vector{};
    vector.iov_base = const_cast<void*>(data);
    vector.iov_len = bytes;
    msghdr message{};
    message.msg_iov = &vector;
    message.msg_iovlen = 1U;
    alignas(cmsghdr)
        std::array<std::byte, CMSG_SPACE(sizeof(int))> control{};
    if (attached_descriptor >= 0) {
        message.msg_control = control.data();
        message.msg_controllen = control.size();
        cmsghdr* const rights = CMSG_FIRSTHDR(&message);
        if (rights == nullptr) {
            SetSystemError(system_error_number, EINVAL);
            return IoResult::kFailed;
        }
        rights->cmsg_level = SOL_SOCKET;
        rights->cmsg_type = SCM_RIGHTS;
        rights->cmsg_len = CMSG_LEN(sizeof(int));
        std::memcpy(
            CMSG_DATA(rights),
            &attached_descriptor,
            sizeof(attached_descriptor));
    }
    for (;;) {
        const ssize_t sent =
            ::sendmsg(socket_fd, &message, MSG_NOSIGNAL);
        if (sent == static_cast<ssize_t>(bytes)) {
            return IoResult::kOk;
        }
        if (sent >= 0) {
            SetSystemError(system_error_number, EPROTO);
            return IoResult::kProtocol;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            SetSystemError(system_error_number, errno);
            return IoResult::kFailed;
        }
        const WaitResult ready = WaitForDescriptor(
            socket_fd, POLLOUT, deadline, system_error_number);
        if (ready == WaitResult::kTimeout) {
            return IoResult::kTimeout;
        }
        if (ready == WaitResult::kFailed) {
            return IoResult::kFailed;
        }
    }
}

[[nodiscard]] bool CloseReceivedRights(
    msghdr* message,
    int* first_descriptor,
    std::size_t* descriptor_count) noexcept {
    if (message == nullptr || first_descriptor == nullptr ||
        descriptor_count == nullptr) {
        return false;
    }
    *first_descriptor = -1;
    *descriptor_count = 0U;
    bool valid = true;
    for (cmsghdr* header = CMSG_FIRSTHDR(message);
         header != nullptr;
         header = CMSG_NXTHDR(message, header)) {
        if (header->cmsg_level != SOL_SOCKET ||
            header->cmsg_type != SCM_RIGHTS ||
            header->cmsg_len < CMSG_LEN(sizeof(int))) {
            valid = false;
            continue;
        }
        const std::size_t payload_bytes =
            header->cmsg_len - CMSG_LEN(0U);
        if (payload_bytes % sizeof(int) != 0U) {
            valid = false;
            continue;
        }
        const std::size_t count = payload_bytes / sizeof(int);
        const auto* const descriptors =
            reinterpret_cast<const int*>(CMSG_DATA(header));
        for (std::size_t index = 0U; index < count; ++index) {
            if (*descriptor_count == 0U) {
                *first_descriptor = descriptors[index];
            } else {
                static_cast<void>(::close(descriptors[index]));
            }
            ++(*descriptor_count);
        }
    }
    return valid;
}

template <typename Packet>
[[nodiscard]] IoResult ReceivePacket(
    int socket_fd,
    Packet* output,
    int* received_descriptor,
    std::size_t* received_descriptor_count,
    const IoDeadline& deadline,
    int* system_error_number) noexcept {
    static_assert(std::is_trivially_copyable_v<Packet>);
    if (socket_fd < 0 || output == nullptr ||
        received_descriptor == nullptr ||
        received_descriptor_count == nullptr) {
        SetSystemError(system_error_number, EINVAL);
        return IoResult::kFailed;
    }
    *output = {};
    *received_descriptor = -1;
    *received_descriptor_count = 0U;
    alignas(cmsghdr) std::array<
        std::byte,
        CMSG_SPACE(
            sizeof(int) * kMaximumReceivedDescriptors)>
        control{};
    iovec vector{};
    vector.iov_base = output;
    vector.iov_len = sizeof(Packet);
    msghdr message{};
    message.msg_iov = &vector;
    message.msg_iovlen = 1U;
    message.msg_control = control.data();
    message.msg_controllen = control.size();
    for (;;) {
        message.msg_flags = 0;
        message.msg_controllen = control.size();
        const ssize_t received = ::recvmsg(
            socket_fd,
            &message,
#ifdef MSG_CMSG_CLOEXEC
            MSG_CMSG_CLOEXEC
#else
            0
#endif
        );
        if (received < 0 && errno == EINTR) {
            continue;
        }
        if (received < 0 &&
            (errno == EAGAIN || errno == EWOULDBLOCK)) {
            const WaitResult ready = WaitForDescriptor(
                socket_fd, POLLIN, deadline, system_error_number);
            if (ready == WaitResult::kTimeout) {
                return IoResult::kTimeout;
            }
            if (ready == WaitResult::kFailed) {
                return IoResult::kFailed;
            }
            continue;
        }
        if (received <= 0) {
            SetSystemError(
                system_error_number,
                received == 0 ? ECONNRESET : errno);
            return IoResult::kFailed;
        }
        const bool ancillary_valid = CloseReceivedRights(
            &message,
            received_descriptor,
            received_descriptor_count);
        const bool shape_valid =
            static_cast<std::size_t>(received) == sizeof(Packet) &&
            (message.msg_flags & (MSG_TRUNC | MSG_CTRUNC)) == 0;
        if (!shape_valid || !ancillary_valid ||
            *received_descriptor_count >
                kMaximumReceivedDescriptors) {
            CloseDescriptor(received_descriptor);
            SetSystemError(system_error_number, EPROTO);
            return IoResult::kProtocol;
        }
#ifndef MSG_CMSG_CLOEXEC
        if (*received_descriptor >= 0) {
            const int flags =
                ::fcntl(*received_descriptor, F_GETFD);
            if (flags < 0 ||
                ::fcntl(
                    *received_descriptor,
                    F_SETFD,
                    flags | FD_CLOEXEC) != 0) {
                SetSystemError(system_error_number, errno);
                CloseDescriptor(received_descriptor);
                return IoResult::kFailed;
            }
        }
#endif
        return IoResult::kOk;
    }
}

[[nodiscard]] bool BasicRingDescriptorValid(
    int descriptor,
    const OrderEventDeltaSessionV1& session,
    int* system_error_number) noexcept {
    if (descriptor < 0 || !EventSessionValid(session)) {
        SetSystemError(system_error_number, EINVAL);
        return false;
    }
    const int access_flags = ::fcntl(descriptor, F_GETFL);
    const int descriptor_flags = ::fcntl(descriptor, F_GETFD);
    const int seals = ::fcntl(descriptor, F_GET_SEALS);
    if (access_flags < 0 || descriptor_flags < 0 || seals < 0) {
        SetSystemError(system_error_number, errno);
        return false;
    }
    constexpr int required_seals =
        F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_FUTURE_WRITE |
        F_SEAL_SEAL;
    struct stat descriptor_stat {};
    if (::fstat(descriptor, &descriptor_stat) != 0) {
        SetSystemError(system_error_number, errno);
        return false;
    }
    if ((access_flags & O_ACCMODE) != O_RDONLY ||
        (descriptor_flags & FD_CLOEXEC) == 0 ||
        (seals & required_seals) != required_seals ||
        !S_ISREG(descriptor_stat.st_mode) ||
        descriptor_stat.st_size <= 0 ||
        static_cast<std::uint64_t>(descriptor_stat.st_size) !=
            session.total_mapping_bytes) {
        SetSystemError(system_error_number, EPROTO);
        return false;
    }
    return true;
}

[[nodiscard]] bool HeaderMatchesSession(
    const OrderEventDeltaHeaderV1& header,
    const OrderEventDeltaSessionV1& session) noexcept {
    if (header.magic != kOrderEventDeltaMagicV1 ||
        header.abi_major != kOrderEventDeltaWireMajorV1 ||
        header.abi_minor != kOrderEventDeltaWireMinorV1 ||
        header.header_bytes != sizeof(OrderEventDeltaHeaderV1) ||
        header.endian_marker != kOrderEventDeltaEndianMarkerV1 ||
        header.total_mapping_bytes != session.total_mapping_bytes ||
        IdentityFromBytes(header.run_id) != session.run_id ||
        header.session_epoch != session.session_epoch ||
        header.trade_date != session.trade_date ||
        header.ring_capacity != session.ring_capacity ||
        header.slot_stride != sizeof(OrderEventDeltaSlotV1) ||
        header.slots_offset != sizeof(OrderEventDeltaHeaderV1) ||
        header.temporal_coverage !=
            static_cast<std::uint32_t>(
                session.temporal_coverage) ||
        header.stream_quality !=
            static_cast<std::uint32_t>(session.stream_quality) ||
        !ObjectBytesZero(header.reserved)) {
        return false;
    }
    return KnownProducerState(
               Atomic(header.producer_state)
                   .load(std::memory_order_acquire)) &&
           KnownHeaderFlags(
               Atomic(header.flags).load(std::memory_order_acquire));
}

void SnapshotToResponse(
    const OrderEventDeltaControlSnapshotV1& snapshot,
    OrderEventDeltaControlGetSessionResponseV1* response) noexcept {
    SourceSessionToWire(
        snapshot.source_session, &response->source_session);
    CopyIdentity(
        snapshot.event_session.run_id,
        &response->event_run_id);
    response->event_session_epoch =
        snapshot.event_session.session_epoch;
    response->event_trade_date =
        snapshot.event_session.trade_date;
    response->event_producer_state =
        snapshot.event_producer_state;
    response->event_header_flags =
        snapshot.event_header_flags;
    response->event_ring_capacity =
        snapshot.event_session.ring_capacity;
    response->event_total_mapping_bytes =
        snapshot.event_session.total_mapping_bytes;
    response->event_published_sequence =
        snapshot.event_published_sequence;
    response->source_tick_consumed_sequence =
        snapshot.source_tick_consumed_sequence;
    response->heartbeat_monotonic_ns =
        snapshot.heartbeat_monotonic_ns;
    response->producer_started_monotonic_ns =
        snapshot.producer_started_monotonic_ns;
    response->source_temporal_coverage =
        static_cast<std::uint32_t>(
            snapshot.source_session.temporal_coverage);
    response->source_stream_quality =
        static_cast<std::uint32_t>(
            snapshot.source_session.stream_quality);
    response->event_temporal_coverage =
        static_cast<std::uint32_t>(
            snapshot.event_session.temporal_coverage);
    response->event_stream_quality =
        static_cast<std::uint32_t>(
            snapshot.event_session.stream_quality);
}

[[nodiscard]] OrderEventDeltaControlSnapshotV1
SnapshotFromResponse(
    const OrderEventDeltaControlGetSessionResponseV1&
        response) noexcept {
    OrderEventDeltaControlSnapshotV1 result{};
    result.source_session =
        SourceSessionFromWire(response.source_session);
    result.source_session.temporal_coverage =
        static_cast<OrderEventDeltaTemporalCoverageV1>(
            response.source_temporal_coverage);
    result.source_session.stream_quality =
        static_cast<OrderEventDeltaStreamQualityV1>(
            response.source_stream_quality);
    result.event_session.run_id =
        IdentityFromBytes(response.event_run_id);
    result.event_session.session_epoch =
        response.event_session_epoch;
    result.event_session.trade_date =
        response.event_trade_date;
    result.event_session.ring_capacity =
        response.event_ring_capacity;
    result.event_session.total_mapping_bytes =
        response.event_total_mapping_bytes;
    result.event_session.temporal_coverage =
        static_cast<OrderEventDeltaTemporalCoverageV1>(
            response.event_temporal_coverage);
    result.event_session.stream_quality =
        static_cast<OrderEventDeltaStreamQualityV1>(
            response.event_stream_quality);
    result.event_published_sequence =
        response.event_published_sequence;
    result.source_tick_consumed_sequence =
        response.source_tick_consumed_sequence;
    result.heartbeat_monotonic_ns =
        response.heartbeat_monotonic_ns;
    result.producer_started_monotonic_ns =
        response.producer_started_monotonic_ns;
    result.event_producer_state =
        response.event_producer_state;
    result.event_header_flags =
        response.event_header_flags;
    return result;
}

[[nodiscard]] std::uint64_t NextRequestId() noexcept {
    static std::atomic<std::uint64_t> next{1U};
    for (;;) {
        const std::uint64_t value =
            next.fetch_add(1U, std::memory_order_relaxed);
        if (value != 0U) {
            return value;
        }
    }
}

}  // namespace

std::string_view OrderEventDeltaControlServerCreateErrorNameV1(
    OrderEventDeltaControlServerCreateErrorV1 error) noexcept {
    switch (error) {
        case OrderEventDeltaControlServerCreateErrorV1::kNone:
            return "none";
        case OrderEventDeltaControlServerCreateErrorV1::kNullOutput:
            return "null_output";
        case OrderEventDeltaControlServerCreateErrorV1::
            kInvalidConfiguration:
            return "invalid_configuration";
        case OrderEventDeltaControlServerCreateErrorV1::
            kRingUnavailable:
            return "ring_unavailable";
        case OrderEventDeltaControlServerCreateErrorV1::
            kRingDescriptorInvalid:
            return "ring_descriptor_invalid";
        case OrderEventDeltaControlServerCreateErrorV1::
            kSocketPathExists:
            return "socket_path_exists";
        case OrderEventDeltaControlServerCreateErrorV1::
            kSocketCreateFailed:
            return "socket_create_failed";
        case OrderEventDeltaControlServerCreateErrorV1::
            kSocketBindFailed:
            return "socket_bind_failed";
        case OrderEventDeltaControlServerCreateErrorV1::
            kResourceExhausted:
            return "resource_exhausted";
        case OrderEventDeltaControlServerCreateErrorV1::
            kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

std::string_view OrderEventDeltaControlClientErrorNameV1(
    OrderEventDeltaControlClientErrorV1 error) noexcept {
    switch (error) {
        case OrderEventDeltaControlClientErrorV1::kNone:
            return "none";
        case OrderEventDeltaControlClientErrorV1::kInvalidArgument:
            return "invalid_argument";
        case OrderEventDeltaControlClientErrorV1::
            kSocketPathInvalid:
            return "socket_path_invalid";
        case OrderEventDeltaControlClientErrorV1::kConnectFailed:
            return "connect_failed";
        case OrderEventDeltaControlClientErrorV1::
            kPeerCredentialRejected:
            return "peer_credential_rejected";
        case OrderEventDeltaControlClientErrorV1::kTimeout:
            return "timeout";
        case OrderEventDeltaControlClientErrorV1::
            kTransportFailed:
            return "transport_failed";
        case OrderEventDeltaControlClientErrorV1::kProtocolError:
            return "protocol_error";
        case OrderEventDeltaControlClientErrorV1::
            kUnsupportedVersion:
            return "unsupported_version";
        case OrderEventDeltaControlClientErrorV1::kUnavailable:
            return "unavailable";
        case OrderEventDeltaControlClientErrorV1::
            kSourceSessionMismatch:
            return "source_session_mismatch";
        case OrderEventDeltaControlClientErrorV1::
            kRingDescriptorInvalid:
            return "ring_descriptor_invalid";
        case OrderEventDeltaControlClientErrorV1::
            kResourceExhausted:
            return "resource_exhausted";
        case OrderEventDeltaControlClientErrorV1::
            kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

class OrderEventDeltaControlServerV1::Impl final {
public:
    explicit Impl(OrderEventDeltaControlServerConfigV1 config)
        : config_(std::move(config)) {}

    ~Impl() {
        Stop();
        SafeUnlinkSocket();
        if (header_mapping_ != MAP_FAILED) {
            static_cast<void>(::munmap(
                header_mapping_,
                sizeof(OrderEventDeltaHeaderV1)));
        }
        CloseDescriptor(&ring_descriptor_);
        CloseDescriptor(&stop_event_fd_);
        CloseDescriptor(&listener_fd_);
    }

    [[nodiscard]] OrderEventDeltaControlServerCreateErrorV1
    Initialize(int* system_error_number) noexcept {
        SetSystemError(system_error_number, 0);
        if constexpr (std::endian::native != std::endian::little) {
            return OrderEventDeltaControlServerCreateErrorV1::
                kInvalidConfiguration;
        }
        std::string socket_path;
        if (!SourceSessionValid(config_.source_session) ||
            config_.event_ring == nullptr ||
            !TimeoutValid(config_.request_timeout) ||
            !AbsoluteSocketPath(
                config_.control_socket_path, &socket_path)) {
            return OrderEventDeltaControlServerCreateErrorV1::
                kInvalidConfiguration;
        }

        event_session_ = config_.event_ring->session();
        if (!EventSessionValid(event_session_) ||
            event_session_.trade_date !=
                config_.source_session.trade_date ||
            event_session_.temporal_coverage !=
                config_.source_session.temporal_coverage ||
            event_session_.stream_quality !=
                config_.source_session.stream_quality) {
            return OrderEventDeltaControlServerCreateErrorV1::
                kInvalidConfiguration;
        }
        if (config_.event_ring->state() !=
            OrderEventDeltaProducerStateV1::kActive) {
            return OrderEventDeltaControlServerCreateErrorV1::
                kRingUnavailable;
        }
        if (!config_.event_ring->DuplicateReadOnlyDescriptor(
                &ring_descriptor_, system_error_number)) {
            return OrderEventDeltaControlServerCreateErrorV1::
                kRingDescriptorInvalid;
        }
        if (!BasicRingDescriptorValid(
                ring_descriptor_,
                event_session_,
                system_error_number)) {
            return OrderEventDeltaControlServerCreateErrorV1::
                kRingDescriptorInvalid;
        }
        std::unique_ptr<OrderEventDeltaRingReaderV1> validator;
        const OrderEventDeltaReaderOpenErrorV1 open_error =
            OrderEventDeltaRingReaderV1::Open(
                ring_descriptor_,
                event_session_,
                &validator,
                system_error_number);
        if (open_error !=
                OrderEventDeltaReaderOpenErrorV1::kNone ||
            validator == nullptr) {
            return open_error ==
                           OrderEventDeltaReaderOpenErrorV1::
                               kUnavailable
                       ? OrderEventDeltaControlServerCreateErrorV1::
                             kRingUnavailable
                       : OrderEventDeltaControlServerCreateErrorV1::
                             kRingDescriptorInvalid;
        }
        header_mapping_ = ::mmap(
            nullptr,
            sizeof(OrderEventDeltaHeaderV1),
            PROT_READ,
            MAP_SHARED,
            ring_descriptor_,
            0);
        if (header_mapping_ == MAP_FAILED) {
            SetSystemError(system_error_number, errno);
            return OrderEventDeltaControlServerCreateErrorV1::
                kRingDescriptorInvalid;
        }
        header_ = static_cast<const OrderEventDeltaHeaderV1*>(
            header_mapping_);
        OrderEventDeltaControlSnapshotV1 snapshot{};
        if (!TakeSnapshot(&snapshot, true)) {
            return OrderEventDeltaControlServerCreateErrorV1::
                kRingUnavailable;
        }
        return BindSocket(socket_path, system_error_number);
    }

    [[nodiscard]] bool Start(int* system_error_number) noexcept {
        SetSystemError(system_error_number, 0);
        if (lifecycle_ != Lifecycle::kCreated) {
            SetSystemError(system_error_number, EALREADY);
            return false;
        }
        OrderEventDeltaControlSnapshotV1 snapshot{};
        if (!TakeSnapshot(&snapshot, true)) {
            SetSystemError(system_error_number, EHOSTDOWN);
            return false;
        }
        stop_requested_.store(false, std::memory_order_release);
        ready_.store(true, std::memory_order_release);
        try {
            control_thread_ =
                std::thread([this] { ControlLoop(); });
        } catch (const std::system_error& error) {
            ready_.store(false, std::memory_order_release);
            SetSystemError(system_error_number, error.code().value());
            return false;
        } catch (...) {
            ready_.store(false, std::memory_order_release);
            SetSystemError(system_error_number, EAGAIN);
            return false;
        }
        lifecycle_ = Lifecycle::kRunning;
        return true;
    }

    void Stop() noexcept {
        ready_.store(false, std::memory_order_release);
        if (lifecycle_ != Lifecycle::kRunning) {
            if (lifecycle_ == Lifecycle::kCreated) {
                CloseDescriptor(&listener_fd_);
                SafeUnlinkSocket();
                lifecycle_ = Lifecycle::kStopped;
            }
            return;
        }
        stop_requested_.store(true, std::memory_order_release);
        if (stop_event_fd_ >= 0) {
            const std::uint64_t notification = 1U;
            ssize_t result = -1;
            do {
                result = ::write(
                    stop_event_fd_,
                    &notification,
                    sizeof(notification));
            } while (result < 0 && errno == EINTR);
        }
        if (control_thread_.joinable()) {
            control_thread_.join();
        }
        CloseDescriptor(&listener_fd_);
        SafeUnlinkSocket();
        lifecycle_ = Lifecycle::kStopped;
    }

    [[nodiscard]] bool ready() const noexcept {
        return ready_.load(std::memory_order_acquire);
    }

    [[nodiscard]] const std::filesystem::path&
    control_socket_path() const noexcept {
        return config_.control_socket_path;
    }

private:
    enum class Lifecycle {
        kCreated,
        kRunning,
        kStopped,
    };

    [[nodiscard]] OrderEventDeltaControlServerCreateErrorV1
    BindSocket(
        const std::string& socket_path,
        int* system_error_number) noexcept {
        try {
            const std::filesystem::path parent =
                config_.control_socket_path.parent_path();
            int directory_flags =
                O_RDONLY | O_DIRECTORY | O_CLOEXEC;
#ifdef O_NOFOLLOW
            directory_flags |= O_NOFOLLOW;
#endif
            const int directory_fd =
                ::open(parent.c_str(), directory_flags);
            struct stat directory_stat {};
            int directory_error = 0;
            bool directory_ok = false;
            if (directory_fd < 0) {
                directory_error = errno;
            } else if (::fstat(
                           directory_fd,
                           &directory_stat) != 0) {
                directory_error = errno;
            } else {
                directory_ok =
                    S_ISDIR(directory_stat.st_mode) &&
                    directory_stat.st_uid == ::geteuid() &&
                    (directory_stat.st_mode & 0077) == 0;
                if (!directory_ok) {
                    directory_error = EACCES;
                }
            }
            if (directory_fd >= 0) {
                static_cast<void>(::close(directory_fd));
            }
            if (!directory_ok) {
                SetSystemError(
                    system_error_number, directory_error);
                return OrderEventDeltaControlServerCreateErrorV1::
                    kInvalidConfiguration;
            }

            struct stat existing {};
            if (::lstat(socket_path.c_str(), &existing) == 0) {
                SetSystemError(system_error_number, EEXIST);
                return OrderEventDeltaControlServerCreateErrorV1::
                    kSocketPathExists;
            }
            if (errno != ENOENT) {
                SetSystemError(system_error_number, errno);
                return OrderEventDeltaControlServerCreateErrorV1::
                    kSocketBindFailed;
            }

            listener_fd_ = ::socket(
                AF_UNIX,
                SOCK_SEQPACKET | SOCK_NONBLOCK | SOCK_CLOEXEC,
                0);
            if (listener_fd_ < 0) {
                SetSystemError(system_error_number, errno);
                return OrderEventDeltaControlServerCreateErrorV1::
                    kSocketCreateFailed;
            }
            sockaddr_un address{};
            address.sun_family = AF_UNIX;
            std::memcpy(
                address.sun_path,
                socket_path.c_str(),
                socket_path.size() + 1U);
            const mode_t old_mask = ::umask(0077);
            const int bind_result = ::bind(
                listener_fd_,
                reinterpret_cast<const sockaddr*>(&address),
                static_cast<socklen_t>(
                    offsetof(sockaddr_un, sun_path) +
                    socket_path.size() + 1U));
            static_cast<void>(::umask(old_mask));
            if (bind_result != 0) {
                SetSystemError(system_error_number, errno);
                return OrderEventDeltaControlServerCreateErrorV1::
                    kSocketBindFailed;
            }
            struct stat socket_stat {};
            if (::lstat(socket_path.c_str(), &socket_stat) != 0 ||
                !S_ISSOCK(socket_stat.st_mode) ||
                socket_stat.st_uid != ::geteuid()) {
                SetSystemError(
                    system_error_number,
                    errno == 0 ? EACCES : errno);
                return OrderEventDeltaControlServerCreateErrorV1::
                    kSocketBindFailed;
            }
            socket_device_ = socket_stat.st_dev;
            socket_inode_ = socket_stat.st_ino;
            socket_bound_ = true;
            if (::chmod(socket_path.c_str(), 0600) != 0 ||
                ::listen(listener_fd_, kListenBacklog) != 0) {
                SetSystemError(system_error_number, errno);
                SafeUnlinkSocket();
                return OrderEventDeltaControlServerCreateErrorV1::
                    kSocketBindFailed;
            }
            stop_event_fd_ =
                ::eventfd(0U, EFD_CLOEXEC | EFD_NONBLOCK);
            if (stop_event_fd_ < 0) {
                SetSystemError(system_error_number, errno);
                return OrderEventDeltaControlServerCreateErrorV1::
                    kSocketCreateFailed;
            }
            return OrderEventDeltaControlServerCreateErrorV1::kNone;
        } catch (...) {
            return OrderEventDeltaControlServerCreateErrorV1::
                kUnexpectedFailure;
        }
    }

    void SafeUnlinkSocket() noexcept {
        if (!socket_bound_) {
            return;
        }
        struct stat current {};
        if (::lstat(
                config_.control_socket_path.c_str(),
                &current) == 0 &&
            current.st_dev == socket_device_ &&
            current.st_ino == socket_inode_ &&
            S_ISSOCK(current.st_mode)) {
            static_cast<void>(
                ::unlink(config_.control_socket_path.c_str()));
        }
        socket_bound_ = false;
    }

    [[nodiscard]] bool TakeSnapshot(
        OrderEventDeltaControlSnapshotV1* output,
        bool require_active) const noexcept {
        if (output == nullptr || header_ == nullptr ||
            !HeaderMatchesSession(*header_, event_session_)) {
            return false;
        }
        *output = {};
        output->source_session = config_.source_session;
        output->event_session = event_session_;

        const std::uint32_t initial_state =
            Atomic(header_->producer_state)
                .load(std::memory_order_acquire);
        // The source acquire precedes the event-prefix acquire by contract.
        output->source_tick_consumed_sequence =
            Atomic(header_->source_tick_consumed_sequence)
                .load(std::memory_order_acquire);
        output->event_published_sequence =
            Atomic(header_->event_published_sequence)
                .load(std::memory_order_acquire);
        output->heartbeat_monotonic_ns =
            Atomic(header_->heartbeat_monotonic_ns)
                .load(std::memory_order_acquire);
        output->producer_started_monotonic_ns =
            header_->producer_started_monotonic_ns;
        output->event_header_flags =
            Atomic(header_->flags).load(std::memory_order_acquire);
        output->event_producer_state =
            Atomic(header_->producer_state)
                .load(std::memory_order_acquire);
        if (!KnownProducerState(initial_state) ||
            !KnownProducerState(output->event_producer_state) ||
            !KnownHeaderFlags(output->event_header_flags)) {
            return false;
        }
        if (!require_active) {
            return true;
        }
        const std::uint32_t active = static_cast<std::uint32_t>(
            OrderEventDeltaProducerStateV1::kActive);
        return initial_state == active &&
               output->event_producer_state == active &&
               output->event_header_flags == 0U;
    }

    [[nodiscard]] bool PeerUidAllowed(int client) const noexcept {
        ucred credentials{};
        socklen_t bytes =
            static_cast<socklen_t>(sizeof(credentials));
        return client >= 0 &&
               ::getsockopt(
                   client,
                   SOL_SOCKET,
                   SO_PEERCRED,
                   &credentials,
                   &bytes) == 0 &&
               bytes == sizeof(credentials) &&
               credentials.uid == ::geteuid();
    }

    void HandleClient(int client) noexcept {
        const IoDeadline deadline =
            MakeDeadline(config_.request_timeout);
        OrderEventDeltaControlGetSessionRequestV1 request{};
        int unexpected_descriptor = -1;
        std::size_t unexpected_descriptor_count = 0U;
        int ignored_error = 0;
        const IoResult received = ReceivePacket(
            client,
            &request,
            &unexpected_descriptor,
            &unexpected_descriptor_count,
            deadline,
            &ignored_error);
        CloseDescriptor(&unexpected_descriptor);
        if (received != IoResult::kOk ||
            unexpected_descriptor_count != 0U) {
            return;
        }

        OrderEventDeltaControlGetSessionResponseV1 response{};
        response.magic = kOrderEventDeltaControlMagicV1;
        response.protocol_major =
            kOrderEventDeltaControlProtocolMajorV1;
        response.protocol_minor =
            kOrderEventDeltaControlProtocolMinorV1;
        response.message_bytes =
            static_cast<std::uint32_t>(sizeof(response));
        response.request_id = request.request_id;

        OrderEventDeltaControlSnapshotV1 snapshot{};
        const bool snapshot_valid =
            TakeSnapshot(&snapshot, false);
        if (snapshot_valid) {
            SnapshotToResponse(snapshot, &response);
        }

        OrderEventDeltaControlStatusV1 status =
            OrderEventDeltaControlStatusV1::kOk;
        const OrderEventDeltaSourceSessionV1 requested_source_session =
            SourceSessionFromWire(
                request.expected_source_session);
        OrderEventDeltaSourceSessionV1 requested =
            requested_source_session;
        requested.temporal_coverage =
            static_cast<OrderEventDeltaTemporalCoverageV1>(
                request.expected_source_temporal_coverage);
        requested.stream_quality =
            static_cast<OrderEventDeltaStreamQualityV1>(
                request.expected_source_stream_quality);
        if (request.magic != kOrderEventDeltaControlMagicV1 ||
            request.opcode != static_cast<std::uint16_t>(
                                  OrderEventDeltaControlOpcodeV1::
                                      kGetSession) ||
            request.message_bytes != sizeof(request) ||
            request.flags != 0U || request.reserved0 != 0U ||
            request.request_id == 0U ||
            request.reserved != 0U) {
            status =
                OrderEventDeltaControlStatusV1::kInvalidRequest;
        } else if (
            request.protocol_major !=
                kOrderEventDeltaControlProtocolMajorV1 ||
            request.protocol_minor !=
                kOrderEventDeltaControlProtocolMinorV1) {
            status = OrderEventDeltaControlStatusV1::
                kUnsupportedVersion;
        } else if (!SourceSessionValid(requested)) {
            status =
                OrderEventDeltaControlStatusV1::kInvalidRequest;
        } else if (
            requested != config_.source_session) {
            status = OrderEventDeltaControlStatusV1::
                kSourceSessionMismatch;
        } else if (!snapshot_valid) {
            status =
                OrderEventDeltaControlStatusV1::kInternalError;
        } else {
            const std::uint32_t active =
                static_cast<std::uint32_t>(
                    OrderEventDeltaProducerStateV1::kActive);
            if (snapshot.event_producer_state != active ||
                snapshot.event_header_flags != 0U) {
                status =
                    OrderEventDeltaControlStatusV1::kUnavailable;
            }
        }

        bool attach_descriptor =
            status == OrderEventDeltaControlStatusV1::kOk;
        if (attach_descriptor) {
            // Re-read immediately before send. This cannot freeze a producer
            // transition after send, so the client repeats the ACTIVE check.
            OrderEventDeltaControlSnapshotV1 final_snapshot{};
            if (!TakeSnapshot(&final_snapshot, true)) {
                status =
                    OrderEventDeltaControlStatusV1::kUnavailable;
                attach_descriptor = false;
                if (TakeSnapshot(&final_snapshot, false)) {
                    snapshot = final_snapshot;
                    SnapshotToResponse(snapshot, &response);
                }
            } else {
                snapshot = final_snapshot;
                SnapshotToResponse(snapshot, &response);
            }
        }
        response.status = static_cast<std::uint16_t>(status);
        static_cast<void>(SendPacket(
            client,
            &response,
            sizeof(response),
            attach_descriptor ? ring_descriptor_ : -1,
            deadline,
            &ignored_error));
    }

    void ControlLoop() noexcept {
        std::array<pollfd, 2U> descriptors{};
        descriptors[0U].fd = listener_fd_;
        descriptors[0U].events = POLLIN;
        descriptors[1U].fd = stop_event_fd_;
        descriptors[1U].events = POLLIN;
        while (!stop_requested_.load(std::memory_order_acquire)) {
            int result = -1;
            do {
                result =
                    ::poll(descriptors.data(), descriptors.size(), -1);
            } while (result < 0 && errno == EINTR);
            if (result < 0) {
                ready_.store(false, std::memory_order_release);
                return;
            }
            if ((descriptors[1U].revents & POLLIN) != 0) {
                return;
            }
            if ((descriptors[0U].revents &
                 (POLLERR | POLLHUP | POLLNVAL)) != 0 ||
                (descriptors[1U].revents &
                 (POLLERR | POLLHUP | POLLNVAL)) != 0) {
                ready_.store(false, std::memory_order_release);
                return;
            }
            if ((descriptors[0U].revents & POLLIN) == 0) {
                continue;
            }
            for (;;) {
                const int client = ::accept4(
                    listener_fd_,
                    nullptr,
                    nullptr,
                    SOCK_NONBLOCK | SOCK_CLOEXEC);
                if (client < 0) {
                    if (errno == EINTR) {
                        continue;
                    }
                    if (errno == EAGAIN ||
                        errno == EWOULDBLOCK) {
                        break;
                    }
                    ready_.store(false, std::memory_order_release);
                    return;
                }
                if (PeerUidAllowed(client)) {
                    HandleClient(client);
                }
                static_cast<void>(::close(client));
            }
        }
    }

    OrderEventDeltaControlServerConfigV1 config_;
    OrderEventDeltaSessionV1 event_session_{};
    int ring_descriptor_ = -1;
    void* header_mapping_ = MAP_FAILED;
    const OrderEventDeltaHeaderV1* header_ = nullptr;
    int listener_fd_ = -1;
    int stop_event_fd_ = -1;
    dev_t socket_device_ = 0;
    ino_t socket_inode_ = 0;
    bool socket_bound_ = false;
    Lifecycle lifecycle_ = Lifecycle::kCreated;
    std::atomic<bool> stop_requested_{false};
    std::atomic<bool> ready_{false};
    std::thread control_thread_;
};

OrderEventDeltaControlServerV1::OrderEventDeltaControlServerV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

OrderEventDeltaControlServerV1::~OrderEventDeltaControlServerV1() =
    default;

OrderEventDeltaControlServerCreateErrorV1
OrderEventDeltaControlServerV1::Create(
    OrderEventDeltaControlServerConfigV1 config,
    std::unique_ptr<OrderEventDeltaControlServerV1>* output,
    int* system_error_number) noexcept {
    SetSystemError(system_error_number, 0);
    if (output == nullptr) {
        return OrderEventDeltaControlServerCreateErrorV1::
            kNullOutput;
    }
    output->reset();
    try {
        auto impl = std::make_unique<Impl>(std::move(config));
        const OrderEventDeltaControlServerCreateErrorV1 error =
            impl->Initialize(system_error_number);
        if (error !=
            OrderEventDeltaControlServerCreateErrorV1::kNone) {
            return error;
        }
        output->reset(new OrderEventDeltaControlServerV1(
            std::move(impl)));
        return OrderEventDeltaControlServerCreateErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return OrderEventDeltaControlServerCreateErrorV1::
            kResourceExhausted;
    } catch (...) {
        return OrderEventDeltaControlServerCreateErrorV1::
            kUnexpectedFailure;
    }
}

bool OrderEventDeltaControlServerV1::Start(
    int* system_error_number) noexcept {
    if (impl_ == nullptr) {
        SetSystemError(system_error_number, EINVAL);
        return false;
    }
    return impl_->Start(system_error_number);
}

void OrderEventDeltaControlServerV1::Stop() noexcept {
    if (impl_ != nullptr) {
        impl_->Stop();
    }
}

bool OrderEventDeltaControlServerV1::ready() const noexcept {
    return impl_ != nullptr && impl_->ready();
}

const std::filesystem::path&
OrderEventDeltaControlServerV1::control_socket_path()
    const noexcept {
    static const std::filesystem::path empty;
    return impl_ == nullptr ? empty : impl_->control_socket_path();
}

namespace {

[[nodiscard]] OrderEventDeltaControlClientErrorV1 ConnectSocket(
    const std::string& path,
    const IoDeadline& deadline,
    int* output,
    int* system_error_number) noexcept {
    *output = -1;
    struct stat socket_stat {};
    if (::lstat(path.c_str(), &socket_stat) != 0) {
        SetSystemError(system_error_number, errno);
        return OrderEventDeltaControlClientErrorV1::
            kSocketPathInvalid;
    }
    if (!S_ISSOCK(socket_stat.st_mode) ||
        socket_stat.st_uid != ::geteuid() ||
        (socket_stat.st_mode & 0077) != 0) {
        SetSystemError(system_error_number, EACCES);
        return OrderEventDeltaControlClientErrorV1::
            kSocketPathInvalid;
    }
    int socket_fd = ::socket(
        AF_UNIX,
        SOCK_SEQPACKET | SOCK_NONBLOCK | SOCK_CLOEXEC,
        0);
    if (socket_fd < 0) {
        SetSystemError(system_error_number, errno);
        return OrderEventDeltaControlClientErrorV1::
            kConnectFailed;
    }
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::memcpy(
        address.sun_path, path.c_str(), path.size() + 1U);
    int result = ::connect(
        socket_fd,
        reinterpret_cast<const sockaddr*>(&address),
        static_cast<socklen_t>(
            offsetof(sockaddr_un, sun_path) + path.size() + 1U));
    if (result != 0 &&
        (errno == EINPROGRESS || errno == EAGAIN ||
         errno == EWOULDBLOCK)) {
        const WaitResult ready = WaitForDescriptor(
            socket_fd, POLLOUT, deadline, system_error_number);
        if (ready != WaitResult::kReady) {
            CloseDescriptor(&socket_fd);
            return ready == WaitResult::kTimeout
                       ? OrderEventDeltaControlClientErrorV1::
                             kTimeout
                       : OrderEventDeltaControlClientErrorV1::
                             kConnectFailed;
        }
        int socket_error = 0;
        socklen_t error_bytes =
            static_cast<socklen_t>(sizeof(socket_error));
        if (::getsockopt(
                socket_fd,
                SOL_SOCKET,
                SO_ERROR,
                &socket_error,
                &error_bytes) != 0 ||
            error_bytes != sizeof(socket_error) ||
            socket_error != 0) {
            SetSystemError(
                system_error_number,
                socket_error != 0 ? socket_error : errno);
            CloseDescriptor(&socket_fd);
            return OrderEventDeltaControlClientErrorV1::
                kConnectFailed;
        }
    } else if (result != 0) {
        SetSystemError(system_error_number, errno);
        CloseDescriptor(&socket_fd);
        return OrderEventDeltaControlClientErrorV1::
            kConnectFailed;
    }

    ucred credentials{};
    socklen_t credential_bytes =
        static_cast<socklen_t>(sizeof(credentials));
    if (::getsockopt(
            socket_fd,
            SOL_SOCKET,
            SO_PEERCRED,
            &credentials,
            &credential_bytes) != 0 ||
        credential_bytes != sizeof(credentials) ||
        credentials.uid != ::geteuid()) {
        SetSystemError(
            system_error_number,
            errno == 0 ? EACCES : errno);
        CloseDescriptor(&socket_fd);
        return OrderEventDeltaControlClientErrorV1::
            kPeerCredentialRejected;
    }
    *output = socket_fd;
    return OrderEventDeltaControlClientErrorV1::kNone;
}

[[nodiscard]] bool ResponseEnvelopeValid(
    const OrderEventDeltaControlGetSessionResponseV1& response,
    std::uint64_t request_id) noexcept {
    return response.magic == kOrderEventDeltaControlMagicV1 &&
           response.protocol_major ==
               kOrderEventDeltaControlProtocolMajorV1 &&
           response.protocol_minor ==
               kOrderEventDeltaControlProtocolMinorV1 &&
           response.flags == 0U &&
           response.message_bytes == sizeof(response) &&
           response.reserved0 == 0U &&
           response.request_id == request_id &&
           response.reserved_event == 0U &&
           ObjectBytesZero(response.reserved);
}

[[nodiscard]] bool SuccessfulSnapshotValid(
    const OrderEventDeltaControlSnapshotV1& snapshot,
    const OrderEventDeltaSourceSessionV1& expected) noexcept {
    return SourceSessionValid(snapshot.source_session) &&
           snapshot.source_session == expected &&
           EventSessionValid(snapshot.event_session) &&
           snapshot.event_session.trade_date ==
               snapshot.source_session.trade_date &&
           snapshot.event_session.temporal_coverage ==
               snapshot.source_session.temporal_coverage &&
           snapshot.event_session.stream_quality ==
               snapshot.source_session.stream_quality &&
           snapshot.event_producer_state ==
               static_cast<std::uint32_t>(
                   OrderEventDeltaProducerStateV1::kActive) &&
           snapshot.event_header_flags == 0U;
}

}  // namespace

OrderEventDeltaControlClientErrorV1
OrderEventDeltaControlGetSessionV1(
    const OrderEventDeltaControlClientConfigV1& config,
    OrderEventDeltaControlSnapshotV1* output_snapshot,
    int* output_read_only_descriptor,
    int* system_error_number) noexcept {
    SetSystemError(system_error_number, 0);
    if (output_snapshot == nullptr ||
        output_read_only_descriptor == nullptr) {
        return OrderEventDeltaControlClientErrorV1::
            kInvalidArgument;
    }
    *output_snapshot = {};
    *output_read_only_descriptor = -1;
    try {
        std::string socket_path;
        if constexpr (std::endian::native != std::endian::little) {
            return OrderEventDeltaControlClientErrorV1::
                kInvalidArgument;
        }
        if (!SourceSessionValid(
                config.expected_source_session) ||
            !TimeoutValid(config.timeout) ||
            !AbsoluteSocketPath(
                config.control_socket_path, &socket_path)) {
            return OrderEventDeltaControlClientErrorV1::
                kInvalidArgument;
        }
        const IoDeadline deadline = MakeDeadline(config.timeout);
        int socket_fd = -1;
        const OrderEventDeltaControlClientErrorV1 connect_error =
            ConnectSocket(
                socket_path,
                deadline,
                &socket_fd,
                system_error_number);
        if (connect_error !=
            OrderEventDeltaControlClientErrorV1::kNone) {
            return connect_error;
        }

        OrderEventDeltaControlGetSessionRequestV1 request{};
        request.magic = kOrderEventDeltaControlMagicV1;
        request.protocol_major =
            kOrderEventDeltaControlProtocolMajorV1;
        request.protocol_minor =
            kOrderEventDeltaControlProtocolMinorV1;
        request.opcode = static_cast<std::uint16_t>(
            OrderEventDeltaControlOpcodeV1::kGetSession);
        request.message_bytes =
            static_cast<std::uint32_t>(sizeof(request));
        request.request_id = NextRequestId();
        SourceSessionToWire(
            config.expected_source_session,
            &request.expected_source_session);
        request.expected_source_temporal_coverage =
            static_cast<std::uint32_t>(
                config.expected_source_session.temporal_coverage);
        request.expected_source_stream_quality =
            static_cast<std::uint32_t>(
                config.expected_source_session.stream_quality);

        IoResult io = SendPacket(
            socket_fd,
            &request,
            sizeof(request),
            -1,
            deadline,
            system_error_number);
        if (io != IoResult::kOk) {
            CloseDescriptor(&socket_fd);
            return io == IoResult::kTimeout
                       ? OrderEventDeltaControlClientErrorV1::
                             kTimeout
                       : OrderEventDeltaControlClientErrorV1::
                             kTransportFailed;
        }

        OrderEventDeltaControlGetSessionResponseV1 response{};
        int ring_descriptor = -1;
        std::size_t descriptor_count = 0U;
        io = ReceivePacket(
            socket_fd,
            &response,
            &ring_descriptor,
            &descriptor_count,
            deadline,
            system_error_number);
        CloseDescriptor(&socket_fd);
        if (io != IoResult::kOk) {
            CloseDescriptor(&ring_descriptor);
            return io == IoResult::kTimeout
                       ? OrderEventDeltaControlClientErrorV1::
                             kTimeout
                       : io == IoResult::kProtocol
                             ? OrderEventDeltaControlClientErrorV1::
                                   kProtocolError
                             : OrderEventDeltaControlClientErrorV1::
                                   kTransportFailed;
        }
        if (!ResponseEnvelopeValid(
                response, request.request_id)) {
            CloseDescriptor(&ring_descriptor);
            return OrderEventDeltaControlClientErrorV1::
                kProtocolError;
        }

        const auto status =
            static_cast<OrderEventDeltaControlStatusV1>(
                response.status);
        if (status != OrderEventDeltaControlStatusV1::kOk) {
            if (descriptor_count != 0U) {
                CloseDescriptor(&ring_descriptor);
                return OrderEventDeltaControlClientErrorV1::
                    kProtocolError;
            }
            switch (status) {
                case OrderEventDeltaControlStatusV1::
                    kUnsupportedVersion:
                    return OrderEventDeltaControlClientErrorV1::
                        kUnsupportedVersion;
                case OrderEventDeltaControlStatusV1::
                    kUnavailable:
                case OrderEventDeltaControlStatusV1::
                    kInternalError:
                    return OrderEventDeltaControlClientErrorV1::
                        kUnavailable;
                case OrderEventDeltaControlStatusV1::
                    kSourceSessionMismatch:
                    return OrderEventDeltaControlClientErrorV1::
                        kSourceSessionMismatch;
                case OrderEventDeltaControlStatusV1::
                    kInvalidRequest:
                case OrderEventDeltaControlStatusV1::kOk:
                    return OrderEventDeltaControlClientErrorV1::
                        kProtocolError;
            }
            return OrderEventDeltaControlClientErrorV1::
                kProtocolError;
        }
        if (descriptor_count != 1U || ring_descriptor < 0) {
            CloseDescriptor(&ring_descriptor);
            return OrderEventDeltaControlClientErrorV1::
                kProtocolError;
        }
        const OrderEventDeltaControlSnapshotV1 snapshot =
            SnapshotFromResponse(response);
        if (!SuccessfulSnapshotValid(
                snapshot,
                config.expected_source_session)) {
            CloseDescriptor(&ring_descriptor);
            return snapshot.source_session !=
                           config.expected_source_session
                       ? OrderEventDeltaControlClientErrorV1::
                             kSourceSessionMismatch
                       : OrderEventDeltaControlClientErrorV1::
                             kProtocolError;
        }
        if (!BasicRingDescriptorValid(
                ring_descriptor,
                snapshot.event_session,
                system_error_number)) {
            CloseDescriptor(&ring_descriptor);
            return OrderEventDeltaControlClientErrorV1::
                kRingDescriptorInvalid;
        }
        *output_snapshot = snapshot;
        *output_read_only_descriptor = ring_descriptor;
        return OrderEventDeltaControlClientErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return OrderEventDeltaControlClientErrorV1::
            kResourceExhausted;
    } catch (...) {
        return OrderEventDeltaControlClientErrorV1::
            kUnexpectedFailure;
    }
}

OrderEventDeltaControlClientErrorV1
OrderEventDeltaControlProbeV1(
    const OrderEventDeltaControlClientConfigV1& config,
    OrderEventDeltaControlSnapshotV1* output_snapshot,
    int* system_error_number) noexcept {
    if (output_snapshot == nullptr) {
        SetSystemError(system_error_number, 0);
        return OrderEventDeltaControlClientErrorV1::
            kInvalidArgument;
    }
    int descriptor = -1;
    const OrderEventDeltaControlClientErrorV1 error =
        OrderEventDeltaControlGetSessionV1(
            config,
            output_snapshot,
            &descriptor,
            system_error_number);
    CloseDescriptor(&descriptor);
    return error;
}

OrderEventDeltaControlClientErrorV1
OrderEventDeltaControlConnectV1(
    const OrderEventDeltaControlClientConfigV1& config,
    OrderEventDeltaControlSnapshotV1* output_snapshot,
    std::unique_ptr<OrderEventDeltaRingReaderV1>* output_reader,
    int* system_error_number) noexcept {
    return OrderEventDeltaControlConnectAtV1(
        config,
        1U,
        output_snapshot,
        output_reader,
        system_error_number);
}

OrderEventDeltaControlClientErrorV1
OrderEventDeltaControlConnectAtV1(
    const OrderEventDeltaControlClientConfigV1& config,
    std::uint64_t start_event_sequence,
    OrderEventDeltaControlSnapshotV1* output_snapshot,
    std::unique_ptr<OrderEventDeltaRingReaderV1>* output_reader,
    int* system_error_number) noexcept {
    SetSystemError(system_error_number, 0);
    if (start_event_sequence == 0U || output_snapshot == nullptr ||
        output_reader == nullptr) {
        return OrderEventDeltaControlClientErrorV1::
            kInvalidArgument;
    }
    *output_snapshot = {};
    output_reader->reset();
    int descriptor = -1;
    OrderEventDeltaControlSnapshotV1 snapshot{};
    const OrderEventDeltaControlClientErrorV1 control_error =
        OrderEventDeltaControlGetSessionV1(
            config,
            &snapshot,
            &descriptor,
            system_error_number);
    if (control_error !=
        OrderEventDeltaControlClientErrorV1::kNone) {
        return control_error;
    }
    std::unique_ptr<OrderEventDeltaRingReaderV1> reader;
    const OrderEventDeltaReaderOpenErrorV1 open_error =
        OrderEventDeltaRingReaderV1::OpenAt(
            descriptor,
            snapshot.event_session,
            start_event_sequence,
            &reader,
            system_error_number);
    CloseDescriptor(&descriptor);
    if (open_error != OrderEventDeltaReaderOpenErrorV1::kNone ||
        reader == nullptr) {
        return open_error ==
                       OrderEventDeltaReaderOpenErrorV1::kUnavailable
                   ? OrderEventDeltaControlClientErrorV1::
                         kUnavailable
                   : OrderEventDeltaControlClientErrorV1::
                         kRingDescriptorInvalid;
    }
    if (reader->session() != snapshot.event_session ||
        reader->state() !=
            OrderEventDeltaProducerStateV1::kActive) {
        return OrderEventDeltaControlClientErrorV1::kUnavailable;
    }
    *output_snapshot = snapshot;
    *output_reader = std::move(reader);
    return OrderEventDeltaControlClientErrorV1::kNone;
}

}  // namespace l2flow::ipc
