#include "l2flow/ipc/partial_order_event_control_v2.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <utility>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <sys/un.h>
#include <unistd.h>

#ifndef F_SEAL_FUTURE_WRITE
#define F_SEAL_FUTURE_WRITE 0x0010
#endif

namespace l2flow::ipc {
namespace {

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

template <typename Value, std::size_t Size>
[[nodiscard]] bool AllZero(
    const std::array<Value, Size>& values) noexcept {
    return std::all_of(
        values.begin(), values.end(), [](Value value) noexcept {
            return value == Value{};
        });
}

[[nodiscard]] std::array<std::uint8_t, 16U> WireIdentity(
    const common::Identity128& identity) noexcept {
    std::array<std::uint8_t, 16U> result{};
    for (std::size_t index = 0U; index < result.size(); ++index) {
        result[index] = std::to_integer<std::uint8_t>(identity[index]);
    }
    return result;
}

[[nodiscard]] common::Identity128 NativeIdentity(
    const std::array<std::uint8_t, 16U>& identity) noexcept {
    common::Identity128 result{};
    for (std::size_t index = 0U; index < result.size(); ++index) {
        result[index] = static_cast<std::byte>(identity[index]);
    }
    return result;
}

[[nodiscard]] bool ValidTradeDate(std::uint32_t value) noexcept {
    const std::uint32_t year = value / 10'000U;
    const std::uint32_t month = (value / 100U) % 100U;
    const std::uint32_t day = value % 100U;
    if (year < 1992U || year > 2200U || month == 0U ||
        month > 12U || day == 0U) {
        return false;
    }
    constexpr std::array<std::uint32_t, 12U> days{
        31U, 28U, 31U, 30U, 31U, 30U,
        31U, 31U, 30U, 31U, 30U, 31U};
    std::uint32_t maximum = days[month - 1U];
    const bool leap =
        (year % 4U == 0U && year % 100U != 0U) ||
        year % 400U == 0U;
    if (month == 2U && leap) {
        maximum = 29U;
    }
    return day <= maximum;
}

[[nodiscard]] bool ValidTimeout(
    std::chrono::milliseconds timeout) noexcept {
    return timeout > std::chrono::milliseconds::zero() &&
           timeout <= std::chrono::seconds(60);
}

template <typename Value>
[[nodiscard]] std::atomic_ref<Value> Atomic(Value& value) noexcept {
    return std::atomic_ref<Value>(value);
}

template <typename Value>
[[nodiscard]] std::atomic_ref<Value> Atomic(const Value& value) noexcept {
    return std::atomic_ref<Value>(const_cast<Value&>(value));
}

static_assert(std::atomic_ref<std::uint32_t>::is_always_lock_free);
static_assert(std::atomic_ref<std::uint64_t>::is_always_lock_free);
static_assert(std::atomic_ref<std::int64_t>::is_always_lock_free);

[[nodiscard]] bool ReadMonotonicNs(std::uint64_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    timespec value{};
    if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0 ||
        value.tv_sec < 0 || value.tv_nsec < 0 ||
        value.tv_nsec >= 1'000'000'000L) {
        return false;
    }
    const auto seconds = static_cast<std::uint64_t>(value.tv_sec);
    const auto nanoseconds = static_cast<std::uint64_t>(value.tv_nsec);
    if (seconds >
        (std::numeric_limits<std::uint64_t>::max() - nanoseconds) /
            1'000'000'000ULL) {
        return false;
    }
    *output = seconds * 1'000'000'000ULL + nanoseconds;
    return true;
}

[[nodiscard]] bool ValidLifecycleHeartbeatTimeout(
    std::uint64_t value) noexcept {
    constexpr std::uint64_t minimum = 1'000'000ULL;
    constexpr std::uint64_t maximum = 300'000'000'000ULL;
    return value >= minimum && value <= maximum;
}

[[nodiscard]] bool LifecyclePageImmutableCanonical(
    const PartialEventBrokerLifecyclePageV2& page,
    const common::Identity128& expected_run_id,
    std::uint64_t expected_session_epoch,
    std::uint32_t expected_trade_date) noexcept {
    return page.magic == kPartialEventBrokerLifecycleMagicV2 &&
           page.protocol_major == kPartialEventBrokerProtocolMajorV2 &&
           page.protocol_minor == kPartialEventBrokerProtocolMinorV2 &&
           page.page_bytes == sizeof(page) && page.reserved0 == 0U &&
           NativeIdentity(page.run_id) == expected_run_id &&
           page.session_epoch == expected_session_epoch &&
           page.trade_date == expected_trade_date && page.broker_pid > 0;
}

[[nodiscard]] bool LifecycleStateCanonical(
    PartialEventBrokerStateV2 state,
    std::uint32_t stale) noexcept {
    if (state < PartialEventBrokerStateV2::kUnavailable ||
        state > PartialEventBrokerStateV2::kStoppedClean || stale > 1U) {
        return false;
    }
    const bool expected_stale =
        state != PartialEventBrokerStateV2::kReady &&
        state != PartialEventBrokerStateV2::kStoppedClean;
    return (stale != 0U) == expected_stale;
}

[[nodiscard]] bool SocketAddress(
    const std::filesystem::path& path,
    sockaddr_un* output,
    socklen_t* output_size) {
    if (output == nullptr || output_size == nullptr || path.empty() ||
        !path.is_absolute()) {
        return false;
    }
    const std::string text = path.string();
    if (text.empty() || text.size() >= sizeof(output->sun_path)) {
        return false;
    }
    *output = {};
    output->sun_family = AF_UNIX;
    std::memcpy(output->sun_path, text.c_str(), text.size() + 1U);
    const std::size_t size =
        offsetof(sockaddr_un, sun_path) + text.size() + 1U;
    if (size >
        static_cast<std::size_t>(
            std::numeric_limits<socklen_t>::max())) {
        return false;
    }
    *output_size = static_cast<socklen_t>(size);
    return true;
}

[[nodiscard]] bool TrustedSocketPath(
    const std::filesystem::path& path,
    struct stat* output,
    int* system_error_number) noexcept {
    if (output == nullptr) {
        if (system_error_number != nullptr) {
            *system_error_number = EINVAL;
        }
        return false;
    }
    *output = {};
    if (::lstat(path.c_str(), output) != 0) {
        if (system_error_number != nullptr) {
            *system_error_number = errno;
        }
        return false;
    }
    if (!S_ISSOCK(output->st_mode) ||
        output->st_uid != ::geteuid() ||
        (output->st_mode & 0077) != 0) {
        if (system_error_number != nullptr) {
            *system_error_number = EACCES;
        }
        return false;
    }
    return true;
}

void SetSocketTimeout(
    int descriptor,
    std::chrono::milliseconds timeout) noexcept {
    const auto seconds =
        std::chrono::duration_cast<std::chrono::seconds>(timeout);
    const auto remainder = timeout - seconds;
    timeval value{};
    value.tv_sec = static_cast<time_t>(seconds.count());
    value.tv_usec = static_cast<suseconds_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(remainder)
            .count());
    static_cast<void>(::setsockopt(
        descriptor, SOL_SOCKET, SO_RCVTIMEO, &value, sizeof(value)));
    static_cast<void>(::setsockopt(
        descriptor, SOL_SOCKET, SO_SNDTIMEO, &value, sizeof(value)));
}

[[nodiscard]] UniqueFd Connect(
    const std::filesystem::path& path,
    std::chrono::milliseconds timeout,
    bool* peer_rejected,
    pid_t* connected_peer,
    int* system_error_number) {
    if (peer_rejected != nullptr) {
        *peer_rejected = false;
    }
    if (connected_peer != nullptr) {
        *connected_peer = -1;
    }
    sockaddr_un address{};
    socklen_t address_size = 0;
    if (!SocketAddress(path, &address, &address_size)) {
        if (system_error_number != nullptr) {
            *system_error_number = EINVAL;
        }
        return {};
    }
    struct stat path_before {};
    int path_error = 0;
    if (!TrustedSocketPath(
            path, &path_before, &path_error)) {
        if (system_error_number != nullptr) {
            *system_error_number = path_error;
        }
        if (peer_rejected != nullptr && path_error == EACCES) {
            *peer_rejected = true;
        }
        return {};
    }
    UniqueFd descriptor(
        ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0));
    if (descriptor.get() < 0) {
        if (system_error_number != nullptr) {
            *system_error_number = errno;
        }
        return {};
    }
    SetSocketTimeout(descriptor.get(), timeout);
    if (::connect(
            descriptor.get(),
            reinterpret_cast<const sockaddr*>(&address),
            address_size) != 0) {
        if (system_error_number != nullptr) {
            *system_error_number = errno;
        }
        return {};
    }
    struct stat path_after {};
    ucred credentials{};
    socklen_t credential_bytes =
        static_cast<socklen_t>(sizeof(credentials));
    path_error = 0;
    const bool path_trusted = TrustedSocketPath(
        path, &path_after, &path_error);
    const bool credentials_read =
        ::getsockopt(
            descriptor.get(),
            SOL_SOCKET,
            SO_PEERCRED,
            &credentials,
            &credential_bytes) == 0;
    if (!path_trusted ||
        path_after.st_dev != path_before.st_dev ||
        path_after.st_ino != path_before.st_ino ||
        !credentials_read ||
        credential_bytes !=
            static_cast<socklen_t>(sizeof(credentials)) ||
        credentials.pid <= 0 || credentials.uid != ::geteuid()) {
        if (system_error_number != nullptr) {
            *system_error_number =
                !path_trusted && path_error != 0
                    ? path_error
                    : EACCES;
        }
        if (peer_rejected != nullptr) {
            *peer_rejected = true;
        }
        return {};
    }
    if (connected_peer != nullptr) {
        *connected_peer = credentials.pid;
    }
    return descriptor;
}

[[nodiscard]] bool BasicDescriptorValid(
    int descriptor,
    struct stat* descriptor_status,
    PartialOrderEventHeaderV2* header,
    int* system_error_number) noexcept {
    if (descriptor < 0 || descriptor_status == nullptr ||
        header == nullptr) {
        if (system_error_number != nullptr) {
            *system_error_number = EINVAL;
        }
        return false;
    }
    *descriptor_status = {};
    *header = {};
    const int access_flags = ::fcntl(descriptor, F_GETFL);
    const int descriptor_flags = ::fcntl(descriptor, F_GETFD);
    const int seals = ::fcntl(descriptor, F_GET_SEALS);
    constexpr int required_seals =
        F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_FUTURE_WRITE |
        F_SEAL_SEAL;
    if (access_flags < 0 || descriptor_flags < 0 || seals < 0 ||
        ::fstat(descriptor, descriptor_status) != 0) {
        if (system_error_number != nullptr) {
            *system_error_number = errno;
        }
        return false;
    }
    if ((access_flags & O_ACCMODE) != O_RDONLY ||
        (descriptor_flags & FD_CLOEXEC) == 0 ||
        (seals & required_seals) != required_seals ||
        !S_ISREG(descriptor_status->st_mode) ||
        descriptor_status->st_size <
            static_cast<off_t>(sizeof(*header))) {
        if (system_error_number != nullptr) {
            *system_error_number = EPROTO;
        }
        return false;
    }
    ssize_t bytes = -1;
    do {
        bytes = ::pread(descriptor, header, sizeof(*header), 0);
    } while (bytes < 0 && errno == EINTR);
    if (bytes != static_cast<ssize_t>(sizeof(*header)) ||
        !PartialOrderEventHeaderLayoutCanonicalV2(*header) ||
        header->total_mapping_bytes !=
            static_cast<std::uint64_t>(descriptor_status->st_size)) {
        if (system_error_number != nullptr) {
            *system_error_number = bytes < 0 ? errno : EPROTO;
        }
        return false;
    }
    return true;
}

[[nodiscard]] bool LifecycleDescriptorValid(
    int descriptor,
    const common::Identity128& expected_run_id,
    std::uint64_t expected_session_epoch,
    std::uint32_t expected_trade_date,
    PartialEventBrokerLifecyclePageV2* page,
    int* system_error_number) noexcept {
    if (descriptor < 0 || page == nullptr) {
        if (system_error_number != nullptr) {
            *system_error_number = EINVAL;
        }
        return false;
    }
    *page = {};
    struct stat descriptor_status {};
    const int access_flags = ::fcntl(descriptor, F_GETFL);
    const int descriptor_flags = ::fcntl(descriptor, F_GETFD);
    const int seals = ::fcntl(descriptor, F_GET_SEALS);
    constexpr int required_seals =
        F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_FUTURE_WRITE |
        F_SEAL_SEAL;
    if (access_flags < 0 || descriptor_flags < 0 || seals < 0 ||
        ::fstat(descriptor, &descriptor_status) != 0) {
        if (system_error_number != nullptr) {
            *system_error_number = errno;
        }
        return false;
    }
    if ((access_flags & O_ACCMODE) != O_RDONLY ||
        (descriptor_flags & FD_CLOEXEC) == 0 ||
        (seals & required_seals) != required_seals ||
        !S_ISREG(descriptor_status.st_mode) ||
        descriptor_status.st_size != static_cast<off_t>(sizeof(*page))) {
        if (system_error_number != nullptr) {
            *system_error_number = EPROTO;
        }
        return false;
    }
    ssize_t bytes = -1;
    do {
        bytes = ::pread(descriptor, page, sizeof(*page), 0);
    } while (bytes < 0 && errno == EINTR);
    if (bytes != static_cast<ssize_t>(sizeof(*page)) ||
        !LifecyclePageImmutableCanonical(
            *page,
            expected_run_id,
            expected_session_epoch,
            expected_trade_date)) {
        if (system_error_number != nullptr) {
            *system_error_number = bytes < 0 ? errno : EPROTO;
        }
        return false;
    }
    return true;
}

[[nodiscard]] bool DescriptorMatchesSession(
    int descriptor,
    const PartialOrderEventJournalSessionV2& session,
    int* system_error_number) noexcept {
    struct stat descriptor_status {};
    PartialOrderEventHeaderV2 header{};
    if (!BasicDescriptorValid(
            descriptor,
            &descriptor_status,
            &header,
            system_error_number)) {
        return false;
    }
    if (NativeIdentity(header.run_id) != session.run_id ||
        header.session_epoch != session.session_epoch ||
        header.trade_date != session.trade_date ||
        header.publication_generation !=
            session.publication_generation ||
        header.correction_epoch != session.correction_epoch ||
        header.coverage_start_unix_ns !=
            session.coverage_start_unix_ns ||
        header.ordering_quality != session.ordering_quality ||
        header.event_capacity != session.event_capacity ||
        header.channel_capacity !=
            session.affected_channel_capacity ||
        header.order_state_capacity != session.order_state_capacity ||
        header.total_mapping_bytes != session.total_mapping_bytes) {
        if (system_error_number != nullptr) {
            *system_error_number = EPROTO;
        }
        return false;
    }
    return true;
}

[[nodiscard]] bool DescriptorMatchesResponse(
    int descriptor,
    const PartialEventBrokerResponseV2& response,
    int* system_error_number) noexcept {
    struct stat descriptor_status {};
    PartialOrderEventHeaderV2 header{};
    if (!BasicDescriptorValid(
            descriptor,
            &descriptor_status,
            &header,
            system_error_number)) {
        return false;
    }
    if (NativeIdentity(header.run_id) !=
            NativeIdentity(response.run_id) ||
        header.session_epoch != response.session_epoch ||
        header.trade_date != response.trade_date ||
        header.publication_generation !=
            response.publication_generation ||
        header.correction_epoch != response.correction_epoch ||
        header.ordering_quality != response.ordering_quality) {
        if (system_error_number != nullptr) {
            *system_error_number = EPROTO;
        }
        return false;
    }
    return true;
}

template <typename Packet>
[[nodiscard]] bool SendPacket(
    int socket_descriptor,
    const Packet& packet,
    int passed_descriptor = -1,
    int second_passed_descriptor = -1) noexcept {
    iovec vector{};
    vector.iov_base = const_cast<Packet*>(&packet);
    vector.iov_len = sizeof(packet);
    std::array<std::byte, CMSG_SPACE(sizeof(int) * 2U)> control{};
    msghdr message{};
    message.msg_iov = &vector;
    message.msg_iovlen = 1U;
    std::array<int, 2U> descriptors{};
    std::size_t descriptor_count = 0U;
    if (passed_descriptor >= 0) {
        descriptors[descriptor_count++] = passed_descriptor;
    }
    if (second_passed_descriptor >= 0) {
        descriptors[descriptor_count++] = second_passed_descriptor;
    }
    if (descriptor_count != 0U) {
        message.msg_control = control.data();
        message.msg_controllen = control.size();
        cmsghdr* header = CMSG_FIRSTHDR(&message);
        if (header == nullptr) {
            return false;
        }
        header->cmsg_level = SOL_SOCKET;
        header->cmsg_type = SCM_RIGHTS;
        header->cmsg_len = CMSG_LEN(sizeof(int) * descriptor_count);
        std::memcpy(
            CMSG_DATA(header),
            descriptors.data(),
            sizeof(int) * descriptor_count);
    }
    const ssize_t sent = ::sendmsg(socket_descriptor, &message, MSG_NOSIGNAL);
    return sent == static_cast<ssize_t>(sizeof(packet));
}

template <typename Packet>
[[nodiscard]] bool ReceivePacket(
    int socket_descriptor,
    Packet* packet,
    int expected_descriptor_count,
    UniqueFd* received_descriptor,
    UniqueFd* second_received_descriptor = nullptr) noexcept {
    if (packet == nullptr || received_descriptor == nullptr) {
        return false;
    }
    *packet = {};
    received_descriptor->Reset();
    if (second_received_descriptor != nullptr) {
        second_received_descriptor->Reset();
    }
    iovec vector{};
    vector.iov_base = packet;
    vector.iov_len = sizeof(*packet);
    std::array<std::byte, CMSG_SPACE(sizeof(int) * 2U)> control{};
    msghdr message{};
    message.msg_iov = &vector;
    message.msg_iovlen = 1U;
    message.msg_control = control.data();
    message.msg_controllen = control.size();
    const ssize_t received =
        ::recvmsg(socket_descriptor, &message, MSG_CMSG_CLOEXEC);
    const bool payload_valid =
        received == static_cast<ssize_t>(sizeof(*packet)) &&
        (message.msg_flags & (MSG_TRUNC | MSG_CTRUNC)) == 0;
    bool ancillary_valid = true;
    std::size_t descriptor_count = 0U;
    for (cmsghdr* header = CMSG_FIRSTHDR(&message);
         header != nullptr;
         header = CMSG_NXTHDR(&message, header)) {
        if (header->cmsg_level != SOL_SOCKET ||
            header->cmsg_type != SCM_RIGHTS) {
            ancillary_valid = false;
            continue;
        }
        const std::size_t header_bytes = CMSG_LEN(0U);
        if (header->cmsg_len < header_bytes) {
            ancillary_valid = false;
            continue;
        }
        const std::size_t payload_bytes =
            header->cmsg_len - header_bytes;
        if (payload_bytes == 0U ||
            payload_bytes % sizeof(int) != 0U) {
            ancillary_valid = false;
            continue;
        }
        const std::size_t count = payload_bytes / sizeof(int);
        for (std::size_t index = 0U; index < count; ++index) {
            int descriptor = -1;
            std::memcpy(
                &descriptor,
                CMSG_DATA(header) + index * sizeof(int),
                sizeof(descriptor));
            ++descriptor_count;
            if (descriptor_count == 1U) {
                received_descriptor->Reset(descriptor);
            } else if (
                descriptor_count == 2U &&
                second_received_descriptor != nullptr) {
                second_received_descriptor->Reset(descriptor);
            } else if (descriptor >= 0) {
                static_cast<void>(::close(descriptor));
            }
        }
    }
    if (!payload_valid || !ancillary_valid) {
        return false;
    }
    return expected_descriptor_count < 0
               ? descriptor_count <= 2U
               : descriptor_count ==
                     static_cast<std::size_t>(
                         expected_descriptor_count);
}

[[nodiscard]] bool PublicResponseCanonical(
    const PartialEventBrokerResponseV2& response) noexcept {
    return response.magic == kPartialEventBrokerResponseMagicV2 &&
           response.protocol_major ==
               kPartialEventBrokerProtocolMajorV2 &&
           response.protocol_minor ==
               kPartialEventBrokerProtocolMinorV2 &&
           response.response_bytes == sizeof(response) &&
           response.result >= PartialEventBrokerResultV2::kOk &&
           response.result <=
               PartialEventBrokerResultV2::kInternalError &&
           response.broker_state >=
               PartialEventBrokerStateV2::kUnavailable &&
           response.broker_state <=
               PartialEventBrokerStateV2::kStoppedClean &&
           response.broker_stale <= 1U &&
           response.descriptor_count <= 2U &&
           AllZero(response.reserved);
}

[[nodiscard]] bool HandoffResponseCanonical(
    const PartialEventHandoffResponseV2& response) noexcept {
    return response.magic == kPartialEventHandoffResponseMagicV2 &&
           response.protocol_major ==
               kPartialEventBrokerProtocolMajorV2 &&
           response.protocol_minor ==
               kPartialEventBrokerProtocolMinorV2 &&
           response.response_bytes == sizeof(response) &&
           response.result >= PartialEventBrokerResultV2::kOk &&
           response.result <=
               PartialEventBrokerResultV2::kInternalError &&
           response.reserved0 == 0U && AllZero(response.reserved);
}

[[nodiscard]] PartialEventHandoffRequestV2 HandoffRequest(
    const PartialOrderEventJournalSessionV2& session) noexcept {
    PartialEventHandoffRequestV2 result{};
    result.run_id = WireIdentity(session.run_id);
    result.session_epoch = session.session_epoch;
    result.publication_generation = session.publication_generation;
    result.correction_epoch = session.correction_epoch;
    result.coverage_start_unix_ns = session.coverage_start_unix_ns;
    result.event_capacity = session.event_capacity;
    result.order_state_capacity = session.order_state_capacity;
    result.trade_date = session.trade_date;
    result.ordering_quality = session.ordering_quality;
    result.affected_channel_capacity =
        session.affected_channel_capacity;
    result.total_mapping_bytes = session.total_mapping_bytes;
    return result;
}

}  // namespace

class PartialEventBrokerLifecycleReaderV2::Impl final {
public:
    Impl() noexcept = default;
    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;

    ~Impl() {
        if (mapping_ != MAP_FAILED) {
            static_cast<void>(::munmap(mapping_, mapping_bytes_));
        }
        if (descriptor_ >= 0) {
            static_cast<void>(::close(descriptor_));
        }
    }

    [[nodiscard]] PartialEventBrokerLifecycleOpenErrorV2 Open(
        int source_descriptor,
        const common::Identity128& expected_run_id,
        std::uint64_t expected_session_epoch,
        std::uint32_t expected_trade_date,
        int* system_error_number) noexcept {
        if (source_descriptor < 0 ||
            common::IsZeroIdentity(expected_run_id) ||
            expected_session_epoch == 0U ||
            !ValidTradeDate(expected_trade_date)) {
            return PartialEventBrokerLifecycleOpenErrorV2::kInvalidDescriptor;
        }
        do {
            descriptor_ =
                ::fcntl(source_descriptor, F_DUPFD_CLOEXEC, 0);
        } while (descriptor_ < 0 && errno == EINTR);
        if (descriptor_ < 0) {
            if (system_error_number != nullptr) {
                *system_error_number = errno;
            }
            return PartialEventBrokerLifecycleOpenErrorV2::kInvalidDescriptor;
        }

        struct stat descriptor_status {};
        if (::fstat(descriptor_, &descriptor_status) != 0) {
            if (system_error_number != nullptr) {
                *system_error_number = errno;
            }
            return PartialEventBrokerLifecycleOpenErrorV2::
                kDescriptorStatFailed;
        }
        const int access_flags = ::fcntl(descriptor_, F_GETFL);
        if (!S_ISREG(descriptor_status.st_mode) || access_flags < 0 ||
            (access_flags & O_ACCMODE) != O_RDONLY ||
            descriptor_status.st_size !=
                static_cast<off_t>(
                    kPartialEventBrokerLifecycleBytesV2)) {
            if (system_error_number != nullptr) {
                *system_error_number = access_flags < 0 ? errno : EPROTO;
            }
            return PartialEventBrokerLifecycleOpenErrorV2::kInvalidDescriptor;
        }
        constexpr int required_seals =
            F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_FUTURE_WRITE |
            F_SEAL_SEAL;
        const int actual_seals = ::fcntl(descriptor_, F_GET_SEALS);
        if (actual_seals < 0 ||
            (actual_seals & required_seals) != required_seals) {
            if (system_error_number != nullptr) {
                *system_error_number = actual_seals < 0 ? errno : EPROTO;
            }
            return PartialEventBrokerLifecycleOpenErrorV2::
                kDescriptorSealMismatch;
        }

        mapping_bytes_ = kPartialEventBrokerLifecycleBytesV2;
        mapping_ = ::mmap(
            nullptr,
            mapping_bytes_,
            PROT_READ,
            MAP_SHARED,
            descriptor_,
            0);
        if (mapping_ == MAP_FAILED) {
            if (system_error_number != nullptr) {
                *system_error_number = errno;
            }
            return PartialEventBrokerLifecycleOpenErrorV2::kMappingFailed;
        }
        page_ = static_cast<const PartialEventBrokerLifecyclePageV2*>(mapping_);
        if (!LifecyclePageImmutableCanonical(
                *page_,
                expected_run_id,
                expected_session_epoch,
                expected_trade_date)) {
            return PartialEventBrokerLifecycleOpenErrorV2::
                kSessionMismatch;
        }
        PartialEventBrokerLifecycleSnapshotV2 snapshot{};
        if (ReadSnapshot(&snapshot, true) !=
            PartialEventBrokerLifecycleReadResultV2::kOk) {
            return PartialEventBrokerLifecycleOpenErrorV2::kNoStableSnapshot;
        }
        return PartialEventBrokerLifecycleOpenErrorV2::kNone;
    }

    [[nodiscard]] PartialEventBrokerLifecycleReadResultV2 ReadSnapshot(
        PartialEventBrokerLifecycleSnapshotV2* output,
        bool evaluate_heartbeat) const noexcept {
        if (output == nullptr || page_ == nullptr) {
            return PartialEventBrokerLifecycleReadResultV2::kInvalidArgument;
        }
        constexpr std::size_t attempts = 64U;
        for (std::size_t attempt = 0U; attempt < attempts; ++attempt) {
            const std::uint64_t start =
                Atomic(page_->commit_tag).load(std::memory_order_acquire);
            if (start == 0U || (start & 1U) != 0U) {
                continue;
            }
            PartialEventBrokerLifecycleSnapshotV2 local{};
            local.heartbeat_monotonic_ns =
                Atomic(page_->heartbeat_monotonic_ns)
                    .load(std::memory_order_relaxed);
            local.heartbeat_timeout_ns =
                Atomic(page_->heartbeat_timeout_ns)
                    .load(std::memory_order_relaxed);
            local.lifecycle_epoch = Atomic(page_->lifecycle_epoch)
                                        .load(std::memory_order_relaxed);
            local.publication_generation =
                Atomic(page_->publication_generation)
                    .load(std::memory_order_relaxed);
            local.correction_epoch = Atomic(page_->correction_epoch)
                                         .load(std::memory_order_relaxed);
            local.expected_worker = Atomic(page_->expected_worker)
                                        .load(std::memory_order_relaxed);
            local.broker_pid = Atomic(page_->broker_pid)
                                   .load(std::memory_order_relaxed);
            local.broker_state = Atomic(page_->broker_state)
                                     .load(std::memory_order_relaxed);
            const std::uint32_t stale = Atomic(page_->broker_stale)
                                            .load(std::memory_order_relaxed);
            std::atomic_thread_fence(std::memory_order_acq_rel);
            if (Atomic(page_->commit_tag).load(std::memory_order_acquire) !=
                start) {
                continue;
            }
            local.asynchronously_failed_lease_epoch =
                Atomic(page_->asynchronously_failed_lease_epoch)
                    .load(std::memory_order_acquire);
            if (local.heartbeat_monotonic_ns == 0U ||
                !ValidLifecycleHeartbeatTimeout(
                    local.heartbeat_timeout_ns) ||
                local.expected_worker < -1 || local.expected_worker == 0 ||
                local.broker_pid <= 0 ||
                !LifecycleStateCanonical(local.broker_state, stale) ||
                ((local.publication_generation == 0U) !=
                 (local.correction_epoch == 0U))) {
                return PartialEventBrokerLifecycleReadResultV2::kCorrupt;
            }
            local.broker_stale = stale != 0U;
            std::uint64_t now = 0U;
            local.heartbeat_expired =
                evaluate_heartbeat &&
                (!ReadMonotonicNs(&now) ||
                 now < local.heartbeat_monotonic_ns ||
                 now - local.heartbeat_monotonic_ns >
                     local.heartbeat_timeout_ns);
            const bool signalled_failure =
                local.expected_worker > 0 && local.lifecycle_epoch != 0U &&
                local.asynchronously_failed_lease_epoch ==
                    local.lifecycle_epoch;
            if (signalled_failure ||
                (local.heartbeat_expired &&
                 local.broker_state !=
                     PartialEventBrokerStateV2::kStoppedClean)) {
                local.broker_state =
                    local.publication_generation == 0U
                        ? PartialEventBrokerStateV2::kUnavailable
                        : PartialEventBrokerStateV2::kStale;
                local.broker_stale = true;
            }
            *output = local;
            return PartialEventBrokerLifecycleReadResultV2::kOk;
        }
        return PartialEventBrokerLifecycleReadResultV2::kInconsistent;
    }

private:
    int descriptor_ = -1;
    void* mapping_ = MAP_FAILED;
    std::size_t mapping_bytes_ = 0U;
    const PartialEventBrokerLifecyclePageV2* page_ = nullptr;
};

PartialEventBrokerLifecycleReaderV2::PartialEventBrokerLifecycleReaderV2(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

PartialEventBrokerLifecycleReaderV2::~PartialEventBrokerLifecycleReaderV2() =
    default;

PartialEventBrokerLifecycleOpenErrorV2
PartialEventBrokerLifecycleReaderV2::OpenDescriptor(
    int descriptor,
    const common::Identity128& expected_run_id,
    std::uint64_t expected_session_epoch,
    std::uint32_t expected_trade_date,
    std::unique_ptr<PartialEventBrokerLifecycleReaderV2>* output,
    int* system_error_number) noexcept {
    if (system_error_number != nullptr) {
        *system_error_number = 0;
    }
    if (output == nullptr) {
        return PartialEventBrokerLifecycleOpenErrorV2::kNullOutput;
    }
    output->reset();
    try {
        auto impl = std::make_unique<Impl>();
        const auto error = impl->Open(
            descriptor,
            expected_run_id,
            expected_session_epoch,
            expected_trade_date,
            system_error_number);
        if (error != PartialEventBrokerLifecycleOpenErrorV2::kNone) {
            return error;
        }
        *output = std::unique_ptr<PartialEventBrokerLifecycleReaderV2>(
            new PartialEventBrokerLifecycleReaderV2(std::move(impl)));
        return PartialEventBrokerLifecycleOpenErrorV2::kNone;
    } catch (const std::bad_alloc&) {
        return PartialEventBrokerLifecycleOpenErrorV2::kMappingFailed;
    } catch (...) {
        return PartialEventBrokerLifecycleOpenErrorV2::kUnexpectedFailure;
    }
}

PartialEventBrokerLifecycleReadResultV2
PartialEventBrokerLifecycleReaderV2::ReadSnapshot(
    PartialEventBrokerLifecycleSnapshotV2* output,
    bool evaluate_heartbeat) const noexcept {
    return impl_ == nullptr
               ? PartialEventBrokerLifecycleReadResultV2::kInvalidArgument
               : impl_->ReadSnapshot(output, evaluate_heartbeat);
}

std::string_view PartialEventBrokerResultNameV2(
    PartialEventBrokerResultV2 result) noexcept {
    switch (result) {
        case PartialEventBrokerResultV2::kOk:
            return "ok";
        case PartialEventBrokerResultV2::kUnavailable:
            return "unavailable";
        case PartialEventBrokerResultV2::kProtocolError:
            return "protocol_error";
        case PartialEventBrokerResultV2::kPeerRejected:
            return "peer_rejected";
        case PartialEventBrokerResultV2::kSessionMismatch:
            return "session_mismatch";
        case PartialEventBrokerResultV2::kGenerationRejected:
            return "generation_rejected";
        case PartialEventBrokerResultV2::kDescriptorRejected:
            return "descriptor_rejected";
        case PartialEventBrokerResultV2::kContinuityRejected:
            return "continuity_rejected";
        case PartialEventBrokerResultV2::kInternalError:
            return "internal_error";
    }
    return "unknown";
}

std::string_view PartialEventBrokerStateNameV2(
    PartialEventBrokerStateV2 state) noexcept {
    switch (state) {
        case PartialEventBrokerStateV2::kUnavailable:
            return "unavailable";
        case PartialEventBrokerStateV2::kReady:
            return "ready";
        case PartialEventBrokerStateV2::kStale:
            return "stale";
        case PartialEventBrokerStateV2::kRestarting:
            return "restarting";
        case PartialEventBrokerStateV2::kStoppedClean:
            return "stopped_clean";
    }
    return "unknown";
}

PartialEventBrokerResultV2 SubmitPartialEventGenerationV2(
    const std::filesystem::path& worker_handoff_socket_path,
    int descriptor,
    const PartialOrderEventJournalSessionV2& session,
    std::chrono::milliseconds timeout,
    PartialEventHandoffResponseV2* response,
    int* system_error_number) noexcept {
    if (response != nullptr) {
        *response = {};
    }
    if (system_error_number != nullptr) {
        *system_error_number = 0;
    }
    if (descriptor < 0 || !ValidTimeout(timeout)) {
        return PartialEventBrokerResultV2::kDescriptorRejected;
    }
    if (!DescriptorMatchesSession(
            descriptor, session, system_error_number)) {
        return PartialEventBrokerResultV2::kDescriptorRejected;
    }
    try {
        bool peer_rejected = false;
        UniqueFd socket = Connect(
            worker_handoff_socket_path,
            timeout,
            &peer_rejected,
            nullptr,
            system_error_number);
        if (socket.get() < 0) {
            return peer_rejected
                       ? PartialEventBrokerResultV2::kPeerRejected
                       : PartialEventBrokerResultV2::kUnavailable;
        }
        const PartialEventHandoffRequestV2 request =
            HandoffRequest(session);
        if (!SendPacket(socket.get(), request, descriptor)) {
            if (system_error_number != nullptr) {
                *system_error_number = errno;
            }
            return PartialEventBrokerResultV2::kProtocolError;
        }
        PartialEventHandoffResponseV2 received{};
        UniqueFd unexpected_descriptor;
        if (!ReceivePacket(
                socket.get(),
                &received,
                0,
                &unexpected_descriptor) ||
            !HandoffResponseCanonical(received)) {
            if (system_error_number != nullptr) {
                *system_error_number = errno;
            }
            return PartialEventBrokerResultV2::kProtocolError;
        }
        if (response != nullptr) {
            *response = received;
        }
        return received.result;
    } catch (...) {
        return PartialEventBrokerResultV2::kInternalError;
    }
}

PartialEventBrokerResultV2 RequestPartialEventGenerationV2(
    const std::filesystem::path& public_socket_path,
    const common::Identity128& expected_run_id,
    std::uint64_t expected_session_epoch,
    std::uint32_t expected_trade_date,
    std::uint64_t minimum_publication_generation,
    std::chrono::milliseconds timeout,
    int* output_descriptor,
    int* output_lifecycle_descriptor,
    PartialEventBrokerResponseV2* response,
    int* system_error_number) noexcept {
    if (output_descriptor == nullptr ||
        output_lifecycle_descriptor == nullptr) {
        return PartialEventBrokerResultV2::kDescriptorRejected;
    }
    *output_descriptor = -1;
    *output_lifecycle_descriptor = -1;
    if (response != nullptr) {
        *response = {};
    }
    if (system_error_number != nullptr) {
        *system_error_number = 0;
    }
    if (output_descriptor == output_lifecycle_descriptor) {
        return PartialEventBrokerResultV2::kDescriptorRejected;
    }
    if (common::IsZeroIdentity(expected_run_id) ||
        expected_session_epoch == 0U ||
        !ValidTradeDate(expected_trade_date) ||
        !ValidTimeout(timeout)) {
        return PartialEventBrokerResultV2::kSessionMismatch;
    }
    try {
        bool peer_rejected = false;
        pid_t connected_peer = -1;
        UniqueFd socket = Connect(
            public_socket_path,
            timeout,
            &peer_rejected,
            &connected_peer,
            system_error_number);
        if (socket.get() < 0) {
            return peer_rejected
                       ? PartialEventBrokerResultV2::kPeerRejected
                       : PartialEventBrokerResultV2::kUnavailable;
        }
        PartialEventBrokerRequestV2 request{};
        request.run_id = WireIdentity(expected_run_id);
        request.session_epoch = expected_session_epoch;
        request.minimum_publication_generation =
            minimum_publication_generation;
        request.trade_date = expected_trade_date;
        if (!SendPacket(socket.get(), request)) {
            if (system_error_number != nullptr) {
                *system_error_number = errno;
            }
            return PartialEventBrokerResultV2::kProtocolError;
        }
        PartialEventBrokerResponseV2 received{};
        UniqueFd descriptor;
        UniqueFd lifecycle_descriptor;
        if (!ReceivePacket(
                socket.get(),
                &received,
                -1,
                &descriptor,
                &lifecycle_descriptor) ||
            !PublicResponseCanonical(received) ||
            (received.result == PartialEventBrokerResultV2::kOk
                 ? received.descriptor_count != 2U ||
                       descriptor.get() < 0 ||
                       lifecycle_descriptor.get() < 0
                 : received.descriptor_count != 0U ||
                       descriptor.get() >= 0 ||
                       lifecycle_descriptor.get() >= 0)) {
            return PartialEventBrokerResultV2::kProtocolError;
        }
        if (received.result != PartialEventBrokerResultV2::kOk) {
            if (response != nullptr) {
                *response = received;
            }
            return received.result;
        }
        if (NativeIdentity(received.run_id) != expected_run_id ||
            received.session_epoch != expected_session_epoch ||
            received.trade_date != expected_trade_date ||
            received.publication_generation <
                minimum_publication_generation) {
            return PartialEventBrokerResultV2::kSessionMismatch;
        }
        if (!DescriptorMatchesResponse(
                descriptor.get(), received, system_error_number)) {
            return PartialEventBrokerResultV2::kDescriptorRejected;
        }
        PartialEventBrokerLifecyclePageV2 lifecycle_page{};
        if (!LifecycleDescriptorValid(
                lifecycle_descriptor.get(),
                expected_run_id,
                expected_session_epoch,
                expected_trade_date,
                &lifecycle_page,
                system_error_number) ||
            lifecycle_page.broker_pid !=
                static_cast<std::int64_t>(connected_peer)) {
            return PartialEventBrokerResultV2::kDescriptorRejected;
        }
        std::unique_ptr<PartialEventBrokerLifecycleReaderV2> lifecycle_reader;
        if (PartialEventBrokerLifecycleReaderV2::OpenDescriptor(
                lifecycle_descriptor.get(),
                expected_run_id,
                expected_session_epoch,
                expected_trade_date,
                &lifecycle_reader,
                system_error_number) !=
                PartialEventBrokerLifecycleOpenErrorV2::kNone ||
            lifecycle_reader == nullptr) {
            return PartialEventBrokerResultV2::kDescriptorRejected;
        }
        PartialEventBrokerLifecycleSnapshotV2 lifecycle_snapshot{};
        if (lifecycle_reader->ReadSnapshot(&lifecycle_snapshot) !=
                PartialEventBrokerLifecycleReadResultV2::kOk ||
            lifecycle_snapshot.publication_generation !=
                received.publication_generation ||
            lifecycle_snapshot.correction_epoch !=
                received.correction_epoch) {
            return PartialEventBrokerResultV2::kGenerationRejected;
        }
        if (response != nullptr) {
            *response = received;
        }
        *output_descriptor = descriptor.Release();
        *output_lifecycle_descriptor = lifecycle_descriptor.Release();
        return received.result;
    } catch (...) {
        return PartialEventBrokerResultV2::kInternalError;
    }
}

}  // namespace l2flow::ipc
