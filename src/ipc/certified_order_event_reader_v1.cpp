#include "l2flow/ipc/certified_order_event_reader_v1.h"

#include "l2flow/ipc/realtime_certified_service_v1.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <memory>
#include <new>
#include <utility>

#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#ifndef F_SEAL_FUTURE_WRITE
#define F_SEAL_FUTURE_WRITE 0x0010
#endif

namespace l2flow::ipc {
namespace {

constexpr std::size_t kReadAttempts = 8U;

enum class StableCopyResult : std::uint8_t {
    kCopied = 0U,
    kNeverPublished,
    kInconsistent,
    kInvalid,
};

template <typename T>
[[nodiscard]] std::atomic_ref<T> Atomic(
    const T& value) noexcept {
    return std::atomic_ref<T>(const_cast<T&>(value));
}

template <typename Value, std::size_t Size>
[[nodiscard]] bool AllZero(
    const std::array<Value, Size>& values) noexcept {
    return std::all_of(
        values.begin(), values.end(), [](Value value) {
            return value == Value{};
        });
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

[[nodiscard]] bool ExpectedSessionValid(
    const RealtimeCertifiedExpectedSessionV1& expected) noexcept {
    return std::any_of(
               expected.run_id.begin(),
               expected.run_id.end(),
               [](std::uint8_t value) { return value != 0U; }) &&
           expected.session_epoch != 0U &&
           realtime_certified_wire_v1_detail::ValidTradeDate(
               expected.trade_date);
}

[[nodiscard]] bool ExpectedSessionEqual(
    const RealtimeCertifiedExpectedSessionV1& left,
    const RealtimeCertifiedExpectedSessionV1& right) noexcept {
    return left.run_id == right.run_id &&
           left.session_epoch == right.session_epoch &&
           left.trade_date == right.trade_date;
}

struct EventStatus final {
    std::uint32_t coverage_flags = 0U;
    std::uint64_t publish_tag = 0U;
    std::uint64_t heartbeat_monotonic_ns = 0U;
    std::uint64_t canonical_apply_frontier = 0U;
    std::uint64_t event_published_sequence = 0U;
    std::uint64_t generation = 0U;
    std::uint64_t shanghai_order_state_count = 0U;
    std::uint64_t shenzhen_order_state_count = 0U;
    std::uint64_t committed_mapping_bytes = 0U;
};

[[nodiscard]] bool ProducerFrozen(
    RealtimeCertifiedStateV1 state) noexcept {
    return state == RealtimeCertifiedStateV1::kFrozenConflict ||
           state == RealtimeCertifiedStateV1::kFrozenResource;
}

[[nodiscard]] StableCopyResult CopyEventStatus(
    const CertifiedOrderEventHeaderV1& header,
    EventStatus* output) noexcept {
    if (output == nullptr) {
        return StableCopyResult::kInvalid;
    }
    for (std::size_t attempt = 0U; attempt < kReadAttempts;
         ++attempt) {
        const std::uint64_t begin =
            Atomic(header.status_publish_tag)
                .load(std::memory_order_acquire);
        if (begin == 0U) {
            return StableCopyResult::kInvalid;
        }
        if ((begin & 1U) != 0U) {
            continue;
        }
        EventStatus status{};
        status.publish_tag = begin;
        status.coverage_flags =
            Atomic(header.flags).load(std::memory_order_relaxed);
        status.heartbeat_monotonic_ns =
            Atomic(header.heartbeat_monotonic_ns)
                .load(std::memory_order_relaxed);
        status.canonical_apply_frontier =
            Atomic(header.canonical_apply_frontier)
                .load(std::memory_order_relaxed);
        status.event_published_sequence =
            Atomic(header.event_published_sequence)
                .load(std::memory_order_relaxed);
        status.generation =
            Atomic(header.generation)
                .load(std::memory_order_relaxed);
        status.shanghai_order_state_count =
            Atomic(header.shanghai_order_state_count)
                .load(std::memory_order_relaxed);
        status.shenzhen_order_state_count =
            Atomic(header.shenzhen_order_state_count)
                .load(std::memory_order_relaxed);
        status.committed_mapping_bytes =
            Atomic(header.committed_mapping_bytes)
                .load(std::memory_order_relaxed);
        const std::uint64_t reserved =
            Atomic(header.reserved_status)
                .load(std::memory_order_relaxed);
        std::atomic_thread_fence(std::memory_order_acq_rel);
        const std::uint64_t end =
            Atomic(header.status_publish_tag)
                .load(std::memory_order_acquire);
        if (begin != end || (end & 1U) != 0U) {
            continue;
        }
        if ((status.coverage_flags &
                 ~kCertifiedOrderEventKnownCoverageFlagsV1) != 0U ||
            (status.coverage_flags &
                 kCertifiedOrderEventCoverageFromOpenV1) == 0U ||
            status.heartbeat_monotonic_ns == 0U ||
            status.generation !=
                status.canonical_apply_frontier ||
            status.event_published_sequence >
                header.event_capacity ||
            status.committed_mapping_bytes <
                kCertifiedOrderEventHeaderBytesV1 ||
            status.committed_mapping_bytes >
                header.total_mapping_bytes ||
            status.committed_mapping_bytes % 4096U != 0U ||
            reserved != 0U) {
            return StableCopyResult::kInvalid;
        }
        *output = status;
        return StableCopyResult::kCopied;
    }
    return StableCopyResult::kInconsistent;
}

[[nodiscard]] CertifiedOrderEventHeaderV1 CopyHeader(
    const CertifiedOrderEventHeaderV1& source,
    const EventStatus& status) noexcept {
    CertifiedOrderEventHeaderV1 result{};
    result.magic = source.magic;
    result.abi_major = source.abi_major;
    result.abi_minor = source.abi_minor;
    result.header_bytes = source.header_bytes;
    result.endian_marker = source.endian_marker;
    result.flags = status.coverage_flags;
    result.total_mapping_bytes = source.total_mapping_bytes;
    result.run_id = source.run_id;
    result.session_epoch = source.session_epoch;
    result.trade_date = source.trade_date;
    result.reserved_identity = source.reserved_identity;
    result.slots_offset = source.slots_offset;
    result.event_capacity = source.event_capacity;
    result.slot_stride = source.slot_stride;
    result.region_alignment = source.region_alignment;
    result.reserved_layout = source.reserved_layout;
    result.status_publish_tag = status.publish_tag;
    result.heartbeat_monotonic_ns =
        status.heartbeat_monotonic_ns;
    result.canonical_apply_frontier =
        status.canonical_apply_frontier;
    result.event_published_sequence =
        status.event_published_sequence;
    result.generation = status.generation;
    result.shanghai_order_state_count =
        status.shanghai_order_state_count;
    result.shenzhen_order_state_count =
        status.shenzhen_order_state_count;
    result.committed_mapping_bytes =
        status.committed_mapping_bytes;
    result.reserved_status = 0U;
    result.reserved = source.reserved;
    return result;
}

[[nodiscard]] StableCopyResult CopySlot(
    const CertifiedOrderEventSlotV1& source,
    CertifiedOrderEventEnvelopeV1* output) noexcept {
    if (output == nullptr) {
        return StableCopyResult::kInvalid;
    }
    for (std::size_t attempt = 0U; attempt < kReadAttempts;
         ++attempt) {
        const std::uint64_t begin =
            Atomic(source.publish_tag)
                .load(std::memory_order_acquire);
        if (begin == 0U) {
            return StableCopyResult::kNeverPublished;
        }
        if ((begin & 1U) != 0U) {
            continue;
        }
        CertifiedOrderEventEnvelopeV1 envelope{};
        envelope.canonical_apply_sequence =
            Atomic(source.canonical_apply_sequence)
                .load(std::memory_order_relaxed);
        std::array<std::uint64_t, 40U> words{};
        for (std::size_t index = 0U; index < words.size(); ++index) {
            words[index] =
                Atomic(source.payload_words[index])
                    .load(std::memory_order_relaxed);
        }
        std::atomic_thread_fence(std::memory_order_acq_rel);
        const std::uint64_t end =
            Atomic(source.publish_tag)
                .load(std::memory_order_acquire);
        if (begin != end || (end & 1U) != 0U) {
            continue;
        }
        if (!AllZero(source.reserved)) {
            return StableCopyResult::kInvalid;
        }
        std::memcpy(
            &envelope.event, words.data(), sizeof(envelope.event));
        *output = envelope;
        return StableCopyResult::kCopied;
    }
    return StableCopyResult::kInconsistent;
}

[[nodiscard]] bool SocketPathSyntaxValid(
    const std::filesystem::path& path) noexcept {
    const auto& native = path.native();
    sockaddr_un address{};
    return !native.empty() && native.front() == '/' &&
           native.size() < sizeof(address.sun_path) &&
           ::strnlen(native.c_str(), sizeof(address.sun_path)) ==
               native.size();
}

[[nodiscard]] bool ConfigureTimeout(
    int descriptor,
    std::chrono::milliseconds timeout) noexcept {
    timeval value{};
    value.tv_sec =
        static_cast<time_t>(timeout.count() / 1000);
    value.tv_usec = static_cast<suseconds_t>(
        (timeout.count() % 1000) * 1000);
    return ::setsockopt(
               descriptor,
               SOL_SOCKET,
               SO_RCVTIMEO,
               &value,
               sizeof(value)) == 0 &&
           ::setsockopt(
               descriptor,
               SOL_SOCKET,
               SO_SNDTIMEO,
               &value,
               sizeof(value)) == 0;
}

[[nodiscard]] std::uint64_t NextNonce() noexcept {
    static std::atomic<std::uint64_t> next{1U};
    std::uint64_t value =
        next.fetch_add(1U, std::memory_order_relaxed);
    if (value == 0U) {
        value = next.fetch_add(1U, std::memory_order_relaxed);
    }
    return value;
}

[[nodiscard]] CertifiedOrderEventReaderOpenErrorV1
MapServiceStatus(std::uint16_t status) noexcept {
    switch (static_cast<RealtimeCertifiedControlStatusV1>(status)) {
        case RealtimeCertifiedControlStatusV1::kOk:
            return CertifiedOrderEventReaderOpenErrorV1::kNone;
        case RealtimeCertifiedControlStatusV1::kInvalidRequest:
            return CertifiedOrderEventReaderOpenErrorV1::kProtocolError;
        case RealtimeCertifiedControlStatusV1::kUnavailable:
            return CertifiedOrderEventReaderOpenErrorV1::kUnavailable;
        case RealtimeCertifiedControlStatusV1::kInternal:
            return CertifiedOrderEventReaderOpenErrorV1::
                kServiceInternal;
    }
    return CertifiedOrderEventReaderOpenErrorV1::kProtocolError;
}

struct ReceivedEventSession final {
    CertifiedOrderEventControlResponseV1 response{};
    int descriptor = -1;
    std::size_t descriptor_count = 0U;
};

[[nodiscard]] bool ReceiveEventSession(
    int socket_fd,
    ReceivedEventSession* output,
    int* system_error_number) noexcept {
    *output = {};
    output->descriptor = -1;
    std::array<std::byte, CMSG_SPACE(sizeof(int) * 2U)> control{};
    iovec vector{};
    vector.iov_base = &output->response;
    vector.iov_len = sizeof(output->response);
    msghdr message{};
    message.msg_iov = &vector;
    message.msg_iovlen = 1U;
    message.msg_control = control.data();
    message.msg_controllen = control.size();
    ssize_t received = -1;
    do {
        received = ::recvmsg(
            socket_fd,
            &message,
#ifdef MSG_CMSG_CLOEXEC
            MSG_CMSG_CLOEXEC
#else
            0
#endif
        );
    } while (received < 0 && errno == EINTR);
    if (received <= 0) {
        SetSystemError(
            system_error_number,
            received < 0 ? errno : ECONNRESET);
        return false;
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
        const std::size_t bytes =
            header->cmsg_len - CMSG_LEN(0U);
        if (bytes % sizeof(int) != 0U) {
            ancillary_valid = false;
            continue;
        }
        const std::size_t count = bytes / sizeof(int);
        for (std::size_t index = 0U; index < count; ++index) {
            int descriptor = -1;
            std::memcpy(
                &descriptor,
                CMSG_DATA(header) + index * sizeof(int),
                sizeof(descriptor));
            if (output->descriptor_count == 0U) {
                output->descriptor = descriptor;
            } else {
                static_cast<void>(::close(descriptor));
            }
            ++output->descriptor_count;
        }
    }
    if (static_cast<std::size_t>(received) !=
            sizeof(output->response) ||
        (message.msg_flags & (MSG_TRUNC | MSG_CTRUNC)) != 0 ||
        !ancillary_valid || output->descriptor_count > 1U) {
        CloseDescriptor(&output->descriptor);
        return false;
    }
#ifndef MSG_CMSG_CLOEXEC
    if (output->descriptor >= 0) {
        const int flags = ::fcntl(output->descriptor, F_GETFD);
        if (flags < 0 ||
            ::fcntl(
                output->descriptor,
                F_SETFD,
                flags | FD_CLOEXEC) != 0) {
            SetSystemError(system_error_number, errno);
            CloseDescriptor(&output->descriptor);
            return false;
        }
    }
#endif
    return true;
}

[[nodiscard]] CertifiedOrderEventReaderOpenErrorV1
RequestEventDescriptor(
    const RealtimeCertifiedReaderOpenOptionsV1& options,
    int* output_descriptor,
    std::uint32_t* output_coverage_flags,
    int* system_error_number) noexcept {
    *output_descriptor = -1;
    if (output_coverage_flags == nullptr) {
        return CertifiedOrderEventReaderOpenErrorV1::kInvalidArgument;
    }
    *output_coverage_flags = 0U;
    if (!SocketPathSyntaxValid(options.control_socket_path)) {
        return CertifiedOrderEventReaderOpenErrorV1::
            kSocketPathInvalid;
    }
    const auto& native = options.control_socket_path.native();
    struct stat path_stat {};
    if (::lstat(native.c_str(), &path_stat) != 0 ||
        !S_ISSOCK(path_stat.st_mode) ||
        path_stat.st_uid != ::geteuid()) {
        SetSystemError(system_error_number, errno);
        return CertifiedOrderEventReaderOpenErrorV1::
            kSocketPathUnsafe;
    }

    int socket_fd =
        ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    if (socket_fd < 0) {
        SetSystemError(system_error_number, errno);
        return CertifiedOrderEventReaderOpenErrorV1::
            kSocketCreateFailed;
    }
    if (!ConfigureTimeout(socket_fd, options.timeout)) {
        SetSystemError(system_error_number, errno);
        CloseDescriptor(&socket_fd);
        return CertifiedOrderEventReaderOpenErrorV1::
            kSocketCreateFailed;
    }
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::memcpy(
        address.sun_path, native.c_str(), native.size() + 1U);
    if (::connect(
            socket_fd,
            reinterpret_cast<const sockaddr*>(&address),
            sizeof(address)) != 0) {
        SetSystemError(system_error_number, errno);
        CloseDescriptor(&socket_fd);
        return CertifiedOrderEventReaderOpenErrorV1::
            kSocketConnectFailed;
    }
#ifdef SO_PEERCRED
    ucred peer{};
    socklen_t peer_bytes = sizeof(peer);
    if (::getsockopt(
            socket_fd,
            SOL_SOCKET,
            SO_PEERCRED,
            &peer,
            &peer_bytes) != 0 ||
        peer_bytes != sizeof(peer) ||
        peer.uid != ::geteuid()) {
        SetSystemError(system_error_number, EPERM);
        CloseDescriptor(&socket_fd);
        return CertifiedOrderEventReaderOpenErrorV1::
            kSocketPathUnsafe;
    }
#endif

    RealtimeCertifiedControlRequestV1 request{};
    request.opcode = static_cast<std::uint16_t>(
        RealtimeCertifiedControlOpcodeV1::kGetEventHistory);
    request.nonce = NextNonce();
    ssize_t sent = -1;
    do {
        sent = ::send(
            socket_fd,
            &request,
            sizeof(request),
            MSG_NOSIGNAL);
    } while (sent < 0 && errno == EINTR);
    if (sent != static_cast<ssize_t>(sizeof(request))) {
        SetSystemError(
            system_error_number,
            sent < 0 ? errno : EPROTO);
        CloseDescriptor(&socket_fd);
        return CertifiedOrderEventReaderOpenErrorV1::
            kRequestSendFailed;
    }

    ReceivedEventSession received{};
    if (!ReceiveEventSession(
            socket_fd, &received, system_error_number)) {
        CloseDescriptor(&socket_fd);
        return CertifiedOrderEventReaderOpenErrorV1::
            kResponseReceiveFailed;
    }
    CloseDescriptor(&socket_fd);

    const auto& response = received.response;
    const bool prefix_valid =
        response.magic ==
            kCertifiedOrderEventControlResponseMagicV1 &&
        response.abi_major ==
            kCertifiedOrderEventWireMajorV1 &&
        response.abi_minor ==
            kCertifiedOrderEventWireMinorV1 &&
        response.response_bytes == sizeof(response) &&
        response.nonce == request.nonce &&
        response.reserved0 == 0U &&
        (response.coverage_flags &
             ~kCertifiedOrderEventKnownCoverageFlagsV1) == 0U &&
        AllZero(response.reserved) &&
        response.status <= static_cast<std::uint16_t>(
            RealtimeCertifiedControlStatusV1::kInternal);
    if (!prefix_valid) {
        CloseDescriptor(&received.descriptor);
        return CertifiedOrderEventReaderOpenErrorV1::kProtocolError;
    }

    const auto service_error = MapServiceStatus(response.status);
    RealtimeCertifiedExpectedSessionV1 actual{};
    actual.run_id = response.run_id;
    actual.session_epoch = response.session_epoch;
    actual.trade_date = response.trade_date;
    if (!ExpectedSessionEqual(
            actual, options.expected_session)) {
        CloseDescriptor(&received.descriptor);
        return CertifiedOrderEventReaderOpenErrorV1::
            kSessionMismatch;
    }
    if (service_error !=
        CertifiedOrderEventReaderOpenErrorV1::kNone) {
        const bool canonical_error =
            received.descriptor_count == 0U &&
            response.mapping_bytes >=
                kCertifiedOrderEventHeaderBytesV1;
        CloseDescriptor(&received.descriptor);
        return canonical_error
                   ? service_error
                   : CertifiedOrderEventReaderOpenErrorV1::
                         kProtocolError;
    }
    if (received.descriptor_count != 1U ||
        received.descriptor < 0 ||
        response.mapping_bytes <
            kCertifiedOrderEventHeaderBytesV1 ||
        response.event_capacity == 0U ||
        response.slot_stride !=
            kCertifiedOrderEventSlotBytesV1) {
        CloseDescriptor(&received.descriptor);
        return CertifiedOrderEventReaderOpenErrorV1::kProtocolError;
    }
    struct stat descriptor_stat {};
    if (::fstat(received.descriptor, &descriptor_stat) != 0 ||
        descriptor_stat.st_size < 0 ||
        static_cast<std::uint64_t>(descriptor_stat.st_size) !=
            response.mapping_bytes) {
        SetSystemError(system_error_number, errno);
        CloseDescriptor(&received.descriptor);
        return CertifiedOrderEventReaderOpenErrorV1::
            kDescriptorInvalid;
    }
    *output_descriptor = received.descriptor;
    *output_coverage_flags = response.coverage_flags;
    received.descriptor = -1;
    return CertifiedOrderEventReaderOpenErrorV1::kNone;
}

}  // namespace

class CertifiedOrderEventReaderV1::Impl final {
public:
    ~Impl() {
        if (event_mapping_ != MAP_FAILED) {
            static_cast<void>(
                ::munmap(event_mapping_, event_mapping_bytes_));
        }
    }

    [[nodiscard]] static CertifiedOrderEventReaderOpenErrorV1
    OpenEventDescriptor(
        int descriptor,
        const RealtimeCertifiedExpectedSessionV1& expected,
        std::unique_ptr<RealtimeCertifiedReaderV1> tick,
        std::unique_ptr<Impl>* output,
        int* system_error_number) noexcept {
        if (descriptor < 0 || tick == nullptr ||
            output == nullptr || !ExpectedSessionValid(expected)) {
            return CertifiedOrderEventReaderOpenErrorV1::
                kInvalidArgument;
        }
        output->reset();
        if constexpr (std::endian::native != std::endian::little) {
            return CertifiedOrderEventReaderOpenErrorV1::
                kInvalidArgument;
        }
        const int descriptor_flags =
            ::fcntl(descriptor, F_GETFL);
        const int seals = ::fcntl(descriptor, F_GET_SEALS);
        constexpr int required_seals =
            F_SEAL_GROW | F_SEAL_SHRINK |
            F_SEAL_FUTURE_WRITE | F_SEAL_SEAL;
        struct stat descriptor_stat {};
        if (descriptor_flags < 0 || seals < 0 ||
            (descriptor_flags & O_ACCMODE) != O_RDONLY ||
            (seals & required_seals) != required_seals ||
            ::fstat(descriptor, &descriptor_stat) != 0 ||
            !S_ISREG(descriptor_stat.st_mode) ||
            descriptor_stat.st_size <
                static_cast<off_t>(
                    kCertifiedOrderEventHeaderBytesV1)) {
            SetSystemError(system_error_number, EACCES);
            return CertifiedOrderEventReaderOpenErrorV1::
                kDescriptorInvalid;
        }
        const std::uint64_t mapping_bytes =
            static_cast<std::uint64_t>(descriptor_stat.st_size);
        if (mapping_bytes >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max())) {
            return CertifiedOrderEventReaderOpenErrorV1::
                kLayoutInvalid;
        }
        void* const mapping = ::mmap(
            nullptr,
            static_cast<std::size_t>(mapping_bytes),
            PROT_READ,
            MAP_SHARED,
            descriptor,
            0);
        if (mapping == MAP_FAILED) {
            SetSystemError(system_error_number, errno);
            return CertifiedOrderEventReaderOpenErrorV1::
                kMappingFailed;
        }
        const auto* const header =
            static_cast<const CertifiedOrderEventHeaderV1*>(
                mapping);
        EventStatus status{};
        const StableCopyResult status_result =
            CopyEventStatus(*header, &status);
        const CertifiedOrderEventHeaderV1 header_copy =
            CopyHeader(*header, status);
        if (status_result != StableCopyResult::kCopied ||
            !CertifiedOrderEventHeaderCanonicalV1(header_copy) ||
            header_copy.total_mapping_bytes != mapping_bytes) {
            static_cast<void>(::munmap(
                mapping, static_cast<std::size_t>(mapping_bytes)));
            return CertifiedOrderEventReaderOpenErrorV1::
                kLayoutInvalid;
        }
        RealtimeCertifiedExpectedSessionV1 actual{};
        actual.run_id = header_copy.run_id;
        actual.session_epoch = header_copy.session_epoch;
        actual.trade_date = header_copy.trade_date;
        if (!ExpectedSessionEqual(actual, expected)) {
            static_cast<void>(::munmap(
                mapping, static_cast<std::size_t>(mapping_bytes)));
            return CertifiedOrderEventReaderOpenErrorV1::
                kSessionMismatch;
        }
        auto impl = std::unique_ptr<Impl>(
            new (std::nothrow) Impl());
        if (!impl) {
            static_cast<void>(::munmap(
                mapping, static_cast<std::size_t>(mapping_bytes)));
            return CertifiedOrderEventReaderOpenErrorV1::
                kResourceExhausted;
        }
        impl->tick_ = std::move(tick);
        impl->event_mapping_ = mapping;
        impl->event_mapping_bytes_ =
            static_cast<std::size_t>(mapping_bytes);
        impl->event_header_ = header;
        impl->event_slots_ = reinterpret_cast<
            const CertifiedOrderEventSlotV1*>(
                static_cast<const std::byte*>(mapping) +
                header_copy.slots_offset);
        impl->session_ = expected;
        impl->event_capacity_ = header_copy.event_capacity;
        *output = std::move(impl);
        return CertifiedOrderEventReaderOpenErrorV1::kNone;
    }

    [[nodiscard]] CertifiedOrderEventReadResultV1 ReadStatus(
        CertifiedOrderEventStatusSnapshotV1* output) const noexcept {
        if (output == nullptr) {
            return CertifiedOrderEventReadResultV1::kOutOfRange;
        }
        RealtimeCertifiedStatusSnapshotV1 tick{};
        const auto tick_result = tick_->ReadStatus(&tick);
        if (tick_result != RealtimeCertifiedReadResultV1::kOk) {
            return tick_result ==
                           RealtimeCertifiedReadResultV1::kInconsistent
                       ? CertifiedOrderEventReadResultV1::
                             kInconsistent
                       : CertifiedOrderEventReadResultV1::kCorrupt;
        }
        EventStatus event{};
        const StableCopyResult event_result =
            CopyEventStatus(*event_header_, &event);
        if (event_result == StableCopyResult::kInconsistent) {
            return CertifiedOrderEventReadResultV1::kInconsistent;
        }
        if (event_result != StableCopyResult::kCopied) {
            return CertifiedOrderEventReadResultV1::kCorrupt;
        }
        CertifiedOrderEventStatusSnapshotV1 result{};
        result.tick = tick;
        result.coverage_flags = event.coverage_flags;
        result.event_publish_tag = event.publish_tag;
        result.event_heartbeat_monotonic_ns =
            event.heartbeat_monotonic_ns;
        result.event_canonical_apply_frontier =
            event.canonical_apply_frontier;
        result.event_published_sequence =
            event.event_published_sequence;
        result.event_generation = event.generation;
        result.shanghai_order_state_count =
            event.shanghai_order_state_count;
        result.shenzhen_order_state_count =
            event.shenzhen_order_state_count;
        result.committed_mapping_bytes =
            event.committed_mapping_bytes;
        result.coherent_canonical_apply_frontier =
            std::min(
                tick.canonical_apply_frontier,
                event.canonical_apply_frontier);
        *output = result;
        return CertifiedOrderEventReadResultV1::kOk;
    }

    [[nodiscard]] CertifiedOrderEventReadResultV1 ReadEvent(
        std::uint64_t sequence,
        CertifiedOrderEventEnvelopeV1* output) const noexcept {
        if (output == nullptr || sequence == 0U) {
            return CertifiedOrderEventReadResultV1::kOutOfRange;
        }
        for (std::size_t attempt = 0U; attempt < kReadAttempts;
             ++attempt) {
            CertifiedOrderEventStatusSnapshotV1 status{};
            const auto status_result = ReadStatus(&status);
            if (status_result !=
                CertifiedOrderEventReadResultV1::kOk) {
                return status_result;
            }
            if (sequence > event_capacity_) {
                return ReadBeyondCapacity(
                    sequence, status, nullptr);
            }
            if (sequence > status.event_published_sequence) {
                return status.event_published_sequence == 0U
                           ? CertifiedOrderEventReadResultV1::kNoData
                           : CertifiedOrderEventReadResultV1::
                                 kNotYetPublished;
            }
            CertifiedOrderEventEnvelopeV1 envelope{};
            const StableCopyResult slot_result =
                CopySlot(event_slots_[sequence - 1U], &envelope);
            if (slot_result == StableCopyResult::kInconsistent ||
                slot_result == StableCopyResult::kNeverPublished) {
                continue;
            }
            if (slot_result != StableCopyResult::kCopied ||
                !CertifiedOrderEventEnvelopeCanonicalV1(
                    envelope, session_.trade_date, sequence)) {
                return CertifiedOrderEventReadResultV1::kCorrupt;
            }
            if (envelope.canonical_apply_sequence >
                status.coherent_canonical_apply_frontier) {
                return CertifiedOrderEventReadResultV1::
                    kNotYetPublished;
            }
            *output = envelope;
            return CertifiedOrderEventReadResultV1::kOk;
        }
        return CertifiedOrderEventReadResultV1::kInconsistent;
    }

    [[nodiscard]] CertifiedOrderEventReadResultV1 Read(
        std::uint64_t expected,
        std::span<CertifiedOrderEventEnvelopeV1> output,
        CertifiedOrderEventReadBatchResultV1* result) const noexcept {
        if (result == nullptr || expected == 0U) {
            return CertifiedOrderEventReadResultV1::kOutOfRange;
        }
        *result = {};
        result->next_event_sequence = expected;
        const auto status_result = ReadStatus(&result->status);
        if (status_result != CertifiedOrderEventReadResultV1::kOk) {
            return status_result;
        }
        if (expected > event_capacity_) {
            return ReadBeyondCapacity(
                expected, result->status, result);
        }
        std::uint64_t sequence = expected;
        while (result->written < output.size() &&
               sequence <=
                   result->status.event_published_sequence) {
            CertifiedOrderEventEnvelopeV1 envelope{};
            const StableCopyResult copied =
                CopySlot(event_slots_[sequence - 1U], &envelope);
            if (copied == StableCopyResult::kInconsistent ||
                copied == StableCopyResult::kNeverPublished) {
                return CertifiedOrderEventReadResultV1::
                    kInconsistent;
            }
            if (copied != StableCopyResult::kCopied ||
                !CertifiedOrderEventEnvelopeCanonicalV1(
                    envelope, session_.trade_date, sequence)) {
                return CertifiedOrderEventReadResultV1::kCorrupt;
            }
            if (envelope.canonical_apply_sequence >
                result->status
                    .coherent_canonical_apply_frontier) {
                break;
            }
            output[result->written] = envelope;
            ++result->written;
            ++sequence;
        }
        result->next_event_sequence = sequence;
        return CertifiedOrderEventReadResultV1::kOk;
    }

private:
    [[nodiscard]] CertifiedOrderEventReadResultV1
    ReadBeyondCapacity(
        std::uint64_t sequence,
        const CertifiedOrderEventStatusSnapshotV1& status,
        CertifiedOrderEventReadBatchResultV1* result) const noexcept {
        // event_capacity + 1 is the natural dense cursor only after every
        // physical Event slot has been published and the final slot is at or
        // below the coherent Tick/Event cut. `sequence > event_capacity_`
        // makes sequence - 1 safe even when the capacity is UINT64_MAX.
        if (sequence - 1U != event_capacity_ ||
            status.event_published_sequence != event_capacity_) {
            return CertifiedOrderEventReadResultV1::kOutOfRange;
        }
        CertifiedOrderEventEnvelopeV1 final_event{};
        const StableCopyResult copied =
            CopySlot(event_slots_[event_capacity_ - 1U], &final_event);
        if (copied == StableCopyResult::kInconsistent ||
            copied == StableCopyResult::kNeverPublished) {
            return CertifiedOrderEventReadResultV1::kInconsistent;
        }
        if (copied != StableCopyResult::kCopied ||
            !CertifiedOrderEventEnvelopeCanonicalV1(
                final_event, session_.trade_date, event_capacity_)) {
            return CertifiedOrderEventReadResultV1::kCorrupt;
        }
        const bool coherent =
            final_event.canonical_apply_sequence <=
            status.coherent_canonical_apply_frontier;
        if (ProducerFrozen(status.tick.state)) {
            return CertifiedOrderEventReadResultV1::kProducerFailed;
        }
        if (status.tick.state ==
            RealtimeCertifiedStateV1::kStopped) {
            return coherent
                       ? CertifiedOrderEventReadResultV1::kEndOfStream
                       : CertifiedOrderEventReadResultV1::kCorrupt;
        }
        if (!coherent) {
            return CertifiedOrderEventReadResultV1::kNotYetPublished;
        }
        if (result != nullptr) {
            result->next_event_sequence = sequence;
        }
        return CertifiedOrderEventReadResultV1::kNotYetPublished;
    }

public:
    std::unique_ptr<RealtimeCertifiedReaderV1> tick_;
    void* event_mapping_ = MAP_FAILED;
    std::size_t event_mapping_bytes_ = 0U;
    const CertifiedOrderEventHeaderV1* event_header_ = nullptr;
    const CertifiedOrderEventSlotV1* event_slots_ = nullptr;
    RealtimeCertifiedExpectedSessionV1 session_{};
    std::uint64_t event_capacity_ = 0U;
};

std::string_view CertifiedOrderEventReaderOpenErrorNameV1(
    CertifiedOrderEventReaderOpenErrorV1 error) noexcept {
    switch (error) {
        case CertifiedOrderEventReaderOpenErrorV1::kNone:
            return "none";
        case CertifiedOrderEventReaderOpenErrorV1::kNullOutput:
            return "null_output";
        case CertifiedOrderEventReaderOpenErrorV1::kInvalidArgument:
            return "invalid_argument";
        case CertifiedOrderEventReaderOpenErrorV1::
            kTickReaderOpenFailed:
            return "tick_reader_open_failed";
        case CertifiedOrderEventReaderOpenErrorV1::
            kSocketPathInvalid:
            return "socket_path_invalid";
        case CertifiedOrderEventReaderOpenErrorV1::
            kSocketPathUnsafe:
            return "socket_path_unsafe";
        case CertifiedOrderEventReaderOpenErrorV1::
            kSocketCreateFailed:
            return "socket_create_failed";
        case CertifiedOrderEventReaderOpenErrorV1::
            kSocketConnectFailed:
            return "socket_connect_failed";
        case CertifiedOrderEventReaderOpenErrorV1::
            kRequestSendFailed:
            return "request_send_failed";
        case CertifiedOrderEventReaderOpenErrorV1::
            kResponseReceiveFailed:
            return "response_receive_failed";
        case CertifiedOrderEventReaderOpenErrorV1::kProtocolError:
            return "protocol_error";
        case CertifiedOrderEventReaderOpenErrorV1::kUnavailable:
            return "unavailable";
        case CertifiedOrderEventReaderOpenErrorV1::kServiceInternal:
            return "service_internal";
        case CertifiedOrderEventReaderOpenErrorV1::
            kDescriptorInvalid:
            return "descriptor_invalid";
        case CertifiedOrderEventReaderOpenErrorV1::kMappingFailed:
            return "mapping_failed";
        case CertifiedOrderEventReaderOpenErrorV1::kLayoutInvalid:
            return "layout_invalid";
        case CertifiedOrderEventReaderOpenErrorV1::kSessionMismatch:
            return "session_mismatch";
        case CertifiedOrderEventReaderOpenErrorV1::
            kResourceExhausted:
            return "resource_exhausted";
        case CertifiedOrderEventReaderOpenErrorV1::
            kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

std::string_view CertifiedOrderEventReadResultNameV1(
    CertifiedOrderEventReadResultV1 result) noexcept {
    switch (result) {
        case CertifiedOrderEventReadResultV1::kOk:
            return "ok";
        case CertifiedOrderEventReadResultV1::kNoData:
            return "no_data";
        case CertifiedOrderEventReadResultV1::kNotYetPublished:
            return "not_yet_published";
        case CertifiedOrderEventReadResultV1::kOutOfRange:
            return "out_of_range";
        case CertifiedOrderEventReadResultV1::kInconsistent:
            return "inconsistent";
        case CertifiedOrderEventReadResultV1::kCorrupt:
            return "corrupt";
        case CertifiedOrderEventReadResultV1::kEndOfStream:
            return "end_of_stream";
        case CertifiedOrderEventReadResultV1::kProducerFailed:
            return "producer_failed";
    }
    return "unknown";
}

CertifiedOrderEventReaderV1::CertifiedOrderEventReaderV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

CertifiedOrderEventReaderV1::~CertifiedOrderEventReaderV1() =
    default;

CertifiedOrderEventReaderOpenErrorV1
CertifiedOrderEventReaderV1::OpenDescriptorsForTest(
    int tick_descriptor,
    int event_descriptor,
    const RealtimeCertifiedExpectedSessionV1& expected_session,
    std::unique_ptr<CertifiedOrderEventReaderV1>* output,
    int* system_error_number) noexcept {
    SetSystemError(system_error_number, 0);
    if (output == nullptr) {
        return CertifiedOrderEventReaderOpenErrorV1::kNullOutput;
    }
    output->reset();
    std::unique_ptr<RealtimeCertifiedReaderV1> tick;
    if (RealtimeCertifiedReaderV1::OpenDescriptorForTest(
            tick_descriptor,
            expected_session,
            &tick,
            system_error_number) !=
        RealtimeCertifiedReaderOpenErrorV1::kNone) {
        return CertifiedOrderEventReaderOpenErrorV1::
            kTickReaderOpenFailed;
    }
    std::unique_ptr<Impl> impl;
    const auto error = Impl::OpenEventDescriptor(
        event_descriptor,
        expected_session,
        std::move(tick),
        &impl,
        system_error_number);
    if (error != CertifiedOrderEventReaderOpenErrorV1::kNone) {
        return error;
    }
    auto reader = std::unique_ptr<CertifiedOrderEventReaderV1>(
        new (std::nothrow)
            CertifiedOrderEventReaderV1(std::move(impl)));
    if (!reader) {
        return CertifiedOrderEventReaderOpenErrorV1::
            kResourceExhausted;
    }
    *output = std::move(reader);
    return CertifiedOrderEventReaderOpenErrorV1::kNone;
}

CertifiedOrderEventReaderOpenErrorV1
CertifiedOrderEventReaderV1::Open(
    RealtimeCertifiedReaderOpenOptionsV1 options,
    std::unique_ptr<CertifiedOrderEventReaderV1>* output,
    int* system_error_number) noexcept {
    SetSystemError(system_error_number, 0);
    if (output == nullptr) {
        return CertifiedOrderEventReaderOpenErrorV1::kNullOutput;
    }
    output->reset();
    if (!ExpectedSessionValid(options.expected_session) ||
        options.timeout.count() <= 0 ||
        options.timeout > std::chrono::hours(24)) {
        return CertifiedOrderEventReaderOpenErrorV1::
            kInvalidArgument;
    }
    std::unique_ptr<RealtimeCertifiedReaderV1> tick;
    if (RealtimeCertifiedReaderV1::Open(
            options, &tick, system_error_number) !=
        RealtimeCertifiedReaderOpenErrorV1::kNone) {
        return CertifiedOrderEventReaderOpenErrorV1::
            kTickReaderOpenFailed;
    }
    int descriptor = -1;
    std::uint32_t control_coverage_flags = 0U;
    const auto request_error = RequestEventDescriptor(
        options,
        &descriptor,
        &control_coverage_flags,
        system_error_number);
    if (request_error !=
        CertifiedOrderEventReaderOpenErrorV1::kNone) {
        return request_error;
    }
    std::unique_ptr<Impl> impl;
    const auto open_error = Impl::OpenEventDescriptor(
        descriptor,
        options.expected_session,
        std::move(tick),
        &impl,
        system_error_number);
    CloseDescriptor(&descriptor);
    if (open_error !=
        CertifiedOrderEventReaderOpenErrorV1::kNone) {
        return open_error;
    }
    CertifiedOrderEventStatusSnapshotV1 status{};
    if (impl->ReadStatus(&status) !=
            CertifiedOrderEventReadResultV1::kOk ||
        status.coverage_flags != control_coverage_flags) {
        return CertifiedOrderEventReaderOpenErrorV1::kProtocolError;
    }
    auto reader = std::unique_ptr<CertifiedOrderEventReaderV1>(
        new (std::nothrow)
            CertifiedOrderEventReaderV1(std::move(impl)));
    if (!reader) {
        return CertifiedOrderEventReaderOpenErrorV1::
            kResourceExhausted;
    }
    *output = std::move(reader);
    return CertifiedOrderEventReaderOpenErrorV1::kNone;
}

CertifiedOrderEventReadResultV1
CertifiedOrderEventReaderV1::ReadStatus(
    CertifiedOrderEventStatusSnapshotV1* output) const noexcept {
    return impl_ == nullptr
               ? CertifiedOrderEventReadResultV1::kCorrupt
               : impl_->ReadStatus(output);
}

CertifiedOrderEventReadResultV1
CertifiedOrderEventReaderV1::ReadEvent(
    std::uint64_t sequence,
    CertifiedOrderEventEnvelopeV1* output) const noexcept {
    return impl_ == nullptr
               ? CertifiedOrderEventReadResultV1::kCorrupt
               : impl_->ReadEvent(sequence, output);
}

CertifiedOrderEventReadResultV1
CertifiedOrderEventReaderV1::Read(
    std::uint64_t expected,
    std::span<CertifiedOrderEventEnvelopeV1> output,
    CertifiedOrderEventReadBatchResultV1* result) const noexcept {
    return impl_ == nullptr
               ? CertifiedOrderEventReadResultV1::kCorrupt
               : impl_->Read(expected, output, result);
}

const RealtimeCertifiedExpectedSessionV1&
CertifiedOrderEventReaderV1::session() const noexcept {
    static const RealtimeCertifiedExpectedSessionV1 empty{};
    return impl_ == nullptr ? empty : impl_->session_;
}

std::uint64_t
CertifiedOrderEventReaderV1::event_capacity() const noexcept {
    return impl_ == nullptr ? 0U : impl_->event_capacity_;
}

}  // namespace l2flow::ipc
