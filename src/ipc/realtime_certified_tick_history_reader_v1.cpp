#include "l2flow/ipc/realtime_certified_tick_history_reader_v1.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <new>
#include <utility>

#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

namespace l2flow::ipc {
namespace {

template <typename Value, std::size_t Size>
[[nodiscard]] bool AllZero(
    const std::array<Value, Size>& values) noexcept {
    return std::all_of(
        values.begin(), values.end(), [](Value value) noexcept {
            return value == Value{};
        });
}

void SetSystemError(int* output, int value) noexcept {
    if (output != nullptr) {
        *output = value;
    }
}

void CloseDescriptor(int* descriptor) noexcept {
    if (descriptor == nullptr || *descriptor < 0) {
        return;
    }
    int result = -1;
    do {
        result = ::close(*descriptor);
    } while (result < 0 && errno == EINTR);
    *descriptor = -1;
}

[[nodiscard]] bool ExpectedSessionValid(
    const RealtimeCertifiedExpectedSessionV1& expected) noexcept {
    return !AllZero(expected.run_id) &&
           expected.session_epoch != 0U &&
           realtime_certified_wire_v1_detail::ValidTradeDate(
               expected.trade_date);
}

[[nodiscard]] bool ResponseSessionMatches(
    const RealtimeCertifiedTickHistoryControlResponseV1& response,
    const RealtimeCertifiedExpectedSessionV1& expected) noexcept {
    return response.run_id == expected.run_id &&
           response.session_epoch == expected.session_epoch &&
           response.trade_date == expected.trade_date;
}

[[nodiscard]] bool SocketPathSyntaxValid(
    const std::filesystem::path& path) noexcept {
    const auto& native = path.native();
    if (native.empty() || native.front() != '/') {
        return false;
    }
    sockaddr_un address{};
    return native.size() < sizeof(address.sun_path) &&
           ::strnlen(native.c_str(), sizeof(address.sun_path)) ==
               native.size();
}

[[nodiscard]] bool ConfigureTimeout(
    int descriptor,
    std::chrono::milliseconds timeout) noexcept {
    if (timeout.count() == 0) {
        return true;
    }
    timeval value{};
    value.tv_sec = static_cast<time_t>(timeout.count() / 1000);
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
    const std::uint64_t first =
        next.fetch_add(1U, std::memory_order_relaxed);
    return first != 0U
               ? first
               : next.fetch_add(1U, std::memory_order_relaxed);
}

struct ReceivedTickHistory final {
    RealtimeCertifiedTickHistoryControlResponseV1 response{};
    int descriptor = -1;
    std::size_t descriptor_count = 0U;
};

void CloseReceived(ReceivedTickHistory* received) noexcept {
    if (received != nullptr) {
        CloseDescriptor(&received->descriptor);
    }
}

[[nodiscard]] bool ReceiveResponse(
    int socket_descriptor,
    ReceivedTickHistory* output,
    int* system_error_number) noexcept {
    if (socket_descriptor < 0 || output == nullptr) {
        return false;
    }
    *output = {};
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
            socket_descriptor,
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
        const std::size_t payload_bytes =
            header->cmsg_len - CMSG_LEN(0U);
        if (payload_bytes % sizeof(int) != 0U) {
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
            if (output->descriptor_count == 0U) {
                output->descriptor = descriptor;
            } else {
                CloseDescriptor(&descriptor);
            }
            ++output->descriptor_count;
        }
    }
    const bool packet_valid =
        static_cast<std::size_t>(received) ==
            sizeof(output->response) &&
        (message.msg_flags & (MSG_TRUNC | MSG_CTRUNC)) == 0;
    if (!packet_valid || !ancillary_valid ||
        output->descriptor_count > 1U) {
        CloseReceived(output);
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
            const int error = errno;
            CloseReceived(output);
            SetSystemError(system_error_number, error);
            return false;
        }
    }
#endif
    return true;
}

[[nodiscard]] bool ResponseCanonical(
    const RealtimeCertifiedTickHistoryControlResponseV1& response,
    std::uint64_t nonce) noexcept {
    return response.magic ==
               kRealtimeCertifiedControlResponseMagicV1 &&
           response.abi_major ==
               kRealtimeCertifiedWireMajorV1 &&
           response.abi_minor ==
               kRealtimeCertifiedWireMinorV1 &&
           response.response_bytes == sizeof(response) &&
           response.nonce == nonce && response.reserved0 == 0U &&
           AllZero(response.reserved) &&
           response.status <= static_cast<std::uint16_t>(
               RealtimeCertifiedControlStatusV1::kInternal);
}

[[nodiscard]] RealtimeCertifiedReaderOpenErrorV1 MapStatus(
    std::uint16_t status) noexcept {
    switch (static_cast<RealtimeCertifiedControlStatusV1>(status)) {
        case RealtimeCertifiedControlStatusV1::kOk:
            return RealtimeCertifiedReaderOpenErrorV1::kNone;
        case RealtimeCertifiedControlStatusV1::kInvalidRequest:
            return RealtimeCertifiedReaderOpenErrorV1::kProtocolError;
        case RealtimeCertifiedControlStatusV1::kUnavailable:
            return RealtimeCertifiedReaderOpenErrorV1::kUnavailable;
        case RealtimeCertifiedControlStatusV1::kInternal:
            return RealtimeCertifiedReaderOpenErrorV1::
                kServiceInternal;
    }
    return RealtimeCertifiedReaderOpenErrorV1::kProtocolError;
}

[[nodiscard]] RealtimeCertifiedReaderOpenErrorV1 MapJournalOpen(
    CertifiedTickJournalOpenErrorV1 error) noexcept {
    switch (error) {
        case CertifiedTickJournalOpenErrorV1::kNone:
            return RealtimeCertifiedReaderOpenErrorV1::kNone;
        case CertifiedTickJournalOpenErrorV1::kNullOutput:
            return RealtimeCertifiedReaderOpenErrorV1::kNullOutput;
        case CertifiedTickJournalOpenErrorV1::kInvalidArgument:
            return RealtimeCertifiedReaderOpenErrorV1::
                kInvalidArgument;
        case CertifiedTickJournalOpenErrorV1::kUnsupportedEndian:
            return RealtimeCertifiedReaderOpenErrorV1::
                kUnsupportedEndian;
        case CertifiedTickJournalOpenErrorV1::kDescriptorInvalid:
            return RealtimeCertifiedReaderOpenErrorV1::
                kDescriptorInvalid;
        case CertifiedTickJournalOpenErrorV1::kMappingFailed:
            return RealtimeCertifiedReaderOpenErrorV1::kMappingFailed;
        case CertifiedTickJournalOpenErrorV1::kLayoutInvalid:
            return RealtimeCertifiedReaderOpenErrorV1::kLayoutInvalid;
        case CertifiedTickJournalOpenErrorV1::kSessionMismatch:
            return RealtimeCertifiedReaderOpenErrorV1::
                kSessionMismatch;
        case CertifiedTickJournalOpenErrorV1::kResourceExhausted:
            return RealtimeCertifiedReaderOpenErrorV1::
                kResourceExhausted;
        case CertifiedTickJournalOpenErrorV1::kUnexpectedFailure:
            return RealtimeCertifiedReaderOpenErrorV1::
                kUnexpectedFailure;
    }
    return RealtimeCertifiedReaderOpenErrorV1::kUnexpectedFailure;
}

}  // namespace

RealtimeCertifiedTickHistoryReaderV1::
    RealtimeCertifiedTickHistoryReaderV1(
        std::unique_ptr<CertifiedTickJournalReaderV1> journal)
        noexcept
    : journal_(std::move(journal)) {}

RealtimeCertifiedTickHistoryReaderV1::
    ~RealtimeCertifiedTickHistoryReaderV1() = default;

RealtimeCertifiedReaderOpenErrorV1
RealtimeCertifiedTickHistoryReaderV1::OpenDescriptorForTest(
    int descriptor,
    const RealtimeCertifiedExpectedSessionV1& expected_session,
    std::uint64_t tick_capacity,
    std::uint64_t total_mapping_bytes,
    std::unique_ptr<RealtimeCertifiedTickHistoryReaderV1>* output,
    int* system_error_number) noexcept {
    SetSystemError(system_error_number, 0);
    if (output == nullptr) {
        return RealtimeCertifiedReaderOpenErrorV1::kNullOutput;
    }
    output->reset();
    if (!ExpectedSessionValid(expected_session) ||
        tick_capacity == 0U ||
        total_mapping_bytes <
            kCertifiedTickJournalHeaderBytesV1) {
        return RealtimeCertifiedReaderOpenErrorV1::kInvalidArgument;
    }
    CertifiedTickJournalSessionV1 journal_session{};
    std::memcpy(
        journal_session.run_id.data(),
        expected_session.run_id.data(),
        expected_session.run_id.size());
    journal_session.session_epoch = expected_session.session_epoch;
    journal_session.trade_date = expected_session.trade_date;
    journal_session.tick_capacity = tick_capacity;
    journal_session.total_mapping_bytes = total_mapping_bytes;
    std::unique_ptr<CertifiedTickJournalReaderV1> journal;
    const auto journal_error = CertifiedTickJournalReaderV1::Open(
        descriptor,
        journal_session,
        &journal,
        system_error_number);
    const auto mapped_error = MapJournalOpen(journal_error);
    if (mapped_error != RealtimeCertifiedReaderOpenErrorV1::kNone) {
        return mapped_error;
    }
    if (journal == nullptr) {
        return RealtimeCertifiedReaderOpenErrorV1::
            kUnexpectedFailure;
    }
    auto reader = std::unique_ptr<
        RealtimeCertifiedTickHistoryReaderV1>(
        new (std::nothrow) RealtimeCertifiedTickHistoryReaderV1(
            std::move(journal)));
    if (reader == nullptr) {
        return RealtimeCertifiedReaderOpenErrorV1::
            kResourceExhausted;
    }
    *output = std::move(reader);
    return RealtimeCertifiedReaderOpenErrorV1::kNone;
}

RealtimeCertifiedReaderOpenErrorV1
RealtimeCertifiedTickHistoryReaderV1::Open(
    RealtimeCertifiedReaderOpenOptionsV1 options,
    std::unique_ptr<RealtimeCertifiedTickHistoryReaderV1>* output,
    int* system_error_number) noexcept {
    SetSystemError(system_error_number, 0);
    if (output == nullptr) {
        return RealtimeCertifiedReaderOpenErrorV1::kNullOutput;
    }
    output->reset();
    if (!ExpectedSessionValid(options.expected_session) ||
        options.timeout.count() < 0 ||
        options.timeout > std::chrono::hours(24)) {
        return RealtimeCertifiedReaderOpenErrorV1::kInvalidArgument;
    }
    if (!SocketPathSyntaxValid(options.control_socket_path)) {
        return RealtimeCertifiedReaderOpenErrorV1::
            kSocketPathInvalid;
    }
    const auto& native = options.control_socket_path.native();
    struct stat path_status {};
    if (::lstat(native.c_str(), &path_status) != 0) {
        SetSystemError(system_error_number, errno);
        return RealtimeCertifiedReaderOpenErrorV1::kSocketPathUnsafe;
    }
    if (!S_ISSOCK(path_status.st_mode)) {
        SetSystemError(system_error_number, ENOTSOCK);
        return RealtimeCertifiedReaderOpenErrorV1::kSocketPathUnsafe;
    }
    if (path_status.st_uid != ::geteuid()) {
        SetSystemError(system_error_number, EPERM);
        return RealtimeCertifiedReaderOpenErrorV1::kSocketPathUnsafe;
    }

    int socket_descriptor =
        ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    if (socket_descriptor < 0) {
        SetSystemError(system_error_number, errno);
        return RealtimeCertifiedReaderOpenErrorV1::
            kSocketCreateFailed;
    }
    if (!ConfigureTimeout(socket_descriptor, options.timeout)) {
        const int error = errno;
        CloseDescriptor(&socket_descriptor);
        SetSystemError(system_error_number, error);
        return RealtimeCertifiedReaderOpenErrorV1::
            kSocketCreateFailed;
    }
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::memcpy(
        address.sun_path, native.c_str(), native.size() + 1U);
    if (::connect(
            socket_descriptor,
            reinterpret_cast<const sockaddr*>(&address),
            static_cast<socklen_t>(sizeof(address))) != 0) {
        const int error = errno;
        CloseDescriptor(&socket_descriptor);
        SetSystemError(system_error_number, error);
        return RealtimeCertifiedReaderOpenErrorV1::
            kSocketConnectFailed;
    }
#ifdef SO_PEERCRED
    ucred peer{};
    socklen_t peer_bytes = sizeof(peer);
    const int peer_result = ::getsockopt(
        socket_descriptor,
        SOL_SOCKET,
        SO_PEERCRED,
        &peer,
        &peer_bytes);
    if (peer_result != 0 || peer_bytes != sizeof(peer) ||
        peer.uid != ::geteuid()) {
        const int error = peer_result != 0 ? errno : EPERM;
        CloseDescriptor(&socket_descriptor);
        SetSystemError(system_error_number, error);
        return RealtimeCertifiedReaderOpenErrorV1::kSocketPathUnsafe;
    }
#endif

    RealtimeCertifiedControlRequestV1 request{};
    request.opcode = static_cast<std::uint16_t>(
        RealtimeCertifiedControlOpcodeV1::kGetTickHistory);
    request.nonce = NextNonce();
    ssize_t sent = -1;
    do {
        sent = ::send(
            socket_descriptor,
            &request,
            sizeof(request),
            MSG_NOSIGNAL);
    } while (sent < 0 && errno == EINTR);
    if (sent < 0 ||
        static_cast<std::size_t>(sent) != sizeof(request)) {
        const int error = sent < 0 ? errno : EPROTO;
        CloseDescriptor(&socket_descriptor);
        SetSystemError(system_error_number, error);
        return RealtimeCertifiedReaderOpenErrorV1::
            kRequestSendFailed;
    }

    ReceivedTickHistory received{};
    if (!ReceiveResponse(
            socket_descriptor,
            &received,
            system_error_number)) {
        CloseDescriptor(&socket_descriptor);
        return RealtimeCertifiedReaderOpenErrorV1::
            kResponseReceiveFailed;
    }
    CloseDescriptor(&socket_descriptor);
    if (!ResponseCanonical(received.response, request.nonce)) {
        CloseReceived(&received);
        return RealtimeCertifiedReaderOpenErrorV1::kProtocolError;
    }
    if (!ResponseSessionMatches(
            received.response, options.expected_session)) {
        CloseReceived(&received);
        return RealtimeCertifiedReaderOpenErrorV1::kSessionMismatch;
    }
    const auto service_error = MapStatus(received.response.status);
    if (service_error != RealtimeCertifiedReaderOpenErrorV1::kNone) {
        const bool descriptor_absent =
            received.descriptor_count == 0U;
        CloseReceived(&received);
        return descriptor_absent
                   ? service_error
                   : RealtimeCertifiedReaderOpenErrorV1::
                         kProtocolError;
    }
    if (received.descriptor_count != 1U ||
        received.descriptor < 0 ||
        received.response.mapping_bytes <
            kCertifiedTickJournalHeaderBytesV1 ||
        received.response.tick_capacity == 0U) {
        CloseReceived(&received);
        return RealtimeCertifiedReaderOpenErrorV1::kProtocolError;
    }
    struct stat descriptor_status {};
    const int stat_result =
        ::fstat(received.descriptor, &descriptor_status);
    if (stat_result != 0 || descriptor_status.st_size < 0 ||
        static_cast<std::uint64_t>(descriptor_status.st_size) !=
            received.response.mapping_bytes) {
        const int error = stat_result != 0 ? errno : EPROTO;
        CloseReceived(&received);
        SetSystemError(system_error_number, error);
        return RealtimeCertifiedReaderOpenErrorV1::
            kDescriptorInvalid;
    }
    const auto result = OpenDescriptorForTest(
        received.descriptor,
        options.expected_session,
        received.response.tick_capacity,
        received.response.mapping_bytes,
        output,
        system_error_number);
    CloseReceived(&received);
    return result;
}

CertifiedTickJournalReadResultV1
RealtimeCertifiedTickHistoryReaderV1::ReadStatus(
    CertifiedTickJournalStatusV1* output) const noexcept {
    return journal_ == nullptr
               ? CertifiedTickJournalReadResultV1::kCorrupt
               : journal_->ReadStatus(output);
}

CertifiedTickJournalReadResultV1
RealtimeCertifiedTickHistoryReaderV1::ReadOne(
    std::uint64_t sequence,
    RealtimeCertifiedTickEnvelopeV1* output) const noexcept {
    return journal_ == nullptr
               ? CertifiedTickJournalReadResultV1::kCorrupt
               : journal_->ReadOne(sequence, output);
}

CertifiedTickJournalReadResultV1
RealtimeCertifiedTickHistoryReaderV1::Read(
    std::uint64_t first,
    std::span<RealtimeCertifiedTickEnvelopeV1> output,
    CertifiedTickJournalReadBatchV1* result) const noexcept {
    return journal_ == nullptr
               ? CertifiedTickJournalReadResultV1::kCorrupt
               : journal_->Read(first, output, result);
}

CertifiedTickJournalReadResultV1
RealtimeCertifiedTickHistoryReaderV1::ReadSlots(
    std::uint64_t first,
    std::span<RealtimeCertifiedTickSlotV1> output,
    CertifiedTickJournalReadBatchV1* result) const noexcept {
    return journal_ == nullptr
               ? CertifiedTickJournalReadResultV1::kCorrupt
               : journal_->ReadSlots(first, output, result);
}

CertifiedTickJournalReadResultV1
RealtimeCertifiedTickHistoryReaderV1::ReadSlotBytes(
    std::uint64_t first,
    std::span<std::byte> output,
    CertifiedTickJournalReadBatchV1* result) const noexcept {
    return journal_ == nullptr
               ? CertifiedTickJournalReadResultV1::kCorrupt
               : journal_->ReadSlotBytes(first, output, result);
}

const CertifiedTickJournalSessionV1&
RealtimeCertifiedTickHistoryReaderV1::session() const noexcept {
    static constexpr CertifiedTickJournalSessionV1 empty{};
    return journal_ == nullptr ? empty : journal_->session();
}

}  // namespace l2flow::ipc
