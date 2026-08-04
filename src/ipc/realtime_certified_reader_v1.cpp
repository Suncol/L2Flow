#include "l2flow/ipc/realtime_certified_reader_v1.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <new>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

namespace l2flow::ipc {
namespace {

constexpr std::size_t kReadAttempts = 8U;

enum class StableCopyResult : std::uint8_t {
    kCopied = 0U,
    kNeverPublished,
    kInconsistent,
    kInvalid,
};

template <typename Integer>
[[nodiscard]] std::atomic_ref<Integer> Atomic(
    const Integer& value) noexcept {
    return std::atomic_ref<Integer>(
        const_cast<Integer&>(value));
}

template <typename Value, std::size_t Size>
[[nodiscard]] bool AllZero(
    const std::array<Value, Size>& values) noexcept {
    return std::all_of(
        values.begin(),
        values.end(),
        [](Value value) noexcept { return value == Value{}; });
}

template <typename Value, std::size_t Size>
[[nodiscard]] bool AnyNonzero(
    const std::array<Value, Size>& values) noexcept {
    return !AllZero(values);
}

void SetSystemError(
    int* system_error_number,
    int value) noexcept {
    if (system_error_number != nullptr) {
        *system_error_number = value;
    }
}

[[nodiscard]] bool ExpectedSessionValid(
    const RealtimeCertifiedExpectedSessionV1& expected) noexcept {
    return AnyNonzero(expected.run_id) &&
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

[[nodiscard]] bool StatusCountersValid(
    const RealtimeCertifiedStatusSnapshotV1& status,
    std::uint32_t channel_capacity) noexcept {
    std::uint64_t accounted_messages = 0U;
    if (!realtime_certified_wire_v1_detail::CheckedAdd(
            status.certified_tick_count,
            status.exact_duplicate_message_count,
            &accounted_messages) ||
        accounted_messages >
            status.observed_native_message_count ||
        status.certified_tick_count !=
            status.canonical_apply_frontier ||
        status.gap_recovered_count >
            status.gap_opened_count ||
        status.channel_state_count > channel_capacity) {
        return false;
    }
    std::uint64_t exceptional_channels =
        status.gap_open_channel_count;
    if (!realtime_certified_wire_v1_detail::CheckedAdd(
            exceptional_channels,
            status.catching_up_channel_count,
            &exceptional_channels) ||
        !realtime_certified_wire_v1_detail::CheckedAdd(
            exceptional_channels,
            status.frozen_channel_count,
            &exceptional_channels)) {
        return false;
    }
    return exceptional_channels <= status.channel_state_count;
}

[[nodiscard]] bool StatusStateValid(
    const RealtimeCertifiedStatusSnapshotV1& status) noexcept {
    const bool empty_activity =
        status.canonical_apply_frontier == 0U &&
        status.observed_native_message_count == 0U &&
        status.certified_tick_count == 0U &&
        status.exact_duplicate_message_count == 0U &&
        status.gap_opened_count == 0U &&
        status.gap_recovered_count == 0U &&
        status.conflicting_duplicate_count == 0U &&
        status.resource_exhaustion_count == 0U &&
        status.pending_token_count == 0U &&
        status.gap_open_channel_count == 0U &&
        status.catching_up_channel_count == 0U &&
        status.frozen_channel_count == 0U;

    switch (status.state) {
        case RealtimeCertifiedStateV1::kDisabled:
            return status.correction_epoch == 0U &&
                   empty_activity &&
                   status.channel_state_count == 0U;
        case RealtimeCertifiedStateV1::kNoData:
            return status.correction_epoch != 0U &&
                   empty_activity;
        case RealtimeCertifiedStateV1::kContiguous:
            return status.correction_epoch != 0U &&
                   status.channel_state_count != 0U &&
                   status.gap_open_channel_count == 0U &&
                   status.catching_up_channel_count == 0U &&
                   status.frozen_channel_count == 0U &&
                   status.pending_token_count == 0U;
        case RealtimeCertifiedStateV1::kGapOpen:
            return status.correction_epoch != 0U &&
                   status.gap_open_channel_count != 0U &&
                   status.frozen_channel_count == 0U &&
                   status.pending_token_count != 0U;
        case RealtimeCertifiedStateV1::kCatchingUp:
            return status.correction_epoch != 0U &&
                   status.gap_open_channel_count == 0U &&
                   status.catching_up_channel_count != 0U &&
                   status.frozen_channel_count == 0U &&
                   status.pending_token_count != 0U;
        case RealtimeCertifiedStateV1::kFrozenConflict:
            return status.correction_epoch != 0U &&
                   status.conflicting_duplicate_count != 0U;
        case RealtimeCertifiedStateV1::kFrozenResource:
            return status.correction_epoch != 0U &&
                   status.resource_exhaustion_count != 0U;
        case RealtimeCertifiedStateV1::kStopped:
            return true;
        case RealtimeCertifiedStateV1::kDegraded:
            return status.correction_epoch != 0U &&
                   status.frozen_channel_count != 0U;
    }
    return false;
}

[[nodiscard]] StableCopyResult CopyStatus(
    const RealtimeCertifiedHeaderV1& header,
    RealtimeCertifiedStatusSnapshotV1* output) noexcept {
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
        if (!RealtimeCertifiedPublishTagStableV1(begin)) {
            continue;
        }

        RealtimeCertifiedStatusSnapshotV1 snapshot{};
        snapshot.publish_tag = begin;
        snapshot.heartbeat_monotonic_ns =
            Atomic(header.heartbeat_monotonic_ns)
                .load(std::memory_order_relaxed);
        snapshot.canonical_apply_frontier =
            Atomic(header.canonical_apply_frontier)
                .load(std::memory_order_relaxed);
        snapshot.correction_epoch =
            Atomic(header.correction_epoch)
                .load(std::memory_order_relaxed);
        snapshot.observed_native_message_count =
            Atomic(header.observed_native_message_count)
                .load(std::memory_order_relaxed);
        snapshot.certified_tick_count =
            Atomic(header.certified_tick_count)
                .load(std::memory_order_relaxed);
        snapshot.exact_duplicate_message_count =
            Atomic(header.exact_duplicate_message_count)
                .load(std::memory_order_relaxed);
        snapshot.gap_opened_count =
            Atomic(header.gap_opened_count)
                .load(std::memory_order_relaxed);
        snapshot.gap_recovered_count =
            Atomic(header.gap_recovered_count)
                .load(std::memory_order_relaxed);
        snapshot.conflicting_duplicate_count =
            Atomic(header.conflicting_duplicate_count)
                .load(std::memory_order_relaxed);
        snapshot.resource_exhaustion_count =
            Atomic(header.resource_exhaustion_count)
                .load(std::memory_order_relaxed);
        snapshot.pending_token_count =
            Atomic(header.pending_token_count)
                .load(std::memory_order_relaxed);
        const std::uint32_t state =
            Atomic(header.aggregate_state)
                .load(std::memory_order_relaxed);
        snapshot.channel_state_count =
            Atomic(header.channel_state_count)
                .load(std::memory_order_relaxed);
        snapshot.gap_open_channel_count =
            Atomic(header.gap_open_channel_count)
                .load(std::memory_order_relaxed);
        snapshot.catching_up_channel_count =
            Atomic(header.catching_up_channel_count)
                .load(std::memory_order_relaxed);
        snapshot.frozen_channel_count =
            Atomic(header.frozen_channel_count)
                .load(std::memory_order_relaxed);
        const std::uint32_t reserved_status =
            Atomic(header.reserved_status)
                .load(std::memory_order_relaxed);

        std::atomic_thread_fence(std::memory_order_acq_rel);
        const std::uint64_t end =
            Atomic(header.status_publish_tag)
                .load(std::memory_order_acquire);
        if (begin != end ||
            !RealtimeCertifiedPublishTagStableV1(end)) {
            continue;
        }
        if (snapshot.heartbeat_monotonic_ns == 0U ||
            !RealtimeCertifiedStateValidV1(state) ||
            reserved_status != 0U) {
            return StableCopyResult::kInvalid;
        }
        snapshot.state =
            static_cast<RealtimeCertifiedStateV1>(state);
        if (!StatusCountersValid(
                snapshot, header.channel_state_capacity) ||
            !StatusStateValid(snapshot)) {
            return StableCopyResult::kInvalid;
        }
        *output = snapshot;
        return StableCopyResult::kCopied;
    }
    return StableCopyResult::kInconsistent;
}

void ApplyStatusToHeaderCopy(
    const RealtimeCertifiedStatusSnapshotV1& status,
    RealtimeCertifiedHeaderV1* header) noexcept {
    header->status_publish_tag = status.publish_tag;
    header->heartbeat_monotonic_ns =
        status.heartbeat_monotonic_ns;
    header->canonical_apply_frontier =
        status.canonical_apply_frontier;
    header->correction_epoch = status.correction_epoch;
    header->observed_native_message_count =
        status.observed_native_message_count;
    header->certified_tick_count = status.certified_tick_count;
    header->exact_duplicate_message_count =
        status.exact_duplicate_message_count;
    header->gap_opened_count = status.gap_opened_count;
    header->gap_recovered_count = status.gap_recovered_count;
    header->conflicting_duplicate_count =
        status.conflicting_duplicate_count;
    header->resource_exhaustion_count =
        status.resource_exhaustion_count;
    header->pending_token_count = status.pending_token_count;
    header->aggregate_state =
        static_cast<std::uint32_t>(status.state);
    header->channel_state_count = status.channel_state_count;
    header->gap_open_channel_count =
        status.gap_open_channel_count;
    header->catching_up_channel_count =
        status.catching_up_channel_count;
    header->frozen_channel_count = status.frozen_channel_count;
    header->reserved_status = 0U;
}

[[nodiscard]] RealtimeCertifiedHeaderV1 CopyImmutableHeader(
    const RealtimeCertifiedHeaderV1& source) noexcept {
    RealtimeCertifiedHeaderV1 copy{};
    copy.magic = source.magic;
    copy.abi_major = source.abi_major;
    copy.abi_minor = source.abi_minor;
    copy.header_bytes = source.header_bytes;
    copy.endian_marker = source.endian_marker;
    copy.flags = source.flags;
    copy.total_mapping_bytes = source.total_mapping_bytes;
    copy.run_id = source.run_id;
    copy.session_epoch = source.session_epoch;
    copy.trade_date = source.trade_date;
    copy.reserved_identity = source.reserved_identity;
    copy.latest_offset = source.latest_offset;
    copy.certified_ring_offset = source.certified_ring_offset;
    copy.channel_state_offset = source.channel_state_offset;
    copy.latest_capacity = source.latest_capacity;
    copy.certified_ring_capacity =
        source.certified_ring_capacity;
    copy.channel_state_capacity = source.channel_state_capacity;
    copy.latest_stride = source.latest_stride;
    copy.certified_ring_stride = source.certified_ring_stride;
    copy.channel_state_stride = source.channel_state_stride;
    copy.region_alignment = source.region_alignment;
    copy.reserved_layout0 = source.reserved_layout0;
    copy.reserved_layout = source.reserved_layout;
    copy.reserved = source.reserved;
    return copy;
}

[[nodiscard]] StableCopyResult CopyTickSlot(
    const RealtimeCertifiedTickSlotV1& source,
    RealtimeCertifiedTickEnvelopeV1* output) noexcept {
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
        if (!RealtimeCertifiedPublishTagStableV1(begin)) {
            continue;
        }

        RealtimeCertifiedTickSlotV1 copy{};
        copy.publish_tag = begin;
        for (std::size_t index = 0U;
             index < copy.reserved0.size();
             ++index) {
            copy.reserved0[index] =
                Atomic(source.reserved0[index])
                    .load(std::memory_order_relaxed);
        }
        for (std::size_t index = 0U;
             index < copy.payload_words.size();
             ++index) {
            copy.payload_words[index] =
                Atomic(source.payload_words[index])
                    .load(std::memory_order_relaxed);
        }

        std::atomic_thread_fence(std::memory_order_acq_rel);
        const std::uint64_t end =
            Atomic(source.publish_tag)
                .load(std::memory_order_acquire);
        if (begin != end ||
            !RealtimeCertifiedPublishTagStableV1(end)) {
            continue;
        }
        RealtimeCertifiedTickEnvelopeV1 envelope{};
        if (!RealtimeCertifiedTickSlotDecodeV1(copy, &envelope)) {
            return StableCopyResult::kInvalid;
        }
        *output = envelope;
        return StableCopyResult::kCopied;
    }
    return StableCopyResult::kInconsistent;
}

[[nodiscard]] StableCopyResult CopyChannelState(
    const RealtimeCertifiedChannelStateV1& source,
    RealtimeCertifiedChannelStateV1* output) noexcept {
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
        if (!RealtimeCertifiedPublishTagStableV1(begin)) {
            continue;
        }

        RealtimeCertifiedChannelStateV1 copy{};
        copy.publish_tag = begin;
        copy.feed_epoch =
            Atomic(source.feed_epoch)
                .load(std::memory_order_relaxed);
        copy.origin_sequence =
            Atomic(source.origin_sequence)
                .load(std::memory_order_relaxed);
        copy.observed_contiguous_frontier =
            Atomic(source.observed_contiguous_frontier)
                .load(std::memory_order_relaxed);
        copy.certified_published_frontier =
            Atomic(source.certified_published_frontier)
                .load(std::memory_order_relaxed);
        copy.highest_observed_sequence =
            Atomic(source.highest_observed_sequence)
                .load(std::memory_order_relaxed);
        copy.canonical_apply_frontier =
            Atomic(source.canonical_apply_frontier)
                .load(std::memory_order_relaxed);
        copy.observed_native_message_count =
            Atomic(source.observed_native_message_count)
                .load(std::memory_order_relaxed);
        copy.certified_tick_count =
            Atomic(source.certified_tick_count)
                .load(std::memory_order_relaxed);
        copy.exact_duplicate_message_count =
            Atomic(source.exact_duplicate_message_count)
                .load(std::memory_order_relaxed);
        copy.pending_token_count =
            Atomic(source.pending_token_count)
                .load(std::memory_order_relaxed);
        copy.gap_opened_count =
            Atomic(source.gap_opened_count)
                .load(std::memory_order_relaxed);
        copy.gap_recovered_count =
            Atomic(source.gap_recovered_count)
                .load(std::memory_order_relaxed);
        copy.state =
            Atomic(source.state).load(std::memory_order_relaxed);
        copy.trade_date =
            Atomic(source.trade_date)
                .load(std::memory_order_relaxed);
        copy.channel =
            Atomic(source.channel).load(std::memory_order_relaxed);
        copy.market =
            Atomic(source.market).load(std::memory_order_relaxed);
        copy.channel_correction_epoch =
            Atomic(source.channel_correction_epoch)
                .load(std::memory_order_relaxed);
        for (std::size_t index = 0U;
             index < copy.reserved.size();
             ++index) {
            copy.reserved[index] =
                Atomic(source.reserved[index])
                    .load(std::memory_order_relaxed);
        }

        std::atomic_thread_fence(std::memory_order_acq_rel);
        const std::uint64_t end =
            Atomic(source.publish_tag)
                .load(std::memory_order_acquire);
        if (begin != end ||
            !RealtimeCertifiedPublishTagStableV1(end)) {
            continue;
        }
        if (!RealtimeCertifiedChannelStateCanonicalV1(copy)) {
            return StableCopyResult::kInvalid;
        }
        *output = copy;
        return StableCopyResult::kCopied;
    }
    return StableCopyResult::kInconsistent;
}

[[nodiscard]] RealtimeCertifiedReadResultV1 MapCopyResult(
    StableCopyResult result,
    RealtimeCertifiedReadResultV1 never_published) noexcept {
    switch (result) {
        case StableCopyResult::kCopied:
            return RealtimeCertifiedReadResultV1::kOk;
        case StableCopyResult::kNeverPublished:
            return never_published;
        case StableCopyResult::kInconsistent:
            return RealtimeCertifiedReadResultV1::kInconsistent;
        case StableCopyResult::kInvalid:
            return RealtimeCertifiedReadResultV1::kCorrupt;
    }
    return RealtimeCertifiedReadResultV1::kCorrupt;
}

[[nodiscard]] bool EnvelopeIdentityValid(
    const RealtimeCertifiedTickEnvelopeV1& envelope,
    const RealtimeCertifiedExpectedSessionV1& session,
    std::uint32_t latest_capacity) noexcept {
    return RealtimeCertifiedTickEnvelopeCanonicalV1(envelope) &&
           envelope.payload.common.trade_date ==
               session.trade_date &&
           envelope.payload.common.ordinal < latest_capacity;
}

[[nodiscard]] bool SocketPathSyntaxValid(
    const std::filesystem::path& path) noexcept {
    const auto& native = path.native();
    if (native.empty() || native.front() != '/') {
        return false;
    }
    sockaddr_un address{};
    if (native.size() >= sizeof(address.sun_path)) {
        return false;
    }
    return ::strnlen(native.c_str(), sizeof(address.sun_path)) ==
           native.size();
}

[[nodiscard]] bool ConfigureTimeout(
    int socket_fd,
    std::chrono::milliseconds timeout) noexcept {
    if (timeout.count() == 0) {
        return true;
    }
    timeval value{};
    value.tv_sec = static_cast<time_t>(
        timeout.count() / 1000);
    value.tv_usec = static_cast<suseconds_t>(
        (timeout.count() % 1000) * 1000);
    return ::setsockopt(
               socket_fd,
               SOL_SOCKET,
               SO_RCVTIMEO,
               &value,
               sizeof(value)) == 0 &&
           ::setsockopt(
               socket_fd,
               SOL_SOCKET,
               SO_SNDTIMEO,
               &value,
               sizeof(value)) == 0;
}

[[nodiscard]] std::uint64_t NextNonce() noexcept {
    static std::atomic<std::uint64_t> next{1U};
    const std::uint64_t first =
        next.fetch_add(1U, std::memory_order_relaxed);
    if (first != 0U) {
        return first;
    }
    return next.fetch_add(1U, std::memory_order_relaxed);
}

struct ReceivedSession final {
    RealtimeCertifiedControlResponseV1 response{};
    int descriptor = -1;
    std::size_t descriptor_count = 0U;
};

void CloseReceivedDescriptor(ReceivedSession* received) noexcept {
    if (received != nullptr && received->descriptor >= 0) {
        static_cast<void>(::close(received->descriptor));
        received->descriptor = -1;
    }
}

[[nodiscard]] bool ReceiveSession(
    int socket_fd,
    ReceivedSession* output,
    int* system_error_number) noexcept {
    if (socket_fd < 0 || output == nullptr) {
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

    ssize_t result = -1;
    do {
        result = ::recvmsg(
            socket_fd,
            &message,
#ifdef MSG_CMSG_CLOEXEC
            MSG_CMSG_CLOEXEC
#else
            0
#endif
        );
    } while (result < 0 && errno == EINTR);
    if (result <= 0) {
        SetSystemError(
            system_error_number,
            result < 0 ? errno : ECONNRESET);
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
                static_cast<void>(::close(descriptor));
            }
            ++output->descriptor_count;
        }
    }

    const bool packet_valid =
        static_cast<std::size_t>(result) ==
            sizeof(output->response) &&
        (message.msg_flags & (MSG_TRUNC | MSG_CTRUNC)) == 0;
    if (!packet_valid || !ancillary_valid ||
        output->descriptor_count > 1U) {
        CloseReceivedDescriptor(output);
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
            CloseReceivedDescriptor(output);
            SetSystemError(system_error_number, error);
            return false;
        }
    }
#endif
    return true;
}

[[nodiscard]] bool ResponsePrefixValid(
    const RealtimeCertifiedControlResponseV1& response,
    std::uint64_t nonce) noexcept {
    return response.magic ==
               kRealtimeCertifiedControlResponseMagicV1 &&
           response.abi_major ==
               kRealtimeCertifiedWireMajorV1 &&
           response.abi_minor ==
               kRealtimeCertifiedWireMinorV1 &&
           response.response_bytes == sizeof(response) &&
           response.nonce == nonce &&
           response.reserved0 == 0U &&
           AllZero(response.reserved) &&
           response.status <= static_cast<std::uint16_t>(
               RealtimeCertifiedControlStatusV1::kInternal);
}

[[nodiscard]] bool ErrorResponseCanonical(
    const RealtimeCertifiedControlResponseV1& response,
    const RealtimeCertifiedExpectedSessionV1& expected) noexcept {
    RealtimeCertifiedExpectedSessionV1 actual{};
    actual.run_id = response.run_id;
    actual.session_epoch = response.session_epoch;
    actual.trade_date = response.trade_date;
    return response.mapping_bytes >=
               kRealtimeCertifiedHeaderBytesV1 &&
           ExpectedSessionEqual(actual, expected);
}

[[nodiscard]] RealtimeCertifiedReaderOpenErrorV1 MapServiceStatus(
    std::uint16_t value) noexcept {
    switch (
        static_cast<RealtimeCertifiedControlStatusV1>(value)) {
        case RealtimeCertifiedControlStatusV1::kOk:
            return RealtimeCertifiedReaderOpenErrorV1::kNone;
        case RealtimeCertifiedControlStatusV1::kInvalidRequest:
            return RealtimeCertifiedReaderOpenErrorV1::kProtocolError;
        case RealtimeCertifiedControlStatusV1::kUnavailable:
            return RealtimeCertifiedReaderOpenErrorV1::kUnavailable;
        case RealtimeCertifiedControlStatusV1::kInternal:
            return RealtimeCertifiedReaderOpenErrorV1::kServiceInternal;
    }
    return RealtimeCertifiedReaderOpenErrorV1::kProtocolError;
}

}  // namespace

class RealtimeCertifiedReaderV1::Impl final {
public:
    Impl() = default;
    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;

    ~Impl() {
        if (mapping_ != MAP_FAILED) {
            static_cast<void>(::munmap(mapping_, mapping_bytes_));
            mapping_ = MAP_FAILED;
        }
    }

    [[nodiscard]] static RealtimeCertifiedReaderOpenErrorV1
    OpenDescriptor(
        int descriptor,
        const RealtimeCertifiedExpectedSessionV1& expected,
        std::unique_ptr<Impl>* output,
        int* system_error_number) noexcept {
        if (output == nullptr) {
            return RealtimeCertifiedReaderOpenErrorV1::kNullOutput;
        }
        output->reset();
        if (descriptor < 0 || !ExpectedSessionValid(expected)) {
            return RealtimeCertifiedReaderOpenErrorV1::kInvalidArgument;
        }
        if constexpr (std::endian::native != std::endian::little) {
            return RealtimeCertifiedReaderOpenErrorV1::
                kUnsupportedEndian;
        }

        const int descriptor_flags =
            ::fcntl(descriptor, F_GETFL);
        const int seals = ::fcntl(descriptor, F_GET_SEALS);
        constexpr int required_seals =
            F_SEAL_GROW | F_SEAL_SHRINK |
            F_SEAL_FUTURE_WRITE | F_SEAL_SEAL;
        struct stat descriptor_stat {};
        const int stat_result =
            ::fstat(descriptor, &descriptor_stat);
        if (descriptor_flags < 0 || seals < 0 ||
            (descriptor_flags & O_ACCMODE) != O_RDONLY ||
            (seals & required_seals) != required_seals ||
            stat_result != 0 ||
            !S_ISREG(descriptor_stat.st_mode) ||
            descriptor_stat.st_size <
                static_cast<off_t>(
                    kRealtimeCertifiedHeaderBytesV1)) {
            const int error =
                descriptor_flags < 0 || seals < 0 ||
                        stat_result != 0
                    ? errno
                    : EACCES;
            SetSystemError(system_error_number, error);
            return RealtimeCertifiedReaderOpenErrorV1::
                kDescriptorInvalid;
        }
        const std::uint64_t mapping_bytes =
            static_cast<std::uint64_t>(descriptor_stat.st_size);
        if (mapping_bytes >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max())) {
            return RealtimeCertifiedReaderOpenErrorV1::
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
            return RealtimeCertifiedReaderOpenErrorV1::
                kMappingFailed;
        }
        const auto* const header =
            static_cast<const RealtimeCertifiedHeaderV1*>(mapping);

        RealtimeCertifiedStatusSnapshotV1 status{};
        const StableCopyResult status_result =
            CopyStatus(*header, &status);
        RealtimeCertifiedHeaderV1 header_copy =
            CopyImmutableHeader(*header);
        if (status_result == StableCopyResult::kCopied) {
            ApplyStatusToHeaderCopy(status, &header_copy);
        }
        const bool header_valid =
            status_result == StableCopyResult::kCopied &&
            RealtimeCertifiedHeaderCanonicalV1(header_copy) &&
            header_copy.total_mapping_bytes == mapping_bytes;
        if (!header_valid) {
            static_cast<void>(::munmap(
                mapping,
                static_cast<std::size_t>(mapping_bytes)));
            return RealtimeCertifiedReaderOpenErrorV1::
                kLayoutInvalid;
        }

        RealtimeCertifiedExpectedSessionV1 actual{};
        actual.run_id = header_copy.run_id;
        actual.session_epoch = header_copy.session_epoch;
        actual.trade_date = header_copy.trade_date;
        if (!ExpectedSessionEqual(actual, expected)) {
            static_cast<void>(::munmap(
                mapping,
                static_cast<std::size_t>(mapping_bytes)));
            return RealtimeCertifiedReaderOpenErrorV1::
                kSessionMismatch;
        }

        auto impl = std::unique_ptr<Impl>(
            new (std::nothrow) Impl());
        if (!impl) {
            static_cast<void>(::munmap(
                mapping,
                static_cast<std::size_t>(mapping_bytes)));
            return RealtimeCertifiedReaderOpenErrorV1::
                kResourceExhausted;
        }
        impl->mapping_ = mapping;
        impl->mapping_bytes_ =
            static_cast<std::size_t>(mapping_bytes);
        impl->header_ = header;
        impl->session_ = actual;
        const auto* const base =
            static_cast<const std::byte*>(mapping);
        impl->latest_ = reinterpret_cast<
            const RealtimeCertifiedTickSlotV1*>(
                base + header_copy.latest_offset);
        impl->ring_ = reinterpret_cast<
            const RealtimeCertifiedTickSlotV1*>(
                base + header_copy.certified_ring_offset);
        impl->channels_ = reinterpret_cast<
            const RealtimeCertifiedChannelStateV1*>(
                base + header_copy.channel_state_offset);
        impl->latest_capacity_ = header_copy.latest_capacity;
        impl->ring_capacity_ =
            header_copy.certified_ring_capacity;
        impl->channel_capacity_ =
            header_copy.channel_state_capacity;
        *output = std::move(impl);
        return RealtimeCertifiedReaderOpenErrorV1::kNone;
    }

    [[nodiscard]] RealtimeCertifiedReadResultV1 ReadStatus(
        RealtimeCertifiedStatusSnapshotV1* output) const noexcept {
        if (output == nullptr) {
            return RealtimeCertifiedReadResultV1::kOutOfRange;
        }
        RealtimeCertifiedStatusSnapshotV1 status{};
        const StableCopyResult result =
            CopyStatus(*header_, &status);
        if (result == StableCopyResult::kCopied) {
            *output = status;
        }
        return MapCopyResult(
            result, RealtimeCertifiedReadResultV1::kNoData);
    }

    [[nodiscard]] RealtimeCertifiedReadResultV1 ReadLatest(
        std::size_t ordinal,
        RealtimeCertifiedTickEnvelopeV1* output) const noexcept {
        if (output == nullptr || ordinal >= latest_capacity_) {
            return RealtimeCertifiedReadResultV1::kOutOfRange;
        }
        for (std::size_t attempt = 0U; attempt < kReadAttempts;
             ++attempt) {
            RealtimeCertifiedTickEnvelopeV1 envelope{};
            const StableCopyResult slot_result =
                CopyTickSlot(latest_[ordinal], &envelope);
            if (slot_result != StableCopyResult::kCopied) {
                return MapCopyResult(
                    slot_result,
                    RealtimeCertifiedReadResultV1::kNoData);
            }
            if (!EnvelopeIdentityValid(
                    envelope, session_, latest_capacity_) ||
                envelope.payload.common.ordinal != ordinal) {
                return RealtimeCertifiedReadResultV1::kCorrupt;
            }

            RealtimeCertifiedStatusSnapshotV1 status{};
            const StableCopyResult status_result =
                CopyStatus(*header_, &status);
            if (status_result != StableCopyResult::kCopied) {
                return MapCopyResult(
                    status_result,
                    RealtimeCertifiedReadResultV1::kNoData);
            }
            if (envelope.canonical_apply_sequence >
                    status.canonical_apply_frontier ||
                envelope.correction_epoch >
                    status.correction_epoch) {
                continue;
            }
            *output = envelope;
            return RealtimeCertifiedReadResultV1::kOk;
        }
        return RealtimeCertifiedReadResultV1::kInconsistent;
    }

    [[nodiscard]] RealtimeCertifiedReadResultV1 ReadCanonical(
        std::uint64_t canonical_apply_sequence,
        RealtimeCertifiedTickEnvelopeV1* output) const noexcept {
        if (output == nullptr || canonical_apply_sequence == 0U) {
            return RealtimeCertifiedReadResultV1::kOutOfRange;
        }
        for (std::size_t attempt = 0U; attempt < kReadAttempts;
             ++attempt) {
            RealtimeCertifiedStatusSnapshotV1 status{};
            const StableCopyResult status_result =
                CopyStatus(*header_, &status);
            if (status_result != StableCopyResult::kCopied) {
                return MapCopyResult(
                    status_result,
                    RealtimeCertifiedReadResultV1::kNoData);
            }
            if (canonical_apply_sequence >
                status.canonical_apply_frontier) {
                return RealtimeCertifiedReadResultV1::
                    kNotYetPublished;
            }
            const std::uint64_t oldest_retained =
                status.canonical_apply_frontier > ring_capacity_
                    ? status.canonical_apply_frontier -
                          ring_capacity_ +
                          1U
                    : 1U;
            if (canonical_apply_sequence < oldest_retained) {
                return RealtimeCertifiedReadResultV1::kOverwritten;
            }

            std::uint32_t index = 0U;
            if (!RealtimeCertifiedRingSlotIndexV1(
                    canonical_apply_sequence,
                    ring_capacity_,
                    &index)) {
                return RealtimeCertifiedReadResultV1::kCorrupt;
            }
            RealtimeCertifiedTickEnvelopeV1 envelope{};
            const StableCopyResult slot_result =
                CopyTickSlot(ring_[index], &envelope);
            if (slot_result == StableCopyResult::kInconsistent) {
                continue;
            }
            if (slot_result == StableCopyResult::kNeverPublished) {
                continue;
            }
            if (slot_result == StableCopyResult::kInvalid) {
                return RealtimeCertifiedReadResultV1::kCorrupt;
            }
            if (!EnvelopeIdentityValid(
                    envelope, session_, latest_capacity_)) {
                return RealtimeCertifiedReadResultV1::kCorrupt;
            }
            if (envelope.canonical_apply_sequence >
                canonical_apply_sequence) {
                // The sole writer publishes a slot before advancing the
                // header frontier. A newer envelope beyond the status copy is
                // an in-flight, still-hidden commit, not evidence that the
                // requested position was already outside the *published*
                // retention window. Retry and classify only against a newer
                // coherent status snapshot.
                if (envelope.canonical_apply_sequence >
                    status.canonical_apply_frontier) {
                    continue;
                }
                return RealtimeCertifiedReadResultV1::kOverwritten;
            }
            if (envelope.canonical_apply_sequence <
                canonical_apply_sequence) {
                continue;
            }
            if (envelope.correction_epoch >
                status.correction_epoch) {
                continue;
            }
            *output = envelope;
            return RealtimeCertifiedReadResultV1::kOk;
        }
        return RealtimeCertifiedReadResultV1::kInconsistent;
    }

    [[nodiscard]] RealtimeCertifiedReadResultV1 ReadChannelState(
        std::size_t row_index,
        RealtimeCertifiedChannelStateV1* output) const noexcept {
        if (output == nullptr || row_index >= channel_capacity_) {
            return RealtimeCertifiedReadResultV1::kOutOfRange;
        }
        for (std::size_t attempt = 0U; attempt < kReadAttempts;
             ++attempt) {
            RealtimeCertifiedStatusSnapshotV1 status{};
            const StableCopyResult status_result =
                CopyStatus(*header_, &status);
            if (status_result != StableCopyResult::kCopied) {
                return MapCopyResult(
                    status_result,
                    RealtimeCertifiedReadResultV1::kNoData);
            }
            if (row_index >= status.channel_state_count) {
                return RealtimeCertifiedReadResultV1::kNoData;
            }

            RealtimeCertifiedChannelStateV1 row{};
            const StableCopyResult row_result =
                CopyChannelState(channels_[row_index], &row);
            if (row_result == StableCopyResult::kInconsistent) {
                continue;
            }
            if (row_result != StableCopyResult::kCopied) {
                return MapCopyResult(
                    row_result,
                    RealtimeCertifiedReadResultV1::kNoData);
            }
            std::atomic_thread_fence(std::memory_order_acq_rel);
            const std::uint64_t status_end =
                Atomic(header_->status_publish_tag)
                    .load(std::memory_order_acquire);
            if (status_end != status.publish_tag ||
                !RealtimeCertifiedPublishTagStableV1(status_end)) {
                continue;
            }
            if (row.publish_tag > status.publish_tag) {
                // The sole writer prepares changed rows at the next aggregate
                // stable tag before committing that same tag in the header.
                // A future row is therefore valid but not externally visible.
                continue;
            }
            if (row.trade_date != session_.trade_date) {
                return RealtimeCertifiedReadResultV1::kCorrupt;
            }
            *output = row;
            return RealtimeCertifiedReadResultV1::kOk;
        }
        return RealtimeCertifiedReadResultV1::kInconsistent;
    }

    void* mapping_ = MAP_FAILED;
    std::size_t mapping_bytes_ = 0U;
    const RealtimeCertifiedHeaderV1* header_ = nullptr;
    const RealtimeCertifiedTickSlotV1* latest_ = nullptr;
    const RealtimeCertifiedTickSlotV1* ring_ = nullptr;
    const RealtimeCertifiedChannelStateV1* channels_ = nullptr;
    RealtimeCertifiedExpectedSessionV1 session_{};
    std::uint32_t latest_capacity_ = 0U;
    std::uint32_t ring_capacity_ = 0U;
    std::uint32_t channel_capacity_ = 0U;
};

std::string_view RealtimeCertifiedReaderOpenErrorNameV1(
    RealtimeCertifiedReaderOpenErrorV1 error) noexcept {
    switch (error) {
        case RealtimeCertifiedReaderOpenErrorV1::kNone:
            return "NONE";
        case RealtimeCertifiedReaderOpenErrorV1::kNullOutput:
            return "NULL_OUTPUT";
        case RealtimeCertifiedReaderOpenErrorV1::kInvalidArgument:
            return "INVALID_ARGUMENT";
        case RealtimeCertifiedReaderOpenErrorV1::kUnsupportedEndian:
            return "UNSUPPORTED_ENDIAN";
        case RealtimeCertifiedReaderOpenErrorV1::kSocketPathInvalid:
            return "SOCKET_PATH_INVALID";
        case RealtimeCertifiedReaderOpenErrorV1::kSocketPathUnsafe:
            return "SOCKET_PATH_UNSAFE";
        case RealtimeCertifiedReaderOpenErrorV1::kSocketCreateFailed:
            return "SOCKET_CREATE_FAILED";
        case RealtimeCertifiedReaderOpenErrorV1::kSocketConnectFailed:
            return "SOCKET_CONNECT_FAILED";
        case RealtimeCertifiedReaderOpenErrorV1::kRequestSendFailed:
            return "REQUEST_SEND_FAILED";
        case RealtimeCertifiedReaderOpenErrorV1::kResponseReceiveFailed:
            return "RESPONSE_RECEIVE_FAILED";
        case RealtimeCertifiedReaderOpenErrorV1::kProtocolError:
            return "PROTOCOL_ERROR";
        case RealtimeCertifiedReaderOpenErrorV1::kUnavailable:
            return "UNAVAILABLE";
        case RealtimeCertifiedReaderOpenErrorV1::kServiceInternal:
            return "SERVICE_INTERNAL";
        case RealtimeCertifiedReaderOpenErrorV1::kDescriptorInvalid:
            return "DESCRIPTOR_INVALID";
        case RealtimeCertifiedReaderOpenErrorV1::kMappingFailed:
            return "MAPPING_FAILED";
        case RealtimeCertifiedReaderOpenErrorV1::kLayoutInvalid:
            return "LAYOUT_INVALID";
        case RealtimeCertifiedReaderOpenErrorV1::kSessionMismatch:
            return "SESSION_MISMATCH";
        case RealtimeCertifiedReaderOpenErrorV1::kResourceExhausted:
            return "RESOURCE_EXHAUSTED";
        case RealtimeCertifiedReaderOpenErrorV1::kUnexpectedFailure:
            return "UNEXPECTED_FAILURE";
    }
    return "UNKNOWN";
}

std::string_view RealtimeCertifiedReadResultNameV1(
    RealtimeCertifiedReadResultV1 result) noexcept {
    switch (result) {
        case RealtimeCertifiedReadResultV1::kOk:
            return "OK";
        case RealtimeCertifiedReadResultV1::kNoData:
            return "NO_DATA";
        case RealtimeCertifiedReadResultV1::kNotYetPublished:
            return "NOT_YET_PUBLISHED";
        case RealtimeCertifiedReadResultV1::kOverwritten:
            return "OVERWRITTEN";
        case RealtimeCertifiedReadResultV1::kOutOfRange:
            return "OUT_OF_RANGE";
        case RealtimeCertifiedReadResultV1::kInconsistent:
            return "INCONSISTENT";
        case RealtimeCertifiedReadResultV1::kCorrupt:
            return "CORRUPT";
    }
    return "UNKNOWN";
}

RealtimeCertifiedReaderV1::RealtimeCertifiedReaderV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

RealtimeCertifiedReaderV1::~RealtimeCertifiedReaderV1() = default;

RealtimeCertifiedReaderOpenErrorV1
RealtimeCertifiedReaderV1::OpenDescriptorForTest(
    int descriptor,
    const RealtimeCertifiedExpectedSessionV1& expected_session,
    std::unique_ptr<RealtimeCertifiedReaderV1>* output,
    int* system_error_number) noexcept {
    SetSystemError(system_error_number, 0);
    if (output == nullptr) {
        return RealtimeCertifiedReaderOpenErrorV1::kNullOutput;
    }
    output->reset();

    std::unique_ptr<Impl> impl;
    const RealtimeCertifiedReaderOpenErrorV1 result =
        Impl::OpenDescriptor(
            descriptor,
            expected_session,
            &impl,
            system_error_number);
    if (result != RealtimeCertifiedReaderOpenErrorV1::kNone) {
        return result;
    }
    auto reader = std::unique_ptr<RealtimeCertifiedReaderV1>(
        new (std::nothrow)
            RealtimeCertifiedReaderV1(std::move(impl)));
    if (!reader) {
        return RealtimeCertifiedReaderOpenErrorV1::
            kResourceExhausted;
    }
    *output = std::move(reader);
    return RealtimeCertifiedReaderOpenErrorV1::kNone;
}

RealtimeCertifiedReaderOpenErrorV1
RealtimeCertifiedReaderV1::Open(
    RealtimeCertifiedReaderOpenOptionsV1 options,
    std::unique_ptr<RealtimeCertifiedReaderV1>* output,
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
    struct stat path_stat {};
    if (::lstat(native.c_str(), &path_stat) != 0 ||
        !S_ISSOCK(path_stat.st_mode) ||
        path_stat.st_uid != ::geteuid()) {
        SetSystemError(system_error_number, errno);
        return RealtimeCertifiedReaderOpenErrorV1::
            kSocketPathUnsafe;
    }

    const int socket_fd =
        ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    if (socket_fd < 0) {
        SetSystemError(system_error_number, errno);
        return RealtimeCertifiedReaderOpenErrorV1::
            kSocketCreateFailed;
    }
    if (!ConfigureTimeout(socket_fd, options.timeout)) {
        const int error = errno;
        static_cast<void>(::close(socket_fd));
        SetSystemError(system_error_number, error);
        return RealtimeCertifiedReaderOpenErrorV1::
            kSocketCreateFailed;
    }

    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::memcpy(
        address.sun_path,
        native.c_str(),
        native.size() + 1U);
    if (::connect(
            socket_fd,
            reinterpret_cast<const sockaddr*>(&address),
            static_cast<socklen_t>(sizeof(address))) != 0) {
        const int error = errno;
        static_cast<void>(::close(socket_fd));
        SetSystemError(system_error_number, error);
        return RealtimeCertifiedReaderOpenErrorV1::
            kSocketConnectFailed;
    }

#ifdef SO_PEERCRED
    ucred peer{};
    socklen_t peer_bytes = sizeof(peer);
    const int peer_result = ::getsockopt(
            socket_fd,
            SOL_SOCKET,
            SO_PEERCRED,
            &peer,
            &peer_bytes);
    if (peer_result != 0 ||
        peer_bytes != sizeof(peer) ||
        peer.uid != ::geteuid()) {
        const int error = peer_result != 0 ? errno : EPERM;
        static_cast<void>(::close(socket_fd));
        SetSystemError(system_error_number, error);
        return RealtimeCertifiedReaderOpenErrorV1::
            kSocketPathUnsafe;
    }
#endif

    RealtimeCertifiedControlRequestV1 request{};
    request.nonce = NextNonce();
    ssize_t sent = -1;
    do {
        sent = ::send(
            socket_fd,
            &request,
            sizeof(request),
            MSG_NOSIGNAL);
    } while (sent < 0 && errno == EINTR);
    if (sent < 0 ||
        static_cast<std::size_t>(sent) != sizeof(request)) {
        const int error = sent < 0 ? errno : EPROTO;
        static_cast<void>(::close(socket_fd));
        SetSystemError(system_error_number, error);
        return RealtimeCertifiedReaderOpenErrorV1::
            kRequestSendFailed;
    }

    ReceivedSession received{};
    if (!ReceiveSession(
            socket_fd, &received, system_error_number)) {
        static_cast<void>(::close(socket_fd));
        return RealtimeCertifiedReaderOpenErrorV1::
            kResponseReceiveFailed;
    }
    static_cast<void>(::close(socket_fd));

    if (!ResponsePrefixValid(received.response, request.nonce)) {
        CloseReceivedDescriptor(&received);
        return RealtimeCertifiedReaderOpenErrorV1::kProtocolError;
    }
    const auto service_result =
        MapServiceStatus(received.response.status);
    if (service_result !=
        RealtimeCertifiedReaderOpenErrorV1::kNone) {
        const bool canonical =
            received.descriptor_count == 0U &&
            ErrorResponseCanonical(
                received.response, options.expected_session);
        CloseReceivedDescriptor(&received);
        return canonical
                   ? service_result
                   : RealtimeCertifiedReaderOpenErrorV1::
                         kProtocolError;
    }

    RealtimeCertifiedExpectedSessionV1 response_session{};
    response_session.run_id = received.response.run_id;
    response_session.session_epoch =
        received.response.session_epoch;
    response_session.trade_date = received.response.trade_date;
    if (!ExpectedSessionEqual(
            response_session, options.expected_session)) {
        CloseReceivedDescriptor(&received);
        return RealtimeCertifiedReaderOpenErrorV1::
            kSessionMismatch;
    }
    if (received.descriptor_count != 1U ||
        received.descriptor < 0 ||
        received.response.mapping_bytes <
            kRealtimeCertifiedHeaderBytesV1) {
        CloseReceivedDescriptor(&received);
        return RealtimeCertifiedReaderOpenErrorV1::kProtocolError;
    }

    struct stat descriptor_stat {};
    const int stat_result =
        ::fstat(received.descriptor, &descriptor_stat);
    if (stat_result != 0 ||
        descriptor_stat.st_size < 0 ||
        static_cast<std::uint64_t>(descriptor_stat.st_size) !=
            received.response.mapping_bytes) {
        const int error = stat_result != 0 ? errno : EPROTO;
        CloseReceivedDescriptor(&received);
        SetSystemError(system_error_number, error);
        return RealtimeCertifiedReaderOpenErrorV1::
            kDescriptorInvalid;
    }

    const auto result = OpenDescriptorForTest(
        received.descriptor,
        options.expected_session,
        output,
        system_error_number);
    CloseReceivedDescriptor(&received);
    return result;
}

RealtimeCertifiedReadResultV1
RealtimeCertifiedReaderV1::ReadStatus(
    RealtimeCertifiedStatusSnapshotV1* output) const noexcept {
    return impl_ != nullptr
               ? impl_->ReadStatus(output)
               : RealtimeCertifiedReadResultV1::kCorrupt;
}

RealtimeCertifiedReadResultV1
RealtimeCertifiedReaderV1::ReadLatest(
    std::size_t ordinal,
    RealtimeCertifiedTickEnvelopeV1* output) const noexcept {
    return impl_ != nullptr
               ? impl_->ReadLatest(ordinal, output)
               : RealtimeCertifiedReadResultV1::kCorrupt;
}

RealtimeCertifiedReadResultV1
RealtimeCertifiedReaderV1::ReadCanonical(
    std::uint64_t canonical_apply_sequence,
    RealtimeCertifiedTickEnvelopeV1* output) const noexcept {
    return impl_ != nullptr
               ? impl_->ReadCanonical(
                     canonical_apply_sequence, output)
               : RealtimeCertifiedReadResultV1::kCorrupt;
}

RealtimeCertifiedReadResultV1
RealtimeCertifiedReaderV1::ReadChannelState(
    std::size_t row_index,
    RealtimeCertifiedChannelStateV1* output) const noexcept {
    return impl_ != nullptr
               ? impl_->ReadChannelState(row_index, output)
               : RealtimeCertifiedReadResultV1::kCorrupt;
}

const RealtimeCertifiedExpectedSessionV1&
RealtimeCertifiedReaderV1::session() const noexcept {
    return impl_->session_;
}

std::uint32_t
RealtimeCertifiedReaderV1::latest_capacity() const noexcept {
    return impl_->latest_capacity_;
}

std::uint32_t
RealtimeCertifiedReaderV1::certified_ring_capacity()
    const noexcept {
    return impl_->ring_capacity_;
}

std::uint32_t
RealtimeCertifiedReaderV1::channel_state_capacity()
    const noexcept {
    return impl_->channel_capacity_;
}

static_assert(
    std::atomic_ref<std::uint8_t>::is_always_lock_free,
    "Certified reader requires lock-free 8-bit atomic_ref");
static_assert(
    std::atomic_ref<std::uint32_t>::is_always_lock_free,
    "Certified reader requires lock-free 32-bit atomic_ref");
static_assert(
    std::atomic_ref<std::uint64_t>::is_always_lock_free,
    "Certified reader requires lock-free 64-bit atomic_ref");
static_assert(
    std::atomic_ref<std::int64_t>::is_always_lock_free,
    "Certified reader requires lock-free signed 64-bit atomic_ref");

}  // namespace l2flow::ipc
