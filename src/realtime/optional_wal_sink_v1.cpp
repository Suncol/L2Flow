#include "l2flow/realtime/optional_wal_sink_v1.h"

#include "l2flow/common/crc32c.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <mutex>
#include <new>
#include <semaphore>
#include <span>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace l2flow::realtime {
namespace {

inline constexpr std::size_t kRecordBytesOffset = 0U;
inline constexpr std::size_t kRecordMagicOffset = 4U;
inline constexpr std::size_t kRecordVersionOffset = 8U;
inline constexpr std::size_t kRecordPrefixBytesOffset = 10U;
inline constexpr std::size_t kRecordBodyBytesOffset = 12U;
inline constexpr std::size_t kRecordSourceSlotOffset = 16U;
inline constexpr std::size_t kRecordRunIdOffset = 20U;
inline constexpr std::size_t kRecordGlobalSequenceOffset = 36U;
inline constexpr std::size_t kRecordSourceSequenceOffset = 44U;
inline constexpr std::size_t kRecordRealtimeOffset = 52U;
inline constexpr std::size_t kRecordMonotonicOffset = 60U;
inline constexpr std::size_t kRecordVendorHeadOffset = 68U;

static_assert(
    kRecordVendorHeadOffset + l2flow::sdk::kVendorHeadBytes ==
    kOptionalWalRecordPrefixBytesV1);
static_assert(kOptionalWalRecordPrefixBytesV1 <=
              std::numeric_limits<std::uint16_t>::max());

void StoreU16Little(
    std::span<std::byte> output,
    std::size_t offset,
    std::uint16_t value) noexcept {
    output[offset] = static_cast<std::byte>(value & 0xffU);
    output[offset + 1U] =
        static_cast<std::byte>((value >> 8U) & 0xffU);
}

void StoreU32Little(
    std::span<std::byte> output,
    std::size_t offset,
    std::uint32_t value) noexcept {
    for (std::size_t index = 0U; index < 4U; ++index) {
        output[offset + index] = static_cast<std::byte>(
            (value >> static_cast<unsigned int>(index * 8U)) & 0xffU);
    }
}

void StoreU64Little(
    std::span<std::byte> output,
    std::size_t offset,
    std::uint64_t value) noexcept {
    for (std::size_t index = 0U; index < 8U; ++index) {
        output[offset + index] = static_cast<std::byte>(
            (value >> static_cast<unsigned int>(index * 8U)) & 0xffU);
    }
}

void SaturatingIncrement(
    std::atomic<std::uint64_t>* value) noexcept {
    std::uint64_t current = value->load(std::memory_order_relaxed);
    while (current != std::numeric_limits<std::uint64_t>::max() &&
           !value->compare_exchange_weak(
               current,
               current + 1U,
               std::memory_order_relaxed,
               std::memory_order_relaxed)) {
    }
}

[[nodiscard]] std::uint64_t EncodeFailure(
    OptionalWalFailureKindV1 kind,
    int error_number) noexcept {
    const std::uint64_t encoded_errno = static_cast<std::uint64_t>(
        static_cast<std::uint32_t>(std::max(error_number, 0)));
    return (encoded_errno << 32U) |
           static_cast<std::uint64_t>(kind);
}

[[nodiscard]] OptionalWalFailureKindV1 DecodeFailureKind(
    std::uint64_t failure) noexcept {
    return static_cast<OptionalWalFailureKindV1>(
        static_cast<std::uint8_t>(failure & 0xffU));
}

[[nodiscard]] int DecodeFailureErrno(
    std::uint64_t failure) noexcept {
    const std::uint32_t encoded =
        static_cast<std::uint32_t>(failure >> 32U);
    if (encoded > static_cast<std::uint32_t>(
                      std::numeric_limits<int>::max())) {
        return std::numeric_limits<int>::max();
    }
    return static_cast<int>(encoded);
}

class ProducerGateExit final {
public:
    explicit ProducerGateExit(std::atomic_flag* gate) noexcept
        : gate_(gate) {}

    ~ProducerGateExit() noexcept {
        gate_->clear(std::memory_order_release);
    }

    ProducerGateExit(const ProducerGateExit&) = delete;
    ProducerGateExit& operator=(const ProducerGateExit&) = delete;

private:
    std::atomic_flag* gate_;
};

}  // namespace

std::string_view OptionalWalCreateErrorNameV1(
    OptionalWalCreateErrorV1 error) noexcept {
    switch (error) {
        case OptionalWalCreateErrorV1::kNone:
            return "none";
        case OptionalWalCreateErrorV1::kNullOutput:
            return "null_output";
        case OptionalWalCreateErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case OptionalWalCreateErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

std::string_view OptionalWalEnqueueResultNameV1(
    OptionalWalEnqueueResultV1 result) noexcept {
    switch (result) {
        case OptionalWalEnqueueResultV1::kAccepted:
            return "accepted";
        case OptionalWalEnqueueResultV1::kDisabled:
            return "disabled";
        case OptionalWalEnqueueResultV1::kInvalidMessage:
            return "invalid_message";
        case OptionalWalEnqueueResultV1::kQueueFull:
            return "queue_full";
        case OptionalWalEnqueueResultV1::kConcurrentProducer:
            return "concurrent_producer";
        case OptionalWalEnqueueResultV1::kStopped:
            return "stopped";
        case OptionalWalEnqueueResultV1::kWriterFailed:
            return "writer_failed";
    }
    return "unknown";
}

std::string_view OptionalWalFailureKindNameV1(
    OptionalWalFailureKindV1 kind) noexcept {
    switch (kind) {
        case OptionalWalFailureKindV1::kNone:
            return "none";
        case OptionalWalFailureKindV1::kOpen:
            return "open";
        case OptionalWalFailureKindV1::kNotRegularFile:
            return "not_regular_file";
        case OptionalWalFailureKindV1::kThreadStart:
            return "thread_start";
        case OptionalWalFailureKindV1::kRecordTooLarge:
            return "record_too_large";
        case OptionalWalFailureKindV1::kQueueCounterExhausted:
            return "queue_counter_exhausted";
        case OptionalWalFailureKindV1::kWrite:
            return "write";
        case OptionalWalFailureKindV1::kSync:
            return "sync";
        case OptionalWalFailureKindV1::kClose:
            return "close";
    }
    return "unknown";
}

class OptionalWalSinkV1::Impl final {
public:
    explicit Impl(OptionalWalSinkConfigV1 config)
        : config_(std::move(config)),
          queue_(config_.enabled ? config_.queue_capacity : 0U) {}

    ~Impl() { StopAndDrain(); }

    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;

    void Start() noexcept {
        if (!config_.enabled) {
            finished_.store(true, std::memory_order_release);
            return;
        }
        if (!OpenFile()) {
            finished_.store(true, std::memory_order_release);
            return;
        }
        try {
            worker_ = std::thread([this] { Run(); });
        } catch (...) {
            Trip(OptionalWalFailureKindV1::kThreadStart, EAGAIN);
            CloseFile();
            finished_.store(true, std::memory_order_release);
            return;
        }
        accepting_.store(true, std::memory_order_release);
    }

    [[nodiscard]] OptionalWalEnqueueResultV1 TryEnqueue(
        OwnedIngressMessageHandleV1 message) noexcept {
        if (!config_.enabled) {
            return OptionalWalEnqueueResultV1::kDisabled;
        }
        if (failure_.load(std::memory_order_acquire) != 0U) {
            RejectCoverage();
            return OptionalWalEnqueueResultV1::kWriterFailed;
        }
        if (!accepting_.load(std::memory_order_acquire)) {
            return OptionalWalEnqueueResultV1::kStopped;
        }
        if (!message) {
            RejectCoverage();
            return OptionalWalEnqueueResultV1::kInvalidMessage;
        }
        if (producer_gate_.test_and_set(std::memory_order_acquire)) {
            if (failure_.load(std::memory_order_acquire) != 0U) {
                RejectCoverage();
                return OptionalWalEnqueueResultV1::kWriterFailed;
            }
            if (!accepting_.load(std::memory_order_acquire)) {
                return OptionalWalEnqueueResultV1::kStopped;
            }
            RejectCoverage();
            return OptionalWalEnqueueResultV1::kConcurrentProducer;
        }
        const ProducerGateExit clear_gate(&producer_gate_);

        if (failure_.load(std::memory_order_acquire) != 0U) {
            RejectCoverage();
            return OptionalWalEnqueueResultV1::kWriterFailed;
        }
        if (!accepting_.load(std::memory_order_acquire)) {
            return OptionalWalEnqueueResultV1::kStopped;
        }

        const std::uint64_t write =
            write_position_.load(std::memory_order_relaxed);
        const std::uint64_t read =
            read_position_.load(std::memory_order_acquire);
        if (write == std::numeric_limits<std::uint64_t>::max()) {
            Trip(OptionalWalFailureKindV1::kQueueCounterExhausted, 0);
            RejectCoverage();
            return OptionalWalEnqueueResultV1::kWriterFailed;
        }
        const std::uint64_t occupied = write - read;
        if (occupied >= static_cast<std::uint64_t>(queue_.size())) {
            RejectCoverage();
            return OptionalWalEnqueueResultV1::kQueueFull;
        }

        const std::size_t index = static_cast<std::size_t>(
            write % static_cast<std::uint64_t>(queue_.size()));
        queue_[index] = std::move(message);
        write_position_.store(write + 1U, std::memory_order_release);
        SaturatingIncrement(&accepted_records_);
        available_.release();
        return OptionalWalEnqueueResultV1::kAccepted;
    }

    void StopAndDrain() noexcept {
        std::lock_guard<std::mutex> stop_lock(stop_mutex_);
        if (!config_.enabled) {
            finished_.store(true, std::memory_order_release);
            return;
        }

        accepting_.store(false, std::memory_order_release);
        while (producer_gate_.test_and_set(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        producer_gate_.clear(std::memory_order_release);

        const bool was_requested =
            stop_requested_.exchange(true, std::memory_order_acq_rel);
        if (!was_requested && worker_.joinable()) {
            available_.release();
        }
        if (worker_.joinable()) {
            worker_.join();
        }
        if (!finished_.load(std::memory_order_acquire)) {
            CloseFile();
            finished_.store(true, std::memory_order_release);
        }
    }

    [[nodiscard]] OptionalWalSnapshotV1 Snapshot() const noexcept {
        OptionalWalSnapshotV1 result{};
        // A true acquire observes every final counter/failure publication
        // sequenced before the writer's release-store of finished_. Reading
        // this first prevents a "finished" snapshot with stale progress.
        result.finished = finished_.load(std::memory_order_acquire);
        result.accepted_records =
            accepted_records_.load(std::memory_order_acquire);
        result.written_records =
            written_records_.load(std::memory_order_acquire);
        result.rejected_records =
            rejected_records_.load(std::memory_order_acquire);
        result.abandoned_records =
            abandoned_records_.load(std::memory_order_acquire);
        const std::uint64_t failure =
            failure_.load(std::memory_order_acquire);
        result.failure_kind = DecodeFailureKind(failure);
        result.error_number = DecodeFailureErrno(failure);
        result.enabled = config_.enabled;
        result.accepting =
            accepting_.load(std::memory_order_acquire);
        result.stop_requested =
            stop_requested_.load(std::memory_order_acquire);
        result.coverage_lost =
            coverage_lost_.load(std::memory_order_acquire);
        return result;
    }

private:
    [[nodiscard]] bool OpenFile() noexcept {
        // O_NONBLOCK makes inspection of an existing FIFO/device safe. The
        // descriptor is opened without O_TRUNC, proven regular with fstat,
        // then (only for the explicit replacement mode) truncated.
        int flags = O_RDWR | O_CREAT | O_CLOEXEC | O_NONBLOCK;
#ifdef O_NOFOLLOW
        flags |= O_NOFOLLOW;
#endif
        if (!config_.replace_existing) {
            flags |= O_EXCL;
        }
        const int opened = ::open(config_.path.c_str(), flags, 0600);
        if (opened < 0) {
            Trip(OptionalWalFailureKindV1::kOpen, errno);
            return false;
        }

        struct stat status {};
        if (::fstat(opened, &status) != 0) {
            const int saved_errno = errno;
            static_cast<void>(::close(opened));
            Trip(OptionalWalFailureKindV1::kOpen, saved_errno);
            return false;
        }
        if (!S_ISREG(status.st_mode)) {
            static_cast<void>(::close(opened));
            Trip(OptionalWalFailureKindV1::kNotRegularFile, EINVAL);
            return false;
        }
        const int status_flags = ::fcntl(opened, F_GETFL);
        if (status_flags < 0 ||
            ::fcntl(opened, F_SETFL, status_flags & ~O_NONBLOCK) != 0) {
            const int saved_errno = errno;
            static_cast<void>(::close(opened));
            Trip(OptionalWalFailureKindV1::kOpen, saved_errno);
            return false;
        }
        if (config_.replace_existing && ::ftruncate(opened, 0) != 0) {
            const int saved_errno = errno;
            static_cast<void>(::close(opened));
            Trip(OptionalWalFailureKindV1::kOpen, saved_errno);
            return false;
        }
        fd_ = opened;
        return true;
    }

    void Trip(OptionalWalFailureKindV1 kind, int error_number) noexcept {
        std::uint64_t expected = 0U;
        static_cast<void>(failure_.compare_exchange_strong(
            expected,
            EncodeFailure(kind, error_number),
            std::memory_order_acq_rel,
            std::memory_order_acquire));
        coverage_lost_.store(true, std::memory_order_release);
        accepting_.store(false, std::memory_order_release);
    }

    void RejectCoverage() noexcept {
        SaturatingIncrement(&rejected_records_);
        coverage_lost_.store(true, std::memory_order_release);
    }

    [[nodiscard]] bool TryPop(
        OwnedIngressMessageHandleV1* output) noexcept {
        const std::uint64_t read =
            read_position_.load(std::memory_order_relaxed);
        const std::uint64_t write =
            write_position_.load(std::memory_order_acquire);
        if (read == write) {
            return false;
        }
        const std::size_t index = static_cast<std::size_t>(
            read % static_cast<std::uint64_t>(queue_.size()));
        *output = std::move(queue_[index]);
        queue_[index].reset();
        read_position_.store(read + 1U, std::memory_order_release);
        return true;
    }

    void Run() noexcept {
        for (;;) {
            available_.acquire();
            OwnedIngressMessageHandleV1 message;
            if (TryPop(&message)) {
                if (failure_.load(std::memory_order_acquire) == 0U &&
                    WriteRecord(*message)) {
                    SaturatingIncrement(&written_records_);
                } else {
                    SaturatingIncrement(&abandoned_records_);
                }
                continue;
            }
            if (stop_requested_.load(std::memory_order_acquire)) {
                break;
            }
        }

        if (fd_ >= 0 && config_.sync_on_stop &&
            failure_.load(std::memory_order_acquire) == 0U &&
            ::fdatasync(fd_) != 0) {
            Trip(OptionalWalFailureKindV1::kSync, errno);
        }
        CloseFile();
        finished_.store(true, std::memory_order_release);
    }

    [[nodiscard]] bool WriteRecord(
        const OwnedIngressMessageV1& message) noexcept {
        const std::size_t body_size = message.body().size();
        constexpr std::size_t fixed_record_bytes =
            kOptionalWalRecordPrefixBytesV1 +
            kOptionalWalRecordChecksumBytesV1;
        if (body_size >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max()) -
                fixed_record_bytes) {
            Trip(OptionalWalFailureKindV1::kRecordTooLarge, 0);
            return false;
        }
        const std::uint32_t record_bytes = static_cast<std::uint32_t>(
            fixed_record_bytes + body_size);

        std::array<std::byte, kOptionalWalRecordPrefixBytesV1> prefix{};
        StoreU32Little(prefix, kRecordBytesOffset, record_bytes);
        StoreU32Little(
            prefix, kRecordMagicOffset, kOptionalWalRecordMagicV1);
        StoreU16Little(
            prefix, kRecordVersionOffset, kOptionalWalRecordVersionV1);
        StoreU16Little(
            prefix,
            kRecordPrefixBytesOffset,
            static_cast<std::uint16_t>(
                kOptionalWalRecordPrefixBytesV1));
        StoreU32Little(
            prefix,
            kRecordBodyBytesOffset,
            static_cast<std::uint32_t>(body_size));
        prefix[kRecordSourceSlotOffset] =
            static_cast<std::byte>(message.source_slot());
        std::memcpy(
            prefix.data() + kRecordRunIdOffset,
            message.run_id().data(),
            message.run_id().size());
        StoreU64Little(
            prefix,
            kRecordGlobalSequenceOffset,
            message.global_ingress_sequence());
        StoreU64Little(
            prefix,
            kRecordSourceSequenceOffset,
            message.source_sequence());
        StoreU64Little(
            prefix,
            kRecordRealtimeOffset,
            message.recv_realtime_ns());
        StoreU64Little(
            prefix,
            kRecordMonotonicOffset,
            message.recv_monotonic_ns());
        std::memcpy(
            prefix.data() + kRecordVendorHeadOffset,
            message.vendor_head_bytes().data(),
            message.vendor_head_bytes().size());

        l2flow::common::Crc32cState checksum;
        checksum.Update(std::span<const std::byte>(prefix).subspan(
            sizeof(std::uint32_t)));
        checksum.Update(message.body());
        std::array<std::byte, kOptionalWalRecordChecksumBytesV1>
            checksum_bytes{};
        StoreU32Little(checksum_bytes, 0U, checksum.Finalize());

        int write_errno = 0;
        if (!WriteAll(prefix, &write_errno) ||
            !WriteAll(message.body(), &write_errno) ||
            !WriteAll(checksum_bytes, &write_errno)) {
            Trip(OptionalWalFailureKindV1::kWrite, write_errno);
            return false;
        }
        return true;
    }

    [[nodiscard]] bool WriteAll(
        std::span<const std::byte> bytes,
        int* error_number) noexcept {
        std::size_t offset = 0U;
        while (offset < bytes.size()) {
            const ssize_t written = ::write(
                fd_, bytes.data() + offset, bytes.size() - offset);
            if (written > 0) {
                offset += static_cast<std::size_t>(written);
                continue;
            }
            if (written < 0 && errno == EINTR) {
                continue;
            }
            *error_number = written == 0 ? EIO : errno;
            return false;
        }
        return true;
    }

    void CloseFile() noexcept {
        if (fd_ < 0) {
            return;
        }
        const int closing = fd_;
        fd_ = -1;
        if (::close(closing) != 0) {
            Trip(OptionalWalFailureKindV1::kClose, errno);
        }
    }

    OptionalWalSinkConfigV1 config_;
    std::vector<OwnedIngressMessageHandleV1> queue_;
    std::counting_semaphore<
        std::numeric_limits<int>::max()>
        available_{0};
    std::thread worker_;
    int fd_ = -1;

    std::atomic_flag producer_gate_ = ATOMIC_FLAG_INIT;
    std::mutex stop_mutex_;
    std::atomic<std::uint64_t> write_position_{0U};
    std::atomic<std::uint64_t> read_position_{0U};
    std::atomic<std::uint64_t> accepted_records_{0U};
    std::atomic<std::uint64_t> written_records_{0U};
    std::atomic<std::uint64_t> rejected_records_{0U};
    std::atomic<std::uint64_t> abandoned_records_{0U};
    std::atomic<std::uint64_t> failure_{0U};
    std::atomic<bool> accepting_{false};
    std::atomic<bool> stop_requested_{false};
    std::atomic<bool> finished_{false};
    std::atomic<bool> coverage_lost_{false};
};

OptionalWalSinkV1::OptionalWalSinkV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

OptionalWalSinkV1::~OptionalWalSinkV1() = default;

OptionalWalCreateErrorV1 OptionalWalSinkV1::Create(
    OptionalWalSinkConfigV1 config,
    std::unique_ptr<OptionalWalSinkV1>* output) noexcept {
    if (output == nullptr) {
        return OptionalWalCreateErrorV1::kNullOutput;
    }
    output->reset();

    const std::size_t semaphore_max = static_cast<std::size_t>(
        std::numeric_limits<int>::max());
    if (config.enabled &&
        (config.path.empty() ||
         config.path.find('\0') != std::string::npos ||
         config.queue_capacity == 0U ||
         config.queue_capacity >= semaphore_max)) {
        return OptionalWalCreateErrorV1::kInvalidConfiguration;
    }

    try {
        auto impl = std::make_unique<Impl>(std::move(config));
        auto candidate = std::unique_ptr<OptionalWalSinkV1>(
            new OptionalWalSinkV1(std::move(impl)));
        candidate->impl_->Start();
        *output = std::move(candidate);
    } catch (const std::bad_alloc&) {
        return OptionalWalCreateErrorV1::kResourceExhausted;
    } catch (...) {
        return OptionalWalCreateErrorV1::kResourceExhausted;
    }
    return OptionalWalCreateErrorV1::kNone;
}

OptionalWalEnqueueResultV1 OptionalWalSinkV1::TryEnqueue(
    OwnedIngressMessageHandleV1 message) noexcept {
    return impl_->TryEnqueue(std::move(message));
}

void OptionalWalSinkV1::StopAndDrain() noexcept {
    impl_->StopAndDrain();
}

OptionalWalSnapshotV1 OptionalWalSinkV1::Snapshot() const noexcept {
    return impl_->Snapshot();
}

}  // namespace l2flow::realtime
