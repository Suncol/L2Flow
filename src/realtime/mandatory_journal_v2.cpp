#include "l2flow/realtime/mandatory_journal_v2.h"

#include "l2flow/common/crc32c.h"

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
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

constexpr auto kMaximumBatchDelay = std::chrono::seconds(60);

static_assert(
    kMandatoryJournalRecordVendorHeadOffsetV2 +
            l2flow::sdk::kVendorHeadBytes <=
        kMandatoryJournalRecordPrefixBytesV2);
static_assert(
    kMandatoryJournalRecordPrefixBytesV2 <=
    std::numeric_limits<std::uint16_t>::max());

void SetSystemError(int* output, int value) noexcept {
    if (output != nullptr) {
        *output = value;
    }
}

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

[[nodiscard]] std::uint64_t EncodeFailure(
    MandatoryJournalFailureKindV2 kind,
    int error_number) noexcept {
    return (static_cast<std::uint64_t>(
                static_cast<std::uint8_t>(kind))
            << 32U) |
           static_cast<std::uint32_t>(error_number);
}

[[nodiscard]] MandatoryJournalFailureKindV2 DecodeFailureKind(
    std::uint64_t failure) noexcept {
    return static_cast<MandatoryJournalFailureKindV2>(
        static_cast<std::uint8_t>(failure >> 32U));
}

[[nodiscard]] int DecodeFailureErrno(std::uint64_t failure) noexcept {
    return static_cast<int>(static_cast<std::uint32_t>(failure));
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

class ProducerGateExit final {
public:
    explicit ProducerGateExit(std::atomic_flag* gate) noexcept
        : gate_(gate) {}
    ~ProducerGateExit() {
        gate_->clear(std::memory_order_release);
    }

    ProducerGateExit(const ProducerGateExit&) = delete;
    ProducerGateExit& operator=(const ProducerGateExit&) = delete;

private:
    std::atomic_flag* gate_;
};

}  // namespace

std::string_view MandatoryJournalCreateErrorNameV2(
    MandatoryJournalCreateErrorV2 error) noexcept {
    switch (error) {
        case MandatoryJournalCreateErrorV2::kNone:
            return "none";
        case MandatoryJournalCreateErrorV2::kNullOutput:
            return "null_output";
        case MandatoryJournalCreateErrorV2::kInvalidConfiguration:
            return "invalid_configuration";
        case MandatoryJournalCreateErrorV2::kOpen:
            return "open";
        case MandatoryJournalCreateErrorV2::kFileStatus:
            return "file_status";
        case MandatoryJournalCreateErrorV2::kFileMode:
            return "file_mode";
        case MandatoryJournalCreateErrorV2::kThreadStart:
            return "thread_start";
        case MandatoryJournalCreateErrorV2::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

std::string_view MandatoryJournalAppendResultNameV2(
    MandatoryJournalAppendResultV2 result) noexcept {
    switch (result) {
        case MandatoryJournalAppendResultV2::kAccepted:
            return "accepted";
        case MandatoryJournalAppendResultV2::kInvalidMessage:
            return "invalid_message";
        case MandatoryJournalAppendResultV2::kSequenceError:
            return "sequence_error";
        case MandatoryJournalAppendResultV2::kQueueFull:
            return "queue_full";
        case MandatoryJournalAppendResultV2::kConcurrentProducer:
            return "concurrent_producer";
        case MandatoryJournalAppendResultV2::kStopped:
            return "stopped";
        case MandatoryJournalAppendResultV2::kFailed:
            return "failed";
    }
    return "unknown";
}

std::string_view MandatoryJournalFailureKindNameV2(
    MandatoryJournalFailureKindV2 kind) noexcept {
    switch (kind) {
        case MandatoryJournalFailureKindV2::kNone:
            return "none";
        case MandatoryJournalFailureKindV2::kInvalidMessage:
            return "invalid_message";
        case MandatoryJournalFailureKindV2::kSequence:
            return "sequence";
        case MandatoryJournalFailureKindV2::kQueueFull:
            return "queue_full";
        case MandatoryJournalFailureKindV2::kConcurrentProducer:
            return "concurrent_producer";
        case MandatoryJournalFailureKindV2::kRecordTooLarge:
            return "record_too_large";
        case MandatoryJournalFailureKindV2::kWrite:
            return "write";
        case MandatoryJournalFailureKindV2::kSync:
            return "sync";
        case MandatoryJournalFailureKindV2::kClose:
            return "close";
    }
    return "unknown";
}

class MandatoryJournalV2::Impl final {
public:
    explicit Impl(MandatoryJournalConfigV2 config)
        : config_(std::move(config)),
          queue_(config_.queue_capacity) {
        batch_.reserve(config_.max_batch_records);
    }

    ~Impl() { StopAndDrain(); }

    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;

    [[nodiscard]] MandatoryJournalCreateErrorV2 Start(
        int* system_error_number) noexcept {
        SetSystemError(system_error_number, 0);
        const MandatoryJournalCreateErrorV2 open_error =
            OpenFreshFile(system_error_number);
        if (open_error != MandatoryJournalCreateErrorV2::kNone) {
            finished_.store(true, std::memory_order_release);
            return open_error;
        }
        try {
            worker_ = std::thread([this] { Run(); });
        } catch (...) {
            const int saved_errno = EAGAIN;
            CleanupFailedCreate();
            finished_.store(true, std::memory_order_release);
            SetSystemError(system_error_number, saved_errno);
            return MandatoryJournalCreateErrorV2::kThreadStart;
        }
        accepting_.store(true, std::memory_order_release);
        return MandatoryJournalCreateErrorV2::kNone;
    }

    [[nodiscard]] MandatoryJournalAppendResultV2 TryAppend(
        OwnedIngressMessageHandleV1&& message) noexcept {
        if (failure_.load(std::memory_order_acquire) != 0U) {
            Reject();
            return MandatoryJournalAppendResultV2::kFailed;
        }
        if (!accepting_.load(std::memory_order_acquire)) {
            return MandatoryJournalAppendResultV2::kStopped;
        }
        if (producer_gate_.test_and_set(std::memory_order_acquire)) {
            Reject();
            FailProducer(
                MandatoryJournalFailureKindV2::kConcurrentProducer, 0);
            return MandatoryJournalAppendResultV2::kConcurrentProducer;
        }
        const ProducerGateExit clear_gate(&producer_gate_);

        if (failure_.load(std::memory_order_acquire) != 0U) {
            Reject();
            return MandatoryJournalAppendResultV2::kFailed;
        }
        if (!accepting_.load(std::memory_order_acquire)) {
            return MandatoryJournalAppendResultV2::kStopped;
        }
        if (!message) {
            Reject();
            FailProducer(
                MandatoryJournalFailureKindV2::kInvalidMessage, 0);
            return MandatoryJournalAppendResultV2::kInvalidMessage;
        }

        const std::uint64_t sequence =
            message->global_ingress_sequence();
        if (sequence != next_accepted_sequence_ ||
            sequence == std::numeric_limits<std::uint64_t>::max() ||
            (accepted_run_id_set_ &&
             message->run_id() != accepted_run_id_)) {
            Reject();
            FailProducer(MandatoryJournalFailureKindV2::kSequence, 0);
            return MandatoryJournalAppendResultV2::kSequenceError;
        }

        const std::uint64_t write =
            write_position_.load(std::memory_order_relaxed);
        const std::uint64_t read =
            read_position_.load(std::memory_order_acquire);
        if (write == std::numeric_limits<std::uint64_t>::max() ||
            write - read >=
                static_cast<std::uint64_t>(queue_.size())) {
            Reject();
            FailProducer(MandatoryJournalFailureKindV2::kQueueFull, 0);
            return MandatoryJournalAppendResultV2::kQueueFull;
        }

        if (!accepted_run_id_set_) {
            accepted_run_id_ = message->run_id();
            accepted_run_id_set_ = true;
        }
        const std::size_t index = static_cast<std::size_t>(
            write % static_cast<std::uint64_t>(queue_.size()));
        queue_[index] = std::move(message);
        write_position_.store(write + 1U, std::memory_order_release);
        accepted_sequence_.store(sequence, std::memory_order_release);
        next_accepted_sequence_ = sequence + 1U;
        available_.release();
        return MandatoryJournalAppendResultV2::kAccepted;
    }

    void StopAndDrain() noexcept {
        std::lock_guard<std::mutex> stop_lock(stop_mutex_);
        if (stop_complete_) {
            return;
        }
        accepting_.store(false, std::memory_order_release);
        while (producer_gate_.test_and_set(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        producer_gate_.clear(std::memory_order_release);

        stop_requested_.store(true, std::memory_order_release);
        WakeWriter();
        if (worker_.joinable()) {
            worker_.join();
        } else if (fd_ >= 0) {
            CloseFile();
            finished_.store(true, std::memory_order_release);
        }
        for (OwnedIngressMessageHandleV1& message : queue_) {
            message.reset();
        }
        batch_.clear();
        stop_complete_ = true;
    }

    [[nodiscard]] MandatoryJournalSnapshotV2 Snapshot() const noexcept {
        MandatoryJournalSnapshotV2 result{};
        result.finished = finished_.load(std::memory_order_acquire);
        result.rejected_records =
            rejected_records_.load(std::memory_order_acquire);
        // Load downstream-to-upstream. Each prefix is monotonic, so this
        // order prevents a concurrent advance from producing a torn snapshot
        // with durable > written or written > accepted.
        result.durable_sequence =
            durable_sequence_.load(std::memory_order_acquire);
        result.written_sequence =
            written_sequence_.load(std::memory_order_acquire);
        result.accepted_sequence =
            accepted_sequence_.load(std::memory_order_acquire);
        // Both admitted and written streams start at one and are dense.
        result.accepted_records = result.accepted_sequence;
        result.written_records = result.written_sequence;
        result.durability_lag_records =
            result.accepted_sequence - result.durable_sequence;
        const std::uint64_t failure =
            failure_.load(std::memory_order_acquire);
        result.failure_kind = DecodeFailureKind(failure);
        result.error_number = DecodeFailureErrno(failure);
        result.accepting =
            accepting_.load(std::memory_order_acquire);
        result.stop_requested =
            stop_requested_.load(std::memory_order_acquire);
        return result;
    }

private:
    [[nodiscard]] MandatoryJournalCreateErrorV2 OpenFreshFile(
        int* system_error_number) noexcept {
        int flags = O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC;
#ifdef O_NOFOLLOW
        flags |= O_NOFOLLOW;
#endif
        const int opened = ::open(config_.path.c_str(), flags, 0600);
        if (opened < 0) {
            SetSystemError(system_error_number, errno);
            return MandatoryJournalCreateErrorV2::kOpen;
        }
        fd_ = opened;
        file_created_ = true;

        struct stat status {};
        if (::fstat(fd_, &status) != 0) {
            const int saved_errno = errno;
            CleanupFailedCreate();
            SetSystemError(system_error_number, saved_errno);
            return MandatoryJournalCreateErrorV2::kFileStatus;
        }
        if (!S_ISREG(status.st_mode)) {
            CleanupFailedCreate();
            SetSystemError(system_error_number, EINVAL);
            return MandatoryJournalCreateErrorV2::kFileStatus;
        }
        if (::fchmod(fd_, 0600) != 0) {
            const int saved_errno = errno;
            CleanupFailedCreate();
            SetSystemError(system_error_number, saved_errno);
            return MandatoryJournalCreateErrorV2::kFileMode;
        }
        return MandatoryJournalCreateErrorV2::kNone;
    }

    void CleanupFailedCreate() noexcept {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
            fd_ = -1;
        }
        if (file_created_) {
            static_cast<void>(::unlink(config_.path.c_str()));
            file_created_ = false;
        }
    }

    void Reject() noexcept {
        SaturatingIncrement(&rejected_records_);
    }

    [[nodiscard]] bool RecordFailure(
        MandatoryJournalFailureKindV2 kind,
        int error_number,
        bool abort_writer) noexcept {
        std::uint64_t expected = 0U;
        const bool first_failure = failure_.compare_exchange_strong(
            expected,
            EncodeFailure(kind, error_number),
            std::memory_order_acq_rel,
            std::memory_order_acquire);
        accepting_.store(false, std::memory_order_release);
        if (abort_writer) {
            writer_abort_.store(true, std::memory_order_release);
        }
        stop_requested_.store(true, std::memory_order_release);
        WakeWriter();
        return first_failure;
    }

    void FailProducer(
        MandatoryJournalFailureKindV2 kind,
        int error_number) noexcept {
        static_cast<void>(RecordFailure(kind, error_number, false));
    }

    void FailWriter(
        MandatoryJournalFailureKindV2 kind,
        int error_number) noexcept {
        if (RecordFailure(kind, error_number, true) &&
            config_.failure != nullptr) {
            config_.failure(
                config_.failure_context, kind, error_number);
        }
    }

    [[nodiscard]] bool QueueEmpty() const noexcept {
        return read_position_.load(std::memory_order_acquire) ==
               write_position_.load(std::memory_order_acquire);
    }

    void WakeWriter() noexcept {
        bool expected = false;
        if (control_wake_pending_.compare_exchange_strong(
                expected,
                true,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            available_.release();
        }
    }

    void ConsumeControlWake() noexcept {
        control_wake_pending_.store(false, std::memory_order_release);
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
            if (writer_abort_.load(std::memory_order_acquire)) {
                break;
            }
            if (stop_requested_.load(std::memory_order_acquire) &&
                QueueEmpty()) {
                break;
            }

            available_.acquire();
            if (writer_abort_.load(std::memory_order_acquire)) {
                break;
            }

            OwnedIngressMessageHandleV1 first;
            if (!TryPop(&first)) {
                ConsumeControlWake();
                if (stop_requested_.load(std::memory_order_acquire) &&
                    QueueEmpty()) {
                    break;
                }
                continue;
            }
            batch_.clear();
            batch_.push_back(std::move(first));

            const auto deadline =
                std::chrono::steady_clock::now() +
                config_.max_batch_delay;
            while (batch_.size() < config_.max_batch_records) {
                if (!available_.try_acquire_until(deadline)) {
                    break;
                }
                OwnedIngressMessageHandleV1 next;
                if (!TryPop(&next)) {
                    ConsumeControlWake();
                    if (stop_requested_.load(std::memory_order_acquire)) {
                        break;
                    }
                    continue;
                }
                batch_.push_back(std::move(next));
            }

            if (!CommitBatch()) {
                break;
            }
            batch_.clear();
        }

        batch_.clear();
        CloseFile();
        finished_.store(true, std::memory_order_release);
    }

    [[nodiscard]] bool CommitBatch() noexcept {
        if (batch_.empty() || fd_ < 0) {
            FailWriter(MandatoryJournalFailureKindV2::kWrite, EBADF);
            return false;
        }

        for (const OwnedIngressMessageHandleV1& message : batch_) {
            if (!message ||
                message->global_ingress_sequence() !=
                    next_written_sequence_ ||
                message->global_ingress_sequence() ==
                    std::numeric_limits<std::uint64_t>::max() ||
                (written_run_id_set_ &&
                 message->run_id() != written_run_id_)) {
                FailWriter(MandatoryJournalFailureKindV2::kSequence, 0);
                return false;
            }
            if (!written_run_id_set_) {
                written_run_id_ = message->run_id();
                written_run_id_set_ = true;
            }

            MandatoryJournalFailureKindV2 write_failure =
                MandatoryJournalFailureKindV2::kNone;
            int write_errno = 0;
            if (!WriteRecord(
                    *message, &write_failure, &write_errno)) {
                FailWriter(write_failure, write_errno);
                return false;
            }
            const std::uint64_t sequence =
                message->global_ingress_sequence();
            written_sequence_.store(sequence, std::memory_order_release);
            next_written_sequence_ = sequence + 1U;
        }

        if (config_.before_sync_for_test != nullptr) {
            config_.before_sync_for_test(
                config_.before_sync_context_for_test);
        }
        if (::fdatasync(fd_) != 0) {
            FailWriter(MandatoryJournalFailureKindV2::kSync, errno);
            return false;
        }

        const std::uint64_t durable =
            batch_.back()->global_ingress_sequence();
        durable_sequence_.store(durable, std::memory_order_release);
        if (config_.durable != nullptr) {
            config_.durable(config_.durable_context, durable);
        }
        batch_.clear();  // Release Journal's intrusive references now.
        return true;
    }

    [[nodiscard]] bool WriteRecord(
        const OwnedIngressMessageV1& message,
        MandatoryJournalFailureKindV2* failure_kind,
        int* error_number) noexcept {
        if (failure_kind == nullptr || error_number == nullptr) {
            return false;
        }
        *failure_kind = MandatoryJournalFailureKindV2::kWrite;
        *error_number = 0;

        const std::size_t body_size = message.body().size();
        constexpr std::size_t fixed_record_bytes =
            kMandatoryJournalRecordPrefixBytesV2 +
            kMandatoryJournalRecordChecksumBytesV2;
        if (message.wire_size() > config_.maximum_message_bytes ||
            body_size >
                static_cast<std::size_t>(
                    std::numeric_limits<std::uint32_t>::max()) -
                    fixed_record_bytes) {
            *failure_kind =
                MandatoryJournalFailureKindV2::kRecordTooLarge;
            return false;
        }
        const std::uint32_t record_bytes = static_cast<std::uint32_t>(
            fixed_record_bytes + body_size);

        const l2flow::sdk::VendorHeadView head = message.vendor_head();
        if (head.service_id() != message.key().service_id ||
            head.service_version() != message.key().service_version ||
            head.message_id() != message.key().message_id ||
            message.source_slot() >= kOwnedIngressSourceCountV1) {
            *failure_kind =
                MandatoryJournalFailureKindV2::kInvalidMessage;
            return false;
        }

        std::array<std::byte, kMandatoryJournalRecordPrefixBytesV2>
            prefix{};
        StoreU32Little(
            prefix, kMandatoryJournalRecordBytesOffsetV2, record_bytes);
        StoreU32Little(
            prefix,
            kMandatoryJournalRecordMagicOffsetV2,
            kMandatoryJournalRecordMagicV2);
        StoreU16Little(
            prefix,
            kMandatoryJournalRecordVersionOffsetV2,
            kMandatoryJournalRecordVersionV2);
        StoreU16Little(
            prefix,
            kMandatoryJournalRecordPrefixBytesOffsetV2,
            static_cast<std::uint16_t>(
                kMandatoryJournalRecordPrefixBytesV2));
        StoreU32Little(
            prefix,
            kMandatoryJournalRecordBodyBytesOffsetV2,
            static_cast<std::uint32_t>(body_size));
        prefix[kMandatoryJournalRecordSourceSlotOffsetV2] =
            static_cast<std::byte>(message.source_slot());
        prefix[kMandatoryJournalRecordServiceIdOffsetV2] =
            static_cast<std::byte>(message.key().service_id);
        StoreU16Little(
            prefix,
            kMandatoryJournalRecordServiceVersionOffsetV2,
            message.key().service_version);
        StoreU16Little(
            prefix,
            kMandatoryJournalRecordMessageIdOffsetV2,
            message.key().message_id);
        std::memcpy(
            prefix.data() + kMandatoryJournalRecordRunIdOffsetV2,
            message.run_id().data(),
            message.run_id().size());
        StoreU64Little(
            prefix,
            kMandatoryJournalRecordGlobalSequenceOffsetV2,
            message.global_ingress_sequence());
        StoreU64Little(
            prefix,
            kMandatoryJournalRecordSourceSequenceOffsetV2,
            message.source_sequence());
        StoreU64Little(
            prefix,
            kMandatoryJournalRecordTickSequenceOffsetV2,
            message.tick_stream_sequence());
        StoreU64Little(
            prefix,
            kMandatoryJournalRecordRealtimeOffsetV2,
            message.recv_realtime_ns());
        StoreU64Little(
            prefix,
            kMandatoryJournalRecordMonotonicOffsetV2,
            message.recv_monotonic_ns());
        std::memcpy(
            prefix.data() + kMandatoryJournalRecordVendorHeadOffsetV2,
            message.vendor_head_bytes().data(),
            message.vendor_head_bytes().size());

        l2flow::common::Crc32cState checksum;
        checksum.Update(std::span<const std::byte>(prefix).subspan(
            sizeof(std::uint32_t)));
        checksum.Update(message.body());
        std::array<std::byte, kMandatoryJournalRecordChecksumBytesV2>
            checksum_bytes{};
        StoreU32Little(checksum_bytes, 0U, checksum.Finalize());

        if (!WriteAll(prefix, error_number) ||
            !WriteAll(message.body(), error_number) ||
            !WriteAll(checksum_bytes, error_number)) {
            *failure_kind = MandatoryJournalFailureKindV2::kWrite;
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
            FailWriter(MandatoryJournalFailureKindV2::kClose, errno);
        }
    }

    MandatoryJournalConfigV2 config_;
    std::vector<OwnedIngressMessageHandleV1> queue_;
    std::vector<OwnedIngressMessageHandleV1> batch_;
    std::counting_semaphore<std::numeric_limits<int>::max()>
        available_{0};
    std::thread worker_;
    int fd_ = -1;
    bool file_created_ = false;

    std::atomic_flag producer_gate_ = ATOMIC_FLAG_INIT;
    std::uint64_t next_accepted_sequence_ = 1U;
    l2flow::common::Identity128 accepted_run_id_{};
    bool accepted_run_id_set_ = false;
    std::atomic<std::uint64_t> write_position_{0U};
    std::atomic<std::uint64_t> read_position_{0U};

    std::uint64_t next_written_sequence_ = 1U;
    l2flow::common::Identity128 written_run_id_{};
    bool written_run_id_set_ = false;

    std::atomic<std::uint64_t> rejected_records_{0U};
    std::atomic<std::uint64_t> accepted_sequence_{0U};
    std::atomic<std::uint64_t> written_sequence_{0U};
    std::atomic<std::uint64_t> durable_sequence_{0U};
    std::atomic<std::uint64_t> failure_{0U};
    std::atomic<bool> control_wake_pending_{false};
    std::atomic<bool> accepting_{false};
    std::atomic<bool> stop_requested_{false};
    std::atomic<bool> writer_abort_{false};
    std::atomic<bool> finished_{false};

    std::mutex stop_mutex_;
    bool stop_complete_ = false;
};

MandatoryJournalV2::MandatoryJournalV2(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

MandatoryJournalV2::~MandatoryJournalV2() = default;

MandatoryJournalCreateErrorV2 MandatoryJournalV2::Create(
    MandatoryJournalConfigV2 config,
    std::unique_ptr<MandatoryJournalV2>* output,
    int* system_error_number) noexcept {
    if (output == nullptr) {
        SetSystemError(system_error_number, EINVAL);
        return MandatoryJournalCreateErrorV2::kNullOutput;
    }
    output->reset();
    SetSystemError(system_error_number, 0);
    if (config.path.empty() ||
        config.maximum_message_bytes < l2flow::sdk::kVendorHeadBytes ||
        config.maximum_message_bytes >
            kOwnedIngressMaximumMessageBytesV1 ||
        config.queue_capacity == 0U ||
        config.queue_capacity >
            static_cast<std::size_t>(
                std::numeric_limits<int>::max() - 1) ||
        config.max_batch_records == 0U ||
        config.max_batch_records > config.queue_capacity ||
        config.max_batch_delay <= std::chrono::microseconds::zero() ||
        config.max_batch_delay > kMaximumBatchDelay) {
        SetSystemError(system_error_number, EINVAL);
        return MandatoryJournalCreateErrorV2::kInvalidConfiguration;
    }

    try {
        std::unique_ptr<Impl> impl(new Impl(std::move(config)));
        std::unique_ptr<MandatoryJournalV2> journal(
            new MandatoryJournalV2(std::move(impl)));
        const MandatoryJournalCreateErrorV2 start_error =
            journal->impl_->Start(system_error_number);
        if (start_error != MandatoryJournalCreateErrorV2::kNone) {
            return start_error;
        }
        *output = std::move(journal);
        return MandatoryJournalCreateErrorV2::kNone;
    } catch (const std::bad_alloc&) {
        SetSystemError(system_error_number, ENOMEM);
        return MandatoryJournalCreateErrorV2::kResourceExhausted;
    } catch (...) {
        SetSystemError(system_error_number, EINVAL);
        return MandatoryJournalCreateErrorV2::kInvalidConfiguration;
    }
}

MandatoryJournalAppendResultV2 MandatoryJournalV2::TryAppend(
    OwnedIngressMessageHandleV1 message) noexcept {
    return impl_->TryAppend(std::move(message));
}

void MandatoryJournalV2::StopAndDrain() noexcept {
    impl_->StopAndDrain();
}

MandatoryJournalSnapshotV2 MandatoryJournalV2::Snapshot() const noexcept {
    return impl_->Snapshot();
}

}  // namespace l2flow::realtime
