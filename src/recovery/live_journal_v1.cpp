#include "l2flow/recovery/live_journal_v1.h"

#include "l2flow/common/crc32c.h"
#include "l2flow/common/sha256.h"
#include "l2flow/realtime/native_sequence_recovery_v1.h"
#include "l2flow/sdk/vendor_head_view.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <limits>
#include <mutex>
#include <new>
#include <span>
#include <system_error>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <utility>

namespace l2flow::recovery {
namespace {

namespace common = l2flow::common;
namespace realtime = l2flow::realtime;
namespace sdk = l2flow::sdk;
namespace mdl = datayes::mdl;

constexpr std::size_t kSegmentHeaderBytes = 128U;
constexpr std::size_t kRecordHeaderBytes = 192U;
constexpr std::uint16_t kJournalMajor = 1U;
constexpr std::uint16_t kJournalMinor = 0U;
constexpr std::uint32_t kLittleEndianMarker = 0x01020304U;
constexpr std::size_t kSegmentCrcOffset = 80U;
constexpr std::size_t kRecordDigestOffset = 128U;
constexpr std::size_t kRecordBodyCrcOffset = 160U;
constexpr std::size_t kRecordCrcOffset = 164U;
constexpr std::array<std::byte, 8U> kSegmentMagic{
    std::byte{'L'}, std::byte{'2'}, std::byte{'F'}, std::byte{'L'},
    std::byte{'J'}, std::byte{'S'}, std::byte{'1'}, std::byte{0U}};
constexpr std::array<std::byte, 8U> kRecordMagic{
    std::byte{'L'}, std::byte{'2'}, std::byte{'F'}, std::byte{'L'},
    std::byte{'J'}, std::byte{'R'}, std::byte{'1'}, std::byte{0U}};

static_assert(std::endian::native == std::endian::little);
static_assert(sizeof(mdl::MDLMessageHead) == sdk::kVendorHeadBytes);

template <typename Integer, std::size_t Size>
void StoreInteger(
    std::array<std::byte, Size>* bytes,
    std::size_t offset,
    Integer value) noexcept {
    static_assert(std::is_integral_v<Integer>);
    std::memcpy(bytes->data() + offset, &value, sizeof(value));
}

template <typename Integer, std::size_t Size>
[[nodiscard]] Integer LoadInteger(
    const std::array<std::byte, Size>& bytes,
    std::size_t offset) noexcept {
    static_assert(std::is_integral_v<Integer>);
    Integer value{};
    std::memcpy(&value, bytes.data() + offset, sizeof(value));
    return value;
}

[[nodiscard]] bool IdentityNonzero(
    const common::Identity128& identity) noexcept {
    return std::any_of(
        identity.begin(), identity.end(), [](std::byte value) noexcept {
            return value != std::byte{0U};
        });
}

[[nodiscard]] bool AddWithin(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* output) noexcept {
    if (output == nullptr ||
        right > std::numeric_limits<std::uint64_t>::max() - left) {
        return false;
    }
    *output = left + right;
    return true;
}

[[nodiscard]] std::optional<std::size_t> TupleIndex(
    const sdk::MessageKey& key) noexcept {
    for (std::size_t index = 0U;
         index < sdk::kProductionMessageKeysV1.size();
         ++index) {
        if (sdk::kProductionMessageKeysV1[index] == key) {
            return index;
        }
    }
    return std::nullopt;
}

[[nodiscard]] std::string SegmentName(std::uint64_t index) {
    std::array<char, 64U> buffer{};
    const int length = std::snprintf(
        buffer.data(),
        buffer.size(),
        "segment-%010llu.wal",
        static_cast<unsigned long long>(index));
    if (length <= 0 ||
        static_cast<std::size_t>(length) >= buffer.size()) {
        return {};
    }
    return std::string(
        buffer.data(), static_cast<std::size_t>(length));
}

[[nodiscard]] bool WriteFull(
    int fd,
    std::span<const std::byte> bytes,
    int* system_error_number) noexcept {
    std::size_t written = 0U;
    while (written < bytes.size()) {
        const ssize_t result = ::write(
            fd,
            bytes.data() + written,
            bytes.size() - written);
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (system_error_number != nullptr) {
                *system_error_number = errno;
            }
            return false;
        }
        if (result == 0) {
            if (system_error_number != nullptr) {
                *system_error_number = EIO;
            }
            return false;
        }
        written += static_cast<std::size_t>(result);
    }
    return true;
}

enum class PreadDisposition : std::uint8_t {
    kOk = 0U,
    kEof,
    kFailed,
};

[[nodiscard]] PreadDisposition PreadFull(
    int fd,
    std::uint64_t offset,
    std::span<std::byte> bytes,
    int* system_error_number) noexcept {
    std::size_t received = 0U;
    while (received < bytes.size()) {
        if (offset > static_cast<std::uint64_t>(
                         std::numeric_limits<off_t>::max()) -
                         received) {
            if (system_error_number != nullptr) {
                *system_error_number = EOVERFLOW;
            }
            return PreadDisposition::kFailed;
        }
        const off_t position = static_cast<off_t>(offset + received);
        const ssize_t result = ::pread(
            fd,
            bytes.data() + received,
            bytes.size() - received,
            position);
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (system_error_number != nullptr) {
                *system_error_number = errno;
            }
            return PreadDisposition::kFailed;
        }
        if (result == 0) {
            if (received == 0U) {
                return PreadDisposition::kEof;
            }
            if (system_error_number != nullptr) {
                *system_error_number = EIO;
            }
            return PreadDisposition::kFailed;
        }
        received += static_cast<std::size_t>(result);
    }
    return PreadDisposition::kOk;
}

[[nodiscard]] std::array<std::byte, kSegmentHeaderBytes>
BuildSegmentHeader(
    const LiveJournalConfigV1& config,
    std::uint64_t segment_index,
    std::uint64_t first_serial,
    std::uint64_t created_realtime_ns,
    std::uint64_t created_monotonic_ns) noexcept {
    std::array<std::byte, kSegmentHeaderBytes> header{};
    std::copy(kSegmentMagic.begin(), kSegmentMagic.end(), header.begin());
    StoreInteger(&header, 8U, kJournalMajor);
    StoreInteger(&header, 10U, kJournalMinor);
    StoreInteger(
        &header, 12U, static_cast<std::uint32_t>(header.size()));
    StoreInteger(&header, 16U, kLittleEndianMarker);
    StoreInteger(&header, 20U, config.trade_date);
    StoreInteger(&header, 24U, segment_index);
    StoreInteger(&header, 32U, first_serial);
    std::copy(
        config.run_id.begin(), config.run_id.end(), header.begin() + 40U);
    StoreInteger(&header, 56U, created_realtime_ns);
    StoreInteger(&header, 64U, created_monotonic_ns);
    StoreInteger<std::uint64_t>(&header, 72U, 0U);
    StoreInteger<std::uint32_t>(&header, kSegmentCrcOffset, 0U);
    const std::uint32_t crc = common::ComputeCrc32c(header);
    StoreInteger(&header, kSegmentCrcOffset, crc);
    return header;
}

[[nodiscard]] bool SegmentHeaderValid(
    const std::array<std::byte, kSegmentHeaderBytes>& input,
    const LiveJournalConfigV1& config,
    std::uint64_t expected_segment,
    std::uint64_t expected_first_serial) noexcept {
    if (!std::equal(
            kSegmentMagic.begin(), kSegmentMagic.end(), input.begin()) ||
        LoadInteger<std::uint16_t>(input, 8U) != kJournalMajor ||
        LoadInteger<std::uint16_t>(input, 10U) != kJournalMinor ||
        LoadInteger<std::uint32_t>(input, 12U) != input.size() ||
        LoadInteger<std::uint32_t>(input, 16U) !=
            kLittleEndianMarker ||
        LoadInteger<std::uint32_t>(input, 20U) != config.trade_date ||
        LoadInteger<std::uint64_t>(input, 24U) != expected_segment ||
        LoadInteger<std::uint64_t>(input, 32U) !=
            expected_first_serial ||
        !std::equal(
            config.run_id.begin(),
            config.run_id.end(),
            input.begin() + 40U) ||
        LoadInteger<std::uint64_t>(input, 72U) != 0U) {
        return false;
    }
    auto copy = input;
    const std::uint32_t expected_crc =
        LoadInteger<std::uint32_t>(copy, kSegmentCrcOffset);
    StoreInteger<std::uint32_t>(&copy, kSegmentCrcOffset, 0U);
    return expected_crc != 0U &&
           common::ComputeCrc32c(copy) == expected_crc &&
           std::all_of(
               input.begin() + 84U,
               input.end(),
               [](std::byte value) noexcept {
                   return value == std::byte{0U};
               });
}

[[nodiscard]] bool ConfigValid(const LiveJournalConfigV1& config) {
    const std::uint64_t largest_record =
        static_cast<std::uint64_t>(kRecordHeaderBytes) +
        config.maximum_message_bytes - sdk::kVendorHeadBytes;
    return config.directory.is_absolute() &&
           !config.directory.empty() && IdentityNonzero(config.run_id) &&
           config.trade_date != 0U &&
           config.maximum_message_bytes >= sdk::kVendorHeadBytes &&
           config.maximum_message_bytes <=
               realtime::kOwnedIngressMaximumMessageBytesV1 &&
           config.segment_maximum_bytes >=
               kLiveJournalMinimumSegmentBytesV1 &&
           config.segment_maximum_bytes <=
               kLiveJournalMaximumSegmentBytesV1 &&
           largest_record <=
               config.segment_maximum_bytes - kSegmentHeaderBytes &&
           config.maximum_total_bytes >=
               kSegmentHeaderBytes + largest_record &&
           config.queue_capacity_records != 0U &&
           config.queue_capacity_records <=
               kLiveJournalMaximumQueueRecordsV1 &&
           config.sync_batch_records != 0U &&
           config.sync_batch_records <= config.queue_capacity_records &&
           config.sync_interval > std::chrono::milliseconds::zero() &&
           config.sync_interval <= std::chrono::seconds(1);
}

}  // namespace

std::string_view LiveJournalErrorNameV1(
    LiveJournalErrorV1 error) noexcept {
    switch (error) {
        case LiveJournalErrorV1::kNone:
            return "none";
        case LiveJournalErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case LiveJournalErrorV1::kDirectoryCreateFailed:
            return "directory_create_failed";
        case LiveJournalErrorV1::kDirectoryOpenFailed:
            return "directory_open_failed";
        case LiveJournalErrorV1::kDirectoryNotEmpty:
            return "directory_not_empty";
        case LiveJournalErrorV1::kSegmentCreateFailed:
            return "segment_create_failed";
        case LiveJournalErrorV1::kSegmentOpenFailed:
            return "segment_open_failed";
        case LiveJournalErrorV1::kWriteFailed:
            return "write_failed";
        case LiveJournalErrorV1::kSyncFailed:
            return "sync_failed";
        case LiveJournalErrorV1::kCapacityExhausted:
            return "capacity_exhausted";
        case LiveJournalErrorV1::kQueueExhausted:
            return "queue_exhausted";
        case LiveJournalErrorV1::kInvalidCapture:
            return "invalid_capture";
        case LiveJournalErrorV1::kSequenceExhausted:
            return "sequence_exhausted";
        case LiveJournalErrorV1::kCorruptRecord:
            return "corrupt_record";
        case LiveJournalErrorV1::kStopped:
            return "stopped";
        case LiveJournalErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case LiveJournalErrorV1::kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

class MdlLiveJournalV1::Impl final
    : public std::enable_shared_from_this<MdlLiveJournalV1::Impl> {
public:
    struct PendingRecord final {
        sdk::VendorHeadBytes vendor_head{};
        std::vector<std::byte> body;
        sdk::MessageKey key{};
        std::uint64_t global_serial = 0U;
        std::uint64_t tuple_serial = 0U;
        std::uint64_t recv_realtime_ns = 0U;
        std::uint64_t recv_monotonic_ns = 0U;
        std::uint64_t native_sequence = 0U;
        std::uint64_t segment_index = 0U;
        std::uint32_t native_channel = 0U;
        std::uint8_t native_market = 0U;
        std::uint8_t source_slot = 0U;
        bool native_valid = false;

        [[nodiscard]] std::uint64_t disk_bytes() const noexcept {
            return kRecordHeaderBytes + body.size();
        }
    };

    explicit Impl(LiveJournalConfigV1 config)
        : config_(std::move(config)) {}

    ~Impl() {
        static_cast<void>(StopAndFlush());
        if (writer_fd_ >= 0) {
            static_cast<void>(::close(writer_fd_));
        }
        if (directory_fd_ >= 0) {
            static_cast<void>(::close(directory_fd_));
        }
    }

    [[nodiscard]] LiveJournalErrorV1 Initialize(
        int* system_error_number) noexcept {
        if (system_error_number != nullptr) {
            *system_error_number = 0;
        }
        if (!ConfigValid(config_)) {
            return LiveJournalErrorV1::kInvalidConfiguration;
        }
        try {
            std::error_code filesystem_error;
            const bool exists =
                std::filesystem::exists(config_.directory, filesystem_error);
            if (filesystem_error) {
                if (system_error_number != nullptr) {
                    *system_error_number = filesystem_error.value();
                }
                return LiveJournalErrorV1::kDirectoryCreateFailed;
            }
            if (!exists &&
                !std::filesystem::create_directory(
                    config_.directory, filesystem_error)) {
                if (system_error_number != nullptr) {
                    *system_error_number = filesystem_error.value();
                }
                return LiveJournalErrorV1::kDirectoryCreateFailed;
            }
            if (filesystem_error ||
                !std::filesystem::is_directory(
                    config_.directory, filesystem_error)) {
                if (system_error_number != nullptr) {
                    *system_error_number = filesystem_error.value();
                }
                return LiveJournalErrorV1::kDirectoryOpenFailed;
            }
            const auto begin = std::filesystem::directory_iterator(
                config_.directory, filesystem_error);
            if (filesystem_error) {
                if (system_error_number != nullptr) {
                    *system_error_number = filesystem_error.value();
                }
                return LiveJournalErrorV1::kDirectoryOpenFailed;
            }
            if (begin != std::filesystem::directory_iterator{}) {
                return LiveJournalErrorV1::kDirectoryNotEmpty;
            }
            directory_fd_ = ::open(
                config_.directory.c_str(),
                O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
            if (directory_fd_ < 0) {
                if (system_error_number != nullptr) {
                    *system_error_number = errno;
                }
                return LiveJournalErrorV1::kDirectoryOpenFailed;
            }
            int create_error = 0;
            if (!OpenNewSegment(1U, 1U, &create_error)) {
                if (system_error_number != nullptr) {
                    *system_error_number = create_error;
                }
                return create_error == ENOSPC
                           ? LiveJournalErrorV1::kCapacityExhausted
                           : LiveJournalErrorV1::kSegmentCreateFailed;
            }
            reserved_bytes_ = kSegmentHeaderBytes;
            committed_bytes_ = kSegmentHeaderBytes;
            reservation_segment_bytes_ = kSegmentHeaderBytes;
            reservation_segment_count_ = 1U;
            state_ = LiveJournalStateV1::kWriting;
            failed_.store(false, std::memory_order_release);
            writer_thread_ = std::thread(
                [self = shared_from_this()] { self->WriterLoop(); });
            return LiveJournalErrorV1::kNone;
        } catch (const std::bad_alloc&) {
            return LiveJournalErrorV1::kResourceExhausted;
        } catch (...) {
            return LiveJournalErrorV1::kUnexpectedFailure;
        }
    }

    [[nodiscard]] bool Capture(
        const realtime::RealtimeIngressCaptureInputV1& input) noexcept {
        if (input.inspection == nullptr || !*input.inspection ||
            input.inspection->wire_size() >
                config_.maximum_message_bytes ||
            input.recv_realtime_ns == 0U ||
            input.recv_monotonic_ns == 0U) {
            Fail(LiveJournalErrorV1::kInvalidCapture, EINVAL);
            return false;
        }
        const std::optional<std::size_t> tuple =
            TupleIndex(input.inspection->key());
        if (!tuple.has_value()) {
            Fail(LiveJournalErrorV1::kInvalidCapture, EINVAL);
            return false;
        }
        std::unique_ptr<PendingRecord> pending;
        try {
            pending = std::make_unique<PendingRecord>();
            pending->vendor_head =
                input.inspection->vendor_head_bytes();
            pending->body.assign(
                input.inspection->body().begin(),
                input.inspection->body().end());
            pending->key = input.inspection->key();
            pending->recv_realtime_ns = input.recv_realtime_ns;
            pending->recv_monotonic_ns = input.recv_monotonic_ns;
            pending->source_slot = input.inspection->source_slot();
            realtime::NativeSequenceDescriptorV1 native{};
            if (realtime::ExtractNativeSequenceV1(
                    pending->key, pending->body, &native) ==
                realtime::NativeSequenceExtractErrorV1::kNone) {
                pending->native_valid = true;
                pending->native_market =
                    static_cast<std::uint8_t>(native.domain.market);
                pending->native_channel = native.domain.channel;
                pending->native_sequence = native.sequence;
            }
        } catch (...) {
            Fail(LiveJournalErrorV1::kResourceExhausted, ENOMEM);
            return false;
        }

        try {
            std::lock_guard<std::mutex> lock(mutex_);
            if (state_ != LiveJournalStateV1::kWriting ||
                error_ != LiveJournalErrorV1::kNone) {
                return false;
            }
            if (queue_.size() >= config_.queue_capacity_records) {
                FailLocked(LiveJournalErrorV1::kQueueExhausted, ENOBUFS);
                return false;
            }
            if (accepted_serial_ ==
                    std::numeric_limits<std::uint64_t>::max() ||
                accepted_tuple_serials_[*tuple] ==
                    std::numeric_limits<std::uint64_t>::max()) {
                FailLocked(
                    LiveJournalErrorV1::kSequenceExhausted, EOVERFLOW);
                return false;
            }
            const std::uint64_t record_bytes = pending->disk_bytes();
            std::uint64_t additional = record_bytes;
            std::uint64_t next_segment_bytes = 0U;
            std::uint64_t next_segment_count =
                reservation_segment_count_;
            if (!AddWithin(
                    reservation_segment_bytes_,
                    record_bytes,
                    &next_segment_bytes)) {
                FailLocked(
                    LiveJournalErrorV1::kSequenceExhausted, EOVERFLOW);
                return false;
            }
            if (next_segment_bytes > config_.segment_maximum_bytes) {
                if (next_segment_count ==
                    std::numeric_limits<std::uint64_t>::max()) {
                    FailLocked(
                        LiveJournalErrorV1::kSequenceExhausted,
                        EOVERFLOW);
                    return false;
                }
                ++next_segment_count;
                next_segment_bytes = kSegmentHeaderBytes + record_bytes;
                additional += kSegmentHeaderBytes;
            }
            std::uint64_t next_reserved = 0U;
            if (!AddWithin(reserved_bytes_, additional, &next_reserved) ||
                next_reserved > config_.maximum_total_bytes) {
                FailLocked(
                    LiveJournalErrorV1::kCapacityExhausted, ENOSPC);
                return false;
            }
            pending->global_serial = accepted_serial_ + 1U;
            pending->tuple_serial =
                accepted_tuple_serials_[*tuple] + 1U;
            pending->segment_index = next_segment_count;
            accepted_serial_ = pending->global_serial;
            accepted_tuple_serials_[*tuple] = pending->tuple_serial;
            reservation_segment_count_ = next_segment_count;
            reservation_segment_bytes_ = next_segment_bytes;
            reserved_bytes_ = next_reserved;
            queue_.push_back(std::move(pending));
            queue_high_water_ = std::max(queue_high_water_, queue_.size());
            queue_cv_.notify_one();
            return true;
        } catch (...) {
            Fail(LiveJournalErrorV1::kUnexpectedFailure, EIO);
            return false;
        }
    }

    [[nodiscard]] bool CaptureTupleFence(
        const sdk::MessageKey& key,
        LiveJournalTupleFenceV1* output) const noexcept {
        if (output == nullptr) {
            return false;
        }
        const std::optional<std::size_t> tuple = TupleIndex(key);
        if (!tuple.has_value()) {
            return false;
        }
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            if (state_ != LiveJournalStateV1::kWriting ||
                error_ != LiveJournalErrorV1::kNone) {
                return false;
            }
            output->key = key;
            output->accepted_global_serial = accepted_serial_;
            output->accepted_tuple_serial =
                accepted_tuple_serials_[*tuple];
            return true;
        } catch (...) {
            return false;
        }
    }

    [[nodiscard]] LiveJournalSnapshotV1 Snapshot() const noexcept {
        LiveJournalSnapshotV1 result{};
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            result.state = state_;
            result.error = error_;
            result.system_error_number = system_error_number_;
            result.accepted_serial = accepted_serial_;
            result.committed_serial = committed_serial_;
            result.reserved_bytes = reserved_bytes_;
            result.committed_bytes = committed_bytes_;
            result.segment_count = reservation_segment_count_;
            result.queue_depth = queue_.size();
            result.queue_high_water = queue_high_water_;
            result.accepted_tuple_serials = accepted_tuple_serials_;
        } catch (...) {
            result.state = LiveJournalStateV1::kFailed;
            result.error = LiveJournalErrorV1::kUnexpectedFailure;
        }
        return result;
    }

    [[nodiscard]] bool failed() const noexcept {
        return failed_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool StopAndFlush() noexcept {
        std::lock_guard<std::mutex> stop_lock(stop_mutex_);
        try {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (state_ == LiveJournalStateV1::kWriting) {
                    state_ = LiveJournalStateV1::kStopping;
                }
                stop_requested_ = true;
                queue_cv_.notify_all();
                commit_cv_.notify_all();
            }
            if (writer_thread_.joinable()) {
                writer_thread_.join();
            }
            const LiveJournalSnapshotV1 snapshot = Snapshot();
            return snapshot.state == LiveJournalStateV1::kStopped &&
                   snapshot.error == LiveJournalErrorV1::kNone &&
                   snapshot.committed_serial == snapshot.accepted_serial;
        } catch (...) {
            Fail(LiveJournalErrorV1::kUnexpectedFailure, EIO);
            return false;
        }
    }

    [[nodiscard]] const LiveJournalConfigV1& config() const noexcept {
        return config_;
    }

    [[nodiscard]] int directory_fd() const noexcept {
        return directory_fd_;
    }

    [[nodiscard]] bool WaitReadable(
        std::uint64_t serial,
        std::chrono::steady_clock::time_point deadline,
        LiveJournalReadDispositionV1* disposition,
        LiveJournalErrorV1* error,
        int* system_error_number) const noexcept {
        if (disposition == nullptr || error == nullptr ||
            system_error_number == nullptr || serial == 0U) {
            return false;
        }
        try {
            std::unique_lock<std::mutex> lock(mutex_);
            const auto ready = [this, serial] {
                return committed_serial_ >= serial ||
                       state_ == LiveJournalStateV1::kFailed ||
                       (state_ == LiveJournalStateV1::kStopped &&
                        committed_serial_ < serial);
            };
            if (!ready() && !commit_cv_.wait_until(lock, deadline, ready)) {
                *disposition = LiveJournalReadDispositionV1::kTimeout;
                *error = LiveJournalErrorV1::kNone;
                *system_error_number = 0;
                return true;
            }
            if (committed_serial_ >= serial) {
                *disposition = LiveJournalReadDispositionV1::kRecord;
                *error = LiveJournalErrorV1::kNone;
                *system_error_number = 0;
                return true;
            }
            if (state_ == LiveJournalStateV1::kFailed) {
                *disposition = LiveJournalReadDispositionV1::kFailed;
                *error = error_;
                *system_error_number = system_error_number_;
                return true;
            }
            *disposition = LiveJournalReadDispositionV1::kEnd;
            *error = LiveJournalErrorV1::kNone;
            *system_error_number = 0;
            return true;
        } catch (...) {
            *disposition = LiveJournalReadDispositionV1::kFailed;
            *error = LiveJournalErrorV1::kUnexpectedFailure;
            *system_error_number = EIO;
            return true;
        }
    }

    void FailFromReader(
        LiveJournalErrorV1 error,
        int system_error_number) noexcept {
        Fail(error, system_error_number);
    }

private:
    [[nodiscard]] bool OpenNewSegment(
        std::uint64_t segment_index,
        std::uint64_t first_serial,
        int* system_error_number) noexcept {
        if (system_error_number != nullptr) {
            *system_error_number = 0;
        }
        if (writer_fd_ >= 0) {
            // One writer batch may straddle a segment boundary.  Synchronize
            // the old segment before closing it so publishing the batch's
            // final committed serial also covers every earlier segment in
            // that batch; close(2) alone is not a durability boundary.
            if (::fdatasync(writer_fd_) != 0) {
                writer_rotation_sync_failed_ = true;
                if (system_error_number != nullptr) {
                    *system_error_number = errno;
                }
                return false;
            }
            static_cast<void>(::close(writer_fd_));
            writer_fd_ = -1;
        }
        const std::string name = SegmentName(segment_index);
        if (name.empty()) {
            if (system_error_number != nullptr) {
                *system_error_number = EOVERFLOW;
            }
            return false;
        }
        writer_fd_ = ::openat(
            directory_fd_,
            name.c_str(),
            O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
            S_IRUSR | S_IWUSR);
        if (writer_fd_ < 0) {
            if (system_error_number != nullptr) {
                *system_error_number = errno;
            }
            return false;
        }
        const auto now = std::chrono::system_clock::now().time_since_epoch();
        const auto mono = std::chrono::steady_clock::now().time_since_epoch();
        const std::uint64_t realtime_ns = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(now)
                .count());
        const std::uint64_t monotonic_ns = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(mono)
                .count());
        const auto header = BuildSegmentHeader(
            config_,
            segment_index,
            first_serial,
            realtime_ns,
            monotonic_ns);
        int write_error = 0;
        if (!WriteFull(writer_fd_, header, &write_error) ||
            ::fdatasync(writer_fd_) != 0 || ::fsync(directory_fd_) != 0) {
            if (system_error_number != nullptr) {
                *system_error_number =
                    write_error != 0 ? write_error : errno;
            }
            return false;
        }
        writer_segment_index_ = segment_index;
        writer_segment_bytes_ = kSegmentHeaderBytes;
        writer_total_bytes_ += kSegmentHeaderBytes;
        return true;
    }

    [[nodiscard]] bool WriteRecord(
        const PendingRecord& pending,
        int* system_error_number) noexcept {
        if (pending.global_serial == 0U ||
            pending.tuple_serial == 0U ||
            pending.segment_index == 0U ||
            pending.body.size() >
                config_.maximum_message_bytes - sdk::kVendorHeadBytes) {
            if (system_error_number != nullptr) {
                *system_error_number = EINVAL;
            }
            return false;
        }
        if (pending.segment_index != writer_segment_index_) {
            if (pending.segment_index != writer_segment_index_ + 1U) {
                if (system_error_number != nullptr) {
                    *system_error_number = EINVAL;
                }
                return false;
            }
            if (!OpenNewSegment(
                    pending.segment_index,
                    pending.global_serial,
                    system_error_number)) {
                return false;
            }
        }
        const std::uint64_t record_bytes_u64 = pending.disk_bytes();
        if (record_bytes_u64 >
                std::numeric_limits<std::uint32_t>::max() ||
            writer_segment_bytes_ + record_bytes_u64 >
                config_.segment_maximum_bytes ||
            writer_total_bytes_ + record_bytes_u64 >
                config_.maximum_total_bytes) {
            if (system_error_number != nullptr) {
                *system_error_number = ENOSPC;
            }
            return false;
        }
        std::array<std::byte, kRecordHeaderBytes> header{};
        std::copy(kRecordMagic.begin(), kRecordMagic.end(), header.begin());
        StoreInteger(&header, 8U, kJournalMajor);
        StoreInteger(&header, 10U, kJournalMinor);
        StoreInteger(
            &header, 12U, static_cast<std::uint32_t>(header.size()));
        StoreInteger(
            &header, 16U, static_cast<std::uint32_t>(record_bytes_u64));
        StoreInteger(
            &header, 20U, static_cast<std::uint32_t>(pending.body.size()));
        StoreInteger(&header, 24U, pending.global_serial);
        StoreInteger(&header, 32U, pending.tuple_serial);
        StoreInteger(&header, 40U, pending.recv_realtime_ns);
        StoreInteger(&header, 48U, pending.recv_monotonic_ns);
        const sdk::VendorHeadView vendor_head(pending.vendor_head);
        StoreInteger(&header, 56U, vendor_head.sequence_id());
        StoreInteger(&header, 64U, pending.native_sequence);
        StoreInteger(&header, 72U, pending.native_channel);
        StoreInteger(&header, 76U, config_.trade_date);
        StoreInteger(&header, 80U, pending.segment_index);
        StoreInteger(&header, 88U, writer_segment_bytes_);
        header[96U] = std::byte{pending.key.service_id};
        header[97U] = std::byte{pending.native_market};
        header[98U] = static_cast<std::byte>(
            pending.native_valid ? 1U : 0U);
        header[99U] = std::byte{pending.source_slot};
        StoreInteger(&header, 100U, pending.key.service_version);
        StoreInteger(&header, 102U, pending.key.message_id);
        std::copy(
            pending.vendor_head.begin(),
            pending.vendor_head.end(),
            header.begin() + 104U);
        const common::Sha256Digest digest =
            common::ComputeSha256(pending.body);
        std::copy(
            digest.begin(),
            digest.end(),
            header.begin() + kRecordDigestOffset);
        StoreInteger(
            &header,
            kRecordBodyCrcOffset,
            common::ComputeCrc32c(pending.body));
        StoreInteger<std::uint32_t>(&header, kRecordCrcOffset, 0U);
        common::Crc32cState crc;
        crc.Update(header);
        crc.Update(pending.body);
        StoreInteger(&header, kRecordCrcOffset, crc.Finalize());
        return WriteFull(writer_fd_, header, system_error_number) &&
               WriteFull(writer_fd_, pending.body, system_error_number) &&
               (writer_segment_bytes_ += record_bytes_u64, true) &&
               (writer_total_bytes_ += record_bytes_u64, true);
    }

    void WriterLoop() noexcept {
        try {
            for (;;) {
                std::vector<std::unique_ptr<PendingRecord>> batch;
                {
                    std::unique_lock<std::mutex> lock(mutex_);
                    if (queue_.empty() && !stop_requested_ &&
                        state_ != LiveJournalStateV1::kFailed) {
                        static_cast<void>(queue_cv_.wait_for(
                            lock, config_.sync_interval));
                    }
                    if (state_ == LiveJournalStateV1::kFailed) {
                        commit_cv_.notify_all();
                        return;
                    }
                    if (queue_.empty() && stop_requested_) {
                        state_ = LiveJournalStateV1::kStopped;
                        commit_cv_.notify_all();
                        return;
                    }
                    const std::size_t count = std::min(
                        config_.sync_batch_records, queue_.size());
                    batch.reserve(count);
                    for (std::size_t index = 0U; index < count; ++index) {
                        batch.push_back(std::move(queue_.front()));
                        queue_.pop_front();
                    }
                }
                if (batch.empty()) {
                    continue;
                }
                int write_error = 0;
                writer_rotation_sync_failed_ = false;
                bool written = true;
                for (const auto& pending : batch) {
                    if (pending == nullptr ||
                        !WriteRecord(*pending, &write_error)) {
                        written = false;
                        break;
                    }
                }
                if (!written || ::fdatasync(writer_fd_) != 0) {
                    Fail(
                        written || writer_rotation_sync_failed_
                                ? LiveJournalErrorV1::kSyncFailed
                                : write_error == ENOSPC
                                ? LiveJournalErrorV1::kCapacityExhausted
                                : LiveJournalErrorV1::kWriteFailed,
                        write_error != 0 ? write_error : errno);
                    return;
                }
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    committed_serial_ = batch.back()->global_serial;
                    committed_bytes_ = writer_total_bytes_;
                    commit_cv_.notify_all();
                }
            }
        } catch (const std::bad_alloc&) {
            Fail(LiveJournalErrorV1::kResourceExhausted, ENOMEM);
        } catch (...) {
            Fail(LiveJournalErrorV1::kUnexpectedFailure, EIO);
        }
    }

    void Fail(
        LiveJournalErrorV1 error,
        int system_error_number) noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            FailLocked(error, system_error_number);
        } catch (...) {
        }
    }

    void FailLocked(
        LiveJournalErrorV1 error,
        int system_error_number) noexcept {
        if (error_ == LiveJournalErrorV1::kNone) {
            error_ = error;
            system_error_number_ = system_error_number;
        }
        state_ = LiveJournalStateV1::kFailed;
        failed_.store(true, std::memory_order_release);
        stop_requested_ = true;
        queue_cv_.notify_all();
        commit_cv_.notify_all();
    }

    LiveJournalConfigV1 config_;
    int directory_fd_ = -1;
    int writer_fd_ = -1;
    std::uint64_t writer_segment_index_ = 0U;
    std::uint64_t writer_segment_bytes_ = 0U;
    std::uint64_t writer_total_bytes_ = 0U;

    mutable std::mutex mutex_;
    mutable std::condition_variable queue_cv_;
    mutable std::condition_variable commit_cv_;
    std::deque<std::unique_ptr<PendingRecord>> queue_;
    std::thread writer_thread_;
    std::mutex stop_mutex_;
    LiveJournalStateV1 state_ = LiveJournalStateV1::kFailed;
    LiveJournalErrorV1 error_ = LiveJournalErrorV1::kNone;
    int system_error_number_ = 0;
    std::atomic<bool> failed_{true};
    bool stop_requested_ = false;
    std::uint64_t accepted_serial_ = 0U;
    std::uint64_t committed_serial_ = 0U;
    std::uint64_t reserved_bytes_ = 0U;
    std::uint64_t committed_bytes_ = 0U;
    std::uint64_t reservation_segment_bytes_ = 0U;
    std::uint64_t reservation_segment_count_ = 0U;
    // Accessed only by the writer thread after initialization.
    bool writer_rotation_sync_failed_ = false;
    std::size_t queue_high_water_ = 0U;
    std::array<std::uint64_t, sdk::kProductionMessageCountV1>
        accepted_tuple_serials_{};
};

class MdlLiveJournalReaderV1::Impl final {
public:
    explicit Impl(std::shared_ptr<MdlLiveJournalV1::Impl> journal)
        : journal_(std::move(journal)) {}

    ~Impl() {
        if (segment_fd_ >= 0) {
            static_cast<void>(::close(segment_fd_));
        }
    }

    [[nodiscard]] LiveJournalReadResultV1 ReadNext(
        std::chrono::steady_clock::time_point deadline) noexcept {
        LiveJournalReadResultV1 result{};
        if (journal_ == nullptr || next_serial_ == 0U) {
            result.error = LiveJournalErrorV1::kUnexpectedFailure;
            result.system_error_number = EINVAL;
            return result;
        }
        LiveJournalReadDispositionV1 availability =
            LiveJournalReadDispositionV1::kFailed;
        if (!journal_->WaitReadable(
                next_serial_,
                deadline,
                &availability,
                &result.error,
                &result.system_error_number)) {
            result.error = LiveJournalErrorV1::kUnexpectedFailure;
            result.system_error_number = EIO;
            return result;
        }
        if (availability != LiveJournalReadDispositionV1::kRecord) {
            result.disposition = availability;
            return result;
        }
        try {
            for (;;) {
                if (segment_fd_ < 0 && !OpenSegment(&result)) {
                    return result;
                }
                std::array<std::byte, kRecordHeaderBytes> header{};
                int read_error = 0;
                const PreadDisposition read = PreadFull(
                    segment_fd_, segment_offset_, header, &read_error);
                if (read == PreadDisposition::kEof) {
                    static_cast<void>(::close(segment_fd_));
                    segment_fd_ = -1;
                    if (segment_index_ ==
                        std::numeric_limits<std::uint64_t>::max()) {
                        return Corrupt(EOVERFLOW);
                    }
                    ++segment_index_;
                    segment_offset_ = kSegmentHeaderBytes;
                    continue;
                }
                if (read != PreadDisposition::kOk) {
                    return Corrupt(read_error);
                }
                const std::uint32_t body_bytes =
                    LoadInteger<std::uint32_t>(header, 20U);
                const std::uint32_t record_bytes =
                    LoadInteger<std::uint32_t>(header, 16U);
                if (!RecordHeaderBasicValid(
                        header, record_bytes, body_bytes)) {
                    return Corrupt(EILSEQ);
                }
                std::vector<std::byte> body(body_bytes);
                const PreadDisposition body_read = PreadFull(
                    segment_fd_,
                    segment_offset_ + kRecordHeaderBytes,
                    body,
                    &read_error);
                if (body_read != PreadDisposition::kOk) {
                    return Corrupt(
                        read_error != 0 ? read_error : EILSEQ);
                }
                if (!RecordIntegrityValid(header, body)) {
                    return Corrupt(EILSEQ);
                }
                std::unique_ptr<MdlLiveJournalRecordV1> record(
                    new MdlLiveJournalRecordV1());
                std::memcpy(
                    &record->head_,
                    header.data() + 104U,
                    sizeof(record->head_));
                record->body_ = std::move(body);
                record->key_.service_id =
                    std::to_integer<std::uint8_t>(header[96U]);
                record->key_.service_version =
                    LoadInteger<std::uint16_t>(header, 100U);
                record->key_.message_id =
                    LoadInteger<std::uint16_t>(header, 102U);
                record->global_serial_ = next_serial_;
                record->tuple_serial_ =
                    LoadInteger<std::uint64_t>(header, 32U);
                record->recv_realtime_ns_ =
                    LoadInteger<std::uint64_t>(header, 40U);
                record->recv_monotonic_ns_ =
                    LoadInteger<std::uint64_t>(header, 48U);
                record->native_sequence_ =
                    LoadInteger<std::uint64_t>(header, 64U);
                record->native_channel_ =
                    LoadInteger<std::uint32_t>(header, 72U);
                record->native_market_ =
                    std::to_integer<std::uint8_t>(header[97U]);
                record->native_sequence_valid_ =
                    std::to_integer<std::uint8_t>(header[98U]) == 1U;
                segment_offset_ += record_bytes;
                ++next_serial_;
                result.disposition =
                    LiveJournalReadDispositionV1::kRecord;
                result.error = LiveJournalErrorV1::kNone;
                result.system_error_number = 0;
                result.record = std::move(record);
                return result;
            }
        } catch (const std::bad_alloc&) {
            return Failed(
                LiveJournalErrorV1::kResourceExhausted, ENOMEM);
        } catch (...) {
            return Failed(
                LiveJournalErrorV1::kUnexpectedFailure, EIO);
        }
    }

    [[nodiscard]] std::uint64_t next_serial() const noexcept {
        return next_serial_;
    }

private:
    [[nodiscard]] bool OpenSegment(
        LiveJournalReadResultV1* result) noexcept {
        const std::string name = SegmentName(segment_index_);
        if (name.empty()) {
            *result = Corrupt(EOVERFLOW);
            return false;
        }
        segment_fd_ = ::openat(
            journal_->directory_fd(),
            name.c_str(),
            O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
        if (segment_fd_ < 0) {
            *result = Failed(
                LiveJournalErrorV1::kSegmentOpenFailed, errno);
            return false;
        }
        std::array<std::byte, kSegmentHeaderBytes> header{};
        int read_error = 0;
        if (PreadFull(segment_fd_, 0U, header, &read_error) !=
                PreadDisposition::kOk ||
            !SegmentHeaderValid(
                header,
                journal_->config(),
                segment_index_,
                next_serial_)) {
            *result = Corrupt(
                read_error != 0 ? read_error : EILSEQ);
            return false;
        }
        segment_offset_ = kSegmentHeaderBytes;
        return true;
    }

    [[nodiscard]] bool RecordHeaderBasicValid(
        const std::array<std::byte, kRecordHeaderBytes>& header,
        std::uint32_t record_bytes,
        std::uint32_t body_bytes) const noexcept {
        if (!std::equal(
                kRecordMagic.begin(),
                kRecordMagic.end(),
                header.begin()) ||
            LoadInteger<std::uint16_t>(header, 8U) != kJournalMajor ||
            LoadInteger<std::uint16_t>(header, 10U) != kJournalMinor ||
            LoadInteger<std::uint32_t>(header, 12U) != header.size() ||
            record_bytes != kRecordHeaderBytes + body_bytes ||
            body_bytes > journal_->config().maximum_message_bytes -
                             sdk::kVendorHeadBytes ||
            LoadInteger<std::uint64_t>(header, 24U) != next_serial_ ||
            LoadInteger<std::uint64_t>(header, 32U) == 0U ||
            LoadInteger<std::uint64_t>(header, 40U) == 0U ||
            LoadInteger<std::uint64_t>(header, 48U) == 0U ||
            LoadInteger<std::uint32_t>(header, 76U) !=
                journal_->config().trade_date ||
            LoadInteger<std::uint64_t>(header, 80U) != segment_index_ ||
            LoadInteger<std::uint64_t>(header, 88U) != segment_offset_ ||
            std::to_integer<std::uint8_t>(header[98U]) > 1U ||
            std::to_integer<std::uint8_t>(header[99U]) >=
                realtime::kOwnedIngressSourceCountV1 ||
            !std::all_of(
                header.begin() + 168U,
                header.end(),
                [](std::byte value) noexcept {
                    return value == std::byte{0U};
                })) {
            return false;
        }
        sdk::VendorHeadBytes head_bytes{};
        std::copy_n(
            header.begin() + 104U,
            head_bytes.size(),
            head_bytes.begin());
        const sdk::VendorHeadView head(head_bytes);
        const sdk::MessageKey key{
            std::to_integer<std::uint8_t>(header[96U]),
            LoadInteger<std::uint16_t>(header, 100U),
            LoadInteger<std::uint16_t>(header, 102U)};
        return TupleIndex(key).has_value() &&
               head.head_size() == sdk::kVendorHeadBytes &&
               head.message_size() == sdk::kVendorHeadBytes + body_bytes &&
               head.service_id() == key.service_id &&
               head.service_version() == key.service_version &&
               head.message_id() == key.message_id &&
               head.sequence_id() ==
                   LoadInteger<std::uint64_t>(header, 56U);
    }

    [[nodiscard]] bool RecordIntegrityValid(
        const std::array<std::byte, kRecordHeaderBytes>& header,
        std::span<const std::byte> body) const noexcept {
        const common::Sha256Digest digest = common::ComputeSha256(body);
        if (!std::equal(
                digest.begin(),
                digest.end(),
                header.begin() + kRecordDigestOffset) ||
            common::ComputeCrc32c(body) !=
                LoadInteger<std::uint32_t>(
                    header, kRecordBodyCrcOffset)) {
            return false;
        }
        auto copy = header;
        const std::uint32_t expected =
            LoadInteger<std::uint32_t>(copy, kRecordCrcOffset);
        StoreInteger<std::uint32_t>(&copy, kRecordCrcOffset, 0U);
        common::Crc32cState crc;
        crc.Update(copy);
        crc.Update(body);
        return expected != 0U && crc.Finalize() == expected;
    }

    [[nodiscard]] LiveJournalReadResultV1 Corrupt(
        int system_error_number) noexcept {
        journal_->FailFromReader(
            LiveJournalErrorV1::kCorruptRecord,
            system_error_number);
        return Failed(
            LiveJournalErrorV1::kCorruptRecord,
            system_error_number);
    }

    [[nodiscard]] static LiveJournalReadResultV1 Failed(
        LiveJournalErrorV1 error,
        int system_error_number) noexcept {
        LiveJournalReadResultV1 result{};
        result.disposition = LiveJournalReadDispositionV1::kFailed;
        result.error = error;
        result.system_error_number = system_error_number;
        return result;
    }

    std::shared_ptr<MdlLiveJournalV1::Impl> journal_;
    int segment_fd_ = -1;
    std::uint64_t segment_index_ = 1U;
    std::uint64_t segment_offset_ = kSegmentHeaderBytes;
    std::uint64_t next_serial_ = 1U;
};

mdl::MDLMessageHead* MdlLiveJournalRecordV1::GetHead() const {
    return const_cast<mdl::MDLMessageHead*>(&head_);
}

char* MdlLiveJournalRecordV1::GetBody() const {
    return body_.empty()
               ? nullptr
               : reinterpret_cast<char*>(
                     const_cast<std::byte*>(body_.data()));
}

std::size_t MdlLiveJournalRecordV1::wire_size() const noexcept {
    return sizeof(head_) + body_.size();
}

MdlLiveJournalReaderV1::MdlLiveJournalReaderV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

MdlLiveJournalReaderV1::~MdlLiveJournalReaderV1() = default;

LiveJournalReadResultV1 MdlLiveJournalReaderV1::ReadNext(
    std::chrono::steady_clock::time_point deadline) noexcept {
    return impl_ == nullptr
               ? LiveJournalReadResultV1{}
               : impl_->ReadNext(deadline);
}

std::uint64_t MdlLiveJournalReaderV1::next_serial() const noexcept {
    return impl_ == nullptr ? 0U : impl_->next_serial();
}

MdlLiveJournalV1::MdlLiveJournalV1(
    std::shared_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

MdlLiveJournalV1::~MdlLiveJournalV1() {
    static_cast<void>(StopAndFlush());
}

LiveJournalErrorV1 MdlLiveJournalV1::Create(
    LiveJournalConfigV1 config,
    std::shared_ptr<MdlLiveJournalV1>* output,
    int* system_error_number) noexcept {
    if (output == nullptr) {
        return LiveJournalErrorV1::kInvalidConfiguration;
    }
    output->reset();
    try {
        auto impl = std::make_shared<Impl>(std::move(config));
        const LiveJournalErrorV1 error =
            impl->Initialize(system_error_number);
        if (error != LiveJournalErrorV1::kNone) {
            return error;
        }
        *output = std::shared_ptr<MdlLiveJournalV1>(
            new MdlLiveJournalV1(std::move(impl)));
        return LiveJournalErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return LiveJournalErrorV1::kResourceExhausted;
    } catch (...) {
        return LiveJournalErrorV1::kUnexpectedFailure;
    }
}

bool MdlLiveJournalV1::Capture(
    const realtime::RealtimeIngressCaptureInputV1& input) noexcept {
    return impl_ != nullptr && impl_->Capture(input);
}

bool MdlLiveJournalV1::CaptureTupleFence(
    const sdk::MessageKey& key,
    LiveJournalTupleFenceV1* output) const noexcept {
    return impl_ != nullptr && impl_->CaptureTupleFence(key, output);
}

bool MdlLiveJournalV1::CreateReader(
    std::unique_ptr<MdlLiveJournalReaderV1>* output) noexcept {
    if (output == nullptr || impl_ == nullptr) {
        return false;
    }
    output->reset();
    try {
        output->reset(new MdlLiveJournalReaderV1(
            std::make_unique<MdlLiveJournalReaderV1::Impl>(impl_)));
        return true;
    } catch (...) {
        return false;
    }
}

bool MdlLiveJournalV1::failed() const noexcept {
    return impl_ == nullptr || impl_->failed();
}

LiveJournalSnapshotV1 MdlLiveJournalV1::Snapshot() const noexcept {
    return impl_ == nullptr ? LiveJournalSnapshotV1{} : impl_->Snapshot();
}

bool MdlLiveJournalV1::StopAndFlush() noexcept {
    return impl_ != nullptr && impl_->StopAndFlush();
}

}  // namespace l2flow::recovery
