#include "l2flow/ipc/partial_order_event_journal_v2.h"

#include "l2flow/common/crc32c.h"
#include "l2flow/ipc/certified_order_event_wire_v1.h"
#include "l2flow/ipc/instrument_derived_event_wire_v1.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <span>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#ifndef F_SEAL_FUTURE_WRITE
#define F_SEAL_FUTURE_WRITE 0x0010
#endif

namespace l2flow::ipc {
namespace {

static_assert(kPartialOrderEventOrderStateSlotBytesV2 <= 4096U);

template <typename T>
[[nodiscard]] std::atomic_ref<T> Atomic(T& value) noexcept {
    return std::atomic_ref<T>(value);
}

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
    if (descriptor == nullptr || *descriptor < 0) {
        return;
    }
    int result = -1;
    do {
        result = ::close(*descriptor);
    } while (result < 0 && errno == EINTR);
    *descriptor = -1;
}

[[nodiscard]] bool IdentityNonzero(
    const common::Identity128& identity) noexcept {
    return std::any_of(
        identity.begin(), identity.end(), [](std::byte value) {
            return value != std::byte{0};
        });
}

void CopyIdentity(
    const common::Identity128& source,
    std::array<std::uint8_t, 16U>* output) noexcept {
    std::memcpy(output->data(), source.data(), output->size());
}

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
    constexpr std::uint64_t kBillion = 1'000'000'000U;
    const std::uint64_t seconds =
        static_cast<std::uint64_t>(value.tv_sec);
    if (seconds >
        (std::numeric_limits<std::uint64_t>::max() -
         static_cast<std::uint64_t>(value.tv_nsec)) /
            kBillion) {
        return false;
    }
    *output =
        seconds * kBillion +
        static_cast<std::uint64_t>(value.tv_nsec);
    return *output != 0U;
}

[[nodiscard]] bool AllocateBacking(
    int descriptor,
    std::uint64_t offset,
    std::uint64_t bytes,
    int* system_error_number) noexcept {
    if (descriptor < 0 || bytes == 0U ||
        offset > static_cast<std::uint64_t>(
                     std::numeric_limits<off_t>::max()) ||
        bytes > static_cast<std::uint64_t>(
                    std::numeric_limits<off_t>::max()) ||
        offset >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max()) -
                bytes) {
        SetSystemError(system_error_number, EOVERFLOW);
        return false;
    }
    int result = -1;
    do {
        result = ::fallocate(
            descriptor,
            0,
            static_cast<off_t>(offset),
            static_cast<off_t>(bytes));
    } while (result != 0 && errno == EINTR);
    if (result != 0) {
        SetSystemError(system_error_number, errno);
        return false;
    }
    return true;
}

[[nodiscard]] bool StateValueValid(
    PartialOrderEventServiceStateV2 state) noexcept {
    return state >= PartialOrderEventServiceStateV2::kInitializing &&
           state <= PartialOrderEventServiceStateV2::kStoppedClean;
}

[[nodiscard]] bool ErrorValueValid(
    PartialOrderEventLastErrorV2 error) noexcept {
    return error >= PartialOrderEventLastErrorV2::kNone &&
           error <=
               PartialOrderEventLastErrorV2::kPublicationInvariant;
}

[[nodiscard]] bool StateRequiresStale(
    PartialOrderEventServiceStateV2 state) noexcept {
    return state == PartialOrderEventServiceStateV2::kReordering ||
           state == PartialOrderEventServiceStateV2::kCatchingUp ||
           state ==
               PartialOrderEventServiceStateV2::kFrozenConflict ||
           state ==
               PartialOrderEventServiceStateV2::kFrozenResource ||
           state == PartialOrderEventServiceStateV2::kRestarting ||
           state ==
               PartialOrderEventServiceStateV2::kCorrectionPending;
}

struct OrderKey final {
    std::uint32_t market = 0U;
    std::uint32_t instrument_id = 0U;
    std::int64_t channel = 0;
    std::int64_t order_id = 0;

    [[nodiscard]] friend bool operator==(
        const OrderKey&,
        const OrderKey&) noexcept = default;
};

[[nodiscard]] bool KeyLess(
    const PartialOrderEventChannelHealthV2& left,
    const PartialOrderEventChannelHealthV2& right) noexcept {
    if (left.market != right.market) {
        return left.market < right.market;
    }
    return left.channel < right.channel;
}

[[nodiscard]] std::uint64_t Mix64(std::uint64_t value) noexcept {
    value ^= value >> 30U;
    value *= 0xbf58476d1ce4e5b9ULL;
    value ^= value >> 27U;
    value *= 0x94d049bb133111ebULL;
    value ^= value >> 31U;
    return value;
}

[[nodiscard]] std::uint64_t HashOrderKey(
    const OrderKey& key) noexcept {
    std::uint64_t value =
        (static_cast<std::uint64_t>(key.market) << 32U) |
        static_cast<std::uint64_t>(key.instrument_id);
    value = Mix64(value);
    value ^= Mix64(std::bit_cast<std::uint64_t>(key.channel));
    value ^= Mix64(std::bit_cast<std::uint64_t>(key.order_id));
    return Mix64(value);
}

template <typename Wire>
void AtomicStoreWords(Wire* destination, const Wire& source) noexcept {
    static_assert(sizeof(Wire) % sizeof(std::uint64_t) == 0U);
    std::array<
        std::uint64_t,
        sizeof(Wire) / sizeof(std::uint64_t)>
        words{};
    std::memcpy(words.data(), &source, sizeof(source));
    auto* const destination_words =
        reinterpret_cast<std::uint64_t*>(destination);
    for (std::size_t index = 0U; index < words.size(); ++index) {
        Atomic(destination_words[index])
            .store(words[index], std::memory_order_relaxed);
    }
}

}  // namespace

class PartialOrderEventJournalProducerV2::Impl final {
public:
    explicit Impl(PartialOrderEventJournalConfigV2 config)
        : config_(std::move(config)) {}

    ~Impl() {
        CloseDescriptor(&read_only_fd_);
        if (mapping_ != MAP_FAILED) {
            static_cast<void>(::munmap(
                mapping_, static_cast<std::size_t>(mapping_bytes_)));
        }
        CloseDescriptor(&memfd_);
    }

    [[nodiscard]] PartialOrderEventJournalCreateErrorV2 Initialize(
        int* system_error_number) noexcept {
        SetSystemError(system_error_number, 0);
        if constexpr (std::endian::native != std::endian::little) {
            return PartialOrderEventJournalCreateErrorV2::
                kInvalidConfiguration;
        }
        if (!IdentityNonzero(config_.run_id) ||
            config_.session_epoch == 0U || config_.trade_date == 0U ||
            config_.publication_generation == 0U ||
            config_.correction_epoch == 0U ||
            config_.coverage_start_unix_ns == 0U ||
            config_.ordering_quality !=
                PartialOrderEventOrderingQualityV2::
                    kBoundedReorderedPartial ||
            config_.event_capacity == 0U ||
            config_.affected_channel_capacity == 0U ||
            config_.order_state_capacity == 0U ||
            (config_.order_state_capacity &
                 (config_.order_state_capacity - 1U)) != 0U ||
            config_.lazy_commit_chunk_bytes < 4096U ||
            config_.lazy_commit_chunk_bytes % 4096U != 0U) {
            return PartialOrderEventJournalCreateErrorV2::
                kInvalidConfiguration;
        }

        using namespace partial_order_event_wire_v2_detail;
        std::uint64_t event_bytes = 0U;
        std::uint64_t event_end = 0U;
        std::uint64_t channel_bytes = 0U;
        std::uint64_t order_bytes = 0U;
        std::uint64_t logical_end = 0U;
        if (!CheckedMultiply(
                config_.event_capacity,
                kPartialOrderEventSlotBytesV2,
                &event_bytes) ||
            !CheckedAdd(
                kPartialOrderEventHeaderBytesV2,
                event_bytes,
                &event_end) ||
            !AlignUp(event_end, 4096U, &channel_bank_offsets_[0U]) ||
            !CheckedMultiply(
                config_.affected_channel_capacity,
                kPartialOrderEventChannelHealthBytesV2,
                &channel_bytes) ||
            !AlignUp(channel_bytes, 4096U, &channel_bank_bytes_) ||
            !CheckedAdd(
                channel_bank_offsets_[0U],
                channel_bank_bytes_,
                &channel_bank_offsets_[1U]) ||
            !CheckedAdd(
                channel_bank_offsets_[1U],
                channel_bank_bytes_,
                &order_states_offset_) ||
            !CheckedMultiply(
                config_.order_state_capacity,
                kPartialOrderEventOrderStateSlotBytesV2,
                &order_bytes) ||
            !CheckedAdd(
                order_states_offset_, order_bytes, &logical_end) ||
            !AlignUp(logical_end, 4096U, &mapping_bytes_) ||
            (config_.maximum_mapping_bytes != 0U &&
             mapping_bytes_ > config_.maximum_mapping_bytes) ||
            mapping_bytes_ > static_cast<std::uint64_t>(
                                 std::numeric_limits<std::size_t>::max()) ||
            mapping_bytes_ > static_cast<std::uint64_t>(
                                 std::numeric_limits<off_t>::max())) {
            return PartialOrderEventJournalCreateErrorV2::
                kLayoutOverflow;
        }

        maximum_state_updates_per_commit_ =
            config_.maximum_order_state_updates_per_commit == 0U
                ? config_.order_state_capacity
                : config_.maximum_order_state_updates_per_commit;
        if (maximum_state_updates_per_commit_ >
                config_.order_state_capacity ||
            maximum_state_updates_per_commit_ >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max()) ||
            maximum_state_updates_per_commit_ >
                static_cast<std::uint64_t>(
                    state_plans_.max_size()) ||
            config_.order_state_capacity >
                static_cast<std::uint64_t>(
                    state_plan_slots_.max_size())) {
            return PartialOrderEventJournalCreateErrorV2::
                kInvalidConfiguration;
        }
        order_state_region_bytes_ = mapping_bytes_ - order_states_offset_;
        order_state_backing_chunk_count_ =
            order_state_region_bytes_ / config_.lazy_commit_chunk_bytes +
            (order_state_region_bytes_ % config_.lazy_commit_chunk_bytes != 0U
                 ? 1U
                 : 0U);
        if (order_state_backing_chunk_count_ == 0U ||
            order_state_backing_chunk_count_ >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max()) ||
            order_state_backing_chunk_count_ >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max() - 63U)) {
            return PartialOrderEventJournalCreateErrorV2::
                kLayoutOverflow;
        }
        const std::size_t backing_word_count =
            (static_cast<std::size_t>(
                 order_state_backing_chunk_count_) +
             63U) /
            64U;
        try {
            state_plans_.reserve(static_cast<std::size_t>(
                maximum_state_updates_per_commit_));
            state_plan_slots_.resize(
                static_cast<std::size_t>(config_.order_state_capacity));
            order_state_backing_words_.assign(backing_word_count, 0U);
            order_state_planning_words_.assign(backing_word_count, 0U);
            const std::uint64_t maximum_planned_chunks = std::min(
                order_state_backing_chunk_count_,
                maximum_state_updates_per_commit_ >
                        std::numeric_limits<std::uint64_t>::max() / 2U
                    ? std::numeric_limits<std::uint64_t>::max()
                    : maximum_state_updates_per_commit_ * 2U);
            order_state_chunk_plans_.reserve(
                static_cast<std::size_t>(maximum_planned_chunks));
        } catch (const std::bad_alloc&) {
            return PartialOrderEventJournalCreateErrorV2::
                kResourceExhausted;
        } catch (...) {
            return PartialOrderEventJournalCreateErrorV2::
                kUnexpectedFailure;
        }

        memfd_ = ::memfd_create(
            "l2flow-partial-order-events-v2",
            MFD_CLOEXEC | MFD_ALLOW_SEALING);
        if (memfd_ < 0 ||
            ::ftruncate(memfd_, static_cast<off_t>(mapping_bytes_)) !=
                0) {
            SetSystemError(system_error_number, errno);
            return PartialOrderEventJournalCreateErrorV2::
                kMappingCreateFailed;
        }
        if (!AllocateBacking(
                memfd_,
                0U,
                kPartialOrderEventHeaderBytesV2,
                system_error_number) ||
            !AllocateBacking(
                memfd_,
                channel_bank_offsets_[0U],
                channel_bank_bytes_ * 2U,
                system_error_number)) {
            return PartialOrderEventJournalCreateErrorV2::
                kResourceExhausted;
        }

        mapping_ = ::mmap(
            nullptr,
            static_cast<std::size_t>(mapping_bytes_),
            PROT_READ | PROT_WRITE,
            MAP_SHARED,
            memfd_,
            0);
        if (mapping_ == MAP_FAILED ||
            ::madvise(
                mapping_,
                static_cast<std::size_t>(mapping_bytes_),
                MADV_DONTFORK) != 0) {
            SetSystemError(system_error_number, errno);
            return PartialOrderEventJournalCreateErrorV2::
                kMappingCreateFailed;
        }
        std::memset(mapping_, 0, kPartialOrderEventHeaderBytesV2);
        std::memset(
            static_cast<std::byte*>(mapping_) +
                channel_bank_offsets_[0U],
            0,
            static_cast<std::size_t>(channel_bank_bytes_ * 2U));

        constexpr char kDescriptorPrefix[] = "/proc/self/fd/";
        std::array<
            char,
            sizeof(kDescriptorPrefix) +
                static_cast<std::size_t>(
                    std::numeric_limits<int>::digits10) +
                1U>
            descriptor_path{};
        std::memcpy(
            descriptor_path.data(),
            kDescriptorPrefix,
            sizeof(kDescriptorPrefix) - 1U);
        char* const descriptor_number_begin =
            descriptor_path.data() + sizeof(kDescriptorPrefix) - 1U;
        const auto descriptor_conversion = std::to_chars(
            descriptor_number_begin,
            descriptor_path.data() + descriptor_path.size() - 1U,
            memfd_);
        if (descriptor_conversion.ec != std::errc{}) {
            SetSystemError(system_error_number, EOVERFLOW);
            return PartialOrderEventJournalCreateErrorV2::
                kUnexpectedFailure;
        }
        *descriptor_conversion.ptr = '\0';
        read_only_fd_ =
            ::open(descriptor_path.data(), O_RDONLY | O_CLOEXEC);
        if (read_only_fd_ < 0) {
            SetSystemError(system_error_number, errno);
            return PartialOrderEventJournalCreateErrorV2::
                kReadOnlyHandleFailed;
        }
        constexpr int seals =
            F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_FUTURE_WRITE |
            F_SEAL_SEAL;
        if (::fcntl(memfd_, F_ADD_SEALS, seals) != 0) {
            SetSystemError(system_error_number, errno);
            return PartialOrderEventJournalCreateErrorV2::kSealFailed;
        }

        header_ = static_cast<PartialOrderEventHeaderV2*>(mapping_);
        event_slots_ = reinterpret_cast<PartialOrderEventSlotV2*>(
            static_cast<std::byte*>(mapping_) +
            kPartialOrderEventHeaderBytesV2);
        for (std::size_t bank = 0U; bank < channel_banks_.size();
             ++bank) {
            channel_banks_[bank] =
                reinterpret_cast<PartialOrderEventChannelHealthV2*>(
                    static_cast<std::byte*>(mapping_) +
                    channel_bank_offsets_[bank]);
        }
        order_state_slots_ =
            reinterpret_cast<PartialOrderEventOrderStateSlotV2*>(
                static_cast<std::byte*>(mapping_) +
                order_states_offset_);

        std::uint64_t heartbeat = 0U;
        if (!ReadMonotonicNs(&heartbeat)) {
            return PartialOrderEventJournalCreateErrorV2::
                kUnexpectedFailure;
        }
        header_->magic = kPartialOrderEventMagicV2;
        header_->abi_major = kPartialOrderEventWireMajorV2;
        header_->abi_minor = kPartialOrderEventWireMinorV2;
        header_->header_bytes = kPartialOrderEventHeaderBytesV2;
        header_->endian_marker = kPartialOrderEventEndianMarkerV2;
        header_->temporal_coverage =
            PartialOrderEventTemporalCoverageV2::kProcessStart;
        header_->total_mapping_bytes = mapping_bytes_;
        CopyIdentity(config_.run_id, &header_->run_id);
        header_->session_epoch = config_.session_epoch;
        header_->trade_date = config_.trade_date;
        header_->publication_generation =
            config_.publication_generation;
        header_->correction_epoch = config_.correction_epoch;
        header_->coverage_start_unix_ns =
            config_.coverage_start_unix_ns;
        header_->ordering_quality = config_.ordering_quality;
        header_->slots_offset = kPartialOrderEventHeaderBytesV2;
        header_->event_capacity = config_.event_capacity;
        header_->slot_stride = kPartialOrderEventSlotBytesV2;
        header_->region_alignment = kPartialOrderEventAlignmentV2;
        header_->channel_bank_offsets[0U] =
            channel_bank_offsets_[0U];
        header_->channel_bank_offsets[1U] =
            channel_bank_offsets_[1U];
        header_->channel_bank_bytes = channel_bank_bytes_;
        header_->channel_capacity =
            config_.affected_channel_capacity;
        header_->channel_stride =
            kPartialOrderEventChannelHealthBytesV2;
        header_->order_states_offset = order_states_offset_;
        header_->order_state_capacity = config_.order_state_capacity;
        header_->order_state_stride =
            kPartialOrderEventOrderStateSlotBytesV2;

        PartialOrderEventCommitCutV2 initial{};
        initial.commit_sequence = 1U;
        initial.heartbeat_monotonic_ns = heartbeat;
        initial.history_generation = 0U;
        initial.order_state_generation =
            config_.publication_generation;
        initial.committed_event_region_bytes =
            committed_event_region_bytes_;
        initial.state =
            PartialOrderEventServiceStateV2::kInitializing;
        initial.stale = 1U;
        initial.active_channel_bank = 0U;
        initial.channel_bank_crc32c = common::ComputeCrc32c(
            std::span<const std::byte>{});
        initial.cut_crc32c =
            PartialOrderEventCommitCutCrc32cV2(initial);
        initial.publish_tag = 2U;
        header_->cuts[0U] = initial;

        if (!PartialOrderEventHeaderLayoutCanonicalV2(*header_) ||
            !PartialOrderEventCommitCutCanonicalV2(
                *header_, header_->cuts[0U], 0U)) {
            return PartialOrderEventJournalCreateErrorV2::
                kUnexpectedFailure;
        }
        return PartialOrderEventJournalCreateErrorV2::kNone;
    }

    [[nodiscard]] PartialOrderEventJournalPublishErrorV2
    EnsureEventWritable(std::uint64_t required_event_count) noexcept {
        if (failed_) {
            return PartialOrderEventJournalPublishErrorV2::kFailed;
        }
        if (required_event_count > config_.event_capacity) {
            return PartialOrderEventJournalPublishErrorV2::
                kEventCapacity;
        }
        using namespace partial_order_event_wire_v2_detail;
        std::uint64_t slot_bytes = 0U;
        std::uint64_t required_bytes = 0U;
        std::uint64_t page_target = 0U;
        if (!CheckedMultiply(
                required_event_count,
                kPartialOrderEventSlotBytesV2,
                &slot_bytes) ||
            !CheckedAdd(
                kPartialOrderEventHeaderBytesV2,
                slot_bytes,
                &required_bytes) ||
            !AlignUp(required_bytes, 4096U, &page_target) ||
            page_target > channel_bank_offsets_[0U]) {
            return PartialOrderEventJournalPublishErrorV2::
                kEventCapacity;
        }
        if (page_target <= committed_event_region_bytes_) {
            return PartialOrderEventJournalPublishErrorV2::kNone;
        }
        std::uint64_t target = channel_bank_offsets_[0U];
        if (page_target <=
            std::numeric_limits<std::uint64_t>::max() -
                (config_.lazy_commit_chunk_bytes - 1U)) {
            target =
                ((page_target + config_.lazy_commit_chunk_bytes - 1U) /
                 config_.lazy_commit_chunk_bytes) *
                config_.lazy_commit_chunk_bytes;
        }
        target = std::min(target, channel_bank_offsets_[0U]);
        int system_error_number = 0;
        if (!AllocateBacking(
                memfd_,
                committed_event_region_bytes_,
                target - committed_event_region_bytes_,
                &system_error_number)) {
            last_system_error_ = system_error_number;
            return PartialOrderEventJournalPublishErrorV2::
                kBackingCommitFailed;
        }
        committed_event_region_bytes_ = target;
        return PartialOrderEventJournalPublishErrorV2::kNone;
    }

    [[nodiscard]] PartialOrderEventJournalPublishErrorV2
    PublishCanonicalTick(
        std::uint64_t canonical_apply_sequence,
        const PartialOrderEventStatusUpdateV2& status,
        std::span<const InstrumentDerivedEventV1> events,
        std::span<const PartialOrderEventChannelHealthV2>
            affected_channels) noexcept {
        if (canonical_apply_sequence == 0U ||
            canonical_apply_sequence >
                std::numeric_limits<std::uint64_t>::max() / 2U ||
            canonical_apply_frontier_ ==
                std::numeric_limits<std::uint64_t>::max() ||
            canonical_apply_sequence != canonical_apply_frontier_ + 1U) {
            return PartialOrderEventJournalPublishErrorV2::
                kCanonicalSequence;
        }
        return Commit(
            canonical_apply_sequence,
            status,
            events,
            affected_channels,
            true);
    }

    [[nodiscard]] PartialOrderEventJournalPublishErrorV2 PublishStatus(
        const PartialOrderEventStatusUpdateV2& status,
        std::span<const PartialOrderEventChannelHealthV2>
            affected_channels) noexcept {
        return Commit(
            canonical_apply_frontier_,
            status,
            {},
            affected_channels,
            false);
    }

    [[nodiscard]] bool DuplicateReadOnlyDescriptor(
        int* output,
        int* system_error_number) const noexcept {
        SetSystemError(system_error_number, 0);
        if (output == nullptr || read_only_fd_ < 0) {
            SetSystemError(system_error_number, EINVAL);
            return false;
        }
        *output = -1;
        int duplicate = -1;
        do {
            duplicate = ::fcntl(read_only_fd_, F_DUPFD_CLOEXEC, 0);
        } while (duplicate < 0 && errno == EINTR);
        if (duplicate < 0) {
            SetSystemError(system_error_number, errno);
            return false;
        }
        *output = duplicate;
        return true;
    }

    [[nodiscard]] PartialOrderEventJournalSessionV2 session()
        const noexcept {
        return {
            config_.run_id,
            config_.session_epoch,
            config_.trade_date,
            config_.publication_generation,
            config_.correction_epoch,
            config_.coverage_start_unix_ns,
            config_.ordering_quality,
            config_.event_capacity,
            config_.affected_channel_capacity,
            config_.order_state_capacity,
            mapping_bytes_};
    }

    [[nodiscard]] std::uint64_t commit_sequence() const noexcept {
        return commit_sequence_;
    }

    [[nodiscard]] std::uint64_t canonical_apply_frontier()
        const noexcept {
        return canonical_apply_frontier_;
    }

    [[nodiscard]] std::uint64_t published_event_frontier()
        const noexcept {
        return published_event_frontier_;
    }

    [[nodiscard]] PartialOrderEventJournalResourceSnapshotV2
    ResourceSnapshot() const noexcept {
        return {
            committed_event_region_bytes_,
            order_state_backed_bytes_,
            order_state_backed_chunks_,
            order_state_backing_allocation_calls_};
    }

    [[nodiscard]] bool failed() const noexcept { return failed_; }

    void SetCommitFailpointForTest(
        PartialOrderEventCommitFailpointV2 failpoint) noexcept {
        failpoint_ = failpoint;
    }

private:
    struct StatePlan final {
        OrderKey key{};
        std::uint64_t slot_index = 0U;
        std::size_t event_index = 0U;
        std::uint64_t expected_version_tag = 0U;
        std::uint32_t version_index = 0U;
        bool new_key = false;
    };

    struct StatePlanSlot final {
        std::uint64_t epoch = 0U;
        std::uint64_t plan_index = 0U;
    };

    [[nodiscard]] PartialOrderEventJournalPublishErrorV2 Commit(
        std::uint64_t new_canonical_frontier,
        const PartialOrderEventStatusUpdateV2& status,
        std::span<const InstrumentDerivedEventV1> events,
        std::span<const PartialOrderEventChannelHealthV2>
            affected_channels,
        bool advances_canonical) noexcept {
        if (failed_) {
            return PartialOrderEventJournalPublishErrorV2::kFailed;
        }
        if (!StateValueValid(status.state) ||
            !ErrorValueValid(status.last_error) ||
            status.captured_source_frontier <
                captured_source_frontier_ ||
            status.reorder_high_water < reorder_high_water_ ||
            (StateRequiresStale(status.state) && !status.stale) ||
            (status.state ==
                 PartialOrderEventServiceStateV2::kContiguous &&
             (status.stale ||
              status.last_error !=
                  PartialOrderEventLastErrorV2::kNone)) ||
            (!advances_canonical && !events.empty())) {
            return PartialOrderEventJournalPublishErrorV2::
                kInvalidArgument;
        }
        if (commit_sequence_ >=
            std::numeric_limits<std::uint64_t>::max() / 2U) {
            return PartialOrderEventJournalPublishErrorV2::
                kPublicationInvariant;
        }
        if (affected_channels.size() >
            config_.affected_channel_capacity) {
            return PartialOrderEventJournalPublishErrorV2::
                kChannelCapacity;
        }
        if (events.size() >
                std::numeric_limits<std::uint64_t>::max() -
                    published_event_frontier_ ||
            (!events.empty() &&
             events.size() - 1U >
                 static_cast<std::size_t>(
                     std::numeric_limits<std::uint32_t>::max())) ||
            static_cast<std::uint64_t>(events.size()) >
                config_.event_capacity - published_event_frontier_) {
            return PartialOrderEventJournalPublishErrorV2::
                kEventCapacity;
        }

        const std::uint64_t new_commit_sequence = commit_sequence_ + 1U;
        std::uint64_t pending_count = 0U;
        std::uint64_t oldest_gap_age_ns = 0U;
        for (std::size_t index = 0U; index < affected_channels.size();
             ++index) {
            PartialOrderEventChannelHealthV2 normalized =
                affected_channels[index];
            normalized.commit_sequence = new_commit_sequence;
            if (affected_channels[index].commit_sequence != 0U ||
                (affected_channels[index].flags &
                     kPartialOrderEventChannelAffectedV2) == 0U ||
                !PartialOrderEventChannelHealthCanonicalV2(
                    normalized, new_commit_sequence) ||
                (index != 0U &&
                 !KeyLess(
                     affected_channels[index - 1U],
                     affected_channels[index]))) {
                return PartialOrderEventJournalPublishErrorV2::
                    kInvalidArgument;
            }
            if (affected_channels[index].pending_count >
                std::numeric_limits<std::uint64_t>::max() -
                    pending_count) {
                return PartialOrderEventJournalPublishErrorV2::
                    kInvalidArgument;
            }
            pending_count += affected_channels[index].pending_count;
            oldest_gap_age_ns = std::max(
                oldest_gap_age_ns,
                affected_channels[index].oldest_gap_age_ns);
        }
        if (pending_count > status.reorder_high_water ||
            (status.state ==
                 PartialOrderEventServiceStateV2::kContiguous &&
             (!affected_channels.empty() || pending_count != 0U ||
              oldest_gap_age_ns != 0U))) {
            return PartialOrderEventJournalPublishErrorV2::
                kInvalidArgument;
        }

        const std::uint64_t new_event_frontier =
            published_event_frontier_ +
            static_cast<std::uint64_t>(events.size());
        const auto writable = EnsureEventWritable(new_event_frontier);
        if (writable != PartialOrderEventJournalPublishErrorV2::kNone) {
            return writable;
        }

        state_plans_.clear();
        BeginStatePlanEpoch();
        constexpr std::size_t kInlineProjectionRows = 3U;
        std::array<
            l2flow_instrument_derived_event_row_v1,
            kInlineProjectionRows>
            inline_rows{};
        const bool inline_projection = events.size() <= inline_rows.size();
        for (std::size_t index = 0U; index < events.size(); ++index) {
            const std::uint64_t sequence =
                published_event_frontier_ +
                static_cast<std::uint64_t>(index) + 1U;
            PartialOrderEventSlotV2& event_slot =
                event_slots_[sequence - 1U];
            if (Atomic(event_slot.publish_tag)
                        .load(std::memory_order_acquire) != 0U ||
                !partial_order_event_wire_v2_detail::AllZero(
                    event_slot.reserved)) {
                return PartialOrderEventJournalPublishErrorV2::
                    kPublicationInvariant;
            }
            l2flow_instrument_derived_event_row_v1 scratch{};
            auto& row = inline_projection ? inline_rows[index] : scratch;
            if (!events[index].source_tick_event_ordinal_valid ||
                events[index].source_tick_event_ordinal !=
                    static_cast<std::uint32_t>(index) ||
                !ProjectInstrumentDerivedEventWireV1(
                    events[index], &row) ||
                !CertifiedOrderEventRowCanonicalV1(
                    row, config_.trade_date, sequence)) {
                return PartialOrderEventJournalPublishErrorV2::
                    kProjectionError;
            }
            if (row.event_kind ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_REVISION_V1) {
                const OrderKey key{
                    row.market,
                    row.instrument_id,
                    row.channel,
                    row.order_id};
                const auto plan_result = PlanStateUpdate(key, index);
                if (plan_result !=
                    PartialOrderEventJournalPublishErrorV2::kNone) {
                    return plan_result;
                }
            }
        }

        const auto backing_error = EnsureOrderStateBacking();
        if (backing_error !=
            PartialOrderEventJournalPublishErrorV2::kNone) {
            return backing_error;
        }

        std::uint64_t heartbeat = 0U;
        if (!ReadMonotonicNs(&heartbeat)) {
            return PartialOrderEventJournalPublishErrorV2::
                kPublicationInvariant;
        }
        const std::uint32_t target_bank = current_cut_bank_ ^ 1U;
        PartialOrderEventCommitCutV2& target_cut =
            header_->cuts[target_bank];
        const std::uint64_t odd_tag = new_commit_sequence * 2U - 1U;
        std::uint64_t expected_target_tag =
            new_commit_sequence == 2U
                ? 0U
                : (new_commit_sequence - 2U) * 2U;
        if (!Atomic(target_cut.publish_tag)
                 .compare_exchange_strong(
                     expected_target_tag,
                     odd_tag,
                     std::memory_order_acq_rel,
                     std::memory_order_acquire)) {
            return FailTerminal(
                PartialOrderEventJournalPublishErrorV2::
                    kPublicationInvariant);
        }
        std::atomic_thread_fence(std::memory_order_release);
        if (TripFailpoint(
                PartialOrderEventCommitFailpointV2::
                    kAfterTargetCutInvalidated)) {
            return PartialOrderEventJournalPublishErrorV2::
                kInjectedFailure;
        }

        common::Crc32cState channel_crc;
        for (std::size_t index = 0U; index < affected_channels.size();
             ++index) {
            PartialOrderEventChannelHealthV2 normalized =
                affected_channels[index];
            normalized.commit_sequence = new_commit_sequence;
            channel_crc.Update(std::as_bytes(
                std::span{&normalized, std::size_t{1U}}));
            AtomicStoreWords(&channel_banks_[target_bank][index], normalized);
        }
        if (TripFailpoint(
                PartialOrderEventCommitFailpointV2::
                    kAfterChannelBankWritten)) {
            return PartialOrderEventJournalPublishErrorV2::
                kInjectedFailure;
        }

        std::uint64_t new_shanghai_count = shanghai_order_state_count_;
        std::uint64_t new_shenzhen_count = shenzhen_order_state_count_;
        for (const StatePlan& plan : state_plans_) {
            PartialOrderEventOrderStateSlotV2& state_slot =
                order_state_slots_[plan.slot_index];
            if (plan.new_key) {
                std::uint64_t expected = 0U;
                if (!Atomic(state_slot.key_publish_tag)
                         .compare_exchange_strong(
                             expected,
                             1U,
                             std::memory_order_acq_rel,
                             std::memory_order_acquire)) {
                    return FailTerminal(
                        PartialOrderEventJournalPublishErrorV2::
                            kPublicationInvariant);
                }
                std::atomic_thread_fence(std::memory_order_release);
                if (TripFailpoint(
                        PartialOrderEventCommitFailpointV2::
                            kAfterOrderStateKeyInvalidated)) {
                    return PartialOrderEventJournalPublishErrorV2::
                        kInjectedFailure;
                }
                Atomic(state_slot.market)
                    .store(plan.key.market, std::memory_order_relaxed);
                Atomic(state_slot.instrument_id)
                    .store(
                        plan.key.instrument_id,
                        std::memory_order_relaxed);
                Atomic(state_slot.channel)
                    .store(plan.key.channel, std::memory_order_relaxed);
                Atomic(state_slot.order_id)
                    .store(plan.key.order_id, std::memory_order_relaxed);
                Atomic(state_slot.key_publish_tag)
                    .store(2U, std::memory_order_release);
                if (plan.key.market ==
                    L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1) {
                    ++new_shanghai_count;
                } else {
                    ++new_shenzhen_count;
                }
            }
            PartialOrderEventOrderStateVersionV2& version =
                state_slot.versions[plan.version_index];
            l2flow_instrument_derived_event_row_v1 projected{};
            const l2flow_instrument_derived_event_row_v1* state_row =
                nullptr;
            if (inline_projection) {
                state_row = &inline_rows[plan.event_index];
            } else {
                const std::uint64_t state_event_sequence =
                    published_event_frontier_ +
                    static_cast<std::uint64_t>(plan.event_index) + 1U;
                if (!ProjectInstrumentDerivedEventWireV1(
                        events[plan.event_index], &projected) ||
                    !CertifiedOrderEventRowCanonicalV1(
                        projected,
                        config_.trade_date,
                        state_event_sequence) ||
                    projected.event_kind !=
                        L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_REVISION_V1) {
                    return FailTerminal(
                        PartialOrderEventJournalPublishErrorV2::
                            kProjectionError);
                }
                state_row = &projected;
            }
            std::uint64_t expected_version_tag =
                plan.expected_version_tag;
            if (!Atomic(version.publish_tag)
                     .compare_exchange_strong(
                         expected_version_tag,
                         new_canonical_frontier * 2U - 1U,
                         std::memory_order_acq_rel,
                         std::memory_order_acquire)) {
                return FailTerminal(
                    PartialOrderEventJournalPublishErrorV2::
                        kPublicationInvariant);
            }
            std::atomic_thread_fence(std::memory_order_release);
            if (TripFailpoint(
                    PartialOrderEventCommitFailpointV2::
                        kAfterOrderStateVersionInvalidated)) {
                return PartialOrderEventJournalPublishErrorV2::
                    kInjectedFailure;
            }
            Atomic(version.canonical_apply_sequence)
                .store(
                    new_canonical_frontier,
                    std::memory_order_relaxed);
            std::array<std::uint64_t, 40U> words{};
            std::memcpy(
                words.data(), state_row, sizeof(*state_row));
            for (std::size_t word = 0U; word < words.size(); ++word) {
                Atomic(version.payload_words[word])
                    .store(words[word], std::memory_order_relaxed);
            }
            Atomic(version.publish_tag)
                .store(
                    new_canonical_frontier * 2U,
                    std::memory_order_release);
        }

        for (std::size_t index = 0U; index < events.size(); ++index) {
            const std::uint64_t sequence =
                published_event_frontier_ +
                static_cast<std::uint64_t>(index) + 1U;
            PartialOrderEventSlotV2& event_slot =
                event_slots_[sequence - 1U];
            std::uint64_t expected = 0U;
            if (!Atomic(event_slot.publish_tag)
                     .compare_exchange_strong(
                         expected,
                         1U,
                         std::memory_order_acq_rel,
                         std::memory_order_acquire)) {
                return FailTerminal(
                        PartialOrderEventJournalPublishErrorV2::
                            kPublicationInvariant);
            }
            std::atomic_thread_fence(std::memory_order_release);
        }
        for (std::size_t index = 0U; index < events.size(); ++index) {
            const std::uint64_t sequence =
                published_event_frontier_ +
                static_cast<std::uint64_t>(index) + 1U;
            PartialOrderEventSlotV2& event_slot =
                event_slots_[sequence - 1U];
            l2flow_instrument_derived_event_row_v1 scratch{};
            const l2flow_instrument_derived_event_row_v1* row = nullptr;
            if (inline_projection) {
                row = &inline_rows[index];
            } else {
                if (!ProjectInstrumentDerivedEventWireV1(
                        events[index], &scratch) ||
                    !CertifiedOrderEventRowCanonicalV1(
                        scratch, config_.trade_date, sequence)) {
                    return FailTerminal(
                        PartialOrderEventJournalPublishErrorV2::
                            kProjectionError);
                }
                row = &scratch;
            }
            Atomic(event_slot.canonical_apply_sequence)
                .store(
                    new_canonical_frontier,
                    std::memory_order_relaxed);
            std::array<std::uint64_t, 40U> words{};
            std::memcpy(words.data(), row, sizeof(*row));
            for (std::size_t word = 0U; word < words.size(); ++word) {
                Atomic(event_slot.payload_words[word])
                    .store(words[word], std::memory_order_relaxed);
            }
            Atomic(event_slot.publish_tag)
                .store(2U, std::memory_order_release);
        }
        if (TripFailpoint(
                PartialOrderEventCommitFailpointV2::
                    kAfterEventRowsWritten)) {
            return PartialOrderEventJournalPublishErrorV2::
                kInjectedFailure;
        }

        PartialOrderEventCommitCutV2 next{};
        next.commit_sequence = new_commit_sequence;
        next.heartbeat_monotonic_ns = heartbeat;
        next.captured_source_frontier =
            status.captured_source_frontier;
        next.canonical_apply_frontier = new_canonical_frontier;
        next.event_published_frontier = new_event_frontier;
        next.history_generation = new_canonical_frontier;
        next.order_state_generation =
            config_.publication_generation;
        next.order_state_canonical_frontier = new_canonical_frontier;
        next.committed_event_region_bytes =
            committed_event_region_bytes_;
        next.shanghai_order_state_count = new_shanghai_count;
        next.shenzhen_order_state_count = new_shenzhen_count;
        next.pending_count = pending_count;
        next.reorder_high_water = status.reorder_high_water;
        next.oldest_gap_age_ns = oldest_gap_age_ns;
        next.affected_channel_count =
            static_cast<std::uint32_t>(affected_channels.size());
        next.channel_health_count =
            static_cast<std::uint32_t>(affected_channels.size());
        next.state = status.state;
        next.stale = status.stale ? 1U : 0U;
        next.last_error = status.last_error;
        next.active_channel_bank = target_bank;
        next.channel_bank_crc32c = channel_crc.Finalize();
        next.cut_crc32c = PartialOrderEventCommitCutCrc32cV2(next);
        next.publish_tag = odd_tag;
        AtomicStoreWords(&target_cut, next);
        if (TripFailpoint(
                PartialOrderEventCommitFailpointV2::
                    kBeforeCutPublished)) {
            return PartialOrderEventJournalPublishErrorV2::
                kInjectedFailure;
        }
        Atomic(target_cut.publish_tag)
            .store(new_commit_sequence * 2U, std::memory_order_release);

        commit_sequence_ = new_commit_sequence;
        current_cut_bank_ = target_bank;
        captured_source_frontier_ = status.captured_source_frontier;
        canonical_apply_frontier_ = new_canonical_frontier;
        published_event_frontier_ = new_event_frontier;
        reorder_high_water_ = status.reorder_high_water;
        shanghai_order_state_count_ = new_shanghai_count;
        shenzhen_order_state_count_ = new_shenzhen_count;
        return PartialOrderEventJournalPublishErrorV2::kNone;
    }

    [[nodiscard]] PartialOrderEventJournalPublishErrorV2
    EnsureOrderStateBacking() noexcept {
        order_state_chunk_plans_.clear();
        const auto clear_planning = [this]() noexcept {
            for (const std::uint64_t chunk : order_state_chunk_plans_) {
                const std::size_t word =
                    static_cast<std::size_t>(chunk / 64U);
                const std::uint64_t mask = 1ULL << (chunk % 64U);
                order_state_planning_words_[word] &= ~mask;
            }
        };
        for (const StatePlan& plan : state_plans_) {
            std::uint64_t relative_slot_offset = 0U;
            std::uint64_t relative_slot_end = 0U;
            if (!partial_order_event_wire_v2_detail::CheckedMultiply(
                    plan.slot_index,
                    kPartialOrderEventOrderStateSlotBytesV2,
                    &relative_slot_offset) ||
                !partial_order_event_wire_v2_detail::CheckedAdd(
                    relative_slot_offset,
                    kPartialOrderEventOrderStateSlotBytesV2,
                    &relative_slot_end) ||
                relative_slot_end == 0U ||
                relative_slot_end > order_state_region_bytes_) {
                clear_planning();
                return PartialOrderEventJournalPublishErrorV2::
                    kPublicationInvariant;
            }
            const std::uint64_t first_chunk =
                relative_slot_offset / config_.lazy_commit_chunk_bytes;
            const std::uint64_t last_chunk =
                (relative_slot_end - 1U) /
                config_.lazy_commit_chunk_bytes;
            for (std::uint64_t chunk = first_chunk; chunk <= last_chunk;
                 ++chunk) {
                if (chunk >= order_state_backing_chunk_count_) {
                    clear_planning();
                    return PartialOrderEventJournalPublishErrorV2::
                        kPublicationInvariant;
                }
                const std::size_t word =
                    static_cast<std::size_t>(chunk / 64U);
                const std::uint64_t mask = 1ULL << (chunk % 64U);
                if ((order_state_backing_words_[word] & mask) != 0U ||
                    (order_state_planning_words_[word] & mask) != 0U) {
                    continue;
                }
                if (order_state_chunk_plans_.size() ==
                    order_state_chunk_plans_.capacity()) {
                    clear_planning();
                    return PartialOrderEventJournalPublishErrorV2::
                        kPublicationInvariant;
                }
                order_state_planning_words_[word] |= mask;
                order_state_chunk_plans_.push_back(chunk);
            }
        }
        std::sort(
            order_state_chunk_plans_.begin(),
            order_state_chunk_plans_.end());
        std::size_t plan_index = 0U;
        while (plan_index < order_state_chunk_plans_.size()) {
            const std::uint64_t first_chunk =
                order_state_chunk_plans_[plan_index];
            std::uint64_t chunk_end = first_chunk + 1U;
            ++plan_index;
            while (plan_index < order_state_chunk_plans_.size() &&
                   order_state_chunk_plans_[plan_index] == chunk_end) {
                ++chunk_end;
                ++plan_index;
            }
            std::uint64_t relative_begin = 0U;
            std::uint64_t relative_end = 0U;
            if (!partial_order_event_wire_v2_detail::CheckedMultiply(
                    first_chunk,
                    config_.lazy_commit_chunk_bytes,
                    &relative_begin) ||
                !partial_order_event_wire_v2_detail::CheckedMultiply(
                    chunk_end,
                    config_.lazy_commit_chunk_bytes,
                    &relative_end)) {
                clear_planning();
                return PartialOrderEventJournalPublishErrorV2::
                    kPublicationInvariant;
            }
            relative_end = std::min(
                relative_end, order_state_region_bytes_);
            std::uint64_t absolute_begin = 0U;
            if (!partial_order_event_wire_v2_detail::CheckedAdd(
                    order_states_offset_,
                    relative_begin,
                    &absolute_begin) ||
                relative_end <= relative_begin) {
                clear_planning();
                return PartialOrderEventJournalPublishErrorV2::
                    kPublicationInvariant;
            }
            int system_error_number = 0;
            if (!AllocateBacking(
                    memfd_,
                    absolute_begin,
                    relative_end - relative_begin,
                    &system_error_number)) {
                last_system_error_ = system_error_number;
                clear_planning();
                return PartialOrderEventJournalPublishErrorV2::
                    kBackingCommitFailed;
            }
            ++order_state_backing_allocation_calls_;
            order_state_backed_bytes_ += relative_end - relative_begin;
            for (std::uint64_t chunk = first_chunk; chunk < chunk_end;
                 ++chunk) {
                const std::size_t word =
                    static_cast<std::size_t>(chunk / 64U);
                const std::uint64_t mask = 1ULL << (chunk % 64U);
                order_state_backing_words_[word] |= mask;
                ++order_state_backed_chunks_;
            }
        }
        clear_planning();
        return PartialOrderEventJournalPublishErrorV2::kNone;
    }

    void BeginStatePlanEpoch() noexcept {
        if (state_plan_epoch_ ==
            std::numeric_limits<std::uint64_t>::max()) {
            std::fill(
                state_plan_slots_.begin(),
                state_plan_slots_.end(),
                StatePlanSlot{});
            state_plan_epoch_ = 1U;
            return;
        }
        ++state_plan_epoch_;
    }

    [[nodiscard]] PartialOrderEventJournalPublishErrorV2 PlanStateUpdate(
        const OrderKey& key,
        std::size_t event_index) noexcept {
        const std::uint64_t mask = config_.order_state_capacity - 1U;
        const std::uint64_t start = HashOrderKey(key) & mask;
        for (std::uint64_t probe = 0U;
             probe < config_.order_state_capacity;
             ++probe) {
            const std::uint64_t index = (start + probe) & mask;
            StatePlanSlot& planned_slot =
                state_plan_slots_[static_cast<std::size_t>(index)];
            if (planned_slot.epoch == state_plan_epoch_) {
                if (planned_slot.plan_index >= state_plans_.size()) {
                    return PartialOrderEventJournalPublishErrorV2::
                        kPublicationInvariant;
                }
                StatePlan& planned = state_plans_[
                    static_cast<std::size_t>(planned_slot.plan_index)];
                if (planned.key == key) {
                    planned.event_index = event_index;
                    return PartialOrderEventJournalPublishErrorV2::kNone;
                }
                continue;
            }

            PartialOrderEventOrderStateSlotV2& slot =
                order_state_slots_[index];
            const std::uint64_t key_tag =
                Atomic(slot.key_publish_tag)
                    .load(std::memory_order_acquire);
            if (key_tag == 0U) {
                return AddStatePlan(
                    key,
                    event_index,
                    index,
                    0U,
                    0U,
                    true,
                    &planned_slot);
            }
            if (key_tag != 2U ||
                !partial_order_event_wire_v2_detail::AllZero(
                    slot.reserved_identity)) {
                return PartialOrderEventJournalPublishErrorV2::
                    kPublicationInvariant;
            }
            const OrderKey existing{
                Atomic(slot.market).load(std::memory_order_relaxed),
                Atomic(slot.instrument_id).load(std::memory_order_relaxed),
                Atomic(slot.channel).load(std::memory_order_relaxed),
                Atomic(slot.order_id).load(std::memory_order_relaxed)};
            std::atomic_thread_fence(std::memory_order_acq_rel);
            if (Atomic(slot.key_publish_tag)
                    .load(std::memory_order_acquire) != key_tag) {
                return PartialOrderEventJournalPublishErrorV2::
                    kPublicationInvariant;
            }
            if (existing == key) {
                std::uint32_t version_index = 0U;
                std::uint64_t expected_version_tag = 0U;
                const auto select = SelectOlderVersion(
                    slot, &version_index, &expected_version_tag);
                if (select !=
                    PartialOrderEventJournalPublishErrorV2::kNone) {
                    return select;
                }
                return AddStatePlan(
                    key,
                    event_index,
                    index,
                    version_index,
                    expected_version_tag,
                    false,
                    &planned_slot);
            }
        }
        return PartialOrderEventJournalPublishErrorV2::
            kOrderStateCapacity;
    }

    [[nodiscard]] PartialOrderEventJournalPublishErrorV2 AddStatePlan(
        const OrderKey& key,
        std::size_t event_index,
        std::uint64_t slot_index,
        std::uint32_t version_index,
        std::uint64_t expected_version_tag,
        bool new_key,
        StatePlanSlot* planned_slot) noexcept {
        if (planned_slot == nullptr ||
            state_plans_.size() >= maximum_state_updates_per_commit_ ||
            state_plans_.size() == state_plans_.capacity()) {
            return PartialOrderEventJournalPublishErrorV2::
                kOrderStateCapacity;
        }
        StatePlan plan{};
        plan.key = key;
        plan.slot_index = slot_index;
        plan.event_index = event_index;
        plan.version_index = version_index;
        plan.expected_version_tag = expected_version_tag;
        plan.new_key = new_key;
        const std::uint64_t plan_index =
            static_cast<std::uint64_t>(state_plans_.size());
        state_plans_.push_back(plan);
        planned_slot->epoch = state_plan_epoch_;
        planned_slot->plan_index = plan_index;
        return PartialOrderEventJournalPublishErrorV2::kNone;
    }

    [[nodiscard]] PartialOrderEventJournalPublishErrorV2
    SelectOlderVersion(
        const PartialOrderEventOrderStateSlotV2& slot,
        std::uint32_t* output_index,
        std::uint64_t* output_tag) const noexcept {
        if (output_index == nullptr || output_tag == nullptr) {
            return PartialOrderEventJournalPublishErrorV2::
                kInvalidArgument;
        }
        std::array<std::uint64_t, 2U> sequences{};
        for (std::size_t index = 0U; index < sequences.size(); ++index) {
            const auto& version = slot.versions[index];
            const std::uint64_t tag =
                Atomic(version.publish_tag)
                    .load(std::memory_order_acquire);
            if (tag == 0U) {
                *output_index = static_cast<std::uint32_t>(index);
                *output_tag = 0U;
                return PartialOrderEventJournalPublishErrorV2::kNone;
            }
            const std::uint64_t sequence =
                Atomic(version.canonical_apply_sequence)
                    .load(std::memory_order_relaxed);
            std::atomic_thread_fence(std::memory_order_acq_rel);
            if (Atomic(version.publish_tag)
                        .load(std::memory_order_acquire) != tag ||
                sequence == 0U ||
                sequence >
                    std::numeric_limits<std::uint64_t>::max() / 2U ||
                tag != sequence * 2U) {
                return PartialOrderEventJournalPublishErrorV2::
                    kPublicationInvariant;
            }
            sequences[index] = sequence;
        }
        *output_index = sequences[0U] <= sequences[1U] ? 0U : 1U;
        *output_tag =
            sequences[static_cast<std::size_t>(*output_index)] * 2U;
        return PartialOrderEventJournalPublishErrorV2::kNone;
    }

    [[nodiscard]] bool TripFailpoint(
        PartialOrderEventCommitFailpointV2 phase) noexcept {
        if (failpoint_ != phase) {
            return false;
        }
        failpoint_ = PartialOrderEventCommitFailpointV2::kNone;
        failed_ = true;
        return true;
    }

    [[nodiscard]] PartialOrderEventJournalPublishErrorV2 FailTerminal(
        PartialOrderEventJournalPublishErrorV2 error) noexcept {
        failed_ = true;
        return error;
    }

    PartialOrderEventJournalConfigV2 config_{};
    int memfd_ = -1;
    int read_only_fd_ = -1;
    void* mapping_ = MAP_FAILED;
    PartialOrderEventHeaderV2* header_ = nullptr;
    PartialOrderEventSlotV2* event_slots_ = nullptr;
    std::array<PartialOrderEventChannelHealthV2*, 2U>
        channel_banks_{};
    PartialOrderEventOrderStateSlotV2* order_state_slots_ = nullptr;
    std::uint64_t mapping_bytes_ = 0U;
    std::array<std::uint64_t, 2U> channel_bank_offsets_{};
    std::uint64_t channel_bank_bytes_ = 0U;
    std::uint64_t order_states_offset_ = 0U;
    std::uint64_t order_state_region_bytes_ = 0U;
    std::uint64_t order_state_backing_chunk_count_ = 0U;
    std::uint64_t committed_event_region_bytes_ =
        kPartialOrderEventHeaderBytesV2;
    std::uint64_t commit_sequence_ = 1U;
    std::uint32_t current_cut_bank_ = 0U;
    std::uint64_t captured_source_frontier_ = 0U;
    std::uint64_t canonical_apply_frontier_ = 0U;
    std::uint64_t published_event_frontier_ = 0U;
    std::uint64_t reorder_high_water_ = 0U;
    std::uint64_t shanghai_order_state_count_ = 0U;
    std::uint64_t shenzhen_order_state_count_ = 0U;
    std::vector<StatePlan> state_plans_;
    std::vector<StatePlanSlot> state_plan_slots_;
    std::vector<std::uint64_t> order_state_backing_words_;
    std::vector<std::uint64_t> order_state_planning_words_;
    std::vector<std::uint64_t> order_state_chunk_plans_;
    std::uint64_t maximum_state_updates_per_commit_ = 0U;
    std::uint64_t state_plan_epoch_ = 0U;
    std::uint64_t order_state_backed_bytes_ = 0U;
    std::uint64_t order_state_backed_chunks_ = 0U;
    std::uint64_t order_state_backing_allocation_calls_ = 0U;
    int last_system_error_ = 0;
    bool failed_ = false;
    PartialOrderEventCommitFailpointV2 failpoint_ =
        PartialOrderEventCommitFailpointV2::kNone;
};

std::string_view PartialOrderEventJournalCreateErrorNameV2(
    PartialOrderEventJournalCreateErrorV2 error) noexcept {
    switch (error) {
        case PartialOrderEventJournalCreateErrorV2::kNone:
            return "none";
        case PartialOrderEventJournalCreateErrorV2::kNullOutput:
            return "null_output";
        case PartialOrderEventJournalCreateErrorV2::
            kInvalidConfiguration:
            return "invalid_configuration";
        case PartialOrderEventJournalCreateErrorV2::kLayoutOverflow:
            return "layout_overflow";
        case PartialOrderEventJournalCreateErrorV2::
            kMappingCreateFailed:
            return "mapping_create_failed";
        case PartialOrderEventJournalCreateErrorV2::
            kReadOnlyHandleFailed:
            return "read_only_handle_failed";
        case PartialOrderEventJournalCreateErrorV2::kSealFailed:
            return "seal_failed";
        case PartialOrderEventJournalCreateErrorV2::
            kResourceExhausted:
            return "resource_exhausted";
        case PartialOrderEventJournalCreateErrorV2::
            kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

std::string_view PartialOrderEventJournalPublishErrorNameV2(
    PartialOrderEventJournalPublishErrorV2 error) noexcept {
    switch (error) {
        case PartialOrderEventJournalPublishErrorV2::kNone:
            return "none";
        case PartialOrderEventJournalPublishErrorV2::kInvalidArgument:
            return "invalid_argument";
        case PartialOrderEventJournalPublishErrorV2::
            kCanonicalSequence:
            return "canonical_sequence";
        case PartialOrderEventJournalPublishErrorV2::kEventCapacity:
            return "event_capacity";
        case PartialOrderEventJournalPublishErrorV2::kChannelCapacity:
            return "channel_capacity";
        case PartialOrderEventJournalPublishErrorV2::
            kOrderStateCapacity:
            return "order_state_capacity";
        case PartialOrderEventJournalPublishErrorV2::
            kBackingCommitFailed:
            return "backing_commit_failed";
        case PartialOrderEventJournalPublishErrorV2::
            kProjectionError:
            return "projection_error";
        case PartialOrderEventJournalPublishErrorV2::
            kPublicationInvariant:
            return "publication_invariant";
        case PartialOrderEventJournalPublishErrorV2::
            kInjectedFailure:
            return "injected_failure";
        case PartialOrderEventJournalPublishErrorV2::kFailed:
            return "failed";
    }
    return "unknown";
}

PartialOrderEventJournalProducerV2::
    PartialOrderEventJournalProducerV2(
        std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

PartialOrderEventJournalProducerV2::~PartialOrderEventJournalProducerV2() =
    default;

PartialOrderEventJournalCreateErrorV2
PartialOrderEventJournalProducerV2::Create(
    PartialOrderEventJournalConfigV2 config,
    std::shared_ptr<PartialOrderEventJournalProducerV2>* output,
    int* system_error_number) noexcept {
    SetSystemError(system_error_number, 0);
    if (output == nullptr) {
        return PartialOrderEventJournalCreateErrorV2::kNullOutput;
    }
    output->reset();
    try {
        auto impl = std::make_unique<Impl>(std::move(config));
        const auto error = impl->Initialize(system_error_number);
        if (error != PartialOrderEventJournalCreateErrorV2::kNone) {
            return error;
        }
        *output = std::shared_ptr<PartialOrderEventJournalProducerV2>(
            new PartialOrderEventJournalProducerV2(std::move(impl)));
        return PartialOrderEventJournalCreateErrorV2::kNone;
    } catch (const std::bad_alloc&) {
        return PartialOrderEventJournalCreateErrorV2::
            kResourceExhausted;
    } catch (...) {
        return PartialOrderEventJournalCreateErrorV2::
            kUnexpectedFailure;
    }
}

PartialOrderEventJournalPublishErrorV2
PartialOrderEventJournalProducerV2::EnsureEventWritable(
    std::uint64_t required_event_count) noexcept {
    return impl_->EnsureEventWritable(required_event_count);
}

PartialOrderEventJournalPublishErrorV2
PartialOrderEventJournalProducerV2::PublishCanonicalTick(
    std::uint64_t canonical_apply_sequence,
    const PartialOrderEventStatusUpdateV2& status,
    std::span<const InstrumentDerivedEventV1> events,
    std::span<const PartialOrderEventChannelHealthV2>
        affected_channels) noexcept {
    return impl_->PublishCanonicalTick(
        canonical_apply_sequence, status, events, affected_channels);
}

PartialOrderEventJournalPublishErrorV2
PartialOrderEventJournalProducerV2::PublishStatus(
    const PartialOrderEventStatusUpdateV2& status,
    std::span<const PartialOrderEventChannelHealthV2>
        affected_channels) noexcept {
    return impl_->PublishStatus(status, affected_channels);
}

bool PartialOrderEventJournalProducerV2::DuplicateReadOnlyDescriptor(
    int* output,
    int* system_error_number) const noexcept {
    return impl_->DuplicateReadOnlyDescriptor(
        output, system_error_number);
}

PartialOrderEventJournalSessionV2
PartialOrderEventJournalProducerV2::session() const noexcept {
    return impl_->session();
}

std::uint64_t PartialOrderEventJournalProducerV2::commit_sequence()
    const noexcept {
    return impl_->commit_sequence();
}

std::uint64_t
PartialOrderEventJournalProducerV2::canonical_apply_frontier()
    const noexcept {
    return impl_->canonical_apply_frontier();
}

std::uint64_t
PartialOrderEventJournalProducerV2::published_event_frontier()
    const noexcept {
    return impl_->published_event_frontier();
}

PartialOrderEventJournalResourceSnapshotV2
PartialOrderEventJournalProducerV2::ResourceSnapshot() const noexcept {
    return impl_->ResourceSnapshot();
}

bool PartialOrderEventJournalProducerV2::failed() const noexcept {
    return impl_->failed();
}

void PartialOrderEventJournalProducerV2::SetCommitFailpointForTest(
    PartialOrderEventCommitFailpointV2 failpoint) noexcept {
    impl_->SetCommitFailpointForTest(failpoint);
}

}  // namespace l2flow::ipc
