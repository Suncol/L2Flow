#include "l2flow/ipc/partial_order_event_journal_v2.h"
#include "l2flow/ipc/partial_order_event_journal_v3.h"

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
#include <type_traits>
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
static_assert(kPartialOrderEventOrderStateSlotBytesV3 <= 4096U);

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
    explicit Impl(
        PartialOrderEventJournalConfigV2 config,
        bool compact_state_references)
        : config_(std::move(config)),
          compact_state_references_(compact_state_references),
          order_state_slot_bytes_(
              compact_state_references
                  ? kPartialOrderEventOrderStateSlotBytesV3
                  : kPartialOrderEventOrderStateSlotBytesV2) {}

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
                order_state_slot_bytes_,
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
        maximum_events_per_commit_ =
            config_.maximum_events_per_commit == 0U
                ? config_.event_capacity
                : config_.maximum_events_per_commit;
        compact_state_planning_ =
            maximum_state_updates_per_commit_ <= 4'096U;
        if (maximum_state_updates_per_commit_ >
                config_.order_state_capacity ||
            maximum_state_updates_per_commit_ >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max()) ||
            maximum_state_updates_per_commit_ >
                static_cast<std::uint64_t>(
                    state_plans_.max_size()) ||
            maximum_events_per_commit_ > config_.event_capacity ||
            maximum_events_per_commit_ >
                static_cast<std::uint64_t>(
                    projected_rows_.max_size()) ||
            (!compact_state_planning_ &&
             config_.order_state_capacity >
                static_cast<std::uint64_t>(
                    state_plan_slots_.max_size()))) {
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
            projected_rows_.reserve(
                static_cast<std::size_t>(
                    maximum_events_per_commit_));
            state_plans_.reserve(static_cast<std::size_t>(
                maximum_state_updates_per_commit_));
            const std::size_t state_plan_slot_count =
                compact_state_planning_
                    ? std::bit_ceil(
                          static_cast<std::size_t>(
                              maximum_state_updates_per_commit_) *
                          2U)
                    : static_cast<std::size_t>(
                          config_.order_state_capacity);
            state_plan_slots_.resize(state_plan_slot_count);
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
            compact_state_references_
                ? "l2flow-partial-order-events-v3"
                : "l2flow-partial-order-events-v2",
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
        compact_order_state_slots_ =
            reinterpret_cast<PartialOrderEventOrderStateSlotV3*>(
                static_cast<std::byte*>(mapping_) +
                order_states_offset_);

        std::uint64_t heartbeat = 0U;
        if (!ReadMonotonicNs(&heartbeat)) {
            return PartialOrderEventJournalCreateErrorV2::
                kUnexpectedFailure;
        }
        header_->magic = compact_state_references_
                             ? kPartialOrderEventMagicV3
                             : kPartialOrderEventMagicV2;
        header_->abi_major = compact_state_references_
                                 ? kPartialOrderEventWireMajorV3
                                 : kPartialOrderEventWireMajorV2;
        header_->abi_minor = compact_state_references_
                                 ? kPartialOrderEventWireMinorV3
                                 : kPartialOrderEventWireMinorV2;
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
            static_cast<std::uint32_t>(order_state_slot_bytes_);

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

        const bool header_valid = compact_state_references_
                                      ? PartialOrderEventHeaderLayoutCanonicalV3(
                                            *header_)
                                      : PartialOrderEventHeaderLayoutCanonicalV2(
                                            *header_);
        const bool cut_valid = compact_state_references_
                                   ? PartialOrderEventCommitCutCanonicalV3(
                                         *header_, header_->cuts[0U], 0U)
                                   : PartialOrderEventCommitCutCanonicalV2(
                                         *header_, header_->cuts[0U], 0U);
        if (!header_valid || !cut_valid) {
            return PartialOrderEventJournalCreateErrorV2::
                kUnexpectedFailure;
        }
        return PartialOrderEventJournalCreateErrorV2::kNone;
    }

    [[nodiscard]] PartialOrderEventJournalPublishErrorV2
    PreallocateBacking(int* system_error_number) noexcept {
        SetSystemError(system_error_number, 0);
        if (failed_ || commit_sequence_ != 1U ||
            canonical_apply_frontier_ != 0U ||
            published_event_frontier_ != 0U) {
            return PartialOrderEventJournalPublishErrorV2::
                kInvalidArgument;
        }
        if (fully_preallocated_) {
            return PartialOrderEventJournalPublishErrorV2::kNone;
        }

        if (committed_event_region_bytes_ < channel_bank_offsets_[0U]) {
            if (!AllocateBacking(
                    memfd_,
                    committed_event_region_bytes_,
                    channel_bank_offsets_[0U] -
                        committed_event_region_bytes_,
                    system_error_number)) {
                last_system_error_ =
                    system_error_number == nullptr
                        ? errno
                        : *system_error_number;
                return PartialOrderEventJournalPublishErrorV2::
                    kBackingCommitFailed;
            }
            ++event_backing_allocation_calls_;
            committed_event_region_bytes_ = channel_bank_offsets_[0U];
        }

        if (order_state_backed_chunks_ <
            order_state_backing_chunk_count_) {
            if (!AllocateBacking(
                    memfd_,
                    order_states_offset_,
                    order_state_region_bytes_,
                    system_error_number)) {
                last_system_error_ =
                    system_error_number == nullptr
                        ? errno
                        : *system_error_number;
                return PartialOrderEventJournalPublishErrorV2::
                    kBackingCommitFailed;
            }
            std::fill(
                order_state_backing_words_.begin(),
                order_state_backing_words_.end(),
                std::numeric_limits<std::uint64_t>::max());
            const std::uint64_t remainder =
                order_state_backing_chunk_count_ % 64U;
            if (remainder != 0U) {
                order_state_backing_words_.back() =
                    (1ULL << remainder) - 1U;
            }
            order_state_backed_bytes_ = order_state_region_bytes_;
            order_state_backed_chunks_ =
                order_state_backing_chunk_count_;
            ++order_state_backing_allocation_calls_;
        }
        if (config_.prefault_event_pages) {
            ++event_prefault_attempts_;
#if defined(MADV_POPULATE_WRITE)
            const std::uint64_t event_region_bytes =
                channel_bank_offsets_[0U] -
                kPartialOrderEventHeaderBytesV2;
            if (::madvise(
                    static_cast<std::byte*>(mapping_) +
                        kPartialOrderEventHeaderBytesV2,
                    static_cast<std::size_t>(event_region_bytes),
                    MADV_POPULATE_WRITE) != 0) {
                const int advice_error = errno;
                last_system_error_ = advice_error;
                SetSystemError(system_error_number, advice_error);
                return PartialOrderEventJournalPublishErrorV2::
                    kBackingCommitFailed;
            }
            event_prefaulted_bytes_ = event_region_bytes;
#else
            last_system_error_ = ENOTSUP;
            SetSystemError(system_error_number, ENOTSUP);
            return PartialOrderEventJournalPublishErrorV2::
                kBackingCommitFailed;
#endif
        }
        if (config_.prefault_order_state_pages) {
            ++order_state_prefault_attempts_;
#if defined(MADV_POPULATE_WRITE)
            if (::madvise(
                    static_cast<std::byte*>(mapping_) +
                        order_states_offset_,
                    static_cast<std::size_t>(order_state_region_bytes_),
                    MADV_POPULATE_WRITE) != 0) {
                const int advice_error = errno;
                last_system_error_ = advice_error;
                SetSystemError(system_error_number, advice_error);
                return PartialOrderEventJournalPublishErrorV2::
                    kBackingCommitFailed;
            }
            order_state_prefaulted_bytes_ = order_state_region_bytes_;
#else
            last_system_error_ = ENOTSUP;
            SetSystemError(system_error_number, ENOTSUP);
            return PartialOrderEventJournalPublishErrorV2::
                kBackingCommitFailed;
#endif
        }
        fully_preallocated_ = true;
        return PartialOrderEventJournalPublishErrorV2::kNone;
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
        if (fully_preallocated_) {
            return PartialOrderEventJournalPublishErrorV2::kNone;
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
        ++event_backing_allocation_calls_;
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
        const PartialOrderEventCanonicalSliceV2 slice{
            canonical_apply_sequence, events.size()};
        return PublishCanonicalBatch(
            std::span{&slice, std::size_t{1U}},
            status,
            events,
            affected_channels);
    }

    [[nodiscard]] PartialOrderEventJournalPublishErrorV2
    PublishCanonicalBatch(
        std::span<const PartialOrderEventCanonicalSliceV2> slices,
        const PartialOrderEventStatusUpdateV2& status,
        std::span<const InstrumentDerivedEventV1> events,
        std::span<const PartialOrderEventChannelHealthV2>
            affected_channels) noexcept {
        if (slices.empty()) {
            return PartialOrderEventJournalPublishErrorV2::
                kInvalidArgument;
        }
        return Commit(
            slices,
            status,
            events,
            {},
            affected_channels,
            true);
    }

    [[nodiscard]] PartialOrderEventJournalPublishErrorV2
    PublishCanonicalBatchProjected(
        std::span<const PartialOrderEventCanonicalSliceV2> slices,
        const PartialOrderEventStatusUpdateV2& status,
        std::span<const l2flow_instrument_derived_event_row_v1> events,
        std::span<const PartialOrderEventChannelHealthV2>
            affected_channels) noexcept {
        if (slices.empty()) {
            return PartialOrderEventJournalPublishErrorV2::
                kInvalidArgument;
        }
        return Commit(
            slices,
            status,
            {},
            events,
            affected_channels,
            true);
    }

    [[nodiscard]] PartialOrderEventJournalPublishErrorV2 PublishStatus(
        const PartialOrderEventStatusUpdateV2& status,
        std::span<const PartialOrderEventChannelHealthV2>
            affected_channels) noexcept {
        return Commit(
            {},
            status,
            {},
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
            order_state_backing_allocation_calls_,
            event_backing_allocation_calls_,
            event_prefault_attempts_,
            event_prefaulted_bytes_,
            order_state_prefault_attempts_,
            order_state_prefaulted_bytes_,
            fully_preallocated_};
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
        std::uint64_t canonical_apply_sequence = 0U;
        std::uint64_t expected_version_tag = 0U;
        std::uint32_t version_index = 0U;
        bool new_key = false;
    };

    struct StatePlanSlot final {
        std::uint64_t epoch = 0U;
        std::uint64_t order_slot_index = 0U;
        std::uint64_t plan_index = 0U;
    };

    [[nodiscard]] PartialOrderEventJournalPublishErrorV2 Commit(
        std::span<const PartialOrderEventCanonicalSliceV2> slices,
        const PartialOrderEventStatusUpdateV2& status,
        std::span<const InstrumentDerivedEventV1> events,
        std::span<const l2flow_instrument_derived_event_row_v1>
            projected_events,
        std::span<const PartialOrderEventChannelHealthV2>
            affected_channels,
        bool advances_canonical) noexcept {
        if (!events.empty() && !projected_events.empty()) {
            return PartialOrderEventJournalPublishErrorV2::
                kInvalidArgument;
        }
        const std::size_t event_count = projected_events.empty()
                                            ? events.size()
                                            : projected_events.size();
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
            (advances_canonical != !slices.empty()) ||
            (!advances_canonical && event_count != 0U)) {
            return PartialOrderEventJournalPublishErrorV2::
                kInvalidArgument;
        }

        std::uint64_t new_canonical_frontier = canonical_apply_frontier_;
        std::uint64_t sliced_event_count = 0U;
        if (advances_canonical) {
            std::uint64_t expected_sequence = canonical_apply_frontier_;
            for (const PartialOrderEventCanonicalSliceV2& slice : slices) {
                if (expected_sequence ==
                        std::numeric_limits<std::uint64_t>::max() ||
                    slice.canonical_apply_sequence == 0U ||
                    slice.canonical_apply_sequence >
                        std::numeric_limits<std::uint64_t>::max() / 2U ||
                    slice.canonical_apply_sequence !=
                        expected_sequence + 1U) {
                    return PartialOrderEventJournalPublishErrorV2::
                        kCanonicalSequence;
                }
                if ((slice.event_count != 0U &&
                     slice.event_count - 1U >
                         static_cast<std::size_t>(
                             std::numeric_limits<std::uint32_t>::max())) ||
                    static_cast<std::uint64_t>(slice.event_count) >
                        std::numeric_limits<std::uint64_t>::max() -
                            sliced_event_count) {
                    return PartialOrderEventJournalPublishErrorV2::
                        kInvalidArgument;
                }
                sliced_event_count +=
                    static_cast<std::uint64_t>(slice.event_count);
                expected_sequence = slice.canonical_apply_sequence;
            }
            new_canonical_frontier = expected_sequence;
            if (sliced_event_count !=
                static_cast<std::uint64_t>(event_count)) {
                return PartialOrderEventJournalPublishErrorV2::
                    kInvalidArgument;
            }
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
        if (event_count >
                std::numeric_limits<std::uint64_t>::max() -
                    published_event_frontier_ ||
            static_cast<std::uint64_t>(event_count) >
                maximum_events_per_commit_ ||
            static_cast<std::uint64_t>(event_count) >
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
            static_cast<std::uint64_t>(event_count);
        const auto writable = EnsureEventWritable(new_event_frontier);
        if (writable != PartialOrderEventJournalPublishErrorV2::kNone) {
            return writable;
        }

        state_plans_.clear();
        BeginStatePlanEpoch();
        if (events.size() > projected_rows_.capacity()) {
            return PartialOrderEventJournalPublishErrorV2::kEventCapacity;
        }
        projected_rows_.resize(events.size());
        std::size_t event_index = 0U;
        for (const PartialOrderEventCanonicalSliceV2& slice : slices) {
            for (std::size_t ordinal = 0U;
                 ordinal < slice.event_count;
                 ++ordinal) {
                const std::uint64_t sequence =
                    published_event_frontier_ +
                    static_cast<std::uint64_t>(event_index) + 1U;
                const l2flow_instrument_derived_event_row_v1* row = nullptr;
                if (projected_events.empty()) {
                    l2flow_instrument_derived_event_row_v1* const
                        projected_row = &projected_rows_[event_index];
                    row = projected_row;
                    if (!events[event_index]
                             .source_tick_event_ordinal_valid ||
                        events[event_index].source_tick_event_ordinal !=
                            static_cast<std::uint32_t>(ordinal) ||
                        !ProjectInstrumentDerivedEventWireV1(
                            events[event_index], projected_row)) {
                        return PartialOrderEventJournalPublishErrorV2::
                            kProjectionError;
                    }
                } else {
                    row = &projected_events[event_index];
                    if (row->reserved1[0U] !=
                            L2FLOW_INSTRUMENT_DERIVED_EVENT_SOURCE_TICK_ORDINAL_VALID_V2 ||
                        row->reserved0 !=
                            static_cast<std::uint32_t>(ordinal)) {
                        return PartialOrderEventJournalPublishErrorV2::
                            kProjectionError;
                    }
                }
                if (
                    !CertifiedOrderEventRowCanonicalV1(
                        *row, config_.trade_date, sequence)) {
                    return PartialOrderEventJournalPublishErrorV2::
                        kProjectionError;
                }
                if (row->event_kind ==
                    L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_REVISION_V1) {
                    const OrderKey key{
                        row->market,
                        row->instrument_id,
                        row->channel,
                        row->order_id};
                    const auto plan_result = PlanStateUpdate(
                        key,
                        event_index,
                        slice.canonical_apply_sequence);
                    if (plan_result !=
                        PartialOrderEventJournalPublishErrorV2::kNone) {
                        return plan_result;
                    }
                }
                ++event_index;
            }
        }
        const std::span<const l2flow_instrument_derived_event_row_v1>
            commit_rows = projected_events.empty()
                              ? std::span<const l2flow_instrument_derived_event_row_v1>{
                                    projected_rows_.data(),
                                    projected_rows_.size()}
                              : projected_events;

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
            const auto state_error = compact_state_references_
                                         ? CommitStatePlan(
                                               compact_order_state_slots_[
                                                   plan.slot_index],
                                               plan,
                                               commit_rows,
                                               &new_shanghai_count,
                                               &new_shenzhen_count)
                                         : CommitStatePlan(
                                               order_state_slots_[
                                                   plan.slot_index],
                                               plan,
                                               commit_rows,
                                               &new_shanghai_count,
                                               &new_shenzhen_count);
            if (state_error !=
                PartialOrderEventJournalPublishErrorV2::kNone) {
                return state_error;
            }
        }

        event_index = 0U;
        for (const PartialOrderEventCanonicalSliceV2& slice : slices) {
            for (std::size_t ordinal = 0U;
                 ordinal < slice.event_count;
                 ++ordinal) {
                const std::uint64_t sequence =
                    published_event_frontier_ +
                    static_cast<std::uint64_t>(event_index) + 1U;
                PartialOrderEventSlotV2& event_slot =
                    event_slots_[sequence - 1U];
                const l2flow_instrument_derived_event_row_v1& row =
                    commit_rows[event_index];
                // Event slots are append-only and remain beyond the stable
                // cut until this complete batch is published. No conforming
                // reader can access this slot yet, so no odd-tag invalidation
                // or writer-side RMW is required. The final release tag makes
                // the complete ordinary payload copy visible before the cut.
                event_slot.canonical_apply_sequence =
                    slice.canonical_apply_sequence;
                std::memcpy(
                    event_slot.payload_words.data(), &row, sizeof(row));
                Atomic(event_slot.publish_tag)
                    .store(2U, std::memory_order_release);
                ++event_index;
            }
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

    template <typename StateSlot>
    [[nodiscard]] PartialOrderEventJournalPublishErrorV2 CommitStatePlan(
        StateSlot& state_slot,
        const StatePlan& plan,
        std::span<const l2flow_instrument_derived_event_row_v1> rows,
        std::uint64_t* shanghai_count,
        std::uint64_t* shenzhen_count) noexcept {
        if (shanghai_count == nullptr || shenzhen_count == nullptr ||
            plan.event_index >= rows.size() ||
            plan.version_index >= state_slot.versions.size()) {
            return FailTerminal(
                PartialOrderEventJournalPublishErrorV2::
                    kPublicationInvariant);
        }
        if (plan.new_key) {
            Atomic(state_slot.key_publish_tag)
                .store(1U, std::memory_order_release);
            std::atomic_thread_fence(std::memory_order_release);
            if (TripFailpoint(
                    PartialOrderEventCommitFailpointV2::
                        kAfterOrderStateKeyInvalidated)) {
                return PartialOrderEventJournalPublishErrorV2::
                    kInjectedFailure;
            }
            state_slot.market = plan.key.market;
            state_slot.instrument_id = plan.key.instrument_id;
            state_slot.channel = plan.key.channel;
            state_slot.order_id = plan.key.order_id;
            Atomic(state_slot.key_publish_tag)
                .store(2U, std::memory_order_release);
            if (plan.key.market ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1) {
                ++(*shanghai_count);
            } else {
                ++(*shenzhen_count);
            }
        }

        auto& version = state_slot.versions[plan.version_index];
        const auto& state_row = rows[plan.event_index];
        if (state_row.event_kind !=
            L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_REVISION_V1) {
            return FailTerminal(
                PartialOrderEventJournalPublishErrorV2::kProjectionError);
        }
        if (Atomic(version.publish_tag)
                .load(std::memory_order_acquire) !=
            plan.expected_version_tag) {
            return FailTerminal(
                PartialOrderEventJournalPublishErrorV2::
                    kPublicationInvariant);
        }
        Atomic(version.publish_tag)
            .store(
                plan.canonical_apply_sequence * 2U - 1U,
                std::memory_order_release);
        std::atomic_thread_fence(std::memory_order_release);
        if (TripFailpoint(
                PartialOrderEventCommitFailpointV2::
                    kAfterOrderStateVersionInvalidated)) {
            return PartialOrderEventJournalPublishErrorV2::
                kInjectedFailure;
        }
        Atomic(version.canonical_apply_sequence)
            .store(
                plan.canonical_apply_sequence,
                std::memory_order_relaxed);
        if constexpr (std::is_same_v<
                          StateSlot,
                          PartialOrderEventOrderStateSlotV3>) {
            const std::uint64_t derived_event_sequence =
                published_event_frontier_ +
                static_cast<std::uint64_t>(plan.event_index) + 1U;
            Atomic(version.derived_event_sequence)
                .store(
                    derived_event_sequence,
                    std::memory_order_relaxed);
        } else {
            std::array<std::uint64_t, 40U> state_words{};
            std::memcpy(
                state_words.data(), &state_row, sizeof(state_row));
            for (std::size_t word = 0U;
                 word < state_words.size();
                 ++word) {
                Atomic(version.payload_words[word])
                    .store(state_words[word], std::memory_order_relaxed);
            }
        }
        Atomic(version.publish_tag)
            .store(
                plan.canonical_apply_sequence * 2U,
                std::memory_order_release);
        return PartialOrderEventJournalPublishErrorV2::kNone;
    }

    [[nodiscard]] PartialOrderEventJournalPublishErrorV2
    EnsureOrderStateBacking() noexcept {
        if (fully_preallocated_) {
            return PartialOrderEventJournalPublishErrorV2::kNone;
        }
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
                    order_state_slot_bytes_,
                    &relative_slot_offset) ||
                !partial_order_event_wire_v2_detail::CheckedAdd(
                    relative_slot_offset,
                    order_state_slot_bytes_,
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

    [[nodiscard]] StatePlanSlot* FindStatePlanSlot(
        std::uint64_t order_slot_index) noexcept {
        if (!compact_state_planning_) {
            return &state_plan_slots_[
                static_cast<std::size_t>(order_slot_index)];
        }
        const std::size_t mask = state_plan_slots_.size() - 1U;
        std::size_t bucket =
            static_cast<std::size_t>(Mix64(order_slot_index)) & mask;
        for (std::size_t probe = 0U;
             probe < state_plan_slots_.size();
             ++probe) {
            StatePlanSlot& slot = state_plan_slots_[bucket];
            if (slot.epoch != state_plan_epoch_ ||
                slot.order_slot_index == order_slot_index) {
                return &slot;
            }
            bucket = (bucket + 1U) & mask;
        }
        return nullptr;
    }

    [[nodiscard]] PartialOrderEventJournalPublishErrorV2 PlanStateUpdate(
        const OrderKey& key,
        std::size_t event_index,
        std::uint64_t canonical_apply_sequence) noexcept {
        if (!state_plans_.empty() && state_plans_.back().key == key) {
            state_plans_.back().event_index = event_index;
            state_plans_.back().canonical_apply_sequence =
                canonical_apply_sequence;
            return PartialOrderEventJournalPublishErrorV2::kNone;
        }
        return compact_state_references_
                   ? PlanStateUpdateInTable(
                         compact_order_state_slots_,
                         key,
                         event_index,
                         canonical_apply_sequence)
                   : PlanStateUpdateInTable(
                         order_state_slots_,
                         key,
                         event_index,
                         canonical_apply_sequence);
    }

    template <typename StateSlot>
    [[nodiscard]] PartialOrderEventJournalPublishErrorV2
    PlanStateUpdateInTable(
        StateSlot* order_state_slots,
        const OrderKey& key,
        std::size_t event_index,
        std::uint64_t canonical_apply_sequence) noexcept {
        if (order_state_slots == nullptr) {
            return PartialOrderEventJournalPublishErrorV2::
                kPublicationInvariant;
        }
        const std::uint64_t mask = config_.order_state_capacity - 1U;
        const std::uint64_t start = HashOrderKey(key) & mask;
        for (std::uint64_t probe = 0U;
             probe < config_.order_state_capacity;
             ++probe) {
            const std::uint64_t index = (start + probe) & mask;
            StatePlanSlot* const planned_slot =
                FindStatePlanSlot(index);
            if (planned_slot == nullptr) {
                return PartialOrderEventJournalPublishErrorV2::
                    kPublicationInvariant;
            }
            if (planned_slot->epoch == state_plan_epoch_) {
                if (planned_slot->order_slot_index != index ||
                    planned_slot->plan_index >= state_plans_.size()) {
                    return PartialOrderEventJournalPublishErrorV2::
                        kPublicationInvariant;
                }
                StatePlan& planned = state_plans_[
                    static_cast<std::size_t>(
                        planned_slot->plan_index)];
                if (planned.key == key) {
                    planned.event_index = event_index;
                    planned.canonical_apply_sequence =
                        canonical_apply_sequence;
                    return PartialOrderEventJournalPublishErrorV2::kNone;
                }
                continue;
            }

            StateSlot& slot = order_state_slots[index];
            const std::uint64_t key_tag =
                Atomic(slot.key_publish_tag)
                    .load(std::memory_order_acquire);
            if (key_tag == 0U) {
                return AddStatePlan(
                    key,
                    event_index,
                    canonical_apply_sequence,
                    index,
                    0U,
                    0U,
                    true,
                    planned_slot);
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
                    canonical_apply_sequence,
                    index,
                    version_index,
                    expected_version_tag,
                    false,
                    planned_slot);
            }
        }
        return PartialOrderEventJournalPublishErrorV2::
            kOrderStateCapacity;
    }

    [[nodiscard]] PartialOrderEventJournalPublishErrorV2 AddStatePlan(
        const OrderKey& key,
        std::size_t event_index,
        std::uint64_t canonical_apply_sequence,
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
        plan.canonical_apply_sequence = canonical_apply_sequence;
        plan.version_index = version_index;
        plan.expected_version_tag = expected_version_tag;
        plan.new_key = new_key;
        const std::uint64_t plan_index =
            static_cast<std::uint64_t>(state_plans_.size());
        state_plans_.push_back(plan);
        planned_slot->epoch = state_plan_epoch_;
        planned_slot->order_slot_index = slot_index;
        planned_slot->plan_index = plan_index;
        return PartialOrderEventJournalPublishErrorV2::kNone;
    }

    template <typename StateSlot>
    [[nodiscard]] PartialOrderEventJournalPublishErrorV2
    SelectOlderVersion(
        const StateSlot& slot,
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
            std::uint64_t derived_event_sequence = 1U;
            std::uint64_t reserved = 0U;
            if constexpr (std::is_same_v<
                              StateSlot,
                              PartialOrderEventOrderStateSlotV3>) {
                derived_event_sequence =
                    Atomic(version.derived_event_sequence)
                        .load(std::memory_order_relaxed);
                reserved = Atomic(version.reserved)
                               .load(std::memory_order_relaxed);
            }
            std::atomic_thread_fence(std::memory_order_acq_rel);
            if (Atomic(version.publish_tag)
                        .load(std::memory_order_acquire) != tag ||
                sequence == 0U ||
                sequence >
                    std::numeric_limits<std::uint64_t>::max() / 2U ||
                tag != sequence * 2U || derived_event_sequence == 0U ||
                derived_event_sequence > published_event_frontier_ ||
                reserved != 0U) {
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
    bool compact_state_references_ = false;
    std::uint64_t order_state_slot_bytes_ = 0U;
    int memfd_ = -1;
    int read_only_fd_ = -1;
    void* mapping_ = MAP_FAILED;
    PartialOrderEventHeaderV2* header_ = nullptr;
    PartialOrderEventSlotV2* event_slots_ = nullptr;
    std::array<PartialOrderEventChannelHealthV2*, 2U>
        channel_banks_{};
    PartialOrderEventOrderStateSlotV2* order_state_slots_ = nullptr;
    PartialOrderEventOrderStateSlotV3* compact_order_state_slots_ =
        nullptr;
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
    std::vector<l2flow_instrument_derived_event_row_v1>
        projected_rows_;
    std::vector<StatePlan> state_plans_;
    std::vector<StatePlanSlot> state_plan_slots_;
    std::vector<std::uint64_t> order_state_backing_words_;
    std::vector<std::uint64_t> order_state_planning_words_;
    std::vector<std::uint64_t> order_state_chunk_plans_;
    std::uint64_t maximum_events_per_commit_ = 0U;
    std::uint64_t maximum_state_updates_per_commit_ = 0U;
    std::uint64_t state_plan_epoch_ = 0U;
    std::uint64_t order_state_backed_bytes_ = 0U;
    std::uint64_t order_state_backed_chunks_ = 0U;
    std::uint64_t order_state_backing_allocation_calls_ = 0U;
    std::uint64_t event_backing_allocation_calls_ = 0U;
    std::uint64_t event_prefault_attempts_ = 0U;
    std::uint64_t event_prefaulted_bytes_ = 0U;
    std::uint64_t order_state_prefault_attempts_ = 0U;
    std::uint64_t order_state_prefaulted_bytes_ = 0U;
    int last_system_error_ = 0;
    bool failed_ = false;
    bool fully_preallocated_ = false;
    bool compact_state_planning_ = false;
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
    return CreateInternal(
        std::move(config), false, output, system_error_number);
}

PartialOrderEventJournalCreateErrorV2
PartialOrderEventJournalProducerV2::CreateInternal(
    PartialOrderEventJournalConfigV2 config,
    bool compact_state_references,
    std::shared_ptr<PartialOrderEventJournalProducerV2>* output,
    int* system_error_number) noexcept {
    SetSystemError(system_error_number, 0);
    if (output == nullptr) {
        return PartialOrderEventJournalCreateErrorV2::kNullOutput;
    }
    output->reset();
    try {
        auto impl = std::make_unique<Impl>(
            std::move(config), compact_state_references);
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
PartialOrderEventJournalProducerV2::PreallocateBacking(
    int* system_error_number) noexcept {
    return impl_->PreallocateBacking(system_error_number);
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
PartialOrderEventJournalProducerV2::PublishCanonicalBatch(
    std::span<const PartialOrderEventCanonicalSliceV2> slices,
    const PartialOrderEventStatusUpdateV2& status,
    std::span<const InstrumentDerivedEventV1> events,
    std::span<const PartialOrderEventChannelHealthV2>
        affected_channels) noexcept {
    return impl_->PublishCanonicalBatch(
        slices, status, events, affected_channels);
}

PartialOrderEventJournalPublishErrorV2
PartialOrderEventJournalProducerV2::PublishCanonicalBatchProjected(
    std::span<const PartialOrderEventCanonicalSliceV2> slices,
    const PartialOrderEventStatusUpdateV2& status,
    std::span<const l2flow_instrument_derived_event_row_v1> events,
    std::span<const PartialOrderEventChannelHealthV2>
        affected_channels) noexcept {
    return impl_->PublishCanonicalBatchProjected(
        slices, status, events, affected_channels);
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
