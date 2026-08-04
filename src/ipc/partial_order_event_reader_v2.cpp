#include "l2flow/ipc/partial_order_event_reader_v2.h"
#include "l2flow/ipc/partial_order_event_reader_v3.h"

#include "l2flow/common/crc32c.h"

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
#include <memory>
#include <new>
#include <span>
#include <utility>

#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef F_SEAL_FUTURE_WRITE
#define F_SEAL_FUTURE_WRITE 0x0010
#endif

namespace l2flow::ipc {
namespace {

constexpr std::size_t kReadAttempts = 8U;

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

[[nodiscard]] bool ExpectedSessionValid(
    const PartialOrderEventExpectedSessionV2& expected) noexcept {
    return IdentityNonzero(expected.run_id) &&
           expected.session_epoch != 0U && expected.trade_date != 0U &&
           expected.publication_generation != 0U &&
           expected.correction_epoch != 0U;
}

[[nodiscard]] bool SessionMatches(
    const PartialOrderEventHeaderV2& header,
    const PartialOrderEventExpectedSessionV2& expected) noexcept {
    std::array<std::uint8_t, 16U> expected_run_id{};
    std::memcpy(
        expected_run_id.data(),
        expected.run_id.data(),
        expected_run_id.size());
    return header.run_id == expected_run_id &&
           header.session_epoch == expected.session_epoch &&
           header.trade_date == expected.trade_date &&
           header.publication_generation ==
               expected.publication_generation &&
           header.correction_epoch == expected.correction_epoch;
}

template <typename Wire>
void AtomicCopyWords(const Wire& source, Wire* output) noexcept {
    static_assert(sizeof(Wire) % sizeof(std::uint64_t) == 0U);
    std::array<
        std::uint64_t,
        sizeof(Wire) / sizeof(std::uint64_t)>
        words{};
    const auto* const source_words =
        reinterpret_cast<const std::uint64_t*>(&source);
    for (std::size_t index = 0U; index < words.size(); ++index) {
        words[index] = Atomic(source_words[index])
                           .load(std::memory_order_relaxed);
    }
    std::memcpy(
        static_cast<void*>(output), words.data(), sizeof(*output));
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
    const PartialOrderEventOrderKeyV2& key) noexcept {
    std::uint64_t value =
        (static_cast<std::uint64_t>(key.market) << 32U) |
        static_cast<std::uint64_t>(key.instrument_id);
    value = Mix64(value);
    value ^= Mix64(std::bit_cast<std::uint64_t>(key.channel));
    value ^= Mix64(std::bit_cast<std::uint64_t>(key.order_id));
    return Mix64(value);
}

[[nodiscard]] bool KeyValid(
    const PartialOrderEventOrderKeyV2& key) noexcept {
    return (key.market ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1 ||
            key.market ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHENZHEN_V1) &&
           key.instrument_id != 0U && key.channel >= 0 &&
           (key.market !=
                L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1 ||
            key.channel > 0) &&
           key.order_id > 0;
}

[[nodiscard]] bool KeyEqual(
    const PartialOrderEventOrderStateSlotV2& slot,
    const PartialOrderEventOrderKeyV2& key) noexcept {
    return slot.market == key.market &&
           slot.instrument_id == key.instrument_id &&
           slot.channel == key.channel && slot.order_id == key.order_id;
}

[[nodiscard]] bool ChannelKeyLess(
    const PartialOrderEventChannelHealthV2& left,
    const PartialOrderEventChannelHealthV2& right) noexcept {
    if (left.market != right.market) {
        return left.market < right.market;
    }
    return left.channel < right.channel;
}

enum class StableCopyResult : std::uint8_t {
    kCopied = 0U,
    kAbsent,
    // An odd immutable-key or version tag belongs to a replacement not
    // authorized by the selected cut. It is skipped like future data.
    kUncommitted,
    kInconsistent,
    kCorrupt,
};

struct CompactStateReference final {
    std::uint64_t canonical_apply_sequence = 0U;
    std::uint64_t derived_event_sequence = 0U;
};

}  // namespace

class PartialOrderEventReaderV2::Impl final {
public:
    ~Impl() {
        if (mapping_ != MAP_FAILED) {
            static_cast<void>(::munmap(mapping_, mapping_bytes_));
        }
        CloseDescriptor(&descriptor_);
    }

    [[nodiscard]] PartialOrderEventReaderOpenErrorV2 Initialize(
        int source_descriptor,
        const PartialOrderEventExpectedSessionV2& expected_session,
        bool compact_state_references,
        int* system_error_number) noexcept {
        compact_state_references_ = compact_state_references;
        SetSystemError(system_error_number, 0);
        if constexpr (std::endian::native != std::endian::little) {
            return PartialOrderEventReaderOpenErrorV2::
                kIncompatibleLayout;
        }
        if (source_descriptor < 0) {
            return PartialOrderEventReaderOpenErrorV2::
                kInvalidDescriptor;
        }
        if (!ExpectedSessionValid(expected_session)) {
            return PartialOrderEventReaderOpenErrorV2::
                kInvalidExpectedSession;
        }

        do {
            descriptor_ =
                ::fcntl(source_descriptor, F_DUPFD_CLOEXEC, 0);
        } while (descriptor_ < 0 && errno == EINTR);
        if (descriptor_ < 0) {
            SetSystemError(system_error_number, errno);
            return PartialOrderEventReaderOpenErrorV2::
                kInvalidDescriptor;
        }

        struct stat descriptor_stat {};
        if (::fstat(descriptor_, &descriptor_stat) != 0) {
            SetSystemError(system_error_number, errno);
            return PartialOrderEventReaderOpenErrorV2::
                kDescriptorStatFailed;
        }
        const int descriptor_status = ::fcntl(descriptor_, F_GETFL);
        if (!S_ISREG(descriptor_stat.st_mode) || descriptor_status < 0 ||
            (descriptor_status & O_ACCMODE) != O_RDONLY) {
            SetSystemError(
                system_error_number,
                descriptor_status < 0 ? errno : EACCES);
            return PartialOrderEventReaderOpenErrorV2::
                kInvalidDescriptor;
        }
        if (descriptor_stat.st_size <
            static_cast<off_t>(kPartialOrderEventHeaderBytesV2)) {
            SetSystemError(system_error_number, EPROTO);
            return PartialOrderEventReaderOpenErrorV2::
                kDescriptorStatFailed;
        }
        constexpr int required_seals =
            F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_FUTURE_WRITE |
            F_SEAL_SEAL;
        const int actual_seals = ::fcntl(descriptor_, F_GET_SEALS);
        if (actual_seals < 0 ||
            (actual_seals & required_seals) != required_seals) {
            SetSystemError(system_error_number, errno);
            return PartialOrderEventReaderOpenErrorV2::
                kDescriptorSealMismatch;
        }
        if (static_cast<std::uintmax_t>(descriptor_stat.st_size) >
            static_cast<std::uintmax_t>(
                std::numeric_limits<std::size_t>::max())) {
            return PartialOrderEventReaderOpenErrorV2::
                kIncompatibleLayout;
        }
        mapping_bytes_ =
            static_cast<std::size_t>(descriptor_stat.st_size);
        mapping_ = ::mmap(
            nullptr,
            mapping_bytes_,
            PROT_READ,
            MAP_SHARED,
            descriptor_,
            0);
        if (mapping_ == MAP_FAILED) {
            SetSystemError(system_error_number, errno);
            return PartialOrderEventReaderOpenErrorV2::kMappingFailed;
        }
        header_ = static_cast<const PartialOrderEventHeaderV2*>(mapping_);
        const bool header_valid = compact_state_references_
                                      ? PartialOrderEventHeaderLayoutCanonicalV3(
                                            *header_)
                                      : PartialOrderEventHeaderLayoutCanonicalV2(
                                            *header_);
        if (!header_valid ||
            header_->total_mapping_bytes != mapping_bytes_) {
            return PartialOrderEventReaderOpenErrorV2::
                kIncompatibleLayout;
        }
        if (!SessionMatches(*header_, expected_session)) {
            return PartialOrderEventReaderOpenErrorV2::kSessionMismatch;
        }
        event_slots_ = reinterpret_cast<const PartialOrderEventSlotV2*>(
            static_cast<const std::byte*>(mapping_) +
            header_->slots_offset);
        for (std::size_t bank = 0U; bank < channel_banks_.size();
             ++bank) {
            channel_banks_[bank] =
                reinterpret_cast<
                    const PartialOrderEventChannelHealthV2*>(
                    static_cast<const std::byte*>(mapping_) +
                    header_->channel_bank_offsets[bank]);
        }
        order_state_slots_ =
            reinterpret_cast<const PartialOrderEventOrderStateSlotV2*>(
                static_cast<const std::byte*>(mapping_) +
                header_->order_states_offset);
        compact_order_state_slots_ =
            reinterpret_cast<const PartialOrderEventOrderStateSlotV3*>(
                static_cast<const std::byte*>(mapping_) +
                header_->order_states_offset);

        PartialOrderEventStatusSnapshotV2 status{};
        std::uint32_t bank = 0U;
        std::uint64_t tag = 0U;
        if (ReadStableStatus(&status, &bank, &tag) !=
            PartialOrderEventReadResultV2::kOk) {
            return PartialOrderEventReaderOpenErrorV2::
                kNoStablePublicationCut;
        }
        return PartialOrderEventReaderOpenErrorV2::kNone;
    }

    [[nodiscard]] PartialOrderEventReadResultV2 ReadStatus(
        PartialOrderEventStatusSnapshotV2* output) const noexcept {
        if (output == nullptr) {
            return PartialOrderEventReadResultV2::kInvalidArgument;
        }
        PartialOrderEventStatusSnapshotV2 local{};
        std::uint32_t bank = 0U;
        std::uint64_t tag = 0U;
        const auto result = ReadStableStatus(&local, &bank, &tag);
        if (result == PartialOrderEventReadResultV2::kOk) {
            std::atomic_thread_fence(std::memory_order_acq_rel);
            if (Atomic(header_->cuts[bank].publish_tag)
                    .load(std::memory_order_acquire) != tag) {
                return PartialOrderEventReadResultV2::kInconsistent;
            }
            *output = local;
        }
        return result;
    }

    [[nodiscard]] PartialOrderEventReadResultV2 ReadEvent(
        std::uint64_t derived_event_sequence,
        PartialOrderEventEnvelopeV2* output) const noexcept {
        if (output == nullptr || derived_event_sequence == 0U) {
            return PartialOrderEventReadResultV2::kInvalidArgument;
        }
        PartialOrderEventStatusSnapshotV2 status{};
        std::uint32_t bank = 0U;
        std::uint64_t tag = 0U;
        const auto status_result =
            ReadStableStatus(&status, &bank, &tag);
        if (status_result != PartialOrderEventReadResultV2::kOk) {
            return status_result;
        }
        if (derived_event_sequence >
            status.cut.event_published_frontier) {
            return PartialOrderEventReadResultV2::kNotYetPublished;
        }
        PartialOrderEventEnvelopeV2 local{};
        const auto copy_result = CopyEvent(
            derived_event_sequence,
            status.cut.event_published_frontier,
            &local);
        if (copy_result == StableCopyResult::kCopied) {
            *output = local;
            return PartialOrderEventReadResultV2::kOk;
        }
        return copy_result == StableCopyResult::kInconsistent
                   ? PartialOrderEventReadResultV2::kInconsistent
                   : PartialOrderEventReadResultV2::kCorrupt;
    }

    [[nodiscard]] PartialOrderEventReadResultV2 ReadEvents(
        std::uint64_t first_derived_event_sequence,
        std::span<PartialOrderEventEnvelopeV2> output,
        PartialOrderEventReadBatchResultV2* result) const noexcept {
        if (first_derived_event_sequence == 0U || output.empty() ||
            result == nullptr) {
            return PartialOrderEventReadResultV2::kInvalidArgument;
        }
        *result = {};
        PartialOrderEventStatusSnapshotV2 status{};
        std::uint32_t bank = 0U;
        std::uint64_t tag = 0U;
        const auto status_result =
            ReadStableStatus(&status, &bank, &tag);
        if (status_result != PartialOrderEventReadResultV2::kOk) {
            return status_result;
        }
        if (first_derived_event_sequence >
            status.cut.event_published_frontier) {
            result->next_event_sequence =
                first_derived_event_sequence;
            result->status = status;
            return PartialOrderEventReadResultV2::kNotYetPublished;
        }
        const std::uint64_t available =
            status.cut.event_published_frontier -
            first_derived_event_sequence + 1U;
        const std::size_t count = std::min(
            output.size(),
            static_cast<std::size_t>(std::min<std::uint64_t>(
                available,
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max()))));
        for (std::size_t index = 0U; index < count; ++index) {
            const std::uint64_t sequence =
                first_derived_event_sequence +
                static_cast<std::uint64_t>(index);
            PartialOrderEventEnvelopeV2 local{};
            const auto copy_result = CopyEvent(
                sequence,
                status.cut.event_published_frontier,
                &local);
            if (copy_result != StableCopyResult::kCopied) {
                return copy_result == StableCopyResult::kInconsistent
                           ? PartialOrderEventReadResultV2::kInconsistent
                           : PartialOrderEventReadResultV2::kCorrupt;
            }
            output[index] = local;
        }
        result->rows_read = count;
        result->next_event_sequence =
            first_derived_event_sequence +
            static_cast<std::uint64_t>(count);
        result->status = status;
        return PartialOrderEventReadResultV2::kOk;
    }

    [[nodiscard]] PartialOrderEventReadResultV2 ReadAffectedChannels(
        std::span<PartialOrderEventChannelHealthV2> output,
        std::size_t* rows_read,
        PartialOrderEventStatusSnapshotV2* output_status) const noexcept {
        if (rows_read == nullptr) {
            return PartialOrderEventReadResultV2::kInvalidArgument;
        }
        *rows_read = 0U;
        PartialOrderEventStatusSnapshotV2 status{};
        std::uint32_t bank = 0U;
        std::uint64_t tag = 0U;
        const auto status_result =
            ReadStableStatus(&status, &bank, &tag);
        if (status_result != PartialOrderEventReadResultV2::kOk) {
            return status_result;
        }
        const std::size_t count = status.cut.channel_health_count;
        if (output.size() < count) {
            *rows_read = count;
            if (output_status != nullptr) {
                *output_status = status;
            }
            return PartialOrderEventReadResultV2::kOutputTooSmall;
        }
        common::Crc32cState crc;
        for (std::size_t index = 0U; index < count; ++index) {
            PartialOrderEventChannelHealthV2 local{};
            AtomicCopyWords(channel_banks_[bank][index], &local);
            if (!PartialOrderEventChannelHealthCanonicalV2(
                    local, status.cut.commit_sequence) ||
                (index != 0U &&
                 !ChannelKeyLess(output[index - 1U], local))) {
                return PartialOrderEventReadResultV2::kCorrupt;
            }
            output[index] = local;
            crc.Update(std::as_bytes(
                std::span{&local, std::size_t{1U}}));
        }
        std::atomic_thread_fence(std::memory_order_acq_rel);
        if (Atomic(header_->cuts[bank].publish_tag)
                    .load(std::memory_order_acquire) != tag) {
            return PartialOrderEventReadResultV2::kInconsistent;
        }
        if (crc.Finalize() != status.cut.channel_bank_crc32c) {
            return PartialOrderEventReadResultV2::kCorrupt;
        }
        *rows_read = count;
        if (output_status != nullptr) {
            *output_status = status;
        }
        return PartialOrderEventReadResultV2::kOk;
    }

    [[nodiscard]] PartialOrderEventReadResultV2 FindOrderState(
        const PartialOrderEventOrderKeyV2& key,
        PartialOrderEventOrderStateV2* output,
        PartialOrderEventStatusSnapshotV2* output_status) const noexcept {
        if (!KeyValid(key) || output == nullptr) {
            return PartialOrderEventReadResultV2::kInvalidArgument;
        }
        return compact_state_references_
                   ? FindOrderStateInTable(
                         compact_order_state_slots_,
                         key,
                         output,
                         output_status)
                   : FindOrderStateInTable(
                         order_state_slots_, key, output, output_status);
    }

    template <typename StateSlot>
    [[nodiscard]] PartialOrderEventReadResultV2 FindOrderStateInTable(
        const StateSlot* order_state_slots,
        const PartialOrderEventOrderKeyV2& key,
        PartialOrderEventOrderStateV2* output,
        PartialOrderEventStatusSnapshotV2* output_status) const noexcept {
        if (order_state_slots == nullptr) {
            return PartialOrderEventReadResultV2::kCorrupt;
        }
        PartialOrderEventStatusSnapshotV2 status{};
        std::uint32_t cut_bank = 0U;
        std::uint64_t cut_tag = 0U;
        const auto status_result =
            ReadStableStatus(&status, &cut_bank, &cut_tag);
        if (status_result != PartialOrderEventReadResultV2::kOk) {
            return status_result;
        }
        const std::uint64_t mask = header_->order_state_capacity - 1U;
        const std::uint64_t start = HashOrderKey(key) & mask;
        for (std::uint64_t probe = 0U;
             probe < header_->order_state_capacity;
             ++probe) {
            const std::uint64_t index = (start + probe) & mask;
            PartialOrderEventOrderStateSlotV2 slot_identity{};
            const auto key_result =
                CopyStateKey(order_state_slots[index], &slot_identity);
            if (key_result == StableCopyResult::kAbsent) {
                return PartialOrderEventReadResultV2::kNotFound;
            }
            if (key_result == StableCopyResult::kUncommitted) {
                continue;
            }
            if (key_result != StableCopyResult::kCopied) {
                return key_result == StableCopyResult::kInconsistent
                           ? PartialOrderEventReadResultV2::kInconsistent
                           : PartialOrderEventReadResultV2::kCorrupt;
            }
            if (!KeyEqual(slot_identity, key)) {
                continue;
            }
            PartialOrderEventOrderStateV2 local{};
            const auto state_result = CopyVisibleOrderState(
                order_state_slots[index],
                slot_identity,
                status.cut.order_state_canonical_frontier,
                status.cut.event_published_frontier,
                &local);
            std::atomic_thread_fence(std::memory_order_acq_rel);
            if (Atomic(header_->cuts[cut_bank].publish_tag)
                    .load(std::memory_order_acquire) != cut_tag) {
                return PartialOrderEventReadResultV2::kInconsistent;
            }
            if (state_result == StableCopyResult::kAbsent) {
                return PartialOrderEventReadResultV2::kNotFound;
            }
            if (state_result != StableCopyResult::kCopied) {
                return state_result == StableCopyResult::kInconsistent
                           ? PartialOrderEventReadResultV2::kInconsistent
                           : PartialOrderEventReadResultV2::kCorrupt;
            }
            *output = local;
            if (output_status != nullptr) {
                *output_status = status;
            }
            return PartialOrderEventReadResultV2::kOk;
        }
        return PartialOrderEventReadResultV2::kNotFound;
    }

    [[nodiscard]] PartialOrderEventReadResultV2 ReadOrderStates(
        std::uint64_t first_physical_slot,
        std::span<PartialOrderEventOrderStateV2> output,
        PartialOrderEventOrderStateBatchResultV2* result) const noexcept {
        if (first_physical_slot >= header_->order_state_capacity ||
            output.empty() || result == nullptr) {
            return PartialOrderEventReadResultV2::kInvalidArgument;
        }
        return compact_state_references_
                   ? ReadOrderStatesFromTable(
                         compact_order_state_slots_,
                         first_physical_slot,
                         output,
                         result)
                   : ReadOrderStatesFromTable(
                         order_state_slots_,
                         first_physical_slot,
                         output,
                         result);
    }

    template <typename StateSlot>
    [[nodiscard]] PartialOrderEventReadResultV2
    ReadOrderStatesFromTable(
        const StateSlot* order_state_slots,
        std::uint64_t first_physical_slot,
        std::span<PartialOrderEventOrderStateV2> output,
        PartialOrderEventOrderStateBatchResultV2* result) const noexcept {
        if (order_state_slots == nullptr) {
            return PartialOrderEventReadResultV2::kCorrupt;
        }
        *result = {};
        PartialOrderEventStatusSnapshotV2 status{};
        std::uint32_t cut_bank = 0U;
        std::uint64_t cut_tag = 0U;
        const auto status_result =
            ReadStableStatus(&status, &cut_bank, &cut_tag);
        if (status_result != PartialOrderEventReadResultV2::kOk) {
            return status_result;
        }
        std::uint64_t slot_index = first_physical_slot;
        std::size_t output_index = 0U;
        while (slot_index < header_->order_state_capacity &&
               output_index < output.size()) {
            PartialOrderEventOrderStateSlotV2 identity{};
            const auto key_result = CopyStateKey(
                order_state_slots[slot_index], &identity);
            if (key_result == StableCopyResult::kCopied) {
                PartialOrderEventOrderStateV2 state{};
                const auto state_result = CopyVisibleOrderState(
                    order_state_slots[slot_index],
                    identity,
                    status.cut.order_state_canonical_frontier,
                    status.cut.event_published_frontier,
                    &state);
                if (state_result == StableCopyResult::kCopied) {
                    output[output_index] = state;
                    ++output_index;
                } else if (
                    state_result != StableCopyResult::kAbsent) {
                    return state_result ==
                                   StableCopyResult::kInconsistent
                               ? PartialOrderEventReadResultV2::
                                     kInconsistent
                               : PartialOrderEventReadResultV2::kCorrupt;
                }
            } else if (
                key_result != StableCopyResult::kAbsent &&
                key_result != StableCopyResult::kUncommitted) {
                return key_result == StableCopyResult::kInconsistent
                           ? PartialOrderEventReadResultV2::kInconsistent
                           : PartialOrderEventReadResultV2::kCorrupt;
            }
            ++slot_index;
        }
        std::atomic_thread_fence(std::memory_order_acq_rel);
        if (Atomic(header_->cuts[cut_bank].publish_tag)
                .load(std::memory_order_acquire) != cut_tag) {
            return PartialOrderEventReadResultV2::kInconsistent;
        }
        result->rows_read = output_index;
        result->next_physical_slot = slot_index;
        result->status = status;
        return PartialOrderEventReadResultV2::kOk;
    }

private:
    [[nodiscard]] PartialOrderEventReadResultV2 ReadStableStatus(
        PartialOrderEventStatusSnapshotV2* output,
        std::uint32_t* output_bank,
        std::uint64_t* output_tag) const noexcept {
        if (output == nullptr || output_bank == nullptr ||
            output_tag == nullptr) {
            return PartialOrderEventReadResultV2::kInvalidArgument;
        }
        std::array<PartialOrderEventCommitCutV2, 2U> cuts{};
        std::array<bool, 2U> valid{};
        for (std::uint32_t bank = 0U; bank < 2U; ++bank) {
            valid[bank] = CopyCut(bank, &cuts[bank]);
        }
        if (!valid[0U] && !valid[1U]) {
            return PartialOrderEventReadResultV2::kInconsistent;
        }
        std::uint32_t selected = valid[1U] ? 1U : 0U;
        if (valid[0U] &&
            (!valid[1U] ||
             cuts[0U].commit_sequence > cuts[1U].commit_sequence)) {
            selected = 0U;
        }
        if (valid[0U] && valid[1U] &&
            cuts[0U].commit_sequence == cuts[1U].commit_sequence) {
            return PartialOrderEventReadResultV2::kCorrupt;
        }

        PartialOrderEventStatusSnapshotV2 status{};
        status.run_id = header_->run_id;
        status.session_epoch = header_->session_epoch;
        status.trade_date = header_->trade_date;
        status.temporal_coverage = header_->temporal_coverage;
        status.publication_generation =
            header_->publication_generation;
        status.correction_epoch = header_->correction_epoch;
        status.coverage_start_unix_ns =
            header_->coverage_start_unix_ns;
        status.ordering_quality = header_->ordering_quality;
        status.event_capacity = header_->event_capacity;
        status.channel_capacity = header_->channel_capacity;
        status.order_state_capacity = header_->order_state_capacity;
        status.cut = cuts[selected];
        *output = status;
        *output_bank = selected;
        *output_tag = cuts[selected].publish_tag;
        return PartialOrderEventReadResultV2::kOk;
    }

    [[nodiscard]] bool CopyCut(
        std::uint32_t bank,
        PartialOrderEventCommitCutV2* output) const noexcept {
        if (bank > 1U || output == nullptr) {
            return false;
        }
        const PartialOrderEventCommitCutV2& source =
            header_->cuts[bank];
        for (std::size_t attempt = 0U; attempt < kReadAttempts;
             ++attempt) {
            const std::uint64_t begin =
                Atomic(source.publish_tag)
                    .load(std::memory_order_acquire);
            if (begin == 0U || (begin & 1U) != 0U) {
                continue;
            }
            PartialOrderEventCommitCutV2 local{};
            AtomicCopyWords(source, &local);
            std::atomic_thread_fence(std::memory_order_acq_rel);
            const std::uint64_t end =
                Atomic(source.publish_tag)
                    .load(std::memory_order_acquire);
            if (begin != end || local.publish_tag != begin ||
                (end & 1U) != 0U) {
                continue;
            }
            const bool canonical = compact_state_references_
                                       ? PartialOrderEventCommitCutCanonicalV3(
                                             *header_, local, bank)
                                       : PartialOrderEventCommitCutCanonicalV2(
                                             *header_, local, bank);
            if (!canonical) {
                return false;
            }
            *output = local;
            return true;
        }
        return false;
    }

    [[nodiscard]] StableCopyResult CopyEvent(
        std::uint64_t sequence,
        std::uint64_t visible_frontier,
        PartialOrderEventEnvelopeV2* output) const noexcept {
        if (output == nullptr || sequence == 0U ||
            sequence > visible_frontier) {
            return StableCopyResult::kCorrupt;
        }
        const PartialOrderEventSlotV2& source =
            event_slots_[sequence - 1U];
        for (std::size_t attempt = 0U; attempt < kReadAttempts;
             ++attempt) {
            const std::uint64_t begin =
                Atomic(source.publish_tag)
                    .load(std::memory_order_acquire);
            if (begin == 0U) {
                return StableCopyResult::kCorrupt;
            }
            if ((begin & 1U) != 0U) {
                continue;
            }
            PartialOrderEventEnvelopeV2 envelope{};
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
            if (begin != 2U ||
                !partial_order_event_wire_v2_detail::AllZero(
                    source.reserved)) {
                return StableCopyResult::kCorrupt;
            }
            std::memcpy(
                &envelope.event, words.data(), sizeof(envelope.event));
            if (!PartialOrderEventEnvelopeCanonicalV2(
                    envelope, header_->trade_date, sequence)) {
                return StableCopyResult::kCorrupt;
            }
            *output = envelope;
            return StableCopyResult::kCopied;
        }
        return StableCopyResult::kInconsistent;
    }

    template <typename StateSlot>
    [[nodiscard]] StableCopyResult CopyStateKey(
        const StateSlot& source,
        PartialOrderEventOrderStateSlotV2* output) const noexcept {
        if (output == nullptr) {
            return StableCopyResult::kCorrupt;
        }
        for (std::size_t attempt = 0U; attempt < kReadAttempts;
             ++attempt) {
            const std::uint64_t begin =
                Atomic(source.key_publish_tag)
                    .load(std::memory_order_acquire);
            if (begin == 0U) {
                return StableCopyResult::kAbsent;
            }
            if ((begin & 1U) != 0U) {
                return StableCopyResult::kUncommitted;
            }
            PartialOrderEventOrderStateSlotV2 identity{};
            identity.key_publish_tag = begin;
            identity.market =
                Atomic(source.market).load(std::memory_order_relaxed);
            identity.instrument_id = Atomic(source.instrument_id)
                                         .load(std::memory_order_relaxed);
            identity.channel =
                Atomic(source.channel).load(std::memory_order_relaxed);
            identity.order_id =
                Atomic(source.order_id).load(std::memory_order_relaxed);
            std::atomic_thread_fence(std::memory_order_acq_rel);
            const std::uint64_t end =
                Atomic(source.key_publish_tag)
                    .load(std::memory_order_acquire);
            if (begin != end || (end & 1U) != 0U) {
                continue;
            }
            if (begin != 2U || identity.market == 0U ||
                identity.instrument_id == 0U || identity.channel < 0 ||
                identity.order_id <= 0 ||
                !partial_order_event_wire_v2_detail::AllZero(
                    source.reserved_identity)) {
                return StableCopyResult::kCorrupt;
            }
            *output = identity;
            return StableCopyResult::kCopied;
        }
        return StableCopyResult::kInconsistent;
    }

    [[nodiscard]] StableCopyResult CopyVisibleOrderState(
        const PartialOrderEventOrderStateSlotV2& source,
        const PartialOrderEventOrderStateSlotV2& identity,
        std::uint64_t visible_canonical_frontier,
        std::uint64_t,
        PartialOrderEventOrderStateV2* output) const noexcept {
        if (output == nullptr) {
            return StableCopyResult::kCorrupt;
        }
        std::array<PartialOrderEventOrderStateV2, 2U> states{};
        std::array<bool, 2U> visible{};
        for (std::size_t index = 0U; index < states.size(); ++index) {
            const auto result = CopyStateVersion(
                source.versions[index], &states[index]);
            if (result == StableCopyResult::kInconsistent ||
                result == StableCopyResult::kCorrupt) {
                return result;
            }
            visible[index] =
                result == StableCopyResult::kCopied &&
                states[index].canonical_apply_sequence <=
                    visible_canonical_frontier;
            if (visible[index] &&
                !PartialOrderEventOrderStateCanonicalV2(
                    states[index],
                    header_->trade_date,
                    identity.market,
                    identity.instrument_id,
                    identity.channel,
                    identity.order_id)) {
                return StableCopyResult::kCorrupt;
            }
        }
        if (!visible[0U] && !visible[1U]) {
            return StableCopyResult::kAbsent;
        }
        std::size_t selected = visible[1U] ? 1U : 0U;
        if (visible[0U] &&
            (!visible[1U] ||
             states[0U].canonical_apply_sequence >
                 states[1U].canonical_apply_sequence)) {
            selected = 0U;
        }
        if (visible[0U] && visible[1U] &&
            states[0U].canonical_apply_sequence ==
                states[1U].canonical_apply_sequence) {
            return StableCopyResult::kCorrupt;
        }
        *output = states[selected];
        return StableCopyResult::kCopied;
    }

    [[nodiscard]] StableCopyResult CopyVisibleOrderState(
        const PartialOrderEventOrderStateSlotV3& source,
        const PartialOrderEventOrderStateSlotV2& identity,
        std::uint64_t visible_canonical_frontier,
        std::uint64_t visible_event_frontier,
        PartialOrderEventOrderStateV2* output) const noexcept {
        if (output == nullptr) {
            return StableCopyResult::kCorrupt;
        }
        std::array<CompactStateReference, 2U> references{};
        std::array<bool, 2U> visible{};
        for (std::size_t index = 0U; index < references.size(); ++index) {
            const auto result = CopyCompactStateVersion(
                source.versions[index], &references[index]);
            if (result == StableCopyResult::kInconsistent ||
                result == StableCopyResult::kCorrupt) {
                return result;
            }
            visible[index] =
                result == StableCopyResult::kCopied &&
                references[index].canonical_apply_sequence <=
                    visible_canonical_frontier &&
                references[index].derived_event_sequence <=
                    visible_event_frontier;
        }
        if (!visible[0U] && !visible[1U]) {
            return StableCopyResult::kAbsent;
        }
        std::size_t selected = visible[1U] ? 1U : 0U;
        if (visible[0U] &&
            (!visible[1U] ||
             references[0U].canonical_apply_sequence >
                 references[1U].canonical_apply_sequence)) {
            selected = 0U;
        }
        if (visible[0U] && visible[1U] &&
            references[0U].canonical_apply_sequence ==
                references[1U].canonical_apply_sequence) {
            return StableCopyResult::kCorrupt;
        }

        PartialOrderEventEnvelopeV2 envelope{};
        const CompactStateReference& reference = references[selected];
        const auto event_result = CopyEvent(
            reference.derived_event_sequence,
            visible_event_frontier,
            &envelope);
        if (event_result != StableCopyResult::kCopied) {
            return event_result;
        }
        PartialOrderEventOrderStateV2 state{};
        state.canonical_apply_sequence =
            reference.canonical_apply_sequence;
        state.order_revision = envelope.event;
        if (envelope.canonical_apply_sequence !=
                reference.canonical_apply_sequence ||
            envelope.event.derived_event_sequence !=
                reference.derived_event_sequence ||
            envelope.event.event_kind !=
                L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_REVISION_V1 ||
            !PartialOrderEventOrderStateCanonicalV2(
                state,
                header_->trade_date,
                identity.market,
                identity.instrument_id,
                identity.channel,
                identity.order_id)) {
            return StableCopyResult::kCorrupt;
        }
        *output = state;
        return StableCopyResult::kCopied;
    }

    [[nodiscard]] StableCopyResult CopyStateVersion(
        const PartialOrderEventOrderStateVersionV2& source,
        PartialOrderEventOrderStateV2* output) const noexcept {
        for (std::size_t attempt = 0U; attempt < kReadAttempts;
             ++attempt) {
            const std::uint64_t begin =
                Atomic(source.publish_tag)
                    .load(std::memory_order_acquire);
            if (begin == 0U) {
                return StableCopyResult::kAbsent;
            }
            if ((begin & 1U) != 0U) {
                return StableCopyResult::kUncommitted;
            }
            PartialOrderEventOrderStateV2 state{};
            state.canonical_apply_sequence =
                Atomic(source.canonical_apply_sequence)
                    .load(std::memory_order_relaxed);
            std::array<std::uint64_t, 40U> words{};
            for (std::size_t index = 0U; index < words.size(); ++index) {
                words[index] = Atomic(source.payload_words[index])
                                   .load(std::memory_order_relaxed);
            }
            std::atomic_thread_fence(std::memory_order_acq_rel);
            const std::uint64_t end =
                Atomic(source.publish_tag)
                    .load(std::memory_order_acquire);
            if (begin != end || (end & 1U) != 0U) {
                continue;
            }
            if (state.canonical_apply_sequence == 0U ||
                state.canonical_apply_sequence >
                    std::numeric_limits<std::uint64_t>::max() / 2U ||
                begin != state.canonical_apply_sequence * 2U ||
                !partial_order_event_wire_v2_detail::AllZero(
                    source.reserved)) {
                return StableCopyResult::kCorrupt;
            }
            std::memcpy(
                &state.order_revision,
                words.data(),
                sizeof(state.order_revision));
            *output = state;
            return StableCopyResult::kCopied;
        }
        return StableCopyResult::kInconsistent;
    }

    [[nodiscard]] StableCopyResult CopyCompactStateVersion(
        const PartialOrderEventOrderStateVersionV3& source,
        CompactStateReference* output) const noexcept {
        if (output == nullptr) {
            return StableCopyResult::kCorrupt;
        }
        for (std::size_t attempt = 0U; attempt < kReadAttempts;
             ++attempt) {
            const std::uint64_t begin =
                Atomic(source.publish_tag)
                    .load(std::memory_order_acquire);
            if (begin == 0U) {
                return StableCopyResult::kAbsent;
            }
            if ((begin & 1U) != 0U) {
                return StableCopyResult::kUncommitted;
            }
            CompactStateReference reference{};
            reference.canonical_apply_sequence =
                Atomic(source.canonical_apply_sequence)
                    .load(std::memory_order_relaxed);
            reference.derived_event_sequence =
                Atomic(source.derived_event_sequence)
                    .load(std::memory_order_relaxed);
            const std::uint64_t reserved =
                Atomic(source.reserved).load(std::memory_order_relaxed);
            std::atomic_thread_fence(std::memory_order_acq_rel);
            const std::uint64_t end =
                Atomic(source.publish_tag)
                    .load(std::memory_order_acquire);
            if (begin != end || (end & 1U) != 0U) {
                continue;
            }
            if (reference.canonical_apply_sequence == 0U ||
                reference.canonical_apply_sequence >
                    std::numeric_limits<std::uint64_t>::max() / 2U ||
                begin != reference.canonical_apply_sequence * 2U ||
                reference.derived_event_sequence == 0U ||
                reserved != 0U) {
                return StableCopyResult::kCorrupt;
            }
            *output = reference;
            return StableCopyResult::kCopied;
        }
        return StableCopyResult::kInconsistent;
    }

    int descriptor_ = -1;
    void* mapping_ = MAP_FAILED;
    std::size_t mapping_bytes_ = 0U;
    const PartialOrderEventHeaderV2* header_ = nullptr;
    const PartialOrderEventSlotV2* event_slots_ = nullptr;
    std::array<const PartialOrderEventChannelHealthV2*, 2U>
        channel_banks_{};
    const PartialOrderEventOrderStateSlotV2* order_state_slots_ =
        nullptr;
    const PartialOrderEventOrderStateSlotV3*
        compact_order_state_slots_ = nullptr;
    bool compact_state_references_ = false;
};

std::string_view PartialOrderEventReaderOpenErrorNameV2(
    PartialOrderEventReaderOpenErrorV2 error) noexcept {
    switch (error) {
        case PartialOrderEventReaderOpenErrorV2::kNone:
            return "none";
        case PartialOrderEventReaderOpenErrorV2::kNullOutput:
            return "null_output";
        case PartialOrderEventReaderOpenErrorV2::kInvalidDescriptor:
            return "invalid_descriptor";
        case PartialOrderEventReaderOpenErrorV2::
            kInvalidExpectedSession:
            return "invalid_expected_session";
        case PartialOrderEventReaderOpenErrorV2::
            kDescriptorStatFailed:
            return "descriptor_stat_failed";
        case PartialOrderEventReaderOpenErrorV2::
            kDescriptorSealMismatch:
            return "descriptor_seal_mismatch";
        case PartialOrderEventReaderOpenErrorV2::kMappingFailed:
            return "mapping_failed";
        case PartialOrderEventReaderOpenErrorV2::
            kIncompatibleLayout:
            return "incompatible_layout";
        case PartialOrderEventReaderOpenErrorV2::kSessionMismatch:
            return "session_mismatch";
        case PartialOrderEventReaderOpenErrorV2::
            kNoStablePublicationCut:
            return "no_stable_publication_cut";
        case PartialOrderEventReaderOpenErrorV2::
            kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

std::string_view PartialOrderEventReadResultNameV2(
    PartialOrderEventReadResultV2 result) noexcept {
    switch (result) {
        case PartialOrderEventReadResultV2::kOk:
            return "ok";
        case PartialOrderEventReadResultV2::kInvalidArgument:
            return "invalid_argument";
        case PartialOrderEventReadResultV2::kInconsistent:
            return "inconsistent";
        case PartialOrderEventReadResultV2::kNotYetPublished:
            return "not_yet_published";
        case PartialOrderEventReadResultV2::kOutputTooSmall:
            return "output_too_small";
        case PartialOrderEventReadResultV2::kNotFound:
            return "not_found";
        case PartialOrderEventReadResultV2::kCorrupt:
            return "corrupt";
    }
    return "unknown";
}

PartialOrderEventReaderV2::PartialOrderEventReaderV2(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

PartialOrderEventReaderV2::~PartialOrderEventReaderV2() = default;

PartialOrderEventReaderOpenErrorV2
PartialOrderEventReaderV2::OpenDescriptor(
    int descriptor,
    const PartialOrderEventExpectedSessionV2& expected_session,
    std::unique_ptr<PartialOrderEventReaderV2>* output,
    int* system_error_number) noexcept {
    return OpenDescriptorInternal(
        descriptor,
        expected_session,
        false,
        output,
        system_error_number);
}

PartialOrderEventReaderOpenErrorV2
PartialOrderEventReaderV2::OpenDescriptorInternal(
    int descriptor,
    const PartialOrderEventExpectedSessionV2& expected_session,
    bool compact_state_references,
    std::unique_ptr<PartialOrderEventReaderV2>* output,
    int* system_error_number) noexcept {
    SetSystemError(system_error_number, 0);
    if (output == nullptr) {
        return PartialOrderEventReaderOpenErrorV2::kNullOutput;
    }
    output->reset();
    try {
        auto impl = std::make_unique<Impl>();
        const auto error = impl->Initialize(
            descriptor,
            expected_session,
            compact_state_references,
            system_error_number);
        if (error != PartialOrderEventReaderOpenErrorV2::kNone) {
            return error;
        }
        *output = std::unique_ptr<PartialOrderEventReaderV2>(
            new PartialOrderEventReaderV2(std::move(impl)));
        return PartialOrderEventReaderOpenErrorV2::kNone;
    } catch (const std::bad_alloc&) {
        return PartialOrderEventReaderOpenErrorV2::
            kUnexpectedFailure;
    } catch (...) {
        return PartialOrderEventReaderOpenErrorV2::
            kUnexpectedFailure;
    }
}

PartialOrderEventReadResultV2 PartialOrderEventReaderV2::ReadStatus(
    PartialOrderEventStatusSnapshotV2* output) const noexcept {
    return impl_->ReadStatus(output);
}

PartialOrderEventReadResultV2 PartialOrderEventReaderV2::ReadEvent(
    std::uint64_t derived_event_sequence,
    PartialOrderEventEnvelopeV2* output) const noexcept {
    return impl_->ReadEvent(derived_event_sequence, output);
}

PartialOrderEventReadResultV2 PartialOrderEventReaderV2::ReadEvents(
    std::uint64_t first_derived_event_sequence,
    std::span<PartialOrderEventEnvelopeV2> output,
    PartialOrderEventReadBatchResultV2* result) const noexcept {
    return impl_->ReadEvents(
        first_derived_event_sequence, output, result);
}

PartialOrderEventReadResultV2
PartialOrderEventReaderV2::ReadAffectedChannels(
    std::span<PartialOrderEventChannelHealthV2> output,
    std::size_t* rows_read,
    PartialOrderEventStatusSnapshotV2* status) const noexcept {
    return impl_->ReadAffectedChannels(output, rows_read, status);
}

PartialOrderEventReadResultV2
PartialOrderEventReaderV2::FindOrderState(
    const PartialOrderEventOrderKeyV2& key,
    PartialOrderEventOrderStateV2* output,
    PartialOrderEventStatusSnapshotV2* status) const noexcept {
    return impl_->FindOrderState(key, output, status);
}

PartialOrderEventReadResultV2
PartialOrderEventReaderV2::ReadOrderStates(
    std::uint64_t first_physical_slot,
    std::span<PartialOrderEventOrderStateV2> output,
    PartialOrderEventOrderStateBatchResultV2* result) const noexcept {
    return impl_->ReadOrderStates(first_physical_slot, output, result);
}

}  // namespace l2flow::ipc
