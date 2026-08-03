#include "l2flow/ipc/certified_tick_journal_v1.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
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
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#ifndef F_SEAL_FUTURE_WRITE
#define F_SEAL_FUTURE_WRITE 0x0010
#endif

namespace l2flow::ipc {
namespace {

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

[[nodiscard]] bool IdentityMatches(
    const std::array<std::uint8_t, 16U>& wire,
    const common::Identity128& expected) noexcept {
    return std::memcmp(
               wire.data(), expected.data(), wire.size()) == 0;
}

[[nodiscard]] bool ReadMonotonicNs(
    std::uint64_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    timespec value{};
    if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0 ||
        value.tv_sec < 0 || value.tv_nsec < 0 ||
        value.tv_nsec >= 1'000'000'000L) {
        return false;
    }
    constexpr std::uint64_t billion = 1'000'000'000U;
    const std::uint64_t seconds =
        static_cast<std::uint64_t>(value.tv_sec);
    if (seconds >
        (std::numeric_limits<std::uint64_t>::max() -
         static_cast<std::uint64_t>(value.tv_nsec)) /
            billion) {
        return false;
    }
    *output =
        seconds * billion + static_cast<std::uint64_t>(value.tv_nsec);
    return *output != 0U;
}

[[nodiscard]] bool AllocateBacking(
    int descriptor,
    std::uint64_t offset,
    std::uint64_t bytes,
    int* system_error_number) noexcept {
    const auto maximum = static_cast<std::uint64_t>(
        std::numeric_limits<off_t>::max());
    if (descriptor < 0 || bytes == 0U || offset > maximum ||
        bytes > maximum || offset > maximum - bytes) {
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

[[nodiscard]] bool CheckedLayout(
    std::uint64_t tick_capacity,
    std::uint64_t* mapping_bytes) noexcept {
    return certified_tick_journal_v1_detail::CheckedLayoutBytes(
        tick_capacity, mapping_bytes);
}

[[nodiscard]] bool RequiredCommittedBytes(
    std::uint64_t tick_count,
    std::uint64_t* output) noexcept {
    return certified_tick_journal_v1_detail::CheckedLayoutBytes(
        tick_count, output);
}

[[nodiscard]] bool ReadStatusSnapshot(
    const CertifiedTickJournalHeaderV1& header,
    CertifiedTickJournalStatusV1* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    constexpr std::size_t attempts = 8U;
    for (std::size_t attempt = 0U; attempt < attempts; ++attempt) {
        const std::uint64_t begin =
            Atomic(header.status_publish_tag)
                .load(std::memory_order_acquire);
        if (begin == 0U || (begin & 1U) != 0U) {
            continue;
        }
        CertifiedTickJournalStatusV1 status{};
        status.publish_tag = begin;
        status.heartbeat_monotonic_ns =
            Atomic(header.heartbeat_monotonic_ns)
                .load(std::memory_order_relaxed);
        status.canonical_apply_frontier =
            Atomic(header.canonical_apply_frontier)
                .load(std::memory_order_relaxed);
        status.generation =
            Atomic(header.generation)
                .load(std::memory_order_relaxed);
        status.published_tick_count =
            Atomic(header.published_tick_count)
                .load(std::memory_order_relaxed);
        status.committed_mapping_bytes =
            Atomic(header.committed_mapping_bytes)
                .load(std::memory_order_relaxed);
        const std::uint32_t state =
            Atomic(header.state).load(std::memory_order_relaxed);
        const std::uint32_t failure =
            Atomic(header.failure_code)
                .load(std::memory_order_relaxed);
        std::atomic_thread_fence(std::memory_order_acq_rel);
        const std::uint64_t end =
            Atomic(header.status_publish_tag)
                .load(std::memory_order_acquire);
        if (begin != end || (end & 1U) != 0U) {
            continue;
        }
        if (!certified_tick_journal_v1_detail::StateValid(state) ||
            !certified_tick_journal_v1_detail::AppendErrorValid(
                failure)) {
            return false;
        }
        status.state =
            static_cast<CertifiedTickJournalStateV1>(state);
        status.failure =
            static_cast<CertifiedTickJournalAppendErrorV1>(failure);
        *output = status;
        return true;
    }
    return false;
}

[[nodiscard]] bool StatusMatchesLayout(
    const CertifiedTickJournalStatusV1& status,
    std::uint64_t tick_capacity,
    std::uint64_t total_mapping_bytes) noexcept {
    if (!certified_tick_journal_v1_detail::StatusCanonical(
            status, tick_capacity, total_mapping_bytes)) {
        return false;
    }
    std::uint64_t required = 0U;
    return RequiredCommittedBytes(
               status.canonical_apply_frontier, &required) &&
           required <= status.committed_mapping_bytes;
}

[[nodiscard]] bool CopyTickSlotSnapshot(
    const RealtimeCertifiedTickSlotV1& slot,
    RealtimeCertifiedTickSlotV1* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    constexpr std::size_t attempts = 8U;
    for (std::size_t attempt = 0U; attempt < attempts; ++attempt) {
        const std::uint64_t begin =
            Atomic(slot.publish_tag).load(std::memory_order_acquire);
        if (begin == 0U || (begin & 1U) != 0U) {
            continue;
        }
        RealtimeCertifiedTickSlotV1 copy{};
        copy.publish_tag = begin;
        for (std::size_t index = 0U;
             index < copy.reserved0.size();
             ++index) {
            copy.reserved0[index] =
                Atomic(slot.reserved0[index])
                    .load(std::memory_order_relaxed);
        }
        for (std::size_t index = 0U;
             index < copy.payload_words.size();
             ++index) {
            copy.payload_words[index] =
                Atomic(slot.payload_words[index])
                    .load(std::memory_order_relaxed);
        }
        std::atomic_thread_fence(std::memory_order_acq_rel);
        const std::uint64_t end =
            Atomic(slot.publish_tag).load(std::memory_order_acquire);
        if (begin != end || (end & 1U) != 0U) {
            continue;
        }
        *output = copy;
        return true;
    }
    return false;
}

[[nodiscard]] bool CopyTickSlot(
    const RealtimeCertifiedTickSlotV1& slot,
    RealtimeCertifiedTickEnvelopeV1* output) noexcept {
    RealtimeCertifiedTickSlotV1 copy{};
    return CopyTickSlotSnapshot(slot, &copy) &&
           RealtimeCertifiedTickSlotDecodeV1(copy, output);
}

[[nodiscard]] CertifiedTickJournalReadResultV1
ReadBeyondCapacityResult(
    std::uint64_t first,
    const CertifiedTickJournalSessionV1& session,
    const CertifiedTickJournalStatusV1& status) noexcept {
    // capacity + 1 is the natural sequential cursor after every addressable
    // slot has been consumed. It has no backing slot: while ACTIVE it remains
    // a waitable tail, and once terminal it must expose that coherent status
    // cut instead of looking like an arbitrary out-of-range seek.
    // `first > capacity` makes first - 1 safe.
    const bool natural_full_tail =
        first - 1U == session.tick_capacity &&
        status.canonical_apply_frontier == session.tick_capacity;
    if (natural_full_tail) {
        switch (status.state) {
            case CertifiedTickJournalStateV1::kComplete:
                return CertifiedTickJournalReadResultV1::kEndOfStream;
            case CertifiedTickJournalStateV1::kFailed:
                return CertifiedTickJournalReadResultV1::kProducerFailed;
            case CertifiedTickJournalStateV1::kActive:
                return CertifiedTickJournalReadResultV1::kNotYetPublished;
        }
    }
    return CertifiedTickJournalReadResultV1::kOutOfRange;
}

}  // namespace

class CertifiedTickJournalProducerV1::Impl final {
public:
    explicit Impl(CertifiedTickJournalConfigV1 config)
        : config_(std::move(config)) {}

    ~Impl() {
        static_cast<void>(Stop());
        CloseDescriptor(&read_only_fd_);
        if (mapping_ != MAP_FAILED) {
            static_cast<void>(::munmap(
                mapping_, static_cast<std::size_t>(mapping_bytes_)));
        }
        CloseDescriptor(&memfd_);
    }

    [[nodiscard]] CertifiedTickJournalCreateErrorV1 Initialize(
        int* system_error_number) noexcept {
        SetSystemError(system_error_number, 0);
        if constexpr (std::endian::native != std::endian::little) {
            return CertifiedTickJournalCreateErrorV1::
                kInvalidConfiguration;
        }
        if (!IdentityNonzero(config_.run_id) ||
            config_.session_epoch == 0U ||
            !realtime_certified_wire_v1_detail::ValidTradeDate(
                config_.trade_date) ||
            config_.tick_capacity == 0U ||
            (config_.maximum_mapping_bytes != 0U &&
             config_.maximum_mapping_bytes <
                 kCertifiedTickJournalHeaderBytesV1) ||
            config_.lazy_commit_chunk_bytes < 4096U ||
            config_.lazy_commit_chunk_bytes % 4096U != 0U) {
            return CertifiedTickJournalCreateErrorV1::
                kInvalidConfiguration;
        }
        if (!CheckedLayout(config_.tick_capacity, &mapping_bytes_) ||
            (config_.maximum_mapping_bytes != 0U &&
             mapping_bytes_ > config_.maximum_mapping_bytes) ||
            mapping_bytes_ > static_cast<std::uint64_t>(
                                 std::numeric_limits<std::size_t>::max()) ||
            mapping_bytes_ > static_cast<std::uint64_t>(
                                 std::numeric_limits<off_t>::max())) {
            return CertifiedTickJournalCreateErrorV1::kLayoutOverflow;
        }

        memfd_ = ::memfd_create(
            "l2flow-certified-tick-journal-v1",
            MFD_CLOEXEC | MFD_ALLOW_SEALING);
        if (memfd_ < 0 ||
            ::ftruncate(
                memfd_, static_cast<off_t>(mapping_bytes_)) != 0) {
            SetSystemError(system_error_number, errno);
            return CertifiedTickJournalCreateErrorV1::
                kMappingCreateFailed;
        }
        if (!AllocateBacking(
                memfd_,
                0U,
                kCertifiedTickJournalHeaderBytesV1,
                system_error_number)) {
            return CertifiedTickJournalCreateErrorV1::
                kResourceExhausted;
        }
        committed_mapping_bytes_ =
            kCertifiedTickJournalHeaderBytesV1;
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
            return CertifiedTickJournalCreateErrorV1::
                kMappingCreateFailed;
        }
        std::memset(mapping_, 0, kCertifiedTickJournalHeaderBytesV1);

        const std::string proc_path =
            "/proc/self/fd/" + std::to_string(memfd_);
        read_only_fd_ =
            ::open(proc_path.c_str(), O_RDONLY | O_CLOEXEC);
        if (read_only_fd_ < 0) {
            SetSystemError(system_error_number, errno);
            return CertifiedTickJournalCreateErrorV1::
                kReadOnlyHandleFailed;
        }
        constexpr int seals =
            F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_FUTURE_WRITE |
            F_SEAL_SEAL;
        if (::fcntl(memfd_, F_ADD_SEALS, seals) != 0) {
            SetSystemError(system_error_number, errno);
            return CertifiedTickJournalCreateErrorV1::kSealFailed;
        }

        header_ = static_cast<CertifiedTickJournalHeaderV1*>(mapping_);
        slots_ = reinterpret_cast<RealtimeCertifiedTickSlotV1*>(
            static_cast<std::byte*>(mapping_) +
            kCertifiedTickJournalHeaderBytesV1);
        std::uint64_t now = 0U;
        if (!ReadMonotonicNs(&now)) {
            return CertifiedTickJournalCreateErrorV1::
                kUnexpectedFailure;
        }
        header_->magic = kCertifiedTickJournalMagicV1;
        header_->abi_major = kCertifiedTickJournalWireMajorV1;
        header_->abi_minor = kCertifiedTickJournalWireMinorV1;
        header_->header_bytes = kCertifiedTickJournalHeaderBytesV1;
        header_->endian_marker =
            kCertifiedTickJournalEndianMarkerV1;
        header_->total_mapping_bytes = mapping_bytes_;
        CopyIdentity(config_.run_id, &header_->run_id);
        header_->session_epoch = config_.session_epoch;
        header_->trade_date = config_.trade_date;
        header_->slots_offset = kCertifiedTickJournalHeaderBytesV1;
        header_->tick_capacity = config_.tick_capacity;
        header_->slot_stride = kCertifiedTickJournalSlotBytesV1;
        header_->region_alignment = kCertifiedTickJournalAlignmentV1;
        header_->status_publish_tag = 2U;
        header_->heartbeat_monotonic_ns = now;
        header_->committed_mapping_bytes = committed_mapping_bytes_;
        header_->state = static_cast<std::uint32_t>(
            CertifiedTickJournalStateV1::kActive);
        header_->coverage_kind = static_cast<std::uint32_t>(
            CertifiedTickJournalCoverageKindV1::kFromOpen);
        header_->coverage_start_unix_ns = 0U;
        if (!CertifiedTickJournalHeaderCanonicalV1(*header_)) {
            return CertifiedTickJournalCreateErrorV1::
                kUnexpectedFailure;
        }
        return CertifiedTickJournalCreateErrorV1::kNone;
    }

    [[nodiscard]] CertifiedTickJournalAppendErrorV1 EnsureWritable(
        std::uint64_t required_tick_count) noexcept {
        if (failed_) {
            return CertifiedTickJournalAppendErrorV1::kFailed;
        }
        if (stopped_) {
            return CertifiedTickJournalAppendErrorV1::kStopped;
        }
        if (required_tick_count > config_.tick_capacity) {
            return Fail(
                CertifiedTickJournalAppendErrorV1::kTickCapacity);
        }
        std::uint64_t page_target = 0U;
        if (!RequiredCommittedBytes(required_tick_count, &page_target) ||
            page_target > mapping_bytes_) {
            return Fail(
                CertifiedTickJournalAppendErrorV1::kTickCapacity);
        }
        if (page_target <= committed_mapping_bytes_) {
            return CertifiedTickJournalAppendErrorV1::kNone;
        }

        std::uint64_t target = mapping_bytes_;
        const std::uint64_t chunk = config_.lazy_commit_chunk_bytes;
        if (page_target <=
            std::numeric_limits<std::uint64_t>::max() -
                (chunk - 1U)) {
            target = ((page_target + chunk - 1U) / chunk) * chunk;
        }
        target = std::min(target, mapping_bytes_);
        int system_error_number = 0;
        if (!AllocateBacking(
                memfd_,
                committed_mapping_bytes_,
                target - committed_mapping_bytes_,
                &system_error_number)) {
            last_system_error_ = system_error_number;
            return Fail(
                CertifiedTickJournalAppendErrorV1::
                    kBackingCommitFailed);
        }
        committed_mapping_bytes_ = target;
        return CertifiedTickJournalAppendErrorV1::kNone;
    }

    [[nodiscard]] CertifiedTickJournalAppendErrorV1 Append(
        std::span<const RealtimeCertifiedTickEnvelopeV1> ticks)
        noexcept {
        if (failed_) {
            return CertifiedTickJournalAppendErrorV1::kFailed;
        }
        if (stopped_) {
            return CertifiedTickJournalAppendErrorV1::kStopped;
        }
        if (ticks.empty()) {
            return Fail(
                CertifiedTickJournalAppendErrorV1::kInvalidArgument);
        }
        static_assert(
            sizeof(std::size_t) <= sizeof(std::uint64_t));
        if (static_cast<std::uint64_t>(ticks.size()) >
                config_.tick_capacity - canonical_apply_frontier_) {
            return Fail(
                CertifiedTickJournalAppendErrorV1::kTickCapacity);
        }
        const std::uint64_t count =
            static_cast<std::uint64_t>(ticks.size());
        const std::uint64_t new_frontier =
            canonical_apply_frontier_ + count;
        const auto writable = EnsureWritable(new_frontier);
        if (writable != CertifiedTickJournalAppendErrorV1::kNone) {
            return writable;
        }

        const std::uint64_t header_tag =
            Atomic(header_->status_publish_tag)
                .load(std::memory_order_acquire);
        if (header_tag == 0U || (header_tag & 1U) != 0U ||
            header_tag >
                std::numeric_limits<std::uint64_t>::max() - 2U) {
            return Fail(
                CertifiedTickJournalAppendErrorV1::
                    kPublicationInvariant);
        }

        for (std::size_t index = 0U; index < ticks.size(); ++index) {
            const std::uint64_t expected_sequence =
                canonical_apply_frontier_ +
                static_cast<std::uint64_t>(index) + 1U;
            const auto& tick = ticks[index];
            if (tick.canonical_apply_sequence != expected_sequence) {
                return Fail(
                    CertifiedTickJournalAppendErrorV1::
                        kCanonicalSequence);
            }
            if (!RealtimeCertifiedTickEnvelopeCanonicalV1(tick) ||
                tick.payload.common.trade_date != config_.trade_date) {
                return Fail(
                    CertifiedTickJournalAppendErrorV1::
                        kEnvelopeInvalid);
            }
            const auto& slot = slots_[expected_sequence - 1U];
            if (Atomic(slot.publish_tag)
                        .load(std::memory_order_acquire) != 0U ||
                !realtime_certified_wire_v1_detail::AllZero(
                    slot.reserved0)) {
                return Fail(
                    CertifiedTickJournalAppendErrorV1::
                        kPublicationInvariant);
            }
        }

        for (std::size_t index = 0U; index < ticks.size(); ++index) {
            const std::uint64_t sequence =
                canonical_apply_frontier_ +
                static_cast<std::uint64_t>(index) + 1U;
            std::uint64_t expected_tag = 0U;
            if (!Atomic(slots_[sequence - 1U].publish_tag)
                     .compare_exchange_strong(
                         expected_tag,
                         1U,
                         std::memory_order_acq_rel,
                         std::memory_order_acquire)) {
                return Fail(
                    CertifiedTickJournalAppendErrorV1::
                        kPublicationInvariant);
            }
        }

        for (std::size_t index = 0U; index < ticks.size(); ++index) {
            const std::uint64_t sequence =
                canonical_apply_frontier_ +
                static_cast<std::uint64_t>(index) + 1U;
            auto& slot = slots_[sequence - 1U];
            const auto words = std::bit_cast<
                std::array<
                    std::uint64_t,
                    kRealtimeCertifiedTickEnvelopeWordsV1>>(
                ticks[index]);
            for (std::size_t word = 0U;
                 word < slot.payload_words.size();
                 ++word) {
                Atomic(slot.payload_words[word])
                    .store(
                        word < words.size() ? words[word] : 0U,
                        std::memory_order_relaxed);
            }
            Atomic(slot.publish_tag)
                .store(2U, std::memory_order_release);
        }

        // The envelope timestamp was sampled by the same serialized
        // CERTIFIED worker immediately before this append and is part of the
        // canonical payload validated above. Reusing it avoids a second
        // clock_gettime syscall on every Tick and leaves no clock failure
        // point after all dependent projections have committed.
        const std::uint64_t heartbeat =
            ticks.back().certified_monotonic_ns;
        std::atomic_ref<std::uint64_t> tag =
            Atomic(header_->status_publish_tag);
        std::uint64_t expected_header_tag = header_tag;
        if (!tag.compare_exchange_strong(
                expected_header_tag,
                header_tag + 1U,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            return Fail(
                CertifiedTickJournalAppendErrorV1::
                    kPublicationInvariant);
        }
        Atomic(header_->heartbeat_monotonic_ns)
            .store(heartbeat, std::memory_order_relaxed);
        Atomic(header_->canonical_apply_frontier)
            .store(new_frontier, std::memory_order_relaxed);
        Atomic(header_->generation)
            .store(new_frontier, std::memory_order_relaxed);
        Atomic(header_->published_tick_count)
            .store(new_frontier, std::memory_order_relaxed);
        Atomic(header_->committed_mapping_bytes)
            .store(
                committed_mapping_bytes_,
                std::memory_order_relaxed);
        tag.store(header_tag + 2U, std::memory_order_release);
        canonical_apply_frontier_ = new_frontier;
        return CertifiedTickJournalAppendErrorV1::kNone;
    }

    [[nodiscard]] bool Stop() noexcept {
        if (failed_) {
            return false;
        }
        if (stopped_) {
            return true;
        }
        if (!PublishLifecycle(
                CertifiedTickJournalStateV1::kComplete,
                CertifiedTickJournalAppendErrorV1::kNone)) {
            static_cast<void>(Fail(
                CertifiedTickJournalAppendErrorV1::
                    kPublicationInvariant));
            return false;
        }
        stopped_ = true;
        return true;
    }

    [[nodiscard]] bool MarkUpstreamFailed() noexcept {
        if (failed_) {
            return true;
        }
        if (stopped_) {
            return false;
        }
        return Fail(
                   CertifiedTickJournalAppendErrorV1::
                       kUpstreamFailed) ==
               CertifiedTickJournalAppendErrorV1::kUpstreamFailed;
    }

    [[nodiscard]] bool MarkHistoryFailed(
        CertifiedTickJournalAppendErrorV1 failure) noexcept {
        if (failed_) {
            return true;
        }
        if (stopped_ ||
            (failure != CertifiedTickJournalAppendErrorV1::
                            kSourceRetentionLost &&
             failure != CertifiedTickJournalAppendErrorV1::
                            kIncompleteNativePrefix &&
             failure != CertifiedTickJournalAppendErrorV1::
                            kSourceReadFailed)) {
            return false;
        }
        return Fail(failure) == failure;
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

    [[nodiscard]] CertifiedTickJournalSessionV1 session()
        const noexcept {
        return {
            config_.run_id,
            config_.session_epoch,
            config_.trade_date,
            config_.tick_capacity,
            mapping_bytes_,
            CertifiedTickJournalCoverageKindV1::kFromOpen,
            0U};
    }

    [[nodiscard]] CertifiedTickJournalStatusV1 status()
        const noexcept {
        CertifiedTickJournalStatusV1 result{};
        if (header_ != nullptr) {
            static_cast<void>(ReadStatusSnapshot(*header_, &result));
        }
        return result;
    }

    [[nodiscard]] std::uint64_t canonical_apply_frontier()
        const noexcept {
        return canonical_apply_frontier_;
    }

    [[nodiscard]] std::uint64_t committed_mapping_bytes()
        const noexcept {
        return committed_mapping_bytes_;
    }

    [[nodiscard]] bool failed() const noexcept { return failed_; }

    [[nodiscard]] CertifiedTickJournalAppendErrorV1 last_error()
        const noexcept {
        return last_error_;
    }

private:
    [[nodiscard]] bool PublishLifecycle(
        CertifiedTickJournalStateV1 state,
        CertifiedTickJournalAppendErrorV1 failure) noexcept {
        if (header_ == nullptr) {
            return false;
        }
        std::uint64_t heartbeat = 0U;
        if (!ReadMonotonicNs(&heartbeat)) {
            return false;
        }
        std::atomic_ref<std::uint64_t> tag =
            Atomic(header_->status_publish_tag);
        const std::uint64_t stable =
            tag.load(std::memory_order_acquire);
        if (stable == 0U || (stable & 1U) != 0U ||
            stable >
                std::numeric_limits<std::uint64_t>::max() - 2U) {
            return false;
        }
        std::uint64_t expected = stable;
        if (!tag.compare_exchange_strong(
                expected,
                stable + 1U,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            return false;
        }
        Atomic(header_->heartbeat_monotonic_ns)
            .store(heartbeat, std::memory_order_relaxed);
        Atomic(header_->committed_mapping_bytes)
            .store(
                committed_mapping_bytes_,
                std::memory_order_relaxed);
        Atomic(header_->state)
            .store(
                static_cast<std::uint32_t>(state),
                std::memory_order_relaxed);
        Atomic(header_->failure_code)
            .store(
                static_cast<std::uint32_t>(failure),
                std::memory_order_relaxed);
        tag.store(stable + 2U, std::memory_order_release);
        return true;
    }

    [[nodiscard]] CertifiedTickJournalAppendErrorV1 Fail(
        CertifiedTickJournalAppendErrorV1 error) noexcept {
        if (failed_) {
            return CertifiedTickJournalAppendErrorV1::kFailed;
        }
        failed_ = true;
        last_error_ = error;
        static_cast<void>(PublishLifecycle(
            CertifiedTickJournalStateV1::kFailed, error));
        return error;
    }

    CertifiedTickJournalConfigV1 config_{};
    int memfd_ = -1;
    int read_only_fd_ = -1;
    void* mapping_ = MAP_FAILED;
    std::uint64_t mapping_bytes_ = 0U;
    std::uint64_t committed_mapping_bytes_ = 0U;
    CertifiedTickJournalHeaderV1* header_ = nullptr;
    RealtimeCertifiedTickSlotV1* slots_ = nullptr;
    std::uint64_t canonical_apply_frontier_ = 0U;
    int last_system_error_ = 0;
    CertifiedTickJournalAppendErrorV1 last_error_ =
        CertifiedTickJournalAppendErrorV1::kNone;
    bool stopped_ = false;
    bool failed_ = false;
};

class CertifiedTickJournalReaderV1::Impl final {
public:
    Impl() = default;

    ~Impl() {
        if (mapping_ != MAP_FAILED) {
            static_cast<void>(::munmap(mapping_, mapping_bytes_));
        }
    }

    [[nodiscard]] CertifiedTickJournalOpenErrorV1 Initialize(
        int descriptor,
        const CertifiedTickJournalSessionV1& expected_session,
        int* system_error_number) noexcept {
        SetSystemError(system_error_number, 0);
        if constexpr (std::endian::native != std::endian::little) {
            return CertifiedTickJournalOpenErrorV1::
                kUnsupportedEndian;
        }
        if (descriptor < 0 || !IdentityNonzero(expected_session.run_id) ||
            expected_session.session_epoch == 0U ||
            !realtime_certified_wire_v1_detail::ValidTradeDate(
                expected_session.trade_date) ||
            expected_session.tick_capacity == 0U ||
            expected_session.coverage_kind !=
                CertifiedTickJournalCoverageKindV1::kFromOpen ||
            expected_session.coverage_start_unix_ns != 0U ||
            expected_session.total_mapping_bytes <
                kCertifiedTickJournalHeaderBytesV1 ||
            expected_session.total_mapping_bytes >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max())) {
            return CertifiedTickJournalOpenErrorV1::kInvalidArgument;
        }

        const int descriptor_flags = ::fcntl(descriptor, F_GETFL);
        if (descriptor_flags < 0) {
            SetSystemError(system_error_number, errno);
            return CertifiedTickJournalOpenErrorV1::
                kDescriptorInvalid;
        }
        const int seals = ::fcntl(descriptor, F_GET_SEALS);
        if (seals < 0) {
            SetSystemError(system_error_number, errno);
            return CertifiedTickJournalOpenErrorV1::
                kDescriptorInvalid;
        }
        struct stat descriptor_status {};
        if (::fstat(descriptor, &descriptor_status) != 0) {
            SetSystemError(system_error_number, errno);
            return CertifiedTickJournalOpenErrorV1::
                kDescriptorInvalid;
        }
        if ((descriptor_flags & O_ACCMODE) != O_RDONLY ||
            !S_ISREG(descriptor_status.st_mode) ||
            descriptor_status.st_size < 0 ||
            static_cast<std::uint64_t>(descriptor_status.st_size) !=
                expected_session.total_mapping_bytes) {
            SetSystemError(system_error_number, EINVAL);
            return CertifiedTickJournalOpenErrorV1::
                kDescriptorInvalid;
        }
        constexpr int required_seals =
            F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_FUTURE_WRITE |
            F_SEAL_SEAL;
        if ((seals & required_seals) != required_seals) {
            SetSystemError(system_error_number, EINVAL);
            return CertifiedTickJournalOpenErrorV1::
                kDescriptorInvalid;
        }
        std::uint64_t expected_mapping_bytes = 0U;
        if (!CheckedLayout(
                expected_session.tick_capacity,
                &expected_mapping_bytes) ||
            expected_mapping_bytes !=
                expected_session.total_mapping_bytes) {
            return CertifiedTickJournalOpenErrorV1::kInvalidArgument;
        }

        mapping_bytes_ = static_cast<std::size_t>(
            expected_session.total_mapping_bytes);
        mapping_ = ::mmap(
            nullptr,
            mapping_bytes_,
            PROT_READ,
            MAP_SHARED,
            descriptor,
            0);
        if (mapping_ == MAP_FAILED) {
            SetSystemError(system_error_number, errno);
            return CertifiedTickJournalOpenErrorV1::kMappingFailed;
        }
        header_ =
            static_cast<const CertifiedTickJournalHeaderV1*>(mapping_);

        CertifiedTickJournalStatusV1 status{};
        if (!ReadStatusSnapshot(*header_, &status)) {
            return CertifiedTickJournalOpenErrorV1::kLayoutInvalid;
        }
        CertifiedTickJournalHeaderV1 stable{};
        stable.magic = header_->magic;
        stable.abi_major = header_->abi_major;
        stable.abi_minor = header_->abi_minor;
        stable.header_bytes = header_->header_bytes;
        stable.endian_marker = header_->endian_marker;
        stable.flags = header_->flags;
        stable.total_mapping_bytes = header_->total_mapping_bytes;
        stable.run_id = header_->run_id;
        stable.session_epoch = header_->session_epoch;
        stable.trade_date = header_->trade_date;
        stable.reserved_identity = header_->reserved_identity;
        stable.slots_offset = header_->slots_offset;
        stable.tick_capacity = header_->tick_capacity;
        stable.slot_stride = header_->slot_stride;
        stable.region_alignment = header_->region_alignment;
        stable.status_publish_tag = status.publish_tag;
        stable.heartbeat_monotonic_ns =
            status.heartbeat_monotonic_ns;
        stable.canonical_apply_frontier =
            status.canonical_apply_frontier;
        stable.generation = status.generation;
        stable.published_tick_count = status.published_tick_count;
        stable.committed_mapping_bytes =
            status.committed_mapping_bytes;
        stable.state = static_cast<std::uint32_t>(status.state);
        stable.failure_code =
            static_cast<std::uint32_t>(status.failure);
        stable.coverage_kind = header_->coverage_kind;
        stable.reserved_coverage = header_->reserved_coverage;
        stable.coverage_start_unix_ns =
            header_->coverage_start_unix_ns;
        stable.reserved = header_->reserved;
        if (!CertifiedTickJournalHeaderCanonicalV1(stable) ||
            !StatusMatchesLayout(
                status,
                stable.tick_capacity,
                stable.total_mapping_bytes)) {
            return CertifiedTickJournalOpenErrorV1::kLayoutInvalid;
        }
        if (!IdentityMatches(
                stable.run_id, expected_session.run_id) ||
            stable.session_epoch != expected_session.session_epoch ||
            stable.trade_date != expected_session.trade_date ||
            stable.tick_capacity != expected_session.tick_capacity ||
            stable.total_mapping_bytes !=
                expected_session.total_mapping_bytes ||
            stable.coverage_kind != static_cast<std::uint32_t>(
                expected_session.coverage_kind) ||
            stable.coverage_start_unix_ns !=
                expected_session.coverage_start_unix_ns) {
            return CertifiedTickJournalOpenErrorV1::kSessionMismatch;
        }
        session_ = expected_session;
        slots_ = reinterpret_cast<const RealtimeCertifiedTickSlotV1*>(
            static_cast<const std::byte*>(mapping_) +
            kCertifiedTickJournalHeaderBytesV1);
        return CertifiedTickJournalOpenErrorV1::kNone;
    }

    [[nodiscard]] CertifiedTickJournalReadResultV1 ReadStatus(
        CertifiedTickJournalStatusV1* output) const noexcept {
        if (output == nullptr) {
            return CertifiedTickJournalReadResultV1::kInvalidArgument;
        }
        CertifiedTickJournalStatusV1 status{};
        if (!ReadStatusSnapshot(*header_, &status)) {
            return CertifiedTickJournalReadResultV1::kInconsistent;
        }
        if (!StatusMatchesLayout(
                status,
                session_.tick_capacity,
                session_.total_mapping_bytes)) {
            return CertifiedTickJournalReadResultV1::kCorrupt;
        }
        *output = status;
        return CertifiedTickJournalReadResultV1::kOk;
    }

    [[nodiscard]] CertifiedTickJournalReadResultV1 ReadOne(
        std::uint64_t sequence,
        RealtimeCertifiedTickEnvelopeV1* output) const noexcept {
        if (output == nullptr) {
            return CertifiedTickJournalReadResultV1::kInvalidArgument;
        }
        CertifiedTickJournalReadBatchV1 batch{};
        return Read(sequence, std::span(output, 1U), &batch);
    }

    [[nodiscard]] CertifiedTickJournalReadResultV1 Read(
        std::uint64_t first,
        std::span<RealtimeCertifiedTickEnvelopeV1> output,
        CertifiedTickJournalReadBatchV1* result) const noexcept {
        if (result == nullptr || output.empty() || first == 0U) {
            return CertifiedTickJournalReadResultV1::kInvalidArgument;
        }
        *result = {};
        result->next_canonical_apply_sequence = first;
        const auto status_result = ReadStatus(&result->status);
        if (status_result != CertifiedTickJournalReadResultV1::kOk) {
            return status_result;
        }
        if (first > session_.tick_capacity) {
            return ReadBeyondCapacityResult(
                first, session_, result->status);
        }
        if (first > result->status.canonical_apply_frontier) {
            switch (result->status.state) {
                case CertifiedTickJournalStateV1::kActive:
                    return CertifiedTickJournalReadResultV1::
                        kNotYetPublished;
                case CertifiedTickJournalStateV1::kComplete:
                    return CertifiedTickJournalReadResultV1::
                        kEndOfStream;
                case CertifiedTickJournalStateV1::kFailed:
                    return CertifiedTickJournalReadResultV1::
                        kProducerFailed;
            }
            return CertifiedTickJournalReadResultV1::kCorrupt;
        }
        const std::uint64_t available =
            result->status.canonical_apply_frontier - first + 1U;
        const std::uint64_t requested =
            static_cast<std::uint64_t>(output.size());
        const std::uint64_t count = std::min(available, requested);
        for (std::uint64_t offset = 0U; offset < count; ++offset) {
            const std::uint64_t sequence = first + offset;
            RealtimeCertifiedTickEnvelopeV1 envelope{};
            if (!CopyTickSlot(slots_[sequence - 1U], &envelope) ||
                envelope.canonical_apply_sequence != sequence ||
                envelope.payload.common.trade_date !=
                    session_.trade_date) {
                return CertifiedTickJournalReadResultV1::kCorrupt;
            }
            output[static_cast<std::size_t>(offset)] = envelope;
            ++result->written;
            result->next_canonical_apply_sequence = sequence + 1U;
        }
        return CertifiedTickJournalReadResultV1::kOk;
    }

    [[nodiscard]] CertifiedTickJournalReadResultV1 ReadSlots(
        std::uint64_t first,
        std::span<RealtimeCertifiedTickSlotV1> output,
        CertifiedTickJournalReadBatchV1* result) const noexcept {
        if (output.empty()) {
            return CertifiedTickJournalReadResultV1::kInvalidArgument;
        }
        return ReadSlotCopies(
            first,
            output.size(),
            result,
            [&output](
                std::size_t index,
                const RealtimeCertifiedTickSlotV1& slot) noexcept {
                output[index] = slot;
            });
    }

    [[nodiscard]] CertifiedTickJournalReadResultV1 ReadSlotBytes(
        std::uint64_t first,
        std::span<std::byte> output,
        CertifiedTickJournalReadBatchV1* result) const noexcept {
        if (output.empty() ||
            output.size() % kCertifiedTickJournalSlotBytesV1 != 0U) {
            return CertifiedTickJournalReadResultV1::kInvalidArgument;
        }
        const std::size_t row_count =
            output.size() / kCertifiedTickJournalSlotBytesV1;
        return ReadSlotCopies(
            first,
            row_count,
            result,
            [&output](
                std::size_t index,
                const RealtimeCertifiedTickSlotV1& slot) noexcept {
                std::memcpy(
                    output.data() +
                        index * kCertifiedTickJournalSlotBytesV1,
                    &slot,
                    sizeof(slot));
            });
    }

    [[nodiscard]] const CertifiedTickJournalSessionV1& session()
        const noexcept {
        return session_;
    }

private:
    template <typename Store>
    [[nodiscard]] CertifiedTickJournalReadResultV1 ReadSlotCopies(
        std::uint64_t first,
        std::size_t requested_rows,
        CertifiedTickJournalReadBatchV1* result,
        Store&& store) const noexcept {
        if (result == nullptr || requested_rows == 0U || first == 0U) {
            return CertifiedTickJournalReadResultV1::kInvalidArgument;
        }
        *result = {};
        result->next_canonical_apply_sequence = first;
        const auto status_result = ReadStatus(&result->status);
        if (status_result != CertifiedTickJournalReadResultV1::kOk) {
            return status_result;
        }
        if (first > session_.tick_capacity) {
            return ReadBeyondCapacityResult(
                first, session_, result->status);
        }
        if (first > result->status.canonical_apply_frontier) {
            switch (result->status.state) {
                case CertifiedTickJournalStateV1::kActive:
                    return CertifiedTickJournalReadResultV1::
                        kNotYetPublished;
                case CertifiedTickJournalStateV1::kComplete:
                    return CertifiedTickJournalReadResultV1::
                        kEndOfStream;
                case CertifiedTickJournalStateV1::kFailed:
                    return CertifiedTickJournalReadResultV1::
                        kProducerFailed;
            }
            return CertifiedTickJournalReadResultV1::kCorrupt;
        }
        const std::uint64_t available =
            result->status.canonical_apply_frontier - first + 1U;
        static_assert(sizeof(std::size_t) <= sizeof(std::uint64_t));
        const std::uint64_t requested =
            static_cast<std::uint64_t>(requested_rows);
        const std::uint64_t count = std::min(available, requested);
        for (std::uint64_t offset = 0U; offset < count; ++offset) {
            const std::uint64_t sequence = first + offset;
            RealtimeCertifiedTickSlotV1 slot{};
            RealtimeCertifiedTickEnvelopeV1 envelope{};
            if (!CopyTickSlotSnapshot(
                    slots_[sequence - 1U], &slot) ||
                !RealtimeCertifiedTickSlotDecodeV1(slot, &envelope) ||
                envelope.canonical_apply_sequence != sequence ||
                envelope.payload.common.trade_date !=
                    session_.trade_date) {
                return CertifiedTickJournalReadResultV1::kCorrupt;
            }
            store(static_cast<std::size_t>(offset), slot);
            ++result->written;
            result->next_canonical_apply_sequence = sequence + 1U;
        }
        return CertifiedTickJournalReadResultV1::kOk;
    }

    void* mapping_ = MAP_FAILED;
    std::size_t mapping_bytes_ = 0U;
    const CertifiedTickJournalHeaderV1* header_ = nullptr;
    const RealtimeCertifiedTickSlotV1* slots_ = nullptr;
    CertifiedTickJournalSessionV1 session_{};
};

std::string_view CertifiedTickJournalCreateErrorNameV1(
    CertifiedTickJournalCreateErrorV1 error) noexcept {
    switch (error) {
        case CertifiedTickJournalCreateErrorV1::kNone:
            return "none";
        case CertifiedTickJournalCreateErrorV1::kNullOutput:
            return "null_output";
        case CertifiedTickJournalCreateErrorV1::
            kInvalidConfiguration:
            return "invalid_configuration";
        case CertifiedTickJournalCreateErrorV1::kLayoutOverflow:
            return "layout_overflow";
        case CertifiedTickJournalCreateErrorV1::
            kMappingCreateFailed:
            return "mapping_create_failed";
        case CertifiedTickJournalCreateErrorV1::
            kReadOnlyHandleFailed:
            return "read_only_handle_failed";
        case CertifiedTickJournalCreateErrorV1::kSealFailed:
            return "seal_failed";
        case CertifiedTickJournalCreateErrorV1::
            kResourceExhausted:
            return "resource_exhausted";
        case CertifiedTickJournalCreateErrorV1::
            kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

std::string_view CertifiedTickJournalAppendErrorNameV1(
    CertifiedTickJournalAppendErrorV1 error) noexcept {
    switch (error) {
        case CertifiedTickJournalAppendErrorV1::kNone:
            return "none";
        case CertifiedTickJournalAppendErrorV1::kInvalidArgument:
            return "invalid_argument";
        case CertifiedTickJournalAppendErrorV1::kCanonicalSequence:
            return "canonical_sequence";
        case CertifiedTickJournalAppendErrorV1::kTickCapacity:
            return "tick_capacity";
        case CertifiedTickJournalAppendErrorV1::
            kBackingCommitFailed:
            return "backing_commit_failed";
        case CertifiedTickJournalAppendErrorV1::kEnvelopeInvalid:
            return "envelope_invalid";
        case CertifiedTickJournalAppendErrorV1::
            kPublicationInvariant:
            return "publication_invariant";
        case CertifiedTickJournalAppendErrorV1::kUpstreamFailed:
            return "upstream_failed";
        case CertifiedTickJournalAppendErrorV1::kStopped:
            return "stopped";
        case CertifiedTickJournalAppendErrorV1::kFailed:
            return "failed";
        case CertifiedTickJournalAppendErrorV1::
            kSourceRetentionLost:
            return "source_retention_lost";
        case CertifiedTickJournalAppendErrorV1::
            kIncompleteNativePrefix:
            return "incomplete_native_prefix";
        case CertifiedTickJournalAppendErrorV1::kSourceReadFailed:
            return "source_read_failed";
    }
    return "unknown";
}

std::string_view CertifiedTickJournalOpenErrorNameV1(
    CertifiedTickJournalOpenErrorV1 error) noexcept {
    switch (error) {
        case CertifiedTickJournalOpenErrorV1::kNone:
            return "none";
        case CertifiedTickJournalOpenErrorV1::kNullOutput:
            return "null_output";
        case CertifiedTickJournalOpenErrorV1::kInvalidArgument:
            return "invalid_argument";
        case CertifiedTickJournalOpenErrorV1::kUnsupportedEndian:
            return "unsupported_endian";
        case CertifiedTickJournalOpenErrorV1::kDescriptorInvalid:
            return "descriptor_invalid";
        case CertifiedTickJournalOpenErrorV1::kMappingFailed:
            return "mapping_failed";
        case CertifiedTickJournalOpenErrorV1::kLayoutInvalid:
            return "layout_invalid";
        case CertifiedTickJournalOpenErrorV1::kSessionMismatch:
            return "session_mismatch";
        case CertifiedTickJournalOpenErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case CertifiedTickJournalOpenErrorV1::kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

std::string_view CertifiedTickJournalReadResultNameV1(
    CertifiedTickJournalReadResultV1 result) noexcept {
    switch (result) {
        case CertifiedTickJournalReadResultV1::kOk:
            return "ok";
        case CertifiedTickJournalReadResultV1::kInvalidArgument:
            return "invalid_argument";
        case CertifiedTickJournalReadResultV1::kNotYetPublished:
            return "not_yet_published";
        case CertifiedTickJournalReadResultV1::kEndOfStream:
            return "end_of_stream";
        case CertifiedTickJournalReadResultV1::kProducerFailed:
            return "producer_failed";
        case CertifiedTickJournalReadResultV1::kOutOfRange:
            return "out_of_range";
        case CertifiedTickJournalReadResultV1::kInconsistent:
            return "inconsistent";
        case CertifiedTickJournalReadResultV1::kCorrupt:
            return "corrupt";
    }
    return "unknown";
}

CertifiedTickJournalProducerV1::CertifiedTickJournalProducerV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

CertifiedTickJournalProducerV1::~CertifiedTickJournalProducerV1() =
    default;

CertifiedTickJournalCreateErrorV1
CertifiedTickJournalProducerV1::Create(
    CertifiedTickJournalConfigV1 config,
    std::shared_ptr<CertifiedTickJournalProducerV1>* output,
    int* system_error_number) noexcept {
    SetSystemError(system_error_number, 0);
    if (output == nullptr) {
        return CertifiedTickJournalCreateErrorV1::kNullOutput;
    }
    output->reset();
    try {
        auto impl = std::make_unique<Impl>(std::move(config));
        const auto error = impl->Initialize(system_error_number);
        if (error != CertifiedTickJournalCreateErrorV1::kNone) {
            return error;
        }
        output->reset(
            new CertifiedTickJournalProducerV1(std::move(impl)));
        return CertifiedTickJournalCreateErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return CertifiedTickJournalCreateErrorV1::
            kResourceExhausted;
    } catch (...) {
        return CertifiedTickJournalCreateErrorV1::
            kUnexpectedFailure;
    }
}

CertifiedTickJournalAppendErrorV1
CertifiedTickJournalProducerV1::EnsureWritable(
    std::uint64_t required_tick_count) noexcept {
    return impl_ == nullptr
               ? CertifiedTickJournalAppendErrorV1::kFailed
               : impl_->EnsureWritable(required_tick_count);
}

CertifiedTickJournalAppendErrorV1
CertifiedTickJournalProducerV1::Append(
    std::span<const RealtimeCertifiedTickEnvelopeV1> ticks) noexcept {
    return impl_ == nullptr
               ? CertifiedTickJournalAppendErrorV1::kFailed
               : impl_->Append(ticks);
}

bool CertifiedTickJournalProducerV1::Stop() noexcept {
    return impl_ != nullptr && impl_->Stop();
}

bool CertifiedTickJournalProducerV1::MarkUpstreamFailed() noexcept {
    return impl_ != nullptr && impl_->MarkUpstreamFailed();
}

bool CertifiedTickJournalProducerV1::MarkHistoryFailed(
    CertifiedTickJournalAppendErrorV1 failure) noexcept {
    return impl_ != nullptr && impl_->MarkHistoryFailed(failure);
}

bool CertifiedTickJournalProducerV1::DuplicateReadOnlyDescriptor(
    int* output, int* system_error_number) const noexcept {
    return impl_ != nullptr &&
           impl_->DuplicateReadOnlyDescriptor(
               output, system_error_number);
}

CertifiedTickJournalSessionV1
CertifiedTickJournalProducerV1::session() const noexcept {
    return impl_ == nullptr ? CertifiedTickJournalSessionV1{}
                            : impl_->session();
}

CertifiedTickJournalStatusV1
CertifiedTickJournalProducerV1::status() const noexcept {
    return impl_ == nullptr ? CertifiedTickJournalStatusV1{}
                            : impl_->status();
}

std::uint64_t
CertifiedTickJournalProducerV1::canonical_apply_frontier()
    const noexcept {
    return impl_ == nullptr ? 0U
                            : impl_->canonical_apply_frontier();
}

std::uint64_t
CertifiedTickJournalProducerV1::committed_mapping_bytes()
    const noexcept {
    return impl_ == nullptr ? 0U
                            : impl_->committed_mapping_bytes();
}

bool CertifiedTickJournalProducerV1::failed() const noexcept {
    return impl_ == nullptr || impl_->failed();
}

CertifiedTickJournalAppendErrorV1
CertifiedTickJournalProducerV1::last_error() const noexcept {
    return impl_ == nullptr
               ? CertifiedTickJournalAppendErrorV1::kFailed
               : impl_->last_error();
}

CertifiedTickJournalReaderV1::CertifiedTickJournalReaderV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

CertifiedTickJournalReaderV1::~CertifiedTickJournalReaderV1() =
    default;

CertifiedTickJournalOpenErrorV1
CertifiedTickJournalReaderV1::Open(
    int descriptor,
    const CertifiedTickJournalSessionV1& expected_session,
    std::unique_ptr<CertifiedTickJournalReaderV1>* output,
    int* system_error_number) noexcept {
    SetSystemError(system_error_number, 0);
    if (output == nullptr) {
        return CertifiedTickJournalOpenErrorV1::kNullOutput;
    }
    output->reset();
    try {
        auto impl = std::make_unique<Impl>();
        const auto error = impl->Initialize(
            descriptor, expected_session, system_error_number);
        if (error != CertifiedTickJournalOpenErrorV1::kNone) {
            return error;
        }
        output->reset(
            new CertifiedTickJournalReaderV1(std::move(impl)));
        return CertifiedTickJournalOpenErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return CertifiedTickJournalOpenErrorV1::
            kResourceExhausted;
    } catch (...) {
        return CertifiedTickJournalOpenErrorV1::
            kUnexpectedFailure;
    }
}

CertifiedTickJournalReadResultV1
CertifiedTickJournalReaderV1::ReadStatus(
    CertifiedTickJournalStatusV1* output) const noexcept {
    return impl_ == nullptr
               ? CertifiedTickJournalReadResultV1::kCorrupt
               : impl_->ReadStatus(output);
}

CertifiedTickJournalReadResultV1
CertifiedTickJournalReaderV1::ReadOne(
    std::uint64_t sequence,
    RealtimeCertifiedTickEnvelopeV1* output) const noexcept {
    return impl_ == nullptr
               ? CertifiedTickJournalReadResultV1::kCorrupt
               : impl_->ReadOne(sequence, output);
}

CertifiedTickJournalReadResultV1
CertifiedTickJournalReaderV1::Read(
    std::uint64_t first,
    std::span<RealtimeCertifiedTickEnvelopeV1> output,
    CertifiedTickJournalReadBatchV1* result) const noexcept {
    return impl_ == nullptr
               ? CertifiedTickJournalReadResultV1::kCorrupt
               : impl_->Read(first, output, result);
}

CertifiedTickJournalReadResultV1
CertifiedTickJournalReaderV1::ReadSlots(
    std::uint64_t first,
    std::span<RealtimeCertifiedTickSlotV1> output,
    CertifiedTickJournalReadBatchV1* result) const noexcept {
    return impl_ == nullptr
               ? CertifiedTickJournalReadResultV1::kCorrupt
               : impl_->ReadSlots(first, output, result);
}

CertifiedTickJournalReadResultV1
CertifiedTickJournalReaderV1::ReadSlotBytes(
    std::uint64_t first,
    std::span<std::byte> output,
    CertifiedTickJournalReadBatchV1* result) const noexcept {
    return impl_ == nullptr
               ? CertifiedTickJournalReadResultV1::kCorrupt
               : impl_->ReadSlotBytes(first, output, result);
}

const CertifiedTickJournalSessionV1&
CertifiedTickJournalReaderV1::session() const noexcept {
    static constexpr CertifiedTickJournalSessionV1 empty{};
    return impl_ == nullptr ? empty : impl_->session();
}

}  // namespace l2flow::ipc
