#include "l2flow/ipc/certified_order_event_journal_v1.h"

#include "l2flow/ipc/instrument_derived_event_wire_v1.h"

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
    if (descriptor < 0 || bytes == 0U ||
        offset >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max()) ||
        bytes >
            static_cast<std::uint64_t>(
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

}  // namespace

class CertifiedOrderEventJournalProducerV1::Impl final {
public:
    explicit Impl(CertifiedOrderEventJournalConfigV1 config)
        : config_(std::move(config)) {}

    ~Impl() {
        CloseDescriptor(&read_only_fd_);
        if (mapping_ != MAP_FAILED) {
            static_cast<void>(::munmap(
                mapping_, static_cast<std::size_t>(mapping_bytes_)));
        }
        CloseDescriptor(&memfd_);
    }

    [[nodiscard]] CertifiedOrderEventJournalCreateErrorV1
    Initialize(int* system_error_number) noexcept {
        SetSystemError(system_error_number, 0);
        if constexpr (std::endian::native != std::endian::little) {
            return CertifiedOrderEventJournalCreateErrorV1::
                kInvalidConfiguration;
        }
        if (!IdentityNonzero(config_.run_id) ||
            config_.session_epoch == 0U ||
            config_.trade_date == 0U ||
            config_.event_capacity == 0U ||
            (config_.maximum_mapping_bytes != 0U &&
             config_.maximum_mapping_bytes <
                 kCertifiedOrderEventHeaderBytesV1) ||
            config_.lazy_commit_chunk_bytes < 4096U ||
            config_.lazy_commit_chunk_bytes % 4096U != 0U ||
            (config_.temporal_coverage !=
                 CertifiedOrderEventTemporalCoverageV1::kFromOpen &&
             config_.temporal_coverage !=
                 CertifiedOrderEventTemporalCoverageV1::
                     kFromProcessStart) ||
            ((config_.temporal_coverage ==
                  CertifiedOrderEventTemporalCoverageV1::kFromOpen) !=
             (config_.coverage_start_unix_ns == 0U))) {
            return CertifiedOrderEventJournalCreateErrorV1::
                kInvalidConfiguration;
        }

        using namespace certified_order_event_wire_v1_detail;
        std::uint64_t slot_bytes = 0U;
        std::uint64_t logical_end = 0U;
        if (!CheckedMultiply(
                config_.event_capacity,
                kCertifiedOrderEventSlotBytesV1,
                &slot_bytes) ||
            !CheckedAdd(
                kCertifiedOrderEventHeaderBytesV1,
                slot_bytes,
                &logical_end) ||
            !AlignUp(logical_end, 4096U, &mapping_bytes_) ||
            (config_.maximum_mapping_bytes != 0U &&
             mapping_bytes_ > config_.maximum_mapping_bytes) ||
            mapping_bytes_ >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max()) ||
            mapping_bytes_ >
                static_cast<std::uint64_t>(
                    std::numeric_limits<off_t>::max())) {
            return CertifiedOrderEventJournalCreateErrorV1::
                kLayoutOverflow;
        }

        memfd_ = ::memfd_create(
            "l2flow-certified-order-events-v1",
            MFD_CLOEXEC | MFD_ALLOW_SEALING);
        if (memfd_ < 0 ||
            ::ftruncate(
                memfd_, static_cast<off_t>(mapping_bytes_)) != 0) {
            SetSystemError(system_error_number, errno);
            return CertifiedOrderEventJournalCreateErrorV1::
                kMappingCreateFailed;
        }
        if (!AllocateBacking(
                memfd_,
                0U,
                kCertifiedOrderEventHeaderBytesV1,
                system_error_number)) {
            return CertifiedOrderEventJournalCreateErrorV1::
                kResourceExhausted;
        }
        committed_mapping_bytes_ =
            kCertifiedOrderEventHeaderBytesV1;

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
            return CertifiedOrderEventJournalCreateErrorV1::
                kMappingCreateFailed;
        }
        std::memset(
            mapping_, 0, kCertifiedOrderEventHeaderBytesV1);

        const std::string proc_path =
            "/proc/self/fd/" + std::to_string(memfd_);
        read_only_fd_ =
            ::open(proc_path.c_str(), O_RDONLY | O_CLOEXEC);
        if (read_only_fd_ < 0) {
            SetSystemError(system_error_number, errno);
            return CertifiedOrderEventJournalCreateErrorV1::
                kReadOnlyHandleFailed;
        }
        constexpr int seals =
            F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_FUTURE_WRITE |
            F_SEAL_SEAL;
        if (::fcntl(memfd_, F_ADD_SEALS, seals) != 0) {
            SetSystemError(system_error_number, errno);
            return CertifiedOrderEventJournalCreateErrorV1::
                kSealFailed;
        }

        header_ =
            static_cast<CertifiedOrderEventHeaderV1*>(mapping_);
        slots_ = reinterpret_cast<CertifiedOrderEventSlotV1*>(
            static_cast<std::byte*>(mapping_) +
            kCertifiedOrderEventHeaderBytesV1);
        std::uint64_t now = 0U;
        if (!ReadMonotonicNs(&now)) {
            return CertifiedOrderEventJournalCreateErrorV1::
                kUnexpectedFailure;
        }
        header_->magic = kCertifiedOrderEventMagicV1;
        header_->abi_major = kCertifiedOrderEventWireMajorV1;
        header_->abi_minor = kCertifiedOrderEventWireMinorV1;
        header_->header_bytes =
            kCertifiedOrderEventHeaderBytesV1;
        header_->endian_marker =
            kCertifiedOrderEventEndianMarkerV1;
        header_->flags =
            config_.temporal_coverage ==
                    CertifiedOrderEventTemporalCoverageV1::kFromOpen
                ? kCertifiedOrderEventCoverageFromOpenV1
                : kCertifiedOrderEventCoverageFromProcessStartV1;
        header_->total_mapping_bytes = mapping_bytes_;
        CopyIdentity(config_.run_id, &header_->run_id);
        header_->session_epoch = config_.session_epoch;
        header_->trade_date = config_.trade_date;
        header_->slots_offset =
            kCertifiedOrderEventHeaderBytesV1;
        header_->event_capacity = config_.event_capacity;
        header_->slot_stride = kCertifiedOrderEventSlotBytesV1;
        header_->region_alignment =
            kCertifiedOrderEventAlignmentV1;
        header_->coverage_start_unix_ns =
            config_.coverage_start_unix_ns;
        header_->status_publish_tag = 2U;
        header_->heartbeat_monotonic_ns = now;
        header_->committed_mapping_bytes =
            committed_mapping_bytes_;
        if (!CertifiedOrderEventHeaderCanonicalV1(*header_)) {
            return CertifiedOrderEventJournalCreateErrorV1::
                kUnexpectedFailure;
        }
        return CertifiedOrderEventJournalCreateErrorV1::kNone;
    }

    [[nodiscard]] CertifiedOrderEventJournalPublishErrorV1
    EnsureWritable(
        std::uint64_t required_event_count) noexcept {
        if (failed_) {
            return CertifiedOrderEventJournalPublishErrorV1::kFailed;
        }
        if (required_event_count > config_.event_capacity) {
            return Fail(
                CertifiedOrderEventJournalPublishErrorV1::
                    kEventCapacity);
        }
        using namespace certified_order_event_wire_v1_detail;
        std::uint64_t slots_bytes = 0U;
        std::uint64_t required_bytes = 0U;
        std::uint64_t page_target = 0U;
        if (!CheckedMultiply(
                required_event_count,
                kCertifiedOrderEventSlotBytesV1,
                &slots_bytes) ||
            !CheckedAdd(
                kCertifiedOrderEventHeaderBytesV1,
                slots_bytes,
                &required_bytes) ||
            !AlignUp(required_bytes, 4096U, &page_target) ||
            page_target > mapping_bytes_) {
            return Fail(
                CertifiedOrderEventJournalPublishErrorV1::
                    kEventCapacity);
        }
        if (page_target <= committed_mapping_bytes_) {
            return CertifiedOrderEventJournalPublishErrorV1::kNone;
        }

        std::uint64_t target = mapping_bytes_;
        if (page_target <=
            std::numeric_limits<std::uint64_t>::max() -
                (config_.lazy_commit_chunk_bytes - 1U)) {
            target =
                ((page_target +
                  config_.lazy_commit_chunk_bytes - 1U) /
                 config_.lazy_commit_chunk_bytes) *
                config_.lazy_commit_chunk_bytes;
        }
        target = std::min(target, mapping_bytes_);
        const std::uint64_t bytes =
            target - committed_mapping_bytes_;
        int system_error_number = 0;
        if (!AllocateBacking(
                memfd_,
                committed_mapping_bytes_,
                bytes,
                &system_error_number)) {
            last_system_error_ = system_error_number;
            return Fail(
                CertifiedOrderEventJournalPublishErrorV1::
                    kBackingCommitFailed);
        }
        committed_mapping_bytes_ = target;
        return CertifiedOrderEventJournalPublishErrorV1::kNone;
    }

    [[nodiscard]] CertifiedOrderEventJournalPublishErrorV1
    PublishCanonicalTick(
        std::uint64_t canonical_apply_sequence,
        std::uint64_t shanghai_order_state_count,
        std::uint64_t shenzhen_order_state_count,
        std::span<const InstrumentDerivedEventV1> events) noexcept {
        if (failed_) {
            return CertifiedOrderEventJournalPublishErrorV1::kFailed;
        }
        if (canonical_apply_sequence == 0U ||
            canonical_apply_sequence !=
                canonical_apply_frontier_ + 1U ||
            canonical_apply_frontier_ ==
                std::numeric_limits<std::uint64_t>::max()) {
            return Fail(
                CertifiedOrderEventJournalPublishErrorV1::
                    kCanonicalSequence);
        }
        if (events.size() >
                std::numeric_limits<std::uint64_t>::max() -
                    published_event_sequence_ ||
            (!events.empty() &&
             events.size() - 1U >
                 static_cast<std::size_t>(
                     std::numeric_limits<std::uint32_t>::max())) ||
            static_cast<std::uint64_t>(events.size()) >
                config_.event_capacity -
                    published_event_sequence_) {
            return Fail(
                CertifiedOrderEventJournalPublishErrorV1::
                    kEventCapacity);
        }
        const std::uint64_t new_event_frontier =
            published_event_sequence_ +
            static_cast<std::uint64_t>(events.size());
        const auto writable = EnsureWritable(new_event_frontier);
        if (writable !=
            CertifiedOrderEventJournalPublishErrorV1::kNone) {
            return writable;
        }
        std::uint64_t heartbeat = 0U;
        if (!ReadMonotonicNs(&heartbeat)) {
            return Fail(
                CertifiedOrderEventJournalPublishErrorV1::
                    kPublicationInvariant);
        }
        const std::uint64_t header_tag =
            Atomic(header_->status_publish_tag)
                .load(std::memory_order_acquire);
        if (header_tag == 0U || (header_tag & 1U) != 0U ||
            header_tag >
                std::numeric_limits<std::uint64_t>::max() - 2U) {
            return Fail(
                CertifiedOrderEventJournalPublishErrorV1::
                    kPublicationInvariant);
        }

        // Every ordinary source Tick emits at most three rows. Stage that hot
        // path once on the stack; a potentially large Shanghai end-status
        // batch uses an allocation-free validation pass and deterministic
        // reprojection after the complete preflight.
        constexpr std::size_t kInlineProjectionRows = 3U;
        std::array<
            l2flow_instrument_derived_event_row_v1,
            kInlineProjectionRows>
            inline_rows{};
        const bool inline_projection =
            events.size() <= inline_rows.size();

        // Validate every projection and every destination while all
        // publication tags remain unchanged.
        for (std::size_t index = 0U; index < events.size(); ++index) {
            const std::uint64_t sequence =
                published_event_sequence_ +
                static_cast<std::uint64_t>(index) + 1U;
            CertifiedOrderEventSlotV1& slot =
                slots_[sequence - 1U];
            if (Atomic(slot.publish_tag)
                    .load(std::memory_order_acquire) != 0U ||
                !certified_order_event_wire_v1_detail::AllZero(
                    slot.reserved)) {
                return Fail(
                    CertifiedOrderEventJournalPublishErrorV1::
                        kPublicationInvariant);
            }
            l2flow_instrument_derived_event_row_v1 scratch{};
            auto& row = inline_projection
                            ? inline_rows[index]
                            : scratch;
            if (!events[index].source_tick_event_ordinal_valid ||
                events[index].source_tick_event_ordinal !=
                    static_cast<std::uint32_t>(index) ||
                !ProjectInstrumentDerivedEventWireV1(
                    events[index], &row) ||
                !CertifiedOrderEventRowCanonicalV1(
                    row, config_.trade_date, sequence)) {
                return Fail(
                    CertifiedOrderEventJournalPublishErrorV1::
                        kProjectionError);
            }
        }

        // Claim every append-only destination with an acquire/release CAS.
        // There is one serialized writer, so a failure after the complete
        // preflight is corruption, not ordinary contention. The old header
        // prefix remains the visibility gate even in that terminal case.
        for (std::size_t index = 0U; index < events.size(); ++index) {
            const std::uint64_t sequence =
                published_event_sequence_ +
                static_cast<std::uint64_t>(index) + 1U;
            std::uint64_t expected_tag = 0U;
            if (!Atomic(slots_[sequence - 1U].publish_tag)
                     .compare_exchange_strong(
                         expected_tag,
                         1U,
                         std::memory_order_acq_rel,
                         std::memory_order_acquire)) {
                return Fail(
                    CertifiedOrderEventJournalPublishErrorV1::
                        kPublicationInvariant);
            }
        }

        // Copy each validated immutable row while its tag is odd, then release
        // the stable even tag. Readers use acquire/tag, relaxed payload,
        // acquire-fence, acquire/tag and therefore cannot accept a torn row.
        for (std::size_t index = 0U; index < events.size(); ++index) {
            const std::uint64_t sequence =
                published_event_sequence_ +
                static_cast<std::uint64_t>(index) + 1U;
            CertifiedOrderEventSlotV1& slot =
                slots_[sequence - 1U];
            l2flow_instrument_derived_event_row_v1 scratch{};
            const l2flow_instrument_derived_event_row_v1* row =
                nullptr;
            if (inline_projection) {
                row = &inline_rows[index];
            } else {
                if (!ProjectInstrumentDerivedEventWireV1(
                        events[index], &scratch) ||
                    !CertifiedOrderEventRowCanonicalV1(
                        scratch,
                        config_.trade_date,
                        sequence)) {
                    return Fail(
                        CertifiedOrderEventJournalPublishErrorV1::
                            kProjectionError);
                }
                row = &scratch;
            }
            Atomic(slot.canonical_apply_sequence)
                .store(
                    canonical_apply_sequence,
                    std::memory_order_relaxed);
            std::array<std::uint64_t, 40U> words{};
            std::memcpy(words.data(), row, sizeof(*row));
            for (std::size_t word = 0U; word < words.size(); ++word) {
                Atomic(slot.payload_words[word])
                    .store(words[word], std::memory_order_relaxed);
            }
            Atomic(slot.publish_tag)
                .store(2U, std::memory_order_release);
        }

        std::atomic_ref<std::uint64_t> tag =
            Atomic(header_->status_publish_tag);
        std::uint64_t expected_header_tag = header_tag;
        if (!tag.compare_exchange_strong(
                expected_header_tag,
                header_tag + 1U,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            return Fail(
                CertifiedOrderEventJournalPublishErrorV1::
                    kPublicationInvariant);
        }
        Atomic(header_->heartbeat_monotonic_ns)
            .store(heartbeat, std::memory_order_relaxed);
        Atomic(header_->canonical_apply_frontier)
            .store(
                canonical_apply_sequence,
                std::memory_order_relaxed);
        Atomic(header_->event_published_sequence)
            .store(
                new_event_frontier,
                std::memory_order_relaxed);
        Atomic(header_->generation)
            .store(
                canonical_apply_sequence,
                std::memory_order_relaxed);
        Atomic(header_->shanghai_order_state_count)
            .store(
                shanghai_order_state_count,
                std::memory_order_relaxed);
        Atomic(header_->shenzhen_order_state_count)
            .store(
                shenzhen_order_state_count,
                std::memory_order_relaxed);
        Atomic(header_->committed_mapping_bytes)
            .store(
                committed_mapping_bytes_,
                std::memory_order_relaxed);
        tag.store(header_tag + 2U, std::memory_order_release);

        canonical_apply_frontier_ =
            canonical_apply_sequence;
        published_event_sequence_ = new_event_frontier;
        return CertifiedOrderEventJournalPublishErrorV1::kNone;
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
            duplicate =
                ::fcntl(read_only_fd_, F_DUPFD_CLOEXEC, 0);
        } while (duplicate < 0 && errno == EINTR);
        if (duplicate < 0) {
            SetSystemError(system_error_number, errno);
            return false;
        }
        *output = duplicate;
        return true;
    }

    [[nodiscard]] CertifiedOrderEventJournalSessionV1 session()
        const noexcept {
        return {
            config_.run_id,
            config_.session_epoch,
            config_.trade_date,
            config_.event_capacity,
            mapping_bytes_,
            config_.temporal_coverage,
            config_.coverage_start_unix_ns};
    }

    [[nodiscard]] std::uint64_t canonical_apply_frontier()
        const noexcept {
        return canonical_apply_frontier_;
    }

    [[nodiscard]] std::uint64_t published_event_sequence()
        const noexcept {
        return published_event_sequence_;
    }

    [[nodiscard]] std::uint64_t committed_mapping_bytes()
        const noexcept {
        return committed_mapping_bytes_;
    }

    [[nodiscard]] bool MarkStartupPrefixRecovered() noexcept {
        if (failed_ || header_ == nullptr) {
            return false;
        }
        const std::uint32_t current_flags =
            Atomic(header_->flags).load(std::memory_order_acquire);
        if ((current_flags &
             kCertifiedOrderEventStartupPrefixRecoveredV1) != 0U) {
            return true;
        }
        if (current_flags !=
            kCertifiedOrderEventCoverageFromOpenV1) {
            failed_ = true;
            return false;
        }
        std::atomic_ref<std::uint64_t> tag =
            Atomic(header_->status_publish_tag);
        const std::uint64_t stable =
            tag.load(std::memory_order_acquire);
        if (stable == 0U || (stable & 1U) != 0U ||
            stable >
                std::numeric_limits<std::uint64_t>::max() - 2U) {
            failed_ = true;
            return false;
        }
        std::uint64_t expected = stable;
        if (!tag.compare_exchange_strong(
                expected,
                stable + 1U,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            failed_ = true;
            return false;
        }
        Atomic(header_->flags).store(
            current_flags |
                kCertifiedOrderEventStartupPrefixRecoveredV1,
            std::memory_order_relaxed);
        tag.store(stable + 2U, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool FinalizeProcessStartCoverage(
        std::uint64_t coverage_start_unix_ns) noexcept {
        if (failed_ || header_ == nullptr ||
            config_.temporal_coverage !=
                CertifiedOrderEventTemporalCoverageV1::
                    kFromProcessStart ||
            coverage_start_unix_ns == 0U ||
            coverage_start_unix_ns <
                config_.coverage_start_unix_ns ||
            process_start_coverage_finalized_) {
            return false;
        }
        const std::uint32_t current_flags =
            Atomic(header_->flags).load(std::memory_order_acquire);
        if (current_flags !=
            kCertifiedOrderEventCoverageFromProcessStartV1) {
            failed_ = true;
            return false;
        }
        std::atomic_ref<std::uint64_t> tag =
            Atomic(header_->status_publish_tag);
        const std::uint64_t stable =
            tag.load(std::memory_order_acquire);
        if (stable == 0U || (stable & 1U) != 0U ||
            stable >
                std::numeric_limits<std::uint64_t>::max() - 2U) {
            failed_ = true;
            return false;
        }
        std::uint64_t expected = stable;
        if (!tag.compare_exchange_strong(
                expected,
                stable + 1U,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            failed_ = true;
            return false;
        }
        Atomic(header_->coverage_start_unix_ns)
            .store(coverage_start_unix_ns, std::memory_order_relaxed);
        tag.store(stable + 2U, std::memory_order_release);
        config_.coverage_start_unix_ns = coverage_start_unix_ns;
        process_start_coverage_finalized_ = true;
        if (!CertifiedOrderEventHeaderCanonicalV1(*header_)) {
            failed_ = true;
            return false;
        }
        return true;
    }

    [[nodiscard]] std::uint32_t coverage_flags() const noexcept {
        return header_ == nullptr
                   ? 0U
                   : Atomic(header_->flags)
                         .load(std::memory_order_acquire);
    }

    [[nodiscard]] std::uint64_t coverage_start_unix_ns() const
        noexcept {
        return config_.coverage_start_unix_ns;
    }

    [[nodiscard]] bool failed() const noexcept {
        return failed_;
    }

private:
    [[nodiscard]] CertifiedOrderEventJournalPublishErrorV1 Fail(
        CertifiedOrderEventJournalPublishErrorV1 error) noexcept {
        failed_ = true;
        return error;
    }

    CertifiedOrderEventJournalConfigV1 config_{};
    int memfd_ = -1;
    int read_only_fd_ = -1;
    void* mapping_ = MAP_FAILED;
    std::uint64_t mapping_bytes_ = 0U;
    std::uint64_t committed_mapping_bytes_ = 0U;
    CertifiedOrderEventHeaderV1* header_ = nullptr;
    CertifiedOrderEventSlotV1* slots_ = nullptr;
    std::uint64_t canonical_apply_frontier_ = 0U;
    std::uint64_t published_event_sequence_ = 0U;
    int last_system_error_ = 0;
    bool failed_ = false;
    bool process_start_coverage_finalized_ = false;
};

std::string_view CertifiedOrderEventJournalCreateErrorNameV1(
    CertifiedOrderEventJournalCreateErrorV1 error) noexcept {
    switch (error) {
        case CertifiedOrderEventJournalCreateErrorV1::kNone:
            return "none";
        case CertifiedOrderEventJournalCreateErrorV1::kNullOutput:
            return "null_output";
        case CertifiedOrderEventJournalCreateErrorV1::
            kInvalidConfiguration:
            return "invalid_configuration";
        case CertifiedOrderEventJournalCreateErrorV1::kLayoutOverflow:
            return "layout_overflow";
        case CertifiedOrderEventJournalCreateErrorV1::
            kMappingCreateFailed:
            return "mapping_create_failed";
        case CertifiedOrderEventJournalCreateErrorV1::
            kReadOnlyHandleFailed:
            return "read_only_handle_failed";
        case CertifiedOrderEventJournalCreateErrorV1::kSealFailed:
            return "seal_failed";
        case CertifiedOrderEventJournalCreateErrorV1::
            kResourceExhausted:
            return "resource_exhausted";
        case CertifiedOrderEventJournalCreateErrorV1::
            kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

std::string_view CertifiedOrderEventJournalPublishErrorNameV1(
    CertifiedOrderEventJournalPublishErrorV1 error) noexcept {
    switch (error) {
        case CertifiedOrderEventJournalPublishErrorV1::kNone:
            return "none";
        case CertifiedOrderEventJournalPublishErrorV1::
            kInvalidArgument:
            return "invalid_argument";
        case CertifiedOrderEventJournalPublishErrorV1::
            kCanonicalSequence:
            return "canonical_sequence";
        case CertifiedOrderEventJournalPublishErrorV1::
            kEventCapacity:
            return "event_capacity";
        case CertifiedOrderEventJournalPublishErrorV1::
            kBackingCommitFailed:
            return "backing_commit_failed";
        case CertifiedOrderEventJournalPublishErrorV1::
            kProjectionError:
            return "projection_error";
        case CertifiedOrderEventJournalPublishErrorV1::
            kPublicationInvariant:
            return "publication_invariant";
        case CertifiedOrderEventJournalPublishErrorV1::kFailed:
            return "failed";
    }
    return "unknown";
}

CertifiedOrderEventJournalProducerV1::
    CertifiedOrderEventJournalProducerV1(
        std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

CertifiedOrderEventJournalProducerV1::
    ~CertifiedOrderEventJournalProducerV1() = default;

CertifiedOrderEventJournalCreateErrorV1
CertifiedOrderEventJournalProducerV1::Create(
    CertifiedOrderEventJournalConfigV1 config,
    std::shared_ptr<CertifiedOrderEventJournalProducerV1>* output,
    int* system_error_number) noexcept {
    SetSystemError(system_error_number, 0);
    if (output == nullptr) {
        return CertifiedOrderEventJournalCreateErrorV1::kNullOutput;
    }
    output->reset();
    try {
        auto impl = std::make_unique<Impl>(std::move(config));
        const auto error = impl->Initialize(system_error_number);
        if (error != CertifiedOrderEventJournalCreateErrorV1::kNone) {
            return error;
        }
        output->reset(
            new CertifiedOrderEventJournalProducerV1(
                std::move(impl)));
        return CertifiedOrderEventJournalCreateErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return CertifiedOrderEventJournalCreateErrorV1::
            kResourceExhausted;
    } catch (...) {
        return CertifiedOrderEventJournalCreateErrorV1::
            kUnexpectedFailure;
    }
}

CertifiedOrderEventJournalPublishErrorV1
CertifiedOrderEventJournalProducerV1::EnsureWritable(
    std::uint64_t required_event_count) noexcept {
    return impl_ == nullptr
               ? CertifiedOrderEventJournalPublishErrorV1::kFailed
               : impl_->EnsureWritable(required_event_count);
}

CertifiedOrderEventJournalPublishErrorV1
CertifiedOrderEventJournalProducerV1::PublishCanonicalTick(
    std::uint64_t canonical_apply_sequence,
    std::uint64_t shanghai_order_state_count,
    std::uint64_t shenzhen_order_state_count,
    std::span<const InstrumentDerivedEventV1> events) noexcept {
    return impl_ == nullptr
               ? CertifiedOrderEventJournalPublishErrorV1::kFailed
               : impl_->PublishCanonicalTick(
                     canonical_apply_sequence,
                     shanghai_order_state_count,
                     shenzhen_order_state_count,
                     events);
}

bool CertifiedOrderEventJournalProducerV1::
    DuplicateReadOnlyDescriptor(
        int* output, int* system_error_number) const noexcept {
    return impl_ != nullptr &&
           impl_->DuplicateReadOnlyDescriptor(
               output, system_error_number);
}

CertifiedOrderEventJournalSessionV1
CertifiedOrderEventJournalProducerV1::session() const noexcept {
    return impl_ == nullptr
               ? CertifiedOrderEventJournalSessionV1{}
               : impl_->session();
}

std::uint64_t CertifiedOrderEventJournalProducerV1::
    canonical_apply_frontier() const noexcept {
    return impl_ == nullptr ? 0U
                            : impl_->canonical_apply_frontier();
}

std::uint64_t CertifiedOrderEventJournalProducerV1::
    published_event_sequence() const noexcept {
    return impl_ == nullptr ? 0U
                            : impl_->published_event_sequence();
}

std::uint64_t CertifiedOrderEventJournalProducerV1::
    committed_mapping_bytes() const noexcept {
    return impl_ == nullptr ? 0U
                            : impl_->committed_mapping_bytes();
}

bool CertifiedOrderEventJournalProducerV1::
    MarkStartupPrefixRecovered() noexcept {
    return impl_ != nullptr &&
           impl_->MarkStartupPrefixRecovered();
}

bool CertifiedOrderEventJournalProducerV1::
    FinalizeProcessStartCoverage(
        std::uint64_t coverage_start_unix_ns) noexcept {
    return impl_ != nullptr &&
           impl_->FinalizeProcessStartCoverage(
               coverage_start_unix_ns);
}

std::uint32_t CertifiedOrderEventJournalProducerV1::coverage_flags()
    const noexcept {
    return impl_ == nullptr ? 0U : impl_->coverage_flags();
}

std::uint64_t CertifiedOrderEventJournalProducerV1::
    coverage_start_unix_ns() const noexcept {
    return impl_ == nullptr ? 0U
                            : impl_->coverage_start_unix_ns();
}

bool CertifiedOrderEventJournalProducerV1::failed() const noexcept {
    return impl_ == nullptr || impl_->failed();
}

}  // namespace l2flow::ipc
