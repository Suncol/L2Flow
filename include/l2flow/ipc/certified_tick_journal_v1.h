#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/ipc/realtime_certified_wire_v1.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <string_view>
#include <type_traits>

namespace l2flow::ipc {

// Independent append-only storage for the complete CERTIFIED Tick prefix.
// Unlike the bounded realtime certified ring, canonical sequence N always
// occupies slot N - 1 and is never overwritten.
inline constexpr std::array<std::uint8_t, 8U>
    kCertifiedTickJournalMagicV1{
        'L', '2', 'F', 'C', 'T', 'J', 'R', '1'};
inline constexpr std::uint16_t
    kCertifiedTickJournalWireMajorV1 = 1U;
inline constexpr std::uint16_t
    kCertifiedTickJournalWireMinorV1 = 1U;
inline constexpr std::uint32_t
    kCertifiedTickJournalEndianMarkerV1 = 0x01020304U;
inline constexpr std::size_t
    kCertifiedTickJournalHeaderBytesV1 = 4096U;
inline constexpr std::size_t
    kCertifiedTickJournalSlotBytesV1 =
        kRealtimeCertifiedTickSlotBytesV1;
inline constexpr std::uint32_t
    kCertifiedTickJournalAlignmentV1 = 64U;

enum class CertifiedTickJournalStateV1 : std::uint32_t {
    kActive = 1U,
    kComplete = 2U,
    kFailed = 3U,
};

// CERTIFIED V1 proves exchange-native continuity from the documented origin
// sequence and therefore exposes only FROM_OPEN coverage. Keeping this on the
// journal itself lets direct C/C++ readers verify temporal coverage without
// borrowing metadata from the FAST control mapping.
enum class CertifiedTickJournalCoverageKindV1 : std::uint32_t {
    kFromOpen = 1U,
};

struct alignas(4096) CertifiedTickJournalHeaderV1 final {
    std::array<std::uint8_t, 8U> magic{};
    std::uint16_t abi_major = 0U;
    std::uint16_t abi_minor = 0U;
    std::uint32_t header_bytes = 0U;
    std::uint32_t endian_marker = 0U;
    std::uint32_t flags = 0U;
    std::uint64_t total_mapping_bytes = 0U;

    std::array<std::uint8_t, 16U> run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
    std::uint32_t reserved_identity = 0U;

    std::uint64_t slots_offset = 0U;
    std::uint64_t tick_capacity = 0U;
    std::uint32_t slot_stride = 0U;
    std::uint32_t region_alignment = 0U;

    // status_publish_tag is a seqcount. The fields through failure_code are
    // one coherent status cut. Static identity/layout and reserved bytes do
    // not change after the descriptor becomes available.
    std::uint64_t status_publish_tag = 0U;
    std::uint64_t heartbeat_monotonic_ns = 0U;
    std::uint64_t canonical_apply_frontier = 0U;
    std::uint64_t generation = 0U;
    std::uint64_t published_tick_count = 0U;
    std::uint64_t committed_mapping_bytes = 0U;
    std::uint32_t state = 0U;
    std::uint32_t failure_code = 0U;

    std::uint32_t coverage_kind = 0U;
    std::uint32_t reserved_coverage = 0U;
    // FROM_OPEN has no process-start boundary; this must remain zero.
    std::uint64_t coverage_start_unix_ns = 0U;
    std::array<std::uint8_t, 3936U> reserved{};
};
static_assert(
    sizeof(CertifiedTickJournalHeaderV1) ==
    kCertifiedTickJournalHeaderBytesV1);
static_assert(alignof(CertifiedTickJournalHeaderV1) == 4096U);
static_assert(
    std::is_standard_layout_v<CertifiedTickJournalHeaderV1>);
static_assert(
    std::is_trivially_copyable_v<CertifiedTickJournalHeaderV1>);
static_assert(
    offsetof(CertifiedTickJournalHeaderV1, slots_offset) == 64U);
static_assert(
    offsetof(
        CertifiedTickJournalHeaderV1,
        status_publish_tag) == 88U);
static_assert(
    offsetof(CertifiedTickJournalHeaderV1, coverage_kind) == 144U);
static_assert(
    offsetof(CertifiedTickJournalHeaderV1, reserved) == 160U);

struct CertifiedTickJournalConfigV1 final {
    common::Identity128 run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
    std::uint64_t tick_capacity = 0U;
    // Zero derives the exact checked mapping bound from tick_capacity.
    std::uint64_t maximum_mapping_bytes = 0U;
    // Physical pages are reserved only as publication approaches a chunk.
    std::uint64_t lazy_commit_chunk_bytes =
        64ULL * 1024ULL * 1024ULL;
};

struct CertifiedTickJournalSessionV1 final {
    common::Identity128 run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
    std::uint64_t tick_capacity = 0U;
    std::uint64_t total_mapping_bytes = 0U;
    CertifiedTickJournalCoverageKindV1 coverage_kind =
        CertifiedTickJournalCoverageKindV1::kFromOpen;
    std::uint64_t coverage_start_unix_ns = 0U;

    [[nodiscard]] friend bool operator==(
        const CertifiedTickJournalSessionV1&,
        const CertifiedTickJournalSessionV1&) noexcept = default;
};

enum class CertifiedTickJournalCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kLayoutOverflow,
    kMappingCreateFailed,
    kReadOnlyHandleFailed,
    kSealFailed,
    kResourceExhausted,
    kUnexpectedFailure,
};

enum class CertifiedTickJournalAppendErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kCanonicalSequence,
    kTickCapacity,
    kBackingCommitFailed,
    kEnvelopeInvalid,
    kPublicationInvariant,
    // The owning CERTIFIED worker froze for a terminal reason outside this
    // journal (for example native conflict or another projection failure).
    kUpstreamFailed,
    kStopped,
    kFailed,
    // The independent History writer fell behind the bounded CERTIFIED ring
    // and the next canonical source slot was overwritten. Bounded
    // CERTIFIED remains healthy; only the append-only History suffix is lost.
    kSourceRetentionLost,
    // The owning CERTIFIED session stopped with an unresolved native gap, so
    // its last-good prefix must not be represented as a complete stream.
    kIncompleteNativePrefix,
    // A supposedly retained, publicly committed bounded slot could not be
    // decoded consistently by the independent History writer.
    kSourceReadFailed,
};

enum class CertifiedTickJournalOpenErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidArgument,
    kUnsupportedEndian,
    kDescriptorInvalid,
    kMappingFailed,
    kLayoutInvalid,
    kSessionMismatch,
    kResourceExhausted,
    kUnexpectedFailure,
};

enum class CertifiedTickJournalReadResultV1 : std::uint8_t {
    kOk = 0U,
    kInvalidArgument,
    kNotYetPublished,
    kEndOfStream,
    kProducerFailed,
    kOutOfRange,
    kInconsistent,
    kCorrupt,
};

[[nodiscard]] std::string_view
CertifiedTickJournalCreateErrorNameV1(
    CertifiedTickJournalCreateErrorV1 error) noexcept;
[[nodiscard]] std::string_view
CertifiedTickJournalAppendErrorNameV1(
    CertifiedTickJournalAppendErrorV1 error) noexcept;
[[nodiscard]] std::string_view
CertifiedTickJournalOpenErrorNameV1(
    CertifiedTickJournalOpenErrorV1 error) noexcept;
[[nodiscard]] std::string_view
CertifiedTickJournalReadResultNameV1(
    CertifiedTickJournalReadResultV1 result) noexcept;

struct CertifiedTickJournalStatusV1 final {
    std::uint64_t publish_tag = 0U;
    std::uint64_t heartbeat_monotonic_ns = 0U;
    std::uint64_t canonical_apply_frontier = 0U;
    std::uint64_t generation = 0U;
    std::uint64_t published_tick_count = 0U;
    std::uint64_t committed_mapping_bytes = 0U;
    CertifiedTickJournalStateV1 state =
        CertifiedTickJournalStateV1::kActive;
    CertifiedTickJournalAppendErrorV1 failure =
        CertifiedTickJournalAppendErrorV1::kNone;
};

struct CertifiedTickJournalReadBatchV1 final {
    std::size_t written = 0U;
    std::uint64_t next_canonical_apply_sequence = 0U;
    CertifiedTickJournalStatusV1 status{};
};

namespace certified_tick_journal_v1_detail {

template <typename T, std::size_t Size>
[[nodiscard]] constexpr bool AllZero(
    const std::array<T, Size>& values) noexcept {
    return std::all_of(
        values.begin(), values.end(), [](T value) noexcept {
            return value == T{};
        });
}

[[nodiscard]] constexpr bool StateValid(
    std::uint32_t value) noexcept {
    return value >= static_cast<std::uint32_t>(
                        CertifiedTickJournalStateV1::kActive) &&
           value <= static_cast<std::uint32_t>(
                        CertifiedTickJournalStateV1::kFailed);
}

[[nodiscard]] constexpr bool AppendErrorValid(
    std::uint32_t value) noexcept {
    return value <= static_cast<std::uint32_t>(
                        CertifiedTickJournalAppendErrorV1::
                            kSourceReadFailed);
}

[[nodiscard]] constexpr bool CheckedAlignUp(
    std::uint64_t value,
    std::uint64_t alignment,
    std::uint64_t* output) noexcept {
    if (output == nullptr || alignment == 0U ||
        (alignment & (alignment - 1U)) != 0U ||
        value > std::numeric_limits<std::uint64_t>::max() -
                    (alignment - 1U)) {
        return false;
    }
    *output = (value + alignment - 1U) & ~(alignment - 1U);
    return true;
}

[[nodiscard]] constexpr bool CheckedLayoutBytes(
    std::uint64_t tick_capacity,
    std::uint64_t* output) noexcept {
    std::uint64_t slot_bytes = 0U;
    std::uint64_t logical_end = 0U;
    return realtime_certified_wire_v1_detail::CheckedMultiply(
               tick_capacity,
               kCertifiedTickJournalSlotBytesV1,
               &slot_bytes) &&
           realtime_certified_wire_v1_detail::CheckedAdd(
               kCertifiedTickJournalHeaderBytesV1,
               slot_bytes,
               &logical_end) &&
           CheckedAlignUp(logical_end, 4096U, output);
}

[[nodiscard]] constexpr bool StatusCanonical(
    const CertifiedTickJournalStatusV1& status,
    std::uint64_t tick_capacity,
    std::uint64_t total_mapping_bytes) noexcept {
    if (status.publish_tag == 0U ||
        (status.publish_tag & 1U) != 0U ||
        status.heartbeat_monotonic_ns == 0U ||
        status.canonical_apply_frontier > tick_capacity ||
        status.generation != status.canonical_apply_frontier ||
        status.published_tick_count !=
            status.canonical_apply_frontier ||
        status.committed_mapping_bytes <
            kCertifiedTickJournalHeaderBytesV1 ||
        status.committed_mapping_bytes > total_mapping_bytes ||
        status.committed_mapping_bytes % 4096U != 0U) {
        return false;
    }
    const auto state = status.state;
    if (state == CertifiedTickJournalStateV1::kFailed) {
        return status.failure !=
                   CertifiedTickJournalAppendErrorV1::kNone &&
               status.failure !=
                   CertifiedTickJournalAppendErrorV1::kStopped;
    }
    return status.failure ==
           CertifiedTickJournalAppendErrorV1::kNone;
}

}  // namespace certified_tick_journal_v1_detail

// Validate a stable, process-local header copy. Live shared status fields
// must first be acquired through the journal reader's seqcount protocol.
[[nodiscard]] constexpr bool CertifiedTickJournalHeaderCanonicalV1(
    const CertifiedTickJournalHeaderV1& header) noexcept {
    const bool identity_nonzero = std::any_of(
        header.run_id.begin(),
        header.run_id.end(),
        [](std::uint8_t value) noexcept {
            return value != 0U;
        });
    CertifiedTickJournalStatusV1 status{};
    status.publish_tag = header.status_publish_tag;
    status.heartbeat_monotonic_ns =
        header.heartbeat_monotonic_ns;
    status.canonical_apply_frontier =
        header.canonical_apply_frontier;
    status.generation = header.generation;
    status.published_tick_count = header.published_tick_count;
    status.committed_mapping_bytes =
        header.committed_mapping_bytes;
    if (!certified_tick_journal_v1_detail::StateValid(
            header.state) ||
        !certified_tick_journal_v1_detail::AppendErrorValid(
            header.failure_code)) {
        return false;
    }
    status.state =
        static_cast<CertifiedTickJournalStateV1>(header.state);
    status.failure =
        static_cast<CertifiedTickJournalAppendErrorV1>(
            header.failure_code);
    std::uint64_t expected_mapping_bytes = 0U;
    return header.magic == kCertifiedTickJournalMagicV1 &&
           header.abi_major ==
               kCertifiedTickJournalWireMajorV1 &&
           header.abi_minor ==
               kCertifiedTickJournalWireMinorV1 &&
           header.header_bytes ==
               kCertifiedTickJournalHeaderBytesV1 &&
           header.endian_marker ==
               kCertifiedTickJournalEndianMarkerV1 &&
           header.flags == 0U && identity_nonzero &&
           header.session_epoch != 0U &&
           realtime_certified_wire_v1_detail::ValidTradeDate(
               header.trade_date) &&
           header.reserved_identity == 0U &&
           header.slots_offset ==
               kCertifiedTickJournalHeaderBytesV1 &&
           header.tick_capacity != 0U &&
           header.slot_stride ==
               kCertifiedTickJournalSlotBytesV1 &&
           header.region_alignment ==
               kCertifiedTickJournalAlignmentV1 &&
           header.coverage_kind == static_cast<std::uint32_t>(
               CertifiedTickJournalCoverageKindV1::kFromOpen) &&
           header.reserved_coverage == 0U &&
           header.coverage_start_unix_ns == 0U &&
           certified_tick_journal_v1_detail::CheckedLayoutBytes(
               header.tick_capacity,
               &expected_mapping_bytes) &&
           header.total_mapping_bytes == expected_mapping_bytes &&
           certified_tick_journal_v1_detail::AllZero(
               header.reserved) &&
           certified_tick_journal_v1_detail::StatusCanonical(
               status,
               header.tick_capacity,
               header.total_mapping_bytes);
}

// Append is serial-only. A whole input span is physically stabilized before
// one header frontier publication, so a reader observes either the old or the
// complete new dense prefix. Any non-kNone append failure except kStopped or
// an already-failed kFailed permanently fail-closes writes while preserving
// the previously published prefix.
class CertifiedTickJournalProducerV1 final {
public:
    CertifiedTickJournalProducerV1(
        const CertifiedTickJournalProducerV1&) = delete;
    CertifiedTickJournalProducerV1& operator=(
        const CertifiedTickJournalProducerV1&) = delete;
    CertifiedTickJournalProducerV1(
        CertifiedTickJournalProducerV1&&) = delete;
    CertifiedTickJournalProducerV1& operator=(
        CertifiedTickJournalProducerV1&&) = delete;
    ~CertifiedTickJournalProducerV1();

    [[nodiscard]] static CertifiedTickJournalCreateErrorV1 Create(
        CertifiedTickJournalConfigV1 config,
        std::shared_ptr<CertifiedTickJournalProducerV1>* output,
        int* system_error_number = nullptr) noexcept;

    [[nodiscard]] CertifiedTickJournalAppendErrorV1 EnsureWritable(
        std::uint64_t required_tick_count) noexcept;
    [[nodiscard]] CertifiedTickJournalAppendErrorV1 Append(
        std::span<const RealtimeCertifiedTickEnvelopeV1> ticks)
        noexcept;
    [[nodiscard]] CertifiedTickJournalAppendErrorV1 Append(
        const RealtimeCertifiedTickEnvelopeV1& tick) noexcept {
        return Append(std::span(&tick, 1U));
    }

    // Monotonically transitions ACTIVE to COMPLETE. It is idempotent.
    [[nodiscard]] bool Stop() noexcept;
    // Worker-only terminal transition used when the owning CERTIFIED
    // pipeline fails outside Append. The last published prefix remains
    // readable and subsequent tail reads report producer failure.
    [[nodiscard]] bool MarkUpstreamFailed() noexcept;
    // Writer-only terminal transition for failures that are local to the
    // asynchronous History path. Only the three source/incomplete errors are
    // accepted; the last-good dense prefix remains readable.
    [[nodiscard]] bool MarkHistoryFailed(
        CertifiedTickJournalAppendErrorV1 failure) noexcept;

    [[nodiscard]] bool DuplicateReadOnlyDescriptor(
        int* output,
        int* system_error_number = nullptr) const noexcept;
    [[nodiscard]] CertifiedTickJournalSessionV1 session()
        const noexcept;
    [[nodiscard]] CertifiedTickJournalStatusV1 status()
        const noexcept;
    [[nodiscard]] std::uint64_t canonical_apply_frontier()
        const noexcept;
    [[nodiscard]] std::uint64_t committed_mapping_bytes()
        const noexcept;
    [[nodiscard]] bool failed() const noexcept;
    [[nodiscard]] CertifiedTickJournalAppendErrorV1 last_error()
        const noexcept;

private:
    class Impl;
    explicit CertifiedTickJournalProducerV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

// The descriptor is borrowed during Open and remains owned by the caller.
// The reader owns only its read-only mapping. Returned Tick envelopes are
// ordinary copies and remain valid independently of reader/producer lifetime.
class CertifiedTickJournalReaderV1 final {
public:
    CertifiedTickJournalReaderV1(
        const CertifiedTickJournalReaderV1&) = delete;
    CertifiedTickJournalReaderV1& operator=(
        const CertifiedTickJournalReaderV1&) = delete;
    CertifiedTickJournalReaderV1(
        CertifiedTickJournalReaderV1&&) = delete;
    CertifiedTickJournalReaderV1& operator=(
        CertifiedTickJournalReaderV1&&) = delete;
    ~CertifiedTickJournalReaderV1();

    [[nodiscard]] static CertifiedTickJournalOpenErrorV1 Open(
        int descriptor,
        const CertifiedTickJournalSessionV1& expected_session,
        std::unique_ptr<CertifiedTickJournalReaderV1>* output,
        int* system_error_number = nullptr) noexcept;

    [[nodiscard]] CertifiedTickJournalReadResultV1 ReadStatus(
        CertifiedTickJournalStatusV1* output) const noexcept;
    [[nodiscard]] CertifiedTickJournalReadResultV1 ReadOne(
        std::uint64_t canonical_apply_sequence,
        RealtimeCertifiedTickEnvelopeV1* output) const noexcept;
    [[nodiscard]] CertifiedTickJournalReadResultV1 Read(
        std::uint64_t first_canonical_apply_sequence,
        std::span<RealtimeCertifiedTickEnvelopeV1> output,
        CertifiedTickJournalReadBatchV1* result) const noexcept;
    // After an exactly full journal, capacity + 1 is the natural sequential
    // tail cursor. It returns kNotYetPublished while ACTIVE, kEndOfStream when
    // COMPLETE, or kProducerFailed when FAILED; larger cursors remain
    // kOutOfRange. The same rule applies to both fixed-slot bulk surfaces.
    // Copies the exact stable 512-byte wire slots. This is the preferred
    // bulk surface for FFI consumers because rows have one fixed stride and
    // payload_words begins at the wire-defined offset 64.
    [[nodiscard]] CertifiedTickJournalReadResultV1 ReadSlots(
        std::uint64_t first_canonical_apply_sequence,
        std::span<RealtimeCertifiedTickSlotV1> output,
        CertifiedTickJournalReadBatchV1* result) const noexcept;
    // Same rows as ReadSlots, copied to an arbitrary-alignment FFI buffer.
    // output.size() must be a nonzero multiple of 512 bytes.
    [[nodiscard]] CertifiedTickJournalReadResultV1 ReadSlotBytes(
        std::uint64_t first_canonical_apply_sequence,
        std::span<std::byte> output,
        CertifiedTickJournalReadBatchV1* result) const noexcept;

    [[nodiscard]] const CertifiedTickJournalSessionV1& session()
        const noexcept;

private:
    class Impl;
    explicit CertifiedTickJournalReaderV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::ipc
