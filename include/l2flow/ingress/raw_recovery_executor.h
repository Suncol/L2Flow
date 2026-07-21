#pragma once

#include "l2flow/ingress/raw_recovery.h"

#include <cstddef>
#include <cstdint>
#include <span>

namespace l2flow::ingress {

struct RawRecoveryWriteResult final {
    std::size_t bytes_written = 0U;
    int error_number = 0;
};

class RawRecoveryIo {
public:
    virtual ~RawRecoveryIo() = default;

    // True only when this retained recovery view has independently
    // inventoried the namespace and proved that no surviving manifest,
    // index, report, control page, temporary publication, or unknown entry
    // can depend on a journal suffix. Recovery must not accept this fact as
    // an untrusted call-site option.
    [[nodiscard]] virtual bool
    DependentArtifactsAbsentProven() const noexcept = 0;
    // This is false for a bare filesystem recovery session. A production
    // coordinator-backed composition may return true only for the exact
    // currently authorized RESUME_CONNECT or CONSUMED/ACTIVE continuation
    // token represented by plan.r11_orphan.
    [[nodiscard]] virtual bool R11OrphanAdoptionAuthorized(
        const RawRecoveryPlanV1& plan) const noexcept = 0;

    // Synchronizes Raw-root, capture-date and stream-day directories in that
    // order. Return 0 on success, otherwise an errno-style value.
    [[nodiscard]] virtual int SyncParentDirectories() noexcept = 0;
    [[nodiscard]] virtual int TruncateJournal(
        std::uint64_t size) noexcept = 0;
    [[nodiscard]] virtual int SyncJournal() noexcept = 0;
    [[nodiscard]] virtual int TruncateSegment(
        std::uint32_t segment_sequence,
        std::uint64_t size) noexcept = 0;
    [[nodiscard]] virtual int SyncSegment(
        std::uint32_t segment_sequence,
        bool include_allocation_metadata) noexcept = 0;
    [[nodiscard]] virtual RawRecoveryWriteResult
    WriteJournalSome(
        std::uint64_t offset,
        std::span<const std::byte> bytes) noexcept = 0;
};

enum class RawRecoveryExecutionFailureV1 : std::uint8_t {
    kNone = 0U,
    kPlanFatal,
    kDependentArtifactProofMissing,
    kR11OrphanAuthorizationMissing,
    kInvalidPlan,
    kParentDirectorySync,
    kJournalTruncate,
    kJournalSync,
    kSegmentTruncate,
    kSegmentSync,
    kMarkerEncode,
    kJournalWrite,
    kCursorOverflow,
};

struct RawRecoveryExecutionResultV1 final {
    RawRecoveryExecutionFailureV1 failure =
        RawRecoveryExecutionFailureV1::kNone;
    int error_number = 0;
    bool mutated = false;
    bool cursor_publishable = false;
    std::uint64_t retained_journal_size = 0U;
    RawRecoveryCursorV1 recovered_cursor{};
};

// Executes only the bounded mutations described by a successful analysis
// plan. It never creates a segment, manifest, index or control page.
[[nodiscard]] RawRecoveryExecutionResultV1
ExecuteRawRecoveryPlanV1(
    const RawRecoveryPlanV1& plan,
    RawRecoveryIo& io) noexcept;

}  // namespace l2flow::ingress
