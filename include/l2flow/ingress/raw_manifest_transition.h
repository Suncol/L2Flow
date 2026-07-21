#pragma once

#include "l2flow/ingress/raw_manifest_v1.h"
#include "l2flow/ingress/raw_segment_artifacts.h"
#include "l2flow/ingress/raw_wal_writer.h"

#include <cstdint>
#include <span>
#include <string>
#include <string_view>

namespace l2flow::ingress {

inline constexpr std::uint64_t
    kRawManifestInitialGenerationV1 = 1U;

enum class RawManifestTransitionErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidSegmentHeader,
    kFinalizationContinuationUnsupported,
    kInvalidInitializedSnapshot,
    kInvalidSealedMetadata,
    kInvalidPreviousManifest,
    kMissingOpenEntry,
    kUnexpectedOpenEntry,
    kSegmentIdentityMismatch,
    kGenerationOverflow,
    kClosedEntryCountOverflow,
    kRecoveredMetadataOrder,
    kRecoveredClosedPrefixConflict,
    kRecoveredOpenConflict,
    kManifestValidation,
    kAllocationFailure,
};

[[nodiscard]] std::string_view
RawManifestTransitionErrorV1Name(
    RawManifestTransitionErrorV1 error) noexcept;

// Constructs the exact initial open entry for a normal header-only segment.
// The segment digest is SHA-256 over the canonical 4096-byte SegmentHeaderV1
// wire. The accepted marker is reconstructed from the coherent initialized
// writer snapshot and encoded with marker_flags=0. Output changes only on
// success.
[[nodiscard]] RawManifestTransitionErrorV1
BuildOpenRawManifestEntryV1(
    const SegmentHeaderV1& segment,
    const RawWalWriterSnapshot& initialized_snapshot,
    RawManifestSegmentEntryV1* output) noexcept;

// The two continuation locators are a pure function of the immutable
// SegmentHeaderV1 grant identities.  Keeping their construction here avoids
// accepting caller-invented paths for a segment whose bytes already bind the
// reserve UUID and finalization cycle.
[[nodiscard]] RawManifestTransitionErrorV1
FinalizationContinuationLocatorsV1(
    const SegmentHeaderV1& segment,
    std::string* maintenance_report_locator,
    std::string* archive_locator) noexcept;

// Constructs the header-only open-manifest barrier required before the first
// record of the one permitted FINALIZATION_CONTINUATION segment.  Unlike the
// normal builder, this requires the exact continuation flag and all three
// immutable grant identities, and installs only the deterministic locators
// returned by FinalizationContinuationLocatorsV1().
[[nodiscard]] RawManifestTransitionErrorV1
BuildOpenFinalizationContinuationRawManifestEntryV1(
    const SegmentHeaderV1& segment,
    const RawWalWriterSnapshot& initialized_snapshot,
    RawManifestSegmentEntryV1* output) noexcept;

// Constructs one normal closed entry from immutable facts produced after the
// segment/index barrier. FINALIZATION_CONTINUATION needs its independently
// derived deterministic locators and is deliberately rejected here.
// Output changes only on success.
[[nodiscard]] RawManifestTransitionErrorV1
BuildClosedRawManifestEntryV1(
    const RawSealedSegmentMetadataV1& metadata,
    RawManifestSegmentEntryV1* output) noexcept;

// Constructs the closed form of the same continuation entry.  The locators
// are re-derived from the sealed header rather than copied from caller data.
[[nodiscard]] RawManifestTransitionErrorV1
BuildClosedFinalizationContinuationRawManifestEntryV1(
    const RawSealedSegmentMetadataV1& metadata,
    RawManifestSegmentEntryV1* output) noexcept;

// Creates generation 1 with no closed entries and one header-only open entry.
// Segment sequence/base/ingress chain validation is delegated to the
// authoritative RawManifestV1 model validator. Output changes only on
// success.
[[nodiscard]] RawManifestTransitionErrorV1
BuildFreshOpenRawManifestV1(
    const SegmentHeaderV1& segment,
    const RawWalWriterSnapshot& initialized_snapshot,
    RawManifestV1* output) noexcept;

// Converts the prior open entry into the next immutable closed entry,
// increments generation/count with checked arithmetic, and recomputes the
// closed frontier. Every previously closed entry remains byte-for-byte
// append-only. Output changes only on success.
[[nodiscard]] RawManifestTransitionErrorV1
TransitionOpenRawManifestToClosedV1(
    const RawManifestV1& previous,
    const RawSealedSegmentMetadataV1& metadata,
    RawManifestV1* output) noexcept;

// Adds the exact next header-only normal open entry to a manifest whose prior
// state has no open entry. Generation advances while the closed frontier
// remains identical. Output changes only on success.
[[nodiscard]] RawManifestTransitionErrorV1
TransitionClosedRawManifestToNextOpenV1(
    const RawManifestV1& previous,
    const SegmentHeaderV1& next_segment,
    const RawWalWriterSnapshot& initialized_snapshot,
    RawManifestV1* output) noexcept;

// Adds the single header-only continuation open entry to a closed manifest.
// This is a separate API so normal rotation can never manufacture the
// emergency-only flag or grant identities.
[[nodiscard]] RawManifestTransitionErrorV1
TransitionClosedRawManifestToFinalizationContinuationOpenV1(
    const RawManifestV1& previous,
    const SegmentHeaderV1& continuation_segment,
    const RawWalWriterSnapshot& initialized_snapshot,
    RawManifestV1* output) noexcept;

// Reconstructs the closed-only manifest for a recovery scan whose complete
// sealed metadata is strictly ordered by segment sequence.
//
// With no existing manifest, generation 1 is constructed from the complete
// metadata set. With an existing manifest, every closed entry must match the
// corresponding rebuilt entry exactly. If the predecessor has an open entry,
// exactly one additional metadata item is allowed and that old open must be
// the immutable identity/non-regressing predecessor of the new closed entry.
// A predecessor without an open entry permits no additional segment. Output
// is changed only on success.
[[nodiscard]] RawManifestTransitionErrorV1
BuildRecoveredClosedRawManifestV1(
    std::span<const RawSealedSegmentMetadataV1>
        sealed_segments,
    const RawManifestV1* existing_manifest,
    RawManifestV1* output) noexcept;

}  // namespace l2flow::ingress
