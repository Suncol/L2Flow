#pragma once

#include "l2flow/ingress/raw_index_v1.h"
#include "l2flow/ingress/raw_reader.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace l2flow::ingress {

inline constexpr std::uint32_t
    kRawSegmentArtifactMaximumSequenceV1 = 99'999'999U;
inline constexpr std::uint64_t
    kRawSegmentArtifactDefaultMaximumBytesV1 =
        UINT64_C(4) * UINT64_C(1024) * UINT64_C(1024) *
        UINT64_C(1024);

// The deterministic names are:
//   segment-00000001.idx
//   .segment-00000001.idx.raw-index.tmp
// and the retained source must still be named:
//   segment-00000001.raw
struct RawSegmentArtifactNamesV1 final {
    std::string segment_name;
    std::string index_name;
    std::string index_temporary_name;
};

struct RawSegmentArtifactOptionsV1 final {
    RawV1Digest expected_raw_schema_sha256{};
    std::uint64_t sample_record_interval =
        kRawIndexV1DefaultRecordInterval;
    std::uint64_t sample_raw_bytes_interval =
        kRawIndexV1DefaultRawBytesInterval;
    // Checked before any file-sized allocation.
    std::uint64_t maximum_segment_bytes =
        kRawSegmentArtifactDefaultMaximumBytesV1;
};

// Publishing an index is causally downstream of both barriers. These are
// explicit caller proofs, not facts inferred from an index or pathname.
struct RawSegmentArtifactCausalProofV1 final {
    bool segment_truncated_and_synced_to_logical_end = false;
    bool accepted_seal_marker_journal_synced = false;
    // These values bind the two completion assertions to this exact plan.
    std::uint64_t synced_segment_logical_end_offset = 0U;
    RawV1Digest synced_segment_sha256{};
    RawV1DurableMarkerWire
        journal_synced_sealed_marker_bytes{};
};

enum class RawSegmentArtifactFailureV1 : std::uint8_t {
    kNone = 0U,
    kInvalidInput,
    kInvalidOptions,
    kNameOverflow,
    kDirectoryUnsafe,
    kSegmentUnsafe,
    kSegmentTooLarge,
    kSegmentRead,
    kSegmentChanged,
    kSegmentPathMismatch,
    kRawScanInvalid,
    kRawSchemaMismatch,
    kSealMarkerInvalid,
    kSealMarkerMismatch,
    kIndexBuild,
    kIndexSelfValidation,
    kAllocationFailure,
    kPlanInvalid,
    kCausalProofMissing,
    kNamespaceInspection,
    kAmbiguousFinalAndTemporary,
    kFinalUnsafe,
    kFinalConflict,
    kTemporaryUnsafe,
    kTemporaryConflict,
    kFinalOpen,
    kTemporaryOpen,
    kTemporaryCreate,
    kArtifactWrite,
    kArtifactSync,
    kArtifactRename,
    kDirectorySync,
    kNameToInodeMismatch,
    kArtifactReadback,
    kArtifactChanged,
};

[[nodiscard]] std::string_view RawSegmentArtifactFailureV1Name(
    RawSegmentArtifactFailureV1 failure) noexcept;

struct RawSegmentArtifactFileInfoV1 final {
    bool regular_file = false;
    bool directory = false;
    std::uint64_t owner_user_id = 0U;
    std::uint32_t permission_bits = 0U;
    std::uint64_t link_count = 0U;
    std::uint64_t size = 0U;
    std::uint64_t device = 0U;
    std::uint64_t inode = 0U;
    std::int64_t modification_seconds = 0;
    std::int64_t modification_nanoseconds = 0;
    std::int64_t change_seconds = 0;
    std::int64_t change_nanoseconds = 0;
    int open_flags = 0;
    int descriptor_flags = 0;

    friend bool operator==(
        const RawSegmentArtifactFileInfoV1&,
        const RawSegmentArtifactFileInfoV1&) = default;
};

struct RawSegmentArtifactIoStepV1 final {
    std::size_t byte_count = 0U;
    int error_number = 0;
};

struct RawSegmentArtifactOpenResultV1 final {
    int descriptor = -1;
    int error_number = 0;
};

// Syscall seam used by both the retained-fd plan loader and the publisher.
// Methods return errno-style values (zero on success). InspectName returns
// ENOENT for an absent name. OpenExisting must use no-follow, nonblocking,
// close-on-exec and no-atime flags; CreateExclusive must additionally use
// O_CREAT|O_EXCL and mode 0600. RenameNoReplace must never overwrite.
class RawSegmentArtifactIoV1 {
public:
    virtual ~RawSegmentArtifactIoV1() = default;

    [[nodiscard]] virtual std::uint64_t
    EffectiveUserId() const noexcept = 0;
    [[nodiscard]] virtual int InspectDescriptor(
        int descriptor,
        RawSegmentArtifactFileInfoV1* info) noexcept = 0;
    [[nodiscard]] virtual int InspectName(
        int directory_descriptor,
        std::string_view name,
        RawSegmentArtifactFileInfoV1* info) noexcept = 0;
    [[nodiscard]] virtual RawSegmentArtifactOpenResultV1
    OpenExisting(
        int directory_descriptor,
        std::string_view name,
        bool writable) noexcept = 0;
    [[nodiscard]] virtual RawSegmentArtifactOpenResultV1
    CreateExclusive(
        int directory_descriptor,
        std::string_view name) noexcept = 0;
    [[nodiscard]] virtual RawSegmentArtifactIoStepV1 ReadSome(
        int descriptor,
        std::uint64_t offset,
        std::span<std::byte> output) noexcept = 0;
    [[nodiscard]] virtual RawSegmentArtifactIoStepV1 WriteSome(
        int descriptor,
        std::uint64_t offset,
        std::span<const std::byte> input) noexcept = 0;
    [[nodiscard]] virtual int SyncFile(
        int descriptor) noexcept = 0;
    [[nodiscard]] virtual int SyncDirectory(
        int directory_descriptor) noexcept = 0;
    [[nodiscard]] virtual int RenameNoReplace(
        int directory_descriptor,
        std::string_view old_name,
        std::string_view new_name) noexcept = 0;
    [[nodiscard]] virtual int NameMatchesDescriptor(
        int directory_descriptor,
        std::string_view name,
        int descriptor) noexcept = 0;
    virtual void Close(int descriptor) noexcept = 0;
};

// Immutable facts handed to RawManifestV1 construction after the index
// publication barrier. No pathname or index can alter these facts.
struct RawSealedSegmentMetadataV1 final {
    SegmentHeaderV1 segment{};
    DurableMarkerV1 accepted_sealed_marker{};
    RawV1DurableMarkerWire accepted_sealed_marker_bytes{};
    RawV1Digest segment_sha256{};
    RawV1Digest index_sha256{};
    std::uint64_t logical_end_offset = 0U;
    std::uint64_t record_count = 0U;
    std::optional<std::uint64_t>
        actual_first_ingress_sequence;
    std::optional<std::uint64_t>
        actual_last_ingress_sequence;
};

struct RawSegmentArtifactPlanV1 final {
    RawSegmentArtifactFailureV1 failure =
        RawSegmentArtifactFailureV1::kNone;
    int error_number = 0;
    std::uint64_t evidence_offset = 0U;
    RawV1Error codec_error = RawV1Error::kNone;
    RawReaderError reader_error = RawReaderError::kNone;
    RawIndexV1Error index_error = RawIndexV1Error::kNone;

    RawSegmentArtifactOptionsV1 options{};
    RawSegmentArtifactNamesV1 names{};
    RawSealedSegmentMetadataV1 metadata{};
    std::vector<std::byte> index_bytes;

    // Set only by PrepareRawSegmentArtifactPlanForFdV1 after a stable,
    // secure, name-bound retained-fd read. Pure planning leaves it false.
    bool retained_segment_fd_bound = false;
    RawSegmentArtifactFileInfoV1 retained_segment_snapshot{};

    [[nodiscard]] bool ok() const noexcept {
        return failure == RawSegmentArtifactFailureV1::kNone;
    }
};

// Pure deterministic plan: validates every Raw record in exact_segment_bytes,
// validates the exact accepted seal marker and schema, hashes exactly
// [0, size), and builds/validates RawIndexV1. It performs no syscall.
[[nodiscard]] RawSegmentArtifactPlanV1
BuildRawSegmentArtifactPlanV1(
    std::shared_ptr<const std::vector<std::byte>>
        exact_segment_bytes,
    const RawV1DurableMarkerWire& accepted_sealed_marker,
    RawSegmentArtifactOptionsV1 options) noexcept;

// Normal R6/R7 path. The caller supplies the segment digest and the exact
// RawIndexBuilderV1 maintained while every complete record was appended.
// Taking the builder (rather than arbitrary index bytes) preserves the
// 4096-record/4-MiB threshold invariant. This function finalizes and
// mechanically validates namespace/range/seal/index binding without reading
// a segment fd. BindRawSegmentArtifactPlanToFdV1 must follow.
[[nodiscard]] RawSegmentArtifactPlanV1
BuildIncrementalRawSegmentArtifactPlanV1(
    RawSealedSegmentMetadataV1 metadata,
    const RawIndexBuilderV1& index_builder,
    RawSegmentArtifactOptionsV1 options) noexcept;

// Binds an intrinsically valid incremental plan to the stable retained final
// segment inode. Only the exact 4096-byte header is read and compared; the
// normal writer path therefore does not rescan a sealed multi-GiB segment.
// The fd remains caller-owned and open through publication.
[[nodiscard]] RawSegmentArtifactPlanV1
BindRawSegmentArtifactPlanToFdV1(
    int stream_directory_fd,
    int sealed_segment_fd,
    RawSegmentArtifactPlanV1 plan,
    RawSegmentArtifactIoV1& io) noexcept;

// Recovery/rebuild path. Securely and stably reads the caller-retained final
// segment fd in full, bounded by options.maximum_segment_bytes, then invokes
// the pure validating scanner. It must not be used as the normal R6/R7 writer
// path. The fd remains caller-owned and open through publication.
[[nodiscard]] RawSegmentArtifactPlanV1
PrepareRawSegmentArtifactPlanForFdV1(
    int stream_directory_fd,
    int sealed_segment_fd,
    const RawV1DurableMarkerWire& accepted_sealed_marker,
    RawSegmentArtifactOptionsV1 options,
    RawSegmentArtifactIoV1& io) noexcept;

// POSIX composition of the same recovery/rebuild planning path. It performs
// only retained-fd/name-bound inspection and reads; it does not create,
// rename, synchronize, truncate, or otherwise mutate an artifact namespace.
[[nodiscard]] RawSegmentArtifactPlanV1
PrepareRawSegmentArtifactPlanForPosixFdV1(
    int stream_directory_fd,
    int sealed_segment_fd,
    const RawV1DurableMarkerWire& accepted_sealed_marker,
    RawSegmentArtifactOptionsV1 options) noexcept;

enum class RawSegmentArtifactExistingStateV1
    : std::uint8_t {
    kAbsent = 0U,
    kMatchingFinal,
    kMatchingCompleteTemporary,
};

struct RawSegmentArtifactInspectionV1 final {
    RawSegmentArtifactFailureV1 failure =
        RawSegmentArtifactFailureV1::kNone;
    int error_number = 0;
    RawSegmentArtifactExistingStateV1 state =
        RawSegmentArtifactExistingStateV1::kAbsent;
    RawSealedSegmentMetadataV1 metadata{};

    [[nodiscard]] bool ok() const noexcept {
        return failure ==
               RawSegmentArtifactFailureV1::kNone;
    }
};

// Read-only POSIX inspection of deterministic final/tmp index candidates
// against one retained-fd-bound rebuild plan. An absent artifact is a
// successful kAbsent result. Unsafe, partial, conflicting, or simultaneous
// final+tmp candidates fail closed. This function never adopts or publishes.
[[nodiscard]] RawSegmentArtifactInspectionV1
InspectExistingRawSegmentArtifactAtV1(
    int stream_directory_fd,
    int sealed_segment_fd,
    const RawSegmentArtifactPlanV1& plan) noexcept;

enum class RawSegmentArtifactDispositionV1 : std::uint8_t {
    kNone = 0U,
    kPublishedNew,
    kAdoptedCompleteTemporary,
    kAcceptedExistingFinal,
};

struct RawSegmentArtifactPublishResultV1 final {
    RawSegmentArtifactFailureV1 failure =
        RawSegmentArtifactFailureV1::kNone;
    int error_number = 0;
    RawSegmentArtifactDispositionV1 disposition =
        RawSegmentArtifactDispositionV1::kNone;
    bool namespace_mutated = false;
    bool directory_synced = false;
    RawSealedSegmentMetadataV1 metadata{};

    [[nodiscard]] bool ok() const noexcept {
        return failure == RawSegmentArtifactFailureV1::kNone;
    }
};

// Publishes only the deterministic RawIndexV1 artifact. A valid complete
// deterministic tmp is adopted; a partial/conflicting tmp is preserved and
// rejected. A pre-existing final is accepted only when causal_proof is
// complete and exact bytes/hash, security metadata, name-to-inode and
// readback all match. No path is overwritten or unlinked.
[[nodiscard]] RawSegmentArtifactPublishResultV1
PublishRawSegmentArtifactPlanV1(
    int stream_directory_fd,
    int sealed_segment_fd,
    const RawSegmentArtifactPlanV1& plan,
    RawSegmentArtifactCausalProofV1 causal_proof,
    RawSegmentArtifactIoV1& io) noexcept;

// POSIX recovery/rebuild composition. Normal R6/R7 uses the incremental plan,
// retained-fd binding and PublishRawSegmentArtifactPlanV1 separately.
[[nodiscard]] RawSegmentArtifactPublishResultV1
BuildAndPublishRawSegmentArtifactAtV1(
    int stream_directory_fd,
    int sealed_segment_fd,
    const RawV1DurableMarkerWire& accepted_sealed_marker,
    RawSegmentArtifactOptionsV1 options,
    RawSegmentArtifactCausalProofV1 causal_proof) noexcept;

// Normal writer-path POSIX composition. Securely opens the retained final
// segment named by an incremental plan, binds only its exact 4096-byte
// header/stable inode metadata, and publishes the deterministic index. It
// never scans record bytes.
[[nodiscard]] RawSegmentArtifactPublishResultV1
BindAndPublishIncrementalRawSegmentArtifactAtV1(
    int stream_directory_fd,
    RawSegmentArtifactPlanV1 plan,
    RawSegmentArtifactCausalProofV1
        causal_proof) noexcept;

}  // namespace l2flow::ingress
