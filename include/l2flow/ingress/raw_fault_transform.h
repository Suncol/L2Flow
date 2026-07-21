#pragma once

#include "l2flow/ingress/raw_replay.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace l2flow::ingress {

// This tag identifies an owned, typed, in-memory transform plan only. It is
// deliberately not the frozen InjectedRawV1 segment magic and is never
// serialized. ProduceInjectedRawSegmentV1 is the sole plan-to-wire seam.
inline constexpr std::array<std::byte, 8U>
    kInjectedRawUnframedPlanMagic{
        std::byte{'I'},
        std::byte{'R'},
        std::byte{'P'},
        std::byte{'L'},
        std::byte{'A'},
        std::byte{'N'},
        std::byte{'0'},
        std::byte{'0'}};

enum class RawLogicalSelectionAlgorithm : std::uint8_t {
    // A stateless SplitMix64 finalizer.  Each decision is derived from seed,
    // input ordinal, purpose, and lane, so changing one enabled fault does not
    // shift the random stream used by another fault.
    kSplitMix64StatelessV1 = 0U,
};

enum class RawLogicalMutationKind : std::uint8_t {
    kNone = 0U,
    // XOR exactly one deterministically selected vendor-body byte.  The
    // 23-byte vendor head, including its SequenceID, remains byte-identical.
    kVendorBodyByteXor,
};

struct RawLogicalFaultRule final {
    RawLogicalSelectionAlgorithm algorithm =
        RawLogicalSelectionAlgorithm::kSplitMix64StatelessV1;
    std::uint64_t seed = 0U;

    // Zero disables a selection.  Otherwise a stable draw selects the fault
    // when draw % one_in == 0; one therefore selects every input record.
    std::uint64_t drop_one_in = 0U;
    std::uint64_t duplicate_one_in = 0U;
    std::uint64_t mutate_one_in = 0U;

    // Number of additional occurrences when duplicate selection succeeds.
    std::uint32_t duplicate_additional_copies = 0U;

    // Zero or one preserves order.  Values >= 2 shuffle independently inside
    // fixed-size chunks, bounding displacement to reorder_window - 1.
    std::uint32_t reorder_window = 0U;

    RawLogicalMutationKind mutation =
        RawLogicalMutationKind::kNone;
    std::byte mutation_xor_mask{0U};
};

struct RawLogicalFaultLimits final {
    std::uint64_t max_input_records = 1'000'000U;
    std::uint64_t max_output_records = 2'000'000U;
    std::uint64_t max_total_vendor_bytes =
        1'073'741'824U;
    std::uint32_t max_duplicate_additional_copies = 16U;
    std::uint32_t max_reorder_window = 4096U;
};

// These values are provenance for the semantic plan. The pure transform
// accepts any nonzero, non-Raw proposed injected identity; the framing
// producer separately requires the exact frozen InjectedRawV1 schema digest.
// The namespace publisher must generate run_id and synthetic_namespace_id
// with GenerateIdentity128 and check durable registries before this pure
// transform; this function rejects zero, alias, and parent-ID collisions
// visible in its input.
struct InjectedRawPlanIdentity final {
    std::array<std::byte, 8U> plan_magic =
        kInjectedRawUnframedPlanMagic;
    RawV1Identity run_id{};
    RawV1Identity synthetic_namespace_id{};
    RawV1Digest raw_schema_sha256{};
    RawV1Digest injected_schema_identity_sha256{};
    RawV1Digest parent_raw_identity_sha256{};
    RawV1Digest fault_rule_sha256{};
    bool synthetic = true;
};

struct InjectedRawParentLocator final {
    std::uint32_t capture_date = 0U;
    std::uint32_t source_stream_id = 0U;
    RawV1Identity stream_day_id{};
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t record_start_wal_pos = 0U;
    std::uint64_t record_end_wal_pos = 0U;
    // One is the first emitted occurrence; duplicate copies use 2, 3, ...
    // Dropped locators use occurrence one.
    std::uint32_t occurrence = 0U;
};

struct InjectedRawMutationEvidence final {
    RawLogicalMutationKind kind =
        RawLogicalMutationKind::kNone;
    std::uint64_t vendor_body_offset = 0U;
    std::byte before{0U};
    std::byte after{0U};
};

// This is an owned semantic model, not RawRecordHeaderV1 and not wire bytes.
// synthetic_ingress_sequence/capture_meta.ingress_sequence are reassigned
// after all drop, duplicate, and reorder decisions.
struct InjectedRawPlanRecord final {
    std::uint64_t synthetic_ingress_sequence = 0U;
    CaptureMetaV1 capture_meta{};
    RawV1VendorHead vendor_head{};
    std::vector<std::byte> vendor_body;
    InjectedRawParentLocator parent{};
    RawReplayProvenance parent_provenance =
        RawReplayProvenance::kDurable;
    std::optional<InjectedRawMutationEvidence> mutation;
};

struct InjectedRawTransformPlan final {
    InjectedRawPlanIdentity identity{};
    RawLogicalFaultRule rule{};
    std::vector<InjectedRawPlanRecord> records;
    std::vector<InjectedRawParentLocator> dropped_locators;
};

enum class RawLogicalFaultError : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kNullInput,
    kUnknownAlgorithm,
    kUnknownMutation,
    kInvalidRule,
    kInvalidLimits,
    kInvalidPlanIdentity,
    kRawAndInjectedSchemaIdentityAlias,
    kInvalidValidatedInputContext,
    kDuplicateParentLocator,
    kEmptyMutationTarget,
    kArithmeticOverflow,
    kResourceLimitExceeded,
    kResourceExhausted,
};

[[nodiscard]] std::string_view RawLogicalFaultErrorName(
    RawLogicalFaultError error) noexcept;

// Input records contain RawRecordView objects created by the validating Raw
// reader/replay path. The function never accepts unchecked header/body spans
// and returns the owned semantic input to ProduceInjectedRawSegmentV1; it
// never emits either RawV1 or InjectedRawV1 framing itself.
[[nodiscard]] RawLogicalFaultError BuildRawLogicalFaultPlan(
    std::span<const RawReplayRecord> input,
    const InjectedRawPlanIdentity& identity,
    const RawLogicalFaultRule& rule,
    const RawLogicalFaultLimits& limits,
    InjectedRawTransformPlan* output) noexcept;

enum class RawPhysicalSegmentState : std::uint8_t {
    kSealed = 0U,
    kHighestOpen,
};

enum class RawPhysicalRewriteKind : std::uint8_t {
    // Rewrites every byte in the exact half-open range with byte ^ xor_mask.
    // A zero mask is rejected.
    kXorMask = 0U,
};

struct RawPhysicalFaultLimits final {
    std::uint64_t max_source_bytes = 8'589'934'592U;
    std::uint64_t max_rewrite_bytes = 1'048'576U;
};

struct RawPhysicalCorruptionRequest final {
    // The input is immutable.  The fixture first copies it into a new owned
    // vector, then rewrites only the requested range.
    std::shared_ptr<const std::vector<std::byte>> source_bytes;
    RawPhysicalSegmentState segment_state =
        RawPhysicalSegmentState::kHighestOpen;
    // Both are exclusive, record-aligned Raw segment offsets.  logical_end is
    // the end of the complete validating-reader prefix before corruption.
    std::uint64_t durable_end_offset = 0U;
    std::uint64_t logical_end_offset = 0U;
    std::uint64_t rewrite_offset = 0U;
    std::uint64_t rewrite_length = 0U;
    RawPhysicalRewriteKind rewrite_kind =
        RawPhysicalRewriteKind::kXorMask;
    std::byte xor_mask{0U};
};

enum class RawPhysicalOriginalDurability : std::uint8_t {
    kSealedDurable = 0U,
    kHighestOpenDurable,
    kHighestOpenDurableBoundaryOverlap,
    kHighestOpenAppendOnly,
};

enum class RawPhysicalRecoveryExpectation : std::uint8_t {
    kFatalRawCorruption = 0U,
    kTruncatedInvalidTail,
};

enum class RawPhysicalDurableReaderExpectation : std::uint8_t {
    kRejectCorruption = 0U,
    kHiddenByDurableBoundary,
};

struct RawPhysicalCorruptionOracle final {
    std::uint64_t corrupted_begin_offset = 0U;
    std::uint64_t corrupted_end_offset = 0U;
    std::uint64_t durable_end_offset = 0U;
    std::uint64_t original_logical_end_offset = 0U;
    RawPhysicalOriginalDurability original_durability =
        RawPhysicalOriginalDurability::kHighestOpenDurable;
    RawPhysicalRecoveryExpectation recovery_expectation =
        RawPhysicalRecoveryExpectation::kFatalRawCorruption;
    RawPhysicalDurableReaderExpectation
        durable_reader_expectation =
            RawPhysicalDurableReaderExpectation::
                kRejectCorruption;
    // For kTruncatedInvalidTail this is the exact range recovery is expected
    // to discard.  For fatal corruption both values are zero.
    std::uint64_t expected_invalid_tail_begin_offset = 0U;
    std::uint64_t expected_invalid_tail_end_offset = 0U;
    RawReaderError validating_reader_error =
        RawReaderError::kNone;
    RawV1Error validating_codec_error = RawV1Error::kNone;
};

struct RawPhysicalCorruptionFixture final {
    std::shared_ptr<const std::vector<std::byte>> derived_bytes;
    RawPhysicalCorruptionOracle oracle{};
};

enum class RawPhysicalFaultError : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kNullSource,
    kUnknownSegmentState,
    kUnknownRewriteKind,
    kInvalidLimits,
    kInvalidBoundary,
    kInvalidRewriteRange,
    kZeroRewriteMask,
    kOriginalRawInvalid,
    kRewriteDidNotInvalidateObject,
    kOracleMismatch,
    kArithmeticOverflow,
    kResourceLimitExceeded,
    kResourceExhausted,
};

[[nodiscard]] std::string_view RawPhysicalFaultErrorName(
    RawPhysicalFaultError error) noexcept;

// Produces an immutable derived byte vector and verifies its oracle with the
// validating reader.  No encoder or CRC function is called after the rewrite.
[[nodiscard]] RawPhysicalFaultError
CreateRawPhysicalCorruptionFixture(
    const RawPhysicalCorruptionRequest& request,
    const RawPhysicalFaultLimits& limits,
    RawPhysicalCorruptionFixture* output) noexcept;

}  // namespace l2flow::ingress
