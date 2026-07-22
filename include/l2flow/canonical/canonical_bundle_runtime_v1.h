#pragma once

#include "l2flow/canonical/canonical_normalizer_v1.h"
#include "l2flow/canonical/canonical_segment_v1.h"
#include "l2flow/canonical/source_frontier_v1.h"
#include "l2flow/ingress/raw_live_tail.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

namespace l2flow::canonical {

// A route owns no storage.  The writer and all other borrowed objects in the
// coordinator configuration must outlive the coordinator.
struct CanonicalBundleSinkV1 final {
    CanonicalFamilyV1 family = CanonicalFamilyV1::kQuality;
    std::uint32_t shard = 0U;
    CanonicalSegmentWriterV1* writer = nullptr;
};

enum class CanonicalBundleOperationV1 : std::uint8_t {
    kPublishRecord = 1U,
    kCommitNormalizer = 2U,
    kAdvanceSegmentProgress = 3U,
    kCommitSourceFrontier = 4U,
};

// Optional deterministic fault-injection/telemetry seam.  Returning false
// rejects the operation at the named pre-call point.  It must not throw.
using CanonicalBundleOperationHookV1 = bool (*)(
    void* context,
    CanonicalBundleOperationV1 operation,
    std::size_t ordinal) noexcept;

// SourceFrontier proves only that a cursor is below the append high-water
// mark; it cannot prove that an intermediate (ingress, WAL) pair is an actual
// record boundary, nor bind that cursor to decoded content.  These verifiers
// are therefore mandatory.  They must consult the caller's already ordered
// feeder/validated Raw reader and authenticate the exact next record plus the
// complete immutable envelope supplied to Process*.  Merely repeating the
// component-wise frontier comparison is invalid.  They must not throw.
using CanonicalMarketEnvelopeVerifierV1 = bool (*)(
    void* context,
    const SourceFrontierV1& processed,
    const CanonicalRawContextV1& raw,
    const l2flow::market::MarketMessageViewV1& message) noexcept;

using CanonicalControlEnvelopeVerifierV1 = bool (*)(
    void* context,
    const SourceFrontierV1& processed,
    const CanonicalRawContextV1& raw,
    const l2flow::control::ControlRecordV1& control,
    std::int64_t recv_realtime_ns,
    std::int64_t recv_monotonic_ns) noexcept;

// A decoded Raw record can intentionally produce no Canonical record.  The
// reason is explicit and closed so callers cannot use progress-only commits
// as a generic data-loss escape hatch.
enum class CanonicalNoOutputReasonV1 : std::uint8_t {
    kOptionalMarketMessage = 1U,
    kUnmodeledControlMessage = 2U,
};

using CanonicalNoOutputEnvelopeVerifierV1 = bool (*)(
    void* context,
    const SourceFrontierV1& processed,
    const CanonicalRawContextV1& raw,
    const l2flow::market::MarketMessageViewV1& message,
    CanonicalNoOutputReasonV1 reason) noexcept;

using CanonicalSegmentTransitionVerifierV1 = bool (*)(
    void* context,
    const SourceFrontierV1& processed,
    const l2flow::ingress::RawLiveSegmentTransitionV1& transition) noexcept;

struct CanonicalBundleCoordinatorConfigV1 final {
    CanonicalNormalizerV1* normalizer = nullptr;
    SourceFrontierPageV1* source_frontier = nullptr;
    // Exact route manifest: Snapshot and Tick require one sink for every
    // normalizer shard; Quality and Control require exactly shard zero.
    std::vector<CanonicalBundleSinkV1> sinks;
    // Shared output-generation provenance.  Every family/shard sink must
    // carry these exact values; registry identity is additionally checked
    // against the normalizer's live registry object.
    std::uint64_t canonical_generation = 0U;
    l2flow::common::Sha256Digest normalizer_build_sha256{};
    l2flow::common::Sha256Digest normalizer_config_sha256{};
    // SourceFrontier uses a bounded cross-thread/process progress writer.
    // BUSY is retried across the full decode/commit operation, including the
    // irreversible post-sink processed-frontier commit.  Persistent BUSY
    // beyond this budget revokes the generation.  Must be in (0, 10 s].
    std::chrono::nanoseconds source_frontier_busy_timeout =
        std::chrono::milliseconds{100};
    CanonicalMarketEnvelopeVerifierV1 market_envelope_verifier = nullptr;
    CanonicalControlEnvelopeVerifierV1 control_envelope_verifier = nullptr;
    // Optional at Create for source compatibility with older callers, but
    // mandatory (fail closed) whenever the corresponding Process method is
    // used.
    CanonicalNoOutputEnvelopeVerifierV1 no_output_envelope_verifier = nullptr;
    CanonicalSegmentTransitionVerifierV1
        segment_transition_verifier = nullptr;
    void* envelope_verifier_context = nullptr;
    CanonicalBundleOperationHookV1 operation_hook = nullptr;
    void* operation_hook_context = nullptr;
};

enum class CanonicalBundleErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kResourceExhausted,
    kInvalidFrontier,
    kIdentityMismatch,
    kSourceFatal,
    kDayStartRequired,
    kRawNotNext,
    kRawNotAppended,
    kEnvelopeNotVerified,
    kInvalidNoOutputReason,
    kInvalidSegmentTransition,
    kNormalizePrepareFailed,
    kMissingSink,
    kSinkPreflightFailed,
    kInjectedFailure,
    kSinkPublishFailed,
    kReceiptFailure,
    kNormalizerCommitFailed,
    kSegmentProgressFailed,
    kFrontierCommitFailed,
};

[[nodiscard]] std::string_view CanonicalBundleErrorNameV1(
    CanonicalBundleErrorV1 error) noexcept;

struct CanonicalBundleResultV1 final {
    CanonicalBundleErrorV1 error = CanonicalBundleErrorV1::kNone;
    CanonicalNormalizePrepareResultV1 normalize{};
    CanonicalSegmentErrorV1 segment_error =
        CanonicalSegmentErrorV1::kNone;
    SourceFrontierErrorV1 frontier_error =
        SourceFrontierErrorV1::kNone;
    CanonicalNormalizeCommitErrorV1 commit_error =
        CanonicalNormalizeCommitErrorV1::kNone;
    std::uint32_t published_records = 0U;

    [[nodiscard]] bool ok() const noexcept {
        return error == CanonicalBundleErrorV1::kNone;
    }
};

// Single ordered consumer for already-appended feeder/Raw messages.  This
// class never writes CSV, Raw WAL, callback journals or feeder metadata.  It
// only injects normalized records into caller-provided Canonical mmap sinks
// and commits their logical visibility through SourceFrontier.
class CanonicalBundleCoordinatorV1 final {
public:
    CanonicalBundleCoordinatorV1(
        const CanonicalBundleCoordinatorV1&) = delete;
    CanonicalBundleCoordinatorV1& operator=(
        const CanonicalBundleCoordinatorV1&) = delete;
    CanonicalBundleCoordinatorV1(
        CanonicalBundleCoordinatorV1&&) = delete;
    CanonicalBundleCoordinatorV1& operator=(
        CanonicalBundleCoordinatorV1&&) = delete;
    ~CanonicalBundleCoordinatorV1();

    [[nodiscard]] static CanonicalBundleErrorV1 Create(
        CanonicalBundleCoordinatorConfigV1 config,
        std::unique_ptr<CanonicalBundleCoordinatorV1>* output) noexcept;

    [[nodiscard]] CanonicalBundleResultV1 ProcessMarket(
        const CanonicalRawContextV1& raw,
        const l2flow::market::MarketMessageViewV1& message) noexcept;

    // Same authenticated Raw envelope as ProcessMarket, but canonicalizes an
    // immutable event already decoded once in source order.
    [[nodiscard]] CanonicalBundleResultV1 ProcessDecodedMarket(
        const CanonicalRawContextV1& raw,
        const l2flow::market::MarketMessageViewV1& message,
        const l2flow::market::RetainedMarketEventV1& decoded) noexcept;

    // Records a data-level failure already returned by the one source-order
    // decoder.  The same mandatory market envelope verifier authenticates
    // raw/message; only the normalizer's closed set of recordable decoder
    // errors is accepted.
    [[nodiscard]] CanonicalBundleResultV1 ProcessMarketDecodeFailure(
        const CanonicalRawContextV1& raw,
        const l2flow::market::MarketMessageViewV1& message,
        l2flow::market::MarketDecodeErrorV1 decode_error) noexcept;

    [[nodiscard]] CanonicalBundleResultV1 ProcessControl(
        const CanonicalRawContextV1& raw,
        const l2flow::control::ControlRecordV1& control,
        std::int64_t recv_realtime_ns,
        std::int64_t recv_monotonic_ns) noexcept;

    // Advances every sink cursor and SourceFrontier for an authenticated Raw
    // record that intentionally maps to no Canonical record.  It never
    // allocates event IDs or changes normalizer sequence/phase state.
    [[nodiscard]] CanonicalBundleResultV1 ProcessNoOutput(
        const CanonicalRawContextV1& raw,
        const l2flow::market::MarketMessageViewV1& message,
        CanonicalNoOutputReasonV1 reason) noexcept;

    // Commits the header-only WAL range when RawLiveTail crosses a segment.
    // Processed ingress sequence stays unchanged; processed WAL advances to
    // transition.next_data_begin_wal_pos.
    [[nodiscard]] CanonicalBundleResultV1 ProcessSegmentTransition(
        const l2flow::ingress::RawLiveSegmentTransitionV1& transition)
        noexcept;

private:
    class Impl;
    explicit CanonicalBundleCoordinatorV1(
        std::unique_ptr<Impl> impl) noexcept;
    [[nodiscard]] CanonicalBundleResultV1 ProcessMarketImpl(
        const CanonicalRawContextV1& raw,
        const l2flow::market::MarketMessageViewV1& message,
        const l2flow::market::RetainedMarketEventV1* decoded,
        l2flow::market::MarketDecodeErrorV1 decoded_failure,
        bool use_decoded_failure) noexcept;
    std::unique_ptr<Impl> impl_;
};

enum class CanonicalCommittedReadErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullArgument,
    kInvalidFrontier,
    kSegmentReadFailed,
    kIdentityMismatch,
    kRecordCorrupt,
    kNotCommitted,
};

[[nodiscard]] std::string_view CanonicalCommittedReadErrorNameV1(
    CanonicalCommittedReadErrorV1 error) noexcept;

// Returns a segment-backed view only when the record is covered by an
// acquire-read global processed prefix from the exact Raw writer/generation.
// The view remains physically valid only while reader lives.  Its logical
// authorization is a point-in-time proof, not a lease: callers must tag
// derived state with the generation and discard it if the live SourceFrontier
// later becomes FATAL.
[[nodiscard]] CanonicalCommittedReadErrorV1
ReadCommittedCanonicalRecordV1(
    const SourceFrontierPageV1& frontier_page,
    const CanonicalSegmentReaderV1& reader,
    std::uint64_t record_index,
    std::span<const std::byte>* record,
    SourceFrontierV1* proof) noexcept;

}  // namespace l2flow::canonical
