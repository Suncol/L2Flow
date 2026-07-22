#pragma once

#include "l2flow/canonical/canonical_bundle_runtime_v1.h"
#include "l2flow/control/control_decoder.h"
#include "l2flow/ingress/raw_live_tail.h"
#include "l2flow/market/instrument_history_v1.h"
#include "l2flow/market/market_decoder.h"
#include "l2flow/market/raw_market_adapter_v1.h"
#include "l2flow/sdk/subscription_manifest.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

namespace l2flow::runtime {

// Owns one exact Canonical family/shard writer.  A source pipeline has one
// such entry for every route required by CanonicalBundleCoordinatorV1.
struct ProductionCanonicalSinkV1 final {
    l2flow::canonical::CanonicalFamilyV1 family =
        l2flow::canonical::CanonicalFamilyV1::kQuality;
    std::uint32_t shard = 0U;
    std::unique_ptr<l2flow::canonical::CanonicalSegmentWriterV1> writer;
};

struct ProductionSourcePipelineConfigV1 final {
    std::uint8_t source_slot = 0U;
    l2flow::sdk::IngressKind ingress_kind =
        l2flow::sdk::IngressKind::ShSnapshot;
    // Raw capture namespace and market business date remain independent.
    std::uint32_t trade_date = 0U;
    // Immutable route generation.  This is not Raw control_generation.
    std::uint64_t source_generation = 0U;
    std::uint64_t canonical_generation = 0U;
    l2flow::common::Sha256Digest normalizer_build_sha256{};
    l2flow::common::Sha256Digest normalizer_config_sha256{};
    // Invoked synchronously while the pipeline execution mutex is held.
    // The hook must not re-enter Step(), MarkFatal(), or an evidence capture
    // on this pipeline.
    l2flow::canonical::CanonicalBundleOperationHookV1 operation_hook =
        nullptr;
    void* operation_hook_context = nullptr;
};

struct ProductionInstrumentRegistryIdentityV1 final {
    std::uint64_t version = 0U;
    l2flow::common::Sha256Digest sha256{};

    [[nodiscard]] friend constexpr bool operator==(
        const ProductionInstrumentRegistryIdentityV1&,
        const ProductionInstrumentRegistryIdentityV1&) noexcept = default;
};

enum class ProductionSourceCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kNullDependency,
    kInvalidConfiguration,
    kSourceIdentityMismatch,
    kFrontierMismatch,
    kControlDecoderMismatch,
    kMarketDecoderInvalid,
    kCanonicalCoordinatorCreateFailed,
    kResourceExhausted,
};

[[nodiscard]] std::string_view ProductionSourceCreateErrorNameV1(
    ProductionSourceCreateErrorV1 error) noexcept;

enum class ProductionSourceFailureV1 : std::uint8_t {
    kNone = 0U,
    kRawTailFailure,
    kRawRecordMissing,
    kRawAdapterFailure,
    kFrontierFailure,
    kControlDecoderFailure,
    kUnexpectedMessage,
    kMarketDecoderFailure,
    kMarketRetainFailure,
    kCanonicalFailure,
    kHistoryEnvelopeFailure,
    kHistorySubmitFailure,
    kHistoryCommitFailure,
    // An unfinished standalone pipeline was destroyed by its owner.
    kLifecycleAbort,
    // This source had no local fault; its aggregate generation was revoked
    // because a peer source or aggregate-owned component failed.
    kCoordinatedFailStop,
    kUnexpectedFailure,
};

[[nodiscard]] std::string_view ProductionSourceFailureNameV1(
    ProductionSourceFailureV1 failure) noexcept;

enum class ProductionSourceStepKindV1 : std::uint8_t {
    kProgress = 0U,
    // The Raw record is already Canonical-committed, but its owned history
    // envelope is retained by the pipeline until a later retry succeeds.
    kBackpressure,
    kWouldBlock,
    kEnd,
    kFatal,
};

struct ProductionSourceStepResultV1 final {
    ProductionSourceStepKindV1 kind =
        ProductionSourceStepKindV1::kWouldBlock;
    ProductionSourceFailureV1 failure = ProductionSourceFailureV1::kNone;
    l2flow::ingress::RawLiveTailError raw_tail_error =
        l2flow::ingress::RawLiveTailError::kNone;
    l2flow::market::RawMarketAdapterErrorV1 raw_adapter_error =
        l2flow::market::RawMarketAdapterErrorV1::kNone;
    l2flow::canonical::SourceFrontierErrorV1 frontier_error =
        l2flow::canonical::SourceFrontierErrorV1::kNone;
    l2flow::control::ControlProcessErrorV1 control_error =
        l2flow::control::ControlProcessErrorV1::kNone;
    l2flow::market::MarketDecodeErrorV1 decode_error =
        l2flow::market::MarketDecodeErrorV1::kNone;
    l2flow::market::RetainedMarketEventCreateErrorV1 retain_error =
        l2flow::market::RetainedMarketEventCreateErrorV1::kNone;
    l2flow::canonical::CanonicalBundleErrorV1 canonical_error =
        l2flow::canonical::CanonicalBundleErrorV1::kNone;
    l2flow::market::OwnedInstrumentEventCreateErrorV1 history_create_error =
        l2flow::market::OwnedInstrumentEventCreateErrorV1::kNone;
    l2flow::market::InstrumentHistorySubmitErrorV1 history_submit_error =
        l2flow::market::InstrumentHistorySubmitErrorV1::kNone;
    l2flow::market::InstrumentHistoryBarrierWaitErrorV1
        history_barrier_error =
            l2flow::market::InstrumentHistoryBarrierWaitErrorV1::kNone;
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t wal_pos = 0U;
};

struct ProductionSourceSnapshotV1 final {
    std::uint64_t raw_records = 0U;
    std::uint64_t market_records = 0U;
    std::uint64_t control_records = 0U;
    std::uint64_t no_output_records = 0U;
    std::uint64_t decode_quality_records = 0U;
    std::uint64_t segment_transitions = 0U;
    std::uint64_t history_submissions = 0U;
    l2flow::market::InstrumentHistorySourceFrontierV1 history_frontier{};
    bool history_pending = false;
    bool history_draining = false;
    bool ended = false;
    bool fatal = false;
    ProductionSourceStepResultV1 terminal{};
};

// One source-local activation sample.  CaptureActivationEvidence() acquires
// the same execution mutex as Step()/MarkFatal() while collecting every
// field, so no source Step can be spliced between these observations.  Raw
// producer/control publication and the history consumer remain independent;
// this structure does not claim a cross-component or cross-source total
// order.
struct ProductionSourceActivationEvidenceV1 final {
    ProductionSourceSnapshotV1 pipeline{};
    l2flow::control::ControlDecoderSnapshotV1 control{};
    l2flow::ingress::RawLiveControlSampleV1 raw_control{};
    l2flow::canonical::SourceFrontierV1 source_frontier{};
    l2flow::canonical::SourceFrontierErrorV1 source_frontier_read_error =
        l2flow::canonical::SourceFrontierErrorV1::kNone;
    l2flow::market::InstrumentHistoryBarrierV1 history_barrier{};
};

// Allocation-free evidence for the post-publication validity monitor.  Like
// activation evidence, it is captured under the source execution mutex, but
// it intentionally omits decoder subscription vectors and history barriers.
struct ProductionSourceActiveValidityEvidenceV1 final {
    l2flow::control::ControlDecoderReadinessSummaryV1 control{};
    l2flow::ingress::RawLiveControlSampleV1 raw_control{};
    l2flow::canonical::SourceFrontierV1 source_frontier{};
    l2flow::canonical::SourceFrontierErrorV1 source_frontier_read_error =
        l2flow::canonical::SourceFrontierErrorV1::kNone;
    l2flow::market::InstrumentHistorySourceFrontierV1 history_frontier{};
    bool pipeline_ended = false;
    bool pipeline_fatal = false;
};

// Per-Step local subset.  It detects decoder disconnect/readiness loss and
// source/history revocation without touching the Raw source.  Raw freshness
// is sampled separately at the aggregate's bounded validation cadence.
struct ProductionSourceActiveLocalEvidenceV1 final {
    l2flow::control::ControlDecoderReadinessSummaryV1 control{};
    l2flow::canonical::SourceFrontierV1 source_frontier{};
    l2flow::canonical::SourceFrontierErrorV1 source_frontier_read_error =
        l2flow::canonical::SourceFrontierErrorV1::kNone;
    l2flow::market::InstrumentHistorySourceFrontierV1 history_frontier{};
    bool pipeline_ended = false;
    bool pipeline_fatal = false;
};

// One authoritative source-order consumer.  Captured/append SourceFrontier
// progress belongs to the SDK callback and the sole Raw writer respectively;
// this derived consumer only verifies append coverage and advances processed.
// It always runs the control decoder before the market decoder, decodes a
// required market message exactly once, commits its Canonical bundle, and
// only then transfers the retained event to the fixed instrument mapping.
// Step() has exactly one caller thread.
//
// RawLiveTail borrows its RawLiveTailSource; that source must outlive this
// object.  source_frontier and history are likewise borrowed.  The instrument
// registry borrowed by the normalizer/market decoder must also outlive this
// object.  The pipeline owns the tail, both decoders, normalizer, Canonical
// writers and coordinator.
//
// A kProgress result after a market record means its history envelope was
// admitted, not necessarily appended by the asynchronous history worker.
// A consumer that combines Canonical output with instrument history must wait
// until Snapshot().history_frontier.acknowledged_source_sequence reaches the
// event's origin ingress sequence (or wait on an exact history barrier).
class ProductionSourcePipelineV1 final {
public:
    ProductionSourcePipelineV1(const ProductionSourcePipelineV1&) = delete;
    ProductionSourcePipelineV1& operator=(
        const ProductionSourcePipelineV1&) = delete;
    ProductionSourcePipelineV1(ProductionSourcePipelineV1&&) = delete;
    ProductionSourcePipelineV1& operator=(
        ProductionSourcePipelineV1&&) = delete;
    ~ProductionSourcePipelineV1();

    [[nodiscard]] static ProductionSourceCreateErrorV1 Create(
        ProductionSourcePipelineConfigV1 config,
        std::unique_ptr<l2flow::ingress::RawLiveTail> live_tail,
        std::unique_ptr<l2flow::control::ControlDecoderV1> control_decoder,
        std::unique_ptr<l2flow::canonical::CanonicalNormalizerV1> normalizer,
        l2flow::canonical::SourceFrontierPageV1* source_frontier,
        std::vector<ProductionCanonicalSinkV1> sinks,
        l2flow::market::InstrumentHistoryRuntimeV1* history,
        std::unique_ptr<ProductionSourcePipelineV1>* output) noexcept;

    [[nodiscard]] ProductionSourceStepResultV1 Step() noexcept;
    [[nodiscard]] ProductionSourceSnapshotV1 Snapshot() const noexcept;
    [[nodiscard]] l2flow::control::ControlDecoderSnapshotV1
    ControlSnapshot() const;
    [[nodiscard]] l2flow::canonical::SourceFrontierV1 Frontier() const
        noexcept;

    // May allocate while copying ControlDecoder subscription state and is
    // therefore intentionally not noexcept.  Raw control is always sampled
    // from the source; a failed read never reuses cached evidence.
    [[nodiscard]] ProductionSourceActivationEvidenceV1
    CaptureActivationEvidence() const;

    [[nodiscard]] ProductionSourceActiveValidityEvidenceV1
    CaptureActiveValidityEvidence() const noexcept;

    [[nodiscard]] ProductionSourceActiveLocalEvidenceV1
    CaptureActiveLocalEvidence() const noexcept;

    // Idempotent fail-stop used by the aggregate runtime.  It is serialized
    // with Step(), revokes the SourceFrontier before latching every Canonical
    // sink, and closes the source's direct history query path.
    void MarkFatal(ProductionSourceFailureV1 failure) noexcept;

    [[nodiscard]] const ProductionSourcePipelineConfigV1& config()
        const noexcept;
    [[nodiscard]] const ProductionInstrumentRegistryIdentityV1&
    registry_identity() const noexcept;
    // Borrowed identity only.  The aggregate uses this to prove that all
    // four pipelines submit into the exact history runtime it coordinates.
    [[nodiscard]] l2flow::market::InstrumentHistoryRuntimeV1*
    history_runtime() const noexcept;
    // Borrowed identity only.  ProductionServiceV1 uses pointer equality to
    // prove that the capture callback and source-order decoder publish/read
    // the exact same SourceFrontier page before either runtime starts.
    [[nodiscard]] const l2flow::canonical::SourceFrontierPageV1*
    source_frontier_page() const noexcept;

private:
    class Impl;
    explicit ProductionSourcePipelineV1(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::runtime
