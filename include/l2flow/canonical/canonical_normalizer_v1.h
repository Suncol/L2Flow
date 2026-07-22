#pragma once

#include "l2flow/canonical/canonical_schema_v1.h"
#include "l2flow/canonical/sequence_guard_v1.h"
#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/market/market_decoder.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <variant>
#include <vector>

namespace l2flow::market {
class InstrumentRegistryV1;
}

namespace l2flow::control {
struct ControlRecordV1;
}

namespace l2flow::canonical {

enum class CanonicalFamilyV1 : std::uint8_t {
    kSnapshot = 1U,
    kTick = 2U,
    kQuality = 3U,
    kControl = 4U,
};

using CanonicalRecordVariantV1 = std::variant<
    CanonicalTickRecordV1,
    CanonicalSnapshotRecordV1,
    CanonicalQualityRecordV1,
    CanonicalControlRecordV1>;

struct RoutedCanonicalRecordV1 final {
    CanonicalFamilyV1 family = CanonicalFamilyV1::kQuality;
    std::uint32_t shard = 0U;
    CanonicalRecordVariantV1 record{};
};

// Raw namespace and business date are deliberately separate.  A naked WAL
// cursor is not globally meaningful; capture_date/source/stream_day_id plus
// the exact writer generation give origin_wal_end_pos its identity.  The
// connection epoch here is the authoritative value reconstructed by the
// ordered control decoder, never the callback hint.
struct CanonicalRawContextV1 final {
    std::uint32_t capture_date = 0U;
    std::uint32_t trade_date = 0U;
    std::uint32_t source_stream_id = 0U;
    l2flow::common::Identity128 stream_day_id{};
    l2flow::common::Identity128 source_writer_instance{};
    std::uint64_t source_generation = 0U;
    std::uint64_t origin_ingress_sequence = 0U;
    std::uint64_t origin_wal_end_pos = 0U;
    std::uint32_t authoritative_connection_epoch = 0U;
    ClockEpochIdentityV1 clock_epoch{};
    std::uint64_t upstream_quality_flags = 0U;
};

struct CanonicalSegmentContextV1 final {
    std::uint32_t capture_date = 0U;
    std::uint32_t trade_date = 0U;
    std::uint32_t source_stream_id = 0U;
    l2flow::common::Identity128 stream_day_id{};
    ClockEpochIdentityV1 clock_epoch{};
};

enum class InitialSequencePolicyV1 : std::uint8_t {
    // The caller explicitly accepts that capture began inside an existing
    // sequence.  The first accepted event carries START_UNKNOWN.
    kAllowUnknownFirst = 1U,
    // The caller supplies an authoritative first sequence.  Any later first
    // observation is an exact leading gap; an earlier one is backward.
    kRequireConfiguredFirst = 2U,
};

struct CanonicalSequencePolicyV1 final {
    // Zero is invalid.  Persist/hash this external policy version with the
    // normalizer configuration; this type intentionally contains no
    // reconnect/session reset switch.
    std::uint32_t policy_version = 0U;
    InitialSequencePolicyV1 vendor_initial =
        InitialSequencePolicyV1::kAllowUnknownFirst;
    InitialSequencePolicyV1 exchange_initial =
        InitialSequencePolicyV1::kAllowUnknownFirst;
    struct ExpectedFirstV1 final {
        SequenceScopeKeyV1 scope{};
        std::uint64_t sequence = 0U;
    };
    // Required-first values are keyed by the exact all-day scope.  A single
    // scalar cannot correctly describe independent vendor message families
    // or exchange channels.  Missing required entries fail Prepare closed.
    std::vector<ExpectedFirstV1> expected_first_by_scope;
};

struct CanonicalNormalizerConfigV1 final {
    std::uint32_t capture_date = 0U;
    std::uint32_t trade_date = 0U;
    std::uint32_t source_stream_id = 0U;
    l2flow::common::Identity128 stream_day_id{};
    std::uint32_t shard_count = 16U;
    // A Phase-4 immutable registry is required because Canonical market
    // records may not invent runtime IDs for unknown instruments.
    const l2flow::market::InstrumentRegistryV1* instrument_registry = nullptr;
    l2flow::market::MarketDecoderLimitsV1 decoder_limits{};
    CanonicalSequencePolicyV1 sequence_policy{};
    std::uint64_t maximum_sequence_scopes = 65'536U;
    std::uint64_t maximum_seen_entries_per_scope = 10'000'000U;
    std::uint64_t maximum_seen_payload_bytes_per_scope =
        2U * 1024U * 1024U * 1024U;
    std::uint64_t maximum_phase_products = 100'000U;
};

enum class CanonicalNormalizerCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kResourceExhausted,
};

enum class CanonicalNormalizePrepareErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullTransaction,
    kTransactionStillActive,
    kNormalizerBusy,
    kNormalizerFatal,
    kInvalidRawContext,
    kInvalidMarketView,
    kInvalidDecodedEvent,
    kInvalidDecodedFailure,
    kControlAdapterFailure,
    kSequencePolicyMissing,
    kScopeCapacity,
    kSequenceGuardFailure,
    kDecodeFailureRecorded,
    kCanonicalValidationFailure,
    kEventIdExhausted,
    kResourceExhausted,
    kUnexpectedFailure,
};

enum class CanonicalNormalizeCommitErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidTransaction,
    kWrongNormalizer,
    kVersionMismatch,
    kPublicationMismatch,
    kSequenceCommitFailure,
    kNormalizerFatal,
};

[[nodiscard]] std::string_view CanonicalNormalizerCreateErrorNameV1(
    CanonicalNormalizerCreateErrorV1 error) noexcept;
[[nodiscard]] std::string_view CanonicalNormalizePrepareErrorNameV1(
    CanonicalNormalizePrepareErrorV1 error) noexcept;
[[nodiscard]] std::string_view CanonicalNormalizeCommitErrorNameV1(
    CanonicalNormalizeCommitErrorV1 error) noexcept;

struct CanonicalPublicationContractV1 final {
    std::uint32_t record_count = 0U;
    l2flow::common::Sha256Digest ordered_records_sha256{};

    [[nodiscard]] friend constexpr bool operator==(
        const CanonicalPublicationContractV1&,
        const CanonicalPublicationContractV1&) noexcept = default;
};

// Independently recomputes the exact ordered bundle receipt from routed
// records.  Runtime publishers use this after every sink publication has
// succeeded; they must not merely echo the transaction's expected receipt.
[[nodiscard]] bool ComputeCanonicalPublicationContractV1(
    std::span<const RoutedCanonicalRecordV1> records,
    CanonicalPublicationContractV1* contract) noexcept;

struct CanonicalNormalizePrepareResultV1 final {
    CanonicalNormalizePrepareErrorV1 error =
        CanonicalNormalizePrepareErrorV1::kNone;
    l2flow::market::MarketDecodeErrorV1 decode_error =
        l2flow::market::MarketDecodeErrorV1::kNone;
    SequenceGuardOutcomeV1 vendor_outcome =
        SequenceGuardOutcomeV1::kPoisoned;
    SequenceGuardOutcomeV1 exchange_outcome =
        SequenceGuardOutcomeV1::kPoisoned;
    bool has_vendor_outcome = false;
    bool has_exchange_outcome = false;
    bool business_record_planned = false;
    std::uint32_t record_count = 0U;
    bool transaction_prepared = false;

    [[nodiscard]] bool ok() const noexcept {
        return error == CanonicalNormalizePrepareErrorV1::kNone ||
               error == CanonicalNormalizePrepareErrorV1::
                   kDecodeFailureRecorded;
    }
};

class CanonicalNormalizationTransactionV1 final {
public:
    CanonicalNormalizationTransactionV1() noexcept;
    CanonicalNormalizationTransactionV1(
        const CanonicalNormalizationTransactionV1&) = delete;
    CanonicalNormalizationTransactionV1& operator=(
        const CanonicalNormalizationTransactionV1&) = delete;
    CanonicalNormalizationTransactionV1(
        CanonicalNormalizationTransactionV1&&) noexcept;
    CanonicalNormalizationTransactionV1& operator=(
        CanonicalNormalizationTransactionV1&&) noexcept;
    ~CanonicalNormalizationTransactionV1();

    [[nodiscard]] bool active() const noexcept;
    [[nodiscard]] std::span<const RoutedCanonicalRecordV1>
    records() const noexcept;
    [[nodiscard]] CanonicalPublicationContractV1
    publication_contract() const noexcept;
    [[nodiscard]] CanonicalSegmentContextV1
    segment_context() const noexcept;

private:
    friend class CanonicalNormalizerV1;
    class Impl;
    std::unique_ptr<Impl> impl_;
};

struct CanonicalNormalizerSnapshotV1 final {
    std::uint64_t version = 0U;
    std::uint64_t vendor_scope_count = 0U;
    std::uint64_t exchange_scope_count = 0U;
    std::uint64_t phase_product_count = 0U;
    std::uint64_t snapshot_records_committed = 0U;
    std::uint64_t tick_records_committed = 0U;
    std::uint64_t quality_records_committed = 0U;
    std::uint64_t control_records_committed = 0U;
    bool transaction_active = false;
    bool fatal = false;
};

// Returns instrument_id % shard_count.  Zero IDs and zero shard counts are
// rejected rather than silently routing unknown instruments to shard zero.
[[nodiscard]] bool CanonicalShardForInstrumentV1(
    std::uint32_t instrument_id,
    std::uint32_t shard_count,
    std::uint32_t* shard) noexcept;

// Single writer.  Sequence guards and SH phase attribution are all-day state;
// connection, subscription and clock-epoch changes never recreate them.
class CanonicalNormalizerV1 final {
public:
    CanonicalNormalizerV1(const CanonicalNormalizerV1&) = delete;
    CanonicalNormalizerV1& operator=(const CanonicalNormalizerV1&) = delete;
    CanonicalNormalizerV1(CanonicalNormalizerV1&&) = delete;
    CanonicalNormalizerV1& operator=(CanonicalNormalizerV1&&) = delete;
    ~CanonicalNormalizerV1();

    [[nodiscard]] static CanonicalNormalizerCreateErrorV1 Create(
        CanonicalNormalizerConfigV1 config,
        std::unique_ptr<CanonicalNormalizerV1>* output) noexcept;

    [[nodiscard]] CanonicalNormalizePrepareResultV1 Prepare(
        const CanonicalRawContextV1& raw,
        const l2flow::market::MarketMessageViewV1& message,
        CanonicalNormalizationTransactionV1* transaction) noexcept;

    // Production callers decode exactly once in source order, retain the
    // immutable event for instrument fan-out/history, and pass that same
    // event here.  `message` remains mandatory because Raw-envelope
    // authentication and vendor duplicate evidence cover the exact wire
    // bytes; this overload never invokes the internal decoder.
    [[nodiscard]] CanonicalNormalizePrepareResultV1 PrepareDecoded(
        const CanonicalRawContextV1& raw,
        const l2flow::market::MarketMessageViewV1& message,
        const l2flow::market::RetainedMarketEventV1& decoded,
        CanonicalNormalizationTransactionV1* transaction) noexcept;

    // Completes the single-decode path when the source-ordered decoder
    // rejected a recognized market message for a recordable data error.
    // Unsupported messages use the coordinator's explicit no-output path;
    // operational/programming failures are rejected rather than converted
    // into durable data-quality records.
    [[nodiscard]] CanonicalNormalizePrepareResultV1 PrepareDecodedFailure(
        const CanonicalRawContextV1& raw,
        const l2flow::market::MarketMessageViewV1& message,
        l2flow::market::MarketDecodeErrorV1 decode_error,
        CanonicalNormalizationTransactionV1* transaction) noexcept;

    // Control records join the same per-family event-ID transaction and the
    // same SourceFrontier commit path as market records.  Runtime callers must
    // not allocate shard_event_id through the low-level adapter themselves.
    [[nodiscard]] CanonicalNormalizePrepareResultV1 PrepareControl(
        const CanonicalRawContextV1& raw,
        const l2flow::control::ControlRecordV1& control,
        std::int64_t recv_realtime_ns,
        std::int64_t recv_monotonic_ns,
        CanonicalNormalizationTransactionV1* transaction) noexcept;

    // The receipt is the exact count/hash contract returned by Prepare.  It
    // is supplied only after every routed sink has published its record.  A
    // partial cross-family publish is not abortable: the caller must discard
    // the whole open generation and replay from the last sealed generation.
    [[nodiscard]] CanonicalNormalizeCommitErrorV1 CommitPublished(
        CanonicalNormalizationTransactionV1* transaction,
        const CanonicalPublicationContractV1& receipt) noexcept;

    // Valid only before any record in the plan has been published.
    [[nodiscard]] bool Abort(
        CanonicalNormalizationTransactionV1* transaction) noexcept;

    // Enters the irreversible generation-fatal state.  If transaction is an
    // active transaction owned by this normalizer, its uncommitted sequence
    // tokens are detached while the generation remains non-reusable.  A null
    // transaction is valid after CommitPublished when a later segment/frontier
    // publication fails.
    [[nodiscard]] bool FailStop(
        CanonicalNormalizationTransactionV1* transaction) noexcept;

    [[nodiscard]] CanonicalNormalizerSnapshotV1 Snapshot() const noexcept;
    [[nodiscard]] const CanonicalNormalizerConfigV1& config() const noexcept;

private:
    class Impl;
    explicit CanonicalNormalizerV1(std::unique_ptr<Impl> impl) noexcept;
    [[nodiscard]] CanonicalNormalizePrepareResultV1 PrepareImpl(
        const CanonicalRawContextV1& raw,
        const l2flow::market::MarketMessageViewV1& message,
        const l2flow::market::RetainedMarketEventV1* decoded,
        l2flow::market::MarketDecodeErrorV1 decoded_failure,
        bool use_decoded_failure,
        CanonicalNormalizationTransactionV1* transaction) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::canonical
