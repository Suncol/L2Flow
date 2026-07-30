#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

namespace l2flow::market {

class DailyInstrumentCatalogSnapshotV2;
class InstrumentRuntimeStateV2;
class RealtimeHistoryEventInputV1;
class RealtimeHistoryRecordV1;
struct RealtimeHistoryWatermarkV1;

inline constexpr std::size_t kIntradayInstrumentStoreSourceCountV1 = 4U;
inline constexpr std::size_t
    kIntradayInstrumentStoreMinimumSegmentBytesV1 = 4U * 1024U;
inline constexpr std::size_t
    kIntradayInstrumentStoreMaximumSegmentBytesV1 =
        16U * 1024U * 1024U;
inline constexpr std::size_t
    kIntradayInstrumentStoreMaximumBatchRecordsV1 = 1024U * 1024U;

struct IntradayInstrumentStoreConfigV1 final {
    // Each instrument/source lane owns append-only fixed-target-byte
    // segments. Record headers grow forward and exact event payloads grow
    // backward. An individual event may require a larger segment, up to the
    // absolute maximum, but existing segments never move.
    std::size_t segment_target_bytes = 64U * 1024U;
    // Both limits are required. They are global session limits, not
    // per-instrument limits; reaching either one never evicts an older record.
    // The byte limit is one conservative logical budget shared by base index
    // state, allocated segments, and retained record accounting. It is not a
    // promise about allocator RSS.
    std::uint64_t maximum_session_records = 0U;
    std::uint64_t maximum_session_accounted_bytes = 0U;
    // A cursor is streaming and may traverse the complete session, but one
    // ReadBatch call cannot exceed this fixed allocation/work boundary.
    std::size_t maximum_records_per_batch = 64U * 1024U;
    // This is an operational assertion, not a fact inferred from sequence 1.
    // It may be true only when the process began before the first market
    // message and has remained continuously healthy.
    bool coverage_from_open = false;
};

// The decoder resolves the daily-catalog ordinal once. The history runtime
// carries this value through its source×worker queue so append does not
// repeat an ID lookup. A token is valid only for the Store session epoch that
// created it.
struct InstrumentRouteTokenV1 final {
    std::uint32_t instrument_id = 0U;
    std::size_t ordinal = std::numeric_limits<std::size_t>::max();
    std::uint32_t worker = std::numeric_limits<std::uint32_t>::max();
    std::size_t worker_local_row = std::numeric_limits<std::size_t>::max();
    std::uint64_t session_epoch = 0U;
};

enum class IntradayInstrumentStoreCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kResourceExhausted,
};

enum class IntradayInstrumentStoreAppendErrorV1 : std::uint8_t {
    kNone = 0U,
    kCoverageLost,
    kInvalidRecord,
    kWrongWorker,
    kSequenceNotIncreasing,
    kRecordCapacity,
    kByteCapacity,
    kResourceExhausted,
};

enum class IntradayInstrumentStoreGenerationErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kCoverageLost,
    kInvalidWorker,
    kInvalidGeneration,
    kInvalidWatermark,
    kIncompleteWorkerSet,
    kResourceExhausted,
};

enum class IntradayInstrumentStoreQueryErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidArgument,
    kNotFound,
    kBatchLimitExceeded,
    kResourceExhausted,
};

[[nodiscard]] std::string_view
IntradayInstrumentStoreCreateErrorNameV1(
    IntradayInstrumentStoreCreateErrorV1 error) noexcept;
[[nodiscard]] std::string_view
IntradayInstrumentStoreAppendErrorNameV1(
    IntradayInstrumentStoreAppendErrorV1 error) noexcept;
[[nodiscard]] std::string_view
IntradayInstrumentStoreGenerationErrorNameV1(
    IntradayInstrumentStoreGenerationErrorV1 error) noexcept;
[[nodiscard]] std::string_view
IntradayInstrumentStoreQueryErrorNameV1(
    IntradayInstrumentStoreQueryErrorV1 error) noexcept;

enum class IntradayInstrumentScanDirectionV1 : std::uint8_t {
    kOldestFirst = 0U,
    kNewestFirst,
};

// Sequence ranges are half-open. UINT64_MAX is a legal exclusive generation
// cut but is never assigned to a message.
struct IntradayInstrumentScanOptionsV1 final {
    std::uint64_t ingress_sequence_begin_inclusive = 1U;
    std::uint64_t ingress_sequence_end_exclusive =
        std::numeric_limits<std::uint64_t>::max();
    std::uint64_t maximum_records =
        std::numeric_limits<std::uint64_t>::max();
    IntradayInstrumentScanDirectionV1 direction =
        IntradayInstrumentScanDirectionV1::kOldestFirst;
};

// Pointers in this summary are borrowed from the matching immutable
// generation. They remain valid while that generation or one of its cursors
// remains alive.
struct IntradayInstrumentSummaryV1 final {
    std::uint32_t instrument_id = 0U;
    std::array<std::uint64_t, kIntradayInstrumentStoreSourceCountV1>
        source_record_counts{};
    std::uint64_t record_count = 0U;
    const RealtimeHistoryRecordV1* latest_snapshot = nullptr;
    const RealtimeHistoryRecordV1* latest_tick = nullptr;
};

// Immutable accounting for one tick-only delta cursor. Source slots 1 and 3
// are selected; snapshot slots 0 and 2 are zero in every count vector.
struct IntradayInstrumentTickDeltaSummaryV1 final {
    std::uint32_t instrument_id = 0U;
    std::array<std::uint8_t, kIntradayInstrumentStoreSourceCountV1>
        selected_source_mask{};
    std::uint64_t ingress_sequence_begin_inclusive = 0U;
    std::uint64_t ingress_sequence_end_exclusive = 0U;
    std::array<std::uint64_t, kIntradayInstrumentStoreSourceCountV1>
        base_tick_source_record_counts{};
    std::array<std::uint64_t, kIntradayInstrumentStoreSourceCountV1>
        target_tick_source_record_counts{};
    std::array<std::uint64_t, kIntradayInstrumentStoreSourceCountV1>
        delta_tick_source_record_counts{};
    std::uint64_t delta_tick_record_count = 0U;
};

struct IntradayInstrumentStoreSnapshotV1 final {
    std::uint64_t maximum_session_records = 0U;
    std::uint64_t maximum_session_accounted_bytes = 0U;
    std::uint64_t appended_records = 0U;
    std::uint64_t accounted_record_bytes = 0U;
    std::uint64_t allocated_index_bytes = 0U;
    std::uint64_t allocated_segments = 0U;
    std::uint64_t failed_appends = 0U;
    std::uint64_t latest_generation = 0U;
    bool coverage_from_open = false;
    bool coverage_lost = false;
};

class IntradayInstrumentCursorV1 final {
public:
    IntradayInstrumentCursorV1(
        const IntradayInstrumentCursorV1&) = delete;
    IntradayInstrumentCursorV1& operator=(
        const IntradayInstrumentCursorV1&) = delete;
    IntradayInstrumentCursorV1(
        IntradayInstrumentCursorV1&&) noexcept;
    IntradayInstrumentCursorV1& operator=(
        IntradayInstrumentCursorV1&&) noexcept;
    ~IntradayInstrumentCursorV1();

    // The caller owns the pointer array. Returned record pointers are borrowed
    // and remain valid for this cursor's lifetime. A successful zero-sized
    // batch means end-of-stream.
    [[nodiscard]] IntradayInstrumentStoreQueryErrorV1 ReadBatch(
        std::span<const RealtimeHistoryRecordV1*> output,
        std::size_t* written) noexcept;
    [[nodiscard]] bool done() const noexcept;

private:
    class Impl;
    explicit IntradayInstrumentCursorV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;

    friend class IntradayInstrumentStoreGenerationV1;
};

class IntradayInstrumentTickDeltaCursorV1 final {
public:
    IntradayInstrumentTickDeltaCursorV1(
        const IntradayInstrumentTickDeltaCursorV1&) = delete;
    IntradayInstrumentTickDeltaCursorV1& operator=(
        const IntradayInstrumentTickDeltaCursorV1&) = delete;
    IntradayInstrumentTickDeltaCursorV1(
        IntradayInstrumentTickDeltaCursorV1&&) noexcept;
    IntradayInstrumentTickDeltaCursorV1& operator=(
        IntradayInstrumentTickDeltaCursorV1&&) noexcept;
    ~IntradayInstrumentTickDeltaCursorV1();

    // Records are emitted oldest-first in process ingress order. Returned
    // pointers are borrowed and remain valid for this cursor's lifetime. A
    // successful zero-sized batch means end-of-stream.
    [[nodiscard]] IntradayInstrumentStoreQueryErrorV1 ReadBatch(
        std::span<const RealtimeHistoryRecordV1*> output,
        std::size_t* written) noexcept;
    [[nodiscard]] bool done() const noexcept;
    [[nodiscard]] const IntradayInstrumentTickDeltaSummaryV1& summary()
        const noexcept;

private:
    class Impl;
    explicit IntradayInstrumentTickDeltaCursorV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;

    friend class IntradayInstrumentStoreGenerationV1;
};

class IntradayUniverseCursorV1 final {
public:
    IntradayUniverseCursorV1(
        const IntradayUniverseCursorV1&) = delete;
    IntradayUniverseCursorV1& operator=(
        const IntradayUniverseCursorV1&) = delete;
    IntradayUniverseCursorV1(
        IntradayUniverseCursorV1&&) noexcept;
    IntradayUniverseCursorV1& operator=(
        IntradayUniverseCursorV1&&) noexcept;
    ~IntradayUniverseCursorV1();

    // Records are emitted in ascending instrument-id order and then in
    // ascending process ingress order within each instrument.
    [[nodiscard]] IntradayInstrumentStoreQueryErrorV1 ReadBatch(
        std::span<const RealtimeHistoryRecordV1*> output,
        std::size_t* written) noexcept;
    [[nodiscard]] bool done() const noexcept;

private:
    class Impl;
    explicit IntradayUniverseCursorV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;

    friend class IntradayInstrumentStoreGenerationV1;
};

class IntradayInstrumentStoreGenerationV1 final {
public:
    IntradayInstrumentStoreGenerationV1(
        const IntradayInstrumentStoreGenerationV1&) = delete;
    IntradayInstrumentStoreGenerationV1& operator=(
        const IntradayInstrumentStoreGenerationV1&) = delete;
    IntradayInstrumentStoreGenerationV1(
        IntradayInstrumentStoreGenerationV1&&) = delete;
    IntradayInstrumentStoreGenerationV1& operator=(
        IntradayInstrumentStoreGenerationV1&&) = delete;
    ~IntradayInstrumentStoreGenerationV1();

    [[nodiscard]] const RealtimeHistoryWatermarkV1& watermark()
        const noexcept;
    [[nodiscard]] std::size_t instrument_count() const noexcept;
    [[nodiscard]] std::uint64_t record_count() const noexcept;
    [[nodiscard]] std::uint64_t accounted_record_bytes() const noexcept;
    [[nodiscard]] std::uint64_t allocated_index_bytes() const noexcept;
    [[nodiscard]] bool coverage_from_open() const noexcept;
    // Process-local provenance of the Store session that owns every retained
    // segment in this generation. It is not the realtime IPC session epoch
    // and must never be serialized as one. Successive generations from the
    // same IntradayInstrumentStoreV1 return the same nonzero value. Distinct
    // successfully created Store sessions receive distinct values for the
    // lifetime of this process; values are never reused. Exhausting this
    // identity space makes IntradayInstrumentStoreV1::Create fail with
    // kResourceExhausted.
    [[nodiscard]] std::uint64_t store_session_epoch() const noexcept;
    // This is the exact CatalogSnapshot carried by the matching watermark,
    // not a later snapshot acquired while the generation was built.
    [[nodiscard]] const std::shared_ptr<
        const DailyInstrumentCatalogSnapshotV2>&
    catalog_snapshot() const noexcept;

    [[nodiscard]] IntradayInstrumentStoreQueryErrorV1 Find(
        std::uint32_t instrument_id,
        IntradayInstrumentSummaryV1* output) const noexcept;
    // Allocation-free O(1) access in ascending instrument-id order.
    // ordinal >= instrument_count() returns kNotFound.
    [[nodiscard]] IntradayInstrumentStoreQueryErrorV1 SummaryAt(
        std::size_t ordinal,
        IntradayInstrumentSummaryV1* output) const noexcept;

    // begin == end is a valid empty half-open range and returns a cursor
    // whose first ReadBatch is the explicit zero-sized terminal batch.
    [[nodiscard]] IntradayInstrumentStoreQueryErrorV1
    OpenInstrumentCursor(
        std::uint32_t instrument_id,
        IntradayInstrumentScanOptionsV1 options,
        std::unique_ptr<IntradayInstrumentCursorV1>* output)
        const noexcept;

    // Convenience for the newest N records. The cursor emits newest-first so
    // it never has to materialize or reverse the complete history.
    [[nodiscard]] IntradayInstrumentStoreQueryErrorV1 OpenTailCursor(
        std::uint32_t instrument_id,
        std::uint64_t count,
        std::unique_ptr<IntradayInstrumentCursorV1>* output)
        const noexcept;

    // Opens the tick-only half-open delta
    // [ingress_sequence_begin_inclusive, watermark().ingress_sequence_exclusive).
    // begin == end is valid and immediately done; begin == 0 or begin > end is
    // invalid. Boundary location starts from each target tick-lane tail.
    [[nodiscard]] IntradayInstrumentStoreQueryErrorV1
    OpenInstrumentTickDeltaCursor(
        std::uint32_t instrument_id,
        std::uint64_t ingress_sequence_begin_inclusive,
        std::unique_ptr<IntradayInstrumentTickDeltaCursorV1>* output)
        const noexcept;

    [[nodiscard]] IntradayInstrumentStoreQueryErrorV1
    OpenUniverseCursor(
        IntradayInstrumentScanOptionsV1 options,
        std::unique_ptr<IntradayUniverseCursorV1>* output)
        const noexcept;
    // Opens one independent cursor over the half-open instrument-ordinal
    // range. Ordinals follow SummaryAt ordering. An empty valid range returns
    // a cursor that is immediately done.
    [[nodiscard]] IntradayInstrumentStoreQueryErrorV1
    OpenUniverseRangeCursor(
        std::size_t ordinal_begin,
        std::size_t ordinal_end_exclusive,
        IntradayInstrumentScanOptionsV1 options,
        std::unique_ptr<IntradayUniverseCursorV1>* output)
        const noexcept;

private:
    class Impl;
    explicit IntradayInstrumentStoreGenerationV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;

    friend class IntradayInstrumentStoreV1;
    friend class IntradayInstrumentCursorV1::Impl;
    friend class IntradayInstrumentTickDeltaCursorV1::Impl;
    friend class IntradayUniverseCursorV1::Impl;
};

// One lightweight slice token is published by its owning worker only after
// all four source fences for a generation have arrived. Publishing the token
// is O(1): endpoint rows are materialized by the background generation
// builder. A row changed after the fence lazily preserves its constant-size
// pre-cut endpoint on that row's first post-cut append.
class IntradayInstrumentStoreWorkerSliceV1 final {
public:
    IntradayInstrumentStoreWorkerSliceV1(
        const IntradayInstrumentStoreWorkerSliceV1&) = delete;
    IntradayInstrumentStoreWorkerSliceV1& operator=(
        const IntradayInstrumentStoreWorkerSliceV1&) = delete;
    ~IntradayInstrumentStoreWorkerSliceV1();

    // Number of catalog rows represented by this worker's token. Summing this
    // value across the complete worker set equals the exact snapshot's
    // bound_count(), never Store capacity. It is not work performed on the
    // marker path.
    [[nodiscard]] std::size_t captured_instrument_count() const noexcept;

private:
    class Impl;
    explicit IntradayInstrumentStoreWorkerSliceV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;

    friend class IntradayInstrumentStoreV1;
};

// This required store is the sole retained session-history authority. It owns
// no worker threads: the routing runtime invokes Append and CaptureWorker from
// each permanent instrument owner, so every lane has one writer. The normal
// append path is lock-free; only the first append to a row after a generation
// fence takes that row's short endpoint-freeze lock. Append is deliberately a
// trusted-runtime boundary: admission must provide globally dense/unique
// ingress sequences and dense/unique per-source sequences. The store validates
// lane monotonicity and generation totals but does not create a second global
// sequencing authority. Any append, capture, or build error means complete
// coverage can no longer be proven and the owner must fail closed.
class IntradayInstrumentStoreV1 final {
public:
    IntradayInstrumentStoreV1(
        const IntradayInstrumentStoreV1&) = delete;
    IntradayInstrumentStoreV1& operator=(
        const IntradayInstrumentStoreV1&) = delete;
    IntradayInstrumentStoreV1(
        IntradayInstrumentStoreV1&&) = delete;
    IntradayInstrumentStoreV1& operator=(
        IntradayInstrumentStoreV1&&) = delete;
    ~IntradayInstrumentStoreV1();

    // A successfully returned instance is always the required session store.
    [[nodiscard]] static IntradayInstrumentStoreCreateErrorV1 Create(
        IntradayInstrumentStoreConfigV1 config,
        std::uint32_t worker_count,
        std::array<std::uint32_t,
                   kIntradayInstrumentStoreSourceCountV1>
            source_stream_ids,
        const InstrumentRuntimeStateV2* runtime_state,
        std::unique_ptr<IntradayInstrumentStoreV1>* output) noexcept;

    [[nodiscard]] IntradayInstrumentStoreQueryErrorV1 ResolveRouteToken(
        std::size_t ordinal,
        std::uint32_t instrument_id,
        InstrumentRouteTokenV1* output) const noexcept;

    [[nodiscard]] IntradayInstrumentStoreAppendErrorV1 Append(
        std::uint32_t worker,
        const InstrumentRouteTokenV1& route,
        RealtimeHistoryEventInputV1&& input) noexcept;
    // On success the optional receipt points at the exact immutable record
    // placement owned by this Store session. The pointer remains valid until
    // the Store is destroyed. It is an in-process handoff only and must never
    // be used as a cross-process ABI. On every failure *appended_record is
    // reset to nullptr.
    [[nodiscard]] IntradayInstrumentStoreAppendErrorV1 Append(
        std::uint32_t worker,
        const InstrumentRouteTokenV1& route,
        RealtimeHistoryEventInputV1&& input,
        const RealtimeHistoryRecordV1** appended_record) noexcept;

    [[nodiscard]] IntradayInstrumentStoreGenerationErrorV1 CaptureWorker(
        std::uint32_t worker,
        std::uint64_t generation,
        const std::shared_ptr<
            const DailyInstrumentCatalogSnapshotV2>& catalog_snapshot,
        std::unique_ptr<IntradayInstrumentStoreWorkerSliceV1>* output)
        noexcept;

    [[nodiscard]] IntradayInstrumentStoreGenerationErrorV1 BuildGeneration(
        const RealtimeHistoryWatermarkV1& watermark,
        std::vector<
            std::unique_ptr<IntradayInstrumentStoreWorkerSliceV1>>
            worker_slices,
        std::shared_ptr<const IntradayInstrumentStoreGenerationV1>* output)
        noexcept;

    // BuildGeneration is intentionally publication-free and may run outside
    // the history commit lock. The history runtime calls PublishGeneration
    // only after reacquiring that lock and proving that the built handle is
    // still the exact healthy generation to publish.
    [[nodiscard]] IntradayInstrumentStoreGenerationErrorV1
    PublishGeneration(
        const std::shared_ptr<
            const IntradayInstrumentStoreGenerationV1>& generation)
        noexcept;

    void MarkCoverageLost() noexcept;
    [[nodiscard]] IntradayInstrumentStoreSnapshotV1 Snapshot()
        const noexcept;
    // One acquire load for latency-sensitive dependent read models.
    [[nodiscard]] bool coverage_lost() const noexcept;
    [[nodiscard]] std::uint32_t WorkerForInstrument(
        std::uint32_t instrument_id) const noexcept;
    [[nodiscard]] const IntradayInstrumentStoreConfigV1& config()
        const noexcept;

private:
    class Impl;
    explicit IntradayInstrumentStoreV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::market
