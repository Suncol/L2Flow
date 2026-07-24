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

class InstrumentRegistryV1;
class RealtimeHistoryRecordV1;
struct RealtimeHistoryWatermarkV1;

inline constexpr std::size_t kIntradayInstrumentStoreSourceCountV1 = 4U;
inline constexpr std::size_t
    kIntradayInstrumentStoreMaximumChunkRecordsV1 = 64U * 1024U;
inline constexpr std::size_t
    kIntradayInstrumentStoreMaximumBatchRecordsV1 = 1024U * 1024U;

// Disabled preserves the bounded RealtimeHistory V1 behavior exactly.
// Shadow records an independently observable coverage loss but does not make
// the bounded history fatal. Required and Primary fail the owning history
// runtime closed; Primary currently retains the bounded V1 generation only as
// a compatibility view for the V1 factor engine.
enum class IntradayInstrumentStoreModeV1 : std::uint8_t {
    kDisabled = 0U,
    kShadow,
    kRequired,
    kPrimary,
};

[[nodiscard]] std::string_view IntradayInstrumentStoreModeNameV1(
    IntradayInstrumentStoreModeV1 mode) noexcept;

struct IntradayInstrumentStoreConfigV1 final {
    IntradayInstrumentStoreModeV1 mode =
        IntradayInstrumentStoreModeV1::kDisabled;
    // Chunks contain stable record owners. They are append-only and are
    // reclaimed only when the complete trade-day session is released.
    std::size_t chunk_record_capacity = 1024U;
    // Both limits are required whenever mode is enabled. They are global
    // session limits, not per-instrument limits; reaching either one never
    // evicts an older record. The byte limit is one conservative logical
    // budget shared by base index state, allocated chunks, and retained
    // record accounting. It is not a promise about allocator RSS.
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

struct IntradayInstrumentStoreSnapshotV1 final {
    IntradayInstrumentStoreModeV1 mode =
        IntradayInstrumentStoreModeV1::kDisabled;
    std::uint64_t maximum_session_records = 0U;
    std::uint64_t maximum_session_accounted_bytes = 0U;
    std::uint64_t appended_records = 0U;
    std::uint64_t accounted_record_bytes = 0U;
    std::uint64_t allocated_index_bytes = 0U;
    std::uint64_t allocated_chunks = 0U;
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
    [[nodiscard]] IntradayInstrumentStoreModeV1 mode() const noexcept;
    [[nodiscard]] bool coverage_from_open() const noexcept;

    [[nodiscard]] IntradayInstrumentStoreQueryErrorV1 Find(
        std::uint32_t instrument_id,
        IntradayInstrumentSummaryV1* output) const noexcept;

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

    [[nodiscard]] IntradayInstrumentStoreQueryErrorV1
    OpenUniverseCursor(
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
    friend class IntradayUniverseCursorV1::Impl;
};

// One slice is captured by its owning worker only after all four source
// fences for a generation have arrived. It contains lane endpoints and latest
// locators, never record-handle copies.
class IntradayInstrumentStoreWorkerSliceV1 final {
public:
    IntradayInstrumentStoreWorkerSliceV1(
        const IntradayInstrumentStoreWorkerSliceV1&) = delete;
    IntradayInstrumentStoreWorkerSliceV1& operator=(
        const IntradayInstrumentStoreWorkerSliceV1&) = delete;
    ~IntradayInstrumentStoreWorkerSliceV1();

private:
    class Impl;
    explicit IntradayInstrumentStoreWorkerSliceV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;

    friend class IntradayInstrumentStoreV1;
};

// This component owns no worker threads. RealtimeHistoryRuntimeV1 invokes
// Append and CaptureWorker from the existing permanent instrument owner, so
// each lane has one writer and no append lock. Append is deliberately a
// trusted-runtime boundary: the owning history admission authority must
// provide globally dense/unique ingress sequences and dense/unique
// per-source sequences. The store validates lane monotonicity and generation
// totals but does not create a second global sequencing authority.
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

    // Disabled is represented by the owner not constructing this component;
    // the direct factory accepts enabled modes only.
    [[nodiscard]] static IntradayInstrumentStoreCreateErrorV1 Create(
        IntradayInstrumentStoreConfigV1 config,
        std::uint32_t worker_count,
        const InstrumentRegistryV1* registry,
        std::unique_ptr<IntradayInstrumentStoreV1>* output) noexcept;

    [[nodiscard]] IntradayInstrumentStoreAppendErrorV1 Append(
        std::uint32_t worker,
        std::shared_ptr<const RealtimeHistoryRecordV1> record) noexcept;

    [[nodiscard]] IntradayInstrumentStoreGenerationErrorV1 CaptureWorker(
        std::uint32_t worker,
        std::uint64_t generation,
        std::unique_ptr<IntradayInstrumentStoreWorkerSliceV1>* output)
        noexcept;

    [[nodiscard]] IntradayInstrumentStoreGenerationErrorV1 BuildGeneration(
        const RealtimeHistoryWatermarkV1& watermark,
        std::vector<
            std::unique_ptr<IntradayInstrumentStoreWorkerSliceV1>>
            worker_slices,
        std::shared_ptr<const IntradayInstrumentStoreGenerationV1>* output)
        noexcept;

    void MarkCoverageLost() noexcept;
    [[nodiscard]] IntradayInstrumentStoreSnapshotV1 Snapshot()
        const noexcept;
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

// Conservative record-side logical accounting for one retained object graph
// plus its lane owner. The session byte cap additionally charges base index
// state and whole chunk allocations. Neither value claims to equal allocator
// RSS.
[[nodiscard]] std::uint64_t
EstimateIntradayInstrumentRecordBytesV1(
    const std::shared_ptr<const RealtimeHistoryRecordV1>& record) noexcept;

}  // namespace l2flow::market
