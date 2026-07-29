#pragma once

#include "l2flow/market/kline_types_v1.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>

namespace l2flow::market {

// Converts only valid Shanghai Tick trade events and valid Shenzhen
// Transaction trade events. It never consults recv_realtime, recv_monotonic,
// SDK LocalTime, or a local clock.
[[nodiscard]] KLineTradeProjectionV1 ProjectKLineTradeV1(
    const DecodedMarketEventV1& event,
    std::uint64_t ingress_sequence,
    KLineTradeV1* output) noexcept;

class KLineCursorV1 final {
public:
    KLineCursorV1(const KLineCursorV1&) = delete;
    KLineCursorV1& operator=(const KLineCursorV1&) = delete;
    KLineCursorV1(KLineCursorV1&&) noexcept;
    KLineCursorV1& operator=(KLineCursorV1&&) noexcept;
    ~KLineCursorV1();

    // Bars are copied in ascending window-start order. A successful empty
    // batch means end-of-stream.
    [[nodiscard]] KLineQueryErrorV1 ReadBatch(
        std::span<KLineBarV1> output,
        std::size_t* written) noexcept;
    [[nodiscard]] bool done() const noexcept;

private:
    class Impl;
    explicit KLineCursorV1(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;

    friend class KLineAggregatorSnapshotV1;
};

// Immutable, structure-sharing view captured by one permanent owner worker.
class KLineAggregatorSnapshotV1 final {
public:
    KLineAggregatorSnapshotV1(const KLineAggregatorSnapshotV1&) = delete;
    KLineAggregatorSnapshotV1& operator=(
        const KLineAggregatorSnapshotV1&) = delete;
    KLineAggregatorSnapshotV1(KLineAggregatorSnapshotV1&&) = delete;
    KLineAggregatorSnapshotV1& operator=(
        KLineAggregatorSnapshotV1&&) = delete;
    ~KLineAggregatorSnapshotV1();

    [[nodiscard]] std::uint32_t trade_date() const noexcept;
    [[nodiscard]] std::size_t window_count() const noexcept;
    [[nodiscard]] std::span<const KLineWindowSpecV1> windows()
        const noexcept;
    [[nodiscard]] std::uint64_t bar_count() const noexcept;

    // Copies the bar with the greatest window_start_ns_since_midnight for the
    // requested instrument/window. The immutable snapshot owns the indexed
    // series, so this lookup performs no allocation. A configured window with
    // no bar and an unknown instrument/window both return kNotFound.
    [[nodiscard]] KLineQueryErrorV1 GetLatestBar(
        std::uint32_t instrument_id,
        std::uint32_t window_id,
        KLineBarV1* output) const noexcept;

    [[nodiscard]] KLineQueryErrorV1 OpenInstrumentCursor(
        std::uint32_t instrument_id,
        std::uint32_t window_id,
        std::unique_ptr<KLineCursorV1>* output) const noexcept;

private:
    class Impl;
    explicit KLineAggregatorSnapshotV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;

    friend class KLineAggregatorV1;
};

// Internal exact-cut token used by the owner-index runtime. The permanent
// owner publishes one token only after all source fences for the generation
// have arrived. Publishing the token never enumerates instruments or series;
// the background generation builder materializes the immutable snapshot.
class KLineAggregatorCutV1 final {
public:
    KLineAggregatorCutV1(const KLineAggregatorCutV1&) = delete;
    KLineAggregatorCutV1& operator=(const KLineAggregatorCutV1&) = delete;
    KLineAggregatorCutV1(KLineAggregatorCutV1&&) = delete;
    KLineAggregatorCutV1& operator=(KLineAggregatorCutV1&&) = delete;
    ~KLineAggregatorCutV1();

private:
    class Impl;
    explicit KLineAggregatorCutV1(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;

    friend class KLineAggregatorV1;
};

// One history owner worker is the sole append writer. Generic captures are
// eager and owner-local. Production owner-index cuts use a per-row lock only
// for the first post-cut update or a concurrent background materialization;
// ordinary appends remain lock-free. Captures share immutable chunks, so a
// later append clones only the touched series metadata and chunk.
class KLineAggregatorV1 final {
public:
    KLineAggregatorV1(const KLineAggregatorV1&) = delete;
    KLineAggregatorV1& operator=(const KLineAggregatorV1&) = delete;
    KLineAggregatorV1(KLineAggregatorV1&&) = delete;
    KLineAggregatorV1& operator=(KLineAggregatorV1&&) = delete;
    ~KLineAggregatorV1();

    [[nodiscard]] static KLineCreateErrorV1 Create(
        KLineAggregatorConfigV1 config,
        std::unique_ptr<KLineAggregatorV1>* output) noexcept;

    [[nodiscard]] KLineAppendErrorV1 Append(
        const KLineTradeV1& trade) noexcept;
    [[nodiscard]] KLineAppendErrorV1 Append(
        const KLineTradeV1& trade,
        std::size_t owner_local_row) noexcept;
    // Standalone generic mode (instrument_capacity == 0) retains eager
    // immutable capture. Owner-index mode deliberately rejects this API so
    // production cannot accidentally perform an O(active-series) owner
    // capture.
    [[nodiscard]] KLineCaptureErrorV1 Capture(
        std::shared_ptr<const KLineAggregatorSnapshotV1>* output)
        noexcept;

    // Production owner-index capture is two-phase. CaptureOwnerCut runs on
    // the permanent owner after its source fences and performs constant work.
    // MaterializeOwnerCut runs on the background generation builder while the
    // owner continues appending. represented_owner_rows is the exact bound
    // catalog prefix assigned to this owner at the cut.
    [[nodiscard]] KLineCaptureErrorV1 CaptureOwnerCut(
        std::uint64_t generation,
        std::size_t represented_owner_rows,
        std::unique_ptr<KLineAggregatorCutV1>* output) noexcept;
    [[nodiscard]] KLineCaptureErrorV1 MaterializeOwnerCut(
        const KLineAggregatorCutV1& cut,
        std::shared_ptr<const KLineAggregatorSnapshotV1>* output)
        noexcept;

    [[nodiscard]] const KLineAggregatorConfigV1& config() const noexcept;
    [[nodiscard]] std::uint64_t bar_count() const noexcept;

private:
    class Impl;
    explicit KLineAggregatorV1(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::market
