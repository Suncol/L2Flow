#pragma once

#include "l2flow/market/intraday_instrument_store_v1.h"
#include "l2flow/market/kline_aggregator_v1.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

namespace l2flow::market {

enum class RealtimeKLineGenerationErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidInput,
    kResourceExhausted,
};

[[nodiscard]] std::string_view RealtimeKLineGenerationErrorNameV1(
    RealtimeKLineGenerationErrorV1 error) noexcept;

// Immutable exact-prefix KLine publication. It retains the exact store
// generation from the same four-source fence, so consumers never need to
// combine independently acquired "latest" handles.
class RealtimeKLineGenerationV1 final {
public:
    RealtimeKLineGenerationV1(const RealtimeKLineGenerationV1&) = delete;
    RealtimeKLineGenerationV1& operator=(
        const RealtimeKLineGenerationV1&) = delete;
    RealtimeKLineGenerationV1(RealtimeKLineGenerationV1&&) = delete;
    RealtimeKLineGenerationV1& operator=(
        RealtimeKLineGenerationV1&&) = delete;
    ~RealtimeKLineGenerationV1();

    [[nodiscard]] static RealtimeKLineGenerationErrorV1 Build(
        std::shared_ptr<
            const IntradayInstrumentStoreGenerationV1> input_store,
        std::vector<
            std::shared_ptr<const KLineAggregatorSnapshotV1>>
            worker_snapshots,
        std::uint32_t worker_count,
        std::shared_ptr<const RealtimeKLineGenerationV1>* output)
        noexcept;

    [[nodiscard]] const RealtimeHistoryWatermarkV1& watermark()
        const noexcept;
    [[nodiscard]] const std::shared_ptr<
        const IntradayInstrumentStoreGenerationV1>&
    input_store() const noexcept;
    [[nodiscard]] std::uint32_t worker_count() const noexcept;
    [[nodiscard]] std::span<const KLineWindowSpecV1> windows()
        const noexcept;
    [[nodiscard]] std::uint64_t bar_count() const noexcept;
    [[nodiscard]] bool coverage_from_open() const noexcept;

    // Allocation-free point lookup in this exact immutable generation. The
    // selected bar has the greatest window_start_ns_since_midnight for the
    // requested instrument/window.
    [[nodiscard]] KLineQueryErrorV1 GetLatestBar(
        std::uint32_t instrument_id,
        std::uint32_t window_id,
        KLineBarV1* output) const noexcept;

    // Returns all non-empty bars since process coverage began for one
    // instrument/window, in ascending window-start order. When
    // coverage_from_open() is true this is the complete from-open view for
    // the generation's accepted ingress prefix.
    [[nodiscard]] KLineQueryErrorV1 OpenInstrumentCursor(
        std::uint32_t instrument_id,
        std::uint32_t window_id,
        std::unique_ptr<KLineCursorV1>* output) const noexcept;

private:
    class Impl;
    explicit RealtimeKLineGenerationV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::market
