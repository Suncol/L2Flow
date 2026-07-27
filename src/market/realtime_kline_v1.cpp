#include "l2flow/market/realtime_kline_v1.h"

#include "l2flow/market/realtime_history_v1.h"

#include <limits>
#include <new>
#include <utility>

namespace l2flow::market {

std::string_view RealtimeKLineGenerationErrorNameV1(
    RealtimeKLineGenerationErrorV1 error) noexcept {
    switch (error) {
        case RealtimeKLineGenerationErrorV1::kNone:
            return "none";
        case RealtimeKLineGenerationErrorV1::kNullOutput:
            return "null_output";
        case RealtimeKLineGenerationErrorV1::kInvalidInput:
            return "invalid_input";
        case RealtimeKLineGenerationErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

class RealtimeKLineGenerationV1::Impl final {
public:
    std::shared_ptr<const IntradayInstrumentStoreGenerationV1>
        input_store;
    std::vector<std::shared_ptr<const KLineAggregatorSnapshotV1>>
        worker_snapshots;
    std::uint32_t worker_count = 0U;
    std::uint64_t bar_count = 0U;
};

RealtimeKLineGenerationV1::RealtimeKLineGenerationV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

RealtimeKLineGenerationV1::~RealtimeKLineGenerationV1() = default;

RealtimeKLineGenerationErrorV1 RealtimeKLineGenerationV1::Build(
    std::shared_ptr<const IntradayInstrumentStoreGenerationV1>
        input_store,
    std::vector<std::shared_ptr<const KLineAggregatorSnapshotV1>>
        worker_snapshots,
    std::uint32_t worker_count,
    std::shared_ptr<const RealtimeKLineGenerationV1>* output) noexcept {
    if (output == nullptr) {
        return RealtimeKLineGenerationErrorV1::kNullOutput;
    }
    output->reset();
    if (input_store == nullptr || worker_count == 0U ||
        worker_snapshots.size() !=
            static_cast<std::size_t>(worker_count)) {
        return RealtimeKLineGenerationErrorV1::kInvalidInput;
    }
    const std::uint32_t trade_date =
        input_store->watermark().trade_date;
    std::size_t expected_window_count = 0U;
    std::span<const KLineWindowSpecV1> expected_windows;
    std::uint64_t total_bars = 0U;
    for (std::size_t worker = 0U;
         worker < worker_snapshots.size();
         ++worker) {
        const auto& snapshot = worker_snapshots[worker];
        if (snapshot == nullptr ||
            snapshot->trade_date() != trade_date ||
            snapshot->window_count() == 0U ||
            (worker != 0U &&
             snapshot->window_count() != expected_window_count) ||
            total_bars >
                std::numeric_limits<std::uint64_t>::max() -
                    snapshot->bar_count()) {
            return RealtimeKLineGenerationErrorV1::kInvalidInput;
        }
        if (worker == 0U) {
            expected_windows = snapshot->windows();
        } else {
            const std::span<const KLineWindowSpecV1> windows =
                snapshot->windows();
            for (std::size_t index = 0U;
                 index < windows.size();
                 ++index) {
                if (windows[index].window_id !=
                        expected_windows[index].window_id ||
                    windows[index].duration_ns !=
                        expected_windows[index].duration_ns) {
                    return RealtimeKLineGenerationErrorV1::
                        kInvalidInput;
                }
            }
        }
        expected_window_count = snapshot->window_count();
        total_bars += snapshot->bar_count();
    }
    try {
        auto impl = std::make_unique<Impl>();
        impl->input_store = std::move(input_store);
        impl->worker_snapshots = std::move(worker_snapshots);
        impl->worker_count = worker_count;
        impl->bar_count = total_bars;
        output->reset(
            new RealtimeKLineGenerationV1(std::move(impl)));
        return RealtimeKLineGenerationErrorV1::kNone;
    } catch (...) {
        return RealtimeKLineGenerationErrorV1::kResourceExhausted;
    }
}

const RealtimeHistoryWatermarkV1&
RealtimeKLineGenerationV1::watermark() const noexcept {
    return impl_->input_store->watermark();
}

const std::shared_ptr<const IntradayInstrumentStoreGenerationV1>&
RealtimeKLineGenerationV1::input_store() const noexcept {
    return impl_->input_store;
}

std::uint32_t RealtimeKLineGenerationV1::worker_count() const noexcept {
    return impl_->worker_count;
}

std::span<const KLineWindowSpecV1>
RealtimeKLineGenerationV1::windows() const noexcept {
    return impl_ == nullptr || impl_->worker_snapshots.empty() ||
                   impl_->worker_snapshots.front() == nullptr
               ? std::span<const KLineWindowSpecV1>{}
               : impl_->worker_snapshots.front()->windows();
}

std::uint64_t RealtimeKLineGenerationV1::bar_count() const noexcept {
    return impl_->bar_count;
}

bool RealtimeKLineGenerationV1::coverage_from_open() const noexcept {
    return impl_->input_store->coverage_from_open();
}

KLineQueryErrorV1 RealtimeKLineGenerationV1::GetLatestBar(
    std::uint32_t instrument_id,
    std::uint32_t window_id,
    KLineBarV1* output) const noexcept {
    if (output == nullptr) {
        return KLineQueryErrorV1::kNullOutput;
    }
    *output = KLineBarV1{};
    if (instrument_id == 0U || window_id == 0U ||
        impl_ == nullptr || impl_->worker_count == 0U) {
        return KLineQueryErrorV1::kInvalidArgument;
    }
    const std::uint32_t worker =
        instrument_id % impl_->worker_count;
    if (worker >= impl_->worker_snapshots.size() ||
        impl_->worker_snapshots[worker] == nullptr) {
        return KLineQueryErrorV1::kNotFound;
    }
    return impl_->worker_snapshots[worker]->GetLatestBar(
        instrument_id, window_id, output);
}

KLineQueryErrorV1 RealtimeKLineGenerationV1::OpenInstrumentCursor(
    std::uint32_t instrument_id,
    std::uint32_t window_id,
    std::unique_ptr<KLineCursorV1>* output) const noexcept {
    if (output == nullptr) {
        return KLineQueryErrorV1::kNullOutput;
    }
    output->reset();
    if (instrument_id == 0U || window_id == 0U ||
        impl_ == nullptr || impl_->worker_count == 0U) {
        return KLineQueryErrorV1::kInvalidArgument;
    }
    const std::uint32_t worker =
        instrument_id % impl_->worker_count;
    if (worker >= impl_->worker_snapshots.size() ||
        impl_->worker_snapshots[worker] == nullptr) {
        return KLineQueryErrorV1::kNotFound;
    }
    return impl_->worker_snapshots[worker]->OpenInstrumentCursor(
        instrument_id, window_id, output);
}

}  // namespace l2flow::market
