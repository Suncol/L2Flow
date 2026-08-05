#pragma once

#include "l2flow/ipc/realtime_wire_v3.h"
#include "l2flow/market/fast_tick_store_v1.h"
#include "l2flow/market/mutable_kline_history_v1.h"
#include "l2flow/market/ordered_event_history_v1.h"
#include "l2flow/runtime/realtime_planes_v1.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

namespace l2flow::ipc {

struct InstrumentDataServiceConfigV3 final {
    std::size_t maximum_fast_rows_per_read = 64U * 1024U;
    std::size_t maximum_event_changes_per_read = 64U * 1024U;
    std::size_t maximum_kline_changes_per_read = 64U * 1024U;
    std::size_t maximum_instruments_per_batch = 1024U;
};

enum class InstrumentDataServiceErrorV3 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kCursorMismatch,
    kBatchLimitExceeded,
    kNotFound,
    kFastReadFailed,
    kEventReadFailed,
    kKLineReadFailed,
    kResourceExhausted,
};

[[nodiscard]] std::string_view InstrumentDataServiceErrorNameV3(
    InstrumentDataServiceErrorV3 error) noexcept;

struct FastTickDeltaRowV3 final {
    std::uint64_t instrument_tick_sequence = 0U;
    l2flow::market::CompactFastTickV1 tick{};
};

struct FastTickDeltaV3 final {
    FastTickCursorWireV3 next{};
    std::uint64_t captured_tail = 0U;
    bool coverage_from_open = false;
    bool coverage_complete = false;
    std::vector<FastTickDeltaRowV3> rows;
};

struct EventStableViewV3 final {
    std::shared_ptr<const l2flow::market::EventStableRootV1> root;
    EventChangeCursorWireV3 next_changes{};
    InstrumentStableStatusWireV3 status{};
};

struct KLineStableViewV3 final {
    std::shared_ptr<const l2flow::market::KLineStableRootV1> root;
    KLineChangeCursorWireV3 next_changes{};
    InstrumentStableStatusWireV3 status{};
};

// The V3 service is a dependency-light IPC composition boundary. `planes`
// supplied to Create must outlive the service. Returned shared stable roots
// own their immutable snapshots and may outlive either object. It samples
// only one instrument per request. A batch is merely a list of independent
// samples; it never claims a cross-instrument or cross-plane atomic cut.
class InstrumentDataServiceV3 final {
public:
    InstrumentDataServiceV3(const InstrumentDataServiceV3&) = delete;
    InstrumentDataServiceV3& operator=(
        const InstrumentDataServiceV3&) = delete;
    InstrumentDataServiceV3(InstrumentDataServiceV3&&) = delete;
    InstrumentDataServiceV3& operator=(InstrumentDataServiceV3&&) =
        delete;
    ~InstrumentDataServiceV3();

    [[nodiscard]] static InstrumentDataServiceErrorV3 Create(
        InstrumentDataServiceConfigV3 config,
        const l2flow::runtime::RealtimePlanesV1* planes,
        std::unique_ptr<InstrumentDataServiceV3>* output) noexcept;

    [[nodiscard]] InstrumentDataServiceErrorV3 ReadFastDelta(
        const FastTickCursorWireV3& cursor,
        std::size_t maximum_rows,
        FastTickDeltaV3* output) const noexcept;

    [[nodiscard]] InstrumentDataServiceErrorV3 ReadFastBatch(
        std::span<const FastTickCursorWireV3> cursors,
        std::size_t maximum_rows_per_instrument,
        std::vector<FastTickDeltaV3>* output) const noexcept;

    [[nodiscard]] InstrumentDataServiceErrorV3 AcquireEventStable(
        std::uint32_t instrument_id,
        EventStableViewV3* output) const noexcept;
    [[nodiscard]] InstrumentDataServiceErrorV3 ReadEventChanges(
        EventChangeCursorWireV3* cursor,
        std::span<l2flow::market::EventMutationV1> output,
        std::size_t* written) const noexcept;

    [[nodiscard]] InstrumentDataServiceErrorV3 AcquireKLineStable(
        std::uint32_t instrument_id,
        KLineStableViewV3* output) const noexcept;
    [[nodiscard]] InstrumentDataServiceErrorV3 ReadKLineChanges(
        KLineChangeCursorWireV3* cursor,
        std::span<l2flow::market::KLineMutationV1> output,
        std::size_t* written) const noexcept;

    [[nodiscard]] const InstrumentDataServiceConfigV3& config()
        const noexcept;

private:
    class Impl;
    explicit InstrumentDataServiceV3(std::unique_ptr<Impl> impl)
        noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::ipc
