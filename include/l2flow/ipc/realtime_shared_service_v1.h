#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/market/intraday_instrument_store_v1.h"
#include "l2flow/market/instrument_registry.h"
#include "l2flow/market/realtime_history_v1.h"
#include "l2flow/market/realtime_kline_v1.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string_view>
#include <vector>

namespace l2flow::ipc {

// Optional, process-local benchmark telemetry. This is deliberately not part
// of the history wire ABI. Durations use CLOCK_MONOTONIC and cover one
// successfully sent nonterminal history page. Observers can be invoked
// concurrently by independent history-reader workers.
struct RealtimeHistoryPageStageTimingV1 final {
    std::uint64_t open_request_id = 0U;
    std::uint64_t read_request_id = 0U;
    std::uint64_t generation = 0U;
    std::uint32_t instrument_id = 0U;
    std::uint32_t reserved0 = 0U;
    std::uint64_t page_index = 0U;
    std::uint32_t record_count = 0U;
    std::uint32_t snapshot_count = 0U;
    std::uint32_t tick_count = 0U;
    std::uint32_t clock_read_failures = 0U;
    std::uint64_t page_mapping_bytes = 0U;
    std::uint64_t cursor_read_ns = 0U;
    std::uint64_t classify_layout_ns = 0U;
    std::uint64_t memfd_prepare_ns = 0U;
    std::uint64_t projection_ns = 0U;
    std::uint64_t memfd_finalize_ns = 0U;
    std::uint64_t build_total_ns = 0U;
    std::uint64_t token_ns = 0U;
    std::uint64_t send_ns = 0U;
};

class RealtimeHistoryPageStageObserverV1 {
public:
    virtual ~RealtimeHistoryPageStageObserverV1() = default;

    virtual void ObserveHistoryPageStageTiming(
        const RealtimeHistoryPageStageTimingV1& timing) noexcept = 0;
};

struct RealtimeSharedServiceConfigV1 final {
    l2flow::common::Identity128 run_id{};
    std::uint64_t session_epoch = 1U;
    std::uint32_t trade_date = 0U;
    const l2flow::market::InstrumentRegistryV1* registry = nullptr;
    std::vector<l2flow::market::KLineWindowSpecV1> kline_windows;
    // One global mixed-tick ring. Capacity must cover the deployment's
    // maximum tolerated consumer pause and maximum in-flight reorder span.
    std::uint64_t tick_ring_capacity = 262'144U;
    std::uint64_t maximum_mapping_bytes = 2ULL * 1024ULL * 1024ULL *
                                          1024ULL;
    // History readers and V2 tick-delta sessions pin one immutable Store
    // generation for the lifetime of a cursor/session. These limits bound
    // pinned generations, worker threads, and each completed sealed page;
    // they do not limit total history length or pages intentionally retained
    // by a same-UID client after SCM_RIGHTS transfer. The trust boundary
    // treats same-UID clients as trusted.
    std::uint32_t maximum_history_readers = 8U;
    std::uint32_t maximum_history_page_records = 16'384U;
    std::uint64_t maximum_history_page_bytes =
        64ULL * 1024ULL * 1024ULL;
    std::chrono::milliseconds history_reader_idle_timeout{
        std::chrono::seconds(30)};
    // Optional non-owning observer for benchmark-only stage timing. It must
    // outlive the service and remain safe for concurrent noexcept callbacks.
    // A null observer adds no benchmark clock reads to the history path.
    RealtimeHistoryPageStageObserverV1* history_stage_observer = nullptr;
    // Absolute path below an operator-owned directory. V1 never unlinks a
    // pre-existing path.
    std::filesystem::path control_socket_path;
};

enum class RealtimeSharedServiceCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kLayoutOverflow,
    kMappingCreateFailed,
    kReadOnlyHandleFailed,
    kSealFailed,
    kSocketCreateFailed,
    kSocketPathExists,
    kSocketBindFailed,
    kResourceExhausted,
    kUnexpectedFailure,
};

[[nodiscard]] std::string_view
RealtimeSharedServiceCreateErrorNameV1(
    RealtimeSharedServiceCreateErrorV1 error) noexcept;

// Linux memfd data plane plus AF_UNIX/SCM_RIGHTS discovery control plane.
// The service is an application-layer composition object; Pipeline and Store
// depend only on the generic RealtimeAppliedRecordSinkV1 interface.
class RealtimeSharedMarketServiceV1 final
    : public l2flow::market::RealtimeAppliedRecordSinkV1 {
public:
    RealtimeSharedMarketServiceV1(
        const RealtimeSharedMarketServiceV1&) = delete;
    RealtimeSharedMarketServiceV1& operator=(
        const RealtimeSharedMarketServiceV1&) = delete;
    RealtimeSharedMarketServiceV1(
        RealtimeSharedMarketServiceV1&&) = delete;
    RealtimeSharedMarketServiceV1& operator=(
        RealtimeSharedMarketServiceV1&&) = delete;
    ~RealtimeSharedMarketServiceV1() override;

    [[nodiscard]] static RealtimeSharedServiceCreateErrorV1 Create(
        RealtimeSharedServiceConfigV1 config,
        std::shared_ptr<RealtimeSharedMarketServiceV1>* output,
        int* system_error_number = nullptr) noexcept;

    // Create prepares the writable data plane and binds the socket before the
    // SDK can produce synchronous callbacks. Start begins accepting clients
    // only after Pipeline creation succeeds.
    [[nodiscard]] bool Start(int* system_error_number = nullptr) noexcept;

    [[nodiscard]] bool PublishApplied(
        std::size_t registry_ordinal,
        const l2flow::market::RealtimeHistoryRecordV1& record)
        noexcept override;
    void MarkCoverageLost() noexcept override;

    // Publishes the latest bar for every instrument×configured window from
    // this exact immutable generation. It never recomputes KLine.
    [[nodiscard]] bool PublishKLineGeneration(
        const l2flow::market::RealtimeKLineGenerationV1& generation)
        noexcept;

    // Publishes the next exact immutable generation from one Store session for
    // future history cursors. Generation numbers, cuts, source identities, and
    // per-instrument counts must not regress. Existing cursors retain the
    // generation acquired when opened.
    [[nodiscard]] bool PublishStoreGeneration(
        std::shared_ptr<const l2flow::market::
                            IntradayInstrumentStoreGenerationV1>
            generation) noexcept;

    void MarkDraining() noexcept;
    // Call only after MarkDraining and after every producer has joined.
    // FAILED is terminal and can never be overwritten by STOPPED_CLEAN.
    [[nodiscard]] bool MarkStoppedClean(
        std::uint64_t final_admitted_tick_sequence) noexcept;
    void MarkFailed() noexcept;
    void StopControl() noexcept;

    [[nodiscard]] std::uint64_t mapping_bytes() const noexcept;
    [[nodiscard]] bool failed() const noexcept;
    [[nodiscard]] const std::filesystem::path& control_socket_path()
        const noexcept;

private:
    class Impl;
    explicit RealtimeSharedMarketServiceV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::ipc
