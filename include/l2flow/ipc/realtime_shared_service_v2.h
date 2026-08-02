#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/market/daily_instrument_catalog_v2.h"
#include "l2flow/market/realtime_history_v1.h"
#include "l2flow/market/realtime_kline_v1.h"
#include "l2flow/ipc/realtime_store_generation_sink_v2.h"
#include "l2flow/realtime/processing_progress_v2.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string_view>
#include <vector>

namespace l2flow::ipc {

// Optional process-local measurements for one successfully emitted,
// nonterminal history page. They are deliberately outside the wire ABI.
// Supplying no observer leaves the production history path free of these
// additional clock reads.
struct RealtimeHistoryPageStageTimingV2 final {
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

class RealtimeHistoryPageStageObserverV2 {
public:
    virtual ~RealtimeHistoryPageStageObserverV2() = default;

    virtual void ObserveHistoryPageStageTiming(
        const RealtimeHistoryPageStageTimingV2& timing) noexcept = 0;
};

struct RealtimeSharedServiceConfigV2 final {
    l2flow::common::Identity128 run_id{};
    std::uint64_t session_epoch = 1U;
    std::uint32_t trade_date = 0U;
    // Immutable identity source. Create copies every exact key and identity
    // row into shared memory before Start can make the mapping ACTIVE.
    std::shared_ptr<
        const l2flow::market::DailyInstrumentCatalogV2>
        daily_catalog;
    std::vector<l2flow::market::KLineWindowSpecV1> kline_windows;
    // Explicit service semantics. A preview leaves every value false and is
    // started with StartLivePartial(). Ordinary complete services set their
    // known strong claims before Start(); from-open CERTIFIED may publish its
    // independent flag later with MarkCertifiedPrefixValid(). Online recovery
    // may instead prepare that flag while INITIALIZING and start behind the
    // shared control_exposure_gate, so the first obtainable descriptor already
    // contains the complete promoted capability set.
    bool coverage_from_open = false;
    bool startup_prefix_recovered = false;
    bool full_day_kline_valid = false;
    bool full_day_factor_valid = false;
    bool certified_prefix_valid = false;
    // Optional monotonic control-plane exposure gate. When non-null and
    // false, the control thread may be running but closes new clients before
    // dispatching any request or descriptor. Online promotion shares one
    // gate with the CERTIFIED service and flips it exactly once only after
    // both control planes and all prefix barriers are ready. Normal sessions
    // leave this null and preserve the existing control path.
    std::shared_ptr<const std::atomic<bool>> control_exposure_gate;
    std::uint64_t tick_ring_capacity = 262'144U;
    // Fixed for the session. Exhaustion is fatal; V2 deliberately has no
    // rollover or variable-size compatibility path.
    std::uint64_t key_arena_bytes = 16ULL * 1024ULL * 1024ULL;
    std::uint64_t maximum_mapping_bytes =
        2ULL * 1024ULL * 1024ULL * 1024ULL;
    // History and tick-delta readers pin immutable Store generations. These
    // bounds limit service workers and one sealed memfd page, not the total
    // retained history length or the number of pages a trusted same-UID
    // client has already received.
    std::uint32_t maximum_history_readers = 8U;
    std::uint32_t maximum_history_page_records = 16'384U;
    std::uint64_t maximum_history_page_bytes =
        64ULL * 1024ULL * 1024ULL;
    std::chrono::milliseconds history_reader_idle_timeout{
        std::chrono::seconds(30)};
    RealtimeHistoryPageStageObserverV2* history_stage_observer =
        nullptr;
    // Absolute path below an operator-owned directory. V2 never unlinks a
    // pre-existing path.
    std::filesystem::path control_socket_path;
};

enum class RealtimeSharedServiceCreateErrorV2 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kCatalogMismatch,
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
RealtimeSharedServiceCreateErrorNameV2(
    RealtimeSharedServiceCreateErrorV2 error) noexcept;

// Wire V2.4 service. History and delta readers bind to one immutable daily
// catalog generation and never enter the live callback/decoder path.
class RealtimeSharedMarketServiceV2 final
    : public l2flow::market::RealtimeAppliedRecordSinkV1,
      public l2flow::realtime::ProcessingProgressSinkV2,
      public RealtimeStoreGenerationSinkV2 {
public:
    RealtimeSharedMarketServiceV2(
        const RealtimeSharedMarketServiceV2&) = delete;
    RealtimeSharedMarketServiceV2& operator=(
        const RealtimeSharedMarketServiceV2&) = delete;
    RealtimeSharedMarketServiceV2(
        RealtimeSharedMarketServiceV2&&) = delete;
    RealtimeSharedMarketServiceV2& operator=(
        RealtimeSharedMarketServiceV2&&) = delete;
    ~RealtimeSharedMarketServiceV2() override;

    [[nodiscard]] static RealtimeSharedServiceCreateErrorV2 Create(
        RealtimeSharedServiceConfigV2 config,
        std::shared_ptr<RealtimeSharedMarketServiceV2>* output,
        int* system_error_number = nullptr) noexcept;

    // Starts only the control plane. All catalog rows are already published,
    // so ACTIVE always begins with bound_count == capacity.
    [[nodiscard]] bool Start(int* system_error_number = nullptr) noexcept;
    // Recovery previews use StartLivePartial() and expose only point reads.
    // A standalone process-start session may explicitly expose immutable
    // Store generations through History and tick-delta, plus explicitly
    // configured KLine carrying process-start coverage metadata, without
    // claiming coverage from market open. Both variants retain LIVE_PARTIAL
    // and every strong prefix flag remains false. Factor/CERTIFIED remain
    // unavailable.
    [[nodiscard]] bool StartLivePartial(
        int* system_error_number = nullptr) noexcept;
    [[nodiscard]] bool StartLivePartialWithProcessStartHistory(
        int* system_error_number = nullptr) noexcept;

    // A standalone LIVE_PARTIAL service with configured KLine calls this
    // exactly once before its first KLine generation. The boundary is the
    // conservative guaranteed-coverage point sampled after SDK Connect has
    // succeeded; any synchronous Connect callbacks remain partial input.
    // Repeating the same nonzero value is idempotent; a different value,
    // another service mode, or an already published KLine generation is
    // rejected.
    [[nodiscard]] bool PrepareProcessStartKLineCoverage(
        std::uint64_t coverage_start_unix_ns) noexcept;

    // Online recovery calls this only after the CERTIFIED Tick/Event prefix
    // barrier succeeds and before Start(). It publishes the immutable
    // capability flag while the mapping is still INITIALIZING, so the first
    // externally obtainable FAST descriptor already carries the complete
    // capability set. Ordinary from-open startup continues to use the
    // post-Start MarkCertifiedPrefixValid() path.
    [[nodiscard]] bool PrepareCertifiedPrefixValidBeforeStart() noexcept;

    [[nodiscard]] bool PublishApplied(
        std::size_t ordinal,
        const l2flow::market::RealtimeHistoryRecordV1& record)
        noexcept override;
    [[nodiscard]] bool PublishProcessingProgress(
        l2flow::realtime::ProcessingProgressV2 progress)
        noexcept override;
    void MarkCoverageLost() noexcept override;

    [[nodiscard]] bool PublishKLineGeneration(
        const l2flow::market::RealtimeKLineGenerationV1& generation)
        noexcept;
    [[nodiscard]] bool PublishStoreGeneration(
        const std::shared_ptr<const l2flow::market::
                                  IntradayInstrumentStoreGenerationV1>&
            generation) noexcept override;

    void MarkDraining() noexcept;
    [[nodiscard]] bool MarkCertifiedPrefixValid() noexcept;
    // Call only after MarkDraining and after every producer has joined.
    [[nodiscard]] bool MarkStoppedClean(
        std::uint64_t final_admitted_tick_sequence) noexcept;
    void MarkFailed() noexcept;
    void StopControl() noexcept;

    [[nodiscard]] std::uint64_t mapping_bytes() const noexcept;
    [[nodiscard]] std::uint64_t key_arena_used_bytes() const noexcept;
    // Low-frequency owner health sample: the exact dense Tick prefix that is
    // currently readable from the ring. This is one acquire load and is not
    // used by callback or reader hot paths.
    [[nodiscard]] std::uint64_t tick_contiguous_published_sequence()
        const noexcept;
    [[nodiscard]] bool failed() const noexcept;
    [[nodiscard]] const std::filesystem::path& control_socket_path()
        const noexcept;

private:
    class Impl;
    explicit RealtimeSharedMarketServiceV2(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::ipc
