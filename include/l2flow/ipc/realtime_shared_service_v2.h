#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/market/observed_instrument_directory_v2.h"
#include "l2flow/market/realtime_history_v1.h"
#include "l2flow/market/realtime_kline_v1.h"
#include "l2flow/ipc/realtime_store_generation_sink_v2.h"
#include "l2flow/realtime/processing_progress_v2.h"

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
    // Non-owning. The directory must outlive the service and must still have
    // an empty bound prefix when Create is called.
    const l2flow::market::ObservedInstrumentDirectoryV2* directory =
        nullptr;
    std::vector<l2flow::market::KLineWindowSpecV1> kline_windows;
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
    kDirectoryNotEmpty,
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

// Hard Wire V2 replacement. There is no V1 adapter or authoritative-catalog
// mode. History and delta readers bind to one immutable observed-universe
// generation and never enter the live callback/processing path.
class RealtimeSharedMarketServiceV2 final
    : public l2flow::market::RealtimeAppliedRecordSinkV1,
      public l2flow::market::ObservedInstrumentBindingSinkV2,
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

    // Starts only the minimal GET_SESSION/SCM_RIGHTS control plane and makes
    // the mapping ACTIVE immediately, including when bound_count is zero.
    [[nodiscard]] bool Start(int* system_error_number = nullptr) noexcept;

    [[nodiscard]] bool PublishObservedInstrumentBinding(
        const l2flow::market::ObservedInstrumentBindResultV2& binding)
        noexcept override;
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
    // Call only after MarkDraining and after every producer has joined.
    [[nodiscard]] bool MarkStoppedClean(
        std::uint64_t final_admitted_tick_sequence) noexcept;
    void MarkFailed() noexcept;
    void StopControl() noexcept;

    [[nodiscard]] std::uint64_t mapping_bytes() const noexcept;
    [[nodiscard]] std::uint64_t key_arena_used_bytes() const noexcept;
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
