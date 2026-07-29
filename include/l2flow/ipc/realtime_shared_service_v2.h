#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/market/observed_instrument_directory_v2.h"
#include "l2flow/market/realtime_history_v1.h"
#include "l2flow/market/realtime_kline_v1.h"
#include "l2flow/realtime/processing_progress_v2.h"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string_view>
#include <vector>

namespace l2flow::ipc {

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

// Hard Wire V2 replacement. There is no V1 adapter, authoritative-catalog
// mode, history cursor, or delta-session control opcode.
class RealtimeSharedMarketServiceV2 final
    : public l2flow::market::RealtimeAppliedRecordSinkV1,
      public l2flow::market::ObservedInstrumentBindingSinkV2,
      public l2flow::realtime::ProcessingProgressSinkV2 {
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
