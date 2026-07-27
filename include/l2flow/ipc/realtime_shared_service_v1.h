#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/market/instrument_registry.h"
#include "l2flow/market/realtime_history_v1.h"
#include "l2flow/market/realtime_kline_v1.h"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string_view>
#include <vector>

namespace l2flow::ipc {

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

    void MarkDraining() noexcept;
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
