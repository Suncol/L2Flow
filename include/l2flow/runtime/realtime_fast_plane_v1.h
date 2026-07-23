#pragma once

#include "l2flow/ingress/fast_capture_sink_v1.h"
#include "l2flow/market/instrument_history_v1.h"
#include "l2flow/sdk/subscription_manifest.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

namespace l2flow::market {
class InstrumentRegistryV1;
}

namespace l2flow::runtime {

inline constexpr std::size_t kRealtimeFastPlaneSourceCountV1 = 4U;

enum class RealtimeFastPlaneCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kNullRegistry,
    kHistoryCreateFailed,
    kResourceExhausted,
    kThreadStartFailed,
};

[[nodiscard]] std::string_view RealtimeFastPlaneCreateErrorNameV1(
    RealtimeFastPlaneCreateErrorV1 error) noexcept;

enum class RealtimeFastPlaneFailureV1 : std::uint8_t {
    kNone = 0U,
    kCaptureFull,
    kCaptureInvalid,
    kRingCorrupt,
    kDecodeFailed,
    kRetainFailed,
    kEnvelopeFailed,
    kHistoryFailed,
    kUpstreamFatal,
    kUnexpectedFailure,
};

[[nodiscard]] std::string_view RealtimeFastPlaneFailureNameV1(
    RealtimeFastPlaneFailureV1 failure) noexcept;

struct RealtimeFastPlaneConfigV1 final {
    std::uint32_t capture_date = 0U;
    std::uint32_t trade_date = 0U;
    std::uint64_t first_ingress_sequence = 1U;
    std::uint32_t max_message_bytes = 0U;
    std::size_t input_ring_capacity_bytes_per_source = 0U;
    std::array<l2flow::sdk::IngressKind,
               kRealtimeFastPlaneSourceCountV1>
        source_kinds{
            l2flow::sdk::IngressKind::ShSnapshot,
            l2flow::sdk::IngressKind::ShTick,
            l2flow::sdk::IngressKind::SzSnapshot,
            l2flow::sdk::IngressKind::SzTick,
        };
    l2flow::market::InstrumentHistoryRuntimeConfigV1 history{};
};

struct RealtimeFastPlaneSourceSnapshotV1 final {
    l2flow::sdk::IngressKind ingress_kind =
        l2flow::sdk::IngressKind::ShSnapshot;
    std::uint32_t source_stream_id = 0U;
    std::uint64_t captured_records = 0U;
    std::uint64_t decoded_records = 0U;
    std::uint64_t ignored_records = 0U;
    std::uint64_t history_submissions = 0U;
    std::uint64_t history_backpressure_retries = 0U;
    std::uint64_t last_captured_sequence = 0U;
    std::uint64_t last_processed_sequence = 0U;
    std::uint64_t last_submitted_sequence = 0U;
    std::uint64_t ring_used_bytes = 0U;
    std::uint64_t ring_capacity_bytes = 0U;
    l2flow::market::InstrumentHistorySourceFrontierV1 history_frontier{};
    RealtimeFastPlaneFailureV1 failure =
        RealtimeFastPlaneFailureV1::kNone;
    std::uint64_t failure_sequence = 0U;
    bool worker_exited = false;
    bool terminal_prefix_complete = false;
};

struct RealtimeFastPlaneSnapshotV1 final {
    std::array<RealtimeFastPlaneSourceSnapshotV1,
               kRealtimeFastPlaneSourceCountV1>
        sources{};
    bool accepting = false;
    bool stopped = false;
    bool fatal = false;
    bool clean_drain = false;
};

// Shadow/canary Fast Plane:
//   callback-owned bytes -> one bounded SPSC ring per source
//   -> four source-order decoder workers
//   -> a private multi-worker InstrumentHistoryRuntimeV1.
//
// It deliberately does not consume Raw/WAL/Canonical state. The borrowed
// immutable registry must outlive this runtime. StopAndDrain() must run only
// after every producer callback has quiesced. The capture sink is SPSC per
// source: one source_stream_id has exactly one externally serialized producer,
// while distinct sources may publish concurrently.
class RealtimeFastPlaneRuntimeV1 final {
public:
    RealtimeFastPlaneRuntimeV1(
        const RealtimeFastPlaneRuntimeV1&) = delete;
    RealtimeFastPlaneRuntimeV1& operator=(
        const RealtimeFastPlaneRuntimeV1&) = delete;
    RealtimeFastPlaneRuntimeV1(
        RealtimeFastPlaneRuntimeV1&&) = delete;
    RealtimeFastPlaneRuntimeV1& operator=(
        RealtimeFastPlaneRuntimeV1&&) = delete;
    ~RealtimeFastPlaneRuntimeV1();

    [[nodiscard]] static RealtimeFastPlaneCreateErrorV1 Create(
        RealtimeFastPlaneConfigV1 config,
        const l2flow::market::InstrumentRegistryV1* registry,
        std::unique_ptr<RealtimeFastPlaneRuntimeV1>* output) noexcept;

    [[nodiscard]] l2flow::ingress::FastCaptureSinkRefV1
    capture_sink() noexcept;

    [[nodiscard]] RealtimeFastPlaneSnapshotV1 Snapshot() const noexcept;

    // Low-latency query facade. These methods expose only the private Fast
    // Plane history and reject the entire serving generation after any Fast
    // Plane fatal condition.
    [[nodiscard]] l2flow::market::InstrumentHistoryQueryErrorV1 Latest(
        std::uint32_t instrument_id,
        std::uint8_t source_slot,
        l2flow::market::InstrumentHistoryLaneV1 lane,
        l2flow::market::InstrumentHistoryRecordHandleV1* output)
        const noexcept;
    [[nodiscard]] l2flow::market::InstrumentHistoryQueryErrorV1 Tail(
        std::uint32_t instrument_id,
        std::uint8_t source_slot,
        l2flow::market::InstrumentHistoryLaneV1 lane,
        std::size_t count,
        std::vector<l2flow::market::InstrumentHistoryRecordHandleV1>* output)
        const noexcept;
    [[nodiscard]] const RealtimeFastPlaneConfigV1& config() const noexcept;
    [[nodiscard]] const l2flow::market::InstrumentRegistryV1*
    registry() const noexcept;

    void StopAndDrain() noexcept;

private:
    class Impl;
    explicit RealtimeFastPlaneRuntimeV1(
        std::unique_ptr<Impl> impl) noexcept;

    static l2flow::ingress::FastCapturePublishResultV1 TryPublishCopy(
        void* context,
        const l2flow::ingress::CaptureMetaV1& metadata,
        std::span<const std::byte, l2flow::sdk::kVendorHeadBytes> head,
        std::span<const std::byte> body) noexcept;
    static void InvalidateGeneration(
        void* context,
        std::uint32_t source_stream_id,
        std::uint64_t ingress_sequence) noexcept;

    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::runtime
