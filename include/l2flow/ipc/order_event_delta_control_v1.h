#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/ipc/order_event_delta_control_wire_v1.h"
#include "l2flow/ipc/order_event_delta_ring_v1.h"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string_view>

namespace l2flow::ipc {

// Identifies the upstream Wire V2 tick source consumed by the event
// aggregator. It is intentionally separate from the derived event-ring
// session: restarting either process must not silently rebind the other.
struct OrderEventDeltaSourceSessionV1 final {
    l2flow::common::Identity128 run_id{};
    l2flow::common::Sha256Digest catalog_digest{};
    std::uint64_t session_epoch = 0U;
    std::uint64_t catalog_generation = 0U;
    std::uint64_t catalog_version = 0U;
    std::uint32_t trade_date = 0U;
    std::uint32_t catalog_trade_date = 0U;
    std::uint32_t capacity = 0U;
    std::uint32_t bound_count = 0U;
    std::uint32_t catalog_scope = 0U;
    std::uint32_t coverage_complete = 0U;
    OrderEventDeltaTemporalCoverageV1 temporal_coverage =
        OrderEventDeltaTemporalCoverageV1::kFromMarketOpen;
    OrderEventDeltaStreamQualityV1 stream_quality =
        OrderEventDeltaStreamQualityV1::
            kLocalTickStreamContiguous;

    [[nodiscard]] friend bool operator==(
        const OrderEventDeltaSourceSessionV1&,
        const OrderEventDeltaSourceSessionV1&) noexcept = default;
};

struct OrderEventDeltaControlSnapshotV1 final {
    OrderEventDeltaSourceSessionV1 source_session{};
    OrderEventDeltaSessionV1 event_session{};
    std::uint64_t event_published_sequence = 0U;
    std::uint64_t source_tick_consumed_sequence = 0U;
    std::uint64_t heartbeat_monotonic_ns = 0U;
    std::uint64_t producer_started_monotonic_ns = 0U;
    std::uint32_t event_producer_state = 0U;
    std::uint32_t event_header_flags = 0U;

    [[nodiscard]] friend bool operator==(
        const OrderEventDeltaControlSnapshotV1&,
        const OrderEventDeltaControlSnapshotV1&) noexcept = default;
};

struct OrderEventDeltaControlServerConfigV1 final {
    OrderEventDeltaSourceSessionV1 source_session{};
    // Non-owning. The producer must outlive the server, including its
    // internally joined control thread. Source and ring temporal/quality
    // contracts must match exactly.
    const OrderEventDeltaRingProducerV1* event_ring = nullptr;
    // Absolute pathname below a same-UID, owner-only directory. Create never
    // unlinks an existing filesystem object.
    std::filesystem::path control_socket_path;
    // Bounds the complete receive-and-response lifetime of one connection.
    std::chrono::milliseconds request_timeout{
        std::chrono::milliseconds(1000)};
};

enum class OrderEventDeltaControlServerCreateErrorV1 :
    std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kRingUnavailable,
    kRingDescriptorInvalid,
    kSocketPathExists,
    kSocketCreateFailed,
    kSocketBindFailed,
    kResourceExhausted,
    kUnexpectedFailure,
};

enum class OrderEventDeltaControlClientErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kSocketPathInvalid,
    kConnectFailed,
    kPeerCredentialRejected,
    kTimeout,
    kTransportFailed,
    kProtocolError,
    kUnsupportedVersion,
    kUnavailable,
    kSourceSessionMismatch,
    kRingDescriptorInvalid,
    kResourceExhausted,
    kUnexpectedFailure,
};

[[nodiscard]] std::string_view
OrderEventDeltaControlServerCreateErrorNameV1(
    OrderEventDeltaControlServerCreateErrorV1 error) noexcept;
[[nodiscard]] std::string_view
OrderEventDeltaControlClientErrorNameV1(
    OrderEventDeltaControlClientErrorV1 error) noexcept;

class OrderEventDeltaControlServerV1 final {
public:
    OrderEventDeltaControlServerV1(
        const OrderEventDeltaControlServerV1&) = delete;
    OrderEventDeltaControlServerV1& operator=(
        const OrderEventDeltaControlServerV1&) = delete;
    OrderEventDeltaControlServerV1(
        OrderEventDeltaControlServerV1&&) = delete;
    OrderEventDeltaControlServerV1& operator=(
        OrderEventDeltaControlServerV1&&) = delete;
    ~OrderEventDeltaControlServerV1();

    [[nodiscard]] static
    OrderEventDeltaControlServerCreateErrorV1 Create(
        OrderEventDeltaControlServerConfigV1 config,
        std::unique_ptr<OrderEventDeltaControlServerV1>* output,
        int* system_error_number = nullptr) noexcept;

    // Lifecycle calls are serial-only. A server is single-start: after Stop,
    // it cannot be restarted. Start succeeds (READY) only while the ring is
    // ACTIVE and its declared local stream has not lost coverage. The
    // temporal origin may be market-open or process-start and is never
    // upgraded implicitly. Every successful GET_SESSION repeats that same
    // test immediately before transferring the descriptor.
    [[nodiscard]] bool Start(
        int* system_error_number = nullptr) noexcept;
    void Stop() noexcept;
    [[nodiscard]] bool ready() const noexcept;
    [[nodiscard]] const std::filesystem::path&
    control_socket_path() const noexcept;

private:
    class Impl;
    explicit OrderEventDeltaControlServerV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

struct OrderEventDeltaControlClientConfigV1 final {
    std::filesystem::path control_socket_path;
    // Mandatory. The response and server-side request validation must both
    // match this source run and complete frozen daily-catalog identity.
    OrderEventDeltaSourceSessionV1 expected_source_session{};
    // Bounds connect + request + response as one operation.
    std::chrono::milliseconds timeout{
        std::chrono::milliseconds(1000)};
};

// On success, output_read_only_descriptor owns exactly one CLOEXEC O_RDONLY
// descriptor and the caller must close it. The function validates the fixed
// control wire, same-UID server credentials, response source identity,
// descriptor access mode/seals/size, and ACTIVE response state. It does not
// mmap the ring; Connect below performs the complete ring layout/session
// validation. Outputs are reset before any operation and remain reset on
// failure.
[[nodiscard]] OrderEventDeltaControlClientErrorV1
OrderEventDeltaControlGetSessionV1(
    const OrderEventDeltaControlClientConfigV1& config,
    OrderEventDeltaControlSnapshotV1* output_snapshot,
    int* output_read_only_descriptor,
    int* system_error_number = nullptr) noexcept;

// Lightweight router/startup gate. It performs GET_SESSION validation and
// immediately closes the received descriptor without mapping the ring.
[[nodiscard]] OrderEventDeltaControlClientErrorV1
OrderEventDeltaControlProbeV1(
    const OrderEventDeltaControlClientConfigV1& config,
    OrderEventDeltaControlSnapshotV1* output_snapshot,
    int* system_error_number = nullptr) noexcept;

// Data-plane attachment. It consumes the received descriptor through the
// ring reader's full seal/layout/session validator and returns a live reader.
[[nodiscard]] OrderEventDeltaControlClientErrorV1
OrderEventDeltaControlConnectV1(
    const OrderEventDeltaControlClientConfigV1& config,
    OrderEventDeltaControlSnapshotV1* output_snapshot,
    std::unique_ptr<OrderEventDeltaRingReaderV1>* output_reader,
    int* system_error_number = nullptr) noexcept;

// Identical authenticated control attachment with an explicit positive live
// event boundary established by a trusted history checkpoint. ConnectV1 is
// exactly equivalent to ConnectAtV1(..., 1, ...).
[[nodiscard]] OrderEventDeltaControlClientErrorV1
OrderEventDeltaControlConnectAtV1(
    const OrderEventDeltaControlClientConfigV1& config,
    std::uint64_t start_event_sequence,
    OrderEventDeltaControlSnapshotV1* output_snapshot,
    std::unique_ptr<OrderEventDeltaRingReaderV1>* output_reader,
    int* system_error_number = nullptr) noexcept;

}  // namespace l2flow::ipc
