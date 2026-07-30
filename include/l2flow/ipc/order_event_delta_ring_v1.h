#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/ipc/order_event_delta_wire_v1.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>

namespace l2flow::ipc {

struct OrderEventDeltaRingConfigV1 final {
    l2flow::common::Identity128 run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
    std::uint64_t ring_capacity = 262'144U;
    std::uint64_t maximum_mapping_bytes =
        2ULL * 1024ULL * 1024ULL * 1024ULL;
    std::uint64_t producer_started_monotonic_ns = 0U;
};

struct OrderEventDeltaSessionV1 final {
    l2flow::common::Identity128 run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
    std::uint64_t ring_capacity = 0U;
    std::uint64_t total_mapping_bytes = 0U;

    [[nodiscard]] friend bool operator==(
        const OrderEventDeltaSessionV1&,
        const OrderEventDeltaSessionV1&) noexcept = default;
};

enum class OrderEventDeltaRingCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kLayoutOverflow,
    kMappingCreateFailed,
    kReadOnlyHandleFailed,
    kSealFailed,
    kResourceExhausted,
    kUnexpectedFailure,
};

enum class OrderEventDeltaPublishErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kUnavailable,
    kSourceSequenceGap,
    kBatchTooLarge,
    kSequenceExhausted,
    kSlotTagExhausted,
    kFailed,
};

enum class OrderEventDeltaReaderOpenErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidArgument,
    kDescriptorInvalid,
    kDescriptorNotReadOnly,
    kMappingFailed,
    kLayoutInvalid,
    kSessionMismatch,
    kUnavailable,
    kResourceExhausted,
    kUnexpectedFailure,
};

enum class OrderEventDeltaReadErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kUnavailable,
    kLayoutInvalid,
    kInconsistentRead,
    kOverrun,
};

[[nodiscard]] std::string_view OrderEventDeltaRingCreateErrorNameV1(
    OrderEventDeltaRingCreateErrorV1 error) noexcept;
[[nodiscard]] std::string_view OrderEventDeltaPublishErrorNameV1(
    OrderEventDeltaPublishErrorV1 error) noexcept;
[[nodiscard]] std::string_view OrderEventDeltaReaderOpenErrorNameV1(
    OrderEventDeltaReaderOpenErrorV1 error) noexcept;
[[nodiscard]] std::string_view OrderEventDeltaReadErrorNameV1(
    OrderEventDeltaReadErrorV1 error) noexcept;

// Structural validator shared by the producer and reader. expected_trade_date
// and expected_source_tick_sequence must be nonzero. The producer assigns the
// event_delta_sequence, so this function deliberately accepts either zero or
// a nonzero value in that field.
[[nodiscard]] bool OrderEventDeltaPayloadCanonicalV1(
    const OrderEventDeltaPayloadV1& payload,
    std::uint32_t expected_trade_date,
    std::uint64_t expected_source_tick_sequence) noexcept;

class OrderEventDeltaRingProducerV1 final {
public:
    OrderEventDeltaRingProducerV1(
        const OrderEventDeltaRingProducerV1&) = delete;
    OrderEventDeltaRingProducerV1& operator=(
        const OrderEventDeltaRingProducerV1&) = delete;
    OrderEventDeltaRingProducerV1(
        OrderEventDeltaRingProducerV1&&) = delete;
    OrderEventDeltaRingProducerV1& operator=(
        OrderEventDeltaRingProducerV1&&) = delete;
    ~OrderEventDeltaRingProducerV1();

    [[nodiscard]] static OrderEventDeltaRingCreateErrorV1 Create(
        OrderEventDeltaRingConfigV1 config,
        std::unique_ptr<OrderEventDeltaRingProducerV1>* output,
        int* system_error_number = nullptr) noexcept;

    // Exactly one serial owner calls every method on this object.
    //
    // source_tick_sequence must be the immediately following sequence even
    // when events is empty. A batch is fully validated before any mutation.
    // Every event slot is release-published before the complete event prefix
    // is advanced, and that prefix is release-published before the source
    // cursor. Readers therefore cannot observe a partial source-tick batch as
    // part of the public event prefix.
    //
    // A source gap, batch larger than the ring, sequence exhaustion, or slot
    // tag exhaustion permanently fails the producer. V1 intentionally has no
    // reset, catch-up, or recovery operation.
    [[nodiscard]] OrderEventDeltaPublishErrorV1 PublishSourceTick(
        std::uint64_t source_tick_sequence,
        std::span<const OrderEventDeltaPayloadV1> events) noexcept;

    // Returns a new O_RDONLY descriptor. The caller owns it. This is the data
    // plane only; a standalone process must transfer the descriptor and the
    // exact Session value over its authenticated control plane.
    [[nodiscard]] bool DuplicateReadOnlyDescriptor(
        int* output,
        int* system_error_number = nullptr) const noexcept;

    [[nodiscard]] bool UpdateHeartbeat(
        std::uint64_t monotonic_ns) noexcept;
    [[nodiscard]] bool BeginDraining() noexcept;
    [[nodiscard]] bool StopClean() noexcept;
    void MarkFailed() noexcept;

    [[nodiscard]] OrderEventDeltaSessionV1 session() const noexcept;
    [[nodiscard]] std::uint64_t published_event_sequence()
        const noexcept;
    [[nodiscard]] std::uint64_t consumed_source_tick_sequence()
        const noexcept;
    [[nodiscard]] OrderEventDeltaProducerStateV1 state()
        const noexcept;

private:
    class Impl;
    explicit OrderEventDeltaRingProducerV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

struct OrderEventDeltaReadResultV1 final {
    std::size_t written = 0U;
    std::uint64_t next_sequence = 0U;
    // Diagnostic only. On overrun this is the newly observed lower bound; V1
    // does not authorize skipping to it or resuming after loss.
    std::uint64_t observed_sequence = 0U;
    std::uint64_t published_event_sequence = 0U;
    std::uint64_t consumed_source_tick_sequence = 0U;
    // The core exposes, but does not interpret, producer liveness. A
    // standalone control plane must combine this value with its own
    // monotonic clock and process/socket liveness policy.
    std::uint64_t heartbeat_monotonic_ns = 0U;
    std::uint32_t producer_state = 0U;
    std::uint32_t header_flags = 0U;
};

class OrderEventDeltaRingReaderV1 final {
public:
    OrderEventDeltaRingReaderV1(
        const OrderEventDeltaRingReaderV1&) = delete;
    OrderEventDeltaRingReaderV1& operator=(
        const OrderEventDeltaRingReaderV1&) = delete;
    OrderEventDeltaRingReaderV1(
        OrderEventDeltaRingReaderV1&&) = delete;
    OrderEventDeltaRingReaderV1& operator=(
        OrderEventDeltaRingReaderV1&&) = delete;
    ~OrderEventDeltaRingReaderV1();

    // The input descriptor must be O_RDONLY and carry the complete immutable
    // memfd seal set. Open duplicates it, so the caller may close its copy
    // after success. expected_session is mandatory and prevents attaching a
    // cursor to a restarted or wrong-day producer.
    [[nodiscard]] static OrderEventDeltaReaderOpenErrorV1 Open(
        int read_only_descriptor,
        const OrderEventDeltaSessionV1& expected_session,
        std::unique_ptr<OrderEventDeltaRingReaderV1>* output,
        int* system_error_number = nullptr) noexcept;

    // Serial-only and stateful. A new reader starts at sequence 1, and
    // expected_sequence must equal the next_sequence returned by its preceding
    // successful call. A successful zero-row read means the published event
    // prefix has not reached expected_sequence. Overrun never changes
    // next_sequence and is terminal for that live cursor. Reopening also
    // starts at 1, so V1 cannot be used to skip lost history by selecting the
    // current oldest slot.
    [[nodiscard]] OrderEventDeltaReadErrorV1 Read(
        std::uint64_t expected_sequence,
        std::span<OrderEventDeltaPayloadV1> output,
        OrderEventDeltaReadResultV1* result) const noexcept;

    [[nodiscard]] OrderEventDeltaSessionV1 session() const noexcept;
    [[nodiscard]] OrderEventDeltaProducerStateV1 state()
        const noexcept;

private:
    class Impl;
    explicit OrderEventDeltaRingReaderV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::ipc
