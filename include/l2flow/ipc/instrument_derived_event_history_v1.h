#pragma once

#include "l2flow/ipc/instrument_raw_event_history_v2.h"
#include "l2flow/market/shanghai_order_event_aggregator_v1.h"
#include "l2flow/market/shenzhen_order_event_projector_v1.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace l2flow::ipc {

// A derived history event retains the market-specific, lossless analytical
// payload. derived_event_sequence is dense only within this in-memory
// instrument session. It is not a vendor sequence and is never substituted
// for BizIndex or ApplSeqNum.
using InstrumentDerivedEventPayloadV1 = std::variant<
    market::ShanghaiOrderRevisionEventV1,
    market::ShanghaiTradeEventV1,
    market::ShanghaiCancelEventV1,
    market::ShanghaiStatusEventV1,
    market::ShenzhenOrderRevisionEventV1,
    market::ShenzhenTradeEventV1,
    market::ShenzhenCancelEventV1>;

struct InstrumentDerivedEventV1 final {
    std::uint64_t derived_event_sequence = 0U;
    InstrumentDerivedEventPayloadV1 payload{};
    // Stable only within the source tick identified by the payload anchor.
    // This is the zero-based order in which the deterministic market core
    // emitted this event for that tick. Product-local dense sequences are
    // deliberately not used. Source-free Finalize rows keep valid=false.
    std::uint32_t source_tick_event_ordinal = 0U;
    bool source_tick_event_ordinal_valid = false;
};

// This checkpoint is valid only for the live
// InstrumentDerivedEventHistorySessionV1 which returned it. The raw
// checkpoint is independently useful as a retained-Store boundary, but this
// wrapper does not serialize the order-state machine. Consequently a derived
// checkpoint cannot recreate state after process restart and is not a WAL or
// crash-recovery mechanism.
struct InstrumentDerivedEventCheckpointV1 final {
    InstrumentRawEventHistoryCheckpointV2 raw_checkpoint{};
    std::uint64_t derived_event_sequence_exclusive = 1U;
    std::uint64_t order_state_count = 0U;
    std::uint32_t instrument_id = 0U;
    std::uint32_t trade_date = 0U;
    market::MarketV1 market = market::MarketV1::kUnknown;
    bool finalized = false;
};

struct InstrumentDerivedEventHistoryPageV1 final {
    std::vector<InstrumentDerivedEventV1> events;
    std::uint64_t raw_page_index = 0U;
    std::uint64_t cumulative_raw_event_count = 0U;
    std::uint64_t first_ingress_sequence = 0U;
    std::uint64_t last_ingress_sequence = 0U;
    std::uint64_t first_tick_stream_sequence = 0U;
    std::uint64_t last_tick_stream_sequence = 0U;
    bool eof = false;
};

struct InstrumentDerivedEventHistoryConfigV1 final {
    std::string control_socket_path;
    l2flow_shm_session_info_v2 expected_session{};
    std::uint32_t instrument_id = 0U;
    market::MarketV1 market = market::MarketV1::kUnknown;
    std::size_t maximum_order_states = 0U;
    std::uint32_t timeout_ms = 0U;
};

enum class InstrumentDerivedEventHistoryErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kInvalidState,
    kCheckpointMismatch,
    kRawHistoryError,
    kWireProjectionError,
    kAggregationError,
    kResourceExhausted,
    kFailed,
};

[[nodiscard]] std::string_view
InstrumentDerivedEventHistoryErrorNameV1(
    InstrumentDerivedEventHistoryErrorV1 error) noexcept;

class InstrumentDerivedEventHistorySessionV1 final {
public:
    // The session, its current read, and returned page are serial-only.
    // ReadPage owns no background thread. Raw pages are mapped once by the
    // existing reader and reduced synchronously in C++, keeping the generation
    // publication interval plus one page-reduction call as the latency bound.
    InstrumentDerivedEventHistorySessionV1(
        const InstrumentDerivedEventHistorySessionV1&) = delete;
    InstrumentDerivedEventHistorySessionV1& operator=(
        const InstrumentDerivedEventHistorySessionV1&) = delete;
    InstrumentDerivedEventHistorySessionV1(
        InstrumentDerivedEventHistorySessionV1&&) = delete;
    InstrumentDerivedEventHistorySessionV1& operator=(
        InstrumentDerivedEventHistorySessionV1&&) = delete;
    ~InstrumentDerivedEventHistorySessionV1();

    [[nodiscard]] static InstrumentDerivedEventHistoryErrorV1 Create(
        InstrumentDerivedEventHistoryConfigV1 config,
        std::unique_ptr<InstrumentDerivedEventHistorySessionV1>* output)
        noexcept;

    // Starts [retained origin, pinned target]. Full may be opened only on a
    // pristine session because its replay constructs the authoritative order
    // state used by every later update.
    [[nodiscard]] InstrumentDerivedEventHistoryErrorV1 BeginFull(
        std::uint64_t expected_generation,
        std::uint32_t requested_raw_page_records) noexcept;

    // Starts (base.raw_checkpoint, pinned target]. base must be the exact
    // checkpoint returned by the preceding explicit EOF on this same object.
    // This preserves T -> A and every other lifecycle transition across
    // immutable-generation boundaries without replaying the full day.
    [[nodiscard]] InstrumentDerivedEventHistoryErrorV1 BeginUpdate(
        const InstrumentDerivedEventCheckpointV1& base,
        std::uint64_t expected_generation,
        std::uint32_t requested_raw_page_records) noexcept;

    // Reduces at most one raw immutable page. The output vector is reused and
    // replaced on every call. A raw row can emit more than one derived row
    // (for example one trade plus two order revisions), so this is an owned
    // vector rather than a borrowed fixed-stride view.
    //
    // EOF is a separate successful empty page. Only after that call does
    // VerifiedCheckpoint succeed. Closing/destroying with an unfinished read,
    // or any projection/aggregation failure after mutation, fail-closes this
    // stateful session because the order-state machines are intentionally not
    // copied for rollback.
    [[nodiscard]] InstrumentDerivedEventHistoryErrorV1 ReadPage(
        InstrumentDerivedEventHistoryPageV1* output) noexcept;

    [[nodiscard]] InstrumentDerivedEventHistoryErrorV1
    VerifiedCheckpoint(
        InstrumentDerivedEventCheckpointV1* output) const noexcept;

    // Source-free finalization is explicit and is never triggered by an
    // immutable-generation boundary, lunch break, or wall clock. Call this
    // only at a clean trading-day boundary. It permanently seals the session
    // and returns the final order revisions; it does not advance the raw
    // checkpoint.
    [[nodiscard]] InstrumentDerivedEventHistoryErrorV1
    FinalizeTradingDay(
        std::vector<InstrumentDerivedEventV1>* output) noexcept;

    [[nodiscard]] bool read_active() const noexcept;
    [[nodiscard]] bool failed() const noexcept;
    [[nodiscard]] bool finalized() const noexcept;
    [[nodiscard]] std::uint64_t next_derived_event_sequence()
        const noexcept;
    [[nodiscard]] std::size_t order_state_count() const noexcept;
    [[nodiscard]] InstrumentRawEventHistoryErrorV2
    last_raw_error() const noexcept;
    [[nodiscard]] const InstrumentDerivedEventHistoryConfigV1&
    config() const noexcept;

private:
    class Impl;
    explicit InstrumentDerivedEventHistorySessionV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::ipc
