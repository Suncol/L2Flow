#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/ipc/certified_order_event_history_v1.h"
#include "l2flow/ipc/realtime_certified_wire_v1.h"
#include "l2flow/market/daily_instrument_catalog_v2.h"
#include "l2flow/market/realtime_history_v1.h"
#include "l2flow/realtime/native_sequence_observer_v1.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string_view>

namespace l2flow::ipc {

inline constexpr std::uint64_t
    kRealtimeCertifiedDefaultQueueCapacityV1 = 262'144U;
inline constexpr std::uint64_t
    kRealtimeCertifiedDefaultPendingEntriesV1 = 65'536U;
inline constexpr std::uint64_t
    kRealtimeCertifiedDefaultDuplicateRetentionV1 = 65'536U;
inline constexpr std::uint64_t
    kRealtimeCertifiedDefaultReorderSpanV1 = 1'048'576U;
inline constexpr std::uint32_t
    kRealtimeCertifiedDefaultChannelCapacityV1 = 4096U;

// Minimal same-UID fd handoff protocol for the independent CERTIFIED
// sidecar. It is deliberately not an extension of Realtime Control V2.
inline constexpr std::uint64_t
    kRealtimeCertifiedControlRequestMagicV1 =
        0x315145524346324cULL;  // "L2FCREQ1" little-endian
inline constexpr std::uint64_t
    kRealtimeCertifiedControlResponseMagicV1 =
        0x315053524346324cULL;  // "L2FCRSP1" little-endian

enum class RealtimeCertifiedControlOpcodeV1 : std::uint16_t {
    kGetSession = 1U,
    // Additive request. The original kGetSession response and descriptor
    // remain byte-for-byte unchanged.
    kGetEventHistory = 2U,
};

enum class RealtimeCertifiedControlStatusV1 : std::uint16_t {
    kOk = 0U,
    kInvalidRequest = 1U,
    kUnavailable = 2U,
    kInternal = 3U,
};

struct RealtimeCertifiedControlRequestV1 final {
    std::uint64_t magic = kRealtimeCertifiedControlRequestMagicV1;
    std::uint16_t abi_major = kRealtimeCertifiedWireMajorV1;
    std::uint16_t abi_minor = kRealtimeCertifiedWireMinorV1;
    std::uint16_t opcode = static_cast<std::uint16_t>(
        RealtimeCertifiedControlOpcodeV1::kGetSession);
    std::uint16_t request_bytes = sizeof(RealtimeCertifiedControlRequestV1);
    std::uint64_t nonce = 0U;
    std::uint64_t reserved = 0U;
};
static_assert(sizeof(RealtimeCertifiedControlRequestV1) == 32U);

struct RealtimeCertifiedControlResponseV1 final {
    std::uint64_t magic = kRealtimeCertifiedControlResponseMagicV1;
    std::uint16_t abi_major = kRealtimeCertifiedWireMajorV1;
    std::uint16_t abi_minor = kRealtimeCertifiedWireMinorV1;
    std::uint16_t status = 0U;
    std::uint16_t response_bytes =
        sizeof(RealtimeCertifiedControlResponseV1);
    std::uint64_t nonce = 0U;
    std::uint64_t mapping_bytes = 0U;
    std::array<std::uint8_t, 16U> run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
    std::uint32_t reserved0 = 0U;
    std::array<std::uint8_t, 64U> reserved{};
};
static_assert(sizeof(RealtimeCertifiedControlResponseV1) == 128U);

struct RealtimeCertifiedServiceConfigV1 final {
    l2flow::common::Identity128 run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
    std::shared_ptr<
        const l2flow::market::DailyInstrumentCatalogV2>
        daily_catalog;

    // Required FAST sink. PublishApplied always invokes this first. Once FAST
    // succeeds, every certification error is absorbed and represented only in
    // CERTIFIED state.
    std::shared_ptr<l2flow::market::RealtimeAppliedRecordSinkV1>
        fast_sink;

    std::uint64_t certified_tick_ring_capacity = 262'144U;
    std::uint32_t channel_capacity =
        kRealtimeCertifiedDefaultChannelCapacityV1;
    std::uint64_t handoff_queue_capacity =
        kRealtimeCertifiedDefaultQueueCapacityV1;
    std::uint64_t maximum_pending_entries =
        kRealtimeCertifiedDefaultPendingEntriesV1;
    std::uint64_t maximum_pending_entries_per_channel = 16'384U;
    std::uint64_t certified_duplicate_retention_entries =
        kRealtimeCertifiedDefaultDuplicateRetentionV1;
    std::uint64_t maximum_reorder_span =
        kRealtimeCertifiedDefaultReorderSpanV1;
    std::uint64_t maximum_mapping_bytes =
        2ULL * 1024ULL * 1024ULL * 1024ULL;

    // The Event projector is part of the same certified commit. Production
    // exposes an independent append-only full-day wire journal; the legacy
    // in-process immutable-generation view remains available and is gated to
    // the same last Tick frontier already published to external readers.
    std::size_t maximum_order_states = 1'000'000U;
    std::size_t maximum_derived_events = 16'000'000U;
    // Zero derives the exact checked full-day wire size from
    // maximum_derived_events. A nonzero value is an additional operator cap.
    std::uint64_t maximum_derived_event_mapping_bytes = 0U;
    std::uint64_t derived_event_lazy_commit_chunk_bytes =
        64ULL * 1024ULL * 1024ULL;

    // Absolute path below an operator-owned directory. Create never unlinks a
    // pre-existing path.
    std::filesystem::path control_socket_path;
};

enum class RealtimeCertifiedServiceCreateErrorV1 : std::uint8_t {
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
    kRecoveryCreateFailed,
    kEventProjectorCreateFailed,
    kResourceExhausted,
    kUnexpectedFailure,
};

[[nodiscard]] std::string_view
RealtimeCertifiedServiceCreateErrorNameV1(
    RealtimeCertifiedServiceCreateErrorV1 error) noexcept;

struct RealtimeCertifiedServiceSnapshotV1 final {
    RealtimeCertifiedStateV1 state =
        RealtimeCertifiedStateV1::kNoData;
    std::uint64_t canonical_apply_frontier = 0U;
    std::uint64_t correction_epoch = 0U;
    std::uint64_t observed_native_message_count = 0U;
    std::uint64_t certified_tick_count = 0U;
    std::uint64_t exact_duplicate_message_count = 0U;
    std::uint64_t gap_opened_count = 0U;
    std::uint64_t gap_recovered_count = 0U;
    std::uint64_t conflicting_duplicate_count = 0U;
    std::uint64_t resource_exhaustion_count = 0U;
    std::uint64_t pending_token_count = 0U;
    std::uint64_t enqueued_observations = 0U;
    std::uint64_t enqueued_applied_records = 0U;
    std::uint64_t processed_handoffs = 0U;
    std::uint64_t dropped_handoffs = 0U;
    std::uint32_t channel_count = 0U;
    std::uint32_t gap_open_channel_count = 0U;
    std::uint32_t catching_up_channel_count = 0U;
    std::uint32_t frozen_channel_count = 0U;
    bool worker_running = false;
    bool globally_frozen_resource = false;
    // False means the five bounded seqcount attempts all raced a writer. The
    // counter-only fields below remain current, but callers must not treat
    // the wire-derived state/frontiers as one coherent snapshot.
    bool wire_snapshot_consistent = false;
};

// Default-on production composition for native-gap recovery. It is both the
// pipeline's optional observation tap and its applied-record sink wrapper.
// Reader work never enters either producer method.
class RealtimeCertifiedMarketServiceV1 final
    : public l2flow::market::RealtimeAppliedRecordSinkV1,
      public l2flow::realtime::NativeSequenceObservationSinkV1 {
public:
    RealtimeCertifiedMarketServiceV1(
        const RealtimeCertifiedMarketServiceV1&) = delete;
    RealtimeCertifiedMarketServiceV1& operator=(
        const RealtimeCertifiedMarketServiceV1&) = delete;
    RealtimeCertifiedMarketServiceV1(
        RealtimeCertifiedMarketServiceV1&&) = delete;
    RealtimeCertifiedMarketServiceV1& operator=(
        RealtimeCertifiedMarketServiceV1&&) = delete;
    ~RealtimeCertifiedMarketServiceV1() override;

    [[nodiscard]] static RealtimeCertifiedServiceCreateErrorV1 Create(
        RealtimeCertifiedServiceConfigV1 config,
        std::shared_ptr<RealtimeCertifiedMarketServiceV1>* output,
        int* system_error_number = nullptr) noexcept;

    [[nodiscard]] bool Start(
        int* system_error_number = nullptr) noexcept;

    // Two-phase startup is used by CSV intraday recovery: the worker must
    // consume the recovered prefix while the query control socket remains
    // unavailable. Start() is exactly StartWorker() followed by
    // StartControl(), preserving the ordinary live-from-open API.
    [[nodiscard]] bool StartWorker(
        int* system_error_number = nullptr) noexcept;
    [[nodiscard]] bool StartControl(
        int* system_error_number = nullptr) noexcept;
    // Enqueues a FIFO worker barrier, waits until all handoffs before it have
    // been projected and their headers committed, and only then starts the
    // query control thread. Later live handoffs do not extend this barrier.
    // timeout must be positive and no greater than 24 hours.
    [[nodiscard]] bool ActivateControlAfterPrefix(
        std::chrono::milliseconds timeout,
        int* system_error_number = nullptr) noexcept;

    // Required FAST publication happens first. A false return can therefore
    // mean only that FAST itself failed. Queue pressure, conflicts, gaps, and
    // every other certification failure return true after FAST succeeds.
    [[nodiscard]] bool PublishApplied(
        std::size_t ordinal,
        const l2flow::market::RealtimeHistoryRecordV1& record)
        noexcept override;
    void MarkCoverageLost() noexcept override;
    void QuiesceRecordReferences() noexcept override;

    void ObserveNativeSequence(
        const l2flow::realtime::NativeSequenceObservationV1&
            observation) noexcept override;
    void MarkNativeSequenceObservationFailure(
        l2flow::realtime::NativeSequenceObservationFailureV1 failure,
        const l2flow::sdk::MessageKey& message_key) noexcept override;

    void MarkDraining() noexcept;
    void MarkStoppedClean() noexcept;
    void StopControl() noexcept;

    [[nodiscard]] RealtimeCertifiedServiceSnapshotV1 Snapshot()
        const noexcept;
    [[nodiscard]] bool ReadLatestForTest(
        std::size_t ordinal,
        RealtimeCertifiedTickEnvelopeV1* output) const noexcept;
    [[nodiscard]] bool ReadCanonicalForTest(
        std::uint64_t canonical_apply_sequence,
        RealtimeCertifiedTickEnvelopeV1* output) const noexcept;
    [[nodiscard]] bool DuplicateReadOnlyDescriptorForTest(
        int* output_fd) const noexcept;
    [[nodiscard]] bool WaitUntilIdleForTest(
        std::chrono::milliseconds timeout) const noexcept;

    // Legacy in-process mirror. It never returns the projector's physically
    // staged N generation while the public Tick header is still at N-1.
    [[nodiscard]] CertifiedOrderEventHistoryErrorV1
    AcquireEventGeneration(
        CertifiedOrderEventHistorySnapshotV1* output) const noexcept;

    [[nodiscard]] std::uint64_t mapping_bytes() const noexcept;
    [[nodiscard]] std::uint64_t handoff_queue_capacity() const noexcept;
    [[nodiscard]] const std::filesystem::path&
    control_socket_path() const noexcept;

private:
    class Impl;
    explicit RealtimeCertifiedMarketServiceV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::ipc
