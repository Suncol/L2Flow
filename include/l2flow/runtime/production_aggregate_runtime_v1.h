#pragma once

#include "l2flow/market/instrument_history_v1.h"
#include "l2flow/route/production_route_controller_v1.h"
#include "l2flow/runtime/production_source_pipeline_v1.h"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::runtime {

inline constexpr std::size_t kProductionAggregateSourceCountV1 =
    l2flow::route::kProductionRouteSourceCountV1;
static_assert(
    kProductionAggregateSourceCountV1 ==
    l2flow::market::kInstrumentHistorySourceCountV1);

// Stable, nonzero route-revocation reasons.  Values are part of the
// operational contract and must not be renumbered within V1.
inline constexpr std::uint32_t
    kProductionAggregateFatalSourceFailureV1 = 0x4C324101U;
inline constexpr std::uint32_t
    kProductionAggregateFatalActiveSourceEndV1 = 0x4C324102U;
inline constexpr std::uint32_t
    kProductionAggregateFatalInternalFailureV1 = 0x4C324103U;
inline constexpr std::uint32_t
    kProductionAggregateFatalStoppedActiveV1 = 0x4C324104U;
inline constexpr std::chrono::nanoseconds
    kProductionAggregateMaximumSourceFrontierBusyTimeoutV1 =
        std::chrono::seconds{10};

struct ProductionAggregateRuntimeConfigV1 final {
    // Timed condition-variable waits keep an idle Raw tail, a full history
    // queue and failed route persistence from busy-spinning.  These wait
    // durations and active_validation_interval must be in (0, 100 ms].
    std::chrono::nanoseconds idle_wait = std::chrono::microseconds{200};
    std::chrono::nanoseconds history_backpressure_wait =
        std::chrono::microseconds{50};
    std::chrono::nanoseconds route_retry_wait =
        std::chrono::milliseconds{10};
    // The allocation-free local validity subset is checked after every Step.
    // A fresh Raw control read is bounded by this cadence to keep it off the
    // per-market-record hot path.  This is not a writer-heartbeat age budget.
    std::chrono::nanoseconds active_validation_interval =
        std::chrono::milliseconds{1};
    // Maximum age of the Raw writer's host CLOCK_MONOTONIC heartbeat.  A
    // custom writer clock must preserve that epoch.  This must match the
    // producer liveness contract and must be positive.  The
    // first scheduled validation after this budget expires revokes Active;
    // host scheduling can delay that observation.
    std::chrono::nanoseconds writer_heartbeat_timeout =
        std::chrono::seconds{30};
    // A progress-page writer may be descheduled while holding its seqlock.
    // kBusy is transient until it remains continuous for this duration.
    // Must be in (0, 10 s].
    std::chrono::nanoseconds source_frontier_busy_timeout =
        std::chrono::milliseconds{100};
};

// Exact per-source evidence used to bind the aggregate to the immutable
// active route.  It deliberately contains four independent source frontiers;
// their positions do not imply a cross-source total order.
struct ProductionAggregateRouteBindingV1 final {
    std::array<ProductionSourcePipelineConfigV1,
               kProductionAggregateSourceCountV1>
        source_configs{};
    std::array<l2flow::canonical::SourceFrontierV1,
               kProductionAggregateSourceCountV1>
        source_frontiers{};
    std::array<l2flow::market::InstrumentHistorySourceFrontierV1,
               kProductionAggregateSourceCountV1>
        history_frontiers{};
    std::array<ProductionInstrumentRegistryIdentityV1,
               kProductionAggregateSourceCountV1>
        registry_identities{};
};

enum class ProductionAggregateRouteBindingErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidManifest,
    kTradeDateMismatch,
    kSourceSlotMismatch,
    kSourceIdentityMismatch,
    kClockIdentityMismatch,
    kSchemaIdentityMismatch,
    kRegistryIdentityMismatch,
    kSourceFatal,
    kHistoryFatal,
};

[[nodiscard]] std::string_view
ProductionAggregateRouteBindingErrorNameV1(
    ProductionAggregateRouteBindingErrorV1 error) noexcept;

[[nodiscard]] ProductionAggregateRouteBindingErrorV1
ValidateProductionAggregateRouteBindingV1(
    const l2flow::route::ProductionRouteManifestV1& active_manifest,
    const ProductionAggregateRouteBindingV1& binding) noexcept;

// Minimum source-local production activation contract.  It is deliberately
// narrower than ControlReadinessGateV1: the pure evaluator below validates a
// coherent Raw sample and nonzero heartbeat but has no clock input.  The
// aggregate additionally enforces writer_heartbeat_timeout against
// CLOCK_MONOTONIC before publication and throughout Active operation.
// Decoder-lag and durability-lag budgets remain outside this V1 contract.
enum class ProductionSourceActivationGateErrorV1 : std::uint8_t {
    kNone = 0U,
    kEvidenceUnavailable,
    kPipelineFatal,
    kSourceEnded,
    kHistoryAdmissionPending,
    kHistoryDrainPending,
    kControlIdentityMismatch,
    kControlPoisoned,
    kControlEvidenceIncomplete,
    kRawControlUnavailable,
    kRawControlInvalid,
    kRawControlFatal,
    kRawHeartbeatMissing,
    kRawHeartbeatRegression,
    kRawHeartbeatClockUnavailable,
    kRawHeartbeatClockRegression,
    kRawHeartbeatTimedOut,
    kSourceFrontierUnavailable,
    kSourceFrontierBusy,
    kSourceIdentityMismatch,
    kClockIdentityMismatch,
    kSourceCursorMismatch,
    kSourceNotHealthy,
    kHistoryFatal,
    kHistoryBarrierInvalid,
    kHistoryBarrierPending,
    kRawDurabilityAnchorPending,
    kCanonicalProcessedAnchorPending,
};

[[nodiscard]] std::string_view ProductionSourceActivationGateErrorNameV1(
    ProductionSourceActivationGateErrorV1 error) noexcept;

struct ProductionSourceActivationGateResultV1 final {
    ProductionSourceActivationGateErrorV1 error =
        ProductionSourceActivationGateErrorV1::kNone;
    // Intrinsic integrity/Fatal failure.  An Active route treats every
    // non-ready result as fail-stop, including readiness loss.
    bool fail_stop = false;
    // The Raw tail has ended or is irreversibly draining its end barrier.
    bool source_ended = false;

    [[nodiscard]] bool ready() const noexcept {
        return error == ProductionSourceActivationGateErrorV1::kNone;
    }
};

[[nodiscard]] ProductionSourceActivationGateResultV1
EvaluateProductionSourceActivationGateV1(
    const l2flow::route::ProductionRouteSourceV1& route_source,
    const ProductionSourcePipelineConfigV1& config,
    const ProductionSourceActivationEvidenceV1& evidence,
    l2flow::market::InstrumentHistoryBarrierWaitErrorV1
        barrier_wait_error) noexcept;

// Allocation-free post-publication form with a fresh Raw sample.  The
// aggregate evaluates it at active_validation_interval (and immediately
// after publication); any non-ready result revokes the Active route.
[[nodiscard]] ProductionSourceActivationGateResultV1
EvaluateProductionSourceActiveValidityV1(
    const l2flow::route::ProductionRouteSourceV1& route_source,
    const ProductionSourcePipelineConfigV1& config,
    const ProductionSourceActiveValidityEvidenceV1& evidence) noexcept;

// Raw-free local subset evaluated after every source Step so a committed
// disconnect/control-readiness loss is revoked without cadence delay.
[[nodiscard]] ProductionSourceActivationGateResultV1
EvaluateProductionSourceActiveLocalValidityV1(
    const l2flow::route::ProductionRouteSourceV1& route_source,
    const ProductionSourcePipelineConfigV1& config,
    const ProductionSourceActiveLocalEvidenceV1& evidence) noexcept;

enum class ProductionAggregateCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kNullDependency,
    kInvalidConfiguration,
    kSourceSlotMismatch,
    kSourceIdentityMismatch,
    kHistoryRuntimeMismatch,
    kTradeDateMismatch,
    kRouteManifestInvalid,
    kRouteManifestMismatch,
    kSourceFatal,
    kHistoryFatal,
    kResourceExhausted,
    kThreadStartFailed,
};

[[nodiscard]] std::string_view ProductionAggregateCreateErrorNameV1(
    ProductionAggregateCreateErrorV1 error) noexcept;

enum class ProductionAggregateStateV1 : std::uint8_t {
    kRunning = 0U,
    kDraining,
    kStopping,
    kStopped,
    kFatal,
};

enum class ProductionAggregateBeginDrainErrorV1 : std::uint8_t {
    kNone = 0U,
    kReentrantCall,
    kNotRunning,
    kFatal,
};

[[nodiscard]] std::string_view ProductionAggregateBeginDrainErrorNameV1(
    ProductionAggregateBeginDrainErrorV1 error) noexcept;

struct ProductionAggregateBeginDrainResultV1 final {
    ProductionAggregateBeginDrainErrorV1 error =
        ProductionAggregateBeginDrainErrorV1::kNone;
    l2flow::route::ProductionRouteControllerResultV1 route{};
    bool already_draining = false;
    bool route_revocation_required = false;

    [[nodiscard]] bool ok() const noexcept {
        return error == ProductionAggregateBeginDrainErrorV1::kNone &&
               (!route_revocation_required || route.ok());
    }
};

enum class ProductionAggregateDrainWaitErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidTimeout,
    kNotDraining,
    kTimeout,
    kSourceFatal,
    kFatal,
    kQuiescedBeforeEnd,
};

[[nodiscard]] std::string_view ProductionAggregateDrainWaitErrorNameV1(
    ProductionAggregateDrainWaitErrorV1 error) noexcept;

struct ProductionAggregateDrainWaitResultV1 final {
    ProductionAggregateDrainWaitErrorV1 error =
        ProductionAggregateDrainWaitErrorV1::kNone;
    // Exact first source that reported kSourceFatal; UINT8_MAX otherwise.
    std::uint8_t source_slot =
        std::numeric_limits<std::uint8_t>::max();

    [[nodiscard]] bool ok() const noexcept {
        return error == ProductionAggregateDrainWaitErrorV1::kNone;
    }
};

struct ProductionAggregateSnapshotV1 final {
    ProductionAggregateStateV1 state =
        ProductionAggregateStateV1::kStopped;
    std::array<ProductionSourceSnapshotV1,
               kProductionAggregateSourceCountV1>
        sources{};
    std::array<bool, kProductionAggregateSourceCountV1> worker_exited{};
    bool active_route_published = false;
    // A store call crossed rename but could not prove the final route.  The
    // runtime is fail-stopped and retries only the identical Active bytes
    // before publishing its Fatal successor.
    bool active_route_publication_uncertain = false;
    bool fatal_route_published = false;
    // True after BeginDrain has proved either that no Active route exists or
    // that its durable Fatal successor was accepted.
    bool drain_route_revoked = false;
    // True only when Snapshot() is called re-entrantly by the route store
    // operation hook.  In that case last_*_publish are intentionally omitted
    // because the hook already runs while the publication mutex is held.
    bool route_operation_hook_active = false;
    std::array<bool, kProductionAggregateSourceCountV1>
        source_frontier_busy{};
    std::uint32_t fatal_reason_code = 0U;
    l2flow::route::ProductionRouteControllerResultV1
        last_active_publish{};
    l2flow::route::ProductionRouteControllerResultV1
        last_fatal_publish{};
};

// This is evidence, not a READY verdict.  Sources advance independently while
// it is copied; the barriers are the exact history submissions visible at the
// end of each per-source sample.  A caller may wait for all four barriers and
// apply its own control/readiness policy before explicitly calling
// PublishActive().
struct ProductionAggregateActivationEvidenceV1 final {
    ProductionAggregateStateV1 state =
        ProductionAggregateStateV1::kStopped;
    std::array<ProductionSourceActivationEvidenceV1,
               kProductionAggregateSourceCountV1>
        sources{};
    bool active_route_published = false;
    std::uint32_t fatal_reason_code = 0U;
    // Re-entrant source/route operation hooks receive metadata only;
    // per-source evidence is intentionally suppressed to avoid self-locking
    // and to preserve the route/source lock order.
    bool evidence_suppressed_by_operation_hook = false;
};

enum class ProductionAggregateBarrierErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidTimeout,
    kInvalidBarrier,
    kTimeout,
    kSourceFatal,
};

[[nodiscard]] std::string_view ProductionAggregateBarrierErrorNameV1(
    ProductionAggregateBarrierErrorV1 error) noexcept;

struct ProductionAggregateBarrierResultV1 final {
    ProductionAggregateBarrierErrorV1 error =
        ProductionAggregateBarrierErrorV1::kNone;
    // Set to the failing slot; UINT8_MAX on success or a timeout-argument
    // failure that is not attributable to one source.
    std::uint8_t source_slot =
        std::numeric_limits<std::uint8_t>::max();
    l2flow::market::InstrumentHistoryBarrierWaitErrorV1 history_error =
        l2flow::market::InstrumentHistoryBarrierWaitErrorV1::kNone;

    [[nodiscard]] bool ok() const noexcept {
        return error == ProductionAggregateBarrierErrorV1::kNone;
    }
};

enum class ProductionAggregatePublishErrorV1 : std::uint8_t {
    kNone = 0U,
    kRouteControllerUnavailable,
    kReentrantCall,
    kNotRunning,
    kSourceEnded,
    kReadinessIncomplete,
    kSourceBindingMismatch,
    kSourceFatal,
    kHistoryFatal,
    kFatal,
    kRouteFailure,
    // The active publication linearized, but a concurrent source failure,
    // source end, or orderly stop required immediate fatal-route publication.
    kBecameFatal,
};

[[nodiscard]] std::string_view ProductionAggregatePublishErrorNameV1(
    ProductionAggregatePublishErrorV1 error) noexcept;

struct ProductionAggregatePublishResultV1 final {
    ProductionAggregatePublishErrorV1 error =
        ProductionAggregatePublishErrorV1::kNone;
    l2flow::route::ProductionRouteControllerResultV1 route{};
    bool already_active = false;
    ProductionSourceActivationGateErrorV1 activation_error =
        ProductionSourceActivationGateErrorV1::kNone;
    std::uint8_t source_slot =
        std::numeric_limits<std::uint8_t>::max();

    [[nodiscard]] bool ok() const noexcept {
        return error == ProductionAggregatePublishErrorV1::kNone &&
               route.authoritative();
    }
};

// Owns exactly four source-order pipelines and one thread per source.  The
// InstrumentHistoryRuntimeV1 and optional route controller are borrowed and
// must outlive this object.  Create() verifies that all four pipelines were
// created with that exact history runtime.  While
// the aggregate exists, route publication must go through this object rather
// than directly through the borrowed controller, so its lifecycle snapshot
// remains complete.  Create() binds its immutable active manifest to the
// pipelines' trade date, schema, registry and exact source/Canonical
// identities/generations; every
// PublishActive() revalidates those identities, applies the minimum
// production activation contract twice around zero-time history-barrier
// checks, and only then touches the route.  Once Active, each source thread
// continuously enforces the allocation-free subset after every Step.  Route
// build_sha256/config_sha256 and endpoint ownership are deployment inputs to
// the controller; V1 has no pipeline accessor from which to derive them, so
// their correctness remains an explicit deployment construction invariant.
//
// Clean shutdown is deliberately split at the external Raw-producer
// boundary: BeginDrain() first closes/revokes the route while all source loops
// keep consuming; the owner then quiesces the four Raw producers and calls
// WaitForDrain(), which succeeds only after every pipeline has observed Raw
// End and acknowledged its exact history barrier.  Stop() then joins the
// already-drained workers.  Direct Stop() remains available: it performs the
// same fail-closed route revocation, then immediately stops local admission
// and joins after retrying any already-committed pending history envelope.
// The borrowed history runtime is never stopped here.  Its external owner
// calls StopAndDrain() only after this aggregate has stopped.  A permanent
// route-storage failure blocks BeginDrain()/Stop() while Snapshot() exposes
// the last failure to another thread.  Process death cannot run this protocol;
// consumers must also enforce the live SourceFrontier/heartbeat contract.
class ProductionAggregateRuntimeV1 final {
public:
    ProductionAggregateRuntimeV1(
        const ProductionAggregateRuntimeV1&) = delete;
    ProductionAggregateRuntimeV1& operator=(
        const ProductionAggregateRuntimeV1&) = delete;
    ProductionAggregateRuntimeV1(
        ProductionAggregateRuntimeV1&&) = delete;
    ProductionAggregateRuntimeV1& operator=(
        ProductionAggregateRuntimeV1&&) = delete;
    ~ProductionAggregateRuntimeV1();

    [[nodiscard]] static ProductionAggregateCreateErrorV1 Create(
        ProductionAggregateRuntimeConfigV1 config,
        std::array<std::unique_ptr<ProductionSourcePipelineV1>,
                   kProductionAggregateSourceCountV1>
            sources,
        l2flow::market::InstrumentHistoryRuntimeV1* history,
        l2flow::route::ProductionRouteControllerV1* route_controller,
        std::unique_ptr<ProductionAggregateRuntimeV1>* output) noexcept;

    [[nodiscard]] ProductionAggregateSnapshotV1 Snapshot() const noexcept;

    // Phase 1 of clean shutdown.  It atomically closes the Active-publication
    // gate, transitions Running -> Draining, and (when Active may exist)
    // retries with route_retry_wait until the durable Fatal successor is
    // accepted.  Source loops continue consuming while this call runs and
    // after it returns.  A permanent route-store failure intentionally blocks.
    [[nodiscard]] ProductionAggregateBeginDrainResultV1
    BeginDrain() noexcept;

    // Phase 2 observation.  The service calls this after quiescing all four
    // Raw producers.  Success proves every source pipeline returned End;
    // ProductionSourcePipelineV1 returns End only after its exact history
    // barrier is acknowledged.  timeout is one aggregate deadline.
    [[nodiscard]] ProductionAggregateDrainWaitResultV1 WaitForDrain(
        std::chrono::nanoseconds timeout) const noexcept;

    // May allocate while copying decoder subscription state and therefore is
    // intentionally not noexcept.  A re-entrant source/route operation hook
    // receives metadata-only evidence with the suppression flag set.
    [[nodiscard]] ProductionAggregateActivationEvidenceV1
    CaptureActivationEvidence() const;

    // timeout is one aggregate deadline, not a per-source timeout.
    [[nodiscard]] ProductionAggregateBarrierResultV1 WaitForHistoryBarriers(
        const ProductionAggregateActivationEvidenceV1& evidence,
        std::chrono::nanoseconds timeout) const noexcept;

    // Publication occurs only through this explicit call.  It evaluates the
    // documented minimum contract, not full ControlReadinessGateV1 READY.
    // The supplied operation hook may inspect external state, but calls back
    // into this aggregate's PublishActive()/Stop() are rejected.  Snapshot()
    // remains nonblocking in that hook and marks its route result fields as
    // suppressed.
    [[nodiscard]] ProductionAggregatePublishResultV1 PublishActive(
        const l2flow::route::ProductionRouteStoreOptionsV1& options = {},
        std::string* diagnostic = nullptr) noexcept;

    // Idempotent and safe to call concurrently.  Clean shutdown is
    // BeginDrain -> externally quiesce four Raw producers -> WaitForDrain ->
    // Stop.  Direct Stop remains an emergency/convenience path: it first
    // performs the same durable route revocation, then immediately requests
    // local source quiescence and joins without requiring external Raw End. It
    // is an external-owner operation and must not be called re-entrantly from
    // a source pipeline hook or route-store operation hook.  Such a call is
    // detected, latches aggregate fail-stop without consuming the external
    // Stop(), and returns without attempting a self-join or self-lock.
    void Stop() noexcept;

private:
    class Impl;
    explicit ProductionAggregateRuntimeV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::runtime
