#pragma once

#include "l2flow/ingress/raw_production_runtime.h"
#include "l2flow/market/instrument_history_v1.h"
#include "l2flow/market/instrument_registry.h"
#include "l2flow/route/production_route_controller_v1.h"
#include "l2flow/runtime/production_aggregate_runtime_v1.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::apps {

inline constexpr std::size_t kProductionServiceSourceCountV1 =
    l2flow::runtime::kProductionAggregateSourceCountV1;

// Immutable capture-side identity retained for the complete runtime
// lifetime.  ProductionServiceV1 compares it with the source pipeline,
// SourceFrontier page and route manifest before the first SDK Connect.
// source_frontier is an identity-only borrowed pointer, never an ownership or
// mutation capability.
struct ProductionCaptureBindingV1 final {
    std::uint8_t source_slot = UINT8_MAX;
    l2flow::sdk::IngressKind ingress_kind =
        l2flow::sdk::IngressKind::ShSnapshot;
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    l2flow::common::Identity128 stream_day_id{};
    l2flow::common::Identity128 writer_instance{};
    std::uint64_t source_generation = 0U;
    const l2flow::canonical::SourceFrontierPageV1* source_frontier = nullptr;

    [[nodiscard]] friend constexpr bool operator==(
        const ProductionCaptureBindingV1&,
        const ProductionCaptureBindingV1&) noexcept = default;
};

// Allocation-free composition predicate used by Create() before ownership is
// transferred to worker threads.  expected_source_slot is the fixed position
// in the four-source route array.
[[nodiscard]] bool ProductionCaptureBindingMatchesV1(
    const ProductionCaptureBindingV1& capture,
    std::uint8_t expected_source_slot,
    const l2flow::runtime::ProductionSourcePipelineConfigV1& pipeline,
    const l2flow::route::ProductionRouteSourceV1& route_source,
    const l2flow::canonical::SourceFrontierV1& frontier,
    const l2flow::canonical::SourceFrontierPageV1* pipeline_frontier_page)
    noexcept;

// Lifecycle-only type erasure keeps the service composition testable and
// permits a deployment builder to prepare all four Raw runtimes before any
// SDK Connect.  It is never used on the market-data hot path.
class ProductionCaptureRuntimeV1 {
public:
    virtual ~ProductionCaptureRuntimeV1() = default;
    // The returned object must retain the same value and address until this
    // runtime is destroyed.
    [[nodiscard]] virtual const ProductionCaptureBindingV1& binding()
        const noexcept = 0;
    [[nodiscard]] virtual bool Start(std::string* diagnostic) noexcept = 0;
    [[nodiscard]] virtual bool Stop(std::string* diagnostic) noexcept = 0;
};

// Takes ownership of one concrete Raw runtime.  The adapter preserves it
// until after the aggregate pipeline has stopped, which also preserves the
// POSIX source borrowed by TakeFreshPipelineLiveTail().
[[nodiscard]] std::unique_ptr<ProductionCaptureRuntimeV1>
OwnRawProductionCaptureRuntimeV1(
    std::unique_ptr<l2flow::ingress::RawProductionRuntimeV1> runtime)
    noexcept;

// A source builder places its SourceFrontier POSIX mapping, producer and
// processor leases, and any other borrowed-lifetime capabilities here.  The
// service destroys this anchor only after aggregate, capture and history
// teardown.
class ProductionSourceLifetimeV1 {
public:
    virtual ~ProductionSourceLifetimeV1() = default;
};

struct ProductionServiceConfigV1 final {
    l2flow::runtime::ProductionAggregateRuntimeConfigV1 aggregate{};
    // After all four capture Start() calls have returned, Start() waits
    // synchronously for the control/readiness gates and authoritative route
    // publication.  The vendor Connect() API has no timeout/cancellation
    // contract, so this deadline does not bound those four capture Start()
    // calls.  A deployment that requires a hard startup bound must enforce a
    // process-level supervisor deadline and ultimately terminate the process
    // if Connect() does not return.
    //
    // All durations must be positive; retry/barrier waits may not exceed
    // 100 ms and the two service deadlines may not exceed ten minutes.
    std::chrono::nanoseconds activation_timeout =
        std::chrono::seconds{30};
    std::chrono::nanoseconds activation_retry_wait =
        std::chrono::milliseconds{1};
    std::chrono::nanoseconds history_barrier_wait =
        std::chrono::milliseconds{10};
    // Stop() uses one deadline after all four Raw producers have quiesced.
    std::chrono::nanoseconds drain_timeout = std::chrono::seconds{30};
};

// All objects are already prepared but none of the four capture runtimes may
// have called SDK Connect.  This split is intentional: deployment-specific
// fresh Raw provisioning remains outside the generic aggregate service, while
// the service owns the one ordering point at which all validated sources are
// allowed to connect.
struct ProductionServiceInputsV1 final {
    std::unique_ptr<l2flow::market::InstrumentRegistryV1> registry;
    std::unique_ptr<l2flow::market::InstrumentHistoryRuntimeV1> history;
    std::unique_ptr<l2flow::route::ProductionRouteControllerV1>
        route_controller;
    std::array<std::unique_ptr<ProductionSourceLifetimeV1>,
               kProductionServiceSourceCountV1>
        source_lifetimes{};
    std::array<std::unique_ptr<ProductionCaptureRuntimeV1>,
               kProductionServiceSourceCountV1>
        captures{};
    std::array<
        std::unique_ptr<l2flow::runtime::ProductionSourcePipelineV1>,
        kProductionServiceSourceCountV1>
        pipelines{};
};

enum class ProductionServiceCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kNullRegistry,
    kNullHistory,
    kNullRouteController,
    kNullSourceLifetime,
    kNullCapture,
    kNullPipeline,
    kInvalidRouteManifest,
    kRegistryIdentityMismatch,
    kHistorySourceSetMismatch,
    kPipelineSourceMismatch,
    kCaptureBindingMismatch,
    kAggregateCreateFailed,
    kResourceExhausted,
};

[[nodiscard]] std::string_view ProductionServiceCreateErrorNameV1(
    ProductionServiceCreateErrorV1 error) noexcept;

enum class ProductionServiceStateV1 : std::uint8_t {
    kReady = 0U,
    kStarting,
    kActive,
    kStopping,
    kStopped,
    kFailed,
};

enum class ProductionServiceStartErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidState,
    // Stop() was requested while a capture Start or activation wait was in
    // progress.  The service completes fail-closed cleanup before Start()
    // returns this value.  A vendor Connect() which never returns can delay
    // both calls; see ProductionServiceConfigV1::activation_timeout.
    kCancelled,
    kCaptureStartFailed,
    kActivationTimeout,
    kHistoryBarrierFailure,
    kRoutePublicationFailure,
    kAggregateFatal,
    kUnexpectedFailure,
};

[[nodiscard]] std::string_view ProductionServiceStartErrorNameV1(
    ProductionServiceStartErrorV1 error) noexcept;

struct ProductionServiceStartResultV1 final {
    ProductionServiceStartErrorV1 error =
        ProductionServiceStartErrorV1::kNone;
    std::uint8_t source_slot = UINT8_MAX;
    l2flow::runtime::ProductionAggregateBarrierResultV1 barrier{};
    l2flow::runtime::ProductionAggregatePublishResultV1 publication{};
    std::string diagnostic;
    bool already_active = false;

    [[nodiscard]] bool ok() const noexcept {
        return error == ProductionServiceStartErrorV1::kNone &&
               (already_active || publication.ok());
    }
};

enum class ProductionServiceStopErrorV1 : std::uint8_t {
    kNone = 0U,
    kBeginDrainFailed,
    kCaptureStopFailed,
    kDrainFailed,
};

[[nodiscard]] std::string_view ProductionServiceStopErrorNameV1(
    ProductionServiceStopErrorV1 error) noexcept;

struct ProductionServiceStopResultV1 final {
    ProductionServiceStopErrorV1 error =
        ProductionServiceStopErrorV1::kNone;
    l2flow::runtime::ProductionAggregateBeginDrainResultV1 begin_drain{};
    l2flow::runtime::ProductionAggregateDrainWaitResultV1 drain{};
    std::array<bool, kProductionServiceSourceCountV1> capture_stopped{};
    std::array<std::string, kProductionServiceSourceCountV1>
        capture_diagnostics{};
    bool already_stopped = false;

    [[nodiscard]] bool ok() const noexcept {
        return error == ProductionServiceStopErrorV1::kNone;
    }
};

struct ProductionServiceSnapshotV1 final {
    ProductionServiceStateV1 state = ProductionServiceStateV1::kFailed;
    std::array<bool, kProductionServiceSourceCountV1> capture_started{};
    l2flow::runtime::ProductionAggregateSnapshotV1 aggregate{};
};

// One-process owner of the exact four-source ordering topology:
//
//   four Raw producers -> four source-order decoders/Canonical pipelines
//      -> one fixed instrument-worker history -> one aggregate route.
//
// Create() performs every topology/identity validation before Start() is able
// to call the first SDK Connect.  Start() publishes no route until all four
// aggregate gates pass.  Stop() performs the two-phase aggregate protocol:
// durable route revocation, four Raw clean stops, pipeline/history drain, then
// worker join.  Canonical sinks remain fixed-capacity; segment-full is a
// source Fatal and therefore revokes the route rather than rolling implicitly.
class ProductionServiceV1 final {
public:
    ProductionServiceV1(const ProductionServiceV1&) = delete;
    ProductionServiceV1& operator=(const ProductionServiceV1&) = delete;
    ProductionServiceV1(ProductionServiceV1&&) = delete;
    ProductionServiceV1& operator=(ProductionServiceV1&&) = delete;
    ~ProductionServiceV1();

    [[nodiscard]] static ProductionServiceCreateErrorV1 Create(
        ProductionServiceConfigV1 config,
        ProductionServiceInputsV1 inputs,
        std::unique_ptr<ProductionServiceV1>* output,
        l2flow::runtime::ProductionAggregateCreateErrorV1*
            aggregate_error = nullptr) noexcept;

    [[nodiscard]] ProductionServiceStartResultV1 Start() noexcept;
    [[nodiscard]] ProductionServiceStopResultV1 Stop() noexcept;
    [[nodiscard]] ProductionServiceSnapshotV1 Snapshot() const noexcept;

    // In-process only.  No external endpoint/server is implied by these
    // accessors; callers bind every query to the currently authoritative route
    // generation and discard handles after route revocation.
    // Only the const query/barrier surface is exposed.  Admission, source
    // revocation and worker shutdown remain exclusively owned by this
    // service; factor code cannot become a second SPSC producer.
    [[nodiscard]] const l2flow::market::InstrumentHistoryRuntimeV1& history()
        const noexcept;
    [[nodiscard]] const l2flow::market::InstrumentRegistryV1& registry()
        const noexcept;

private:
    class Impl;
    explicit ProductionServiceV1(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::apps
