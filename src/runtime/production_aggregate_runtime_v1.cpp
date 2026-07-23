#include "l2flow/runtime/production_aggregate_runtime_v1.h"

#include "l2flow/canonical/canonical_schema_v1.h"
#include "l2flow/sdk/subscription_manifest.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <limits>
#include <mutex>
#include <new>
#include <thread>
#include <utility>

#include <time.h>

namespace l2flow::runtime {
namespace {

constexpr std::chrono::nanoseconds kMaximumWait =
    std::chrono::milliseconds{100};
constexpr std::size_t kImmediateActiveConvergenceAttempts = 3U;

thread_local const void* gProductionAggregateSourceLoopOwner = nullptr;
thread_local const void* gProductionAggregateRouteHookOwner = nullptr;

struct AggregateRouteHookContext final {
    const void* owner = nullptr;
    l2flow::route::ProductionRouteStoreOperationHookV1 hook = nullptr;
    void* hook_context = nullptr;
};

[[nodiscard]] bool InvokeAggregateRouteHook(
    void* context,
    l2flow::route::ProductionRouteStoreOperationV1 operation) noexcept {
    auto* bridge = static_cast<AggregateRouteHookContext*>(context);
    if (bridge == nullptr || bridge->owner == nullptr ||
        bridge->hook == nullptr) {
        return false;
    }
    struct OwnerGuard final {
        const void* previous = nullptr;
        ~OwnerGuard() { gProductionAggregateRouteHookOwner = previous; }
    } owner_guard{gProductionAggregateRouteHookOwner};
    gProductionAggregateRouteHookOwner = bridge->owner;
    return bridge->hook(bridge->hook_context, operation);
}

[[nodiscard]] bool ValidWait(
    std::chrono::nanoseconds value) noexcept {
    return value.count() > 0 && value <= kMaximumWait;
}

[[nodiscard]] std::uint64_t SteadyNowNanoseconds() noexcept {
    const auto count =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count();
    return count > 0 ? static_cast<std::uint64_t>(count) : 1U;
}

[[nodiscard]] bool MonotonicNowNanoseconds(
    std::uint64_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    struct timespec value {};
    if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0 ||
        value.tv_sec < 0 || value.tv_nsec < 0 ||
        value.tv_nsec >= 1'000'000'000L) {
        return false;
    }
    constexpr std::uint64_t kNanosecondsPerSecond = 1'000'000'000ULL;
    const auto seconds = static_cast<std::uint64_t>(value.tv_sec);
    const auto nanoseconds = static_cast<std::uint64_t>(value.tv_nsec);
    if (seconds >
        (std::numeric_limits<std::uint64_t>::max() - nanoseconds) /
            kNanosecondsPerSecond) {
        return false;
    }
    *output = seconds * kNanosecondsPerSecond + nanoseconds;
    return true;
}

void SetDiagnostic(
    std::string* diagnostic,
    std::string_view message) noexcept {
    if (diagnostic == nullptr) {
        return;
    }
    try {
        diagnostic->assign(message.data(), message.size());
    } catch (...) {
    }
}

[[nodiscard]] ProductionAggregateBarrierErrorV1 MapBarrierError(
    l2flow::market::InstrumentHistoryBarrierWaitErrorV1 error) noexcept {
    switch (error) {
        case l2flow::market::InstrumentHistoryBarrierWaitErrorV1::kNone:
            return ProductionAggregateBarrierErrorV1::kNone;
        case l2flow::market::InstrumentHistoryBarrierWaitErrorV1::
                kInvalidBarrier:
            return ProductionAggregateBarrierErrorV1::kInvalidBarrier;
        case l2flow::market::InstrumentHistoryBarrierWaitErrorV1::kTimeout:
            return ProductionAggregateBarrierErrorV1::kTimeout;
        case l2flow::market::InstrumentHistoryBarrierWaitErrorV1::
                kSourceFatal:
            return ProductionAggregateBarrierErrorV1::kSourceFatal;
    }
    return ProductionAggregateBarrierErrorV1::kInvalidBarrier;
}

[[nodiscard]] std::uint32_t SourceStreamIdForKind(
    l2flow::sdk::IngressKind kind) noexcept {
    switch (kind) {
        case l2flow::sdk::IngressKind::ShSnapshot:
            return l2flow::route::kProductionRouteSourceStreamIdsV1[0U];
        case l2flow::sdk::IngressKind::ShTick:
            return l2flow::route::kProductionRouteSourceStreamIdsV1[1U];
        case l2flow::sdk::IngressKind::SzSnapshot:
            return l2flow::route::kProductionRouteSourceStreamIdsV1[2U];
        case l2flow::sdk::IngressKind::SzTick:
            return l2flow::route::kProductionRouteSourceStreamIdsV1[3U];
    }
    return 0U;
}

[[nodiscard]] ProductionSourceActivationGateResultV1 GateFailure(
    ProductionSourceActivationGateErrorV1 error,
    bool fail_stop = false,
    bool source_ended = false) noexcept {
    return ProductionSourceActivationGateResultV1{
        error, fail_stop, source_ended};
}

[[nodiscard]] ProductionSourceActivationGateResultV1
EvaluateRawHeartbeatAge(
    const l2flow::ingress::RawLiveControlSampleV1& sample,
    std::chrono::nanoseconds timeout) noexcept {
    using GateError = ProductionSourceActivationGateErrorV1;
    if (!sample.ok()) {
        return GateFailure(GateError::kRawControlUnavailable);
    }
    const std::uint64_t heartbeat =
        sample.snapshot.heartbeat_monotonic_ns;
    if (heartbeat == 0U) {
        return GateFailure(GateError::kRawHeartbeatMissing);
    }
    std::uint64_t now = 0U;
    if (!MonotonicNowNanoseconds(&now)) {
        return GateFailure(GateError::kRawHeartbeatClockUnavailable);
    }
    if (now < heartbeat) {
        return GateFailure(
            GateError::kRawHeartbeatClockRegression, true);
    }
    const auto timeout_ns = static_cast<std::uint64_t>(timeout.count());
    if (now - heartbeat > timeout_ns) {
        return GateFailure(GateError::kRawHeartbeatTimedOut);
    }
    return {};
}

[[nodiscard]] l2flow::control::ControlDecoderReadinessSummaryV1
ReadinessSummaryOf(
    const l2flow::control::ControlDecoderSnapshotV1& snapshot) noexcept {
    return l2flow::control::ControlDecoderReadinessSummaryV1{
        snapshot.source_stream_id,
        snapshot.capture_date,
        snapshot.stream_day_id,
        snapshot.processed_ingress_sequence,
        snapshot.processed_record_end_wal_pos,
        snapshot.disconnected_window,
        snapshot.poisoned,
        snapshot.control_ready,
        snapshot.decoder_evidence_ready};
}

[[nodiscard]] ProductionSourceActivationGateResultV1
EvaluateSourceValidityCommon(
    const l2flow::route::ProductionRouteSourceV1& route_source,
    const ProductionSourcePipelineConfigV1& config,
    const l2flow::control::ControlDecoderReadinessSummaryV1& control,
    const l2flow::ingress::RawLiveControlSampleV1* raw_control,
    const l2flow::canonical::SourceFrontierV1& source_frontier,
    l2flow::canonical::SourceFrontierErrorV1 frontier_error,
    const l2flow::market::InstrumentHistorySourceFrontierV1& history,
    bool pipeline_ended,
    bool pipeline_fatal) noexcept {
    using GateError = ProductionSourceActivationGateErrorV1;
    if (pipeline_fatal) {
        return GateFailure(GateError::kPipelineFatal, true);
    }
    if (pipeline_ended) {
        return GateFailure(GateError::kSourceEnded, false, true);
    }

    if (control.source_stream_id != route_source.source_stream_id ||
        control.capture_date != route_source.capture_date ||
        control.stream_day_id != route_source.stream_day_id) {
        return GateFailure(GateError::kControlIdentityMismatch, true);
    }
    if (control.poisoned) {
        return GateFailure(GateError::kControlPoisoned, true);
    }

    const l2flow::ingress::RawControlSnapshot* raw = nullptr;
    bool raw_unavailable = false;
    if (raw_control != nullptr && !raw_control->ok()) {
        if (raw_control->error ==
            l2flow::ingress::RawLiveTailError::kControlUnavailable) {
            raw_unavailable = true;
        } else {
            return GateFailure(GateError::kRawControlInvalid, true);
        }
    } else if (raw_control != nullptr) {
        raw = &raw_control->snapshot;
        if (raw->source_stream_id != route_source.source_stream_id ||
            raw->capture_date != route_source.capture_date ||
            raw->stream_day_id != route_source.stream_day_id ||
            raw->writer_instance != route_source.writer_instance) {
            return GateFailure(GateError::kSourceIdentityMismatch, true);
        }
        if (raw->fatal_state != 0U) {
            return GateFailure(GateError::kRawControlFatal, true);
        }
    }

    if (frontier_error ==
        l2flow::canonical::SourceFrontierErrorV1::kBusy) {
        return GateFailure(GateError::kSourceFrontierBusy);
    }
    if (frontier_error !=
        l2flow::canonical::SourceFrontierErrorV1::kNone) {
        return GateFailure(GateError::kSourceFrontierUnavailable, true);
    }
    const std::uint32_t configured_source =
        SourceStreamIdForKind(config.ingress_kind);
    if (configured_source == 0U ||
        configured_source != route_source.source_stream_id ||
        source_frontier.source_stream_id !=
            route_source.source_stream_id ||
        source_frontier.capture_date != route_source.capture_date ||
        source_frontier.stream_day_id != route_source.stream_day_id ||
        source_frontier.writer_instance != route_source.writer_instance ||
        config.source_generation != route_source.source_generation ||
        config.canonical_generation !=
            route_source.canonical_generation ||
        source_frontier.generation != route_source.source_generation) {
        return GateFailure(GateError::kSourceIdentityMismatch, true);
    }
    if (!l2flow::canonical::ClockEpochIdentityV1Valid(
            source_frontier.clock_epoch) ||
        l2flow::route::ComputeProductionRouteClockEpochIdentitySha256V1(
            source_frontier.clock_epoch) !=
            route_source.clock_epoch_identity_sha256) {
        return GateFailure(GateError::kClockIdentityMismatch, true);
    }
    if (control.processed_ingress_sequence !=
            source_frontier.processed_ingress_sequence ||
        control.processed_record_end_wal_pos >
            source_frontier.processed_global_wal_pos ||
        (raw != nullptr &&
         (raw->append_ingress_sequence <
              source_frontier.processed_ingress_sequence ||
          raw->append_global_wal_pos <
              source_frontier.processed_global_wal_pos))) {
        return GateFailure(GateError::kSourceCursorMismatch, true);
    }
    if (source_frontier.source_state ==
        l2flow::canonical::SourceStateV1::kFatal) {
        return GateFailure(GateError::kPipelineFatal, true);
    }
    if (history.fatal) {
        return GateFailure(GateError::kHistoryFatal, true);
    }

    if (control.disconnected_window || !control.control_ready ||
        !control.decoder_evidence_ready) {
        return GateFailure(GateError::kControlEvidenceIncomplete);
    }
    if (raw_unavailable) {
        return GateFailure(GateError::kRawControlUnavailable);
    }
    if (raw != nullptr && raw->heartbeat_monotonic_ns == 0U) {
        return GateFailure(GateError::kRawHeartbeatMissing);
    }
    if (source_frontier.source_state !=
        l2flow::canonical::SourceStateV1::kHealthy) {
        return GateFailure(GateError::kSourceNotHealthy);
    }

    if (raw != nullptr &&
        route_source.durable_ingress_sequence != 0U &&
        (raw->durable_ingress_sequence <
             route_source.durable_ingress_sequence ||
         raw->durable_global_wal_pos <
             route_source.durable_global_wal_pos)) {
        return GateFailure(GateError::kRawDurabilityAnchorPending);
    }
    if (route_source.durable_ingress_sequence != 0U &&
        (source_frontier.processed_ingress_sequence <
             route_source.durable_ingress_sequence ||
         source_frontier.processed_global_wal_pos <
             route_source.durable_global_wal_pos)) {
        return GateFailure(GateError::kCanonicalProcessedAnchorPending);
    }
    return {};
}

}  // namespace

ProductionAggregateRouteBindingErrorV1
ValidateProductionAggregateRouteBindingV1(
    const l2flow::route::ProductionRouteManifestV1& active_manifest,
    const ProductionAggregateRouteBindingV1& binding) noexcept {
    if (l2flow::route::ValidateProductionRouteManifestV1(
            active_manifest) !=
            l2flow::route::ProductionRouteManifestErrorV1::kNone ||
        active_manifest.state !=
            l2flow::route::ProductionRouteStateV1::kActive) {
        return ProductionAggregateRouteBindingErrorV1::kInvalidManifest;
    }
    if (active_manifest.schema_sha256 !=
        l2flow::canonical::CanonicalSchemaDescriptorSha256V1()) {
        return ProductionAggregateRouteBindingErrorV1::
            kSchemaIdentityMismatch;
    }

    for (std::size_t index = 0U;
         index < kProductionAggregateSourceCountV1;
         ++index) {
        const auto& config = binding.source_configs[index];
        const auto& frontier = binding.source_frontiers[index];
        const auto& history = binding.history_frontiers[index];
        const auto& registry = binding.registry_identities[index];
        const auto& route_source = active_manifest.sources[index];
        const std::uint8_t source_slot =
            static_cast<std::uint8_t>(index);

        if (config.source_slot != source_slot) {
            return ProductionAggregateRouteBindingErrorV1::
                kSourceSlotMismatch;
        }
        if (config.trade_date != active_manifest.trade_date) {
            return ProductionAggregateRouteBindingErrorV1::
                kTradeDateMismatch;
        }
        if (frontier.source_state ==
            l2flow::canonical::SourceStateV1::kFatal) {
            return ProductionAggregateRouteBindingErrorV1::kSourceFatal;
        }
        if (history.fatal) {
            return ProductionAggregateRouteBindingErrorV1::kHistoryFatal;
        }
        if (registry.version != active_manifest.registry_version ||
            registry.sha256 != active_manifest.registry_sha256) {
            return ProductionAggregateRouteBindingErrorV1::
                kRegistryIdentityMismatch;
        }

        const std::uint32_t configured_source =
            SourceStreamIdForKind(config.ingress_kind);
        if (configured_source == 0U ||
            configured_source != route_source.source_stream_id ||
            frontier.source_stream_id != route_source.source_stream_id ||
            frontier.capture_date != route_source.capture_date ||
            frontier.stream_day_id != route_source.stream_day_id ||
            frontier.writer_instance != route_source.writer_instance ||
            config.source_generation !=
                route_source.source_generation ||
            config.canonical_generation !=
                route_source.canonical_generation ||
            frontier.generation != route_source.source_generation) {
            return ProductionAggregateRouteBindingErrorV1::
                kSourceIdentityMismatch;
        }
        if (!l2flow::canonical::ClockEpochIdentityV1Valid(
                frontier.clock_epoch) ||
            l2flow::route::
                ComputeProductionRouteClockEpochIdentitySha256V1(
                    frontier.clock_epoch) !=
                route_source.clock_epoch_identity_sha256) {
            return ProductionAggregateRouteBindingErrorV1::
                kClockIdentityMismatch;
        }
    }
    return ProductionAggregateRouteBindingErrorV1::kNone;
}

ProductionSourceActivationGateResultV1
EvaluateProductionSourceActivationGateV1(
    const l2flow::route::ProductionRouteSourceV1& route_source,
    const ProductionSourcePipelineConfigV1& config,
    const ProductionSourceActivationEvidenceV1& evidence,
    l2flow::market::InstrumentHistoryBarrierWaitErrorV1
        barrier_wait_error) noexcept {
    using GateError = ProductionSourceActivationGateErrorV1;
    const ProductionSourceActivationGateResultV1 common =
        EvaluateSourceValidityCommon(
            route_source,
            config,
            ReadinessSummaryOf(evidence.control),
            &evidence.raw_control,
            evidence.source_frontier,
            evidence.source_frontier_read_error,
            evidence.pipeline.history_frontier,
            evidence.pipeline.ended,
            evidence.pipeline.fatal);
    if (!common.ready() && common.fail_stop) {
        return common;
    }
    const auto& barrier = evidence.history_barrier;
    const auto& history = evidence.pipeline.history_frontier;
    if (!barrier.valid || barrier.source_slot != config.source_slot ||
        barrier.ticket != history.submitted_ticket ||
        barrier.source_sequence != history.submitted_source_sequence ||
        barrier_wait_error == l2flow::market::
            InstrumentHistoryBarrierWaitErrorV1::kInvalidBarrier ||
        barrier_wait_error == l2flow::market::
            InstrumentHistoryBarrierWaitErrorV1::kSourceFatal) {
        return GateFailure(GateError::kHistoryBarrierInvalid, true);
    }
    if (evidence.pipeline.history_draining) {
        return GateFailure(GateError::kHistoryDrainPending, false, true);
    }
    if (evidence.pipeline.history_pending) {
        return GateFailure(GateError::kHistoryAdmissionPending);
    }
    if (!common.ready()) {
        return common;
    }
    switch (barrier_wait_error) {
        case l2flow::market::InstrumentHistoryBarrierWaitErrorV1::kNone:
            return {};
        case l2flow::market::InstrumentHistoryBarrierWaitErrorV1::kTimeout:
            return GateFailure(GateError::kHistoryBarrierPending);
        case l2flow::market::InstrumentHistoryBarrierWaitErrorV1::
                kInvalidBarrier:
        case l2flow::market::InstrumentHistoryBarrierWaitErrorV1::
                kSourceFatal:
            break;
    }
    return GateFailure(GateError::kHistoryBarrierInvalid, true);
}

ProductionSourceActivationGateResultV1
EvaluateProductionSourceActiveValidityV1(
    const l2flow::route::ProductionRouteSourceV1& route_source,
    const ProductionSourcePipelineConfigV1& config,
    const ProductionSourceActiveValidityEvidenceV1& evidence) noexcept {
    return EvaluateSourceValidityCommon(
        route_source,
        config,
        evidence.control,
        &evidence.raw_control,
        evidence.source_frontier,
        evidence.source_frontier_read_error,
        evidence.history_frontier,
        evidence.pipeline_ended,
        evidence.pipeline_fatal);
}

ProductionSourceActivationGateResultV1
EvaluateProductionSourceActiveLocalValidityV1(
    const l2flow::route::ProductionRouteSourceV1& route_source,
    const ProductionSourcePipelineConfigV1& config,
    const ProductionSourceActiveLocalEvidenceV1& evidence) noexcept {
    return EvaluateSourceValidityCommon(
        route_source,
        config,
        evidence.control,
        nullptr,
        evidence.source_frontier,
        evidence.source_frontier_read_error,
        evidence.history_frontier,
        evidence.pipeline_ended,
        evidence.pipeline_fatal);
}

class ProductionAggregateRuntimeV1::Impl final {
public:
    Impl(
        ProductionAggregateRuntimeConfigV1 runtime_config,
        std::array<std::unique_ptr<ProductionSourcePipelineV1>,
                   kProductionAggregateSourceCountV1>
            source_pipelines,
        l2flow::market::InstrumentHistoryRuntimeV1* history_runtime,
        l2flow::route::ProductionRouteControllerV1* controller) noexcept
        : config(runtime_config),
          sources(std::move(source_pipelines)),
          history(history_runtime),
          route_controller(controller) {
        for (std::size_t index = 0U;
             index < kProductionAggregateSourceCountV1;
             ++index) {
            source_ended[index].store(false, std::memory_order_relaxed);
            worker_exited[index].store(false, std::memory_order_relaxed);
            last_raw_heartbeat[index].store(0U, std::memory_order_relaxed);
            last_raw_validation_ns[index].store(
                0U, std::memory_order_relaxed);
            source_frontier_busy_since_ns[index].store(
                0U, std::memory_order_relaxed);
        }
    }

    [[nodiscard]] bool Start() noexcept {
        std::size_t started = 0U;
        try {
            for (; started < kProductionAggregateSourceCountV1;
                 ++started) {
                workers[started] = std::thread(
                    [this, started]() noexcept { SourceLoop(started); });
            }
            {
                // The startup wait is untimed.  Pair the predicate update
                // with the same mutex used by condition.wait() so a notify
                // cannot land between its final false check and blocking.
                std::lock_guard<std::mutex> lock(wait_mutex);
                start_released.store(true, std::memory_order_release);
            }
            condition.notify_all();
            return true;
        } catch (...) {
            {
                std::lock_guard<std::mutex> lock(wait_mutex);
                stop_requested.store(true, std::memory_order_release);
            }
            condition.notify_all();
            for (std::size_t index = 0U; index < started; ++index) {
                if (workers[index].joinable()) {
                    workers[index].join();
                }
            }
            state.store(
                ProductionAggregateStateV1::kStopped,
                std::memory_order_release);
            return false;
        }
    }

    void WaitShort(
        std::chrono::nanoseconds duration,
        bool wake_when_active) {
        std::unique_lock<std::mutex> lock(wait_mutex);
        condition.wait_for(lock, duration, [this, wake_when_active]() {
            return stop_requested.load(std::memory_order_acquire) ||
                   global_fatal.load(std::memory_order_acquire) ||
                   (wake_when_active &&
                    active_route_published.load(
                        std::memory_order_acquire));
        });
    }

    void LatchGlobalFatal(std::uint32_t reason_code) noexcept {
        if (reason_code == 0U) {
            reason_code = kProductionAggregateFatalInternalFailureV1;
        }
        std::uint32_t expected = 0U;
        static_cast<void>(fatal_reason_code.compare_exchange_strong(
            expected,
            reason_code,
            std::memory_order_acq_rel,
            std::memory_order_acquire));
        global_fatal.store(true, std::memory_order_release);
        state.store(
            ProductionAggregateStateV1::kFatal,
            std::memory_order_release);
        condition.notify_all();
    }

    void LatchSourceFatal(
        std::size_t index,
        std::uint32_t reason_code =
            kProductionAggregateFatalSourceFailureV1) noexcept {
        if (index < kProductionAggregateSourceCountV1) {
            std::uint8_t no_source =
                std::numeric_limits<std::uint8_t>::max();
            static_cast<void>(fatal_source_slot.compare_exchange_strong(
                no_source,
                static_cast<std::uint8_t>(index),
                std::memory_order_acq_rel,
                std::memory_order_acquire));
        }
        LatchGlobalFatal(reason_code);
    }

    [[nodiscard]] bool AnySourceEnded() const noexcept {
        for (const auto& ended : source_ended) {
            if (ended.load(std::memory_order_acquire)) {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] bool AllSourcesEnded() const noexcept {
        for (const auto& ended : source_ended) {
            if (!ended.load(std::memory_order_acquire)) {
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] ProductionAggregateRouteBindingV1 RouteBinding(
        const ProductionAggregateActivationEvidenceV1& evidence)
        const noexcept {
        ProductionAggregateRouteBindingV1 binding{};
        for (std::size_t index = 0U;
             index < kProductionAggregateSourceCountV1;
             ++index) {
            binding.source_configs[index] = sources[index]->config();
            binding.source_frontiers[index] =
                evidence.sources[index].source_frontier;
            binding.history_frontiers[index] =
                evidence.sources[index].pipeline.history_frontier;
            binding.registry_identities[index] =
                sources[index]->registry_identity();
        }
        return binding;
    }

    struct ActivationEvaluation final {
        ProductionAggregateRouteBindingErrorV1 binding_error =
            ProductionAggregateRouteBindingErrorV1::kNone;
        ProductionSourceActivationGateResultV1 gate{};
        std::uint8_t source_slot =
            std::numeric_limits<std::uint8_t>::max();

        [[nodiscard]] bool ready() const noexcept {
            return binding_error ==
                       ProductionAggregateRouteBindingErrorV1::kNone &&
                   gate.ready();
        }
    };

    [[nodiscard]] ActivationEvaluation EvaluateActivationEvidence(
        const ProductionAggregateActivationEvidenceV1& evidence)
        const noexcept {
        ActivationEvaluation result{};
        // An unsuccessful seqlock sample carries no stable frontier fields,
        // so classify it before immutable identity binding.  The full gate
        // preserves Fatal/integrity priority over retryable BUSY.  Treating
        // the zero-initialized frontier payload as an identity mismatch would
        // incorrectly turn ordinary writer descheduling into a permanent
        // failure.
        for (std::size_t index = 0U;
             index < kProductionAggregateSourceCountV1;
             ++index) {
            const auto read_error =
                evidence.sources[index].source_frontier_read_error;
            if (read_error !=
                l2flow::canonical::SourceFrontierErrorV1::kNone) {
                const auto barrier_error = history->WaitForBarrier(
                    evidence.sources[index].history_barrier,
                    std::chrono::nanoseconds::zero());
                result.gate = EvaluateProductionSourceActivationGateV1(
                    route_controller->active_manifest().sources[index],
                    sources[index]->config(),
                    evidence.sources[index],
                    barrier_error);
                result.source_slot = static_cast<std::uint8_t>(index);
                return result;
            }
        }
        result.binding_error = ValidateProductionAggregateRouteBindingV1(
            route_controller->active_manifest(), RouteBinding(evidence));
        if (result.binding_error !=
            ProductionAggregateRouteBindingErrorV1::kNone) {
            return result;
        }
        for (std::size_t index = 0U;
             index < kProductionAggregateSourceCountV1;
             ++index) {
            const auto barrier_error = history->WaitForBarrier(
                evidence.sources[index].history_barrier,
                std::chrono::nanoseconds::zero());
            result.gate = EvaluateProductionSourceActivationGateV1(
                route_controller->active_manifest().sources[index],
                sources[index]->config(),
                evidence.sources[index],
                barrier_error);
            if (result.gate.ready()) {
                result.gate = EvaluateRawHeartbeatAge(
                    evidence.sources[index].raw_control,
                    config.writer_heartbeat_timeout);
            }
            if (!result.gate.ready()) {
                result.source_slot = static_cast<std::uint8_t>(index);
                return result;
            }
        }
        return result;
    }

    void PublishFatalIfActive() noexcept {
        if (route_controller == nullptr ||
            fatal_route_published.load(std::memory_order_acquire)) {
            return;
        }
        const std::uint32_t reason =
            fatal_reason_code.load(std::memory_order_acquire);
        if (reason == 0U) {
            return;
        }
        std::lock_guard<std::mutex> lock(route_mutex);
        if (fatal_route_published.load(std::memory_order_relaxed)) {
            return;
        }
        if (active_route_publication_uncertain.load(
                std::memory_order_acquire) &&
            !active_route_published.load(std::memory_order_acquire)) {
            // The controller retains the exact immutable Active manifest.
            // Converge that generation before requesting its Fatal
            // successor; never publish a successor over unverified state.
            last_active_publish = route_controller->PublishActive();
            if (last_active_publish.authoritative()) {
                active_route_published.store(
                    true, std::memory_order_release);
                active_route_publication_uncertain.store(
                    false, std::memory_order_release);
            }
        }
        if (!active_route_published.load(std::memory_order_acquire)) {
            return;
        }
        last_fatal_publish = route_controller->PublishFatal(reason);
        if (last_fatal_publish.ok()) {
            fatal_route_published.store(true, std::memory_order_release);
            condition.notify_all();
        }
    }

    void WaitFatalRetry() {
        std::unique_lock<std::mutex> lock(wait_mutex);
        condition.wait_for(lock, config.route_retry_wait, [this]() {
            return stop_requested.load(std::memory_order_acquire) ||
                   fatal_route_published.load(std::memory_order_acquire);
        });
    }

    void WaitShutdownRouteRetry() {
        std::unique_lock<std::mutex> lock(wait_mutex);
        condition.wait_for(lock, config.route_retry_wait, [this]() {
            return fatal_route_published.load(std::memory_order_acquire);
        });
    }

    void RetryFatalRouteWhileRunning() noexcept {
        if (fatal_retry_owner.test_and_set(std::memory_order_acq_rel)) {
            return;
        }
        while ((active_route_published.load(std::memory_order_acquire) ||
                active_route_publication_uncertain.load(
                    std::memory_order_acquire)) &&
               !fatal_route_published.load(std::memory_order_acquire) &&
               !stop_requested.load(std::memory_order_acquire)) {
            PublishFatalIfActive();
            if (!fatal_route_published.load(std::memory_order_acquire)) {
                WaitFatalRetry();
            }
        }
        fatal_retry_owner.clear(std::memory_order_release);
    }

    void ConvergeFatalRouteForShutdown() noexcept {
        while ((active_route_published.load(std::memory_order_acquire) ||
                active_route_publication_uncertain.load(
                    std::memory_order_acquire)) &&
               !fatal_route_published.load(std::memory_order_acquire)) {
            std::uint32_t no_reason = 0U;
            static_cast<void>(fatal_reason_code.compare_exchange_strong(
                no_reason,
                kProductionAggregateFatalStoppedActiveV1,
                std::memory_order_acq_rel,
                std::memory_order_acquire));
            PublishFatalIfActive();
            if (!fatal_route_published.load(std::memory_order_acquire)) {
                WaitShutdownRouteRetry();
            }
        }
    }

    [[nodiscard]] static ProductionSourceFailureV1 FailureForGate(
        ProductionSourceActivationGateErrorV1 error) noexcept {
        switch (error) {
            case ProductionSourceActivationGateErrorV1::
                    kRawControlUnavailable:
            case ProductionSourceActivationGateErrorV1::
                    kRawControlInvalid:
            case ProductionSourceActivationGateErrorV1::kRawControlFatal:
            case ProductionSourceActivationGateErrorV1::
                    kRawHeartbeatMissing:
            case ProductionSourceActivationGateErrorV1::
                    kRawHeartbeatRegression:
            case ProductionSourceActivationGateErrorV1::
                    kRawHeartbeatClockUnavailable:
            case ProductionSourceActivationGateErrorV1::
                    kRawHeartbeatClockRegression:
            case ProductionSourceActivationGateErrorV1::
                    kRawHeartbeatTimedOut:
                return ProductionSourceFailureV1::kRawTailFailure;
            case ProductionSourceActivationGateErrorV1::
                    kControlIdentityMismatch:
            case ProductionSourceActivationGateErrorV1::kControlPoisoned:
            case ProductionSourceActivationGateErrorV1::
                    kControlEvidenceIncomplete:
                return ProductionSourceFailureV1::kControlDecoderFailure;
            case ProductionSourceActivationGateErrorV1::kHistoryFatal:
            case ProductionSourceActivationGateErrorV1::
                    kHistoryBarrierInvalid:
            case ProductionSourceActivationGateErrorV1::
                    kHistoryBarrierPending:
                return ProductionSourceFailureV1::kHistoryCommitFailure;
            case ProductionSourceActivationGateErrorV1::
                    kSourceFrontierUnavailable:
            case ProductionSourceActivationGateErrorV1::
                    kSourceFrontierBusy:
            case ProductionSourceActivationGateErrorV1::
                    kSourceIdentityMismatch:
            case ProductionSourceActivationGateErrorV1::
                    kClockIdentityMismatch:
            case ProductionSourceActivationGateErrorV1::
                    kSourceCursorMismatch:
            case ProductionSourceActivationGateErrorV1::kSourceNotHealthy:
                return ProductionSourceFailureV1::kFrontierFailure;
            case ProductionSourceActivationGateErrorV1::kNone:
            case ProductionSourceActivationGateErrorV1::
                    kEvidenceUnavailable:
            case ProductionSourceActivationGateErrorV1::kPipelineFatal:
            case ProductionSourceActivationGateErrorV1::kSourceEnded:
            case ProductionSourceActivationGateErrorV1::
                    kHistoryAdmissionPending:
            case ProductionSourceActivationGateErrorV1::
                    kHistoryDrainPending:
            case ProductionSourceActivationGateErrorV1::
                    kRawDurabilityAnchorPending:
            case ProductionSourceActivationGateErrorV1::
                    kCanonicalProcessedAnchorPending:
                return ProductionSourceFailureV1::kUnexpectedFailure;
        }
        return ProductionSourceFailureV1::kUnexpectedFailure;
    }

    [[nodiscard]] bool FrontierBusyWithinBudget(
        std::size_t index,
        const ProductionSourceActivationGateResultV1& validity) noexcept {
        if (validity.error !=
            ProductionSourceActivationGateErrorV1::kSourceFrontierBusy) {
            source_frontier_busy_since_ns[index].store(
                0U, std::memory_order_release);
            return false;
        }
        const std::uint64_t now = SteadyNowNanoseconds();
        std::uint64_t since = source_frontier_busy_since_ns[index].load(
            std::memory_order_acquire);
        if (since == 0U) {
            std::uint64_t empty = 0U;
            if (source_frontier_busy_since_ns[index].compare_exchange_strong(
                    empty,
                    now,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                since = now;
            } else {
                since = empty;
            }
        }
        const std::uint64_t timeout = static_cast<std::uint64_t>(
            config.source_frontier_busy_timeout.count());
        return now >= since && now - since < timeout;
    }

    void ValidateActiveSource(
        std::size_t index,
        bool force_raw,
        bool track_heartbeat_regression) noexcept {
        if (state.load(std::memory_order_acquire) !=
                ProductionAggregateStateV1::kRunning ||
            !active_route_published.load(std::memory_order_acquire) ||
            global_fatal.load(std::memory_order_acquire)) {
            return;
        }

        const ProductionSourceActiveLocalEvidenceV1 local =
            sources[index]->CaptureActiveLocalEvidence();
        ProductionSourceActivationGateResultV1 validity =
            EvaluateProductionSourceActiveLocalValidityV1(
                route_controller->active_manifest().sources[index],
                sources[index]->config(), local);
        if (!validity.ready()) {
            if (FrontierBusyWithinBudget(index, validity)) {
                return;
            }
            // BeginDrain linearizes at Running -> Draining.  Raw producers
            // are allowed to quiesce after that boundary, so an Active-only
            // validity failure sampled across it must not poison a clean
            // drain.  Pipeline Step() remains responsible for real decode,
            // frontier and history failures while draining.
            if (state.load(std::memory_order_acquire) !=
                ProductionAggregateStateV1::kRunning) {
                return;
            }
            sources[index]->MarkFatal(FailureForGate(validity.error));
            LatchSourceFatal(index);
            PublishFatalIfActive();
            return;
        }
        source_frontier_busy_since_ns[index].store(
            0U, std::memory_order_release);

        const std::uint64_t now = SteadyNowNanoseconds();
        const std::uint64_t last =
            last_raw_validation_ns[index].load(std::memory_order_acquire);
        const std::uint64_t interval = static_cast<std::uint64_t>(
            config.active_validation_interval.count());
        const bool due = force_raw || last == 0U || now < last ||
            now - last >= interval;
        if (!due) {
            return;
        }

        const ProductionSourceActiveValidityEvidenceV1 evidence =
            sources[index]->CaptureActiveValidityEvidence();
        validity = EvaluateProductionSourceActiveValidityV1(
            route_controller->active_manifest().sources[index],
            sources[index]->config(), evidence);
        if (validity.ready()) {
            validity = EvaluateRawHeartbeatAge(
                evidence.raw_control,
                config.writer_heartbeat_timeout);
        }
        if (validity.ready() && track_heartbeat_regression) {
            const std::uint64_t heartbeat =
                evidence.raw_control.snapshot.heartbeat_monotonic_ns;
            const std::uint64_t previous =
                last_raw_heartbeat[index].load(std::memory_order_relaxed);
            if (previous != 0U && heartbeat < previous) {
                validity = GateFailure(
                    ProductionSourceActivationGateErrorV1::
                        kRawHeartbeatRegression,
                    true);
            } else {
                last_raw_heartbeat[index].store(
                    heartbeat, std::memory_order_relaxed);
            }
        }
        if (validity.ready()) {
            last_raw_validation_ns[index].store(
                now, std::memory_order_release);
            return;
        }

        if (FrontierBusyWithinBudget(index, validity)) {
            return;
        }

        if (state.load(std::memory_order_acquire) !=
            ProductionAggregateStateV1::kRunning) {
            return;
        }

        sources[index]->MarkFatal(FailureForGate(validity.error));
        LatchSourceFatal(index);
        PublishFatalIfActive();
    }

    void HandleSourceStep(
        std::size_t index,
        const ProductionSourceStepResultV1& result) {
        switch (result.kind) {
            case ProductionSourceStepKindV1::kProgress:
                return;
            case ProductionSourceStepKindV1::kBackpressure:
                WaitShort(config.history_backpressure_wait, false);
                return;
            case ProductionSourceStepKindV1::kWouldBlock:
                WaitShort(config.idle_wait, false);
                return;
            case ProductionSourceStepKindV1::kEnd:
                source_ended[index].store(true, std::memory_order_release);
                if (state.load(std::memory_order_acquire) ==
                        ProductionAggregateStateV1::kRunning &&
                    active_route_published.load(
                        std::memory_order_acquire)) {
                    LatchGlobalFatal(
                        kProductionAggregateFatalActiveSourceEndV1);
                }
                condition.notify_all();
                return;
            case ProductionSourceStepKindV1::kFatal:
                LatchSourceFatal(index);
                return;
        }
        LatchGlobalFatal(kProductionAggregateFatalInternalFailureV1);
    }

    void SourceLoop(std::size_t index) noexcept {
        struct OwnerGuard final {
            const void* previous = nullptr;
            ~OwnerGuard() {
                gProductionAggregateSourceLoopOwner = previous;
            }
        } owner_guard{gProductionAggregateSourceLoopOwner};
        gProductionAggregateSourceLoopOwner = this;
        try {
            {
                std::unique_lock<std::mutex> lock(wait_mutex);
                condition.wait(lock, [this]() {
                    return start_released.load(std::memory_order_acquire) ||
                           stop_requested.load(std::memory_order_acquire);
                });
            }
            for (;;) {
                if (global_fatal.load(std::memory_order_acquire)) {
                    sources[index]->MarkFatal(
                        ProductionSourceFailureV1::kCoordinatedFailStop);
                    RetryFatalRouteWhileRunning();
                    break;
                }

                if (stop_requested.load(std::memory_order_acquire)) {
                    // Once stop is requested, Step() is called only to retry
                    // an envelope whose Raw/Canonical commit already
                    // completed, or to finish an already captured end
                    // barrier.  No new Raw record is admitted.
                    const ProductionSourceSnapshotV1 snapshot =
                        sources[index]->Snapshot();
                    if (snapshot.fatal) {
                        LatchSourceFatal(index);
                        continue;
                    }
                    if (snapshot.history_frontier.fatal) {
                        HandleSourceStep(index, sources[index]->Step());
                        continue;
                    }
                    if (!snapshot.history_pending &&
                        !snapshot.history_draining) {
                        break;
                    }
                    const ProductionSourceStepResultV1 result =
                        sources[index]->Step();
                    HandleSourceStep(index, result);
                    continue;
                }

                if (source_ended[index].load(
                        std::memory_order_acquire)) {
                    const ProductionAggregateStateV1 lifecycle =
                        state.load(std::memory_order_acquire);
                    if (lifecycle ==
                            ProductionAggregateStateV1::kDraining ||
                        lifecycle ==
                            ProductionAggregateStateV1::kStopping) {
                        break;
                    }
                    if (lifecycle ==
                            ProductionAggregateStateV1::kRunning &&
                        active_route_published.load(
                            std::memory_order_acquire)) {
                        LatchGlobalFatal(
                            kProductionAggregateFatalActiveSourceEndV1);
                        continue;
                    }
                    WaitShort(config.idle_wait, true);
                    continue;
                }

                const ProductionSourceStepResultV1 result =
                    sources[index]->Step();
                if (result.kind != ProductionSourceStepKindV1::kEnd &&
                    result.kind != ProductionSourceStepKindV1::kFatal) {
                    ValidateActiveSource(index, false, true);
                }
                HandleSourceStep(index, result);
            }
        } catch (...) {
            LatchGlobalFatal(
                kProductionAggregateFatalInternalFailureV1);
            sources[index]->MarkFatal(
                ProductionSourceFailureV1::kUnexpectedFailure);
            RetryFatalRouteWhileRunning();
        }
        worker_exited[index].store(true, std::memory_order_release);
        condition.notify_all();
    }

    [[nodiscard]] ProductionAggregateSnapshotV1 Snapshot() const noexcept {
        ProductionAggregateSnapshotV1 result{};
        result.state = state.load(std::memory_order_acquire);
        result.active_route_published =
            active_route_published.load(std::memory_order_acquire);
        result.active_route_publication_uncertain =
            active_route_publication_uncertain.load(
                std::memory_order_acquire);
        result.fatal_route_published =
            fatal_route_published.load(std::memory_order_acquire);
        result.drain_route_revoked =
            drain_route_revoked.load(std::memory_order_acquire);
        result.route_operation_hook_active =
            gProductionAggregateRouteHookOwner == this;
        result.fatal_reason_code =
            fatal_reason_code.load(std::memory_order_acquire);
        for (std::size_t index = 0U;
             index < kProductionAggregateSourceCountV1;
             ++index) {
            result.sources[index] = sources[index]->Snapshot();
            result.worker_exited[index] =
                worker_exited[index].load(std::memory_order_acquire);
            result.source_frontier_busy[index] =
                source_frontier_busy_since_ns[index].load(
                    std::memory_order_acquire) != 0U;
        }
        if (!result.route_operation_hook_active) {
            std::lock_guard<std::mutex> lock(route_mutex);
            result.last_active_publish = last_active_publish;
            result.last_fatal_publish = last_fatal_publish;
        }
        return result;
    }

    [[nodiscard]] bool AuthoritativeRouteServing() const noexcept {
        return state.load(std::memory_order_acquire) ==
                   ProductionAggregateStateV1::kRunning &&
               active_route_published.load(std::memory_order_acquire) &&
               !active_route_publication_uncertain.load(
                   std::memory_order_acquire) &&
               !global_fatal.load(std::memory_order_acquire) &&
               !fatal_route_published.load(std::memory_order_acquire) &&
               !drain_route_revoked.load(std::memory_order_acquire) &&
               !stop_requested.load(std::memory_order_acquire);
    }

    [[nodiscard]] ProductionAggregateBeginDrainResultV1
    BeginDrain() noexcept {
        ProductionAggregateBeginDrainResultV1 result{};
        if (gProductionAggregateSourceLoopOwner == this ||
            gProductionAggregateRouteHookOwner == this) {
            result.error =
                ProductionAggregateBeginDrainErrorV1::kReentrantCall;
            LatchGlobalFatal(kProductionAggregateFatalInternalFailureV1);
            return result;
        }

        std::lock_guard<std::mutex> drain_lock(drain_mutex);
        ProductionAggregateStateV1 observed =
            state.load(std::memory_order_acquire);
        if (observed == ProductionAggregateStateV1::kStopped) {
            result.error =
                ProductionAggregateBeginDrainErrorV1::kNotRunning;
            return result;
        }
        result.already_draining =
            observed == ProductionAggregateStateV1::kDraining ||
            observed == ProductionAggregateStateV1::kStopping;
        while (observed == ProductionAggregateStateV1::kRunning &&
               !state.compare_exchange_weak(
                   observed,
                   ProductionAggregateStateV1::kDraining,
                   std::memory_order_acq_rel,
                   std::memory_order_acquire)) {
        }

        {
            // Synchronize with a PublishActive call that may already have
            // passed its Running check.  Once this lock is acquired, no later
            // publication can pass because state is no longer Running.
            std::lock_guard<std::mutex> route_lock(route_mutex);
            result.route_revocation_required =
                active_route_published.load(std::memory_order_acquire) ||
                active_route_publication_uncertain.load(
                    std::memory_order_acquire);
            if (result.route_revocation_required) {
                std::uint32_t no_reason = 0U;
                static_cast<void>(fatal_reason_code.compare_exchange_strong(
                    no_reason,
                    kProductionAggregateFatalStoppedActiveV1,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire));
            }
        }

        ConvergeFatalRouteForShutdown();
        drain_route_revoked.store(
            !result.route_revocation_required ||
                fatal_route_published.load(std::memory_order_acquire),
            std::memory_order_release);
        if (result.route_revocation_required) {
            std::lock_guard<std::mutex> route_lock(route_mutex);
            result.route = last_fatal_publish;
        }
        if (global_fatal.load(std::memory_order_acquire)) {
            result.error = ProductionAggregateBeginDrainErrorV1::kFatal;
        }
        condition.notify_all();
        return result;
    }

    [[nodiscard]] ProductionAggregateDrainWaitResultV1 WaitForDrain(
        std::chrono::nanoseconds timeout) const noexcept {
        ProductionAggregateDrainWaitResultV1 result{};
        if (timeout.count() < 0) {
            result.error =
                ProductionAggregateDrainWaitErrorV1::kInvalidTimeout;
            return result;
        }
        const ProductionAggregateStateV1 current =
            state.load(std::memory_order_acquire);
        if (current == ProductionAggregateStateV1::kRunning) {
            result.error =
                ProductionAggregateDrainWaitErrorV1::kNotDraining;
            return result;
        }

        const auto terminal = [this]() noexcept {
            return AllSourcesEnded() ||
                   global_fatal.load(std::memory_order_acquire) ||
                   stop_requested.load(std::memory_order_acquire);
        };
        {
            std::unique_lock<std::mutex> lock(wait_mutex);
            if (!terminal() &&
                !condition.wait_for(lock, timeout, terminal)) {
                result.error =
                    ProductionAggregateDrainWaitErrorV1::kTimeout;
                return result;
            }
        }
        if (global_fatal.load(std::memory_order_acquire)) {
            const std::uint8_t source =
                fatal_source_slot.load(std::memory_order_acquire);
            if (source < kProductionAggregateSourceCountV1) {
                result.error =
                    ProductionAggregateDrainWaitErrorV1::kSourceFatal;
                result.source_slot = source;
            } else {
                result.error =
                    ProductionAggregateDrainWaitErrorV1::kFatal;
            }
            return result;
        }
        if (AllSourcesEnded()) {
            return result;
        }
        result.error =
            ProductionAggregateDrainWaitErrorV1::kQuiescedBeforeEnd;
        return result;
    }

    [[nodiscard]] ProductionAggregateActivationEvidenceV1
    CaptureActivationEvidence() const {
        ProductionAggregateActivationEvidenceV1 result{};
        result.state = state.load(std::memory_order_acquire);
        result.active_route_published =
            active_route_published.load(std::memory_order_acquire);
        result.fatal_reason_code =
            fatal_reason_code.load(std::memory_order_acquire);
        result.evidence_suppressed_by_operation_hook =
            gProductionAggregateSourceLoopOwner == this ||
            gProductionAggregateRouteHookOwner == this;
        if (result.evidence_suppressed_by_operation_hook) {
            return result;
        }
        for (std::size_t index = 0U;
             index < kProductionAggregateSourceCountV1;
             ++index) {
            result.sources[index] =
                sources[index]->CaptureActivationEvidence();
        }
        return result;
    }

    [[nodiscard]] ProductionAggregateBarrierResultV1
    WaitForHistoryBarriers(
        const ProductionAggregateActivationEvidenceV1& evidence,
        std::chrono::nanoseconds timeout) const noexcept {
        ProductionAggregateBarrierResultV1 result{};
        if (timeout.count() < 0) {
            result.error =
                ProductionAggregateBarrierErrorV1::kInvalidTimeout;
            return result;
        }
        const auto start = std::chrono::steady_clock::now();
        for (std::size_t index = 0U;
             index < kProductionAggregateSourceCountV1;
            ++index) {
            const std::uint8_t source_slot =
                static_cast<std::uint8_t>(index);
            const auto& barrier =
                evidence.sources[index].history_barrier;
            if (!barrier.valid ||
                barrier.source_slot != source_slot) {
                result.source_slot = source_slot;
                result.error =
                    ProductionAggregateBarrierErrorV1::kInvalidBarrier;
                result.history_error = l2flow::market::
                    InstrumentHistoryBarrierWaitErrorV1::kInvalidBarrier;
                return result;
            }
            const auto elapsed =
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - start);
            const std::chrono::nanoseconds remaining =
                elapsed >= timeout ? std::chrono::nanoseconds::zero()
                                   : timeout - elapsed;
            result.history_error = history->WaitForBarrier(
                barrier, remaining);
            result.error = MapBarrierError(result.history_error);
            if (result.error !=
                ProductionAggregateBarrierErrorV1::kNone) {
                result.source_slot = source_slot;
                return result;
            }
        }
        return result;
    }

    [[nodiscard]] ProductionAggregatePublishResultV1 PublishActive(
        const l2flow::route::ProductionRouteStoreOptionsV1& options,
        std::string* diagnostic) noexcept {
        ProductionAggregatePublishResultV1 result{};
        if (route_controller == nullptr) {
            result.error = ProductionAggregatePublishErrorV1::
                kRouteControllerUnavailable;
            SetDiagnostic(
                diagnostic,
                "production route controller is not configured");
            return result;
        }
        if (gProductionAggregateSourceLoopOwner == this ||
            gProductionAggregateRouteHookOwner == this) {
            result.error =
                ProductionAggregatePublishErrorV1::kReentrantCall;
            SetDiagnostic(
                diagnostic,
                "active publication cannot run inside an aggregate hook");
            return result;
        }
        if (global_fatal.load(std::memory_order_acquire)) {
            result.error = ProductionAggregatePublishErrorV1::kFatal;
            SetDiagnostic(diagnostic, "aggregate runtime is fail-stopped");
            return result;
        }
        if (state.load(std::memory_order_acquire) !=
                ProductionAggregateStateV1::kRunning ||
            stop_requested.load(std::memory_order_acquire)) {
            result.error = ProductionAggregatePublishErrorV1::kNotRunning;
            SetDiagnostic(diagnostic, "aggregate runtime is not running");
            return result;
        }
        if (AnySourceEnded()) {
            result.error = ProductionAggregatePublishErrorV1::kSourceEnded;
            SetDiagnostic(
                diagnostic,
                "a source ended before active publication");
            return result;
        }

        const auto handle_evaluation_failure =
            [this, diagnostic](const ActivationEvaluation& evaluation) {
                ProductionAggregatePublishResultV1 failed{};
                if (evaluation.binding_error !=
                    ProductionAggregateRouteBindingErrorV1::kNone) {
                    switch (evaluation.binding_error) {
                        case ProductionAggregateRouteBindingErrorV1::
                                kSourceFatal:
                            failed.error = ProductionAggregatePublishErrorV1::
                                kSourceFatal;
                            LatchGlobalFatal(
                                kProductionAggregateFatalSourceFailureV1);
                            break;
                        case ProductionAggregateRouteBindingErrorV1::
                                kHistoryFatal:
                            failed.error = ProductionAggregatePublishErrorV1::
                                kHistoryFatal;
                            LatchGlobalFatal(
                                kProductionAggregateFatalSourceFailureV1);
                            break;
                        case ProductionAggregateRouteBindingErrorV1::kNone:
                            break;
                        case ProductionAggregateRouteBindingErrorV1::
                                kInvalidManifest:
                        case ProductionAggregateRouteBindingErrorV1::
                                kTradeDateMismatch:
                        case ProductionAggregateRouteBindingErrorV1::
                                kSourceSlotMismatch:
                        case ProductionAggregateRouteBindingErrorV1::
                                kSourceIdentityMismatch:
                        case ProductionAggregateRouteBindingErrorV1::
                                kClockIdentityMismatch:
                        case ProductionAggregateRouteBindingErrorV1::
                                kSchemaIdentityMismatch:
                        case ProductionAggregateRouteBindingErrorV1::
                                kRegistryIdentityMismatch:
                            failed.error = ProductionAggregatePublishErrorV1::
                                kSourceBindingMismatch;
                            LatchGlobalFatal(
                                kProductionAggregateFatalInternalFailureV1);
                            break;
                    }
                    SetDiagnostic(
                        diagnostic,
                        ProductionAggregateRouteBindingErrorNameV1(
                            evaluation.binding_error));
                } else {
                    failed.activation_error = evaluation.gate.error;
                    failed.source_slot = evaluation.source_slot;
                    if (evaluation.gate.source_ended) {
                        failed.error =
                            ProductionAggregatePublishErrorV1::kSourceEnded;
                    } else if (evaluation.gate.fail_stop) {
                        switch (evaluation.gate.error) {
                            case ProductionSourceActivationGateErrorV1::
                                    kHistoryFatal:
                            case ProductionSourceActivationGateErrorV1::
                                    kHistoryBarrierInvalid:
                                failed.error =
                                    ProductionAggregatePublishErrorV1::
                                        kHistoryFatal;
                                break;
                            case ProductionSourceActivationGateErrorV1::
                                    kPipelineFatal:
                            case ProductionSourceActivationGateErrorV1::
                                    kControlPoisoned:
                            case ProductionSourceActivationGateErrorV1::
                                    kRawControlFatal:
                                failed.error =
                                    ProductionAggregatePublishErrorV1::
                                        kSourceFatal;
                                break;
                            default:
                                failed.error =
                                    ProductionAggregatePublishErrorV1::
                                        kSourceBindingMismatch;
                                break;
                        }
                        LatchGlobalFatal(
                            kProductionAggregateFatalSourceFailureV1);
                    } else {
                        failed.error = ProductionAggregatePublishErrorV1::
                            kReadinessIncomplete;
                    }
                    SetDiagnostic(
                        diagnostic,
                        ProductionSourceActivationGateErrorNameV1(
                            evaluation.gate.error));
                }

                if (active_route_published.load(
                        std::memory_order_acquire) &&
                    failed.error != ProductionAggregatePublishErrorV1::
                        kFatal) {
                    LatchGlobalFatal(
                        kProductionAggregateFatalSourceFailureV1);
                    failed.error =
                        ProductionAggregatePublishErrorV1::kBecameFatal;
                }
                if (global_fatal.load(std::memory_order_acquire)) {
                    PublishFatalIfActive();
                }
                return failed;
            };

        ProductionAggregateActivationEvidenceV1 first{};
        ProductionAggregateActivationEvidenceV1 final{};
        try {
            first = CaptureActivationEvidence();
            const ActivationEvaluation first_evaluation =
                EvaluateActivationEvidence(first);
            if (!first_evaluation.ready()) {
                return handle_evaluation_failure(first_evaluation);
            }

            // The first zero-time barrier checks acknowledgement of the
            // sampled submissions.  Capture again afterwards so publication
            // never combines pre-wait control/Raw/frontier evidence with a
            // later history result.  Sources may legitimately advance.
            final = CaptureActivationEvidence();
            const ActivationEvaluation final_evaluation =
                EvaluateActivationEvidence(final);
            if (!final_evaluation.ready()) {
                return handle_evaluation_failure(final_evaluation);
            }
        } catch (const std::bad_alloc&) {
            result.error =
                ProductionAggregatePublishErrorV1::kReadinessIncomplete;
            result.activation_error =
                ProductionSourceActivationGateErrorV1::
                    kEvidenceUnavailable;
            SetDiagnostic(
                diagnostic,
                ProductionSourceActivationGateErrorNameV1(
                    result.activation_error));
            return result;
        } catch (...) {
            LatchGlobalFatal(kProductionAggregateFatalInternalFailureV1);
            result.error = ProductionAggregatePublishErrorV1::kFatal;
            result.activation_error =
                ProductionSourceActivationGateErrorV1::
                    kEvidenceUnavailable;
            SetDiagnostic(diagnostic, "activation evidence capture failed");
            PublishFatalIfActive();
            return result;
        }

        bool uncertain_active = false;
        {
            // No source execution mutex is acquired under route_mutex.  The
            // final captured evidence is immutable; sources may only move
            // forward or trigger the continuous Active validity monitor.
            std::lock_guard<std::mutex> lock(route_mutex);
            if (global_fatal.load(std::memory_order_acquire)) {
                result.error = ProductionAggregatePublishErrorV1::kFatal;
                SetDiagnostic(
                    diagnostic,
                    "aggregate became fail-stopped before publication");
                return result;
            }
            if (state.load(std::memory_order_acquire) !=
                    ProductionAggregateStateV1::kRunning ||
                stop_requested.load(std::memory_order_acquire)) {
                result.error =
                    ProductionAggregatePublishErrorV1::kNotRunning;
                SetDiagnostic(
                    diagnostic,
                    "aggregate stopped before active publication");
                return result;
            }
            if (AnySourceEnded()) {
                result.error =
                    ProductionAggregatePublishErrorV1::kSourceEnded;
                SetDiagnostic(
                    diagnostic,
                    "a source ended before active publication");
                return result;
            }
            if (active_route_published.load(std::memory_order_acquire)) {
                result.route = last_active_publish;
                result.route.already_complete = true;
                result.already_active = true;
                return result;
            }

            AggregateRouteHookContext hook_bridge{
                this, options.operation_hook,
                options.operation_hook_context};
            l2flow::route::ProductionRouteStoreOptionsV1 guarded_options =
                options;
            if (guarded_options.operation_hook != nullptr) {
                guarded_options.operation_hook = &InvokeAggregateRouteHook;
                guarded_options.operation_hook_context = &hook_bridge;
            }
            result.route = route_controller->PublishActive(
                guarded_options, diagnostic);
            uncertain_active = result.route.publish_result.renamed;
            for (std::size_t attempt = 0U;
                 !result.route.authoritative() && uncertain_active &&
                 attempt < kImmediateActiveConvergenceAttempts;
                 ++attempt) {
                // Omit the caller's fault hook.  The controller/store retries
                // the exact retained bytes and adopts only an exact readback.
                result.route = route_controller->PublishActive();
                uncertain_active = uncertain_active ||
                    result.route.publish_result.renamed;
            }
            last_active_publish = result.route;
            if (!result.route.authoritative()) {
                result.error =
                    ProductionAggregatePublishErrorV1::kRouteFailure;
                if (uncertain_active) {
                    active_route_publication_uncertain.store(
                        true, std::memory_order_release);
                    LatchGlobalFatal(
                        kProductionAggregateFatalInternalFailureV1);
                }
            } else {
                // Establish the regression baseline before the release that
                // lets source workers enter their continuous validators.
                // The post-publication caller also samples Raw, but must not
                // move this baseline: its sample can complete after a worker
                // captured an older heartbeat and would make that valid
                // in-flight sample look like a regression.
                for (std::size_t index = 0U;
                     index < kProductionAggregateSourceCountV1;
                     ++index) {
                    last_raw_heartbeat[index].store(
                        final.sources[index]
                            .raw_control.snapshot.heartbeat_monotonic_ns,
                        std::memory_order_relaxed);
                }
                active_route_published.store(
                    true, std::memory_order_release);
                active_route_publication_uncertain.store(
                    false, std::memory_order_release);
            }
        }
        if (!result.route.authoritative()) {
            if (uncertain_active) {
                condition.notify_all();
                PublishFatalIfActive();
            }
            return result;
        }
        condition.notify_all();

        // Close the route-I/O window with a new allocation-free sample.  This
        // is not a total-order claim: a later failure is handled by the same
        // check after every subsequent source Step.
        for (std::size_t index = 0U;
             index < kProductionAggregateSourceCountV1;
             ++index) {
            ValidateActiveSource(index, true, false);
        }

        if (global_fatal.load(std::memory_order_acquire)) {
            result.error =
                ProductionAggregatePublishErrorV1::kBecameFatal;
        } else if (state.load(std::memory_order_acquire) !=
                       ProductionAggregateStateV1::kRunning ||
                   stop_requested.load(std::memory_order_acquire)) {
            // Preserve orderly-stop history draining.  The route is revoked,
            // but source pipelines are not made fatal merely because this
            // publication raced with Stop().
            std::uint32_t no_reason = 0U;
            static_cast<void>(fatal_reason_code.compare_exchange_strong(
                no_reason,
                kProductionAggregateFatalStoppedActiveV1,
                std::memory_order_acq_rel,
                std::memory_order_acquire));
            result.error =
                ProductionAggregatePublishErrorV1::kBecameFatal;
        } else if (AnySourceEnded()) {
            LatchGlobalFatal(
                kProductionAggregateFatalActiveSourceEndV1);
            result.error =
                ProductionAggregatePublishErrorV1::kBecameFatal;
        }
        if (result.error ==
            ProductionAggregatePublishErrorV1::kBecameFatal) {
            PublishFatalIfActive();
        }
        return result;
    }

    void Stop() noexcept {
        if (gProductionAggregateSourceLoopOwner == this ||
            gProductionAggregateRouteHookOwner == this) {
            // A source hook may own a pipeline execution mutex and run on a
            // join target; a route hook already owns route_mutex.  Do not
            // self-join or self-lock, and do not consume stop_once.  Fail-stop
            // the generation and leave durable shutdown to its external
            // owner.
            LatchGlobalFatal(kProductionAggregateFatalInternalFailureV1);
            return;
        }

        // Revoke an Active generation before local admission is closed.  In
        // the explicit clean path this is an idempotent second BeginDrain();
        // in the direct path it is the entire first shutdown phase.  A
        // permanent route-store failure intentionally blocks here while the
        // source loops continue consuming.
        static_cast<void>(BeginDrain());

        std::call_once(stop_once, [this]() noexcept {
            ProductionAggregateStateV1 lifecycle =
                state.load(std::memory_order_acquire);
            while ((lifecycle == ProductionAggregateStateV1::kRunning ||
                    lifecycle == ProductionAggregateStateV1::kDraining) &&
                   !state.compare_exchange_weak(
                       lifecycle,
                       ProductionAggregateStateV1::kStopping,
                       std::memory_order_acq_rel,
                       std::memory_order_acquire)) {
            }
            {
                std::lock_guard<std::mutex> lock(wait_mutex);
                stop_requested.store(true, std::memory_order_release);
            }
            condition.notify_all();
            for (auto& worker : workers) {
                if (worker.joinable()) {
                    worker.join();
                }
            }

            // Close the race in which Stop was observed immediately after an
            // asynchronous history/source failure but before its source loop
            // performed another Step().  The external history owner remains
            // responsible for the later full StopAndDrain boundary.
            for (const auto& source : sources) {
                const ProductionSourceSnapshotV1 snapshot =
                    source->Snapshot();
                if (snapshot.fatal ||
                    snapshot.history_frontier.fatal) {
                    if (!snapshot.fatal) {
                        source->MarkFatal(
                            ProductionSourceFailureV1::
                                kHistoryCommitFailure);
                    }
                    LatchGlobalFatal(
                        kProductionAggregateFatalSourceFailureV1);
                    break;
                }
            }

            if (global_fatal.load(std::memory_order_acquire)) {
                // Every Step() caller is now joined, so this also covers a
                // stop/fatal race in which a peer exited just before observing
                // the aggregate fatal latch.
                for (auto& source : sources) {
                    source->MarkFatal(
                        ProductionSourceFailureV1::kCoordinatedFailStop);
                }
                state.store(
                    ProductionAggregateStateV1::kFatal,
                    std::memory_order_release);
            } else {
                if (active_route_published.load(
                        std::memory_order_acquire)) {
                    std::uint32_t no_reason = 0U;
                    static_cast<void>(
                        fatal_reason_code.compare_exchange_strong(
                            no_reason,
                            kProductionAggregateFatalStoppedActiveV1,
                            std::memory_order_acq_rel,
                            std::memory_order_acquire));
                }
            }
        });

        // Fail-closed shutdown: once Active may have linearized, do not return
        // while its exact Fatal successor remains unproven.  Persistent route
        // storage failure deliberately blocks here; Snapshot() remains
        // available from another thread and exposes the last store result.
        ConvergeFatalRouteForShutdown();
        drain_route_revoked.store(
            !active_route_published.load(std::memory_order_acquire) ||
                fatal_route_published.load(std::memory_order_acquire),
            std::memory_order_release);
        if (!global_fatal.load(std::memory_order_acquire)) {
            state.store(
                ProductionAggregateStateV1::kStopped,
                std::memory_order_release);
        }
    }

    ProductionAggregateRuntimeConfigV1 config{};
    std::array<std::unique_ptr<ProductionSourcePipelineV1>,
               kProductionAggregateSourceCountV1>
        sources;
    l2flow::market::InstrumentHistoryRuntimeV1* history = nullptr;
    l2flow::route::ProductionRouteControllerV1* route_controller = nullptr;
    std::array<std::thread, kProductionAggregateSourceCountV1> workers;

    std::atomic<ProductionAggregateStateV1> state{
        ProductionAggregateStateV1::kRunning};
    std::atomic<bool> start_released{false};
    std::atomic<bool> stop_requested{false};
    std::atomic<bool> global_fatal{false};
    std::atomic<bool> active_route_published{false};
    std::atomic<bool> active_route_publication_uncertain{false};
    std::atomic<bool> fatal_route_published{false};
    std::atomic<bool> drain_route_revoked{false};
    std::atomic_flag fatal_retry_owner = ATOMIC_FLAG_INIT;
    std::atomic<std::uint32_t> fatal_reason_code{0U};
    std::atomic<std::uint8_t> fatal_source_slot{
        std::numeric_limits<std::uint8_t>::max()};
    std::array<std::atomic<bool>, kProductionAggregateSourceCountV1>
        source_ended{};
    std::array<std::atomic<bool>, kProductionAggregateSourceCountV1>
        worker_exited{};
    std::array<std::atomic<std::uint64_t>,
               kProductionAggregateSourceCountV1>
        last_raw_heartbeat{};
    std::array<std::atomic<std::uint64_t>,
               kProductionAggregateSourceCountV1>
        last_raw_validation_ns{};
    std::array<std::atomic<std::uint64_t>,
               kProductionAggregateSourceCountV1>
        source_frontier_busy_since_ns{};

    mutable std::mutex wait_mutex;
    mutable std::condition_variable condition;
    std::mutex drain_mutex;
    mutable std::mutex route_mutex;
    l2flow::route::ProductionRouteControllerResultV1
        last_active_publish{};
    l2flow::route::ProductionRouteControllerResultV1
        last_fatal_publish{};
    std::once_flag stop_once;
};

std::string_view ProductionAggregateRouteBindingErrorNameV1(
    ProductionAggregateRouteBindingErrorV1 error) noexcept {
    switch (error) {
        case ProductionAggregateRouteBindingErrorV1::kNone:
            return "none";
        case ProductionAggregateRouteBindingErrorV1::kInvalidManifest:
            return "invalid_manifest";
        case ProductionAggregateRouteBindingErrorV1::kTradeDateMismatch:
            return "trade_date_mismatch";
        case ProductionAggregateRouteBindingErrorV1::kSourceSlotMismatch:
            return "source_slot_mismatch";
        case ProductionAggregateRouteBindingErrorV1::
                kSourceIdentityMismatch:
            return "source_identity_mismatch";
        case ProductionAggregateRouteBindingErrorV1::
                kClockIdentityMismatch:
            return "clock_identity_mismatch";
        case ProductionAggregateRouteBindingErrorV1::
                kSchemaIdentityMismatch:
            return "schema_identity_mismatch";
        case ProductionAggregateRouteBindingErrorV1::
                kRegistryIdentityMismatch:
            return "registry_identity_mismatch";
        case ProductionAggregateRouteBindingErrorV1::kSourceFatal:
            return "source_fatal";
        case ProductionAggregateRouteBindingErrorV1::kHistoryFatal:
            return "history_fatal";
    }
    return "invalid_production_aggregate_route_binding_error";
}

std::string_view ProductionSourceActivationGateErrorNameV1(
    ProductionSourceActivationGateErrorV1 error) noexcept {
    switch (error) {
        case ProductionSourceActivationGateErrorV1::kNone:
            return "none";
        case ProductionSourceActivationGateErrorV1::kEvidenceUnavailable:
            return "evidence_unavailable";
        case ProductionSourceActivationGateErrorV1::kPipelineFatal:
            return "pipeline_fatal";
        case ProductionSourceActivationGateErrorV1::kSourceEnded:
            return "source_ended";
        case ProductionSourceActivationGateErrorV1::
                kHistoryAdmissionPending:
            return "history_admission_pending";
        case ProductionSourceActivationGateErrorV1::
                kHistoryDrainPending:
            return "history_drain_pending";
        case ProductionSourceActivationGateErrorV1::
                kControlIdentityMismatch:
            return "control_identity_mismatch";
        case ProductionSourceActivationGateErrorV1::kControlPoisoned:
            return "control_poisoned";
        case ProductionSourceActivationGateErrorV1::
                kControlEvidenceIncomplete:
            return "control_evidence_incomplete";
        case ProductionSourceActivationGateErrorV1::
                kRawControlUnavailable:
            return "raw_control_unavailable";
        case ProductionSourceActivationGateErrorV1::kRawControlInvalid:
            return "raw_control_invalid";
        case ProductionSourceActivationGateErrorV1::kRawControlFatal:
            return "raw_control_fatal";
        case ProductionSourceActivationGateErrorV1::
                kRawHeartbeatMissing:
            return "raw_heartbeat_missing";
        case ProductionSourceActivationGateErrorV1::
                kRawHeartbeatRegression:
            return "raw_heartbeat_regression";
        case ProductionSourceActivationGateErrorV1::
                kRawHeartbeatClockUnavailable:
            return "raw_heartbeat_clock_unavailable";
        case ProductionSourceActivationGateErrorV1::
                kRawHeartbeatClockRegression:
            return "raw_heartbeat_clock_regression";
        case ProductionSourceActivationGateErrorV1::
                kRawHeartbeatTimedOut:
            return "raw_heartbeat_timed_out";
        case ProductionSourceActivationGateErrorV1::
                kSourceFrontierUnavailable:
            return "source_frontier_unavailable";
        case ProductionSourceActivationGateErrorV1::
                kSourceFrontierBusy:
            return "source_frontier_busy";
        case ProductionSourceActivationGateErrorV1::
                kSourceIdentityMismatch:
            return "source_identity_mismatch";
        case ProductionSourceActivationGateErrorV1::
                kClockIdentityMismatch:
            return "clock_identity_mismatch";
        case ProductionSourceActivationGateErrorV1::
                kSourceCursorMismatch:
            return "source_cursor_mismatch";
        case ProductionSourceActivationGateErrorV1::kSourceNotHealthy:
            return "source_not_healthy";
        case ProductionSourceActivationGateErrorV1::kHistoryFatal:
            return "history_fatal";
        case ProductionSourceActivationGateErrorV1::
                kHistoryBarrierInvalid:
            return "history_barrier_invalid";
        case ProductionSourceActivationGateErrorV1::
                kHistoryBarrierPending:
            return "history_barrier_pending";
        case ProductionSourceActivationGateErrorV1::
                kRawDurabilityAnchorPending:
            return "raw_durability_anchor_pending";
        case ProductionSourceActivationGateErrorV1::
                kCanonicalProcessedAnchorPending:
            return "canonical_processed_anchor_pending";
    }
    return "invalid_production_source_activation_gate_error";
}

std::string_view ProductionAggregateCreateErrorNameV1(
    ProductionAggregateCreateErrorV1 error) noexcept {
    switch (error) {
        case ProductionAggregateCreateErrorV1::kNone:
            return "none";
        case ProductionAggregateCreateErrorV1::kNullOutput:
            return "null_output";
        case ProductionAggregateCreateErrorV1::kNullDependency:
            return "null_dependency";
        case ProductionAggregateCreateErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case ProductionAggregateCreateErrorV1::kSourceSlotMismatch:
            return "source_slot_mismatch";
        case ProductionAggregateCreateErrorV1::kSourceIdentityMismatch:
            return "source_identity_mismatch";
        case ProductionAggregateCreateErrorV1::kHistoryRuntimeMismatch:
            return "history_runtime_mismatch";
        case ProductionAggregateCreateErrorV1::kTradeDateMismatch:
            return "trade_date_mismatch";
        case ProductionAggregateCreateErrorV1::kRouteManifestInvalid:
            return "route_manifest_invalid";
        case ProductionAggregateCreateErrorV1::kRouteManifestMismatch:
            return "route_manifest_mismatch";
        case ProductionAggregateCreateErrorV1::kSourceFatal:
            return "source_fatal";
        case ProductionAggregateCreateErrorV1::kHistoryFatal:
            return "history_fatal";
        case ProductionAggregateCreateErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case ProductionAggregateCreateErrorV1::kThreadStartFailed:
            return "thread_start_failed";
    }
    return "invalid_production_aggregate_create_error";
}

std::string_view ProductionAggregateBeginDrainErrorNameV1(
    ProductionAggregateBeginDrainErrorV1 error) noexcept {
    switch (error) {
        case ProductionAggregateBeginDrainErrorV1::kNone:
            return "none";
        case ProductionAggregateBeginDrainErrorV1::kReentrantCall:
            return "reentrant_call";
        case ProductionAggregateBeginDrainErrorV1::kNotRunning:
            return "not_running";
        case ProductionAggregateBeginDrainErrorV1::kFatal:
            return "fatal";
    }
    return "invalid_production_aggregate_begin_drain_error";
}

std::string_view ProductionAggregateDrainWaitErrorNameV1(
    ProductionAggregateDrainWaitErrorV1 error) noexcept {
    switch (error) {
        case ProductionAggregateDrainWaitErrorV1::kNone:
            return "none";
        case ProductionAggregateDrainWaitErrorV1::kInvalidTimeout:
            return "invalid_timeout";
        case ProductionAggregateDrainWaitErrorV1::kNotDraining:
            return "not_draining";
        case ProductionAggregateDrainWaitErrorV1::kTimeout:
            return "timeout";
        case ProductionAggregateDrainWaitErrorV1::kSourceFatal:
            return "source_fatal";
        case ProductionAggregateDrainWaitErrorV1::kFatal:
            return "fatal";
        case ProductionAggregateDrainWaitErrorV1::kQuiescedBeforeEnd:
            return "quiesced_before_end";
    }
    return "invalid_production_aggregate_drain_wait_error";
}

std::string_view ProductionAggregateBarrierErrorNameV1(
    ProductionAggregateBarrierErrorV1 error) noexcept {
    switch (error) {
        case ProductionAggregateBarrierErrorV1::kNone:
            return "none";
        case ProductionAggregateBarrierErrorV1::kInvalidTimeout:
            return "invalid_timeout";
        case ProductionAggregateBarrierErrorV1::kInvalidBarrier:
            return "invalid_barrier";
        case ProductionAggregateBarrierErrorV1::kTimeout:
            return "timeout";
        case ProductionAggregateBarrierErrorV1::kSourceFatal:
            return "source_fatal";
    }
    return "invalid_production_aggregate_barrier_error";
}

std::string_view ProductionAggregatePublishErrorNameV1(
    ProductionAggregatePublishErrorV1 error) noexcept {
    switch (error) {
        case ProductionAggregatePublishErrorV1::kNone:
            return "none";
        case ProductionAggregatePublishErrorV1::
                kRouteControllerUnavailable:
            return "route_controller_unavailable";
        case ProductionAggregatePublishErrorV1::kReentrantCall:
            return "reentrant_call";
        case ProductionAggregatePublishErrorV1::kNotRunning:
            return "not_running";
        case ProductionAggregatePublishErrorV1::kSourceEnded:
            return "source_ended";
        case ProductionAggregatePublishErrorV1::kReadinessIncomplete:
            return "readiness_incomplete";
        case ProductionAggregatePublishErrorV1::kSourceBindingMismatch:
            return "source_binding_mismatch";
        case ProductionAggregatePublishErrorV1::kSourceFatal:
            return "source_fatal";
        case ProductionAggregatePublishErrorV1::kHistoryFatal:
            return "history_fatal";
        case ProductionAggregatePublishErrorV1::kFatal:
            return "fatal";
        case ProductionAggregatePublishErrorV1::kRouteFailure:
            return "route_failure";
        case ProductionAggregatePublishErrorV1::kBecameFatal:
            return "became_fatal";
    }
    return "invalid_production_aggregate_publish_error";
}

ProductionAggregateRuntimeV1::ProductionAggregateRuntimeV1(
    std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}

ProductionAggregateRuntimeV1::~ProductionAggregateRuntimeV1() {
    impl_->Stop();
}

ProductionAggregateCreateErrorV1 ProductionAggregateRuntimeV1::Create(
    ProductionAggregateRuntimeConfigV1 config,
    std::array<std::unique_ptr<ProductionSourcePipelineV1>,
               kProductionAggregateSourceCountV1>
        sources,
    l2flow::market::InstrumentHistoryRuntimeV1* history,
    l2flow::route::ProductionRouteControllerV1* route_controller,
    std::unique_ptr<ProductionAggregateRuntimeV1>* output) noexcept {
    if (output == nullptr) {
        return ProductionAggregateCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (history == nullptr) {
        return ProductionAggregateCreateErrorV1::kNullDependency;
    }
    if (!ValidWait(config.idle_wait) ||
        !ValidWait(config.history_backpressure_wait) ||
        !ValidWait(config.route_retry_wait) ||
        !ValidWait(config.active_validation_interval) ||
        config.writer_heartbeat_timeout.count() <= 0 ||
        config.source_frontier_busy_timeout.count() <= 0 ||
        config.source_frontier_busy_timeout >
            kProductionAggregateMaximumSourceFrontierBusyTimeoutV1) {
        return ProductionAggregateCreateErrorV1::kInvalidConfiguration;
    }

    const auto& history_config = history->config();
    const std::uint32_t trade_date = sources.front() == nullptr
        ? 0U
        : sources.front()->config().trade_date;
    for (std::size_t index = 0U;
         index < kProductionAggregateSourceCountV1;
         ++index) {
        if (sources[index] == nullptr) {
            return ProductionAggregateCreateErrorV1::kNullDependency;
        }
        if (sources[index]->history_runtime() != history) {
            return ProductionAggregateCreateErrorV1::
                kHistoryRuntimeMismatch;
        }
        if (sources[index]->config().source_slot !=
            static_cast<std::uint8_t>(index)) {
            return ProductionAggregateCreateErrorV1::kSourceSlotMismatch;
        }
        if (sources[index]->config().trade_date != trade_date) {
            return ProductionAggregateCreateErrorV1::kTradeDateMismatch;
        }
        const std::uint32_t expected_source =
            l2flow::route::kProductionRouteSourceStreamIdsV1[index];
        if (history_config.source_stream_ids[index] != expected_source ||
            SourceStreamIdForKind(
                sources[index]->config().ingress_kind) !=
                expected_source) {
            return ProductionAggregateCreateErrorV1::
                kSourceIdentityMismatch;
        }
    }

    if (route_controller != nullptr) {
        ProductionAggregateRouteBindingV1 binding{};
        for (std::size_t index = 0U;
             index < kProductionAggregateSourceCountV1;
             ++index) {
            binding.source_configs[index] = sources[index]->config();
            binding.source_frontiers[index] = sources[index]->Frontier();
            binding.history_frontiers[index] = history->Frontier(
                static_cast<std::uint8_t>(index));
            binding.registry_identities[index] =
                sources[index]->registry_identity();
        }
        switch (ValidateProductionAggregateRouteBindingV1(
            route_controller->active_manifest(), binding)) {
            case ProductionAggregateRouteBindingErrorV1::kNone:
                break;
            case ProductionAggregateRouteBindingErrorV1::kInvalidManifest:
                return ProductionAggregateCreateErrorV1::
                    kRouteManifestInvalid;
            case ProductionAggregateRouteBindingErrorV1::
                    kTradeDateMismatch:
                return ProductionAggregateCreateErrorV1::
                    kTradeDateMismatch;
            case ProductionAggregateRouteBindingErrorV1::
                    kSourceSlotMismatch:
                return ProductionAggregateCreateErrorV1::
                    kSourceSlotMismatch;
            case ProductionAggregateRouteBindingErrorV1::
                    kSourceIdentityMismatch:
            case ProductionAggregateRouteBindingErrorV1::
                    kClockIdentityMismatch:
            case ProductionAggregateRouteBindingErrorV1::
                    kSchemaIdentityMismatch:
            case ProductionAggregateRouteBindingErrorV1::
                    kRegistryIdentityMismatch:
                return ProductionAggregateCreateErrorV1::
                    kRouteManifestMismatch;
            case ProductionAggregateRouteBindingErrorV1::kSourceFatal:
                return ProductionAggregateCreateErrorV1::kSourceFatal;
            case ProductionAggregateRouteBindingErrorV1::kHistoryFatal:
                return ProductionAggregateCreateErrorV1::kHistoryFatal;
        }
    }

    try {
        auto impl = std::make_unique<Impl>(
            config,
            std::move(sources),
            history,
            route_controller);
        // Allocate every owning object before starting threads.  Otherwise a
        // later wrapper-allocation failure would destroy joinable std::thread
        // objects and terminate the process.
        auto candidate = std::unique_ptr<ProductionAggregateRuntimeV1>(
            new ProductionAggregateRuntimeV1(std::move(impl)));
        if (!candidate->impl_->Start()) {
            return ProductionAggregateCreateErrorV1::kThreadStartFailed;
        }
        *output = std::move(candidate);
        return ProductionAggregateCreateErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return ProductionAggregateCreateErrorV1::kResourceExhausted;
    } catch (...) {
        return ProductionAggregateCreateErrorV1::kThreadStartFailed;
    }
}

ProductionAggregateSnapshotV1 ProductionAggregateRuntimeV1::Snapshot()
    const noexcept {
    return impl_->Snapshot();
}

bool ProductionAggregateRuntimeV1::AuthoritativeRouteServing()
    const noexcept {
    return impl_->AuthoritativeRouteServing();
}

ProductionAggregateBeginDrainResultV1
ProductionAggregateRuntimeV1::BeginDrain() noexcept {
    return impl_->BeginDrain();
}

ProductionAggregateDrainWaitResultV1
ProductionAggregateRuntimeV1::WaitForDrain(
    std::chrono::nanoseconds timeout) const noexcept {
    return impl_->WaitForDrain(timeout);
}

ProductionAggregateActivationEvidenceV1
ProductionAggregateRuntimeV1::CaptureActivationEvidence() const {
    return impl_->CaptureActivationEvidence();
}

ProductionAggregateBarrierResultV1
ProductionAggregateRuntimeV1::WaitForHistoryBarriers(
    const ProductionAggregateActivationEvidenceV1& evidence,
    std::chrono::nanoseconds timeout) const noexcept {
    return impl_->WaitForHistoryBarriers(evidence, timeout);
}

ProductionAggregatePublishResultV1
ProductionAggregateRuntimeV1::PublishActive(
    const l2flow::route::ProductionRouteStoreOptionsV1& options,
    std::string* diagnostic) noexcept {
    return impl_->PublishActive(options, diagnostic);
}

void ProductionAggregateRuntimeV1::Stop() noexcept {
    impl_->Stop();
}

}  // namespace l2flow::runtime
