#include "l2flow/apps/production_service_v1.h"

#include "l2flow/route/production_route_v1.h"
#include "l2flow/sdk/subscription_manifest.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <mutex>
#include <new>
#include <thread>
#include <utility>

namespace l2flow::apps {
namespace {

using namespace std::chrono_literals;

inline constexpr std::chrono::nanoseconds kMaximumServiceDeadlineV1 =
    std::chrono::minutes{10};
inline constexpr std::chrono::nanoseconds kMaximumServicePollWaitV1 =
    std::chrono::milliseconds{100};

class RawCaptureRuntimeAdapterV1 final
    : public ProductionCaptureRuntimeV1 {
public:
    explicit RawCaptureRuntimeAdapterV1(
        std::unique_ptr<l2flow::ingress::RawProductionRuntimeV1> runtime)
        noexcept
        : binding_(MakeBinding(*runtime)), runtime_(std::move(runtime)) {}

    [[nodiscard]] const ProductionCaptureBindingV1& binding()
        const noexcept override {
        return binding_;
    }

    [[nodiscard]] bool Start(std::string* diagnostic) noexcept override {
        return runtime_ != nullptr && runtime_->Initialize(diagnostic);
    }

    [[nodiscard]] bool Stop(std::string* diagnostic) noexcept override {
        return runtime_ != nullptr && runtime_->Stop(diagnostic);
    }

private:
    [[nodiscard]] static ProductionCaptureBindingV1 MakeBinding(
        const l2flow::ingress::RawProductionRuntimeV1& runtime) noexcept {
        const auto& raw = runtime.capture_binding();
        ProductionCaptureBindingV1 result{};
        result.source_slot = raw.source_slot;
        result.ingress_kind = raw.ingress_kind;
        result.source_stream_id = raw.source_stream_id;
        result.capture_date = raw.capture_date;
        result.stream_day_id = raw.stream_day_id;
        result.writer_instance = raw.writer_instance;
        result.source_generation = raw.source_generation;
        result.source_frontier = raw.source_frontier;
        return result;
    }

    const ProductionCaptureBindingV1 binding_{};
    std::unique_ptr<l2flow::ingress::RawProductionRuntimeV1> runtime_;
};

[[nodiscard]] bool ValidConfig(
    const ProductionServiceConfigV1& config) noexcept {
    return config.activation_timeout > 0ns &&
           config.activation_timeout <= kMaximumServiceDeadlineV1 &&
           config.activation_retry_wait > 0ns &&
           config.activation_retry_wait <= kMaximumServicePollWaitV1 &&
           config.history_barrier_wait > 0ns &&
           config.history_barrier_wait <= kMaximumServicePollWaitV1 &&
           config.drain_timeout > 0ns &&
           config.drain_timeout <= kMaximumServiceDeadlineV1;
}

[[nodiscard]] std::chrono::nanoseconds Remaining(
    std::chrono::steady_clock::time_point deadline) noexcept {
    const auto now = std::chrono::steady_clock::now();
    return now >= deadline
        ? std::chrono::nanoseconds::zero()
        : std::chrono::duration_cast<std::chrono::nanoseconds>(deadline - now);
}

[[nodiscard]] bool RetriablePublication(
    const l2flow::runtime::ProductionAggregatePublishResultV1& result)
    noexcept {
    return result.error == l2flow::runtime::
               ProductionAggregatePublishErrorV1::kReadinessIncomplete;
}

}  // namespace

bool ProductionCaptureBindingMatchesV1(
    const ProductionCaptureBindingV1& capture,
    std::uint8_t expected_source_slot,
    const l2flow::runtime::ProductionSourcePipelineConfigV1& pipeline,
    const l2flow::route::ProductionRouteSourceV1& route_source,
    const l2flow::canonical::SourceFrontierV1& frontier,
    const l2flow::canonical::SourceFrontierPageV1* pipeline_frontier_page)
    noexcept {
    if (expected_source_slot >= kProductionServiceSourceCountV1 ||
        pipeline.source_slot != expected_source_slot) {
        return false;
    }
    try {
        const auto& spec = l2flow::sdk::GetIngressSpec(pipeline.ingress_kind);
        return capture.source_slot == expected_source_slot &&
               capture.ingress_kind == pipeline.ingress_kind &&
               capture.source_stream_id == spec.source_stream_id &&
               capture.source_stream_id == route_source.source_stream_id &&
               capture.source_stream_id == frontier.source_stream_id &&
               capture.capture_date == route_source.capture_date &&
               capture.capture_date == frontier.capture_date &&
               capture.stream_day_id == route_source.stream_day_id &&
               capture.stream_day_id == frontier.stream_day_id &&
               capture.writer_instance == route_source.writer_instance &&
               capture.writer_instance == frontier.writer_instance &&
               capture.source_generation != 0U &&
               capture.source_generation == pipeline.source_generation &&
               capture.source_generation == route_source.source_generation &&
               capture.source_generation == frontier.generation &&
               capture.source_frontier != nullptr &&
               capture.source_frontier == pipeline_frontier_page;
    } catch (...) {
        return false;
    }
}

std::unique_ptr<ProductionCaptureRuntimeV1>
OwnRawProductionCaptureRuntimeV1(
    std::unique_ptr<l2flow::ingress::RawProductionRuntimeV1> runtime)
    noexcept {
    if (runtime == nullptr) {
        return nullptr;
    }
    try {
        return std::make_unique<RawCaptureRuntimeAdapterV1>(
            std::move(runtime));
    } catch (...) {
        return nullptr;
    }
}

class ProductionServiceV1::Impl final {
public:
    Impl(
        ProductionServiceConfigV1 config_value,
        ProductionServiceInputsV1 inputs,
        std::unique_ptr<l2flow::runtime::ProductionAggregateRuntimeV1>
            aggregate_value) noexcept
        : config(std::move(config_value)),
          // Lifetime anchors are intentionally the first owned dependencies;
          // reverse destruction releases them last.
          source_lifetimes(std::move(inputs.source_lifetimes)),
          registry(std::move(inputs.registry)),
          history(std::move(inputs.history)),
          route_controller(std::move(inputs.route_controller)),
          captures(std::move(inputs.captures)),
          aggregate(std::move(aggregate_value)) {}

    ~Impl() {
        static_cast<void>(Stop());
    }

    [[nodiscard]] ProductionServiceStartResultV1 Start() noexcept {
        std::lock_guard<std::mutex> lock(lifecycle_mutex);
        ProductionServiceStartResultV1 result{};
        const ProductionServiceStateV1 current =
            state.load(std::memory_order_acquire);
        if (current == ProductionServiceStateV1::kActive) {
            if (start_cancel_requested.load(std::memory_order_acquire)) {
                result.error = ProductionServiceStartErrorV1::kCancelled;
                FailStart();
                return result;
            }
            result.already_active = true;
            result.publication = aggregate->PublishActive();
            if (!result.publication.ok()) {
                result.error = ProductionServiceStartErrorV1::kAggregateFatal;
                result.already_active = false;
            } else if (start_cancel_requested.load(
                           std::memory_order_acquire)) {
                // Stop() linearized before this re-publication completed.
                // Revoke the just-confirmed Active route before returning.
                result.error = ProductionServiceStartErrorV1::kCancelled;
                result.already_active = false;
                FailStart();
            }
            return result;
        }
        if (current != ProductionServiceStateV1::kReady) {
            result.error = ProductionServiceStartErrorV1::kInvalidState;
            return result;
        }
        state.store(ProductionServiceStateV1::kStarting,
                    std::memory_order_release);

        const auto cancel_if_requested = [this, &result]() noexcept {
            if (!start_cancel_requested.load(std::memory_order_acquire)) {
                return false;
            }
            result.error = ProductionServiceStartErrorV1::kCancelled;
            FailStart();
            return true;
        };

        try {
            if (cancel_if_requested()) {
                return result;
            }
            for (std::size_t index = 0U; index < captures.size(); ++index) {
                std::string diagnostic;
                if (!captures[index]->Start(&diagnostic)) {
                    result.error =
                        ProductionServiceStartErrorV1::kCaptureStartFailed;
                    result.source_slot = static_cast<std::uint8_t>(index);
                    result.diagnostic = std::move(diagnostic);
                    FailStart();
                    return result;
                }
                capture_started[index].store(true, std::memory_order_release);
                // Stop() sets this latch without waiting for lifecycle_mutex,
                // so it can cancel activation as soon as the current capture
                // Start() returns.  It cannot interrupt a vendor Connect()
                // which is still blocked inside that call.
                if (cancel_if_requested()) {
                    return result;
                }
            }

            const auto deadline =
                std::chrono::steady_clock::now() + config.activation_timeout;
            while (Remaining(deadline) > 0ns) {
                if (cancel_if_requested()) {
                    return result;
                }
                const auto evidence = aggregate->CaptureActivationEvidence();
                if (evidence.state ==
                        l2flow::runtime::ProductionAggregateStateV1::kFatal ||
                    evidence.fatal_reason_code != 0U) {
                    result.error =
                        ProductionServiceStartErrorV1::kAggregateFatal;
                    FailStart();
                    return result;
                }
                if (evidence.evidence_suppressed_by_operation_hook) {
                    result.error =
                        ProductionServiceStartErrorV1::kUnexpectedFailure;
                    result.diagnostic =
                        "activation evidence was suppressed outside a route hook";
                    FailStart();
                    return result;
                }

                const std::chrono::nanoseconds barrier_wait = std::min(
                    config.history_barrier_wait, Remaining(deadline));
                if (barrier_wait <= 0ns) {
                    break;
                }
                result.barrier = aggregate->WaitForHistoryBarriers(
                    evidence, barrier_wait);
                if (cancel_if_requested()) {
                    return result;
                }
                if (!result.barrier.ok()) {
                    if (result.barrier.error == l2flow::runtime::
                            ProductionAggregateBarrierErrorV1::kTimeout) {
                        std::this_thread::sleep_for(std::min(
                            config.activation_retry_wait,
                            Remaining(deadline)));
                        continue;
                    }
                    result.error =
                        ProductionServiceStartErrorV1::kHistoryBarrierFailure;
                    result.source_slot = result.barrier.source_slot;
                    FailStart();
                    return result;
                }

                result.diagnostic.clear();
                if (cancel_if_requested()) {
                    return result;
                }
                result.publication =
                    aggregate->PublishActive({}, &result.diagnostic);
                if (result.publication.ok()) {
                    if (cancel_if_requested()) {
                        return result;
                    }
                    // Stop() first publishes its cancellation latch, then
                    // atomically changes Starting to Stopping.  This CAS is
                    // the Start/Stop linearization point: a cancellation
                    // which wins cannot be overwritten by Active, and a
                    // cancellation which follows a successful CAS observes
                    // Active and executes the normal durable drain path.
                    ProductionServiceStateV1 expected =
                        ProductionServiceStateV1::kStarting;
                    if (!state.compare_exchange_strong(
                            expected,
                            ProductionServiceStateV1::kActive,
                            std::memory_order_acq_rel,
                            std::memory_order_acquire)) {
                        result.error =
                            expected == ProductionServiceStateV1::kStopping ||
                                    start_cancel_requested.load(
                                        std::memory_order_acquire)
                                ? ProductionServiceStartErrorV1::kCancelled
                                : ProductionServiceStartErrorV1::
                                      kUnexpectedFailure;
                        FailStart();
                    }
                    return result;
                }
                if (!RetriablePublication(result.publication)) {
                    result.error =
                        result.publication.error == l2flow::runtime::
                                ProductionAggregatePublishErrorV1::kFatal
                            ? ProductionServiceStartErrorV1::kAggregateFatal
                            : ProductionServiceStartErrorV1::
                                  kRoutePublicationFailure;
                    result.source_slot = result.publication.source_slot;
                    FailStart();
                    return result;
                }
                std::this_thread::sleep_for(std::min(
                    config.activation_retry_wait, Remaining(deadline)));
            }
            result.error = ProductionServiceStartErrorV1::kActivationTimeout;
            FailStart();
            return result;
        } catch (...) {
            result.error = ProductionServiceStartErrorV1::kUnexpectedFailure;
            FailStart();
            return result;
        }
    }

    [[nodiscard]] ProductionServiceStopResultV1 Stop() noexcept {
        // Do not wait for lifecycle_mutex before requesting cancellation:
        // Start() intentionally serializes SDK lifecycle calls under that
        // mutex, but observes this latch after every returned operation.
        // The vendor Connect() contract itself is not bounded or cancellable.
        start_cancel_requested.store(true, std::memory_order_release);
        ProductionServiceStateV1 starting =
            ProductionServiceStateV1::kStarting;
        static_cast<void>(state.compare_exchange_strong(
            starting,
            ProductionServiceStateV1::kStopping,
            std::memory_order_acq_rel,
            std::memory_order_acquire));
        std::lock_guard<std::mutex> lock(lifecycle_mutex);
        ProductionServiceStopResultV1 result{};
        const ProductionServiceStateV1 current =
            state.load(std::memory_order_acquire);
        if (current == ProductionServiceStateV1::kStopped ||
            current == ProductionServiceStateV1::kFailed) {
            result.already_stopped = true;
            return result;
        }
        if (current == ProductionServiceStateV1::kReady) {
            // No producer was connected, so no Raw source can publish End.
            // The aggregate's documented emergency path is the only bounded
            // teardown; waiting for a drain here would manufacture a timeout.
            aggregate->Stop();
            history->StopAndDrain();
            state.store(ProductionServiceStateV1::kStopped,
                        std::memory_order_release);
            return result;
        }
        state.store(ProductionServiceStateV1::kStopping,
                    std::memory_order_release);

        result.begin_drain = aggregate->BeginDrain();
        if (!result.begin_drain.ok()) {
            result.error = ProductionServiceStopErrorV1::kBeginDrainFailed;
        }

        bool capture_failure = false;
        for (std::size_t index = 0U; index < captures.size(); ++index) {
            if (!capture_started[index].load(std::memory_order_acquire)) {
                continue;
            }
            result.capture_stopped[index] =
                captures[index]->Stop(&result.capture_diagnostics[index]);
            capture_failure = capture_failure || !result.capture_stopped[index];
            capture_started[index].store(false, std::memory_order_release);
        }
        if (capture_failure &&
            result.error == ProductionServiceStopErrorV1::kNone) {
            result.error = ProductionServiceStopErrorV1::kCaptureStopFailed;
        }

        if (result.begin_drain.ok()) {
            result.drain = aggregate->WaitForDrain(config.drain_timeout);
            if (!result.drain.ok() &&
                result.error == ProductionServiceStopErrorV1::kNone) {
                result.error = ProductionServiceStopErrorV1::kDrainFailed;
            }
        }
        aggregate->Stop();
        history->StopAndDrain();
        state.store(ProductionServiceStateV1::kStopped,
                    std::memory_order_release);
        return result;
    }

    [[nodiscard]] ProductionServiceSnapshotV1 Snapshot() const noexcept {
        ProductionServiceSnapshotV1 result{};
        result.state = state.load(std::memory_order_acquire);
        for (std::size_t index = 0U; index < captures.size(); ++index) {
            result.capture_started[index] =
                capture_started[index].load(std::memory_order_acquire);
        }
        result.aggregate = aggregate->Snapshot();
        return result;
    }

    void FailStart() noexcept {
        // No successful Start() result is returned unless Active publication
        // was authoritative.  Emergency aggregate stop closes publication and
        // joins source workers before the possibly partial Raw capture set is
        // stopped; all captured bytes remain in Raw even when Canonical cannot
        // be declared complete.
        aggregate->Stop();
        for (std::size_t index = 0U; index < captures.size(); ++index) {
            if (capture_started[index].exchange(false,
                                                std::memory_order_acq_rel)) {
                std::string ignored;
                static_cast<void>(captures[index]->Stop(&ignored));
            }
        }
        history->StopAndDrain();
        state.store(ProductionServiceStateV1::kFailed,
                    std::memory_order_release);
    }

    ProductionServiceConfigV1 config{};
    std::array<std::unique_ptr<ProductionSourceLifetimeV1>,
               kProductionServiceSourceCountV1>
        source_lifetimes{};
    std::unique_ptr<l2flow::market::InstrumentRegistryV1> registry;
    std::unique_ptr<l2flow::market::InstrumentHistoryRuntimeV1> history;
    std::unique_ptr<l2flow::route::ProductionRouteControllerV1>
        route_controller;
    std::array<std::unique_ptr<ProductionCaptureRuntimeV1>,
               kProductionServiceSourceCountV1>
        captures{};
    std::unique_ptr<l2flow::runtime::ProductionAggregateRuntimeV1> aggregate;

    mutable std::mutex lifecycle_mutex;
    std::atomic<ProductionServiceStateV1> state{
        ProductionServiceStateV1::kReady};
    std::array<std::atomic<bool>, kProductionServiceSourceCountV1>
        capture_started{};
    std::atomic<bool> start_cancel_requested{false};
};

ProductionServiceV1::ProductionServiceV1(
    std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}

ProductionServiceV1::~ProductionServiceV1() = default;

ProductionServiceCreateErrorV1 ProductionServiceV1::Create(
    ProductionServiceConfigV1 config,
    ProductionServiceInputsV1 inputs,
    std::unique_ptr<ProductionServiceV1>* output,
    l2flow::runtime::ProductionAggregateCreateErrorV1*
        aggregate_error) noexcept {
    if (aggregate_error != nullptr) {
        *aggregate_error =
            l2flow::runtime::ProductionAggregateCreateErrorV1::kNone;
    }
    if (output == nullptr) {
        return ProductionServiceCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (!ValidConfig(config)) {
        return ProductionServiceCreateErrorV1::kInvalidConfiguration;
    }
    if (inputs.registry == nullptr) {
        return ProductionServiceCreateErrorV1::kNullRegistry;
    }
    if (inputs.history == nullptr) {
        return ProductionServiceCreateErrorV1::kNullHistory;
    }
    if (inputs.route_controller == nullptr) {
        return ProductionServiceCreateErrorV1::kNullRouteController;
    }
    const auto& manifest = inputs.route_controller->active_manifest();
    if (l2flow::route::ValidateProductionRouteManifestV1(manifest) !=
            l2flow::route::ProductionRouteManifestErrorV1::kNone ||
        manifest.state != l2flow::route::ProductionRouteStateV1::kActive) {
        return ProductionServiceCreateErrorV1::kInvalidRouteManifest;
    }
    if (manifest.registry_version != inputs.registry->registry_version() ||
        manifest.registry_sha256 != inputs.registry->registry_sha256()) {
        return ProductionServiceCreateErrorV1::kRegistryIdentityMismatch;
    }
    if (inputs.history->config().source_stream_ids !=
        l2flow::route::kProductionRouteSourceStreamIdsV1) {
        return ProductionServiceCreateErrorV1::kHistorySourceSetMismatch;
    }
    for (std::size_t index = 0U; index < kProductionServiceSourceCountV1;
         ++index) {
        if (inputs.source_lifetimes[index] == nullptr) {
            return ProductionServiceCreateErrorV1::kNullSourceLifetime;
        }
        if (inputs.captures[index] == nullptr) {
            return ProductionServiceCreateErrorV1::kNullCapture;
        }
        if (inputs.pipelines[index] == nullptr) {
            return ProductionServiceCreateErrorV1::kNullPipeline;
        }
    }
    try {
        for (std::size_t index = 0U;
             index < kProductionServiceSourceCountV1; ++index) {
            const auto& pipeline = *inputs.pipelines[index];
            const auto& pipeline_config = pipeline.config();
            const auto& spec =
                l2flow::sdk::GetIngressSpec(pipeline_config.ingress_kind);
            const auto& route_source = manifest.sources[index];
            const auto frontier_evidence =
                pipeline.CaptureActiveLocalEvidence();
            const auto& frontier = frontier_evidence.source_frontier;
            const auto& registry_identity = pipeline.registry_identity();
            if (pipeline_config.source_slot != index ||
                spec.source_stream_id !=
                    l2flow::route::kProductionRouteSourceStreamIdsV1[index] ||
                spec.source_stream_id != route_source.source_stream_id ||
                pipeline_config.trade_date != manifest.trade_date ||
                pipeline_config.source_generation !=
                    route_source.source_generation ||
                pipeline_config.canonical_generation !=
                    route_source.canonical_generation ||
                pipeline.history_runtime() != inputs.history.get() ||
                registry_identity.version !=
                    inputs.registry->registry_version() ||
                registry_identity.sha256 !=
                    inputs.registry->registry_sha256() ||
                frontier_evidence.source_frontier_read_error !=
                    l2flow::canonical::SourceFrontierErrorV1::kNone ||
                frontier.source_stream_id != route_source.source_stream_id ||
                frontier.capture_date != route_source.capture_date ||
                frontier.stream_day_id != route_source.stream_day_id ||
                frontier.writer_instance != route_source.writer_instance ||
                frontier.generation != route_source.source_generation ||
                l2flow::route::
                        ComputeProductionRouteClockEpochIdentitySha256V1(
                            frontier.clock_epoch) !=
                    route_source.clock_epoch_identity_sha256) {
                return ProductionServiceCreateErrorV1::kPipelineSourceMismatch;
            }

            const auto& capture = inputs.captures[index]->binding();
            if (!ProductionCaptureBindingMatchesV1(
                    capture,
                    static_cast<std::uint8_t>(index),
                    pipeline_config,
                    route_source,
                    frontier,
                    pipeline.source_frontier_page())) {
                return ProductionServiceCreateErrorV1::
                    kCaptureBindingMismatch;
            }
        }
    } catch (...) {
        return ProductionServiceCreateErrorV1::kPipelineSourceMismatch;
    }

    std::unique_ptr<l2flow::runtime::ProductionAggregateRuntimeV1> aggregate;
    const auto aggregate_result =
        l2flow::runtime::ProductionAggregateRuntimeV1::Create(
            config.aggregate,
            std::move(inputs.pipelines),
            inputs.history.get(),
            inputs.route_controller.get(),
            &aggregate);
    if (aggregate_error != nullptr) {
        *aggregate_error = aggregate_result;
    }
    if (aggregate_result !=
            l2flow::runtime::ProductionAggregateCreateErrorV1::kNone ||
        aggregate == nullptr) {
        return ProductionServiceCreateErrorV1::kAggregateCreateFailed;
    }

    try {
        auto impl = std::make_unique<Impl>(
            std::move(config), std::move(inputs), std::move(aggregate));
        output->reset(new ProductionServiceV1(std::move(impl)));
        return ProductionServiceCreateErrorV1::kNone;
    } catch (...) {
        return ProductionServiceCreateErrorV1::kResourceExhausted;
    }
}

ProductionServiceStartResultV1 ProductionServiceV1::Start() noexcept {
    return impl_->Start();
}

ProductionServiceStopResultV1 ProductionServiceV1::Stop() noexcept {
    return impl_->Stop();
}

ProductionServiceSnapshotV1 ProductionServiceV1::Snapshot() const noexcept {
    return impl_->Snapshot();
}

const l2flow::market::InstrumentHistoryRuntimeV1&
ProductionServiceV1::history() const noexcept {
    return *impl_->history;
}

const l2flow::market::InstrumentRegistryV1& ProductionServiceV1::registry()
    const noexcept {
    return *impl_->registry;
}

std::string_view ProductionServiceCreateErrorNameV1(
    ProductionServiceCreateErrorV1 error) noexcept {
    switch (error) {
        case ProductionServiceCreateErrorV1::kNone: return "none";
        case ProductionServiceCreateErrorV1::kNullOutput: return "null_output";
        case ProductionServiceCreateErrorV1::kInvalidConfiguration: return "invalid_configuration";
        case ProductionServiceCreateErrorV1::kNullRegistry: return "null_registry";
        case ProductionServiceCreateErrorV1::kNullHistory: return "null_history";
        case ProductionServiceCreateErrorV1::kNullRouteController: return "null_route_controller";
        case ProductionServiceCreateErrorV1::kNullSourceLifetime: return "null_source_lifetime";
        case ProductionServiceCreateErrorV1::kNullCapture: return "null_capture";
        case ProductionServiceCreateErrorV1::kNullPipeline: return "null_pipeline";
        case ProductionServiceCreateErrorV1::kInvalidRouteManifest: return "invalid_route_manifest";
        case ProductionServiceCreateErrorV1::kRegistryIdentityMismatch: return "registry_identity_mismatch";
        case ProductionServiceCreateErrorV1::kHistorySourceSetMismatch: return "history_source_set_mismatch";
        case ProductionServiceCreateErrorV1::kPipelineSourceMismatch: return "pipeline_source_mismatch";
        case ProductionServiceCreateErrorV1::kCaptureBindingMismatch: return "capture_binding_mismatch";
        case ProductionServiceCreateErrorV1::kAggregateCreateFailed: return "aggregate_create_failed";
        case ProductionServiceCreateErrorV1::kResourceExhausted: return "resource_exhausted";
    }
    return "unknown";
}

std::string_view ProductionServiceStartErrorNameV1(
    ProductionServiceStartErrorV1 error) noexcept {
    switch (error) {
        case ProductionServiceStartErrorV1::kNone: return "none";
        case ProductionServiceStartErrorV1::kInvalidState: return "invalid_state";
        case ProductionServiceStartErrorV1::kCancelled: return "cancelled";
        case ProductionServiceStartErrorV1::kCaptureStartFailed: return "capture_start_failed";
        case ProductionServiceStartErrorV1::kActivationTimeout: return "activation_timeout";
        case ProductionServiceStartErrorV1::kHistoryBarrierFailure: return "history_barrier_failure";
        case ProductionServiceStartErrorV1::kRoutePublicationFailure: return "route_publication_failure";
        case ProductionServiceStartErrorV1::kAggregateFatal: return "aggregate_fatal";
        case ProductionServiceStartErrorV1::kUnexpectedFailure: return "unexpected_failure";
    }
    return "unknown";
}

std::string_view ProductionServiceStopErrorNameV1(
    ProductionServiceStopErrorV1 error) noexcept {
    switch (error) {
        case ProductionServiceStopErrorV1::kNone: return "none";
        case ProductionServiceStopErrorV1::kBeginDrainFailed: return "begin_drain_failed";
        case ProductionServiceStopErrorV1::kCaptureStopFailed: return "capture_stop_failed";
        case ProductionServiceStopErrorV1::kDrainFailed: return "drain_failed";
    }
    return "unknown";
}

}  // namespace l2flow::apps
