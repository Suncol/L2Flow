#include "l2flow/ingress/raw_ingress_app.h"

#include "l2flow/common/identity128.h"

#include <algorithm>
#include <cerrno>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace l2flow::ingress {

class RawUnifiedCallbackRouter final
    : public datayes::mdl::MessageHandlerBase {
public:
    explicit RawUnifiedCallbackRouter(
        CallbackHandler& handler) noexcept
        : handler_(handler) {}

    void OnMessage(
        datayes::mdl::Subscriber*,
        const datayes::mdl::MDLMessage* message)
        noexcept override {
        handler_.OnMDLAPIMessage(message);
    }

private:
    CallbackHandler& handler_;
};

namespace {

CaptureClock& RequireClock(
    const std::unique_ptr<CaptureClock>& clock) {
    if (clock == nullptr) {
        throw std::invalid_argument(
            "Raw ingress capture clock is null");
    }
    return *clock;
}

RawWalSink& RequireSink(
    const std::unique_ptr<RawWalSink>& sink) {
    if (sink == nullptr) {
        throw std::invalid_argument(
            "Raw ingress prepared sink is null");
    }
    return *sink;
}

bool HasNul(std::string_view value) noexcept {
    return value.find('\0') != std::string_view::npos;
}

bool CheckedAdd(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* output) noexcept {
    if (output == nullptr ||
        left >
            std::numeric_limits<std::uint64_t>::max() -
                right) {
        return false;
    }
    *output = left + right;
    return true;
}

bool CheckedMultiply(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* output) noexcept {
    if (output == nullptr ||
        (left != 0U &&
         right >
             std::numeric_limits<std::uint64_t>::max() /
                 left)) {
        return false;
    }
    *output = left * right;
    return true;
}

bool SameCaptureMetrics(
    const CaptureMetricsSnapshot& left,
    const CaptureMetricsSnapshot& right) noexcept {
    return left.callback_invocations ==
               right.callback_invocations &&
           left.captured_records ==
               right.captured_records &&
           left.captured_vendor_bytes ==
               right.captured_vendor_bytes &&
           left.captured_framed_wal_bytes ==
               right.captured_framed_wal_bytes &&
           left.callback_reentry ==
               right.callback_reentry &&
           left.callback_exceptions ==
               right.callback_exceptions &&
           left.callbacks_after_stop ==
               right.callbacks_after_stop &&
           left.callbacks_after_fatal ==
               right.callbacks_after_fatal &&
           left.ring_overflow == right.ring_overflow &&
           left.captured_ingress_sequence ==
               right.captured_ingress_sequence &&
           left.callback_inflight ==
               right.callback_inflight &&
           left.invalid_messages ==
               right.invalid_messages &&
           left.latency_ns == right.latency_ns;
}

bool SameCaptureWorkerSnapshot(
    const RawCaptureWorkerSnapshot& left,
    const RawCaptureWorkerSnapshot& right) noexcept {
    return left.append == right.append &&
           left.durable == right.durable &&
           left.failure_kind == right.failure_kind &&
           left.writer_failure.kind ==
               right.writer_failure.kind &&
           left.writer_failure.error_number ==
               right.writer_failure.error_number &&
           left.stop_requested == right.stop_requested &&
           left.startup_complete ==
               right.startup_complete &&
           left.startup_succeeded ==
               right.startup_succeeded &&
           left.finished == right.finished &&
           left.emergency_pause_requested ==
               right.emergency_pause_requested &&
           left.emergency_paused ==
               right.emergency_paused &&
           left.emergency_abandoned ==
               right.emergency_abandoned;
}

bool SameWalSnapshot(
    const RawWalWriterSnapshot& left,
    const RawWalWriterSnapshot& right) noexcept {
    return left.append == right.append &&
           left.durable == right.durable &&
           left.journal_logical_size ==
               right.journal_logical_size &&
           left.initialized == right.initialized &&
           left.sealed == right.sealed &&
           left.closed == right.closed &&
           left.fatal == right.fatal;
}

}  // namespace

bool RawIngressCleanStopEvidenceV1::exact()
    const noexcept {
    if (!reconciliation.exact() ||
        callback.callback_inflight ||
        ring_used_bytes != 0U ||
        callback.captured_records !=
            capture.append.records ||
        callback.captured_records !=
            capture.durable.records ||
        callback.captured_vendor_bytes !=
            capture.append.vendor_bytes ||
        callback.captured_vendor_bytes !=
            capture.durable.vendor_bytes ||
        capture.append.framed_wal_bytes !=
            capture.durable.framed_wal_bytes ||
        capture.failure_kind !=
            RawCaptureWorkerFailureKind::kNone ||
        !capture.stop_requested ||
        !capture.startup_complete ||
        !capture.startup_succeeded ||
        !capture.finished ||
        !final_wal.initialized ||
        !final_wal.sealed ||
        !final_wal.closed ||
        final_wal.fatal ||
        final_wal.append != final_wal.durable ||
        started_runtime.append.global_wal_pos !=
            started_runtime.durable.global_wal_pos ||
        started_runtime.append.ingress_sequence !=
            started_runtime.durable.ingress_sequence ||
        started_runtime.append.segment_offset !=
            started_runtime.durable.segment_offset ||
        final_sink_identity.writer_instance !=
            started_runtime.writer_instance ||
        final_sink_identity.stream_day_id !=
            started_runtime.stream_day_id ||
        final_sink_identity.source_stream_id !=
            started_runtime.source_stream_id ||
        final_sink_identity.capture_date !=
            started_runtime.capture_date ||
        final_sink_identity.segment_sequence <
            started_runtime.current_segment_sequence ||
        final_wal.append.global_wal_pos <
            started_runtime.append.global_wal_pos ||
        final_wal.append.ingress_sequence <
            started_runtime.append.ingress_sequence ||
        final_wal.append.global_wal_pos <
            final_wal.append.segment_offset ||
        final_sink_identity.segment_base_wal_pos !=
            final_wal.append.global_wal_pos -
                final_wal.append.segment_offset ||
        !observer.generation_active ||
        !observer.observer_healthy ||
        observer.generation.writer_instance !=
            final_sink_identity.writer_instance ||
        observer.generation.stream_day_id !=
            final_sink_identity.stream_day_id ||
        observer.generation.source_stream_id !=
            final_sink_identity.source_stream_id ||
        observer.generation.capture_date !=
            final_sink_identity.capture_date ||
        observer.observer_processed_segment_sequence !=
            final_sink_identity.segment_sequence ||
        observer.observer_processed_wal_pos !=
            final_wal.append.global_wal_pos ||
        observer.observer_processed_ingress_sequence !=
            final_wal.append.ingress_sequence ||
        observer.observer_processed_segment_offset !=
            final_wal.append.segment_offset) {
        return false;
    }

    const std::uint64_t added_segments =
        static_cast<std::uint64_t>(
            final_sink_identity.segment_sequence -
            started_runtime.current_segment_sequence);
    std::uint64_t added_header_bytes = 0U;
    std::uint64_t expected_wal_growth = 0U;
    std::uint64_t expected_final_ingress = 0U;
    if (!CheckedMultiply(
            added_segments,
            kRawV1SegmentHeaderBytes,
            &added_header_bytes) ||
        !CheckedAdd(
            capture.append.framed_wal_bytes,
            added_header_bytes,
            &expected_wal_growth) ||
        final_wal.append.global_wal_pos -
                started_runtime.append.global_wal_pos !=
            expected_wal_growth ||
        !CheckedAdd(
            started_runtime.append.ingress_sequence,
            callback.captured_records,
            &expected_final_ingress) ||
        final_wal.append.ingress_sequence !=
            expected_final_ingress) {
        return false;
    }
    if (callback.captured_records == 0U) {
        return capture.append.last_ingress_sequence == 0U &&
               capture.durable.last_ingress_sequence == 0U &&
               callback.captured_ingress_sequence == 0U;
    }
    return capture.append.last_ingress_sequence ==
               final_wal.append.ingress_sequence &&
           capture.durable.last_ingress_sequence ==
               final_wal.durable.ingress_sequence &&
           callback.captured_ingress_sequence ==
               final_wal.append.ingress_sequence;
}

const l2flow::sdk::IngressSpec&
RawIngressApp::ValidateAndGetSpec(
    const RawIngressAppConfigV1& config) {
    const std::string stable_error =
        ValidateRawIngressConfig(config.stable);
    if (!stable_error.empty()) {
        throw std::invalid_argument(
            "invalid Raw ingress config: " +
            stable_error);
    }
    const std::string runtime_error =
        ValidateRawIngressRuntimeState(
            config.stable, config.recovered);
    if (!runtime_error.empty()) {
        throw std::invalid_argument(
            "invalid recovered Raw runtime state: " +
            runtime_error);
    }
    if (config.connect_generation == 0U) {
        throw std::invalid_argument(
            "Raw ingress Connect generation is zero");
    }
    if (config.endpoint == nullptr ||
        config.endpoint->ingress_kind() !=
            config.stable.kind ||
        config.endpoint->contract_sha256() !=
            config.stable.endpoint_contract_sha256 ||
        config.endpoint
            ->resolved_server_address()
            .empty() ||
        HasNul(
            config.endpoint
                ->resolved_server_address()) ||
        config.endpoint->message_encoding() ==
            datayes::mdl::MDLEID_UNDEFINED) {
        throw std::invalid_argument(
            "endpoint contract does not match stable Raw config");
    }
    if (config.credential_token.empty() ||
        config.credential_token.size() > 4096U ||
        HasNul(config.credential_token)) {
        throw std::invalid_argument(
            "Raw ingress credential token is invalid");
    }
    return l2flow::sdk::GetIngressSpec(
        config.stable.kind);
}

std::size_t RawIngressApp::CheckedRingCapacity(
    const RawIngressConfig& config) {
    if (config.ring_capacity_bytes >
        static_cast<std::uint64_t>(
            std::numeric_limits<std::size_t>::max())) {
        throw std::invalid_argument(
            "Raw ingress ring exceeds size_t");
    }
    return static_cast<std::size_t>(
        config.ring_capacity_bytes);
}

CallbackHandlerConfig RawIngressApp::MakeHandlerConfig(
    const RawIngressAppConfigV1& config,
    const l2flow::sdk::IngressSpec& spec) {
    CallbackHandlerConfig result;
    result.source_stream_id = spec.source_stream_id;
    result.market_service_id =
        spec.market_service_id;
    result.max_message_bytes =
        config.stable.max_message_bytes;
    result.capture_date =
        config.recovered.capture_date;
    result.first_ingress_sequence =
        config.recovered
            .recovered_next_ingress_sequence;
    return result;
}

RawCaptureWorkerConfig
RawIngressApp::MakeCaptureConfig(
    const RawIngressConfig& config,
    const RawIngressAppOptionsV1& options,
    RawIngressApp* app) noexcept {
    RawCaptureWorkerConfig result;
    result.durable_interval_ns =
        static_cast<std::uint64_t>(
            config.sync_interval_milliseconds) *
        UINT64_C(1'000'000);
    result.durable_batch_bytes =
        config.sync_bytes;
    result.failure_sink = {
        &RawIngressApp::CaptureFailure, app};
    result.monotonic_now =
        options.worker_monotonic_now;
    result.monotonic_clock_context =
        options.worker_monotonic_clock_context;
    return result;
}

RawReadinessWorkerConfig
RawIngressApp::MakeReadinessWorkerConfig(
    const RawIngressAppOptionsV1& options,
    RawIngressApp* app) noexcept {
    RawReadinessWorkerConfig result;
    result.monotonic_now =
        options.worker_monotonic_now;
    result.monotonic_clock_context =
        options.worker_monotonic_clock_context;
    result.final_catch_up_timeout_ns =
        options.observer_final_catch_up_timeout_ns;
    result.failure_callback =
        &RawIngressApp::ReadinessFailure;
    result.failure_context = app;
    return result;
}

RawIngressApp::RawIngressApp(
    RawIngressAppConfigV1 config,
    std::shared_ptr<l2flow::sdk::SdkFactory>
        sdk_factory,
    std::unique_ptr<CaptureClock> clock,
    std::unique_ptr<RawWalSink> prepared_sink,
    std::unique_ptr<RawLiveTail> live_tail,
    std::unique_ptr<RawIngressCleanStopGateV1>
        clean_stop_gate,
    RawIngressAppOptionsV1 options,
    RawIngressLifecycleObserver* lifecycle_observer)
    : config_(std::move(config)),
      spec_(&ValidateAndGetSpec(config_)),
      sdk_factory_(std::move(sdk_factory)),
      clock_(std::move(clock)),
      sink_(std::move(prepared_sink)),
      live_tail_(std::move(live_tail)),
      clean_stop_gate_(std::move(clean_stop_gate)),
      options_(std::move(options)),
      lifecycle_observer_(lifecycle_observer),
      ring_(
          CheckedRingCapacity(config_.stable),
          config_.stable.max_message_bytes),
      handler_(
          MakeHandlerConfig(config_, *spec_),
          ring_,
          RequireClock(clock_),
          capture_fatal_,
          capture_metrics_),
      sdk_callback_router_(
          std::make_unique<
              RawUnifiedCallbackRouter>(handler_)) {
    static_cast<void>(RequireSink(sink_));
    if (sdk_factory_ == nullptr ||
        live_tail_ == nullptr ||
        clean_stop_gate_ == nullptr) {
        throw std::invalid_argument(
            "Raw ingress runtime dependency is null");
    }
    if (options_.callback_quiesce_timeout <=
            std::chrono::milliseconds::zero() ||
        options_.emergency_writer_pause_timeout <=
            std::chrono::milliseconds::zero() ||
        options_.observer_maximum_lag_bytes == 0U ||
        options_.observer_heartbeat_timeout_ns == 0U ||
        options_
                .observer_final_catch_up_timeout_ns ==
            0U ||
        HasNul(options_.sdk_log_runtime_prefix)) {
        throw std::invalid_argument(
            "invalid Raw ingress app options");
    }
    if (live_tail_->writer_instance() !=
            config_.recovered.writer_instance ||
        live_tail_->stream_day_id() !=
            config_.recovered.stream_day_id ||
        live_tail_->source_stream_id() !=
            config_.recovered.source_stream_id ||
        live_tail_->capture_date() !=
            config_.recovered.capture_date ||
        live_tail_->segment_sequence() !=
            config_.recovered.current_segment_sequence ||
        live_tail_->initial_global_wal_pos() !=
            config_.recovered.append.global_wal_pos ||
        live_tail_->segment_offset() !=
            config_.recovered.append.segment_offset ||
        live_tail_->next_ingress_sequence() !=
            config_.recovered
                .recovered_next_ingress_sequence) {
        throw std::invalid_argument(
            "live tail does not start at recovered append cursor");
    }

    RawReadinessObserverConfig observer_config;
    observer_config.source_stream_id =
        spec_->source_stream_id;
    observer_config.market_service_id =
        spec_->market_service_id;
    observer_config.required_market_messages =
        spec_->required;
    observer_config.maximum_lag_bytes =
        options_.observer_maximum_lag_bytes;
    observer_config.heartbeat_timeout_ns =
        options_.observer_heartbeat_timeout_ns;
    if (RawReadinessObserver::Create(
            std::move(observer_config),
            &readiness_observer_) !=
        RawReadinessObserverCreateError::kNone) {
        throw std::invalid_argument(
            "cannot create Raw readiness observer");
    }

    capture_worker_ =
        std::make_unique<RawCaptureWorker>(
            MakeCaptureConfig(
                config_.stable, options_, this),
            ring_,
            *sink_);
    RawReadinessObserverGeneration generation;
    generation.writer_instance =
        config_.recovered.writer_instance;
    generation.stream_day_id =
        config_.recovered.stream_day_id;
    generation.source_stream_id =
        config_.recovered.source_stream_id;
    generation.capture_date =
        config_.recovered.capture_date;
    generation.connect_generation =
        config_.connect_generation;
    generation.recovery_wal_pos =
        config_.recovered.append.global_wal_pos;
    generation.recovery_next_ingress_sequence =
        config_.recovered
            .recovered_next_ingress_sequence;
    generation.recovery_segment_sequence =
        config_.recovered.current_segment_sequence;
    generation.recovery_segment_offset =
        config_.recovered.append.segment_offset;
    readiness_worker_ =
        std::make_unique<RawReadinessWorker>(
            MakeReadinessWorkerConfig(
                options_, this),
            *live_tail_,
            *readiness_observer_,
            std::move(generation));
}

RawIngressApp::~RawIngressApp() {
    emergency_ack_control_->active_epoch_.store(
        0U, std::memory_order_release);
    RawIngressAppState expected =
        RawIngressAppState::kConstructed;
    if (state_.compare_exchange_strong(
            expected,
            RawIngressAppState::kStopped,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        // Disposal of an object that was never started is intentionally
        // silent, but it is not a clean stop and publishes no evidence.
        return;
    }
    static_cast<void>(Stop(nullptr));
}

bool RawIngressApp::Initialize(
    std::string* error) noexcept {
    std::lock_guard<std::mutex> lock(
        lifecycle_mutex_);
    if (state_.load(std::memory_order_acquire) ==
        RawIngressAppState::kRunning) {
        CopyError(error);
        return !fatal();
    }
    if (state_.load(std::memory_order_acquire) !=
        RawIngressAppState::kConstructed) {
        SetFailureLiteral(
            "RawIngressApp cannot be initialized after stopping");
        CopyError(error);
        return false;
    }
    state_.store(
        RawIngressAppState::kInitializing,
        std::memory_order_release);

    std::string_view stage =
        "prepared Raw sink validation";
    try {
        if (!PreparedSinkMatchesRecovery()) {
            SetFailureLiteral(
                "prepared Raw sink does not match recovered cursor");
            static_cast<void>(StopLocked());
            CopyError(error);
            return false;
        }

        stage = "Raw capture/readiness workers";
        StartWorkers();
        if (fatal()) {
            SetFailureLiteral(
                "Raw worker failed before SDK construction");
            static_cast<void>(StopLocked());
            CopyError(error);
            return false;
        }

        stage = "SdkFactory::Create";
        manager_ = sdk_factory_->Create(
            config_.stable.work_threads,
            config_.stable.io_threads);
        if (manager_ == nullptr) {
            SetFailureLiteral(
                "SdkFactory::Create returned null");
            static_cast<void>(StopLocked());
            CopyError(error);
            return false;
        }

        stage = "SdkManager::EnableLog";
        manager_->EnableLog(
            options_.sdk_log_runtime_prefix.empty()
                ? std::string_view(
                      config_.stable.sdk_log_prefix)
                : std::string_view(
                      options_.sdk_log_runtime_prefix),
            false);
        stage = "SdkManager::CreateSubscriber";
        subscriber_ =
            manager_->CreateSubscriber(
                sdk_callback_router_.get(), false);
        if (subscriber_ == nullptr) {
            SetFailureLiteral(
                "SdkManager::CreateSubscriber returned null");
            static_cast<void>(StopLocked());
            CopyError(error);
            return false;
        }

        stage = "SdkSubscriber::SetServerAddress";
        subscriber_->SetServerAddress(
            config_.endpoint
                ->resolved_server_address());
        stage = "SdkSubscriber::SetUserName";
        subscriber_->SetUserName(
            config_.credential_token);
        stage = "SdkSubscriber::SetHeartbeatInterval";
        subscriber_->SetHeartbeatInterval(
            config_.stable
                .heartbeat_interval_seconds);
        stage = "SdkSubscriber::SetHeartbeatTimeout";
        subscriber_->SetHeartbeatTimeout(
            config_.stable
                .heartbeat_timeout_seconds);
        stage = "SdkSubscriber::SetMessageEncoding";
        subscriber_->SetMessageEncoding(
            config_.endpoint->message_encoding());
        stage = "SdkSubscriber::EnableMergeMessage";
        subscriber_->EnableMergeMessage(
            config_.endpoint->merge_message());
        stage = "SdkSubscriber::SetSendMacAuth";
        subscriber_->SetSendMacAuth(
            config_.endpoint->send_mac_auth());
        stage = "SdkSubscriber::EnableServerSelect";
        subscriber_->EnableServerSelect(
            config_.endpoint->server_select());
        stage = "SdkSubscriber::AddSubscription";
        AddSubscriptions();

        Observe(
            RawIngressLifecycleEvent::
                kSdkConnectStarting);
        stage = "SdkSubscriber::Connect";
        const std::string connect_error =
            subscriber_->Connect();
        if (!connect_error.empty()) {
            SetFailure(
                "SDK Connect failed: " +
                connect_error);
            static_cast<void>(StopLocked());
            CopyError(error);
            return false;
        }
        state_.store(
            RawIngressAppState::kRunning,
            std::memory_order_release);
        if (fatal()) {
            SetFailureLiteral(
                "Raw capture failed during SDK Connect");
            static_cast<void>(StopLocked());
            CopyError(error);
            return false;
        }
        CopyError(error);
        return true;
    } catch (const std::exception& exception) {
        SetFailureException(stage, exception);
    } catch (...) {
        SetFailureLiteral(
            "Raw ingress lifecycle stage threw");
    }

    static_cast<void>(StopLocked());
    CopyError(error);
    return false;
}

bool RawIngressApp::Stop(
    std::string* error) noexcept {
    std::lock_guard<std::mutex> lock(
        lifecycle_mutex_);
    const bool result = StopLocked();
    CopyError(error);
    return result;
}

std::unique_ptr<RawEmergencyWriterAckV1>
RawIngressApp::BeginEmergencyStop(
    std::string* error) noexcept {
    std::lock_guard<std::mutex> lock(
        lifecycle_mutex_);
    const RawIngressAppState current =
        state_.load(std::memory_order_acquire);
    if (emergency_ack_issued_) {
        if (error != nullptr) {
            try {
                *error =
                    "Raw emergency writer ACK was already issued";
            } catch (...) {
            }
        }
        return nullptr;
    }
    if (current != RawIngressAppState::kRunning) {
        SetFailureLiteral(
            "Raw emergency writer ACK requires a Running app");
        CopyError(error);
        return nullptr;
    }
    if (fatal()) {
        SetFailureLiteral(
            "fatal Raw ingress cannot acknowledge emergency writer ownership");
        CopyError(error);
        return nullptr;
    }

    const RawWalSinkIdentityV1 before_identity =
        sink_->identity();
    const RawWalWriterSnapshot before_wal =
        sink_->Snapshot();
    if (!ActiveSinkIdentityValid(
            before_identity, before_wal)) {
        SetFailureLiteral(
            "Raw emergency writer identity/cursor mismatch");
        CopyError(error);
        return nullptr;
    }

    state_.store(
        RawIngressAppState::kStopping,
        std::memory_order_release);
    if (!ShutdownAndQuiesceLocked(true)) {
        static_cast<void>(StopLocked());
        CopyError(error);
        return nullptr;
    }
    if (!capture_worker_->RequestEmergencyPause() ||
        !capture_worker_->WaitForEmergencyPause(
            options_.emergency_writer_pause_timeout)) {
        SetFailureLiteral(
            "Raw writer did not reach the emergency pause barrier");
        static_cast<void>(StopLocked());
        CopyError(error);
        return nullptr;
    }
    Observe(
        RawIngressLifecycleEvent::
            kEmergencyWriterPaused);

    RawEmergencyWriterAckFactsV1 facts;
    facts.started_runtime = config_.recovered;
    facts.writer = sink_->identity();
    facts.callback = capture_metrics_.Snapshot();
    facts.capture = capture_worker_->Snapshot();
    facts.wal = sink_->Snapshot();
    facts.ring_consumed_position =
        ring_.consumed_position();
    facts.ring_published_position =
        ring_.published_position();
    if (facts.ring_published_position >=
        facts.ring_consumed_position) {
        facts.ring_used_bytes =
            facts.ring_published_position -
            facts.ring_consumed_position;
    }
    if (facts.callback.captured_records >=
        facts.capture.append.records) {
        facts.queued_record_count =
            facts.callback.captured_records -
            facts.capture.append.records;
    }
    if (facts.callback.captured_framed_wal_bytes >=
        facts.capture.append.framed_wal_bytes) {
        facts.queued_framed_wal_bytes =
            facts.callback.captured_framed_wal_bytes -
            facts.capture.append.framed_wal_bytes;
    }
    facts.sdk_shutdown_returned =
        sdk_shutdown_returned_;
    facts.callback_quiesced =
        callback_quiesced_;
    facts.regular_writer_mutation_stopped =
        facts.capture.emergency_paused;
    facts.same_process_ring_suffix_retained = true;

    if (!ActiveSinkIdentityValid(
            facts.writer, facts.wal) ||
        facts.ring_used_bytes >
            static_cast<std::uint64_t>(
                ring_.capacity_bytes()) ||
        !facts.exact()) {
        SetFailureLiteral(
            "Raw emergency writer ACK snapshot is not exact");
        static_cast<void>(StopLocked());
        CopyError(error);
        return nullptr;
    }
    if (emergency_ack_epoch_ ==
        std::numeric_limits<std::uint64_t>::max()) {
        SetFailureLiteral(
            "Raw emergency writer ACK epoch exhausted");
        static_cast<void>(StopLocked());
        CopyError(error);
        return nullptr;
    }

    try {
        const std::uint64_t epoch =
            emergency_ack_epoch_ + 1U;
        std::unique_ptr<RawEmergencyWriterAckV1> ack(
            new RawEmergencyWriterAckV1(
                facts,
                this,
                sink_.get(),
                &ring_,
                emergency_ack_control_,
                epoch));
        emergency_ack_epoch_ = epoch;
        emergency_ack_issued_ = true;
        emergency_ack_control_->active_epoch_.store(
            epoch, std::memory_order_release);
        Observe(
            RawIngressLifecycleEvent::
                kEmergencyWriterAckFrozen);
        CopyError(error);
        return ack;
    } catch (...) {
        SetFailureLiteral(
            "Raw emergency writer ACK allocation failed");
        static_cast<void>(StopLocked());
        CopyError(error);
        return nullptr;
    }
}

RawIngressAppState RawIngressApp::state()
    const noexcept {
    return state_.load(std::memory_order_acquire);
}

bool RawIngressApp::fatal() const noexcept {
    return lifecycle_fatal_.load(
               std::memory_order_acquire) ||
           capture_fatal_.tripped();
}

std::string RawIngressApp::last_error() const {
    std::lock_guard<std::mutex> lock(
        lifecycle_mutex_);
    return last_error_;
}

CaptureMetricsSnapshot
RawIngressApp::capture_metrics() const noexcept {
    return capture_metrics_.Snapshot();
}

RawCaptureWorkerSnapshot
RawIngressApp::capture_snapshot() const noexcept {
    return capture_worker_->Snapshot();
}

RawReadinessObserverSnapshot
RawIngressApp::observer_snapshot() const {
    return readiness_observer_->Snapshot();
}

RawCaptureReconciliation
RawIngressApp::reconciliation() const noexcept {
    return capture_worker_->Reconcile(
        capture_metrics_.Snapshot());
}

std::string RawIngressApp::prometheus_metrics() const {
    const CaptureMetricsSnapshot callback =
        capture_metrics_.Snapshot();
    const RawCaptureWorkerSnapshot capture =
        capture_worker_->Snapshot();
    const RawReadinessObserverSnapshot observer =
        readiness_observer_->Snapshot();
    const RawWalWriterSnapshot wal =
        sink_->Snapshot();
    const RawCaptureReconciliation exact =
        capture_worker_->Reconcile(callback);
    const RawWalFailure wal_failure =
        sink_->failure();
    const std::string_view kind =
        l2flow::sdk::ToString(config_.stable.kind);
    const std::uint32_t source_stream_id =
        spec_->source_stream_id;
    const std::uint64_t durability_lag_bytes =
        wal.append.global_wal_pos >=
                wal.durable.global_wal_pos
            ? wal.append.global_wal_pos -
                  wal.durable.global_wal_pos
            : 0U;
    const std::uint64_t observer_lag_bytes =
        wal.append.global_wal_pos >=
                observer.observer_processed_wal_pos
            ? wal.append.global_wal_pos -
                  observer.observer_processed_wal_pos
            : 0U;

    std::ostringstream output;
    output
        << "l2flow_raw_build_info{ingress_kind=\""
        << kind
        << "\",source_stream_id=\""
        << source_stream_id
        << "\"} 1\n"
        << "l2flow_raw_runtime_info{ingress_kind=\""
        << kind
        << "\",source_stream_id=\""
        << source_stream_id
        << "\",capture_date=\""
        << config_.recovered.capture_date
        << "\",stream_day_id=\""
        << l2flow::common::Identity128Hex(
               config_.recovered.stream_day_id)
        << "\",writer_instance=\""
        << l2flow::common::Identity128Hex(
               config_.recovered.writer_instance)
        << "\",connect_generation=\""
        << config_.connect_generation
        << "\"} 1\n"
        << "l2flow_raw_callback_records_total "
        << callback.captured_records << '\n'
        << "l2flow_raw_callback_vendor_bytes_total "
        << callback.captured_vendor_bytes << '\n'
        << "l2flow_raw_append_records_total "
        << capture.append.records << '\n'
        << "l2flow_raw_append_vendor_bytes_total "
        << capture.append.vendor_bytes << '\n'
        << "l2flow_raw_append_framed_wal_bytes_total "
        << capture.append.framed_wal_bytes << '\n'
        << "l2flow_raw_durable_records_total "
        << capture.durable.records << '\n'
        << "l2flow_raw_durable_vendor_bytes_total "
        << capture.durable.vendor_bytes << '\n'
        << "l2flow_raw_durable_framed_wal_bytes_total "
        << capture.durable.framed_wal_bytes << '\n'
        << "l2flow_raw_append_global_wal_pos "
        << wal.append.global_wal_pos << '\n'
        << "l2flow_raw_durable_global_wal_pos "
        << wal.durable.global_wal_pos << '\n'
        << "l2flow_raw_append_ingress_sequence "
        << wal.append.ingress_sequence << '\n'
        << "l2flow_raw_durable_ingress_sequence "
        << wal.durable.ingress_sequence << '\n'
        << "l2flow_raw_append_segment_offset "
        << wal.append.segment_offset << '\n'
        << "l2flow_raw_durable_segment_offset "
        << wal.durable.segment_offset << '\n'
        << "l2flow_raw_current_segment_sequence "
        << sink_->identity().segment_sequence << '\n'
        << "l2flow_raw_durability_lag_bytes "
        << durability_lag_bytes << '\n'
        << "l2flow_raw_ring_used_bytes "
        << ring_.used_bytes() << '\n'
        << "l2flow_raw_ring_capacity_bytes "
        << config_.stable.ring_capacity_bytes << '\n'
        << "l2flow_raw_observer_processed_wal_pos "
        << observer.observer_processed_wal_pos << '\n'
        << "l2flow_raw_observer_processed_ingress_sequence "
        << observer.observer_processed_ingress_sequence
        << '\n'
        << "l2flow_raw_observer_lag_bytes "
        << observer_lag_bytes << '\n'
        << "l2flow_raw_observer_healthy "
        << (observer.observer_healthy ? 1 : 0) << '\n'
        << "l2flow_raw_observational_evidence_complete "
        << (observer.evidence_complete ? 1 : 0) << '\n'
        << "l2flow_raw_reconciliation_exact "
        << (exact.exact() ? 1 : 0) << '\n'
        << "l2flow_raw_fatal "
        << (fatal() ? 1 : 0) << '\n'
        << "l2flow_raw_fatal_reason "
        << static_cast<std::uint32_t>(
               capture_fatal_.reason())
        << '\n'
        << "l2flow_raw_wal_failure_kind "
        << static_cast<std::uint32_t>(
               wal_failure.kind)
        << '\n'
        << "l2flow_raw_wal_failure_errno "
        << wal_failure.error_number << '\n';
    return output.str();
}

RawObservationalGateResult
RawIngressApp::EvaluateReadiness(
    const RawControlSnapshot& sampled_control,
    std::uint64_t now_monotonic_ns) const {
    RawObservationalGateResult result =
        readiness_observer_->Evaluate(
            sampled_control, now_monotonic_ns);
    if (state_.load(std::memory_order_acquire) !=
            RawIngressAppState::kRunning ||
        fatal()) {
        result.ready = false;
    }
    return result;
}

void RawIngressApp::AddSubscriptions() {
    for (const l2flow::sdk::MessageKey& key :
         spec_->required) {
        subscriber_->AddSubscription(key);
    }
    if (config_.stable.include_optional_index) {
        for (const l2flow::sdk::MessageKey& key :
             spec_->optional) {
            subscriber_->AddSubscription(key);
        }
    }
}

void RawIngressApp::StartWorkers() {
    if (capture_start_attempted_ ||
        readiness_start_attempted_ ||
        capture_thread_.joinable() ||
        readiness_thread_.joinable()) {
        throw std::logic_error(
            "Raw ingress workers already started");
    }
    capture_start_attempted_ = true;
    capture_thread_ = std::thread(
        [this]() noexcept {
            const bool result =
                capture_worker_->Run();
            capture_result_.store(
                result, std::memory_order_relaxed);
            capture_result_known_.store(
                true, std::memory_order_release);
        });
    while (!capture_worker_->startup_complete()) {
        std::this_thread::yield();
    }
    if (!capture_worker_->startup_succeeded()) {
        throw std::runtime_error(
            "Raw capture worker rejected prepared sink");
    }
    Observe(
        RawIngressLifecycleEvent::
            kCaptureWorkerStarted);

    readiness_start_attempted_ = true;
    readiness_thread_ = std::thread(
        [this]() noexcept {
            const bool result =
                readiness_worker_->Run();
            readiness_result_.store(
                result, std::memory_order_relaxed);
            readiness_result_known_.store(
                true, std::memory_order_release);
        });
    while (!readiness_worker_->Snapshot()
                .startup_complete) {
        std::this_thread::yield();
    }
    if (!readiness_worker_->Snapshot()
             .startup_succeeded) {
        throw std::runtime_error(
            "Raw readiness worker rejected generation");
    }
    Observe(
        RawIngressLifecycleEvent::
            kReadinessWorkerStarted);
}

bool RawIngressApp::PreparedSinkMatchesRecovery()
    const noexcept {
    const RawWalWriterSnapshot snapshot =
        sink_->Snapshot();
    const RawWalSinkIdentityV1 identity =
        sink_->identity();
    return snapshot.initialized &&
           !snapshot.sealed &&
           !snapshot.closed &&
           !snapshot.fatal &&
           identity.writer_instance ==
               config_.recovered.writer_instance &&
           identity.stream_day_id ==
               config_.recovered.stream_day_id &&
           identity.source_stream_id ==
               config_.recovered.source_stream_id &&
           identity.capture_date ==
               config_.recovered.capture_date &&
           identity.segment_sequence ==
               config_.recovered
                   .current_segment_sequence &&
           snapshot.append.global_wal_pos >=
               snapshot.append.segment_offset &&
           identity.segment_base_wal_pos ==
               snapshot.append.global_wal_pos -
                   snapshot.append.segment_offset &&
           snapshot.append == snapshot.durable &&
           snapshot.append.global_wal_pos ==
               config_.recovered.append.global_wal_pos &&
           snapshot.append.ingress_sequence ==
               config_.recovered.append.ingress_sequence &&
           snapshot.append.segment_offset ==
               config_.recovered.append.segment_offset &&
           config_.recovered.append.global_wal_pos ==
               config_.recovered.durable.global_wal_pos &&
           config_.recovered.append.ingress_sequence ==
               config_.recovered.durable.ingress_sequence &&
           config_.recovered.append.segment_offset ==
               config_.recovered.durable.segment_offset;
}

bool RawIngressApp::ActiveSinkIdentityValid(
    const RawWalSinkIdentityV1& identity,
    const RawWalWriterSnapshot& wal) const noexcept {
    return wal.initialized &&
           !wal.sealed &&
           !wal.closed &&
           !wal.fatal &&
           identity.writer_instance ==
               config_.recovered.writer_instance &&
           identity.stream_day_id ==
               config_.recovered.stream_day_id &&
           identity.source_stream_id ==
               config_.recovered.source_stream_id &&
           identity.capture_date ==
               config_.recovered.capture_date &&
           identity.segment_sequence >=
               config_.recovered
                   .current_segment_sequence &&
           wal.append.global_wal_pos >=
               wal.append.segment_offset &&
           identity.segment_base_wal_pos ==
               wal.append.global_wal_pos -
                   wal.append.segment_offset &&
           wal.append.global_wal_pos >=
               wal.durable.global_wal_pos &&
           wal.append.ingress_sequence >=
               wal.durable.ingress_sequence;
}

bool RawIngressApp::ShutdownAndQuiesceLocked(
    bool preserve_callbacks_until_shutdown) noexcept {
    const auto begin_handler_stopping = [this]() noexcept {
        if (handler_stopping_begun_) {
            return;
        }
        handler_.BeginStopping();
        handler_stopping_begun_ = true;
        Observe(
            RawIngressLifecycleEvent::
                kHandlerBeginStopping);
    };
    const auto shutdown_sdk = [this]() noexcept {
        if (sdk_shutdown_returned_) {
            return;
        }
        if (manager_ != nullptr) {
            try {
                manager_->Shutdown();
            } catch (const std::exception& exception) {
                SetFailureException(
                    "SdkManager::Shutdown", exception);
                std::terminate();
            } catch (...) {
                SetFailureLiteral(
                    "SdkManager::Shutdown threw");
                std::terminate();
            }
        }
        sdk_shutdown_returned_ = true;
    };
    if (preserve_callbacks_until_shutdown) {
        // Phase-2 emergency semantics: STOPPING/READY revocation has already
        // happened, but callbacks that the SDK began before Shutdown returns
        // must still publish into the existing ring. The callback gate is
        // closed immediately after that SDK barrier, before Quiesce.
        shutdown_sdk();
        begin_handler_stopping();
    } else {
        // Preserve the established ordinary Stop ordering.
        begin_handler_stopping();
        shutdown_sdk();
    }
    if (!callback_quiesced_) {
        if (!handler_.Quiesce(
                options_.callback_quiesce_timeout)) {
            SetFailureLiteral(
                "Raw callback handler did not quiesce");
            std::terminate();
        }
        callback_quiesced_ = true;
        Observe(
            RawIngressLifecycleEvent::
                kHandlerQuiesced);
    }
    return true;
}

bool RawIngressApp::StopLocked() noexcept {
    const RawIngressAppState current =
        state_.load(std::memory_order_acquire);
    if (current == RawIngressAppState::kStopped) {
        return !fatal();
    }
    if (current == RawIngressAppState::kConstructed) {
        SetFailureLiteral(
            "RawIngressApp was stopped before initialization; no clean-stop evidence exists");
        state_.store(
            RawIngressAppState::kStopped,
            std::memory_order_release);
        return false;
    }
    if (emergency_ack_issued_) {
        return StopEmergencyLocked();
    }
    state_.store(
        RawIngressAppState::kStopping,
        std::memory_order_release);

    if (!ShutdownAndQuiesceLocked(false)) {
        return false;
    }

    bool capture_clean = false;
    if (capture_start_attempted_) {
        capture_worker_->StopAndDrain();
        if (capture_thread_.joinable()) {
            try {
                capture_thread_.join();
            } catch (...) {
                SetFailureLiteral(
                    "Raw capture worker join failed");
            }
        } else if (!capture_result_known_.load(
                       std::memory_order_acquire)) {
            const bool result =
                capture_worker_->Run();
            capture_result_.store(
                result, std::memory_order_relaxed);
            capture_result_known_.store(
                true, std::memory_order_release);
        }
        capture_clean =
            capture_result_known_.load(
                std::memory_order_acquire) &&
            capture_result_.load(
                std::memory_order_relaxed);
        if (!capture_clean) {
            SetFailureLiteral(
                "Raw capture worker did not drain and seal cleanly");
        }
        Observe(
            RawIngressLifecycleEvent::
                kCaptureWorkerJoined);
    }

    const RawWalWriterSnapshot final_wal =
        sink_->Snapshot();
    const RawWalSinkIdentityV1 final_sink_identity =
        sink_->identity();
    bool readiness_clean = false;
    if (readiness_start_attempted_) {
        if (!capture_clean ||
            !final_wal.sealed ||
            !final_wal.closed ||
            final_wal.fatal ||
            final_wal.append !=
                final_wal.durable ||
            final_sink_identity.writer_instance !=
                config_.recovered.writer_instance ||
            final_sink_identity.stream_day_id !=
                config_.recovered.stream_day_id ||
            final_sink_identity.source_stream_id !=
                config_.recovered.source_stream_id ||
            final_sink_identity.capture_date !=
                config_.recovered.capture_date ||
            final_wal.append.global_wal_pos <
                final_wal.append.segment_offset ||
            final_sink_identity
                    .segment_base_wal_pos !=
                final_wal.append.global_wal_pos -
                    final_wal.append.segment_offset) {
            SetFailureLiteral(
                "cannot arm Raw observer final catch-up");
            readiness_worker_->Abort();
        } else {
            RawReadinessStopCursorV1 final_cursor;
            final_cursor.writer_instance =
                final_sink_identity.writer_instance;
            final_cursor.stream_day_id =
                final_sink_identity.stream_day_id;
            final_cursor.source_stream_id =
                final_sink_identity.source_stream_id;
            final_cursor.capture_date =
                final_sink_identity.capture_date;
            final_cursor.segment_sequence =
                final_sink_identity.segment_sequence;
            final_cursor.global_wal_pos =
                final_wal.append.global_wal_pos;
            final_cursor.ingress_sequence =
                final_wal.append.ingress_sequence;
            final_cursor.segment_offset =
                final_wal.append.segment_offset;
            if (!readiness_worker_->StopAt(
                    final_cursor)) {
                SetFailureLiteral(
                    "cannot arm Raw observer final catch-up");
                readiness_worker_->Abort();
            }
        }
        if (readiness_thread_.joinable()) {
            try {
                readiness_thread_.join();
            } catch (...) {
                SetFailureLiteral(
                    "Raw readiness worker join failed");
            }
        } else if (!readiness_result_known_.load(
                       std::memory_order_acquire)) {
            const bool result =
                readiness_worker_->Run();
            readiness_result_.store(
                result, std::memory_order_relaxed);
            readiness_result_known_.store(
                true, std::memory_order_release);
        }
        readiness_clean =
            readiness_result_known_.load(
                std::memory_order_acquire) &&
            readiness_result_.load(
                std::memory_order_relaxed);
        if (!readiness_clean) {
            SetFailureLiteral(
                "Raw readiness observer did not catch up cleanly");
        }
        Observe(
            RawIngressLifecycleEvent::
                kReadinessWorkerJoined);
    }

    const RawCaptureReconciliation exact =
        reconciliation();
    if (capture_clean && readiness_clean &&
        !exact.exact()) {
        SetFailureLiteral(
            "Raw clean-stop reconciliation is not exact");
    }
    if (capture_clean && readiness_clean &&
        exact.exact() && !fatal()) {
        RawIngressCleanStopEvidenceV1 evidence;
        evidence.started_runtime =
            config_.recovered;
        evidence.callback =
            capture_metrics_.Snapshot();
        evidence.capture =
            capture_worker_->Snapshot();
        evidence.observer =
            readiness_observer_->Snapshot();
        evidence.final_sink_identity =
            final_sink_identity;
        evidence.final_wal = final_wal;
        evidence.reconciliation = exact;
        evidence.ring_used_bytes =
            static_cast<std::uint64_t>(
                ring_.used_bytes());
        if (!evidence.exact()) {
            SetFailureLiteral(
                "Raw clean-stop WAL/observer reconciliation is not exact");
        } else if (!clean_stop_gate_->Complete(evidence)) {
            SetFailureLiteral(
                "Raw sealed-certificate/coordinator barrier failed");
        } else {
            Observe(
                RawIngressLifecycleEvent::
                    kCleanStopBarrierComplete);
        }
    }

    if (subscriber_ != nullptr) {
        std::string release_error;
        if (!subscriber_->Release(
                &release_error)) {
            SetFailure(
                release_error.empty()
                    ? "Raw Subscriber release failed"
                    : std::move(release_error));
            std::terminate();
        }
        subscriber_.reset();
    }
    if (manager_ != nullptr) {
        std::string release_error;
        if (!manager_->Release(
                &release_error)) {
            SetFailure(
                release_error.empty()
                    ? "Raw IOManager release failed"
                    : std::move(release_error));
            std::terminate();
        }
        manager_.reset();
    }
    state_.store(
        RawIngressAppState::kStopped,
        std::memory_order_release);
    return !fatal();
}

bool RawIngressApp::StopEmergencyLocked() noexcept {
    emergency_ack_control_->active_epoch_.store(
        0U, std::memory_order_release);
    SetFailureLiteral(
        emergency_ack_consumed_
            ? "Raw emergency finalization did not complete the normal clean-stop gate"
            : "Raw emergency writer ACK was abandoned before finalization");

    if (capture_thread_.joinable()) {
        capture_worker_->AbandonEmergencyPause();
        try {
            capture_thread_.join();
        } catch (...) {
            SetFailureLiteral(
                "Raw emergency capture worker join failed");
        }
        Observe(
            RawIngressLifecycleEvent::
                kCaptureWorkerJoined);
    }

    if (readiness_start_attempted_) {
        readiness_worker_->Abort();
        if (readiness_thread_.joinable()) {
            try {
                readiness_thread_.join();
            } catch (...) {
                SetFailureLiteral(
                    "Raw emergency readiness worker join failed");
            }
            Observe(
                RawIngressLifecycleEvent::
                    kReadinessWorkerJoined);
        }
    }

    if (subscriber_ != nullptr) {
        std::string release_error;
        if (!subscriber_->Release(
                &release_error)) {
            SetFailure(
                release_error.empty()
                    ? "Raw Subscriber release failed"
                    : std::move(release_error));
            std::terminate();
        }
        subscriber_.reset();
    }
    if (manager_ != nullptr) {
        std::string release_error;
        if (!manager_->Release(
                &release_error)) {
            SetFailure(
                release_error.empty()
                    ? "Raw IOManager release failed"
                    : std::move(release_error));
            std::terminate();
        }
        manager_.reset();
    }
    state_.store(
        RawIngressAppState::kStopped,
        std::memory_order_release);
    return false;
}

bool RawIngressApp::
ConsumeEmergencyWriterAckForFinalization(
    RawEmergencyWriterAckV1& ack,
    RawWalSink** writer,
    ByteRing** ring,
    std::string* error) noexcept {
    if (writer != nullptr) {
        *writer = nullptr;
    }
    if (ring != nullptr) {
        *ring = nullptr;
    }
    std::lock_guard<std::mutex> lock(
        lifecycle_mutex_);
    const RawCaptureWorkerSnapshot current_capture =
        capture_worker_->Snapshot();
    const CaptureMetricsSnapshot current_callback =
        capture_metrics_.Snapshot();
    const RawWalWriterSnapshot current_wal =
        sink_->Snapshot();
    const RawWalSinkIdentityV1 current_identity =
        sink_->identity();
    const bool current_ring_matches =
        ring_.published_position() ==
            ack.facts_.ring_published_position &&
        ring_.consumed_position() ==
            ack.facts_.ring_consumed_position &&
        static_cast<std::uint64_t>(
            ring_.used_bytes()) ==
            ack.facts_.ring_used_bytes;
    if (writer == nullptr || ring == nullptr ||
        state_.load(std::memory_order_acquire) !=
            RawIngressAppState::kStopping ||
        !emergency_ack_issued_ ||
        emergency_ack_consumed_ ||
        ack.consumed_ ||
        ack.owner_ != this ||
        ack.writer_ != sink_.get() ||
        ack.ring_ != &ring_ ||
        ack.control_ != emergency_ack_control_ ||
        ack.epoch_ == 0U ||
        ack.epoch_ != emergency_ack_epoch_ ||
        emergency_ack_control_->active_epoch_.load(
            std::memory_order_acquire) !=
            ack.epoch_ ||
        !ack.facts_.exact() ||
        !sdk_shutdown_returned_ ||
        !callback_quiesced_ ||
        !ActiveSinkIdentityValid(
            current_identity, current_wal) ||
        current_identity != ack.facts_.writer ||
        !SameWalSnapshot(
            current_wal, ack.facts_.wal) ||
        !SameCaptureMetrics(
            current_callback,
            ack.facts_.callback) ||
        !SameCaptureWorkerSnapshot(
            current_capture,
            ack.facts_.capture) ||
        !current_ring_matches ||
        fatal()) {
        if (error != nullptr) {
            try {
                *error =
                    "Raw emergency writer ACK is stale, foreign, or already consumed";
            } catch (...) {
            }
        }
        return false;
    }

    capture_worker_->AbandonEmergencyPause();
    if (capture_thread_.joinable()) {
        try {
            capture_thread_.join();
        } catch (...) {
            SetFailureLiteral(
                "Raw emergency capture worker handoff join failed");
            CopyError(error);
            return false;
        }
    }
    if (!capture_worker_->Snapshot()
             .emergency_abandoned ||
        sink_->identity() != current_identity ||
        !SameWalSnapshot(
            sink_->Snapshot(), current_wal) ||
        ring_.published_position() !=
            ack.facts_.ring_published_position ||
        ring_.consumed_position() !=
            ack.facts_.ring_consumed_position) {
        SetFailureLiteral(
            "Raw emergency writer changed during finalization handoff");
        CopyError(error);
        return false;
    }

    ack.consumed_ = true;
    emergency_ack_consumed_ = true;
    emergency_ack_control_->active_epoch_.store(
        0U, std::memory_order_release);
    *writer = sink_.get();
    *ring = &ring_;
    CopyError(error);
    return true;
}

void RawIngressApp::SetFailure(
    std::string message) noexcept {
    lifecycle_fatal_.store(
        true, std::memory_order_release);
    if (!last_error_.empty()) {
        return;
    }
    try {
        last_error_ = std::move(message);
    } catch (...) {
    }
}

void RawIngressApp::SetFailureLiteral(
    const char* message) noexcept {
    try {
        SetFailure(std::string(message));
    } catch (...) {
        lifecycle_fatal_.store(
            true, std::memory_order_release);
    }
}

void RawIngressApp::SetFailureException(
    std::string_view stage,
    const std::exception& exception) noexcept {
    try {
        SetFailure(
            std::string(stage) +
            " threw: " + exception.what());
    } catch (...) {
        SetFailureLiteral(
            "Raw ingress lifecycle stage threw");
    }
}

void RawIngressApp::CopyError(
    std::string* error) const noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        *error = last_error_;
    } catch (...) {
    }
}

void RawIngressApp::Observe(
    RawIngressLifecycleEvent event) noexcept {
    if (lifecycle_observer_ != nullptr) {
        lifecycle_observer_->Observe(event);
    }
}

void RawIngressApp::CaptureFailure(
    void* context,
    RawCaptureFatalSignal signal) noexcept {
    auto* app =
        static_cast<RawIngressApp*>(context);
    const l2flow::ops::FatalReason reason =
        signal ==
                RawCaptureFatalSignal::
                    kRingCorruption
            ? l2flow::ops::FatalReason::
                  RING_CORRUPTION
            : l2flow::ops::FatalReason::
                  RAW_WAL_IO;
    static_cast<void>(
        app->capture_fatal_.trip(reason));
}

void RawIngressApp::ReadinessFailure(
    void* context,
    RawReadinessWorkerFailureKind) noexcept {
    auto* app =
        static_cast<RawIngressApp*>(context);
    static_cast<void>(
        app->capture_fatal_.trip(
            l2flow::ops::FatalReason::
                RAW_READINESS_OBSERVER));
}

}  // namespace l2flow::ingress
