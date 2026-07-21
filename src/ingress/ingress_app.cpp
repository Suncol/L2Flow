#include "l2flow/ingress/ingress_app.h"

#include "l2flow/build_manifest.h"

#include <chrono>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace l2flow::ingress {

class UnifiedCallbackRouter final
    : public datayes::mdl::MessageHandlerBase {
public:
    explicit UnifiedCallbackRouter(
        CallbackHandler& handler) noexcept
        : handler_(handler) {}

    void OnMessage(
        datayes::mdl::Subscriber*,
        const datayes::mdl::MDLMessage* message) noexcept override {
        // All four accepted callback families intentionally share the same
        // CallbackHandler capture path. In particular, do not inspect the
        // vendor head here: BeginStopping() must gate a late SDK callback
        // before any vendor-memory access.
        handler_.OnMDLAPIMessage(message);
    }

private:
    CallbackHandler& handler_;
};

namespace {

CaptureClock& RequireClock(
    const std::unique_ptr<CaptureClock>& clock) {
    if (clock == nullptr) {
        throw std::invalid_argument("capture clock is null");
    }
    return *clock;
}

}  // namespace

const l2flow::sdk::IngressSpec& IngressApp::ValidateAndGetSpec(
    const l2flow::sdk::IngressConfig& config) {
    const std::string error =
        l2flow::sdk::ValidateIngressConfig(config);
    if (!error.empty()) {
        throw std::invalid_argument(
            "invalid ingress config: " + error);
    }
    return l2flow::sdk::GetIngressSpec(config.kind);
}

std::size_t IngressApp::CheckedRingCapacity(
    const l2flow::sdk::IngressConfig& config) {
    if (config.ring_capacity_bytes >
        static_cast<std::uint64_t>(
            std::numeric_limits<std::size_t>::max())) {
        throw std::invalid_argument(
            "ring capacity exceeds size_t");
    }
    return static_cast<std::size_t>(
        config.ring_capacity_bytes);
}

CallbackHandlerConfig IngressApp::MakeHandlerConfig(
    const l2flow::sdk::IngressConfig& config,
    const l2flow::sdk::IngressSpec& spec) {
    CallbackHandlerConfig result;
    result.source_stream_id = spec.source_stream_id;
    result.market_service_id = spec.market_service_id;
    result.max_message_bytes = config.max_message_bytes;
    result.capture_date = config.capture_date;
    result.first_ingress_sequence =
        config.first_ingress_sequence;
    return result;
}

ShadowCaptureConfig IngressApp::MakeShadowConfig(
    const l2flow::sdk::IngressSpec& spec) {
    ShadowCaptureConfig result;
    result.source_stream_id = spec.source_stream_id;
    result.market_service_id = spec.market_service_id;
    result.required_market_messages = spec.required;
    return result;
}

IngressApp::IngressApp(
    l2flow::sdk::IngressConfig config,
    std::shared_ptr<l2flow::sdk::SdkFactory> sdk_factory,
    std::unique_ptr<CaptureClock> clock,
    std::unique_ptr<ShadowCaptureOutput> shadow_output,
    IngressAppOptions options,
    IngressLifecycleObserver* observer)
    : config_(std::move(config)),
      spec_(&ValidateAndGetSpec(config_)),
      sdk_factory_(std::move(sdk_factory)),
      clock_(std::move(clock)),
      pending_shadow_output_(std::move(shadow_output)),
      options_(options),
      observer_(observer),
      ring_(CheckedRingCapacity(config_),
            config_.max_message_bytes),
      handler_(
          MakeHandlerConfig(config_, *spec_),
          ring_,
          RequireClock(clock_),
          capture_fatal_,
          capture_metrics_),
      sdk_callback_router_(
          std::make_unique<UnifiedCallbackRouter>(
              handler_)) {
    if (sdk_factory_ == nullptr) {
        throw std::invalid_argument("SDK factory is null");
    }
    if (options_.callback_quiesce_timeout <=
        std::chrono::milliseconds::zero()) {
        throw std::invalid_argument(
            "callback quiesce timeout must be positive");
    }
    if (options_.sdk_log_runtime_prefix.find('\0') !=
        std::string::npos) {
        throw std::invalid_argument(
            "runtime SDK log prefix contains NUL");
    }
}

IngressApp::~IngressApp() {
    static_cast<void>(Stop(nullptr));
}

bool IngressApp::Initialize(std::string* error) noexcept {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (state_.load(std::memory_order_acquire) ==
        IngressAppState::Running) {
        CopyError(error);
        return !fatal();
    }
    if (state_.load(std::memory_order_acquire) !=
        IngressAppState::Constructed) {
        SetFailureLiteral(
            "IngressApp cannot be initialized after stopping");
        CopyError(error);
        return false;
    }

    state_.store(
        IngressAppState::Initializing,
        std::memory_order_release);
    std::string_view stage = "SdkFactory::Create";
    try {
        manager_ = sdk_factory_->Create(
            config_.work_threads, config_.io_threads);
        if (manager_ == nullptr) {
            SetFailureLiteral("SdkFactory::Create returned null");
            static_cast<void>(StopLocked());
            CopyError(error);
            return false;
        }

        stage = "SdkManager::EnableLog";
        manager_->EnableLog(
            options_.sdk_log_runtime_prefix.empty()
                ? std::string_view(config_.sdk_log_prefix)
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

        // Keep this setter order identical to design.md 4.2.
        stage = "SdkSubscriber::SetServerAddress";
        subscriber_->SetServerAddress(
            config_.endpoint.resolved_server_address);
        stage = "SdkSubscriber::SetUserName";
        subscriber_->SetUserName(config_.token);
        stage = "SdkSubscriber::SetHeartbeatInterval";
        subscriber_->SetHeartbeatInterval(
            config_.heartbeat_interval_seconds);
        stage = "SdkSubscriber::SetHeartbeatTimeout";
        subscriber_->SetHeartbeatTimeout(
            config_.heartbeat_timeout_seconds);
        stage = "SdkSubscriber::SetMessageEncoding";
        subscriber_->SetMessageEncoding(
            config_.endpoint.message_encoding);
        stage = "SdkSubscriber::EnableMergeMessage";
        subscriber_->EnableMergeMessage(
            config_.endpoint.merge_message);
        stage = "SdkSubscriber::SetSendMacAuth";
        subscriber_->SetSendMacAuth(
            *config_.endpoint.send_mac_auth);
        stage = "SdkSubscriber::EnableServerSelect";
        subscriber_->EnableServerSelect(
            config_.endpoint.server_select);

        stage = "SdkSubscriber::AddSubscription";
        AddSubscriptions();

        stage = "ShadowCaptureWriter::Run";
        StartShadowWriter();
        if (capture_fatal_.tripped()) {
            SetFailureLiteral(
                "shadow writer failed before SDK Connect");
            static_cast<void>(StopLocked());
            CopyError(error);
            return false;
        }

        stage = "SdkSubscriber::Connect";
        const std::string connect_error =
            subscriber_->Connect();
        if (!connect_error.empty()) {
            SetFailure("SDK Connect failed: " + connect_error);
            static_cast<void>(StopLocked());
            CopyError(error);
            return false;
        }

        state_.store(
            IngressAppState::Running,
            std::memory_order_release);
        if (fatal()) {
            SetFailureLiteral(
                "capture failed during SDK Connect");
            static_cast<void>(StopLocked());
            CopyError(error);
            return false;
        }
        CopyError(error);
        return true;
    } catch (const std::exception& exception) {
        SetFailureException(stage, exception);
    } catch (...) {
        SetFailureStage(
            stage, " threw an unknown exception");
    }

    static_cast<void>(StopLocked());
    CopyError(error);
    return false;
}

bool IngressApp::Stop(std::string* error) noexcept {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    const bool result = StopLocked();
    CopyError(error);
    return result;
}

IngressAppState IngressApp::state() const noexcept {
    return state_.load(std::memory_order_acquire);
}

bool IngressApp::fatal() const noexcept {
    return lifecycle_fatal_.load(std::memory_order_acquire) ||
           capture_fatal_.tripped();
}

std::string IngressApp::last_error() const {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    return last_error_;
}

CaptureMetricsSnapshot IngressApp::capture_metrics() const noexcept {
    return capture_metrics_.Snapshot();
}

ShadowCaptureStats IngressApp::shadow_stats() const noexcept {
    const ShadowCaptureWriter* const writer =
        published_shadow_writer_.load(std::memory_order_acquire);
    if (writer == nullptr) {
        return {};
    }
    return writer->Snapshot();
}

bool IngressApp::ready_for_shadow() const noexcept {
    return shadow_readiness_generation() != 0U;
}

std::uint64_t
IngressApp::shadow_readiness_generation() const noexcept {
    const ShadowCaptureWriter* const writer =
        published_shadow_writer_.load(std::memory_order_acquire);
    if (state_.load(std::memory_order_acquire) !=
            IngressAppState::Running ||
        fatal() || writer == nullptr) {
        return 0U;
    }
    return writer->readiness_generation();
}

ShadowCaptureReconciliation
IngressApp::reconciliation() const noexcept {
    const ShadowCaptureWriter* const writer =
        published_shadow_writer_.load(std::memory_order_acquire);
    if (writer == nullptr) {
        const CaptureMetricsSnapshot callback =
            capture_metrics_.Snapshot();
        return ShadowCaptureReconciliation{
            callback.captured_records,
            0U,
            callback.captured_vendor_bytes,
            0U};
    }
    return writer->Reconcile(
        capture_metrics_.Snapshot());
}

std::string IngressApp::prometheus_metrics() const {
    std::ostringstream output;
    output << RenderPrometheusMetrics(
        spec_->service_name,
        spec_->source_stream_id,
        capture_metrics_.Snapshot(),
        static_cast<std::uint64_t>(ring_.used_bytes()),
        static_cast<std::uint64_t>(ring_.capacity_bytes()),
        fatal());
    output << "mdl_build_info{service=\"" << spec_->service_name
           << "\",source_stream_id=\"" << spec_->source_stream_id
           << "\",build_manifest_sha256=\""
           << l2flow::build_manifest::kSha256
           << "\",sdk_baseline_sha256=\""
           << l2flow::build_manifest::kSdkBaselineSha256
           << "\",config_sha256=\""
           << l2flow::sdk::IngressConfigSha256(config_)
           << "\"} 1\n";

    const ShadowCaptureStats shadow = shadow_stats();
    const auto common_label = [this, &output]() {
        output << "{service=\"" << spec_->service_name
               << "\",source_stream_id=\"" << spec_->source_stream_id
               << "\"}";
    };
    output << "mdl_shadow_sink_records_total";
    common_label();
    output << ' ' << shadow.sink_records << '\n';
    output << "mdl_shadow_sink_vendor_bytes_total";
    common_label();
    output << ' ' << shadow.sink_vendor_bytes << '\n';
    output << "mdl_shadow_sink_file_bytes";
    common_label();
    output << ' ' << shadow.sink_file_bytes << '\n';
    output << "mdl_shadow_last_ingress_sequence";
    common_label();
    output << ' ' << shadow.last_ingress_sequence << '\n';
    output << "mdl_shadow_logon_response_headers_total";
    common_label();
    output << ' ' << shadow.logon_response_headers << '\n';
    output << "mdl_shadow_subscribe_response_headers_total";
    common_label();
    output << ' ' << shadow.subscribe_response_headers << '\n';
    output << "mdl_shadow_logon_ok_responses_total";
    common_label();
    output << ' ' << shadow.logon_ok_responses << '\n';
    output << "mdl_shadow_logon_failed_responses_total";
    common_label();
    output << ' ' << shadow.logon_failed_responses << '\n';
    output << "mdl_shadow_malformed_control_responses_total";
    common_label();
    output << ' ' << shadow.malformed_control_responses << '\n';
    output << "mdl_shadow_latest_logon_ok";
    common_label();
    output << ' ' << (shadow.latest_logon_ok ? 1 : 0) << '\n';
    output << "mdl_shadow_readiness_generation";
    common_label();
    output << ' ' << shadow.readiness_generation << '\n';
    output << "mdl_shadow_ready";
    common_label();
    output << ' ' << (shadow_readiness_generation() != 0U ? 1 : 0)
           << '\n';

    for (std::size_t index = 0U; index < spec_->required.size(); ++index) {
        const l2flow::sdk::MessageKey& key =
            spec_->required[index];
        const std::uint64_t bit =
            std::uint64_t{1U} << index;
        const auto required_label =
            [this, &output, &key]() {
                output << "{service=\"" << spec_->service_name
                       << "\",source_stream_id=\""
                       << spec_->source_stream_id
                       << "\",service_id=\""
                       << static_cast<unsigned>(key.service_id)
                       << "\",service_version=\""
                       << key.service_version
                       << "\",message_id=\""
                       << key.message_id << "\"}";
            };
        output << "mdl_shadow_required_first_seen";
        required_label();
        output << ' '
               << ((shadow.required_first_seen_mask & bit) != 0U ? 1 : 0)
               << '\n';
        output << "mdl_shadow_required_subscription_ok";
        required_label();
        output << ' '
               << ((shadow.required_subscription_ok_mask & bit) != 0U
                       ? 1
                       : 0)
               << '\n';
        output << "mdl_shadow_required_subscription_failed";
        required_label();
        output << ' '
               << ((shadow.required_subscription_failed_mask & bit) != 0U
                       ? 1
                       : 0)
               << '\n';
        output << "mdl_shadow_required_subscription_failure_observed";
        required_label();
        output << ' '
               << ((shadow.required_subscription_failure_observed_mask &
                    bit) != 0U
                       ? 1
                       : 0)
               << '\n';
    }
    return output.str();
}

void IngressApp::AddSubscriptions() {
    for (const l2flow::sdk::MessageKey& key : spec_->required) {
        subscriber_->AddSubscription(key);
    }
    if (config_.include_optional_index) {
        for (const l2flow::sdk::MessageKey& key :
             spec_->optional) {
            subscriber_->AddSubscription(key);
        }
    }
}

void IngressApp::StartShadowWriter() {
    if (shadow_writer_ != nullptr ||
        shadow_thread_.joinable()) {
        throw std::logic_error(
            "shadow writer was already started");
    }

    const ShadowCaptureConfig shadow_config =
        MakeShadowConfig(*spec_);
    if (pending_shadow_output_ != nullptr) {
        shadow_writer_ =
            std::make_unique<ShadowCaptureWriter>(
                shadow_config,
                ring_,
                capture_fatal_,
                std::move(pending_shadow_output_));
    } else {
        shadow_writer_ =
            std::make_unique<ShadowCaptureWriter>(
                shadow_config,
                ring_,
                capture_fatal_,
                config_.shadow_capture_path);
    }
    if (capture_fatal_.tripped()) {
        return;
    }
    published_shadow_writer_.store(
        shadow_writer_.get(), std::memory_order_release);

    shadow_thread_ = std::thread(
        [this]() noexcept {
            const bool result = shadow_writer_->Run();
            shadow_result_.store(
                result, std::memory_order_relaxed);
            shadow_result_known_.store(
                true, std::memory_order_release);
        });
    while (!shadow_writer_->startup_complete()) {
        std::this_thread::yield();
    }
    if (!shadow_writer_->startup_succeeded()) {
        throw std::runtime_error(
            "shadow writer could not write its file header");
    }

    // The complete file header is visible before Connect, so even a
    // synchronous Connect callback always has a live consumer and sink.
    Observe(IngressLifecycleEvent::ShadowWriterStarted);
}

bool IngressApp::StopLocked() noexcept {
    const IngressAppState current =
        state_.load(std::memory_order_acquire);
    if (current == IngressAppState::Stopped) {
        return !fatal();
    }
    state_.store(
        IngressAppState::Stopping,
        std::memory_order_release);

    handler_.BeginStopping();
    Observe(IngressLifecycleEvent::HandlerBeginStopping);

    if (manager_ != nullptr) {
        try {
            manager_->Shutdown();
        } catch (const std::exception& exception) {
            SetFailureException(
                "SdkManager::Shutdown", exception);
            // A throwing opaque SDK cannot prove that it stopped spawning
            // callbacks. Returning would eventually destroy the raw callback
            // target and permit a late SDK thread to use freed memory.
            std::terminate();
        } catch (...) {
            SetFailureLiteral(
                "SdkManager::Shutdown threw an unknown exception");
            std::terminate();
        }
    }

    const bool quiesced = handler_.Quiesce(
        options_.callback_quiesce_timeout);
    if (!quiesced) {
        SetFailureLiteral(
            "callback handler did not quiesce within the configured timeout");
        // Neither returning nor waiting forever is an acceptable service
        // state. Process fail-stop lets the OS reclaim SDK threads without
        // ever unwinding the callback target underneath them.
        std::terminate();
    }
    Observe(IngressLifecycleEvent::HandlerQuiesced);

    if (shadow_writer_ != nullptr) {
        Observe(
            IngressLifecycleEvent::ShadowWriterStopRequested);
        shadow_writer_->StopAndDrain();

        if (shadow_thread_.joinable()) {
            try {
                shadow_thread_.join();
            } catch (const std::exception& exception) {
                SetFailureException(
                    "shadow writer join", exception);
            } catch (...) {
                SetFailureLiteral(
                    "shadow writer join threw an unknown exception");
            }
        } else if (!shadow_result_known_.load(
                       std::memory_order_acquire)) {
            // A thread-construction exception still gets a synchronous,
            // ordered drain and output finalization.
            const bool result = shadow_writer_->Run();
            shadow_result_.store(
                result, std::memory_order_relaxed);
            shadow_result_known_.store(
                true, std::memory_order_release);
        }
        Observe(IngressLifecycleEvent::ShadowWriterJoined);

        if (!shadow_result_known_.load(
                std::memory_order_acquire) ||
            !shadow_result_.load(std::memory_order_relaxed)) {
            SetFailureLiteral(
                "shadow writer did not stop and drain cleanly");
        }
    }

    if (subscriber_ != nullptr) {
        std::string release_error;
        if (!subscriber_->Release(&release_error)) {
            if (release_error.empty()) {
                SetFailureLiteral("Subscriber release failed");
            } else {
                SetFailure(std::move(release_error));
            }
            // The lifecycle contract requires the handler to outlive the
            // Subscriber. A failed opaque ReleaseRef means that lifetime
            // cannot be proved, even though Shutdown returned.
            std::terminate();
        }
        subscriber_.reset();
    }
    if (manager_ != nullptr) {
        std::string release_error;
        if (!manager_->Release(&release_error)) {
            if (release_error.empty()) {
                SetFailureLiteral("IOManager release failed");
            } else {
                SetFailure(std::move(release_error));
            }
            // As above, never unwind callback-owned storage while a vendor
            // manager may remain alive.
            std::terminate();
        }
        manager_.reset();
    }

    state_.store(
        IngressAppState::Stopped,
        std::memory_order_release);
    return !fatal();
}

void IngressApp::SetFailure(std::string message) noexcept {
    lifecycle_fatal_.store(true, std::memory_order_release);
    if (!last_error_.empty()) {
        return;
    }
    try {
        last_error_ = std::move(message);
    } catch (...) {
    }
}

void IngressApp::SetFailureLiteral(
    const char* message) noexcept {
    try {
        SetFailure(std::string(message));
    } catch (...) {
        lifecycle_fatal_.store(true, std::memory_order_release);
    }
}

void IngressApp::SetFailureException(
    std::string_view stage,
    const std::exception& exception) noexcept {
    try {
        SetFailure(
            std::string(stage) + " threw: " + exception.what());
    } catch (...) {
        SetFailureLiteral("ingress lifecycle stage threw");
    }
}

void IngressApp::SetFailureStage(
    std::string_view stage,
    const char* suffix) noexcept {
    try {
        SetFailure(std::string(stage) + suffix);
    } catch (...) {
        SetFailureLiteral("ingress lifecycle stage failed");
    }
}

void IngressApp::CopyError(std::string* error) const noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        *error = last_error_;
    } catch (...) {
    }
}

void IngressApp::Observe(
    IngressLifecycleEvent event) noexcept {
    if (observer_ != nullptr) {
        observer_->Observe(event);
    }
}

}  // namespace l2flow::ingress
