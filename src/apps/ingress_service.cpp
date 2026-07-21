#include "l2flow/apps/ingress_service.h"

#include "l2flow/apps/monitor_policy.h"
#include "l2flow/apps/service_path_policy.h"
#include "l2flow/ingress/capture_clock.h"
#include "l2flow/ingress/ingress_app.h"
#include "l2flow/ops/credential.h"
#include "l2flow/ops/metrics_worker.h"
#include "l2flow/ops/stable_output_prefix.h"
#include "l2flow/ops/systemd_notify.h"
#include "l2flow/sdk/endpoint_contract.h"
#include "l2flow/sdk/sdk_runtime.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <pthread.h>
#include <time.h>
#include <unistd.h>

namespace l2flow::apps {
namespace {

using Clock = std::chrono::steady_clock;

template <typename Duration>
Clock::time_point DeadlineAfter(
    Clock::time_point now,
    Duration duration) noexcept {
    if (duration <= Duration::zero()) {
        return now;
    }
    const Clock::duration remaining =
        Clock::time_point::max() - now;
    if (duration >=
        std::chrono::duration_cast<Duration>(remaining)) {
        return Clock::time_point::max();
    }
    return now +
           std::chrono::duration_cast<Clock::duration>(duration);
}

constexpr std::array<std::string_view, 14U> kKnownOptions{{
    "--baseline",
    "--archive",
    "--library",
    "--endpoint-contract",
    "--endpoint-contract-sha256",
    "--credential-name",
    "--credential-path",
    "--capture-date",
    "--shadow-path",
    "--sdk-log-prefix",
    "--metrics-path",
    "--ring-bytes",
    "--max-message-bytes",
    "--include-optional-index",
}};

constexpr std::array<std::string_view, 10U> kRequiredOptions{{
    "--baseline",
    "--archive",
    "--library",
    "--endpoint-contract",
    "--endpoint-contract-sha256",
    "--credential-name",
    "--capture-date",
    "--shadow-path",
    "--sdk-log-prefix",
    "--metrics-path",
}};

bool HasNul(std::string_view value) noexcept {
    return value.find('\0') != std::string_view::npos;
}

bool IsLowercaseSha256(std::string_view value) noexcept {
    if (value.size() != 64U) {
        return false;
    }
    return std::all_of(
        value.begin(),
        value.end(),
        [](char character) {
            return (character >= '0' && character <= '9') ||
                   (character >= 'a' && character <= 'f');
        });
}

bool IsKnownOption(std::string_view option) noexcept {
    return std::find(
               kKnownOptions.begin(), kKnownOptions.end(), option) !=
           kKnownOptions.end();
}

template <typename Integer>
bool ParseUnsignedDecimal(
    std::string_view text,
    Integer* result) noexcept {
    static_assert(std::is_integral_v<Integer>);
    static_assert(std::is_unsigned_v<Integer>);
    if (result == nullptr || text.empty() || HasNul(text)) {
        return false;
    }
    Integer parsed = 0;
    const char* const first = text.data();
    const char* const last = text.data() + text.size();
    const std::from_chars_result conversion =
        std::from_chars(first, last, parsed, 10);
    if (conversion.ec != std::errc{} ||
        conversion.ptr != last) {
        return false;
    }
    *result = parsed;
    return true;
}

IngressServiceParseResult ParseFailure(
    IngressServiceParseResult result,
    std::string error) {
    result.success = false;
    result.show_help = false;
    result.error = std::move(error);
    return result;
}

std::optional<std::string_view> EnvironmentValue(
    const char* name) noexcept {
    const char* const value = std::getenv(name);
    if (value == nullptr) {
        return std::nullopt;
    }
    return std::string_view(value);
}

std::string SafeDiagnostic(
    std::string message,
    std::string_view secret) {
    if (!secret.empty()) {
        constexpr std::string_view replacement = "<redacted>";
        std::size_t position = 0U;
        while ((position = message.find(secret, position)) !=
               std::string::npos) {
            message.replace(
                position, secret.size(), replacement);
            position += replacement.size();
        }
    }
    constexpr std::size_t maximum_bytes = 4096U;
    if (message.size() > maximum_bytes) {
        message.resize(maximum_bytes);
        message.append("...");
    }
    for (char& character : message) {
        const unsigned char value =
            static_cast<unsigned char>(character);
        if (value < 0x20U || value == 0x7fU) {
            character = ' ';
        }
    }
    return message;
}

void LogFailure(
    std::string_view stage,
    std::string detail,
    std::string_view secret = {}) {
    std::cerr << stage;
    if (!detail.empty()) {
        std::cerr << ": "
                  << SafeDiagnostic(
                         std::move(detail), secret);
    }
    std::cerr << '\n';
}

bool SendNotification(
    std::string_view state,
    std::string* error) {
    const l2flow::ops::NotifyResult result =
        l2flow::ops::NotifySystemd(state);
    if (result.status != l2flow::ops::NotifyStatus::Error) {
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }
    if (error != nullptr) {
        *error = result.error.empty()
                     ? "systemd notification failed"
                     : result.error;
    }
    return false;
}

l2flow::ops::CredentialResult ReadServiceCredential(
    const std::string& path,
    bool systemd_source) {
    if (!systemd_source) {
        return l2flow::ops::ReadCredentialFile(path);
    }

    l2flow::ops::CredentialPolicy effective_user_policy;
    const uid_t effective_owner = ::geteuid();
    effective_user_policy.expected_owner =
        effective_owner;
    l2flow::ops::CredentialResult result =
        l2flow::ops::ReadCredentialFile(
            path, effective_user_policy);
    if (result.ok() || effective_owner == 0 ||
        result.error !=
            "credential owner does not match policy") {
        return result;
    }

    // systemd may expose a service credential either as a service-UID-owned
    // 0400 file or as a root-owned 0400 file made readable through the
    // protected credential mount/ACL. The explicit fallback path below never
    // gets this exception and remains root-owned only.
    return l2flow::ops::ReadCredentialFile(path);
}

enum class SignalWaitResult {
    Timeout,
    StopRequested,
    Error,
};

class BlockedStopSignals final {
public:
    BlockedStopSignals() {
        if (::sigemptyset(&set_) != 0 ||
            ::sigaddset(&set_, SIGINT) != 0 ||
            ::sigaddset(&set_, SIGTERM) != 0) {
            error_ = "cannot construct the stop-signal set";
            return;
        }
        const int block_error =
            ::pthread_sigmask(SIG_BLOCK, &set_, &previous_);
        if (block_error != 0) {
            error_ =
                std::string("pthread_sigmask failed: ") +
                std::strerror(block_error);
            return;
        }
        active_ = true;
    }

    ~BlockedStopSignals() {
        if (active_) {
            static_cast<void>(
                ::pthread_sigmask(
                    SIG_SETMASK, &previous_, nullptr));
        }
    }

    BlockedStopSignals(const BlockedStopSignals&) = delete;
    BlockedStopSignals& operator=(
        const BlockedStopSignals&) = delete;

    [[nodiscard]] bool ok() const noexcept {
        return active_;
    }

    [[nodiscard]] const std::string& error() const noexcept {
        return error_;
    }

    SignalWaitResult WaitFor(
        Clock::duration duration,
        std::string* error) const {
        if (!active_) {
            if (error != nullptr) {
                *error = error_;
            }
            return SignalWaitResult::Error;
        }

        if (duration < Clock::duration::zero()) {
            duration = Clock::duration::zero();
        }
        const auto nanoseconds =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                duration);
        const auto seconds =
            std::chrono::duration_cast<std::chrono::seconds>(
                nanoseconds);
        const auto remainder =
            nanoseconds -
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                seconds);
        struct timespec timeout {};
        timeout.tv_sec =
            static_cast<time_t>(seconds.count());
        timeout.tv_nsec =
            static_cast<long>(remainder.count());

        const int signal_number =
            ::sigtimedwait(&set_, nullptr, &timeout);
        if (signal_number == SIGINT ||
            signal_number == SIGTERM) {
            if (error != nullptr) {
                error->clear();
            }
            return SignalWaitResult::StopRequested;
        }
        if (signal_number < 0 &&
            (errno == EAGAIN || errno == EINTR)) {
            if (error != nullptr) {
                error->clear();
            }
            return SignalWaitResult::Timeout;
        }
        if (error != nullptr) {
            *error =
                signal_number < 0
                    ? std::string("sigtimedwait failed: ") +
                          std::strerror(errno)
                    : "sigtimedwait returned an unexpected signal";
        }
        return SignalWaitResult::Error;
    }

    SignalWaitResult Poll(std::string* error) const {
        return WaitFor(Clock::duration::zero(), error);
    }

private:
    sigset_t set_{};
    sigset_t previous_{};
    bool active_ = false;
    std::string error_;
};

struct MonitorResult final {
    bool clean = false;
    std::string error;
};

bool SubmitMetricsSnapshot(
    const l2flow::ingress::IngressApp& app,
    l2flow::ops::MetricsWorker& metrics_worker,
    std::string* error) noexcept {
    try {
        if (!metrics_worker.Submit(
                app.prometheus_metrics())) {
            if (error != nullptr) {
                *error =
                    "metrics snapshot was not accepted by the worker";
            }
            return false;
        }
        if (error != nullptr) {
            error->clear();
        }
        return true;
    } catch (...) {
        if (error != nullptr) {
            try {
                *error =
                    "metrics snapshot rendering raised an exception";
            } catch (...) {
            }
        }
        return false;
    }
}

MonitorResult MonitorIngress(
    l2flow::ingress::IngressApp& app,
    const WatchdogSettings& watchdog,
    const BlockedStopSignals& signals,
    l2flow::ops::MetricsWorker& metrics_worker) {
    MonitorResult result;
    MonitorPolicy policy;
    Clock::time_point next_status = Clock::now();
    Clock::time_point next_metrics = Clock::now();
    Clock::time_point next_metrics_failure_log =
        Clock::time_point::min();
    Clock::time_point next_watchdog =
        Clock::time_point::max();

    for (;;) {
        // A stop already pending after a blocking SDK startup must win over
        // READY, STATUS and watchdog emission.
        const SignalWaitResult pending_signal =
            signals.Poll(&result.error);
        if (pending_signal == SignalWaitResult::Error) {
            return result;
        }

        MonitorPolicyInput policy_input;
        policy_input.stop_requested =
            pending_signal ==
            SignalWaitResult::StopRequested;
        policy_input.fatal = app.fatal();
        if (policy_input.stop_requested ||
            policy_input.fatal) {
            const MonitorPolicyOutput terminal =
                policy.Step(policy_input);
            if (terminal.terminal ==
                MonitorTerminal::CleanStop) {
                result.clean = true;
                return result;
            }
            result.error = app.last_error();
            if (result.error.empty()) {
                result.error =
                    "ingress fatal latch was tripped";
            }
            return result;
        }

        const Clock::time_point now = Clock::now();
        if (now >= next_metrics) {
            std::string metrics_error;
            static_cast<void>(
                SubmitMetricsSnapshot(
                    app,
                    metrics_worker,
                    &metrics_error));
            std::string publication_error =
                metrics_worker.TakeLastError();
            if (!publication_error.empty()) {
                if (metrics_error.empty()) {
                    metrics_error =
                        std::move(publication_error);
                } else {
                    metrics_error.append(
                        "; asynchronous publication also failed");
                }
            }
            if (!metrics_error.empty() &&
                now >= next_metrics_failure_log) {
                LogFailure(
                    "periodic metrics publication failed",
                    std::move(metrics_error));
                next_metrics_failure_log =
                    DeadlineAfter(
                        now, std::chrono::minutes(1));
            }
            next_metrics =
                DeadlineAfter(
                    now, std::chrono::seconds(1));
        }

        // A logon can be replaced while the monitor samples. Require one
        // stable generation on both sides of the cumulative evidence
        // snapshot so old connection facts cannot be paired with a new
        // generation. Control transitions are rare, so retrying the loop is
        // bounded in every stable state.
        const std::uint64_t generation_before =
            app.shadow_readiness_generation();
        const l2flow::ingress::ShadowCaptureStats shadow =
            app.shadow_stats();
        const std::uint64_t current_generation =
            app.shadow_readiness_generation();
        if (generation_before != current_generation ||
            shadow.readiness_generation != current_generation) {
            continue;
        }
        policy_input.fatal = app.fatal();
        policy_input.current_readiness_generation =
            current_generation;
        policy_input.failed_logon_responses =
            shadow.logon_failed_responses;
        policy_input
            .required_subscription_failure_observed_mask =
                shadow
                    .required_subscription_failure_observed_mask;
        policy_input.status_due = now >= next_status;
        policy_input.watchdog_due =
            watchdog.enabled && now >= next_watchdog;
        const MonitorPolicyOutput action =
            policy.Step(policy_input);

        if (action.terminal ==
            MonitorTerminal::Fatal) {
            result.error = app.last_error();
            if (result.error.empty()) {
                result.error =
                    "ingress fatal latch was tripped";
            }
            return result;
        }
        if (action.terminal ==
            MonitorTerminal::LostAfterReady) {
            std::string notify_error;
            if (!SendNotification(
                    "STATUS=readiness-lost",
                    &notify_error)) {
                result.error =
                    "ingress readiness was lost after READY; "
                    "the degraded STATUS notification also failed";
            } else {
                result.error =
                    "ingress readiness was lost after READY";
            }
            return result;
        }
        if (action.terminal ==
            MonitorTerminal::FailedBeforeReady) {
            std::string notify_error;
            if (!SendNotification(
                    "STATUS=logon-or-required-subscription-failed",
                    &notify_error)) {
                result.error =
                    "SDK reported a failed logon or required subscription; "
                    "the failure STATUS notification also failed";
            } else {
                result.error =
                    "SDK reported a failed logon or required subscription";
            }
            return result;
        }
        if (action.terminal ==
            MonitorTerminal::CleanStop) {
            result.clean = true;
            return result;
        }

        if (app.fatal()) {
            continue;
        }
        const auto readiness_observation_is_current =
            [&app, current_generation]() noexcept {
                return !app.fatal() &&
                       app.shadow_readiness_generation() ==
                           current_generation;
            };
        if ((action.send_ready ||
             action.send_ready_status) &&
            !readiness_observation_is_current()) {
            continue;
        }

        if (action.send_ready) {
            if (!SendNotification(
                    "READY=1\nSTATUS=ready",
                    &result.error)) {
                return result;
            }
            policy.ConfirmReadySent(
                current_generation);
            next_status =
                DeadlineAfter(
                    now, std::chrono::seconds(1));
            if (watchdog.enabled) {
                next_watchdog =
                    DeadlineAfter(
                        now, watchdog.notify_interval);
            }
        } else if (action.send_waiting_status ||
                   action.send_ready_status) {
            if (!SendNotification(
                    action.send_ready_status
                        ? std::string_view("STATUS=ready")
                        : std::string_view(
                              "STATUS=waiting-for-ready"),
                    &result.error)) {
                return result;
            }
            next_status =
                DeadlineAfter(
                    now, std::chrono::seconds(1));
        }

        // Recheck after notifications: a capture failure can race this
        // monitor. Looping returns through the fatal branch before another
        // watchdog pulse.
        if (app.fatal()) {
            continue;
        }
        // Every readiness terminal branch above returns before this section,
        // so an observed degraded service cannot emit another watchdog pulse.
        if (action.send_watchdog) {
            if (!readiness_observation_is_current()) {
                continue;
            }
            if (!SendNotification(
                    "WATCHDOG=1", &result.error)) {
                return result;
            }
            next_watchdog =
                DeadlineAfter(
                    now, watchdog.notify_interval);
        }

        const Clock::time_point deadline =
            std::min(
                std::min(next_status, next_watchdog),
                next_metrics);
        const SignalWaitResult wait_result =
            signals.WaitFor(
                deadline - Clock::now(), &result.error);
        if (wait_result == SignalWaitResult::StopRequested) {
            result.clean = true;
            return result;
        }
        if (wait_result == SignalWaitResult::Error) {
            return result;
        }
    }
}

bool StopAndReconcile(
    l2flow::ingress::IngressApp& app,
    std::string_view secret,
    bool service_clean,
    l2flow::ops::MetricsWorker& metrics_worker) {
    bool clean = service_clean;
    std::string notify_error;
    if (!SendNotification(
            "STOPPING=1\nSTATUS=stopping",
            &notify_error)) {
        LogFailure(
            "STOPPING notification failed",
            std::move(notify_error),
            secret);
        clean = false;
    }

    std::string stop_error;
    if (!app.Stop(&stop_error)) {
        LogFailure(
            "ingress shutdown failed",
            std::move(stop_error),
            secret);
        clean = false;
    }

    const l2flow::ingress::ShadowCaptureReconciliation
        reconciliation = app.reconciliation();
    if (!reconciliation.exact()) {
        LogFailure(
            "shadow capture reconciliation failed",
            "callback and sink counts or byte totals differ");
        clean = false;
    }

    std::string metrics_error;
    if (!SubmitMetricsSnapshot(
            app,
            metrics_worker,
            &metrics_error)) {
        LogFailure(
            "final metrics snapshot submission failed",
            std::move(metrics_error));
    }
    metrics_worker.StopAndJoin();
    metrics_error = metrics_worker.TakeLastError();
    if (!metrics_error.empty()) {
        LogFailure(
            "metrics publication failed during shutdown drain",
            std::move(metrics_error));
    }

    const std::string_view final_status =
        clean
            ? std::string_view("STATUS=stopped")
            : std::string_view("STATUS=stopped-with-error");
    if (!SendNotification(final_status, &notify_error)) {
        LogFailure(
            "final STATUS notification failed",
            std::move(notify_error),
            secret);
        clean = false;
    }
    return clean;
}

bool StartupStopRequested(
    const BlockedStopSignals& signals,
    bool* requested,
    std::string* error) {
    const SignalWaitResult result = signals.Poll(error);
    if (result == SignalWaitResult::Error) {
        return false;
    }
    *requested =
        result == SignalWaitResult::StopRequested;
    return true;
}

}  // namespace

IngressServiceConfig DefaultIngressServiceConfig(
    l2flow::sdk::IngressKind kind) {
    IngressServiceConfig result;
    result.ingress =
        l2flow::sdk::DefaultIngressConfig(kind);
    return result;
}

IngressServiceParseResult ParseIngressServiceArguments(
    l2flow::sdk::IngressKind kind,
    std::span<const std::string_view> arguments) {
    IngressServiceParseResult result;
    try {
        result.config =
            DefaultIngressServiceConfig(kind);
    } catch (...) {
        result.error = "unknown ingress kind";
        return result;
    }

    for (const std::string_view argument : arguments) {
        if (HasNul(argument)) {
            return ParseFailure(
                std::move(result),
                "command-line arguments must not contain NUL bytes");
        }
    }

    const auto help = std::find(
        arguments.begin(), arguments.end(), "--help");
    if (help != arguments.end()) {
        if (arguments.size() == 1U) {
            result.show_help = true;
            return result;
        }
        return ParseFailure(
            std::move(result),
            "--help must be used without other arguments");
    }

    std::set<std::string_view> seen;
    for (std::size_t index = 0U;
         index < arguments.size();
         ++index) {
        const std::string_view option = arguments[index];
        if (!IsKnownOption(option)) {
            return ParseFailure(
                std::move(result),
                "unknown command-line option or positional argument");
        }
        if (!seen.insert(option).second) {
            return ParseFailure(
                std::move(result),
                "duplicate option: " + std::string(option));
        }
        if (index + 1U >= arguments.size() ||
            arguments[index + 1U].starts_with("--")) {
            return ParseFailure(
                std::move(result),
                "missing value for " + std::string(option));
        }
        const std::string_view value =
            arguments[++index];
        if (value.empty()) {
            return ParseFailure(
                std::move(result),
                "empty value for " + std::string(option));
        }

        if (option == "--baseline") {
            result.config.preflight_paths.baseline_json =
                std::filesystem::path(std::string(value));
        } else if (option == "--archive") {
            result.config.preflight_paths.sdk_archive =
                std::filesystem::path(std::string(value));
        } else if (option == "--library") {
            result.config.preflight_paths.shared_library =
                std::filesystem::path(std::string(value));
        } else if (option == "--endpoint-contract") {
            result.config.endpoint_contract_path = value;
        } else if (
            option == "--endpoint-contract-sha256") {
            if (!IsLowercaseSha256(value)) {
                return ParseFailure(
                    std::move(result),
                    "invalid value for --endpoint-contract-sha256");
            }
            result.config.endpoint_contract_sha256 = value;
        } else if (option == "--credential-name") {
            result.config.ingress.credential_name = value;
        } else if (option == "--credential-path") {
            result.config.credential_path =
                std::string(value);
        } else if (option == "--capture-date") {
            if (value.size() != 8U ||
                !ParseUnsignedDecimal(
                    value,
                    &result.config.ingress.capture_date)) {
                return ParseFailure(
                    std::move(result),
                    "invalid value for --capture-date");
            }
        } else if (option == "--shadow-path") {
            result.config.ingress.shadow_capture_path =
                value;
        } else if (option == "--sdk-log-prefix") {
            result.config.ingress.sdk_log_prefix = value;
        } else if (option == "--metrics-path") {
            result.config.ingress.metrics_textfile_path =
                value;
        } else if (option == "--ring-bytes") {
            if (!ParseUnsignedDecimal(
                    value,
                    &result.config.ingress
                         .ring_capacity_bytes)) {
                return ParseFailure(
                    std::move(result),
                    "invalid value for --ring-bytes");
            }
        } else if (
            option == "--max-message-bytes") {
            if (!ParseUnsignedDecimal(
                    value,
                    &result.config.ingress
                         .max_message_bytes)) {
                return ParseFailure(
                    std::move(result),
                    "invalid value for --max-message-bytes");
            }
        } else if (
            option == "--include-optional-index") {
            if (value != "true" && value != "false") {
                return ParseFailure(
                    std::move(result),
                    "invalid value for --include-optional-index");
            }
            result.config.ingress.include_optional_index =
                value == "true";
        }
    }

    for (const std::string_view required :
         kRequiredOptions) {
        if (seen.count(required) == 0U) {
            return ParseFailure(
                std::move(result),
                "missing required option: " +
                    std::string(required));
        }
    }

    if (result.config.ingress.ring_capacity_bytes <
        l2flow::sdk::kDefaultMinimumRingBytes) {
        return ParseFailure(
            std::move(result),
            "--ring-bytes is below the production minimum");
    }
    if (!std::filesystem::path(
            result.config.endpoint_contract_path).is_absolute()) {
        return ParseFailure(
            std::move(result),
            "--endpoint-contract must be an absolute path");
    }
    if (!std::filesystem::path(
            result.config.ingress.shadow_capture_path).is_absolute() ||
        !std::filesystem::path(
            result.config.ingress.sdk_log_prefix).is_absolute() ||
        !std::filesystem::path(
            result.config.ingress.metrics_textfile_path).is_absolute()) {
        return ParseFailure(
            std::move(result),
            "shadow, SDK log, and metrics paths must be absolute");
    }
    const std::string path_policy_error =
        ValidateIngressServicePathPolicy(
            result.config,
            std::nullopt,
            false);
    if (!path_policy_error.empty()) {
        return ParseFailure(
            std::move(result),
            path_policy_error);
    }

    result.success = true;
    return result;
}

std::string IngressServiceUsage(
    l2flow::sdk::IngressKind kind) {
    const l2flow::sdk::IngressSpec& spec =
        l2flow::sdk::GetIngressSpec(kind);
    std::ostringstream output;
    output
        << "Usage: " << spec.service_name
        << " --baseline <path> --archive <path>"
           " --library <path>\n"
        << "  --endpoint-contract <absolute-path>"
           " --endpoint-contract-sha256 <64-lowercase-hex>\n"
        << "  --credential-name <name>"
           " [--credential-path <root-owned-0400-path>]\n"
        << "  --capture-date <YYYYMMDD>"
           " --shadow-path <absolute-path>"
           " --sdk-log-prefix <absolute-path>"
           " --metrics-path <absolute-path>\n"
        << "  [--ring-bytes <uint64>]"
           " [--max-message-bytes <uint32>]\n"
        << "  [--include-optional-index <true|false>]\n"
        << "Defaults: systemd credential lookup,"
           " include-optional-index=false,\n"
        << "          ring-bytes="
        << l2flow::sdk::kDefaultMinimumRingBytes
        << ", max-message-bytes="
        << l2flow::sdk::kDefaultMaxMessageBytes
        << ", work-threads=" << spec.default_work_threads
        << ", io-threads=" << spec.default_io_threads
        << ".\n";
    return output.str();
}

bool ParseWatchdogEnvironment(
    std::optional<std::string_view> notify_socket,
    std::optional<std::string_view> watchdog_usec,
    WatchdogSettings* settings,
    std::string* error) {
    if (error != nullptr) {
        error->clear();
    }
    if (settings == nullptr) {
        if (error != nullptr) {
            *error = "watchdog settings output is null";
        }
        return false;
    }
    *settings = {};

    if (!notify_socket.has_value() ||
        notify_socket->empty()) {
        return true;
    }
    if (HasNul(*notify_socket)) {
        if (error != nullptr) {
            *error = "NOTIFY_SOCKET contains a NUL byte";
        }
        return false;
    }
    if (!watchdog_usec.has_value()) {
        return true;
    }
    if (watchdog_usec->empty() ||
        HasNul(*watchdog_usec)) {
        if (error != nullptr) {
            *error =
                "WATCHDOG_USEC must be a non-empty decimal integer";
        }
        return false;
    }

    std::uint64_t microseconds = 0U;
    if (!ParseUnsignedDecimal(
            *watchdog_usec, &microseconds)) {
        if (error != nullptr) {
            *error =
                "WATCHDOG_USEC must be a decimal uint64 integer";
        }
        return false;
    }
    using Rep = std::chrono::microseconds::rep;
    static_assert(std::is_signed_v<Rep>);
    const auto maximum_rep =
        static_cast<std::uint64_t>(
            std::numeric_limits<Rep>::max());
    // The design requires a period strictly smaller than WatchdogSec/2.
    // Subtracting before integer division preserves that inequality for both
    // odd and even WATCHDOG_USEC values.
    const std::uint64_t notify_interval =
        microseconds == 0U
            ? 0U
            : (microseconds - 1U) / 2U;
    const auto remaining_clock_microseconds =
        std::chrono::duration_cast<
            std::chrono::microseconds>(
            Clock::time_point::max() - Clock::now())
            .count();
    if (microseconds > maximum_rep ||
        notify_interval == 0U ||
        remaining_clock_microseconds <= 0 ||
        notify_interval >
            static_cast<std::uint64_t>(
                remaining_clock_microseconds)) {
        if (error != nullptr) {
            *error =
                "WATCHDOG_USEC is outside the supported positive range";
        }
        return false;
    }

    settings->enabled = true;
    settings->notify_interval =
        std::chrono::microseconds(
            static_cast<Rep>(notify_interval));
    return true;
}

ReadinessDecision EvaluateReadiness(
    bool ready_sent,
    std::uint64_t announced_readiness_generation,
    std::uint64_t current_readiness_generation,
    std::uint64_t failed_logon_responses,
    std::uint64_t
        required_subscription_failure_observed_mask) noexcept {
    const bool current_ready =
        current_readiness_generation != 0U;
    const bool explicit_failure =
        failed_logon_responses != 0U ||
        required_subscription_failure_observed_mask != 0U;
    const bool replaced_after_ready =
        ready_sent &&
        current_readiness_generation !=
            announced_readiness_generation;
    if (ready_sent &&
        (!current_ready || replaced_after_ready ||
         explicit_failure)) {
        return ReadinessDecision::LostAfterReady;
    }
    if (explicit_failure) {
        return ReadinessDecision::FailedBeforeReady;
    }
    if (current_ready) {
        return ready_sent
                   ? ReadinessDecision::RemainsReady
                   : ReadinessDecision::BecameReady;
    }
    return ReadinessDecision::Waiting;
}

int RunIngressService(
    l2flow::sdk::IngressKind kind,
    int argc,
    const char* const argv[]) noexcept {
    std::string secret_for_redaction;
    std::unique_ptr<BlockedStopSignals> blocked_signals;
    std::unique_ptr<l2flow::ops::MetricsWorker>
        metrics_worker;
    std::unique_ptr<l2flow::ops::StableOutputPrefix>
        sdk_log_prefix;
    std::unique_ptr<l2flow::ingress::IngressApp> app;
    bool cleanup_completed = false;
    try {
        if (argc < 0 ||
            (argc > 0 &&
             (argv == nullptr || argv[0] == nullptr))) {
            LogFailure(
                "invalid process arguments",
                "argc/argv are inconsistent");
            return 2;
        }

        std::vector<std::string_view> arguments;
        if (argc > 1) {
            arguments.reserve(
                static_cast<std::size_t>(argc - 1));
        }
        for (int index = 1; index < argc; ++index) {
            if (argv[index] == nullptr) {
                LogFailure(
                    "invalid process arguments",
                    "argv contains a null entry");
                return 2;
            }
            arguments.emplace_back(argv[index]);
        }

        IngressServiceParseResult parsed =
            ParseIngressServiceArguments(kind, arguments);
        if (parsed.show_help) {
            std::cout << IngressServiceUsage(kind);
            return 0;
        }
        if (!parsed.success) {
            LogFailure(
                "invalid command line",
                std::move(parsed.error));
            std::cerr << IngressServiceUsage(kind);
            return 2;
        }

        WatchdogSettings watchdog;
        std::string error;
        if (!ParseWatchdogEnvironment(
                EnvironmentValue("NOTIFY_SOCKET"),
                EnvironmentValue("WATCHDOG_USEC"),
                &watchdog,
                &error)) {
            LogFailure(
                "invalid watchdog environment",
                std::move(error));
            return 2;
        }

        blocked_signals =
            std::make_unique<BlockedStopSignals>();
        if (!blocked_signals->ok()) {
            LogFailure(
                "cannot block SIGINT/SIGTERM",
                blocked_signals->error());
            return 1;
        }

        bool stop_requested = false;
        if (!StartupStopRequested(
                *blocked_signals,
                &stop_requested,
                &error)) {
            LogFailure(
                "cannot inspect pending stop signals",
                std::move(error));
            return 1;
        }
        if (stop_requested) {
            return 0;
        }

        IngressServiceConfig config =
            std::move(parsed.config);
        l2flow::sdk::EndpointContract endpoint_contract;
        if (!l2flow::sdk::LoadEndpointContractFile(
                config.endpoint_contract_path,
                config.endpoint_contract_sha256,
                kind,
                &endpoint_contract,
                &error)) {
            LogFailure(
                "endpoint contract loading failed",
                std::move(error));
            return 1;
        }
        config.ingress.endpoint =
            std::move(endpoint_contract);
        std::string credential_path;
        const bool systemd_credential =
            !config.credential_path.has_value();
        if (config.credential_path.has_value()) {
            credential_path =
                *config.credential_path;
        } else {
            const std::optional<std::string> resolved =
                l2flow::ops::ResolveSystemdCredentialPath(
                    config.ingress.credential_name,
                    &error);
            if (!resolved.has_value()) {
                LogFailure(
                    "systemd credential resolution failed",
                    std::move(error));
                return 1;
            }
            credential_path = *resolved;
        }
        const std::string path_policy_error =
            ValidateIngressServicePathPolicy(
                config,
                credential_path);
        if (!path_policy_error.empty()) {
            LogFailure(
                "service path policy failed",
                path_policy_error);
            return 1;
        }

        sdk_log_prefix =
            l2flow::ops::OpenStableOutputPrefix(
                config.ingress.sdk_log_prefix,
                &error);
        if (sdk_log_prefix == nullptr) {
            LogFailure(
                "SDK log prefix stabilization failed",
                std::move(error));
            return 1;
        }

        l2flow::ops::CredentialResult credential =
            ReadServiceCredential(
                credential_path,
                systemd_credential);
        if (!credential.ok()) {
            LogFailure(
                "credential loading failed",
                std::move(credential.error));
            return 1;
        }
        secret_for_redaction = credential.token;
        config.ingress.token =
            std::move(credential.token);

        const std::string validation_error =
            l2flow::sdk::ValidateIngressConfig(
                config.ingress);
        if (!validation_error.empty()) {
            LogFailure(
                "ingress configuration validation failed",
                validation_error,
                secret_for_redaction);
            return 1;
        }

        if (!StartupStopRequested(
                *blocked_signals,
                &stop_requested,
                &error)) {
            LogFailure(
                "cannot inspect pending stop signals",
                std::move(error));
            return 1;
        }
        if (stop_requested) {
            return 0;
        }

        const l2flow::baseline::PreflightReport preflight =
            l2flow::baseline::RunVendorPreflight(
                config.preflight_paths);
        if (!preflight.passed()) {
            LogFailure(
                "full vendor preflight failed",
                l2flow::baseline::PreflightReportJson(
                    preflight, true),
                secret_for_redaction);
            return 1;
        }

        if (!StartupStopRequested(
                *blocked_signals,
                &stop_requested,
                &error)) {
            LogFailure(
                "cannot inspect pending stop signals",
                std::move(error));
            return 1;
        }
        if (stop_requested) {
            return 0;
        }

        std::shared_ptr<l2flow::sdk::SdkFactory>
            sdk_factory =
                l2flow::sdk::LoadApprovedSdkFactory(
                    config.preflight_paths.shared_library,
                    &error);
        if (sdk_factory == nullptr) {
            LogFailure(
                "approved SDK loading failed",
                std::move(error),
                secret_for_redaction);
            return 1;
        }

        if (!StartupStopRequested(
                *blocked_signals,
                &stop_requested,
                &error)) {
            LogFailure(
                "cannot inspect pending stop signals",
                std::move(error));
            return 1;
        }
        if (stop_requested) {
            return 0;
        }

        metrics_worker = std::make_unique<
            l2flow::ops::MetricsWorker>(
                config.ingress.metrics_textfile_path);
        l2flow::ingress::IngressAppOptions app_options;
        app_options.sdk_log_runtime_prefix =
            sdk_log_prefix->stable_prefix();
        app = std::make_unique<
            l2flow::ingress::IngressApp>(
                std::move(config.ingress),
                std::move(sdk_factory),
                std::make_unique<
                    l2flow::ingress::LinuxCaptureClock>(),
                nullptr,
                std::move(app_options));

        bool clean = true;
        if (!SendNotification(
                "STATUS=initializing", &error)) {
            LogFailure(
                "initial STATUS notification failed",
                std::move(error),
                secret_for_redaction);
            clean = false;
        }

        std::string initialize_error;
        if (clean &&
            !app->Initialize(&initialize_error)) {
            LogFailure(
                "ingress initialization failed",
                std::move(initialize_error),
                secret_for_redaction);
            clean = false;
        }

        if (clean) {
            const MonitorResult monitor =
                MonitorIngress(
                    *app,
                    watchdog,
                    *blocked_signals,
                    *metrics_worker);
            if (!monitor.clean) {
                LogFailure(
                    "ingress monitor failed",
                    monitor.error,
                    secret_for_redaction);
                clean = false;
            }
        }

        clean = StopAndReconcile(
            *app,
            secret_for_redaction,
            clean,
            *metrics_worker);
        cleanup_completed = true;
        return clean ? 0 : 1;
    } catch (const std::exception& exception) {
        LogFailure(
            "ingress service raised an exception",
            exception.what(),
            secret_for_redaction);
        if (app != nullptr &&
            metrics_worker != nullptr &&
            !cleanup_completed) {
            try {
                static_cast<void>(StopAndReconcile(
                    *app,
                    secret_for_redaction,
                    false,
                    *metrics_worker));
            } catch (...) {
            }
        }
        return 1;
    } catch (...) {
        LogFailure(
            "ingress service raised an unknown exception",
            {},
            secret_for_redaction);
        if (app != nullptr &&
            metrics_worker != nullptr &&
            !cleanup_completed) {
            try {
                static_cast<void>(StopAndReconcile(
                    *app,
                    secret_for_redaction,
                    false,
                    *metrics_worker));
            } catch (...) {
            }
        }
        return 1;
    }
}

}  // namespace l2flow::apps
