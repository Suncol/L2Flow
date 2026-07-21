#pragma once

#include "l2flow/baseline/vendor_baseline.h"
#include "l2flow/sdk/ingress_config.h"

#include <chrono>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace l2flow::apps {

// The non-secret, process-level inputs that surround one IngressConfig.
// credential_path is deliberately optional: an absent value selects
// $CREDENTIALS_DIRECTORY/<credential_name>.
struct IngressServiceConfig final {
    l2flow::baseline::PreflightPaths preflight_paths;
    l2flow::sdk::IngressConfig ingress;
    std::string endpoint_contract_path;
    std::string endpoint_contract_sha256;
    std::optional<std::string> credential_path;
};

struct IngressServiceParseResult final {
    IngressServiceConfig config;
    bool success = false;
    bool show_help = false;
    std::string error;
};

// Returns the SDK thread, ring, message-size and heartbeat defaults for one
// of the four immutable ingress kinds. Paths, endpoint contract identity,
// credential identity and capture date remain unset.
IngressServiceConfig DefaultIngressServiceConfig(
    l2flow::sdk::IngressKind kind);

// Parses option/value pairs (argv excluding argv[0]). Unknown options,
// duplicates, missing values, non-canonical booleans, invalid encodings,
// integer overflow, empty values and embedded NUL bytes are rejected.
IngressServiceParseResult ParseIngressServiceArguments(
    l2flow::sdk::IngressKind kind,
    std::span<const std::string_view> arguments);

// Static usage text: it contains option syntax and defaults, never runtime
// argument values or credential bytes.
std::string IngressServiceUsage(l2flow::sdk::IngressKind kind);

struct WatchdogSettings final {
    bool enabled = false;
    std::chrono::microseconds notify_interval{0};
};

// Pure environment mapping used by the runner. WATCHDOG_USEC is relevant only
// when NOTIFY_SOCKET is present and non-empty. A configured value must be a
// strictly decimal, positive interval whose half is representable and nonzero.
bool ParseWatchdogEnvironment(
    std::optional<std::string_view> notify_socket,
    std::optional<std::string_view> watchdog_usec,
    WatchdogSettings* settings,
    std::string* error);

enum class ReadinessDecision : std::uint8_t {
    Waiting = 0U,
    BecameReady,
    RemainsReady,
    FailedBeforeReady,
    LostAfterReady,
};

// Pure monitor transition used to keep READY/error precedence testable. Zero
// current_readiness_generation means not ready. Once READY was announced, a
// changed generation or any cumulative failed logon/required-subscription
// evidence is terminal even if the current generation has already recovered.
ReadinessDecision EvaluateReadiness(
    bool ready_sent,
    std::uint64_t announced_readiness_generation,
    std::uint64_t current_readiness_generation,
    std::uint64_t failed_logon_responses,
    std::uint64_t
        required_subscription_failure_observed_mask) noexcept;

// Common entry point used by the four macro-selected executables.
int RunIngressService(
    l2flow::sdk::IngressKind kind,
    int argc,
    const char* const argv[]) noexcept;

}  // namespace l2flow::apps
