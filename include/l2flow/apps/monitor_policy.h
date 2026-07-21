#pragma once

#include <cstdint>

namespace l2flow::apps {

// One stable observation made by the service monitor. The counters and mask
// are cumulative: once either failure field is nonzero, this process instance
// cannot become (or remain) ready.
struct MonitorPolicyInput final {
    bool stop_requested = false;
    bool fatal = false;
    std::uint64_t current_readiness_generation = 0U;
    std::uint64_t failed_logon_responses = 0U;
    std::uint64_t
        required_subscription_failure_observed_mask = 0U;
    bool status_due = false;
    bool watchdog_due = false;
};

enum class MonitorTerminal : std::uint8_t {
    None = 0U,
    CleanStop,
    Fatal,
    FailedBeforeReady,
    LostAfterReady,
};

// Notification flags are mutually constrained by MonitorPolicy::Step:
// terminal results carry no notification actions; send_ready remains pending
// until the caller confirms a successful notification; and send_watchdog is
// possible only on a step that began with confirmed READY state.
struct MonitorPolicyOutput final {
    MonitorTerminal terminal = MonitorTerminal::None;
    bool send_ready = false;
    bool send_waiting_status = false;
    bool send_ready_status = false;
    bool send_watchdog = false;
};

// A deterministic, side-effect-free (apart from its explicit state)
// transition policy for the production service monitor. The caller owns all
// clocks, I/O and notification error handling.
class MonitorPolicy final {
public:
    MonitorPolicy() = default;

    MonitorPolicyOutput Step(
        const MonitorPolicyInput& input) noexcept;

    // Commits READY only after the caller has sent READY=1 successfully.
    // A zero generation is ignored.
    void ConfirmReadySent(
        std::uint64_t readiness_generation) noexcept;

    [[nodiscard]] bool ready_sent() const noexcept;

    [[nodiscard]] std::uint64_t
    announced_readiness_generation() const noexcept;

private:
    bool ready_sent_ = false;
    std::uint64_t announced_readiness_generation_ = 0U;
};

}  // namespace l2flow::apps
