#include "l2flow/apps/monitor_policy.h"

#include "l2flow/apps/ingress_service.h"

namespace l2flow::apps {

MonitorPolicyOutput MonitorPolicy::Step(
    const MonitorPolicyInput& input) noexcept {
    MonitorPolicyOutput output;

    // Terminal conditions deliberately precede every notification decision.
    if (input.stop_requested) {
        output.terminal = MonitorTerminal::CleanStop;
        return output;
    }
    if (input.fatal) {
        output.terminal = MonitorTerminal::Fatal;
        return output;
    }

    const ReadinessDecision readiness =
        EvaluateReadiness(
            ready_sent_,
            announced_readiness_generation_,
            input.current_readiness_generation,
            input.failed_logon_responses,
            input.required_subscription_failure_observed_mask);
    if (readiness ==
        ReadinessDecision::LostAfterReady) {
        output.terminal =
            MonitorTerminal::LostAfterReady;
        return output;
    }
    if (readiness ==
        ReadinessDecision::FailedBeforeReady) {
        output.terminal =
            MonitorTerminal::FailedBeforeReady;
        return output;
    }

    if (readiness ==
        ReadinessDecision::BecameReady) {
        output.send_ready = true;

        // READY carries the initial ready status. A watchdog pulse is allowed
        // only after the caller confirms this notification and a later
        // observation begins in the ready state.
        return output;
    }

    if (input.status_due) {
        if (ready_sent_) {
            output.send_ready_status = true;
        } else {
            output.send_waiting_status = true;
        }
    }
    if (ready_sent_ && input.watchdog_due) {
        output.send_watchdog = true;
    }
    return output;
}

void MonitorPolicy::ConfirmReadySent(
    std::uint64_t readiness_generation) noexcept {
    if (ready_sent_ || readiness_generation == 0U) {
        return;
    }
    ready_sent_ = true;
    announced_readiness_generation_ =
        readiness_generation;
}

bool MonitorPolicy::ready_sent() const noexcept {
    return ready_sent_;
}

std::uint64_t
MonitorPolicy::announced_readiness_generation() const noexcept {
    return announced_readiness_generation_;
}

}  // namespace l2flow::apps
