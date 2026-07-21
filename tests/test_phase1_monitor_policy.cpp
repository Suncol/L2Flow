#include "l2flow/apps/monitor_policy.h"

#include <cstdint>
#include <iostream>
#include <string>

namespace apps = l2flow::apps;

namespace {

struct TestContext final {
    void Expect(
        bool condition,
        const std::string& description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    int failures = 0;
};

bool HasNoNotification(
    const apps::MonitorPolicyOutput& output) {
    return !output.send_ready &&
           !output.send_waiting_status &&
           !output.send_ready_status &&
           !output.send_watchdog;
}

void CheckCompleteActionSequence(TestContext* test) {
    apps::MonitorPolicy policy;

    apps::MonitorPolicyInput waiting;
    waiting.status_due = true;
    waiting.watchdog_due = true;
    const apps::MonitorPolicyOutput waiting_output =
        policy.Step(waiting);
    test->Expect(
        waiting_output.terminal ==
                apps::MonitorTerminal::None &&
            !waiting_output.send_ready &&
            waiting_output.send_waiting_status &&
            !waiting_output.send_ready_status &&
            !waiting_output.send_watchdog,
        "a due pre-ready step sends only waiting status");

    apps::MonitorPolicyInput became_ready;
    became_ready.current_readiness_generation = 41U;
    became_ready.status_due = true;
    became_ready.watchdog_due = true;
    const apps::MonitorPolicyOutput ready_output =
        policy.Step(became_ready);
    test->Expect(
        ready_output.terminal ==
                apps::MonitorTerminal::None &&
            ready_output.send_ready &&
            !ready_output.send_waiting_status &&
            !ready_output.send_ready_status &&
            !ready_output.send_watchdog,
        "the first complete generation sends READY alone");
    test->Expect(
        !policy.ready_sent(),
        "READY is not committed before notification confirmation");
    policy.ConfirmReadySent(41U);
    test->Expect(
        policy.ready_sent() &&
            policy.announced_readiness_generation() == 41U,
        "the policy records the generation announced by READY");

    apps::MonitorPolicyInput stable;
    stable.current_readiness_generation = 41U;
    stable.status_due = true;
    stable.watchdog_due = true;
    const apps::MonitorPolicyOutput stable_output =
        policy.Step(stable);
    test->Expect(
        stable_output.terminal ==
                apps::MonitorTerminal::None &&
            !stable_output.send_ready &&
            !stable_output.send_waiting_status &&
            stable_output.send_ready_status &&
            stable_output.send_watchdog,
        "a later stable ready step sends due ready status and watchdog");

    stable.status_due = false;
    stable.watchdog_due = false;
    const apps::MonitorPolicyOutput idle_output =
        policy.Step(stable);
    test->Expect(
        idle_output.terminal ==
                apps::MonitorTerminal::None &&
            HasNoNotification(idle_output),
        "a stable step with no due work has no action");
}

void CheckTerminalPriority(TestContext* test) {
    apps::MonitorPolicy policy;
    apps::MonitorPolicyInput all;
    all.stop_requested = true;
    all.fatal = true;
    all.current_readiness_generation = 9U;
    all.failed_logon_responses = 1U;
    all.required_subscription_failure_observed_mask = 2U;
    all.status_due = true;
    all.watchdog_due = true;
    const apps::MonitorPolicyOutput stop = policy.Step(all);
    test->Expect(
        stop.terminal ==
                apps::MonitorTerminal::CleanStop &&
            HasNoNotification(stop) &&
            !policy.ready_sent(),
        "stop wins over fatal, readiness failure and notifications");

    all.stop_requested = false;
    const apps::MonitorPolicyOutput fatal = policy.Step(all);
    test->Expect(
        fatal.terminal ==
                apps::MonitorTerminal::Fatal &&
            HasNoNotification(fatal) &&
            !policy.ready_sent(),
        "fatal wins over readiness failure and notifications");

    all.fatal = false;
    const apps::MonitorPolicyOutput failure =
        policy.Step(all);
    test->Expect(
        failure.terminal ==
                apps::MonitorTerminal::FailedBeforeReady &&
            HasNoNotification(failure) &&
            !policy.ready_sent(),
        "pre-ready cumulative failure wins over READY and due work");
}

void CheckReadyExactlyOnce(TestContext* test) {
    apps::MonitorPolicy unconfirmed;
    apps::MonitorPolicyInput unconfirmed_input;
    unconfirmed_input.current_readiness_generation = 5U;
    test->Expect(
        unconfirmed.Step(unconfirmed_input).send_ready &&
            unconfirmed.Step(unconfirmed_input).send_ready &&
            !unconfirmed.ready_sent(),
        "an unconfirmed READY action remains retryable and uncommitted");

    apps::MonitorPolicy policy;
    apps::MonitorPolicyInput input;
    input.current_readiness_generation = 7U;

    std::uint32_t ready_count = 0U;
    for (std::uint32_t index = 0U; index < 8U; ++index) {
        const apps::MonitorPolicyOutput output =
            policy.Step(input);
        ready_count += output.send_ready ? 1U : 0U;
        if (output.send_ready) {
            policy.ConfirmReadySent(
                input.current_readiness_generation);
        }
        test->Expect(
            output.terminal ==
                apps::MonitorTerminal::None,
            "a stable ready generation remains nonterminal");
    }
    test->Expect(
        ready_count == 1U,
        "READY is emitted exactly once for the service lifetime");
}

void CheckWatchdogGating(TestContext* test) {
    apps::MonitorPolicy never_ready;
    apps::MonitorPolicyInput waiting;
    waiting.watchdog_due = true;
    for (std::uint32_t index = 0U; index < 4U; ++index) {
        const apps::MonitorPolicyOutput output =
            never_ready.Step(waiting);
        test->Expect(
            !output.send_watchdog,
            "watchdog is never emitted before READY");
    }

    apps::MonitorPolicy ready;
    apps::MonitorPolicyInput first_ready;
    first_ready.current_readiness_generation = 11U;
    first_ready.watchdog_due = true;
    test->Expect(
        !ready.Step(first_ready).send_watchdog,
        "the READY transition itself does not emit watchdog");
    ready.ConfirmReadySent(
        first_ready.current_readiness_generation);
    test->Expect(
        ready.Step(first_ready).send_watchdog,
        "a later stable ready step may emit a due watchdog");
}

void CheckReadyLosses(TestContext* test) {
    const auto make_ready = [](
                                apps::MonitorPolicy* policy,
                                std::uint64_t generation) {
        apps::MonitorPolicyInput input;
        input.current_readiness_generation = generation;
        const apps::MonitorPolicyOutput output =
            policy->Step(input);
        if (output.send_ready) {
            policy->ConfirmReadySent(generation);
        }
        return output;
    };

    apps::MonitorPolicy replaced;
    static_cast<void>(make_ready(&replaced, 3U));
    apps::MonitorPolicyInput replacement;
    replacement.current_readiness_generation = 4U;
    replacement.status_due = true;
    replacement.watchdog_due = true;
    const apps::MonitorPolicyOutput replacement_output =
        replaced.Step(replacement);
    test->Expect(
        replacement_output.terminal ==
                apps::MonitorTerminal::LostAfterReady &&
            HasNoNotification(replacement_output),
        "a replacement readiness generation is terminal after READY");

    apps::MonitorPolicy disappeared;
    static_cast<void>(make_ready(&disappeared, 3U));
    apps::MonitorPolicyInput no_generation;
    const apps::MonitorPolicyOutput no_generation_output =
        disappeared.Step(no_generation);
    test->Expect(
        no_generation_output.terminal ==
                apps::MonitorTerminal::LostAfterReady &&
            HasNoNotification(no_generation_output),
        "loss of the announced generation is terminal");

    apps::MonitorPolicy failed_logon;
    static_cast<void>(make_ready(&failed_logon, 3U));
    apps::MonitorPolicyInput logon_failure;
    logon_failure.current_readiness_generation = 3U;
    logon_failure.failed_logon_responses = 1U;
    logon_failure.status_due = true;
    logon_failure.watchdog_due = true;
    const apps::MonitorPolicyOutput failed_logon_output =
        failed_logon.Step(logon_failure);
    test->Expect(
        failed_logon_output.terminal ==
                apps::MonitorTerminal::LostAfterReady &&
            HasNoNotification(failed_logon_output),
        "cumulative failed logon is terminal after READY");

    apps::MonitorPolicy failed_subscription;
    static_cast<void>(make_ready(&failed_subscription, 3U));
    apps::MonitorPolicyInput subscription_failure;
    subscription_failure.current_readiness_generation = 3U;
    subscription_failure
        .required_subscription_failure_observed_mask = 0x4U;
    const apps::MonitorPolicyOutput
        failed_subscription_output =
            failed_subscription.Step(subscription_failure);
    test->Expect(
        failed_subscription_output.terminal ==
                apps::MonitorTerminal::LostAfterReady &&
            HasNoNotification(failed_subscription_output),
        "cumulative required subscription failure is terminal after READY");

    apps::MonitorPolicy fatal;
    static_cast<void>(make_ready(&fatal, 3U));
    apps::MonitorPolicyInput fatal_input;
    fatal_input.current_readiness_generation = 3U;
    fatal_input.fatal = true;
    fatal_input.status_due = true;
    fatal_input.watchdog_due = true;
    const apps::MonitorPolicyOutput fatal_output =
        fatal.Step(fatal_input);
    test->Expect(
        fatal_output.terminal ==
                apps::MonitorTerminal::Fatal &&
            HasNoNotification(fatal_output),
        "fatal remains terminal after READY");

    apps::MonitorPolicy stop;
    static_cast<void>(make_ready(&stop, 3U));
    apps::MonitorPolicyInput stop_input;
    stop_input.current_readiness_generation = 0U;
    stop_input.failed_logon_responses = 1U;
    stop_input.stop_requested = true;
    stop_input.status_due = true;
    stop_input.watchdog_due = true;
    const apps::MonitorPolicyOutput stop_output =
        stop.Step(stop_input);
    test->Expect(
        stop_output.terminal ==
                apps::MonitorTerminal::CleanStop &&
            HasNoNotification(stop_output),
        "stop remains highest priority after READY");
}

void CheckPreReadyFailureKinds(TestContext* test) {
    apps::MonitorPolicy logon;
    apps::MonitorPolicyInput logon_input;
    logon_input.failed_logon_responses = 1U;
    test->Expect(
        logon.Step(logon_input).terminal ==
            apps::MonitorTerminal::FailedBeforeReady,
        "failed logon terminates before READY");

    apps::MonitorPolicy subscription;
    apps::MonitorPolicyInput subscription_input;
    subscription_input
        .required_subscription_failure_observed_mask = 1U;
    test->Expect(
        subscription.Step(subscription_input).terminal ==
            apps::MonitorTerminal::FailedBeforeReady,
        "required subscription failure terminates before READY");
}

}  // namespace

int main() {
    TestContext test;
    CheckCompleteActionSequence(&test);
    CheckTerminalPriority(&test);
    CheckReadyExactlyOnce(&test);
    CheckWatchdogGating(&test);
    CheckReadyLosses(&test);
    CheckPreReadyFailureKinds(&test);

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " monitor policy checks failed\n";
        return 1;
    }
    std::cout
        << "monitor policy preserves terminal priority, READY and watchdog contracts\n";
    return 0;
}
