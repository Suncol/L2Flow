#include "../apps/managed_sidecar_process_v1.h"

#include <chrono>
#include <csignal>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <sys/wait.h>
#include <unistd.h>

namespace {

class Test final {
public:
    void Expect(bool condition, const char* message) {
        if (!condition) {
            ++failures_;
            std::cerr << "FAIL: " << message << '\n';
        }
    }

    [[nodiscard]] int failures() const noexcept {
        return failures_;
    }

private:
    int failures_ = 0;
};

[[nodiscard]] std::filesystem::path SelfExecutable() {
    return std::filesystem::read_symlink("/proc/self/exe");
}

}  // namespace

int main(int argc, char** argv) {
    if (argc == 2 && std::string(argv[1]) == "--child-exit-seven") {
        return 7;
    }
    if (argc == 2 && std::string(argv[1]) == "--child-wait") {
        for (;;) {
            ::pause();
        }
    }
    if (argc == 2 &&
        std::string(argv[1]) == "--child-ignore-term") {
        static_cast<void>(::signal(SIGTERM, SIG_IGN));
        for (;;) {
            ::pause();
        }
    }

    using l2flow::apps::ManagedSidecarProcessErrorV1;
    using l2flow::apps::ManagedSidecarProcessV1;
    using l2flow::apps::ManagedSidecarWaitStatusDescriptionV1;
    Test test;

    test.Expect(
        ManagedSidecarWaitStatusDescriptionV1(-1) ==
            "status_unavailable",
        "missing wait status is not reported as a successful exit");
    test.Expect(
        ManagedSidecarWaitStatusDescriptionV1(7 << 8) ==
            "exited exit_code=7",
        "normal child exit status is decoded");

    std::unique_ptr<ManagedSidecarProcessV1> child;
    int system_error = 0;
    test.Expect(
        ManagedSidecarProcessV1::Spawn(
            "relative-program",
            {},
            &child,
            &system_error) ==
            ManagedSidecarProcessErrorV1::kInvalidArgument &&
            child == nullptr,
        "relative executable is rejected");

    const std::filesystem::path self = SelfExecutable();
    const std::vector<std::string> exit_arguments{
        "--child-exit-seven"};
    test.Expect(
        ManagedSidecarProcessV1::Spawn(
            self,
            exit_arguments,
            &child,
            &system_error) == ManagedSidecarProcessErrorV1::kNone &&
            child != nullptr && child->pid() > 0,
        "spawn owned child");
    int wait_status = 0;
    test.Expect(
        child != nullptr &&
            child->WaitForExit(
                std::chrono::milliseconds(2000), &wait_status) &&
            WIFEXITED(wait_status) && WEXITSTATUS(wait_status) == 7 &&
            child->pid() == -1,
        "wait and preserve exact child exit status");
    test.Expect(
        ManagedSidecarWaitStatusDescriptionV1(wait_status) ==
            "exited exit_code=7",
        "reaped child exit diagnostic retains the exit code");
    child.reset();

    const std::vector<std::string> wait_arguments{"--child-wait"};
    test.Expect(
        ManagedSidecarProcessV1::Spawn(
            self,
            wait_arguments,
            &child,
            &system_error) == ManagedSidecarProcessErrorV1::kNone &&
            child != nullptr,
        "spawn stoppable child");
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    test.Expect(
        child != nullptr && child->Running(),
        "owned child is running before stop");
    if (child != nullptr) {
        child->RequestStop();
        wait_status = 0;
        test.Expect(
            child->WaitForExit(
                std::chrono::milliseconds(2000), &wait_status) &&
                WIFSIGNALED(wait_status) &&
                WTERMSIG(wait_status) == SIGTERM,
            "SIGTERM stops only the owned child");
    }
    child.reset();

    const std::vector<std::string> ignore_arguments{
        "--child-ignore-term"};
    test.Expect(
        ManagedSidecarProcessV1::Spawn(
            self,
            ignore_arguments,
            &child,
            &system_error) == ManagedSidecarProcessErrorV1::kNone &&
            child != nullptr,
        "spawn TERM-resistant child for bounded SIGKILL fallback");
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    const auto stop_begin = std::chrono::steady_clock::now();
    const bool stopped =
        child != nullptr &&
        child->StopAndWait(std::chrono::milliseconds(50));
    const auto stop_elapsed =
        std::chrono::steady_clock::now() - stop_begin;
    wait_status = -1;
    const bool still_running =
        child != nullptr && child->Running(&wait_status);
    test.Expect(
        stopped && !still_running && WIFSIGNALED(wait_status) &&
            WTERMSIG(wait_status) == SIGKILL &&
            stop_elapsed < std::chrono::seconds(2),
        "TERM timeout falls back to exact-child SIGKILL without an "
        "unbounded wait");
    const std::string killed_description =
        ManagedSidecarWaitStatusDescriptionV1(wait_status);
    test.Expect(
        killed_description.find("signaled signal=") == 0U &&
            killed_description.find(std::to_string(SIGKILL)) !=
                std::string::npos,
        "signaled child diagnostic retains the terminating signal");
    child.reset();

    if (test.failures() != 0) {
        return 1;
    }
    std::cout << "PASS: managed sidecar process v1\n";
    return 0;
}
