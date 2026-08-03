#include "managed_sidecar_process_v1.h"

#include <cerrno>
#include <csignal>
#include <new>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

namespace l2flow::apps {

std::string_view ManagedSidecarProcessErrorNameV1(
    ManagedSidecarProcessErrorV1 error) noexcept {
    switch (error) {
        case ManagedSidecarProcessErrorV1::kNone:
            return "none";
        case ManagedSidecarProcessErrorV1::kNullOutput:
            return "null_output";
        case ManagedSidecarProcessErrorV1::kInvalidArgument:
            return "invalid_argument";
        case ManagedSidecarProcessErrorV1::kSpawnFailed:
            return "spawn_failed";
        case ManagedSidecarProcessErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case ManagedSidecarProcessErrorV1::kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

std::string ManagedSidecarWaitStatusDescriptionV1(int wait_status) {
    if (wait_status < 0) {
        return "status_unavailable";
    }
    if (WIFEXITED(wait_status)) {
        return "exited exit_code=" +
               std::to_string(WEXITSTATUS(wait_status));
    }
    if (WIFSIGNALED(wait_status)) {
        std::string result =
            "signaled signal=" +
            std::to_string(WTERMSIG(wait_status));
#ifdef WCOREDUMP
        result += WCOREDUMP(wait_status) != 0
                      ? " core_dumped=true"
                      : " core_dumped=false";
#endif
        return result;
    }
    if (WIFSTOPPED(wait_status)) {
        return "stopped signal=" +
               std::to_string(WSTOPSIG(wait_status));
    }
#ifdef WIFCONTINUED
    if (WIFCONTINUED(wait_status)) {
        return "continued";
    }
#endif
    return "unknown raw=" + std::to_string(wait_status);
}

ManagedSidecarProcessV1::ManagedSidecarProcessV1(pid_t pid) noexcept
    : pid_(pid) {}

ManagedSidecarProcessV1::~ManagedSidecarProcessV1() {
    static_cast<void>(
        StopAndWait(std::chrono::milliseconds(2000)));
}

ManagedSidecarProcessErrorV1 ManagedSidecarProcessV1::Spawn(
    const std::filesystem::path& executable,
    std::span<const std::string> arguments,
    std::unique_ptr<ManagedSidecarProcessV1>* output,
    int* system_error_number) noexcept {
    if (output == nullptr) {
        return ManagedSidecarProcessErrorV1::kNullOutput;
    }
    output->reset();
    if (system_error_number != nullptr) {
        *system_error_number = 0;
    }
    try {
        const std::string executable_text = executable.string();
        if (executable_text.empty() || !executable.is_absolute()) {
            return ManagedSidecarProcessErrorV1::kInvalidArgument;
        }

        std::vector<std::string> owned_arguments;
        owned_arguments.reserve(arguments.size() + 1U);
        owned_arguments.emplace_back(executable_text);
        owned_arguments.insert(
            owned_arguments.end(), arguments.begin(), arguments.end());

        std::vector<char*> argv;
        argv.reserve(owned_arguments.size() + 1U);
        for (std::string& argument : owned_arguments) {
            argv.push_back(argument.data());
        }
        argv.push_back(nullptr);

        pid_t child = -1;
        const int spawn_error = ::posix_spawn(
            &child,
            executable_text.c_str(),
            nullptr,
            nullptr,
            argv.data(),
            environ);
        if (spawn_error != 0 || child <= 0) {
            if (system_error_number != nullptr) {
                *system_error_number =
                    spawn_error != 0 ? spawn_error : ECHILD;
            }
            return ManagedSidecarProcessErrorV1::kSpawnFailed;
        }

        try {
            *output = std::unique_ptr<ManagedSidecarProcessV1>(
                new ManagedSidecarProcessV1(child));
        } catch (...) {
            static_cast<void>(::kill(child, SIGTERM));
            int status = 0;
            while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
            }
            throw;
        }
        return ManagedSidecarProcessErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return ManagedSidecarProcessErrorV1::kResourceExhausted;
    } catch (...) {
        return ManagedSidecarProcessErrorV1::kUnexpectedFailure;
    }
}

bool ManagedSidecarProcessV1::Running(int* wait_status) noexcept {
    if (wait_status != nullptr) {
        *wait_status = wait_status_;
    }
    if (reaped_ || pid_ <= 0) {
        return false;
    }
    int status = 0;
    pid_t result = -1;
    do {
        result = ::waitpid(pid_, &status, WNOHANG);
    } while (result < 0 && errno == EINTR);
    if (result == 0) {
        return true;
    }
    if (result == pid_ || (result < 0 && errno == ECHILD)) {
        reaped_ = true;
        wait_status_ = result == pid_ ? status : -1;
        if (wait_status != nullptr) {
            *wait_status = wait_status_;
        }
    }
    return false;
}

bool ManagedSidecarProcessV1::WaitForExit(
    std::chrono::milliseconds timeout,
    int* wait_status) noexcept {
    if (timeout < std::chrono::milliseconds::zero()) {
        return false;
    }
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        if (!Running(wait_status)) {
            return reaped_;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void ManagedSidecarProcessV1::RequestStop() noexcept {
    if (!reaped_ && pid_ > 0) {
        static_cast<void>(::kill(pid_, SIGTERM));
    }
}

bool ManagedSidecarProcessV1::StopAndWait(
    std::chrono::milliseconds timeout) noexcept {
    if (reaped_ || pid_ <= 0) {
        return reaped_;
    }
    RequestStop();
    if (WaitForExit(timeout)) {
        return true;
    }
    // The target is the exact still-owned positive child PID.  SIGKILL is a
    // last-resort teardown. Reaping remains bounded as well: a child stuck in
    // uninterruptible kernel sleep must not block router shutdown forever.
    const int kill_result = ::kill(pid_, SIGKILL);
    if (kill_result != 0 && errno != ESRCH) {
        return false;
    }
    return WaitForExit(timeout);
}

pid_t ManagedSidecarProcessV1::pid() const noexcept {
    return reaped_ ? -1 : pid_;
}

}  // namespace l2flow::apps
