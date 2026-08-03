#pragma once

#include <chrono>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <string_view>

#include <sys/types.h>

namespace l2flow::apps {

enum class ManagedSidecarProcessErrorV1 : unsigned char {
    kNone = 0U,
    kNullOutput,
    kInvalidArgument,
    kSpawnFailed,
    kResourceExhausted,
    kUnexpectedFailure,
};

[[nodiscard]] std::string_view ManagedSidecarProcessErrorNameV1(
    ManagedSidecarProcessErrorV1 error) noexcept;

// Decodes the raw waitpid status without treating ECHILD/no-status (-1) as a
// successful exit. Intended for failure and lifecycle logs only.
[[nodiscard]] std::string ManagedSidecarWaitStatusDescriptionV1(
    int wait_status);

// Owns exactly one child created with posix_spawn.  Destruction sends SIGTERM
// only to that exact positive PID and reaps it; it never targets a process
// group or an unresolved identifier.
class ManagedSidecarProcessV1 final {
public:
    ManagedSidecarProcessV1(const ManagedSidecarProcessV1&) = delete;
    ManagedSidecarProcessV1& operator=(
        const ManagedSidecarProcessV1&) = delete;
    ManagedSidecarProcessV1(ManagedSidecarProcessV1&&) = delete;
    ManagedSidecarProcessV1& operator=(
        ManagedSidecarProcessV1&&) = delete;
    ~ManagedSidecarProcessV1();

    [[nodiscard]] static ManagedSidecarProcessErrorV1 Spawn(
        const std::filesystem::path& executable,
        std::span<const std::string> arguments,
        std::unique_ptr<ManagedSidecarProcessV1>* output,
        int* system_error_number = nullptr) noexcept;

    // Returns true while the child is still running.  If it has exited, this
    // method reaps it exactly once and optionally returns the wait status.
    [[nodiscard]] bool Running(int* wait_status = nullptr) noexcept;

    // Waits without blocking longer than timeout.  A true return means the
    // child was reaped and wait_status contains the raw waitpid status.
    [[nodiscard]] bool WaitForExit(
        std::chrono::milliseconds timeout,
        int* wait_status = nullptr) noexcept;

    void RequestStop() noexcept;
    // Applies the same finite timeout to graceful SIGTERM and, if needed, to
    // exact-child SIGKILL reaping. It never performs an unbounded waitpid.
    // False means the child could not be reaped within those bounds.
    [[nodiscard]] bool StopAndWait(
        std::chrono::milliseconds timeout) noexcept;

    [[nodiscard]] pid_t pid() const noexcept;

private:
    explicit ManagedSidecarProcessV1(pid_t pid) noexcept;

    pid_t pid_ = -1;
    // -1 means the kernel no longer identifies this PID as our child but did
    // not return an authoritative wait status (ECHILD). Never synthesize a
    // successful exit status for that case.
    int wait_status_ = -1;
    bool reaped_ = false;
};

}  // namespace l2flow::apps
