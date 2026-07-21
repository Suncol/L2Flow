#include "l2flow/ops/metrics_worker.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <sys/stat.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>

namespace ops = l2flow::ops;

namespace {

using namespace std::chrono_literals;

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

class TempDirectory final {
public:
    TempDirectory() {
        std::array<char, 64U> pattern{};
        constexpr std::string_view value =
            "/tmp/l2flow-metrics-worker-XXXXXX";
        static_assert(value.size() < pattern.size());
        std::copy(
            value.begin(), value.end(), pattern.begin());
        const char* const created = ::mkdtemp(pattern.data());
        if (created == nullptr) {
            throw std::runtime_error(
                std::string("mkdtemp failed: ") +
                std::strerror(errno));
        }
        path_ = created;
    }

    ~TempDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] const std::filesystem::path& path()
        const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

[[nodiscard]] std::string ReadText(
    const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    return std::string(
        std::istreambuf_iterator<char>(input),
        std::istreambuf_iterator<char>());
}

[[nodiscard]] bool WaitForChild(
    pid_t child,
    int* status,
    std::chrono::milliseconds timeout) {
    const auto deadline =
        std::chrono::steady_clock::now() + timeout;
    for (;;) {
        pid_t waited = -1;
        do {
            waited = ::waitpid(
                child, status, WNOHANG);
        } while (waited < 0 && errno == EINTR);
        if (waited == child) {
            return true;
        }
        if (waited < 0) {
            return false;
        }
        if (std::chrono::steady_clock::now() >=
            deadline) {
            break;
        }
        std::this_thread::sleep_for(1ms);
    }

    static_cast<void>(::kill(child, SIGKILL));
    pid_t reaped = -1;
    do {
        reaped = ::waitpid(child, status, 0);
    } while (reaped < 0 && errno == EINTR);
    static_cast<void>(reaped);
    return false;
}

[[nodiscard]] bool IsExactLeaseCollision(
    const std::string& path) {
    std::string error;
    std::unique_ptr<ops::PrometheusTextfileLease>
        collision =
            ops::AcquirePrometheusTextfileLease(
                path, &error);
    return collision == nullptr &&
           error == "metrics textfile is already leased";
}

enum class BackendBehavior {
    kSucceed,
    kFail,
    kThrow,
};

struct PublishCall final {
    std::string path;
    std::string contents;
};

class ControlledBackend final
    : public ops::MetricsPublishBackend {
public:
    explicit ControlledBackend(bool block_first = false)
        : block_first_(block_first) {}

    [[nodiscard]] bool Publish(
        const std::string& path,
        std::string_view contents,
        std::string* error) override {
        BackendBehavior behavior = BackendBehavior::kSucceed;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            calls_.push_back(
                PublishCall{path, std::string(contents)});
            const std::size_t call_number = calls_.size();
            const auto behavior_iterator =
                behavior_.find(std::string(contents));
            if (behavior_iterator != behavior_.end()) {
                behavior = behavior_iterator->second;
            }
            changed_.notify_all();
            if (block_first_ && call_number == 1U) {
                changed_.wait(
                    lock,
                    [this] { return first_released_; });
            }
        }

        if (behavior == BackendBehavior::kThrow) {
            throw std::runtime_error(
                std::string("backend leaked: ") +
                std::string(contents));
        }
        if (behavior == BackendBehavior::kFail) {
            if (error != nullptr) {
                *error =
                    std::string("backend leaked: ") +
                    std::string(contents);
            }
            return false;
        }
        return true;
    }

    void SetBehavior(
        std::string contents,
        BackendBehavior behavior) {
        const std::lock_guard<std::mutex> lock(mutex_);
        behavior_[std::move(contents)] = behavior;
    }

    [[nodiscard]] bool WaitForCallCount(
        std::size_t expected) {
        std::unique_lock<std::mutex> lock(mutex_);
        return changed_.wait_for(
            lock,
            5s,
            [this, expected] {
                return calls_.size() >= expected;
            });
    }

    void ReleaseFirst() {
        {
            const std::lock_guard<std::mutex> lock(mutex_);
            first_released_ = true;
        }
        changed_.notify_all();
    }

    [[nodiscard]] std::vector<PublishCall> Calls() const {
        const std::lock_guard<std::mutex> lock(mutex_);
        return calls_;
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable changed_;
    std::vector<PublishCall> calls_;
    std::unordered_map<std::string, BackendBehavior> behavior_;
    bool block_first_ = false;
    bool first_released_ = false;
};

class SelfStoppingBackend final
    : public ops::MetricsPublishBackend {
public:
    void Attach(ops::MetricsWorker* worker) {
        const std::lock_guard<std::mutex> lock(mutex_);
        worker_ = worker;
    }

    [[nodiscard]] bool Publish(
        const std::string& path,
        std::string_view contents,
        std::string* error) override {
        static_cast<void>(path);
        static_cast<void>(contents);
        static_cast<void>(error);
        ops::MetricsWorker* worker = nullptr;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            entered_ = true;
            changed_.notify_all();
            changed_.wait(
                lock,
                [this] { return release_; });
            worker = worker_;
        }
        if (worker == nullptr) {
            return false;
        }
        worker->StopAndJoin();
        self_stop_returned_.store(
            true, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool WaitUntilEntered() {
        std::unique_lock<std::mutex> lock(mutex_);
        return changed_.wait_for(
            lock,
            5s,
            [this] { return entered_; });
    }

    void Release() {
        {
            const std::lock_guard<std::mutex> lock(mutex_);
            release_ = true;
        }
        changed_.notify_all();
    }

    [[nodiscard]] bool SelfStopReturned() const noexcept {
        return self_stop_returned_.load(
            std::memory_order_acquire);
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable changed_;
    ops::MetricsWorker* worker_ = nullptr;
    bool entered_ = false;
    bool release_ = false;
    std::atomic<bool> self_stop_returned_{false};
};

[[nodiscard]] bool WaitForCompleted(
    const ops::MetricsWorker& worker,
    std::uint64_t expected) {
    const auto deadline =
        std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < deadline) {
        if (worker.Snapshot().completed >= expected) {
            return true;
        }
        std::this_thread::sleep_for(1ms);
    }
    return worker.Snapshot().completed >= expected;
}

void CheckLatestWinsAndStopDrain(TestContext* test) {
    const auto backend =
        std::make_shared<ControlledBackend>(true);
    ops::MetricsWorker worker("/unused/metrics.prom", backend);

    test->Expect(
        worker.Submit("first"),
        "first payload is accepted");
    test->Expect(
        backend->WaitForCallCount(1U),
        "worker begins the first blocking backend attempt");

    // The backend is still blocked here. These calls returning proves Submit
    // does not execute the publication operation on the caller.
    test->Expect(
        worker.Submit("obsolete"),
        "Submit returns while backend is blocked");
    test->Expect(
        worker.Submit("latest"),
        "a newer Submit returns while backend is blocked");

    const ops::MetricsWorkerSnapshot blocked =
        worker.Snapshot();
    test->Expect(
        blocked.submitted == 3U &&
            blocked.completed == 0U &&
            blocked.failed == 0U &&
            blocked.dropped == 1U,
        "blocked snapshot accounts for one replaced pending value");

    std::atomic<bool> stop_returned{false};
    std::thread stopper([&] {
        worker.StopAndJoin();
        stop_returned.store(true, std::memory_order_release);
    });
    std::this_thread::sleep_for(20ms);
    test->Expect(
        !stop_returned.load(std::memory_order_acquire),
        "StopAndJoin waits for the in-flight backend attempt");

    backend->ReleaseFirst();
    stopper.join();

    const std::vector<PublishCall> calls = backend->Calls();
    test->Expect(
        calls.size() == 2U &&
            calls[0].path == "/unused/metrics.prom" &&
            calls[0].contents == "first" &&
            calls[1].contents == "latest",
        "stop drains the current and latest pending values only");

    const ops::MetricsWorkerSnapshot finished =
        worker.Snapshot();
    test->Expect(
        finished.submitted == 3U &&
            finished.completed == 2U &&
            finished.failed == 0U &&
            finished.dropped == 1U,
        "final counters preserve latest-wins accounting");
    test->Expect(
        !worker.Submit("after-stop") &&
            worker.Snapshot().submitted == 3U,
        "shutdown rejects submissions without changing counters");
}

void CheckFailureIsolationAndErrorPrivacy(
    TestContext* test) {
    const auto backend =
        std::make_shared<ControlledBackend>();
    constexpr std::string_view failure_secret =
        "failure-secret-metric-body";
    constexpr std::string_view exception_secret =
        "exception-secret-metric-body";
    backend->SetBehavior(
        std::string(failure_secret),
        BackendBehavior::kFail);
    backend->SetBehavior(
        std::string(exception_secret),
        BackendBehavior::kThrow);

    ops::MetricsWorker worker("/unused/metrics.prom", backend);
    test->Expect(
        worker.Submit(std::string(failure_secret)),
        "backend-failure payload is accepted");
    test->Expect(
        WaitForCompleted(worker, 1U),
        "false backend result is recorded");

    ops::MetricsWorkerSnapshot snapshot = worker.Snapshot();
    test->Expect(
        snapshot.submitted == 1U &&
            snapshot.completed == 1U &&
            snapshot.failed == 1U &&
            snapshot.last_error_generation == 1U,
        "backend false result advances failure counters");
    const std::string first_error = worker.TakeLastError();
    test->Expect(
        !first_error.empty() &&
            first_error.find(failure_secret) ==
                std::string::npos,
        "worker never forwards metric contents in failure text");
    test->Expect(
        worker.TakeLastError().empty() &&
            worker.Snapshot().last_error_generation == 1U,
        "taking the error clears text but not its generation");

    test->Expect(
        worker.Submit("success-after-failure"),
        "worker accepts work after backend failure");
    test->Expect(
        WaitForCompleted(worker, 2U),
        "worker completes work after backend failure");
    test->Expect(
        worker.Submit(std::string(exception_secret)),
        "backend-exception payload is accepted");
    test->Expect(
        WaitForCompleted(worker, 3U),
        "backend exception is contained by worker thread");

    const std::string exception_error =
        worker.TakeLastError();
    test->Expect(
        !exception_error.empty() &&
            exception_error.find(exception_secret) ==
                std::string::npos,
        "worker never exposes exception text containing metrics");

    test->Expect(
        worker.Submit("success-after-exception"),
        "worker remains alive after a backend exception");
    worker.StopAndJoin();

    snapshot = worker.Snapshot();
    test->Expect(
        snapshot.submitted == 4U &&
            snapshot.completed == 4U &&
            snapshot.failed == 2U &&
            snapshot.dropped == 0U &&
            snapshot.last_error_generation == 2U,
        "failure and exception accounting is exact");
    test->Expect(
        backend->Calls().size() == 4U,
        "failure and exception do not terminate the worker");
}

void CheckPayloadBoundAndBackendValidation(
    TestContext* test) {
    const auto backend =
        std::make_shared<ControlledBackend>();
    ops::MetricsWorker worker("/unused/metrics.prom", backend);

    std::string oversized(
        ops::kMaximumMetricsWorkerPayloadBytes + 1U,
        'x');
    test->Expect(
        !worker.Submit(std::move(oversized)),
        "payload larger than the marked-textfile body limit is rejected");
    test->Expect(
        worker.Snapshot().submitted == 0U &&
            backend->Calls().empty(),
        "rejected payload never reaches the backend");

    std::string maximum(
        ops::kMaximumMetricsWorkerPayloadBytes,
        'y');
    test->Expect(
        worker.Submit(std::move(maximum)),
        "payload exactly at the marked-textfile body limit is accepted");
    worker.StopAndJoin();
    const std::vector<PublishCall> calls = backend->Calls();
    test->Expect(
        calls.size() == 1U &&
            calls[0].contents.size() ==
                ops::kMaximumMetricsWorkerPayloadBytes,
        "exact-limit payload reaches the backend intact");

    bool rejected_null = false;
    try {
        ops::MetricsWorker invalid(
            "/unused/metrics.prom",
            std::shared_ptr<ops::MetricsPublishBackend>{});
    } catch (const std::invalid_argument&) {
        rejected_null = true;
    }
    test->Expect(
        rejected_null,
        "constructor rejects a null backend");
}

void CheckDefaultFilesystemBackend(TestContext* test) {
    TempDirectory temporary;
    const std::filesystem::path target =
        temporary.path() / "metrics.prom";
    constexpr std::string_view body =
        "l2flow_metrics_worker_default_backend 7\n";

    {
        ops::MetricsWorker worker(target.string());
        test->Expect(
            worker.Submit(std::string(body)),
            "default worker accepts a real textfile snapshot");
        worker.StopAndJoin();
        const ops::MetricsWorkerSnapshot snapshot =
            worker.Snapshot();
        test->Expect(
            snapshot.submitted == 1U &&
                snapshot.completed == 1U &&
                snapshot.failed == 0U &&
                snapshot.dropped == 0U,
            "default worker drains exactly one successful publication");

        const pid_t independent_child = ::fork();
        test->Expect(
            independent_child >= 0,
            "creates an independent lease-contender process");
        if (independent_child == 0) {
            ::execl(
                "/proc/self/exe",
                "/proc/self/exe",
                "--try-default-worker",
                target.c_str(),
                static_cast<char*>(nullptr));
            std::_Exit(127);
        }
        if (independent_child > 0) {
            int status = 0;
            const bool waited = WaitForChild(
                independent_child,
                &status,
                7s);
            test->Expect(
                waited &&
                    WIFEXITED(status) &&
                    WEXITSTATUS(status) == 0,
                "an independently exec'd process receives the exact "
                "live-lease collision classification");
        }

        const bool same_target_collision =
            IsExactLeaseCollision(target.string());
        bool same_target_constructor_rejected = false;
        try {
            ops::MetricsWorker collision(target.string());
        } catch (const std::runtime_error&) {
            same_target_constructor_rejected = true;
        }
        test->Expect(
            same_target_collision &&
                same_target_constructor_rejected,
            "a live worker reports the exact same-target collision "
            "and default construction fails");

        const std::string case_variant =
            (temporary.path() / "METRICS.PROM").string();
        const bool case_variant_collision =
            IsExactLeaseCollision(case_variant);
        bool case_variant_constructor_rejected = false;
        try {
            ops::MetricsWorker collision(case_variant);
        } catch (const std::runtime_error&) {
            case_variant_constructor_rejected = true;
        }
        test->Expect(
            case_variant_collision &&
                case_variant_constructor_rejected,
            "a live worker reports the exact ASCII-case collision "
            "and default construction fails");
    }

    const std::string expected =
        std::string(ops::kPrometheusTextfileMagic) +
        std::string(body);
    test->Expect(
        ReadText(target) == expected,
        "default worker writes the typed metrics body exactly");
    struct stat metadata {};
    test->Expect(
        ::stat(target.c_str(), &metadata) == 0 &&
            S_ISREG(metadata.st_mode) &&
            (metadata.st_mode & 07777) == 0600 &&
            metadata.st_uid == ::geteuid() &&
            metadata.st_nlink == 1,
        "default worker output remains a private singly linked file");

    bool reacquired = true;
    try {
        ops::MetricsWorker replacement(target.string());
        replacement.StopAndJoin();
    } catch (...) {
        reacquired = false;
    }
    test->Expect(
        reacquired,
        "the persistent sidecar can be reacquired after owner destruction");
}

void CheckRetainedDirectoryPublication(TestContext* test) {
    TempDirectory temporary;
    const std::filesystem::path selected =
        temporary.path() / "selected";
    const std::filesystem::path moved =
        temporary.path() / "moved";
    std::filesystem::create_directory(selected);
    static_cast<void>(::chmod(selected.c_str(), 0700));
    const std::filesystem::path configured_target =
        selected / "metrics.prom";

    ops::MetricsWorker worker(configured_target.string());
    std::filesystem::rename(selected, moved);
    std::filesystem::create_directory(selected);
    static_cast<void>(::chmod(selected.c_str(), 0700));

    constexpr std::string_view body =
        "l2flow_retained_directory_publish 1\n";
    test->Expect(
        worker.Submit(std::string(body)),
        "worker accepts publication after its directory is renamed");
    worker.StopAndJoin();

    const std::filesystem::path retained_target =
        moved / "metrics.prom";
    test->Expect(
        ReadText(retained_target) ==
            std::string(ops::kPrometheusTextfileMagic) +
                std::string(body),
        "worker publishes relative to the leased directory inode");
    test->Expect(
        !std::filesystem::exists(configured_target),
        "replacement of the original path cannot redirect a live worker");
}

[[nodiscard]] bool RunConcurrentSelfStopScenario() {
    const auto backend =
        std::make_shared<SelfStoppingBackend>();
    ops::MetricsWorker worker(
        "/unused/metrics.prom", backend);
    backend->Attach(&worker);
    if (!worker.Submit("self-stop") ||
        !backend->WaitUntilEntered()) {
        return false;
    }

    std::thread external_stopper(
        [&worker] { worker.StopAndJoin(); });
    const auto deadline =
        std::chrono::steady_clock::now() + 5s;
    while (worker.Submit("not-yet-stopped") &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(1ms);
    }
    if (std::chrono::steady_clock::now() >= deadline) {
        backend->Release();
        external_stopper.join();
        return false;
    }
    // The backend is still blocked and has not requested stop. Therefore a
    // rejected Submit proves the external caller has completed StopAndJoin's
    // state transition. An implementation that holds a lifecycle mutex from
    // that transition through join now deterministically deadlocks when the
    // backend makes the re-entrant stop request below.
    backend->Release();
    external_stopper.join();
    return backend->SelfStopReturned();
}

void CheckConcurrentSelfStopDoesNotDeadlock(
    TestContext* test) {
    const pid_t child = ::fork();
    test->Expect(
        child >= 0,
        "creates an isolated metrics self-stop regression process");
    if (child < 0) {
        return;
    }
    if (child == 0) {
        static_cast<void>(::alarm(7U));
        std::_Exit(
            RunConcurrentSelfStopScenario() ? 0 : 1);
    }

    int status = 0;
    const bool waited = WaitForChild(
        child, &status, 9s);
    test->Expect(
        waited &&
            WIFEXITED(status) &&
            WEXITSTATUS(status) == 0,
        "backend self-stop and an external join complete without deadlock");
}

}  // namespace

int main(int argc, char** argv) {
    if (argc == 3 &&
        std::string_view(argv[1]) ==
            "--try-default-worker") {
        static_cast<void>(::alarm(5U));
        return IsExactLeaseCollision(argv[2]) ? 0 : 1;
    }

    TestContext test;
    CheckConcurrentSelfStopDoesNotDeadlock(&test);
    CheckLatestWinsAndStopDrain(&test);
    CheckFailureIsolationAndErrorPrivacy(&test);
    CheckPayloadBoundAndBackendValidation(&test);
    CheckDefaultFilesystemBackend(&test);
    CheckRetainedDirectoryPublication(&test);

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " metrics-worker assertion(s) failed\n";
        return 1;
    }
    std::cout
        << "metrics worker bounded async publication checks passed\n";
    return 0;
}
