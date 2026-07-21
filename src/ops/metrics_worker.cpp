#include "l2flow/ops/metrics_worker.h"

#include "l2flow/ops/metrics_textfile.h"

#include <condition_variable>
#include <exception>
#include <limits>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <utility>

namespace l2flow::ops {
namespace {

thread_local const void*
    g_metrics_worker_thread_identity = nullptr;

void SaturatingIncrement(std::uint64_t* value) noexcept {
    if (*value != std::numeric_limits<std::uint64_t>::max()) {
        ++(*value);
    }
}

class TextfileMetricsPublishBackend final
    : public MetricsPublishBackend {
public:
    explicit TextfileMetricsPublishBackend(
        std::unique_ptr<PrometheusTextfileLease>
            lease) noexcept
        : lease_(std::move(lease)) {}

    [[nodiscard]] bool Publish(
        const std::string& path,
        std::string_view contents,
        std::string* error) override {
        static_cast<void>(path);
        return lease_->Publish(contents, error);
    }

private:
    std::unique_ptr<PrometheusTextfileLease> lease_;
};

[[nodiscard]] std::shared_ptr<MetricsPublishBackend>
MakeTextfileBackend(const std::string& path) {
    std::string error;
    std::unique_ptr<PrometheusTextfileLease> lease =
        AcquirePrometheusTextfileLease(
            path,
            &error);
    if (lease == nullptr) {
        throw std::runtime_error(
            "metrics textfile ownership lease failed");
    }
    return std::make_shared<
        TextfileMetricsPublishBackend>(
            std::move(lease));
}

}  // namespace

class MetricsWorker::Impl final {
public:
    Impl(
        std::string path,
        std::shared_ptr<MetricsPublishBackend> backend)
        : path_(std::move(path)),
          backend_(std::move(backend)),
          worker_(&Impl::ThreadMain, this) {}

    ~Impl() {
        StopAndJoin();
    }

    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;

    [[nodiscard]] bool Submit(std::string contents) noexcept {
        if (contents.size() >
            kMaximumMetricsWorkerPayloadBytes) {
            return false;
        }

        try {
            {
                const std::lock_guard<std::mutex> lock(state_mutex_);
                if (!accepting_) {
                    return false;
                }
                if (pending_.has_value()) {
                    SaturatingIncrement(&snapshot_.dropped);
                }
                pending_ = std::move(contents);
                SaturatingIncrement(&snapshot_.submitted);
            }
            state_changed_.notify_one();
            return true;
        } catch (...) {
            return false;
        }
    }

    [[nodiscard]] MetricsWorkerSnapshot Snapshot() const noexcept {
        try {
            const std::lock_guard<std::mutex> lock(state_mutex_);
            return snapshot_;
        } catch (...) {
            return {};
        }
    }

    [[nodiscard]] std::string TakeLastError() noexcept {
        try {
            const std::lock_guard<std::mutex> lock(state_mutex_);
            std::string result;
            result.swap(last_error_);
            return result;
        } catch (...) {
            return {};
        }
    }

    void StopAndJoin() noexcept {
        try {
            {
                const std::lock_guard<std::mutex> state_lock(
                    state_mutex_);
                accepting_ = false;
                stop_requested_ = true;
            }
            state_changed_.notify_one();

            // A backend may request stop from this worker. Test a TLS identity
            // before entering call_once: an external caller may already be
            // inside join(), and making the worker wait for that once-call
            // would deadlock the joiner. Unlike std::thread::id, this token
            // cannot be confused by OS thread-id reuse after Run returns.
            if (g_metrics_worker_thread_identity == this) {
                return;
            }
            std::call_once(
                join_once_,
                [this] {
                    if (worker_.joinable()) {
                        worker_.join();
                    }
                });
        } catch (...) {
            // std::mutex and a correctly owned joinable thread do not fail in
            // normal operation. Keep this API noexcept; the worker entry point
            // also has its own exception boundary.
        }
    }

private:
    void RecordAttempt(bool succeeded, bool backend_threw) noexcept {
        try {
            const std::lock_guard<std::mutex> lock(state_mutex_);
            in_flight_ = false;
            SaturatingIncrement(&snapshot_.completed);
            if (!succeeded) {
                SaturatingIncrement(&snapshot_.failed);
                SaturatingIncrement(
                    &snapshot_.last_error_generation);
                try {
                    last_error_ =
                        backend_threw
                            ? "metrics publication backend threw"
                            : "metrics publication failed";
                } catch (...) {
                    last_error_.clear();
                }
            }
        } catch (...) {
        }
    }

    void RecordUnexpectedWorkerFailure() noexcept {
        try {
            const std::lock_guard<std::mutex> lock(state_mutex_);
            accepting_ = false;
            stop_requested_ = true;
            if (in_flight_) {
                in_flight_ = false;
                SaturatingIncrement(&snapshot_.completed);
                SaturatingIncrement(&snapshot_.failed);
            }
            if (pending_.has_value()) {
                pending_.reset();
                SaturatingIncrement(&snapshot_.dropped);
            }
            SaturatingIncrement(
                &snapshot_.last_error_generation);
            try {
                last_error_ =
                    "metrics publication worker failed";
            } catch (...) {
                last_error_.clear();
            }
        } catch (...) {
        }
    }

    void Run() {
        for (;;) {
            std::string contents;
            {
                std::unique_lock<std::mutex> lock(state_mutex_);
                state_changed_.wait(
                    lock,
                    [this] {
                        return stop_requested_ ||
                               pending_.has_value();
                    });
                if (!pending_.has_value()) {
                    if (stop_requested_) {
                        return;
                    }
                    continue;
                }
                contents = std::move(*pending_);
                pending_.reset();
                in_flight_ = true;
            }

            bool succeeded = false;
            bool backend_threw = false;
            try {
                std::string ignored_backend_error;
                succeeded = backend_->Publish(
                    path_, contents, &ignored_backend_error);
            } catch (...) {
                backend_threw = true;
            }
            RecordAttempt(succeeded, backend_threw);
        }
    }

    void ThreadMain() noexcept {
        g_metrics_worker_thread_identity = this;
        try {
            Run();
        } catch (...) {
            RecordUnexpectedWorkerFailure();
        }
        g_metrics_worker_thread_identity = nullptr;
    }

    const std::string path_;
    const std::shared_ptr<MetricsPublishBackend> backend_;

    mutable std::mutex state_mutex_;
    std::condition_variable state_changed_;
    std::optional<std::string> pending_;
    bool in_flight_ = false;
    bool accepting_ = true;
    bool stop_requested_ = false;
    MetricsWorkerSnapshot snapshot_;
    std::string last_error_;

    std::once_flag join_once_;
    std::thread worker_;
};

MetricsWorker::MetricsWorker(std::string path)
    : MetricsWorker(
          path,
          MakeTextfileBackend(path)) {}

MetricsWorker::MetricsWorker(
    std::string path,
    std::shared_ptr<MetricsPublishBackend> backend) {
    if (backend == nullptr) {
        throw std::invalid_argument(
            "metrics publication backend must be non-null");
    }
    impl_ = std::make_unique<Impl>(
        std::move(path), std::move(backend));
}

MetricsWorker::~MetricsWorker() = default;

bool MetricsWorker::Submit(std::string contents) noexcept {
    return impl_->Submit(std::move(contents));
}

MetricsWorkerSnapshot MetricsWorker::Snapshot() const noexcept {
    return impl_->Snapshot();
}

std::string MetricsWorker::TakeLastError() noexcept {
    return impl_->TakeLastError();
}

void MetricsWorker::StopAndJoin() noexcept {
    impl_->StopAndJoin();
}

}  // namespace l2flow::ops
