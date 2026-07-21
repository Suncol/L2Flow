#pragma once

#include "l2flow/ops/metrics_textfile.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::ops {

// Keep this limit equal to the limit enforced by PublishPrometheusTextfile.
// MetricsWorker rejects a larger payload before it enters the worker queue.
inline constexpr std::size_t kMaximumMetricsWorkerPayloadBytes =
    kMaximumPrometheusTextfileBodyBytes;

// A test seam for the blocking publication operation. Implementations may
// block and may throw. MetricsWorker catches every exception before it can
// escape the worker thread. Backend error text is deliberately not exposed by
// MetricsWorker because an injected backend could accidentally include metric
// contents in it.
class MetricsPublishBackend {
public:
    virtual ~MetricsPublishBackend() = default;

    [[nodiscard]] virtual bool Publish(
        const std::string& path,
        std::string_view contents,
        std::string* error) = 0;
};

struct MetricsWorkerSnapshot final {
    // Every counter is monotonic and saturates at UINT64_MAX.

    // Accepted Submit calls.
    std::uint64_t submitted = 0U;

    // Backend attempts which returned or threw. Failed attempts are included.
    std::uint64_t completed = 0U;

    // Subset of completed attempts which returned false or threw.
    std::uint64_t failed = 0U;

    // Accepted pending values replaced before their backend attempt began.
    std::uint64_t dropped = 0U;

    // Generation advanced for every backend or internal worker failure.
    std::uint64_t last_error_generation = 0U;
};

// Owns one publication thread and at most one pending metrics payload. Submit
// never performs filesystem I/O. If a pending payload already exists, Submit
// atomically replaces it with the latest value and increments dropped.
class MetricsWorker final {
public:
    // Acquires a persistent ownership lease at construction, then publishes
    // relative to the retained destination-directory descriptor. Throws
    // std::runtime_error if the path cannot be validated or leased.
    explicit MetricsWorker(std::string path);

    // backend must be non-null and must outlive no object other than the shared
    // ownership held by this worker.
    MetricsWorker(
        std::string path,
        std::shared_ptr<MetricsPublishBackend> backend);

    ~MetricsWorker();

    MetricsWorker(const MetricsWorker&) = delete;
    MetricsWorker& operator=(const MetricsWorker&) = delete;
    MetricsWorker(MetricsWorker&&) = delete;
    MetricsWorker& operator=(MetricsWorker&&) = delete;

    // Returns false without changing counters if the payload exceeds the
    // bounded limit or shutdown has started. No backend I/O occurs here.
    [[nodiscard]] bool Submit(std::string contents) noexcept;

    [[nodiscard]] MetricsWorkerSnapshot Snapshot() const noexcept;

    // Atomically takes the latest fixed, content-free failure classification.
    // The generation in Snapshot is not cleared.
    [[nodiscard]] std::string TakeLastError() noexcept;

    // Stops accepting submissions, finishes the current attempt and the latest
    // pending value (if any), and joins the worker. Repeated calls are safe.
    // A backend must not destroy its owning worker. If it calls StopAndJoin
    // from the publication thread, the call can only request stop; an external
    // owner must subsequently call StopAndJoin to perform the join.
    void StopAndJoin() noexcept;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::ops
