#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/ipc/partial_order_event_control_v2.h"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string_view>

#include <sys/types.h>

namespace l2flow::apps {

enum class PartialEventStableBrokerCreateErrorV2 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kLifecyclePageCreateFailed,
    kPublicSocketCreateFailed,
    kHandoffSocketCreateFailed,
    kThreadCreateFailed,
    kResourceExhausted,
    kUnexpectedFailure,
};

[[nodiscard]] std::string_view
PartialEventStableBrokerCreateErrorNameV2(
    PartialEventStableBrokerCreateErrorV2 error) noexcept;

struct PartialEventStableBrokerConfigV2 final {
    std::filesystem::path public_socket_path;
    // Empty for same-process composition. A nonempty path enables the exact
    // worker-PID SCM_RIGHTS handoff listener.
    std::filesystem::path worker_handoff_socket_path;
    common::Identity128 expected_run_id{};
    std::uint64_t expected_session_epoch = 0U;
    std::uint32_t expected_trade_date = 0U;
    std::uint64_t maximum_mapping_bytes = 0U;
    uid_t allowed_uid = static_cast<uid_t>(-1);
    std::uint32_t listen_backlog = 16U;
    std::chrono::milliseconds request_timeout{500};
    std::chrono::milliseconds lifecycle_heartbeat_interval{250};
    std::chrono::milliseconds lifecycle_heartbeat_timeout{3000};
};

struct PartialEventStableBrokerSnapshotV2 final {
    ipc::PartialEventBrokerStateV2 state =
        ipc::PartialEventBrokerStateV2::kUnavailable;
    bool has_generation = false;
    pid_t expected_worker = -1;
    std::uint64_t lifecycle_epoch = 0U;
    std::uint64_t publication_generation = 0U;
    std::uint64_t correction_epoch = 0U;
    std::uint64_t adopted_commit_sequence = 0U;
    std::uint64_t adopted_canonical_frontier = 0U;
    std::uint64_t adopted_event_frontier = 0U;
};

// This broker owns both socket paths for its full lifetime. It deliberately
// does not spawn or restart a worker: the router supervisor must name the
// exact expected child PID before that worker may hand off a generation.
class PartialEventStableBrokerV2 final {
public:
    PartialEventStableBrokerV2(const PartialEventStableBrokerV2&) = delete;
    PartialEventStableBrokerV2& operator=(
        const PartialEventStableBrokerV2&) = delete;
    PartialEventStableBrokerV2(PartialEventStableBrokerV2&&) = delete;
    PartialEventStableBrokerV2& operator=(
        PartialEventStableBrokerV2&&) = delete;
    ~PartialEventStableBrokerV2();

    [[nodiscard]] static PartialEventStableBrokerCreateErrorV2 Create(
        PartialEventStableBrokerConfigV2 config,
        std::unique_ptr<PartialEventStableBrokerV2>* output,
        int* system_error_number = nullptr) noexcept;

    // Same-process composition path. The broker duplicates and validates the
    // descriptor before returning; the caller retains ownership of descriptor.
    // The first ordinary generation may still be INITIALIZING. Every later
    // ordinary generation must stay in the same correction epoch and extend the
    // prior Event prefix; a full correction replacement must use the explicit
    // promotion API below.
    [[nodiscard]] ipc::PartialEventBrokerResultV2 AdoptGeneration(
        int descriptor,
        const ipc::PartialOrderEventJournalSessionV2& session) noexcept;

    // Explicit supervisor promotion for correction_epoch + 1. The caller must
    // have quiesced the candidate writer after retained-input replay and local
    // catch-up. The broker additionally requires a non-stale CONTIGUOUS or
    // STOPPED_CLEAN cut with no last error, pending input, or affected channel.
    // This is a process-local barrier, not a feeder marker, writer ACK,
    // manifest barrier, or native-completeness proof.
    [[nodiscard]] ipc::PartialEventBrokerResultV2
    PromoteCorrectedGeneration(
        int descriptor,
        const ipc::PartialOrderEventJournalSessionV2& session) noexcept;

    // Marks the next exact worker PID allowed to use the private handoff
    // socket. Existing last-good readers remain valid and public attaches keep
    // receiving that mapping with RESTARTING+stale.
    [[nodiscard]] bool MarkWorkerRestarting(
        pid_t expected_worker,
        std::uint64_t* lease_epoch = nullptr) noexcept;
    // Failure hot paths use this lock-free signal. Public attach/Snapshot fold
    // it into STALE immediately; the supervisor may later call
    // MarkWorkerFailed to finalize ownership state.
    void SignalWorkerFailed(
        pid_t worker,
        std::uint64_t lease_epoch) noexcept;
    void MarkWorkerFailed(
        pid_t worker,
        std::uint64_t lease_epoch) noexcept;
    void MarkWorkerStoppedClean(
        pid_t worker,
        std::uint64_t lease_epoch) noexcept;

    [[nodiscard]] ipc::PartialEventBrokerStateV2 state() const noexcept;
    [[nodiscard]] std::uint64_t publication_generation() const noexcept;
    [[nodiscard]] std::uint64_t correction_epoch() const noexcept;
    [[nodiscard]] PartialEventStableBrokerSnapshotV2 Snapshot()
        const noexcept;

    // Idempotently stops listener threads and unlinks only the two paths this
    // instance successfully bound. Already-issued client fds remain valid.
    void Stop() noexcept;

private:
    class Impl;
    explicit PartialEventStableBrokerV2(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::apps
