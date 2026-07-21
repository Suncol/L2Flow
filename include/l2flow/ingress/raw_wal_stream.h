#pragma once

#include "l2flow/ingress/raw_segment_accumulator.h"
#include "l2flow/ingress/raw_wal_writer.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

namespace l2flow::ingress {

using RawWalStreamMonotonicNow =
    std::uint64_t (*)(void* context) noexcept;

struct RawWalStreamLimitsV1 final {
    // Logical target, excluding any fallocated zero tail. A record that would
    // cross this boundary triggers rotation before any of its bytes are
    // written.
    std::uint64_t segment_target_bytes =
        UINT64_C(4) * UINT64_C(1024) *
        UINT64_C(1024) * UINT64_C(1024);
    std::uint64_t segment_max_age_ns =
        UINT64_C(5) * UINT64_C(60) *
        UINT64_C(1'000'000'000);
    // Exact maximum framed RawRecordV1 size derived from the configured
    // callback message bound. It must fit in an otherwise empty segment.
    std::uint64_t maximum_record_bytes = 0U;
    RawWalStreamMonotonicNow monotonic_now = nullptr;
    void* monotonic_clock_context = nullptr;
};

// Secure namespace work completed by R10/R11. RawWalStreamWriter owns the
// returned I/O backend, attaches its incremental observer, and performs R12.
struct RawWalNextSegmentBootstrapV1 final {
    RawWalWriterConfig writer_config{};
    std::unique_ptr<RawWalIo> io;
};

// Persistence boundary behind the R1-R14 stream state machine. Production
// implementations bind/publish the incremental index and closed manifest in
// PublishClosedSegment(), create the typed next segment in
// CreateNextSegment(), then publish R13 and R14 separately. Each method must
// be idempotent for one exact causal input and fail closed on conflict.
class RawWalStreamBackendV1 {
public:
    virtual ~RawWalStreamBackendV1() = default;

    // R6-R9: bind the no-rescan plan to the sealed inode, publish its index,
    // publish the append-only closed manifest, and complete the directory
    // barrier.
    [[nodiscard]] virtual bool PublishClosedSegment(
        RawSegmentArtifactPlanV1 plan,
        const RawWalWriterSnapshot&
            sealed_snapshot) noexcept = 0;

    // R10-R11 only. The returned config must describe the exact normal
    // rotation plan, use kExistingJournal, and assert persisted headers.
    [[nodiscard]] virtual bool CreateNextSegment(
        const RawWalRotationPlan& rotation,
        std::uint64_t opened_monotonic_ns,
        RawWalNextSegmentBootstrapV1*
            bootstrap) noexcept = 0;

    // R13. This must complete the current RawManifestV1 file/dir barrier.
    [[nodiscard]] virtual bool PublishOpenManifest(
        const SegmentHeaderV1& segment,
        const RawWalWriterSnapshot&
            initialized_snapshot) noexcept = 0;

    // R14 for a new segment and the per-record/per-durable progress
    // publication for an already exposed segment.
    [[nodiscard]] virtual bool PublishControl(
        const SegmentHeaderV1& segment,
        const RawWalWriterSnapshot&
            snapshot) noexcept = 0;
};

// One logical stream-day sink over any number of normal Raw segments. All
// methods except Snapshot()/failure() are called by the sole writer thread.
// Rotation never splits a record and age never rotates an empty segment.
class RawWalStreamWriter final : public RawWalSink {
public:
    RawWalStreamWriter(
        RawWalWriterConfig initial_writer_config,
        std::unique_ptr<RawWalIo> initial_io,
        RawSegmentArtifactOptionsV1 artifact_options,
        RawWalStreamLimitsV1 limits,
        RawWalStreamBackendV1& backend);
    ~RawWalStreamWriter() override;

    RawWalStreamWriter(
        const RawWalStreamWriter&) = delete;
    RawWalStreamWriter& operator=(
        const RawWalStreamWriter&) = delete;
    RawWalStreamWriter(
        RawWalStreamWriter&&) = delete;
    RawWalStreamWriter& operator=(
        RawWalStreamWriter&&) = delete;

    // Initializes the already-created first segment, then publishes its open
    // manifest and control identity before it can be handed to the ring
    // consumer.
    [[nodiscard]] bool Initialize() noexcept;

    [[nodiscard]] bool AppendRecord(
        const RawWalRecordInputV1& input) noexcept override;
    [[nodiscard]] bool FlushDurable() noexcept override;
    [[nodiscard]] bool SealAndClose() noexcept override;
    [[nodiscard]] RawWalWriterSnapshot
    Snapshot() const noexcept override;
    [[nodiscard]] RawWalFailure
    failure() const noexcept override;
    [[nodiscard]] RawWalSinkIdentityV1
    identity() const noexcept override;

    [[nodiscard]] std::uint64_t rotation_count()
        const noexcept {
        return rotation_count_.load(
            std::memory_order_acquire);
    }
    [[nodiscard]] std::uint64_t current_segment_records()
        const noexcept {
        return current_segment_records_.load(
            std::memory_order_acquire);
    }

private:
    struct SegmentSession;

    [[nodiscard]] std::uint64_t MonotonicNowNs()
        const noexcept;
    [[nodiscard]] bool StartSession(
        RawWalWriterConfig config,
        std::unique_ptr<RawWalIo> io,
        std::uint64_t opened_monotonic_ns,
        std::unique_ptr<SegmentSession>*
            session) noexcept;
    [[nodiscard]] bool RotationDueBefore(
        std::uint64_t record_bytes,
        std::uint64_t now_ns,
        bool* due) noexcept;
    [[nodiscard]] bool Rotate(
        std::uint64_t now_ns) noexcept;
    [[nodiscard]] bool PublishCurrentControl() noexcept;
    [[nodiscard]] bool FinalizeCurrentSegment() noexcept;
    void Trip(
        RawWalFailureKind kind,
        int error_number) noexcept;

    RawWalWriterConfig initial_writer_config_{};
    std::unique_ptr<RawWalIo> initial_io_;
    RawSegmentArtifactOptionsV1 artifact_options_{};
    RawWalStreamLimitsV1 limits_{};
    RawWalStreamBackendV1& backend_;
    std::vector<std::unique_ptr<SegmentSession>>
        sessions_;
    SegmentSession* current_ = nullptr;
    std::atomic<RawWalWriter*> published_writer_{
        nullptr};
    std::atomic<std::uint64_t>
        current_segment_records_{0U};
    std::atomic<std::uint64_t> rotation_count_{0U};
    std::uint64_t last_monotonic_ns_ = 0U;
    bool have_clock_ = false;
    bool initialized_ = false;
    bool closed_ = false;

    std::atomic<bool> fatal_{false};
    std::atomic<std::uint8_t> failure_kind_{
        static_cast<std::uint8_t>(
            RawWalFailureKind::kNone)};
    std::atomic<int> failure_errno_{0};
};

}  // namespace l2flow::ingress
