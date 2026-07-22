#include "l2flow/control/control_production_controller.h"

#include "l2flow/common/identity128.h"
#include "l2flow/ingress/raw_replay.h"

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace l2flow::control {
namespace {

void SetError(std::string* error, std::string_view message) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        error->assign(message);
    } catch (...) {
    }
}

[[nodiscard]] bool SafeDirectory(const struct stat& status) noexcept {
    return S_ISDIR(status.st_mode) &&
           status.st_uid == ::geteuid() &&
           (status.st_mode & static_cast<mode_t>(07777U)) ==
               static_cast<mode_t>(0700U);
}

[[nodiscard]] int RetainCheckpointDirectory(int supplied) noexcept {
    if (supplied < 0) {
        return -1;
    }
    struct stat before {};
    if (::fstat(supplied, &before) != 0 || !SafeDirectory(before)) {
        return -2;
    }
    int retained = -1;
    do {
        retained = ::openat(
            supplied,
            ".",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC);
    } while (retained < 0 && errno == EINTR);
    struct stat after {};
    if (retained < 0 || ::fstat(retained, &after) != 0 ||
        !SafeDirectory(after) || before.st_dev != after.st_dev ||
        before.st_ino != after.st_ino) {
        if (retained >= 0) {
            static_cast<void>(::close(retained));
        }
        return -2;
    }
    return retained;
}

[[nodiscard]] std::uint64_t DefaultMonotonicNow(void*) noexcept {
    const auto now = std::chrono::steady_clock::now()
        .time_since_epoch();
    const auto ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
    return ns <= 0 ? 0U : static_cast<std::uint64_t>(ns);
}

[[nodiscard]] bool IsCommittedMalformed(
    const ControlProcessResultV1& result) noexcept {
    const bool malformed =
        result.error == ControlProcessErrorV1::kMalformedApiControl ||
        result.error == ControlProcessErrorV1::kMalformedSysControl;
    return malformed && result.cursor_committed &&
           result.control_state_poisoned &&
           result.control_record.has_value() &&
           result.control_record->control_type ==
               ControlTypeV1::kDecodeError;
}

[[nodiscard]] l2flow::ingress::RawReplaySegmentContext
ReplayContext(
    const l2flow::ingress::RawSegmentScanResult& scan) noexcept {
    l2flow::ingress::RawReplaySegmentContext context;
    context.source_stream_id = scan.segment.source_stream_id;
    context.capture_date = scan.segment.capture_date;
    context.stream_day_id = scan.segment.stream_day_id;
    context.segment_sequence = scan.segment.segment_sequence;
    context.segment_base_wal_pos = scan.segment.segment_base_wal_pos;
    context.config_sha256 = scan.segment.config_sha256;
    context.raw_schema_sha256 = scan.segment.raw_schema_sha256;
    context.clock_epoch.algorithm = scan.segment.clock_epoch_algorithm;
    context.clock_epoch.digest = scan.segment.clock_epoch_digest;
    context.clock_epoch.label = scan.segment.clock_epoch_label;
    return context;
}

[[nodiscard]] std::optional<l2flow::ingress::RawReplayRecord>
FindBoundaryRecord(
    const l2flow::ingress::RawProductionReplaySnapshotV1& replay,
    const ControlDecoderSnapshotV1& checkpoint) {
    for (const l2flow::ingress::RawSegmentScanResult& scan : replay.scans) {
        for (const l2flow::ingress::RawRecordView& record : scan.records) {
            if (record.header().ingress_sequence ==
                    checkpoint.processed_ingress_sequence &&
                record.record_start_wal_pos() ==
                    checkpoint.processed_record_start_wal_pos &&
                record.record_end_wal_pos() ==
                    checkpoint.processed_record_end_wal_pos) {
                return l2flow::ingress::RawReplayRecord{
                    record,
                    ReplayContext(scan),
                    l2flow::ingress::RawReplayProvenance::kDurable};
            }
        }
    }
    return std::nullopt;
}

[[nodiscard]] bool SameTerminalControl(
    const l2flow::ingress::RawControlSnapshot& control,
    const l2flow::ingress::RawReadinessStopCursorV1& cursor) noexcept {
    return control.fatal_state == 0U &&
           control.writer_instance == cursor.writer_instance &&
           control.stream_day_id == cursor.stream_day_id &&
           control.source_stream_id == cursor.source_stream_id &&
           control.capture_date == cursor.capture_date &&
           control.segment_sequence == cursor.segment_sequence &&
           control.append_global_wal_pos == cursor.global_wal_pos &&
           control.append_ingress_sequence == cursor.ingress_sequence &&
           control.append_segment_offset == cursor.segment_offset &&
           control.durable_global_wal_pos == cursor.global_wal_pos &&
           control.durable_ingress_sequence == cursor.ingress_sequence &&
           control.durable_segment_offset == cursor.segment_offset;
}

}  // namespace

ControlProductionControllerV1::ControlProductionControllerV1(
    ControlProductionControllerConfigV1 config,
    std::unique_ptr<
        l2flow::ingress::RawProductionAuthoritativeRuntimeV1> runtime,
    int checkpoint_directory_fd) noexcept
    : runtime_(std::move(runtime)),
      config_(std::move(config)),
      delegated_failure_callback_(config_.worker.failure_callback),
      delegated_failure_context_(config_.worker.failure_context),
      checkpoint_directory_fd_(checkpoint_directory_fd) {
    config_.worker.failure_callback =
        &ControlProductionControllerV1::WorkerFailure;
    config_.worker.failure_context = this;
    snapshot_.checkpoint_enabled = checkpoint_directory_fd_ >= 0;
}

ControlProductionControllerV1::~ControlProductionControllerV1() {
    RequestFailureSupervisorExit();
    JoinFailureSupervisor();
    if (runtime_ != nullptr &&
        (runtime_->app_state() ==
             l2flow::ingress::RawIngressAppState::kRunning ||
         runtime_->app_state() ==
             l2flow::ingress::RawIngressAppState::kInitializing)) {
        static_cast<void>(runtime_->Stop(nullptr));
    }
    if (worker_thread_.joinable() ||
        state_.load(std::memory_order_acquire) !=
            ControlProductionControllerStateV1::kStopped) {
        AbortAndJoin();
    }
    if (checkpoint_directory_fd_ >= 0) {
        static_cast<void>(::close(checkpoint_directory_fd_));
    }
}

ControlProductionControllerCreateErrorV1
ControlProductionControllerV1::Create(
    ControlProductionControllerConfigV1 config,
    std::unique_ptr<
        l2flow::ingress::RawProductionAuthoritativeRuntimeV1> runtime,
    std::unique_ptr<ControlRecordSinkV1> record_sink,
    std::unique_ptr<ControlProductionControllerV1>* output,
    std::string* error) noexcept {
    if (output == nullptr) {
        return ControlProductionControllerCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (runtime == nullptr || record_sink == nullptr) {
        SetError(error, "production controller dependency is null");
        return ControlProductionControllerCreateErrorV1::kNullDependency;
    }
    if (l2flow::common::IsZeroIdentity(config.worker.writer_instance) ||
        config.worker.connect_generation == 0U ||
        config.worker.connect_generation != runtime->connect_generation() ||
        config.worker.record_publish_timeout_ns == 0U ||
        config.worker.final_catch_up_timeout_ns == 0U ||
        config.startup_timeout_ns == 0U ||
        config.decoder.source_stream_id == 0U ||
        config.decoder.capture_date == 0U ||
        l2flow::common::IsZeroIdentity(config.decoder.stream_day_id)) {
        SetError(error, "production controller configuration is invalid");
        return ControlProductionControllerCreateErrorV1::kInvalidConfig;
    }

    const int checkpoint_fd =
        RetainCheckpointDirectory(config.checkpoint_directory_fd);
    if (checkpoint_fd == -2) {
        SetError(error, "checkpoint directory is unsafe");
        return ControlProductionControllerCreateErrorV1::
            kCheckpointDirectoryUnsafe;
    }
    try {
        auto controller =
            std::unique_ptr<ControlProductionControllerV1>(
                new ControlProductionControllerV1(
                    std::move(config),
                    std::move(runtime),
                    checkpoint_fd));
        const ControlProductionControllerCreateErrorV1 prepared =
            controller->Prepare(std::move(record_sink), error);
        if (prepared != ControlProductionControllerCreateErrorV1::kNone) {
            return prepared;
        }
        *output = std::move(controller);
        SetError(error, {});
        return ControlProductionControllerCreateErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        if (checkpoint_fd >= 0) {
            static_cast<void>(::close(checkpoint_fd));
        }
        return ControlProductionControllerCreateErrorV1::kAllocationFailure;
    } catch (...) {
        if (checkpoint_fd >= 0) {
            static_cast<void>(::close(checkpoint_fd));
        }
        return ControlProductionControllerCreateErrorV1::kInvalidConfig;
    }
}

ControlProductionControllerCreateErrorV1
ControlProductionControllerV1::Prepare(
    std::unique_ptr<ControlRecordSinkV1> record_sink,
    std::string* error) noexcept {
    try {
        l2flow::ingress::RawProductionReplaySnapshotResultV1 replay_result =
            runtime_->PrepareAuthoritativeReplay(config_.replay_limits);
        {
            std::lock_guard<std::mutex> lock(snapshot_mutex_);
            snapshot_.replay_snapshot_error = replay_result.error;
        }
        if (!replay_result.ok()) {
            SetCreateFailure(
                ControlProductionControllerCreateErrorV1::kReplaySnapshot);
            SetError(error, "authoritative Raw replay snapshot failed");
            return ControlProductionControllerCreateErrorV1::kReplaySnapshot;
        }
        l2flow::ingress::RawProductionReplaySnapshotV1 replay =
            std::move(replay_result.snapshot);
        if (replay.live_attach.writer_instance !=
                config_.worker.writer_instance ||
            replay.live_attach.source_stream_id !=
                config_.decoder.source_stream_id ||
            replay.live_attach.capture_date != config_.decoder.capture_date ||
            replay.live_attach.stream_day_id !=
                config_.decoder.stream_day_id) {
            SetCreateFailure(
                ControlProductionControllerCreateErrorV1::kInvalidConfig);
            SetError(error, "controller namespace does not match Raw replay");
            return ControlProductionControllerCreateErrorV1::kInvalidConfig;
        }

        std::unique_ptr<ControlDecoderV1> decoder;
        std::optional<std::uint64_t> suffix_begin;
        if (checkpoint_directory_fd_ >= 0) {
            ControlCheckpointPosixLoadResultV1 loaded =
                LoadLatestControlCheckpointV1At(
                    checkpoint_directory_fd_, replay.control, error);
            {
                std::lock_guard<std::mutex> lock(snapshot_mutex_);
                snapshot_.checkpoint_load_error = loaded.error;
            }
            if (loaded.error ==
                ControlCheckpointPosixStoreErrorV1::kNoEligibleCheckpoint) {
                // Checkpoints are optional acceleration; full Raw replay is
                // the authority-preserving fallback.
            } else if (!loaded.ok()) {
                SetCreateFailure(
                    ControlProductionControllerCreateErrorV1::
                        kCheckpointDiscovery);
                SetError(error, "checkpoint discovery failed closed");
                return ControlProductionControllerCreateErrorV1::
                    kCheckpointDiscovery;
            } else {
                {
                    std::lock_guard<std::mutex> lock(snapshot_mutex_);
                    snapshot_.checkpoint_loaded = true;
                }
                const ControlDecoderSnapshotV1& checkpoint_state =
                    loaded.checkpoint->state;
                const std::optional<l2flow::ingress::RawReplayRecord>
                    boundary = FindBoundaryRecord(replay, checkpoint_state);
                if (!boundary.has_value()) {
                    SetCreateFailure(
                        ControlProductionControllerCreateErrorV1::
                            kCheckpointBoundaryMissing);
                    SetError(error,
                        "checkpoint boundary is absent from validated Raw");
                    return ControlProductionControllerCreateErrorV1::
                        kCheckpointBoundaryMissing;
                }
                const ControlDecoderCreateErrorV1 restore =
                    ControlDecoderV1::Restore(
                        config_.decoder,
                        *loaded.checkpoint,
                        *boundary,
                        replay.control,
                        &decoder);
                {
                    std::lock_guard<std::mutex> lock(snapshot_mutex_);
                    snapshot_.checkpoint_restore_error = restore;
                }
                if (restore == ControlDecoderCreateErrorV1::kNone &&
                    decoder != nullptr) {
                    if (checkpoint_state.processed_ingress_sequence ==
                        std::numeric_limits<std::uint64_t>::max()) {
                        SetCreateFailure(
                            ControlProductionControllerCreateErrorV1::
                                kDecoderCreate);
                        return ControlProductionControllerCreateErrorV1::
                            kDecoderCreate;
                    }
                    suffix_begin =
                        checkpoint_state.processed_ingress_sequence + 1U;
                    std::lock_guard<std::mutex> lock(snapshot_mutex_);
                    snapshot_.checkpoint_restored = true;
                } else {
                    decoder.reset();
                    std::lock_guard<std::mutex> lock(snapshot_mutex_);
                    snapshot_.checkpoint_rejected_to_full_replay = true;
                }
            }
        }

        if (decoder == nullptr) {
            const ControlDecoderCreateErrorV1 create =
                ControlDecoderV1::Create(config_.decoder, &decoder);
            if (create != ControlDecoderCreateErrorV1::kNone ||
                decoder == nullptr) {
                SetCreateFailure(
                    ControlProductionControllerCreateErrorV1::kDecoderCreate);
                SetError(error, "control decoder creation failed");
                return ControlProductionControllerCreateErrorV1::
                    kDecoderCreate;
            }
            suffix_begin.reset();
        }

        if (!ReplayIntoDecoder(
                replay,
                suffix_begin,
                *decoder,
                *record_sink,
                error)) {
            ControlProductionControllerCreateErrorV1 failure;
            {
                std::lock_guard<std::mutex> lock(snapshot_mutex_);
                failure = snapshot_.create_error;
            }
            return failure;
        }

        const ControlDecoderSnapshotV1 final_decoder = decoder->Snapshot();
        if (final_decoder.next_ingress_sequence !=
            replay.live_attach.next_ingress_sequence) {
            SetCreateFailure(
                ControlProductionControllerCreateErrorV1::kReplayFailure);
            SetError(error, "decoder did not reach exact live frontier");
            return ControlProductionControllerCreateErrorV1::kReplayFailure;
        }
        std::unique_ptr<ControlLiveWorkerReplayProofV1> proof;
        const ControlLiveWorkerReplayProofErrorV1 proof_error =
            ControlLiveWorkerReplayProofV1::Create(
                config_.worker.writer_instance,
                replay.scans,
                replay.live_attach,
                final_decoder,
                &proof);
        if (proof_error != ControlLiveWorkerReplayProofErrorV1::kNone ||
            proof == nullptr) {
            SetCreateFailure(
                ControlProductionControllerCreateErrorV1::kReplayProof);
            SetError(error, "validated replay proof construction failed");
            return ControlProductionControllerCreateErrorV1::kReplayProof;
        }

        std::unique_ptr<l2flow::ingress::RawLiveTail> tail;
        const l2flow::ingress::RawLiveTailError attach =
            runtime_->AttachAuthoritativeTail(replay.live_attach, &tail);
        if (attach != l2flow::ingress::RawLiveTailError::kNone ||
            tail == nullptr) {
            SetCreateFailure(
                ControlProductionControllerCreateErrorV1::kLiveTailAttach);
            SetError(error, "authoritative live tail attach failed");
            return ControlProductionControllerCreateErrorV1::kLiveTailAttach;
        }
        const ControlLiveWorkerCreateErrorV1 worker_error =
            ControlLiveWorkerV1::CreateWithReplayProof(
                config_.worker,
                *proof,
                std::move(tail),
                std::move(decoder),
                std::move(record_sink),
                &worker_);
        if (worker_error != ControlLiveWorkerCreateErrorV1::kNone ||
            worker_ == nullptr) {
            SetCreateFailure(
                ControlProductionControllerCreateErrorV1::kWorkerCreate);
            SetError(error, "control live worker creation failed");
            return ControlProductionControllerCreateErrorV1::kWorkerCreate;
        }
        if (!runtime_->InstallExternalTailConsumer(this, error)) {
            SetCreateFailure(
                ControlProductionControllerCreateErrorV1::kConsumerInstall);
            return ControlProductionControllerCreateErrorV1::kConsumerInstall;
        }
        state_.store(
            ControlProductionControllerStateV1::kPrepared,
            std::memory_order_release);
        return ControlProductionControllerCreateErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        SetCreateFailure(
            ControlProductionControllerCreateErrorV1::kAllocationFailure);
        return ControlProductionControllerCreateErrorV1::kAllocationFailure;
    } catch (...) {
        SetCreateFailure(
            ControlProductionControllerCreateErrorV1::kReplayFailure);
        return ControlProductionControllerCreateErrorV1::kReplayFailure;
    }
}

bool ControlProductionControllerV1::ReplayIntoDecoder(
    const l2flow::ingress::RawProductionReplaySnapshotV1& replay,
    std::optional<std::uint64_t> begin_ingress_sequence,
    ControlDecoderV1& decoder,
    ControlRecordSinkV1& sink,
    std::string* error) noexcept {
    try {
        std::vector<l2flow::ingress::RawReplaySegmentInput> inputs;
        inputs.reserve(replay.scans.size());
        for (const l2flow::ingress::RawSegmentScanResult& scan :
             replay.scans) {
            inputs.push_back({
                &scan,
                l2flow::ingress::RawReplayScanExtent::kDurableOnly,
                scan.validated_end_wal_pos});
        }
        l2flow::ingress::RawReplayFilter filter;
        filter.source_stream_ids = {replay.control.source_stream_id};
        filter.capture_dates = {replay.control.capture_date};
        filter.ingress_sequence.begin = begin_ingress_sequence;
        l2flow::ingress::RawReplayRunSettings settings;
        settings.pace = l2flow::ingress::RawReplayPace::kAsFastAsPossible;
        std::unique_ptr<l2flow::ingress::RawReplayEngine> engine;
        const l2flow::ingress::RawReplayError create =
            l2flow::ingress::RawReplayEngine::Create(
                inputs,
                filter,
                settings,
                nullptr,
                nullptr,
                &engine);
        if (create != l2flow::ingress::RawReplayError::kNone ||
            engine == nullptr) {
            SetCreateFailure(
                ControlProductionControllerCreateErrorV1::kReplayCreate);
            SetError(error, "Raw replay engine creation failed");
            return false;
        }

        for (;;) {
            l2flow::ingress::RawReplayStep step = engine->Next();
            if (step.kind == l2flow::ingress::RawReplayStepKind::kEnd) {
                return true;
            }
            if (step.kind ==
                    l2flow::ingress::RawReplayStepKind::kClockEpochBoundary ||
                step.kind ==
                    l2flow::ingress::RawReplayStepKind::kMonotonicRegression) {
                continue;
            }
            if (step.kind != l2flow::ingress::RawReplayStepKind::kRecord ||
                !step.record.has_value()) {
                SetCreateFailure(
                    ControlProductionControllerCreateErrorV1::kReplayFailure);
                SetError(error, "Raw replay engine failed");
                return false;
            }
            ControlProcessResultV1 processed =
                decoder.Process(*step.record);
            const bool malformed = IsCommittedMalformed(processed);
            if (!processed.ok() && !malformed) {
                SetCreateFailure(
                    ControlProductionControllerCreateErrorV1::kReplayFailure);
                SetError(error, "control decoder rejected Raw replay");
                return false;
            }
            {
                std::lock_guard<std::mutex> lock(snapshot_mutex_);
                ++snapshot_.replayed_records;
            }
            if (!processed.control_record.has_value()) {
                continue;
            }
            ControlRecordWireV1 wire{};
            if (EncodeControlRecordV1(*processed.control_record, &wire) !=
                ControlRecordV1Error::kNone) {
                SetCreateFailure(
                    ControlProductionControllerCreateErrorV1::
                        kDerivedSinkFailure);
                return false;
            }
            const std::uint64_t now = MonotonicNowNs();
            if (now == 0U ||
                now > std::numeric_limits<std::uint64_t>::max() -
                    config_.worker.record_publish_timeout_ns) {
                SetCreateFailure(
                    ControlProductionControllerCreateErrorV1::
                        kDerivedSinkFailure);
                return false;
            }
            const std::uint64_t deadline =
                now + config_.worker.record_publish_timeout_ns;
            const ControlRecordPublishResultV1 published = sink.Publish(
                *processed.control_record, wire, deadline);
            const std::uint64_t finished = MonotonicNowNs();
            if ((published != ControlRecordPublishResultV1::kPublishedNew &&
                 published !=
                     ControlRecordPublishResultV1::kAcceptedIdentical) ||
                finished == 0U || finished < now || finished > deadline) {
                SetCreateFailure(
                    ControlProductionControllerCreateErrorV1::
                        kDerivedSinkFailure);
                SetError(error, "durable derived replay publication failed");
                return false;
            }
            std::lock_guard<std::mutex> lock(snapshot_mutex_);
            ++snapshot_.replayed_control_records;
        }
    } catch (...) {
        SetCreateFailure(
            ControlProductionControllerCreateErrorV1::kReplayFailure);
        return false;
    }
}

bool ControlProductionControllerV1::Initialize(
    std::string* error) noexcept {
    if (runtime_ == nullptr ||
        state_.load(std::memory_order_acquire) !=
            ControlProductionControllerStateV1::kPrepared) {
        SetError(error, "production controller is not prepared");
        return false;
    }
    const bool initialized = runtime_->Initialize(error);
    if (!initialized) {
        RequestFailureSupervisorExit();
        JoinFailureSupervisor();
    }
    return initialized;
}

bool ControlProductionControllerV1::Stop(std::string* error) noexcept {
    if (runtime_ == nullptr) {
        SetError(error, "production runtime is absent");
        return false;
    }
    const bool stopped = runtime_->Stop(error);
    RequestFailureSupervisorExit();
    JoinFailureSupervisor();
    return stopped;
}

bool ControlProductionControllerV1::StartBeforeConnect(
    std::string* error) noexcept {
    ControlProductionControllerStateV1 expected =
        ControlProductionControllerStateV1::kPrepared;
    if (!state_.compare_exchange_strong(
            expected,
            ControlProductionControllerStateV1::kStarting,
            std::memory_order_acq_rel,
            std::memory_order_acquire) ||
        worker_ == nullptr || worker_thread_.joinable()) {
        SetError(error, "control worker cannot start");
        return false;
    }
    const std::uint64_t started = MonotonicNowNs();
    if (started == 0U) {
        state_.store(
            ControlProductionControllerStateV1::kFailed,
            std::memory_order_release);
        SetError(error, "controller monotonic clock is invalid");
        return false;
    }
    if (!StartFailureSupervisor()) {
        state_.store(
            ControlProductionControllerStateV1::kFailed,
            std::memory_order_release);
        SetError(error, "failure supervisor thread creation failed");
        return false;
    }
    try {
        worker_thread_ = std::thread([this]() noexcept {
            const bool result = worker_->Run();
            worker_thread_result_.store(result, std::memory_order_relaxed);
            worker_thread_result_known_.store(true, std::memory_order_release);
        });
    } catch (...) {
        RequestFailureSupervisorExit();
        state_.store(
            ControlProductionControllerStateV1::kFailed,
            std::memory_order_release);
        SetError(error, "control worker thread creation failed");
        return false;
    }

    for (;;) {
        const ControlLiveWorkerSnapshotV1 worker = worker_->Snapshot();
        if (worker.startup_complete) {
            if (worker.startup_succeeded && worker.decoder_healthy &&
                worker.failure == ControlLiveWorkerFailureV1::kNone) {
                state_.store(
                    ControlProductionControllerStateV1::kRunning,
                    std::memory_order_release);
                SetError(error, {});
                return true;
            }
            AbortAndJoin();
            SetError(error, "control worker startup failed");
            return false;
        }
        const std::uint64_t now = MonotonicNowNs();
        if (now == 0U || now < started ||
            now - started >= config_.startup_timeout_ns) {
            AbortAndJoin();
            SetError(error, "control worker startup timed out");
            return false;
        }
        std::this_thread::yield();
    }
}

bool ControlProductionControllerV1::StopAtAndJoin(
    const l2flow::ingress::RawReadinessStopCursorV1& final_cursor,
    l2flow::ingress::RawIngressTailConsumerEvidenceV1* evidence,
    std::string* error) noexcept {
    if (evidence == nullptr || worker_ == nullptr) {
        SetError(error, "terminal control evidence output is null");
        AbortAndJoin();
        return false;
    }
    ControlProductionControllerStateV1 expected =
        ControlProductionControllerStateV1::kRunning;
    if (!state_.compare_exchange_strong(
            expected,
            ControlProductionControllerStateV1::kStopping,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        AbortAndJoin();
        SetError(error, "controller was not healthy at normal stop");
        return false;
    }
    ControlLiveStopCursorV1 target;
    target.writer_instance = final_cursor.writer_instance;
    target.stream_day_id = final_cursor.stream_day_id;
    target.source_stream_id = final_cursor.source_stream_id;
    target.capture_date = final_cursor.capture_date;
    target.segment_sequence = final_cursor.segment_sequence;
    target.global_wal_pos = final_cursor.global_wal_pos;
    target.ingress_sequence = final_cursor.ingress_sequence;
    target.segment_offset = final_cursor.segment_offset;
    const bool armed = worker_->StopAt(target);
    if (!armed) {
        worker_->Abort();
    }
    JoinWorker();
    const ControlLiveWorkerSnapshotV1 terminal = worker_->Snapshot();
    const bool exact =
        armed &&
        worker_thread_result_known_.load(std::memory_order_acquire) &&
        worker_thread_result_.load(std::memory_order_relaxed) &&
        terminal.finished && terminal.startup_succeeded &&
        terminal.decoder_healthy &&
        terminal.failure == ControlLiveWorkerFailureV1::kNone &&
        terminal.processed_segment_sequence ==
            final_cursor.segment_sequence &&
        terminal.processed_wal_pos == final_cursor.global_wal_pos &&
        terminal.processed_ingress_sequence ==
            final_cursor.ingress_sequence &&
        terminal.processed_segment_offset == final_cursor.segment_offset;
    if (!exact) {
        state_.store(
            ControlProductionControllerStateV1::kFailed,
            std::memory_order_release);
        SetError(error, "control worker terminal cursor is not exact");
        return false;
    }

    PublishFinalCheckpoint(final_cursor);
    evidence->kind =
        l2flow::ingress::RawIngressTailConsumerKindV1::
            kPhase3AuthoritativeControl;
    evidence->writer_instance = final_cursor.writer_instance;
    evidence->stream_day_id = final_cursor.stream_day_id;
    evidence->source_stream_id = final_cursor.source_stream_id;
    evidence->capture_date = final_cursor.capture_date;
    evidence->processed_segment_sequence = final_cursor.segment_sequence;
    evidence->processed_global_wal_pos = final_cursor.global_wal_pos;
    evidence->processed_ingress_sequence = final_cursor.ingress_sequence;
    evidence->processed_segment_offset = final_cursor.segment_offset;
    evidence->generation_active = true;
    evidence->healthy = true;
    evidence->authoritative_control = true;
    state_.store(
        ControlProductionControllerStateV1::kStopped,
        std::memory_order_release);
    RequestFailureSupervisorExit();
    SetError(error, {});
    return true;
}

void ControlProductionControllerV1::AbortAndJoin() noexcept {
    const ControlProductionControllerStateV1 current =
        state_.load(std::memory_order_acquire);
    if (current == ControlProductionControllerStateV1::kStopped) {
        JoinWorker();
        return;
    }
    {
        std::lock_guard<std::mutex> lock(failure_supervisor_mutex_);
        // AbortAndJoin is invoked by the Raw runtime's lifecycle owner for
        // startup/Connect/abnormal teardown. The runtime is already stopping;
        // converting this deliberate worker abort back into a supervisor
        // runtime_->Stop() request would create a redundant concurrent
        // lifecycle call against an interface that does not promise it.
        failure_stop_suppressed_ = true;
    }
    if (worker_ != nullptr) {
        worker_->Abort();
    }
    JoinWorker();
    state_.store(
        ControlProductionControllerStateV1::kFailed,
        std::memory_order_release);
    RequestFailureSupervisorExit();
}

void ControlProductionControllerV1::JoinWorker() noexcept {
    if (worker_thread_.joinable()) {
        try {
            worker_thread_.join();
        } catch (...) {
            std::terminate();
        }
    }
}

bool ControlProductionControllerV1::StartFailureSupervisor() noexcept {
    std::lock_guard<std::mutex> lock(failure_supervisor_mutex_);
    if (failure_supervisor_thread_.joinable()) {
        return false;
    }
    failure_stop_requested_ = false;
    failure_stop_suppressed_ = false;
    failure_supervisor_exit_requested_ = false;
    try {
        failure_supervisor_thread_ = std::thread([this]() noexcept {
            FailureSupervisorRun();
        });
        return true;
    } catch (...) {
        return false;
    }
}

void ControlProductionControllerV1::FailureSupervisorRun() noexcept {
    bool stop_generation = false;
    {
        std::unique_lock<std::mutex> lock(failure_supervisor_mutex_);
        failure_supervisor_cv_.wait(lock, [this]() noexcept {
            return failure_stop_requested_ ||
                   failure_supervisor_exit_requested_;
        });
        // A fatal request wins over a concurrent destructor/normal-stop exit
        // request. This ensures READY revocation is followed by an actual SDK
        // generation stop rather than silently abandoning a live runtime.
        stop_generation = failure_stop_requested_;
    }
    if (stop_generation && runtime_ != nullptr) {
        static_cast<void>(runtime_->Stop(nullptr));
    }
}

void ControlProductionControllerV1::RequestFailureSupervisorExit()
    noexcept {
    {
        std::lock_guard<std::mutex> lock(failure_supervisor_mutex_);
        failure_supervisor_exit_requested_ = true;
    }
    failure_supervisor_cv_.notify_all();
}

void ControlProductionControllerV1::JoinFailureSupervisor() noexcept {
    if (failure_supervisor_thread_.joinable()) {
        if (failure_supervisor_thread_.get_id() ==
            std::this_thread::get_id()) {
            return;
        }
        try {
            failure_supervisor_thread_.join();
        } catch (...) {
            std::terminate();
        }
    }
}

bool ControlProductionControllerV1::Healthy() const noexcept {
    if (state_.load(std::memory_order_acquire) !=
            ControlProductionControllerStateV1::kRunning ||
        worker_ == nullptr) {
        return false;
    }
    const ControlLiveWorkerSnapshotV1 worker = worker_->Snapshot();
    return worker.startup_complete && worker.startup_succeeded &&
           worker.decoder_healthy && !worker.finished &&
           worker.failure == ControlLiveWorkerFailureV1::kNone;
}

ControlProductionReadinessSampleV1
ControlProductionControllerV1::EvaluateReadiness(
    const ControlReadinessGateConfigV1& config,
    const std::optional<MarketSilenceProofV1>& calendar_proof,
    std::uint64_t now_monotonic_ns,
    std::uint64_t sampled_realtime_ns) const noexcept {
    ControlProductionReadinessSampleV1 result;
    result.controller_state = state_.load(std::memory_order_acquire);
    result.ingress_state = runtime_ == nullptr
        ? l2flow::ingress::RawIngressAppState::kStopped
        : runtime_->app_state();
    result.capture_pipeline_healthy =
        runtime_ != nullptr &&
        result.ingress_state ==
            l2flow::ingress::RawIngressAppState::kRunning &&
        !runtime_->app_fatal() && Healthy();
    if (worker_ != nullptr) {
        result.gate = worker_->EvaluateReadiness(
            config,
            calendar_proof,
            now_monotonic_ns,
            sampled_realtime_ns,
            result.capture_pipeline_healthy);
    }
    return result;
}

void ControlProductionControllerV1::PublishFinalCheckpoint(
    const l2flow::ingress::RawReadinessStopCursorV1& final_cursor) noexcept {
    if (checkpoint_directory_fd_ < 0 || worker_ == nullptr) {
        return;
    }
    const std::optional<ControlDecoderCheckpointV1> checkpoint =
        worker_->Checkpoint();
    if (!checkpoint.has_value()) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(snapshot_mutex_);
        snapshot_.checkpoint_publication_attempted = true;
    }
    const l2flow::ingress::RawProductionControlSampleV1 control =
        runtime_->SampleControlFresh();
    if (!control.ok() ||
        !SameTerminalControl(control.snapshot, final_cursor)) {
        std::lock_guard<std::mutex> lock(snapshot_mutex_);
        snapshot_.checkpoint_publish_error =
            ControlCheckpointPosixStoreErrorV1::kInvalidRawFrontier;
        return;
    }
    ControlCheckpointPosixPublishResultV1 published =
        PublishControlCheckpointV1At(
            checkpoint_directory_fd_,
            *checkpoint,
            control.snapshot,
            nullptr);
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    snapshot_.checkpoint_publish_error = published.error;
    snapshot_.checkpoint_disposition = published.disposition;
    snapshot_.checkpoint_published = published.ok();
}

std::uint64_t ControlProductionControllerV1::MonotonicNowNs()
    const noexcept {
    const ControlLiveWorkerMonotonicNow now =
        config_.worker.monotonic_now == nullptr
            ? &DefaultMonotonicNow
            : config_.worker.monotonic_now;
    return now(config_.worker.monotonic_clock_context);
}

void ControlProductionControllerV1::WorkerFailure(
    void* context,
    ControlLiveWorkerFailureV1 failure) noexcept {
    if (context != nullptr) {
        static_cast<ControlProductionControllerV1*>(context)
            ->OnWorkerFailure(failure);
    }
}

void ControlProductionControllerV1::OnWorkerFailure(
    ControlLiveWorkerFailureV1 failure) noexcept {
    {
        std::lock_guard<std::mutex> lock(snapshot_mutex_);
        snapshot_.worker_failure = failure;
    }
    const ControlProductionControllerStateV1 previous = state_.exchange(
        ControlProductionControllerStateV1::kFailed,
        std::memory_order_acq_rel);
    {
        std::lock_guard<std::mutex> lock(failure_supervisor_mutex_);
        // StartBeforeConnect failures are synchronously observed by
        // RawIngressApp::Initialize, while failures during exact terminal
        // catch-up occur after SDK shutdown has already begun. Only an
        // unplanned failure of a fully running controller needs the independent
        // supervisor to initiate SDK-generation shutdown.
        if (previous == ControlProductionControllerStateV1::kRunning &&
            !failure_stop_suppressed_) {
            failure_stop_requested_ = true;
        }
    }
    failure_supervisor_cv_.notify_all();
    if (delegated_failure_callback_ != nullptr) {
        delegated_failure_callback_(
            delegated_failure_context_, failure);
    }
}

void ControlProductionControllerV1::SetCreateFailure(
    ControlProductionControllerCreateErrorV1 failure) noexcept {
    {
        std::lock_guard<std::mutex> lock(snapshot_mutex_);
        snapshot_.create_error = failure;
    }
    state_.store(
        ControlProductionControllerStateV1::kFailed,
        std::memory_order_release);
}

ControlProductionControllerSnapshotV1
ControlProductionControllerV1::Snapshot() const noexcept {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    ControlProductionControllerSnapshotV1 result = snapshot_;
    result.state = state_.load(std::memory_order_acquire);
    result.worker_thread_result_known =
        worker_thread_result_known_.load(std::memory_order_acquire);
    result.worker_thread_result =
        worker_thread_result_.load(std::memory_order_relaxed);
    if (worker_ != nullptr) {
        const ControlLiveWorkerSnapshotV1 worker = worker_->Snapshot();
        result.worker_failure = worker.failure;
        result.worker_process_error = worker.process_error;
        result.worker_live_tail_error = worker.live_tail_error;
    }
    return result;
}

ControlDecoderSnapshotV1
ControlProductionControllerV1::DecoderSnapshot() const {
    return worker_ == nullptr
        ? ControlDecoderSnapshotV1{}
        : worker_->DecoderSnapshot();
}

}  // namespace l2flow::control
