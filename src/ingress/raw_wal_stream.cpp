#include "l2flow/ingress/raw_wal_stream.h"

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <utility>

namespace l2flow::ingress {
namespace {

[[nodiscard]] bool CheckedAdd(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* result) noexcept {
    if (result == nullptr ||
        left >
            std::numeric_limits<std::uint64_t>::max() -
                right) {
        return false;
    }
    *result = left + right;
    return true;
}

[[nodiscard]] bool RotationConfigMatches(
    const RawWalWriterConfig& config,
    const RawWalRotationPlan& rotation) noexcept {
    return config.initialization_mode ==
               RawWalInitializationMode::kExistingJournal &&
           config.headers_already_persisted &&
           config.commit_observer == nullptr &&
           config.segment_sequence ==
               rotation.next_segment_sequence &&
           config.segment_base_wal_pos ==
               rotation.next_segment_base_wal_pos &&
           config.first_ingress_sequence ==
               rotation.next_first_ingress_sequence &&
           config.initial_durable_ingress_sequence ==
               rotation.initial_durable_ingress_sequence &&
           config.existing_journal
                   .journal_append_offset ==
               rotation.existing_journal
                   .journal_append_offset &&
           config.existing_journal
                   .previous_segment_sequence ==
               rotation.existing_journal
                   .previous_segment_sequence &&
           config.existing_journal
                   .previous_sealed_cursor ==
               rotation.existing_journal
                   .previous_sealed_cursor &&
           config.existing_journal
                   .previous_marker_flags ==
               rotation.existing_journal
                   .previous_marker_flags;
}

}  // namespace

struct RawWalStreamWriter::SegmentSession final {
    RawWalWriterConfig config{};
    SegmentHeaderV1 header{};
    std::unique_ptr<RawSegmentAccumulatorV1>
        accumulator;
    std::unique_ptr<RawWalWriter> writer;
    std::uint64_t opened_monotonic_ns = 0U;
};

RawWalStreamWriter::RawWalStreamWriter(
    RawWalWriterConfig initial_writer_config,
    std::unique_ptr<RawWalIo> initial_io,
    RawSegmentArtifactOptionsV1 artifact_options,
    RawWalStreamLimitsV1 limits,
    RawWalStreamBackendV1& backend)
    : initial_writer_config_(
          std::move(initial_writer_config)),
      initial_io_(std::move(initial_io)),
      artifact_options_(std::move(artifact_options)),
      limits_(limits),
      backend_(backend) {
    std::uint64_t minimum_segment_bytes = 0U;
    if (initial_io_ == nullptr ||
        initial_writer_config_.commit_observer !=
            nullptr ||
        limits_.segment_max_age_ns == 0U ||
        limits_.maximum_record_bytes == 0U ||
        limits_.maximum_record_bytes >
            std::numeric_limits<std::uint32_t>::max() ||
        !CheckedAdd(
            kRawV1SegmentHeaderBytes,
            limits_.maximum_record_bytes,
            &minimum_segment_bytes) ||
        limits_.segment_target_bytes <
            minimum_segment_bytes ||
        artifact_options_.maximum_segment_bytes <
            limits_.segment_target_bytes) {
        throw std::invalid_argument(
            "invalid Raw WAL stream configuration");
    }
}

RawWalStreamWriter::~RawWalStreamWriter() {
    EndMutationBatch();
}

bool RawWalStreamWriter::BeginMutationBatch() noexcept {
    if (mutation_batch_open_ || !initialized_ || closed_ ||
        current_ == nullptr ||
        fatal_.load(std::memory_order_acquire)) {
        Trip(RawWalFailureKind::kInvalidState, EBUSY);
        return false;
    }
    if (!backend_.BeginMutationBatch()) {
        Trip(RawWalFailureKind::kControlPublish, EIO);
        return false;
    }
    if (!current_->writer->BeginMutationBatch()) {
        backend_.EndMutationBatch();
        const RawWalFailure failure = current_->writer->failure();
        Trip(
            failure.kind == RawWalFailureKind::kNone
                ? RawWalFailureKind::kSegmentWrite
                : failure.kind,
            failure.error_number == 0 ? EIO : failure.error_number);
        return false;
    }
    mutation_batch_open_ = true;
    return true;
}

void RawWalStreamWriter::EndMutationBatch() noexcept {
    if (!mutation_batch_open_) {
        return;
    }
    if (current_ != nullptr) {
        current_->writer->EndMutationBatch();
    }
    backend_.EndMutationBatch();
    mutation_batch_open_ = false;
}

bool RawWalStreamWriter::Initialize() noexcept {
    if (initialized_ || closed_ ||
        fatal_.load(std::memory_order_acquire)) {
        Trip(RawWalFailureKind::kInvalidState, EALREADY);
        return false;
    }
    const std::uint64_t now_ns = MonotonicNowNs();
    last_monotonic_ns_ = now_ns;
    have_clock_ = true;

    std::unique_ptr<SegmentSession> session;
    if (!StartSession(
            std::move(initial_writer_config_),
            std::move(initial_io_),
            now_ns,
            &session)) {
        return false;
    }
    SegmentSession* const candidate = session.get();
    try {
        sessions_.push_back(std::move(session));
    } catch (...) {
        Trip(RawWalFailureKind::kRotationFactory, ENOMEM);
        return false;
    }
    current_ = candidate;
    const RawWalWriterSnapshot snapshot =
        candidate->writer->Snapshot();
    if (!backend_.PublishOpenManifest(
            candidate->header, snapshot)) {
        Trip(RawWalFailureKind::kManifestPublish, EIO);
        return false;
    }
    if (!backend_.PublishControl(
            candidate->header, snapshot)) {
        Trip(RawWalFailureKind::kControlPublish, EIO);
        return false;
    }

    current_segment_records_.store(
        0U, std::memory_order_release);
    published_writer_.store(
        candidate->writer.get(),
        std::memory_order_release);
    initialized_ = true;
    return true;
}

bool RawWalStreamWriter::AppendRecord(
    const RawWalRecordInputV1& input) noexcept {
    if (!initialized_ || closed_ || current_ == nullptr ||
        fatal_.load(std::memory_order_acquire)) {
        if (!fatal_.load(std::memory_order_relaxed)) {
            Trip(RawWalFailureKind::kInvalidState, EPERM);
        }
        return false;
    }

    RawRecordLayoutV1 layout{};
    if (ComputeRawRecordLayoutV1(
            input.vendor_body.size(),
            &layout) != RawV1Error::kNone ||
        layout.record_size == 0U ||
        layout.record_size >
            limits_.maximum_record_bytes) {
        Trip(RawWalFailureKind::kRotationPolicy, EFBIG);
        return false;
    }

    const std::uint64_t now_ns = MonotonicNowNs();
    if (have_clock_ && now_ns < last_monotonic_ns_) {
        Trip(RawWalFailureKind::kRotationPolicy, ERANGE);
        return false;
    }
    last_monotonic_ns_ = now_ns;
    have_clock_ = true;

    bool rotation_due = false;
    if (!RotationDueBefore(
            layout.record_size,
            now_ns,
            &rotation_due)) {
        return false;
    }
    if (rotation_due) {
        const bool reopen_batch = mutation_batch_open_;
        if (reopen_batch) {
            EndMutationBatch();
        }
        if (!Rotate(now_ns)) {
            return false;
        }
        if (reopen_batch && !BeginMutationBatch()) {
            return false;
        }
    }
    if (current_ == nullptr ||
        !current_->writer->AppendRecord(input)) {
        if (current_ != nullptr) {
            const RawWalFailure writer_failure =
                current_->writer->failure();
            Trip(
                writer_failure.kind ==
                        RawWalFailureKind::kNone
                    ? RawWalFailureKind::kSegmentWrite
                    : writer_failure.kind,
                writer_failure.error_number == 0
                    ? EIO
                    : writer_failure.error_number);
        }
        return false;
    }

    const std::uint64_t records =
        current_segment_records_.load(
            std::memory_order_relaxed);
    if (records ==
        std::numeric_limits<std::uint64_t>::max()) {
        Trip(RawWalFailureKind::kRotationPolicy, EOVERFLOW);
        return false;
    }
    current_segment_records_.store(
        records + 1U, std::memory_order_release);
    return PublishCurrentControl();
}

bool RawWalStreamWriter::FlushDurable() noexcept {
    if (!initialized_ || closed_ || current_ == nullptr ||
        fatal_.load(std::memory_order_acquire)) {
        if (!fatal_.load(std::memory_order_relaxed)) {
            Trip(RawWalFailureKind::kInvalidState, EPERM);
        }
        return false;
    }
    if (!current_->writer->FlushDurable()) {
        const RawWalFailure writer_failure =
            current_->writer->failure();
        Trip(
            writer_failure.kind ==
                    RawWalFailureKind::kNone
                ? RawWalFailureKind::kSegmentSync
                : writer_failure.kind,
            writer_failure.error_number == 0
                ? EIO
                : writer_failure.error_number);
        return false;
    }
    return PublishCurrentControl();
}

bool RawWalStreamWriter::SealAndClose() noexcept {
    if (closed_) {
        return !fatal_.load(std::memory_order_acquire);
    }
    if (!initialized_ || current_ == nullptr ||
        fatal_.load(std::memory_order_acquire)) {
        if (!fatal_.load(std::memory_order_relaxed)) {
            Trip(RawWalFailureKind::kInvalidState, EPERM);
        }
        return false;
    }
    EndMutationBatch();
    if (!FinalizeCurrentSegment()) {
        return false;
    }
    closed_ = true;
    return true;
}

RawWalWriterSnapshot
RawWalStreamWriter::Snapshot() const noexcept {
    RawWalWriterSnapshot snapshot{};
    RawWalWriter* const writer =
        published_writer_.load(std::memory_order_acquire);
    if (writer != nullptr) {
        snapshot = writer->Snapshot();
    }
    if (fatal_.load(std::memory_order_acquire)) {
        snapshot.fatal = true;
    }
    return snapshot;
}

RawWalFailure RawWalStreamWriter::failure() const noexcept {
    if (!fatal_.load(std::memory_order_acquire)) {
        RawWalWriter* const writer =
            published_writer_.load(
                std::memory_order_acquire);
        return writer == nullptr
                   ? RawWalFailure{}
                   : writer->failure();
    }
    return {
        static_cast<RawWalFailureKind>(
            failure_kind_.load(
                std::memory_order_relaxed)),
        failure_errno_.load(std::memory_order_relaxed)};
}

RawWalSinkIdentityV1
RawWalStreamWriter::identity() const noexcept {
    RawWalWriter* const writer =
        published_writer_.load(
            std::memory_order_acquire);
    return writer == nullptr
               ? RawWalSinkIdentityV1{}
               : writer->identity();
}

std::uint64_t
RawWalStreamWriter::MonotonicNowNs() const noexcept {
    if (limits_.monotonic_now != nullptr) {
        return limits_.monotonic_now(
            limits_.monotonic_clock_context);
    }
    const auto count =
        std::chrono::duration_cast<
            std::chrono::nanoseconds>(
            std::chrono::steady_clock::now()
                .time_since_epoch())
            .count();
    return count <= 0
               ? 0U
               : static_cast<std::uint64_t>(count);
}

bool RawWalStreamWriter::StartSession(
    RawWalWriterConfig config,
    std::unique_ptr<RawWalIo> io,
    std::uint64_t opened_monotonic_ns,
    std::unique_ptr<SegmentSession>*
        session) noexcept {
    if (io == nullptr || session == nullptr ||
        config.commit_observer != nullptr) {
        Trip(RawWalFailureKind::kRotationFactory, EINVAL);
        return false;
    }
    SegmentHeaderV1 header{};
    if (DecodeSegmentHeaderV1(
            config.segment_header_wire,
            &header) != RawV1Error::kNone ||
        header.raw_schema_sha256 !=
            artifact_options_
                .expected_raw_schema_sha256) {
        Trip(RawWalFailureKind::kRotationFactory, EILSEQ);
        return false;
    }

    try {
        auto created =
            std::make_unique<SegmentSession>();
        created->config = std::move(config);
        created->header = header;
        created->opened_monotonic_ns =
            opened_monotonic_ns;
        created->accumulator =
            std::make_unique<
                RawSegmentAccumulatorV1>(
                artifact_options_);
        if (created->accumulator->error() !=
            RawSegmentAccumulatorErrorV1::kNone) {
            Trip(
                RawWalFailureKind::kRotationFactory,
                EINVAL);
            return false;
        }
        created->config.commit_observer =
            created->accumulator.get();
        created->writer =
            std::make_unique<RawWalWriter>(
                created->config, std::move(io));
        if (!created->writer->Initialize()) {
            const RawWalFailure writer_failure =
                created->writer->failure();
            Trip(
                writer_failure.kind ==
                        RawWalFailureKind::kNone
                    ? RawWalFailureKind::
                          kRotationFactory
                    : writer_failure.kind,
                writer_failure.error_number == 0
                    ? EIO
                    : writer_failure.error_number);
            return false;
        }
        const RawWalWriterSnapshot snapshot =
            created->writer->Snapshot();
        std::uint64_t expected_global_wal_pos = 0U;
        if (!snapshot.initialized ||
            snapshot.sealed ||
            snapshot.closed ||
            snapshot.fatal ||
            snapshot.append != snapshot.durable ||
            snapshot.append.segment_offset !=
                kRawV1SegmentHeaderBytes ||
            snapshot.append.ingress_sequence !=
                created->config
                    .initial_durable_ingress_sequence ||
            !CheckedAdd(
                created->header.segment_base_wal_pos,
                kRawV1SegmentHeaderBytes,
                &expected_global_wal_pos) ||
            snapshot.append.global_wal_pos !=
                expected_global_wal_pos) {
            Trip(
                RawWalFailureKind::kRotationFactory,
                EILSEQ);
            return false;
        }
        *session = std::move(created);
        return true;
    } catch (...) {
        Trip(RawWalFailureKind::kRotationFactory, ENOMEM);
        return false;
    }
}

bool RawWalStreamWriter::RotationDueBefore(
    std::uint64_t record_bytes,
    std::uint64_t now_ns,
    bool* due) noexcept {
    if (due == nullptr || current_ == nullptr ||
        record_bytes == 0U ||
        record_bytes > limits_.maximum_record_bytes ||
        now_ns < current_->opened_monotonic_ns) {
        Trip(RawWalFailureKind::kRotationPolicy, EINVAL);
        return false;
    }
    const RawWalWriterSnapshot snapshot =
        current_->writer->Snapshot();
    if (!snapshot.initialized || snapshot.sealed ||
        snapshot.closed || snapshot.fatal ||
        snapshot.append.segment_offset <
            kRawV1SegmentHeaderBytes ||
        snapshot.append.segment_offset >
            limits_.segment_target_bytes) {
        Trip(RawWalFailureKind::kRotationPolicy, EILSEQ);
        return false;
    }
    const bool bytes_due =
        record_bytes >
        limits_.segment_target_bytes -
            snapshot.append.segment_offset;
    const bool age_due =
        current_segment_records_.load(
            std::memory_order_acquire) != 0U &&
        now_ns - current_->opened_monotonic_ns >=
            limits_.segment_max_age_ns;
    *due = bytes_due || age_due;
    return true;
}

bool RawWalStreamWriter::Rotate(
    std::uint64_t now_ns) noexcept {
    if (current_ == nullptr ||
        current_segment_records_.load(
            std::memory_order_acquire) == 0U) {
        Trip(RawWalFailureKind::kRotationPolicy, EINVAL);
        return false;
    }
    if (!FinalizeCurrentSegment()) {
        return false;
    }
    const RawWalWriterSnapshot sealed_snapshot =
        current_->writer->Snapshot();
    const RawWalRotationPlan rotation =
        PlanRawWalRotation(
            current_->header, sealed_snapshot);
    if (!rotation.ok()) {
        Trip(RawWalFailureKind::kRotationPolicy, EILSEQ);
        return false;
    }

    RawWalNextSegmentBootstrapV1 bootstrap{};
    if (!backend_.CreateNextSegment(
            rotation, now_ns, &bootstrap)) {
        Trip(RawWalFailureKind::kRotationFactory, EIO);
        return false;
    }
    if (bootstrap.io == nullptr ||
        !RotationConfigMatches(
            bootstrap.writer_config, rotation)) {
        Trip(RawWalFailureKind::kRotationFactory, EILSEQ);
        return false;
    }

    std::unique_ptr<SegmentSession> next;
    if (!StartSession(
            std::move(bootstrap.writer_config),
            std::move(bootstrap.io),
            now_ns,
            &next)) {
        return false;
    }
    SegmentSession* const candidate = next.get();
    try {
        sessions_.push_back(std::move(next));
    } catch (...) {
        Trip(RawWalFailureKind::kRotationFactory, ENOMEM);
        return false;
    }
    const RawWalWriterSnapshot initialized_snapshot =
        candidate->writer->Snapshot();
    if (!backend_.PublishOpenManifest(
            candidate->header,
            initialized_snapshot)) {
        Trip(RawWalFailureKind::kManifestPublish, EIO);
        return false;
    }
    if (!backend_.PublishControl(
            candidate->header,
            initialized_snapshot)) {
        Trip(RawWalFailureKind::kControlPublish, EIO);
        return false;
    }

    current_ = candidate;
    current_segment_records_.store(
        0U, std::memory_order_release);
    rotation_count_.fetch_add(
        1U, std::memory_order_release);
    published_writer_.store(
        candidate->writer.get(),
        std::memory_order_release);
    return true;
}

bool RawWalStreamWriter::PublishCurrentControl() noexcept {
    if (current_ == nullptr) {
        Trip(RawWalFailureKind::kControlPublish, EINVAL);
        return false;
    }
    const RawWalWriterSnapshot snapshot =
        current_->writer->Snapshot();
    if (snapshot.fatal ||
        !backend_.PublishControl(
            current_->header, snapshot)) {
        if (snapshot.fatal) {
            const RawWalFailure writer_failure =
                current_->writer->failure();
            Trip(
                writer_failure.kind,
                writer_failure.error_number);
        } else {
            Trip(
                RawWalFailureKind::kControlPublish,
                EIO);
        }
        return false;
    }
    return true;
}

bool RawWalStreamWriter::FinalizeCurrentSegment() noexcept {
    if (current_ == nullptr) {
        Trip(RawWalFailureKind::kRotationArtifact, EINVAL);
        return false;
    }
    if (!current_->writer->SealAndClose()) {
        const RawWalFailure writer_failure =
            current_->writer->failure();
        Trip(
            writer_failure.kind ==
                    RawWalFailureKind::kNone
                ? RawWalFailureKind::kRotationArtifact
                : writer_failure.kind,
            writer_failure.error_number == 0
                ? EIO
                : writer_failure.error_number);
        return false;
    }
    const RawWalWriterSnapshot sealed_snapshot =
        current_->writer->Snapshot();
    RawSegmentArtifactPlanV1 artifact_plan{};
    if (!sealed_snapshot.sealed ||
        !sealed_snapshot.closed ||
        sealed_snapshot.fatal ||
        sealed_snapshot.append !=
            sealed_snapshot.durable ||
        !current_->accumulator->TakeArtifactPlan(
            &artifact_plan) ||
        !artifact_plan.ok()) {
        Trip(RawWalFailureKind::kRotationArtifact, EILSEQ);
        return false;
    }
    if (!backend_.PublishClosedSegment(
            std::move(artifact_plan),
            sealed_snapshot)) {
        Trip(RawWalFailureKind::kRotationArtifact, EIO);
        return false;
    }
    return true;
}

void RawWalStreamWriter::Trip(
    RawWalFailureKind kind,
    int error_number) noexcept {
    std::uint8_t expected =
        static_cast<std::uint8_t>(
            RawWalFailureKind::kNone);
    if (failure_kind_.compare_exchange_strong(
            expected,
            static_cast<std::uint8_t>(kind),
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        failure_errno_.store(
            error_number == 0 ? EIO : error_number,
            std::memory_order_relaxed);
        fatal_.store(true, std::memory_order_release);

        // The control page is volatile, but readers use its fatal bit to
        // stop treating a previously healthy writer as READY.  Preserve the
        // underlying writer cursor and publish a best-effort fatal overlay;
        // failure here cannot replace the first causal error and must never
        // recurse into Trip().
        if (current_ != nullptr) {
            RawWalWriterSnapshot snapshot =
                current_->writer->Snapshot();
            if (snapshot.initialized) {
                snapshot.fatal = true;
                static_cast<void>(
                    backend_.PublishControl(
                        current_->header, snapshot));
            }
        }
    }
}

}  // namespace l2flow::ingress
