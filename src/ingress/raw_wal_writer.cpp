#include "l2flow/ingress/raw_wal_writer.h"

#include "l2flow/ingress/raw_v1.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <utility>
#include <vector>

namespace l2flow::ingress {
namespace {

constexpr std::size_t kMaximumIoVectors = 8U;

bool CheckedAdd(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* result) noexcept {
    if (result == nullptr ||
        left > std::numeric_limits<std::uint64_t>::max() - right) {
        return false;
    }
    *result = left + right;
    return true;
}

bool IsDefaultExistingJournalInit(
    const RawWalExistingJournalInit& initialization) noexcept {
    return initialization.journal_append_offset == 0U &&
           initialization.previous_segment_sequence == 0U &&
           initialization.previous_sealed_cursor ==
               RawWalCursor{} &&
           initialization.previous_marker_flags == 0U;
}

bool IsDefaultRecoveredOpenInit(
    const RawWalRecoveredOpenInit& initialization) noexcept {
    return initialization.journal_append_offset == 0U &&
           initialization.recovered_cursor == RawWalCursor{} &&
           initialization.accepted_marker_flags == 0U;
}

// These three format-specific adapters are defined after the Raw V1 codec is
// frozen. Keeping all writer state transitions below expressed only in terms
// of validated wire bytes makes the ordering proof independent of the codec's
// in-memory representation.
bool ValidateBootstrapWire(
    const RawWalWriterConfig& config) noexcept;

bool EncodeRecordWire(
    const RawWalRecordInputV1& input,
    std::vector<std::byte>* wire) noexcept;

bool EncodeMarkerWire(
    const RawWalWriterConfig& config,
    std::uint64_t durable_global_wal_pos,
    std::uint64_t durable_ingress_sequence,
    std::uint64_t durable_segment_offset,
    std::uint32_t marker_flags,
    std::array<std::byte, kRawWalDurableMarkerBytes>* wire) noexcept;

static_assert(
    kRawWalSegmentHeaderBytes ==
    kRawV1SegmentHeaderBytes);
static_assert(
    kRawWalJournalHeaderBytes ==
    kRawV1JournalHeaderBytes);
static_assert(
    kRawWalRecordHeaderBytes ==
    kRawV1RecordHeaderBytes);
static_assert(
    kRawWalRecordTrailerBytes ==
    kRawV1RecordTrailerBytes);
static_assert(
    kRawWalDurableMarkerBytes ==
    kRawV1DurableMarkerBytes);
static_assert(
    kRawWalSegmentSealedFlag ==
    kRawV1SegmentSealed);

bool ValidateBootstrapWire(
    const RawWalWriterConfig& config) noexcept {
    SegmentHeaderV1 segment;
    DurableJournalHeaderV1 journal;
    if (DecodeSegmentHeaderV1(
            config.segment_header_wire,
            &segment) != RawV1Error::kNone ||
        DecodeDurableJournalHeaderV1(
            config.journal_header_wire,
            &journal) != RawV1Error::kNone) {
        return false;
    }
    return segment.source_stream_id ==
               config.source_stream_id &&
           segment.capture_date == config.capture_date &&
           segment.segment_sequence ==
               config.segment_sequence &&
           segment.segment_base_wal_pos ==
               config.segment_base_wal_pos &&
           segment.first_ingress_sequence ==
               config.first_ingress_sequence &&
           journal.source_stream_id ==
               config.source_stream_id &&
           journal.capture_date == config.capture_date &&
           segment.stream_day_id ==
               journal.stream_day_id &&
           segment.raw_schema_sha256 ==
               journal.raw_schema_sha256;
}

bool EncodeRecordWire(
    const RawWalRecordInputV1& input,
    std::vector<std::byte>* wire) noexcept {
    if (wire == nullptr ||
        input.vendor_head.size() !=
            kVendorMessageHeadBytes) {
        return false;
    }
    RawRecordInputV1 codec_input;
    codec_input.meta = input.meta;
    std::copy(
        input.vendor_head.begin(),
        input.vendor_head.end(),
        codec_input.vendor_head.begin());
    codec_input.vendor_body = input.vendor_body;
    return EncodeRawRecordV1(
               codec_input, wire, nullptr) ==
           RawV1Error::kNone;
}

bool EncodeMarkerWire(
    const RawWalWriterConfig& config,
    std::uint64_t durable_global_wal_pos,
    std::uint64_t durable_ingress_sequence,
    std::uint64_t durable_segment_offset,
    std::uint32_t marker_flags,
    std::array<std::byte, kRawWalDurableMarkerBytes>* wire) noexcept {
    if (wire == nullptr) {
        return false;
    }
    DurableMarkerV1 marker;
    marker.source_stream_id = config.source_stream_id;
    marker.segment_sequence = config.segment_sequence;
    marker.durable_global_wal_pos =
        durable_global_wal_pos;
    marker.durable_ingress_sequence =
        durable_ingress_sequence;
    marker.durable_segment_offset =
        durable_segment_offset;
    marker.marker_flags = marker_flags;
    return EncodeDurableMarkerV1(
               marker, wire) ==
           RawV1Error::kNone;
}

}  // namespace

RawWalRotationPlan PlanRawWalRotation(
    const SegmentHeaderV1& sealed_segment_header,
    const RawWalWriterSnapshot& sealed_snapshot) noexcept {
    RawWalRotationPlan plan;
    RawV1SegmentHeaderWire validated_wire{};
    if (EncodeSegmentHeaderV1(
            sealed_segment_header,
            &validated_wire) != RawV1Error::kNone) {
        plan.error = RawWalRotationError::kInvalidHeader;
        return plan;
    }
    if (sealed_segment_header.segment_flags != 0U) {
        plan.error =
            RawWalRotationError::
                kFinalizationContinuationUnsupported;
        return plan;
    }
    if (!sealed_snapshot.initialized ||
        !sealed_snapshot.sealed ||
        !sealed_snapshot.closed ||
        sealed_snapshot.fatal) {
        plan.error =
            RawWalRotationError::kSnapshotNotSealed;
        return plan;
    }
    if (sealed_snapshot.append !=
            sealed_snapshot.durable ||
        sealed_snapshot.durable.segment_offset <
            kRawWalSegmentHeaderBytes ||
        sealed_snapshot.durable.ingress_sequence <
            sealed_segment_header
                    .first_ingress_sequence -
                1U) {
        plan.error =
            RawWalRotationError::kSnapshotCursorMismatch;
        return plan;
    }

    std::uint64_t expected_global_wal_pos = 0U;
    if (!CheckedAdd(
            sealed_segment_header.segment_base_wal_pos,
            sealed_snapshot.durable.segment_offset,
            &expected_global_wal_pos)) {
        plan.error =
            RawWalRotationError::kWalPositionOverflow;
        return plan;
    }
    if (expected_global_wal_pos !=
        sealed_snapshot.durable.global_wal_pos) {
        plan.error =
            RawWalRotationError::kSnapshotCursorMismatch;
        return plan;
    }
    if (sealed_snapshot.journal_logical_size <
            kRawWalJournalHeaderBytes ||
        ((sealed_snapshot.journal_logical_size -
          kRawWalJournalHeaderBytes) %
         kRawWalDurableMarkerBytes) != 0U ||
        sealed_snapshot.journal_logical_size >
            std::numeric_limits<std::uint64_t>::max() -
                kRawWalDurableMarkerBytes) {
        plan.error =
            RawWalRotationError::kJournalOffsetInvalid;
        return plan;
    }
    if (sealed_segment_header.segment_sequence ==
        std::numeric_limits<std::uint32_t>::max()) {
        plan.error =
            RawWalRotationError::kSegmentSequenceOverflow;
        return plan;
    }
    if (sealed_snapshot.durable.ingress_sequence ==
        std::numeric_limits<std::uint64_t>::max()) {
        plan.error =
            RawWalRotationError::kIngressSequenceOverflow;
        return plan;
    }
    if (expected_global_wal_pos >
        std::numeric_limits<std::uint64_t>::max() -
            kRawWalSegmentHeaderBytes) {
        plan.error =
            RawWalRotationError::kWalPositionOverflow;
        return plan;
    }

    plan.next_segment_sequence =
        sealed_segment_header.segment_sequence + 1U;
    plan.next_segment_flags = 0U;
    plan.next_segment_base_wal_pos =
        expected_global_wal_pos;
    plan.next_first_ingress_sequence =
        sealed_snapshot.durable.ingress_sequence + 1U;
    plan.initial_durable_ingress_sequence =
        sealed_snapshot.durable.ingress_sequence;
    plan.existing_journal.journal_append_offset =
        sealed_snapshot.journal_logical_size;
    plan.existing_journal.previous_segment_sequence =
        sealed_segment_header.segment_sequence;
    plan.existing_journal.previous_sealed_cursor =
        sealed_snapshot.durable;
    plan.existing_journal.previous_marker_flags =
        kRawWalSegmentSealedFlag;
    return plan;
}

RawWalWriter::RawWalWriter(
    RawWalWriterConfig config,
    std::unique_ptr<RawWalIo> io)
    : config_(std::move(config)),
      io_(std::move(io)) {}

RawWalWriter::~RawWalWriter() {
    if (io_ != nullptr &&
        (!segment_closed_ || !journal_closed_)) {
        static_cast<void>(CloseBoth());
    }
}

bool RawWalWriter::Initialize() noexcept {
    if (fatal_.load(std::memory_order_acquire)) {
        return false;
    }
    if (initialized_.load(std::memory_order_acquire) ||
        sealed_.load(std::memory_order_acquire) ||
        closed_.load(std::memory_order_acquire)) {
        Trip(RawWalFailureKind::kInvalidState, EALREADY);
        return false;
    }
    if (!ValidateConfiguration()) {
        return false;
    }

    if (config_.initialization_mode ==
        RawWalInitializationMode::kRecoveredSealOnly) {
        journal_write_offset_ =
            config_.recovered_open.journal_append_offset;
        journal_logical_size_.store(
            journal_write_offset_, std::memory_order_release);
        logical_end_offset_ =
            config_.recovered_open.recovered_cursor
                .segment_offset;
        last_ingress_sequence_ =
            config_.recovered_open.recovered_cursor
                .ingress_sequence;
        PublishAppend(
            logical_end_offset_, last_ingress_sequence_);
        if (fatal_.load(std::memory_order_acquire)) {
            return false;
        }
        PublishDurable(
            logical_end_offset_, last_ingress_sequence_);
        if (fatal_.load(std::memory_order_acquire)) {
            return false;
        }
        initialized_.store(true, std::memory_order_release);
        return true;
    }

    if (config_.initialization_mode ==
        RawWalInitializationMode::kFreshJournal) {
        if (!config_.headers_already_persisted) {
            const RawWalIoVector journal_header{
                std::span<const std::byte>(
                    config_.journal_header_wire.data(),
                    config_.journal_header_wire.size())};
            if (!WriteAll(
                    RawWalFile::kJournal,
                    0U,
                    std::span<const RawWalIoVector>(
                        &journal_header, 1U),
                    RawWalFailureKind::kJournalWrite) ||
                !Sync(
                    RawWalFile::kJournal,
                    RawWalFailureKind::kJournalSync)) {
                return false;
            }
        }
        journal_write_offset_ =
            kRawWalJournalHeaderBytes;
    } else {
        journal_write_offset_ =
            config_.existing_journal
                .journal_append_offset;
    }
    journal_logical_size_.store(
        journal_write_offset_, std::memory_order_release);

    if (!config_.headers_already_persisted) {
        const RawWalIoVector segment_header{
            std::span<const std::byte>(
                config_.segment_header_wire.data(),
                config_.segment_header_wire.size())};
        if (!WriteAll(
                RawWalFile::kSegment,
                0U,
                std::span<const RawWalIoVector>(
                    &segment_header, 1U),
                RawWalFailureKind::kSegmentWrite) ||
            !Sync(
                RawWalFile::kSegment,
                RawWalFailureKind::kSegmentSync)) {
            return false;
        }
    }

    logical_end_offset_ = kRawWalSegmentHeaderBytes;
    last_ingress_sequence_ =
        config_.initial_durable_ingress_sequence;
    if (!AppendMarker(0U, false)) {
        return false;
    }
    if (config_.commit_observer != nullptr &&
        !config_.commit_observer->OnSegmentOpened(
            config_.segment_header_wire)) {
        Trip(
            RawWalFailureKind::kCommitObserver,
            EIO);
        return false;
    }

    PublishAppend(
        logical_end_offset_, last_ingress_sequence_);
    if (fatal_.load(std::memory_order_acquire)) {
        return false;
    }
    PublishDurable(
        logical_end_offset_, last_ingress_sequence_);
    if (fatal_.load(std::memory_order_acquire)) {
        return false;
    }
    initialized_.store(true, std::memory_order_release);
    return true;
}

bool RawWalWriter::AppendRecord(
    const RawWalRecordInputV1& input) noexcept {
    if (fatal_.load(std::memory_order_acquire)) {
        return false;
    }
    if (!initialized_.load(std::memory_order_acquire) ||
        sealed_.load(std::memory_order_acquire) ||
        closed_.load(std::memory_order_acquire) ||
        config_.initialization_mode ==
            RawWalInitializationMode::kRecoveredSealOnly) {
        Trip(RawWalFailureKind::kInvalidState, EPERM);
        return false;
    }
    if (input.meta.source_stream_id !=
            config_.source_stream_id ||
        input.meta.capture_date != config_.capture_date ||
        input.vendor_head.size() !=
            kVendorMessageHeadBytes ||
        last_ingress_sequence_ ==
            std::numeric_limits<std::uint64_t>::max() ||
        input.meta.ingress_sequence !=
            last_ingress_sequence_ + 1U) {
        Trip(RawWalFailureKind::kInvalidRecord, EINVAL);
        return false;
    }

    std::vector<std::byte> wire;
    if (!EncodeRecordWire(input, &wire) ||
        wire.size() <
            kRawWalRecordHeaderBytes +
                kRawWalRecordTrailerBytes ||
        (wire.size() % 8U) != 0U) {
        Trip(RawWalFailureKind::kInvalidRecord, EINVAL);
        return false;
    }

    const std::uint64_t record_size =
        static_cast<std::uint64_t>(wire.size());
    std::uint64_t new_logical_end = 0U;
    std::uint64_t new_global_end = 0U;
    std::uint64_t record_start_global_wal_pos = 0U;
    if (!CheckedAdd(
            logical_end_offset_,
            record_size,
            &new_logical_end) ||
        !CheckedAdd(
            config_.segment_base_wal_pos,
            logical_end_offset_,
            &record_start_global_wal_pos) ||
        !CheckedAdd(
            config_.segment_base_wal_pos,
            new_logical_end,
            &new_global_end)) {
        static_cast<void>(new_global_end);
        Trip(RawWalFailureKind::kCursorOverflow, EOVERFLOW);
        return false;
    }

    const std::size_t prefix_size =
        wire.size() - kRawWalRecordTrailerBytes;
    const std::size_t header_size =
        kRawWalRecordHeaderBytes;
    const std::array<RawWalIoVector, 2U> prefix_vectors{{
        {std::span<const std::byte>(
            wire.data(), header_size)},
        {std::span<const std::byte>(
            wire.data() + header_size,
            prefix_size - header_size)},
    }};
    if (!WriteAll(
            RawWalFile::kSegment,
            logical_end_offset_,
            prefix_vectors,
            RawWalFailureKind::kSegmentWrite)) {
        return false;
    }

    std::uint64_t trailer_offset = 0U;
    if (!CheckedAdd(
            logical_end_offset_,
            static_cast<std::uint64_t>(prefix_size),
            &trailer_offset)) {
        Trip(RawWalFailureKind::kCursorOverflow, EOVERFLOW);
        return false;
    }
    const RawWalIoVector trailer{
        std::span<const std::byte>(
            wire.data() + prefix_size,
            kRawWalRecordTrailerBytes)};
    if (!WriteAll(
            RawWalFile::kSegment,
            trailer_offset,
            std::span<const RawWalIoVector>(
                &trailer, 1U),
            RawWalFailureKind::kSegmentWrite)) {
        return false;
    }
    if (config_.commit_observer != nullptr &&
        !config_.commit_observer->OnRecordCommitted(
            wire,
            logical_end_offset_,
            record_start_global_wal_pos)) {
        Trip(
            RawWalFailureKind::kCommitObserver,
            EIO);
        return false;
    }

    logical_end_offset_ = new_logical_end;
    last_ingress_sequence_ = input.meta.ingress_sequence;
    PublishAppend(
        logical_end_offset_, last_ingress_sequence_);
    return !fatal_.load(std::memory_order_acquire);
}

bool RawWalWriter::FlushDurable() noexcept {
    if (fatal_.load(std::memory_order_acquire)) {
        return false;
    }
    if (!initialized_.load(std::memory_order_acquire) ||
        sealed_.load(std::memory_order_acquire) ||
        closed_.load(std::memory_order_acquire)) {
        Trip(RawWalFailureKind::kInvalidState, EPERM);
        return false;
    }
    if (durable_segment_offset_.load(
            std::memory_order_acquire) ==
        logical_end_offset_) {
        return true;
    }
    if (!Sync(
            RawWalFile::kSegment,
            RawWalFailureKind::kSegmentSync)) {
        return false;
    }
    return AppendMarker(0U, true);
}

bool RawWalWriter::SealAndClose() noexcept {
    if (closed_.load(std::memory_order_acquire)) {
        return sealed_.load(std::memory_order_acquire) &&
               !fatal_.load(std::memory_order_acquire);
    }
    if (fatal_.load(std::memory_order_acquire)) {
        static_cast<void>(CloseBoth());
        return false;
    }
    if (!initialized_.load(std::memory_order_acquire) ||
        sealed_.load(std::memory_order_acquire)) {
        Trip(RawWalFailureKind::kInvalidState, EPERM);
        static_cast<void>(CloseBoth());
        return false;
    }

    if (!TruncateSegment(logical_end_offset_) ||
        !Sync(
            RawWalFile::kSegment,
            RawWalFailureKind::kSegmentSync) ||
        !AppendMarker(kRawWalSegmentSealedFlag, true)) {
        static_cast<void>(CloseBoth());
        return false;
    }
    sealed_.store(true, std::memory_order_release);
    return CloseBoth();
}

RawWalWriterSnapshot RawWalWriter::Snapshot() const noexcept {
    RawWalWriterSnapshot result;
    for (;;) {
        const std::uint64_t before =
            snapshot_generation_.load(
                std::memory_order_acquire);
        if ((before & 1U) != 0U) {
            continue;
        }
        result.append.global_wal_pos =
            append_global_wal_pos_.load(
                std::memory_order_relaxed);
        result.append.ingress_sequence =
            append_ingress_sequence_.load(
                std::memory_order_relaxed);
        result.append.segment_offset =
            append_segment_offset_.load(
                std::memory_order_relaxed);
        result.durable.global_wal_pos =
            durable_global_wal_pos_.load(
                std::memory_order_relaxed);
        result.durable.ingress_sequence =
            durable_ingress_sequence_.load(
                std::memory_order_relaxed);
        result.durable.segment_offset =
            durable_segment_offset_.load(
                std::memory_order_relaxed);
        result.journal_logical_size =
            journal_logical_size_.load(
                std::memory_order_relaxed);
        result.initialized =
            initialized_.load(std::memory_order_relaxed);
        result.sealed =
            sealed_.load(std::memory_order_relaxed);
        result.closed =
            closed_.load(std::memory_order_relaxed);
        result.fatal =
            fatal_.load(std::memory_order_relaxed);
        std::atomic_thread_fence(
            std::memory_order_seq_cst);
        const std::uint64_t after =
            snapshot_generation_.load(
                std::memory_order_acquire);
        if (before == after && (after & 1U) == 0U) {
            return result;
        }
    }
}

RawWalFailure RawWalWriter::failure() const noexcept {
    if (!fatal_.load(std::memory_order_acquire)) {
        return {};
    }
    return {
        static_cast<RawWalFailureKind>(
            failure_kind_.load(std::memory_order_relaxed)),
        failure_errno_.load(std::memory_order_relaxed)};
}

RawWalSinkIdentityV1
RawWalWriter::identity() const noexcept {
    RawWalSinkIdentityV1 result;
    SegmentHeaderV1 segment;
    if (DecodeSegmentHeaderV1(
            config_.segment_header_wire,
            &segment) != RawV1Error::kNone) {
        return result;
    }
    result.writer_instance =
        config_.writer_instance;
    result.stream_day_id = segment.stream_day_id;
    result.source_stream_id =
        segment.source_stream_id;
    result.capture_date = segment.capture_date;
    result.segment_sequence =
        segment.segment_sequence;
    result.segment_base_wal_pos =
        segment.segment_base_wal_pos;
    result.first_ingress_sequence =
        segment.first_ingress_sequence;
    return result;
}

bool RawWalWriter::ValidateConfiguration() noexcept {
    std::uint64_t initial_end = 0U;
    SegmentHeaderV1 segment_header;
    const bool segment_header_valid =
        DecodeSegmentHeaderV1(
            config_.segment_header_wire,
            &segment_header) == RawV1Error::kNone;
    const bool basic_sequence_valid =
        config_.first_ingress_sequence != 0U &&
        config_.initial_durable_ingress_sequence !=
            std::numeric_limits<std::uint64_t>::max();
    if (io_ == nullptr ||
        config_.source_stream_id == 0U ||
        config_.capture_date == 0U ||
        !basic_sequence_valid ||
        !segment_header_valid ||
        segment_header.segment_flags != 0U ||
        !CheckedAdd(
            config_.segment_base_wal_pos,
            kRawWalSegmentHeaderBytes,
            &initial_end) ||
        !ValidateBootstrapWire(config_)) {
        static_cast<void>(initial_end);
        Trip(
            RawWalFailureKind::kInvalidConfiguration,
            EINVAL);
        return false;
    }

    bool initialization_valid = false;
    switch (config_.initialization_mode) {
        case RawWalInitializationMode::kFreshJournal:
            initialization_valid =
                config_.segment_sequence == 1U &&
                config_.segment_base_wal_pos == 0U &&
                config_.first_ingress_sequence == 1U &&
                config_
                        .initial_durable_ingress_sequence ==
                    0U &&
                IsDefaultExistingJournalInit(
                    config_.existing_journal) &&
                IsDefaultRecoveredOpenInit(
                    config_.recovered_open);
            break;
        case RawWalInitializationMode::kExistingJournal: {
            const RawWalExistingJournalInit&
                existing = config_.existing_journal;
            const bool journal_offset_valid =
                existing.journal_append_offset >=
                    kRawWalJournalHeaderBytes &&
                ((existing.journal_append_offset -
                  kRawWalJournalHeaderBytes) %
                 kRawWalDurableMarkerBytes) == 0U &&
                existing.journal_append_offset <=
                    std::numeric_limits<
                        std::uint64_t>::max() -
                        kRawWalDurableMarkerBytes;
            const bool previous_sequence_valid =
                existing.previous_segment_sequence != 0U &&
                existing.previous_segment_sequence !=
                    std::numeric_limits<
                        std::uint32_t>::max() &&
                config_.segment_sequence ==
                    existing.previous_segment_sequence + 1U;
            const bool previous_cursor_valid =
                existing.previous_marker_flags ==
                    kRawWalSegmentSealedFlag &&
                existing.previous_sealed_cursor
                        .segment_offset >=
                    kRawWalSegmentHeaderBytes &&
                existing.previous_sealed_cursor
                        .global_wal_pos >=
                    existing.previous_sealed_cursor
                        .segment_offset &&
                config_.segment_base_wal_pos ==
                    existing.previous_sealed_cursor
                        .global_wal_pos &&
                config_
                        .initial_durable_ingress_sequence ==
                    existing.previous_sealed_cursor
                        .ingress_sequence;
            initialization_valid =
                config_.segment_sequence > 1U &&
                config_.first_ingress_sequence ==
                    config_
                            .initial_durable_ingress_sequence +
                        1U &&
                journal_offset_valid &&
                previous_sequence_valid &&
                previous_cursor_valid &&
                IsDefaultRecoveredOpenInit(
                    config_.recovered_open);
            break;
        }
        case RawWalInitializationMode::kRecoveredSealOnly: {
            const RawWalRecoveredOpenInit& recovered =
                config_.recovered_open;
            std::uint64_t expected_global_wal_pos = 0U;
            const bool journal_offset_valid =
                recovered.journal_append_offset >=
                    kRawWalJournalHeaderBytes +
                        kRawWalDurableMarkerBytes &&
                ((recovered.journal_append_offset -
                  kRawWalJournalHeaderBytes) %
                 kRawWalDurableMarkerBytes) == 0U &&
                recovered.journal_append_offset <=
                    std::numeric_limits<
                        std::uint64_t>::max() -
                        kRawWalDurableMarkerBytes;
            const bool cursor_valid =
                recovered.accepted_marker_flags == 0U &&
                recovered.recovered_cursor.segment_offset >=
                    kRawWalSegmentHeaderBytes &&
                recovered.recovered_cursor
                        .ingress_sequence ==
                    config_
                        .initial_durable_ingress_sequence &&
                recovered.recovered_cursor
                        .ingress_sequence >=
                    config_.first_ingress_sequence - 1U &&
                CheckedAdd(
                    config_.segment_base_wal_pos,
                    recovered.recovered_cursor
                        .segment_offset,
                    &expected_global_wal_pos) &&
                recovered.recovered_cursor
                        .global_wal_pos ==
                    expected_global_wal_pos;
            initialization_valid =
                config_.segment_sequence != 0U &&
                config_.headers_already_persisted &&
                config_.commit_observer == nullptr &&
                journal_offset_valid &&
                cursor_valid &&
                IsDefaultExistingJournalInit(
                    config_.existing_journal);
            break;
        }
        default:
            initialization_valid = false;
            break;
    }
    if (!initialization_valid) {
        Trip(
            RawWalFailureKind::kInvalidConfiguration,
            EINVAL);
        return false;
    }
    return true;
}

bool RawWalWriter::WriteAll(
    RawWalFile file,
    std::uint64_t offset,
    std::span<const RawWalIoVector> vectors,
    RawWalFailureKind failure_kind) noexcept {
    if (io_ == nullptr ||
        vectors.empty() ||
        vectors.size() > kMaximumIoVectors) {
        Trip(failure_kind, EINVAL);
        return false;
    }

    std::uint64_t total_size = 0U;
    for (const RawWalIoVector& vector : vectors) {
        if (vector.bytes.size() >
                static_cast<std::size_t>(
                    std::numeric_limits<std::uint64_t>::max()) ||
            !CheckedAdd(
                total_size,
                static_cast<std::uint64_t>(
                    vector.bytes.size()),
                &total_size)) {
            Trip(failure_kind, EOVERFLOW);
            return false;
        }
    }
    std::uint64_t final_offset = 0U;
    if (total_size == 0U ||
        !CheckedAdd(offset, total_size, &final_offset)) {
        static_cast<void>(final_offset);
        Trip(failure_kind, EOVERFLOW);
        return false;
    }

    std::size_t vector_index = 0U;
    std::size_t within_vector = 0U;
    std::uint64_t current_offset = offset;
    while (vector_index < vectors.size()) {
        while (vector_index < vectors.size() &&
               within_vector ==
                   vectors[vector_index].bytes.size()) {
            ++vector_index;
            within_vector = 0U;
        }
        if (vector_index == vectors.size()) {
            break;
        }

        std::array<RawWalIoVector, kMaximumIoVectors>
            pending{};
        std::size_t pending_count = 0U;
        pending[pending_count++].bytes =
            vectors[vector_index].bytes.subspan(
                within_vector);
        for (std::size_t index = vector_index + 1U;
             index < vectors.size();
             ++index) {
            if (!vectors[index].bytes.empty()) {
                pending[pending_count++].bytes =
                    vectors[index].bytes;
            }
        }

        std::size_t pending_bytes = 0U;
        for (std::size_t index = 0U;
             index < pending_count;
             ++index) {
            if (pending[index].bytes.size() >
                    std::numeric_limits<std::size_t>::max() -
                        pending_bytes) {
                Trip(failure_kind, EOVERFLOW);
                return false;
            }
            pending_bytes += pending[index].bytes.size();
        }

        const RawWalWriteResult result =
            io_->WritevSome(
                file,
                current_offset,
                std::span<const RawWalIoVector>(
                    pending.data(), pending_count));
        if (result.error_number == EINTR &&
            result.bytes_written == 0U) {
            continue;
        }
        if (result.error_number != 0) {
            Trip(failure_kind, result.error_number);
            return false;
        }
        if (result.bytes_written == 0U ||
            result.bytes_written > pending_bytes) {
            Trip(failure_kind, EIO);
            return false;
        }

        std::uint64_t advanced_offset = 0U;
        if (!CheckedAdd(
                current_offset,
                static_cast<std::uint64_t>(
                    result.bytes_written),
                &advanced_offset)) {
            Trip(failure_kind, EOVERFLOW);
            return false;
        }
        current_offset = advanced_offset;

        std::size_t remaining =
            result.bytes_written;
        while (remaining > 0U) {
            const std::size_t available =
                vectors[vector_index].bytes.size() -
                within_vector;
            if (remaining < available) {
                within_vector += remaining;
                remaining = 0U;
            } else {
                remaining -= available;
                ++vector_index;
                within_vector = 0U;
                while (vector_index < vectors.size() &&
                       vectors[vector_index].bytes.empty()) {
                    ++vector_index;
                }
            }
        }
    }
    return current_offset == final_offset;
}

bool RawWalWriter::Sync(
    RawWalFile file,
    RawWalFailureKind failure_kind) noexcept {
    if (io_ == nullptr) {
        Trip(failure_kind, EBADF);
        return false;
    }
    for (;;) {
        const int error_number = io_->Fdatasync(file);
        if (error_number == EINTR) {
            continue;
        }
        if (error_number != 0) {
            Trip(failure_kind, error_number);
            return false;
        }
        return true;
    }
}

bool RawWalWriter::TruncateSegment(
    std::uint64_t logical_size) noexcept {
    if (io_ == nullptr) {
        Trip(RawWalFailureKind::kSegmentTruncate, EBADF);
        return false;
    }
    for (;;) {
        const int error_number =
            io_->Truncate(
                RawWalFile::kSegment, logical_size);
        if (error_number == EINTR) {
            continue;
        }
        if (error_number != 0) {
            Trip(
                RawWalFailureKind::kSegmentTruncate,
                error_number);
            return false;
        }
        return true;
    }
}

bool RawWalWriter::AppendMarker(
    std::uint32_t marker_flags,
    bool publish_durable) noexcept {
    std::uint64_t durable_global_wal_pos = 0U;
    if (!CheckedAdd(
            config_.segment_base_wal_pos,
            logical_end_offset_,
            &durable_global_wal_pos)) {
        Trip(RawWalFailureKind::kCursorOverflow, EOVERFLOW);
        return false;
    }

    std::array<std::byte, kRawWalDurableMarkerBytes>
        marker{};
    if (!EncodeMarkerWire(
            config_,
            durable_global_wal_pos,
            last_ingress_sequence_,
            logical_end_offset_,
            marker_flags,
            &marker)) {
        Trip(
            RawWalFailureKind::kInvalidConfiguration,
            EINVAL);
        return false;
    }
    const RawWalIoVector marker_vector{
        std::span<const std::byte>(
            marker.data(), marker.size())};
    if (!WriteAll(
            RawWalFile::kJournal,
            journal_write_offset_,
            std::span<const RawWalIoVector>(
                &marker_vector, 1U),
            RawWalFailureKind::kJournalWrite)) {
        return false;
    }

    std::uint64_t new_journal_offset = 0U;
    if (!CheckedAdd(
            journal_write_offset_,
            kRawWalDurableMarkerBytes,
            &new_journal_offset)) {
        Trip(RawWalFailureKind::kCursorOverflow, EOVERFLOW);
        return false;
    }
    journal_write_offset_ = new_journal_offset;
    journal_logical_size_.store(
        journal_write_offset_, std::memory_order_release);

    if (!Sync(
            RawWalFile::kJournal,
            RawWalFailureKind::kJournalSync)) {
        return false;
    }
    if (publish_durable) {
        PublishDurable(
            logical_end_offset_, last_ingress_sequence_);
    }
    if (marker_flags == kRawWalSegmentSealedFlag &&
        config_.commit_observer != nullptr) {
        const RawWalCursor sealed_cursor{
            durable_global_wal_pos,
            last_ingress_sequence_,
            logical_end_offset_};
        if (!config_.commit_observer->OnSegmentSealed(
                marker, sealed_cursor)) {
            Trip(
                RawWalFailureKind::kCommitObserver,
                EIO);
            return false;
        }
    }
    return !fatal_.load(std::memory_order_acquire);
}

bool RawWalWriter::CloseBoth() noexcept {
    if (io_ == nullptr) {
        Trip(RawWalFailureKind::kClose, EBADF);
        return false;
    }

    int first_error = 0;
    if (!segment_closed_) {
        const int error_number =
            io_->Close(RawWalFile::kSegment);
        if (error_number == 0) {
            segment_closed_ = true;
        } else {
            first_error = error_number;
        }
    }
    if (!journal_closed_) {
        const int error_number =
            io_->Close(RawWalFile::kJournal);
        if (error_number == 0) {
            journal_closed_ = true;
        } else if (first_error == 0) {
            first_error = error_number;
        }
    }
    if (first_error != 0) {
        Trip(RawWalFailureKind::kClose, first_error);
        return false;
    }
    closed_.store(true, std::memory_order_release);
    return true;
}

void RawWalWriter::Trip(
    RawWalFailureKind kind,
    int error_number) noexcept {
    std::uint8_t expected = static_cast<std::uint8_t>(
        RawWalFailureKind::kNone);
    if (failure_kind_.compare_exchange_strong(
            expected,
            static_cast<std::uint8_t>(kind),
            std::memory_order_relaxed,
            std::memory_order_relaxed)) {
        failure_errno_.store(
            error_number == 0 ? EIO : error_number,
            std::memory_order_relaxed);
        fatal_.store(true, std::memory_order_release);
    }
}

void RawWalWriter::PublishAppend(
    std::uint64_t segment_offset,
    std::uint64_t ingress_sequence) noexcept {
    std::uint64_t global_wal_pos = 0U;
    if (!CheckedAdd(
            config_.segment_base_wal_pos,
            segment_offset,
            &global_wal_pos)) {
        Trip(RawWalFailureKind::kCursorOverflow, EOVERFLOW);
        return;
    }
    const std::uint64_t generation =
        snapshot_generation_.load(
            std::memory_order_relaxed);
    if ((generation & 1U) != 0U ||
        generation >
            std::numeric_limits<std::uint64_t>::max() - 2U) {
        Trip(RawWalFailureKind::kCursorOverflow, EOVERFLOW);
        return;
    }
    snapshot_generation_.store(
        generation + 1U, std::memory_order_release);
    append_global_wal_pos_.store(
        global_wal_pos, std::memory_order_relaxed);
    append_ingress_sequence_.store(
        ingress_sequence, std::memory_order_relaxed);
    append_segment_offset_.store(
        segment_offset, std::memory_order_relaxed);
    snapshot_generation_.store(
        generation + 2U, std::memory_order_release);
}

void RawWalWriter::PublishDurable(
    std::uint64_t segment_offset,
    std::uint64_t ingress_sequence) noexcept {
    std::uint64_t global_wal_pos = 0U;
    if (!CheckedAdd(
            config_.segment_base_wal_pos,
            segment_offset,
            &global_wal_pos)) {
        Trip(RawWalFailureKind::kCursorOverflow, EOVERFLOW);
        return;
    }
    const std::uint64_t generation =
        snapshot_generation_.load(
            std::memory_order_relaxed);
    if ((generation & 1U) != 0U ||
        generation >
            std::numeric_limits<std::uint64_t>::max() - 2U) {
        Trip(RawWalFailureKind::kCursorOverflow, EOVERFLOW);
        return;
    }
    snapshot_generation_.store(
        generation + 1U, std::memory_order_release);
    durable_global_wal_pos_.store(
        global_wal_pos, std::memory_order_relaxed);
    durable_ingress_sequence_.store(
        ingress_sequence, std::memory_order_relaxed);
    durable_segment_offset_.store(
        segment_offset, std::memory_order_relaxed);
    snapshot_generation_.store(
        generation + 2U, std::memory_order_release);
}

}  // namespace l2flow::ingress
