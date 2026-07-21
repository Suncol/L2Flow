#include "l2flow/ingress/raw_live_tail.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <limits>
#include <new>
#include <utility>
#include <vector>

namespace l2flow::ingress {
namespace {

bool IsZeroIdentity(
    const l2flow::common::Identity128& identity) noexcept {
    return std::all_of(
        identity.begin(),
        identity.end(),
        [](std::byte value) noexcept {
            return value == std::byte{0};
        });
}

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

bool FitsSize(std::uint64_t value) noexcept {
    if constexpr (
        sizeof(std::size_t) < sizeof(std::uint64_t)) {
        return value <= static_cast<std::uint64_t>(
                            std::numeric_limits<std::size_t>::max());
    }
    static_cast<void>(value);
    return true;
}

}  // namespace

RawLiveTail::RawLiveTail(
    RawLiveTailSource* source,
    RawLiveTailAttachV1 attach) noexcept
    : source_(source),
      attach_(std::move(attach)),
      segment_sequence_(attach_.segment_sequence),
      segment_offset_(attach_.segment_offset),
      next_ingress_sequence_(attach_.next_ingress_sequence) {}

RawLiveTailError RawLiveTail::Attach(
    RawLiveTailSource* source,
    const RawLiveTailAttachV1& attach,
    std::unique_ptr<RawLiveTail>* output) noexcept {
    if (output == nullptr) {
        return RawLiveTailError::kInvalidAttach;
    }
    if (source == nullptr) {
        return RawLiveTailError::kNullSource;
    }
    if (IsZeroIdentity(attach.writer_instance) ||
        IsZeroIdentity(attach.stream_day_id) ||
        attach.source_stream_id == 0U ||
        attach.capture_date == 0U ||
        attach.segment_sequence == 0U ||
        attach.global_wal_pos == 0U ||
        attach.segment_offset < kRawV1SegmentHeaderBytes ||
        attach.next_ingress_sequence == 0U) {
        return RawLiveTailError::kInvalidAttach;
    }

    RawControlSnapshot control;
    std::uint64_t generation = 0U;
    const int control_error =
        source->ReadControl(&control, &generation);
    if (control_error != 0) {
        return RawLiveTailError::kControlUnavailable;
    }
    static_cast<void>(generation);
    if (control.writer_instance != attach.writer_instance) {
        return RawLiveTailError::kInstanceChanged;
    }
    if (control.stream_day_id != attach.stream_day_id ||
        control.source_stream_id != attach.source_stream_id ||
        control.capture_date != attach.capture_date) {
        return RawLiveTailError::kControlIdentityMismatch;
    }
    if (control.segment_sequence < attach.segment_sequence ||
        control.durable_global_wal_pos >
            control.append_global_wal_pos ||
        control.durable_ingress_sequence >
            control.append_ingress_sequence) {
        return RawLiveTailError::kControlCursorInvalid;
    }

    RawLiveSegmentInfo segment;
    if (source->InspectSegment(
            attach.segment_sequence, &segment) != 0) {
        return RawLiveTailError::kSegmentUnavailable;
    }
    if (segment.header.source_stream_id !=
            attach.source_stream_id ||
        segment.header.capture_date != attach.capture_date ||
        segment.header.stream_day_id != attach.stream_day_id ||
        segment.header.segment_sequence !=
            attach.segment_sequence ||
        segment.visible_end_offset < attach.segment_offset) {
        return RawLiveTailError::kSegmentIdentityMismatch;
    }
    std::uint64_t expected_global_wal_pos = 0U;
    if (!CheckedAdd(
            segment.header.segment_base_wal_pos,
            attach.segment_offset,
            &expected_global_wal_pos) ||
        expected_global_wal_pos !=
            attach.global_wal_pos ||
        (control.segment_sequence ==
             attach.segment_sequence &&
         (attach.segment_offset >
              control.append_segment_offset ||
          attach.global_wal_pos >
              control.append_global_wal_pos))) {
        return RawLiveTailError::kControlCursorInvalid;
    }

    try {
        output->reset(new RawLiveTail(source, attach));
    } catch (...) {
        return RawLiveTailError::kResourceExhausted;
    }
    return RawLiveTailError::kNone;
}

RawLiveTailStep RawLiveTail::Fail(
    RawLiveTailError error,
    int error_number,
    RawReaderError reader_error,
    RawV1Error codec_error) noexcept {
    RawLiveTailStep result;
    result.kind =
        error == RawLiveTailError::kInstanceChanged
            ? RawLiveTailStepKind::kInstanceChanged
            : RawLiveTailStepKind::kError;
    result.error = error;
    result.error_number = error_number;
    result.reader_error = reader_error;
    result.codec_error = codec_error;
    terminal_ = true;
    return result;
}

bool RawLiveTail::ValidateControl(
    const RawControlSnapshot& snapshot) const noexcept {
    return snapshot.writer_instance == attach_.writer_instance &&
           snapshot.stream_day_id == attach_.stream_day_id &&
           snapshot.source_stream_id ==
               attach_.source_stream_id &&
           snapshot.capture_date == attach_.capture_date &&
           snapshot.segment_sequence >= segment_sequence_ &&
           snapshot.durable_global_wal_pos <=
               snapshot.append_global_wal_pos &&
           snapshot.durable_ingress_sequence <=
               snapshot.append_ingress_sequence;
}

bool RawLiveTail::ReadAll(
    std::uint32_t segment_sequence,
    std::uint64_t offset,
    std::span<std::byte> output,
    RawLiveTailStep* failure) noexcept {
    std::size_t completed = 0U;
    while (completed < output.size()) {
        std::uint64_t current_offset = 0U;
        if (!CheckedAdd(
                offset,
                static_cast<std::uint64_t>(completed),
                &current_offset)) {
            *failure = Fail(
                RawLiveTailError::kCursorOverflow,
                EOVERFLOW);
            return false;
        }
        const RawLiveReadResult result =
            source_->ReadSegmentSome(
                segment_sequence,
                current_offset,
                output.subspan(completed));
        if (result.error_number == EINTR &&
            result.bytes_read == 0U) {
            continue;
        }
        if (result.error_number != 0) {
            *failure = Fail(
                RawLiveTailError::kReadFailure,
                result.error_number);
            return false;
        }
        if (result.bytes_read == 0U ||
            result.bytes_read >
                output.size() - completed) {
            *failure = Fail(
                RawLiveTailError::kShortRead,
                EIO);
            return false;
        }
        completed += result.bytes_read;
    }
    return true;
}

RawLiveTailStep RawLiveTail::Next() noexcept {
    if (terminal_) {
        RawLiveTailStep result;
        result.kind = RawLiveTailStepKind::kEnd;
        return result;
    }

    for (;;) {
    RawControlSnapshot before;
    std::uint64_t before_generation = 0U;
    const int before_error =
        source_->ReadControl(
            &before, &before_generation);
    if (before_error != 0) {
        if (before_error == ESTALE) {
            return Fail(
                RawLiveTailError::kInstanceChanged,
                ESTALE);
        }
        return Fail(
            RawLiveTailError::kControlUnavailable,
            before_error);
    }
    if (before.writer_instance !=
        attach_.writer_instance) {
        return Fail(RawLiveTailError::kInstanceChanged);
    }
    if (!ValidateControl(before)) {
        return Fail(
            before.stream_day_id != attach_.stream_day_id ||
                    before.source_stream_id !=
                        attach_.source_stream_id ||
                    before.capture_date != attach_.capture_date
                ? RawLiveTailError::kControlIdentityMismatch
                : RawLiveTailError::kControlCursorInvalid,
            EINVAL);
    }

    RawLiveSegmentInfo segment;
    const int inspect_error =
        source_->InspectSegment(
            segment_sequence_, &segment);
    if (inspect_error != 0) {
        return Fail(
            RawLiveTailError::kSegmentUnavailable,
            inspect_error);
    }
    if (segment.header.source_stream_id !=
            attach_.source_stream_id ||
        segment.header.capture_date !=
            attach_.capture_date ||
        segment.header.stream_day_id !=
            attach_.stream_day_id ||
        segment.header.segment_sequence !=
            segment_sequence_ ||
        segment.visible_end_offset <
            kRawV1SegmentHeaderBytes) {
        return Fail(
            RawLiveTailError::kSegmentIdentityMismatch,
            EINVAL);
    }

    std::uint64_t visible_end =
        segment.visible_end_offset;
    if (segment_sequence_ == before.segment_sequence) {
        std::uint64_t expected_global = 0U;
        if (before.append_segment_offset <
                kRawV1SegmentHeaderBytes ||
            !CheckedAdd(
                segment.header.segment_base_wal_pos,
                before.append_segment_offset,
                &expected_global) ||
            expected_global !=
                before.append_global_wal_pos) {
            return Fail(
                RawLiveTailError::kControlCursorInvalid,
                EINVAL);
        }
        visible_end = std::min(
            visible_end,
            before.append_segment_offset);
    } else if (!segment.sealed) {
        return Fail(
            RawLiveTailError::kSegmentOrderViolation,
            EINVAL);
    }
    if (segment_offset_ > visible_end) {
        return Fail(
            RawLiveTailError::kControlCursorInvalid,
            EINVAL);
    }
    if (segment_offset_ == visible_end) {
        if (segment_sequence_ <
            before.segment_sequence) {
            if (!segment.sealed ||
                segment_sequence_ ==
                    std::numeric_limits<std::uint32_t>::max()) {
                return Fail(
                    RawLiveTailError::kSegmentOrderViolation,
                    EINVAL);
            }
            RawLiveSegmentInfo next;
            const std::uint32_t next_sequence =
                segment_sequence_ + 1U;
            const int next_error =
                source_->InspectSegment(
                    next_sequence, &next);
            std::uint64_t expected_next_base = 0U;
            std::uint64_t next_data_begin_wal_pos = 0U;
            if (next_error != 0 ||
                next.header.source_stream_id !=
                    attach_.source_stream_id ||
                next.header.capture_date !=
                    attach_.capture_date ||
                next.header.stream_day_id !=
                    attach_.stream_day_id ||
                next.header.segment_sequence !=
                    next_sequence ||
                next.visible_end_offset <
                    kRawV1SegmentHeaderBytes ||
                (next_sequence <
                         before.segment_sequence &&
                 !next.sealed) ||
                !CheckedAdd(
                    segment.header.segment_base_wal_pos,
                    visible_end,
                    &expected_next_base) ||
                next.header.segment_base_wal_pos !=
                    expected_next_base ||
                !CheckedAdd(
                    next.header.segment_base_wal_pos,
                    kRawV1SegmentHeaderBytes,
                    &next_data_begin_wal_pos) ||
                next.header.first_ingress_sequence !=
                    next_ingress_sequence_ ||
                (next_sequence ==
                         before.segment_sequence &&
                 (before.append_segment_offset <
                      kRawV1SegmentHeaderBytes ||
                  next.header.segment_base_wal_pos >
                      before.append_global_wal_pos ||
                  before.append_segment_offset >
                      std::numeric_limits<
                          std::uint64_t>::max() -
                          next.header
                              .segment_base_wal_pos ||
                  next.header.segment_base_wal_pos +
                          before
                              .append_segment_offset !=
                      before
                          .append_global_wal_pos))) {
                return Fail(
                    RawLiveTailError::kSegmentOrderViolation,
                    next_error == 0 ? EINVAL : next_error);
            }
            RawLiveTailStep result;
            result.kind =
                RawLiveTailStepKind::
                    kSegmentTransition;
            result.segment_transition.emplace(
                RawLiveSegmentTransitionV1{
                    attach_.writer_instance,
                    segment.header,
                    visible_end,
                    next.header,
                    next_data_begin_wal_pos,
                    next_ingress_sequence_,
                    before_generation});
            segment_sequence_ = next_sequence;
            segment_offset_ = kRawV1SegmentHeaderBytes;
            return result;
        }
        RawLiveTailStep result;
        result.kind = segment.sealed
                          ? RawLiveTailStepKind::kEnd
                          : RawLiveTailStepKind::kWouldBlock;
        if (segment.sealed) {
            terminal_ = true;
        }
        return result;
    }

    if (visible_end - segment_offset_ <
        kRawV1RecordHeaderBytes) {
        return Fail(
            RawLiveTailError::kRecordPastPublication,
            EINVAL);
    }
    std::array<std::byte, kRawV1RecordHeaderBytes>
        header_wire{};
    RawLiveTailStep read_failure;
    if (!ReadAll(
            segment_sequence_,
            segment_offset_,
            header_wire,
            &read_failure)) {
        return read_failure;
    }
    RawRecordHeaderV1 header;
    const RawV1Error header_error =
        DecodeRawRecordHeaderV1(
            header_wire, &header);
    if (header_error != RawV1Error::kNone) {
        return Fail(
            RawLiveTailError::kRecordHeaderInvalid,
            EINVAL,
            RawReaderError::kRecordHeaderInvalid,
            header_error);
    }
    const std::uint64_t record_size =
        header.record_size;
    std::uint64_t record_end = 0U;
    if (!CheckedAdd(
            segment_offset_,
            record_size,
            &record_end)) {
        return Fail(
            RawLiveTailError::kCursorOverflow,
            EOVERFLOW);
    }
    if (record_end > visible_end ||
        !FitsSize(record_size)) {
        return Fail(
            RawLiveTailError::kRecordPastPublication,
            EINVAL);
    }

    std::shared_ptr<std::vector<std::byte>> mutable_wire;
    try {
        mutable_wire =
            std::make_shared<std::vector<std::byte>>(
                static_cast<std::size_t>(record_size));
    } catch (...) {
        return Fail(
            RawLiveTailError::kResourceExhausted,
            ENOMEM);
    }
    std::copy(
        header_wire.begin(),
        header_wire.end(),
        mutable_wire->begin());
    if (record_size > kRawV1RecordHeaderBytes &&
        !ReadAll(
            segment_sequence_,
            segment_offset_ +
                kRawV1RecordHeaderBytes,
            std::span<std::byte>(
                mutable_wire->data() +
                    kRawV1RecordHeaderBytes,
                mutable_wire->size() -
                    kRawV1RecordHeaderBytes),
            &read_failure)) {
        return read_failure;
    }

    RawControlSnapshot after;
    std::uint64_t after_generation = 0U;
    const int after_error =
        source_->ReadControl(
            &after, &after_generation);
    if (after_error != 0) {
        if (after_error == ESTALE) {
            return Fail(
                RawLiveTailError::kInstanceChanged,
                ESTALE);
        }
        return Fail(
            RawLiveTailError::kControlUnavailable,
            after_error);
    }
    if (after.writer_instance !=
        attach_.writer_instance) {
        return Fail(RawLiveTailError::kInstanceChanged);
    }
    if (!ValidateControl(after)) {
        return Fail(
            RawLiveTailError::kControlCursorInvalid,
            EINVAL);
    }
    std::uint64_t record_end_wal = 0U;
    if (!CheckedAdd(
            segment.header.segment_base_wal_pos,
            record_end,
            &record_end_wal) ||
        record_end_wal > after.append_global_wal_pos) {
        return Fail(
            RawLiveTailError::kRecordPastPublication,
            EINVAL);
    }

    std::shared_ptr<const std::vector<std::byte>>
        immutable_wire = std::move(mutable_wire);
    RawOwnedRecordResult decoded =
        DecodeOwnedRawRecordV1(
            immutable_wire,
            segment.header,
            segment_offset_,
            next_ingress_sequence_);
    if (!decoded.ok()) {
        return Fail(
            RawLiveTailError::kRecordInvalid,
            EINVAL,
            decoded.error,
            decoded.codec_error);
    }

    RawLiveTailStep result;
    result.kind = RawLiveTailStepKind::kRecord;
    result.record.emplace(
        RawLiveRecord{
            std::move(*decoded.record),
            segment.header,
            record_end_wal <=
                    after.durable_global_wal_pos
                ? RawLiveRecordProvenance::kDurable
                : RawLiveRecordProvenance::kAppendVisible,
            attach_.writer_instance,
            after_generation});
    segment_offset_ = record_end;
    if (next_ingress_sequence_ ==
        std::numeric_limits<std::uint64_t>::max()) {
        terminal_ = true;
        result.error =
            RawLiveTailError::kIngressSequenceExhausted;
    } else {
        ++next_ingress_sequence_;
    }
    static_cast<void>(before_generation);
    return result;
    }
}

}  // namespace l2flow::ingress
