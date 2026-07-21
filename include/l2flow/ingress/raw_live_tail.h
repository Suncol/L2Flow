#pragma once

#include "l2flow/ingress/raw_control_page.h"
#include "l2flow/ingress/raw_reader.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>

namespace l2flow::ingress {

struct RawLiveReadResult final {
    std::size_t bytes_read = 0U;
    int error_number = 0;
};

// A source-provided, already validated segment boundary. For an open segment,
// visible_end_offset is only an upper bound; RawLiveTail additionally caps it
// by the coherent append cursor from the control snapshot. For a sealed
// segment it must be the exact accepted seal offset.
struct RawLiveSegmentInfo final {
    SegmentHeaderV1 header{};
    std::uint64_t visible_end_offset = 0U;
    bool sealed = false;
};

// Production implementations retain the stream directory/control mapping and
// use pread into caller-owned buffers. They must not expose mutable mmap spans.
class RawLiveTailSource {
public:
    virtual ~RawLiveTailSource() = default;

    // Returns zero or an errno-style value.
    [[nodiscard]] virtual int ReadControl(
        RawControlSnapshot* snapshot,
        std::uint64_t* generation) noexcept = 0;
    [[nodiscard]] virtual int InspectSegment(
        std::uint32_t segment_sequence,
        RawLiveSegmentInfo* info) noexcept = 0;
    [[nodiscard]] virtual RawLiveReadResult ReadSegmentSome(
        std::uint32_t segment_sequence,
        std::uint64_t offset,
        std::span<std::byte> output) noexcept = 0;
};

enum class RawLiveRecordProvenance : std::uint8_t {
    kDurable = 0U,
    kAppendVisible,
};

struct RawLiveRecord final {
    RawRecordView view;
    SegmentHeaderV1 segment{};
    RawLiveRecordProvenance provenance =
        RawLiveRecordProvenance::kAppendVisible;
    l2flow::common::Identity128 writer_instance{};
    std::uint64_t control_generation = 0U;
};

// Explicit proof that a validated sealed segment ended and the next
// validated segment header occupies the immediately following WAL range.
// Consumers must process this frontier before any record from next_segment.
struct RawLiveSegmentTransitionV1 final {
    l2flow::common::Identity128 writer_instance{};
    SegmentHeaderV1 previous_segment{};
    std::uint64_t previous_segment_end_offset = 0U;
    SegmentHeaderV1 next_segment{};
    // Exclusive end of next_segment's validated 4096-byte header.
    std::uint64_t next_data_begin_wal_pos = 0U;
    // Unchanged across the header; this must equal
    // next_segment.first_ingress_sequence.
    std::uint64_t next_ingress_sequence = 0U;
    std::uint64_t control_generation = 0U;
};

enum class RawLiveTailError : std::uint8_t {
    kNone = 0U,
    kNullSource,
    kInvalidAttach,
    kControlUnavailable,
    kControlIdentityMismatch,
    kControlCursorInvalid,
    kInstanceChanged,
    kSegmentUnavailable,
    kSegmentIdentityMismatch,
    kSegmentOrderViolation,
    kCursorOverflow,
    kReadFailure,
    kShortRead,
    kRecordHeaderInvalid,
    kRecordPastPublication,
    kRecordInvalid,
    kIngressSequenceExhausted,
    kResourceExhausted,
};

enum class RawLiveTailStepKind : std::uint8_t {
    kRecord = 0U,
    kSegmentTransition,
    kWouldBlock,
    kEnd,
    kInstanceChanged,
    kError,
};

struct RawLiveTailStep final {
    RawLiveTailStepKind kind = RawLiveTailStepKind::kWouldBlock;
    RawLiveTailError error = RawLiveTailError::kNone;
    RawReaderError reader_error = RawReaderError::kNone;
    RawV1Error codec_error = RawV1Error::kNone;
    int error_number = 0;
    std::optional<RawLiveRecord> record;
    std::optional<RawLiveSegmentTransitionV1>
        segment_transition;
};

struct RawLiveTailAttachV1 final {
    l2flow::common::Identity128 writer_instance{};
    l2flow::common::Identity128 stream_day_id{};
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    std::uint32_t segment_sequence = 0U;
    // Exclusive, record-aligned cursor obtained from Raw recovery.
    std::uint64_t global_wal_pos = 0U;
    std::uint64_t segment_offset = 0U;
    // Sequence expected at segment_offset.
    std::uint64_t next_ingress_sequence = 0U;
};

// Nonblocking, single-consumer append-visible tail. Each returned record owns
// a pread copy. Crossing a segment boundary returns one explicit
// kSegmentTransition before records from the new segment. Any writer-instance
// change invalidates the attach and requires the caller to run the full
// control/namespace/recovery attach gate again.
class RawLiveTail final {
public:
    RawLiveTail(const RawLiveTail&) = delete;
    RawLiveTail& operator=(const RawLiveTail&) = delete;
    RawLiveTail(RawLiveTail&&) = delete;
    RawLiveTail& operator=(RawLiveTail&&) = delete;
    ~RawLiveTail() = default;

    [[nodiscard]] static RawLiveTailError Attach(
        RawLiveTailSource* source,
        const RawLiveTailAttachV1& attach,
        std::unique_ptr<RawLiveTail>* output) noexcept;

    [[nodiscard]] RawLiveTailStep Next() noexcept;

    [[nodiscard]] std::uint32_t segment_sequence() const noexcept {
        return segment_sequence_;
    }
    [[nodiscard]] std::uint64_t
    initial_global_wal_pos() const noexcept {
        return attach_.global_wal_pos;
    }
    [[nodiscard]] std::uint64_t segment_offset() const noexcept {
        return segment_offset_;
    }
    [[nodiscard]] std::uint64_t next_ingress_sequence() const noexcept {
        return next_ingress_sequence_;
    }
    [[nodiscard]] const l2flow::common::Identity128&
    writer_instance() const noexcept {
        return attach_.writer_instance;
    }
    [[nodiscard]] const l2flow::common::Identity128&
    stream_day_id() const noexcept {
        return attach_.stream_day_id;
    }
    [[nodiscard]] std::uint32_t
    source_stream_id() const noexcept {
        return attach_.source_stream_id;
    }
    [[nodiscard]] std::uint32_t
    capture_date() const noexcept {
        return attach_.capture_date;
    }

private:
    RawLiveTail(
        RawLiveTailSource* source,
        RawLiveTailAttachV1 attach) noexcept;

    [[nodiscard]] RawLiveTailStep Fail(
        RawLiveTailError error,
        int error_number = 0,
        RawReaderError reader_error = RawReaderError::kNone,
        RawV1Error codec_error = RawV1Error::kNone) noexcept;
    [[nodiscard]] bool ValidateControl(
        const RawControlSnapshot& snapshot) const noexcept;
    [[nodiscard]] bool ReadAll(
        std::uint32_t segment_sequence,
        std::uint64_t offset,
        std::span<std::byte> output,
        RawLiveTailStep* failure) noexcept;

    RawLiveTailSource* source_ = nullptr;
    RawLiveTailAttachV1 attach_{};
    std::uint32_t segment_sequence_ = 0U;
    std::uint64_t segment_offset_ = 0U;
    std::uint64_t next_ingress_sequence_ = 0U;
    bool terminal_ = false;
};

}  // namespace l2flow::ingress
