#pragma once

#include "l2flow/common/sha256.h"
#include "l2flow/ingress/raw_index_v1.h"
#include "l2flow/ingress/raw_segment_artifacts.h"
#include "l2flow/ingress/raw_wal_writer.h"

#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

namespace l2flow::ingress {

enum class RawSegmentAccumulatorErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidOptions,
    kInvalidState,
    kInvalidSegmentHeader,
    kSchemaMismatch,
    kRecordCoordinateMismatch,
    kInvalidRecord,
    kIndexUpdate,
    kHashUpdate,
    kInvalidSealMarker,
    kSealCursorMismatch,
    kHashFinalize,
    kIndexFinalize,
    kIndexSelfValidation,
};

[[nodiscard]] std::string_view
RawSegmentAccumulatorErrorV1Name(
    RawSegmentAccumulatorErrorV1 error) noexcept;

// Writer-thread observer that hashes the exact header and every fully
// committed record incrementally and feeds every record to RawIndexBuilderV1.
// It never reads the segment fd. A successful seal therefore makes R6/R7
// computation independent of the logical segment size.
class RawSegmentAccumulatorV1 final
    : public RawWalCommitObserver {
public:
    explicit RawSegmentAccumulatorV1(
        RawSegmentArtifactOptionsV1 options) noexcept;

    RawSegmentAccumulatorV1(
        const RawSegmentAccumulatorV1&) = delete;
    RawSegmentAccumulatorV1& operator=(
        const RawSegmentAccumulatorV1&) = delete;
    RawSegmentAccumulatorV1(
        RawSegmentAccumulatorV1&&) = delete;
    RawSegmentAccumulatorV1& operator=(
        RawSegmentAccumulatorV1&&) = delete;

    [[nodiscard]] bool OnSegmentOpened(
        std::span<const std::byte>
            segment_header_wire) noexcept override;
    [[nodiscard]] bool OnRecordCommitted(
        std::span<const std::byte> record_wire,
        std::uint64_t record_start_segment_offset,
        std::uint64_t
            record_start_global_wal_pos) noexcept override;
    [[nodiscard]] bool OnSegmentSealed(
        std::span<const std::byte>
            accepted_marker_wire,
        const RawWalCursor&
            sealed_cursor) noexcept override;

    [[nodiscard]] RawSegmentAccumulatorErrorV1
    error() const noexcept {
        return error_;
    }
    [[nodiscard]] bool opened() const noexcept {
        return opened_;
    }
    [[nodiscard]] bool sealed() const noexcept {
        return sealed_;
    }
    [[nodiscard]] std::uint64_t record_count() const noexcept {
        return record_count_;
    }
    [[nodiscard]] std::uint64_t logical_end_offset()
        const noexcept {
        return hasher_.total_bytes();
    }

    // One-shot ownership transfer. Output is unchanged on failure.
    [[nodiscard]] bool TakeArtifactPlan(
        RawSegmentArtifactPlanV1* plan) noexcept;

private:
    void Trip(
        RawSegmentAccumulatorErrorV1 error) noexcept;

    RawSegmentArtifactOptionsV1 options_{};
    l2flow::common::Sha256Hasher hasher_;
    SegmentHeaderV1 segment_{};
    std::optional<RawIndexBuilderV1> index_builder_;
    RawSegmentArtifactPlanV1 artifact_plan_{};
    std::uint64_t record_count_ = 0U;
    std::optional<std::uint64_t>
        first_ingress_sequence_;
    std::optional<std::uint64_t>
        last_ingress_sequence_;
    bool opened_ = false;
    bool sealed_ = false;
    bool artifact_plan_taken_ = false;
    RawSegmentAccumulatorErrorV1 error_ =
        RawSegmentAccumulatorErrorV1::kNone;
};

}  // namespace l2flow::ingress
