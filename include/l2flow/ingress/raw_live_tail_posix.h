#pragma once

#include "l2flow/ingress/raw_live_tail.h"
#include "l2flow/ingress/raw_recovery.h"

#include <cstdint>
#include <memory>
#include <string>

namespace l2flow::ingress {

inline constexpr std::uint32_t
    kRawLiveTailPosixAbsoluteMaxSegments = 100'000U;

struct RawLiveTailPosixLimitsV1 final {
    std::uint32_t max_segments =
        kRawLiveTailPosixAbsoluteMaxSegments;
    std::uint64_t max_segment_bytes =
        UINT64_C(4) * UINT64_C(1024) * UINT64_C(1024) *
        UINT64_C(1024);
    std::uint64_t max_journal_markers = UINT64_C(1'000'000);
    std::uint32_t max_control_reattach_attempts = 8U;
};

// Exact capability facts established by service-level recovery before a new
// control inode is exposed. A source refuses an initial control cursor older
// than this accepted journal frontier.
struct RawLiveTailPosixAttachGateV1 final {
    l2flow::common::Identity128 writer_instance{};
    l2flow::common::Identity128 stream_day_id{};
    RawRecoveryCursorV1 recovered_durable{};
};

// Production, single-consumer Raw live-tail source. It retains a verified
// alias of the stream-day directory and read-only retained descriptors for
// durable.journal and every segment it observes. Mutable Raw segments are
// never mmap'ed; ReadSegmentSome() always performs one pread into caller-owned
// storage and reports the real POSIX short-read/EINTR result.
//
// The control.page mapping is the only mutable mapping. A final-name inode
// replacement is detected around every snapshot read. A changed writer
// instance permanently fences this source; a caller must construct a new
// source through the complete recovery-frontier attach gate.
class RawLiveTailPosixSource final : public RawLiveTailSource {
public:
    ~RawLiveTailPosixSource() override;

    RawLiveTailPosixSource(
        const RawLiveTailPosixSource&) = delete;
    RawLiveTailPosixSource& operator=(
        const RawLiveTailPosixSource&) = delete;
    RawLiveTailPosixSource(
        RawLiveTailPosixSource&&) = delete;
    RawLiveTailPosixSource& operator=(
        RawLiveTailPosixSource&&) = delete;

    [[nodiscard]] int ReadControl(
        RawControlSnapshot* snapshot,
        std::uint64_t* generation) noexcept override;
    [[nodiscard]] int InspectSegment(
        std::uint32_t segment_sequence,
        RawLiveSegmentInfo* info) noexcept override;
    [[nodiscard]] RawLiveReadResult ReadSegmentSome(
        std::uint32_t segment_sequence,
        std::uint64_t offset,
        std::span<std::byte> output) noexcept override;

    // Diagnostic accessors expose only flags, never borrowed descriptors.
    [[nodiscard]] int directory_open_flags() const noexcept;
    [[nodiscard]] int control_open_flags() const noexcept;
    [[nodiscard]] int journal_open_flags() const noexcept;
    [[nodiscard]] int segment_open_flags(
        std::uint32_t segment_sequence) const noexcept;
    [[nodiscard]] std::size_t retained_segment_count()
        const noexcept;

private:
    struct Impl;

    friend std::unique_ptr<RawLiveTailPosixSource>
    OpenRawLiveTailPosixSource(
        int,
        std::uint32_t,
        std::uint32_t,
        const RawLiveTailPosixAttachGateV1&,
        RawLiveTailPosixLimitsV1,
        std::string*) noexcept;

    explicit RawLiveTailPosixSource(
        std::unique_ptr<Impl> impl) noexcept;

    std::unique_ptr<Impl> impl_;
};

// stream_directory_fd is only an authority/identity input. The returned
// source reopens "." with O_DIRECTORY|O_NOFOLLOW|O_NOATIME, verifies that it
// is the same private service-owned directory, and then retains that alias.
// Attach fails unless control.page, durable.journal, the current segment, and
// the control snapshot's durable marker all form one validated namespace.
[[nodiscard]] std::unique_ptr<RawLiveTailPosixSource>
OpenRawLiveTailPosixSource(
    int stream_directory_fd,
    std::uint32_t expected_source_stream_id,
    std::uint32_t expected_capture_date,
    const RawLiveTailPosixAttachGateV1& attach_gate,
    RawLiveTailPosixLimitsV1 limits = {},
    std::string* error = nullptr) noexcept;

}  // namespace l2flow::ingress
