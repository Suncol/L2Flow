#pragma once

#include "l2flow/ipc/realtime_certified_service_v1.h"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string_view>

namespace l2flow::ipc {

// The expected identity must come from the already-open FAST V2 session.
// Requiring all three fields prevents a stale CERTIFIED socket or descriptor
// from being silently paired with another run/day.
struct RealtimeCertifiedExpectedSessionV1 final {
    std::array<std::uint8_t, 16U> run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
};

struct RealtimeCertifiedReaderOpenOptionsV1 final {
    std::filesystem::path control_socket_path;
    RealtimeCertifiedExpectedSessionV1 expected_session{};
    std::chrono::milliseconds timeout{1000};
};

enum class RealtimeCertifiedReaderOpenErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidArgument,
    kUnsupportedEndian,
    kSocketPathInvalid,
    kSocketPathUnsafe,
    kSocketCreateFailed,
    kSocketConnectFailed,
    kRequestSendFailed,
    kResponseReceiveFailed,
    kProtocolError,
    kUnavailable,
    kServiceInternal,
    kDescriptorInvalid,
    kMappingFailed,
    kLayoutInvalid,
    kSessionMismatch,
    kResourceExhausted,
    kUnexpectedFailure,
};

[[nodiscard]] std::string_view
RealtimeCertifiedReaderOpenErrorNameV1(
    RealtimeCertifiedReaderOpenErrorV1 error) noexcept;

enum class RealtimeCertifiedReadResultV1 : std::uint8_t {
    kOk = 0U,
    kNoData,
    kNotYetPublished,
    kOverwritten,
    kOutOfRange,
    kInconsistent,
    kCorrupt,
};

[[nodiscard]] std::string_view
RealtimeCertifiedReadResultNameV1(
    RealtimeCertifiedReadResultV1 result) noexcept;

struct RealtimeCertifiedStatusSnapshotV1 final {
    std::uint64_t publish_tag = 0U;
    std::uint64_t heartbeat_monotonic_ns = 0U;
    std::uint64_t canonical_apply_frontier = 0U;
    std::uint64_t correction_epoch = 0U;
    std::uint64_t observed_native_message_count = 0U;
    std::uint64_t certified_tick_count = 0U;
    std::uint64_t exact_duplicate_message_count = 0U;
    std::uint64_t gap_opened_count = 0U;
    std::uint64_t gap_recovered_count = 0U;
    std::uint64_t conflicting_duplicate_count = 0U;
    std::uint64_t resource_exhaustion_count = 0U;
    std::uint64_t pending_token_count = 0U;
    RealtimeCertifiedStateV1 state =
        RealtimeCertifiedStateV1::kNoData;
    std::uint32_t channel_state_count = 0U;
    std::uint32_t gap_open_channel_count = 0U;
    std::uint32_t catching_up_channel_count = 0U;
    std::uint32_t frozen_channel_count = 0U;
};

class RealtimeCertifiedReaderV1 final {
public:
    RealtimeCertifiedReaderV1(
        const RealtimeCertifiedReaderV1&) = delete;
    RealtimeCertifiedReaderV1& operator=(
        const RealtimeCertifiedReaderV1&) = delete;
    RealtimeCertifiedReaderV1(
        RealtimeCertifiedReaderV1&&) = delete;
    RealtimeCertifiedReaderV1& operator=(
        RealtimeCertifiedReaderV1&&) = delete;
    ~RealtimeCertifiedReaderV1();

    // Connects to the same-UID SOCK_SEQPACKET endpoint, sends the independent
    // CERTIFIED GET_SESSION request, accepts exactly one SCM_RIGHTS fd on an
    // OK response, and closes that fd after a successful read-only mmap.
    [[nodiscard]] static RealtimeCertifiedReaderOpenErrorV1 Open(
        RealtimeCertifiedReaderOpenOptionsV1 options,
        std::unique_ptr<RealtimeCertifiedReaderV1>* output,
        int* system_error_number = nullptr) noexcept;

    // Test seam. descriptor is borrowed and remains owned by the caller.
    // Production-equivalent validation still requires O_RDONLY plus the
    // shrink/grow/future-write/seal memfd seals.
    [[nodiscard]] static RealtimeCertifiedReaderOpenErrorV1
    OpenDescriptorForTest(
        int descriptor,
        const RealtimeCertifiedExpectedSessionV1& expected_session,
        std::unique_ptr<RealtimeCertifiedReaderV1>* output,
        int* system_error_number = nullptr) noexcept;

    [[nodiscard]] RealtimeCertifiedReadResultV1 ReadStatus(
        RealtimeCertifiedStatusSnapshotV1* output) const noexcept;

    // ordinal is the zero-based frozen FAST V2 catalog ordinal.  NO_DATA
    // means this instrument has no certified tick yet, regardless of whether
    // the aggregate state is contiguous, gapped, catching up, or frozen.
    [[nodiscard]] RealtimeCertifiedReadResultV1 ReadLatest(
        std::size_t ordinal,
        RealtimeCertifiedTickEnvelopeV1* output) const noexcept;

    // canonical_apply_sequence is one-based and dense.  NOT_YET_PUBLISHED
    // and OVERWRITTEN are distinct normal cursor outcomes.
    [[nodiscard]] RealtimeCertifiedReadResultV1 ReadCanonical(
        std::uint64_t canonical_apply_sequence,
        RealtimeCertifiedTickEnvelopeV1* output) const noexcept;

    [[nodiscard]] RealtimeCertifiedReadResultV1 ReadChannelState(
        std::size_t row_index,
        RealtimeCertifiedChannelStateV1* output) const noexcept;

    [[nodiscard]] const RealtimeCertifiedExpectedSessionV1&
    session() const noexcept;
    [[nodiscard]] std::uint32_t latest_capacity() const noexcept;
    [[nodiscard]] std::uint32_t certified_ring_capacity()
        const noexcept;
    [[nodiscard]] std::uint32_t channel_state_capacity()
        const noexcept;

private:
    class Impl;
    explicit RealtimeCertifiedReaderV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::ipc
