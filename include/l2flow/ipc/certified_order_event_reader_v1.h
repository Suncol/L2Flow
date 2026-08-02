#pragma once

#include "l2flow/ipc/certified_order_event_wire_v1.h"
#include "l2flow/ipc/realtime_certified_reader_v1.h"

#include <cstdint>
#include <memory>
#include <span>
#include <string_view>

namespace l2flow::ipc {

enum class CertifiedOrderEventReaderOpenErrorV1
    : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidArgument,
    kTickReaderOpenFailed,
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

enum class CertifiedOrderEventReadResultV1 : std::uint8_t {
    kOk = 0U,
    kNoData,
    kNotYetPublished,
    kOutOfRange,
    kInconsistent,
    kCorrupt,
};

[[nodiscard]] std::string_view
CertifiedOrderEventReaderOpenErrorNameV1(
    CertifiedOrderEventReaderOpenErrorV1 error) noexcept;
[[nodiscard]] std::string_view
CertifiedOrderEventReadResultNameV1(
    CertifiedOrderEventReadResultV1 result) noexcept;

struct CertifiedOrderEventStatusSnapshotV1 final {
    RealtimeCertifiedStatusSnapshotV1 tick{};
    std::uint32_t coverage_flags = 0U;
    std::uint64_t event_publish_tag = 0U;
    std::uint64_t event_heartbeat_monotonic_ns = 0U;
    std::uint64_t event_canonical_apply_frontier = 0U;
    std::uint64_t event_published_sequence = 0U;
    std::uint64_t event_generation = 0U;
    std::uint64_t shanghai_order_state_count = 0U;
    std::uint64_t shenzhen_order_state_count = 0U;
    std::uint64_t committed_mapping_bytes = 0U;

    // The sole consumer-visible cut. Event may be physically ahead because
    // its append is intentionally fallible and precedes Tick slot overwrite;
    // such rows remain hidden until the Tick header reaches the same input.
    std::uint64_t coherent_canonical_apply_frontier = 0U;

    [[nodiscard]] bool coverage_from_open() const noexcept {
        return (coverage_flags &
                kCertifiedOrderEventCoverageFromOpenV1) != 0U;
    }
    [[nodiscard]] bool startup_prefix_recovered() const noexcept {
        return (coverage_flags &
                kCertifiedOrderEventStartupPrefixRecoveredV1) != 0U;
    }
};

struct CertifiedOrderEventReadBatchResultV1 final {
    std::size_t written = 0U;
    std::uint64_t next_event_sequence = 0U;
    CertifiedOrderEventStatusSnapshotV1 status{};
};

class CertifiedOrderEventReaderV1 final {
public:
    CertifiedOrderEventReaderV1(
        const CertifiedOrderEventReaderV1&) = delete;
    CertifiedOrderEventReaderV1& operator=(
        const CertifiedOrderEventReaderV1&) = delete;
    CertifiedOrderEventReaderV1(
        CertifiedOrderEventReaderV1&&) = delete;
    CertifiedOrderEventReaderV1& operator=(
        CertifiedOrderEventReaderV1&&) = delete;
    ~CertifiedOrderEventReaderV1();

    // Opens both independent mappings from the same authenticated service
    // endpoint. kGetSession remains the Tick request; a second, additive
    // kGetEventHistory request obtains the Event fd.
    [[nodiscard]] static CertifiedOrderEventReaderOpenErrorV1 Open(
        RealtimeCertifiedReaderOpenOptionsV1 options,
        std::unique_ptr<CertifiedOrderEventReaderV1>* output,
        int* system_error_number = nullptr) noexcept;

    // Both descriptors are borrowed. Production-equivalent read-only access,
    // seal, layout, and exact run/session/day checks still apply.
    [[nodiscard]] static CertifiedOrderEventReaderOpenErrorV1
    OpenDescriptorsForTest(
        int tick_descriptor,
        int event_descriptor,
        const RealtimeCertifiedExpectedSessionV1& expected_session,
        std::unique_ptr<CertifiedOrderEventReaderV1>* output,
        int* system_error_number = nullptr) noexcept;

    [[nodiscard]] CertifiedOrderEventReadResultV1 ReadStatus(
        CertifiedOrderEventStatusSnapshotV1* output) const noexcept;

    // derived_event_sequence is one-based and append-only for the whole day.
    // A physically published row whose source canonical sequence is above the
    // coherent cut returns NOT_YET_PUBLISHED, never OK.
    [[nodiscard]] CertifiedOrderEventReadResultV1 ReadEvent(
        std::uint64_t derived_event_sequence,
        CertifiedOrderEventEnvelopeV1* output) const noexcept;

    // Serial cursor helper. expected_event_sequence is one-based; a
    // successful zero-row result means no further row is visible at the
    // captured coherent cut.
    [[nodiscard]] CertifiedOrderEventReadResultV1 Read(
        std::uint64_t expected_event_sequence,
        std::span<CertifiedOrderEventEnvelopeV1> output,
        CertifiedOrderEventReadBatchResultV1* result) const noexcept;

    [[nodiscard]] const RealtimeCertifiedExpectedSessionV1&
    session() const noexcept;
    [[nodiscard]] std::uint64_t event_capacity() const noexcept;

private:
    class Impl;
    explicit CertifiedOrderEventReaderV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::ipc
