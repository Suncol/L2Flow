#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/ipc/partial_order_event_journal_v2.h"

#include <array>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string_view>
#include <type_traits>

namespace l2flow::ipc {

inline constexpr std::array<std::uint8_t, 8U>
    kPartialEventBrokerRequestMagicV2{
        'L', '2', 'F', 'P', 'B', 'R', 'Q', '2'};
inline constexpr std::array<std::uint8_t, 8U>
    kPartialEventBrokerResponseMagicV2{
        'L', '2', 'F', 'P', 'B', 'R', 'S', '2'};
inline constexpr std::array<std::uint8_t, 8U>
    kPartialEventHandoffRequestMagicV2{
        'L', '2', 'F', 'P', 'B', 'H', 'Q', '2'};
inline constexpr std::array<std::uint8_t, 8U>
    kPartialEventHandoffResponseMagicV2{
        'L', '2', 'F', 'P', 'B', 'H', 'S', '2'};
inline constexpr std::uint16_t kPartialEventBrokerProtocolMajorV2 = 2U;
inline constexpr std::uint16_t kPartialEventBrokerProtocolMinorV2 = 1U;
inline constexpr std::array<std::uint8_t, 8U>
    kPartialEventBrokerLifecycleMagicV2{
        'L', '2', 'F', 'P', 'B', 'L', 'C', '2'};
inline constexpr std::uint32_t kPartialEventBrokerLifecycleBytesV2 = 128U;

enum class PartialEventBrokerResultV2 : std::uint32_t {
    kOk = 0U,
    kUnavailable,
    kProtocolError,
    kPeerRejected,
    kSessionMismatch,
    kGenerationRejected,
    kDescriptorRejected,
    kContinuityRejected,
    kInternalError,
};

enum class PartialEventBrokerStateV2 : std::uint32_t {
    kUnavailable = 1U,
    kReady,
    kStale,
    kRestarting,
    kStoppedClean,
};

[[nodiscard]] std::string_view PartialEventBrokerResultNameV2(
    PartialEventBrokerResultV2 result) noexcept;
[[nodiscard]] std::string_view PartialEventBrokerStateNameV2(
    PartialEventBrokerStateV2 state) noexcept;

struct PartialEventBrokerRequestV2 final {
    std::array<std::uint8_t, 8U> magic =
        kPartialEventBrokerRequestMagicV2;
    std::uint16_t protocol_major =
        kPartialEventBrokerProtocolMajorV2;
    std::uint16_t protocol_minor =
        kPartialEventBrokerProtocolMinorV2;
    std::uint32_t request_bytes = 128U;
    std::array<std::uint8_t, 16U> run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint64_t minimum_publication_generation = 0U;
    std::uint32_t trade_date = 0U;
    std::uint32_t reserved0 = 0U;
    std::array<std::uint8_t, 72U> reserved{};
};
static_assert(sizeof(PartialEventBrokerRequestV2) == 128U);
static_assert(std::is_trivially_copyable_v<PartialEventBrokerRequestV2>);

// broker_state and broker_stale describe worker lifecycle only. The mapping's
// own gap/reorder status remains authoritative and is read from the returned
// descriptor with PartialOrderEventReaderV2.
struct PartialEventBrokerResponseV2 final {
    std::array<std::uint8_t, 8U> magic =
        kPartialEventBrokerResponseMagicV2;
    std::uint16_t protocol_major =
        kPartialEventBrokerProtocolMajorV2;
    std::uint16_t protocol_minor =
        kPartialEventBrokerProtocolMinorV2;
    std::uint32_t response_bytes = 128U;
    PartialEventBrokerResultV2 result =
        PartialEventBrokerResultV2::kInternalError;
    PartialEventBrokerStateV2 broker_state =
        PartialEventBrokerStateV2::kUnavailable;
    std::uint32_t broker_stale = 1U;
    // A successful V2.1 public response carries the journal descriptor first
    // and the broker lifecycle descriptor second.
    std::uint32_t descriptor_count = 0U;
    std::array<std::uint8_t, 16U> run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint64_t publication_generation = 0U;
    std::uint64_t correction_epoch = 0U;
    std::uint64_t adopted_commit_sequence = 0U;
    std::uint64_t adopted_canonical_frontier = 0U;
    std::uint64_t adopted_event_frontier = 0U;
    std::uint32_t trade_date = 0U;
    PartialOrderEventOrderingQualityV2 ordering_quality =
        PartialOrderEventOrderingQualityV2::kBoundedReorderedPartial;
    std::array<std::uint8_t, 24U> reserved{};
};
static_assert(sizeof(PartialEventBrokerResponseV2) == 128U);
static_assert(std::is_trivially_copyable_v<PartialEventBrokerResponseV2>);

// This page is independent from the Event journal. The router-owned broker is
// its only writer, while every client maps an O_RDONLY descriptor. The async
// failure field is intentionally outside the commit-tag transaction so the
// Event failure callback can fail closed with one lock-free release store.
struct alignas(64) PartialEventBrokerLifecyclePageV2 final {
    std::array<std::uint8_t, 8U> magic =
        kPartialEventBrokerLifecycleMagicV2;
    std::uint16_t protocol_major = kPartialEventBrokerProtocolMajorV2;
    std::uint16_t protocol_minor = kPartialEventBrokerProtocolMinorV2;
    std::uint32_t page_bytes = kPartialEventBrokerLifecycleBytesV2;
    std::array<std::uint8_t, 16U> run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
    std::uint32_t reserved0 = 0U;
    std::uint64_t commit_tag = 0U;
    std::uint64_t heartbeat_monotonic_ns = 0U;
    std::uint64_t heartbeat_timeout_ns = 0U;
    std::uint64_t lifecycle_epoch = 0U;
    std::uint64_t publication_generation = 0U;
    std::uint64_t correction_epoch = 0U;
    std::int64_t expected_worker = -1;
    std::uint64_t asynchronously_failed_lease_epoch = 0U;
    std::int64_t broker_pid = -1;
    PartialEventBrokerStateV2 broker_state =
        PartialEventBrokerStateV2::kUnavailable;
    std::uint32_t broker_stale = 1U;
};
static_assert(
    sizeof(PartialEventBrokerLifecyclePageV2) ==
    kPartialEventBrokerLifecycleBytesV2);
static_assert(
    alignof(PartialEventBrokerLifecyclePageV2) == 64U);
static_assert(
    std::is_trivially_copyable_v<PartialEventBrokerLifecyclePageV2>);

struct PartialEventBrokerLifecycleSnapshotV2 final {
    std::uint64_t heartbeat_monotonic_ns = 0U;
    std::uint64_t heartbeat_timeout_ns = 0U;
    std::uint64_t lifecycle_epoch = 0U;
    std::uint64_t publication_generation = 0U;
    std::uint64_t correction_epoch = 0U;
    std::int64_t expected_worker = -1;
    std::uint64_t asynchronously_failed_lease_epoch = 0U;
    std::int64_t broker_pid = -1;
    PartialEventBrokerStateV2 broker_state =
        PartialEventBrokerStateV2::kUnavailable;
    bool broker_stale = true;
    bool heartbeat_expired = true;
};

enum class PartialEventBrokerLifecycleOpenErrorV2 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidDescriptor,
    kDescriptorStatFailed,
    kDescriptorSealMismatch,
    kMappingFailed,
    kIncompatibleLayout,
    kSessionMismatch,
    kNoStableSnapshot,
    kUnexpectedFailure,
};

enum class PartialEventBrokerLifecycleReadResultV2 : std::uint8_t {
    kOk = 0U,
    kInvalidArgument,
    kInconsistent,
    kCorrupt,
};

class PartialEventBrokerLifecycleReaderV2 final {
public:
    PartialEventBrokerLifecycleReaderV2(
        const PartialEventBrokerLifecycleReaderV2&) = delete;
    PartialEventBrokerLifecycleReaderV2& operator=(
        const PartialEventBrokerLifecycleReaderV2&) = delete;
    PartialEventBrokerLifecycleReaderV2(
        PartialEventBrokerLifecycleReaderV2&&) = delete;
    PartialEventBrokerLifecycleReaderV2& operator=(
        PartialEventBrokerLifecycleReaderV2&&) = delete;
    ~PartialEventBrokerLifecycleReaderV2();

    [[nodiscard]] static PartialEventBrokerLifecycleOpenErrorV2
    OpenDescriptor(
        int descriptor,
        const common::Identity128& expected_run_id,
        std::uint64_t expected_session_epoch,
        std::uint32_t expected_trade_date,
        std::unique_ptr<PartialEventBrokerLifecycleReaderV2>* output,
        int* system_error_number = nullptr) noexcept;

    [[nodiscard]] PartialEventBrokerLifecycleReadResultV2 ReadSnapshot(
        PartialEventBrokerLifecycleSnapshotV2* output,
        bool evaluate_heartbeat = true) const noexcept;

private:
    class Impl;
    explicit PartialEventBrokerLifecycleReaderV2(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

struct PartialEventHandoffRequestV2 final {
    std::array<std::uint8_t, 8U> magic =
        kPartialEventHandoffRequestMagicV2;
    std::uint16_t protocol_major =
        kPartialEventBrokerProtocolMajorV2;
    std::uint16_t protocol_minor =
        kPartialEventBrokerProtocolMinorV2;
    std::uint32_t request_bytes = 128U;
    std::array<std::uint8_t, 16U> run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint64_t publication_generation = 0U;
    std::uint64_t correction_epoch = 0U;
    std::uint64_t coverage_start_unix_ns = 0U;
    std::uint64_t event_capacity = 0U;
    std::uint64_t order_state_capacity = 0U;
    std::uint32_t trade_date = 0U;
    PartialOrderEventOrderingQualityV2 ordering_quality =
        PartialOrderEventOrderingQualityV2::kBoundedReorderedPartial;
    std::uint32_t affected_channel_capacity = 0U;
    std::uint32_t reserved0 = 0U;
    std::uint64_t total_mapping_bytes = 0U;
    std::array<std::uint8_t, 24U> reserved{};
};
static_assert(sizeof(PartialEventHandoffRequestV2) == 128U);
static_assert(std::is_trivially_copyable_v<PartialEventHandoffRequestV2>);

struct PartialEventHandoffResponseV2 final {
    std::array<std::uint8_t, 8U> magic =
        kPartialEventHandoffResponseMagicV2;
    std::uint16_t protocol_major =
        kPartialEventBrokerProtocolMajorV2;
    std::uint16_t protocol_minor =
        kPartialEventBrokerProtocolMinorV2;
    std::uint32_t response_bytes = 64U;
    PartialEventBrokerResultV2 result =
        PartialEventBrokerResultV2::kInternalError;
    std::uint32_t reserved0 = 0U;
    std::uint64_t accepted_publication_generation = 0U;
    std::uint64_t accepted_correction_epoch = 0U;
    std::array<std::uint8_t, 24U> reserved{};
};
static_assert(sizeof(PartialEventHandoffResponseV2) == 64U);
static_assert(std::is_trivially_copyable_v<PartialEventHandoffResponseV2>);

// Worker-side helper. descriptor must be an O_RDONLY journal descriptor.
[[nodiscard]] PartialEventBrokerResultV2 SubmitPartialEventGenerationV2(
    const std::filesystem::path& worker_handoff_socket_path,
    int descriptor,
    const PartialOrderEventJournalSessionV2& session,
    std::chrono::milliseconds timeout,
    PartialEventHandoffResponseV2* response = nullptr,
    int* system_error_number = nullptr) noexcept;

// Client-side helper. On kOk it returns two owned O_RDONLY descriptors: the
// Event journal and the independent broker lifecycle page. On any other result
// both outputs remain -1.
[[nodiscard]] PartialEventBrokerResultV2 RequestPartialEventGenerationV2(
    const std::filesystem::path& public_socket_path,
    const common::Identity128& expected_run_id,
    std::uint64_t expected_session_epoch,
    std::uint32_t expected_trade_date,
    std::uint64_t minimum_publication_generation,
    std::chrono::milliseconds timeout,
    int* output_descriptor,
    int* output_lifecycle_descriptor,
    PartialEventBrokerResponseV2* response = nullptr,
    int* system_error_number = nullptr) noexcept;

}  // namespace l2flow::ipc
