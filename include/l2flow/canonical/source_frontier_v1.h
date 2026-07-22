#pragma once

#include "l2flow/canonical/canonical_schema_v1.h"
#include "l2flow/common/identity128.h"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>

namespace l2flow::canonical {

inline constexpr std::uint32_t kSourceFrontierPageMagicV1 =
    0x31465253U;  // "SRF1" as little-endian bytes.
inline constexpr std::uint16_t kSourceFrontierPageVersionV1 = 1U;
inline constexpr std::size_t kSourceFrontierPageBytesV1 = 4096U;
inline constexpr std::chrono::nanoseconds
    kSourceFrontierDefaultBusyTimeoutV1 = std::chrono::milliseconds{100};
inline constexpr std::chrono::nanoseconds
    kSourceFrontierMaximumBusyTimeoutV1 = std::chrono::seconds{10};

enum class SourceStateV1 : std::uint32_t {
    kRecovering = 0U,
    kHealthy = 1U,
    kDisconnected = 2U,
    kFatal = 3U,
};

// Stable semantic snapshot.  safe_processed_frontier_ns is an exclusive time
// boundary: it proves that any not-yet-observed callback has receive time >=
// the boundary.  Consequently an event at exactly the boundary is not safe by
// frontier proof alone.
struct SourceFrontierV1 final {
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    l2flow::common::Identity128 stream_day_id{};
    ClockEpochIdentityV1 clock_epoch{};
    l2flow::common::Identity128 writer_instance{};
    std::uint64_t generation = 0U;
    std::uint64_t captured_ingress_sequence = 0U;
    std::uint64_t append_ingress_sequence = 0U;
    std::uint64_t processed_ingress_sequence = 0U;
    std::uint64_t append_global_wal_pos = 0U;
    std::uint64_t processed_global_wal_pos = 0U;
    std::int64_t last_appended_recv_monotonic_ns = 0;
    std::int64_t safe_processed_frontier_ns = 0;
    std::uint64_t callback_inflight = 0U;
    // Monotonic transition counter.  Callback entry increments it before the
    // callback may sample receive time; completion increments it after
    // captured progress is published and before inflight is decremented.
    // Readers compare a before/after value to defeat 0->1->0 ABA.
    std::uint64_t callback_generation = 0U;
    // Seqlock generation covering source state plus append/processed/time
    // progress.  Stable snapshots always carry an even value.
    std::uint64_t progress_generation = 0U;
    SourceStateV1 source_state = SourceStateV1::kRecovering;
    std::uint64_t quality_flags = 0U;

    [[nodiscard]] friend constexpr bool operator==(
        const SourceFrontierV1&,
        const SourceFrontierV1&) noexcept = default;
};

struct SourceFrontierConfigV1 final {
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    l2flow::common::Identity128 stream_day_id{};
    ClockEpochIdentityV1 clock_epoch{};
    l2flow::common::Identity128 writer_instance{};
    std::uint64_t generation = 0U;
    std::uint64_t initial_ingress_sequence = 0U;
    std::uint64_t initial_global_wal_pos = 0U;
    SourceStateV1 initial_state = SourceStateV1::kRecovering;
    std::uint64_t initial_quality_flags = 0U;
};

// Opaque fixed shared page.  All concurrent fields are accessed exclusively
// through the helpers below, which use lock-free __atomic operations.  The
// byte layout is frozen in source_frontier_v1.cpp and covered by tests; no
// process may reinterpret it as std::atomic<T>.
struct alignas(64) SourceFrontierPageV1 final {
    std::array<std::byte, kSourceFrontierPageBytesV1> bytes{};
};
static_assert(sizeof(SourceFrontierPageV1) == 4096U);
static_assert(alignof(SourceFrontierPageV1) == 64U);

enum class SourceFrontierErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullArgument,
    kUnsupportedHost,
    kInvalidConfiguration,
    kAlreadyInitialized,
    kInvalidPage,
    kIdentityChanged,
    kInvalidState,
    kCounterOverflow,
    kIngressRegression,
    kWalRegression,
    kReceiveTimeRegression,
    kNotCaptured,
    kNotAppended,
    // The page identity is unchanged, but a bounded stable-read or progress
    // writer acquisition window expired while another writer owned it.
    // Callers may retry; they must not reinterpret this as identity drift.
    kBusy,
};

[[nodiscard]] const char* SourceFrontierErrorNameV1(
    SourceFrontierErrorV1 error) noexcept;

[[nodiscard]] bool SourceFrontierAtomicsLockFreeV1() noexcept;

[[nodiscard]] SourceFrontierErrorV1 InitializeSourceFrontierPageV1(
    const SourceFrontierConfigV1& config,
    SourceFrontierPageV1* page) noexcept;

[[nodiscard]] SourceFrontierErrorV1 ReadSourceFrontierV1(
    const SourceFrontierPageV1& page,
    SourceFrontierV1* output) noexcept;

[[nodiscard]] SourceFrontierErrorV1 PublishSourceStateV1(
    SourceFrontierPageV1* page,
    const l2flow::common::Identity128& expected_writer_instance,
    std::uint64_t expected_generation,
    SourceStateV1 state,
    std::uint64_t quality_flags) noexcept;

// Callback gate.  Construction increments callback_inflight before the caller
// may sample receive time.  CompleteCaptured publishes the captured Raw
// ingress sequence and then leaves the gate.  Destruction without completion
// fail-stops the source, preventing a dropped callback from looking idle.
class SourceFrontierCallbackGuardV1 final {
public:
    SourceFrontierCallbackGuardV1(
        SourceFrontierPageV1* page,
        const l2flow::common::Identity128& expected_writer_instance,
        std::uint64_t expected_generation,
        std::chrono::nanoseconds busy_timeout =
            kSourceFrontierDefaultBusyTimeoutV1) noexcept;
    ~SourceFrontierCallbackGuardV1();

    SourceFrontierCallbackGuardV1(
        const SourceFrontierCallbackGuardV1&) = delete;
    SourceFrontierCallbackGuardV1& operator=(
        const SourceFrontierCallbackGuardV1&) = delete;
    SourceFrontierCallbackGuardV1(
        SourceFrontierCallbackGuardV1&&) = delete;
    SourceFrontierCallbackGuardV1& operator=(
        SourceFrontierCallbackGuardV1&&) = delete;

    [[nodiscard]] SourceFrontierErrorV1 error() const noexcept {
        return error_;
    }
    [[nodiscard]] bool entered() const noexcept {
        return error_ == SourceFrontierErrorV1::kNone && entered_;
    }

    [[nodiscard]] SourceFrontierErrorV1 CompleteCaptured(
        std::uint64_t ingress_sequence) noexcept;

private:
    SourceFrontierPageV1* page_ = nullptr;
    l2flow::common::Identity128 expected_writer_instance_{};
    std::uint64_t expected_generation_ = 0U;
    std::chrono::nanoseconds busy_timeout_ =
        kSourceFrontierDefaultBusyTimeoutV1;
    SourceFrontierErrorV1 error_ =
        SourceFrontierErrorV1::kNullArgument;
    bool entered_ = false;
    bool completed_ = false;
};

[[nodiscard]] SourceFrontierErrorV1 PublishAppendProgressV1(
    SourceFrontierPageV1* page,
    const l2flow::common::Identity128& expected_writer_instance,
    std::uint64_t expected_generation,
    std::uint64_t ingress_sequence,
    std::uint64_t global_wal_pos,
    std::int64_t last_recv_monotonic_ns) noexcept;

// Must be called only after every Canonical/Quality/Control output caused by
// this Raw prefix has been release-published.  Progress may advance WAL with
// the same ingress sequence for a segment-header/epoch boundary.
[[nodiscard]] SourceFrontierErrorV1 PublishProcessedProgressV1(
    SourceFrontierPageV1* page,
    const l2flow::common::Identity128& expected_writer_instance,
    std::uint64_t expected_generation,
    std::uint64_t ingress_sequence,
    std::uint64_t global_wal_pos,
    std::int64_t last_processed_recv_monotonic_ns) noexcept;

[[nodiscard]] bool SourceFrontierCaughtUpV1(
    const SourceFrontierV1& value) noexcept;

enum class IdleFrontierResultV1 : std::uint8_t {
    kPublished = 0U,
    kNoAdvance,
    kNotCaughtUp,
    kCallbackInflight,
    kSourceUnhealthy,
    kObservationChanged,
    kClockRegression,
    kInvalidPage,
};

// The caller performs exactly: acquire read first; sample
// CLOCK_MONOTONIC_RAW; acquire read second; then call this function.  Keeping
// the observations explicit makes every interleaving model-testable.
[[nodiscard]] IdleFrontierResultV1 TryPublishIdleFrontierV1(
    SourceFrontierPageV1* page,
    const SourceFrontierV1& first,
    const SourceFrontierV1& second,
    std::int64_t sampled_monotonic_raw_ns) noexcept;

}  // namespace l2flow::canonical
