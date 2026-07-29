#pragma once

#include "l2flow/realtime/owned_ingress_message_v1.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::realtime {

// A journal file is a dense sequence of independently CRC-framed records.
// There is deliberately no session header, resume contract, replay API, or
// clean-stop marker: this sink is created fresh for one normal trading-day
// process and is not an intraday recovery mechanism. Multi-byte fields are
// little-endian. CRC32C covers every byte after total_record_bytes and before
// the trailing checksum.
inline constexpr std::uint32_t kMandatoryJournalRecordMagicV2 =
    0x324a324cU;
inline constexpr std::uint16_t kMandatoryJournalRecordVersionV2 = 2U;
inline constexpr std::size_t kMandatoryJournalRecordBytesOffsetV2 = 0U;
inline constexpr std::size_t kMandatoryJournalRecordMagicOffsetV2 = 4U;
inline constexpr std::size_t kMandatoryJournalRecordVersionOffsetV2 = 8U;
inline constexpr std::size_t kMandatoryJournalRecordPrefixBytesOffsetV2 =
    10U;
inline constexpr std::size_t kMandatoryJournalRecordBodyBytesOffsetV2 = 12U;
inline constexpr std::size_t kMandatoryJournalRecordSourceSlotOffsetV2 = 16U;
inline constexpr std::size_t kMandatoryJournalRecordServiceIdOffsetV2 = 17U;
inline constexpr std::size_t
    kMandatoryJournalRecordServiceVersionOffsetV2 = 18U;
inline constexpr std::size_t kMandatoryJournalRecordMessageIdOffsetV2 = 20U;
inline constexpr std::size_t kMandatoryJournalRecordRunIdOffsetV2 = 24U;
inline constexpr std::size_t
    kMandatoryJournalRecordGlobalSequenceOffsetV2 = 40U;
inline constexpr std::size_t
    kMandatoryJournalRecordSourceSequenceOffsetV2 = 48U;
inline constexpr std::size_t
    kMandatoryJournalRecordTickSequenceOffsetV2 = 56U;
inline constexpr std::size_t
    kMandatoryJournalRecordRealtimeOffsetV2 = 64U;
inline constexpr std::size_t
    kMandatoryJournalRecordMonotonicOffsetV2 = 72U;
inline constexpr std::size_t
    kMandatoryJournalRecordVendorHeadOffsetV2 = 80U;
inline constexpr std::size_t kMandatoryJournalRecordPrefixBytesV2 = 104U;
inline constexpr std::size_t kMandatoryJournalRecordChecksumBytesV2 = 4U;

using MandatoryJournalDurableCallbackV2 = void (*)(
    void* context,
    std::uint64_t durable_sequence) noexcept;

enum class MandatoryJournalFailureKindV2 : std::uint8_t {
    kNone = 0U,
    kInvalidMessage,
    kSequence,
    kQueueFull,
    kConcurrentProducer,
    kRecordTooLarge,
    kWrite,
    kSync,
    kClose,
};

// Invoked exactly once when the writer first observes a terminal writer-side
// failure. It lets the session fail immediately without polling or waiting
// for another SDK callback. The callback runs on the writer thread and must
// not stop or destroy this Journal.
using MandatoryJournalFailureCallbackV2 = void (*)(
    void* context,
    MandatoryJournalFailureKindV2 failure_kind,
    int error_number) noexcept;

// Test-only synchronization seam. A test may block the writer immediately
// before fdatasync to prove that Journal durability is absent from the
// callback-to-realtime-publication path. Production configurations leave it
// null. The hook never runs on the callback producer.
using MandatoryJournalBeforeSyncForTestV2 = void (*)(
    void* context) noexcept;

struct MandatoryJournalConfigV2 final {
    // Always opened O_CREAT|O_EXCL as a regular 0600 file.
    std::string path;
    std::uint32_t maximum_message_bytes =
        kOwnedIngressMaximumMessageBytesV1;
    std::size_t queue_capacity = 4096U;
    std::size_t max_batch_records = 64U;
    std::chrono::microseconds max_batch_delay{
        std::chrono::microseconds(50)};

    // Optional observation invoked on the writer thread after one complete
    // batch has been fdatasync'd and durable_sequence has been
    // release-published. It must not stop or destroy this Journal.
    MandatoryJournalDurableCallbackV2 durable = nullptr;
    void* durable_context = nullptr;
    MandatoryJournalFailureCallbackV2 failure = nullptr;
    void* failure_context = nullptr;

    MandatoryJournalBeforeSyncForTestV2 before_sync_for_test = nullptr;
    void* before_sync_context_for_test = nullptr;
};

enum class MandatoryJournalCreateErrorV2 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kOpen,
    kFileStatus,
    kFileMode,
    kThreadStart,
    kResourceExhausted,
};

enum class MandatoryJournalAppendResultV2 : std::uint8_t {
    kAccepted = 0U,
    kInvalidMessage,
    kSequenceError,
    kQueueFull,
    kConcurrentProducer,
    kStopped,
    kFailed,
};

[[nodiscard]] std::string_view MandatoryJournalCreateErrorNameV2(
    MandatoryJournalCreateErrorV2 error) noexcept;
[[nodiscard]] std::string_view MandatoryJournalAppendResultNameV2(
    MandatoryJournalAppendResultV2 result) noexcept;
[[nodiscard]] std::string_view MandatoryJournalFailureKindNameV2(
    MandatoryJournalFailureKindV2 kind) noexcept;

struct MandatoryJournalSnapshotV2 final {
    std::uint64_t accepted_records = 0U;
    std::uint64_t written_records = 0U;
    std::uint64_t rejected_records = 0U;

    // Dense-prefix endpoints, never arbitrary maxima.
    std::uint64_t accepted_sequence = 0U;
    std::uint64_t written_sequence = 0U;
    std::uint64_t durable_sequence = 0U;
    std::uint64_t durability_lag_records = 0U;

    MandatoryJournalFailureKindV2 failure_kind =
        MandatoryJournalFailureKindV2::kNone;
    int error_number = 0;
    bool accepting = false;
    bool stop_requested = false;
    bool finished = false;

    [[nodiscard]] bool failed() const noexcept {
        return failure_kind != MandatoryJournalFailureKindV2::kNone;
    }
};

// Required asynchronous persistence sink. TryAppend is allocation-free,
// I/O-free, and nonblocking. Its sole callback producer must submit
// capture_sequence 1,2,3,... without gaps. Admission into this bounded queue
// is mandatory and fail-closed; write and fdatasync happen only on the writer
// thread and never gate realtime processing or Python visibility.
class MandatoryJournalV2 final {
public:
    MandatoryJournalV2(const MandatoryJournalV2&) = delete;
    MandatoryJournalV2& operator=(const MandatoryJournalV2&) = delete;
    MandatoryJournalV2(MandatoryJournalV2&&) = delete;
    MandatoryJournalV2& operator=(MandatoryJournalV2&&) = delete;
    ~MandatoryJournalV2();

    [[nodiscard]] static MandatoryJournalCreateErrorV2 Create(
        MandatoryJournalConfigV2 config,
        std::unique_ptr<MandatoryJournalV2>* output,
        int* system_error_number = nullptr) noexcept;

    [[nodiscard]] MandatoryJournalAppendResultV2 TryAppend(
        OwnedIngressMessageHandleV1 message) noexcept;

    // Idempotent lifecycle operation. The callback producer must be quiesced
    // first. It drains and synchronizes the already admitted prefix unless a
    // sticky writer failure makes that impossible.
    void StopAndDrain() noexcept;

    [[nodiscard]] MandatoryJournalSnapshotV2 Snapshot() const noexcept;

private:
    class Impl;
    explicit MandatoryJournalV2(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::realtime
