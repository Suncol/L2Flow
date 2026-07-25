#pragma once

#include "l2flow/realtime/owned_ingress_message_v1.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::realtime {

// Every WAL record is independently framed. Multi-byte integers use little
// endian encoding. The total record length is followed by magic/version,
// immutable ingress metadata, the 23-byte vendor head, the body, and a CRC32C
// over every byte after total_record_bytes and before the checksum.
inline constexpr std::uint32_t kOptionalWalRecordMagicV1 = 0x574f324cU;
inline constexpr std::uint16_t kOptionalWalRecordVersionV1 = 1U;
inline constexpr std::size_t kOptionalWalRecordPrefixBytesV1 = 91U;
inline constexpr std::size_t kOptionalWalRecordChecksumBytesV1 = 4U;

struct OptionalWalSinkConfigV1 final {
    bool enabled = false;
    std::string path;
    std::size_t queue_capacity = 0U;

    // The default refuses to overwrite an existing path. Replacement is an
    // explicit deployment choice and truncates the path before the worker is
    // started.
    bool replace_existing = false;
    // fdatasync is performed only during clean StopAndDrain. WAL durability
    // is observational and never gates decoder/history/factor progress.
    bool sync_on_stop = true;
};

enum class OptionalWalCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kResourceExhausted,
};

enum class OptionalWalEnqueueResultV1 : std::uint8_t {
    kAccepted = 0U,
    kDisabled,
    kInvalidMessage,
    kQueueFull,
    kConcurrentProducer,
    kStopped,
    kWriterFailed,
};

enum class OptionalWalFailureKindV1 : std::uint8_t {
    kNone = 0U,
    kOpen,
    kNotRegularFile,
    kThreadStart,
    kRecordTooLarge,
    kQueueCounterExhausted,
    kWrite,
    kSync,
    kClose,
};

[[nodiscard]] std::string_view OptionalWalCreateErrorNameV1(
    OptionalWalCreateErrorV1 error) noexcept;
[[nodiscard]] std::string_view OptionalWalEnqueueResultNameV1(
    OptionalWalEnqueueResultV1 result) noexcept;
[[nodiscard]] std::string_view OptionalWalFailureKindNameV1(
    OptionalWalFailureKindV1 kind) noexcept;

struct OptionalWalSnapshotV1 final {
    std::uint64_t accepted_records = 0U;
    std::uint64_t written_records = 0U;
    std::uint64_t rejected_records = 0U;
    std::uint64_t abandoned_records = 0U;
    OptionalWalFailureKindV1 failure_kind =
        OptionalWalFailureKindV1::kNone;
    int error_number = 0;
    bool enabled = false;
    bool accepting = false;
    bool stop_requested = false;
    bool finished = false;
    bool coverage_lost = false;

    [[nodiscard]] bool failed() const noexcept {
        return failure_kind != OptionalWalFailureKindV1::kNone;
    }
};

// Optional, best-effort side branch. TryEnqueue transfers one intrusive
// reference to the same immutable pooled message used by the decoder; it
// performs no body copy, file I/O, or payload allocation and waits for no
// condition variable/mutex. When disabled, the transferred reference is
// released immediately, so the decoder may become the unique holder.
// Exactly one serialized callback producer is expected; concurrent producer
// entry is rejected instead of blocking. Queue pressure or writer failure is
// sticky coverage loss but cannot change realtime decoder/history/factor
// state.
class OptionalWalSinkV1 final {
public:
    OptionalWalSinkV1(const OptionalWalSinkV1&) = delete;
    OptionalWalSinkV1& operator=(const OptionalWalSinkV1&) = delete;
    OptionalWalSinkV1(OptionalWalSinkV1&&) = delete;
    OptionalWalSinkV1& operator=(OptionalWalSinkV1&&) = delete;
    ~OptionalWalSinkV1();

    // An unavailable path or writer-thread start failure is represented by a
    // successfully created sink with sticky failure/coverage_lost. This keeps
    // optional WAL availability out of the production realtime admission
    // decision. Only malformed configuration or object allocation fails
    // Create itself.
    [[nodiscard]] static OptionalWalCreateErrorV1 Create(
        OptionalWalSinkConfigV1 config,
        std::unique_ptr<OptionalWalSinkV1>* output) noexcept;

    [[nodiscard]] OptionalWalEnqueueResultV1 TryEnqueue(
        OwnedIngressMessageHandleV1 message) noexcept;

    // Idempotent. The owner must first quiesce the SDK callback producer; this
    // mirrors the SDK Shutdown -> callback quiescence -> side-branch drain
    // lifecycle and makes the final Snapshot stable. StopAndDrain then closes
    // admission, drains every already accepted handle, performs the optional
    // sync, closes the file, and joins the independent writer thread. A late
    // kStopped call is outside the capture interval and does not by itself
    // mark WAL coverage lost.
    void StopAndDrain() noexcept;

    [[nodiscard]] OptionalWalSnapshotV1 Snapshot() const noexcept;

private:
    class Impl;
    explicit OptionalWalSinkV1(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::realtime
