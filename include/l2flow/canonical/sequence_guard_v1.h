#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string_view>

namespace l2flow::canonical {

// One guard instance owns exactly one scope.  Connection/subscription epochs
// are deliberately absent: reconnecting does not reset a vendor or exchange
// sequence.  A caller that has an audited reset rule must create a new guard
// with a new explicit scope/sequence-epoch identity outside this type.
enum class SequenceScopeKindV1 : std::uint8_t {
    kVendorMessage = 1U,
    kShanghaiChannel = 2U,
    kShenzhenUnifiedChannel = 3U,
};

struct SequenceScopeKeyV1 final {
    SequenceScopeKindV1 kind = SequenceScopeKindV1::kVendorMessage;
    // capture_date for vendor scopes; trade_date for exchange scopes.
    std::uint32_t date = 0U;
    std::uint32_t source_stream_id = 0U;
    // Required only for a vendor scope.  Exchange scopes intentionally span
    // reconnects within their configured source/trade-date run.
    l2flow::common::Identity128 stream_day_id{};
    // Used only for SH/SZ channel scopes.  It is an opaque upstream-validated
    // code: this generic guard does not invent a zero/positive domain rule.
    std::uint32_t channel = 0U;
    // Used only for a vendor-message scope.  Their numeric domains are owned
    // by the pinned vendor schema, not guessed by this generic guard.
    std::uint8_t service_id = 0U;
    std::uint16_t message_id = 0U;

    [[nodiscard]] friend constexpr bool operator==(
        const SequenceScopeKeyV1&,
        const SequenceScopeKeyV1&) noexcept = default;
};

[[nodiscard]] SequenceScopeKeyV1 MakeVendorSequenceScopeV1(
    std::uint32_t capture_date,
    std::uint32_t source_stream_id,
    const l2flow::common::Identity128& stream_day_id,
    std::uint8_t service_id,
    std::uint16_t message_id) noexcept;

[[nodiscard]] SequenceScopeKeyV1 MakeShanghaiChannelScopeV1(
    std::uint32_t trade_date,
    std::uint32_t source_stream_id,
    std::uint32_t channel) noexcept;

// Both SZ 6.33 Order and 6.36 Transaction for one ChannelNo must use the
// same key and therefore the same SequenceGuardV1 instance.
[[nodiscard]] SequenceScopeKeyV1 MakeShenzhenUnifiedChannelScopeV1(
    std::uint32_t trade_date,
    std::uint32_t source_stream_id,
    std::uint32_t channel) noexcept;

using SequencePayloadDigestFunctionV1 = bool (*)(
    void* context,
    std::span<const std::byte> payload,
    l2flow::common::Sha256Digest* digest) noexcept;

struct SequenceGuardConfigV1 final {
    SequenceScopeKeyV1 scope{};
    // Unique accepted observations are retained for the lifetime of the
    // guard.  Neither limit causes eviction; exhaustion becomes a prepared
    // kCapacity transition that poisons the scope only when committed.
    std::uint64_t max_seen_entries = 0U;
    std::uint64_t max_seen_payload_bytes = 0U;
    // When absent, the first observation is accepted with start_unknown.
    // When present, it is the first sequence that authoritative external
    // evidence says should be observed in this scope.
    std::optional<std::uint64_t> expected_first;
    // Optional deterministic test/acceleration seam.  The context is
    // borrowed and must outlive the guard.  Digest equality is never treated
    // as exact equality; payload bytes are compared afterward.
    SequencePayloadDigestFunctionV1 digest_function = nullptr;
    void* digest_context = nullptr;
};

enum class SequenceGuardStateV1 : std::uint8_t {
    kUnseen = 0U,
    kTracking,
    kDegradedGap,
    kPoisoned,
};

enum class SequenceGuardOutcomeV1 : std::uint8_t {
    kFirst = 0U,
    kContiguous,
    kGap,
    kExactDuplicate,
    kConflict,
    kBackward,
    kPoisoned,
    kCapacity,
};

enum class SequenceGuardCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidScope,
    kInvalidLimits,
    kResourceExhausted,
};

enum class SequenceGuardPrepareErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullToken,
    kTokenStillActive,
    kDigestFailure,
    kResourceExhausted,
    kUnexpectedFailure,
};

enum class SequenceGuardCommitErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidToken,
    kWrongGuard,
    kVersionMismatch,
    kVersionExhausted,
};

[[nodiscard]] std::string_view SequenceGuardStateNameV1(
    SequenceGuardStateV1 state) noexcept;
[[nodiscard]] std::string_view SequenceGuardOutcomeNameV1(
    SequenceGuardOutcomeV1 outcome) noexcept;
[[nodiscard]] std::string_view SequenceGuardCreateErrorNameV1(
    SequenceGuardCreateErrorV1 error) noexcept;
[[nodiscard]] std::string_view SequenceGuardPrepareErrorNameV1(
    SequenceGuardPrepareErrorV1 error) noexcept;
[[nodiscard]] std::string_view SequenceGuardCommitErrorNameV1(
    SequenceGuardCommitErrorV1 error) noexcept;

struct SequenceGuardSnapshotV1 final {
    SequenceScopeKeyV1 scope{};
    SequenceGuardStateV1 state = SequenceGuardStateV1::kUnseen;
    std::uint64_t version = 0U;
    std::uint64_t seen_entries = 0U;
    std::uint64_t seen_payload_bytes = 0U;
    std::uint64_t high_sequence = 0U;
    bool has_high_sequence = false;
    // Sticky for a run whose prefix was not authoritatively observed.
    bool start_unknown = false;
};

struct SequenceGuardPrepareResultV1 final {
    SequenceGuardPrepareErrorV1 error =
        SequenceGuardPrepareErrorV1::kNone;
    SequenceGuardOutcomeV1 outcome = SequenceGuardOutcomeV1::kPoisoned;
    SequenceGuardStateV1 state_before = SequenceGuardStateV1::kUnseen;
    std::uint64_t sequence = 0U;
    std::uint64_t high_before = 0U;
    bool had_high_before = false;
    // Preserves the authoritative day-start expectation independently of
    // observed high-water state.  In particular, the first observation may
    // be below expected_first, so high_before is legitimately absent while
    // a backward diagnostic still needs the exact expected value.
    std::uint64_t configured_first = 0U;
    bool had_configured_first = false;
    bool start_unknown_after_commit = false;
    // A gap is the exact half-open interval [missing_begin, missing_end).
    // It is populated only for kGap and never relies on wrapping high+1.
    std::uint64_t missing_begin = 0U;
    std::uint64_t missing_end = 0U;
    bool has_missing_interval = false;
    l2flow::common::Sha256Digest payload_digest{};
    l2flow::common::Sha256Digest prior_payload_digest{};
    bool prior_payload_digest_present = false;
    bool token_prepared = false;

    [[nodiscard]] bool ok() const noexcept {
        return error == SequenceGuardPrepareErrorV1::kNone;
    }
};

namespace sequence_guard_detail {
struct PendingSeenEntryV1;
}  // namespace sequence_guard_detail

// Move-only transaction token.  Preparing a token does not change the guard's
// logical sequence state.  Commit validates both owner and version; Abort
// destroys the prepared payload and leaves the guard byte-for-byte logical
// state unchanged.
class SequenceGuardTokenV1 final {
public:
    SequenceGuardTokenV1() noexcept;
    SequenceGuardTokenV1(const SequenceGuardTokenV1&) = delete;
    SequenceGuardTokenV1& operator=(const SequenceGuardTokenV1&) = delete;
    SequenceGuardTokenV1(SequenceGuardTokenV1&& other) noexcept;
    SequenceGuardTokenV1& operator=(
        SequenceGuardTokenV1&& other) noexcept;
    ~SequenceGuardTokenV1();

    [[nodiscard]] bool active() const noexcept {
        return owner_ != nullptr;
    }
    [[nodiscard]] SequenceGuardOutcomeV1 outcome() const noexcept {
        return outcome_;
    }
    [[nodiscard]] std::uint64_t sequence() const noexcept {
        return sequence_;
    }

private:
    friend class SequenceGuardV1;

    void Reset() noexcept;

    const void* owner_ = nullptr;
    std::uint64_t expected_version_ = 0U;
    std::uint64_t sequence_ = 0U;
    SequenceGuardOutcomeV1 outcome_ =
        SequenceGuardOutcomeV1::kPoisoned;
    bool start_unknown_after_commit_ = false;
    std::uint64_t missing_begin_ = 0U;
    std::uint64_t missing_end_ = 0U;
    std::unique_ptr<sequence_guard_detail::PendingSeenEntryV1>
        pending_entry_;
};

// Single-writer state machine.  Concurrent calls, including concurrent
// Prepare calls, are outside the contract.  Multiple outstanding tokens on
// the one writer are allowed, but only a token prepared against the current
// version can commit.
class SequenceGuardV1 final {
public:
    SequenceGuardV1(const SequenceGuardV1&) = delete;
    SequenceGuardV1& operator=(const SequenceGuardV1&) = delete;
    SequenceGuardV1(SequenceGuardV1&&) = delete;
    SequenceGuardV1& operator=(SequenceGuardV1&&) = delete;
    ~SequenceGuardV1();

    [[nodiscard]] static SequenceGuardCreateErrorV1 Create(
        SequenceGuardConfigV1 config,
        std::unique_ptr<SequenceGuardV1>* output) noexcept;

    [[nodiscard]] SequenceGuardPrepareResultV1 Prepare(
        std::uint64_t sequence,
        std::span<const std::byte> exact_payload,
        SequenceGuardTokenV1* token) noexcept;

    [[nodiscard]] SequenceGuardCommitErrorV1 Commit(
        SequenceGuardTokenV1* token) noexcept;

    // Returns false for a null, inactive, or foreign token.  A stale token
    // from this guard can always be aborted.
    [[nodiscard]] bool Abort(SequenceGuardTokenV1* token) noexcept;

    [[nodiscard]] SequenceGuardSnapshotV1 Snapshot() const noexcept;
    [[nodiscard]] const SequenceGuardConfigV1& config() const noexcept;

private:
    class Impl;
    explicit SequenceGuardV1(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::canonical
