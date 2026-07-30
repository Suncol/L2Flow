#pragma once

#include "l2flow/sdk/market_message_catalog_v1.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <string_view>

namespace l2flow::realtime {

// Exchange-native sequence domains carried by the three production tick
// tuples. Shanghai 4.101.24 is ordered by (Channel, BizIndex). Shenzhen
// 6.101.33 and 6.101.36 deliberately share one
// (ChannelNo, ApplSeqNum) domain.
enum class NativeSequenceMarketV1 : std::uint8_t {
    kShanghai = 1U,
    kShenzhen = 2U,
};

struct NativeSequenceChannelV1 final {
    NativeSequenceMarketV1 market =
        NativeSequenceMarketV1::kShanghai;
    std::uint32_t channel = 0U;

    friend constexpr bool operator==(
        const NativeSequenceChannelV1&,
        const NativeSequenceChannelV1&) = default;
};

struct NativeSequenceDescriptorV1 final {
    NativeSequenceChannelV1 domain{};
    std::uint64_t sequence = 0U;

    friend constexpr bool operator==(
        const NativeSequenceDescriptorV1&,
        const NativeSequenceDescriptorV1&) = default;
};

enum class NativeSequenceExtractErrorV1 : std::uint8_t {
    kNone = 0U,
    // The message is one of the two production snapshot tuples and therefore
    // has no exchange-native tick sequence.
    kNotTracked,
    kNullOutput,
    kUnsupportedMessage,
    kUnsupportedServiceVersion,
    kTruncated,
    kInvalidSequence,
    kInvalidChannel,
};

[[nodiscard]] std::string_view NativeSequenceExtractErrorNameV1(
    NativeSequenceExtractErrorV1 error) noexcept;

// Reads only the fixed native sequence fields. It performs no allocation and
// does not use MDLMessageHead::SequenceID. The caller remains responsible for
// full body/schema validation through the market decoder.
[[nodiscard]] NativeSequenceExtractErrorV1 ExtractNativeSequenceV1(
    const l2flow::sdk::MessageKey& key,
    std::span<const std::byte> body,
    NativeSequenceDescriptorV1* output) noexcept;

enum class NativeSequenceRecoveryRecordClassV1 : std::uint8_t {
    // A target record enters FAST normally, but CERTIFIED cannot advance over
    // it until MarkTargetApplied supplies its successfully applied cookie.
    kTarget = 0U,
    // A record whose exact instrument key and native sequence fields are
    // valid, but whose security id is excluded by the A-share filter. The
    // remaining non-A-share body is intentionally not fully decoded. It still
    // occupies its exchange sequence and is an output-free ready marker for
    // the A-share CERTIFIED projection.
    kFiltered,
};

struct NativeSequenceRecoveryConfigV1 final {
    std::size_t maximum_channels = 0U;
    // Unique native keys retained before certification. Applied-before-
    // observed keys count against the same bound.
    std::size_t maximum_pending_entries = 0U;
    // Prevents one broken channel from consuming the entire global pending
    // budget. It must not exceed maximum_pending_entries.
    std::size_t maximum_pending_entries_per_channel = 0U;
    // Canonical projected payloads retained after certification for duplicate
    // and conflict checks. Retention is global FIFO and may be zero.
    std::size_t certified_duplicate_retention_entries = 0U;
    // Canonical payload is supplied by the applied worker, never copied by the
    // SDK callback. Both limits are hard bounds. A breach freezes only the
    // affected CERTIFIED channel and never rejects FAST.
    std::size_t maximum_canonical_payload_bytes_per_entry = 0U;
    std::size_t maximum_total_canonical_payload_bytes = 0U;
    // Bounds sequence - certified_frontier for an established channel. A
    // breach freezes only that channel's CERTIFIED state.
    std::uint64_t maximum_reorder_span = 0U;

    // Explicit native coverage baseline applied independently to every newly
    // discovered channel. It must be nonzero: the coordinator never infers
    // completeness from the first packet it happens to observe. The production
    // from-open composition uses the default of one because both supported
    // vendor domains are documented as starting at one. A partial-session
    // caller must instead provide its explicit trusted epoch origin.
    std::uint64_t expected_origin_sequence = 1U;

    // Optional caller-certified prefix within the explicit coverage epoch.
    // Zero means expected_origin_sequence - 1. A nonzero checkpoint must be at
    // least expected_origin_sequence - 1 and below the largest representable
    // native sequence. The caller is responsible for restoring every dependent
    // projection state represented by this checkpoint; the coordinator treats
    // it as authoritative and begins waiting at checkpoint + 1.
    //
    std::uint64_t trusted_checkpoint_sequence = 0U;
};

enum class NativeSequenceRecoveryCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kResourceExhausted,
};

enum class NativeSequenceRecoveryChannelStateV1 : std::uint8_t {
    kHealthy = 0U,
    kCatchingUp,
    kRepairing,
    kFrozenConflict,
    kFrozenResource,
};

enum class NativeSequenceRecoveryFreezeReasonV1 : std::uint8_t {
    kNone = 0U,
    kPayloadConflict,
    kPendingEntryCapacity,
    kPerChannelEntryCapacity,
    kPayloadCapacity,
    kReorderWindowExceeded,
    kDuplicateVerificationUnavailable,
    kInternalInvariant,
};

// The token is an ABA-protected reference to one pending native position.
// Target observations may use it for application, while PollCertified also
// returns a token for an output-free filtered position so its commit and
// externally visible channel-state update remain explicit. The integration
// does not need to carry target tokens through Store: MarkTargetApplied also
// supports the stable native key directly.
struct NativeSequenceRecoveryTokenV1 final {
    std::uint32_t channel_slot =
        std::numeric_limits<std::uint32_t>::max();
    std::uint32_t entry_index =
        std::numeric_limits<std::uint32_t>::max();
    std::uint32_t entry_generation = 0U;
    std::uint32_t reserved = 0U;
    std::uint64_t sequence = 0U;

    [[nodiscard]] constexpr bool valid() const noexcept {
        return channel_slot !=
                   std::numeric_limits<std::uint32_t>::max() &&
               entry_index !=
                   std::numeric_limits<std::uint32_t>::max() &&
               entry_generation != 0U && sequence != 0U;
    }
};

enum class NativeSequenceRecoveryObserveDispositionV1 :
    std::uint8_t {
    kAccepted = 0U,
    kGapOpened,
    kGapExtended,
    kBackfill,
    // Identity is the same, but target payload equality remains pending until
    // MarkTargetApplied receives the corresponding canonical payload.
    kDuplicatePending,
    kDuplicateCertified,
    // The key is older than the configured exact duplicate retention window.
    // Its payload can no longer be proved equal, so observing it freezes this
    // CERTIFIED channel as a resource/verification failure. FAST is unchanged.
    kDuplicateOutsideRetention,
    // The sequence precedes this process's first observed sequence for the
    // channel. It is outside this coordinator epoch's declared coverage.
    kBeforeOrigin,
    kPayloadConflict,
    kChannelFrozen,
    kResourceFrozen,
    // The fixed channel table was exhausted. Existing channels remain valid.
    kChannelCapacity,
};

enum class NativeSequenceRecoveryObserveErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidInput,
};

struct NativeSequenceRecoveryObserveResultV1 final {
    NativeSequenceRecoveryObserveDispositionV1 disposition =
        NativeSequenceRecoveryObserveDispositionV1::kAccepted;
    NativeSequenceRecoveryTokenV1 token{};
    NativeSequenceRecoveryChannelStateV1 channel_state =
        NativeSequenceRecoveryChannelStateV1::kHealthy;
    std::uint64_t origin_sequence = 0U;
    std::uint64_t certified_sequence = 0U;
    std::uint64_t observed_contiguous_sequence = 0U;
    std::uint64_t highest_observed_sequence = 0U;

    [[nodiscard]] bool target_token_available() const noexcept {
        return token.valid();
    }
};

enum class NativeSequenceRecoveryApplyErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidInput,
    kNotObserved,
    kInvalidToken,
    kWrongRecordClass,
    kUnexpectedApplication,
    kChannelFrozen,
};

enum class NativeSequenceRecoveryApplyDispositionV1 :
    std::uint8_t {
    kApplied = 0U,
    kExactDuplicate,
    kDuplicateOutsideRetention,
    kPayloadConflict,
    kResourceFrozen,
    kChannelFrozen,
    kChannelCapacity,
};

struct NativeSequenceRecoveryApplyResultV1 final {
    NativeSequenceRecoveryApplyDispositionV1 disposition =
        NativeSequenceRecoveryApplyDispositionV1::kApplied;
    NativeSequenceRecoveryChannelStateV1 channel_state =
        NativeSequenceRecoveryChannelStateV1::kHealthy;
    NativeSequenceRecoveryFreezeReasonV1 freeze_reason =
        NativeSequenceRecoveryFreezeReasonV1::kNone;
    // The first successfully applied cookie remains canonical. A later exact
    // duplicate never replaces it.
    std::uint64_t canonical_applied_cookie = 0U;
    std::uint64_t observed_arrivals = 0U;
    std::uint64_t applied_arrivals = 0U;
};

enum class NativeSequenceRecoveryPollErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kNotReady,
};

struct NativeSequenceCertifiedReadyV1 final {
    NativeSequenceRecoveryTokenV1 token{};
    NativeSequenceDescriptorV1 descriptor{};
    l2flow::sdk::MessageKey message_key{};
    NativeSequenceRecoveryRecordClassV1 record_class =
        NativeSequenceRecoveryRecordClassV1::kTarget;
    // Opaque caller value supplied to MarkTargetApplied. The coordinator
    // neither dereferences nor assigns ordering semantics to it. It is zero
    // for an output-free filtered position.
    std::uint64_t applied_cookie = 0U;
};

enum class NativeSequenceRecoveryCommitErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidToken,
    kNotNext,
    kNotApplied,
    kWrongRecordClass,
    kChannelFrozen,
};

struct NativeSequenceRecoveryChannelSnapshotV1 final {
    NativeSequenceChannelV1 domain{};
    NativeSequenceRecoveryChannelStateV1 state =
        NativeSequenceRecoveryChannelStateV1::kHealthy;
    NativeSequenceRecoveryFreezeReasonV1 freeze_reason =
        NativeSequenceRecoveryFreezeReasonV1::kNone;
    std::uint64_t origin_sequence = 0U;
    std::uint64_t certified_sequence = 0U;
    std::uint64_t observed_contiguous_sequence = 0U;
    std::uint64_t highest_observed_sequence = 0U;
    std::uint64_t unique_sequences = 0U;
    std::uint64_t duplicate_arrivals = 0U;
    std::uint64_t exact_duplicate_applications = 0U;
    // Target duplicate observations whose matching canonical applied
    // notification has not yet been byte-checked. CERTIFIED never advances
    // past the lowest such sequence.
    std::uint64_t unverified_duplicate_applications = 0U;
    std::uint64_t duplicate_outside_retention = 0U;
    std::uint64_t conflicts = 0U;
    std::uint64_t filtered_sequences_certified = 0U;
    std::size_t pending_entries = 0U;
    std::uint64_t missing_sequences = 0U;
    bool coverage_from_sequence_one = false;
};

struct NativeSequenceRecoverySnapshotV1 final {
    std::size_t maximum_channels = 0U;
    std::size_t channel_count = 0U;
    std::size_t frozen_channel_count = 0U;
    std::size_t pending_entries = 0U;
    std::size_t retained_certified_entries = 0U;
    std::size_t canonical_payload_bytes = 0U;
    std::uint64_t channel_capacity_failures = 0U;
    std::uint64_t invalid_observations = 0U;
};

[[nodiscard]] std::string_view
NativeSequenceRecoveryCreateErrorNameV1(
    NativeSequenceRecoveryCreateErrorV1 error) noexcept;
[[nodiscard]] std::string_view
NativeSequenceRecoveryChannelStateNameV1(
    NativeSequenceRecoveryChannelStateV1 state) noexcept;
[[nodiscard]] std::string_view
NativeSequenceRecoveryFreezeReasonNameV1(
    NativeSequenceRecoveryFreezeReasonV1 reason) noexcept;
[[nodiscard]] std::string_view
NativeSequenceRecoveryObserveDispositionNameV1(
    NativeSequenceRecoveryObserveDispositionV1 disposition) noexcept;
[[nodiscard]] std::string_view
NativeSequenceRecoveryApplyDispositionNameV1(
    NativeSequenceRecoveryApplyDispositionV1 disposition) noexcept;

// A single-thread coordinator. All methods except Create/destruction must be
// called by one serialized owner. Construction preallocates its channel table,
// native-key table, entry pool, and retention index. Canonical payload storage
// is allocated only by MarkTargetApplied on the worker/CERTIFIED side and is
// bounded by the configuration. No observation or application result
// authorizes the caller to reject, stop, or mark coverage-lost on FAST:
// conflict and resource outcomes freeze only the affected CERTIFIED channel.
class NativeSequenceRecoveryCoordinatorV1 final {
public:
    NativeSequenceRecoveryCoordinatorV1(
        const NativeSequenceRecoveryCoordinatorV1&) = delete;
    NativeSequenceRecoveryCoordinatorV1& operator=(
        const NativeSequenceRecoveryCoordinatorV1&) = delete;
    NativeSequenceRecoveryCoordinatorV1(
        NativeSequenceRecoveryCoordinatorV1&&) = delete;
    NativeSequenceRecoveryCoordinatorV1& operator=(
        NativeSequenceRecoveryCoordinatorV1&&) = delete;
    ~NativeSequenceRecoveryCoordinatorV1();

    [[nodiscard]] static NativeSequenceRecoveryCreateErrorV1 Create(
        NativeSequenceRecoveryConfigV1 config,
        std::unique_ptr<NativeSequenceRecoveryCoordinatorV1>* output)
        noexcept;

    [[nodiscard]] NativeSequenceRecoveryObserveErrorV1 Observe(
        const NativeSequenceDescriptorV1& descriptor,
        const l2flow::sdk::MessageKey& message_key,
        NativeSequenceRecoveryRecordClassV1 record_class,
        NativeSequenceRecoveryObserveResultV1* output) noexcept;

    // This key-based join may run before or after Observe. For every target
    // arrival the integration calls Observe once and MarkTargetApplied once;
    // filtered arrivals call only Observe.
    //
    // canonical_payload must be a deterministic business projection. The
    // caller must omit or zero arrival-only fields (for example local
    // source/ingress/tick-stream sequences, vendor transport SequenceID, and
    // receive timestamps) before calling. This coordinator performs exact byte
    // comparison; it does not guess which fields are arrival-only.
    [[nodiscard]] NativeSequenceRecoveryApplyErrorV1 MarkTargetApplied(
        const NativeSequenceDescriptorV1& descriptor,
        const l2flow::sdk::MessageKey& message_key,
        std::span<const std::byte> canonical_payload,
        std::uint64_t applied_cookie,
        NativeSequenceRecoveryApplyResultV1* output) noexcept;

    // Optional optimized join for integrations that can preserve the token.
    [[nodiscard]] NativeSequenceRecoveryApplyErrorV1 MarkTargetApplied(
        const NativeSequenceRecoveryTokenV1& token,
        const l2flow::sdk::MessageKey& message_key,
        std::span<const std::byte> canonical_payload,
        std::uint64_t applied_cookie,
        NativeSequenceRecoveryApplyResultV1* output) noexcept;

    // Returns either an applied target or an output-free filtered marker that
    // is exactly certified_sequence + 1 for its channel. Both are advanced
    // only by the caller's explicit CommitCertified. Cross-channel selection
    // is deterministic round-robin and makes no exchange-global-order claim.
    [[nodiscard]] NativeSequenceRecoveryPollErrorV1 PollCertified(
        NativeSequenceCertifiedReadyV1* output) noexcept;

    // Explicitly advances the returned native position. Until this succeeds,
    // PollCertified may return the same token and certified_sequence does not
    // advance. A filtered marker has no Tick/Event side effect. Integrations
    // that commit a target before fallible downstream work must keep every
    // public frontier/slot gated at the prior prefix and terminally freeze
    // that CERTIFIED stream if the downstream transaction then fails.
    [[nodiscard]] NativeSequenceRecoveryCommitErrorV1 CommitCertified(
        const NativeSequenceRecoveryTokenV1& token) noexcept;

    [[nodiscard]] bool ChannelSnapshot(
        const NativeSequenceChannelV1& domain,
        NativeSequenceRecoveryChannelSnapshotV1* output) const noexcept;
    [[nodiscard]] NativeSequenceRecoverySnapshotV1 Snapshot()
        const noexcept;

private:
    class Impl;
    explicit NativeSequenceRecoveryCoordinatorV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::realtime
