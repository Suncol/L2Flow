#include "l2flow/canonical/sequence_guard_v1.h"

#include <algorithm>
#include <limits>
#include <new>
#include <utility>
#include <vector>

namespace l2flow::canonical {
namespace {

bool ScopeValid(const SequenceScopeKeyV1& scope) noexcept {
    if (scope.date == 0U || scope.source_stream_id == 0U) {
        return false;
    }
    const bool zero_stream_day =
        l2flow::common::IsZeroIdentity(scope.stream_day_id);
    switch (scope.kind) {
        case SequenceScopeKindV1::kVendorMessage:
            return !zero_stream_day && scope.channel == 0U;
        case SequenceScopeKindV1::kShanghaiChannel:
        case SequenceScopeKindV1::kShenzhenUnifiedChannel:
            return zero_stream_day && scope.service_id == 0U &&
                   scope.message_id == 0U;
    }
    return false;
}

bool DefaultDigest(
    std::span<const std::byte> payload,
    l2flow::common::Sha256Digest* digest) noexcept {
    if (digest == nullptr) {
        return false;
    }
    *digest = l2flow::common::ComputeSha256(payload);
    return true;
}

}  // namespace

namespace sequence_guard_detail {

struct PendingSeenEntryV1 final {
    std::uint64_t sequence = 0U;
    l2flow::common::Sha256Digest digest{};
    std::vector<std::byte> payload;
};

}  // namespace sequence_guard_detail

SequenceScopeKeyV1 MakeVendorSequenceScopeV1(
    std::uint32_t capture_date,
    std::uint32_t source_stream_id,
    const l2flow::common::Identity128& stream_day_id,
    std::uint8_t service_id,
    std::uint16_t message_id) noexcept {
    SequenceScopeKeyV1 result;
    result.kind = SequenceScopeKindV1::kVendorMessage;
    result.date = capture_date;
    result.source_stream_id = source_stream_id;
    result.stream_day_id = stream_day_id;
    result.service_id = service_id;
    result.message_id = message_id;
    return result;
}

SequenceScopeKeyV1 MakeShanghaiChannelScopeV1(
    std::uint32_t trade_date,
    std::uint32_t source_stream_id,
    std::uint32_t channel) noexcept {
    SequenceScopeKeyV1 result;
    result.kind = SequenceScopeKindV1::kShanghaiChannel;
    result.date = trade_date;
    result.source_stream_id = source_stream_id;
    result.channel = channel;
    return result;
}

SequenceScopeKeyV1 MakeShenzhenUnifiedChannelScopeV1(
    std::uint32_t trade_date,
    std::uint32_t source_stream_id,
    std::uint32_t channel) noexcept {
    SequenceScopeKeyV1 result;
    result.kind = SequenceScopeKindV1::kShenzhenUnifiedChannel;
    result.date = trade_date;
    result.source_stream_id = source_stream_id;
    result.channel = channel;
    return result;
}

std::string_view SequenceGuardStateNameV1(
    SequenceGuardStateV1 state) noexcept {
    switch (state) {
        case SequenceGuardStateV1::kUnseen:
            return "unseen";
        case SequenceGuardStateV1::kTracking:
            return "tracking";
        case SequenceGuardStateV1::kDegradedGap:
            return "degraded_gap";
        case SequenceGuardStateV1::kPoisoned:
            return "poisoned";
    }
    return "unknown";
}

std::string_view SequenceGuardOutcomeNameV1(
    SequenceGuardOutcomeV1 outcome) noexcept {
    switch (outcome) {
        case SequenceGuardOutcomeV1::kFirst:
            return "first";
        case SequenceGuardOutcomeV1::kContiguous:
            return "contiguous";
        case SequenceGuardOutcomeV1::kGap:
            return "gap";
        case SequenceGuardOutcomeV1::kExactDuplicate:
            return "exact_duplicate";
        case SequenceGuardOutcomeV1::kConflict:
            return "conflict";
        case SequenceGuardOutcomeV1::kBackward:
            return "backward";
        case SequenceGuardOutcomeV1::kPoisoned:
            return "poisoned";
        case SequenceGuardOutcomeV1::kCapacity:
            return "capacity";
    }
    return "unknown";
}

std::string_view SequenceGuardCreateErrorNameV1(
    SequenceGuardCreateErrorV1 error) noexcept {
    switch (error) {
        case SequenceGuardCreateErrorV1::kNone:
            return "none";
        case SequenceGuardCreateErrorV1::kNullOutput:
            return "null_output";
        case SequenceGuardCreateErrorV1::kInvalidScope:
            return "invalid_scope";
        case SequenceGuardCreateErrorV1::kInvalidLimits:
            return "invalid_limits";
        case SequenceGuardCreateErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

std::string_view SequenceGuardPrepareErrorNameV1(
    SequenceGuardPrepareErrorV1 error) noexcept {
    switch (error) {
        case SequenceGuardPrepareErrorV1::kNone:
            return "none";
        case SequenceGuardPrepareErrorV1::kNullToken:
            return "null_token";
        case SequenceGuardPrepareErrorV1::kTokenStillActive:
            return "token_still_active";
        case SequenceGuardPrepareErrorV1::kDigestFailure:
            return "digest_failure";
        case SequenceGuardPrepareErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case SequenceGuardPrepareErrorV1::kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

std::string_view SequenceGuardCommitErrorNameV1(
    SequenceGuardCommitErrorV1 error) noexcept {
    switch (error) {
        case SequenceGuardCommitErrorV1::kNone:
            return "none";
        case SequenceGuardCommitErrorV1::kInvalidToken:
            return "invalid_token";
        case SequenceGuardCommitErrorV1::kWrongGuard:
            return "wrong_guard";
        case SequenceGuardCommitErrorV1::kVersionMismatch:
            return "version_mismatch";
        case SequenceGuardCommitErrorV1::kVersionExhausted:
            return "version_exhausted";
    }
    return "unknown";
}

SequenceGuardTokenV1::SequenceGuardTokenV1() noexcept = default;

SequenceGuardTokenV1::SequenceGuardTokenV1(
    SequenceGuardTokenV1&& other) noexcept
    : owner_(other.owner_),
      expected_version_(other.expected_version_),
      sequence_(other.sequence_),
      outcome_(other.outcome_),
      start_unknown_after_commit_(
          other.start_unknown_after_commit_),
      missing_begin_(other.missing_begin_),
      missing_end_(other.missing_end_),
      pending_entry_(std::move(other.pending_entry_)) {
    other.Reset();
}

SequenceGuardTokenV1& SequenceGuardTokenV1::operator=(
    SequenceGuardTokenV1&& other) noexcept {
    if (this == &other) {
        return *this;
    }
    Reset();
    owner_ = other.owner_;
    expected_version_ = other.expected_version_;
    sequence_ = other.sequence_;
    outcome_ = other.outcome_;
    start_unknown_after_commit_ =
        other.start_unknown_after_commit_;
    missing_begin_ = other.missing_begin_;
    missing_end_ = other.missing_end_;
    pending_entry_ = std::move(other.pending_entry_);
    other.Reset();
    return *this;
}

SequenceGuardTokenV1::~SequenceGuardTokenV1() = default;

void SequenceGuardTokenV1::Reset() noexcept {
    pending_entry_.reset();
    owner_ = nullptr;
    expected_version_ = 0U;
    sequence_ = 0U;
    outcome_ = SequenceGuardOutcomeV1::kPoisoned;
    start_unknown_after_commit_ = false;
    missing_begin_ = 0U;
    missing_end_ = 0U;
}

class SequenceGuardV1::Impl final {
public:
    explicit Impl(SequenceGuardConfigV1 value)
        : config(std::move(value)) {}

    [[nodiscard]] const sequence_guard_detail::PendingSeenEntryV1*
    Find(std::uint64_t sequence) const noexcept {
        const auto found = std::lower_bound(
            seen.begin(),
            seen.end(),
            sequence,
            [](const auto& entry, std::uint64_t wanted) {
                return entry->sequence < wanted;
            });
        if (found == seen.end() || (*found)->sequence != sequence) {
            return nullptr;
        }
        return found->get();
    }

    SequenceGuardConfigV1 config{};
    SequenceGuardStateV1 state = SequenceGuardStateV1::kUnseen;
    std::uint64_t version = 0U;
    std::uint64_t seen_payload_bytes = 0U;
    std::uint64_t high_sequence = 0U;
    bool has_high_sequence = false;
    bool start_unknown = false;
    std::vector<std::unique_ptr<
        sequence_guard_detail::PendingSeenEntryV1>> seen;
};

SequenceGuardV1::SequenceGuardV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

SequenceGuardV1::~SequenceGuardV1() = default;

SequenceGuardCreateErrorV1 SequenceGuardV1::Create(
    SequenceGuardConfigV1 config,
    std::unique_ptr<SequenceGuardV1>* output) noexcept {
    if (output == nullptr) {
        return SequenceGuardCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (!ScopeValid(config.scope)) {
        return SequenceGuardCreateErrorV1::kInvalidScope;
    }
    if (config.max_seen_entries == 0U ||
        config.max_seen_payload_bytes == 0U ||
        config.max_seen_entries >
            static_cast<std::uintmax_t>(
                std::numeric_limits<std::size_t>::max())) {
        return SequenceGuardCreateErrorV1::kInvalidLimits;
    }
    try {
        auto impl = std::make_unique<Impl>(std::move(config));
        output->reset(new SequenceGuardV1(std::move(impl)));
        return SequenceGuardCreateErrorV1::kNone;
    } catch (...) {
        return SequenceGuardCreateErrorV1::kResourceExhausted;
    }
}

SequenceGuardPrepareResultV1 SequenceGuardV1::Prepare(
    std::uint64_t sequence,
    std::span<const std::byte> exact_payload,
    SequenceGuardTokenV1* token) noexcept {
    SequenceGuardPrepareResultV1 result;
    result.sequence = sequence;
    result.state_before = impl_->state;
    result.high_before = impl_->high_sequence;
    result.had_high_before = impl_->has_high_sequence;
    if (impl_->config.expected_first.has_value()) {
        result.configured_first = *impl_->config.expected_first;
        result.had_configured_first = true;
    }
    result.start_unknown_after_commit = impl_->start_unknown;

    if (token == nullptr) {
        result.error = SequenceGuardPrepareErrorV1::kNullToken;
        return result;
    }
    if (token->active()) {
        result.error =
            SequenceGuardPrepareErrorV1::kTokenStillActive;
        return result;
    }
    token->Reset();

    const auto prepare_token = [&](SequenceGuardOutcomeV1 outcome) {
        result.outcome = outcome;
        result.token_prepared = true;
        token->owner_ = this;
        token->expected_version_ = impl_->version;
        token->sequence_ = sequence;
        token->outcome_ = outcome;
        token->start_unknown_after_commit_ =
            result.start_unknown_after_commit;
        token->missing_begin_ = result.missing_begin;
        token->missing_end_ = result.missing_end;
    };

    if (impl_->state == SequenceGuardStateV1::kPoisoned) {
        prepare_token(SequenceGuardOutcomeV1::kPoisoned);
        return result;
    }

    const bool digest_ok = impl_->config.digest_function == nullptr
        ? DefaultDigest(exact_payload, &result.payload_digest)
        : impl_->config.digest_function(
              impl_->config.digest_context,
              exact_payload,
              &result.payload_digest);
    if (!digest_ok) {
        result.error = SequenceGuardPrepareErrorV1::kDigestFailure;
        return result;
    }

    if (impl_->has_high_sequence &&
        sequence <= impl_->high_sequence) {
        const auto* prior = impl_->Find(sequence);
        if (prior == nullptr) {
            prepare_token(SequenceGuardOutcomeV1::kBackward);
            return result;
        }
        result.prior_payload_digest = prior->digest;
        result.prior_payload_digest_present = true;
        const bool exact =
            prior->digest == result.payload_digest &&
            prior->payload.size() == exact_payload.size() &&
            std::equal(
                prior->payload.begin(),
                prior->payload.end(),
                exact_payload.begin(),
                exact_payload.end());
        prepare_token(
            exact ? SequenceGuardOutcomeV1::kExactDuplicate
                  : SequenceGuardOutcomeV1::kConflict);
        return result;
    }

    SequenceGuardOutcomeV1 accepted_outcome =
        SequenceGuardOutcomeV1::kContiguous;
    if (!impl_->has_high_sequence) {
        if (!impl_->config.expected_first.has_value()) {
            accepted_outcome = SequenceGuardOutcomeV1::kFirst;
            result.start_unknown_after_commit = true;
        } else if (
            sequence == *impl_->config.expected_first) {
            accepted_outcome = SequenceGuardOutcomeV1::kFirst;
            result.start_unknown_after_commit = false;
        } else if (sequence > *impl_->config.expected_first) {
            accepted_outcome = SequenceGuardOutcomeV1::kGap;
            result.missing_begin = *impl_->config.expected_first;
            result.missing_end = sequence;
            result.has_missing_interval = true;
            result.start_unknown_after_commit = false;
        } else {
            prepare_token(SequenceGuardOutcomeV1::kBackward);
            return result;
        }
    } else {
        // sequence > high is already established.  Subtraction is therefore
        // defined and avoids evaluating high+1 when high is UINT64_MAX.
        const std::uint64_t delta = sequence - impl_->high_sequence;
        if (delta == 1U) {
            accepted_outcome = SequenceGuardOutcomeV1::kContiguous;
        } else {
            accepted_outcome = SequenceGuardOutcomeV1::kGap;
            // delta > 1 proves high <= UINT64_MAX-2, so this increment cannot
            // wrap.  The missing interval remains half-open.
            result.missing_begin = impl_->high_sequence + 1U;
            result.missing_end = sequence;
            result.has_missing_interval = true;
        }
    }

    const bool entry_capacity_exhausted =
        impl_->seen.size() >= impl_->config.max_seen_entries;
    const bool payload_size_unrepresentable =
        static_cast<std::uintmax_t>(exact_payload.size()) >
            std::numeric_limits<std::uint64_t>::max();
    const std::uint64_t payload_size = payload_size_unrepresentable
        ? 0U
        : static_cast<std::uint64_t>(exact_payload.size());
    const bool payload_capacity_exhausted =
        payload_size_unrepresentable ||
        payload_size > impl_->config.max_seen_payload_bytes ||
        impl_->seen_payload_bytes >
            impl_->config.max_seen_payload_bytes - payload_size;
    if (entry_capacity_exhausted || payload_capacity_exhausted) {
        prepare_token(SequenceGuardOutcomeV1::kCapacity);
        return result;
    }

    try {
        // Reserve storage during Prepare, before any Canonical publication.
        // Commit must never allocate, but reserving the configured all-day
        // hard limit at Create would multiply a potentially multi-million
        // entry allocation by every vendor/channel scope.  Geometric growth
        // keeps resident memory proportional to observations while still
        // making the prepared Commit allocation-free.
        if (impl_->seen.size() == impl_->seen.capacity()) {
            const std::uint64_t current_capacity =
                static_cast<std::uint64_t>(impl_->seen.capacity());
            const std::uint64_t doubled =
                current_capacity == 0U
                ? 1U
                : (current_capacity >
                           std::numeric_limits<std::uint64_t>::max() / 2U
                       ? std::numeric_limits<std::uint64_t>::max()
                       : current_capacity * 2U);
            const std::uint64_t desired = std::min(
                impl_->config.max_seen_entries, doubled);
            impl_->seen.reserve(static_cast<std::size_t>(desired));
        }
        auto pending = std::make_unique<
            sequence_guard_detail::PendingSeenEntryV1>();
        pending->sequence = sequence;
        pending->digest = result.payload_digest;
        pending->payload.assign(
            exact_payload.begin(), exact_payload.end());
        token->pending_entry_ = std::move(pending);
    } catch (const std::bad_alloc&) {
        result.error = SequenceGuardPrepareErrorV1::kResourceExhausted;
        return result;
    } catch (const std::length_error&) {
        result.error = SequenceGuardPrepareErrorV1::kResourceExhausted;
        return result;
    } catch (...) {
        result.error = SequenceGuardPrepareErrorV1::kUnexpectedFailure;
        return result;
    }

    prepare_token(accepted_outcome);
    token->start_unknown_after_commit_ =
        result.start_unknown_after_commit;
    return result;
}

SequenceGuardCommitErrorV1 SequenceGuardV1::Commit(
    SequenceGuardTokenV1* token) noexcept {
    if (token == nullptr || !token->active()) {
        return SequenceGuardCommitErrorV1::kInvalidToken;
    }
    if (token->owner_ != this) {
        return SequenceGuardCommitErrorV1::kWrongGuard;
    }
    if (token->expected_version_ != impl_->version) {
        token->Reset();
        return SequenceGuardCommitErrorV1::kVersionMismatch;
    }
    if (impl_->version == std::numeric_limits<std::uint64_t>::max()) {
        impl_->state = SequenceGuardStateV1::kPoisoned;
        token->Reset();
        return SequenceGuardCommitErrorV1::kVersionExhausted;
    }

    switch (token->outcome_) {
        case SequenceGuardOutcomeV1::kFirst:
        case SequenceGuardOutcomeV1::kContiguous:
        case SequenceGuardOutcomeV1::kGap: {
            if (token->pending_entry_ == nullptr ||
                token->pending_entry_->sequence != token->sequence_ ||
                impl_->seen.size() >= impl_->seen.capacity()) {
                impl_->state = SequenceGuardStateV1::kPoisoned;
                ++impl_->version;
                token->Reset();
                return SequenceGuardCommitErrorV1::kInvalidToken;
            }
            const std::uint64_t payload_bytes =
                static_cast<std::uint64_t>(
                    token->pending_entry_->payload.size());
            impl_->seen_payload_bytes += payload_bytes;
            impl_->high_sequence = token->sequence_;
            impl_->has_high_sequence = true;
            impl_->start_unknown =
                token->start_unknown_after_commit_;
            impl_->seen.push_back(std::move(token->pending_entry_));
            if (token->outcome_ == SequenceGuardOutcomeV1::kGap ||
                impl_->state == SequenceGuardStateV1::kDegradedGap) {
                impl_->state = SequenceGuardStateV1::kDegradedGap;
            } else {
                impl_->state = SequenceGuardStateV1::kTracking;
            }
            break;
        }
        case SequenceGuardOutcomeV1::kExactDuplicate:
        case SequenceGuardOutcomeV1::kPoisoned:
            break;
        case SequenceGuardOutcomeV1::kConflict:
        case SequenceGuardOutcomeV1::kBackward:
            impl_->state = SequenceGuardStateV1::kPoisoned;
            break;
        case SequenceGuardOutcomeV1::kCapacity:
            // Capacity on an unconfigured first observation still proves
            // that the captured prefix is unknown.  Preserve that fact when
            // the scope transitions directly from UNSEEN to POISONED so all
            // later terminal quality remains START_UNKNOWN-sticky.
            impl_->start_unknown =
                impl_->start_unknown ||
                token->start_unknown_after_commit_;
            impl_->state = SequenceGuardStateV1::kPoisoned;
            break;
    }

    ++impl_->version;
    token->Reset();
    return SequenceGuardCommitErrorV1::kNone;
}

bool SequenceGuardV1::Abort(
    SequenceGuardTokenV1* token) noexcept {
    if (token == nullptr || !token->active() ||
        token->owner_ != this) {
        return false;
    }
    token->Reset();
    return true;
}

SequenceGuardSnapshotV1 SequenceGuardV1::Snapshot() const noexcept {
    SequenceGuardSnapshotV1 result;
    result.scope = impl_->config.scope;
    result.state = impl_->state;
    result.version = impl_->version;
    result.seen_entries =
        static_cast<std::uint64_t>(impl_->seen.size());
    result.seen_payload_bytes = impl_->seen_payload_bytes;
    result.high_sequence = impl_->high_sequence;
    result.has_high_sequence = impl_->has_high_sequence;
    result.start_unknown = impl_->start_unknown;
    return result;
}

const SequenceGuardConfigV1& SequenceGuardV1::config()
    const noexcept {
    return impl_->config;
}

}  // namespace l2flow::canonical
