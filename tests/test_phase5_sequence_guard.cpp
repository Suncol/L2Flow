#include "l2flow/canonical/sequence_guard_v1.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <utility>

namespace canonical = l2flow::canonical;

namespace {

class TestContext final {
public:
    void Expect(bool condition, std::string_view description) {
        if (!condition) {
            ++failures_;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    [[nodiscard]] int failures() const noexcept {
        return failures_;
    }

private:
    int failures_ = 0;
};

l2flow::common::Identity128 Identity(std::uint8_t seed) {
    l2flow::common::Identity128 result{};
    result[0] = static_cast<std::byte>(seed);
    return result;
}

std::span<const std::byte> Bytes(std::string_view value) {
    return std::as_bytes(std::span(value.data(), value.size()));
}

canonical::SequenceGuardConfigV1 Config(
    std::uint64_t max_entries = 100U,
    std::uint64_t max_payload_bytes = 4096U,
    std::optional<std::uint64_t> expected_first = std::nullopt) {
    canonical::SequenceGuardConfigV1 config;
    config.scope = canonical::MakeVendorSequenceScopeV1(
        20260722U, 2002U, Identity(1U), 6U, 33U);
    config.max_seen_entries = max_entries;
    config.max_seen_payload_bytes = max_payload_bytes;
    config.expected_first = expected_first;
    return config;
}

std::unique_ptr<canonical::SequenceGuardV1> CreateGuard(
    TestContext* test,
    canonical::SequenceGuardConfigV1 config = Config()) {
    std::unique_ptr<canonical::SequenceGuardV1> guard;
    const canonical::SequenceGuardCreateErrorV1 error =
        canonical::SequenceGuardV1::Create(
            std::move(config), &guard);
    test->Expect(
        error == canonical::SequenceGuardCreateErrorV1::kNone &&
            guard != nullptr,
        "valid sequence guard configuration is created");
    return guard;
}

canonical::SequenceGuardPrepareResultV1 PrepareCommit(
    TestContext* test,
    canonical::SequenceGuardV1* guard,
    std::uint64_t sequence,
    std::string_view payload) {
    canonical::SequenceGuardTokenV1 token;
    const canonical::SequenceGuardPrepareResultV1 prepared =
        guard->Prepare(sequence, Bytes(payload), &token);
    test->Expect(
        prepared.ok() && token.active() &&
            prepared.token_prepared,
        "prepare produces one active transaction token");
    test->Expect(
        guard->Commit(&token) ==
                canonical::SequenceGuardCommitErrorV1::kNone &&
            !token.active(),
        "prepared sequence transition commits exactly once");
    return prepared;
}

void TestScopeKeysAndValidation(TestContext* test) {
    const canonical::SequenceScopeKeyV1 vendor =
        canonical::MakeVendorSequenceScopeV1(
            20260722U, 2002U, Identity(1U), 6U, 33U);
    test->Expect(
        vendor.service_id == 6U && vendor.message_id == 33U,
        "vendor sequence scope is ServiceID+MessageID, independent of schema version");

    const canonical::SequenceScopeKeyV1 sz_left =
        canonical::MakeShenzhenUnifiedChannelScopeV1(
            20260722U, 2002U, 7U);
    const canonical::SequenceScopeKeyV1 sz_right =
        canonical::MakeShenzhenUnifiedChannelScopeV1(
            20260722U, 2002U, 7U);
    test->Expect(
        sz_left == sz_right &&
            sz_left.kind == canonical::SequenceScopeKindV1::
                kShenzhenUnifiedChannel &&
            sz_left.message_id == 0U,
        "SZ order and transaction share one channel scope without a message key");

    canonical::SequenceGuardConfigV1 config = Config();
    config.scope = canonical::MakeShanghaiChannelScopeV1(
        20260722U, 1002U, 0U);
    std::unique_ptr<canonical::SequenceGuardV1> guard;
    test->Expect(
        canonical::SequenceGuardV1::Create(config, &guard) ==
                canonical::SequenceGuardCreateErrorV1::kNone &&
            guard != nullptr,
        "generic guard treats channel bits as opaque schema-validated input");

    config = Config();
    config.max_seen_entries = 0U;
    test->Expect(
        canonical::SequenceGuardV1::Create(config, &guard) ==
            canonical::SequenceGuardCreateErrorV1::kInvalidLimits,
        "zero all-day seen capacity is rejected");
}

void TestFirstContiguousGapAndOldDuplicate(TestContext* test) {
    std::unique_ptr<canonical::SequenceGuardV1> guard =
        CreateGuard(test);
    canonical::SequenceGuardTokenV1 token;
    const canonical::SequenceGuardPrepareResultV1 staged_first =
        guard->Prepare(10U, Bytes("ten"), &token);
    test->Expect(
        staged_first.outcome ==
                canonical::SequenceGuardOutcomeV1::kFirst &&
            staged_first.start_unknown_after_commit &&
            guard->Snapshot().state ==
                canonical::SequenceGuardStateV1::kUnseen &&
            guard->Snapshot().seen_entries == 0U,
        "Prepare first is logically read-only and marks an unknown prefix");
    test->Expect(
        guard->Abort(&token) &&
            guard->Snapshot().state ==
                canonical::SequenceGuardStateV1::kUnseen &&
            guard->Snapshot().version == 0U,
        "Abort releases staged bytes without changing state or version");

    const auto first = PrepareCommit(test, guard.get(), 10U, "ten");
    const auto contiguous =
        PrepareCommit(test, guard.get(), 11U, "eleven");
    const auto gap = PrepareCommit(test, guard.get(), 14U, "fourteen");
    test->Expect(
        first.outcome == canonical::SequenceGuardOutcomeV1::kFirst &&
            contiguous.outcome ==
                canonical::SequenceGuardOutcomeV1::kContiguous &&
            gap.outcome == canonical::SequenceGuardOutcomeV1::kGap &&
            gap.has_missing_interval &&
            gap.missing_begin == 12U && gap.missing_end == 14U,
        "contiguous and half-open [12,14) gap transitions are exact");

    const canonical::SequenceGuardSnapshotV1 before_duplicate =
        guard->Snapshot();
    const auto duplicate =
        PrepareCommit(test, guard.get(), 10U, "ten");
    const canonical::SequenceGuardSnapshotV1 after_duplicate =
        guard->Snapshot();
    test->Expect(
        duplicate.outcome ==
                canonical::SequenceGuardOutcomeV1::kExactDuplicate &&
            duplicate.prior_payload_digest_present &&
            before_duplicate.seen_entries ==
                after_duplicate.seen_entries &&
            before_duplicate.seen_payload_bytes ==
                after_duplicate.seen_payload_bytes &&
            after_duplicate.high_sequence == 14U &&
            after_duplicate.state ==
                canonical::SequenceGuardStateV1::kDegradedGap,
        "an old exact duplicate remains identifiable after newer observations");
}

void TestExpectedFirst(TestContext* test) {
    {
        std::unique_ptr<canonical::SequenceGuardV1> guard =
            CreateGuard(test, Config(10U, 100U, 100U));
        const auto first =
            PrepareCommit(test, guard.get(), 100U, "first");
        test->Expect(
            first.outcome ==
                    canonical::SequenceGuardOutcomeV1::kFirst &&
                !guard->Snapshot().start_unknown &&
                guard->Snapshot().state ==
                    canonical::SequenceGuardStateV1::kTracking,
            "authoritative expected first removes START_UNKNOWN");
    }
    {
        std::unique_ptr<canonical::SequenceGuardV1> guard =
            CreateGuard(test, Config(10U, 100U, 100U));
        const auto gap =
            PrepareCommit(test, guard.get(), 103U, "later");
        test->Expect(
            gap.outcome == canonical::SequenceGuardOutcomeV1::kGap &&
                gap.missing_begin == 100U &&
                gap.missing_end == 103U &&
                guard->Snapshot().state ==
                    canonical::SequenceGuardStateV1::kDegradedGap &&
                !guard->Snapshot().start_unknown,
            "first known gap is [expected_first, observed_first)");
    }
    {
        std::unique_ptr<canonical::SequenceGuardV1> guard =
            CreateGuard(test, Config(10U, 100U, 100U));
        const auto backward =
            PrepareCommit(test, guard.get(), 99U, "early");
        test->Expect(
            backward.outcome ==
                    canonical::SequenceGuardOutcomeV1::kBackward &&
                backward.had_configured_first &&
                backward.configured_first == 100U &&
                !backward.had_high_before &&
                guard->Snapshot().state ==
                    canonical::SequenceGuardStateV1::kPoisoned &&
                guard->Snapshot().seen_entries == 0U,
            "observation before authoritative expected first poisons "
            "without retention");
    }
}

void TestConflictBackwardAndPoisonContinuation(TestContext* test) {
    {
        std::unique_ptr<canonical::SequenceGuardV1> guard =
            CreateGuard(test);
        (void)PrepareCommit(test, guard.get(), 5U, "five");
        canonical::SequenceGuardTokenV1 token;
        const auto conflict =
            guard->Prepare(5U, Bytes("FIVE"), &token);
        test->Expect(
            conflict.outcome ==
                    canonical::SequenceGuardOutcomeV1::kConflict &&
                guard->Snapshot().state ==
                    canonical::SequenceGuardStateV1::kTracking,
            "conflict Prepare does not poison before publication succeeds");
        test->Expect(
            guard->Abort(&token) &&
                guard->Snapshot().state ==
                    canonical::SequenceGuardStateV1::kTracking,
            "aborted conflict has no latent poison");

        const auto committed_conflict =
            PrepareCommit(test, guard.get(), 5U, "FIVE");
        const auto poisoned =
            PrepareCommit(test, guard.get(), 6U, "six");
        test->Expect(
            committed_conflict.outcome ==
                    canonical::SequenceGuardOutcomeV1::kConflict &&
                poisoned.outcome ==
                    canonical::SequenceGuardOutcomeV1::kPoisoned &&
                guard->Snapshot().state ==
                    canonical::SequenceGuardStateV1::kPoisoned &&
                guard->Snapshot().high_sequence == 5U &&
                guard->Snapshot().seen_entries == 1U,
            "poisoned scope continues diagnostics without applying "
            "later business data");
    }
    {
        std::unique_ptr<canonical::SequenceGuardV1> guard =
            CreateGuard(test);
        (void)PrepareCommit(test, guard.get(), 5U, "five");
        (void)PrepareCommit(test, guard.get(), 7U, "seven");
        const auto backward =
            PrepareCommit(test, guard.get(), 6U, "six");
        test->Expect(
            backward.outcome ==
                    canonical::SequenceGuardOutcomeV1::kBackward &&
                guard->Snapshot().state ==
                    canonical::SequenceGuardStateV1::kPoisoned,
            "unseen sequence below high is backward rather than a late gap repair");
    }
}

bool ConstantDigest(
    void*,
    std::span<const std::byte>,
    l2flow::common::Sha256Digest* digest) noexcept {
    if (digest == nullptr) {
        return false;
    }
    digest->fill(std::byte{0x5a});
    return true;
}

bool FailingDigest(
    void*,
    std::span<const std::byte>,
    l2flow::common::Sha256Digest*) noexcept {
    return false;
}

void TestDigestCollisionStillComparesBytes(TestContext* test) {
    canonical::SequenceGuardConfigV1 config = Config();
    config.digest_function = &ConstantDigest;
    std::unique_ptr<canonical::SequenceGuardV1> guard =
        CreateGuard(test, config);
    (void)PrepareCommit(test, guard.get(), 1U, "alpha");
    const auto exact =
        PrepareCommit(test, guard.get(), 1U, "alpha");
    test->Expect(
        exact.outcome ==
            canonical::SequenceGuardOutcomeV1::kExactDuplicate,
        "equal digest plus equal bytes is exact duplicate");

    canonical::SequenceGuardTokenV1 conflict_token;
    const auto collision =
        guard->Prepare(1U, Bytes("omega"), &conflict_token);
    test->Expect(
        collision.payload_digest == collision.prior_payload_digest &&
            collision.outcome ==
                canonical::SequenceGuardOutcomeV1::kConflict,
        "equal injected digest with different bytes is conflict, not exact");
    test->Expect(
        guard->Abort(&conflict_token),
        "collision test can abort without poisoning state");

    canonical::SequenceGuardConfigV1 failed_config = Config();
    failed_config.digest_function = &FailingDigest;
    std::unique_ptr<canonical::SequenceGuardV1> failed_guard =
        CreateGuard(test, failed_config);
    canonical::SequenceGuardTokenV1 failed_token;
    const auto failed = failed_guard->Prepare(
        1U, Bytes("payload"), &failed_token);
    test->Expect(
        failed.error ==
                canonical::SequenceGuardPrepareErrorV1::kDigestFailure &&
            !failed_token.active() &&
            failed_guard->Snapshot().state ==
                canonical::SequenceGuardStateV1::kUnseen,
        "digest seam failure is explicit and state-atomic");
}

void TestCapacityIsFailClosedAndNonEvicting(TestContext* test) {
    {
        std::unique_ptr<canonical::SequenceGuardV1> guard =
            CreateGuard(test, Config(1U, 100U));
        (void)PrepareCommit(test, guard.get(), 1U, "one");
        const auto duplicate =
            PrepareCommit(test, guard.get(), 1U, "one");
        test->Expect(
            duplicate.outcome ==
                    canonical::SequenceGuardOutcomeV1::kExactDuplicate &&
                guard->Snapshot().seen_entries == 1U,
            "full entry capacity still identifies retained duplicates");

        canonical::SequenceGuardTokenV1 capacity_token;
        const auto staged_capacity =
            guard->Prepare(2U, Bytes("two"), &capacity_token);
        test->Expect(
            staged_capacity.outcome ==
                    canonical::SequenceGuardOutcomeV1::kCapacity &&
                guard->Snapshot().state ==
                    canonical::SequenceGuardStateV1::kTracking &&
                guard->Snapshot().high_sequence == 1U,
            "capacity Prepare does not evict or mutate accepted history");
        test->Expect(
            guard->Abort(&capacity_token) &&
                guard->Snapshot().state ==
                    canonical::SequenceGuardStateV1::kTracking,
            "capacity transition may be aborted when downstream publish fails");
        const auto capacity =
            PrepareCommit(test, guard.get(), 2U, "two");
        test->Expect(
            capacity.outcome ==
                    canonical::SequenceGuardOutcomeV1::kCapacity &&
                guard->Snapshot().state ==
                    canonical::SequenceGuardStateV1::kPoisoned &&
                guard->Snapshot().seen_entries == 1U &&
                guard->Snapshot().high_sequence == 1U,
            "committed capacity exhaustion poisons without overwriting old evidence");
    }
    {
        std::unique_ptr<canonical::SequenceGuardV1> guard =
            CreateGuard(test, Config(10U, 3U));
        (void)PrepareCommit(test, guard.get(), 1U, "abc");
        const auto capacity =
            PrepareCommit(test, guard.get(), 2U, "d");
        test->Expect(
            capacity.outcome ==
                    canonical::SequenceGuardOutcomeV1::kCapacity &&
                guard->Snapshot().seen_payload_bytes == 3U &&
                guard->Snapshot().seen_entries == 1U,
            "payload-byte capacity is aggregate, fail-closed, and non-evicting");
    }
    {
        std::unique_ptr<canonical::SequenceGuardV1> guard =
            CreateGuard(test, Config(1U, 3U));
        const auto capacity =
            PrepareCommit(test, guard.get(), 10U, "four");
        const canonical::SequenceGuardSnapshotV1 snapshot =
            guard->Snapshot();
        test->Expect(
            capacity.outcome ==
                    canonical::SequenceGuardOutcomeV1::kCapacity &&
                snapshot.state ==
                    canonical::SequenceGuardStateV1::kPoisoned &&
                snapshot.start_unknown && snapshot.seen_entries == 0U &&
                snapshot.seen_payload_bytes == 0U,
            "first unconfigured observation that exceeds capacity preserves sticky START_UNKNOWN while poisoning");
    }
}

void TestUint64Boundary(TestContext* test) {
    constexpr std::uint64_t maximum =
        std::numeric_limits<std::uint64_t>::max();
    std::unique_ptr<canonical::SequenceGuardV1> guard =
        CreateGuard(test);
    const auto first = PrepareCommit(
        test, guard.get(), maximum - 3U, "a");
    const auto gap = PrepareCommit(
        test, guard.get(), maximum, "z");
    const auto duplicate = PrepareCommit(
        test, guard.get(), maximum, "z");
    test->Expect(
        first.outcome == canonical::SequenceGuardOutcomeV1::kFirst &&
            gap.outcome == canonical::SequenceGuardOutcomeV1::kGap &&
            gap.missing_begin == maximum - 2U &&
            gap.missing_end == maximum &&
            duplicate.outcome ==
                canonical::SequenceGuardOutcomeV1::kExactDuplicate &&
            guard->Snapshot().high_sequence == maximum,
        "UINT64_MAX is accepted without high+1 wrap and remains deduplicable");
}

void TestTokenVersionAndOwnership(TestContext* test) {
    std::unique_ptr<canonical::SequenceGuardV1> left =
        CreateGuard(test);
    std::unique_ptr<canonical::SequenceGuardV1> right =
        CreateGuard(test);

    canonical::SequenceGuardTokenV1 first_token;
    canonical::SequenceGuardTokenV1 stale_token;
    const auto first = left->Prepare(1U, Bytes("one"), &first_token);
    const auto stale = left->Prepare(2U, Bytes("two"), &stale_token);
    test->Expect(
        first.outcome == canonical::SequenceGuardOutcomeV1::kFirst &&
            stale.outcome == canonical::SequenceGuardOutcomeV1::kFirst,
        "two prepares observe the same unchanged version");
    test->Expect(
        right->Commit(&first_token) ==
                canonical::SequenceGuardCommitErrorV1::kWrongGuard &&
            first_token.active(),
        "foreign guard cannot consume another guard's token");
    test->Expect(
        left->Commit(&first_token) ==
            canonical::SequenceGuardCommitErrorV1::kNone,
        "token remains usable by its owner after foreign rejection");
    test->Expect(
        left->Commit(&stale_token) ==
                canonical::SequenceGuardCommitErrorV1::kVersionMismatch &&
            !stale_token.active() &&
            left->Snapshot().seen_entries == 1U &&
            left->Snapshot().high_sequence == 1U,
        "stale prepared token cannot commit over a newer version");

    canonical::SequenceGuardTokenV1 active;
    (void)left->Prepare(2U, Bytes("two"), &active);
    const auto rejected = left->Prepare(3U, Bytes("three"), &active);
    test->Expect(
        rejected.error ==
                canonical::SequenceGuardPrepareErrorV1::kTokenStillActive &&
            active.active() && left->Abort(&active),
        "Prepare never silently overwrites an active staged transaction");
}

}  // namespace

int main() {
    TestContext test;
    TestScopeKeysAndValidation(&test);
    TestFirstContiguousGapAndOldDuplicate(&test);
    TestExpectedFirst(&test);
    TestConflictBackwardAndPoisonContinuation(&test);
    TestDigestCollisionStillComparesBytes(&test);
    TestCapacityIsFailClosedAndNonEvicting(&test);
    TestUint64Boundary(&test);
    TestTokenVersionAndOwnership(&test);

    if (test.failures() == 0) {
        std::cout << "phase5 sequence guard tests passed\n";
    }
    return test.failures() == 0 ? 0 : 1;
}
