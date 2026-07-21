#include "l2flow/ingress/raw_recovery_executor.h"

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace ingress = l2flow::ingress;

namespace {

class FakeIo final : public ingress::RawRecoveryIo {
public:
    bool DependentArtifactsAbsentProven()
        const noexcept override {
        return dependent_artifacts_absent_proven;
    }
    bool R11OrphanAdoptionAuthorized(
        const ingress::RawRecoveryPlanV1&)
        const noexcept override {
        return r11_orphan_adoption_authorized;
    }
    int SyncParentDirectories() noexcept override {
        events.emplace_back("parents");
        return TakeFailure();
    }
    int TruncateJournal(std::uint64_t size) noexcept override {
        events.emplace_back(
            "truncate-journal:" + std::to_string(size));
        return TakeFailure();
    }
    int SyncJournal() noexcept override {
        events.emplace_back("sync-journal");
        return TakeFailure();
    }
    int TruncateSegment(
        std::uint32_t sequence,
        std::uint64_t size) noexcept override {
        events.emplace_back(
            "truncate-segment:" +
            std::to_string(sequence) + ":" +
            std::to_string(size));
        return TakeFailure();
    }
    int SyncSegment(
        std::uint32_t sequence,
        bool metadata) noexcept override {
        events.emplace_back(
            "sync-segment:" +
            std::to_string(sequence) + ":" +
            (metadata ? "full" : "data"));
        return TakeFailure();
    }
    ingress::RawRecoveryWriteResult WriteJournalSome(
        std::uint64_t offset,
        std::span<const std::byte> bytes) noexcept override {
        events.emplace_back(
            "write-journal:" + std::to_string(offset));
        const int failure = TakeFailure();
        if (failure != 0) {
            return {0U, failure};
        }
        const std::size_t count =
            maximum_write == 0U
                ? bytes.size()
                : std::min(maximum_write, bytes.size());
        return {count, 0};
    }

    int TakeFailure() noexcept {
        ++calls;
        if (fail_call != 0U && calls == fail_call) {
            return EIO;
        }
        return 0;
    }

    std::vector<std::string> events;
    std::size_t maximum_write = 0U;
    std::size_t fail_call = 0U;
    std::size_t calls = 0U;
    bool dependent_artifacts_absent_proven = false;
    bool r11_orphan_adoption_authorized = false;
};

struct TestContext final {
    void Expect(bool condition, const std::string& description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    int failures = 0;
};

ingress::RawRecoveryPlanV1 BasePlan() {
    ingress::RawRecoveryPlanV1 plan;
    plan.journal_header.source_stream_id = 1001U;
    plan.accepted_journal_size = 4096U + 48U;
    plan.has_accepted_cursor = true;
    plan.accepted_cursor = {
        1U, 4096U, 0U, 4096U, 0U};
    return plan;
}

}  // namespace

int main() {
    TestContext test;

    ingress::RawRecoveryPlanV1 fatal = BasePlan();
    fatal.fatal =
        ingress::RawRecoveryFatalV1::kRawDurableCorruption;
    FakeIo untouched;
    const auto rejected =
        ingress::ExecuteRawRecoveryPlanV1(
            fatal, untouched);
    test.Expect(
        rejected.failure ==
                ingress::RawRecoveryExecutionFailureV1::
                    kPlanFatal &&
            untouched.events.empty(),
        "fatal analysis plan causes zero mutation and zero sync");

    ingress::RawRecoveryPlanV1 rollback = BasePlan();
    rollback.journal_tail =
        ingress::RawRecoveryJournalTailV1::kPartialMarker;
    FakeIo missing_proof;
    const auto proof_rejected =
        ingress::ExecuteRawRecoveryPlanV1(
            rollback, missing_proof);
    test.Expect(
        proof_rejected.failure ==
                ingress::RawRecoveryExecutionFailureV1::
                    kDependentArtifactProofMissing &&
            missing_proof.events.empty(),
        "journal rollback requires independent artifact proof");

    ingress::RawRecoverySegmentPlanV1 segment;
    segment.segment_sequence = 1U;
    segment.segment_base_wal_pos = 0U;
    segment.durable_end_offset = 4096U;
    segment.validated_logical_end_offset = 4240U;
    segment.validated_last_ingress_sequence = 1U;
    segment.append_only_begin_offset = 4096U;
    segment.append_only_end_offset = 4240U;
    segment.tail_begin_offset = 4240U;
    segment.tail_end_offset = 4300U;
    segment.tail =
        ingress::RawRecoverySegmentTailV1::kPartialRecord;
    rollback.segments.push_back(segment);

    FakeIo io;
    io.maximum_write = 7U;
    io.dependent_artifacts_absent_proven = true;
    const auto applied =
        ingress::ExecuteRawRecoveryPlanV1(
            rollback, io);
    test.Expect(
        applied.failure ==
                ingress::RawRecoveryExecutionFailureV1::kNone &&
            applied.cursor_publishable &&
            applied.mutated &&
            applied.recovered_cursor.segment_offset == 4240U,
        "rollback, tail repair and append-only promotion complete");
    const std::vector<std::string> expected_prefix{
        "parents",
        "truncate-journal:4144",
        "sync-journal",
        "truncate-segment:1:4240",
        "sync-segment:1:data",
        "sync-segment:1:data"};
    test.Expect(
        io.events.size() > expected_prefix.size() &&
            std::equal(
                expected_prefix.begin(),
                expected_prefix.end(),
                io.events.begin()) &&
            io.events.back() == "sync-journal",
        "repair ordering starts with parent/journal/segment barriers and ends with journal sync");

    FakeIo failure;
    failure.fail_call = 4U;
    failure.dependent_artifacts_absent_proven = true;
    const auto interrupted =
        ingress::ExecuteRawRecoveryPlanV1(
            rollback, failure);
    test.Expect(
        interrupted.failure ==
                ingress::RawRecoveryExecutionFailureV1::
                    kSegmentTruncate &&
            !interrupted.cursor_publishable,
        "mid-repair failure never publishes a recovered cursor");

    ingress::RawRecoveryPlanV1 initial;
    initial.journal_header.source_stream_id = 1001U;
    initial.accepted_journal_size = 4096U;
    initial.initial_anchor =
        ingress::RawRecoveryInitialAnchorV1::
            kSegmentHeaderOnly;
    ingress::RawRecoverySegmentPlanV1 initial_segment;
    initial_segment.segment_sequence = 1U;
    initial_segment.validated_logical_end_offset = 4096U;
    initial.segments.push_back(initial_segment);
    FakeIo initial_io;
    const auto adopted =
        ingress::ExecuteRawRecoveryPlanV1(
            initial, initial_io);
    test.Expect(
        adopted.failure ==
                ingress::RawRecoveryExecutionFailureV1::kNone &&
            adopted.cursor_publishable &&
            adopted.recovered_cursor.global_wal_pos == 4096U &&
            adopted.recovered_cursor.ingress_sequence == 0U,
        "header-only initial segment gets a synchronized initial marker");

    ingress::RawRecoveryPlanV1 orphan = BasePlan();
    orphan.accepted_cursor = {
        1U, 5000U, 3U, 5000U,
        ingress::kRawV1SegmentSealed};
    orphan.r11_orphan =
        ingress::RawRecoveryR11OrphanV1::
            kNormalHeaderOnly;
    ingress::RawRecoverySegmentPlanV1 sealed;
    sealed.segment_sequence = 1U;
    sealed.segment_base_wal_pos = 0U;
    sealed.durable_end_offset = 5000U;
    sealed.validated_logical_end_offset = 5000U;
    sealed.sealed = true;
    ingress::RawRecoverySegmentPlanV1 next;
    next.segment_sequence = 2U;
    next.segment_base_wal_pos = 5000U;
    next.validated_logical_end_offset = 4096U;
    next.validated_last_ingress_sequence = 3U;
    next.append_only_begin_offset = 4096U;
    next.append_only_end_offset = 4096U;
    next.tail_begin_offset = 4096U;
    next.tail_end_offset = 8192U;
    next.tail =
        ingress::RawRecoverySegmentTailV1::
            kZeroPreallocation;
    orphan.segments = {sealed, next};
    FakeIo orphan_denied;
    const auto denied =
        ingress::ExecuteRawRecoveryPlanV1(
            orphan, orphan_denied);
    test.Expect(
        denied.failure ==
                ingress::RawRecoveryExecutionFailureV1::
                    kR11OrphanAuthorizationMissing &&
            orphan_denied.events.empty(),
        "R11 orphan adoption requires coordinator-backed authorization before mutation");
    FakeIo orphan_io;
    orphan_io.r11_orphan_adoption_authorized = true;
    const auto orphan_adopted =
        ingress::ExecuteRawRecoveryPlanV1(
            orphan, orphan_io);
    test.Expect(
        orphan_adopted.failure ==
                ingress::RawRecoveryExecutionFailureV1::kNone &&
            orphan_adopted.cursor_publishable &&
            orphan_adopted.recovered_cursor
                    .segment_sequence == 2U &&
            orphan_adopted.recovered_cursor
                    .segment_offset == 4096U &&
            std::find(
                orphan_io.events.begin(),
                orphan_io.events.end(),
                "truncate-segment:2:4096") ==
                orphan_io.events.end(),
        "authorized R11 adoption preserves preallocation and publishes the exact header-only marker");

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Phase 2 recovery-executor tests failed\n";
        return 1;
    }
    std::cout << "Phase 2 recovery-executor tests passed\n";
    return 0;
}
