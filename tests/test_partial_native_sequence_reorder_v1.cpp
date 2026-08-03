#include "l2flow/realtime/native_sequence_recovery_v1.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <span>

namespace {

using l2flow::realtime::NativeSequenceCertifiedReadyV1;
using l2flow::realtime::NativeSequenceChannelV1;
using l2flow::realtime::NativeSequenceDescriptorV1;
using l2flow::realtime::NativeSequenceMarketV1;
using l2flow::realtime::NativeSequenceRecoveryApplyDispositionV1;
using l2flow::realtime::NativeSequenceRecoveryApplyErrorV1;
using l2flow::realtime::NativeSequenceRecoveryApplyResultV1;
using l2flow::realtime::NativeSequenceRecoveryChannelSnapshotV1;
using l2flow::realtime::NativeSequenceRecoveryChannelStateV1;
using l2flow::realtime::NativeSequenceRecoveryCommitErrorV1;
using l2flow::realtime::NativeSequenceRecoveryConfigV1;
using l2flow::realtime::NativeSequenceRecoveryCoordinatorV1;
using l2flow::realtime::NativeSequenceRecoveryCoverageModeV1;
using l2flow::realtime::NativeSequenceRecoveryCreateErrorV1;
using l2flow::realtime::NativeSequenceRecoveryFreezeReasonV1;
using l2flow::realtime::NativeSequenceRecoveryObserveDispositionV1;
using l2flow::realtime::NativeSequenceRecoveryObserveErrorV1;
using l2flow::realtime::NativeSequenceRecoveryObserveResultV1;
using l2flow::realtime::NativeSequenceRecoveryOriginProofV1;
using l2flow::realtime::NativeSequenceRecoveryPollErrorV1;
using l2flow::realtime::NativeSequenceRecoveryRecordClassV1;
using l2flow::realtime::NativeSequenceRecoverySealDispositionV1;
using l2flow::realtime::NativeSequenceRecoverySealErrorV1;
using l2flow::realtime::NativeSequenceRecoverySealResultV1;
using l2flow::sdk::MessageKey;

constexpr MessageKey kShenzhenOrder{6U, 101U, 33U};
constexpr MessageKey kShenzhenTransaction{6U, 101U, 36U};
constexpr MessageKey kShanghaiTick{4U, 101U, 24U};

bool Expect(bool condition, const char* message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

NativeSequenceRecoveryConfigV1 PartialConfig() {
    NativeSequenceRecoveryConfigV1 config{};
    config.coverage_mode =
        NativeSequenceRecoveryCoverageModeV1::kProcessStartPartial;
    config.maximum_channels = 8U;
    config.maximum_pending_entries = 64U;
    config.maximum_pending_entries_per_channel = 16U;
    config.certified_duplicate_retention_entries = 16U;
    config.maximum_canonical_payload_bytes_per_entry = 16U;
    config.maximum_total_canonical_payload_bytes = 1024U;
    config.maximum_reorder_span = 64U;
    return config;
}

std::unique_ptr<NativeSequenceRecoveryCoordinatorV1> MakeCoordinator(
    NativeSequenceRecoveryConfigV1 config,
    bool* ok) {
    std::unique_ptr<NativeSequenceRecoveryCoordinatorV1> coordinator;
    *ok &= Expect(
        NativeSequenceRecoveryCoordinatorV1::Create(
            config, &coordinator) ==
                NativeSequenceRecoveryCreateErrorV1::kNone &&
            coordinator != nullptr,
        "create process-start partial coordinator");
    return coordinator;
}

NativeSequenceDescriptorV1 Shenzhen(
    std::uint32_t channel,
    std::uint64_t sequence) {
    return NativeSequenceDescriptorV1{
        NativeSequenceChannelV1{
            NativeSequenceMarketV1::kShenzhen, channel},
        sequence};
}

NativeSequenceDescriptorV1 Shanghai(
    std::uint32_t channel,
    std::uint64_t sequence) {
    return NativeSequenceDescriptorV1{
        NativeSequenceChannelV1{
            NativeSequenceMarketV1::kShanghai, channel},
        sequence};
}

std::array<std::byte, 4U> Payload(std::uint32_t value) {
    return {
        static_cast<std::byte>(value & 0xffU),
        static_cast<std::byte>((value >> 8U) & 0xffU),
        static_cast<std::byte>((value >> 16U) & 0xffU),
        static_cast<std::byte>((value >> 24U) & 0xffU),
    };
}

bool Observe(
    NativeSequenceRecoveryCoordinatorV1* coordinator,
    const NativeSequenceDescriptorV1& descriptor,
    const MessageKey& key,
    NativeSequenceRecoveryRecordClassV1 record_class,
    NativeSequenceRecoveryObserveResultV1* output) {
    return Expect(
        coordinator->Observe(
            descriptor, key, record_class, output) ==
            NativeSequenceRecoveryObserveErrorV1::kNone,
        "observe native position");
}

bool Apply(
    NativeSequenceRecoveryCoordinatorV1* coordinator,
    const NativeSequenceDescriptorV1& descriptor,
    const MessageKey& key,
    std::span<const std::byte> payload,
    std::uint64_t cookie,
    NativeSequenceRecoveryApplyResultV1* output) {
    return Expect(
        coordinator->MarkTargetApplied(
            descriptor, key, payload, cookie, output) ==
            NativeSequenceRecoveryApplyErrorV1::kNone,
        "join canonical applied payload");
}

bool PollAndCommit(
    NativeSequenceRecoveryCoordinatorV1* coordinator,
    std::uint64_t expected_sequence,
    std::uint64_t expected_cookie,
    NativeSequenceRecoveryRecordClassV1 expected_class) {
    NativeSequenceCertifiedReadyV1 ready{};
    bool ok = Expect(
        coordinator->PollCertified(&ready) ==
                NativeSequenceRecoveryPollErrorV1::kNone &&
            ready.descriptor.sequence == expected_sequence &&
            ready.applied_cookie == expected_cookie &&
            ready.record_class == expected_class,
        "poll next partial position in native order");
    if (ok) {
        ok &= Expect(
            coordinator->CommitCertified(ready.token) ==
                NativeSequenceRecoveryCommitErrorV1::kNone,
            "commit partial native position");
    }
    return ok;
}

bool TestStartupLateLowerAndShenzhenSharedDomain() {
    bool ok = true;
    auto coordinator = MakeCoordinator(PartialConfig(), &ok);
    if (!coordinator) {
        return false;
    }

    NativeSequenceRecoveryObserveResultV1 observed{};
    NativeSequenceRecoveryApplyResultV1 applied{};
    const auto payload109 = Payload(109U);
    ok &= Observe(
        coordinator.get(),
        Shenzhen(2012U, 109U),
        kShenzhenTransaction,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Apply(
        coordinator.get(),
        Shenzhen(2012U, 109U),
        kShenzhenTransaction,
        payload109,
        1009U,
        &applied);

    const auto payload108 = Payload(108U);
    ok &= Observe(
        coordinator.get(),
        Shenzhen(2012U, 108U),
        kShenzhenOrder,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Expect(
        observed.disposition ==
                NativeSequenceRecoveryObserveDispositionV1::kBackfill &&
            !observed.origin_sealed &&
            observed.origin_sequence == 108U &&
            observed.observed_contiguous_sequence == 109U,
        "startup late-lower rebases an unsealed SZ 33/36 domain");
    ok &= Apply(
        coordinator.get(),
        Shenzhen(2012U, 108U),
        kShenzhenOrder,
        payload108,
        1008U,
        &applied);

    ok &= Observe(
        coordinator.get(),
        Shenzhen(2012U, 110U),
        kShenzhenOrder,
        NativeSequenceRecoveryRecordClassV1::kFiltered,
        &observed);
    NativeSequenceCertifiedReadyV1 ready{};
    ok &= Expect(
        coordinator->PollCertified(&ready) ==
            NativeSequenceRecoveryPollErrorV1::kNotReady,
        "unsealed partial origin cannot publish first-seen data");
    ok &= Expect(
        coordinator->Snapshot().channel_count == 1U &&
            coordinator->Snapshot().unsealed_channel_count == 1U,
        "SZ order and transaction occupy one channel table entry");

    NativeSequenceRecoverySealResultV1 sealed{};
    ok &= Expect(
        coordinator->SealBoundedOrigin(
            Shenzhen(2012U, 108U).domain, 108U, &sealed) ==
                NativeSequenceRecoverySealErrorV1::kNone &&
            sealed.disposition ==
                NativeSequenceRecoverySealDispositionV1::kSealed &&
            sealed.origin_proof ==
                NativeSequenceRecoveryOriginProofV1::kBoundedPartial &&
            sealed.observed_contiguous_sequence == 110U,
        "explicit bounded seal makes the dense startup prefix eligible");

    ok &= PollAndCommit(
        coordinator.get(),
        108U,
        1008U,
        NativeSequenceRecoveryRecordClassV1::kTarget);
    ok &= PollAndCommit(
        coordinator.get(),
        109U,
        1009U,
        NativeSequenceRecoveryRecordClassV1::kTarget);
    ok &= PollAndCommit(
        coordinator.get(),
        110U,
        0U,
        NativeSequenceRecoveryRecordClassV1::kFiltered);

    NativeSequenceRecoveryChannelSnapshotV1 snapshot{};
    ok &= Expect(
        coordinator->ChannelSnapshot(
            Shenzhen(2012U, 108U).domain, &snapshot) &&
            snapshot.state ==
                NativeSequenceRecoveryChannelStateV1::kHealthy &&
            snapshot.certified_sequence == 110U &&
            snapshot.filtered_sequences_certified == 1U &&
            snapshot.origin_proof ==
                NativeSequenceRecoveryOriginProofV1::kBoundedPartial,
        "bounded SZ prefix drains 108, 109, filtered 110 in native order");
    return ok;
}

bool TestProvenSealGapBackfillAndAppliedBeforeObserve() {
    bool ok = true;
    auto coordinator = MakeCoordinator(PartialConfig(), &ok);
    if (!coordinator) {
        return false;
    }
    const auto channel = Shanghai(7U, 200U).domain;
    NativeSequenceRecoverySealResultV1 sealed{};
    ok &= Expect(
        coordinator->SealProvenOrigin(channel, 200U, &sealed) ==
                NativeSequenceRecoverySealErrorV1::kNone &&
            sealed.disposition ==
                NativeSequenceRecoverySealDispositionV1::kSealed &&
            sealed.origin_proof ==
                NativeSequenceRecoveryOriginProofV1::kNativeOrderProven,
        "caller-proven origin is distinct from bounded partial quality");

    NativeSequenceRecoveryObserveResultV1 observed{};
    NativeSequenceRecoveryApplyResultV1 applied{};
    const auto payload200 = Payload(200U);
    ok &= Apply(
        coordinator.get(),
        Shanghai(7U, 200U),
        kShanghaiTick,
        payload200,
        2200U,
        &applied);
    ok &= Expect(
        applied.applied_arrivals == 1U &&
            applied.observed_arrivals == 0U,
        "partial coordinator retains applied-before-observed join");
    ok &= Observe(
        coordinator.get(),
        Shanghai(7U, 200U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= PollAndCommit(
        coordinator.get(),
        200U,
        2200U,
        NativeSequenceRecoveryRecordClassV1::kTarget);

    ok &= Observe(
        coordinator.get(),
        Shanghai(7U, 202U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kFiltered,
        &observed);
    ok &= Expect(
        observed.disposition ==
                NativeSequenceRecoveryObserveDispositionV1::kGapOpened &&
            observed.channel_state ==
                NativeSequenceRecoveryChannelStateV1::kRepairing,
        "future filtered position opens a real native gap");
    NativeSequenceCertifiedReadyV1 ready{};
    ok &= Expect(
        coordinator->PollCertified(&ready) ==
            NativeSequenceRecoveryPollErrorV1::kNotReady,
        "poll never advances over a gap without a timeout escape");

    const auto payload201 = Payload(201U);
    ok &= Observe(
        coordinator.get(),
        Shanghai(7U, 201U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Expect(
        observed.disposition ==
                NativeSequenceRecoveryObserveDispositionV1::kBackfill &&
            observed.observed_contiguous_sequence == 202U,
        "missing native position closes the observed gap");
    ok &= Apply(
        coordinator.get(),
        Shanghai(7U, 201U),
        kShanghaiTick,
        payload201,
        2201U,
        &applied);
    ok &= PollAndCommit(
        coordinator.get(),
        201U,
        2201U,
        NativeSequenceRecoveryRecordClassV1::kTarget);
    ok &= PollAndCommit(
        coordinator.get(),
        202U,
        0U,
        NativeSequenceRecoveryRecordClassV1::kFiltered);
    return ok;
}

bool TestDuplicateConflictAndChannelIsolation() {
    bool ok = true;
    auto coordinator = MakeCoordinator(PartialConfig(), &ok);
    if (!coordinator) {
        return false;
    }
    NativeSequenceRecoverySealResultV1 sealed{};
    ok &= Expect(
        coordinator->SealBoundedOrigin(
            Shenzhen(4U, 300U).domain, 300U, &sealed) ==
            NativeSequenceRecoverySealErrorV1::kNone,
        "seal duplicate test channel");

    NativeSequenceRecoveryObserveResultV1 observed{};
    NativeSequenceRecoveryApplyResultV1 applied{};
    const auto payload300 = Payload(300U);
    ok &= Observe(
        coordinator.get(),
        Shenzhen(4U, 300U),
        kShenzhenOrder,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Observe(
        coordinator.get(),
        Shenzhen(4U, 300U),
        kShenzhenOrder,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Expect(
        observed.disposition ==
            NativeSequenceRecoveryObserveDispositionV1::
                kDuplicatePending,
        "duplicate waits for canonical payload equality");
    ok &= Apply(
        coordinator.get(),
        Shenzhen(4U, 300U),
        kShenzhenOrder,
        payload300,
        3300U,
        &applied);
    NativeSequenceCertifiedReadyV1 ready{};
    ok &= Expect(
        coordinator->PollCertified(&ready) ==
            NativeSequenceRecoveryPollErrorV1::kNotReady,
        "one applied payload cannot discharge two observed arrivals");
    ok &= Apply(
        coordinator.get(),
        Shenzhen(4U, 300U),
        kShenzhenOrder,
        payload300,
        3301U,
        &applied);
    ok &= Expect(
        applied.disposition ==
                NativeSequenceRecoveryApplyDispositionV1::
                    kExactDuplicate &&
            applied.canonical_applied_cookie == 3300U,
        "exact duplicate preserves first canonical cookie");
    ok &= PollAndCommit(
        coordinator.get(),
        300U,
        3300U,
        NativeSequenceRecoveryRecordClassV1::kTarget);

    ok &= Observe(
        coordinator.get(),
        Shenzhen(4U, 301U),
        kShenzhenOrder,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Observe(
        coordinator.get(),
        Shenzhen(4U, 301U),
        kShenzhenTransaction,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Expect(
        observed.disposition ==
                NativeSequenceRecoveryObserveDispositionV1::
                    kPayloadConflict &&
            observed.channel_state ==
                NativeSequenceRecoveryChannelStateV1::kFrozenConflict,
        "same SZ native key cannot change 6.33/6.36 identity");

    ok &= Expect(
        coordinator->SealProvenOrigin(
            Shanghai(9U, 1U).domain, 1U, &sealed) ==
            NativeSequenceRecoverySealErrorV1::kNone,
        "seal independent channel after peer conflict");
    const auto other_payload = Payload(1U);
    ok &= Observe(
        coordinator.get(),
        Shanghai(9U, 1U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Apply(
        coordinator.get(),
        Shanghai(9U, 1U),
        kShanghaiTick,
        other_payload,
        9001U,
        &applied);
    ok &= PollAndCommit(
        coordinator.get(),
        1U,
        9001U,
        NativeSequenceRecoveryRecordClassV1::kTarget);
    return ok;
}

bool TestOutsideRetentionDuplicateArrivalOrderInvariant() {
    bool ok = true;
    NativeSequenceRecoveryConfigV1 config = PartialConfig();
    config.certified_duplicate_retention_entries = 0U;
    auto observe_first = MakeCoordinator(config, &ok);
    auto apply_first = MakeCoordinator(config, &ok);
    if (!observe_first || !apply_first) {
        return false;
    }

    const auto descriptor = Shenzhen(8U, 350U);
    const auto payload = Payload(350U);
    const auto prime = [&](NativeSequenceRecoveryCoordinatorV1* coordinator) {
        NativeSequenceRecoverySealResultV1 sealed{};
        NativeSequenceRecoveryObserveResultV1 observed{};
        NativeSequenceRecoveryApplyResultV1 applied{};
        bool primed = Expect(
            coordinator->SealBoundedOrigin(
                descriptor.domain, descriptor.sequence, &sealed) ==
                    NativeSequenceRecoverySealErrorV1::kNone &&
                sealed.disposition ==
                    NativeSequenceRecoverySealDispositionV1::kSealed,
            "seal outside-retention duplicate channel");
        primed &= Observe(
            coordinator,
            descriptor,
            kShenzhenOrder,
            NativeSequenceRecoveryRecordClassV1::kTarget,
            &observed);
        primed &= Apply(
            coordinator,
            descriptor,
            kShenzhenOrder,
            payload,
            8350U,
            &applied);
        primed &= PollAndCommit(
            coordinator,
            descriptor.sequence,
            8350U,
            NativeSequenceRecoveryRecordClassV1::kTarget);
        return primed;
    };
    ok &= prime(observe_first.get());
    ok &= prime(apply_first.get());

    NativeSequenceRecoveryObserveResultV1 observed{};
    ok &= Observe(
        observe_first.get(),
        descriptor,
        kShenzhenOrder,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Expect(
        observed.disposition ==
                NativeSequenceRecoveryObserveDispositionV1::
                    kResourceFrozen &&
            observed.channel_state ==
                NativeSequenceRecoveryChannelStateV1::kFrozenResource &&
            !observed.correction_required,
        "observe-first duplicate outside retention freezes as unverifiable");

    NativeSequenceRecoveryApplyResultV1 applied{};
    ok &= Apply(
        apply_first.get(),
        descriptor,
        kShenzhenOrder,
        payload,
        8351U,
        &applied);
    ok &= Expect(
        applied.disposition ==
                NativeSequenceRecoveryApplyDispositionV1::
                    kResourceFrozen &&
            applied.channel_state ==
                NativeSequenceRecoveryChannelStateV1::kFrozenResource &&
            applied.freeze_reason ==
                NativeSequenceRecoveryFreezeReasonV1::
                    kDuplicateVerificationUnavailable &&
            !applied.correction_required,
        "apply-first duplicate outside retention has the same frozen outcome");

    NativeSequenceRecoveryChannelSnapshotV1 observe_snapshot{};
    NativeSequenceRecoveryChannelSnapshotV1 apply_snapshot{};
    ok &= Expect(
        observe_first->ChannelSnapshot(
            descriptor.domain, &observe_snapshot) &&
            apply_first->ChannelSnapshot(
                descriptor.domain, &apply_snapshot) &&
            observe_snapshot.state == apply_snapshot.state &&
            observe_snapshot.freeze_reason == apply_snapshot.freeze_reason &&
            observe_snapshot.certified_sequence ==
                apply_snapshot.certified_sequence &&
            observe_snapshot.freeze_reason ==
                NativeSequenceRecoveryFreezeReasonV1::
                    kDuplicateVerificationUnavailable &&
            observe_snapshot.certified_sequence == descriptor.sequence &&
            !observe_snapshot.correction_required &&
            !apply_snapshot.correction_required,
        "outside-retention duplicate terminal state is arrival-order invariant");
    return ok;
}

bool TestBoundedAndProvenLateLowerRequireCorrection() {
    bool ok = true;
    auto coordinator = MakeCoordinator(PartialConfig(), &ok);
    if (!coordinator) {
        return false;
    }
    NativeSequenceRecoverySealResultV1 sealed{};
    ok &= Expect(
        coordinator->SealBoundedOrigin(
            Shenzhen(10U, 400U).domain, 400U, &sealed) ==
            NativeSequenceRecoverySealErrorV1::kNone,
        "seal bounded correction test channel");
    NativeSequenceRecoveryObserveResultV1 observed{};
    NativeSequenceRecoveryApplyResultV1 applied{};
    const auto payload400 = Payload(400U);
    ok &= Observe(
        coordinator.get(),
        Shenzhen(10U, 400U),
        kShenzhenOrder,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Apply(
        coordinator.get(),
        Shenzhen(10U, 400U),
        kShenzhenOrder,
        payload400,
        4400U,
        &applied);
    ok &= PollAndCommit(
        coordinator.get(),
        400U,
        4400U,
        NativeSequenceRecoveryRecordClassV1::kTarget);

    ok &= Observe(
        coordinator.get(),
        Shenzhen(10U, 399U),
        kShenzhenTransaction,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Expect(
        observed.disposition ==
                NativeSequenceRecoveryObserveDispositionV1::
                    kCorrectionRequired &&
            observed.correction_required &&
            observed.correction_sequence == 399U &&
            observed.target_token_available(),
        "bounded late-before-origin is retained and requests correction");
    const auto payload399 = Payload(399U);
    ok &= Expect(
        coordinator->MarkTargetApplied(
            observed.token,
            kShenzhenTransaction,
            payload399,
            4399U,
            &applied) ==
                NativeSequenceRecoveryApplyErrorV1::kNone &&
            applied.disposition ==
                NativeSequenceRecoveryApplyDispositionV1::
                    kCorrectionRequired,
        "correction record still joins its canonical applied payload");
    ok &= Expect(
        coordinator->CommitCertified(observed.token) ==
            NativeSequenceRecoveryCommitErrorV1::kCorrectionRequired,
        "correction channel cannot mutate the committed prefix");

    NativeSequenceRecoveryChannelSnapshotV1 snapshot{};
    ok &= Expect(
        coordinator->ChannelSnapshot(
            Shenzhen(10U, 400U).domain, &snapshot) &&
            snapshot.certified_sequence == 400U &&
            snapshot.correction_required &&
            snapshot.correction_sequence == 399U &&
            snapshot.correction_arrivals == 1U,
        "last-good bounded prefix remains visible during correction");

    ok &= Expect(
        coordinator->SealProvenOrigin(
            Shanghai(11U, 500U).domain, 500U, &sealed) ==
            NativeSequenceRecoverySealErrorV1::kNone,
        "seal caller-proven correction test channel");
    const auto payload499 = Payload(499U);
    ok &= Apply(
        coordinator.get(),
        Shanghai(11U, 499U),
        kShanghaiTick,
        payload499,
        5499U,
        &applied);
    ok &= Expect(
        applied.disposition ==
                NativeSequenceRecoveryApplyDispositionV1::
                    kCorrectionRequired &&
            applied.correction_sequence == 499U,
        "applied-before-observed late-lower also requests correction");
    ok &= Observe(
        coordinator.get(),
        Shanghai(11U, 499U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Expect(
        observed.disposition ==
                NativeSequenceRecoveryObserveDispositionV1::
                    kCorrectionRequired &&
            observed.target_token_available() &&
            observed.origin_proof ==
                NativeSequenceRecoveryOriginProofV1::
                    kNativeOrderProven,
        "late-lower contradicting a proven seal is never discarded");
    ok &= Expect(
        coordinator->Snapshot().correction_required_channel_count == 2U,
        "both bounded and proven contradictions are independently visible");
    return ok;
}

bool TestSealValidationAndCapacity() {
    bool ok = true;
    auto coordinator = MakeCoordinator(PartialConfig(), &ok);
    if (!coordinator) {
        return false;
    }
    NativeSequenceRecoveryObserveResultV1 observed{};
    ok &= Observe(
        coordinator.get(),
        Shanghai(12U, 901U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kFiltered,
        &observed);
    NativeSequenceRecoverySealResultV1 sealed{};
    ok &= Expect(
        coordinator->SealBoundedOrigin(
            Shanghai(12U, 901U).domain, 902U, &sealed) ==
                NativeSequenceRecoverySealErrorV1::kNone &&
            sealed.disposition ==
                NativeSequenceRecoverySealDispositionV1::
                    kOriginExcludesStaged,
        "seal cannot exclude an already staged native position");
    ok &= Expect(
        coordinator->SealBoundedOrigin(
            Shanghai(12U, 901U).domain, 901U, &sealed) ==
                NativeSequenceRecoverySealErrorV1::kNone &&
            sealed.disposition ==
                NativeSequenceRecoverySealDispositionV1::kSealed,
        "rejected seal leaves channel retryable with a defensible origin");

    NativeSequenceRecoveryConfigV1 capacity_config = PartialConfig();
    capacity_config.maximum_pending_entries = 2U;
    capacity_config.maximum_pending_entries_per_channel = 2U;
    capacity_config.certified_duplicate_retention_entries = 0U;
    auto limited = MakeCoordinator(capacity_config, &ok);
    if (!limited) {
        return false;
    }
    ok &= Observe(
        limited.get(),
        Shenzhen(15U, 700U),
        kShenzhenOrder,
        NativeSequenceRecoveryRecordClassV1::kFiltered,
        &observed);
    ok &= Observe(
        limited.get(),
        Shenzhen(15U, 702U),
        kShenzhenTransaction,
        NativeSequenceRecoveryRecordClassV1::kFiltered,
        &observed);
    ok &= Observe(
        limited.get(),
        Shenzhen(15U, 701U),
        kShenzhenOrder,
        NativeSequenceRecoveryRecordClassV1::kFiltered,
        &observed);
    ok &= Expect(
        observed.disposition ==
                NativeSequenceRecoveryObserveDispositionV1::
                    kResourceFrozen &&
            observed.channel_state ==
                NativeSequenceRecoveryChannelStateV1::kFrozenResource,
        "partial pending capacity is a hard per-channel bound");
    NativeSequenceRecoveryChannelSnapshotV1 snapshot{};
    ok &= Expect(
        limited->ChannelSnapshot(
            Shenzhen(15U, 700U).domain, &snapshot) &&
            snapshot.freeze_reason ==
                NativeSequenceRecoveryFreezeReasonV1::
                    kPerChannelEntryCapacity,
        "capacity exhaustion exposes the exact freeze reason");
    return ok;
}

bool TestPartialConfigurationIsExplicit() {
    bool ok = true;
    NativeSequenceRecoveryConfigV1 invalid = PartialConfig();
    invalid.trusted_checkpoint_sequence = 10U;
    std::unique_ptr<NativeSequenceRecoveryCoordinatorV1> coordinator;
    ok &= Expect(
        NativeSequenceRecoveryCoordinatorV1::Create(
            invalid, &coordinator) ==
            NativeSequenceRecoveryCreateErrorV1::
                kInvalidConfiguration,
        "partial mode cannot smuggle in one global trusted checkpoint");

    NativeSequenceRecoveryConfigV1 explicit_config = PartialConfig();
    explicit_config.coverage_mode =
        NativeSequenceRecoveryCoverageModeV1::kExplicitOrigin;
    coordinator = MakeCoordinator(explicit_config, &ok);
    if (!coordinator) {
        return false;
    }
    NativeSequenceRecoverySealResultV1 sealed{};
    ok &= Expect(
        coordinator->SealBoundedOrigin(
            Shanghai(1U, 1U).domain, 1U, &sealed) ==
            NativeSequenceRecoverySealErrorV1::kWrongCoverageMode,
        "FROM_OPEN mode cannot be relabeled bounded partial");
    return ok;
}

}  // namespace

int main() {
    bool ok = true;
    ok &= TestStartupLateLowerAndShenzhenSharedDomain();
    ok &= TestProvenSealGapBackfillAndAppliedBeforeObserve();
    ok &= TestDuplicateConflictAndChannelIsolation();
    ok &= TestOutsideRetentionDuplicateArrivalOrderInvariant();
    ok &= TestBoundedAndProvenLateLowerRequireCorrection();
    ok &= TestSealValidationAndCapacity();
    ok &= TestPartialConfigurationIsExplicit();
    if (!ok) {
        return 1;
    }
    std::cout << "partial native sequence reorder V1 tests passed\n";
    return 0;
}
