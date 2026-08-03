#include "l2flow/realtime/native_sequence_recovery_v1.h"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string_view>
#include <type_traits>
#include <vector>

namespace {

using l2flow::realtime::ExtractNativeSequenceV1;
using l2flow::realtime::NativeSequenceCertifiedReadyV1;
using l2flow::realtime::NativeSequenceChannelV1;
using l2flow::realtime::NativeSequenceDescriptorV1;
using l2flow::realtime::NativeSequenceExtractErrorV1;
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
using l2flow::realtime::NativeSequenceRecoveryPollErrorV1;
using l2flow::realtime::NativeSequenceRecoveryRecordClassV1;
using l2flow::realtime::NativeSequenceRecoverySealDispositionV1;
using l2flow::realtime::NativeSequenceRecoverySealErrorV1;
using l2flow::realtime::NativeSequenceRecoverySealResultV1;
using l2flow::realtime::NativeSequenceRecoveryTokenV1;
using l2flow::sdk::MessageKey;

constexpr MessageKey kShanghaiTick{4U, 101U, 24U};
constexpr MessageKey kShanghaiSnapshot{4U, 101U, 4U};
constexpr MessageKey kShenzhenSnapshot{6U, 101U, 28U};
constexpr MessageKey kShenzhenOrder{6U, 101U, 33U};
constexpr MessageKey kShenzhenTransaction{6U, 101U, 36U};
constexpr MessageKey kForbiddenCombined{6U, 101U, 53U};

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

template <typename Unsigned>
void WriteLittleEndian(
    std::span<std::byte> bytes,
    std::size_t offset,
    Unsigned value) {
    static_assert(std::is_unsigned_v<Unsigned>);
    for (std::size_t index = 0U; index < sizeof(Unsigned); ++index) {
        const auto shift =
            static_cast<unsigned int>(index * 8U);
        const auto octet = static_cast<unsigned char>(
            (value >> shift) & static_cast<Unsigned>(0xffU));
        bytes[offset + index] = static_cast<std::byte>(octet);
    }
}

std::array<std::byte, 12U> ShanghaiSequenceBody(
    std::int64_t sequence,
    std::int32_t channel) {
    std::array<std::byte, 12U> output{};
    WriteLittleEndian<std::uint64_t>(
        output,
        0U,
        std::bit_cast<std::uint64_t>(sequence));
    WriteLittleEndian<std::uint32_t>(
        output,
        8U,
        std::bit_cast<std::uint32_t>(channel));
    return output;
}

std::array<std::byte, 12U> ShenzhenSequenceBody(
    std::uint32_t channel,
    std::int64_t sequence) {
    std::array<std::byte, 12U> output{};
    WriteLittleEndian<std::uint32_t>(output, 0U, channel);
    WriteLittleEndian<std::uint64_t>(
        output,
        4U,
        std::bit_cast<std::uint64_t>(sequence));
    return output;
}

NativeSequenceDescriptorV1 Shanghai(
    std::uint32_t channel,
    std::uint64_t sequence) {
    return NativeSequenceDescriptorV1{
        NativeSequenceChannelV1{
            NativeSequenceMarketV1::kShanghai, channel},
        sequence};
}

NativeSequenceDescriptorV1 Shenzhen(
    std::uint32_t channel,
    std::uint64_t sequence) {
    return NativeSequenceDescriptorV1{
        NativeSequenceChannelV1{
            NativeSequenceMarketV1::kShenzhen, channel},
        sequence};
}

std::array<std::byte, 4U> Payload(std::uint32_t value) {
    std::array<std::byte, 4U> output{};
    WriteLittleEndian<std::uint32_t>(output, 0U, value);
    return output;
}

NativeSequenceRecoveryConfigV1 StandardConfig() {
    NativeSequenceRecoveryConfigV1 config{};
    config.maximum_channels = 8U;
    config.maximum_pending_entries = 32U;
    config.maximum_pending_entries_per_channel = 16U;
    config.certified_duplicate_retention_entries = 8U;
    config.maximum_canonical_payload_bytes_per_entry = 64U;
    config.maximum_total_canonical_payload_bytes = 1'024U;
    config.maximum_reorder_span = 32U;
    config.expected_origin_sequence = 1U;
    return config;
}

NativeSequenceRecoveryConfigV1 PartialConfig() {
    NativeSequenceRecoveryConfigV1 config = StandardConfig();
    config.coverage_mode =
        NativeSequenceRecoveryCoverageModeV1::kProcessStartPartial;
    return config;
}

std::unique_ptr<NativeSequenceRecoveryCoordinatorV1> MakeCoordinator(
    NativeSequenceRecoveryConfigV1 config,
    bool* ok) {
    std::unique_ptr<NativeSequenceRecoveryCoordinatorV1> output;
    *ok &= Expect(
        NativeSequenceRecoveryCoordinatorV1::Create(
            config, &output) ==
                NativeSequenceRecoveryCreateErrorV1::kNone &&
            output != nullptr,
        "create recovery coordinator");
    return output;
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
        "observe returns no API error");
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
        "key-based apply returns no API error");
}

bool PollAndCommit(
    NativeSequenceRecoveryCoordinatorV1* coordinator,
    std::uint64_t expected_sequence,
    std::uint64_t expected_cookie,
    std::span<const std::byte> expected_payload = {}) {
    NativeSequenceCertifiedReadyV1 ready{};
    bool ok = Expect(
        coordinator->PollCertified(&ready) ==
            NativeSequenceRecoveryPollErrorV1::kNone,
        "certified record is ready");
    ok &= Expect(
        ready.descriptor.sequence == expected_sequence &&
            ready.applied_cookie == expected_cookie,
        "certified record preserves native sequence and first cookie");
    if (!expected_payload.empty()) {
        ok &= Expect(
            ready.canonical_payload.size() == expected_payload.size() &&
                std::equal(
                    ready.canonical_payload.begin(),
                    ready.canonical_payload.end(),
                    expected_payload.begin()),
            "certified record borrows the first canonical payload");
    }
    ok &= Expect(
        coordinator->CommitCertified(ready.token) ==
            NativeSequenceRecoveryCommitErrorV1::kNone,
        "commit certified record");
    return ok;
}

bool TestExtractor() {
    bool ok = true;
    NativeSequenceDescriptorV1 descriptor{};

    const auto sh_body = ShanghaiSequenceBody(123, 7);
    ok &= Expect(
        ExtractNativeSequenceV1(
            kShanghaiTick, sh_body, &descriptor) ==
                NativeSequenceExtractErrorV1::kNone &&
            descriptor == Shanghai(7U, 123U),
        "extract Shanghai Channel/BizIndex");

    const auto sz_body = ShenzhenSequenceBody(0U, 456);
    ok &= Expect(
        ExtractNativeSequenceV1(
            kShenzhenOrder, sz_body, &descriptor) ==
                NativeSequenceExtractErrorV1::kNone &&
            descriptor == Shenzhen(0U, 456U),
        "extract Shenzhen ChannelNo/ApplSeqNum and permit channel zero");
    ok &= Expect(
        ExtractNativeSequenceV1(
            kShenzhenTransaction, sz_body, &descriptor) ==
                NativeSequenceExtractErrorV1::kNone &&
            descriptor == Shenzhen(0U, 456U),
        "order and transaction extract into one Shenzhen domain");

    ok &= Expect(
        ExtractNativeSequenceV1(
            kShanghaiSnapshot, {}, &descriptor) ==
                NativeSequenceExtractErrorV1::kNotTracked &&
            descriptor == NativeSequenceDescriptorV1{},
        "Shanghai snapshot is explicitly untracked");
    ok &= Expect(
        ExtractNativeSequenceV1(
            kShenzhenSnapshot, {}, &descriptor) ==
                NativeSequenceExtractErrorV1::kNotTracked,
        "Shenzhen snapshot is explicitly untracked");
    ok &= Expect(
        ExtractNativeSequenceV1(
            kForbiddenCombined, sz_body, &descriptor) ==
                NativeSequenceExtractErrorV1::kUnsupportedMessage,
        "forbidden combined tick is not a tracked substitute");
    ok &= Expect(
        ExtractNativeSequenceV1(
            MessageKey{4U, 100U, 24U}, sh_body, &descriptor) ==
                NativeSequenceExtractErrorV1::
                    kUnsupportedServiceVersion,
        "tracked tuple rejects another service version");

    const std::array<std::byte, 11U> short_body{};
    ok &= Expect(
        ExtractNativeSequenceV1(
            kShanghaiTick, short_body, &descriptor) ==
                NativeSequenceExtractErrorV1::kTruncated,
        "extractor bounds-checks fixed sequence prefix");
    const auto zero_sequence = ShanghaiSequenceBody(0, 7);
    ok &= Expect(
        ExtractNativeSequenceV1(
            kShanghaiTick, zero_sequence, &descriptor) ==
                NativeSequenceExtractErrorV1::kInvalidSequence,
        "zero native sequence is invalid");
    const auto negative_sequence = ShenzhenSequenceBody(3U, -1);
    ok &= Expect(
        ExtractNativeSequenceV1(
            kShenzhenOrder, negative_sequence, &descriptor) ==
                NativeSequenceExtractErrorV1::kInvalidSequence,
        "negative native sequence is invalid");
    const auto zero_shanghai_channel = ShanghaiSequenceBody(1, 0);
    ok &= Expect(
        ExtractNativeSequenceV1(
            kShanghaiTick, zero_shanghai_channel, &descriptor) ==
                NativeSequenceExtractErrorV1::kInvalidChannel,
        "Shanghai zero channel is invalid");
    ok &= Expect(
        ExtractNativeSequenceV1(
            kShanghaiTick, sh_body, nullptr) ==
            NativeSequenceExtractErrorV1::kNullOutput,
        "extractor rejects null output");
    return ok;
}

bool TestNormalAndFilteredProgress() {
    bool ok = true;
    auto coordinator = MakeCoordinator(StandardConfig(), &ok);
    if (!coordinator) {
        return false;
    }

    NativeSequenceRecoveryObserveResultV1 observed{};
    ok &= Observe(
        coordinator.get(),
        Shanghai(7U, 1U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Expect(
        observed.disposition ==
                NativeSequenceRecoveryObserveDispositionV1::kAccepted &&
            observed.target_token_available() &&
            observed.channel_state ==
                NativeSequenceRecoveryChannelStateV1::kCatchingUp,
        "first target is accepted without blocking FAST");

    NativeSequenceCertifiedReadyV1 ready{};
    ok &= Expect(
        coordinator->PollCertified(&ready) ==
            NativeSequenceRecoveryPollErrorV1::kNotReady,
        "unapplied target cannot enter CERTIFIED");

    const auto first_payload = Payload(101U);
    NativeSequenceRecoveryApplyResultV1 applied{};
    ok &= Apply(
        coordinator.get(),
        Shanghai(7U, 1U),
        kShanghaiTick,
        first_payload,
        11U,
        &applied);
    ok &= Expect(
        applied.disposition ==
                NativeSequenceRecoveryApplyDispositionV1::kApplied &&
            applied.canonical_applied_cookie == 11U,
        "first canonical payload and cookie are recorded");
    ok &= PollAndCommit(
        coordinator.get(), 1U, 11U, first_payload);

    ok &= Observe(
        coordinator.get(),
        Shanghai(7U, 2U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kFiltered,
        &observed);
    ok &= Expect(
        observed.disposition ==
            NativeSequenceRecoveryObserveDispositionV1::kAccepted,
        "filtered sequence occupies the same native domain");
    ok &= Expect(
        coordinator->PollCertified(&ready) ==
                NativeSequenceRecoveryPollErrorV1::kNone &&
            ready.record_class ==
                NativeSequenceRecoveryRecordClassV1::kFiltered &&
            ready.descriptor.sequence == 2U &&
            ready.applied_cookie == 0U &&
            ready.canonical_payload.empty(),
        "filtered frontier is returned as explicit output-free work");
    ok &= Expect(
        coordinator->CommitCertified(ready.token) ==
            NativeSequenceRecoveryCommitErrorV1::kNone,
        "commit output-free filtered frontier");
    ok &= Expect(
        coordinator->PollCertified(&ready) ==
            NativeSequenceRecoveryPollErrorV1::kNotReady,
        "filtered commit emits no target record");

    NativeSequenceRecoveryChannelSnapshotV1 snapshot{};
    ok &= Expect(
        coordinator->ChannelSnapshot(
            Shanghai(7U, 1U).domain, &snapshot) &&
            snapshot.state ==
                NativeSequenceRecoveryChannelStateV1::kHealthy &&
            snapshot.certified_sequence == 2U &&
            snapshot.filtered_sequences_certified == 1U &&
            snapshot.coverage_from_sequence_one,
        "normal and filtered sequences form one healthy certified prefix");
    return ok;
}

bool TestGapBackfillAndOrderedDrain() {
    bool ok = true;
    NativeSequenceRecoveryConfigV1 config = StandardConfig();
    config.certified_duplicate_retention_entries = 0U;
    auto coordinator = MakeCoordinator(config, &ok);
    if (!coordinator) {
        return false;
    }
    NativeSequenceRecoveryObserveResultV1 observed{};
    NativeSequenceRecoveryApplyResultV1 applied{};

    const auto payload1 = Payload(1U);
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
        payload1,
        1U,
        &applied);
    ok &= PollAndCommit(coordinator.get(), 1U, 1U);

    const auto payload3 = Payload(3U);
    ok &= Observe(
        coordinator.get(),
        Shanghai(9U, 3U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Expect(
        observed.disposition ==
                NativeSequenceRecoveryObserveDispositionV1::kGapOpened &&
            observed.observed_contiguous_sequence == 1U &&
            observed.highest_observed_sequence == 3U &&
            observed.channel_state ==
                NativeSequenceRecoveryChannelStateV1::kRepairing,
        "future sequence opens a gap without closing FAST");
    ok &= Apply(
        coordinator.get(),
        Shanghai(9U, 3U),
        kShanghaiTick,
        payload3,
        3U,
        &applied);
    NativeSequenceCertifiedReadyV1 ready{};
    ok &= Expect(
        coordinator->PollCertified(&ready) ==
            NativeSequenceRecoveryPollErrorV1::kNotReady,
        "CERTIFIED cannot skip the missing native sequence");

    NativeSequenceRecoveryChannelSnapshotV1 snapshot{};
    ok &= Expect(
        coordinator->ChannelSnapshot(
            Shanghai(9U, 1U).domain, &snapshot) &&
            snapshot.missing_sequences == 1U,
        "snapshot counts the actual missing native key");

    const auto payload2 = Payload(2U);
    ok &= Observe(
        coordinator.get(),
        Shanghai(9U, 2U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Expect(
        observed.disposition ==
                NativeSequenceRecoveryObserveDispositionV1::kBackfill &&
            observed.observed_contiguous_sequence == 3U &&
            observed.channel_state ==
                NativeSequenceRecoveryChannelStateV1::kCatchingUp,
        "backfill closes the observed gap but awaits certified drain");
    ok &= Apply(
        coordinator.get(),
        Shanghai(9U, 2U),
        kShanghaiTick,
        payload2,
        2U,
        &applied);
    ok &= PollAndCommit(coordinator.get(), 2U, 2U);
    ok &= PollAndCommit(coordinator.get(), 3U, 3U);
    ok &= Expect(
        coordinator->ChannelSnapshot(
            Shanghai(9U, 1U).domain, &snapshot) &&
            snapshot.state ==
                NativeSequenceRecoveryChannelStateV1::kHealthy &&
            snapshot.certified_sequence == 3U &&
            snapshot.missing_sequences == 0U,
        "CERTIFIED drains repaired ticks only in native order");
    return ok;
}

bool TestExplicitOriginRejectsFirstPacketInference() {
    bool ok = true;
    NativeSequenceRecoveryConfigV1 config = StandardConfig();
    config.expected_origin_sequence = 1U;
    config.trusted_checkpoint_sequence = 0U;
    auto coordinator = MakeCoordinator(config, &ok);
    if (!coordinator) {
        return false;
    }

    NativeSequenceRecoveryObserveResultV1 observed{};
    NativeSequenceRecoveryApplyResultV1 applied{};
    const auto payload3 = Payload(3U);
    ok &= Observe(
        coordinator.get(),
        Shanghai(11U, 3U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Expect(
        observed.disposition ==
                NativeSequenceRecoveryObserveDispositionV1::kGapOpened &&
            observed.origin_sequence == 1U &&
            observed.certified_sequence == 0U &&
            observed.observed_contiguous_sequence == 0U &&
            observed.highest_observed_sequence == 3U &&
            observed.channel_state ==
                NativeSequenceRecoveryChannelStateV1::kRepairing,
        "first packet above explicit origin opens a leading gap");
    ok &= Apply(
        coordinator.get(),
        Shanghai(11U, 3U),
        kShanghaiTick,
        payload3,
        3U,
        &applied);

    NativeSequenceCertifiedReadyV1 ready{};
    ok &= Expect(
        coordinator->PollCertified(&ready) ==
            NativeSequenceRecoveryPollErrorV1::kNotReady,
        "leading gap cannot publish the first observed packet");
    NativeSequenceRecoveryChannelSnapshotV1 snapshot{};
    ok &= Expect(
        coordinator->ChannelSnapshot(
            Shanghai(11U, 1U).domain, &snapshot) &&
            snapshot.origin_sequence == 1U &&
            snapshot.certified_sequence == 0U &&
            snapshot.missing_sequences == 2U &&
            snapshot.coverage_from_sequence_one,
        "configured origin exposes two missing leading positions");

    const auto payload1 = Payload(1U);
    ok &= Observe(
        coordinator.get(),
        Shanghai(11U, 1U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Apply(
        coordinator.get(),
        Shanghai(11U, 1U),
        kShanghaiTick,
        payload1,
        1U,
        &applied);
    ok &= PollAndCommit(coordinator.get(), 1U, 1U);
    ok &= Expect(
        coordinator->ChannelSnapshot(
            Shanghai(11U, 1U).domain, &snapshot) &&
            snapshot.certified_sequence == 1U &&
            snapshot.missing_sequences == 1U &&
            snapshot.state ==
                NativeSequenceRecoveryChannelStateV1::kRepairing,
        "partial leading backfill advances only its dense prefix");

    const auto payload2 = Payload(2U);
    ok &= Observe(
        coordinator.get(),
        Shanghai(11U, 2U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Apply(
        coordinator.get(),
        Shanghai(11U, 2U),
        kShanghaiTick,
        payload2,
        2U,
        &applied);
    ok &= PollAndCommit(coordinator.get(), 2U, 2U);
    ok &= PollAndCommit(coordinator.get(), 3U, 3U);
    ok &= Expect(
        coordinator->ChannelSnapshot(
            Shanghai(11U, 1U).domain, &snapshot) &&
            snapshot.certified_sequence == 3U &&
            snapshot.observed_contiguous_sequence == 3U &&
            snapshot.missing_sequences == 0U &&
            snapshot.state ==
                NativeSequenceRecoveryChannelStateV1::kHealthy,
        "complete leading backfill drains in native order");

    NativeSequenceRecoveryConfigV1 resumed = StandardConfig();
    resumed.expected_origin_sequence = 1U;
    resumed.trusted_checkpoint_sequence = 9U;
    auto resumed_coordinator = MakeCoordinator(resumed, &ok);
    if (!resumed_coordinator) {
        return false;
    }
    const auto payload10 = Payload(10U);
    ok &= Observe(
        resumed_coordinator.get(),
        Shanghai(12U, 10U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Apply(
        resumed_coordinator.get(),
        Shanghai(12U, 10U),
        kShanghaiTick,
        payload10,
        10U,
        &applied);
    ok &= PollAndCommit(resumed_coordinator.get(), 10U, 10U);
    ok &= Expect(
        resumed_coordinator->ChannelSnapshot(
            Shanghai(12U, 1U).domain, &snapshot) &&
            snapshot.origin_sequence == 1U &&
            snapshot.certified_sequence == 10U &&
            snapshot.coverage_from_sequence_one,
        "trusted checkpoint begins at checkpoint plus one");
    return ok;
}

bool TestAppliedBeforeObserve() {
    bool ok = true;
    NativeSequenceRecoveryConfigV1 config = StandardConfig();
    config.expected_origin_sequence = 10U;
    auto coordinator = MakeCoordinator(config, &ok);
    if (!coordinator) {
        return false;
    }

    const auto payload = Payload(77U);
    NativeSequenceRecoveryApplyResultV1 applied{};
    ok &= Apply(
        coordinator.get(),
        Shenzhen(0U, 10U),
        kShenzhenOrder,
        payload,
        700U,
        &applied);
    ok &= Expect(
        applied.disposition ==
                NativeSequenceRecoveryApplyDispositionV1::kApplied &&
            applied.observed_arrivals == 0U &&
            applied.applied_arrivals == 1U &&
            coordinator->Snapshot().pending_entries == 1U,
        "application-first notification is retained in bounded staging");
    NativeSequenceCertifiedReadyV1 ready{};
    ok &= Expect(
        coordinator->PollCertified(&ready) ==
            NativeSequenceRecoveryPollErrorV1::kNotReady,
        "application-first staging cannot bypass Observe identity");

    NativeSequenceRecoveryObserveResultV1 observed{};
    ok &= Observe(
        coordinator.get(),
        Shenzhen(0U, 10U),
        kShenzhenOrder,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Expect(
        observed.disposition ==
                NativeSequenceRecoveryObserveDispositionV1::kAccepted &&
            observed.origin_sequence == 10U,
        "later Observe activates the staged application");
    ok &= PollAndCommit(coordinator.get(), 10U, 700U);
    return ok;
}

bool TestPartialSealRejectsAppliedOnlyLower() {
    bool ok = true;
    auto coordinator = MakeCoordinator(PartialConfig(), &ok);
    if (!coordinator) {
        return false;
    }

    NativeSequenceRecoveryApplyResultV1 applied{};
    const auto payload99 = Payload(99U);
    ok &= Apply(
        coordinator.get(),
        Shanghai(18U, 99U),
        kShanghaiTick,
        payload99,
        99U,
        &applied);
    NativeSequenceRecoveryObserveResultV1 observed{};
    ok &= Observe(
        coordinator.get(),
        Shanghai(18U, 100U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kFiltered,
        &observed);

    NativeSequenceRecoverySealResultV1 sealed{};
    ok &= Expect(
        coordinator->SealBoundedOrigin(
            Shanghai(18U, 99U).domain, 100U, &sealed) ==
                NativeSequenceRecoverySealErrorV1::kNone &&
            sealed.disposition ==
                NativeSequenceRecoverySealDispositionV1::
                    kOriginExcludesStaged,
        "seal cannot exclude an applied-only lower position");
    NativeSequenceRecoveryChannelSnapshotV1 snapshot{};
    ok &= Expect(
        coordinator->ChannelSnapshot(
            Shanghai(18U, 99U).domain, &snapshot) &&
            !snapshot.origin_sealed && snapshot.origin_sequence == 100U,
        "rejected seal leaves the provisional origin unchanged");

    ok &= Expect(
        coordinator->SealBoundedOrigin(
            Shanghai(18U, 99U).domain, 99U, &sealed) ==
                NativeSequenceRecoverySealErrorV1::kNone &&
            sealed.disposition ==
                NativeSequenceRecoverySealDispositionV1::kSealed &&
            sealed.observed_contiguous_sequence == 98U &&
            sealed.highest_observed_sequence == 100U,
        "retry at the applied-only minimum seals without inventing observation");
    NativeSequenceCertifiedReadyV1 ready{};
    ok &= Expect(
        coordinator->PollCertified(&ready) ==
            NativeSequenceRecoveryPollErrorV1::kNotReady,
        "applied-only origin remains blocked until its Observe arrives");
    return ok;
}

bool TestPartialSealPreservesSameOriginGap() {
    bool ok = true;
    auto coordinator = MakeCoordinator(PartialConfig(), &ok);
    if (!coordinator) {
        return false;
    }

    NativeSequenceRecoveryObserveResultV1 observed{};
    ok &= Observe(
        coordinator.get(),
        Shanghai(19U, 100U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kFiltered,
        &observed);
    ok &= Observe(
        coordinator.get(),
        Shanghai(19U, 102U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kFiltered,
        &observed);

    NativeSequenceRecoverySealResultV1 sealed{};
    ok &= Expect(
        coordinator->SealBoundedOrigin(
            Shanghai(19U, 100U).domain, 100U, &sealed) ==
                NativeSequenceRecoverySealErrorV1::kNone &&
            sealed.disposition ==
                NativeSequenceRecoverySealDispositionV1::kSealed &&
            sealed.observed_contiguous_sequence == 100U &&
            sealed.highest_observed_sequence == 102U,
        "same-origin seal preserves the observed gap");
    NativeSequenceRecoveryChannelSnapshotV1 snapshot{};
    ok &= Expect(
        coordinator->ChannelSnapshot(
            Shanghai(19U, 100U).domain, &snapshot) &&
            snapshot.missing_sequences == 1U,
        "same-origin seal reports the missing native position");
    ok &= PollAndCommit(coordinator.get(), 100U, 0U);
    NativeSequenceCertifiedReadyV1 ready{};
    ok &= Expect(
        coordinator->PollCertified(&ready) ==
            NativeSequenceRecoveryPollErrorV1::kNotReady,
        "certification cannot skip the preserved pre-seal gap");
    return ok;
}

bool TestPartialLowerOriginDoesNotAdvancePrefix() {
    bool ok = true;
    auto coordinator = MakeCoordinator(PartialConfig(), &ok);
    if (!coordinator) {
        return false;
    }

    NativeSequenceRecoveryObserveResultV1 observed{};
    ok &= Observe(
        coordinator.get(),
        Shanghai(20U, 100U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kFiltered,
        &observed);
    ok &= Observe(
        coordinator.get(),
        Shanghai(20U, 101U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kFiltered,
        &observed);

    NativeSequenceRecoverySealResultV1 sealed{};
    ok &= Expect(
        coordinator->SealBoundedOrigin(
            Shanghai(20U, 100U).domain, 99U, &sealed) ==
                NativeSequenceRecoverySealErrorV1::kNone &&
            sealed.disposition ==
                NativeSequenceRecoverySealDispositionV1::kSealed &&
            sealed.observed_contiguous_sequence == 98U &&
            sealed.highest_observed_sequence == 101U,
        "lower seal origin opens a leading gap instead of preserving the old prefix");
    NativeSequenceRecoveryChannelSnapshotV1 snapshot{};
    NativeSequenceCertifiedReadyV1 ready{};
    ok &= Expect(
        coordinator->ChannelSnapshot(
            Shanghai(20U, 99U).domain, &snapshot) &&
            snapshot.missing_sequences == 1U &&
            coordinator->PollCertified(&ready) ==
                NativeSequenceRecoveryPollErrorV1::kNotReady,
        "unobserved lower origin prevents premature certification");
    return ok;
}

bool TestPartialPureAppliedOnlySeal() {
    bool ok = true;
    auto coordinator = MakeCoordinator(PartialConfig(), &ok);
    if (!coordinator) {
        return false;
    }

    const auto payload100 = Payload(100U);
    NativeSequenceRecoveryApplyResultV1 applied{};
    ok &= Apply(
        coordinator.get(),
        Shanghai(21U, 100U),
        kShanghaiTick,
        payload100,
        100U,
        &applied);
    NativeSequenceRecoverySealResultV1 sealed{};
    ok &= Expect(
        coordinator->SealBoundedOrigin(
            Shanghai(21U, 100U).domain, 100U, &sealed) ==
                NativeSequenceRecoverySealErrorV1::kNone &&
            sealed.disposition ==
                NativeSequenceRecoverySealDispositionV1::kSealed &&
            sealed.observed_contiguous_sequence == 99U &&
            sealed.highest_observed_sequence == 99U,
        "pure applied-only channel seals at an empty observed baseline");
    NativeSequenceCertifiedReadyV1 ready{};
    ok &= Expect(
        coordinator->PollCertified(&ready) ==
            NativeSequenceRecoveryPollErrorV1::kNotReady,
        "pure applied-only position is not certified before Observe");

    NativeSequenceRecoveryObserveResultV1 observed{};
    ok &= Observe(
        coordinator.get(),
        Shanghai(21U, 100U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Expect(
        observed.observed_contiguous_sequence == 100U,
        "later Observe joins and advances the applied-only origin");
    ok &= PollAndCommit(
        coordinator.get(), 100U, 100U, payload100);
    return ok;
}

bool TestDuplicateBarrierAndConflictIsolation() {
    bool ok = true;
    auto coordinator = MakeCoordinator(StandardConfig(), &ok);
    if (!coordinator) {
        return false;
    }
    NativeSequenceRecoveryObserveResultV1 observed{};
    NativeSequenceRecoveryApplyResultV1 applied{};
    const auto payload = Payload(55U);
    const auto conflict_payload = Payload(56U);

    ok &= Observe(
        coordinator.get(),
        Shanghai(2U, 1U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    const NativeSequenceRecoveryTokenV1 first_token =
        observed.token;
    ok &= Observe(
        coordinator.get(),
        Shanghai(2U, 1U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Expect(
        observed.disposition ==
            NativeSequenceRecoveryObserveDispositionV1::
                kDuplicatePending,
        "duplicate arrival is pending, not prematurely called exact");

    ok &= Apply(
        coordinator.get(),
        Shanghai(2U, 1U),
        kShanghaiTick,
        payload,
        100U,
        &applied);
    NativeSequenceCertifiedReadyV1 ready{};
    ok &= Expect(
        coordinator->PollCertified(&ready) ==
            NativeSequenceRecoveryPollErrorV1::kNotReady,
        "known duplicate blocks certification until both payloads apply");
    ok &= Apply(
        coordinator.get(),
        Shanghai(2U, 1U),
        kShanghaiTick,
        payload,
        101U,
        &applied);
    ok &= Expect(
        applied.disposition ==
                NativeSequenceRecoveryApplyDispositionV1::
                    kExactDuplicate &&
            applied.canonical_applied_cookie == 100U,
        "byte-exact duplicate preserves the first canonical cookie");
    ok &= PollAndCommit(coordinator.get(), 1U, 100U);

    ok &= Observe(
        coordinator.get(),
        Shanghai(2U, 1U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Expect(
        observed.disposition ==
            NativeSequenceRecoveryObserveDispositionV1::
                kDuplicateCertified,
        "certified duplicate remains checkable inside retention");
    const auto next_payload = Payload(57U);
    ok &= Observe(
        coordinator.get(),
        Shanghai(2U, 2U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Apply(
        coordinator.get(),
        Shanghai(2U, 2U),
        kShanghaiTick,
        next_payload,
        200U,
        &applied);
    ok &= Expect(
        coordinator->PollCertified(&ready) ==
            NativeSequenceRecoveryPollErrorV1::kNotReady,
        "unverified retained duplicate blocks later channel certification");
    ok &= Apply(
        coordinator.get(),
        Shanghai(2U, 1U),
        kShanghaiTick,
        conflict_payload,
        102U,
        &applied);
    ok &= Expect(
        applied.disposition ==
                NativeSequenceRecoveryApplyDispositionV1::
                    kPayloadConflict &&
            applied.channel_state ==
                NativeSequenceRecoveryChannelStateV1::
                    kFrozenConflict,
        "different canonical bytes freeze only that CERTIFIED channel");
    ok &= Expect(
        coordinator->CommitCertified(first_token) ==
            NativeSequenceRecoveryCommitErrorV1::kChannelFrozen,
        "stale work cannot mutate a frozen channel");

    const auto other_payload = Payload(9U);
    ok &= Observe(
        coordinator.get(),
        Shenzhen(4U, 1U),
        kShenzhenOrder,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Apply(
        coordinator.get(),
        Shenzhen(4U, 1U),
        kShenzhenOrder,
        other_payload,
        900U,
        &applied);
    ok &= PollAndCommit(coordinator.get(), 1U, 900U);

    NativeSequenceRecoveryChannelSnapshotV1 frozen{};
    NativeSequenceRecoveryChannelSnapshotV1 healthy{};
    ok &= Expect(
        coordinator->ChannelSnapshot(
            Shanghai(2U, 1U).domain, &frozen) &&
            frozen.freeze_reason ==
                NativeSequenceRecoveryFreezeReasonV1::
                    kPayloadConflict &&
            frozen.conflicts == 1U &&
            frozen.unverified_duplicate_applications == 1U,
        "conflicting channel exposes a sticky integrity reason");
    ok &= Expect(
        coordinator->ChannelSnapshot(
            Shenzhen(4U, 1U).domain, &healthy) &&
            healthy.state ==
                NativeSequenceRecoveryChannelStateV1::kHealthy &&
            healthy.certified_sequence == 1U,
        "another exchange/channel continues through CERTIFIED");
    return ok;
}

bool TestShenzhenSharedDomainAndTupleConflict() {
    bool ok = true;
    auto coordinator = MakeCoordinator(StandardConfig(), &ok);
    if (!coordinator) {
        return false;
    }
    NativeSequenceRecoveryObserveResultV1 observed{};
    NativeSequenceRecoveryApplyResultV1 applied{};

    ok &= Observe(
        coordinator.get(),
        Shenzhen(12U, 1U),
        kShenzhenOrder,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    const auto payload1 = Payload(1U);
    ok &= Apply(
        coordinator.get(),
        Shenzhen(12U, 1U),
        kShenzhenOrder,
        payload1,
        1U,
        &applied);
    ok &= PollAndCommit(coordinator.get(), 1U, 1U);

    ok &= Observe(
        coordinator.get(),
        Shenzhen(12U, 3U),
        kShenzhenTransaction,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Expect(
        observed.disposition ==
            NativeSequenceRecoveryObserveDispositionV1::kGapOpened,
        "SZ order and transaction share one channel sequence domain");
    const auto payload3 = Payload(3U);
    ok &= Apply(
        coordinator.get(),
        Shenzhen(12U, 3U),
        kShenzhenTransaction,
        payload3,
        3U,
        &applied);

    ok &= Observe(
        coordinator.get(),
        Shenzhen(12U, 2U),
        kShenzhenOrder,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    const auto payload2 = Payload(2U);
    ok &= Apply(
        coordinator.get(),
        Shenzhen(12U, 2U),
        kShenzhenOrder,
        payload2,
        2U,
        &applied);
    ok &= PollAndCommit(coordinator.get(), 2U, 2U);
    ok &= PollAndCommit(coordinator.get(), 3U, 3U);

    ok &= Observe(
        coordinator.get(),
        Shenzhen(12U, 3U),
        kShenzhenOrder,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Expect(
        observed.disposition ==
                NativeSequenceRecoveryObserveDispositionV1::
                    kPayloadConflict &&
            observed.channel_state ==
                NativeSequenceRecoveryChannelStateV1::
                    kFrozenConflict,
        "same SZ native key cannot change message family");
    return ok;
}

bool TestRetentionOutsideAndAbaToken() {
    bool ok = true;
    NativeSequenceRecoveryConfigV1 config = StandardConfig();
    config.certified_duplicate_retention_entries = 1U;
    auto coordinator = MakeCoordinator(config, &ok);
    if (!coordinator) {
        return false;
    }
    NativeSequenceRecoveryObserveResultV1 observed{};
    NativeSequenceRecoveryApplyResultV1 applied{};
    NativeSequenceRecoveryTokenV1 old_token{};

    for (std::uint64_t sequence = 1U; sequence <= 2U; ++sequence) {
        ok &= Observe(
            coordinator.get(),
            Shanghai(3U, sequence),
            kShanghaiTick,
            NativeSequenceRecoveryRecordClassV1::kTarget,
            &observed);
        if (sequence == 1U) {
            old_token = observed.token;
        }
        const auto payload =
            Payload(static_cast<std::uint32_t>(sequence));
        ok &= Apply(
            coordinator.get(),
            Shanghai(3U, sequence),
            kShanghaiTick,
            payload,
            sequence,
            &applied);
        ok &= PollAndCommit(
            coordinator.get(), sequence, sequence);
    }
    const auto old_payload = Payload(1U);
    NativeSequenceRecoveryApplyResultV1 token_result{};
    ok &= Expect(
        coordinator->MarkTargetApplied(
            old_token,
            kShanghaiTick,
            old_payload,
            100U,
            &token_result) ==
            NativeSequenceRecoveryApplyErrorV1::kInvalidToken,
        "generation rejects an ABA-stale entry token");

    ok &= Observe(
        coordinator.get(),
        Shanghai(3U, 1U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Expect(
        observed.disposition ==
                NativeSequenceRecoveryObserveDispositionV1::
                    kResourceFrozen &&
            observed.channel_state ==
                NativeSequenceRecoveryChannelStateV1::
                    kFrozenResource,
        "old duplicate freezes CERTIFIED when exact comparison is no "
        "longer possible");
    ok &= Apply(
        coordinator.get(),
        Shanghai(3U, 1U),
        kShanghaiTick,
        old_payload,
        99U,
        &applied);
    ok &= Expect(
        applied.disposition ==
            NativeSequenceRecoveryApplyDispositionV1::
                kChannelFrozen,
        "late applied counterpart cannot reopen an unverifiable channel");

    return ok;
}

bool TestNonPowerOfTwoRetentionWrapAndReuse() {
    bool ok = true;
    NativeSequenceRecoveryConfigV1 config = StandardConfig();
    config.maximum_pending_entries = 1U;
    config.maximum_pending_entries_per_channel = 1U;
    config.certified_duplicate_retention_entries = 3U;
    auto coordinator = MakeCoordinator(config, &ok);
    if (!coordinator) {
        return false;
    }

    NativeSequenceRecoveryObserveResultV1 observed{};
    NativeSequenceRecoveryApplyResultV1 applied{};
    NativeSequenceRecoveryTokenV1 first_token{};
    for (std::uint64_t sequence = 1U; sequence <= 12U; ++sequence) {
        ok &= Observe(
            coordinator.get(),
            Shanghai(15U, sequence),
            kShanghaiTick,
            NativeSequenceRecoveryRecordClassV1::kTarget,
            &observed);
        if (sequence == 1U) {
            first_token = observed.token;
        }
        const auto payload =
            Payload(static_cast<std::uint32_t>(sequence));
        ok &= Apply(
            coordinator.get(),
            Shanghai(15U, sequence),
            kShanghaiTick,
            payload,
            sequence,
            &applied);
        ok &= PollAndCommit(
            coordinator.get(), sequence, sequence, payload);
    }

    const auto snapshot = coordinator->Snapshot();
    ok &= Expect(
        snapshot.pending_entries == 0U &&
            snapshot.retained_certified_entries == 3U &&
            snapshot.canonical_payload_bytes == 12U &&
            snapshot.frozen_channel_count == 0U,
        "non-power-of-two retention wraps without corrupting accounting");
    const auto first_payload = Payload(1U);
    ok &= Expect(
        coordinator->MarkTargetApplied(
            first_token,
            kShanghaiTick,
            first_payload,
            101U,
            &applied) ==
                NativeSequenceRecoveryApplyErrorV1::kInvalidToken &&
            coordinator->CommitCertified(first_token) ==
                NativeSequenceRecoveryCommitErrorV1::kInvalidToken,
        "reused free-list slot rejects stale apply and commit tokens");

    const auto retained_payload = Payload(10U);
    ok &= Observe(
        coordinator.get(),
        Shanghai(15U, 10U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Apply(
        coordinator.get(),
        Shanghai(15U, 10U),
        kShanghaiTick,
        retained_payload,
        110U,
        &applied);
    ok &= Expect(
        observed.disposition ==
                NativeSequenceRecoveryObserveDispositionV1::
                    kDuplicateCertified &&
            applied.disposition ==
                NativeSequenceRecoveryApplyDispositionV1::
                    kExactDuplicate,
        "wrapped retention still verifies an exact retained duplicate");
    return ok;
}

bool TestZeroRetentionContiguousBacklog() {
    bool ok = true;
    NativeSequenceRecoveryConfigV1 config = StandardConfig();
    config.maximum_pending_entries = 16U;
    config.maximum_pending_entries_per_channel = 16U;
    config.certified_duplicate_retention_entries = 0U;
    auto coordinator = MakeCoordinator(config, &ok);
    if (!coordinator) {
        return false;
    }

    NativeSequenceRecoveryObserveResultV1 observed{};
    NativeSequenceRecoveryApplyResultV1 applied{};
    for (std::uint64_t sequence = 1U; sequence <= 12U; ++sequence) {
        ok &= Observe(
            coordinator.get(),
            Shanghai(16U, sequence),
            kShanghaiTick,
            NativeSequenceRecoveryRecordClassV1::kTarget,
            &observed);
    }
    for (std::uint64_t sequence = 1U; sequence <= 12U; ++sequence) {
        const auto payload =
            Payload(static_cast<std::uint32_t>(sequence));
        ok &= Apply(
            coordinator.get(),
            Shanghai(16U, sequence),
            kShanghaiTick,
            payload,
            sequence,
            &applied);
    }
    for (std::uint64_t sequence = 1U; sequence <= 12U; ++sequence) {
        const auto payload =
            Payload(static_cast<std::uint32_t>(sequence));
        ok &= PollAndCommit(
            coordinator.get(), sequence, sequence, payload);
    }

    const auto snapshot = coordinator->Snapshot();
    ok &= Expect(
        snapshot.pending_entries == 0U &&
            snapshot.retained_certified_entries == 0U &&
            snapshot.canonical_payload_bytes == 0U &&
            snapshot.frozen_channel_count == 0U,
        "successor hints survive immediate release of each committed entry");

    config.maximum_pending_entries = 1U;
    config.maximum_pending_entries_per_channel = 1U;
    auto reused = MakeCoordinator(config, &ok);
    if (!reused) {
        return false;
    }
    for (std::uint64_t sequence = 1U; sequence <= 12U; ++sequence) {
        ok &= Observe(
            reused.get(),
            Shanghai(17U, sequence),
            kShanghaiTick,
            NativeSequenceRecoveryRecordClassV1::kTarget,
            &observed);
        const auto payload =
            Payload(static_cast<std::uint32_t>(sequence));
        ok &= Apply(
            reused.get(),
            Shanghai(17U, sequence),
            kShanghaiTick,
            payload,
            sequence,
            &applied);
        ok &= PollAndCommit(
            reused.get(), sequence, sequence, payload);
    }
    ok &= Expect(
        reused->Snapshot().pending_entries == 0U &&
            reused->Snapshot().frozen_channel_count == 0U,
        "successor hints recover when every commit immediately reuses a slot");
    return ok;
}

bool TestPreallocatedCanonicalPayloadArena() {
    bool ok = true;
    NativeSequenceRecoveryConfigV1 config = StandardConfig();
    config.maximum_pending_entries = 1U;
    config.maximum_pending_entries_per_channel = 1U;
    config.certified_duplicate_retention_entries = 1U;
    config.maximum_canonical_payload_bytes_per_entry = 4U;
    config.maximum_total_canonical_payload_bytes = 8U;
    config.preallocate_canonical_payload_arena = true;
    auto coordinator = MakeCoordinator(config, &ok);
    if (!coordinator) {
        return false;
    }

    NativeSequenceRecoveryObserveResultV1 observed{};
    NativeSequenceRecoveryApplyResultV1 applied{};
    const auto payload1 = Payload(11U);
    const auto payload2 = Payload(22U);
    const std::array<std::byte, 2U> payload3{
        std::byte{0x0bU}, std::byte{0x00U}};

    ok &= Observe(
        coordinator.get(),
        Shanghai(14U, 1U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    const NativeSequenceRecoveryTokenV1 first_token = observed.token;
    ok &= Apply(
        coordinator.get(),
        Shanghai(14U, 1U),
        kShanghaiTick,
        payload1,
        11U,
        &applied);
    ok &= PollAndCommit(
        coordinator.get(), 1U, 11U, payload1);
    ok &= Expect(
        coordinator->Snapshot().retained_certified_entries == 1U &&
            coordinator->Snapshot().canonical_payload_bytes == 4U,
        "preallocated arena accounts only live canonical payload bytes");

    ok &= Observe(
        coordinator.get(),
        Shanghai(14U, 1U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Expect(
        observed.disposition ==
            NativeSequenceRecoveryObserveDispositionV1::
                kDuplicateCertified,
        "retained arena payload remains available for duplicate proof");
    ok &= Apply(
        coordinator.get(),
        Shanghai(14U, 1U),
        kShanghaiTick,
        payload1,
        12U,
        &applied);
    ok &= Expect(
        applied.disposition ==
                NativeSequenceRecoveryApplyDispositionV1::
                    kExactDuplicate &&
            applied.canonical_applied_cookie == 11U,
        "arena exact duplicate preserves the first canonical cookie");

    ok &= Observe(
        coordinator.get(),
        Shanghai(14U, 2U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Apply(
        coordinator.get(),
        Shanghai(14U, 2U),
        kShanghaiTick,
        payload2,
        22U,
        &applied);
    ok &= PollAndCommit(coordinator.get(), 2U, 22U);
    const auto after_eviction = coordinator->Snapshot();
    ok &= Expect(
        after_eviction.retained_certified_entries == 1U &&
            after_eviction.canonical_payload_bytes == 4U,
        "arena retention eviction releases logical payload accounting");

    ok &= Observe(
        coordinator.get(),
        Shanghai(14U, 3U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    NativeSequenceRecoveryApplyResultV1 stale{};
    ok &= Expect(
        coordinator->MarkTargetApplied(
            first_token,
            kShanghaiTick,
            payload1,
            13U,
            &stale) ==
            NativeSequenceRecoveryApplyErrorV1::kInvalidToken,
        "arena slot reuse preserves ABA generation rejection");
    ok &= Apply(
        coordinator.get(),
        Shanghai(14U, 3U),
        kShanghaiTick,
        payload3,
        33U,
        &applied);
    ok &= PollAndCommit(coordinator.get(), 3U, 33U);

    ok &= Observe(
        coordinator.get(),
        Shanghai(14U, 3U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Apply(
        coordinator.get(),
        Shanghai(14U, 3U),
        kShanghaiTick,
        payload3,
        34U,
        &applied);
    ok &= Expect(
        applied.disposition ==
                NativeSequenceRecoveryApplyDispositionV1::
                    kExactDuplicate &&
            applied.canonical_applied_cookie == 33U,
        "reused arena slot compares the replacement payload exactly");
    ok &= Observe(
        coordinator.get(),
        Shanghai(14U, 3U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Apply(
        coordinator.get(),
        Shanghai(14U, 3U),
        kShanghaiTick,
        payload1,
        35U,
        &applied);
    const auto frozen = coordinator->Snapshot();
    ok &= Expect(
        applied.disposition ==
                NativeSequenceRecoveryApplyDispositionV1::
                    kPayloadConflict &&
            applied.channel_state ==
                NativeSequenceRecoveryChannelStateV1::kFrozenConflict &&
            frozen.retained_certified_entries == 0U &&
            frozen.canonical_payload_bytes == 0U,
        "arena conflict freezes the channel and releases live accounting");
    return ok;
}

bool TestResourceBoundsAndChannelIsolation() {
    bool ok = true;
    NativeSequenceRecoveryConfigV1 config = StandardConfig();
    config.maximum_pending_entries = 4U;
    config.maximum_pending_entries_per_channel = 2U;
    config.certified_duplicate_retention_entries = 0U;
    config.maximum_reorder_span = 8U;
    auto coordinator = MakeCoordinator(config, &ok);
    if (!coordinator) {
        return false;
    }
    NativeSequenceRecoveryObserveResultV1 observed{};
    NativeSequenceRecoveryApplyResultV1 applied{};

    ok &= Observe(
        coordinator.get(),
        Shanghai(5U, 1U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Observe(
        coordinator.get(),
        Shanghai(5U, 3U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Observe(
        coordinator.get(),
        Shanghai(5U, 4U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Expect(
        observed.disposition ==
                NativeSequenceRecoveryObserveDispositionV1::
                    kResourceFrozen &&
            observed.channel_state ==
                NativeSequenceRecoveryChannelStateV1::
                    kFrozenResource,
        "per-channel bound freezes only the overflowing CERTIFIED channel");

    const auto payload = Payload(1U);
    ok &= Observe(
        coordinator.get(),
        Shenzhen(8U, 1U),
        kShenzhenOrder,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Apply(
        coordinator.get(),
        Shenzhen(8U, 1U),
        kShenzhenOrder,
        payload,
        8U,
        &applied);
    ok &= PollAndCommit(coordinator.get(), 1U, 8U);
    ok &= Expect(
        coordinator->Snapshot().frozen_channel_count == 1U,
        "resource failure does not spread to healthy channels");

    NativeSequenceRecoveryConfigV1 payload_config =
        StandardConfig();
    payload_config.maximum_canonical_payload_bytes_per_entry =
        2U;
    payload_config.maximum_total_canonical_payload_bytes = 2U;
    auto payload_limited =
        MakeCoordinator(payload_config, &ok);
    if (!payload_limited) {
        return false;
    }
    ok &= Observe(
        payload_limited.get(),
        Shanghai(6U, 1U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Apply(
        payload_limited.get(),
        Shanghai(6U, 1U),
        kShanghaiTick,
        payload,
        1U,
        &applied);
    ok &= Expect(
        applied.disposition ==
                NativeSequenceRecoveryApplyDispositionV1::
                    kResourceFrozen &&
            applied.freeze_reason ==
                NativeSequenceRecoveryFreezeReasonV1::
                    kPayloadCapacity,
        "canonical payload hard limit freezes CERTIFIED, not FAST");
    return ok;
}

bool TestChannelAndReorderBounds() {
    bool ok = true;
    NativeSequenceRecoveryConfigV1 config = StandardConfig();
    config.maximum_channels = 1U;
    config.maximum_reorder_span = 2U;
    auto coordinator = MakeCoordinator(config, &ok);
    if (!coordinator) {
        return false;
    }
    NativeSequenceRecoveryObserveResultV1 observed{};

    ok &= Observe(
        coordinator.get(),
        Shanghai(10U, 1U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Observe(
        coordinator.get(),
        Shenzhen(10U, 1U),
        kShenzhenOrder,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Expect(
        observed.disposition ==
            NativeSequenceRecoveryObserveDispositionV1::
                kChannelCapacity,
        "channel-table exhaustion leaves existing channels intact");

    ok &= Observe(
        coordinator.get(),
        Shanghai(10U, 3U),
        kShanghaiTick,
        NativeSequenceRecoveryRecordClassV1::kTarget,
        &observed);
    ok &= Expect(
        observed.disposition ==
                NativeSequenceRecoveryObserveDispositionV1::
                    kResourceFrozen &&
            observed.channel_state ==
                NativeSequenceRecoveryChannelStateV1::
                    kFrozenResource,
        "reorder span is measured from the certified frontier");
    NativeSequenceRecoveryChannelSnapshotV1 snapshot{};
    ok &= Expect(
        coordinator->ChannelSnapshot(
            Shanghai(10U, 1U).domain, &snapshot) &&
            snapshot.freeze_reason ==
                NativeSequenceRecoveryFreezeReasonV1::
                    kReorderWindowExceeded,
        "reorder-window freeze reason is observable");
    return ok;
}

bool TestConfigurationValidation() {
    bool ok = true;
    std::unique_ptr<NativeSequenceRecoveryCoordinatorV1> coordinator;
    NativeSequenceRecoveryConfigV1 config = StandardConfig();
    config.maximum_pending_entries_per_channel =
        config.maximum_pending_entries + 1U;
    ok &= Expect(
        NativeSequenceRecoveryCoordinatorV1::Create(
            config, &coordinator) ==
            NativeSequenceRecoveryCreateErrorV1::
                kInvalidConfiguration,
        "per-channel capacity cannot exceed global capacity");
    config = StandardConfig();
    config.maximum_total_canonical_payload_bytes =
        config.maximum_canonical_payload_bytes_per_entry - 1U;
    ok &= Expect(
        NativeSequenceRecoveryCoordinatorV1::Create(
            config, &coordinator) ==
            NativeSequenceRecoveryCreateErrorV1::
                kInvalidConfiguration,
        "total canonical payload bound covers one entry");
    config = StandardConfig();
    config.maximum_pending_entries = 2U;
    config.maximum_pending_entries_per_channel = 2U;
    config.certified_duplicate_retention_entries = 1U;
    config.maximum_canonical_payload_bytes_per_entry = 4U;
    config.maximum_total_canonical_payload_bytes = 8U;
    config.preallocate_canonical_payload_arena = true;
    ok &= Expect(
        NativeSequenceRecoveryCoordinatorV1::Create(
            config, &coordinator) ==
            NativeSequenceRecoveryCreateErrorV1::
                kInvalidConfiguration,
        "preallocated arena requires one maximum-size slot per entry");
    config.maximum_total_canonical_payload_bytes = 12U;
    coordinator.reset();
    ok &= Expect(
        NativeSequenceRecoveryCoordinatorV1::Create(
            config, &coordinator) ==
                NativeSequenceRecoveryCreateErrorV1::kNone &&
            coordinator != nullptr,
        "preallocated arena accepts an exact fixed-slot payload bound");
    config = StandardConfig();
    config.maximum_pending_entries = 2U;
    config.maximum_pending_entries_per_channel = 2U;
    config.certified_duplicate_retention_entries = 0U;
    config.maximum_canonical_payload_bytes_per_entry =
        std::numeric_limits<std::size_t>::max();
    config.maximum_total_canonical_payload_bytes =
        std::numeric_limits<std::size_t>::max();
    config.preallocate_canonical_payload_arena = true;
    coordinator.reset();
    ok &= Expect(
        NativeSequenceRecoveryCoordinatorV1::Create(
            config, &coordinator) ==
            NativeSequenceRecoveryCreateErrorV1::
                kInvalidConfiguration,
        "preallocated arena rejects fixed-slot byte-count overflow");
    config = StandardConfig();
    config.expected_origin_sequence = 0U;
    ok &= Expect(
        NativeSequenceRecoveryCoordinatorV1::Create(
            config, &coordinator) ==
            NativeSequenceRecoveryCreateErrorV1::
                kInvalidConfiguration,
        "explicit expected origin cannot be zero");
    config = StandardConfig();
    config.expected_origin_sequence = 10U;
    config.trusted_checkpoint_sequence = 8U;
    ok &= Expect(
        NativeSequenceRecoveryCoordinatorV1::Create(
            config, &coordinator) ==
            NativeSequenceRecoveryCreateErrorV1::
                kInvalidConfiguration,
        "trusted checkpoint cannot precede the coverage baseline");
    config = StandardConfig();
    config.maximum_reorder_span = 2U;
    coordinator.reset();
    ok &= Expect(
        NativeSequenceRecoveryCoordinatorV1::Create(
            config, &coordinator) ==
                NativeSequenceRecoveryCreateErrorV1::kNone &&
            coordinator != nullptr,
        "create bounded explicit-origin coordinator");
    if (coordinator != nullptr) {
        NativeSequenceRecoveryObserveResultV1 observed{};
        ok &= Observe(
            coordinator.get(),
            Shanghai(13U, 3U),
            kShanghaiTick,
            NativeSequenceRecoveryRecordClassV1::kTarget,
            &observed);
        ok &= Expect(
            observed.disposition ==
                    NativeSequenceRecoveryObserveDispositionV1::
                        kResourceFrozen &&
                observed.channel_state ==
                    NativeSequenceRecoveryChannelStateV1::
                        kFrozenResource,
            "first packet beyond reorder bound freezes only CERTIFIED");
    }
    ok &= Expect(
        NativeSequenceRecoveryCoordinatorV1::Create(
            StandardConfig(), nullptr) ==
            NativeSequenceRecoveryCreateErrorV1::kNullOutput,
        "create rejects null output");
    return ok;
}

}  // namespace

int main() {
    bool ok = true;
    ok &= TestExtractor();
    ok &= TestNormalAndFilteredProgress();
    ok &= TestGapBackfillAndOrderedDrain();
    ok &= TestExplicitOriginRejectsFirstPacketInference();
    ok &= TestAppliedBeforeObserve();
    ok &= TestPartialSealRejectsAppliedOnlyLower();
    ok &= TestPartialSealPreservesSameOriginGap();
    ok &= TestPartialLowerOriginDoesNotAdvancePrefix();
    ok &= TestPartialPureAppliedOnlySeal();
    ok &= TestDuplicateBarrierAndConflictIsolation();
    ok &= TestShenzhenSharedDomainAndTupleConflict();
    ok &= TestRetentionOutsideAndAbaToken();
    ok &= TestNonPowerOfTwoRetentionWrapAndReuse();
    ok &= TestZeroRetentionContiguousBacklog();
    ok &= TestPreallocatedCanonicalPayloadArena();
    ok &= TestResourceBoundsAndChannelIsolation();
    ok &= TestChannelAndReorderBounds();
    ok &= TestConfigurationValidation();
    if (!ok) {
        return 1;
    }
    std::cout << "native sequence recovery V1 tests passed\n";
    return 0;
}
