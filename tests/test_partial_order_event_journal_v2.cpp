#include "l2flow/ipc/certified_order_event_wire_v1.h"
#include "l2flow/ipc/partial_order_event_journal_v2.h"
#include "l2flow/ipc/partial_order_event_reader_v2.h"

#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string_view>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

namespace {

namespace ipc = l2flow::ipc;
namespace market = l2flow::market;

constexpr std::uint32_t kTradeDate = 20260803U;

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

class Descriptor final {
public:
    Descriptor() = default;
    Descriptor(const Descriptor&) = delete;
    Descriptor& operator=(const Descriptor&) = delete;
    ~Descriptor() {
        if (value_ >= 0) {
            static_cast<void>(::close(value_));
        }
    }

    [[nodiscard]] int* output() noexcept { return &value_; }
    [[nodiscard]] int get() const noexcept { return value_; }

private:
    int value_ = -1;
};

[[nodiscard]] ipc::PartialOrderEventJournalConfigV2 Config(
    std::uint64_t event_capacity = 64U,
    std::uint64_t order_state_capacity = 32U) noexcept {
    ipc::PartialOrderEventJournalConfigV2 result{};
    result.run_id[0U] = std::byte{0xA5};
    result.run_id[15U] = std::byte{0x5A};
    result.session_epoch = 7U;
    result.trade_date = kTradeDate;
    result.publication_generation = 11U;
    result.correction_epoch = 3U;
    result.coverage_start_unix_ns =
        1'785'700'800'000'000'000ULL;
    result.ordering_quality =
        ipc::PartialOrderEventOrderingQualityV2::
            kBoundedReorderedPartial;
    result.event_capacity = event_capacity;
    result.affected_channel_capacity = 8U;
    result.order_state_capacity = order_state_capacity;
    result.lazy_commit_chunk_bytes = 4096U;
    return result;
}

[[nodiscard]] ipc::PartialOrderEventExpectedSessionV2 Expected(
    const ipc::PartialOrderEventJournalSessionV2& session) noexcept {
    return {
        session.run_id,
        session.session_epoch,
        session.trade_date,
        session.publication_generation,
        session.correction_epoch};
}

[[nodiscard]] ipc::PartialOrderEventStatusUpdateV2 Contiguous(
    std::uint64_t captured_source_frontier) noexcept {
    ipc::PartialOrderEventStatusUpdateV2 result{};
    result.captured_source_frontier = captured_source_frontier;
    result.state =
        ipc::PartialOrderEventServiceStateV2::kContiguous;
    result.stale = false;
    result.last_error = ipc::PartialOrderEventLastErrorV2::kNone;
    return result;
}

[[nodiscard]] ipc::InstrumentDerivedEventV1 ShanghaiRevision(
    std::uint64_t derived_sequence,
    std::uint64_t source_tick,
    std::uint32_t ordinal,
    std::uint32_t instrument_id,
    std::int64_t order_id,
    std::uint64_t revision) noexcept {
    market::ShanghaiOrderRevisionEventV1 event{};
    event.operation = revision == 1U
                          ? market::ShanghaiOrderDeltaOperationV1::kInsert
                          : market::ShanghaiOrderDeltaOperationV1::kUpdate;
    event.source_anchor.native_event_sequence =
        static_cast<std::int64_t>(source_tick);
    event.source_anchor.source_sequence = 1'000U + source_tick;
    event.source_anchor.ingress_sequence = 2'000U + source_tick;
    event.source_anchor.tick_stream_sequence = source_tick;
    event.order.key.trade_date = kTradeDate;
    event.order.key.instrument_id = instrument_id;
    event.order.key.channel = 7;
    event.order.key.order_id = order_id;
    event.order.side = market::SideV1::kBuy;
    event.order.price_p6 = 10'000'000;
    event.order.price_valid = true;
    event.order.published_quantity = 100;
    event.order.published_quantity_valid = true;
    event.order.original_quantity = 100;
    event.order.original_quantity_valid = true;
    event.order.remaining_quantity =
        100 - static_cast<std::int64_t>(revision - 1U);
    event.order.remaining_quantity_valid = true;
    event.order.add_seen = true;
    event.order.apply_to_book = true;
    event.order.revision = revision;

    ipc::InstrumentDerivedEventV1 result{};
    result.derived_event_sequence = derived_sequence;
    result.payload = event;
    result.source_tick_event_ordinal = ordinal;
    result.source_tick_event_ordinal_valid = true;
    return result;
}

[[nodiscard]] bool CreatePair(
    const ipc::PartialOrderEventJournalConfigV2& config,
    std::shared_ptr<ipc::PartialOrderEventJournalProducerV2>* producer,
    std::unique_ptr<ipc::PartialOrderEventReaderV2>* reader) {
    int system_error = 0;
    const auto create_error =
        ipc::PartialOrderEventJournalProducerV2::Create(
            config, producer, &system_error);
    if (create_error !=
            ipc::PartialOrderEventJournalCreateErrorV2::kNone ||
        *producer == nullptr || system_error != 0) {
        std::cerr << "CreatePair producer error="
                  << ipc::PartialOrderEventJournalCreateErrorNameV2(
                         create_error)
                  << " errno=" << system_error << '\n';
        return false;
    }
    Descriptor descriptor;
    if (!(*producer)->DuplicateReadOnlyDescriptor(
            descriptor.output(), &system_error) ||
        system_error != 0) {
        return false;
    }
    const auto reader_error =
        ipc::PartialOrderEventReaderV2::OpenDescriptor(
            descriptor.get(),
            Expected((*producer)->session()),
            reader,
            &system_error);
    if (reader_error != ipc::PartialOrderEventReaderOpenErrorV2::kNone ||
        *reader == nullptr || system_error != 0) {
        std::cerr << "CreatePair reader error="
                  << ipc::PartialOrderEventReaderOpenErrorNameV2(
                         reader_error)
                  << " errno=" << system_error << '\n';
        return false;
    }
    return true;
}

bool TestDistinctAbiAndGenerationIdentity() {
    bool ok = true;
    ok &= Expect(
        ipc::kPartialOrderEventMagicV2 !=
            ipc::kCertifiedOrderEventMagicV1,
        "partial V2 magic is distinct from from-open CERTIFIED V1");
    ok &= Expect(
        ipc::kPartialOrderEventWireMajorV2 !=
            ipc::kCertifiedOrderEventWireMajorV1,
        "partial V2 major is distinct from CERTIFIED V1");

    auto unsupported_quality = Config();
    unsupported_quality.ordering_quality =
        static_cast<ipc::PartialOrderEventOrderingQualityV2>(2U);
    std::shared_ptr<ipc::PartialOrderEventJournalProducerV2>
        rejected_producer;
    ok &= Expect(
        ipc::PartialOrderEventJournalProducerV2::Create(
            unsupported_quality, &rejected_producer) ==
                ipc::PartialOrderEventJournalCreateErrorV2::
                    kInvalidConfiguration &&
            rejected_producer == nullptr,
        "V2 writer rejects unsupported native-order proof claims");

    auto oversized_scratch = Config(2U, 8U);
    oversized_scratch.maximum_events_per_commit = 3U;
    ok &= Expect(
        ipc::PartialOrderEventJournalProducerV2::Create(
            oversized_scratch, &rejected_producer) ==
                ipc::PartialOrderEventJournalCreateErrorV2::
                    kInvalidConfiguration &&
            rejected_producer == nullptr,
        "projection scratch bound cannot exceed Event capacity");

    std::shared_ptr<ipc::PartialOrderEventJournalProducerV2> producer;
    std::unique_ptr<ipc::PartialOrderEventReaderV2> reader;
    ok &= Expect(
        CreatePair(Config(), &producer, &reader),
        "create generation-aware producer and reader");
    if (producer == nullptr || reader == nullptr) {
        return false;
    }
    ipc::PartialOrderEventStatusSnapshotV2 status{};
    ok &= Expect(
        reader->ReadStatus(&status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            status.temporal_coverage ==
                ipc::PartialOrderEventTemporalCoverageV2::
                    kProcessStart &&
            status.ordering_quality ==
                ipc::PartialOrderEventOrderingQualityV2::
                    kBoundedReorderedPartial &&
            status.publication_generation == 11U &&
            status.correction_epoch == 3U &&
            status.cut.commit_sequence == 1U &&
            status.cut.canonical_apply_frontier == 0U &&
            status.cut.event_published_frontier == 0U &&
            status.cut.order_state_generation == 11U &&
            status.cut.order_state_canonical_frontier == 0U &&
            status.cut.state ==
                ipc::PartialOrderEventServiceStateV2::kInitializing &&
            status.cut.stale == 1U,
        "initial cut explicitly reports process-start identity and state");

    Descriptor descriptor;
    int system_error = 0;
    ok &= Expect(
        producer->DuplicateReadOnlyDescriptor(
            descriptor.output(), &system_error),
        "duplicate descriptor for mismatch check");
    auto wrong = Expected(producer->session());
    ++wrong.correction_epoch;
    std::unique_ptr<ipc::PartialOrderEventReaderV2> rejected;
    ok &= Expect(
        ipc::PartialOrderEventReaderV2::OpenDescriptor(
            descriptor.get(), wrong, &rejected, &system_error) ==
            ipc::PartialOrderEventReaderOpenErrorV2::
                kSessionMismatch &&
            rejected == nullptr,
        "reader fails closed across correction epochs");
    return ok;
}

bool TestStatusAndAffectedChannelCut() {
    std::shared_ptr<ipc::PartialOrderEventJournalProducerV2> producer;
    std::unique_ptr<ipc::PartialOrderEventReaderV2> reader;
    bool ok = CreatePair(Config(), &producer, &reader);
    if (!ok) {
        return false;
    }

    ipc::PartialOrderEventChannelHealthV2 health{};
    health.channel = 2012;
    health.expected_native_sequence = 109;
    health.contiguous_native_sequence = 108;
    health.highest_observed_native_sequence = 110;
    health.oldest_missing_native_sequence = 109;
    health.pending_count = 1U;
    health.oldest_gap_age_ns = 250U;
    health.market =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHENZHEN_V1;
    health.flags =
        ipc::kPartialOrderEventChannelOriginEstablishedV2 |
        ipc::kPartialOrderEventChannelAffectedV2 |
        ipc::kPartialOrderEventChannelStaleV2;
    health.state =
        ipc::PartialOrderEventServiceStateV2::kReordering;

    ipc::PartialOrderEventStatusUpdateV2 update{};
    update.captured_source_frontier = 5U;
    update.state =
        ipc::PartialOrderEventServiceStateV2::kReordering;
    update.stale = true;
    update.reorder_high_water = 1U;
    ok &= Expect(
        producer->PublishStatus(update, std::span{&health, 1U}) ==
            ipc::PartialOrderEventJournalPublishErrorV2::kNone,
        "publish gap status without advancing canonical Event history");

    ipc::PartialOrderEventStatusSnapshotV2 status{};
    std::array<ipc::PartialOrderEventChannelHealthV2, 1U> rows{};
    std::size_t rows_read = 0U;
    ok &= Expect(
        reader->ReadAffectedChannels(rows, &rows_read, &status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            rows_read == 1U && rows[0U].commit_sequence == 2U &&
            rows[0U].channel == 2012 &&
            status.cut.captured_source_frontier == 5U &&
            status.cut.canonical_apply_frontier == 0U &&
            status.cut.event_published_frontier == 0U &&
            status.cut.pending_count == 1U &&
            status.cut.affected_channel_count == 1U &&
            status.cut.oldest_gap_age_ns == 250U,
        "reader verifies affected-channel bank and coherent status cut");
    rows_read = 0U;
    ipc::PartialOrderEventStatusSnapshotV2 short_status{};
    ok &= Expect(
        reader->ReadAffectedChannels(
            {}, &rows_read, &short_status) ==
                ipc::PartialOrderEventReadResultV2::kOutputTooSmall &&
            rows_read == 1U &&
            short_status.cut.commit_sequence == 2U,
        "short channel read reports required rows and the same cut");

    std::array<ipc::PartialOrderEventEnvelopeV2, 1U> event_rows{};
    ipc::PartialOrderEventReadBatchResultV2 idle{};
    ok &= Expect(
        reader->ReadEvents(1U, event_rows, &idle) ==
                ipc::PartialOrderEventReadResultV2::kNotYetPublished &&
            idle.rows_read == 0U && idle.next_event_sequence == 1U &&
            idle.status.cut.commit_sequence == 2U,
        "idle Event tail returns monitoring status from its selected cut");
    return ok;
}

bool TestEventAndOrderStateJournal() {
    std::shared_ptr<ipc::PartialOrderEventJournalProducerV2> producer;
    std::unique_ptr<ipc::PartialOrderEventReaderV2> reader;
    bool ok = CreatePair(Config(), &producer, &reader);
    if (!ok) {
        return false;
    }

    auto first = ShanghaiRevision(1U, 1U, 0U, 17U, 42, 1U);
    ok &= Expect(
        producer->PublishCanonicalTick(
            1U,
            Contiguous(10U),
            std::span{&first, 1U}) ==
            ipc::PartialOrderEventJournalPublishErrorV2::kNone,
        "publish first canonical Event and materialized order state");

    ipc::PartialOrderEventStatusSnapshotV2 status{};
    ipc::PartialOrderEventEnvelopeV2 envelope{};
    ok &= Expect(
        reader->ReadEvent(1U, &envelope) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            envelope.canonical_apply_sequence == 1U &&
            envelope.event.derived_event_sequence == 1U &&
            envelope.event.instrument_id == 17U &&
            envelope.event.order_id == 42,
        "reader returns append-only Event inside visible cut");

    const ipc::PartialOrderEventOrderKeyV2 key{
        L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1,
        17U,
        7,
        42};
    ipc::PartialOrderEventOrderStateV2 state{};
    ok &= Expect(
        reader->FindOrderState(key, &state, &status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            state.canonical_apply_sequence == 1U &&
            state.order_revision.revision == 1U &&
            status.cut.shanghai_order_state_count == 1U &&
            status.cut.order_state_canonical_frontier == 1U,
        "order-state lookup uses full instrument-aware key");

    auto second_instrument =
        ShanghaiRevision(2U, 2U, 0U, 18U, 42, 1U);
    ok &= Expect(
        producer->PublishCanonicalTick(
            2U,
            Contiguous(11U),
            std::span{&second_instrument, 1U}) ==
            ipc::PartialOrderEventJournalPublishErrorV2::kNone,
        "same channel/order id in another instrument is a distinct key");
    const ipc::PartialOrderEventOrderKeyV2 second_key{
        L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1,
        18U,
        7,
        42};
    ipc::PartialOrderEventOrderStateV2 second_state{};
    ok &= Expect(
        reader->FindOrderState(second_key, &second_state, &status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            second_state.order_revision.instrument_id == 18U &&
            status.cut.shanghai_order_state_count == 2U,
        "hash equality includes instrument_id");
    return ok;
}

bool TestCanonicalMicrobatchPublishesOneCoherentCut() {
    std::shared_ptr<ipc::PartialOrderEventJournalProducerV2> producer;
    std::unique_ptr<ipc::PartialOrderEventReaderV2> reader;
    bool ok = CreatePair(Config(16U, 32U), &producer, &reader);
    if (!ok) {
        return false;
    }

    const std::array<ipc::PartialOrderEventCanonicalSliceV2, 3U> slices{
        ipc::PartialOrderEventCanonicalSliceV2{1U, 1U},
        ipc::PartialOrderEventCanonicalSliceV2{2U, 3U},
        ipc::PartialOrderEventCanonicalSliceV2{3U, 0U}};
    const std::array<ipc::InstrumentDerivedEventV1, 4U> events{
        ShanghaiRevision(1U, 1U, 0U, 17U, 42, 1U),
        ShanghaiRevision(2U, 2U, 0U, 17U, 42, 2U),
        ShanghaiRevision(3U, 2U, 1U, 17U, 43, 1U),
        ShanghaiRevision(4U, 2U, 2U, 17U, 44, 1U)};
    ok &= Expect(
        producer->PublishCanonicalBatch(
            slices, Contiguous(3U), events) ==
            ipc::PartialOrderEventJournalPublishErrorV2::kNone,
        "publish 1/3/0-Event canonical slices as one cut");

    ipc::PartialOrderEventStatusSnapshotV2 status{};
    ok &= Expect(
        reader->ReadStatus(&status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            producer->commit_sequence() == 2U &&
            producer->canonical_apply_frontier() == 3U &&
            producer->published_event_frontier() == 4U &&
            status.cut.commit_sequence == 2U &&
            status.cut.canonical_apply_frontier == 3U &&
            status.cut.history_generation == 3U &&
            status.cut.event_published_frontier == 4U &&
            status.cut.order_state_canonical_frontier == 3U &&
            status.cut.shanghai_order_state_count == 3U,
        "microbatch advances final frontiers with one publication sequence");

    std::array<ipc::PartialOrderEventEnvelopeV2, 4U> rows{};
    ipc::PartialOrderEventReadBatchResultV2 batch{};
    ok &= Expect(
        reader->ReadEvents(1U, rows, &batch) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            batch.rows_read == rows.size() &&
            batch.next_event_sequence == 5U &&
            batch.status.cut.canonical_apply_frontier == 3U &&
            rows[0U].canonical_apply_sequence == 1U &&
            rows[1U].canonical_apply_sequence == 2U &&
            rows[2U].canonical_apply_sequence == 2U &&
            rows[3U].canonical_apply_sequence == 2U,
        "reader retains each Event's owning canonical slice");

    const ipc::PartialOrderEventOrderKeyV2 repeated_key{
        L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1,
        17U,
        7,
        42};
    ipc::PartialOrderEventOrderStateV2 state{};
    ok &= Expect(
        reader->FindOrderState(repeated_key, &state, &status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            state.canonical_apply_sequence == 2U &&
            state.order_revision.revision == 2U &&
            status.cut.canonical_apply_frontier == 3U,
        "same-key state keeps the final update's real canonical sequence");
    return ok;
}

bool TestCanonicalMicrobatchRejectsInvalidSlicesBeforeMutation() {
    std::shared_ptr<ipc::PartialOrderEventJournalProducerV2> producer;
    std::unique_ptr<ipc::PartialOrderEventReaderV2> reader;
    auto config = Config(8U, 8U);
    config.maximum_events_per_commit = 2U;
    bool ok = CreatePair(config, &producer, &reader);
    if (!ok) {
        return false;
    }

    ok &= Expect(
        producer->PublishCanonicalBatch(
            {}, Contiguous(1U), {}) ==
            ipc::PartialOrderEventJournalPublishErrorV2::kInvalidArgument,
        "empty canonical slice list is not a status commit");

    const std::array<ipc::PartialOrderEventCanonicalSliceV2, 1U>
        wrong_origin{ipc::PartialOrderEventCanonicalSliceV2{2U, 0U}};
    ok &= Expect(
        producer->PublishCanonicalBatch(
            wrong_origin, Contiguous(1U), {}) ==
            ipc::PartialOrderEventJournalPublishErrorV2::
                kCanonicalSequence,
        "microbatch rejects a first slice beyond the next frontier");

    const std::array<ipc::PartialOrderEventCanonicalSliceV2, 2U>
        noncontiguous{
            ipc::PartialOrderEventCanonicalSliceV2{1U, 0U},
            ipc::PartialOrderEventCanonicalSliceV2{3U, 0U}};
    ok &= Expect(
        producer->PublishCanonicalBatch(
            noncontiguous, Contiguous(1U), {}) ==
            ipc::PartialOrderEventJournalPublishErrorV2::
                kCanonicalSequence,
        "microbatch rejects a hole between canonical slices");

    auto one_event = ShanghaiRevision(1U, 1U, 0U, 17U, 42, 1U);
    const std::array<ipc::PartialOrderEventCanonicalSliceV2, 1U>
        wrong_count{ipc::PartialOrderEventCanonicalSliceV2{1U, 2U}};
    ok &= Expect(
        producer->PublishCanonicalBatch(
            wrong_count,
            Contiguous(1U),
            std::span{&one_event, 1U}) ==
            ipc::PartialOrderEventJournalPublishErrorV2::kInvalidArgument,
        "microbatch rejects a slice/Event count mismatch");

    std::array<ipc::InstrumentDerivedEventV1, 2U> wrong_ordinal{
        ShanghaiRevision(1U, 1U, 0U, 17U, 42, 1U),
        ShanghaiRevision(2U, 2U, 1U, 17U, 43, 1U)};
    const std::array<ipc::PartialOrderEventCanonicalSliceV2, 2U>
        two_singletons{
            ipc::PartialOrderEventCanonicalSliceV2{1U, 1U},
            ipc::PartialOrderEventCanonicalSliceV2{2U, 1U}};
    ok &= Expect(
        producer->PublishCanonicalBatch(
            two_singletons, Contiguous(2U), wrong_ordinal) ==
            ipc::PartialOrderEventJournalPublishErrorV2::kProjectionError,
        "source tick ordinal must reset at every canonical slice");

    ipc::PartialOrderEventStatusSnapshotV2 status{};
    ok &= Expect(
        reader->ReadStatus(&status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            status.cut.commit_sequence == 1U &&
            status.cut.canonical_apply_frontier == 0U &&
            status.cut.event_published_frontier == 0U &&
            !producer->failed(),
        "invalid boundaries and ordinals leave the initial cut untouched");

    wrong_ordinal[1U] = ShanghaiRevision(2U, 2U, 0U, 17U, 43, 1U);
    ok &= Expect(
        producer->PublishCanonicalBatch(
            two_singletons, Contiguous(2U), wrong_ordinal) ==
                ipc::PartialOrderEventJournalPublishErrorV2::kNone &&
            reader->ReadStatus(&status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            status.cut.canonical_apply_frontier == 2U &&
            status.cut.event_published_frontier == 2U,
        "corrected ordinal reset remains publishable after rejection");

    const std::array<ipc::PartialOrderEventCanonicalSliceV2, 3U>
        oversized_slices{
            ipc::PartialOrderEventCanonicalSliceV2{3U, 1U},
            ipc::PartialOrderEventCanonicalSliceV2{4U, 1U},
            ipc::PartialOrderEventCanonicalSliceV2{5U, 1U}};
    const std::array<ipc::InstrumentDerivedEventV1, 3U>
        oversized_events{
            ShanghaiRevision(3U, 3U, 0U, 17U, 44, 1U),
            ShanghaiRevision(4U, 4U, 0U, 17U, 45, 1U),
            ShanghaiRevision(5U, 5U, 0U, 17U, 46, 1U)};
    ok &= Expect(
        producer->PublishCanonicalBatch(
            oversized_slices,
            Contiguous(5U),
            oversized_events) ==
            ipc::PartialOrderEventJournalPublishErrorV2::kEventCapacity,
        "microbatch rejects an Event count beyond reserved scratch");
    ok &= Expect(
        reader->ReadStatus(&status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            status.cut.canonical_apply_frontier == 2U &&
            status.cut.event_published_frontier == 2U &&
            !producer->failed(),
        "scratch-bound rejection occurs before publication mutation");
    return ok;
}

bool TestEveryCommitFailpointFallsBackToOldCut() {
    bool ok = true;
    constexpr std::array<ipc::PartialOrderEventCommitFailpointV2, 6U>
        failpoints{
            ipc::PartialOrderEventCommitFailpointV2::
                kAfterTargetCutInvalidated,
            ipc::PartialOrderEventCommitFailpointV2::
                kAfterChannelBankWritten,
            ipc::PartialOrderEventCommitFailpointV2::
                kAfterOrderStateKeyInvalidated,
            ipc::PartialOrderEventCommitFailpointV2::
                kAfterOrderStateVersionInvalidated,
            ipc::PartialOrderEventCommitFailpointV2::
                kAfterEventRowsWritten,
            ipc::PartialOrderEventCommitFailpointV2::
                kBeforeCutPublished};

    for (const auto failpoint : failpoints) {
        std::shared_ptr<ipc::PartialOrderEventJournalProducerV2>
            producer;
        std::unique_ptr<ipc::PartialOrderEventReaderV2> reader;
        ok &= Expect(
            CreatePair(Config(), &producer, &reader),
            "create failpoint fixture");
        if (producer == nullptr || reader == nullptr) {
            return false;
        }
        auto first = ShanghaiRevision(1U, 1U, 0U, 17U, 42, 1U);
        ok &= Expect(
            producer->PublishCanonicalTick(
                1U,
                Contiguous(1U),
                std::span{&first, 1U}) ==
                ipc::PartialOrderEventJournalPublishErrorV2::kNone,
            "publish failpoint baseline");

        // The first future Event updates an existing key and the second adds a
        // new key. Together they prove that all pre-cut microbatch state,
        // including per-slice canonical versions, remains invisible until the
        // one final cut store.
        const std::array<ipc::PartialOrderEventCanonicalSliceV2, 3U>
            slices{
                ipc::PartialOrderEventCanonicalSliceV2{2U, 1U},
                ipc::PartialOrderEventCanonicalSliceV2{3U, 1U},
                ipc::PartialOrderEventCanonicalSliceV2{4U, 0U}};
        const std::array<ipc::InstrumentDerivedEventV1, 2U> future{
            ShanghaiRevision(2U, 2U, 0U, 17U, 42, 2U),
            ShanghaiRevision(3U, 3U, 0U, 19U, 99, 1U)};
        producer->SetCommitFailpointForTest(failpoint);
        ok &= Expect(
            producer->PublishCanonicalBatch(
                slices, Contiguous(4U), future) ==
                    ipc::PartialOrderEventJournalPublishErrorV2::
                        kInjectedFailure &&
                producer->failed(),
            "injected microbatch interruption is terminal for writer");

        ipc::PartialOrderEventStatusSnapshotV2 status{};
        ok &= Expect(
            reader->ReadStatus(&status) ==
                    ipc::PartialOrderEventReadResultV2::kOk &&
                status.cut.commit_sequence == 2U &&
                status.cut.canonical_apply_frontier == 1U &&
                status.cut.event_published_frontier == 1U &&
                status.cut.shanghai_order_state_count == 1U,
            "reader selects highest complete old cut");
        ipc::PartialOrderEventEnvelopeV2 hidden_event{};
        ok &= Expect(
            reader->ReadEvent(2U, &hidden_event) ==
                ipc::PartialOrderEventReadResultV2::kNotYetPublished,
            "future append-only Event row is hidden by old cut");
        const ipc::PartialOrderEventOrderKeyV2 future_key{
            L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1,
            19U,
            7,
            99};
        ipc::PartialOrderEventOrderStateV2 hidden_state{};
        ok &= Expect(
            reader->FindOrderState(future_key, &hidden_state) ==
                ipc::PartialOrderEventReadResultV2::kNotFound,
            "future key/version is filtered by old canonical cut");
        const ipc::PartialOrderEventOrderKeyV2 baseline_key{
            L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1,
            17U,
            7,
            42};
        ipc::PartialOrderEventOrderStateV2 baseline_state{};
        ok &= Expect(
            reader->FindOrderState(
                baseline_key, &baseline_state, &status) ==
                    ipc::PartialOrderEventReadResultV2::kOk &&
                baseline_state.canonical_apply_sequence == 1U &&
                baseline_state.order_revision.revision == 1U,
            "future same-key microbatch version leaves last-good state visible");
    }
    return ok;
}

bool TestFutureVersionCannotReplaceLastGoodState() {
    std::shared_ptr<ipc::PartialOrderEventJournalProducerV2> producer;
    std::unique_ptr<ipc::PartialOrderEventReaderV2> reader;
    bool ok = CreatePair(Config(), &producer, &reader);
    if (!ok) {
        return false;
    }
    for (std::uint64_t revision = 1U; revision <= 3U; ++revision) {
        auto event = ShanghaiRevision(
            revision, revision, 0U, 17U, 42, revision);
        ok &= Expect(
            producer->PublishCanonicalTick(
                revision,
                Contiguous(revision),
                std::span{&event, 1U}) ==
                ipc::PartialOrderEventJournalPublishErrorV2::kNone,
            "publish alternating order-state versions");
    }

    auto future = ShanghaiRevision(4U, 4U, 0U, 17U, 42, 4U);
    producer->SetCommitFailpointForTest(
        ipc::PartialOrderEventCommitFailpointV2::
            kAfterOrderStateVersionInvalidated);
    ok &= Expect(
        producer->PublishCanonicalTick(
            4U,
            Contiguous(4U),
            std::span{&future, 1U}) ==
            ipc::PartialOrderEventJournalPublishErrorV2::
                kInjectedFailure,
        "interrupt while older state version has an odd tag");

    const ipc::PartialOrderEventOrderKeyV2 key{
        L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1,
        17U,
        7,
        42};
    ipc::PartialOrderEventOrderStateV2 state{};
    ipc::PartialOrderEventStatusSnapshotV2 status{};
    ok &= Expect(
        reader->FindOrderState(key, &state, &status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            status.cut.canonical_apply_frontier == 3U &&
            state.canonical_apply_sequence == 3U &&
            state.order_revision.revision == 3U,
        "odd future version is ignored and last-good revision remains readable");
    return ok;
}

bool TestOrderStateBackingChunkIsReused() {
    auto config = Config(16U, 32U);
    config.lazy_commit_chunk_bytes = 64ULL * 1024ULL * 1024ULL;
    std::shared_ptr<ipc::PartialOrderEventJournalProducerV2> producer;
    std::unique_ptr<ipc::PartialOrderEventReaderV2> reader;
    bool ok = CreatePair(config, &producer, &reader);
    if (!ok) {
        return false;
    }
    const auto initial = producer->ResourceSnapshot();
    ok &= Expect(
        initial.event_backing_allocation_calls == 0U &&
            initial.order_state_backed_bytes == 0U &&
            initial.order_state_backed_chunk_count == 0U &&
            initial.order_state_backing_allocation_calls == 0U &&
            !initial.fully_preallocated,
        "Event and order-state regions start logically sparse");

    for (std::uint64_t sequence = 1U; sequence <= 3U; ++sequence) {
        auto event = ShanghaiRevision(
            sequence, sequence, 0U, 17U, 42, sequence);
        ok &= Expect(
            producer->PublishCanonicalTick(
                sequence,
                Contiguous(sequence),
                std::span{&event, 1U}) ==
                ipc::PartialOrderEventJournalPublishErrorV2::kNone,
            "publish repeated revisions in one order-state chunk");
    }
    const auto repeated = producer->ResourceSnapshot();
    ok &= Expect(
        repeated.event_backing_allocation_calls == 1U &&
            repeated.order_state_backed_bytes != 0U &&
            repeated.order_state_backed_chunk_count == 1U &&
            repeated.order_state_backing_allocation_calls == 1U,
        "first revision backs each lazy region and later revisions reuse it");

    auto second_key = ShanghaiRevision(4U, 4U, 0U, 17U, 43, 1U);
    ok &= Expect(
        producer->PublishCanonicalTick(
            4U, Contiguous(4U), std::span{&second_key, 1U}) ==
            ipc::PartialOrderEventJournalPublishErrorV2::kNone,
        "publish another hash key inside the already backed region");
    const auto second = producer->ResourceSnapshot();
    ok &= Expect(
        second.event_backing_allocation_calls ==
                repeated.event_backing_allocation_calls &&
            second.order_state_backed_bytes ==
                repeated.order_state_backed_bytes &&
            second.order_state_backed_chunk_count ==
                repeated.order_state_backed_chunk_count &&
            second.order_state_backing_allocation_calls ==
                repeated.order_state_backing_allocation_calls,
        "another key in the same lazy chunk performs no backing syscall");

    ipc::PartialOrderEventStatusSnapshotV2 status{};
    ok &= Expect(
        reader->ReadStatus(&status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            status.cut.canonical_apply_frontier == 4U &&
            status.cut.shanghai_order_state_count == 2U,
        "chunk reuse preserves the complete public cut and state count");
    return ok;
}

bool TestPreallocatedBackingRemainsStableAcrossCommits() {
    auto config = Config(16U, 32U);
    std::shared_ptr<ipc::PartialOrderEventJournalProducerV2> producer;
    std::unique_ptr<ipc::PartialOrderEventReaderV2> reader;
    bool ok = CreatePair(config, &producer, &reader);
    if (!ok) {
        return false;
    }

    const auto align_page = [](std::uint64_t value) noexcept {
        return ((value + 4095U) / 4096U) * 4096U;
    };
    const std::uint64_t event_region_bytes = align_page(
        static_cast<std::uint64_t>(ipc::kPartialOrderEventHeaderBytesV2) +
        config.event_capacity *
            static_cast<std::uint64_t>(
                ipc::kPartialOrderEventSlotBytesV2));
    const std::uint64_t channel_bank_bytes = align_page(
        static_cast<std::uint64_t>(config.affected_channel_capacity) *
        static_cast<std::uint64_t>(
            ipc::kPartialOrderEventChannelHealthBytesV2));
    const std::uint64_t order_state_offset =
        event_region_bytes + 2U * channel_bank_bytes;
    const std::uint64_t order_state_region_bytes =
        producer->session().total_mapping_bytes - order_state_offset;
    const std::uint64_t order_state_chunk_count =
        (order_state_region_bytes + config.lazy_commit_chunk_bytes - 1U) /
        config.lazy_commit_chunk_bytes;

    int system_error = -1;
    ok &= Expect(
        producer->PreallocateBacking(&system_error) ==
                ipc::PartialOrderEventJournalPublishErrorV2::kNone &&
            system_error == 0,
        "preallocate the complete Event and order-state backing once");
    const auto preallocated = producer->ResourceSnapshot();
    ok &= Expect(
        preallocated.committed_event_region_bytes ==
                event_region_bytes &&
            preallocated.order_state_backed_bytes ==
                order_state_region_bytes &&
            preallocated.order_state_backed_chunk_count ==
                order_state_chunk_count &&
            preallocated.order_state_backing_allocation_calls == 1U &&
            preallocated.event_backing_allocation_calls == 1U &&
            preallocated.order_state_prefault_attempts == 0U &&
            preallocated.order_state_prefaulted_bytes == 0U &&
            preallocated.fully_preallocated,
        "preallocation snapshot accounts for the complete fixed mapping");

    for (std::uint64_t sequence = 1U; sequence <= 3U; ++sequence) {
        auto event = ShanghaiRevision(
            sequence,
            sequence,
            0U,
            17U,
            40 + static_cast<std::int64_t>(sequence),
            1U);
        ok &= Expect(
            producer->PublishCanonicalTick(
                sequence,
                Contiguous(sequence),
                std::span{&event, 1U}) ==
                ipc::PartialOrderEventJournalPublishErrorV2::kNone,
            "publish without growing fully preallocated backing");
        const auto current = producer->ResourceSnapshot();
        ok &= Expect(
            current.committed_event_region_bytes ==
                    preallocated.committed_event_region_bytes &&
                current.event_backing_allocation_calls ==
                    preallocated.event_backing_allocation_calls &&
                current.order_state_backed_bytes ==
                    preallocated.order_state_backed_bytes &&
                current.order_state_backed_chunk_count ==
                    preallocated.order_state_backed_chunk_count &&
                current.order_state_backing_allocation_calls ==
                    preallocated.order_state_backing_allocation_calls &&
                current.order_state_prefault_attempts ==
                    preallocated.order_state_prefault_attempts &&
                current.order_state_prefaulted_bytes ==
                    preallocated.order_state_prefaulted_bytes &&
                current.fully_preallocated,
            "resource counters remain stable after a preallocated commit");
        if (sequence == 1U) {
            system_error = -1;
            ok &= Expect(
                producer->PreallocateBacking(&system_error) ==
                        ipc::PartialOrderEventJournalPublishErrorV2::
                            kInvalidArgument &&
                    system_error == 0,
                "preallocation is rejected after the first published commit");
        }
    }
    return ok;
}

bool TestConfiguredOrderStatePrefaultHasExplicitOutcome() {
    auto config = Config(16U, 32U);
    config.prefault_order_state_pages = true;
    std::shared_ptr<ipc::PartialOrderEventJournalProducerV2> producer;
    std::unique_ptr<ipc::PartialOrderEventReaderV2> reader;
    bool ok = CreatePair(config, &producer, &reader);
    if (!ok) {
        return false;
    }

    int system_error = -1;
    const auto result = producer->PreallocateBacking(&system_error);
    const auto resources = producer->ResourceSnapshot();
    const bool unsupported =
        result ==
            ipc::PartialOrderEventJournalPublishErrorV2::
                kBackingCommitFailed &&
        (system_error == ENOTSUP || system_error == EINVAL ||
         system_error == ENOSYS || system_error == EOPNOTSUPP);
    if (result == ipc::PartialOrderEventJournalPublishErrorV2::kNone) {
        ok &= Expect(
            system_error == 0 && resources.fully_preallocated &&
                resources.event_backing_allocation_calls == 1U &&
                resources.order_state_backing_allocation_calls == 1U &&
                resources.order_state_prefault_attempts == 1U &&
                resources.order_state_prefaulted_bytes ==
                    resources.order_state_backed_bytes &&
                resources.order_state_prefaulted_bytes != 0U,
            "supported prefault synchronously covers the order-state region");

        system_error = -1;
        ok &= Expect(
            producer->PreallocateBacking(&system_error) ==
                    ipc::PartialOrderEventJournalPublishErrorV2::kNone &&
                system_error == 0,
            "successful prefault preallocation is idempotent before publish");
        const auto repeated = producer->ResourceSnapshot();
        ok &= Expect(
            repeated.event_backing_allocation_calls ==
                    resources.event_backing_allocation_calls &&
                repeated.order_state_backing_allocation_calls ==
                    resources.order_state_backing_allocation_calls &&
                repeated.order_state_prefault_attempts ==
                    resources.order_state_prefault_attempts &&
                repeated.order_state_prefaulted_bytes ==
                    resources.order_state_prefaulted_bytes &&
                repeated.fully_preallocated,
            "idempotent preallocation performs no second backing or prefault call");
    } else {
        ok &= Expect(
            unsupported && !resources.fully_preallocated &&
                resources.event_backing_allocation_calls == 1U &&
                resources.order_state_backing_allocation_calls == 1U &&
                resources.order_state_prefault_attempts == 1U &&
                resources.order_state_prefaulted_bytes == 0U,
            "unsupported prefault fails explicitly after backing allocation");
    }
    return ok;
}

bool TestBatchStatePlanDeduplicatesWithoutMutationOnCapacity() {
    auto config = Config(8U, 8U);
    config.maximum_order_state_updates_per_commit = 1U;
    std::shared_ptr<ipc::PartialOrderEventJournalProducerV2> producer;
    std::unique_ptr<ipc::PartialOrderEventReaderV2> reader;
    bool ok = CreatePair(config, &producer, &reader);
    if (!ok) {
        return false;
    }

    std::array<ipc::InstrumentDerivedEventV1, 2U> same_key{
        ShanghaiRevision(1U, 1U, 0U, 17U, 42, 1U),
        ShanghaiRevision(2U, 1U, 1U, 17U, 42, 2U)};
    ok &= Expect(
        producer->PublishCanonicalTick(
            1U, Contiguous(1U), same_key) ==
            ipc::PartialOrderEventJournalPublishErrorV2::kNone,
        "batch plan coalesces repeated revisions of one state key");
    const ipc::PartialOrderEventOrderKeyV2 key{
        L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1,
        17U,
        7,
        42};
    ipc::PartialOrderEventOrderStateV2 state{};
    ipc::PartialOrderEventStatusSnapshotV2 status{};
    ok &= Expect(
        reader->FindOrderState(key, &state, &status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            state.order_revision.revision == 2U &&
            status.cut.canonical_apply_frontier == 1U &&
            status.cut.shanghai_order_state_count == 1U,
        "coalesced plan publishes the last revision and one state key");

    std::array<ipc::InstrumentDerivedEventV1, 2U> distinct_keys{
        ShanghaiRevision(3U, 2U, 0U, 17U, 43, 1U),
        ShanghaiRevision(4U, 2U, 1U, 17U, 44, 1U)};
    ok &= Expect(
        producer->PublishCanonicalTick(
            2U, Contiguous(2U), distinct_keys) ==
            ipc::PartialOrderEventJournalPublishErrorV2::
                kOrderStateCapacity,
        "batch plan bound rejects two distinct state keys");
    ok &= Expect(
        reader->ReadStatus(&status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            status.cut.canonical_apply_frontier == 1U &&
            status.cut.event_published_frontier == 2U &&
            status.cut.shanghai_order_state_count == 1U,
        "plan capacity rejection occurs before public publication mutation");
    return ok;
}

bool TestDefaultScratchCoversLargeEndStyleBatch() {
    constexpr std::size_t kRevisionCount = 4'096U;
    std::shared_ptr<ipc::PartialOrderEventJournalProducerV2> producer;
    std::unique_ptr<ipc::PartialOrderEventReaderV2> reader;
    auto config = Config(kRevisionCount, 8'192U);
    config.lazy_commit_chunk_bytes = 64ULL * 1024ULL * 1024ULL;
    bool ok = CreatePair(config, &producer, &reader);
    if (!ok) {
        return false;
    }
    std::vector<ipc::InstrumentDerivedEventV1> revisions;
    revisions.reserve(kRevisionCount);
    for (std::size_t index = 0U; index < kRevisionCount; ++index) {
        revisions.push_back(ShanghaiRevision(
            static_cast<std::uint64_t>(index) + 1U,
            1U,
            static_cast<std::uint32_t>(index),
            17U,
            1'000 + static_cast<std::int64_t>(index),
            2U));
    }
    ok &= Expect(
        producer->PublishCanonicalTick(
            1U, Contiguous(1U), revisions) ==
            ipc::PartialOrderEventJournalPublishErrorV2::kNone,
        "default scratch derives from state capacity, not a hidden 8-row cap");
    ipc::PartialOrderEventStatusSnapshotV2 status{};
    ok &= Expect(
        reader->ReadStatus(&status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            status.cut.event_published_frontier == kRevisionCount &&
            status.cut.shanghai_order_state_count == kRevisionCount,
        "large single-canonical-input revision batch is complete");

    const auto overflow_sequence =
        std::numeric_limits<std::uint64_t>::max() / 2U + 1U;
    ok &= Expect(
        producer->PublishCanonicalTick(
            overflow_sequence, Contiguous(2U), {}) ==
            ipc::PartialOrderEventJournalPublishErrorV2::
                kCanonicalSequence,
        "canonical sequence cannot overflow doubled version tags");
    return ok;
}

bool TestCreateWithMultiDigitDescriptorPath() {
    std::array<Descriptor, 32U> held_descriptors{};
    int highest_descriptor = -1;
    for (Descriptor& descriptor : held_descriptors) {
        int opened = -1;
        do {
            opened = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
        } while (opened < 0 && errno == EINTR);
        *descriptor.output() = opened;
        if (opened < 0) {
            break;
        }
        highest_descriptor = opened;
        if (highest_descriptor >= 10) {
            break;
        }
    }

    bool ok = Expect(
        highest_descriptor >= 10,
        "reserve every lower descriptor before journal creation");
    if (!ok) {
        return false;
    }
    std::shared_ptr<ipc::PartialOrderEventJournalProducerV2> producer;
    std::unique_ptr<ipc::PartialOrderEventReaderV2> reader;
    ok &= Expect(
        CreatePair(Config(), &producer, &reader),
        "noexcept journal creation handles a multi-digit memfd path");
    return ok;
}

bool TestConcurrentSameKeyPublicationIsCoherent() {
    constexpr std::uint64_t kIterations = 1'000U;
    auto config = Config(kIterations, 32U);
    std::shared_ptr<ipc::PartialOrderEventJournalProducerV2> producer;
    std::unique_ptr<ipc::PartialOrderEventReaderV2> first_reader;
    bool ok = CreatePair(config, &producer, &first_reader);
    if (!ok) {
        return false;
    }

    Descriptor descriptor;
    int system_error = 0;
    std::unique_ptr<ipc::PartialOrderEventReaderV2> second_reader;
    ok &= Expect(
        producer->DuplicateReadOnlyDescriptor(
            descriptor.output(), &system_error) &&
            system_error == 0 &&
            ipc::PartialOrderEventReaderV2::OpenDescriptor(
                descriptor.get(),
                Expected(producer->session()),
                &second_reader,
                &system_error) ==
                ipc::PartialOrderEventReaderOpenErrorV2::kNone &&
            second_reader != nullptr && system_error == 0,
        "open an independent concurrent journal reader");
    if (!ok) {
        return false;
    }

    const ipc::PartialOrderEventOrderKeyV2 key{
        L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1,
        17U,
        7,
        42};
    std::atomic<bool> start{false};
    std::atomic<bool> writer_done{false};
    std::atomic<std::uint32_t> read_failure_code{0U};
    std::atomic<std::uint64_t> read_samples{0U};
    const auto record_read_failure =
        [&](std::uint32_t code) noexcept {
            std::uint32_t expected = 0U;
            static_cast<void>(read_failure_code.compare_exchange_strong(
                expected,
                code,
                std::memory_order_relaxed,
                std::memory_order_relaxed));
        };

    const auto read_loop = [&](
                               std::unique_ptr<
                                   ipc::PartialOrderEventReaderV2>
                                   local_reader) {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        std::uint64_t last_commit_sequence = 0U;
        std::uint64_t last_state_revision = 0U;
        do {
            ipc::PartialOrderEventStatusSnapshotV2 status{};
            const auto status_result = local_reader->ReadStatus(&status);
            if (status_result ==
                ipc::PartialOrderEventReadResultV2::kOk) {
                if (status.cut.commit_sequence < last_commit_sequence ||
                    status.cut.canonical_apply_frontier > kIterations ||
                    status.cut.event_published_frontier !=
                        status.cut.canonical_apply_frontier ||
                    status.cut.order_state_canonical_frontier !=
                        status.cut.canonical_apply_frontier ||
                    (status.cut.canonical_apply_frontier == 0U
                         ? status.cut.shanghai_order_state_count != 0U
                         : status.cut.shanghai_order_state_count != 1U)) {
                    record_read_failure(1U);
                }
                last_commit_sequence = status.cut.commit_sequence;
                if (status.cut.event_published_frontier != 0U) {
                    ipc::PartialOrderEventEnvelopeV2 envelope{};
                    const std::uint64_t sequence =
                        status.cut.event_published_frontier;
                    const auto event_result =
                        local_reader->ReadEvent(sequence, &envelope);
                    if (event_result ==
                        ipc::PartialOrderEventReadResultV2::kOk) {
                        if (envelope.canonical_apply_sequence != sequence ||
                            envelope.event.derived_event_sequence !=
                                sequence ||
                            envelope.event.revision != sequence ||
                            envelope.event.instrument_id !=
                                key.instrument_id ||
                            envelope.event.channel != key.channel ||
                            envelope.event.order_id != key.order_id) {
                            record_read_failure(2U);
                        }
                    } else if (
                        event_result !=
                            ipc::PartialOrderEventReadResultV2::
                                kNotYetPublished &&
                        event_result !=
                            ipc::PartialOrderEventReadResultV2::
                                kInconsistent) {
                        record_read_failure(2U);
                    }
                }
            } else if (
                status_result !=
                ipc::PartialOrderEventReadResultV2::kInconsistent) {
                record_read_failure(3U);
            }

            ipc::PartialOrderEventOrderStateV2 state{};
            ipc::PartialOrderEventStatusSnapshotV2 state_status{};
            const auto state_result = local_reader->FindOrderState(
                key, &state, &state_status);
            if (state_result ==
                ipc::PartialOrderEventReadResultV2::kOk) {
                if (state.canonical_apply_sequence < last_state_revision ||
                    state.canonical_apply_sequence >
                        state_status.cut.order_state_canonical_frontier ||
                    state.order_revision.revision !=
                        state.canonical_apply_sequence ||
                    state.order_revision.instrument_id != key.instrument_id ||
                    state.order_revision.channel != key.channel ||
                    state.order_revision.order_id != key.order_id) {
                    record_read_failure(4U);
                }
                last_state_revision = state.canonical_apply_sequence;
            } else if (
                state_result !=
                    ipc::PartialOrderEventReadResultV2::kInconsistent &&
                state_result !=
                    ipc::PartialOrderEventReadResultV2::kNotFound) {
                record_read_failure(5U);
            }

            std::array<ipc::PartialOrderEventOrderStateV2, 2U> states{};
            ipc::PartialOrderEventOrderStateBatchResultV2 state_batch{};
            const auto state_batch_result = local_reader->ReadOrderStates(
                0U, states, &state_batch);
            if (state_batch_result ==
                ipc::PartialOrderEventReadResultV2::kOk) {
                if (state_batch.rows_read > 1U ||
                    state_batch.next_physical_slot !=
                        config.order_state_capacity ||
                    (state_batch.rows_read == 1U &&
                     (states[0U].canonical_apply_sequence >
                          state_batch.status.cut
                              .order_state_canonical_frontier ||
                      states[0U].order_revision.revision !=
                          states[0U].canonical_apply_sequence ||
                      states[0U].order_revision.instrument_id !=
                          key.instrument_id ||
                      states[0U].order_revision.channel != key.channel ||
                      states[0U].order_revision.order_id != key.order_id))) {
                    record_read_failure(6U);
                }
            } else if (
                state_batch_result !=
                ipc::PartialOrderEventReadResultV2::kInconsistent) {
                record_read_failure(7U);
            }

            std::size_t channel_rows = 0U;
            const auto channels_result =
                local_reader->ReadAffectedChannels({}, &channel_rows);
            if ((channels_result !=
                     ipc::PartialOrderEventReadResultV2::kOk &&
                 channels_result !=
                     ipc::PartialOrderEventReadResultV2::kInconsistent) ||
                (channels_result ==
                     ipc::PartialOrderEventReadResultV2::kOk &&
                 channel_rows != 0U)) {
                record_read_failure(8U);
            }
            read_samples.fetch_add(1U, std::memory_order_relaxed);
        } while (!writer_done.load(std::memory_order_acquire));
    };

    std::thread first_thread(read_loop, std::move(first_reader));
    std::thread second_thread(read_loop, std::move(second_reader));
    start.store(true, std::memory_order_release);
    for (std::uint64_t revision = 1U; revision <= kIterations;
         ++revision) {
        auto event = ShanghaiRevision(
            revision, revision, 0U, 17U, 42, revision);
        if (producer->PublishCanonicalTick(
                revision,
                Contiguous(revision),
                std::span{&event, 1U}) !=
            ipc::PartialOrderEventJournalPublishErrorV2::kNone) {
            ok = false;
            break;
        }
        if ((revision & 15U) == 0U) {
            std::this_thread::yield();
        }
    }
    writer_done.store(true, std::memory_order_release);
    first_thread.join();
    second_thread.join();

    const std::uint32_t failure_code =
        read_failure_code.load(std::memory_order_relaxed);
    if (failure_code != 0U) {
        std::cerr << "concurrent read failure code=" << failure_code << '\n';
    }
    ok &= Expect(
        failure_code == 0U &&
            read_samples.load(std::memory_order_relaxed) != 0U,
        "concurrent readers observe only coherent cuts, Events, and states");
    ipc::PartialOrderEventOrderStateV2 final_state{};
    ipc::PartialOrderEventStatusSnapshotV2 final_status{};
    ok &= Expect(
        producer->commit_sequence() == kIterations + 1U &&
            producer->canonical_apply_frontier() == kIterations &&
            producer->published_event_frontier() == kIterations,
        "writer completes every same-key commit without tag conflict");

    Descriptor final_descriptor;
    std::unique_ptr<ipc::PartialOrderEventReaderV2> final_reader;
    system_error = 0;
    const bool final_reader_opened =
        producer->DuplicateReadOnlyDescriptor(
            final_descriptor.output(), &system_error) &&
        system_error == 0 &&
        ipc::PartialOrderEventReaderV2::OpenDescriptor(
            final_descriptor.get(),
            Expected(producer->session()),
            &final_reader,
            &system_error) ==
            ipc::PartialOrderEventReaderOpenErrorV2::kNone &&
        final_reader != nullptr && system_error == 0;
    ok &= Expect(
        final_reader_opened,
        "open final reader after concurrent publication stops");
    if (final_reader_opened) {
        ok &= Expect(
            final_reader->FindOrderState(
                key, &final_state, &final_status) ==
                    ipc::PartialOrderEventReadResultV2::kOk &&
                final_status.cut.canonical_apply_frontier == kIterations &&
                final_state.canonical_apply_sequence == kIterations &&
                final_state.order_revision.revision == kIterations,
            "final same-key order state is exact after writer stops");
    }
    return ok;
}

}  // namespace

int main() {
    bool ok = true;
    ok &= TestDistinctAbiAndGenerationIdentity();
    ok &= TestStatusAndAffectedChannelCut();
    ok &= TestEventAndOrderStateJournal();
    ok &= TestCanonicalMicrobatchPublishesOneCoherentCut();
    ok &= TestCanonicalMicrobatchRejectsInvalidSlicesBeforeMutation();
    ok &= TestEveryCommitFailpointFallsBackToOldCut();
    ok &= TestFutureVersionCannotReplaceLastGoodState();
    ok &= TestOrderStateBackingChunkIsReused();
    ok &= TestPreallocatedBackingRemainsStableAcrossCommits();
    ok &= TestConfiguredOrderStatePrefaultHasExplicitOutcome();
    ok &= TestBatchStatePlanDeduplicatesWithoutMutationOnCapacity();
    ok &= TestDefaultScratchCoversLargeEndStyleBatch();
    ok &= TestCreateWithMultiDigitDescriptorPath();
    ok &= TestConcurrentSameKeyPublicationIsCoherent();
    if (!ok) {
        return 1;
    }
    std::cout << "partial order Event V2 journal tests passed\n";
    return 0;
}
