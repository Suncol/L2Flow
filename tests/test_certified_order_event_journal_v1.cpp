#include "l2flow/ipc/certified_order_event_journal_v1.h"
#include "l2flow/ipc/certified_order_event_reader_v1.h"
#include "l2flow/ipc/instrument_derived_event_wire_v1.h"
#include "l2flow/ipc/realtime_certified_wire_v1.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <variant>

#include <sys/mman.h>
#include <unistd.h>

namespace {

namespace ipc = l2flow::ipc;
namespace market = l2flow::market;

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

class Descriptor final {
public:
    Descriptor() noexcept = default;
    explicit Descriptor(int value) noexcept : value_(value) {}
    Descriptor(const Descriptor&) = delete;
    Descriptor& operator=(const Descriptor&) = delete;
    ~Descriptor() {
        if (value_ >= 0) {
            static_cast<void>(::close(value_));
        }
    }

    [[nodiscard]] int get() const noexcept { return value_; }
    [[nodiscard]] int* output() noexcept {
        if (value_ >= 0) {
            static_cast<void>(::close(value_));
        }
        value_ = -1;
        return &value_;
    }

private:
    int value_ = -1;
};

class TickStatusFixture final {
public:
    TickStatusFixture() = default;
    TickStatusFixture(const TickStatusFixture&) = delete;
    TickStatusFixture& operator=(const TickStatusFixture&) = delete;
    ~TickStatusFixture() {
        if (read_only_fd_ >= 0) {
            static_cast<void>(::close(read_only_fd_));
        }
        if (mapping_ != MAP_FAILED) {
            static_cast<void>(::munmap(mapping_, mapping_bytes_));
        }
        if (writable_fd_ >= 0) {
            static_cast<void>(::close(writable_fd_));
        }
    }

    [[nodiscard]] bool Create(
        std::uint64_t canonical_frontier,
        ipc::RealtimeCertifiedStateV1 state =
            ipc::RealtimeCertifiedStateV1::kStopped) noexcept {
        constexpr std::uint32_t latest_capacity = 1U;
        constexpr std::uint32_t ring_capacity = 1U;
        constexpr std::uint32_t channel_capacity = 1U;
        mapping_bytes_ =
            ipc::kRealtimeCertifiedHeaderBytesV1 +
            latest_capacity *
                ipc::kRealtimeCertifiedTickSlotBytesV1 +
            ring_capacity *
                ipc::kRealtimeCertifiedTickSlotBytesV1 +
            channel_capacity *
                ipc::kRealtimeCertifiedChannelStateBytesV1;
        writable_fd_ = ::memfd_create(
            "l2flow-event-journal-tick-fixture",
            MFD_CLOEXEC | MFD_ALLOW_SEALING);
        if (writable_fd_ < 0 ||
            ::ftruncate(
                writable_fd_,
                static_cast<off_t>(mapping_bytes_)) != 0) {
            return false;
        }
        mapping_ = ::mmap(
            nullptr,
            mapping_bytes_,
            PROT_READ | PROT_WRITE,
            MAP_SHARED,
            writable_fd_,
            0);
        if (mapping_ == MAP_FAILED) {
            return false;
        }
        std::memset(mapping_, 0, mapping_bytes_);
        header_ = std::construct_at(
            static_cast<ipc::RealtimeCertifiedHeaderV1*>(
                mapping_));
        header_->magic = ipc::kRealtimeCertifiedShmMagicV1;
        header_->abi_major =
            ipc::kRealtimeCertifiedWireMajorV1;
        header_->abi_minor =
            ipc::kRealtimeCertifiedWireMinorV1;
        header_->header_bytes =
            ipc::kRealtimeCertifiedHeaderBytesV1;
        header_->endian_marker =
            ipc::kRealtimeCertifiedLittleEndianMarkerV1;
        header_->total_mapping_bytes = mapping_bytes_;
        header_->run_id[0U] = 0xA5U;
        header_->session_epoch = 7U;
        header_->trade_date = 20260730U;
        header_->latest_offset =
            ipc::kRealtimeCertifiedHeaderBytesV1;
        header_->certified_ring_offset =
            header_->latest_offset +
            latest_capacity *
                ipc::kRealtimeCertifiedTickSlotBytesV1;
        header_->channel_state_offset =
            header_->certified_ring_offset +
            ring_capacity *
                ipc::kRealtimeCertifiedTickSlotBytesV1;
        header_->latest_capacity = latest_capacity;
        header_->certified_ring_capacity = ring_capacity;
        header_->channel_state_capacity = channel_capacity;
        header_->status_publish_tag = 2U;
        header_->heartbeat_monotonic_ns = 1U;
        header_->canonical_apply_frontier =
            canonical_frontier;
        header_->correction_epoch = 1U;
        header_->observed_native_message_count =
            canonical_frontier;
        header_->certified_tick_count = canonical_frontier;
        header_->aggregate_state =
            static_cast<std::uint32_t>(state);
        if (state ==
            ipc::RealtimeCertifiedStateV1::kContiguous) {
            header_->channel_state_count = 1U;
        } else if (
            state ==
            ipc::RealtimeCertifiedStateV1::kFrozenConflict) {
            header_->conflicting_duplicate_count = 1U;
        } else if (
            state ==
            ipc::RealtimeCertifiedStateV1::kFrozenResource) {
            header_->resource_exhaustion_count = 1U;
        }
        if (!ipc::RealtimeCertifiedHeaderCanonicalV1(*header_)) {
            return false;
        }

        const std::string descriptor_path =
            "/proc/self/fd/" + std::to_string(writable_fd_);
        read_only_fd_ =
            ::open(descriptor_path.c_str(), O_RDONLY | O_CLOEXEC);
        if (read_only_fd_ < 0) {
            return false;
        }
        constexpr int seals =
            F_SEAL_GROW | F_SEAL_SHRINK |
            F_SEAL_FUTURE_WRITE | F_SEAL_SEAL;
        return ::fcntl(writable_fd_, F_ADD_SEALS, seals) == 0;
    }

    [[nodiscard]] int read_only_fd() const noexcept {
        return read_only_fd_;
    }

    [[nodiscard]] ipc::RealtimeCertifiedExpectedSessionV1
    expected_session() const noexcept {
        ipc::RealtimeCertifiedExpectedSessionV1 result{};
        if (header_ != nullptr) {
            result.run_id = header_->run_id;
            result.session_epoch = header_->session_epoch;
            result.trade_date = header_->trade_date;
        }
        return result;
    }

private:
    int writable_fd_ = -1;
    int read_only_fd_ = -1;
    void* mapping_ = MAP_FAILED;
    std::size_t mapping_bytes_ = 0U;
    ipc::RealtimeCertifiedHeaderV1* header_ = nullptr;
};

[[nodiscard]] ipc::CertifiedOrderEventJournalConfigV1
JournalConfig(
    const ipc::RealtimeCertifiedExpectedSessionV1& session,
    std::uint64_t capacity,
    std::uint64_t commit_chunk = 4096U) noexcept {
    ipc::CertifiedOrderEventJournalConfigV1 result{};
    std::memcpy(
        result.run_id.data(),
        session.run_id.data(),
        session.run_id.size());
    result.session_epoch = session.session_epoch;
    result.trade_date = session.trade_date;
    result.event_capacity = capacity;
    const std::uint64_t logical =
        ipc::kCertifiedOrderEventHeaderBytesV1 +
        capacity * ipc::kCertifiedOrderEventSlotBytesV1;
    result.maximum_mapping_bytes =
        (logical + 4095U) & ~std::uint64_t{4095U};
    result.lazy_commit_chunk_bytes = commit_chunk;
    return result;
}

[[nodiscard]] ipc::InstrumentDerivedEventV1 ShanghaiTrade(
    std::uint64_t sequence) noexcept {
    market::ShanghaiTradeEventV1 trade{};
    trade.trade_date = 20260730U;
    trade.instrument_id = 1U;
    trade.channel = 1;
    trade.source_anchor.native_event_sequence =
        static_cast<std::int64_t>(sequence);
    trade.source_anchor.source_sequence = sequence;
    trade.source_anchor.ingress_sequence = sequence;
    trade.source_anchor.tick_stream_sequence = sequence;
    trade.buy_order_id =
        static_cast<std::int64_t>(10'000U + sequence);
    trade.sell_order_id =
        static_cast<std::int64_t>(20'000U + sequence);
    trade.price_p6 = 12'345'000;
    trade.quantity = 100;
    ipc::InstrumentDerivedEventV1 result{};
    result.derived_event_sequence = sequence;
    result.source_tick_event_ordinal = 0U;
    result.source_tick_event_ordinal_valid = true;
    result.payload = trade;
    return result;
}

[[nodiscard]] ipc::InstrumentDerivedEventV1 ShenzhenTrade(
    std::uint64_t sequence) noexcept {
    market::ShenzhenTradeEventV1 trade{};
    trade.trade_date = 20260730U;
    trade.instrument_id = 1U;
    // ChannelNo zero is permitted by the documented Shenzhen source ABI.
    trade.channel = 0U;
    trade.source_anchor.native_event_sequence =
        static_cast<std::int64_t>(sequence);
    trade.source_anchor.source_sequence = sequence;
    trade.source_anchor.ingress_sequence = sequence;
    trade.source_anchor.tick_stream_sequence = sequence;
    trade.buy_order_id =
        static_cast<std::int64_t>(10'000U + sequence);
    trade.sell_order_id =
        static_cast<std::int64_t>(20'000U + sequence);
    trade.price_p6 = 12'345'000;
    trade.quantity = 100;
    ipc::InstrumentDerivedEventV1 result{};
    result.derived_event_sequence = sequence;
    result.source_tick_event_ordinal = 0U;
    result.source_tick_event_ordinal_valid = true;
    result.payload = trade;
    return result;
}

[[nodiscard]] bool OpenCombined(
    const TickStatusFixture& tick,
    const std::shared_ptr<
        ipc::CertifiedOrderEventJournalProducerV1>& journal,
    ipc::CertifiedOrderEventCoverageRequirementV1 coverage_requirement,
    std::unique_ptr<ipc::CertifiedOrderEventReaderV1>* output) {
    Descriptor event_fd;
    if (journal == nullptr ||
        !journal->DuplicateReadOnlyDescriptor(event_fd.output())) {
        return false;
    }
    return ipc::CertifiedOrderEventReaderV1::
               OpenDescriptorsForTest(
                   tick.read_only_fd(),
                   event_fd.get(),
                   tick.expected_session(),
                   coverage_requirement,
                   output) ==
               ipc::CertifiedOrderEventReaderOpenErrorV1::kNone &&
           *output != nullptr;
}

bool TestLazyCommitAndCoherentCut() {
    bool ok = true;
    TickStatusFixture tick;
    ok &= Expect(
        tick.Create(1U),
        "create Tick frontier fixture");
    if (!ok) {
        return false;
    }

    std::shared_ptr<
        ipc::CertifiedOrderEventJournalProducerV1>
        journal;
    int system_error = 0;
    const auto config =
        JournalConfig(tick.expected_session(), 8U);
    ok &= Expect(
        ipc::CertifiedOrderEventJournalProducerV1::Create(
            config, &journal, &system_error) ==
                ipc::CertifiedOrderEventJournalCreateErrorV1::
                    kNone &&
            journal != nullptr && system_error == 0,
        "create sparse append-only Event journal");
    if (journal == nullptr) {
        return false;
    }
    ok &= Expect(
        journal->session().total_mapping_bytes ==
                config.maximum_mapping_bytes &&
            journal->committed_mapping_bytes() ==
                ipc::kCertifiedOrderEventHeaderBytesV1,
        "full-day VAS is bounded while only header backing is committed");

    std::unique_ptr<ipc::CertifiedOrderEventReaderV1> reader;
    ok &= Expect(
        OpenCombined(
            tick,
            journal,
            ipc::CertifiedOrderEventCoverageRequirementV1::kFromOpen,
            &reader),
        "open combined Tick/Event descriptor reader");
    if (reader == nullptr) {
        return false;
    }

    ipc::CertifiedOrderEventStatusSnapshotV1 status{};
    ok &= Expect(
        reader->ReadStatus(&status) ==
                ipc::CertifiedOrderEventReadResultV1::kOk &&
            status.coverage_from_open() &&
            !status.startup_prefix_recovered(),
        "ordinary journal starts with from-open, non-recovered coverage");
    ok &= Expect(
        journal->MarkStartupPrefixRecovered() &&
            journal->MarkStartupPrefixRecovered() &&
            reader->ReadStatus(&status) ==
                ipc::CertifiedOrderEventReadResultV1::kOk &&
            status.coverage_from_open() &&
            status.startup_prefix_recovered(),
        "startup-prefix recovery coverage is monotonic and reader-visible");

    auto first = ShanghaiTrade(1U);
    ok &= Expect(
        journal->PublishCanonicalTick(
            1U, 0U, 0U, std::span(&first, 1U)) ==
            ipc::CertifiedOrderEventJournalPublishErrorV1::kNone,
        "publish first Event before Tick/Event coherent read");
    ok &= Expect(
        reader->ReadStatus(&status) ==
                ipc::CertifiedOrderEventReadResultV1::kOk &&
            status.tick.canonical_apply_frontier == 1U &&
            status.event_canonical_apply_frontier == 1U &&
            status.coherent_canonical_apply_frontier == 1U &&
            status.event_published_sequence == 1U &&
            status.committed_mapping_bytes >
                ipc::kCertifiedOrderEventHeaderBytesV1,
        "first Event and lazy backing are externally visible");
    ipc::CertifiedOrderEventEnvelopeV1 envelope{};
    ok &= Expect(
        reader->ReadEvent(1U, &envelope) ==
                ipc::CertifiedOrderEventReadResultV1::kOk &&
            envelope.canonical_apply_sequence == 1U &&
            envelope.event.derived_event_sequence == 1U &&
            envelope.event.record_schema_version ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_ROW_SCHEMA_V2 &&
            envelope.event.reserved0 == 0U &&
            envelope.event.reserved1[0U] ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_SOURCE_TICK_ORDINAL_VALID_V2 &&
            envelope.event.reserved1[1U] == 0U &&
            envelope.event.reserved1[2U] == 0U,
        "read first coherent Event row with source-tick identity");

    auto second = ShanghaiTrade(2U);
    ok &= Expect(
        journal->PublishCanonicalTick(
            2U, 0U, 0U, std::span(&second, 1U)) ==
            ipc::CertifiedOrderEventJournalPublishErrorV1::kNone,
        "physically publish Event ahead of Tick frontier");
    ok &= Expect(
        reader->ReadStatus(&status) ==
                ipc::CertifiedOrderEventReadResultV1::kOk &&
            status.event_canonical_apply_frontier == 2U &&
            status.tick.canonical_apply_frontier == 1U &&
            status.coherent_canonical_apply_frontier == 1U,
        "consumer-visible cut is min(Tick, Event)");
    envelope = {};
    ok &= Expect(
        reader->ReadEvent(2U, &envelope) ==
            ipc::CertifiedOrderEventReadResultV1::
                kNotYetPublished,
        "physically ahead Event row is hidden");
    std::array<ipc::CertifiedOrderEventEnvelopeV1, 4U> rows{};
    ipc::CertifiedOrderEventReadBatchResultV1 batch{};
    ok &= Expect(
        reader->Read(1U, rows, &batch) ==
                ipc::CertifiedOrderEventReadResultV1::kOk &&
            batch.written == 1U &&
            batch.next_event_sequence == 2U &&
            rows[0U].canonical_apply_sequence == 1U,
        "batch read stops exactly at coherent Tick/Event cut");
    return ok;
}

bool TestShenzhenChannelZero() {
    bool ok = true;
    TickStatusFixture tick;
    ok &= Expect(
        tick.Create(1U),
        "create Shenzhen Tick frontier fixture");
    if (!ok) {
        return false;
    }
    std::shared_ptr<
        ipc::CertifiedOrderEventJournalProducerV1>
        journal;
    auto config =
        JournalConfig(tick.expected_session(), 1U);
    config.maximum_mapping_bytes = 0U;
    ok &= Expect(
        ipc::CertifiedOrderEventJournalProducerV1::Create(
            config, &journal) ==
                ipc::CertifiedOrderEventJournalCreateErrorV1::
                    kNone &&
            journal != nullptr,
        "create Shenzhen Event journal");
    if (journal == nullptr) {
        return false;
    }
    auto event = ShenzhenTrade(1U);
    ok &= Expect(
        journal->PublishCanonicalTick(
            1U, 0U, 0U, std::span(&event, 1U)) ==
            ipc::CertifiedOrderEventJournalPublishErrorV1::kNone,
        "publish documented Shenzhen channel zero");
    std::unique_ptr<ipc::CertifiedOrderEventReaderV1> reader;
    ok &= Expect(
        OpenCombined(
            tick,
            journal,
            ipc::CertifiedOrderEventCoverageRequirementV1::kFromOpen,
            &reader),
        "open Shenzhen combined reader");
    ipc::CertifiedOrderEventEnvelopeV1 envelope{};
    ok &= Expect(
        reader != nullptr &&
            reader->ReadEvent(1U, &envelope) ==
                ipc::CertifiedOrderEventReadResultV1::kOk &&
            envelope.event.market ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHENZHEN_V1 &&
            envelope.event.channel == 0,
        "wire validator preserves Shenzhen channel-zero asymmetry");
    return ok;
}

bool TestRejectsNonCanonicalSourceTickIdentity() {
    bool ok = true;
    auto source_free_trade = ShanghaiTrade(1U);
    auto* const trade = std::get_if<market::ShanghaiTradeEventV1>(
        &source_free_trade.payload);
    if (trade != nullptr) {
        trade->source_anchor.tick_stream_sequence = 0U;
    }
    source_free_trade.source_tick_event_ordinal_valid = false;
    l2flow_instrument_derived_event_row_v1 projected{};
    ok &= Expect(
        trade != nullptr &&
            !ipc::ProjectInstrumentDerivedEventWireV1(
                source_free_trade, &projected) &&
            projected.record_schema_version == 0U,
        "only a source-free Finalize row may omit source-tick identity");

    TickStatusFixture tick;
    ok &= Expect(
        tick.Create(0U),
        "create identity-validation Tick frontier fixture");
    if (!ok) {
        return false;
    }
    std::shared_ptr<ipc::CertifiedOrderEventJournalProducerV1> journal;
    ok &= Expect(
        ipc::CertifiedOrderEventJournalProducerV1::Create(
            JournalConfig(tick.expected_session(), 4U), &journal) ==
                ipc::CertifiedOrderEventJournalCreateErrorV1::kNone &&
            journal != nullptr,
        "create identity-validation Event journal");
    if (journal == nullptr) {
        return false;
    }

    auto invalid = ShanghaiTrade(1U);
    invalid.source_tick_event_ordinal_valid = false;
    ok &= Expect(
        journal->PublishCanonicalTick(
            1U, 0U, 0U, std::span(&invalid, 1U)) ==
            ipc::CertifiedOrderEventJournalPublishErrorV1::
                kProjectionError &&
            journal->published_event_sequence() == 0U &&
            journal->failed(),
        "CERT rejects a source-backed Event without a valid ordinal "
        "before publication");

    std::shared_ptr<ipc::CertifiedOrderEventJournalProducerV1>
        second_journal;
    ok &= Expect(
        ipc::CertifiedOrderEventJournalProducerV1::Create(
            JournalConfig(tick.expected_session(), 4U),
            &second_journal) ==
                ipc::CertifiedOrderEventJournalCreateErrorV1::kNone &&
            second_journal != nullptr,
        "create second identity-validation Event journal");
    if (second_journal == nullptr) {
        return false;
    }
    invalid = ShanghaiTrade(1U);
    invalid.source_tick_event_ordinal = 1U;
    ok &= Expect(
        second_journal->PublishCanonicalTick(
            1U, 0U, 0U, std::span(&invalid, 1U)) ==
            ipc::CertifiedOrderEventJournalPublishErrorV1::
                kProjectionError &&
            second_journal->published_event_sequence() == 0U &&
            second_journal->failed(),
        "CERT rejects an ordinal that is not the core output-vector index "
        "before publication");
    return ok;
}

bool TestConcurrentPublicationNeverTears() {
    constexpr std::uint64_t event_count = 10'000U;
    bool ok = true;
    TickStatusFixture tick;
    ok &= Expect(
        tick.Create(event_count),
        "create concurrent Tick frontier fixture");
    if (!ok) {
        return false;
    }
    std::shared_ptr<
        ipc::CertifiedOrderEventJournalProducerV1>
        journal;
    const auto config = JournalConfig(
        tick.expected_session(), event_count, 64U * 1024U);
    ok &= Expect(
        ipc::CertifiedOrderEventJournalProducerV1::Create(
            config, &journal) ==
                ipc::CertifiedOrderEventJournalCreateErrorV1::
                    kNone &&
            journal != nullptr,
        "create concurrent Event journal");
    if (journal == nullptr) {
        return false;
    }
    std::unique_ptr<ipc::CertifiedOrderEventReaderV1> reader;
    ok &= Expect(
        OpenCombined(
            tick,
            journal,
            ipc::CertifiedOrderEventCoverageRequirementV1::kFromOpen,
            &reader),
        "open concurrent combined reader");
    if (reader == nullptr) {
        return false;
    }

    std::atomic<bool> writer_done{false};
    std::atomic<bool> writer_failed{false};
    std::thread writer([&] {
        for (std::uint64_t sequence = 1U;
             sequence <= event_count;
             ++sequence) {
            auto event = ShanghaiTrade(sequence);
            if (journal->PublishCanonicalTick(
                    sequence,
                    0U,
                    0U,
                    std::span(&event, 1U)) !=
                ipc::CertifiedOrderEventJournalPublishErrorV1::
                    kNone) {
                writer_failed.store(true, std::memory_order_release);
                break;
            }
        }
        writer_done.store(true, std::memory_order_release);
    });

    bool reader_corrupt = false;
    std::uint64_t next = 1U;
    while (!writer_done.load(std::memory_order_acquire) ||
           next <= event_count) {
        ipc::CertifiedOrderEventStatusSnapshotV1 status{};
        const auto status_result = reader->ReadStatus(&status);
        if (status_result ==
            ipc::CertifiedOrderEventReadResultV1::kInconsistent) {
            continue;
        }
        if (status_result !=
                ipc::CertifiedOrderEventReadResultV1::kOk ||
            status.event_generation !=
                status.event_canonical_apply_frontier ||
            status.event_published_sequence !=
                status.event_canonical_apply_frontier ||
            status.coherent_canonical_apply_frontier !=
                status.event_canonical_apply_frontier) {
            reader_corrupt = true;
            break;
        }
        while (next <= status.event_published_sequence) {
            ipc::CertifiedOrderEventEnvelopeV1 envelope{};
            const auto read_result =
                reader->ReadEvent(next, &envelope);
            if (read_result ==
                ipc::CertifiedOrderEventReadResultV1::
                    kInconsistent) {
                break;
            }
            if (read_result !=
                    ipc::CertifiedOrderEventReadResultV1::kOk ||
                envelope.canonical_apply_sequence != next ||
                envelope.event.derived_event_sequence != next ||
                envelope.event.native_event_sequence !=
                    static_cast<std::int64_t>(next)) {
                reader_corrupt = true;
                break;
            }
            ++next;
        }
        if (reader_corrupt) {
            break;
        }
        std::this_thread::yield();
    }
    writer.join();
    ok &= Expect(
        !writer_failed.load(std::memory_order_acquire),
        "concurrent writer completed every dense publication");
    ok &= Expect(
        !reader_corrupt,
        "concurrent reader never accepted a torn header or Event row");

    ipc::CertifiedOrderEventStatusSnapshotV1 final_status{};
    ipc::CertifiedOrderEventEnvelopeV1 final_event{};
    ok &= Expect(
        reader->ReadStatus(&final_status) ==
                ipc::CertifiedOrderEventReadResultV1::kOk &&
            final_status.event_canonical_apply_frontier ==
                event_count &&
            final_status.event_published_sequence == event_count &&
            reader->ReadEvent(event_count, &final_event) ==
                ipc::CertifiedOrderEventReadResultV1::kOk &&
            final_event.event.derived_event_sequence == event_count,
        "final stable Event prefix is complete after concurrent read");
    return ok;
}

bool TestProcessStartCoverageContract() {
    bool ok = true;
    TickStatusFixture tick;
    ok &= Expect(
        tick.Create(0U),
        "create empty Tick fixture for process-start coverage");
    if (!ok) {
        return false;
    }

    constexpr std::uint64_t coverage_start =
        1'785'834'365'123'456'789ULL;
    ipc::CertifiedOrderEventJournalConfigV1 config =
        JournalConfig(tick.expected_session(), 8U);
    config.temporal_coverage =
        ipc::CertifiedOrderEventTemporalCoverageV1::
            kFromProcessStart;
    config.coverage_start_unix_ns = coverage_start;
    std::shared_ptr<ipc::CertifiedOrderEventJournalProducerV1>
        journal;
    ok &= Expect(
        ipc::CertifiedOrderEventJournalProducerV1::Create(
            config, &journal) ==
                ipc::CertifiedOrderEventJournalCreateErrorV1::kNone &&
            journal != nullptr,
        "create explicit process-start Event journal");
    if (journal == nullptr) {
        return false;
    }
    const ipc::CertifiedOrderEventJournalSessionV1 provisional_session =
        journal->session();
    ok &= Expect(
        provisional_session.temporal_coverage ==
                ipc::CertifiedOrderEventTemporalCoverageV1::
                    kFromProcessStart &&
            provisional_session.coverage_start_unix_ns == coverage_start &&
            journal->coverage_flags() ==
                ipc::kCertifiedOrderEventCoverageFromProcessStartV1,
        "journal session exposes one process-start temporal contract");

    constexpr std::uint64_t finalized_coverage_start =
        coverage_start + 1'000U;
    ok &= Expect(
        !journal->FinalizeProcessStartCoverage(coverage_start - 1U) &&
            journal->coverage_start_unix_ns() == coverage_start,
        "process-start finalization cannot move the boundary backward");
    ok &= Expect(
        journal->FinalizeProcessStartCoverage(
            finalized_coverage_start) &&
            journal->session().coverage_start_unix_ns ==
                finalized_coverage_start &&
            journal->coverage_start_unix_ns() ==
                finalized_coverage_start,
        "process-start finalization atomically replaces the hidden boundary");
    ok &= Expect(
        !journal->FinalizeProcessStartCoverage(
            finalized_coverage_start + 1U) &&
            journal->coverage_start_unix_ns() ==
                finalized_coverage_start,
        "process-start finalization is one-shot");

    Descriptor event_fd;
    ok &= Expect(
        journal->DuplicateReadOnlyDescriptor(event_fd.output()),
        "duplicate process-start Event descriptor");
    std::unique_ptr<ipc::CertifiedOrderEventReaderV1> rejected;
    ok &= Expect(
        ipc::CertifiedOrderEventReaderV1::OpenDescriptorsForTest(
            tick.read_only_fd(),
            event_fd.get(),
            tick.expected_session(),
            ipc::CertifiedOrderEventCoverageRequirementV1::kFromOpen,
            &rejected) ==
                ipc::CertifiedOrderEventReaderOpenErrorV1::
                    kLayoutInvalid &&
            rejected == nullptr,
        "from-open reader rejects process-start Event coverage");

    std::unique_ptr<ipc::CertifiedOrderEventReaderV1> reader;
    ok &= Expect(
        OpenCombined(
            tick,
            journal,
            ipc::CertifiedOrderEventCoverageRequirementV1::
                kProcessStartPartial,
            &reader),
        "process-start reader explicitly accepts partial Event coverage");
    ipc::CertifiedOrderEventStatusSnapshotV1 status{};
    ok &= Expect(
        reader != nullptr &&
            reader->ReadStatus(&status) ==
                ipc::CertifiedOrderEventReadResultV1::kOk &&
            status.process_start_partial() &&
            !status.coverage_from_open() &&
            !status.startup_prefix_recovered() &&
            status.coverage_start_unix_ns ==
                finalized_coverage_start &&
            reader->coverage_start_unix_ns() ==
                finalized_coverage_start,
        "reader preserves process-start boundary without from-open claim");

    std::shared_ptr<ipc::CertifiedOrderEventJournalProducerV1> invalid;
    config.coverage_start_unix_ns = 0U;
    ok &= Expect(
        ipc::CertifiedOrderEventJournalProducerV1::Create(
            config, &invalid) ==
            ipc::CertifiedOrderEventJournalCreateErrorV1::
                kInvalidConfiguration,
        "process-start coverage requires a nonzero UTC boundary");
    config = JournalConfig(tick.expected_session(), 8U);
    config.coverage_start_unix_ns = coverage_start;
    ok &= Expect(
        ipc::CertifiedOrderEventJournalProducerV1::Create(
            config, &invalid) ==
            ipc::CertifiedOrderEventJournalCreateErrorV1::
                kInvalidConfiguration,
        "from-open coverage rejects a process-start boundary");
    return ok;
}

bool TestFullCapacityNaturalTailLifecycle() {
    struct Case final {
        ipc::RealtimeCertifiedStateV1 state;
        ipc::CertifiedOrderEventReadResultV1 expected;
        std::string_view label;
    };
    constexpr std::array<Case, 3U> cases{{
        {ipc::RealtimeCertifiedStateV1::kContiguous,
         ipc::CertifiedOrderEventReadResultV1::kNotYetPublished,
         "ACTIVE full-capacity tail remains temporarily unavailable"},
        {ipc::RealtimeCertifiedStateV1::kStopped,
         ipc::CertifiedOrderEventReadResultV1::kEndOfStream,
         "clean STOPPED full-capacity tail reports end-of-stream"},
        {ipc::RealtimeCertifiedStateV1::kFrozenResource,
         ipc::CertifiedOrderEventReadResultV1::kProducerFailed,
         "frozen full-capacity tail fails closed"},
    }};

    bool ok = true;
    for (const auto& test : cases) {
        TickStatusFixture tick;
        ok &= Expect(
            tick.Create(1U, test.state),
            "create exact-capacity Tick lifecycle fixture");
        if (!ok) {
            return false;
        }
        std::shared_ptr<
            ipc::CertifiedOrderEventJournalProducerV1>
            journal;
        ok &= Expect(
            ipc::CertifiedOrderEventJournalProducerV1::Create(
                JournalConfig(tick.expected_session(), 2U),
                &journal) ==
                    ipc::CertifiedOrderEventJournalCreateErrorV1::
                        kNone &&
                journal != nullptr,
            "create exact-capacity Event journal");
        if (journal == nullptr) {
            return false;
        }
        std::array<ipc::InstrumentDerivedEventV1, 2U> events{
            ShanghaiTrade(1U), ShanghaiTrade(2U)};
        events[1U].source_tick_event_ordinal = 1U;
        ok &= Expect(
            journal->PublishCanonicalTick(
                1U, 0U, 0U, events) ==
                ipc::CertifiedOrderEventJournalPublishErrorV1::
                    kNone,
            "fill every Event journal slot at one coherent Tick cut");

        std::unique_ptr<ipc::CertifiedOrderEventReaderV1> reader;
        ok &= Expect(
            OpenCombined(
                tick,
                journal,
                ipc::CertifiedOrderEventCoverageRequirementV1::
                    kFromOpen,
                &reader),
            "open exact-capacity combined Event reader");
        if (reader == nullptr) {
            return false;
        }
        std::array<ipc::CertifiedOrderEventEnvelopeV1, 1U> rows{};
        ipc::CertifiedOrderEventReadBatchResultV1 batch{};
        ipc::CertifiedOrderEventEnvelopeV1 row{};
        const bool lifecycle_visible =
            reader->Read(3U, rows, &batch) == test.expected &&
            batch.written == 0U &&
            batch.next_event_sequence == 3U &&
            batch.status.tick.state == test.state &&
            batch.status.event_published_sequence == 2U &&
            batch.status.coherent_canonical_apply_frontier == 1U &&
            reader->ReadEvent(3U, &row) == test.expected;
        ok &= Expect(lifecycle_visible, test.label);

        batch = {};
        ok &= Expect(
            reader->Read(4U, rows, &batch) ==
                    ipc::CertifiedOrderEventReadResultV1::kOutOfRange &&
                batch.written == 0U &&
                batch.next_event_sequence == 4U,
            "natural Event tail exception is limited to capacity plus one");
    }
    return ok;
}

}  // namespace

int main() {
    bool ok = true;
    ok &= TestLazyCommitAndCoherentCut();
    ok &= TestShenzhenChannelZero();
    ok &= TestRejectsNonCanonicalSourceTickIdentity();
    ok &= TestConcurrentPublicationNeverTears();
    ok &= TestProcessStartCoverageContract();
    ok &= TestFullCapacityNaturalTailLifecycle();
    if (ok) {
        std::cout << "PASS: certified order Event journal v1\n";
        return 0;
    }
    return 1;
}
