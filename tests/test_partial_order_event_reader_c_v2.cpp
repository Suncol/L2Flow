#include "../apps/partial_event_stable_broker_v2.h"

#include "l2flow/ipc/partial_order_event_journal_v2.h"
#include "l2flow/ipc/partial_order_event_reader_c_v2.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

namespace {

namespace app = l2flow::apps;
namespace ipc = l2flow::ipc;
namespace market = l2flow::market;

constexpr std::uint32_t kTradeDate = 20260803U;
constexpr std::uint64_t kSessionEpoch = 17U;

class Test final {
public:
    void Expect(bool condition, std::string_view message) {
        if (!condition) {
            ++failures_;
            std::cerr << "FAIL: " << message << '\n';
        }
    }
    [[nodiscard]] int failures() const noexcept { return failures_; }

private:
    int failures_ = 0;
};

class Descriptor final {
public:
    Descriptor() noexcept = default;
    Descriptor(const Descriptor&) = delete;
    Descriptor& operator=(const Descriptor&) = delete;
    Descriptor(Descriptor&& other) noexcept
        : value_(std::exchange(other.value_, -1)) {}
    Descriptor& operator=(Descriptor&& other) noexcept {
        if (this != &other) {
            Reset(std::exchange(other.value_, -1));
        }
        return *this;
    }
    ~Descriptor() { Reset(); }
    [[nodiscard]] int get() const noexcept { return value_; }
    [[nodiscard]] int* output() noexcept {
        Reset();
        return &value_;
    }

private:
    void Reset(int value = -1) noexcept {
        if (value_ >= 0) {
            static_cast<void>(::close(value_));
        }
        value_ = value;
    }
    int value_ = -1;
};

class Reader final {
public:
    Reader() noexcept = default;
    Reader(const Reader&) = delete;
    Reader& operator=(const Reader&) = delete;
    ~Reader() { l2flow_partial_order_event_reader_close_v2(value_); }
    [[nodiscard]] l2flow_partial_order_event_reader_v2* get()
        const noexcept {
        return value_;
    }
    [[nodiscard]] l2flow_partial_order_event_reader_v2** output() noexcept {
        l2flow_partial_order_event_reader_close_v2(value_);
        value_ = nullptr;
        return &value_;
    }

private:
    l2flow_partial_order_event_reader_v2* value_ = nullptr;
};

class TempDirectory final {
public:
    TempDirectory() {
        char path[] = "/tmp/l2flow-partial-c-v2-XXXXXX";
        char* const created = ::mkdtemp(path);
        if (created != nullptr) {
            path_ = created;
        }
    }
    TempDirectory(const TempDirectory&) = delete;
    TempDirectory& operator=(const TempDirectory&) = delete;
    ~TempDirectory() {
        if (!path_.empty()) {
            std::error_code ignored;
            static_cast<void>(std::filesystem::remove(path_, ignored));
        }
    }
    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

[[nodiscard]] l2flow::common::Identity128 RunId() noexcept {
    l2flow::common::Identity128 result{};
    result[0U] = std::byte{0xC3};
    result[15U] = std::byte{0x3C};
    return result;
}

[[nodiscard]] ipc::InstrumentDerivedEventV1 Revision() noexcept {
    market::ShanghaiOrderRevisionEventV1 event{};
    event.operation = market::ShanghaiOrderDeltaOperationV1::kInsert;
    event.source_anchor.native_event_sequence = 1001;
    event.source_anchor.source_sequence = 2001U;
    event.source_anchor.ingress_sequence = 3001U;
    event.source_anchor.tick_stream_sequence = 1U;
    event.order.key.trade_date = kTradeDate;
    event.order.key.instrument_id = 11U;
    event.order.key.channel = 7;
    event.order.key.order_id = 99;
    event.order.side = market::SideV1::kBuy;
    event.order.price_p6 = 10'000'000;
    event.order.price_valid = true;
    event.order.original_quantity = 100;
    event.order.original_quantity_valid = true;
    event.order.remaining_quantity = 80;
    event.order.remaining_quantity_valid = true;
    event.order.add_seen = true;
    event.order.apply_to_book = true;
    event.order.revision = 1U;
    ipc::InstrumentDerivedEventV1 result{};
    result.derived_event_sequence = 1U;
    result.payload = event;
    result.source_tick_event_ordinal = 0U;
    result.source_tick_event_ordinal_valid = true;
    return result;
}

struct Generation final {
    std::shared_ptr<ipc::PartialOrderEventJournalProducerV2> producer;
    Descriptor descriptor;
};

[[nodiscard]] Generation MakeGeneration(
    std::uint64_t publication_generation,
    std::uint64_t correction_epoch,
    Test* test,
    bool stop_clean = false) {
    Generation result{};
    ipc::PartialOrderEventJournalConfigV2 config{};
    config.run_id = RunId();
    config.session_epoch = kSessionEpoch;
    config.trade_date = kTradeDate;
    config.publication_generation = publication_generation;
    config.correction_epoch = correction_epoch;
    config.coverage_start_unix_ns =
        1'785'700'800'000'000'000ULL;
    config.ordering_quality =
        ipc::PartialOrderEventOrderingQualityV2::
            kBoundedReorderedPartial;
    config.event_capacity = 16U;
    config.affected_channel_capacity = 4U;
    config.order_state_capacity = 8U;
    config.maximum_order_state_updates_per_commit = 2U;
    config.lazy_commit_chunk_bytes = 4096U;
    int system_error = 0;
    test->Expect(
        ipc::PartialOrderEventJournalProducerV2::Create(
            config, &result.producer, &system_error) ==
                ipc::PartialOrderEventJournalCreateErrorV2::kNone &&
            result.producer != nullptr && system_error == 0,
        "create partial journal generation");
    if (result.producer == nullptr) {
        return result;
    }
    ipc::PartialOrderEventChannelHealthV2 channel{};
    channel.channel = 0;
    channel.expected_native_sequence = 101;
    channel.contiguous_native_sequence = 100;
    channel.highest_observed_native_sequence = 102;
    channel.oldest_missing_native_sequence = 101;
    channel.pending_count = 1U;
    channel.oldest_gap_age_ns = 50U;
    channel.market =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHENZHEN_V1;
    channel.flags =
        ipc::kPartialOrderEventChannelOriginEstablishedV2 |
        ipc::kPartialOrderEventChannelAffectedV2 |
        ipc::kPartialOrderEventChannelStaleV2;
    channel.state = ipc::PartialOrderEventServiceStateV2::kReordering;
    const auto event = Revision();
    ipc::PartialOrderEventStatusUpdateV2 status{};
    status.captured_source_frontier = 2U;
    status.state = ipc::PartialOrderEventServiceStateV2::kReordering;
    status.stale = true;
    status.reorder_high_water = 2U;
    test->Expect(
        result.producer->PublishCanonicalTick(
            1U,
            status,
            std::span{&event, std::size_t{1U}},
            std::span{&channel, std::size_t{1U}}) ==
            ipc::PartialOrderEventJournalPublishErrorV2::kNone,
        "publish one event, state row, and affected channel");
    if (stop_clean) {
        ipc::PartialOrderEventStatusUpdateV2 stopped{};
        stopped.captured_source_frontier = 2U;
        stopped.reorder_high_water = 2U;
        stopped.state =
            ipc::PartialOrderEventServiceStateV2::kStoppedClean;
        stopped.stale = false;
        test->Expect(
            result.producer->PublishStatus(stopped) ==
                ipc::PartialOrderEventJournalPublishErrorV2::kNone,
            "publish stopped-clean journal cut");
    }
    test->Expect(
        result.producer->DuplicateReadOnlyDescriptor(
            result.descriptor.output(), &system_error) &&
            result.descriptor.get() >= 0 && system_error == 0,
        "duplicate generation descriptor");
    return result;
}

[[nodiscard]] l2flow_partial_order_event_expected_session_v2 Expected()
    noexcept {
    l2flow_partial_order_event_expected_session_v2 result{};
    const auto run_id = RunId();
    std::memcpy(result.run_id, run_id.data(), sizeof(result.run_id));
    result.session_epoch = kSessionEpoch;
    result.trade_date = kTradeDate;
    return result;
}

[[nodiscard]] bool SameCheckpoint(
    const l2flow_partial_order_event_checkpoint_v2& left,
    const l2flow_partial_order_event_checkpoint_v2& right) noexcept {
    return std::memcmp(left.run_id, right.run_id, sizeof(left.run_id)) == 0 &&
           left.session_epoch == right.session_epoch &&
           left.publication_generation == right.publication_generation &&
           left.correction_epoch == right.correction_epoch &&
           left.next_event_sequence == right.next_event_sequence &&
           left.next_order_state_physical_slot ==
               right.next_order_state_physical_slot &&
           left.trade_date == right.trade_date &&
           left.reserved == right.reserved;
}

void ExpectLegacyProtocolDiagnostic(
    const std::filesystem::path& socket_path,
    Test* test) {
    const int descriptor = ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    if (descriptor < 0) {
        test->Expect(false, "create legacy protocol probe socket");
        return;
    }
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    const std::string path = socket_path.string();
    std::memcpy(address.sun_path, path.c_str(), path.size() + 1U);
    const auto address_bytes =
        offsetof(sockaddr_un, sun_path) + path.size() + 1U;
    const bool connected =
        address_bytes <=
            static_cast<std::size_t>(
                std::numeric_limits<socklen_t>::max()) &&
        ::connect(
            descriptor,
            reinterpret_cast<const sockaddr*>(&address),
            static_cast<socklen_t>(address_bytes)) == 0;
    ipc::PartialEventBrokerRequestV2 legacy{};
    legacy.magic[7U] = static_cast<std::uint8_t>('1');
    const ssize_t sent =
        connected
            ? ::send(descriptor, &legacy, sizeof(legacy), MSG_NOSIGNAL)
            : -1;
    ipc::PartialEventBrokerResponseV2 response{};
    const ssize_t received =
        sent == static_cast<ssize_t>(sizeof(legacy))
            ? ::recv(descriptor, &response, sizeof(response), 0)
            : -1;
    static_cast<void>(::close(descriptor));
    test->Expect(
        connected && sent == static_cast<ssize_t>(sizeof(legacy)) &&
            received == static_cast<ssize_t>(sizeof(response)) &&
            response.result ==
                ipc::PartialEventBrokerResultV2::kProtocolError,
        "legacy/wrong-major request receives explicit protocol_error");
}

void TestCAbi(Test* test) {
    TempDirectory directory;
    Generation first = MakeGeneration(1U, 1U, test);
    Generation second = MakeGeneration(2U, 1U, test);
    Generation corrected = MakeGeneration(3U, 2U, test, true);
    if (directory.path().empty() || first.producer == nullptr ||
        second.producer == nullptr || corrected.producer == nullptr) {
        return;
    }
    const auto socket_path = directory.path() / "events.sock";
    app::PartialEventStableBrokerConfigV2 config{};
    config.public_socket_path = socket_path;
    config.expected_run_id = RunId();
    config.expected_session_epoch = kSessionEpoch;
    config.expected_trade_date = kTradeDate;
    config.maximum_mapping_bytes =
        first.producer->session().total_mapping_bytes;
    config.allowed_uid = ::getuid();
    config.request_timeout = std::chrono::milliseconds(1000);
    std::unique_ptr<app::PartialEventStableBrokerV2> broker;
    int system_error = 0;
    test->Expect(
        app::PartialEventStableBrokerV2::Create(
            config, &broker, &system_error) ==
                app::PartialEventStableBrokerCreateErrorV2::kNone &&
            broker != nullptr && system_error == 0,
        "create stable broker");
    if (broker == nullptr) {
        return;
    }
    test->Expect(
        broker->AdoptGeneration(
            first.descriptor.get(), first.producer->session()) ==
            ipc::PartialEventBrokerResultV2::kOk,
        "adopt first generation");
    ExpectLegacyProtocolDiagnostic(socket_path, test);

    const auto expected = Expected();
    Reader reader;
    test->Expect(
        l2flow_partial_order_event_reader_open_v2(
            socket_path.c_str(),
            &expected,
            nullptr,
            1000U,
            reader.output(),
            &system_error) == L2FLOW_PARTIAL_ORDER_EVENT_OPEN_OK_V2 &&
            reader.get() != nullptr && system_error == 0,
        "C ABI connects to broker and receives SCM_RIGHTS descriptor");
    if (reader.get() == nullptr) {
        return;
    }

    l2flow_partial_order_event_session_v2 session{};
    test->Expect(
        l2flow_partial_order_event_reader_session_v2(
            reader.get(), &session) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2 &&
            session.publication_generation == 1U &&
            session.correction_epoch == 1U &&
            session.temporal_coverage ==
                L2FLOW_PARTIAL_ORDER_EVENT_COVERAGE_PROCESS_START_V2 &&
            session.ordering_quality ==
                L2FLOW_PARTIAL_ORDER_EVENT_ORDERING_BOUNDED_REORDERED_PARTIAL_V2 &&
            session.broker_state ==
                L2FLOW_PARTIAL_ORDER_EVENT_BROKER_READY_V2,
        "session is explicitly partial process-start and broker-pinned");

    l2flow_partial_order_event_checkpoint_v2 checkpoint{};
    test->Expect(
        l2flow_partial_order_event_reader_checkpoint_v2(
            reader.get(), 1U, 0U, &checkpoint) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2,
        "create exact generation-aware cursor");
    l2flow_partial_order_event_envelope_v2 events[2U]{};
    l2flow_partial_order_event_read_batch_result_v2 read{};
    test->Expect(
        l2flow_partial_order_event_reader_read_v2(
            reader.get(), &checkpoint, events, 2U, &read) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2 &&
            read.records_written == 1U &&
            read.checkpoint.next_event_sequence == 2U &&
            events[0U].canonical_apply_sequence == 1U &&
            read.status.event_published_frontier == 1U,
        "append-only event cursor advances inside one coherent cut");

    l2flow_partial_order_event_channel_batch_result_v2 channels_query{};
    test->Expect(
        l2flow_partial_order_event_reader_affected_channels_v2(
            reader.get(), nullptr, 0U, &channels_query) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_OUTPUT_TOO_SMALL_V2 &&
            channels_query.required_capacity == 1U,
        "affected-channel query returns exact required capacity");
    l2flow_partial_order_event_channel_health_v2 channels[1U]{};
    l2flow_partial_order_event_channel_batch_result_v2 channels_result{};
    test->Expect(
        l2flow_partial_order_event_reader_affected_channels_v2(
            reader.get(), channels, 1U, &channels_result) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2 &&
            channels_result.records_written == 1U &&
            channels[0U].channel == 0 &&
            channels[0U].oldest_missing_native_sequence == 101,
        "affected-channel health preserves Shenzhen channel zero and gap");

    l2flow_partial_order_event_order_key_v2 key{};
    key.market = L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1;
    key.instrument_id = 11U;
    key.channel = 7;
    key.order_id = 99;
    l2flow_partial_order_event_order_state_v2 state{};
    l2flow_partial_order_event_status_v2 state_status{};
    test->Expect(
        l2flow_partial_order_event_reader_find_order_state_v2(
            reader.get(), &key, &state, &state_status) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2 &&
            state.canonical_apply_sequence == 1U &&
            state.order_revision.remaining_quantity == 80,
        "order-state lookup returns last-good revision");
    l2flow_partial_order_event_order_state_v2 states[2U]{};
    l2flow_partial_order_event_order_state_batch_result_v2 states_result{};
    test->Expect(
        l2flow_partial_order_event_reader_order_states_v2(
            reader.get(), &read.checkpoint, states, 2U, &states_result) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2 &&
            states_result.records_written == 1U &&
            states_result.checkpoint.next_order_state_physical_slot > 0U &&
            states_result.checkpoint.next_event_sequence ==
                read.checkpoint.next_event_sequence,
        "physical-slot enumeration returns visible order states");

    const pid_t worker = ::getpid();
    std::uint64_t worker_lease = 0U;
    test->Expect(
        broker->MarkWorkerRestarting(worker, &worker_lease) &&
            worker_lease != 0U,
        "mark exact worker lease restarting after reader attach");
    l2flow_partial_order_event_status_v2 restarting_status{};
    test->Expect(
        l2flow_partial_order_event_reader_status_v2(
            reader.get(), &restarting_status) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2 &&
            restarting_status.broker_state ==
                L2FLOW_PARTIAL_ORDER_EVENT_BROKER_RESTARTING_V2 &&
            restarting_status.broker_stale == 1U &&
            restarting_status.event_published_frontier == 1U,
        "attached reader observes restarting lifecycle without reconnecting");
    l2flow_partial_order_event_read_batch_result_v2 restarting_read{};
    l2flow_partial_order_event_envelope_v2 restarting_events[1U]{};
    test->Expect(
        l2flow_partial_order_event_reader_read_v2(
            reader.get(),
            &checkpoint,
            restarting_events,
            1U,
            &restarting_read) == L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2 &&
            restarting_read.records_written == 1U &&
            restarting_read.status.broker_state ==
                L2FLOW_PARTIAL_ORDER_EVENT_BROKER_RESTARTING_V2 &&
            restarting_read.status.broker_stale == 1U,
        "restarting worker preserves last-good Event History");
    l2flow_partial_order_event_order_state_v2 restarting_state{};
    l2flow_partial_order_event_status_v2 restarting_state_status{};
    test->Expect(
        l2flow_partial_order_event_reader_find_order_state_v2(
            reader.get(),
            &key,
            &restarting_state,
            &restarting_state_status) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2 &&
            restarting_state.order_revision.remaining_quantity == 80 &&
            restarting_state_status.broker_state ==
                L2FLOW_PARTIAL_ORDER_EVENT_BROKER_RESTARTING_V2 &&
            restarting_state_status.broker_stale == 1U,
        "restarting worker preserves last-good derived order state");

    broker->SignalWorkerFailed(worker, worker_lease);
    l2flow_partial_order_event_read_batch_result_v2
        asynchronously_failed_read{};
    l2flow_partial_order_event_envelope_v2
        asynchronously_failed_events[1U]{};
    test->Expect(
        l2flow_partial_order_event_reader_read_v2(
            reader.get(),
            &checkpoint,
            asynchronously_failed_events,
            1U,
            &asynchronously_failed_read) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2 &&
            asynchronously_failed_read.records_written == 1U &&
            asynchronously_failed_read.checkpoint.next_event_sequence == 2U &&
            asynchronously_failed_events[0U].canonical_apply_sequence ==
                1U &&
            asynchronously_failed_read.status.broker_state ==
                L2FLOW_PARTIAL_ORDER_EVENT_BROKER_STALE_V2 &&
            asynchronously_failed_read.status.broker_stale == 1U,
        "nonempty Event read observes current-lease async failure without hiding last-good rows");
    l2flow_partial_order_event_status_v2 failed_status{};
    test->Expect(
        l2flow_partial_order_event_reader_status_v2(
            reader.get(), &failed_status) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2 &&
            failed_status.broker_state ==
                L2FLOW_PARTIAL_ORDER_EVENT_BROKER_STALE_V2 &&
            failed_status.broker_stale == 1U &&
            failed_status.event_published_frontier == 1U,
        "async worker failure reaches an attached reader as stale");
    broker->MarkWorkerFailed(worker, worker_lease);
    l2flow_partial_order_event_order_state_v2 failed_state{};
    l2flow_partial_order_event_status_v2 failed_state_status{};
    test->Expect(
        l2flow_partial_order_event_reader_find_order_state_v2(
            reader.get(),
            &key,
            &failed_state,
            &failed_state_status) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2 &&
            failed_state.order_revision.remaining_quantity == 80 &&
            failed_state_status.broker_state ==
                L2FLOW_PARTIAL_ORDER_EVENT_BROKER_STALE_V2 &&
            failed_state_status.broker_stale == 1U,
        "finalized worker failure preserves last-good order state");

    Reader failed_attach_reader;
    test->Expect(
        l2flow_partial_order_event_reader_open_v2(
            socket_path.c_str(),
            &expected,
            nullptr,
            1000U,
            failed_attach_reader.output(),
            &system_error) == L2FLOW_PARTIAL_ORDER_EVENT_OPEN_OK_V2 &&
            failed_attach_reader.get() != nullptr && system_error == 0,
        "same stable socket accepts a new reader after worker failure");
    l2flow_partial_order_event_checkpoint_v2 failed_attach_checkpoint{};
    l2flow_partial_order_event_read_batch_result_v2 failed_attach_read{};
    l2flow_partial_order_event_envelope_v2 failed_attach_events[1U]{};
    test->Expect(
        failed_attach_reader.get() != nullptr &&
            l2flow_partial_order_event_reader_checkpoint_v2(
                failed_attach_reader.get(),
                1U,
                0U,
                &failed_attach_checkpoint) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2 &&
            l2flow_partial_order_event_reader_read_v2(
                failed_attach_reader.get(),
                &failed_attach_checkpoint,
                failed_attach_events,
                1U,
                &failed_attach_read) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2 &&
            failed_attach_read.records_written == 1U &&
            failed_attach_events[0U].canonical_apply_sequence == 1U &&
            failed_attach_read.status.broker_state ==
                L2FLOW_PARTIAL_ORDER_EVENT_BROKER_STALE_V2 &&
            failed_attach_read.status.broker_stale == 1U,
        "new reader obtains last-good Event History from the stale broker");
    l2flow_partial_order_event_order_state_v2 failed_attach_state{};
    l2flow_partial_order_event_status_v2 failed_attach_state_status{};
    test->Expect(
        failed_attach_reader.get() != nullptr &&
            l2flow_partial_order_event_reader_find_order_state_v2(
                failed_attach_reader.get(),
                &key,
                &failed_attach_state,
                &failed_attach_state_status) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2 &&
            failed_attach_state.canonical_apply_sequence == 1U &&
            failed_attach_state.order_revision.remaining_quantity == 80 &&
            failed_attach_state_status.broker_state ==
                L2FLOW_PARTIAL_ORDER_EVENT_BROKER_STALE_V2 &&
            failed_attach_state_status.broker_stale == 1U,
        "new reader obtains last-good derived order state after worker failure");

    test->Expect(
        broker->AdoptGeneration(
            second.descriptor.get(), second.producer->session()) ==
            ipc::PartialEventBrokerResultV2::kOk,
        "adopt prefix-compatible successor generation");

    l2flow_partial_order_event_session_v2 replaced_session{};
    test->Expect(
        l2flow_partial_order_event_reader_session_v2(
            reader.get(), &replaced_session) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_FULL_REPLACEMENT_REQUIRED_V2 &&
            replaced_session.publication_generation == 0U,
        "attached session rejects a replacement publication generation");
    l2flow_partial_order_event_status_v2 replaced_status{};
    test->Expect(
        l2flow_partial_order_event_reader_status_v2(
            reader.get(), &replaced_status) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_FULL_REPLACEMENT_REQUIRED_V2 &&
            replaced_status.publication_generation == 0U,
        "attached status rejects a replacement publication generation");
    l2flow_partial_order_event_checkpoint_v2 replaced_checkpoint{};
    test->Expect(
        l2flow_partial_order_event_reader_checkpoint_v2(
            reader.get(), 1U, 0U, &replaced_checkpoint) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_FULL_REPLACEMENT_REQUIRED_V2 &&
            replaced_checkpoint.publication_generation == 0U,
        "attached checkpoint rejects a replacement publication generation");
    constexpr std::uint64_t replacement_row_sentinel =
        std::numeric_limits<std::uint64_t>::max() - 7U;
    l2flow_partial_order_event_read_batch_result_v2 replaced_read{};
    l2flow_partial_order_event_envelope_v2 replaced_event[1U]{};
    replaced_event[0U].canonical_apply_sequence =
        replacement_row_sentinel;
    test->Expect(
        l2flow_partial_order_event_reader_read_v2(
            reader.get(),
            &checkpoint,
            replaced_event,
            1U,
            &replaced_read) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_FULL_REPLACEMENT_REQUIRED_V2 &&
            replaced_read.records_written == 0U &&
            replaced_event[0U].canonical_apply_sequence ==
                replacement_row_sentinel &&
            SameCheckpoint(replaced_read.checkpoint, checkpoint),
        "replacement rejects Event rows before copying or advancing the cursor");
    l2flow_partial_order_event_channel_batch_result_v2 replaced_channels{};
    l2flow_partial_order_event_channel_health_v2 replaced_channel[1U]{};
    replaced_channel[0U].commit_sequence = replacement_row_sentinel;
    test->Expect(
        l2flow_partial_order_event_reader_affected_channels_v2(
            reader.get(),
            replaced_channel,
            1U,
            &replaced_channels) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_FULL_REPLACEMENT_REQUIRED_V2 &&
            replaced_channels.records_written == 0U &&
            replaced_channel[0U].commit_sequence ==
                replacement_row_sentinel,
        "replacement rejects channel rows before copying old mapping data");
    l2flow_partial_order_event_order_state_v2 replaced_state{};
    test->Expect(
        l2flow_partial_order_event_reader_find_order_state_v2(
            reader.get(), &key, &replaced_state, nullptr) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_FULL_REPLACEMENT_REQUIRED_V2 &&
            replaced_state.canonical_apply_sequence == 0U,
        "attached order lookup rejects a replacement publication generation");
    l2flow_partial_order_event_order_state_batch_result_v2
        replaced_states{};
    l2flow_partial_order_event_order_state_v2 replaced_state_rows[1U]{};
    replaced_state_rows[0U].canonical_apply_sequence =
        replacement_row_sentinel;
    test->Expect(
        l2flow_partial_order_event_reader_order_states_v2(
            reader.get(),
            &checkpoint,
            replaced_state_rows,
            1U,
            &replaced_states) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_FULL_REPLACEMENT_REQUIRED_V2 &&
            replaced_states.records_written == 0U &&
            replaced_state_rows[0U].canonical_apply_sequence ==
                replacement_row_sentinel &&
            SameCheckpoint(replaced_states.checkpoint, checkpoint),
        "replacement rejects order-state rows before copying or advancing the cursor");
    Reader rejected_generation;
    test->Expect(
        l2flow_partial_order_event_reader_open_v2(
            socket_path.c_str(),
            &expected,
            &states_result.checkpoint,
            1000U,
            rejected_generation.output(),
            &system_error) ==
                L2FLOW_PARTIAL_ORDER_EVENT_OPEN_FULL_REPLACEMENT_REQUIRED_V2 &&
            rejected_generation.get() == nullptr,
        "saved cursor cannot silently cross publication generation");

    Reader second_reader;
    test->Expect(
        l2flow_partial_order_event_reader_open_v2(
            socket_path.c_str(),
            &expected,
            nullptr,
            1000U,
            second_reader.output(),
            &system_error) == L2FLOW_PARTIAL_ORDER_EVENT_OPEN_OK_V2,
        "attach current successor without an old cursor");
    l2flow_partial_order_event_checkpoint_v2 second_checkpoint{};
    test->Expect(
        second_reader.get() != nullptr &&
            l2flow_partial_order_event_reader_checkpoint_v2(
                second_reader.get(), 2U, 0U, &second_checkpoint) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2,
        "pin successor checkpoint");
    const auto correction_promotion = broker->PromoteCorrectedGeneration(
        corrected.descriptor.get(), corrected.producer->session());
    test->Expect(
        correction_promotion == ipc::PartialEventBrokerResultV2::kOk,
        "explicitly promote the next correction epoch");
    if (correction_promotion != ipc::PartialEventBrokerResultV2::kOk) {
        std::cerr << "correction promotion result="
                  << ipc::PartialEventBrokerResultNameV2(
                         correction_promotion)
                  << '\n';
    }
    l2flow_partial_order_event_status_v2 replaced_correction_status{};
    test->Expect(
        second_reader.get() != nullptr &&
            l2flow_partial_order_event_reader_status_v2(
                second_reader.get(), &replaced_correction_status) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_FULL_REPLACEMENT_REQUIRED_V2 &&
            replaced_correction_status.correction_epoch == 0U,
        "attached reader rejects a replacement correction epoch");
    Reader rejected_correction;
    test->Expect(
        l2flow_partial_order_event_reader_open_v2(
            socket_path.c_str(),
            &expected,
            &second_checkpoint,
            1000U,
            rejected_correction.output(),
            &system_error) ==
                L2FLOW_PARTIAL_ORDER_EVENT_OPEN_FULL_REPLACEMENT_REQUIRED_V2 &&
            rejected_correction.get() == nullptr,
        "saved cursor cannot silently cross correction epoch");

    test->Expect(
        std::string_view(l2flow_partial_order_event_open_error_name_v2(
            L2FLOW_PARTIAL_ORDER_EVENT_OPEN_BROKER_PROTOCOL_ERROR_V2)) ==
            "broker_protocol_error",
        "protocol mismatch has an explicit public diagnostic name");
    test->Expect(
        std::string_view(l2flow_partial_order_event_open_error_name_v2(
            L2FLOW_PARTIAL_ORDER_EVENT_OPEN_UNSUPPORTED_ORDERING_QUALITY_V2)) ==
            "unsupported_ordering_quality",
        "unsupported proof-valued quality has an explicit diagnostic");
}

void TestStoppedCleanHeartbeatExpiry(Test* test) {
    TempDirectory directory;
    Generation generation = MakeGeneration(1U, 1U, test, true);
    if (directory.path().empty() || generation.producer == nullptr) {
        return;
    }
    const auto socket_path = directory.path() / "clean-events.sock";
    app::PartialEventStableBrokerConfigV2 config{};
    config.public_socket_path = socket_path;
    config.expected_run_id = RunId();
    config.expected_session_epoch = kSessionEpoch;
    config.expected_trade_date = kTradeDate;
    config.maximum_mapping_bytes =
        generation.producer->session().total_mapping_bytes;
    config.allowed_uid = ::getuid();
    config.request_timeout = std::chrono::milliseconds(1000);
    config.lifecycle_heartbeat_interval = std::chrono::milliseconds(2);
    config.lifecycle_heartbeat_timeout = std::chrono::milliseconds(10);
    std::unique_ptr<app::PartialEventStableBrokerV2> broker;
    int system_error = 0;
    test->Expect(
        app::PartialEventStableBrokerV2::Create(
            config, &broker, &system_error) ==
                app::PartialEventStableBrokerCreateErrorV2::kNone &&
            broker != nullptr && system_error == 0,
        "create short-heartbeat broker for clean terminal state");
    if (broker == nullptr) {
        return;
    }
    test->Expect(
        broker->AdoptGeneration(
            generation.descriptor.get(), generation.producer->session()) ==
            ipc::PartialEventBrokerResultV2::kOk,
        "adopt stopped-clean generation");

    const auto expected = Expected();
    Reader reader;
    test->Expect(
        l2flow_partial_order_event_reader_open_v2(
            socket_path.c_str(),
            &expected,
            nullptr,
            1000U,
            reader.output(),
            &system_error) == L2FLOW_PARTIAL_ORDER_EVENT_OPEN_OK_V2 &&
            reader.get() != nullptr,
        "attach stopped-clean generation before broker shutdown");
    if (reader.get() == nullptr) {
        return;
    }
    l2flow_partial_order_event_checkpoint_v2 checkpoint{};
    test->Expect(
        l2flow_partial_order_event_reader_checkpoint_v2(
            reader.get(), 1U, 0U, &checkpoint) ==
            L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2,
        "create stopped-clean last-good cursor");

    broker->Stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    l2flow_partial_order_event_status_v2 status{};
    test->Expect(
        l2flow_partial_order_event_reader_status_v2(
            reader.get(), &status) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2 &&
            status.broker_state ==
                L2FLOW_PARTIAL_ORDER_EVENT_BROKER_STOPPED_CLEAN_V2 &&
            status.broker_stale == 0U &&
            status.service_state ==
                L2FLOW_PARTIAL_ORDER_EVENT_STATE_STOPPED_CLEAN_V2,
        "expired heartbeat preserves an explicit stopped-clean terminal state");
    l2flow_partial_order_event_envelope_v2 event[1U]{};
    l2flow_partial_order_event_read_batch_result_v2 read{};
    test->Expect(
        l2flow_partial_order_event_reader_read_v2(
            reader.get(), &checkpoint, event, 1U, &read) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2 &&
            read.records_written == 1U &&
            read.status.broker_state ==
                L2FLOW_PARTIAL_ORDER_EVENT_BROKER_STOPPED_CLEAN_V2 &&
            read.status.broker_stale == 0U,
        "stopped-clean mapping remains readable after heartbeat expiry");
}

}  // namespace

int main() {
    Test test;
    TestCAbi(&test);
    TestStoppedCleanHeartbeatExpiry(&test);
    if (test.failures() != 0) {
        return 1;
    }
    std::cout << "PASS: partial order event reader C ABI v2\n";
    return 0;
}
