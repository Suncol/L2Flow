#include "../apps/partial_event_stable_broker_v2.h"

#include "l2flow/ipc/partial_order_event_control_v2.h"
#include "l2flow/ipc/partial_order_event_reader_v2.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

namespace {

namespace app = l2flow::apps;
namespace ipc = l2flow::ipc;
namespace market = l2flow::market;

constexpr std::uint32_t kTradeDate = 20260803U;
constexpr std::uint64_t kSessionEpoch = 9U;

class Test final {
public:
    void Expect(bool condition, std::string_view message) {
        if (!condition) {
            ++failures_;
            std::cerr << "FAIL: " << message << '\n';
        }
    }
    [[nodiscard]] int failures() const noexcept {
        return failures_;
    }

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
    ~Descriptor() {
        Reset();
    }
    [[nodiscard]] int get() const noexcept {
        return value_;
    }
    [[nodiscard]] int* output() noexcept {
        Reset();
        return &value_;
    }
    void Reset(int value = -1) noexcept {
        if (value_ >= 0) {
            static_cast<void>(::close(value_));
        }
        value_ = value;
    }

private:
    int value_ = -1;
};

class TempDirectory final {
public:
    TempDirectory() {
        char path[] = "/tmp/l2flow-event-broker-v2-XXXXXX";
        char* created = ::mkdtemp(path);
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
    result[0U] = std::byte{0xA5};
    result[15U] = std::byte{0x5A};
    return result;
}

[[nodiscard]] ipc::PartialOrderEventExpectedSessionV2 Expected(
    const ipc::PartialOrderEventJournalSessionV2& session) noexcept {
    ipc::PartialOrderEventExpectedSessionV2 result{};
    result.run_id = session.run_id;
    result.session_epoch = session.session_epoch;
    result.trade_date = session.trade_date;
    result.publication_generation = session.publication_generation;
    result.correction_epoch = session.correction_epoch;
    return result;
}

[[nodiscard]] std::array<std::uint8_t, 16U> WireRunId() noexcept {
    std::array<std::uint8_t, 16U> result{};
    const auto run_id = RunId();
    for (std::size_t index = 0U; index < result.size(); ++index) {
        result[index] = std::to_integer<std::uint8_t>(run_id[index]);
    }
    return result;
}

[[nodiscard]] ipc::InstrumentDerivedEventV1 Revision(
    std::int64_t remaining_quantity) noexcept {
    market::ShanghaiOrderRevisionEventV1 event{};
    event.operation =
        market::ShanghaiOrderDeltaOperationV1::kInsert;
    event.source_anchor.native_event_sequence = 1001;
    event.source_anchor.source_sequence = 2001U;
    event.source_anchor.ingress_sequence = 3001U;
    event.source_anchor.tick_stream_sequence = 1U;
    event.order.key.trade_date = kTradeDate;
    event.order.key.instrument_id = 1U;
    event.order.key.channel = 7;
    event.order.key.order_id = 99;
    event.order.side = market::SideV1::kBuy;
    event.order.price_p6 = 10'000'000;
    event.order.price_valid = true;
    event.order.published_quantity = 100;
    event.order.published_quantity_valid = true;
    event.order.original_quantity = 100;
    event.order.original_quantity_valid = true;
    event.order.remaining_quantity = remaining_quantity;
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
    std::int64_t remaining_quantity,
    Test* test,
    bool publish_event = true,
    std::uint64_t captured_source_frontier = 1U) {
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
    const auto create_error =
        ipc::PartialOrderEventJournalProducerV2::Create(
            config, &result.producer, &system_error);
    test->Expect(
        create_error ==
                ipc::PartialOrderEventJournalCreateErrorV2::kNone &&
            result.producer != nullptr && system_error == 0,
        "create partial Event journal generation");
    if (result.producer == nullptr) {
        return result;
    }
    if (publish_event) {
        const ipc::InstrumentDerivedEventV1 event =
            Revision(remaining_quantity);
        ipc::PartialOrderEventStatusUpdateV2 status{};
        status.captured_source_frontier = captured_source_frontier;
        status.state = ipc::PartialOrderEventServiceStateV2::kContiguous;
        status.stale = false;
        test->Expect(
            result.producer->PublishCanonicalTick(
                1U, status, std::span{&event, std::size_t{1U}}) ==
                ipc::PartialOrderEventJournalPublishErrorV2::kNone,
            "publish one canonical Event row");
    }
    test->Expect(
        result.producer->DuplicateReadOnlyDescriptor(
            result.descriptor.output(), &system_error) &&
            result.descriptor.get() >= 0 && system_error == 0,
        "duplicate read-only generation descriptor");
    return result;
}

[[nodiscard]] bool AppendSecondCanonicalEvent(
    Generation* generation,
    std::int64_t remaining_quantity,
    Test* test,
    bool publish_event = true,
    std::uint64_t captured_source_frontier = 2U) {
    if (generation == nullptr || generation->producer == nullptr) {
        return false;
    }
    ipc::InstrumentDerivedEventV1 event = Revision(remaining_quantity);
    event.derived_event_sequence = 2U;
    auto& revision =
        std::get<market::ShanghaiOrderRevisionEventV1>(event.payload);
    revision.operation = market::ShanghaiOrderDeltaOperationV1::kUpdate;
    revision.source_anchor.native_event_sequence = 1002;
    revision.source_anchor.source_sequence = 2002U;
    revision.source_anchor.ingress_sequence = 3002U;
    revision.source_anchor.tick_stream_sequence = 2U;
    revision.order.last_anchor = revision.source_anchor;
    revision.order.revision = 2U;

    ipc::PartialOrderEventStatusUpdateV2 status{};
    status.captured_source_frontier = captured_source_frontier;
    status.state = ipc::PartialOrderEventServiceStateV2::kContiguous;
    status.stale = false;
    const std::span<const ipc::InstrumentDerivedEventV1> events =
        publish_event
            ? std::span<const ipc::InstrumentDerivedEventV1>(
                  &event, std::size_t{1U})
            : std::span<const ipc::InstrumentDerivedEventV1>{};
    const bool appended =
        generation->producer->PublishCanonicalTick(
            2U, status, events) ==
        ipc::PartialOrderEventJournalPublishErrorV2::kNone;
    test->Expect(appended, "publish second canonical input");
    return appended;
}

[[nodiscard]] bool PublishGenerationStatus(
    Generation* generation,
    ipc::PartialOrderEventServiceStateV2 state,
    bool stale,
    ipc::PartialOrderEventLastErrorV2 last_error,
    std::uint64_t reorder_high_water,
    std::span<const ipc::PartialOrderEventChannelHealthV2>
        affected_channels,
    std::string_view message,
    Test* test) {
    if (generation == nullptr || generation->producer == nullptr) {
        return false;
    }
    ipc::PartialOrderEventStatusUpdateV2 status{};
    status.captured_source_frontier = 1U;
    status.state = state;
    status.stale = stale;
    status.reorder_high_water = reorder_high_water;
    status.last_error = last_error;
    const bool published =
        generation->producer->PublishStatus(status, affected_channels) ==
        ipc::PartialOrderEventJournalPublishErrorV2::kNone;
    test->Expect(published, message);
    return published;
}

[[nodiscard]] ipc::PartialOrderEventChannelHealthV2 AffectedChannel(
    std::uint64_t pending_count) noexcept {
    ipc::PartialOrderEventChannelHealthV2 result{};
    result.channel = 7;
    result.expected_native_sequence = 2;
    result.contiguous_native_sequence = 1;
    result.highest_observed_native_sequence =
        pending_count == 0U ? 1 : 2;
    result.oldest_missing_native_sequence =
        pending_count == 0U ? 0 : 2;
    result.pending_count = pending_count;
    result.oldest_gap_age_ns = pending_count == 0U ? 0U : 1U;
    result.market =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1;
    result.flags =
        ipc::kPartialOrderEventChannelOriginEstablishedV2 |
        ipc::kPartialOrderEventChannelAffectedV2 |
        ipc::kPartialOrderEventChannelStaleV2;
    result.state = pending_count == 0U
                       ? ipc::PartialOrderEventServiceStateV2::
                             kCorrectionPending
                       : ipc::PartialOrderEventServiceStateV2::kCatchingUp;
    return result;
}

[[nodiscard]] std::unique_ptr<app::PartialEventStableBrokerV2>
CreateSameProcessBroker(
    const std::filesystem::path& socket_path,
    std::uint64_t maximum_mapping_bytes,
    Test* test) {
    app::PartialEventStableBrokerConfigV2 config{};
    config.public_socket_path = socket_path;
    config.expected_run_id = RunId();
    config.expected_session_epoch = kSessionEpoch;
    config.expected_trade_date = kTradeDate;
    config.maximum_mapping_bytes = maximum_mapping_bytes;
    config.allowed_uid = ::geteuid();
    std::unique_ptr<app::PartialEventStableBrokerV2> broker;
    int system_error = 0;
    const auto create_error = app::PartialEventStableBrokerV2::Create(
        config, &broker, &system_error);
    test->Expect(
        create_error ==
                app::PartialEventStableBrokerCreateErrorV2::kNone &&
            broker != nullptr && system_error == 0,
        "create same-process broker for generation continuity test");
    return broker;
}

[[nodiscard]] std::unique_ptr<ipc::PartialOrderEventReaderV2> OpenReader(
    int descriptor,
    const ipc::PartialOrderEventJournalSessionV2& session,
    Test* test) {
    ipc::PartialOrderEventExpectedSessionV2 expected{};
    expected.run_id = session.run_id;
    expected.session_epoch = session.session_epoch;
    expected.trade_date = session.trade_date;
    expected.publication_generation = session.publication_generation;
    expected.correction_epoch = session.correction_epoch;
    std::unique_ptr<ipc::PartialOrderEventReaderV2> reader;
    int system_error = 0;
    test->Expect(
        ipc::PartialOrderEventReaderV2::OpenDescriptor(
            descriptor, expected, &reader, &system_error) ==
                ipc::PartialOrderEventReaderOpenErrorV2::kNone &&
            reader != nullptr && system_error == 0,
        "open broker-delivered descriptor with exact generation identity");
    return reader;
}

void TestStableBrokerLifecycle(Test* test) {
    TempDirectory directory;
    test->Expect(!directory.path().empty(), "create broker test directory");
    if (directory.path().empty()) {
        return;
    }
    Generation first = MakeGeneration(1U, 1U, 100, test);
    Generation second = MakeGeneration(2U, 1U, 100, test);
    Generation conflict = MakeGeneration(3U, 1U, 99, test);
    Generation corrected = MakeGeneration(3U, 2U, 99, test);
    Generation unready_corrected = MakeGeneration(3U, 2U, 99, test);
    Generation correction_jump = MakeGeneration(4U, 4U, 99, test);
    Generation post_failure = MakeGeneration(4U, 2U, 99, test);
    if (first.producer == nullptr || second.producer == nullptr ||
        conflict.producer == nullptr || corrected.producer == nullptr ||
        unready_corrected.producer == nullptr ||
        correction_jump.producer == nullptr ||
        post_failure.producer == nullptr) {
        return;
    }
    ipc::PartialOrderEventStatusUpdateV2 unready_status{};
    unready_status.captured_source_frontier = 1U;
    unready_status.state =
        ipc::PartialOrderEventServiceStateV2::kCorrectionPending;
    unready_status.stale = true;
    test->Expect(
        unready_corrected.producer->PublishStatus(unready_status) ==
            ipc::PartialOrderEventJournalPublishErrorV2::kNone,
        "publish a stale correction candidate for promotion rejection");
    if (!AppendSecondCanonicalEvent(&second, 90, test) ||
        !AppendSecondCanonicalEvent(&conflict, 89, test)) {
        return;
    }

    const std::filesystem::path public_socket =
        directory.path() / "events.sock";
    const std::filesystem::path handoff_socket =
        directory.path() / "events.handoff.sock";
    app::PartialEventStableBrokerConfigV2 config{};
    config.public_socket_path = public_socket;
    config.worker_handoff_socket_path = handoff_socket;
    config.expected_run_id = RunId();
    config.expected_session_epoch = kSessionEpoch;
    config.expected_trade_date = kTradeDate;
    config.maximum_mapping_bytes =
        first.producer->session().total_mapping_bytes * 2U;
    config.allowed_uid = ::geteuid();
    config.request_timeout = std::chrono::milliseconds(1000);
    config.lifecycle_heartbeat_interval = std::chrono::milliseconds(10);
    config.lifecycle_heartbeat_timeout = std::chrono::milliseconds(100);
    std::unique_ptr<app::PartialEventStableBrokerV2> broker;
    int system_error = 0;
    const auto create_error = app::PartialEventStableBrokerV2::Create(
        config, &broker, &system_error);
    test->Expect(
        create_error ==
                app::PartialEventStableBrokerCreateErrorV2::kNone &&
            broker != nullptr && system_error == 0,
        "create stable public broker and private handoff listener");
    if (broker == nullptr) {
        std::cerr
            << "broker create error="
            << app::PartialEventStableBrokerCreateErrorNameV2(
                   create_error)
            << " errno=" << system_error << '\n';
    }
    if (broker == nullptr) {
        return;
    }

    int aliased_output_descriptor = 7;
    ipc::PartialEventBrokerResponseV2 aliased_response{};
    test->Expect(
        ipc::RequestPartialEventGenerationV2(
            public_socket,
            RunId(),
            kSessionEpoch,
            kTradeDate,
            0U,
            std::chrono::milliseconds(1000),
            &aliased_output_descriptor,
            &aliased_output_descriptor,
            &aliased_response,
            &system_error) ==
                ipc::PartialEventBrokerResultV2::kDescriptorRejected &&
            aliased_output_descriptor == -1,
        "client rejects aliased journal and lifecycle fd outputs");

    test->Expect(
        ::chmod(public_socket.c_str(), 0666) == 0,
        "make public socket deliberately unsafe for client rejection");
    Descriptor unsafe_socket_descriptor;
    Descriptor unsafe_socket_lifecycle_descriptor;
    ipc::PartialEventBrokerResponseV2 unsafe_socket_response{};
    test->Expect(
        ipc::RequestPartialEventGenerationV2(
            public_socket,
            RunId(),
            kSessionEpoch,
            kTradeDate,
            0U,
            std::chrono::milliseconds(1000),
            unsafe_socket_descriptor.output(),
            unsafe_socket_lifecycle_descriptor.output(),
            &unsafe_socket_response,
            &system_error) ==
                ipc::PartialEventBrokerResultV2::kPeerRejected &&
            unsafe_socket_descriptor.get() == -1 &&
            unsafe_socket_lifecycle_descriptor.get() == -1,
        "client rejects a group/other-accessible broker socket before request");
    test->Expect(
        ::chmod(public_socket.c_str(), S_IRUSR | S_IWUSR) == 0,
        "restore owner-only broker socket mode");

    Descriptor invalid_handoff_descriptor;
    invalid_handoff_descriptor.Reset(
        ::open("/dev/null", O_RDONLY | O_CLOEXEC));
    ipc::PartialEventHandoffResponseV2 invalid_handoff_response{};
    test->Expect(
        invalid_handoff_descriptor.get() >= 0 &&
            ipc::SubmitPartialEventGenerationV2(
                handoff_socket,
                invalid_handoff_descriptor.get(),
                first.producer->session(),
                std::chrono::milliseconds(1000),
                &invalid_handoff_response,
                &system_error) ==
                ipc::PartialEventBrokerResultV2::kDescriptorRejected,
        "handoff helper rejects a non-regular non-journal fd before connect");

    Descriptor client_descriptor;
    Descriptor client_lifecycle_descriptor;
    ipc::PartialEventBrokerResponseV2 response{};
    test->Expect(
        ipc::RequestPartialEventGenerationV2(
            public_socket,
            RunId(),
            kSessionEpoch,
            kTradeDate,
            0U,
            std::chrono::milliseconds(1000),
            client_descriptor.output(),
            client_lifecycle_descriptor.output(),
            &response,
            &system_error) ==
                ipc::PartialEventBrokerResultV2::kUnavailable &&
            client_descriptor.get() == -1 &&
            client_lifecycle_descriptor.get() == -1 &&
            response.broker_state ==
                ipc::PartialEventBrokerStateV2::kUnavailable &&
            response.broker_stale == 1U,
        "public socket explicitly returns unavailable before first generation");

    test->Expect(
        broker->PromoteCorrectedGeneration(
            first.descriptor.get(), first.producer->session()) ==
            ipc::PartialEventBrokerResultV2::kGenerationRejected,
        "correction promotion cannot establish the initial generation");
    test->Expect(
        broker->AdoptGeneration(
            first.descriptor.get(), first.producer->session()) ==
            ipc::PartialEventBrokerResultV2::kOk,
        "same-process adopt validates the first generation");
    test->Expect(
        ipc::RequestPartialEventGenerationV2(
            public_socket,
            RunId(),
            kSessionEpoch,
            kTradeDate,
            1U,
            std::chrono::milliseconds(1000),
            client_descriptor.output(),
            client_lifecycle_descriptor.output(),
            &response,
            &system_error) == ipc::PartialEventBrokerResultV2::kOk &&
            client_descriptor.get() >= 0 &&
            client_lifecycle_descriptor.get() >= 0 &&
            response.descriptor_count == 2U &&
            response.publication_generation == 1U &&
            response.correction_epoch == 1U &&
            response.broker_state ==
                ipc::PartialEventBrokerStateV2::kReady &&
            response.broker_stale == 0U,
        "public client receives the validated current generation");
    auto old_reader = OpenReader(
        client_descriptor.get(), first.producer->session(), test);
    struct stat lifecycle_descriptor_status {};
    const int lifecycle_access_flags =
        ::fcntl(client_lifecycle_descriptor.get(), F_GETFL);
    const int lifecycle_seals =
        ::fcntl(client_lifecycle_descriptor.get(), F_GET_SEALS);
    constexpr int required_lifecycle_seals =
        F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_FUTURE_WRITE | F_SEAL_SEAL;
    test->Expect(
        ::fstat(
            client_lifecycle_descriptor.get(),
            &lifecycle_descriptor_status) == 0 &&
            S_ISREG(lifecycle_descriptor_status.st_mode) &&
            lifecycle_descriptor_status.st_size ==
                static_cast<off_t>(
                    ipc::kPartialEventBrokerLifecycleBytesV2) &&
            lifecycle_access_flags >= 0 &&
            (lifecycle_access_flags & O_ACCMODE) == O_RDONLY &&
            lifecycle_seals >= 0 &&
            (lifecycle_seals & required_lifecycle_seals) ==
                required_lifecycle_seals,
        "lifecycle descriptor is read-only, fixed-size, and fully sealed");
    std::unique_ptr<ipc::PartialEventBrokerLifecycleReaderV2>
        lifecycle_reader;
    test->Expect(
        ipc::PartialEventBrokerLifecycleReaderV2::OpenDescriptor(
            client_lifecycle_descriptor.get(),
            RunId(),
            kSessionEpoch,
            kTradeDate,
            &lifecycle_reader,
            &system_error) ==
                ipc::PartialEventBrokerLifecycleOpenErrorV2::kNone &&
            lifecycle_reader != nullptr,
        "open independently sealed broker lifecycle descriptor");
    ipc::PartialEventBrokerLifecycleSnapshotV2 lifecycle_snapshot{};
    test->Expect(
        lifecycle_reader != nullptr &&
            lifecycle_reader->ReadSnapshot(&lifecycle_snapshot) ==
                ipc::PartialEventBrokerLifecycleReadResultV2::kOk &&
            lifecycle_snapshot.publication_generation == 1U &&
            lifecycle_snapshot.correction_epoch == 1U &&
            lifecycle_snapshot.broker_state ==
                ipc::PartialEventBrokerStateV2::kReady &&
            !lifecycle_snapshot.broker_stale &&
            !lifecycle_snapshot.heartbeat_expired,
        "lifecycle fd exposes the current ready identity and heartbeat");

    const pid_t worker = ::getpid();
    std::uint64_t worker_lease_epoch = 0U;
    test->Expect(
        broker->MarkWorkerRestarting(worker, &worker_lease_epoch) &&
            worker_lease_epoch != 0U,
        "name exact worker PID before private handoff");
    Descriptor restarting_descriptor;
    Descriptor restarting_lifecycle_descriptor;
    test->Expect(
        ipc::RequestPartialEventGenerationV2(
            public_socket,
            RunId(),
            kSessionEpoch,
            kTradeDate,
            1U,
            std::chrono::milliseconds(1000),
            restarting_descriptor.output(),
            restarting_lifecycle_descriptor.output(),
            &response,
            &system_error) == ipc::PartialEventBrokerResultV2::kOk &&
            restarting_descriptor.get() >= 0 &&
            restarting_lifecycle_descriptor.get() >= 0 &&
            response.publication_generation == 1U &&
            response.broker_state ==
                ipc::PartialEventBrokerStateV2::kRestarting &&
            response.broker_stale == 1U,
        "restart keeps public socket and last-good fd available as stale");
    test->Expect(
        lifecycle_reader != nullptr &&
            lifecycle_reader->ReadSnapshot(&lifecycle_snapshot) ==
                ipc::PartialEventBrokerLifecycleReadResultV2::kOk &&
            lifecycle_snapshot.lifecycle_epoch == worker_lease_epoch &&
            lifecycle_snapshot.broker_state ==
                ipc::PartialEventBrokerStateV2::kRestarting &&
            lifecycle_snapshot.broker_stale,
        "an already-attached lifecycle reader observes restarting without reconnect");
    const pid_t wrong_worker =
        worker == std::numeric_limits<pid_t>::max() ? worker - 1 : worker + 1;
    broker->SignalWorkerFailed(wrong_worker, worker_lease_epoch);
    test->Expect(
        broker->Snapshot().state ==
            ipc::PartialEventBrokerStateV2::kRestarting,
        "the active lease cannot be failed by a different worker PID");

    ipc::PartialEventHandoffResponseV2 handoff_response{};
    test->Expect(
        ipc::SubmitPartialEventGenerationV2(
            handoff_socket,
            second.descriptor.get(),
            second.producer->session(),
            std::chrono::milliseconds(1000),
            &handoff_response,
            &system_error) == ipc::PartialEventBrokerResultV2::kOk &&
            handoff_response.accepted_publication_generation == 2U &&
            handoff_response.accepted_correction_epoch == 1U,
        "exact worker hands off matching-prefix generation with SCM_RIGHTS");
    ipc::PartialOrderEventStatusSnapshotV2 old_status{};
    test->Expect(
        old_reader != nullptr &&
            old_reader->ReadStatus(&old_status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            old_status.publication_generation == 1U,
        "old client fd remains readable after atomic generation swap");

    test->Expect(
        broker->AdoptGeneration(
            conflict.descriptor.get(), conflict.producer->session()) ==
            ipc::PartialEventBrokerResultV2::kContinuityRejected &&
            broker->publication_generation() == 2U,
        "same correction epoch rejects a changed historical Event prefix");
    test->Expect(
        broker->AdoptGeneration(
            correction_jump.descriptor.get(),
            correction_jump.producer->session()) ==
            ipc::PartialEventBrokerResultV2::kGenerationRejected &&
            broker->publication_generation() == 2U,
        "correction epoch cannot jump over an unobserved epoch");
    test->Expect(
        broker->PromoteCorrectedGeneration(
            correction_jump.descriptor.get(),
            correction_jump.producer->session()) ==
                ipc::PartialEventBrokerResultV2::kGenerationRejected &&
            broker->publication_generation() == 2U,
        "explicit correction promotion still rejects an epoch jump");
    test->Expect(
        broker->AdoptGeneration(
            corrected.descriptor.get(), corrected.producer->session()) ==
                ipc::PartialEventBrokerResultV2::kGenerationRejected &&
            broker->publication_generation() == 2U,
        "ordinary append-only adoption cannot cross correction epoch");
    test->Expect(
        broker->PromoteCorrectedGeneration(
            unready_corrected.descriptor.get(),
            unready_corrected.producer->session()) ==
                ipc::PartialEventBrokerResultV2::kGenerationRejected &&
            broker->publication_generation() == 2U,
        "correction promotion rejects a stale non-contiguous candidate");
    test->Expect(
        broker->PromoteCorrectedGeneration(
            corrected.descriptor.get(), corrected.producer->session()) ==
                ipc::PartialEventBrokerResultV2::kOk &&
            broker->publication_generation() == 3U &&
            broker->correction_epoch() == 2U &&
            broker->Snapshot().adopted_canonical_frontier == 1U &&
            broker->Snapshot().adopted_event_frontier == 1U,
        "explicit next correction epoch permits a full replacement with lower process-local frontiers");

    broker->SignalWorkerFailed(worker, worker_lease_epoch);
    test->Expect(
        broker->Snapshot().state ==
                ipc::PartialEventBrokerStateV2::kStale &&
            broker->Snapshot().expected_worker == worker,
        "lock-free worker failure signal immediately projects broker stale");
    test->Expect(
        ipc::SubmitPartialEventGenerationV2(
            handoff_socket,
            post_failure.descriptor.get(),
            post_failure.producer->session(),
            std::chrono::milliseconds(1000),
            &handoff_response,
            &system_error) ==
            ipc::PartialEventBrokerResultV2::kGenerationRejected,
        "signalled worker cannot adopt a later generation over stale state");
    Descriptor stale_descriptor;
    Descriptor stale_lifecycle_descriptor;
    test->Expect(
        ipc::RequestPartialEventGenerationV2(
            public_socket,
            RunId(),
            kSessionEpoch,
            kTradeDate,
            3U,
            std::chrono::milliseconds(1000),
            stale_descriptor.output(),
            stale_lifecycle_descriptor.output(),
            &response,
            &system_error) == ipc::PartialEventBrokerResultV2::kOk &&
            stale_descriptor.get() >= 0 &&
            stale_lifecycle_descriptor.get() >= 0 &&
            response.publication_generation == 3U &&
            response.correction_epoch == 2U &&
            response.broker_state ==
                ipc::PartialEventBrokerStateV2::kStale &&
            response.broker_stale == 1U,
        "worker death retains corrected last-good generation as stale");
    broker->MarkWorkerFailed(worker, worker_lease_epoch);
    test->Expect(
        broker->Snapshot().state ==
                ipc::PartialEventBrokerStateV2::kStale &&
            broker->Snapshot().expected_worker == -1,
        "supervisor finalizes the asynchronously signalled failure");

    std::uint64_t replacement_lease_epoch = 0U;
    test->Expect(
        broker->MarkWorkerRestarting(worker, &replacement_lease_epoch) &&
            replacement_lease_epoch > worker_lease_epoch,
        "a replacement worker lease advances lifecycle identity");
    broker->SignalWorkerFailed(worker, worker_lease_epoch);
    test->Expect(
        broker->Snapshot().state ==
            ipc::PartialEventBrokerStateV2::kRestarting,
        "a late failure from the old lease cannot stale the replacement lease");
    broker->SignalWorkerFailed(worker, replacement_lease_epoch);
    test->Expect(
        broker->Snapshot().state ==
            ipc::PartialEventBrokerStateV2::kStale,
        "the active replacement lease still fails closed immediately");
    broker->MarkWorkerFailed(worker, replacement_lease_epoch);

    broker->Stop();
    std::uint64_t stopped_lease_epoch = 99U;
    test->Expect(
        !broker->MarkWorkerRestarting(worker, &stopped_lease_epoch) &&
            stopped_lease_epoch == 0U &&
            broker->AdoptGeneration(
                post_failure.descriptor.get(),
                post_failure.producer->session()) ==
                ipc::PartialEventBrokerResultV2::kGenerationRejected &&
            broker->publication_generation() == 3U,
        "broker stop is a linearization barrier for all later mutations");
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    test->Expect(
        lifecycle_reader != nullptr &&
            lifecycle_reader->ReadSnapshot(&lifecycle_snapshot) ==
                ipc::PartialEventBrokerLifecycleReadResultV2::kOk &&
            lifecycle_snapshot.heartbeat_expired &&
            lifecycle_snapshot.broker_stale &&
            lifecycle_snapshot.broker_state ==
                ipc::PartialEventBrokerStateV2::kStale,
        "stopped heartbeat makes an attached lifecycle reader fail closed");
    test->Expect(
        old_reader != nullptr &&
            old_reader->ReadStatus(&old_status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            old_status.publication_generation == 1U,
        "broker stop leaves the already-issued last-good journal fd readable");
}

void TestBrokerRecoversWithReplacementLease(Test* test) {
    TempDirectory directory;
    test->Expect(
        !directory.path().empty(),
        "create replacement-lease recovery test directory");
    if (directory.path().empty()) {
        return;
    }

    constexpr std::uint64_t correction_epoch = 11U;
    Generation initial =
        MakeGeneration(1U, correction_epoch, 100, test);
    Generation active =
        MakeGeneration(2U, correction_epoch, 100, test);
    Generation replacement =
        MakeGeneration(3U, correction_epoch, 100, test);
    if (initial.producer == nullptr || active.producer == nullptr ||
        replacement.producer == nullptr ||
        !AppendSecondCanonicalEvent(&replacement, 90, test)) {
        return;
    }

    const std::filesystem::path public_socket =
        directory.path() / "recovery.events.sock";
    const std::filesystem::path handoff_socket =
        directory.path() / "recovery.handoff.sock";
    app::PartialEventStableBrokerConfigV2 config{};
    config.public_socket_path = public_socket;
    config.worker_handoff_socket_path = handoff_socket;
    config.expected_run_id = RunId();
    config.expected_session_epoch = kSessionEpoch;
    config.expected_trade_date = kTradeDate;
    config.maximum_mapping_bytes =
        initial.producer->session().total_mapping_bytes * 2U;
    config.allowed_uid = ::geteuid();
    config.request_timeout = std::chrono::milliseconds(1000);
    config.lifecycle_heartbeat_interval = std::chrono::milliseconds(10);
    config.lifecycle_heartbeat_timeout = std::chrono::milliseconds(1000);

    std::unique_ptr<app::PartialEventStableBrokerV2> broker;
    int system_error = 0;
    const auto create_error = app::PartialEventStableBrokerV2::Create(
        config, &broker, &system_error);
    test->Expect(
        create_error ==
                app::PartialEventStableBrokerCreateErrorV2::kNone &&
            broker != nullptr && system_error == 0,
        "create broker for replacement-lease recovery");
    if (broker == nullptr) {
        return;
    }

    test->Expect(
        broker->AdoptGeneration(
            initial.descriptor.get(), initial.producer->session()) ==
                ipc::PartialEventBrokerResultV2::kOk &&
            broker->state() == ipc::PartialEventBrokerStateV2::kReady,
        "adopt the initial ready generation before worker handoff");

    const pid_t worker = ::getpid();
    std::uint64_t failed_lease_epoch = 0U;
    test->Expect(
        broker->MarkWorkerRestarting(worker, &failed_lease_epoch) &&
            failed_lease_epoch != 0U,
        "register the lease that will fail");
    ipc::PartialEventHandoffResponseV2 handoff_response{};
    test->Expect(
        ipc::SubmitPartialEventGenerationV2(
            handoff_socket,
            active.descriptor.get(),
            active.producer->session(),
            std::chrono::milliseconds(1000),
            &handoff_response,
            &system_error) == ipc::PartialEventBrokerResultV2::kOk &&
            handoff_response.accepted_publication_generation == 2U &&
            handoff_response.accepted_correction_epoch == correction_epoch &&
            broker->Snapshot().state ==
                ipc::PartialEventBrokerStateV2::kReady &&
            broker->Snapshot().expected_worker == worker &&
            broker->Snapshot().lifecycle_epoch == failed_lease_epoch,
        "activate the old lease on the second same-epoch generation");

    broker->SignalWorkerFailed(worker, failed_lease_epoch);
    test->Expect(
        broker->Snapshot().state ==
                ipc::PartialEventBrokerStateV2::kStale &&
            broker->Snapshot().publication_generation == 2U,
        "active lease failure makes the second generation stale");
    broker->MarkWorkerFailed(worker, failed_lease_epoch);
    test->Expect(
        broker->Snapshot().state ==
                ipc::PartialEventBrokerStateV2::kStale &&
            broker->Snapshot().expected_worker == -1,
        "supervisor finalizes the failed lease before replacement");

    std::uint64_t replacement_lease_epoch = 0U;
    test->Expect(
        broker->MarkWorkerRestarting(
            worker, &replacement_lease_epoch) &&
            replacement_lease_epoch > failed_lease_epoch,
        "register a distinct replacement lease");
    broker->SignalWorkerFailed(worker, failed_lease_epoch);
    test->Expect(
        broker->Snapshot().state ==
                ipc::PartialEventBrokerStateV2::kRestarting &&
            broker->Snapshot().expected_worker == worker &&
            broker->Snapshot().lifecycle_epoch == replacement_lease_epoch,
        "old lease failure cannot stale the replacement while restarting");

    handoff_response = {};
    test->Expect(
        ipc::SubmitPartialEventGenerationV2(
            handoff_socket,
            replacement.descriptor.get(),
            replacement.producer->session(),
            std::chrono::milliseconds(1000),
            &handoff_response,
            &system_error) == ipc::PartialEventBrokerResultV2::kOk &&
            handoff_response.accepted_publication_generation == 3U &&
            handoff_response.accepted_correction_epoch == correction_epoch,
        "replacement lease submits the next same-epoch generation");
    const app::PartialEventStableBrokerSnapshotV2 recovered =
        broker->Snapshot();
    test->Expect(
        recovered.state == ipc::PartialEventBrokerStateV2::kReady &&
            recovered.has_generation &&
            recovered.expected_worker == worker &&
            recovered.lifecycle_epoch == replacement_lease_epoch &&
            recovered.publication_generation == 3U &&
            recovered.correction_epoch == correction_epoch &&
            recovered.adopted_canonical_frontier == 2U &&
            recovered.adopted_event_frontier == 2U,
        "replacement generation restores READY with advanced frontiers");

    broker->SignalWorkerFailed(worker, failed_lease_epoch);
    const app::PartialEventStableBrokerSnapshotV2 after_late_failure =
        broker->Snapshot();
    test->Expect(
        after_late_failure.state ==
                ipc::PartialEventBrokerStateV2::kReady &&
            after_late_failure.expected_worker == worker &&
            after_late_failure.lifecycle_epoch == replacement_lease_epoch &&
            after_late_failure.publication_generation == 3U,
        "late failure from the old lease cannot pollute recovered READY");

    Descriptor journal_descriptor;
    Descriptor lifecycle_descriptor;
    ipc::PartialEventBrokerResponseV2 response{};
    test->Expect(
        ipc::RequestPartialEventGenerationV2(
            public_socket,
            RunId(),
            kSessionEpoch,
            kTradeDate,
            3U,
            std::chrono::milliseconds(1000),
            journal_descriptor.output(),
            lifecycle_descriptor.output(),
            &response,
            &system_error) == ipc::PartialEventBrokerResultV2::kOk &&
            journal_descriptor.get() >= 0 &&
            lifecycle_descriptor.get() >= 0 &&
            journal_descriptor.get() != lifecycle_descriptor.get() &&
            response.descriptor_count == 2U &&
            response.publication_generation == 3U &&
            response.correction_epoch == correction_epoch &&
            response.adopted_canonical_frontier == 2U &&
            response.adopted_event_frontier == 2U &&
            response.broker_state ==
                ipc::PartialEventBrokerStateV2::kReady &&
            response.broker_stale == 0U,
        "new public connection receives recovered generation and two fds");

    auto journal_reader = OpenReader(
        journal_descriptor.get(), replacement.producer->session(), test);
    ipc::PartialOrderEventStatusSnapshotV2 status{};
    std::array<ipc::PartialOrderEventEnvelopeV2, 2U> events{};
    ipc::PartialOrderEventReadBatchResultV2 batch{};
    test->Expect(
        journal_reader != nullptr &&
            journal_reader->ReadStatus(&status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            status.publication_generation == 3U &&
            status.correction_epoch == correction_epoch &&
            status.cut.canonical_apply_frontier == 2U &&
            status.cut.event_published_frontier == 2U &&
            journal_reader->ReadEvents(1U, events, &batch) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            batch.rows_read == events.size() &&
            events[0U].event.derived_event_sequence == 1U &&
            events[1U].event.derived_event_sequence == 2U,
        "first fd is the recovered journal with the retained prefix and suffix");

    std::unique_ptr<ipc::PartialEventBrokerLifecycleReaderV2>
        lifecycle_reader;
    test->Expect(
        ipc::PartialEventBrokerLifecycleReaderV2::OpenDescriptor(
            lifecycle_descriptor.get(),
            RunId(),
            kSessionEpoch,
            kTradeDate,
            &lifecycle_reader,
            &system_error) ==
                ipc::PartialEventBrokerLifecycleOpenErrorV2::kNone &&
            lifecycle_reader != nullptr,
        "second fd is the broker lifecycle mapping");
    ipc::PartialEventBrokerLifecycleSnapshotV2 lifecycle{};
    test->Expect(
        lifecycle_reader != nullptr &&
            lifecycle_reader->ReadSnapshot(&lifecycle) ==
                ipc::PartialEventBrokerLifecycleReadResultV2::kOk &&
            lifecycle.lifecycle_epoch == replacement_lease_epoch &&
            lifecycle.publication_generation == 3U &&
            lifecycle.correction_epoch == correction_epoch &&
            lifecycle.expected_worker == worker &&
            lifecycle.asynchronously_failed_lease_epoch !=
                replacement_lease_epoch &&
            lifecycle.broker_state ==
                ipc::PartialEventBrokerStateV2::kReady &&
            !lifecycle.broker_stale && !lifecycle.heartbeat_expired,
        "lifecycle fd reports the healthy replacement lease and generation");
}

void TestCorrectionEpochAndFrontierBoundaries(Test* test) {
    TempDirectory directory;
    test->Expect(
        !directory.path().empty(),
        "create correction-boundary test directory");
    if (directory.path().empty()) {
        return;
    }

    constexpr std::uint64_t maximum_epoch =
        std::numeric_limits<std::uint64_t>::max();
    Generation maximum_initial =
        MakeGeneration(1U, maximum_epoch, 100, test);
    Generation maximum_successor =
        MakeGeneration(2U, maximum_epoch, 100, test);
    Generation maximum_different =
        MakeGeneration(3U, maximum_epoch - 1U, 100, test);
    if (maximum_initial.producer == nullptr ||
        maximum_successor.producer == nullptr ||
        maximum_different.producer == nullptr ||
        !AppendSecondCanonicalEvent(&maximum_successor, 90, test) ||
        !AppendSecondCanonicalEvent(&maximum_different, 90, test)) {
        return;
    }
    auto maximum_broker = CreateSameProcessBroker(
        directory.path() / "maximum.sock",
        maximum_initial.producer->session().total_mapping_bytes,
        test);
    if (maximum_broker == nullptr) {
        return;
    }
    test->Expect(
        maximum_broker->AdoptGeneration(
            maximum_initial.descriptor.get(),
            maximum_initial.producer->session()) ==
                ipc::PartialEventBrokerResultV2::kOk &&
            maximum_broker->AdoptGeneration(
                maximum_successor.descriptor.get(),
                maximum_successor.producer->session()) ==
                ipc::PartialEventBrokerResultV2::kOk &&
            maximum_broker->publication_generation() == 2U &&
            maximum_broker->correction_epoch() == maximum_epoch,
        "maximum correction epoch permits a same-epoch prefix successor");
    test->Expect(
        maximum_broker->AdoptGeneration(
            maximum_different.descriptor.get(),
            maximum_different.producer->session()) ==
                ipc::PartialEventBrokerResultV2::kGenerationRejected &&
            maximum_broker->publication_generation() == 2U &&
            maximum_broker->correction_epoch() == maximum_epoch,
        "maximum correction epoch rejects every different epoch");
    test->Expect(
        maximum_broker->PromoteCorrectedGeneration(
            maximum_different.descriptor.get(),
            maximum_different.producer->session()) ==
                ipc::PartialEventBrokerResultV2::kGenerationRejected &&
            maximum_broker->publication_generation() == 2U &&
            maximum_broker->correction_epoch() == maximum_epoch,
        "maximum correction epoch also rejects explicit promotion regression");

    const auto expect_frontier_regression_rejected =
        [&](std::string_view socket_name,
            Generation* initial,
            Generation* candidate,
            std::string_view message) {
            if (initial == nullptr || candidate == nullptr ||
                initial->producer == nullptr ||
                candidate->producer == nullptr) {
                return;
            }
            auto broker = CreateSameProcessBroker(
                directory.path() / socket_name,
                initial->producer->session().total_mapping_bytes,
                test);
            if (broker == nullptr) {
                return;
            }
            test->Expect(
                broker->AdoptGeneration(
                    initial->descriptor.get(),
                    initial->producer->session()) ==
                        ipc::PartialEventBrokerResultV2::kOk &&
                    broker->AdoptGeneration(
                        candidate->descriptor.get(),
                        candidate->producer->session()) ==
                        ipc::PartialEventBrokerResultV2::
                            kGenerationRejected &&
                    broker->publication_generation() ==
                        initial->producer->session()
                            .publication_generation,
                message);
        };

    Generation captured_initial = MakeGeneration(1U, 7U, 100, test);
    Generation captured_regression = MakeGeneration(2U, 7U, 100, test);
    if (!AppendSecondCanonicalEvent(&captured_initial, 90, test) ||
        !AppendSecondCanonicalEvent(
            &captured_regression, 90, test, true, 1U)) {
        return;
    }
    expect_frontier_regression_rejected(
        "captured.sock",
        &captured_initial,
        &captured_regression,
        "same epoch rejects only captured-source frontier regression");

    Generation canonical_initial = MakeGeneration(1U, 8U, 100, test);
    Generation canonical_regression =
        MakeGeneration(2U, 8U, 100, test, true, 2U);
    if (!AppendSecondCanonicalEvent(
            &canonical_initial, 90, test, false, 2U)) {
        return;
    }
    expect_frontier_regression_rejected(
        "canonical.sock",
        &canonical_initial,
        &canonical_regression,
        "same epoch rejects only canonical frontier regression");

    Generation event_initial = MakeGeneration(1U, 9U, 100, test);
    Generation event_regression = MakeGeneration(2U, 9U, 100, test);
    if (!AppendSecondCanonicalEvent(&event_initial, 90, test) ||
        !AppendSecondCanonicalEvent(
            &event_regression, 90, test, false, 2U)) {
        return;
    }
    expect_frontier_regression_rejected(
        "event.sock",
        &event_initial,
        &event_regression,
        "same epoch rejects only Event frontier regression");
}

void TestCorrectionPromotionBarrier(Test* test) {
    TempDirectory directory;
    test->Expect(
        !directory.path().empty(),
        "create correction-promotion barrier test directory");
    if (directory.path().empty()) {
        return;
    }

    constexpr std::uint64_t old_epoch = 20U;
    constexpr std::uint64_t corrected_epoch = old_epoch + 1U;
    Generation initial = MakeGeneration(1U, old_epoch, 100, test);
    Generation same_epoch = MakeGeneration(2U, old_epoch, 100, test);
    Generation invalid_state =
        MakeGeneration(3U, corrected_epoch, 99, test);
    Generation stale = MakeGeneration(3U, corrected_epoch, 99, test);
    Generation failed = MakeGeneration(3U, corrected_epoch, 99, test);
    Generation affected = MakeGeneration(3U, corrected_epoch, 99, test);
    Generation pending = MakeGeneration(3U, corrected_epoch, 99, test);
    Generation stopped = MakeGeneration(3U, corrected_epoch, 99, test);
    if (initial.producer == nullptr || same_epoch.producer == nullptr ||
        invalid_state.producer == nullptr || stale.producer == nullptr ||
        failed.producer == nullptr || affected.producer == nullptr ||
        pending.producer == nullptr || stopped.producer == nullptr) {
        return;
    }

    const ipc::PartialOrderEventChannelHealthV2 affected_channel =
        AffectedChannel(0U);
    const ipc::PartialOrderEventChannelHealthV2 pending_channel =
        AffectedChannel(1U);
    const std::span<const ipc::PartialOrderEventChannelHealthV2> no_channels{};
    if (!PublishGenerationStatus(
            &invalid_state,
            ipc::PartialOrderEventServiceStateV2::kInitializing,
            false,
            ipc::PartialOrderEventLastErrorV2::kNone,
            0U,
            no_channels,
            "publish non-stale but non-promotable correction state",
            test) ||
        !PublishGenerationStatus(
            &stale,
            ipc::PartialOrderEventServiceStateV2::kStoppedClean,
            true,
            ipc::PartialOrderEventLastErrorV2::kNone,
            0U,
            no_channels,
            "publish stale STOPPED_CLEAN correction candidate",
            test) ||
        !PublishGenerationStatus(
            &failed,
            ipc::PartialOrderEventServiceStateV2::kStoppedClean,
            false,
            ipc::PartialOrderEventLastErrorV2::kWorkerExited,
            0U,
            no_channels,
            "publish failed STOPPED_CLEAN correction candidate",
            test) ||
        !PublishGenerationStatus(
            &affected,
            ipc::PartialOrderEventServiceStateV2::kStoppedClean,
            false,
            ipc::PartialOrderEventLastErrorV2::kNone,
            0U,
            std::span{&affected_channel, std::size_t{1U}},
            "publish zero-pending affected correction candidate",
            test) ||
        !PublishGenerationStatus(
            &pending,
            ipc::PartialOrderEventServiceStateV2::kStoppedClean,
            false,
            ipc::PartialOrderEventLastErrorV2::kNone,
            1U,
            std::span{&pending_channel, std::size_t{1U}},
            "publish pending affected correction candidate",
            test) ||
        !PublishGenerationStatus(
            &stopped,
            ipc::PartialOrderEventServiceStateV2::kStoppedClean,
            false,
            ipc::PartialOrderEventLastErrorV2::kNone,
            0U,
            no_channels,
            "publish healthy STOPPED_CLEAN correction candidate",
            test)) {
        return;
    }

    auto broker = CreateSameProcessBroker(
        directory.path() / "promotion.sock",
        initial.producer->session().total_mapping_bytes,
        test);
    if (broker == nullptr) {
        return;
    }
    test->Expect(
        broker->AdoptGeneration(
            initial.descriptor.get(), initial.producer->session()) ==
            ipc::PartialEventBrokerResultV2::kOk,
        "ordinary adoption establishes the promotion test baseline");
    test->Expect(
        broker->PromoteCorrectedGeneration(
            same_epoch.descriptor.get(), same_epoch.producer->session()) ==
                ipc::PartialEventBrokerResultV2::kGenerationRejected &&
            broker->publication_generation() == 1U,
        "explicit correction promotion rejects a same-epoch generation");
    test->Expect(
        broker->AdoptGeneration(
            same_epoch.descriptor.get(), same_epoch.producer->session()) ==
                ipc::PartialEventBrokerResultV2::kOk &&
            broker->publication_generation() == 2U &&
            broker->correction_epoch() == old_epoch,
        "ordinary adoption remains the only same-epoch path");

    const auto expect_rejected =
        [&](const Generation& candidate, std::string_view message) {
            test->Expect(
                broker->PromoteCorrectedGeneration(
                    candidate.descriptor.get(),
                    candidate.producer->session()) ==
                        ipc::PartialEventBrokerResultV2::
                            kGenerationRejected &&
                    broker->publication_generation() == 2U &&
                    broker->correction_epoch() == old_epoch,
                message);
        };
    expect_rejected(
        invalid_state,
        "promotion rejects a non-stale cut outside CONTIGUOUS/STOPPED_CLEAN");
    expect_rejected(stale, "promotion rejects a stale STOPPED_CLEAN cut");
    expect_rejected(failed, "promotion rejects a cut with a last error");
    expect_rejected(
        affected,
        "promotion rejects an affected channel even when pending is zero");
    expect_rejected(pending, "promotion rejects pending native positions");

    test->Expect(
        broker->PromoteCorrectedGeneration(
            stopped.descriptor.get(), stopped.producer->session()) ==
                ipc::PartialEventBrokerResultV2::kOk &&
            broker->publication_generation() == 3U &&
            broker->correction_epoch() == corrected_epoch &&
            broker->state() ==
                ipc::PartialEventBrokerStateV2::kStoppedClean,
        "promotion accepts a healthy empty STOPPED_CLEAN correction cut");
}

void TestReaderRejectsUnsafeDescriptors(Test* test) {
    TempDirectory directory;
    Generation generation = MakeGeneration(1U, 1U, 100, test, false);
    if (directory.path().empty() || generation.producer == nullptr) {
        return;
    }
    const auto expected = Expected(generation.producer->session());
    int system_error = 0;
    std::unique_ptr<ipc::PartialOrderEventReaderV2> reader;

    Descriptor non_regular;
    non_regular.Reset(::open("/dev/null", O_RDONLY | O_CLOEXEC));
    test->Expect(
        non_regular.get() >= 0 &&
            ipc::PartialOrderEventReaderV2::OpenDescriptor(
                non_regular.get(), expected, &reader, &system_error) ==
                ipc::PartialOrderEventReaderOpenErrorV2::
                    kInvalidDescriptor &&
            reader == nullptr,
        "direct reader rejects a non-regular descriptor");

    const std::filesystem::path backing_path =
        directory.path() / "unsafe.bin";
    Descriptor writable;
    writable.Reset(::open(
        backing_path.c_str(),
        O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC,
        S_IRUSR | S_IWUSR));
    test->Expect(
        writable.get() >= 0 &&
            ipc::PartialOrderEventReaderV2::OpenDescriptor(
                writable.get(), expected, &reader, &system_error) ==
                ipc::PartialOrderEventReaderOpenErrorV2::
                    kInvalidDescriptor &&
            reader == nullptr,
        "direct reader rejects a regular O_RDWR descriptor");
    const std::string proc_path =
        "/proc/self/fd/" + std::to_string(writable.get());
    Descriptor read_only;
    read_only.Reset(::open(proc_path.c_str(), O_RDONLY | O_CLOEXEC));
    test->Expect(
        read_only.get() >= 0 &&
            ipc::PartialOrderEventReaderV2::OpenDescriptor(
                read_only.get(), expected, &reader, &system_error) ==
                ipc::PartialOrderEventReaderOpenErrorV2::
                    kDescriptorStatFailed &&
            reader == nullptr,
        "direct reader rejects an undersized regular descriptor");
    test->Expect(
        writable.get() >= 0 &&
            ::ftruncate(
                writable.get(),
                static_cast<off_t>(
                    ipc::kPartialOrderEventHeaderBytesV2)) == 0 &&
            ipc::PartialOrderEventReaderV2::OpenDescriptor(
                read_only.get(), expected, &reader, &system_error) ==
                ipc::PartialOrderEventReaderOpenErrorV2::
                    kDescriptorSealMismatch &&
            reader == nullptr,
        "direct reader rejects an unsealed regular O_RDONLY descriptor");
    static_cast<void>(::unlink(backing_path.c_str()));
}

void TestClientRejectsBrokerSuppliedDescriptors(Test* test) {
    TempDirectory directory;
    Generation generation = MakeGeneration(1U, 1U, 100, test, false);
    Descriptor non_regular;
    non_regular.Reset(::open("/dev/null", O_RDONLY | O_CLOEXEC));
    if (directory.path().empty() || generation.producer == nullptr ||
        non_regular.get() < 0) {
        return;
    }

    const auto request_fake = [&](
                                  std::string_view name,
                                  int passed_descriptor,
                                  std::uint64_t response_generation) {
        const std::filesystem::path socket_path =
            directory.path() / std::string(name);
        const std::string native = socket_path.string();
        if (native.empty() || native.size() >= sizeof(sockaddr_un::sun_path)) {
            return ipc::PartialEventBrokerResultV2::kInternalError;
        }
        Descriptor listener;
        listener.Reset(
            ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0));
        sockaddr_un address{};
        address.sun_family = AF_UNIX;
        std::memcpy(
            address.sun_path, native.c_str(), native.size() + 1U);
        const std::size_t native_address_bytes =
            offsetof(sockaddr_un, sun_path) + native.size() + 1U;
        const socklen_t address_bytes =
            static_cast<socklen_t>(native_address_bytes);
        if (listener.get() < 0 ||
            ::bind(
                listener.get(),
                reinterpret_cast<const sockaddr*>(&address),
                address_bytes) != 0 ||
            ::chmod(socket_path.c_str(), S_IRUSR | S_IWUSR) != 0 ||
            ::listen(listener.get(), 1) != 0) {
            return ipc::PartialEventBrokerResultV2::kInternalError;
        }

        std::atomic<bool> server_ok{false};
        std::thread server([&] {
            pollfd ready{};
            ready.fd = listener.get();
            ready.events = POLLIN;
            if (::poll(&ready, 1U, 1000) <= 0 ||
                (ready.revents & POLLIN) == 0) {
                return;
            }
            Descriptor client;
            client.Reset(::accept4(
                listener.get(), nullptr, nullptr, SOCK_CLOEXEC));
            ipc::PartialEventBrokerRequestV2 request{};
            if (client.get() < 0 ||
                ::recv(
                    client.get(),
                    &request,
                    sizeof(request),
                    0) != static_cast<ssize_t>(sizeof(request))) {
                return;
            }
            ipc::PartialEventBrokerResponseV2 response{};
            response.result = ipc::PartialEventBrokerResultV2::kOk;
            response.broker_state =
                ipc::PartialEventBrokerStateV2::kReady;
            response.broker_stale = 0U;
            response.descriptor_count = 2U;
            response.run_id = WireRunId();
            response.session_epoch = kSessionEpoch;
            response.publication_generation = response_generation;
            response.correction_epoch = 1U;
            response.trade_date = kTradeDate;
            response.ordering_quality =
                ipc::PartialOrderEventOrderingQualityV2::
                    kBoundedReorderedPartial;
            iovec vector{};
            vector.iov_base = &response;
            vector.iov_len = sizeof(response);
            std::array<std::byte, CMSG_SPACE(sizeof(int) * 2U)> control{};
            msghdr message{};
            message.msg_iov = &vector;
            message.msg_iovlen = 1U;
            message.msg_control = control.data();
            message.msg_controllen = control.size();
            cmsghdr* const header = CMSG_FIRSTHDR(&message);
            if (header == nullptr) {
                return;
            }
            header->cmsg_level = SOL_SOCKET;
            header->cmsg_type = SCM_RIGHTS;
            header->cmsg_len = CMSG_LEN(sizeof(int) * 2U);
            const std::array<int, 2U> passed_descriptors{
                passed_descriptor, passed_descriptor};
            std::memcpy(
                CMSG_DATA(header),
                passed_descriptors.data(),
                sizeof(passed_descriptors));
            server_ok.store(
                ::sendmsg(client.get(), &message, MSG_NOSIGNAL) ==
                    static_cast<ssize_t>(sizeof(response)),
                std::memory_order_release);
        });

        Descriptor output;
        Descriptor lifecycle_output;
        ipc::PartialEventBrokerResponseV2 response{};
        int system_error = 0;
        const auto result = ipc::RequestPartialEventGenerationV2(
            socket_path,
            RunId(),
            kSessionEpoch,
            kTradeDate,
            0U,
            std::chrono::milliseconds(1000),
            output.output(),
            lifecycle_output.output(),
            &response,
            &system_error);
        server.join();
        static_cast<void>(::unlink(socket_path.c_str()));
        if (!server_ok.load(std::memory_order_acquire) ||
            output.get() != -1 || lifecycle_output.get() != -1) {
            return ipc::PartialEventBrokerResultV2::kInternalError;
        }
        return result;
    };

    test->Expect(
        request_fake(
            "non-regular.sock", non_regular.get(), 1U) ==
            ipc::PartialEventBrokerResultV2::kDescriptorRejected,
        "client rejects a non-regular SCM_RIGHTS descriptor");
    test->Expect(
        request_fake(
            "identity-mismatch.sock",
            generation.descriptor.get(),
            2U) ==
            ipc::PartialEventBrokerResultV2::kDescriptorRejected,
        "client rejects SCM_RIGHTS journal identity mismatching the response");
}

void TestSameProcessModeDoesNotRequireHandoffPath(Test* test) {
    TempDirectory directory;
    Generation generation = MakeGeneration(1U, 1U, 100, test, false);
    if (directory.path().empty() || generation.producer == nullptr) {
        return;
    }
    app::PartialEventStableBrokerConfigV2 config{};
    config.public_socket_path = directory.path() / "events.sock";
    config.worker_handoff_socket_path.clear();
    config.expected_run_id = RunId();
    config.expected_session_epoch = kSessionEpoch;
    config.expected_trade_date = kTradeDate;
    config.maximum_mapping_bytes =
        generation.producer->session().total_mapping_bytes;
    config.allowed_uid = ::geteuid();
    std::unique_ptr<app::PartialEventStableBrokerV2> broker;
    int system_error = 0;
    const auto create_error = app::PartialEventStableBrokerV2::Create(
        config, &broker, &system_error);
    test->Expect(
        create_error ==
                app::PartialEventStableBrokerCreateErrorV2::kNone &&
            broker != nullptr,
        "same-process broker binds only its public socket");
    if (broker == nullptr) {
        std::cerr
            << "same-process broker create error="
            << app::PartialEventStableBrokerCreateErrorNameV2(
                   create_error)
            << " errno=" << system_error << '\n';
    }
    if (broker != nullptr) {
        test->Expect(
            broker->AdoptGeneration(
                generation.descriptor.get(),
                generation.producer->session()) ==
                    ipc::PartialEventBrokerResultV2::kOk &&
                broker->state() ==
                    ipc::PartialEventBrokerStateV2::kReady,
            "same-process initializing mapping is a ready worker, not stale");

        Descriptor attached;
        Descriptor lifecycle_attached;
        ipc::PartialEventBrokerResponseV2 response{};
        test->Expect(
            ipc::RequestPartialEventGenerationV2(
                config.public_socket_path,
                RunId(),
                kSessionEpoch,
                kTradeDate,
                1U,
                std::chrono::milliseconds(1000),
                attached.output(),
                lifecycle_attached.output(),
                &response,
                &system_error) == ipc::PartialEventBrokerResultV2::kOk &&
                attached.get() >= 0 &&
                lifecycle_attached.get() >= 0 &&
                response.descriptor_count == 2U &&
                response.broker_state ==
                    ipc::PartialEventBrokerStateV2::kReady &&
                response.broker_stale == 0U,
            "broker lifecycle stays ready while mapping status initializes");
    }
}

}  // namespace

int main() {
    Test test;
    TestStableBrokerLifecycle(&test);
    TestBrokerRecoversWithReplacementLease(&test);
    TestCorrectionEpochAndFrontierBoundaries(&test);
    TestCorrectionPromotionBarrier(&test);
    TestReaderRejectsUnsafeDescriptors(&test);
    TestClientRejectsBrokerSuppliedDescriptors(&test);
    TestSameProcessModeDoesNotRequireHandoffPath(&test);
    if (test.failures() != 0) {
        return 1;
    }
    std::cout << "PASS: partial Event stable broker v2\n";
    return 0;
}
