#include "l2flow/ipc/order_event_delta_ring_v1.h"

#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <span>
#include <string_view>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {

namespace ipc = l2flow::ipc;

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAILED: " << message << '\n';
    }
    return condition;
}

l2flow::common::Identity128 RunId(std::uint8_t seed) {
    l2flow::common::Identity128 result{};
    for (std::size_t index = 0U; index < result.size(); ++index) {
        result[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                seed + static_cast<std::uint8_t>(index)));
    }
    return result;
}

ipc::OrderEventDeltaRingConfigV1 Config(
    std::uint64_t capacity,
    std::uint8_t seed = 1U) {
    ipc::OrderEventDeltaRingConfigV1 result{};
    result.run_id = RunId(seed);
    result.session_epoch = 17U;
    result.trade_date = 20260730U;
    result.ring_capacity = capacity;
    result.maximum_mapping_bytes = 64ULL * 1024ULL * 1024ULL;
    result.producer_started_monotonic_ns = 1234U;
    return result;
}

std::unique_ptr<ipc::OrderEventDeltaRingProducerV1> Producer(
    std::uint64_t capacity,
    bool* ok,
    std::uint8_t seed = 1U) {
    std::unique_ptr<ipc::OrderEventDeltaRingProducerV1> result;
    int system_error = -1;
    const auto error =
        ipc::OrderEventDeltaRingProducerV1::Create(
            Config(capacity, seed), &result, &system_error);
    *ok &= Expect(
        error == ipc::OrderEventDeltaRingCreateErrorV1::kNone &&
            result != nullptr && system_error == 0,
        "create event-delta producer");
    return result;
}

std::unique_ptr<ipc::OrderEventDeltaRingReaderV1> Reader(
    const ipc::OrderEventDeltaRingProducerV1& producer,
    bool* ok) {
    int descriptor = -1;
    int system_error = -1;
    *ok &= Expect(
        producer.DuplicateReadOnlyDescriptor(
            &descriptor, &system_error) &&
            descriptor >= 0 && system_error == 0,
        "duplicate read-only event ring descriptor");
    std::unique_ptr<ipc::OrderEventDeltaRingReaderV1> result;
    const auto error = ipc::OrderEventDeltaRingReaderV1::Open(
        descriptor,
        producer.session(),
        &result,
        &system_error);
    if (descriptor >= 0) {
        static_cast<void>(::close(descriptor));
    }
    *ok &= Expect(
        error == ipc::OrderEventDeltaReaderOpenErrorV1::kNone &&
            result != nullptr && system_error == 0,
        "open event-delta reader");
    return result;
}

std::unique_ptr<ipc::OrderEventDeltaRingReaderV1> ReaderAt(
    const ipc::OrderEventDeltaRingProducerV1& producer,
    std::uint64_t start_event_sequence,
    bool* ok) {
    int descriptor = -1;
    int system_error = -1;
    *ok &= Expect(
        producer.DuplicateReadOnlyDescriptor(
            &descriptor, &system_error) &&
            descriptor >= 0 && system_error == 0,
        "duplicate read-only descriptor for OpenAt");
    std::unique_ptr<ipc::OrderEventDeltaRingReaderV1> result;
    const auto error = ipc::OrderEventDeltaRingReaderV1::OpenAt(
        descriptor,
        producer.session(),
        start_event_sequence,
        &result,
        &system_error);
    if (descriptor >= 0) {
        static_cast<void>(::close(descriptor));
    }
    *ok &= Expect(
        error == ipc::OrderEventDeltaReaderOpenErrorV1::kNone &&
            result != nullptr && system_error == 0,
        "open event-delta reader at explicit sequence");
    return result;
}

ipc::OrderEventDeltaPayloadV1 Base(
    std::uint64_t tick_sequence,
    std::uint8_t event_kind,
    std::uint32_t instrument_id = 1U) {
    ipc::OrderEventDeltaPayloadV1 result{};
    result.record_schema_version =
        ipc::kOrderEventDeltaPayloadSchemaV1;
    result.record_bytes = sizeof(result);
    result.trade_date = 20260730U;
    result.instrument_id = instrument_id;
    result.channel = 1;
    result.market =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1;
    result.event_kind = event_kind;
    result.native_event_sequence =
        static_cast<std::int64_t>(tick_sequence);
    result.source_sequence = tick_sequence;
    result.ingress_sequence = tick_sequence;
    result.tick_stream_sequence = tick_sequence;
    result.vendor_sequence_id = tick_sequence;
    result.reserved1[0U] =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_SOURCE_TICK_ORDINAL_VALID_V2;
    return result;
}

ipc::OrderEventDeltaPayloadV1 Order(
    std::uint64_t tick_sequence,
    std::int64_t order_id,
    std::uint32_t instrument_id = 1U) {
    auto result = Base(
        tick_sequence,
        L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_REVISION_V1,
        instrument_id);
    result.order_id = order_id;
    result.revision = 1U;
    result.original_quantity = 100;
    result.original_quantity_valid = 1U;
    result.remaining_quantity = 100;
    result.remaining_quantity_valid = 1U;
    result.side = 1U;
    return result;
}

ipc::OrderEventDeltaPayloadV1 Trade(
    std::uint64_t tick_sequence,
    std::int64_t quantity,
    std::uint32_t instrument_id = 1U) {
    auto result = Base(
        tick_sequence,
        L2FLOW_INSTRUMENT_DERIVED_EVENT_TRADE_V1,
        instrument_id);
    result.buy_order_id = 10;
    result.sell_order_id = 20;
    result.price_p6 = 10'000'000;
    result.price_valid = 1U;
    result.quantity = quantity;
    return result;
}

void TestAbiAndValidation(bool* ok) {
    *ok &= Expect(
        sizeof(ipc::OrderEventDeltaPayloadV1) == 320U &&
            sizeof(ipc::OrderEventDeltaHeaderV1) == 4096U &&
            sizeof(ipc::OrderEventDeltaSlotV1) == 384U &&
            ipc::kOrderEventDeltaWireMinorV1 == 2U &&
            ipc::kOrderEventDeltaPayloadSchemaV1 ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_ROW_SCHEMA_V2 &&
            offsetof(
                ipc::OrderEventDeltaHeaderV1,
                temporal_coverage) == 120U &&
            offsetof(
                ipc::OrderEventDeltaHeaderV1,
                stream_quality) == 124U,
        "fixed event-delta ABI sizes");

    auto event = Order(1U, 42);
    *ok &= Expect(
        ipc::OrderEventDeltaPayloadCanonicalV1(
            event, 20260730U, 1U),
        "canonical order event accepted");
    event.reserved1[1U] = 1U;
    *ok &= Expect(
        !ipc::OrderEventDeltaPayloadCanonicalV1(
            event, 20260730U, 1U),
        "nonzero event reserve rejected");
    event = Order(1U, 42);
    event.reserved1[0U] =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_SOURCE_TICK_ORDINAL_INVALID_V2;
    *ok &= Expect(
        !ipc::OrderEventDeltaPayloadCanonicalV1(
            event, 20260730U, 1U),
        "source-backed row requires explicit ordinal validity");
    event = Trade(1U, 10);
    event.price_valid = 2U;
    *ok &= Expect(
        !ipc::OrderEventDeltaPayloadCanonicalV1(
            event, 20260730U, 1U),
        "noncanonical boolean rejected");
    event = Trade(1U, 10);
    event.record_bytes = 0U;
    *ok &= Expect(
        !ipc::OrderEventDeltaPayloadCanonicalV1(
            event, 20260730U, 1U),
        "wrong record size rejected");
}

void TestCreateAndOpenValidation(bool* ok) {
    {
        auto config = Config(4U);
        config.run_id = {};
        std::unique_ptr<ipc::OrderEventDeltaRingProducerV1> producer;
        *ok &= Expect(
            ipc::OrderEventDeltaRingProducerV1::Create(
                config, &producer) ==
                ipc::OrderEventDeltaRingCreateErrorV1::
                    kInvalidConfiguration &&
                producer == nullptr,
            "all-zero run identity rejected");
    }
    {
        auto config = Config(4U);
        config.temporal_coverage =
            static_cast<ipc::OrderEventDeltaTemporalCoverageV1>(0U);
        std::unique_ptr<ipc::OrderEventDeltaRingProducerV1> producer;
        *ok &= Expect(
            ipc::OrderEventDeltaRingProducerV1::Create(
                config, &producer) ==
                ipc::OrderEventDeltaRingCreateErrorV1::
                    kInvalidConfiguration &&
                producer == nullptr,
            "unknown temporal coverage is rejected");
    }
    {
        auto config = Config(4U);
        config.maximum_mapping_bytes =
            sizeof(ipc::OrderEventDeltaHeaderV1);
        std::unique_ptr<ipc::OrderEventDeltaRingProducerV1> producer;
        *ok &= Expect(
            ipc::OrderEventDeltaRingProducerV1::Create(
                config, &producer) ==
                ipc::OrderEventDeltaRingCreateErrorV1::
                    kLayoutOverflow,
            "mapping bound enforced");
    }

    auto producer = Producer(4U, ok, 10U);
    if (producer == nullptr) {
        return;
    }
    int descriptor = -1;
    *ok &= Expect(
        producer->DuplicateReadOnlyDescriptor(&descriptor),
        "duplicate descriptor for mismatch test");
    auto wrong = producer->session();
    ++wrong.session_epoch;
    std::unique_ptr<ipc::OrderEventDeltaRingReaderV1> reader;
    *ok &= Expect(
        ipc::OrderEventDeltaRingReaderV1::Open(
            descriptor, wrong, &reader) ==
            ipc::OrderEventDeltaReaderOpenErrorV1::
                kSessionMismatch &&
            reader == nullptr,
        "reader rejects wrong session epoch");
    *ok &= Expect(
        ipc::OrderEventDeltaRingReaderV1::OpenAt(
            descriptor, producer->session(), 0U, &reader) ==
                ipc::OrderEventDeltaReaderOpenErrorV1::
                    kInvalidArgument &&
            reader == nullptr,
        "reader rejects zero construction sequence");
    if (descriptor >= 0) {
        static_cast<void>(::close(descriptor));
    }

    descriptor = -1;
    *ok &= Expect(
        producer->DuplicateReadOnlyDescriptor(&descriptor),
        "duplicate descriptor for temporal mismatch test");
    wrong = producer->session();
    wrong.temporal_coverage =
        ipc::OrderEventDeltaTemporalCoverageV1::kFromProcessStart;
    *ok &= Expect(
        ipc::OrderEventDeltaRingReaderV1::Open(
            descriptor, wrong, &reader) ==
                ipc::OrderEventDeltaReaderOpenErrorV1::
                    kSessionMismatch &&
            reader == nullptr,
        "reader rejects wrong temporal coverage identity");
    if (descriptor >= 0) {
        static_cast<void>(::close(descriptor));
    }

    const int writable =
        ::open("/dev/null", O_RDWR | O_CLOEXEC);
    *ok &= Expect(writable >= 0, "open writable descriptor probe");
    if (writable >= 0) {
        *ok &= Expect(
            ipc::OrderEventDeltaRingReaderV1::Open(
                writable, producer->session(), &reader) ==
                ipc::OrderEventDeltaReaderOpenErrorV1::
                    kDescriptorNotReadOnly,
            "reader rejects writable descriptor");
        static_cast<void>(::close(writable));
    }
}

void TestProcessStartCoverageSession(bool* ok) {
    auto config = Config(8U, 15U);
    config.temporal_coverage =
        ipc::OrderEventDeltaTemporalCoverageV1::kFromProcessStart;
    std::unique_ptr<ipc::OrderEventDeltaRingProducerV1> producer;
    *ok &= Expect(
        ipc::OrderEventDeltaRingProducerV1::Create(
            config, &producer) ==
                ipc::OrderEventDeltaRingCreateErrorV1::kNone &&
            producer != nullptr &&
            producer->session().temporal_coverage ==
                ipc::OrderEventDeltaTemporalCoverageV1::
                    kFromProcessStart &&
            producer->session().stream_quality ==
                ipc::OrderEventDeltaStreamQualityV1::
                    kLocalTickStreamContiguous,
        "process-start coverage is an exact ring-session identity");
    if (producer != nullptr) {
        auto reader = Reader(*producer, ok);
        *ok &= Expect(
            reader != nullptr &&
                reader->session() == producer->session(),
            "reader preserves process-start coverage identity");
    }
}

void TestBasicPublication(bool* ok) {
    auto producer = Producer(8U, ok, 20U);
    if (producer == nullptr) {
        return;
    }
    auto reader = Reader(*producer, ok);
    if (reader == nullptr) {
        return;
    }

    std::array<ipc::OrderEventDeltaPayloadV1, 8U> rows{};
    ipc::OrderEventDeltaReadResultV1 result{};
    *ok &= Expect(
        reader->Read(1U, rows, &result) ==
                ipc::OrderEventDeltaReadErrorV1::kNone &&
            result.written == 0U && result.next_sequence == 1U &&
            result.published_event_sequence == 0U &&
            result.consumed_source_tick_sequence == 0U,
        "empty new ring read");

    *ok &= Expect(
        producer->PublishSourceTick(1U, {}) ==
                ipc::OrderEventDeltaPublishErrorV1::kNone &&
            producer->consumed_source_tick_sequence() == 1U &&
            producer->published_event_sequence() == 0U,
        "zero-event source tick advances source cursor");
    *ok &= Expect(
        reader->Read(1U, rows, &result) ==
                ipc::OrderEventDeltaReadErrorV1::kNone &&
            result.written == 0U &&
            result.consumed_source_tick_sequence == 1U,
        "reader observes zero-event source progress");

    std::array<ipc::OrderEventDeltaPayloadV1, 2U> batch{
        Order(2U, 100),
        Trade(2U, 25)};
    batch[1U].reserved0 = 1U;
    batch[0U].derived_event_sequence = 999U;
    *ok &= Expect(
        producer->PublishSourceTick(2U, batch) ==
                ipc::OrderEventDeltaPublishErrorV1::kNone &&
            producer->published_event_sequence() == 2U &&
            producer->consumed_source_tick_sequence() == 2U,
        "two-event source batch published");
    *ok &= Expect(
        reader->Read(1U, rows, &result) ==
                ipc::OrderEventDeltaReadErrorV1::kNone &&
            result.written == 2U && result.next_sequence == 3U &&
            rows[0U].derived_event_sequence == 1U &&
            rows[1U].derived_event_sequence == 2U &&
            rows[0U].tick_stream_sequence == 2U &&
            rows[1U].tick_stream_sequence == 2U &&
            rows[0U].reserved0 == 0U &&
            rows[1U].reserved0 == 1U &&
            rows[0U].reserved1[0U] ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_SOURCE_TICK_ORDINAL_VALID_V2 &&
            rows[1U].reserved1[0U] ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_SOURCE_TICK_ORDINAL_VALID_V2,
        "reader receives producer-assigned dense event sequence");

    auto invalid = Trade(3U, 10);
    invalid.reserved0 = 1U;
    *ok &= Expect(
        producer->PublishSourceTick(
            3U,
            std::span<const ipc::OrderEventDeltaPayloadV1>(
                &invalid, 1U)) ==
                ipc::OrderEventDeltaPublishErrorV1::
                    kInvalidArgument &&
            producer->consumed_source_tick_sequence() == 2U &&
            producer->published_event_sequence() == 2U,
        "invalid batch rejected before mutation");
    invalid.reserved0 = 0U;
    *ok &= Expect(
        producer->PublishSourceTick(
            3U,
            std::span<const ipc::OrderEventDeltaPayloadV1>(
                &invalid, 1U)) ==
                ipc::OrderEventDeltaPublishErrorV1::kNone,
        "corrected unconsumed source tick can be published");

    *ok &= Expect(
        producer->UpdateHeartbeat(777U) &&
            producer->BeginDraining() &&
            producer->state() ==
                ipc::OrderEventDeltaProducerStateV1::kDraining &&
            producer->PublishSourceTick(4U, {}) ==
                ipc::OrderEventDeltaPublishErrorV1::kUnavailable &&
            producer->StopClean() &&
            producer->state() ==
                ipc::OrderEventDeltaProducerStateV1::kStoppedClean,
        "draining and clean-stop state transitions");
    *ok &= Expect(
        reader->Read(3U, rows, &result) ==
                ipc::OrderEventDeltaReadErrorV1::kNone &&
            result.written == 1U &&
            rows[0U].derived_event_sequence == 3U,
        "clean-stopped ring remains drain-readable");
}

void TestOverrunFailClosed(bool* ok) {
    auto producer = Producer(2U, ok, 30U);
    if (producer == nullptr) {
        return;
    }
    auto reader = Reader(*producer, ok);
    if (reader == nullptr) {
        return;
    }
    for (std::uint64_t sequence = 1U; sequence <= 3U;
         ++sequence) {
        const auto event = Order(
            sequence,
            static_cast<std::int64_t>(100U + sequence));
        *ok &= Expect(
            producer->PublishSourceTick(
                sequence,
                std::span<
                    const ipc::OrderEventDeltaPayloadV1>(
                    &event, 1U)) ==
                ipc::OrderEventDeltaPublishErrorV1::kNone,
            "publish event for overrun");
    }
    std::array<ipc::OrderEventDeltaPayloadV1, 2U> rows{};
    ipc::OrderEventDeltaReadResultV1 result{};
    *ok &= Expect(
        reader->Read(1U, rows, &result) ==
                ipc::OrderEventDeltaReadErrorV1::kOverrun &&
            result.written == 0U && result.next_sequence == 1U &&
            result.observed_sequence == 2U,
        "overrun reports loss without advancing cursor");
    *ok &= Expect(
        reader->Read(2U, rows, &result) ==
            ipc::OrderEventDeltaReadErrorV1::kUnavailable,
        "reader latches terminal state after overrun");

    auto reopened = Reader(*producer, ok);
    if (reopened != nullptr) {
        *ok &= Expect(
            reopened->Read(2U, rows, &result) ==
                    ipc::OrderEventDeltaReadErrorV1::
                        kInvalidArgument &&
                result.next_sequence == 1U,
            "new reader cannot select current oldest as recovery");
        *ok &= Expect(
            reopened->Read(1U, rows, &result) ==
                ipc::OrderEventDeltaReadErrorV1::kOverrun,
            "new reader starts at one and detects retained-prefix loss");
    }

    auto resumed = ReaderAt(*producer, 2U, ok);
    if (resumed != nullptr) {
        *ok &= Expect(
            resumed->Read(1U, rows, &result) ==
                    ipc::OrderEventDeltaReadErrorV1::
                        kInvalidArgument &&
                result.next_sequence == 2U,
            "OpenAt cursor rejects a caller-selected sequence");
        *ok &= Expect(
            resumed->Read(2U, rows, &result) ==
                    ipc::OrderEventDeltaReadErrorV1::kNone &&
                result.written == 2U && result.next_sequence == 4U &&
                rows[0U].derived_event_sequence == 2U &&
                rows[1U].derived_event_sequence == 3U,
            "OpenAt resumes from an explicit retained history boundary");
    }
}

void TestSourceGapAndCapacityFailClosed(bool* ok) {
    {
        auto producer = Producer(4U, ok, 40U);
        if (producer == nullptr) {
            return;
        }
        auto reader = Reader(*producer, ok);
        if (reader == nullptr) {
            return;
        }
        *ok &= Expect(
            producer->PublishSourceTick(1U, {}) ==
                    ipc::OrderEventDeltaPublishErrorV1::kNone &&
                producer->PublishSourceTick(3U, {}) ==
                    ipc::OrderEventDeltaPublishErrorV1::
                        kSourceSequenceGap &&
                producer->state() ==
                    ipc::OrderEventDeltaProducerStateV1::kFailed &&
                producer->PublishSourceTick(2U, {}) ==
                    ipc::OrderEventDeltaPublishErrorV1::
                        kUnavailable,
            "source gap permanently fails producer");
        std::array<ipc::OrderEventDeltaPayloadV1, 1U> rows{};
        ipc::OrderEventDeltaReadResultV1 result{};
        *ok &= Expect(
            reader->Read(1U, rows, &result) ==
                ipc::OrderEventDeltaReadErrorV1::kUnavailable,
            "producer coverage failure fail-closes reader");
    }
    {
        auto producer = Producer(2U, ok, 50U);
        if (producer == nullptr) {
            return;
        }
        const std::array<ipc::OrderEventDeltaPayloadV1, 3U> batch{
            Order(1U, 1),
            Order(1U, 2),
            Order(1U, 3)};
        *ok &= Expect(
            producer->PublishSourceTick(1U, batch) ==
                    ipc::OrderEventDeltaPublishErrorV1::
                        kBatchTooLarge &&
                producer->published_event_sequence() == 0U &&
                producer->consumed_source_tick_sequence() == 0U &&
                producer->state() ==
                    ipc::OrderEventDeltaProducerStateV1::kFailed,
            "unretainable source batch fails before publication");
    }
}

void TestSourceCursorAfterWholeBatch(bool* ok) {
    constexpr std::size_t kBatchSize = 4096U;
    auto producer = Producer(kBatchSize, ok, 60U);
    if (producer == nullptr) {
        return;
    }
    auto reader = Reader(*producer, ok);
    if (reader == nullptr) {
        return;
    }
    std::vector<ipc::OrderEventDeltaPayloadV1> batch;
    batch.reserve(kBatchSize);
    for (std::size_t index = 0U; index < kBatchSize; ++index) {
        batch.push_back(Order(
            1U,
            static_cast<std::int64_t>(index + 1U),
            static_cast<std::uint32_t>(index + 1U)));
        batch.back().reserved0 = static_cast<std::uint32_t>(index);
    }

    std::atomic<bool> started{false};
    std::atomic<bool> finished{false};
    ipc::OrderEventDeltaPublishErrorV1 publish_error =
        ipc::OrderEventDeltaPublishErrorV1::kFailed;
    std::thread publishing([&] {
        started.store(true, std::memory_order_release);
        publish_error = producer->PublishSourceTick(1U, batch);
        finished.store(true, std::memory_order_release);
    });
    while (!started.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    bool coherent = true;
    while (!finished.load(std::memory_order_acquire)) {
        ipc::OrderEventDeltaReadResultV1 result{};
        const auto error = reader->Read(1U, {}, &result);
        if (error != ipc::OrderEventDeltaReadErrorV1::kNone) {
            coherent = false;
            break;
        }
        if (result.published_event_sequence != 0U &&
            result.published_event_sequence != kBatchSize) {
            coherent = false;
            break;
        }
        if (result.consumed_source_tick_sequence == 1U &&
            result.published_event_sequence != kBatchSize) {
            coherent = false;
            break;
        }
        std::this_thread::yield();
    }
    publishing.join();
    ipc::OrderEventDeltaReadResultV1 final_result{};
    const auto final_error = reader->Read(1U, {}, &final_result);
    coherent =
        coherent &&
        publish_error == ipc::OrderEventDeltaPublishErrorV1::kNone &&
        final_error == ipc::OrderEventDeltaReadErrorV1::kNone &&
        final_result.consumed_source_tick_sequence == 1U &&
        final_result.published_event_sequence == kBatchSize;
    *ok &= Expect(
        coherent,
        "acquired source cursor never precedes whole-batch event prefix");
}

void TestCrossProcessReadOnlyMapping(bool* ok) {
    auto producer = Producer(8U, ok, 70U);
    if (producer == nullptr) {
        return;
    }
    const auto event = Order(1U, 700);
    *ok &= Expect(
        producer->PublishSourceTick(
            1U,
            std::span<const ipc::OrderEventDeltaPayloadV1>(
                &event, 1U)) ==
            ipc::OrderEventDeltaPublishErrorV1::kNone,
        "publish cross-process fixture");

    int descriptor = -1;
    *ok &= Expect(
        producer->DuplicateReadOnlyDescriptor(&descriptor),
        "duplicate cross-process descriptor");
    if (descriptor < 0) {
        return;
    }
    constexpr int kRequiredSeals =
        F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_FUTURE_WRITE |
        F_SEAL_SEAL;
    const int seals = ::fcntl(descriptor, F_GET_SEALS);
    *ok &= Expect(
        seals >= 0 && (seals & kRequiredSeals) == kRequiredSeals,
        "reader descriptor carries complete immutable seal set");

    const ipc::OrderEventDeltaSessionV1 session =
        producer->session();
    const pid_t child = ::fork();
    *ok &= Expect(child >= 0, "fork cross-process reader");
    if (child == 0) {
        void* writable = ::mmap(
            nullptr,
            static_cast<std::size_t>(session.total_mapping_bytes),
            PROT_READ | PROT_WRITE,
            MAP_SHARED,
            descriptor,
            0);
        if (writable != MAP_FAILED) {
            static_cast<void>(::munmap(
                writable,
                static_cast<std::size_t>(
                    session.total_mapping_bytes)));
            ::_exit(10);
        }
        if (errno != EACCES && errno != EPERM) {
            ::_exit(11);
        }

        std::unique_ptr<ipc::OrderEventDeltaRingReaderV1> reader;
        if (ipc::OrderEventDeltaRingReaderV1::Open(
                descriptor, session, &reader) !=
                ipc::OrderEventDeltaReaderOpenErrorV1::kNone ||
            reader == nullptr) {
            ::_exit(12);
        }
        std::array<ipc::OrderEventDeltaPayloadV1, 1U> rows{};
        ipc::OrderEventDeltaReadResultV1 result{};
        if (reader->Read(1U, rows, &result) !=
                ipc::OrderEventDeltaReadErrorV1::kNone ||
            result.written != 1U ||
            rows[0U].order_id != 700 ||
            rows[0U].derived_event_sequence != 1U) {
            ::_exit(13);
        }
        ::_exit(0);
    }
    if (child > 0) {
        int status = 0;
        const pid_t waited = ::waitpid(child, &status, 0);
        *ok &= Expect(
            waited == child && WIFEXITED(status) &&
                WEXITSTATUS(status) == 0,
            "forked process maps and reads only through O_RDONLY fd");
    }
    static_cast<void>(::close(descriptor));
}

}  // namespace

int main() {
    bool ok = true;
    TestAbiAndValidation(&ok);
    TestCreateAndOpenValidation(&ok);
    TestProcessStartCoverageSession(&ok);
    TestBasicPublication(&ok);
    TestOverrunFailClosed(&ok);
    TestSourceGapAndCapacityFailClosed(&ok);
    TestSourceCursorAfterWholeBatch(&ok);
    TestCrossProcessReadOnlyMapping(&ok);
    if (!ok) {
        return 1;
    }
    std::cout << "order event delta ring v1 tests passed\n";
    return 0;
}
