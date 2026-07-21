#include "l2flow/ingress/raw_capture_worker.h"
#include "l2flow/ingress/raw_v1.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace ingress = l2flow::ingress;

namespace l2flow::ingress {

class ByteRingTestPeer final {
public:
    static void xor_byte(
        ByteRing& ring,
        std::uint64_t absolute_position,
        std::byte mask) noexcept {
        const std::size_t index =
            static_cast<std::size_t>(
                absolute_position %
                ring.capacity_bytes_);
        ring.storage_[index] ^= mask;
    }
};

}  // namespace l2flow::ingress

namespace {

struct TestContext final {
    void Expect(
        bool condition,
        const std::string& description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    int failures = 0;
};

struct MemoryIoState final {
    std::vector<std::byte> segment;
    std::vector<std::byte> journal;
    std::atomic<std::size_t> maximum_write{
        std::numeric_limits<std::size_t>::max()};
    std::atomic<int> fail_next_write{0};
    std::atomic<bool> zero_next_write{false};
    std::atomic<int> fail_next_segment_sync{0};
    std::atomic<int> fail_next_journal_sync{0};
    std::atomic<int> fail_next_truncate{0};
    std::atomic<int> fail_next_close{0};
    std::atomic<std::uint64_t> segment_syncs{0U};
    std::atomic<std::uint64_t> journal_syncs{0U};
};

class MemoryRawWalIo final : public ingress::RawWalIo {
public:
    explicit MemoryRawWalIo(
        std::shared_ptr<MemoryIoState> state)
        : state_(std::move(state)) {}

    ingress::RawWalWriteResult WritevSome(
        ingress::RawWalFile file,
        std::uint64_t offset,
        std::span<const ingress::RawWalIoVector>
            vectors) noexcept override {
        const int failure =
            state_->fail_next_write.exchange(
                0, std::memory_order_acq_rel);
        if (failure != 0) {
            return {0U, failure};
        }
        if (state_->zero_next_write.exchange(
                false, std::memory_order_acq_rel)) {
            return {};
        }

        std::size_t requested = 0U;
        for (const ingress::RawWalIoVector& vector :
             vectors) {
            if (vector.bytes.size() >
                std::numeric_limits<std::size_t>::max() -
                    requested) {
                return {0U, EOVERFLOW};
            }
            requested += vector.bytes.size();
        }
        const std::size_t completed = std::min(
            requested,
            state_->maximum_write.load(
                std::memory_order_acquire));
        if (offset >
                static_cast<std::uint64_t>(
                    std::numeric_limits<
                        std::size_t>::max()) ||
            completed >
                std::numeric_limits<std::size_t>::max() -
                    static_cast<std::size_t>(offset)) {
            return {0U, EOVERFLOW};
        }

        std::vector<std::byte>& output =
            file == ingress::RawWalFile::kSegment
                ? state_->segment
                : state_->journal;
        const std::size_t begin =
            static_cast<std::size_t>(offset);
        if (output.size() < begin + completed) {
            output.resize(begin + completed);
        }

        std::size_t destination = begin;
        std::size_t remaining = completed;
        for (const ingress::RawWalIoVector& vector :
             vectors) {
            const std::size_t count =
                std::min(remaining, vector.bytes.size());
            std::copy_n(
                vector.bytes.begin(),
                count,
                output.begin() +
                    static_cast<std::ptrdiff_t>(
                        destination));
            destination += count;
            remaining -= count;
            if (remaining == 0U) {
                break;
            }
        }
        return {completed, 0};
    }

    int Fdatasync(
        ingress::RawWalFile file) noexcept override {
        if (file == ingress::RawWalFile::kSegment) {
            state_->segment_syncs.fetch_add(
                1U, std::memory_order_relaxed);
            return state_->fail_next_segment_sync.exchange(
                0, std::memory_order_acq_rel);
        }
        state_->journal_syncs.fetch_add(
            1U, std::memory_order_relaxed);
        return state_->fail_next_journal_sync.exchange(
            0, std::memory_order_acq_rel);
    }

    int Truncate(
        ingress::RawWalFile file,
        std::uint64_t logical_size) noexcept override {
        const int failure =
            state_->fail_next_truncate.exchange(
                0, std::memory_order_acq_rel);
        if (failure != 0) {
            return failure;
        }
        if (file != ingress::RawWalFile::kSegment ||
            logical_size >
                static_cast<std::uint64_t>(
                    std::numeric_limits<
                        std::size_t>::max())) {
            return EINVAL;
        }
        state_->segment.resize(
            static_cast<std::size_t>(logical_size));
        return 0;
    }

    int Close(
        ingress::RawWalFile) noexcept override {
        return state_->fail_next_close.exchange(
            0, std::memory_order_acq_rel);
    }

private:
    std::shared_ptr<MemoryIoState> state_;
};

struct FailureState final {
    std::atomic<std::uint64_t> calls{0U};
    std::atomic<std::uint8_t> signal{
        static_cast<std::uint8_t>(
            ingress::RawCaptureFatalSignal::
                kRawWalIo)};
};

void OnFailure(
    void* context,
    ingress::RawCaptureFatalSignal signal) noexcept {
    auto* const state =
        static_cast<FailureState*>(context);
    state->signal.store(
        static_cast<std::uint8_t>(signal),
        std::memory_order_relaxed);
    state->calls.fetch_add(
        1U, std::memory_order_release);
}

struct TestClock final {
    std::atomic<std::uint64_t> now_ns{100U};
};

std::uint64_t ReadClock(void* context) noexcept {
    return static_cast<TestClock*>(context)
        ->now_ns.load(std::memory_order_acquire);
}

template <typename Predicate>
bool WaitUntil(Predicate predicate) {
    constexpr std::size_t kMaximumAttempts =
        1'000'000U;
    for (std::size_t attempt = 0U;
         attempt < kMaximumAttempts;
         ++attempt) {
        if (predicate()) {
            return true;
        }
        std::this_thread::yield();
    }
    return false;
}

void StoreU16(
    std::uint16_t value,
    std::byte* output) {
    output[0] =
        static_cast<std::byte>(value & 0xffU);
    output[1] =
        static_cast<std::byte>(
            (value >> 8U) & 0xffU);
}

void StoreU32(
    std::uint32_t value,
    std::byte* output) {
    for (std::size_t index = 0U; index < 4U;
         ++index) {
        output[index] = static_cast<std::byte>(
            (value >>
             static_cast<unsigned int>(index * 8U)) &
            0xffU);
    }
}

void StoreU64(
    std::uint64_t value,
    std::byte* output) {
    for (std::size_t index = 0U; index < 8U;
         ++index) {
        output[index] = static_cast<std::byte>(
            (value >>
             static_cast<unsigned int>(index * 8U)) &
            0xffU);
    }
}

void FillIdentity(
    ingress::RawV1Identity* identity,
    std::uint8_t seed) {
    for (std::size_t index = 0U;
         index < identity->size();
         ++index) {
        (*identity)[index] =
            static_cast<std::byte>(
                static_cast<std::uint8_t>(
                    seed +
                    static_cast<std::uint8_t>(
                        index)));
    }
}

void FillDigest(
    ingress::RawV1Digest* digest,
    std::uint8_t seed) {
    for (std::size_t index = 0U;
         index < digest->size();
         ++index) {
        (*digest)[index] =
            static_cast<std::byte>(
                static_cast<std::uint8_t>(
                    seed +
                    static_cast<std::uint8_t>(
                        index)));
    }
}

ingress::RawWalWriterConfig MakeWriterConfig() {
    ingress::SegmentHeaderV1 segment;
    segment.source_stream_id = 2002U;
    segment.capture_date = 20260718U;
    FillIdentity(&segment.stream_day_id, 0x10U);
    segment.segment_sequence = 1U;
    segment.segment_base_wal_pos = 0U;
    segment.first_ingress_sequence = 1U;
    segment.created_realtime_ns = 1000U;
    segment.created_monotonic_ns = 2000U;
    FillIdentity(&segment.host_uuid, 0x30U);
    FillIdentity(&segment.linux_boot_id, 0x50U);
    segment.clock_epoch_algorithm = 1U;
    FillDigest(&segment.clock_epoch_digest, 0x70U);
    segment.clock_epoch_label = 0x1234U;
    FillDigest(&segment.sdk_archive_sha256, 0x80U);
    FillDigest(&segment.libmdl_api_sha256, 0x90U);
    FillDigest(
        &segment.endpoint_contract_sha256, 0xa0U);
    FillDigest(&segment.config_sha256, 0xb0U);
    FillDigest(&segment.raw_schema_sha256, 0xc0U);
    FillDigest(&segment.build_manifest_sha256, 0xd0U);

    ingress::DurableJournalHeaderV1 journal;
    journal.capture_date = segment.capture_date;
    journal.source_stream_id =
        segment.source_stream_id;
    journal.stream_day_id = segment.stream_day_id;
    journal.raw_schema_sha256 =
        segment.raw_schema_sha256;
    journal.created_host_uuid = segment.host_uuid;
    journal.created_linux_boot_id =
        segment.linux_boot_id;
    journal.created_clock_epoch_algorithm =
        segment.clock_epoch_algorithm;
    journal.created_clock_epoch_digest =
        segment.clock_epoch_digest;
    journal.created_clock_epoch_label =
        segment.clock_epoch_label;

    ingress::RawWalWriterConfig config;
    if (ingress::EncodeSegmentHeaderV1(
            segment, &config.segment_header_wire) !=
            ingress::RawV1Error::kNone ||
        ingress::EncodeDurableJournalHeaderV1(
            journal, &config.journal_header_wire) !=
            ingress::RawV1Error::kNone) {
        throw std::runtime_error(
            "failed to encode Raw worker fixture");
    }
    config.source_stream_id =
        segment.source_stream_id;
    config.capture_date = segment.capture_date;
    config.segment_sequence =
        segment.segment_sequence;
    config.segment_base_wal_pos =
        segment.segment_base_wal_pos;
    config.first_ingress_sequence =
        segment.first_ingress_sequence;
    config.initial_durable_ingress_sequence = 0U;
    return config;
}

struct RecordFixture final {
    ingress::CaptureMetaV1 meta{};
    std::array<
        std::byte,
        ingress::kVendorMessageHeadBytes> head{};
    std::vector<std::byte> body;
};

RecordFixture MakeRecord(
    std::uint64_t ingress_sequence,
    std::size_t body_size) {
    RecordFixture record;
    record.meta.source_stream_id = 2002U;
    record.meta.connection_epoch_hint = 7U;
    record.meta.ingress_sequence = ingress_sequence;
    record.meta.recv_realtime_ns =
        1'000'000U + ingress_sequence;
    record.meta.recv_monotonic_ns =
        2'000'000U + ingress_sequence;
    record.meta.capture_date = 20260718U;
    record.meta.flags = 0U;
    record.head[0] = static_cast<std::byte>(
        ingress::kVendorMessageHeadBytes);
    StoreU32(
        static_cast<std::uint32_t>(
            record.head.size() + body_size),
        record.head.data() + 1U);
    record.head[5] = std::byte{1U};
    record.head[6] = std::byte{6U};
    StoreU16(101U, record.head.data() + 7U);
    StoreU16(36U, record.head.data() + 9U);
    StoreU32(
        93000123U, record.head.data() + 11U);
    StoreU64(
        9000U + ingress_sequence,
        record.head.data() + 15U);
    record.body.resize(body_size);
    for (std::size_t index = 0U;
         index < body_size;
         ++index) {
        record.body[index] =
            static_cast<std::byte>(
                static_cast<std::uint8_t>(
                    index * 7U +
                    static_cast<std::size_t>(
                        ingress_sequence)));
    }
    return record;
}

bool Push(
    ingress::ByteRing* ring,
    const RecordFixture& record) {
    return ring->try_push_copy(
               record.meta,
               record.head,
               record.body) ==
           ingress::ByteRingPushResult::PUBLISHED;
}

std::uint64_t FramedBytes(
    const RecordFixture& record) {
    ingress::RawRecordLayoutV1 layout;
    if (ingress::ComputeRawRecordLayoutV1(
            record.body.size(), &layout) !=
        ingress::RawV1Error::kNone) {
        throw std::runtime_error(
            "failed to compute Raw worker fixture layout");
    }
    return layout.record_size;
}

std::uint64_t VendorBytes(
    const RecordFixture& record) {
    return static_cast<std::uint64_t>(
        record.head.size() + record.body.size());
}

bool MatchesPrefix(
    const ingress::RawCaptureProgress& progress,
    const std::vector<RecordFixture>& records) {
    if (progress.records > records.size()) {
        return false;
    }
    std::uint64_t vendor_bytes = 0U;
    std::uint64_t framed_bytes = 0U;
    for (std::size_t index = 0U;
         index <
         static_cast<std::size_t>(progress.records);
         ++index) {
        vendor_bytes += VendorBytes(records[index]);
        framed_bytes += FramedBytes(records[index]);
    }
    const std::uint64_t last_sequence =
        progress.records == 0U
            ? 0U
            : records[static_cast<std::size_t>(
                          progress.records - 1U)]
                  .meta.ingress_sequence;
    return progress.vendor_bytes == vendor_bytes &&
           progress.framed_wal_bytes == framed_bytes &&
           progress.last_ingress_sequence ==
               last_sequence;
}

ingress::RawCaptureWorkerConfig MakeWorkerConfig(
    FailureState* failure,
    TestClock* clock,
    std::uint64_t interval_ns,
    std::uint64_t batch_bytes) {
    ingress::RawCaptureWorkerConfig config;
    config.durable_interval_ns = interval_ns;
    config.durable_batch_bytes = batch_bytes;
    config.failure_sink = {&OnFailure, failure};
    config.monotonic_now = &ReadClock;
    config.monotonic_clock_context = clock;
    return config;
}

void TestTimeThresholdDrainAndShortWrites(
    TestContext* test) {
    auto io_state = std::make_shared<MemoryIoState>();
    io_state->maximum_write.store(
        7U, std::memory_order_release);
    ingress::RawWalWriter writer(
        MakeWriterConfig(),
        std::make_unique<MemoryRawWalIo>(io_state));
    test->Expect(
        writer.Initialize(),
        "short writes are retried during writer initialization");

    ingress::ByteRing ring(8192U, 1024U);
    FailureState failure;
    TestClock clock;
    ingress::RawCaptureWorker worker(
        MakeWorkerConfig(
            &failure,
            &clock,
            10U,
            1'000'000U),
        ring,
        writer);
    const std::vector<RecordFixture> records{
        MakeRecord(1U, 13U),
        MakeRecord(2U, 21U),
        MakeRecord(3U, 0U),
        MakeRecord(4U, 55U)};
    std::atomic<bool> run_result{false};
    std::thread consumer([&] {
        run_result.store(
            worker.Run(), std::memory_order_release);
    });
    std::atomic<bool> observe{true};
    std::atomic<bool> incoherent_snapshot{false};
    std::thread observer([&] {
        while (observe.load(std::memory_order_acquire)) {
            const ingress::RawCaptureWorkerSnapshot snapshot =
                worker.Snapshot();
            if (!MatchesPrefix(snapshot.append, records) ||
                !MatchesPrefix(snapshot.durable, records) ||
                snapshot.durable.records >
                    snapshot.append.records ||
                snapshot.durable.vendor_bytes >
                    snapshot.append.vendor_bytes ||
                snapshot.durable.framed_wal_bytes >
                    snapshot.append.framed_wal_bytes) {
                incoherent_snapshot.store(
                    true, std::memory_order_release);
                return;
            }
        }
    });
    test->Expect(
        WaitUntil([&] {
            return worker.startup_complete();
        }) &&
            worker.startup_succeeded(),
        "worker starts only on an initialized writer");

    for (std::size_t index = 0U; index < 3U; ++index) {
        test->Expect(
            Push(&ring, records[index]),
            "multi-record input is published");
    }
    test->Expect(
        WaitUntil([&] {
            return worker.Snapshot().append.records ==
                   3U;
        }),
        "worker appends all records");

    ingress::RawCaptureWorkerSnapshot snapshot =
        worker.Snapshot();
    test->Expect(
        snapshot.durable.records == 0U &&
            snapshot.durable.vendor_bytes == 0U &&
            snapshot.durable.framed_wal_bytes == 0U &&
            writer.Snapshot()
                    .durable.ingress_sequence == 0U,
        "append visibility does not publish durability early");

    clock.now_ns.store(110U, std::memory_order_release);
    test->Expect(
        WaitUntil([&] {
            return worker.Snapshot().durable.records ==
                   3U;
        }),
        "10 ns test interval triggers the durable batch");

    test->Expect(
        Push(&ring, records[3U]),
        "final record is published before producer quiescence");
    // This release is made only after the only producer has returned.
    worker.StopAndDrain();
    consumer.join();
    observe.store(false, std::memory_order_release);
    observer.join();

    std::uint64_t expected_vendor_bytes = 0U;
    std::uint64_t expected_framed_bytes = 0U;
    for (const RecordFixture& record : records) {
        expected_vendor_bytes += VendorBytes(record);
        expected_framed_bytes += FramedBytes(record);
    }
    snapshot = worker.Snapshot();
    test->Expect(
        run_result.load(std::memory_order_acquire) &&
            worker.finished() &&
            snapshot.failure_kind ==
                ingress::RawCaptureWorkerFailureKind::
                    kNone &&
            failure.calls.load(
                std::memory_order_acquire) == 0U,
        "double-EMPTY stop path finishes cleanly");
    test->Expect(
        !incoherent_snapshot.load(
            std::memory_order_acquire),
        "concurrent snapshots never expose torn progress");
    test->Expect(
        snapshot.append ==
                ingress::RawCaptureProgress{
                    records.size(),
                    expected_vendor_bytes,
                    expected_framed_bytes,
                    4U} &&
            snapshot.durable == snapshot.append,
        "append and durable progress reconcile exactly");

    ingress::CaptureMetricsSnapshot callback;
    callback.captured_records = records.size();
    callback.captured_vendor_bytes =
        expected_vendor_bytes;
    test->Expect(
        worker.Reconcile(callback).exact(),
        "callback and final durable capture reconcile");
    const ingress::RawWalWriterSnapshot sealed =
        writer.Snapshot();
    test->Expect(
        sealed.sealed && sealed.closed &&
            sealed.append == sealed.durable &&
            sealed.durable.ingress_sequence == 4U,
        "clean drain flushes, seals, and closes the writer");
}

void TestByteThreshold(TestContext* test) {
    auto io_state = std::make_shared<MemoryIoState>();
    ingress::RawWalWriter writer(
        MakeWriterConfig(),
        std::make_unique<MemoryRawWalIo>(io_state));
    test->Expect(
        writer.Initialize(),
        "byte-threshold writer initializes");

    ingress::ByteRing ring(4096U, 512U);
    FailureState failure;
    TestClock clock;
    const RecordFixture first = MakeRecord(1U, 17U);
    const RecordFixture second = MakeRecord(2U, 17U);
    const std::uint64_t record_bytes =
        FramedBytes(first);
    ingress::RawCaptureWorker worker(
        MakeWorkerConfig(
            &failure,
            &clock,
            1'000'000U,
            record_bytes * 2U),
        ring,
        writer);
    std::atomic<bool> run_result{false};
    std::thread consumer([&] {
        run_result.store(
            worker.Run(), std::memory_order_release);
    });
    test->Expect(
        WaitUntil([&] {
            return worker.startup_complete();
        }),
        "byte-threshold worker starts");
    test->Expect(
        Push(&ring, first) &&
            WaitUntil([&] {
                return worker.Snapshot()
                           .append.records == 1U;
            }),
        "first threshold record appends");
    test->Expect(
        worker.Snapshot().durable.records == 0U,
        "one record remains below the byte threshold");
    test->Expect(
        Push(&ring, second) &&
            WaitUntil([&] {
                return worker.Snapshot()
                           .durable.records == 2U;
            }),
        "second record reaches the exact byte threshold");

    worker.StopAndDrain();
    consumer.join();
    test->Expect(
        run_result.load(std::memory_order_acquire) &&
            worker.Snapshot().durable.records == 2U &&
            failure.calls.load(
                std::memory_order_acquire) == 0U,
        "byte-threshold run seals cleanly");
}

void TestRingCorruption(TestContext* test) {
    auto io_state = std::make_shared<MemoryIoState>();
    ingress::RawWalWriter writer(
        MakeWriterConfig(),
        std::make_unique<MemoryRawWalIo>(io_state));
    test->Expect(
        writer.Initialize(),
        "corruption-test writer initializes");

    ingress::ByteRing ring(4096U, 512U);
    const RecordFixture record = MakeRecord(1U, 8U);
    test->Expect(
        Push(&ring, record),
        "corruption-test record is initially valid");
    l2flow::ingress::ByteRingTestPeer::xor_byte(
        ring,
        sizeof(ingress::CaptureMetaV1),
        std::byte{0xffU});

    FailureState failure;
    TestClock clock;
    ingress::RawCaptureWorker worker(
        MakeWorkerConfig(
            &failure,
            &clock,
            10U,
            1024U),
        ring,
        writer);
    std::atomic<bool> run_result{true};
    std::thread consumer([&] {
        run_result.store(
            worker.Run(), std::memory_order_release);
    });
    consumer.join();
    const ingress::RawCaptureWorkerSnapshot snapshot =
        worker.Snapshot();
    test->Expect(
        !run_result.load(std::memory_order_acquire) &&
            snapshot.finished &&
            snapshot.failure_kind ==
                ingress::RawCaptureWorkerFailureKind::
                    kRingCorruption &&
            snapshot.append.records == 0U &&
            failure.calls.load(
                std::memory_order_acquire) == 1U &&
            failure.signal.load(
                std::memory_order_relaxed) ==
                static_cast<std::uint8_t>(
                    ingress::RawCaptureFatalSignal::
                        kRingCorruption),
        "ring corruption trips the dedicated fatal signal");
    test->Expect(
        !writer.Snapshot().sealed,
        "a corrupt drain is never published as a clean seal");
}

void TestWriterFailures(TestContext* test) {
    {
        auto io_state =
            std::make_shared<MemoryIoState>();
        ingress::RawWalWriter writer(
            MakeWriterConfig(),
            std::make_unique<MemoryRawWalIo>(
                io_state));
        test->Expect(
            writer.Initialize(),
            "append-failure writer initializes");
        io_state->fail_next_write.store(
            EIO, std::memory_order_release);

        ingress::ByteRing ring(4096U, 512U);
        FailureState failure;
        TestClock clock;
        ingress::RawCaptureWorker worker(
            MakeWorkerConfig(
                &failure,
                &clock,
                10U,
                1024U),
            ring,
            writer);
        const RecordFixture record =
            MakeRecord(1U, 9U);
        test->Expect(
            Push(&ring, record),
            "append-failure record is published");
        const bool result = worker.Run();
        const ingress::RawCaptureWorkerSnapshot snapshot =
            worker.Snapshot();
        test->Expect(
            !result &&
                snapshot.failure_kind ==
                    ingress::
                        RawCaptureWorkerFailureKind::
                            kWriterAppend &&
                snapshot.writer_failure.kind ==
                    ingress::RawWalFailureKind::
                        kSegmentWrite &&
                snapshot.writer_failure.error_number ==
                    EIO &&
                snapshot.append.records == 0U &&
                snapshot.durable.records == 0U &&
                failure.calls.load(
                    std::memory_order_acquire) == 1U &&
                failure.signal.load(
                    std::memory_order_relaxed) ==
                    static_cast<std::uint8_t>(
                        ingress::
                            RawCaptureFatalSignal::
                                kRawWalIo),
            "append I/O failure trips RAW_WAL_IO once");
    }

    {
        auto io_state =
            std::make_shared<MemoryIoState>();
        ingress::RawWalWriter writer(
            MakeWriterConfig(),
            std::make_unique<MemoryRawWalIo>(
                io_state));
        test->Expect(
            writer.Initialize(),
            "flush-failure writer initializes");

        ingress::ByteRing ring(4096U, 512U);
        FailureState failure;
        TestClock clock;
        ingress::RawCaptureWorker worker(
            MakeWorkerConfig(
                &failure,
                &clock,
                10U,
                1024U * 1024U),
            ring,
            writer);
        std::atomic<bool> run_result{true};
        std::thread consumer([&] {
            run_result.store(
                worker.Run(),
                std::memory_order_release);
        });
        test->Expect(
            WaitUntil([&] {
                return worker.startup_complete();
            }),
            "flush-failure worker starts");
        const RecordFixture record =
            MakeRecord(1U, 11U);
        test->Expect(
            Push(&ring, record) &&
                WaitUntil([&] {
                    return worker.Snapshot()
                               .append.records == 1U;
                }),
            "flush-failure record reaches append");
        io_state->fail_next_segment_sync.store(
            EIO, std::memory_order_release);
        clock.now_ns.store(
            110U, std::memory_order_release);
        test->Expect(
            WaitUntil([&] {
                return worker.finished();
            }),
            "durability failure terminates the worker");
        consumer.join();

        const ingress::RawCaptureWorkerSnapshot snapshot =
            worker.Snapshot();
        test->Expect(
            !run_result.load(std::memory_order_acquire) &&
                snapshot.failure_kind ==
                    ingress::
                        RawCaptureWorkerFailureKind::
                            kWriterFlush &&
                snapshot.writer_failure.kind ==
                    ingress::RawWalFailureKind::
                        kSegmentSync &&
                snapshot.append.records == 1U &&
                snapshot.durable.records == 0U &&
                writer.Snapshot()
                        .durable.ingress_sequence == 0U &&
                failure.calls.load(
                    std::memory_order_acquire) == 1U,
            "failed flush never advances durable publication");
    }
}

}  // namespace

int main() {
    TestContext test;
    TestTimeThresholdDrainAndShortWrites(&test);
    TestByteThreshold(&test);
    TestRingCorruption(&test);
    TestWriterFailures(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Raw capture worker checks failed\n";
        return 1;
    }
    std::cout
        << "Phase-2 Raw capture worker checks passed\n";
    return 0;
}
