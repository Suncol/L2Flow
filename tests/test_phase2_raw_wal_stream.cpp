#include "l2flow/ingress/raw_v1.h"
#include "l2flow/ingress/raw_capture_worker.h"
#include "l2flow/ingress/raw_wal_stream.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace ingress = l2flow::ingress;

namespace {

struct TestContext final {
    void Expect(bool condition, const std::string& description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }
    int failures = 0;
};

template <std::size_t Size>
void Fill(
    std::array<std::byte, Size>* bytes,
    std::uint8_t seed) {
    for (std::size_t index = 0U;
         index < bytes->size();
         ++index) {
        (*bytes)[index] =
            static_cast<std::byte>(
                static_cast<std::uint8_t>(
                    seed +
                    static_cast<std::uint8_t>(index)));
    }
}

void StoreU16(
    std::uint16_t value,
    std::byte* output) {
    output[0U] =
        static_cast<std::byte>(value & 0xffU);
    output[1U] =
        static_cast<std::byte>((value >> 8U) & 0xffU);
}

void StoreU32(
    std::uint32_t value,
    std::byte* output) {
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        output[index] =
            static_cast<std::byte>(
                (value >>
                 static_cast<unsigned int>(
                     index * 8U)) &
                0xffU);
    }
}

void StoreU64(
    std::uint64_t value,
    std::byte* output) {
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        output[index] =
            static_cast<std::byte>(
                (value >>
                 static_cast<unsigned int>(
                     index * 8U)) &
                0xffU);
    }
}

struct RecordFixture final {
    ingress::CaptureMetaV1 meta{};
    ingress::RawV1VendorHead head{};
    std::vector<std::byte> body;

    [[nodiscard]] ingress::RawWalRecordInputV1 input()
        const noexcept {
        return {meta, head, body};
    }
};

RecordFixture MakeRecord(std::uint64_t sequence) {
    RecordFixture record{};
    record.meta.source_stream_id = 2002U;
    record.meta.connection_epoch_hint = 7U;
    record.meta.ingress_sequence = sequence;
    record.meta.recv_realtime_ns =
        1'000U + sequence;
    record.meta.recv_monotonic_ns =
        900U + sequence;
    record.meta.capture_date = 20260718U;
    record.body.assign(13U, std::byte{0x5a});
    record.head[0U] = static_cast<std::byte>(
        ingress::kVendorMessageHeadBytes);
    StoreU32(
        static_cast<std::uint32_t>(
            ingress::kVendorMessageHeadBytes +
            record.body.size()),
        record.head.data() + 1U);
    record.head[5U] = std::byte{1U};
    record.head[6U] = std::byte{6U};
    StoreU16(101U, record.head.data() + 7U);
    StoreU16(36U, record.head.data() + 9U);
    StoreU32(93000123U, record.head.data() + 11U);
    StoreU64(
        9000U + sequence,
        record.head.data() + 15U);
    return record;
}

std::uint64_t FixtureRecordBytes() {
    ingress::RawRecordLayoutV1 layout{};
    if (ingress::ComputeRawRecordLayoutV1(
            13U, &layout) !=
        ingress::RawV1Error::kNone) {
        return 0U;
    }
    return layout.record_size;
}

struct SharedFiles final {
    std::vector<
        std::shared_ptr<std::vector<std::byte>>>
        segments;
    std::shared_ptr<std::vector<std::byte>> journal =
        std::make_shared<std::vector<std::byte>>();
};

class VectorWalIo final : public ingress::RawWalIo {
public:
    VectorWalIo(
        std::shared_ptr<std::vector<std::byte>> segment,
        std::shared_ptr<std::vector<std::byte>> journal)
        : segment_(std::move(segment)),
          journal_(std::move(journal)) {}

    ingress::RawWalWriteResult WritevSome(
        ingress::RawWalFile file,
        std::uint64_t offset,
        std::span<const ingress::RawWalIoVector>
            vectors) noexcept override {
        if (closed(file) ||
            offset >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max())) {
            return {0U, EBADF};
        }
        std::size_t total = 0U;
        for (const auto& vector : vectors) {
            if (vector.bytes.size() >
                std::numeric_limits<std::size_t>::max() -
                    total) {
                return {0U, EOVERFLOW};
            }
            total += vector.bytes.size();
        }
        const std::size_t start =
            static_cast<std::size_t>(offset);
        if (total >
            std::numeric_limits<std::size_t>::max() -
                start) {
            return {0U, EOVERFLOW};
        }
        std::vector<std::byte>& destination =
            file == ingress::RawWalFile::kSegment
                ? *segment_
                : *journal_;
        if (destination.size() < start + total) {
            try {
                destination.resize(start + total);
            } catch (...) {
                return {0U, ENOMEM};
            }
        }
        std::size_t write_offset = start;
        for (const auto& vector : vectors) {
            std::copy(
                vector.bytes.begin(),
                vector.bytes.end(),
                destination.begin() +
                    static_cast<std::ptrdiff_t>(
                        write_offset));
            write_offset += vector.bytes.size();
        }
        return {total, 0};
    }

    int Fdatasync(ingress::RawWalFile file) noexcept override {
        return closed(file) ? EBADF : 0;
    }

    int Truncate(
        ingress::RawWalFile file,
        std::uint64_t size) noexcept override {
        if (file != ingress::RawWalFile::kSegment ||
            segment_closed_ ||
            size >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max())) {
            return EBADF;
        }
        try {
            segment_->resize(
                static_cast<std::size_t>(size));
            return 0;
        } catch (...) {
            return ENOMEM;
        }
    }

    int Close(ingress::RawWalFile file) noexcept override {
        if (file == ingress::RawWalFile::kSegment) {
            segment_closed_ = true;
        } else {
            journal_closed_ = true;
        }
        return 0;
    }

private:
    [[nodiscard]] bool closed(
        ingress::RawWalFile file) const noexcept {
        return file == ingress::RawWalFile::kSegment
                   ? segment_closed_
                   : journal_closed_;
    }

    std::shared_ptr<std::vector<std::byte>> segment_;
    std::shared_ptr<std::vector<std::byte>> journal_;
    bool segment_closed_ = false;
    bool journal_closed_ = false;
};

struct ConfigFixture final {
    ingress::RawWalWriterConfig config{};
    ingress::SegmentHeaderV1 header{};
};

ConfigFixture MakeConfig() {
    ConfigFixture fixture{};
    ingress::SegmentHeaderV1& segment = fixture.header;
    segment.source_stream_id = 2002U;
    segment.capture_date = 20260718U;
    Fill(&segment.stream_day_id, 0x10U);
    segment.segment_sequence = 1U;
    segment.first_ingress_sequence = 1U;
    segment.created_realtime_ns = 1000U;
    segment.created_monotonic_ns = 900U;
    Fill(&segment.host_uuid, 0x30U);
    Fill(&segment.linux_boot_id, 0x50U);
    segment.clock_epoch_algorithm = 1U;
    Fill(&segment.clock_epoch_digest, 0x70U);
    segment.clock_epoch_label = 77U;
    Fill(&segment.sdk_archive_sha256, 0x80U);
    Fill(&segment.libmdl_api_sha256, 0x90U);
    Fill(&segment.endpoint_contract_sha256, 0xa0U);
    Fill(&segment.config_sha256, 0xb0U);
    Fill(&segment.raw_schema_sha256, 0xc0U);
    Fill(&segment.build_manifest_sha256, 0xd0U);

    ingress::DurableJournalHeaderV1 journal{};
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

    static_cast<void>(
        ingress::EncodeSegmentHeaderV1(
            segment,
            &fixture.config.segment_header_wire));
    static_cast<void>(
        ingress::EncodeDurableJournalHeaderV1(
            journal,
            &fixture.config.journal_header_wire));
    fixture.config.source_stream_id =
        segment.source_stream_id;
    fixture.config.capture_date =
        segment.capture_date;
    fixture.config.segment_sequence = 1U;
    fixture.config.first_ingress_sequence = 1U;
    return fixture;
}

struct Clock final {
    static std::uint64_t Now(void* context) noexcept {
        return static_cast<Clock*>(context)->now;
    }
    std::uint64_t now = 100U;
};

class RecordingBackend final
    : public ingress::RawWalStreamBackendV1 {
public:
    RecordingBackend(
        ConfigFixture base,
        std::shared_ptr<SharedFiles> files)
        : base_(std::move(base)),
          files_(std::move(files)) {}

    bool PublishClosedSegment(
        ingress::RawSegmentArtifactPlanV1 plan,
        const ingress::RawWalWriterSnapshot& snapshot)
        noexcept override {
        events.push_back("closed-" +
            std::to_string(
                plan.metadata.segment.segment_sequence));
        if (fail_closed || !plan.ok() ||
            !snapshot.sealed || !snapshot.closed ||
            snapshot.append != snapshot.durable) {
            return false;
        }
        closed_plans.push_back(std::move(plan));
        return true;
    }

    bool CreateNextSegment(
        const ingress::RawWalRotationPlan& rotation,
        std::uint64_t opened_monotonic_ns,
        ingress::RawWalNextSegmentBootstrapV1* output)
        noexcept override {
        events.push_back("create-" +
            std::to_string(
                rotation.next_segment_sequence));
        if (fail_create || output == nullptr) {
            return false;
        }
        ingress::SegmentHeaderV1 header = base_.header;
        header.segment_sequence =
            rotation.next_segment_sequence;
        header.segment_base_wal_pos =
            rotation.next_segment_base_wal_pos;
        header.first_ingress_sequence =
            rotation.next_first_ingress_sequence;
        header.created_monotonic_ns =
            opened_monotonic_ns;
        header.created_realtime_ns +=
            rotation.next_segment_sequence;

        ingress::RawWalWriterConfig config =
            base_.config;
        if (ingress::EncodeSegmentHeaderV1(
                header,
                &config.segment_header_wire) !=
            ingress::RawV1Error::kNone) {
            return false;
        }
        config.segment_sequence =
            rotation.next_segment_sequence;
        config.segment_base_wal_pos =
            rotation.next_segment_base_wal_pos;
        config.first_ingress_sequence =
            rotation.next_first_ingress_sequence;
        config.initial_durable_ingress_sequence =
            rotation.initial_durable_ingress_sequence;
        config.initialization_mode =
            ingress::RawWalInitializationMode::
                kExistingJournal;
        config.existing_journal =
            rotation.existing_journal;
        config.headers_already_persisted = true;

        auto segment =
            std::make_shared<std::vector<std::byte>>(
                config.segment_header_wire.begin(),
                config.segment_header_wire.end());
        files_->segments.push_back(segment);
        output->writer_config = config;
        output->io =
            std::make_unique<VectorWalIo>(
                segment, files_->journal);
        return true;
    }

    bool PublishOpenManifest(
        const ingress::SegmentHeaderV1& segment,
        const ingress::RawWalWriterSnapshot& snapshot)
        noexcept override {
        events.push_back("open-" +
            std::to_string(segment.segment_sequence));
        return !fail_open &&
               snapshot.initialized &&
               snapshot.append == snapshot.durable;
    }

    bool PublishControl(
        const ingress::SegmentHeaderV1& segment,
        const ingress::RawWalWriterSnapshot& snapshot)
        noexcept override {
        events.push_back("control-" +
            std::to_string(segment.segment_sequence));
        return !fail_control &&
               snapshot.initialized &&
               !snapshot.fatal;
    }

    ConfigFixture base_;
    std::shared_ptr<SharedFiles> files_;
    std::vector<std::string> events;
    std::vector<ingress::RawSegmentArtifactPlanV1>
        closed_plans;
    bool fail_closed = false;
    bool fail_create = false;
    bool fail_open = false;
    bool fail_control = false;
};

struct CaptureFailure final {
    static void Notify(
        void* context,
        ingress::RawCaptureFatalSignal) noexcept {
        ++static_cast<CaptureFailure*>(context)->calls;
    }
    std::uint64_t calls = 0U;
};

struct StreamFixture final {
    StreamFixture(
        std::uint64_t target_bytes,
        std::uint64_t max_age)
        : config(MakeConfig()),
          files(std::make_shared<SharedFiles>()),
          backend(config, files) {
        auto segment =
            std::make_shared<std::vector<std::byte>>();
        files->segments.push_back(segment);
        ingress::RawSegmentArtifactOptionsV1 options{};
        options.expected_raw_schema_sha256 =
            config.header.raw_schema_sha256;
        options.sample_record_interval = 2U;
        options.sample_raw_bytes_interval =
            ingress::kRawIndexV1DefaultRawBytesInterval;
        options.maximum_segment_bytes = 1U << 20U;

        ingress::RawRecordLayoutV1 layout{};
        static_cast<void>(
            ingress::ComputeRawRecordLayoutV1(
                MakeRecord(1U).body.size(),
                &layout));
        record_bytes = layout.record_size;
        ingress::RawWalStreamLimitsV1 limits{};
        limits.segment_target_bytes = target_bytes;
        limits.segment_max_age_ns = max_age;
        limits.maximum_record_bytes = record_bytes;
        limits.monotonic_now = &Clock::Now;
        limits.monotonic_clock_context = &clock;
        stream =
            std::make_unique<ingress::RawWalStreamWriter>(
                config.config,
                std::make_unique<VectorWalIo>(
                    segment, files->journal),
                options,
                limits,
                backend);
    }

    ConfigFixture config;
    std::shared_ptr<SharedFiles> files;
    Clock clock;
    RecordingBackend backend;
    std::uint64_t record_bytes = 0U;
    std::unique_ptr<ingress::RawWalStreamWriter> stream;
};

void TestByteRotationAndOrdering(TestContext* test) {
    const std::uint64_t one_record_target =
        ingress::kRawV1SegmentHeaderBytes +
        FixtureRecordBytes();
    StreamFixture fixture(
        one_record_target, 1'000U);
    test->Expect(
        fixture.stream->Initialize(),
        "stream initializes first open manifest and control");

    fixture.clock.now = 101U;
    const RecordFixture first = MakeRecord(1U);
    test->Expect(
        fixture.stream->AppendRecord(first.input()) &&
            fixture.stream->rotation_count() == 0U,
        "first record fills the target without early rotation");
    fixture.clock.now = 102U;
    const RecordFixture second = MakeRecord(2U);
    test->Expect(
        fixture.stream->AppendRecord(second.input()) &&
            fixture.stream->rotation_count() == 1U &&
            fixture.stream->current_segment_records() ==
                1U,
        "second record rotates before writing and remains whole");
    test->Expect(
        fixture.backend.closed_plans.size() == 1U &&
            fixture.backend.closed_plans[0U]
                    .metadata.record_count ==
                1U &&
            fixture.files->segments.size() == 2U,
        "R6-R12 produce one closed plan and one next segment");

    const auto FindEvent =
        [&fixture](const std::string& value) {
            return std::find(
                fixture.backend.events.begin(),
                fixture.backend.events.end(),
                value);
        };
    const auto closed = FindEvent("closed-1");
    const auto created = FindEvent("create-2");
    const auto opened = FindEvent("open-2");
    test->Expect(
        closed != fixture.backend.events.end() &&
            created != fixture.backend.events.end() &&
            opened != fixture.backend.events.end() &&
            closed < created && created < opened,
        "closed artifact/manifest precedes next create and open manifest");

    test->Expect(
        fixture.stream->FlushDurable() &&
            fixture.stream->SealAndClose(),
        "rotated stream flushes and closes its final segment");
    const ingress::RawWalWriterSnapshot final =
        fixture.stream->Snapshot();
    test->Expect(
        final.sealed && final.closed && !final.fatal &&
            final.append == final.durable &&
            fixture.backend.closed_plans.size() == 2U,
        "clean stop publishes artifacts for the final segment");
}

void TestEmptyAgeDoesNotRotate(TestContext* test) {
    StreamFixture fixture(
        ingress::kRawV1SegmentHeaderBytes +
            4U * FixtureRecordBytes(),
        10U);
    test->Expect(
        fixture.stream->Initialize(),
        "age fixture initializes");
    fixture.clock.now = 200U;
    test->Expect(
        fixture.stream->FlushDurable() &&
            fixture.stream->rotation_count() == 0U,
        "an empty segment does not rotate after max age");
    fixture.clock.now = 201U;
    const RecordFixture first = MakeRecord(1U);
    test->Expect(
        fixture.stream->AppendRecord(first.input()) &&
            fixture.stream->rotation_count() == 0U,
        "first record enters an aged but empty segment without rotation");
    fixture.clock.now = 212U;
    const RecordFixture second = MakeRecord(2U);
    test->Expect(
        fixture.stream->AppendRecord(second.input()) &&
            fixture.stream->rotation_count() == 1U,
        "age rotates a nonempty segment before the next record");
    test->Expect(
        fixture.stream->SealAndClose(),
        "age-rotated fixture closes");
}

void TestRingConsumerAcrossRotation(TestContext* test) {
    StreamFixture fixture(
        ingress::kRawV1SegmentHeaderBytes +
            FixtureRecordBytes(),
        1'000U);
    test->Expect(
        fixture.stream->Initialize(),
        "ring/stream fixture initializes");
    ingress::ByteRing ring(4096U, 36U);
    const RecordFixture first = MakeRecord(1U);
    const RecordFixture second = MakeRecord(2U);
    test->Expect(
        ring.try_push_copy(
            first.meta, first.head, first.body) ==
                ingress::ByteRingPushResult::PUBLISHED &&
            ring.try_push_copy(
                second.meta, second.head, second.body) ==
                ingress::ByteRingPushResult::PUBLISHED,
        "two callback records enter the single-consumer ring");

    CaptureFailure failure{};
    ingress::RawCaptureWorkerConfig worker_config{};
    worker_config.durable_interval_ns = 1'000U;
    worker_config.durable_batch_bytes =
        1U << 20U;
    worker_config.failure_sink = {
        &CaptureFailure::Notify, &failure};
    worker_config.monotonic_now = &Clock::Now;
    worker_config.monotonic_clock_context =
        &fixture.clock;
    ingress::RawCaptureWorker worker(
        worker_config, ring, *fixture.stream);
    worker.StopAndDrain();
    test->Expect(
        worker.Run(),
        "one ring consumer drains through a segment rotation");
    const ingress::RawCaptureWorkerSnapshot snapshot =
        worker.Snapshot();
    test->Expect(
        failure.calls == 0U &&
            snapshot.append.records == 2U &&
            snapshot.durable.records == 2U &&
            snapshot.append.framed_wal_bytes ==
                2U * FixtureRecordBytes() &&
            snapshot.durable.framed_wal_bytes ==
                snapshot.append.framed_wal_bytes &&
            fixture.stream->rotation_count() == 1U,
        "capture progress excludes rotated header bytes and remains exact");
}

}  // namespace

int main() {
    TestContext test;
    TestByteRotationAndOrdering(&test);
    TestEmptyAgeDoesNotRotate(&test);
    TestRingConsumerAcrossRotation(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Raw WAL stream test(s) failed\n";
        return 1;
    }
    std::cout << "Phase 2 Raw WAL stream tests passed\n";
    return 0;
}
