#include "l2flow/consumer/canonical_batch_reader_v1.h"
#include "l2flow/consumer/consumer_c_api_v1.h"

#include "l2flow/canonical/canonical_segment_v1.h"
#include "l2flow/canonical/source_frontier_v1.h"
#include "l2flow/control/quality_flags_v1.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>

#include <unistd.h>

namespace consumer = l2flow::consumer;
namespace canonical = l2flow::canonical;
namespace common = l2flow::common;
namespace control = l2flow::control;
namespace ingress = l2flow::ingress;

namespace {

static_assert(std::is_same_v<
              decltype(std::declval<const consumer::MdlBatchViewV1&>()
                           .records_bytes()),
              std::span<const std::byte>>);

struct TestContext final {
    int failures = 0;

    void Expect(bool condition, std::string_view message) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << message << '\n';
        }
    }
};

template <std::size_t Size>
std::array<std::byte, Size> Pattern(std::uint8_t seed) {
    std::array<std::byte, Size> value{};
    for (std::size_t index = 0U; index < Size; ++index) {
        value[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(seed + index));
    }
    return value;
}

class TemporaryDirectory final {
public:
    TemporaryDirectory() {
        std::array<char, 64U> pattern{};
        constexpr std::string_view source =
            "/tmp/l2flow-phase7-batch-XXXXXX";
        std::copy(source.begin(), source.end(), pattern.begin());
        if (char* created = ::mkdtemp(pattern.data()); created != nullptr) {
            path_ = created;
        }
    }

    ~TemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

struct Fixture final {
    TemporaryDirectory directory;
    canonical::CanonicalSegmentDescriptorV1 descriptor{};
    canonical::SourceFrontierConfigV1 frontier_config{};
    canonical::SourceFrontierPageV1 frontier{};
    ingress::RawControlSnapshot raw{};
    std::unique_ptr<canonical::CanonicalSegmentWriterV1> writer;
    std::filesystem::path segment_path;

    [[nodiscard]] canonical::CanonicalTickRecordV1 MakeRecord(
        std::uint64_t sequence,
        std::uint64_t quality_flags) const noexcept {
        const std::int64_t receive =
            static_cast<std::int64_t>(sequence * 100U);
        canonical::CanonicalTickRecordV1 record{};
        record.header.event_type = canonical::CanonicalEventTypeV1::kTick;
        record.header.record_size = descriptor.record_size;
        record.header.source_stream_id = descriptor.source_stream_id;
        record.header.connection_epoch = 2U;
        record.header.trade_date = descriptor.trade_date;
        record.header.quality_flags = quality_flags;
        record.header.shard_event_id = sequence;
        record.header.origin_ingress_sequence = sequence;
        record.header.origin_wal_end_pos = 4096U + sequence * 4096U;
        record.header.vendor_sequence_id = sequence;
        record.header.exchange_sequence = sequence;
        record.header.recv_realtime_ns = receive + 10;
        record.header.recv_monotonic_ns = receive;
        record.header.instrument_id = 23U;
        record.header.channel = 4U;
        record.header.market = canonical::CanonicalMarketV1::kShanghai;
        record.header.origin_service_version = 101U;
        record.header.origin_message_id = 24U;
        record.header.origin_service_id = 4U;
        record.payload.action = canonical::CanonicalTickActionV1::kStatus;
        record.payload.source_enum_bits =
            static_cast<std::uint64_t>('S');
        return record;
    }

    [[nodiscard]] bool AppendRecord(
        std::uint64_t sequence,
        std::uint64_t quality_flags) {
        const std::uint64_t wal = 4096U + sequence * 4096U;
        const std::int64_t receive =
            static_cast<std::int64_t>(sequence * 100U);
        canonical::SourceFrontierCallbackGuardV1 guard(
            &frontier,
            frontier_config.writer_instance,
            frontier_config.generation);
        if (!guard.entered() ||
            guard.CompleteCaptured(sequence) !=
                canonical::SourceFrontierErrorV1::kNone ||
            canonical::PublishAppendProgressV1(
                &frontier,
                frontier_config.writer_instance,
                frontier_config.generation,
                sequence, wal, receive) !=
                canonical::SourceFrontierErrorV1::kNone) {
            return false;
        }
        const canonical::CanonicalTickRecordV1 record =
            MakeRecord(sequence, quality_flags);
        if (writer->PublishRecord(std::as_bytes(std::span(&record, 1U))) !=
                canonical::CanonicalSegmentErrorV1::kNone ||
            writer->AdvanceProcessedRaw(sequence, wal) !=
                canonical::CanonicalSegmentErrorV1::kNone ||
            canonical::PublishProcessedProgressV1(
                &frontier,
                frontier_config.writer_instance,
                frontier_config.generation,
                sequence, wal, receive) !=
                canonical::SourceFrontierErrorV1::kNone) {
            return false;
        }
        raw.append_global_wal_pos = wal;
        raw.append_ingress_sequence = sequence;
        raw.append_segment_offset = wal;
        raw.durable_global_wal_pos = wal;
        raw.durable_ingress_sequence = sequence;
        raw.durable_segment_offset = wal;
        return true;
    }

    [[nodiscard]] bool PublishPhysicalOnly(std::uint64_t sequence) {
        const std::uint64_t wal = 4096U + sequence * 4096U;
        const std::int64_t receive =
            static_cast<std::int64_t>(sequence * 100U);
        canonical::SourceFrontierCallbackGuardV1 guard(
            &frontier,
            frontier_config.writer_instance,
            frontier_config.generation);
        if (!guard.entered() ||
            guard.CompleteCaptured(sequence) !=
                canonical::SourceFrontierErrorV1::kNone ||
            canonical::PublishAppendProgressV1(
                &frontier,
                frontier_config.writer_instance,
                frontier_config.generation,
                sequence, wal, receive) !=
                canonical::SourceFrontierErrorV1::kNone) {
            return false;
        }
        const canonical::CanonicalTickRecordV1 record =
            MakeRecord(sequence, 0U);
        if (writer->PublishRecord(std::as_bytes(std::span(&record, 1U))) !=
            canonical::CanonicalSegmentErrorV1::kNone) {
            return false;
        }
        raw.append_global_wal_pos = wal;
        raw.append_ingress_sequence = sequence;
        raw.append_segment_offset = wal;
        raw.durable_global_wal_pos = wal;
        raw.durable_ingress_sequence = sequence;
        raw.durable_segment_offset = wal;
        return true;
    }

    [[nodiscard]] bool Initialize() {
        descriptor.event_type = canonical::CanonicalEventTypeV1::kTick;
        descriptor.record_size = static_cast<std::uint32_t>(
            canonical::kCanonicalTickRecordBytesV1);
        descriptor.source_stream_id = 1002U;
        descriptor.shard = 7U;
        descriptor.trade_date = 20260722U;
        descriptor.origin_capture_date = 20260722U;
        descriptor.origin_stream_day_id = Pattern<16U>(0x10U);
        descriptor.origin_source_writer_instance = Pattern<16U>(0x30U);
        descriptor.origin_source_generation = 9U;
        descriptor.clock_epoch.algorithm = 1U;
        descriptor.clock_epoch.digest = Pattern<32U>(0x50U);
        descriptor.clock_epoch.label = 77U;
        descriptor.schema_sha256 =
            canonical::CanonicalSchemaDescriptorSha256V1();
        descriptor.dtype_sha256 =
            canonical::CanonicalDtypeDescriptorSha256V1();
        descriptor.registry_version = 3U;
        descriptor.registry_sha256 = Pattern<32U>(0x70U);
        descriptor.normalizer_build_sha256 = Pattern<32U>(0x90U);
        descriptor.normalizer_config_sha256 = Pattern<32U>(0xb0U);
        descriptor.generation = 12U;
        descriptor.segment_sequence = 1U;
        descriptor.capacity_records = 8U;
        segment_path = directory.path() / "tick.clog";
        canonical::CanonicalSegmentCreateOptionsV1 options{};
        options.segment_path = segment_path;
        options.manifest_path = directory.path() / "tick.manifest";
        options.descriptor = descriptor;
        options.created_realtime_ns = 1000;
        options.created_monotonic_ns = 500;
        if (canonical::CanonicalSegmentWriterV1::Create(options, &writer) !=
                canonical::CanonicalSegmentErrorV1::kNone ||
            writer == nullptr) {
            return false;
        }

        frontier_config.source_stream_id = descriptor.source_stream_id;
        frontier_config.capture_date = descriptor.origin_capture_date;
        frontier_config.stream_day_id = descriptor.origin_stream_day_id;
        frontier_config.clock_epoch = descriptor.clock_epoch;
        frontier_config.writer_instance =
            descriptor.origin_source_writer_instance;
        frontier_config.generation = descriptor.origin_source_generation;
        frontier_config.initial_state = canonical::SourceStateV1::kHealthy;
        if (canonical::InitializeSourceFrontierPageV1(
                frontier_config, &frontier) !=
            canonical::SourceFrontierErrorV1::kNone) {
            return false;
        }

        for (std::uint64_t sequence = 1U; sequence <= 2U; ++sequence) {
            const std::uint64_t quality_flags = sequence == 2U
                ? control::QualityBit(control::QualityFlagV1::kStartUnknown)
                : 0U;
            if (!AppendRecord(sequence, quality_flags)) {
                return false;
            }
        }

        raw.writer_instance = descriptor.origin_source_writer_instance;
        raw.stream_day_id = descriptor.origin_stream_day_id;
        raw.source_stream_id = descriptor.source_stream_id;
        raw.capture_date = descriptor.origin_capture_date;
        raw.segment_sequence = 1U;
        raw.clock_epoch_label = descriptor.clock_epoch.label;
        return true;
    }

    [[nodiscard]] std::shared_ptr<const canonical::CanonicalSegmentReaderV1>
    OpenShared() const {
        std::unique_ptr<canonical::CanonicalSegmentReaderV1> opened;
        if (canonical::CanonicalSegmentReaderV1::Open(
                segment_path, descriptor, &opened) !=
                canonical::CanonicalSegmentErrorV1::kNone ||
            opened == nullptr) {
            return {};
        }
        return std::shared_ptr<const canonical::CanonicalSegmentReaderV1>(
            std::move(opened));
    }
};

bool ObserveRaw(void* context, ingress::RawControlSnapshot* output) noexcept {
    if (context == nullptr || output == nullptr) {
        return false;
    }
    *output = static_cast<Fixture*>(context)->raw;
    return true;
}

int ObserveRawC(
    void* context,
    l2flow_consumer_raw_control_snapshot_v1* output) {
    if (context == nullptr || output == nullptr) {
        return 0;
    }
    const auto& input = static_cast<Fixture*>(context)->raw;
    *output = l2flow_consumer_raw_control_snapshot_v1{};
    std::memcpy(
        output->writer_instance,
        input.writer_instance.data(), input.writer_instance.size());
    std::memcpy(
        output->stream_day_id,
        input.stream_day_id.data(), input.stream_day_id.size());
    output->source_stream_id = input.source_stream_id;
    output->capture_date = input.capture_date;
    output->segment_sequence = input.segment_sequence;
    output->fatal_state = input.fatal_state;
    output->append_global_wal_pos = input.append_global_wal_pos;
    output->append_ingress_sequence = input.append_ingress_sequence;
    output->append_segment_offset = input.append_segment_offset;
    output->durable_global_wal_pos = input.durable_global_wal_pos;
    output->durable_ingress_sequence = input.durable_ingress_sequence;
    output->durable_segment_offset = input.durable_segment_offset;
    output->clock_epoch_label = input.clock_epoch_label;
    output->heartbeat_monotonic_ns = input.heartbeat_monotonic_ns;
    return 1;
}

consumer::CanonicalConsumerAttachSpecV1 Attach(
    const Fixture& fixture) {
    consumer::CanonicalConsumerAttachSpecV1 value{};
    value.event_type = fixture.descriptor.event_type;
    value.record_size = fixture.descriptor.record_size;
    value.schema_sha256 = fixture.descriptor.schema_sha256;
    value.dtype_sha256 = fixture.descriptor.dtype_sha256;
    value.registry_version = fixture.descriptor.registry_version;
    value.registry_sha256 = fixture.descriptor.registry_sha256;
    return value;
}

std::unique_ptr<consumer::CanonicalCommittedBatchReaderV1> BatchReader(
    Fixture* fixture,
    std::shared_ptr<const canonical::CanonicalSegmentReaderV1> mapping,
    std::uint64_t initial_cursor = 0U) {
    consumer::CanonicalBatchReaderConfigV1 config{};
    config.segment_reader = std::move(mapping);
    config.source_frontier = &fixture->frontier;
    config.expected = Attach(*fixture);
    config.initial_canonical_cursor = initial_cursor;
    config.raw_durability_observer = &ObserveRaw;
    config.raw_durability_observer_context = fixture;
    std::unique_ptr<consumer::CanonicalCommittedBatchReaderV1> output;
    if (consumer::CanonicalCommittedBatchReaderV1::Create(
            std::move(config), &output) !=
        consumer::CanonicalBatchErrorV1::kNone) {
        return nullptr;
    }
    return output;
}

void FillCDescriptor(
    const canonical::CanonicalSegmentDescriptorV1& input,
    l2flow_consumer_segment_descriptor_v1* output) {
    *output = l2flow_consumer_segment_descriptor_v1{};
    output->event_type = static_cast<std::uint16_t>(input.event_type);
    output->record_size = input.record_size;
    output->source_stream_id = input.source_stream_id;
    output->shard_id = input.shard;
    output->trade_date = input.trade_date;
    output->origin_capture_date = input.origin_capture_date;
    std::memcpy(
        output->origin_stream_day_id,
        input.origin_stream_day_id.data(), input.origin_stream_day_id.size());
    std::memcpy(
        output->origin_source_writer_instance,
        input.origin_source_writer_instance.data(),
        input.origin_source_writer_instance.size());
    output->origin_source_generation = input.origin_source_generation;
    output->clock_epoch.algorithm = input.clock_epoch.algorithm;
    std::memcpy(
        output->clock_epoch.digest,
        input.clock_epoch.digest.data(), input.clock_epoch.digest.size());
    output->clock_epoch.label = input.clock_epoch.label;
    std::memcpy(
        output->schema_sha256,
        input.schema_sha256.data(), input.schema_sha256.size());
    std::memcpy(
        output->dtype_sha256,
        input.dtype_sha256.data(), input.dtype_sha256.size());
    output->registry_version = input.registry_version;
    std::memcpy(
        output->registry_sha256,
        input.registry_sha256.data(), input.registry_sha256.size());
    std::memcpy(
        output->normalizer_build_sha256,
        input.normalizer_build_sha256.data(),
        input.normalizer_build_sha256.size());
    std::memcpy(
        output->normalizer_config_sha256,
        input.normalizer_config_sha256.data(),
        input.normalizer_config_sha256.size());
    output->canonical_generation = input.generation;
    output->segment_sequence = input.segment_sequence;
    output->capacity_records = input.capacity_records;
}

void FillCAttach(
    const canonical::CanonicalSegmentDescriptorV1& input,
    l2flow_consumer_attach_identity_v1* output) {
    *output = l2flow_consumer_attach_identity_v1{};
    output->event_type = static_cast<std::uint16_t>(input.event_type);
    output->record_size = input.record_size;
    std::memcpy(
        output->schema_sha256,
        input.schema_sha256.data(), input.schema_sha256.size());
    std::memcpy(
        output->dtype_sha256,
        input.dtype_sha256.data(), input.dtype_sha256.size());
    output->registry_version = input.registry_version;
    std::memcpy(
        output->registry_sha256,
        input.registry_sha256.data(), input.registry_sha256.size());
}

void CheckCppBatch(TestContext* test, Fixture* fixture) {
    auto mapping = fixture->OpenShared();
    test->Expect(mapping != nullptr, "canonical mapping opens exactly");
    auto reader = BatchReader(fixture, mapping);
    test->Expect(reader != nullptr, "committed batch reader attaches exactly");
    consumer::MdlBatchViewV1 first{};
    const auto first_peek_error = reader->Peek(2U, 44U, &first);
    if (first_peek_error != consumer::CanonicalBatchErrorV1::kNone) {
        std::cerr << "first Peek error: "
                  << consumer::CanonicalBatchErrorNameV1(first_peek_error)
                  << '\n';
    }
    test->Expect(
        first_peek_error == consumer::CanonicalBatchErrorV1::kNone &&
            first.valid() && first.record_count() == 2U &&
            first.record_size() == canonical::kCanonicalTickRecordBytesV1,
        "Peek returns a read-only zero-copy two-record batch");
    const auto& metadata = first.metadata();
    test->Expect(
        metadata.source_stream_id == fixture->descriptor.source_stream_id &&
            metadata.origin_capture_date ==
                fixture->descriptor.origin_capture_date &&
            metadata.trade_date == fixture->descriptor.trade_date &&
            metadata.origin_stream_day_id ==
                fixture->descriptor.origin_stream_day_id &&
            metadata.family == fixture->descriptor.event_type &&
            metadata.shard_id == fixture->descriptor.shard &&
            metadata.begin_canonical_cursor == 0U &&
            metadata.end_canonical_cursor == 2U &&
            metadata.max_consumed_origin_wal_end_pos == 12288U &&
            metadata.observed_raw_durable_wal_pos == 12288U &&
            metadata.clock_epoch == fixture->descriptor.clock_epoch &&
            metadata.schema_sha256 == fixture->descriptor.schema_sha256 &&
            metadata.dtype_sha256 == fixture->descriptor.dtype_sha256 &&
            metadata.registry_version ==
                fixture->descriptor.registry_version &&
            metadata.registry_sha256 ==
                fixture->descriptor.registry_sha256 &&
            metadata.batch_quality_flags ==
                control::QualityBit(control::QualityFlagV1::kStartUnknown) &&
            metadata.watermark_set_id == 44U &&
            metadata.origin_source_writer_instance ==
                fixture->descriptor.origin_source_writer_instance &&
            metadata.origin_source_generation ==
                fixture->descriptor.origin_source_generation &&
            metadata.canonical_generation == fixture->descriptor.generation,
        "batch metadata carries all schema, registry, cursor, Raw and generation fields");

    consumer::MdlBatchViewV1 wrong_identity{};
    test->Expect(
        reader->Peek(2U, 45U, &wrong_identity) ==
                consumer::CanonicalBatchErrorV1::kInvalidBatch &&
            !wrong_identity.valid() && reader->cursor() == 0U,
        "pending batch rejects a different watermark-set identity");
    test->Expect(
        fixture->AppendRecord(3U, 0U),
        "a third record commits after the plugin-visible Peek");

    consumer::MdlBatchViewV1 retried{};
    test->Expect(
        reader->Peek(4U, 44U, &retried) ==
                consumer::CanonicalBatchErrorV1::kNone &&
            retried.record_count() == 2U &&
            retried.metadata().begin_canonical_cursor == 0U &&
            retried.metadata().end_canonical_cursor == 2U &&
            retried.metadata().watermark_set_id == 44U &&
            retried.records_bytes().data() == first.records_bytes().data() &&
            reader->cursor() == 0U,
        "plugin exception/no Commit replays the exact batch after new data");
    test->Expect(
        reader->Commit(first) == consumer::CanonicalBatchErrorV1::kNone &&
            reader->cursor() == 2U,
        "explicit Commit advances the exclusive next cursor");
    test->Expect(
        reader->Commit(first) ==
            consumer::CanonicalBatchErrorV1::kStaleBatch,
        "the same batch cannot commit twice");

    // The view owns the reader mapping, independent of the cursor object and
    // the caller's original shared_ptr.
    const std::byte first_byte = first.records_bytes().front();
    reader.reset();
    mapping.reset();
    test->Expect(
        first.valid() && first.records_bytes().front() == first_byte,
        "batch view retains mmap lifetime after reader handles are destroyed");

    auto wrong_mapping = fixture->OpenShared();
    consumer::CanonicalBatchReaderConfigV1 bad{};
    bad.segment_reader = std::move(wrong_mapping);
    bad.source_frontier = &fixture->frontier;
    bad.expected = Attach(*fixture);
    bad.expected.registry_version += 1U;
    bad.raw_durability_observer = &ObserveRaw;
    bad.raw_durability_observer_context = fixture;
    std::unique_ptr<consumer::CanonicalCommittedBatchReaderV1> rejected;
    test->Expect(
        consumer::CanonicalCommittedBatchReaderV1::Create(
            std::move(bad), &rejected) ==
            consumer::CanonicalBatchErrorV1::kAttachMismatch,
        "batch attach independently pins exact registry identity");
}

void CheckCBatch(TestContext* test, Fixture* fixture) {
    const std::string path = fixture->segment_path.string();
    l2flow_consumer_batch_open_config_v1 config{};
    config.segment_path = path.c_str();
    FillCDescriptor(fixture->descriptor, &config.expected_segment);
    FillCAttach(fixture->descriptor, &config.expected_attach);
    config.source_frontier_page = &fixture->frontier;
    config.raw_durability_observer = &ObserveRawC;
    config.raw_durability_observer_context = fixture;
    l2flow_consumer_batch_reader_handle_v1* reader = nullptr;
    test->Expect(
        l2flow_consumer_batch_open_v1(&config, &reader) ==
                L2FLOW_CONSUMER_C_OK_V1 &&
            reader != nullptr,
        "C ABI opens an exact descriptor-backed batch reader");
    l2flow_consumer_batch_view_handle_v1* view = nullptr;
    test->Expect(
        l2flow_consumer_batch_peek_v1(reader, 2U, 88U, &view) ==
                L2FLOW_CONSUMER_C_OK_V1 &&
            view != nullptr,
        "C ABI Peek creates an owning batch view handle");
    const std::uint8_t* bytes = nullptr;
    std::size_t count = 0U;
    std::uint32_t size = 0U;
    test->Expect(
        l2flow_consumer_batch_view_records_v1(
            view, &bytes, &count, &size) == L2FLOW_CONSUMER_C_OK_V1 &&
            bytes != nullptr && count == 2U &&
            size == canonical::kCanonicalTickRecordBytesV1,
        "C ABI exposes const mapped bytes plus fixed shape");
    l2flow_consumer_batch_metadata_v1 metadata{};
    test->Expect(
        l2flow_consumer_batch_view_metadata_v1(view, &metadata) ==
                L2FLOW_CONSUMER_C_OK_V1 &&
            metadata.trade_date == fixture->descriptor.trade_date &&
            metadata.registry_version ==
                fixture->descriptor.registry_version &&
            metadata.begin_canonical_cursor == 0U &&
            metadata.end_canonical_cursor == 2U,
        "C batch metadata includes date, registry and exclusive cursors");
    test->Expect(
        l2flow_consumer_batch_commit_v1(reader, view) ==
                L2FLOW_CONSUMER_C_OK_V1 &&
            l2flow_consumer_batch_cursor_v1(reader) == 2U,
        "C ABI Commit advances only after successful processing");
    const std::uint8_t retained = bytes[0];
    l2flow_consumer_batch_reader_release_v1(reader);
    test->Expect(
        bytes[0] == retained,
        "C view handle retains mapping after C reader release");
    l2flow_consumer_batch_view_release_v1(view);
}

void CheckCommitRevocation(TestContext* test, Fixture* fixture) {
    auto reader = BatchReader(fixture, fixture->OpenShared());
    consumer::MdlBatchViewV1 view{};
    test->Expect(
        reader != nullptr &&
            reader->Peek(1U, 99U, &view) ==
                consumer::CanonicalBatchErrorV1::kNone,
        "revocation batch peeks while generation is healthy");
    test->Expect(
        canonical::PublishSourceStateV1(
            &fixture->frontier,
            fixture->frontier_config.writer_instance,
            fixture->frontier_config.generation,
            canonical::SourceStateV1::kFatal,
            0U) == canonical::SourceFrontierErrorV1::kNone,
        "live source generation latches FATAL");
    test->Expect(
        reader->Commit(view) ==
                consumer::CanonicalBatchErrorV1::kCommittedReadFailed &&
            reader->cursor() == 0U,
        "Commit rechecks live frontier and refuses a revoked generation");
}

void CheckFirstRecordLag(TestContext* test) {
    Fixture fixture;
    test->Expect(
        fixture.Initialize(),
        "physical-before-processed fixture initializes");
    if (test->failures != 0) {
        return;
    }
    auto reader = BatchReader(&fixture, fixture.OpenShared(), 2U);
    test->Expect(
        reader != nullptr && fixture.PublishPhysicalOnly(3U),
        "third physical record publishes before processed frontier");
    consumer::MdlBatchViewV1 view{};
    test->Expect(
        reader != nullptr &&
            reader->Peek(1U, 123U, &view) ==
                consumer::CanonicalBatchErrorV1::kWouldBlock &&
            !view.valid() && reader->cursor() == 2U,
        "first physical-but-uncommitted record is normal WouldBlock");
}

}  // namespace

int main() {
    TestContext test;
    Fixture fixture;
    test.Expect(fixture.Initialize(), "Phase 7 batch fixture initializes");
    if (test.failures == 0) {
        CheckCppBatch(&test, &fixture);
        CheckCBatch(&test, &fixture);
        CheckFirstRecordLag(&test);
        CheckCommitRevocation(&test, &fixture);
    }
    if (test.failures != 0) {
        std::cerr << test.failures << " Phase 7 batch checks failed\n";
        return 1;
    }
    std::cout << "Phase 7 batch checks passed\n";
    return 0;
}
