#include "l2flow/canonical/canonical_schema_v1.h"
#include "l2flow/canonical/canonical_segment_v1.h"
#include "l2flow/canonical/source_frontier_v1.h"
#include "l2flow/common/identity128.h"
#include "l2flow/control/control_decoder.h"
#include "l2flow/ingress/raw_live_tail.h"
#include "l2flow/ingress/raw_schema.h"
#include "l2flow/ingress/raw_v1.h"
#include "l2flow/market/instrument_history_v1.h"
#include "l2flow/market/instrument_registry.h"
#include "l2flow/runtime/production_source_pipeline_v1.h"
#include "l2flow/sdk/subscription_manifest.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
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
#include <vector>

namespace canonical = l2flow::canonical;
namespace common = l2flow::common;
namespace control = l2flow::control;
namespace ingress = l2flow::ingress;
namespace market = l2flow::market;
namespace runtime = l2flow::runtime;
namespace sdk = l2flow::sdk;

namespace {

using namespace std::chrono_literals;

constexpr std::uint32_t kCaptureDate = 20260722U;
constexpr std::uint32_t kTradeDate = 20260722U;
constexpr std::uint64_t kSourceGeneration = 9U;
constexpr std::uint64_t kCanonicalGeneration = 3U;
constexpr std::uint32_t kInstrumentId = 50U;

class TestContext final {
public:
    void Expect(bool condition, std::string_view description) {
        if (!condition) {
            ++failures_;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    [[nodiscard]] int failures() const noexcept { return failures_; }

private:
    int failures_ = 0;
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

std::vector<std::byte> Bytes(std::string_view text) {
    const auto bytes = std::as_bytes(
        std::span<const char>(text.data(), text.size()));
    return std::vector<std::byte>(bytes.begin(), bytes.end());
}

class WireWriter final {
public:
    explicit WireWriter(std::size_t fixed_bytes)
        : bytes_(fixed_bytes, std::byte{0U}) {}

    void U16(std::size_t offset, std::uint16_t value) {
        bytes_.at(offset) = static_cast<std::byte>(value & 0xffU);
        bytes_.at(offset + 1U) =
            static_cast<std::byte>((value >> 8U) & 0xffU);
    }

    void U32(std::size_t offset, std::uint32_t value) {
        for (std::size_t index = 0U; index < 4U; ++index) {
            bytes_.at(offset + index) = static_cast<std::byte>(
                (value >> static_cast<unsigned int>(index * 8U)) &
                0xffU);
        }
    }

    void U64(std::size_t offset, std::uint64_t value) {
        for (std::size_t index = 0U; index < 8U; ++index) {
            bytes_.at(offset + index) = static_cast<std::byte>(
                (value >> static_cast<unsigned int>(index * 8U)) &
                0xffU);
        }
    }

    void I32(std::size_t offset, std::int32_t value) {
        U32(offset, static_cast<std::uint32_t>(value));
    }

    void I64(std::size_t offset, std::int64_t value) {
        U64(offset, static_cast<std::uint64_t>(value));
    }

    void Text(std::size_t descriptor, std::string_view text) {
        const std::size_t begin = bytes_.size();
        const auto text_bytes = std::as_bytes(
            std::span<const char>(text.data(), text.size()));
        bytes_.insert(bytes_.end(), text_bytes.begin(), text_bytes.end());
        U16(descriptor, static_cast<std::uint16_t>(text.size()));
        U32(
            descriptor + 2U,
            static_cast<std::uint32_t>(begin - descriptor));
    }

    [[nodiscard]] std::vector<std::byte> Take() && {
        return std::move(bytes_);
    }

private:
    std::vector<std::byte> bytes_;
};

std::vector<std::byte> ShenzhenOrderBody(std::int64_t appl_seq = 9001) {
    WireWriter writer(58U);
    writer.U32(0U, 12U);
    writer.I64(4U, appl_seq);
    writer.I64(30U, 123456);
    writer.I64(38U, 700);
    writer.I32(46U, 49);
    writer.U32(50U, 93000123U);
    writer.I32(54U, 50);
    writer.Text(12U, "010");
    writer.Text(18U, "000001");
    writer.Text(24U, "102");
    return std::move(writer).Take();
}

struct RawMessageSpec final {
    sdk::MessageKey key{};
    std::vector<std::byte> body;
};

void StoreLittleEndian(
    std::array<std::byte, ingress::kVendorMessageHeadBytes>* head,
    std::size_t offset,
    std::uint64_t value,
    std::size_t width) {
    for (std::size_t index = 0U; index < width; ++index) {
        (*head)[offset + index] = static_cast<std::byte>(
            (value >> static_cast<unsigned int>(index * 8U)) & 0xffU);
    }
}

struct SegmentFixture final {
    ingress::SegmentHeaderV1 header{};
    std::vector<std::byte> bytes;
};

struct SourceRecordProgress final {
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t record_end_wal_pos = 0U;
    std::int64_t recv_monotonic_ns = 0;
};

class MemorySource final : public ingress::RawLiveTailSource {
public:
    [[nodiscard]] int ReadControl(
        ingress::RawControlSnapshot* output,
        std::uint64_t* generation) noexcept override {
        *output = control;
        *generation = 2U;
        return 0;
    }

    [[nodiscard]] int InspectSegment(
        std::uint32_t sequence,
        ingress::RawLiveSegmentInfo* output) noexcept override {
        if (sequence != segment.header.segment_sequence) {
            return ENOENT;
        }
        output->header = segment.header;
        output->visible_end_offset =
            static_cast<std::uint64_t>(segment.bytes.size());
        output->sealed = sealed;
        return 0;
    }

    [[nodiscard]] ingress::RawLiveReadResult ReadSegmentSome(
        std::uint32_t sequence,
        std::uint64_t offset,
        std::span<std::byte> output) noexcept override {
        if (sequence != segment.header.segment_sequence ||
            offset > static_cast<std::uint64_t>(segment.bytes.size())) {
            return {0U, ENOENT};
        }
        const std::size_t begin = static_cast<std::size_t>(offset);
        const std::size_t count = std::min(
            output.size(), segment.bytes.size() - begin);
        std::copy_n(
            segment.bytes.begin() + static_cast<std::ptrdiff_t>(begin),
            count,
            output.begin());
        return {count, 0};
    }

    SegmentFixture segment;
    ingress::RawControlSnapshot control{};
    std::vector<SourceRecordProgress> records;
    bool sealed = false;
};

class TempDirectory final {
public:
    TempDirectory() {
        std::array<char, 64U> pattern{};
        const std::string source =
            "/tmp/l2flow_production_pipeline_XXXXXX";
        std::copy(source.begin(), source.end(), pattern.begin());
        if (char* created = ::mkdtemp(pattern.data()); created != nullptr) {
            path_ = created;
        }
    }

    ~TempDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

canonical::ClockEpochIdentityV1 Clock() {
    canonical::ClockEpochIdentityV1 value{};
    value.algorithm = 1U;
    value.digest = Pattern<32U>(0x41U);
    value.label = 7U;
    return value;
}

ingress::SegmentHeaderV1 SegmentHeader(
    const sdk::IngressSpec& spec,
    const common::Sha256Digest& stable_config) {
    ingress::SegmentHeaderV1 header{};
    header.source_stream_id = spec.source_stream_id;
    header.capture_date = kCaptureDate;
    header.stream_day_id = Pattern<16U>(0x11U);
    header.segment_sequence = 1U;
    header.segment_base_wal_pos = 0U;
    header.first_ingress_sequence = 1U;
    header.created_realtime_ns = 10U;
    header.created_monotonic_ns = 20U;
    header.host_uuid = Pattern<16U>(0x21U);
    header.linux_boot_id = Pattern<16U>(0x31U);
    header.clock_epoch_algorithm = Clock().algorithm;
    header.clock_epoch_digest = Clock().digest;
    header.clock_epoch_label = Clock().label;
    header.sdk_archive_sha256 = Pattern<32U>(0x51U);
    header.libmdl_api_sha256 = Pattern<32U>(0x61U);
    header.endpoint_contract_sha256 = Pattern<32U>(0x71U);
    header.config_sha256 = stable_config;
    header.raw_schema_sha256 = ingress::RawSchemaSha256Digest();
    header.build_manifest_sha256 = Pattern<32U>(0x91U);
    return header;
}

bool BuildRawSource(
    const sdk::IngressSpec& spec,
    const common::Sha256Digest& stable_config,
    std::span<const RawMessageSpec> messages,
    const common::Identity128& writer_instance,
    MemorySource* source) {
    source->segment.header = SegmentHeader(spec, stable_config);
    ingress::RawV1SegmentHeaderWire segment_wire{};
    if (ingress::EncodeSegmentHeaderV1(
            source->segment.header, &segment_wire) !=
        ingress::RawV1Error::kNone) {
        return false;
    }
    source->segment.bytes.assign(segment_wire.begin(), segment_wire.end());

    std::uint64_t sequence = 1U;
    for (const RawMessageSpec& message : messages) {
        ingress::RawRecordInputV1 input{};
        input.meta.source_stream_id = spec.source_stream_id;
        input.meta.capture_date = kCaptureDate;
        input.meta.ingress_sequence = sequence;
        input.meta.recv_realtime_ns = 1'000'000U + sequence;
        input.meta.recv_monotonic_ns = 2'000'000U + sequence;
        input.vendor_head.fill(std::byte{0U});
        input.vendor_head[0U] =
            static_cast<std::byte>(ingress::kVendorMessageHeadBytes);
        StoreLittleEndian(
            &input.vendor_head,
            1U,
            ingress::kVendorMessageHeadBytes + message.body.size(),
            4U);
        input.vendor_head[5U] = std::byte{1U};
        input.vendor_head[6U] =
            static_cast<std::byte>(message.key.service_id);
        StoreLittleEndian(
            &input.vendor_head, 7U, message.key.service_version, 2U);
        StoreLittleEndian(
            &input.vendor_head, 9U, message.key.message_id, 2U);
        StoreLittleEndian(&input.vendor_head, 11U, 93000123U, 4U);
        StoreLittleEndian(&input.vendor_head, 15U, 80'000U + sequence, 8U);
        input.vendor_body = message.body;

        std::vector<std::byte> record_wire;
        if (ingress::EncodeRawRecordV1(input, &record_wire) !=
            ingress::RawV1Error::kNone) {
            return false;
        }
        source->segment.bytes.insert(
            source->segment.bytes.end(),
            record_wire.begin(),
            record_wire.end());
        source->records.push_back(SourceRecordProgress{
            sequence,
            static_cast<std::uint64_t>(source->segment.bytes.size()),
            static_cast<std::int64_t>(input.meta.recv_monotonic_ns)});
        ++sequence;
    }

    source->control.writer_instance = writer_instance;
    source->control.stream_day_id = source->segment.header.stream_day_id;
    source->control.source_stream_id = spec.source_stream_id;
    source->control.capture_date = kCaptureDate;
    source->control.segment_sequence = 1U;
    source->control.append_segment_offset =
        static_cast<std::uint64_t>(source->segment.bytes.size());
    source->control.append_global_wal_pos =
        source->control.append_segment_offset;
    source->control.append_ingress_sequence = messages.size();
    source->control.durable_segment_offset =
        source->control.append_segment_offset;
    source->control.durable_global_wal_pos =
        source->control.append_global_wal_pos;
    source->control.durable_ingress_sequence =
        source->control.append_ingress_sequence;
    source->control.heartbeat_monotonic_ns = 10U;
    return true;
}

std::unique_ptr<market::InstrumentRegistryV1> Registry() {
    market::InstrumentRegistryEntryV1 entry{};
    entry.instrument_id = kInstrumentId;
    entry.key.market = market::MarketV1::kShenzhen;
    entry.key.security_id_source = Bytes("102");
    entry.key.security_id = Bytes("000001");
    entry.quantity_unit = market::QuantityUnitV1::kShare;
    entry.security_type = market::SecurityTypeV1::kEquity;
    entry.asset_scope = market::AssetScopeV1::kDocumentedCore;
    std::unique_ptr<market::InstrumentRegistryV1> registry;
    if (market::InstrumentRegistryV1::Create(
            7U, std::span(&entry, 1U), &registry) !=
        market::InstrumentRegistryCreateErrorV1::kNone) {
        return nullptr;
    }
    return registry;
}

std::uint8_t SourceSlot(sdk::IngressKind kind) {
    switch (kind) {
        case sdk::IngressKind::ShSnapshot:
            return 0U;
        case sdk::IngressKind::ShTick:
            return 1U;
        case sdk::IngressKind::SzSnapshot:
            return 2U;
        case sdk::IngressKind::SzTick:
            return 3U;
    }
    return std::numeric_limits<std::uint8_t>::max();
}

market::InstrumentHistoryRuntimeConfigV1 HistoryConfig() {
    market::InstrumentHistoryRuntimeConfigV1 config{};
    config.source_stream_ids = {1001U, 1002U, 2001U, 2002U};
    config.physical_worker_count = 2U;
    config.queue_capacity = 8U;
    config.maximum_inflight_per_source = 64U;
    config.chunk_record_capacity = 4U;
    config.maximum_records_per_logical_shard = 1024U;
    config.maximum_instruments_per_logical_shard = 64U;
    config.maximum_owned_payload_bytes_per_logical_shard =
        4U * 1024U * 1024U;
    return config;
}

class PipelineHarness final {
public:
    explicit PipelineHarness(TestContext* test) : test_(test) {}

    ~PipelineHarness() {
        pipeline.reset();
        if (history != nullptr) {
            history->StopAndDrain();
        }
    }

    [[nodiscard]] bool Initialize(
        sdk::IngressKind kind,
        std::span<const RawMessageSpec> messages,
        bool publish_append = true,
        const market::InstrumentHistoryRuntimeConfigV1*
            history_config_override = nullptr) {
        const sdk::IngressSpec& spec = sdk::GetIngressSpec(kind);
        ingress_kind = kind;
        source_slot = SourceSlot(kind);
        writer_instance = Pattern<16U>(0xa1U);
        stable_config_sha256 = Pattern<32U>(0xb1U);
        normalizer_build_sha256 = Pattern<32U>(0xc1U);
        normalizer_config_sha256 = Pattern<32U>(0xd1U);
        if (directory.path().empty() || source_slot >= 4U ||
            !BuildRawSource(
                spec,
                stable_config_sha256,
                messages,
                writer_instance,
                &source)) {
            return InitializationFailure("Raw source fixture creates");
        }

        registry = Registry();
        const market::InstrumentHistoryRuntimeConfigV1 history_config =
            history_config_override == nullptr
                ? HistoryConfig()
                : *history_config_override;
        if (registry == nullptr ||
            market::InstrumentHistoryRuntimeV1::Create(
                history_config, &history) !=
                market::InstrumentHistoryCreateErrorV1::kNone) {
            return InitializationFailure(
                "registry and instrument history create");
        }

        canonical::SourceFrontierConfigV1 frontier_config{};
        frontier_config.source_stream_id = spec.source_stream_id;
        frontier_config.capture_date = kCaptureDate;
        frontier_config.stream_day_id = source.segment.header.stream_day_id;
        frontier_config.clock_epoch = Clock();
        frontier_config.writer_instance = writer_instance;
        frontier_config.generation = kSourceGeneration;
        frontier_config.initial_global_wal_pos =
            ingress::kRawV1SegmentHeaderBytes;
        frontier_config.initial_state = canonical::SourceStateV1::kHealthy;
        if (canonical::InitializeSourceFrontierPageV1(
                frontier_config, &frontier) !=
            canonical::SourceFrontierErrorV1::kNone) {
            return InitializationFailure("SourceFrontier initializes");
        }
        // Producer-side proof: callback capture release-publishes before the
        // sole Raw writer publishes the corresponding append cursor.  The
        // derived pipeline must never synthesize either frontier.
        for (const SourceRecordProgress& record : source.records) {
            canonical::SourceFrontierCallbackGuardV1 callback(
                &frontier, writer_instance, kSourceGeneration);
            if (callback.error() !=
                    canonical::SourceFrontierErrorV1::kNone ||
                callback.CompleteCaptured(record.ingress_sequence) !=
                    canonical::SourceFrontierErrorV1::kNone) {
                return InitializationFailure(
                    "producer SourceFrontier publishes capture");
            }
            if (publish_append && !PublishAppend(record)) {
                return InitializationFailure(
                    "producer SourceFrontier publishes append");
            }
        }

        ingress::RawLiveTailAttachV1 attach{};
        attach.writer_instance = writer_instance;
        attach.stream_day_id = source.segment.header.stream_day_id;
        attach.source_stream_id = spec.source_stream_id;
        attach.capture_date = kCaptureDate;
        attach.segment_sequence = 1U;
        attach.global_wal_pos = ingress::kRawV1SegmentHeaderBytes;
        attach.segment_offset = ingress::kRawV1SegmentHeaderBytes;
        attach.next_ingress_sequence = 1U;
        std::unique_ptr<ingress::RawLiveTail> tail;
        if (ingress::RawLiveTail::Attach(&source, attach, &tail) !=
            ingress::RawLiveTailError::kNone) {
            return InitializationFailure("Raw live tail attaches");
        }

        control::ControlDecoderConfigV1 control_config{};
        control_config.source_stream_id = spec.source_stream_id;
        control_config.capture_date = kCaptureDate;
        control_config.stream_day_id = source.segment.header.stream_day_id;
        control_config.stable_config_sha256 = stable_config_sha256;
        control_config.required = spec.required;
        control_config.optional = spec.optional;
        std::unique_ptr<control::ControlDecoderV1> control_decoder;
        if (control::ControlDecoderV1::Create(
                control_config, &control_decoder) !=
            control::ControlDecoderCreateErrorV1::kNone) {
            return InitializationFailure("control decoder creates");
        }

        canonical::CanonicalNormalizerConfigV1 normalizer_config{};
        normalizer_config.capture_date = kCaptureDate;
        normalizer_config.trade_date = kTradeDate;
        normalizer_config.source_stream_id = spec.source_stream_id;
        normalizer_config.stream_day_id =
            source.segment.header.stream_day_id;
        normalizer_config.shard_count = 1U;
        normalizer_config.instrument_registry = registry.get();
        normalizer_config.sequence_policy.policy_version = 1U;
        std::unique_ptr<canonical::CanonicalNormalizerV1> normalizer;
        if (canonical::CanonicalNormalizerV1::Create(
                normalizer_config, &normalizer) !=
            canonical::CanonicalNormalizerCreateErrorV1::kNone) {
            return InitializationFailure("Canonical normalizer creates");
        }

        std::vector<runtime::ProductionCanonicalSinkV1> sinks;
        if (!AddSink(
                canonical::CanonicalFamilyV1::kSnapshot,
                canonical::CanonicalEventTypeV1::kSnapshot,
                static_cast<std::uint32_t>(
                    canonical::kCanonicalSnapshotRecordBytesV1),
                1U,
                &sinks) ||
            !AddSink(
                canonical::CanonicalFamilyV1::kTick,
                canonical::CanonicalEventTypeV1::kTick,
                static_cast<std::uint32_t>(
                    canonical::kCanonicalTickRecordBytesV1),
                2U,
                &sinks) ||
            !AddSink(
                canonical::CanonicalFamilyV1::kQuality,
                canonical::CanonicalEventTypeV1::kQuality,
                static_cast<std::uint32_t>(
                    canonical::kCanonicalQualityRecordBytesV1),
                3U,
                &sinks) ||
            !AddSink(
                canonical::CanonicalFamilyV1::kControl,
                canonical::CanonicalEventTypeV1::kControl,
                static_cast<std::uint32_t>(
                    canonical::kCanonicalControlRecordBytesV1),
                4U,
                &sinks)) {
            return InitializationFailure(
                "complete Canonical sink manifest creates");
        }

        runtime::ProductionSourcePipelineConfigV1 pipeline_config{};
        pipeline_config.source_slot = source_slot;
        pipeline_config.ingress_kind = kind;
        pipeline_config.trade_date = kTradeDate;
        pipeline_config.source_generation = kSourceGeneration;
        pipeline_config.canonical_generation = kCanonicalGeneration;
        pipeline_config.normalizer_build_sha256 =
            normalizer_build_sha256;
        pipeline_config.normalizer_config_sha256 =
            normalizer_config_sha256;
        const auto create_error =
            runtime::ProductionSourcePipelineV1::Create(
                pipeline_config,
                std::move(tail),
                std::move(control_decoder),
                std::move(normalizer),
                &frontier,
                std::move(sinks),
                history.get(),
                &pipeline);
        if (create_error !=
                runtime::ProductionSourceCreateErrorV1::kNone ||
            pipeline == nullptr) {
            return InitializationFailure(
                runtime::ProductionSourceCreateErrorNameV1(create_error));
        }
        return true;
    }

    [[nodiscard]] bool PublishAppend(std::size_t record_index) {
        if (record_index >= source.records.size()) {
            return false;
        }
        return PublishAppend(source.records[record_index]);
    }

    sdk::IngressKind ingress_kind = sdk::IngressKind::ShSnapshot;
    std::uint8_t source_slot = 0U;
    MemorySource source;
    TempDirectory directory;
    common::Identity128 writer_instance{};
    common::Sha256Digest stable_config_sha256{};
    common::Sha256Digest normalizer_build_sha256{};
    common::Sha256Digest normalizer_config_sha256{};
    std::unique_ptr<market::InstrumentRegistryV1> registry;
    std::unique_ptr<market::InstrumentHistoryRuntimeV1> history;
    canonical::SourceFrontierPageV1 frontier{};
    std::unique_ptr<runtime::ProductionSourcePipelineV1> pipeline;

private:
    [[nodiscard]] bool PublishAppend(
        const SourceRecordProgress& record) {
        return canonical::PublishAppendProgressV1(
                   &frontier,
                   writer_instance,
                   kSourceGeneration,
                   record.ingress_sequence,
                   record.record_end_wal_pos,
                   record.recv_monotonic_ns) ==
               canonical::SourceFrontierErrorV1::kNone;
    }

    [[nodiscard]] bool InitializationFailure(std::string_view stage) {
        test_->Expect(false, stage);
        return false;
    }

    [[nodiscard]] bool AddSink(
        canonical::CanonicalFamilyV1 family,
        canonical::CanonicalEventTypeV1 event_type,
        std::uint32_t record_size,
        std::uint64_t segment_sequence,
        std::vector<runtime::ProductionCanonicalSinkV1>* sinks) {
        const sdk::IngressSpec& spec = sdk::GetIngressSpec(ingress_kind);
        canonical::CanonicalSegmentDescriptorV1 descriptor{};
        descriptor.event_type = event_type;
        descriptor.record_size = record_size;
        descriptor.source_stream_id = spec.source_stream_id;
        descriptor.shard = 0U;
        descriptor.trade_date = kTradeDate;
        descriptor.origin_capture_date = kCaptureDate;
        descriptor.origin_stream_day_id = source.segment.header.stream_day_id;
        descriptor.origin_source_writer_instance = writer_instance;
        descriptor.origin_source_generation = kSourceGeneration;
        descriptor.clock_epoch = Clock();
        descriptor.schema_sha256 =
            canonical::CanonicalSchemaDescriptorSha256V1();
        descriptor.dtype_sha256 =
            canonical::CanonicalDtypeDescriptorSha256V1();
        descriptor.registry_version = registry->registry_version();
        descriptor.registry_sha256 = registry->registry_sha256();
        descriptor.normalizer_build_sha256 = normalizer_build_sha256;
        descriptor.normalizer_config_sha256 = normalizer_config_sha256;
        descriptor.generation = kCanonicalGeneration;
        descriptor.segment_sequence = segment_sequence;
        descriptor.capacity_records = 8U;

        const std::string stem = "sink-" +
            std::to_string(static_cast<unsigned int>(segment_sequence));
        canonical::CanonicalSegmentCreateOptionsV1 options{};
        options.segment_path = directory.path() / (stem + ".clog");
        options.manifest_path = directory.path() / (stem + ".manifest");
        options.descriptor = descriptor;
        options.created_realtime_ns = 1;
        options.created_monotonic_ns = 1;
        std::unique_ptr<canonical::CanonicalSegmentWriterV1> writer;
        if (canonical::CanonicalSegmentWriterV1::Create(
                options, &writer) !=
            canonical::CanonicalSegmentErrorV1::kNone) {
            return false;
        }
        if (writer->AdvanceProcessedRaw(
                0U, ingress::kRawV1SegmentHeaderBytes) !=
            canonical::CanonicalSegmentErrorV1::kNone) {
            return false;
        }
        sinks->push_back(runtime::ProductionCanonicalSinkV1{
            family, 0U, std::move(writer)});
        return true;
    }

    TestContext* test_ = nullptr;
};

struct BarrierGate final {
    std::atomic<bool> entered{false};
    std::atomic<bool> released{false};
};

void GateFirstHistoryAppend(
    void* context,
    std::uint32_t,
    const market::OwnedInstrumentEventEnvelopeV1& envelope) noexcept {
    auto* gate = static_cast<BarrierGate*>(context);
    if (gate != nullptr && envelope.dispatch_ticket() == 1U) {
        gate->entered.store(true, std::memory_order_release);
        while (!gate->released.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }
}

[[nodiscard]] bool WaitUntil(
    const std::atomic<bool>& value,
    std::chrono::milliseconds timeout = 2s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!value.load(std::memory_order_acquire)) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::yield();
    }
    return true;
}

void TestRawDecodeCanonicalHistory(TestContext* test) {
    const RawMessageSpec message{
        sdk::MessageKey{6U, 101U, 33U}, ShenzhenOrderBody()};
    PipelineHarness harness(test);
    if (!harness.Initialize(
            sdk::IngressKind::SzTick, std::span(&message, 1U))) {
        return;
    }
    test->Expect(
        harness.pipeline->history_runtime() == harness.history.get(),
        "pipeline exposes the exact borrowed history runtime identity");

    const runtime::ProductionSourceStepResultV1 step =
        harness.pipeline->Step();
    test->Expect(
        step.kind == runtime::ProductionSourceStepKindV1::kProgress &&
            step.failure == runtime::ProductionSourceFailureV1::kNone &&
            step.ingress_sequence == 1U,
        "Raw record passes control, one market decode and Canonical commit");

    const market::InstrumentHistoryBarrierV1 barrier =
        harness.history->CaptureBarrier(harness.source_slot);
    test->Expect(
        harness.history->WaitForBarrier(barrier, 2s) ==
            market::InstrumentHistoryBarrierWaitErrorV1::kNone,
        "instrument worker acknowledges the submitted source prefix");

    market::InstrumentHistoryRecordHandleV1 latest;
    const auto query = harness.history->Latest(
        kInstrumentId,
        harness.source_slot,
        market::InstrumentHistoryLaneV1::kTick,
        &latest);
    test->Expect(
        query == market::InstrumentHistoryQueryErrorV1::kNone && latest &&
            latest->source_stream_id() == 2002U &&
            latest->source_sequence() == 1U &&
            latest->instrument_id() == kInstrumentId &&
            latest->kind() == market::MarketEventKindV1::kShenzhenOrder,
        "decoded order is queryable by instrument in the fixed tick lane");

    const runtime::ProductionSourceSnapshotV1 snapshot =
        harness.pipeline->Snapshot();
    const canonical::SourceFrontierV1 frontier =
        harness.pipeline->Frontier();
    const control::ControlDecoderSnapshotV1 control_snapshot =
        harness.pipeline->ControlSnapshot();
    const runtime::ProductionSourceActivationEvidenceV1 activation =
        harness.pipeline->CaptureActivationEvidence();
    const runtime::ProductionSourceActiveLocalEvidenceV1 active_local =
        harness.pipeline->CaptureActiveLocalEvidence();
    const runtime::ProductionSourceActiveValidityEvidenceV1 active_full =
        harness.pipeline->CaptureActiveValidityEvidence();
    test->Expect(
        snapshot.raw_records == 1U && snapshot.market_records == 1U &&
            snapshot.history_submissions == 1U &&
            snapshot.no_output_records == 0U && !snapshot.fatal,
        "pipeline counters expose the one committed market/history record");
    test->Expect(
        frontier.processed_ingress_sequence == 1U &&
            frontier.processed_global_wal_pos == step.wal_pos &&
            control_snapshot.processed_ingress_sequence == 1U,
        "control and Canonical frontiers converge on the exact Raw record");
    test->Expect(
        activation.raw_control.ok() &&
            activation.source_frontier_read_error ==
                canonical::SourceFrontierErrorV1::kNone &&
            activation.control.processed_ingress_sequence == 1U &&
            activation.source_frontier.processed_ingress_sequence == 1U &&
            activation.history_barrier.valid &&
            activation.history_barrier.source_slot == harness.source_slot &&
            activation.history_barrier.ticket ==
                activation.pipeline.history_frontier.submitted_ticket,
        "activation evidence is one source-local execution-mutex sample");
    test->Expect(
        active_local.control.processed_ingress_sequence == 1U &&
            active_local.source_frontier.processed_ingress_sequence == 1U &&
            !active_local.pipeline_fatal && active_full.raw_control.ok() &&
            active_full.control.processed_ingress_sequence == 1U,
        "active monitor exposes allocation-free local and fresh-Raw forms");
}

void TestOptionalMessageAdvancesWithoutHistory(TestContext* test) {
    const RawMessageSpec optional{
        sdk::MessageKey{6U, 101U, 29U},
        std::vector<std::byte>{std::byte{0x5aU}}};
    PipelineHarness harness(test);
    if (!harness.Initialize(
            sdk::IngressKind::SzSnapshot, std::span(&optional, 1U))) {
        return;
    }

    const runtime::ProductionSourceStepResultV1 step =
        harness.pipeline->Step();
    const runtime::ProductionSourceSnapshotV1 snapshot =
        harness.pipeline->Snapshot();
    test->Expect(
        step.kind == runtime::ProductionSourceStepKindV1::kProgress &&
            snapshot.raw_records == 1U && snapshot.no_output_records == 1U &&
            snapshot.market_records == 0U &&
            snapshot.history_submissions == 0U && !snapshot.fatal,
        "declared optional message commits progress without business output");

    market::InstrumentHistoryRecordHandleV1 latest;
    test->Expect(
        harness.history->Latest(
            kInstrumentId,
            harness.source_slot,
            market::InstrumentHistoryLaneV1::kSnapshot,
            &latest) == market::InstrumentHistoryQueryErrorV1::kNotFound &&
            !latest,
        "optional no-output record is never published to instrument history");
    test->Expect(
        harness.pipeline->Frontier().processed_ingress_sequence == 1U &&
            harness.pipeline->ControlSnapshot().processed_ingress_sequence ==
                1U,
        "optional path advances both authoritative cursors exactly once");
}

void TestUnexpectedMessageFailStopsSource(TestContext* test) {
    const RawMessageSpec unexpected{
        sdk::MessageKey{6U, 101U, 29U},
        std::vector<std::byte>{std::byte{0x7bU}}};
    PipelineHarness harness(test);
    if (!harness.Initialize(
            sdk::IngressKind::SzTick, std::span(&unexpected, 1U))) {
        return;
    }

    const runtime::ProductionSourceStepResultV1 step =
        harness.pipeline->Step();
    const runtime::ProductionSourceSnapshotV1 snapshot =
        harness.pipeline->Snapshot();
    const canonical::SourceFrontierV1 frontier =
        harness.pipeline->Frontier();
    test->Expect(
        step.kind == runtime::ProductionSourceStepKindV1::kFatal &&
            step.failure ==
                runtime::ProductionSourceFailureV1::kUnexpectedMessage &&
            snapshot.fatal,
        "message outside the source manifest fails closed");
    test->Expect(
        frontier.source_state == canonical::SourceStateV1::kFatal &&
            frontier.append_ingress_sequence == 1U &&
            frontier.processed_ingress_sequence == 0U,
        "failed record remains uncommitted while the source is revoked");

    market::InstrumentHistoryRecordHandleV1 latest;
    test->Expect(
        harness.history->Latest(
            kInstrumentId,
            harness.source_slot,
            market::InstrumentHistoryLaneV1::kTick,
            &latest) ==
            market::InstrumentHistoryQueryErrorV1::kSourceFatal,
            "source revocation also closes its direct history query path");
}

void TestIdleTailObservesExternalFrontierFatal(TestContext* test) {
    PipelineHarness harness(test);
    if (!harness.Initialize(
            sdk::IngressKind::SzTick,
            std::span<const RawMessageSpec>{})) {
        return;
    }

    const runtime::ProductionSourceStepResultV1 idle =
        harness.pipeline->Step();
    test->Expect(
        idle.kind == runtime::ProductionSourceStepKindV1::kWouldBlock,
        "empty unsealed Raw tail initially would-blocks");
    test->Expect(
        canonical::PublishSourceStateV1(
            &harness.frontier,
            harness.writer_instance,
            kSourceGeneration,
            canonical::SourceStateV1::kFatal,
            0x55U) == canonical::SourceFrontierErrorV1::kNone,
        "external producer can revoke the shared SourceFrontier");

    const runtime::ProductionSourceStepResultV1 fatal =
        harness.pipeline->Step();
    const runtime::ProductionSourceSnapshotV1 snapshot =
        harness.pipeline->Snapshot();
    test->Expect(
        fatal.kind == runtime::ProductionSourceStepKindV1::kFatal &&
            fatal.failure ==
                runtime::ProductionSourceFailureV1::kFrontierFailure &&
            fatal.frontier_error ==
                canonical::SourceFrontierErrorV1::kInvalidState,
        "next Step observes external fatal even while Raw remains idle");
    test->Expect(
        snapshot.fatal &&
            snapshot.terminal.kind ==
                runtime::ProductionSourceStepKindV1::kFatal &&
            snapshot.terminal.failure == fatal.failure &&
            snapshot.terminal.frontier_error == fatal.frontier_error &&
            snapshot.terminal.ingress_sequence == fatal.ingress_sequence &&
            snapshot.terminal.wal_pos == fatal.wal_pos,
        "fatal snapshot publishes the complete terminal result atomically");
}

void TestProducerAppendLagDefersDecode(TestContext* test) {
    const RawMessageSpec message{
        sdk::MessageKey{6U, 101U, 33U}, ShenzhenOrderBody()};
    PipelineHarness harness(test);
    if (!harness.Initialize(
            sdk::IngressKind::SzTick,
            std::span(&message, 1U),
            false)) {
        return;
    }

    const runtime::ProductionSourceStepResultV1 lagging =
        harness.pipeline->Step();
    const control::ControlDecoderSnapshotV1 before =
        harness.pipeline->ControlSnapshot();
    const runtime::ProductionSourceSnapshotV1 before_snapshot =
        harness.pipeline->Snapshot();
    test->Expect(
        lagging.kind == runtime::ProductionSourceStepKindV1::kWouldBlock &&
            lagging.failure ==
                runtime::ProductionSourceFailureV1::kNone &&
            before.next_ingress_sequence == 1U &&
            before.processed_ingress_sequence == 0U &&
            before_snapshot.raw_records == 0U &&
            before_snapshot.market_records == 0U,
        "append-lagged pending Raw record does not enter either decoder");
    test->Expect(
        harness.PublishAppend(0U),
        "producer publishes append coverage for the pending Raw record");

    const runtime::ProductionSourceStepResultV1 committed =
        harness.pipeline->Step();
    const runtime::ProductionSourceStepResultV1 idle =
        harness.pipeline->Step();
    const control::ControlDecoderSnapshotV1 after =
        harness.pipeline->ControlSnapshot();
    const runtime::ProductionSourceSnapshotV1 after_snapshot =
        harness.pipeline->Snapshot();
    test->Expect(
        committed.kind == runtime::ProductionSourceStepKindV1::kProgress &&
            committed.ingress_sequence == 1U &&
            idle.kind == runtime::ProductionSourceStepKindV1::kWouldBlock,
        "covered pending Raw record commits exactly once then tail idles");
    test->Expect(
        after.next_ingress_sequence == 2U &&
            after.processed_ingress_sequence == 1U &&
            after_snapshot.raw_records == 1U &&
            after_snapshot.market_records == 1U &&
            after_snapshot.history_submissions == 1U,
        "coverage retry advances control, decode and history once only");
}

void TestHistoryBackpressureDoesNotRedecode(TestContext* test) {
    BarrierGate gate;
    auto history_config = HistoryConfig();
    history_config.physical_worker_count = 1U;
    history_config.queue_capacity = 1U;
    history_config.before_append_hook = &GateFirstHistoryAppend;
    history_config.before_append_hook_context = &gate;
    const std::array<RawMessageSpec, 3U> messages{{
        {sdk::MessageKey{6U, 101U, 33U}, ShenzhenOrderBody(9001)},
        {sdk::MessageKey{6U, 101U, 33U}, ShenzhenOrderBody(9002)},
        {sdk::MessageKey{6U, 101U, 33U}, ShenzhenOrderBody(9003)},
    }};
    PipelineHarness harness(test);
    if (!harness.Initialize(
            sdk::IngressKind::SzTick,
            messages,
            true,
            &history_config)) {
        return;
    }

    const auto first = harness.pipeline->Step();
    const bool worker_blocked = WaitUntil(gate.entered);
    test->Expect(
        first.kind == runtime::ProductionSourceStepKindV1::kProgress &&
            worker_blocked,
        "first history append is deterministically held by its worker");
    const auto second = harness.pipeline->Step();
    const auto third = harness.pipeline->Step();
    const auto blocked_retry = harness.pipeline->Step();
    const std::uint64_t third_wal_pos =
        harness.source.records[2U].record_end_wal_pos;
    const auto blocked_snapshot = harness.pipeline->Snapshot();
    const auto blocked_control = harness.pipeline->ControlSnapshot();
    test->Expect(
        second.kind == runtime::ProductionSourceStepKindV1::kProgress &&
            third.kind ==
                runtime::ProductionSourceStepKindV1::kBackpressure &&
            blocked_retry.kind ==
                runtime::ProductionSourceStepKindV1::kBackpressure &&
            third.ingress_sequence == 3U &&
            blocked_retry.ingress_sequence == 3U &&
            third.wal_pos == third_wal_pos &&
            blocked_retry.wal_pos == third_wal_pos,
        "full history queue retains the third envelope and Raw identity "
        "across retries");
    test->Expect(
        blocked_snapshot.raw_records == 3U &&
            blocked_snapshot.market_records == 3U &&
            blocked_snapshot.history_submissions == 2U &&
            blocked_snapshot.history_pending &&
            blocked_control.processed_ingress_sequence == 3U,
        "history retry does not repeat control, decode or Canonical commit");

    gate.released.store(true, std::memory_order_release);
    runtime::ProductionSourceStepResultV1 admitted{};
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    do {
        admitted = harness.pipeline->Step();
        if (admitted.kind !=
            runtime::ProductionSourceStepKindV1::kBackpressure) {
            break;
        }
        std::this_thread::yield();
    } while (std::chrono::steady_clock::now() < deadline);
    const auto admitted_snapshot = harness.pipeline->Snapshot();
    test->Expect(
        admitted.kind == runtime::ProductionSourceStepKindV1::kProgress &&
            admitted.ingress_sequence == 3U &&
            admitted.wal_pos == third_wal_pos &&
            admitted_snapshot.raw_records == 3U &&
            admitted_snapshot.market_records == 3U &&
            admitted_snapshot.history_submissions == 3U &&
            !admitted_snapshot.history_pending,
        "retained envelope is admitted after capacity returns without decode");
}

void TestEndWaitsForHistoryBarrier(TestContext* test) {
    BarrierGate gate;
    auto history_config = HistoryConfig();
    history_config.physical_worker_count = 1U;
    history_config.before_append_hook = &GateFirstHistoryAppend;
    history_config.before_append_hook_context = &gate;
    const RawMessageSpec message{
        sdk::MessageKey{6U, 101U, 33U}, ShenzhenOrderBody()};
    PipelineHarness harness(test);
    if (!harness.Initialize(
            sdk::IngressKind::SzTick,
            std::span(&message, 1U),
            true,
            &history_config)) {
        return;
    }

    const auto committed = harness.pipeline->Step();
    const bool worker_blocked = WaitUntil(gate.entered);
    harness.source.sealed = true;
    const auto draining = harness.pipeline->Step();
    const auto draining_snapshot = harness.pipeline->Snapshot();
    test->Expect(
        committed.kind == runtime::ProductionSourceStepKindV1::kProgress &&
            worker_blocked &&
            draining.kind ==
                runtime::ProductionSourceStepKindV1::kBackpressure &&
            draining.history_barrier_error == market::
                InstrumentHistoryBarrierWaitErrorV1::kTimeout,
        "Raw end waits while its exact history barrier is unacknowledged");
    test->Expect(
        draining_snapshot.history_draining &&
            !draining_snapshot.ended && !draining_snapshot.fatal,
        "draining state is visible without prematurely reporting end");

    gate.released.store(true, std::memory_order_release);
    runtime::ProductionSourceStepResultV1 ended{};
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    do {
        ended = harness.pipeline->Step();
        if (ended.kind !=
            runtime::ProductionSourceStepKindV1::kBackpressure) {
            break;
        }
        std::this_thread::yield();
    } while (std::chrono::steady_clock::now() < deadline);
    const auto ended_snapshot = harness.pipeline->Snapshot();
    test->Expect(
        ended.kind == runtime::ProductionSourceStepKindV1::kEnd &&
            ended_snapshot.ended && !ended_snapshot.history_draining &&
            ended_snapshot.history_frontier.acknowledged_source_sequence ==
                1U,
        "pipeline reports end only after the history barrier is complete");
}

void TestUnfinishedDestructionFailsClosed(TestContext* test) {
    PipelineHarness harness(test);
    if (!harness.Initialize(
            sdk::IngressKind::SzTick,
            std::span<const RawMessageSpec>{})) {
        return;
    }

    harness.pipeline.reset();
    canonical::SourceFrontierV1 frontier{};
    const auto read_error = canonical::ReadSourceFrontierV1(
        harness.frontier, &frontier);
    test->Expect(
        read_error == canonical::SourceFrontierErrorV1::kNone &&
            frontier.source_state == canonical::SourceStateV1::kFatal &&
            harness.history->Frontier(harness.source_slot).fatal,
        "unfinished destruction revokes SourceFrontier and history access");
    test->Expect(
        runtime::ProductionSourceFailureNameV1(
            runtime::ProductionSourceFailureV1::kLifecycleAbort) ==
            "lifecycle_abort",
        "unfinished-owner failure has a distinct lifecycle_abort term");
}

}  // namespace

int main() {
    TestContext test;
    TestRawDecodeCanonicalHistory(&test);
    TestOptionalMessageAdvancesWithoutHistory(&test);
    TestUnexpectedMessageFailStopsSource(&test);
    TestIdleTailObservesExternalFrontierFatal(&test);
    TestProducerAppendLagDefersDecode(&test);
    TestHistoryBackpressureDoesNotRedecode(&test);
    TestEndWaitsForHistoryBarrier(&test);
    TestUnfinishedDestructionFailsClosed(&test);
    if (test.failures() != 0) {
        std::cerr << test.failures() << " test(s) failed\n";
        return 1;
    }
    std::cout << "production source pipeline tests passed\n";
    return 0;
}
