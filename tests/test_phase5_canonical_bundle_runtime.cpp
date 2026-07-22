#include "l2flow/canonical/canonical_bundle_runtime_v1.h"
#include "l2flow/common/sha256.h"
#include "l2flow/control/control_record_v1.h"
#include "l2flow/control/quality_flags_v1.h"
#include "l2flow/market/instrument_registry.h"

#include <atomic>
#include <array>
#include <cerrno>
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
#include <vector>

#include <fcntl.h>
#include <unistd.h>

namespace canonical = l2flow::canonical;
namespace common = l2flow::common;
namespace control = l2flow::control;
namespace ingress = l2flow::ingress;
namespace market = l2flow::market;

namespace {

constexpr std::uint32_t kCaptureDate = 20260723U;
constexpr std::uint32_t kTradeDate = 20260722U;
constexpr std::uint32_t kSource = 404U;
constexpr std::size_t kSourceFrontierProgressGenerationOffset = 192U;

constexpr std::uint64_t Wal(std::uint64_t ingress) noexcept {
    return 4096U + ingress * 4096U;
}

struct TestContext final {
    int failures = 0;

    void Expect(bool condition, std::string_view label) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << label << '\n';
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

bool PwriteExact(
    const std::filesystem::path& path,
    std::span<const std::byte> bytes,
    std::uint64_t offset) {
    const int descriptor = ::open(
        path.c_str(), O_RDWR | O_CLOEXEC | O_NOFOLLOW);
    if (descriptor < 0) {
        return false;
    }
    std::size_t written = 0U;
    while (written < bytes.size()) {
        const ssize_t count = ::pwrite(
            descriptor,
            bytes.data() + written,
            bytes.size() - written,
            static_cast<off_t>(offset + written));
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            static_cast<void>(::close(descriptor));
            return false;
        }
        written += static_cast<std::size_t>(count);
    }
    const bool synced = ::fsync(descriptor) == 0;
    const bool closed = ::close(descriptor) == 0;
    return synced && closed;
}

class TempDirectory final {
public:
    TempDirectory() {
        std::array<char, 48U> pattern{};
        const std::string source = "/tmp/l2flow_phase5_bundle_XXXXXX";
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

class WireWriter final {
public:
    explicit WireWriter(std::size_t fixed) : bytes_(fixed, std::byte{0U}) {}

    void U16(std::size_t offset, std::uint16_t value) {
        bytes_.at(offset) = static_cast<std::byte>(value & 0xffU);
        bytes_.at(offset + 1U) =
            static_cast<std::byte>((value >> 8U) & 0xffU);
    }
    void U32(std::size_t offset, std::uint32_t value) {
        for (std::size_t index = 0U; index < 4U; ++index) {
            bytes_.at(offset + index) = static_cast<std::byte>(
                (value >> (index * 8U)) & 0xffU);
        }
    }
    void U64(std::size_t offset, std::uint64_t value) {
        for (std::size_t index = 0U; index < 8U; ++index) {
            bytes_.at(offset + index) = static_cast<std::byte>(
                (value >> (index * 8U)) & 0xffU);
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
        for (char character : text) {
            bytes_.push_back(static_cast<std::byte>(character));
        }
        U16(descriptor, static_cast<std::uint16_t>(text.size()));
        U32(descriptor + 2U,
            static_cast<std::uint32_t>(begin - descriptor));
    }
    std::vector<std::byte> Take() && { return std::move(bytes_); }

private:
    std::vector<std::byte> bytes_;
};

std::vector<std::byte> ShenzhenOrder(std::int64_t sequence) {
    WireWriter writer(58U);
    writer.U32(0U, 12U);
    writer.I64(4U, sequence);
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

std::vector<std::byte> Bytes(std::string_view value) {
    const auto bytes = std::as_bytes(std::span(value.data(), value.size()));
    return std::vector<std::byte>(bytes.begin(), bytes.end());
}

std::unique_ptr<market::InstrumentRegistryV1> Registry() {
    market::InstrumentRegistryEntryV1 entry{};
    entry.instrument_id = 50U;
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

canonical::ClockEpochIdentityV1 Clock() {
    canonical::ClockEpochIdentityV1 clock{};
    clock.algorithm = 1U;
    clock.digest = Pattern<32U>(0x41U);
    clock.label = 17U;
    return clock;
}

struct HookState final {
    bool observe_before_frontier = false;
    bool fail_second_publish = false;
    bool fail_frontier_commit = false;
    bool pause_frontier_commit = false;
    bool observed = false;
    canonical::SourceFrontierPageV1* frontier = nullptr;
    canonical::CanonicalSegmentReaderV1* tick_reader = nullptr;
    canonical::CanonicalSegmentReaderV1* quality_reader = nullptr;
    canonical::CanonicalCommittedReadErrorV1 tick_visibility =
        canonical::CanonicalCommittedReadErrorV1::kNone;
    canonical::CanonicalCommittedReadErrorV1 quality_visibility =
        canonical::CanonicalCommittedReadErrorV1::kNone;
    std::atomic<bool> frontier_commit_locked{false};
    std::atomic<std::uint64_t> frontier_release_generation{0U};
};

std::uint64_t LoadFrontierProgressGeneration(
    const canonical::SourceFrontierPageV1& page) noexcept {
    const auto* value = reinterpret_cast<const std::uint64_t*>(
        page.bytes.data() +
        static_cast<std::ptrdiff_t>(
            kSourceFrontierProgressGenerationOffset));
    return __atomic_load_n(value, __ATOMIC_ACQUIRE);
}

void StoreFrontierProgressGeneration(
    canonical::SourceFrontierPageV1* page,
    std::uint64_t value) noexcept {
    auto* destination = reinterpret_cast<std::uint64_t*>(
        page->bytes.data() +
        static_cast<std::ptrdiff_t>(
            kSourceFrontierProgressGenerationOffset));
    __atomic_store_n(destination, value, __ATOMIC_RELEASE);
}

struct VerifierState final {
    bool reject = false;
    bool fail_normalizer_during_market_verify = false;
    bool fail_normalizer_during_control_verify = false;
    canonical::CanonicalNormalizerV1* normalizer = nullptr;
};

enum class ManifestMutation {
    kNone,
    kMissingSnapshot,
    kDuplicateTick,
    kIllegalQualityShard,
};

bool OperationHook(
    void* opaque,
    canonical::CanonicalBundleOperationV1 operation,
    std::size_t ordinal) noexcept {
    auto* state = static_cast<HookState*>(opaque);
    if (state->fail_second_publish &&
        operation == canonical::CanonicalBundleOperationV1::kPublishRecord &&
        ordinal == 1U) {
        return false;
    }
    if (state->fail_frontier_commit &&
        operation ==
            canonical::CanonicalBundleOperationV1::kCommitSourceFrontier) {
        return false;
    }
    if (state->pause_frontier_commit &&
        operation ==
            canonical::CanonicalBundleOperationV1::kCommitSourceFrontier) {
        const std::uint64_t even =
            LoadFrontierProgressGeneration(*state->frontier);
        if ((even & 1U) != 0U ||
            even > std::numeric_limits<std::uint64_t>::max() - 2U) {
            return false;
        }
        StoreFrontierProgressGeneration(state->frontier, even + 1U);
        state->frontier_release_generation.store(
            even + 2U, std::memory_order_release);
        state->frontier_commit_locked.store(
            true, std::memory_order_release);
    }
    if (state->observe_before_frontier &&
        operation ==
            canonical::CanonicalBundleOperationV1::kCommitSourceFrontier) {
        std::span<const std::byte> record;
        canonical::SourceFrontierV1 proof;
        state->tick_visibility =
            canonical::ReadCommittedCanonicalRecordV1(
                *state->frontier, *state->tick_reader, 1U,
                &record, &proof);
        state->quality_visibility =
            canonical::ReadCommittedCanonicalRecordV1(
                *state->frontier, *state->quality_reader, 0U,
                &record, &proof);
        state->observed = true;
    }
    return true;
}

bool VerifyMarketEnvelope(
    void* opaque,
    const canonical::SourceFrontierV1& processed,
    const canonical::CanonicalRawContextV1& raw,
    const market::MarketMessageViewV1& message) noexcept {
    auto* state = static_cast<VerifierState*>(opaque);
    if (state != nullptr &&
        state->fail_normalizer_during_market_verify &&
        state->normalizer != nullptr) {
        state->fail_normalizer_during_market_verify = false;
        static_cast<void>(state->normalizer->FailStop(nullptr));
    }
    return state != nullptr && !state->reject &&
           processed.processed_ingress_sequence !=
               std::numeric_limits<std::uint64_t>::max() &&
           raw.origin_ingress_sequence ==
               processed.processed_ingress_sequence + 1U &&
           raw.origin_wal_end_pos == Wal(raw.origin_ingress_sequence) &&
           raw.source_writer_instance == processed.writer_instance &&
           raw.source_generation == processed.generation &&
           message.source_sequence == raw.origin_ingress_sequence &&
           message.source_stream_id == raw.source_stream_id &&
           message.trade_date == raw.trade_date &&
           message.service_id == 6U && message.service_version == 101U &&
           message.message_id == 33U && !message.body.empty();
}

bool VerifyControlEnvelope(
    void* opaque,
    const canonical::SourceFrontierV1& processed,
    const canonical::CanonicalRawContextV1& raw,
    const control::ControlRecordV1& record,
    std::int64_t recv_realtime_ns,
    std::int64_t recv_monotonic_ns) noexcept {
    auto* state = static_cast<VerifierState*>(opaque);
    if (state != nullptr &&
        state->fail_normalizer_during_control_verify &&
        state->normalizer != nullptr) {
        state->fail_normalizer_during_control_verify = false;
        static_cast<void>(state->normalizer->FailStop(nullptr));
    }
    return state != nullptr && !state->reject &&
           processed.processed_ingress_sequence !=
               std::numeric_limits<std::uint64_t>::max() &&
           raw.origin_ingress_sequence ==
               processed.processed_ingress_sequence + 1U &&
           raw.origin_wal_end_pos == Wal(raw.origin_ingress_sequence) &&
           raw.source_writer_instance == processed.writer_instance &&
           raw.source_generation == processed.generation &&
           record.origin_ingress_sequence == raw.origin_ingress_sequence &&
           record.origin_record_end_wal_pos == raw.origin_wal_end_pos &&
           recv_realtime_ns == static_cast<std::int64_t>(
               raw.origin_ingress_sequence * 20U) &&
           recv_monotonic_ns == static_cast<std::int64_t>(
               raw.origin_ingress_sequence * 10U);
}

bool VerifyNoOutputEnvelope(
    void* opaque,
    const canonical::SourceFrontierV1& processed,
    const canonical::CanonicalRawContextV1& raw,
    const market::MarketMessageViewV1& message,
    canonical::CanonicalNoOutputReasonV1 reason) noexcept {
    const auto* state = static_cast<const VerifierState*>(opaque);
    return state != nullptr && !state->reject &&
           (reason ==
                canonical::CanonicalNoOutputReasonV1::
                    kOptionalMarketMessage ||
            reason ==
                canonical::CanonicalNoOutputReasonV1::
                    kUnmodeledControlMessage) &&
           processed.processed_ingress_sequence !=
               std::numeric_limits<std::uint64_t>::max() &&
           raw.origin_ingress_sequence ==
               processed.processed_ingress_sequence + 1U &&
           raw.origin_wal_end_pos == Wal(raw.origin_ingress_sequence) &&
           raw.source_writer_instance == processed.writer_instance &&
           raw.source_generation == processed.generation &&
           message.source_sequence == raw.origin_ingress_sequence &&
           message.source_stream_id == raw.source_stream_id &&
           message.trade_date == raw.trade_date &&
           message.service_id != 0U &&
           message.service_version != 0U &&
           message.message_id != 0U && !message.body.empty();
}

bool VerifySegmentTransition(
    void* opaque,
    const canonical::SourceFrontierV1& processed,
    const ingress::RawLiveSegmentTransitionV1& transition) noexcept {
    const auto* state = static_cast<const VerifierState*>(opaque);
    return state != nullptr && !state->reject &&
           transition.writer_instance == processed.writer_instance &&
           transition.previous_segment.source_stream_id ==
               processed.source_stream_id &&
           transition.next_segment.source_stream_id ==
               processed.source_stream_id &&
           transition.previous_segment.capture_date ==
               processed.capture_date &&
           transition.next_segment.capture_date ==
               processed.capture_date &&
           transition.previous_segment.stream_day_id ==
               processed.stream_day_id &&
           transition.next_segment.stream_day_id ==
               processed.stream_day_id;
}

class RuntimeHarness final {
public:
    [[nodiscard]] bool Initialize(
        std::uint64_t tick_capacity,
        std::uint64_t quality_capacity,
        ManifestMutation manifest_mutation = ManifestMutation::kNone) {
        registry = Registry();
        if (registry == nullptr || directory.path().empty()) {
            return false;
        }
        frontier_config.source_stream_id = kSource;
        frontier_config.capture_date = kCaptureDate;
        frontier_config.stream_day_id = Pattern<16U>(0x11U);
        frontier_config.clock_epoch = Clock();
        frontier_config.writer_instance = Pattern<16U>(0x21U);
        frontier_config.generation = 9U;
        frontier_config.initial_state = canonical::SourceStateV1::kHealthy;
        if (canonical::InitializeSourceFrontierPageV1(
                frontier_config, &frontier) !=
            canonical::SourceFrontierErrorV1::kNone) {
            return false;
        }

        canonical::CanonicalNormalizerConfigV1 normalizer_config{};
        normalizer_config.capture_date = kCaptureDate;
        normalizer_config.trade_date = kTradeDate;
        normalizer_config.source_stream_id = kSource;
        normalizer_config.stream_day_id = frontier_config.stream_day_id;
        normalizer_config.instrument_registry = registry.get();
        normalizer_config.sequence_policy.policy_version = 1U;
        normalizer_config.shard_count = 2U;
        if (canonical::CanonicalNormalizerV1::Create(
                normalizer_config, &normalizer) !=
            canonical::CanonicalNormalizerCreateErrorV1::kNone) {
            return false;
        }

        snapshot_descriptor = Descriptor(
            canonical::CanonicalEventTypeV1::kSnapshot,
            static_cast<std::uint32_t>(
                canonical::kCanonicalSnapshotRecordBytesV1),
            0U, 1U, 8U);
        extra_snapshot_descriptor = Descriptor(
            canonical::CanonicalEventTypeV1::kSnapshot,
            static_cast<std::uint32_t>(
                canonical::kCanonicalSnapshotRecordBytesV1),
            1U, 2U, 8U);
        tick_descriptor = Descriptor(
            canonical::CanonicalEventTypeV1::kTick,
            static_cast<std::uint32_t>(
                canonical::kCanonicalTickRecordBytesV1),
            0U, 3U, tick_capacity);
        extra_tick_descriptor = Descriptor(
            canonical::CanonicalEventTypeV1::kTick,
            static_cast<std::uint32_t>(
                canonical::kCanonicalTickRecordBytesV1),
            1U, 4U, tick_capacity);
        quality_descriptor = Descriptor(
            canonical::CanonicalEventTypeV1::kQuality,
            static_cast<std::uint32_t>(
                canonical::kCanonicalQualityRecordBytesV1),
            0U, 5U, quality_capacity);
        control_descriptor = Descriptor(
            canonical::CanonicalEventTypeV1::kControl,
            static_cast<std::uint32_t>(
                canonical::kCanonicalControlRecordBytesV1),
            0U, 6U, 8U);
        if (!CreateSegment(
                "snapshot", snapshot_descriptor, &snapshot_writer) ||
            !CreateSegment(
                "snapshot-1", extra_snapshot_descriptor,
                &extra_snapshot_writer) ||
            !CreateSegment("tick", tick_descriptor, &tick_writer) ||
            !CreateSegment(
                "tick-1", extra_tick_descriptor, &extra_tick_writer) ||
            !CreateSegment("quality", quality_descriptor, &quality_writer) ||
            !CreateSegment("control", control_descriptor, &control_writer)) {
            return false;
        }
        if (canonical::CanonicalSegmentReaderV1::Open(
                directory.path() / "tick.clog",
                tick_descriptor, &tick_reader) !=
                canonical::CanonicalSegmentErrorV1::kNone ||
            canonical::CanonicalSegmentReaderV1::Open(
                directory.path() / "quality.clog",
                quality_descriptor, &quality_reader) !=
                canonical::CanonicalSegmentErrorV1::kNone ||
            canonical::CanonicalSegmentReaderV1::Open(
                directory.path() / "control.clog",
                control_descriptor, &control_reader) !=
                canonical::CanonicalSegmentErrorV1::kNone) {
            return false;
        }

        hook.frontier = &frontier;
        hook.tick_reader = tick_reader.get();
        hook.quality_reader = quality_reader.get();
        verifier.normalizer = normalizer.get();
        canonical::CanonicalBundleCoordinatorConfigV1 coordinator_config{};
        coordinator_config.normalizer = normalizer.get();
        coordinator_config.source_frontier = &frontier;
        coordinator_config.sinks = {
            {canonical::CanonicalFamilyV1::kSnapshot,
             0U, snapshot_writer.get()},
            {canonical::CanonicalFamilyV1::kSnapshot,
             1U, extra_snapshot_writer.get()},
            {canonical::CanonicalFamilyV1::kTick,
             0U, tick_writer.get()},
            {canonical::CanonicalFamilyV1::kTick,
             1U, extra_tick_writer.get()},
            {canonical::CanonicalFamilyV1::kQuality,
             0U, quality_writer.get()},
            {canonical::CanonicalFamilyV1::kControl,
             0U, control_writer.get()},
        };
        switch (manifest_mutation) {
            case ManifestMutation::kNone:
                break;
            case ManifestMutation::kMissingSnapshot:
                coordinator_config.sinks.erase(
                    coordinator_config.sinks.begin());
                break;
            case ManifestMutation::kDuplicateTick:
                coordinator_config.sinks.back() =
                    coordinator_config.sinks[2U];
                break;
            case ManifestMutation::kIllegalQualityShard:
                coordinator_config.sinks[4U].shard = 1U;
                break;
        }
        coordinator_config.canonical_generation = 3U;
        coordinator_config.normalizer_build_sha256 = Pattern<32U>(0x81U);
        coordinator_config.normalizer_config_sha256 = Pattern<32U>(0xa1U);
        coordinator_config.market_envelope_verifier = VerifyMarketEnvelope;
        coordinator_config.control_envelope_verifier =
            VerifyControlEnvelope;
        coordinator_config.no_output_envelope_verifier =
            VerifyNoOutputEnvelope;
        coordinator_config.segment_transition_verifier =
            VerifySegmentTransition;
        coordinator_config.envelope_verifier_context = &verifier;
        coordinator_config.operation_hook = OperationHook;
        coordinator_config.operation_hook_context = &hook;
        coordinator_create_error =
            canonical::CanonicalBundleCoordinatorV1::Create(
                std::move(coordinator_config), &coordinator);
        return manifest_mutation == ManifestMutation::kNone
            ? coordinator_create_error ==
                  canonical::CanonicalBundleErrorV1::kNone
            : true;
    }

    [[nodiscard]] canonical::CanonicalBundleResultV1 Process(
        std::uint64_t ingress,
        std::int64_t business_sequence,
        std::uint64_t vendor_sequence) {
        const std::vector<std::byte> body =
            ShenzhenOrder(business_sequence);
        canonical::SourceFrontierCallbackGuardV1 guard(
            &frontier,
            frontier_config.writer_instance,
            frontier_config.generation);
        if (!guard.entered() ||
            guard.CompleteCaptured(ingress) !=
                canonical::SourceFrontierErrorV1::kNone ||
            canonical::PublishAppendProgressV1(
                &frontier,
                frontier_config.writer_instance,
                frontier_config.generation,
                ingress, Wal(ingress),
                static_cast<std::int64_t>(ingress * 10U)) !=
                canonical::SourceFrontierErrorV1::kNone) {
            return {canonical::CanonicalBundleErrorV1::kInvalidFrontier};
        }
        canonical::CanonicalRawContextV1 raw{};
        raw.capture_date = kCaptureDate;
        raw.trade_date = kTradeDate;
        raw.source_stream_id = kSource;
        raw.stream_day_id = frontier_config.stream_day_id;
        raw.source_writer_instance = frontier_config.writer_instance;
        raw.source_generation = frontier_config.generation;
        raw.origin_ingress_sequence = ingress;
        raw.origin_wal_end_pos = Wal(ingress);
        raw.authoritative_connection_epoch = 1U;
        raw.clock_epoch = Clock();
        market::MarketMessageViewV1 message{};
        message.source_stream_id = kSource;
        message.trade_date = kTradeDate;
        message.source_sequence = ingress;
        message.service_id = 6U;
        message.service_version = 101U;
        message.message_id = 33U;
        message.message_encoding = 1U;
        message.vendor_local_time_raw = 93000000U;
        message.vendor_sequence_id = vendor_sequence;
        message.recv_realtime_ns =
            static_cast<std::int64_t>(ingress * 20U);
        message.recv_monotonic_ns =
            static_cast<std::int64_t>(ingress * 10U);
        message.body = body;
        return coordinator->ProcessMarket(raw, message);
    }

    [[nodiscard]] canonical::CanonicalBundleResultV1 ProcessDirect(
        std::uint64_t ingress,
        std::int64_t business_sequence,
        std::uint64_t vendor_sequence) {
        const std::vector<std::byte> body =
            ShenzhenOrder(business_sequence);
        canonical::CanonicalRawContextV1 raw{};
        raw.capture_date = kCaptureDate;
        raw.trade_date = kTradeDate;
        raw.source_stream_id = kSource;
        raw.stream_day_id = frontier_config.stream_day_id;
        raw.source_writer_instance = frontier_config.writer_instance;
        raw.source_generation = frontier_config.generation;
        raw.origin_ingress_sequence = ingress;
        raw.origin_wal_end_pos = Wal(ingress);
        raw.authoritative_connection_epoch = 1U;
        raw.clock_epoch = Clock();
        market::MarketMessageViewV1 message{};
        message.source_stream_id = kSource;
        message.trade_date = kTradeDate;
        message.source_sequence = ingress;
        message.service_id = 6U;
        message.service_version = 101U;
        message.message_id = 33U;
        message.message_encoding = 1U;
        message.vendor_local_time_raw = 93000000U;
        message.vendor_sequence_id = vendor_sequence;
        message.recv_realtime_ns =
            static_cast<std::int64_t>(ingress * 20U);
        message.recv_monotonic_ns =
            static_cast<std::int64_t>(ingress * 10U);
        message.body = body;
        return coordinator->ProcessMarket(raw, message);
    }

    [[nodiscard]] canonical::SourceFrontierV1 ReadFrontier() const {
        canonical::SourceFrontierV1 value{};
        static_cast<void>(
            canonical::ReadSourceFrontierV1(frontier, &value));
        return value;
    }

    [[nodiscard]] bool AllSegmentsFatal() const {
        const std::array<const canonical::CanonicalSegmentWriterV1*, 6U>
            writers{
                snapshot_writer.get(),
                extra_snapshot_writer.get(),
                tick_writer.get(),
                extra_tick_writer.get(),
                quality_writer.get(),
                control_writer.get()};
        for (const canonical::CanonicalSegmentWriterV1* writer : writers) {
            canonical::CanonicalSegmentControlSnapshotV1 control{};
            if (writer == nullptr ||
                writer->ReadControl(&control) !=
                    canonical::CanonicalSegmentErrorV1::kNone ||
                !control.generation_fatal) {
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] bool AllSegmentsAt(
        std::uint64_t ingress,
        std::uint64_t wal) const {
        const std::array<const canonical::CanonicalSegmentWriterV1*, 6U>
            writers{
                snapshot_writer.get(),
                extra_snapshot_writer.get(),
                tick_writer.get(),
                extra_tick_writer.get(),
                quality_writer.get(),
                control_writer.get()};
        for (const canonical::CanonicalSegmentWriterV1* writer : writers) {
            if (writer == nullptr ||
                writer->header().processed_raw_ingress_sequence != ingress ||
                writer->header().processed_raw_wal_pos != wal) {
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] canonical::CanonicalBundleResultV1 ProcessControl(
        std::uint64_t ingress) {
        canonical::SourceFrontierCallbackGuardV1 guard(
            &frontier,
            frontier_config.writer_instance,
            frontier_config.generation);
        if (!guard.entered() ||
            guard.CompleteCaptured(ingress) !=
                canonical::SourceFrontierErrorV1::kNone ||
            canonical::PublishAppendProgressV1(
                &frontier,
                frontier_config.writer_instance,
                frontier_config.generation,
                ingress, Wal(ingress),
                static_cast<std::int64_t>(ingress * 10U)) !=
                canonical::SourceFrontierErrorV1::kNone) {
            canonical::CanonicalBundleResultV1 failed{};
            failed.error = canonical::CanonicalBundleErrorV1::kInvalidFrontier;
            return failed;
        }
        canonical::CanonicalRawContextV1 raw{};
        raw.capture_date = kCaptureDate;
        raw.trade_date = kTradeDate;
        raw.source_stream_id = kSource;
        raw.stream_day_id = frontier_config.stream_day_id;
        raw.source_writer_instance = frontier_config.writer_instance;
        raw.source_generation = frontier_config.generation;
        raw.origin_ingress_sequence = ingress;
        raw.origin_wal_end_pos = Wal(ingress);
        raw.authoritative_connection_epoch = 1U;
        raw.clock_epoch = Clock();

        control::ControlRecordV1 input{};
        input.flags = control::kControlRecordAddressHashPresent;
        input.control_type = control::ControlTypeV1::kConnecting;
        input.source_stream_id = kSource;
        input.capture_date = kCaptureDate;
        input.stream_day_id = frontier_config.stream_day_id;
        input.vendor_service_id = 1U;
        input.vendor_service_version = 101U;
        input.vendor_message_id = 1U;
        input.origin_ingress_sequence = ingress;
        input.origin_record_end_wal_pos = Wal(ingress);
        input.connection_epoch = 1U;
        input.quality_flags = control::QualityBit(
            control::QualityFlagV1::kSessionUnknown);
        input.required_count = 1U;
        input.address_sha256 = Pattern<32U>(0xc1U);
        input.control_state_sha256 = Pattern<32U>(0xd1U);
        return coordinator->ProcessControl(
            raw,
            input,
            static_cast<std::int64_t>(ingress * 20U),
            static_cast<std::int64_t>(ingress * 10U));
    }

    [[nodiscard]] canonical::CanonicalBundleResultV1 ProcessControlDirect(
        std::uint64_t ingress) {
        canonical::CanonicalRawContextV1 raw{};
        raw.capture_date = kCaptureDate;
        raw.trade_date = kTradeDate;
        raw.source_stream_id = kSource;
        raw.stream_day_id = frontier_config.stream_day_id;
        raw.source_writer_instance = frontier_config.writer_instance;
        raw.source_generation = frontier_config.generation;
        raw.origin_ingress_sequence = ingress;
        raw.origin_wal_end_pos = Wal(ingress);
        raw.authoritative_connection_epoch = 1U;
        raw.clock_epoch = Clock();

        control::ControlRecordV1 input{};
        input.flags = control::kControlRecordAddressHashPresent;
        input.control_type = control::ControlTypeV1::kConnecting;
        input.source_stream_id = kSource;
        input.capture_date = kCaptureDate;
        input.stream_day_id = frontier_config.stream_day_id;
        input.vendor_service_id = 1U;
        input.vendor_service_version = 101U;
        input.vendor_message_id = 1U;
        input.origin_ingress_sequence = ingress;
        input.origin_record_end_wal_pos = Wal(ingress);
        input.connection_epoch = 1U;
        input.quality_flags = control::QualityBit(
            control::QualityFlagV1::kSessionUnknown);
        input.required_count = 1U;
        input.address_sha256 = Pattern<32U>(0xc1U);
        input.control_state_sha256 = Pattern<32U>(0xd1U);
        return coordinator->ProcessControl(
            raw,
            input,
            static_cast<std::int64_t>(ingress * 20U),
            static_cast<std::int64_t>(ingress * 10U));
    }

    TempDirectory directory;
    std::unique_ptr<market::InstrumentRegistryV1> registry;
    canonical::SourceFrontierConfigV1 frontier_config{};
    canonical::SourceFrontierPageV1 frontier{};
    std::unique_ptr<canonical::CanonicalNormalizerV1> normalizer;
    canonical::CanonicalSegmentDescriptorV1 snapshot_descriptor{};
    canonical::CanonicalSegmentDescriptorV1 extra_snapshot_descriptor{};
    canonical::CanonicalSegmentDescriptorV1 tick_descriptor{};
    canonical::CanonicalSegmentDescriptorV1 extra_tick_descriptor{};
    canonical::CanonicalSegmentDescriptorV1 quality_descriptor{};
    canonical::CanonicalSegmentDescriptorV1 control_descriptor{};
    std::unique_ptr<canonical::CanonicalSegmentWriterV1> snapshot_writer;
    std::unique_ptr<canonical::CanonicalSegmentWriterV1>
        extra_snapshot_writer;
    std::unique_ptr<canonical::CanonicalSegmentWriterV1> tick_writer;
    std::unique_ptr<canonical::CanonicalSegmentWriterV1> extra_tick_writer;
    std::unique_ptr<canonical::CanonicalSegmentWriterV1> quality_writer;
    std::unique_ptr<canonical::CanonicalSegmentWriterV1> control_writer;
    std::unique_ptr<canonical::CanonicalSegmentReaderV1> tick_reader;
    std::unique_ptr<canonical::CanonicalSegmentReaderV1> quality_reader;
    std::unique_ptr<canonical::CanonicalSegmentReaderV1> control_reader;
    HookState hook{};
    VerifierState verifier{};
    std::unique_ptr<canonical::CanonicalBundleCoordinatorV1> coordinator;
    canonical::CanonicalBundleErrorV1 coordinator_create_error =
        canonical::CanonicalBundleErrorV1::kInvalidConfiguration;

private:
    [[nodiscard]] canonical::CanonicalSegmentDescriptorV1 Descriptor(
        canonical::CanonicalEventTypeV1 event_type,
        std::uint32_t record_size,
        std::uint32_t shard,
        std::uint64_t segment_sequence,
        std::uint64_t capacity) const {
        canonical::CanonicalSegmentDescriptorV1 descriptor{};
        descriptor.event_type = event_type;
        descriptor.record_size = record_size;
        descriptor.source_stream_id = kSource;
        descriptor.shard = shard;
        descriptor.trade_date = kTradeDate;
        descriptor.origin_capture_date = kCaptureDate;
        descriptor.origin_stream_day_id = frontier_config.stream_day_id;
        descriptor.origin_source_writer_instance =
            frontier_config.writer_instance;
        descriptor.origin_source_generation = frontier_config.generation;
        descriptor.clock_epoch = Clock();
        descriptor.schema_sha256 =
            canonical::CanonicalSchemaDescriptorSha256V1();
        descriptor.dtype_sha256 =
            canonical::CanonicalDtypeDescriptorSha256V1();
        descriptor.registry_version = 7U;
        descriptor.registry_sha256 = registry->registry_sha256();
        descriptor.normalizer_build_sha256 = Pattern<32U>(0x81U);
        descriptor.normalizer_config_sha256 = Pattern<32U>(0xa1U);
        descriptor.generation = 3U;
        descriptor.segment_sequence = segment_sequence;
        descriptor.capacity_records = capacity;
        return descriptor;
    }

    [[nodiscard]] bool CreateSegment(
        std::string_view stem,
        const canonical::CanonicalSegmentDescriptorV1& descriptor,
        std::unique_ptr<canonical::CanonicalSegmentWriterV1>* writer) {
        canonical::CanonicalSegmentCreateOptionsV1 options{};
        options.segment_path = directory.path() /
            (std::string(stem) + ".clog");
        options.manifest_path = directory.path() /
            (std::string(stem) + ".manifest");
        options.descriptor = descriptor;
        options.created_realtime_ns = 1;
        options.created_monotonic_ns = 1;
        return canonical::CanonicalSegmentWriterV1::Create(
                   options, writer) ==
               canonical::CanonicalSegmentErrorV1::kNone;
    }
};

void TestAtomicVisibility(TestContext* test) {
    RuntimeHarness harness;
    test->Expect(harness.Initialize(8U, 8U),
                 "runtime harness initializes exact source namespace");
    if (harness.coordinator == nullptr) {
        return;
    }
    test->Expect(harness.Process(1U, 10, 1U).ok(),
                 "baseline Raw bundle commits");
    harness.hook.observe_before_frontier = true;
    const canonical::CanonicalBundleResultV1 gap =
        harness.Process(2U, 12, 2U);
    test->Expect(
        gap.ok() && gap.published_records == 2U,
        "gap bundle publishes business and quality records");
    test->Expect(
        harness.hook.observed &&
            harness.hook.tick_visibility ==
                canonical::CanonicalCommittedReadErrorV1::kNotCommitted &&
            harness.hook.quality_visibility ==
                canonical::CanonicalCommittedReadErrorV1::kNotCommitted,
        "physical multi-family records stay hidden before global frontier commit");

    std::span<const std::byte> record;
    canonical::SourceFrontierV1 proof;
    test->Expect(
        canonical::ReadCommittedCanonicalRecordV1(
            harness.frontier, *harness.tick_reader, 1U,
            &record, &proof) ==
            canonical::CanonicalCommittedReadErrorV1::kNone &&
            proof.processed_ingress_sequence == 2U,
        "tick becomes visible after the one global commit point");
    test->Expect(
        canonical::ReadCommittedCanonicalRecordV1(
            harness.frontier, *harness.quality_reader, 0U,
            &record, &proof) ==
            canonical::CanonicalCommittedReadErrorV1::kNone,
        "quality becomes visible at the same committed Raw prefix");

    harness.hook.observe_before_frontier = false;
    const canonical::CanonicalBundleResultV1 control_result =
        harness.ProcessControl(3U);
    test->Expect(
        control_result.ok() && control_result.published_records == 1U &&
            harness.normalizer->Snapshot().control_records_committed == 1U &&
            harness.ReadFrontier().processed_ingress_sequence == 3U,
        "API/SYS control joins the same event-ID and frontier transaction");
    const canonical::CanonicalCommittedReadErrorV1 control_read =
        canonical::ReadCommittedCanonicalRecordV1(
            harness.frontier, *harness.control_reader, 0U,
            &record, &proof);
    canonical::CanonicalHeaderV1 control_header{};
    if (control_read == canonical::CanonicalCommittedReadErrorV1::kNone) {
        std::memcpy(&control_header, record.data(), sizeof(control_header));
    }
    test->Expect(
        control_read == canonical::CanonicalCommittedReadErrorV1::kNone &&
            control_header.shard_event_id == 1U,
        "control uses the normalizer-owned event ID and common memory reader");

    test->Expect(
        canonical::PublishSourceStateV1(
            &harness.frontier,
            harness.frontier_config.writer_instance,
            harness.frontier_config.generation,
            canonical::SourceStateV1::kFatal,
            0U) == canonical::SourceFrontierErrorV1::kNone,
        "test can latch source-only FATAL after a committed prefix");
    test->Expect(
        canonical::ReadCommittedCanonicalRecordV1(
            harness.frontier, *harness.tick_reader, 0U,
            &record, &proof) ==
                canonical::CanonicalCommittedReadErrorV1::kNotCommitted &&
            record.empty() && proof.source_stream_id == 0U,
        "SourceFrontier FATAL hides even an otherwise committed record "
        "and clears outputs");
    const canonical::CanonicalBundleResultV1 converged =
        harness.ProcessDirect(4U, 13, 4U);
    test->Expect(
        converged.error == canonical::CanonicalBundleErrorV1::kSourceFatal &&
            harness.normalizer->Snapshot().fatal &&
            harness.AllSegmentsFatal(),
        "Process entry converges source-only FATAL into every local fatal domain");
}

void TestSingleDecodeAndProgressOnlyPaths(TestContext* test) {
    RuntimeHarness harness;
    test->Expect(harness.Initialize(8U, 8U),
                 "single-decode runtime harness initializes");
    if (harness.coordinator == nullptr) {
        return;
    }

    const auto append_record = [&](std::uint64_t ingress) {
        canonical::SourceFrontierCallbackGuardV1 guard(
            &harness.frontier,
            harness.frontier_config.writer_instance,
            harness.frontier_config.generation);
        return guard.entered() &&
               guard.CompleteCaptured(ingress) ==
                   canonical::SourceFrontierErrorV1::kNone &&
               canonical::PublishAppendProgressV1(
                   &harness.frontier,
                   harness.frontier_config.writer_instance,
                   harness.frontier_config.generation,
                   ingress,
                   Wal(ingress),
                   static_cast<std::int64_t>(ingress * 10U)) ==
                   canonical::SourceFrontierErrorV1::kNone;
    };
    const auto raw_for = [&](std::uint64_t ingress,
                             std::uint64_t wal) {
        canonical::CanonicalRawContextV1 raw{};
        raw.capture_date = kCaptureDate;
        raw.trade_date = kTradeDate;
        raw.source_stream_id = kSource;
        raw.stream_day_id = harness.frontier_config.stream_day_id;
        raw.source_writer_instance =
            harness.frontier_config.writer_instance;
        raw.source_generation = harness.frontier_config.generation;
        raw.origin_ingress_sequence = ingress;
        raw.origin_wal_end_pos = wal;
        raw.authoritative_connection_epoch = 1U;
        raw.clock_epoch = Clock();
        return raw;
    };
    const auto message_for = [&](std::span<const std::byte> body,
                                 std::uint64_t ingress,
                                 std::uint16_t message_id,
                                 std::uint64_t vendor_sequence) {
        market::MarketMessageViewV1 message{};
        message.source_stream_id = kSource;
        message.trade_date = kTradeDate;
        message.source_sequence = ingress;
        message.service_id = 6U;
        message.service_version = 101U;
        message.message_id = message_id;
        message.message_encoding = 1U;
        message.vendor_local_time_raw = 93000000U;
        message.vendor_sequence_id = vendor_sequence;
        message.recv_realtime_ns =
            static_cast<std::int64_t>(ingress * 20U);
        message.recv_monotonic_ns =
            static_cast<std::int64_t>(ingress * 10U);
        message.body = body;
        return message;
    };

    std::vector<std::byte> body = ShenzhenOrder(10);
    auto message = message_for(body, 1U, 33U, 1U);
    market::MarketDecoderConfigV1 decoder_config{};
    decoder_config.trade_date = kTradeDate;
    decoder_config.source_stream_id = kSource;
    decoder_config.instrument_registry = harness.registry.get();
    decoder_config.shanghai_phase_attribution =
        market::ShanghaiPhaseAttributionModeV1::kDeferred;
    market::MarketDecoderV1 decoder(decoder_config);
    market::DecodedMarketEventV1 decoded;
    market::RetainedMarketEventV1 retained;
    const bool retained_once =
        decoder.Decode(message, &decoded) ==
            market::MarketDecodeErrorV1::kNone &&
        market::RetainMarketEventV1(std::move(decoded), &retained) ==
            market::RetainedMarketEventCreateErrorV1::kNone;
    test->Expect(
        retained_once && append_record(1U),
        "source pipeline decodes and retains first Raw exactly once");
    const auto decoded_result = harness.coordinator->ProcessDecodedMarket(
        raw_for(1U, Wal(1U)), message, retained);
    test->Expect(
        decoded_result.ok() && decoded_result.published_records == 1U &&
            harness.tick_writer->header().published_records == 1U &&
            harness.ReadFrontier().processed_ingress_sequence == 1U,
        "coordinator publishes borrowed decoded event without decode replay");

    const auto normalizer_after_market = harness.normalizer->Snapshot();
    body.assign(1U, std::byte{0x5aU});
    message = message_for(body, 2U, 99U, 77U);
    test->Expect(append_record(2U),
                 "optional Raw envelope is appended before disposition");
    harness.verifier.reject = true;
    const auto rejected_no_output = harness.coordinator->ProcessNoOutput(
        raw_for(2U, Wal(2U)),
        message,
        canonical::CanonicalNoOutputReasonV1::kOptionalMarketMessage);
    test->Expect(
        rejected_no_output.error ==
                canonical::CanonicalBundleErrorV1::kEnvelopeNotVerified &&
            harness.ReadFrontier().processed_ingress_sequence == 1U &&
            harness.AllSegmentsAt(1U, Wal(1U)),
        "unverified no-output disposition cannot consume Raw");
    harness.verifier.reject = false;
    const auto no_output = harness.coordinator->ProcessNoOutput(
        raw_for(2U, Wal(2U)),
        message,
        canonical::CanonicalNoOutputReasonV1::kOptionalMarketMessage);
    test->Expect(
        no_output.ok() && no_output.published_records == 0U &&
            harness.AllSegmentsAt(2U, Wal(2U)) &&
            harness.ReadFrontier().processed_ingress_sequence == 2U &&
            harness.normalizer->Snapshot().version ==
                normalizer_after_market.version &&
            harness.tick_writer->header().published_records == 1U &&
            harness.quality_writer->header().published_records == 0U,
        "authenticated no-output advances every cursor without fake record or normalizer mutation");

    body.assign(1U, std::byte{0U});
    message = message_for(body, 3U, 33U, 2U);
    test->Expect(append_record(3U),
                 "malformed market Raw is appended before decode outcome");
    const auto decode_failure =
        harness.coordinator->ProcessMarketDecodeFailure(
            raw_for(3U, Wal(3U)),
            message,
            market::MarketDecodeErrorV1::kTruncated);
    test->Expect(
        decode_failure.ok() &&
            decode_failure.normalize.error == canonical::
                CanonicalNormalizePrepareErrorV1::kDecodeFailureRecorded &&
            decode_failure.normalize.decode_error ==
                market::MarketDecodeErrorV1::kTruncated &&
            decode_failure.published_records == 1U &&
            harness.quality_writer->header().published_records == 1U &&
            harness.ReadFrontier().processed_ingress_sequence == 3U,
        "external decoder failure emits the existing quality plan without a second decode");

    constexpr std::uint64_t kNextHeaderEnd = Wal(4U);
    test->Expect(
        canonical::PublishAppendProgressV1(
            &harness.frontier,
            harness.frontier_config.writer_instance,
            harness.frontier_config.generation,
            3U,
            kNextHeaderEnd,
            30) == canonical::SourceFrontierErrorV1::kNone,
        "header-only WAL prefix becomes append-visible");
    ingress::RawLiveSegmentTransitionV1 transition{};
    transition.writer_instance = harness.frontier_config.writer_instance;
    transition.previous_segment.source_stream_id = kSource;
    transition.previous_segment.capture_date = kCaptureDate;
    transition.previous_segment.stream_day_id =
        harness.frontier_config.stream_day_id;
    transition.previous_segment.segment_sequence = 0U;
    transition.previous_segment.segment_base_wal_pos = 4096U;
    transition.previous_segment.clock_epoch_algorithm = Clock().algorithm;
    transition.previous_segment.clock_epoch_digest = Clock().digest;
    transition.previous_segment.clock_epoch_label = Clock().label;
    transition.previous_segment_end_offset = Wal(3U) - 4096U;
    transition.next_segment.source_stream_id = kSource;
    transition.next_segment.capture_date = kCaptureDate;
    transition.next_segment.stream_day_id =
        harness.frontier_config.stream_day_id;
    transition.next_segment.segment_sequence = 1U;
    transition.next_segment.segment_base_wal_pos = Wal(3U);
    transition.next_segment.first_ingress_sequence = 4U;
    transition.next_segment.clock_epoch_algorithm = Clock().algorithm;
    transition.next_segment.clock_epoch_digest = Clock().digest;
    transition.next_segment.clock_epoch_label = Clock().label;
    transition.next_data_begin_wal_pos = kNextHeaderEnd;
    transition.next_ingress_sequence = 4U;
    // Deliberately differs from immutable SourceFrontier generation (9).
    transition.control_generation = 1234U;

    harness.verifier.reject = true;
    const auto rejected_transition =
        harness.coordinator->ProcessSegmentTransition(transition);
    test->Expect(
        rejected_transition.error ==
                canonical::CanonicalBundleErrorV1::kEnvelopeNotVerified &&
            harness.ReadFrontier().processed_global_wal_pos == Wal(3U) &&
            harness.AllSegmentsAt(3U, Wal(3U)),
        "unverified segment transition cannot expose header-only WAL");
    harness.verifier.reject = false;
    const auto before_transition = harness.normalizer->Snapshot();
    const auto transitioned =
        harness.coordinator->ProcessSegmentTransition(transition);
    const auto frontier = harness.ReadFrontier();
    test->Expect(
        transitioned.ok() && transitioned.published_records == 0U &&
            frontier.processed_ingress_sequence == 3U &&
            frontier.processed_global_wal_pos == kNextHeaderEnd &&
            harness.AllSegmentsAt(3U, kNextHeaderEnd) &&
            harness.normalizer->Snapshot().version ==
                before_transition.version &&
            !harness.normalizer->Snapshot().fatal,
        "segment transition advances WAL only across all sinks and ignores mutable control generation as route identity");
}

void TestSegmentFatalHidesCommittedPrefix(TestContext* test) {
    RuntimeHarness harness;
    test->Expect(harness.Initialize(8U, 8U),
                 "segment-fatal visibility harness initializes");
    if (harness.coordinator == nullptr) {
        return;
    }
    test->Expect(harness.Process(1U, 10, 1U).ok(),
                 "segment-fatal visibility baseline commits");

    std::span<const std::byte> record;
    canonical::SourceFrontierV1 proof;
    test->Expect(
        canonical::ReadCommittedCanonicalRecordV1(
            harness.frontier, *harness.tick_reader, 0U,
            &record, &proof) ==
            canonical::CanonicalCommittedReadErrorV1::kNone,
        "baseline record is visible before segment fail-stop");
    test->Expect(
        harness.tick_writer->MarkGenerationFatal() ==
                canonical::CanonicalSegmentErrorV1::kNone &&
            harness.ReadFrontier().source_state ==
                canonical::SourceStateV1::kHealthy,
        "test latches segment-only FATAL while source frontier remains healthy");
    test->Expect(
        canonical::ReadCommittedCanonicalRecordV1(
            harness.frontier, *harness.tick_reader, 0U,
            &record, &proof) ==
                canonical::CanonicalCommittedReadErrorV1::kNotCommitted &&
            record.empty() && proof.source_stream_id == 0U,
        "segment generation_fatal hides the previously committed prefix "
        "and clears outputs");
    const canonical::CanonicalBundleResultV1 converged =
        harness.Process(2U, 11, 2U);
    test->Expect(
        converged.error ==
                canonical::CanonicalBundleErrorV1::kSinkPreflightFailed &&
            converged.segment_error ==
                canonical::CanonicalSegmentErrorV1::kGenerationFatal &&
            harness.ReadFrontier().source_state ==
                canonical::SourceStateV1::kFatal &&
            harness.normalizer->Snapshot().fatal &&
            harness.AllSegmentsFatal(),
        "preflight observes segment-only FATAL and revokes the complete generation");
    canonical::CanonicalSegmentSealResultV1 seal{};
    test->Expect(
        harness.quality_writer->Seal(
            canonical::CanonicalSegmentSealOptionsV1{100, 100},
            &seal) == canonical::CanonicalSegmentErrorV1::kGenerationFatal,
        "preflight fatal convergence prevents an unaffected family from sealing");
}

void TestCorruptPreflightStillPublishesGlobalAnchor(TestContext* test) {
    RuntimeHarness harness;
    test->Expect(harness.Initialize(8U, 8U),
                 "corrupt preflight harness initializes");
    if (harness.coordinator == nullptr) {
        return;
    }
    test->Expect(harness.Process(1U, 10, 1U).ok(),
                 "corrupt preflight baseline commits");

    constexpr std::uint64_t kControlMagicAndVersionOffset =
        canonical::kCanonicalSegmentHeaderBytesV1 + 8U;
    const std::array<std::byte, 8U> corrupt_word{};
    test->Expect(
        PwriteExact(
            harness.directory.path() / "snapshot-1.clog",
            corrupt_word,
            kControlMagicAndVersionOffset),
        "test corrupts one configured control page before preflight");

    const canonical::CanonicalBundleResultV1 failed =
        harness.Process(2U, 11, 2U);
    canonical::CanonicalSegmentControlSnapshotV1 tick_control{};
    test->Expect(
        failed.error ==
                canonical::CanonicalBundleErrorV1::kSinkPreflightFailed &&
            failed.segment_error ==
                canonical::CanonicalSegmentErrorV1::kCorruptControlPage &&
            harness.ReadFrontier().source_state ==
                canonical::SourceStateV1::kFatal &&
            harness.normalizer->Snapshot().fatal &&
            harness.tick_writer->ReadControl(&tick_control) ==
                canonical::CanonicalSegmentErrorV1::kNone &&
            tick_control.generation_fatal,
        "corrupt sink preflight anchors global revocation before "
        "best-effort segment fan-out");
    std::span<const std::byte> record;
    canonical::SourceFrontierV1 proof;
    test->Expect(
        canonical::ReadCommittedCanonicalRecordV1(
            harness.frontier, *harness.tick_reader, 0U,
            &record, &proof) ==
            canonical::CanonicalCommittedReadErrorV1::kNotCommitted,
        "global fatal anchor hides an intact family's old committed prefix "
        "despite another corrupt control page");
}

void TestNormalizerFatalConvergesAtEntryAndPrepare(TestContext* test) {
    {
        RuntimeHarness harness;
        test->Expect(harness.Initialize(8U, 8U),
                     "normalizer entry-fatal harness initializes");
        if (harness.coordinator != nullptr) {
            test->Expect(
                harness.normalizer->FailStop(nullptr),
                "test can latch normalizer-only fatal before Process entry");
            const canonical::CanonicalBundleResultV1 failed =
                harness.ProcessDirect(1U, 10, 1U);
            test->Expect(
                failed.error ==
                        canonical::CanonicalBundleErrorV1::kSourceFatal &&
                    harness.ReadFrontier().source_state ==
                        canonical::SourceStateV1::kFatal &&
                    harness.AllSegmentsFatal(),
                "Process entry publishes the global anchor for normalizer-only fatal");
        }
    }
    {
        RuntimeHarness harness;
        test->Expect(harness.Initialize(8U, 8U),
                     "market Prepare-fatal harness initializes");
        if (harness.coordinator != nullptr) {
            harness.verifier.fail_normalizer_during_market_verify = true;
            const canonical::CanonicalBundleResultV1 failed =
                harness.Process(1U, 10, 1U);
            test->Expect(
                failed.error == canonical::CanonicalBundleErrorV1::
                                    kNormalizePrepareFailed &&
                    failed.normalize.error == canonical::
                        CanonicalNormalizePrepareErrorV1::kNormalizerFatal &&
                    harness.ReadFrontier().source_state ==
                        canonical::SourceStateV1::kFatal &&
                    harness.AllSegmentsFatal(),
                "Prepare self-fatal converges source and all segment domains");
        }
    }
    {
        RuntimeHarness harness;
        test->Expect(harness.Initialize(8U, 8U),
                     "control Prepare-fatal harness initializes");
        if (harness.coordinator != nullptr) {
            harness.verifier.fail_normalizer_during_control_verify = true;
            const canonical::CanonicalBundleResultV1 failed =
                harness.ProcessControl(1U);
            test->Expect(
                failed.error == canonical::CanonicalBundleErrorV1::
                                    kNormalizePrepareFailed &&
                    failed.normalize.error == canonical::
                        CanonicalNormalizePrepareErrorV1::kNormalizerFatal &&
                    harness.ReadFrontier().source_state ==
                        canonical::SourceStateV1::kFatal &&
                    harness.AllSegmentsFatal(),
                "PrepareControl self-fatal converges every fatal domain");
        }
    }
}

void TestCompleteRouteManifestRequired(TestContext* test) {
    for (const ManifestMutation mutation : {
             ManifestMutation::kMissingSnapshot,
             ManifestMutation::kDuplicateTick,
             ManifestMutation::kIllegalQualityShard}) {
        RuntimeHarness harness;
        test->Expect(
            harness.Initialize(8U, 8U, mutation),
            "route-manifest rejection harness reaches coordinator Create");
        test->Expect(
            harness.coordinator_create_error ==
                    canonical::CanonicalBundleErrorV1::kInvalidConfiguration &&
                harness.coordinator == nullptr,
            "Create rejects missing, duplicate, or illegal family/shard routes");
    }
}

void TestCapacityPreflightIsAbortable(TestContext* test) {
    RuntimeHarness harness;
    test->Expect(harness.Initialize(1U, 8U),
                 "capacity harness initializes");
    if (harness.coordinator == nullptr) {
        return;
    }
    test->Expect(harness.Process(1U, 10, 1U).ok(),
                 "capacity harness baseline commits");
    const canonical::CanonicalBundleResultV1 blocked =
        harness.Process(2U, 12, 2U);
    const canonical::SourceFrontierV1 frontier = harness.ReadFrontier();
    test->Expect(
        blocked.error ==
                canonical::CanonicalBundleErrorV1::kSinkPreflightFailed &&
            blocked.segment_error ==
                canonical::CanonicalSegmentErrorV1::kSegmentFull &&
            blocked.published_records == 0U &&
            harness.tick_writer->header().published_records == 1U &&
            harness.quality_writer->header().published_records == 0U &&
            frontier.processed_ingress_sequence == 1U &&
            frontier.source_state == canonical::SourceStateV1::kHealthy &&
            !harness.normalizer->Snapshot().fatal,
        "full sink fails before any publish and leaves the old prefix unchanged for whole-generation replacement");
}

void TestEnvelopeVerifierIsMandatoryAtRuntime(TestContext* test) {
    RuntimeHarness harness;
    test->Expect(harness.Initialize(8U, 8U),
                 "envelope verifier harness initializes");
    if (harness.coordinator == nullptr) {
        return;
    }
    harness.verifier.reject = true;
    const canonical::CanonicalBundleResultV1 rejected =
        harness.Process(1U, 10, 1U);
    test->Expect(
        rejected.error ==
                canonical::CanonicalBundleErrorV1::kEnvelopeNotVerified &&
            harness.ReadFrontier().processed_ingress_sequence == 0U &&
            harness.tick_writer->header().published_records == 0U &&
            harness.quality_writer->header().published_records == 0U &&
            !harness.normalizer->Snapshot().transaction_active,
        "unverified cursor-to-content binding never reaches normalization or mmap");
}

void TestPartialPublishFailStopsGeneration(TestContext* test) {
    RuntimeHarness harness;
    test->Expect(harness.Initialize(8U, 8U),
                 "fault harness initializes");
    if (harness.coordinator == nullptr) {
        return;
    }
    test->Expect(harness.Process(1U, 10, 1U).ok(),
                 "fault harness baseline commits");
    harness.hook.fail_second_publish = true;
    const canonical::CanonicalBundleResultV1 failed =
        harness.Process(2U, 12, 2U);
    const canonical::SourceFrontierV1 frontier = harness.ReadFrontier();
    std::span<const std::byte> record;
    canonical::SourceFrontierV1 proof;
    canonical::CanonicalSegmentControlSnapshotV1 tick_control{};
    canonical::CanonicalSegmentControlSnapshotV1 snapshot_control{};
    canonical::CanonicalSegmentControlSnapshotV1 quality_control{};
    canonical::CanonicalSegmentControlSnapshotV1 control_control{};
    test->Expect(
        failed.error == canonical::CanonicalBundleErrorV1::kInjectedFailure &&
            failed.published_records == 1U &&
            frontier.source_state == canonical::SourceStateV1::kFatal &&
            frontier.processed_ingress_sequence == 1U &&
            harness.normalizer->Snapshot().fatal,
        "failure after first family publish fail-stops without global progress");
    test->Expect(
        harness.snapshot_writer->ReadControl(&snapshot_control) ==
                canonical::CanonicalSegmentErrorV1::kNone &&
            harness.tick_writer->ReadControl(&tick_control) ==
                canonical::CanonicalSegmentErrorV1::kNone &&
            harness.quality_writer->ReadControl(&quality_control) ==
                canonical::CanonicalSegmentErrorV1::kNone &&
            harness.control_writer->ReadControl(&control_control) ==
                canonical::CanonicalSegmentErrorV1::kNone &&
            snapshot_control.generation_fatal &&
            tick_control.generation_fatal &&
            quality_control.generation_fatal &&
            control_control.generation_fatal &&
            harness.AllSegmentsFatal(),
        "post-publication failure latches every configured segment fatal");
    canonical::CanonicalSegmentSealResultV1 seal_result{};
    test->Expect(
        harness.tick_writer->Seal(
            canonical::CanonicalSegmentSealOptionsV1{100, 100},
            &seal_result) ==
            canonical::CanonicalSegmentErrorV1::kGenerationFatal,
        "generation-fatal segment cannot be sealed into a valid manifest");
    test->Expect(
        canonical::PublishProcessedProgressV1(
            &harness.frontier,
            harness.frontier_config.writer_instance,
            harness.frontier_config.generation,
            2U,
            Wal(2U),
            20) == canonical::SourceFrontierErrorV1::kInvalidState &&
            harness.ReadFrontier().processed_ingress_sequence == 1U,
        "FATAL source cannot be advanced later to cover a partial bundle");
    test->Expect(
        canonical::ReadCommittedCanonicalRecordV1(
            harness.frontier, *harness.tick_reader, 1U,
            &record, &proof) ==
            canonical::CanonicalCommittedReadErrorV1::kNotCommitted,
        "partial physical tail remains hidden and requires generation replay");
    test->Expect(
        canonical::ReadCommittedCanonicalRecordV1(
            harness.frontier, *harness.tick_reader, 0U,
            &record, &proof) ==
            canonical::CanonicalCommittedReadErrorV1::kNotCommitted,
        "generation fail-stop invalidates the previously committed prefix too");
}

void TestFrontierCommitFailurePoisonsAdvancedSegments(TestContext* test) {
    RuntimeHarness harness;
    test->Expect(harness.Initialize(8U, 8U),
                 "frontier fault harness initializes");
    if (harness.coordinator == nullptr) {
        return;
    }
    test->Expect(harness.Process(1U, 10, 1U).ok(),
                 "frontier fault baseline commits");
    harness.hook.fail_frontier_commit = true;
    const canonical::CanonicalBundleResultV1 failed =
        harness.Process(2U, 11, 2U);
    canonical::CanonicalSegmentControlSnapshotV1 tick{};
    canonical::CanonicalSegmentControlSnapshotV1 snapshot{};
    canonical::CanonicalSegmentControlSnapshotV1 quality{};
    canonical::CanonicalSegmentControlSnapshotV1 control_segment{};
    test->Expect(
        failed.error == canonical::CanonicalBundleErrorV1::kInjectedFailure &&
            harness.ReadFrontier().processed_ingress_sequence == 1U &&
            harness.snapshot_writer->ReadControl(&snapshot) ==
                canonical::CanonicalSegmentErrorV1::kNone &&
            harness.tick_writer->ReadControl(&tick) ==
                canonical::CanonicalSegmentErrorV1::kNone &&
            harness.quality_writer->ReadControl(&quality) ==
                canonical::CanonicalSegmentErrorV1::kNone &&
            harness.control_writer->ReadControl(&control_segment) ==
                canonical::CanonicalSegmentErrorV1::kNone &&
            snapshot.generation_fatal && tick.generation_fatal &&
            quality.generation_fatal &&
            control_segment.generation_fatal &&
            harness.AllSegmentsFatal(),
        "failure after every local cursor advance poisons the whole generation");
}

void TestFrontierCommitRetriesTransientBusy(TestContext* test) {
    RuntimeHarness harness;
    test->Expect(harness.Initialize(8U, 8U),
                 "frontier BUSY retry harness initializes");
    if (harness.coordinator == nullptr) {
        return;
    }
    test->Expect(harness.Process(1U, 10, 1U).ok(),
                 "frontier BUSY retry baseline commits");
    harness.hook.pause_frontier_commit = true;
    std::thread releaser([&harness]() {
        while (!harness.hook.frontier_commit_locked.load(
            std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{20});
        StoreFrontierProgressGeneration(
            &harness.frontier,
            harness.hook.frontier_release_generation.load(
                std::memory_order_acquire));
    });
    const canonical::CanonicalBundleResultV1 committed =
        harness.Process(2U, 11, 2U);
    releaser.join();
    const canonical::SourceFrontierV1 frontier = harness.ReadFrontier();
    test->Expect(
        committed.ok() &&
            committed.frontier_error ==
                canonical::SourceFrontierErrorV1::kNone &&
            frontier.processed_ingress_sequence == 2U &&
            frontier.source_state == canonical::SourceStateV1::kHealthy &&
            !harness.normalizer->Snapshot().fatal &&
            !harness.AllSegmentsFatal(),
        "post-sink processed frontier safely retries transient BUSY without revoking the generation");
}

}  // namespace

int main() {
    TestContext test;
    TestCompleteRouteManifestRequired(&test);
    TestAtomicVisibility(&test);
    TestSingleDecodeAndProgressOnlyPaths(&test);
    TestSegmentFatalHidesCommittedPrefix(&test);
    TestCorruptPreflightStillPublishesGlobalAnchor(&test);
    TestNormalizerFatalConvergesAtEntryAndPrepare(&test);
    TestCapacityPreflightIsAbortable(&test);
    TestEnvelopeVerifierIsMandatoryAtRuntime(&test);
    TestPartialPublishFailStopsGeneration(&test);
    TestFrontierCommitFailurePoisonsAdvancedSegments(&test);
    TestFrontierCommitRetriesTransientBusy(&test);
    if (test.failures == 0) {
        std::cout << "phase5 canonical bundle runtime tests passed\n";
    }
    return test.failures == 0 ? 0 : 1;
}
