#include "l2flow/apps/production_service_v1.h"

#include "l2flow/canonical/canonical_schema_v1.h"
#include "l2flow/canonical/canonical_segment_v1.h"
#include "l2flow/canonical/source_frontier_v1.h"
#include "l2flow/control/control_decoder.h"
#include "l2flow/ingress/raw_live_tail.h"
#include "l2flow/ingress/raw_schema.h"
#include "l2flow/ingress/raw_v1.h"
#include "l2flow/route/production_route_v1.h"

#include "mdl_sys_msg.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <time.h>
#include <unistd.h>

namespace apps = l2flow::apps;
namespace canonical = l2flow::canonical;
namespace common = l2flow::common;
namespace control = l2flow::control;
namespace ingress = l2flow::ingress;
namespace market = l2flow::market;
namespace route = l2flow::route;
namespace runtime = l2flow::runtime;
namespace sdk = l2flow::sdk;
namespace sys = datayes::mdl::mdl_sys_msg;

namespace {

using namespace std::chrono_literals;

struct TestContext final {
    void Expect(bool condition, std::string_view message) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << message << '\n';
        }
    }
    int failures = 0;
};

template <std::size_t Size>
std::array<std::byte, Size> Pattern(std::uint8_t seed) {
    std::array<std::byte, Size> result{};
    for (std::size_t index = 0U; index < result.size(); ++index) {
        result[index] = static_cast<std::byte>(seed + index);
    }
    return result;
}

std::vector<std::byte> Bytes(std::string_view text) {
    const std::span<const char> chars(text.data(), text.size());
    const auto bytes = std::as_bytes(chars);
    return {bytes.begin(), bytes.end()};
}

void StoreU16(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint16_t value) {
    bytes[offset] = static_cast<std::byte>(value & 0xffU);
    bytes[offset + 1U] =
        static_cast<std::byte>((value >> 8U) & 0xffU);
}

void StoreU32(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint32_t value) {
    for (std::size_t index = 0U; index < 4U; ++index) {
        bytes[offset + index] = static_cast<std::byte>(
            (value >> static_cast<unsigned int>(index * 8U)) & 0xffU);
    }
}

void StoreU64(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint64_t value) {
    for (std::size_t index = 0U; index < 8U; ++index) {
        bytes[offset + index] = static_cast<std::byte>(
            (value >> static_cast<unsigned int>(index * 8U)) & 0xffU);
    }
}

class WireWriter final {
public:
    explicit WireWriter(std::size_t fixed_bytes)
        : bytes_(fixed_bytes, std::byte{0U}) {}

    void U16(std::size_t offset, std::uint16_t value) {
        StoreU16(bytes_, offset, value);
    }
    void U32(std::size_t offset, std::uint32_t value) {
        StoreU32(bytes_, offset, value);
    }
    void U64(std::size_t offset, std::uint64_t value) {
        StoreU64(bytes_, offset, value);
    }
    void I32(std::size_t offset, std::int32_t value) {
        U32(offset, static_cast<std::uint32_t>(value));
    }
    void I64(std::size_t offset, std::int64_t value) {
        U64(offset, static_cast<std::uint64_t>(value));
    }
    void Text(std::size_t descriptor, std::string_view text) {
        const std::size_t start = bytes_.size();
        const auto text_bytes = std::as_bytes(
            std::span<const char>(text.data(), text.size()));
        bytes_.insert(bytes_.end(), text_bytes.begin(), text_bytes.end());
        U16(descriptor, static_cast<std::uint16_t>(text.size()));
        U32(
            descriptor + 2U,
            static_cast<std::uint32_t>(start - descriptor));
    }
    [[nodiscard]] std::vector<std::byte> Take() && {
        return std::move(bytes_);
    }

private:
    std::vector<std::byte> bytes_;
};

struct RawMessage final {
    sdk::MessageKey key{};
    std::vector<std::byte> body;
};

std::vector<std::byte> LogonBody(const sdk::IngressSpec& spec) {
    constexpr std::size_t kFixedBytes = 24U;
    constexpr std::size_t kServicesDescriptor = 12U;
    constexpr std::size_t kServiceOffset = kFixedBytes;
    constexpr std::size_t kMessagesDescriptor = kServiceOffset + 8U;
    constexpr std::size_t kMessagesOffset = kServiceOffset + 16U;
    std::vector<std::byte> body(
        kMessagesOffset + spec.required.size() * 8U,
        std::byte{0U});
    StoreU32(body, kServicesDescriptor, 1U);
    StoreU32(
        body,
        kServicesDescriptor + 4U,
        static_cast<std::uint32_t>(
            kServiceOffset - kServicesDescriptor));
    StoreU32(body, 20U, 0U);
    StoreU32(body, kServiceOffset, spec.market_service_id);
    StoreU32(body, kServiceOffset + 4U, 101U);
    StoreU32(
        body,
        kMessagesDescriptor,
        static_cast<std::uint32_t>(spec.required.size()));
    StoreU32(body, kMessagesDescriptor + 4U, 8U);
    for (std::size_t index = 0U; index < spec.required.size(); ++index) {
        StoreU32(
            body,
            kMessagesOffset + index * 8U,
            spec.required[index].message_id);
        StoreU32(body, kMessagesOffset + index * 8U + 4U, 0U);
    }
    return body;
}

std::vector<std::byte> ShenzhenOrderBody() {
    WireWriter writer(58U);
    writer.U32(0U, 12U);
    writer.I64(4U, 9001);
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

std::vector<std::byte> ShenzhenTransactionBody() {
    WireWriter writer(70U);
    writer.U32(0U, 12U);
    writer.I64(4U, 9002);
    writer.I64(18U, 8101);
    writer.I64(26U, 8202);
    writer.I64(46U, 123456);
    writer.I64(54U, 50);
    writer.I32(62U, 70);
    writer.U32(66U, 93000124U);
    writer.Text(12U, "010");
    writer.Text(34U, "000001");
    writer.Text(40U, "102");
    return std::move(writer).Take();
}

std::vector<RawMessage> ReadinessMessages(const sdk::IngressSpec& spec) {
    std::vector<RawMessage> result;
    result.reserve(spec.required.size() + 1U);
    result.push_back(RawMessage{
        sdk::MessageKey{
            2U,
            101U,
            static_cast<std::uint16_t>(sys::LogonResponse::MessageID)},
        LogonBody(spec)});
    for (const sdk::MessageKey& key : spec.required) {
        std::vector<std::byte> body;
        if (key == sdk::MessageKey{6U, 101U, 33U}) {
            body = ShenzhenOrderBody();
        } else if (key == sdk::MessageKey{6U, 101U, 36U}) {
            body = ShenzhenTransactionBody();
        } else {
            const std::optional<std::size_t> fixed =
                sdk::RequiredMessageFixedBodyBytes(key);
            if (!fixed.has_value()) {
                return {};
            }
            body.assign(*fixed, std::byte{0U});
        }
        result.push_back(RawMessage{key, std::move(body)});
    }
    return result;
}

class TemporaryDirectory final {
public:
    TemporaryDirectory() {
        std::array<char, 64U> pattern{};
        const char* const value = "/tmp/l2flow-production-service-XXXXXX";
        std::memcpy(pattern.data(), value, std::strlen(value) + 1U);
        char* const created = ::mkdtemp(pattern.data());
        if (created != nullptr) {
            path_ = created;
            descriptor_ = ::open(
                path_.c_str(),
                O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
        }
    }
    ~TemporaryDirectory() {
        if (descriptor_ >= 0) {
            static_cast<void>(::close(descriptor_));
        }
        if (!path_.empty()) {
            std::error_code ignored;
            std::filesystem::remove_all(path_, ignored);
        }
    }
    [[nodiscard]] bool ok() const noexcept { return descriptor_ >= 0; }
    [[nodiscard]] int descriptor() const noexcept { return descriptor_; }
    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
    int descriptor_ = -1;
};

[[nodiscard]] std::uint64_t MonotonicNowForTest() noexcept {
    struct timespec value {};
    if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0 ||
        value.tv_sec < 0 || value.tv_nsec < 0 ||
        value.tv_nsec >= 1'000'000'000L) {
        return 1U;
    }
    return static_cast<std::uint64_t>(value.tv_sec) * 1'000'000'000ULL +
        static_cast<std::uint64_t>(value.tv_nsec);
}

struct SourceRecordProgress final {
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t record_end_wal_pos = 0U;
    std::int64_t recv_monotonic_ns = 0;
};

class MemoryRawSource final : public ingress::RawLiveTailSource {
public:
    [[nodiscard]] bool Initialize(
        const sdk::IngressSpec& spec,
        std::size_t source_slot,
        std::uint32_t capture_date,
        const common::Sha256Digest& stable_config,
        const common::Identity128& stream_day_id,
        const common::Identity128& writer_instance,
        const canonical::ClockEpochIdentityV1& clock) {
        header_.source_stream_id = spec.source_stream_id;
        header_.capture_date = capture_date;
        header_.stream_day_id = stream_day_id;
        header_.segment_sequence = 1U;
        header_.segment_base_wal_pos = 0U;
        header_.first_ingress_sequence = 1U;
        header_.created_realtime_ns = 10U;
        header_.created_monotonic_ns = 20U;
        header_.host_uuid = Pattern<16U>(
            static_cast<std::uint8_t>(0x81U + source_slot));
        header_.linux_boot_id = Pattern<16U>(
            static_cast<std::uint8_t>(0x91U + source_slot));
        header_.clock_epoch_algorithm = clock.algorithm;
        header_.clock_epoch_digest = clock.digest;
        header_.clock_epoch_label = clock.label;
        header_.sdk_archive_sha256 = Pattern<32U>(0xa1U);
        header_.libmdl_api_sha256 = Pattern<32U>(0xb1U);
        header_.endpoint_contract_sha256 = Pattern<32U>(0xc1U);
        header_.config_sha256 = stable_config;
        header_.raw_schema_sha256 = ingress::RawSchemaSha256Digest();
        header_.build_manifest_sha256 = Pattern<32U>(0xd1U);

        ingress::RawV1SegmentHeaderWire wire{};
        if (ingress::EncodeSegmentHeaderV1(header_, &wire) !=
            ingress::RawV1Error::kNone) {
            return false;
        }
        bytes_.assign(wire.begin(), wire.end());

        control_.writer_instance = writer_instance;
        control_.stream_day_id = stream_day_id;
        control_.source_stream_id = spec.source_stream_id;
        control_.capture_date = capture_date;
        control_.segment_sequence = 1U;
        control_.append_segment_offset = bytes_.size();
        control_.append_global_wal_pos = bytes_.size();
        control_.durable_segment_offset = bytes_.size();
        control_.durable_global_wal_pos = bytes_.size();
        control_.heartbeat_monotonic_ns = MonotonicNowForTest();
        generation_ = 2U;
        return true;
    }

    [[nodiscard]] SourceRecordProgress Append(const RawMessage& message) {
        std::lock_guard<std::mutex> lock(mutex_);
        const std::uint64_t sequence =
            control_.append_ingress_sequence + 1U;
        const std::int64_t monotonic =
            static_cast<std::int64_t>(2'000'000U + sequence);
        ingress::RawRecordInputV1 input{};
        input.meta.source_stream_id = header_.source_stream_id;
        input.meta.capture_date = header_.capture_date;
        input.meta.ingress_sequence = sequence;
        input.meta.recv_realtime_ns = 1'000'000U + sequence;
        input.meta.recv_monotonic_ns =
            static_cast<std::uint64_t>(monotonic);
        input.vendor_head.fill(std::byte{0U});
        input.vendor_head[0U] =
            static_cast<std::byte>(ingress::kVendorMessageHeadBytes);
        StoreU32(
            input.vendor_head,
            1U,
            static_cast<std::uint32_t>(
                ingress::kVendorMessageHeadBytes + message.body.size()));
        input.vendor_head[5U] = std::byte{1U};
        input.vendor_head[6U] =
            static_cast<std::byte>(message.key.service_id);
        StoreU16(input.vendor_head, 7U, message.key.service_version);
        StoreU16(input.vendor_head, 9U, message.key.message_id);
        StoreU32(input.vendor_head, 11U, 93000123U);
        StoreU64(input.vendor_head, 15U, 80'000U + sequence);
        input.vendor_body = message.body;

        std::vector<std::byte> record_wire;
        if (ingress::EncodeRawRecordV1(input, &record_wire) !=
            ingress::RawV1Error::kNone) {
            return {};
        }
        bytes_.insert(
            bytes_.end(), record_wire.begin(), record_wire.end());
        control_.append_segment_offset = bytes_.size();
        control_.append_global_wal_pos = bytes_.size();
        control_.append_ingress_sequence = sequence;
        control_.durable_segment_offset = bytes_.size();
        control_.durable_global_wal_pos = bytes_.size();
        control_.durable_ingress_sequence = sequence;
        control_.heartbeat_monotonic_ns = MonotonicNowForTest();
        generation_ += 2U;
        return SourceRecordProgress{
            sequence,
            static_cast<std::uint64_t>(bytes_.size()),
            monotonic};
    }

    [[nodiscard]] std::uint64_t NextIngressSequence() const noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        return control_.append_ingress_sequence + 1U;
    }

    [[nodiscard]] const ingress::SegmentHeaderV1& header() const noexcept {
        return header_;
    }

    void Seal() noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        sealed_ = true;
        generation_ += 2U;
    }

    [[nodiscard]] int ReadControl(
        ingress::RawControlSnapshot* output,
        std::uint64_t* generation) noexcept override {
        std::lock_guard<std::mutex> lock(mutex_);
        const std::uint64_t heartbeat = MonotonicNowForTest();
        if (heartbeat > control_.heartbeat_monotonic_ns) {
            control_.heartbeat_monotonic_ns = heartbeat;
            generation_ += 2U;
        }
        *output = control_;
        *generation = generation_;
        return 0;
    }

    [[nodiscard]] int InspectSegment(
        std::uint32_t sequence,
        ingress::RawLiveSegmentInfo* output) noexcept override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (sequence != header_.segment_sequence) {
            return ENOENT;
        }
        output->header = header_;
        output->visible_end_offset = bytes_.size();
        output->sealed = sealed_;
        return 0;
    }

    [[nodiscard]] ingress::RawLiveReadResult ReadSegmentSome(
        std::uint32_t sequence,
        std::uint64_t offset,
        std::span<std::byte> output) noexcept override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (sequence != header_.segment_sequence ||
            offset > static_cast<std::uint64_t>(bytes_.size())) {
            return {0U, ENOENT};
        }
        const std::size_t begin = static_cast<std::size_t>(offset);
        const std::size_t count =
            std::min(output.size(), bytes_.size() - begin);
        std::copy_n(
            bytes_.begin() + static_cast<std::ptrdiff_t>(begin),
            count,
            output.begin());
        return {count, 0};
    }

private:
    mutable std::mutex mutex_;
    ingress::SegmentHeaderV1 header_{};
    std::vector<std::byte> bytes_;
    ingress::RawControlSnapshot control_{};
    std::uint64_t generation_ = 0U;
    bool sealed_ = false;
};

struct CaptureCounters final {
    std::atomic<std::uint32_t> starts{0U};
    std::atomic<std::uint32_t> stops{0U};
};

class DummyCapture final : public apps::ProductionCaptureRuntimeV1 {
public:
    explicit DummyCapture(
        std::shared_ptr<CaptureCounters> counters,
        apps::ProductionCaptureBindingV1 binding = {})
        : counters_(std::move(counters)), binding_(binding) {}
    [[nodiscard]] const apps::ProductionCaptureBindingV1& binding()
        const noexcept override {
        return binding_;
    }
    bool Start(std::string*) noexcept override {
        counters_->starts.fetch_add(1U, std::memory_order_relaxed);
        return true;
    }
    bool Stop(std::string*) noexcept override {
        counters_->stops.fetch_add(1U, std::memory_order_relaxed);
        return true;
    }

private:
    std::shared_ptr<CaptureCounters> counters_;
    const apps::ProductionCaptureBindingV1 binding_{};
};

class DummyLifetime final : public apps::ProductionSourceLifetimeV1 {};

class MemorySourceLifetime final :
    public apps::ProductionSourceLifetimeV1 {
public:
    std::unique_ptr<MemoryRawSource> raw_source;
    std::unique_ptr<canonical::SourceFrontierPageV1> frontier;
};

class SealingCapture final : public apps::ProductionCaptureRuntimeV1 {
public:
    SealingCapture(
        std::shared_ptr<CaptureCounters> counters,
        apps::ProductionCaptureBindingV1 binding,
        MemoryRawSource* raw_source) noexcept
        : counters_(std::move(counters)),
          binding_(binding),
          raw_source_(raw_source) {}

    [[nodiscard]] const apps::ProductionCaptureBindingV1& binding()
        const noexcept override {
        return binding_;
    }
    bool Start(std::string*) noexcept override {
        counters_->starts.fetch_add(1U, std::memory_order_relaxed);
        return true;
    }
    bool Stop(std::string*) noexcept override {
        counters_->stops.fetch_add(1U, std::memory_order_relaxed);
        if (raw_source_ != nullptr) {
            raw_source_->Seal();
        }
        return true;
    }

private:
    std::shared_ptr<CaptureCounters> counters_;
    const apps::ProductionCaptureBindingV1 binding_{};
    MemoryRawSource* const raw_source_ = nullptr;
};

std::unique_ptr<market::InstrumentRegistryV1> MakeRegistry(
    std::uint64_t version) {
    market::InstrumentRegistryEntryV1 entry{};
    entry.instrument_id = 1U;
    entry.key.market = market::MarketV1::kShanghai;
    entry.key.security_id = Bytes("600000");
    entry.quantity_unit = market::QuantityUnitV1::kShare;
    entry.security_type = market::SecurityTypeV1::kEquity;
    entry.asset_scope = market::AssetScopeV1::kDocumentedCore;
    std::unique_ptr<market::InstrumentRegistryV1> result;
    if (market::InstrumentRegistryV1::Create(
            version, std::span<const market::InstrumentRegistryEntryV1>(
                         &entry, 1U),
            &result) != market::InstrumentRegistryCreateErrorV1::kNone) {
        return nullptr;
    }
    return result;
}

market::InstrumentHistoryRuntimeConfigV1 MakeHistoryConfig(
    bool exact_source_set = true) {
    market::InstrumentHistoryRuntimeConfigV1 config{};
    config.source_stream_ids = route::kProductionRouteSourceStreamIdsV1;
    if (!exact_source_set) {
        config.source_stream_ids[3] = 2999U;
    }
    config.physical_worker_count = 2U;
    config.queue_capacity = 8U;
    config.maximum_inflight_per_source = 64U;
    config.chunk_record_capacity = 8U;
    config.maximum_records_per_logical_shard = 1024U;
    config.maximum_instruments_per_logical_shard = 64U;
    config.maximum_owned_payload_bytes_per_logical_shard = 1024U * 1024U;
    return config;
}

std::unique_ptr<market::InstrumentHistoryRuntimeV1> MakeHistory(
    bool exact_source_set) {
    const auto config = MakeHistoryConfig(exact_source_set);
    std::unique_ptr<market::InstrumentHistoryRuntimeV1> result;
    if (market::InstrumentHistoryRuntimeV1::Create(config, &result) !=
        market::InstrumentHistoryCreateErrorV1::kNone) {
        return nullptr;
    }
    return result;
}

route::ProductionRouteManifestV1 MakeManifest(
    const market::InstrumentRegistryV1& registry,
    const std::filesystem::path& root) {
    route::ProductionRouteManifestV1 manifest{};
    manifest.state = route::ProductionRouteStateV1::kActive;
    manifest.generation = 1U;
    manifest.route_instance = Pattern<16U>(0x10U);
    manifest.trade_date = 20260723U;
    manifest.registry_version = registry.registry_version();
    manifest.registry_sha256 = registry.registry_sha256();
    manifest.schema_sha256 = canonical::CanonicalSchemaDescriptorSha256V1();
    manifest.build_sha256 = Pattern<32U>(0x20U);
    manifest.config_sha256 = Pattern<32U>(0x30U);
    for (std::size_t index = 0U; index < manifest.sources.size(); ++index) {
        auto& source = manifest.sources[index];
        source.source_stream_id = route::kProductionRouteSourceStreamIdsV1[index];
        source.capture_date = 20260723U;
        source.stream_day_id = Pattern<16U>(
            static_cast<std::uint8_t>(0x40U + index));
        source.writer_instance = Pattern<16U>(
            static_cast<std::uint8_t>(0x50U + index));
        source.source_generation = 1U + index;
        source.canonical_generation = 11U + index;
        canonical::ClockEpochIdentityV1 clock{};
        clock.algorithm = 1U;
        clock.digest = Pattern<32U>(
            static_cast<std::uint8_t>(0x60U + index));
        clock.label = 1U;
        source.clock_epoch_identity_sha256 =
            route::ComputeProductionRouteClockEpochIdentitySha256V1(clock);
    }
    manifest.endpoints.canonical = (root / "canonical").string();
    manifest.endpoints.history = (root / "history-in-process-only").string();
    manifest.endpoints.state = (root / "state").string();
    manifest.endpoints.factor = (root / "factor").string();
    return manifest;
}

struct Prepared final {
    apps::ProductionServiceInputsV1 inputs{};
    std::shared_ptr<CaptureCounters> counters =
        std::make_shared<CaptureCounters>();
};

Prepared MakePrepared(
    TestContext* test,
    TemporaryDirectory* directory,
    bool exact_history = true,
    bool matching_registry = true) {
    Prepared result{};
    result.inputs.registry = MakeRegistry(7U);
    result.inputs.history = MakeHistory(exact_history);
    test->Expect(
        result.inputs.registry != nullptr && result.inputs.history != nullptr,
        "typed registry/history fixtures create");
    if (result.inputs.registry == nullptr || result.inputs.history == nullptr) {
        return result;
    }
    auto manifest = MakeManifest(*result.inputs.registry, directory->path());
    if (!matching_registry) {
        manifest.registry_sha256[0] ^= std::byte{0x01U};
    }
    test->Expect(
        route::ValidateProductionRouteManifestV1(manifest) ==
            route::ProductionRouteManifestErrorV1::kNone,
        "preflight route fixture is independently valid");
    result.inputs.route_controller =
        std::make_unique<route::ProductionRouteControllerV1>(
            directory->descriptor(), std::move(manifest));
    for (std::size_t index = 0U; index < result.inputs.captures.size(); ++index) {
        result.inputs.source_lifetimes[index] =
            std::make_unique<DummyLifetime>();
        result.inputs.captures[index] =
            std::make_unique<DummyCapture>(result.counters);
    }
    return result;
}

[[nodiscard]] bool AppendReadiness(
    MemoryRawSource* raw_source,
    canonical::SourceFrontierPageV1* frontier,
    const common::Identity128& writer_instance,
    std::uint64_t source_generation,
    const sdk::IngressSpec& spec) {
    const std::vector<RawMessage> messages = ReadinessMessages(spec);
    if (messages.size() != spec.required.size() + 1U) {
        return false;
    }
    for (const RawMessage& message : messages) {
        const std::uint64_t sequence =
            raw_source->NextIngressSequence();
        canonical::SourceFrontierCallbackGuardV1 callback(
            frontier, writer_instance, source_generation);
        if (callback.error() != canonical::SourceFrontierErrorV1::kNone ||
            callback.CompleteCaptured(sequence) !=
                canonical::SourceFrontierErrorV1::kNone) {
            return false;
        }
        const SourceRecordProgress progress = raw_source->Append(message);
        if (progress.ingress_sequence != sequence ||
            progress.record_end_wal_pos == 0U ||
            canonical::PublishAppendProgressV1(
                frontier,
                writer_instance,
                source_generation,
                progress.ingress_sequence,
                progress.record_end_wal_pos,
                progress.recv_monotonic_ns) !=
                canonical::SourceFrontierErrorV1::kNone) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] bool AddCanonicalSink(
    const TemporaryDirectory& directory,
    std::size_t source_slot,
    canonical::CanonicalFamilyV1 family,
    canonical::CanonicalEventTypeV1 event_type,
    std::uint32_t record_size,
    std::uint64_t segment_sequence,
    const route::ProductionRouteSourceV1& route_source,
    const runtime::ProductionSourcePipelineConfigV1& pipeline_config,
    const canonical::ClockEpochIdentityV1& clock,
    const market::InstrumentRegistryV1& registry,
    std::vector<runtime::ProductionCanonicalSinkV1>* sinks) {
    canonical::CanonicalSegmentDescriptorV1 descriptor{};
    descriptor.event_type = event_type;
    descriptor.record_size = record_size;
    descriptor.source_stream_id = route_source.source_stream_id;
    descriptor.shard = 0U;
    descriptor.trade_date = pipeline_config.trade_date;
    descriptor.origin_capture_date = route_source.capture_date;
    descriptor.origin_stream_day_id = route_source.stream_day_id;
    descriptor.origin_source_writer_instance =
        route_source.writer_instance;
    descriptor.origin_source_generation =
        route_source.source_generation;
    descriptor.clock_epoch = clock;
    descriptor.schema_sha256 =
        canonical::CanonicalSchemaDescriptorSha256V1();
    descriptor.dtype_sha256 =
        canonical::CanonicalDtypeDescriptorSha256V1();
    descriptor.registry_version = registry.registry_version();
    descriptor.registry_sha256 = registry.registry_sha256();
    descriptor.normalizer_build_sha256 =
        pipeline_config.normalizer_build_sha256;
    descriptor.normalizer_config_sha256 =
        pipeline_config.normalizer_config_sha256;
    descriptor.generation = route_source.canonical_generation;
    descriptor.segment_sequence = segment_sequence;
    descriptor.capacity_records = 32U;

    const std::string stem = "service-source-" +
        std::to_string(source_slot) + "-family-" +
        std::to_string(segment_sequence);
    canonical::CanonicalSegmentCreateOptionsV1 options{};
    options.segment_path = directory.path() / (stem + ".clog");
    options.manifest_path = directory.path() / (stem + ".manifest");
    options.descriptor = descriptor;
    options.created_realtime_ns = 1;
    options.created_monotonic_ns = 1;
    std::unique_ptr<canonical::CanonicalSegmentWriterV1> writer;
    if (canonical::CanonicalSegmentWriterV1::Create(
            options, &writer) !=
            canonical::CanonicalSegmentErrorV1::kNone ||
        writer == nullptr ||
        writer->AdvanceProcessedRaw(
            0U, ingress::kRawV1SegmentHeaderBytes) !=
            canonical::CanonicalSegmentErrorV1::kNone) {
        return false;
    }
    sinks->push_back(runtime::ProductionCanonicalSinkV1{
        family, 0U, std::move(writer)});
    return true;
}

[[nodiscard]] bool BuildStartableSource(
    TestContext* test,
    TemporaryDirectory* directory,
    std::size_t source_slot,
    const route::ProductionRouteSourceV1& route_source,
    std::uint32_t trade_date,
    market::InstrumentRegistryV1* registry,
    market::InstrumentHistoryRuntimeV1* history,
    const std::shared_ptr<CaptureCounters>& counters,
    apps::ProductionServiceInputsV1* inputs) {
    constexpr std::array<sdk::IngressKind, 4U> kIngressKinds{{
        sdk::IngressKind::ShSnapshot,
        sdk::IngressKind::ShTick,
        sdk::IngressKind::SzSnapshot,
        sdk::IngressKind::SzTick,
    }};
    const sdk::IngressKind ingress_kind = kIngressKinds[source_slot];
    const sdk::IngressSpec& spec = sdk::GetIngressSpec(ingress_kind);
    const common::Sha256Digest stable_config = Pattern<32U>(
        static_cast<std::uint8_t>(0xe1U + source_slot));
    canonical::ClockEpochIdentityV1 clock{};
    clock.algorithm = 1U;
    clock.digest = Pattern<32U>(
        static_cast<std::uint8_t>(0x60U + source_slot));
    clock.label = 1U;

    auto lifetime = std::make_unique<MemorySourceLifetime>();
    lifetime->raw_source = std::make_unique<MemoryRawSource>();
    lifetime->frontier =
        std::make_unique<canonical::SourceFrontierPageV1>();
    if (!lifetime->raw_source->Initialize(
            spec,
            source_slot,
            route_source.capture_date,
            stable_config,
            route_source.stream_day_id,
            route_source.writer_instance,
            clock)) {
        test->Expect(false, "startable Raw source initializes");
        return false;
    }

    runtime::ProductionSourcePipelineConfigV1 pipeline_config{};
    pipeline_config.source_slot =
        static_cast<std::uint8_t>(source_slot);
    pipeline_config.ingress_kind = ingress_kind;
    pipeline_config.trade_date = trade_date;
    pipeline_config.source_generation =
        route_source.source_generation;
    pipeline_config.canonical_generation =
        route_source.canonical_generation;
    pipeline_config.normalizer_build_sha256 = Pattern<32U>(
        static_cast<std::uint8_t>(0xf1U + source_slot));
    pipeline_config.normalizer_config_sha256 = Pattern<32U>(
        static_cast<std::uint8_t>(0x11U + source_slot));

    canonical::SourceFrontierConfigV1 frontier_config{};
    frontier_config.source_stream_id = spec.source_stream_id;
    frontier_config.capture_date = route_source.capture_date;
    frontier_config.stream_day_id = route_source.stream_day_id;
    frontier_config.clock_epoch = clock;
    frontier_config.writer_instance = route_source.writer_instance;
    frontier_config.generation = route_source.source_generation;
    frontier_config.initial_global_wal_pos =
        ingress::kRawV1SegmentHeaderBytes;
    frontier_config.initial_state = canonical::SourceStateV1::kHealthy;
    if (canonical::InitializeSourceFrontierPageV1(
            frontier_config, lifetime->frontier.get()) !=
            canonical::SourceFrontierErrorV1::kNone ||
        !AppendReadiness(
            lifetime->raw_source.get(),
            lifetime->frontier.get(),
            route_source.writer_instance,
            route_source.source_generation,
            spec)) {
        test->Expect(false, "startable source frontier/readiness initializes");
        return false;
    }

    ingress::RawLiveTailAttachV1 attach{};
    attach.writer_instance = route_source.writer_instance;
    attach.stream_day_id = route_source.stream_day_id;
    attach.source_stream_id = spec.source_stream_id;
    attach.capture_date = route_source.capture_date;
    attach.segment_sequence = 1U;
    attach.global_wal_pos = ingress::kRawV1SegmentHeaderBytes;
    attach.segment_offset = ingress::kRawV1SegmentHeaderBytes;
    attach.next_ingress_sequence = 1U;
    std::unique_ptr<ingress::RawLiveTail> tail;
    if (ingress::RawLiveTail::Attach(
            lifetime->raw_source.get(), attach, &tail) !=
            ingress::RawLiveTailError::kNone) {
        test->Expect(false, "startable Raw tail attaches");
        return false;
    }

    control::ControlDecoderConfigV1 control_config{};
    control_config.source_stream_id = spec.source_stream_id;
    control_config.capture_date = route_source.capture_date;
    control_config.stream_day_id = route_source.stream_day_id;
    control_config.stable_config_sha256 = stable_config;
    control_config.required = spec.required;
    control_config.optional = spec.optional;
    std::unique_ptr<control::ControlDecoderV1> control_decoder;
    if (control::ControlDecoderV1::Create(
            control_config, &control_decoder) !=
            control::ControlDecoderCreateErrorV1::kNone) {
        test->Expect(false, "startable control decoder creates");
        return false;
    }

    canonical::CanonicalNormalizerConfigV1 normalizer_config{};
    normalizer_config.capture_date = route_source.capture_date;
    normalizer_config.trade_date = trade_date;
    normalizer_config.source_stream_id = spec.source_stream_id;
    normalizer_config.stream_day_id = route_source.stream_day_id;
    normalizer_config.shard_count = 1U;
    normalizer_config.instrument_registry = registry;
    normalizer_config.sequence_policy.policy_version = 1U;
    std::unique_ptr<canonical::CanonicalNormalizerV1> normalizer;
    if (canonical::CanonicalNormalizerV1::Create(
            normalizer_config, &normalizer) !=
            canonical::CanonicalNormalizerCreateErrorV1::kNone) {
        test->Expect(false, "startable normalizer creates");
        return false;
    }

    std::vector<runtime::ProductionCanonicalSinkV1> sinks;
    if (!AddCanonicalSink(
            *directory,
            source_slot,
            canonical::CanonicalFamilyV1::kSnapshot,
            canonical::CanonicalEventTypeV1::kSnapshot,
            static_cast<std::uint32_t>(
                canonical::kCanonicalSnapshotRecordBytesV1),
            1U,
            route_source,
            pipeline_config,
            clock,
            *registry,
            &sinks) ||
        !AddCanonicalSink(
            *directory,
            source_slot,
            canonical::CanonicalFamilyV1::kTick,
            canonical::CanonicalEventTypeV1::kTick,
            static_cast<std::uint32_t>(
                canonical::kCanonicalTickRecordBytesV1),
            2U,
            route_source,
            pipeline_config,
            clock,
            *registry,
            &sinks) ||
        !AddCanonicalSink(
            *directory,
            source_slot,
            canonical::CanonicalFamilyV1::kQuality,
            canonical::CanonicalEventTypeV1::kQuality,
            static_cast<std::uint32_t>(
                canonical::kCanonicalQualityRecordBytesV1),
            3U,
            route_source,
            pipeline_config,
            clock,
            *registry,
            &sinks) ||
        !AddCanonicalSink(
            *directory,
            source_slot,
            canonical::CanonicalFamilyV1::kControl,
            canonical::CanonicalEventTypeV1::kControl,
            static_cast<std::uint32_t>(
                canonical::kCanonicalControlRecordBytesV1),
            4U,
            route_source,
            pipeline_config,
            clock,
            *registry,
            &sinks)) {
        test->Expect(false, "startable Canonical sinks create");
        return false;
    }

    std::unique_ptr<runtime::ProductionSourcePipelineV1> pipeline;
    const auto pipeline_error =
        runtime::ProductionSourcePipelineV1::Create(
            pipeline_config,
            std::move(tail),
            std::move(control_decoder),
            std::move(normalizer),
            lifetime->frontier.get(),
            std::move(sinks),
            history,
            &pipeline);
    if (pipeline_error != runtime::ProductionSourceCreateErrorV1::kNone ||
        pipeline == nullptr) {
        test->Expect(false, "startable production pipeline creates");
        return false;
    }

    apps::ProductionCaptureBindingV1 binding{};
    binding.source_slot = static_cast<std::uint8_t>(source_slot);
    binding.ingress_kind = ingress_kind;
    binding.source_stream_id = route_source.source_stream_id;
    binding.capture_date = route_source.capture_date;
    binding.stream_day_id = route_source.stream_day_id;
    binding.writer_instance = route_source.writer_instance;
    binding.source_generation = route_source.source_generation;
    binding.source_frontier = lifetime->frontier.get();
    binding.fast_capture_context = inputs->fast_plane.get();
    inputs->captures[source_slot] =
        std::make_unique<SealingCapture>(
            counters, binding, lifetime->raw_source.get());
    inputs->pipelines[source_slot] = std::move(pipeline);
    inputs->source_lifetimes[source_slot] = std::move(lifetime);
    return true;
}

Prepared MakeStartablePrepared(
    TestContext* test,
    TemporaryDirectory* directory,
    bool matching_fast_stream_days = true,
    bool matching_fast_capture_dates = true) {
    Prepared result{};
    result.inputs.registry = MakeRegistry(7U);
    result.inputs.history = MakeHistory(true);
    test->Expect(
        result.inputs.registry != nullptr &&
            result.inputs.history != nullptr,
        "startable registry/history fixtures create");
    if (result.inputs.registry == nullptr ||
        result.inputs.history == nullptr) {
        return result;
    }
    route::ProductionRouteManifestV1 manifest =
        MakeManifest(*result.inputs.registry, directory->path());
    if (!matching_fast_capture_dates) {
        ++manifest.sources.back().capture_date;
    }

    runtime::RealtimeFastPlaneConfigV1 fast_config{};
    fast_config.capture_date = manifest.sources.front().capture_date;
    fast_config.trade_date = manifest.trade_date;
    fast_config.max_message_bytes = 4096U;
    fast_config.input_ring_capacity_bytes_per_source = 64U * 1024U;
    fast_config.history = MakeHistoryConfig();
    for (std::size_t index = 0U; index < manifest.sources.size(); ++index) {
        fast_config.stream_day_ids[index] =
            manifest.sources[index].stream_day_id;
    }
    if (!matching_fast_stream_days) {
        fast_config.stream_day_ids.back()[0U] ^= std::byte{0x01U};
    }
    const auto fast_error = runtime::RealtimeFastPlaneRuntimeV1::Create(
        fast_config,
        result.inputs.registry.get(),
        &result.inputs.fast_plane);
    test->Expect(
        fast_error == runtime::RealtimeFastPlaneCreateErrorV1::kNone &&
            result.inputs.fast_plane != nullptr,
        "shadow Fast Plane creates");
    if (result.inputs.fast_plane == nullptr) {
        return result;
    }

    const ingress::FastCaptureSinkRefV1 sink =
        result.inputs.fast_plane->capture_sink();
    ingress::CaptureMetaV1 invalid_meta{};
    invalid_meta.source_stream_id =
        manifest.sources.front().source_stream_id;
    invalid_meta.capture_date = fast_config.capture_date + 1U;
    invalid_meta.ingress_sequence = 1U;
    invalid_meta.recv_realtime_ns = 1U;
    invalid_meta.recv_monotonic_ns = 1U;
    std::array<std::byte, sdk::kVendorHeadBytes> head{};
    test->Expect(
        sink.PublishCopy(invalid_meta, head, {}) ==
            ingress::FastCapturePublishResultV1::kFatal &&
            result.inputs.fast_plane->Snapshot().fatal,
        "capture-date mismatch latches shadow Fast fatal");

    for (std::size_t index = 0U; index < manifest.sources.size(); ++index) {
        if (!BuildStartableSource(
                test,
                directory,
                index,
                manifest.sources[index],
                manifest.trade_date,
                result.inputs.registry.get(),
                result.inputs.history.get(),
                result.counters,
                &result.inputs)) {
            return result;
        }
    }
    try {
        result.inputs.route_controller =
            std::make_unique<route::ProductionRouteControllerV1>(
                directory->descriptor(), manifest);
    } catch (...) {
        test->Expect(false, "startable route controller creates");
    }
    return result;
}

void TestConfigurationAndDependencyOrder(TestContext* test) {
    apps::ProductionServiceConfigV1 config{};
    config.activation_timeout = std::chrono::nanoseconds::zero();
    std::unique_ptr<apps::ProductionServiceV1> service;
    const auto invalid = apps::ProductionServiceV1::Create(
        config, {}, &service);
    test->Expect(
        invalid == apps::ProductionServiceCreateErrorV1::kInvalidConfiguration &&
            service == nullptr,
        "invalid lifecycle deadlines fail before dependency ownership changes");

    config = {};
    const auto missing = apps::ProductionServiceV1::Create(
        config, {}, &service);
    test->Expect(
        missing == apps::ProductionServiceCreateErrorV1::kNullRegistry,
        "dependency preflight is closed and deterministic");
    test->Expect(
        apps::OwnRawProductionCaptureRuntimeV1(nullptr) == nullptr,
        "null Raw runtime cannot be disguised as a capture dependency");
}

void TestCaptureBindingPredicate(TestContext* test) {
    canonical::SourceFrontierPageV1 page{};
    canonical::SourceFrontierPageV1 foreign_page{};
    runtime::ProductionSourcePipelineConfigV1 pipeline{};
    pipeline.source_slot = 3U;
    pipeline.ingress_kind = sdk::IngressKind::SzTick;
    pipeline.source_generation = 77U;

    route::ProductionRouteSourceV1 source{};
    source.source_stream_id =
        sdk::GetIngressSpec(pipeline.ingress_kind).source_stream_id;
    source.capture_date = 20260723U;
    source.stream_day_id = Pattern<16U>(0x11U);
    source.writer_instance = Pattern<16U>(0x31U);
    source.source_generation = pipeline.source_generation;

    canonical::SourceFrontierV1 frontier{};
    frontier.source_stream_id = source.source_stream_id;
    frontier.capture_date = source.capture_date;
    frontier.stream_day_id = source.stream_day_id;
    frontier.writer_instance = source.writer_instance;
    frontier.generation = source.source_generation;

    apps::ProductionCaptureBindingV1 binding{};
    binding.source_slot = pipeline.source_slot;
    binding.ingress_kind = pipeline.ingress_kind;
    binding.source_stream_id = source.source_stream_id;
    binding.capture_date = source.capture_date;
    binding.stream_day_id = source.stream_day_id;
    binding.writer_instance = source.writer_instance;
    binding.source_generation = source.source_generation;
    binding.source_frontier = &page;

    const auto matches = [&](const apps::ProductionCaptureBindingV1& value) {
        return apps::ProductionCaptureBindingMatchesV1(
            value, 3U, pipeline, source, frontier, &page);
    };
    test->Expect(matches(binding), "exact capture binding is accepted");

    auto changed = binding;
    changed.source_slot = 2U;
    test->Expect(!matches(changed), "capture slot mismatch is rejected");
    changed = binding;
    changed.ingress_kind = sdk::IngressKind::SzSnapshot;
    test->Expect(!matches(changed), "capture kind mismatch is rejected");
    changed = binding;
    ++changed.source_stream_id;
    test->Expect(!matches(changed), "capture source mismatch is rejected");
    changed = binding;
    ++changed.capture_date;
    test->Expect(!matches(changed), "capture date mismatch is rejected");
    changed = binding;
    changed.stream_day_id[0U] ^= std::byte{0x01U};
    test->Expect(!matches(changed), "capture stream-day mismatch is rejected");
    changed = binding;
    changed.writer_instance[0U] ^= std::byte{0x01U};
    test->Expect(!matches(changed), "capture writer mismatch is rejected");
    changed = binding;
    ++changed.source_generation;
    test->Expect(!matches(changed), "capture generation mismatch is rejected");
    changed = binding;
    changed.source_frontier = nullptr;
    test->Expect(!matches(changed), "null capture frontier is rejected");
    changed = binding;
    changed.source_frontier = &foreign_page;
    test->Expect(
        !matches(changed), "different capture frontier page is rejected");

    auto wrong_pipeline = pipeline;
    wrong_pipeline.source_slot = 2U;
    test->Expect(
        !apps::ProductionCaptureBindingMatchesV1(
            binding, 3U, wrong_pipeline, source, frontier, &page),
        "pipeline slot cannot be decoupled from the route position");
}

void TestNoConnectBeforeCompleteTopology(TestContext* test) {
    TemporaryDirectory directory;
    test->Expect(directory.ok(), "route directory opens");
    if (!directory.ok()) {
        return;
    }
    Prepared prepared = MakePrepared(test, &directory);
    std::unique_ptr<apps::ProductionServiceV1> service;
    const auto error = apps::ProductionServiceV1::Create(
        {}, std::move(prepared.inputs), &service);
    test->Expect(
        error == apps::ProductionServiceCreateErrorV1::kNullPipeline &&
            service == nullptr &&
            prepared.counters->starts.load(std::memory_order_relaxed) == 0U &&
            prepared.counters->stops.load(std::memory_order_relaxed) == 0U,
        "all four concrete pipelines must validate before any capture Start");
}

void TestIdentityFailuresPrecedeRuntimeStart(TestContext* test) {
    {
        TemporaryDirectory directory;
        Prepared prepared = MakePrepared(test, &directory, true, false);
        std::unique_ptr<apps::ProductionServiceV1> service;
        const auto error = apps::ProductionServiceV1::Create(
            {}, std::move(prepared.inputs), &service);
        if (error != apps::ProductionServiceCreateErrorV1::
                         kRegistryIdentityMismatch) {
            std::cerr << "registry mismatch returned "
                      << apps::ProductionServiceCreateErrorNameV1(error)
                      << '\n';
        }
        test->Expect(
            error ==
                    apps::ProductionServiceCreateErrorV1::
                        kRegistryIdentityMismatch &&
                prepared.counters->starts.load(std::memory_order_relaxed) == 0U,
            "route/registry identity mismatch fails before SDK lifecycle");
    }
    {
        TemporaryDirectory directory;
        Prepared prepared = MakePrepared(test, &directory, false, true);
        std::unique_ptr<apps::ProductionServiceV1> service;
        const auto error = apps::ProductionServiceV1::Create(
            {}, std::move(prepared.inputs), &service);
        if (error != apps::ProductionServiceCreateErrorV1::
                         kHistorySourceSetMismatch) {
            std::cerr << "history mismatch returned "
                      << apps::ProductionServiceCreateErrorNameV1(error)
                      << '\n';
        }
        test->Expect(
            error ==
                    apps::ProductionServiceCreateErrorV1::
                        kHistorySourceSetMismatch &&
                prepared.counters->starts.load(std::memory_order_relaxed) == 0U,
            "noncanonical four-source history set fails before SDK lifecycle");
    }
    {
        TemporaryDirectory directory;
        Prepared prepared =
            MakeStartablePrepared(test, &directory, false);
        std::unique_ptr<apps::ProductionServiceV1> service;
        const auto error = apps::ProductionServiceV1::Create(
            {}, std::move(prepared.inputs), &service);
        test->Expect(
            error ==
                    apps::ProductionServiceCreateErrorV1::
                        kFastPlaneBindingMismatch &&
                service == nullptr &&
                prepared.counters->starts.load(
                    std::memory_order_relaxed) == 0U,
            "Fast stream-day mismatch fails before SDK lifecycle");
    }
    {
        TemporaryDirectory directory;
        Prepared prepared =
            MakeStartablePrepared(test, &directory, true, false);
        std::unique_ptr<apps::ProductionServiceV1> service;
        const auto error = apps::ProductionServiceV1::Create(
            {}, std::move(prepared.inputs), &service);
        test->Expect(
            error ==
                    apps::ProductionServiceCreateErrorV1::
                        kFastPlaneBindingMismatch &&
                service == nullptr &&
                prepared.counters->starts.load(
                    std::memory_order_relaxed) == 0U,
            "Fast per-source capture-date mismatch fails before SDK lifecycle");
    }
}

void TestFastShadowFatalDoesNotRevokeLegacyRoute(TestContext* test) {
    TemporaryDirectory directory;
    test->Expect(directory.ok(), "shadow isolation route directory opens");
    if (!directory.ok()) {
        return;
    }
    Prepared prepared = MakeStartablePrepared(test, &directory);
    if (prepared.inputs.route_controller == nullptr) {
        return;
    }

    apps::ProductionServiceConfigV1 config{};
    config.aggregate.idle_wait = 100us;
    config.aggregate.history_backpressure_wait = 50us;
    config.aggregate.active_validation_interval = 100us;
    config.aggregate.writer_heartbeat_timeout = 5s;
    config.activation_timeout = 5s;
    config.activation_retry_wait = 1ms;
    config.history_barrier_wait = 10ms;
    config.drain_timeout = 5s;

    std::unique_ptr<apps::ProductionServiceV1> service;
    const auto create_error = apps::ProductionServiceV1::Create(
        config, std::move(prepared.inputs), &service);
    test->Expect(
        create_error == apps::ProductionServiceCreateErrorV1::kNone &&
            service != nullptr,
        "service accepts an independently failed Fast shadow");
    if (service == nullptr) {
        return;
    }

    const apps::ProductionServiceStartResultV1 start = service->Start();
    test->Expect(
        start.ok(),
        "Fast shadow fatal does not block authoritative Start");
    const apps::ProductionServiceSnapshotV1 active = service->Snapshot();
    test->Expect(
        active.state == apps::ProductionServiceStateV1::kActive &&
            active.aggregate.state ==
                runtime::ProductionAggregateStateV1::kRunning &&
            active.aggregate.active_route_published &&
            active.aggregate.fatal_reason_code == 0U,
        "legacy service and aggregate remain authoritative and Active");
    test->Expect(
        active.fast_plane_enabled && active.fast_plane.fatal,
        "snapshot reports the isolated Fast shadow fatal");

    market::InstrumentHistoryRecordHandleV1 latest;
    test->Expect(
        service->FastLatest(
            1U,
            0U,
            market::InstrumentHistoryLaneV1::kSnapshot,
            &latest) ==
                market::InstrumentHistoryQueryErrorV1::kSourceFatal &&
            !latest,
        "FastLatest closes the failed shadow generation");
    std::vector<market::InstrumentHistoryRecordHandleV1> tail;
    test->Expect(
        service->FastTail(
            1U,
            0U,
            market::InstrumentHistoryLaneV1::kSnapshot,
            1U,
            &tail) ==
                market::InstrumentHistoryQueryErrorV1::kSourceFatal &&
            tail.empty(),
        "FastTail closes the failed shadow generation");

    const apps::ProductionServiceStopResultV1 stop = service->Stop();
    test->Expect(
        stop.ok() &&
            service->Snapshot().state ==
                apps::ProductionServiceStateV1::kStopped &&
            prepared.counters->starts.load(std::memory_order_relaxed) == 4U &&
            prepared.counters->stops.load(std::memory_order_relaxed) == 4U,
        "failed Fast shadow and healthy legacy topology stop cleanly");
    const apps::ProductionServiceStopResultV1 repeated = service->Stop();
    test->Expect(
        repeated.ok() && repeated.already_stopped,
        "repeated stop after Fast fatal remains idempotent");
    service.reset();
}

}  // namespace

int main() {
    TestContext test;
    TestConfigurationAndDependencyOrder(&test);
    TestCaptureBindingPredicate(&test);
    TestNoConnectBeforeCompleteTopology(&test);
    TestIdentityFailuresPrecedeRuntimeStart(&test);
    TestFastShadowFatalDoesNotRevokeLegacyRoute(&test);
    if (test.failures != 0) {
        std::cerr << test.failures << " production service test(s) failed\n";
        return 1;
    }
    std::cout << "production service preflight tests passed\n";
    return 0;
}
