#include "l2flow/canonical/canonical_schema_v1.h"
#include "l2flow/canonical/canonical_segment_v1.h"
#include "l2flow/canonical/source_frontier_v1.h"
#include "l2flow/control/control_decoder.h"
#include "l2flow/ingress/raw_live_tail.h"
#include "l2flow/ingress/raw_schema.h"
#include "l2flow/ingress/raw_v1.h"
#include "l2flow/market/instrument_history_v1.h"
#include "l2flow/market/instrument_registry.h"
#include "l2flow/route/production_route_posix_store_v1.h"
#include "l2flow/runtime/production_aggregate_runtime_v1.h"
#include "l2flow/sdk/subscription_manifest.h"

#include "mdl_api_msg.h"
#include "mdl_sys_msg.h"

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
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

namespace api = datayes::mdl::mdl_api_msg;
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

constexpr std::uint32_t kCaptureDate = 20260723U;
constexpr std::uint32_t kTradeDate = 20260723U;
constexpr std::uint64_t kRegistryVersion = 17U;
constexpr std::uint32_t kShenzhenInstrumentId = 501U;

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

constexpr std::array<sdk::IngressKind, 4U> kIngressKinds{{
    sdk::IngressKind::ShSnapshot,
    sdk::IngressKind::ShTick,
    sdk::IngressKind::SzSnapshot,
    sdk::IngressKind::SzTick,
}};

class TestContext final {
public:
    void Check(bool condition, std::string_view expression, int line) {
        if (!condition) {
            ++failures_;
            std::cerr << "line " << line << ": " << expression
                      << " failed\n";
        }
    }

    [[nodiscard]] int failures() const noexcept { return failures_; }

private:
    int failures_ = 0;
};

#define CHECK(test, expression) \
    (test)->Check((expression), #expression, __LINE__)

template <std::size_t Size>
std::array<std::byte, Size> Pattern(std::uint8_t seed) {
    std::array<std::byte, Size> value{};
    for (std::size_t index = 0U; index < value.size(); ++index) {
        value[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(seed + index));
    }
    return value;
}

std::vector<std::byte> Bytes(std::string_view text) {
    const std::span<const char> characters(text.data(), text.size());
    const std::span<const std::byte> bytes =
        std::as_bytes(characters);
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

RawMessage DisconnectedMessage() {
    return RawMessage{
        sdk::MessageKey{
            1U,
            101U,
            static_cast<std::uint16_t>(
                api::DisconnectedEvent::MessageID)},
        std::vector<std::byte>(
            sizeof(api::DisconnectedEvent), std::byte{0U})};
}

class TemporaryDirectory final {
public:
    TemporaryDirectory() {
        std::array<char, 64U> pattern{};
        constexpr std::string_view prefix =
            "/tmp/l2flow-production-aggregate-XXXXXX";
        std::copy(prefix.begin(), prefix.end(), pattern.begin());
        if (char* created = ::mkdtemp(pattern.data()); created != nullptr) {
            path_ = created;
            static_cast<void>(::chmod(path_.c_str(), 0700));
            fd_ = ::open(
                path_.c_str(),
                O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
        }
    }

    ~TemporaryDirectory() {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
        if (!path_.empty()) {
            std::error_code ignored;
            std::filesystem::remove_all(path_, ignored);
        }
    }

    [[nodiscard]] bool valid() const noexcept { return fd_ >= 0; }
    [[nodiscard]] int fd() const noexcept { return fd_; }
    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
    int fd_ = -1;
};

struct SourceRecordProgress final {
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t record_end_wal_pos = 0U;
    std::int64_t recv_monotonic_ns = 0;
};

class MutableRawSource final : public ingress::RawLiveTailSource {
public:
    [[nodiscard]] bool Initialize(
        const sdk::IngressSpec& spec,
        std::size_t source_slot,
        const common::Sha256Digest& stable_config,
        const common::Identity128& writer_instance,
        const canonical::ClockEpochIdentityV1& clock) {
        header_.source_stream_id = spec.source_stream_id;
        header_.capture_date = kCaptureDate;
        header_.stream_day_id = Pattern<16U>(
            static_cast<std::uint8_t>(0x11U + source_slot));
        header_.segment_sequence = 1U;
        header_.segment_base_wal_pos = 0U;
        header_.first_ingress_sequence = 1U;
        header_.created_realtime_ns = 10U;
        header_.created_monotonic_ns = 20U;
        header_.host_uuid = Pattern<16U>(
            static_cast<std::uint8_t>(0x21U + source_slot));
        header_.linux_boot_id = Pattern<16U>(
            static_cast<std::uint8_t>(0x31U + source_slot));
        header_.clock_epoch_algorithm = clock.algorithm;
        header_.clock_epoch_digest = clock.digest;
        header_.clock_epoch_label = clock.label;
        header_.sdk_archive_sha256 = Pattern<32U>(0x51U);
        header_.libmdl_api_sha256 = Pattern<32U>(0x61U);
        header_.endpoint_contract_sha256 = Pattern<32U>(0x71U);
        header_.config_sha256 = stable_config;
        header_.raw_schema_sha256 = ingress::RawSchemaSha256Digest();
        header_.build_manifest_sha256 = Pattern<32U>(0x91U);

        ingress::RawV1SegmentHeaderWire wire{};
        if (ingress::EncodeSegmentHeaderV1(header_, &wire) !=
            ingress::RawV1Error::kNone) {
            return false;
        }
        bytes_.assign(wire.begin(), wire.end());

        control_.writer_instance = writer_instance;
        control_.stream_day_id = header_.stream_day_id;
        control_.source_stream_id = spec.source_stream_id;
        control_.capture_date = kCaptureDate;
        control_.segment_sequence = 1U;
        control_.append_segment_offset = bytes_.size();
        control_.append_global_wal_pos = bytes_.size();
        control_.append_ingress_sequence = 0U;
        control_.durable_segment_offset = bytes_.size();
        control_.durable_global_wal_pos = bytes_.size();
        control_.durable_ingress_sequence = 0U;
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
        input.meta.capture_date = kCaptureDate;
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
        StoreU16(
            input.vendor_head, 7U, message.key.service_version);
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

    [[nodiscard]] int ReadControl(
        ingress::RawControlSnapshot* output,
        std::uint64_t* generation) noexcept override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!heartbeat_frozen_) {
            const std::uint64_t heartbeat = MonotonicNowForTest();
            if (heartbeat > control_.heartbeat_monotonic_ns) {
                control_.heartbeat_monotonic_ns = heartbeat;
                generation_ += 2U;
            }
        }
        *output = control_;
        *generation = generation_;
        return 0;
    }

    void FreezeHeartbeat() noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        heartbeat_frozen_ = true;
    }

    void Seal() noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        sealed_ = true;
        heartbeat_frozen_ = true;
        generation_ += 2U;
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
        const std::size_t count = std::min(
            output.size(), bytes_.size() - begin);
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
    bool heartbeat_frozen_ = false;
    bool sealed_ = false;
};

std::unique_ptr<market::InstrumentRegistryV1> MakeRegistry() {
    market::InstrumentRegistryEntryV1 entry{};
    entry.instrument_id = kShenzhenInstrumentId;
    entry.key.market = market::MarketV1::kShenzhen;
    entry.key.security_id_source = Bytes("102");
    entry.key.security_id = Bytes("000001");
    entry.quantity_unit = market::QuantityUnitV1::kShare;
    entry.security_type = market::SecurityTypeV1::kEquity;
    entry.asset_scope = market::AssetScopeV1::kDocumentedCore;
    std::unique_ptr<market::InstrumentRegistryV1> registry;
    if (market::InstrumentRegistryV1::Create(
            kRegistryVersion,
            std::span<const market::InstrumentRegistryEntryV1>(&entry, 1U),
            &registry) != market::InstrumentRegistryCreateErrorV1::kNone) {
        return nullptr;
    }
    return registry;
}

market::InstrumentHistoryRuntimeConfigV1 HistoryConfig() {
    market::InstrumentHistoryRuntimeConfigV1 config{};
    config.source_stream_ids =
        route::kProductionRouteSourceStreamIdsV1;
    config.physical_worker_count = 4U;
    config.queue_capacity = 64U;
    config.maximum_inflight_per_source = 256U;
    config.chunk_record_capacity = 8U;
    config.maximum_records_per_logical_shard = 4096U;
    config.maximum_instruments_per_logical_shard = 256U;
    config.maximum_owned_payload_bytes_per_logical_shard = 16U * 1024U * 1024U;
    return config;
}

class AggregateFixture final {
public:
    explicit AggregateFixture(TestContext* test) : test_(test) {}

    ~AggregateFixture() { Shutdown(); }

    [[nodiscard]] bool Initialize() {
        if (!directory_.valid()) {
            return Fail("temporary route directory opens");
        }
        registry_ = MakeRegistry();
        if (registry_ == nullptr ||
            market::InstrumentHistoryRuntimeV1::Create(
                HistoryConfig(), &history_) !=
                market::InstrumentHistoryCreateErrorV1::kNone) {
            return Fail("registry and shared history create");
        }

        for (std::size_t index = 0U; index < kIngressKinds.size(); ++index) {
            if (!InitializeSource(index)) {
                return false;
            }
        }

        const route::ProductionRouteManifestV1 manifest = MakeManifest();
        if (route::ValidateProductionRouteManifestV1(manifest) !=
            route::ProductionRouteManifestErrorV1::kNone) {
            return Fail("route manifest validates");
        }
        try {
            route_controller_ =
                std::make_unique<route::ProductionRouteControllerV1>(
                    directory_.fd(), manifest);
        } catch (...) {
            return Fail("route controller creates");
        }

        runtime::ProductionAggregateRuntimeConfigV1 config{};
        config.idle_wait = 100us;
        config.history_backpressure_wait = 50us;
        config.active_validation_interval = 100us;
        config.writer_heartbeat_timeout = 20ms;
        const runtime::ProductionAggregateCreateErrorV1 error =
            runtime::ProductionAggregateRuntimeV1::Create(
                config,
                std::move(pipelines_),
                history_.get(),
                route_controller_.get(),
                &aggregate_);
        if (error != runtime::ProductionAggregateCreateErrorV1::kNone ||
            aggregate_ == nullptr) {
            return Fail(runtime::ProductionAggregateCreateErrorNameV1(error));
        }
        return true;
    }

    [[nodiscard]] bool AppendReadinessSequence() {
        for (std::size_t index = 0U; index < kIngressKinds.size(); ++index) {
            const sdk::IngressSpec& spec =
                sdk::GetIngressSpec(kIngressKinds[index]);
            const std::vector<RawMessage> messages =
                ReadinessMessages(spec);
            if (messages.size() != spec.required.size() + 1U) {
                return Fail("readiness message set builds");
            }
            for (const RawMessage& message : messages) {
                if (!Append(index, message)) {
                    return false;
                }
            }
            expected_processed_[index] =
                static_cast<std::uint64_t>(messages.size());
        }
        return true;
    }

    [[nodiscard]] bool AppendDisconnect(std::size_t index) {
        return Append(index, DisconnectedMessage());
    }

    void FreezeRawHeartbeat(std::size_t index) noexcept {
        raw_sources_[index]->FreezeHeartbeat();
    }

    void SealRawSource(std::size_t index) noexcept {
        raw_sources_[index]->Seal();
    }

    void SealAllRawSources() noexcept {
        for (auto& source : raw_sources_) {
            source->Seal();
        }
    }

    [[nodiscard]] bool WaitUntilReady(
        runtime::ProductionAggregateActivationEvidenceV1* output) {
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < deadline) {
            const runtime::ProductionAggregateActivationEvidenceV1 evidence =
                aggregate_->CaptureActivationEvidence();
            bool ready =
                evidence.state == runtime::ProductionAggregateStateV1::kRunning &&
                !evidence.active_route_published &&
                evidence.fatal_reason_code == 0U;
            for (std::size_t index = 0U;
                 ready && index < evidence.sources.size();
                 ++index) {
                const auto& source = evidence.sources[index];
                ready = !source.pipeline.fatal && !source.pipeline.ended &&
                    !source.pipeline.history_pending &&
                    !source.pipeline.history_draining &&
                    source.control.control_ready &&
                    source.control.decoder_evidence_ready &&
                    source.raw_control.ok() &&
                    source.source_frontier_read_error ==
                        canonical::SourceFrontierErrorV1::kNone &&
                    source.source_frontier.source_state ==
                        canonical::SourceStateV1::kHealthy &&
                    source.source_frontier.processed_ingress_sequence ==
                        expected_processed_[index];
            }
            if (ready && aggregate_->WaitForHistoryBarriers(
                             evidence, 2s).ok()) {
                *output = evidence;
                return true;
            }
            std::this_thread::sleep_for(1ms);
        }
        const runtime::ProductionAggregateActivationEvidenceV1 evidence =
            aggregate_->CaptureActivationEvidence();
        std::cerr << "activation evidence timed out: state="
                  << static_cast<unsigned int>(evidence.state)
                  << " fatal_reason=" << evidence.fatal_reason_code << '\n';
        for (std::size_t index = 0U; index < evidence.sources.size(); ++index) {
            const auto& source = evidence.sources[index];
            std::cerr
                << "  source[" << index << "] pipeline_fatal="
                << source.pipeline.fatal
                << " terminal_failure="
                << static_cast<unsigned int>(
                       source.pipeline.terminal.failure)
                << " control_ready=" << source.control.control_ready
                << " decoder_ready="
                << source.control.decoder_evidence_ready
                << " raw_error="
                << static_cast<unsigned int>(source.raw_control.error)
                << " raw_errno=" << source.raw_control.error_number
                << " raw_generation=" << source.raw_control.generation
                << " raw_append="
                << source.raw_control.snapshot.append_ingress_sequence
                << " raw_durable="
                << source.raw_control.snapshot.durable_ingress_sequence
                << " control_processed="
                << source.control.processed_ingress_sequence
                << " frontier_error="
                << static_cast<unsigned int>(
                       source.source_frontier_read_error)
                << " frontier_state="
                << static_cast<unsigned int>(
                       source.source_frontier.source_state)
                << " frontier_processed="
                << source.source_frontier.processed_ingress_sequence
                << " expected=" << expected_processed_[index]
                << " history_pending=" << source.pipeline.history_pending
                << " history_submitted="
                << source.pipeline.history_frontier.submitted_ticket
                << " history_acknowledged="
                << source.pipeline.history_frontier.acknowledged_ticket
                << " history_fatal="
                << source.pipeline.history_frontier.fatal << '\n';
        }
        return false;
    }

    [[nodiscard]] bool WaitUntilFatal(
        std::uint32_t expected_reason =
            runtime::kProductionAggregateFatalSourceFailureV1) const {
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < deadline) {
            const runtime::ProductionAggregateSnapshotV1 snapshot =
                aggregate_->Snapshot();
            bool every_source_fatal = true;
            for (const auto& source : snapshot.sources) {
                every_source_fatal = every_source_fatal && source.fatal;
            }
            if (snapshot.state ==
                    runtime::ProductionAggregateStateV1::kFatal &&
                snapshot.active_route_published &&
                snapshot.fatal_route_published &&
                snapshot.fatal_reason_code == expected_reason &&
                every_source_fatal) {
                return true;
            }
            std::this_thread::sleep_for(1ms);
        }
        return false;
    }

    void StopAndCheck() {
        aggregate_->Stop();
        aggregate_->Stop();
        const runtime::ProductionAggregateSnapshotV1 snapshot =
            aggregate_->Snapshot();
        CHECK(test_, snapshot.state ==
                         runtime::ProductionAggregateStateV1::kFatal);
        CHECK(test_, snapshot.fatal_route_published);
        CHECK(test_, std::all_of(
                         snapshot.worker_exited.begin(),
                         snapshot.worker_exited.end(),
                         [](bool exited) { return exited; }));
    }

    [[nodiscard]] canonical::SourceFrontierPageV1& frontier(
        std::size_t index) noexcept {
        return frontiers_[index];
    }

    [[nodiscard]] const common::Identity128& writer_instance(
        std::size_t index) const noexcept {
        return writer_instances_[index];
    }

    [[nodiscard]] std::uint64_t source_generation(
        std::size_t index) const noexcept {
        return source_configs_[index].source_generation;
    }

    [[nodiscard]] runtime::ProductionAggregateRuntimeV1* aggregate()
        const noexcept {
        return aggregate_.get();
    }

    [[nodiscard]] market::InstrumentHistoryRuntimeV1* history()
        const noexcept {
        return history_.get();
    }

    [[nodiscard]] int route_directory_fd() const noexcept {
        return directory_.fd();
    }

private:
    [[nodiscard]] bool InitializeSource(std::size_t index) {
        const sdk::IngressSpec& spec =
            sdk::GetIngressSpec(kIngressKinds[index]);
        stable_configs_[index] = Pattern<32U>(
            static_cast<std::uint8_t>(0xa1U + index));
        writer_instances_[index] = Pattern<16U>(
            static_cast<std::uint8_t>(0xb1U + index));
        clocks_[index].algorithm = 1U;
        clocks_[index].digest = Pattern<32U>(
            static_cast<std::uint8_t>(0xc1U + index));
        clocks_[index].label = 7U + index;
        raw_sources_[index] = std::make_unique<MutableRawSource>();
        if (!raw_sources_[index]->Initialize(
                spec,
                index,
                stable_configs_[index],
                writer_instances_[index],
                clocks_[index])) {
            return Fail("Raw source initializes");
        }

        runtime::ProductionSourcePipelineConfigV1 pipeline_config{};
        pipeline_config.source_slot = static_cast<std::uint8_t>(index);
        pipeline_config.ingress_kind = kIngressKinds[index];
        pipeline_config.trade_date = kTradeDate;
        pipeline_config.source_generation = 20U + index;
        pipeline_config.canonical_generation = 40U + index;
        pipeline_config.normalizer_build_sha256 = Pattern<32U>(
            static_cast<std::uint8_t>(0xd1U + index));
        pipeline_config.normalizer_config_sha256 = Pattern<32U>(
            static_cast<std::uint8_t>(0xe1U + index));
        source_configs_[index] = pipeline_config;

        canonical::SourceFrontierConfigV1 frontier_config{};
        frontier_config.source_stream_id = spec.source_stream_id;
        frontier_config.capture_date = kCaptureDate;
        frontier_config.stream_day_id =
            raw_sources_[index]->header().stream_day_id;
        frontier_config.clock_epoch = clocks_[index];
        frontier_config.writer_instance = writer_instances_[index];
        frontier_config.generation = pipeline_config.source_generation;
        frontier_config.initial_global_wal_pos =
            ingress::kRawV1SegmentHeaderBytes;
        frontier_config.initial_state =
            canonical::SourceStateV1::kHealthy;
        if (canonical::InitializeSourceFrontierPageV1(
                frontier_config, &frontiers_[index]) !=
            canonical::SourceFrontierErrorV1::kNone) {
            return Fail("SourceFrontier initializes");
        }

        ingress::RawLiveTailAttachV1 attach{};
        attach.writer_instance = writer_instances_[index];
        attach.stream_day_id = raw_sources_[index]->header().stream_day_id;
        attach.source_stream_id = spec.source_stream_id;
        attach.capture_date = kCaptureDate;
        attach.segment_sequence = 1U;
        attach.global_wal_pos = ingress::kRawV1SegmentHeaderBytes;
        attach.segment_offset = ingress::kRawV1SegmentHeaderBytes;
        attach.next_ingress_sequence = 1U;
        std::unique_ptr<ingress::RawLiveTail> tail;
        if (ingress::RawLiveTail::Attach(
                raw_sources_[index].get(), attach, &tail) !=
            ingress::RawLiveTailError::kNone) {
            return Fail("Raw tail attaches");
        }

        control::ControlDecoderConfigV1 control_config{};
        control_config.source_stream_id = spec.source_stream_id;
        control_config.capture_date = kCaptureDate;
        control_config.stream_day_id = attach.stream_day_id;
        control_config.stable_config_sha256 = stable_configs_[index];
        control_config.required = spec.required;
        control_config.optional = spec.optional;
        std::unique_ptr<control::ControlDecoderV1> control_decoder;
        if (control::ControlDecoderV1::Create(
                control_config, &control_decoder) !=
            control::ControlDecoderCreateErrorV1::kNone) {
            return Fail("control decoder creates");
        }

        canonical::CanonicalNormalizerConfigV1 normalizer_config{};
        normalizer_config.capture_date = kCaptureDate;
        normalizer_config.trade_date = kTradeDate;
        normalizer_config.source_stream_id = spec.source_stream_id;
        normalizer_config.stream_day_id = attach.stream_day_id;
        normalizer_config.shard_count = 1U;
        normalizer_config.instrument_registry = registry_.get();
        normalizer_config.sequence_policy.policy_version = 1U;
        std::unique_ptr<canonical::CanonicalNormalizerV1> normalizer;
        if (canonical::CanonicalNormalizerV1::Create(
                normalizer_config, &normalizer) !=
            canonical::CanonicalNormalizerCreateErrorV1::kNone) {
            return Fail("Canonical normalizer creates");
        }

        std::vector<runtime::ProductionCanonicalSinkV1> sinks;
        if (!AddSink(index, canonical::CanonicalFamilyV1::kSnapshot,
                     canonical::CanonicalEventTypeV1::kSnapshot,
                     static_cast<std::uint32_t>(
                         canonical::kCanonicalSnapshotRecordBytesV1),
                     1U, &sinks) ||
            !AddSink(index, canonical::CanonicalFamilyV1::kTick,
                     canonical::CanonicalEventTypeV1::kTick,
                     static_cast<std::uint32_t>(
                         canonical::kCanonicalTickRecordBytesV1),
                     2U, &sinks) ||
            !AddSink(index, canonical::CanonicalFamilyV1::kQuality,
                     canonical::CanonicalEventTypeV1::kQuality,
                     static_cast<std::uint32_t>(
                         canonical::kCanonicalQualityRecordBytesV1),
                     3U, &sinks) ||
            !AddSink(index, canonical::CanonicalFamilyV1::kControl,
                     canonical::CanonicalEventTypeV1::kControl,
                     static_cast<std::uint32_t>(
                         canonical::kCanonicalControlRecordBytesV1),
                     4U, &sinks)) {
            return Fail("complete Canonical sink set creates");
        }

        const runtime::ProductionSourceCreateErrorV1 error =
            runtime::ProductionSourcePipelineV1::Create(
                pipeline_config,
                std::move(tail),
                std::move(control_decoder),
                std::move(normalizer),
                &frontiers_[index],
                std::move(sinks),
                history_.get(),
                &pipelines_[index]);
        if (error != runtime::ProductionSourceCreateErrorV1::kNone ||
            pipelines_[index] == nullptr) {
            return Fail(runtime::ProductionSourceCreateErrorNameV1(error));
        }
        return true;
    }

    [[nodiscard]] bool AddSink(
        std::size_t source_index,
        canonical::CanonicalFamilyV1 family,
        canonical::CanonicalEventTypeV1 event_type,
        std::uint32_t record_size,
        std::uint64_t segment_sequence,
        std::vector<runtime::ProductionCanonicalSinkV1>* sinks) {
        const sdk::IngressSpec& spec =
            sdk::GetIngressSpec(kIngressKinds[source_index]);
        canonical::CanonicalSegmentDescriptorV1 descriptor{};
        descriptor.event_type = event_type;
        descriptor.record_size = record_size;
        descriptor.source_stream_id = spec.source_stream_id;
        descriptor.shard = 0U;
        descriptor.trade_date = kTradeDate;
        descriptor.origin_capture_date = kCaptureDate;
        descriptor.origin_stream_day_id =
            raw_sources_[source_index]->header().stream_day_id;
        descriptor.origin_source_writer_instance =
            writer_instances_[source_index];
        descriptor.origin_source_generation =
            source_configs_[source_index].source_generation;
        descriptor.clock_epoch = clocks_[source_index];
        descriptor.schema_sha256 =
            canonical::CanonicalSchemaDescriptorSha256V1();
        descriptor.dtype_sha256 =
            canonical::CanonicalDtypeDescriptorSha256V1();
        descriptor.registry_version = registry_->registry_version();
        descriptor.registry_sha256 = registry_->registry_sha256();
        descriptor.normalizer_build_sha256 =
            source_configs_[source_index].normalizer_build_sha256;
        descriptor.normalizer_config_sha256 =
            source_configs_[source_index].normalizer_config_sha256;
        descriptor.generation =
            source_configs_[source_index].canonical_generation;
        descriptor.segment_sequence = segment_sequence;
        descriptor.capacity_records = 32U;

        const std::string stem = "source-" + std::to_string(source_index) +
            "-family-" + std::to_string(segment_sequence);
        canonical::CanonicalSegmentCreateOptionsV1 options{};
        options.segment_path = directory_.path() / (stem + ".clog");
        options.manifest_path = directory_.path() / (stem + ".manifest");
        options.descriptor = descriptor;
        options.created_realtime_ns = 1;
        options.created_monotonic_ns = 1;
        std::unique_ptr<canonical::CanonicalSegmentWriterV1> writer;
        if (canonical::CanonicalSegmentWriterV1::Create(
                options, &writer) !=
            canonical::CanonicalSegmentErrorV1::kNone ||
            writer->AdvanceProcessedRaw(
                0U, ingress::kRawV1SegmentHeaderBytes) !=
                canonical::CanonicalSegmentErrorV1::kNone) {
            return false;
        }
        sinks->push_back(runtime::ProductionCanonicalSinkV1{
            family, 0U, std::move(writer)});
        return true;
    }

    [[nodiscard]] route::ProductionRouteManifestV1 MakeManifest() const {
        route::ProductionRouteManifestV1 manifest{};
        manifest.state = route::ProductionRouteStateV1::kActive;
        manifest.generation = 1U;
        manifest.previous_generation = 0U;
        manifest.route_instance = Pattern<16U>(0x41U);
        manifest.trade_date = kTradeDate;
        manifest.registry_version = registry_->registry_version();
        manifest.registry_sha256 = registry_->registry_sha256();
        manifest.schema_sha256 =
            canonical::CanonicalSchemaDescriptorSha256V1();
        manifest.build_sha256 = Pattern<32U>(0x51U);
        manifest.config_sha256 = Pattern<32U>(0x61U);
        for (std::size_t index = 0U; index < manifest.sources.size(); ++index) {
            route::ProductionRouteSourceV1& source =
                manifest.sources[index];
            source.source_stream_id =
                route::kProductionRouteSourceStreamIdsV1[index];
            source.capture_date = kCaptureDate;
            source.stream_day_id =
                raw_sources_[index]->header().stream_day_id;
            source.writer_instance = writer_instances_[index];
            source.source_generation =
                source_configs_[index].source_generation;
            source.canonical_generation =
                source_configs_[index].canonical_generation;
            source.durable_ingress_sequence = 0U;
            source.durable_global_wal_pos = 0U;
            source.clock_epoch_identity_sha256 =
                route::ComputeProductionRouteClockEpochIdentitySha256V1(
                    clocks_[index]);
        }
        manifest.endpoints.canonical =
            (directory_.path() / "canonical.endpoint").string();
        manifest.endpoints.history =
            (directory_.path() / "history.endpoint").string();
        manifest.endpoints.state =
            (directory_.path() / "state.endpoint").string();
        manifest.endpoints.factor =
            (directory_.path() / "factor.endpoint").string();
        return manifest;
    }

    [[nodiscard]] bool Append(
        std::size_t index,
        const RawMessage& message) {
        const std::uint64_t expected_sequence =
            raw_sources_[index]->NextIngressSequence();
        canonical::SourceFrontierCallbackGuardV1 callback(
            &frontiers_[index],
            writer_instances_[index],
            source_configs_[index].source_generation);
        if (callback.error() !=
            canonical::SourceFrontierErrorV1::kNone) {
            std::cerr << "capture guard source=" << index
                      << " sequence=" << expected_sequence
                      << " error="
                      << canonical::SourceFrontierErrorNameV1(
                             callback.error())
                      << '\n';
            return Fail("producer capture frontier advances");
        }
        const canonical::SourceFrontierErrorV1 complete_error =
            callback.CompleteCaptured(expected_sequence);
        if (complete_error != canonical::SourceFrontierErrorV1::kNone) {
            std::cerr << "capture completion source=" << index
                      << " sequence=" << expected_sequence
                      << " error="
                      << canonical::SourceFrontierErrorNameV1(
                             complete_error)
                      << '\n';
            return Fail("producer capture frontier advances");
        }
        const SourceRecordProgress progress =
            raw_sources_[index]->Append(message);
        if (progress.ingress_sequence != expected_sequence ||
            progress.record_end_wal_pos == 0U ||
            canonical::PublishAppendProgressV1(
                &frontiers_[index],
                writer_instances_[index],
                source_configs_[index].source_generation,
                progress.ingress_sequence,
                progress.record_end_wal_pos,
                progress.recv_monotonic_ns) !=
                canonical::SourceFrontierErrorV1::kNone) {
            return Fail("Raw append frontier advances");
        }
        return true;
    }

    [[nodiscard]] bool Fail(std::string_view stage) {
        test_->Check(false, stage, __LINE__);
        return false;
    }

    void Shutdown() noexcept {
        if (aggregate_ != nullptr) {
            aggregate_->Stop();
            aggregate_.reset();
        }
        for (auto& pipeline : pipelines_) {
            pipeline.reset();
        }
        if (history_ != nullptr) {
            history_->StopAndDrain();
            history_.reset();
        }
        route_controller_.reset();
    }

    TestContext* test_ = nullptr;
    TemporaryDirectory directory_;
    std::unique_ptr<market::InstrumentRegistryV1> registry_;
    std::unique_ptr<market::InstrumentHistoryRuntimeV1> history_;
    std::array<std::unique_ptr<MutableRawSource>, 4U> raw_sources_{};
    std::array<canonical::SourceFrontierPageV1, 4U> frontiers_{};
    std::array<common::Identity128, 4U> writer_instances_{};
    std::array<common::Sha256Digest, 4U> stable_configs_{};
    std::array<canonical::ClockEpochIdentityV1, 4U> clocks_{};
    std::array<runtime::ProductionSourcePipelineConfigV1, 4U>
        source_configs_{};
    std::array<std::uint64_t, 4U> expected_processed_{};
    std::array<std::unique_ptr<runtime::ProductionSourcePipelineV1>, 4U>
        pipelines_{};
    std::unique_ptr<route::ProductionRouteControllerV1> route_controller_;
    std::unique_ptr<runtime::ProductionAggregateRuntimeV1> aggregate_;
};

struct OneShotRouteFault final {
    route::ProductionRouteStoreOperationV1 operation =
        route::ProductionRouteStoreOperationV1::kAfterRename;
    bool fired = false;

    static bool Hook(
        void* context,
        route::ProductionRouteStoreOperationV1 operation) noexcept {
        auto* fault = static_cast<OneShotRouteFault*>(context);
        if (fault != nullptr && !fault->fired &&
            operation == fault->operation) {
            fault->fired = true;
            return false;
        }
        return true;
    }
};

struct ReentrantRouteHookProbe final {
    runtime::ProductionAggregateRuntimeV1* aggregate = nullptr;
    runtime::ProductionAggregatePublishResultV1 nested_publish{};
    runtime::ProductionAggregateBeginDrainResultV1 nested_begin_drain{};
    runtime::ProductionAggregateSnapshotV1 snapshot{};
    runtime::ProductionAggregateActivationEvidenceV1 evidence{};
    bool fired = false;
    bool capture_completed = false;

    static bool Hook(
        void* context,
        route::ProductionRouteStoreOperationV1 operation) noexcept {
        auto* probe = static_cast<ReentrantRouteHookProbe*>(context);
        if (probe == nullptr || probe->aggregate == nullptr ||
            probe->fired || operation !=
                route::ProductionRouteStoreOperationV1::kAfterLock) {
            return true;
        }
        probe->fired = true;
        probe->snapshot = probe->aggregate->Snapshot();
        try {
            probe->evidence =
                probe->aggregate->CaptureActivationEvidence();
            probe->capture_completed = true;
        } catch (...) {
            probe->capture_completed = false;
        }
        probe->nested_publish = probe->aggregate->PublishActive();
        probe->nested_begin_drain = probe->aggregate->BeginDrain();
        probe->aggregate->Stop();
        return true;
    }
};

runtime::ProductionAggregatePublishResultV1 PublishActiveEventually(
    AggregateFixture* fixture,
    const route::ProductionRouteStoreOptionsV1& options = {},
    std::string* diagnostic = nullptr) {
    runtime::ProductionAggregatePublishResultV1 result{};
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    do {
        if (diagnostic != nullptr) {
            diagnostic->clear();
        }
        result = fixture->aggregate()->PublishActive(options, diagnostic);
        if (result.ok() ||
            result.error != runtime::ProductionAggregatePublishErrorV1::
                kReadinessIncomplete) {
            return result;
        }
        std::this_thread::sleep_for(1ms);
    } while (std::chrono::steady_clock::now() < deadline);
    return result;
}

[[nodiscard]] bool AssertActivationLifecycle(
    TestContext* test,
    AggregateFixture* fixture) {
    std::string diagnostic;
    const runtime::ProductionAggregatePublishResultV1 premature =
        fixture->aggregate()->PublishActive({}, &diagnostic);
    CHECK(test, !premature.ok());
    CHECK(test, premature.error ==
                    runtime::ProductionAggregatePublishErrorV1::
                        kReadinessIncomplete);
    CHECK(test, premature.activation_error ==
                    runtime::ProductionSourceActivationGateErrorV1::
                        kControlEvidenceIncomplete);
    CHECK(test, premature.source_slot < 4U);
    const route::ProductionRouteReadResultV1 absent =
        route::ReadProductionRouteManifestV1At(
            fixture->route_directory_fd());
    CHECK(test, absent.error ==
                    route::ProductionRouteStoreErrorV1::kRouteNotFound);
    CHECK(test, !absent.authoritative());
    CHECK(test, absent.manifest == nullptr);
    const runtime::ProductionAggregateSnapshotV1 still_running =
        fixture->aggregate()->Snapshot();
    CHECK(test, still_running.state ==
                    runtime::ProductionAggregateStateV1::kRunning);
    CHECK(test, !still_running.active_route_published);
    CHECK(test, still_running.fatal_reason_code == 0U);

    if (!fixture->AppendReadinessSequence()) {
        return false;
    }
    runtime::ProductionAggregateActivationEvidenceV1 evidence{};
    if (!fixture->WaitUntilReady(&evidence)) {
        CHECK(test, false);
        return false;
    }
    const runtime::ProductionAggregatePublishResultV1 published =
        PublishActiveEventually(fixture, {}, &diagnostic);
    CHECK(test, published.ok());
    CHECK(test, published.route.authoritative());
    CHECK(test, !published.already_active);
    if (!published.ok()) {
        const runtime::ProductionAggregateSnapshotV1 snapshot =
            fixture->aggregate()->Snapshot();
        std::cerr << "active publication failed: error="
                  << runtime::ProductionAggregatePublishErrorNameV1(
                         published.error)
                  << " activation_error="
                  << runtime::ProductionSourceActivationGateErrorNameV1(
                         published.activation_error)
                  << " source_slot="
                  << static_cast<unsigned int>(published.source_slot)
                  << " diagnostic=" << diagnostic
                  << " aggregate_state="
                  << static_cast<unsigned int>(snapshot.state)
                  << " fatal_reason=" << snapshot.fatal_reason_code
                  << '\n';
        for (std::size_t index = 0U;
             index < snapshot.sources.size();
             ++index) {
            const auto& source = snapshot.sources[index];
            std::cerr << "  source[" << index << "] fatal="
                      << source.fatal << " ended=" << source.ended
                      << " terminal_failure="
                      << static_cast<unsigned int>(
                             source.terminal.failure)
                      << " raw_tail_error="
                      << static_cast<unsigned int>(
                             source.terminal.raw_tail_error)
                      << " frontier_error="
                      << static_cast<unsigned int>(
                             source.terminal.frontier_error)
                      << " history_fatal="
                      << source.history_frontier.fatal << '\n';
        }
        return false;
    }

    const route::ProductionRouteReadResultV1 active =
        route::ReadProductionRouteManifestV1At(
            fixture->route_directory_fd());
    CHECK(test, active.authoritative());
    CHECK(test, active.manifest != nullptr);
    CHECK(test, active.manifest != nullptr &&
                    active.manifest->state ==
                        route::ProductionRouteStateV1::kActive);

    market::InstrumentHistoryRecordHandleV1 latest;
    CHECK(test, fixture->history()->Latest(
                    kShenzhenInstrumentId,
                    3U,
                    market::InstrumentHistoryLaneV1::kTick,
                    &latest) == market::InstrumentHistoryQueryErrorV1::kNone);
    CHECK(test, latest &&
                    latest->instrument_id() == kShenzhenInstrumentId &&
                    latest->source_stream_id() == 2002U &&
                    latest->source_sequence() == 3U);
    return true;
}

void AssertFatalRoute(TestContext* test, AggregateFixture* fixture) {
    CHECK(test, fixture->WaitUntilFatal());
    const route::ProductionRouteReadResultV1 fatal =
        route::ReadProductionRouteManifestV1At(
            fixture->route_directory_fd());
    CHECK(test, fatal.error ==
                    route::ProductionRouteStoreErrorV1::kRouteFatal);
    CHECK(test, !fatal.authoritative());
    CHECK(test, fatal.manifest != nullptr);
    CHECK(test, fatal.manifest != nullptr &&
                    fatal.manifest->state ==
                        route::ProductionRouteStateV1::kFatal &&
                    fatal.manifest->generation == 2U &&
                    fatal.manifest->previous_generation == 1U &&
                    fatal.manifest->fatal_reason_code ==
                        runtime::kProductionAggregateFatalSourceFailureV1);
    fixture->StopAndCheck();
}

void TestControlDisconnectRevokesWholeGeneration(TestContext* test) {
    AggregateFixture fixture(test);
    if (!fixture.Initialize()) {
        return;
    }
    if (!AssertActivationLifecycle(test, &fixture)) {
        return;
    }
    if (!fixture.AppendDisconnect(0U)) {
        return;
    }
    AssertFatalRoute(test, &fixture);
}

void TestExternalSourceFatalRevokesWholeGeneration(TestContext* test) {
    AggregateFixture fixture(test);
    if (!fixture.Initialize()) {
        return;
    }
    if (!AssertActivationLifecycle(test, &fixture)) {
        return;
    }
    CHECK(test, canonical::PublishSourceStateV1(
                    &fixture.frontier(2U),
                    fixture.writer_instance(2U),
                    fixture.source_generation(2U),
                    canonical::SourceStateV1::kFatal,
                    0xD00DU) == canonical::SourceFrontierErrorV1::kNone);
    AssertFatalRoute(test, &fixture);
}

void TestStaleRawHeartbeatRevokesWholeGeneration(TestContext* test) {
    AggregateFixture fixture(test);
    if (!fixture.Initialize()) {
        return;
    }
    if (!AssertActivationLifecycle(test, &fixture)) {
        return;
    }
    fixture.FreezeRawHeartbeat(1U);
    AssertFatalRoute(test, &fixture);
}

void TestUncertainActiveConvergesBeforeReturn(TestContext* test) {
    AggregateFixture fixture(test);
    if (!fixture.Initialize() || !fixture.AppendReadinessSequence()) {
        return;
    }
    runtime::ProductionAggregateActivationEvidenceV1 evidence{};
    if (!fixture.WaitUntilReady(&evidence)) {
        CHECK(test, false);
        return;
    }

    OneShotRouteFault fault{};
    route::ProductionRouteStoreOptionsV1 options{};
    options.operation_hook = &OneShotRouteFault::Hook;
    options.operation_hook_context = &fault;
    const runtime::ProductionAggregatePublishResultV1 published =
        PublishActiveEventually(&fixture, options);
    const runtime::ProductionAggregateSnapshotV1 active =
        fixture.aggregate()->Snapshot();
    CHECK(test, fault.fired);
    CHECK(test, published.ok() && published.route.authoritative());
    CHECK(test, active.active_route_published);
    CHECK(test, !active.active_route_publication_uncertain);

    if (!published.ok() || !fixture.AppendDisconnect(0U)) {
        return;
    }
    AssertFatalRoute(test, &fixture);
}

void TestRouteHookReentryFailsClosedWithoutDeadlock(TestContext* test) {
    AggregateFixture fixture(test);
    if (!fixture.Initialize() || !fixture.AppendReadinessSequence()) {
        return;
    }
    runtime::ProductionAggregateActivationEvidenceV1 readiness{};
    if (!fixture.WaitUntilReady(&readiness)) {
        CHECK(test, false);
        return;
    }

    ReentrantRouteHookProbe probe{};
    probe.aggregate = fixture.aggregate();
    route::ProductionRouteStoreOptionsV1 options{};
    options.operation_hook = &ReentrantRouteHookProbe::Hook;
    options.operation_hook_context = &probe;
    const runtime::ProductionAggregatePublishResultV1 published =
        PublishActiveEventually(&fixture, options);

    CHECK(test, probe.fired);
    CHECK(test, probe.snapshot.route_operation_hook_active);
    CHECK(test, probe.capture_completed);
    CHECK(test, probe.evidence.evidence_suppressed_by_operation_hook);
    CHECK(test, probe.nested_publish.error ==
                    runtime::ProductionAggregatePublishErrorV1::
                        kReentrantCall);
    CHECK(test, probe.nested_begin_drain.error ==
                    runtime::ProductionAggregateBeginDrainErrorV1::
                        kReentrantCall);
    CHECK(test, published.route.authoritative());
    CHECK(test, published.error ==
                    runtime::ProductionAggregatePublishErrorV1::
                        kBecameFatal);
    CHECK(test, fixture.WaitUntilFatal(
                    runtime::kProductionAggregateFatalInternalFailureV1));
    const route::ProductionRouteReadResultV1 fatal =
        route::ReadProductionRouteManifestV1At(
            fixture.route_directory_fd());
    CHECK(test, fatal.error ==
                    route::ProductionRouteStoreErrorV1::kRouteFatal);
    CHECK(test, fatal.manifest != nullptr);
    CHECK(test, fatal.manifest != nullptr &&
                    fatal.manifest->fatal_reason_code ==
                        runtime::kProductionAggregateFatalInternalFailureV1);
    fixture.StopAndCheck();
}

void TestTwoStageDrainWaitsForEverySourceEnd(TestContext* test) {
    AggregateFixture fixture(test);
    if (!fixture.Initialize() ||
        !AssertActivationLifecycle(test, &fixture)) {
        return;
    }

    const runtime::ProductionAggregateDrainWaitResultV1 invalid_timeout =
        fixture.aggregate()->WaitForDrain(-1ns);
    CHECK(test, invalid_timeout.error ==
                    runtime::ProductionAggregateDrainWaitErrorV1::
                        kInvalidTimeout);
    const runtime::ProductionAggregateDrainWaitResultV1 not_draining =
        fixture.aggregate()->WaitForDrain(0ns);
    CHECK(test, not_draining.error ==
                    runtime::ProductionAggregateDrainWaitErrorV1::
                        kNotDraining);

    const runtime::ProductionAggregateBeginDrainResultV1 begin =
        fixture.aggregate()->BeginDrain();
    CHECK(test, begin.ok());
    CHECK(test, !begin.already_draining);
    CHECK(test, begin.route_revocation_required);
    CHECK(test, begin.route.ok());
    const runtime::ProductionAggregateSnapshotV1 draining =
        fixture.aggregate()->Snapshot();
    CHECK(test, draining.state ==
                    runtime::ProductionAggregateStateV1::kDraining);
    CHECK(test, draining.drain_route_revoked);
    CHECK(test, draining.fatal_route_published);
    CHECK(test, draining.fatal_reason_code ==
                    runtime::kProductionAggregateFatalStoppedActiveV1);

    // This models the service-owned Raw shutdown boundary.  Sealing also
    // freezes writer heartbeats; that is normal after BeginDrain and must not
    // trigger the Active-only validity monitor.
    fixture.SealAllRawSources();
    const runtime::ProductionAggregateDrainWaitResultV1 drained =
        fixture.aggregate()->WaitForDrain(5s);
    CHECK(test, drained.ok());
    const runtime::ProductionAggregateSnapshotV1 after_drain =
        fixture.aggregate()->Snapshot();
    CHECK(test, after_drain.state ==
                    runtime::ProductionAggregateStateV1::kDraining);
    CHECK(test, std::all_of(
                    after_drain.sources.begin(),
                    after_drain.sources.end(),
                    [](const runtime::ProductionSourceSnapshotV1& source) {
                        return source.ended && !source.fatal &&
                            !source.history_pending &&
                            !source.history_draining &&
                            source.history_frontier.submitted_ticket ==
                                source.history_frontier.acknowledged_ticket;
                    }));

    fixture.aggregate()->Stop();
    const runtime::ProductionAggregateSnapshotV1 stopped =
        fixture.aggregate()->Snapshot();
    CHECK(test, stopped.state ==
                    runtime::ProductionAggregateStateV1::kStopped);
    CHECK(test, std::all_of(
                    stopped.worker_exited.begin(),
                    stopped.worker_exited.end(),
                    [](bool exited) { return exited; }));
}

void TestUnexpectedActiveSourceEndFailsWholeGeneration(TestContext* test) {
    AggregateFixture fixture(test);
    if (!fixture.Initialize() ||
        !AssertActivationLifecycle(test, &fixture)) {
        return;
    }

    fixture.SealRawSource(0U);
    CHECK(test, fixture.WaitUntilFatal(
                    runtime::kProductionAggregateFatalActiveSourceEndV1));
    const route::ProductionRouteReadResultV1 fatal =
        route::ReadProductionRouteManifestV1At(
            fixture.route_directory_fd());
    CHECK(test, fatal.error ==
                    route::ProductionRouteStoreErrorV1::kRouteFatal);
    CHECK(test, fatal.manifest != nullptr);
    CHECK(test, fatal.manifest != nullptr &&
                    fatal.manifest->fatal_reason_code ==
                        runtime::kProductionAggregateFatalActiveSourceEndV1);
    fixture.StopAndCheck();
}

void TestDrainTimeoutThenDirectStop(TestContext* test) {
    AggregateFixture fixture(test);
    if (!fixture.Initialize() ||
        !AssertActivationLifecycle(test, &fixture)) {
        return;
    }

    const runtime::ProductionAggregateBeginDrainResultV1 begin =
        fixture.aggregate()->BeginDrain();
    CHECK(test, begin.ok());
    const runtime::ProductionAggregateDrainWaitResultV1 timed_out =
        fixture.aggregate()->WaitForDrain(5ms);
    CHECK(test, timed_out.error ==
                    runtime::ProductionAggregateDrainWaitErrorV1::kTimeout);
    const runtime::ProductionAggregateSnapshotV1 still_draining =
        fixture.aggregate()->Snapshot();
    CHECK(test, still_draining.state ==
                    runtime::ProductionAggregateStateV1::kDraining);
    CHECK(test, still_draining.drain_route_revoked);

    fixture.aggregate()->Stop();
    const runtime::ProductionAggregateSnapshotV1 stopped =
        fixture.aggregate()->Snapshot();
    CHECK(test, stopped.state ==
                    runtime::ProductionAggregateStateV1::kStopped);
    CHECK(test, std::all_of(
                    stopped.worker_exited.begin(),
                    stopped.worker_exited.end(),
                    [](bool exited) { return exited; }));
    CHECK(test, fixture.aggregate()->WaitForDrain(0ns).error ==
                    runtime::ProductionAggregateDrainWaitErrorV1::
                        kQuiescedBeforeEnd);
}

void TestDrainReportsExactSourceFailure(TestContext* test) {
    AggregateFixture fixture(test);
    if (!fixture.Initialize() ||
        !AssertActivationLifecycle(test, &fixture)) {
        return;
    }

    CHECK(test, fixture.aggregate()->BeginDrain().ok());
    CHECK(test, canonical::PublishSourceStateV1(
                    &fixture.frontier(2U),
                    fixture.writer_instance(2U),
                    fixture.source_generation(2U),
                    canonical::SourceStateV1::kFatal,
                    0xD12AU) == canonical::SourceFrontierErrorV1::kNone);
    const runtime::ProductionAggregateDrainWaitResultV1 failed =
        fixture.aggregate()->WaitForDrain(5s);
    CHECK(test, failed.error ==
                    runtime::ProductionAggregateDrainWaitErrorV1::
                        kSourceFatal);
    CHECK(test, failed.source_slot == 2U);
    fixture.StopAndCheck();
}

void TestStopWaitsForDurableFatalRoute(TestContext* test) {
    AggregateFixture fixture(test);
    if (!fixture.Initialize() ||
        !AssertActivationLifecycle(test, &fixture)) {
        return;
    }
    if (::fchmod(fixture.route_directory_fd(), 0500) != 0) {
        CHECK(test, false);
        return;
    }

    std::atomic<bool> stop_returned{false};
    std::thread stopper([&fixture, &stop_returned]() noexcept {
        fixture.aggregate()->Stop();
        stop_returned.store(true, std::memory_order_release);
    });
    bool failure_observed = false;
    bool draining_observed = false;
    const auto failure_deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < failure_deadline) {
        const runtime::ProductionAggregateSnapshotV1 snapshot =
            fixture.aggregate()->Snapshot();
        if (snapshot.last_fatal_publish.error !=
            route::ProductionRouteControllerErrorV1::kNone) {
            failure_observed = true;
            draining_observed = snapshot.state ==
                runtime::ProductionAggregateStateV1::kDraining;
            break;
        }
        std::this_thread::sleep_for(1ms);
    }
    CHECK(test, failure_observed);
    CHECK(test, draining_observed);
    CHECK(test, !stop_returned.load(std::memory_order_acquire));
    CHECK(test, ::fchmod(fixture.route_directory_fd(), 0700) == 0);
    stopper.join();
    CHECK(test, stop_returned.load(std::memory_order_acquire));

    const runtime::ProductionAggregateSnapshotV1 stopped =
        fixture.aggregate()->Snapshot();
    CHECK(test, stopped.fatal_route_published);
    const route::ProductionRouteReadResultV1 fatal =
        route::ReadProductionRouteManifestV1At(
            fixture.route_directory_fd());
    CHECK(test, fatal.error ==
                    route::ProductionRouteStoreErrorV1::kRouteFatal);
    CHECK(test, fatal.manifest != nullptr);
    CHECK(test, fatal.manifest != nullptr &&
                    fatal.manifest->fatal_reason_code ==
                        runtime::kProductionAggregateFatalStoppedActiveV1);
}

}  // namespace

int main() {
    TestContext test;
    TestControlDisconnectRevokesWholeGeneration(&test);
    TestExternalSourceFatalRevokesWholeGeneration(&test);
    TestStaleRawHeartbeatRevokesWholeGeneration(&test);
    TestUncertainActiveConvergesBeforeReturn(&test);
    TestRouteHookReentryFailsClosedWithoutDeadlock(&test);
    TestTwoStageDrainWaitsForEverySourceEnd(&test);
    TestUnexpectedActiveSourceEndFailsWholeGeneration(&test);
    TestDrainTimeoutThenDirectStop(&test);
    TestDrainReportsExactSourceFailure(&test);
    TestStopWaitsForDurableFatalRoute(&test);
    if (test.failures() != 0) {
        std::cerr << test.failures()
                  << " production aggregate integration check(s) failed\n";
        return 1;
    }
    std::cout << "production aggregate integration checks passed\n";
    return 0;
}
