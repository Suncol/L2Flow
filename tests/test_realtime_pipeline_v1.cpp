#include "l2flow/runtime/realtime_pipeline_v1.h"
#include "l2flow/sdk/production_subscription_v1.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <unistd.h>

namespace {

namespace factor = l2flow::factor;
namespace market = l2flow::market;
namespace mdl = datayes::mdl;
namespace runtime = l2flow::runtime;
namespace sdk = l2flow::sdk;

using namespace std::chrono_literals;

std::uint64_t MonotonicClockNs() {
    struct timespec now {};
    if (::clock_gettime(CLOCK_MONOTONIC, &now) != 0 ||
        now.tv_sec < 0 || now.tv_nsec < 0) {
        return 0U;
    }
    return static_cast<std::uint64_t>(now.tv_sec) * 1'000'000'000ULL +
           static_cast<std::uint64_t>(now.tv_nsec);
}

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

class WireWriter final {
public:
    explicit WireWriter(std::size_t fixed_bytes)
        : bytes_(fixed_bytes, std::byte{0U}) {}

    void StoreU16(std::size_t offset, std::uint16_t value) {
        StoreUnsigned(offset, value);
    }
    void StoreU32(std::size_t offset, std::uint32_t value) {
        StoreUnsigned(offset, value);
    }
    void StoreU64(std::size_t offset, std::uint64_t value) {
        StoreUnsigned(offset, value);
    }

    void StoreString(std::size_t descriptor, std::string_view value) {
        const std::size_t start = bytes_.size();
        StoreU16(descriptor, static_cast<std::uint16_t>(value.size()));
        StoreU32(
            descriptor + sizeof(std::uint16_t),
            value.empty()
                ? 0U
                : static_cast<std::uint32_t>(start - descriptor));
        const auto characters = std::span<const char>(
            value.data(), value.size());
        const auto encoded = std::as_bytes(characters);
        bytes_.insert(bytes_.end(), encoded.begin(), encoded.end());
    }

    [[nodiscard]] std::vector<std::byte> Take() && {
        return std::move(bytes_);
    }

private:
    template <typename Unsigned>
    void StoreUnsigned(std::size_t offset, Unsigned value) {
        static_assert(std::is_unsigned_v<Unsigned>);
        for (std::size_t index = 0U; index < sizeof(Unsigned); ++index) {
            bytes_[offset + index] = static_cast<std::byte>(
                (value >> (index * 8U)) &
                static_cast<Unsigned>(0xffU));
        }
    }

    std::vector<std::byte> bytes_;
};

std::vector<std::byte> Bytes(std::string_view text) {
    const auto bytes = std::as_bytes(std::span(text));
    return {bytes.begin(), bytes.end()};
}

std::unique_ptr<market::InstrumentRegistryV1> MakeRegistry() {
    std::array<market::InstrumentRegistryEntryV1, 2U> entries{};
    entries[0].instrument_id = 18U;
    entries[0].key.market = market::MarketV1::kShenzhen;
    // This is a production fact, not formatting: MDL publishes the opaque
    // four-byte source key "102 ". Trimming it makes live lookups fail.
    entries[0].key.security_id_source = Bytes("102 ");
    entries[0].key.security_id = Bytes("000001");
    entries[0].quantity_unit = market::QuantityUnitV1::kShare;
    entries[0].security_type = market::SecurityTypeV1::kEquity;
    entries[0].asset_scope = market::AssetScopeV1::kDocumentedCore;

    entries[1].instrument_id = 7U;
    entries[1].key.market = market::MarketV1::kShanghai;
    entries[1].key.security_id = Bytes("600007");
    entries[1].quantity_unit = market::QuantityUnitV1::kShare;
    entries[1].security_type = market::SecurityTypeV1::kEquity;
    entries[1].asset_scope = market::AssetScopeV1::kDocumentedCore;

    std::unique_ptr<market::InstrumentRegistryV1> registry;
    if (market::InstrumentRegistryV1::Create(
            91U, entries, &registry) !=
        market::InstrumentRegistryCreateErrorV1::kNone) {
        return nullptr;
    }
    return registry;
}

std::unique_ptr<market::InstrumentRegistryV1> MakeInvalidSourceRegistry() {
    market::InstrumentRegistryEntryV1 entry{};
    entry.instrument_id = 18U;
    entry.key.market = market::MarketV1::kShenzhen;
    entry.key.security_id_source = Bytes("102");
    entry.key.security_id = Bytes("000001");
    entry.quantity_unit = market::QuantityUnitV1::kShare;
    entry.security_type = market::SecurityTypeV1::kEquity;
    entry.asset_scope = market::AssetScopeV1::kDocumentedCore;
    std::unique_ptr<market::InstrumentRegistryV1> registry;
    if (market::InstrumentRegistryV1::Create(
            92U,
            std::span<const market::InstrumentRegistryEntryV1>(&entry, 1U),
            &registry) !=
        market::InstrumentRegistryCreateErrorV1::kNone) {
        return nullptr;
    }
    return registry;
}

std::unique_ptr<market::InstrumentRegistryV1>
MakeUnreachableSecurityIdRegistry() {
    market::InstrumentRegistryEntryV1 entry{};
    entry.instrument_id = 19U;
    entry.key.market = market::MarketV1::kShanghai;
    entry.key.security_id = {
        std::byte{0x36U}, std::byte{0x00U}, std::byte{0x30U}};
    entry.quantity_unit = market::QuantityUnitV1::kShare;
    entry.security_type = market::SecurityTypeV1::kEquity;
    entry.asset_scope = market::AssetScopeV1::kDocumentedCore;
    std::unique_ptr<market::InstrumentRegistryV1> registry;
    if (market::InstrumentRegistryV1::Create(
            93U,
            std::span<const market::InstrumentRegistryEntryV1>(&entry, 1U),
            &registry) !=
        market::InstrumentRegistryCreateErrorV1::kNone) {
        return nullptr;
    }
    return registry;
}

std::vector<std::byte> ShenzhenOrderBody(std::uint64_t sequence) {
    constexpr std::size_t fixed_bytes = 58U;
    WireWriter writer(fixed_bytes);
    writer.StoreU32(0U, 12U);
    writer.StoreU64(4U, sequence);
    writer.StoreU64(30U, 123'456U);
    writer.StoreU64(38U, 201U);
    writer.StoreU32(46U, 49U);
    writer.StoreU32(50U, 93'000'123U);
    writer.StoreU32(54U, 50U);
    writer.StoreString(12U, "010");
    writer.StoreString(18U, "000001");
    writer.StoreString(24U, "102 ");
    return std::move(writer).Take();
}

std::vector<std::byte> ShenzhenSnapshotBody(
    std::int64_t normalized_last_price_p6) {
    constexpr std::size_t fixed_bytes = 224U;
    WireWriter writer(fixed_bytes);
    writer.StoreU32(0U, 93'000'123U);
    writer.StoreU32(4U, 12U);
    writer.StoreU64(32U, 12'000'000U);
    writer.StoreU64(40U, 1U);
    writer.StoreU64(48U, 100U);
    writer.StoreU64(56U, 1'234'560U);
    writer.StoreU64(
        64U, static_cast<std::uint64_t>(normalized_last_price_p6));
    writer.StoreString(8U, "010");
    writer.StoreString(14U, "000001");
    writer.StoreString(20U, "102 ");
    writer.StoreString(26U, "T");
    return std::move(writer).Take();
}

class FakeMessage final : public mdl::MDLMessage {
public:
    FakeMessage(sdk::MessageKey key, std::vector<std::byte> body)
        : body_(std::move(body)) {
        std::memset(&head_, 0, sizeof(head_));
        head_.HeadSize =
            static_cast<std::uint8_t>(sdk::kVendorHeadBytes);
        head_.MessageSize = static_cast<std::uint32_t>(
            sdk::kVendorHeadBytes + body_.size());
        head_.MessageEncoding =
            static_cast<std::uint8_t>(mdl::MDLEID_BINARY);
        head_.ServiceID = key.service_id;
        head_.ServiceVersion = key.service_version;
        head_.MessageID = key.message_id;
        head_.LocalTime.m_Value = 93'000'000U;
        head_.SequenceID = 9988U;
    }

    void AddRef() override {}
    int ReleaseRef() override { return 1; }
    mdl::MDLMessageHead* GetHead() const override {
        return const_cast<mdl::MDLMessageHead*>(&head_);
    }
    char* GetBody() const override {
        return body_.empty()
                   ? nullptr
                   : reinterpret_cast<char*>(
                         const_cast<std::byte*>(body_.data()));
    }
    mdl::MDLMessage* _Copy() const override { return nullptr; }

    void OverwriteBody() noexcept {
        std::fill(body_.begin(), body_.end(), std::byte{0xffU});
    }

private:
    mdl::MDLMessageHead head_{};
    std::vector<std::byte> body_;
};

struct PhysicalSdkState final {
    std::uint32_t manager_creations = 0U;
    std::uint32_t subscriber_creations = 0U;
    std::uint32_t connect_calls = 0U;
    std::uint32_t shutdown_calls = 0U;
    std::uint32_t subscriber_releases = 0U;
    std::uint32_t manager_releases = 0U;
    bool multithread_callback = true;
    mdl::MessageHandlerBase* handler = nullptr;
    std::vector<sdk::MessageKey> subscriptions;
    std::chrono::milliseconds shutdown_delay{0};
    std::uint64_t shutdown_enter_monotonic_ns = 0U;
    std::uint64_t shutdown_exit_monotonic_ns = 0U;
};

class FakeSubscriber final : public sdk::SdkSubscriber {
public:
    explicit FakeSubscriber(std::shared_ptr<PhysicalSdkState> state)
        : state_(std::move(state)) {}

    void SetServerAddress(std::string_view) override {}
    void SetUserName(std::string_view) override {}
    void SetHeartbeatInterval(std::uint32_t) override {}
    void SetHeartbeatTimeout(std::uint32_t) override {}
    void SetMessageEncoding(mdl::MDLMessageEncoding value) override {
        binary_ = value == mdl::MDLEID_BINARY;
    }
    void EnableMergeMessage(bool value) override { merge_ = value; }
    void SetSendMacAuth(bool) override {}
    void EnableServerSelect(bool) override {}
    void AddSubscription(const sdk::MessageKey& key) override {
        state_->subscriptions.push_back(key);
    }
    [[nodiscard]] std::string Connect() override {
        ++state_->connect_calls;
        return binary_ && !merge_ ? std::string{} : "bad test config";
    }
    [[nodiscard]] bool Release(std::string* error) noexcept override {
        if (!released_) {
            ++state_->subscriber_releases;
            released_ = true;
        }
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

private:
    std::shared_ptr<PhysicalSdkState> state_;
    bool binary_ = false;
    bool merge_ = true;
    bool released_ = false;
};

class FakeManager final : public sdk::SdkManager {
public:
    explicit FakeManager(std::shared_ptr<PhysicalSdkState> state)
        : state_(std::move(state)) {}

    void EnableLog(std::string_view, bool) override {}
    [[nodiscard]] std::unique_ptr<sdk::SdkSubscriber> CreateSubscriber(
        mdl::MessageHandlerBase* handler,
        bool multithread_callback) override {
        ++state_->subscriber_creations;
        state_->handler = handler;
        state_->multithread_callback = multithread_callback;
        return std::make_unique<FakeSubscriber>(state_);
    }
    void Shutdown() override {
        ++state_->shutdown_calls;
        state_->shutdown_enter_monotonic_ns = MonotonicClockNs();
        std::this_thread::sleep_for(state_->shutdown_delay);
        state_->shutdown_exit_monotonic_ns = MonotonicClockNs();
        shutdown_ = true;
    }
    [[nodiscard]] bool Release(std::string* error) noexcept override {
        if (!released_ && shutdown_) {
            ++state_->manager_releases;
            released_ = true;
        }
        if (error != nullptr) {
            error->clear();
        }
        return shutdown_;
    }

private:
    std::shared_ptr<PhysicalSdkState> state_;
    bool shutdown_ = false;
    bool released_ = false;
};

class FakeFactory final : public sdk::SdkFactory {
public:
    explicit FakeFactory(std::shared_ptr<PhysicalSdkState> state)
        : state_(std::move(state)) {}

    [[nodiscard]] std::unique_ptr<sdk::SdkManager> Create(
        int,
        int) override {
        ++state_->manager_creations;
        return std::make_unique<FakeManager>(state_);
    }

private:
    std::shared_ptr<PhysicalSdkState> state_;
};

runtime::RealtimePipelineConfigV1 MakeConfig(
    const market::InstrumentRegistryV1* registry,
    std::filesystem::path wal_path = {}) {
    runtime::RealtimePipelineConfigV1 config{};
    config.run_id[0U] = std::byte{0x31U};
    config.run_id[15U] = std::byte{0x73U};
    config.trade_date = 20260724U;
    config.registry = registry;
    config.source_stream_ids = {1001U, 1002U, 2001U, 2002U};
    config.maximum_sdk_message_bytes = 4096U;
    config.decoder_queue_capacity_per_source = 32U;
    config.store_worker_count = 2U;
    config.store_queue_capacity_per_source_worker = 32U;
    config.intraday_store.chunk_record_capacity = 8U;
    config.intraday_store.maximum_session_records = 1024U;
    config.intraday_store.maximum_session_accounted_bytes =
        64U * 1024U * 1024U;
    config.intraday_store.maximum_records_per_batch = 64U;
    config.intraday_store.coverage_from_open = true;
    config.enforce_receive_trade_date = false;
    config.wal.enabled = !wal_path.empty();
    config.wal.path = wal_path.string();
    config.wal.queue_capacity = config.wal.enabled ? 32U : 0U;
    config.wal.replace_existing = false;
    config.sdk.enabled = true;
    config.sdk.server_address = "127.0.0.1:9112";
    config.sdk.user_name = "single-chain-test";
    config.sdk.log_prefix = "single-chain-test";
    config.sdk.message_encoding = mdl::MDLEID_BINARY;
    config.sdk.merge_message = false;
    return config;
}

bool VerifyGeneration(
    TestContext* test,
    const runtime::RealtimePipelineCutResultV1& cut) {
    test->Expect(cut.published(), "store and factor publish together");
    if (!cut.published()) {
        return false;
    }

    market::IntradayInstrumentSummaryV1 first_summary{};
    market::IntradayInstrumentSummaryV1 row{};
    test->Expect(
        cut.store_generation->instrument_count() == 2U &&
            cut.store_generation->SummaryAt(0U, &first_summary) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            first_summary.instrument_id == 7U &&
            cut.store_generation->Find(18U, &row) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone,
        "store generation contains the exact ascending fixed universe");
    test->Expect(
        row.latest_tick != nullptr && row.record_count == 1U,
        "Shenzhen order reaches the instrument store");
    if (row.latest_tick == nullptr) {
        return false;
    }
    std::unique_ptr<market::IntradayInstrumentCursorV1> tail;
    std::array<const market::RealtimeHistoryRecordV1*, 2U> records{};
    std::size_t written = 0U;
    test->Expect(
        cut.store_generation->OpenTailCursor(
            18U, row.record_count, &tail) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            tail != nullptr &&
            tail->ReadBatch(records, &written) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            written == 1U && records[0U] == row.latest_tick &&
            records[0U]->ingress_sequence() == 1U,
        "tail cursor exposes the complete retained instrument prefix");
    const auto* order = market::RetainedMarketEventGetV1<
        market::ShenzhenOrderV1>(row.latest_tick->event());
    test->Expect(
        order != nullptr && order->common.security_id_source == "102 " &&
            order->common.instrument_id == 18U &&
            order->fields.quantity.raw == 201,
        "owned bytes decode with exact four-byte Shenzhen key");
    test->Expect(
        cut.factor_generation->input_store().get() ==
            cut.store_generation.get() &&
            cut.factor_generation->watermark().input_identity_sha256 ==
                cut.store_generation->watermark()
                    .input_identity_sha256,
        "factor publication retains the exact store generation/watermark");
    const factor::RealtimeFactorPointV1* factor_row =
        cut.factor_generation->Find(18U);
    test->Expect(
        factor_row != nullptr && factor_row->values.size() == 1U &&
            !factor_row->values[0U].valid,
        "snapshot projection is explicitly invalid for an order-only store");
    return true;
}

}  // namespace

int main() {
    TestContext test;
    std::unique_ptr<market::InstrumentRegistryV1> registry = MakeRegistry();
    test.Expect(registry != nullptr, "registry creation");
    if (registry == nullptr) {
        return 1;
    }

    std::unique_ptr<market::InstrumentRegistryV1> invalid_registry =
        MakeInvalidSourceRegistry();
    auto invalid_registry_state = std::make_shared<PhysicalSdkState>();
    std::unique_ptr<runtime::RealtimePipelineV1> invalid_pipeline;
    std::string invalid_detail;
    test.Expect(
        invalid_registry != nullptr &&
            runtime::RealtimePipelineV1::CreateForTest(
                MakeConfig(invalid_registry.get()),
                std::make_shared<FakeFactory>(invalid_registry_state),
                &invalid_pipeline,
                &invalid_detail) ==
                runtime::RealtimePipelineCreateErrorV1::
                    kInvalidConfiguration &&
            invalid_pipeline == nullptr &&
            invalid_registry_state->manager_creations == 0U,
        "production registry rejects trimmed Shenzhen source before SDK connect");

    std::unique_ptr<market::InstrumentRegistryV1> unreachable_registry =
        MakeUnreachableSecurityIdRegistry();
    auto unreachable_registry_state =
        std::make_shared<PhysicalSdkState>();
    std::unique_ptr<runtime::RealtimePipelineV1> unreachable_pipeline;
    invalid_detail.clear();
    test.Expect(
        unreachable_registry != nullptr &&
            runtime::RealtimePipelineV1::CreateForTest(
                MakeConfig(unreachable_registry.get()),
                std::make_shared<FakeFactory>(
                    unreachable_registry_state),
                &unreachable_pipeline,
                &invalid_detail) ==
                runtime::RealtimePipelineCreateErrorV1::
                    kInvalidConfiguration &&
            unreachable_pipeline == nullptr &&
            unreachable_registry_state->manager_creations == 0U,
        "production registry rejects SecurityID bytes the decoder cannot match");

    const std::filesystem::path wal_path =
        std::filesystem::path("/tmp") /
        ("l2flow-realtime-pipeline-" +
         std::to_string(static_cast<unsigned long>(::getpid())) + ".wal");
    std::error_code ignored;
    std::filesystem::remove(wal_path, ignored);

    auto sdk_state = std::make_shared<PhysicalSdkState>();
    sdk_state->shutdown_delay = 20ms;
    auto factory = std::make_shared<FakeFactory>(sdk_state);
    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    test.Expect(
        runtime::RealtimePipelineV1::CreateForTest(
            MakeConfig(registry.get(), wal_path),
            factory,
            &pipeline,
            &detail) == runtime::RealtimePipelineCreateErrorV1::kNone &&
            pipeline != nullptr,
        "unified pipeline creation: " + detail);
    if (pipeline == nullptr) {
        return 1;
    }
    test.Expect(
        sdk_state->manager_creations == 1U &&
            sdk_state->subscriber_creations == 1U &&
            sdk_state->connect_calls == 1U &&
            !sdk_state->multithread_callback &&
            sdk_state->subscriptions ==
                std::vector<sdk::MessageKey>(
                    sdk::kProductionMessageKeysV1.begin(),
                    sdk::kProductionMessageKeysV1.end()),
        "one physical manager/subscriber owns exactly five subscriptions");

    FakeMessage message(
        sdk::MessageKey{6U, 101U, 33U},
        ShenzhenOrderBody(20'001U));
    test.Expect(sdk_state->handler != nullptr, "SDK callback is installed");
    sdk_state->handler->OnMessage(nullptr, &message);
    // The callback has returned; destroying/mutating vendor storage must not
    // affect the decoder because the ingress boundary owns one immutable copy.
    message.OverwriteBody();
    // The production terminal path closes SDK callback admission before it
    // places the final markers, so this generation is the exact last accepted
    // prefix rather than a pre-shutdown sample.
    const runtime::RealtimePipelineCutResultV1 cut =
        pipeline->StopAndPublishFinalGeneration(2s);
    static_cast<void>(VerifyGeneration(&test, cut));
    test.Expect(
        cut.published() &&
            cut.store_generation->watermark().recv_monotonic_cut_ns <=
                sdk_state->shutdown_enter_monotonic_ns &&
            sdk_state->shutdown_enter_monotonic_ns <
                sdk_state->shutdown_exit_monotonic_ns,
        "terminal watermark is cut before slow SDK shutdown, not after it");

    const runtime::RealtimePipelineSnapshotV1 after_final =
        pipeline->Snapshot();
    test.Expect(
        after_final.accepted_messages == 1U &&
            after_final.decoded_messages == 1U &&
            after_final.global_ingress_sequence == 1U &&
            after_final.source_sequences[3U] == 1U &&
            after_final.stopped && !after_final.fatal,
        "terminal generation contains the exact last accepted source prefix");
    pipeline->StopAndDrain();
    const runtime::RealtimePipelineSnapshotV1 after_stop =
        pipeline->Snapshot();
    test.Expect(
        after_stop.wal.accepted_records == 1U &&
            after_stop.wal.written_records == 1U &&
            after_stop.wal.finished && !after_stop.wal.coverage_lost,
        "optional WAL drains the same handle without gating realtime");
    test.Expect(
        sdk_state->shutdown_calls == 1U &&
            sdk_state->subscriber_releases == 1U &&
            sdk_state->manager_releases == 1U,
        "SDK shutdown and releases occur exactly once");

    // WAL-disabled composition must produce the same realtime prefix and
    // generation identity. WAL is a side branch, never the decoder's source.
    auto no_wal_state = std::make_shared<PhysicalSdkState>();
    std::unique_ptr<runtime::RealtimePipelineV1> no_wal;
    detail.clear();
    test.Expect(
        runtime::RealtimePipelineV1::CreateForTest(
            MakeConfig(registry.get()),
            std::make_shared<FakeFactory>(no_wal_state),
            &no_wal,
            &detail) == runtime::RealtimePipelineCreateErrorV1::kNone &&
            no_wal != nullptr,
        "WAL-disabled pipeline creation: " + detail);
    if (no_wal != nullptr) {
        FakeMessage second(
            sdk::MessageKey{6U, 101U, 33U},
            ShenzhenOrderBody(20'001U));
        no_wal_state->handler->OnMessage(nullptr, &second);
        const runtime::RealtimePipelineCutResultV1 no_wal_cut =
            no_wal->CutAndPublishGeneration(2s);
        static_cast<void>(VerifyGeneration(&test, no_wal_cut));
        test.Expect(
            no_wal_cut.published() && cut.published() &&
                no_wal_cut.store_generation->watermark()
                        .input_identity_sha256 ==
                    cut.store_generation->watermark()
                        .input_identity_sha256,
            "WAL on/off has identical realtime generation identity");
        test.Expect(
            no_wal->Snapshot().wal.enabled == false,
            "disabled WAL stays outside realtime state");
        no_wal->StopAndDrain();
    }

    auto snapshot_state = std::make_shared<PhysicalSdkState>();
    std::unique_ptr<runtime::RealtimePipelineV1> snapshot_pipeline;
    runtime::RealtimePipelineConfigV1 snapshot_config =
        MakeConfig(registry.get());
    snapshot_config.intraday_store.chunk_record_capacity = 2U;
    snapshot_config.intraday_store.maximum_session_records = 16U;
    snapshot_config.intraday_store.maximum_session_accounted_bytes =
        16U * 1024U * 1024U;
    snapshot_config.intraday_store.maximum_records_per_batch = 4U;
    snapshot_config.intraday_store.coverage_from_open = true;
    detail.clear();
    test.Expect(
        runtime::RealtimePipelineV1::CreateForTest(
            std::move(snapshot_config),
            std::make_shared<FakeFactory>(snapshot_state),
            &snapshot_pipeline,
            &detail) == runtime::RealtimePipelineCreateErrorV1::kNone &&
            snapshot_pipeline != nullptr,
        "snapshot projection pipeline creation: " + detail);
    if (snapshot_pipeline != nullptr) {
        FakeMessage snapshot(
            sdk::MessageKey{6U, 101U, 28U},
            ShenzhenSnapshotBody(12'345'600));
        snapshot_state->handler->OnMessage(nullptr, &snapshot);
        const runtime::RealtimePipelineCutResultV1 snapshot_cut =
            snapshot_pipeline->CutAndPublishGeneration(2s);
        const factor::RealtimeFactorPointV1* point =
            snapshot_cut.published()
                ? snapshot_cut.factor_generation->Find(18U)
                : nullptr;
        test.Expect(
            snapshot_cut.published() && point != nullptr &&
                point->values.size() == 1U &&
                point->values[0U].valid &&
                std::abs(point->values[0U].value - 12.3456) < 1.0e-12,
            "binary SDK snapshot decodes through the store into a valid p6 price projection");
        const std::shared_ptr<
            const market::IntradayInstrumentStoreGenerationV1>
            store_generation = snapshot_cut.store_generation;
        test.Expect(
            store_generation != nullptr &&
                snapshot_cut.factor_generation != nullptr &&
                snapshot_cut.factor_generation->input_store() ==
                    store_generation &&
                snapshot_pipeline->AcquireLatestStoreGeneration() ==
                    store_generation,
            "cut publishes one exact store/factor generation");
        market::IntradayInstrumentSummaryV1 store_row{};
        std::unique_ptr<market::IntradayInstrumentCursorV1>
            store_cursor;
        std::array<const market::RealtimeHistoryRecordV1*, 2U>
            store_batch{};
        std::size_t store_written = 0U;
        const bool store_query_ok =
            store_generation != nullptr &&
            store_generation->coverage_from_open() &&
            store_generation->record_count() == 1U &&
            store_generation->SummaryAt(1U, &store_row) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            store_row.instrument_id == 18U &&
            store_row.record_count == 1U &&
            store_generation->OpenTailCursor(
                18U, store_row.record_count, &store_cursor) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            store_cursor != nullptr &&
            store_cursor->ReadBatch(store_batch, &store_written) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            store_written == 1U &&
            store_batch[0U] == store_row.latest_snapshot;
        test.Expect(
            store_query_ok,
            "required pipeline exposes the complete matching intraday prefix");
        const market::IntradayInstrumentStoreSnapshotV1 store_snapshot =
            snapshot_pipeline->Snapshot().store;
        test.Expect(
            store_snapshot.appended_records == 1U &&
                store_snapshot.coverage_from_open &&
                !store_snapshot.coverage_lost,
            "pipeline snapshot reports healthy from-open store coverage");
        snapshot_pipeline->StopAndDrain();
    }

    auto store_failure_state = std::make_shared<PhysicalSdkState>();
    std::unique_ptr<runtime::RealtimePipelineV1>
        store_failure_pipeline;
    runtime::RealtimePipelineConfigV1 store_failure_config =
        MakeConfig(registry.get());
    store_failure_config.intraday_store.chunk_record_capacity = 2U;
    store_failure_config.intraday_store.maximum_session_records = 1U;
    store_failure_config.intraday_store.maximum_session_accounted_bytes =
        16U * 1024U * 1024U;
    store_failure_config.intraday_store.maximum_records_per_batch = 4U;
    detail.clear();
    test.Expect(
        runtime::RealtimePipelineV1::CreateForTest(
            std::move(store_failure_config),
            std::make_shared<FakeFactory>(store_failure_state),
            &store_failure_pipeline,
            &detail) == runtime::RealtimePipelineCreateErrorV1::kNone &&
            store_failure_pipeline != nullptr,
        "required store-failure pipeline creation: " + detail);
    if (store_failure_pipeline != nullptr) {
        FakeMessage first(
            sdk::MessageKey{6U, 101U, 33U},
            ShenzhenOrderBody(40'001U));
        FakeMessage second(
            sdk::MessageKey{6U, 101U, 33U},
            ShenzhenOrderBody(40'002U));
        store_failure_state->handler->OnMessage(nullptr, &first);
        store_failure_state->handler->OnMessage(nullptr, &second);
        const auto failure_deadline =
            std::chrono::steady_clock::now() + 2s;
        while (!store_failure_pipeline->Snapshot().fatal &&
               std::chrono::steady_clock::now() < failure_deadline) {
            std::this_thread::yield();
        }
        const runtime::RealtimePipelineCutResultV1 failure_cut =
            store_failure_pipeline->CutAndPublishGeneration(2s);
        test.Expect(
            store_failure_pipeline->Snapshot()
                    .store.coverage_lost &&
                failure_cut.error ==
                    runtime::RealtimePipelineCutErrorV1::kFatal &&
                failure_cut.generation_error ==
                    market::RealtimeHistoryGenerationErrorV1::
                        kStoreFailed,
            "required async store failure remains specific in cut diagnostics");
        store_failure_pipeline->StopAndDrain();
    }

    // A production-date guard closes admission cleanly before any wrong-day
    // bytes acquire sequence numbers. The terminal API can still publish the
    // complete prior-day (here deliberately empty) prefix after SDK quiescence.
    auto boundary_state = std::make_shared<PhysicalSdkState>();
    runtime::RealtimePipelineConfigV1 boundary_config =
        MakeConfig(registry.get());
    boundary_config.trade_date = 22001231U;
    boundary_config.enforce_receive_trade_date = true;
    std::unique_ptr<runtime::RealtimePipelineV1> boundary_pipeline;
    detail.clear();
    test.Expect(
        runtime::RealtimePipelineV1::CreateForTest(
            std::move(boundary_config),
            std::make_shared<FakeFactory>(boundary_state),
            &boundary_pipeline,
            &detail) == runtime::RealtimePipelineCreateErrorV1::kNone &&
            boundary_pipeline != nullptr,
        "trade-date guarded pipeline creation: " + detail);
    if (boundary_pipeline != nullptr) {
        FakeMessage wrong_day(
            sdk::MessageKey{6U, 101U, 33U},
            ShenzhenOrderBody(30'001U));
        boundary_state->handler->OnMessage(nullptr, &wrong_day);
        const std::uint64_t boundary_observed_ns = MonotonicClockNs();
        const runtime::RealtimePipelineSnapshotV1 boundary_snapshot =
            boundary_pipeline->Snapshot();
        test.Expect(
            boundary_snapshot.trade_date_boundary_reached &&
                !boundary_snapshot.accepting &&
                !boundary_snapshot.fatal &&
                boundary_snapshot.accepted_messages == 0U &&
                boundary_snapshot.global_ingress_sequence == 0U,
            "wrong UTC+08 date closes admission without publishing bad data");
        std::this_thread::sleep_for(10ms);
        const runtime::RealtimePipelineCutResultV1 boundary_final =
            boundary_pipeline->StopAndPublishFinalGeneration(2s);
        test.Expect(
            boundary_final.published() &&
                boundary_final.store_generation->watermark()
                        .ingress_sequence_exclusive == 1U &&
                boundary_final.store_generation->watermark()
                        .recv_monotonic_cut_ns <= boundary_observed_ns &&
                boundary_pipeline->Snapshot().stopped,
            "date boundary preserves its original prefix-cut timestamp through shutdown");
    }

    std::filesystem::remove(wal_path, ignored);
    return test.failures() == 0 ? 0 : 1;
}
