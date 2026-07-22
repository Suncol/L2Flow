#include "l2flow/canonical/canonical_normalizer_v1.h"
#include "l2flow/control/quality_flags_v1.h"
#include "l2flow/market/instrument_registry.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace canonical = l2flow::canonical;
namespace control = l2flow::control;
namespace market = l2flow::market;

namespace {

constexpr std::uint32_t kCaptureDate = 20260723U;
constexpr std::uint32_t kTradeDate = 20260722U;
constexpr std::uint32_t kSource = 404U;
constexpr std::uint16_t kVersion = 101U;

struct TestContext final {
    void Expect(bool condition, const std::string& label) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << label << '\n';
        }
    }
    int failures = 0;
};

class WireWriter final {
public:
    explicit WireWriter(std::size_t fixed_bytes)
        : bytes_(fixed_bytes, std::byte{0U}) {}

    void StoreU16(std::size_t offset, std::uint16_t value) {
        bytes_.at(offset) = static_cast<std::byte>(value & 0xffU);
        bytes_.at(offset + 1U) = static_cast<std::byte>(
            (value >> 8U) & 0xffU);
    }
    void StoreU32(std::size_t offset, std::uint32_t value) {
        for (std::size_t index = 0U; index < 4U; ++index) {
            bytes_.at(offset + index) = static_cast<std::byte>(
                (value >> static_cast<unsigned int>(index * 8U)) & 0xffU);
        }
    }
    void StoreI32(std::size_t offset, std::int32_t value) {
        StoreU32(offset, static_cast<std::uint32_t>(value));
    }
    void StoreU64(std::size_t offset, std::uint64_t value) {
        for (std::size_t index = 0U; index < 8U; ++index) {
            bytes_.at(offset + index) = static_cast<std::byte>(
                (value >> static_cast<unsigned int>(index * 8U)) & 0xffU);
        }
    }
    void StoreI64(std::size_t offset, std::int64_t value) {
        StoreU64(offset, static_cast<std::uint64_t>(value));
    }
    std::size_t Append(std::size_t count) {
        const std::size_t begin = bytes_.size();
        bytes_.resize(begin + count, std::byte{0U});
        return begin;
    }
    void StoreString(std::size_t descriptor, std::string_view value) {
        const std::size_t begin = Append(value.size());
        for (std::size_t index = 0U; index < value.size(); ++index) {
            bytes_.at(begin + index) = static_cast<std::byte>(
                static_cast<unsigned char>(value[index]));
        }
        StoreU16(descriptor, static_cast<std::uint16_t>(value.size()));
        StoreU32(
            descriptor + 2U,
            static_cast<std::uint32_t>(begin - descriptor));
    }
    std::size_t BeginList(
        std::size_t descriptor,
        std::uint32_t count,
        std::size_t item_size) {
        const std::size_t begin =
            Append(static_cast<std::size_t>(count) * item_size);
        StoreU32(descriptor, count);
        StoreU32(
            descriptor + 4U,
            static_cast<std::uint32_t>(begin - descriptor));
        return begin;
    }
    std::vector<std::byte> Take() && {
        return std::move(bytes_);
    }

private:
    std::vector<std::byte> bytes_;
};

struct ShanghaiTickSpec final {
    std::int64_t sequence = 100;
    std::int32_t channel = 7;
    std::int64_t quantity = 99;
    std::int64_t trade_amount = 7000;
    std::string security_id = "600000";
    std::string type = "A";
    std::string flag = "B";
};

std::vector<std::byte> ShanghaiTick(const ShanghaiTickSpec& spec) {
    WireWriter writer(70U);
    writer.StoreI64(0U, spec.sequence);
    writer.StoreI32(8U, spec.channel);
    writer.StoreU32(18U, 93000123U);
    writer.StoreI64(28U, 1101);
    writer.StoreI64(36U, 2202);
    writer.StoreI32(44U, 12345);
    writer.StoreI64(48U, spec.quantity);
    writer.StoreI64(56U, spec.trade_amount);
    writer.StoreString(12U, spec.security_id);
    writer.StoreString(22U, spec.type);
    writer.StoreString(64U, spec.flag);
    return std::move(writer).Take();
}

struct ShenzhenOrderSpec final {
    std::uint32_t channel = 12U;
    std::int64_t sequence = 10;
    std::int32_t raw_side = 49;
    std::int32_t raw_order_type = 50;
};

std::vector<std::byte> ShenzhenOrder(const ShenzhenOrderSpec& spec) {
    WireWriter writer(58U);
    writer.StoreU32(0U, spec.channel);
    writer.StoreI64(4U, spec.sequence);
    writer.StoreI64(30U, 123456);
    writer.StoreI64(38U, 700);
    writer.StoreI32(46U, spec.raw_side);
    writer.StoreU32(50U, 93000123U);
    writer.StoreI32(54U, spec.raw_order_type);
    writer.StoreString(12U, "010");
    writer.StoreString(18U, "000001");
    writer.StoreString(24U, "102");
    return std::move(writer).Take();
}

struct ShenzhenTransactionSpec final {
    std::uint32_t channel = 12U;
    std::int64_t sequence = 13;
};

std::vector<std::byte> ShenzhenTransaction(
    const ShenzhenTransactionSpec& spec) {
    WireWriter writer(70U);
    writer.StoreU32(0U, spec.channel);
    writer.StoreI64(4U, spec.sequence);
    writer.StoreI64(18U, 8101);
    writer.StoreI64(26U, 8202);
    writer.StoreI64(46U, 123456);
    writer.StoreI64(54U, 50);
    writer.StoreI32(62U, 70);
    writer.StoreU32(66U, 93000123U);
    writer.StoreString(12U, "010");
    writer.StoreString(34U, "000001");
    writer.StoreString(40U, "102");
    return std::move(writer).Take();
}

std::vector<std::byte> ShanghaiSnapshot(
    std::uint32_t bid_queue_count,
    std::int64_t trade_volume = 123000) {
    constexpr std::size_t kFixed = 248U;
    constexpr std::size_t kLevelBytes = 28U;
    constexpr std::size_t kQueueBytes = 16U;
    WireWriter writer(kFixed);
    writer.StoreU32(0U, 93100123U);
    writer.StoreI32(10U, 3);
    writer.StoreI32(14U, 10000);
    writer.StoreI32(18U, 10100);
    writer.StoreI32(22U, 10300);
    writer.StoreI32(26U, 9900);
    writer.StoreI32(30U, 10200);
    writer.StoreI32(34U, 10250);
    writer.StoreU32(44U, 42U);
    writer.StoreI64(48U, trade_volume);
    writer.StoreI64(56U, 45600000);
    writer.StoreU32(220U, 1U);
    writer.StoreU32(224U, 1U);
    writer.StoreI32(244U, 10123);
    writer.StoreString(4U, "600000");
    writer.StoreString(38U, "TRADE");
    const std::size_t bid = writer.BeginList(228U, 1U, kLevelBytes);
    const std::size_t ask = writer.BeginList(236U, 1U, kLevelBytes);
    writer.StoreI32(bid + 4U, 10200);
    writer.StoreI64(bid + 8U, 1000);
    writer.StoreU32(bid + 16U, 77U);
    writer.StoreI32(ask + 4U, 10201);
    writer.StoreI64(ask + 8U, 2000);
    writer.StoreU32(ask + 16U, 2U);
    const std::size_t queue = writer.BeginList(
        bid + 20U, bid_queue_count, kQueueBytes);
    for (std::uint32_t index = 0U; index < bid_queue_count; ++index) {
        writer.StoreI64(
            queue + static_cast<std::size_t>(index) * kQueueBytes + 8U,
            static_cast<std::int64_t>(index + 1U) * 1000);
    }
    (void)writer.BeginList(ask + 20U, 0U, kQueueBytes);
    return std::move(writer).Take();
}

std::vector<std::byte> Bytes(std::string_view value) {
    const std::span<const char> chars(value.data(), value.size());
    const auto bytes = std::as_bytes(chars);
    return std::vector<std::byte>(bytes.begin(), bytes.end());
}

std::unique_ptr<market::InstrumentRegistryV1> Registry() {
    std::vector<market::InstrumentRegistryEntryV1> entries(2U);
    entries[0].instrument_id = 33U;
    entries[0].key.market = market::MarketV1::kShanghai;
    entries[0].key.security_id = Bytes("600000");
    entries[0].quantity_unit = market::QuantityUnitV1::kShare;
    entries[0].security_type = market::SecurityTypeV1::kEquity;
    entries[0].asset_scope = market::AssetScopeV1::kDocumentedCore;
    entries[1].instrument_id = 50U;
    entries[1].key.market = market::MarketV1::kShenzhen;
    entries[1].key.security_id_source = Bytes("102");
    entries[1].key.security_id = Bytes("000001");
    entries[1].quantity_unit = market::QuantityUnitV1::kShare;
    entries[1].security_type = market::SecurityTypeV1::kEquity;
    entries[1].asset_scope = market::AssetScopeV1::kDocumentedCore;
    std::unique_ptr<market::InstrumentRegistryV1> registry;
    if (market::InstrumentRegistryV1::Create(7U, entries, &registry) !=
        market::InstrumentRegistryCreateErrorV1::kNone) {
        return nullptr;
    }
    return registry;
}

canonical::ClockEpochIdentityV1 Clock() {
    canonical::ClockEpochIdentityV1 clock{};
    clock.algorithm = 1U;
    clock.digest[0] = std::byte{0x42U};
    clock.label = 9U;
    return clock;
}

canonical::CanonicalRawContextV1 Raw(
    std::uint64_t ingress,
    std::uint32_t connection_epoch = 1U) {
    canonical::CanonicalRawContextV1 raw{};
    raw.capture_date = kCaptureDate;
    raw.trade_date = kTradeDate;
    raw.source_stream_id = kSource;
    raw.stream_day_id[0] = std::byte{0x11U};
    raw.source_writer_instance[0] = std::byte{0x12U};
    raw.source_generation = 1U;
    raw.origin_ingress_sequence = ingress;
    raw.origin_wal_end_pos = ingress * 100U;
    raw.authoritative_connection_epoch = connection_epoch;
    raw.clock_epoch = Clock();
    return raw;
}

market::MarketMessageViewV1 Message(
    std::uint8_t service,
    std::uint16_t message_id,
    std::span<const std::byte> body,
    std::uint64_t ingress,
    std::uint64_t vendor_sequence) {
    market::MarketMessageViewV1 message{};
    message.source_stream_id = kSource;
    message.trade_date = kTradeDate;
    message.source_sequence = ingress;
    message.service_id = service;
    message.service_version = kVersion;
    message.message_id = message_id;
    message.message_encoding = 1U;
    message.vendor_local_time_raw = 93000000U;
    message.vendor_sequence_id = vendor_sequence;
    message.recv_realtime_ns = 1'000'000 +
        static_cast<std::int64_t>(ingress);
    message.recv_monotonic_ns = 2'000'000 +
        static_cast<std::int64_t>(ingress);
    message.body = body;
    return message;
}

canonical::CanonicalNormalizerConfigV1 NormalizerConfig(
    const market::InstrumentRegistryV1* registry) {
    canonical::CanonicalNormalizerConfigV1 config{};
    config.capture_date = kCaptureDate;
    config.trade_date = kTradeDate;
    config.source_stream_id = kSource;
    config.stream_day_id[0] = std::byte{0x11U};
    config.instrument_registry = registry;
    config.sequence_policy.policy_version = 17U;
    return config;
}

std::unique_ptr<canonical::CanonicalNormalizerV1> NormalizerFromConfig(
    canonical::CanonicalNormalizerConfigV1 config) {
    std::unique_ptr<canonical::CanonicalNormalizerV1> normalizer;
    if (canonical::CanonicalNormalizerV1::Create(
            config, &normalizer) !=
        canonical::CanonicalNormalizerCreateErrorV1::kNone) {
        return nullptr;
    }
    return normalizer;
}

std::unique_ptr<canonical::CanonicalNormalizerV1> Normalizer(
    const market::InstrumentRegistryV1* registry) {
    return NormalizerFromConfig(NormalizerConfig(registry));
}

const canonical::CanonicalTickRecordV1* FindTick(
    const canonical::CanonicalNormalizationTransactionV1& transaction) {
    for (const auto& routed : transaction.records()) {
        if (const auto* tick = std::get_if<
                canonical::CanonicalTickRecordV1>(&routed.record)) {
            return tick;
        }
    }
    return nullptr;
}

const canonical::CanonicalSnapshotRecordV1* FindSnapshot(
    const canonical::CanonicalNormalizationTransactionV1& transaction) {
    for (const auto& routed : transaction.records()) {
        if (const auto* snapshot = std::get_if<
                canonical::CanonicalSnapshotRecordV1>(&routed.record)) {
            return snapshot;
        }
    }
    return nullptr;
}

const canonical::CanonicalQualityRecordV1* FindQuality(
    const canonical::CanonicalNormalizationTransactionV1& transaction,
    canonical::CanonicalQualityTypeV1 type) {
    for (const auto& routed : transaction.records()) {
        if (const auto* quality = std::get_if<
                canonical::CanonicalQualityRecordV1>(&routed.record);
            quality != nullptr && quality->payload.quality_type == type) {
            return quality;
        }
    }
    return nullptr;
}

bool Commit(
    canonical::CanonicalNormalizerV1* normalizer,
    canonical::CanonicalNormalizationTransactionV1* transaction) {
    return normalizer->CommitPublished(
               transaction, transaction->publication_contract()) ==
           canonical::CanonicalNormalizeCommitErrorV1::kNone;
}

void TestTransactionalPhaseAndOrigin(TestContext* context) {
    auto registry = Registry();
    auto normalizer = Normalizer(registry.get());
    context->Expect(normalizer != nullptr, "normalizer creation succeeds");
    if (normalizer == nullptr) {
        return;
    }

    ShanghaiTickSpec status{};
    status.type = "S";
    status.flag = "TRADE";
    std::vector<std::byte> body = ShanghaiTick(status);
    canonical::CanonicalNormalizationTransactionV1 transaction;
    auto prepared = normalizer->Prepare(
        Raw(1U), Message(4U, 24U, body, 1U, 1U), &transaction);
    context->Expect(
        prepared.ok() && prepared.business_record_planned &&
            transaction.active(),
        "SH status Prepare creates an uncommitted business plan");
    context->Expect(
        normalizer->Abort(&transaction),
        "pre-publication Abort succeeds");
    auto snapshot = normalizer->Snapshot();
    context->Expect(
        snapshot.version == 0U && snapshot.phase_product_count == 0U &&
            snapshot.tick_records_committed == 0U,
        "Abort leaves sequence version, phase attribution and event IDs unchanged");

    {
        auto failed_normalizer = Normalizer(registry.get());
        canonical::CanonicalNormalizationTransactionV1 failed_transaction;
        const auto failed_prepare = failed_normalizer->Prepare(
            Raw(1U), Message(4U, 24U, body, 1U, 1U),
            &failed_transaction);
        auto wrong = failed_transaction.publication_contract();
        ++wrong.record_count;
        context->Expect(
            failed_prepare.ok() &&
                failed_normalizer->CommitPublished(
                    &failed_transaction, wrong) ==
                    canonical::CanonicalNormalizeCommitErrorV1::
                        kPublicationMismatch &&
                failed_normalizer->Snapshot().fatal,
            "post-publication receipt mismatch fail-stops the generation");
        context->Expect(
            !failed_normalizer->Abort(&failed_transaction) &&
                !failed_transaction.active(),
            "fatal cleanup detaches the transaction but never marks the generation reusable");
    }

    prepared = normalizer->Prepare(
        Raw(1U), Message(4U, 24U, body, 1U, 1U), &transaction);
    const auto* status_tick = FindTick(transaction);
    context->Expect(
        prepared.ok() && status_tick != nullptr &&
            status_tick->header.shard_event_id == 1U &&
            status_tick->header.trade_date == kTradeDate &&
            transaction.segment_context().capture_date == kCaptureDate &&
            transaction.segment_context().trade_date == kTradeDate &&
            status_tick->header.connection_epoch == 1U &&
            status_tick->payload.source_enum_bits ==
                static_cast<std::uint64_t>('S'),
        "retry reuses ID 1 and keeps capture namespace separate from trade date");
    context->Expect(Commit(normalizer.get(), &transaction),
                    "accepted SH status commits");

    ShanghaiTickSpec add{};
    add.sequence = 101;
    body = ShanghaiTick(add);
    prepared = normalizer->Prepare(
        Raw(2U, 9U), Message(4U, 24U, body, 2U, 2U), &transaction);
    const auto* add_tick = FindTick(transaction);
    context->Expect(
        prepared.ok() && add_tick != nullptr &&
            add_tick->payload.phase ==
                canonical::CanonicalTradingPhaseV1::kContinuous &&
            (add_tick->payload.validity_bitmap &
             canonical::CanonicalTickValidityBitV1(
                 canonical::CanonicalTickValidityV1::kPhase)) != 0U &&
            add_tick->header.connection_epoch == 9U &&
            normalizer->Snapshot().exchange_scope_count == 1U,
        "committed status attributes next SH tick and reconnect does not reset channel guard");
    context->Expect(Commit(normalizer.get(), &transaction),
                    "post-reconnect contiguous SH event commits");
}

void TestDuplicateConflictAndPoison(TestContext* context) {
    auto registry = Registry();
    auto normalizer = Normalizer(registry.get());
    ShanghaiTickSpec spec{};
    spec.type = "S";
    spec.flag = "TRADE";
    std::vector<std::byte> body = ShanghaiTick(spec);
    canonical::CanonicalNormalizationTransactionV1 transaction;
    auto prepared = normalizer->Prepare(
        Raw(1U), Message(4U, 24U, body, 1U, 1U), &transaction);
    context->Expect(prepared.ok() && Commit(normalizer.get(), &transaction),
                    "baseline exchange event commits");

    prepared = normalizer->Prepare(
        Raw(2U), Message(4U, 24U, body, 2U, 1U), &transaction);
    context->Expect(
        prepared.vendor_outcome ==
                canonical::SequenceGuardOutcomeV1::kExactDuplicate &&
            FindTick(transaction) == nullptr &&
            FindQuality(
                transaction,
                canonical::CanonicalQualityTypeV1::
                    kVendorSequenceDuplicate) != nullptr,
        "vendor exact duplicate is byte-exact, diagnosed and suppresses business");
    context->Expect(Commit(normalizer.get(), &transaction),
                    "vendor duplicate diagnostic commits");

    body.push_back(std::byte{0xa5U});
    prepared = normalizer->Prepare(
        Raw(3U), Message(4U, 24U, body, 3U, 2U), &transaction);
    context->Expect(
        prepared.exchange_outcome ==
                canonical::SequenceGuardOutcomeV1::kExactDuplicate &&
            FindTick(transaction) == nullptr &&
            FindQuality(
                transaction,
                canonical::CanonicalQualityTypeV1::
                    kExchangeSequenceDuplicate) != nullptr,
        "new vendor delivery with exact business key/body is an exchange duplicate");
    context->Expect(Commit(normalizer.get(), &transaction),
                    "exchange duplicate diagnostic commits");

    spec.flag = "SUSP";
    body = ShanghaiTick(spec);
    prepared = normalizer->Prepare(
        Raw(4U), Message(4U, 24U, body, 4U, 3U), &transaction);
    const auto* conflict_quality = FindQuality(
        transaction,
        canonical::CanonicalQualityTypeV1::kExchangeSequenceConflict);
    context->Expect(
        prepared.exchange_outcome ==
                canonical::SequenceGuardOutcomeV1::kConflict &&
            FindTick(transaction) == nullptr &&
            conflict_quality != nullptr &&
            conflict_quality->payload.first_bad_origin_wal_end_pos == 400U,
        "same exchange key with different exact evidence poisons the channel");
    context->Expect(Commit(normalizer.get(), &transaction),
                    "exchange conflict commits poison state");

    spec.sequence = 101;
    body = ShanghaiTick(spec);
    prepared = normalizer->Prepare(
        Raw(5U), Message(4U, 24U, body, 5U, 4U), &transaction);
    const auto* poisoned_quality = FindQuality(
        transaction,
        canonical::CanonicalQualityTypeV1::kScopePoisoned);
    context->Expect(
        prepared.exchange_outcome ==
                canonical::SequenceGuardOutcomeV1::kPoisoned &&
            FindTick(transaction) == nullptr &&
            poisoned_quality != nullptr &&
            poisoned_quality->payload.first_bad_origin_wal_end_pos == 400U,
        "poisoned exchange scope preserves the original first-bad WAL");
    context->Expect(Commit(normalizer.get(), &transaction),
                    "poisoned-scope progress still commits vendor observation");
}

void TestGapUnifiedSzAndReconnect(TestContext* context) {
    auto registry = Registry();
    auto normalizer = Normalizer(registry.get());
    canonical::CanonicalNormalizationTransactionV1 transaction;

    ShenzhenOrderSpec order{};
    std::vector<std::byte> body = ShenzhenOrder(order);
    auto prepared = normalizer->Prepare(
        Raw(1U), Message(6U, 33U, body, 1U, 10U), &transaction);
    context->Expect(prepared.ok() && Commit(normalizer.get(), &transaction),
                    "first SZ order starts unified channel scope");

    order.sequence = 12;
    body = ShenzhenOrder(order);
    prepared = normalizer->Prepare(
        Raw(2U), Message(6U, 33U, body, 2U, 11U), &transaction);
    const auto* gap_tick = FindTick(transaction);
    context->Expect(
        prepared.exchange_outcome ==
                canonical::SequenceGuardOutcomeV1::kGap &&
            gap_tick != nullptr &&
            (gap_tick->header.quality_flags &
             control::QualityBit(
                 control::QualityFlagV1::kExchangeSequenceGap)) != 0U &&
            FindQuality(
                transaction,
                canonical::CanonicalQualityTypeV1::
                    kExchangeSequenceGap) != nullptr,
        "forward exchange gap accepts event and propagates sticky degraded quality");
    context->Expect(Commit(normalizer.get(), &transaction),
                    "SZ gap plan commits");

    ShenzhenTransactionSpec trade{};
    body = ShenzhenTransaction(trade);
    prepared = normalizer->Prepare(
        Raw(3U, 8U), Message(6U, 36U, body, 3U, 20U), &transaction);
    context->Expect(
        prepared.exchange_outcome ==
                canonical::SequenceGuardOutcomeV1::kContiguous &&
            FindTick(transaction) != nullptr &&
            normalizer->Snapshot().exchange_scope_count == 1U,
        "SZ 6.36 follows 6.33 in one ChannelNo guard across reconnect");
    context->Expect(Commit(normalizer.get(), &transaction),
                    "unified SZ transaction commits");

    order.sequence = 14;
    body = ShenzhenOrder(order);
    prepared = normalizer->Prepare(
        Raw(4U, 12U), Message(6U, 33U, body, 4U, 12U), &transaction);
    context->Expect(
        prepared.exchange_outcome ==
                canonical::SequenceGuardOutcomeV1::kContiguous &&
            normalizer->Snapshot().exchange_scope_count == 1U,
        "connection epoch change does not recreate the SZ business guard");
    context->Expect(Commit(normalizer.get(), &transaction),
                    "post-reconnect SZ order commits");
}

void TestVendorPoisonProvenanceIsTransactional(TestContext* context) {
    auto registry = Registry();
    auto normalizer = Normalizer(registry.get());
    ShanghaiTickSpec spec{};
    spec.type = "S";
    spec.flag = "TRADE";
    std::vector<std::byte> body = ShanghaiTick(spec);
    canonical::CanonicalNormalizationTransactionV1 transaction;
    auto prepared = normalizer->Prepare(
        Raw(1U), Message(4U, 24U, body, 1U, 1U), &transaction);
    context->Expect(prepared.ok() && Commit(normalizer.get(), &transaction),
                    "vendor poison baseline commits");

    market::MarketMessageViewV1 changed =
        Message(4U, 24U, body, 2U, 1U);
    changed.service_version = 102U;
    prepared = normalizer->Prepare(Raw(2U), changed, &transaction);
    const auto* first_conflict = FindQuality(
        transaction,
        canonical::CanonicalQualityTypeV1::kVendorSequenceConflict);
    context->Expect(
        prepared.vendor_outcome ==
                canonical::SequenceGuardOutcomeV1::kConflict &&
            first_conflict != nullptr &&
            first_conflict->payload.first_bad_origin_wal_end_pos == 200U,
        "service-version change conflicts in the same vendor sequence scope");
    context->Expect(
        normalizer->Abort(&transaction),
        "aborting a prepared vendor conflict leaves poison uncommitted");

    changed = Message(4U, 24U, body, 3U, 1U);
    changed.service_version = 102U;
    prepared = normalizer->Prepare(Raw(3U), changed, &transaction);
    const auto* retried_conflict = FindQuality(
        transaction,
        canonical::CanonicalQualityTypeV1::kVendorSequenceConflict);
    context->Expect(
        prepared.vendor_outcome ==
                canonical::SequenceGuardOutcomeV1::kConflict &&
            retried_conflict != nullptr &&
            retried_conflict->payload.first_bad_origin_wal_end_pos == 300U,
        "retry after Abort records the retry WAL as the first committed fault");
    context->Expect(Commit(normalizer.get(), &transaction),
                    "retried vendor conflict commits poison");

    prepared = normalizer->Prepare(
        Raw(4U), Message(4U, 24U, body, 4U, 2U), &transaction);
    const auto* poisoned = FindQuality(
        transaction, canonical::CanonicalQualityTypeV1::kScopePoisoned);
    context->Expect(
        prepared.vendor_outcome ==
                canonical::SequenceGuardOutcomeV1::kPoisoned &&
            poisoned != nullptr &&
            poisoned->payload.first_bad_origin_wal_end_pos == 300U,
        "all later vendor poison diagnostics retain the committed first-bad WAL");
    context->Expect(Commit(normalizer.get(), &transaction),
                    "vendor poisoned progress commits");
}

void TestRejectedSemanticsAndStartUnknown(TestContext* context) {
    auto registry = Registry();
    {
        auto normalizer = Normalizer(registry.get());
        ShanghaiTickSpec negative{};
        negative.quantity = -1;
        const std::vector<std::byte> body = ShanghaiTick(negative);
        canonical::CanonicalNormalizationTransactionV1 transaction;
        const auto prepared = normalizer->Prepare(
            Raw(1U), Message(4U, 24U, body, 1U, 1U), &transaction);
        const auto* rejected = FindQuality(
            transaction,
            canonical::CanonicalQualityTypeV1::kNormalizationRejected);
        context->Expect(
            prepared.ok() && FindTick(transaction) == nullptr &&
                rejected != nullptr &&
                rejected->payload.actual_sequence == 100U &&
                (rejected->header.quality_flags &
                 control::QualityBit(
                     control::QualityFlagV1::kStartUnknown)) != 0U &&
                (rejected->header.quality_flags &
                 control::QualityBit(
                     control::QualityFlagV1::kUnknownEnum)) == 0U,
            "negative native tick quantity is rejected without an enum mislabel");
        context->Expect(Commit(normalizer.get(), &transaction),
                        "tick semantic rejection commits sequence state");
    }
    {
        auto normalizer = Normalizer(registry.get());
        ShenzhenOrderSpec invalid{};
        invalid.sequence = 0;
        const std::vector<std::byte> body = ShenzhenOrder(invalid);
        canonical::CanonicalNormalizationTransactionV1 transaction;
        const auto prepared = normalizer->Prepare(
            Raw(1U), Message(6U, 33U, body, 1U, 1U), &transaction);
        const auto* rejected = FindQuality(
            transaction,
            canonical::CanonicalQualityTypeV1::kNormalizationRejected);
        context->Expect(
            prepared.ok() && rejected != nullptr &&
                rejected->payload.actual_sequence == 0U &&
                rejected->payload.scope_type ==
                    canonical::CanonicalQualityScopeV1::kChannel,
            "invalid channel sequence never substitutes vendor SequenceID");
        context->Expect(Commit(normalizer.get(), &transaction),
                        "invalid channel sequence diagnostic commits vendor progress");
    }
    {
        auto normalizer = Normalizer(registry.get());
        ShenzhenOrderSpec wide_raw{};
        // Low 16 bits equal ASCII '1'/Buy, but the original int32 is not
        // representable by Canonical's u16 source-enum slot.
        wide_raw.raw_side = 0x10031;
        const std::vector<std::byte> body = ShenzhenOrder(wide_raw);
        canonical::CanonicalNormalizationTransactionV1 transaction;
        const auto prepared = normalizer->Prepare(
            Raw(1U), Message(6U, 33U, body, 1U, 1U), &transaction);
        const auto* tick = FindTick(transaction);
        context->Expect(
            prepared.ok() && tick != nullptr &&
                (tick->payload.source_enum_bits & 0xffffU) == 0U &&
                tick->payload.side == canonical::CanonicalSideV1::kUnknown &&
                (tick->header.quality_flags &
                 control::QualityBit(
                     control::QualityFlagV1::kUnknownEnum)) != 0U,
            "unrepresentable int32 raw enum uses zero sentinel instead of colliding with its low 16 bits");
        context->Expect(Commit(normalizer.get(), &transaction),
                        "wide raw-enum diagnostic tick commits");
    }
    {
        auto normalizer = Normalizer(registry.get());
        ShanghaiTickSpec unknown{};
        unknown.security_id = "999999";
        const std::vector<std::byte> body = ShanghaiTick(unknown);
        canonical::CanonicalNormalizationTransactionV1 transaction;
        const auto prepared = normalizer->Prepare(
            Raw(1U), Message(4U, 24U, body, 1U, 1U), &transaction);
        const auto* quality = FindQuality(
            transaction,
            canonical::CanonicalQualityTypeV1::kInstrumentUnknown);
        context->Expect(
            prepared.ok() && quality != nullptr &&
                (quality->header.quality_flags &
                 control::QualityBit(
                     control::QualityFlagV1::kStartUnknown)) != 0U,
            "first unknown-instrument Quality carries unknown-prefix state");
        context->Expect(Commit(normalizer.get(), &transaction),
                        "unknown instrument diagnostic commits");
    }
    {
        auto normalizer = Normalizer(registry.get());
        const std::array<std::byte, 1U> truncated{std::byte{0U}};
        canonical::CanonicalNormalizationTransactionV1 transaction;
        const auto prepared = normalizer->Prepare(
            Raw(1U), Message(4U, 24U, truncated, 1U, 1U), &transaction);
        const auto* quality = FindQuality(
            transaction, canonical::CanonicalQualityTypeV1::kDecodeError);
        context->Expect(
            prepared.error ==
                    canonical::CanonicalNormalizePrepareErrorV1::
                        kDecodeFailureRecorded &&
                quality != nullptr &&
                (quality->header.quality_flags &
                 control::QualityBit(
                     control::QualityFlagV1::kStartUnknown)) != 0U,
            "first decode-error Quality carries unknown-prefix state");
        context->Expect(Commit(normalizer.get(), &transaction),
                        "decode-error diagnostic commits");
    }
}

void TestPerScopeExpectedFirstPolicy(TestContext* context) {
    auto registry = Registry();
    canonical::CanonicalNormalizerConfigV1 invalid =
        NormalizerConfig(registry.get());
    invalid.sequence_policy.expected_first_by_scope.push_back({
        canonical::MakeVendorSequenceScopeV1(
            kCaptureDate, kSource, invalid.stream_day_id, 4U, 24U),
        10U});
    context->Expect(
        NormalizerFromConfig(invalid) == nullptr,
        "AllowUnknown rejects contradictory expected-first entries");

    invalid = NormalizerConfig(registry.get());
    invalid.sequence_policy.vendor_initial =
        canonical::InitialSequencePolicyV1::kRequireConfiguredFirst;
    context->Expect(
        NormalizerFromConfig(invalid) == nullptr,
        "required-first policy rejects an empty vendor scope table");

    canonical::CanonicalNormalizerConfigV1 configured =
        NormalizerConfig(registry.get());
    configured.sequence_policy.vendor_initial =
        canonical::InitialSequencePolicyV1::kRequireConfiguredFirst;
    configured.sequence_policy.exchange_initial =
        canonical::InitialSequencePolicyV1::kRequireConfiguredFirst;
    configured.sequence_policy.expected_first_by_scope = {
        {canonical::MakeVendorSequenceScopeV1(
             kCaptureDate, kSource, configured.stream_day_id, 4U, 24U),
         10U},
        {canonical::MakeShanghaiChannelScopeV1(
             kTradeDate, kSource, 7U),
         100U},
    };
    auto normalizer = NormalizerFromConfig(configured);
    context->Expect(normalizer != nullptr,
                    "exact per-scope expected-first table is accepted");
    if (normalizer != nullptr) {
        ShanghaiTickSpec status{};
        status.type = "S";
        status.flag = "TRADE";
        const std::vector<std::byte> body = ShanghaiTick(status);
        canonical::CanonicalNormalizationTransactionV1 transaction;
        const auto prepared = normalizer->Prepare(
            Raw(1U), Message(4U, 24U, body, 1U, 10U), &transaction);
        const auto* tick = FindTick(transaction);
        context->Expect(
            prepared.vendor_outcome ==
                    canonical::SequenceGuardOutcomeV1::kFirst &&
                prepared.exchange_outcome ==
                    canonical::SequenceGuardOutcomeV1::kFirst &&
                tick != nullptr &&
                (tick->header.quality_flags &
                 control::QualityBit(
                     control::QualityFlagV1::kStartUnknown)) == 0U,
            "authoritative first values avoid false START_UNKNOWN");
        context->Expect(Commit(normalizer.get(), &transaction),
                        "configured first values commit");
    }

    normalizer = NormalizerFromConfig(configured);
    context->Expect(normalizer != nullptr,
                    "configured backward probe creates a fresh normalizer");
    if (normalizer != nullptr) {
        ShanghaiTickSpec early{};
        early.sequence = 99;
        early.type = "S";
        early.flag = "TRADE";
        const std::vector<std::byte> body = ShanghaiTick(early);
        canonical::CanonicalNormalizationTransactionV1 transaction;
        const auto prepared = normalizer->Prepare(
            Raw(1U), Message(4U, 24U, body, 1U, 10U), &transaction);
        const auto* quality = FindQuality(
            transaction,
            canonical::CanonicalQualityTypeV1::
                kExchangeSequenceBackward);
        context->Expect(
            prepared.ok() && transaction.active() &&
                prepared.exchange_outcome ==
                    canonical::SequenceGuardOutcomeV1::kBackward &&
                quality != nullptr &&
                quality->payload.expected_sequence == 100U &&
                quality->payload.actual_sequence == 99U,
            "first observation below configured exchange start emits the exact expected/actual backward diagnostic");
        context->Expect(Commit(normalizer.get(), &transaction),
                        "configured initial backward diagnostic commits and poisons its scope");
    }

    canonical::CanonicalNormalizerConfigV1 missing_exchange =
        NormalizerConfig(registry.get());
    missing_exchange.sequence_policy.vendor_initial =
        canonical::InitialSequencePolicyV1::kRequireConfiguredFirst;
    missing_exchange.sequence_policy.exchange_initial =
        canonical::InitialSequencePolicyV1::kRequireConfiguredFirst;
    missing_exchange.sequence_policy.expected_first_by_scope = {
        {canonical::MakeVendorSequenceScopeV1(
             kCaptureDate, kSource, missing_exchange.stream_day_id,
             6U, 33U),
         1U},
        {canonical::MakeShanghaiChannelScopeV1(
             kTradeDate, kSource, 7U),
         100U},
    };
    normalizer = NormalizerFromConfig(missing_exchange);
    context->Expect(normalizer != nullptr,
                    "policy may enumerate only authoritative known scopes");
    if (normalizer != nullptr) {
        const std::vector<std::byte> body = ShenzhenOrder({});
        canonical::CanonicalNormalizationTransactionV1 transaction;
        const auto prepared = normalizer->Prepare(
            Raw(1U), Message(6U, 33U, body, 1U, 1U), &transaction);
        const auto snapshot = normalizer->Snapshot();
        context->Expect(
            prepared.error ==
                    canonical::CanonicalNormalizePrepareErrorV1::
                        kSequencePolicyMissing &&
                !transaction.active() && snapshot.version == 0U &&
                snapshot.vendor_scope_count == 0U &&
                snapshot.exchange_scope_count == 0U,
            "missing channel policy rolls back the prepared vendor scope completely");
    }

    normalizer = NormalizerFromConfig(missing_exchange);
    context->Expect(normalizer != nullptr,
                    "zero-sequence bypass probe creates a fresh normalizer");
    if (normalizer != nullptr) {
        ShenzhenOrderSpec zero_sequence{};
        zero_sequence.sequence = 0;
        const std::vector<std::byte> body = ShenzhenOrder(zero_sequence);
        canonical::CanonicalNormalizationTransactionV1 transaction;
        const auto prepared = normalizer->Prepare(
            Raw(1U), Message(6U, 33U, body, 1U, 1U), &transaction);
        const auto snapshot = normalizer->Snapshot();
        context->Expect(
            prepared.error ==
                    canonical::CanonicalNormalizePrepareErrorV1::
                        kSequencePolicyMissing &&
                !transaction.active() && snapshot.version == 0U &&
                snapshot.vendor_scope_count == 0U &&
                snapshot.exchange_scope_count == 0U,
            "zero business sequence cannot bypass an exact RequireConfiguredFirst scope entry");
    }
}

void TestSchemaUnknownQualityProjection(TestContext* context) {
    const std::array<std::byte, 1U> body{std::byte{0U}};

    {
        auto registry = Registry();
        auto normalizer = Normalizer(registry.get());
        canonical::CanonicalNormalizationTransactionV1 transaction;
        const auto prepared = normalizer->Prepare(
            Raw(1U), Message(4U, 25U, body, 1U, 1U), &transaction);
        const auto* quality = FindQuality(
            transaction, canonical::CanonicalQualityTypeV1::kSchemaUnknown);
        context->Expect(
            prepared.error ==
                    canonical::CanonicalNormalizePrepareErrorV1::
                        kDecodeFailureRecorded &&
                quality != nullptr && quality->payload.detail_code == 4U &&
                (quality->header.quality_flags &
                 control::QualityBit(
                     control::QualityFlagV1::kSchemaUnknown)) != 0U &&
                canonical::ValidateCanonicalQualityRecordV1(*quality) ==
                    canonical::CanonicalValidationErrorV1::kNone,
            "unsupported message emits schema-unknown quality with its required frozen bit");
        context->Expect(Commit(normalizer.get(), &transaction),
                        "unsupported-message schema diagnostic commits");
    }

    {
        auto registry = Registry();
        auto normalizer = Normalizer(registry.get());
        auto message = Message(4U, 24U, body, 1U, 1U);
        message.service_version = 102U;
        canonical::CanonicalNormalizationTransactionV1 transaction;
        const auto prepared = normalizer->Prepare(
            Raw(1U), message, &transaction);
        const auto* quality = FindQuality(
            transaction, canonical::CanonicalQualityTypeV1::kSchemaUnknown);
        context->Expect(
            prepared.error ==
                    canonical::CanonicalNormalizePrepareErrorV1::
                        kDecodeFailureRecorded &&
                quality != nullptr && quality->payload.detail_code == 5U &&
                (quality->header.quality_flags &
                 control::QualityBit(
                     control::QualityFlagV1::kSchemaUnknown)) != 0U &&
                canonical::ValidateCanonicalQualityRecordV1(*quality) ==
                    canonical::CanonicalValidationErrorV1::kNone,
            "unsupported service version emits schema-unknown quality with exact origin semantics");
        context->Expect(Commit(normalizer.get(), &transaction),
                        "unsupported-version schema diagnostic commits");
    }
}

void TestSnapshotNativeQuantityConversion(TestContext* context) {
    auto registry = Registry();
    auto normalizer = Normalizer(registry.get());
    canonical::CanonicalNormalizationTransactionV1 transaction;
    std::vector<std::byte> body = ShanghaiSnapshot(1U);
    auto prepared = normalizer->Prepare(
        Raw(1U), Message(4U, 4U, body, 1U, 1U), &transaction);
    const auto* snapshot = FindSnapshot(transaction);
    context->Expect(
        prepared.ok() && snapshot != nullptr &&
            snapshot->payload.volume_native == 123 &&
            snapshot->payload.bid_quantity_native[0] == 1 &&
            snapshot->payload.ask_quantity_native[0] == 2 &&
            snapshot->payload.bid1_queue_quantity_native[0] == 1,
        "SH p3 quantities convert exactly to integral native units");
    context->Expect(Commit(normalizer.get(), &transaction),
                    "integral snapshot commits");

    body = ShanghaiSnapshot(1U, 123001);
    prepared = normalizer->Prepare(
        Raw(2U), Message(4U, 4U, body, 2U, 2U), &transaction);
    context->Expect(
        prepared.ok() && FindSnapshot(transaction) == nullptr &&
            FindQuality(
                transaction,
                canonical::CanonicalQualityTypeV1::kSnapshotRejected) !=
                nullptr,
        "non-integral p3 snapshot quantity is rejected rather than rounded");
    context->Expect(Commit(normalizer.get(), &transaction),
                    "non-integral snapshot rejection commits");

    body = ShanghaiSnapshot(1U, -1000);
    prepared = normalizer->Prepare(
        Raw(3U), Message(4U, 4U, body, 3U, 3U), &transaction);
    context->Expect(
        prepared.ok() && FindSnapshot(transaction) == nullptr &&
            FindQuality(
                transaction,
                canonical::CanonicalQualityTypeV1::kSnapshotRejected) !=
                nullptr,
        "negative snapshot quantity is rejected");
    context->Expect(Commit(normalizer.get(), &transaction),
                    "negative snapshot rejection commits");
}

void TestSnapshotQueueRejectionAndShard(TestContext* context) {
    auto registry = Registry();
    auto normalizer = Normalizer(registry.get());
    std::vector<std::byte> body = ShanghaiSnapshot(51U);
    canonical::CanonicalNormalizationTransactionV1 transaction;
    auto prepared = normalizer->Prepare(
        Raw(1U), Message(4U, 4U, body, 1U, 1U), &transaction);
    context->Expect(
        prepared.ok() && !prepared.business_record_planned &&
            FindQuality(
                transaction,
                canonical::CanonicalQualityTypeV1::kSnapshotRejected) !=
                nullptr,
        "revealed queue count greater than 50 rejects the atomic snapshot");
    context->Expect(Commit(normalizer.get(), &transaction),
                    "snapshot rejection still advances vendor sequence");

    ShanghaiTickSpec tick{};
    body = ShanghaiTick(tick);
    prepared = normalizer->Prepare(
        Raw(2U), Message(4U, 24U, body, 2U, 1U), &transaction);
    const auto records = transaction.records();
    bool routed_to_expected_shard = false;
    for (const auto& routed : records) {
        if (std::holds_alternative<canonical::CanonicalTickRecordV1>(
                routed.record)) {
            routed_to_expected_shard = routed.shard == (33U % 16U);
        }
    }
    context->Expect(
        prepared.ok() && routed_to_expected_shard,
        "market routing is exactly instrument_id modulo shard_count");
    context->Expect(Commit(normalizer.get(), &transaction),
                    "routed tick commits");
}

void TestSingleDecodeSeams(TestContext* context) {
    auto registry = Registry();
    auto normalizer = Normalizer(registry.get());
    market::MarketDecoderConfigV1 decoder_config{};
    decoder_config.trade_date = kTradeDate;
    decoder_config.source_stream_id = kSource;
    decoder_config.instrument_registry = registry.get();
    decoder_config.shanghai_phase_attribution =
        market::ShanghaiPhaseAttributionModeV1::kDeferred;
    market::MarketDecoderV1 decoder(decoder_config);

    ShanghaiTickSpec status{};
    status.type = "S";
    status.flag = "TRADE";
    std::vector<std::byte> body = ShanghaiTick(status);
    auto message = Message(4U, 24U, body, 1U, 1U);
    market::DecodedMarketEventV1 decoded;
    market::RetainedMarketEventV1 retained;
    context->Expect(
        decoder.Decode(message, &decoded) ==
                market::MarketDecodeErrorV1::kNone &&
            market::RetainMarketEventV1(
                std::move(decoded), &retained) ==
                market::RetainedMarketEventCreateErrorV1::kNone,
        "source-order deferred decoder retains SH status once");
    canonical::CanonicalNormalizationTransactionV1 transaction;
    auto prepared = normalizer->PrepareDecoded(
        Raw(1U), message, retained, &transaction);
    context->Expect(
        prepared.ok() && FindTick(transaction) != nullptr &&
            Commit(normalizer.get(), &transaction),
        "retained SH status canonicalizes without decoder replay");

    ShanghaiTickSpec add{};
    add.sequence = 101;
    body = ShanghaiTick(add);
    message = Message(4U, 24U, body, 2U, 2U);
    context->Expect(
        decoder.Decode(message, &decoded) ==
                market::MarketDecodeErrorV1::kNone &&
            market::RetainMarketEventV1(
                std::move(decoded), &retained) ==
                market::RetainedMarketEventCreateErrorV1::kNone,
        "source-order deferred decoder retains next SH tick once");
    prepared = normalizer->PrepareDecoded(
        Raw(2U), message, retained, &transaction);
    const auto* attributed = FindTick(transaction);
    context->Expect(
        prepared.ok() && attributed != nullptr &&
            attributed->payload.phase ==
                canonical::CanonicalTradingPhaseV1::kContinuous &&
            Commit(normalizer.get(), &transaction),
        "canonical transactional phase state attributes borrowed tick");

    const auto before_mismatch = normalizer->Snapshot();
    auto mismatched_message = Message(4U, 24U, body, 3U, 3U);
    prepared = normalizer->PrepareDecoded(
        Raw(3U), mismatched_message, retained, &transaction);
    context->Expect(
        prepared.error == canonical::CanonicalNormalizePrepareErrorV1::
                              kInvalidDecodedEvent &&
            !transaction.active() &&
            normalizer->Snapshot().version == before_mismatch.version &&
            !normalizer->Snapshot().fatal,
        "retained event origin mismatch fails closed without poisoning state");

    body.assign(1U, std::byte{0U});
    message = Message(4U, 24U, body, 3U, 3U);
    prepared = normalizer->PrepareDecodedFailure(
        Raw(3U),
        message,
        market::MarketDecodeErrorV1::kTruncated,
        &transaction);
    context->Expect(
        prepared.error == canonical::CanonicalNormalizePrepareErrorV1::
                              kDecodeFailureRecorded &&
            prepared.decode_error ==
                market::MarketDecodeErrorV1::kTruncated &&
            FindQuality(
                transaction,
                canonical::CanonicalQualityTypeV1::kDecodeError) !=
                nullptr &&
            Commit(normalizer.get(), &transaction),
        "external one-pass truncated result reuses durable decode-quality plan");

    prepared = normalizer->PrepareDecodedFailure(
        Raw(4U),
        Message(4U, 24U, body, 4U, 4U),
        market::MarketDecodeErrorV1::kUnsupportedMessage,
        &transaction);
    context->Expect(
        prepared.error == canonical::CanonicalNormalizePrepareErrorV1::
                              kInvalidDecodedFailure &&
            !transaction.active() && !normalizer->Snapshot().fatal,
        "unsupported outcome cannot enter decoded-failure seam");
}

}  // namespace

int main() {
    TestContext context;
    TestTransactionalPhaseAndOrigin(&context);
    TestDuplicateConflictAndPoison(&context);
    TestGapUnifiedSzAndReconnect(&context);
    TestVendorPoisonProvenanceIsTransactional(&context);
    TestRejectedSemanticsAndStartUnknown(&context);
    TestPerScopeExpectedFirstPolicy(&context);
    TestSchemaUnknownQualityProjection(&context);
    TestSnapshotNativeQuantityConversion(&context);
    TestSnapshotQueueRejectionAndShard(&context);
    TestSingleDecodeSeams(&context);
    if (context.failures != 0) {
        std::cerr << context.failures
                  << " Phase-5 canonical normalizer assertion(s) failed\n";
        return 1;
    }
    std::cout << "phase5 canonical normalizer tests passed\n";
    return 0;
}
