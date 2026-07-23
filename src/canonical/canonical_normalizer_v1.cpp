#include "l2flow/canonical/canonical_normalizer_v1.h"

#include "l2flow/canonical/canonical_control_adapter_v1.h"
#include "l2flow/canonical/market_sequence_evidence_v1.h"
#include "l2flow/control/quality_flags_v1.h"
#include "l2flow/market/instrument_registry.h"

#include <algorithm>
#include <array>
#include <bit>
#include <compare>
#include <limits>
#include <map>
#include <new>
#include <stdexcept>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace l2flow::canonical {
namespace {

using l2flow::control::QualityBit;
using l2flow::control::QualityFlagV1;
using l2flow::market::AggressorV1;
using l2flow::market::DecodedMarketCommonV1;
using l2flow::market::DecodedMarketEventV1;
using l2flow::market::LimitPriceSemanticsV1;
using l2flow::market::MarketDecodeErrorV1;
using l2flow::market::MarketEventKindV1;
using l2flow::market::MarketMessageViewV1;
using l2flow::market::MarketV1;
using l2flow::market::OrderTypeV1;
using l2flow::market::QuantityUnitV1;
using l2flow::market::RetainedMarketEventV1;
using l2flow::market::ShanghaiSnapshotV1;
using l2flow::market::ShanghaiTickV1;
using l2flow::market::ShenzhenOrderV1;
using l2flow::market::ShenzhenSnapshotV1;
using l2flow::market::ShenzhenTransactionV1;
using l2flow::market::SideV1;
using l2flow::market::SnapshotBookV1;
using l2flow::market::TickActionV1;
using l2flow::market::TickFieldsV1;
using l2flow::market::TradingPhaseV1;

// A retained event already owns all callback-scoped data.  Canonical
// normalization borrows the exact concrete object so the production seam
// neither decodes twice nor copies the multi-KiB snapshot alternative.
using BorrowedDecodedMarketEventV1 = std::variant<
    const ShanghaiSnapshotV1*,
    const ShanghaiTickV1*,
    const ShenzhenSnapshotV1*,
    const ShenzhenOrderV1*,
    const ShenzhenTransactionV1*>;

BorrowedDecodedMarketEventV1 BorrowDecodedMarketEvent(
    const DecodedMarketEventV1& event) noexcept {
    return std::visit(
        [](const auto& value) -> BorrowedDecodedMarketEventV1 {
            return &value;
        },
        event);
}

bool BorrowRetainedMarketEvent(
    const RetainedMarketEventV1& event,
    BorrowedDecodedMarketEventV1* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    return std::visit(
        [&](const auto& owner) noexcept {
            if (owner == nullptr) {
                return false;
            }
            *output = owner.get();
            return true;
        },
        event);
}

const DecodedMarketCommonV1& BorrowedMarketCommon(
    const BorrowedDecodedMarketEventV1& event) noexcept {
    return std::visit(
        [](const auto* value) noexcept
            -> const DecodedMarketCommonV1& {
            return value->common;
        },
        event);
}

bool SameDecodedOrigin(
    const MarketMessageViewV1& retained,
    const MarketMessageViewV1& authenticated) noexcept {
    return retained.source_stream_id == authenticated.source_stream_id &&
           retained.trade_date == authenticated.trade_date &&
           retained.source_sequence == authenticated.source_sequence &&
           retained.service_id == authenticated.service_id &&
           retained.service_version == authenticated.service_version &&
           retained.message_id == authenticated.message_id &&
           retained.message_encoding == authenticated.message_encoding &&
           retained.vendor_local_time_raw ==
               authenticated.vendor_local_time_raw &&
           retained.vendor_sequence_id ==
               authenticated.vendor_sequence_id &&
           retained.recv_realtime_ns == authenticated.recv_realtime_ns &&
           retained.recv_monotonic_ns ==
               authenticated.recv_monotonic_ns &&
           retained.body.empty();
}

bool BorrowedEventMatchesMessage(
    const BorrowedDecodedMarketEventV1& event,
    const MarketMessageViewV1& message) noexcept {
    if (!SameDecodedOrigin(BorrowedMarketCommon(event).origin, message)) {
        return false;
    }
    return std::visit(
        [&](const auto* value) noexcept {
            using Event = std::remove_cv_t<
                std::remove_pointer_t<decltype(value)>>;
            const DecodedMarketCommonV1& common = value->common;
            if constexpr (std::is_same_v<Event, ShanghaiSnapshotV1>) {
                return message.service_id == 4U &&
                       message.service_version == 101U &&
                       message.message_id == 4U &&
                       common.kind ==
                           MarketEventKindV1::kShanghaiSnapshot &&
                       common.market == MarketV1::kShanghai &&
                       !common.sh_phase_attribution_deferred;
            } else if constexpr (std::is_same_v<Event, ShanghaiTickV1>) {
                return message.service_id == 4U &&
                       message.service_version == 101U &&
                       message.message_id == 24U &&
                       common.kind == MarketEventKindV1::kShanghaiTick &&
                       common.market == MarketV1::kShanghai &&
                       common.sh_phase_attribution_deferred;
            } else if constexpr (
                std::is_same_v<Event, ShenzhenSnapshotV1>) {
                return message.service_id == 6U &&
                       message.service_version == 101U &&
                       message.message_id == 28U &&
                       common.kind ==
                           MarketEventKindV1::kShenzhenSnapshot &&
                       common.market == MarketV1::kShenzhen &&
                       !common.sh_phase_attribution_deferred;
            } else if constexpr (
                std::is_same_v<Event, ShenzhenOrderV1>) {
                return message.service_id == 6U &&
                       message.service_version == 101U &&
                       message.message_id == 33U &&
                       common.kind == MarketEventKindV1::kShenzhenOrder &&
                       common.market == MarketV1::kShenzhen &&
                       !common.sh_phase_attribution_deferred;
            } else {
                return message.service_id == 6U &&
                       message.service_version == 101U &&
                       message.message_id == 36U &&
                       common.kind ==
                           MarketEventKindV1::kShenzhenTransaction &&
                       common.market == MarketV1::kShenzhen &&
                       !common.sh_phase_attribution_deferred;
            }
        },
        event);
}

bool RecordableExternalDecodeFailure(
    MarketDecodeErrorV1 error) noexcept {
    switch (error) {
        case MarketDecodeErrorV1::kInvalidInput:
        case MarketDecodeErrorV1::kTruncated:
        case MarketDecodeErrorV1::kOffsetInvalid:
        case MarketDecodeErrorV1::kRangeOverlap:
        case MarketDecodeErrorV1::kCountExceeded:
        case MarketDecodeErrorV1::kCountMismatch:
        case MarketDecodeErrorV1::kTextInvalid:
        case MarketDecodeErrorV1::kFixedPointOverflow:
            return true;
        case MarketDecodeErrorV1::kNone:
        case MarketDecodeErrorV1::kNullOutput:
        case MarketDecodeErrorV1::kInvalidConfiguration:
        case MarketDecodeErrorV1::kUnsupportedMessage:
        case MarketDecodeErrorV1::kUnsupportedServiceVersion:
        case MarketDecodeErrorV1::kPhaseProductLimitExceeded:
        case MarketDecodeErrorV1::kResourceExhausted:
        case MarketDecodeErrorV1::kUnexpectedFailure:
            return false;
    }
    return false;
}

bool RecognizedCoreMessageShape(
    const MarketMessageViewV1& message) noexcept {
    return message.service_version == 101U &&
           ((message.service_id == 4U &&
             (message.message_id == 4U || message.message_id == 24U)) ||
            (message.service_id == 6U &&
             (message.message_id == 28U || message.message_id == 33U ||
              message.message_id == 36U)));
}

constexpr std::string_view kBundleHashDomainV1 =
    "L2FLOW_PHASE5_CANONICAL_PUBLICATION_BUNDLE_V1";

bool InitialPolicyValid(
    InitialSequencePolicyV1 mode,
    std::size_t configured_scope_count) noexcept {
    switch (mode) {
        case InitialSequencePolicyV1::kAllowUnknownFirst:
            return configured_scope_count == 0U;
        case InitialSequencePolicyV1::kRequireConfiguredFirst:
            return configured_scope_count != 0U;
    }
    return false;
}

bool ConfigShapeValid(const CanonicalNormalizerConfigV1& config) noexcept {
    if (!CanonicalHostIsLittleEndianV1() ||
        config.capture_date == 0U || config.trade_date == 0U ||
        config.source_stream_id == 0U ||
        l2flow::common::IsZeroIdentity(config.stream_day_id) ||
        config.shard_count == 0U || config.instrument_registry == nullptr ||
        config.sequence_policy.policy_version == 0U ||
        config.maximum_sequence_scopes == 0U ||
        config.maximum_seen_entries_per_scope == 0U ||
        config.maximum_seen_payload_bytes_per_scope == 0U ||
        config.maximum_phase_products == 0U) {
        return false;
    }
    std::size_t vendor_entries = 0U;
    std::size_t exchange_entries = 0U;
    const auto& entries = config.sequence_policy.expected_first_by_scope;
    for (std::size_t index = 0U; index < entries.size(); ++index) {
        const auto& entry = entries[index];
        const auto& scope = entry.scope;
        if (scope.source_stream_id != config.source_stream_id) {
            return false;
        }
        if (scope.kind == SequenceScopeKindV1::kVendorMessage) {
            if (scope.date != config.capture_date ||
                scope.stream_day_id != config.stream_day_id ||
                scope.channel != 0U || scope.service_id == 0U ||
                scope.message_id == 0U || entry.sequence == 0U) {
                return false;
            }
            ++vendor_entries;
        } else if (
            scope.kind == SequenceScopeKindV1::kShanghaiChannel ||
            scope.kind ==
                SequenceScopeKindV1::kShenzhenUnifiedChannel) {
            if (scope.date != config.trade_date ||
                !l2flow::common::IsZeroIdentity(scope.stream_day_id) ||
                scope.service_id != 0U || scope.message_id != 0U ||
                entry.sequence == 0U) {
                return false;
            }
            ++exchange_entries;
        } else {
            return false;
        }
        for (std::size_t prior = 0U; prior < index; ++prior) {
            if (entries[prior].scope == scope) {
                return false;
            }
        }
    }
    return InitialPolicyValid(
               config.sequence_policy.vendor_initial,
               vendor_entries) &&
           InitialPolicyValid(
               config.sequence_policy.exchange_initial,
               exchange_entries);
}

bool RawContextValid(
    const CanonicalNormalizerConfigV1& config,
    const CanonicalRawContextV1& raw) noexcept {
    return raw.capture_date == config.capture_date &&
           raw.trade_date == config.trade_date &&
           raw.source_stream_id == config.source_stream_id &&
           raw.stream_day_id == config.stream_day_id &&
           !l2flow::common::IsZeroIdentity(
               raw.source_writer_instance) &&
           raw.source_generation != 0U &&
           raw.origin_ingress_sequence != 0U &&
           raw.origin_wal_end_pos != 0U &&
           ClockEpochIdentityV1Valid(raw.clock_epoch) &&
           (raw.upstream_quality_flags &
            ~kCanonicalQualityFlagsMaskV1) == 0U;
}

bool MessageViewValid(
    const CanonicalNormalizerConfigV1& config,
    const CanonicalRawContextV1& raw,
    const MarketMessageViewV1& message) noexcept {
    return message.source_stream_id == config.source_stream_id &&
           message.trade_date == config.trade_date &&
           message.source_sequence == raw.origin_ingress_sequence &&
           message.service_id != 0U &&
           message.service_version != 0U &&
           message.message_id != 0U &&
           message.vendor_sequence_id != 0U &&
           message.recv_realtime_ns >= 0 &&
           message.recv_monotonic_ns >= 0 &&
           (message.body.empty() || message.body.data() != nullptr);
}

bool SequenceAccepted(SequenceGuardOutcomeV1 outcome) noexcept {
    return outcome == SequenceGuardOutcomeV1::kFirst ||
           outcome == SequenceGuardOutcomeV1::kContiguous ||
           outcome == SequenceGuardOutcomeV1::kGap;
}

std::uint64_t OutcomeQualityBit(
    bool vendor,
    SequenceGuardOutcomeV1 outcome) noexcept {
    if (vendor) {
        switch (outcome) {
            case SequenceGuardOutcomeV1::kGap:
                return QualityBit(QualityFlagV1::kVendorSequenceGap);
            case SequenceGuardOutcomeV1::kExactDuplicate:
                return QualityBit(
                    QualityFlagV1::kVendorSequenceDuplicate);
            case SequenceGuardOutcomeV1::kConflict:
            case SequenceGuardOutcomeV1::kBackward:
            case SequenceGuardOutcomeV1::kPoisoned:
            case SequenceGuardOutcomeV1::kCapacity:
                return QualityBit(
                    QualityFlagV1::kVendorSequenceConflict);
            case SequenceGuardOutcomeV1::kFirst:
            case SequenceGuardOutcomeV1::kContiguous:
                return 0U;
        }
    }
    switch (outcome) {
        case SequenceGuardOutcomeV1::kGap:
            return QualityBit(QualityFlagV1::kExchangeSequenceGap);
        case SequenceGuardOutcomeV1::kBackward:
            return QualityBit(
                QualityFlagV1::kExchangeSequenceBackward);
        case SequenceGuardOutcomeV1::kConflict:
        case SequenceGuardOutcomeV1::kPoisoned:
        case SequenceGuardOutcomeV1::kCapacity:
            return QualityBit(
                QualityFlagV1::kExchangeSequenceConflict);
        case SequenceGuardOutcomeV1::kExactDuplicate:
        case SequenceGuardOutcomeV1::kFirst:
        case SequenceGuardOutcomeV1::kContiguous:
            return 0U;
    }
    return 0U;
}

CanonicalQualityTypeV1 OutcomeQualityType(
    bool vendor,
    SequenceGuardOutcomeV1 outcome) noexcept {
    if (outcome == SequenceGuardOutcomeV1::kCapacity) {
        return CanonicalQualityTypeV1::kSequenceCapacityExhausted;
    }
    if (outcome == SequenceGuardOutcomeV1::kPoisoned) {
        return CanonicalQualityTypeV1::kScopePoisoned;
    }
    if (vendor) {
        if (outcome == SequenceGuardOutcomeV1::kGap) {
            return CanonicalQualityTypeV1::kVendorSequenceGap;
        }
        if (outcome == SequenceGuardOutcomeV1::kExactDuplicate) {
            return CanonicalQualityTypeV1::kVendorSequenceDuplicate;
        }
        return CanonicalQualityTypeV1::kVendorSequenceConflict;
    }
    if (outcome == SequenceGuardOutcomeV1::kGap) {
        return CanonicalQualityTypeV1::kExchangeSequenceGap;
    }
    if (outcome == SequenceGuardOutcomeV1::kBackward) {
        return CanonicalQualityTypeV1::kExchangeSequenceBackward;
    }
    if (outcome == SequenceGuardOutcomeV1::kExactDuplicate) {
        // The frozen Phase-3 bitmap has no exchange-duplicate bit.  The
        // Phase-5 typed quality record carries the unambiguous diagnosis.
        return CanonicalQualityTypeV1::kExchangeSequenceDuplicate;
    }
    return CanonicalQualityTypeV1::kExchangeSequenceConflict;
}

CanonicalMarketV1 ToCanonicalMarket(MarketV1 value) noexcept {
    switch (value) {
        case MarketV1::kShanghai:
            return CanonicalMarketV1::kShanghai;
        case MarketV1::kShenzhen:
            return CanonicalMarketV1::kShenzhen;
        case MarketV1::kUnknown:
            return CanonicalMarketV1::kUnknown;
    }
    return CanonicalMarketV1::kUnknown;
}

CanonicalTickActionV1 ToCanonicalAction(TickActionV1 value) noexcept {
    switch (value) {
        case TickActionV1::kAdd:
            return CanonicalTickActionV1::kAdd;
        case TickActionV1::kCancel:
            return CanonicalTickActionV1::kCancel;
        case TickActionV1::kTrade:
            return CanonicalTickActionV1::kTrade;
        case TickActionV1::kStatus:
            return CanonicalTickActionV1::kStatus;
        case TickActionV1::kUnknown:
            return CanonicalTickActionV1::kUnknown;
    }
    return CanonicalTickActionV1::kUnknown;
}

CanonicalSideV1 ToCanonicalSide(SideV1 value) noexcept {
    switch (value) {
        case SideV1::kBuy:
            return CanonicalSideV1::kBuy;
        case SideV1::kSell:
            return CanonicalSideV1::kSell;
        case SideV1::kBorrow:
            return CanonicalSideV1::kBorrow;
        case SideV1::kLend:
            return CanonicalSideV1::kLend;
        case SideV1::kUnknown:
            return CanonicalSideV1::kUnknown;
    }
    return CanonicalSideV1::kUnknown;
}

CanonicalOrderTypeV1 ToCanonicalOrderType(OrderTypeV1 value) noexcept {
    switch (value) {
        case OrderTypeV1::kMarket:
            return CanonicalOrderTypeV1::kMarket;
        case OrderTypeV1::kLimit:
            return CanonicalOrderTypeV1::kLimit;
        case OrderTypeV1::kSameSideBest:
            return CanonicalOrderTypeV1::kSameSideBest;
        case OrderTypeV1::kUnknown:
            return CanonicalOrderTypeV1::kUnknown;
    }
    return CanonicalOrderTypeV1::kUnknown;
}

CanonicalAggressorV1 ToCanonicalAggressor(AggressorV1 value) noexcept {
    switch (value) {
        case AggressorV1::kBuy:
            return CanonicalAggressorV1::kBuy;
        case AggressorV1::kSell:
            return CanonicalAggressorV1::kSell;
        case AggressorV1::kNeutral:
            return CanonicalAggressorV1::kNeutral;
        case AggressorV1::kUnknown:
            return CanonicalAggressorV1::kUnknown;
    }
    return CanonicalAggressorV1::kUnknown;
}

CanonicalQuantityUnitV1 ToCanonicalQuantityUnit(
    QuantityUnitV1 value) noexcept {
    switch (value) {
        case QuantityUnitV1::kShare:
            return CanonicalQuantityUnitV1::kShare;
        case QuantityUnitV1::kFundUnit:
            return CanonicalQuantityUnitV1::kFundUnit;
        case QuantityUnitV1::kLot:
            return CanonicalQuantityUnitV1::kLot;
        case QuantityUnitV1::kBondPiece:
            return CanonicalQuantityUnitV1::kBondPiece;
        case QuantityUnitV1::kIndexUnit:
            return CanonicalQuantityUnitV1::kIndexUnit;
        case QuantityUnitV1::kUnknown:
            return CanonicalQuantityUnitV1::kUnknown;
    }
    return CanonicalQuantityUnitV1::kUnknown;
}

CanonicalTradingPhaseV1 ToCanonicalPhase(TradingPhaseV1 value) noexcept {
    switch (value) {
        case TradingPhaseV1::kStart:
            return CanonicalTradingPhaseV1::kStart;
        case TradingPhaseV1::kOpeningCall:
            return CanonicalTradingPhaseV1::kOpeningCall;
        case TradingPhaseV1::kContinuous:
            return CanonicalTradingPhaseV1::kContinuous;
        case TradingPhaseV1::kSuspended:
            return CanonicalTradingPhaseV1::kSuspended;
        case TradingPhaseV1::kClosingCall:
            return CanonicalTradingPhaseV1::kClosingCall;
        case TradingPhaseV1::kClosed:
            return CanonicalTradingPhaseV1::kClosed;
        case TradingPhaseV1::kEnd:
            return CanonicalTradingPhaseV1::kEnd;
        case TradingPhaseV1::kUnknown:
            return CanonicalTradingPhaseV1::kUnknown;
    }
    return CanonicalTradingPhaseV1::kUnknown;
}

CanonicalLimitSemanticsV1 ToCanonicalLimitSemantics(
    LimitPriceSemanticsV1 value) noexcept {
    switch (value) {
        case LimitPriceSemanticsV1::kFinite:
            return CanonicalLimitSemanticsV1::kFinite;
        case LimitPriceSemanticsV1::kNoLimit:
            return CanonicalLimitSemanticsV1::kNoLimit;
        case LimitPriceSemanticsV1::kUnknown:
            return CanonicalLimitSemanticsV1::kUnknown;
    }
    return CanonicalLimitSemanticsV1::kUnknown;
}

std::uint64_t PackTextPrefix8(std::string_view value) noexcept {
    std::uint64_t result = 0U;
    const std::size_t count = std::min<std::size_t>(value.size(), 8U);
    for (std::size_t index = 0U; index < count; ++index) {
        result |= static_cast<std::uint64_t>(
                      static_cast<unsigned char>(value[index]))
                  << (index * 8U);
    }
    return result;
}

std::uint64_t PackFourU16(
    std::uint16_t first,
    std::uint16_t second,
    std::uint16_t third = 0U,
    std::uint16_t fourth = 0U) noexcept {
    return static_cast<std::uint64_t>(first) |
           (static_cast<std::uint64_t>(second) << 16U) |
           (static_cast<std::uint64_t>(third) << 32U) |
           (static_cast<std::uint64_t>(fourth) << 48U);
}

std::uint16_t ExactU16OrZero(std::int32_t value) noexcept {
    return value >= 0 &&
                   static_cast<std::uint32_t>(value) <=
                       std::numeric_limits<std::uint16_t>::max()
        ? static_cast<std::uint16_t>(value)
        : 0U;
}

std::uint16_t ShanghaiActionCode(
    l2flow::market::TickActionV1 action) noexcept {
    switch (action) {
        case l2flow::market::TickActionV1::kAdd:
            return static_cast<std::uint16_t>('A');
        case l2flow::market::TickActionV1::kCancel:
            return static_cast<std::uint16_t>('D');
        case l2flow::market::TickActionV1::kTrade:
            return static_cast<std::uint16_t>('T');
        case l2flow::market::TickActionV1::kStatus:
            return static_cast<std::uint16_t>('S');
        case l2flow::market::TickActionV1::kUnknown:
            return 0U;
    }
    return 0U;
}

std::uint16_t ShanghaiTickFlagCode(
    l2flow::market::TickActionV1 action,
    std::string_view raw_tick_flag) noexcept {
    // Status carries a multi-byte phase token (START/OCALL/...).  Canonical
    // preserves the normalized phase and leaves that full token in Raw; a
    // first-byte truncation here would be indistinguishable from the B/S/N
    // one-byte tick flag domain.
    if (action == l2flow::market::TickActionV1::kStatus ||
        raw_tick_flag.size() != 1U) {
        return 0U;
    }
    const char value = raw_tick_flag.front();
    if ((action == l2flow::market::TickActionV1::kAdd ||
         action == l2flow::market::TickActionV1::kCancel) &&
        (value == 'B' || value == 'S')) {
        return static_cast<std::uint16_t>(
            static_cast<unsigned char>(value));
    }
    if (action == l2flow::market::TickActionV1::kTrade &&
        (value == 'B' || value == 'S' || value == 'N')) {
        return static_cast<std::uint16_t>(
            static_cast<unsigned char>(value));
    }
    return 0U;
}

CanonicalHeaderV1 MakeHeader(
    const CanonicalRawContextV1& raw,
    const MarketMessageViewV1& message,
    CanonicalEventTypeV1 event_type,
    std::uint32_t record_size,
    std::uint8_t sub_index) noexcept {
    CanonicalHeaderV1 header{};
    header.event_type = event_type;
    header.record_size = record_size;
    header.source_stream_id = raw.source_stream_id;
    header.connection_epoch = raw.authoritative_connection_epoch;
    header.trade_date = raw.trade_date;
    header.quality_flags = raw.upstream_quality_flags;
    header.origin_ingress_sequence = raw.origin_ingress_sequence;
    header.origin_wal_end_pos = raw.origin_wal_end_pos;
    header.vendor_sequence_id = message.vendor_sequence_id;
    header.recv_realtime_ns = message.recv_realtime_ns;
    header.recv_monotonic_ns = message.recv_monotonic_ns;
    header.origin_service_version = message.service_version;
    header.origin_message_id = message.message_id;
    header.origin_service_id = message.service_id;
    header.sub_index = sub_index;
    return header;
}

CanonicalHeaderV1 MakeBusinessHeader(
    const CanonicalRawContextV1& raw,
    const MarketMessageViewV1& message,
    const DecodedMarketCommonV1& common,
    CanonicalEventTypeV1 event_type,
    std::uint32_t record_size,
    std::uint64_t exchange_sequence,
    std::uint32_t channel,
    std::uint64_t extra_quality) noexcept {
    CanonicalHeaderV1 header =
        MakeHeader(raw, message, event_type, record_size, 0U);
    header.quality_flags |= common.quality_flags | extra_quality;
    header.exchange_sequence = exchange_sequence;
    header.exchange_time_ns = common.exchange_time.unix_nanoseconds_valid
        ? common.exchange_time.unix_nanoseconds
        : 0;
    header.instrument_id = common.instrument_id;
    header.channel = channel;
    header.market = ToCanonicalMarket(common.market);
    return header;
}

void SetScalar(
    const l2flow::market::DecimalValueV1& source,
    CanonicalSnapshotScalarValidityV1 bit,
    std::int64_t* value,
    std::uint64_t* validity) noexcept {
    if (source.valid) {
        *value = source.normalized_p6;
        *validity |= CanonicalSnapshotValidityBitV1(bit);
    }
}

bool ConvertQuantityNative(
    const l2flow::market::QuantityValueV1& source,
    std::int64_t* value) noexcept {
    if (!source.valid || source.raw < 0 || source.scale > 18U) {
        return false;
    }
    std::int64_t divisor = 1;
    for (std::uint8_t index = 0U; index < source.scale; ++index) {
        divisor *= 10;
    }
    if (source.raw % divisor == 0) {
        *value = source.raw / divisor;
        return true;
    }
    return false;
}

void SetQuantityScalar(
    const l2flow::market::QuantityValueV1& source,
    CanonicalSnapshotScalarValidityV1 bit,
    std::int64_t* value,
    std::uint64_t* validity) noexcept {
    if (ConvertQuantityNative(source, value)) {
        *validity |= CanonicalSnapshotValidityBitV1(bit);
    }
}

bool QuantityNativeRepresentable(
    const l2flow::market::QuantityValueV1& source) noexcept {
    std::int64_t ignored = 0;
    return !source.valid || ConvertQuantityNative(source, &ignored);
}

bool BookQuantitiesNativeRepresentable(
    const SnapshotBookV1& book) noexcept {
    const std::size_t bid_depth = std::min<std::size_t>(
        book.retained_bid_depth, book.bids.size());
    const std::size_t ask_depth = std::min<std::size_t>(
        book.retained_ask_depth, book.asks.size());
    for (std::size_t index = 0U; index < bid_depth; ++index) {
        if (!QuantityNativeRepresentable(book.bids[index].quantity)) {
            return false;
        }
    }
    for (std::size_t index = 0U; index < ask_depth; ++index) {
        if (!QuantityNativeRepresentable(book.asks[index].quantity)) {
            return false;
        }
    }
    const std::size_t bid_queue = std::min<std::size_t>(
        book.bid1_queue.retained_count,
        book.bid1_queue.quantities.size());
    const std::size_t ask_queue = std::min<std::size_t>(
        book.ask1_queue.retained_count,
        book.ask1_queue.quantities.size());
    for (std::size_t index = 0U; index < bid_queue; ++index) {
        if (!QuantityNativeRepresentable(
                book.bid1_queue.quantities[index])) {
            return false;
        }
    }
    for (std::size_t index = 0U; index < ask_queue; ++index) {
        if (!QuantityNativeRepresentable(
                book.ask1_queue.quantities[index])) {
            return false;
        }
    }
    return true;
}

bool SnapshotQuantitiesNativeRepresentable(
    const BorrowedDecodedMarketEventV1& event) noexcept {
    return std::visit(
        [](const auto* pointer) noexcept {
            using Event = std::remove_cv_t<
                std::remove_pointer_t<decltype(pointer)>>;
            const Event& value = *pointer;
            if constexpr (std::is_same_v<Event, ShanghaiSnapshotV1>) {
                return QuantityNativeRepresentable(value.trade_volume) &&
                       QuantityNativeRepresentable(
                           value.total_bid_volume) &&
                       QuantityNativeRepresentable(
                           value.total_ask_volume) &&
                       BookQuantitiesNativeRepresentable(value.book);
            } else if constexpr (
                std::is_same_v<Event, ShenzhenSnapshotV1>) {
                return QuantityNativeRepresentable(value.volume) &&
                       QuantityNativeRepresentable(
                           value.total_bid_quantity) &&
                       QuantityNativeRepresentable(
                           value.total_ask_quantity) &&
                       BookQuantitiesNativeRepresentable(value.book);
            }
            return true;
        },
        event);
}

bool DecimalNonnegative(
    const l2flow::market::DecimalValueV1& value) noexcept {
    return !value.valid ||
           (value.raw >= 0 && value.normalized_p6 >= 0);
}

bool SnapshotBookAbsoluteValuesValid(
    const SnapshotBookV1& book) noexcept {
    const std::size_t bid_depth = std::min<std::size_t>(
        book.retained_bid_depth, book.bids.size());
    const std::size_t ask_depth = std::min<std::size_t>(
        book.retained_ask_depth, book.asks.size());
    for (std::size_t index = 0U; index < bid_depth; ++index) {
        if (!DecimalNonnegative(book.bids[index].price)) {
            return false;
        }
    }
    for (std::size_t index = 0U; index < ask_depth; ++index) {
        if (!DecimalNonnegative(book.asks[index].price)) {
            return false;
        }
    }
    return true;
}

bool SnapshotAbsoluteValuesValid(
    const BorrowedDecodedMarketEventV1& event) noexcept {
    return std::visit(
        [](const auto* pointer) noexcept {
            using Event = std::remove_cv_t<
                std::remove_pointer_t<decltype(pointer)>>;
            const Event& value = *pointer;
            if constexpr (std::is_same_v<Event, ShanghaiSnapshotV1>) {
                return DecimalNonnegative(value.pre_close_price) &&
                       DecimalNonnegative(value.open_price) &&
                       DecimalNonnegative(value.high_price) &&
                       DecimalNonnegative(value.low_price) &&
                       DecimalNonnegative(value.last_price) &&
                       DecimalNonnegative(value.close_price) &&
                       DecimalNonnegative(value.turnover) &&
                       DecimalNonnegative(
                           value.weighted_average_bid_price) &&
                       DecimalNonnegative(
                           value.weighted_average_ask_price) &&
                       DecimalNonnegative(value.iopv) &&
                       SnapshotBookAbsoluteValuesValid(value.book);
            } else if constexpr (
                std::is_same_v<Event, ShenzhenSnapshotV1>) {
                return value.trade_count >= 0 &&
                       DecimalNonnegative(value.pre_close_price) &&
                       DecimalNonnegative(value.open_price) &&
                       DecimalNonnegative(value.high_price) &&
                       DecimalNonnegative(value.low_price) &&
                       DecimalNonnegative(value.last_price) &&
                       DecimalNonnegative(value.turnover) &&
                       DecimalNonnegative(
                           value.weighted_average_bid_price) &&
                       DecimalNonnegative(
                           value.weighted_average_ask_price) &&
                       DecimalNonnegative(value.high_limit_price) &&
                       DecimalNonnegative(value.low_limit_price) &&
                       DecimalNonnegative(value.iopv) &&
                       SnapshotBookAbsoluteValuesValid(value.book);
            }
            return true;
        },
        event);
}

void CopyBook(
    const SnapshotBookV1& source,
    CanonicalSnapshotPayloadV1* target) noexcept {
    target->actual_bid_depth = source.actual_bid_depth;
    target->actual_ask_depth = source.actual_ask_depth;
    target->scalar_validity |= CanonicalSnapshotValidityBitV1(
        CanonicalSnapshotScalarValidityV1::kActualBidDepth);
    target->scalar_validity |= CanonicalSnapshotValidityBitV1(
        CanonicalSnapshotScalarValidityV1::kActualAskDepth);
    if (source.actual_bid_depth > kCanonicalDepthLevelsV1) {
        target->snapshot_flags |= CanonicalSnapshotFlagBitV1(
            CanonicalSnapshotFlagV1::kBidDepthTruncatedTo10);
    }
    if (source.actual_ask_depth > kCanonicalDepthLevelsV1) {
        target->snapshot_flags |= CanonicalSnapshotFlagBitV1(
            CanonicalSnapshotFlagV1::kAskDepthTruncatedTo10);
    }
    const std::size_t bid_count = std::min<std::size_t>(
        source.retained_bid_depth, kCanonicalDepthLevelsV1);
    const std::size_t ask_count = std::min<std::size_t>(
        source.retained_ask_depth, kCanonicalDepthLevelsV1);
    for (std::size_t index = 0U; index < bid_count; ++index) {
        const std::uint16_t bit = static_cast<std::uint16_t>(1U << index);
        if (source.bids[index].price.valid) {
            target->bid_price_p6[index] =
                source.bids[index].price.normalized_p6;
            target->bid_price_validity |= bit;
        }
        if (ConvertQuantityNative(
                source.bids[index].quantity,
                &target->bid_quantity_native[index])) {
            target->bid_quantity_validity |= bit;
        }
        if (source.bids[index].order_count_valid) {
            target->bid_order_count[index] =
                source.bids[index].order_count;
            target->bid_order_count_validity |= bit;
        }
    }
    for (std::size_t index = 0U; index < ask_count; ++index) {
        const std::uint16_t bit = static_cast<std::uint16_t>(1U << index);
        if (source.asks[index].price.valid) {
            target->ask_price_p6[index] =
                source.asks[index].price.normalized_p6;
            target->ask_price_validity |= bit;
        }
        if (ConvertQuantityNative(
                source.asks[index].quantity,
                &target->ask_quantity_native[index])) {
            target->ask_quantity_validity |= bit;
        }
        if (source.asks[index].order_count_valid) {
            target->ask_order_count[index] =
                source.asks[index].order_count;
            target->ask_order_count_validity |= bit;
        }
    }

    target->bid1_total_order_count = source.bid1_queue.total_order_count;
    target->bid1_revealed_count =
        source.bid1_queue.actual_revealed_count;
    target->ask1_total_order_count = source.ask1_queue.total_order_count;
    target->ask1_revealed_count =
        source.ask1_queue.actual_revealed_count;
    target->scalar_validity |= CanonicalSnapshotValidityBitV1(
        CanonicalSnapshotScalarValidityV1::kBid1TotalOrderCount);
    target->scalar_validity |= CanonicalSnapshotValidityBitV1(
        CanonicalSnapshotScalarValidityV1::kBid1RevealedCount);
    target->scalar_validity |= CanonicalSnapshotValidityBitV1(
        CanonicalSnapshotScalarValidityV1::kAsk1TotalOrderCount);
    target->scalar_validity |= CanonicalSnapshotValidityBitV1(
        CanonicalSnapshotScalarValidityV1::kAsk1RevealedCount);
    const std::size_t bid_queue_count = std::min<std::size_t>(
        source.bid1_queue.retained_count, kCanonicalQueueEntriesV1);
    const std::size_t ask_queue_count = std::min<std::size_t>(
        source.ask1_queue.retained_count, kCanonicalQueueEntriesV1);
    for (std::size_t index = 0U; index < bid_queue_count; ++index) {
        if (ConvertQuantityNative(
                source.bid1_queue.quantities[index],
                &target->bid1_queue_quantity_native[index])) {
            target->bid_queue_validity |= std::uint64_t{1U} << index;
        }
    }
    for (std::size_t index = 0U; index < ask_queue_count; ++index) {
        if (ConvertQuantityNative(
                source.ask1_queue.quantities[index],
                &target->ask1_queue_quantity_native[index])) {
            target->ask_queue_validity |= std::uint64_t{1U} << index;
        }
    }
}

CanonicalTickPayloadV1 MakeTickPayload(
    const TickFieldsV1& fields,
    QuantityUnitV1 quantity_unit,
    std::uint64_t source_enum_bits) noexcept {
    CanonicalTickPayloadV1 payload{};
    payload.validity_bitmap = fields.validity_bitmap;
    if ((fields.validity_bitmap &
         l2flow::market::kTickPriceValidV1) != 0U) {
        payload.price_p6 = fields.price.normalized_p6;
    }
    if ((fields.validity_bitmap &
         l2flow::market::kTickQuantityValidV1) != 0U) {
        payload.quantity_native = fields.quantity.raw;
    }
    if ((fields.validity_bitmap &
         l2flow::market::kTickTradeAmountValidV1) != 0U) {
        payload.trade_amount_p6 = fields.trade_amount.normalized_p6;
    }
    if ((fields.validity_bitmap &
         l2flow::market::kTickMatchedQuantityValidV1) != 0U) {
        payload.matched_quantity_native = fields.matched_quantity.raw;
    }
    if ((fields.validity_bitmap &
         l2flow::market::kTickPrimaryOrderIdValidV1) != 0U) {
        payload.primary_order_id = fields.primary_order_id;
    }
    if ((fields.validity_bitmap &
         l2flow::market::kTickBuyOrderIdValidV1) != 0U) {
        payload.buy_order_id = fields.buy_order_id;
    }
    if ((fields.validity_bitmap &
         l2flow::market::kTickSellOrderIdValidV1) != 0U) {
        payload.sell_order_id = fields.sell_order_id;
    }
    payload.source_enum_bits = source_enum_bits;
    payload.action = ToCanonicalAction(fields.action);
    payload.side = ToCanonicalSide(fields.side);
    payload.order_type = ToCanonicalOrderType(fields.order_type);
    payload.aggressor = ToCanonicalAggressor(fields.aggressor);
    payload.quantity_unit = ToCanonicalQuantityUnit(quantity_unit);
    payload.phase = ToCanonicalPhase(fields.phase);
    return payload;
}

CanonicalSnapshotRecordV1 MakeShanghaiSnapshot(
    const CanonicalRawContextV1& raw,
    const MarketMessageViewV1& message,
    const ShanghaiSnapshotV1& source,
    std::uint64_t extra_quality) noexcept {
    CanonicalSnapshotRecordV1 record{};
    record.header = MakeBusinessHeader(
        raw, message, source.common,
        CanonicalEventTypeV1::kSnapshot,
        static_cast<std::uint32_t>(kCanonicalSnapshotRecordBytesV1),
        0U, 0U, extra_quality);
    auto& target = record.payload;
    if (source.common.exchange_time.unix_nanoseconds_valid) {
        target.scalar_validity |= CanonicalSnapshotValidityBitV1(
            CanonicalSnapshotScalarValidityV1::kExchangeTime);
    }
    SetScalar(source.pre_close_price,
              CanonicalSnapshotScalarValidityV1::kPreClosePrice,
              &target.pre_close_price_p6, &target.scalar_validity);
    SetScalar(source.open_price,
              CanonicalSnapshotScalarValidityV1::kOpenPrice,
              &target.open_price_p6, &target.scalar_validity);
    SetScalar(source.high_price,
              CanonicalSnapshotScalarValidityV1::kHighPrice,
              &target.high_price_p6, &target.scalar_validity);
    SetScalar(source.low_price,
              CanonicalSnapshotScalarValidityV1::kLowPrice,
              &target.low_price_p6, &target.scalar_validity);
    SetScalar(source.last_price,
              CanonicalSnapshotScalarValidityV1::kLastPrice,
              &target.last_price_p6, &target.scalar_validity);
    SetScalar(source.close_price,
              CanonicalSnapshotScalarValidityV1::kClosePrice,
              &target.close_price_p6, &target.scalar_validity);
    SetQuantityScalar(source.trade_volume,
                      CanonicalSnapshotScalarValidityV1::kVolumeNative,
                      &target.volume_native, &target.scalar_validity);
    SetScalar(source.turnover,
              CanonicalSnapshotScalarValidityV1::kTurnoverP6,
              &target.turnover_p6, &target.scalar_validity);
    target.trade_count = source.trade_count;
    target.scalar_validity |= CanonicalSnapshotValidityBitV1(
        CanonicalSnapshotScalarValidityV1::kTradeCount);
    SetQuantityScalar(
        source.total_bid_volume,
        CanonicalSnapshotScalarValidityV1::kTotalBidQuantity,
        &target.total_bid_quantity_native, &target.scalar_validity);
    SetQuantityScalar(
        source.total_ask_volume,
        CanonicalSnapshotScalarValidityV1::kTotalAskQuantity,
        &target.total_ask_quantity_native, &target.scalar_validity);
    SetScalar(source.weighted_average_bid_price,
              CanonicalSnapshotScalarValidityV1::kWeightedBidPrice,
              &target.weighted_bid_price_p6, &target.scalar_validity);
    SetScalar(source.weighted_average_ask_price,
              CanonicalSnapshotScalarValidityV1::kWeightedAskPrice,
              &target.weighted_ask_price_p6, &target.scalar_validity);
    SetScalar(source.iopv,
              CanonicalSnapshotScalarValidityV1::kIopv,
              &target.iopv_p6, &target.scalar_validity);
    target.image_status = std::bit_cast<std::uint32_t>(source.image_status);
    target.scalar_validity |= CanonicalSnapshotValidityBitV1(
        CanonicalSnapshotScalarValidityV1::kImageStatus);
    if (source.instrument_status_valid) {
        target.status_code_bits = PackTextPrefix8(source.instrument_status);
        target.scalar_validity |= CanonicalSnapshotValidityBitV1(
            CanonicalSnapshotScalarValidityV1::kStatusCode);
    }
    target.quantity_unit = ToCanonicalQuantityUnit(
        source.common.quantity_unit);
    if (target.quantity_unit != CanonicalQuantityUnitV1::kUnknown) {
        target.scalar_validity |= CanonicalSnapshotValidityBitV1(
            CanonicalSnapshotScalarValidityV1::kQuantityUnit);
    }
    CopyBook(source.book, &target);
    return record;
}

CanonicalSnapshotRecordV1 MakeShenzhenSnapshot(
    const CanonicalRawContextV1& raw,
    const MarketMessageViewV1& message,
    const ShenzhenSnapshotV1& source,
    std::uint64_t extra_quality) noexcept {
    CanonicalSnapshotRecordV1 record{};
    record.header = MakeBusinessHeader(
        raw, message, source.common,
        CanonicalEventTypeV1::kSnapshot,
        static_cast<std::uint32_t>(kCanonicalSnapshotRecordBytesV1),
        0U, source.channel, extra_quality);
    auto& target = record.payload;
    if (source.common.exchange_time.unix_nanoseconds_valid) {
        target.scalar_validity |= CanonicalSnapshotValidityBitV1(
            CanonicalSnapshotScalarValidityV1::kExchangeTime);
    }
    SetScalar(source.pre_close_price,
              CanonicalSnapshotScalarValidityV1::kPreClosePrice,
              &target.pre_close_price_p6, &target.scalar_validity);
    SetScalar(source.open_price,
              CanonicalSnapshotScalarValidityV1::kOpenPrice,
              &target.open_price_p6, &target.scalar_validity);
    SetScalar(source.high_price,
              CanonicalSnapshotScalarValidityV1::kHighPrice,
              &target.high_price_p6, &target.scalar_validity);
    SetScalar(source.low_price,
              CanonicalSnapshotScalarValidityV1::kLowPrice,
              &target.low_price_p6, &target.scalar_validity);
    SetScalar(source.last_price,
              CanonicalSnapshotScalarValidityV1::kLastPrice,
              &target.last_price_p6, &target.scalar_validity);
    SetQuantityScalar(source.volume,
                      CanonicalSnapshotScalarValidityV1::kVolumeNative,
                      &target.volume_native, &target.scalar_validity);
    SetScalar(source.turnover,
              CanonicalSnapshotScalarValidityV1::kTurnoverP6,
              &target.turnover_p6, &target.scalar_validity);
    if (source.trade_count >= 0) {
        target.trade_count = source.trade_count;
        target.scalar_validity |= CanonicalSnapshotValidityBitV1(
            CanonicalSnapshotScalarValidityV1::kTradeCount);
    }
    SetQuantityScalar(
        source.total_bid_quantity,
        CanonicalSnapshotScalarValidityV1::kTotalBidQuantity,
        &target.total_bid_quantity_native, &target.scalar_validity);
    SetQuantityScalar(
        source.total_ask_quantity,
        CanonicalSnapshotScalarValidityV1::kTotalAskQuantity,
        &target.total_ask_quantity_native, &target.scalar_validity);
    SetScalar(source.weighted_average_bid_price,
              CanonicalSnapshotScalarValidityV1::kWeightedBidPrice,
              &target.weighted_bid_price_p6, &target.scalar_validity);
    SetScalar(source.weighted_average_ask_price,
              CanonicalSnapshotScalarValidityV1::kWeightedAskPrice,
              &target.weighted_ask_price_p6, &target.scalar_validity);
    SetScalar(source.high_limit_price,
              CanonicalSnapshotScalarValidityV1::kHighLimitPrice,
              &target.high_limit_price_p6, &target.scalar_validity);
    SetScalar(source.low_limit_price,
              CanonicalSnapshotScalarValidityV1::kLowLimitPrice,
              &target.low_limit_price_p6, &target.scalar_validity);
    target.high_limit_semantics = ToCanonicalLimitSemantics(
        source.high_limit_semantics);
    target.low_limit_semantics = ToCanonicalLimitSemantics(
        source.low_limit_semantics);
    SetScalar(source.iopv,
              CanonicalSnapshotScalarValidityV1::kIopv,
              &target.iopv_p6, &target.scalar_validity);
    if (source.trading_phase_code_valid) {
        target.raw_phase_bits = PackTextPrefix8(source.trading_phase_code);
        target.scalar_validity |= CanonicalSnapshotValidityBitV1(
            CanonicalSnapshotScalarValidityV1::kRawPhase);
    }
    target.quantity_unit = ToCanonicalQuantityUnit(
        source.common.quantity_unit);
    if (target.quantity_unit != CanonicalQuantityUnitV1::kUnknown) {
        target.scalar_validity |= CanonicalSnapshotValidityBitV1(
            CanonicalSnapshotScalarValidityV1::kQuantityUnit);
    }
    CopyBook(source.book, &target);
    return record;
}

struct VendorScopeKey final {
    std::uint8_t service_id = 0U;
    std::uint16_t message_id = 0U;
    auto operator<=>(const VendorScopeKey&) const = default;
};

struct ExchangeScopeKey final {
    SequenceScopeKindV1 kind = SequenceScopeKindV1::kShanghaiChannel;
    std::uint32_t channel = 0U;
    auto operator<=>(const ExchangeScopeKey&) const = default;
};

struct GuardSlot final {
    std::unique_ptr<SequenceGuardV1> guard;
    std::uint64_t poison_quality_bit = 0U;
    // Sticky provenance for every later kScopePoisoned observation.  The
    // current Raw cursor is not the first fault once a scope is poisoned.
    std::uint64_t first_bad_origin_wal_end_pos = 0U;
};

using VendorScopeMap = std::map<VendorScopeKey, GuardSlot>;
using ExchangeScopeMap = std::map<ExchangeScopeKey, GuardSlot>;
using PhaseMap = std::map<std::string, TradingPhaseV1>;

using BusinessSequenceIdentity = MarketBusinessSequenceIdentityV1;

BusinessSequenceIdentity BusinessSequenceOf(
    const BorrowedDecodedMarketEventV1& event) noexcept {
    return std::visit(
        [](const auto* pointer) noexcept {
            return MarketBusinessSequenceIdentityOfV1(*pointer);
        },
        event);
}

std::uint32_t DecodedChannel(
    const BorrowedDecodedMarketEventV1& event) noexcept {
    return std::visit(
        [](const auto* pointer) noexcept -> std::uint32_t {
            using Event = std::remove_cv_t<
                std::remove_pointer_t<decltype(pointer)>>;
            const Event& value = *pointer;
            if constexpr (std::is_same_v<Event, ShanghaiTickV1>) {
                return std::bit_cast<std::uint32_t>(value.channel);
            } else if constexpr (
                std::is_same_v<Event, ShenzhenSnapshotV1> ||
                std::is_same_v<Event, ShenzhenOrderV1> ||
                std::is_same_v<Event, ShenzhenTransactionV1>) {
                return value.channel;
            }
            return 0U;
        },
        event);
}

bool SnapshotQueueCountsValid(
    const BorrowedDecodedMarketEventV1& event) noexcept {
    return std::visit(
        [](const auto* pointer) noexcept {
            using Event = std::remove_cv_t<
                std::remove_pointer_t<decltype(pointer)>>;
            const Event& value = *pointer;
            if constexpr (std::is_same_v<Event, ShanghaiSnapshotV1> ||
                          std::is_same_v<Event, ShenzhenSnapshotV1>) {
                return value.book.bid1_queue.actual_revealed_count <=
                           kCanonicalQueueEntriesV1 &&
                       value.book.ask1_queue.actual_revealed_count <=
                           kCanonicalQueueEntriesV1;
            }
            return true;
        },
        event);
}

[[noreturn]] void ThrowSequenceEvidenceFailure(
    MarketSequenceEvidenceErrorV1 error) {
    if (error == MarketSequenceEvidenceErrorV1::kResourceExhausted) {
        throw std::bad_alloc();
    }
    if (error == MarketSequenceEvidenceErrorV1::kValueTooLarge) {
        throw std::length_error("sequence evidence too large");
    }
    throw std::logic_error("sequence evidence construction failed");
}

std::vector<std::byte> MakeExchangeEvidence(
    const BorrowedDecodedMarketEventV1& event) {
    std::vector<std::byte> evidence;
    const MarketSequenceEvidenceErrorV1 error = std::visit(
        [&evidence](const auto* pointer) {
            using Event = std::remove_cv_t<
                std::remove_pointer_t<decltype(pointer)>>;
            if constexpr (
                std::is_same_v<Event, ShanghaiTickV1> ||
                std::is_same_v<Event, ShenzhenOrderV1> ||
                std::is_same_v<Event, ShenzhenTransactionV1>) {
                return BuildExchangeSequenceEvidenceV1(
                    *pointer, &evidence);
            }
            return MarketSequenceEvidenceErrorV1::
                kNoBusinessSequence;
        },
        event);
    if (error != MarketSequenceEvidenceErrorV1::kNone) {
        ThrowSequenceEvidenceFailure(error);
    }
    return evidence;
}

std::vector<std::byte> MakeVendorEvidence(
    const MarketMessageViewV1& message) {
    std::vector<std::byte> evidence;
    const MarketSequenceEvidenceErrorV1 error =
        BuildVendorSequenceEvidenceV1(message, &evidence);
    if (error != MarketSequenceEvidenceErrorV1::kNone) {
        ThrowSequenceEvidenceFailure(error);
    }
    return evidence;
}

std::uint64_t VendorScopeId(const MarketMessageViewV1& message) noexcept {
    return CanonicalVendorMessageScopeIdV1(
        message.service_id, message.message_id);
}

std::uint64_t ExchangeScopeId(
    SequenceScopeKindV1 kind,
    std::uint32_t channel) noexcept {
    const CanonicalMarketV1 market =
        kind == SequenceScopeKindV1::kShanghaiChannel
        ? CanonicalMarketV1::kShanghai
        : CanonicalMarketV1::kShenzhen;
    return CanonicalChannelScopeIdV1(market, channel);
}

std::uint64_t SnapshotScopeId(CanonicalMarketV1 market) noexcept {
    return CanonicalSnapshotFamilyScopeIdV1(market);
}

bool CanonicalBusinessSemanticsValid(
    const BorrowedDecodedMarketEventV1& event) noexcept {
    return std::visit(
        [](const auto* pointer) noexcept {
            using Event = std::remove_cv_t<
                std::remove_pointer_t<decltype(pointer)>>;
            const Event& value = *pointer;
            if constexpr (std::is_same_v<Event, ShanghaiTickV1> ||
                          std::is_same_v<Event, ShenzhenOrderV1> ||
                          std::is_same_v<Event, ShenzhenTransactionV1>) {
                const bool quantity_valid =
                    (value.fields.validity_bitmap &
                     l2flow::market::kTickQuantityValidV1) == 0U ||
                    (value.fields.quantity.valid &&
                     value.fields.quantity.scale == 0U &&
                     value.fields.quantity.raw >= 0);
                const bool matched_quantity_valid =
                    (value.fields.validity_bitmap &
                     l2flow::market::kTickMatchedQuantityValidV1) == 0U ||
                    (value.fields.matched_quantity.valid &&
                     value.fields.matched_quantity.scale == 0U &&
                     value.fields.matched_quantity.raw >= 0);
                const bool required_quantity =
                    value.fields.action == TickActionV1::kStatus ||
                    (value.fields.validity_bitmap &
                     l2flow::market::kTickQuantityValidV1) != 0U;
                const bool absolute_values_valid =
                    DecimalNonnegative(value.fields.price) &&
                    DecimalNonnegative(value.fields.trade_amount) &&
                    (value.common.market_notices &
                     l2flow::market::MarketNoticeBitV1(
                         l2flow::market::MarketNoticeV1::
                             kMatchedQuantityDomainInvalid)) == 0U;
                return value.fields.action != TickActionV1::kUnknown &&
                       required_quantity && quantity_valid &&
                       matched_quantity_valid && absolute_values_valid;
            }
            return true;
        },
        event);
}

std::uint64_t CanonicalBusinessSemanticsQualityFlags(
    const BorrowedDecodedMarketEventV1& event) noexcept {
    return std::visit(
        [](const auto* pointer) noexcept -> std::uint64_t {
            using Event = std::remove_cv_t<
                std::remove_pointer_t<decltype(pointer)>>;
            const Event& value = *pointer;
            if constexpr (std::is_same_v<Event, ShanghaiTickV1> ||
                          std::is_same_v<Event, ShenzhenOrderV1> ||
                          std::is_same_v<Event, ShenzhenTransactionV1>) {
                return value.fields.action == TickActionV1::kUnknown
                    ? QualityBit(QualityFlagV1::kUnknownEnum)
                    : 0U;
            }
            return 0U;
        },
        event);
}

std::span<const std::byte> RecordBytes(
    const CanonicalRecordVariantV1& record) noexcept {
    return std::visit(
        [](const auto& value) noexcept -> std::span<const std::byte> {
            return std::as_bytes(std::span(&value, 1U));
        },
        record);
}

CanonicalHeaderV1& MutableHeader(
    CanonicalRecordVariantV1& record) noexcept {
    return std::visit(
        [](auto& value) noexcept -> CanonicalHeaderV1& {
            return value.header;
        },
        record);
}

CanonicalValidationErrorV1 ValidateRecord(
    const CanonicalRecordVariantV1& record) noexcept {
    return std::visit(
        [](const auto& value) noexcept {
            using Record = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<Record, CanonicalTickRecordV1>) {
                return ValidateCanonicalTickRecordV1(value);
            } else if constexpr (
                std::is_same_v<Record, CanonicalSnapshotRecordV1>) {
                return ValidateCanonicalSnapshotRecordV1(value);
            } else if constexpr (
                std::is_same_v<Record, CanonicalQualityRecordV1>) {
                return ValidateCanonicalQualityRecordV1(value);
            } else {
                return ValidateCanonicalControlRecordV1(value);
            }
        },
        record);
}

bool HashUpdateU32(
    l2flow::common::Sha256Hasher* hasher,
    std::uint32_t value) noexcept {
    std::array<std::byte, 4U> bytes{};
    for (std::size_t index = 0U; index < bytes.size(); ++index) {
        bytes[index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
    return hasher->Update(bytes);
}

bool ComputePublicationContractImpl(
    std::span<const RoutedCanonicalRecordV1> records,
    CanonicalPublicationContractV1* contract) noexcept {
    if (contract == nullptr ||
        records.size() > std::numeric_limits<std::uint32_t>::max()) {
        return false;
    }
    l2flow::common::Sha256Hasher hasher;
    const auto domain = std::as_bytes(std::span(
        kBundleHashDomainV1.data(), kBundleHashDomainV1.size()));
    const std::array<std::byte, 1U> separator{std::byte{0U}};
    if (!hasher.Update(domain) || !hasher.Update(separator) ||
        !HashUpdateU32(
            &hasher, static_cast<std::uint32_t>(records.size()))) {
        return false;
    }
    for (const auto& routed : records) {
        const std::array<std::byte, 1U> family{
            static_cast<std::byte>(routed.family)};
        const std::span<const std::byte> bytes = RecordBytes(routed.record);
        if (bytes.size() > std::numeric_limits<std::uint32_t>::max() ||
            !hasher.Update(family) ||
            !HashUpdateU32(&hasher, routed.shard) ||
            !HashUpdateU32(
                &hasher, static_cast<std::uint32_t>(bytes.size())) ||
            !hasher.Update(bytes)) {
            return false;
        }
    }
    CanonicalPublicationContractV1 result{};
    result.record_count = static_cast<std::uint32_t>(records.size());
    if (!hasher.Finalize(&result.ordered_records_sha256)) {
        return false;
    }
    *contract = result;
    return true;
}

std::size_t FamilyIndex(CanonicalFamilyV1 family) noexcept {
    switch (family) {
        case CanonicalFamilyV1::kSnapshot:
            return 0U;
        case CanonicalFamilyV1::kTick:
            return 1U;
        case CanonicalFamilyV1::kQuality:
            return 2U;
        case CanonicalFamilyV1::kControl:
            return 3U;
    }
    return 3U;
}

}  // namespace

bool ComputeCanonicalPublicationContractV1(
    std::span<const RoutedCanonicalRecordV1> records,
    CanonicalPublicationContractV1* contract) noexcept {
    return ComputePublicationContractImpl(records, contract);
}

class CanonicalNormalizationTransactionV1::Impl final {
public:
    CanonicalNormalizerV1* owner = nullptr;
    std::uint64_t expected_version = 0U;
    bool active = false;
    CanonicalSegmentContextV1 segment_context{};
    std::vector<RoutedCanonicalRecordV1> records;
    CanonicalPublicationContractV1 publication_contract{};
    GuardSlot* vendor_slot = nullptr;
    GuardSlot* exchange_slot = nullptr;
    VendorScopeMap::node_type staged_vendor_scope{};
    ExchangeScopeMap::node_type staged_exchange_scope{};
    SequenceGuardTokenV1 vendor_token{};
    SequenceGuardTokenV1 exchange_token{};
    bool has_vendor_token = false;
    bool has_exchange_token = false;
    std::uint64_t vendor_poison_bit_after_commit = 0U;
    std::uint64_t exchange_poison_bit_after_commit = 0U;
    std::uint64_t vendor_first_bad_after_commit = 0U;
    std::uint64_t exchange_first_bad_after_commit = 0U;
    TradingPhaseV1* phase_slot = nullptr;
    PhaseMap::node_type staged_phase{};
    TradingPhaseV1 phase_after_commit = TradingPhaseV1::kUnknown;
    bool has_phase_update = false;
};

class CanonicalNormalizerV1::Impl final {
public:
    explicit Impl(CanonicalNormalizerConfigV1 value)
        : config(std::move(value)),
          decoder(l2flow::market::MarketDecoderConfigV1{
              config.trade_date,
              config.source_stream_id,
              config.instrument_registry,
              l2flow::market::ShanghaiPhaseAttributionModeV1::kDeferred,
              config.decoder_limits}) {
        for (auto& family : last_event_ids) {
            family.resize(config.shard_count, 0U);
        }
    }

    CanonicalNormalizerConfigV1 config{};
    l2flow::market::MarketDecoderV1 decoder;
    VendorScopeMap vendor_scopes;
    ExchangeScopeMap exchange_scopes;
    PhaseMap phases;
    std::array<std::vector<std::uint64_t>, 4U> last_event_ids;
    CanonicalNormalizationTransactionV1::Impl* active_transaction = nullptr;
    std::uint64_t version = 0U;
    std::uint64_t snapshot_records_committed = 0U;
    std::uint64_t tick_records_committed = 0U;
    std::uint64_t quality_records_committed = 0U;
    std::uint64_t control_records_committed = 0U;
    bool fatal = false;
};

namespace {

bool ResolveExpectedFirst(
    const CanonicalSequencePolicyV1& policy,
    const SequenceScopeKeyV1& scope,
    std::optional<std::uint64_t>* expected) noexcept {
    const InitialSequencePolicyV1 mode =
        scope.kind == SequenceScopeKindV1::kVendorMessage
        ? policy.vendor_initial
        : policy.exchange_initial;
    if (mode == InitialSequencePolicyV1::kAllowUnknownFirst) {
        *expected = std::nullopt;
        return true;
    }
    for (const auto& configured : policy.expected_first_by_scope) {
        if (configured.scope == scope) {
            *expected = configured.sequence;
            return true;
        }
    }
    return false;
}

template <typename NormalizerImpl, typename TransactionImpl>
GuardSlot* GetVendorSlot(
    NormalizerImpl* impl,
    TransactionImpl* pending,
    const MarketMessageViewV1& message,
    CanonicalNormalizePrepareErrorV1* error) {
    const VendorScopeKey key{
        message.service_id, message.message_id};
    auto found = impl->vendor_scopes.find(key);
    if (found != impl->vendor_scopes.end()) {
        return &found->second;
    }
    const std::uint64_t scope_count =
        static_cast<std::uint64_t>(impl->vendor_scopes.size()) +
        static_cast<std::uint64_t>(impl->exchange_scopes.size()) +
        (pending->staged_vendor_scope.empty() ? 0U : 1U) +
        (pending->staged_exchange_scope.empty() ? 0U : 1U);
    if (scope_count >= impl->config.maximum_sequence_scopes) {
        *error = CanonicalNormalizePrepareErrorV1::kScopeCapacity;
        return nullptr;
    }
    SequenceGuardConfigV1 guard_config{};
    guard_config.scope = MakeVendorSequenceScopeV1(
        impl->config.capture_date,
        impl->config.source_stream_id,
        impl->config.stream_day_id,
        message.service_id,
        message.message_id);
    guard_config.max_seen_entries =
        impl->config.maximum_seen_entries_per_scope;
    guard_config.max_seen_payload_bytes =
        impl->config.maximum_seen_payload_bytes_per_scope;
    if (!ResolveExpectedFirst(
            impl->config.sequence_policy,
            guard_config.scope,
            &guard_config.expected_first)) {
        *error = CanonicalNormalizePrepareErrorV1::kSequencePolicyMissing;
        return nullptr;
    }
    GuardSlot slot{};
    const SequenceGuardCreateErrorV1 create_error =
        SequenceGuardV1::Create(guard_config, &slot.guard);
    if (create_error != SequenceGuardCreateErrorV1::kNone) {
        *error = create_error == SequenceGuardCreateErrorV1::kResourceExhausted
            ? CanonicalNormalizePrepareErrorV1::kResourceExhausted
            : CanonicalNormalizePrepareErrorV1::kSequenceGuardFailure;
        return nullptr;
    }
    VendorScopeMap staging;
    const auto inserted = staging.emplace(key, std::move(slot));
    if (!inserted.second) {
        *error = CanonicalNormalizePrepareErrorV1::kUnexpectedFailure;
        return nullptr;
    }
    pending->staged_vendor_scope = staging.extract(inserted.first);
    return &pending->staged_vendor_scope.mapped();
}

template <typename NormalizerImpl, typename TransactionImpl>
GuardSlot* GetExchangeSlot(
    NormalizerImpl* impl,
    TransactionImpl* pending,
    const BusinessSequenceIdentity& identity,
    CanonicalNormalizePrepareErrorV1* error) {
    const ExchangeScopeKey key{identity.kind, identity.channel};
    auto found = impl->exchange_scopes.find(key);
    if (found != impl->exchange_scopes.end()) {
        return &found->second;
    }
    const std::uint64_t scope_count =
        static_cast<std::uint64_t>(impl->vendor_scopes.size()) +
        static_cast<std::uint64_t>(impl->exchange_scopes.size()) +
        (pending->staged_vendor_scope.empty() ? 0U : 1U) +
        (pending->staged_exchange_scope.empty() ? 0U : 1U);
    if (scope_count >= impl->config.maximum_sequence_scopes) {
        *error = CanonicalNormalizePrepareErrorV1::kScopeCapacity;
        return nullptr;
    }
    SequenceGuardConfigV1 guard_config{};
    guard_config.scope = identity.kind ==
            SequenceScopeKindV1::kShanghaiChannel
        ? MakeShanghaiChannelScopeV1(
              impl->config.trade_date,
              impl->config.source_stream_id,
              identity.channel)
        : MakeShenzhenUnifiedChannelScopeV1(
              impl->config.trade_date,
              impl->config.source_stream_id,
              identity.channel);
    guard_config.max_seen_entries =
        impl->config.maximum_seen_entries_per_scope;
    guard_config.max_seen_payload_bytes =
        impl->config.maximum_seen_payload_bytes_per_scope;
    if (!ResolveExpectedFirst(
            impl->config.sequence_policy,
            guard_config.scope,
            &guard_config.expected_first)) {
        *error = CanonicalNormalizePrepareErrorV1::kSequencePolicyMissing;
        return nullptr;
    }
    GuardSlot slot{};
    const SequenceGuardCreateErrorV1 create_error =
        SequenceGuardV1::Create(guard_config, &slot.guard);
    if (create_error != SequenceGuardCreateErrorV1::kNone) {
        *error = create_error == SequenceGuardCreateErrorV1::kResourceExhausted
            ? CanonicalNormalizePrepareErrorV1::kResourceExhausted
            : CanonicalNormalizePrepareErrorV1::kSequenceGuardFailure;
        return nullptr;
    }
    ExchangeScopeMap staging;
    const auto inserted = staging.emplace(key, std::move(slot));
    if (!inserted.second) {
        *error = CanonicalNormalizePrepareErrorV1::kUnexpectedFailure;
        return nullptr;
    }
    pending->staged_exchange_scope = staging.extract(inserted.first);
    return &pending->staged_exchange_scope.mapped();
}

CanonicalQualityRecordV1 MakeSequenceQuality(
    const CanonicalRawContextV1& raw,
    const MarketMessageViewV1& message,
    bool vendor,
    SequenceGuardOutcomeV1 outcome,
    const SequenceGuardPrepareResultV1& sequence,
    std::uint64_t scope_id,
    std::uint32_t channel,
    CanonicalMarketV1 market,
    std::uint8_t sub_index,
    std::uint64_t sticky_poison_bit,
    std::uint64_t sticky_first_bad_origin_wal_end_pos) noexcept {
    CanonicalQualityRecordV1 record{};
    record.header = MakeHeader(
        raw, message, CanonicalEventTypeV1::kQuality,
        static_cast<std::uint32_t>(kCanonicalQualityRecordBytesV1),
        sub_index);
    record.header.market = market;
    record.header.channel = channel;
    if (!vendor) {
        record.header.exchange_sequence = sequence.sequence;
    }
    record.header.quality_flags |=
        OutcomeQualityBit(vendor, outcome) | sticky_poison_bit;
    if (sequence.start_unknown_after_commit) {
        record.header.quality_flags |=
            QualityBit(QualityFlagV1::kStartUnknown);
    }
    record.payload.expected_sequence =
        sequence.has_missing_interval ? sequence.missing_begin
        : (!sequence.had_high_before && sequence.had_configured_first
               ? sequence.configured_first
               : sequence.high_before);
    record.payload.actual_sequence = sequence.sequence;
    record.payload.first_bad_origin_wal_end_pos =
        sticky_first_bad_origin_wal_end_pos != 0U
        ? sticky_first_bad_origin_wal_end_pos
        : raw.origin_wal_end_pos;
    record.payload.scope_id = scope_id;
    record.payload.payload_sha256 = sequence.payload_digest;
    record.payload.related_connection_epoch =
        raw.authoritative_connection_epoch;
    record.payload.quality_type = OutcomeQualityType(vendor, outcome);
    record.payload.scope_type = vendor
        ? CanonicalQualityScopeV1::kVendorMessage
        : CanonicalQualityScopeV1::kChannel;
    record.payload.detail_code = static_cast<std::uint16_t>(outcome) + 1U;
    return record;
}

CanonicalQualityRecordV1 MakeDecodeQuality(
    const CanonicalRawContextV1& raw,
    const MarketMessageViewV1& message,
    MarketDecodeErrorV1 decode_error,
    CanonicalQualityTypeV1 quality_type,
    CanonicalQualityScopeV1 scope_type,
    std::uint64_t scope_id,
    std::uint64_t actual_sequence,
    std::uint8_t sub_index,
    std::uint64_t extra_quality,
    const DecodedMarketCommonV1* decoded_common = nullptr,
    std::uint32_t channel = 0U,
    std::uint64_t exchange_sequence = 0U) noexcept {
    CanonicalQualityRecordV1 record{};
    record.header = MakeHeader(
        raw, message, CanonicalEventTypeV1::kQuality,
        static_cast<std::uint32_t>(kCanonicalQualityRecordBytesV1),
        sub_index);
    record.header.quality_flags |=
        l2flow::market::MarketDecodeQualityFlagsV1(decode_error) |
        extra_quality;
    if (quality_type == CanonicalQualityTypeV1::kSchemaUnknown) {
        record.header.quality_flags |=
            QualityBit(QualityFlagV1::kSchemaUnknown);
    }
    if (decoded_common != nullptr) {
        record.header.market = ToCanonicalMarket(decoded_common->market);
        record.header.instrument_id = decoded_common->instrument_id;
        record.header.channel = channel;
        record.header.exchange_sequence = exchange_sequence;
        if (decoded_common->exchange_time.unix_nanoseconds_valid) {
            record.header.exchange_time_ns =
                decoded_common->exchange_time.unix_nanoseconds;
        }
    }
    record.payload.actual_sequence = actual_sequence;
    record.payload.first_bad_origin_wal_end_pos = raw.origin_wal_end_pos;
    record.payload.scope_id = scope_id;
    record.payload.payload_sha256 =
        l2flow::common::ComputeSha256(message.body);
    record.payload.related_connection_epoch =
        raw.authoritative_connection_epoch;
    record.payload.quality_type = quality_type;
    record.payload.scope_type = scope_type;
    record.payload.detail_code = static_cast<std::uint16_t>(decode_error);
    return record;
}

template <typename NormalizerImpl>
bool AssignEventIdsAndValidate(
    NormalizerImpl* impl,
    std::vector<RoutedCanonicalRecordV1>* records,
    CanonicalNormalizePrepareErrorV1* error) noexcept {
    for (std::size_t index = 0U; index < records->size(); ++index) {
        auto& routed = (*records)[index];
        const std::size_t family = FamilyIndex(routed.family);
        if (routed.shard >= impl->config.shard_count) {
            *error = CanonicalNormalizePrepareErrorV1::
                kCanonicalValidationFailure;
            return false;
        }
        std::uint64_t offset = 1U;
        for (std::size_t prior = 0U; prior < index; ++prior) {
            const auto& preceding = (*records)[prior];
            if (preceding.family == routed.family &&
                preceding.shard == routed.shard) {
                if (offset == std::numeric_limits<std::uint64_t>::max()) {
                    *error = CanonicalNormalizePrepareErrorV1::
                        kEventIdExhausted;
                    return false;
                }
                ++offset;
            }
        }
        if (impl->last_event_ids[family][routed.shard] ==
                std::numeric_limits<std::uint64_t>::max() ||
            impl->last_event_ids[family][routed.shard] >
                std::numeric_limits<std::uint64_t>::max() - offset) {
            *error = CanonicalNormalizePrepareErrorV1::kEventIdExhausted;
            return false;
        }
        MutableHeader(routed.record).shard_event_id =
            impl->last_event_ids[family][routed.shard] + offset;
        if (ValidateRecord(routed.record) !=
            CanonicalValidationErrorV1::kNone) {
            *error = CanonicalNormalizePrepareErrorV1::
                kCanonicalValidationFailure;
            return false;
        }
    }
    return true;
}

void AppendSequenceQualityIfNeeded(
    std::vector<RoutedCanonicalRecordV1>* records,
    const CanonicalRawContextV1& raw,
    const MarketMessageViewV1& message,
    bool vendor,
    const SequenceGuardPrepareResultV1& result,
    std::uint64_t scope_id,
    std::uint32_t channel,
    CanonicalMarketV1 market,
    std::uint8_t sub_index,
    std::uint64_t sticky_poison_bit,
    std::uint64_t sticky_first_bad_origin_wal_end_pos) {
    if (result.outcome == SequenceGuardOutcomeV1::kFirst ||
        result.outcome == SequenceGuardOutcomeV1::kContiguous) {
        return;
    }
    records->push_back(RoutedCanonicalRecordV1{
        CanonicalFamilyV1::kQuality,
        0U,
        MakeSequenceQuality(
            raw, message, vendor, result.outcome, result,
            scope_id, channel, market, sub_index,
            sticky_poison_bit,
            sticky_first_bad_origin_wal_end_pos)});
}

}  // namespace

CanonicalNormalizationTransactionV1::
CanonicalNormalizationTransactionV1() noexcept = default;

CanonicalNormalizationTransactionV1::
CanonicalNormalizationTransactionV1(
    CanonicalNormalizationTransactionV1&& other) noexcept
    : impl_(std::move(other.impl_)) {}

CanonicalNormalizationTransactionV1&
CanonicalNormalizationTransactionV1::operator=(
    CanonicalNormalizationTransactionV1&& other) noexcept {
    if (this == &other) {
        return *this;
    }
    if (impl_ != nullptr && impl_->active && impl_->owner != nullptr) {
        (void)impl_->owner->Abort(this);
    }
    impl_ = std::move(other.impl_);
    return *this;
}

CanonicalNormalizationTransactionV1::~
CanonicalNormalizationTransactionV1() {
    if (impl_ != nullptr && impl_->active && impl_->owner != nullptr) {
        (void)impl_->owner->Abort(this);
    }
}

bool CanonicalNormalizationTransactionV1::active() const noexcept {
    return impl_ != nullptr && impl_->active;
}

std::span<const RoutedCanonicalRecordV1>
CanonicalNormalizationTransactionV1::records() const noexcept {
    return impl_ == nullptr
        ? std::span<const RoutedCanonicalRecordV1>{}
        : std::span<const RoutedCanonicalRecordV1>(impl_->records);
}

CanonicalPublicationContractV1
CanonicalNormalizationTransactionV1::publication_contract() const noexcept {
    return impl_ == nullptr ? CanonicalPublicationContractV1{}
                            : impl_->publication_contract;
}

CanonicalSegmentContextV1
CanonicalNormalizationTransactionV1::segment_context() const noexcept {
    return impl_ == nullptr ? CanonicalSegmentContextV1{}
                            : impl_->segment_context;
}

CanonicalNormalizerV1::CanonicalNormalizerV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

CanonicalNormalizerV1::~CanonicalNormalizerV1() {
    if (impl_ != nullptr && impl_->active_transaction != nullptr) {
        auto* transaction = impl_->active_transaction;
        if (transaction->has_exchange_token) {
            (void)transaction->exchange_slot->guard->Abort(
                &transaction->exchange_token);
        }
        if (transaction->has_vendor_token) {
            (void)transaction->vendor_slot->guard->Abort(
                &transaction->vendor_token);
        }
        transaction->active = false;
        transaction->owner = nullptr;
        impl_->active_transaction = nullptr;
    }
}

CanonicalNormalizerCreateErrorV1 CanonicalNormalizerV1::Create(
    CanonicalNormalizerConfigV1 config,
    std::unique_ptr<CanonicalNormalizerV1>* output) noexcept {
    if (output == nullptr) {
        return CanonicalNormalizerCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (!ConfigShapeValid(config)) {
        return CanonicalNormalizerCreateErrorV1::kInvalidConfiguration;
    }
    config.decoder_limits.maximum_phase_products =
        static_cast<std::size_t>(std::min<std::uint64_t>(
            config.maximum_phase_products,
            std::numeric_limits<std::size_t>::max()));
    try {
        auto impl = std::make_unique<Impl>(std::move(config));
        if (!impl->decoder.configuration_valid()) {
            return CanonicalNormalizerCreateErrorV1::kInvalidConfiguration;
        }
        output->reset(new CanonicalNormalizerV1(std::move(impl)));
        return CanonicalNormalizerCreateErrorV1::kNone;
    } catch (...) {
        return CanonicalNormalizerCreateErrorV1::kResourceExhausted;
    }
}

CanonicalNormalizePrepareResultV1 CanonicalNormalizerV1::Prepare(
    const CanonicalRawContextV1& raw,
    const MarketMessageViewV1& message,
    CanonicalNormalizationTransactionV1* transaction) noexcept {
    return PrepareImpl(
        raw, message, nullptr, MarketDecodeErrorV1::kNone, false,
        transaction);
}

CanonicalNormalizePrepareResultV1 CanonicalNormalizerV1::PrepareDecoded(
    const CanonicalRawContextV1& raw,
    const MarketMessageViewV1& message,
    const RetainedMarketEventV1& decoded,
    CanonicalNormalizationTransactionV1* transaction) noexcept {
    return PrepareImpl(
        raw, message, &decoded, MarketDecodeErrorV1::kNone, false,
        transaction);
}

CanonicalNormalizePrepareResultV1
CanonicalNormalizerV1::PrepareDecodedFailure(
    const CanonicalRawContextV1& raw,
    const MarketMessageViewV1& message,
    MarketDecodeErrorV1 decode_error,
    CanonicalNormalizationTransactionV1* transaction) noexcept {
    return PrepareImpl(
        raw, message, nullptr, decode_error, true, transaction);
}

CanonicalNormalizePrepareResultV1 CanonicalNormalizerV1::PrepareImpl(
    const CanonicalRawContextV1& raw,
    const MarketMessageViewV1& message,
    const RetainedMarketEventV1* retained,
    MarketDecodeErrorV1 decoded_failure,
    bool use_decoded_failure,
    CanonicalNormalizationTransactionV1* transaction) noexcept {
    CanonicalNormalizePrepareResultV1 result{};
    if (transaction == nullptr) {
        result.error = CanonicalNormalizePrepareErrorV1::kNullTransaction;
        return result;
    }
    if (transaction->active()) {
        result.error =
            CanonicalNormalizePrepareErrorV1::kTransactionStillActive;
        return result;
    }
    if (impl_->fatal) {
        result.error = CanonicalNormalizePrepareErrorV1::kNormalizerFatal;
        return result;
    }
    if (impl_->active_transaction != nullptr) {
        result.error = CanonicalNormalizePrepareErrorV1::kNormalizerBusy;
        return result;
    }
    if (!RawContextValid(impl_->config, raw)) {
        result.error = CanonicalNormalizePrepareErrorV1::kInvalidRawContext;
        return result;
    }
    if (!MessageViewValid(impl_->config, raw, message)) {
        result.error = CanonicalNormalizePrepareErrorV1::kInvalidMarketView;
        return result;
    }
    BorrowedDecodedMarketEventV1 supplied_decoded{};
    if (retained != nullptr &&
        (!BorrowRetainedMarketEvent(*retained, &supplied_decoded) ||
         !BorrowedEventMatchesMessage(supplied_decoded, message))) {
        result.error =
            CanonicalNormalizePrepareErrorV1::kInvalidDecodedEvent;
        return result;
    }
    if (use_decoded_failure &&
        (!RecordableExternalDecodeFailure(decoded_failure) ||
         !RecognizedCoreMessageShape(message))) {
        result.error =
            CanonicalNormalizePrepareErrorV1::kInvalidDecodedFailure;
        return result;
    }
    if (impl_->version == std::numeric_limits<std::uint64_t>::max()) {
        impl_->fatal = true;
        result.error = CanonicalNormalizePrepareErrorV1::kNormalizerFatal;
        return result;
    }

    try {
        auto pending = std::make_unique<
            CanonicalNormalizationTransactionV1::Impl>();
        pending->owner = this;
        pending->expected_version = impl_->version;
        pending->segment_context = CanonicalSegmentContextV1{
            raw.capture_date,
            raw.trade_date,
            raw.source_stream_id,
            raw.stream_day_id,
            raw.clock_epoch};

        CanonicalNormalizePrepareErrorV1 local_error =
            CanonicalNormalizePrepareErrorV1::kNone;
        GuardSlot* vendor = GetVendorSlot(
            impl_.get(), pending.get(), message, &local_error);
        if (vendor == nullptr) {
            result.error = local_error;
            return result;
        }
        if (vendor->guard->Snapshot().version ==
            std::numeric_limits<std::uint64_t>::max()) {
            impl_->fatal = true;
            result.error = CanonicalNormalizePrepareErrorV1::kNormalizerFatal;
            return result;
        }
        pending->vendor_slot = vendor;
        const std::vector<std::byte> vendor_evidence =
            MakeVendorEvidence(message);
        const SequenceGuardPrepareResultV1 vendor_result =
            vendor->guard->Prepare(
                message.vendor_sequence_id,
                vendor_evidence,
                &pending->vendor_token);
        if (!vendor_result.ok() || !vendor_result.token_prepared) {
            if (vendor_result.error ==
                SequenceGuardPrepareErrorV1::kResourceExhausted) {
                result.error = CanonicalNormalizePrepareErrorV1::
                    kResourceExhausted;
            } else if (vendor_result.error ==
                       SequenceGuardPrepareErrorV1::kUnexpectedFailure) {
                impl_->fatal = true;
                result.error = CanonicalNormalizePrepareErrorV1::
                    kNormalizerFatal;
            } else {
                result.error = CanonicalNormalizePrepareErrorV1::
                    kSequenceGuardFailure;
            }
            return result;
        }
        pending->has_vendor_token = true;
        result.has_vendor_outcome = true;
        result.vendor_outcome = vendor_result.outcome;
        std::uint64_t vendor_sequence_context_quality = 0U;
        const SequenceGuardSnapshotV1 vendor_snapshot_before =
            vendor->guard->Snapshot();
        if (vendor_result.start_unknown_after_commit) {
            vendor_sequence_context_quality |=
                QualityBit(QualityFlagV1::kStartUnknown);
        }
        if (vendor_snapshot_before.state ==
                SequenceGuardStateV1::kDegradedGap ||
            vendor_result.outcome == SequenceGuardOutcomeV1::kGap) {
            vendor_sequence_context_quality |=
                QualityBit(QualityFlagV1::kVendorSequenceGap);
        }
        vendor_sequence_context_quality |= vendor->poison_quality_bit;

        const std::uint64_t vendor_outcome_bit = OutcomeQualityBit(
            true, vendor_result.outcome);
        if (vendor_result.outcome == SequenceGuardOutcomeV1::kConflict ||
            vendor_result.outcome == SequenceGuardOutcomeV1::kBackward ||
            vendor_result.outcome == SequenceGuardOutcomeV1::kCapacity) {
            pending->vendor_poison_bit_after_commit = vendor_outcome_bit;
            pending->vendor_first_bad_after_commit =
                raw.origin_wal_end_pos;
        }
        AppendSequenceQualityIfNeeded(
            &pending->records,
            raw,
            message,
            true,
            vendor_result,
            VendorScopeId(message),
            0U,
            CanonicalMarketV1::kUnknown,
            1U,
            vendor->poison_quality_bit,
            vendor->first_bad_origin_wal_end_pos);

        if (SequenceAccepted(vendor_result.outcome)) {
            DecodedMarketEventV1 decoded_storage;
            BorrowedDecodedMarketEventV1 decoded{};
            MarketDecodeErrorV1 decode_error = use_decoded_failure
                ? decoded_failure
                : MarketDecodeErrorV1::kNone;
            if (retained == nullptr && !use_decoded_failure) {
                decode_error =
                    impl_->decoder.Decode(message, &decoded_storage);
                if (decode_error == MarketDecodeErrorV1::kNone) {
                    decoded = BorrowDecodedMarketEvent(decoded_storage);
                }
            } else if (retained != nullptr) {
                decoded = supplied_decoded;
            }
            result.decode_error = decode_error;
            if (decode_error != MarketDecodeErrorV1::kNone) {
                if (decode_error == MarketDecodeErrorV1::kResourceExhausted) {
                    (void)vendor->guard->Abort(&pending->vendor_token);
                    result.error = CanonicalNormalizePrepareErrorV1::
                        kResourceExhausted;
                    return result;
                }
                if (decode_error == MarketDecodeErrorV1::kUnexpectedFailure ||
                    decode_error == MarketDecodeErrorV1::kInvalidConfiguration ||
                    decode_error == MarketDecodeErrorV1::kNullOutput ||
                    decode_error == MarketDecodeErrorV1::
                        kPhaseProductLimitExceeded) {
                    (void)vendor->guard->Abort(&pending->vendor_token);
                    impl_->fatal = true;
                    result.error = CanonicalNormalizePrepareErrorV1::
                        kNormalizerFatal;
                    return result;
                }
                const CanonicalQualityTypeV1 type =
                    decode_error == MarketDecodeErrorV1::
                            kUnsupportedServiceVersion ||
                        decode_error == MarketDecodeErrorV1::
                            kUnsupportedMessage
                    ? CanonicalQualityTypeV1::kSchemaUnknown
                    : CanonicalQualityTypeV1::kDecodeError;
                pending->records.push_back(RoutedCanonicalRecordV1{
                    CanonicalFamilyV1::kQuality,
                    0U,
                    MakeDecodeQuality(
                        raw, message, decode_error, type,
                        CanonicalQualityScopeV1::kVendorMessage,
                        VendorScopeId(message),
                        message.vendor_sequence_id, 3U,
                        vendor_sequence_context_quality)});
                result.error = CanonicalNormalizePrepareErrorV1::
                    kDecodeFailureRecorded;
            } else {
                const DecodedMarketCommonV1& common =
                    BorrowedMarketCommon(decoded);
                const BusinessSequenceIdentity business =
                    BusinessSequenceOf(decoded);
                SequenceGuardPrepareResultV1 exchange_result{};
                bool business_sequence_accepted = true;
                std::uint64_t business_sequence_context_quality =
                    vendor_sequence_context_quality;
                if (business.present) {
                    GuardSlot* exchange = GetExchangeSlot(
                        impl_.get(), pending.get(), business, &local_error);
                    if (exchange == nullptr) {
                        (void)vendor->guard->Abort(&pending->vendor_token);
                        result.error = local_error;
                        return result;
                    }
                    if (exchange->guard->Snapshot().version ==
                        std::numeric_limits<std::uint64_t>::max()) {
                        (void)vendor->guard->Abort(&pending->vendor_token);
                        impl_->fatal = true;
                        result.error = CanonicalNormalizePrepareErrorV1::
                            kNormalizerFatal;
                        return result;
                    }
                    pending->exchange_slot = exchange;
                    const SequenceGuardSnapshotV1 exchange_snapshot_before =
                        exchange->guard->Snapshot();
                    if (exchange_snapshot_before.start_unknown) {
                        business_sequence_context_quality |=
                            QualityBit(QualityFlagV1::kStartUnknown);
                    }
                    if (exchange_snapshot_before.state ==
                        SequenceGuardStateV1::kDegradedGap) {
                        business_sequence_context_quality |=
                            QualityBit(
                                QualityFlagV1::kExchangeSequenceGap);
                    }
                    business_sequence_context_quality |=
                        exchange->poison_quality_bit;
                    if (!business.sequence_valid) {
                        // Scope policy is resolved before rejecting the
                        // sequence value.  Otherwise a missing exact
                        // RequireConfiguredFirst entry could be bypassed by
                        // submitting sequence zero for that channel.
                        business_sequence_accepted = false;
                        pending->records.push_back(RoutedCanonicalRecordV1{
                            CanonicalFamilyV1::kQuality,
                            0U,
                            MakeDecodeQuality(
                                raw,
                                message,
                                MarketDecodeErrorV1::kInvalidInput,
                                CanonicalQualityTypeV1::
                                    kNormalizationRejected,
                                CanonicalQualityScopeV1::kChannel,
                                ExchangeScopeId(
                                    business.kind, business.channel),
                                0U,
                                3U,
                                common.quality_flags |
                                    business_sequence_context_quality,
                                &common,
                                business.channel,
                                0U)});
                    } else {
                    const std::vector<std::byte> evidence =
                        MakeExchangeEvidence(decoded);
                    exchange_result = exchange->guard->Prepare(
                        business.sequence,
                        evidence,
                        &pending->exchange_token);
                    if (!exchange_result.ok() ||
                        !exchange_result.token_prepared) {
                        (void)vendor->guard->Abort(&pending->vendor_token);
                        if (exchange_result.error ==
                            SequenceGuardPrepareErrorV1::
                                kResourceExhausted) {
                            result.error = CanonicalNormalizePrepareErrorV1::
                                kResourceExhausted;
                        } else if (exchange_result.error ==
                                   SequenceGuardPrepareErrorV1::
                                       kUnexpectedFailure) {
                            impl_->fatal = true;
                            result.error = CanonicalNormalizePrepareErrorV1::
                                kNormalizerFatal;
                        } else {
                            result.error = CanonicalNormalizePrepareErrorV1::
                                kSequenceGuardFailure;
                        }
                        return result;
                    }
                    pending->has_exchange_token = true;
                    result.has_exchange_outcome = true;
                    result.exchange_outcome = exchange_result.outcome;
                    if (exchange_result.start_unknown_after_commit) {
                        business_sequence_context_quality |=
                            QualityBit(QualityFlagV1::kStartUnknown);
                    }
                    if (exchange_result.outcome ==
                        SequenceGuardOutcomeV1::kGap) {
                        business_sequence_context_quality |=
                            QualityBit(
                                QualityFlagV1::kExchangeSequenceGap);
                    }
                    business_sequence_accepted =
                        SequenceAccepted(exchange_result.outcome);
                    const std::uint64_t exchange_outcome_bit =
                        OutcomeQualityBit(false, exchange_result.outcome);
                    if (exchange_result.outcome ==
                            SequenceGuardOutcomeV1::kConflict ||
                        exchange_result.outcome ==
                            SequenceGuardOutcomeV1::kBackward ||
                        exchange_result.outcome ==
                            SequenceGuardOutcomeV1::kCapacity) {
                        pending->exchange_poison_bit_after_commit =
                            exchange_outcome_bit;
                        pending->exchange_first_bad_after_commit =
                            raw.origin_wal_end_pos;
                    }
                    AppendSequenceQualityIfNeeded(
                        &pending->records,
                        raw,
                        message,
                        false,
                        exchange_result,
                        ExchangeScopeId(
                            business.kind, business.channel),
                        business.channel,
                        ToCanonicalMarket(common.market),
                        2U,
                        exchange->poison_quality_bit |
                            vendor_sequence_context_quality,
                        exchange->first_bad_origin_wal_end_pos);
                    }
                }

                const bool queue_counts_valid =
                    SnapshotQueueCountsValid(decoded);
                const bool snapshot_values_valid =
                    SnapshotQuantitiesNativeRepresentable(decoded) &&
                    SnapshotAbsoluteValuesValid(decoded);
                if (!queue_counts_valid || !snapshot_values_valid) {
                    pending->records.push_back(RoutedCanonicalRecordV1{
                        CanonicalFamilyV1::kQuality,
                        0U,
                        MakeDecodeQuality(
                            raw, message,
                            queue_counts_valid
                                ? MarketDecodeErrorV1::kInvalidInput
                                : MarketDecodeErrorV1::kCountExceeded,
                            CanonicalQualityTypeV1::kSnapshotRejected,
                            CanonicalQualityScopeV1::kSnapshotFamily,
                            SnapshotScopeId(
                                ToCanonicalMarket(common.market)),
                            0U, 3U,
                            common.quality_flags |
                                business_sequence_context_quality,
                            &common,
                            DecodedChannel(decoded),
                            business.sequence)});
                }

                const bool canonical_semantics_valid =
                    CanonicalBusinessSemanticsValid(decoded);
                if (business_sequence_accepted &&
                    !canonical_semantics_valid) {
                    pending->records.push_back(RoutedCanonicalRecordV1{
                        CanonicalFamilyV1::kQuality,
                        0U,
                        MakeDecodeQuality(
                            raw, message,
                            MarketDecodeErrorV1::kInvalidInput,
                            CanonicalQualityTypeV1::kNormalizationRejected,
                            CanonicalQualityScopeV1::kChannel,
                            ExchangeScopeId(
                                business.kind, business.channel),
                            business.sequence, 3U,
                            CanonicalBusinessSemanticsQualityFlags(
                                decoded) |
                                common.quality_flags |
                                business_sequence_context_quality,
                            &common,
                            business.channel,
                            business.sequence)});
                }

                if (business_sequence_accepted && queue_counts_valid &&
                    snapshot_values_valid &&
                    canonical_semantics_valid &&
                    common.instrument_id != 0U) {
                    const std::uint64_t sticky_quality =
                        business_sequence_context_quality;

                    std::uint32_t shard = 0U;
                    if (!CanonicalShardForInstrumentV1(
                            common.instrument_id,
                            impl_->config.shard_count,
                            &shard)) {
                        (void)pending->vendor_slot->guard->Abort(
                            &pending->vendor_token);
                        if (pending->has_exchange_token) {
                            (void)pending->exchange_slot->guard->Abort(
                                &pending->exchange_token);
                        }
                        result.error = CanonicalNormalizePrepareErrorV1::
                            kCanonicalValidationFailure;
                        return result;
                    }

                    std::visit(
                        [&](const auto* pointer) {
                            using Event = std::remove_cv_t<
                                std::remove_pointer_t<decltype(pointer)>>;
                            const Event& value = *pointer;
                            if constexpr (std::is_same_v<Event,
                                                         ShanghaiTickV1>) {
                                TickFieldsV1 attributed_fields =
                                    value.fields;
                                if (attributed_fields.action !=
                                        TickActionV1::kStatus &&
                                    value.common.security_id_valid) {
                                    const auto phase = impl_->phases.find(
                                        value.common.security_id);
                                    if (phase != impl_->phases.end() &&
                                        phase->second !=
                                            TradingPhaseV1::kUnknown) {
                                        attributed_fields.phase =
                                            phase->second;
                                        attributed_fields.validity_bitmap |=
                                            l2flow::market::
                                                kTickPhaseValidV1;
                                    }
                                }
                                CanonicalTickRecordV1 record{};
                                record.header = MakeBusinessHeader(
                                    raw, message, value.common,
                                    CanonicalEventTypeV1::kTick,
                                    static_cast<std::uint32_t>(
                                        kCanonicalTickRecordBytesV1),
                                    static_cast<std::uint64_t>(
                                        value.business_index),
                                    std::bit_cast<std::uint32_t>(
                                        value.channel),
                                    sticky_quality);
                                record.payload = MakeTickPayload(
                                    attributed_fields,
                                    value.common.quantity_unit,
                                    PackFourU16(
                                        ShanghaiActionCode(
                                            attributed_fields.action),
                                        ShanghaiTickFlagCode(
                                            attributed_fields.action,
                                            value.raw_tick_flag)));
                                pending->records.push_back(
                                    RoutedCanonicalRecordV1{
                                        CanonicalFamilyV1::kTick,
                                        shard,
                                        record});
                                if (attributed_fields.action ==
                                        TickActionV1::kStatus &&
                                    attributed_fields.phase !=
                                        TradingPhaseV1::kUnknown &&
                                    value.common.security_id_valid &&
                                    value.raw_tick_flag_valid) {
                                    auto phase_slot = impl_->phases.find(
                                        value.common.security_id);
                                    if (phase_slot == impl_->phases.end() &&
                                        (impl_->phases.size() +
                                         (pending->staged_phase.empty()
                                              ? 0U
                                              : 1U)) >=
                                            impl_->config.
                                                maximum_phase_products) {
                                        throw std::length_error(
                                            "phase product capacity");
                                    }
                                    if (phase_slot == impl_->phases.end()) {
                                        PhaseMap staging;
                                        const auto inserted = staging.emplace(
                                            value.common.security_id,
                                            TradingPhaseV1::kUnknown);
                                        if (!inserted.second) {
                                            throw std::logic_error(
                                                "phase staging collision");
                                        }
                                        pending->staged_phase =
                                            staging.extract(inserted.first);
                                        pending->phase_slot =
                                            &pending->staged_phase.mapped();
                                    } else {
                                        pending->phase_slot =
                                            &phase_slot->second;
                                    }
                                    pending->phase_after_commit =
                                        attributed_fields.phase;
                                    pending->has_phase_update = true;
                                }
                            } else if constexpr (std::is_same_v<
                                                     Event,
                                                     ShenzhenOrderV1>) {
                                CanonicalTickRecordV1 record{};
                                record.header = MakeBusinessHeader(
                                    raw, message, value.common,
                                    CanonicalEventTypeV1::kTick,
                                    static_cast<std::uint32_t>(
                                        kCanonicalTickRecordBytesV1),
                                    static_cast<std::uint64_t>(
                                        value.application_sequence),
                                    value.channel, sticky_quality);
                                record.payload = MakeTickPayload(
                                    value.fields,
                                    value.common.quantity_unit,
                                    PackFourU16(
                                        ExactU16OrZero(value.raw_side),
                                        ExactU16OrZero(
                                            value.raw_order_type)));
                                pending->records.push_back(
                                    RoutedCanonicalRecordV1{
                                        CanonicalFamilyV1::kTick,
                                        shard,
                                        record});
                            } else if constexpr (std::is_same_v<
                                                     Event,
                                                     ShenzhenTransactionV1>) {
                                CanonicalTickRecordV1 record{};
                                record.header = MakeBusinessHeader(
                                    raw, message, value.common,
                                    CanonicalEventTypeV1::kTick,
                                    static_cast<std::uint32_t>(
                                        kCanonicalTickRecordBytesV1),
                                    static_cast<std::uint64_t>(
                                        value.application_sequence),
                                    value.channel, sticky_quality);
                                record.payload = MakeTickPayload(
                                    value.fields,
                                    value.common.quantity_unit,
                                    PackFourU16(
                                        ExactU16OrZero(
                                            value.raw_execution_type),
                                        0U));
                                pending->records.push_back(
                                    RoutedCanonicalRecordV1{
                                        CanonicalFamilyV1::kTick,
                                        shard,
                                        record});
                            } else if constexpr (std::is_same_v<
                                                     Event,
                                                     ShanghaiSnapshotV1>) {
                                pending->records.push_back(
                                    RoutedCanonicalRecordV1{
                                        CanonicalFamilyV1::kSnapshot,
                                        shard,
                                        MakeShanghaiSnapshot(
                                            raw, message, value,
                                            sticky_quality)});
                            } else {
                                pending->records.push_back(
                                    RoutedCanonicalRecordV1{
                                        CanonicalFamilyV1::kSnapshot,
                                        shard,
                                        MakeShenzhenSnapshot(
                                            raw, message, value,
                                            sticky_quality)});
                            }
                        },
                        decoded);
                    result.business_record_planned = true;
                } else if (common.instrument_id == 0U &&
                           business_sequence_accepted &&
                           queue_counts_valid &&
                           snapshot_values_valid &&
                           canonical_semantics_valid) {
                    pending->records.push_back(RoutedCanonicalRecordV1{
                        CanonicalFamilyV1::kQuality,
                        0U,
                        MakeDecodeQuality(
                            raw, message,
                            MarketDecodeErrorV1::kInvalidInput,
                            CanonicalQualityTypeV1::kInstrumentUnknown,
                            CanonicalQualityScopeV1::kInstrument,
                            0U, 0U, 3U,
                            QualityBit(
                                QualityFlagV1::kInstrumentUnknown) |
                                common.quality_flags |
                                business_sequence_context_quality,
                            &common,
                            DecodedChannel(decoded),
                            business.sequence)});
                }
            }
        }

        std::stable_sort(
            pending->records.begin(),
            pending->records.end(),
            [](const RoutedCanonicalRecordV1& left,
               const RoutedCanonicalRecordV1& right) noexcept {
                return std::visit(
                           [](const auto& value) {
                               return value.header.sub_index;
                           },
                           left.record) <
                       std::visit(
                           [](const auto& value) {
                               return value.header.sub_index;
                           },
                           right.record);
            });

        if (!AssignEventIdsAndValidate(
                impl_.get(), &pending->records, &local_error)) {
            if (pending->has_exchange_token) {
                (void)pending->exchange_slot->guard->Abort(
                    &pending->exchange_token);
            }
            (void)pending->vendor_slot->guard->Abort(
                &pending->vendor_token);
            result.error = local_error;
            return result;
        }
        if (!ComputeCanonicalPublicationContractV1(
                pending->records, &pending->publication_contract)) {
            if (pending->has_exchange_token) {
                (void)pending->exchange_slot->guard->Abort(
                    &pending->exchange_token);
            }
            (void)pending->vendor_slot->guard->Abort(
                &pending->vendor_token);
            result.error = CanonicalNormalizePrepareErrorV1::
                kUnexpectedFailure;
            return result;
        }
        pending->active = true;
        impl_->active_transaction = pending.get();
        transaction->impl_ = std::move(pending);
        result.record_count = transaction->impl_->publication_contract.
            record_count;
        result.transaction_prepared = true;
        return result;
    } catch (const std::bad_alloc&) {
        result.error = CanonicalNormalizePrepareErrorV1::kResourceExhausted;
        return result;
    } catch (const std::length_error&) {
        result.error = CanonicalNormalizePrepareErrorV1::kScopeCapacity;
        return result;
    } catch (...) {
        result.error = CanonicalNormalizePrepareErrorV1::kUnexpectedFailure;
        return result;
    }
}

CanonicalNormalizePrepareResultV1 CanonicalNormalizerV1::PrepareControl(
    const CanonicalRawContextV1& raw,
    const l2flow::control::ControlRecordV1& control,
    std::int64_t recv_realtime_ns,
    std::int64_t recv_monotonic_ns,
    CanonicalNormalizationTransactionV1* transaction) noexcept {
    CanonicalNormalizePrepareResultV1 result{};
    if (transaction == nullptr) {
        result.error = CanonicalNormalizePrepareErrorV1::kNullTransaction;
        return result;
    }
    if (transaction->active()) {
        result.error =
            CanonicalNormalizePrepareErrorV1::kTransactionStillActive;
        return result;
    }
    if (impl_->fatal) {
        result.error = CanonicalNormalizePrepareErrorV1::kNormalizerFatal;
        return result;
    }
    if (impl_->active_transaction != nullptr) {
        result.error = CanonicalNormalizePrepareErrorV1::kNormalizerBusy;
        return result;
    }
    if (!RawContextValid(impl_->config, raw)) {
        result.error = CanonicalNormalizePrepareErrorV1::kInvalidRawContext;
        return result;
    }
    if (impl_->version == std::numeric_limits<std::uint64_t>::max()) {
        impl_->fatal = true;
        result.error = CanonicalNormalizePrepareErrorV1::kNormalizerFatal;
        return result;
    }

    try {
        auto pending = std::make_unique<
            CanonicalNormalizationTransactionV1::Impl>();
        pending->owner = this;
        pending->expected_version = impl_->version;
        pending->segment_context = CanonicalSegmentContextV1{
            raw.capture_date,
            raw.trade_date,
            raw.source_stream_id,
            raw.stream_day_id,
            raw.clock_epoch};

        CanonicalControlRecordV1 record{};
        const CanonicalControlAdapterErrorV1 adapter_error =
            MakeCanonicalControlRecordV1(
                raw,
                control,
                recv_realtime_ns,
                recv_monotonic_ns,
                1U,
                &record);
        if (adapter_error != CanonicalControlAdapterErrorV1::kNone) {
            result.error =
                CanonicalNormalizePrepareErrorV1::kControlAdapterFailure;
            return result;
        }
        pending->records.push_back(RoutedCanonicalRecordV1{
            CanonicalFamilyV1::kControl, 0U, record});
        CanonicalNormalizePrepareErrorV1 local_error =
            CanonicalNormalizePrepareErrorV1::kNone;
        if (!AssignEventIdsAndValidate(
                impl_.get(), &pending->records, &local_error)) {
            result.error = local_error;
            return result;
        }
        if (!ComputeCanonicalPublicationContractV1(
                pending->records, &pending->publication_contract)) {
            result.error =
                CanonicalNormalizePrepareErrorV1::kUnexpectedFailure;
            return result;
        }
        pending->active = true;
        impl_->active_transaction = pending.get();
        transaction->impl_ = std::move(pending);
        result.record_count =
            transaction->impl_->publication_contract.record_count;
        result.transaction_prepared = true;
        return result;
    } catch (const std::bad_alloc&) {
        result.error = CanonicalNormalizePrepareErrorV1::kResourceExhausted;
        return result;
    } catch (const std::length_error&) {
        result.error = CanonicalNormalizePrepareErrorV1::kScopeCapacity;
        return result;
    } catch (...) {
        result.error = CanonicalNormalizePrepareErrorV1::kUnexpectedFailure;
        return result;
    }
}

CanonicalNormalizeCommitErrorV1 CanonicalNormalizerV1::CommitPublished(
    CanonicalNormalizationTransactionV1* transaction,
    const CanonicalPublicationContractV1& receipt) noexcept {
    if (transaction == nullptr || transaction->impl_ == nullptr ||
        !transaction->impl_->active) {
        return CanonicalNormalizeCommitErrorV1::kInvalidTransaction;
    }
    auto* pending = transaction->impl_.get();
    if (pending->owner != this ||
        impl_->active_transaction != pending) {
        return CanonicalNormalizeCommitErrorV1::kWrongNormalizer;
    }
    if (impl_->fatal) {
        return CanonicalNormalizeCommitErrorV1::kNormalizerFatal;
    }
    if (pending->expected_version != impl_->version) {
        impl_->fatal = true;
        return CanonicalNormalizeCommitErrorV1::kVersionMismatch;
    }
    if (!(receipt == pending->publication_contract)) {
        // CommitPublished is called only after sink publication.  A mismatch
        // means the visible bundle cannot be reconciled with the prepared
        // logical transition; reusing IDs or guards would make a partial
        // generation look valid.  Fail-stop and require whole-generation
        // discard/replay from the last seal.
        impl_->fatal = true;
        return CanonicalNormalizeCommitErrorV1::kPublicationMismatch;
    }

    // New all-day state is allocated as detached map nodes during Prepare.
    // Node insertion transfers ownership without allocation after records are
    // visible.  Abort/error destruction before this point simply releases the
    // staged nodes, so uncommitted inputs cannot consume scope capacity.
    if (!pending->staged_vendor_scope.empty()) {
        auto inserted = impl_->vendor_scopes.insert(
            std::move(pending->staged_vendor_scope));
        if (!inserted.inserted) {
            impl_->fatal = true;
            return CanonicalNormalizeCommitErrorV1::kNormalizerFatal;
        }
        pending->vendor_slot = &inserted.position->second;
    }
    if (!pending->staged_exchange_scope.empty()) {
        auto inserted = impl_->exchange_scopes.insert(
            std::move(pending->staged_exchange_scope));
        if (!inserted.inserted) {
            impl_->fatal = true;
            return CanonicalNormalizeCommitErrorV1::kNormalizerFatal;
        }
        pending->exchange_slot = &inserted.position->second;
    }
    if (!pending->staged_phase.empty()) {
        auto inserted = impl_->phases.insert(
            std::move(pending->staged_phase));
        if (!inserted.inserted) {
            impl_->fatal = true;
            return CanonicalNormalizeCommitErrorV1::kNormalizerFatal;
        }
        pending->phase_slot = &inserted.position->second;
    }

    if (pending->has_vendor_token &&
        pending->vendor_slot->guard->Commit(&pending->vendor_token) !=
            SequenceGuardCommitErrorV1::kNone) {
        impl_->fatal = true;
        return CanonicalNormalizeCommitErrorV1::kSequenceCommitFailure;
    }
    if (pending->has_exchange_token &&
        pending->exchange_slot->guard->Commit(&pending->exchange_token) !=
            SequenceGuardCommitErrorV1::kNone) {
        impl_->fatal = true;
        return CanonicalNormalizeCommitErrorV1::kSequenceCommitFailure;
    }

    if (pending->has_phase_update) {
        if (pending->phase_slot == nullptr) {
            impl_->fatal = true;
            return CanonicalNormalizeCommitErrorV1::kNormalizerFatal;
        }
        *pending->phase_slot = pending->phase_after_commit;
    }

    if (pending->vendor_poison_bit_after_commit != 0U) {
        pending->vendor_slot->poison_quality_bit =
            pending->vendor_poison_bit_after_commit;
        pending->vendor_slot->first_bad_origin_wal_end_pos =
            pending->vendor_first_bad_after_commit;
    }
    if (pending->exchange_poison_bit_after_commit != 0U) {
        pending->exchange_slot->poison_quality_bit =
            pending->exchange_poison_bit_after_commit;
        pending->exchange_slot->first_bad_origin_wal_end_pos =
            pending->exchange_first_bad_after_commit;
    }
    for (const auto& routed : pending->records) {
        const std::size_t family = FamilyIndex(routed.family);
        const std::uint64_t id = std::visit(
            [](const auto& value) { return value.header.shard_event_id; },
            routed.record);
        impl_->last_event_ids[family][routed.shard] = id;
        switch (routed.family) {
            case CanonicalFamilyV1::kSnapshot:
                ++impl_->snapshot_records_committed;
                break;
            case CanonicalFamilyV1::kTick:
                ++impl_->tick_records_committed;
                break;
            case CanonicalFamilyV1::kQuality:
                ++impl_->quality_records_committed;
                break;
            case CanonicalFamilyV1::kControl:
                ++impl_->control_records_committed;
                break;
        }
    }
    ++impl_->version;
    impl_->active_transaction = nullptr;
    pending->active = false;
    pending->owner = nullptr;
    transaction->impl_.reset();
    return CanonicalNormalizeCommitErrorV1::kNone;
}

bool CanonicalNormalizerV1::Abort(
    CanonicalNormalizationTransactionV1* transaction) noexcept {
    if (transaction == nullptr || transaction->impl_ == nullptr ||
        !transaction->impl_->active) {
        return false;
    }
    auto* pending = transaction->impl_.get();
    if (pending->owner != this ||
        impl_->active_transaction != pending) {
        return false;
    }
    const bool generation_reusable = !impl_->fatal;
    bool ok = true;
    if (pending->has_exchange_token) {
        ok = pending->exchange_slot->guard->Abort(
                 &pending->exchange_token) &&
             ok;
    }
    if (pending->has_vendor_token) {
        ok = pending->vendor_slot->guard->Abort(
                 &pending->vendor_token) &&
             ok;
    }
    impl_->active_transaction = nullptr;
    pending->active = false;
    pending->owner = nullptr;
    transaction->impl_.reset();
    return ok && generation_reusable;
}

bool CanonicalNormalizerV1::FailStop(
    CanonicalNormalizationTransactionV1* transaction) noexcept {
    impl_->fatal = true;
    if (transaction == nullptr) {
        return true;
    }
    if (transaction->impl_ == nullptr ||
        !transaction->impl_->active) {
        return false;
    }
    auto* pending = transaction->impl_.get();
    if (pending->owner != this ||
        impl_->active_transaction != pending) {
        return false;
    }
    bool ok = true;
    if (pending->has_exchange_token) {
        ok = pending->exchange_slot->guard->Abort(
                 &pending->exchange_token) &&
             ok;
    }
    if (pending->has_vendor_token) {
        ok = pending->vendor_slot->guard->Abort(
                 &pending->vendor_token) &&
             ok;
    }
    impl_->active_transaction = nullptr;
    pending->active = false;
    pending->owner = nullptr;
    transaction->impl_.reset();
    return ok;
}

CanonicalNormalizerSnapshotV1 CanonicalNormalizerV1::Snapshot()
    const noexcept {
    CanonicalNormalizerSnapshotV1 result{};
    result.version = impl_->version;
    result.vendor_scope_count = static_cast<std::uint64_t>(
        impl_->vendor_scopes.size());
    result.exchange_scope_count = static_cast<std::uint64_t>(
        impl_->exchange_scopes.size());
    result.phase_product_count = static_cast<std::uint64_t>(std::count_if(
        impl_->phases.begin(),
        impl_->phases.end(),
        [](const auto& entry) noexcept {
            return entry.second != TradingPhaseV1::kUnknown;
        }));
    result.snapshot_records_committed =
        impl_->snapshot_records_committed;
    result.tick_records_committed = impl_->tick_records_committed;
    result.quality_records_committed = impl_->quality_records_committed;
    result.control_records_committed = impl_->control_records_committed;
    result.transaction_active = impl_->active_transaction != nullptr;
    result.fatal = impl_->fatal;
    return result;
}

const CanonicalNormalizerConfigV1& CanonicalNormalizerV1::config()
    const noexcept {
    return impl_->config;
}

bool CanonicalShardForInstrumentV1(
    std::uint32_t instrument_id,
    std::uint32_t shard_count,
    std::uint32_t* shard) noexcept {
    if (instrument_id == 0U || shard_count == 0U || shard == nullptr) {
        return false;
    }
    *shard = instrument_id % shard_count;
    return true;
}

std::string_view CanonicalNormalizerCreateErrorNameV1(
    CanonicalNormalizerCreateErrorV1 error) noexcept {
    switch (error) {
        case CanonicalNormalizerCreateErrorV1::kNone:
            return "none";
        case CanonicalNormalizerCreateErrorV1::kNullOutput:
            return "null_output";
        case CanonicalNormalizerCreateErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case CanonicalNormalizerCreateErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

std::string_view CanonicalNormalizePrepareErrorNameV1(
    CanonicalNormalizePrepareErrorV1 error) noexcept {
    switch (error) {
        case CanonicalNormalizePrepareErrorV1::kNone:
            return "none";
        case CanonicalNormalizePrepareErrorV1::kNullTransaction:
            return "null_transaction";
        case CanonicalNormalizePrepareErrorV1::kTransactionStillActive:
            return "transaction_still_active";
        case CanonicalNormalizePrepareErrorV1::kNormalizerBusy:
            return "normalizer_busy";
        case CanonicalNormalizePrepareErrorV1::kNormalizerFatal:
            return "normalizer_fatal";
        case CanonicalNormalizePrepareErrorV1::kInvalidRawContext:
            return "invalid_raw_context";
        case CanonicalNormalizePrepareErrorV1::kInvalidMarketView:
            return "invalid_market_view";
        case CanonicalNormalizePrepareErrorV1::kInvalidDecodedEvent:
            return "invalid_decoded_event";
        case CanonicalNormalizePrepareErrorV1::kInvalidDecodedFailure:
            return "invalid_decoded_failure";
        case CanonicalNormalizePrepareErrorV1::kControlAdapterFailure:
            return "control_adapter_failure";
        case CanonicalNormalizePrepareErrorV1::kSequencePolicyMissing:
            return "sequence_policy_missing";
        case CanonicalNormalizePrepareErrorV1::kScopeCapacity:
            return "scope_capacity";
        case CanonicalNormalizePrepareErrorV1::kSequenceGuardFailure:
            return "sequence_guard_failure";
        case CanonicalNormalizePrepareErrorV1::kDecodeFailureRecorded:
            return "decode_failure_recorded";
        case CanonicalNormalizePrepareErrorV1::kCanonicalValidationFailure:
            return "canonical_validation_failure";
        case CanonicalNormalizePrepareErrorV1::kEventIdExhausted:
            return "event_id_exhausted";
        case CanonicalNormalizePrepareErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case CanonicalNormalizePrepareErrorV1::kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

std::string_view CanonicalNormalizeCommitErrorNameV1(
    CanonicalNormalizeCommitErrorV1 error) noexcept {
    switch (error) {
        case CanonicalNormalizeCommitErrorV1::kNone:
            return "none";
        case CanonicalNormalizeCommitErrorV1::kInvalidTransaction:
            return "invalid_transaction";
        case CanonicalNormalizeCommitErrorV1::kWrongNormalizer:
            return "wrong_normalizer";
        case CanonicalNormalizeCommitErrorV1::kVersionMismatch:
            return "version_mismatch";
        case CanonicalNormalizeCommitErrorV1::kPublicationMismatch:
            return "publication_mismatch";
        case CanonicalNormalizeCommitErrorV1::kSequenceCommitFailure:
            return "sequence_commit_failure";
        case CanonicalNormalizeCommitErrorV1::kNormalizerFatal:
            return "normalizer_fatal";
    }
    return "unknown";
}

}  // namespace l2flow::canonical
