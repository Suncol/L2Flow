#include "l2flow/canonical/canonical_schema_v1.h"

#include "l2flow/control/quality_flags_v1.h"

#include "mdl_api_msg.h"
#include "mdl_api_types.h"
#include "mdl_shl2_msg.h"
#include "mdl_szl2_msg.h"
#include "mdl_sys_msg.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <span>
#include <string_view>

namespace l2flow::canonical {
namespace {

namespace api = datayes::mdl::mdl_api_msg;
namespace sys = datayes::mdl::mdl_sys_msg;
namespace sh = datayes::mdl::mdl_shl2_msg;
namespace sz = datayes::mdl::mdl_szl2_msg;

using l2flow::control::QualityBit;
using l2flow::control::QualityFlagV1;

static_assert(datayes::mdl::MDLSID_MDL_API == 1U);
static_assert(datayes::mdl::MDLSID_MDL_SYS == 2U);
static_assert(api::MDLVID_MDL_API == 101U);
static_assert(sys::MDLVID_MDL_SYS == 101U);
static_assert(sh::SHL2MarketData::ServiceID == 4U);
static_assert(sh::SHL2MarketData::ServiceVer == 101U);
static_assert(sh::SHL2MarketData::MessageID == 4U);
static_assert(sh::NGTSTick::ServiceID == 4U);
static_assert(sh::NGTSTick::ServiceVer == 101U);
static_assert(sh::NGTSTick::MessageID == 24U);
static_assert(sz::Snapshot300111_v2::ServiceID == 6U);
static_assert(sz::Snapshot300111_v2::ServiceVer == 101U);
static_assert(sz::Snapshot300111_v2::MessageID == 28U);
static_assert(sz::Order300192_v2::ServiceID == 6U);
static_assert(sz::Order300192_v2::ServiceVer == 101U);
static_assert(sz::Order300192_v2::MessageID == 33U);
static_assert(sz::Transaction300191_v2::ServiceID == 6U);
static_assert(sz::Transaction300191_v2::ServiceVer == 101U);
static_assert(sz::Transaction300191_v2::MessageID == 36U);

constexpr std::string_view kSchemaDescriptor =
    "l2flow.canonical.schema.v1\n"
    "byte_order=little;alignment=natural-8;invalid_values=zero;"
    "reserved=zero\n"
    "magic=MCE1:0x3145434d;schema_version=1;"
    "quality_flags_mask=0x0000000fffffffff\n"
    "event_type=unknown:0,snapshot:1,tick:2,quality:3,control:4\n"
    "market=unknown:0,shanghai:1,shenzhen:2\n"
    "tick_action=unknown:0,add:1,cancel:2,trade:3,status:4\n"
    "side=unknown:0,buy:1,sell:2,borrow:3,lend:4\n"
    "order_type=unknown:0,market:1,limit:2,same_side_best:3\n"
    "aggressor=unknown:0,buy:1,sell:2,neutral:3\n"
    "quantity_unit=unknown:0,share:1,fund_unit:2,lot:3,"
    "bond_piece:4,index_unit:5\n"
    "phase=unknown:0,start:1,opening_call:2,continuous:3,"
    "suspended:4,closing_call:5,closed:6,end:7\n"
    "limit_semantics=unknown:0,finite:1,no_limit:2\n"
    "quality_type=unknown:0,vendor_gap:1,vendor_duplicate:2,"
    "vendor_conflict:3,exchange_gap:4,exchange_backward:5,"
    "exchange_conflict:6,decode_error:7,schema_unknown:8,"
    "scope_poisoned:9,source_state:10,clock_epoch_changed:11,"
    "snapshot_rejected:12,instrument_unknown:13,"
    "sequence_capacity_exhausted:14,normalization_rejected:15,"
    "exchange_duplicate:16\n"
    "quality_scope=unknown:0,stream:1,vendor_message:2,channel:3,"
    "instrument:4,snapshot_family:5,tick_family:6\n"
    "control_type=unknown:0,connecting:1,connect_error:2,"
    "disconnected:3,logon_success:4,logon_failure:5,"
    "subscription_accepted:6,subscription_rejected:7,"
    "service_status:8,session_status:9,decode_error:10\n"
    "tick_validity=price:0,quantity:1,trade_amount:2,"
    "matched_quantity:3,primary_order_id:4,buy_order_id:5,"
    "sell_order_id:6,exchange_time:7,side:8,order_type:9,"
    "aggressor:10,phase:11;business_flags_mask=0\n"
    "snapshot_scalar_validity=pre_close:0,open:1,high:2,low:3,"
    "last:4,close:5,volume:6,turnover:7,trade_count:8,"
    "total_bid_quantity:9,total_ask_quantity:10,weighted_bid:11,"
    "weighted_ask:12,high_limit:13,low_limit:14,iopv:15,"
    "exchange_time:16,raw_phase:17,normalized_phase:18,"
    "image_status:19,status_code:20,quantity_unit:21,"
    "actual_bid_depth:22,actual_ask_depth:23,"
    "bid1_total_order_count:24,bid1_revealed_count:25,"
    "ask1_total_order_count:26,ask1_revealed_count:27\n"
    "snapshot_flags=bid_depth_truncated_to_10:0,"
    "ask_depth_truncated_to_10:1;queue_count_max=50;"
    "depth_retained=10\n"
    "control_flags=response_manifest_hash:0,address_hash:1,"
    "error_text_hash:2,required_failure:3,optional_failure:4\n"
    "header_rules=origin_cursor_nonzero;origin_service_version_nonzero_"
    "except_control_decode_error;trade_date_nonzero;market_tick_require_"
    "instrument_and_vendor_sequence_and_sub_index_0;quality_requires_vendor_"
    "sequence_and_sub_index_1_to_3;recv_times_nonnegative;control_market_"
    "instrument_channel_exchange_fields_and_vendor_sequence_and_sub_index_"
    "zero\n"
    "business_origin=sh_snapshot:4/101/4+sh+channel0+exchange_sequence0;"
    "sh_tick:4/101/24+sh+exchange_sequence_positive;sz_snapshot:6/101/28+"
    "sz+exchange_sequence0;sz_order:6/101/33+sz+exchange_sequence_positive_"
    "i64;sz_transaction:6/101/36+sz+exchange_sequence_positive_i64;sh_tick_"
    "exchange_sequence_positive_i64;channel_is_"
    "opaque_u32;exchange_time_valid_iff_positive_else_zero\n"
    "tick_origin_validity=sh_add:price|quantity|matched_quantity|primary_order_"
    "id|exchange_time|side|phase;sh_cancel:quantity|primary_order_id|exchange_"
    "time|side|phase;sh_trade:price|quantity|trade_amount|buy_order_id|sell_"
    "order_id|exchange_time|aggressor|phase;sh_status:exchange_time|phase;sz_"
    "order:price|quantity|primary_order_id|exchange_time|side|order_type;sz_"
    "trade:price|quantity|buy_order_id|sell_order_id|exchange_time;sz_cancel:"
    "quantity|primary_order_id|buy_order_id|sell_order_id|exchange_time|side\n"
    "tick_action_required=add:quantity;cancel:quantity;trade:quantity;"
    "status:none;tick_absolute_nonnegative=price,quantity,trade_amount,"
    "matched_quantity,exchange_time;order_ids=positive_when_valid\n"
    "tick_source_enum=slots_are_u16;unrepresentable_or_unavailable_is_0;"
    "unused_slots_zero;sh:action_ascii_A_D_T_S+single_byte_B_S_N_or_0_with_"
    "status_flag_0;sz_order:exact_u16_or_0_side+order_type_with_action_add_"
    "and_primary_order_id_equal_exchange_sequence+price_only_for_limit;sz_"
    "transaction:70_trade_or_52_cancel+cancel_primary_and_side_iff_exactly_one_"
    "order_id_present\n"
    "snapshot_absolute_nonnegative=all_price,volume,turnover,trade_count,"
    "total_quantity,book_price,book_quantity,queue_quantity,exchange_time;"
    "actual_depth_and_queue_counts_always_valid;order_count_validity_equals_"
    "retained_depth;queue_revealed_max=50_and_not_above_total;depth0_has_"
    "zero_counts_and_queue;depth_positive_total_equals_level0_order_count;"
    "sh_queue_validity_may_have_nullable_holes;sz_queue_validity_equals_"
    "revealed_prefix\n"
    "quality_scope_matrix=vendor_gap|vendor_duplicate|vendor_conflict|"
    "decode_error|schema_unknown:vendor_message;exchange_gap|exchange_"
    "backward|exchange_conflict|exchange_duplicate|normalization_rejected:"
    "channel;source_state|clock_epoch_changed:stream;snapshot_rejected:"
    "snapshot_family;instrument_unknown:instrument;scope_poisoned|sequence_"
    "capacity_exhausted:vendor_message|channel;source_state|clock_epoch_"
    "changed=unreachable_rejected_v1;quality_first_bad_wal=nonzero_and_not_"
    "after_origin;quality_related_epoch=header_epoch;quality_human_code=0;"
    "quality_hash=scope_poisoned:zero|all_other_reachable:nonzero;channel_scope_"
    "requires_known_market_with_opaque_u32_channel;snapshot_scope_requires_"
    "market;scope_id="
    "stream:source_stream_id|vendor:(service_id<<32)|message_id|channel:"
    "(shanghai:2|shenzhen:3)<<32|channel|snapshot:(1<<32)|market|"
    "instrument_unknown:0;scope_type_is_part_of_identity;scope_id_is_not_"
    "globally_unique;full_scope_also_uses_source_and_segment_capture_or_"
    "trade_date_and_stream_day_identity;vendor_actual=header_vendor_sequence;channel_actual="
    "header_exchange_sequence;sequence_gap_expected_lt_actual;sequence_"
    "backward_expected_gt_actual;duplicate_or_conflict_expected_ge_actual;"
    "capacity_actual_positive_and_expected_le_actual;poison_actual_positive;"
    "decode_or_schema_expected0_actual_positive;normalization_expected0;"
    "snapshot_rejected_or_instrument_unknown_expected0_actual0;"
    "quality_sub_index=vendor_sequence:1|exchange_sequence:2|decode_or_"
    "rejection:3;sequence_detail=gap3|duplicate4|conflict5|backward6|poison7|"
    "capacity8;decode_detail=invalid3|unsupported_message4|unsupported_"
    "version5|truncated6|offset7|overlap8|count_exceeded9|count_mismatch10|"
    "text11|fixed_overflow12;required_quality_bits=vendor_gap|duplicate|"
    "conflict:matching_vendor_bit;exchange_gap|backward|conflict:matching_"
    "exchange_bit;poison|capacity:scope_conflict_bit;decode_detail6:decode_"
    "truncated|7_or_8:decode_offset|11:decode_text;schema_unknown:schema_"
    "unknown;instrument_unknown:instrument_unknown;no_required_bit=exchange_"
    "duplicate|decode_detail3_9_10_12|normalization_rejected|snapshot_"
    "rejected;quality_origin=decode:recognized_core101|schema_detail4:"
    "unrecognized_tuple|schema_detail5:recognized_tuple_non101|exchange_"
    "sequence_or_normalization:tick_core101|snapshot_rejected:snapshot_"
    "core101|instrument_unknown:any_core101\n"
    "control_projection=connecting:address;connect_error|disconnected:"
    "address+error;logon_success|logon_failure|subscription_accepted|"
    "subscription_rejected:response;service_status|session_status:none;"
    "decode_error:code_nonzero_no_hash;required_count=1..64_total;"
    "logon|subscription_response_entries_max=1000000;service|session_"
    "response_entries_max=4096;control_quality_min=connecting|connect_error|"
    "disconnected|logon_failure|decode_error:session_unknown;connect_error|"
    "disconnected:source_disconnected_or_decode_diagnosis;decode_error:"
    "precise_schema_or_truncated_or_offset_bit\n"
    "control_origin_identity=api101:connecting|connect_error|disconnected;"
    "sys101:logon|subscription|service_status|session_status;decode_error:"
    "recognized_message+code_range+version_reachability\n"
    "record_sizes=header:112,tick_payload:80,tick:192,"
    "snapshot_payload:1936,snapshot:2048,quality_payload:80,"
    "quality:192,canonical_control_payload:144,"
    "canonical_control:256\n"
    "clock_epoch_identity=algorithm+sha256;label=nonidentity-index\n";

constexpr std::string_view kDtypeDescriptor =
    "l2flow.canonical.dtype.v1|endian=<|align=8|"
    "header[112]{magic:u4@0,schema_version:u2@4,event_type:u2@6,"
    "record_size:u4@8,source_stream_id:u4@12,connection_epoch:u4@16,"
    "trade_date:u4@20,quality_flags:u8@24,shard_event_id:u8@32,"
    "origin_ingress_sequence:u8@40,origin_wal_end_pos:u8@48,"
    "vendor_sequence_id:u8@56,exchange_sequence:u8@64,"
    "exchange_time_ns:i8@72,recv_realtime_ns:i8@80,"
    "recv_monotonic_ns:i8@88,instrument_id:u4@96,channel:u4@100,"
    "market:u2@104,origin_service_version:u2@106,"
    "origin_message_id:u2@108,origin_service_id:u1@110,"
    "sub_index:u1@111}|"
    "tick_payload[80]{price_p6:i8@0,quantity_native:i8@8,"
    "trade_amount_p6:i8@16,matched_quantity_native:i8@24,"
    "primary_order_id:i8@32,buy_order_id:i8@40,sell_order_id:i8@48,"
    "validity_bitmap:u4@56,business_flags:u4@60,"
    "source_enum_bits:u8@64,action:u1@72,side:u1@73,"
    "order_type:u1@74,aggressor:u1@75,quantity_unit:u1@76,"
    "phase:u1@77,reserved:V2@78}|tick_record[192]{header:@0,"
    "payload:@112}|"
    "snapshot_payload[1936]{pre_close_price_p6:i8@0,open_price_p6:i8@8,"
    "high_price_p6:i8@16,low_price_p6:i8@24,last_price_p6:i8@32,"
    "close_price_p6:i8@40,volume_native:i8@48,turnover_p6:i8@56,"
    "trade_count:i8@64,total_bid_quantity_native:i8@72,"
    "total_ask_quantity_native:i8@80,weighted_bid_price_p6:i8@88,"
    "weighted_ask_price_p6:i8@96,high_limit_price_p6:i8@104,"
    "low_limit_price_p6:i8@112,iopv_p6:i8@120,"
    "bid_price_p6:(10)i8@128,bid_quantity_native:(10)i8@208,"
    "ask_price_p6:(10)i8@288,ask_quantity_native:(10)i8@368,"
    "bid1_queue_quantity_native:(50)i8@448,"
    "ask1_queue_quantity_native:(50)i8@848,"
    "bid_order_count:(10)u4@1248,ask_order_count:(10)u4@1288,"
    "raw_phase_bits:u8@1328,status_code_bits:u8@1336,"
    "scalar_validity:u8@1344,bid_queue_validity:u8@1352,"
    "ask_queue_validity:u8@1360,snapshot_flags:u4@1368,"
    "image_status:u4@1372,actual_bid_depth:u4@1376,"
    "actual_ask_depth:u4@1380,bid1_total_order_count:u4@1384,"
    "bid1_revealed_count:u4@1388,ask1_total_order_count:u4@1392,"
    "ask1_revealed_count:u4@1396,bid_price_validity:u2@1400,"
    "bid_quantity_validity:u2@1402,bid_order_count_validity:u2@1404,"
    "ask_price_validity:u2@1406,ask_quantity_validity:u2@1408,"
    "ask_order_count_validity:u2@1410,phase:u1@1412,"
    "quantity_unit:u1@1413,high_limit_semantics:u1@1414,"
    "low_limit_semantics:u1@1415,reserved:V520@1416}|"
    "snapshot_record[2048]{header:@0,payload:@112}|"
    "quality_payload[80]{expected_sequence:u8@0,actual_sequence:u8@8,"
    "first_bad_origin_wal_end_pos:u8@16,scope_id:u8@24,"
    "payload_sha256:V32@32,related_connection_epoch:u4@64,"
    "quality_type:u2@68,scope_type:u2@70,detail_code:u2@72,"
    "human_code_id:u2@74,reserved:V4@76}|"
    "quality_record[192]{header:@0,payload:@112}|"
    "canonical_control_payload[144]{response_manifest_sha256:V32@0,"
    "address_sha256:V32@32,error_text_sha256:V32@64,"
    "return_or_error_code:u4@96,connection_epoch:u4@100,"
    "subscription_epoch:u4@104,required_count:u4@108,"
    "required_ok_count:u4@112,required_failed_count:u4@116,"
    "optional_count:u4@120,optional_ok_count:u4@124,"
    "optional_failed_count:u4@128,response_entry_count:u4@132,"
    "control_type:u2@136,flags:u2@138,reserved:V4@140}|"
    "canonical_control_record[256]{header:@0,payload:@112}";

[[nodiscard]] constexpr bool EventTypeKnown(
    CanonicalEventTypeV1 value) noexcept {
    switch (value) {
        case CanonicalEventTypeV1::kSnapshot:
        case CanonicalEventTypeV1::kTick:
        case CanonicalEventTypeV1::kQuality:
        case CanonicalEventTypeV1::kControl:
            return true;
        case CanonicalEventTypeV1::kUnknown:
            return false;
    }
    return false;
}

[[nodiscard]] constexpr bool MarketKnown(
    CanonicalMarketV1 value) noexcept {
    switch (value) {
        case CanonicalMarketV1::kUnknown:
        case CanonicalMarketV1::kShanghai:
        case CanonicalMarketV1::kShenzhen:
            return true;
    }
    return false;
}

enum class BusinessOriginKindV1 : std::uint8_t {
    kUnknown = 0U,
    kShanghaiSnapshot,
    kShanghaiTick,
    kShenzhenSnapshot,
    kShenzhenOrder,
    kShenzhenTransaction,
};

[[nodiscard]] constexpr bool RecognizedCoreMessageTuple(
    const CanonicalHeaderV1& header) noexcept {
    return (header.origin_service_id ==
                sh::SHL2MarketData::ServiceID &&
            (header.origin_message_id ==
                 sh::SHL2MarketData::MessageID ||
             header.origin_message_id == sh::NGTSTick::MessageID)) ||
           (header.origin_service_id ==
                sz::Snapshot300111_v2::ServiceID &&
            (header.origin_message_id ==
                 sz::Snapshot300111_v2::MessageID ||
             header.origin_message_id ==
                 sz::Order300192_v2::MessageID ||
             header.origin_message_id ==
                 sz::Transaction300191_v2::MessageID));
}

[[nodiscard]] constexpr BusinessOriginKindV1 BusinessOriginKind(
    const CanonicalHeaderV1& header) noexcept {
    if (header.origin_service_version != 101U) {
        return BusinessOriginKindV1::kUnknown;
    }
    if (header.origin_service_id == sh::SHL2MarketData::ServiceID) {
        if (header.origin_message_id == sh::SHL2MarketData::MessageID) {
            return BusinessOriginKindV1::kShanghaiSnapshot;
        }
        if (header.origin_message_id == sh::NGTSTick::MessageID) {
            return BusinessOriginKindV1::kShanghaiTick;
        }
        return BusinessOriginKindV1::kUnknown;
    }
    if (header.origin_service_id == sz::Snapshot300111_v2::ServiceID) {
        switch (header.origin_message_id) {
            case sz::Snapshot300111_v2::MessageID:
                return BusinessOriginKindV1::kShenzhenSnapshot;
            case sz::Order300192_v2::MessageID:
                return BusinessOriginKindV1::kShenzhenOrder;
            case sz::Transaction300191_v2::MessageID:
                return BusinessOriginKindV1::kShenzhenTransaction;
            default:
                return BusinessOriginKindV1::kUnknown;
        }
    }
    return BusinessOriginKindV1::kUnknown;
}

[[nodiscard]] constexpr bool TickActionKnown(
    CanonicalTickActionV1 value) noexcept {
    switch (value) {
        case CanonicalTickActionV1::kAdd:
        case CanonicalTickActionV1::kCancel:
        case CanonicalTickActionV1::kTrade:
        case CanonicalTickActionV1::kStatus:
            return true;
        case CanonicalTickActionV1::kUnknown:
            return false;
    }
    return false;
}

[[nodiscard]] constexpr bool SideRecognized(CanonicalSideV1 value) noexcept {
    switch (value) {
        case CanonicalSideV1::kUnknown:
        case CanonicalSideV1::kBuy:
        case CanonicalSideV1::kSell:
        case CanonicalSideV1::kBorrow:
        case CanonicalSideV1::kLend:
            return true;
    }
    return false;
}

[[nodiscard]] constexpr bool OrderTypeRecognized(
    CanonicalOrderTypeV1 value) noexcept {
    switch (value) {
        case CanonicalOrderTypeV1::kUnknown:
        case CanonicalOrderTypeV1::kMarket:
        case CanonicalOrderTypeV1::kLimit:
        case CanonicalOrderTypeV1::kSameSideBest:
            return true;
    }
    return false;
}

[[nodiscard]] constexpr bool AggressorRecognized(
    CanonicalAggressorV1 value) noexcept {
    switch (value) {
        case CanonicalAggressorV1::kUnknown:
        case CanonicalAggressorV1::kBuy:
        case CanonicalAggressorV1::kSell:
        case CanonicalAggressorV1::kNeutral:
            return true;
    }
    return false;
}

[[nodiscard]] constexpr bool QuantityUnitRecognized(
    CanonicalQuantityUnitV1 value) noexcept {
    switch (value) {
        case CanonicalQuantityUnitV1::kUnknown:
        case CanonicalQuantityUnitV1::kShare:
        case CanonicalQuantityUnitV1::kFundUnit:
        case CanonicalQuantityUnitV1::kLot:
        case CanonicalQuantityUnitV1::kBondPiece:
        case CanonicalQuantityUnitV1::kIndexUnit:
            return true;
    }
    return false;
}

[[nodiscard]] constexpr bool PhaseRecognized(
    CanonicalTradingPhaseV1 value) noexcept {
    switch (value) {
        case CanonicalTradingPhaseV1::kUnknown:
        case CanonicalTradingPhaseV1::kStart:
        case CanonicalTradingPhaseV1::kOpeningCall:
        case CanonicalTradingPhaseV1::kContinuous:
        case CanonicalTradingPhaseV1::kSuspended:
        case CanonicalTradingPhaseV1::kClosingCall:
        case CanonicalTradingPhaseV1::kClosed:
        case CanonicalTradingPhaseV1::kEnd:
            return true;
    }
    return false;
}

[[nodiscard]] constexpr bool LimitSemanticsRecognized(
    CanonicalLimitSemanticsV1 value) noexcept {
    switch (value) {
        case CanonicalLimitSemanticsV1::kUnknown:
        case CanonicalLimitSemanticsV1::kFinite:
        case CanonicalLimitSemanticsV1::kNoLimit:
            return true;
    }
    return false;
}

[[nodiscard]] constexpr bool QualityTypeKnown(
    CanonicalQualityTypeV1 value) noexcept {
    switch (value) {
        case CanonicalQualityTypeV1::kVendorSequenceGap:
        case CanonicalQualityTypeV1::kVendorSequenceDuplicate:
        case CanonicalQualityTypeV1::kVendorSequenceConflict:
        case CanonicalQualityTypeV1::kExchangeSequenceGap:
        case CanonicalQualityTypeV1::kExchangeSequenceBackward:
        case CanonicalQualityTypeV1::kExchangeSequenceConflict:
        case CanonicalQualityTypeV1::kDecodeError:
        case CanonicalQualityTypeV1::kSchemaUnknown:
        case CanonicalQualityTypeV1::kScopePoisoned:
        case CanonicalQualityTypeV1::kSourceState:
        case CanonicalQualityTypeV1::kClockEpochChanged:
        case CanonicalQualityTypeV1::kSnapshotRejected:
        case CanonicalQualityTypeV1::kInstrumentUnknown:
        case CanonicalQualityTypeV1::kSequenceCapacityExhausted:
        case CanonicalQualityTypeV1::kNormalizationRejected:
        case CanonicalQualityTypeV1::kExchangeSequenceDuplicate:
            return true;
        case CanonicalQualityTypeV1::kUnknown:
            return false;
    }
    return false;
}

[[nodiscard]] constexpr bool QualityScopeKnown(
    CanonicalQualityScopeV1 value) noexcept {
    switch (value) {
        case CanonicalQualityScopeV1::kStream:
        case CanonicalQualityScopeV1::kVendorMessage:
        case CanonicalQualityScopeV1::kChannel:
        case CanonicalQualityScopeV1::kInstrument:
        case CanonicalQualityScopeV1::kSnapshotFamily:
        case CanonicalQualityScopeV1::kTickFamily:
            return true;
        case CanonicalQualityScopeV1::kUnknown:
            return false;
    }
    return false;
}

[[nodiscard]] constexpr bool ControlTypeKnown(
    CanonicalControlTypeV1 value) noexcept {
    switch (value) {
        case CanonicalControlTypeV1::kConnecting:
        case CanonicalControlTypeV1::kConnectError:
        case CanonicalControlTypeV1::kDisconnected:
        case CanonicalControlTypeV1::kLogonSuccess:
        case CanonicalControlTypeV1::kLogonFailure:
        case CanonicalControlTypeV1::kSubscriptionAccepted:
        case CanonicalControlTypeV1::kSubscriptionRejected:
        case CanonicalControlTypeV1::kServiceStatus:
        case CanonicalControlTypeV1::kSessionStatus:
        case CanonicalControlTypeV1::kDecodeError:
            return true;
        case CanonicalControlTypeV1::kUnknown:
            return false;
    }
    return false;
}

[[nodiscard]] bool ControlOriginIdentityMatchesType(
    const CanonicalHeaderV1& header,
    const CanonicalControlPayloadV1& payload) noexcept {
    const bool api_identity =
        header.origin_service_id == datayes::mdl::MDLSID_MDL_API;
    const bool sys_identity =
        header.origin_service_id == datayes::mdl::MDLSID_MDL_SYS;
    switch (payload.control_type) {
        case CanonicalControlTypeV1::kConnecting:
            return api_identity &&
                   header.origin_service_version == api::MDLVID_MDL_API &&
                   header.origin_message_id ==
                       api::MDLMID_MDL_API_ConnectingEvent;
        case CanonicalControlTypeV1::kConnectError:
            return api_identity &&
                   header.origin_service_version == api::MDLVID_MDL_API &&
                   header.origin_message_id ==
                       api::MDLMID_MDL_API_ConnectErrorEvent;
        case CanonicalControlTypeV1::kDisconnected:
            return api_identity &&
                   header.origin_service_version == api::MDLVID_MDL_API &&
                   header.origin_message_id ==
                       api::MDLMID_MDL_API_DisconnectedEvent;
        case CanonicalControlTypeV1::kLogonSuccess:
        case CanonicalControlTypeV1::kLogonFailure:
            return sys_identity &&
                   header.origin_service_version == sys::MDLVID_MDL_SYS &&
                   header.origin_message_id ==
                       sys::MDLMID_MDL_SYS_LogonResponse;
        case CanonicalControlTypeV1::kSubscriptionAccepted:
        case CanonicalControlTypeV1::kSubscriptionRejected:
            return sys_identity &&
                   header.origin_service_version == sys::MDLVID_MDL_SYS &&
                   header.origin_message_id ==
                       sys::MDLMID_MDL_SYS_SubscribeResponse;
        case CanonicalControlTypeV1::kServiceStatus:
            return sys_identity &&
                   header.origin_service_version == sys::MDLVID_MDL_SYS &&
                   header.origin_message_id ==
                       sys::MDLMID_MDL_SYS_ServiceStatus;
        case CanonicalControlTypeV1::kSessionStatus:
            return sys_identity &&
                   header.origin_service_version == sys::MDLVID_MDL_SYS &&
                   header.origin_message_id ==
                       sys::MDLMID_MDL_SYS_SessionStatus;
        case CanonicalControlTypeV1::kDecodeError: {
            const std::uint32_t code = payload.return_or_error_code;
            const std::uint8_t local = static_cast<std::uint8_t>(code);
            const bool version_matches = local == 3U
                ? header.origin_service_version != 101U
                : header.origin_service_version == 101U;
            if (!version_matches) {
                return false;
            }
            if (api_identity && code >= 0x0103U && code <= 0x0106U) {
                return header.origin_message_id ==
                           api::MDLMID_MDL_API_ConnectingEvent ||
                       header.origin_message_id ==
                           api::MDLMID_MDL_API_ConnectErrorEvent ||
                       header.origin_message_id ==
                           api::MDLMID_MDL_API_DisconnectedEvent;
            }
            if (sys_identity && code >= 0x0203U && code <= 0x0208U) {
                return header.origin_message_id ==
                           sys::MDLMID_MDL_SYS_LogonResponse ||
                       header.origin_message_id ==
                           sys::MDLMID_MDL_SYS_SubscribeResponse ||
                       header.origin_message_id ==
                           sys::MDLMID_MDL_SYS_ServiceStatus ||
                       header.origin_message_id ==
                           sys::MDLMID_MDL_SYS_SessionStatus;
            }
            return false;
        }
        case CanonicalControlTypeV1::kUnknown:
            return false;
    }
    return false;
}

[[nodiscard]] constexpr std::uint32_t ExpectedRecordSize(
    CanonicalEventTypeV1 event_type) noexcept {
    switch (event_type) {
        case CanonicalEventTypeV1::kSnapshot:
            return static_cast<std::uint32_t>(
                kCanonicalSnapshotRecordBytesV1);
        case CanonicalEventTypeV1::kTick:
            return static_cast<std::uint32_t>(kCanonicalTickRecordBytesV1);
        case CanonicalEventTypeV1::kQuality:
            return static_cast<std::uint32_t>(
                kCanonicalQualityRecordBytesV1);
        case CanonicalEventTypeV1::kControl:
            return static_cast<std::uint32_t>(
                kCanonicalControlRecordBytesV1);
        case CanonicalEventTypeV1::kUnknown:
            return 0U;
    }
    return 0U;
}

[[nodiscard]] bool DigestNonzero(
    const l2flow::common::Sha256Digest& digest) noexcept {
    return std::any_of(
        digest.begin(), digest.end(), [](std::byte byte) {
            return byte != std::byte{0U};
        });
}

template <std::size_t Size>
[[nodiscard]] bool BytesAllZero(
    const std::array<std::byte, Size>& bytes) noexcept {
    return std::all_of(bytes.begin(), bytes.end(), [](std::byte byte) {
        return byte == std::byte{0U};
    });
}

[[nodiscard]] constexpr bool BitSet(
    std::uint32_t mask,
    CanonicalTickValidityV1 bit) noexcept {
    return (mask & CanonicalTickValidityBitV1(bit)) != 0U;
}

[[nodiscard]] constexpr bool BitSet(
    std::uint64_t mask,
    CanonicalSnapshotScalarValidityV1 bit) noexcept {
    return (mask & CanonicalSnapshotValidityBitV1(bit)) != 0U;
}

[[nodiscard]] constexpr std::uint16_t SourceEnumSlot(
    std::uint64_t bits,
    std::size_t slot) noexcept {
    return static_cast<std::uint16_t>(bits >> (slot * 16U));
}

[[nodiscard]] constexpr CanonicalSideV1 ShenzhenSideFromRaw(
    std::uint16_t raw) noexcept {
    switch (raw) {
        case 49U:
            return CanonicalSideV1::kBuy;
        case 50U:
            return CanonicalSideV1::kSell;
        case 71U:
            return CanonicalSideV1::kBorrow;
        case 70U:
            return CanonicalSideV1::kLend;
        default:
            return CanonicalSideV1::kUnknown;
    }
}

[[nodiscard]] constexpr CanonicalOrderTypeV1 ShenzhenOrderTypeFromRaw(
    std::uint16_t raw) noexcept {
    switch (raw) {
        case 49U:
            return CanonicalOrderTypeV1::kMarket;
        case 50U:
            return CanonicalOrderTypeV1::kLimit;
        case 85U:
            return CanonicalOrderTypeV1::kSameSideBest;
        default:
            return CanonicalOrderTypeV1::kUnknown;
    }
}

[[nodiscard]] bool TickOriginProjectionValid(
    BusinessOriginKindV1 origin,
    const CanonicalHeaderV1& header,
    const CanonicalTickPayloadV1& payload) noexcept {
    const std::uint16_t first = SourceEnumSlot(
        payload.source_enum_bits, 0U);
    const std::uint16_t second = SourceEnumSlot(
        payload.source_enum_bits, 1U);
    const std::uint16_t third = SourceEnumSlot(
        payload.source_enum_bits, 2U);
    const std::uint16_t fourth = SourceEnumSlot(
        payload.source_enum_bits, 3U);
    if (header.sub_index != 0U || header.exchange_sequence == 0U ||
        header.exchange_sequence >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::int64_t>::max()) ||
        third != 0U || fourth != 0U) {
        return false;
    }
    const auto validity_is_subset_of =
        [&](std::initializer_list<CanonicalTickValidityV1> allowed) noexcept {
            std::uint32_t mask = 0U;
            for (const CanonicalTickValidityV1 bit : allowed) {
                mask |= CanonicalTickValidityBitV1(bit);
            }
            return (payload.validity_bitmap & ~mask) == 0U;
        };
    const bool price_valid = BitSet(
        payload.validity_bitmap, CanonicalTickValidityV1::kPrice);
    const bool primary_valid = BitSet(
        payload.validity_bitmap, CanonicalTickValidityV1::kPrimaryOrderId);
    const bool buy_valid = BitSet(
        payload.validity_bitmap, CanonicalTickValidityV1::kBuyOrderId);
    const bool sell_valid = BitSet(
        payload.validity_bitmap, CanonicalTickValidityV1::kSellOrderId);
    const bool side_valid = BitSet(
        payload.validity_bitmap, CanonicalTickValidityV1::kSide);
    switch (origin) {
        case BusinessOriginKindV1::kShanghaiTick: {
            if (header.market != CanonicalMarketV1::kShanghai) {
                return false;
            }
            std::uint16_t required_action = 0U;
            switch (payload.action) {
                case CanonicalTickActionV1::kAdd:
                    required_action = static_cast<std::uint16_t>('A');
                    if (first != required_action ||
                        !validity_is_subset_of({
                            CanonicalTickValidityV1::kPrice,
                            CanonicalTickValidityV1::kQuantity,
                            CanonicalTickValidityV1::kMatchedQuantity,
                            CanonicalTickValidityV1::kPrimaryOrderId,
                            CanonicalTickValidityV1::kExchangeTime,
                            CanonicalTickValidityV1::kSide,
                            CanonicalTickValidityV1::kPhase}) ||
                        (primary_valid && !side_valid)) {
                        return false;
                    }
                    if (second == static_cast<std::uint16_t>('B')) {
                        return payload.side == CanonicalSideV1::kBuy;
                    }
                    if (second == static_cast<std::uint16_t>('S')) {
                        return payload.side == CanonicalSideV1::kSell;
                    }
                    return second == 0U &&
                           payload.side == CanonicalSideV1::kUnknown;
                case CanonicalTickActionV1::kCancel:
                    required_action = static_cast<std::uint16_t>('D');
                    if (first != required_action ||
                        !validity_is_subset_of({
                            CanonicalTickValidityV1::kQuantity,
                            CanonicalTickValidityV1::kPrimaryOrderId,
                            CanonicalTickValidityV1::kExchangeTime,
                            CanonicalTickValidityV1::kSide,
                            CanonicalTickValidityV1::kPhase}) ||
                        (primary_valid && !side_valid)) {
                        return false;
                    }
                    if (second == static_cast<std::uint16_t>('B')) {
                        return payload.side == CanonicalSideV1::kBuy;
                    }
                    if (second == static_cast<std::uint16_t>('S')) {
                        return payload.side == CanonicalSideV1::kSell;
                    }
                    return second == 0U &&
                           payload.side == CanonicalSideV1::kUnknown;
                case CanonicalTickActionV1::kTrade:
                    required_action = static_cast<std::uint16_t>('T');
                    if (first != required_action ||
                        !validity_is_subset_of({
                            CanonicalTickValidityV1::kPrice,
                            CanonicalTickValidityV1::kQuantity,
                            CanonicalTickValidityV1::kTradeAmount,
                            CanonicalTickValidityV1::kBuyOrderId,
                            CanonicalTickValidityV1::kSellOrderId,
                            CanonicalTickValidityV1::kExchangeTime,
                            CanonicalTickValidityV1::kAggressor,
                            CanonicalTickValidityV1::kPhase})) {
                        return false;
                    }
                    if (second == static_cast<std::uint16_t>('B')) {
                        return payload.aggressor ==
                               CanonicalAggressorV1::kBuy;
                    }
                    if (second == static_cast<std::uint16_t>('S')) {
                        return payload.aggressor ==
                               CanonicalAggressorV1::kSell;
                    }
                    if (second == static_cast<std::uint16_t>('N')) {
                        return payload.aggressor ==
                               CanonicalAggressorV1::kNeutral;
                    }
                    return second == 0U &&
                           payload.aggressor ==
                               CanonicalAggressorV1::kUnknown;
                case CanonicalTickActionV1::kStatus:
                    return first == static_cast<std::uint16_t>('S') &&
                           second == 0U &&
                           validity_is_subset_of({
                               CanonicalTickValidityV1::kExchangeTime,
                               CanonicalTickValidityV1::kPhase});
                case CanonicalTickActionV1::kUnknown:
                    return false;
            }
            return false;
        }
        case BusinessOriginKindV1::kShenzhenOrder:
            return header.market == CanonicalMarketV1::kShenzhen &&
                   payload.action == CanonicalTickActionV1::kAdd &&
                   validity_is_subset_of({
                       CanonicalTickValidityV1::kPrice,
                       CanonicalTickValidityV1::kQuantity,
                       CanonicalTickValidityV1::kPrimaryOrderId,
                       CanonicalTickValidityV1::kExchangeTime,
                       CanonicalTickValidityV1::kSide,
                       CanonicalTickValidityV1::kOrderType}) &&
                   (!price_valid || second == 50U) &&
                   payload.side == ShenzhenSideFromRaw(first) &&
                   payload.order_type ==
                       ShenzhenOrderTypeFromRaw(second) &&
                   BitSet(
                       payload.validity_bitmap,
                       CanonicalTickValidityV1::kPrimaryOrderId) &&
                   payload.primary_order_id > 0 &&
                   static_cast<std::uint64_t>(
                       payload.primary_order_id) ==
                       header.exchange_sequence;
        case BusinessOriginKindV1::kShenzhenTransaction: {
            if (header.market != CanonicalMarketV1::kShenzhen ||
                second != 0U) {
                return false;
            }
            if (first == 70U &&
                payload.action == CanonicalTickActionV1::kTrade) {
                return validity_is_subset_of({
                    CanonicalTickValidityV1::kPrice,
                    CanonicalTickValidityV1::kQuantity,
                    CanonicalTickValidityV1::kBuyOrderId,
                    CanonicalTickValidityV1::kSellOrderId,
                    CanonicalTickValidityV1::kExchangeTime});
            }
            if (first != 52U ||
                payload.action != CanonicalTickActionV1::kCancel ||
                !validity_is_subset_of({
                    CanonicalTickValidityV1::kQuantity,
                    CanonicalTickValidityV1::kPrimaryOrderId,
                    CanonicalTickValidityV1::kBuyOrderId,
                    CanonicalTickValidityV1::kSellOrderId,
                    CanonicalTickValidityV1::kExchangeTime,
                    CanonicalTickValidityV1::kSide})) {
                return false;
            }
            if (buy_valid != sell_valid) {
                return primary_valid && side_valid &&
                       (buy_valid
                            ? (payload.primary_order_id ==
                                   payload.buy_order_id &&
                               payload.side == CanonicalSideV1::kBuy)
                            : (payload.primary_order_id ==
                                   payload.sell_order_id &&
                               payload.side == CanonicalSideV1::kSell));
            }
            return !primary_valid && !side_valid &&
                   payload.side == CanonicalSideV1::kUnknown;
        }
        case BusinessOriginKindV1::kShanghaiSnapshot:
        case BusinessOriginKindV1::kShenzhenSnapshot:
        case BusinessOriginKindV1::kUnknown:
            return false;
    }
    return false;
}

template <typename Integer>
[[nodiscard]] constexpr bool ValueMatchesValidity(
    Integer value,
    bool valid) noexcept {
    return valid || value == Integer{0};
}

[[nodiscard]] constexpr std::uint16_t AllowedLevelValidity(
    std::uint32_t actual_depth) noexcept {
    if (actual_depth >= kCanonicalDepthLevelsV1) {
        return kCanonicalLevelValidityMaskV1;
    }
    if (actual_depth == 0U) {
        return 0U;
    }
    return static_cast<std::uint16_t>(
        (std::uint16_t{1U} << actual_depth) - 1U);
}

[[nodiscard]] constexpr std::uint64_t AllowedQueueValidity(
    std::uint32_t revealed_count) noexcept {
    if (revealed_count >= kCanonicalQueueEntriesV1) {
        return kCanonicalQueueValidityMaskV1;
    }
    if (revealed_count == 0U) {
        return 0U;
    }
    return (std::uint64_t{1U} << revealed_count) - 1U;
}

template <typename Integer, std::size_t Size>
[[nodiscard]] bool ArrayMatchesValidity(
    const std::array<Integer, Size>& values,
    std::uint64_t validity) noexcept {
    for (std::size_t index = 0U; index < Size; ++index) {
        const bool valid =
            (validity & (std::uint64_t{1U} << index)) != 0U;
        if (!ValueMatchesValidity(values[index], valid)) {
            return false;
        }
    }
    return true;
}

template <typename Integer, std::size_t Size>
[[nodiscard]] bool ValidArrayValuesAreNonnegative(
    const std::array<Integer, Size>& values,
    std::uint64_t validity) noexcept {
    for (std::size_t index = 0U; index < Size; ++index) {
        if ((validity & (std::uint64_t{1U} << index)) != 0U &&
            values[index] < Integer{0}) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] CanonicalValidationErrorV1 ValidateExpectedHeader(
    const CanonicalHeaderV1& header,
    CanonicalEventTypeV1 event_type) noexcept {
    const CanonicalValidationErrorV1 common =
        ValidateCanonicalHeaderV1(header);
    if (common != CanonicalValidationErrorV1::kNone) {
        return common;
    }
    if (header.event_type != event_type) {
        return CanonicalValidationErrorV1::kEventTypeMismatch;
    }
    return CanonicalValidationErrorV1::kNone;
}

[[nodiscard]] constexpr bool QualityScopeMatchesType(
    CanonicalQualityTypeV1 type,
    CanonicalQualityScopeV1 scope) noexcept {
    switch (type) {
        case CanonicalQualityTypeV1::kVendorSequenceGap:
        case CanonicalQualityTypeV1::kVendorSequenceDuplicate:
        case CanonicalQualityTypeV1::kVendorSequenceConflict:
        case CanonicalQualityTypeV1::kDecodeError:
        case CanonicalQualityTypeV1::kSchemaUnknown:
            return scope == CanonicalQualityScopeV1::kVendorMessage;
        case CanonicalQualityTypeV1::kExchangeSequenceGap:
        case CanonicalQualityTypeV1::kExchangeSequenceBackward:
        case CanonicalQualityTypeV1::kExchangeSequenceConflict:
        case CanonicalQualityTypeV1::kExchangeSequenceDuplicate:
        case CanonicalQualityTypeV1::kNormalizationRejected:
            return scope == CanonicalQualityScopeV1::kChannel;
        case CanonicalQualityTypeV1::kScopePoisoned:
        case CanonicalQualityTypeV1::kSequenceCapacityExhausted:
            return scope == CanonicalQualityScopeV1::kVendorMessage ||
                   scope == CanonicalQualityScopeV1::kChannel;
        case CanonicalQualityTypeV1::kSourceState:
        case CanonicalQualityTypeV1::kClockEpochChanged:
            return scope == CanonicalQualityScopeV1::kStream;
        case CanonicalQualityTypeV1::kSnapshotRejected:
            return scope == CanonicalQualityScopeV1::kSnapshotFamily;
        case CanonicalQualityTypeV1::kInstrumentUnknown:
            return scope == CanonicalQualityScopeV1::kInstrument;
        case CanonicalQualityTypeV1::kUnknown:
            return false;
    }
    return false;
}

[[nodiscard]] constexpr bool QualityScopeIdMatchesHeader(
    const CanonicalHeaderV1& header,
    const CanonicalQualityPayloadV1& payload) noexcept {
    switch (payload.scope_type) {
        case CanonicalQualityScopeV1::kStream:
            return payload.scope_id ==
                       CanonicalStreamScopeIdV1(header.source_stream_id) &&
                   header.market == CanonicalMarketV1::kUnknown &&
                   header.instrument_id == 0U && header.channel == 0U &&
                   header.exchange_sequence == 0U &&
                   header.exchange_time_ns == 0;
        case CanonicalQualityScopeV1::kVendorMessage:
            return payload.scope_id == CanonicalVendorMessageScopeIdV1(
                       header.origin_service_id,
                       header.origin_message_id) &&
                   header.vendor_sequence_id == payload.actual_sequence &&
                   header.market == CanonicalMarketV1::kUnknown &&
                   header.instrument_id == 0U && header.channel == 0U &&
                   header.exchange_sequence == 0U &&
                   header.exchange_time_ns == 0;
        case CanonicalQualityScopeV1::kChannel:
            return header.market != CanonicalMarketV1::kUnknown &&
                   payload.scope_id == CanonicalChannelScopeIdV1(
                       header.market, header.channel) &&
                   header.exchange_sequence == payload.actual_sequence &&
                   (header.sub_index != 2U ||
                    (header.instrument_id == 0U &&
                     header.exchange_time_ns == 0));
        case CanonicalQualityScopeV1::kInstrument:
            return payload.quality_type ==
                       CanonicalQualityTypeV1::kInstrumentUnknown &&
                   payload.scope_id == 0U &&
                   header.instrument_id == 0U &&
                   header.market != CanonicalMarketV1::kUnknown;
        case CanonicalQualityScopeV1::kSnapshotFamily:
            return header.market != CanonicalMarketV1::kUnknown &&
                   payload.scope_id == CanonicalSnapshotFamilyScopeIdV1(
                       header.market);
        case CanonicalQualityScopeV1::kTickFamily:
        case CanonicalQualityScopeV1::kUnknown:
            return false;
    }
    return false;
}

[[nodiscard]] constexpr bool IsTickOrigin(
    BusinessOriginKindV1 origin) noexcept {
    return origin == BusinessOriginKindV1::kShanghaiTick ||
           origin == BusinessOriginKindV1::kShenzhenOrder ||
           origin == BusinessOriginKindV1::kShenzhenTransaction;
}

[[nodiscard]] constexpr bool IsSnapshotOrigin(
    BusinessOriginKindV1 origin) noexcept {
    return origin == BusinessOriginKindV1::kShanghaiSnapshot ||
           origin == BusinessOriginKindV1::kShenzhenSnapshot;
}

[[nodiscard]] constexpr bool OriginMarketMatches(
    BusinessOriginKindV1 origin,
    CanonicalMarketV1 market) noexcept {
    switch (origin) {
        case BusinessOriginKindV1::kShanghaiSnapshot:
        case BusinessOriginKindV1::kShanghaiTick:
            return market == CanonicalMarketV1::kShanghai;
        case BusinessOriginKindV1::kShenzhenSnapshot:
        case BusinessOriginKindV1::kShenzhenOrder:
        case BusinessOriginKindV1::kShenzhenTransaction:
            return market == CanonicalMarketV1::kShenzhen;
        case BusinessOriginKindV1::kUnknown:
            return false;
    }
    return false;
}

[[nodiscard]] constexpr bool HasQualityFlag(
    std::uint64_t flags,
    QualityFlagV1 flag) noexcept {
    return (flags & QualityBit(flag)) != 0U;
}

[[nodiscard]] constexpr bool DetailIsOneOf(
    std::uint16_t detail,
    std::initializer_list<std::uint16_t> allowed) noexcept {
    for (const std::uint16_t value : allowed) {
        if (detail == value) {
            return true;
        }
    }
    return false;
}

}  // namespace

std::string_view CanonicalValidationErrorNameV1(
    CanonicalValidationErrorV1 error) noexcept {
    switch (error) {
        case CanonicalValidationErrorV1::kNone:
            return "none";
        case CanonicalValidationErrorV1::kUnsupportedHostEndian:
            return "unsupported_host_endian";
        case CanonicalValidationErrorV1::kInvalidMagic:
            return "invalid_magic";
        case CanonicalValidationErrorV1::kUnsupportedSchemaVersion:
            return "unsupported_schema_version";
        case CanonicalValidationErrorV1::kUnknownEventType:
            return "unknown_event_type";
        case CanonicalValidationErrorV1::kEventTypeMismatch:
            return "event_type_mismatch";
        case CanonicalValidationErrorV1::kInvalidRecordSize:
            return "invalid_record_size";
        case CanonicalValidationErrorV1::kInvalidSourceStream:
            return "invalid_source_stream";
        case CanonicalValidationErrorV1::kInvalidOriginCursor:
            return "invalid_origin_cursor";
        case CanonicalValidationErrorV1::kInvalidShardEventId:
            return "invalid_shard_event_id";
        case CanonicalValidationErrorV1::kUnknownQualityFlags:
            return "unknown_quality_flags";
        case CanonicalValidationErrorV1::kUnknownMarket:
            return "unknown_market";
        case CanonicalValidationErrorV1::kMissingInstrument:
            return "missing_instrument";
        case CanonicalValidationErrorV1::kUnknownEnum:
            return "unknown_enum";
        case CanonicalValidationErrorV1::kUnknownFlags:
            return "unknown_flags";
        case CanonicalValidationErrorV1::kUnknownValidityBits:
            return "unknown_validity_bits";
        case CanonicalValidationErrorV1::kNonzeroReserved:
            return "nonzero_reserved";
        case CanonicalValidationErrorV1::kInvalidFieldValue:
            return "invalid_field_value";
        case CanonicalValidationErrorV1::kInconsistentFieldValidity:
            return "inconsistent_field_validity";
        case CanonicalValidationErrorV1::kSnapshotQueueTooLong:
            return "snapshot_queue_too_long";
        case CanonicalValidationErrorV1::kInconsistentSnapshotDepth:
            return "inconsistent_snapshot_depth";
        case CanonicalValidationErrorV1::kInvalidCounts:
            return "invalid_counts";
        case CanonicalValidationErrorV1::kHashPresenceMismatch:
            return "hash_presence_mismatch";
    }
    return "invalid_validation_error";
}

CanonicalValidationErrorV1 ValidateCanonicalHeaderV1(
    const CanonicalHeaderV1& header) noexcept {
    if (!CanonicalHostIsLittleEndianV1()) {
        return CanonicalValidationErrorV1::kUnsupportedHostEndian;
    }
    if (header.magic != kCanonicalMagicV1) {
        return CanonicalValidationErrorV1::kInvalidMagic;
    }
    if (header.schema_version != kCanonicalSchemaVersionV1) {
        return CanonicalValidationErrorV1::kUnsupportedSchemaVersion;
    }
    if (!EventTypeKnown(header.event_type)) {
        return CanonicalValidationErrorV1::kUnknownEventType;
    }
    if (header.record_size != ExpectedRecordSize(header.event_type)) {
        return CanonicalValidationErrorV1::kInvalidRecordSize;
    }
    if (header.source_stream_id == 0U) {
        return CanonicalValidationErrorV1::kInvalidSourceStream;
    }
    if (header.origin_ingress_sequence == 0U ||
        header.origin_wal_end_pos == 0U ||
        header.origin_service_id == 0U ||
        header.origin_message_id == 0U) {
        return CanonicalValidationErrorV1::kInvalidOriginCursor;
    }
    // A recognized API/SYS message with an unsupported (including zero)
    // version is itself represented as a Control DecodeError.  Market
    // records and non-decode controls enforce a nonzero version below/in
    // their type-specific validator; the common header must not erase this
    // authoritative Phase-3 fact.
    if (header.event_type != CanonicalEventTypeV1::kControl &&
        header.origin_service_version == 0U) {
        return CanonicalValidationErrorV1::kInvalidOriginCursor;
    }
    if (header.shard_event_id == 0U) {
        return CanonicalValidationErrorV1::kInvalidShardEventId;
    }
    if (header.recv_realtime_ns < 0 || header.recv_monotonic_ns < 0) {
        return CanonicalValidationErrorV1::kInvalidFieldValue;
    }
    if ((header.quality_flags & ~kCanonicalQualityFlagsMaskV1) != 0U) {
        return CanonicalValidationErrorV1::kUnknownQualityFlags;
    }
    if (!MarketKnown(header.market)) {
        return CanonicalValidationErrorV1::kUnknownMarket;
    }
    if (header.trade_date == 0U) {
        return CanonicalValidationErrorV1::kInvalidFieldValue;
    }
    if (header.event_type == CanonicalEventTypeV1::kSnapshot ||
        header.event_type == CanonicalEventTypeV1::kTick) {
        if (header.market == CanonicalMarketV1::kUnknown) {
            return CanonicalValidationErrorV1::kUnknownMarket;
        }
        if (header.instrument_id == 0U) {
            return CanonicalValidationErrorV1::kMissingInstrument;
        }
        if (header.vendor_sequence_id == 0U || header.sub_index != 0U) {
            return CanonicalValidationErrorV1::kInvalidOriginCursor;
        }
    }
    if (header.event_type == CanonicalEventTypeV1::kQuality &&
        (header.vendor_sequence_id == 0U || header.sub_index == 0U ||
         header.sub_index > 3U)) {
        return CanonicalValidationErrorV1::kInvalidOriginCursor;
    }
    if (header.event_type == CanonicalEventTypeV1::kControl &&
        (header.market != CanonicalMarketV1::kUnknown ||
         header.instrument_id != 0U || header.channel != 0U ||
         header.exchange_sequence != 0U || header.exchange_time_ns != 0 ||
         header.vendor_sequence_id != 0U || header.sub_index != 0U)) {
        return CanonicalValidationErrorV1::kInvalidFieldValue;
    }
    return CanonicalValidationErrorV1::kNone;
}

CanonicalValidationErrorV1 ValidateCanonicalTickRecordV1(
    const CanonicalTickRecordV1& record) noexcept {
    const CanonicalValidationErrorV1 header_error = ValidateExpectedHeader(
        record.header, CanonicalEventTypeV1::kTick);
    if (header_error != CanonicalValidationErrorV1::kNone) {
        return header_error;
    }
    const BusinessOriginKindV1 origin = BusinessOriginKind(record.header);
    if (origin != BusinessOriginKindV1::kShanghaiTick &&
        origin != BusinessOriginKindV1::kShenzhenOrder &&
        origin != BusinessOriginKindV1::kShenzhenTransaction) {
        return CanonicalValidationErrorV1::kInvalidOriginCursor;
    }
    const CanonicalTickPayloadV1& payload = record.payload;
    if (payload.reserved != 0U) {
        return CanonicalValidationErrorV1::kNonzeroReserved;
    }
    if ((payload.validity_bitmap & ~kCanonicalTickValidityMaskV1) != 0U) {
        return CanonicalValidationErrorV1::kUnknownValidityBits;
    }
    if ((payload.business_flags & ~kCanonicalTickBusinessFlagsMaskV1) != 0U) {
        return CanonicalValidationErrorV1::kUnknownFlags;
    }
    if (!TickActionKnown(payload.action) || !SideRecognized(payload.side) ||
        !OrderTypeRecognized(payload.order_type) ||
        !AggressorRecognized(payload.aggressor) ||
        !QuantityUnitRecognized(payload.quantity_unit) ||
        !PhaseRecognized(payload.phase)) {
        return CanonicalValidationErrorV1::kUnknownEnum;
    }

    const std::uint32_t validity = payload.validity_bitmap;
    std::uint32_t action_allowed = 0U;
    switch (payload.action) {
        case CanonicalTickActionV1::kAdd:
            action_allowed =
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kPrice) |
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kQuantity) |
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kMatchedQuantity) |
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kPrimaryOrderId) |
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kExchangeTime) |
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kSide) |
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kOrderType) |
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kPhase);
            break;
        case CanonicalTickActionV1::kCancel:
            action_allowed =
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kQuantity) |
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kPrimaryOrderId) |
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kBuyOrderId) |
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kSellOrderId) |
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kExchangeTime) |
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kSide) |
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kPhase);
            break;
        case CanonicalTickActionV1::kTrade:
            action_allowed =
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kPrice) |
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kQuantity) |
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kTradeAmount) |
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kBuyOrderId) |
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kSellOrderId) |
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kExchangeTime) |
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kAggressor) |
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kPhase);
            break;
        case CanonicalTickActionV1::kStatus:
            action_allowed =
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kExchangeTime) |
                CanonicalTickValidityBitV1(
                    CanonicalTickValidityV1::kPhase);
            break;
        case CanonicalTickActionV1::kUnknown:
            break;
    }
    if ((validity & ~action_allowed) != 0U) {
        return CanonicalValidationErrorV1::kInvalidFieldValue;
    }
    if ((payload.action == CanonicalTickActionV1::kAdd ||
         payload.action == CanonicalTickActionV1::kCancel ||
         payload.action == CanonicalTickActionV1::kTrade) &&
        !BitSet(validity, CanonicalTickValidityV1::kQuantity)) {
        return CanonicalValidationErrorV1::kInvalidFieldValue;
    }
    if (!ValueMatchesValidity(
            payload.price_p6,
            BitSet(validity, CanonicalTickValidityV1::kPrice)) ||
        !ValueMatchesValidity(
            payload.quantity_native,
            BitSet(validity, CanonicalTickValidityV1::kQuantity)) ||
        !ValueMatchesValidity(
            payload.trade_amount_p6,
            BitSet(validity, CanonicalTickValidityV1::kTradeAmount)) ||
        !ValueMatchesValidity(
            payload.matched_quantity_native,
            BitSet(validity, CanonicalTickValidityV1::kMatchedQuantity)) ||
        !ValueMatchesValidity(
            payload.primary_order_id,
            BitSet(validity, CanonicalTickValidityV1::kPrimaryOrderId)) ||
        !ValueMatchesValidity(
            payload.buy_order_id,
            BitSet(validity, CanonicalTickValidityV1::kBuyOrderId)) ||
        !ValueMatchesValidity(
            payload.sell_order_id,
            BitSet(validity, CanonicalTickValidityV1::kSellOrderId)) ||
        !ValueMatchesValidity(
            record.header.exchange_time_ns,
            BitSet(validity, CanonicalTickValidityV1::kExchangeTime))) {
        return CanonicalValidationErrorV1::kInconsistentFieldValidity;
    }

    if ((BitSet(validity, CanonicalTickValidityV1::kPrimaryOrderId) &&
         payload.primary_order_id <= 0) ||
        (BitSet(validity, CanonicalTickValidityV1::kBuyOrderId) &&
         payload.buy_order_id <= 0) ||
        (BitSet(validity, CanonicalTickValidityV1::kSellOrderId) &&
         payload.sell_order_id <= 0)) {
        return CanonicalValidationErrorV1::kInvalidFieldValue;
    }
    if ((BitSet(validity, CanonicalTickValidityV1::kQuantity) &&
         payload.quantity_native < 0) ||
        (BitSet(validity, CanonicalTickValidityV1::kMatchedQuantity) &&
         payload.matched_quantity_native < 0) ||
        (BitSet(validity, CanonicalTickValidityV1::kPrice) &&
         payload.price_p6 < 0) ||
        (BitSet(validity, CanonicalTickValidityV1::kTradeAmount) &&
         payload.trade_amount_p6 < 0) ||
        (BitSet(validity, CanonicalTickValidityV1::kExchangeTime) &&
         record.header.exchange_time_ns < 0)) {
        return CanonicalValidationErrorV1::kInvalidFieldValue;
    }
    if (BitSet(validity, CanonicalTickValidityV1::kExchangeTime) &&
        record.header.exchange_time_ns == 0) {
        return CanonicalValidationErrorV1::kInvalidFieldValue;
    }

    const bool side_valid =
        BitSet(validity, CanonicalTickValidityV1::kSide);
    const bool order_type_valid =
        BitSet(validity, CanonicalTickValidityV1::kOrderType);
    const bool aggressor_valid =
        BitSet(validity, CanonicalTickValidityV1::kAggressor);
    const bool phase_valid =
        BitSet(validity, CanonicalTickValidityV1::kPhase);
    if ((side_valid != (payload.side != CanonicalSideV1::kUnknown)) ||
        (order_type_valid !=
         (payload.order_type != CanonicalOrderTypeV1::kUnknown)) ||
        (aggressor_valid !=
         (payload.aggressor != CanonicalAggressorV1::kUnknown)) ||
        (phase_valid !=
         (payload.phase != CanonicalTradingPhaseV1::kUnknown))) {
        return CanonicalValidationErrorV1::kInconsistentFieldValidity;
    }
    if (!TickOriginProjectionValid(origin, record.header, payload)) {
        return CanonicalValidationErrorV1::kInvalidFieldValue;
    }
    return CanonicalValidationErrorV1::kNone;
}

CanonicalValidationErrorV1 ValidateCanonicalSnapshotRecordV1(
    const CanonicalSnapshotRecordV1& record) noexcept {
    const CanonicalValidationErrorV1 header_error = ValidateExpectedHeader(
        record.header, CanonicalEventTypeV1::kSnapshot);
    if (header_error != CanonicalValidationErrorV1::kNone) {
        return header_error;
    }
    const BusinessOriginKindV1 origin = BusinessOriginKind(record.header);
    if (origin == BusinessOriginKindV1::kShanghaiSnapshot) {
        if (record.header.market != CanonicalMarketV1::kShanghai ||
            record.header.channel != 0U ||
            record.header.exchange_sequence != 0U) {
            return CanonicalValidationErrorV1::kInvalidFieldValue;
        }
    } else if (origin == BusinessOriginKindV1::kShenzhenSnapshot) {
        if (record.header.market != CanonicalMarketV1::kShenzhen ||
            record.header.exchange_sequence != 0U) {
            return CanonicalValidationErrorV1::kInvalidFieldValue;
        }
    } else {
        return CanonicalValidationErrorV1::kInvalidOriginCursor;
    }
    const CanonicalSnapshotPayloadV1& payload = record.payload;
    if (!BytesAllZero(payload.reserved)) {
        return CanonicalValidationErrorV1::kNonzeroReserved;
    }
    if ((payload.scalar_validity &
         ~kCanonicalSnapshotScalarValidityMaskV1) != 0U ||
        (payload.bid_price_validity &
         ~kCanonicalLevelValidityMaskV1) != 0U ||
        (payload.bid_quantity_validity &
         ~kCanonicalLevelValidityMaskV1) != 0U ||
        (payload.bid_order_count_validity &
         ~kCanonicalLevelValidityMaskV1) != 0U ||
        (payload.ask_price_validity &
         ~kCanonicalLevelValidityMaskV1) != 0U ||
        (payload.ask_quantity_validity &
         ~kCanonicalLevelValidityMaskV1) != 0U ||
        (payload.ask_order_count_validity &
         ~kCanonicalLevelValidityMaskV1) != 0U ||
        (payload.bid_queue_validity &
         ~kCanonicalQueueValidityMaskV1) != 0U ||
        (payload.ask_queue_validity &
         ~kCanonicalQueueValidityMaskV1) != 0U) {
        return CanonicalValidationErrorV1::kUnknownValidityBits;
    }
    if ((payload.snapshot_flags & ~kCanonicalSnapshotFlagsMaskV1) != 0U) {
        return CanonicalValidationErrorV1::kUnknownFlags;
    }
    if (!PhaseRecognized(payload.phase) ||
        !QuantityUnitRecognized(payload.quantity_unit) ||
        !LimitSemanticsRecognized(payload.high_limit_semantics) ||
        !LimitSemanticsRecognized(payload.low_limit_semantics)) {
        return CanonicalValidationErrorV1::kUnknownEnum;
    }

    const std::uint64_t validity = payload.scalar_validity;
    const std::array<std::pair<std::int64_t,
                               CanonicalSnapshotScalarValidityV1>,
                     16U>
        scalar_values{{
            {payload.pre_close_price_p6,
             CanonicalSnapshotScalarValidityV1::kPreClosePrice},
            {payload.open_price_p6,
             CanonicalSnapshotScalarValidityV1::kOpenPrice},
            {payload.high_price_p6,
             CanonicalSnapshotScalarValidityV1::kHighPrice},
            {payload.low_price_p6,
             CanonicalSnapshotScalarValidityV1::kLowPrice},
            {payload.last_price_p6,
             CanonicalSnapshotScalarValidityV1::kLastPrice},
            {payload.close_price_p6,
             CanonicalSnapshotScalarValidityV1::kClosePrice},
            {payload.volume_native,
             CanonicalSnapshotScalarValidityV1::kVolumeNative},
            {payload.turnover_p6,
             CanonicalSnapshotScalarValidityV1::kTurnoverP6},
            {payload.trade_count,
             CanonicalSnapshotScalarValidityV1::kTradeCount},
            {payload.total_bid_quantity_native,
             CanonicalSnapshotScalarValidityV1::kTotalBidQuantity},
            {payload.total_ask_quantity_native,
             CanonicalSnapshotScalarValidityV1::kTotalAskQuantity},
            {payload.weighted_bid_price_p6,
             CanonicalSnapshotScalarValidityV1::kWeightedBidPrice},
            {payload.weighted_ask_price_p6,
             CanonicalSnapshotScalarValidityV1::kWeightedAskPrice},
            {payload.high_limit_price_p6,
             CanonicalSnapshotScalarValidityV1::kHighLimitPrice},
            {payload.low_limit_price_p6,
             CanonicalSnapshotScalarValidityV1::kLowLimitPrice},
            {payload.iopv_p6,
             CanonicalSnapshotScalarValidityV1::kIopv},
        }};
    for (const auto& [value, bit] : scalar_values) {
        if (!ValueMatchesValidity(value, BitSet(validity, bit))) {
            return CanonicalValidationErrorV1::kInconsistentFieldValidity;
        }
    }
    if ((BitSet(validity,
                CanonicalSnapshotScalarValidityV1::kPreClosePrice) &&
         payload.pre_close_price_p6 < 0) ||
        (BitSet(validity,
                CanonicalSnapshotScalarValidityV1::kOpenPrice) &&
         payload.open_price_p6 < 0) ||
        (BitSet(validity,
                CanonicalSnapshotScalarValidityV1::kHighPrice) &&
         payload.high_price_p6 < 0) ||
        (BitSet(validity,
                CanonicalSnapshotScalarValidityV1::kLowPrice) &&
         payload.low_price_p6 < 0) ||
        (BitSet(validity,
                CanonicalSnapshotScalarValidityV1::kLastPrice) &&
         payload.last_price_p6 < 0) ||
        (BitSet(validity,
                CanonicalSnapshotScalarValidityV1::kClosePrice) &&
         payload.close_price_p6 < 0) ||
        (BitSet(validity,
                CanonicalSnapshotScalarValidityV1::kVolumeNative) &&
         payload.volume_native < 0) ||
        (BitSet(validity,
                CanonicalSnapshotScalarValidityV1::kTurnoverP6) &&
         payload.turnover_p6 < 0) ||
        (BitSet(validity,
                CanonicalSnapshotScalarValidityV1::kTradeCount) &&
         payload.trade_count < 0) ||
        (BitSet(validity,
                CanonicalSnapshotScalarValidityV1::kTotalBidQuantity) &&
         payload.total_bid_quantity_native < 0) ||
        (BitSet(validity,
                CanonicalSnapshotScalarValidityV1::kTotalAskQuantity) &&
         payload.total_ask_quantity_native < 0) ||
        (BitSet(validity,
                CanonicalSnapshotScalarValidityV1::kWeightedBidPrice) &&
         payload.weighted_bid_price_p6 < 0) ||
        (BitSet(validity,
                CanonicalSnapshotScalarValidityV1::kWeightedAskPrice) &&
         payload.weighted_ask_price_p6 < 0) ||
        (BitSet(validity,
                CanonicalSnapshotScalarValidityV1::kHighLimitPrice) &&
         payload.high_limit_price_p6 < 0) ||
        (BitSet(validity,
                CanonicalSnapshotScalarValidityV1::kLowLimitPrice) &&
         payload.low_limit_price_p6 < 0) ||
        (BitSet(validity,
                CanonicalSnapshotScalarValidityV1::kIopv) &&
         payload.iopv_p6 < 0) ||
        (BitSet(validity,
                CanonicalSnapshotScalarValidityV1::kExchangeTime) &&
         record.header.exchange_time_ns < 0)) {
        return CanonicalValidationErrorV1::kInvalidFieldValue;
    }
    if (BitSet(
            validity,
            CanonicalSnapshotScalarValidityV1::kExchangeTime) &&
        record.header.exchange_time_ns == 0) {
        return CanonicalValidationErrorV1::kInvalidFieldValue;
    }
    if (!ValueMatchesValidity(
            record.header.exchange_time_ns,
            BitSet(validity,
                   CanonicalSnapshotScalarValidityV1::kExchangeTime)) ||
        !ValueMatchesValidity(
            payload.raw_phase_bits,
            BitSet(validity,
                   CanonicalSnapshotScalarValidityV1::kRawPhase)) ||
        !ValueMatchesValidity(
            payload.status_code_bits,
            BitSet(validity,
                   CanonicalSnapshotScalarValidityV1::kStatusCode)) ||
        !ValueMatchesValidity(
            payload.image_status,
            BitSet(validity,
                   CanonicalSnapshotScalarValidityV1::kImageStatus))) {
        return CanonicalValidationErrorV1::kInconsistentFieldValidity;
    }

    const bool phase_valid = BitSet(
        validity, CanonicalSnapshotScalarValidityV1::kNormalizedPhase);
    const bool quantity_unit_valid = BitSet(
        validity, CanonicalSnapshotScalarValidityV1::kQuantityUnit);
    if ((phase_valid !=
         (payload.phase != CanonicalTradingPhaseV1::kUnknown)) ||
        (quantity_unit_valid !=
         (payload.quantity_unit != CanonicalQuantityUnitV1::kUnknown))) {
        return CanonicalValidationErrorV1::kInconsistentFieldValidity;
    }

    const bool high_limit_valid = BitSet(
        validity, CanonicalSnapshotScalarValidityV1::kHighLimitPrice);
    const bool low_limit_valid = BitSet(
        validity, CanonicalSnapshotScalarValidityV1::kLowLimitPrice);
    if (high_limit_valid !=
            (payload.high_limit_semantics ==
             CanonicalLimitSemanticsV1::kFinite) ||
        low_limit_valid !=
            (payload.low_limit_semantics ==
             CanonicalLimitSemanticsV1::kFinite)) {
        return CanonicalValidationErrorV1::kInconsistentFieldValidity;
    }

    const bool bid_depth_valid = BitSet(
        validity, CanonicalSnapshotScalarValidityV1::kActualBidDepth);
    const bool ask_depth_valid = BitSet(
        validity, CanonicalSnapshotScalarValidityV1::kActualAskDepth);
    const bool bid_truncated =
        (payload.snapshot_flags & CanonicalSnapshotFlagBitV1(
             CanonicalSnapshotFlagV1::kBidDepthTruncatedTo10)) != 0U;
    const bool ask_truncated =
        (payload.snapshot_flags & CanonicalSnapshotFlagBitV1(
             CanonicalSnapshotFlagV1::kAskDepthTruncatedTo10)) != 0U;
    if (!bid_depth_valid || !ask_depth_valid ||
        bid_truncated !=
            (bid_depth_valid &&
             payload.actual_bid_depth > kCanonicalDepthLevelsV1) ||
        ask_truncated !=
            (ask_depth_valid &&
             payload.actual_ask_depth > kCanonicalDepthLevelsV1)) {
        return CanonicalValidationErrorV1::kInconsistentSnapshotDepth;
    }

    const std::uint16_t allowed_bid_levels =
        bid_depth_valid ? AllowedLevelValidity(payload.actual_bid_depth) : 0U;
    const std::uint16_t allowed_ask_levels =
        ask_depth_valid ? AllowedLevelValidity(payload.actual_ask_depth) : 0U;
    if ((payload.bid_price_validity & ~allowed_bid_levels) != 0U ||
        (payload.bid_quantity_validity & ~allowed_bid_levels) != 0U ||
        (payload.bid_order_count_validity & ~allowed_bid_levels) != 0U ||
        (payload.ask_price_validity & ~allowed_ask_levels) != 0U ||
        (payload.ask_quantity_validity & ~allowed_ask_levels) != 0U ||
        (payload.ask_order_count_validity & ~allowed_ask_levels) != 0U) {
        return CanonicalValidationErrorV1::kInconsistentSnapshotDepth;
    }
    if (payload.bid_order_count_validity != allowed_bid_levels ||
        payload.ask_order_count_validity != allowed_ask_levels) {
        return CanonicalValidationErrorV1::kInconsistentSnapshotDepth;
    }
    if (!ArrayMatchesValidity(
            payload.bid_price_p6, payload.bid_price_validity) ||
        !ArrayMatchesValidity(
            payload.bid_quantity_native,
            payload.bid_quantity_validity) ||
        !ArrayMatchesValidity(
            payload.bid_order_count,
            payload.bid_order_count_validity) ||
        !ArrayMatchesValidity(
            payload.ask_price_p6, payload.ask_price_validity) ||
        !ArrayMatchesValidity(
            payload.ask_quantity_native,
            payload.ask_quantity_validity) ||
        !ArrayMatchesValidity(
            payload.ask_order_count,
            payload.ask_order_count_validity)) {
        return CanonicalValidationErrorV1::kInconsistentFieldValidity;
    }
    if (!ValidArrayValuesAreNonnegative(
            payload.bid_price_p6,
            payload.bid_price_validity) ||
        !ValidArrayValuesAreNonnegative(
            payload.ask_price_p6,
            payload.ask_price_validity) ||
        !ValidArrayValuesAreNonnegative(
            payload.bid_quantity_native,
            payload.bid_quantity_validity) ||
        !ValidArrayValuesAreNonnegative(
            payload.ask_quantity_native,
            payload.ask_quantity_validity)) {
        return CanonicalValidationErrorV1::kInvalidFieldValue;
    }

    const bool bid_total_valid = BitSet(
        validity,
        CanonicalSnapshotScalarValidityV1::kBid1TotalOrderCount);
    const bool bid_revealed_valid = BitSet(
        validity,
        CanonicalSnapshotScalarValidityV1::kBid1RevealedCount);
    const bool ask_total_valid = BitSet(
        validity,
        CanonicalSnapshotScalarValidityV1::kAsk1TotalOrderCount);
    const bool ask_revealed_valid = BitSet(
        validity,
        CanonicalSnapshotScalarValidityV1::kAsk1RevealedCount);
    if (!bid_total_valid || !bid_revealed_valid ||
        !ask_total_valid || !ask_revealed_valid) {
        return CanonicalValidationErrorV1::kInconsistentFieldValidity;
    }
    if ((bid_revealed_valid &&
         payload.bid1_revealed_count > kCanonicalQueueEntriesV1) ||
        (ask_revealed_valid &&
         payload.ask1_revealed_count > kCanonicalQueueEntriesV1)) {
        return CanonicalValidationErrorV1::kSnapshotQueueTooLong;
    }
    if (payload.bid1_revealed_count >
            payload.bid1_total_order_count ||
        payload.ask1_revealed_count >
            payload.ask1_total_order_count) {
        return CanonicalValidationErrorV1::kInvalidCounts;
    }
    if ((payload.actual_bid_depth == 0U &&
         (payload.bid1_total_order_count != 0U ||
          payload.bid1_revealed_count != 0U ||
          payload.bid_queue_validity != 0U)) ||
        (payload.actual_ask_depth == 0U &&
         (payload.ask1_total_order_count != 0U ||
          payload.ask1_revealed_count != 0U ||
          payload.ask_queue_validity != 0U)) ||
        (payload.actual_bid_depth != 0U &&
         payload.bid1_total_order_count !=
             payload.bid_order_count[0]) ||
        (payload.actual_ask_depth != 0U &&
         payload.ask1_total_order_count !=
             payload.ask_order_count[0])) {
        return CanonicalValidationErrorV1::kInvalidCounts;
    }
    const std::uint64_t allowed_bid_queue = bid_revealed_valid
                                                ? AllowedQueueValidity(
                                                      payload
                                                          .bid1_revealed_count)
                                                : 0U;
    const std::uint64_t allowed_ask_queue = ask_revealed_valid
                                                ? AllowedQueueValidity(
                                                      payload
                                                          .ask1_revealed_count)
                                                : 0U;
    if ((payload.bid_queue_validity & ~allowed_bid_queue) != 0U ||
        (payload.ask_queue_validity & ~allowed_ask_queue) != 0U) {
        return CanonicalValidationErrorV1::kInconsistentFieldValidity;
    }
    if (origin == BusinessOriginKindV1::kShenzhenSnapshot &&
        (payload.bid_queue_validity != allowed_bid_queue ||
         payload.ask_queue_validity != allowed_ask_queue)) {
        return CanonicalValidationErrorV1::kInconsistentFieldValidity;
    }
    if (!ArrayMatchesValidity(
            payload.bid1_queue_quantity_native,
            payload.bid_queue_validity) ||
        !ArrayMatchesValidity(
            payload.ask1_queue_quantity_native,
            payload.ask_queue_validity)) {
        return CanonicalValidationErrorV1::kInconsistentFieldValidity;
    }
    if (!ValidArrayValuesAreNonnegative(
            payload.bid1_queue_quantity_native,
            payload.bid_queue_validity) ||
        !ValidArrayValuesAreNonnegative(
            payload.ask1_queue_quantity_native,
            payload.ask_queue_validity)) {
        return CanonicalValidationErrorV1::kInvalidFieldValue;
    }
    return CanonicalValidationErrorV1::kNone;
}

CanonicalValidationErrorV1 ValidateCanonicalQualityRecordV1(
    const CanonicalQualityRecordV1& record) noexcept {
    const CanonicalValidationErrorV1 header_error = ValidateExpectedHeader(
        record.header, CanonicalEventTypeV1::kQuality);
    if (header_error != CanonicalValidationErrorV1::kNone) {
        return header_error;
    }
    const CanonicalQualityPayloadV1& payload = record.payload;
    if (payload.reserved != 0U) {
        return CanonicalValidationErrorV1::kNonzeroReserved;
    }
    if (!QualityTypeKnown(payload.quality_type) ||
        !QualityScopeKnown(payload.scope_type)) {
        return CanonicalValidationErrorV1::kUnknownEnum;
    }
    if (!QualityScopeMatchesType(
            payload.quality_type, payload.scope_type)) {
        return CanonicalValidationErrorV1::kInvalidFieldValue;
    }
    if (payload.quality_type == CanonicalQualityTypeV1::kSourceState ||
        payload.quality_type ==
            CanonicalQualityTypeV1::kClockEpochChanged) {
        // V1 has no producer or frozen detail/header projection for these
        // two enum values.  Accepting an arbitrary stream-scoped payload
        // would silently assign semantics that the schema cannot prove.
        return CanonicalValidationErrorV1::kInvalidFieldValue;
    }
    const bool hash_present = DigestNonzero(payload.payload_sha256);
    if ((payload.quality_type ==
             CanonicalQualityTypeV1::kScopePoisoned &&
         hash_present) ||
        (payload.quality_type !=
             CanonicalQualityTypeV1::kScopePoisoned &&
         !hash_present)) {
        return CanonicalValidationErrorV1::kHashPresenceMismatch;
    }
    if (payload.first_bad_origin_wal_end_pos == 0U ||
        payload.first_bad_origin_wal_end_pos >
            record.header.origin_wal_end_pos) {
        return CanonicalValidationErrorV1::kInvalidOriginCursor;
    }
    if (payload.related_connection_epoch !=
        record.header.connection_epoch) {
        return CanonicalValidationErrorV1::kInvalidFieldValue;
    }
    if (!QualityScopeIdMatchesHeader(record.header, payload)) {
        return CanonicalValidationErrorV1::kInvalidFieldValue;
    }
    if (payload.human_code_id != 0U ||
        record.header.exchange_time_ns < 0) {
        return CanonicalValidationErrorV1::kInvalidFieldValue;
    }

    const auto require_flag = [&](QualityFlagV1 flag) noexcept {
        return HasQualityFlag(record.header.quality_flags, flag);
    };
    const BusinessOriginKindV1 origin = BusinessOriginKind(record.header);
    switch (payload.quality_type) {
        case CanonicalQualityTypeV1::kVendorSequenceGap:
            if (record.header.sub_index != 1U ||
                payload.detail_code != 3U ||
                payload.expected_sequence == 0U ||
                payload.expected_sequence >= payload.actual_sequence ||
                !require_flag(QualityFlagV1::kVendorSequenceGap)) {
                return CanonicalValidationErrorV1::kInvalidFieldValue;
            }
            break;
        case CanonicalQualityTypeV1::kVendorSequenceDuplicate:
            if (record.header.sub_index != 1U ||
                payload.detail_code != 4U ||
                payload.actual_sequence == 0U ||
                payload.expected_sequence < payload.actual_sequence ||
                !require_flag(
                    QualityFlagV1::kVendorSequenceDuplicate)) {
                return CanonicalValidationErrorV1::kInvalidFieldValue;
            }
            break;
        case CanonicalQualityTypeV1::kVendorSequenceConflict:
            if (record.header.sub_index != 1U ||
                !DetailIsOneOf(payload.detail_code, {5U, 6U}) ||
                payload.actual_sequence == 0U ||
                payload.expected_sequence < payload.actual_sequence ||
                !require_flag(
                    QualityFlagV1::kVendorSequenceConflict)) {
                return CanonicalValidationErrorV1::kInvalidFieldValue;
            }
            break;
        case CanonicalQualityTypeV1::kExchangeSequenceGap:
            if (record.header.sub_index != 2U ||
                payload.detail_code != 3U ||
                payload.expected_sequence == 0U ||
                payload.expected_sequence >= payload.actual_sequence ||
                !IsTickOrigin(origin) ||
                !OriginMarketMatches(origin, record.header.market) ||
                !require_flag(QualityFlagV1::kExchangeSequenceGap)) {
                return CanonicalValidationErrorV1::kInvalidFieldValue;
            }
            break;
        case CanonicalQualityTypeV1::kExchangeSequenceBackward:
            if (record.header.sub_index != 2U ||
                payload.detail_code != 6U ||
                payload.actual_sequence == 0U ||
                payload.expected_sequence <= payload.actual_sequence ||
                !IsTickOrigin(origin) ||
                !OriginMarketMatches(origin, record.header.market) ||
                !require_flag(
                    QualityFlagV1::kExchangeSequenceBackward)) {
                return CanonicalValidationErrorV1::kInvalidFieldValue;
            }
            break;
        case CanonicalQualityTypeV1::kExchangeSequenceConflict:
            if (record.header.sub_index != 2U ||
                payload.detail_code != 5U ||
                payload.actual_sequence == 0U ||
                payload.expected_sequence < payload.actual_sequence ||
                !IsTickOrigin(origin) ||
                !OriginMarketMatches(origin, record.header.market) ||
                !require_flag(
                    QualityFlagV1::kExchangeSequenceConflict)) {
                return CanonicalValidationErrorV1::kInvalidFieldValue;
            }
            break;
        case CanonicalQualityTypeV1::kExchangeSequenceDuplicate:
            if (record.header.sub_index != 2U ||
                payload.detail_code != 4U ||
                payload.actual_sequence == 0U ||
                payload.expected_sequence < payload.actual_sequence ||
                !IsTickOrigin(origin) ||
                !OriginMarketMatches(origin, record.header.market)) {
                return CanonicalValidationErrorV1::kInvalidFieldValue;
            }
            break;
        case CanonicalQualityTypeV1::kScopePoisoned: {
            const bool vendor = payload.scope_type ==
                CanonicalQualityScopeV1::kVendorMessage;
            if (record.header.sub_index != (vendor ? 1U : 2U) ||
                payload.detail_code != 7U ||
                payload.actual_sequence == 0U ||
                !(vendor
                      ? require_flag(
                            QualityFlagV1::kVendorSequenceConflict)
                      : (IsTickOrigin(origin) &&
                         OriginMarketMatches(origin,
                                             record.header.market) &&
                         require_flag(
                             QualityFlagV1::kExchangeSequenceConflict)))) {
                return CanonicalValidationErrorV1::kInvalidFieldValue;
            }
            break;
        }
        case CanonicalQualityTypeV1::kSequenceCapacityExhausted: {
            const bool vendor = payload.scope_type ==
                CanonicalQualityScopeV1::kVendorMessage;
            if (record.header.sub_index != (vendor ? 1U : 2U) ||
                payload.detail_code != 8U ||
                payload.actual_sequence == 0U ||
                payload.expected_sequence > payload.actual_sequence ||
                !(vendor
                      ? require_flag(
                            QualityFlagV1::kVendorSequenceConflict)
                      : (IsTickOrigin(origin) &&
                         OriginMarketMatches(origin,
                                             record.header.market) &&
                         require_flag(
                             QualityFlagV1::kExchangeSequenceConflict)))) {
                return CanonicalValidationErrorV1::kInvalidFieldValue;
            }
            break;
        }
        case CanonicalQualityTypeV1::kDecodeError: {
            if (record.header.sub_index != 3U ||
                payload.expected_sequence != 0U ||
                payload.actual_sequence == 0U ||
                record.header.origin_service_version != 101U ||
                !RecognizedCoreMessageTuple(record.header) ||
                !DetailIsOneOf(
                    payload.detail_code,
                    {3U, 6U, 7U, 8U, 9U, 10U, 11U, 12U})) {
                return CanonicalValidationErrorV1::kInvalidFieldValue;
            }
            const bool required_present =
                payload.detail_code == 6U
                ? require_flag(QualityFlagV1::kDecodeTruncated)
                : (payload.detail_code == 7U ||
                   payload.detail_code == 8U)
                      ? require_flag(
                            QualityFlagV1::kDecodeOffsetInvalid)
                : payload.detail_code == 11U
                      ? require_flag(QualityFlagV1::kDecodeTextInvalid)
                      : true;
            if (!required_present) {
                return CanonicalValidationErrorV1::kInvalidFieldValue;
            }
            break;
        }
        case CanonicalQualityTypeV1::kSchemaUnknown:
            if (record.header.sub_index != 3U ||
                payload.expected_sequence != 0U ||
                payload.actual_sequence == 0U ||
                !DetailIsOneOf(payload.detail_code, {4U, 5U}) ||
                (payload.detail_code == 4U &&
                 RecognizedCoreMessageTuple(record.header)) ||
                (payload.detail_code == 5U &&
                 (!RecognizedCoreMessageTuple(record.header) ||
                  record.header.origin_service_version == 101U)) ||
                !require_flag(QualityFlagV1::kSchemaUnknown)) {
                return CanonicalValidationErrorV1::kInvalidFieldValue;
            }
            break;
        case CanonicalQualityTypeV1::kNormalizationRejected:
            if (record.header.sub_index != 3U ||
                payload.detail_code != 3U ||
                payload.expected_sequence != 0U ||
                !IsTickOrigin(origin) ||
                !OriginMarketMatches(origin, record.header.market)) {
                return CanonicalValidationErrorV1::kInvalidFieldValue;
            }
            break;
        case CanonicalQualityTypeV1::kSnapshotRejected:
            if (record.header.sub_index != 3U ||
                !DetailIsOneOf(payload.detail_code, {3U, 9U}) ||
                payload.expected_sequence != 0U ||
                payload.actual_sequence != 0U ||
                record.header.exchange_sequence != 0U ||
                !IsSnapshotOrigin(origin) ||
                !OriginMarketMatches(origin, record.header.market)) {
                return CanonicalValidationErrorV1::kInvalidFieldValue;
            }
            break;
        case CanonicalQualityTypeV1::kInstrumentUnknown:
            if (record.header.sub_index != 3U ||
                payload.detail_code != 3U ||
                payload.expected_sequence != 0U ||
                payload.actual_sequence != 0U ||
                origin == BusinessOriginKindV1::kUnknown ||
                !OriginMarketMatches(origin, record.header.market) ||
                !require_flag(QualityFlagV1::kInstrumentUnknown)) {
                return CanonicalValidationErrorV1::kInvalidFieldValue;
            }
            break;
        case CanonicalQualityTypeV1::kSourceState:
        case CanonicalQualityTypeV1::kClockEpochChanged:
        case CanonicalQualityTypeV1::kUnknown:
            return CanonicalValidationErrorV1::kInvalidFieldValue;
    }
    return CanonicalValidationErrorV1::kNone;
}

CanonicalValidationErrorV1 ValidateCanonicalControlRecordV1(
    const CanonicalControlRecordV1& record) noexcept {
    const CanonicalValidationErrorV1 header_error = ValidateExpectedHeader(
        record.header, CanonicalEventTypeV1::kControl);
    if (header_error != CanonicalValidationErrorV1::kNone) {
        return header_error;
    }
    const CanonicalControlPayloadV1& payload = record.payload;
    if (payload.reserved != 0U) {
        return CanonicalValidationErrorV1::kNonzeroReserved;
    }
    if (!ControlTypeKnown(payload.control_type)) {
        return CanonicalValidationErrorV1::kUnknownEnum;
    }
    if (!ControlOriginIdentityMatchesType(record.header, payload)) {
        return CanonicalValidationErrorV1::kInvalidFieldValue;
    }
    if ((payload.control_type == CanonicalControlTypeV1::kDecodeError &&
         payload.return_or_error_code == 0U) ||
        (payload.control_type != CanonicalControlTypeV1::kDecodeError &&
         record.header.origin_service_version == 0U)) {
        return CanonicalValidationErrorV1::kInvalidFieldValue;
    }
    if ((payload.flags & ~kCanonicalControlFlagsMaskV1) != 0U) {
        return CanonicalValidationErrorV1::kUnknownFlags;
    }
    if (payload.connection_epoch != record.header.connection_epoch) {
        return CanonicalValidationErrorV1::kInvalidFieldValue;
    }

    const std::uint64_t flags = record.header.quality_flags;
    const bool session_unknown = HasQualityFlag(
        flags, QualityFlagV1::kSessionUnknown);
    const bool source_disconnected = HasQualityFlag(
        flags, QualityFlagV1::kSourceDisconnected);
    const bool schema_unknown = HasQualityFlag(
        flags, QualityFlagV1::kSchemaUnknown);
    const bool decode_truncated = HasQualityFlag(
        flags, QualityFlagV1::kDecodeTruncated);
    const bool decode_offset = HasQualityFlag(
        flags, QualityFlagV1::kDecodeOffsetInvalid);
    const bool has_decode_diagnosis =
        schema_unknown || decode_truncated || decode_offset;
    if (((payload.control_type == CanonicalControlTypeV1::kConnecting ||
          payload.control_type == CanonicalControlTypeV1::kConnectError ||
          payload.control_type == CanonicalControlTypeV1::kDisconnected ||
          payload.control_type == CanonicalControlTypeV1::kLogonFailure ||
          payload.control_type == CanonicalControlTypeV1::kDecodeError) &&
         !session_unknown) ||
        ((payload.control_type == CanonicalControlTypeV1::kConnectError ||
          payload.control_type == CanonicalControlTypeV1::kDisconnected) &&
         !source_disconnected && !has_decode_diagnosis)) {
        return CanonicalValidationErrorV1::kInvalidFieldValue;
    }
    if (payload.control_type == CanonicalControlTypeV1::kDecodeError) {
        const std::uint8_t local = static_cast<std::uint8_t>(
            payload.return_or_error_code);
        const bool precise_quality = local == 3U
            ? schema_unknown
            : (local == 4U ? decode_truncated : decode_offset);
        if (!precise_quality) {
            return CanonicalValidationErrorV1::kInvalidFieldValue;
        }
    }

    const std::uint64_t required_resolved =
        static_cast<std::uint64_t>(payload.required_ok_count) +
        payload.required_failed_count;
    const std::uint64_t optional_resolved =
        static_cast<std::uint64_t>(payload.optional_ok_count) +
        payload.optional_failed_count;
    const std::uint64_t total_entries =
        static_cast<std::uint64_t>(payload.required_count) +
        payload.optional_count;
    if (required_resolved > payload.required_count ||
        optional_resolved > payload.optional_count ||
        payload.required_count == 0U || total_entries > 64U) {
        return CanonicalValidationErrorV1::kInvalidCounts;
    }

    const bool response_hash_present =
        (payload.flags & CanonicalControlFlagBitV1(
             CanonicalControlFlagV1::kResponseManifestHashPresent)) != 0U;
    const bool address_hash_present =
        (payload.flags & CanonicalControlFlagBitV1(
             CanonicalControlFlagV1::kAddressHashPresent)) != 0U;
    const bool error_hash_present =
        (payload.flags & CanonicalControlFlagBitV1(
             CanonicalControlFlagV1::kErrorTextHashPresent)) != 0U;
    if (response_hash_present !=
            DigestNonzero(payload.response_manifest_sha256) ||
        address_hash_present != DigestNonzero(payload.address_sha256) ||
        error_hash_present != DigestNonzero(payload.error_text_sha256)) {
        return CanonicalValidationErrorV1::kHashPresenceMismatch;
    }

    const bool no_response =
        !response_hash_present && payload.response_entry_count == 0U;
    const bool no_address_or_error =
        !address_hash_present && !error_hash_present;
    bool type_projection_valid = false;
    switch (payload.control_type) {
        case CanonicalControlTypeV1::kConnecting:
            type_projection_valid =
                payload.return_or_error_code == 0U && no_response &&
                address_hash_present && !error_hash_present;
            break;
        case CanonicalControlTypeV1::kConnectError:
        case CanonicalControlTypeV1::kDisconnected:
            type_projection_valid =
                payload.return_or_error_code == 0U && no_response &&
                address_hash_present && error_hash_present;
            break;
        case CanonicalControlTypeV1::kLogonSuccess:
            type_projection_valid =
                payload.return_or_error_code == 0U &&
                response_hash_present && no_address_or_error &&
                payload.response_entry_count <= 1'000'000U;
            break;
        case CanonicalControlTypeV1::kLogonFailure:
            type_projection_valid =
                payload.return_or_error_code != 0U &&
                response_hash_present && no_address_or_error &&
                payload.response_entry_count <= 1'000'000U;
            break;
        case CanonicalControlTypeV1::kSubscriptionAccepted:
            type_projection_valid =
                payload.return_or_error_code == 0U &&
                response_hash_present && no_address_or_error &&
                payload.response_entry_count <= 1'000'000U;
            break;
        case CanonicalControlTypeV1::kSubscriptionRejected:
            type_projection_valid =
                payload.return_or_error_code == 0U &&
                response_hash_present && no_address_or_error &&
                payload.response_entry_count != 0U &&
                payload.response_entry_count <= 1'000'000U;
            break;
        case CanonicalControlTypeV1::kServiceStatus:
            type_projection_valid =
                !response_hash_present && no_address_or_error &&
                payload.response_entry_count <= 4096U;
            break;
        case CanonicalControlTypeV1::kSessionStatus:
            type_projection_valid =
                payload.return_or_error_code == 0U &&
                !response_hash_present && no_address_or_error &&
                payload.response_entry_count <= 4096U;
            break;
        case CanonicalControlTypeV1::kDecodeError:
            type_projection_valid =
                payload.return_or_error_code != 0U && no_response &&
                no_address_or_error;
            break;
        case CanonicalControlTypeV1::kUnknown:
            break;
    }
    if (!type_projection_valid) {
        return CanonicalValidationErrorV1::kInvalidFieldValue;
    }

    const bool required_failure =
        (payload.flags & CanonicalControlFlagBitV1(
             CanonicalControlFlagV1::kRequiredFailure)) != 0U;
    const bool optional_failure =
        (payload.flags & CanonicalControlFlagBitV1(
             CanonicalControlFlagV1::kOptionalFailure)) != 0U;
    if (required_failure != (payload.required_failed_count != 0U) ||
        optional_failure != (payload.optional_failed_count != 0U)) {
        return CanonicalValidationErrorV1::kInvalidCounts;
    }
    return CanonicalValidationErrorV1::kNone;
}

std::string_view CanonicalSchemaDescriptorV1() noexcept {
    return kSchemaDescriptor;
}

l2flow::common::Sha256Digest
CanonicalSchemaDescriptorSha256V1() noexcept {
    return l2flow::common::ComputeSha256(kSchemaDescriptor);
}

std::string_view CanonicalDtypeDescriptorV1() noexcept {
    return kDtypeDescriptor;
}

l2flow::common::Sha256Digest
CanonicalDtypeDescriptorSha256V1() noexcept {
    return l2flow::common::ComputeSha256(kDtypeDescriptor);
}

}  // namespace l2flow::canonical
