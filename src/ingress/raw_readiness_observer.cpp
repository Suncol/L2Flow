#include "l2flow/ingress/raw_readiness_observer.h"

#include "l2flow/ingress/required_market_validator.h"

#include "mdl_shl2_msg.h"
#include "mdl_sys_msg.h"
#include "mdl_szl2_msg.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <new>
#include <optional>
#include <span>
#include <utility>

namespace l2flow::ingress {
namespace {

namespace sh = datayes::mdl::mdl_shl2_msg;
namespace sz = datayes::mdl::mdl_szl2_msg;

constexpr std::uint8_t kApiServiceId =
    static_cast<std::uint8_t>(datayes::mdl::MDLSID_MDL_API);
constexpr std::uint8_t kSystemServiceId =
    static_cast<std::uint8_t>(datayes::mdl::MDLSID_MDL_SYS);
constexpr std::uint16_t kSystemServiceVersion =
    static_cast<std::uint16_t>(
        datayes::mdl::mdl_sys_msg::LogonResponse::ServiceVer);
constexpr std::uint16_t kLogonResponseMessageId =
    static_cast<std::uint16_t>(
        datayes::mdl::mdl_sys_msg::LogonResponse::MessageID);
constexpr std::uint16_t kSubscribeResponseMessageId =
    static_cast<std::uint16_t>(
        datayes::mdl::mdl_sys_msg::SubscribeResponse::MessageID);
constexpr std::size_t kMdlListBytes = 8U;
constexpr std::size_t kLogonResponseBytes = 24U;
constexpr std::size_t kLogonServicesListOffset = 12U;
constexpr std::size_t kLogonReturnCodeOffset = 20U;
constexpr std::size_t kSubscribeServicesListOffset = 0U;
constexpr std::size_t kServiceItemBytes = 16U;
constexpr std::size_t kServiceIdOffset = 0U;
constexpr std::size_t kServiceVersionOffset = 4U;
constexpr std::size_t kServiceMessagesListOffset = 8U;
constexpr std::size_t kMessageStatusItemBytes = 8U;
constexpr std::size_t kMessageIdOffset = 0U;
constexpr std::size_t kMessageStatusOffset = 4U;
constexpr std::size_t kMdlStringBytes = 6U;
constexpr std::size_t kMdlStringOffsetFieldOffset = 2U;

constexpr l2flow::sdk::MessageKey kShSnapshotKey{
    4U, 101U, 4U};
constexpr l2flow::sdk::MessageKey kShTickKey{
    4U, 101U, 24U};
constexpr l2flow::sdk::MessageKey kSzSnapshotKey{
    6U, 101U, 28U};
constexpr l2flow::sdk::MessageKey kSzOrderKey{
    6U, 101U, 33U};
constexpr l2flow::sdk::MessageKey kSzTransactionKey{
    6U, 101U, 36U};

bool CheckedAddU64(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* output) noexcept {
    if (output == nullptr ||
        left >
            std::numeric_limits<std::uint64_t>::max() -
                right) {
        return false;
    }
    *output = left + right;
    return true;
}

static_assert(kSystemServiceVersion ==
              datayes::mdl::mdl_sys_msg::SubscribeResponse::ServiceVer);
static_assert(sizeof(datayes::mdl::MDLList) == kMdlListBytes);
static_assert(
    sizeof(datayes::mdl::mdl_sys_msg::LogonResponse) ==
    kLogonResponseBytes);
static_assert(
    offsetof(datayes::mdl::mdl_sys_msg::LogonResponse, Services) ==
    kLogonServicesListOffset);
static_assert(
    offsetof(datayes::mdl::mdl_sys_msg::LogonResponse, ReturnCode) ==
    kLogonReturnCodeOffset);
static_assert(
    sizeof(datayes::mdl::mdl_sys_msg::SubscribeResponse) ==
    kMdlListBytes);
static_assert(
    offsetof(datayes::mdl::mdl_sys_msg::SubscribeResponse, Services) ==
    kSubscribeServicesListOffset);
static_assert(
    sizeof(datayes::mdl::mdl_sys_msg::LogonResponse::ServicesItem) ==
    kServiceItemBytes);
static_assert(
    offsetof(
        datayes::mdl::mdl_sys_msg::LogonResponse::ServicesItem,
        Messages) == kServiceMessagesListOffset);
static_assert(
    sizeof(datayes::mdl::mdl_sys_msg::LogonResponse::ServicesItem::
               MessagesItem) == kMessageStatusItemBytes);
static_assert(sizeof(datayes::mdl::MDLString) == kMdlStringBytes);
static_assert(
    offsetof(datayes::mdl::MDLString, Offset) ==
    kMdlStringOffsetFieldOffset);
static_assert(sizeof(sh::SHL2MarketData) == 248U);
static_assert(
    sizeof(sh::SHL2MarketData::BidLevelsItem) == 28U);
static_assert(
    sizeof(sh::SHL2MarketData::BidLevelsItem::NOrdersItem) ==
    16U);
static_assert(
    sizeof(sh::SHL2MarketData::SellLevelsItem) == 28U);
static_assert(
    sizeof(sh::SHL2MarketData::SellLevelsItem::NoOrdersItem) ==
    16U);
static_assert(sizeof(sh::NGTSTick) == 70U);
static_assert(sizeof(sz::Snapshot300111_v2) == 224U);
static_assert(
    sizeof(sz::Snapshot300111_v2::BidPriceLevelItem) == 28U);
static_assert(
    sizeof(sz::Snapshot300111_v2::BidPriceLevelItem::OrdersItem) ==
    8U);
static_assert(
    sizeof(sz::Snapshot300111_v2::AskPriceLevelItem) == 28U);
static_assert(
    sizeof(sz::Snapshot300111_v2::AskPriceLevelItem::OrdersItem) ==
    8U);
static_assert(sizeof(sz::Order300192_v2) == 58U);
static_assert(sizeof(sz::Transaction300191_v2) == 70U);

// The helpers through ValidateRequiredMarketBody intentionally preserve the
// Phase-1 ShadowCaptureWriter's little-endian, checked-relative-offset and
// aggregate nested-list budget semantics. Keeping the SDK layout assertions
// beside them makes a vendor-header drift a compile failure instead of a
// silent readiness-policy change.
bool CheckedAddSize(
    std::size_t left,
    std::size_t right,
    std::size_t* result) noexcept {
    if (left >
        std::numeric_limits<std::size_t>::max() - right) {
        return false;
    }
    *result = left + right;
    return true;
}

bool LoadU32(
    std::span<const std::byte> bytes,
    std::size_t offset,
    std::uint32_t* value) noexcept {
    if (offset > bytes.size() ||
        sizeof(std::uint32_t) > bytes.size() - offset) {
        return false;
    }
    *value =
        std::to_integer<std::uint32_t>(bytes[offset]) |
        (std::to_integer<std::uint32_t>(
             bytes[offset + 1U])
         << 8U) |
        (std::to_integer<std::uint32_t>(
             bytes[offset + 2U])
         << 16U) |
        (std::to_integer<std::uint32_t>(
             bytes[offset + 3U])
         << 24U);
    return true;
}

bool LoadU16(
    std::span<const std::byte> bytes,
    std::size_t offset,
    std::uint16_t* value) noexcept {
    if (offset > bytes.size() ||
        sizeof(std::uint16_t) > bytes.size() - offset) {
        return false;
    }
    *value =
        std::to_integer<std::uint16_t>(bytes[offset]) |
        static_cast<std::uint16_t>(
            std::to_integer<std::uint16_t>(
                bytes[offset + 1U])
            << 8U);
    return true;
}

struct CheckedListRange final {
    std::size_t start = 0U;
    std::size_t count = 0U;
};

bool ReadListRange(
    std::span<const std::byte> bytes,
    std::size_t descriptor_offset,
    std::size_t item_bytes,
    std::size_t maximum_count,
    CheckedListRange* range) noexcept {
    std::uint32_t length = 0U;
    std::uint32_t relative_offset = 0U;
    std::size_t relative_offset_field = 0U;
    if (item_bytes == 0U ||
        !CheckedAddSize(
            descriptor_offset,
            sizeof(std::uint32_t),
            &relative_offset_field) ||
        !LoadU32(bytes, descriptor_offset, &length) ||
        !LoadU32(
            bytes,
            relative_offset_field,
            &relative_offset)) {
        return false;
    }
    range->count = static_cast<std::size_t>(length);
    if (range->count == 0U) {
        if (relative_offset == 0U) {
            range->start = descriptor_offset;
            return true;
        }
        if (!CheckedAddSize(
                descriptor_offset,
                static_cast<std::size_t>(relative_offset),
                &range->start) ||
            range->start > bytes.size()) {
            return false;
        }
        return true;
    }
    if (range->count > maximum_count ||
        relative_offset < kMdlListBytes) {
        return false;
    }

    std::size_t start = 0U;
    if (!CheckedAddSize(
            descriptor_offset,
            static_cast<std::size_t>(relative_offset),
            &start) ||
        range->count >
            std::numeric_limits<std::size_t>::max() /
                item_bytes) {
        return false;
    }
    const std::size_t total_bytes =
        range->count * item_bytes;
    if (start > bytes.size() ||
        total_bytes > bytes.size() - start) {
        return false;
    }
    range->start = start;
    return true;
}

bool ValidateStringRange(
    std::span<const std::byte> bytes,
    std::size_t descriptor_offset,
    std::size_t minimum_data_start) noexcept {
    std::uint16_t length = 0U;
    std::uint32_t relative_offset = 0U;
    std::size_t offset_field = 0U;
    if (!CheckedAddSize(
            descriptor_offset,
            kMdlStringOffsetFieldOffset,
            &offset_field) ||
        !LoadU16(bytes, descriptor_offset, &length) ||
        !LoadU32(bytes, offset_field, &relative_offset)) {
        return false;
    }
    if (length == 0U && relative_offset == 0U) {
        return true;
    }

    std::size_t start = 0U;
    if (!CheckedAddSize(
            descriptor_offset,
            static_cast<std::size_t>(relative_offset),
            &start) ||
        start > bytes.size()) {
        return false;
    }
    if (length == 0U) {
        return true;
    }
    return relative_offset >= kMdlStringBytes &&
           start >= minimum_data_start &&
           static_cast<std::size_t>(length) <=
               bytes.size() - start;
}

bool ValidateStringFields(
    std::span<const std::byte> body,
    std::size_t fixed_body_bytes,
    std::span<const std::size_t> descriptor_offsets) noexcept {
    for (const std::size_t descriptor_offset :
         descriptor_offsets) {
        if (!ValidateStringRange(
                body,
                descriptor_offset,
                fixed_body_bytes)) {
            return false;
        }
    }
    return true;
}

bool ValidateNestedListFields(
    std::span<const std::byte> body,
    std::size_t fixed_body_bytes,
    std::size_t top_descriptor_offset,
    std::size_t top_item_bytes,
    std::size_t nested_descriptor_in_item,
    std::size_t nested_item_bytes,
    std::size_t* aggregate_nested_items) noexcept {
    if (top_item_bytes == 0U || nested_item_bytes == 0U) {
        return false;
    }
    CheckedListRange top;
    if (!ReadListRange(
            body,
            top_descriptor_offset,
            top_item_bytes,
            body.size() / top_item_bytes,
            &top)) {
        return false;
    }
    if (top.count == 0U) {
        return true;
    }
    if (top.start < fixed_body_bytes ||
        top.count >
            std::numeric_limits<std::size_t>::max() /
                top_item_bytes) {
        return false;
    }
    std::size_t top_end = 0U;
    if (!CheckedAddSize(
            top.start,
            top.count * top_item_bytes,
            &top_end)) {
        return false;
    }

    const std::size_t nested_budget =
        body.size() / nested_item_bytes;
    if (*aggregate_nested_items > nested_budget) {
        return false;
    }
    for (std::size_t index = 0U;
         index < top.count;
         ++index) {
        const std::size_t item_offset =
            top.start + index * top_item_bytes;
        std::size_t nested_descriptor = 0U;
        if (!CheckedAddSize(
                item_offset,
                nested_descriptor_in_item,
                &nested_descriptor)) {
            return false;
        }
        CheckedListRange nested;
        if (!ReadListRange(
                body,
                nested_descriptor,
                nested_item_bytes,
                nested_budget,
                &nested)) {
            return false;
        }
        if (nested.count != 0U &&
            nested.start < top_end) {
            return false;
        }
        if (nested.count >
            nested_budget - *aggregate_nested_items) {
            return false;
        }
        *aggregate_nested_items += nested.count;
    }
    return true;
}

bool ValidateShSnapshotBody(
    std::span<const std::byte> body) noexcept {
    constexpr std::array<std::size_t, 2U> strings{
        offsetof(sh::SHL2MarketData, SecurityID),
        offsetof(sh::SHL2MarketData, InstruStatus),
    };
    if (body.size() < sizeof(sh::SHL2MarketData) ||
        !ValidateStringFields(
            body,
            sizeof(sh::SHL2MarketData),
            strings)) {
        return false;
    }

    std::size_t aggregate_nested_items = 0U;
    return ValidateNestedListFields(
               body,
               sizeof(sh::SHL2MarketData),
               offsetof(sh::SHL2MarketData, BidLevels),
               sizeof(sh::SHL2MarketData::BidLevelsItem),
               offsetof(
                   sh::SHL2MarketData::BidLevelsItem,
                   NOrders),
               sizeof(
                   sh::SHL2MarketData::BidLevelsItem::
                       NOrdersItem),
               &aggregate_nested_items) &&
           ValidateNestedListFields(
               body,
               sizeof(sh::SHL2MarketData),
               offsetof(sh::SHL2MarketData, SellLevels),
               sizeof(sh::SHL2MarketData::SellLevelsItem),
               offsetof(
                   sh::SHL2MarketData::SellLevelsItem,
                   NoOrders),
               sizeof(
                   sh::SHL2MarketData::SellLevelsItem::
                       NoOrdersItem),
               &aggregate_nested_items);
}

bool ValidateShTickBody(
    std::span<const std::byte> body) noexcept {
    constexpr std::array<std::size_t, 3U> strings{
        offsetof(sh::NGTSTick, SecurityID),
        offsetof(sh::NGTSTick, Type),
        offsetof(sh::NGTSTick, TickBSFlag),
    };
    return body.size() >= sizeof(sh::NGTSTick) &&
           ValidateStringFields(
               body, sizeof(sh::NGTSTick), strings);
}

bool ValidateSzSnapshotBody(
    std::span<const std::byte> body) noexcept {
    constexpr std::array<std::size_t, 4U> strings{
        offsetof(sz::Snapshot300111_v2, MDStreamID),
        offsetof(sz::Snapshot300111_v2, SecurityID),
        offsetof(
            sz::Snapshot300111_v2,
            SecurityIDSource),
        offsetof(
            sz::Snapshot300111_v2,
            TradingPhaseCode),
    };
    if (body.size() < sizeof(sz::Snapshot300111_v2) ||
        !ValidateStringFields(
            body,
            sizeof(sz::Snapshot300111_v2),
            strings)) {
        return false;
    }

    std::size_t aggregate_nested_items = 0U;
    return ValidateNestedListFields(
               body,
               sizeof(sz::Snapshot300111_v2),
               offsetof(
                   sz::Snapshot300111_v2,
                   BidPriceLevel),
               sizeof(
                   sz::Snapshot300111_v2::
                       BidPriceLevelItem),
               offsetof(
                   sz::Snapshot300111_v2::
                       BidPriceLevelItem,
                   Orders),
               sizeof(
                   sz::Snapshot300111_v2::
                       BidPriceLevelItem::OrdersItem),
               &aggregate_nested_items) &&
           ValidateNestedListFields(
               body,
               sizeof(sz::Snapshot300111_v2),
               offsetof(
                   sz::Snapshot300111_v2,
                   AskPriceLevel),
               sizeof(
                   sz::Snapshot300111_v2::
                       AskPriceLevelItem),
               offsetof(
                   sz::Snapshot300111_v2::
                       AskPriceLevelItem,
                   Orders),
               sizeof(
                   sz::Snapshot300111_v2::
                       AskPriceLevelItem::OrdersItem),
               &aggregate_nested_items);
}

bool ValidateSzOrderBody(
    std::span<const std::byte> body) noexcept {
    constexpr std::array<std::size_t, 3U> strings{
        offsetof(sz::Order300192_v2, MDStreamID),
        offsetof(sz::Order300192_v2, SecurityID),
        offsetof(sz::Order300192_v2, SecurityIDSource),
    };
    return body.size() >= sizeof(sz::Order300192_v2) &&
           ValidateStringFields(
               body,
               sizeof(sz::Order300192_v2),
               strings);
}

bool ValidateSzTransactionBody(
    std::span<const std::byte> body) noexcept {
    constexpr std::array<std::size_t, 3U> strings{
        offsetof(sz::Transaction300191_v2, MDStreamID),
        offsetof(sz::Transaction300191_v2, SecurityID),
        offsetof(
            sz::Transaction300191_v2,
            SecurityIDSource),
    };
    return body.size() >= sizeof(sz::Transaction300191_v2) &&
           ValidateStringFields(
               body,
               sizeof(sz::Transaction300191_v2),
               strings);
}

bool ValidateRequiredMarketBody(
    const l2flow::sdk::MessageKey& key,
    std::span<const std::byte> body) noexcept {
    if (key == kShSnapshotKey) {
        return ValidateShSnapshotBody(body);
    }
    if (key == kShTickKey) {
        return ValidateShTickBody(body);
    }
    if (key == kSzSnapshotKey) {
        return ValidateSzSnapshotBody(body);
    }
    if (key == kSzOrderKey) {
        return ValidateSzOrderBody(body);
    }
    if (key == kSzTransactionKey) {
        return ValidateSzTransactionBody(body);
    }
    return false;
}

bool IsValidConfig(
    const RawReadinessObserverConfig& config) noexcept {
    if (config.source_stream_id == 0U ||
        config.market_service_id == 0U ||
        config.market_service_id == kApiServiceId ||
        config.market_service_id == kSystemServiceId ||
        config.required_market_messages.empty() ||
        config.required_market_messages.size() > 64U ||
        config.heartbeat_timeout_ns == 0U) {
        return false;
    }
    for (std::size_t index = 0U;
         index < config.required_market_messages.size();
         ++index) {
        const l2flow::sdk::MessageKey& key =
            config.required_market_messages[index];
        if (key.service_id != config.market_service_id ||
            key.service_version == 0U ||
            key.message_id == 0U ||
            !l2flow::sdk::RequiredMessageFixedBodyBytes(
                 key)
                 .has_value()) {
            return false;
        }
        for (std::size_t prior = 0U;
             prior < index;
             ++prior) {
            if (key ==
                config.required_market_messages[prior]) {
                return false;
            }
        }
    }
    return true;
}

}  // namespace

bool ValidateRequiredMarketBodyBounds(
    const l2flow::sdk::MessageKey& key,
    std::span<const std::byte> body) noexcept {
    return ValidateRequiredMarketBody(key, body);
}

class RawReadinessObserver::Impl final {
public:
    explicit Impl(RawReadinessObserverConfig config)
        : config_(std::move(config)),
          required_mask_(
              config_.required_market_messages.size() == 64U
                  ? std::numeric_limits<std::uint64_t>::max()
                  : ((std::uint64_t{1U}
                      << config_.required_market_messages.size()) -
                     1U)) {}

    [[nodiscard]] bool BeginGeneration(
        const RawReadinessObserverGeneration& generation,
        std::uint64_t now_monotonic_ns) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (l2flow::common::IsZeroIdentity(
                generation.writer_instance) ||
            l2flow::common::IsZeroIdentity(
                generation.stream_day_id) ||
            generation.source_stream_id !=
                config_.source_stream_id ||
            generation.capture_date == 0U ||
            generation.connect_generation == 0U ||
            generation.recovery_wal_pos <
                kRawV1SegmentHeaderBytes ||
            generation.recovery_next_ingress_sequence == 0U ||
            generation.recovery_segment_sequence == 0U ||
            generation.recovery_segment_offset <
                kRawV1SegmentHeaderBytes ||
            generation.recovery_wal_pos <
                generation.recovery_segment_offset) {
            return false;
        }
        if (active_ &&
            generation.writer_instance ==
                generation_.writer_instance &&
            generation.connect_generation <=
                generation_.connect_generation) {
            return false;
        }

        generation_ = generation;
        active_ = true;
        healthy_ = true;
        heartbeat_published_ = true;
        processed_wal_pos_ = generation.recovery_wal_pos;
        processed_ingress_sequence_ =
            generation.recovery_next_ingress_sequence - 1U;
        next_ingress_sequence_ =
            generation.recovery_next_ingress_sequence;
        current_segment_sequence_ =
            generation.recovery_segment_sequence;
        current_segment_offset_ =
            generation.recovery_segment_offset;
        current_segment_base_wal_pos_ =
            generation.recovery_wal_pos -
            generation.recovery_segment_offset;
        heartbeat_monotonic_ns_ = now_monotonic_ns;
        logon_generation_ = 0U;
        logon_failed_responses_ = 0U;
        required_subscription_failure_observed_mask_ = 0U;
        latest_logon_ok_ = false;
        required_first_seen_mask_ = 0U;
        required_subscription_ok_mask_ = 0U;
        required_subscription_failed_mask_ = 0U;
        return true;
    }

    [[nodiscard]] RawReadinessObserveResult Observe(
        const RawRecordView& record,
        const l2flow::common::Identity128& writer_instance,
        std::uint64_t now_monotonic_ns) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!active_) {
            return RawReadinessObserveResult::kNoGeneration;
        }
        if (writer_instance != generation_.writer_instance) {
            return RawReadinessObserveResult::
                kWriterInstanceMismatch;
        }
        const RawRecordHeaderV1& header = record.header();
        if (header.source_stream_id !=
                generation_.source_stream_id ||
            header.capture_date != generation_.capture_date) {
            healthy_ = false;
            return RawReadinessObserveResult::
                kNamespaceMismatch;
        }
        std::uint64_t expected_record_start_wal_pos = 0U;
        if (record.record_start_offset() !=
                current_segment_offset_ ||
            !CheckedAddU64(
                current_segment_base_wal_pos_,
                record.record_start_offset(),
                &expected_record_start_wal_pos) ||
            expected_record_start_wal_pos !=
                record.record_start_wal_pos() ||
            record.record_start_wal_pos() !=
                processed_wal_pos_ ||
            record.record_end_wal_pos() <=
                record.record_start_wal_pos() ||
            record.record_end_offset() <=
                record.record_start_offset()) {
            healthy_ = false;
            return RawReadinessObserveResult::
                kWalCursorMismatch;
        }
        if (header.ingress_sequence !=
            next_ingress_sequence_) {
            healthy_ = false;
            return RawReadinessObserveResult::
                kIngressSequenceMismatch;
        }
        if (heartbeat_published_ &&
            now_monotonic_ns < heartbeat_monotonic_ns_) {
            healthy_ = false;
            return RawReadinessObserveResult::
                kClockRegression;
        }

        // Cursor/heartbeat publication covers every validated Raw record,
        // including a malformed control body. The latter still invalidates
        // readiness and is surfaced to the owner for fatal handling.
        processed_wal_pos_ = record.record_end_wal_pos();
        processed_ingress_sequence_ =
            header.ingress_sequence;
        current_segment_offset_ =
            record.record_end_offset();
        heartbeat_monotonic_ns_ = now_monotonic_ns;
        heartbeat_published_ = true;

        const RawReadinessObserveResult observed =
            ObserveBody(record);
        if (observed ==
            RawReadinessObserveResult::kMalformedControl) {
            healthy_ = false;
            return observed;
        }
        if (next_ingress_sequence_ ==
            std::numeric_limits<std::uint64_t>::max()) {
            healthy_ = false;
            return RawReadinessObserveResult::
                kIngressSequenceExhausted;
        }
        ++next_ingress_sequence_;
        return RawReadinessObserveResult::kProcessed;
    }

    [[nodiscard]] RawReadinessObserveResult
    ObserveSegmentTransition(
        const RawLiveSegmentTransitionV1& transition,
        std::uint64_t now_monotonic_ns) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!active_) {
            return RawReadinessObserveResult::kNoGeneration;
        }
        if (transition.writer_instance !=
            generation_.writer_instance) {
            return RawReadinessObserveResult::
                kWriterInstanceMismatch;
        }
        const SegmentHeaderV1& previous =
            transition.previous_segment;
        const SegmentHeaderV1& next =
            transition.next_segment;
        if (previous.stream_day_id !=
                generation_.stream_day_id ||
            next.stream_day_id !=
                generation_.stream_day_id ||
            previous.source_stream_id !=
                generation_.source_stream_id ||
            next.source_stream_id !=
                generation_.source_stream_id ||
            previous.capture_date !=
                generation_.capture_date ||
            next.capture_date !=
                generation_.capture_date) {
            healthy_ = false;
            return RawReadinessObserveResult::
                kNamespaceMismatch;
        }
        if (heartbeat_published_ &&
            now_monotonic_ns < heartbeat_monotonic_ns_) {
            healthy_ = false;
            return RawReadinessObserveResult::
                kClockRegression;
        }

        std::uint64_t previous_end_wal_pos = 0U;
        std::uint64_t next_data_begin_wal_pos = 0U;
        if (previous.segment_sequence !=
                current_segment_sequence_ ||
            previous.segment_base_wal_pos !=
                current_segment_base_wal_pos_ ||
            transition.previous_segment_end_offset !=
                current_segment_offset_ ||
            transition.previous_segment_end_offset <
                kRawV1SegmentHeaderBytes ||
            previous.segment_sequence ==
                std::numeric_limits<
                    std::uint32_t>::max() ||
            next.segment_sequence !=
                previous.segment_sequence + 1U ||
            !CheckedAddU64(
                previous.segment_base_wal_pos,
                transition
                    .previous_segment_end_offset,
                &previous_end_wal_pos) ||
            previous_end_wal_pos !=
                processed_wal_pos_ ||
            next.segment_base_wal_pos !=
                previous_end_wal_pos ||
            next.first_ingress_sequence !=
                next_ingress_sequence_ ||
            transition.next_ingress_sequence !=
                next_ingress_sequence_ ||
            !CheckedAddU64(
                next.segment_base_wal_pos,
                kRawV1SegmentHeaderBytes,
                &next_data_begin_wal_pos) ||
            transition.next_data_begin_wal_pos !=
                next_data_begin_wal_pos) {
            healthy_ = false;
            return RawReadinessObserveResult::
                kSegmentTransitionMismatch;
        }

        processed_wal_pos_ = next_data_begin_wal_pos;
        current_segment_sequence_ =
            next.segment_sequence;
        current_segment_base_wal_pos_ =
            next.segment_base_wal_pos;
        current_segment_offset_ =
            kRawV1SegmentHeaderBytes;
        heartbeat_monotonic_ns_ = now_monotonic_ns;
        heartbeat_published_ = true;
        return RawReadinessObserveResult::kProcessed;
    }

    [[nodiscard]] bool PublishHeartbeat(
        const l2flow::common::Identity128& writer_instance,
        std::uint64_t now_monotonic_ns) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!active_ ||
            writer_instance != generation_.writer_instance ||
            (heartbeat_published_ &&
             now_monotonic_ns < heartbeat_monotonic_ns_)) {
            if (active_ &&
                writer_instance == generation_.writer_instance) {
                healthy_ = false;
            }
            return false;
        }
        heartbeat_monotonic_ns_ = now_monotonic_ns;
        heartbeat_published_ = true;
        return true;
    }

    [[nodiscard]] RawReadinessObserverSnapshot Snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        RawReadinessObserverSnapshot result;
        result.generation = generation_;
        result.observer_processed_wal_pos =
            processed_wal_pos_;
        result.observer_processed_ingress_sequence =
            processed_ingress_sequence_;
        result.observer_processed_segment_sequence =
            current_segment_sequence_;
        result.observer_processed_segment_offset =
            current_segment_offset_;
        result.observer_heartbeat_monotonic_ns =
            heartbeat_monotonic_ns_;
        result.logon_generation = logon_generation_;
        result.required_market_mask = required_mask_;
        result.required_first_seen_mask =
            required_first_seen_mask_;
        result.required_subscription_ok_mask =
            required_subscription_ok_mask_;
        result.required_subscription_failed_mask =
            required_subscription_failed_mask_;
        result.generation_active = active_;
        result.observer_healthy = active_ && healthy_;
        result.latest_logon_ok = latest_logon_ok_;
        result.evidence_complete = EvidenceComplete();
        return result;
    }

    [[nodiscard]] RawObservationalGateResult Evaluate(
        const RawControlSnapshot& sampled_append,
        std::uint64_t now_monotonic_ns) const {
        std::lock_guard<std::mutex> lock(mutex_);
        RawObservationalGateResult result;
        result.sampled_append_wal_pos =
            sampled_append.append_global_wal_pos;
        result.observer_processed_wal_pos =
            processed_wal_pos_;
        result.connect_generation =
            generation_.connect_generation;
        result.logon_generation = logon_generation_;

        if (!active_) {
            result.reason =
                RawObservationalGateReason::kNoGeneration;
            return result;
        }
        if (sampled_append.writer_instance !=
            generation_.writer_instance) {
            result.reason =
                RawObservationalGateReason::
                    kWriterInstanceMismatch;
            return result;
        }
        if (sampled_append.stream_day_id !=
                generation_.stream_day_id ||
            sampled_append.source_stream_id !=
                generation_.source_stream_id ||
            sampled_append.capture_date !=
                generation_.capture_date) {
            result.reason =
                RawObservationalGateReason::
                    kNamespaceMismatch;
            return result;
        }
        if (sampled_append.fatal_state != 0U) {
            result.reason =
                RawObservationalGateReason::kWriterFatal;
            return result;
        }
        if (sampled_append.heartbeat_monotonic_ns == 0U) {
            result.reason =
                RawObservationalGateReason::
                    kWriterHeartbeatMissing;
            return result;
        }
        if (now_monotonic_ns <
            sampled_append.heartbeat_monotonic_ns) {
            result.reason =
                RawObservationalGateReason::
                    kWriterHeartbeatClockRegression;
            return result;
        }
        if (now_monotonic_ns -
                sampled_append.heartbeat_monotonic_ns >
            config_.heartbeat_timeout_ns) {
            result.reason =
                RawObservationalGateReason::
                    kWriterHeartbeatTimedOut;
            return result;
        }
        if (sampled_append.append_global_wal_pos <
            generation_.recovery_wal_pos) {
            result.reason =
                RawObservationalGateReason::
                    kAppendBeforeRecovery;
            return result;
        }
        if (!heartbeat_published_) {
            result.reason =
                RawObservationalGateReason::
                    kHeartbeatMissing;
            return result;
        }
        if (now_monotonic_ns < heartbeat_monotonic_ns_) {
            result.reason =
                RawObservationalGateReason::
                    kHeartbeatClockRegression;
            return result;
        }
        result.heartbeat_age_ns =
            now_monotonic_ns - heartbeat_monotonic_ns_;
        if (result.heartbeat_age_ns >
            config_.heartbeat_timeout_ns) {
            result.reason =
                RawObservationalGateReason::
                    kHeartbeatTimedOut;
            return result;
        }
        if (!healthy_) {
            result.reason =
                RawObservationalGateReason::
                    kObserverUnhealthy;
            return result;
        }
        if (!EvidenceComplete()) {
            result.reason =
                RawObservationalGateReason::
                    kEvidenceIncomplete;
            return result;
        }
        if (sampled_append.append_global_wal_pos >
            processed_wal_pos_) {
            result.lag_bytes =
                sampled_append.append_global_wal_pos -
                processed_wal_pos_;
            result.reason =
                result.lag_bytes > config_.maximum_lag_bytes
                    ? RawObservationalGateReason::
                          kLagExceeded
                    : RawObservationalGateReason::
                          kNotCaughtUp;
            return result;
        }

        // processed_wal_pos_ may be greater than this sampled value because
        // the observer can advance after the caller acquires control.page.
        result.ready = true;
        result.reason = RawObservationalGateReason::kReady;
        return result;
    }

private:
    [[nodiscard]] bool EvidenceComplete() const noexcept {
        return active_ &&
               healthy_ &&
               logon_generation_ != 0U &&
               latest_logon_ok_ &&
               logon_failed_responses_ == 0U &&
               required_subscription_failure_observed_mask_ == 0U &&
               required_subscription_ok_mask_ == required_mask_ &&
               required_subscription_failed_mask_ == 0U &&
               required_first_seen_mask_ == required_mask_;
    }

    [[nodiscard]] RawReadinessObserveResult ObserveBody(
        const RawRecordView& record) {
        const RawRecordHeaderV1& header = record.header();
        const l2flow::sdk::MessageKey key{
            header.vendor_service_id,
            header.vendor_service_version,
            header.vendor_message_id,
        };

        if (key.service_id == kSystemServiceId) {
            if (key.message_id == kLogonResponseMessageId) {
                if (logon_generation_ ==
                    std::numeric_limits<std::uint64_t>::max()) {
                    return RawReadinessObserveResult::
                        kMalformedControl;
                }
                ++logon_generation_;
                latest_logon_ok_ = false;
                required_first_seen_mask_ = 0U;
                required_subscription_failed_mask_ = 0U;
                required_subscription_ok_mask_ = 0U;
                if (key.service_version !=
                    kSystemServiceVersion) {
                    return RawReadinessObserveResult::
                        kMalformedControl;
                }

                std::uint32_t return_code = 0U;
                if (record.vendor_body().size() <
                        kLogonResponseBytes ||
                    !LoadU32(
                        record.vendor_body(),
                        kLogonReturnCodeOffset,
                        &return_code)) {
                    return RawReadinessObserveResult::
                        kMalformedControl;
                }
                const bool accepted =
                    return_code ==
                    static_cast<std::uint32_t>(
                        datayes::mdl::MDLEC_OK);
                if (!ObserveSubscriptionStatuses(
                        record.vendor_body(),
                        kLogonServicesListOffset,
                        kLogonResponseBytes,
                        accepted)) {
                    return RawReadinessObserveResult::
                        kMalformedControl;
                }
                latest_logon_ok_ = accepted;
                if (!accepted) {
                    ++logon_failed_responses_;
                }
            } else if (
                key.message_id ==
                kSubscribeResponseMessageId) {
                if (key.service_version !=
                        kSystemServiceVersion ||
                    !ObserveSubscriptionStatuses(
                        record.vendor_body(),
                        kSubscribeServicesListOffset,
                        kMdlListBytes,
                        true)) {
                    return RawReadinessObserveResult::
                        kMalformedControl;
                }
            }
        }

        if (key.service_id != config_.market_service_id) {
            return RawReadinessObserveResult::kProcessed;
        }
        for (std::size_t index = 0U;
             index < config_.required_market_messages.size();
             ++index) {
            if (key != config_.required_market_messages[index]) {
                continue;
            }
            const std::optional<std::size_t> fixed_body_bytes =
                l2flow::sdk::RequiredMessageFixedBodyBytes(key);
            if (fixed_body_bytes.has_value() &&
                record.vendor_body().size() >=
                    *fixed_body_bytes &&
                ValidateRequiredMarketBodyBounds(
                    key, record.vendor_body())) {
                required_first_seen_mask_ |=
                    std::uint64_t{1U} << index;
            }
            break;
        }
        return RawReadinessObserveResult::kProcessed;
    }

    [[nodiscard]] bool ObserveSubscriptionStatuses(
        std::span<const std::byte> body,
        std::size_t services_list_offset,
        std::size_t minimum_services_start,
        bool apply_statuses) {
        constexpr std::size_t kMaximumServices = 4096U;
        constexpr std::size_t kMaximumMessagesPerService =
            1'000'000U;
        CheckedListRange services;
        if (!ReadListRange(
                body,
                services_list_offset,
                kServiceItemBytes,
                kMaximumServices,
                &services)) {
            return false;
        }
        std::size_t services_end = 0U;
        if (services.count >
                std::numeric_limits<std::size_t>::max() /
                    kServiceItemBytes ||
            !CheckedAddSize(
                services.start,
                services.count * kServiceItemBytes,
                &services_end) ||
            (services.count != 0U &&
             services.start < minimum_services_start)) {
            return false;
        }

        std::uint64_t seen_mask = 0U;
        std::uint64_t ok_mask = 0U;
        std::uint64_t failed_mask = 0U;
        const std::size_t aggregate_message_budget =
            body.size() / kMessageStatusItemBytes;
        std::size_t aggregate_messages = 0U;
        for (std::size_t service_index = 0U;
             service_index < services.count;
             ++service_index) {
            const std::size_t service_offset =
                services.start +
                service_index * kServiceItemBytes;
            std::uint32_t service_id = 0U;
            std::uint32_t service_version = 0U;
            if (!LoadU32(
                    body,
                    service_offset + kServiceIdOffset,
                    &service_id) ||
                !LoadU32(
                    body,
                    service_offset + kServiceVersionOffset,
                    &service_version)) {
                return false;
            }
            const std::size_t messages_descriptor =
                service_offset +
                kServiceMessagesListOffset;
            CheckedListRange messages;
            if (!ReadListRange(
                    body,
                    messages_descriptor,
                    kMessageStatusItemBytes,
                    kMaximumMessagesPerService,
                    &messages)) {
                return false;
            }
            if (messages.count != 0U &&
                messages.start < services_end) {
                return false;
            }
            if (aggregate_messages >
                    aggregate_message_budget ||
                messages.count >
                    aggregate_message_budget -
                        aggregate_messages) {
                return false;
            }
            aggregate_messages += messages.count;

            for (std::size_t message_index = 0U;
                 message_index < messages.count;
                 ++message_index) {
                const std::size_t message_offset =
                    messages.start +
                    message_index *
                        kMessageStatusItemBytes;
                std::uint32_t message_id = 0U;
                std::uint32_t status = 0U;
                if (!LoadU32(
                        body,
                        message_offset + kMessageIdOffset,
                        &message_id) ||
                    !LoadU32(
                        body,
                        message_offset + kMessageStatusOffset,
                        &status)) {
                    return false;
                }

                for (std::size_t required_index = 0U;
                     required_index <
                         config_.required_market_messages.size();
                     ++required_index) {
                    const l2flow::sdk::MessageKey& required =
                        config_.required_market_messages[
                            required_index];
                    if (service_id != required.service_id ||
                        service_version !=
                            required.service_version ||
                        message_id != required.message_id) {
                        continue;
                    }
                    const std::uint64_t bit =
                        std::uint64_t{1U}
                        << required_index;
                    if ((seen_mask & bit) != 0U) {
                        return false;
                    }
                    seen_mask |= bit;
                    if (status ==
                        static_cast<std::uint32_t>(
                            datayes::mdl::MDLEC_OK)) {
                        ok_mask |= bit;
                    } else {
                        failed_mask |= bit;
                    }
                    break;
                }
            }
        }

        if (apply_statuses) {
            required_subscription_failed_mask_ =
                (required_subscription_failed_mask_ &
                 ~seen_mask) |
                failed_mask;
            required_subscription_ok_mask_ =
                (required_subscription_ok_mask_ &
                 ~seen_mask) |
                ok_mask;
            required_subscription_failure_observed_mask_ |=
                failed_mask;
        }
        return true;
    }

    RawReadinessObserverConfig config_;
    const std::uint64_t required_mask_;
    mutable std::mutex mutex_;
    RawReadinessObserverGeneration generation_{};
    bool active_ = false;
    bool healthy_ = false;
    bool heartbeat_published_ = false;
    std::uint64_t processed_wal_pos_ = 0U;
    std::uint64_t processed_ingress_sequence_ = 0U;
    std::uint64_t next_ingress_sequence_ = 0U;
    std::uint32_t current_segment_sequence_ = 0U;
    std::uint64_t current_segment_base_wal_pos_ = 0U;
    std::uint64_t current_segment_offset_ = 0U;
    std::uint64_t heartbeat_monotonic_ns_ = 0U;
    std::uint64_t logon_generation_ = 0U;
    std::uint64_t logon_failed_responses_ = 0U;
    std::uint64_t required_subscription_failure_observed_mask_ = 0U;
    std::uint64_t required_first_seen_mask_ = 0U;
    std::uint64_t required_subscription_ok_mask_ = 0U;
    std::uint64_t required_subscription_failed_mask_ = 0U;
    bool latest_logon_ok_ = false;
};

RawReadinessObserver::RawReadinessObserver(
    RawReadinessObserverConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

RawReadinessObserver::~RawReadinessObserver() = default;

RawReadinessObserverCreateError RawReadinessObserver::Create(
    RawReadinessObserverConfig config,
    std::unique_ptr<RawReadinessObserver>* output) {
    if (output == nullptr) {
        return RawReadinessObserverCreateError::kNullOutput;
    }
    output->reset();
    if (!IsValidConfig(config)) {
        return RawReadinessObserverCreateError::kInvalidConfig;
    }
    try {
        *output = std::unique_ptr<RawReadinessObserver>(
            new RawReadinessObserver(std::move(config)));
    } catch (const std::bad_alloc&) {
        return RawReadinessObserverCreateError::
            kResourceExhausted;
    }
    return RawReadinessObserverCreateError::kNone;
}

bool RawReadinessObserver::BeginGeneration(
    const RawReadinessObserverGeneration& generation,
    std::uint64_t now_monotonic_ns) {
    return impl_->BeginGeneration(
        generation, now_monotonic_ns);
}

RawReadinessObserveResult RawReadinessObserver::Observe(
    const RawRecordView& record,
    const l2flow::common::Identity128& writer_instance,
    std::uint64_t now_monotonic_ns) {
    return impl_->Observe(
        record, writer_instance, now_monotonic_ns);
}

RawReadinessObserveResult RawReadinessObserver::Observe(
    const RawReplayRecord& record,
    const l2flow::common::Identity128& writer_instance,
    std::uint64_t now_monotonic_ns) {
    return impl_->Observe(
        record.view, writer_instance, now_monotonic_ns);
}

RawReadinessObserveResult
RawReadinessObserver::ObserveSegmentTransition(
    const RawLiveSegmentTransitionV1& transition,
    std::uint64_t now_monotonic_ns) {
    return impl_->ObserveSegmentTransition(
        transition, now_monotonic_ns);
}

bool RawReadinessObserver::PublishHeartbeat(
    const l2flow::common::Identity128& writer_instance,
    std::uint64_t now_monotonic_ns) {
    return impl_->PublishHeartbeat(
        writer_instance, now_monotonic_ns);
}

RawReadinessObserverSnapshot
RawReadinessObserver::Snapshot() const {
    return impl_->Snapshot();
}

RawObservationalGateResult RawReadinessObserver::Evaluate(
    const RawControlSnapshot& sampled_append,
    std::uint64_t now_monotonic_ns) const {
    return impl_->Evaluate(
        sampled_append, now_monotonic_ns);
}

}  // namespace l2flow::ingress
