#include "l2flow/market/market_types_v1.h"

#include <limits>
#include <string>
#include <type_traits>
#include <variant>

namespace l2flow::market {
namespace {

std::size_t SaturatingAdd(
    std::size_t left,
    std::size_t right) noexcept {
    if (left > std::numeric_limits<std::size_t>::max() - right) {
        return std::numeric_limits<std::size_t>::max();
    }
    return left + right;
}

std::size_t CommonDynamicBytes(
    const DecodedMarketCommonV1& common) noexcept {
    std::size_t result = common.security_id.capacity();
    result = SaturatingAdd(
        result, common.security_id_source.capacity());
    result = SaturatingAdd(result, common.md_stream_id.capacity());
    return result;
}

template <typename Value>
std::size_t AlternativeDynamicBytes(const Value& value) noexcept {
    std::size_t result = CommonDynamicBytes(value.common);
    if constexpr (std::is_same_v<Value, ShanghaiSnapshotV1>) {
        result = SaturatingAdd(
            result, value.instrument_status.capacity());
    } else if constexpr (std::is_same_v<Value, ShanghaiTickV1>) {
        result = SaturatingAdd(result, value.raw_type.capacity());
        result = SaturatingAdd(
            result, value.raw_tick_flag.capacity());
    } else if constexpr (
        std::is_same_v<Value, ShenzhenSnapshotV1>) {
        result = SaturatingAdd(
            result, value.trading_phase_code.capacity());
    }
    return result;
}

}  // namespace

const DecodedMarketCommonV1& MarketCommonV1(
    const DecodedMarketEventV1& event) noexcept {
    return std::visit(
        [](const auto& value) -> const DecodedMarketCommonV1& {
            return value.common;
        },
        event);
}

DecodedMarketCommonV1& MarketCommonV1(
    DecodedMarketEventV1& event) noexcept {
    return std::visit(
        [](auto& value) -> DecodedMarketCommonV1& {
            return value.common;
        },
        event);
}

RetainedMarketEventCreateErrorV1 RetainMarketEventV1(
    DecodedMarketEventV1 event,
    RetainedMarketEventV1* output) noexcept {
    if (output == nullptr) {
        return RetainedMarketEventCreateErrorV1::kNullOutput;
    }
    try {
        RetainedMarketEventV1 candidate = std::visit(
            [](auto&& value) -> RetainedMarketEventV1 {
                using Value = std::decay_t<decltype(value)>;
                using Owner = std::unique_ptr<const Value>;
                return RetainedMarketEventV1(
                    std::in_place_type<Owner>,
                    std::make_unique<const Value>(
                        std::move(value)));
            },
            std::move(event));
        *output = std::move(candidate);
        return RetainedMarketEventCreateErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return RetainedMarketEventCreateErrorV1::kResourceExhausted;
    } catch (...) {
        return RetainedMarketEventCreateErrorV1::kUnexpectedFailure;
    }
}

std::size_t EstimateOwnedMarketEventBytesV1(
    const DecodedMarketEventV1& event) noexcept {
    return std::visit(
        [](const auto& value) {
            std::size_t result = sizeof(DecodedMarketEventV1);
            using Value = std::decay_t<decltype(value)>;
            result = SaturatingAdd(
                result, AlternativeDynamicBytes<Value>(value));
            return result;
        },
        event);
}

std::size_t EstimateRetainedMarketEventBytesV1(
    const DecodedMarketEventV1& event) noexcept {
    return std::visit(
        [](const auto& value) {
            using Value = std::decay_t<decltype(value)>;
            std::size_t result = sizeof(RetainedMarketEventV1);
            result = SaturatingAdd(result, sizeof(Value));
            result = SaturatingAdd(
                result, AlternativeDynamicBytes<Value>(value));
            return result;
        },
        event);
}

std::size_t EstimateRetainedMarketEventBytesV1(
    const RetainedMarketEventV1& event) noexcept {
    return std::visit(
        [](const auto& owner) {
            using Element =
                typename std::decay_t<decltype(owner)>::element_type;
            using Value = std::remove_const_t<Element>;
            if (owner == nullptr) {
                return std::numeric_limits<std::size_t>::max();
            }
            std::size_t result = sizeof(RetainedMarketEventV1);
            result = SaturatingAdd(result, sizeof(Value));
            result = SaturatingAdd(
                result, AlternativeDynamicBytes<Value>(*owner));
            return result;
        },
        event);
}

}  // namespace l2flow::market
