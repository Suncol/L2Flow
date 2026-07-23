#include "l2flow/canonical/market_sequence_evidence_v1.h"

#include <bit>
#include <limits>
#include <new>
#include <span>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace l2flow::canonical {
namespace {

using l2flow::market::DecodedMarketCommonV1;
using l2flow::market::DecimalValueV1;
using l2flow::market::QuantityValueV1;
using l2flow::market::ShanghaiTickV1;
using l2flow::market::ShenzhenOrderV1;
using l2flow::market::ShenzhenTransactionV1;
using l2flow::market::TickFieldsV1;

class ExchangeEvidenceBuilderV1 final {
public:
    explicit ExchangeEvidenceBuilderV1(
        std::vector<std::byte>* output) : output_(output) {}

    [[nodiscard]] bool Begin() {
        output_->clear();
        output_->reserve(256U);
        return Text("L2FLOW_EXCHANGE_BUSINESS_EVIDENCE_V1");
    }

    void U8(std::uint8_t value) {
        output_->push_back(static_cast<std::byte>(value));
    }

    void U32(std::uint32_t value) {
        for (std::size_t index = 0U; index < 4U; ++index) {
            U8(static_cast<std::uint8_t>(
                (value >> (index * 8U)) & 0xffU));
        }
    }

    void U64(std::uint64_t value) {
        for (std::size_t index = 0U; index < 8U; ++index) {
            U8(static_cast<std::uint8_t>(
                (value >> (index * 8U)) & 0xffU));
        }
    }

    void I32(std::int32_t value) {
        U32(static_cast<std::uint32_t>(value));
    }

    void I64(std::int64_t value) {
        U64(static_cast<std::uint64_t>(value));
    }

    void Bool(bool value) {
        U8(value ? 1U : 0U);
    }

    [[nodiscard]] bool Text(std::string_view value) {
        if (value.size() >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max())) {
            return false;
        }
        U32(static_cast<std::uint32_t>(value.size()));
        const auto bytes = std::as_bytes(std::span(
            value.data(), value.size()));
        output_->insert(output_->end(), bytes.begin(), bytes.end());
        return true;
    }

    void Decimal(const DecimalValueV1& value) {
        I64(value.raw);
        I64(value.normalized_p6);
        U8(value.scale);
        Bool(value.valid);
        Bool(value.is_null);
    }

    void Quantity(const QuantityValueV1& value) {
        I64(value.raw);
        U8(value.scale);
        Bool(value.valid);
        Bool(value.is_null);
    }

    [[nodiscard]] bool Common(const DecodedMarketCommonV1& value) {
        U8(static_cast<std::uint8_t>(value.kind));
        U8(static_cast<std::uint8_t>(value.market));
        U32(value.exchange_time.raw_hhmmssmmm);
        U64(value.exchange_time.nanoseconds_since_midnight);
        I64(value.exchange_time.unix_nanoseconds);
        Bool(value.exchange_time.valid);
        Bool(value.exchange_time.is_null);
        Bool(value.exchange_time.unix_nanoseconds_valid);
        return Text(value.security_id) &&
               Text(value.security_id_source) &&
               Text(value.md_stream_id) &&
               AppendCommonValidity(value);
    }

    void Fields(const TickFieldsV1& value) {
        U8(static_cast<std::uint8_t>(value.action));
        U8(static_cast<std::uint8_t>(value.side));
        U8(static_cast<std::uint8_t>(value.order_type));
        U8(static_cast<std::uint8_t>(value.aggressor));
        U8(static_cast<std::uint8_t>(value.phase));
        Decimal(value.price);
        Quantity(value.quantity);
        Decimal(value.trade_amount);
        Quantity(value.matched_quantity);
        I64(value.primary_order_id);
        I64(value.buy_order_id);
        I64(value.sell_order_id);
        U32(value.validity_bitmap);
    }

private:
    [[nodiscard]] bool AppendCommonValidity(
        const DecodedMarketCommonV1& value) {
        Bool(value.security_id_valid);
        Bool(value.security_id_source_valid);
        Bool(value.md_stream_id_valid);
        return true;
    }

    std::vector<std::byte>* output_ = nullptr;
};

template <typename Append>
MarketSequenceEvidenceErrorV1 BuildExchange(
    std::vector<std::byte>* output,
    Append append) noexcept {
    if (output == nullptr) {
        return MarketSequenceEvidenceErrorV1::kNullOutput;
    }
    try {
        ExchangeEvidenceBuilderV1 builder(output);
        if (!builder.Begin() || !append(&builder)) {
            output->clear();
            return MarketSequenceEvidenceErrorV1::kValueTooLarge;
        }
        return MarketSequenceEvidenceErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        output->clear();
        return MarketSequenceEvidenceErrorV1::kResourceExhausted;
    } catch (const std::length_error&) {
        output->clear();
        return MarketSequenceEvidenceErrorV1::kResourceExhausted;
    } catch (...) {
        output->clear();
        return MarketSequenceEvidenceErrorV1::kUnexpectedFailure;
    }
}

}  // namespace

MarketBusinessSequenceIdentityV1
MarketBusinessSequenceIdentityOfV1(
    const l2flow::market::ShanghaiSnapshotV1&) noexcept {
    return {};
}

MarketBusinessSequenceIdentityV1
MarketBusinessSequenceIdentityOfV1(
    const ShanghaiTickV1& event) noexcept {
    MarketBusinessSequenceIdentityV1 result{};
    result.present = true;
    result.kind = SequenceScopeKindV1::kShanghaiChannel;
    result.channel = std::bit_cast<std::uint32_t>(event.channel);
    result.sequence_valid = event.business_index > 0;
    if (result.sequence_valid) {
        result.sequence =
            static_cast<std::uint64_t>(event.business_index);
    }
    return result;
}

MarketBusinessSequenceIdentityV1
MarketBusinessSequenceIdentityOfV1(
    const l2flow::market::ShenzhenSnapshotV1&) noexcept {
    return {};
}

MarketBusinessSequenceIdentityV1
MarketBusinessSequenceIdentityOfV1(
    const ShenzhenOrderV1& event) noexcept {
    MarketBusinessSequenceIdentityV1 result{};
    result.present = true;
    result.kind = SequenceScopeKindV1::kShenzhenUnifiedChannel;
    result.channel = event.channel;
    result.sequence_valid = event.application_sequence > 0;
    if (result.sequence_valid) {
        result.sequence =
            static_cast<std::uint64_t>(event.application_sequence);
    }
    return result;
}

MarketBusinessSequenceIdentityV1
MarketBusinessSequenceIdentityOfV1(
    const ShenzhenTransactionV1& event) noexcept {
    MarketBusinessSequenceIdentityV1 result{};
    result.present = true;
    result.kind = SequenceScopeKindV1::kShenzhenUnifiedChannel;
    result.channel = event.channel;
    result.sequence_valid = event.application_sequence > 0;
    if (result.sequence_valid) {
        result.sequence =
            static_cast<std::uint64_t>(event.application_sequence);
    }
    return result;
}

MarketBusinessSequenceIdentityV1
MarketBusinessSequenceIdentityOfV1(
    const l2flow::market::DecodedMarketEventV1& event) noexcept {
    return std::visit(
        [](const auto& value) noexcept {
            return MarketBusinessSequenceIdentityOfV1(value);
        },
        event);
}

MarketSequenceEvidenceErrorV1 BuildVendorSequenceEvidenceV1(
    const l2flow::market::MarketMessageViewV1& message,
    std::vector<std::byte>* output) noexcept {
    if (output == nullptr) {
        return MarketSequenceEvidenceErrorV1::kNullOutput;
    }
    if (message.body.size() >
        std::numeric_limits<std::size_t>::max() - 7U) {
        output->clear();
        return MarketSequenceEvidenceErrorV1::kValueTooLarge;
    }
    try {
        output->clear();
        output->reserve(7U + message.body.size());
        output->push_back(static_cast<std::byte>(
            message.service_version & 0xffU));
        output->push_back(static_cast<std::byte>(
            (message.service_version >> 8U) & 0xffU));
        output->push_back(
            static_cast<std::byte>(message.message_encoding));
        for (std::size_t index = 0U; index < 4U; ++index) {
            output->push_back(static_cast<std::byte>(
                (message.vendor_local_time_raw >> (index * 8U)) &
                0xffU));
        }
        output->insert(
            output->end(), message.body.begin(), message.body.end());
        return MarketSequenceEvidenceErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        output->clear();
        return MarketSequenceEvidenceErrorV1::kResourceExhausted;
    } catch (const std::length_error&) {
        output->clear();
        return MarketSequenceEvidenceErrorV1::kResourceExhausted;
    } catch (...) {
        output->clear();
        return MarketSequenceEvidenceErrorV1::kUnexpectedFailure;
    }
}

MarketSequenceEvidenceErrorV1 BuildExchangeSequenceEvidenceV1(
    const ShanghaiTickV1& event,
    std::vector<std::byte>* output) noexcept {
    return BuildExchange(
        output,
        [&event](ExchangeEvidenceBuilderV1* evidence) {
            if (!evidence->Common(event.common)) {
                return false;
            }
            evidence->U8(1U);
            evidence->I64(event.business_index);
            evidence->I32(event.channel);
            if (!evidence->Text(event.raw_type) ||
                !evidence->Text(event.raw_tick_flag)) {
                return false;
            }
            evidence->Bool(event.raw_type_valid);
            evidence->Bool(event.raw_tick_flag_valid);
            evidence->Fields(event.fields);
            return true;
        });
}

MarketSequenceEvidenceErrorV1 BuildExchangeSequenceEvidenceV1(
    const ShenzhenOrderV1& event,
    std::vector<std::byte>* output) noexcept {
    return BuildExchange(
        output,
        [&event](ExchangeEvidenceBuilderV1* evidence) {
            if (!evidence->Common(event.common)) {
                return false;
            }
            evidence->U8(2U);
            evidence->U32(event.channel);
            evidence->I64(event.application_sequence);
            evidence->I32(event.raw_side);
            evidence->I32(event.raw_order_type);
            evidence->Fields(event.fields);
            return true;
        });
}

MarketSequenceEvidenceErrorV1 BuildExchangeSequenceEvidenceV1(
    const ShenzhenTransactionV1& event,
    std::vector<std::byte>* output) noexcept {
    return BuildExchange(
        output,
        [&event](ExchangeEvidenceBuilderV1* evidence) {
            if (!evidence->Common(event.common)) {
                return false;
            }
            evidence->U8(3U);
            evidence->U32(event.channel);
            evidence->I64(event.application_sequence);
            evidence->I32(event.raw_execution_type);
            evidence->Fields(event.fields);
            return true;
        });
}

MarketSequenceEvidenceErrorV1 BuildExchangeSequenceEvidenceV1(
    const l2flow::market::DecodedMarketEventV1& event,
    std::vector<std::byte>* output) noexcept {
    return std::visit(
        [output](const auto& value) noexcept {
            using Value = std::decay_t<decltype(value)>;
            if constexpr (
                std::is_same_v<Value, ShanghaiTickV1> ||
                std::is_same_v<Value, ShenzhenOrderV1> ||
                std::is_same_v<Value, ShenzhenTransactionV1>) {
                return BuildExchangeSequenceEvidenceV1(value, output);
            }
            if (output != nullptr) {
                output->clear();
            }
            return output == nullptr
                ? MarketSequenceEvidenceErrorV1::kNullOutput
                : MarketSequenceEvidenceErrorV1::
                      kNoBusinessSequence;
        },
        event);
}

std::string_view MarketSequenceEvidenceErrorNameV1(
    MarketSequenceEvidenceErrorV1 error) noexcept {
    switch (error) {
        case MarketSequenceEvidenceErrorV1::kNone:
            return "none";
        case MarketSequenceEvidenceErrorV1::kNullOutput:
            return "null_output";
        case MarketSequenceEvidenceErrorV1::kNoBusinessSequence:
            return "no_business_sequence";
        case MarketSequenceEvidenceErrorV1::kValueTooLarge:
            return "value_too_large";
        case MarketSequenceEvidenceErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case MarketSequenceEvidenceErrorV1::kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

}  // namespace l2flow::canonical
