#include "l2flow/market/raw_market_adapter_v1.h"

#include <limits>

namespace l2flow::market {

namespace {

bool ValidTradeDateShape(std::uint32_t value) noexcept {
    const std::uint32_t year = value / 10000U;
    const std::uint32_t month = (value / 100U) % 100U;
    const std::uint32_t day = value % 100U;
    if (year < 1992U || year > 9999U || month == 0U || month > 12U ||
        day == 0U) {
        return false;
    }
    constexpr std::uint32_t days_by_month[] = {
        31U, 28U, 31U, 30U, 31U, 30U,
        31U, 31U, 30U, 31U, 30U, 31U};
    std::uint32_t maximum = days_by_month[month - 1U];
    const bool leap =
        (year % 4U == 0U && year % 100U != 0U) || year % 400U == 0U;
    if (month == 2U && leap) {
        maximum = 29U;
    }
    return day <= maximum;
}

}  // namespace

const char* RawMarketAdapterErrorNameV1(
    RawMarketAdapterErrorV1 error) noexcept {
    switch (error) {
        case RawMarketAdapterErrorV1::kNone:
            return "none";
        case RawMarketAdapterErrorV1::kNullOutput:
            return "null_output";
        case RawMarketAdapterErrorV1::kInvalidTradeDate:
            return "invalid_trade_date";
        case RawMarketAdapterErrorV1::kNamespaceMismatch:
            return "namespace_mismatch";
        case RawMarketAdapterErrorV1::kInvalidIngressSequence:
            return "invalid_ingress_sequence";
        case RawMarketAdapterErrorV1::kReceiveTimeOutOfRange:
            return "receive_time_out_of_range";
        case RawMarketAdapterErrorV1::kBodySizeMismatch:
            return "body_size_mismatch";
    }
    return "invalid_raw_market_adapter_error";
}

RawMarketAdapterErrorV1 MakeMarketMessageViewFromRawV1(
    const l2flow::ingress::RawRecordView& record,
    std::uint32_t expected_source_stream_id,
    std::uint32_t expected_capture_date,
    std::uint32_t trade_date,
    MarketMessageViewV1* output) noexcept {
    if (output == nullptr) {
        return RawMarketAdapterErrorV1::kNullOutput;
    }
    if (!ValidTradeDateShape(trade_date)) {
        return RawMarketAdapterErrorV1::kInvalidTradeDate;
    }
    const l2flow::ingress::RawRecordHeaderV1& header = record.header();
    if (expected_source_stream_id == 0U || expected_capture_date == 0U ||
        header.source_stream_id != expected_source_stream_id ||
        header.capture_date != expected_capture_date) {
        return RawMarketAdapterErrorV1::kNamespaceMismatch;
    }
    if (header.ingress_sequence == 0U) {
        return RawMarketAdapterErrorV1::kInvalidIngressSequence;
    }
    constexpr std::uint64_t maximum_signed =
        static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
    if (header.recv_realtime_ns > maximum_signed ||
        header.recv_monotonic_ns > maximum_signed) {
        return RawMarketAdapterErrorV1::kReceiveTimeOutOfRange;
    }
    if (record.vendor_body().size() != header.vendor_body_size) {
        return RawMarketAdapterErrorV1::kBodySizeMismatch;
    }

    MarketMessageViewV1 candidate{};
    candidate.source_stream_id = header.source_stream_id;
    candidate.trade_date = trade_date;
    candidate.source_sequence = header.ingress_sequence;
    candidate.service_id = header.vendor_service_id;
    candidate.service_version = header.vendor_service_version;
    candidate.message_id = header.vendor_message_id;
    candidate.message_encoding = header.vendor_message_encoding;
    candidate.vendor_local_time_raw = header.vendor_local_time_raw;
    candidate.vendor_sequence_id = header.vendor_sequence_id;
    candidate.recv_realtime_ns =
        static_cast<std::int64_t>(header.recv_realtime_ns);
    candidate.recv_monotonic_ns =
        static_cast<std::int64_t>(header.recv_monotonic_ns);
    candidate.body = record.vendor_body();
    *output = candidate;
    return RawMarketAdapterErrorV1::kNone;
}

RawMarketAdapterErrorV1 MakeMarketMessageViewFromRawV1(
    const l2flow::ingress::RawLiveRecord& record,
    std::uint32_t trade_date,
    MarketMessageViewV1* output) noexcept {
    const l2flow::ingress::RawRecordHeaderV1& header = record.view.header();
    if (record.segment.source_stream_id == 0U ||
        record.segment.capture_date == 0U ||
        record.segment.source_stream_id != header.source_stream_id ||
        record.segment.capture_date != header.capture_date) {
        return output == nullptr
            ? RawMarketAdapterErrorV1::kNullOutput
            : RawMarketAdapterErrorV1::kNamespaceMismatch;
    }
    return MakeMarketMessageViewFromRawV1(
        record.view,
        record.segment.source_stream_id,
        record.segment.capture_date,
        trade_date,
        output);
}

}  // namespace l2flow::market
