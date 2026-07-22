#include "l2flow/ingress/raw_reader.h"
#include "l2flow/ingress/raw_v1.h"
#include "l2flow/market/raw_market_adapter_v1.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string_view>
#include <vector>

namespace ingress = l2flow::ingress;
namespace market = l2flow::market;

namespace {

int failures = 0;

void Expect(bool condition, std::string_view description) {
    if (!condition) {
        ++failures;
        std::cerr << "FAIL: " << description << '\n';
    }
}

void StoreLittleEndian(
    std::array<std::byte, ingress::kVendorMessageHeadBytes>* head,
    std::size_t offset,
    std::uint64_t value,
    std::size_t width) {
    for (std::size_t index = 0U; index < width; ++index) {
        (*head)[offset + index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
}

ingress::RawOwnedRecordResult MakeRecord() {
    ingress::RawRecordInputV1 input{};
    input.meta.source_stream_id = 1002U;
    input.meta.capture_date = 20260722U;
    input.meta.ingress_sequence = 17U;
    input.meta.recv_realtime_ns = 123'456U;
    input.meta.recv_monotonic_ns = 123'000U;
    input.vendor_head.fill(std::byte{0});
    input.vendor_head[0U] =
        static_cast<std::byte>(ingress::kVendorMessageHeadBytes);
    StoreLittleEndian(
        &input.vendor_head,
        1U,
        ingress::kVendorMessageHeadBytes + 3U,
        4U);
    input.vendor_head[5U] = std::byte{7U};
    input.vendor_head[6U] = std::byte{4U};
    StoreLittleEndian(&input.vendor_head, 7U, 101U, 2U);
    StoreLittleEndian(&input.vendor_head, 9U, 24U, 2U);
    StoreLittleEndian(&input.vendor_head, 11U, 930'001U, 4U);
    StoreLittleEndian(&input.vendor_head, 15U, 998'877U, 8U);
    const std::array<std::byte, 3U> body{
        std::byte{0x11U}, std::byte{0x22U}, std::byte{0x33U}};
    input.vendor_body = body;

    std::vector<std::byte> encoded;
    Expect(
        ingress::EncodeRawRecordV1(input, &encoded) ==
            ingress::RawV1Error::kNone,
        "fixture Raw record encodes");

    ingress::SegmentHeaderV1 segment{};
    segment.source_stream_id = input.meta.source_stream_id;
    segment.capture_date = input.meta.capture_date;
    segment.segment_base_wal_pos = 0U;
    return ingress::DecodeOwnedRawRecordV1(
        std::make_shared<const std::vector<std::byte>>(
            std::move(encoded)),
        segment,
        ingress::kRawV1SegmentHeaderBytes,
        input.meta.ingress_sequence);
}

void TestMappingAndLifetime() {
    ingress::RawOwnedRecordResult owned = MakeRecord();
    Expect(owned.ok(), "fixture Raw record decodes");
    if (!owned.ok()) {
        return;
    }

    market::MarketMessageViewV1 message{};
    const auto error = market::MakeMarketMessageViewFromRawV1(
        *owned.record, 1002U, 20260722U, 20260722U, &message);
    Expect(
        error == market::RawMarketAdapterErrorV1::kNone,
        "valid Raw record maps");
    Expect(message.source_stream_id == 1002U, "source maps");
    Expect(message.trade_date == 20260722U, "trade date is explicit");
    Expect(message.source_sequence == 17U, "source sequence maps");
    Expect(message.service_id == 4U, "service maps");
    Expect(message.service_version == 101U, "version maps");
    Expect(message.message_id == 24U, "message maps");
    Expect(message.message_encoding == 7U, "encoding maps");
    Expect(
        message.vendor_local_time_raw == 930'001U,
        "vendor local time maps");
    Expect(
        message.vendor_sequence_id == 998'877U,
        "vendor sequence maps");
    Expect(message.recv_realtime_ns == 123'456, "realtime maps");
    Expect(message.recv_monotonic_ns == 123'000, "monotonic maps");
    Expect(message.body.size() == 3U, "borrowed body maps");
    Expect(
        message.body[1U] == std::byte{0x22U},
        "borrowed body content maps");
}

void TestFailClosedValidation() {
    ingress::RawOwnedRecordResult owned = MakeRecord();
    Expect(owned.ok(), "validation fixture decodes");
    if (!owned.ok()) {
        return;
    }

    market::MarketMessageViewV1 message{};
    Expect(
        market::MakeMarketMessageViewFromRawV1(
            *owned.record, 1001U, 20260722U, 20260722U, &message) ==
            market::RawMarketAdapterErrorV1::kNamespaceMismatch,
        "foreign source is rejected");
    Expect(
        market::MakeMarketMessageViewFromRawV1(
            *owned.record, 1002U, 20260722U, 20260229U, &message) ==
            market::RawMarketAdapterErrorV1::kInvalidTradeDate,
        "invalid calendar date is rejected");
    Expect(
        market::MakeMarketMessageViewFromRawV1(
            *owned.record, 1002U, 20260722U, 10000101U, &message) ==
            market::RawMarketAdapterErrorV1::kInvalidTradeDate,
        "five-digit calendar year is rejected");
    Expect(
        market::MakeMarketMessageViewFromRawV1(
            *owned.record, 1002U, 20260722U, 20260722U, nullptr) ==
            market::RawMarketAdapterErrorV1::kNullOutput,
        "null output is rejected");

    ingress::RawLiveRecord live{
        *owned.record,
        {},
        ingress::RawLiveRecordProvenance::kDurable,
        {},
        1U};
    live.segment.source_stream_id = 1002U;
    live.segment.capture_date = 20260721U;
    Expect(
        market::MakeMarketMessageViewFromRawV1(
            live, 20260722U, &message) ==
            market::RawMarketAdapterErrorV1::kNamespaceMismatch,
        "live segment namespace mismatch is rejected");
}

}  // namespace

int main() {
    TestMappingAndLifetime();
    TestFailClosedValidation();
    if (failures != 0) {
        std::cerr << failures << " test(s) failed\n";
        return 1;
    }
    std::cout << "production Raw market adapter tests passed\n";
    return 0;
}
