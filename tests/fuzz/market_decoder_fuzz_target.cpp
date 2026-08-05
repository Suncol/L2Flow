#include "l2flow/market/market_decoder.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

namespace market = l2flow::market;

namespace {

struct MessageKey final {
    std::uint8_t service_id;
    std::uint16_t message_id;
};

// Fuzz-control prefix:
//   byte 0     core-message selector
//   byte 1     even => supported version 101, odd => bytes 2..3
//   bytes 2..3 candidate service version, little-endian
// Every byte after the fixed four-byte prefix is passed through as body data.
constexpr std::size_t kControlBytes = 4U;
constexpr std::uint16_t kSupportedServiceVersion = 101U;
constexpr std::uint32_t kTradeDate = 20260722U;
constexpr std::uint32_t kSourceStreamId = 404U;

constexpr std::array<MessageKey, 3U> kCoreMessages{{
    {4U, 24U},
    {6U, 33U},
    {6U, 36U},
}};

std::uint8_t ByteAtOrZero(
    const std::uint8_t* data,
    std::size_t size,
    std::size_t offset) noexcept {
    return offset < size ? data[offset] : std::uint8_t{0U};
}

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(
    const std::uint8_t* data,
    std::size_t size) {
    if (data == nullptr) {
        return 0;
    }

    const MessageKey key =
        kCoreMessages[ByteAtOrZero(data, size, 0U) %
                      kCoreMessages.size()];
    const std::uint8_t version_mode = ByteAtOrZero(data, size, 1U);
    const std::uint16_t candidate_version =
        static_cast<std::uint16_t>(
            ByteAtOrZero(data, size, 2U)) |
        static_cast<std::uint16_t>(
            static_cast<std::uint16_t>(
                ByteAtOrZero(data, size, 3U))
            << 8U);
    const std::uint16_t service_version =
        (version_mode & 1U) == 0U
            ? kSupportedServiceVersion
            : candidate_version;

    const std::size_t body_offset =
        size < kControlBytes ? size : kControlBytes;
    const std::span<const std::uint8_t> body_u8(
        data + body_offset,
        size - body_offset);

    market::MarketDecoderConfigV1 config{};
    config.trade_date = kTradeDate;
    config.source_stream_id = kSourceStreamId;
    market::MarketDecoderV1 decoder(config);

    market::MarketMessageViewV1 input{};
    input.source_stream_id = kSourceStreamId;
    input.trade_date = kTradeDate;
    input.source_sequence = 1U;
    input.service_id = key.service_id;
    input.service_version = service_version;
    input.message_id = key.message_id;
    input.message_encoding = 0U;
    input.vendor_local_time_raw = 93'000'000U;
    input.vendor_sequence_id = 1U;
    input.recv_realtime_ns = 1U;
    input.recv_monotonic_ns = 1U;
    input.body = std::as_bytes(body_u8);

    market::DecodedMarketEventV1 output{};
    const market::MarketDecodeErrorV1 result =
        decoder.Decode(input, &output);
    (void)result;
    return 0;
}
