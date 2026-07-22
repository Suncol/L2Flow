#include "l2flow/market/market_session.h"

#include "mdl_shl2_msg.h"
#include "mdl_szl2_msg.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <utility>

namespace l2flow::market {
namespace {

namespace sh = datayes::mdl::mdl_shl2_msg;
namespace sz = datayes::mdl::mdl_szl2_msg;

bool ValidFamilySet(
    const std::array<MarketStreamFamilyV1,
                     kSessionStoreStreamCountV1>& families) noexcept {
    std::array<bool, kSessionStoreStreamCountV1> seen{};
    for (const MarketStreamFamilyV1 family : families) {
        const std::size_t index = static_cast<std::size_t>(family);
        if (index >= seen.size() || seen[index]) {
            return false;
        }
        seen[index] = true;
    }
    return true;
}

}  // namespace

MarketSessionV1::MarketSessionV1(
    MarketSessionConfigV1 config,
    std::array<std::unique_ptr<MarketDecoderV1>,
               kSessionStoreStreamCountV1> decoders,
    std::unique_ptr<MarketSessionStoreV1> store) noexcept
    : config_(std::move(config)),
      decoders_(std::move(decoders)),
      store_(std::move(store)) {}

MarketSessionCreateErrorV1 MarketSessionV1::Create(
    MarketSessionConfigV1 config,
    std::unique_ptr<MarketSessionV1>* output) noexcept {
    if (output == nullptr) {
        return MarketSessionCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (!ValidFamilySet(config.families)) {
        return MarketSessionCreateErrorV1::kInvalidFamilySet;
    }

    try {
        std::array<std::unique_ptr<MarketDecoderV1>,
                   kSessionStoreStreamCountV1>
            decoders{};
        for (std::size_t index = 0U; index < decoders.size(); ++index) {
            const SessionStreamKeyV1& stream = config.store.streams[index];
            MarketDecoderConfigV1 decoder_config;
            decoder_config.trade_date = stream.capture_date;
            decoder_config.source_stream_id = stream.source_stream_id;
            decoder_config.instrument_registry =
                config.instrument_registry;
            decoder_config.limits = config.decoder_limits;
            decoders[index] =
                std::make_unique<MarketDecoderV1>(decoder_config);
            if (!decoders[index]->configuration_valid()) {
                return MarketSessionCreateErrorV1::
                    kInvalidDecoderConfiguration;
            }
        }

        std::unique_ptr<MarketSessionStoreV1> store;
        const SessionStoreCreateErrorV1 store_error =
            MarketSessionStoreV1::Create(config.store, &store);
        if (store_error != SessionStoreCreateErrorV1::kNone) {
            return MarketSessionCreateErrorV1::kStoreCreateFailed;
        }
        output->reset(new MarketSessionV1(
            std::move(config), std::move(decoders), std::move(store)));
        return MarketSessionCreateErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return MarketSessionCreateErrorV1::kResourceExhausted;
    } catch (...) {
        return MarketSessionCreateErrorV1::kUnexpectedFailure;
    }
}

std::size_t MarketSessionV1::FindSource(
    std::uint32_t source_stream_id) const noexcept {
    for (std::size_t index = 0U;
         index < config_.store.streams.size();
         ++index) {
        if (config_.store.streams[index].source_stream_id ==
            source_stream_id) {
            return index;
        }
    }
    return kSessionStoreStreamCountV1;
}

bool MarketSessionV1::MessageMatchesFamily(
    const MarketMessageViewV1& input,
    MarketStreamFamilyV1 family) const noexcept {
    switch (family) {
        case MarketStreamFamilyV1::kShanghaiSnapshot:
            return input.service_id == sh::SHL2MarketData::ServiceID &&
                   input.message_id == sh::SHL2MarketData::MessageID;
        case MarketStreamFamilyV1::kShanghaiTick:
            return input.service_id == sh::NGTSTick::ServiceID &&
                   input.message_id == sh::NGTSTick::MessageID;
        case MarketStreamFamilyV1::kShenzhenSnapshot:
            return input.service_id == sz::Snapshot300111_v2::ServiceID &&
                   input.message_id == sz::Snapshot300111_v2::MessageID;
        case MarketStreamFamilyV1::kShenzhenTick:
            return input.service_id == sz::Order300192_v2::ServiceID &&
                   (input.message_id == sz::Order300192_v2::MessageID ||
                    input.message_id ==
                        sz::Transaction300191_v2::MessageID);
    }
    return false;
}

void MarketSessionV1::Poison(std::size_t stream_index) noexcept {
    poisoned_[stream_index] = true;
}

MarketSessionInjectResultV1 MarketSessionV1::Inject(
    const MarketMessageViewV1& input) noexcept {
    MarketSessionInjectResultV1 result;
    result.stream_index = FindSource(input.source_stream_id);
    if (result.stream_index == kSessionStoreStreamCountV1) {
        result.error = MarketSessionInjectErrorV1::kUnknownSourceStream;
        return result;
    }

    std::lock_guard<std::mutex> source_lock(
        source_mutexes_[result.stream_index]);
    if (poisoned_[result.stream_index]) {
        result.error = MarketSessionInjectErrorV1::kSourcePoisoned;
        result.source_poisoned = true;
        return result;
    }
    if (!MessageMatchesFamily(
            input, config_.families[result.stream_index])) {
        Poison(result.stream_index);
        result.error = MarketSessionInjectErrorV1::kWrongStreamFamily;
        result.source_poisoned = true;
        return result;
    }
    if (input.recv_monotonic_ns < 0) {
        Poison(result.stream_index);
        result.error = MarketSessionInjectErrorV1::kInvalidReceiveTime;
        result.source_poisoned = true;
        return result;
    }

    DecodedMarketEventV1 decoded;
    result.decode_error =
        decoders_[result.stream_index]->Decode(input, &decoded);
    if (result.decode_error != MarketDecodeErrorV1::kNone) {
        Poison(result.stream_index);
        result.error = MarketSessionInjectErrorV1::kDecodeFailed;
        result.source_poisoned = true;
        return result;
    }

    RetainedMarketEventV1 retained;
    result.retain_error =
        RetainMarketEventV1(std::move(decoded), &retained);
    if (result.retain_error !=
        RetainedMarketEventCreateErrorV1::kNone) {
        Poison(result.stream_index);
        result.error = MarketSessionInjectErrorV1::kRetainFailed;
        result.source_poisoned = true;
        return result;
    }

    const std::size_t estimated_bytes =
        EstimateRetainedMarketEventBytesV1(retained);
    if (estimated_bytes == std::numeric_limits<std::size_t>::max() ||
        static_cast<std::uintmax_t>(estimated_bytes) >
            std::numeric_limits<std::uint64_t>::max()) {
        Poison(result.stream_index);
        result.error = MarketSessionInjectErrorV1::kStoreAppendFailed;
        result.append.error =
            SessionAppendErrorV1::kMaxPayloadBytesExceeded;
        result.source_poisoned = true;
        return result;
    }

    SessionOwnedRecordV1<RetainedMarketEventV1> record{
        config_.store.streams[result.stream_index],
        input.source_sequence,
        static_cast<std::uint64_t>(input.recv_monotonic_ns),
        static_cast<std::uint64_t>(estimated_bytes),
        std::move(retained)};
    result.append = store_->Append(std::move(record));
    if (!result.append.accepted()) {
        Poison(result.stream_index);
        result.error = MarketSessionInjectErrorV1::kStoreAppendFailed;
        result.source_poisoned = true;
        return result;
    }
    return result;
}

bool MarketSessionV1::source_poisoned(
    std::size_t stream_index) const noexcept {
    if (stream_index >= source_mutexes_.size()) {
        return true;
    }
    std::lock_guard<std::mutex> lock(source_mutexes_[stream_index]);
    return poisoned_[stream_index];
}

}  // namespace l2flow::market
