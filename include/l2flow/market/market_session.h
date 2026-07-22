#pragma once

#include "l2flow/market/market_decoder.h"
#include "l2flow/market/session_store.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>

namespace l2flow::market {

class InstrumentRegistryV1;

enum class MarketStreamFamilyV1 : std::uint8_t {
    kShanghaiSnapshot = 0U,
    kShanghaiTick,
    kShenzhenSnapshot,
    kShenzhenTick,
};

using MarketSessionStoreV1 = SessionStoreV1<RetainedMarketEventV1>;
using MarketSessionSnapshotV1 =
    SessionStoreSnapshotV1<RetainedMarketEventV1>;

struct MarketSessionConfigV1 final {
    SessionStoreConfigV1 store{};
    // Indexes correspond to store.streams.  The default production topology
    // is SH snapshot, SH tick, SZ snapshot, SZ tick; SZ tick accepts both
    // 6.33 and 6.36 in their one source order.
    std::array<MarketStreamFamilyV1, kSessionStoreStreamCountV1>
        families{
            MarketStreamFamilyV1::kShanghaiSnapshot,
            MarketStreamFamilyV1::kShanghaiTick,
            MarketStreamFamilyV1::kShenzhenSnapshot,
            MarketStreamFamilyV1::kShenzhenTick};
    // Borrowed immutable registry.  When non-null, it must outlive the
    // created MarketSessionV1 and all Inject calls.
    const InstrumentRegistryV1* instrument_registry = nullptr;
    MarketDecoderLimitsV1 decoder_limits{};
};

enum class MarketSessionCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidFamilySet,
    kInvalidDecoderConfiguration,
    kStoreCreateFailed,
    kResourceExhausted,
    kUnexpectedFailure,
};

enum class MarketSessionInjectErrorV1 : std::uint8_t {
    kNone = 0U,
    kUnknownSourceStream,
    kSourcePoisoned,
    kWrongStreamFamily,
    kInvalidReceiveTime,
    kDecodeFailed,
    kRetainFailed,
    kStoreAppendFailed,
};

struct MarketSessionInjectResultV1 final {
    MarketSessionInjectErrorV1 error =
        MarketSessionInjectErrorV1::kNone;
    MarketDecodeErrorV1 decode_error = MarketDecodeErrorV1::kNone;
    RetainedMarketEventCreateErrorV1 retain_error =
        RetainedMarketEventCreateErrorV1::kNone;
    // Populated only when admission reaches the store.  This compact result
    // avoids taking the store mutex a second time to build a full diagnostic
    // watermark on every feeder callback; Watermark() remains available off
    // the hot path.
    SessionAppendResultV1 append{};
    std::size_t stream_index = kSessionStoreStreamCountV1;
    bool source_poisoned = false;

    [[nodiscard]] bool accepted() const noexcept {
        return error == MarketSessionInjectErrorV1::kNone;
    }
};

// Direct injection seam for an existing feeder/Raw consumer.  It performs no
// CSV or WAL I/O.  Callers pass a synchronous head/body-derived view; Decode
// copies all factor-relevant fields before Inject returns, and the session
// store retains the owned event according to its explicit retention budget.
// For each source, the feeder must call Inject serially in authoritative,
// strictly increasing source_sequence order.  The per-source mutex prevents
// data races but deliberately does not reorder concurrently submitted calls;
// an out-of-order admission fail-stops that source instead of hiding a hole.
class MarketSessionV1 final {
public:
    MarketSessionV1(const MarketSessionV1&) = delete;
    MarketSessionV1& operator=(const MarketSessionV1&) = delete;
    MarketSessionV1(MarketSessionV1&&) = delete;
    MarketSessionV1& operator=(MarketSessionV1&&) = delete;
    ~MarketSessionV1() = default;

    [[nodiscard]] static MarketSessionCreateErrorV1 Create(
        MarketSessionConfigV1 config,
        std::unique_ptr<MarketSessionV1>* output) noexcept;

    [[nodiscard]] MarketSessionInjectResultV1 Inject(
        const MarketMessageViewV1& input) noexcept;

    [[nodiscard]] SessionSnapshotErrorV1 Snapshot(
        MarketSessionSnapshotV1* output) noexcept {
        return store_->Snapshot(output);
    }

    [[nodiscard]] SessionStoreWatermarkV1 Watermark()
        const noexcept {
        return store_->Watermark();
    }

    [[nodiscard]] bool source_poisoned(
        std::size_t stream_index) const noexcept;

    [[nodiscard]] const MarketSessionConfigV1& config()
        const noexcept {
        return config_;
    }

private:
    MarketSessionV1(
        MarketSessionConfigV1 config,
        std::array<std::unique_ptr<MarketDecoderV1>,
                   kSessionStoreStreamCountV1> decoders,
        std::unique_ptr<MarketSessionStoreV1> store) noexcept;

    [[nodiscard]] std::size_t FindSource(
        std::uint32_t source_stream_id) const noexcept;
    [[nodiscard]] bool MessageMatchesFamily(
        const MarketMessageViewV1& input,
        MarketStreamFamilyV1 family) const noexcept;
    void Poison(std::size_t stream_index) noexcept;

    MarketSessionConfigV1 config_{};
    std::array<std::unique_ptr<MarketDecoderV1>,
               kSessionStoreStreamCountV1>
        decoders_{};
    std::unique_ptr<MarketSessionStoreV1> store_;
    mutable std::array<std::mutex, kSessionStoreStreamCountV1>
        source_mutexes_{};
    std::array<bool, kSessionStoreStreamCountV1> poisoned_{};
};

}  // namespace l2flow::market
