#include "l2flow/runtime/realtime_fast_plane_v1.h"

#include "l2flow/canonical/market_sequence_evidence_v1.h"
#include "l2flow/ingress/byte_ring.h"
#include "l2flow/market/instrument_registry.h"
#include "l2flow/market/market_decoder.h"
#include "l2flow/market/market_types_v1.h"
#include "l2flow/sdk/vendor_head_view.h"

#include <algorithm>
#include <atomic>
#include <compare>
#include <deque>
#include <limits>
#include <map>
#include <mutex>
#include <new>
#include <optional>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>

namespace l2flow::runtime {
namespace {

using l2flow::ingress::ByteRingPopResult;
using l2flow::ingress::ByteRingPushResult;
using l2flow::ingress::FastCapturePublishResultV1;
using l2flow::market::InstrumentHistorySubmitErrorV1;
using l2flow::canonical::MarketBusinessSequenceIdentityV1;

enum class ExactDedupOutcomeV1 : std::uint8_t {
    kFirstSeen = 0U,
    kExactDuplicate,
    kConflict,
    kStorageFailure,
    kUnexpectedFailure,
};

struct ExactDedupSlotV1 final {
    std::uint64_t sequence = 0U;
    std::vector<std::byte> evidence;
};

static_assert(
    std::is_nothrow_move_constructible_v<ExactDedupSlotV1>);
static_assert(
    std::is_nothrow_move_assignable_v<ExactDedupSlotV1>);

class ExactSequenceIndexV1;

enum class ExactDedupStorageV1 : std::uint8_t {
    kNone = 0U,
    kRecordHigh,
    kLate,
};

struct ExactDedupTokenV1 final {
    ExactSequenceIndexV1* owner = nullptr;
    ExactDedupStorageV1 storage = ExactDedupStorageV1::kNone;
    std::uint64_t sequence = 0U;

    ExactDedupTokenV1() = default;
    ExactDedupTokenV1(const ExactDedupTokenV1&) = delete;
    ExactDedupTokenV1& operator=(const ExactDedupTokenV1&) = delete;
    ExactDedupTokenV1(ExactDedupTokenV1&&) = delete;
    ExactDedupTokenV1& operator=(ExactDedupTokenV1&&) = delete;
    ~ExactDedupTokenV1();

    [[nodiscard]] bool active() const noexcept {
        return owner != nullptr &&
               storage != ExactDedupStorageV1::kNone;
    }

    void Reset() noexcept {
        owner = nullptr;
        storage = ExactDedupStorageV1::kNone;
        sequence = 0U;
    }
};

struct ExactDedupPrepareResultV1 final {
    ExactDedupOutcomeV1 outcome =
        ExactDedupOutcomeV1::kUnexpectedFailure;
    std::size_t evidence_bytes = 0U;
    bool token_prepared = false;
};

class ExactSequenceIndexV1 final {
public:
    [[nodiscard]] ExactDedupPrepareResultV1 Prepare(
        std::uint64_t sequence,
        std::span<const std::byte> evidence,
        ExactDedupTokenV1* token) noexcept {
        ExactDedupPrepareResultV1 result;
        result.evidence_bytes = evidence.size();
        if (token == nullptr || token->active()) {
            return result;
        }
        token->Reset();

        if (!record_highs_.empty() &&
            sequence == record_highs_.back().sequence) {
            result.outcome = CompareEvidence(
                record_highs_.back().evidence, evidence);
            return result;
        }
        const bool record_high =
            record_highs_.empty() ||
            sequence > record_highs_.back().sequence;
        if (!record_high) {
            const auto found = std::lower_bound(
                record_highs_.begin(),
                record_highs_.end(),
                sequence,
                [](const ExactDedupSlotV1& slot,
                   std::uint64_t wanted) noexcept {
                    return slot.sequence < wanted;
                });
            if (found != record_highs_.end() &&
                found->sequence == sequence) {
                result.outcome = CompareEvidence(
                    found->evidence, evidence);
                return result;
            }
            const auto late = late_entries_.find(sequence);
            if (late != late_entries_.end()) {
                result.outcome = CompareEvidence(
                    late->second, evidence);
                return result;
            }
        }

        try {
            std::vector<std::byte> retained_evidence;
            retained_evidence.assign(
                evidence.begin(), evidence.end());
            if (record_high) {
                record_highs_.push_back(ExactDedupSlotV1{
                    sequence,
                    std::move(retained_evidence),
                });
            } else {
                const auto inserted = late_entries_.try_emplace(
                    sequence, std::move(retained_evidence));
                if (!inserted.second) {
                    result.outcome =
                        ExactDedupOutcomeV1::kUnexpectedFailure;
                    return result;
                }
            }
            token->owner = this;
            token->storage =
                record_high
                ? ExactDedupStorageV1::kRecordHigh
                : ExactDedupStorageV1::kLate;
            token->sequence = sequence;
            result.outcome = ExactDedupOutcomeV1::kFirstSeen;
            result.token_prepared = true;
            return result;
        } catch (const std::bad_alloc&) {
            result.outcome =
                ExactDedupOutcomeV1::kStorageFailure;
            return result;
        } catch (const std::length_error&) {
            result.outcome =
                ExactDedupOutcomeV1::kStorageFailure;
            return result;
        } catch (...) {
            result.outcome =
                ExactDedupOutcomeV1::kUnexpectedFailure;
            return result;
        }
    }

    [[nodiscard]] bool Commit(
        ExactDedupTokenV1* token) noexcept {
        if (token == nullptr || !token->active() ||
            token->owner != this) {
            return false;
        }
        const bool present =
            token->storage == ExactDedupStorageV1::kRecordHigh
            ? !record_highs_.empty() &&
                  record_highs_.back().sequence == token->sequence
            : token->storage == ExactDedupStorageV1::kLate &&
                  late_entries_.find(token->sequence) !=
                      late_entries_.end();
        if (!present) {
            Abort(token);
            return false;
        }
        token->Reset();
        return true;
    }

    void Abort(ExactDedupTokenV1* token) noexcept {
        if (token != nullptr && token->owner == this) {
            if (token->storage ==
                    ExactDedupStorageV1::kRecordHigh &&
                !record_highs_.empty() &&
                record_highs_.back().sequence == token->sequence) {
                record_highs_.pop_back();
            } else if (
                token->storage == ExactDedupStorageV1::kLate) {
                const auto found =
                    late_entries_.find(token->sequence);
                if (found != late_entries_.end()) {
                    late_entries_.erase(found);
                }
            }
            token->Reset();
        }
    }

private:
    [[nodiscard]] static ExactDedupOutcomeV1 CompareEvidence(
        const std::vector<std::byte>& retained,
        std::span<const std::byte> candidate) noexcept {
        return retained.size() == candidate.size() &&
                       std::equal(
                           retained.begin(),
                           retained.end(),
                           candidate.begin(),
                           candidate.end())
               ? ExactDedupOutcomeV1::kExactDuplicate
               : ExactDedupOutcomeV1::kConflict;
    }

    // Arrival-time record highs are strictly increasing and append without
    // relocating prior slots. First-seen keys below the current maximum use a
    // tree side index, avoiding an O(N) vector middle insertion.
    std::deque<ExactDedupSlotV1> record_highs_;
    std::map<std::uint64_t, std::vector<std::byte>> late_entries_;
};

ExactDedupTokenV1::~ExactDedupTokenV1() {
    if (owner != nullptr) {
        owner->Abort(this);
    }
}

[[nodiscard]] bool Contains(
    const std::vector<l2flow::sdk::MessageKey>& keys,
    const l2flow::sdk::MessageKey& wanted) noexcept {
    return std::find(keys.begin(), keys.end(), wanted) != keys.end();
}

[[nodiscard]] bool ValidConfig(
    const RealtimeFastPlaneConfigV1& config,
    const l2flow::market::InstrumentRegistryV1* registry) noexcept {
    if (registry == nullptr || registry->empty() ||
        config.capture_date == 0U ||
        config.trade_date == 0U ||
        config.first_ingress_sequence == 0U ||
        config.max_message_bytes < l2flow::sdk::kVendorHeadBytes ||
        config.input_ring_capacity_bytes_per_source == 0U ||
        config.maximum_phase_products == 0U ||
        config.maximum_phase_products >
            static_cast<std::uintmax_t>(
                std::numeric_limits<std::size_t>::max())) {
        return false;
    }
    std::array<bool, kRealtimeFastPlaneSourceCountV1> seen{};
    try {
        for (std::size_t slot = 0U; slot < config.source_kinds.size();
             ++slot) {
            const auto kind_index =
                static_cast<std::size_t>(config.source_kinds[slot]);
            if (kind_index >= seen.size() || seen[kind_index]) {
                return false;
            }
            seen[kind_index] = true;
            const auto& spec =
                l2flow::sdk::GetIngressSpec(config.source_kinds[slot]);
            if (l2flow::common::IsZeroIdentity(
                    config.stream_day_ids[slot]) ||
                config.history.source_stream_ids[slot] !=
                    spec.source_stream_id) {
                return false;
            }
        }
    } catch (...) {
        return false;
    }
    return true;
}

}  // namespace

class RealtimeFastPlaneRuntimeV1::Impl final {
public:
    struct VendorScopeKey final {
        std::uint8_t service_id = 0U;
        std::uint16_t message_id = 0U;
        auto operator<=>(const VendorScopeKey&) const = default;
    };

    struct ExchangeScopeKey final {
        l2flow::canonical::SequenceScopeKindV1 kind =
            l2flow::canonical::SequenceScopeKindV1::kShanghaiChannel;
        std::uint32_t channel = 0U;
        auto operator<=>(const ExchangeScopeKey&) const = default;
    };

    struct PhaseChannelSlot final {
        std::uint32_t channel = 0U;
        l2flow::market::TradingPhaseV1 phase =
            l2flow::market::TradingPhaseV1::kUnknown;
        std::uint64_t status_sequence = 0U;
        bool has_status_watermark = false;
    };

    struct PhaseSlot final {
        std::uint32_t instrument_id = 0U;
        std::vector<PhaseChannelSlot> channels;
        std::size_t known_phase_channels = 0U;
    };

    struct Source final {
        Source(
            std::uint8_t source_slot,
            l2flow::sdk::IngressKind ingress_kind,
            std::size_t ring_capacity,
            std::uint32_t max_message_bytes,
            std::uint64_t first_ingress_sequence,
            std::vector<PhaseSlot> phase_slots,
            l2flow::market::MarketDecoderConfigV1 decoder_config)
            : slot(source_slot),
              kind(ingress_kind),
              spec(&l2flow::sdk::GetIngressSpec(ingress_kind)),
              ring(ring_capacity, max_message_bytes),
              scratch(ring.max_body_bytes()),
              decoder(std::move(decoder_config)),
              next_capture_sequence(first_ingress_sequence),
              phases(std::move(phase_slots)) {
            vendor_evidence.reserve(max_message_bytes);
            exchange_evidence.reserve(256U);
        }

        std::uint8_t slot = 0U;
        l2flow::sdk::IngressKind kind =
            l2flow::sdk::IngressKind::ShSnapshot;
        const l2flow::sdk::IngressSpec* spec = nullptr;
        l2flow::ingress::ByteRing ring;
        l2flow::ingress::ByteRingRecord scratch;
        l2flow::market::MarketDecoderV1 decoder;
        std::thread worker;
        std::uint64_t next_capture_sequence = 1U;
        std::map<VendorScopeKey, ExactSequenceIndexV1>
            vendor_dedup;
        std::map<ExchangeScopeKey, ExactSequenceIndexV1>
            exchange_dedup;
        std::vector<std::byte> vendor_evidence;
        std::vector<std::byte> exchange_evidence;
        std::vector<PhaseSlot> phases;

        std::atomic<std::uint64_t> captured_records{0U};
        std::atomic<std::uint64_t> decoded_records{0U};
        std::atomic<std::uint64_t> ignored_records{0U};
        std::atomic<std::uint64_t> vendor_duplicate_records{0U};
        std::atomic<std::uint64_t> exchange_duplicate_records{0U};
        std::atomic<std::uint64_t>
            vendor_conflict_dropped_records{0U};
        std::atomic<std::uint64_t>
            exchange_conflict_dropped_records{0U};
        std::atomic<std::uint64_t> phase_status_commits{0U};
        std::atomic<std::uint64_t> phase_stale_status_records{0U};
        std::atomic<std::uint64_t> phase_attributed_records{0U};
        std::atomic<std::uint64_t> phase_unknown_records{0U};
        std::atomic<std::uint64_t> phase_product_count{0U};
        std::atomic<std::uint64_t> vendor_dedup_entries{0U};
        std::atomic<std::uint64_t> exchange_dedup_entries{0U};
        std::atomic<std::uint64_t>
            vendor_dedup_evidence_bytes{0U};
        std::atomic<std::uint64_t>
            exchange_dedup_evidence_bytes{0U};
        std::atomic<std::uint64_t> history_submissions{0U};
        std::atomic<std::uint64_t> history_backpressure_retries{0U};
        std::atomic<std::uint64_t> last_captured_sequence{0U};
        std::atomic<std::uint64_t> last_processed_sequence{0U};
        std::atomic<std::uint64_t> last_submitted_sequence{0U};
        std::atomic_flag failure_latched = ATOMIC_FLAG_INIT;
        std::atomic<RealtimeFastPlaneFailureV1> failure{
            RealtimeFastPlaneFailureV1::kNone};
        std::atomic<std::uint64_t> failure_sequence{0U};
        std::atomic<std::uint64_t> failure_vendor_sequence{0U};
        std::atomic<std::uint64_t> failure_business_sequence{0U};
        std::atomic<std::uint32_t> failure_channel{0U};
        std::atomic<bool> worker_exited{false};
    };

    Impl(
        RealtimeFastPlaneConfigV1 config_value,
        const l2flow::market::InstrumentRegistryV1* registry_value,
        std::unique_ptr<l2flow::market::InstrumentHistoryRuntimeV1>
            history_value)
        : config(std::move(config_value)),
          registry(registry_value),
          history(std::move(history_value)) {}

    ~Impl() {
        StopAndDrain();
    }

    [[nodiscard]] bool BuildSources() {
        for (std::size_t slot = 0U; slot < sources.size(); ++slot) {
            const auto& spec =
                l2flow::sdk::GetIngressSpec(config.source_kinds[slot]);
            std::vector<PhaseSlot> phase_slots;
            if (config.source_kinds[slot] ==
                l2flow::sdk::IngressKind::ShTick) {
                phase_slots.reserve(std::min(
                    registry->size(),
                    static_cast<std::size_t>(
                        config.maximum_phase_products)));
                for (const auto& entry : registry->entries()) {
                    if (entry.key.market ==
                        l2flow::market::MarketV1::kShanghai) {
                        if (phase_slots.size() >=
                            config.maximum_phase_products) {
                            return false;
                        }
                        phase_slots.push_back(PhaseSlot{
                            entry.instrument_id,
                            {},
                            0U,
                        });
                    }
                }
                std::sort(
                    phase_slots.begin(),
                    phase_slots.end(),
                    [](const PhaseSlot& left,
                       const PhaseSlot& right) noexcept {
                        return left.instrument_id < right.instrument_id;
                    });
            }
            l2flow::market::MarketDecoderConfigV1 decoder_config{};
            decoder_config.trade_date = config.trade_date;
            decoder_config.source_stream_id = spec.source_stream_id;
            decoder_config.instrument_registry = registry;
            decoder_config.shanghai_phase_attribution =
                l2flow::market::ShanghaiPhaseAttributionModeV1::kDeferred;
            decoder_config.limits.maximum_body_bytes =
                static_cast<std::size_t>(
                    config.max_message_bytes -
                    l2flow::sdk::kVendorHeadBytes);
            decoder_config.limits.maximum_phase_products =
                static_cast<std::size_t>(
                    config.maximum_phase_products);
            sources[slot] = std::make_unique<Source>(
                static_cast<std::uint8_t>(slot),
                config.source_kinds[slot],
                config.input_ring_capacity_bytes_per_source,
                config.max_message_bytes,
                config.first_ingress_sequence,
                std::move(phase_slots),
                decoder_config);
            if (!sources[slot]->decoder.configuration_valid()) {
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] bool StartWorkers() noexcept {
        try {
            for (auto& source : sources) {
                Source* const borrowed = source.get();
                source->worker = std::thread([this, borrowed]() noexcept {
                    Run(borrowed);
                });
            }
            return true;
        } catch (...) {
            accepting.store(false, std::memory_order_release);
            for (auto& source : sources) {
                if (source != nullptr && source->worker.joinable()) {
                    source->worker.join();
                }
            }
            return false;
        }
    }

    [[nodiscard]] FastCapturePublishResultV1 Publish(
        const l2flow::ingress::CaptureMetaV1& metadata,
        std::span<const std::byte, l2flow::sdk::kVendorHeadBytes> head,
        std::span<const std::byte> body) noexcept {
        if (fatal.load(std::memory_order_acquire)) {
            return FastCapturePublishResultV1::kFatal;
        }
        if (!accepting.load(std::memory_order_acquire)) {
            return FastCapturePublishResultV1::kStopped;
        }
        if (metadata.capture_date != config.capture_date ||
            metadata.recv_realtime_ns >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::int64_t>::max()) ||
            metadata.recv_monotonic_ns >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::int64_t>::max())) {
            LatchFailure(
                nullptr,
                metadata.ingress_sequence,
                RealtimeFastPlaneFailureV1::kCaptureInvalid);
            return FastCapturePublishResultV1::kFatal;
        }
        Source* const source = FindSource(metadata.source_stream_id);
        if (source == nullptr) {
            LatchFailure(
                nullptr,
                metadata.ingress_sequence,
                RealtimeFastPlaneFailureV1::kCaptureInvalid);
            return FastCapturePublishResultV1::kFatal;
        }
        if (metadata.ingress_sequence !=
                source->next_capture_sequence ||
            metadata.ingress_sequence ==
                std::numeric_limits<std::uint64_t>::max()) {
            LatchFailure(
                source,
                metadata.ingress_sequence,
                RealtimeFastPlaneFailureV1::kCaptureInvalid);
            return FastCapturePublishResultV1::kFatal;
        }
        const ByteRingPushResult result =
            source->ring.try_push_copy(metadata, head, body);
        if (result == ByteRingPushResult::PUBLISHED) {
            source->captured_records.fetch_add(
                1U, std::memory_order_relaxed);
            source->last_captured_sequence.store(
                metadata.ingress_sequence, std::memory_order_release);
            ++source->next_capture_sequence;
            return FastCapturePublishResultV1::kPublished;
        }
        if (result == ByteRingPushResult::FULL) {
            LatchFailure(
                source,
                metadata.ingress_sequence,
                RealtimeFastPlaneFailureV1::kCaptureFull);
            return FastCapturePublishResultV1::kFull;
        }
        LatchFailure(
            source,
            metadata.ingress_sequence,
            RealtimeFastPlaneFailureV1::kCaptureInvalid);
        return FastCapturePublishResultV1::kFatal;
    }

    [[nodiscard]] Source* FindSource(
        std::uint32_t source_stream_id) noexcept {
        for (auto& candidate : sources) {
            if (candidate != nullptr &&
                candidate->spec->source_stream_id == source_stream_id) {
                return candidate.get();
            }
        }
        return nullptr;
    }

    void Invalidate(
        std::uint32_t source_stream_id,
        std::uint64_t ingress_sequence) noexcept {
        LatchFailure(
            FindSource(source_stream_id),
            ingress_sequence,
            RealtimeFastPlaneFailureV1::kUpstreamFatal);
    }

    void Run(Source* source) noexcept {
        if (source == nullptr) {
            return;
        }
        try {
            for (;;) {
                if (fatal.load(std::memory_order_acquire)) {
                    RevokeHistory();
                    break;
                }
                if (history->AnySourceFatal()) {
                    LatchFailure(
                        nullptr,
                        source->last_processed_sequence.load(
                            std::memory_order_acquire),
                        RealtimeFastPlaneFailureV1::kHistoryFailed);
                    RevokeHistory();
                    break;
                }
                const ByteRingPopResult popped =
                    source->ring.try_pop(source->scratch);
                if (popped == ByteRingPopResult::RECORD) {
                    if (!ProcessRecord(source, source->scratch)) {
                        break;
                    }
                    continue;
                }
                if (popped != ByteRingPopResult::EMPTY) {
                    LatchFailure(
                        source,
                        0U,
                        RealtimeFastPlaneFailureV1::kRingCorrupt);
                    RevokeHistory();
                    break;
                }
                if (!accepting.load(std::memory_order_acquire)) {
                    break;
                }
                std::this_thread::yield();
            }
        } catch (...) {
            LatchFailure(
                source,
                0U,
                RealtimeFastPlaneFailureV1::kUnexpectedFailure);
        }
        if (fatal.load(std::memory_order_acquire)) {
            RevokeHistory();
        }
        source->worker_exited.store(true, std::memory_order_release);
    }

    struct PendingPhaseV1 final {
        std::size_t slot = std::numeric_limits<std::size_t>::max();
        std::size_t channel_slot =
            std::numeric_limits<std::size_t>::max();
        l2flow::market::TradingPhaseV1 phase =
            l2flow::market::TradingPhaseV1::kUnknown;
        bool update = false;
        bool advance_watermark = false;
        bool status = false;
        bool stale_status = false;
        bool attributed = false;
        bool unknown = false;
        std::uint32_t status_channel = 0U;
        std::uint64_t status_sequence = 0U;
    };

    [[nodiscard]] ExactSequenceIndexV1* GetVendorDedup(
        Source* source,
        const l2flow::market::MarketMessageViewV1& message) noexcept {
        const VendorScopeKey key{
            message.service_id,
            message.message_id,
        };
        try {
            return &source->vendor_dedup.try_emplace(key)
                        .first->second;
        } catch (...) {
            return nullptr;
        }
    }

    [[nodiscard]] ExactSequenceIndexV1* GetExchangeDedup(
        Source* source,
        const MarketBusinessSequenceIdentityV1& identity) noexcept {
        const ExchangeScopeKey key{
            identity.kind,
            identity.channel,
        };
        try {
            return &source->exchange_dedup.try_emplace(key)
                        .first->second;
        } catch (...) {
            return nullptr;
        }
    }

    static void SaturatingAdd(
        std::atomic<std::uint64_t>* target,
        std::size_t value) noexcept {
        if (target == nullptr) {
            return;
        }
        const std::uint64_t increment =
            static_cast<std::uintmax_t>(value) >
                    std::numeric_limits<std::uint64_t>::max()
            ? std::numeric_limits<std::uint64_t>::max()
            : static_cast<std::uint64_t>(value);
        std::uint64_t current =
            target->load(std::memory_order_relaxed);
        for (;;) {
            const std::uint64_t desired =
                current >
                        std::numeric_limits<std::uint64_t>::max() -
                            increment
                ? std::numeric_limits<std::uint64_t>::max()
                : current + increment;
            if (target->compare_exchange_weak(
                    current,
                    desired,
                    std::memory_order_relaxed,
                    std::memory_order_relaxed)) {
                return;
            }
        }
    }

    [[nodiscard]] bool CommitDedup(
        Source* source,
        bool vendor,
        ExactSequenceIndexV1* index,
        ExactDedupTokenV1* token,
        std::size_t evidence_bytes) noexcept {
        if (source == nullptr || index == nullptr || token == nullptr ||
            !index->Commit(token)) {
            return false;
        }
        (vendor
             ? source->vendor_dedup_entries
             : source->exchange_dedup_entries)
            .fetch_add(1U, std::memory_order_relaxed);
        SaturatingAdd(
            vendor
                ? &source->vendor_dedup_evidence_bytes
                : &source->exchange_dedup_evidence_bytes,
            evidence_bytes);
        return true;
    }

    static void AbortDedup(
        ExactSequenceIndexV1* vendor_index,
        ExactDedupTokenV1* vendor_token,
        ExactSequenceIndexV1* exchange_index,
        ExactDedupTokenV1* exchange_token) noexcept {
        if (exchange_index != nullptr) {
            exchange_index->Abort(exchange_token);
        }
        if (vendor_index != nullptr) {
            vendor_index->Abort(vendor_token);
        }
    }

    [[nodiscard]] PhaseSlot* FindPhaseSlot(
        Source* source,
        std::uint32_t instrument_id,
        std::size_t* index) noexcept {
        const auto found = std::lower_bound(
            source->phases.begin(),
            source->phases.end(),
            instrument_id,
            [](const PhaseSlot& slot,
               std::uint32_t wanted) noexcept {
                return slot.instrument_id < wanted;
            });
        if (found == source->phases.end() ||
            found->instrument_id != instrument_id) {
            return nullptr;
        }
        if (index != nullptr) {
            *index = static_cast<std::size_t>(
                found - source->phases.begin());
        }
        return &*found;
    }

    [[nodiscard]] static PhaseChannelSlot* FindPhaseChannel(
        PhaseSlot* phase_slot,
        std::uint32_t channel,
        std::size_t* index) noexcept {
        if (phase_slot == nullptr) {
            return nullptr;
        }
        const auto found = std::lower_bound(
            phase_slot->channels.begin(),
            phase_slot->channels.end(),
            channel,
            [](const PhaseChannelSlot& slot,
               std::uint32_t wanted) noexcept {
                return slot.channel < wanted;
            });
        if (found == phase_slot->channels.end() ||
            found->channel != channel) {
            return nullptr;
        }
        if (index != nullptr) {
            *index = static_cast<std::size_t>(
                found - phase_slot->channels.begin());
        }
        return &*found;
    }

    [[nodiscard]] static PhaseChannelSlot* EnsurePhaseChannel(
        PhaseSlot* phase_slot,
        std::uint32_t channel,
        std::size_t* index) noexcept {
        if (phase_slot == nullptr) {
            return nullptr;
        }
        if (PhaseChannelSlot* const existing =
                FindPhaseChannel(phase_slot, channel, index);
            existing != nullptr) {
            return existing;
        }
        const auto position = std::lower_bound(
            phase_slot->channels.begin(),
            phase_slot->channels.end(),
            channel,
            [](const PhaseChannelSlot& slot,
               std::uint32_t wanted) noexcept {
                return slot.channel < wanted;
            });
        const std::size_t insertion_index =
            static_cast<std::size_t>(
                position - phase_slot->channels.begin());
        try {
            phase_slot->channels.insert(
                position, PhaseChannelSlot{channel});
        } catch (...) {
            return nullptr;
        }
        if (index != nullptr) {
            *index = insertion_index;
        }
        return &phase_slot->channels[insertion_index];
    }

    [[nodiscard]] bool PrepareShanghaiPhase(
        Source* source,
        l2flow::market::DecodedMarketEventV1* event,
        const MarketBusinessSequenceIdentityV1& business,
        PendingPhaseV1* pending) noexcept {
        if (source == nullptr || event == nullptr || pending == nullptr) {
            return false;
        }
        auto* const tick =
            std::get_if<l2flow::market::ShanghaiTickV1>(event);
        if (tick == nullptr) {
            return true;
        }
        if (!tick->common.sh_phase_attribution_deferred) {
            return false;
        }
        pending->status =
            tick->fields.action ==
            l2flow::market::TickActionV1::kStatus;
        std::size_t slot_index =
            std::numeric_limits<std::size_t>::max();
        PhaseSlot* const phase_slot = FindPhaseSlot(
            source, tick->common.instrument_id, &slot_index);
        if (pending->status) {
            pending->unknown =
                tick->fields.phase ==
                l2flow::market::TradingPhaseV1::kUnknown;
            if (phase_slot != nullptr &&
                tick->common.security_id_valid &&
                business.present &&
                business.sequence_valid) {
                std::size_t channel_slot_index =
                    std::numeric_limits<std::size_t>::max();
                PhaseChannelSlot* const channel_slot =
                    EnsurePhaseChannel(
                        phase_slot,
                        business.channel,
                        &channel_slot_index);
                if (channel_slot == nullptr) {
                    return false;
                }
                pending->slot = slot_index;
                pending->channel_slot = channel_slot_index;
                pending->status_channel = business.channel;
                pending->status_sequence = business.sequence;
                const bool newer =
                    !channel_slot->has_status_watermark ||
                    business.sequence >
                        channel_slot->status_sequence;
                if (newer) {
                    pending->advance_watermark = true;
                    if (!pending->unknown &&
                        tick->raw_tick_flag_valid) {
                        pending->phase = tick->fields.phase;
                        pending->update = true;
                    }
                } else {
                    pending->stale_status = true;
                }
            }
        } else if (PhaseChannelSlot* const channel_slot =
                       business.present && business.sequence_valid
                       ? FindPhaseChannel(
                             phase_slot, business.channel, nullptr)
                       : nullptr;
                   channel_slot != nullptr &&
                   channel_slot->phase !=
                       l2flow::market::TradingPhaseV1::kUnknown) {
            tick->fields.phase = channel_slot->phase;
            tick->fields.validity_bitmap |=
                l2flow::market::kTickPhaseValidV1;
            pending->attributed = true;
        } else {
            tick->fields.phase =
                l2flow::market::TradingPhaseV1::kUnknown;
            tick->fields.validity_bitmap &=
                ~l2flow::market::kTickPhaseValidV1;
            pending->unknown = true;
        }
        tick->common.sh_phase_attribution_deferred = false;
        return true;
    }

    void CommitShanghaiPhase(
        Source* source,
        const PendingPhaseV1& pending) noexcept {
        if ((pending.advance_watermark || pending.update) &&
            pending.slot < source->phases.size()) {
            PhaseSlot& phase_slot = source->phases[pending.slot];
            if (pending.channel_slot < phase_slot.channels.size()) {
                PhaseChannelSlot& channel_slot =
                    phase_slot.channels[pending.channel_slot];
                if (channel_slot.channel == pending.status_channel) {
                    if (pending.advance_watermark) {
                        channel_slot.status_sequence =
                            pending.status_sequence;
                        channel_slot.has_status_watermark = true;
                    }
                    if (pending.update) {
                        if (channel_slot.phase ==
                            l2flow::market::TradingPhaseV1::kUnknown) {
                            if (phase_slot.known_phase_channels == 0U) {
                                source->phase_product_count.fetch_add(
                                    1U, std::memory_order_relaxed);
                            }
                            ++phase_slot.known_phase_channels;
                        }
                        channel_slot.phase = pending.phase;
                        source->phase_status_commits.fetch_add(
                            1U, std::memory_order_relaxed);
                    }
                }
            }
        }
        if (pending.stale_status) {
            source->phase_stale_status_records.fetch_add(
                1U, std::memory_order_relaxed);
        }
        if (pending.attributed) {
            source->phase_attributed_records.fetch_add(
                1U, std::memory_order_relaxed);
        }
        if (pending.unknown) {
            source->phase_unknown_records.fetch_add(
                1U, std::memory_order_relaxed);
        }
    }

    [[nodiscard]] bool ProcessRecord(
        Source* source,
        const l2flow::ingress::ByteRingRecord& record) noexcept {
        const l2flow::sdk::VendorHeadView head(record.head);
        const l2flow::sdk::MessageKey key{
            head.service_id(),
            head.service_version(),
            head.message_id(),
        };
        if (!Contains(source->spec->required, key)) {
            source->ignored_records.fetch_add(
                1U, std::memory_order_relaxed);
            source->last_processed_sequence.store(
                record.meta.ingress_sequence, std::memory_order_release);
            return true;
        }

        l2flow::market::MarketMessageViewV1 message{};
        message.source_stream_id = record.meta.source_stream_id;
        message.trade_date = config.trade_date;
        message.source_sequence = record.meta.ingress_sequence;
        message.service_id = head.service_id();
        message.service_version = head.service_version();
        message.message_id = head.message_id();
        message.message_encoding = head.message_encoding();
        message.vendor_local_time_raw = head.local_time_raw();
        message.vendor_sequence_id = head.sequence_id();
        message.recv_realtime_ns =
            static_cast<std::int64_t>(record.meta.recv_realtime_ns);
        message.recv_monotonic_ns =
            static_cast<std::int64_t>(record.meta.recv_monotonic_ns);
        message.body = record.body;

        if (message.vendor_sequence_id == 0U ||
            l2flow::canonical::BuildVendorSequenceEvidenceV1(
                message, &source->vendor_evidence) !=
                l2flow::canonical::MarketSequenceEvidenceErrorV1::kNone) {
            LatchFailure(
                source,
                record.meta.ingress_sequence,
                RealtimeFastPlaneFailureV1::kDedupFailed,
                message.vendor_sequence_id);
            return false;
        }
        ExactSequenceIndexV1* const vendor_index =
            GetVendorDedup(source, message);
        ExactDedupTokenV1 vendor_token;
        const auto vendor_prepared =
            vendor_index == nullptr
            ? ExactDedupPrepareResultV1{}
            : vendor_index->Prepare(
            message.vendor_sequence_id,
            source->vendor_evidence,
            &vendor_token);
        if (vendor_prepared.outcome ==
                ExactDedupOutcomeV1::kExactDuplicate ||
            vendor_prepared.outcome ==
                ExactDedupOutcomeV1::kConflict) {
            (vendor_prepared.outcome ==
                     ExactDedupOutcomeV1::kExactDuplicate
                 ? source->vendor_duplicate_records
                 : source->vendor_conflict_dropped_records)
                .fetch_add(1U, std::memory_order_relaxed);
            source->last_processed_sequence.store(
                record.meta.ingress_sequence,
                std::memory_order_release);
            return true;
        }
        if (vendor_prepared.outcome !=
                ExactDedupOutcomeV1::kFirstSeen ||
            !vendor_prepared.token_prepared) {
            LatchFailure(
                source,
                record.meta.ingress_sequence,
                RealtimeFastPlaneFailureV1::kDedupFailed,
                message.vendor_sequence_id);
            return false;
        }

        l2flow::market::DecodedMarketEventV1 decoded{};
        if (source->decoder.Decode(message, &decoded) !=
            l2flow::market::MarketDecodeErrorV1::kNone) {
            AbortDedup(
                vendor_index, &vendor_token, nullptr, nullptr);
            LatchFailure(
                source,
                record.meta.ingress_sequence,
                RealtimeFastPlaneFailureV1::kDecodeFailed);
            return false;
        }
        source->decoded_records.fetch_add(1U, std::memory_order_relaxed);

        const MarketBusinessSequenceIdentityV1 business =
            l2flow::canonical::MarketBusinessSequenceIdentityOfV1(
                decoded);
        ExactSequenceIndexV1* exchange_index = nullptr;
        ExactDedupTokenV1 exchange_token;
        std::size_t exchange_evidence_bytes = 0U;
        if (business.present) {
            if (!business.sequence_valid) {
                AbortDedup(
                    vendor_index, &vendor_token, nullptr, nullptr);
                LatchFailure(
                    source,
                    record.meta.ingress_sequence,
                    RealtimeFastPlaneFailureV1::kDedupFailed,
                    message.vendor_sequence_id,
                    business.sequence,
                    business.channel);
                return false;
            }
            if (l2flow::canonical::BuildExchangeSequenceEvidenceV1(
                    decoded, &source->exchange_evidence) !=
                l2flow::canonical::
                        MarketSequenceEvidenceErrorV1::kNone) {
                AbortDedup(
                    vendor_index, &vendor_token, nullptr, nullptr);
                LatchFailure(
                    source,
                    record.meta.ingress_sequence,
                    RealtimeFastPlaneFailureV1::kDedupFailed,
                    message.vendor_sequence_id,
                    business.sequence,
                    business.channel);
                return false;
            }
            exchange_index = GetExchangeDedup(source, business);
            const auto exchange_prepared =
                exchange_index == nullptr
                ? ExactDedupPrepareResultV1{}
                : exchange_index->Prepare(
                business.sequence,
                source->exchange_evidence,
                &exchange_token);
            if (exchange_prepared.outcome ==
                    ExactDedupOutcomeV1::kExactDuplicate ||
                exchange_prepared.outcome ==
                    ExactDedupOutcomeV1::kConflict) {
                if (!CommitDedup(
                        source,
                        true,
                        vendor_index,
                        &vendor_token,
                        vendor_prepared.evidence_bytes)) {
                    LatchFailure(
                        source,
                        record.meta.ingress_sequence,
                        RealtimeFastPlaneFailureV1::kDedupFailed,
                        message.vendor_sequence_id,
                        business.sequence,
                        business.channel);
                    return false;
                }
                (exchange_prepared.outcome ==
                         ExactDedupOutcomeV1::kExactDuplicate
                     ? source->exchange_duplicate_records
                     : source->exchange_conflict_dropped_records)
                    .fetch_add(1U, std::memory_order_relaxed);
                source->last_processed_sequence.store(
                    record.meta.ingress_sequence,
                    std::memory_order_release);
                return true;
            }
            if (exchange_prepared.outcome !=
                    ExactDedupOutcomeV1::kFirstSeen ||
                !exchange_prepared.token_prepared) {
                AbortDedup(
                    vendor_index, &vendor_token,
                    exchange_index, &exchange_token);
                LatchFailure(
                    source,
                    record.meta.ingress_sequence,
                    RealtimeFastPlaneFailureV1::kDedupFailed,
                    message.vendor_sequence_id,
                    business.sequence,
                    business.channel);
                return false;
            }
            exchange_evidence_bytes =
                exchange_prepared.evidence_bytes;
        }

        PendingPhaseV1 pending_phase{};
        if (!PrepareShanghaiPhase(
                source, &decoded, business, &pending_phase)) {
            AbortDedup(
                vendor_index, &vendor_token,
                exchange_index, &exchange_token);
            LatchFailure(
                source,
                record.meta.ingress_sequence,
                RealtimeFastPlaneFailureV1::kPhaseFailed,
                message.vendor_sequence_id,
                business.sequence,
                business.channel);
            return false;
        }

        l2flow::market::RetainedMarketEventV1 retained{};
        if (l2flow::market::RetainMarketEventV1(
                std::move(decoded), &retained) !=
            l2flow::market::RetainedMarketEventCreateErrorV1::kNone) {
            AbortDedup(
                vendor_index, &vendor_token,
                exchange_index, &exchange_token);
            LatchFailure(
                source,
                record.meta.ingress_sequence,
                RealtimeFastPlaneFailureV1::kRetainFailed);
            return false;
        }
        std::optional<l2flow::market::OwnedInstrumentEventEnvelopeV1>
            envelope;
        if (l2flow::market::OwnedInstrumentEventEnvelopeV1::Create(
                source->slot, std::move(retained), &envelope) !=
                l2flow::market::OwnedInstrumentEventCreateErrorV1::kNone ||
            !envelope.has_value()) {
            AbortDedup(
                vendor_index, &vendor_token,
                exchange_index, &exchange_token);
            LatchFailure(
                source,
                record.meta.ingress_sequence,
                RealtimeFastPlaneFailureV1::kEnvelopeFailed);
            return false;
        }

        // Admission precedes logical state commit so a rejected history
        // enqueue cannot consume first-seen or phase state. Prepare has staged
        // the exact evidence inside the private index; this source's single
        // worker makes Commit allocation-free and Abort able to erase it.
        bool submitted_to_history = false;
        for (;;) {
            if (fatal.load(std::memory_order_acquire)) {
                AbortDedup(
                    vendor_index, &vendor_token,
                    exchange_index, &exchange_token);
                return false;
            }
            const InstrumentHistorySubmitErrorV1 submitted =
                history->TrySubmit(std::move(*envelope));
            if (submitted == InstrumentHistorySubmitErrorV1::kNone) {
                submitted_to_history = true;
                break;
            }
            if (submitted == InstrumentHistorySubmitErrorV1::kQueueFull ||
                submitted ==
                    InstrumentHistorySubmitErrorV1::kInflightLimit) {
                source->history_backpressure_retries.fetch_add(
                    1U, std::memory_order_relaxed);
                std::this_thread::yield();
                continue;
            }
            AbortDedup(
                vendor_index, &vendor_token,
                exchange_index, &exchange_token);
            LatchFailure(
                source,
                record.meta.ingress_sequence,
                RealtimeFastPlaneFailureV1::kHistoryFailed);
            return false;
        }
        if (!submitted_to_history ||
            !CommitDedup(
                source,
                true,
                vendor_index,
                &vendor_token,
                vendor_prepared.evidence_bytes) ||
            (exchange_index != nullptr &&
             !CommitDedup(
                 source,
                 false,
                 exchange_index,
                 &exchange_token,
                 exchange_evidence_bytes))) {
            LatchFailure(
                source,
                record.meta.ingress_sequence,
                RealtimeFastPlaneFailureV1::kDedupFailed,
                message.vendor_sequence_id,
                business.sequence,
                business.channel);
            return false;
        }
        CommitShanghaiPhase(source, pending_phase);
        source->history_submissions.fetch_add(
            1U, std::memory_order_relaxed);
        source->last_submitted_sequence.store(
            record.meta.ingress_sequence, std::memory_order_release);
        source->last_processed_sequence.store(
            record.meta.ingress_sequence, std::memory_order_release);
        if (history->AnySourceFatal()) {
            LatchFailure(
                nullptr,
                record.meta.ingress_sequence,
                RealtimeFastPlaneFailureV1::kHistoryFailed);
            return false;
        }
        return true;
    }

    // Callback-safe terminal latch: atomic-only and bounded. History
    // revocation is intentionally deferred to decoder/control threads.
    void LatchFailure(
        Source* source,
        std::uint64_t sequence,
        RealtimeFastPlaneFailureV1 failure_value,
        std::uint64_t vendor_sequence = 0U,
        std::uint64_t business_sequence = 0U,
        std::uint32_t channel = 0U) noexcept {
        if (source != nullptr &&
            !source->failure_latched.test_and_set(
                std::memory_order_acq_rel)) {
            source->failure_sequence.store(
                sequence, std::memory_order_relaxed);
            source->failure_vendor_sequence.store(
                vendor_sequence, std::memory_order_relaxed);
            source->failure_business_sequence.store(
                business_sequence, std::memory_order_relaxed);
            source->failure_channel.store(
                channel, std::memory_order_relaxed);
            source->failure.store(
                failure_value, std::memory_order_release);
        }
        fatal.store(true, std::memory_order_release);
        accepting.store(false, std::memory_order_release);
    }

    void RevokeHistory() noexcept {
        bool expected = false;
        if (!history_revoked.compare_exchange_strong(
                expected,
                true,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            return;
        }
        for (std::size_t slot = 0U; slot < sources.size(); ++slot) {
            history->MarkSourceFatal(static_cast<std::uint8_t>(slot));
        }
    }

    [[nodiscard]] bool ServingHealthy() const noexcept {
        if (!accepting.load(std::memory_order_acquire) ||
            stopped.load(std::memory_order_acquire) ||
            fatal.load(std::memory_order_acquire)) {
            return false;
        }
        return !history->AnySourceFatal();
    }

    [[nodiscard]] RealtimeFastPlaneSnapshotV1 Snapshot() const noexcept {
        RealtimeFastPlaneSnapshotV1 result{};
        result.accepting = accepting.load(std::memory_order_acquire);
        result.stopped = stopped.load(std::memory_order_acquire);
        result.fatal = fatal.load(std::memory_order_acquire);
        const bool history_fatal = history->AnySourceFatal();
        result.accepting = result.accepting && !history_fatal;
        result.fatal = result.fatal || history_fatal;
        for (std::size_t slot = 0U; slot < sources.size(); ++slot) {
            const Source& source = *sources[slot];
            auto& target = result.sources[slot];
            target.ingress_kind = source.kind;
            target.source_stream_id = source.spec->source_stream_id;
            target.captured_records =
                source.captured_records.load(std::memory_order_relaxed);
            target.decoded_records =
                source.decoded_records.load(std::memory_order_relaxed);
            target.ignored_records =
                source.ignored_records.load(std::memory_order_relaxed);
            target.vendor_duplicate_records =
                source.vendor_duplicate_records.load(
                    std::memory_order_relaxed);
            target.exchange_duplicate_records =
                source.exchange_duplicate_records.load(
                    std::memory_order_relaxed);
            target.vendor_conflict_dropped_records =
                source.vendor_conflict_dropped_records.load(
                    std::memory_order_relaxed);
            target.exchange_conflict_dropped_records =
                source.exchange_conflict_dropped_records.load(
                    std::memory_order_relaxed);
            target.phase_status_commits =
                source.phase_status_commits.load(
                    std::memory_order_relaxed);
            target.phase_stale_status_records =
                source.phase_stale_status_records.load(
                    std::memory_order_relaxed);
            target.phase_attributed_records =
                source.phase_attributed_records.load(
                    std::memory_order_relaxed);
            target.phase_unknown_records =
                source.phase_unknown_records.load(
                    std::memory_order_relaxed);
            target.phase_product_count =
                source.phase_product_count.load(
                    std::memory_order_relaxed);
            target.vendor_dedup_entries =
                source.vendor_dedup_entries.load(
                    std::memory_order_relaxed);
            target.exchange_dedup_entries =
                source.exchange_dedup_entries.load(
                    std::memory_order_relaxed);
            target.vendor_dedup_evidence_bytes =
                source.vendor_dedup_evidence_bytes.load(
                    std::memory_order_relaxed);
            target.exchange_dedup_evidence_bytes =
                source.exchange_dedup_evidence_bytes.load(
                    std::memory_order_relaxed);
            target.history_submissions =
                source.history_submissions.load(std::memory_order_relaxed);
            target.history_backpressure_retries =
                source.history_backpressure_retries.load(
                    std::memory_order_relaxed);
            target.last_captured_sequence =
                source.last_captured_sequence.load(
                    std::memory_order_acquire);
            target.last_processed_sequence =
                source.last_processed_sequence.load(
                    std::memory_order_acquire);
            target.last_submitted_sequence =
                source.last_submitted_sequence.load(
                    std::memory_order_acquire);
            target.ring_used_bytes =
                static_cast<std::uint64_t>(source.ring.used_bytes());
            target.ring_capacity_bytes =
                static_cast<std::uint64_t>(source.ring.capacity_bytes());
            target.history_frontier = history->Frontier(source.slot);
            target.failure =
                source.failure.load(std::memory_order_acquire);
            target.failure_sequence =
                source.failure_sequence.load(std::memory_order_acquire);
            target.failure_vendor_sequence =
                source.failure_vendor_sequence.load(
                    std::memory_order_acquire);
            target.failure_business_sequence =
                source.failure_business_sequence.load(
                    std::memory_order_acquire);
            target.failure_channel =
                source.failure_channel.load(std::memory_order_acquire);
            target.worker_exited =
                source.worker_exited.load(std::memory_order_acquire);
            target.terminal_prefix_complete =
                result.stopped &&
                target.failure == RealtimeFastPlaneFailureV1::kNone &&
                target.worker_exited &&
                target.last_processed_sequence ==
                    target.last_captured_sequence &&
                target.ignored_records +
                        target.history_submissions +
                        target.vendor_duplicate_records +
                        target.exchange_duplicate_records +
                        target.vendor_conflict_dropped_records +
                        target.exchange_conflict_dropped_records ==
                    target.captured_records &&
                target.history_submissions +
                        target.exchange_duplicate_records +
                        target.exchange_conflict_dropped_records ==
                    target.decoded_records &&
                target.history_frontier.submitted_ticket ==
                    target.history_frontier.acknowledged_ticket &&
                !target.history_frontier.fatal;
            result.fatal = result.fatal || target.history_frontier.fatal ||
                           target.failure !=
                               RealtimeFastPlaneFailureV1::kNone;
        }
        result.clean_drain = result.stopped && !result.fatal &&
            std::all_of(
                result.sources.begin(),
                result.sources.end(),
                [](const auto& source) noexcept {
                    return source.terminal_prefix_complete;
                });
        return result;
    }

    void StopAndDrain() noexcept {
        std::lock_guard<std::mutex> lock(lifecycle_mutex);
        if (stopped.load(std::memory_order_acquire)) {
            return;
        }
        accepting.store(false, std::memory_order_release);
        for (auto& source : sources) {
            if (source != nullptr && source->worker.joinable()) {
                source->worker.join();
            }
        }
        if (fatal.load(std::memory_order_acquire)) {
            RevokeHistory();
        }
        if (history != nullptr) {
            history->StopAndDrain();
        }
        stopped.store(true, std::memory_order_release);
    }

    RealtimeFastPlaneConfigV1 config{};
    const l2flow::market::InstrumentRegistryV1* registry = nullptr;
    std::unique_ptr<l2flow::market::InstrumentHistoryRuntimeV1> history;
    std::array<std::unique_ptr<Source>,
               kRealtimeFastPlaneSourceCountV1>
        sources{};
    std::atomic<bool> accepting{true};
    std::atomic<bool> stopped{false};
    std::atomic<bool> fatal{false};
    std::atomic<bool> history_revoked{false};
    mutable std::mutex lifecycle_mutex;
};

RealtimeFastPlaneRuntimeV1::RealtimeFastPlaneRuntimeV1(
    std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}

RealtimeFastPlaneRuntimeV1::~RealtimeFastPlaneRuntimeV1() = default;

RealtimeFastPlaneCreateErrorV1 RealtimeFastPlaneRuntimeV1::Create(
    RealtimeFastPlaneConfigV1 config,
    const l2flow::market::InstrumentRegistryV1* registry,
    std::unique_ptr<RealtimeFastPlaneRuntimeV1>* output) noexcept {
    if (output == nullptr) {
        return RealtimeFastPlaneCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (registry == nullptr) {
        return RealtimeFastPlaneCreateErrorV1::kNullRegistry;
    }
    if (!ValidConfig(config, registry)) {
        return RealtimeFastPlaneCreateErrorV1::kInvalidConfiguration;
    }
    std::unique_ptr<l2flow::market::InstrumentHistoryRuntimeV1> history;
    const auto history_error =
        l2flow::market::InstrumentHistoryRuntimeV1::Create(
            config.history, &history);
    if (history_error !=
            l2flow::market::InstrumentHistoryCreateErrorV1::kNone ||
        history == nullptr) {
        return RealtimeFastPlaneCreateErrorV1::kHistoryCreateFailed;
    }
    try {
        auto impl = std::make_unique<Impl>(
            std::move(config), registry, std::move(history));
        if (!impl->BuildSources()) {
            return RealtimeFastPlaneCreateErrorV1::kInvalidConfiguration;
        }
        if (!impl->StartWorkers()) {
            return RealtimeFastPlaneCreateErrorV1::kThreadStartFailed;
        }
        output->reset(new RealtimeFastPlaneRuntimeV1(std::move(impl)));
        return RealtimeFastPlaneCreateErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return RealtimeFastPlaneCreateErrorV1::kResourceExhausted;
    } catch (...) {
        return RealtimeFastPlaneCreateErrorV1::kInvalidConfiguration;
    }
}

l2flow::ingress::FastCaptureSinkRefV1
RealtimeFastPlaneRuntimeV1::capture_sink() noexcept {
    return l2flow::ingress::FastCaptureSinkRefV1{
        this,
        &RealtimeFastPlaneRuntimeV1::TryPublishCopy,
        &RealtimeFastPlaneRuntimeV1::InvalidateGeneration,
    };
}

l2flow::ingress::FastCapturePublishResultV1
RealtimeFastPlaneRuntimeV1::TryPublishCopy(
    void* context,
    const l2flow::ingress::CaptureMetaV1& metadata,
    std::span<const std::byte, l2flow::sdk::kVendorHeadBytes> head,
    std::span<const std::byte> body) noexcept {
    auto* const runtime =
        static_cast<RealtimeFastPlaneRuntimeV1*>(context);
    return runtime == nullptr || runtime->impl_ == nullptr
        ? l2flow::ingress::FastCapturePublishResultV1::kFatal
        : runtime->impl_->Publish(metadata, head, body);
}

void RealtimeFastPlaneRuntimeV1::InvalidateGeneration(
    void* context,
    std::uint32_t source_stream_id,
    std::uint64_t ingress_sequence) noexcept {
    auto* const runtime =
        static_cast<RealtimeFastPlaneRuntimeV1*>(context);
    if (runtime != nullptr && runtime->impl_ != nullptr) {
        runtime->impl_->Invalidate(source_stream_id, ingress_sequence);
    }
}

RealtimeFastPlaneSnapshotV1
RealtimeFastPlaneRuntimeV1::Snapshot() const noexcept {
    return impl_->Snapshot();
}

l2flow::market::InstrumentHistoryQueryErrorV1
RealtimeFastPlaneRuntimeV1::Latest(
    std::uint32_t instrument_id,
    std::uint8_t source_slot,
    l2flow::market::InstrumentHistoryLaneV1 lane,
    l2flow::market::InstrumentHistoryRecordHandleV1* output) const noexcept {
    if (output == nullptr) {
        return l2flow::market::InstrumentHistoryQueryErrorV1::kNullOutput;
    }
    *output = {};
    if (!impl_->ServingHealthy()) {
        return l2flow::market::InstrumentHistoryQueryErrorV1::kSourceFatal;
    }
    const auto error = impl_->history->LatestProvisional(
        instrument_id, source_slot, lane, output);
    if (!impl_->ServingHealthy()) {
        *output = {};
        return l2flow::market::InstrumentHistoryQueryErrorV1::kSourceFatal;
    }
    return error;
}

l2flow::market::InstrumentHistoryQueryErrorV1
RealtimeFastPlaneRuntimeV1::Tail(
    std::uint32_t instrument_id,
    std::uint8_t source_slot,
    l2flow::market::InstrumentHistoryLaneV1 lane,
    std::size_t count,
    std::vector<l2flow::market::InstrumentHistoryRecordHandleV1>* output)
    const noexcept {
    if (output == nullptr) {
        return l2flow::market::InstrumentHistoryQueryErrorV1::kNullOutput;
    }
    output->clear();
    if (!impl_->ServingHealthy()) {
        return l2flow::market::InstrumentHistoryQueryErrorV1::kSourceFatal;
    }
    const auto error = impl_->history->TailProvisional(
        instrument_id, source_slot, lane, count, output);
    if (!impl_->ServingHealthy()) {
        output->clear();
        return l2flow::market::InstrumentHistoryQueryErrorV1::kSourceFatal;
    }
    return error;
}

const RealtimeFastPlaneConfigV1&
RealtimeFastPlaneRuntimeV1::config() const noexcept {
    return impl_->config;
}

const l2flow::market::InstrumentRegistryV1*
RealtimeFastPlaneRuntimeV1::registry() const noexcept {
    return impl_->registry;
}

void RealtimeFastPlaneRuntimeV1::StopAndDrain() noexcept {
    impl_->StopAndDrain();
}

std::string_view RealtimeFastPlaneCreateErrorNameV1(
    RealtimeFastPlaneCreateErrorV1 error) noexcept {
    switch (error) {
        case RealtimeFastPlaneCreateErrorV1::kNone:
            return "none";
        case RealtimeFastPlaneCreateErrorV1::kNullOutput:
            return "null_output";
        case RealtimeFastPlaneCreateErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case RealtimeFastPlaneCreateErrorV1::kNullRegistry:
            return "null_registry";
        case RealtimeFastPlaneCreateErrorV1::kHistoryCreateFailed:
            return "history_create_failed";
        case RealtimeFastPlaneCreateErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case RealtimeFastPlaneCreateErrorV1::kThreadStartFailed:
            return "thread_start_failed";
    }
    return "unknown";
}

std::string_view RealtimeFastPlaneFailureNameV1(
    RealtimeFastPlaneFailureV1 failure) noexcept {
    switch (failure) {
        case RealtimeFastPlaneFailureV1::kNone:
            return "none";
        case RealtimeFastPlaneFailureV1::kCaptureFull:
            return "capture_full";
        case RealtimeFastPlaneFailureV1::kCaptureInvalid:
            return "capture_invalid";
        case RealtimeFastPlaneFailureV1::kRingCorrupt:
            return "ring_corrupt";
        case RealtimeFastPlaneFailureV1::kDecodeFailed:
            return "decode_failed";
        case RealtimeFastPlaneFailureV1::kDedupFailed:
            return "dedup_failed";
        case RealtimeFastPlaneFailureV1::kPhaseFailed:
            return "phase_failed";
        case RealtimeFastPlaneFailureV1::kRetainFailed:
            return "retain_failed";
        case RealtimeFastPlaneFailureV1::kEnvelopeFailed:
            return "envelope_failed";
        case RealtimeFastPlaneFailureV1::kHistoryFailed:
            return "history_failed";
        case RealtimeFastPlaneFailureV1::kUpstreamFatal:
            return "upstream_fatal";
        case RealtimeFastPlaneFailureV1::kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

}  // namespace l2flow::runtime
