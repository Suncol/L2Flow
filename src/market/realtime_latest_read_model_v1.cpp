#include "l2flow/market/realtime_latest_read_model_v1.h"

#include "l2flow/market/market_types_v1.h"
#include "l2flow/market/observed_instrument_directory_v2.h"
#include "l2flow/market/realtime_history_v1.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <span>
#include <utility>

namespace l2flow::market {
namespace {

[[nodiscard]] bool ValidRecordKind(
    RealtimeLatestRecordKindV1 kind) noexcept {
    switch (kind) {
        case RealtimeLatestRecordKindV1::kSnapshot:
        case RealtimeLatestRecordKindV1::kTick:
            return true;
    }
    return false;
}

[[nodiscard]] bool LatestKindForMarketEvent(
    MarketEventKindV1 event_kind,
    RealtimeLatestRecordKindV1* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    if (IsSnapshotEventKindV1(event_kind)) {
        *output = RealtimeLatestRecordKindV1::kSnapshot;
        return true;
    }
    if (IsTickEventKindV1(event_kind)) {
        *output = RealtimeLatestRecordKindV1::kTick;
        return true;
    }
    return false;
}

void ClearViews(
    std::span<RealtimeLatestRecordViewV1> output) noexcept {
    std::fill(
        output.begin(), output.end(), RealtimeLatestRecordViewV1{});
}

}  // namespace

std::string_view RealtimeLatestReadModelCreateErrorNameV1(
    RealtimeLatestReadModelCreateErrorV1 error) noexcept {
    switch (error) {
        case RealtimeLatestReadModelCreateErrorV1::kNone:
            return "none";
        case RealtimeLatestReadModelCreateErrorV1::kNullOutput:
            return "null_output";
        case RealtimeLatestReadModelCreateErrorV1::
            kInvalidConfiguration:
            return "invalid_configuration";
        case RealtimeLatestReadModelCreateErrorV1::
            kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

std::string_view RealtimeLatestPublishErrorNameV1(
    RealtimeLatestPublishErrorV1 error) noexcept {
    switch (error) {
        case RealtimeLatestPublishErrorV1::kNone:
            return "none";
        case RealtimeLatestPublishErrorV1::kCoverageLost:
            return "coverage_lost";
        case RealtimeLatestPublishErrorV1::kNullRecord:
            return "null_record";
        case RealtimeLatestPublishErrorV1::kInvalidOrdinal:
            return "invalid_ordinal";
        case RealtimeLatestPublishErrorV1::kUnboundInstrument:
            return "unbound_instrument";
        case RealtimeLatestPublishErrorV1::kInstrumentMismatch:
            return "instrument_mismatch";
        case RealtimeLatestPublishErrorV1::kInvalidRecordKind:
            return "invalid_record_kind";
        case RealtimeLatestPublishErrorV1::kIngressConflict:
            return "ingress_conflict";
    }
    return "unknown";
}

std::string_view RealtimeLatestQueryErrorNameV1(
    RealtimeLatestQueryErrorV1 error) noexcept {
    switch (error) {
        case RealtimeLatestQueryErrorV1::kNone:
            return "none";
        case RealtimeLatestQueryErrorV1::kNullOutput:
            return "null_output";
        case RealtimeLatestQueryErrorV1::kInvalidArgument:
            return "invalid_argument";
        case RealtimeLatestQueryErrorV1::kUnavailable:
            return "unavailable";
        case RealtimeLatestQueryErrorV1::kCoverageLost:
            return "coverage_lost";
    }
    return "unknown";
}

class RealtimeLatestReadModelV1::Impl final {
public:
    // Snapshot readers and high-frequency tick publishers use separate
    // 64-byte-aligned slots. On the target 64-byte cache-line deployments,
    // this also isolates one hot instrument from adjacent instruments.
    struct alignas(64) PublishedSlot final {
        std::atomic<const RealtimeHistoryRecordV1*> record{nullptr};
    };

    Impl(
        const ObservedInstrumentDirectoryV2* directory,
        std::size_t slot_count)
        : directory_(directory),
          slot_count_(slot_count),
          snapshot_slots_(
              std::make_unique<PublishedSlot[]>(slot_count)),
          tick_slots_(
              std::make_unique<PublishedSlot[]>(slot_count)) {}

    [[nodiscard]] std::atomic<const RealtimeHistoryRecordV1*>*
    Target(
        std::size_t ordinal,
        RealtimeLatestRecordKindV1 kind) noexcept {
        if (ordinal >= slot_count_) {
            return nullptr;
        }
        switch (kind) {
            case RealtimeLatestRecordKindV1::kSnapshot:
                return &snapshot_slots_[ordinal].record;
            case RealtimeLatestRecordKindV1::kTick:
                return &tick_slots_[ordinal].record;
        }
        return nullptr;
    }

    [[nodiscard]] const std::atomic<
        const RealtimeHistoryRecordV1*>*
    Target(
        std::size_t ordinal,
        RealtimeLatestRecordKindV1 kind) const noexcept {
        if (ordinal >= slot_count_) {
            return nullptr;
        }
        switch (kind) {
            case RealtimeLatestRecordKindV1::kSnapshot:
                return &snapshot_slots_[ordinal].record;
            case RealtimeLatestRecordKindV1::kTick:
                return &tick_slots_[ordinal].record;
        }
        return nullptr;
    }

    void QueryOne(
        RealtimeLatestRecordKindV1 kind,
        std::uint32_t instrument_id,
        RealtimeLatestRecordViewV1* output) const noexcept {
        *output = RealtimeLatestRecordViewV1{};
        output->instrument_id = instrument_id;
        if (instrument_id == 0U ||
            static_cast<std::size_t>(instrument_id) > slot_count_) {
            output->status =
                RealtimeLatestRecordStatusV1::kInvalidInstrumentId;
            return;
        }

        std::size_t ordinal = 0U;
        if (directory_->ResolveBoundId(instrument_id, &ordinal) !=
            ObservedInstrumentDirectoryErrorV2::kNone) {
            output->status = RealtimeLatestRecordStatusV1::kUnbound;
            return;
        }
        const std::atomic<const RealtimeHistoryRecordV1*>* const
            target = Target(ordinal, kind);
        const RealtimeHistoryRecordV1* const record =
            target == nullptr
                ? nullptr
                : target->load(std::memory_order_acquire);
        if (record == nullptr) {
            output->status =
                RealtimeLatestRecordStatusV1::kBoundNoTypeData;
            return;
        }
        output->status = RealtimeLatestRecordStatusV1::kAvailable;
        output->record = record;
    }

    const ObservedInstrumentDirectoryV2* directory_ = nullptr;
    std::size_t slot_count_ = 0U;
    std::unique_ptr<PublishedSlot[]> snapshot_slots_;
    std::unique_ptr<PublishedSlot[]> tick_slots_;
    std::atomic<bool> coverage_lost_{false};
};

RealtimeLatestReadModelV1::RealtimeLatestReadModelV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

RealtimeLatestReadModelV1::~RealtimeLatestReadModelV1() = default;

RealtimeLatestReadModelCreateErrorV1
RealtimeLatestReadModelV1::Create(
    const ObservedInstrumentDirectoryV2* directory,
    std::unique_ptr<RealtimeLatestReadModelV1>* output) noexcept {
    if (output == nullptr) {
        return RealtimeLatestReadModelCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (directory == nullptr || directory->capacity() == 0U) {
        return RealtimeLatestReadModelCreateErrorV1::
            kInvalidConfiguration;
    }

    try {
        auto impl =
            std::make_unique<RealtimeLatestReadModelV1::Impl>(
                directory, directory->capacity());
        output->reset(new RealtimeLatestReadModelV1(std::move(impl)));
        return RealtimeLatestReadModelCreateErrorV1::kNone;
    } catch (...) {
        output->reset();
        return RealtimeLatestReadModelCreateErrorV1::
            kResourceExhausted;
    }
}

RealtimeLatestPublishErrorV1
RealtimeLatestReadModelV1::PublishApplied(
    std::size_t ordinal,
    const RealtimeHistoryRecordV1* record,
    bool* updated) noexcept {
    if (updated != nullptr) {
        *updated = false;
    }
    if (impl_->coverage_lost_.load(std::memory_order_acquire)) {
        return RealtimeLatestPublishErrorV1::kCoverageLost;
    }
    if (record == nullptr) {
        return RealtimeLatestPublishErrorV1::kNullRecord;
    }
    if (ordinal >= impl_->slot_count_) {
        return RealtimeLatestPublishErrorV1::kInvalidOrdinal;
    }
    ObservedInstrumentEntryViewV2 entry{};
    if (impl_->directory_->LookupByOrdinal(ordinal, &entry) !=
            ObservedInstrumentDirectoryErrorV2::kNone ||
        !entry.bound()) {
        return RealtimeLatestPublishErrorV1::kUnboundInstrument;
    }
    if (entry.ordinal != ordinal ||
        entry.instrument_id !=
            static_cast<std::uint32_t>(ordinal + 1U) ||
        record->instrument_id() != entry.instrument_id) {
        return RealtimeLatestPublishErrorV1::kInstrumentMismatch;
    }
    RealtimeLatestRecordKindV1 kind =
        RealtimeLatestRecordKindV1::kSnapshot;
    if (!LatestKindForMarketEvent(record->kind(), &kind)) {
        return RealtimeLatestPublishErrorV1::kInvalidRecordKind;
    }
    std::atomic<const RealtimeHistoryRecordV1*>* const target =
        impl_->Target(ordinal, kind);
    if (target == nullptr || record->ingress_sequence() == 0U) {
        return RealtimeLatestPublishErrorV1::kInvalidRecordKind;
    }

    const RealtimeHistoryRecordV1* current =
        target->load(std::memory_order_relaxed);
    if (current != record &&
        current != nullptr &&
        current->ingress_sequence() ==
            record->ingress_sequence()) {
        return RealtimeLatestPublishErrorV1::kIngressConflict;
    }
    if (current != nullptr &&
        current->ingress_sequence() >
            record->ingress_sequence()) {
        return RealtimeLatestPublishErrorV1::kNone;
    }
    if (current == record) {
        return RealtimeLatestPublishErrorV1::kNone;
    }
    // Exactly one permanent owner publishes a directory ordinal. Readers use
    // acquire loads; a release store is sufficient and avoids a locked RMW on
    // every market event.
    target->store(record, std::memory_order_release);
    if (updated != nullptr) {
        *updated = true;
    }
    return RealtimeLatestPublishErrorV1::kNone;
}

RealtimeLatestQueryErrorV1 RealtimeLatestReadModelV1::GetLatest(
    RealtimeLatestRecordKindV1 kind,
    std::uint32_t instrument_id,
    RealtimeLatestRecordViewV1* output) const noexcept {
    if (output == nullptr) {
        return RealtimeLatestQueryErrorV1::kNullOutput;
    }
    *output = RealtimeLatestRecordViewV1{};
    if (!ValidRecordKind(kind)) {
        return RealtimeLatestQueryErrorV1::kInvalidArgument;
    }
    if (impl_->coverage_lost_.load(std::memory_order_acquire)) {
        return RealtimeLatestQueryErrorV1::kCoverageLost;
    }
    impl_->QueryOne(kind, instrument_id, output);
    if (impl_->coverage_lost_.load(std::memory_order_acquire)) {
        *output = RealtimeLatestRecordViewV1{};
        return RealtimeLatestQueryErrorV1::kCoverageLost;
    }
    return RealtimeLatestQueryErrorV1::kNone;
}

RealtimeLatestQueryErrorV1
RealtimeLatestReadModelV1::GetLatestRecords(
    RealtimeLatestRecordKindV1 kind,
    std::span<const std::uint32_t> instrument_ids,
    std::span<RealtimeLatestRecordViewV1> output) const noexcept {
    ClearViews(output);
    if (!ValidRecordKind(kind) ||
        instrument_ids.size() != output.size()) {
        return RealtimeLatestQueryErrorV1::kInvalidArgument;
    }
    if (impl_->coverage_lost_.load(std::memory_order_acquire)) {
        return RealtimeLatestQueryErrorV1::kCoverageLost;
    }
    for (std::size_t index = 0U;
         index < instrument_ids.size();
         ++index) {
        impl_->QueryOne(kind, instrument_ids[index], &output[index]);
    }
    if (impl_->coverage_lost_.load(std::memory_order_acquire)) {
        ClearViews(output);
        return RealtimeLatestQueryErrorV1::kCoverageLost;
    }
    return RealtimeLatestQueryErrorV1::kNone;
}

RealtimeLatestQueryErrorV1
RealtimeLatestReadModelV1::GetLatestSnapshot(
    std::uint32_t instrument_id,
    RealtimeLatestRecordViewV1* output) const noexcept {
    return GetLatest(
        RealtimeLatestRecordKindV1::kSnapshot,
        instrument_id,
        output);
}

RealtimeLatestQueryErrorV1
RealtimeLatestReadModelV1::GetLatestSnapshots(
    std::span<const std::uint32_t> instrument_ids,
    std::span<RealtimeLatestRecordViewV1> output) const noexcept {
    return GetLatestRecords(
        RealtimeLatestRecordKindV1::kSnapshot,
        instrument_ids,
        output);
}

RealtimeLatestQueryErrorV1
RealtimeLatestReadModelV1::GetLatestTick(
    std::uint32_t instrument_id,
    RealtimeLatestRecordViewV1* output) const noexcept {
    return GetLatest(
        RealtimeLatestRecordKindV1::kTick,
        instrument_id,
        output);
}

RealtimeLatestQueryErrorV1
RealtimeLatestReadModelV1::GetLatestTicks(
    std::span<const std::uint32_t> instrument_ids,
    std::span<RealtimeLatestRecordViewV1> output) const noexcept {
    return GetLatestRecords(
        RealtimeLatestRecordKindV1::kTick,
        instrument_ids,
        output);
}

void RealtimeLatestReadModelV1::MarkCoverageLost() noexcept {
    impl_->coverage_lost_.store(true, std::memory_order_release);
}

bool RealtimeLatestReadModelV1::coverage_lost() const noexcept {
    return impl_->coverage_lost_.load(std::memory_order_acquire);
}

}  // namespace l2flow::market
