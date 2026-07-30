#include "l2flow/factor/realtime_factor_engine_v1.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <span>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace l2flow::factor {
namespace {

constexpr std::size_t kMaximumDefinitionTextBytes = 4096U;
constexpr double kNormalizedP6Divisor = 1'000'000.0;

struct FactorPublicationCommitV1 final {
    std::shared_ptr<const RealtimeFactorGenerationV1>* slot =
        nullptr;
    std::shared_ptr<const RealtimeFactorGenerationV1> generation;
};

void CommitFactorPublicationV1(void* opaque) noexcept {
    auto* const commit =
        static_cast<FactorPublicationCommitV1*>(opaque);
    std::atomic_store_explicit(
        commit->slot, commit->generation, std::memory_order_release);
}

[[nodiscard]] bool DefinitionTextValid(std::string_view value) noexcept {
    return !value.empty() && value.size() <= kMaximumDefinitionTextBytes &&
           value.find('\0') == std::string_view::npos;
}

[[nodiscard]] bool DefinitionsValid(
    std::span<const RealtimeFactorDefinitionV1> definitions) noexcept {
    if (definitions.empty() ||
        definitions.size() > kRealtimeFactorMaximumDefinitionsV1) {
        return false;
    }
    for (std::size_t index = 0U; index < definitions.size(); ++index) {
        const RealtimeFactorDefinitionV1& definition = definitions[index];
        if (!DefinitionTextValid(definition.factor_id) ||
            !DefinitionTextValid(definition.factor_version) ||
            !DefinitionTextValid(definition.value_semantics)) {
            return false;
        }
        for (std::size_t previous = 0U; previous < index; ++previous) {
            if (definitions[previous].factor_id == definition.factor_id) {
                return false;
            }
        }
    }
    return true;
}

[[nodiscard]] bool DefinitionsExactEqual(
    std::span<const RealtimeFactorDefinitionV1> left,
    std::span<const RealtimeFactorDefinitionV1> right) noexcept {
    if (left.size() != right.size()) {
        return false;
    }
    for (std::size_t index = 0U; index < left.size(); ++index) {
        if (!RealtimeFactorDefinitionExactEqualV1(
                left[index], right[index])) {
            return false;
        }
    }
    return true;
}

template <typename Value>
[[nodiscard]] bool SameSharedOwnerAndPointer(
    const std::shared_ptr<const Value>& left,
    const std::shared_ptr<const Value>& right) noexcept {
    return left.get() == right.get() &&
           !left.owner_before(right) &&
           !right.owner_before(left);
}

[[nodiscard]] const l2flow::market::DecimalValueV1* SnapshotLastPrice(
    const l2flow::market::RealtimeHistoryRecordV1* record) noexcept {
    if (record == nullptr) {
        return nullptr;
    }
    const l2flow::market::StoredMarketEventViewV1 event = record->event();
    const auto* shanghai =
        l2flow::market::StoredMarketEventGetV1<
            l2flow::market::ShanghaiSnapshotV1>(event);
    if (shanghai != nullptr) {
        return &shanghai->last_price;
    }
    const auto* shenzhen =
        l2flow::market::StoredMarketEventGetV1<
            l2flow::market::ShenzhenSnapshotV1>(event);
    return shenzhen == nullptr ? nullptr : &shenzhen->last_price;
}

[[nodiscard]] bool SnapshotFactorEligible(
    const l2flow::market::RealtimeHistoryRecordV1* record) noexcept {
    const l2flow::market::DecimalValueV1* const last_price =
        SnapshotLastPrice(record);
    return last_price != nullptr && last_price->valid &&
           !last_price->is_null &&
           last_price->normalized_p6 > 0;
}

[[nodiscard]] bool StoreMatchesDailyCatalog(
    const l2flow::market::IntradayInstrumentStoreGenerationV1& store,
    std::vector<std::uint32_t>* eligible_instrument_ids) {
    if (eligible_instrument_ids == nullptr) {
        return false;
    }
    eligible_instrument_ids->clear();
    const l2flow::market::RealtimeHistoryWatermarkV1& watermark =
        store.watermark();
    const auto& catalog = store.catalog_snapshot();
    if (watermark.generation == 0U || catalog == nullptr ||
        !SameSharedOwnerAndPointer(
            watermark.catalog_snapshot, catalog) ||
        catalog->catalog_scope() !=
            l2flow::market::InstrumentCatalogScopeV2::
                kDeclaredDailyAShare ||
        !catalog->coverage_complete() ||
        catalog->trade_date() != watermark.trade_date ||
        catalog->catalog_version() == 0U ||
        catalog->session_epoch() == 0U ||
        catalog->capacity() == 0U ||
        catalog->bound_count() != catalog->capacity() ||
        catalog->catalog_generation() != 1U ||
        !watermark.processing_progress.valid() ||
        watermark.processing_progress.applied_sequence !=
            watermark.ingress_sequence_exclusive - 1U ||
        store.instrument_count() != catalog->bound_count()) {
        return false;
    }

    eligible_instrument_ids->reserve(
        catalog->factor_eligible_count());
    std::size_t available_count = 0U;
    std::size_t snapshot_available_count = 0U;
    std::size_t tick_available_count = 0U;
    std::size_t factor_eligible_count = 0U;
    for (std::size_t index = 0U;
         index < store.instrument_count();
         ++index) {
        l2flow::market::IntradayInstrumentSummaryV1 summary{};
        l2flow::market::InstrumentRuntimeEntryViewV2 entry{};
        if (store.SummaryAt(index, &summary) !=
                l2flow::market::IntradayInstrumentStoreQueryErrorV1::kNone ||
            catalog->EntryAt(index, &entry) !=
                l2flow::market::InstrumentRuntimeStateErrorV2::kNone ||
            !entry.bound() || entry.ordinal != index ||
            entry.instrument_id != summary.instrument_id ||
            entry.instrument_id !=
                static_cast<std::uint32_t>(index + 1U) ||
            entry.has_snapshot !=
                (summary.latest_snapshot != nullptr) ||
            entry.has_tick != (summary.latest_tick != nullptr) ||
            entry.factor_eligible !=
                SnapshotFactorEligible(summary.latest_snapshot) ||
            entry.available() !=
                (summary.latest_snapshot != nullptr ||
                 summary.latest_tick != nullptr)) {
            return false;
        }
        available_count += entry.available() ? 1U : 0U;
        snapshot_available_count += entry.has_snapshot ? 1U : 0U;
        tick_available_count += entry.has_tick ? 1U : 0U;
        if (entry.factor_eligible) {
            eligible_instrument_ids->push_back(entry.instrument_id);
            ++factor_eligible_count;
        }
    }
    return available_count == catalog->available_count() &&
           snapshot_available_count ==
               catalog->snapshot_available_count() &&
           tick_available_count == catalog->tick_available_count() &&
           factor_eligible_count ==
               catalog->factor_eligible_count() &&
           eligible_instrument_ids->size() ==
               factor_eligible_count;
}

[[nodiscard]] bool FactorOutputValid(
    std::span<const RealtimeFactorPointV1> points,
    std::span<const std::uint32_t> instrument_ids,
    std::size_t definition_count) noexcept {
    if (points.size() != instrument_ids.size()) {
        return false;
    }
    for (std::size_t row = 0U; row < points.size(); ++row) {
        const RealtimeFactorPointV1& point = points[row];
        if (point.instrument_id != instrument_ids[row] ||
            point.values.size() != definition_count) {
            return false;
        }
        for (const RealtimeFactorValueV1& value : point.values) {
            if (!std::isfinite(value.value)) {
                return false;
            }
            if (!value.valid &&
                (value.value != 0.0 || std::signbit(value.value))) {
                return false;
            }
        }
    }
    return true;
}

}  // namespace

bool RealtimeFactorDefinitionExactEqualV1(
    const RealtimeFactorDefinitionV1& left,
    const RealtimeFactorDefinitionV1& right) noexcept {
    return left.factor_id == right.factor_id &&
           left.factor_version == right.factor_version &&
           left.value_semantics == right.value_semantics;
}

std::string_view RealtimeFactorCalculatorErrorNameV1(
    RealtimeFactorCalculatorErrorV1 error) noexcept {
    switch (error) {
        case RealtimeFactorCalculatorErrorV1::kNone:
            return "none";
        case RealtimeFactorCalculatorErrorV1::kNullOutput:
            return "null_output";
        case RealtimeFactorCalculatorErrorV1::kInvalidStore:
            return "invalid_store";
        case RealtimeFactorCalculatorErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case RealtimeFactorCalculatorErrorV1::kCalculationFailed:
            return "calculation_failed";
    }
    return "invalid_realtime_factor_calculator_error";
}

SnapshotLastPriceProjectionV1::SnapshotLastPriceProjectionV1()
    : definitions_{{RealtimeFactorDefinitionV1{
          "snapshot_last_price_projection",
          "v1",
          "latest valid strictly-positive decoded snapshot "
          "last_price.normalized_p6 divided by 1000000; literal price "
          "projection, not a derived financial factor"}}} {}

std::span<const RealtimeFactorDefinitionV1>
SnapshotLastPriceProjectionV1::definitions() const noexcept {
    return definitions_;
}

RealtimeFactorCalculatorErrorV1
SnapshotLastPriceProjectionV1::Calculate(
    const l2flow::market::IntradayInstrumentStoreGenerationV1& store,
    std::vector<RealtimeFactorPointV1>* output) const noexcept {
    if (output == nullptr) {
        return RealtimeFactorCalculatorErrorV1::kNullOutput;
    }
    try {
        std::vector<RealtimeFactorPointV1> candidate;
        const auto& catalog = store.catalog_snapshot();
        if (catalog == nullptr ||
            catalog->bound_count() != store.instrument_count()) {
            return RealtimeFactorCalculatorErrorV1::kInvalidStore;
        }
        candidate.reserve(catalog->factor_eligible_count());
        for (std::size_t ordinal = 0U;
             ordinal < store.instrument_count();
             ++ordinal) {
            l2flow::market::IntradayInstrumentSummaryV1 instrument{};
            l2flow::market::InstrumentRuntimeEntryViewV2 entry{};
            if (store.SummaryAt(ordinal, &instrument) !=
                    l2flow::market::IntradayInstrumentStoreQueryErrorV1::
                        kNone ||
                catalog->EntryAt(ordinal, &entry) !=
                    l2flow::market::
                        InstrumentRuntimeStateErrorV2::kNone ||
                entry.instrument_id != instrument.instrument_id) {
                return RealtimeFactorCalculatorErrorV1::kInvalidStore;
            }
            if (!entry.factor_eligible) {
                continue;
            }
            RealtimeFactorPointV1 point{};
            point.instrument_id = instrument.instrument_id;
            point.values.resize(1U);
            const l2flow::market::DecimalValueV1* last_price =
                SnapshotLastPrice(instrument.latest_snapshot);
            if (last_price == nullptr || !last_price->valid ||
                last_price->is_null ||
                last_price->normalized_p6 <= 0) {
                return RealtimeFactorCalculatorErrorV1::kInvalidStore;
            }
            point.values[0U].value =
                static_cast<double>(last_price->normalized_p6) /
                kNormalizedP6Divisor;
            point.values[0U].valid = true;
            candidate.push_back(std::move(point));
        }
        if (candidate.size() != catalog->factor_eligible_count()) {
            return RealtimeFactorCalculatorErrorV1::kInvalidStore;
        }
        *output = std::move(candidate);
        return RealtimeFactorCalculatorErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return RealtimeFactorCalculatorErrorV1::kResourceExhausted;
    } catch (const std::length_error&) {
        return RealtimeFactorCalculatorErrorV1::kResourceExhausted;
    } catch (...) {
        return RealtimeFactorCalculatorErrorV1::kCalculationFailed;
    }
}

RealtimeFactorGenerationV1::RealtimeFactorGenerationV1(
    std::shared_ptr<
        const l2flow::market::IntradayInstrumentStoreGenerationV1>
        input_store,
    std::vector<RealtimeFactorDefinitionV1> definitions,
    std::vector<RealtimeFactorPointV1> points) noexcept
    : input_store_(std::move(input_store)),
      watermark_(input_store_->watermark()),
      definitions_(std::move(definitions)),
      points_(std::move(points)) {}

const RealtimeFactorPointV1* RealtimeFactorGenerationV1::Find(
    std::uint32_t instrument_id) const noexcept {
    const auto iterator = std::lower_bound(
        points_.begin(),
        points_.end(),
        instrument_id,
        [](const RealtimeFactorPointV1& point,
           std::uint32_t expected) noexcept {
            return point.instrument_id < expected;
        });
    return iterator != points_.end() &&
                   iterator->instrument_id == instrument_id
               ? &*iterator
               : nullptr;
}

std::string_view RealtimeFactorEngineCreateErrorNameV1(
    RealtimeFactorEngineCreateErrorV1 error) noexcept {
    switch (error) {
        case RealtimeFactorEngineCreateErrorV1::kNone:
            return "none";
        case RealtimeFactorEngineCreateErrorV1::kNullOutput:
            return "null_output";
        case RealtimeFactorEngineCreateErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case RealtimeFactorEngineCreateErrorV1::kInvalidDefinitions:
            return "invalid_definitions";
        case RealtimeFactorEngineCreateErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case RealtimeFactorEngineCreateErrorV1::kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "invalid_realtime_factor_engine_create_error";
}

std::string_view RealtimeFactorPublishErrorNameV1(
    RealtimeFactorPublishErrorV1 error) noexcept {
    switch (error) {
        case RealtimeFactorPublishErrorV1::kNone:
            return "none";
        case RealtimeFactorPublishErrorV1::kNullStore:
            return "null_store";
        case RealtimeFactorPublishErrorV1::kInvalidStore:
            return "invalid_store";
        case RealtimeFactorPublishErrorV1::kStoreNotCurrentOrHealthy:
            return "store_not_current_or_healthy";
        case RealtimeFactorPublishErrorV1::kAlreadyPublished:
            return "already_published";
        case RealtimeFactorPublishErrorV1::kGenerationNotIncreasing:
            return "generation_not_increasing";
        case RealtimeFactorPublishErrorV1::kCalculatorSchemaChanged:
            return "calculator_schema_changed";
        case RealtimeFactorPublishErrorV1::kCalculatorFailed:
            return "calculator_failed";
        case RealtimeFactorPublishErrorV1::kInvalidFactorOutput:
            return "invalid_factor_output";
        case RealtimeFactorPublishErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case RealtimeFactorPublishErrorV1::kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "invalid_realtime_factor_publish_error";
}

RealtimeFactorEngineV1::RealtimeFactorEngineV1(
    RealtimeFactorEngineConfigV1 config,
    std::vector<RealtimeFactorDefinitionV1> definitions) noexcept
    : config_(std::move(config)),
      definitions_(std::move(definitions)),
      latest_(std::shared_ptr<const RealtimeFactorGenerationV1>{}) {}

RealtimeFactorEngineCreateErrorV1 RealtimeFactorEngineV1::Create(
    RealtimeFactorEngineConfigV1 config,
    std::unique_ptr<RealtimeFactorEngineV1>* output) noexcept {
    if (output == nullptr) {
        return RealtimeFactorEngineCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (config.generation_runtime == nullptr ||
        config.calculator == nullptr) {
        return RealtimeFactorEngineCreateErrorV1::kInvalidConfiguration;
    }

    const std::span<const RealtimeFactorDefinitionV1> live_definitions =
        config.calculator->definitions();
    if (!DefinitionsValid(live_definitions)) {
        return RealtimeFactorEngineCreateErrorV1::kInvalidDefinitions;
    }

    try {
        std::vector<RealtimeFactorDefinitionV1> definitions(
            live_definitions.begin(), live_definitions.end());

        output->reset(new RealtimeFactorEngineV1(
            std::move(config),
            std::move(definitions)));
        return RealtimeFactorEngineCreateErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return RealtimeFactorEngineCreateErrorV1::kResourceExhausted;
    } catch (const std::length_error&) {
        return RealtimeFactorEngineCreateErrorV1::kResourceExhausted;
    } catch (...) {
        return RealtimeFactorEngineCreateErrorV1::kUnexpectedFailure;
    }
}

RealtimeFactorPublishResultV1
RealtimeFactorEngineV1::CalculateAndPublish(
    std::shared_ptr<
        const l2flow::market::IntradayInstrumentStoreGenerationV1>
        store) noexcept {
    RealtimeFactorPublishResultV1 result{};
    if (store == nullptr) {
        result.error = RealtimeFactorPublishErrorV1::kNullStore;
        return result;
    }

    try {
        const std::lock_guard<std::mutex> guard(publish_mutex_);
        if (!config_.generation_runtime->IsGenerationCurrentAndHealthy(
                store)) {
            result.error = RealtimeFactorPublishErrorV1::
                kStoreNotCurrentOrHealthy;
            return result;
        }
        std::vector<std::uint32_t> eligible_instrument_ids;
        if (!StoreMatchesDailyCatalog(
                *store, &eligible_instrument_ids)) {
            result.error = RealtimeFactorPublishErrorV1::kInvalidStore;
            return result;
        }

        const std::shared_ptr<const RealtimeFactorGenerationV1> previous =
            std::atomic_load_explicit(
                &latest_, std::memory_order_acquire);
        if (previous != nullptr) {
            if (SameSharedOwnerAndPointer(
                    previous->input_store(), store)) {
                result.error =
                    RealtimeFactorPublishErrorV1::kAlreadyPublished;
                return result;
            }
            if (store->watermark().generation <=
                previous->watermark().generation) {
                result.error = RealtimeFactorPublishErrorV1::
                    kGenerationNotIncreasing;
                return result;
            }
        }

        if (!DefinitionsExactEqual(
                definitions_, config_.calculator->definitions())) {
            result.error = RealtimeFactorPublishErrorV1::
                kCalculatorSchemaChanged;
            return result;
        }

        std::vector<RealtimeFactorPointV1> points;
        result.calculator_error =
            config_.calculator->Calculate(*store, &points);
        if (result.calculator_error !=
            RealtimeFactorCalculatorErrorV1::kNone) {
            result.error =
                RealtimeFactorPublishErrorV1::kCalculatorFailed;
            return result;
        }

        if (!DefinitionsExactEqual(
                definitions_, config_.calculator->definitions())) {
            result.error = RealtimeFactorPublishErrorV1::
                kCalculatorSchemaChanged;
            return result;
        }
        if (!FactorOutputValid(
                points,
                eligible_instrument_ids,
                definitions_.size())) {
            result.error = RealtimeFactorPublishErrorV1::
                kInvalidFactorOutput;
            return result;
        }

        std::shared_ptr<const RealtimeFactorGenerationV1> candidate(
            new RealtimeFactorGenerationV1(
                store, definitions_, std::move(points)));

        // Calculation and allocation happen outside the generation commit
        // lock.
        // The small guarded action below makes the final eligibility check
        // and whole-factor-generation store indivisible with respect to a
        // store-generation replacement, fatal transition, or stop.
        FactorPublicationCommitV1 commit{&latest_, candidate};
        if (!config_.generation_runtime->CommitIfCurrentAndHealthy(
                store, &CommitFactorPublicationV1, &commit)) {
            result.error = RealtimeFactorPublishErrorV1::
                kStoreNotCurrentOrHealthy;
            return result;
        }
        result.generation = std::move(candidate);
        return result;
    } catch (const std::bad_alloc&) {
        result.error = RealtimeFactorPublishErrorV1::kResourceExhausted;
        return result;
    } catch (const std::length_error&) {
        result.error = RealtimeFactorPublishErrorV1::kResourceExhausted;
        return result;
    } catch (...) {
        result.error = RealtimeFactorPublishErrorV1::kUnexpectedFailure;
        return result;
    }
}

std::shared_ptr<const RealtimeFactorGenerationV1>
RealtimeFactorEngineV1::AcquireLatestGeneration() const noexcept {
    return std::atomic_load_explicit(
        &latest_, std::memory_order_acquire);
}

}  // namespace l2flow::factor
