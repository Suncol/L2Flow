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

[[nodiscard]] bool HistoryMatchesRegistry(
    const l2flow::market::RealtimeHistoryGenerationV1& history,
    const l2flow::market::InstrumentRegistryV1& registry,
    std::span<const std::uint32_t> instrument_ids) noexcept {
    const l2flow::market::RealtimeHistoryWatermarkV1& watermark =
        history.watermark();
    if (watermark.generation == 0U ||
        watermark.registry_version != registry.registry_version() ||
        watermark.registry_sha256 != registry.registry_sha256()) {
        return false;
    }

    const std::span<
        const l2flow::market::RealtimeInstrumentGenerationV1>
        instruments = history.instruments();
    if (instruments.size() != instrument_ids.size()) {
        return false;
    }
    for (std::size_t index = 0U; index < instruments.size(); ++index) {
        if (instruments[index].instrument_id != instrument_ids[index]) {
            return false;
        }
    }
    return true;
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

[[nodiscard]] const l2flow::market::DecimalValueV1* SnapshotLastPrice(
    const l2flow::market::RealtimeHistoryRecordHandleV1& record) noexcept {
    if (record == nullptr) {
        return nullptr;
    }
    const l2flow::market::RetainedMarketEventV1& event = record->event();
    const auto* shanghai =
        l2flow::market::RetainedMarketEventGetV1<
            l2flow::market::ShanghaiSnapshotV1>(event);
    if (shanghai != nullptr) {
        return &shanghai->last_price;
    }
    const auto* shenzhen =
        l2flow::market::RetainedMarketEventGetV1<
            l2flow::market::ShenzhenSnapshotV1>(event);
    return shenzhen == nullptr ? nullptr : &shenzhen->last_price;
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
        case RealtimeFactorCalculatorErrorV1::kInvalidHistory:
            return "invalid_history";
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
    const l2flow::market::RealtimeHistoryGenerationV1& history,
    std::vector<RealtimeFactorPointV1>* output) const noexcept {
    if (output == nullptr) {
        return RealtimeFactorCalculatorErrorV1::kNullOutput;
    }
    try {
        std::vector<RealtimeFactorPointV1> candidate;
        const std::span<
            const l2flow::market::RealtimeInstrumentGenerationV1>
            instruments = history.instruments();
        candidate.reserve(instruments.size());
        for (const l2flow::market::RealtimeInstrumentGenerationV1&
                 instrument : instruments) {
            RealtimeFactorPointV1 point{};
            point.instrument_id = instrument.instrument_id;
            point.values.resize(1U);
            const l2flow::market::DecimalValueV1* last_price =
                SnapshotLastPrice(instrument.latest_snapshot);
            if (last_price != nullptr && last_price->valid &&
                !last_price->is_null &&
                last_price->normalized_p6 > 0) {
                point.values[0U].value =
                    static_cast<double>(last_price->normalized_p6) /
                    kNormalizedP6Divisor;
                point.values[0U].valid = true;
            }
            candidate.push_back(std::move(point));
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
    std::shared_ptr<const l2flow::market::RealtimeHistoryGenerationV1>
        input_history,
    std::vector<RealtimeFactorDefinitionV1> definitions,
    std::vector<RealtimeFactorPointV1> points) noexcept
    : input_history_(std::move(input_history)),
      watermark_(input_history_->watermark()),
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
        case RealtimeFactorPublishErrorV1::kNullHistory:
            return "null_history";
        case RealtimeFactorPublishErrorV1::kInvalidHistory:
            return "invalid_history";
        case RealtimeFactorPublishErrorV1::kHistoryNotCurrentOrHealthy:
            return "history_not_current_or_healthy";
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
    std::vector<RealtimeFactorDefinitionV1> definitions,
    std::vector<std::uint32_t> instrument_ids) noexcept
    : config_(std::move(config)),
      definitions_(std::move(definitions)),
      instrument_ids_(std::move(instrument_ids)),
      latest_(std::shared_ptr<const RealtimeFactorGenerationV1>{}) {}

RealtimeFactorEngineCreateErrorV1 RealtimeFactorEngineV1::Create(
    RealtimeFactorEngineConfigV1 config,
    std::unique_ptr<RealtimeFactorEngineV1>* output) noexcept {
    if (output == nullptr) {
        return RealtimeFactorEngineCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (config.registry == nullptr || config.history_runtime == nullptr ||
        config.calculator == nullptr || config.registry->empty()) {
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
        std::vector<std::uint32_t> instrument_ids;
        instrument_ids.reserve(config.registry->size());
        for (const l2flow::market::InstrumentRegistryEntryV1& entry :
             config.registry->entries()) {
            instrument_ids.push_back(entry.instrument_id);
        }
        std::sort(instrument_ids.begin(), instrument_ids.end());

        output->reset(new RealtimeFactorEngineV1(
            std::move(config),
            std::move(definitions),
            std::move(instrument_ids)));
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
    std::shared_ptr<const l2flow::market::RealtimeHistoryGenerationV1>
        history) noexcept {
    RealtimeFactorPublishResultV1 result{};
    if (history == nullptr) {
        result.error = RealtimeFactorPublishErrorV1::kNullHistory;
        return result;
    }

    try {
        const std::lock_guard<std::mutex> guard(publish_mutex_);
        if (!HistoryMatchesRegistry(
                *history, *config_.registry, instrument_ids_)) {
            result.error = RealtimeFactorPublishErrorV1::kInvalidHistory;
            return result;
        }
        if (!config_.history_runtime->IsGenerationCurrentAndHealthy(
                history)) {
            result.error = RealtimeFactorPublishErrorV1::
                kHistoryNotCurrentOrHealthy;
            return result;
        }

        const std::shared_ptr<const RealtimeFactorGenerationV1> previous =
            std::atomic_load_explicit(
                &latest_, std::memory_order_acquire);
        if (previous != nullptr) {
            if (previous->input_history().get() == history.get()) {
                result.error =
                    RealtimeFactorPublishErrorV1::kAlreadyPublished;
                return result;
            }
            if (history->watermark().generation <=
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
            config_.calculator->Calculate(*history, &points);
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
                points, instrument_ids_, definitions_.size())) {
            result.error = RealtimeFactorPublishErrorV1::
                kInvalidFactorOutput;
            return result;
        }

        std::shared_ptr<const RealtimeFactorGenerationV1> candidate(
            new RealtimeFactorGenerationV1(
                history, definitions_, std::move(points)));

        // Calculation and allocation happen outside the history commit lock.
        // The small guarded action below makes the final eligibility check
        // and whole-factor-generation store indivisible with respect to a
        // history replacement, fatal transition, or stop.
        FactorPublicationCommitV1 commit{&latest_, candidate};
        if (!config_.history_runtime->CommitIfCurrentAndHealthy(
                history, &CommitFactorPublicationV1, &commit)) {
            result.error = RealtimeFactorPublishErrorV1::
                kHistoryNotCurrentOrHealthy;
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
