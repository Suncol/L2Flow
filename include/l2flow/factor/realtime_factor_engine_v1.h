#pragma once

#include "l2flow/market/realtime_history_v1.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace l2flow::factor {

inline constexpr std::size_t kRealtimeFactorMaximumDefinitionsV1 = 1024U;

// A definition identifies the meaning and version of one scalar column.  The
// engine treats the ordered definition list as immutable schema: calculators
// may implement arbitrary documented mathematics, but may not silently
// change a column's meaning between generations.
struct RealtimeFactorDefinitionV1 final {
    std::string factor_id;
    std::string factor_version;
    std::string value_semantics;
};

[[nodiscard]] bool RealtimeFactorDefinitionExactEqualV1(
    const RealtimeFactorDefinitionV1& left,
    const RealtimeFactorDefinitionV1& right) noexcept;

// Missing/undefined is represented explicitly, never by NaN.  Every value,
// including an invalid one, must be finite.  Invalid values additionally use
// canonical +0.0 so consumers cannot accidentally assign meaning to payload
// bits hidden behind valid=false.
struct RealtimeFactorValueV1 final {
    double value = 0.0;
    bool valid = false;
};

// One row of a calculator's staged output.  values are in the exact order of
// RealtimeFactorCalculatorV1::definitions().
struct RealtimeFactorPointV1 final {
    std::uint32_t instrument_id = 0U;
    std::vector<RealtimeFactorValueV1> values;
};

enum class RealtimeFactorCalculatorErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidStore,
    kResourceExhausted,
    kCalculationFailed,
};

[[nodiscard]] std::string_view RealtimeFactorCalculatorErrorNameV1(
    RealtimeFactorCalculatorErrorV1 error) noexcept;

// A calculator is a pure, non-reentrant observed-universe transformation. It
// receives one immutable Store generation and must return exactly one row,
// in ascending ID order, for each instrument marked factor_eligible in that
// generation's exact CatalogSnapshot. It must not infer an authoritative
// exchange-wide denominator from this subset.
class RealtimeFactorCalculatorV1 {
public:
    virtual ~RealtimeFactorCalculatorV1() = default;

    // The returned schema and its backing storage must remain immutable for
    // the calculator's lifetime.  The engine snapshots and revalidates it.
    [[nodiscard]] virtual std::span<const RealtimeFactorDefinitionV1>
    definitions() const noexcept = 0;

    // On success output contains a complete candidate batch.  On failure its
    // contents are ignored.  Implementations must not throw across noexcept.
    [[nodiscard]] virtual RealtimeFactorCalculatorErrorV1 Calculate(
        const l2flow::market::IntradayInstrumentStoreGenerationV1& store,
        std::vector<RealtimeFactorPointV1>* output) const noexcept = 0;
};

// A deliberately literal example/default calculator.  This is not presented
// as alpha, fair value, microprice, imbalance, or any other financial model.
// It projects the latest valid, strictly positive decoded snapshot
// last_price.normalized_p6 into decimal price units by dividing by 1,000,000.
// Instruments without a positive snapshot last price are outside the
// factor-eligible input set and therefore produce no row.
class SnapshotLastPriceProjectionV1 final
    : public RealtimeFactorCalculatorV1 {
public:
    SnapshotLastPriceProjectionV1();

    [[nodiscard]] std::span<const RealtimeFactorDefinitionV1>
    definitions() const noexcept override;
    [[nodiscard]] RealtimeFactorCalculatorErrorV1 Calculate(
        const l2flow::market::IntradayInstrumentStoreGenerationV1& store,
        std::vector<RealtimeFactorPointV1>* output) const noexcept override;

private:
    std::array<RealtimeFactorDefinitionV1, 1U> definitions_;
};

// One immutable observed-universe publication. Its watermark and exact
// CatalogSnapshot are retained through input_store. Readers acquire this
// object once and cannot observe mismatched catalog/count/factor generations.
class RealtimeFactorGenerationV1 final {
public:
    [[nodiscard]] const l2flow::market::RealtimeHistoryWatermarkV1&
    watermark() const noexcept {
        return watermark_;
    }
    [[nodiscard]] std::span<const RealtimeFactorDefinitionV1>
    definitions() const noexcept {
        return definitions_;
    }
    [[nodiscard]] std::span<const RealtimeFactorPointV1> points()
        const noexcept {
        return points_;
    }
    [[nodiscard]] const RealtimeFactorPointV1* Find(
        std::uint32_t instrument_id) const noexcept;
    [[nodiscard]] const std::shared_ptr<
        const l2flow::market::IntradayInstrumentStoreGenerationV1>&
    input_store() const noexcept {
        return input_store_;
    }
    [[nodiscard]] const std::shared_ptr<const
        l2flow::market::ObservedInstrumentCatalogSnapshotV2>&
    catalog_snapshot() const noexcept {
        return input_store_->catalog_snapshot();
    }
    [[nodiscard]] l2flow::market::ObservedInstrumentCatalogScopeV2
    catalog_scope() const noexcept {
        return catalog_snapshot()->catalog_scope();
    }
    [[nodiscard]] bool coverage_complete() const noexcept {
        return catalog_snapshot()->coverage_complete();
    }
    [[nodiscard]] std::uint64_t session_epoch() const noexcept {
        return catalog_snapshot()->session_epoch();
    }
    [[nodiscard]] std::size_t capacity() const noexcept {
        return catalog_snapshot()->capacity();
    }
    [[nodiscard]] std::uint64_t catalog_generation() const noexcept {
        return catalog_snapshot()->catalog_generation();
    }
    [[nodiscard]] std::uint64_t data_state_generation() const noexcept {
        return catalog_snapshot()->data_state_generation();
    }
    [[nodiscard]] const l2flow::common::Sha256Digest& catalog_digest()
        const noexcept {
        return catalog_snapshot()->catalog_digest();
    }
    [[nodiscard]] std::size_t bound_count() const noexcept {
        return catalog_snapshot()->bound_count();
    }
    [[nodiscard]] std::size_t universe_count() const noexcept {
        return bound_count();
    }
    [[nodiscard]] std::size_t available_count() const noexcept {
        return catalog_snapshot()->available_count();
    }
    [[nodiscard]] std::size_t snapshot_available_count() const noexcept {
        return catalog_snapshot()->snapshot_available_count();
    }
    [[nodiscard]] std::size_t tick_available_count() const noexcept {
        return catalog_snapshot()->tick_available_count();
    }
    [[nodiscard]] std::size_t factor_eligible_count() const noexcept {
        return catalog_snapshot()->factor_eligible_count();
    }
    [[nodiscard]] std::uint64_t input_applied_sequence() const noexcept {
        return watermark_.processing_progress.applied_sequence;
    }
    [[nodiscard]] std::uint64_t capture_accepted_sequence()
        const noexcept {
        return watermark_.processing_progress.accepted_sequence;
    }
    [[nodiscard]] std::uint64_t capture_durable_sequence() const noexcept {
        return watermark_.processing_progress.durable_sequence;
    }
    [[nodiscard]] std::uint64_t processing_lag_records()
        const noexcept {
        return watermark_.processing_progress.processing_lag_records();
    }
    [[nodiscard]] std::uint64_t durability_lag_records()
        const noexcept {
        return watermark_.processing_progress.durability_lag_records();
    }

private:
    friend class RealtimeFactorEngineV1;

    RealtimeFactorGenerationV1(
        std::shared_ptr<
            const l2flow::market::IntradayInstrumentStoreGenerationV1>
            input_store,
        std::vector<RealtimeFactorDefinitionV1> definitions,
        std::vector<RealtimeFactorPointV1> points) noexcept;

    std::shared_ptr<
        const l2flow::market::IntradayInstrumentStoreGenerationV1>
        input_store_;
    l2flow::market::RealtimeHistoryWatermarkV1 watermark_{};
    std::vector<RealtimeFactorDefinitionV1> definitions_;
    std::vector<RealtimeFactorPointV1> points_;
};

struct RealtimeFactorEngineConfigV1 final {
    // generation_runtime must outlive the engine. The calculator is
    // shared-owned because a calculation may be deliberately long-running.
    const l2flow::market::RealtimeHistoryRuntimeV1* generation_runtime =
        nullptr;
    std::shared_ptr<const RealtimeFactorCalculatorV1> calculator;
};

enum class RealtimeFactorEngineCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kInvalidDefinitions,
    kResourceExhausted,
    kUnexpectedFailure,
};

enum class RealtimeFactorPublishErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullStore,
    kInvalidStore,
    kStoreNotCurrentOrHealthy,
    kAlreadyPublished,
    kGenerationNotIncreasing,
    kCalculatorSchemaChanged,
    kCalculatorFailed,
    kInvalidFactorOutput,
    kResourceExhausted,
    kUnexpectedFailure,
};

[[nodiscard]] std::string_view RealtimeFactorEngineCreateErrorNameV1(
    RealtimeFactorEngineCreateErrorV1 error) noexcept;
[[nodiscard]] std::string_view RealtimeFactorPublishErrorNameV1(
    RealtimeFactorPublishErrorV1 error) noexcept;

struct RealtimeFactorPublishResultV1 final {
    RealtimeFactorPublishErrorV1 error =
        RealtimeFactorPublishErrorV1::kNone;
    RealtimeFactorCalculatorErrorV1 calculator_error =
        RealtimeFactorCalculatorErrorV1::kNone;
    std::shared_ptr<const RealtimeFactorGenerationV1> generation;

    [[nodiscard]] bool published() const noexcept {
        return error == RealtimeFactorPublishErrorV1::kNone &&
               generation != nullptr;
    }
};

class RealtimeFactorEngineV1 final {
public:
    RealtimeFactorEngineV1(const RealtimeFactorEngineV1&) = delete;
    RealtimeFactorEngineV1& operator=(const RealtimeFactorEngineV1&) = delete;
    RealtimeFactorEngineV1(RealtimeFactorEngineV1&&) = delete;
    RealtimeFactorEngineV1& operator=(RealtimeFactorEngineV1&&) = delete;
    ~RealtimeFactorEngineV1() = default;

    [[nodiscard]] static RealtimeFactorEngineCreateErrorV1 Create(
        RealtimeFactorEngineConfigV1 config,
        std::unique_ptr<RealtimeFactorEngineV1>* output) noexcept;

    // Calls are serialized so two calculations cannot race to publish the
    // same generation.  The expensive calculation is staged off-publication;
    // store health/current-generation status is checked both before it and
    // immediately before the single release publication.
    [[nodiscard]] RealtimeFactorPublishResultV1 CalculateAndPublish(
        std::shared_ptr<
            const l2flow::market::IntradayInstrumentStoreGenerationV1>
            store) noexcept;

    [[nodiscard]] std::shared_ptr<const RealtimeFactorGenerationV1>
    AcquireLatestGeneration() const noexcept;

private:
    RealtimeFactorEngineV1(
        RealtimeFactorEngineConfigV1 config,
        std::vector<RealtimeFactorDefinitionV1> definitions) noexcept;

    RealtimeFactorEngineConfigV1 config_;
    std::vector<RealtimeFactorDefinitionV1> definitions_;
    mutable std::mutex publish_mutex_;
    // Use the standardized shared_ptr atomic free functions. This preserves
    // the same acquire/release publication contract on libstdc++ versions
    // that predate atomic<shared_ptr>'s C++20 specialization.
    std::shared_ptr<const RealtimeFactorGenerationV1> latest_;
};

}  // namespace l2flow::factor
