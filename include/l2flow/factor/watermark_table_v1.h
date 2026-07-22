#pragma once

#include "l2flow/factor/factor_watermark_v1.h"
#include "l2flow/factor/latest_factor_v1.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

namespace l2flow::factor {

enum class WatermarkTableErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidWatermark,
    kIdConflict,
    kIdentityMismatch,
    kNotFound,
    kResourceExhausted,
};

enum class WatermarkTableAppendDispositionV1 : std::uint8_t {
    kAppended = 1U,
    kAlreadyPresent = 2U,
};

struct WatermarkTableAppendResultV1 final {
    WatermarkTableErrorV1 error = WatermarkTableErrorV1::kNone;
    WatermarkTableAppendDispositionV1 disposition =
        WatermarkTableAppendDispositionV1::kAppended;

    [[nodiscard]] bool ok() const noexcept {
        return error == WatermarkTableErrorV1::kNone;
    }
};

[[nodiscard]] std::string_view WatermarkTableErrorNameV1(
    WatermarkTableErrorV1 error) noexcept;

// Process-local V1 table primitive.  It is append-only: entries are never
// replaced or removed.  Re-appending byte/field-exact content is idempotent;
// reusing an ID for any different full watermark map is a conflict.  A later
// POSIX persistence layer can journal this same immutable model without
// weakening its ID semantics.
class WatermarkTableV1 final {
public:
    WatermarkTableV1() = default;
    WatermarkTableV1(const WatermarkTableV1&) = delete;
    WatermarkTableV1& operator=(const WatermarkTableV1&) = delete;
    WatermarkTableV1(WatermarkTableV1&&) = delete;
    WatermarkTableV1& operator=(WatermarkTableV1&&) = delete;
    ~WatermarkTableV1() = default;

    [[nodiscard]] WatermarkTableAppendResultV1 Append(
        const FactorInputWatermarkSetV1& watermark) noexcept;

    [[nodiscard]] WatermarkTableErrorV1 Resolve(
        std::uint64_t watermark_set_id,
        std::shared_ptr<const FactorInputWatermarkSetV1>* output)
        const noexcept;

    [[nodiscard]] std::size_t size() const noexcept {
        return entries_.size();
    }

private:
    std::vector<std::shared_ptr<const FactorInputWatermarkSetV1>> entries_;
};

// Resolves a Latest Factor reference and verifies that the run-local table ID
// names the same stable input identity carried by the slot.
[[nodiscard]] WatermarkTableErrorV1 ResolveLatestFactorWatermarkV1(
    const WatermarkTableV1& table,
    const LatestFactorValueV1& latest,
    std::shared_ptr<const FactorInputWatermarkSetV1>* output) noexcept;

struct LatestFactorWatermarkPublishResultV1 final {
    WatermarkTableErrorV1 watermark_error =
        WatermarkTableErrorV1::kNone;
    LatestFactorPublishResultV1 publish{};

    [[nodiscard]] bool ok() const noexcept {
        return watermark_error == WatermarkTableErrorV1::kNone &&
               publish.ok();
    }
};

// Preferred process-local publication path. It first proves that the
// append-only table already contains the exact ID -> input-identity mapping,
// then delegates to the opaque slot publisher. Because V1 table rows are
// never removed or replaced, a successful check cannot become an orphan
// within the same externally synchronized table lifetime. The lower-level C
// slot ABI intentionally has no table handle and remains caller-protocol only.
[[nodiscard]] LatestFactorWatermarkPublishResultV1
PublishLatestFactorWithWatermarkV1(
    const WatermarkTableV1& table,
    LatestFactorSlotV1* slot,
    const LatestFactorValueV1& value) noexcept;

}  // namespace l2flow::factor
