#include "l2flow/factor/watermark_table_v1.h"

#include <new>

namespace l2flow::factor {

std::string_view WatermarkTableErrorNameV1(
    WatermarkTableErrorV1 error) noexcept {
    switch (error) {
        case WatermarkTableErrorV1::kNone:
            return "none";
        case WatermarkTableErrorV1::kNullOutput:
            return "null_output";
        case WatermarkTableErrorV1::kInvalidWatermark:
            return "invalid_watermark";
        case WatermarkTableErrorV1::kIdConflict:
            return "id_conflict";
        case WatermarkTableErrorV1::kIdentityMismatch:
            return "identity_mismatch";
        case WatermarkTableErrorV1::kNotFound:
            return "not_found";
        case WatermarkTableErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "invalid_watermark_table_error";
}

WatermarkTableAppendResultV1 WatermarkTableV1::Append(
    const FactorInputWatermarkSetV1& watermark) noexcept {
    WatermarkTableAppendResultV1 result{};
    if (ValidateFactorInputWatermarkSetV1(watermark) !=
        FactorWatermarkErrorV1::kNone) {
        result.error = WatermarkTableErrorV1::kInvalidWatermark;
        return result;
    }
    for (const auto& existing : entries_) {
        if (existing->watermark_set_id == watermark.watermark_set_id) {
            if (!FactorInputWatermarkSetExactEqualV1(*existing, watermark)) {
                result.error = WatermarkTableErrorV1::kIdConflict;
                return result;
            }
            result.disposition =
                WatermarkTableAppendDispositionV1::kAlreadyPresent;
            return result;
        }
    }
    try {
        entries_.push_back(
            std::make_shared<const FactorInputWatermarkSetV1>(watermark));
    } catch (const std::bad_alloc&) {
        result.error = WatermarkTableErrorV1::kResourceExhausted;
        return result;
    } catch (...) {
        result.error = WatermarkTableErrorV1::kResourceExhausted;
        return result;
    }
    result.disposition = WatermarkTableAppendDispositionV1::kAppended;
    return result;
}

WatermarkTableErrorV1 WatermarkTableV1::Resolve(
    std::uint64_t watermark_set_id,
    std::shared_ptr<const FactorInputWatermarkSetV1>* output) const noexcept {
    if (output == nullptr) {
        return WatermarkTableErrorV1::kNullOutput;
    }
    output->reset();
    if (watermark_set_id == 0U) {
        return WatermarkTableErrorV1::kNotFound;
    }
    for (const auto& entry : entries_) {
        if (entry->watermark_set_id == watermark_set_id) {
            *output = entry;
            return WatermarkTableErrorV1::kNone;
        }
    }
    return WatermarkTableErrorV1::kNotFound;
}

WatermarkTableErrorV1 ResolveLatestFactorWatermarkV1(
    const WatermarkTableV1& table,
    const LatestFactorValueV1& latest,
    std::shared_ptr<const FactorInputWatermarkSetV1>* output) noexcept {
    if (output == nullptr) {
        return WatermarkTableErrorV1::kNullOutput;
    }
    output->reset();
    std::shared_ptr<const FactorInputWatermarkSetV1> resolved;
    const WatermarkTableErrorV1 error =
        table.Resolve(latest.watermark_set_id, &resolved);
    if (error != WatermarkTableErrorV1::kNone) {
        return error;
    }
    if (resolved->input_identity_sha256 !=
        latest.input_identity_sha256) {
        return WatermarkTableErrorV1::kIdentityMismatch;
    }
    *output = std::move(resolved);
    return WatermarkTableErrorV1::kNone;
}

LatestFactorWatermarkPublishResultV1
PublishLatestFactorWithWatermarkV1(
    const WatermarkTableV1& table,
    LatestFactorSlotV1* slot,
    const LatestFactorValueV1& value) noexcept {
    LatestFactorWatermarkPublishResultV1 result{};
    std::shared_ptr<const FactorInputWatermarkSetV1> resolved;
    result.watermark_error = ResolveLatestFactorWatermarkV1(
        table, value, &resolved);
    if (result.watermark_error != WatermarkTableErrorV1::kNone) {
        return result;
    }
    result.publish = PublishLatestFactorV1(slot, value);
    return result;
}

}  // namespace l2flow::factor
