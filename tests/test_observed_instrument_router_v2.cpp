#include "l2flow/control/quality_flags_v1.h"
#include "l2flow/market/market_decoder.h"

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <span>
#include <string_view>
#include <vector>

namespace {

using l2flow::control::QualityBit;
using l2flow::control::QualityFlagV1;
using namespace l2flow::market;

void StoreU16(
    std::vector<std::byte>* bytes,
    std::size_t offset,
    std::uint16_t value) {
    (*bytes)[offset] = static_cast<std::byte>(value & 0xffU);
    (*bytes)[offset + 1U] =
        static_cast<std::byte>((value >> 8U) & 0xffU);
}

void StoreU32(
    std::vector<std::byte>* bytes,
    std::size_t offset,
    std::uint32_t value) {
    for (std::size_t index = 0U; index < 4U; ++index) {
        (*bytes)[offset + index] = static_cast<std::byte>(
            (value >> static_cast<unsigned int>(index * 8U)) & 0xffU);
    }
}

void StoreDescriptor(
    std::vector<std::byte>* bytes,
    std::size_t descriptor_offset,
    std::size_t value_offset,
    std::string_view value) {
    StoreU16(
        bytes,
        descriptor_offset,
        static_cast<std::uint16_t>(value.size()));
    StoreU32(
        bytes,
        descriptor_offset + 2U,
        static_cast<std::uint32_t>(value_offset - descriptor_offset));
    for (std::size_t index = 0U; index < value.size(); ++index) {
        (*bytes)[value_offset + index] =
            static_cast<std::byte>(
                static_cast<unsigned char>(value[index]));
    }
}

bool Check(bool value, const char* message) {
    if (!value) {
        std::cerr << message << '\n';
    }
    return value;
}

bool TestAllocationFreeExtraction() {
    std::vector<std::byte> sh_body(248U + 6U);
    StoreDescriptor(&sh_body, 4U, 248U, "600000");
    MarketMessageViewV1 sh{};
    sh.service_id = 4U;
    sh.service_version = 101U;
    sh.message_id = 4U;
    sh.body = sh_body;
    ObservedInstrumentKeyViewV2 key{};
    if (!Check(
            ExtractObservedInstrumentKeyV2(sh, 64U, &key) ==
                MarketDecodeErrorV1::kNone,
            "Shanghai key extraction failed") ||
        !Check(key.market == MarketV1::kShanghai, "wrong Shanghai market") ||
        !Check(key.security_id_source.empty(), "Shanghai source is not empty") ||
        !Check(key.security_id.size() == 6U, "wrong Shanghai id size")) {
        return false;
    }

    std::vector<std::byte> sz_body(224U + 9U);
    StoreDescriptor(&sz_body, 14U, 224U, "000001");
    StoreDescriptor(&sz_body, 20U, 230U, "102");
    MarketMessageViewV1 sz{};
    sz.service_id = 6U;
    sz.service_version = 101U;
    sz.message_id = 28U;
    sz.body = sz_body;
    if (!Check(
            ExtractObservedInstrumentKeyV2(sz, 64U, &key) ==
                MarketDecodeErrorV1::kNone,
            "Shenzhen key extraction failed") ||
        !Check(key.market == MarketV1::kShenzhen, "wrong Shenzhen market") ||
        !Check(key.security_id.size() == 6U, "wrong Shenzhen id size") ||
        !Check(key.security_id_source.size() == 3U, "wrong source size")) {
        return false;
    }

    StoreDescriptor(&sz_body, 20U, 224U, "000");
    sz.body = sz_body;
    return Check(
        ExtractObservedInstrumentKeyV2(sz, 64U, &key) ==
            MarketDecodeErrorV1::kRangeOverlap,
        "overlapping key strings were accepted");
}

bool TestApplyIdentity() {
    ShanghaiSnapshotV1 snapshot{};
    snapshot.common.market = MarketV1::kShanghai;
    snapshot.common.security_id = "600000";
    snapshot.common.security_id_valid = true;
    snapshot.common.quality_flags =
        QualityBit(QualityFlagV1::kInstrumentUnknown) |
        QualityBit(QualityFlagV1::kQtyUnitUnknown);
    DecodedMarketEventV1 event(std::move(snapshot));

    const std::string_view security_id = "600000";
    ObservedInstrumentIdentityViewV2 identity{};
    identity.key.market = MarketV1::kShanghai;
    identity.key.security_id = std::as_bytes(std::span(security_id));
    identity.instrument_id = 7U;
    identity.ordinal = 6U;
    identity.quantity_unit = QuantityUnitV1::kShare;
    identity.security_type = SecurityTypeV1::kEquity;
    identity.asset_scope = AssetScopeV1::kUnknown;
    if (!Check(
            ApplyObservedInstrumentIdentityV2(identity, &event),
            "identity apply failed")) {
        return false;
    }
    const auto& applied = std::get<ShanghaiSnapshotV1>(event).common;
    return Check(applied.instrument_id == 7U, "instrument id not applied") &&
           Check(applied.ordinal == 6U, "ordinal not applied") &&
           Check(
               (applied.quality_flags &
                QualityBit(QualityFlagV1::kInstrumentUnknown)) == 0U,
               "unknown instrument flag not cleared") &&
           Check(
               (applied.quality_flags &
                QualityBit(QualityFlagV1::kQtyUnitUnknown)) == 0U,
               "unknown unit flag not cleared");
}

}  // namespace

int main() {
    if (!TestAllocationFreeExtraction() || !TestApplyIdentity()) {
        return 1;
    }
    std::cout << "observed instrument router V2 tests passed\n";
    return 0;
}
