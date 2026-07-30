#pragma once

#include "l2flow/market/daily_instrument_catalog_v2.h"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string_view>

namespace l2flow::market {

inline constexpr std::string_view kDailyInstrumentCatalogFileMagicV2 =
    "L2FLOW_DAILY_INSTRUMENT_CATALOG_V2";
inline constexpr std::uintmax_t kDailyInstrumentCatalogMaximumFileBytesV2 =
    64ULL * 1024ULL * 1024ULL;
inline constexpr std::size_t kDailyInstrumentCatalogMaximumLineBytesV2 =
    4096U;
inline constexpr std::size_t kDailyInstrumentCatalogMaximumSourceRowsV2 =
    1'000'000U;
inline constexpr std::size_t
    kDailyInstrumentCatalogMaximumIdentifierBytesV2 = 512U;

// Canonical ASCII/LF-only format:
//
//   L2FLOW_DAILY_INSTRUMENT_CATALOG_V2<TAB>trade_date<TAB>
//       catalog_version<TAB>sh+sz<TAB>complete<LF>
//   market<TAB>security_id_source_hex_or_-<TAB>security_id_hex<TAB>
//       quantity_unit<TAB>security_type<TAB>asset_scope<TAB>
//       external_instrument_id_hex_or_-<LF>
//
// market is "sh" or "sz". Enum tokens match their public names without the
// leading k and use lowercase snake_case. Hex is lowercase, even-length and
// represents the exact printable-ASCII SDK bytes; no text normalization is
// performed. Shanghai source is "-", while Shenzhen source is nonempty.
struct DailyInstrumentCatalogFileOptionsV2 final {
    std::filesystem::path path;
    std::uint32_t expected_trade_date = 0U;
    std::uint64_t expected_catalog_version = 0U;
    std::uint64_t session_epoch = 0U;
};

enum class DailyInstrumentCatalogFileErrorV2 : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kPathNotAbsolute,
    kSymlinkRejected,
    kWrongFileType,
    kFileEmpty,
    kFileTooLarge,
    kOpenFailed,
    kReadFailed,
    kFileChanged,
    kMissingFinalNewline,
    kInvalidText,
    kLineTooLong,
    kInvalidHeader,
    kTradeDateMismatch,
    kCatalogVersionMismatch,
    kInvalidFieldCount,
    kInvalidMarket,
    kInvalidHex,
    kIdentifierTooLong,
    kInvalidQuantityUnit,
    kInvalidSecurityType,
    kInvalidAssetScope,
    kTooManyRows,
    kCatalogRejected,
    kResourceExhausted,
    kUnexpectedFailure,
};

[[nodiscard]] std::string_view DailyInstrumentCatalogFileErrorNameV2(
    DailyInstrumentCatalogFileErrorV2 error) noexcept;

struct DailyInstrumentCatalogFileResultV2 final {
    DailyInstrumentCatalogFileErrorV2 error =
        DailyInstrumentCatalogFileErrorV2::kNone;
    DailyInstrumentCatalogCreateErrorV2 catalog_error =
        DailyInstrumentCatalogCreateErrorV2::kNone;
    std::size_t line = 0U;
    std::size_t source_row_count = 0U;
    std::unique_ptr<DailyInstrumentCatalogV2> catalog;

    [[nodiscard]] bool ok() const noexcept {
        return error == DailyInstrumentCatalogFileErrorV2::kNone &&
               catalog_error ==
                   DailyInstrumentCatalogCreateErrorV2::kNone &&
               catalog != nullptr;
    }
};

[[nodiscard]] DailyInstrumentCatalogFileResultV2
LoadDailyInstrumentCatalogFileV2(
    const DailyInstrumentCatalogFileOptionsV2& options) noexcept;

}  // namespace l2flow::market
