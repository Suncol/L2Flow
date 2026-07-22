#pragma once

#include "l2flow/common/sha256.h"
#include "l2flow/market/instrument_registry.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

namespace l2flow::market {

inline constexpr std::string_view kInstrumentRegistryFileMagicV1 =
    "L2FLOW_INSTRUMENT_REGISTRY_V1";
inline constexpr std::size_t kInstrumentRegistryFileMaximumBytesV1 =
    64U * 1024U * 1024U;
inline constexpr std::size_t kInstrumentRegistryFileMaximumLineBytesV1 =
    4096U;
inline constexpr std::size_t kInstrumentRegistryFileMaximumEntriesV1 =
    1'000'000U;
inline constexpr std::size_t kInstrumentRegistryMaximumIdentifierBytesV1 =
    512U;

// Canonical UTF-8/ASCII wire text (LF only):
//
//   L2FLOW_INSTRUMENT_REGISTRY_V1<TAB>registry_version<LF>
//   instrument_id<TAB>market<TAB>security_id_source_hex_or_-<TAB>
//       security_id_hex<TAB>quantity_unit<TAB>security_type<TAB>
//       asset_scope<LF>
//
// Every number is minimal unsigned decimal; opaque identifiers use lowercase
// even-length hexadecimal ("-" is the sole spelling of an empty source).
// There are no comments, blank lines, quoting, escaping, CR bytes or optional
// columns.  This deliberately keeps deployment parsing independent of locale
// and preserves the exact opaque identifier bytes used by the decoder.
struct InstrumentRegistryFileOptionsV1 final {
    // Borrowed for this call.  The loader opens exactly one non-symlink file
    // component below this retained directory.
    int directory_fd = -1;
    std::string_view file_name{};
    std::uint32_t expected_owner_uid = 0U;
    // Both identities are mandatory deployment pins.  The version must match
    // the file header; the digest is compared with InstrumentRegistryV1's
    // canonical, input-order-independent registry digest.
    std::uint64_t expected_registry_version = 0U;
    l2flow::common::Sha256Digest expected_registry_sha256{};
};

enum class InstrumentRegistryFileErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kInvalidName,
    kInvalidDirectory,
    kDirectoryOwnerMismatch,
    kUnsafeDirectoryMode,
    kOpenFailed,
    kSymlinkRejected,
    kWrongFileType,
    kFileOwnerMismatch,
    kUnsafeFileMode,
    kWrongLinkCount,
    kFileEmpty,
    kFileTooLarge,
    kFileChanged,
    kReadFailed,
    kMissingFinalNewline,
    kInvalidText,
    kLineTooLong,
    kInvalidHeader,
    kRegistryVersionMismatch,
    kInvalidFieldCount,
    kInvalidNumber,
    kInvalidMarket,
    kInvalidHex,
    kIdentifierTooLong,
    kInvalidQuantityUnit,
    kInvalidSecurityType,
    kInvalidAssetScope,
    kTooManyEntries,
    kRegistryRejected,
    kRegistrySha256Mismatch,
    kResourceExhausted,
};

[[nodiscard]] std::string_view InstrumentRegistryFileErrorNameV1(
    InstrumentRegistryFileErrorV1 error) noexcept;

struct InstrumentRegistryFileResultV1 final {
    InstrumentRegistryFileErrorV1 error =
        InstrumentRegistryFileErrorV1::kNone;
    InstrumentRegistryCreateErrorV1 registry_error =
        InstrumentRegistryCreateErrorV1::kNone;
    int system_error_number = 0;
    // One-based.  Zero denotes a file/identity error not attributable to one
    // line, or a duplicate rejected only after canonical registry creation.
    std::size_t line = 0U;
    std::unique_ptr<InstrumentRegistryV1> registry;

    [[nodiscard]] bool ok() const noexcept {
        return error == InstrumentRegistryFileErrorV1::kNone &&
               registry_error == InstrumentRegistryCreateErrorV1::kNone &&
               registry != nullptr;
    }
};

[[nodiscard]] InstrumentRegistryFileResultV1
LoadInstrumentRegistryFileV1(
    const InstrumentRegistryFileOptionsV1& options) noexcept;

}  // namespace l2flow::market
