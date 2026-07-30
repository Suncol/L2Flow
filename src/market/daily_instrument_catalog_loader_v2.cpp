#include "l2flow/market/daily_instrument_catalog_loader_v2.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <limits>
#include <new>
#include <span>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace l2flow::market {
namespace {

class ScopedFd final {
public:
    explicit ScopedFd(int value = -1) noexcept : value_(value) {}
    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;
    ~ScopedFd() {
        if (value_ >= 0) {
            static_cast<void>(::close(value_));
        }
    }

    [[nodiscard]] int get() const noexcept { return value_; }

private:
    int value_ = -1;
};

[[nodiscard]] bool SameFileSnapshot(
    const struct stat& left,
    const struct stat& right) noexcept {
    return left.st_dev == right.st_dev &&
           left.st_ino == right.st_ino &&
           left.st_mode == right.st_mode &&
           left.st_size == right.st_size &&
           left.st_mtim.tv_sec == right.st_mtim.tv_sec &&
           left.st_mtim.tv_nsec == right.st_mtim.tv_nsec &&
           left.st_ctim.tv_sec == right.st_ctim.tv_sec &&
           left.st_ctim.tv_nsec == right.st_ctim.tv_nsec;
}

[[nodiscard]] bool ReadExactFile(
    int descriptor,
    std::span<char> output) noexcept {
    std::size_t offset = 0U;
    while (offset < output.size()) {
        const ssize_t read_bytes = ::read(
            descriptor,
            output.data() + offset,
            output.size() - offset);
        if (read_bytes > 0) {
            offset += static_cast<std::size_t>(read_bytes);
            continue;
        }
        if (read_bytes < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

[[nodiscard]] DailyInstrumentCatalogFileResultV2 Failure(
    DailyInstrumentCatalogFileErrorV2 error,
    std::size_t line = 0U) noexcept {
    DailyInstrumentCatalogFileResultV2 result{};
    result.error = error;
    result.line = line;
    return result;
}

template <typename Integer>
[[nodiscard]] bool ParseCanonicalUnsigned(
    std::string_view text,
    Integer* output) noexcept {
    static_assert(std::is_unsigned_v<Integer>);
    if (output == nullptr || text.empty() ||
        (text.size() > 1U && text.front() == '0')) {
        return false;
    }
    Integer parsed = 0U;
    const auto conversion = std::from_chars(
        text.data(), text.data() + text.size(), parsed, 10);
    if (conversion.ec != std::errc{} ||
        conversion.ptr != text.data() + text.size()) {
        return false;
    }
    *output = parsed;
    return true;
}

template <std::size_t Count>
[[nodiscard]] bool SplitExact(
    std::string_view line,
    std::array<std::string_view, Count>* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    std::size_t begin = 0U;
    for (std::size_t index = 0U; index < Count; ++index) {
        const std::size_t separator = line.find('\t', begin);
        if (index + 1U == Count) {
            if (separator != std::string_view::npos) {
                return false;
            }
            (*output)[index] = line.substr(begin);
            return true;
        }
        if (separator == std::string_view::npos) {
            return false;
        }
        (*output)[index] =
            line.substr(begin, separator - begin);
        begin = separator + 1U;
    }
    return false;
}

[[nodiscard]] int HexDigit(unsigned char value) noexcept {
    if (value >= static_cast<unsigned char>('0') &&
        value <= static_cast<unsigned char>('9')) {
        return static_cast<int>(
            value - static_cast<unsigned char>('0'));
    }
    if (value >= static_cast<unsigned char>('a') &&
        value <= static_cast<unsigned char>('f')) {
        return static_cast<int>(
                   value - static_cast<unsigned char>('a')) +
               10;
    }
    return -1;
}

[[nodiscard]] bool DecodeIdentifier(
    std::string_view text,
    bool allow_empty,
    std::vector<std::byte>* output,
    DailyInstrumentCatalogFileErrorV2* error) {
    if (output == nullptr || error == nullptr) {
        return false;
    }
    output->clear();
    if (allow_empty && text == "-") {
        return true;
    }
    if (text.empty() || text == "-" || text.size() % 2U != 0U) {
        *error = DailyInstrumentCatalogFileErrorV2::kInvalidHex;
        return false;
    }
    if (text.size() / 2U >
        kDailyInstrumentCatalogMaximumIdentifierBytesV2) {
        *error =
            DailyInstrumentCatalogFileErrorV2::kIdentifierTooLong;
        return false;
    }
    output->reserve(text.size() / 2U);
    for (std::size_t index = 0U; index < text.size(); index += 2U) {
        const int high =
            HexDigit(static_cast<unsigned char>(text[index]));
        const int low =
            HexDigit(static_cast<unsigned char>(text[index + 1U]));
        if (high < 0 || low < 0) {
            *error = DailyInstrumentCatalogFileErrorV2::kInvalidHex;
            return false;
        }
        output->push_back(
            static_cast<std::byte>((high << 4U) | low));
    }
    return true;
}

[[nodiscard]] bool ParseMarket(
    std::string_view token,
    MarketV1* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    if (token == "sh") {
        *output = MarketV1::kShanghai;
        return true;
    }
    if (token == "sz") {
        *output = MarketV1::kShenzhen;
        return true;
    }
    return false;
}

[[nodiscard]] bool ParseQuantityUnit(
    std::string_view token,
    QuantityUnitV1* output) noexcept {
    constexpr std::array<std::pair<std::string_view, QuantityUnitV1>, 6U>
        values{{
            {"unknown", QuantityUnitV1::kUnknown},
            {"share", QuantityUnitV1::kShare},
            {"fund_unit", QuantityUnitV1::kFundUnit},
            {"lot", QuantityUnitV1::kLot},
            {"bond_piece", QuantityUnitV1::kBondPiece},
            {"index_unit", QuantityUnitV1::kIndexUnit},
        }};
    const auto found = std::find_if(
        values.begin(),
        values.end(),
        [token](const auto& value) noexcept {
            return value.first == token;
        });
    if (found == values.end() || output == nullptr) {
        return false;
    }
    *output = found->second;
    return true;
}

[[nodiscard]] bool ParseSecurityType(
    std::string_view token,
    SecurityTypeV1* output) noexcept {
    constexpr std::array<std::pair<std::string_view, SecurityTypeV1>, 8U>
        values{{
            {"unknown", SecurityTypeV1::kUnknown},
            {"equity", SecurityTypeV1::kEquity},
            {"fund", SecurityTypeV1::kFund},
            {"bond", SecurityTypeV1::kBond},
            {"convertible_bond", SecurityTypeV1::kConvertibleBond},
            {"index", SecurityTypeV1::kIndex},
            {"warrant", SecurityTypeV1::kWarrant},
            {"option", SecurityTypeV1::kOption},
        }};
    const auto found = std::find_if(
        values.begin(),
        values.end(),
        [token](const auto& value) noexcept {
            return value.first == token;
        });
    if (found == values.end() || output == nullptr) {
        return false;
    }
    *output = found->second;
    return true;
}

[[nodiscard]] bool ParseAssetScope(
    std::string_view token,
    AssetScopeV1* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    if (token == "unknown") {
        *output = AssetScopeV1::kUnknown;
        return true;
    }
    if (token == "documented_core") {
        *output = AssetScopeV1::kDocumentedCore;
        return true;
    }
    if (token == "outside_documented_core") {
        *output = AssetScopeV1::kOutsideDocumentedCore;
        return true;
    }
    return false;
}

[[nodiscard]] DailyInstrumentCatalogFileResultV2 ParseFile(
    std::string_view bytes,
    const DailyInstrumentCatalogFileOptionsV2& options) {
    if (bytes.empty()) {
        return Failure(DailyInstrumentCatalogFileErrorV2::kFileEmpty);
    }
    if (bytes.back() != '\n') {
        return Failure(
            DailyInstrumentCatalogFileErrorV2::kMissingFinalNewline);
    }
    if (std::any_of(
            bytes.begin(),
            bytes.end(),
            [](unsigned char value) noexcept {
                return value == 0U ||
                       value == static_cast<unsigned char>('\r') ||
                       (value < 0x20U &&
                        value != static_cast<unsigned char>('\n') &&
                        value != static_cast<unsigned char>('\t')) ||
                       value > 0x7eU;
            })) {
        return Failure(DailyInstrumentCatalogFileErrorV2::kInvalidText);
    }

    std::vector<DailyInstrumentSourceEntryV2> entries;
    std::size_t line_number = 0U;
    std::size_t begin = 0U;
    while (begin < bytes.size()) {
        const std::size_t end = bytes.find('\n', begin);
        if (end == std::string_view::npos) {
            return Failure(
                DailyInstrumentCatalogFileErrorV2::
                    kMissingFinalNewline);
        }
        ++line_number;
        const std::string_view line =
            bytes.substr(begin, end - begin);
        begin = end + 1U;
        if (line.empty()) {
            return Failure(
                DailyInstrumentCatalogFileErrorV2::kInvalidText,
                line_number);
        }
        if (line.size() >
            kDailyInstrumentCatalogMaximumLineBytesV2) {
            return Failure(
                DailyInstrumentCatalogFileErrorV2::kLineTooLong,
                line_number);
        }

        if (line_number == 1U) {
            std::array<std::string_view, 5U> fields{};
            std::uint32_t trade_date = 0U;
            std::uint64_t version = 0U;
            if (!SplitExact(line, &fields) ||
                fields[0] != kDailyInstrumentCatalogFileMagicV2 ||
                !ParseCanonicalUnsigned(fields[1], &trade_date) ||
                !ParseCanonicalUnsigned(fields[2], &version) ||
                fields[3] != "sh+sz" ||
                fields[4] != "complete") {
                return Failure(
                    DailyInstrumentCatalogFileErrorV2::kInvalidHeader,
                    1U);
            }
            if (trade_date != options.expected_trade_date) {
                return Failure(
                    DailyInstrumentCatalogFileErrorV2::
                        kTradeDateMismatch,
                    1U);
            }
            if (version != options.expected_catalog_version) {
                return Failure(
                    DailyInstrumentCatalogFileErrorV2::
                        kCatalogVersionMismatch,
                    1U);
            }
            continue;
        }

        if (entries.size() >=
            kDailyInstrumentCatalogMaximumSourceRowsV2) {
            return Failure(
                DailyInstrumentCatalogFileErrorV2::kTooManyRows,
                line_number);
        }
        std::array<std::string_view, 7U> fields{};
        if (!SplitExact(line, &fields)) {
            return Failure(
                DailyInstrumentCatalogFileErrorV2::kInvalidFieldCount,
                line_number);
        }
        DailyInstrumentSourceEntryV2 entry{};
        if (!ParseMarket(fields[0], &entry.key.market)) {
            return Failure(
                DailyInstrumentCatalogFileErrorV2::kInvalidMarket,
                line_number);
        }
        DailyInstrumentCatalogFileErrorV2 identifier_error =
            DailyInstrumentCatalogFileErrorV2::kInvalidHex;
        if (!DecodeIdentifier(
                fields[1],
                true,
                &entry.key.security_id_source,
                &identifier_error) ||
            !DecodeIdentifier(
                fields[2],
                false,
                &entry.key.security_id,
                &identifier_error) ||
            !DecodeIdentifier(
                fields[6],
                true,
                &entry.external_instrument_id,
                &identifier_error)) {
            return Failure(identifier_error, line_number);
        }
        if (!ParseQuantityUnit(
                fields[3], &entry.metadata.quantity_unit)) {
            return Failure(
                DailyInstrumentCatalogFileErrorV2::
                    kInvalidQuantityUnit,
                line_number);
        }
        if (!ParseSecurityType(
                fields[4], &entry.metadata.security_type)) {
            return Failure(
                DailyInstrumentCatalogFileErrorV2::
                    kInvalidSecurityType,
                line_number);
        }
        if (!ParseAssetScope(
                fields[5], &entry.metadata.asset_scope)) {
            return Failure(
                DailyInstrumentCatalogFileErrorV2::
                    kInvalidAssetScope,
                line_number);
        }
        entries.push_back(std::move(entry));
    }
    if (line_number == 1U) {
        return Failure(
            DailyInstrumentCatalogFileErrorV2::kCatalogRejected,
            1U);
    }

    DailyInstrumentCatalogConfigV2 config{};
    config.trade_date = options.expected_trade_date;
    config.catalog_version = options.expected_catalog_version;
    config.session_epoch = options.session_epoch;
    config.market_scope = kDailyCatalogMainlandScopeV2;
    config.coverage_complete = true;
    DailyInstrumentCatalogFileResultV2 result{};
    result.source_row_count = entries.size();
    result.catalog_error = DailyInstrumentCatalogV2::Create(
        config, entries, &result.catalog);
    if (result.catalog_error !=
            DailyInstrumentCatalogCreateErrorV2::kNone ||
        result.catalog == nullptr) {
        result.error =
            DailyInstrumentCatalogFileErrorV2::kCatalogRejected;
    }
    return result;
}

}  // namespace

std::string_view DailyInstrumentCatalogFileErrorNameV2(
    DailyInstrumentCatalogFileErrorV2 error) noexcept {
    switch (error) {
        case DailyInstrumentCatalogFileErrorV2::kNone:
            return "none";
        case DailyInstrumentCatalogFileErrorV2::kInvalidArgument:
            return "invalid_argument";
        case DailyInstrumentCatalogFileErrorV2::kPathNotAbsolute:
            return "path_not_absolute";
        case DailyInstrumentCatalogFileErrorV2::kSymlinkRejected:
            return "symlink_rejected";
        case DailyInstrumentCatalogFileErrorV2::kWrongFileType:
            return "wrong_file_type";
        case DailyInstrumentCatalogFileErrorV2::kFileEmpty:
            return "file_empty";
        case DailyInstrumentCatalogFileErrorV2::kFileTooLarge:
            return "file_too_large";
        case DailyInstrumentCatalogFileErrorV2::kOpenFailed:
            return "open_failed";
        case DailyInstrumentCatalogFileErrorV2::kReadFailed:
            return "read_failed";
        case DailyInstrumentCatalogFileErrorV2::kFileChanged:
            return "file_changed";
        case DailyInstrumentCatalogFileErrorV2::kMissingFinalNewline:
            return "missing_final_newline";
        case DailyInstrumentCatalogFileErrorV2::kInvalidText:
            return "invalid_text";
        case DailyInstrumentCatalogFileErrorV2::kLineTooLong:
            return "line_too_long";
        case DailyInstrumentCatalogFileErrorV2::kInvalidHeader:
            return "invalid_header";
        case DailyInstrumentCatalogFileErrorV2::kTradeDateMismatch:
            return "trade_date_mismatch";
        case DailyInstrumentCatalogFileErrorV2::kCatalogVersionMismatch:
            return "catalog_version_mismatch";
        case DailyInstrumentCatalogFileErrorV2::kInvalidFieldCount:
            return "invalid_field_count";
        case DailyInstrumentCatalogFileErrorV2::kInvalidMarket:
            return "invalid_market";
        case DailyInstrumentCatalogFileErrorV2::kInvalidHex:
            return "invalid_hex";
        case DailyInstrumentCatalogFileErrorV2::kIdentifierTooLong:
            return "identifier_too_long";
        case DailyInstrumentCatalogFileErrorV2::kInvalidQuantityUnit:
            return "invalid_quantity_unit";
        case DailyInstrumentCatalogFileErrorV2::kInvalidSecurityType:
            return "invalid_security_type";
        case DailyInstrumentCatalogFileErrorV2::kInvalidAssetScope:
            return "invalid_asset_scope";
        case DailyInstrumentCatalogFileErrorV2::kTooManyRows:
            return "too_many_rows";
        case DailyInstrumentCatalogFileErrorV2::kCatalogRejected:
            return "catalog_rejected";
        case DailyInstrumentCatalogFileErrorV2::kResourceExhausted:
            return "resource_exhausted";
        case DailyInstrumentCatalogFileErrorV2::kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

DailyInstrumentCatalogFileResultV2
LoadDailyInstrumentCatalogFileV2(
    const DailyInstrumentCatalogFileOptionsV2& options) noexcept {
    try {
        if (options.path.empty() ||
            options.expected_trade_date == 0U ||
            options.expected_catalog_version == 0U ||
            options.session_epoch == 0U) {
            return Failure(
                DailyInstrumentCatalogFileErrorV2::kInvalidArgument);
        }
        if (!options.path.is_absolute()) {
            return Failure(
                DailyInstrumentCatalogFileErrorV2::kPathNotAbsolute);
        }
        struct stat path_before {};
        if (::lstat(options.path.c_str(), &path_before) != 0) {
            return Failure(
                DailyInstrumentCatalogFileErrorV2::kOpenFailed);
        }
        if (S_ISLNK(path_before.st_mode)) {
            return Failure(
                DailyInstrumentCatalogFileErrorV2::kSymlinkRejected);
        }
        if (!S_ISREG(path_before.st_mode)) {
            return Failure(
                DailyInstrumentCatalogFileErrorV2::kWrongFileType);
        }
        const ScopedFd input(::open(
            options.path.c_str(),
            O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
        if (input.get() < 0) {
            return Failure(
                errno == ELOOP
                    ? DailyInstrumentCatalogFileErrorV2::
                          kSymlinkRejected
                    : DailyInstrumentCatalogFileErrorV2::kOpenFailed);
        }
        struct stat opened_before {};
        if (::fstat(input.get(), &opened_before) != 0) {
            return Failure(
                DailyInstrumentCatalogFileErrorV2::kReadFailed);
        }
        if (!SameFileSnapshot(path_before, opened_before)) {
            return Failure(
                DailyInstrumentCatalogFileErrorV2::kFileChanged);
        }
        if (!S_ISREG(opened_before.st_mode)) {
            return Failure(
                DailyInstrumentCatalogFileErrorV2::kWrongFileType);
        }
        if (opened_before.st_size == 0) {
            return Failure(
                DailyInstrumentCatalogFileErrorV2::kFileEmpty);
        }
        if (opened_before.st_size < 0 ||
            static_cast<std::uintmax_t>(opened_before.st_size) >
                kDailyInstrumentCatalogMaximumFileBytesV2) {
            return Failure(
                DailyInstrumentCatalogFileErrorV2::kFileTooLarge);
        }
        const std::size_t size_before =
            static_cast<std::size_t>(opened_before.st_size);
        std::string bytes(
            size_before, '\0');
        if (!ReadExactFile(input.get(), bytes)) {
            return Failure(
                DailyInstrumentCatalogFileErrorV2::kReadFailed);
        }
        char extra = '\0';
        ssize_t extra_bytes = -1;
        do {
            extra_bytes = ::read(input.get(), &extra, 1U);
        } while (extra_bytes < 0 && errno == EINTR);
        if (extra_bytes != 0) {
            return Failure(
                extra_bytes > 0
                    ? DailyInstrumentCatalogFileErrorV2::kFileChanged
                    : DailyInstrumentCatalogFileErrorV2::kReadFailed);
        }
        struct stat opened_after {};
        struct stat path_after {};
        if (::fstat(input.get(), &opened_after) != 0 ||
            ::lstat(options.path.c_str(), &path_after) != 0 ||
            !SameFileSnapshot(opened_before, opened_after) ||
            !SameFileSnapshot(opened_after, path_after)) {
            return Failure(
                DailyInstrumentCatalogFileErrorV2::kFileChanged);
        }
        return ParseFile(bytes, options);
    } catch (const std::bad_alloc&) {
        return Failure(
            DailyInstrumentCatalogFileErrorV2::kResourceExhausted);
    } catch (const std::length_error&) {
        return Failure(
            DailyInstrumentCatalogFileErrorV2::kResourceExhausted);
    } catch (...) {
        return Failure(
            DailyInstrumentCatalogFileErrorV2::kUnexpectedFailure);
    }
}

}  // namespace l2flow::market
