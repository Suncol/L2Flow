#include "l2flow/market/instrument_registry_loader_v1.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace l2flow::market {
namespace {

[[nodiscard]] bool DigestNonzero(
    const l2flow::common::Sha256Digest& digest) noexcept {
    return std::any_of(
        digest.begin(), digest.end(),
        [](std::byte value) noexcept { return value != std::byte{0}; });
}

[[nodiscard]] bool ValidName(std::string_view name) noexcept {
    return !name.empty() && name != "." && name != ".." &&
           name.size() <= 255U &&
           name.find('/') == std::string_view::npos &&
           name.find('\0') == std::string_view::npos;
}

[[nodiscard]] InstrumentRegistryFileResultV1 Failure(
    InstrumentRegistryFileErrorV1 error,
    std::size_t line = 0U,
    int system_error_number = 0) noexcept {
    InstrumentRegistryFileResultV1 result{};
    result.error = error;
    result.line = line;
    result.system_error_number = system_error_number;
    return result;
}

void CloseDescriptor(int descriptor) noexcept {
    if (descriptor >= 0) {
        // Retrying close after EINTR can close a reused descriptor on Linux.
        static_cast<void>(::close(descriptor));
    }
}

class FileDescriptor final {
public:
    explicit FileDescriptor(int value = -1) noexcept : value_(value) {}
    ~FileDescriptor() { CloseDescriptor(value_); }
    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;

    [[nodiscard]] int get() const noexcept { return value_; }

private:
    int value_ = -1;
};

[[nodiscard]] bool SameStableFile(
    const struct stat& left,
    const struct stat& right) noexcept {
    return left.st_dev == right.st_dev && left.st_ino == right.st_ino &&
           left.st_size == right.st_size &&
           left.st_mtim.tv_sec == right.st_mtim.tv_sec &&
           left.st_mtim.tv_nsec == right.st_mtim.tv_nsec &&
           left.st_ctim.tv_sec == right.st_ctim.tv_sec &&
           left.st_ctim.tv_nsec == right.st_ctim.tv_nsec;
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
    const auto converted = std::from_chars(
        text.data(), text.data() + text.size(), parsed, 10);
    if (converted.ec != std::errc{} ||
        converted.ptr != text.data() + text.size()) {
        return false;
    }
    *output = parsed;
    return true;
}

[[nodiscard]] int HexDigit(unsigned char value) noexcept {
    if (value >= static_cast<unsigned char>('0') &&
        value <= static_cast<unsigned char>('9')) {
        return static_cast<int>(value - static_cast<unsigned char>('0'));
    }
    if (value >= static_cast<unsigned char>('a') &&
        value <= static_cast<unsigned char>('f')) {
        return static_cast<int>(value - static_cast<unsigned char>('a')) + 10;
    }
    return -1;
}

[[nodiscard]] bool DecodeIdentifier(
    std::string_view text,
    bool allow_empty,
    std::vector<std::byte>* output,
    InstrumentRegistryFileErrorV1* error) {
    if (output == nullptr || error == nullptr) {
        return false;
    }
    output->clear();
    if (allow_empty && text == "-") {
        return true;
    }
    if (text.empty() || text == "-" || (text.size() % 2U) != 0U) {
        *error = InstrumentRegistryFileErrorV1::kInvalidHex;
        return false;
    }
    if (text.size() / 2U > kInstrumentRegistryMaximumIdentifierBytesV1) {
        *error = InstrumentRegistryFileErrorV1::kIdentifierTooLong;
        return false;
    }
    output->reserve(text.size() / 2U);
    for (std::size_t index = 0U; index < text.size(); index += 2U) {
        const int high = HexDigit(static_cast<unsigned char>(text[index]));
        const int low = HexDigit(static_cast<unsigned char>(text[index + 1U]));
        if (high < 0 || low < 0) {
            *error = InstrumentRegistryFileErrorV1::kInvalidHex;
            return false;
        }
        output->push_back(static_cast<std::byte>((high << 4U) | low));
    }
    return true;
}

template <std::size_t Count>
[[nodiscard]] bool SplitExact(
    std::string_view line,
    std::array<std::string_view, Count>* fields) noexcept {
    if (fields == nullptr) {
        return false;
    }
    std::size_t begin = 0U;
    for (std::size_t index = 0U; index < Count; ++index) {
        const std::size_t separator = line.find('\t', begin);
        if (index + 1U == Count) {
            if (separator != std::string_view::npos) {
                return false;
            }
            (*fields)[index] = line.substr(begin);
            return true;
        }
        if (separator == std::string_view::npos) {
            return false;
        }
        (*fields)[index] = line.substr(begin, separator - begin);
        begin = separator + 1U;
    }
    return false;
}

[[nodiscard]] bool ParseMarket(
    std::string_view token,
    MarketV1* output) noexcept {
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
    static constexpr std::array<std::pair<std::string_view, QuantityUnitV1>, 6U>
        values{{
            {"unknown", QuantityUnitV1::kUnknown},
            {"share", QuantityUnitV1::kShare},
            {"fund_unit", QuantityUnitV1::kFundUnit},
            {"lot", QuantityUnitV1::kLot},
            {"bond_piece", QuantityUnitV1::kBondPiece},
            {"index_unit", QuantityUnitV1::kIndexUnit},
        }};
    const auto found = std::find_if(
        values.begin(), values.end(),
        [token](const auto& value) noexcept { return value.first == token; });
    if (found == values.end()) {
        return false;
    }
    *output = found->second;
    return true;
}

[[nodiscard]] bool ParseSecurityType(
    std::string_view token,
    SecurityTypeV1* output) noexcept {
    static constexpr std::array<std::pair<std::string_view, SecurityTypeV1>, 8U>
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
        values.begin(), values.end(),
        [token](const auto& value) noexcept { return value.first == token; });
    if (found == values.end()) {
        return false;
    }
    *output = found->second;
    return true;
}

[[nodiscard]] bool ParseAssetScope(
    std::string_view token,
    AssetScopeV1* output) noexcept {
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

[[nodiscard]] InstrumentRegistryFileResultV1 ParseFile(
    std::string_view bytes,
    const InstrumentRegistryFileOptionsV1& options) {
    if (bytes.back() != '\n') {
        return Failure(InstrumentRegistryFileErrorV1::kMissingFinalNewline);
    }
    if (std::any_of(
            bytes.begin(), bytes.end(), [](unsigned char value) noexcept {
                return value == 0U || value == static_cast<unsigned char>('\r') ||
                       (value < 0x20U && value != static_cast<unsigned char>('\n') &&
                        value != static_cast<unsigned char>('\t')) ||
                       value > 0x7eU;
            })) {
        return Failure(InstrumentRegistryFileErrorV1::kInvalidText);
    }

    std::vector<InstrumentRegistryEntryV1> entries;
    std::size_t line_number = 0U;
    std::size_t begin = 0U;
    while (begin < bytes.size()) {
        const std::size_t end = bytes.find('\n', begin);
        if (end == std::string_view::npos) {
            return Failure(InstrumentRegistryFileErrorV1::kMissingFinalNewline);
        }
        ++line_number;
        const std::string_view line = bytes.substr(begin, end - begin);
        begin = end + 1U;
        if (line.empty()) {
            return Failure(InstrumentRegistryFileErrorV1::kInvalidText, line_number);
        }
        if (line.size() > kInstrumentRegistryFileMaximumLineBytesV1) {
            return Failure(InstrumentRegistryFileErrorV1::kLineTooLong, line_number);
        }

        if (line_number == 1U) {
            std::array<std::string_view, 2U> header{};
            std::uint64_t version = 0U;
            if (!SplitExact(line, &header) ||
                header[0] != kInstrumentRegistryFileMagicV1 ||
                !ParseCanonicalUnsigned(header[1], &version) || version == 0U) {
                return Failure(InstrumentRegistryFileErrorV1::kInvalidHeader, 1U);
            }
            if (version != options.expected_registry_version) {
                return Failure(
                    InstrumentRegistryFileErrorV1::kRegistryVersionMismatch, 1U);
            }
            continue;
        }

        if (entries.size() >= kInstrumentRegistryFileMaximumEntriesV1) {
            return Failure(
                InstrumentRegistryFileErrorV1::kTooManyEntries, line_number);
        }
        std::array<std::string_view, 7U> fields{};
        if (!SplitExact(line, &fields)) {
            return Failure(
                InstrumentRegistryFileErrorV1::kInvalidFieldCount, line_number);
        }

        InstrumentRegistryEntryV1 entry{};
        if (!ParseCanonicalUnsigned(fields[0], &entry.instrument_id) ||
            entry.instrument_id == 0U) {
            return Failure(
                InstrumentRegistryFileErrorV1::kInvalidNumber, line_number);
        }
        if (!ParseMarket(fields[1], &entry.key.market)) {
            return Failure(
                InstrumentRegistryFileErrorV1::kInvalidMarket, line_number);
        }
        InstrumentRegistryFileErrorV1 identifier_error =
            InstrumentRegistryFileErrorV1::kInvalidHex;
        if (!DecodeIdentifier(
                fields[2], true, &entry.key.security_id_source,
                &identifier_error) ||
            !DecodeIdentifier(
                fields[3], false, &entry.key.security_id,
                &identifier_error)) {
            return Failure(identifier_error, line_number);
        }
        if (!ParseQuantityUnit(fields[4], &entry.quantity_unit)) {
            return Failure(
                InstrumentRegistryFileErrorV1::kInvalidQuantityUnit,
                line_number);
        }
        if (!ParseSecurityType(fields[5], &entry.security_type)) {
            return Failure(
                InstrumentRegistryFileErrorV1::kInvalidSecurityType,
                line_number);
        }
        if (!ParseAssetScope(fields[6], &entry.asset_scope)) {
            return Failure(
                InstrumentRegistryFileErrorV1::kInvalidAssetScope,
                line_number);
        }
        entries.push_back(std::move(entry));
    }

    if (line_number == 0U) {
        return Failure(InstrumentRegistryFileErrorV1::kFileEmpty);
    }
    InstrumentRegistryFileResultV1 result{};
    result.registry_error = InstrumentRegistryV1::Create(
        options.expected_registry_version, entries, &result.registry);
    if (result.registry_error != InstrumentRegistryCreateErrorV1::kNone ||
        result.registry == nullptr) {
        result.error = InstrumentRegistryFileErrorV1::kRegistryRejected;
        return result;
    }
    if (result.registry->registry_sha256() !=
        options.expected_registry_sha256) {
        result.registry.reset();
        result.error =
            InstrumentRegistryFileErrorV1::kRegistrySha256Mismatch;
        return result;
    }
    return result;
}

}  // namespace

std::string_view InstrumentRegistryFileErrorNameV1(
    InstrumentRegistryFileErrorV1 error) noexcept {
    switch (error) {
        case InstrumentRegistryFileErrorV1::kNone: return "none";
        case InstrumentRegistryFileErrorV1::kInvalidArgument: return "invalid_argument";
        case InstrumentRegistryFileErrorV1::kInvalidName: return "invalid_name";
        case InstrumentRegistryFileErrorV1::kInvalidDirectory: return "invalid_directory";
        case InstrumentRegistryFileErrorV1::kDirectoryOwnerMismatch: return "directory_owner_mismatch";
        case InstrumentRegistryFileErrorV1::kUnsafeDirectoryMode: return "unsafe_directory_mode";
        case InstrumentRegistryFileErrorV1::kOpenFailed: return "open_failed";
        case InstrumentRegistryFileErrorV1::kSymlinkRejected: return "symlink_rejected";
        case InstrumentRegistryFileErrorV1::kWrongFileType: return "wrong_file_type";
        case InstrumentRegistryFileErrorV1::kFileOwnerMismatch: return "file_owner_mismatch";
        case InstrumentRegistryFileErrorV1::kUnsafeFileMode: return "unsafe_file_mode";
        case InstrumentRegistryFileErrorV1::kWrongLinkCount: return "wrong_link_count";
        case InstrumentRegistryFileErrorV1::kFileEmpty: return "file_empty";
        case InstrumentRegistryFileErrorV1::kFileTooLarge: return "file_too_large";
        case InstrumentRegistryFileErrorV1::kFileChanged: return "file_changed";
        case InstrumentRegistryFileErrorV1::kReadFailed: return "read_failed";
        case InstrumentRegistryFileErrorV1::kMissingFinalNewline: return "missing_final_newline";
        case InstrumentRegistryFileErrorV1::kInvalidText: return "invalid_text";
        case InstrumentRegistryFileErrorV1::kLineTooLong: return "line_too_long";
        case InstrumentRegistryFileErrorV1::kInvalidHeader: return "invalid_header";
        case InstrumentRegistryFileErrorV1::kRegistryVersionMismatch: return "registry_version_mismatch";
        case InstrumentRegistryFileErrorV1::kInvalidFieldCount: return "invalid_field_count";
        case InstrumentRegistryFileErrorV1::kInvalidNumber: return "invalid_number";
        case InstrumentRegistryFileErrorV1::kInvalidMarket: return "invalid_market";
        case InstrumentRegistryFileErrorV1::kInvalidHex: return "invalid_hex";
        case InstrumentRegistryFileErrorV1::kIdentifierTooLong: return "identifier_too_long";
        case InstrumentRegistryFileErrorV1::kInvalidQuantityUnit: return "invalid_quantity_unit";
        case InstrumentRegistryFileErrorV1::kInvalidSecurityType: return "invalid_security_type";
        case InstrumentRegistryFileErrorV1::kInvalidAssetScope: return "invalid_asset_scope";
        case InstrumentRegistryFileErrorV1::kTooManyEntries: return "too_many_entries";
        case InstrumentRegistryFileErrorV1::kRegistryRejected: return "registry_rejected";
        case InstrumentRegistryFileErrorV1::kRegistrySha256Mismatch: return "registry_sha256_mismatch";
        case InstrumentRegistryFileErrorV1::kResourceExhausted: return "resource_exhausted";
    }
    return "unknown";
}

InstrumentRegistryFileResultV1 LoadInstrumentRegistryFileV1(
    const InstrumentRegistryFileOptionsV1& options) noexcept {
    try {
        if (options.directory_fd < 0 ||
            options.expected_registry_version == 0U ||
            !DigestNonzero(options.expected_registry_sha256)) {
            return Failure(InstrumentRegistryFileErrorV1::kInvalidArgument);
        }
        if (!ValidName(options.file_name)) {
            return Failure(InstrumentRegistryFileErrorV1::kInvalidName);
        }

        struct stat directory_status {};
        if (::fstat(options.directory_fd, &directory_status) != 0) {
            return Failure(
                InstrumentRegistryFileErrorV1::kInvalidDirectory, 0U, errno);
        }
        if (!S_ISDIR(directory_status.st_mode)) {
            return Failure(InstrumentRegistryFileErrorV1::kInvalidDirectory);
        }
        if (static_cast<std::uint64_t>(directory_status.st_uid) !=
            static_cast<std::uint64_t>(options.expected_owner_uid)) {
            return Failure(
                InstrumentRegistryFileErrorV1::kDirectoryOwnerMismatch);
        }
        if ((directory_status.st_mode & (S_IWGRP | S_IWOTH)) != 0) {
            return Failure(InstrumentRegistryFileErrorV1::kUnsafeDirectoryMode);
        }

        const std::string name(options.file_name);
        int descriptor = -1;
        do {
            descriptor = ::openat(
                options.directory_fd, name.c_str(),
                O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
        } while (descriptor < 0 && errno == EINTR);
        if (descriptor < 0) {
            return Failure(
                errno == ELOOP
                    ? InstrumentRegistryFileErrorV1::kSymlinkRejected
                    : InstrumentRegistryFileErrorV1::kOpenFailed,
                0U, errno);
        }
        FileDescriptor file(descriptor);

        struct stat before {};
        if (::fstat(file.get(), &before) != 0) {
            return Failure(
                InstrumentRegistryFileErrorV1::kOpenFailed, 0U, errno);
        }
        if (!S_ISREG(before.st_mode)) {
            return Failure(InstrumentRegistryFileErrorV1::kWrongFileType);
        }
        if (static_cast<std::uint64_t>(before.st_uid) !=
            static_cast<std::uint64_t>(options.expected_owner_uid)) {
            return Failure(InstrumentRegistryFileErrorV1::kFileOwnerMismatch);
        }
        const mode_t permissions = before.st_mode & 07777U;
        if (permissions != static_cast<mode_t>(0400U) &&
            permissions != static_cast<mode_t>(0600U)) {
            return Failure(InstrumentRegistryFileErrorV1::kUnsafeFileMode);
        }
        if (before.st_nlink != 1) {
            return Failure(InstrumentRegistryFileErrorV1::kWrongLinkCount);
        }
        if (before.st_size <= 0) {
            return Failure(InstrumentRegistryFileErrorV1::kFileEmpty);
        }
        if (static_cast<std::uintmax_t>(before.st_size) >
            kInstrumentRegistryFileMaximumBytesV1) {
            return Failure(InstrumentRegistryFileErrorV1::kFileTooLarge);
        }

        std::string bytes(static_cast<std::size_t>(before.st_size), '\0');
        std::size_t offset = 0U;
        while (offset < bytes.size()) {
            const ssize_t read_bytes = ::pread(
                file.get(), bytes.data() + offset, bytes.size() - offset,
                static_cast<off_t>(offset));
            if (read_bytes < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return Failure(
                    InstrumentRegistryFileErrorV1::kReadFailed, 0U, errno);
            }
            if (read_bytes == 0) {
                return Failure(InstrumentRegistryFileErrorV1::kFileChanged);
            }
            offset += static_cast<std::size_t>(read_bytes);
        }

        struct stat after {};
        if (::fstat(file.get(), &after) != 0) {
            return Failure(
                InstrumentRegistryFileErrorV1::kReadFailed, 0U, errno);
        }
        if (!SameStableFile(before, after)) {
            return Failure(InstrumentRegistryFileErrorV1::kFileChanged);
        }
        return ParseFile(bytes, options);
    } catch (const std::bad_alloc&) {
        return Failure(InstrumentRegistryFileErrorV1::kResourceExhausted);
    } catch (const std::length_error&) {
        return Failure(InstrumentRegistryFileErrorV1::kResourceExhausted);
    } catch (...) {
        return Failure(InstrumentRegistryFileErrorV1::kInvalidArgument);
    }
}

}  // namespace l2flow::market
