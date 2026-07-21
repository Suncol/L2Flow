#include "l2flow/ingress/raw_manifest_store.h"

#include "l2flow/common/identity128.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include <fcntl.h>
#include <linux/fs.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace l2flow::ingress {
namespace {

class ScopedFd final {
public:
    explicit ScopedFd(int fd = -1) noexcept : fd_(fd) {}
    ~ScopedFd() {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
    }

    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;

    ScopedFd(ScopedFd&& other) noexcept
        : fd_(std::exchange(other.fd_, -1)) {}
    ScopedFd& operator=(ScopedFd&& other) noexcept {
        if (this != &other) {
            Reset(std::exchange(other.fd_, -1));
        }
        return *this;
    }

    [[nodiscard]] int get() const noexcept {
        return fd_;
    }

    void Reset(int fd = -1) noexcept {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
        fd_ = fd;
    }

private:
    int fd_ = -1;
};

void SetError(
    std::string* error,
    std::string message) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        *error = std::move(message);
    } catch (...) {
    }
}

[[nodiscard]] bool SameNamespace(
    const RawManifestNamespaceV1& left,
    const RawManifestNamespaceV1& right) noexcept {
    return left.capture_date == right.capture_date &&
           left.source_stream_id == right.source_stream_id &&
           left.stream_day_id == right.stream_day_id;
}

[[nodiscard]] bool IsExpectedNamespaceValid(
    const RawManifestNamespaceV1& value) noexcept {
    return value.capture_date != 0U &&
           value.source_stream_id != 0U &&
           !l2flow::common::IsZeroIdentity(value.stream_day_id);
}

[[nodiscard]] bool IsValidUtf8(std::string_view text) noexcept {
    std::size_t index = 0U;
    while (index < text.size()) {
        const std::uint8_t first =
            static_cast<std::uint8_t>(
                static_cast<unsigned char>(text[index]));
        if (first <= 0x7fU) {
            ++index;
            continue;
        }

        std::size_t continuation_count = 0U;
        std::uint8_t second_min = 0x80U;
        std::uint8_t second_max = 0xbfU;
        if (first >= 0xc2U && first <= 0xdfU) {
            continuation_count = 1U;
        } else if (first == 0xe0U) {
            continuation_count = 2U;
            second_min = 0xa0U;
        } else if (first >= 0xe1U && first <= 0xecU) {
            continuation_count = 2U;
        } else if (first == 0xedU) {
            continuation_count = 2U;
            second_max = 0x9fU;
        } else if (first >= 0xeeU && first <= 0xefU) {
            continuation_count = 2U;
        } else if (first == 0xf0U) {
            continuation_count = 3U;
            second_min = 0x90U;
        } else if (first >= 0xf1U && first <= 0xf3U) {
            continuation_count = 3U;
        } else if (first == 0xf4U) {
            continuation_count = 3U;
            second_max = 0x8fU;
        } else {
            return false;
        }
        if (continuation_count > text.size() - index - 1U) {
            return false;
        }
        const std::uint8_t second =
            static_cast<std::uint8_t>(
                static_cast<unsigned char>(text[index + 1U]));
        if (second < second_min || second > second_max) {
            return false;
        }
        for (std::size_t offset = 2U;
             offset <= continuation_count;
             ++offset) {
            const std::uint8_t continuation =
                static_cast<std::uint8_t>(
                    static_cast<unsigned char>(
                        text[index + offset]));
            if (continuation < 0x80U ||
                continuation > 0xbfU) {
                return false;
            }
        }
        index += continuation_count + 1U;
    }
    return true;
}

[[nodiscard]] int HexDigit(char character) noexcept {
    if (character >= '0' && character <= '9') {
        return character - '0';
    }
    if (character >= 'a' && character <= 'f') {
        return character - 'a' + 10;
    }
    if (character >= 'A' && character <= 'F') {
        return character - 'A' + 10;
    }
    return -1;
}

class JsonParser final {
public:
    explicit JsonParser(std::string_view input) noexcept
        : input_(input) {}

    [[nodiscard]] bool ParseManifest(
        RawManifestV1* output) {
        RawManifestV1 value{};
        if (!ConsumeLiteral("{\"closed_entries\":") ||
            !ParseClosedEntries(&value.closed_entries) ||
            !ConsumeLiteral(",\"closed_entry_count\":") ||
            !ParseU64String(&value.closed_entry_count) ||
            !ConsumeLiteral(",\"closed_prefix_sha256\":") ||
            !ParseHex(&value.closed_prefix_sha256) ||
            !ConsumeLiteral(",\"manifest_generation\":") ||
            !ParseU64String(&value.manifest_generation) ||
            !ConsumeLiteral(",\"namespace\":") ||
            !ParseNamespace(&value.namespace_identity) ||
            !ConsumeLiteral(",\"open_entry\":")) {
            return false;
        }
        if (PeekLiteral("null")) {
            if (!ConsumeLiteral("null")) {
                return false;
            }
        } else {
            RawManifestSegmentEntryV1 entry{};
            if (!ParseEntry(&entry)) {
                return false;
            }
            value.open_entry = std::move(entry);
        }
        if (!ConsumeLiteral(",\"schema_version\":") ||
            !ParseU32Number(&value.schema_version) ||
            !ConsumeLiteral("}") ||
            position_ != input_.size()) {
            return false;
        }
        *output = std::move(value);
        return true;
    }

private:
    [[nodiscard]] bool PeekLiteral(
        std::string_view literal) const noexcept {
        return literal.size() <= input_.size() - position_ &&
               input_.substr(position_, literal.size()) == literal;
    }

    [[nodiscard]] bool ConsumeLiteral(
        std::string_view literal) noexcept {
        if (!PeekLiteral(literal)) {
            return false;
        }
        position_ += literal.size();
        return true;
    }

    [[nodiscard]] bool ParseU32Number(
        std::uint32_t* output) noexcept {
        if (output == nullptr || position_ >= input_.size()) {
            return false;
        }
        const std::size_t begin = position_;
        if (input_[position_] == '0') {
            ++position_;
            if (position_ < input_.size() &&
                input_[position_] >= '0' &&
                input_[position_] <= '9') {
                return false;
            }
        } else {
            if (input_[position_] < '1' ||
                input_[position_] > '9') {
                return false;
            }
            do {
                ++position_;
            } while (position_ < input_.size() &&
                     input_[position_] >= '0' &&
                     input_[position_] <= '9');
        }
        std::uint32_t value = 0U;
        const char* const first = input_.data() + begin;
        const char* const last = input_.data() + position_;
        const auto conversion =
            std::from_chars(first, last, value);
        if (conversion.ec != std::errc{} ||
            conversion.ptr != last) {
            return false;
        }
        *output = value;
        return true;
    }

    [[nodiscard]] bool ParseString(std::string* output) {
        if (output == nullptr ||
            !ConsumeLiteral("\"")) {
            return false;
        }
        std::string value;
        while (position_ < input_.size()) {
            const unsigned char byte =
                static_cast<unsigned char>(input_[position_++]);
            if (byte == static_cast<unsigned char>('"')) {
                if (!IsValidUtf8(value)) {
                    return false;
                }
                *output = std::move(value);
                return true;
            }
            if (byte < 0x20U) {
                return false;
            }
            if (byte != static_cast<unsigned char>('\\')) {
                value.push_back(static_cast<char>(byte));
                continue;
            }
            if (position_ >= input_.size()) {
                return false;
            }
            const char escape = input_[position_++];
            switch (escape) {
            case '"':
                value.push_back('"');
                break;
            case '\\':
                value.push_back('\\');
                break;
            case 'b':
                value.push_back('\b');
                break;
            case 't':
                value.push_back('\t');
                break;
            case 'n':
                value.push_back('\n');
                break;
            case 'f':
                value.push_back('\f');
                break;
            case 'r':
                value.push_back('\r');
                break;
            case 'u': {
                if (input_.size() - position_ < 4U) {
                    return false;
                }
                std::uint32_t code_point = 0U;
                for (std::size_t index = 0U; index < 4U; ++index) {
                    const int digit =
                        HexDigit(input_[position_ + index]);
                    if (digit < 0) {
                        return false;
                    }
                    code_point =
                        (code_point << 4U) |
                        static_cast<std::uint32_t>(digit);
                }
                position_ += 4U;
                // Canonical RawManifestV1 only uses \u escapes for
                // U+0000..U+001F. Accepting ASCII here lets the final
                // canonical byte comparison distinguish non-canonical
                // escapes without implementing a second serializer.
                if (code_point > 0x7fU) {
                    return false;
                }
                value.push_back(
                    static_cast<char>(code_point));
                break;
            }
            default:
                return false;
            }
        }
        return false;
    }

    [[nodiscard]] bool ParseU64String(
        std::uint64_t* output) {
        std::string text;
        if (output == nullptr ||
            !ParseString(&text) ||
            text.empty() ||
            (text.size() > 1U && text.front() == '0') ||
            !std::all_of(
                text.begin(),
                text.end(),
                [](char character) {
                    return character >= '0' &&
                           character <= '9';
                })) {
            return false;
        }
        std::uint64_t value = 0U;
        const auto conversion =
            std::from_chars(
                text.data(),
                text.data() + text.size(),
                value);
        if (conversion.ec != std::errc{} ||
            conversion.ptr != text.data() + text.size()) {
            return false;
        }
        *output = value;
        return true;
    }

    [[nodiscard]] bool ParseNullableU64String(
        std::optional<std::uint64_t>* output) {
        if (output == nullptr) {
            return false;
        }
        if (PeekLiteral("null")) {
            if (!ConsumeLiteral("null")) {
                return false;
            }
            output->reset();
            return true;
        }
        std::uint64_t value = 0U;
        if (!ParseU64String(&value)) {
            return false;
        }
        *output = value;
        return true;
    }

    template <std::size_t Size>
    [[nodiscard]] bool ParseHex(
        std::array<std::byte, Size>* output) {
        if (output == nullptr) {
            return false;
        }
        std::string text;
        if (!ParseString(&text) ||
            text.size() != Size * 2U) {
            return false;
        }
        std::array<std::byte, Size> value{};
        for (std::size_t index = 0U; index < Size; ++index) {
            const char high_character = text[index * 2U];
            const char low_character = text[index * 2U + 1U];
            if (!((high_character >= '0' &&
                   high_character <= '9') ||
                  (high_character >= 'a' &&
                   high_character <= 'f')) ||
                !((low_character >= '0' &&
                   low_character <= '9') ||
                  (low_character >= 'a' &&
                   low_character <= 'f'))) {
                return false;
            }
            const int high = HexDigit(high_character);
            const int low = HexDigit(low_character);
            value[index] = static_cast<std::byte>(
                static_cast<unsigned int>(
                    (high << 4) | low));
        }
        *output = value;
        return true;
    }

    template <std::size_t Size>
    [[nodiscard]] bool ParseNullableHex(
        std::array<std::byte, Size>* output) {
        if (output == nullptr) {
            return false;
        }
        if (PeekLiteral("null")) {
            if (!ConsumeLiteral("null")) {
                return false;
            }
            output->fill(std::byte{0});
            return true;
        }
        return ParseHex(output);
    }

    [[nodiscard]] bool ParseNullableString(
        std::optional<std::string>* output) {
        if (output == nullptr) {
            return false;
        }
        if (PeekLiteral("null")) {
            if (!ConsumeLiteral("null")) {
                return false;
            }
            output->reset();
            return true;
        }
        std::string value;
        if (!ParseString(&value)) {
            return false;
        }
        *output = std::move(value);
        return true;
    }

    [[nodiscard]] bool ParseNamespace(
        RawManifestNamespaceV1* output) {
        RawManifestNamespaceV1 value{};
        if (output == nullptr ||
            !ConsumeLiteral("{\"capture_date\":") ||
            !ParseU32Number(&value.capture_date) ||
            !ConsumeLiteral(",\"source_stream_id\":") ||
            !ParseU32Number(&value.source_stream_id) ||
            !ConsumeLiteral(",\"stream_day_id\":") ||
            !ParseHex(&value.stream_day_id) ||
            !ConsumeLiteral("}")) {
            return false;
        }
        *output = value;
        return true;
    }

    [[nodiscard]] bool ParseState(
        RawManifestSegmentStateV1* output) {
        std::string state;
        if (output == nullptr ||
            !ParseString(&state)) {
            return false;
        }
        if (state == "open") {
            *output = RawManifestSegmentStateV1::kOpen;
            return true;
        }
        if (state == "closed") {
            *output = RawManifestSegmentStateV1::kClosed;
            return true;
        }
        return false;
    }

    [[nodiscard]] bool ParseEntry(
        RawManifestSegmentEntryV1* output) {
        RawManifestSegmentEntryV1 value{};
        if (output == nullptr ||
            !ConsumeLiteral("{\"accepted_marker_bytes\":") ||
            !ParseHex(&value.accepted_marker_bytes) ||
            !ConsumeLiteral(",\"accepted_marker_sha256\":") ||
            !ParseHex(&value.accepted_marker_sha256) ||
            !ConsumeLiteral(
                ",\"actual_first_ingress_sequence\":") ||
            !ParseNullableU64String(
                &value.actual_first_ingress_sequence) ||
            !ConsumeLiteral(
                ",\"actual_last_ingress_sequence\":") ||
            !ParseNullableU64String(
                &value.actual_last_ingress_sequence) ||
            !ConsumeLiteral(",\"archive_locator\":") ||
            !ParseNullableString(&value.archive_locator) ||
            !ConsumeLiteral(",\"build_manifest_sha256\":") ||
            !ParseHex(&value.build_manifest_sha256) ||
            !ConsumeLiteral(",\"clock_epoch_algorithm\":") ||
            !ParseU32Number(&value.clock_epoch_algorithm) ||
            !ConsumeLiteral(",\"clock_epoch_digest\":") ||
            !ParseHex(&value.clock_epoch_digest) ||
            !ConsumeLiteral(",\"clock_epoch_label\":") ||
            !ParseU64String(&value.clock_epoch_label) ||
            !ConsumeLiteral(",\"config_sha256\":") ||
            !ParseHex(&value.config_sha256) ||
            !ConsumeLiteral(
                ",\"endpoint_contract_sha256\":") ||
            !ParseHex(&value.endpoint_contract_sha256) ||
            !ConsumeLiteral(",\"finalization_cycle_id\":") ||
            !ParseNullableHex(&value.finalization_cycle_id) ||
            !ConsumeLiteral(",\"host_uuid\":") ||
            !ParseHex(&value.host_uuid) ||
            !ConsumeLiteral(",\"immutable_grant_sha256\":") ||
            !ParseNullableHex(&value.immutable_grant_sha256) ||
            !ConsumeLiteral(",\"libmdl_api_sha256\":") ||
            !ParseHex(&value.libmdl_api_sha256) ||
            !ConsumeLiteral(",\"linux_boot_id\":") ||
            !ParseHex(&value.linux_boot_id) ||
            !ConsumeLiteral(
                ",\"maintenance_report_locator\":") ||
            !ParseNullableString(
                &value.maintenance_report_locator) ||
            !ConsumeLiteral(",\"namespace\":") ||
            !ParseNamespace(&value.namespace_identity) ||
            !ConsumeLiteral(
                ",\"next_expected_first_ingress_sequence\":") ||
            !ParseU64String(
                &value.next_expected_first_ingress_sequence) ||
            !ConsumeLiteral(",\"raw_schema_sha256\":") ||
            !ParseHex(&value.raw_schema_sha256) ||
            !ConsumeLiteral(",\"record_count\":") ||
            !ParseU64String(&value.record_count) ||
            !ConsumeLiteral(",\"reserve_state_uuid\":") ||
            !ParseNullableHex(&value.reserve_state_uuid) ||
            !ConsumeLiteral(",\"sdk_archive_sha256\":") ||
            !ParseHex(&value.sdk_archive_sha256) ||
            !ConsumeLiteral(",\"segment_base_wal_pos\":") ||
            !ParseU64String(&value.segment_base_wal_pos) ||
            !ConsumeLiteral(",\"segment_flags\":") ||
            !ParseU32Number(&value.segment_flags) ||
            !ConsumeLiteral(",\"segment_logical_length\":") ||
            !ParseU64String(&value.segment_logical_length) ||
            !ConsumeLiteral(",\"segment_sequence\":") ||
            !ParseU32Number(&value.segment_sequence) ||
            !ConsumeLiteral(",\"segment_sha256\":") ||
            !ParseHex(&value.segment_sha256) ||
            !ConsumeLiteral(",\"state\":") ||
            !ParseState(&value.state) ||
            !ConsumeLiteral("}")) {
            return false;
        }
        *output = std::move(value);
        return true;
    }

    [[nodiscard]] bool ParseClosedEntries(
        std::vector<RawManifestSegmentEntryV1>* output) {
        if (output == nullptr ||
            !ConsumeLiteral("[")) {
            return false;
        }
        std::vector<RawManifestSegmentEntryV1> entries;
        if (ConsumeLiteral("]")) {
            *output = std::move(entries);
            return true;
        }
        for (;;) {
            RawManifestSegmentEntryV1 entry{};
            if (!ParseEntry(&entry)) {
                return false;
            }
            entries.push_back(std::move(entry));
            if (ConsumeLiteral("]")) {
                *output = std::move(entries);
                return true;
            }
            if (!ConsumeLiteral(",")) {
                return false;
            }
        }
    }

    std::string_view input_;
    std::size_t position_ = 0U;
};

struct FileToken final {
    dev_t device = 0;
    ino_t inode = 0;
    off_t size = 0;
};

[[nodiscard]] bool IsPrivateDirectory(
    int directory_fd,
    struct stat* status = nullptr) noexcept {
    struct stat local {};
    if (directory_fd < 0 ||
        ::fstat(directory_fd, &local) != 0 ||
        !S_ISDIR(local.st_mode) ||
        local.st_uid != ::geteuid() ||
        (local.st_mode & (S_IWGRP | S_IWOTH)) != 0) {
        return false;
    }
    if (status != nullptr) {
        *status = local;
    }
    return true;
}

[[nodiscard]] bool SameInode(
    const struct stat& left,
    const struct stat& right) noexcept {
    return left.st_dev == right.st_dev &&
           left.st_ino == right.st_ino;
}

[[nodiscard]] bool SameStableFileMetadata(
    const struct stat& left,
    const struct stat& right) noexcept {
    return SameInode(left, right) &&
           left.st_mode == right.st_mode &&
           left.st_uid == right.st_uid &&
           left.st_nlink == right.st_nlink &&
           left.st_size == right.st_size &&
           left.st_mtim.tv_sec == right.st_mtim.tv_sec &&
           left.st_mtim.tv_nsec == right.st_mtim.tv_nsec &&
           left.st_ctim.tv_sec == right.st_ctim.tv_sec &&
           left.st_ctim.tv_nsec == right.st_ctim.tv_nsec;
}

[[nodiscard]] bool IsSafeManifestFile(
    const struct stat& status) noexcept {
    return S_ISREG(status.st_mode) &&
           status.st_uid == ::geteuid() &&
           status.st_nlink == 1 &&
           (status.st_mode & 07777U) == 0600U;
}

[[nodiscard]] bool NamedFileMatches(
    int directory_fd,
    const char* name,
    const FileToken& token) noexcept {
    struct stat named {};
    return ::fstatat(
               directory_fd,
               name,
               &named,
               AT_SYMLINK_NOFOLLOW) == 0 &&
           named.st_dev == token.device &&
           named.st_ino == token.inode &&
           named.st_size == token.size;
}

[[nodiscard]] int OpenAtLoop(
    int directory_fd,
    const char* name,
    int flags,
    mode_t mode = 0U) noexcept {
    for (;;) {
        const int fd = ::openat(
            directory_fd, name, flags, mode);
        if (fd >= 0 || errno != EINTR) {
            return fd;
        }
    }
}

[[nodiscard]] bool FsyncLoop(int fd) noexcept {
    for (;;) {
        if (::fsync(fd) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

[[nodiscard]] bool PreadAll(
    int fd,
    std::span<char> output) noexcept {
    std::size_t complete = 0U;
    while (complete < output.size()) {
        const std::size_t request = std::min(
            output.size() - complete,
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max()));
        const ssize_t result = ::pread(
            fd,
            output.data() + complete,
            request,
            static_cast<off_t>(complete));
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (result == 0) {
            errno = EIO;
            return false;
        }
        complete += static_cast<std::size_t>(result);
    }
    return true;
}

[[nodiscard]] bool PwriteAll(
    int fd,
    std::span<const char> input) noexcept {
    std::size_t complete = 0U;
    while (complete < input.size()) {
        const std::size_t request = std::min(
            input.size() - complete,
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max()));
        const ssize_t result = ::pwrite(
            fd,
            input.data() + complete,
            request,
            static_cast<off_t>(complete));
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (result == 0) {
            errno = EIO;
            return false;
        }
        complete += static_cast<std::size_t>(result);
    }
    return true;
}

[[nodiscard]] RawManifestStoreError OpenNoAtimeDirectoryAlias(
    int retained_directory_fd,
    ScopedFd* alias,
    std::string* error) noexcept {
    struct stat retained_status {};
    if (alias == nullptr ||
        !IsPrivateDirectory(
            retained_directory_fd,
            &retained_status)) {
        SetError(
            error,
            "Raw manifest directory is not a private service-owned directory");
        return RawManifestStoreError::kUnsafeDirectory;
    }
    alias->Reset(OpenAtLoop(
        retained_directory_fd,
        ".",
        O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_NONBLOCK |
            O_CLOEXEC | O_NOATIME));
    if (alias->get() < 0) {
        SetError(
            error,
            std::string(
                "cannot retain a no-atime Raw manifest directory: ") +
                std::strerror(errno));
        return RawManifestStoreError::kUnsafeDirectory;
    }
    struct stat alias_status {};
    const int flags = ::fcntl(alias->get(), F_GETFL);
    if (!IsPrivateDirectory(alias->get(), &alias_status) ||
        !SameInode(retained_status, alias_status) ||
        flags < 0 ||
        (flags & O_NOATIME) == 0) {
        SetError(
            error,
            "Raw manifest no-atime directory alias failed validation");
        return RawManifestStoreError::kUnsafeDirectory;
    }
    return RawManifestStoreError::kNone;
}

struct LoadedManifest final {
    RawManifestV1 manifest{};
    std::string bytes;
    FileToken token{};
};

[[nodiscard]] RawManifestStoreError LoadNamedManifestAt(
    int directory_fd,
    const char* name,
    const RawManifestNamespaceV1& expected_namespace,
    std::size_t maximum_bytes,
    LoadedManifest* loaded,
    RawManifestV1Error* model_error,
    std::string* error) noexcept {
    if (loaded == nullptr ||
        maximum_bytes == 0U ||
        !IsExpectedNamespaceValid(expected_namespace)) {
        SetError(error, "Raw manifest load arguments are invalid");
        return RawManifestStoreError::kInvalidArgument;
    }
    ScopedFd file(OpenAtLoop(
        directory_fd,
        name,
        O_RDONLY | O_NOFOLLOW | O_NONBLOCK |
            O_CLOEXEC | O_NOATIME));
    if (file.get() < 0) {
        if (errno == ENOENT) {
            SetError(error, "Raw manifest does not exist");
            return RawManifestStoreError::kNotFound;
        }
        SetError(
            error,
            std::string("cannot open Raw manifest: ") +
                std::strerror(errno));
        return RawManifestStoreError::kUnsafeFile;
    }

    struct stat before {};
    const int flags = ::fcntl(file.get(), F_GETFL);
    if (::fstat(file.get(), &before) != 0 ||
        !IsSafeManifestFile(before) ||
        flags < 0 ||
        (flags & O_ACCMODE) != O_RDONLY ||
        (flags & O_NOATIME) == 0) {
        SetError(
            error,
            "Raw manifest has an unsafe type, owner, mode, link count, or open flags");
        return RawManifestStoreError::kUnsafeFile;
    }
    if (before.st_size < 0 ||
        static_cast<std::uintmax_t>(before.st_size) >
            static_cast<std::uintmax_t>(maximum_bytes) ||
        static_cast<std::uintmax_t>(before.st_size) >
            static_cast<std::uintmax_t>(
                std::numeric_limits<std::size_t>::max())) {
        SetError(
            error,
            "Raw manifest exceeds the caller-supplied read bound");
        return RawManifestStoreError::kFileTooLarge;
    }

    LoadedManifest result;
    try {
        result.bytes.resize(
            static_cast<std::size_t>(before.st_size));
    } catch (...) {
        SetError(error, "cannot allocate Raw manifest input buffer");
        return RawManifestStoreError::kAllocationFailure;
    }
    if (!PreadAll(file.get(), std::span<char>(result.bytes))) {
        SetError(
            error,
            std::string("cannot read complete Raw manifest bytes: ") +
                std::strerror(errno));
        return RawManifestStoreError::kReadFailure;
    }

    struct stat after {};
    if (::fstat(file.get(), &after) != 0 ||
        !SameStableFileMetadata(before, after)) {
        SetError(
            error,
            "Raw manifest metadata changed while it was being read");
        return RawManifestStoreError::kReadFailure;
    }
    result.token = {
        after.st_dev,
        after.st_ino,
        after.st_size};
    if (!NamedFileMatches(
            directory_fd, name, result.token)) {
        SetError(
            error,
            "Raw manifest pathname no longer names the validated inode");
        return RawManifestStoreError::kUnsafeFile;
    }

    const RawManifestStoreError parse_result =
        ParseRawManifestJcs(
            result.bytes,
            &result.manifest,
            model_error);
    if (parse_result != RawManifestStoreError::kNone) {
        SetError(
            error,
            std::string("Raw manifest bytes are invalid: ") +
                std::string(
                    RawManifestStoreErrorName(parse_result)));
        return parse_result;
    }
    if (!SameNamespace(
            result.manifest.namespace_identity,
            expected_namespace)) {
        SetError(
            error,
            "Raw manifest belongs to a different stream-day namespace");
        return RawManifestStoreError::kNamespaceMismatch;
    }
    *loaded = std::move(result);
    return RawManifestStoreError::kNone;
}

[[nodiscard]] bool ValidateLeaseBinding(
    const RawWriterLease& lease,
    const RawManifestNamespaceV1& expected_namespace,
    std::string* error) noexcept {
    if (!IsExpectedNamespaceValid(expected_namespace) ||
        lease.source_stream_id() !=
            expected_namespace.source_stream_id ||
        lease.capture_date() != expected_namespace.capture_date ||
        !IsPrivateDirectory(lease.directory_descriptor())) {
        SetError(
            error,
            "Raw writer lease does not bind the requested manifest namespace");
        return false;
    }
    struct stat lease_status {};
    struct stat named_status {};
    const int flags =
        ::fcntl(lease.descriptor(), F_GETFL);
    if (::fstat(lease.descriptor(), &lease_status) != 0 ||
        ::fstatat(
            lease.directory_descriptor(),
            kRawWriterLeaseFilename,
            &named_status,
            AT_SYMLINK_NOFOLLOW) != 0 ||
        !S_ISREG(lease_status.st_mode) ||
        lease_status.st_uid != ::geteuid() ||
        lease_status.st_nlink != 1 ||
        (lease_status.st_mode & 07777U) != 0600U ||
        !SameInode(lease_status, named_status) ||
        flags < 0 ||
        (flags & O_ACCMODE) != O_RDWR ||
        (flags & O_APPEND) != 0) {
        SetError(
            error,
            "Raw writer lease descriptor or pathname binding is unsafe");
        return false;
    }
    return true;
}

[[nodiscard]] int RenameNoReplace(
    int directory_fd,
    const char* old_name,
    const char* new_name) noexcept {
    return static_cast<int>(::syscall(
        SYS_renameat2,
        directory_fd,
        old_name,
        directory_fd,
        new_name,
        RENAME_NOREPLACE));
}

[[nodiscard]] int RenameReplace(
    int directory_fd,
    const char* old_name,
    const char* new_name) noexcept {
    for (;;) {
        const int result = ::renameat(
            directory_fd,
            old_name,
            directory_fd,
            new_name);
        if (result == 0 || errno != EINTR) {
            return result;
        }
    }
}

[[nodiscard]] RawManifestStoreError EnsureCandidateSynced(
    int directory_fd,
    const std::string& desired_bytes,
    const FileToken& expected_token,
    std::string* error) noexcept {
    ScopedFd candidate(OpenAtLoop(
        directory_fd,
        kRawManifestTemporaryFilename,
        O_RDWR | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC));
    if (candidate.get() < 0) {
        SetError(
            error,
            std::string("cannot retain Raw manifest temporary: ") +
                std::strerror(errno));
        return RawManifestStoreError::kUnsafeFile;
    }
    struct stat status {};
    const int flags = ::fcntl(candidate.get(), F_GETFL);
    if (::fstat(candidate.get(), &status) != 0 ||
        !IsSafeManifestFile(status) ||
        flags < 0 ||
        (flags & O_ACCMODE) != O_RDWR ||
        (flags & O_APPEND) != 0 ||
        status.st_size !=
            static_cast<off_t>(desired_bytes.size()) ||
        status.st_dev != expected_token.device ||
        status.st_ino != expected_token.inode ||
        !NamedFileMatches(
            directory_fd,
            kRawManifestTemporaryFilename,
            expected_token)) {
        SetError(
            error,
            "Raw manifest temporary descriptor is unsafe or changed");
        return RawManifestStoreError::kUnsafeFile;
    }
    if (!FsyncLoop(candidate.get())) {
        SetError(
            error,
            std::string("cannot fsync Raw manifest temporary: ") +
                std::strerror(errno));
        return RawManifestStoreError::kSyncFailure;
    }
    return RawManifestStoreError::kNone;
}

}  // namespace

RawManifestFilenameKind ClassifyRawManifestFilename(
    std::string_view name) noexcept {
    return name == kRawManifestCurrentFilename
               ? RawManifestFilenameKind::kCurrent
               : RawManifestFilenameKind::kInvalid;
}

std::string_view RawManifestStoreErrorName(
    RawManifestStoreError error) noexcept {
    switch (error) {
    case RawManifestStoreError::kNone:
        return "none";
    case RawManifestStoreError::kInvalidArgument:
        return "invalid_argument";
    case RawManifestStoreError::kUnsupportedFilename:
        return "unsupported_filename";
    case RawManifestStoreError::kUnsafeDirectory:
        return "unsafe_directory";
    case RawManifestStoreError::kNotFound:
        return "not_found";
    case RawManifestStoreError::kUnsafeFile:
        return "unsafe_file";
    case RawManifestStoreError::kFileTooLarge:
        return "file_too_large";
    case RawManifestStoreError::kReadFailure:
        return "read_failure";
    case RawManifestStoreError::kInvalidJson:
        return "invalid_json";
    case RawManifestStoreError::kNonCanonicalJson:
        return "non_canonical_json";
    case RawManifestStoreError::kInvalidModel:
        return "invalid_model";
    case RawManifestStoreError::kNamespaceMismatch:
        return "namespace_mismatch";
    case RawManifestStoreError::kAmbiguousTemporary:
        return "ambiguous_temporary";
    case RawManifestStoreError::kWriteFailure:
        return "write_failure";
    case RawManifestStoreError::kSyncFailure:
        return "sync_failure";
    case RawManifestStoreError::kPublishConflict:
        return "publish_conflict";
    case RawManifestStoreError::kReadbackFailure:
        return "readback_failure";
    case RawManifestStoreError::kAllocationFailure:
        return "allocation_failure";
    }
    return "unknown";
}

RawManifestStoreError ParseRawManifestJcs(
    std::string_view bytes,
    RawManifestV1* manifest,
    RawManifestV1Error* model_error) noexcept {
    if (model_error != nullptr) {
        *model_error = RawManifestV1Error::kNone;
    }
    if (manifest == nullptr) {
        return RawManifestStoreError::kInvalidArgument;
    }
    try {
        RawManifestV1 decoded{};
        JsonParser parser(bytes);
        if (!parser.ParseManifest(&decoded)) {
            return RawManifestStoreError::kInvalidJson;
        }
        const RawManifestV1Error validation =
            ValidateManifestModel(decoded);
        if (validation != RawManifestV1Error::kNone) {
            if (model_error != nullptr) {
                *model_error = validation;
            }
            return RawManifestStoreError::kInvalidModel;
        }
        std::string canonical;
        const RawManifestV1Error encode_result =
            EncodeRawManifestJcs(decoded, &canonical);
        if (encode_result != RawManifestV1Error::kNone) {
            if (model_error != nullptr) {
                *model_error = encode_result;
            }
            return RawManifestStoreError::kInvalidModel;
        }
        if (canonical != bytes) {
            return RawManifestStoreError::kNonCanonicalJson;
        }
        *manifest = std::move(decoded);
        return RawManifestStoreError::kNone;
    } catch (const std::bad_alloc&) {
        return RawManifestStoreError::kAllocationFailure;
    } catch (...) {
        return RawManifestStoreError::kAllocationFailure;
    }
}

RawManifestStoreError LoadCurrentRawManifestAt(
    int retained_directory_fd,
    const RawManifestNamespaceV1& expected_namespace,
    std::size_t maximum_bytes,
    RawManifestV1* manifest,
    std::string* canonical_bytes,
    RawManifestV1Error* model_error,
    std::string* error,
    const RawManifestV1* append_only_predecessor) noexcept {
    if (model_error != nullptr) {
        *model_error = RawManifestV1Error::kNone;
    }
    SetError(error, {});
    if (manifest == nullptr) {
        SetError(error, "Raw manifest output is null");
        return RawManifestStoreError::kInvalidArgument;
    }
    ScopedFd directory;
    const RawManifestStoreError directory_result =
        OpenNoAtimeDirectoryAlias(
            retained_directory_fd,
            &directory,
            error);
    if (directory_result != RawManifestStoreError::kNone) {
        return directory_result;
    }
    try {
        LoadedManifest loaded;
        const RawManifestStoreError load_result =
            LoadNamedManifestAt(
                directory.get(),
                kRawManifestCurrentFilename,
                expected_namespace,
                maximum_bytes,
                &loaded,
                model_error,
                error);
        if (load_result != RawManifestStoreError::kNone) {
            return load_result;
        }
        if (append_only_predecessor != nullptr) {
            const RawManifestV1Error chain_result =
                ValidateManifestModel(
                    loaded.manifest,
                    append_only_predecessor);
            if (chain_result != RawManifestV1Error::kNone) {
                if (model_error != nullptr) {
                    *model_error = chain_result;
                }
                SetError(
                    error,
                    std::string(
                        "Raw manifest violates the trusted append-only predecessor: ") +
                        std::string(
                            RawManifestV1ErrorName(chain_result)));
                return RawManifestStoreError::kInvalidModel;
            }
        }
        if (canonical_bytes != nullptr) {
            *canonical_bytes = loaded.bytes;
        }
        *manifest = std::move(loaded.manifest);
        SetError(error, {});
        return RawManifestStoreError::kNone;
    } catch (const std::bad_alloc&) {
        SetError(error, "cannot allocate Raw manifest load result");
        return RawManifestStoreError::kAllocationFailure;
    } catch (...) {
        SetError(error, "unexpected Raw manifest load failure");
        return RawManifestStoreError::kAllocationFailure;
    }
}

RawManifestStoreError PublishCurrentRawManifest(
    const RawWriterLease& lease,
    const RawManifestNamespaceV1& expected_namespace,
    const RawManifestV1& manifest,
    std::size_t maximum_bytes,
    RawManifestV1Error* model_error,
    std::string* error) noexcept {
    if (model_error != nullptr) {
        *model_error = RawManifestV1Error::kNone;
    }
    SetError(error, {});
    if (maximum_bytes == 0U ||
        !ValidateLeaseBinding(
            lease, expected_namespace, error) ||
        !SameNamespace(
            manifest.namespace_identity,
            expected_namespace)) {
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "Raw manifest publication namespace is invalid");
        }
        return !SameNamespace(
                   manifest.namespace_identity,
                   expected_namespace)
                   ? RawManifestStoreError::kNamespaceMismatch
                   : RawManifestStoreError::kInvalidArgument;
    }

    try {
        std::string desired_bytes;
        RawManifestV1Error validation =
            EncodeRawManifestJcs(
                manifest, &desired_bytes);
        if (validation != RawManifestV1Error::kNone) {
            if (model_error != nullptr) {
                *model_error = validation;
            }
            SetError(
                error,
                std::string("Raw manifest model is invalid: ") +
                    std::string(
                        RawManifestV1ErrorName(validation)));
            return RawManifestStoreError::kInvalidModel;
        }
        if (desired_bytes.size() > maximum_bytes ||
            desired_bytes.size() >
                static_cast<std::size_t>(
                    std::numeric_limits<off_t>::max())) {
            SetError(
                error,
                "Raw manifest exceeds the caller-supplied publication bound");
            return RawManifestStoreError::kFileTooLarge;
        }

        const int directory_fd =
            lease.directory_descriptor();
        LoadedManifest current;
        const RawManifestStoreError current_result =
            LoadNamedManifestAt(
                directory_fd,
                kRawManifestCurrentFilename,
                expected_namespace,
                maximum_bytes,
                &current,
                model_error,
                error);
        const bool has_current =
            current_result == RawManifestStoreError::kNone;
        if (!has_current &&
            current_result != RawManifestStoreError::kNotFound) {
            return current_result;
        }

        struct stat temporary_status {};
        const bool temporary_exists =
            ::fstatat(
                directory_fd,
                kRawManifestTemporaryFilename,
                &temporary_status,
                AT_SYMLINK_NOFOLLOW) == 0;
        const int temporary_stat_error =
            temporary_exists ? 0 : errno;
        if (!temporary_exists &&
            temporary_stat_error != ENOENT) {
            SetError(
                error,
                std::string(
                    "cannot inspect Raw manifest temporary: ") +
                    std::strerror(temporary_stat_error));
            return RawManifestStoreError::kUnsafeFile;
        }

        if (has_current &&
            current.bytes == desired_bytes) {
            if (temporary_exists) {
                SetError(
                    error,
                    "current and temporary Raw manifests coexist ambiguously");
                return RawManifestStoreError::kAmbiguousTemporary;
            }
            SetError(error, {});
            return RawManifestStoreError::kNone;
        }

        if (has_current) {
            validation =
                ValidateManifestModel(
                    manifest, &current.manifest);
            if (validation != RawManifestV1Error::kNone) {
                if (model_error != nullptr) {
                    *model_error = validation;
                }
                SetError(
                    error,
                    std::string(
                        "Raw manifest is not an append-only successor: ") +
                        std::string(
                            RawManifestV1ErrorName(validation)));
                return RawManifestStoreError::kInvalidModel;
            }
        }

        LoadedManifest candidate;
        ScopedFd newly_created_candidate;
        if (temporary_exists) {
            const RawManifestStoreError temp_result =
                LoadNamedManifestAt(
                    directory_fd,
                    kRawManifestTemporaryFilename,
                    expected_namespace,
                    maximum_bytes,
                    &candidate,
                    model_error,
                    error);
            if (temp_result != RawManifestStoreError::kNone ||
                candidate.bytes != desired_bytes) {
                SetError(
                    error,
                    "Raw manifest temporary is unsafe, partial, or differs from the retry bytes");
                return RawManifestStoreError::kAmbiguousTemporary;
            }
        } else {
            newly_created_candidate.Reset(OpenAtLoop(
                directory_fd,
                kRawManifestTemporaryFilename,
                O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW |
                    O_NONBLOCK | O_CLOEXEC,
                0600));
            if (newly_created_candidate.get() < 0 ||
                ::fchmod(
                    newly_created_candidate.get(),
                    0600) != 0) {
                SetError(
                    error,
                    std::string(
                        "cannot create typed Raw manifest temporary: ") +
                        std::strerror(errno));
                return RawManifestStoreError::kWriteFailure;
            }
            struct stat created_status {};
            if (::fstat(
                    newly_created_candidate.get(),
                    &created_status) != 0 ||
                !IsSafeManifestFile(created_status) ||
                created_status.st_size != 0 ||
                !PwriteAll(
                    newly_created_candidate.get(),
                    std::span<const char>(desired_bytes))) {
                SetError(
                    error,
                    std::string(
                        "cannot write complete Raw manifest temporary: ") +
                        std::strerror(errno));
                return RawManifestStoreError::kWriteFailure;
            }
            if (!FsyncLoop(
                    newly_created_candidate.get())) {
                SetError(
                    error,
                    std::string(
                        "cannot fsync complete Raw manifest temporary: ") +
                        std::strerror(errno));
                return RawManifestStoreError::kSyncFailure;
            }
            struct stat synced_status {};
            if (::fstat(
                    newly_created_candidate.get(),
                    &synced_status) != 0 ||
                !IsSafeManifestFile(synced_status) ||
                synced_status.st_size !=
                    static_cast<off_t>(
                        desired_bytes.size())) {
                SetError(
                    error,
                    "Raw manifest temporary changed before publication");
                return RawManifestStoreError::kUnsafeFile;
            }
            candidate.manifest = manifest;
            candidate.bytes = desired_bytes;
            candidate.token = {
                synced_status.st_dev,
                synced_status.st_ino,
                synced_status.st_size};
            if (!NamedFileMatches(
                    directory_fd,
                    kRawManifestTemporaryFilename,
                    candidate.token)) {
                SetError(
                    error,
                    "Raw manifest temporary pathname does not name the written inode");
                return RawManifestStoreError::kUnsafeFile;
            }
        }

        const RawManifestStoreError candidate_sync =
            EnsureCandidateSynced(
                directory_fd,
                desired_bytes,
                candidate.token,
                error);
        if (candidate_sync != RawManifestStoreError::kNone) {
            return candidate_sync;
        }
        if (has_current &&
            !NamedFileMatches(
                directory_fd,
                kRawManifestCurrentFilename,
                current.token)) {
            SetError(
                error,
                "Raw manifest current name changed before replacement");
            return RawManifestStoreError::kPublishConflict;
        }
        if (!NamedFileMatches(
                directory_fd,
                kRawManifestTemporaryFilename,
                candidate.token)) {
            SetError(
                error,
                "Raw manifest temporary name changed before publication");
            return RawManifestStoreError::kPublishConflict;
        }

        const int rename_result =
            has_current
                ? RenameReplace(
                      directory_fd,
                      kRawManifestTemporaryFilename,
                      kRawManifestCurrentFilename)
                : RenameNoReplace(
                      directory_fd,
                      kRawManifestTemporaryFilename,
                      kRawManifestCurrentFilename);
        if (rename_result != 0) {
            SetError(
                error,
                std::string(
                    "cannot atomically publish Raw manifest: ") +
                    std::strerror(errno));
            return RawManifestStoreError::kPublishConflict;
        }
        if (!FsyncLoop(directory_fd)) {
            SetError(
                error,
                std::string(
                    "cannot fsync Raw manifest parent directory: ") +
                    std::strerror(errno));
            return RawManifestStoreError::kSyncFailure;
        }

        LoadedManifest readback;
        const RawManifestStoreError readback_result =
            LoadNamedManifestAt(
                directory_fd,
                kRawManifestCurrentFilename,
                expected_namespace,
                maximum_bytes,
                &readback,
                model_error,
                error);
        if (readback_result != RawManifestStoreError::kNone ||
            readback.bytes != desired_bytes) {
            SetError(
                error,
                "published Raw manifest failed strict byte-for-byte readback");
            return RawManifestStoreError::kReadbackFailure;
        }
        if (has_current) {
            validation =
                ValidateManifestModel(
                    readback.manifest,
                    &current.manifest);
            if (validation != RawManifestV1Error::kNone) {
                if (model_error != nullptr) {
                    *model_error = validation;
                }
                SetError(
                    error,
                    "published Raw manifest failed append-only readback validation");
                return RawManifestStoreError::kReadbackFailure;
            }
        }
        SetError(error, {});
        return RawManifestStoreError::kNone;
    } catch (const std::bad_alloc&) {
        SetError(error, "cannot allocate Raw manifest publication state");
        return RawManifestStoreError::kAllocationFailure;
    } catch (...) {
        SetError(error, "unexpected Raw manifest publication failure");
        return RawManifestStoreError::kAllocationFailure;
    }
}

}  // namespace l2flow::ingress
