#include "l2flow/ingress/shadow_capture.h"

#include "l2flow/ops/metrics_textfile.h"
#include "l2flow/ops/stable_output_prefix.h"
#include "l2flow/sdk/vendor_head_view.h"

#include "mdl_shl2_msg.h"
#include "mdl_sys_msg.h"
#include "mdl_szl2_msg.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <limits>
#include <optional>
#include <stdexcept>
#include <thread>
#include <utility>

#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace l2flow::ingress {
namespace {

namespace sh = datayes::mdl::mdl_shl2_msg;
namespace sz = datayes::mdl::mdl_szl2_msg;

constexpr std::uint8_t kApiServiceId =
    static_cast<std::uint8_t>(datayes::mdl::MDLSID_MDL_API);
constexpr std::uint8_t kSystemServiceId =
    static_cast<std::uint8_t>(datayes::mdl::MDLSID_MDL_SYS);
constexpr std::uint16_t kSystemServiceVersion =
    static_cast<std::uint16_t>(
        datayes::mdl::mdl_sys_msg::LogonResponse::ServiceVer);
constexpr std::uint16_t kLogonResponseMessageId =
    static_cast<std::uint16_t>(
        datayes::mdl::mdl_sys_msg::LogonResponse::MessageID);
constexpr std::uint16_t kSubscribeResponseMessageId =
    static_cast<std::uint16_t>(
        datayes::mdl::mdl_sys_msg::SubscribeResponse::MessageID);
constexpr std::size_t kMdlListBytes = 8U;
constexpr std::size_t kLogonResponseBytes = 24U;
constexpr std::size_t kLogonServicesListOffset = 12U;
constexpr std::size_t kLogonReturnCodeOffset = 20U;
constexpr std::size_t kSubscribeServicesListOffset = 0U;
constexpr std::size_t kServiceItemBytes = 16U;
constexpr std::size_t kServiceIdOffset = 0U;
constexpr std::size_t kServiceVersionOffset = 4U;
constexpr std::size_t kServiceMessagesListOffset = 8U;
constexpr std::size_t kMessageStatusItemBytes = 8U;
constexpr std::size_t kMessageIdOffset = 0U;
constexpr std::size_t kMessageStatusOffset = 4U;
constexpr std::size_t kMdlStringBytes = 6U;
constexpr std::size_t kMdlStringOffsetFieldOffset = 2U;

constexpr l2flow::sdk::MessageKey kShSnapshotKey{
    4U, 101U, 4U};
constexpr l2flow::sdk::MessageKey kShTickKey{
    4U, 101U, 24U};
constexpr l2flow::sdk::MessageKey kSzSnapshotKey{
    6U, 101U, 28U};
constexpr l2flow::sdk::MessageKey kSzOrderKey{
    6U, 101U, 33U};
constexpr l2flow::sdk::MessageKey kSzTransactionKey{
    6U, 101U, 36U};

static_assert(kSystemServiceVersion ==
              datayes::mdl::mdl_sys_msg::SubscribeResponse::ServiceVer);
static_assert(kVendorMessageHeadBytes ==
              l2flow::sdk::kVendorHeadBytes);
static_assert(sizeof(datayes::mdl::MDLList) == kMdlListBytes);
static_assert(
    sizeof(datayes::mdl::mdl_sys_msg::LogonResponse) ==
    kLogonResponseBytes);
static_assert(
    offsetof(datayes::mdl::mdl_sys_msg::LogonResponse, Services) ==
    kLogonServicesListOffset);
static_assert(
    offsetof(datayes::mdl::mdl_sys_msg::LogonResponse, ReturnCode) ==
    kLogonReturnCodeOffset);
static_assert(
    sizeof(datayes::mdl::mdl_sys_msg::SubscribeResponse) ==
    kMdlListBytes);
static_assert(
    offsetof(datayes::mdl::mdl_sys_msg::SubscribeResponse, Services) ==
    kSubscribeServicesListOffset);
static_assert(
    sizeof(datayes::mdl::mdl_sys_msg::LogonResponse::ServicesItem) ==
    kServiceItemBytes);
static_assert(
    offsetof(
        datayes::mdl::mdl_sys_msg::LogonResponse::ServicesItem,
        Messages) == kServiceMessagesListOffset);
static_assert(
    sizeof(datayes::mdl::mdl_sys_msg::LogonResponse::ServicesItem::
               MessagesItem) == kMessageStatusItemBytes);
static_assert(sizeof(datayes::mdl::MDLString) == kMdlStringBytes);
static_assert(
    offsetof(datayes::mdl::MDLString, Length) == 0U);
static_assert(
    offsetof(datayes::mdl::MDLString, Offset) ==
    kMdlStringOffsetFieldOffset);
static_assert(sizeof(sh::SHL2MarketData) == 248U);
static_assert(
    sizeof(sh::SHL2MarketData::BidLevelsItem) == 28U);
static_assert(
    sizeof(sh::SHL2MarketData::BidLevelsItem::NOrdersItem) ==
    16U);
static_assert(
    sizeof(sh::SHL2MarketData::SellLevelsItem) == 28U);
static_assert(
    sizeof(sh::SHL2MarketData::SellLevelsItem::NoOrdersItem) ==
    16U);
static_assert(sizeof(sh::NGTSTick) == 70U);
static_assert(sizeof(sz::Snapshot300111_v2) == 224U);
static_assert(
    sizeof(sz::Snapshot300111_v2::BidPriceLevelItem) == 28U);
static_assert(
    sizeof(sz::Snapshot300111_v2::BidPriceLevelItem::OrdersItem) ==
    8U);
static_assert(
    sizeof(sz::Snapshot300111_v2::AskPriceLevelItem) == 28U);
static_assert(
    sizeof(sz::Snapshot300111_v2::AskPriceLevelItem::OrdersItem) ==
    8U);
static_assert(sizeof(sz::Order300192_v2) == 58U);
static_assert(sizeof(sz::Transaction300191_v2) == 70U);

class PosixShadowCaptureOutput final : public ShadowCaptureOutput {
public:
    explicit PosixShadowCaptureOutput(int fd) noexcept : fd_(fd) {}

    ~PosixShadowCaptureOutput() override {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
    }

    ShadowOutputWriteResult WriteSome(
        std::span<const std::byte> bytes) noexcept override {
        if (fd_ < 0) {
            return {0U, EBADF};
        }
        if (bytes.empty()) {
            return {};
        }

        constexpr std::size_t maximum_write =
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max());
        const std::size_t requested =
            std::min(bytes.size(), maximum_write);
        const ssize_t result =
            ::write(fd_, bytes.data(), requested);
        if (result < 0) {
            return {0U, errno};
        }
        return {static_cast<std::size_t>(result), 0};
    }

    int Fdatasync() noexcept override {
        if (fd_ < 0) {
            return EBADF;
        }
        return ::fdatasync(fd_) == 0 ? 0 : errno;
    }

    int Close() noexcept override {
        if (fd_ < 0) {
            return 0;
        }
        const int closing_fd = std::exchange(fd_, -1);
        return ::close(closing_fd) == 0 ? 0 : errno;
    }

private:
    int fd_;
};

class ScopedFd final {
public:
    ScopedFd() noexcept = default;
    explicit ScopedFd(int fd) noexcept : fd_(fd) {}

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

    [[nodiscard]] int Release() noexcept {
        return std::exchange(fd_, -1);
    }

    void Reset(int fd) noexcept {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
        fd_ = fd;
    }

private:
    int fd_ = -1;
};

bool ValidateAbsoluteOutputPath(const std::string& path,
                                int* error_number) noexcept {
    if (path.size() < 2U || path.front() != '/') {
        *error_number = EINVAL;
        return false;
    }

    std::size_t component_start = 1U;
    for (;;) {
        const std::size_t separator =
            path.find('/', component_start);
        const std::size_t component_end =
            separator == std::string::npos
                ? path.size()
                : separator;
        if (component_end == component_start) {
            *error_number = EINVAL;
            return false;
        }
        const std::size_t component_size =
            component_end - component_start;
        const bool is_dot =
            component_size == 1U &&
            path[component_start] == '.';
        const bool is_dot_dot =
            component_size == 2U &&
            path[component_start] == '.' &&
            path[component_start + 1U] == '.';
        if (is_dot || is_dot_dot) {
            *error_number = EINVAL;
            return false;
        }
        if (separator == std::string::npos) {
            return true;
        }
        component_start = separator + 1U;
    }
}

bool IsTrustedDirectoryOwner(
    uid_t owner,
    uid_t namespace_root_owner) noexcept {
    return owner == ::geteuid() ||
           owner == 0 ||
           owner == namespace_root_owner;
}

bool ValidateAncestorTransition(int parent_fd,
                                int child_fd,
                                uid_t namespace_root_owner,
                                int* error_number) noexcept {
    struct stat parent {};
    struct stat child {};
    if (::fstat(parent_fd, &parent) != 0 ||
        ::fstat(child_fd, &child) != 0) {
        *error_number = errno;
        return false;
    }
    if (!S_ISDIR(parent.st_mode) ||
        !S_ISDIR(child.st_mode) ||
        !IsTrustedDirectoryOwner(
            parent.st_uid,
            namespace_root_owner)) {
        *error_number = EACCES;
        return false;
    }
    const bool writable_by_other =
        (parent.st_mode &
         (S_IWGRP | S_IWOTH)) != 0;
    const bool sticky =
        (parent.st_mode & S_ISVTX) != 0;
    if (writable_by_other &&
        (!sticky ||
         !IsTrustedDirectoryOwner(
             child.st_uid,
             namespace_root_owner))) {
        *error_number = EACCES;
        return false;
    }
    return true;
}

bool OpenStableParentDirectory(const std::string& path,
                               ScopedFd* directory,
                               std::string* basename,
                               int* error_number) {
    if (!ValidateAbsoluteOutputPath(path, error_number)) {
        return false;
    }

    constexpr int kDirectoryFlags =
        O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;
    int root_fd = -1;
    do {
        root_fd = ::open("/", kDirectoryFlags);
    } while (root_fd < 0 && errno == EINTR);
    if (root_fd < 0) {
        *error_number = errno;
        return false;
    }
    ScopedFd current(root_fd);
    struct stat namespace_root {};
    if (::fstat(
            current.get(),
            &namespace_root) != 0) {
        *error_number = errno;
        return false;
    }
    if (!S_ISDIR(namespace_root.st_mode)) {
        *error_number = ENOTDIR;
        return false;
    }

    const std::size_t final_separator = path.rfind('/');
    std::string output_basename =
        path.substr(final_separator + 1U);
    if (l2flow::ops::
            IsSdkLogDirectoryMarkerFilename(
                output_basename) ||
        l2flow::ops::
            IsPrometheusTextfileLeaseFilename(
                output_basename)) {
        *error_number = EINVAL;
        return false;
    }
    std::size_t component_start = 1U;
    while (component_start < final_separator) {
        const std::size_t separator =
            path.find('/', component_start);
        const std::string component =
            path.substr(
                component_start,
                separator - component_start);
        int child_fd = -1;
        do {
            child_fd = ::openat(
                current.get(),
                component.c_str(),
                kDirectoryFlags);
        } while (child_fd < 0 && errno == EINTR);
        if (child_fd < 0) {
            *error_number = errno;
            return false;
        }
        if (!ValidateAncestorTransition(
                current.get(),
                child_fd,
                namespace_root.st_uid,
                error_number)) {
            static_cast<void>(::close(child_fd));
            return false;
        }
        current.Reset(child_fd);
        component_start = separator + 1U;
    }

    struct stat directory_status {};
    if (::fstat(current.get(), &directory_status) != 0) {
        *error_number = errno;
        return false;
    }
    if (!S_ISDIR(directory_status.st_mode)) {
        *error_number = ENOTDIR;
        return false;
    }
    if (directory_status.st_uid != ::geteuid() ||
        (directory_status.st_mode &
         (S_IWGRP | S_IWOTH)) != 0) {
        *error_number = EACCES;
        return false;
    }
    struct stat sdk_log_marker {};
    if (::fstatat(
            current.get(),
            l2flow::ops::kSdkLogDirectoryMarkerFilename.data(),
            &sdk_log_marker,
            AT_SYMLINK_NOFOLLOW) == 0) {
        *error_number = EACCES;
        return false;
    }
    if (errno != ENOENT) {
        *error_number = errno;
        return false;
    }

    *basename = std::move(output_basename);
    *directory = std::move(current);
    return true;
}

bool HasShadowCaptureFileMagic(
    int fd,
    int* error_number) noexcept {
    std::array<char, kShadowCaptureFileMagic.size()>
        observed{};
    std::size_t offset = 0U;
    while (offset < observed.size()) {
        const ssize_t count =
            ::pread(
                fd,
                observed.data() + offset,
                observed.size() - offset,
                static_cast<off_t>(offset));
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count < 0) {
            *error_number = errno;
            return false;
        }
        if (count == 0) {
            *error_number = EINVAL;
            return false;
        }
        offset += static_cast<std::size_t>(count);
    }
    if (!std::equal(
            observed.begin(),
            observed.end(),
            kShadowCaptureFileMagic.begin())) {
        *error_number = EINVAL;
        return false;
    }
    return true;
}

std::unique_ptr<ShadowCaptureOutput> OpenPosixOutput(
    const std::string& path,
    int* error_number) {
    constexpr mode_t kShadowFileMode =
        S_IRUSR | S_IWUSR;
    ScopedFd directory;
    std::string basename;
    if (!OpenStableParentDirectory(
            path, &directory, &basename, error_number)) {
        return nullptr;
    }

    constexpr int kFileFlags =
        O_RDWR | O_CREAT | O_NONBLOCK | O_NOCTTY |
        O_CLOEXEC | O_NOFOLLOW;
    int fd = -1;
    do {
        fd = ::openat(
            directory.get(),
            basename.c_str(),
            kFileFlags | O_EXCL,
            kShadowFileMode);
    } while (fd < 0 && errno == EINTR);
    const bool created = fd >= 0;
    if (fd < 0 && errno == EEXIST) {
        do {
            fd = ::openat(
                directory.get(),
                basename.c_str(),
                kFileFlags & ~O_CREAT);
        } while (fd < 0 && errno == EINTR);
    }
    if (fd < 0) {
        *error_number = errno;
        return nullptr;
    }
    ScopedFd output(fd);

    struct stat status {};
    if (::fstat(output.get(), &status) != 0) {
        *error_number = errno;
        return nullptr;
    }
    if (!S_ISREG(status.st_mode) ||
        status.st_nlink != 1) {
        *error_number = EINVAL;
        return nullptr;
    }
    if (status.st_uid != ::geteuid()) {
        *error_number = EACCES;
        return nullptr;
    }
    const mode_t actual_mode = status.st_mode & 07777;
    if ((!created && actual_mode != kShadowFileMode) ||
        (created &&
         ::fchmod(output.get(), kShadowFileMode) != 0)) {
        *error_number = created ? errno : EACCES;
        return nullptr;
    }
    int lock_result = 0;
    do {
        lock_result =
            ::flock(output.get(), LOCK_EX | LOCK_NB);
    } while (lock_result != 0 && errno == EINTR);
    if (lock_result != 0) {
        *error_number = errno;
        return nullptr;
    }
    struct stat locked_status {};
    struct stat named_status {};
    if (::fstat(output.get(), &locked_status) != 0 ||
        ::fstatat(
            directory.get(),
            basename.c_str(),
            &named_status,
            AT_SYMLINK_NOFOLLOW) != 0) {
        *error_number = errno;
        return nullptr;
    }
    if (!S_ISREG(locked_status.st_mode) ||
        !S_ISREG(named_status.st_mode) ||
        locked_status.st_uid != ::geteuid() ||
        named_status.st_uid != ::geteuid() ||
        locked_status.st_nlink != 1 ||
        named_status.st_nlink != 1 ||
        (locked_status.st_mode & 07777) !=
            kShadowFileMode ||
        (named_status.st_mode & 07777) !=
            kShadowFileMode ||
        locked_status.st_dev != named_status.st_dev ||
        locked_status.st_ino != named_status.st_ino) {
        *error_number = EACCES;
        return nullptr;
    }
    if (!created &&
        !HasShadowCaptureFileMagic(
            output.get(),
            error_number)) {
        return nullptr;
    }
    int truncate_result = 0;
    do {
        truncate_result = ::ftruncate(output.get(), 0);
    } while (truncate_result != 0 && errno == EINTR);
    if (truncate_result != 0) {
        *error_number = errno;
        return nullptr;
    }

    return std::make_unique<PosixShadowCaptureOutput>(
        output.Release());
}

bool CheckedAddSize(std::size_t left,
                    std::size_t right,
                    std::size_t* result) noexcept {
    if (left > std::numeric_limits<std::size_t>::max() - right) {
        return false;
    }
    *result = left + right;
    return true;
}

bool CheckedAddU64(std::uint64_t left,
                   std::uint64_t right,
                   std::uint64_t* result) noexcept {
    if (left > std::numeric_limits<std::uint64_t>::max() - right) {
        return false;
    }
    *result = left + right;
    return true;
}

bool LoadU32(std::span<const std::byte> bytes,
             std::size_t offset,
             std::uint32_t* value) noexcept {
    if (offset > bytes.size() ||
        sizeof(std::uint32_t) > bytes.size() - offset) {
        return false;
    }
    *value =
        std::to_integer<std::uint32_t>(bytes[offset]) |
        (std::to_integer<std::uint32_t>(bytes[offset + 1U]) << 8U) |
        (std::to_integer<std::uint32_t>(bytes[offset + 2U]) << 16U) |
        (std::to_integer<std::uint32_t>(bytes[offset + 3U]) << 24U);
    return true;
}

bool LoadU16(std::span<const std::byte> bytes,
             std::size_t offset,
             std::uint16_t* value) noexcept {
    if (offset > bytes.size() ||
        sizeof(std::uint16_t) > bytes.size() - offset) {
        return false;
    }
    *value =
        std::to_integer<std::uint16_t>(bytes[offset]) |
        static_cast<std::uint16_t>(
            std::to_integer<std::uint16_t>(bytes[offset + 1U])
            << 8U);
    return true;
}

struct CheckedListRange final {
    std::size_t start = 0U;
    std::size_t count = 0U;
};

bool ReadListRange(std::span<const std::byte> bytes,
                   std::size_t descriptor_offset,
                   std::size_t item_bytes,
                   std::size_t maximum_count,
                   CheckedListRange* range) noexcept {
    std::uint32_t length = 0U;
    std::uint32_t relative_offset = 0U;
    std::size_t relative_offset_field = 0U;
    if (item_bytes == 0U ||
        !CheckedAddSize(descriptor_offset,
                        sizeof(std::uint32_t),
                        &relative_offset_field) ||
        !LoadU32(bytes, descriptor_offset, &length) ||
        !LoadU32(bytes,
                 relative_offset_field,
                 &relative_offset)) {
        return false;
    }
    range->count = static_cast<std::size_t>(length);
    if (range->count == 0U) {
        if (relative_offset == 0U) {
            range->start = descriptor_offset;
            return true;
        }
        if (!CheckedAddSize(
                descriptor_offset,
                static_cast<std::size_t>(relative_offset),
                &range->start) ||
            range->start > bytes.size()) {
            return false;
        }
        return true;
    }
    if (range->count > maximum_count ||
        relative_offset < kMdlListBytes) {
        return false;
    }

    std::size_t start = 0U;
    std::size_t total_bytes = 0U;
    if (!CheckedAddSize(
            descriptor_offset,
            static_cast<std::size_t>(relative_offset),
            &start) ||
        range->count >
            std::numeric_limits<std::size_t>::max() / item_bytes) {
        return false;
    }
    total_bytes = range->count * item_bytes;
    if (start > bytes.size() ||
        total_bytes > bytes.size() - start) {
        return false;
    }
    range->start = start;
    return true;
}

bool ValidateStringRange(std::span<const std::byte> bytes,
                         std::size_t descriptor_offset,
                         std::size_t minimum_data_start) noexcept {
    std::uint16_t length = 0U;
    std::uint32_t relative_offset = 0U;
    std::size_t offset_field = 0U;
    if (!CheckedAddSize(
            descriptor_offset,
            kMdlStringOffsetFieldOffset,
            &offset_field) ||
        !LoadU16(bytes, descriptor_offset, &length) ||
        !LoadU32(bytes, offset_field, &relative_offset)) {
        return false;
    }

    if (length == 0U && relative_offset == 0U) {
        return true;
    }

    std::size_t start = 0U;
    if (!CheckedAddSize(
            descriptor_offset,
            static_cast<std::size_t>(relative_offset),
            &start) ||
        start > bytes.size()) {
        return false;
    }
    if (length == 0U) {
        return true;
    }
    return relative_offset >= kMdlStringBytes &&
           start >= minimum_data_start &&
           static_cast<std::size_t>(length) <=
               bytes.size() - start;
}

bool ValidateStringFields(
    std::span<const std::byte> body,
    std::size_t fixed_body_bytes,
    std::span<const std::size_t> descriptor_offsets) noexcept {
    for (const std::size_t descriptor_offset :
         descriptor_offsets) {
        if (!ValidateStringRange(
                body, descriptor_offset, fixed_body_bytes)) {
            return false;
        }
    }
    return true;
}

bool ValidateNestedListFields(
    std::span<const std::byte> body,
    std::size_t fixed_body_bytes,
    std::size_t top_descriptor_offset,
    std::size_t top_item_bytes,
    std::size_t nested_descriptor_in_item,
    std::size_t nested_item_bytes,
    std::size_t* aggregate_nested_items) noexcept {
    if (top_item_bytes == 0U || nested_item_bytes == 0U) {
        return false;
    }
    CheckedListRange top;
    if (!ReadListRange(
            body,
            top_descriptor_offset,
            top_item_bytes,
            body.size() / top_item_bytes,
            &top)) {
        return false;
    }
    if (top.count == 0U) {
        return true;
    }
    if (top.start < fixed_body_bytes ||
        top.count >
            std::numeric_limits<std::size_t>::max() /
                top_item_bytes) {
        return false;
    }
    std::size_t top_end = 0U;
    if (!CheckedAddSize(
            top.start,
            top.count * top_item_bytes,
            &top_end)) {
        return false;
    }

    const std::size_t nested_budget =
        body.size() / nested_item_bytes;
    if (*aggregate_nested_items > nested_budget) {
        return false;
    }
    for (std::size_t index = 0U; index < top.count; ++index) {
        const std::size_t item_offset =
            top.start + index * top_item_bytes;
        std::size_t nested_descriptor = 0U;
        if (!CheckedAddSize(
                item_offset,
                nested_descriptor_in_item,
                &nested_descriptor)) {
            return false;
        }
        CheckedListRange nested;
        if (!ReadListRange(
                body,
                nested_descriptor,
                nested_item_bytes,
                nested_budget,
                &nested)) {
            return false;
        }
        if (nested.count != 0U && nested.start < top_end) {
            return false;
        }
        if (nested.count >
            nested_budget - *aggregate_nested_items) {
            return false;
        }
        *aggregate_nested_items += nested.count;
    }
    return true;
}

bool ValidateShSnapshotBody(
    std::span<const std::byte> body) noexcept {
    constexpr std::array<std::size_t, 2U> strings{
        offsetof(sh::SHL2MarketData, SecurityID),
        offsetof(sh::SHL2MarketData, InstruStatus),
    };
    if (body.size() < sizeof(sh::SHL2MarketData) ||
        !ValidateStringFields(
            body, sizeof(sh::SHL2MarketData), strings)) {
        return false;
    }

    std::size_t aggregate_nested_items = 0U;
    return ValidateNestedListFields(
               body,
               sizeof(sh::SHL2MarketData),
               offsetof(sh::SHL2MarketData, BidLevels),
               sizeof(sh::SHL2MarketData::BidLevelsItem),
               offsetof(
                   sh::SHL2MarketData::BidLevelsItem,
                   NOrders),
               sizeof(
                   sh::SHL2MarketData::BidLevelsItem::
                       NOrdersItem),
               &aggregate_nested_items) &&
           ValidateNestedListFields(
               body,
               sizeof(sh::SHL2MarketData),
               offsetof(sh::SHL2MarketData, SellLevels),
               sizeof(sh::SHL2MarketData::SellLevelsItem),
               offsetof(
                   sh::SHL2MarketData::SellLevelsItem,
                   NoOrders),
               sizeof(
                   sh::SHL2MarketData::SellLevelsItem::
                       NoOrdersItem),
               &aggregate_nested_items);
}

bool ValidateShTickBody(
    std::span<const std::byte> body) noexcept {
    constexpr std::array<std::size_t, 3U> strings{
        offsetof(sh::NGTSTick, SecurityID),
        offsetof(sh::NGTSTick, Type),
        offsetof(sh::NGTSTick, TickBSFlag),
    };
    return body.size() >= sizeof(sh::NGTSTick) &&
           ValidateStringFields(
               body, sizeof(sh::NGTSTick), strings);
}

bool ValidateSzSnapshotBody(
    std::span<const std::byte> body) noexcept {
    constexpr std::array<std::size_t, 4U> strings{
        offsetof(sz::Snapshot300111_v2, MDStreamID),
        offsetof(sz::Snapshot300111_v2, SecurityID),
        offsetof(sz::Snapshot300111_v2, SecurityIDSource),
        offsetof(sz::Snapshot300111_v2, TradingPhaseCode),
    };
    if (body.size() < sizeof(sz::Snapshot300111_v2) ||
        !ValidateStringFields(
            body, sizeof(sz::Snapshot300111_v2), strings)) {
        return false;
    }

    std::size_t aggregate_nested_items = 0U;
    return ValidateNestedListFields(
               body,
               sizeof(sz::Snapshot300111_v2),
               offsetof(
                   sz::Snapshot300111_v2,
                   BidPriceLevel),
               sizeof(
                   sz::Snapshot300111_v2::
                       BidPriceLevelItem),
               offsetof(
                   sz::Snapshot300111_v2::
                       BidPriceLevelItem,
                   Orders),
               sizeof(
                   sz::Snapshot300111_v2::
                       BidPriceLevelItem::OrdersItem),
               &aggregate_nested_items) &&
           ValidateNestedListFields(
               body,
               sizeof(sz::Snapshot300111_v2),
               offsetof(
                   sz::Snapshot300111_v2,
                   AskPriceLevel),
               sizeof(
                   sz::Snapshot300111_v2::
                       AskPriceLevelItem),
               offsetof(
                   sz::Snapshot300111_v2::
                       AskPriceLevelItem,
                   Orders),
               sizeof(
                   sz::Snapshot300111_v2::
                       AskPriceLevelItem::OrdersItem),
               &aggregate_nested_items);
}

bool ValidateSzOrderBody(
    std::span<const std::byte> body) noexcept {
    constexpr std::array<std::size_t, 3U> strings{
        offsetof(sz::Order300192_v2, MDStreamID),
        offsetof(sz::Order300192_v2, SecurityID),
        offsetof(sz::Order300192_v2, SecurityIDSource),
    };
    return body.size() >= sizeof(sz::Order300192_v2) &&
           ValidateStringFields(
               body, sizeof(sz::Order300192_v2), strings);
}

bool ValidateSzTransactionBody(
    std::span<const std::byte> body) noexcept {
    constexpr std::array<std::size_t, 3U> strings{
        offsetof(sz::Transaction300191_v2, MDStreamID),
        offsetof(sz::Transaction300191_v2, SecurityID),
        offsetof(
            sz::Transaction300191_v2,
            SecurityIDSource),
    };
    return body.size() >= sizeof(sz::Transaction300191_v2) &&
           ValidateStringFields(
               body, sizeof(sz::Transaction300191_v2), strings);
}

bool ValidateRequiredMarketBody(
    const l2flow::sdk::MessageKey& key,
    std::span<const std::byte> body) noexcept {
    if (key == kShSnapshotKey) {
        return ValidateShSnapshotBody(body);
    }
    if (key == kShTickKey) {
        return ValidateShTickBody(body);
    }
    if (key == kSzSnapshotKey) {
        return ValidateSzSnapshotBody(body);
    }
    if (key == kSzOrderKey) {
        return ValidateSzOrderBody(body);
    }
    if (key == kSzTransactionKey) {
        return ValidateSzTransactionBody(body);
    }
    return false;
}

}  // namespace

ShadowCaptureWriter::ShadowCaptureWriter(
    ShadowCaptureConfig config,
    ByteRing& ring,
    l2flow::ops::FatalLatch& fatal,
    const std::string& output_path)
    : config_(std::move(config)),
      ring_(ring),
      fatal_(fatal),
      record_(ring.max_body_bytes()) {
    ValidateConfig();
    if (output_path.empty() ||
        output_path.find('\0') != std::string::npos) {
        throw std::invalid_argument("invalid shadow output path");
    }

    int open_error = 0;
    output_ = OpenPosixOutput(output_path, &open_error);
    if (output_ == nullptr) {
        static_cast<void>(open_error);
        TripOutputFailure();
    }
}

ShadowCaptureWriter::ShadowCaptureWriter(
    ShadowCaptureConfig config,
    ByteRing& ring,
    l2flow::ops::FatalLatch& fatal,
    std::unique_ptr<ShadowCaptureOutput> output)
    : config_(std::move(config)),
      ring_(ring),
      fatal_(fatal),
      output_(std::move(output)),
      record_(ring.max_body_bytes()) {
    ValidateConfig();
    if (output_ == nullptr) {
        throw std::invalid_argument("shadow output is null");
    }
}

ShadowCaptureWriter::~ShadowCaptureWriter() {
    StopAndDrain();
    if (!output_closed_) {
        static_cast<void>(FinalizeOutput());
    }
}

bool ShadowCaptureWriter::Run() noexcept {
    bool expected = false;
    if (!run_started_.compare_exchange_strong(
            expected,
            true,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        return false;
    }

    bool success = output_ != nullptr;
    if (success) {
        success = WriteFileHeader();
    }
    startup_succeeded_.store(success, std::memory_order_relaxed);
    startup_complete_.store(true, std::memory_order_release);

    try {
        while (success) {
            ByteRingPopResult result = ring_.try_pop(record_);
            if (result == ByteRingPopResult::EMPTY) {
                if (!stop_requested_.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                    continue;
                }

                // The stop acquire synchronizes with the owner after producer
                // quiescence.  Re-read the independent ring publication after
                // that acquire before declaring the drain complete.
                result = ring_.try_pop(record_);
                if (result == ByteRingPopResult::EMPTY) {
                    break;
                }
            }

            if (result == ByteRingPopResult::CORRUPT ||
                result == ByteRingPopResult::OUTPUT_TOO_SMALL) {
                TripRingCorruption();
                success = false;
                break;
            }
            if (result != ByteRingPopResult::RECORD) {
                TripRingCorruption();
                success = false;
                break;
            }
            if (!ValidateRecord(record_) || !WriteRecord(record_)) {
                success = false;
                break;
            }
        }
    } catch (...) {
        TripRingCorruption();
        success = false;
    }

    const bool finalized = FinalizeOutput();
    success = success && finalized;
    finished_.store(true, std::memory_order_release);
    return success;
}

void ShadowCaptureWriter::StopAndDrain() noexcept {
    stop_requested_.store(true, std::memory_order_release);
}

bool ShadowCaptureWriter::stop_requested() const noexcept {
    return stop_requested_.load(std::memory_order_acquire);
}

bool ShadowCaptureWriter::startup_complete() const noexcept {
    return startup_complete_.load(std::memory_order_acquire);
}

bool ShadowCaptureWriter::startup_succeeded() const noexcept {
    return startup_complete() &&
           startup_succeeded_.load(std::memory_order_relaxed);
}

bool ShadowCaptureWriter::finished() const noexcept {
    return finished_.load(std::memory_order_acquire);
}

ShadowCaptureStats ShadowCaptureWriter::Snapshot() const noexcept {
    ShadowCaptureStats result;
    // sink_records is the per-record release publication.  An acquire here
    // makes the associated byte/observation counter updates visible.
    result.sink_records =
        sink_records_.load(std::memory_order_acquire);
    result.sink_vendor_bytes =
        sink_vendor_bytes_.load(std::memory_order_relaxed);
    result.sink_file_bytes =
        sink_file_bytes_.load(std::memory_order_relaxed);
    result.last_ingress_sequence =
        last_ingress_sequence_.load(std::memory_order_relaxed);
    result.logon_response_headers =
        logon_response_headers_.load(std::memory_order_relaxed);
    result.subscribe_response_headers =
        subscribe_response_headers_.load(std::memory_order_relaxed);
    result.logon_ok_responses =
        logon_ok_responses_.load(std::memory_order_relaxed);
    result.logon_failed_responses =
        logon_failed_responses_.load(std::memory_order_relaxed);
    result.malformed_control_responses =
        malformed_control_responses_.load(std::memory_order_relaxed);
    result.required_first_seen_mask =
        required_first_seen_mask_.load(std::memory_order_relaxed);
    result.required_subscription_ok_mask =
        required_subscription_ok_mask_.load(std::memory_order_relaxed);
    result.required_subscription_failed_mask =
        required_subscription_failed_mask_.load(
            std::memory_order_relaxed);
    result.required_subscription_failure_observed_mask =
        required_subscription_failure_observed_mask_.load(
            std::memory_order_relaxed);
    result.readiness_generation =
        readiness_generation_.load(std::memory_order_relaxed);
    result.latest_logon_ok =
        latest_logon_ok_.load(std::memory_order_relaxed);
    return result;
}

ShadowCaptureReconciliation ShadowCaptureWriter::Reconcile(
    const CaptureMetricsSnapshot& callback) const noexcept {
    const ShadowCaptureStats sink = Snapshot();
    ShadowCaptureReconciliation result;
    result.callback_records = callback.captured_records;
    result.sink_records = sink.sink_records;
    result.callback_vendor_bytes =
        callback.captured_vendor_bytes;
    result.sink_vendor_bytes = sink.sink_vendor_bytes;
    return result;
}

bool ShadowCaptureWriter::all_required_market_seen() const noexcept {
    return required_first_seen_mask_.load(std::memory_order_acquire) ==
           required_mask_;
}

bool ShadowCaptureWriter::latest_logon_ok() const noexcept {
    return latest_logon_ok_.load(std::memory_order_acquire);
}

bool ShadowCaptureWriter::all_required_subscriptions_ok() const noexcept {
    return required_subscription_ok_mask_.load(
               std::memory_order_acquire) == required_mask_;
}

std::uint64_t
ShadowCaptureWriter::required_market_mask() const noexcept {
    return required_mask_;
}

std::uint64_t
ShadowCaptureWriter::readiness_generation() const noexcept {
    return readiness_generation_.load(std::memory_order_acquire);
}

void ShadowCaptureWriter::ValidateConfig() {
    if (config_.source_stream_id == 0U ||
        config_.market_service_id == 0U ||
        config_.market_service_id == kApiServiceId ||
        config_.market_service_id == kSystemServiceId ||
        ring_.max_message_bytes() < kVendorMessageHeadBytes ||
        config_.required_market_messages.empty() ||
        config_.required_market_messages.size() > 64U) {
        throw std::invalid_argument(
            "invalid shadow capture configuration");
    }

    for (std::size_t index = 0U;
         index < config_.required_market_messages.size();
         ++index) {
        const l2flow::sdk::MessageKey& key =
            config_.required_market_messages[index];
        if (key.service_id != config_.market_service_id ||
            key.service_version == 0U || key.message_id == 0U) {
            throw std::invalid_argument(
                "required shadow message is not a market message");
        }
        for (std::size_t prior = 0U; prior < index; ++prior) {
            if (key == config_.required_market_messages[prior]) {
                throw std::invalid_argument(
                    "duplicate required shadow message");
            }
        }
    }

    required_mask_ =
        config_.required_market_messages.size() == 64U
            ? std::numeric_limits<std::uint64_t>::max()
            : ((std::uint64_t{1U}
                << config_.required_market_messages.size()) -
               1U);
}

bool ShadowCaptureWriter::WriteFileHeader() noexcept {
    ShadowCaptureFileHeaderV1 header{};
    header.record_header_bytes =
        static_cast<std::uint32_t>(
            sizeof(ShadowCaptureRecordHeaderV1));
    header.capture_meta_bytes =
        static_cast<std::uint32_t>(sizeof(CaptureMetaV1));
    header.vendor_head_bytes =
        static_cast<std::uint32_t>(kVendorMessageHeadBytes);
    header.source_stream_id = config_.source_stream_id;
    header.max_message_bytes = ring_.max_message_bytes();
    header.required_market_count =
        static_cast<std::uint32_t>(
            config_.required_market_messages.size());

    return WriteAll(std::as_bytes(
        std::span<const ShadowCaptureFileHeaderV1>(&header, 1U)));
}

bool ShadowCaptureWriter::ValidateRecord(
    const ByteRingRecord& record) noexcept {
    if (record.meta.source_stream_id != config_.source_stream_id ||
        record.meta.ingress_sequence == 0U ||
        (previous_ingress_sequence_ != 0U &&
         (previous_ingress_sequence_ ==
              std::numeric_limits<std::uint64_t>::max() ||
          record.meta.ingress_sequence !=
              previous_ingress_sequence_ + 1U))) {
        TripRingCorruption();
        return false;
    }

    const l2flow::sdk::VendorHeadView head(record.head);
    std::size_t expected_message_bytes = 0U;
    if (!CheckedAddSize(kVendorMessageHeadBytes,
                        record.body.size(),
                        &expected_message_bytes) ||
        expected_message_bytes > ring_.max_message_bytes() ||
        expected_message_bytes >
            std::numeric_limits<std::uint32_t>::max() ||
        head.head_size() != kVendorMessageHeadBytes ||
        head.message_size() != expected_message_bytes ||
        (head.service_id() != kApiServiceId &&
         head.service_id() != kSystemServiceId &&
         head.service_id() != config_.market_service_id)) {
        TripRingCorruption();
        return false;
    }
    return true;
}

bool ShadowCaptureWriter::WriteRecord(
    const ByteRingRecord& record) noexcept {
    std::size_t unaligned_record_bytes = 0U;
    std::size_t vendor_message_bytes = 0U;
    if (!CheckedAddSize(kVendorMessageHeadBytes,
                        record.body.size(),
                        &vendor_message_bytes) ||
        !CheckedAddSize(sizeof(ShadowCaptureRecordHeaderV1),
                        vendor_message_bytes,
                        &unaligned_record_bytes) ||
        unaligned_record_bytes >
            std::numeric_limits<std::size_t>::max() -
                (kShadowCaptureRecordAlignment - 1U)) {
        TripRingCorruption();
        return false;
    }

    const std::size_t record_bytes =
        (unaligned_record_bytes +
         (kShadowCaptureRecordAlignment - 1U)) &
        ~(kShadowCaptureRecordAlignment - 1U);
    const std::size_t padding_bytes =
        record_bytes - unaligned_record_bytes;
    if (vendor_message_bytes >
            std::numeric_limits<std::uint32_t>::max() ||
        padding_bytes >
            std::numeric_limits<std::uint32_t>::max()) {
        TripRingCorruption();
        return false;
    }

    const std::uint64_t completed_records =
        sink_records_.load(std::memory_order_relaxed);
    if (completed_records ==
        std::numeric_limits<std::uint64_t>::max()) {
        TripRingCorruption();
        return false;
    }
    const std::uint64_t current_vendor_bytes =
        sink_vendor_bytes_.load(std::memory_order_relaxed);
    std::uint64_t next_vendor_bytes = 0U;
    if (!CheckedAddU64(
            current_vendor_bytes,
            static_cast<std::uint64_t>(vendor_message_bytes),
            &next_vendor_bytes)) {
        TripRingCorruption();
        return false;
    }
    const std::uint64_t current_file_bytes =
        sink_file_bytes_.load(std::memory_order_relaxed);
    std::uint64_t next_file_bytes = 0U;
    if (!CheckedAddU64(
            current_file_bytes,
            static_cast<std::uint64_t>(record_bytes),
            &next_file_bytes)) {
        TripOutputFailure();
        return false;
    }

    ShadowCaptureRecordHeaderV1 header{};
    header.record_bytes = static_cast<std::uint64_t>(record_bytes);
    header.sink_record_index = completed_records + 1U;
    header.meta = record.meta;
    header.vendor_message_bytes =
        static_cast<std::uint32_t>(vendor_message_bytes);
    header.padding_bytes =
        static_cast<std::uint32_t>(padding_bytes);

    if (!WriteAll(std::as_bytes(
            std::span<const ShadowCaptureRecordHeaderV1>(
                &header, 1U))) ||
        !WriteAll(std::span<const std::byte>(
            record.head.data(), record.head.size())) ||
        !WriteAll(std::span<const std::byte>(
            record.body.data(), record.body.size()))) {
        return false;
    }

    constexpr std::array<std::byte,
                         kShadowCaptureRecordAlignment>
        zero_padding{};
    if (!WriteAll(std::span<const std::byte>(
            zero_padding.data(), padding_bytes))) {
        return false;
    }
    if (sink_file_bytes_.load(std::memory_order_relaxed) !=
        next_file_bytes) {
        TripOutputFailure();
        return false;
    }

    const bool observation_ok = ObserveRecord(record);
    previous_ingress_sequence_ = record.meta.ingress_sequence;
    sink_vendor_bytes_.store(next_vendor_bytes,
                             std::memory_order_relaxed);
    last_ingress_sequence_.store(record.meta.ingress_sequence,
                                 std::memory_order_relaxed);
    sink_records_.store(completed_records + 1U,
                        std::memory_order_release);
    return observation_ok;
}

bool ShadowCaptureWriter::ObserveRecord(
    const ByteRingRecord& record) noexcept {
    const l2flow::sdk::VendorHeadView head(record.head);
    const l2flow::sdk::MessageKey key{
        head.service_id(),
        head.service_version(),
        head.message_id(),
    };

    if (key.service_id == kSystemServiceId) {
        if (key.message_id == kLogonResponseMessageId) {
            // Invalidate readiness before publishing the replacement
            // generation. This prevents a monitor from attributing old
            // connection facts to the new LogonResponse.
            readiness_generation_.store(
                0U, std::memory_order_release);
            const std::uint64_t previous_generation =
                logon_response_headers_.load(
                    std::memory_order_relaxed);
            if (previous_generation ==
                std::numeric_limits<std::uint64_t>::max()) {
                malformed_control_responses_.fetch_add(
                    1U, std::memory_order_relaxed);
                static_cast<void>(fatal_.trip(
                    l2flow::ops::FatalReason::
                        MALFORMED_CONTROL_MESSAGE));
                return false;
            }
            current_logon_generation_ =
                previous_generation + 1U;
            logon_response_headers_.store(
                current_logon_generation_,
                std::memory_order_relaxed);
            // A logon response begins a new observation generation, including
            // malformed and rejected responses. Clear every prior connection
            // readiness fact before inspecting the new body.
            latest_logon_ok_.store(
                false, std::memory_order_release);
            required_first_seen_mask_.store(
                0U, std::memory_order_release);
            required_subscription_failed_mask_.store(
                0U, std::memory_order_relaxed);
            required_subscription_ok_mask_.store(
                0U, std::memory_order_release);
            if (key.service_version !=
                kSystemServiceVersion) {
                malformed_control_responses_.fetch_add(
                    1U, std::memory_order_relaxed);
                static_cast<void>(fatal_.trip(
                    l2flow::ops::FatalReason::
                        MALFORMED_CONTROL_MESSAGE));
                return false;
            }
            std::uint32_t return_code = 0U;
            if (record.body.size() < kLogonResponseBytes ||
                !LoadU32(record.body,
                         kLogonReturnCodeOffset,
                         &return_code)) {
                malformed_control_responses_.fetch_add(
                    1U, std::memory_order_relaxed);
                static_cast<void>(fatal_.trip(
                    l2flow::ops::FatalReason::
                        MALFORMED_CONTROL_MESSAGE));
                return false;
            }
            const bool accepted =
                return_code ==
                static_cast<std::uint32_t>(
                    datayes::mdl::MDLEC_OK);

            if (!ObserveSubscriptionStatuses(
                    record.body,
                    kLogonServicesListOffset,
                    kLogonResponseBytes,
                    accepted)) {
                malformed_control_responses_.fetch_add(
                    1U, std::memory_order_relaxed);
                static_cast<void>(fatal_.trip(
                    l2flow::ops::FatalReason::
                        MALFORMED_CONTROL_MESSAGE));
                return false;
            }
            latest_logon_ok_.store(
                accepted, std::memory_order_release);
            if (accepted) {
                logon_ok_responses_.fetch_add(
                    1U, std::memory_order_relaxed);
            } else {
                logon_failed_responses_.fetch_add(
                    1U, std::memory_order_relaxed);
            }
            RefreshReadinessGeneration();
        } else if (
            key.message_id == kSubscribeResponseMessageId) {
            subscribe_response_headers_.fetch_add(
                1U, std::memory_order_relaxed);
            if (key.service_version !=
                kSystemServiceVersion) {
                malformed_control_responses_.fetch_add(
                    1U, std::memory_order_relaxed);
                static_cast<void>(fatal_.trip(
                    l2flow::ops::FatalReason::
                        MALFORMED_CONTROL_MESSAGE));
                return false;
            }
            if (!ObserveSubscriptionStatuses(
                    record.body,
                    kSubscribeServicesListOffset,
                    kMdlListBytes,
                    true)) {
                malformed_control_responses_.fetch_add(
                    1U, std::memory_order_relaxed);
                static_cast<void>(fatal_.trip(
                    l2flow::ops::FatalReason::
                        MALFORMED_CONTROL_MESSAGE));
                return false;
            }
        }
    }

    if (key.service_id != config_.market_service_id) {
        return true;
    }
    for (std::size_t index = 0U;
         index < config_.required_market_messages.size();
         ++index) {
        if (key == config_.required_market_messages[index]) {
            const std::optional<std::size_t> fixed_body_bytes =
                l2flow::sdk::RequiredMessageFixedBodyBytes(key);
            if (!fixed_body_bytes.has_value() ||
                record.body.size() < *fixed_body_bytes ||
                !ValidateRequiredMarketBody(
                    key, record.body)) {
                // Preserve the callback fact in the shadow file, but do not
                // call a truncated or offset-invalid body a legal readiness
                // record.
                return true;
            }
            required_first_seen_mask_.fetch_or(
                std::uint64_t{1U} << index,
                std::memory_order_release);
            RefreshReadinessGeneration();
            return true;
        }
    }
    return true;
}

bool ShadowCaptureWriter::ObserveSubscriptionStatuses(
    std::span<const std::byte> body,
    std::size_t services_list_offset,
    std::size_t minimum_services_start,
    bool apply_statuses) noexcept {
    constexpr std::size_t kMaximumServices = 4096U;
    constexpr std::size_t kMaximumMessagesPerService = 1'000'000U;
    CheckedListRange services;
    if (!ReadListRange(body,
                       services_list_offset,
                       kServiceItemBytes,
                       kMaximumServices,
                       &services)) {
        return false;
    }
    std::size_t services_end = 0U;
    if (services.count >
            std::numeric_limits<std::size_t>::max() /
                kServiceItemBytes ||
        !CheckedAddSize(
            services.start,
            services.count * kServiceItemBytes,
            &services_end) ||
        (services.count != 0U &&
         services.start < minimum_services_start)) {
        return false;
    }

    std::uint64_t seen_mask = 0U;
    std::uint64_t ok_mask = 0U;
    std::uint64_t failed_mask = 0U;
    // Nested MDLList offsets are relative and can overlap. Bound aggregate
    // iteration by the number of complete status items physically present in
    // the body so thousands of descriptors cannot all point at one giant
    // list and multiply work far beyond input size.
    const std::size_t aggregate_message_budget =
        body.size() / kMessageStatusItemBytes;
    std::size_t aggregate_messages = 0U;
    for (std::size_t service_index = 0U;
         service_index < services.count;
         ++service_index) {
        const std::size_t service_offset =
            services.start + service_index * kServiceItemBytes;
        std::uint32_t service_id = 0U;
        std::uint32_t service_version = 0U;
        if (!LoadU32(body,
                     service_offset + kServiceIdOffset,
                     &service_id) ||
            !LoadU32(body,
                     service_offset + kServiceVersionOffset,
                     &service_version)) {
            return false;
        }
        const std::size_t messages_descriptor =
            service_offset + kServiceMessagesListOffset;
        CheckedListRange messages;
        if (!ReadListRange(body,
                           messages_descriptor,
                           kMessageStatusItemBytes,
                           kMaximumMessagesPerService,
                           &messages)) {
            return false;
        }
        if (messages.count != 0U &&
            messages.start < services_end) {
            return false;
        }
        if (messages.count >
            aggregate_message_budget - aggregate_messages) {
            return false;
        }
        aggregate_messages += messages.count;

        for (std::size_t message_index = 0U;
             message_index < messages.count;
             ++message_index) {
            const std::size_t message_offset =
                messages.start +
                message_index * kMessageStatusItemBytes;
            std::uint32_t message_id = 0U;
            std::uint32_t status = 0U;
            if (!LoadU32(body,
                         message_offset + kMessageIdOffset,
                         &message_id) ||
                !LoadU32(body,
                         message_offset + kMessageStatusOffset,
                         &status)) {
                return false;
            }

            for (std::size_t required_index = 0U;
                 required_index <
                     config_.required_market_messages.size();
                 ++required_index) {
                const l2flow::sdk::MessageKey& required =
                    config_.required_market_messages[required_index];
                if (service_id != required.service_id ||
                    service_version != required.service_version ||
                    message_id != required.message_id) {
                    continue;
                }
                const std::uint64_t bit =
                    std::uint64_t{1U} << required_index;
                if ((seen_mask & bit) != 0U) {
                    return false;
                }
                seen_mask |= bit;
                if (status ==
                    static_cast<std::uint32_t>(
                        datayes::mdl::MDLEC_OK)) {
                    ok_mask |= bit;
                } else {
                    failed_mask |= bit;
                }
                break;
            }
        }
    }

    if (apply_statuses) {
        const std::uint64_t previous_ok =
            required_subscription_ok_mask_.load(
                std::memory_order_relaxed);
        const std::uint64_t previous_failed =
            required_subscription_failed_mask_.load(
                std::memory_order_relaxed);
        required_subscription_failed_mask_.store(
            (previous_failed & ~seen_mask) | failed_mask,
            std::memory_order_relaxed);
        required_subscription_ok_mask_.store(
            (previous_ok & ~seen_mask) | ok_mask,
            std::memory_order_release);
        if (failed_mask != 0U) {
            required_subscription_failure_observed_mask_.fetch_or(
                failed_mask, std::memory_order_relaxed);
        }
        RefreshReadinessGeneration();
    }
    return true;
}

void ShadowCaptureWriter::RefreshReadinessGeneration() noexcept {
    const bool ready =
        current_logon_generation_ != 0U &&
        latest_logon_ok_.load(std::memory_order_relaxed) &&
        logon_failed_responses_.load(
            std::memory_order_relaxed) == 0U &&
        required_subscription_failure_observed_mask_.load(
            std::memory_order_relaxed) == 0U &&
        required_subscription_ok_mask_.load(
            std::memory_order_relaxed) == required_mask_ &&
        required_subscription_failed_mask_.load(
            std::memory_order_relaxed) == 0U &&
        required_first_seen_mask_.load(
            std::memory_order_relaxed) == required_mask_;
    readiness_generation_.store(
        ready ? current_logon_generation_ : 0U,
        std::memory_order_release);
}

bool ShadowCaptureWriter::WriteAll(
    std::span<const std::byte> bytes) noexcept {
    if (output_ == nullptr) {
        TripOutputFailure();
        return false;
    }
    const std::uint64_t starting_file_bytes =
        sink_file_bytes_.load(std::memory_order_relaxed);
    std::uint64_t final_file_bytes = 0U;
    if (!CheckedAddU64(
            starting_file_bytes,
            static_cast<std::uint64_t>(bytes.size()),
            &final_file_bytes)) {
        TripOutputFailure();
        return false;
    }

    std::size_t offset = 0U;
    while (offset < bytes.size()) {
        const ShadowOutputWriteResult result =
            output_->WriteSome(bytes.subspan(offset));
        if (result.error_number == EINTR &&
            result.bytes_written == 0U) {
            continue;
        }
        if (result.error_number != 0 ||
            result.bytes_written == 0U ||
            result.bytes_written > bytes.size() - offset) {
            TripOutputFailure();
            return false;
        }

        const std::uint64_t current_file_bytes =
            sink_file_bytes_.load(std::memory_order_relaxed);
        std::uint64_t next_file_bytes = 0U;
        if (!CheckedAddU64(
                current_file_bytes,
                static_cast<std::uint64_t>(
                    result.bytes_written),
                &next_file_bytes)) {
            TripOutputFailure();
            return false;
        }
        sink_file_bytes_.store(next_file_bytes,
                               std::memory_order_relaxed);
        offset += result.bytes_written;
    }
    if (sink_file_bytes_.load(std::memory_order_relaxed) !=
        final_file_bytes) {
        TripOutputFailure();
        return false;
    }
    return true;
}

bool ShadowCaptureWriter::FinalizeOutput() noexcept {
    if (output_closed_) {
        return true;
    }
    output_closed_ = true;
    if (output_ == nullptr) {
        return false;
    }

    int sync_error = 0;
    do {
        sync_error = output_->Fdatasync();
    } while (sync_error == EINTR);

    const int close_error = output_->Close();
    output_.reset();
    if (sync_error != 0 || close_error != 0) {
        TripOutputFailure();
        return false;
    }
    return true;
}

void ShadowCaptureWriter::TripRingCorruption() noexcept {
    static_cast<void>(
        fatal_.trip(l2flow::ops::FatalReason::RING_CORRUPTION));
}

void ShadowCaptureWriter::TripOutputFailure() noexcept {
    static_cast<void>(
        fatal_.trip(l2flow::ops::FatalReason::SHADOW_SINK_IO));
}

}  // namespace l2flow::ingress
