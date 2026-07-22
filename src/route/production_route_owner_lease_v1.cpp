#include "l2flow/route/production_route_owner_lease_v1.h"

#include "l2flow/common/sha256.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <linux/fs.h>
#include <poll.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

namespace l2flow::route {
namespace {

constexpr std::array<std::byte, 8U> kMetadataMagic{{
    std::byte{'L'}, std::byte{'2'}, std::byte{'R'}, std::byte{'L'},
    std::byte{'I'}, std::byte{'V'}, std::byte{'1'}, std::byte{0U}}};
constexpr std::uint16_t kMetadataVersion = 1U;
constexpr std::size_t kMetadataPrefixBytes = 120U;
constexpr std::size_t kMetadataBytes = 152U;
constexpr std::size_t kMaximumProcFileBytes = 4096U;
constexpr std::string_view kMetadataDigestDomain =
    "L2FLOW_PRODUCTION_ROUTE_LIVE_METADATA_V1";

std::mutex g_owner_mutex;
std::set<std::pair<std::uint64_t, std::uint64_t>> g_owned_lock_files;

class ScopedFd final {
public:
    ScopedFd(int descriptor = -1) noexcept
        : descriptor_(descriptor) {}
    ~ScopedFd() {
        Reset();
    }

    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;

    ScopedFd(ScopedFd&& other) noexcept
        : descriptor_(std::exchange(other.descriptor_, -1)) {}
    ScopedFd& operator=(ScopedFd&& other) noexcept {
        if (this != &other) {
            Reset(std::exchange(other.descriptor_, -1));
        }
        return *this;
    }

    [[nodiscard]] int get() const noexcept {
        return descriptor_;
    }

    [[nodiscard]] int Release() noexcept {
        return std::exchange(descriptor_, -1);
    }

    void Reset(int replacement = -1) noexcept {
        if (descriptor_ >= 0) {
            static_cast<void>(::close(descriptor_));
        }
        descriptor_ = replacement;
    }

private:
    int descriptor_ = -1;
};

struct LeaseBinding final {
    std::uint64_t generation = 0U;
    l2flow::common::Identity128 route_instance{};
    l2flow::common::Sha256Digest route_sha256{};
    std::uint64_t lock_device = 0U;
    std::uint64_t lock_inode = 0U;
    std::uint32_t owner_pid = 0U;
    std::uint64_t owner_start_ticks = 0U;
    std::array<std::byte, 16U> linux_boot_id{};

    [[nodiscard]] friend bool operator==(
        const LeaseBinding&,
        const LeaseBinding&) noexcept = default;
};

struct LoadedMetadata final {
    ScopedFd descriptor;
    struct stat status {};
    std::array<std::byte, kMetadataBytes> bytes{};
    LeaseBinding binding{};
};

void SetDiagnostic(
    std::string* diagnostic,
    std::string_view message) noexcept {
    if (diagnostic == nullptr) {
        return;
    }
    try {
        diagnostic->assign(message.data(), message.size());
    } catch (...) {
    }
}

[[nodiscard]] bool IsSafeDirectory(
    const struct stat& status) noexcept {
    return S_ISDIR(status.st_mode) &&
           status.st_uid == ::geteuid() &&
           (status.st_mode & 07777U) == 0700U;
}

[[nodiscard]] bool IsSafeFile(
    const struct stat& status) noexcept {
    return S_ISREG(status.st_mode) &&
           status.st_uid == ::geteuid() &&
           (status.st_mode & 07777U) == 0600U &&
           status.st_nlink == 1;
}

[[nodiscard]] bool SameNamedFile(
    const struct stat& opened,
    const struct stat& named) noexcept {
    return opened.st_dev == named.st_dev &&
           opened.st_ino == named.st_ino &&
           opened.st_mode == named.st_mode &&
           opened.st_uid == named.st_uid &&
           opened.st_gid == named.st_gid &&
           opened.st_nlink == named.st_nlink;
}

template <typename Value>
[[nodiscard]] bool ToU64(Value value, std::uint64_t* output) noexcept {
    static_assert(std::is_integral_v<Value>);
    if (output == nullptr) {
        return false;
    }
    if constexpr (std::is_signed_v<Value>) {
        if (value < 0) {
            return false;
        }
    }
    using Unsigned = std::make_unsigned_t<Value>;
    const Unsigned converted = static_cast<Unsigned>(value);
    if constexpr (sizeof(Unsigned) > sizeof(std::uint64_t)) {
        if (converted > static_cast<Unsigned>(
                            std::numeric_limits<std::uint64_t>::max())) {
            return false;
        }
    }
    *output = static_cast<std::uint64_t>(converted);
    return true;
}

[[nodiscard]] int OpenAtNoIntr(
    int directory_fd,
    std::string_view name,
    int flags,
    mode_t mode = 0) noexcept {
    for (;;) {
        const int result = (flags & O_CREAT) != 0
            ? ::openat(directory_fd, name.data(), flags, mode)
            : ::openat(directory_fd, name.data(), flags);
        if (result >= 0 || errno != EINTR) {
            return result;
        }
    }
}

enum class NamedEntryState : std::uint8_t {
    kMissing = 0U,
    kPresent,
    kFailure,
};

[[nodiscard]] NamedEntryState InspectNamedEntry(
    int directory_fd,
    std::string_view name) noexcept {
    struct stat status {};
    if (::fstatat(
            directory_fd, name.data(), &status,
            AT_SYMLINK_NOFOLLOW) == 0) {
        return NamedEntryState::kPresent;
    }
    return errno == ENOENT
        ? NamedEntryState::kMissing
        : NamedEntryState::kFailure;
}

[[nodiscard]] int OpenNoIntr(
    const char* path,
    int flags) noexcept {
    for (;;) {
        const int result = ::open(path, flags);
        if (result >= 0 || errno != EINTR) {
            return result;
        }
    }
}

[[nodiscard]] bool FsyncNoIntr(int descriptor) noexcept {
    for (;;) {
        if (::fsync(descriptor) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

[[nodiscard]] bool WriteAllAt(
    int descriptor,
    std::span<const std::byte> bytes) noexcept {
    std::size_t offset = 0U;
    while (offset < bytes.size()) {
        const std::size_t count = std::min<std::size_t>(
            bytes.size() - offset,
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max()));
        const ssize_t written = ::pwrite(
            descriptor,
            bytes.data() + offset,
            count,
            static_cast<off_t>(offset));
        if (written > 0) {
            offset += static_cast<std::size_t>(written);
            continue;
        }
        if (written < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

[[nodiscard]] bool ReadAllAt(
    int descriptor,
    std::span<std::byte> bytes) noexcept {
    std::size_t offset = 0U;
    while (offset < bytes.size()) {
        const std::size_t count = std::min<std::size_t>(
            bytes.size() - offset,
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max()));
        const ssize_t read_count = ::pread(
            descriptor,
            bytes.data() + offset,
            count,
            static_cast<off_t>(offset));
        if (read_count > 0) {
            offset += static_cast<std::size_t>(read_count);
            continue;
        }
        if (read_count < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

[[nodiscard]] bool UnlinkAtNoIntr(
    int directory_fd,
    std::string_view name) noexcept {
    for (;;) {
        if (::unlinkat(directory_fd, name.data(), 0) == 0 ||
            errno == ENOENT) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

[[nodiscard]] bool RenameAtNoIntr(
    int directory_fd,
    std::string_view from,
    std::string_view to) noexcept {
    for (;;) {
        if (::renameat(
                directory_fd,
                from.data(),
                directory_fd,
                to.data()) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

[[nodiscard]] bool RenameNoReplaceAtNoIntr(
    int directory_fd,
    std::string_view from,
    std::string_view to) noexcept {
#if defined(SYS_renameat2) && defined(RENAME_NOREPLACE)
    for (;;) {
        if (::syscall(
                SYS_renameat2,
                directory_fd, from.data(),
                directory_fd, to.data(),
                RENAME_NOREPLACE) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
#else
    static_cast<void>(directory_fd);
    static_cast<void>(from);
    static_cast<void>(to);
    errno = ENOSYS;
    return false;
#endif
}

[[nodiscard]] bool ReadBoundedFile(
    const char* path,
    std::string* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    output->clear();
    const int descriptor = OpenNoIntr(path, O_RDONLY | O_CLOEXEC);
    if (descriptor < 0) {
        return false;
    }
    ScopedFd file(descriptor);
    try {
        std::array<char, 512U> buffer{};
        for (;;) {
            ssize_t count = 0;
            do {
                count = ::read(descriptor, buffer.data(), buffer.size());
            } while (count < 0 && errno == EINTR);
            if (count < 0) {
                output->clear();
                return false;
            }
            if (count == 0) {
                return !output->empty();
            }
            if (output->size() > kMaximumProcFileBytes -
                                     static_cast<std::size_t>(count)) {
                output->clear();
                return false;
            }
            output->append(buffer.data(), static_cast<std::size_t>(count));
        }
    } catch (...) {
        output->clear();
        return false;
    }
}

[[nodiscard]] int HexNibble(char value) noexcept {
    if (value >= '0' && value <= '9') {
        return value - '0';
    }
    if (value >= 'a' && value <= 'f') {
        return value - 'a' + 10;
    }
    if (value >= 'A' && value <= 'F') {
        return value - 'A' + 10;
    }
    return -1;
}

[[nodiscard]] bool ReadLinuxBootId(
    std::array<std::byte, 16U>* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    std::string text;
    if (!ReadBoundedFile("/proc/sys/kernel/random/boot_id", &text)) {
        return false;
    }
    if (!text.empty() && text.back() == '\n') {
        text.pop_back();
    }
    if (text.size() != 36U || text[8U] != '-' || text[13U] != '-' ||
        text[18U] != '-' || text[23U] != '-') {
        return false;
    }
    std::array<std::byte, 16U> parsed{};
    std::size_t output_index = 0U;
    for (std::size_t index = 0U; index < text.size();) {
        if (text[index] == '-') {
            ++index;
            continue;
        }
        if (index + 1U >= text.size() || output_index >= parsed.size()) {
            return false;
        }
        const int high = HexNibble(text[index]);
        const int low = HexNibble(text[index + 1U]);
        if (high < 0 || low < 0) {
            return false;
        }
        parsed[output_index++] = static_cast<std::byte>(
            static_cast<unsigned int>((high << 4) | low));
        index += 2U;
    }
    if (output_index != parsed.size()) {
        return false;
    }
    *output = parsed;
    return true;
}

[[nodiscard]] bool ParseU64(
    std::string_view text,
    std::uint64_t* output) noexcept {
    if (output == nullptr || text.empty()) {
        return false;
    }
    std::uint64_t parsed = 0U;
    const auto conversion = std::from_chars(
        text.data(), text.data() + text.size(), parsed, 10);
    if (conversion.ec != std::errc{} ||
        conversion.ptr != text.data() + text.size()) {
        return false;
    }
    *output = parsed;
    return true;
}

[[nodiscard]] bool ReadProcessStartTicks(
    std::uint32_t pid,
    std::uint64_t* output) noexcept {
    if (pid == 0U || output == nullptr) {
        return false;
    }
    std::string path;
    try {
        path = "/proc/" + std::to_string(pid) + "/stat";
    } catch (...) {
        return false;
    }
    std::string text;
    if (!ReadBoundedFile(path.c_str(), &text)) {
        return false;
    }
    const std::size_t close = text.rfind(')');
    if (close == std::string::npos || close + 2U >= text.size() ||
        text[close + 1U] != ' ') {
        return false;
    }
    std::size_t position = close + 2U;
    for (std::uint32_t field = 3U; field <= 22U; ++field) {
        while (position < text.size() && text[position] == ' ') {
            ++position;
        }
        const std::size_t begin = position;
        while (position < text.size() && text[position] != ' ' &&
               text[position] != '\n') {
            ++position;
        }
        if (begin == position) {
            return false;
        }
        if (field == 22U) {
            return ParseU64(
                std::string_view(text).substr(begin, position - begin),
                output);
        }
    }
    return false;
}

[[nodiscard]] bool CurrentOwnerIdentity(
    std::uint32_t* pid,
    std::uint64_t* start_ticks,
    std::array<std::byte, 16U>* boot_id) noexcept {
    if (pid == nullptr || start_ticks == nullptr || boot_id == nullptr) {
        return false;
    }
    const pid_t process_id = ::getpid();
    if (process_id <= 0 ||
        static_cast<std::uintmax_t>(process_id) >
            std::numeric_limits<std::uint32_t>::max()) {
        return false;
    }
    const auto converted = static_cast<std::uint32_t>(process_id);
    if (!ReadProcessStartTicks(converted, start_ticks) ||
        !ReadLinuxBootId(boot_id)) {
        return false;
    }
    *pid = converted;
    return true;
}

void StoreU16(std::uint16_t value, std::byte* output) noexcept {
    output[0] = static_cast<std::byte>(value & 0xffU);
    output[1] = static_cast<std::byte>((value >> 8U) & 0xffU);
}

void StoreU32(std::uint32_t value, std::byte* output) noexcept {
    for (std::size_t index = 0U; index < 4U; ++index) {
        output[index] = static_cast<std::byte>(
            (value >> static_cast<unsigned int>(index * 8U)) & 0xffU);
    }
}

void StoreU64(std::uint64_t value, std::byte* output) noexcept {
    for (std::size_t index = 0U; index < 8U; ++index) {
        output[index] = static_cast<std::byte>(
            (value >> static_cast<unsigned int>(index * 8U)) & 0xffU);
    }
}

[[nodiscard]] std::uint16_t LoadU16(const std::byte* input) noexcept {
    return static_cast<std::uint16_t>(
        std::to_integer<std::uint16_t>(input[0]) |
        (std::to_integer<std::uint16_t>(input[1]) << 8U));
}

[[nodiscard]] std::uint32_t LoadU32(const std::byte* input) noexcept {
    std::uint32_t value = 0U;
    for (std::size_t index = 0U; index < 4U; ++index) {
        value |= std::to_integer<std::uint32_t>(input[index])
                 << static_cast<unsigned int>(index * 8U);
    }
    return value;
}

[[nodiscard]] std::uint64_t LoadU64(const std::byte* input) noexcept {
    std::uint64_t value = 0U;
    for (std::size_t index = 0U; index < 8U; ++index) {
        value |= std::to_integer<std::uint64_t>(input[index])
                 << static_cast<unsigned int>(index * 8U);
    }
    return value;
}

[[nodiscard]] bool ComputeMetadataDigest(
    std::span<const std::byte> prefix,
    l2flow::common::Sha256Digest* output) noexcept {
    if (output == nullptr || prefix.size() != kMetadataPrefixBytes) {
        return false;
    }
    const std::span<const char> domain(
        kMetadataDigestDomain.data(), kMetadataDigestDomain.size());
    constexpr std::array<std::byte, 1U> separator{std::byte{0U}};
    l2flow::common::Sha256Hasher hasher;
    return hasher.Update(std::as_bytes(domain)) &&
           hasher.Update(separator) && hasher.Update(prefix) &&
           hasher.Finalize(output);
}

[[nodiscard]] bool EncodeBinding(
    const LeaseBinding& binding,
    std::array<std::byte, kMetadataBytes>* output) noexcept {
    if (output == nullptr || binding.generation == 0U ||
        l2flow::common::IsZeroIdentity(binding.route_instance) ||
        binding.owner_pid == 0U || binding.owner_start_ticks == 0U) {
        return false;
    }
    std::array<std::byte, kMetadataBytes> encoded{};
    std::copy(kMetadataMagic.begin(), kMetadataMagic.end(), encoded.begin());
    StoreU16(kMetadataVersion, encoded.data() + 8U);
    StoreU16(
        static_cast<std::uint16_t>(kMetadataBytes), encoded.data() + 10U);
    StoreU64(binding.generation, encoded.data() + 16U);
    std::copy(
        binding.route_instance.begin(),
        binding.route_instance.end(),
        encoded.begin() + 24U);
    std::copy(
        binding.route_sha256.begin(),
        binding.route_sha256.end(),
        encoded.begin() + 40U);
    StoreU64(binding.lock_device, encoded.data() + 72U);
    StoreU64(binding.lock_inode, encoded.data() + 80U);
    StoreU32(binding.owner_pid, encoded.data() + 88U);
    StoreU64(binding.owner_start_ticks, encoded.data() + 96U);
    std::copy(
        binding.linux_boot_id.begin(),
        binding.linux_boot_id.end(),
        encoded.begin() + 104U);
    l2flow::common::Sha256Digest digest{};
    if (!ComputeMetadataDigest(
            std::span<const std::byte>(
                encoded.data(), kMetadataPrefixBytes),
            &digest)) {
        return false;
    }
    std::copy(digest.begin(), digest.end(), encoded.begin() + 120U);
    *output = encoded;
    return true;
}

[[nodiscard]] bool DecodeBinding(
    const std::array<std::byte, kMetadataBytes>& encoded,
    LeaseBinding* output) noexcept {
    if (output == nullptr ||
        !std::equal(kMetadataMagic.begin(), kMetadataMagic.end(),
                    encoded.begin()) ||
        LoadU16(encoded.data() + 8U) != kMetadataVersion ||
        LoadU16(encoded.data() + 10U) != kMetadataBytes ||
        LoadU32(encoded.data() + 12U) != 0U ||
        LoadU32(encoded.data() + 92U) != 0U) {
        return false;
    }
    l2flow::common::Sha256Digest expected{};
    if (!ComputeMetadataDigest(
            std::span<const std::byte>(
                encoded.data(), kMetadataPrefixBytes),
            &expected) ||
        !std::equal(expected.begin(), expected.end(),
                    encoded.begin() + 120U)) {
        return false;
    }
    LeaseBinding candidate{};
    candidate.generation = LoadU64(encoded.data() + 16U);
    std::copy_n(
        encoded.begin() + 24U,
        candidate.route_instance.size(),
        candidate.route_instance.begin());
    std::copy_n(
        encoded.begin() + 40U,
        candidate.route_sha256.size(),
        candidate.route_sha256.begin());
    candidate.lock_device = LoadU64(encoded.data() + 72U);
    candidate.lock_inode = LoadU64(encoded.data() + 80U);
    candidate.owner_pid = LoadU32(encoded.data() + 88U);
    candidate.owner_start_ticks = LoadU64(encoded.data() + 96U);
    std::copy_n(
        encoded.begin() + 104U,
        candidate.linux_boot_id.size(),
        candidate.linux_boot_id.begin());
    if (candidate.generation == 0U ||
        l2flow::common::IsZeroIdentity(candidate.route_instance) ||
        candidate.owner_pid == 0U ||
        candidate.owner_pid > static_cast<std::uintmax_t>(
                                  std::numeric_limits<pid_t>::max()) ||
        candidate.owner_start_ticks == 0U) {
        return false;
    }
    *output = candidate;
    return true;
}

[[nodiscard]] bool ManifestBinding(
    const ProductionRouteManifestV1& manifest,
    LeaseBinding* binding) noexcept {
    if (binding == nullptr || !manifest.authoritative() ||
        ValidateProductionRouteManifestV1(manifest) !=
            ProductionRouteManifestErrorV1::kNone) {
        return false;
    }
    try {
        std::vector<std::byte> encoded;
        if (EncodeProductionRouteManifestV1(manifest, &encoded) !=
            ProductionRouteManifestErrorV1::kNone) {
            return false;
        }
        binding->generation = manifest.generation;
        binding->route_instance = manifest.route_instance;
        binding->route_sha256 =
            l2flow::common::ComputeSha256(encoded);
        return true;
    } catch (...) {
        return false;
    }
}

[[nodiscard]] bool NamedFileMatches(
    int directory_fd,
    std::string_view name,
    int descriptor,
    struct stat* output = nullptr) noexcept {
    struct stat opened {};
    struct stat named {};
    if (::fstat(descriptor, &opened) != 0 ||
        ::fstatat(
            directory_fd,
            name.data(),
            &named,
            AT_SYMLINK_NOFOLLOW) != 0 ||
        !IsSafeFile(opened) || !IsSafeFile(named) ||
        !SameNamedFile(opened, named)) {
        return false;
    }
    if (output != nullptr) {
        *output = opened;
    }
    return true;
}

[[nodiscard]] bool SetOwnerLock(int descriptor) noexcept {
    struct flock lock {};
    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = 0;
    lock.l_len = 0;
    for (;;) {
        if (::fcntl(descriptor, F_SETLK, &lock) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

enum class LockOwnerCheck : std::uint8_t {
    kMatches = 0U,
    kMissing,
    kMismatch,
    kFailure,
};

[[nodiscard]] LockOwnerCheck CheckOwnerLock(
    int descriptor,
    std::uint32_t expected_pid) noexcept {
    struct flock lock {};
    lock.l_type = F_RDLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = 0;
    lock.l_len = 0;
    for (;;) {
        if (::fcntl(descriptor, F_GETLK, &lock) == 0) {
            break;
        }
        if (errno != EINTR) {
            return LockOwnerCheck::kFailure;
        }
    }
    if (lock.l_type == F_UNLCK) {
        return LockOwnerCheck::kMissing;
    }
    if (lock.l_type != F_WRLCK || lock.l_pid <= 0) {
        return LockOwnerCheck::kMismatch;
    }
    return static_cast<std::uintmax_t>(lock.l_pid) == expected_pid
        ? LockOwnerCheck::kMatches
        : LockOwnerCheck::kMismatch;
}

[[nodiscard]] int DuplicateDescriptor(int descriptor) noexcept {
    for (;;) {
        const int duplicate = ::fcntl(descriptor, F_DUPFD_CLOEXEC, 0);
        if (duplicate >= 0 || errno != EINTR) {
            return duplicate;
        }
    }
}

[[nodiscard]] int OpenPidFd(std::uint32_t pid) noexcept {
#if defined(__linux__) && defined(SYS_pidfd_open)
    for (;;) {
        const long result = ::syscall(
            SYS_pidfd_open, static_cast<pid_t>(pid), 0U);
        if (result >= 0 &&
            result <= std::numeric_limits<int>::max()) {
            return static_cast<int>(result);
        }
        if (result >= 0) {
            static_cast<void>(::close(static_cast<int>(result)));
            errno = EOVERFLOW;
            return -1;
        }
        if (errno != EINTR) {
            return -1;
        }
    }
#else
    static_cast<void>(pid);
    errno = ENOSYS;
    return -1;
#endif
}

[[nodiscard]] bool PidFdAlive(int descriptor) noexcept {
    struct pollfd probe {};
    probe.fd = descriptor;
    probe.events = POLLIN;
    for (;;) {
        const int result = ::poll(&probe, 1U, 0);
        if (result == 0) {
            return true;
        }
        if (result > 0) {
            return false;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

[[nodiscard]] ProductionRouteOwnerLeaseErrorV1
ProbeCurrentProcessPidFd() noexcept {
    const pid_t process_id = ::getpid();
    if (process_id <= 0 ||
        static_cast<std::uintmax_t>(process_id) >
            std::numeric_limits<std::uint32_t>::max()) {
        return ProductionRouteOwnerLeaseErrorV1::kOwnerMissing;
    }
    const int descriptor = OpenPidFd(
        static_cast<std::uint32_t>(process_id));
    if (descriptor < 0) {
        const int error_number = errno;
        if (error_number == ENOSYS || error_number == EINVAL ||
            error_number == EPERM || error_number == EACCES) {
            return ProductionRouteOwnerLeaseErrorV1::kUnsupported;
        }
        if (error_number == ESRCH) {
            return ProductionRouteOwnerLeaseErrorV1::kOwnerMissing;
        }
        if (error_number == ENOMEM) {
            return ProductionRouteOwnerLeaseErrorV1::kAllocationFailure;
        }
        return ProductionRouteOwnerLeaseErrorV1::kOpenFailure;
    }
    ScopedFd pid_fd(descriptor);
    return PidFdAlive(descriptor)
        ? ProductionRouteOwnerLeaseErrorV1::kNone
        : ProductionRouteOwnerLeaseErrorV1::kOwnerMissing;
}

[[nodiscard]] ProductionRouteOwnerLeaseErrorV1 LoadMetadata(
    int directory_fd,
    LoadedMetadata* output) noexcept {
    if (output == nullptr) {
        return ProductionRouteOwnerLeaseErrorV1::kInvalidArgument;
    }
    *output = LoadedMetadata{};
    const int descriptor = OpenAtNoIntr(
        directory_fd,
        kProductionRouteOwnerMetadataFilenameV1,
        O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    if (descriptor < 0) {
        return errno == ENOENT
            ? ProductionRouteOwnerLeaseErrorV1::kOwnerMissing
            : ProductionRouteOwnerLeaseErrorV1::kOpenFailure;
    }
    output->descriptor.Reset(descriptor);
    if (!NamedFileMatches(
            directory_fd,
            kProductionRouteOwnerMetadataFilenameV1,
            descriptor,
            &output->status)) {
        return ProductionRouteOwnerLeaseErrorV1::kUnsafeFile;
    }
    if (output->status.st_size != static_cast<off_t>(kMetadataBytes)) {
        return ProductionRouteOwnerLeaseErrorV1::kInvalidLease;
    }
    if (!ReadAllAt(descriptor, output->bytes)) {
        return ProductionRouteOwnerLeaseErrorV1::kReadFailure;
    }
    if (!DecodeBinding(output->bytes, &output->binding)) {
        return ProductionRouteOwnerLeaseErrorV1::kInvalidLease;
    }
    return ProductionRouteOwnerLeaseErrorV1::kNone;
}

[[nodiscard]] bool BindingMatchesRoute(
    const LeaseBinding& binding,
    const ProductionRouteReadResultV1& route) noexcept {
    return route.authoritative() && route.manifest != nullptr &&
           binding.generation == route.manifest->generation &&
           binding.route_instance == route.manifest->route_instance &&
           binding.route_sha256 == route.encoded_sha256;
}

}  // namespace

class LiveProductionRouteGuardV1::Impl final {
public:
    int directory_fd = -1;
    int metadata_fd = -1;
    int lock_fd = -1;
    int pid_fd = -1;
    struct stat metadata_status {};
    struct stat lock_status {};
    std::array<std::byte, kMetadataBytes> metadata_bytes{};
    LeaseBinding binding{};

    ~Impl() {
        if (pid_fd >= 0) {
            static_cast<void>(::close(pid_fd));
        }
        if (lock_fd >= 0) {
            static_cast<void>(::close(lock_fd));
        }
        if (metadata_fd >= 0) {
            static_cast<void>(::close(metadata_fd));
        }
        if (directory_fd >= 0) {
            static_cast<void>(::close(directory_fd));
        }
    }

    [[nodiscard]] ProductionRouteOwnerLeaseErrorV1 Validate(
        std::string* diagnostic) const noexcept {
        if (directory_fd < 0 || metadata_fd < 0 || lock_fd < 0 ||
            pid_fd < 0 || !PidFdAlive(pid_fd)) {
            SetDiagnostic(diagnostic, "route owner process is not live");
            return ProductionRouteOwnerLeaseErrorV1::kOwnerMissing;
        }
        std::array<std::byte, 16U> current_boot{};
        std::uint64_t current_start = 0U;
        if (!ReadLinuxBootId(&current_boot) ||
            current_boot != binding.linux_boot_id ||
            !ReadProcessStartTicks(binding.owner_pid, &current_start) ||
            current_start != binding.owner_start_ticks) {
            SetDiagnostic(diagnostic, "route owner process identity changed");
            return ProductionRouteOwnerLeaseErrorV1::kOwnerMissing;
        }
        struct stat current_metadata {};
        struct stat current_lock {};
        std::array<std::byte, kMetadataBytes> current_bytes{};
        if (!NamedFileMatches(
                directory_fd,
                kProductionRouteOwnerMetadataFilenameV1,
                metadata_fd,
                &current_metadata) ||
            !SameNamedFile(metadata_status, current_metadata) ||
            current_metadata.st_size != static_cast<off_t>(kMetadataBytes) ||
            !ReadAllAt(metadata_fd, current_bytes) ||
            current_bytes != metadata_bytes ||
            !NamedFileMatches(
                directory_fd,
                kProductionRouteOwnerLockFilenameV1,
                lock_fd,
                &current_lock) ||
            !SameNamedFile(lock_status, current_lock)) {
            SetDiagnostic(diagnostic, "route owner files changed");
            return ProductionRouteOwnerLeaseErrorV1::kRouteChanged;
        }
        const LockOwnerCheck lock =
            CheckOwnerLock(lock_fd, binding.owner_pid);
        if (lock == LockOwnerCheck::kMissing) {
            SetDiagnostic(diagnostic, "route owner lock is not held");
            return ProductionRouteOwnerLeaseErrorV1::kOwnerMissing;
        }
        if (lock == LockOwnerCheck::kMismatch) {
            SetDiagnostic(diagnostic, "route owner lock identity changed");
            return ProductionRouteOwnerLeaseErrorV1::kManifestMismatch;
        }
        if (lock == LockOwnerCheck::kFailure) {
            SetDiagnostic(diagnostic, "cannot inspect route owner lock");
            return ProductionRouteOwnerLeaseErrorV1::kLockFailure;
        }

        ProductionRouteReadResultV1 route =
            ReadProductionRouteManifestV1At(
                directory_fd, binding.generation, diagnostic);
        if (!BindingMatchesRoute(binding, route)) {
            SetDiagnostic(diagnostic, "production route changed");
            return ProductionRouteOwnerLeaseErrorV1::kRouteChanged;
        }
        if (!PidFdAlive(pid_fd) ||
            CheckOwnerLock(lock_fd, binding.owner_pid) !=
                LockOwnerCheck::kMatches) {
            SetDiagnostic(diagnostic, "route owner exited during validation");
            return ProductionRouteOwnerLeaseErrorV1::kOwnerMissing;
        }
        SetDiagnostic(diagnostic, {});
        return ProductionRouteOwnerLeaseErrorV1::kNone;
    }
};

std::string_view ProductionRouteOwnerLeaseErrorNameV1(
    ProductionRouteOwnerLeaseErrorV1 error) noexcept {
    switch (error) {
        case ProductionRouteOwnerLeaseErrorV1::kNone:
            return "none";
        case ProductionRouteOwnerLeaseErrorV1::kInvalidArgument:
            return "invalid_argument";
        case ProductionRouteOwnerLeaseErrorV1::kUnsupported:
            return "unsupported";
        case ProductionRouteOwnerLeaseErrorV1::kUnsafeDirectory:
            return "unsafe_directory";
        case ProductionRouteOwnerLeaseErrorV1::kOpenFailure:
            return "open_failure";
        case ProductionRouteOwnerLeaseErrorV1::kUnsafeFile:
            return "unsafe_file";
        case ProductionRouteOwnerLeaseErrorV1::kHeldByAnotherOwner:
            return "held_by_another_owner";
        case ProductionRouteOwnerLeaseErrorV1::kLockFailure:
            return "lock_failure";
        case ProductionRouteOwnerLeaseErrorV1::kEncodeFailure:
            return "encode_failure";
        case ProductionRouteOwnerLeaseErrorV1::kWriteFailure:
            return "write_failure";
        case ProductionRouteOwnerLeaseErrorV1::kFileSyncFailure:
            return "file_sync_failure";
        case ProductionRouteOwnerLeaseErrorV1::kDirectorySyncFailure:
            return "directory_sync_failure";
        case ProductionRouteOwnerLeaseErrorV1::kReadFailure:
            return "read_failure";
        case ProductionRouteOwnerLeaseErrorV1::kInvalidLease:
            return "invalid_lease";
        case ProductionRouteOwnerLeaseErrorV1::kManifestMismatch:
            return "manifest_mismatch";
        case ProductionRouteOwnerLeaseErrorV1::kOwnerMissing:
            return "owner_missing";
        case ProductionRouteOwnerLeaseErrorV1::kRouteNotActive:
            return "route_not_active";
        case ProductionRouteOwnerLeaseErrorV1::kRouteChanged:
            return "route_changed";
        case ProductionRouteOwnerLeaseErrorV1::kAllocationFailure:
            return "allocation_failure";
        case ProductionRouteOwnerLeaseErrorV1::kFreshArtifactPresent:
            return "fresh_artifact_present";
    }
    return "unknown";
}

ProductionRouteOwnerLeaseV1::~ProductionRouteOwnerLeaseV1() {
    if (descriptor_ < 0) {
        return;
    }
    const std::lock_guard<std::mutex> lock(g_owner_mutex);
    static_cast<void>(::close(descriptor_));
    descriptor_ = -1;
    g_owned_lock_files.erase({device_, inode_});
}

ProductionRouteOwnerLeaseErrorV1 ProductionRouteOwnerLeaseV1::Acquire(
    int retained_directory_fd,
    const ProductionRouteManifestV1& active_manifest,
    std::unique_ptr<ProductionRouteOwnerLeaseV1>* output,
    std::string* diagnostic) noexcept {
    return AcquireImpl(
        retained_directory_fd, active_manifest, false, output,
        diagnostic);
}

ProductionRouteOwnerLeaseErrorV1
ProductionRouteOwnerLeaseV1::AcquireFresh(
    int retained_directory_fd,
    const ProductionRouteManifestV1& active_manifest,
    std::unique_ptr<ProductionRouteOwnerLeaseV1>* output,
    std::string* diagnostic) noexcept {
    return AcquireImpl(
        retained_directory_fd, active_manifest, true, output,
        diagnostic);
}

ProductionRouteOwnerLeaseErrorV1
ProductionRouteOwnerLeaseV1::AcquireImpl(
    int retained_directory_fd,
    const ProductionRouteManifestV1& active_manifest,
    bool fresh_only,
    std::unique_ptr<ProductionRouteOwnerLeaseV1>* output,
    std::string* diagnostic) noexcept {
    if (output == nullptr || retained_directory_fd < 0) {
        SetDiagnostic(diagnostic, "invalid production route owner input");
        return ProductionRouteOwnerLeaseErrorV1::kInvalidArgument;
    }
    output->reset();
    LeaseBinding binding{};
    if (!ManifestBinding(active_manifest, &binding)) {
        SetDiagnostic(diagnostic, "active route cannot bind an owner lease");
        return ProductionRouteOwnerLeaseErrorV1::kInvalidArgument;
    }
    if (fresh_only &&
        (active_manifest.generation != 1U ||
         active_manifest.previous_generation != 0U)) {
        SetDiagnostic(
            diagnostic,
            "fresh route owner requires generation 1 and zero predecessor");
        return ProductionRouteOwnerLeaseErrorV1::kInvalidArgument;
    }
    struct stat directory_status {};
    if (::fstat(retained_directory_fd, &directory_status) != 0 ||
        !IsSafeDirectory(directory_status)) {
        SetDiagnostic(diagnostic, "route owner directory is unsafe");
        return ProductionRouteOwnerLeaseErrorV1::kUnsafeDirectory;
    }

    // A live route requires consumers to pin and revalidate this exact
    // process.  Refuse the generation before creating owner files when the
    // kernel or its syscall policy cannot provide pidfd_open(2).
    const ProductionRouteOwnerLeaseErrorV1 pidfd_capability =
        ProbeCurrentProcessPidFd();
    if (pidfd_capability != ProductionRouteOwnerLeaseErrorV1::kNone) {
        switch (pidfd_capability) {
            case ProductionRouteOwnerLeaseErrorV1::kUnsupported:
                SetDiagnostic(
                    diagnostic,
                    "pidfd liveness capability is unsupported or denied");
                break;
            case ProductionRouteOwnerLeaseErrorV1::kAllocationFailure:
                SetDiagnostic(
                    diagnostic,
                    "cannot allocate current-process pidfd capability");
                break;
            case ProductionRouteOwnerLeaseErrorV1::kOwnerMissing:
                SetDiagnostic(
                    diagnostic,
                    "current process is not live for pidfd validation");
                break;
            default:
                SetDiagnostic(
                    diagnostic,
                    "cannot open current-process pidfd capability");
                break;
        }
        return pidfd_capability;
    }

    std::uint64_t directory_device = 0U;
    std::uint64_t directory_inode = 0U;
    if (!ToU64(directory_status.st_dev, &directory_device) ||
        !ToU64(directory_status.st_ino, &directory_inode)) {
        SetDiagnostic(diagnostic, "route owner directory identity is invalid");
        return ProductionRouteOwnerLeaseErrorV1::kUnsafeDirectory;
    }
    const std::pair<std::uint64_t, std::uint64_t> owner_key{
        directory_device, directory_inode};
    // POSIX record locks are process-associated: closing any descriptor for
    // the same lock inode would release this process's existing lock.  Fence
    // duplicate opens by retained directory identity before opening the
    // fixed lock name.
    std::unique_lock<std::mutex> process_lock(g_owner_mutex);
    if (g_owned_lock_files.contains(owner_key)) {
        SetDiagnostic(diagnostic, "route owner lock is already held in-process");
        return fresh_only
            ? ProductionRouteOwnerLeaseErrorV1::kFreshArtifactPresent
            : ProductionRouteOwnerLeaseErrorV1::kHeldByAnotherOwner;
    }
    if (fresh_only) {
        constexpr std::array<std::string_view, 3U> kOwnerArtifacts{{
            kProductionRouteOwnerLockFilenameV1,
            kProductionRouteOwnerMetadataFilenameV1,
            kProductionRouteOwnerMetadataTemporaryFilenameV1}};
        for (const std::string_view name : kOwnerArtifacts) {
            const NamedEntryState state =
                InspectNamedEntry(retained_directory_fd, name);
            if (state == NamedEntryState::kPresent) {
                SetDiagnostic(
                    diagnostic,
                    "fresh route owner namespace contains an old artifact");
                return ProductionRouteOwnerLeaseErrorV1::
                    kFreshArtifactPresent;
            }
            if (state == NamedEntryState::kFailure) {
                SetDiagnostic(
                    diagnostic,
                    "cannot prove the fresh route owner namespace absent");
                return ProductionRouteOwnerLeaseErrorV1::kOpenFailure;
            }
        }
    }

    bool created = false;
    int lock_descriptor = OpenAtNoIntr(
        retained_directory_fd,
        kProductionRouteOwnerLockFilenameV1,
        O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK,
        0600);
    if (lock_descriptor >= 0) {
        created = true;
    } else if (!fresh_only && errno == EEXIST) {
        lock_descriptor = OpenAtNoIntr(
            retained_directory_fd,
            kProductionRouteOwnerLockFilenameV1,
            O_RDWR | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    }
    if (lock_descriptor < 0) {
        if (fresh_only && errno == EEXIST) {
            SetDiagnostic(
                diagnostic,
                "fresh route owner lock appeared during acquisition");
            return ProductionRouteOwnerLeaseErrorV1::
                kFreshArtifactPresent;
        }
        SetDiagnostic(diagnostic, "cannot open route owner lock");
        return ProductionRouteOwnerLeaseErrorV1::kOpenFailure;
    }
    ScopedFd owner_lock(lock_descriptor);
    struct stat lock_status {};
    if ((created && ::fchmod(lock_descriptor, 0600) != 0) ||
        !NamedFileMatches(
            retained_directory_fd,
            kProductionRouteOwnerLockFilenameV1,
            lock_descriptor,
            &lock_status) ||
        !ToU64(lock_status.st_dev, &binding.lock_device) ||
        !ToU64(lock_status.st_ino, &binding.lock_inode) ||
        !CurrentOwnerIdentity(
            &binding.owner_pid,
            &binding.owner_start_ticks,
            &binding.linux_boot_id)) {
        SetDiagnostic(diagnostic, "route owner lock or identity is invalid");
        return ProductionRouteOwnerLeaseErrorV1::kUnsafeFile;
    }

    if (!SetOwnerLock(lock_descriptor)) {
        const auto error = (errno == EAGAIN || errno == EACCES)
            ? ProductionRouteOwnerLeaseErrorV1::kHeldByAnotherOwner
            : ProductionRouteOwnerLeaseErrorV1::kLockFailure;
        SetDiagnostic(diagnostic, "cannot acquire route owner lock");
        return error;
    }
    try {
        g_owned_lock_files.insert(owner_key);
    } catch (...) {
        SetDiagnostic(diagnostic, "cannot register route owner lock");
        return ProductionRouteOwnerLeaseErrorV1::kAllocationFailure;
    }
    process_lock.unlock();
    bool registered = true;
    const auto unregister = [&]() noexcept {
        if (!registered) {
            return;
        }
        const std::lock_guard<std::mutex> lock(g_owner_mutex);
        owner_lock.Reset();
        g_owned_lock_files.erase(owner_key);
        registered = false;
    };

    std::array<std::byte, kMetadataBytes> encoded{};
    if (!EncodeBinding(binding, &encoded)) {
        unregister();
        SetDiagnostic(diagnostic, "cannot encode route owner metadata");
        return ProductionRouteOwnerLeaseErrorV1::kEncodeFailure;
    }
    if (!fresh_only && !UnlinkAtNoIntr(
            retained_directory_fd,
            kProductionRouteOwnerMetadataTemporaryFilenameV1)) {
        unregister();
        SetDiagnostic(diagnostic, "cannot clear stale owner metadata temporary");
        return ProductionRouteOwnerLeaseErrorV1::kWriteFailure;
    }
    const int temporary_descriptor = OpenAtNoIntr(
        retained_directory_fd,
        kProductionRouteOwnerMetadataTemporaryFilenameV1,
        O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK,
        0600);
    if (temporary_descriptor < 0) {
        const int open_error = errno;
        unregister();
        if (fresh_only && open_error == EEXIST) {
            SetDiagnostic(
                diagnostic,
                "fresh route owner metadata temporary appeared");
            return ProductionRouteOwnerLeaseErrorV1::
                kFreshArtifactPresent;
        }
        SetDiagnostic(diagnostic, "cannot create route owner metadata");
        return ProductionRouteOwnerLeaseErrorV1::kOpenFailure;
    }
    ScopedFd temporary(temporary_descriptor);
    struct stat temporary_status {};
    if (::fchmod(temporary_descriptor, 0600) != 0 ||
        ::fstat(temporary_descriptor, &temporary_status) != 0 ||
        !IsSafeFile(temporary_status) ||
        !WriteAllAt(temporary_descriptor, encoded)) {
        unregister();
        SetDiagnostic(diagnostic, "cannot write route owner metadata");
        return ProductionRouteOwnerLeaseErrorV1::kWriteFailure;
    }
    if (!FsyncNoIntr(temporary_descriptor)) {
        unregister();
        SetDiagnostic(diagnostic, "cannot sync route owner metadata");
        return ProductionRouteOwnerLeaseErrorV1::kFileSyncFailure;
    }
    const bool metadata_renamed = fresh_only
        ? RenameNoReplaceAtNoIntr(
              retained_directory_fd,
              kProductionRouteOwnerMetadataTemporaryFilenameV1,
              kProductionRouteOwnerMetadataFilenameV1)
        : RenameAtNoIntr(
              retained_directory_fd,
              kProductionRouteOwnerMetadataTemporaryFilenameV1,
              kProductionRouteOwnerMetadataFilenameV1);
    const int metadata_rename_error = errno;
    if (!metadata_renamed) {
        unregister();
        if (fresh_only && metadata_rename_error == EEXIST) {
            SetDiagnostic(
                diagnostic,
                "fresh route owner metadata appeared during publication");
            return ProductionRouteOwnerLeaseErrorV1::
                kFreshArtifactPresent;
        }
        SetDiagnostic(diagnostic, "cannot publish route owner metadata");
        return ProductionRouteOwnerLeaseErrorV1::kWriteFailure;
    }
    temporary.Reset();
    if (!FsyncNoIntr(retained_directory_fd)) {
        unregister();
        SetDiagnostic(diagnostic, "cannot sync route owner directory");
        return ProductionRouteOwnerLeaseErrorV1::kDirectorySyncFailure;
    }
    try {
        output->reset(new ProductionRouteOwnerLeaseV1(
            owner_lock.Release(), directory_device, directory_inode));
    } catch (...) {
        unregister();
        SetDiagnostic(diagnostic, "cannot allocate route owner lease");
        return ProductionRouteOwnerLeaseErrorV1::kAllocationFailure;
    }
    registered = false;
    SetDiagnostic(diagnostic, {});
    return ProductionRouteOwnerLeaseErrorV1::kNone;
}

LiveProductionRouteGuardV1::LiveProductionRouteGuardV1(
    std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}

LiveProductionRouteGuardV1::~LiveProductionRouteGuardV1() = default;

ProductionRouteOwnerLeaseErrorV1 LiveProductionRouteGuardV1::Validate(
    std::string* diagnostic) const noexcept {
    if (impl_ == nullptr) {
        SetDiagnostic(diagnostic, "route guard is empty");
        return ProductionRouteOwnerLeaseErrorV1::kInvalidArgument;
    }
    return impl_->Validate(diagnostic);
}

LiveProductionRouteReadResultV1 ReadLiveProductionRouteV1At(
    int retained_directory_fd,
    std::uint64_t minimum_generation,
    std::string* diagnostic) noexcept {
    LiveProductionRouteReadResultV1 result{};
    result.route = ReadProductionRouteManifestV1At(
        retained_directory_fd, minimum_generation, diagnostic);
    if (!result.route.authoritative()) {
        result.error = ProductionRouteOwnerLeaseErrorV1::kRouteNotActive;
        return result;
    }
    struct stat directory_status {};
    if (::fstat(retained_directory_fd, &directory_status) != 0 ||
        !IsSafeDirectory(directory_status)) {
        result.error = ProductionRouteOwnerLeaseErrorV1::kUnsafeDirectory;
        SetDiagnostic(diagnostic, "route owner directory is unsafe");
        return result;
    }

    LoadedMetadata metadata;
    result.error = LoadMetadata(retained_directory_fd, &metadata);
    if (result.error != ProductionRouteOwnerLeaseErrorV1::kNone) {
        SetDiagnostic(diagnostic, "cannot load route owner metadata");
        return result;
    }
    if (!BindingMatchesRoute(metadata.binding, result.route)) {
        result.error = ProductionRouteOwnerLeaseErrorV1::kManifestMismatch;
        SetDiagnostic(diagnostic, "route owner metadata does not match route");
        return result;
    }
    const pid_t self = ::getpid();
    if (self > 0 && static_cast<std::uintmax_t>(self) ==
                        metadata.binding.owner_pid) {
        result.error = ProductionRouteOwnerLeaseErrorV1::kInvalidArgument;
        SetDiagnostic(
            diagnostic,
            "same-process consumers require the injected route capability");
        return result;
    }
    std::array<std::byte, 16U> current_boot{};
    std::uint64_t current_start = 0U;
    if (!ReadLinuxBootId(&current_boot) ||
        current_boot != metadata.binding.linux_boot_id ||
        !ReadProcessStartTicks(
            metadata.binding.owner_pid, &current_start) ||
        current_start != metadata.binding.owner_start_ticks) {
        result.error = ProductionRouteOwnerLeaseErrorV1::kOwnerMissing;
        SetDiagnostic(diagnostic, "route owner process identity is stale");
        return result;
    }

    const int lock_descriptor = OpenAtNoIntr(
        retained_directory_fd,
        kProductionRouteOwnerLockFilenameV1,
        O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    if (lock_descriptor < 0) {
        result.error = errno == ENOENT
            ? ProductionRouteOwnerLeaseErrorV1::kOwnerMissing
            : ProductionRouteOwnerLeaseErrorV1::kOpenFailure;
        SetDiagnostic(diagnostic, "cannot open route owner lock");
        return result;
    }
    ScopedFd owner_lock(lock_descriptor);
    struct stat lock_status {};
    std::uint64_t lock_device = 0U;
    std::uint64_t lock_inode = 0U;
    if (!NamedFileMatches(
            retained_directory_fd,
            kProductionRouteOwnerLockFilenameV1,
            lock_descriptor,
            &lock_status) ||
        !ToU64(lock_status.st_dev, &lock_device) ||
        !ToU64(lock_status.st_ino, &lock_inode) ||
        lock_device != metadata.binding.lock_device ||
        lock_inode != metadata.binding.lock_inode) {
        result.error = ProductionRouteOwnerLeaseErrorV1::kManifestMismatch;
        SetDiagnostic(diagnostic, "route owner lock does not match metadata");
        return result;
    }
    const LockOwnerCheck owner =
        CheckOwnerLock(lock_descriptor, metadata.binding.owner_pid);
    if (owner == LockOwnerCheck::kMissing) {
        result.error = ProductionRouteOwnerLeaseErrorV1::kOwnerMissing;
        SetDiagnostic(diagnostic, "active route has no live owner lock");
        return result;
    }
    if (owner == LockOwnerCheck::kMismatch) {
        result.error = ProductionRouteOwnerLeaseErrorV1::kManifestMismatch;
        SetDiagnostic(diagnostic, "route owner lock PID does not match metadata");
        return result;
    }
    if (owner == LockOwnerCheck::kFailure) {
        result.error = ProductionRouteOwnerLeaseErrorV1::kLockFailure;
        SetDiagnostic(diagnostic, "cannot inspect route owner lock");
        return result;
    }
    const int pid_fd = OpenPidFd(metadata.binding.owner_pid);
    if (pid_fd < 0 || !PidFdAlive(pid_fd)) {
        if (pid_fd >= 0) {
            static_cast<void>(::close(pid_fd));
        }
        result.error = ProductionRouteOwnerLeaseErrorV1::kOwnerMissing;
        SetDiagnostic(diagnostic, "cannot pin the live route owner process");
        return result;
    }
    ScopedFd owner_process(pid_fd);
    const int directory_duplicate = DuplicateDescriptor(retained_directory_fd);
    if (directory_duplicate < 0) {
        result.error = ProductionRouteOwnerLeaseErrorV1::kOpenFailure;
        SetDiagnostic(diagnostic, "cannot retain route directory");
        return result;
    }
    ScopedFd retained_directory(directory_duplicate);

    std::unique_ptr<LiveProductionRouteGuardV1::Impl> impl;
    try {
        impl = std::make_unique<LiveProductionRouteGuardV1::Impl>();
        impl->directory_fd = retained_directory.Release();
        impl->metadata_fd = metadata.descriptor.Release();
        impl->lock_fd = owner_lock.Release();
        impl->pid_fd = owner_process.Release();
        impl->metadata_status = metadata.status;
        impl->lock_status = lock_status;
        impl->metadata_bytes = metadata.bytes;
        impl->binding = metadata.binding;
        auto guard = std::unique_ptr<LiveProductionRouteGuardV1>(
            new LiveProductionRouteGuardV1(std::move(impl)));
        result.error = guard->Validate(diagnostic);
        if (result.error != ProductionRouteOwnerLeaseErrorV1::kNone) {
            return result;
        }
        result.guard = std::move(guard);
    } catch (...) {
        result.error = ProductionRouteOwnerLeaseErrorV1::kAllocationFailure;
        SetDiagnostic(diagnostic, "cannot allocate route owner guard");
        return result;
    }
    SetDiagnostic(diagnostic, {});
    return result;
}

}  // namespace l2flow::route
