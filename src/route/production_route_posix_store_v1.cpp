#include "l2flow/route/production_route_posix_store_v1.h"

#include "l2flow/common/sha256.h"

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <linux/fs.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace l2flow::route {

namespace {

// flock is process-safe only when publishers use independent open file
// descriptions.  Callers in one process may intentionally share the retained
// directory descriptor, so publication also has an in-process serialization
// gate.  Route changes are rare control-plane operations; a global mutex keeps
// the contract unambiguous across different runtime wrappers.
std::timed_mutex g_publish_mutex;

constexpr std::chrono::nanoseconds kMaximumLockTimeout =
    std::chrono::seconds{5};

class ScopedFd final {
public:
    ScopedFd() noexcept = default;
    explicit ScopedFd(int fd) noexcept : fd_(fd) {}
    ~ScopedFd() {
        Reset();
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

    void Reset(int replacement = -1) noexcept {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
        fd_ = replacement;
    }

private:
    int fd_ = -1;
};

class ScopedFlock final {
public:
    explicit ScopedFlock(int fd) noexcept : fd_(fd) {}
    ~ScopedFlock() {
        if (locked_) {
            for (;;) {
                if (::flock(fd_, LOCK_UN) == 0 || errno != EINTR) {
                    break;
                }
            }
        }
    }

    ScopedFlock(const ScopedFlock&) = delete;
    ScopedFlock& operator=(const ScopedFlock&) = delete;

    [[nodiscard]] bool LockFor(
        std::chrono::nanoseconds timeout) noexcept {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        for (;;) {
            if (::flock(fd_, LOCK_EX | LOCK_NB) == 0) {
                locked_ = true;
                return true;
            }
            const int failure = errno;
            if (failure != EINTR && failure != EWOULDBLOCK &&
                failure != EAGAIN) {
                return false;
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                return false;
            }
            std::this_thread::yield();
        }
    }

private:
    int fd_ = -1;
    bool locked_ = false;
};

struct LoadedFile final {
    ScopedFd fd;
    struct stat status {};
    std::vector<std::byte> bytes;
};

enum class LoadKind : std::uint8_t {
    kMissing = 0U,
    kLoaded,
    kFailure,
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

[[nodiscard]] bool InvokeHook(
    const ProductionRouteStoreOptionsV1& options,
    ProductionRouteStoreOperationV1 operation) noexcept {
    return options.operation_hook == nullptr ||
           options.operation_hook(
               options.operation_hook_context, operation);
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

[[nodiscard]] bool SameStableFile(
    const struct stat& before,
    const struct stat& after) noexcept {
    return before.st_dev == after.st_dev &&
           before.st_ino == after.st_ino &&
           before.st_mode == after.st_mode &&
           before.st_uid == after.st_uid &&
           before.st_gid == after.st_gid &&
           before.st_nlink == after.st_nlink &&
           before.st_size == after.st_size;
}

[[nodiscard]] int OpenAtNoIntr(
    int directory_fd,
    const char* name,
    int flags,
    mode_t mode = 0) noexcept {
    for (;;) {
        const int result = (flags & O_CREAT) != 0
            ? ::openat(directory_fd, name, flags, mode)
            : ::openat(directory_fd, name, flags);
        if (result >= 0 || errno != EINTR) {
            return result;
        }
    }
}

[[nodiscard]] bool FsyncNoIntr(int fd) noexcept {
    for (;;) {
        if (::fsync(fd) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

[[nodiscard]] bool RenameAtNoIntr(
    int directory_fd,
    const char* old_name,
    const char* new_name) noexcept {
    for (;;) {
        if (::renameat(directory_fd, old_name, directory_fd, new_name) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

[[nodiscard]] bool RenameNoReplaceAtNoIntr(
    int directory_fd,
    const char* old_name,
    const char* new_name) noexcept {
#if defined(SYS_renameat2) && defined(RENAME_NOREPLACE)
    for (;;) {
        if (::syscall(
                SYS_renameat2,
                directory_fd, old_name,
                directory_fd, new_name,
                RENAME_NOREPLACE) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
#else
    static_cast<void>(directory_fd);
    static_cast<void>(old_name);
    static_cast<void>(new_name);
    errno = ENOSYS;
    return false;
#endif
}

[[nodiscard]] bool UnlinkAtNoIntr(
    int directory_fd,
    const char* name) noexcept {
    for (;;) {
        if (::unlinkat(directory_fd, name, 0) == 0) {
            return true;
        }
        if (errno == ENOENT) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

[[nodiscard]] bool WriteAllAt(
    int fd,
    std::span<const std::byte> bytes) noexcept {
    std::size_t offset = 0U;
    while (offset < bytes.size()) {
        const std::size_t remaining = bytes.size() - offset;
        const std::size_t bounded = std::min<std::size_t>(
            remaining,
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max()));
        const ssize_t written = ::pwrite(
            fd, bytes.data() + offset, bounded,
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

[[nodiscard]] LoadKind LoadFileAt(
    int directory_fd,
    const char* name,
    LoadedFile* output,
    ProductionRouteStoreErrorV1* error,
    bool allow_partial = false) noexcept {
    if (output == nullptr || error == nullptr) {
        return LoadKind::kFailure;
    }
    output->fd.Reset();
    output->bytes.clear();
    const int descriptor = OpenAtNoIntr(
        directory_fd, name,
        O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    if (descriptor < 0) {
        if (errno == ENOENT) {
            return LoadKind::kMissing;
        }
        *error = ProductionRouteStoreErrorV1::kReadFailure;
        return LoadKind::kFailure;
    }
    output->fd.Reset(descriptor);
    struct stat before {};
    if (::fstat(descriptor, &before) != 0 || !IsSafeFile(before)) {
        *error = ProductionRouteStoreErrorV1::kUnsafeRouteFile;
        return LoadKind::kFailure;
    }
    if (before.st_size < 0 ||
        (!allow_partial && before.st_size == 0) ||
        static_cast<std::uintmax_t>(before.st_size) >
            kProductionRouteMaximumEncodedBytesV1) {
        *error = ProductionRouteStoreErrorV1::kManifestInvalid;
        return LoadKind::kFailure;
    }
    try {
        output->bytes.resize(static_cast<std::size_t>(before.st_size));
    } catch (...) {
        *error = ProductionRouteStoreErrorV1::kAllocationFailure;
        return LoadKind::kFailure;
    }
    std::size_t offset = 0U;
    while (offset < output->bytes.size()) {
        const ssize_t count = ::pread(
            descriptor,
            output->bytes.data() + offset,
            output->bytes.size() - offset,
            static_cast<off_t>(offset));
        if (count > 0) {
            offset += static_cast<std::size_t>(count);
            continue;
        }
        if (count < 0 && errno == EINTR) {
            continue;
        }
        *error = ProductionRouteStoreErrorV1::kReadFailure;
        return LoadKind::kFailure;
    }
    struct stat after {};
    if (::fstat(descriptor, &after) != 0 ||
        !SameStableFile(before, after) || !IsSafeFile(after)) {
        *error = ProductionRouteStoreErrorV1::kUnsafeRouteFile;
        return LoadKind::kFailure;
    }
    output->status = after;
    return LoadKind::kLoaded;
}

[[nodiscard]] bool EqualBytes(
    std::span<const std::byte> left,
    std::span<const std::byte> right) noexcept {
    return left.size() == right.size() &&
           std::equal(left.begin(), left.end(), right.begin());
}

[[nodiscard]] bool IsStrictPrefix(
    std::span<const std::byte> prefix,
    std::span<const std::byte> complete) noexcept {
    return prefix.size() < complete.size() &&
           std::equal(prefix.begin(), prefix.end(), complete.begin());
}

[[nodiscard]] bool DecodeLoaded(
    const LoadedFile& loaded,
    ProductionRouteManifestV1* manifest,
    ProductionRouteManifestErrorV1* manifest_error) noexcept {
    *manifest_error = DecodeProductionRouteManifestV1(
        loaded.bytes, manifest);
    return *manifest_error == ProductionRouteManifestErrorV1::kNone;
}

[[nodiscard]] ProductionRoutePublishResultV1 PublishFailure(
    ProductionRouteStoreErrorV1 error,
    const ProductionRouteManifestV1& manifest,
    std::span<const std::byte> encoded,
    std::string* diagnostic,
    std::string_view message) noexcept {
    SetDiagnostic(diagnostic, message);
    ProductionRoutePublishResultV1 result;
    result.error = error;
    result.generation = manifest.generation;
    result.state = manifest.state;
    result.encoded_sha256 = l2flow::common::ComputeSha256(encoded);
    return result;
}

}  // namespace

std::string_view ProductionRouteStoreErrorNameV1(
    ProductionRouteStoreErrorV1 error) noexcept {
    switch (error) {
        case ProductionRouteStoreErrorV1::kNone:
            return "none";
        case ProductionRouteStoreErrorV1::kInvalidArgument:
            return "invalid_argument";
        case ProductionRouteStoreErrorV1::kUnsafeDirectory:
            return "unsafe_directory";
        case ProductionRouteStoreErrorV1::kLockFailure:
            return "lock_failure";
        case ProductionRouteStoreErrorV1::kRouteNotFound:
            return "route_not_found";
        case ProductionRouteStoreErrorV1::kUnsafeRouteFile:
            return "unsafe_route_file";
        case ProductionRouteStoreErrorV1::kReadFailure:
            return "read_failure";
        case ProductionRouteStoreErrorV1::kManifestInvalid:
            return "manifest_invalid";
        case ProductionRouteStoreErrorV1::kRouteFatal:
            return "route_fatal";
        case ProductionRouteStoreErrorV1::kGenerationRegression:
            return "generation_regression";
        case ProductionRouteStoreErrorV1::kGenerationConflict:
            return "generation_conflict";
        case ProductionRouteStoreErrorV1::kPreviousGenerationMismatch:
            return "previous_generation_mismatch";
        case ProductionRouteStoreErrorV1::kTemporaryConflict:
            return "temporary_conflict";
        case ProductionRouteStoreErrorV1::kTemporaryCreateFailure:
            return "temporary_create_failure";
        case ProductionRouteStoreErrorV1::kWriteFailure:
            return "write_failure";
        case ProductionRouteStoreErrorV1::kFileSyncFailure:
            return "file_sync_failure";
        case ProductionRouteStoreErrorV1::kRenameFailure:
            return "rename_failure";
        case ProductionRouteStoreErrorV1::kDirectorySyncFailure:
            return "directory_sync_failure";
        case ProductionRouteStoreErrorV1::kReadbackFailure:
            return "readback_failure";
        case ProductionRouteStoreErrorV1::kInjectedFailure:
            return "injected_failure";
        case ProductionRouteStoreErrorV1::kAllocationFailure:
            return "allocation_failure";
        case ProductionRouteStoreErrorV1::kFreshArtifactPresent:
            return "fresh_artifact_present";
        case ProductionRouteStoreErrorV1::kFreshPreparationFailure:
            return "fresh_preparation_failure";
    }
    return "unknown";
}

namespace {

ProductionRoutePublishResultV1 PublishProductionRouteManifestImplV1At(
    int retained_directory_fd,
    const ProductionRouteManifestV1& manifest,
    bool fresh_only,
    ProductionRouteFreshPreparationHookV1 preparation_hook,
    void* preparation_hook_context,
    const ProductionRouteStoreOptionsV1& options,
    std::string* diagnostic) noexcept {
    std::vector<std::byte> encoded;
    const ProductionRouteManifestErrorV1 encode_error =
        EncodeProductionRouteManifestV1(manifest, &encoded);
    if (encode_error != ProductionRouteManifestErrorV1::kNone) {
        ProductionRoutePublishResultV1 result;
        result.error = ProductionRouteStoreErrorV1::kManifestInvalid;
        result.manifest_error = encode_error;
        result.generation = manifest.generation;
        result.state = manifest.state;
        SetDiagnostic(diagnostic, "route manifest validation failed");
        return result;
    }
    if (retained_directory_fd < 0) {
        return PublishFailure(
            ProductionRouteStoreErrorV1::kInvalidArgument,
            manifest, encoded, diagnostic,
            "retained directory descriptor is invalid");
    }
    if (fresh_only &&
        (preparation_hook == nullptr ||
         manifest.state != ProductionRouteStateV1::kActive ||
         manifest.generation != 1U ||
         manifest.previous_generation != 0U)) {
        return PublishFailure(
            ProductionRouteStoreErrorV1::kInvalidArgument,
            manifest, encoded, diagnostic,
            "fresh route requires Active generation 1, zero predecessor, "
            "and an owner preparation hook");
    }
    if (options.lock_timeout.count() <= 0 ||
        options.lock_timeout > kMaximumLockTimeout) {
        return PublishFailure(
            ProductionRouteStoreErrorV1::kInvalidArgument,
            manifest, encoded, diagnostic,
            "route publication lock timeout is outside (0, 5s]");
    }
    std::unique_lock<std::timed_mutex> process_lock(
        g_publish_mutex, std::defer_lock);
    if (!process_lock.try_lock_for(options.lock_timeout)) {
        return PublishFailure(
            ProductionRouteStoreErrorV1::kLockFailure,
            manifest, encoded, diagnostic,
            "timed out acquiring in-process route publication gate");
    }
    struct stat directory_status {};
    if (::fstat(retained_directory_fd, &directory_status) != 0 ||
        !IsSafeDirectory(directory_status)) {
        return PublishFailure(
            ProductionRouteStoreErrorV1::kUnsafeDirectory,
            manifest, encoded, diagnostic,
            "route directory is not an owner-only retained directory");
    }

    ScopedFlock lock(retained_directory_fd);
    if (!lock.LockFor(options.lock_timeout)) {
        return PublishFailure(
            ProductionRouteStoreErrorV1::kLockFailure,
            manifest, encoded, diagnostic,
            "cannot lock route directory");
    }
    if (!InvokeHook(options, ProductionRouteStoreOperationV1::kAfterLock)) {
        return PublishFailure(
            ProductionRouteStoreErrorV1::kInjectedFailure,
            manifest, encoded, diagnostic,
            "injected failure after route lock");
    }

    LoadedFile current;
    ProductionRouteStoreErrorV1 load_error =
        ProductionRouteStoreErrorV1::kNone;
    const LoadKind current_kind = LoadFileAt(
        retained_directory_fd, kProductionRouteFilenameV1.data(),
        &current, &load_error);
    ProductionRouteManifestV1 current_manifest;
    ProductionRouteManifestErrorV1 current_manifest_error =
        ProductionRouteManifestErrorV1::kNone;
    if (current_kind == LoadKind::kFailure) {
        ProductionRoutePublishResultV1 result = PublishFailure(
            load_error, manifest, encoded, diagnostic,
            "cannot safely load current route");
        result.manifest_error = current_manifest_error;
        return result;
    }
    if (fresh_only && current_kind == LoadKind::kLoaded) {
        return PublishFailure(
            ProductionRouteStoreErrorV1::kFreshArtifactPresent,
            manifest, encoded, diagnostic,
            "fresh route current already exists");
    }
    if (fresh_only) {
        LoadedFile fresh_temporary;
        load_error = ProductionRouteStoreErrorV1::kNone;
        const LoadKind fresh_temporary_kind = LoadFileAt(
            retained_directory_fd,
            kProductionRouteTemporaryFilenameV1.data(),
            &fresh_temporary, &load_error, true);
        if (fresh_temporary_kind == LoadKind::kFailure) {
            return PublishFailure(
                load_error, manifest, encoded, diagnostic,
                "cannot prove the fresh route temporary absent");
        }
        if (fresh_temporary_kind == LoadKind::kLoaded) {
            return PublishFailure(
                ProductionRouteStoreErrorV1::kFreshArtifactPresent,
                manifest, encoded, diagnostic,
                "fresh route temporary already exists");
        }
        if (!preparation_hook(preparation_hook_context)) {
            return PublishFailure(
                ProductionRouteStoreErrorV1::kFreshPreparationFailure,
                manifest, encoded, diagnostic,
                "fresh route owner preparation failed");
        }
    }
    if (current_kind == LoadKind::kLoaded &&
        !DecodeLoaded(
            current, &current_manifest, &current_manifest_error)) {
        ProductionRoutePublishResultV1 result = PublishFailure(
            ProductionRouteStoreErrorV1::kManifestInvalid,
            manifest, encoded, diagnostic,
            "current route manifest is invalid");
        result.manifest_error = current_manifest_error;
        return result;
    }

    const bool current_exact =
        current_kind == LoadKind::kLoaded &&
        EqualBytes(current.bytes, encoded);
    if (current_kind == LoadKind::kMissing) {
        if (manifest.previous_generation != 0U) {
            return PublishFailure(
                ProductionRouteStoreErrorV1::kPreviousGenerationMismatch,
                manifest, encoded, diagnostic,
                "first route must have zero previous generation");
        }
    } else if (manifest.generation < current_manifest.generation) {
        return PublishFailure(
            ProductionRouteStoreErrorV1::kGenerationRegression,
            manifest, encoded, diagnostic,
            "route generation regressed");
    } else if (manifest.generation == current_manifest.generation) {
        if (!current_exact) {
            return PublishFailure(
                ProductionRouteStoreErrorV1::kGenerationConflict,
                manifest, encoded, diagnostic,
                "same route generation has different bytes");
        }

        LoadedFile temporary;
        load_error = ProductionRouteStoreErrorV1::kNone;
        const LoadKind temporary_kind = LoadFileAt(
            retained_directory_fd,
            kProductionRouteTemporaryFilenameV1.data(),
            &temporary, &load_error, true);
        if (temporary_kind == LoadKind::kFailure) {
            return PublishFailure(
                load_error, manifest, encoded, diagnostic,
                "cannot safely inspect route temporary");
        }
        if (temporary_kind == LoadKind::kLoaded) {
            if (!EqualBytes(temporary.bytes, encoded) &&
                !IsStrictPrefix(temporary.bytes, encoded)) {
                return PublishFailure(
                    ProductionRouteStoreErrorV1::kTemporaryConflict,
                    manifest, encoded, diagnostic,
                    "route temporary conflicts with current generation");
            }
            temporary.fd.Reset();
            if (!UnlinkAtNoIntr(
                    retained_directory_fd,
                    kProductionRouteTemporaryFilenameV1.data())) {
                return PublishFailure(
                    ProductionRouteStoreErrorV1::kTemporaryConflict,
                    manifest, encoded, diagnostic,
                    "cannot remove verified duplicate route temporary");
            }
        }
        if (!FsyncNoIntr(current.fd.get())) {
            return PublishFailure(
                ProductionRouteStoreErrorV1::kFileSyncFailure,
                manifest, encoded, diagnostic,
                "cannot sync existing route");
        }
        if (!InvokeHook(
                options,
                ProductionRouteStoreOperationV1::kBeforeDirectorySync)) {
            return PublishFailure(
                ProductionRouteStoreErrorV1::kInjectedFailure,
                manifest, encoded, diagnostic,
                "injected failure before route directory sync");
        }
        if (!FsyncNoIntr(retained_directory_fd)) {
            return PublishFailure(
                ProductionRouteStoreErrorV1::kDirectorySyncFailure,
                manifest, encoded, diagnostic,
                "cannot sync route directory");
        }
        if (!InvokeHook(
                options,
                ProductionRouteStoreOperationV1::kAfterDirectorySync)) {
            return PublishFailure(
                ProductionRouteStoreErrorV1::kInjectedFailure,
                manifest, encoded, diagnostic,
                "injected failure after route directory sync");
        }
        ProductionRoutePublishResultV1 result;
        result.disposition =
            ProductionRoutePublishDispositionV1::kAcceptedExisting;
        result.generation = manifest.generation;
        result.state = manifest.state;
        result.encoded_sha256 = l2flow::common::ComputeSha256(encoded);
        result.file_synced = true;
        result.directory_synced = true;
        return result;
    } else if (manifest.previous_generation !=
               current_manifest.generation) {
        return PublishFailure(
            ProductionRouteStoreErrorV1::kPreviousGenerationMismatch,
            manifest, encoded, diagnostic,
            "route predecessor does not match current generation");
    } else if (manifest.route_instance ==
               current_manifest.route_instance) {
        return PublishFailure(
            ProductionRouteStoreErrorV1::kGenerationConflict,
            manifest, encoded, diagnostic,
            "successor route reused the current route identity");
    }

    LoadedFile temporary;
    load_error = ProductionRouteStoreErrorV1::kNone;
    LoadKind temporary_kind = LoadFileAt(
        retained_directory_fd,
        kProductionRouteTemporaryFilenameV1.data(),
        &temporary, &load_error, true);
    bool adopted_temporary = false;
    if (temporary_kind == LoadKind::kFailure) {
        return PublishFailure(
            load_error, manifest, encoded, diagnostic,
            "cannot safely inspect route temporary");
    }
    if (fresh_only && temporary_kind == LoadKind::kLoaded) {
        return PublishFailure(
            ProductionRouteStoreErrorV1::kFreshArtifactPresent,
            manifest, encoded, diagnostic,
            "fresh route temporary appeared during owner preparation");
    }
    if (temporary_kind == LoadKind::kLoaded) {
        if (EqualBytes(temporary.bytes, encoded)) {
            adopted_temporary = true;
        } else if (IsStrictPrefix(temporary.bytes, encoded)) {
            // pwrite advances strictly from offset zero.  Therefore only an
            // exact prefix can be proven to be a recoverable interrupted write
            // of this immutable manifest.  Remove it under both publication
            // locks and recreate it below; any other bytes remain a conflict.
            temporary.fd.Reset();
            if (!UnlinkAtNoIntr(
                    retained_directory_fd,
                    kProductionRouteTemporaryFilenameV1.data())) {
                return PublishFailure(
                    ProductionRouteStoreErrorV1::kTemporaryConflict,
                    manifest, encoded, diagnostic,
                    "cannot remove recoverable partial route temporary");
            }
            temporary_kind = LoadKind::kMissing;
        } else {
            return PublishFailure(
                ProductionRouteStoreErrorV1::kTemporaryConflict,
                manifest, encoded, diagnostic,
                "route temporary has conflicting bytes");
        }
    }
    if (temporary_kind == LoadKind::kMissing) {
        const int temporary_fd = OpenAtNoIntr(
            retained_directory_fd,
            kProductionRouteTemporaryFilenameV1.data(),
            O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC |
                O_NOFOLLOW | O_NONBLOCK,
            0600);
        if (temporary_fd < 0) {
            return PublishFailure(
                ProductionRouteStoreErrorV1::kTemporaryCreateFailure,
                manifest, encoded, diagnostic,
                "cannot create route temporary");
        }
        temporary.fd.Reset(temporary_fd);
        if (::fchmod(temporary_fd, 0600) != 0) {
            return PublishFailure(
                ProductionRouteStoreErrorV1::kTemporaryCreateFailure,
                manifest, encoded, diagnostic,
                "cannot set route temporary mode");
        }
    }
    if (!InvokeHook(
            options,
            ProductionRouteStoreOperationV1::kAfterTemporaryOpen)) {
        return PublishFailure(
            ProductionRouteStoreErrorV1::kInjectedFailure,
            manifest, encoded, diagnostic,
            "injected failure after route temporary open");
    }
    if (!adopted_temporary) {
        if (!WriteAllAt(temporary.fd.get(), encoded)) {
            return PublishFailure(
                ProductionRouteStoreErrorV1::kWriteFailure,
                manifest, encoded, diagnostic,
                "cannot write route temporary");
        }
        if (!InvokeHook(
                options,
                ProductionRouteStoreOperationV1::kAfterWrite)) {
            return PublishFailure(
                ProductionRouteStoreErrorV1::kInjectedFailure,
                manifest, encoded, diagnostic,
                "injected failure after route write");
        }
    }
    if (!FsyncNoIntr(temporary.fd.get())) {
        return PublishFailure(
            ProductionRouteStoreErrorV1::kFileSyncFailure,
            manifest, encoded, diagnostic,
            "cannot sync route temporary");
    }
    if (!InvokeHook(
            options,
            ProductionRouteStoreOperationV1::kAfterFileSync)) {
        return PublishFailure(
            ProductionRouteStoreErrorV1::kInjectedFailure,
            manifest, encoded, diagnostic,
            "injected failure after route file sync");
    }
    if (!InvokeHook(
            options,
            ProductionRouteStoreOperationV1::kBeforeRename)) {
        return PublishFailure(
            ProductionRouteStoreErrorV1::kInjectedFailure,
            manifest, encoded, diagnostic,
            "injected failure before route rename");
    }
    const bool renamed = fresh_only
        ? RenameNoReplaceAtNoIntr(
              retained_directory_fd,
              kProductionRouteTemporaryFilenameV1.data(),
              kProductionRouteFilenameV1.data())
        : RenameAtNoIntr(
              retained_directory_fd,
              kProductionRouteTemporaryFilenameV1.data(),
              kProductionRouteFilenameV1.data());
    const int rename_error = errno;
    if (!renamed) {
        return PublishFailure(
            fresh_only && rename_error == EEXIST
                ? ProductionRouteStoreErrorV1::kFreshArtifactPresent
                : ProductionRouteStoreErrorV1::kRenameFailure,
            manifest, encoded, diagnostic,
            "cannot atomically publish route");
    }
    if (!InvokeHook(
            options,
            ProductionRouteStoreOperationV1::kAfterRename)) {
        ProductionRoutePublishResultV1 result = PublishFailure(
            ProductionRouteStoreErrorV1::kInjectedFailure,
            manifest, encoded, diagnostic,
            "injected failure after route rename");
        result.file_synced = true;
        result.renamed = true;
        return result;
    }
    if (!InvokeHook(
            options,
            ProductionRouteStoreOperationV1::kBeforeDirectorySync)) {
        ProductionRoutePublishResultV1 result = PublishFailure(
            ProductionRouteStoreErrorV1::kInjectedFailure,
            manifest, encoded, diagnostic,
            "injected failure before route directory sync");
        result.file_synced = true;
        result.renamed = true;
        return result;
    }
    if (!FsyncNoIntr(retained_directory_fd)) {
        ProductionRoutePublishResultV1 result = PublishFailure(
            ProductionRouteStoreErrorV1::kDirectorySyncFailure,
            manifest, encoded, diagnostic,
            "cannot sync route directory");
        result.file_synced = true;
        result.renamed = true;
        return result;
    }
    if (!InvokeHook(
            options,
            ProductionRouteStoreOperationV1::kAfterDirectorySync)) {
        ProductionRoutePublishResultV1 result = PublishFailure(
            ProductionRouteStoreErrorV1::kInjectedFailure,
            manifest, encoded, diagnostic,
            "injected failure after route directory sync");
        result.file_synced = true;
        result.renamed = true;
        result.directory_synced = true;
        return result;
    }
    if (!InvokeHook(
            options,
            ProductionRouteStoreOperationV1::kBeforeReadback)) {
        ProductionRoutePublishResultV1 result = PublishFailure(
            ProductionRouteStoreErrorV1::kInjectedFailure,
            manifest, encoded, diagnostic,
            "injected failure before route readback");
        result.file_synced = true;
        result.renamed = true;
        result.directory_synced = true;
        return result;
    }

    LoadedFile readback;
    load_error = ProductionRouteStoreErrorV1::kNone;
    if (LoadFileAt(
            retained_directory_fd, kProductionRouteFilenameV1.data(),
            &readback, &load_error) != LoadKind::kLoaded ||
        !EqualBytes(readback.bytes, encoded)) {
        ProductionRoutePublishResultV1 result = PublishFailure(
            ProductionRouteStoreErrorV1::kReadbackFailure,
            manifest, encoded, diagnostic,
            "published route readback differs");
        result.file_synced = true;
        result.renamed = true;
        result.directory_synced = true;
        return result;
    }
    ProductionRouteManifestV1 decoded_readback;
    ProductionRouteManifestErrorV1 readback_error =
        ProductionRouteManifestErrorV1::kNone;
    if (!DecodeLoaded(readback, &decoded_readback, &readback_error) ||
        decoded_readback != manifest) {
        ProductionRoutePublishResultV1 result = PublishFailure(
            ProductionRouteStoreErrorV1::kReadbackFailure,
            manifest, encoded, diagnostic,
            "published route cannot be validated");
        result.manifest_error = readback_error;
        result.file_synced = true;
        result.renamed = true;
        result.directory_synced = true;
        return result;
    }

    ProductionRoutePublishResultV1 result;
    result.disposition = adopted_temporary
        ? ProductionRoutePublishDispositionV1::kAdoptedCompleteTemporary
        : ProductionRoutePublishDispositionV1::kPublishedNew;
    result.generation = manifest.generation;
    result.state = manifest.state;
    result.encoded_sha256 = l2flow::common::ComputeSha256(encoded);
    result.file_synced = true;
    result.renamed = true;
    result.directory_synced = true;
    return result;
}

}  // namespace

ProductionRoutePublishResultV1 PublishProductionRouteManifestV1At(
    int retained_directory_fd,
    const ProductionRouteManifestV1& manifest,
    const ProductionRouteStoreOptionsV1& options,
    std::string* diagnostic) noexcept {
    return PublishProductionRouteManifestImplV1At(
        retained_directory_fd, manifest, false, nullptr, nullptr,
        options, diagnostic);
}

ProductionRoutePublishResultV1
PublishFreshProductionRouteManifestV1At(
    int retained_directory_fd,
    const ProductionRouteManifestV1& manifest,
    ProductionRouteFreshPreparationHookV1 preparation_hook,
    void* preparation_hook_context,
    const ProductionRouteStoreOptionsV1& options,
    std::string* diagnostic) noexcept {
    return PublishProductionRouteManifestImplV1At(
        retained_directory_fd, manifest, true, preparation_hook,
        preparation_hook_context, options, diagnostic);
}

ProductionRouteReadResultV1 ReadProductionRouteManifestV1At(
    int retained_directory_fd,
    std::uint64_t minimum_generation,
    std::string* diagnostic) noexcept {
    ProductionRouteReadResultV1 result;
    if (retained_directory_fd < 0) {
        result.error = ProductionRouteStoreErrorV1::kInvalidArgument;
        SetDiagnostic(diagnostic, "retained directory descriptor is invalid");
        return result;
    }
    struct stat directory_status {};
    if (::fstat(retained_directory_fd, &directory_status) != 0 ||
        !IsSafeDirectory(directory_status)) {
        result.error = ProductionRouteStoreErrorV1::kUnsafeDirectory;
        SetDiagnostic(diagnostic, "route directory is unsafe");
        return result;
    }

    LoadedFile loaded;
    ProductionRouteStoreErrorV1 load_error =
        ProductionRouteStoreErrorV1::kNone;
    const LoadKind kind = LoadFileAt(
        retained_directory_fd, kProductionRouteFilenameV1.data(),
        &loaded, &load_error);
    if (kind == LoadKind::kMissing) {
        result.error = ProductionRouteStoreErrorV1::kRouteNotFound;
        SetDiagnostic(diagnostic, "production route is absent");
        return result;
    }
    if (kind == LoadKind::kFailure) {
        result.error = load_error;
        SetDiagnostic(diagnostic, "production route cannot be read safely");
        return result;
    }

    try {
        std::unique_ptr<ProductionRouteManifestV1> manifest(
            new ProductionRouteManifestV1());
        result.manifest_error = DecodeProductionRouteManifestV1(
            loaded.bytes, manifest.get());
        if (result.manifest_error !=
            ProductionRouteManifestErrorV1::kNone) {
            result.error = ProductionRouteStoreErrorV1::kManifestInvalid;
            SetDiagnostic(diagnostic, "production route manifest is invalid");
            return result;
        }
        result.encoded_sha256 =
            l2flow::common::ComputeSha256(loaded.bytes);
        if (manifest->generation < minimum_generation) {
            result.error =
                ProductionRouteStoreErrorV1::kGenerationRegression;
            SetDiagnostic(
                diagnostic,
                "production route is older than the required generation");
            return result;
        }
        const bool fatal =
            manifest->state == ProductionRouteStateV1::kFatal;
        result.manifest = std::move(manifest);
        if (fatal) {
            result.error = ProductionRouteStoreErrorV1::kRouteFatal;
            SetDiagnostic(diagnostic, "production route is explicitly fatal");
            return result;
        }
        return result;
    } catch (...) {
        result.error = ProductionRouteStoreErrorV1::kAllocationFailure;
        SetDiagnostic(diagnostic, "production route allocation failed");
        return result;
    }
}

}  // namespace l2flow::route
