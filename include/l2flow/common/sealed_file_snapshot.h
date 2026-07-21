#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>

namespace l2flow::common {

// Linux-only immutable byte snapshot backed by a close-on-exec memfd.  A
// successfully constructed instance has F_SEAL_WRITE, F_SEAL_GROW,
// F_SEAL_SHRINK, and F_SEAL_SEAL applied before it is returned.
class SealedFileSnapshot final {
public:
    SealedFileSnapshot() noexcept = default;
    ~SealedFileSnapshot();

    SealedFileSnapshot(SealedFileSnapshot&& other) noexcept;
    SealedFileSnapshot& operator=(SealedFileSnapshot&& other) noexcept;

    SealedFileSnapshot(const SealedFileSnapshot&) = delete;
    SealedFileSnapshot& operator=(const SealedFileSnapshot&) = delete;

    [[nodiscard]] bool valid() const noexcept;
    [[nodiscard]] int fd() const noexcept;
    [[nodiscard]] std::uint64_t size() const noexcept;

    // The returned path identifies this retained descriptor.  It is suitable
    // for read-only parsers and dlopen() while this object remains alive.
    [[nodiscard]] std::string proc_fd_path() const;

private:
    friend bool CreateSealedFileSnapshot(
        const std::filesystem::path&,
        SealedFileSnapshot*,
        std::string*,
        std::optional<std::uint64_t>,
        std::optional<std::uint64_t>) noexcept;
    friend bool CreateSealedFileSnapshotFromOpenFd(
        int,
        SealedFileSnapshot*,
        std::string*,
        std::optional<std::uint64_t>,
        std::optional<std::uint64_t>) noexcept;

    SealedFileSnapshot(int fd, std::uint64_t size) noexcept;
    void Reset() noexcept;

    int fd_ = -1;
    std::uint64_t size_ = 0;
};

// Opens `source` with O_NOFOLLOW|O_CLOEXEC|O_NONBLOCK, requires a regular
// file, copies its exact bytes to a new memfd, and seals the result.  Error
// messages deliberately never contain the source pathname. If exact_size is
// set, a size mismatch is rejected before creating or populating a memfd. If
// maximum_size is set, a larger source is rejected before memfd creation.
bool CreateSealedFileSnapshot(
    const std::filesystem::path& source,
    SealedFileSnapshot* snapshot,
    std::string* error,
    std::optional<std::uint64_t> exact_size =
        std::nullopt,
    std::optional<std::uint64_t> maximum_size =
        std::nullopt) noexcept;

// Copies an already-open regular-file descriptor into a new sealed memfd.
// The source descriptor and its file offset remain owned by the caller and
// are not changed.
bool CreateSealedFileSnapshotFromOpenFd(
    int source_fd,
    SealedFileSnapshot* snapshot,
    std::string* error,
    std::optional<std::uint64_t> exact_size =
        std::nullopt,
    std::optional<std::uint64_t> maximum_size =
        std::nullopt) noexcept;

// Validates the invariants required before a descriptor may bypass the copy
// step and be used as an already-sealed snapshot.
bool ValidateSealedFileSnapshotFd(
    int fd,
    std::uint64_t* size,
    std::string* error) noexcept;

}  // namespace l2flow::common
