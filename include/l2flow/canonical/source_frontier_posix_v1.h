#pragma once

#include "l2flow/canonical/source_frontier_v1.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::canonical {

inline constexpr std::uint32_t kSourceFrontierPosixFileModeV1 = 0600U;

// Immutable part of a SourceFrontier page identity.  Progress, state and
// quality fields are deliberately excluded.
struct SourceFrontierIdentityV1 final {
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    l2flow::common::Identity128 stream_day_id{};
    ClockEpochIdentityV1 clock_epoch{};
    l2flow::common::Identity128 writer_instance{};
    std::uint64_t generation = 0U;

    [[nodiscard]] friend constexpr bool operator==(
        const SourceFrontierIdentityV1&,
        const SourceFrontierIdentityV1&) noexcept = default;
};

[[nodiscard]] SourceFrontierIdentityV1 SourceFrontierIdentityFromConfigV1(
    const SourceFrontierConfigV1& config) noexcept;

// directory_fd is borrowed for Create/Attach only.  It must identify a real
// directory owned by expected_owner_uid and not writable by group/other.
// file_name must be one non-empty path component.  The live mapping retains a
// CLOEXEC duplicate of directory_fd so role acquisition remains safe after
// the caller closes its descriptor.
struct SourceFrontierPosixFileOptionsV1 final {
    int directory_fd = -1;
    std::string_view file_name{};
    std::uint32_t expected_owner_uid = 0U;
};

enum class SourceFrontierPosixErrorV1 : std::uint8_t {
    kNone = 0U,
    kUnsupportedPlatform,
    kInvalidArgument,
    kInvalidName,
    kInvalidDirectory,
    kDirectoryOwnerMismatch,
    kUnsafeDirectoryMode,
    kOpenFailed,
    kAlreadyExists,
    kSymlinkRejected,
    kWrongFileType,
    kFileOwnerMismatch,
    kWrongFileMode,
    kWrongLinkCount,
    kWrongFileSize,
    kAllocationFailed,
    kMapFailed,
    kSyncFailed,
    kSourceFrontierFailure,
    kIdentityMismatch,
    kPathReplaced,
    kRoleConflict,
    kOfdLocksUnsupported,
    kLockFailed,
    kResourceExhausted,
};

[[nodiscard]] const char* SourceFrontierPosixErrorNameV1(
    SourceFrontierPosixErrorV1 error) noexcept;

// system_errno is nonzero only when a failed POSIX operation supplied errno
// (or, for posix_fallocate, its returned error number).  frontier_error is
// non-kNone only when initialization/read validation failed in the portable
// SourceFrontier implementation.
struct SourceFrontierPosixResultV1 final {
    SourceFrontierPosixErrorV1 error = SourceFrontierPosixErrorV1::kNone;
    int system_errno = 0;
    SourceFrontierErrorV1 frontier_error = SourceFrontierErrorV1::kNone;

    [[nodiscard]] explicit operator bool() const noexcept {
        return error == SourceFrontierPosixErrorV1::kNone;
    }
};

enum class SourceFrontierWriterRoleV1 : std::uint8_t {
    // Owns callback/captured and Raw append publication.
    kProducer = 0U,
    // Owns decoded/Canonical processed publication.
    kProcessor = 1U,
};

class SourceFrontierPosixRoleLeaseV1 final {
public:
    ~SourceFrontierPosixRoleLeaseV1();

    SourceFrontierPosixRoleLeaseV1(
        const SourceFrontierPosixRoleLeaseV1&) = delete;
    SourceFrontierPosixRoleLeaseV1& operator=(
        const SourceFrontierPosixRoleLeaseV1&) = delete;
    SourceFrontierPosixRoleLeaseV1(
        SourceFrontierPosixRoleLeaseV1&&) = delete;
    SourceFrontierPosixRoleLeaseV1& operator=(
        SourceFrontierPosixRoleLeaseV1&&) = delete;

    [[nodiscard]] SourceFrontierWriterRoleV1 role() const noexcept {
        return role_;
    }

private:
    friend class SourceFrontierPosixMappingV1;

    SourceFrontierPosixRoleLeaseV1(
        int descriptor,
        SourceFrontierWriterRoleV1 role) noexcept;

    int descriptor_ = -1;
    SourceFrontierWriterRoleV1 role_ =
        SourceFrontierWriterRoleV1::kProducer;
};

// Linux MAP_SHARED owner/attacher for one exact 4096-byte SourceFrontier page.
// The object owns its mapping and file/directory descriptors.  It never
// unlinks or reinitializes the page.  Callers must keep the mapping alive for
// every raw page pointer they hand to ingress or processing components.
class SourceFrontierPosixMappingV1 final {
public:
    ~SourceFrontierPosixMappingV1();

    SourceFrontierPosixMappingV1(
        const SourceFrontierPosixMappingV1&) = delete;
    SourceFrontierPosixMappingV1& operator=(
        const SourceFrontierPosixMappingV1&) = delete;
    SourceFrontierPosixMappingV1(
        SourceFrontierPosixMappingV1&&) = delete;
    SourceFrontierPosixMappingV1& operator=(
        SourceFrontierPosixMappingV1&&) = delete;

    [[nodiscard]] static SourceFrontierPosixResultV1 Create(
        const SourceFrontierPosixFileOptionsV1& options,
        const SourceFrontierConfigV1& config,
        std::unique_ptr<SourceFrontierPosixMappingV1>* output) noexcept;

    [[nodiscard]] static SourceFrontierPosixResultV1 Attach(
        const SourceFrontierPosixFileOptionsV1& options,
        const SourceFrontierIdentityV1& expected_identity,
        std::unique_ptr<SourceFrontierPosixMappingV1>* output) noexcept;

    [[nodiscard]] SourceFrontierPageV1* page() noexcept {
        return page_;
    }
    [[nodiscard]] const SourceFrontierPageV1* page() const noexcept {
        return page_;
    }
    [[nodiscard]] const SourceFrontierIdentityV1& identity() const noexcept {
        return identity_;
    }

    // Acquires an independent nonblocking Linux OFD write lock on the byte
    // assigned to role.  Producer and processor bytes do not conflict, while
    // a second instance of the same role does.  No fallback to process-scoped
    // POSIX locks is made.  Closing/crashing releases the kernel lease but
    // does not change source_state or revoke any externally published route.
    [[nodiscard]] SourceFrontierPosixResultV1 AcquireRole(
        SourceFrontierWriterRoleV1 role,
        std::unique_ptr<SourceFrontierPosixRoleLeaseV1>* output) const
        noexcept;

private:
    [[nodiscard]] static SourceFrontierPosixResultV1 FinishOpen(
        const SourceFrontierPosixFileOptionsV1& options,
        const std::string& file_name,
        int file_descriptor,
        SourceFrontierPageV1* page,
        std::uint64_t device,
        std::uint64_t inode,
        SourceFrontierIdentityV1 identity,
        std::unique_ptr<SourceFrontierPosixMappingV1>* output) noexcept;

    SourceFrontierPosixMappingV1(
        int directory_descriptor,
        int file_descriptor,
        SourceFrontierPageV1* page,
        std::string_view file_name,
        std::uint32_t expected_owner_uid,
        std::uint64_t device,
        std::uint64_t inode,
        SourceFrontierIdentityV1 identity);

    int directory_descriptor_ = -1;
    int file_descriptor_ = -1;
    SourceFrontierPageV1* page_ = nullptr;
    std::string file_name_{};
    std::uint32_t expected_owner_uid_ = 0U;
    std::uint64_t device_ = 0U;
    std::uint64_t inode_ = 0U;
    SourceFrontierIdentityV1 identity_{};
};

}  // namespace l2flow::canonical
