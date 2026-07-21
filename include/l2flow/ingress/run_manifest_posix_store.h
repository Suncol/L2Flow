#pragma once

#include "l2flow/ingress/run_manifest_v1.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::ingress {

inline constexpr std::string_view
    kRunManifestV1FilenamePrefix = "run-manifest-";
inline constexpr std::string_view
    kRunManifestV1FilenameSuffix = ".json";
inline constexpr std::string_view
    kRunManifestV1TemporarySuffix =
        ".run-manifest-v1.tmp";

enum class RunManifestPosixStoreErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kUnsafeDirectory,
    kMalformedCandidateName,
    kAmbiguousCandidates,
    kUnsafeCandidate,
    kCandidateConflict,
    kTemporaryCreate,
    kWriteFailure,
    kSyncFailure,
    kPublishConflict,
    kReadbackFailure,
    kAllocationFailure,
};

[[nodiscard]] std::string_view
RunManifestPosixStoreErrorV1Name(
    RunManifestPosixStoreErrorV1 error) noexcept;

enum class RunManifestPosixDispositionV1
    : std::uint8_t {
    kNone = 0U,
    kPublishedNew,
    kAdoptedCompleteTemporary,
    kAcceptedExistingFinal,
};

// The directory is a dedicated owner-only run metadata directory. Unrelated
// names may coexist, but a second name in the RunManifestV1 final/temporary
// grammar is ambiguous and fails closed.
[[nodiscard]] RunManifestPosixStoreErrorV1
RunManifestV1Filename(
    const BuiltRunManifestV1& manifest,
    std::string* filename) noexcept;

[[nodiscard]] RunManifestPosixStoreErrorV1
RunManifestV1TemporaryFilename(
    const BuiltRunManifestV1& manifest,
    std::string* filename) noexcept;

class PublishedRunManifestReceiptV1 final {
public:
    ~PublishedRunManifestReceiptV1();

    PublishedRunManifestReceiptV1(
        const PublishedRunManifestReceiptV1&) = delete;
    PublishedRunManifestReceiptV1& operator=(
        const PublishedRunManifestReceiptV1&) = delete;
    PublishedRunManifestReceiptV1(
        PublishedRunManifestReceiptV1&&) = delete;
    PublishedRunManifestReceiptV1& operator=(
        PublishedRunManifestReceiptV1&&) = delete;

    [[nodiscard]] std::string_view filename()
        const noexcept {
        return filename_;
    }
    [[nodiscard]] const RawV1Identity& run_id()
        const noexcept {
        return run_id_;
    }
    [[nodiscard]] const RawV1Digest& sha256()
        const noexcept {
        return sha256_;
    }
    [[nodiscard]] std::size_t byte_count()
        const noexcept {
        return byte_count_;
    }

    // Revalidates the retained actual directory inode, final name->inode
    // mapping, owner-only/single-link metadata, exact size and SHA-256. It
    // does not trust a pathname supplied by the caller.
    [[nodiscard]] bool Validate(
        std::string* diagnostic = nullptr) const noexcept;

private:
    friend struct RunManifestPosixPublishAccessV1;

    PublishedRunManifestReceiptV1(
        RawV1Identity run_id,
        RawV1Digest sha256,
        std::size_t byte_count,
        std::string filename,
        int directory_fd,
        int file_fd,
        std::uint64_t directory_device,
        std::uint64_t directory_inode,
        std::uint64_t file_device,
        std::uint64_t file_inode) noexcept;

    RawV1Identity run_id_{};
    RawV1Digest sha256_{};
    std::size_t byte_count_ = 0U;
    std::string filename_;
    int directory_fd_ = -1;
    int file_fd_ = -1;
    std::uint64_t directory_device_ = 0U;
    std::uint64_t directory_inode_ = 0U;
    std::uint64_t file_device_ = 0U;
    std::uint64_t file_inode_ = 0U;
};

struct RunManifestPosixPublishResultV1 final {
    RunManifestPosixStoreErrorV1 error =
        RunManifestPosixStoreErrorV1::kNone;
    RunManifestPosixDispositionV1 disposition =
        RunManifestPosixDispositionV1::kNone;
    RawV1Digest manifest_sha256{};
    std::string filename;
    std::unique_ptr<PublishedRunManifestReceiptV1>
        receipt;
    std::uint32_t observed_candidate_count = 0U;
    bool file_synced = false;
    bool directory_synced = false;

    [[nodiscard]] bool ok() const noexcept {
        return error ==
               RunManifestPosixStoreErrorV1::kNone;
    }
};

// Publishes immutable BuiltRunManifestV1 bytes relative to one retained
// owner-only directory descriptor:
//
//   deterministic O_EXCL 0600 tmp -> complete pwrite -> fsync(tmp)
//   -> renameat2(RENAME_NOREPLACE) -> fsync(actual retained parent)
//   -> stable exact readback/hash/name->inode verification
//
// A complete exact temporary from the sole allowed crash window is adopted.
// An existing final is accepted only when its stable exact bytes equal the
// built capability. Partial/conflicting/unsafe/second candidates are never
// removed or overwritten. This function does not create directories and does
// not imply that a service has wired the resulting receipt.
[[nodiscard]] RunManifestPosixPublishResultV1
PublishRunManifestV1At(
    int retained_directory_fd,
    const BuiltRunManifestV1& manifest,
    std::string* diagnostic = nullptr) noexcept;

}  // namespace l2flow::ingress
