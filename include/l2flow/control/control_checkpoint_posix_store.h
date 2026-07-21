#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/control/control_checkpoint_v1.h"
#include "l2flow/ingress/raw_control_page.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace l2flow::control {

inline constexpr std::string_view
    kControlCheckpointV1FilenamePrefix =
        "control-checkpoint-v1-";
inline constexpr std::string_view
    kControlCheckpointV1FilenameSuffix = ".bin";
inline constexpr std::string_view
    kControlCheckpointV1TemporarySuffix =
        ".control-checkpoint-v1.tmp";
inline constexpr std::size_t
    kControlCheckpointV1MaximumWireBytes =
        kControlCheckpointV1HeaderBytes +
        kControlCheckpointV1MaximumEntries *
            kControlCheckpointV1EntryBytes +
        kControlCheckpointV1TrailerBytes;
inline constexpr std::uint32_t
    kControlCheckpointV1MaximumFinalCandidates = 4096U;

enum class ControlCheckpointPosixStoreErrorV1
    : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kInvalidCheckpoint,
    kInvalidRawFrontier,
    kNamespaceMismatch,
    kCheckpointPastDurableFrontier,
    kUnsafeDirectory,
    kUnsafeCandidate,
    kCandidateConflict,
    kTemporaryCreate,
    kWriteFailure,
    kSyncFailure,
    kPublishConflict,
    kReadbackFailure,
    kMalformedCandidateName,
    kCandidateLimitExceeded,
    kInconsistentCandidates,
    kNoEligibleCheckpoint,
    kAllocationFailure,
};

[[nodiscard]] std::string_view
ControlCheckpointPosixStoreErrorV1Name(
    ControlCheckpointPosixStoreErrorV1 error) noexcept;

enum class ControlCheckpointPosixDispositionV1
    : std::uint8_t {
    kNone = 0U,
    kPublishedNew,
    kAdoptedCompleteTemporary,
    kAcceptedExistingFinal,
};

// The final name binds all three Raw namespace components and both exclusive
// processed cursors. A different state at the same boundary therefore meets
// the same immutable name and is rejected as a byte conflict.
[[nodiscard]] ControlCheckpointPosixStoreErrorV1
ControlCheckpointV1Filename(
    const ControlDecoderCheckpointV1& checkpoint,
    std::string* filename) noexcept;

[[nodiscard]] ControlCheckpointPosixStoreErrorV1
ControlCheckpointV1TemporaryFilename(
    const ControlDecoderCheckpointV1& checkpoint,
    std::string* filename) noexcept;

// Non-forgeable process-local proof that the exact checkpoint bytes crossed
// the file-fsync and retained actual-parent-directory fsync barriers. The
// proof retains both directory and file descriptors and the inode identities
// to which those barriers applied. It is not itself a durable artifact.
//
// This storage receipt proves canonical bytes and processed<=durable, not the
// existence of the referenced Raw record. Restore/attach must still supply the
// validated durable RawReplayRecord boundary required by ControlDecoderV1::
// Restore; a receipt can never authorize skipping that independent Raw check.
class PublishedControlCheckpointReceiptV1 final {
public:
    ~PublishedControlCheckpointReceiptV1();

    PublishedControlCheckpointReceiptV1(
        const PublishedControlCheckpointReceiptV1&) = delete;
    PublishedControlCheckpointReceiptV1& operator=(
        const PublishedControlCheckpointReceiptV1&) = delete;
    PublishedControlCheckpointReceiptV1(
        PublishedControlCheckpointReceiptV1&&) = delete;
    PublishedControlCheckpointReceiptV1& operator=(
        PublishedControlCheckpointReceiptV1&&) = delete;

    [[nodiscard]] std::string_view filename() const noexcept {
        return filename_;
    }
    [[nodiscard]] std::uint32_t source_stream_id()
        const noexcept {
        return source_stream_id_;
    }
    [[nodiscard]] std::uint32_t capture_date() const noexcept {
        return capture_date_;
    }
    [[nodiscard]] const l2flow::common::Identity128&
    stream_day_id() const noexcept {
        return stream_day_id_;
    }
    [[nodiscard]] std::uint64_t
    processed_ingress_sequence() const noexcept {
        return processed_ingress_sequence_;
    }
    [[nodiscard]] std::uint64_t
    processed_record_end_wal_pos() const noexcept {
        return processed_record_end_wal_pos_;
    }
    [[nodiscard]] const l2flow::common::Sha256Digest&
    checkpoint_sha256() const noexcept {
        return checkpoint_sha256_;
    }
    [[nodiscard]] const l2flow::common::Sha256Digest&
    barrier_identity_sha256() const noexcept {
        return barrier_identity_sha256_;
    }
    [[nodiscard]] std::size_t byte_count() const noexcept {
        return byte_count_;
    }
    [[nodiscard]] const l2flow::ingress::RawControlSnapshot&
    observed_durable_frontier() const noexcept {
        return durable_frontier_;
    }

    // Pure readback revalidation: retained directory/file inode identities,
    // final name->inode mapping, exact canonical bytes, codec/hash, namespace,
    // processed<=observed-durable proof, absence of the deterministic tmp,
    // and the private barrier identity are all recomputed. No pathname from a
    // caller is trusted and no file is synchronized, removed, or replaced.
    [[nodiscard]] bool Validate(
        std::string* diagnostic = nullptr) const noexcept;

private:
    friend struct ControlCheckpointPosixPublishAccessV1;

    PublishedControlCheckpointReceiptV1(
        std::uint32_t source_stream_id,
        std::uint32_t capture_date,
        l2flow::common::Identity128 stream_day_id,
        std::uint64_t processed_ingress_sequence,
        std::uint64_t processed_record_end_wal_pos,
        l2flow::ingress::RawControlSnapshot durable_frontier,
        l2flow::common::Sha256Digest checkpoint_sha256,
        l2flow::common::Sha256Digest barrier_identity_sha256,
        std::size_t byte_count,
        std::string filename,
        int directory_fd,
        int file_fd,
        std::uint64_t directory_device,
        std::uint64_t directory_inode,
        std::uint64_t file_device,
        std::uint64_t file_inode) noexcept;

    std::uint32_t source_stream_id_ = 0U;
    std::uint32_t capture_date_ = 0U;
    l2flow::common::Identity128 stream_day_id_{};
    std::uint64_t processed_ingress_sequence_ = 0U;
    std::uint64_t processed_record_end_wal_pos_ = 0U;
    l2flow::ingress::RawControlSnapshot durable_frontier_{};
    l2flow::common::Sha256Digest checkpoint_sha256_{};
    l2flow::common::Sha256Digest barrier_identity_sha256_{};
    std::size_t byte_count_ = 0U;
    std::string filename_;
    int directory_fd_ = -1;
    int file_fd_ = -1;
    std::uint64_t directory_device_ = 0U;
    std::uint64_t directory_inode_ = 0U;
    std::uint64_t file_device_ = 0U;
    std::uint64_t file_inode_ = 0U;
    bool file_barrier_complete_ = false;
    bool directory_barrier_complete_ = false;
};

struct ControlCheckpointPosixPublishResultV1 final {
    ControlCheckpointPosixStoreErrorV1 error =
        ControlCheckpointPosixStoreErrorV1::kNone;
    ControlCheckpointPosixDispositionV1 disposition =
        ControlCheckpointPosixDispositionV1::kNone;
    l2flow::common::Sha256Digest checkpoint_sha256{};
    l2flow::common::Sha256Digest barrier_identity_sha256{};
    std::string filename;
    std::unique_ptr<PublishedControlCheckpointReceiptV1> receipt;
    std::uint32_t observed_candidate_count = 0U;
    bool file_synced = false;
    bool directory_synced = false;

    [[nodiscard]] bool ok() const noexcept {
        return error ==
                   ControlCheckpointPosixStoreErrorV1::kNone &&
               receipt != nullptr;
    }
};

struct ControlCheckpointPosixLoadResultV1 final {
    ControlCheckpointPosixStoreErrorV1 error =
        ControlCheckpointPosixStoreErrorV1::kNone;
    std::string filename;
    l2flow::common::Sha256Digest checkpoint_sha256{};
    std::vector<std::byte> encoded_checkpoint;
    std::optional<ControlDecoderCheckpointV1> checkpoint;
    std::uint32_t observed_final_candidate_count = 0U;
    std::uint32_t namespace_candidate_count = 0U;

    [[nodiscard]] bool ok() const noexcept {
        return error ==
                   ControlCheckpointPosixStoreErrorV1::kNone &&
               checkpoint.has_value() &&
               !encoded_checkpoint.empty() &&
               !filename.empty();
    }
};

// Restart discovery performs a read-only scan of immutable finals beneath a
// retained owner-only directory. Exact temporary names and unrelated names
// are ignored and never removed. Every final in the requested Raw namespace
// must be a safe same-filesystem 0600 single-link regular file whose stable
// bytes decode and re-encode canonically, whose name exactly matches its model,
// and whose processed cursors are no later than current_raw_durable_frontier.
// Eligible cursors must form one strict increasing chain in both ingress and
// WAL dimensions; the unique candidate maximal in both is returned.
//
// This API only discovers owned checkpoint bytes/model. It does not prove
// that the processed cursor is a real Raw record boundary. Before using the
// returned model, the caller must locate the corresponding validated durable
// RawReplayRecord and call ControlDecoderV1::Restore. Neither a successful
// load nor a publication receipt authorizes skipping that Raw validation.
[[nodiscard]] ControlCheckpointPosixLoadResultV1
LoadLatestControlCheckpointV1At(
    int retained_directory_fd,
    const l2flow::ingress::RawControlSnapshot&
        current_raw_durable_frontier,
    std::string* diagnostic = nullptr) noexcept;

// Publishes an already encoded checkpoint only if decode, model validation,
// deterministic re-encoding, Raw cursor alignment, and the observed durable
// upper bound all validate. Real-record boundary proof remains an attach-time
// ControlDecoderV1::Restore obligation as documented on the receipt.
[[nodiscard]] ControlCheckpointPosixPublishResultV1
PublishControlCheckpointV1At(
    int retained_directory_fd,
    std::span<const std::byte> encoded_checkpoint,
    const l2flow::ingress::RawControlSnapshot& durable_frontier,
    std::string* diagnostic = nullptr) noexcept;

// Convenience overload which validates and deterministically encodes the
// logical model before entering the exact same publication path.
[[nodiscard]] ControlCheckpointPosixPublishResultV1
PublishControlCheckpointV1At(
    int retained_directory_fd,
    const ControlDecoderCheckpointV1& checkpoint,
    const l2flow::ingress::RawControlSnapshot& durable_frontier,
    std::string* diagnostic = nullptr) noexcept;

}  // namespace l2flow::control
