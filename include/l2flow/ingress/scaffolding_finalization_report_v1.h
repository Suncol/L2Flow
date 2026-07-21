#pragma once

#include "l2flow/ingress/empty_anchor_tombstone_v1.h"
#include "l2flow/ingress/raw_manifest_v1.h"
#include "l2flow/ingress/reserve_state_v1.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace l2flow::ingress {

inline constexpr std::uint32_t
    kScaffoldingFinalizationReportV1SchemaVersion = 1U;
inline constexpr std::size_t
    kScaffoldingFinalizationReportV1MaximumBytes = 65'536U;
inline constexpr std::size_t
    kScaffoldingFinalizationReportV1MaximumObjects = 16U;
inline constexpr std::size_t
    kScaffoldingFinalizationReportV1MaximumActions = 16U;

// Updated together with schemas/scaffolding_finalization_report_v1.json.
inline constexpr std::size_t
    kScaffoldingFinalizationReportV1SchemaBytes = 7'652U;
inline constexpr std::string_view
    kScaffoldingFinalizationReportV1SchemaSha256Hex =
        "c04040c93c565e14d55856616d87fb71"
        "8c53a31ec730c7d31399b98c7867899a";

enum class ScaffoldingObjectRoleV1 : std::uint8_t {
    kCaptureDirectory = 1U,
    kStreamDirectory = 2U,
    kWriterLeaseTemporary = 3U,
    kWriterLeaseFinal = 4U,
    kMaintenanceDirectory = 5U,
    kJournalTemporary = 6U,
    kJournalFinal = 7U,
};

enum class ScaffoldingObjectStartStateV1 : std::uint8_t {
    kAbsent = 1U,
    kValidFinalOrCompleteTemporary = 2U,
    kRecognizedPartialTemporary = 3U,
};

enum class ScaffoldingObjectPostStateV1 : std::uint8_t {
    kAbsent = 1U,
    kValidDirectory = 2U,
    kValidFile = 3U,
    kHeaderOnlyJournal = 4U,
};

enum class ScaffoldingActionKindV1 : std::uint8_t {
    kEnsureOrValidateParent = 1U,
    kCompleteOrRebuildTemporary = 2U,
    kAdoptNoReplace = 3U,
    kCleanupRecognizedPartial = 4U,
    kSyncObject = 5U,
    kSyncParent = 6U,
    kRevalidatePostState = 7U,
};

struct ScaffoldingPostStateBarriersV1 final {
    bool object_synced = false;
    bool parent_directory_synced = false;
    bool retained_fd_revalidated = false;
};

struct ScaffoldingObjectStateV1 final {
    ScaffoldingObjectRoleV1 role =
        ScaffoldingObjectRoleV1::kCaptureDirectory;
    ScaffoldingObjectStartStateV1 start_state =
        ScaffoldingObjectStartStateV1::kAbsent;
    ScaffoldingObjectPostStateV1 post_state =
        ScaffoldingObjectPostStateV1::kAbsent;
    ScaffoldingPostStateBarriersV1 barriers{};
};

struct ScaffoldingActionResultV1 final {
    std::uint8_t action_id = 0U;
    ScaffoldingActionKindV1 action_kind =
        ScaffoldingActionKindV1::kEnsureOrValidateParent;
    ScaffoldingObjectRoleV1 object_role =
        ScaffoldingObjectRoleV1::kCaptureDirectory;
    ScaffoldingObjectPostStateV1 post_state =
        ScaffoldingObjectPostStateV1::kAbsent;
    bool completed = false;
};

// This report is the bounded terminal audit for SCAFFOLDING_ONLY.  It binds
// the immutable snapshot and action vectors, but never claims that the
// snapshot digest can reconstruct a removed/renamed inode.  It represents no
// executor identity, Raw seal, Raw manifest, segment, marker or record.
struct ScaffoldingFinalizationReportV1 final {
    std::uint32_t schema_version =
        kScaffoldingFinalizationReportV1SchemaVersion;
    RawV1Identity reserve_state_uuid{};
    RawV1Identity finalization_cycle_id{};
    RawManifestNamespaceV1 planned_namespace{};
    RawV1Identity planned_recovery_attempt_id{};
    RawV1Digest immutable_grant_sha256{};
    RawV1Digest object_snapshot_sha256{};
    std::uint64_t observed_object_bitmap = 0U;
    std::uint64_t required_action_bitmap = 0U;
    std::vector<ScaffoldingObjectStateV1> objects;
    std::vector<ScaffoldingActionResultV1> actions;

    RawV1Digest journal_header_sha256{};
    std::uint64_t marker_count = 0U;
    std::uint64_t segment_count = 0U;
    std::uint64_t record_count = 0U;
    bool index_absent = false;
    bool manifest_absent = false;
    bool control_absent = false;
    RawV1Digest empty_anchor_tombstone_sha256{};
};

enum class ScaffoldingFinalizationReportV1Error : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidArgument,
    kInvalidNamespace,
    kInvalidIdentity,
    kInvalidSnapshot,
    kInvalidObjectVector,
    kInvalidActionVector,
    kInvalidPostState,
    kNotHeaderOnly,
    kGrantKindMismatch,
    kSidecarMismatch,
    kInvalidCanonicalJson,
    kFilenameInvalid,
    kEncodedSizeExceeded,
    kAllocationFailure,
};

[[nodiscard]] std::string_view
ScaffoldingFinalizationReportV1ErrorName(
    ScaffoldingFinalizationReportV1Error error) noexcept;

class BuiltScaffoldingFinalizationReportV1 final {
public:
    ~BuiltScaffoldingFinalizationReportV1() = default;

    BuiltScaffoldingFinalizationReportV1(
        const BuiltScaffoldingFinalizationReportV1&) = delete;
    BuiltScaffoldingFinalizationReportV1& operator=(
        const BuiltScaffoldingFinalizationReportV1&) = delete;
    BuiltScaffoldingFinalizationReportV1(
        BuiltScaffoldingFinalizationReportV1&&) = delete;
    BuiltScaffoldingFinalizationReportV1& operator=(
        BuiltScaffoldingFinalizationReportV1&&) = delete;

    [[nodiscard]] const ScaffoldingFinalizationReportV1&
    model() const noexcept {
        return model_;
    }
    [[nodiscard]] std::string_view canonical_jcs() const noexcept {
        return canonical_jcs_;
    }
    [[nodiscard]] std::string_view filename() const noexcept {
        return filename_;
    }
    [[nodiscard]] const RawV1Digest& report_sha256() const noexcept {
        return report_sha256_;
    }

private:
    friend ScaffoldingFinalizationReportV1Error
    BuildScaffoldingFinalizationReportCapabilityV1(
        const ScaffoldingFinalizationReportV1&,
        std::uint8_t,
        const BuiltEmptyAnchorTombstoneV1&,
        std::unique_ptr<
            BuiltScaffoldingFinalizationReportV1>*) noexcept;

    BuiltScaffoldingFinalizationReportV1(
        ScaffoldingFinalizationReportV1 model,
        std::string canonical_jcs,
        std::string filename,
        RawV1Digest report_sha256) noexcept
        : model_(std::move(model)),
          canonical_jcs_(std::move(canonical_jcs)),
          filename_(std::move(filename)),
          report_sha256_(report_sha256) {}

    ScaffoldingFinalizationReportV1 model_{};
    std::string canonical_jcs_;
    std::string filename_;
    RawV1Digest report_sha256_{};
};

[[nodiscard]] ScaffoldingFinalizationReportV1Error
ValidateScaffoldingFinalizationReportV1(
    const ScaffoldingFinalizationReportV1& report) noexcept;

[[nodiscard]] ScaffoldingFinalizationReportV1Error
EncodeScaffoldingFinalizationReportV1Jcs(
    const ScaffoldingFinalizationReportV1& report,
    std::string* output) noexcept;

[[nodiscard]] ScaffoldingFinalizationReportV1Error
ParseScaffoldingFinalizationReportV1Jcs(
    std::string_view exact_bytes,
    ScaffoldingFinalizationReportV1* output) noexcept;

// scaffolding-<cycle-id>-<immutable-grant-sha256>.json
[[nodiscard]] ScaffoldingFinalizationReportV1Error
ScaffoldingFinalizationReportV1Filename(
    const ScaffoldingFinalizationReportV1& report,
    std::string* output) noexcept;

[[nodiscard]] ScaffoldingFinalizationReportV1Error
BuildScaffoldingFinalizationReportCapabilityV1(
    const ScaffoldingFinalizationReportV1& report,
    std::uint8_t immutable_grant_flags,
    const BuiltEmptyAnchorTombstoneV1& empty_tombstone,
    std::unique_ptr<
        BuiltScaffoldingFinalizationReportV1>* output) noexcept;

}  // namespace l2flow::ingress
