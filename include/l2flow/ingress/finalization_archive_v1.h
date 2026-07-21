#pragma once

#include "l2flow/ingress/raw_manifest_v1.h"
#include "l2flow/ingress/reserve_state_v1.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace l2flow::ingress {

inline constexpr std::uint32_t
    kFinalizationArchiveV1SchemaVersion = 1U;
inline constexpr std::size_t
    kFinalizationArchiveV1MaximumManifestBytes =
        1U * 1024U * 1024U;
inline constexpr std::size_t
    kFinalizationArchiveV1MaximumRawManifestBytes =
        16U * 1024U * 1024U;
inline constexpr std::size_t
    kFinalizationArchiveV1MaximumTotalArtifactBytes =
        272U * 1024U * 1024U;
inline constexpr std::size_t
    kFinalizationArchiveV1MaximumArtifacts = 128U;
inline constexpr std::size_t
    kFinalizationArchiveV1MaximumSourceLocatorBytes = 512U;

inline constexpr std::string_view
    kFinalizationArchiveV1ManifestFilename =
        "archive.json";
inline constexpr std::string_view
    kFinalizationArchiveV1StateHeaderFilename =
        "state-header.bin";
inline constexpr std::string_view
    kFinalizationArchiveV1AllDoneSlotFilename =
        "state-slot.bin";
inline constexpr std::string_view
    kFinalizationArchiveV1EvidenceDirectory =
        "evidence";

// Updated together with schemas/finalization_archive_v1.json.
inline constexpr std::size_t
    kFinalizationArchiveV1SchemaBytes = 3'972U;
inline constexpr std::string_view
    kFinalizationArchiveV1SchemaSha256Hex =
        "bbfa09e135edb5f1f0e354c9481948e"
        "b6d555e4ddf1082e30c8c38a64744ce54";

enum class FinalizationArchiveArtifactTypeV1
    : std::uint8_t {
    kFinalizationReport = 1U,
    kScaffoldingFinalizationReport = 2U,
    kPreexistingRecoveryReport = 3U,
    kRawManifest = 4U,
    kSealedRawCertificate = 5U,
    kEmptyAnchorTombstone = 6U,
};

// Caller-owned exact evidence.  The builder parses and canonicalizes every
// JSON artifact, cross-validates it against the all-DONE state, and copies
// the bytes into the resulting capability.  Publication never opens a
// source locator.  The later maintenance cleanup may resolve only the
// capability-frozen, namespace-derived locator through an authoritative
// published archive receipt.
struct FinalizationArchiveArtifactInputV1 final {
    FinalizationArchiveArtifactTypeV1 artifact_type =
        FinalizationArchiveArtifactTypeV1::
            kFinalizationReport;
    // Only non-report retained artifacts supply a locator.  Cleanup-eligible
    // report locators are builder-derived and this field must be empty.
    std::string source_locator;
    std::string exact_bytes;
    // FINALIZATION_REPORT and PREEXISTING_RECOVERY_REPORT require the exact
    // immutable short Raw-route slug mapped to the parsed source_stream_id
    // by the built-in ingress manifest (for example, 1001 -> sh-snapshot).
    // The builder rejects every other legal slug instead of accepting a
    // caller-selected route. All other types require this field to be empty.
    std::string source_stream_slug;
};

struct FinalizationArchiveArtifactV1 final {
    std::string archive_path;
    FinalizationArchiveArtifactTypeV1 artifact_type =
        FinalizationArchiveArtifactTypeV1::
            kFinalizationReport;
    std::uint64_t byte_count = 0U;
    RawManifestNamespaceV1 namespace_identity{};
    RawV1Digest sha256{};
    std::string source_locator;

    friend bool operator==(
        const FinalizationArchiveArtifactV1&,
        const FinalizationArchiveArtifactV1&) = default;
};

// FinalizationArchiveV1 is the bounded JCS manifest at the root of one
// immutable archive tree.  The state hash covers:
//   exact 4096-byte immutable header || exact 32768-byte highest all-DONE slot.
// Artifact paths are builder-derived names under evidence/; source locators
// are never used as archive-relative paths.
struct FinalizationArchiveV1 final {
    std::uint32_t schema_version =
        kFinalizationArchiveV1SchemaVersion;
    RawV1Identity reserve_state_uuid{};
    RawV1Identity finalization_cycle_id{};
    RawV1Digest reserve_state_schema_sha256{};
    std::uint64_t all_done_generation = 0U;
    std::string immutable_state_header_path =
        std::string(
            kFinalizationArchiveV1StateHeaderFilename);
    RawV1Digest immutable_state_header_sha256{};
    std::string all_done_slot_path =
        std::string(
            kFinalizationArchiveV1AllDoneSlotFilename);
    RawV1Digest all_done_slot_sha256{};
    RawV1Digest state_sha256{};
    std::uint64_t artifact_count = 0U;
    std::vector<FinalizationArchiveArtifactV1>
        artifacts;

    friend bool operator==(
        const FinalizationArchiveV1&,
        const FinalizationArchiveV1&) = default;
};

enum class FinalizationArchiveV1Error : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidArgument,
    kInvalidStateHeader,
    kStateNotConsumed,
    kStateNotAllDone,
    kInvalidStateSlot,
    kStateEncodingFailure,
    kInvalidIdentity,
    kInvalidArtifactType,
    kInvalidSourceLocator,
    kArtifactLimitExceeded,
    kArtifactSizeExceeded,
    kTotalSizeExceeded,
    kArtifactNotCanonical,
    kArtifactNamespaceMismatch,
    kArtifactIdentityMismatch,
    kArtifactHashMismatch,
    kArtifactFilenameMismatch,
    kDuplicateArtifact,
    kConflictingArtifact,
    kMissingFinalReport,
    kMissingPreexistingReport,
    kMissingManifest,
    kMissingSidecar,
    kUnexpectedArtifact,
    kGrantMismatch,
    kFrontierMismatch,
    kInvalidArchivePath,
    kInvalidCanonicalJson,
    kEncodedSizeExceeded,
    kAllocationFailure,
};

[[nodiscard]] std::string_view
FinalizationArchiveV1ErrorName(
    FinalizationArchiveV1Error error) noexcept;

class BuiltFinalizationArchiveArtifactV1 final {
public:
    BuiltFinalizationArchiveArtifactV1(
        const BuiltFinalizationArchiveArtifactV1&) =
        delete;
    BuiltFinalizationArchiveArtifactV1& operator=(
        const BuiltFinalizationArchiveArtifactV1&) =
        delete;
    BuiltFinalizationArchiveArtifactV1(
        BuiltFinalizationArchiveArtifactV1&&) noexcept =
        default;
    BuiltFinalizationArchiveArtifactV1& operator=(
        BuiltFinalizationArchiveArtifactV1&&) noexcept =
        default;

    [[nodiscard]] const
        FinalizationArchiveArtifactV1&
    model() const noexcept {
        return model_;
    }
    [[nodiscard]] std::string_view exact_bytes()
        const noexcept {
        return exact_bytes_;
    }

private:
    friend FinalizationArchiveV1Error
    BuildFinalizationArchiveCapabilityV1(
        const ReserveCoordinatorHeaderV1&,
        const ReserveStateSlotV1&,
        std::span<
            const FinalizationArchiveArtifactInputV1>,
        std::unique_ptr<
            class BuiltFinalizationArchiveV1>*) noexcept;

    BuiltFinalizationArchiveArtifactV1(
        FinalizationArchiveArtifactV1 model,
        std::string exact_bytes) noexcept
        : model_(std::move(model)),
          exact_bytes_(std::move(exact_bytes)) {}

    FinalizationArchiveArtifactV1 model_{};
    std::string exact_bytes_;
};

// The only POSIX publication input.  Its private constructor freezes the
// exact state wires, exact canonical evidence bytes, deterministic tree
// names, manifest bytes, and all content hashes after full causal
// cross-validation.
class BuiltFinalizationArchiveV1 final {
public:
    ~BuiltFinalizationArchiveV1() = default;

    BuiltFinalizationArchiveV1(
        const BuiltFinalizationArchiveV1&) = delete;
    BuiltFinalizationArchiveV1& operator=(
        const BuiltFinalizationArchiveV1&) = delete;
    BuiltFinalizationArchiveV1(
        BuiltFinalizationArchiveV1&&) = delete;
    BuiltFinalizationArchiveV1& operator=(
        BuiltFinalizationArchiveV1&&) = delete;

    [[nodiscard]] const FinalizationArchiveV1&
    model() const noexcept {
        return model_;
    }
    [[nodiscard]] std::string_view canonical_jcs()
        const noexcept {
        return canonical_jcs_;
    }
    [[nodiscard]] const RawV1Digest&
    manifest_sha256() const noexcept {
        return manifest_sha256_;
    }
    [[nodiscard]] std::string_view directory_name()
        const noexcept {
        return directory_name_;
    }
    // Logical directory locator recorded by continuation manifests.  The
    // trailing slash distinguishes this retained tree from a regular-file
    // maintenance locator.
    [[nodiscard]] std::string_view directory_locator()
        const noexcept {
        return directory_locator_;
    }
    [[nodiscard]] std::string_view
    temporary_directory_name() const noexcept {
        return temporary_directory_name_;
    }
    [[nodiscard]] std::span<const std::byte>
    state_header_bytes() const noexcept {
        return state_header_bytes_;
    }
    [[nodiscard]] std::span<const std::byte>
    all_done_slot_bytes() const noexcept {
        return all_done_slot_bytes_;
    }
    [[nodiscard]] const std::vector<
        BuiltFinalizationArchiveArtifactV1>&
    artifacts() const noexcept {
        return artifacts_;
    }

private:
    friend FinalizationArchiveV1Error
    BuildFinalizationArchiveCapabilityV1(
        const ReserveCoordinatorHeaderV1&,
        const ReserveStateSlotV1&,
        std::span<
            const FinalizationArchiveArtifactInputV1>,
        std::unique_ptr<
            BuiltFinalizationArchiveV1>*) noexcept;

    BuiltFinalizationArchiveV1(
        FinalizationArchiveV1 model,
        std::string canonical_jcs,
        RawV1Digest manifest_sha256,
        std::string directory_name,
        std::string directory_locator,
        std::string temporary_directory_name,
        ReserveStateV1HeaderWire state_header_bytes,
        ReserveStateV1SlotWire all_done_slot_bytes,
        std::vector<
            BuiltFinalizationArchiveArtifactV1>
            artifacts) noexcept
        : model_(std::move(model)),
          canonical_jcs_(std::move(canonical_jcs)),
          manifest_sha256_(manifest_sha256),
          directory_name_(std::move(directory_name)),
          directory_locator_(std::move(directory_locator)),
          temporary_directory_name_(
              std::move(temporary_directory_name)),
          state_header_bytes_(state_header_bytes),
          all_done_slot_bytes_(all_done_slot_bytes),
          artifacts_(std::move(artifacts)) {}

    FinalizationArchiveV1 model_{};
    std::string canonical_jcs_;
    RawV1Digest manifest_sha256_{};
    std::string directory_name_;
    std::string directory_locator_;
    std::string temporary_directory_name_;
    ReserveStateV1HeaderWire state_header_bytes_{};
    ReserveStateV1SlotWire all_done_slot_bytes_{};
    std::vector<
        BuiltFinalizationArchiveArtifactV1>
        artifacts_;
};

[[nodiscard]] FinalizationArchiveV1Error
ValidateFinalizationArchiveV1(
    const FinalizationArchiveV1& archive) noexcept;

[[nodiscard]] FinalizationArchiveV1Error
EncodeFinalizationArchiveV1Jcs(
    const FinalizationArchiveV1& archive,
    std::string* output) noexcept;

[[nodiscard]] FinalizationArchiveV1Error
ParseFinalizationArchiveV1Jcs(
    std::string_view exact_bytes,
    FinalizationArchiveV1* output) noexcept;

// finalization-<32-lowercase-hex-old-reserve-uuid>-
// <32-lowercase-hex-cycle-id>
[[nodiscard]] FinalizationArchiveV1Error
FinalizationArchiveV1DirectoryName(
    const FinalizationArchiveV1& archive,
    std::string* output) noexcept;

// .<final-dir>.finalization-archive-v1.tmp
[[nodiscard]] FinalizationArchiveV1Error
FinalizationArchiveV1TemporaryDirectoryName(
    const FinalizationArchiveV1& archive,
    std::string* output) noexcept;

[[nodiscard]] FinalizationArchiveV1Error
BuildFinalizationArchiveCapabilityV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& all_done_slot,
    std::span<
        const FinalizationArchiveArtifactInputV1>
        artifacts,
    std::unique_ptr<
        BuiltFinalizationArchiveV1>* output) noexcept;

}  // namespace l2flow::ingress
