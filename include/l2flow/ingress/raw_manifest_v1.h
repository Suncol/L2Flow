#pragma once

#include "l2flow/ingress/raw_v1.h"

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace l2flow::ingress {

inline constexpr std::uint32_t kRawManifestV1SchemaVersion = 1U;
inline constexpr std::string_view
    kRawManifestOpenEntryCommitmentDomainV1 =
        "L2FLOW_RAW_OPEN_MANIFEST_ENTRY_COMMITMENT_V1";

struct RawManifestNamespaceV1 final {
    std::uint32_t capture_date = 0U;
    std::uint32_t source_stream_id = 0U;
    RawV1Identity stream_day_id{};

    friend bool operator==(
        const RawManifestNamespaceV1&,
        const RawManifestNamespaceV1&) = default;
};

enum class RawManifestSegmentStateV1 : std::uint8_t {
    kOpen = 0U,
    kClosed,
};

// A typed RawManifest entry. Integer fields wider than 32 bits are encoded as
// JSON decimal strings by EncodeRawManifestJcs; callers never provide their
// textual representation. The accepted marker wire includes its CRC and its
// digest binds those exact 48 bytes.
struct RawManifestSegmentEntryV1 final {
    RawManifestNamespaceV1 namespace_identity{};
    std::uint32_t segment_sequence = 0U;
    std::uint32_t segment_flags = 0U;
    RawManifestSegmentStateV1 state =
        RawManifestSegmentStateV1::kOpen;

    std::uint64_t segment_base_wal_pos = 0U;
    std::uint64_t segment_logical_length = 0U;
    RawV1Digest segment_sha256{};
    std::uint64_t record_count = 0U;
    std::optional<std::uint64_t>
        actual_first_ingress_sequence;
    std::optional<std::uint64_t>
        actual_last_ingress_sequence;
    // The immutable SegmentHeader first_ingress_sequence: for an empty
    // segment this remains the sequence expected from the next real record.
    std::uint64_t next_expected_first_ingress_sequence = 0U;

    RawV1DurableMarkerWire accepted_marker_bytes{};
    RawV1Digest accepted_marker_sha256{};

    RawV1Identity host_uuid{};
    RawV1Identity linux_boot_id{};
    std::uint32_t clock_epoch_algorithm = 0U;
    RawV1Digest clock_epoch_digest{};
    std::uint64_t clock_epoch_label = 0U;
    RawV1Digest sdk_archive_sha256{};
    RawV1Digest libmdl_api_sha256{};
    RawV1Digest endpoint_contract_sha256{};
    RawV1Digest config_sha256{};
    RawV1Digest raw_schema_sha256{};
    RawV1Digest build_manifest_sha256{};

    // These fields are all absent for a normal segment and all present for a
    // FINALIZATION_CONTINUATION segment. Locators are deterministic names,
    // not report/archive content hashes.
    RawV1Identity reserve_state_uuid{};
    RawV1Identity finalization_cycle_id{};
    RawV1Digest immutable_grant_sha256{};
    std::optional<std::string> maintenance_report_locator;
    std::optional<std::string> archive_locator;

    friend bool operator==(
        const RawManifestSegmentEntryV1&,
        const RawManifestSegmentEntryV1&) = default;
};

struct RawManifestV1 final {
    std::uint32_t schema_version =
        kRawManifestV1SchemaVersion;
    std::uint64_t manifest_generation = 0U;
    RawManifestNamespaceV1 namespace_identity{};
    std::vector<RawManifestSegmentEntryV1> closed_entries;
    std::optional<RawManifestSegmentEntryV1> open_entry;

    // This is stored explicitly on wire and must equal closed_entries.size().
    std::uint64_t closed_entry_count = 0U;
    // SHA-256 over the exact canonical RawManifestFrontierV1 bytes. The
    // frontier excludes manifest_generation and open_entry.
    RawV1Digest closed_prefix_sha256{};
};

enum class RawManifestV1Error : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kUnsupportedSchemaVersion,
    kInvalidNamespace,
    kClosedEntryCountMismatch,
    kClosedPrefixMismatch,
    kInvalidEntryState,
    kUnknownSegmentFlags,
    kInvalidIdentity,
    kInvalidDigest,
    kInvalidMarker,
    kMarkerHashMismatch,
    kMarkerMismatch,
    kInvalidRecordRange,
    kSegmentOrderViolation,
    kWalDiscontinuity,
    kIngressDiscontinuity,
    kInvalidFinalization,
    kInvalidUtf8,
    kGenerationRegression,
    kNotAppendOnly,
    kLengthOverflow,
    kAllocationFailure,
};

[[nodiscard]] std::string_view RawManifestV1ErrorName(
    RawManifestV1Error error) noexcept;

// Computes SHA-256 over one exact accepted-marker wire image.
[[nodiscard]] RawV1Digest ComputeAcceptedMarkerSha256(
    std::span<const std::byte> marker_bytes) noexcept;

// Computes the exact RawManifestFrontierV1 JCS bytes and digest for all
// closed entries in `manifest`. It intentionally does not compare the result
// with manifest.closed_prefix_sha256, allowing construction of a new model.
// canonical_frontier may be null.
[[nodiscard]] RawManifestV1Error ComputeClosedPrefix(
    const RawManifestV1& manifest,
    RawV1Digest* digest,
    std::string* canonical_frontier = nullptr) noexcept;

// Commits only the exact canonical open-entry object, not the mutable whole
// manifest. The V1 hash domain is:
// ASCII(kRawManifestOpenEntryCommitmentDomainV1) || 0x00 ||
// RFC 8785 JCS(RawManifestSegmentEntryV1).
[[nodiscard]] RawManifestV1Error
ComputeRawManifestOpenEntryCommitmentV1(
    const RawManifestV1& manifest,
    RawV1Digest* digest,
    std::string* canonical_open_entry = nullptr) noexcept;

// Checks intrinsic model invariants. When previous is supplied, this also
// enforces monotonic generation and append-only preservation of every
// previously closed entry and its reported frontier.
//
// This typed-model validator is not a general JSON parser. In particular,
// unknown-field policy and canonical re-encoding of externally supplied JSON
// bytes belong to the recovery parser layer.
[[nodiscard]] RawManifestV1Error ValidateManifestModel(
    const RawManifestV1& manifest,
    const RawManifestV1* previous = nullptr) noexcept;

// Emits RFC 8785-compatible canonical JSON for this integer/string-only V1
// model: UTF-8, no BOM, no trailing newline, UTF-16/JCS key order (all schema
// keys are ASCII), shortest JSON escapes, uint64 decimal strings, lowercase
// identity/hash/marker hex. The output is changed only on success.
[[nodiscard]] RawManifestV1Error EncodeRawManifestJcs(
    const RawManifestV1& manifest,
    std::string* output) noexcept;

}  // namespace l2flow::ingress
