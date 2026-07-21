#pragma once

#include "l2flow/ingress/empty_anchor_tombstone_v1.h"
#include "l2flow/ingress/raw_sealed_certificate_v1.h"
#include "l2flow/ingress/reserve_state_v1.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace l2flow::ingress {

inline constexpr std::uint32_t kFinalizationReportV1SchemaVersion = 1U;
inline constexpr std::size_t kFinalizationReportV1MaximumBytes = 65'536U;
inline constexpr std::size_t kFinalizationReportV1MaximumSegments = 2U;

// Updated together with schemas/finalization_report_v1.json.
inline constexpr std::size_t kFinalizationReportV1SchemaBytes =
    13'370U;
inline constexpr std::string_view
    kFinalizationReportV1SchemaSha256Hex =
        "b393d8b1ec7b3af57c7f47ec50a40ff0"
        "68e04d471f1fb49ecf0e0315f1ad154e";

enum class FinalizationReportResultV1 : std::uint8_t {
    kSealedRaw = 1U,
    kEmptyAnchorOnly = 2U,
};

enum class FinalizationReportTailClassificationV1 : std::uint8_t {
    kNone = 1U,
    kPartialRecordDiscarded = 2U,
    kInvalidRecordDiscarded = 3U,
    kUnknown = 4U,
};

enum class FinalizationReportGapClassificationV1 : std::uint8_t {
    kNone = 1U,
    kAckedRingDrained = 2U,
    kFencedRingLostUnknown = 3U,
    kUnknown = 4U,
};

enum class FinalizationReportSegmentKindV1 : std::uint8_t {
    kCurrent = 1U,
    kFinalizationContinuation = 2U,
};

struct FinalizationReportCursorV1 final {
    std::uint32_t segment_sequence = 0U;
    std::uint64_t global_wal_pos = 0U;
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t segment_offset = 0U;

    friend bool operator==(
        const FinalizationReportCursorV1&,
        const FinalizationReportCursorV1&) = default;
};

struct FinalizationReportSegmentV1 final {
    FinalizationReportSegmentKindV1 kind =
        FinalizationReportSegmentKindV1::kCurrent;
    std::uint32_t segment_sequence = 0U;
    std::uint64_t segment_base_wal_pos = 0U;
    std::uint64_t logical_length = 0U;
    RawV1Digest segment_sha256{};
};

struct FinalizationReportSealV1 final {
    std::uint32_t segment_sequence = 0U;
    RawV1DurableMarkerWire accepted_sealed_marker_bytes{};
    RawV1Digest accepted_sealed_marker_sha256{};
};

struct FinalizationReportManifestFrontierV1 final {
    std::uint64_t closed_entry_count = 0U;
    std::uint32_t frontier_segment_sequence = 0U;
    RawV1Digest frontier_segment_sha256{};
    RawV1Digest frontier_accepted_seal_sha256{};
    RawV1Digest closed_prefix_sha256{};
};

// Unsigned components preserve the exact observation without signed
// overflow or an ambiguous accounting sign.  Net change is
// allocated-released in each dimension.  Publication of this report is not
// included in these values.
struct RawAllocationDeltaBeforeReportV1 final {
    std::uint64_t allocated_bytes = 0U;
    std::uint64_t released_bytes = 0U;
    std::uint64_t allocated_inodes = 0U;
    std::uint64_t released_inodes = 0U;
};

struct FinalizationPreexistingRecoveryReportV1 final {
    RawV1Identity recovery_attempt_id{};
    RawV1Digest report_sha256{};
};

// The tag fixes the nullable shape:
// - SEALED_RAW has cursors, segments, final seal, frontier and certificate;
// - EMPTY_ANCHOR_ONLY has the exact journal hash, zero marker count and
//   tombstone, while every Raw segment/seal/frontier/cursor field is absent.
// No executor identity or whole-manifest content hash is representable.
struct FinalizationReportV1 final {
    std::uint32_t schema_version =
        kFinalizationReportV1SchemaVersion;
    RawV1Identity reserve_state_uuid{};
    RawV1Identity finalization_cycle_id{};
    RawManifestNamespaceV1 namespace_identity{};
    ReserveAckStatusV1 ack_status =
        ReserveAckStatusV1::kUnused;
    RawV1Identity ack_writer_instance{};
    RawV1Digest immutable_grant_sha256{};

    std::optional<FinalizationReportCursorV1>
        initial_append_cursor;
    std::optional<FinalizationReportCursorV1>
        initial_durable_cursor;
    std::optional<FinalizationReportCursorV1>
        final_append_cursor;
    std::optional<FinalizationReportCursorV1>
        final_durable_cursor;
    FinalizationReportTailClassificationV1
        tail_classification =
            FinalizationReportTailClassificationV1::kNone;
    bool tail_classification_valid = false;
    FinalizationReportGapClassificationV1
        gap_classification =
            FinalizationReportGapClassificationV1::kNone;
    bool gap_classification_valid = false;
    RawAllocationDeltaBeforeReportV1
        raw_allocation_delta_before_report{};

    FinalizationReportResultV1 result =
        FinalizationReportResultV1::kSealedRaw;
    std::vector<FinalizationReportSegmentV1> segments;
    std::optional<FinalizationReportSealV1> final_seal;
    std::optional<FinalizationReportManifestFrontierV1>
        manifest_frontier;
    RawV1Digest sealed_raw_certificate_sha256{};

    RawV1Digest journal_header_sha256{};
    std::optional<std::uint64_t> marker_count;
    RawV1Digest empty_anchor_tombstone_sha256{};

    std::optional<FinalizationPreexistingRecoveryReportV1>
        preexisting_recovery_report;
};

enum class FinalizationReportV1Error : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidArgument,
    kInvalidNamespace,
    kInvalidIdentity,
    kInvalidAck,
    kInvalidClassification,
    kInvalidCursor,
    kInvalidSegment,
    kInvalidSeal,
    kInvalidFrontier,
    kInvalidResultShape,
    kGrantKindMismatch,
    kSidecarMismatch,
    kInvalidPreexistingReport,
    kInvalidCanonicalJson,
    kFilenameInvalid,
    kEncodedSizeExceeded,
    kAllocationFailure,
};

[[nodiscard]] std::string_view FinalizationReportV1ErrorName(
    FinalizationReportV1Error error) noexcept;

class BuiltFinalizationReportV1 final {
public:
    ~BuiltFinalizationReportV1() = default;

    BuiltFinalizationReportV1(
        const BuiltFinalizationReportV1&) = delete;
    BuiltFinalizationReportV1& operator=(
        const BuiltFinalizationReportV1&) = delete;
    BuiltFinalizationReportV1(
        BuiltFinalizationReportV1&&) = delete;
    BuiltFinalizationReportV1& operator=(
        BuiltFinalizationReportV1&&) = delete;

    [[nodiscard]] const FinalizationReportV1&
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
    friend FinalizationReportV1Error
    BuildFinalizationReportCapabilityV1(
        const FinalizationReportV1&,
        std::uint8_t,
        const BuiltSealedRawCertificateV1*,
        const BuiltEmptyAnchorTombstoneV1*,
        std::unique_ptr<BuiltFinalizationReportV1>*) noexcept;

    BuiltFinalizationReportV1(
        FinalizationReportV1 model,
        std::string canonical_jcs,
        std::string filename,
        RawV1Digest report_sha256) noexcept
        : model_(std::move(model)),
          canonical_jcs_(std::move(canonical_jcs)),
          filename_(std::move(filename)),
          report_sha256_(report_sha256) {}

    FinalizationReportV1 model_{};
    std::string canonical_jcs_;
    std::string filename_;
    RawV1Digest report_sha256_{};
};

[[nodiscard]] FinalizationReportV1Error
ValidateFinalizationReportV1(
    const FinalizationReportV1& report) noexcept;

[[nodiscard]] FinalizationReportV1Error
EncodeFinalizationReportV1Jcs(
    const FinalizationReportV1& report,
    std::string* output) noexcept;

[[nodiscard]] FinalizationReportV1Error
ParseFinalizationReportV1Jcs(
    std::string_view exact_bytes,
    FinalizationReportV1* output) noexcept;

// finalization-<32-lowercase-hex-cycle-id>.json
[[nodiscard]] FinalizationReportV1Error
FinalizationReportV1Filename(
    const FinalizationReportV1& report,
    std::string* output) noexcept;

// The private publication capability is produced only after intrinsic
// validation, exact sidecar cross-validation and immutable grant-kind
// validation.  `immutable_grant_flags` is the frozen grant flag from the
// coordinator state; a future publisher must still bind this capability to
// its exact DEBITED action token.
[[nodiscard]] FinalizationReportV1Error
BuildFinalizationReportCapabilityV1(
    const FinalizationReportV1& report,
    std::uint8_t immutable_grant_flags,
    const BuiltSealedRawCertificateV1* sealed_certificate,
    const BuiltEmptyAnchorTombstoneV1* empty_tombstone,
    std::unique_ptr<BuiltFinalizationReportV1>* output) noexcept;

}  // namespace l2flow::ingress
