#pragma once

#include "l2flow/ingress/empty_anchor_tombstone_v1.h"
#include "l2flow/ingress/raw_manifest_v1.h"
#include "l2flow/ingress/raw_recovery_executor.h"
#include "l2flow/ingress/raw_reserve_registry.h"
#include "l2flow/ingress/raw_sealed_certificate_v1.h"
#include "l2flow/ingress/raw_wal_writer.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace l2flow::ingress {

inline constexpr std::uint32_t
    kRecoveryMaintenanceReportV1SchemaVersion = 1U;
inline constexpr std::size_t
    kRecoveryMaintenanceReportV1MaximumBytes = 65'536U;
inline constexpr std::size_t
    kRecoveryMaintenanceReportV1SchemaBytes = 21'235U;
inline constexpr std::string_view
    kRecoveryMaintenanceReportV1SchemaSha256Hex =
        "5bb9cf47bfac7d4747dd70e7f83095f7"
        "e43aaa318dac82b90a1da79726ac5420";

enum class RecoveryMaintenanceOpenVariantV1
    : std::uint8_t {
    kReuseOpen = 1U,
    kNewOpenAfterSealed = 2U,
};

enum class RecoveryMaintenanceIntentV1
    : std::uint8_t {
    kResumeConnect = 1U,
    kRecoverSealOnly = 2U,
};

enum class RecoveryMaintenanceResultV1
    : std::uint8_t {
    kResumedOpen = 1U,
    kSealedRaw = 2U,
    kEmptyAnchorOnly = 3U,
};

enum class RecoveryMaintenancePreviousTerminalKindV1
    : std::uint8_t {
    kNone = 0U,
    kCanonicalZero = 1U,
    kSealed = 2U,
};

struct RecoveryMaintenanceCursorV1 final {
    std::uint32_t segment_sequence = 0U;
    std::uint64_t global_wal_pos = 0U;
    std::uint64_t ingress_sequence = 0U;
    std::uint32_t marker_flags = 0U;
    std::uint64_t segment_offset = 0U;

    friend bool operator==(
        const RecoveryMaintenanceCursorV1&,
        const RecoveryMaintenanceCursorV1&) = default;
};

struct RecoveryMaintenanceClosedFrontierV1 final {
    std::uint64_t closed_entry_count = 0U;
    RawV1Digest closed_prefix_sha256{};
    RawV1Digest frontier_seal_marker_sha256{};
    std::uint32_t frontier_segment_sequence = 0U;
    RawV1Digest frontier_segment_sha256{};
};

struct RecoveryMaintenanceRangeV1 final {
    std::uint64_t analyzed_journal_size = 0U;
    std::uint64_t final_durable_journal_size = 0U;
    std::uint64_t initial_accepted_journal_size = 0U;
    RecoveryMaintenanceCursorV1 initial_durable_cursor{};
    std::uint64_t post_repair_journal_size = 0U;
    std::uint32_t range_segment_sequence = 0U;
    std::uint64_t raw_append_begin_offset = 0U;
    std::uint64_t raw_append_end_offset = 0U;
    std::uint64_t raw_repair_begin_offset = 0U;
    std::uint64_t raw_repair_end_offset = 0U;
};

struct RecoveryMaintenanceEndpointMarkerV1 final {
    RecoveryMaintenanceCursorV1 cursor{};
    RawV1DurableMarkerWire marker_bytes{};
    RawV1Digest marker_sha256{};
};

struct RecoveryMaintenanceReuseSegmentV1 final {
    std::uint64_t reported_logical_end_offset = 0U;
    std::uint64_t segment_base_wal_pos = 0U;
    RawV1Digest segment_header_sha256{};
    RawV1Digest segment_prefix_sha256{};
    std::uint32_t segment_sequence = 0U;
};

struct RecoveryMaintenanceNewSegmentV1 final {
    std::uint64_t segment_base_wal_pos = 0U;
    RawV1Digest segment_header_sha256{};
    std::uint32_t segment_sequence = 0U;
};

struct RecoveryMaintenancePreviousTerminalV1 final {
    RecoveryMaintenancePreviousTerminalKindV1 kind =
        RecoveryMaintenancePreviousTerminalKindV1::kNone;
    RawV1DurableMarkerWire marker_bytes{};
    RawV1Digest marker_sha256{};
    std::uint32_t segment_sequence = 0U;
    RawV1Digest segment_sha256{};
};

// Builder-only input for NEW_OPEN_AFTER_SEALED. `kind` is either an exact
// preceding SEGMENT_SEALED boundary or the canonical zero terminal used by a
// pure journal anchor. At most one reopens_* digest may be nonzero.
struct RecoveryMaintenanceNewOpenPredecessorV1 final {
    RecoveryMaintenancePreviousTerminalV1 terminal{};
    RawV1Digest reopens_empty_tombstone_sha256{};
    RawV1Digest reopens_sealed_raw_certificate_sha256{};
};

struct RecoveryMaintenanceOpenBoundaryV1 final {
    RecoveryMaintenanceEndpointMarkerV1 endpoint_marker{};
    RawV1Digest manifest_entry_commitment_sha256{};
    std::uint64_t manifest_generation = 0U;
    RecoveryMaintenanceOpenVariantV1 open_variant =
        RecoveryMaintenanceOpenVariantV1::kReuseOpen;
    RecoveryMaintenanceNewSegmentV1 new_segment{};
    RecoveryMaintenancePreviousTerminalV1 previous_terminal{};
    RawV1Digest reopens_empty_tombstone_sha256{};
    RawV1Digest reopens_sealed_raw_certificate_sha256{};
    RecoveryMaintenanceReuseSegmentV1 reuse_segment{};
};

struct RecoveryMaintenanceSealedBoundaryV1 final {
    RawV1DurableMarkerWire accepted_sealed_marker_bytes{};
    RawV1Digest accepted_sealed_marker_sha256{};
    std::uint64_t last_segment_base_wal_pos = 0U;
    std::uint32_t last_segment_flags = 0U;
    std::uint64_t last_segment_logical_length = 0U;
    std::uint32_t last_segment_sequence = 0U;
    RawV1Digest last_segment_sha256{};
};

// Durable bytes contain no writer/executor or reserve-cycle identity. The
// recovery attempt is the immutable registry attempt fixed before mutation.
// `result` is the tag for the three frozen payload shapes. RESUMED_OPEN keeps
// the historical C++ projections below; SEALED_RAW uses sealed_boundary and
// EMPTY_ANCHOR_ONLY uses the three zero counts. Fields that are inapplicable
// to a tag are encoded as JSON null and must remain zero/disengaged here.
struct RecoveryMaintenanceReportV1 final {
    std::uint32_t schema_version =
        kRecoveryMaintenanceReportV1SchemaVersion;
    RecoveryMaintenanceIntentV1 intent =
        RecoveryMaintenanceIntentV1::kResumeConnect;
    RecoveryMaintenanceResultV1 result =
        RecoveryMaintenanceResultV1::kResumedOpen;
    RawManifestNamespaceV1 namespace_identity{};
    RecoveryMaintenanceClosedFrontierV1 closed_frontier{};
    RawV1Digest
        current_empty_anchor_tombstone_sha256{};
    RawV1Digest
        current_sealed_raw_certificate_sha256{};
    RecoveryMaintenanceCursorV1 final_durable_cursor{};
    RawV1Digest journal_header_sha256{};
    RawRecoveryJournalTailV1 journal_tail =
        RawRecoveryJournalTailV1::kNone;
    std::optional<std::uint64_t> marker_count;
    RecoveryMaintenanceOpenBoundaryV1 open_boundary{};
    std::optional<std::uint64_t> record_count;
    RawV1Identity recovery_attempt_id{};
    RecoveryMaintenanceRangeV1 recovery_range{};
    RecoveryMaintenanceSealedBoundaryV1 sealed_boundary{};
    std::optional<std::uint64_t> segment_count;
    RawRecoverySegmentTailV1 segment_tail =
        RawRecoverySegmentTailV1::kNone;
};

enum class RecoveryMaintenanceReportV1Error
    : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidArgument,
    kInvalidNamespace,
    kInvalidRecoveryAttempt,
    kInvalidJournalHeader,
    kRecoveryPlanFatal,
    kRecoveryExecutionInvalid,
    kInvalidManifest,
    kMissingOpenManifest,
    kOpenSegmentInvalid,
    kOpenSegmentMismatch,
    kMarkerInvalid,
    kCursorMismatch,
    kRangeInvalid,
    kFrontierMismatch,
    kVariantMismatch,
    kSidecarMismatch,
    kCommitmentMismatch,
    kInvalidCanonicalJson,
    kFilenameInvalid,
    kEncodedSizeExceeded,
    kAllocationFailure,
};

[[nodiscard]] std::string_view
RecoveryMaintenanceReportV1ErrorName(
    RecoveryMaintenanceReportV1Error error) noexcept;

class BuiltRecoveryMaintenanceReportV1 final {
public:
    ~BuiltRecoveryMaintenanceReportV1() = default;

    BuiltRecoveryMaintenanceReportV1(
        const BuiltRecoveryMaintenanceReportV1&) = delete;
    BuiltRecoveryMaintenanceReportV1& operator=(
        const BuiltRecoveryMaintenanceReportV1&) = delete;
    BuiltRecoveryMaintenanceReportV1(
        BuiltRecoveryMaintenanceReportV1&&) = delete;
    BuiltRecoveryMaintenanceReportV1& operator=(
        BuiltRecoveryMaintenanceReportV1&&) = delete;

    [[nodiscard]] const RecoveryMaintenanceReportV1&
    model() const noexcept {
        return model_;
    }
    [[nodiscard]] std::string_view
    canonical_jcs() const noexcept {
        return canonical_jcs_;
    }
    [[nodiscard]] std::string_view
    filename() const noexcept {
        return filename_;
    }
    [[nodiscard]] const RawV1Digest&
    report_sha256() const noexcept {
        return report_sha256_;
    }
    [[nodiscard]] const RawV1Identity&
    writer_instance() const noexcept {
        return writer_instance_;
    }
    [[nodiscard]] const RecoveryMaintenanceCursorV1&
    control_cursor() const noexcept {
        return control_cursor_;
    }

private:
    friend RecoveryMaintenanceReportV1Error
    BuildResumedOpenRecoveryMaintenanceReportV1(
        const RawReserveRegistryEntryKeyV1&,
        const RawV1JournalHeaderWire&,
        const RawRecoveryPlanV1&,
        const RawRecoveryExecutionResultV1&,
        std::uint64_t,
        const RawManifestV1&,
        std::shared_ptr<
            const std::vector<std::byte>>,
        const RawWalSinkIdentityV1&,
        const RawWalWriterSnapshot&,
        const RecoveryMaintenanceNewOpenPredecessorV1*,
        std::unique_ptr<
            BuiltRecoveryMaintenanceReportV1>*) noexcept;
    friend RecoveryMaintenanceReportV1Error
    BuildSealedRawRecoveryMaintenanceReportV1(
        const RawReserveRegistryEntryKeyV1&,
        const RawV1JournalHeaderWire&,
        const RawRecoveryPlanV1&,
        const RawRecoveryExecutionResultV1&,
        std::uint64_t,
        const BuiltSealedRawCertificateV1&,
        std::unique_ptr<
            BuiltRecoveryMaintenanceReportV1>*) noexcept;
    friend RecoveryMaintenanceReportV1Error
    BuildEmptyAnchorOnlyRecoveryMaintenanceReportV1(
        const RawReserveRegistryEntryKeyV1&,
        const RawV1JournalHeaderWire&,
        const RawRecoveryPlanV1&,
        const RawRecoveryExecutionResultV1&,
        std::uint64_t,
        const BuiltEmptyAnchorTombstoneV1&,
        std::unique_ptr<
            BuiltRecoveryMaintenanceReportV1>*) noexcept;

    BuiltRecoveryMaintenanceReportV1(
        RecoveryMaintenanceReportV1 model,
        std::string canonical_jcs,
        std::string filename,
        RawV1Digest report_sha256,
        RawV1Identity writer_instance,
        RecoveryMaintenanceCursorV1
            control_cursor) noexcept
        : model_(std::move(model)),
          canonical_jcs_(std::move(canonical_jcs)),
          filename_(std::move(filename)),
          report_sha256_(report_sha256),
          writer_instance_(writer_instance),
          control_cursor_(control_cursor) {}

    RecoveryMaintenanceReportV1 model_{};
    std::string canonical_jcs_;
    std::string filename_;
    RawV1Digest report_sha256_{};
    RawV1Identity writer_instance_{};
    RecoveryMaintenanceCursorV1 control_cursor_{};
};

// A null predecessor builds REUSE_OPEN. A non-null predecessor builds
// NEW_OPEN_AFTER_SEALED and must describe either the exact prior sealed
// frontier or the canonical zero terminal. The exact open-segment buffer is
// used only to bind the immutable header/prefix SHA; durable bytes remain
// bounded and contain no segment payload.
[[nodiscard]] RecoveryMaintenanceReportV1Error
BuildResumedOpenRecoveryMaintenanceReportV1(
    const RawReserveRegistryEntryKeyV1& recovery_key,
    const RawV1JournalHeaderWire& journal_header_bytes,
    const RawRecoveryPlanV1& analysis,
    const RawRecoveryExecutionResultV1& execution,
    std::uint64_t analyzed_journal_size,
    const RawManifestV1& final_open_manifest,
    std::shared_ptr<const std::vector<std::byte>>
        final_open_segment,
    const RawWalSinkIdentityV1& final_sink_identity,
    const RawWalWriterSnapshot& final_wal,
    const RecoveryMaintenanceNewOpenPredecessorV1*
        new_open_predecessor,
    std::unique_ptr<
        BuiltRecoveryMaintenanceReportV1>* output) noexcept;

// Builds SEALED_RAW only from a successful exact recovery analysis/execution
// and the non-forgeable certificate capability produced by the sealed
// certificate builder. No caller-supplied digest or boolean can stand in for
// that sidecar authority.
[[nodiscard]] RecoveryMaintenanceReportV1Error
BuildSealedRawRecoveryMaintenanceReportV1(
    const RawReserveRegistryEntryKeyV1& recovery_key,
    const RawV1JournalHeaderWire& journal_header_bytes,
    const RawRecoveryPlanV1& analysis,
    const RawRecoveryExecutionResultV1& execution,
    std::uint64_t analyzed_journal_size,
    const BuiltSealedRawCertificateV1&
        sealed_certificate,
    std::unique_ptr<
        BuiltRecoveryMaintenanceReportV1>* output) noexcept;

// Builds EMPTY_ANCHOR_ONLY only from successful journal-only recovery facts
// and the non-forgeable zero-count tombstone capability.
[[nodiscard]] RecoveryMaintenanceReportV1Error
BuildEmptyAnchorOnlyRecoveryMaintenanceReportV1(
    const RawReserveRegistryEntryKeyV1& recovery_key,
    const RawV1JournalHeaderWire& journal_header_bytes,
    const RawRecoveryPlanV1& analysis,
    const RawRecoveryExecutionResultV1& execution,
    std::uint64_t analyzed_journal_size,
    const BuiltEmptyAnchorTombstoneV1&
        empty_anchor_tombstone,
    std::unique_ptr<
        BuiltRecoveryMaintenanceReportV1>* output) noexcept;

[[nodiscard]] RecoveryMaintenanceReportV1Error
ValidateRecoveryMaintenanceReportV1(
    const RecoveryMaintenanceReportV1& report) noexcept;

[[nodiscard]] RecoveryMaintenanceReportV1Error
EncodeRecoveryMaintenanceReportV1Jcs(
    const RecoveryMaintenanceReportV1& report,
    std::string* output) noexcept;

[[nodiscard]] RecoveryMaintenanceReportV1Error
ParseRecoveryMaintenanceReportV1Jcs(
    std::string_view exact_bytes,
    RecoveryMaintenanceReportV1* output) noexcept;

[[nodiscard]] RecoveryMaintenanceReportV1Error
RecoveryMaintenanceReportV1Filename(
    const RecoveryMaintenanceReportV1& report,
    std::string* output) noexcept;

}  // namespace l2flow::ingress
