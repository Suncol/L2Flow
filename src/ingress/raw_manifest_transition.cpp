#include "l2flow/ingress/raw_manifest_transition.h"

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <span>
#include <type_traits>
#include <utility>

namespace l2flow::ingress {
namespace {

template <std::size_t Size>
[[nodiscard]] bool IsZero(
    const std::array<std::byte, Size>& value) noexcept {
    return std::all_of(
        value.begin(),
        value.end(),
        [](std::byte byte) {
            return byte == std::byte{0};
        });
}

[[nodiscard]] bool CheckedAdd(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* result) noexcept {
    if (result == nullptr ||
        left >
            std::numeric_limits<std::uint64_t>::max() -
                right) {
        return false;
    }
    *result = left + right;
    return true;
}

[[nodiscard]] bool CheckedMultiply(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* result) noexcept {
    if (result == nullptr ||
        (left != 0U &&
         right >
             std::numeric_limits<std::uint64_t>::max() /
                 left)) {
        return false;
    }
    *result = left * right;
    return true;
}

[[nodiscard]] bool SameMarker(
    const DurableMarkerV1& left,
    const DurableMarkerV1& right) noexcept {
    return left.source_stream_id ==
               right.source_stream_id &&
           left.segment_sequence ==
               right.segment_sequence &&
           left.durable_global_wal_pos ==
               right.durable_global_wal_pos &&
           left.durable_ingress_sequence ==
               right.durable_ingress_sequence &&
           left.durable_segment_offset ==
               right.durable_segment_offset &&
           left.marker_crc32c == right.marker_crc32c &&
           left.marker_flags == right.marker_flags;
}

[[nodiscard]] bool SameNamespace(
    const RawManifestNamespaceV1& left,
    const RawManifestNamespaceV1& right) noexcept {
    return left.capture_date == right.capture_date &&
           left.source_stream_id ==
               right.source_stream_id &&
           left.stream_day_id == right.stream_day_id;
}

[[nodiscard]] bool SameImmutableSegmentIdentity(
    const RawManifestSegmentEntryV1& left,
    const RawManifestSegmentEntryV1& right) noexcept {
    return SameNamespace(
               left.namespace_identity,
               right.namespace_identity) &&
           left.segment_sequence == right.segment_sequence &&
           left.segment_flags == right.segment_flags &&
           left.segment_base_wal_pos ==
               right.segment_base_wal_pos &&
           left.next_expected_first_ingress_sequence ==
               right.next_expected_first_ingress_sequence &&
           left.host_uuid == right.host_uuid &&
           left.linux_boot_id == right.linux_boot_id &&
           left.clock_epoch_algorithm ==
               right.clock_epoch_algorithm &&
           left.clock_epoch_digest ==
               right.clock_epoch_digest &&
           left.clock_epoch_label ==
               right.clock_epoch_label &&
           left.sdk_archive_sha256 ==
               right.sdk_archive_sha256 &&
           left.libmdl_api_sha256 ==
               right.libmdl_api_sha256 &&
           left.endpoint_contract_sha256 ==
               right.endpoint_contract_sha256 &&
           left.config_sha256 == right.config_sha256 &&
           left.raw_schema_sha256 ==
               right.raw_schema_sha256 &&
           left.build_manifest_sha256 ==
               right.build_manifest_sha256 &&
           left.reserve_state_uuid ==
               right.reserve_state_uuid &&
           left.finalization_cycle_id ==
               right.finalization_cycle_id &&
           left.immutable_grant_sha256 ==
               right.immutable_grant_sha256 &&
           left.maintenance_report_locator ==
               right.maintenance_report_locator &&
           left.archive_locator == right.archive_locator;
}

[[nodiscard]] RawManifestNamespaceV1 NamespaceOf(
    const SegmentHeaderV1& segment) noexcept {
    return {
        segment.capture_date,
        segment.source_stream_id,
        segment.stream_day_id};
}

void CopyNormalSegmentIdentity(
    const SegmentHeaderV1& segment,
    RawManifestSegmentEntryV1* entry) noexcept {
    entry->namespace_identity = NamespaceOf(segment);
    entry->segment_sequence = segment.segment_sequence;
    entry->segment_flags = segment.segment_flags;
    entry->segment_base_wal_pos =
        segment.segment_base_wal_pos;
    entry->next_expected_first_ingress_sequence =
        segment.first_ingress_sequence;
    entry->host_uuid = segment.host_uuid;
    entry->linux_boot_id = segment.linux_boot_id;
    entry->clock_epoch_algorithm =
        segment.clock_epoch_algorithm;
    entry->clock_epoch_digest =
        segment.clock_epoch_digest;
    entry->clock_epoch_label =
        segment.clock_epoch_label;
    entry->sdk_archive_sha256 =
        segment.sdk_archive_sha256;
    entry->libmdl_api_sha256 =
        segment.libmdl_api_sha256;
    entry->endpoint_contract_sha256 =
        segment.endpoint_contract_sha256;
    entry->config_sha256 = segment.config_sha256;
    entry->raw_schema_sha256 =
        segment.raw_schema_sha256;
    entry->build_manifest_sha256 =
        segment.build_manifest_sha256;
}

[[nodiscard]] RawManifestTransitionErrorV1
EncodeNormalSegmentHeader(
    const SegmentHeaderV1& segment,
    RawV1SegmentHeaderWire* wire) noexcept {
    if ((segment.segment_flags &
         ~kRawV1SegmentFlagsMask) != 0U) {
        return RawManifestTransitionErrorV1::
            kInvalidSegmentHeader;
    }
    if ((segment.segment_flags &
         kRawV1FinalizationContinuation) != 0U) {
        return RawManifestTransitionErrorV1::
            kFinalizationContinuationUnsupported;
    }
    return EncodeSegmentHeaderV1(segment, wire) ==
                   RawV1Error::kNone
               ? RawManifestTransitionErrorV1::kNone
               : RawManifestTransitionErrorV1::
                     kInvalidSegmentHeader;
}

[[nodiscard]] RawManifestTransitionErrorV1
EncodeFinalizationContinuationSegmentHeader(
    const SegmentHeaderV1& segment,
    RawV1SegmentHeaderWire* wire) noexcept {
    if (segment.segment_flags !=
            kRawV1FinalizationContinuation ||
        l2flow::common::IsZeroIdentity(
            segment.reserve_state_uuid) ||
        l2flow::common::IsZeroIdentity(
            segment.finalization_cycle_id) ||
        IsZero(segment.immutable_grant_sha256)) {
        return RawManifestTransitionErrorV1::
            kInvalidSegmentHeader;
    }
    return EncodeSegmentHeaderV1(segment, wire) ==
                   RawV1Error::kNone
               ? RawManifestTransitionErrorV1::kNone
               : RawManifestTransitionErrorV1::
                     kInvalidSegmentHeader;
}

[[nodiscard]] RawManifestTransitionErrorV1
BuildFinalizationContinuationLocators(
    const SegmentHeaderV1& segment,
    std::string* maintenance_report_locator,
    std::string* archive_locator) noexcept {
    if (maintenance_report_locator == nullptr ||
        archive_locator == nullptr ||
        segment.segment_flags !=
            kRawV1FinalizationContinuation ||
        l2flow::common::IsZeroIdentity(
            segment.reserve_state_uuid) ||
        l2flow::common::IsZeroIdentity(
            segment.finalization_cycle_id) ||
        IsZero(segment.immutable_grant_sha256)) {
        return RawManifestTransitionErrorV1::
            kInvalidSegmentHeader;
    }
    try {
        const std::string reserve_uuid =
            l2flow::common::Identity128Hex(
                segment.reserve_state_uuid);
        const std::string cycle_id =
            l2flow::common::Identity128Hex(
                segment.finalization_cycle_id);
        std::string report =
            "maintenance/finalization-" +
            cycle_id + ".json";
        std::string archive =
            "reserve-audit/finalization-" +
            reserve_uuid + "-" + cycle_id + "/";
        maintenance_report_locator->swap(report);
        archive_locator->swap(archive);
        return RawManifestTransitionErrorV1::kNone;
    } catch (...) {
        return RawManifestTransitionErrorV1::
            kAllocationFailure;
    }
}

[[nodiscard]] bool JournalSizePlausibleForInitializedSegment(
    std::uint32_t segment_sequence,
    std::uint64_t journal_size) noexcept {
    if (journal_size < kRawV1JournalHeaderBytes ||
        (journal_size - kRawV1JournalHeaderBytes) %
                kRawV1DurableMarkerBytes !=
            0U) {
        return false;
    }

    // Each preceding normal segment contributes at least its header-only and
    // sealed marker. The current initialized segment contributes its
    // header-only marker.
    std::uint64_t preceding_twice = 0U;
    std::uint64_t minimum_markers = 0U;
    std::uint64_t minimum_marker_bytes = 0U;
    std::uint64_t minimum_journal_size = 0U;
    if (segment_sequence == 0U ||
        !CheckedMultiply(
            static_cast<std::uint64_t>(
                segment_sequence - 1U),
            2U,
            &preceding_twice) ||
        !CheckedAdd(
            preceding_twice,
            1U,
            &minimum_markers) ||
        !CheckedMultiply(
            minimum_markers,
            kRawV1DurableMarkerBytes,
            &minimum_marker_bytes) ||
        !CheckedAdd(
            kRawV1JournalHeaderBytes,
            minimum_marker_bytes,
            &minimum_journal_size)) {
        return false;
    }
    return journal_size >= minimum_journal_size;
}

[[nodiscard]] RawManifestTransitionErrorV1
ValidatePrevious(
    const RawManifestV1& previous) noexcept {
    return ValidateManifestModel(previous) ==
                   RawManifestV1Error::kNone
               ? RawManifestTransitionErrorV1::kNone
               : RawManifestTransitionErrorV1::
                     kInvalidPreviousManifest;
}

[[nodiscard]] RawManifestTransitionErrorV1
MapManifestError(RawManifestV1Error result) noexcept {
    if (result == RawManifestV1Error::kNone) {
        return RawManifestTransitionErrorV1::kNone;
    }
    if (result == RawManifestV1Error::kAllocationFailure) {
        return RawManifestTransitionErrorV1::
            kAllocationFailure;
    }
    if (result == RawManifestV1Error::kInvalidNamespace ||
        result ==
            RawManifestV1Error::kSegmentOrderViolation ||
        result == RawManifestV1Error::kWalDiscontinuity ||
        result == RawManifestV1Error::kIngressDiscontinuity ||
        result == RawManifestV1Error::kNotAppendOnly) {
        return RawManifestTransitionErrorV1::
            kSegmentIdentityMismatch;
    }
    return RawManifestTransitionErrorV1::
        kManifestValidation;
}

[[nodiscard]] RawManifestTransitionErrorV1
SetClosedFrontier(RawManifestV1* manifest) noexcept {
    RawV1Digest digest{};
    const RawManifestV1Error result =
        ComputeClosedPrefix(*manifest, &digest);
    if (result != RawManifestV1Error::kNone) {
        return MapManifestError(result);
    }
    manifest->closed_prefix_sha256 = digest;
    return RawManifestTransitionErrorV1::kNone;
}

[[nodiscard]] RawManifestTransitionErrorV1
ValidateTransition(
    const RawManifestV1& candidate,
    const RawManifestV1* previous) noexcept {
    return MapManifestError(
        ValidateManifestModel(candidate, previous));
}

template <typename Value>
void CommitOutput(Value* output, Value* candidate) noexcept {
    static_assert(std::is_nothrow_swappable_v<Value>);
    using std::swap;
    swap(*output, *candidate);
}

}  // namespace

std::string_view RawManifestTransitionErrorV1Name(
    RawManifestTransitionErrorV1 error) noexcept {
    switch (error) {
    case RawManifestTransitionErrorV1::kNone:
        return "none";
    case RawManifestTransitionErrorV1::kNullOutput:
        return "null_output";
    case RawManifestTransitionErrorV1::
        kInvalidSegmentHeader:
        return "invalid_segment_header";
    case RawManifestTransitionErrorV1::
        kFinalizationContinuationUnsupported:
        return "finalization_continuation_unsupported";
    case RawManifestTransitionErrorV1::
        kInvalidInitializedSnapshot:
        return "invalid_initialized_snapshot";
    case RawManifestTransitionErrorV1::
        kInvalidSealedMetadata:
        return "invalid_sealed_metadata";
    case RawManifestTransitionErrorV1::
        kInvalidPreviousManifest:
        return "invalid_previous_manifest";
    case RawManifestTransitionErrorV1::kMissingOpenEntry:
        return "missing_open_entry";
    case RawManifestTransitionErrorV1::
        kUnexpectedOpenEntry:
        return "unexpected_open_entry";
    case RawManifestTransitionErrorV1::
        kSegmentIdentityMismatch:
        return "segment_identity_mismatch";
    case RawManifestTransitionErrorV1::
        kGenerationOverflow:
        return "generation_overflow";
    case RawManifestTransitionErrorV1::
        kClosedEntryCountOverflow:
        return "closed_entry_count_overflow";
    case RawManifestTransitionErrorV1::
        kRecoveredMetadataOrder:
        return "recovered_metadata_order";
    case RawManifestTransitionErrorV1::
        kRecoveredClosedPrefixConflict:
        return "recovered_closed_prefix_conflict";
    case RawManifestTransitionErrorV1::
        kRecoveredOpenConflict:
        return "recovered_open_conflict";
    case RawManifestTransitionErrorV1::
        kManifestValidation:
        return "manifest_validation";
    case RawManifestTransitionErrorV1::
        kAllocationFailure:
        return "allocation_failure";
    }
    return "unknown";
}

RawManifestTransitionErrorV1
BuildOpenRawManifestEntryV1(
    const SegmentHeaderV1& segment,
    const RawWalWriterSnapshot& initialized_snapshot,
    RawManifestSegmentEntryV1* output) noexcept {
    if (output == nullptr) {
        return RawManifestTransitionErrorV1::kNullOutput;
    }

    RawV1SegmentHeaderWire header_wire{};
    const RawManifestTransitionErrorV1 header_result =
        EncodeNormalSegmentHeader(segment, &header_wire);
    if (header_result !=
        RawManifestTransitionErrorV1::kNone) {
        return header_result;
    }

    std::uint64_t expected_global_wal_pos = 0U;
    if (!CheckedAdd(
            segment.segment_base_wal_pos,
            kRawV1SegmentHeaderBytes,
            &expected_global_wal_pos) ||
        segment.first_ingress_sequence == 0U ||
        !initialized_snapshot.initialized ||
        initialized_snapshot.sealed ||
        initialized_snapshot.closed ||
        initialized_snapshot.fatal ||
        initialized_snapshot.append !=
            initialized_snapshot.durable ||
        initialized_snapshot.durable.segment_offset !=
            kRawV1SegmentHeaderBytes ||
        initialized_snapshot.durable.global_wal_pos !=
            expected_global_wal_pos ||
        initialized_snapshot.durable.ingress_sequence !=
            segment.first_ingress_sequence - 1U ||
        !JournalSizePlausibleForInitializedSegment(
            segment.segment_sequence,
            initialized_snapshot.journal_logical_size)) {
        return RawManifestTransitionErrorV1::
            kInvalidInitializedSnapshot;
    }

    DurableMarkerV1 marker{};
    marker.source_stream_id = segment.source_stream_id;
    marker.segment_sequence = segment.segment_sequence;
    marker.durable_global_wal_pos =
        initialized_snapshot.durable.global_wal_pos;
    marker.durable_ingress_sequence =
        initialized_snapshot.durable.ingress_sequence;
    marker.durable_segment_offset =
        initialized_snapshot.durable.segment_offset;
    marker.marker_flags = 0U;

    RawManifestSegmentEntryV1 candidate{};
    candidate.state = RawManifestSegmentStateV1::kOpen;
    candidate.segment_logical_length =
        kRawV1SegmentHeaderBytes;
    candidate.segment_sha256 =
        l2flow::common::ComputeSha256(header_wire);
    candidate.record_count = 0U;
    if (IsZero(candidate.segment_sha256) ||
        EncodeDurableMarkerV1(
            marker,
            &candidate.accepted_marker_bytes) !=
            RawV1Error::kNone) {
        return RawManifestTransitionErrorV1::
            kInvalidInitializedSnapshot;
    }
    candidate.accepted_marker_sha256 =
        ComputeAcceptedMarkerSha256(
            candidate.accepted_marker_bytes);
    CopyNormalSegmentIdentity(segment, &candidate);
    CommitOutput(output, &candidate);
    return RawManifestTransitionErrorV1::kNone;
}

RawManifestTransitionErrorV1
FinalizationContinuationLocatorsV1(
    const SegmentHeaderV1& segment,
    std::string* maintenance_report_locator,
    std::string* archive_locator) noexcept {
    return BuildFinalizationContinuationLocators(
        segment,
        maintenance_report_locator,
        archive_locator);
}

RawManifestTransitionErrorV1
BuildOpenFinalizationContinuationRawManifestEntryV1(
    const SegmentHeaderV1& segment,
    const RawWalWriterSnapshot& initialized_snapshot,
    RawManifestSegmentEntryV1* output) noexcept {
    if (output == nullptr) {
        return RawManifestTransitionErrorV1::kNullOutput;
    }

    RawV1SegmentHeaderWire header_wire{};
    const RawManifestTransitionErrorV1 header_result =
        EncodeFinalizationContinuationSegmentHeader(
            segment, &header_wire);
    if (header_result !=
        RawManifestTransitionErrorV1::kNone) {
        return header_result;
    }

    std::uint64_t expected_global_wal_pos = 0U;
    if (!CheckedAdd(
            segment.segment_base_wal_pos,
            kRawV1SegmentHeaderBytes,
            &expected_global_wal_pos) ||
        segment.first_ingress_sequence == 0U ||
        !initialized_snapshot.initialized ||
        initialized_snapshot.sealed ||
        initialized_snapshot.closed ||
        initialized_snapshot.fatal ||
        initialized_snapshot.append !=
            initialized_snapshot.durable ||
        initialized_snapshot.durable.segment_offset !=
            kRawV1SegmentHeaderBytes ||
        initialized_snapshot.durable.global_wal_pos !=
            expected_global_wal_pos ||
        initialized_snapshot.durable.ingress_sequence !=
            segment.first_ingress_sequence - 1U ||
        !JournalSizePlausibleForInitializedSegment(
            segment.segment_sequence,
            initialized_snapshot.journal_logical_size)) {
        return RawManifestTransitionErrorV1::
            kInvalidInitializedSnapshot;
    }

    DurableMarkerV1 marker{};
    marker.source_stream_id = segment.source_stream_id;
    marker.segment_sequence = segment.segment_sequence;
    marker.durable_global_wal_pos =
        initialized_snapshot.durable.global_wal_pos;
    marker.durable_ingress_sequence =
        initialized_snapshot.durable.ingress_sequence;
    marker.durable_segment_offset =
        initialized_snapshot.durable.segment_offset;
    marker.marker_flags = 0U;

    try {
        RawManifestSegmentEntryV1 candidate{};
        candidate.state =
            RawManifestSegmentStateV1::kOpen;
        candidate.segment_logical_length =
            kRawV1SegmentHeaderBytes;
        candidate.segment_sha256 =
            l2flow::common::ComputeSha256(header_wire);
        candidate.record_count = 0U;
        if (IsZero(candidate.segment_sha256) ||
            EncodeDurableMarkerV1(
                marker,
                &candidate.accepted_marker_bytes) !=
                RawV1Error::kNone) {
            return RawManifestTransitionErrorV1::
                kInvalidInitializedSnapshot;
        }
        candidate.accepted_marker_sha256 =
            ComputeAcceptedMarkerSha256(
                candidate.accepted_marker_bytes);
        CopyNormalSegmentIdentity(segment, &candidate);
        candidate.reserve_state_uuid =
            segment.reserve_state_uuid;
        candidate.finalization_cycle_id =
            segment.finalization_cycle_id;
        candidate.immutable_grant_sha256 =
            segment.immutable_grant_sha256;
        std::string report_locator;
        std::string archive_locator;
        const RawManifestTransitionErrorV1 locator_result =
            BuildFinalizationContinuationLocators(
                segment,
                &report_locator,
                &archive_locator);
        if (locator_result !=
            RawManifestTransitionErrorV1::kNone) {
            return locator_result;
        }
        candidate.maintenance_report_locator =
            std::move(report_locator);
        candidate.archive_locator =
            std::move(archive_locator);
        CommitOutput(output, &candidate);
        return RawManifestTransitionErrorV1::kNone;
    } catch (...) {
        return RawManifestTransitionErrorV1::
            kAllocationFailure;
    }
}

RawManifestTransitionErrorV1
BuildClosedRawManifestEntryV1(
    const RawSealedSegmentMetadataV1& metadata,
    RawManifestSegmentEntryV1* output) noexcept {
    if (output == nullptr) {
        return RawManifestTransitionErrorV1::kNullOutput;
    }

    RawV1SegmentHeaderWire header_wire{};
    const RawManifestTransitionErrorV1 header_result =
        EncodeNormalSegmentHeader(
            metadata.segment, &header_wire);
    if (header_result !=
        RawManifestTransitionErrorV1::kNone) {
        return header_result;
    }

    DurableMarkerV1 decoded_marker{};
    std::uint64_t expected_global_wal_pos = 0U;
    if (metadata.logical_end_offset <
            kRawV1SegmentHeaderBytes ||
        IsZero(metadata.segment_sha256) ||
        IsZero(metadata.index_sha256) ||
        !CheckedAdd(
            metadata.segment.segment_base_wal_pos,
            metadata.logical_end_offset,
            &expected_global_wal_pos) ||
        DecodeDurableMarkerV1(
            metadata.accepted_sealed_marker_bytes,
            &decoded_marker) != RawV1Error::kNone ||
        !SameMarker(
            decoded_marker,
            metadata.accepted_sealed_marker) ||
        decoded_marker.marker_flags !=
            kRawV1SegmentSealed ||
        decoded_marker.source_stream_id !=
            metadata.segment.source_stream_id ||
        decoded_marker.segment_sequence !=
            metadata.segment.segment_sequence ||
        decoded_marker.durable_global_wal_pos !=
            expected_global_wal_pos ||
        decoded_marker.durable_segment_offset !=
            metadata.logical_end_offset) {
        return RawManifestTransitionErrorV1::
            kInvalidSealedMetadata;
    }

    if (metadata.record_count == 0U) {
        if (metadata.actual_first_ingress_sequence
                .has_value() ||
            metadata.actual_last_ingress_sequence
                .has_value() ||
            metadata.logical_end_offset !=
                kRawV1SegmentHeaderBytes ||
            metadata.segment_sha256 !=
                l2flow::common::ComputeSha256(
                    header_wire) ||
            decoded_marker.durable_ingress_sequence !=
                metadata.segment.first_ingress_sequence -
                    1U) {
            return RawManifestTransitionErrorV1::
                kInvalidSealedMetadata;
        }
    } else {
        if (!metadata.actual_first_ingress_sequence
                 .has_value() ||
            !metadata.actual_last_ingress_sequence
                 .has_value()) {
            return RawManifestTransitionErrorV1::
                kInvalidSealedMetadata;
        }
        const std::uint64_t first =
            *metadata.actual_first_ingress_sequence;
        const std::uint64_t last =
            *metadata.actual_last_ingress_sequence;
        if (metadata.logical_end_offset ==
                kRawV1SegmentHeaderBytes ||
            first !=
                metadata.segment.first_ingress_sequence ||
            last !=
                decoded_marker.durable_ingress_sequence ||
            last < first ||
            last ==
                std::numeric_limits<std::uint64_t>::max() ||
            last - first != metadata.record_count - 1U) {
            return RawManifestTransitionErrorV1::
                kInvalidSealedMetadata;
        }
    }

    RawManifestSegmentEntryV1 candidate{};
    candidate.state =
        RawManifestSegmentStateV1::kClosed;
    candidate.segment_logical_length =
        metadata.logical_end_offset;
    candidate.segment_sha256 =
        metadata.segment_sha256;
    candidate.record_count = metadata.record_count;
    candidate.actual_first_ingress_sequence =
        metadata.actual_first_ingress_sequence;
    candidate.actual_last_ingress_sequence =
        metadata.actual_last_ingress_sequence;
    candidate.accepted_marker_bytes =
        metadata.accepted_sealed_marker_bytes;
    candidate.accepted_marker_sha256 =
        ComputeAcceptedMarkerSha256(
            candidate.accepted_marker_bytes);
    CopyNormalSegmentIdentity(
        metadata.segment, &candidate);
    CommitOutput(output, &candidate);
    return RawManifestTransitionErrorV1::kNone;
}

RawManifestTransitionErrorV1
BuildClosedFinalizationContinuationRawManifestEntryV1(
    const RawSealedSegmentMetadataV1& metadata,
    RawManifestSegmentEntryV1* output) noexcept {
    if (output == nullptr) {
        return RawManifestTransitionErrorV1::kNullOutput;
    }

    RawV1SegmentHeaderWire header_wire{};
    const RawManifestTransitionErrorV1 header_result =
        EncodeFinalizationContinuationSegmentHeader(
            metadata.segment, &header_wire);
    if (header_result !=
        RawManifestTransitionErrorV1::kNone) {
        return header_result;
    }

    DurableMarkerV1 decoded_marker{};
    std::uint64_t expected_global_wal_pos = 0U;
    if (metadata.logical_end_offset <
            kRawV1SegmentHeaderBytes ||
        IsZero(metadata.segment_sha256) ||
        IsZero(metadata.index_sha256) ||
        !CheckedAdd(
            metadata.segment.segment_base_wal_pos,
            metadata.logical_end_offset,
            &expected_global_wal_pos) ||
        DecodeDurableMarkerV1(
            metadata.accepted_sealed_marker_bytes,
            &decoded_marker) != RawV1Error::kNone ||
        !SameMarker(
            decoded_marker,
            metadata.accepted_sealed_marker) ||
        decoded_marker.marker_flags !=
            kRawV1SegmentSealed ||
        decoded_marker.source_stream_id !=
            metadata.segment.source_stream_id ||
        decoded_marker.segment_sequence !=
            metadata.segment.segment_sequence ||
        decoded_marker.durable_global_wal_pos !=
            expected_global_wal_pos ||
        decoded_marker.durable_segment_offset !=
            metadata.logical_end_offset) {
        return RawManifestTransitionErrorV1::
            kInvalidSealedMetadata;
    }

    if (metadata.record_count == 0U) {
        if (metadata.actual_first_ingress_sequence
                .has_value() ||
            metadata.actual_last_ingress_sequence
                .has_value() ||
            metadata.logical_end_offset !=
                kRawV1SegmentHeaderBytes ||
            metadata.segment_sha256 !=
                l2flow::common::ComputeSha256(
                    header_wire) ||
            decoded_marker.durable_ingress_sequence !=
                metadata.segment.first_ingress_sequence -
                    1U) {
            return RawManifestTransitionErrorV1::
                kInvalidSealedMetadata;
        }
    } else {
        if (!metadata.actual_first_ingress_sequence
                 .has_value() ||
            !metadata.actual_last_ingress_sequence
                 .has_value()) {
            return RawManifestTransitionErrorV1::
                kInvalidSealedMetadata;
        }
        const std::uint64_t first =
            *metadata.actual_first_ingress_sequence;
        const std::uint64_t last =
            *metadata.actual_last_ingress_sequence;
        if (metadata.logical_end_offset ==
                kRawV1SegmentHeaderBytes ||
            first !=
                metadata.segment.first_ingress_sequence ||
            last !=
                decoded_marker.durable_ingress_sequence ||
            last < first ||
            last ==
                std::numeric_limits<std::uint64_t>::max() ||
            last - first != metadata.record_count - 1U) {
            return RawManifestTransitionErrorV1::
                kInvalidSealedMetadata;
        }
    }

    try {
        RawManifestSegmentEntryV1 candidate{};
        candidate.state =
            RawManifestSegmentStateV1::kClosed;
        candidate.segment_logical_length =
            metadata.logical_end_offset;
        candidate.segment_sha256 =
            metadata.segment_sha256;
        candidate.record_count = metadata.record_count;
        candidate.actual_first_ingress_sequence =
            metadata.actual_first_ingress_sequence;
        candidate.actual_last_ingress_sequence =
            metadata.actual_last_ingress_sequence;
        candidate.accepted_marker_bytes =
            metadata.accepted_sealed_marker_bytes;
        candidate.accepted_marker_sha256 =
            ComputeAcceptedMarkerSha256(
                candidate.accepted_marker_bytes);
        CopyNormalSegmentIdentity(
            metadata.segment, &candidate);
        candidate.reserve_state_uuid =
            metadata.segment.reserve_state_uuid;
        candidate.finalization_cycle_id =
            metadata.segment.finalization_cycle_id;
        candidate.immutable_grant_sha256 =
            metadata.segment.immutable_grant_sha256;
        std::string report_locator;
        std::string archive_locator;
        const RawManifestTransitionErrorV1 locator_result =
            BuildFinalizationContinuationLocators(
                metadata.segment,
                &report_locator,
                &archive_locator);
        if (locator_result !=
            RawManifestTransitionErrorV1::kNone) {
            return locator_result;
        }
        candidate.maintenance_report_locator =
            std::move(report_locator);
        candidate.archive_locator =
            std::move(archive_locator);
        CommitOutput(output, &candidate);
        return RawManifestTransitionErrorV1::kNone;
    } catch (...) {
        return RawManifestTransitionErrorV1::
            kAllocationFailure;
    }
}

RawManifestTransitionErrorV1
BuildFreshOpenRawManifestV1(
    const SegmentHeaderV1& segment,
    const RawWalWriterSnapshot& initialized_snapshot,
    RawManifestV1* output) noexcept {
    if (output == nullptr) {
        return RawManifestTransitionErrorV1::kNullOutput;
    }

    try {
        RawManifestSegmentEntryV1 open_entry{};
        RawManifestTransitionErrorV1 result =
            BuildOpenRawManifestEntryV1(
                segment,
                initialized_snapshot,
                &open_entry);
        if (result !=
            RawManifestTransitionErrorV1::kNone) {
            return result;
        }

        RawManifestV1 candidate{};
        candidate.manifest_generation =
            kRawManifestInitialGenerationV1;
        candidate.namespace_identity =
            open_entry.namespace_identity;
        candidate.open_entry = std::move(open_entry);
        candidate.closed_entry_count = 0U;
        result = SetClosedFrontier(&candidate);
        if (result !=
            RawManifestTransitionErrorV1::kNone) {
            return result;
        }
        result = ValidateTransition(candidate, nullptr);
        if (result !=
            RawManifestTransitionErrorV1::kNone) {
            return result;
        }
        CommitOutput(output, &candidate);
        return RawManifestTransitionErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return RawManifestTransitionErrorV1::
            kAllocationFailure;
    } catch (...) {
        return RawManifestTransitionErrorV1::
            kAllocationFailure;
    }
}

RawManifestTransitionErrorV1
TransitionOpenRawManifestToClosedV1(
    const RawManifestV1& previous,
    const RawSealedSegmentMetadataV1& metadata,
    RawManifestV1* output) noexcept {
    if (output == nullptr) {
        return RawManifestTransitionErrorV1::kNullOutput;
    }
    RawManifestTransitionErrorV1 result =
        ValidatePrevious(previous);
    if (result !=
        RawManifestTransitionErrorV1::kNone) {
        return result;
    }
    if (!previous.open_entry.has_value()) {
        return RawManifestTransitionErrorV1::
            kMissingOpenEntry;
    }
    if (previous.manifest_generation ==
        std::numeric_limits<std::uint64_t>::max()) {
        return RawManifestTransitionErrorV1::
            kGenerationOverflow;
    }
    if (previous.closed_entry_count ==
        std::numeric_limits<std::uint64_t>::max()) {
        return RawManifestTransitionErrorV1::
            kClosedEntryCountOverflow;
    }

    try {
        RawManifestSegmentEntryV1 closed_entry{};
        const bool continuation =
            (previous.open_entry->segment_flags &
             kRawV1FinalizationContinuation) != 0U;
        result = continuation
                     ? BuildClosedFinalizationContinuationRawManifestEntryV1(
                           metadata, &closed_entry)
                     : BuildClosedRawManifestEntryV1(
                           metadata, &closed_entry);
        if (result !=
            RawManifestTransitionErrorV1::kNone) {
            return result;
        }
        if (!SameImmutableSegmentIdentity(
                *previous.open_entry, closed_entry)) {
            return RawManifestTransitionErrorV1::
                kSegmentIdentityMismatch;
        }

        RawManifestV1 candidate = previous;
        candidate.manifest_generation =
            previous.manifest_generation + 1U;
        candidate.closed_entries.push_back(
            std::move(closed_entry));
        candidate.open_entry.reset();
        candidate.closed_entry_count =
            previous.closed_entry_count + 1U;
        if (candidate.closed_entries.size() >
                static_cast<std::size_t>(
                    std::numeric_limits<std::uint64_t>::
                        max()) ||
            candidate.closed_entry_count !=
                static_cast<std::uint64_t>(
                    candidate.closed_entries.size())) {
            return RawManifestTransitionErrorV1::
                kClosedEntryCountOverflow;
        }
        result = SetClosedFrontier(&candidate);
        if (result !=
            RawManifestTransitionErrorV1::kNone) {
            return result;
        }
        result = ValidateTransition(
            candidate, &previous);
        if (result !=
            RawManifestTransitionErrorV1::kNone) {
            return result;
        }
        CommitOutput(output, &candidate);
        return RawManifestTransitionErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return RawManifestTransitionErrorV1::
            kAllocationFailure;
    } catch (...) {
        return RawManifestTransitionErrorV1::
            kAllocationFailure;
    }
}

RawManifestTransitionErrorV1
TransitionClosedRawManifestToNextOpenV1(
    const RawManifestV1& previous,
    const SegmentHeaderV1& next_segment,
    const RawWalWriterSnapshot& initialized_snapshot,
    RawManifestV1* output) noexcept {
    if (output == nullptr) {
        return RawManifestTransitionErrorV1::kNullOutput;
    }
    RawManifestTransitionErrorV1 result =
        ValidatePrevious(previous);
    if (result !=
        RawManifestTransitionErrorV1::kNone) {
        return result;
    }
    if (previous.open_entry.has_value()) {
        return RawManifestTransitionErrorV1::
            kUnexpectedOpenEntry;
    }
    if (previous.manifest_generation ==
        std::numeric_limits<std::uint64_t>::max()) {
        return RawManifestTransitionErrorV1::
            kGenerationOverflow;
    }

    try {
        RawManifestSegmentEntryV1 open_entry{};
        result = BuildOpenRawManifestEntryV1(
            next_segment,
            initialized_snapshot,
            &open_entry);
        if (result !=
            RawManifestTransitionErrorV1::kNone) {
            return result;
        }

        RawManifestV1 candidate = previous;
        candidate.manifest_generation =
            previous.manifest_generation + 1U;
        candidate.open_entry = std::move(open_entry);
        result = SetClosedFrontier(&candidate);
        if (result !=
            RawManifestTransitionErrorV1::kNone) {
            return result;
        }
        if (candidate.closed_prefix_sha256 !=
            previous.closed_prefix_sha256) {
            return RawManifestTransitionErrorV1::
                kSegmentIdentityMismatch;
        }
        result = ValidateTransition(
            candidate, &previous);
        if (result !=
            RawManifestTransitionErrorV1::kNone) {
            return result;
        }
        CommitOutput(output, &candidate);
        return RawManifestTransitionErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return RawManifestTransitionErrorV1::
            kAllocationFailure;
    } catch (...) {
        return RawManifestTransitionErrorV1::
            kAllocationFailure;
    }
}

RawManifestTransitionErrorV1
TransitionClosedRawManifestToFinalizationContinuationOpenV1(
    const RawManifestV1& previous,
    const SegmentHeaderV1& continuation_segment,
    const RawWalWriterSnapshot& initialized_snapshot,
    RawManifestV1* output) noexcept {
    if (output == nullptr) {
        return RawManifestTransitionErrorV1::kNullOutput;
    }
    RawManifestTransitionErrorV1 result =
        ValidatePrevious(previous);
    if (result !=
        RawManifestTransitionErrorV1::kNone) {
        return result;
    }
    if (previous.open_entry.has_value()) {
        return RawManifestTransitionErrorV1::
            kUnexpectedOpenEntry;
    }
    if (previous.manifest_generation ==
        std::numeric_limits<std::uint64_t>::max()) {
        return RawManifestTransitionErrorV1::
            kGenerationOverflow;
    }

    try {
        RawManifestSegmentEntryV1 open_entry{};
        result =
            BuildOpenFinalizationContinuationRawManifestEntryV1(
                continuation_segment,
                initialized_snapshot,
                &open_entry);
        if (result !=
            RawManifestTransitionErrorV1::kNone) {
            return result;
        }

        RawManifestV1 candidate = previous;
        candidate.manifest_generation =
            previous.manifest_generation + 1U;
        candidate.open_entry = std::move(open_entry);
        result = SetClosedFrontier(&candidate);
        if (result !=
            RawManifestTransitionErrorV1::kNone) {
            return result;
        }
        if (candidate.closed_prefix_sha256 !=
            previous.closed_prefix_sha256) {
            return RawManifestTransitionErrorV1::
                kSegmentIdentityMismatch;
        }
        result = ValidateTransition(
            candidate, &previous);
        if (result !=
            RawManifestTransitionErrorV1::kNone) {
            return result;
        }
        CommitOutput(output, &candidate);
        return RawManifestTransitionErrorV1::kNone;
    } catch (...) {
        return RawManifestTransitionErrorV1::
            kAllocationFailure;
    }
}

RawManifestTransitionErrorV1
BuildRecoveredClosedRawManifestV1(
    std::span<const RawSealedSegmentMetadataV1>
        sealed_segments,
    const RawManifestV1* existing_manifest,
    RawManifestV1* output) noexcept {
    if (output == nullptr) {
        return RawManifestTransitionErrorV1::kNullOutput;
    }
    if (sealed_segments.empty() ||
        sealed_segments.size() >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint64_t>::max())) {
        return RawManifestTransitionErrorV1::
            kRecoveredMetadataOrder;
    }

    try {
        RawManifestV1 candidate{};
        candidate.closed_entries.reserve(
            sealed_segments.size());
        std::uint32_t previous_sequence = 0U;
        for (std::size_t metadata_index = 0U;
             metadata_index < sealed_segments.size();
             ++metadata_index) {
            const RawSealedSegmentMetadataV1& metadata =
                sealed_segments[metadata_index];
            if (metadata.segment.segment_sequence == 0U ||
                (previous_sequence == 0U &&
                 metadata.segment.segment_sequence != 1U) ||
                (previous_sequence != 0U &&
                 (previous_sequence ==
                      std::numeric_limits<
                          std::uint32_t>::max() ||
                  metadata.segment.segment_sequence !=
                      previous_sequence + 1U))) {
                return RawManifestTransitionErrorV1::
                    kRecoveredMetadataOrder;
            }
            RawManifestSegmentEntryV1 closed_entry{};
            const bool continuation =
                (metadata.segment.segment_flags &
                 kRawV1FinalizationContinuation) != 0U;
            const bool represented_continuation =
                continuation &&
                existing_manifest != nullptr &&
                ((metadata_index <
                      existing_manifest
                          ->closed_entries.size() &&
                  (existing_manifest
                       ->closed_entries[metadata_index]
                       .segment_flags &
                   kRawV1FinalizationContinuation) !=
                      0U) ||
                 (metadata_index ==
                      existing_manifest
                          ->closed_entries.size() &&
                  existing_manifest->open_entry
                      .has_value() &&
                  (existing_manifest->open_entry
                       ->segment_flags &
                   kRawV1FinalizationContinuation) !=
                      0U));
            const RawManifestTransitionErrorV1
                entry_result =
                    represented_continuation
                        ? BuildClosedFinalizationContinuationRawManifestEntryV1(
                              metadata,
                              &closed_entry)
                        : BuildClosedRawManifestEntryV1(
                              metadata,
                              &closed_entry);
            if (entry_result !=
                RawManifestTransitionErrorV1::kNone) {
                return entry_result;
            }
            if (candidate.closed_entries.empty()) {
                candidate.namespace_identity =
                    closed_entry.namespace_identity;
            }
            candidate.closed_entries.push_back(
                std::move(closed_entry));
            previous_sequence =
                metadata.segment.segment_sequence;
        }
        candidate.closed_entry_count =
            static_cast<std::uint64_t>(
                candidate.closed_entries.size());

        if (existing_manifest == nullptr) {
            candidate.manifest_generation =
                kRawManifestInitialGenerationV1;
            RawManifestTransitionErrorV1 result =
                SetClosedFrontier(&candidate);
            if (result !=
                RawManifestTransitionErrorV1::kNone) {
                return result;
            }
            result = ValidateTransition(
                candidate, nullptr);
            if (result !=
                RawManifestTransitionErrorV1::kNone) {
                return result;
            }
            CommitOutput(output, &candidate);
            return RawManifestTransitionErrorV1::kNone;
        }

        RawManifestTransitionErrorV1 result =
            ValidatePrevious(*existing_manifest);
        if (result !=
            RawManifestTransitionErrorV1::kNone) {
            return result;
        }
        const std::size_t existing_closed_count =
            existing_manifest->closed_entries.size();
        const std::size_t expected_recovered_count =
            existing_closed_count +
            (existing_manifest->open_entry.has_value()
                 ? 1U
                 : 0U);
        if (expected_recovered_count <
                existing_closed_count ||
            sealed_segments.size() !=
                expected_recovered_count) {
            return existing_manifest->open_entry.has_value()
                       ? RawManifestTransitionErrorV1::
                             kRecoveredOpenConflict
                       : RawManifestTransitionErrorV1::
                             kRecoveredClosedPrefixConflict;
        }
        for (std::size_t index = 0U;
             index < existing_closed_count;
             ++index) {
            if (candidate.closed_entries[index] !=
                existing_manifest
                    ->closed_entries[index]) {
                return RawManifestTransitionErrorV1::
                    kRecoveredClosedPrefixConflict;
            }
        }

        if (!existing_manifest->open_entry.has_value()) {
            // Idempotent recovery of an already closed manifest must not
            // manufacture a new generation.
            RawManifestV1 unchanged =
                *existing_manifest;
            CommitOutput(output, &unchanged);
            return RawManifestTransitionErrorV1::kNone;
        }
        if (existing_manifest->manifest_generation ==
            std::numeric_limits<std::uint64_t>::max()) {
            return RawManifestTransitionErrorV1::
                kGenerationOverflow;
        }
        if (!SameImmutableSegmentIdentity(
                *existing_manifest->open_entry,
                candidate.closed_entries[
                    existing_closed_count])) {
            return RawManifestTransitionErrorV1::
                kRecoveredOpenConflict;
        }

        candidate.manifest_generation =
            existing_manifest->manifest_generation + 1U;
        candidate.open_entry.reset();
        result = SetClosedFrontier(&candidate);
        if (result !=
            RawManifestTransitionErrorV1::kNone) {
            return result;
        }
        const RawManifestV1Error model_result =
            ValidateManifestModel(
                candidate, existing_manifest);
        if (model_result != RawManifestV1Error::kNone) {
            return model_result ==
                           RawManifestV1Error::
                               kAllocationFailure
                       ? RawManifestTransitionErrorV1::
                             kAllocationFailure
                       : RawManifestTransitionErrorV1::
                             kRecoveredOpenConflict;
        }
        CommitOutput(output, &candidate);
        return RawManifestTransitionErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return RawManifestTransitionErrorV1::
            kAllocationFailure;
    } catch (...) {
        return RawManifestTransitionErrorV1::
            kAllocationFailure;
    }
}

}  // namespace l2flow::ingress
