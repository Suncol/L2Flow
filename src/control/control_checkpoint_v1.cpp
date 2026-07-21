#include "l2flow/control/control_checkpoint_v1.h"

#include "l2flow/common/crc32c.h"
#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/control/quality_flags_v1.h"
#include "l2flow/ingress/raw_v1.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <span>
#include <tuple>

namespace l2flow::control {
namespace {

constexpr std::uint32_t kLoggedInFlag = phase3_schema_v1::
    control_checkpoint_flag_v1_value::kLoggedIn;
constexpr std::uint32_t kDisconnectedWindowFlag = phase3_schema_v1::
    control_checkpoint_flag_v1_value::kDisconnectedWindow;
constexpr std::uint32_t kConnectionSwitchedFlag = phase3_schema_v1::
    control_checkpoint_flag_v1_value::kConnectionSwitched;
constexpr std::uint32_t kPoisonedFlag = phase3_schema_v1::
    control_checkpoint_flag_v1_value::kPoisoned;
constexpr std::uint32_t kControlReadyFlag = phase3_schema_v1::
    control_checkpoint_flag_v1_value::kControlReady;
constexpr std::uint32_t kDecoderEvidenceReadyFlag = phase3_schema_v1::
    control_checkpoint_flag_v1_value::kDecoderEvidenceReady;
constexpr std::uint32_t kCheckpointFlagsMask =
    kLoggedInFlag | kDisconnectedWindowFlag |
    kConnectionSwitchedFlag | kPoisonedFlag |
    kControlReadyFlag | kDecoderEvidenceReadyFlag;
constexpr std::uint64_t kDecodeErrorQualityMask =
    QualityBit(QualityFlagV1::kDecodeTruncated) |
    QualityBit(QualityFlagV1::kDecodeOffsetInvalid) |
    QualityBit(QualityFlagV1::kSchemaUnknown);
constexpr std::uint64_t kPersistedQualityMask =
    QualityBit(QualityFlagV1::kSubscriptionChanged) |
    kDecodeErrorQualityMask |
    QualityBit(QualityFlagV1::kNoncanonicalEmptyOffset) |
    QualityBit(QualityFlagV1::kUnauthorized);
constexpr std::uint64_t kRawCursorAlignment =
    static_cast<std::uint64_t>(
        l2flow::ingress::kRawV1RecordAlignment);
constexpr std::uint64_t kMinimumRawRecordBytes =
    static_cast<std::uint64_t>(
        l2flow::ingress::kRawV1RecordHeaderBytes) +
    static_cast<std::uint64_t>(
        l2flow::ingress::kRawV1RecordTrailerBytes);

namespace offset = phase3_schema_v1::control_checkpoint_v1_offset;
namespace entry_offset =
    phase3_schema_v1::control_checkpoint_entry_v1_offset;

void StoreU16(
    std::uint16_t value,
    std::span<std::byte> output,
    std::size_t at) noexcept {
    output[at] = static_cast<std::byte>(value & 0xffU);
    output[at + 1U] =
        static_cast<std::byte>((value >> 8U) & 0xffU);
}

void StoreU32(
    std::uint32_t value,
    std::span<std::byte> output,
    std::size_t at) noexcept {
    for (std::size_t index = 0U; index < 4U; ++index) {
        output[at + index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
}

void StoreU64(
    std::uint64_t value,
    std::span<std::byte> output,
    std::size_t at) noexcept {
    for (std::size_t index = 0U; index < 8U; ++index) {
        output[at + index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
}

std::uint16_t LoadU16(
    std::span<const std::byte> input,
    std::size_t at) noexcept {
    return static_cast<std::uint16_t>(
        std::to_integer<std::uint16_t>(input[at]) |
        static_cast<std::uint16_t>(
            std::to_integer<std::uint16_t>(input[at + 1U])
            << 8U));
}

std::uint32_t LoadU32(
    std::span<const std::byte> input,
    std::size_t at) noexcept {
    std::uint32_t value = 0U;
    for (std::size_t index = 0U; index < 4U; ++index) {
        value |= std::to_integer<std::uint32_t>(input[at + index])
                 << (index * 8U);
    }
    return value;
}

std::uint64_t LoadU64(
    std::span<const std::byte> input,
    std::size_t at) noexcept {
    std::uint64_t value = 0U;
    for (std::size_t index = 0U; index < 8U; ++index) {
        value |= std::to_integer<std::uint64_t>(input[at + index])
                 << (index * 8U);
    }
    return value;
}

bool IsZero(
    std::span<const std::byte> input,
    std::size_t at,
    std::size_t size) noexcept {
    return std::all_of(
        input.begin() + at,
        input.begin() + at + size,
        [](std::byte value) { return value == std::byte{0}; });
}

bool DigestIsZero(
    const l2flow::common::Sha256Digest& digest) noexcept {
    return std::all_of(
        digest.begin(), digest.end(), [](std::byte value) {
            return value == std::byte{0};
        });
}

void StoreDigest(
    const l2flow::common::Sha256Digest& digest,
    std::span<std::byte> output,
    std::size_t at) noexcept {
    std::copy(digest.begin(), digest.end(), output.begin() + at);
}

l2flow::common::Sha256Digest LoadDigest(
    std::span<const std::byte> input,
    std::size_t at) noexcept {
    l2flow::common::Sha256Digest digest{};
    std::copy_n(input.begin() + at, digest.size(), digest.begin());
    return digest;
}

bool KeyLess(
    const l2flow::sdk::MessageKey& left,
    const l2flow::sdk::MessageKey& right) noexcept {
    return std::tie(
               left.service_id,
               left.service_version,
               left.message_id) <
           std::tie(
               right.service_id,
               right.service_version,
               right.message_id);
}

bool SessionPhaseValid(ControlSessionPhaseV1 phase) noexcept {
    switch (phase) {
        case ControlSessionPhaseV1::kInitial:
        case ControlSessionPhaseV1::kConnecting:
        case ControlSessionPhaseV1::kLoggedIn:
        case ControlSessionPhaseV1::kConnectError:
        case ControlSessionPhaseV1::kDisconnected:
        case ControlSessionPhaseV1::kLogonFailed:
        case ControlSessionPhaseV1::kPoisoned:
            return true;
    }
    return false;
}

std::uint32_t EncodeFlags(
    const ControlDecoderSnapshotV1& state) noexcept {
    return (state.logged_in ? kLoggedInFlag : 0U) |
           (state.disconnected_window
                ? kDisconnectedWindowFlag
                : 0U) |
           (state.connection_switched ? kConnectionSwitchedFlag : 0U) |
           (state.poisoned ? kPoisonedFlag : 0U) |
           (state.control_ready ? kControlReadyFlag : 0U) |
           (state.decoder_evidence_ready
                ? kDecoderEvidenceReadyFlag
                : 0U);
}

void DecodeFlags(
    std::uint32_t flags,
    ControlDecoderSnapshotV1* state) noexcept {
    state->logged_in = (flags & kLoggedInFlag) != 0U;
    state->disconnected_window =
        (flags & kDisconnectedWindowFlag) != 0U;
    state->connection_switched =
        (flags & kConnectionSwitchedFlag) != 0U;
    state->poisoned = (flags & kPoisonedFlag) != 0U;
    state->control_ready = (flags & kControlReadyFlag) != 0U;
    state->decoder_evidence_ready =
        (flags & kDecoderEvidenceReadyFlag) != 0U;
}

std::uint64_t ComputeRequiredMask(
    const ControlDecoderSnapshotV1& state) noexcept {
    std::uint64_t mask = 0U;
    for (std::size_t index = 0U;
         index < state.subscriptions.size();
         ++index) {
        if (state.subscriptions[index].policy ==
            SubscriptionPolicyV1::kRequired) {
            mask |= std::uint64_t{1U} << index;
        }
    }
    return mask;
}

std::uint64_t ComputeEffectiveMask(
    const ControlDecoderSnapshotV1& state) noexcept {
    std::uint64_t mask = 0U;
    for (std::size_t index = 0U;
         index < state.subscriptions.size();
         ++index) {
        const ControlSubscriptionStateV1& entry =
            state.subscriptions[index];
        if (entry.status_known && entry.status == 0U) {
            mask |= std::uint64_t{1U} << index;
        }
    }
    return mask;
}

bool CheckedCounterSum(
    std::span<const std::uint64_t> values,
    std::uint64_t* sum) noexcept {
    std::uint64_t result = 0U;
    for (const std::uint64_t value : values) {
        if (value >
            std::numeric_limits<std::uint64_t>::max() - result) {
            return false;
        }
        result += value;
    }
    *sum = result;
    return true;
}

}  // namespace

std::string_view ControlCheckpointV1ErrorName(
    ControlCheckpointV1Error error) noexcept {
    switch (error) {
        case ControlCheckpointV1Error::kNone:
            return "none";
        case ControlCheckpointV1Error::kNullOutput:
            return "null_output";
        case ControlCheckpointV1Error::kInvalidModel:
            return "invalid_model";
        case ControlCheckpointV1Error::kInvalidWireSize:
            return "invalid_wire_size";
        case ControlCheckpointV1Error::kInvalidMagic:
            return "invalid_magic";
        case ControlCheckpointV1Error::kUnsupportedVersion:
            return "unsupported_version";
        case ControlCheckpointV1Error::kInvalidHeaderSize:
            return "invalid_header_size";
        case ControlCheckpointV1Error::kInvalidEntrySize:
            return "invalid_entry_size";
        case ControlCheckpointV1Error::kInvalidTotalSize:
            return "invalid_total_size";
        case ControlCheckpointV1Error::kUnknownFlags:
            return "unknown_flags";
        case ControlCheckpointV1Error::kNonzeroReserved:
            return "nonzero_reserved";
        case ControlCheckpointV1Error::kHeaderCrcMismatch:
            return "header_crc_mismatch";
        case ControlCheckpointV1Error::kDigestMismatch:
            return "digest_mismatch";
        case ControlCheckpointV1Error::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

ControlCheckpointV1Error ValidateControlDecoderCheckpointV1(
    const ControlDecoderCheckpointV1& checkpoint) noexcept {
    const ControlDecoderSnapshotV1& state = checkpoint.state;
    if (checkpoint.schema_version !=
            kControlDecoderCheckpointVersionV1 ||
        state.source_stream_id == 0U || state.capture_date == 0U ||
        l2flow::common::IsZeroIdentity(state.stream_day_id) ||
        DigestIsZero(state.stable_config_sha256) ||
        DigestIsZero(state.requested_manifest_sha256) ||
        DigestIsZero(state.state_sha256) ||
        state.subscriptions.empty() ||
        state.subscriptions.size() >
            kControlCheckpointV1MaximumEntries ||
        !SessionPhaseValid(state.session_phase) ||
        state.counters.processed_records == 0U ||
        state.next_ingress_sequence == 0U ||
        state.processed_ingress_sequence ==
            std::numeric_limits<std::uint64_t>::max() ||
        state.next_ingress_sequence !=
            state.processed_ingress_sequence + 1U ||
        state.counters.processed_records !=
            state.processed_ingress_sequence ||
        state.processed_record_start_wal_pos <
            l2flow::ingress::kRawV1SegmentHeaderBytes ||
        state.processed_record_end_wal_pos <=
            state.processed_record_start_wal_pos ||
        state.processed_record_end_wal_pos -
                state.processed_record_start_wal_pos <
            kMinimumRawRecordBytes ||
        (state.processed_record_start_wal_pos %
         kRawCursorAlignment) != 0U ||
        (state.processed_record_end_wal_pos %
         kRawCursorAlignment) != 0U ||
        (state.poisoned !=
         (state.session_phase == ControlSessionPhaseV1::kPoisoned)) ||
        (state.logged_in !=
         (state.session_phase == ControlSessionPhaseV1::kLoggedIn)) ||
        (!state.poisoned &&
         state.connection_epoch != state.counters.logon_success) ||
        ((state.counters.control_decode_errors != 0U) !=
         state.poisoned) ||
        (state.logged_in && state.connection_epoch == 0U) ||
        (state.logged_in && state.disconnected_window) ||
        ((state.session_phase ==
              ControlSessionPhaseV1::kInitial) &&
         (state.connection_epoch != 0U ||
          state.disconnected_window)) ||
        ((state.session_phase ==
              ControlSessionPhaseV1::kConnectError ||
          state.session_phase ==
              ControlSessionPhaseV1::kDisconnected) &&
         !state.disconnected_window) ||
        (state.session_phase ==
             ControlSessionPhaseV1::kDisconnected &&
         state.counters.disconnect == 0U) ||
        (state.session_phase ==
             ControlSessionPhaseV1::kLogonFailed &&
         state.counters.logon_failure == 0U) ||
        (state.connection_switched && state.connection_epoch < 2U) ||
        state.connection_epoch > state.counters.logon_success ||
        (state.session_phase != ControlSessionPhaseV1::kInitial &&
         state.counters.emitted_control_records == 0U) ||
        state.counters.emitted_control_records >
            state.counters.processed_records ||
        state.counters.logon_success >
            state.counters.emitted_control_records ||
        state.counters.logon_failure >
            state.counters.emitted_control_records ||
        state.counters.disconnect >
            state.counters.emitted_control_records ||
        state.counters.subscription_responses >
            state.counters.emitted_control_records ||
        state.counters.control_decode_errors >
            state.counters.emitted_control_records ||
        (state.quality_flags & ~kPersistedQualityMask) != 0U) {
        return ControlCheckpointV1Error::kInvalidModel;
    }

    const std::array<std::uint64_t, 5U> typed_events{
        state.counters.logon_success,
        state.counters.logon_failure,
        state.counters.disconnect,
        state.counters.subscription_responses,
        state.counters.control_decode_errors};
    std::uint64_t typed_event_count = 0U;
    if (!CheckedCounterSum(typed_events, &typed_event_count) ||
        typed_event_count > state.counters.emitted_control_records) {
        return ControlCheckpointV1Error::kInvalidModel;
    }
    const std::uint64_t untyped_event_count =
        state.counters.emitted_control_records - typed_event_count;
    const std::array<std::uint64_t, 2U> address_event_evidence{
        state.counters.disconnect, untyped_event_count};
    std::uint64_t address_event_count = 0U;
    if (!CheckedCounterSum(
            address_event_evidence, &address_event_count)) {
        return ControlCheckpointV1Error::kInvalidModel;
    }

    const bool current_address_present =
        !DigestIsZero(state.current_address_sha256);
    const bool last_success_address_present =
        !DigestIsZero(state.last_success_address_sha256);
    const bool api_address_phase =
        state.session_phase == ControlSessionPhaseV1::kConnecting ||
        state.session_phase == ControlSessionPhaseV1::kConnectError ||
        state.session_phase == ControlSessionPhaseV1::kDisconnected;
    const bool untyped_api_phase =
        state.session_phase == ControlSessionPhaseV1::kConnecting ||
        state.session_phase == ControlSessionPhaseV1::kConnectError;
    if ((state.session_phase == ControlSessionPhaseV1::kInitial &&
         (current_address_present ||
          last_success_address_present)) ||
        (api_address_phase && !current_address_present) ||
        (untyped_api_phase && untyped_event_count == 0U) ||
        (current_address_present && address_event_count == 0U) ||
        (state.disconnected_window &&
         (!current_address_present || address_event_count == 0U)) ||
        (state.connection_epoch == 0U &&
         last_success_address_present) ||
        (last_success_address_present &&
         !current_address_present) ||
        (state.logged_in &&
         state.current_address_sha256 !=
             state.last_success_address_sha256) ||
        (state.connection_switched &&
         (!current_address_present ||
          !last_success_address_present ||
          address_event_count < 2U))) {
        return ControlCheckpointV1Error::kInvalidModel;
    }

    const std::uint64_t decode_error_quality =
        state.quality_flags & kDecodeErrorQualityMask;
    const bool subscription_changed_quality =
        (state.quality_flags &
         QualityBit(QualityFlagV1::kSubscriptionChanged)) != 0U;
    const bool unauthorized_quality =
        (state.quality_flags &
         QualityBit(QualityFlagV1::kUnauthorized)) != 0U;
    const bool noncanonical_empty_offset_quality =
        (state.quality_flags &
         QualityBit(QualityFlagV1::kNoncanonicalEmptyOffset)) != 0U;
    const bool subscription_change_event_possible =
        state.counters.logon_success >= 2U ||
        (state.counters.logon_success != 0U &&
         state.counters.subscription_responses != 0U);
    if ((state.quality_flags != 0U &&
         state.counters.emitted_control_records == 0U) ||
        ((decode_error_quality != 0U) !=
         (state.counters.control_decode_errors != 0U)) ||
        (noncanonical_empty_offset_quality &&
         state.counters.emitted_control_records <=
             state.counters.control_decode_errors) ||
        (subscription_changed_quality &&
         (state.connection_epoch == 0U ||
          state.subscription_epoch == 0U ||
          !subscription_change_event_possible)) ||
        (unauthorized_quality &&
         (state.counters.logon_failure == 0U ||
          state.logged_in))) {
        return ControlCheckpointV1Error::kInvalidModel;
    }

    const bool response_cursor_present =
        state.latest_response_ingress_sequence != 0U ||
        state.latest_response_record_end_wal_pos != 0U ||
        !DigestIsZero(state.response_manifest_sha256);
    if (response_cursor_present &&
        (state.latest_response_ingress_sequence == 0U ||
         state.latest_response_record_end_wal_pos == 0U ||
         DigestIsZero(state.response_manifest_sha256) ||
         (state.latest_response_record_end_wal_pos %
          kRawCursorAlignment) != 0U ||
         !((state.latest_response_ingress_sequence ==
                state.processed_ingress_sequence &&
            state.latest_response_record_end_wal_pos ==
                state.processed_record_end_wal_pos) ||
           (state.latest_response_ingress_sequence <
                state.processed_ingress_sequence &&
            state.latest_response_record_end_wal_pos <=
                state.processed_record_start_wal_pos)))) {
        return ControlCheckpointV1Error::kInvalidModel;
    }
    const bool response_event_present =
        state.counters.logon_success != 0U ||
        state.counters.logon_failure != 0U ||
        state.counters.subscription_responses != 0U;
    if (response_cursor_present != response_event_present) {
        return ControlCheckpointV1Error::kInvalidModel;
    }
    if (!response_cursor_present &&
        (!DigestIsZero(state.response_manifest_sha256) ||
         state.latest_response_ingress_sequence != 0U ||
         state.latest_response_record_end_wal_pos != 0U)) {
        return ControlCheckpointV1Error::kInvalidModel;
    }
    std::uint64_t maximum_subscription_changes = 0U;
    const std::array<std::uint64_t, 2U> response_events{
        state.counters.logon_success,
        state.counters.subscription_responses};
    if (!CheckedCounterSum(
            response_events, &maximum_subscription_changes) ||
        static_cast<std::uint64_t>(state.subscription_epoch) >
            maximum_subscription_changes) {
        return ControlCheckpointV1Error::kInvalidModel;
    }
    for (std::size_t index = 0U;
         index < state.subscriptions.size();
         ++index) {
        const ControlSubscriptionStateV1& entry =
            state.subscriptions[index];
        if (entry.key.service_id <= 2U ||
            entry.key.service_version == 0U ||
            entry.key.message_id == 0U ||
            (entry.policy != SubscriptionPolicyV1::kRequired &&
             entry.policy != SubscriptionPolicyV1::kOptional) ||
            (!entry.status_known && entry.status != 0U) ||
            (state.connection_epoch == 0U && entry.status_known) ||
            (index != 0U &&
             !KeyLess(
                 state.subscriptions[index - 1U].key,
                 entry.key))) {
            return ControlCheckpointV1Error::kInvalidModel;
        }
    }
    const l2flow::common::Sha256Digest requested_manifest =
        ComputeRequestedSubscriptionManifestSha256V1(
            state.subscriptions);
    l2flow::common::Sha256Digest recomputed_state{};
    if (DigestIsZero(requested_manifest) ||
        requested_manifest != state.requested_manifest_sha256 ||
        !ComputeControlDecoderStateSha256V1(
            state, &recomputed_state) ||
        recomputed_state != state.state_sha256) {
        return ControlCheckpointV1Error::kInvalidModel;
    }
    const std::uint64_t required = ComputeRequiredMask(state);
    const std::uint64_t effective = ComputeEffectiveMask(state);
    const bool control_ready =
        !state.poisoned && state.logged_in &&
        state.session_phase == ControlSessionPhaseV1::kLoggedIn &&
        (effective & required) == required;
    const bool decoder_evidence_ready =
        control_ready &&
        (state.required_first_seen_mask & required) == required;
    if (state.effective_success_mask != effective ||
        (state.required_first_seen_mask & ~required) != 0U ||
        (state.required_first_seen_mask & ~effective) != 0U ||
        (state.connection_epoch == 0U &&
         (effective != 0U || state.subscription_epoch != 0U)) ||
        (effective != 0U && state.subscription_epoch == 0U) ||
        state.control_ready != control_ready ||
        state.decoder_evidence_ready != decoder_evidence_ready) {
        return ControlCheckpointV1Error::kInvalidModel;
    }
    return ControlCheckpointV1Error::kNone;
}

ControlCheckpointV1Error EncodeControlDecoderCheckpointV1(
    const ControlDecoderCheckpointV1& checkpoint,
    std::vector<std::byte>* wire) noexcept {
    if (wire == nullptr) {
        return ControlCheckpointV1Error::kNullOutput;
    }
    const ControlCheckpointV1Error validation =
        ValidateControlDecoderCheckpointV1(checkpoint);
    if (validation != ControlCheckpointV1Error::kNone) {
        return validation;
    }
    const std::size_t entries_bytes =
        checkpoint.state.subscriptions.size() *
        kControlCheckpointV1EntryBytes;
    const std::size_t total_size =
        kControlCheckpointV1HeaderBytes + entries_bytes +
        kControlCheckpointV1TrailerBytes;
    if (total_size > std::numeric_limits<std::uint32_t>::max()) {
        return ControlCheckpointV1Error::kInvalidTotalSize;
    }

    try {
        std::vector<std::byte> encoded(total_size, std::byte{0});
        std::span<std::byte> bytes(encoded);
        const ControlDecoderSnapshotV1& state = checkpoint.state;
        StoreU32(kControlCheckpointV1Magic, bytes, offset::kMagic);
        StoreU16(
            kControlCheckpointV1Version, bytes, offset::kVersion);
        StoreU16(
            static_cast<std::uint16_t>(
                kControlCheckpointV1HeaderBytes),
            bytes,
            offset::kHeaderSize);
        StoreU32(
            static_cast<std::uint32_t>(total_size),
            bytes,
            offset::kTotalSize);
        StoreU16(
            static_cast<std::uint16_t>(
                kControlCheckpointV1EntryBytes),
            bytes,
            offset::kEntrySize);
        StoreU32(
            static_cast<std::uint32_t>(state.subscriptions.size()),
            bytes,
            offset::kEntryCount);
        StoreU32(EncodeFlags(state), bytes, offset::kFlags);
        StoreU32(
            state.source_stream_id, bytes, offset::kSourceStreamId);
        StoreU32(state.capture_date, bytes, offset::kCaptureDate);
        std::copy(
            state.stream_day_id.begin(),
            state.stream_day_id.end(),
            bytes.begin() + offset::kStreamDayId);
        StoreDigest(
            state.stable_config_sha256,
            bytes,
            offset::kStableConfigSha256);
        StoreDigest(
            state.requested_manifest_sha256,
            bytes,
            offset::kRequestedManifestSha256);
        StoreDigest(
            state.response_manifest_sha256,
            bytes,
            offset::kResponseManifestSha256);
        StoreDigest(
            state.current_address_sha256,
            bytes,
            offset::kCurrentAddressSha256);
        StoreDigest(
            state.last_success_address_sha256,
            bytes,
            offset::kLastSuccessAddressSha256);
        StoreDigest(state.state_sha256, bytes, offset::kStateSha256);
        StoreU32(
            state.connection_epoch, bytes, offset::kConnectionEpoch);
        StoreU32(
            state.subscription_epoch,
            bytes,
            offset::kSubscriptionEpoch);
        bytes[offset::kSessionPhase] = static_cast<std::byte>(
            static_cast<std::uint8_t>(state.session_phase));
        StoreU64(
            state.next_ingress_sequence,
            bytes,
            offset::kNextIngressSequence);
        StoreU64(
            state.processed_ingress_sequence,
            bytes,
            offset::kProcessedIngressSequence);
        StoreU64(
            state.processed_record_start_wal_pos,
            bytes,
            offset::kProcessedRecordStartWalPos);
        StoreU64(
            state.processed_record_end_wal_pos,
            bytes,
            offset::kProcessedRecordEndWalPos);
        StoreU64(
            state.latest_response_ingress_sequence,
            bytes,
            offset::kLatestResponseIngressSequence);
        StoreU64(
            state.latest_response_record_end_wal_pos,
            bytes,
            offset::kLatestResponseRecordEndWalPos);
        StoreU64(state.quality_flags, bytes, offset::kQualityFlags);
        StoreU64(
            state.required_first_seen_mask,
            bytes,
            offset::kRequiredFirstSeenMask);
        StoreU64(
            state.effective_success_mask,
            bytes,
            offset::kEffectiveSuccessMask);
        StoreU64(
            state.counters.processed_records,
            bytes,
            offset::kProcessedRecords);
        StoreU64(
            state.counters.emitted_control_records,
            bytes,
            offset::kEmittedControlRecords);
        StoreU64(
            state.counters.logon_success,
            bytes,
            offset::kLogonSuccess);
        StoreU64(
            state.counters.logon_failure,
            bytes,
            offset::kLogonFailure);
        StoreU64(
            state.counters.disconnect, bytes, offset::kDisconnect);
        StoreU64(
            state.counters.subscription_responses,
            bytes,
            offset::kSubscriptionResponses);
        StoreU64(
            state.counters.control_decode_errors,
            bytes,
            offset::kControlDecodeErrors);

        for (std::size_t index = 0U;
             index < state.subscriptions.size();
             ++index) {
            const std::size_t entry =
                kControlCheckpointV1HeaderBytes +
                index * kControlCheckpointV1EntryBytes;
            const ControlSubscriptionStateV1& value =
                state.subscriptions[index];
            bytes[entry + entry_offset::kServiceId] =
                static_cast<std::byte>(value.key.service_id);
            bytes[entry + entry_offset::kPolicy] =
                static_cast<std::byte>(
                    static_cast<std::uint8_t>(value.policy));
            bytes[entry + entry_offset::kStatusKnown] =
                static_cast<std::byte>(
                    value.status_known ? 1U : 0U);
            StoreU16(
                value.key.service_version,
                bytes,
                entry + entry_offset::kServiceVersion);
            StoreU16(
                value.key.message_id,
                bytes,
                entry + entry_offset::kMessageId);
            StoreU32(
                value.status,
                bytes,
                entry + entry_offset::kStatus);
        }

        std::array<std::byte, kControlCheckpointV1HeaderBytes>
            header_copy{};
        std::copy_n(
            bytes.begin(), header_copy.size(), header_copy.begin());
        StoreU32(0U, header_copy, offset::kHeaderCrc32c);
        StoreU32(
            l2flow::common::ComputeCrc32c(header_copy),
            bytes,
            offset::kHeaderCrc32c);
        const l2flow::common::Sha256Digest digest =
            l2flow::common::ComputeSha256(
                std::span<const std::byte>(bytes).first(
                    total_size - kControlCheckpointV1TrailerBytes));
        StoreDigest(
            digest,
            bytes,
            total_size - kControlCheckpointV1TrailerBytes);
        *wire = std::move(encoded);
        return ControlCheckpointV1Error::kNone;
    } catch (const std::bad_alloc&) {
        return ControlCheckpointV1Error::kResourceExhausted;
    } catch (...) {
        return ControlCheckpointV1Error::kResourceExhausted;
    }
}

ControlCheckpointV1Error DecodeControlDecoderCheckpointV1(
    std::span<const std::byte> wire,
    ControlDecoderCheckpointV1* checkpoint) noexcept {
    if (checkpoint == nullptr) {
        return ControlCheckpointV1Error::kNullOutput;
    }
    if (wire.size() <
        kControlCheckpointV1HeaderBytes +
            kControlCheckpointV1TrailerBytes) {
        return ControlCheckpointV1Error::kInvalidWireSize;
    }
    if (LoadU32(wire, offset::kMagic) != kControlCheckpointV1Magic) {
        return ControlCheckpointV1Error::kInvalidMagic;
    }
    if (LoadU16(wire, offset::kVersion) !=
        kControlCheckpointV1Version) {
        return ControlCheckpointV1Error::kUnsupportedVersion;
    }
    if (LoadU16(wire, offset::kHeaderSize) !=
        kControlCheckpointV1HeaderBytes) {
        return ControlCheckpointV1Error::kInvalidHeaderSize;
    }
    if (LoadU16(wire, offset::kEntrySize) !=
        kControlCheckpointV1EntryBytes) {
        return ControlCheckpointV1Error::kInvalidEntrySize;
    }
    const std::uint32_t entry_count =
        LoadU32(wire, offset::kEntryCount);
    if (entry_count == 0U ||
        entry_count > kControlCheckpointV1MaximumEntries) {
        return ControlCheckpointV1Error::kInvalidTotalSize;
    }
    const std::size_t expected_size =
        kControlCheckpointV1HeaderBytes +
        static_cast<std::size_t>(entry_count) *
            kControlCheckpointV1EntryBytes +
        kControlCheckpointV1TrailerBytes;
    if (wire.size() != expected_size ||
        LoadU32(wire, offset::kTotalSize) != expected_size) {
        return ControlCheckpointV1Error::kInvalidTotalSize;
    }
    const std::uint32_t flags = LoadU32(wire, offset::kFlags);
    if ((flags & ~kCheckpointFlagsMask) != 0U) {
        return ControlCheckpointV1Error::kUnknownFlags;
    }
    if (!IsZero(wire, offset::kReserved0, 2U) ||
        !IsZero(wire, offset::kReserved1, 7U) ||
        !IsZero(wire, offset::kReservedTail, 124U)) {
        return ControlCheckpointV1Error::kNonzeroReserved;
    }
    for (std::size_t index = 0U; index < entry_count; ++index) {
        const std::size_t entry =
            kControlCheckpointV1HeaderBytes +
            index * kControlCheckpointV1EntryBytes;
        if (!IsZero(wire, entry + entry_offset::kReserved0, 1U) ||
            !IsZero(
                wire, entry + entry_offset::kReservedTail, 4U)) {
            return ControlCheckpointV1Error::kNonzeroReserved;
        }
    }

    std::array<std::byte, kControlCheckpointV1HeaderBytes>
        header_copy{};
    std::copy_n(wire.begin(), header_copy.size(), header_copy.begin());
    const std::uint32_t stored_crc =
        LoadU32(wire, offset::kHeaderCrc32c);
    StoreU32(0U, header_copy, offset::kHeaderCrc32c);
    if (l2flow::common::ComputeCrc32c(header_copy) != stored_crc) {
        return ControlCheckpointV1Error::kHeaderCrcMismatch;
    }
    const l2flow::common::Sha256Digest expected_digest =
        l2flow::common::ComputeSha256(
            wire.first(
                wire.size() - kControlCheckpointV1TrailerBytes));
    if (!std::equal(
            expected_digest.begin(),
            expected_digest.end(),
            wire.end() -
                static_cast<std::ptrdiff_t>(
                    kControlCheckpointV1TrailerBytes))) {
        return ControlCheckpointV1Error::kDigestMismatch;
    }

    try {
        ControlDecoderCheckpointV1 decoded;
        decoded.schema_version = kControlDecoderCheckpointVersionV1;
        ControlDecoderSnapshotV1& state = decoded.state;
        state.source_stream_id =
            LoadU32(wire, offset::kSourceStreamId);
        state.capture_date = LoadU32(wire, offset::kCaptureDate);
        std::copy_n(
            wire.begin() + offset::kStreamDayId,
            state.stream_day_id.size(),
            state.stream_day_id.begin());
        state.stable_config_sha256 =
            LoadDigest(wire, offset::kStableConfigSha256);
        state.requested_manifest_sha256 =
            LoadDigest(wire, offset::kRequestedManifestSha256);
        state.response_manifest_sha256 =
            LoadDigest(wire, offset::kResponseManifestSha256);
        state.current_address_sha256 =
            LoadDigest(wire, offset::kCurrentAddressSha256);
        state.last_success_address_sha256 =
            LoadDigest(wire, offset::kLastSuccessAddressSha256);
        state.state_sha256 = LoadDigest(wire, offset::kStateSha256);
        state.connection_epoch =
            LoadU32(wire, offset::kConnectionEpoch);
        state.subscription_epoch =
            LoadU32(wire, offset::kSubscriptionEpoch);
        state.session_phase = static_cast<ControlSessionPhaseV1>(
            std::to_integer<std::uint8_t>(
                wire[offset::kSessionPhase]));
        state.next_ingress_sequence =
            LoadU64(wire, offset::kNextIngressSequence);
        state.processed_ingress_sequence =
            LoadU64(wire, offset::kProcessedIngressSequence);
        state.processed_record_start_wal_pos =
            LoadU64(wire, offset::kProcessedRecordStartWalPos);
        state.processed_record_end_wal_pos =
            LoadU64(wire, offset::kProcessedRecordEndWalPos);
        state.latest_response_ingress_sequence =
            LoadU64(wire, offset::kLatestResponseIngressSequence);
        state.latest_response_record_end_wal_pos =
            LoadU64(wire, offset::kLatestResponseRecordEndWalPos);
        state.quality_flags = LoadU64(wire, offset::kQualityFlags);
        state.required_first_seen_mask =
            LoadU64(wire, offset::kRequiredFirstSeenMask);
        state.effective_success_mask =
            LoadU64(wire, offset::kEffectiveSuccessMask);
        state.counters.processed_records =
            LoadU64(wire, offset::kProcessedRecords);
        state.counters.emitted_control_records =
            LoadU64(wire, offset::kEmittedControlRecords);
        state.counters.logon_success =
            LoadU64(wire, offset::kLogonSuccess);
        state.counters.logon_failure =
            LoadU64(wire, offset::kLogonFailure);
        state.counters.disconnect =
            LoadU64(wire, offset::kDisconnect);
        state.counters.subscription_responses =
            LoadU64(wire, offset::kSubscriptionResponses);
        state.counters.control_decode_errors =
            LoadU64(wire, offset::kControlDecodeErrors);
        DecodeFlags(flags, &state);

        state.subscriptions.reserve(entry_count);
        for (std::size_t index = 0U; index < entry_count; ++index) {
            const std::size_t entry =
                kControlCheckpointV1HeaderBytes +
                index * kControlCheckpointV1EntryBytes;
            ControlSubscriptionStateV1 value;
            value.key.service_id = std::to_integer<std::uint8_t>(
                wire[entry + entry_offset::kServiceId]);
            value.policy = static_cast<SubscriptionPolicyV1>(
                std::to_integer<std::uint8_t>(
                    wire[entry + entry_offset::kPolicy]));
            const std::uint8_t known =
                std::to_integer<std::uint8_t>(
                    wire[entry + entry_offset::kStatusKnown]);
            if (known > 1U) {
                return ControlCheckpointV1Error::kInvalidModel;
            }
            value.status_known = known != 0U;
            value.key.service_version = LoadU16(
                wire, entry + entry_offset::kServiceVersion);
            value.key.message_id =
                LoadU16(wire, entry + entry_offset::kMessageId);
            value.status =
                LoadU32(wire, entry + entry_offset::kStatus);
            state.subscriptions.push_back(value);
        }
        const ControlCheckpointV1Error validation =
            ValidateControlDecoderCheckpointV1(decoded);
        if (validation != ControlCheckpointV1Error::kNone) {
            return validation;
        }
        *checkpoint = std::move(decoded);
        return ControlCheckpointV1Error::kNone;
    } catch (const std::bad_alloc&) {
        return ControlCheckpointV1Error::kResourceExhausted;
    } catch (...) {
        return ControlCheckpointV1Error::kResourceExhausted;
    }
}

}  // namespace l2flow::control
