#include "l2flow/control/control_decoder.h"

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/control/api_decoder.h"
#include "l2flow/control/control_checkpoint_v1.h"
#include "l2flow/control/quality_flags_v1.h"
#include "l2flow/control/raw_frontier_v1.h"
#include "l2flow/control/sys_decoder.h"
#include "l2flow/ingress/required_market_validator.h"
#include "l2flow/ingress/raw_schema.h"

#include "mdl_api_types.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <span>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

namespace l2flow::control {
namespace {

constexpr std::uint8_t kApiServiceId = 1U;
constexpr std::uint8_t kSysServiceId = 2U;
constexpr std::uint32_t kMdlOk = 0U;
constexpr std::uint32_t kMdlUnauthorized = 5U;
constexpr std::uint16_t kApiDecodeErrorDomain = 0x0100U;
constexpr std::uint16_t kSysDecodeErrorDomain = 0x0200U;

bool DigestIsZero(
    const l2flow::common::Sha256Digest& digest) noexcept {
    return std::all_of(
        digest.begin(), digest.end(), [](std::byte value) {
            return value == std::byte{0};
        });
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

bool SubscriptionLess(
    const ControlSubscriptionStateV1& left,
    const ControlSubscriptionStateV1& right) noexcept {
    return KeyLess(left.key, right.key);
}

bool UpdateBytes(
    l2flow::common::Sha256Hasher* hasher,
    std::span<const std::byte> bytes) noexcept {
    return hasher->Update(bytes);
}

bool UpdateU8(
    l2flow::common::Sha256Hasher* hasher,
    std::uint8_t value) noexcept {
    const std::array<std::byte, 1U> bytes{
        static_cast<std::byte>(value)};
    return UpdateBytes(hasher, bytes);
}

bool UpdateBool(
    l2flow::common::Sha256Hasher* hasher,
    bool value) noexcept {
    return UpdateU8(hasher, value ? 1U : 0U);
}

bool UpdateU16(
    l2flow::common::Sha256Hasher* hasher,
    std::uint16_t value) noexcept {
    std::array<std::byte, 2U> bytes{};
    for (std::size_t index = 0U; index < bytes.size(); ++index) {
        bytes[index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
    return UpdateBytes(hasher, bytes);
}

bool UpdateU32(
    l2flow::common::Sha256Hasher* hasher,
    std::uint32_t value) noexcept {
    std::array<std::byte, 4U> bytes{};
    for (std::size_t index = 0U; index < bytes.size(); ++index) {
        bytes[index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
    return UpdateBytes(hasher, bytes);
}

bool UpdateU64(
    l2flow::common::Sha256Hasher* hasher,
    std::uint64_t value) noexcept {
    std::array<std::byte, 8U> bytes{};
    for (std::size_t index = 0U; index < bytes.size(); ++index) {
        bytes[index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
    return UpdateBytes(hasher, bytes);
}

bool UpdateDigest(
    l2flow::common::Sha256Hasher* hasher,
    const l2flow::common::Sha256Digest& digest) noexcept {
    return UpdateBytes(hasher, digest);
}

std::uint64_t SubscriptionMask(
    const std::vector<ControlSubscriptionStateV1>& subscriptions,
    bool required_only,
    bool known_only,
    bool successful_only,
    bool failed_only) noexcept {
    std::uint64_t mask = 0U;
    for (std::size_t index = 0U; index < subscriptions.size(); ++index) {
        const ControlSubscriptionStateV1& entry = subscriptions[index];
        if (required_only &&
            entry.policy != SubscriptionPolicyV1::kRequired) {
            continue;
        }
        if (known_only && !entry.status_known) {
            continue;
        }
        if (successful_only &&
            (!entry.status_known || entry.status != kMdlOk)) {
            continue;
        }
        if (failed_only &&
            (!entry.status_known || entry.status == kMdlOk)) {
            continue;
        }
        mask |= std::uint64_t{1U} << index;
    }
    return mask;
}

std::uint64_t RequiredMask(
    const std::vector<ControlSubscriptionStateV1>& subscriptions) noexcept {
    return SubscriptionMask(
        subscriptions, true, false, false, false);
}

std::uint64_t EffectiveSuccessMask(
    const std::vector<ControlSubscriptionStateV1>& subscriptions) noexcept {
    return SubscriptionMask(
        subscriptions, false, true, true, false);
}

std::optional<std::size_t> FindSubscription(
    const std::vector<ControlSubscriptionStateV1>& subscriptions,
    const SubscriptionStatusV1& status) noexcept {
    if (status.service_id >
            std::numeric_limits<std::uint8_t>::max() ||
        status.service_version >
            std::numeric_limits<std::uint16_t>::max() ||
        status.message_id >
            std::numeric_limits<std::uint16_t>::max()) {
        return std::nullopt;
    }
    const l2flow::sdk::MessageKey key{
        static_cast<std::uint8_t>(status.service_id),
        static_cast<std::uint16_t>(status.service_version),
        static_cast<std::uint16_t>(status.message_id)};
    const auto found = std::lower_bound(
        subscriptions.begin(),
        subscriptions.end(),
        key,
        [](const ControlSubscriptionStateV1& entry,
           const l2flow::sdk::MessageKey& wanted) {
            return KeyLess(entry.key, wanted);
        });
    if (found == subscriptions.end() || found->key != key) {
        return std::nullopt;
    }
    return static_cast<std::size_t>(
        std::distance(subscriptions.begin(), found));
}

bool ComputeStateHashImpl(
    const ControlDecoderSnapshotV1& state,
    l2flow::common::Sha256Digest* digest) noexcept {
    constexpr std::string_view kDomain =
        "L2FLOW_PHASE3_CONTROL_STATE_V1";
    const std::span<const char> domain_chars(
        kDomain.data(), kDomain.size());
    static constexpr std::array<std::byte, 1U> separator{
        std::byte{0}};
    if (digest == nullptr ||
        state.subscriptions.size() >
            std::numeric_limits<std::uint32_t>::max()) {
        return false;
    }
    l2flow::common::Sha256Hasher hasher;
    if (!hasher.Update(std::as_bytes(domain_chars)) ||
        !hasher.Update(separator) ||
        !UpdateU32(&hasher, state.source_stream_id) ||
        !UpdateU32(&hasher, state.capture_date) ||
        !UpdateBytes(&hasher, state.stream_day_id) ||
        !UpdateDigest(&hasher, state.stable_config_sha256) ||
        !UpdateDigest(&hasher, state.requested_manifest_sha256) ||
        !UpdateDigest(&hasher, state.response_manifest_sha256) ||
        !UpdateDigest(&hasher, state.current_address_sha256) ||
        !UpdateDigest(&hasher, state.last_success_address_sha256) ||
        !UpdateU8(
            &hasher,
            static_cast<std::uint8_t>(state.session_phase)) ||
        !UpdateU32(&hasher, state.connection_epoch) ||
        !UpdateU32(&hasher, state.subscription_epoch) ||
        !UpdateU64(&hasher, state.next_ingress_sequence) ||
        !UpdateU64(&hasher, state.processed_ingress_sequence) ||
        !UpdateU64(
            &hasher, state.processed_record_start_wal_pos) ||
        !UpdateU64(&hasher, state.processed_record_end_wal_pos) ||
        !UpdateU64(
            &hasher, state.latest_response_ingress_sequence) ||
        !UpdateU64(
            &hasher, state.latest_response_record_end_wal_pos) ||
        !UpdateU64(&hasher, state.quality_flags) ||
        !UpdateU64(&hasher, state.required_first_seen_mask) ||
        !UpdateU64(&hasher, state.effective_success_mask) ||
        !UpdateBool(&hasher, state.logged_in) ||
        !UpdateBool(&hasher, state.disconnected_window) ||
        !UpdateBool(&hasher, state.connection_switched) ||
        !UpdateBool(&hasher, state.poisoned) ||
        !UpdateU64(&hasher, state.counters.processed_records) ||
        !UpdateU64(
            &hasher, state.counters.emitted_control_records) ||
        !UpdateU64(&hasher, state.counters.logon_success) ||
        !UpdateU64(&hasher, state.counters.logon_failure) ||
        !UpdateU64(&hasher, state.counters.disconnect) ||
        !UpdateU64(
            &hasher, state.counters.subscription_responses) ||
        !UpdateU64(
            &hasher, state.counters.control_decode_errors) ||
        !UpdateU32(
            &hasher,
            static_cast<std::uint32_t>(state.subscriptions.size()))) {
        return false;
    }
    for (const ControlSubscriptionStateV1& entry :
         state.subscriptions) {
        if (!UpdateU8(&hasher, entry.key.service_id) ||
            !UpdateU16(&hasher, entry.key.service_version) ||
            !UpdateU16(&hasher, entry.key.message_id) ||
            !UpdateU8(
                &hasher,
                static_cast<std::uint8_t>(entry.policy)) ||
            !UpdateBool(&hasher, entry.status_known) ||
            !UpdateU32(&hasher, entry.status)) {
            return false;
        }
    }
    return hasher.Finalize(digest);
}

void RecomputeReadiness(ControlDecoderSnapshotV1* state) noexcept {
    const std::uint64_t required = RequiredMask(state->subscriptions);
    const std::uint64_t required_ok = SubscriptionMask(
        state->subscriptions, true, true, true, false);
    state->effective_success_mask =
        EffectiveSuccessMask(state->subscriptions);
    state->control_ready =
        !state->poisoned && state->logged_in &&
        state->session_phase == ControlSessionPhaseV1::kLoggedIn &&
        required_ok == required;
    state->decoder_evidence_ready =
        state->control_ready &&
        (state->required_first_seen_mask & required) == required;
}

ControlRecordAttributionV1 Attribution(
    const ControlDecoderSnapshotV1& state) noexcept {
    ControlRecordAttributionV1 result;
    result.connection_epoch = state.connection_epoch;
    result.subscription_epoch = state.subscription_epoch;
    result.quality_flags = state.quality_flags;
    if (!state.logged_in) {
        result.quality_flags |=
            QualityBit(QualityFlagV1::kSessionUnknown);
    }
    if (state.disconnected_window) {
        result.quality_flags |=
            QualityBit(QualityFlagV1::kSessionUnknown) |
            QualityBit(QualityFlagV1::kSourceDisconnected);
    }
    if (state.connection_switched) {
        result.quality_flags |=
            QualityBit(QualityFlagV1::kConnectionSwitched);
    }
    return result;
}

void FillCounts(
    const ControlDecoderSnapshotV1& state,
    ControlRecordV1* record) noexcept {
    for (const ControlSubscriptionStateV1& entry :
         state.subscriptions) {
        if (entry.policy == SubscriptionPolicyV1::kRequired) {
            ++record->required_count;
            if (entry.status_known && entry.status == kMdlOk) {
                ++record->required_ok_count;
            } else if (entry.status_known) {
                ++record->required_failed_count;
            }
        } else {
            ++record->optional_count;
            if (entry.status_known && entry.status == kMdlOk) {
                ++record->optional_ok_count;
            } else if (entry.status_known) {
                ++record->optional_failed_count;
            }
        }
    }
    if (record->required_failed_count != 0U) {
        record->flags |= kControlRecordRequiredFailure;
    }
    if (record->optional_failed_count != 0U) {
        record->flags |= kControlRecordOptionalFailure;
    }
}

ControlRecordV1 BaseRecord(
    const l2flow::ingress::RawRecordView& input,
    const ControlDecoderSnapshotV1& state,
    ControlTypeV1 type) noexcept {
    const l2flow::ingress::RawRecordHeaderV1& header =
        input.header();
    const ControlRecordAttributionV1 attribution =
        Attribution(state);
    ControlRecordV1 record;
    record.control_type = type;
    record.source_stream_id = header.source_stream_id;
    record.capture_date = header.capture_date;
    record.stream_day_id = state.stream_day_id;
    record.vendor_service_id = header.vendor_service_id;
    record.vendor_service_version = header.vendor_service_version;
    record.vendor_message_id = header.vendor_message_id;
    record.connection_epoch = attribution.connection_epoch;
    record.subscription_epoch = attribution.subscription_epoch;
    record.origin_ingress_sequence = header.ingress_sequence;
    record.origin_record_end_wal_pos =
        input.record_end_wal_pos();
    record.quality_flags = attribution.quality_flags;
    record.control_state_sha256 = state.state_sha256;
    FillCounts(state, &record);
    return record;
}

bool ConfigValid(const ControlDecoderConfigV1& config) noexcept {
    if (config.source_stream_id == 0U ||
        config.capture_date == 0U ||
        l2flow::common::IsZeroIdentity(config.stream_day_id) ||
        DigestIsZero(config.stable_config_sha256) ||
        config.required.empty() ||
        config.required.size() > 64U ||
        config.optional.size() > 64U ||
        config.required.size() + config.optional.size() > 64U) {
        return false;
    }
    std::vector<l2flow::sdk::MessageKey> keys;
    const std::uint8_t market_service_id =
        config.required.front().service_id;
    for (const l2flow::sdk::MessageKey& key : config.required) {
        if (key.service_id != market_service_id ||
            !l2flow::sdk::RequiredMessageFixedBodyBytes(key)
                 .has_value()) {
            return false;
        }
    }
    for (const l2flow::sdk::MessageKey& key : config.optional) {
        if (key.service_id != market_service_id) {
            return false;
        }
    }
    try {
        keys.reserve(config.required.size() + config.optional.size());
        keys.insert(
            keys.end(), config.required.begin(), config.required.end());
        keys.insert(
            keys.end(), config.optional.begin(), config.optional.end());
    } catch (...) {
        return false;
    }
    for (const l2flow::sdk::MessageKey& key : keys) {
        if (key.service_id <= kSysServiceId ||
            key.service_version == 0U || key.message_id == 0U) {
            return false;
        }
    }
    std::sort(keys.begin(), keys.end(), KeyLess);
    return std::adjacent_find(keys.begin(), keys.end()) == keys.end();
}

bool RecordCoordinatesMatchSegment(
    const l2flow::ingress::RawRecordView& record,
    std::uint64_t segment_base_wal_pos) noexcept {
    return record.record_start_offset() >=
               l2flow::ingress::kRawV1SegmentHeaderBytes &&
           record.record_end_offset() >
               record.record_start_offset() &&
           record.record_start_wal_pos() >= segment_base_wal_pos &&
           record.record_end_wal_pos() >= segment_base_wal_pos &&
           record.record_start_wal_pos() - segment_base_wal_pos ==
               record.record_start_offset() &&
           record.record_end_wal_pos() - segment_base_wal_pos ==
               record.record_end_offset();
}

bool RawFrontierStructurallyValid(
    const l2flow::ingress::RawControlSnapshot& frontier) noexcept {
    return !l2flow::common::IsZeroIdentity(frontier.writer_instance) &&
           !l2flow::common::IsZeroIdentity(frontier.stream_day_id) &&
           frontier.source_stream_id != 0U &&
           frontier.capture_date != 0U &&
           frontier.fatal_state == 0U &&
           RawFrontierCursorShapeValidV1(frontier);
}

bool BoundaryMatchesCheckpoint(
    const ControlDecoderConfigV1& config,
    const ControlDecoderCheckpointV1& checkpoint,
    const l2flow::ingress::RawReplayRecord& boundary,
    const l2flow::ingress::RawControlSnapshot& durable_frontier)
    noexcept {
    const ControlDecoderSnapshotV1& state = checkpoint.state;
    const l2flow::ingress::RawRecordHeaderV1& header =
        boundary.view.header();
    if (!RawFrontierStructurallyValid(durable_frontier)) {
        return false;
    }
    const std::uint64_t frontier_segment_base_wal_pos =
        durable_frontier.append_global_wal_pos -
        durable_frontier.append_segment_offset;
    const bool boundary_in_current_segment =
        boundary.segment.segment_sequence ==
        durable_frontier.segment_sequence;
    const bool segment_coordinates_ordered =
        boundary_in_current_segment
            ? boundary.segment.segment_base_wal_pos ==
                      frontier_segment_base_wal_pos &&
                  boundary.view.record_end_offset() <=
                      durable_frontier.durable_segment_offset &&
                  (durable_frontier.durable_ingress_sequence !=
                       state.processed_ingress_sequence ||
                   boundary.view.record_end_wal_pos() ==
                       durable_frontier.durable_global_wal_pos)
            : boundary.segment.segment_sequence <
                      durable_frontier.segment_sequence &&
                  boundary.view.record_end_wal_pos() <=
                      frontier_segment_base_wal_pos;
    return state.counters.processed_records != 0U &&
           boundary.provenance ==
               l2flow::ingress::RawReplayProvenance::kDurable &&
           segment_coordinates_ordered &&
           durable_frontier.source_stream_id ==
               config.source_stream_id &&
           durable_frontier.capture_date == config.capture_date &&
           durable_frontier.stream_day_id == config.stream_day_id &&
           durable_frontier.append_global_wal_pos >=
               durable_frontier.durable_global_wal_pos &&
           durable_frontier.append_ingress_sequence >=
               durable_frontier.durable_ingress_sequence &&
           RawCursorAdvancePlausibleV1(
               state.processed_record_end_wal_pos,
               state.processed_ingress_sequence,
               durable_frontier.durable_global_wal_pos,
               durable_frontier.durable_ingress_sequence) &&
           boundary.segment.source_stream_id ==
               config.source_stream_id &&
           boundary.segment.capture_date == config.capture_date &&
           boundary.segment.stream_day_id == config.stream_day_id &&
           boundary.segment.segment_sequence != 0U &&
           boundary.segment.segment_sequence <=
               durable_frontier.segment_sequence &&
           boundary.segment.config_sha256 ==
               config.stable_config_sha256 &&
           boundary.segment.raw_schema_sha256 ==
               l2flow::ingress::RawSchemaSha256Digest() &&
           RecordCoordinatesMatchSegment(
               boundary.view,
               boundary.segment.segment_base_wal_pos) &&
           header.source_stream_id == config.source_stream_id &&
           header.capture_date == config.capture_date &&
           header.ingress_sequence ==
               state.processed_ingress_sequence &&
           boundary.view.record_start_wal_pos() ==
               state.processed_record_start_wal_pos &&
           boundary.view.record_end_wal_pos() ==
               state.processed_record_end_wal_pos &&
           boundary.view.record_end_wal_pos() >
               boundary.view.record_start_wal_pos();
}

std::vector<ControlSubscriptionStateV1> BuildSubscriptions(
    const ControlDecoderConfigV1& config) {
    std::vector<ControlSubscriptionStateV1> subscriptions;
    subscriptions.reserve(
        config.required.size() + config.optional.size());
    for (const l2flow::sdk::MessageKey& key : config.required) {
        subscriptions.push_back(ControlSubscriptionStateV1{
            key, SubscriptionPolicyV1::kRequired, 0U, false});
    }
    for (const l2flow::sdk::MessageKey& key : config.optional) {
        subscriptions.push_back(ControlSubscriptionStateV1{
            key, SubscriptionPolicyV1::kOptional, 0U, false});
    }
    std::sort(
        subscriptions.begin(),
        subscriptions.end(),
        SubscriptionLess);
    return subscriptions;
}

bool ApplyStatuses(
    std::vector<ControlSubscriptionStateV1>* subscriptions,
    std::span<const SubscriptionStatusV1> statuses,
    bool reset_before_apply) noexcept {
    if (reset_before_apply) {
        for (ControlSubscriptionStateV1& entry : *subscriptions) {
            entry.status = 0U;
            entry.status_known = false;
        }
    }
    for (const SubscriptionStatusV1& status : statuses) {
        const std::optional<std::size_t> index =
            FindSubscription(*subscriptions, status);
        if (!index.has_value()) {
            continue;
        }
        (*subscriptions)[*index].status = status.status;
        (*subscriptions)[*index].status_known = true;
    }
    return true;
}

}  // namespace

bool ComputeControlDecoderStateSha256V1(
    const ControlDecoderSnapshotV1& state,
    l2flow::common::Sha256Digest* digest) noexcept {
    return ComputeStateHashImpl(state, digest);
}

class ControlDecoderV1::Impl final {
public:
    explicit Impl(ControlDecoderConfigV1 config)
        : state_{} {
        state_.source_stream_id = config.source_stream_id;
        state_.capture_date = config.capture_date;
        state_.stream_day_id = config.stream_day_id;
        state_.stable_config_sha256 = config.stable_config_sha256;
        state_.subscriptions = BuildSubscriptions(config);
        state_.requested_manifest_sha256 =
            ComputeRequestedSubscriptionManifestSha256V1(
                state_.subscriptions);
        state_.next_ingress_sequence = 1U;
        state_.processed_ingress_sequence = 0U;
        RecomputeReadiness(&state_);
        if (!ComputeControlDecoderStateSha256V1(
                state_, &state_.state_sha256)) {
            throw std::bad_alloc();
        }
    }

    [[nodiscard]] bool Restore(
        const ControlDecoderCheckpointV1& checkpoint) {
        if (checkpoint.schema_version !=
                kControlDecoderCheckpointVersionV1 ||
            checkpoint.state.source_stream_id !=
                state_.source_stream_id ||
            checkpoint.state.capture_date != state_.capture_date ||
            checkpoint.state.stream_day_id != state_.stream_day_id ||
            checkpoint.state.stable_config_sha256 !=
                state_.stable_config_sha256 ||
            checkpoint.state.requested_manifest_sha256 !=
                state_.requested_manifest_sha256 ||
            checkpoint.state.subscriptions.size() !=
                state_.subscriptions.size()) {
            return false;
        }
        for (std::size_t index = 0U;
             index < state_.subscriptions.size();
             ++index) {
            if (checkpoint.state.subscriptions[index].key !=
                    state_.subscriptions[index].key ||
                checkpoint.state.subscriptions[index].policy !=
                    state_.subscriptions[index].policy) {
                return false;
            }
        }
        if (checkpoint.state.next_ingress_sequence == 0U ||
            checkpoint.state.processed_ingress_sequence ==
                std::numeric_limits<std::uint64_t>::max() ||
            checkpoint.state.next_ingress_sequence !=
                checkpoint.state.processed_ingress_sequence + 1U ||
            (checkpoint.state.counters.processed_records != 0U &&
             (checkpoint.state.processed_record_start_wal_pos == 0U ||
              checkpoint.state.processed_record_end_wal_pos <=
                  checkpoint.state.processed_record_start_wal_pos)) ||
            (checkpoint.state.poisoned &&
             checkpoint.state.session_phase !=
                 ControlSessionPhaseV1::kPoisoned) ||
            (checkpoint.state.logged_in &&
             checkpoint.state.session_phase !=
                 ControlSessionPhaseV1::kLoggedIn)) {
            return false;
        }

        ControlDecoderSnapshotV1 candidate = checkpoint.state;
        RecomputeReadiness(&candidate);
        if (candidate.control_ready !=
                checkpoint.state.control_ready ||
            candidate.decoder_evidence_ready !=
                checkpoint.state.decoder_evidence_ready) {
            return false;
        }
        l2flow::common::Sha256Digest computed{};
        if (!ComputeControlDecoderStateSha256V1(
                candidate, &computed) ||
            computed != checkpoint.state.state_sha256) {
            return false;
        }
        state_ = std::move(candidate);
        return true;
    }

    [[nodiscard]] ControlProcessResultV1 Process(
        const l2flow::ingress::RawRecordView& record) noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        ControlProcessResultV1 result;
        const l2flow::ingress::RawRecordHeaderV1& header =
            record.header();
        if (header.source_stream_id != state_.source_stream_id ||
            header.capture_date != state_.capture_date) {
            result.error = ControlProcessErrorV1::kNamespaceMismatch;
            return result;
        }
        if (header.ingress_sequence != state_.next_ingress_sequence) {
            result.error =
                ControlProcessErrorV1::kIngressSequenceMismatch;
            return result;
        }
        if (record.record_end_wal_pos() <=
                record.record_start_wal_pos() ||
            (state_.processed_record_end_wal_pos != 0U &&
             record.record_start_wal_pos() <
                 state_.processed_record_end_wal_pos)) {
            result.error = ControlProcessErrorV1::kWalCursorRegression;
            return result;
        }
        if (header.ingress_sequence ==
                std::numeric_limits<std::uint64_t>::max() ||
            state_.counters.processed_records ==
                std::numeric_limits<std::uint64_t>::max()) {
            return RejectCounterOverflow();
        }

        if (header.vendor_service_id == kApiServiceId) {
            DecodedApiControlV1 decoded{};
            const ApiControlDecodeErrorV1 error = DecodeApiControlV1(
                header.vendor_service_version,
                header.vendor_message_id,
                record.vendor_body(),
                &decoded);
            if (error == ApiControlDecodeErrorV1::kUnsupportedMessage) {
                return CommitOrdinary(record, std::nullopt);
            }
            if (error == ApiControlDecodeErrorV1::kResourceExhausted) {
                result.error =
                    ControlProcessErrorV1::kResourceExhausted;
                result.attribution = Attribution(state_);
                result.control_state_poisoned = state_.poisoned;
                return result;
            }
            if (error != ApiControlDecodeErrorV1::kNone) {
                return CommitMalformed(
                    record,
                    ControlProcessErrorV1::kMalformedApiControl,
                    static_cast<std::uint16_t>(
                        kApiDecodeErrorDomain |
                        static_cast<std::uint16_t>(error)),
                    ApiDecodeQualityFlagsV1(error));
            }
            return CommitApi(record, decoded);
        }
        if (header.vendor_service_id == kSysServiceId) {
            DecodedSysControlV1 decoded{};
            const SysControlDecodeErrorV1 error = DecodeSysControlV1(
                header.vendor_service_version,
                header.vendor_message_id,
                record.vendor_body(),
                &decoded);
            if (error == SysControlDecodeErrorV1::kUnsupportedMessage) {
                return CommitOrdinary(record, std::nullopt);
            }
            if (error == SysControlDecodeErrorV1::kResourceExhausted) {
                result.error =
                    ControlProcessErrorV1::kResourceExhausted;
                result.attribution = Attribution(state_);
                result.control_state_poisoned = state_.poisoned;
                return result;
            }
            if (error != SysControlDecodeErrorV1::kNone) {
                return CommitMalformed(
                    record,
                    ControlProcessErrorV1::kMalformedSysControl,
                    static_cast<std::uint16_t>(
                        kSysDecodeErrorDomain |
                        static_cast<std::uint16_t>(error)),
                    SysDecodeQualityFlagsV1(error));
            }
            return CommitSys(record, decoded);
        }
        return CommitMarket(record);
    }

    [[nodiscard]] ControlDecoderSnapshotV1 Snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return state_;
    }

    [[nodiscard]] ControlDecoderCheckpointV1 Checkpoint() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return ControlDecoderCheckpointV1{
            kControlDecoderCheckpointVersionV1, state_};
    }

    [[nodiscard]] bool ReplayNamespaceMatches(
        const l2flow::ingress::RawReplayRecord& record)
        const noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        const l2flow::ingress::RawReplaySegmentContext& segment =
            record.segment;
        return segment.source_stream_id == state_.source_stream_id &&
               segment.capture_date == state_.capture_date &&
               segment.stream_day_id == state_.stream_day_id &&
               segment.segment_sequence != 0U &&
               segment.config_sha256 ==
                   state_.stable_config_sha256 &&
               segment.raw_schema_sha256 ==
                   l2flow::ingress::RawSchemaSha256Digest() &&
               (record.provenance ==
                    l2flow::ingress::RawReplayProvenance::kDurable ||
                record.provenance == l2flow::ingress::
                    RawReplayProvenance::kRecoveredAppendOnly) &&
               RecordCoordinatesMatchSegment(
                   record.view, segment.segment_base_wal_pos);
    }

    [[nodiscard]] bool LiveNamespaceMatches(
        const l2flow::ingress::RawLiveRecord& record)
        const noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        return !l2flow::common::IsZeroIdentity(
                   record.writer_instance) &&
               record.control_generation != 0U &&
               (record.provenance ==
                    l2flow::ingress::RawLiveRecordProvenance::kDurable ||
                record.provenance == l2flow::ingress::
                    RawLiveRecordProvenance::kAppendVisible) &&
               record.segment.source_stream_id ==
                   state_.source_stream_id &&
               record.segment.capture_date == state_.capture_date &&
               record.segment.stream_day_id == state_.stream_day_id &&
               record.segment.segment_sequence != 0U &&
               record.segment.config_sha256 ==
                   state_.stable_config_sha256 &&
               record.segment.raw_schema_sha256 ==
                   l2flow::ingress::RawSchemaSha256Digest() &&
               RecordCoordinatesMatchSegment(
                   record.view,
                   record.segment.segment_base_wal_pos);
    }

private:
    static void AdvanceCursor(
        ControlDecoderSnapshotV1* state,
        const l2flow::ingress::RawRecordView& record) noexcept {
        state->processed_ingress_sequence =
            record.header().ingress_sequence;
        state->next_ingress_sequence =
            record.header().ingress_sequence + 1U;
        state->processed_record_start_wal_pos =
            record.record_start_wal_pos();
        state->processed_record_end_wal_pos =
            record.record_end_wal_pos();
        ++state->counters.processed_records;
    }

    [[nodiscard]] static bool FinalizeState(
        ControlDecoderSnapshotV1* state) noexcept {
        RecomputeReadiness(state);
        return ComputeControlDecoderStateSha256V1(
            *state, &state->state_sha256);
    }

    [[nodiscard]] ControlProcessResultV1 RejectCounterOverflow()
        noexcept {
        ControlProcessResultV1 result;
        result.error = ControlProcessErrorV1::kCounterOverflow;
        result.attribution = Attribution(state_);
        result.control_state_poisoned = state_.poisoned;
        return result;
    }

    [[nodiscard]] ControlProcessResultV1 CommitOrdinary(
        const l2flow::ingress::RawRecordView& record,
        std::optional<ControlRecordV1> control_record) noexcept {
        AdvanceCursor(&state_, record);
        if (!FinalizeState(&state_)) {
            state_.poisoned = true;
            state_.logged_in = false;
            state_.session_phase = ControlSessionPhaseV1::kPoisoned;
            RecomputeReadiness(&state_);
            ControlProcessResultV1 failed;
            failed.error = ControlProcessErrorV1::kStateHashFailure;
            failed.attribution = Attribution(state_);
            failed.cursor_committed = true;
            failed.control_state_poisoned = true;
            return failed;
        }
        ControlProcessResultV1 result;
        result.attribution = Attribution(state_);
        result.cursor_committed = true;
        result.control_state_poisoned = state_.poisoned;
        if (control_record.has_value()) {
            control_record->control_state_sha256 = state_.state_sha256;
            control_record->quality_flags =
                result.attribution.quality_flags;
            result.control_record = std::move(control_record);
        }
        return result;
    }

    [[nodiscard]] ControlProcessResultV1 CommitMalformed(
        const l2flow::ingress::RawRecordView& record,
        ControlProcessErrorV1 process_error,
        std::uint16_t decode_error,
        std::uint64_t quality_flags) noexcept {
        if (state_.counters.control_decode_errors ==
                std::numeric_limits<std::uint64_t>::max() ||
            state_.counters.emitted_control_records ==
                std::numeric_limits<std::uint64_t>::max()) {
            return RejectCounterOverflow();
        }
        ++state_.counters.control_decode_errors;
        state_.poisoned = true;
        state_.logged_in = false;
        state_.session_phase = ControlSessionPhaseV1::kPoisoned;
        state_.quality_flags |= quality_flags;
        AdvanceCursor(&state_, record);
        ++state_.counters.emitted_control_records;
        if (!FinalizeState(&state_)) {
            ControlProcessResultV1 failed;
            failed.error = ControlProcessErrorV1::kStateHashFailure;
            failed.attribution = Attribution(state_);
            failed.cursor_committed = true;
            failed.control_state_poisoned = true;
            return failed;
        }
        ControlRecordV1 control =
            BaseRecord(record, state_, ControlTypeV1::kDecodeError);
        control.decode_error = decode_error;
        if ((quality_flags &
             QualityBit(
                 QualityFlagV1::kNoncanonicalEmptyOffset)) != 0U) {
            control.flags |=
                kControlRecordNoncanonicalEmptyOffset;
        }
        ControlProcessResultV1 result;
        result.error = process_error;
        result.attribution = Attribution(state_);
        result.control_record = control;
        result.cursor_committed = true;
        result.control_state_poisoned = true;
        return result;
    }

    [[nodiscard]] ControlProcessResultV1 CommitApi(
        const l2flow::ingress::RawRecordView& record,
        const DecodedApiControlV1& decoded) noexcept {
        if (state_.counters.emitted_control_records ==
                std::numeric_limits<std::uint64_t>::max() ||
            (decoded.control_type == ControlTypeV1::kDisconnected &&
             state_.counters.disconnect ==
                 std::numeric_limits<std::uint64_t>::max())) {
            return RejectCounterOverflow();
        }

        ControlDecoderSnapshotV1 candidate;
        try {
            candidate = state_;
        } catch (...) {
            ControlProcessResultV1 failed;
            failed.error =
                ControlProcessErrorV1::kResourceExhausted;
            failed.attribution = Attribution(state_);
            failed.control_state_poisoned = state_.poisoned;
            return failed;
        }
        if (!candidate.poisoned) {
            if (decoded.address_present) {
                candidate.current_address_sha256 =
                    decoded.address_sha256;
            }
            switch (decoded.control_type) {
                case ControlTypeV1::kConnecting:
                    candidate.logged_in = false;
                    candidate.session_phase =
                        ControlSessionPhaseV1::kConnecting;
                    break;
                case ControlTypeV1::kConnectError:
                    candidate.logged_in = false;
                    candidate.disconnected_window = true;
                    candidate.session_phase =
                        ControlSessionPhaseV1::kConnectError;
                    break;
                case ControlTypeV1::kDisconnected:
                    candidate.logged_in = false;
                    candidate.disconnected_window = true;
                    candidate.session_phase =
                        ControlSessionPhaseV1::kDisconnected;
                    ++candidate.counters.disconnect;
                    break;
                default:
                    break;
            }
        } else if (decoded.control_type ==
                   ControlTypeV1::kDisconnected) {
            ++candidate.counters.disconnect;
        }
        if (decoded.noncanonical_empty_offset) {
            candidate.quality_flags |= QualityBit(
                QualityFlagV1::kNoncanonicalEmptyOffset);
        }
        AdvanceCursor(&candidate, record);
        ++candidate.counters.emitted_control_records;
        if (!FinalizeState(&candidate)) {
            ControlProcessResultV1 failed;
            failed.error = ControlProcessErrorV1::kStateHashFailure;
            failed.attribution = Attribution(state_);
            failed.control_state_poisoned = state_.poisoned;
            return failed;
        }
        state_ = std::move(candidate);

        ControlRecordV1 control =
            BaseRecord(record, state_, decoded.control_type);
        control.address_sha256 = decoded.address_sha256;
        control.error_text_sha256 = decoded.error_text_sha256;
        if (decoded.address_present) {
            control.flags |= kControlRecordAddressHashPresent;
        }
        if (decoded.error_text_present) {
            control.flags |= kControlRecordErrorTextHashPresent;
        }
        if (decoded.noncanonical_empty_offset) {
            control.flags |=
                kControlRecordNoncanonicalEmptyOffset;
        }
        ControlProcessResultV1 result;
        result.attribution = Attribution(state_);
        result.control_record = control;
        result.cursor_committed = true;
        result.control_state_poisoned = state_.poisoned;
        return result;
    }

    [[nodiscard]] ControlProcessResultV1 CommitSys(
        const l2flow::ingress::RawRecordView& record,
        const DecodedSysControlV1& decoded) noexcept {
        if (state_.counters.emitted_control_records ==
            std::numeric_limits<std::uint64_t>::max()) {
            return RejectCounterOverflow();
        }
        if ((decoded.control_type == ControlTypeV1::kLogonSuccess &&
             (state_.counters.logon_success ==
                  std::numeric_limits<std::uint64_t>::max() ||
              (!state_.poisoned &&
               state_.connection_epoch ==
                   std::numeric_limits<std::uint32_t>::max()))) ||
            (decoded.control_type == ControlTypeV1::kLogonFailure &&
             state_.counters.logon_failure ==
                 std::numeric_limits<std::uint64_t>::max()) ||
            ((decoded.control_type ==
                  ControlTypeV1::kSubscriptionAccepted ||
              decoded.control_type ==
                  ControlTypeV1::kSubscriptionRejected) &&
             state_.counters.subscription_responses ==
                 std::numeric_limits<std::uint64_t>::max())) {
            return RejectCounterOverflow();
        }

        ControlDecoderSnapshotV1 candidate;
        try {
            candidate = state_;
        } catch (...) {
            ControlProcessResultV1 failed;
            failed.error =
                ControlProcessErrorV1::kResourceExhausted;
            failed.attribution = Attribution(state_);
            failed.control_state_poisoned = state_.poisoned;
            return failed;
        }

        if (decoded.response_manifest_present) {
            candidate.response_manifest_sha256 =
                decoded.response_manifest_sha256;
            candidate.latest_response_ingress_sequence =
                record.header().ingress_sequence;
            candidate.latest_response_record_end_wal_pos =
                record.record_end_wal_pos();
        }
        if (decoded.noncanonical_empty_offset) {
            candidate.quality_flags |= QualityBit(
                QualityFlagV1::kNoncanonicalEmptyOffset);
        }

        if (decoded.control_type == ControlTypeV1::kLogonSuccess) {
            ++candidate.counters.logon_success;
            if (!candidate.poisoned) {
                const std::uint32_t previous_connection_epoch =
                    candidate.connection_epoch;
                const std::uint64_t previous_effective =
                    candidate.effective_success_mask;
                ApplyStatuses(
                    &candidate.subscriptions,
                    decoded.subscription_statuses,
                    true);
                const std::uint64_t next_effective =
                    EffectiveSuccessMask(candidate.subscriptions);
                const bool subscription_changed =
                    next_effective != previous_effective;
                if (subscription_changed &&
                    candidate.subscription_epoch ==
                        std::numeric_limits<std::uint32_t>::max()) {
                    return RejectCounterOverflow();
                }
                const bool connection_switched =
                    previous_connection_epoch != 0U &&
                    !DigestIsZero(candidate.current_address_sha256) &&
                    !DigestIsZero(
                        candidate.last_success_address_sha256) &&
                    candidate.current_address_sha256 !=
                        candidate.last_success_address_sha256;
                ++candidate.connection_epoch;
                if (subscription_changed) {
                    ++candidate.subscription_epoch;
                    if (previous_connection_epoch != 0U) {
                        candidate.quality_flags |= QualityBit(
                            QualityFlagV1::kSubscriptionChanged);
                    }
                }
                candidate.effective_success_mask = next_effective;
                candidate.logged_in = true;
                candidate.disconnected_window = false;
                candidate.connection_switched = connection_switched;
                candidate.session_phase =
                    ControlSessionPhaseV1::kLoggedIn;
                candidate.required_first_seen_mask = 0U;
                candidate.last_success_address_sha256 =
                    candidate.current_address_sha256;
                candidate.quality_flags &=
                    ~QualityBit(QualityFlagV1::kUnauthorized);
            }
        } else if (
            decoded.control_type == ControlTypeV1::kLogonFailure) {
            ++candidate.counters.logon_failure;
            if (!candidate.poisoned) {
                candidate.logged_in = false;
                candidate.session_phase =
                    ControlSessionPhaseV1::kLogonFailed;
                candidate.required_first_seen_mask = 0U;
                if (decoded.return_or_error_code ==
                    kMdlUnauthorized) {
                    candidate.quality_flags |= QualityBit(
                        QualityFlagV1::kUnauthorized);
                }
            }
        } else if (
            decoded.control_type ==
                ControlTypeV1::kSubscriptionAccepted ||
            decoded.control_type ==
                ControlTypeV1::kSubscriptionRejected) {
            ++candidate.counters.subscription_responses;
            if (!candidate.poisoned && candidate.logged_in) {
                const std::uint64_t previous_effective =
                    candidate.effective_success_mask;
                ApplyStatuses(
                    &candidate.subscriptions,
                    decoded.subscription_statuses,
                    false);
                const std::uint64_t next_effective =
                    EffectiveSuccessMask(candidate.subscriptions);
                const bool subscription_changed =
                    next_effective != previous_effective;
                if (subscription_changed &&
                    candidate.subscription_epoch ==
                        std::numeric_limits<std::uint32_t>::max()) {
                    return RejectCounterOverflow();
                }
                if (subscription_changed) {
                    ++candidate.subscription_epoch;
                    candidate.quality_flags |= QualityBit(
                        QualityFlagV1::kSubscriptionChanged);
                }
                candidate.effective_success_mask = next_effective;
                candidate.required_first_seen_mask &= next_effective;
            }
        }

        AdvanceCursor(&candidate, record);
        ++candidate.counters.emitted_control_records;
        if (!FinalizeState(&candidate)) {
            ControlProcessResultV1 failed;
            failed.error = ControlProcessErrorV1::kStateHashFailure;
            failed.attribution = Attribution(state_);
            failed.control_state_poisoned = state_.poisoned;
            return failed;
        }
        state_ = std::move(candidate);
        ControlRecordV1 control =
            BaseRecord(record, state_, decoded.control_type);
        control.return_or_error_code =
            decoded.return_or_error_code;
        control.response_entry_count =
            decoded.response_entry_count;
        control.response_manifest_sha256 =
            decoded.response_manifest_sha256;
        if (decoded.response_manifest_present) {
            control.flags |=
                kControlRecordResponseManifestHashPresent;
        }
        if (decoded.noncanonical_empty_offset) {
            control.flags |=
                kControlRecordNoncanonicalEmptyOffset;
        }
        ControlProcessResultV1 result;
        result.attribution = Attribution(state_);
        result.control_record = control;
        result.cursor_committed = true;
        result.control_state_poisoned = state_.poisoned;
        return result;
    }

    [[nodiscard]] ControlProcessResultV1 CommitMarket(
        const l2flow::ingress::RawRecordView& record) noexcept {
        if (!state_.poisoned && state_.logged_in) {
            const l2flow::sdk::MessageKey key{
                record.header().vendor_service_id,
                record.header().vendor_service_version,
                record.header().vendor_message_id};
            const auto found = std::lower_bound(
                state_.subscriptions.begin(),
                state_.subscriptions.end(),
                key,
                [](const ControlSubscriptionStateV1& entry,
                   const l2flow::sdk::MessageKey& wanted) {
                    return KeyLess(entry.key, wanted);
                });
            if (found != state_.subscriptions.end() &&
                found->key == key &&
                found->policy == SubscriptionPolicyV1::kRequired &&
                found->status_known && found->status == kMdlOk &&
                l2flow::ingress::ValidateRequiredMarketBodyBounds(
                    key, record.vendor_body())) {
                const std::size_t index =
                    static_cast<std::size_t>(std::distance(
                        state_.subscriptions.begin(), found));
                state_.required_first_seen_mask |=
                    std::uint64_t{1U} << index;
            }
        }
        return CommitOrdinary(record, std::nullopt);
    }

    mutable std::mutex mutex_;
    ControlDecoderSnapshotV1 state_;
};

l2flow::common::Sha256Digest
ComputeRequestedSubscriptionManifestSha256V1(
    std::span<const ControlSubscriptionStateV1>
        sorted_subscriptions) noexcept {
    constexpr std::string_view kDomain =
        "L2FLOW_PHASE3_REQUESTED_SUBSCRIPTION_MANIFEST_V1";
    const std::span<const char> domain_chars(
        kDomain.data(), kDomain.size());
    static constexpr std::array<std::byte, 1U> separator{
        std::byte{0}};
    l2flow::common::Sha256Digest digest{};
    if (sorted_subscriptions.size() >
        std::numeric_limits<std::uint32_t>::max()) {
        return digest;
    }
    for (std::size_t index = 0U;
         index < sorted_subscriptions.size();
         ++index) {
        const ControlSubscriptionStateV1& entry =
            sorted_subscriptions[index];
        if ((entry.policy != SubscriptionPolicyV1::kRequired &&
             entry.policy != SubscriptionPolicyV1::kOptional) ||
            (index != 0U &&
             !SubscriptionLess(
                 sorted_subscriptions[index - 1U], entry))) {
            return {};
        }
    }
    l2flow::common::Sha256Hasher hasher;
    if (!hasher.Update(std::as_bytes(domain_chars)) ||
        !hasher.Update(separator) ||
        !UpdateU32(
            &hasher,
            static_cast<std::uint32_t>(
                sorted_subscriptions.size()))) {
        return {};
    }
    for (const ControlSubscriptionStateV1& entry :
         sorted_subscriptions) {
        if (!UpdateU8(&hasher, entry.key.service_id) ||
            !UpdateU16(&hasher, entry.key.service_version) ||
            !UpdateU16(&hasher, entry.key.message_id) ||
            !UpdateU8(
                &hasher,
                static_cast<std::uint8_t>(entry.policy))) {
            return {};
        }
    }
    if (!hasher.Finalize(&digest)) {
        return {};
    }
    return digest;
}

ControlDecoderV1::ControlDecoderV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

ControlDecoderV1::~ControlDecoderV1() = default;

ControlDecoderCreateErrorV1 ControlDecoderV1::Create(
    ControlDecoderConfigV1 config,
    std::unique_ptr<ControlDecoderV1>* output) noexcept {
    if (output == nullptr) {
        return ControlDecoderCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (!ConfigValid(config)) {
        return ControlDecoderCreateErrorV1::kInvalidConfig;
    }
    try {
        auto impl = std::make_unique<Impl>(std::move(config));
        *output = std::unique_ptr<ControlDecoderV1>(
            new ControlDecoderV1(std::move(impl)));
        return ControlDecoderCreateErrorV1::kNone;
    } catch (...) {
        return ControlDecoderCreateErrorV1::kResourceExhausted;
    }
}

ControlDecoderCreateErrorV1 ControlDecoderV1::Restore(
    ControlDecoderConfigV1 config,
    const ControlDecoderCheckpointV1& checkpoint,
    const l2flow::ingress::RawReplayRecord& boundary_record,
    const l2flow::ingress::RawControlSnapshot& durable_frontier,
    std::unique_ptr<ControlDecoderV1>* output) noexcept {
    if (output == nullptr) {
        return ControlDecoderCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (!ConfigValid(config)) {
        return ControlDecoderCreateErrorV1::kInvalidConfig;
    }
    if (ValidateControlDecoderCheckpointV1(checkpoint) !=
            ControlCheckpointV1Error::kNone ||
        !BoundaryMatchesCheckpoint(
            config,
            checkpoint,
            boundary_record,
            durable_frontier)) {
        return ControlDecoderCreateErrorV1::kInvalidCheckpoint;
    }
    try {
        auto impl = std::make_unique<Impl>(std::move(config));
        if (!impl->Restore(checkpoint)) {
            return ControlDecoderCreateErrorV1::kInvalidCheckpoint;
        }
        *output = std::unique_ptr<ControlDecoderV1>(
            new ControlDecoderV1(std::move(impl)));
        return ControlDecoderCreateErrorV1::kNone;
    } catch (...) {
        return ControlDecoderCreateErrorV1::kResourceExhausted;
    }
}

ControlProcessResultV1 ControlDecoderV1::Process(
    const l2flow::ingress::RawLiveRecord& record) noexcept {
    if (!impl_->LiveNamespaceMatches(record)) {
        ControlProcessResultV1 result;
        result.error = ControlProcessErrorV1::kLiveSegmentMismatch;
        return result;
    }
    return impl_->Process(record.view);
}

ControlProcessResultV1 ControlDecoderV1::Process(
    const l2flow::ingress::RawReplayRecord& record) noexcept {
    if (!impl_->ReplayNamespaceMatches(record)) {
        ControlProcessResultV1 result;
        result.error = ControlProcessErrorV1::kReplaySegmentMismatch;
        return result;
    }
    ControlProcessResultV1 result = impl_->Process(record.view);
    if (result.cursor_committed &&
        record.provenance ==
            l2flow::ingress::RawReplayProvenance::
                kRecoveredAppendOnly) {
        const std::uint64_t provenance = QualityBit(
            QualityFlagV1::kRawRecoveredAppendOnly);
        result.attribution.quality_flags |= provenance;
        if (result.control_record.has_value()) {
            result.control_record->quality_flags |= provenance;
        }
    }
    return result;
}

ControlDecoderSnapshotV1 ControlDecoderV1::Snapshot() const {
    return impl_->Snapshot();
}

ControlDecoderCheckpointV1 ControlDecoderV1::Checkpoint() const {
    return impl_->Checkpoint();
}

}  // namespace l2flow::control
