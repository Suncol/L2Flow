#include "l2flow/ipc/partial_order_event_reader_c_v2.h"

#include "l2flow/common/identity128.h"
#include "l2flow/ipc/partial_order_event_control_v2.h"
#include "l2flow/ipc/partial_order_event_reader_v2.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include <sys/stat.h>
#include <unistd.h>

namespace ipc = l2flow::ipc;

struct l2flow_partial_order_event_reader_v2 final {
    std::unique_ptr<ipc::PartialOrderEventReaderV2> reader;
    std::unique_ptr<ipc::PartialEventBrokerLifecycleReaderV2>
        lifecycle_reader;
    ipc::PartialEventBrokerResponseV2 broker_response{};
    std::uint64_t total_mapping_bytes = 0U;
    std::uint64_t event_capacity = 0U;
    std::uint64_t order_state_capacity = 0U;
    std::uint32_t channel_capacity = 0U;
    mutable std::mutex scratch_mutex;
    mutable std::vector<ipc::PartialOrderEventEnvelopeV2> event_scratch;
    mutable std::vector<ipc::PartialOrderEventChannelHealthV2>
        channel_scratch;
    mutable std::vector<ipc::PartialOrderEventOrderStateV2> state_scratch;
};

namespace {

constexpr std::uint32_t kStatusSchemaVersion = 1U;
constexpr std::uint32_t kResultSchemaVersion = 1U;

static_assert(
    sizeof(l2flow_partial_order_event_expected_session_v2) == 32U);
static_assert(sizeof(l2flow_partial_order_event_checkpoint_v2) == 64U);
static_assert(sizeof(l2flow_partial_order_event_session_v2) == 104U);
static_assert(sizeof(l2flow_partial_order_event_status_v2) == 256U);
static_assert(
    sizeof(l2flow_partial_order_event_envelope_v2) ==
    sizeof(ipc::PartialOrderEventEnvelopeV2));
static_assert(sizeof(l2flow_partial_order_event_envelope_v2) == 328U);
static_assert(
    sizeof(l2flow_partial_order_event_order_state_v2) ==
    sizeof(ipc::PartialOrderEventOrderStateV2));
static_assert(sizeof(l2flow_partial_order_event_order_state_v2) == 328U);
static_assert(
    sizeof(l2flow_partial_order_event_channel_health_v2) ==
    sizeof(ipc::PartialOrderEventChannelHealthV2));
static_assert(sizeof(l2flow_partial_order_event_channel_health_v2) == 128U);
static_assert(
    sizeof(l2flow_partial_order_event_read_batch_result_v2) == 368U);
static_assert(
    sizeof(l2flow_partial_order_event_channel_batch_result_v2) == 312U);
static_assert(
    sizeof(l2flow_partial_order_event_order_state_batch_result_v2) ==
    368U);
static_assert(
    static_cast<int>(ipc::PartialOrderEventReadResultV2::kCorrupt) ==
    L2FLOW_PARTIAL_ORDER_EVENT_READ_CORRUPT_V2);
static_assert(
    static_cast<std::uint32_t>(
        ipc::PartialOrderEventTemporalCoverageV2::kProcessStart) ==
    L2FLOW_PARTIAL_ORDER_EVENT_COVERAGE_PROCESS_START_V2);
static_assert(
    static_cast<std::uint32_t>(
        ipc::PartialOrderEventOrderingQualityV2::
            kBoundedReorderedPartial) ==
    L2FLOW_PARTIAL_ORDER_EVENT_ORDERING_BOUNDED_REORDERED_PARTIAL_V2);
static_assert(
    static_cast<std::uint32_t>(
        ipc::PartialOrderEventServiceStateV2::kStoppedClean) ==
    L2FLOW_PARTIAL_ORDER_EVENT_STATE_STOPPED_CLEAN_V2);
static_assert(
    static_cast<std::uint32_t>(
        ipc::PartialOrderEventLastErrorV2::kPublicationInvariant) ==
    L2FLOW_PARTIAL_ORDER_EVENT_ERROR_PUBLICATION_INVARIANT_V2);
static_assert(
    static_cast<std::uint32_t>(
        ipc::PartialEventBrokerStateV2::kStoppedClean) ==
    L2FLOW_PARTIAL_ORDER_EVENT_BROKER_STOPPED_CLEAN_V2);

class Descriptor final {
public:
    Descriptor() noexcept = default;
    Descriptor(const Descriptor&) = delete;
    Descriptor& operator=(const Descriptor&) = delete;
    ~Descriptor() {
        if (value_ >= 0) {
            static_cast<void>(::close(value_));
        }
    }
    [[nodiscard]] int get() const noexcept { return value_; }
    [[nodiscard]] int* output() noexcept { return &value_; }

private:
    int value_ = -1;
};

[[nodiscard]] bool IdentityNonzero(const std::uint8_t* identity) noexcept {
    if (identity == nullptr) {
        return false;
    }
    for (std::size_t index = 0U; index < 16U; ++index) {
        if (identity[index] != 0U) {
            return true;
        }
    }
    return false;
}

[[nodiscard]] l2flow::common::Identity128 NativeIdentity(
    const std::uint8_t* identity) noexcept {
    l2flow::common::Identity128 result{};
    std::memcpy(result.data(), identity, result.size());
    return result;
}

[[nodiscard]] bool ExpectedValid(
    const l2flow_partial_order_event_expected_session_v2& expected) noexcept {
    return IdentityNonzero(expected.run_id) &&
           expected.session_epoch != 0U && expected.trade_date != 0U &&
           expected.reserved == 0U;
}

[[nodiscard]] bool CheckpointCanonical(
    const l2flow_partial_order_event_checkpoint_v2& checkpoint) noexcept {
    return IdentityNonzero(checkpoint.run_id) &&
           checkpoint.session_epoch != 0U && checkpoint.trade_date != 0U &&
           checkpoint.publication_generation != 0U &&
           checkpoint.correction_epoch != 0U &&
           checkpoint.next_event_sequence != 0U &&
           checkpoint.reserved == 0U;
}

[[nodiscard]] bool CheckpointBaseMatches(
    const l2flow_partial_order_event_checkpoint_v2& checkpoint,
    const l2flow_partial_order_event_expected_session_v2& expected) noexcept {
    return std::memcmp(
               checkpoint.run_id,
               expected.run_id,
               sizeof(checkpoint.run_id)) == 0 &&
           checkpoint.session_epoch == expected.session_epoch &&
           checkpoint.trade_date == expected.trade_date;
}

[[nodiscard]] bool CheckpointMatches(
    const l2flow_partial_order_event_checkpoint_v2& checkpoint,
    const ipc::PartialEventBrokerResponseV2& response) noexcept {
    return std::memcmp(
               checkpoint.run_id,
               response.run_id.data(),
               sizeof(checkpoint.run_id)) == 0 &&
           checkpoint.session_epoch == response.session_epoch &&
           checkpoint.trade_date == response.trade_date &&
           checkpoint.publication_generation ==
               response.publication_generation &&
           checkpoint.correction_epoch == response.correction_epoch;
}

void CopyCheckpoint(
    const ipc::PartialEventBrokerResponseV2& response,
    std::uint64_t next_event_sequence,
    std::uint64_t next_order_state_physical_slot,
    l2flow_partial_order_event_checkpoint_v2* output) noexcept {
    *output = {};
    std::memcpy(
        output->run_id, response.run_id.data(), sizeof(output->run_id));
    output->session_epoch = response.session_epoch;
    output->publication_generation = response.publication_generation;
    output->correction_epoch = response.correction_epoch;
    output->next_event_sequence = next_event_sequence;
    output->next_order_state_physical_slot =
        next_order_state_physical_slot;
    output->trade_date = response.trade_date;
}

[[nodiscard]] int ReadLiveBrokerStatus(
    const l2flow_partial_order_event_reader_v2& reader,
    ipc::PartialEventBrokerResponseV2* output,
    bool evaluate_heartbeat = true) noexcept {
    *output = reader.broker_response;
    ipc::PartialEventBrokerLifecycleSnapshotV2 lifecycle{};
    if (reader.lifecycle_reader == nullptr ||
        reader.lifecycle_reader->ReadSnapshot(
            &lifecycle, evaluate_heartbeat) !=
            ipc::PartialEventBrokerLifecycleReadResultV2::kOk) {
        output->broker_state = ipc::PartialEventBrokerStateV2::kStale;
        output->broker_stale = 1U;
        return L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2;
    }
    if (lifecycle.publication_generation !=
            reader.broker_response.publication_generation ||
        lifecycle.correction_epoch !=
            reader.broker_response.correction_epoch) {
        // The descriptor still contains its last-good data, but no call may
        // silently mix that data with the broker's replacement identity.
        output->broker_state = ipc::PartialEventBrokerStateV2::kStale;
        output->broker_stale = 1U;
        return
            L2FLOW_PARTIAL_ORDER_EVENT_READ_FULL_REPLACEMENT_REQUIRED_V2;
    }
    output->broker_state = lifecycle.broker_state;
    output->broker_stale = lifecycle.broker_stale ? 1U : 0U;
    return L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2;
}

void CopyStatus(
    const ipc::PartialOrderEventStatusSnapshotV2& source,
    const ipc::PartialEventBrokerResponseV2& broker,
    l2flow_partial_order_event_status_v2* output) noexcept {
    *output = {};
    output->status_schema_version = kStatusSchemaVersion;
    output->status_bytes = sizeof(*output);
    std::memcpy(output->run_id, source.run_id.data(), sizeof(output->run_id));
    output->session_epoch = source.session_epoch;
    output->publication_generation = source.publication_generation;
    output->correction_epoch = source.correction_epoch;
    output->coverage_start_unix_ns = source.coverage_start_unix_ns;
    output->event_capacity = source.event_capacity;
    output->order_state_capacity = source.order_state_capacity;
    output->commit_sequence = source.cut.commit_sequence;
    output->heartbeat_monotonic_ns = source.cut.heartbeat_monotonic_ns;
    output->captured_source_frontier =
        source.cut.captured_source_frontier;
    output->canonical_apply_frontier =
        source.cut.canonical_apply_frontier;
    output->event_published_frontier =
        source.cut.event_published_frontier;
    output->history_generation = source.cut.history_generation;
    output->order_state_generation = source.cut.order_state_generation;
    output->order_state_canonical_frontier =
        source.cut.order_state_canonical_frontier;
    output->committed_event_region_bytes =
        source.cut.committed_event_region_bytes;
    output->shanghai_order_state_count =
        source.cut.shanghai_order_state_count;
    output->shenzhen_order_state_count =
        source.cut.shenzhen_order_state_count;
    output->pending_count = source.cut.pending_count;
    output->reorder_high_water = source.cut.reorder_high_water;
    output->oldest_gap_age_ns = source.cut.oldest_gap_age_ns;
    output->trade_date = source.trade_date;
    output->temporal_coverage =
        static_cast<std::uint32_t>(source.temporal_coverage);
    output->ordering_quality =
        static_cast<std::uint32_t>(source.ordering_quality);
    output->service_state =
        static_cast<std::uint32_t>(source.cut.state);
    output->stale = source.cut.stale;
    output->last_error =
        static_cast<std::uint32_t>(source.cut.last_error);
    output->affected_channel_count = source.cut.affected_channel_count;
    output->channel_health_count = source.cut.channel_health_count;
    output->affected_channel_capacity = source.channel_capacity;
    output->broker_state =
        static_cast<std::uint32_t>(broker.broker_state);
    output->broker_stale = broker.broker_stale;
}

void CopyChannel(
    const ipc::PartialOrderEventChannelHealthV2& source,
    l2flow_partial_order_event_channel_health_v2* output) noexcept {
    *output = {};
    output->commit_sequence = source.commit_sequence;
    output->channel = source.channel;
    output->expected_native_sequence = source.expected_native_sequence;
    output->contiguous_native_sequence =
        source.contiguous_native_sequence;
    output->highest_observed_native_sequence =
        source.highest_observed_native_sequence;
    output->oldest_missing_native_sequence =
        source.oldest_missing_native_sequence;
    output->pending_count = source.pending_count;
    output->oldest_gap_age_ns = source.oldest_gap_age_ns;
    output->market = source.market;
    output->flags = source.flags;
    output->service_state = static_cast<std::uint32_t>(source.state);
    output->last_error = static_cast<std::uint32_t>(source.last_error);
}

[[nodiscard]] int MapBrokerOpenResult(
    ipc::PartialEventBrokerResultV2 result) noexcept {
    switch (result) {
        case ipc::PartialEventBrokerResultV2::kOk:
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_OK_V2;
        case ipc::PartialEventBrokerResultV2::kUnavailable:
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_BROKER_UNAVAILABLE_V2;
        case ipc::PartialEventBrokerResultV2::kProtocolError:
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_BROKER_PROTOCOL_ERROR_V2;
        case ipc::PartialEventBrokerResultV2::kPeerRejected:
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_BROKER_PEER_REJECTED_V2;
        case ipc::PartialEventBrokerResultV2::kSessionMismatch:
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_BROKER_SESSION_MISMATCH_V2;
        case ipc::PartialEventBrokerResultV2::kGenerationRejected:
            return
                L2FLOW_PARTIAL_ORDER_EVENT_OPEN_BROKER_GENERATION_REJECTED_V2;
        case ipc::PartialEventBrokerResultV2::kDescriptorRejected:
            return
                L2FLOW_PARTIAL_ORDER_EVENT_OPEN_BROKER_DESCRIPTOR_REJECTED_V2;
        case ipc::PartialEventBrokerResultV2::kContinuityRejected:
            return
                L2FLOW_PARTIAL_ORDER_EVENT_OPEN_BROKER_CONTINUITY_REJECTED_V2;
        case ipc::PartialEventBrokerResultV2::kInternalError:
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_BROKER_INTERNAL_ERROR_V2;
    }
    return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_UNEXPECTED_FAILURE_V2;
}

[[nodiscard]] int MapReaderOpenResult(
    ipc::PartialOrderEventReaderOpenErrorV2 result) noexcept {
    switch (result) {
        case ipc::PartialOrderEventReaderOpenErrorV2::kNone:
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_OK_V2;
        case ipc::PartialOrderEventReaderOpenErrorV2::kNullOutput:
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_NULL_OUTPUT_V2;
        case ipc::PartialOrderEventReaderOpenErrorV2::kInvalidDescriptor:
            return
                L2FLOW_PARTIAL_ORDER_EVENT_OPEN_BROKER_DESCRIPTOR_REJECTED_V2;
        case ipc::PartialOrderEventReaderOpenErrorV2::kInvalidExpectedSession:
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_INVALID_ARGUMENT_V2;
        case ipc::PartialOrderEventReaderOpenErrorV2::kDescriptorStatFailed:
            return
                L2FLOW_PARTIAL_ORDER_EVENT_OPEN_DESCRIPTOR_STAT_FAILED_V2;
        case ipc::PartialOrderEventReaderOpenErrorV2::kDescriptorSealMismatch:
            return
                L2FLOW_PARTIAL_ORDER_EVENT_OPEN_DESCRIPTOR_SEAL_MISMATCH_V2;
        case ipc::PartialOrderEventReaderOpenErrorV2::kMappingFailed:
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_MAPPING_FAILED_V2;
        case ipc::PartialOrderEventReaderOpenErrorV2::kIncompatibleLayout:
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_INCOMPATIBLE_LAYOUT_V2;
        case ipc::PartialOrderEventReaderOpenErrorV2::kSessionMismatch:
            return
                L2FLOW_PARTIAL_ORDER_EVENT_OPEN_MAPPING_SESSION_MISMATCH_V2;
        case ipc::PartialOrderEventReaderOpenErrorV2::
            kNoStablePublicationCut:
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_NO_STABLE_CUT_V2;
        case ipc::PartialOrderEventReaderOpenErrorV2::kUnexpectedFailure:
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_UNEXPECTED_FAILURE_V2;
    }
    return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_UNEXPECTED_FAILURE_V2;
}

[[nodiscard]] int MapLifecycleOpenResult(
    ipc::PartialEventBrokerLifecycleOpenErrorV2 result,
    int system_error_number) noexcept {
    switch (result) {
        case ipc::PartialEventBrokerLifecycleOpenErrorV2::kNone:
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_OK_V2;
        case ipc::PartialEventBrokerLifecycleOpenErrorV2::kNullOutput:
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_NULL_OUTPUT_V2;
        case ipc::PartialEventBrokerLifecycleOpenErrorV2::kInvalidDescriptor:
            return
                L2FLOW_PARTIAL_ORDER_EVENT_OPEN_BROKER_DESCRIPTOR_REJECTED_V2;
        case ipc::PartialEventBrokerLifecycleOpenErrorV2::
            kDescriptorStatFailed:
            return
                L2FLOW_PARTIAL_ORDER_EVENT_OPEN_DESCRIPTOR_STAT_FAILED_V2;
        case ipc::PartialEventBrokerLifecycleOpenErrorV2::
            kDescriptorSealMismatch:
            return
                L2FLOW_PARTIAL_ORDER_EVENT_OPEN_DESCRIPTOR_SEAL_MISMATCH_V2;
        case ipc::PartialEventBrokerLifecycleOpenErrorV2::kMappingFailed:
            return system_error_number == 0
                       ? L2FLOW_PARTIAL_ORDER_EVENT_OPEN_RESOURCE_EXHAUSTED_V2
                       : L2FLOW_PARTIAL_ORDER_EVENT_OPEN_MAPPING_FAILED_V2;
        case ipc::PartialEventBrokerLifecycleOpenErrorV2::
            kIncompatibleLayout:
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_INCOMPATIBLE_LAYOUT_V2;
        case ipc::PartialEventBrokerLifecycleOpenErrorV2::kSessionMismatch:
            return
                L2FLOW_PARTIAL_ORDER_EVENT_OPEN_MAPPING_SESSION_MISMATCH_V2;
        case ipc::PartialEventBrokerLifecycleOpenErrorV2::kNoStableSnapshot:
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_NO_STABLE_CUT_V2;
        case ipc::PartialEventBrokerLifecycleOpenErrorV2::
            kUnexpectedFailure:
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_UNEXPECTED_FAILURE_V2;
    }
    return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_UNEXPECTED_FAILURE_V2;
}

[[nodiscard]] bool ReaderValid(
    const l2flow_partial_order_event_reader_v2* reader) noexcept {
    return reader != nullptr && reader->reader != nullptr;
}

[[nodiscard]] int ReadStatus(
    const l2flow_partial_order_event_reader_v2* reader,
    l2flow_partial_order_event_status_v2* output) noexcept {
    if (!ReaderValid(reader) || output == nullptr) {
        return L2FLOW_PARTIAL_ORDER_EVENT_READ_INVALID_ARGUMENT_V2;
    }
    ipc::PartialOrderEventStatusSnapshotV2 status{};
    const auto result = reader->reader->ReadStatus(&status);
    ipc::PartialEventBrokerResponseV2 broker{};
    const int lifecycle_result = ReadLiveBrokerStatus(*reader, &broker);
    if (lifecycle_result != L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2) {
        *output = {};
        return lifecycle_result;
    }
    if (result == ipc::PartialOrderEventReadResultV2::kOk) {
        CopyStatus(status, broker, output);
    } else {
        *output = {};
    }
    return static_cast<int>(result);
}

[[nodiscard]] ipc::PartialOrderEventExpectedSessionV2 ExpectedMapping(
    const ipc::PartialEventBrokerResponseV2& response) noexcept {
    ipc::PartialOrderEventExpectedSessionV2 result{};
    std::memcpy(
        result.run_id.data(), response.run_id.data(), result.run_id.size());
    result.session_epoch = response.session_epoch;
    result.trade_date = response.trade_date;
    result.publication_generation = response.publication_generation;
    result.correction_epoch = response.correction_epoch;
    return result;
}

}  // namespace

extern "C" int l2flow_partial_order_event_reader_open_v2(
    const char* absolute_control_socket_path,
    const l2flow_partial_order_event_expected_session_v2* expected_session,
    const l2flow_partial_order_event_checkpoint_v2* checkpoint,
    std::uint32_t timeout_ms,
    l2flow_partial_order_event_reader_v2** output,
    int* system_error_number) {
    if (system_error_number != nullptr) {
        *system_error_number = 0;
    }
    if (output == nullptr) {
        return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_NULL_OUTPUT_V2;
    }
    *output = nullptr;
    if (absolute_control_socket_path == nullptr || expected_session == nullptr ||
        timeout_ms == 0U || timeout_ms > 60'000U ||
        !ExpectedValid(*expected_session) ||
        (checkpoint != nullptr &&
         (!CheckpointCanonical(*checkpoint) ||
          !CheckpointBaseMatches(*checkpoint, *expected_session)))) {
        return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_INVALID_ARGUMENT_V2;
    }
    try {
        const std::filesystem::path socket_path(
            absolute_control_socket_path);
        if (socket_path.empty() || !socket_path.is_absolute()) {
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_INVALID_ARGUMENT_V2;
        }
        Descriptor descriptor;
        Descriptor lifecycle_descriptor;
        ipc::PartialEventBrokerResponseV2 response{};
        const auto broker_result = ipc::RequestPartialEventGenerationV2(
            socket_path,
            NativeIdentity(expected_session->run_id),
            expected_session->session_epoch,
            expected_session->trade_date,
            checkpoint == nullptr
                ? 0U
                : checkpoint->publication_generation,
            std::chrono::milliseconds(timeout_ms),
            descriptor.output(),
            lifecycle_descriptor.output(),
            &response,
            system_error_number);
        if (broker_result != ipc::PartialEventBrokerResultV2::kOk) {
            return MapBrokerOpenResult(broker_result);
        }
        if (response.ordering_quality !=
            ipc::PartialOrderEventOrderingQualityV2::
                kBoundedReorderedPartial) {
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_UNSUPPORTED_ORDERING_QUALITY_V2;
        }
        if (checkpoint != nullptr &&
            !CheckpointMatches(*checkpoint, response)) {
            return
                L2FLOW_PARTIAL_ORDER_EVENT_OPEN_FULL_REPLACEMENT_REQUIRED_V2;
        }

        struct stat descriptor_stat {};
        const int stat_result = ::fstat(descriptor.get(), &descriptor_stat);
        if (stat_result != 0 || descriptor_stat.st_size <= 0) {
            if (system_error_number != nullptr) {
                *system_error_number = stat_result != 0 ? errno : EINVAL;
            }
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_DESCRIPTOR_STAT_FAILED_V2;
        }
        const auto mapping_bytes =
            static_cast<std::uintmax_t>(descriptor_stat.st_size);
        if (mapping_bytes >
            static_cast<std::uintmax_t>(
                std::numeric_limits<std::uint64_t>::max())) {
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_INCOMPATIBLE_LAYOUT_V2;
        }

        std::unique_ptr<ipc::PartialOrderEventReaderV2> native;
        const auto reader_result =
            ipc::PartialOrderEventReaderV2::OpenDescriptor(
                descriptor.get(),
                ExpectedMapping(response),
                &native,
                system_error_number);
        if (reader_result !=
                ipc::PartialOrderEventReaderOpenErrorV2::kNone ||
            native == nullptr) {
            return MapReaderOpenResult(reader_result);
        }
        std::unique_ptr<ipc::PartialEventBrokerLifecycleReaderV2> lifecycle;
        int lifecycle_system_error = 0;
        const auto lifecycle_result =
            ipc::PartialEventBrokerLifecycleReaderV2::OpenDescriptor(
                lifecycle_descriptor.get(),
                NativeIdentity(expected_session->run_id),
                expected_session->session_epoch,
                expected_session->trade_date,
                &lifecycle,
                &lifecycle_system_error);
        if (system_error_number != nullptr) {
            *system_error_number = lifecycle_system_error;
        }
        if (lifecycle_result !=
                ipc::PartialEventBrokerLifecycleOpenErrorV2::kNone ||
            lifecycle == nullptr) {
            return MapLifecycleOpenResult(
                lifecycle_result, lifecycle_system_error);
        }
        std::unique_ptr<l2flow_partial_order_event_reader_v2> wrapper(
            new (std::nothrow) l2flow_partial_order_event_reader_v2{});
        if (wrapper == nullptr) {
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_RESOURCE_EXHAUSTED_V2;
        }
        wrapper->reader = std::move(native);
        wrapper->lifecycle_reader = std::move(lifecycle);
        wrapper->broker_response = response;
        wrapper->total_mapping_bytes =
            static_cast<std::uint64_t>(mapping_bytes);

        ipc::PartialOrderEventStatusSnapshotV2 status{};
        if (wrapper->reader->ReadStatus(&status) !=
            ipc::PartialOrderEventReadResultV2::kOk) {
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_NO_STABLE_CUT_V2;
        }
        if (status.ordering_quality !=
            ipc::PartialOrderEventOrderingQualityV2::
                kBoundedReorderedPartial) {
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_UNSUPPORTED_ORDERING_QUALITY_V2;
        }
        wrapper->event_capacity = status.event_capacity;
        wrapper->order_state_capacity = status.order_state_capacity;
        wrapper->channel_capacity = status.channel_capacity;
        if (checkpoint != nullptr &&
            checkpoint->next_event_sequence > status.event_capacity) {
            // event_capacity + 1 is the only legal idle cursor beyond storage.
            const bool natural_tail =
                status.event_capacity !=
                    std::numeric_limits<std::uint64_t>::max() &&
                checkpoint->next_event_sequence ==
                    status.event_capacity + 1U;
            if (!natural_tail) {
                return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_INVALID_ARGUMENT_V2;
            }
        }
        if (checkpoint != nullptr &&
            checkpoint->next_order_state_physical_slot >
                status.order_state_capacity) {
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_INVALID_ARGUMENT_V2;
        }
        ipc::PartialEventBrokerResponseV2 live_broker{};
        if (ReadLiveBrokerStatus(*wrapper, &live_broker) ==
            L2FLOW_PARTIAL_ORDER_EVENT_READ_FULL_REPLACEMENT_REQUIRED_V2) {
            return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_FULL_REPLACEMENT_REQUIRED_V2;
        }
        *output = wrapper.release();
        return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_OK_V2;
    } catch (const std::bad_alloc&) {
        return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_RESOURCE_EXHAUSTED_V2;
    } catch (...) {
        return L2FLOW_PARTIAL_ORDER_EVENT_OPEN_UNEXPECTED_FAILURE_V2;
    }
}

extern "C" void l2flow_partial_order_event_reader_close_v2(
    l2flow_partial_order_event_reader_v2* reader) {
    delete reader;
}

extern "C" int l2flow_partial_order_event_reader_session_v2(
    const l2flow_partial_order_event_reader_v2* reader,
    l2flow_partial_order_event_session_v2* output) {
    if (!ReaderValid(reader) || output == nullptr) {
        return L2FLOW_PARTIAL_ORDER_EVENT_READ_INVALID_ARGUMENT_V2;
    }
    l2flow_partial_order_event_status_v2 status{};
    const int result = ReadStatus(reader, &status);
    if (result != L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2) {
        *output = {};
        return result;
    }
    *output = {};
    std::memcpy(output->run_id, status.run_id, sizeof(output->run_id));
    output->session_epoch = status.session_epoch;
    output->publication_generation = status.publication_generation;
    output->correction_epoch = status.correction_epoch;
    output->coverage_start_unix_ns = status.coverage_start_unix_ns;
    output->event_capacity = status.event_capacity;
    output->order_state_capacity = status.order_state_capacity;
    output->total_mapping_bytes = reader->total_mapping_bytes;
    output->trade_date = status.trade_date;
    output->temporal_coverage = status.temporal_coverage;
    output->ordering_quality = status.ordering_quality;
    output->affected_channel_capacity =
        status.affected_channel_capacity;
    output->broker_state = status.broker_state;
    output->broker_stale = status.broker_stale;
    return L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2;
}

extern "C" int l2flow_partial_order_event_reader_status_v2(
    const l2flow_partial_order_event_reader_v2* reader,
    l2flow_partial_order_event_status_v2* output) {
    return ReadStatus(reader, output);
}

extern "C" int l2flow_partial_order_event_reader_checkpoint_v2(
    const l2flow_partial_order_event_reader_v2* reader,
    std::uint64_t next_event_sequence,
    std::uint64_t next_order_state_physical_slot,
    l2flow_partial_order_event_checkpoint_v2* output) {
    if (!ReaderValid(reader) || output == nullptr ||
        next_event_sequence == 0U ||
        (next_event_sequence > reader->event_capacity &&
         (reader->event_capacity ==
              std::numeric_limits<std::uint64_t>::max() ||
          next_event_sequence != reader->event_capacity + 1U)) ||
        next_order_state_physical_slot > reader->order_state_capacity) {
        return L2FLOW_PARTIAL_ORDER_EVENT_READ_INVALID_ARGUMENT_V2;
    }
    *output = {};
    ipc::PartialEventBrokerResponseV2 live_broker{};
    const int lifecycle_result =
        ReadLiveBrokerStatus(*reader, &live_broker);
    if (lifecycle_result != L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2) {
        return lifecycle_result;
    }
    CopyCheckpoint(
        reader->broker_response,
        next_event_sequence,
        next_order_state_physical_slot,
        output);
    return L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2;
}

extern "C" int l2flow_partial_order_event_reader_read_v2(
    const l2flow_partial_order_event_reader_v2* reader,
    const l2flow_partial_order_event_checkpoint_v2* checkpoint,
    l2flow_partial_order_event_envelope_v2* output,
    std::size_t capacity,
    l2flow_partial_order_event_read_batch_result_v2* result) {
    if (result == nullptr) {
        return L2FLOW_PARTIAL_ORDER_EVENT_READ_INVALID_ARGUMENT_V2;
    }
    *result = {};
    result->result_schema_version = kResultSchemaVersion;
    result->result_bytes = sizeof(*result);
    if (!ReaderValid(reader) || checkpoint == nullptr || capacity == 0U ||
        output == nullptr || !CheckpointCanonical(*checkpoint)) {
        return L2FLOW_PARTIAL_ORDER_EVENT_READ_INVALID_ARGUMENT_V2;
    }
    result->checkpoint = *checkpoint;
    if (!CheckpointMatches(*checkpoint, reader->broker_response)) {
        return
            L2FLOW_PARTIAL_ORDER_EVENT_READ_FULL_REPLACEMENT_REQUIRED_V2;
    }
    try {
        std::lock_guard lock(reader->scratch_mutex);
        const std::size_t native_capacity = static_cast<std::size_t>(
            std::min<std::uint64_t>(
                static_cast<std::uint64_t>(capacity),
                reader->event_capacity));
        if (native_capacity == 0U) {
            return L2FLOW_PARTIAL_ORDER_EVENT_READ_INVALID_ARGUMENT_V2;
        }
        reader->event_scratch.resize(native_capacity);
        ipc::PartialOrderEventReadBatchResultV2 native_result{};
        const auto read = reader->reader->ReadEvents(
            checkpoint->next_event_sequence,
            std::span<ipc::PartialOrderEventEnvelopeV2>(
                reader->event_scratch.data(), native_capacity),
            &native_result);
        ipc::PartialEventBrokerResponseV2 live_broker{};
        const int lifecycle_result =
            ReadLiveBrokerStatus(
                *reader,
                &live_broker,
                native_result.rows_read == 0U);
        if (lifecycle_result != L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2) {
            return lifecycle_result;
        }
        result->records_written = native_result.rows_read;
        if (native_result.status.cut.commit_sequence != 0U) {
            CopyStatus(
                native_result.status,
                live_broker,
                &result->status);
        }
        if (native_result.next_event_sequence != 0U) {
            CopyCheckpoint(
                reader->broker_response,
                native_result.next_event_sequence,
                checkpoint->next_order_state_physical_slot,
                &result->checkpoint);
        }
        if (native_result.rows_read > native_capacity) {
            return L2FLOW_PARTIAL_ORDER_EVENT_READ_CORRUPT_V2;
        }
        if (native_result.rows_read != 0U) {
            std::memcpy(
                output,
                reader->event_scratch.data(),
                native_result.rows_read * sizeof(*output));
        }
        return static_cast<int>(read);
    } catch (const std::bad_alloc&) {
        return L2FLOW_PARTIAL_ORDER_EVENT_READ_RESOURCE_EXHAUSTED_V2;
    } catch (...) {
        return L2FLOW_PARTIAL_ORDER_EVENT_READ_UNEXPECTED_FAILURE_V2;
    }
}

extern "C" int l2flow_partial_order_event_reader_affected_channels_v2(
    const l2flow_partial_order_event_reader_v2* reader,
    l2flow_partial_order_event_channel_health_v2* output,
    std::size_t capacity,
    l2flow_partial_order_event_channel_batch_result_v2* result) {
    if (result == nullptr) {
        return L2FLOW_PARTIAL_ORDER_EVENT_READ_INVALID_ARGUMENT_V2;
    }
    *result = {};
    result->result_schema_version = kResultSchemaVersion;
    result->result_bytes = sizeof(*result);
    if (!ReaderValid(reader) || (capacity != 0U && output == nullptr)) {
        return L2FLOW_PARTIAL_ORDER_EVENT_READ_INVALID_ARGUMENT_V2;
    }
    try {
        std::lock_guard lock(reader->scratch_mutex);
        const std::size_t native_capacity = static_cast<std::size_t>(
            std::min<std::uint64_t>(
                static_cast<std::uint64_t>(capacity),
                reader->channel_capacity));
        reader->channel_scratch.resize(native_capacity);
        std::size_t rows = 0U;
        ipc::PartialOrderEventStatusSnapshotV2 status{};
        std::span<ipc::PartialOrderEventChannelHealthV2> native_output{};
        if (native_capacity != 0U) {
            native_output = std::span<
                ipc::PartialOrderEventChannelHealthV2>(
                reader->channel_scratch.data(), native_capacity);
        }
        const auto read = reader->reader->ReadAffectedChannels(
            native_output, &rows, &status);
        ipc::PartialEventBrokerResponseV2 live_broker{};
        const int lifecycle_result =
            ReadLiveBrokerStatus(*reader, &live_broker);
        if (lifecycle_result != L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2) {
            return lifecycle_result;
        }
        if (status.cut.commit_sequence != 0U) {
            CopyStatus(status, live_broker, &result->status);
        }
        result->required_capacity = rows;
        if (read == ipc::PartialOrderEventReadResultV2::kOk) {
            result->records_written = rows;
            for (std::size_t index = 0U; index < rows; ++index) {
                CopyChannel(reader->channel_scratch[index], &output[index]);
            }
        }
        return static_cast<int>(read);
    } catch (const std::bad_alloc&) {
        return L2FLOW_PARTIAL_ORDER_EVENT_READ_RESOURCE_EXHAUSTED_V2;
    } catch (...) {
        return L2FLOW_PARTIAL_ORDER_EVENT_READ_UNEXPECTED_FAILURE_V2;
    }
}

extern "C" int l2flow_partial_order_event_reader_find_order_state_v2(
    const l2flow_partial_order_event_reader_v2* reader,
    const l2flow_partial_order_event_order_key_v2* key,
    l2flow_partial_order_event_order_state_v2* output,
    l2flow_partial_order_event_status_v2* output_status) {
    if (!ReaderValid(reader) || key == nullptr || output == nullptr) {
        return L2FLOW_PARTIAL_ORDER_EVENT_READ_INVALID_ARGUMENT_V2;
    }
    *output = {};
    if (output_status != nullptr) {
        *output_status = {};
    }
    ipc::PartialOrderEventOrderKeyV2 native_key{};
    native_key.market = key->market;
    native_key.instrument_id = key->instrument_id;
    native_key.channel = key->channel;
    native_key.order_id = key->order_id;
    ipc::PartialOrderEventOrderStateV2 native_state{};
    ipc::PartialOrderEventStatusSnapshotV2 native_status{};
    const auto read = reader->reader->FindOrderState(
        native_key, &native_state, &native_status);
    ipc::PartialEventBrokerResponseV2 live_broker{};
    const int lifecycle_result =
        ReadLiveBrokerStatus(*reader, &live_broker);
    if (lifecycle_result != L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2) {
        return lifecycle_result;
    }
    if (read == ipc::PartialOrderEventReadResultV2::kOk) {
        std::memcpy(output, &native_state, sizeof(*output));
        if (output_status != nullptr) {
            CopyStatus(
                native_status,
                live_broker,
                output_status);
        }
    }
    return static_cast<int>(read);
}

extern "C" int l2flow_partial_order_event_reader_order_states_v2(
    const l2flow_partial_order_event_reader_v2* reader,
    const l2flow_partial_order_event_checkpoint_v2* checkpoint,
    l2flow_partial_order_event_order_state_v2* output,
    std::size_t capacity,
    l2flow_partial_order_event_order_state_batch_result_v2* result) {
    if (result == nullptr) {
        return L2FLOW_PARTIAL_ORDER_EVENT_READ_INVALID_ARGUMENT_V2;
    }
    *result = {};
    result->result_schema_version = kResultSchemaVersion;
    result->result_bytes = sizeof(*result);
    if (!ReaderValid(reader) || checkpoint == nullptr || output == nullptr ||
        capacity == 0U || !CheckpointCanonical(*checkpoint)) {
        return L2FLOW_PARTIAL_ORDER_EVENT_READ_INVALID_ARGUMENT_V2;
    }
    result->checkpoint = *checkpoint;
    if (!CheckpointMatches(*checkpoint, reader->broker_response)) {
        return
            L2FLOW_PARTIAL_ORDER_EVENT_READ_FULL_REPLACEMENT_REQUIRED_V2;
    }
    const std::uint64_t first_physical_slot =
        checkpoint->next_order_state_physical_slot;
    if (first_physical_slot == reader->order_state_capacity) {
        return ReadStatus(reader, &result->status);
    }
    if (first_physical_slot > reader->order_state_capacity) {
        return L2FLOW_PARTIAL_ORDER_EVENT_READ_INVALID_ARGUMENT_V2;
    }
    try {
        std::lock_guard lock(reader->scratch_mutex);
        const std::size_t native_capacity = static_cast<std::size_t>(
            std::min<std::uint64_t>(
                static_cast<std::uint64_t>(capacity),
                reader->order_state_capacity));
        if (native_capacity == 0U) {
            return L2FLOW_PARTIAL_ORDER_EVENT_READ_INVALID_ARGUMENT_V2;
        }
        reader->state_scratch.resize(native_capacity);
        ipc::PartialOrderEventOrderStateBatchResultV2 native_result{};
        const auto read = reader->reader->ReadOrderStates(
            first_physical_slot,
            std::span<ipc::PartialOrderEventOrderStateV2>(
                reader->state_scratch.data(), native_capacity),
            &native_result);
        ipc::PartialEventBrokerResponseV2 live_broker{};
        const int lifecycle_result =
            ReadLiveBrokerStatus(*reader, &live_broker);
        if (lifecycle_result != L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2) {
            return lifecycle_result;
        }
        result->records_written = native_result.rows_read;
        CopyCheckpoint(
            reader->broker_response,
            checkpoint->next_event_sequence,
            native_result.next_physical_slot,
            &result->checkpoint);
        if (native_result.status.cut.commit_sequence != 0U) {
            CopyStatus(
                native_result.status,
                live_broker,
                &result->status);
        }
        if (native_result.rows_read > native_capacity) {
            return L2FLOW_PARTIAL_ORDER_EVENT_READ_CORRUPT_V2;
        }
        if (native_result.rows_read != 0U) {
            std::memcpy(
                output,
                reader->state_scratch.data(),
                native_result.rows_read * sizeof(*output));
        }
        return static_cast<int>(read);
    } catch (const std::bad_alloc&) {
        return L2FLOW_PARTIAL_ORDER_EVENT_READ_RESOURCE_EXHAUSTED_V2;
    } catch (...) {
        return L2FLOW_PARTIAL_ORDER_EVENT_READ_UNEXPECTED_FAILURE_V2;
    }
}

extern "C" const char*
l2flow_partial_order_event_open_error_name_v2(int error) {
    constexpr std::string_view names[] = {
        "ok",
        "null_output",
        "invalid_argument",
        "broker_unavailable",
        "broker_protocol_error",
        "broker_peer_rejected",
        "broker_session_mismatch",
        "broker_generation_rejected",
        "broker_descriptor_rejected",
        "broker_continuity_rejected",
        "broker_internal_error",
        "descriptor_stat_failed",
        "descriptor_seal_mismatch",
        "mapping_failed",
        "incompatible_layout",
        "mapping_session_mismatch",
        "no_stable_cut",
        "full_replacement_required",
        "resource_exhausted",
        "unexpected_failure",
        "unsupported_ordering_quality",
    };
    if (error < 0 ||
        static_cast<std::size_t>(error) >= std::size(names)) {
        return "unknown";
    }
    return names[static_cast<std::size_t>(error)].data();
}

extern "C" const char*
l2flow_partial_order_event_read_result_name_v2(int result) {
    constexpr std::string_view names[] = {
        "ok",
        "invalid_argument",
        "inconsistent",
        "not_yet_published",
        "output_too_small",
        "not_found",
        "corrupt",
        "full_replacement_required",
        "resource_exhausted",
        "unexpected_failure",
    };
    if (result < 0 ||
        static_cast<std::size_t>(result) >= std::size(names)) {
        return "unknown";
    }
    return names[static_cast<std::size_t>(result)].data();
}
