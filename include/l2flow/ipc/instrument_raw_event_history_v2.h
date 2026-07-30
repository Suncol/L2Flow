#pragma once

#include "l2flow/ipc/realtime_shm_reader_c_v2.h"
#include "l2flow/ipc/realtime_wire_v2.h"

#include <cstddef>
#include <cstdint>
#include <span>
#include <utility>

namespace l2flow::ipc {

// This API exposes the immutable, normalized Wire V2 tick records retained by
// the IntradayInstrumentStore. It is an input/replay interface. It does not
// expose derived or canonical ORDER/TRADE/CANCEL aggregates. Related session
// and cursor objects are not concurrently callable; callers serialize read,
// open, status, reset, and destruction operations.
enum class InstrumentRawEventHistoryErrorV2 : int {
    kNone = L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_OK_V2,
    kInvalidArgument =
        L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_INVALID_ARGUMENT_V2,
    kSystemError =
        L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_SYSTEM_ERROR_V2,
    kProtocolError =
        L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_PROTOCOL_ERROR_V2,
    kAbiMismatch =
        L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_ABI_MISMATCH_V2,
    kUnavailable =
        L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_UNAVAILABLE_V2,
    kNotFound =
        L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_NOT_FOUND_V2,
    kResourceExhausted =
        L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_RESOURCE_EXHAUSTED_V2,
    kCheckpointMismatch =
        L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_CHECKPOINT_MISMATCH_V2,
    kNotReady =
        L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_NOT_READY_V2,
    kClosed = L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_CLOSED_V2,
};

[[nodiscard]] inline const char* InstrumentRawEventHistoryErrorNameV2(
    InstrumentRawEventHistoryErrorV2 error) noexcept {
    return l2flow_instrument_raw_event_history_error_name_v2(
        static_cast<int>(error));
}

using InstrumentRawEventHistoryEndpointV2 =
    l2flow_instrument_raw_event_history_endpoint_v2;
using InstrumentRawEventHistoryCheckpointV2 =
    l2flow_instrument_raw_event_history_checkpoint_v2;
using InstrumentRawEventHistoryMetadataV2 =
    l2flow_instrument_raw_event_history_metadata_v2;

class InstrumentRawEventHistoryPageViewV2 final {
public:
    InstrumentRawEventHistoryPageViewV2() = default;

    // The span is borrowed from one sealed read-only page mapping. It remains
    // valid only until the next ReadPage call on the same cursor or until that
    // cursor is closed/destroyed. Keeping this PageView object does not extend
    // the mapping lifetime.
    [[nodiscard]] std::span<const RealtimeWireTickPayloadV2>
    records() const noexcept {
        if (page_.event_records == nullptr ||
            page_.event_record_stride !=
                sizeof(RealtimeWireTickPayloadV2)) {
            return {};
        }
        return {
            static_cast<const RealtimeWireTickPayloadV2*>(
                page_.event_records),
            page_.event_record_count};
    }

    [[nodiscard]] bool eof() const noexcept {
        return page_.eof != 0U;
    }
    [[nodiscard]] std::uint64_t page_index() const noexcept {
        return page_.page_index;
    }
    [[nodiscard]] std::uint64_t mapping_bytes() const noexcept {
        return page_.mapping_bytes;
    }
    [[nodiscard]] std::uint64_t first_ingress_sequence()
        const noexcept {
        return page_.first_ingress_sequence;
    }
    [[nodiscard]] std::uint64_t last_ingress_sequence()
        const noexcept {
        return page_.last_ingress_sequence;
    }
    [[nodiscard]] std::uint64_t first_tick_stream_sequence()
        const noexcept {
        return page_.first_tick_stream_sequence;
    }
    [[nodiscard]] std::uint64_t last_tick_stream_sequence()
        const noexcept {
        return page_.last_tick_stream_sequence;
    }
    [[nodiscard]] std::uint64_t cumulative_record_count()
        const noexcept {
        return page_.cumulative_record_count;
    }
    [[nodiscard]] std::span<const std::uint64_t, 4U>
    cumulative_source_record_counts() const noexcept {
        return {page_.cumulative_source_record_counts};
    }

private:
    l2flow_instrument_raw_event_history_page_v2 page_{};

    friend class InstrumentRawEventHistoryCursorV2;
};

class InstrumentRawEventHistoryCursorV2 final {
public:
    InstrumentRawEventHistoryCursorV2() = default;
    InstrumentRawEventHistoryCursorV2(
        const InstrumentRawEventHistoryCursorV2&) = delete;
    InstrumentRawEventHistoryCursorV2& operator=(
        const InstrumentRawEventHistoryCursorV2&) = delete;

    InstrumentRawEventHistoryCursorV2(
        InstrumentRawEventHistoryCursorV2&& other) noexcept
        : cursor_(std::exchange(other.cursor_, nullptr)) {}

    InstrumentRawEventHistoryCursorV2& operator=(
        InstrumentRawEventHistoryCursorV2&& other) noexcept {
        if (this != &other) {
            Reset();
            cursor_ = std::exchange(other.cursor_, nullptr);
        }
        return *this;
    }

    ~InstrumentRawEventHistoryCursorV2() {
        Reset();
    }

    [[nodiscard]] bool is_open() const noexcept {
        l2flow_instrument_raw_event_history_metadata_v2 metadata{};
        return cursor_ != nullptr &&
               l2flow_instrument_raw_event_history_cursor_metadata_v2(
                   cursor_, &metadata) ==
                   L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_OK_V2;
    }

    void Reset() noexcept {
        l2flow_instrument_raw_event_history_cursor_close_v2(cursor_);
        cursor_ = nullptr;
    }

    [[nodiscard]] InstrumentRawEventHistoryErrorV2 Metadata(
        InstrumentRawEventHistoryMetadataV2* output) const noexcept {
        return static_cast<InstrumentRawEventHistoryErrorV2>(
            l2flow_instrument_raw_event_history_cursor_metadata_v2(
                cursor_, output));
    }

    // A successful non-EOF page contains one borrowed contiguous span of raw
    // normalized Wire tick records. EOF is explicit and has an empty span.
    [[nodiscard]] InstrumentRawEventHistoryErrorV2 ReadPage(
        InstrumentRawEventHistoryPageViewV2* output) noexcept {
        if (output == nullptr) {
            return InstrumentRawEventHistoryErrorV2::
                kInvalidArgument;
        }
        return static_cast<InstrumentRawEventHistoryErrorV2>(
            l2flow_instrument_raw_event_history_cursor_read_v2(
                cursor_, &output->page_));
    }

    // A target checkpoint becomes trustworthy only after ReadPage returned
    // the explicit EOF page and reconciled all advertised source counts.
    [[nodiscard]] InstrumentRawEventHistoryErrorV2
    VerifiedCheckpoint(
        InstrumentRawEventHistoryCheckpointV2* output)
        const noexcept {
        return static_cast<InstrumentRawEventHistoryErrorV2>(
            l2flow_instrument_raw_event_history_cursor_verified_checkpoint_v2(
                cursor_, output));
    }

private:
    l2flow_instrument_raw_event_history_cursor_v2* cursor_ =
        nullptr;

    friend class InstrumentRawEventHistorySessionV2;
};

class InstrumentRawEventHistorySessionV2 final {
public:
    InstrumentRawEventHistorySessionV2() = default;
    InstrumentRawEventHistorySessionV2(
        const InstrumentRawEventHistorySessionV2&) = delete;
    InstrumentRawEventHistorySessionV2& operator=(
        const InstrumentRawEventHistorySessionV2&) = delete;

    InstrumentRawEventHistorySessionV2(
        InstrumentRawEventHistorySessionV2&& other) noexcept
        : session_(std::exchange(other.session_, nullptr)) {}

    InstrumentRawEventHistorySessionV2& operator=(
        InstrumentRawEventHistorySessionV2&& other) noexcept {
        if (this != &other) {
            Reset();
            session_ = std::exchange(other.session_, nullptr);
        }
        return *this;
    }

    ~InstrumentRawEventHistorySessionV2() {
        Reset();
    }

    // expected_session is obtained from l2flow_shm_reader_session_v2 and
    // binds the control peer to the already-open shared-memory identity.
    // expected_generation==0 pins the latest immutable Store generation.
    [[nodiscard]] static InstrumentRawEventHistoryErrorV2 Open(
        const char* absolute_control_socket_path,
        const l2flow_shm_session_info_v2& expected_session,
        std::uint64_t expected_generation,
        std::uint32_t timeout_ms,
        InstrumentRawEventHistorySessionV2* output) noexcept {
        if (output == nullptr || output->session_ != nullptr) {
            return InstrumentRawEventHistoryErrorV2::
                kInvalidArgument;
        }
        return static_cast<InstrumentRawEventHistoryErrorV2>(
            l2flow_instrument_raw_event_history_session_open_v2(
                absolute_control_socket_path,
                &expected_session,
                expected_generation,
                timeout_ms,
                &output->session_));
    }

    [[nodiscard]] bool is_open() const noexcept {
        l2flow_instrument_raw_event_history_endpoint_v2 endpoint{};
        return session_ != nullptr &&
               l2flow_instrument_raw_event_history_session_target_v2(
                   session_, &endpoint) ==
                   L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_OK_V2;
    }

    void Reset() noexcept {
        l2flow_instrument_raw_event_history_session_close_v2(
            session_);
        session_ = nullptr;
    }

    [[nodiscard]] InstrumentRawEventHistoryErrorV2 Target(
        InstrumentRawEventHistoryEndpointV2* output) const noexcept {
        return static_cast<InstrumentRawEventHistoryErrorV2>(
            l2flow_instrument_raw_event_history_session_target_v2(
                session_, output));
    }

    // Reads every retained raw normalized tick for the instrument from the
    // retained session origin through this session's pinned target.
    [[nodiscard]] InstrumentRawEventHistoryErrorV2 OpenFull(
        std::uint32_t instrument_id,
        std::uint32_t requested_page_records,
        InstrumentRawEventHistoryCursorV2* output) noexcept {
        if (output == nullptr || output->cursor_ != nullptr) {
            return InstrumentRawEventHistoryErrorV2::
                kInvalidArgument;
        }
        return static_cast<InstrumentRawEventHistoryErrorV2>(
            l2flow_instrument_raw_event_history_open_full_v2(
                session_,
                instrument_id,
                requested_page_records,
                &output->cursor_));
    }

    // Reads the exact half-open suffix after a verified predecessor
    // checkpoint through this session's pinned target generation.
    [[nodiscard]] InstrumentRawEventHistoryErrorV2 OpenUpdate(
        std::uint32_t instrument_id,
        std::uint32_t requested_page_records,
        const InstrumentRawEventHistoryCheckpointV2&
            base_checkpoint,
        InstrumentRawEventHistoryCursorV2* output) noexcept {
        if (output == nullptr || output->cursor_ != nullptr) {
            return InstrumentRawEventHistoryErrorV2::
                kInvalidArgument;
        }
        return static_cast<InstrumentRawEventHistoryErrorV2>(
            l2flow_instrument_raw_event_history_open_update_v2(
                session_,
                instrument_id,
                requested_page_records,
                &base_checkpoint,
                &output->cursor_));
    }

private:
    l2flow_instrument_raw_event_history_session_v2* session_ =
        nullptr;
};

}  // namespace l2flow::ipc
