#pragma once

#include "l2flow/ipc/order_event_delta_wire_v1.h"

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace l2flow::apps {

struct OrderEventAggregatorOptionsV1 final {
    std::filesystem::path source_control_socket;
    std::filesystem::path event_control_socket;
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
    std::size_t maximum_shanghai_order_states = 0U;
    std::size_t maximum_shenzhen_order_states = 0U;
    std::uint64_t event_ring_capacity = 0U;
    std::uint64_t event_maximum_mapping_bytes = 0U;
    std::size_t read_batch_records = 0U;
    std::uint32_t poll_interval_ms = 0U;
    std::uint32_t control_timeout_ms = 0U;
};

enum class OrderEventAggregatorParseResultV1 : std::uint8_t {
    kOk = 0U,
    kHelp,
    kError,
};

[[nodiscard]] inline constexpr std::string_view
OrderEventAggregatorHelpV1() noexcept {
    return
        "Usage: mdl-order-event-aggregator [options]\n"
        "\n"
        "Required options:\n"
        "  --source-socket PATH              Wire V2 GET_SESSION socket\n"
        "  --event-socket PATH               derived-event control socket\n"
        "  --session-epoch N                 expected source/session epoch\n"
        "  --trade-date YYYYMMDD             expected trading day\n"
        "  --shanghai-state-capacity N       maximum Shanghai order states\n"
        "  --shenzhen-state-capacity N       maximum Shenzhen order states\n"
        "  --event-ring-capacity N           derived-event ring row capacity\n"
        "  --event-maximum-mapping-bytes N   hard event mapping byte limit\n"
        "  --read-batch-records N            Wire ticks copied per read\n"
        "  --poll-ms N                       idle poll interval, 1..1000\n"
        "  --timeout-ms N                    control I/O timeout, 1..60000\n"
        "\n"
        "Other:\n"
        "  --help                            print this help and exit\n"
        "\n"
        "The process always starts at global tick sequence 1. It has no\n"
        "skip, overrun catch-up, WAL, recovery, Parquet, or compaction "
        "mode.\n";
}

namespace order_event_aggregator_cli_detail {

[[nodiscard]] inline bool ValidTradeDate(
    std::uint32_t value) noexcept {
    const std::uint32_t year = value / 10'000U;
    const std::uint32_t month = (value / 100U) % 100U;
    const std::uint32_t day = value % 100U;
    if (year < 1992U || year > 2200U || month == 0U ||
        month > 12U || day == 0U) {
        return false;
    }
    constexpr std::uint32_t days_by_month[12]{
        31U, 28U, 31U, 30U, 31U, 30U,
        31U, 31U, 30U, 31U, 30U, 31U};
    std::uint32_t maximum_day = days_by_month[month - 1U];
    const bool leap =
        (year % 4U == 0U && year % 100U != 0U) ||
        year % 400U == 0U;
    if (month == 2U && leap) {
        maximum_day = 29U;
    }
    return day <= maximum_day;
}

[[nodiscard]] inline bool ParseUnsigned(
    std::string_view text,
    std::uint64_t* output) noexcept {
    if (output == nullptr || text.empty()) {
        return false;
    }
    std::uint64_t value = 0U;
    const char* const begin = text.data();
    const char* const end = begin + text.size();
    const auto parsed = std::from_chars(begin, end, value, 10);
    if (parsed.ec != std::errc{} || parsed.ptr != end) {
        return false;
    }
    *output = value;
    return true;
}

[[nodiscard]] inline bool SocketPathValid(
    std::string_view value) {
    // Linux sockaddr_un::sun_path is 108 bytes including the terminator.
    if (value.empty() || value.size() >= 108U ||
        value.find('\0') != std::string_view::npos) {
        return false;
    }
    const std::filesystem::path path(value);
    return path.is_absolute() && !path.filename().empty() &&
           path.filename() != "." && path.filename() != "..";
}

inline void SetError(
    std::string* output,
    std::string_view message) {
    if (output != nullptr) {
        output->assign(message);
    }
}

}  // namespace order_event_aggregator_cli_detail

[[nodiscard]] inline OrderEventAggregatorParseResultV1
ParseOrderEventAggregatorArgumentsV1(
    std::span<const std::string_view> arguments,
    OrderEventAggregatorOptionsV1* output,
    std::string* error_message) noexcept {
    using namespace order_event_aggregator_cli_detail;
    if (output == nullptr || error_message == nullptr) {
        return OrderEventAggregatorParseResultV1::kError;
    }
    *output = {};
    error_message->clear();
    try {
        for (std::string_view argument : arguments) {
            if (argument == "--help") {
                return OrderEventAggregatorParseResultV1::kHelp;
            }
        }

        OrderEventAggregatorOptionsV1 parsed{};
        bool source_socket_seen = false;
        bool event_socket_seen = false;
        bool epoch_seen = false;
        bool date_seen = false;
        bool shanghai_seen = false;
        bool shenzhen_seen = false;
        bool ring_seen = false;
        bool mapping_seen = false;
        bool batch_seen = false;
        bool poll_seen = false;
        bool timeout_seen = false;

        const auto duplicate = [&](bool* seen,
                                   std::string_view name) {
            if (*seen) {
                SetError(
                    error_message,
                    std::string("duplicate option: ") +
                        std::string(name));
                return true;
            }
            *seen = true;
            return false;
        };
        for (std::size_t index = 0U; index < arguments.size();
             ++index) {
            const std::string_view name = arguments[index];
            if (name.empty() || name.front() != '-') {
                SetError(
                    error_message,
                    std::string("unexpected positional argument: ") +
                        std::string(name));
                return OrderEventAggregatorParseResultV1::kError;
            }
            if (index + 1U >= arguments.size()) {
                SetError(
                    error_message,
                    std::string("missing value for ") +
                        std::string(name));
                return OrderEventAggregatorParseResultV1::kError;
            }
            const std::string_view value = arguments[++index];
            std::uint64_t numeric = 0U;

            if (name == "--source-socket") {
                if (duplicate(&source_socket_seen, name) ||
                    !SocketPathValid(value)) {
                    if (error_message->empty()) {
                        SetError(
                            error_message,
                            "--source-socket must be an absolute "
                            "pathname shorter than 108 bytes");
                    }
                    return OrderEventAggregatorParseResultV1::kError;
                }
                parsed.source_control_socket =
                    std::filesystem::path(value).lexically_normal();
            } else if (name == "--event-socket") {
                if (duplicate(&event_socket_seen, name) ||
                    !SocketPathValid(value)) {
                    if (error_message->empty()) {
                        SetError(
                            error_message,
                            "--event-socket must be an absolute "
                            "pathname shorter than 108 bytes");
                    }
                    return OrderEventAggregatorParseResultV1::kError;
                }
                parsed.event_control_socket =
                    std::filesystem::path(value).lexically_normal();
            } else if (name == "--session-epoch") {
                if (duplicate(&epoch_seen, name) ||
                    !ParseUnsigned(value, &numeric) ||
                    numeric == 0U) {
                    if (error_message->empty()) {
                        SetError(
                            error_message,
                            "--session-epoch must be a positive "
                            "uint64");
                    }
                    return OrderEventAggregatorParseResultV1::kError;
                }
                parsed.session_epoch = numeric;
            } else if (name == "--trade-date") {
                if (duplicate(&date_seen, name) ||
                    !ParseUnsigned(value, &numeric) ||
                    numeric >
                        std::numeric_limits<std::uint32_t>::max() ||
                    !ValidTradeDate(
                        static_cast<std::uint32_t>(numeric))) {
                    if (error_message->empty()) {
                        SetError(
                            error_message,
                            "--trade-date must be a valid YYYYMMDD "
                            "date");
                    }
                    return OrderEventAggregatorParseResultV1::kError;
                }
                parsed.trade_date =
                    static_cast<std::uint32_t>(numeric);
            } else if (
                name == "--shanghai-state-capacity" ||
                name == "--shenzhen-state-capacity") {
                bool* const seen =
                    name == "--shanghai-state-capacity"
                        ? &shanghai_seen
                        : &shenzhen_seen;
                if (duplicate(seen, name) ||
                    !ParseUnsigned(value, &numeric) ||
                    numeric == 0U ||
                    numeric >
                        static_cast<std::uint64_t>(
                            std::numeric_limits<std::size_t>::max())) {
                    if (error_message->empty()) {
                        SetError(
                            error_message,
                            std::string(name) +
                                " must be a positive size_t");
                    }
                    return OrderEventAggregatorParseResultV1::kError;
                }
                if (name == "--shanghai-state-capacity") {
                    parsed.maximum_shanghai_order_states =
                        static_cast<std::size_t>(numeric);
                } else {
                    parsed.maximum_shenzhen_order_states =
                        static_cast<std::size_t>(numeric);
                }
            } else if (name == "--event-ring-capacity") {
                if (duplicate(&ring_seen, name) ||
                    !ParseUnsigned(value, &numeric) ||
                    numeric == 0U) {
                    if (error_message->empty()) {
                        SetError(
                            error_message,
                            "--event-ring-capacity must be a "
                            "positive uint64");
                    }
                    return OrderEventAggregatorParseResultV1::kError;
                }
                parsed.event_ring_capacity = numeric;
            } else if (
                name == "--event-maximum-mapping-bytes") {
                if (duplicate(&mapping_seen, name) ||
                    !ParseUnsigned(value, &numeric) ||
                    numeric == 0U) {
                    if (error_message->empty()) {
                        SetError(
                            error_message,
                            "--event-maximum-mapping-bytes must be "
                            "a positive uint64");
                    }
                    return OrderEventAggregatorParseResultV1::kError;
                }
                parsed.event_maximum_mapping_bytes = numeric;
            } else if (name == "--read-batch-records") {
                constexpr std::uint64_t kMaximumBatch = 1'048'576U;
                if (duplicate(&batch_seen, name) ||
                    !ParseUnsigned(value, &numeric) ||
                    numeric == 0U || numeric > kMaximumBatch ||
                    numeric >
                        static_cast<std::uint64_t>(
                            std::numeric_limits<std::size_t>::max())) {
                    if (error_message->empty()) {
                        SetError(
                            error_message,
                            "--read-batch-records must be in "
                            "1..1048576");
                    }
                    return OrderEventAggregatorParseResultV1::kError;
                }
                parsed.read_batch_records =
                    static_cast<std::size_t>(numeric);
            } else if (
                name == "--poll-ms" || name == "--timeout-ms") {
                bool* const seen =
                    name == "--poll-ms" ? &poll_seen : &timeout_seen;
                const std::uint64_t maximum =
                    name == "--poll-ms" ? 1'000U : 60'000U;
                if (duplicate(seen, name) ||
                    !ParseUnsigned(value, &numeric) ||
                    numeric == 0U || numeric > maximum) {
                    if (error_message->empty()) {
                        SetError(
                            error_message,
                            std::string(name) + " must be in 1.." +
                                std::to_string(maximum));
                    }
                    return OrderEventAggregatorParseResultV1::kError;
                }
                if (name == "--poll-ms") {
                    parsed.poll_interval_ms =
                        static_cast<std::uint32_t>(numeric);
                } else {
                    parsed.control_timeout_ms =
                        static_cast<std::uint32_t>(numeric);
                }
            } else {
                SetError(
                    error_message,
                    std::string("unknown option: ") +
                        std::string(name));
                return OrderEventAggregatorParseResultV1::kError;
            }
        }

        if (!source_socket_seen || !event_socket_seen ||
            !epoch_seen || !date_seen || !shanghai_seen ||
            !shenzhen_seen || !ring_seen || !mapping_seen ||
            !batch_seen || !poll_seen || !timeout_seen) {
            SetError(
                error_message,
                "all options listed under Required options must be "
                "provided exactly once");
            return OrderEventAggregatorParseResultV1::kError;
        }
        if (parsed.source_control_socket ==
            parsed.event_control_socket) {
            SetError(
                error_message,
                "source and event control sockets must differ");
            return OrderEventAggregatorParseResultV1::kError;
        }
        constexpr std::uint64_t header_bytes =
            ipc::kOrderEventDeltaHeaderBytesV1;
        constexpr std::uint64_t slot_bytes =
            ipc::kOrderEventDeltaSlotBytesV1;
        const std::uint64_t maximum =
            std::numeric_limits<std::uint64_t>::max();
        if (parsed.event_ring_capacity >
            (maximum - header_bytes) / slot_bytes) {
            SetError(error_message, "event ring layout overflows uint64");
            return OrderEventAggregatorParseResultV1::kError;
        }
        const std::uint64_t minimum_mapping_bytes =
            header_bytes +
            parsed.event_ring_capacity * slot_bytes;
        constexpr std::uint64_t page_bytes = 4'096U;
        if (minimum_mapping_bytes >
            maximum - (page_bytes - 1U)) {
            SetError(error_message, "event ring layout overflows uint64");
            return OrderEventAggregatorParseResultV1::kError;
        }
        const std::uint64_t aligned_mapping_bytes =
            (minimum_mapping_bytes + page_bytes - 1U) &
            ~(page_bytes - 1U);
        if (parsed.event_maximum_mapping_bytes >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max())) {
            SetError(
                error_message,
                "--event-maximum-mapping-bytes exceeds size_t");
            return OrderEventAggregatorParseResultV1::kError;
        }
        if (parsed.event_maximum_mapping_bytes <
            aligned_mapping_bytes) {
            SetError(
                error_message,
                "--event-maximum-mapping-bytes is smaller than "
                "the requested ring layout");
            return OrderEventAggregatorParseResultV1::kError;
        }
        *output = std::move(parsed);
        return OrderEventAggregatorParseResultV1::kOk;
    } catch (...) {
        *output = {};
        SetError(error_message, "argument parsing exhausted resources");
        return OrderEventAggregatorParseResultV1::kError;
    }
}

}  // namespace l2flow::apps
