#include "l2flow/common/identity128.h"
#include "l2flow/common/linux_thread_affinity_v1.h"
#include "l2flow/common/sha256.h"
#if !defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V2)
#error "mdl-production-router requires Linux realtime IPC Wire V2"
#endif
#include "l2flow/ipc/realtime_certified_service_v1.h"
#include "l2flow/ipc/realtime_shared_service_v2.h"
#include "l2flow/ipc/realtime_wire_v2.h"
#include "l2flow/market/daily_instrument_catalog_loader_v2.h"
#include "l2flow/market/instrument_runtime_state_v2.h"
#include "l2flow/recovery/startup_replay_v1.h"
#include "l2flow/recovery/live_journal_v1.h"
#include "l2flow/recovery/online_recovery_v1.h"
#include "l2flow/runtime/realtime_pipeline_v1.h"

#include "event_cpu_partition_v1.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <csignal>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <unistd.h>
#include <time.h>

namespace {

namespace common = l2flow::common;
namespace app = l2flow::apps;
namespace factor = l2flow::factor;
namespace ipc = l2flow::ipc;
namespace market = l2flow::market;
namespace recovery = l2flow::recovery;
namespace runtime = l2flow::runtime;

static_assert(market::kRealtimeHistorySourceCountV1 == 4U);

constexpr std::size_t kOnlineRecoveryOverlapRetentionPerTuple = 262'144U;
constexpr std::uint64_t
    kOnlineRecoveryPreviewOutstandingHighWaterRecords = 64U;
constexpr auto kOnlineRecoveryPreviewDrainPollInterval =
    std::chrono::microseconds(50);

volatile std::sig_atomic_t g_stop_requested = 0;

extern "C" void HandleStopSignal(int) noexcept {
    g_stop_requested = 1;
}

bool InstallStopSignalHandlers(int* error_number) noexcept {
    struct sigaction action {};
    action.sa_handler = &HandleStopSignal;
    action.sa_flags = 0;
    if (::sigemptyset(&action.sa_mask) != 0 ||
        ::sigaction(SIGINT, &action, nullptr) != 0 ||
        ::sigaction(SIGTERM, &action, nullptr) != 0) {
        if (error_number != nullptr) {
            *error_number = errno;
        }
        return false;
    }
    return true;
}

bool CurrentFixedUtc8TradeDate(std::uint32_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    const auto shifted =
        std::chrono::system_clock::now() + std::chrono::hours(8);
    const std::chrono::year_month_day calendar(
        std::chrono::floor<std::chrono::days>(shifted));
    if (!calendar.ok()) {
        return false;
    }
    const int year = static_cast<int>(calendar.year());
    const unsigned int month =
        static_cast<unsigned int>(calendar.month());
    const unsigned int day = static_cast<unsigned int>(calendar.day());
    if (year <= 0 || year > 9999 || month == 0U || day == 0U) {
        return false;
    }
    *output = static_cast<std::uint32_t>(year) * 10'000U +
              static_cast<std::uint32_t>(month) * 100U +
              static_cast<std::uint32_t>(day);
    return true;
}

bool IsValidTradeDate(std::uint32_t value) noexcept {
    const std::uint32_t year_value = value / 10'000U;
    const std::uint32_t month_value = (value / 100U) % 100U;
    const std::uint32_t day_value = value % 100U;
    if (year_value == 0U || year_value > 9999U) {
        return false;
    }
    const std::chrono::year_month_day date{
        std::chrono::year(static_cast<int>(year_value)),
        std::chrono::month(static_cast<unsigned int>(month_value)),
        std::chrono::day(static_cast<unsigned int>(day_value))};
    return date.ok();
}

enum class IntervalWaitResult : std::uint8_t {
    kElapsed = 0U,
    kSignal,
    kTradeDateBoundary,
    kClockFailure,
};

IntervalWaitResult WaitForInterval(
    std::chrono::milliseconds interval,
    std::uint32_t expected_trade_date) {
    const auto deadline = std::chrono::steady_clock::now() + interval;
    constexpr auto maximum_sleep = std::chrono::milliseconds(100);
    for (;;) {
        if (g_stop_requested != 0) {
            return IntervalWaitResult::kSignal;
        }
        std::uint32_t current_trade_date = 0U;
        if (!CurrentFixedUtc8TradeDate(&current_trade_date)) {
            return IntervalWaitResult::kClockFailure;
        }
        if (current_trade_date != expected_trade_date) {
            return IntervalWaitResult::kTradeDateBoundary;
        }
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            return IntervalWaitResult::kElapsed;
        }
        const auto remaining = deadline - now;
        const auto maximum = std::chrono::duration_cast<
            std::chrono::steady_clock::duration>(maximum_sleep);
        std::this_thread::sleep_for(std::min(remaining, maximum));
    }
}

struct Options final {
    std::filesystem::path sdk_library;
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
    std::filesystem::path daily_catalog;
    std::uint64_t catalog_version = 0U;
    std::string server_address;
    std::string user_name;
    std::string sdk_log_prefix = "l2flow-realtime";
    std::uint32_t instrument_store_workers = 4U;
    // Zero keeps the existing one-decoder-owner-per-source path.
    // Operators may enable the bounded stateless parse farm after validating
    // the exact deployment's callback-to-reader latency and CPU placement.
    std::uint32_t parallel_decoder_workers = 0U;
    std::uint64_t decoder_queue_records_per_source = 65'536U;
    std::uint64_t store_queue_records_per_source_worker = 32'768U;
    std::uint64_t certified_handoff_queue_records = 4'194'304U;
    std::uint64_t intraday_store_maximum_records = 0U;
    std::uint64_t intraday_store_memory_bytes = 0U;
    std::uint32_t intraday_store_segment_kib = 64U;
    std::uint32_t intraday_store_batch_records = 64U * 1024U;
    bool intraday_store_from_open = false;
    // Explicit process-start-only service for a mid-session launch that does
    // not reconstruct the market-open prefix.  This is intentionally not a
    // coverage source. It exposes LIVE_PARTIAL latest reads plus immutable
    // process-start History/tick-delta generations without an open-prefix
    // claim.
    bool intraday_live_partial = false;
    std::filesystem::path intraday_recovery_csv_dir;
    bool intraday_recovery_mode_set = false;
    std::filesystem::path intraday_recovery_journal_dir;
    std::filesystem::path live_preview_ipc_socket;
    std::uint64_t intraday_recovery_journal_maximum_bytes =
        512ULL * 1024ULL * 1024ULL * 1024ULL;
    std::uint64_t intraday_recovery_journal_segment_bytes =
        256ULL * 1024ULL * 1024ULL;
    std::uint64_t intraday_recovery_journal_queue_records = 65'536U;
    std::uint32_t
        intraday_recovery_certified_high_watermark_percent = 75U;
    std::uint32_t intraday_recovery_boundary_alignment_ms = 10'000U;
    std::uint32_t intraday_recovery_progress_interval_seconds = 10U;
    std::uint32_t intraday_recovery_warmup_seconds = 30U * 60U;
    std::uint32_t intraday_recovery_backpressure_seconds = 30U;
    bool intraday_store_maximum_records_set = false;
    bool intraday_store_memory_set = false;
    bool intraday_recovery_tuning_set = false;
    bool intraday_recovery_online_tuning_set = false;

    // Each duration in milliseconds is also its stable public window_id.
    std::vector<std::uint32_t> kline_windows_ms;
    std::uint32_t generation_interval_ms = 1000U;
    std::uint32_t generation_timeout_ms = 10'000U;

    std::filesystem::path ipc_socket;
    // Optional strict logical-CPU partition. When set, the router's current
    // allowed mask is split into disjoint FAST=complement and Event=requested
    // masks before any long-lived service or Pipeline thread is created.
    std::string event_cpu_set;
    std::uint64_t ipc_tick_ring_records = 262'144U;
    std::uint64_t ipc_key_arena_bytes =
        16ULL * 1024ULL * 1024ULL;
    std::uint64_t ipc_maximum_mapping_bytes =
        2ULL * 1024ULL * 1024ULL * 1024ULL;
    // Native-gap recovery and its independent CERTIFIED service are enabled
    // by default. The disable option runs only the required FAST service.
    bool native_gap_recovery_enabled = true;
    std::uint64_t native_maximum_backward_displacement = 0U;
    bool native_maximum_backward_displacement_set = false;
    std::filesystem::path certified_ipc_socket;
    bool certified_ipc_socket_set = false;
};

void PrintUsage(std::ostream& output) {
    output
        << "Usage: mdl-production-router [required options] [optional]\n"
        << "Required:\n"
        << "  --sdk-library PATH            vendor .so selected by operator\n"
        << "  --session-epoch N             positive u64 session identity\n"
        << "  --trade-date YYYYMMDD         fixed UTC+08:00 trading date\n"
        << "  --daily-catalog PATH          absolute strict V2 catalog file\n"
        << "  --catalog-version N           positive u64 source version\n"
        << "  --server-address HOST:PORT    vendor endpoint\n"
        << "  --user-name VALUE             nonempty vendor user/token field\n"
        << "  --ipc-socket PATH             absolute GET_SESSION UDS path; "
           "recovered FAST online or standalone partial endpoint\n"
        << "  --intraday-store-max-records N\n"
        << "                                positive u64 session record cap\n"
        << "  --intraday-store-memory-gib N positive u64 logical total GiB cap\n"
        << "  exactly one startup mode:\n"
        << "    --intraday-store-from-open  assert this process captured "
           "from market open\n"
        << "    --intraday-recovery-csv-dir PATH\n"
        << "                                absolute same-day vendor CSV "
           "directory complete from open\n"
        << "    --intraday-live-partial    mid-session FAST plus bounded "
           "canonical Event; no from-open claim\n"
        << "  optional explicit CSV recovery selector:\n"
        << "    --intraday-recovery-mode MODE\n"
        << "                                online (the only supported mode)\n"
        << "  CSV recovery additionally requires:\n"
        << "    --live-preview-ipc-socket PATH\n"
        << "                                LIVE_PARTIAL latest-value socket\n"
        << "    --intraday-recovery-journal-dir PATH\n"
        << "                                empty local append-only WAL directory\n"
        << "  optional online recovery tuning:\n"
        << "    --intraday-recovery-journal-max-gib N\n"
        << "                                positive u64, default 512\n"
        << "    --intraday-recovery-journal-segment-mib N\n"
        << "                                1..4096, default 256\n"
        << "    --intraday-recovery-journal-queue-records N\n"
        << "                                1..4194304, default 65536\n"
        << "    --intraday-recovery-certified-high-watermark-percent N\n"
        << "                                51..89, default 75 (pause is 90)\n"
        << "    --intraday-recovery-boundary-alignment-ms N\n"
        << "                                1..60000, default 10000\n"
        << "    --intraday-recovery-progress-interval-seconds N\n"
        << "                                1..3600, default 10; structured "
           "cold-path progress log\n"
        << "Optional:\n"
        << "  --intraday-recovery-warmup-seconds N\n"
        << "                                1..86400, default 1800\n"
        << "  --intraday-recovery-backpressure-seconds N\n"
        << "                                1..86400, default 30\n"
        << "  --sdk-log-prefix PATH         default l2flow-realtime\n"
        << "  --instrument-store-workers N  1..256, default 4\n"
        << "  --parallel-decoder-workers N  0..64, default 0; 0 keeps "
           "the ordered legacy path\n"
        << "  --decoder-queue-records-per-source N\n"
        << "                                positive u64, default 65536\n"
        << "  --store-queue-records-per-source-worker N\n"
        << "                                positive u64, default 32768\n"
        << "  --certified-handoff-queue-records N\n"
        << "                                power of two, default 4194304\n"
        << "  --intraday-store-segment-kib N\n"
        << "                                4..16384, default 64\n"
        << "  --intraday-store-batch-records N\n"
        << "                                1..1048576, default 65536\n"
        << "  --kline-windows-ms LIST       comma-separated unique durations "
           "in 1..86400000;\n"
        << "                                duration-ms is the window id; "
           "partial mode publishes\n"
        << "                                process-start latest KLine IPC in "
           "natural exchange windows;\n"
        << "                                published bars strictly spanning "
           "the boundary are\n"
        << "                                left-truncated and not full-day\n"
        << "  --generation-interval-ms N    1..60000, default 1000\n"
        << "  --generation-timeout-ms N     1..600000, default 10000\n"
        << "  --ipc-tick-ring-records N     positive u64, default 262144\n"
        << "  --ipc-key-arena-mib N         positive u64, default 16\n"
        << "  --ipc-max-mapping-mib N       positive u64, default 2048\n"
        << "  --certified-ipc-socket PATH   optional absolute CERTIFIED "
           "sidecar UDS;\n"
        << "                                default <ipc-socket>.certified\n"
        << "  --disable-native-gap-recovery disable the default CERTIFIED "
           "recovery sidecar;\n"
        << "                                FAST Wire V2 remains unchanged\n"
        << "  --native-maximum-backward-displacement N\n"
        << "                                required for partial canonical "
           "Event; declared per-channel callback disorder bound\n"
        << "  --event-cpu-set LIST          optional strict logical CPU set for "
           "canonical Event work;\n"
        << "                                router/FAST inherits the nonempty "
           "allowed complement\n"
        << "  --help\n\n"
        << "The catalog must declare complete Shanghai+Shenzhen A-share "
           "subscription coverage for the exact trade date. Identity is "
           "frozen before IPC becomes ACTIVE and SDK Connect; catalog misses "
           "and capacity failures are fatal.\n";
}

bool ParseU32(std::string_view text, std::uint32_t* output) noexcept {
    if (output == nullptr || text.empty()) {
        return false;
    }
    std::uint32_t value = 0U;
    const std::from_chars_result parsed = std::from_chars(
        text.data(), text.data() + text.size(), value, 10);
    if (parsed.ec != std::errc{} ||
        parsed.ptr != text.data() + text.size()) {
        return false;
    }
    *output = value;
    return true;
}

bool ParseU64(std::string_view text, std::uint64_t* output) noexcept {
    if (output == nullptr || text.empty()) {
        return false;
    }
    std::uint64_t value = 0U;
    const std::from_chars_result parsed = std::from_chars(
        text.data(), text.data() + text.size(), value, 10);
    if (parsed.ec != std::errc{} ||
        parsed.ptr != text.data() + text.size()) {
        return false;
    }
    *output = value;
    return true;
}

bool ParsePositiveScaledBytes(
    std::string_view text,
    std::uint64_t scale,
    std::uint64_t* output) noexcept {
    std::uint64_t units = 0U;
    if (output == nullptr || scale == 0U ||
        !ParseU64(text, &units) || units == 0U ||
        units > std::numeric_limits<std::uint64_t>::max() / scale) {
        return false;
    }
    *output = units * scale;
    return true;
}

bool ParseKLineWindows(
    std::string_view text,
    std::vector<std::uint32_t>* output,
    std::string* error) {
    if (output == nullptr || error == nullptr || text.empty()) {
        return false;
    }
    std::vector<std::uint32_t> parsed;
    std::size_t begin = 0U;
    while (begin < text.size()) {
        const std::size_t comma = text.find(',', begin);
        const std::size_t end =
            comma == std::string_view::npos ? text.size() : comma;
        std::uint32_t duration_ms = 0U;
        if (end == begin ||
            !ParseU32(
                text.substr(begin, end - begin), &duration_ms) ||
            duration_ms == 0U || duration_ms > 86'400'000U ||
            std::find(
                parsed.begin(), parsed.end(), duration_ms) !=
                parsed.end()) {
            *error =
                "--kline-windows-ms requires unique comma-separated "
                "durations in 1..86400000";
            return false;
        }
        parsed.push_back(duration_ms);
        if (parsed.size() > market::kKLineMaximumWindowsV1) {
            *error = "--kline-windows-ms accepts at most 32 windows";
            return false;
        }
        if (comma == std::string_view::npos) {
            break;
        }
        if (comma + 1U == text.size()) {
            *error =
                "--kline-windows-ms must not end with a comma";
            return false;
        }
        begin = comma + 1U;
    }
    *output = std::move(parsed);
    return true;
}

bool TakeValue(
    int argc,
    char* argv[],
    int* index,
    std::string_view option,
    std::string_view* output,
    std::string* error) {
    if (index == nullptr || output == nullptr || error == nullptr ||
        *index + 1 >= argc || argv[*index + 1] == nullptr) {
        if (error != nullptr) {
            *error = std::string(option) + " requires a value";
        }
        return false;
    }
    *output = argv[++*index];
    if (output->empty()) {
        *error = std::string(option) + " rejects an empty value";
        return false;
    }
    return true;
}

bool ParseOptions(
    int argc,
    char* argv[],
    Options* output,
    bool* help,
    std::string* error) {
    if (output == nullptr || help == nullptr || error == nullptr) {
        return false;
    }
    *help = false;
    Options parsed{};
    std::set<std::string_view> seen;
    for (int index = 1; index < argc; ++index) {
        if (argv[index] == nullptr) {
            *error = "argv contains null";
            return false;
        }
        const std::string_view option(argv[index]);
        if (option == "--help") {
            if (argc != 2) {
                *error = "--help must be used alone";
                return false;
            }
            *help = true;
            return true;
        }
        if (option == "--intraday-store-from-open") {
            if (!seen.insert(option).second) {
                *error = "duplicate option: " + std::string(option);
                return false;
            }
            parsed.intraday_store_from_open = true;
            continue;
        }
        if (option == "--intraday-live-partial") {
            if (!seen.insert(option).second) {
                *error = "duplicate option: " + std::string(option);
                return false;
            }
            parsed.intraday_live_partial = true;
            continue;
        }
        if (option == "--disable-native-gap-recovery") {
            if (!seen.insert(option).second) {
                *error = "duplicate option: " + std::string(option);
                return false;
            }
            parsed.native_gap_recovery_enabled = false;
            continue;
        }

        if (option != "--sdk-library" &&
            option != "--session-epoch" &&
            option != "--trade-date" &&
            option != "--daily-catalog" &&
            option != "--catalog-version" &&
            option != "--server-address" &&
            option != "--user-name" &&
            option != "--sdk-log-prefix" &&
            option != "--instrument-store-workers" &&
            option != "--parallel-decoder-workers" &&
            option != "--decoder-queue-records-per-source" &&
            option != "--store-queue-records-per-source-worker" &&
            option != "--certified-handoff-queue-records" &&
            option != "--intraday-store-max-records" &&
            option != "--intraday-store-memory-gib" &&
            option != "--intraday-store-segment-kib" &&
            option != "--intraday-store-batch-records" &&
            option != "--intraday-recovery-csv-dir" &&
            option != "--intraday-recovery-mode" &&
            option != "--intraday-recovery-journal-dir" &&
            option != "--intraday-recovery-journal-max-gib" &&
            option != "--intraday-recovery-journal-segment-mib" &&
            option != "--intraday-recovery-journal-queue-records" &&
            option !=
                "--intraday-recovery-certified-high-watermark-percent" &&
            option !=
                "--intraday-recovery-boundary-alignment-ms" &&
            option !=
                "--intraday-recovery-progress-interval-seconds" &&
            option != "--intraday-recovery-warmup-seconds" &&
            option != "--intraday-recovery-backpressure-seconds" &&
            option != "--kline-windows-ms" &&
            option != "--generation-interval-ms" &&
            option != "--generation-timeout-ms" &&
            option != "--ipc-socket" &&
            option != "--live-preview-ipc-socket" &&
            option != "--event-cpu-set" &&
            option != "--ipc-tick-ring-records" &&
            option != "--ipc-key-arena-mib" &&
            option != "--ipc-max-mapping-mib" &&
            option != "--native-maximum-backward-displacement" &&
            option != "--certified-ipc-socket") {
            *error = "unknown option: " + std::string(option);
            return false;
        }
        if (!seen.insert(option).second) {
            *error = "duplicate option: " + std::string(option);
            return false;
        }

        std::string_view value;
        if (!TakeValue(
                argc, argv, &index, option, &value, error)) {
            return false;
        }
        if (option == "--sdk-library") {
            parsed.sdk_library = std::string(value);
        } else if (option == "--session-epoch") {
            if (!ParseU64(value, &parsed.session_epoch) ||
                parsed.session_epoch == 0U) {
                *error = "--session-epoch must be positive u64";
                return false;
            }
        } else if (option == "--trade-date") {
            if (value.size() != 8U ||
                !ParseU32(value, &parsed.trade_date) ||
                !IsValidTradeDate(parsed.trade_date)) {
                *error = "--trade-date must be a valid YYYYMMDD date";
                return false;
            }
        } else if (option == "--daily-catalog") {
            parsed.daily_catalog = std::string(value);
        } else if (option == "--catalog-version") {
            if (!ParseU64(value, &parsed.catalog_version) ||
                parsed.catalog_version == 0U) {
                *error = "--catalog-version must be positive u64";
                return false;
            }
        } else if (option == "--server-address") {
            parsed.server_address = value;
        } else if (option == "--user-name") {
            parsed.user_name = value;
        } else if (option == "--sdk-log-prefix") {
            parsed.sdk_log_prefix = value;
        } else if (option == "--instrument-store-workers") {
            if (!ParseU32(
                    value, &parsed.instrument_store_workers) ||
                parsed.instrument_store_workers == 0U ||
                parsed.instrument_store_workers > 256U) {
                *error =
                    "--instrument-store-workers must be 1..256";
                return false;
            }
        } else if (option == "--parallel-decoder-workers") {
            if (!ParseU32(
                    value, &parsed.parallel_decoder_workers) ||
                parsed.parallel_decoder_workers >
                    runtime::kRealtimeParallelDecoderMaximumWorkersV1) {
                *error =
                    "--parallel-decoder-workers must be 0..64";
                return false;
            }
        } else if (option ==
                   "--decoder-queue-records-per-source") {
            if (!ParseU64(
                    value,
                    &parsed.decoder_queue_records_per_source) ||
                parsed.decoder_queue_records_per_source == 0U ||
                parsed.decoder_queue_records_per_source >
                    std::numeric_limits<std::size_t>::max()) {
                *error =
                    "--decoder-queue-records-per-source must fit a "
                    "positive size_t";
                return false;
            }
        } else if (option ==
                   "--store-queue-records-per-source-worker") {
            if (!ParseU64(
                    value,
                    &parsed.store_queue_records_per_source_worker) ||
                parsed.store_queue_records_per_source_worker == 0U ||
                parsed.store_queue_records_per_source_worker >
                    std::numeric_limits<std::size_t>::max()) {
                *error =
                    "--store-queue-records-per-source-worker must fit "
                    "a positive size_t";
                return false;
            }
        } else if (option ==
                   "--certified-handoff-queue-records") {
            if (!ParseU64(
                    value,
                    &parsed.certified_handoff_queue_records) ||
                parsed.certified_handoff_queue_records == 0U ||
                (parsed.certified_handoff_queue_records &
                 (parsed.certified_handoff_queue_records - 1U)) != 0U ||
                parsed.certified_handoff_queue_records >
                    std::numeric_limits<std::size_t>::max()) {
                *error =
                    "--certified-handoff-queue-records must be a "
                    "power of two that fits size_t";
                return false;
            }
        } else if (option == "--intraday-store-max-records") {
            if (!ParseU64(
                    value,
                    &parsed.intraday_store_maximum_records) ||
                parsed.intraday_store_maximum_records == 0U) {
                *error =
                    "--intraday-store-max-records must be positive u64";
                return false;
            }
            parsed.intraday_store_maximum_records_set = true;
        } else if (option == "--intraday-store-memory-gib") {
            constexpr std::uint64_t bytes_per_gib =
                std::uint64_t{1024U} * 1024U * 1024U;
            if (!ParsePositiveScaledBytes(
                    value,
                    bytes_per_gib,
                    &parsed.intraday_store_memory_bytes)) {
                *error =
                    "--intraday-store-memory-gib must be a positive u64 "
                    "whose byte conversion does not overflow";
                return false;
            }
            parsed.intraday_store_memory_set = true;
        } else if (option == "--intraday-store-segment-kib") {
            if (!ParseU32(
                    value, &parsed.intraday_store_segment_kib) ||
                parsed.intraday_store_segment_kib <
                    market::
                        kIntradayInstrumentStoreMinimumSegmentBytesV1 /
                        1024U ||
                parsed.intraday_store_segment_kib >
                    market::
                        kIntradayInstrumentStoreMaximumSegmentBytesV1 /
                        1024U) {
                *error =
                    "--intraday-store-segment-kib must be 4..16384";
                return false;
            }
        } else if (option == "--intraday-store-batch-records") {
            if (!ParseU32(
                    value,
                    &parsed.intraday_store_batch_records) ||
                parsed.intraday_store_batch_records == 0U ||
                parsed.intraday_store_batch_records >
                    market::
                        kIntradayInstrumentStoreMaximumBatchRecordsV1) {
                *error =
                    "--intraday-store-batch-records must be 1..1048576";
                return false;
            }
        } else if (option == "--intraday-recovery-csv-dir") {
            parsed.intraday_recovery_csv_dir =
                std::string(value);
        } else if (option == "--intraday-recovery-mode") {
            if (value != "online") {
                *error =
                    "--intraday-recovery-mode must be online; "
                    "blocking CSV recovery is no longer supported";
                return false;
            }
            parsed.intraday_recovery_mode_set = true;
        } else if (option == "--intraday-recovery-journal-dir") {
            parsed.intraday_recovery_journal_dir =
                std::string(value);
            parsed.intraday_recovery_online_tuning_set = true;
        } else if (
            option == "--intraday-recovery-journal-max-gib") {
            constexpr std::uint64_t bytes_per_gib =
                std::uint64_t{1024U} * 1024U * 1024U;
            if (!ParsePositiveScaledBytes(
                    value,
                    bytes_per_gib,
                    &parsed
                         .intraday_recovery_journal_maximum_bytes)) {
                *error =
                    "--intraday-recovery-journal-max-gib must be a "
                    "positive u64 whose byte conversion does not overflow";
                return false;
            }
            parsed.intraday_recovery_online_tuning_set = true;
        } else if (
            option == "--intraday-recovery-journal-segment-mib") {
            constexpr std::uint64_t bytes_per_mib =
                std::uint64_t{1024U} * 1024U;
            if (!ParsePositiveScaledBytes(
                    value,
                    bytes_per_mib,
                    &parsed
                         .intraday_recovery_journal_segment_bytes) ||
                parsed.intraday_recovery_journal_segment_bytes <
                    recovery::kLiveJournalMinimumSegmentBytesV1 ||
                parsed.intraday_recovery_journal_segment_bytes >
                    recovery::kLiveJournalMaximumSegmentBytesV1) {
                *error =
                    "--intraday-recovery-journal-segment-mib must be "
                    "1..4096";
                return false;
            }
            parsed.intraday_recovery_online_tuning_set = true;
        } else if (
            option == "--intraday-recovery-journal-queue-records") {
            if (!ParseU64(
                    value,
                    &parsed
                         .intraday_recovery_journal_queue_records) ||
                parsed.intraday_recovery_journal_queue_records == 0U ||
                parsed.intraday_recovery_journal_queue_records >
                    recovery::kLiveJournalMaximumQueueRecordsV1) {
                *error =
                    "--intraday-recovery-journal-queue-records must be "
                    "1..4194304";
                return false;
            }
            parsed.intraday_recovery_online_tuning_set = true;
        } else if (
            option ==
            "--intraday-recovery-certified-high-watermark-percent") {
            if (!ParseU32(
                    value,
                    &parsed
                         .intraday_recovery_certified_high_watermark_percent) ||
                parsed
                        .intraday_recovery_certified_high_watermark_percent <
                    51U ||
                parsed
                        .intraday_recovery_certified_high_watermark_percent >
                    89U) {
                *error =
                    "--intraday-recovery-certified-high-watermark-percent "
                    "must be 51..89";
                return false;
            }
            parsed.intraday_recovery_online_tuning_set = true;
        } else if (
            option ==
            "--intraday-recovery-boundary-alignment-ms") {
            if (!ParseU32(
                    value,
                    &parsed
                         .intraday_recovery_boundary_alignment_ms) ||
                parsed.intraday_recovery_boundary_alignment_ms == 0U ||
                parsed.intraday_recovery_boundary_alignment_ms >
                    60'000U) {
                *error =
                    "--intraday-recovery-boundary-alignment-ms must "
                    "be 1..60000";
                return false;
            }
            parsed.intraday_recovery_online_tuning_set = true;
        } else if (
            option ==
            "--intraday-recovery-progress-interval-seconds") {
            if (!ParseU32(
                    value,
                    &parsed
                         .intraday_recovery_progress_interval_seconds) ||
                parsed.intraday_recovery_progress_interval_seconds == 0U ||
                parsed.intraday_recovery_progress_interval_seconds >
                    3'600U) {
                *error =
                    "--intraday-recovery-progress-interval-seconds must "
                    "be 1..3600";
                return false;
            }
            parsed.intraday_recovery_online_tuning_set = true;
        } else if (
            option == "--intraday-recovery-warmup-seconds") {
            if (!ParseU32(
                    value,
                    &parsed
                         .intraday_recovery_warmup_seconds) ||
                parsed.intraday_recovery_warmup_seconds == 0U ||
                parsed.intraday_recovery_warmup_seconds >
                    86'400U) {
                *error =
                    "--intraday-recovery-warmup-seconds must be "
                    "1..86400";
                return false;
            }
            parsed.intraday_recovery_tuning_set = true;
        } else if (
            option ==
            "--intraday-recovery-backpressure-seconds") {
            if (!ParseU32(
                    value,
                    &parsed
                         .intraday_recovery_backpressure_seconds) ||
                parsed
                        .intraday_recovery_backpressure_seconds ==
                    0U ||
                parsed
                        .intraday_recovery_backpressure_seconds >
                    86'400U) {
                *error =
                    "--intraday-recovery-backpressure-seconds must "
                    "be 1..86400";
                return false;
            }
            parsed.intraday_recovery_tuning_set = true;
        } else if (option == "--kline-windows-ms") {
            if (!ParseKLineWindows(
                    value, &parsed.kline_windows_ms, error)) {
                return false;
            }
        } else if (option == "--generation-interval-ms") {
            if (!ParseU32(
                    value, &parsed.generation_interval_ms) ||
                parsed.generation_interval_ms == 0U ||
                parsed.generation_interval_ms > 60'000U) {
                *error =
                    "--generation-interval-ms must be 1..60000";
                return false;
            }
        } else if (option == "--generation-timeout-ms") {
            if (!ParseU32(
                    value, &parsed.generation_timeout_ms) ||
                parsed.generation_timeout_ms == 0U ||
                parsed.generation_timeout_ms > 600'000U) {
                *error =
                    "--generation-timeout-ms must be 1..600000";
                return false;
            }
        } else if (option == "--ipc-socket") {
            parsed.ipc_socket = std::string(value);
        } else if (option == "--live-preview-ipc-socket") {
            parsed.live_preview_ipc_socket = std::string(value);
        } else if (option == "--event-cpu-set") {
            common::LinuxCpuSetV1 parsed_cpu_set{};
            const common::LinuxCpuSetParseErrorV1 cpu_error =
                common::ParseLinuxCpuSetV1(value, &parsed_cpu_set);
            if (cpu_error != common::LinuxCpuSetParseErrorV1::kNone) {
                *error =
                    "--event-cpu-set is invalid: " +
                    std::string(
                        common::LinuxCpuSetParseErrorNameV1(
                            cpu_error));
                return false;
            }
            parsed.event_cpu_set = std::string(value);
        } else if (option == "--ipc-tick-ring-records") {
            if (!ParseU64(
                    value, &parsed.ipc_tick_ring_records) ||
                parsed.ipc_tick_ring_records == 0U) {
                *error =
                    "--ipc-tick-ring-records must be positive u64";
                return false;
            }
        } else if (option == "--ipc-key-arena-mib") {
            constexpr std::uint64_t bytes_per_mib =
                std::uint64_t{1024U} * 1024U;
            if (!ParsePositiveScaledBytes(
                    value,
                    bytes_per_mib,
                    &parsed.ipc_key_arena_bytes)) {
                *error =
                    "--ipc-key-arena-mib must be a positive u64 whose "
                    "byte conversion does not overflow";
                return false;
            }
        } else if (option == "--ipc-max-mapping-mib") {
            constexpr std::uint64_t bytes_per_mib =
                std::uint64_t{1024U} * 1024U;
            if (!ParsePositiveScaledBytes(
                    value,
                    bytes_per_mib,
                    &parsed.ipc_maximum_mapping_bytes)) {
                *error =
                    "--ipc-max-mapping-mib must be a positive u64 whose "
                    "byte conversion does not overflow";
                return false;
            }
        } else if (
            option == "--native-maximum-backward-displacement") {
            if (!ParseU64(
                    value,
                    &parsed.native_maximum_backward_displacement)) {
                *error =
                    "--native-maximum-backward-displacement must be u64";
                return false;
            }
            parsed.native_maximum_backward_displacement_set = true;
        } else if (option == "--certified-ipc-socket") {
            parsed.certified_ipc_socket = std::string(value);
            parsed.certified_ipc_socket_set = true;
        }
    }

    if (parsed.sdk_library.empty() ||
        parsed.session_epoch == 0U ||
        parsed.trade_date == 0U ||
        parsed.daily_catalog.empty() ||
        parsed.catalog_version == 0U ||
        parsed.server_address.empty() ||
        parsed.user_name.empty() ||
        parsed.ipc_socket.empty()) {
        *error = "all required options must be supplied";
        return false;
    }
    if (!parsed.ipc_socket.is_absolute()) {
        *error = "--ipc-socket must be an absolute path";
        return false;
    }
    if (!parsed.daily_catalog.is_absolute()) {
        *error = "--daily-catalog must be an absolute path";
        return false;
    }
    if (!parsed.intraday_recovery_csv_dir.empty() &&
        !parsed.intraday_recovery_csv_dir.is_absolute()) {
        *error =
            "--intraday-recovery-csv-dir must be an absolute path";
        return false;
    }
    if (!parsed.intraday_recovery_journal_dir.empty() &&
        !parsed.intraday_recovery_journal_dir.is_absolute()) {
        *error =
            "--intraday-recovery-journal-dir must be an absolute path";
        return false;
    }
    if (!parsed.live_preview_ipc_socket.empty() &&
        !parsed.live_preview_ipc_socket.is_absolute()) {
        *error = "--live-preview-ipc-socket must be an absolute path";
        return false;
    }
    const bool csv_recovery =
        !parsed.intraday_recovery_csv_dir.empty();
    const unsigned int startup_mode_count =
        static_cast<unsigned int>(parsed.intraday_store_from_open) +
        static_cast<unsigned int>(csv_recovery) +
        static_cast<unsigned int>(parsed.intraday_live_partial);
    if (startup_mode_count != 1U) {
        *error =
            "production requires exactly one of "
            "--intraday-store-from-open, "
            "--intraday-recovery-csv-dir, or "
            "--intraday-live-partial";
        return false;
    }
    if (parsed.intraday_live_partial) {
        if (parsed.native_gap_recovery_enabled &&
            !parsed.native_maximum_backward_displacement_set) {
            *error =
                "--intraday-live-partial canonical Event requires "
                "--native-maximum-backward-displacement";
            return false;
        }
        if (!parsed.native_gap_recovery_enabled &&
            parsed.native_maximum_backward_displacement_set) {
            *error =
                "--native-maximum-backward-displacement cannot be "
                "combined with --disable-native-gap-recovery";
            return false;
        }
        if (parsed.native_gap_recovery_enabled &&
            (parsed.native_maximum_backward_displacement >=
                 ipc::kRealtimeCertifiedDefaultReorderSpanV1 ||
             parsed.native_maximum_backward_displacement >= 16'384U)) {
            *error =
                "--native-maximum-backward-displacement must be below "
                "both the reorder span and per-channel pending capacity";
            return false;
        }
    } else if (parsed.native_maximum_backward_displacement_set) {
        *error =
            "--native-maximum-backward-displacement requires "
            "--intraday-live-partial";
        return false;
    }
    if (!parsed.event_cpu_set.empty() &&
        !parsed.native_gap_recovery_enabled) {
        *error =
            "--event-cpu-set requires the canonical Event service";
        return false;
    }
    if (parsed.certified_ipc_socket_set &&
        !parsed.native_gap_recovery_enabled) {
        *error =
            "--certified-ipc-socket cannot be combined with "
            "--disable-native-gap-recovery";
        return false;
    }
    if (parsed.native_gap_recovery_enabled) {
        if (!parsed.certified_ipc_socket_set) {
            parsed.certified_ipc_socket =
                parsed.ipc_socket.string() + ".certified";
        }
        if (!parsed.certified_ipc_socket.is_absolute()) {
            *error = "--certified-ipc-socket must be an absolute path";
            return false;
        }
        if (parsed.certified_ipc_socket == parsed.ipc_socket) {
            *error =
                "--certified-ipc-socket must be distinct from "
                "--ipc-socket";
            return false;
        }
    }
    if (!parsed.intraday_store_maximum_records_set ||
        !parsed.intraday_store_memory_set) {
        *error =
            "production requires explicit positive "
            "--intraday-store-max-records and --intraday-store-memory-gib";
        return false;
    }
    if (!csv_recovery && parsed.intraday_recovery_mode_set) {
        *error =
            "--intraday-recovery-mode requires "
            "--intraday-recovery-csv-dir";
        return false;
    }
    if (csv_recovery) {
        if (parsed.intraday_recovery_journal_dir.empty() ||
            parsed.live_preview_ipc_socket.empty()) {
            *error =
                "online recovery requires "
                "--intraday-recovery-journal-dir and "
                "--live-preview-ipc-socket";
            return false;
        }
        if (parsed.intraday_recovery_journal_segment_bytes >
            parsed.intraday_recovery_journal_maximum_bytes) {
            *error =
                "journal segment size must not exceed journal capacity";
            return false;
        }
        if (parsed.intraday_recovery_journal_dir.lexically_normal() ==
            parsed.intraday_recovery_csv_dir.lexically_normal()) {
            *error =
                "--intraday-recovery-journal-dir must be distinct from "
                "the immutable CSV directory";
            return false;
        }
        if (parsed.live_preview_ipc_socket == parsed.ipc_socket ||
            (parsed.native_gap_recovery_enabled &&
             parsed.live_preview_ipc_socket ==
                 parsed.certified_ipc_socket)) {
            *error =
                "--live-preview-ipc-socket must be distinct from recovered "
                "and CERTIFIED sockets";
            return false;
        }
    } else if (parsed.intraday_recovery_online_tuning_set ||
               !parsed.live_preview_ipc_socket.empty()) {
        *error =
            "journal/live-preview options require "
            "--intraday-recovery-csv-dir";
        return false;
    }
    if (!csv_recovery &&
        parsed.intraday_recovery_tuning_set) {
        *error =
            "--intraday-recovery-* tuning options require "
            "--intraday-recovery-csv-dir";
        return false;
    }
    if (parsed.ipc_key_arena_bytes >
        parsed.ipc_maximum_mapping_bytes) {
        *error =
            "--ipc-key-arena-mib must not exceed "
            "--ipc-max-mapping-mib";
        return false;
    }
    *output = std::move(parsed);
    return true;
}

bool MinimumTickRingCapacity(
    const runtime::RealtimePipelineConfigV1& config,
    std::uint64_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    std::size_t applied_window = 0U;
    if (!runtime::RealtimePipelineAppliedWindowCapacityV1(
            config, &applied_window)) {
        return false;
    }
    *output = static_cast<std::uint64_t>(applied_window);
    return true;
}

std::vector<market::KLineWindowSpecV1> BuildKLineWindows(
    const Options& options) {
    std::vector<market::KLineWindowSpecV1> windows;
    windows.reserve(options.kline_windows_ms.size());
    for (const std::uint32_t duration_ms :
         options.kline_windows_ms) {
        market::KLineWindowSpecV1 window{};
        window.window_id = duration_ms;
        window.duration_ns =
            static_cast<std::uint64_t>(duration_ms) *
            market::kKLineNanosecondsPerMillisecondV1;
        windows.push_back(window);
    }
    return windows;
}

bool PublishKLineGeneration(
    const runtime::RealtimePipelineCutResultV1& cut,
    const std::shared_ptr<ipc::RealtimeSharedMarketServiceV2>&
        ipc_service,
    std::string_view context) {
    if (!cut.kline_enabled) {
        return true;
    }
    if (cut.kline_generation == nullptr ||
        !ipc_service->PublishKLineGeneration(
            *cut.kline_generation)) {
        std::cerr
            << "mdl-production-router: " << context
            << " IPC KLine generation publication failed\n";
        return false;
    }
    return true;
}

void ReportFatalSnapshot(
    const runtime::RealtimePipelineSnapshotV1& snapshot) {
    std::cerr
        << "mdl-production-router: pipeline failed: accepted_sequence="
        << snapshot.processing_progress.accepted_sequence
        << " applied_sequence="
        << snapshot.processing_progress.applied_sequence
        << " processing_lag_records="
        << snapshot.processing_progress.processing_lag_records()
        << " filtered_messages=" << snapshot.filtered_messages
        << " last_decode_error="
        << static_cast<unsigned int>(snapshot.last_decode_error)
        << '\n';
}

struct OnlineRunState final {
    void Fail(std::string value) noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex);
            if (detail.empty()) {
                detail = std::move(value);
            }
        } catch (...) {
        }
        failed.store(true, std::memory_order_release);
    }

    [[nodiscard]] std::string Detail() const {
        std::lock_guard<std::mutex> lock(mutex);
        return detail;
    }

    std::atomic<bool> promoted{false};
    std::atomic<bool> failed{false};
    std::atomic<bool> abort_requested{false};
    // Serializes the final control-plane exposure with shutdown's decision to
    // cancel an unpromoted rebuild.  CSV replay and journal catch-up never
    // take this mutex.
    std::mutex promotion_mutex;
    mutable std::mutex mutex;
    std::string detail;
};

[[nodiscard]] bool CertifiedServiceTerminalHealthy(
    const ipc::RealtimeCertifiedServiceSnapshotV1& snapshot,
    bool require_control_running = false) noexcept {
    using ControlState =
        ipc::RealtimeCertifiedServiceSnapshotV1::ControlState;
    const bool control_healthy =
        snapshot.control_state == ControlState::kRunning ||
        (!require_control_running &&
         snapshot.control_state == ControlState::kNotStarted);
    return snapshot.worker_running && control_healthy &&
           !snapshot.globally_frozen_resource &&
           snapshot.resource_exhaustion_count == 0U &&
           snapshot.dropped_handoffs == 0U &&
           snapshot.conflicting_duplicate_count == 0U;
}

bool CurrentRealtimeNs(std::uint64_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    const auto count =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    if (count <= 0) {
        return false;
    }
    *output = static_cast<std::uint64_t>(count);
    return true;
}

runtime::RealtimePipelineConfigV1 BuildOnlinePipelineBase(
    const Options& options,
    const common::Identity128& run_id,
    const std::shared_ptr<const market::DailyInstrumentCatalogV2>&
        daily_catalog,
    market::InstrumentRuntimeStateV2* runtime_state) {
    runtime::RealtimePipelineConfigV1 config{};
    config.run_id = run_id;
    config.trade_date = options.trade_date;
    config.daily_catalog = daily_catalog;
    config.runtime_state = runtime_state;
    config.source_stream_ids = {1001U, 1002U, 2001U, 2002U};
    config.store_worker_count = options.instrument_store_workers;
    config.parallel_decoder_worker_count =
        options.parallel_decoder_workers;
    config.decoder_queue_capacity_per_source =
        static_cast<std::size_t>(
            options.decoder_queue_records_per_source);
    config.store_queue_capacity_per_source_worker =
        static_cast<std::size_t>(
            options.store_queue_records_per_source_worker);
    config.intraday_store.segment_target_bytes =
        static_cast<std::size_t>(
            options.intraday_store_segment_kib) *
        1024U;
    config.intraday_store.maximum_session_records =
        options.intraday_store_maximum_records;
    config.intraday_store.maximum_session_accounted_bytes =
        options.intraday_store_memory_bytes;
    config.intraday_store.maximum_records_per_batch =
        static_cast<std::size_t>(
            options.intraday_store_batch_records);
    config.enforce_receive_trade_date = true;
    config.tick_ring_capacity =
        static_cast<std::size_t>(options.ipc_tick_ring_records);
    return config;
}

ipc::RealtimeSharedServiceConfigV2 BuildOnlineIpcConfig(
    const Options& options,
    const common::Identity128& run_id,
    const std::shared_ptr<const market::DailyInstrumentCatalogV2>&
        daily_catalog,
    std::vector<market::KLineWindowSpecV1> kline_windows,
    std::filesystem::path socket) {
    ipc::RealtimeSharedServiceConfigV2 config{};
    config.run_id = run_id;
    config.session_epoch = options.session_epoch;
    config.trade_date = options.trade_date;
    config.daily_catalog = daily_catalog;
    config.kline_windows = std::move(kline_windows);
    config.tick_ring_capacity = options.ipc_tick_ring_records;
    config.key_arena_bytes = options.ipc_key_arena_bytes;
    config.maximum_mapping_bytes = options.ipc_maximum_mapping_bytes;
    config.control_socket_path = std::move(socket);
    return config;
}

int RunLivePartial(
    const Options& options,
    const common::Identity128& run_id,
    const std::shared_ptr<const market::DailyInstrumentCatalogV2>&
        daily_catalog,
    market::InstrumentRuntimeStateV2* runtime_state) {
    const std::vector<market::KLineWindowSpecV1> kline_windows =
        BuildKLineWindows(options);
    std::uint64_t provisional_event_coverage_unix_ns = 0U;
    if (!CurrentRealtimeNs(&provisional_event_coverage_unix_ns)) {
        std::cerr
            << "mdl-production-router: LIVE_PARTIAL coverage clock "
               "read failed\n";
        return 1;
    }

    const auto exposure_gate =
        std::make_shared<std::atomic<bool>>(false);
    ipc::RealtimeSharedServiceConfigV2 ipc_config =
        BuildOnlineIpcConfig(
            options,
            run_id,
            daily_catalog,
            kline_windows,
            options.ipc_socket);
    ipc_config.coverage_from_open = false;
    ipc_config.startup_prefix_recovered = false;
    ipc_config.full_day_kline_valid = false;
    ipc_config.full_day_factor_valid = false;
    ipc_config.certified_prefix_valid = false;
    ipc_config.control_exposure_gate = exposure_gate;

    std::shared_ptr<ipc::RealtimeSharedMarketServiceV2> ipc_service;
    int ipc_system_error = 0;
    const ipc::RealtimeSharedServiceCreateErrorV2 ipc_error =
        ipc::RealtimeSharedMarketServiceV2::Create(
            std::move(ipc_config),
            &ipc_service,
            &ipc_system_error);
    if (ipc_error !=
            ipc::RealtimeSharedServiceCreateErrorV2::kNone ||
        ipc_service == nullptr ||
        !ipc_service->StartLivePartialWithProcessStartHistory(
            &ipc_system_error)) {
        std::cerr
            << "mdl-production-router: LIVE_PARTIAL FAST start failed: "
            << ipc::RealtimeSharedServiceCreateErrorNameV2(ipc_error)
            << " errno=" << ipc_system_error << '\n';
        if (ipc_service != nullptr) {
            ipc_service->MarkFailed();
            ipc_service->StopControl();
        }
        return 1;
    }

    std::shared_ptr<ipc::RealtimeCertifiedMarketServiceV1>
        certified_service;
    bool event_available = false;
    if (options.native_gap_recovery_enabled) {
        constexpr std::size_t maximum_size =
            std::numeric_limits<std::size_t>::max();
        if (options.intraday_store_maximum_records > maximum_size ||
            options.intraday_store_maximum_records >
                maximum_size / 4U) {
            std::cerr
                << "mdl-production-router: partial canonical Event "
                   "unavailable: capacity is not representable; "
                   "FAST remains available\n";
        } else {
            const std::size_t maximum_order_states =
                static_cast<std::size_t>(
                    options.intraday_store_maximum_records);
            ipc::RealtimeCertifiedServiceConfigV1 certified_config{};
            certified_config.run_id = run_id;
            certified_config.session_epoch = options.session_epoch;
            certified_config.trade_date = options.trade_date;
            certified_config.daily_catalog = daily_catalog;
            certified_config.fast_sink = ipc_service;
            certified_config.certified_tick_ring_capacity =
                options.ipc_tick_ring_records;
            certified_config.handoff_queue_capacity =
                options.certified_handoff_queue_records;
            certified_config.maximum_mapping_bytes =
                options.ipc_maximum_mapping_bytes;
            certified_config.maximum_order_states =
                maximum_order_states;
            certified_config.maximum_derived_events =
                maximum_order_states * 4U;
            certified_config.maximum_certified_ticks =
                options.intraday_store_maximum_records;
            certified_config.worker_cpu_set = options.event_cpu_set;
            certified_config.control_cpu_set = options.event_cpu_set;
            certified_config.tick_history_worker_cpu_set =
                options.event_cpu_set;
            certified_config.control_exposure_gate = exposure_gate;
            certified_config.control_socket_path =
                options.certified_ipc_socket;
            certified_config.origin_policy =
                l2flow::realtime::NativeSequenceOriginPolicyV1::
                    kBoundedProcessStart;
            certified_config.maximum_backward_displacement =
                options.native_maximum_backward_displacement;
            certified_config.event_temporal_coverage =
                ipc::CertifiedOrderEventTemporalCoverageV1::
                    kFromProcessStart;
            certified_config.event_coverage_start_unix_ns =
                provisional_event_coverage_unix_ns;

            int certified_system_error = 0;
            const ipc::RealtimeCertifiedServiceCreateErrorV1
                certified_error =
                    ipc::RealtimeCertifiedMarketServiceV1::Create(
                        std::move(certified_config),
                        &certified_service,
                        &certified_system_error);
            if (certified_error !=
                    ipc::RealtimeCertifiedServiceCreateErrorV1::kNone ||
                certified_service == nullptr ||
                !certified_service->StartWorker(
                    &certified_system_error) ||
                !certified_service->StartControl(
                    &certified_system_error)) {
                std::cerr
                    << "mdl-production-router: partial canonical Event "
                       "start failed: "
                    << ipc::RealtimeCertifiedServiceCreateErrorNameV1(
                           certified_error)
                    << " errno=" << certified_system_error
                    << "; FAST remains available\n";
                if (certified_service != nullptr) {
                    certified_service->StopControl();
                    certified_service.reset();
                }
            } else {
                event_available = true;
            }
        }
    }

    runtime::RealtimePipelineConfigV1 pipeline_config =
        BuildOnlinePipelineBase(
            options, run_id, daily_catalog, runtime_state);
    pipeline_config.intraday_store.coverage_from_open = false;
    pipeline_config.kline.windows = kline_windows;
    pipeline_config.factor_generation_enabled = false;
    pipeline_config.sdk.enabled = true;
    pipeline_config.sdk.library_path = options.sdk_library;
    pipeline_config.sdk.server_address = options.server_address;
    pipeline_config.sdk.user_name = options.user_name;
    pipeline_config.sdk.log_prefix = options.sdk_log_prefix + "-partial";
    pipeline_config.sdk.message_encoding =
        datayes::mdl::MDLEID_BINARY;
    pipeline_config.sdk.merge_message = false;
    pipeline_config.applied_record_sink =
        certified_service != nullptr
            ? std::static_pointer_cast<
                  market::RealtimeAppliedRecordSinkV1>(
                  certified_service)
            : std::static_pointer_cast<
                  market::RealtimeAppliedRecordSinkV1>(
                  ipc_service);
    if (certified_service != nullptr) {
        pipeline_config.native_sequence_observation_sink =
            certified_service;
    }
    pipeline_config.processing_progress_sink = ipc_service;
    pipeline_config.store_generation_sink = ipc_service;

    std::uint64_t minimum_tick_ring = 0U;
    if (!MinimumTickRingCapacity(
            pipeline_config, &minimum_tick_ring) ||
        options.ipc_tick_ring_records < minimum_tick_ring) {
        std::cerr
            << "mdl-production-router: LIVE_PARTIAL tick ring is "
               "smaller than the applied dispatch window\n";
        if (certified_service != nullptr) {
            certified_service->StopControl();
        }
        ipc_service->MarkFailed();
        ipc_service->StopControl();
        return 1;
    }

    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    const runtime::RealtimePipelineCreateErrorV1 create_error =
        runtime::RealtimePipelineV1::Create(
            std::move(pipeline_config), &pipeline, &detail);
    if (create_error !=
            runtime::RealtimePipelineCreateErrorV1::kNone ||
        pipeline == nullptr) {
        std::cerr
            << "mdl-production-router: LIVE_PARTIAL pipeline "
               "create/connect failed: "
            << runtime::RealtimePipelineCreateErrorNameV1(create_error)
            << (detail.empty() ? "" : ": ") << detail << '\n';
        if (certified_service != nullptr) {
            certified_service->StopControl();
        }
        ipc_service->MarkFailed();
        ipc_service->StopControl();
        return 1;
    }

    std::uint64_t process_start_coverage_unix_ns = 0U;
    if (!CurrentRealtimeNs(&process_start_coverage_unix_ns) ||
        !ipc_service->PrepareProcessStartHistoryCoverage(
            process_start_coverage_unix_ns) ||
        (!kline_windows.empty() &&
         !ipc_service->PrepareProcessStartKLineCoverage(
             process_start_coverage_unix_ns))) {
        std::cerr
            << "mdl-production-router: LIVE_PARTIAL coverage "
               "preparation failed\n";
        pipeline->StopAndDrain();
        if (certified_service != nullptr) {
            certified_service->StopControl();
        }
        ipc_service->MarkFailed();
        ipc_service->StopControl();
        return 1;
    }
    if (certified_service != nullptr) {
        int coverage_system_error = 0;
        if (!certified_service->FinalizeProcessStartCoverage(
                process_start_coverage_unix_ns,
                std::chrono::milliseconds(
                    options.generation_timeout_ms),
                &coverage_system_error)) {
            event_available = false;
            std::cerr
                << "mdl-production-router: partial canonical Event "
                   "coverage finalization failed after SDK Connect: errno="
                << coverage_system_error
                << "; FAST remains available\n";
            certified_service->StopControl();
        }
    }
    if (event_available && certified_service != nullptr &&
        !certified_service->ControlRunningConfirmed()) {
        event_available = false;
        std::cerr
            << "mdl-production-router: partial canonical Event control "
               "failed before exposure; FAST remains available\n";
        certified_service->StopControl();
    }

    exposure_gate->store(true, std::memory_order_release);
    std::cerr
        << "mdl-production-router: LIVE_PARTIAL available: "
        << "socket=" << ipc_service->control_socket_path()
        << " run_id=" << common::Identity128Hex(run_id)
        << " coverage_from_open=false"
        << " startup_prefix_recovered=false"
        << " full_day_kline_valid=false"
        << " kline_enabled="
        << (!kline_windows.empty() ? "true" : "false")
        << " window_count=" << kline_windows.size()
        << " kline_quality="
        << (!kline_windows.empty()
                ? "PROCESS_START_PARTIAL"
                : "DISABLED")
        << " coverage_start_unix_ns="
        << process_start_coverage_unix_ns
        << " full_day_factor_valid=false"
        << " certified_prefix_valid=false"
        << " event_available="
        << (event_available ? "true" : "false")
        << " event_socket="
        << (event_available
                ? options.certified_ipc_socket.string()
                : std::string("none"))
        << " event_temporal_coverage="
        << (event_available ? "PROCESS_START_PARTIAL" : "UNAVAILABLE")
        << " event_ordering="
        << (event_available ? "PER_CHANNEL_NATIVE_CANONICAL" : "NONE")
        << " event_full_day_complete=false"
        << " maximum_backward_displacement="
        << options.native_maximum_backward_displacement
        << " event_logical_cpu_disjoint="
        << (!options.event_cpu_set.empty() ? "true" : "false")
        << " recovery="
        << (event_available ? "bounded" : "disabled")
        << '\n';

    const auto interval =
        std::chrono::milliseconds(options.generation_interval_ms);
    const auto timeout =
        std::chrono::milliseconds(options.generation_timeout_ms);
    int exit_code = 0;
    bool event_failure_reported = !event_available;
    bool channel_degradation_reported = false;
    while (g_stop_requested == 0) {
        const IntervalWaitResult wait =
            WaitForInterval(interval, options.trade_date);
        if (wait == IntervalWaitResult::kSignal) {
            break;
        }
        if (wait != IntervalWaitResult::kElapsed) {
            std::cerr
                << "mdl-production-router: LIVE_PARTIAL trade-date/clock "
                   "boundary failed\n";
            exit_code = 1;
            break;
        }
        if (pipeline->fatal() || ipc_service->failed()) {
            ReportFatalSnapshot(pipeline->Snapshot());
            exit_code = 1;
            break;
        }

        if (certified_service != nullptr) {
            const ipc::RealtimeCertifiedServiceSnapshotV1 event_snapshot =
                certified_service->Snapshot();
            const bool event_failed =
                event_snapshot.globally_frozen_resource ||
                event_snapshot.state ==
                    ipc::RealtimeCertifiedStateV1::kFrozenConflict ||
                event_snapshot.state ==
                    ipc::RealtimeCertifiedStateV1::kFrozenResource ||
                event_snapshot.control_state ==
                    ipc::RealtimeCertifiedServiceSnapshotV1::
                        ControlState::kFailed;
            if (event_failed && !event_failure_reported) {
                event_failure_reported = true;
                event_available = false;
                std::cerr
                    << "mdl-production-router: partial canonical Event "
                       "failed after ACTIVE; FAST remains available: "
                    << "state="
                    << static_cast<std::uint32_t>(
                           event_snapshot.state)
                    << " frozen_channel_count="
                    << event_snapshot.frozen_channel_count
                    << " dropped_handoffs="
                    << event_snapshot.dropped_handoffs << '\n';
            } else if (
                event_snapshot.state ==
                    ipc::RealtimeCertifiedStateV1::kDegraded &&
                !channel_degradation_reported) {
                channel_degradation_reported = true;
                std::cerr
                    << "mdl-production-router: partial canonical Event "
                       "is channel-isolated DEGRADED; healthy channels "
                       "remain available: frozen_channel_count="
                    << event_snapshot.frozen_channel_count << '\n';
            }
        }

        const runtime::RealtimePipelineCutResultV1 cut =
            pipeline->CutAndPublishGeneration(timeout);
        if (!cut.published()) {
            std::cerr
                << "mdl-production-router: LIVE_PARTIAL generation "
                   "failed: "
                << runtime::RealtimePipelineCutErrorNameV1(cut.error)
                << " generation="
                << market::RealtimeHistoryGenerationErrorNameV1(
                       cut.generation_error)
                << '\n';
            exit_code = 1;
            break;
        }
        if (!PublishKLineGeneration(
                cut, ipc_service, "LIVE_PARTIAL periodic")) {
            exit_code = 1;
            break;
        }
    }

    if (!ipc_service->failed()) {
        ipc_service->MarkDraining();
    }
    if (certified_service != nullptr) {
        certified_service->MarkDraining();
    }
    runtime::RealtimePipelineCutResultV1 final{};
    if (exit_code == 0 && !pipeline->fatal()) {
        final = pipeline->StopAndPublishFinalGeneration(timeout);
        if (!final.published()) {
            std::cerr
                << "mdl-production-router: LIVE_PARTIAL final generation "
                   "failed: "
                << runtime::RealtimePipelineCutErrorNameV1(final.error)
                << '\n';
            exit_code = 1;
        } else if (!PublishKLineGeneration(
                       final, ipc_service, "LIVE_PARTIAL final")) {
            exit_code = 1;
        }
    } else {
        pipeline->StopAndDrain();
    }

    const runtime::RealtimePipelineSnapshotV1 snapshot =
        pipeline->Snapshot();
    if (snapshot.fatal) {
        exit_code = 1;
    }

    ipc::RealtimeCertifiedServiceSnapshotV1 event_snapshot{};
    if (certified_service != nullptr) {
        if (exit_code == 0) {
            certified_service->MarkStoppedClean();
            event_snapshot = certified_service->Snapshot();
        } else {
            event_snapshot = certified_service->Snapshot();
        }
        certified_service->StopControl();
    }
    if (exit_code == 0) {
        if (!ipc_service->MarkStoppedClean(
                snapshot.tick_stream_sequence)) {
            exit_code = 1;
        }
    }
    if (exit_code != 0) {
        ipc_service->MarkFailed();
    }

    std::cerr
        << "mdl-production-router: LIVE_PARTIAL final: exit_code="
        << exit_code
        << " accepted=" << snapshot.accepted_messages
        << " applied="
        << snapshot.processing_progress.applied_sequence
        << " filtered=" << snapshot.filtered_messages
        << " records=" << snapshot.store.appended_records
        << " tick_stream_sequence=" << snapshot.tick_stream_sequence
        << " coverage_from_open=false"
        << " event_state="
        << static_cast<std::uint32_t>(event_snapshot.state)
        << " event_frozen_channel_count="
        << event_snapshot.frozen_channel_count
        << " event_canonical_apply_frontier="
        << event_snapshot.canonical_apply_frontier
        << " recovery="
        << (certified_service != nullptr ? "bounded" : "disabled")
        << '\n';
    return exit_code;
}
int RunOnlineRecovery(
    const Options& options,
    const common::Identity128& recovered_run_id,
    const std::shared_ptr<const market::DailyInstrumentCatalogV2>&
        daily_catalog,
    const std::vector<market::KLineWindowSpecV1>& kline_windows) {
    common::Identity128 preview_run_id{};
    int entropy_error = 0;
    if (!common::GenerateIdentity128(
            &preview_run_id, &entropy_error) ||
        preview_run_id == recovered_run_id) {
        std::cerr
            << "mdl-production-router: preview run-id entropy failed: "
            << "errno=" << entropy_error << '\n';
        return 1;
    }

    std::unique_ptr<market::InstrumentRuntimeStateV2>
        preview_runtime_state;
    std::unique_ptr<market::InstrumentRuntimeStateV2>
        shadow_runtime_state;
    const market::InstrumentRuntimeStateErrorV2 preview_state_error =
        market::InstrumentRuntimeStateV2::Create(
            *daily_catalog, &preview_runtime_state);
    const market::InstrumentRuntimeStateErrorV2 shadow_state_error =
        market::InstrumentRuntimeStateV2::Create(
            *daily_catalog, &shadow_runtime_state);
    if (preview_state_error !=
            market::InstrumentRuntimeStateErrorV2::kNone ||
        shadow_state_error !=
            market::InstrumentRuntimeStateErrorV2::kNone ||
        preview_runtime_state == nullptr ||
        shadow_runtime_state == nullptr) {
        std::cerr
            << "mdl-production-router: online recovery runtime state "
               "create failed: preview="
            << market::InstrumentRuntimeStateErrorNameV2(
                   preview_state_error)
            << " shadow="
            << market::InstrumentRuntimeStateErrorNameV2(
                   shadow_state_error)
            << '\n';
        return 1;
    }

    recovery::LiveJournalConfigV1 journal_config{};
    journal_config.directory =
        options.intraday_recovery_journal_dir;
    journal_config.run_id = preview_run_id;
    journal_config.trade_date = options.trade_date;
    journal_config.segment_maximum_bytes =
        options.intraday_recovery_journal_segment_bytes;
    journal_config.maximum_total_bytes =
        options.intraday_recovery_journal_maximum_bytes;
    journal_config.queue_capacity_records =
        static_cast<std::size_t>(
            options.intraday_recovery_journal_queue_records);
    journal_config.sync_batch_records = std::min<std::size_t>(
        256U, journal_config.queue_capacity_records);
    std::shared_ptr<recovery::MdlLiveJournalV1> live_journal;
    int journal_system_error = 0;
    const recovery::LiveJournalErrorV1 journal_error =
        recovery::MdlLiveJournalV1::Create(
            std::move(journal_config),
            &live_journal,
            &journal_system_error);
    if (journal_error != recovery::LiveJournalErrorV1::kNone ||
        live_journal == nullptr) {
        std::cerr
            << "mdl-production-router: live journal create failed: "
            << recovery::LiveJournalErrorNameV1(journal_error)
            << " errno=" << journal_system_error << '\n';
        return 1;
    }

    ipc::RealtimeSharedServiceConfigV2 preview_ipc_config =
        BuildOnlineIpcConfig(
            options,
            preview_run_id,
            daily_catalog,
            {},
            options.live_preview_ipc_socket);
    preview_ipc_config.coverage_from_open = false;
    std::shared_ptr<ipc::RealtimeSharedMarketServiceV2>
        preview_ipc_service;
    int ipc_system_error = 0;
    const ipc::RealtimeSharedServiceCreateErrorV2
        preview_ipc_error =
            ipc::RealtimeSharedMarketServiceV2::Create(
                std::move(preview_ipc_config),
                &preview_ipc_service,
                &ipc_system_error);
    if (preview_ipc_error !=
            ipc::RealtimeSharedServiceCreateErrorV2::kNone ||
        preview_ipc_service == nullptr) {
        std::cerr
            << "mdl-production-router: LIVE_PARTIAL IPC create failed: "
            << ipc::RealtimeSharedServiceCreateErrorNameV2(
                   preview_ipc_error)
            << " errno=" << ipc_system_error << '\n';
        static_cast<void>(live_journal->StopAndFlush());
        return 1;
    }
    std::string detail;

    // Connect the sole SDK owner immediately so the journal covers the whole
    // startup interval, but keep its control plane in INITIALIZING until the
    // fixed recovery-side mappings, pools, and parked worker exist. Incremental
    // CSV/Store allocations remain behind the recovery governor. INITIALIZING
    // accepts internal publications without exposing clients.
    runtime::RealtimePipelineConfigV1 preview_config =
        BuildOnlinePipelineBase(
            options,
            preview_run_id,
            daily_catalog,
            preview_runtime_state.get());
    preview_config.intraday_store.coverage_from_open = false;
    // Preview deliberately publishes no KLine generation.  Its internal
    // partial Store supplies lifetime-safe latest records, while the control
    // service rejects History/delta until clients switch to recovered IPC.
    preview_config.kline.windows.clear();
    preview_config.sdk.enabled = true;
    preview_config.sdk.library_path = options.sdk_library;
    preview_config.sdk.server_address = options.server_address;
    preview_config.sdk.user_name = options.user_name;
    preview_config.sdk.log_prefix =
        options.sdk_log_prefix + "-preview";
    preview_config.sdk.message_encoding =
        datayes::mdl::MDLEID_BINARY;
    preview_config.sdk.merge_message = false;
    preview_config.live_ingress_capture_sink = live_journal;
    preview_config.applied_record_sink = preview_ipc_service;
    preview_config.processing_progress_sink = preview_ipc_service;
    preview_config.store_generation_sink = preview_ipc_service;

    std::uint64_t preview_minimum_tick_ring = 0U;
    if (!MinimumTickRingCapacity(
            preview_config, &preview_minimum_tick_ring) ||
        options.ipc_tick_ring_records < preview_minimum_tick_ring) {
        std::cerr
            << "mdl-production-router: preview tick ring is smaller than "
               "the applied window\n";
        preview_ipc_service->MarkFailed();
        preview_ipc_service->StopControl();
        static_cast<void>(live_journal->StopAndFlush());
        return 1;
    }

    std::unique_ptr<runtime::RealtimePipelineV1> preview_pipeline;
    const runtime::RealtimePipelineCreateErrorV1 preview_create_error =
        runtime::RealtimePipelineV1::Create(
            std::move(preview_config),
            &preview_pipeline,
            &detail);
    if (preview_create_error !=
            runtime::RealtimePipelineCreateErrorV1::kNone ||
        preview_pipeline == nullptr) {
        std::cerr
            << "mdl-production-router: preview pipeline create failed: "
            << runtime::RealtimePipelineCreateErrorNameV1(
                   preview_create_error)
            << (detail.empty() ? "" : ": ") << detail << '\n';
        preview_ipc_service->MarkFailed();
        preview_ipc_service->StopControl();
        static_cast<void>(live_journal->StopAndFlush());
        return 1;
    }

    ipc::RealtimeSharedServiceConfigV2 recovered_ipc_config =
        BuildOnlineIpcConfig(
            options,
            recovered_run_id,
            daily_catalog,
            kline_windows,
            options.ipc_socket);
    // Both recovered control planes run behind one monotonic gate. They may
    // finish thread startup before promotion, but neither can transfer a
    // descriptor until the recovery thread performs the single release store.
    const auto online_exposure_gate =
        std::make_shared<std::atomic<bool>>(false);
    recovered_ipc_config.coverage_from_open = true;
    recovered_ipc_config.startup_prefix_recovered = true;
    recovered_ipc_config.full_day_kline_valid =
        !kline_windows.empty();
    recovered_ipc_config.full_day_factor_valid = true;
    recovered_ipc_config.certified_prefix_valid = false;
    recovered_ipc_config.control_exposure_gate =
        online_exposure_gate;
    std::shared_ptr<ipc::RealtimeSharedMarketServiceV2>
        recovered_ipc_service;
    ipc_system_error = 0;
    const ipc::RealtimeSharedServiceCreateErrorV2
        recovered_ipc_error =
            ipc::RealtimeSharedMarketServiceV2::Create(
                std::move(recovered_ipc_config),
                &recovered_ipc_service,
                &ipc_system_error);
    if (recovered_ipc_error !=
            ipc::RealtimeSharedServiceCreateErrorV2::kNone ||
        recovered_ipc_service == nullptr) {
        std::cerr
            << "mdl-production-router: recovered IPC create failed: "
            << ipc::RealtimeSharedServiceCreateErrorNameV2(
                   recovered_ipc_error)
            << " errno=" << ipc_system_error << '\n';
        preview_pipeline->StopAndDrain();
        preview_ipc_service->MarkFailed();
        preview_ipc_service->StopControl();
        static_cast<void>(live_journal->StopAndFlush());
        return 1;
    }

    std::shared_ptr<ipc::RealtimeCertifiedMarketServiceV1>
        certified_service;
    if (options.native_gap_recovery_enabled) {
        constexpr std::size_t maximum_size =
            std::numeric_limits<std::size_t>::max();
        if (options.intraday_store_maximum_records > maximum_size ||
            options.intraday_store_maximum_records >
                maximum_size / 4U) {
            std::cerr
                << "mdl-production-router: online CERTIFIED capacity "
                   "is not representable\n";
            preview_pipeline->StopAndDrain();
            recovered_ipc_service->MarkFailed();
            preview_ipc_service->MarkFailed();
            static_cast<void>(live_journal->StopAndFlush());
            return 1;
        }
        const std::size_t maximum_order_states =
            static_cast<std::size_t>(
                options.intraday_store_maximum_records);
        ipc::RealtimeCertifiedServiceConfigV1 certified_config{};
        certified_config.run_id = recovered_run_id;
        certified_config.session_epoch = options.session_epoch;
        certified_config.trade_date = options.trade_date;
        certified_config.daily_catalog = daily_catalog;
        certified_config.fast_sink = recovered_ipc_service;
        certified_config.certified_tick_ring_capacity =
            options.ipc_tick_ring_records;
        certified_config.handoff_queue_capacity =
            options.certified_handoff_queue_records;
        certified_config.maximum_mapping_bytes =
            options.ipc_maximum_mapping_bytes;
        certified_config.maximum_order_states = maximum_order_states;
        certified_config.maximum_derived_events =
            maximum_order_states * 4U;
        // The Store bound covers every applied record and is therefore a
        // conservative upper bound for the Tick-only canonical journal. Do
        // not silently fall back to the library's smaller convenience
        // default: exhausting Tick History must not freeze an otherwise
        // correctly sized recovered session.
        certified_config.maximum_certified_ticks =
            options.intraday_store_maximum_records;
        certified_config.worker_cpu_set = options.event_cpu_set;
        certified_config.control_cpu_set = options.event_cpu_set;
        certified_config.tick_history_worker_cpu_set =
            options.event_cpu_set;
        certified_config.control_exposure_gate =
            online_exposure_gate;
        // This worker is built before the recovered prefix exists.  A
        // repeatable readiness probe must never be sufficient to expose its
        // control plane; only the final one-shot prefix commit authorizes
        // StartControl().  Ordinary from-open services keep the default
        // policy and therefore retain their existing startup path.
        certified_config.control_requires_prefix_commit = true;
        certified_config.control_socket_path =
            options.certified_ipc_socket;
        int certified_system_error = 0;
        const ipc::RealtimeCertifiedServiceCreateErrorV1
            certified_error =
                ipc::RealtimeCertifiedMarketServiceV1::Create(
                    std::move(certified_config),
                    &certified_service,
                    &certified_system_error);
        if (certified_error !=
                ipc::RealtimeCertifiedServiceCreateErrorV1::kNone ||
            certified_service == nullptr ||
            !certified_service->StartWorker(
                &certified_system_error)) {
            std::cerr
                << "mdl-production-router: online CERTIFIED worker "
                   "start failed: "
                << ipc::RealtimeCertifiedServiceCreateErrorNameV1(
                       certified_error)
                << " errno=" << certified_system_error << '\n';
            if (certified_service != nullptr) {
                certified_service->StopControl();
            }
            preview_pipeline->StopAndDrain();
            recovered_ipc_service->MarkFailed();
            preview_ipc_service->MarkFailed();
            static_cast<void>(live_journal->StopAndFlush());
            return 1;
        }
    }

    runtime::RealtimePipelineConfigV1 shadow_config =
        BuildOnlinePipelineBase(
            options,
            recovered_run_id,
            daily_catalog,
            shadow_runtime_state.get());
    shadow_config.intraday_store.coverage_from_open = true;
    shadow_config.kline.windows = kline_windows;
    shadow_config.sdk.enabled = false;
    shadow_config.external_ingress_enabled = true;
    shadow_config.applied_record_sink =
        certified_service != nullptr
            ? std::static_pointer_cast<
                  market::RealtimeAppliedRecordSinkV1>(
                  certified_service)
            : std::static_pointer_cast<
                  market::RealtimeAppliedRecordSinkV1>(
                  recovered_ipc_service);
    if (certified_service != nullptr) {
        shadow_config.native_sequence_observation_sink =
            certified_service;
    }
    shadow_config.processing_progress_sink = recovered_ipc_service;
    shadow_config.store_generation_sink = recovered_ipc_service;
    std::uint64_t shadow_minimum_tick_ring = 0U;
    if (!MinimumTickRingCapacity(
            shadow_config, &shadow_minimum_tick_ring) ||
        options.ipc_tick_ring_records < shadow_minimum_tick_ring) {
        std::cerr
            << "mdl-production-router: shadow tick ring is smaller than "
               "the applied window\n";
        preview_pipeline->StopAndDrain();
        if (certified_service != nullptr) {
            certified_service->StopControl();
        }
        recovered_ipc_service->MarkFailed();
        preview_ipc_service->MarkFailed();
        static_cast<void>(live_journal->StopAndFlush());
        return 1;
    }
    std::unique_ptr<runtime::RealtimePipelineV1> shadow_pipeline;
    detail.clear();
    const runtime::RealtimePipelineCreateErrorV1 shadow_create_error =
        runtime::RealtimePipelineV1::Create(
            std::move(shadow_config),
            &shadow_pipeline,
            &detail);
    if (shadow_create_error !=
            runtime::RealtimePipelineCreateErrorV1::kNone ||
        shadow_pipeline == nullptr) {
        std::cerr
            << "mdl-production-router: shadow pipeline create failed: "
            << runtime::RealtimePipelineCreateErrorNameV1(
                   shadow_create_error)
            << (detail.empty() ? "" : ": ") << detail << '\n';
        preview_pipeline->StopAndDrain();
        if (certified_service != nullptr) {
            certified_service->StopControl();
        }
        recovered_ipc_service->MarkFailed();
        preview_ipc_service->MarkFailed();
        static_cast<void>(live_journal->StopAndFlush());
        return 1;
    }

    recovery::StartupReplayConfigV1 replay_config{};
    replay_config.directory = options.intraday_recovery_csv_dir;
    replay_config.maximum_message_bytes =
        16U * 1024U * 1024U;
    replay_config.boundary_alignment_timeout_ms =
        options.intraday_recovery_boundary_alignment_ms;
    // Keep a single alignment seam inside one tuple's retained overlap
    // identity budget. This aggregate cap is deliberately conservative when
    // both Shenzhen files contribute selected rows.
    replay_config.maximum_boundary_alignment_records =
        kOnlineRecoveryOverlapRetentionPerTuple;
    std::shared_ptr<recovery::StartupReplaySourceV1> replay_source;
    try {
        replay_source = std::make_shared<
            recovery::MdlCsvStartupReplaySourceV1>(
            std::move(replay_config));
    } catch (...) {
        std::cerr
            << "mdl-production-router: online CSV replay source create "
               "failed\n";
        preview_pipeline->StopAndDrain();
        shadow_pipeline->StopAndDrain();
        if (certified_service != nullptr) {
            certified_service->StopControl();
        }
        recovered_ipc_service->MarkFailed();
        preview_ipc_service->MarkFailed();
        static_cast<void>(live_journal->StopAndFlush());
        return 1;
    }

    OnlineRunState online_state;
    recovery::OnlineRecoveryConfigV1 online_config{};
    online_config.live_journal = live_journal;
    online_config.csv_replay_source = replay_source;
    online_config.shadow_pipeline = shadow_pipeline.get();
    online_config.trade_date = options.trade_date;
    online_config.source_stream_ids =
        {1001U, 1002U, 2001U, 2002U};
    online_config.overlap_retention_per_tuple =
        kOnlineRecoveryOverlapRetentionPerTuple;
    const auto recovery_warmup_timeout = std::chrono::seconds(
        options.intraday_recovery_warmup_seconds);
    online_config.warmup_timeout = recovery_warmup_timeout;
    online_config.per_record_admission_timeout =
        std::chrono::seconds(
            options.intraday_recovery_backpressure_seconds);
    online_config.cancel_requested = [&online_state]() noexcept {
        const bool abort_requested =
            online_state.abort_requested.load(
                std::memory_order_acquire);
        return abort_requested ||
               (g_stop_requested != 0 &&
                !online_state.promoted.load(
                    std::memory_order_acquire));
    };
    online_config.preview_live_status =
        [&preview_pipeline]() noexcept {
            return preview_pipeline->LiveStatus();
        };
    online_config.control_planes_healthy =
        [&preview_ipc_service,
         &recovered_ipc_service]() noexcept {
            return !preview_ipc_service->failed() &&
                   !recovered_ipc_service->failed();
        };
    online_config.preview_outstanding_high_water_records =
        kOnlineRecoveryPreviewOutstandingHighWaterRecords;
    online_config.certified_high_watermark_percent =
        options
            .intraday_recovery_certified_high_watermark_percent;
    if (certified_service != nullptr) {
        online_config.certified_pressure_sample =
            [certified_service]() noexcept {
                const ipc::RealtimeCertifiedServiceSnapshotV1 snapshot =
                    certified_service->Snapshot();
                recovery::OnlineRecoveryCertifiedPressureV1 result{};
                result.healthy =
                    CertifiedServiceTerminalHealthy(snapshot);
                const std::uint64_t maximum =
                    std::numeric_limits<std::uint64_t>::max();
                const std::uint64_t enqueued =
                    snapshot.enqueued_observations >
                            maximum -
                                snapshot.enqueued_applied_records
                        ? maximum
                        : snapshot.enqueued_observations +
                              snapshot.enqueued_applied_records;
                const std::uint64_t pending =
                    enqueued > snapshot.processed_handoffs
                        ? enqueued - snapshot.processed_handoffs
                        : 0U;
                const std::uint64_t capacity =
                    certified_service->handoff_queue_capacity();
                if (capacity == 0U || pending >= capacity) {
                    result.utilization_percent = 100U;
                    return result;
                }
                result.utilization_percent =
                    static_cast<std::uint32_t>(
                    (pending * 100U) / capacity);
                return result;
            };
    }
    std::unique_ptr<recovery::OnlineRecoveryHandoffV1> handoff;
    detail.clear();
    const recovery::OnlineRecoveryErrorV1 handoff_error =
        recovery::OnlineRecoveryHandoffV1::Create(
            std::move(online_config), &handoff, &detail);
    if (handoff_error != recovery::OnlineRecoveryErrorV1::kNone ||
        handoff == nullptr) {
        std::cerr
            << "mdl-production-router: online recovery handoff create "
               "failed: "
            << recovery::OnlineRecoveryErrorNameV1(handoff_error)
            << (detail.empty() ? "" : ": ") << detail << '\n';
        preview_pipeline->StopAndDrain();
        shadow_pipeline->StopAndDrain();
        if (certified_service != nullptr) {
            certified_service->StopControl();
        }
        recovered_ipc_service->MarkFailed();
        preview_ipc_service->MarkFailed();
        static_cast<void>(live_journal->StopAndFlush());
        return 1;
    }

    struct OnlineStartupHealth final {
        bool terminal_healthy = false;
        std::uint64_t preview_lag_records =
            std::numeric_limits<std::uint64_t>::max();
    };
    const auto sample_online_startup_health = [&]() noexcept {
        const runtime::RealtimePipelineLiveStatusV1 preview =
            preview_pipeline->LiveStatus();
        const runtime::RealtimePipelineLiveStatusV1 shadow =
            shadow_pipeline->LiveStatus();
        const recovery::LiveJournalSnapshotV1 journal =
            live_journal->Snapshot();
        bool certified_healthy = true;
        if (certified_service != nullptr) {
            const ipc::RealtimeCertifiedServiceSnapshotV1 certified =
                certified_service->Snapshot();
            certified_healthy =
                CertifiedServiceTerminalHealthy(certified);
        }
        OnlineStartupHealth result{};
        result.preview_lag_records =
            preview.processing_progress.processing_lag_records();
        result.terminal_healthy =
            g_stop_requested == 0 && preview.healthy() &&
            shadow.healthy() && certified_healthy && journal.healthy() &&
            journal.state == recovery::LiveJournalStateV1::kWriting &&
            !preview_ipc_service->failed() &&
            !recovered_ipc_service->failed();
        return result;
    };

    const auto interval =
        std::chrono::milliseconds(options.generation_interval_ms);
    const auto timeout =
        std::chrono::milliseconds(options.generation_timeout_ms);
    std::mutex recovery_start_mutex;
    std::condition_variable recovery_start_cv;
    bool recovery_start_released = false;
    bool recovery_start_cancelled = false;
    std::thread recovery_thread;
    const auto cancel_recovery_start = [&]() noexcept {
        {
            std::lock_guard<std::mutex> lock(recovery_start_mutex);
            recovery_start_cancelled = true;
        }
        recovery_start_cv.notify_all();
    };
    const auto release_recovery_start = [&]() noexcept {
        {
            std::lock_guard<std::mutex> lock(recovery_start_mutex);
            recovery_start_released = true;
        }
        recovery_start_cv.notify_all();
    };
    const auto fail_online_startup = [&]() noexcept {
        // StartLivePartial may already have succeeded before the second health
        // sample.  Publish FAILED first so no client can acquire or continue
        // reading a session while producer shutdown and queue joins run.
        preview_ipc_service->MarkFailed();
        recovered_ipc_service->MarkFailed();
        cancel_recovery_start();
        if (recovery_thread.joinable()) {
            recovery_thread.join();
        }
        preview_pipeline->StopAndDrain();
        shadow_pipeline->StopAndDrain();
        if (certified_service != nullptr) {
            certified_service->StopControl();
        }
        preview_ipc_service->StopControl();
        static_cast<void>(live_journal->StopAndFlush());
    };
    try {
        recovery_thread = std::thread(
            [&online_state,
             &handoff,
             &live_journal,
             &preview_pipeline,
             &preview_ipc_service,
             &shadow_pipeline,
             &recovered_run_id,
             &recovered_ipc_service,
             &certified_service,
             &online_exposure_gate,
             &recovery_start_mutex,
             &recovery_start_cv,
             &recovery_start_released,
             &recovery_start_cancelled,
             recovery_warmup_timeout,
             timeout]() noexcept {
            {
                std::unique_lock<std::mutex> lock(
                    recovery_start_mutex);
                recovery_start_cv.wait(
                    lock,
                    [&]() noexcept {
                        return recovery_start_released ||
                               recovery_start_cancelled;
                    });
                if (recovery_start_cancelled) {
                    return;
                }
            }
            const auto recovery_cancelled = [&online_state]() noexcept {
                return g_stop_requested != 0 ||
                       online_state.abort_requested.load(
                           std::memory_order_acquire);
            };
            const auto report_handoff_failure =
                [&online_state,
                 &recovered_ipc_service,
                 &recovery_cancelled](
                    recovery::OnlineRecoveryErrorV1 error,
                    const std::string& failure_detail) {
                    // Once shutdown has won the pre-promotion decision,
                    // owner-stop and journal-End observations are consequences
                    // of cancellation rather than a second startup failure.
                    if (recovery_cancelled()) {
                        return;
                    }
                    online_state.Fail(
                        "background recovery failed before promotion: " +
                        std::string(
                            recovery::OnlineRecoveryErrorNameV1(error)) +
                        (failure_detail.empty()
                             ? std::string()
                             : ": " + failure_detail));
                    recovered_ipc_service->MarkFailed();
                };

            // Every cancellable wait shares one absolute budget across CSV,
            // candidate catch-up, applied-prefix drains, probes, the cut, and
            // final commit. Candidate retries therefore cannot extend
            // recovery indefinitely. Cut setup/calculator work and a final
            // commit tail that already owns Completing retain their documented
            // non-preemptible lifecycle semantics.
            recovery::OnlineRecoveryCandidateV1 candidate =
                handoff->PrepareInitialCandidate(
                    std::chrono::steady_clock::now() +
                    recovery_warmup_timeout);
            if (!candidate.ready()) {
                report_handoff_failure(
                    candidate.error, candidate.detail);
                return;
            }
            const std::uint64_t initial_candidate_journal_frontier =
                candidate.journal_frontier;
            const auto warmup_deadline = candidate.warmup_deadline;
            const auto operation_deadline =
                [warmup_deadline, timeout]() noexcept {
                    return std::min(
                        warmup_deadline,
                        std::chrono::steady_clock::now() + timeout);
                };
            const auto prefix_operation_timeout =
                [warmup_deadline,
                 timeout](std::chrono::milliseconds* output) noexcept {
                    if (output == nullptr) {
                        return false;
                    }
                    const auto now =
                        std::chrono::steady_clock::now();
                    if (now >= warmup_deadline) {
                        return false;
                    }
                    const auto remaining =
                        std::chrono::duration_cast<
                            std::chrono::milliseconds>(
                            warmup_deadline - now);
                    if (remaining <=
                        std::chrono::milliseconds::zero()) {
                        return false;
                    }
                    *output = std::min(timeout, remaining);
                    return *output >
                           std::chrono::milliseconds::zero();
                };

            std::uint64_t prefix_probe_count = 0U;
            ipc::RealtimeCertifiedPrefixFenceResultV1
                probed_prefix{};
            if (certified_service != nullptr) {
                for (;;) {
                    // IngestExternalMessage returns after decoder admission,
                    // not after History/CERTIFIED handoff.  Waiting for the
                    // shadow applied frontier before enqueueing the FIFO
                    // probe prevents the probe from overtaking this exact
                    // candidate prefix.
                    bool applied = false;
                    while (!applied) {
                        if (recovery_cancelled()) {
                            report_handoff_failure(
                                recovery::OnlineRecoveryErrorV1::
                                    kCancelled,
                                "online recovery was cancelled while "
                                "waiting for the shadow applied prefix");
                            return;
                        }
                        const runtime::RealtimePipelineLiveStatusV1
                            before_wait =
                                shadow_pipeline->LiveStatus();
                        if (!before_wait.healthy() ||
                            before_wait.processing_progress
                                    .accepted_sequence <
                                candidate
                                    .shadow_ingress_frontier) {
                            online_state.Fail(
                                "shadow pipeline is not healthy at a "
                                "recovery candidate boundary");
                            recovered_ipc_service->MarkFailed();
                            certified_service->StopControl();
                            return;
                        }
                        applied = shadow_pipeline
                                      ->WaitAppliedThroughPrefix(
                                          candidate
                                              .shadow_ingress_frontier,
                                          operation_deadline());
                        if (applied) {
                            const runtime::RealtimePipelineLiveStatusV1
                                after_wait =
                                    shadow_pipeline->LiveStatus();
                            if (!after_wait.healthy() ||
                                after_wait.processing_progress
                                        .accepted_sequence <
                                    candidate
                                        .shadow_ingress_frontier ||
                                after_wait.processing_progress
                                        .applied_sequence <
                                    candidate
                                        .shadow_ingress_frontier) {
                                online_state.Fail(
                                    "shadow pipeline lost the recovery "
                                    "candidate prefix after applied wait");
                                recovered_ipc_service->MarkFailed();
                                certified_service->StopControl();
                                return;
                            }
                            break;
                        }
                        const runtime::RealtimePipelineLiveStatusV1
                            shadow_status =
                                shadow_pipeline->LiveStatus();
                        if (!shadow_status.healthy() ||
                            shadow_status.processing_progress
                                    .accepted_sequence <
                                candidate
                                    .shadow_ingress_frontier) {
                            online_state.Fail(
                                "shadow pipeline failed while waiting "
                                "for a recovery candidate prefix");
                            recovered_ipc_service->MarkFailed();
                            certified_service->StopControl();
                            return;
                        }
                        if (std::chrono::steady_clock::now() >=
                            warmup_deadline) {
                            online_state.Fail(
                                "online recovery warmup expired while "
                                "waiting for the shadow applied prefix");
                            recovered_ipc_service->MarkFailed();
                            certified_service->StopControl();
                            return;
                        }
                    }

                    std::chrono::milliseconds probe_timeout{0};
                    if (!prefix_operation_timeout(&probe_timeout)) {
                        online_state.Fail(
                            "online recovery warmup expired before "
                            "CERTIFIED prefix probe");
                        recovered_ipc_service->MarkFailed();
                        certified_service->StopControl();
                        return;
                    }
                    int certified_error = 0;
                    probed_prefix = {};
                    const auto probe_error =
                        certified_service->ProbePrefixFence(
                            probe_timeout,
                            &probed_prefix,
                            &certified_error);
                    if (probe_error ==
                        ipc::RealtimeCertifiedPrefixFenceOperationErrorV1::
                            kTimedOut) {
                        if (std::chrono::steady_clock::now() <
                            warmup_deadline) {
                            continue;
                        }
                    }
                    if (probe_error !=
                        ipc::RealtimeCertifiedPrefixFenceOperationErrorV1::
                            kNone) {
                        online_state.Fail(
                            "CERTIFIED recovery prefix probe failed: " +
                            std::string(
                                ipc::
                                    RealtimeCertifiedPrefixFenceOperationErrorNameV1(
                                        probe_error)) +
                            " errno=" +
                            std::to_string(certified_error));
                        recovered_ipc_service->MarkFailed();
                        certified_service->StopControl();
                        return;
                    }
                    ++prefix_probe_count;
                    if (probed_prefix.ready()) {
                        break;
                    }
                    if (!probed_prefix.retryable()) {
                        online_state.Fail(
                            "CERTIFIED recovery prefix is terminal: "
                            "state=" +
                            std::to_string(static_cast<std::uint32_t>(
                                probed_prefix.state)));
                        recovered_ipc_service->MarkFailed();
                        certified_service->StopControl();
                        return;
                    }

                    // Preserve the first healthy journal prefix.  In
                    // particular, if B+1 repairs the current gap while B+2
                    // opens a new one, promotion must freeze at B+1 rather
                    // than batching through B+2.
                    for (;;) {
                        if (recovery_cancelled()) {
                            report_handoff_failure(
                                recovery::OnlineRecoveryErrorV1::
                                    kCancelled,
                                "online recovery was cancelled while "
                                "waiting for a native repair");
                            return;
                        }
                        const auto now =
                            std::chrono::steady_clock::now();
                        if (now >= warmup_deadline) {
                            online_state.Fail(
                                "online recovery warmup expired while "
                                "waiting for a native repair");
                            recovered_ipc_service->MarkFailed();
                            certified_service->StopControl();
                            return;
                        }
                        const recovery::
                            OnlineRecoveryCandidateAdvanceV1 advance =
                                handoff->CatchUpOneBeforePromotion(
                                    std::min(
                                        warmup_deadline,
                                        now +
                                            std::chrono::milliseconds(
                                                100)));
                        if (advance.disposition ==
                            recovery::
                                OnlineRecoveryCandidateAdvanceDispositionV1::
                                    kIdle) {
                            continue;
                        }
                        if (advance.disposition !=
                                recovery::
                                    OnlineRecoveryCandidateAdvanceDispositionV1::
                                        kAdvanced ||
                            !advance.candidate.ready()) {
                            report_handoff_failure(
                                advance.error,
                                advance.detail.empty()
                                    ? advance.candidate.detail
                                    : advance.detail);
                            return;
                        }
                        candidate = advance.candidate;
                        break;
                    }
                }
            }

            const recovery::OnlineRecoveryBoundaryV1 boundary =
                handoff->FreezePromotionBoundary();
            if (!boundary.ready()) {
                report_handoff_failure(
                    boundary.error, boundary.detail);
                return;
            }
            const auto preview_owner_healthy =
                [&preview_pipeline,
                 &preview_ipc_service]() noexcept -> bool {
                    return preview_pipeline->LiveIngressHealthy() &&
                           !preview_ipc_service->failed();
                };
            const auto promotion_inputs_healthy =
                [&live_journal,
                 &preview_owner_healthy,
                 &recovered_ipc_service,
                 &certified_service,
                 &shadow_pipeline,
                 &boundary](bool require_certified_control_running)
                    noexcept -> bool {
                    const recovery::LiveJournalSnapshotV1 journal =
                        live_journal->Snapshot();
                    const runtime::RealtimePipelineLiveStatusV1 shadow =
                        shadow_pipeline->LiveStatus();
                    bool certified_healthy = true;
                    if (certified_service != nullptr) {
                        const ipc::RealtimeCertifiedServiceSnapshotV1
                            certified = certified_service->Snapshot();
                        certified_healthy =
                            CertifiedServiceTerminalHealthy(
                                certified,
                                require_certified_control_running) &&
                            (!require_certified_control_running ||
                             certified_service
                                 ->ControlRunningConfirmed());
                    }
                    return journal.healthy() &&
                           journal.state ==
                               recovery::LiveJournalStateV1::kWriting &&
                           journal.accepted_serial >=
                               boundary.journal_frontier &&
                           journal.committed_serial >=
                               boundary.journal_frontier &&
                           shadow.healthy() &&
                           shadow.processing_progress.accepted_sequence >=
                               boundary.shadow_ingress_frontier &&
                           shadow.processing_progress.applied_sequence >=
                               boundary.shadow_ingress_frontier &&
                           preview_owner_healthy() &&
                           !recovered_ipc_service->failed() &&
                           certified_healthy;
                };
            const auto cut_now = std::chrono::steady_clock::now();
            if (cut_now >= warmup_deadline) {
                online_state.Fail(
                    "online recovery warmup expired before the final "
                    "generation cut");
                recovered_ipc_service->MarkFailed();
                if (certified_service != nullptr) {
                    certified_service->StopControl();
                }
                return;
            }
            const auto cut_timeout = std::min(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    timeout),
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    warmup_deadline - cut_now));
            const runtime::RealtimePipelineCutResultV1 cut =
                shadow_pipeline->CutAndPublishGeneration(cut_timeout);
            const bool cut_matches_candidate =
                cut.published() &&
                cut.store_generation->watermark()
                        .processing_progress.accepted_sequence ==
                    boundary.shadow_ingress_frontier &&
                cut.store_generation->watermark()
                        .processing_progress.applied_sequence ==
                    boundary.shadow_ingress_frontier;
            if (!cut_matches_candidate ||
                !PublishKLineGeneration(
                    cut,
                    recovered_ipc_service,
                    "online recovery promotion")) {
                online_state.Fail(
                    "shadow promotion generation failed: " +
                    std::string(
                        runtime::RealtimePipelineCutErrorNameV1(
                            cut.error)) +
                    (cut.published() && !cut_matches_candidate
                         ? ": promotion watermark does not match the "
                           "frozen shadow prefix"
                         : std::string()));
                recovered_ipc_service->MarkFailed();
                return;
            }
            ipc::CertifiedOrderEventHistoryGenerationV1
                promoted_event_generation{};
            if (certified_service != nullptr) {
                std::chrono::milliseconds barrier_timeout{0};
                if (!prefix_operation_timeout(&barrier_timeout)) {
                    online_state.Fail(
                        "online recovery warmup expired before the final "
                        "CERTIFIED prefix commit");
                    recovered_ipc_service->MarkFailed();
                    certified_service->StopControl();
                    return;
                }
                int certified_error = 0;
                ipc::RealtimeCertifiedPrefixFenceResultV1
                    committed_prefix{};
                const bool prefix_committed =
                    certified_service->WaitForPrefixBarrier(
                        barrier_timeout,
                        &committed_prefix,
                        &certified_error);
                bool committed_prefix_matches_probe = false;
                if (prefix_committed && committed_prefix.ready() &&
                    committed_prefix.event_history.valid() &&
                    probed_prefix.ready() &&
                    probed_prefix.event_history.valid()) {
                    const auto committed_event =
                        committed_prefix.event_history.generation();
                    const auto probed_event =
                        probed_prefix.event_history.generation();
                    committed_prefix_matches_probe =
                        committed_prefix.state == probed_prefix.state &&
                        committed_prefix.canonical_apply_frontier ==
                            probed_prefix.canonical_apply_frontier &&
                        committed_prefix.tick_journal_frontier ==
                            committed_prefix
                                .canonical_apply_frontier &&
                        committed_prefix.tick_journal_frontier ==
                            probed_prefix.tick_journal_frontier &&
                        committed_prefix.correction_epoch ==
                            probed_prefix.correction_epoch &&
                        committed_prefix.event_journal_frontier ==
                            probed_prefix.event_journal_frontier &&
                        committed_prefix.event_published_sequence ==
                            probed_prefix.event_published_sequence &&
                        committed_event.input_frontier
                                .canonical_apply_sequence ==
                            probed_event.input_frontier
                                .canonical_apply_sequence &&
                        committed_event.event_count ==
                            probed_event.event_count;
                }
                if (!committed_prefix_matches_probe) {
                    online_state.Fail(
                        "CERTIFIED promotion prefix barrier failed or "
                        "changed the frozen semantic prefix: operation=" +
                        std::string(
                            ipc::
                                RealtimeCertifiedPrefixFenceOperationErrorNameV1(
                                    committed_prefix.operation_error)) +
                        " state=" +
                        std::to_string(static_cast<std::uint32_t>(
                            committed_prefix.state)) +
                        " errno=" +
                        std::to_string(certified_error));
                    recovered_ipc_service->MarkFailed();
                    certified_service->StopControl();
                    return;
                }
                promoted_event_generation =
                    committed_prefix.event_history.generation();
            }
            std::uint64_t promotion_ns = 0U;
            {
                std::lock_guard<std::mutex> promotion(
                    online_state.promotion_mutex);
                if (g_stop_requested != 0 ||
                    online_state.abort_requested.load(
                        std::memory_order_acquire)) {
                    return;
                }
                if (std::chrono::steady_clock::now() >=
                    warmup_deadline) {
                    online_state.Fail(
                        "online recovery warmup expired before "
                        "promotion exposure");
                    recovered_ipc_service->MarkFailed();
                    if (certified_service != nullptr) {
                        certified_service->StopControl();
                    }
                    return;
                }
                if (!promotion_inputs_healthy(false)) {
                    online_state.Fail(
                        "online journal/preview owner failed before "
                        "promotion exposure");
                    preview_ipc_service->MarkFailed();
                    recovered_ipc_service->MarkFailed();
                    if (certified_service != nullptr) {
                        certified_service->StopControl();
                    }
                    return;
                }
                int start_error = 0;
                // The CERTIFIED prefix is already sealed. Publish its FAST
                // capability while FAST is still INITIALIZING, then start both
                // controls behind their shared false gate. No request can
                // receive either descriptor during these fallible steps.
                if (certified_service != nullptr &&
                    !recovered_ipc_service
                         ->PrepareCertifiedPrefixValidBeforeStart()) {
                    online_state.Fail(
                        "recovered FAST certified-prefix preparation failed");
                    recovered_ipc_service->MarkFailed();
                    certified_service->StopControl();
                    return;
                }
                if (certified_service != nullptr &&
                    !certified_service->StartControl(&start_error)) {
                    online_state.Fail(
                        "CERTIFIED control activation failed: errno=" +
                        std::to_string(start_error));
                    recovered_ipc_service->MarkFailed();
                    certified_service->StopControl();
                    return;
                }
                start_error = 0;
                if (!recovered_ipc_service->Start(&start_error)) {
                    online_state.Fail(
                        "recovered FAST activation failed: errno=" +
                        std::to_string(start_error));
                    recovered_ipc_service->MarkFailed();
                    if (certified_service != nullptr) {
                        certified_service->StopControl();
                    }
                    return;
                }
                if (std::chrono::steady_clock::now() >=
                    warmup_deadline) {
                    online_state.Fail(
                        "online recovery warmup expired during "
                        "promotion control activation");
                    recovered_ipc_service->MarkFailed();
                    if (certified_service != nullptr) {
                        certified_service->StopControl();
                    }
                    return;
                }
                if (!promotion_inputs_healthy(true)) {
                    online_state.Fail(
                        "online journal/preview owner failed during "
                        "promotion exposure");
                    preview_ipc_service->MarkFailed();
                    recovered_ipc_service->MarkFailed();
                    if (certified_service != nullptr) {
                        certified_service->StopControl();
                    }
                    return;
                }
                if (!CurrentRealtimeNs(&promotion_ns) ||
                    !handoff->MarkPromoted(promotion_ns)) {
                    online_state.Fail(
                        "promotion completion timestamp publication failed");
                    recovered_ipc_service->MarkFailed();
                    if (certified_service != nullptr) {
                        certified_service->StopControl();
                    }
                    return;
                }
                // This is the only external exposure linearization point.
                // Both accept loops check the same acquire-loaded atomic before
                // dispatching any request or transferring any descriptor.
                online_exposure_gate->store(
                    true, std::memory_order_release);
                // The main thread starts periodic shadow cuts only after it
                // observes this flag. Publish it after the exposure gate so a
                // periodic cut can never race the final gate transition and
                // make an already-failed shadow externally visible.
                online_state.promoted.store(
                    true, std::memory_order_release);
            }
            std::cerr
                << "mdl-production-router: online recovery promoted: "
                << "socket="
                << recovered_ipc_service->control_socket_path()
                << " run_id="
                << common::Identity128Hex(recovered_run_id)
                << " initial_journal_frontier="
                << initial_candidate_journal_frontier
                << " journal_frontier=" << boundary.journal_frontier
                << " prepromotion_journal_extension="
                << (boundary.journal_frontier >=
                            initial_candidate_journal_frontier
                        ? boundary.journal_frontier -
                              initial_candidate_journal_frontier
                        : 0U)
                << " shadow_ingress_frontier="
                << boundary.shadow_ingress_frontier
                << " promotion_realtime_ns=" << promotion_ns
                << " coverage_from_open=true"
                << " startup_prefix_recovered=true"
                << " full_day_kline_valid="
                << (cut.kline_enabled ? "true" : "false")
                << " full_day_factor_valid=true"
                << " certified_prefix_valid="
                << (certified_service != nullptr ? "true" : "false")
                << " event_service="
                << (certified_service != nullptr
                        ? "CERTIFIED_CANONICAL"
                        : "DISABLED")
                << " event_visibility="
                << (certified_service != nullptr
                        ? "AT_SHARED_PROMOTION_GATE"
                        : "UNAVAILABLE")
                << " event_prefix_barrier="
                << (certified_service != nullptr ? "true" : "false")
                << " event_prefix_probes=" << prefix_probe_count
                << " event_generation="
                << promoted_event_generation.generation
                << " event_count="
                << promoted_event_generation.event_count
                << " event_tick_frontier="
                << promoted_event_generation.input_frontier
                       .tick_stream_sequence
                << '\n';

            for (;;) {
                // PumpNext's journal-tail pressure gate samples and fail-closes
                // the journal, both FAST control planes, both Pipelines, and
                // CERTIFIED before every read, including one that ultimately
                // returns idle. Do not sample
                // preview LiveStatus a second time per permanent tail record:
                // that would add needless reads of the exposed FAST progress
                // cache lines for the rest of the process lifetime.
                const recovery::OnlineRecoveryPumpResultV1 pumped =
                    handoff->PumpNext(
                        std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(100));
                if (pumped.disposition ==
                        recovery::OnlineRecoveryPumpDispositionV1::
                            kRecord ||
                    pumped.disposition ==
                        recovery::OnlineRecoveryPumpDispositionV1::kIdle) {
                    continue;
                }
                if (pumped.disposition ==
                    recovery::OnlineRecoveryPumpDispositionV1::kEnd) {
                    return;
                }
                online_state.Fail(
                    "post-promotion journal tail failed: " +
                    std::string(
                        recovery::OnlineRecoveryErrorNameV1(
                            pumped.error)) +
                    (pumped.detail.empty()
                         ? std::string()
                         : ": " + pumped.detail));
                preview_ipc_service->MarkFailed();
                recovered_ipc_service->MarkFailed();
                if (certified_service != nullptr) {
                    certified_service->StopControl();
                }
                return;
            }
        });
    } catch (...) {
        std::cerr
            << "mdl-production-router: online recovery thread create failed\n";
        fail_online_startup();
        return 1;
    }

    bool preview_drained = false;
    std::uint64_t preview_start_lag =
        std::numeric_limits<std::uint64_t>::max();
    const auto preview_drain_deadline =
        std::chrono::steady_clock::now() + timeout;
    for (;;) {
        const OnlineStartupHealth health =
            sample_online_startup_health();
        preview_start_lag = health.preview_lag_records;
        if (!health.terminal_healthy) {
            break;
        }
        if (preview_start_lag <
            kOnlineRecoveryPreviewOutstandingHighWaterRecords) {
            preview_drained = true;
            break;
        }
        const auto now = std::chrono::steady_clock::now();
        if (now >= preview_drain_deadline) {
            break;
        }
        std::this_thread::sleep_until(std::min(
            preview_drain_deadline,
            now + kOnlineRecoveryPreviewDrainPollInterval));
    }
    if (!preview_drained) {
        std::cerr
            << "mdl-production-router: LIVE_PARTIAL pre-exposure drain "
               "failed: lag_records="
            << preview_start_lag << '\n';
        fail_online_startup();
        return 1;
    }

    ipc_system_error = 0;
    if (!preview_ipc_service->StartLivePartial(&ipc_system_error)) {
        std::cerr
            << "mdl-production-router: LIVE_PARTIAL exposure rejected: "
            << "errno=" << ipc_system_error << '\n';
        fail_online_startup();
        return 1;
    }
    const OnlineStartupHealth post_start_health =
        sample_online_startup_health();
    if (!post_start_health.terminal_healthy) {
        std::cerr
            << "mdl-production-router: LIVE_PARTIAL post-exposure health "
               "check failed\n";
        fail_online_startup();
        return 1;
    }
    std::cerr
        << "mdl-production-router: LIVE_PARTIAL available: socket="
        << preview_ipc_service->control_socket_path()
        << " run_id=" << common::Identity128Hex(preview_run_id)
        << " server_state=LIVE_PARTIAL"
        << " coverage_from_open=false"
        << " startup_prefix_recovered=false"
        << " full_day_kline_valid=false"
        << " full_day_factor_valid=false"
        << " certified_prefix_valid=false"
        << " history_control=unavailable"
        << " journal_dir="
        << options.intraday_recovery_journal_dir
        << " startup_lag_records="
        << post_start_health.preview_lag_records
        << '\n';

    const auto recovery_progress_interval =
        std::chrono::seconds(
            options.intraday_recovery_progress_interval_seconds);
    const auto recovery_progress_started =
        std::chrono::steady_clock::now();
    auto recovery_progress_last_sample =
        recovery_progress_started;
    auto recovery_progress_next_sample =
        recovery_progress_started + recovery_progress_interval;
    recovery::OnlineRecoverySnapshotV1
        recovery_progress_last_snapshot = handoff->Snapshot();
    release_recovery_start();

    int exit_code = 0;
    while (g_stop_requested == 0) {
        const IntervalWaitResult wait =
            WaitForInterval(interval, options.trade_date);
        if (wait == IntervalWaitResult::kSignal) {
            break;
        }
        if (wait != IntervalWaitResult::kElapsed) {
            std::cerr
                << "mdl-production-router: online runtime date/clock "
                   "boundary failed\n";
            exit_code = 1;
            break;
        }
        const recovery::LiveJournalSnapshotV1 journal_snapshot =
            live_journal->Snapshot();
        if (preview_pipeline->fatal() ||
            preview_ipc_service->failed() ||
            !journal_snapshot.healthy()) {
            std::cerr
                << "mdl-production-router: LIVE_PARTIAL/journal failed: "
                << recovery::LiveJournalErrorNameV1(
                       journal_snapshot.error)
                << " accepted=" << journal_snapshot.accepted_serial
                << " committed=" << journal_snapshot.committed_serial
                << '\n';
            exit_code = 1;
            break;
        }
        if (online_state.failed.load(std::memory_order_acquire)) {
            std::cerr
                << "mdl-production-router: " << online_state.Detail()
                << '\n';
            exit_code = 1;
            break;
        }
        if (!online_state.promoted.load(std::memory_order_acquire)) {
            const auto progress_now =
                std::chrono::steady_clock::now();
            if (progress_now >= recovery_progress_next_sample) {
                const recovery::OnlineRecoverySnapshotV1
                    recovery_snapshot = handoff->Snapshot();
                const runtime::RealtimePipelineLiveStatusV1
                    preview_status = preview_pipeline->LiveStatus();
                const runtime::RealtimePipelineLiveStatusV1
                    shadow_status = shadow_pipeline->LiveStatus();
                const auto sample_ms_count =
                    std::chrono::duration_cast<
                        std::chrono::milliseconds>(
                        progress_now -
                        recovery_progress_last_sample)
                        .count();
                const std::uint64_t sample_ms =
                    sample_ms_count > 0
                        ? static_cast<std::uint64_t>(sample_ms_count)
                        : 1U;
                const auto elapsed_ms_count =
                    std::chrono::duration_cast<
                        std::chrono::milliseconds>(
                        progress_now - recovery_progress_started)
                        .count();
                const std::uint64_t elapsed_ms =
                    elapsed_ms_count > 0
                        ? static_cast<std::uint64_t>(elapsed_ms_count)
                        : 0U;
                const auto difference = [](
                                            std::uint64_t current,
                                            std::uint64_t previous) noexcept {
                    return current >= previous
                               ? current - previous
                               : 0U;
                };
                const auto rate_per_second =
                    [sample_ms, &difference](
                        std::uint64_t current,
                        std::uint64_t previous) noexcept {
                        const std::uint64_t delta =
                            difference(current, previous);
                        if (delta >
                            std::numeric_limits<std::uint64_t>::max() /
                                1'000U) {
                            return std::numeric_limits<std::uint64_t>::max();
                        }
                        return (delta * 1'000U) / sample_ms;
                    };
                const recovery::StartupReplayCountsV1& csv_counts =
                    recovery_snapshot.csv_publications_by_message;
                const recovery::StartupReplayCountsV1& prior_csv_counts =
                    recovery_progress_last_snapshot
                        .csv_publications_by_message;
                const recovery::StartupReplayBoundaryAlignmentStatsV1&
                    alignment =
                        recovery_snapshot.csv_boundary_alignment;

                ipc::RealtimeCertifiedServiceSnapshotV1
                    certified_snapshot{};
                std::uint64_t certified_pending = 0U;
                if (certified_service != nullptr) {
                    certified_snapshot = certified_service->Snapshot();
                    const std::uint64_t maximum =
                        std::numeric_limits<std::uint64_t>::max();
                    const std::uint64_t certified_enqueued =
                        certified_snapshot.enqueued_observations >
                                maximum -
                                    certified_snapshot
                                        .enqueued_applied_records
                            ? maximum
                            : certified_snapshot.enqueued_observations +
                                  certified_snapshot
                                      .enqueued_applied_records;
                    certified_pending = difference(
                        certified_enqueued,
                        certified_snapshot.processed_handoffs);
                }

                std::cerr
                    << "mdl-production-router: online recovery progress: "
                    << "schema=1"
                    << " elapsed_ms=" << elapsed_ms
                    << " warmup_budget_seconds="
                    << options.intraday_recovery_warmup_seconds
                    << " phase="
                    << recovery::OnlineRecoveryPhaseNameV1(
                           recovery_snapshot.phase)
                    << " error="
                    << recovery::OnlineRecoveryErrorNameV1(
                           recovery_snapshot.error)
                    << " csv_complete="
                    << (recovery_snapshot.csv_complete ? "true" : "false")
                    << " csv_publications="
                    << recovery_snapshot.csv_publications
                    << " csv_rate_records_per_second="
                    << rate_per_second(
                           recovery_snapshot.csv_publications,
                           recovery_progress_last_snapshot
                               .csv_publications)
                    << " csv_shanghai_snapshots="
                    << csv_counts.shanghai_snapshots
                    << " csv_delta_shanghai_snapshots="
                    << difference(
                           csv_counts.shanghai_snapshots,
                           prior_csv_counts.shanghai_snapshots)
                    << " csv_shanghai_ticks="
                    << csv_counts.shanghai_ticks
                    << " csv_delta_shanghai_ticks="
                    << difference(
                           csv_counts.shanghai_ticks,
                           prior_csv_counts.shanghai_ticks)
                    << " csv_shenzhen_snapshots="
                    << csv_counts.shenzhen_snapshots
                    << " csv_delta_shenzhen_snapshots="
                    << difference(
                           csv_counts.shenzhen_snapshots,
                           prior_csv_counts.shenzhen_snapshots)
                    << " csv_shenzhen_orders="
                    << csv_counts.shenzhen_orders
                    << " csv_delta_shenzhen_orders="
                    << difference(
                           csv_counts.shenzhen_orders,
                           prior_csv_counts.shenzhen_orders)
                    << " csv_shenzhen_transactions="
                    << csv_counts.shenzhen_transactions
                    << " csv_delta_shenzhen_transactions="
                    << difference(
                           csv_counts.shenzhen_transactions,
                           prior_csv_counts.shenzhen_transactions)
                    << " csv_filtered="
                    << recovery_snapshot.csv_filtered_publications
                    << " csv_parser_checkpoints="
                    << recovery_snapshot.csv_parser_checkpoint_events
                    << " preview_accepted="
                    << preview_status.processing_progress.accepted_sequence
                    << " preview_applied="
                    << preview_status.processing_progress.applied_sequence
                    << " preview_lag="
                    << preview_status.processing_progress
                           .processing_lag_records()
                    << " shadow_accepted="
                    << shadow_status.processing_progress.accepted_sequence
                    << " shadow_applied="
                    << shadow_status.processing_progress.applied_sequence
                    << " shadow_lag="
                    << shadow_status.processing_progress
                           .processing_lag_records()
                    << " journal_accepted="
                    << journal_snapshot.accepted_serial
                    << " journal_committed="
                    << journal_snapshot.committed_serial
                    << " journal_consumed="
                    << recovery_snapshot.last_journal_serial
                    << " journal_durable_backlog="
                    << difference(
                           journal_snapshot.accepted_serial,
                           journal_snapshot.committed_serial)
                    << " journal_capture_backlog="
                    << difference(
                           journal_snapshot.accepted_serial,
                           recovery_snapshot.last_journal_serial)
                    << " candidate_ready="
                    << (recovery_snapshot.candidate_boundary_ready
                            ? "true"
                            : "false")
                    << " candidate_journal_frontier="
                    << recovery_snapshot.candidate_journal_frontier
                    << " candidate_shadow_frontier="
                    << recovery_snapshot
                           .candidate_shadow_ingress_frontier
                    << " promotion_boundary_ready="
                    << (recovery_snapshot.promotion_boundary_ready
                            ? "true"
                            : "false")
                    << " alignment_phase="
                    << recovery::
                           StartupReplayBoundaryAlignmentPhaseNameV1(
                               alignment.phase)
                    << " alignment_attempted="
                    << (alignment.attempted ? "true" : "false")
                    << " alignment_sealed="
                    << (alignment.sealed ? "true" : "false")
                    << " alignment_wait_ms=" << alignment.waited_ms
                    << " alignment_pending_messages="
                    << alignment.pending_messages
                    << " alignment_pending_channels="
                    << alignment.pending_channels
                    << " alignment_missing_channel="
                    << alignment.representative_missing_channel
                    << " alignment_missing_appl_seq="
                    << alignment.representative_missing_appl_seq
                    << " order_initial_cut="
                    << alignment.initial_order_cut
                    << " order_sealed_cut="
                    << alignment.sealed_order_cut
                    << " transaction_initial_cut="
                    << alignment.initial_transaction_cut
                    << " transaction_sealed_cut="
                    << alignment.sealed_transaction_cut
                    << " certified_enabled="
                    << (certified_service != nullptr ? "true" : "false")
                    << " certified_worker_running="
                    << (certified_snapshot.worker_running
                            ? "true"
                            : "false")
                    << " certified_pending=" << certified_pending
                    << " certified_gap_open_channels="
                    << certified_snapshot.gap_open_channel_count
                    << " certified_catching_up_channels="
                    << certified_snapshot.catching_up_channel_count
                    << " certified_frozen_channels="
                    << certified_snapshot.frozen_channel_count
                    << '\n';

                recovery_progress_last_snapshot = recovery_snapshot;
                recovery_progress_last_sample = progress_now;
                recovery_progress_next_sample =
                    progress_now + recovery_progress_interval;
            }
            continue;
        }
        const runtime::RealtimePipelineCutResultV1 cut =
            shadow_pipeline->CutAndPublishGeneration(timeout);
        if (!cut.published() ||
            !PublishKLineGeneration(
                cut, recovered_ipc_service, "online periodic")) {
            std::cerr
                << "mdl-production-router: online recovered generation "
                   "failed: "
                << runtime::RealtimePipelineCutErrorNameV1(cut.error)
                << '\n';
            exit_code = 1;
            break;
        }
    }

    bool promoted = false;
    {
        std::lock_guard<std::mutex> promotion(
            online_state.promotion_mutex);
        promoted =
            online_state.promoted.load(std::memory_order_acquire);
        if (!promoted || exit_code != 0) {
            online_state.abort_requested.store(
                true, std::memory_order_release);
        }
    }

    if (!preview_ipc_service->failed()) {
        preview_ipc_service->MarkDraining();
    }
    if (promoted && !recovered_ipc_service->failed()) {
        recovered_ipc_service->MarkDraining();
    }
    if (certified_service != nullptr && promoted) {
        certified_service->MarkDraining();
    }
    if (certified_service != nullptr && !promoted) {
        // Abort an in-flight candidate probe/final wait before joining the
        // recovery thread. Prefix calls do not hold the lifecycle mutex while
        // waiting, so stopping the worker wakes them with cancellation instead
        // of delaying shutdown for the full operation timeout.
        certified_service->StopControl();
    }

    // Once promoted, shutdown is a drain rather than cancellation: first
    // authorize the expected preview lifecycle transition, then quiesce the
    // only SDK owner, durably close the journal, and let the recovery thread
    // consume the exact accepted prefix through End.
    const auto clean_tail_deadline =
        std::chrono::steady_clock::now() +
        std::chrono::seconds(
            options.intraday_recovery_backpressure_seconds);
    if (exit_code == 0 && promoted &&
        !handoff->BeginCleanShutdownTailDrain(
            clean_tail_deadline)) {
        std::cerr
            << "mdl-production-router: clean shutdown tail drain "
               "transition failed\n";
        exit_code = 1;
        online_state.abort_requested.store(
            true, std::memory_order_release);
    }

    runtime::RealtimePipelineCutResultV1 preview_final{};
    if (exit_code == 0 && !preview_pipeline->fatal()) {
        preview_final =
            preview_pipeline->StopAndPublishFinalGeneration(timeout);
        if (!preview_final.published()) {
            std::cerr
                << "mdl-production-router: preview final generation failed\n";
            exit_code = 1;
        }
    } else {
        preview_pipeline->StopAndDrain();
    }
    if (!live_journal->StopAndFlush()) {
        const recovery::LiveJournalSnapshotV1 snapshot =
            live_journal->Snapshot();
        std::cerr
            << "mdl-production-router: journal final flush failed: "
            << recovery::LiveJournalErrorNameV1(snapshot.error)
            << " accepted=" << snapshot.accepted_serial
            << " committed=" << snapshot.committed_serial << '\n';
        exit_code = 1;
    }
    if (recovery_thread.joinable()) {
        recovery_thread.join();
    }
    if (online_state.failed.load(std::memory_order_acquire)) {
        std::cerr
            << "mdl-production-router: " << online_state.Detail() << '\n';
        exit_code = 1;
    }

    const recovery::LiveJournalSnapshotV1 drained_journal =
        live_journal->Snapshot();
    const recovery::OnlineRecoverySnapshotV1 drained_recovery =
        handoff->Snapshot();
    if (promoted && exit_code == 0 &&
        (drained_journal.state !=
             recovery::LiveJournalStateV1::kStopped ||
         !drained_journal.healthy() ||
         drained_journal.committed_serial !=
             drained_journal.accepted_serial ||
         drained_recovery.error !=
             recovery::OnlineRecoveryErrorV1::kNone ||
         drained_recovery.last_journal_serial !=
             drained_journal.committed_serial)) {
        std::cerr
            << "mdl-production-router: promoted journal tail did not "
               "drain the durable prefix exactly: accepted="
            << drained_journal.accepted_serial
            << " committed=" << drained_journal.committed_serial
            << " consumed=" << drained_recovery.last_journal_serial
            << " recovery_error="
            << recovery::OnlineRecoveryErrorNameV1(
                   drained_recovery.error)
            << '\n';
        exit_code = 1;
    }

    runtime::RealtimePipelineCutResultV1 recovered_final{};
    if (promoted && exit_code == 0 && !shadow_pipeline->fatal()) {
        recovered_final =
            shadow_pipeline->StopAndPublishFinalGeneration(timeout);
        if (!recovered_final.published() ||
            !PublishKLineGeneration(
                recovered_final,
                recovered_ipc_service,
                "online final")) {
            std::cerr
                << "mdl-production-router: recovered final generation "
                   "failed\n";
            exit_code = 1;
        }
    } else {
        shadow_pipeline->StopAndDrain();
    }

    const runtime::RealtimePipelineSnapshotV1 preview_snapshot =
        preview_pipeline->Snapshot();
    const runtime::RealtimePipelineSnapshotV1 shadow_snapshot =
        shadow_pipeline->Snapshot();
    if (preview_snapshot.fatal || shadow_snapshot.fatal) {
        exit_code = 1;
    }
    if (certified_service != nullptr) {
        if (promoted && exit_code == 0) {
            certified_service->MarkStoppedClean();
        } else {
            certified_service->StopControl();
        }
    }
    if (exit_code == 0 &&
        !preview_ipc_service->MarkStoppedClean(
            preview_snapshot.tick_stream_sequence)) {
        exit_code = 1;
    }
    if (promoted && exit_code == 0) {
        if (!recovered_ipc_service->MarkStoppedClean(
                shadow_snapshot.tick_stream_sequence)) {
            exit_code = 1;
        }
    } else {
        recovered_ipc_service->MarkFailed();
    }
    if (exit_code != 0) {
        preview_ipc_service->MarkFailed();
        recovered_ipc_service->MarkFailed();
    }

    const recovery::LiveJournalSnapshotV1 final_journal =
        live_journal->Snapshot();
    const recovery::OnlineRecoverySnapshotV1 final_recovery =
        handoff->Snapshot();
    std::cerr
        << "mdl-production-router: online final: exit_code="
        << exit_code
        << " preview_records="
        << preview_snapshot.store.appended_records
        << " recovered_records="
        << shadow_snapshot.store.appended_records
        << " journal_accepted=" << final_journal.accepted_serial
        << " journal_committed=" << final_journal.committed_serial
        << " journal_bytes=" << final_journal.committed_bytes
        << " journal_duplicates_suppressed="
        << final_recovery.journal_duplicates_suppressed
        << " journal_last_read="
        << final_recovery.last_journal_serial
        << " promotion_journal_frontier="
        << final_recovery.promotion_journal_frontier
        << " promotion_shadow_ingress_frontier="
        << final_recovery.promotion_shadow_ingress_frontier
        << " csv_publications="
        << final_recovery.csv_publications
        << " csv_shanghai_snapshots="
        << final_recovery.csv_publications_by_message.shanghai_snapshots
        << " csv_shanghai_ticks="
        << final_recovery.csv_publications_by_message.shanghai_ticks
        << " csv_shenzhen_snapshots="
        << final_recovery.csv_publications_by_message.shenzhen_snapshots
        << " csv_shenzhen_orders="
        << final_recovery.csv_publications_by_message.shenzhen_orders
        << " csv_shenzhen_transactions="
        << final_recovery.csv_publications_by_message
               .shenzhen_transactions
        << " csv_alignment_phase="
        << recovery::StartupReplayBoundaryAlignmentPhaseNameV1(
               final_recovery.csv_boundary_alignment.phase)
        << " csv_alignment_attempted="
        << (final_recovery.csv_boundary_alignment.attempted
                ? "true"
                : "false")
        << " csv_alignment_sealed="
        << (final_recovery.csv_boundary_alignment.sealed
                ? "true"
                : "false")
        << " csv_alignment_wait_ms="
        << final_recovery.csv_boundary_alignment.waited_ms
        << " csv_alignment_polls="
        << final_recovery.csv_boundary_alignment.poll_count
        << " csv_alignment_scanned_records="
        << final_recovery.csv_boundary_alignment.scanned_records
        << " csv_alignment_order_scanned_bytes="
        << final_recovery.csv_boundary_alignment.order_scanned_bytes
        << " csv_alignment_transaction_scanned_bytes="
        << final_recovery.csv_boundary_alignment
               .transaction_scanned_bytes
        << " csv_alignment_records="
        << final_recovery.csv_boundary_alignment.extension_records
        << " csv_alignment_order_bytes="
        << final_recovery.csv_boundary_alignment.order_extension_bytes
        << " csv_alignment_transaction_bytes="
        << final_recovery.csv_boundary_alignment
               .transaction_extension_bytes
        << " csv_alignment_pending_messages="
        << final_recovery.csv_boundary_alignment.pending_messages
        << " csv_alignment_pending_channels="
        << final_recovery.csv_boundary_alignment.pending_channels
        << " csv_alignment_order_mandatory_partial="
        << (final_recovery.csv_boundary_alignment
                    .order_mandatory_partial
                ? "true"
                : "false")
        << " csv_alignment_transaction_mandatory_partial="
        << (final_recovery.csv_boundary_alignment
                    .transaction_mandatory_partial
                ? "true"
                : "false")
        << " csv_alignment_representative_missing_channel="
        << final_recovery.csv_boundary_alignment
               .representative_missing_channel
        << " csv_alignment_representative_missing_appl_seq="
        << final_recovery.csv_boundary_alignment
               .representative_missing_appl_seq
        << " csv_order_initial_cut="
        << final_recovery.csv_boundary_alignment.initial_order_cut
        << " csv_order_sealed_cut="
        << final_recovery.csv_boundary_alignment.sealed_order_cut
        << " csv_order_available_end="
        << final_recovery.csv_boundary_alignment.order_available_end
        << " csv_transaction_initial_cut="
        << final_recovery.csv_boundary_alignment
               .initial_transaction_cut
        << " csv_transaction_sealed_cut="
        << final_recovery.csv_boundary_alignment
               .sealed_transaction_cut
        << " csv_transaction_available_end="
        << final_recovery.csv_boundary_alignment
               .transaction_available_end
        << " journal_suffix_publications="
        << final_recovery.journal_suffix_publications
        << " replay_throttle_events="
        << final_recovery.replay_throttle_events
        << " replay_pause_events="
        << final_recovery.replay_pause_events
        << " promotion_realtime_ns="
        << final_recovery.promotion_realtime_ns
        << " recovered=" << (promoted ? "true" : "false")
        << '\n';
    return exit_code;
}

[[nodiscard]] bool ApplyEventCpuIsolation(
    const Options& options) noexcept {
    if (options.event_cpu_set.empty()) {
        // Deliberate no-op: ordinary deployments retain their prior inherited
        // affinity and pay no affinity syscall on the latency-sensitive path.
        return true;
    }

    common::LinuxCpuSetV1 event_cpus{};
    const common::LinuxCpuSetParseErrorV1 parse_error =
        common::ParseLinuxCpuSetV1(
            options.event_cpu_set, &event_cpus);
    if (parse_error != common::LinuxCpuSetParseErrorV1::kNone) {
        std::cerr
            << "mdl-production-router: event CPU set parse failed: "
            << common::LinuxCpuSetParseErrorNameV1(parse_error)
            << '\n';
        return false;
    }

    common::LinuxCpuSetV1 original_allowed{};
    int system_error = 0;
    const common::LinuxThreadAffinityErrorV1 read_error =
        common::ReadCurrentLinuxThreadAffinityV1(
            &original_allowed, &system_error);
    if (read_error !=
            common::LinuxThreadAffinityErrorV1::kNone ||
        original_allowed.empty()) {
        std::cerr
            << "mdl-production-router: cannot read original CPU affinity: "
            << common::LinuxThreadAffinityErrorNameV1(read_error)
            << " errno=" << system_error << '\n';
        return false;
    }
    common::LinuxCpuSetV1 fast_cpus{};
    const app::EventCpuPartitionErrorV1 partition_error =
        app::BuildEventCpuPartitionV1(
            original_allowed, event_cpus, &fast_cpus);
    if (partition_error != app::EventCpuPartitionErrorV1::kNone) {
        std::cerr
            << "mdl-production-router: --event-cpu-set cannot form a "
               "strict FAST/Event partition: "
            << app::EventCpuPartitionErrorNameV1(partition_error)
            << '\n';
        return false;
    }

    common::LinuxCpuSetV1 observed_fast{};
    system_error = 0;
    const common::LinuxThreadAffinityErrorV1 apply_error =
        common::ApplyCurrentLinuxThreadAffinityExactV1(
            fast_cpus, &observed_fast, &system_error);
    if (apply_error !=
            common::LinuxThreadAffinityErrorV1::kNone ||
        !(observed_fast == fast_cpus)) {
        std::cerr
            << "mdl-production-router: FAST CPU affinity apply/readback "
               "failed: "
            << common::LinuxThreadAffinityErrorNameV1(apply_error)
            << " errno=" << system_error << '\n';
        return false;
    }
    std::cerr
        << "mdl-production-router: strict CPU partition active: "
        << "event_cpu_set=" << options.event_cpu_set
        << " event_cpu_count=" << event_cpus.count()
        << " fast_cpu_count=" << fast_cpus.count()
        << " disjoint_logical_cpus=true\n";
    return true;
}

int Run(const Options& options) {
    std::uint32_t current_trade_date = 0U;
    if (!CurrentFixedUtc8TradeDate(&current_trade_date)) {
        std::cerr
            << "mdl-production-router: cannot read fixed UTC+08:00 date\n";
        return 1;
    }
    if (current_trade_date != options.trade_date) {
        std::cerr
            << "mdl-production-router: --trade-date must equal the current "
               "fixed UTC+08:00 date (current="
            << current_trade_date << ")\n";
        return 1;
    }

    common::Identity128 run_id{};
    int entropy_error = 0;
    if (!common::GenerateIdentity128(&run_id, &entropy_error)) {
        std::cerr
            << "mdl-production-router: run-id entropy failed: errno="
            << entropy_error << '\n';
        return 1;
    }

    market::DailyInstrumentCatalogFileOptionsV2 catalog_options{};
    catalog_options.path = options.daily_catalog;
    catalog_options.expected_trade_date = options.trade_date;
    catalog_options.expected_catalog_version =
        options.catalog_version;
    catalog_options.session_epoch = options.session_epoch;
    market::DailyInstrumentCatalogFileResultV2 catalog_result =
        market::LoadDailyInstrumentCatalogFileV2(catalog_options);
    if (!catalog_result.ok()) {
        std::cerr
            << "mdl-production-router: daily catalog load failed: "
            << market::DailyInstrumentCatalogFileErrorNameV2(
                   catalog_result.error)
            << " catalog_error="
            << market::DailyInstrumentCatalogCreateErrorNameV2(
                   catalog_result.catalog_error)
            << " line=" << catalog_result.line
            << '\n';
        return 1;
    }
    std::shared_ptr<const market::DailyInstrumentCatalogV2>
        daily_catalog(std::move(catalog_result.catalog));
    if (daily_catalog == nullptr ||
        daily_catalog->market_scope() !=
            market::kDailyCatalogMainlandScopeV2 ||
        !daily_catalog->coverage_complete()) {
        std::cerr
            << "mdl-production-router: daily catalog lacks declared "
               "complete Shanghai+Shenzhen A-share coverage\n";
        return 1;
    }
    // No long-lived service/Pipeline thread has been created yet. Threads
    // created below inherit the FAST complement; Event workers explicitly set
    // and read back options.event_cpu_set on their own native threads.
    if (!ApplyEventCpuIsolation(options)) {
        return 1;
    }
    if (!options.intraday_recovery_csv_dir.empty()) {
        return RunOnlineRecovery(
            options,
            run_id,
            daily_catalog,
            BuildKLineWindows(options));
    }
    std::unique_ptr<market::InstrumentRuntimeStateV2> runtime_state;
    const market::InstrumentRuntimeStateErrorV2 runtime_error =
        market::InstrumentRuntimeStateV2::Create(
            *daily_catalog, &runtime_state);
    if (runtime_error !=
            market::InstrumentRuntimeStateErrorV2::kNone ||
        runtime_state == nullptr) {
        std::cerr
            << "mdl-production-router: instrument runtime state create "
               "failed: "
            << market::InstrumentRuntimeStateErrorNameV2(
                   runtime_error)
            << '\n';
        return 1;
    }
    if (options.intraday_live_partial) {
        return RunLivePartial(
            options,
            run_id,
            daily_catalog,
            runtime_state.get());
    }

    const std::vector<market::KLineWindowSpecV1> kline_windows =
        BuildKLineWindows(options);

    runtime::RealtimePipelineConfigV1 pipeline_config{};
    pipeline_config.run_id = run_id;
    pipeline_config.trade_date = options.trade_date;
    pipeline_config.daily_catalog = daily_catalog;
    pipeline_config.runtime_state = runtime_state.get();
    pipeline_config.source_stream_ids =
        {1001U, 1002U, 2001U, 2002U};
    pipeline_config.store_worker_count =
        options.instrument_store_workers;
    pipeline_config.parallel_decoder_worker_count =
        options.parallel_decoder_workers;
    pipeline_config.decoder_queue_capacity_per_source =
        static_cast<std::size_t>(
            options.decoder_queue_records_per_source);
    pipeline_config.store_queue_capacity_per_source_worker =
        static_cast<std::size_t>(
            options.store_queue_records_per_source_worker);
    pipeline_config.intraday_store.segment_target_bytes =
        static_cast<std::size_t>(
            options.intraday_store_segment_kib) *
        1024U;
    pipeline_config.intraday_store.maximum_session_records =
        options.intraday_store_maximum_records;
    pipeline_config.intraday_store.maximum_session_accounted_bytes =
        options.intraday_store_memory_bytes;
    pipeline_config.intraday_store.maximum_records_per_batch =
        static_cast<std::size_t>(
            options.intraday_store_batch_records);
    pipeline_config.intraday_store.coverage_from_open =
        options.intraday_store_from_open;
    pipeline_config.kline.windows = kline_windows;
    pipeline_config.enforce_receive_trade_date = true;
    pipeline_config.tick_ring_capacity =
        static_cast<std::size_t>(
            options.ipc_tick_ring_records);
    pipeline_config.sdk.enabled = true;
    pipeline_config.sdk.library_path = options.sdk_library;
    pipeline_config.sdk.server_address = options.server_address;
    pipeline_config.sdk.user_name = options.user_name;
    pipeline_config.sdk.log_prefix = options.sdk_log_prefix;
    pipeline_config.sdk.message_encoding =
        datayes::mdl::MDLEID_BINARY;
    pipeline_config.sdk.merge_message = false;
    std::uint64_t minimum_tick_ring_capacity = 0U;
    if (!MinimumTickRingCapacity(
            pipeline_config, &minimum_tick_ring_capacity)) {
        std::cerr
            << "mdl-production-router: applied dispatch window "
               "overflowed u64\n";
        return 1;
    }
    if (options.ipc_tick_ring_records <
        minimum_tick_ring_capacity) {
        std::cerr
            << "mdl-production-router: --ipc-tick-ring-records must "
               "cover the explicitly bounded applied dispatch window "
               "(minimum="
            << minimum_tick_ring_capacity << ")\n";
        return 1;
    }

    ipc::RealtimeSharedServiceConfigV2 ipc_config{};
    ipc_config.run_id = run_id;
    ipc_config.session_epoch = options.session_epoch;
    ipc_config.trade_date = options.trade_date;
    ipc_config.daily_catalog = daily_catalog;
    ipc_config.kline_windows = kline_windows;
    ipc_config.coverage_from_open = true;
    ipc_config.startup_prefix_recovered = false;
    ipc_config.full_day_kline_valid = !kline_windows.empty();
    ipc_config.full_day_factor_valid = true;
    ipc_config.certified_prefix_valid = false;
    ipc_config.tick_ring_capacity =
        options.ipc_tick_ring_records;
    ipc_config.key_arena_bytes = options.ipc_key_arena_bytes;
    ipc_config.maximum_mapping_bytes =
        options.ipc_maximum_mapping_bytes;
    ipc_config.control_socket_path = options.ipc_socket;

    std::shared_ptr<ipc::RealtimeSharedMarketServiceV2> ipc_service;
    int ipc_system_error = 0;
    const ipc::RealtimeSharedServiceCreateErrorV2 ipc_error =
        ipc::RealtimeSharedMarketServiceV2::Create(
            std::move(ipc_config),
            &ipc_service,
            &ipc_system_error);
    if (ipc_error !=
            ipc::RealtimeSharedServiceCreateErrorV2::kNone ||
        ipc_service == nullptr) {
        std::cerr
            << "mdl-production-router: IPC V2 service create failed: "
            << ipc::RealtimeSharedServiceCreateErrorNameV2(
                   ipc_error)
            << " errno=" << ipc_system_error << '\n';
        return 1;
    }

    const auto start_fast_control = [&]() -> bool {
        ipc_system_error = 0;
        if (!ipc_service->Start(&ipc_system_error)) {
            std::cerr
                << "mdl-production-router: IPC V2 control start failed: "
                   "errno="
                << ipc_system_error << '\n';
            return false;
        }
        std::cerr
            << "mdl-production-router: IPC V2 ACTIVE: socket="
            << ipc_service->control_socket_path()
            << " capacity=" << daily_catalog->instrument_count()
            << " bound_count=" << daily_catalog->instrument_count()
            << " catalog_scope=DECLARED_DAILY_A_SHARE"
            << " coverage_complete=true"
            << " coverage_from_open=true"
            << " coverage_source=LIVE_FROM_OPEN"
            << " catalog_version=" << daily_catalog->catalog_version()
            << " catalog_digest="
            << common::Sha256Hex(daily_catalog->catalog_digest())
            << " mapping_bytes=" << ipc_service->mapping_bytes()
            << " key_arena_bytes=" << options.ipc_key_arena_bytes
            << " decoder_queue_records_per_source="
            << options.decoder_queue_records_per_source
            << " parallel_decoder_workers="
            << options.parallel_decoder_workers
            << " store_queue_records_per_source_worker="
            << options.store_queue_records_per_source_worker
            << " mainland_a_share_filter=true"
            << '\n';
        return true;
    };

    if (!start_fast_control()) {
        ipc_service->MarkFailed();
        ipc_service->StopControl();
        return 1;
    }
    std::shared_ptr<ipc::RealtimeCertifiedMarketServiceV1>
        certified_service;
    // FAST is the required service. CERTIFIED is enabled by default, but it
    // remains an optional, fail-open sidecar: configuration, allocation,
    // socket, or thread-start failures must not make an otherwise healthy
    // FAST session unavailable.
    pipeline_config.applied_record_sink = ipc_service;
    if (options.native_gap_recovery_enabled) {
        constexpr std::size_t maximum_size =
            std::numeric_limits<std::size_t>::max();
        const bool certified_capacity_representable =
            options.intraday_store_maximum_records <= maximum_size &&
            options.intraday_store_maximum_records <=
                maximum_size / 4U;
        if (!certified_capacity_representable) {
            std::cerr
                << "mdl-production-router: CERTIFIED V1 DEGRADED: "
                   "capacity cannot represent four derived events per "
                   "stored record; the required FAST path remains\n";
        } else {
            const std::size_t maximum_order_states =
                static_cast<std::size_t>(
                    options.intraday_store_maximum_records);
            const std::size_t maximum_derived_events =
                maximum_order_states * 4U;

            ipc::RealtimeCertifiedServiceConfigV1 certified_config{};
            certified_config.run_id = run_id;
            certified_config.session_epoch = options.session_epoch;
            certified_config.trade_date = options.trade_date;
            certified_config.daily_catalog = daily_catalog;
            certified_config.fast_sink = ipc_service;
            certified_config.certified_tick_ring_capacity =
                options.ipc_tick_ring_records;
            certified_config.handoff_queue_capacity =
                options.certified_handoff_queue_records;
            certified_config.maximum_mapping_bytes =
                options.ipc_maximum_mapping_bytes;
            certified_config.maximum_order_states =
                maximum_order_states;
            certified_config.maximum_derived_events =
                maximum_derived_events;
            certified_config.maximum_certified_ticks =
                options.intraday_store_maximum_records;
            certified_config.worker_cpu_set =
                options.event_cpu_set;
            certified_config.control_cpu_set =
                options.event_cpu_set;
            certified_config.tick_history_worker_cpu_set =
                options.event_cpu_set;
            certified_config.control_socket_path =
                options.certified_ipc_socket;
            int certified_system_error = 0;
            const ipc::RealtimeCertifiedServiceCreateErrorV1
                certified_error =
                    ipc::RealtimeCertifiedMarketServiceV1::Create(
                        std::move(certified_config),
                        &certified_service,
                        &certified_system_error);
            if (certified_error !=
                    ipc::RealtimeCertifiedServiceCreateErrorV1::kNone ||
                certified_service == nullptr) {
                std::cerr
                    << "mdl-production-router: CERTIFIED V1 DEGRADED: "
                       "service create failed: "
                    << ipc::RealtimeCertifiedServiceCreateErrorNameV1(
                           certified_error)
                    << " errno=" << certified_system_error
                    << "; the required FAST path remains\n";
                certified_service.reset();
            } else {
                certified_system_error = 0;
                const bool certified_started =
                    certified_service->Start(
                        &certified_system_error);
                if (!certified_started) {
                    std::cerr
                        << "mdl-production-router: CERTIFIED V1 "
                           "DEGRADED: worker/control start failed: errno="
                        << certified_system_error
                        << "; the required FAST path remains\n";
                    certified_service->StopControl();
                    certified_service.reset();
                } else {
                    if (!ipc_service->MarkCertifiedPrefixValid()) {
                        std::cerr
                            << "mdl-production-router: CERTIFIED V1 "
                               "DEGRADED: FAST certified-prefix flag "
                               "publication failed\n";
                        certified_service->StopControl();
                        certified_service.reset();
                    }
                }
            }
        }
        if (certified_service != nullptr) {
            std::cerr
                << "mdl-production-router: CERTIFIED V1 ACTIVE: "
                   "socket="
                << certified_service->control_socket_path()
                << " mapping_bytes="
                << certified_service->mapping_bytes()
                << " handoff_queue_records="
                << certified_service->handoff_queue_capacity()
                << " native_gap_recovery=true"
                << " event_service=CERTIFIED_CANONICAL"
                << " event_history=FULL_DAY_APPEND_ONLY"
                << " event_temporal_coverage=FROM_MARKET_OPEN"
                << " event_logical_cpu_disjoint="
                << (!options.event_cpu_set.empty() ? "true" : "false")
                << " fast_wire_abi_unchanged=true\n";
            pipeline_config.applied_record_sink = certified_service;
            pipeline_config.native_sequence_observation_sink =
                certified_service;
        }
    } else {
        std::cerr
            << "mdl-production-router: native gap recovery disabled; "
               "FAST Wire V2 only\n";
    }
    pipeline_config.processing_progress_sink = ipc_service;
    pipeline_config.store_generation_sink = ipc_service;

    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    const runtime::RealtimePipelineCreateErrorV1 create_error =
        runtime::RealtimePipelineV1::Create(
            std::move(pipeline_config), &pipeline, &detail);
    if (create_error !=
            runtime::RealtimePipelineCreateErrorV1::kNone ||
        pipeline == nullptr) {
        std::cerr
            << "mdl-production-router: pipeline create failed: "
            << runtime::RealtimePipelineCreateErrorNameV1(
                   create_error)
            << (detail.empty() ? "" : ": ") << detail << '\n';
        if (certified_service != nullptr) {
            const ipc::RealtimeCertifiedServiceSnapshotV1
                startup_certified_snapshot =
                    certified_service->Snapshot();
            std::cerr
                << "mdl-production-router: CERTIFIED startup failure: "
                << "state="
                << static_cast<std::uint32_t>(
                       startup_certified_snapshot.state)
                << " canonical_apply_frontier="
                << startup_certified_snapshot.canonical_apply_frontier
                << " observed_native_messages="
                << startup_certified_snapshot
                       .observed_native_message_count
                << " enqueued_observations="
                << startup_certified_snapshot.enqueued_observations
                << " enqueued_applied_records="
                << startup_certified_snapshot.enqueued_applied_records
                << " processed_handoffs="
                << startup_certified_snapshot.processed_handoffs
                << " pending_token_count="
                << startup_certified_snapshot.pending_token_count
                << " conflicts="
                << startup_certified_snapshot
                       .conflicting_duplicate_count
                << " resource_exhaustions="
                << startup_certified_snapshot
                       .resource_exhaustion_count
                << " dropped_handoffs="
                << startup_certified_snapshot.dropped_handoffs
                << " worker_running="
                << (startup_certified_snapshot.worker_running
                        ? "true"
                        : "false")
                << " globally_frozen_resource="
                << (startup_certified_snapshot
                            .globally_frozen_resource
                        ? "true"
                        : "false")
                << " frozen_channel_count="
                << startup_certified_snapshot.frozen_channel_count
                << " wire_snapshot_consistent="
                << (startup_certified_snapshot
                            .wire_snapshot_consistent
                        ? "true"
                        : "false")
                << " fast_failed="
                << (ipc_service->failed() ? "true" : "false")
                << '\n';
            certified_service->StopControl();
        }
        ipc_service->MarkFailed();
        ipc_service->StopControl();
        return 1;
    }

    const auto interval =
        std::chrono::milliseconds(
            options.generation_interval_ms);
    const auto timeout =
        std::chrono::milliseconds(
            options.generation_timeout_ms);
    int exit_code = 0;
    bool certified_control_failure_reported = false;
    while (g_stop_requested == 0) {
        const IntervalWaitResult wait =
            WaitForInterval(interval, options.trade_date);
        if (wait == IntervalWaitResult::kSignal) {
            break;
        }
        if (wait == IntervalWaitResult::kClockFailure) {
            std::cerr
                << "mdl-production-router: civil-date clock read "
                   "failed\n";
            exit_code = 1;
            break;
        }
        if (wait == IntervalWaitResult::kTradeDateBoundary) {
            std::cerr
                << "mdl-production-router: trade-date boundary "
                   "reached; publishing final prior-day generation\n";
            break;
        }
        if (ipc_service->failed()) {
            std::cerr
                << "mdl-production-router: IPC V2 projection failed\n";
            exit_code = 1;
            break;
        }
        if (certified_service != nullptr &&
            !certified_control_failure_reported &&
            certified_service->ControlStateSnapshot() ==
                ipc::RealtimeCertifiedServiceSnapshotV1::
                    ControlState::kFailed) {
            // Native certification is explicitly fail-open to FAST in the
            // ordinary from-open composition. Surface the lost accept loop
            // without adding a producer/read-path check or killing FAST.
            std::cerr
                << "mdl-production-router: CERTIFIED control unavailable; "
                   "FAST remains active\n";
            certified_control_failure_reported = true;
        }

        const runtime::RealtimePipelineSnapshotV1 before_cut =
            pipeline->Snapshot();
        if (before_cut.fatal) {
            ReportFatalSnapshot(before_cut);
            exit_code = 1;
            break;
        }
        if (before_cut.trade_date_boundary_reached) {
            std::cerr
                << "mdl-production-router: callback observed "
                   "trade-date boundary; publishing final prior-day "
                   "generation\n";
            break;
        }

        const runtime::RealtimePipelineCutResultV1 cut =
            pipeline->CutAndPublishGeneration(timeout);
        if (!cut.published()) {
            const runtime::RealtimePipelineSnapshotV1 failed_snapshot =
                pipeline->Snapshot();
            if (cut.error ==
                    runtime::RealtimePipelineCutErrorV1::kStopped &&
                failed_snapshot.trade_date_boundary_reached) {
                std::cerr
                    << "mdl-production-router: callback closed "
                       "admission at trade-date boundary; publishing "
                       "final generation\n";
                break;
            }
            std::cerr
                << "mdl-production-router: generation failed: "
                << runtime::RealtimePipelineCutErrorNameV1(
                       cut.error)
                << " generation="
                << market::RealtimeHistoryGenerationErrorNameV1(
                       cut.generation_error)
                << " factor="
                << factor::RealtimeFactorPublishErrorNameV1(
                       cut.factor_result.error)
                << '\n';
            exit_code = 1;
            break;
        }
        if (!PublishKLineGeneration(
                cut, ipc_service, "periodic")) {
            exit_code = 1;
            break;
        }
    }

    if (!ipc_service->failed()) {
        ipc_service->MarkDraining();
    }
    if (certified_service != nullptr) {
        certified_service->MarkDraining();
    }

    if (exit_code == 0 && !pipeline->fatal()) {
        const runtime::RealtimePipelineCutResultV1 final_cut =
            pipeline->StopAndPublishFinalGeneration(timeout);
        if (!final_cut.published()) {
            std::cerr
                << "mdl-production-router: final generation failed: "
                << runtime::RealtimePipelineCutErrorNameV1(
                       final_cut.error)
                << " generation="
                << market::RealtimeHistoryGenerationErrorNameV1(
                       final_cut.generation_error)
                << " factor="
                << factor::RealtimeFactorPublishErrorNameV1(
                       final_cut.factor_result.error)
                << '\n';
            exit_code = 1;
        } else if (!PublishKLineGeneration(
                       final_cut, ipc_service, "final")) {
            exit_code = 1;
        }
    } else {
        pipeline->StopAndDrain();
    }

    const runtime::RealtimePipelineSnapshotV1 final_snapshot =
        pipeline->Snapshot();
    if (final_snapshot.fatal) {
        ReportFatalSnapshot(final_snapshot);
        exit_code = 1;
    }
    if (ipc_service->failed()) {
        std::cerr
            << "mdl-production-router: IPC V2 projection ended in "
               "FAILED state\n";
        exit_code = 1;
    }

    ipc::RealtimeCertifiedServiceSnapshotV1 certified_snapshot{};
    if (certified_service != nullptr) {
        if (exit_code == 0) {
            certified_service->MarkStoppedClean();
        } else {
            certified_snapshot = certified_service->Snapshot();
            certified_service->StopControl();
        }
        if (exit_code == 0) {
            certified_snapshot = certified_service->Snapshot();
        }
    }

    if (exit_code == 0) {
        if (!ipc_service->MarkStoppedClean(
                final_snapshot.tick_stream_sequence)) {
            std::cerr
                << "mdl-production-router: IPC V2 final mixed-tick "
                   "prefix is incomplete\n";
            exit_code = 1;
            ipc_service->MarkFailed();
        }
    } else {
        ipc_service->MarkFailed();
    }

    const std::shared_ptr<const market::RealtimeKLineGenerationV1>
        final_kline = pipeline->AcquireLatestKLineGeneration();
    std::shared_ptr<
        const market::DailyInstrumentCatalogSnapshotV2>
        final_catalog;
    const market::InstrumentRuntimeStateErrorV2
        catalog_snapshot_error =
            runtime_state->AcquireSnapshot(&final_catalog);
    const market::IntradayInstrumentStoreSnapshotV1& final_store =
        final_snapshot.store;
    std::cerr
        << "mdl-production-router: final: records="
        << final_store.appended_records
        << " record_bytes=" << final_store.accounted_record_bytes
        << " index_bytes=" << final_store.allocated_index_bytes
        << " byte_limit="
        << final_store.maximum_session_accounted_bytes
        << " coverage_from_open="
        << (final_store.coverage_from_open ? "true" : "false")
        << " coverage_lost="
        << (final_store.coverage_lost ? "true" : "false")
        << " kline_bars="
        << (final_kline == nullptr ? 0U : final_kline->bar_count())
        << " accepted_sequence="
        << final_snapshot.processing_progress.accepted_sequence
        << " applied_sequence="
        << final_snapshot.processing_progress.applied_sequence
        << " processing_lag_records="
        << final_snapshot.processing_progress.processing_lag_records()
        << " mainland_a_share_filter=mandatory"
        << " filtered_messages="
        << final_snapshot.filtered_messages
        << " filtered_by_source_sh_snapshot_sh_tick_sz_snapshot_sz_tick="
        << final_snapshot.filtered_messages_by_source[0] << ','
        << final_snapshot.filtered_messages_by_source[1] << ','
        << final_snapshot.filtered_messages_by_source[2] << ','
        << final_snapshot.filtered_messages_by_source[3];
    if (catalog_snapshot_error ==
            market::InstrumentRuntimeStateErrorV2::kNone &&
        final_catalog != nullptr) {
        std::cerr
            << " catalog_scope=DECLARED_DAILY_A_SHARE"
            << " coverage_complete="
            << (final_catalog->coverage_complete()
                    ? "true"
                    : "false")
            << " catalog_generation="
            << final_catalog->catalog_generation()
            << " bound_count=" << final_catalog->bound_count()
            << " available_count="
            << final_catalog->available_count()
            << " snapshot_available_count="
            << final_catalog->snapshot_available_count()
            << " tick_available_count="
            << final_catalog->tick_available_count()
            << " factor_eligible_count="
            << final_catalog->factor_eligible_count();
    } else {
        std::cerr
            << " catalog_snapshot_error="
            << market::InstrumentRuntimeStateErrorNameV2(
                   catalog_snapshot_error);
    }
    std::cerr << '\n';

    if (certified_service != nullptr) {
        std::cerr
            << "mdl-production-router: CERTIFIED final: state="
            << static_cast<std::uint32_t>(
                   certified_snapshot.state)
            << " canonical_apply_frontier="
            << certified_snapshot.canonical_apply_frontier
            << " observed_native_messages="
            << certified_snapshot.observed_native_message_count
            << " exact_duplicates="
            << certified_snapshot.exact_duplicate_message_count
            << " gaps_opened="
            << certified_snapshot.gap_opened_count
            << " gaps_recovered="
            << certified_snapshot.gap_recovered_count
            << " conflicts="
            << certified_snapshot.conflicting_duplicate_count
            << " resource_exhaustions="
            << certified_snapshot.resource_exhaustion_count
            << " dropped_handoffs="
            << certified_snapshot.dropped_handoffs
            << " fast_remained_independent=true\n";
        certified_service->StopControl();
    }

    ipc_service->StopControl();
    return exit_code;
}

}  // namespace

int main(int argc, char* argv[]) {
    Options options{};
    bool help = false;
    std::string error;
    if (!ParseOptions(
            argc, argv, &options, &help, &error)) {
        std::cerr
            << "mdl-production-router: " << error << '\n';
        PrintUsage(std::cerr);
        return 2;
    }
    if (help) {
        PrintUsage(std::cout);
        return 0;
    }

    int signal_error = 0;
    if (!InstallStopSignalHandlers(&signal_error)) {
        std::cerr
            << "mdl-production-router: cannot install stop handlers: "
               "errno="
            << signal_error << '\n';
        return 1;
    }
    return Run(options);
}
