#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
#include "l2flow/ipc/realtime_shared_service_v1.h"
#endif
#include "l2flow/market/instrument_registry_loader_v1.h"
#include "l2flow/runtime/realtime_pipeline_v1.h"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

namespace {

namespace common = l2flow::common;
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
namespace ipc = l2flow::ipc;
#endif
namespace market = l2flow::market;
namespace runtime = l2flow::runtime;

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
    std::filesystem::path registry_directory;
    std::string registry_file;
    std::uint64_t registry_version = 0U;
    common::Sha256Digest registry_sha256{};
    bool registry_sha_set = false;
    std::uint32_t trade_date = 0U;
    std::string server_address;
    std::string user_name;
    std::string sdk_log_prefix = "l2flow-realtime";
    std::filesystem::path wal_path;
    bool replace_wal = false;
    std::uint32_t instrument_store_workers = 4U;
    std::uint64_t intraday_store_maximum_records = 0U;
    std::uint64_t intraday_store_memory_bytes = 0U;
    std::uint32_t intraday_store_segment_kib = 64U;
    std::uint32_t intraday_store_batch_records = 64U * 1024U;
    bool intraday_store_from_open = false;
    bool intraday_store_partial_session = false;
    bool intraday_store_maximum_records_set = false;
    bool intraday_store_memory_set = false;
    // Each duration in milliseconds is also its stable public window_id.
    std::vector<std::uint32_t> kline_windows_ms;
    std::uint32_t generation_interval_ms = 1000U;
    std::uint32_t generation_timeout_ms = 10'000U;
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
    std::filesystem::path ipc_socket;
    std::uint64_t ipc_tick_ring_records = 262'144U;
    std::uint64_t ipc_maximum_mapping_bytes =
        2ULL * 1024ULL * 1024ULL * 1024ULL;
    bool ipc_tick_ring_records_set = false;
    bool ipc_maximum_mapping_set = false;
#endif
};

void PrintUsage(std::ostream& output) {
    output
        << "Usage: mdl-production-router [required options] [optional]\n"
        << "Required:\n"
        << "  --sdk-library PATH            vendor .so selected by operator\n"
        << "  --registry-directory DIR      secure registry directory\n"
        << "  --registry-file NAME          registry file below DIR\n"
        << "  --registry-version N          canonical registry version\n"
        << "  --registry-sha256 HEX64       canonical registry identity\n"
        << "  --trade-date YYYYMMDD         decoder trading date\n"
        << "  --server-address HOST:PORT    vendor endpoint\n"
        << "  --user-name VALUE             nonempty vendor user/token field\n"
        << "  --intraday-store-max-records N\n"
        << "                                positive u64 session record cap\n"
        << "  --intraday-store-memory-gib N positive u64 logical total GiB cap\n"
        << "Coverage mode (choose exactly one):\n"
        << "  --intraday-store-from-open    require continuous coverage from "
           "market open\n"
        << "  --partial-session             mid-session diagnostic; does not "
           "claim coverage from open\n"
        << "Optional:\n"
        << "  --sdk-log-prefix PATH         default l2flow-realtime\n"
        << "  --wal-path PATH               enable independent audit WAL\n"
        << "  --replace-wal                 explicitly truncate WAL path\n"
        << "  --instrument-store-workers N  1..256, default 4\n"
        << "  --intraday-store-segment-kib N\n"
        << "                                4..16384, default 64\n"
        << "  --intraday-store-batch-records N\n"
        << "                                1..1048576, default 65536\n"
        << "  --kline-windows-ms LIST        comma-separated 1..86400000 "
           "ms windows;\n"
        << "                                duration-ms is the window id\n"
        << "  --generation-interval-ms N    1..60000, default 1000\n"
        << "  --generation-timeout-ms N     1..600000, default 10000\n";
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
    output
        << "  --ipc-socket PATH             enable read-only memfd/UDS "
           "service\n"
        << "  --ipc-tick-ring-records N     mixed-tick ring capacity, "
           "default 262144\n"
        << "  --ipc-max-mapping-mib N       shared-memory hard cap, "
           "default 2048\n";
#endif
    output
        << "  --help\n\n"
        << "The SDK path is passed directly to dlopen/dlsym. No SDK digest, "
           "baseline, archive, ELF, ABI, or exact-byte approval is used.\n";
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
    if (*index + 1 >= argc || argv[*index + 1] == nullptr) {
        *error = std::string(option) + " requires a value";
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
        if (option == "--replace-wal") {
            if (!seen.insert(option).second) {
                *error = "duplicate --replace-wal";
                return false;
            }
            parsed.replace_wal = true;
            continue;
        }
        if (option == "--intraday-store-from-open") {
            if (!seen.insert(option).second) {
                *error = "duplicate --intraday-store-from-open";
                return false;
            }
            parsed.intraday_store_from_open = true;
            continue;
        }
        if (option == "--partial-session") {
            if (!seen.insert(option).second) {
                *error = "duplicate --partial-session";
                return false;
            }
            parsed.intraday_store_partial_session = true;
            continue;
        }
        if (option != "--sdk-library" &&
            option != "--registry-directory" &&
            option != "--registry-file" &&
            option != "--registry-version" &&
            option != "--registry-sha256" &&
            option != "--trade-date" &&
            option != "--server-address" &&
            option != "--user-name" &&
            option != "--sdk-log-prefix" &&
            option != "--wal-path" &&
            option != "--instrument-store-workers" &&
            option != "--intraday-store-max-records" &&
            option != "--intraday-store-memory-gib" &&
            option != "--intraday-store-segment-kib" &&
            option != "--intraday-store-batch-records" &&
            option != "--kline-windows-ms" &&
            option != "--generation-interval-ms" &&
            option != "--generation-timeout-ms"
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
            &&
            option != "--ipc-socket" &&
            option != "--ipc-tick-ring-records" &&
            option != "--ipc-max-mapping-mib"
#endif
        ) {
            *error = "unknown option: " + std::string(option);
            return false;
        }
        if (!seen.insert(option).second) {
            *error = "duplicate option: " + std::string(option);
            return false;
        }
        std::string_view value;
        if (!TakeValue(argc, argv, &index, option, &value, error)) {
            return false;
        }
        if (option == "--sdk-library") {
            parsed.sdk_library = std::string(value);
        } else if (option == "--registry-directory") {
            parsed.registry_directory = std::string(value);
        } else if (option == "--registry-file") {
            parsed.registry_file = value;
        } else if (option == "--registry-version") {
            if (!ParseU64(value, &parsed.registry_version) ||
                parsed.registry_version == 0U) {
                *error = "--registry-version must be positive u64";
                return false;
            }
        } else if (option == "--registry-sha256") {
            if (!common::ParseSha256Hex(
                    value, &parsed.registry_sha256, error)) {
                return false;
            }
            parsed.registry_sha_set = true;
        } else if (option == "--trade-date") {
            if (!ParseU32(value, &parsed.trade_date)) {
                *error = "--trade-date must be YYYYMMDD";
                return false;
            }
        } else if (option == "--server-address") {
            parsed.server_address = value;
        } else if (option == "--user-name") {
            parsed.user_name = value;
        } else if (option == "--sdk-log-prefix") {
            parsed.sdk_log_prefix = value;
        } else if (option == "--wal-path") {
            parsed.wal_path = std::string(value);
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
        } else if (option == "--ipc-socket") {
            parsed.ipc_socket = std::string(value);
        } else if (option == "--ipc-tick-ring-records") {
            if (!ParseU64(
                    value, &parsed.ipc_tick_ring_records) ||
                parsed.ipc_tick_ring_records == 0U) {
                *error =
                    "--ipc-tick-ring-records must be positive u64";
                return false;
            }
            parsed.ipc_tick_ring_records_set = true;
        } else if (option == "--ipc-max-mapping-mib") {
            std::uint64_t mib = 0U;
            constexpr std::uint64_t bytes_per_mib =
                std::uint64_t{1024U} * 1024U;
            if (!ParseU64(value, &mib) || mib == 0U ||
                mib > std::numeric_limits<std::uint64_t>::max() /
                          bytes_per_mib) {
                *error =
                    "--ipc-max-mapping-mib must be a positive u64 "
                    "whose byte conversion does not overflow";
                return false;
            }
            parsed.ipc_maximum_mapping_bytes = mib * bytes_per_mib;
            parsed.ipc_maximum_mapping_set = true;
#endif
        } else if (option == "--intraday-store-max-records") {
            if (!ParseU64(
                    value, &parsed.intraday_store_maximum_records) ||
                parsed.intraday_store_maximum_records == 0U) {
                *error =
                    "--intraday-store-max-records must be positive u64";
                return false;
            }
            parsed.intraday_store_maximum_records_set = true;
        } else if (option == "--intraday-store-memory-gib") {
            std::uint64_t gib = 0U;
            constexpr std::uint64_t bytes_per_gib =
                std::uint64_t{1024U} * 1024U * 1024U;
            if (!ParseU64(value, &gib) || gib == 0U ||
                gib > std::numeric_limits<std::uint64_t>::max() /
                          bytes_per_gib) {
                *error =
                    "--intraday-store-memory-gib must be a positive u64 "
                    "whose byte conversion does not overflow";
                return false;
            }
            parsed.intraday_store_memory_bytes = gib * bytes_per_gib;
            parsed.intraday_store_memory_set = true;
        } else if (option == "--intraday-store-segment-kib") {
            if (!ParseU32(
                    value, &parsed.intraday_store_segment_kib) ||
                parsed.intraday_store_segment_kib <
                    market::kIntradayInstrumentStoreMinimumSegmentBytesV1 /
                        1024U ||
                parsed.intraday_store_segment_kib >
                    market::kIntradayInstrumentStoreMaximumSegmentBytesV1 /
                        1024U) {
                *error =
                    "--intraday-store-segment-kib must be 4..16384";
                return false;
            }
        } else if (option == "--intraday-store-batch-records") {
            if (!ParseU32(
                    value, &parsed.intraday_store_batch_records) ||
                parsed.intraday_store_batch_records == 0U ||
                parsed.intraday_store_batch_records >
                    market::
                        kIntradayInstrumentStoreMaximumBatchRecordsV1) {
                *error =
                    "--intraday-store-batch-records must be 1..1048576";
                return false;
            }
        } else if (option == "--kline-windows-ms") {
            if (!ParseKLineWindows(
                    value, &parsed.kline_windows_ms, error)) {
                return false;
            }
        } else {
            std::uint32_t number = 0U;
            if (!ParseU32(value, &number)) {
                *error = std::string(option) + " must be u32";
                return false;
            }
            if (option == "--instrument-store-workers") {
                if (number == 0U || number > 256U) {
                    *error =
                        "--instrument-store-workers must be 1..256";
                    return false;
                }
                parsed.instrument_store_workers = number;
            } else if (option == "--generation-interval-ms") {
                if (number == 0U || number > 60'000U) {
                    *error = "--generation-interval-ms must be 1..60000";
                    return false;
                }
                parsed.generation_interval_ms = number;
            } else {
                if (number == 0U || number > 600'000U) {
                    *error = "--generation-timeout-ms must be 1..600000";
                    return false;
                }
                parsed.generation_timeout_ms = number;
            }
        }
    }

    if (parsed.sdk_library.empty() ||
        parsed.registry_directory.empty() ||
        parsed.registry_file.empty() ||
        parsed.registry_version == 0U || !parsed.registry_sha_set ||
        parsed.trade_date == 0U || parsed.server_address.empty() ||
        parsed.user_name.empty()) {
        *error = "all required options must be supplied";
        return false;
    }
    if (parsed.replace_wal && parsed.wal_path.empty()) {
        *error = "--replace-wal requires --wal-path";
        return false;
    }
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
    if (parsed.ipc_socket.empty() &&
        (parsed.ipc_tick_ring_records_set ||
         parsed.ipc_maximum_mapping_set)) {
        *error =
            "--ipc-tick-ring-records and --ipc-max-mapping-mib "
            "require --ipc-socket";
        return false;
    }
#endif
    if (!parsed.intraday_store_maximum_records_set ||
        !parsed.intraday_store_memory_set) {
        *error =
            "store-only production requires explicit positive "
            "--intraday-store-max-records and --intraday-store-memory-gib";
        return false;
    }
    if (parsed.intraday_store_from_open ==
        parsed.intraday_store_partial_session) {
        *error =
            "choose exactly one of --intraday-store-from-open and "
            "--partial-session";
        return false;
    }
    *output = std::move(parsed);
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

    int directory_flags = O_RDONLY | O_DIRECTORY | O_CLOEXEC;
#ifdef O_NOFOLLOW
    directory_flags |= O_NOFOLLOW;
#endif
    const int registry_directory_fd =
        ::open(options.registry_directory.c_str(), directory_flags);
    if (registry_directory_fd < 0) {
        std::cerr << "mdl-production-router: cannot open registry directory\n";
        return 1;
    }

    market::InstrumentRegistryFileOptionsV1 registry_options{};
    registry_options.directory_fd = registry_directory_fd;
    registry_options.file_name = options.registry_file;
    registry_options.expected_owner_uid =
        static_cast<std::uint32_t>(::getuid());
    registry_options.expected_registry_version = options.registry_version;
    registry_options.expected_registry_sha256 = options.registry_sha256;
    market::InstrumentRegistryFileResultV1 registry_result =
        market::LoadInstrumentRegistryFileV1(registry_options);
    static_cast<void>(::close(registry_directory_fd));
    if (!registry_result.ok()) {
        std::cerr
            << "mdl-production-router: registry load failed: "
            << market::InstrumentRegistryFileErrorNameV1(
                   registry_result.error)
            << " line=" << registry_result.line
            << " errno=" << registry_result.system_error_number << '\n';
        return 1;
    }

    common::Identity128 run_id{};
    int entropy_error = 0;
    if (!common::GenerateIdentity128(&run_id, &entropy_error)) {
        std::cerr << "mdl-production-router: run-id entropy failed: errno="
                  << entropy_error << '\n';
        return 1;
    }

    runtime::RealtimePipelineConfigV1 config{};
    config.run_id = run_id;
    config.trade_date = options.trade_date;
    config.registry = registry_result.registry.get();
    config.source_stream_ids = {1001U, 1002U, 2001U, 2002U};
    config.store_worker_count = options.instrument_store_workers;
    config.intraday_store.segment_target_bytes =
        static_cast<std::size_t>(options.intraday_store_segment_kib) * 1024U;
    config.intraday_store.maximum_session_records =
        options.intraday_store_maximum_records;
    config.intraday_store.maximum_session_accounted_bytes =
        options.intraday_store_memory_bytes;
    config.intraday_store.maximum_records_per_batch =
        static_cast<std::size_t>(options.intraday_store_batch_records);
    config.intraday_store.coverage_from_open =
        options.intraday_store_from_open;
    config.kline.windows.reserve(options.kline_windows_ms.size());
    for (const std::uint32_t duration_ms :
         options.kline_windows_ms) {
        market::KLineWindowSpecV1 window{};
        window.window_id = duration_ms;
        window.duration_ns =
            static_cast<std::uint64_t>(duration_ms) *
            market::kKLineNanosecondsPerMillisecondV1;
        config.kline.windows.push_back(window);
    }
    config.enforce_receive_trade_date = true;
    config.wal.enabled = !options.wal_path.empty();
    config.wal.path = options.wal_path.string();
    config.wal.queue_capacity = config.wal.enabled ? 8192U : 0U;
    config.wal.replace_existing = options.replace_wal;
    config.sdk.enabled = true;
    config.sdk.library_path = options.sdk_library;
    config.sdk.server_address = options.server_address;
    config.sdk.user_name = options.user_name;
    config.sdk.log_prefix = options.sdk_log_prefix;
    config.sdk.message_encoding = datayes::mdl::MDLEID_BINARY;
    config.sdk.merge_message = false;

#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
    std::shared_ptr<ipc::RealtimeSharedMarketServiceV1> ipc_service;
    if (!options.ipc_socket.empty()) {
        constexpr std::uint64_t tick_source_count = 2U;
        const std::uint64_t decoder_pending =
            tick_source_count *
            static_cast<std::uint64_t>(
                config.decoder_queue_capacity_per_source);
        const std::uint64_t history_pending =
            tick_source_count *
            static_cast<std::uint64_t>(config.store_worker_count) *
            static_cast<std::uint64_t>(
                config.store_queue_capacity_per_source_worker);
        // In addition to queued messages, each serial tick decoder and each
        // Store worker can own one in-flight tick. Include all of them so two
        // concurrently published sequences can never alias the same ring
        // slot merely because an earlier worker is delayed.
        const std::uint64_t reorder_guard =
            tick_source_count +
            static_cast<std::uint64_t>(config.store_worker_count);
        if (decoder_pending >
                std::numeric_limits<std::uint64_t>::max() -
                    history_pending - reorder_guard ||
            options.ipc_tick_ring_records <
                decoder_pending + history_pending + reorder_guard) {
            std::cerr
                << "mdl-production-router: --ipc-tick-ring-records "
                   "must cover the maximum in-flight mixed-tick "
                   "reorder span (minimum="
                << decoder_pending + history_pending + reorder_guard
                << ")\n";
            return 1;
        }
        ipc::RealtimeSharedServiceConfigV1 ipc_config{};
        ipc_config.run_id = run_id;
        ipc_config.trade_date = options.trade_date;
        ipc_config.registry = registry_result.registry.get();
        ipc_config.kline_windows = config.kline.windows;
        ipc_config.tick_ring_capacity =
            options.ipc_tick_ring_records;
        ipc_config.maximum_mapping_bytes =
            options.ipc_maximum_mapping_bytes;
        ipc_config.control_socket_path = options.ipc_socket;
        int ipc_system_error = 0;
        const ipc::RealtimeSharedServiceCreateErrorV1 ipc_error =
            ipc::RealtimeSharedMarketServiceV1::Create(
                std::move(ipc_config),
                &ipc_service,
                &ipc_system_error);
        if (ipc_error !=
                ipc::RealtimeSharedServiceCreateErrorV1::kNone ||
            ipc_service == nullptr) {
            std::cerr
                << "mdl-production-router: IPC service create failed: "
                << ipc::RealtimeSharedServiceCreateErrorNameV1(
                       ipc_error)
                << " errno=" << ipc_system_error << '\n';
            return 1;
        }
        config.applied_record_sink = ipc_service;
    }
#endif

    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    const runtime::RealtimePipelineCreateErrorV1 create_error =
        runtime::RealtimePipelineV1::Create(
            std::move(config), &pipeline, &detail);
    if (create_error != runtime::RealtimePipelineCreateErrorV1::kNone ||
        pipeline == nullptr) {
        std::cerr
            << "mdl-production-router: pipeline create failed: "
            << runtime::RealtimePipelineCreateErrorNameV1(create_error)
            << (detail.empty() ? "" : ": ") << detail << '\n';
        return 1;
    }
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
    if (ipc_service != nullptr) {
        int ipc_system_error = 0;
        if (!ipc_service->Start(&ipc_system_error)) {
            std::cerr
                << "mdl-production-router: IPC control start failed: "
                   "errno="
                << ipc_system_error << '\n';
            pipeline->StopAndDrain();
            ipc_service->MarkFailed();
            return 1;
        }
        std::cerr
            << "mdl-production-router: IPC active: socket="
            << ipc_service->control_socket_path()
            << " mapping_bytes=" << ipc_service->mapping_bytes()
            << '\n';
    }
#endif

    const auto interval =
        std::chrono::milliseconds(options.generation_interval_ms);
    const auto timeout =
        std::chrono::milliseconds(options.generation_timeout_ms);
    int exit_code = 0;
    bool wal_warning_reported = false;
    const auto report_wal_coverage =
        [&pipeline, &wal_warning_reported]() {
            const l2flow::realtime::OptionalWalSnapshotV1 wal =
                pipeline->Snapshot().wal;
            if (wal.enabled && wal.coverage_lost &&
                !wal_warning_reported) {
                std::cerr
                    << "mdl-production-router: optional WAL coverage lost; "
                       "realtime publication continues: failure="
                    << l2flow::realtime::OptionalWalFailureKindNameV1(
                           wal.failure_kind)
                    << " errno=" << wal.error_number
                    << " accepted=" << wal.accepted_records
                    << " written=" << wal.written_records
                    << " rejected=" << wal.rejected_records
                    << " abandoned=" << wal.abandoned_records << '\n';
                wal_warning_reported = true;
            }
        };
    report_wal_coverage();
    while (g_stop_requested == 0) {
        const IntervalWaitResult wait =
            WaitForInterval(interval, options.trade_date);
        if (wait == IntervalWaitResult::kSignal) {
            break;
        }
        if (wait == IntervalWaitResult::kClockFailure) {
            std::cerr
                << "mdl-production-router: civil-date clock read failed\n";
            exit_code = 1;
            break;
        }
        if (wait == IntervalWaitResult::kTradeDateBoundary) {
            std::cerr
                << "mdl-production-router: trade-date boundary reached; "
                   "publishing final prior-day generation\n";
            break;
        }
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
        if (ipc_service != nullptr && ipc_service->failed()) {
            std::cerr
                << "mdl-production-router: IPC service lost coverage\n";
            exit_code = 1;
            break;
        }
#endif
        const runtime::RealtimePipelineSnapshotV1 before_cut =
            pipeline->Snapshot();
        if (before_cut.trade_date_boundary_reached) {
            std::cerr
                << "mdl-production-router: callback observed trade-date "
                   "boundary; publishing final prior-day generation\n";
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
                    << "mdl-production-router: callback closed admission at "
                       "trade-date boundary; publishing final generation\n";
                break;
            }
            std::cerr
                << "mdl-production-router: generation failed: "
                << runtime::RealtimePipelineCutErrorNameV1(cut.error)
                << " generation="
                << market::RealtimeHistoryGenerationErrorNameV1(
                       cut.generation_error)
                << " factor="
                << l2flow::factor::RealtimeFactorPublishErrorNameV1(
                       cut.factor_result.error)
                << '\n';
            exit_code = 1;
            break;
        }
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
        if (ipc_service != nullptr && cut.kline_enabled &&
            (cut.kline_generation == nullptr ||
             !ipc_service->PublishKLineGeneration(
                 *cut.kline_generation))) {
            std::cerr
                << "mdl-production-router: IPC KLine publication "
                   "failed\n";
            exit_code = 1;
            break;
        }
#endif
        report_wal_coverage();
    }

#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
    if (ipc_service != nullptr) {
        ipc_service->MarkDraining();
    }
#endif
    if (exit_code == 0 && !pipeline->fatal()) {
        const runtime::RealtimePipelineCutResultV1 final_cut =
            pipeline->StopAndPublishFinalGeneration(timeout);
        if (!final_cut.published()) {
            std::cerr
                << "mdl-production-router: final generation failed: "
                << runtime::RealtimePipelineCutErrorNameV1(
                       final_cut.error)
                << '\n';
            exit_code = 1;
        }
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
        else if (
            ipc_service != nullptr && final_cut.kline_enabled &&
            (final_cut.kline_generation == nullptr ||
             !ipc_service->PublishKLineGeneration(
                 *final_cut.kline_generation))) {
            std::cerr
                << "mdl-production-router: final IPC KLine "
                   "publication failed\n";
            exit_code = 1;
        }
#endif
    } else {
        pipeline->StopAndDrain();
    }
    report_wal_coverage();
    const market::IntradayInstrumentStoreSnapshotV1 final_store =
        pipeline->Snapshot().store;
    const std::shared_ptr<const market::RealtimeKLineGenerationV1>
        final_kline = pipeline->AcquireLatestKLineGeneration();
    std::cerr
        << "mdl-production-router: intraday store final: records="
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
        << '\n';
    if (pipeline->fatal()) {
        exit_code = 1;
    }
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
    if (ipc_service != nullptr) {
        const std::uint64_t final_tick_sequence =
            pipeline->Snapshot().tick_stream_sequence;
        if (exit_code == 0 &&
            !ipc_service->MarkStoppedClean(final_tick_sequence)) {
            std::cerr
                << "mdl-production-router: IPC final tick prefix is "
                   "incomplete\n";
            exit_code = 1;
        }
        if (exit_code != 0) {
            ipc_service->MarkFailed();
        }
    }
#endif
    return exit_code;
}

}  // namespace

int main(int argc, char* argv[]) {
    Options options{};
    bool help = false;
    std::string error;
    if (!ParseOptions(argc, argv, &options, &help, &error)) {
        std::cerr << "mdl-production-router: " << error << '\n';
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
            << "mdl-production-router: cannot install stop handlers: errno="
            << signal_error << '\n';
        return 1;
    }
    return Run(options);
}
