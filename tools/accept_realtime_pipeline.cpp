#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/market/instrument_registry_loader_v1.h"
#include "l2flow/runtime/realtime_pipeline_v1.h"
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
#include "l2flow/ipc/realtime_shared_service_v1.h"
#endif

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <new>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sched.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

namespace {

namespace common = l2flow::common;
namespace factor = l2flow::factor;
namespace market = l2flow::market;
namespace runtime = l2flow::runtime;
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
namespace ipc = l2flow::ipc;
#endif

constexpr std::size_t kEventKindCount = 5U;
constexpr std::uint64_t kNanosecondsPerSecond = UINT64_C(1'000'000'000);

struct Options final {
    std::filesystem::path sdk_library;
    std::filesystem::path registry_directory;
    std::string registry_file;
    std::uint64_t registry_version = 0U;
    common::Sha256Digest registry_sha256{};
    bool registry_sha_set = false;
    std::uint32_t trade_date = 0U;
    std::string server_address;
    std::filesystem::path user_name_file;
    std::filesystem::path sdk_log_prefix;
    std::filesystem::path report_json;
    std::filesystem::path samples_csv;
    std::filesystem::path per_instrument_read_timings_csv;
    std::uint32_t duration_seconds = 600U;
    std::uint32_t generation_interval_ms = 1000U;
    std::uint32_t generation_timeout_ms = 10'000U;
    std::uint32_t instrument_store_workers = 4U;
    std::uint32_t acquire_repetitions = 32U;
    std::uint64_t intraday_store_maximum_records = 0U;
    std::uint64_t intraday_store_memory_bytes = 0U;
    std::uint32_t intraday_store_segment_kib = 64U;
    std::uint32_t intraday_store_batch_records = 64U * 1024U;
    std::uint32_t intraday_scan_batch_records = 1024U;
    std::uint32_t intraday_scan_workers = 1U;
    std::vector<std::uint32_t> intraday_reader_cpus;
    std::vector<std::uint32_t> kline_windows_ms;
    std::uint32_t kline_min_consecutive_bars = 0U;
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
    std::filesystem::path ipc_socket;
    std::uint64_t ipc_tick_ring_records = 262'144U;
    std::uint64_t ipc_maximum_mapping_bytes =
        std::uint64_t{2U} * 1024U * 1024U * 1024U;
    bool ipc_tick_ring_records_set = false;
    bool ipc_maximum_mapping_set = false;
#endif
    bool intraday_scan_batch_records_set = false;
    bool intraday_reader_cpus_set = false;
    bool intraday_store_from_open = false;
    bool intraday_store_maximum_records_set = false;
    bool intraday_store_memory_set = false;
    bool measure_stage_latency = false;
    bool partial_session = false;
};

struct UnsignedDistribution final {
    void Add(std::uint64_t value) { values.push_back(value); }
    std::vector<std::uint64_t> values;
};

struct SignedDistribution final {
    void Add(std::int64_t value) { values.push_back(value); }
    std::vector<std::int64_t> values;
};

struct SampleRow final {
    std::uint64_t elapsed_ns = 0U;
    std::uint64_t accepted = 0U;
    std::uint64_t decoded = 0U;
    std::uint64_t accepted_delta = 0U;
    std::uint64_t decoded_delta = 0U;
    std::array<std::uint64_t, market::kRealtimeHistorySourceCountV1>
        source_delta{};
    std::uint64_t generation = 0U;
    std::uint64_t ingress_prefix = 0U;
    std::uint64_t cut_latency_ns = 0U;
    std::uint64_t acquire_p50_ns = 0U;
    std::uint64_t acquire_p99_ns = 0U;
    std::uint64_t generation_age_ns = 0U;
    std::uint64_t global_head_recv_age_ns = 0U;
    std::int64_t global_head_event_age_ns = 0;
    std::uint64_t updated_heads = 0U;
    std::uint64_t updated_recv_age_p50_ns = 0U;
    std::uint64_t updated_recv_age_p99_ns = 0U;
    std::int64_t updated_event_age_p50_ns = 0;
    std::int64_t updated_event_age_p99_ns = 0;
};

struct AcceptanceState final {
    bool valid = true;
    std::string first_error;
    std::uint64_t generations = 0U;
    std::uint64_t universe_rows_checked = 0U;
    std::uint64_t intraday_full_scan_records = 0U;
    std::uint64_t intraday_full_scan_ns = 0U;
    std::uint64_t intraday_scan_partition_ns = 0U;
    std::uint64_t intraday_scan_total_ns = 0U;
    std::uint64_t intraday_scan_ingress_sum = 0U;
    std::uint64_t intraday_scan_ingress_xor = 0U;
    std::uint32_t intraday_scan_last_instrument_id = 0U;
    std::uint64_t intraday_scan_last_ingress_sequence = 0U;
    std::array<std::uint64_t, kEventKindCount>
        intraday_scan_kind_counts{};
    std::vector<std::uint32_t> intraday_reader_cpus;
    std::vector<std::size_t> intraday_scan_ordinal_begins;
    std::vector<std::size_t> intraday_scan_ordinal_ends;
    std::vector<std::uint64_t> intraday_scan_shard_records;
    std::vector<std::uint64_t> intraday_scan_shard_ns;
    std::vector<std::uint32_t>
        intraday_scan_shard_last_instrument_ids;
    std::vector<std::uint64_t>
        intraday_scan_shard_last_ingress_sequences;
    std::uint64_t updated_heads = 0U;
    std::uint64_t event_time_samples = 0U;
    std::array<bool, kEventKindCount> event_kinds_seen{};
    std::vector<std::uint64_t> last_seen_head_by_universe_index;
    UnsignedDistribution cut_latency_ns;
    UnsignedDistribution acquire_latency_ns;
    UnsignedDistribution generation_age_ns;
    UnsignedDistribution global_head_recv_age_ns;
    SignedDistribution global_head_event_age_ns;
    UnsignedDistribution updated_head_recv_age_ns;
    SignedDistribution updated_head_event_age_ns;
    std::vector<SampleRow> samples;
    struct PerInstrumentDirectReadTiming final {
        std::size_t ordinal = 0U;
        std::uint32_t instrument_id = 0U;
        std::uint64_t record_count = 0U;
        std::uint64_t open_ns = 0U;
        std::uint64_t read_ns = 0U;
        std::uint64_t total_ns = 0U;
    };
    bool per_instrument_direct_read_enabled = false;
    std::uint32_t per_instrument_direct_read_cpu = 0U;
    std::uint64_t per_instrument_direct_read_pass_ns = 0U;
    std::uint64_t per_instrument_direct_read_records = 0U;
    std::uint64_t per_instrument_direct_read_nonempty = 0U;
    std::uint64_t per_instrument_direct_read_open_ns = 0U;
    std::uint64_t per_instrument_direct_read_read_ns = 0U;
    std::uint64_t per_instrument_direct_read_total_ns = 0U;
    UnsignedDistribution per_instrument_direct_read_all_total_ns;
    UnsignedDistribution per_instrument_direct_read_nonempty_total_ns;
    UnsignedDistribution per_instrument_direct_read_nonempty_read_ns;
    UnsignedDistribution per_instrument_direct_read_nonempty_ns_per_record;
    std::vector<PerInstrumentDirectReadTiming>
        per_instrument_direct_read_timings;
};

struct KLineWindowAcceptance final {
    std::uint32_t window_id = 0U;
    std::uint64_t duration_ns = 0U;
    std::uint64_t bars = 0U;
    std::uint64_t instruments_with_bars = 0U;
    std::uint64_t trades = 0U;
    std::uint32_t representative_instrument_id = 0U;
    std::uint64_t longest_consecutive_bars = 0U;
    std::vector<market::KLineBarV1> representative_bars;
};

struct KLineAcceptance final {
    bool enabled = false;
    std::uint64_t generation_bar_count = 0U;
    std::vector<KLineWindowAcceptance> windows;
};

class FileDescriptor final {
public:
    explicit FileDescriptor(int value = -1) noexcept : value_(value) {}
    ~FileDescriptor() {
        if (value_ >= 0) {
            static_cast<void>(::close(value_));
        }
    }
    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;
    [[nodiscard]] int get() const noexcept { return value_; }
    [[nodiscard]] bool valid() const noexcept { return value_ >= 0; }

private:
    int value_ = -1;
};

void Fail(AcceptanceState* state, std::string message) {
    state->valid = false;
    if (state->first_error.empty()) {
        state->first_error = std::move(message);
    }
}

[[nodiscard]] bool ClockNs(clockid_t clock, std::uint64_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    struct timespec value {};
    if (::clock_gettime(clock, &value) != 0 || value.tv_sec < 0 ||
        value.tv_nsec < 0 || value.tv_nsec >= 1'000'000'000L) {
        return false;
    }
    const std::uint64_t seconds = static_cast<std::uint64_t>(value.tv_sec);
    if (seconds >
        (std::numeric_limits<std::uint64_t>::max() -
         static_cast<std::uint64_t>(value.tv_nsec)) /
            kNanosecondsPerSecond) {
        return false;
    }
    *output = seconds * kNanosecondsPerSecond +
              static_cast<std::uint64_t>(value.tv_nsec);
    return true;
}

[[nodiscard]] bool ParseU32(
    std::string_view text,
    std::uint32_t* output) noexcept {
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

[[nodiscard]] bool ParseU64(
    std::string_view text,
    std::uint64_t* output) noexcept {
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

[[nodiscard]] bool ParseCpuList(
    std::string_view text,
    std::vector<std::uint32_t>* output,
    std::string* error) {
    if (output == nullptr || error == nullptr || text.empty()) {
        return false;
    }
    std::vector<std::uint32_t> parsed;
    std::size_t begin = 0U;
    for (;;) {
        const std::size_t comma = text.find(',', begin);
        const std::size_t end =
            comma == std::string_view::npos ? text.size() : comma;
        const std::string_view token = text.substr(begin, end - begin);
        std::uint32_t cpu = 0U;
        if (token.empty() || !ParseU32(token, &cpu) ||
            cpu >= static_cast<std::uint32_t>(CPU_SETSIZE)) {
            *error =
                "--intraday-reader-cpus must be a comma-separated list "
                "of CPU IDs below CPU_SETSIZE";
            return false;
        }
        if (std::find(parsed.begin(), parsed.end(), cpu) != parsed.end()) {
            *error = "--intraday-reader-cpus rejects duplicate CPU IDs";
            return false;
        }
        parsed.push_back(cpu);
        if (comma == std::string_view::npos) {
            break;
        }
        begin = comma + 1U;
        if (begin == text.size()) {
            *error = "--intraday-reader-cpus rejects an empty CPU ID";
            return false;
        }
    }
    *output = std::move(parsed);
    return true;
}

[[nodiscard]] bool ParseKLineWindows(
    std::string_view text,
    std::vector<std::uint32_t>* output,
    std::string* error) {
    if (output == nullptr || error == nullptr || text.empty()) {
        return false;
    }
    std::vector<std::uint32_t> parsed;
    std::size_t begin = 0U;
    for (;;) {
        const std::size_t comma = text.find(',', begin);
        const std::size_t end =
            comma == std::string_view::npos ? text.size() : comma;
        std::uint32_t duration_ms = 0U;
        if (end == begin ||
            !ParseU32(text.substr(begin, end - begin), &duration_ms) ||
            duration_ms == 0U || duration_ms > 86'400'000U ||
            std::find(parsed.begin(), parsed.end(), duration_ms) !=
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
        begin = comma + 1U;
        if (begin == text.size()) {
            *error = "--kline-windows-ms rejects an empty duration";
            return false;
        }
    }
    *output = std::move(parsed);
    return true;
}

[[nodiscard]] bool SafeFileName(std::string_view value) noexcept {
    return !value.empty() && value != "." && value != ".." &&
           value.find('/') == std::string_view::npos &&
           value.find('\0') == std::string_view::npos;
}

void PrintUsage(std::ostream& output) {
    output
        << "Usage: accept-realtime-pipeline [options]\n"
        << "Required:\n"
        << "  --sdk-library ABS\n"
        << "  --registry-directory ABS\n"
        << "  --registry-file NAME\n"
        << "  --registry-version N\n"
        << "  --registry-sha256 HEX64\n"
        << "  --trade-date YYYYMMDD\n"
        << "  --server-address HOST:PORT\n"
        << "  --user-name-file ABS\n"
        << "  --sdk-log-prefix ABS\n"
        << "  --report-json ABS\n"
        << "  --samples-csv ABS\n"
        << "  --intraday-store-max-records N\n"
        << "                                positive u64 session record cap\n"
        << "  --intraday-store-memory-gib N positive u64 logical total GiB cap\n"
        << "Coverage mode (choose exactly one):\n"
        << "  --intraday-store-from-open    require continuous coverage from "
           "market open\n"
        << "  --partial-session             latency-diagnostic start without "
           "claiming coverage from market open; requires "
           "--measure-stage-latency\n"
        << "Optional:\n"
        << "  --per-instrument-read-timings-csv ABS\n"
        << "                                post-drain direct full-history read "
           "timings\n"
        << "  --duration-seconds N          default 600\n"
        << "  --generation-interval-ms N    default 1000\n"
        << "  --generation-timeout-ms N     default 10000\n"
        << "  --instrument-store-workers N  default 4\n"
        << "  --acquire-repetitions N       default 32\n"
        << "  --intraday-store-segment-kib N\n"
        << "                                4..16384, default 64\n"
        << "  --intraday-store-batch-records N\n"
        << "                                Store ReadBatch upper bound; "
           "1..1048576, default 65536\n"
        << "  --intraday-scan-batch-records N\n"
        << "                                actual final-scan page; "
           "1..Store upper bound, default min(1024, Store upper bound)\n"
        << "  --intraday-scan-workers N     independent ordinal-range readers; "
           "1..256, default 1\n"
        << "  --intraday-reader-cpus LIST   one allowed CPU per scan worker; "
           "default first allowed CPUs\n"
        << "  --kline-windows-ms LIST       comma-separated event-time windows; "
           "duration-ms is window_id\n"
        << "  --kline-min-consecutive-bars N\n"
        << "                                require one instrument/window to "
           "contain at least N consecutive non-empty bars\n"
        << "  --measure-stage-latency       enable per-message callback/append "
           "latency histograms\n"
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
        << "  --ipc-socket ABS              publish the same accepted Store path "
           "through read-only memfd/UDS IPC\n"
        << "  --ipc-tick-ring-records N     mixed-tick ring capacity, default "
           "262144\n"
        << "  --ipc-max-mapping-mib N       shared-memory hard cap, default "
           "2048 MiB\n"
#endif
        ;
}

[[nodiscard]] bool TakeValue(
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

[[nodiscard]] bool ParseOptions(
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
        if (option == "--intraday-store-from-open") {
            if (!seen.insert(option).second) {
                *error = "duplicate --intraday-store-from-open";
                return false;
            }
            parsed.intraday_store_from_open = true;
            continue;
        }
        if (option == "--measure-stage-latency") {
            if (!seen.insert(option).second) {
                *error = "duplicate --measure-stage-latency";
                return false;
            }
            parsed.measure_stage_latency = true;
            continue;
        }
        if (option == "--partial-session") {
            if (!seen.insert(option).second) {
                *error = "duplicate --partial-session";
                return false;
            }
            parsed.partial_session = true;
            continue;
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
            parsed.sdk_library = value;
        } else if (option == "--registry-directory") {
            parsed.registry_directory = value;
        } else if (option == "--registry-file") {
            parsed.registry_file = value;
        } else if (option == "--registry-version") {
            if (!ParseU64(value, &parsed.registry_version)) {
                *error = "invalid --registry-version";
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
                *error = "invalid --trade-date";
                return false;
            }
        } else if (option == "--server-address") {
            parsed.server_address = value;
        } else if (option == "--user-name-file") {
            parsed.user_name_file = value;
        } else if (option == "--sdk-log-prefix") {
            parsed.sdk_log_prefix = value;
        } else if (option == "--report-json") {
            parsed.report_json = value;
        } else if (option == "--samples-csv") {
            parsed.samples_csv = value;
        } else if (option == "--per-instrument-read-timings-csv") {
            parsed.per_instrument_read_timings_csv = value;
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
        } else if (option == "--ipc-socket") {
            parsed.ipc_socket = value;
        } else if (option == "--ipc-tick-ring-records") {
            if (!ParseU64(value, &parsed.ipc_tick_ring_records) ||
                parsed.ipc_tick_ring_records == 0U) {
                *error = "--ipc-tick-ring-records must be positive u64";
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
                    "--ipc-max-mapping-mib must be a positive u64 whose "
                    "byte conversion does not overflow";
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
                static_cast<std::size_t>(
                    parsed.intraday_store_batch_records) >
                    market::
                        kIntradayInstrumentStoreMaximumBatchRecordsV1) {
                *error =
                    "--intraday-store-batch-records must be 1..1048576";
                return false;
            }
        } else if (option == "--intraday-scan-batch-records") {
            if (!ParseU32(
                    value, &parsed.intraday_scan_batch_records) ||
                parsed.intraday_scan_batch_records == 0U ||
                static_cast<std::size_t>(
                    parsed.intraday_scan_batch_records) >
                    market::
                        kIntradayInstrumentStoreMaximumBatchRecordsV1) {
                *error =
                    "--intraday-scan-batch-records must be 1..1048576";
                return false;
            }
            parsed.intraday_scan_batch_records_set = true;
        } else if (option == "--intraday-reader-cpus") {
            if (!ParseCpuList(
                    value, &parsed.intraday_reader_cpus, error)) {
                return false;
            }
            parsed.intraday_reader_cpus_set = true;
        } else if (option == "--kline-windows-ms") {
            if (!ParseKLineWindows(
                    value, &parsed.kline_windows_ms, error)) {
                return false;
            }
        } else {
            std::uint32_t number = 0U;
            if (!ParseU32(value, &number) || number == 0U) {
                *error = "invalid numeric option: " + std::string(option);
                return false;
            }
            if (option == "--duration-seconds") {
                parsed.duration_seconds = number;
            } else if (option == "--generation-interval-ms") {
                parsed.generation_interval_ms = number;
            } else if (option == "--generation-timeout-ms") {
                parsed.generation_timeout_ms = number;
            } else if (option == "--instrument-store-workers") {
                parsed.instrument_store_workers = number;
            } else if (option == "--acquire-repetitions") {
                parsed.acquire_repetitions = number;
            } else if (option == "--intraday-scan-workers") {
                parsed.intraday_scan_workers = number;
            } else if (option == "--kline-min-consecutive-bars") {
                parsed.kline_min_consecutive_bars = number;
            } else {
                *error = "unknown option: " + std::string(option);
                return false;
            }
        }
    }
    const auto absolute = [](const std::filesystem::path& path) {
        return path.is_absolute() &&
               path.native().find('\0') == std::string::npos;
    };
    if (!absolute(parsed.sdk_library) ||
        !absolute(parsed.registry_directory) ||
        !SafeFileName(parsed.registry_file) ||
        parsed.registry_version == 0U || !parsed.registry_sha_set ||
        parsed.trade_date == 0U || parsed.server_address.empty() ||
        !absolute(parsed.user_name_file) ||
        !absolute(parsed.sdk_log_prefix) ||
        !absolute(parsed.report_json) || !absolute(parsed.samples_csv) ||
        (!parsed.per_instrument_read_timings_csv.empty() &&
         !absolute(parsed.per_instrument_read_timings_csv)) ||
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
        (!parsed.ipc_socket.empty() && !absolute(parsed.ipc_socket)) ||
#endif
        parsed.duration_seconds > 86'400U ||
        parsed.generation_interval_ms > 60'000U ||
        parsed.generation_timeout_ms > 600'000U ||
        parsed.instrument_store_workers > 256U ||
        parsed.acquire_repetitions > 10'000U ||
        parsed.intraday_scan_workers > 256U) {
        *error = "required option missing or option is out of range";
        return false;
    }
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
    if (parsed.ipc_socket.empty() &&
        (parsed.ipc_tick_ring_records_set ||
         parsed.ipc_maximum_mapping_set)) {
        *error =
            "--ipc-tick-ring-records and --ipc-max-mapping-mib require "
            "--ipc-socket";
        return false;
    }
#endif
    if ((!parsed.per_instrument_read_timings_csv.empty() &&
         (parsed.per_instrument_read_timings_csv == parsed.report_json ||
          parsed.per_instrument_read_timings_csv == parsed.samples_csv)) ||
        parsed.report_json == parsed.samples_csv) {
        *error = "report and CSV output paths must be distinct";
        return false;
    }
    if (!parsed.intraday_scan_batch_records_set &&
        parsed.intraday_scan_batch_records >
            parsed.intraday_store_batch_records) {
        parsed.intraday_scan_batch_records =
            parsed.intraday_store_batch_records;
    } else if (parsed.intraday_scan_batch_records >
               parsed.intraday_store_batch_records) {
        *error =
            "--intraday-scan-batch-records cannot exceed "
            "--intraday-store-batch-records";
        return false;
    }
    if (parsed.intraday_reader_cpus_set &&
        parsed.intraday_reader_cpus.size() !=
            static_cast<std::size_t>(
                parsed.intraday_scan_workers)) {
        *error =
            "--intraday-reader-cpus must contain exactly one CPU per "
            "--intraday-scan-workers";
        return false;
    }
    if (!parsed.intraday_store_maximum_records_set ||
        !parsed.intraday_store_memory_set) {
        *error =
            "store-only acceptance requires explicit positive "
            "--intraday-store-max-records and --intraday-store-memory-gib";
        return false;
    }
    if (parsed.partial_session == parsed.intraday_store_from_open ||
        (parsed.partial_session && !parsed.measure_stage_latency)) {
        *error =
            "select exactly one of --intraday-store-from-open or "
            "--partial-session; partial session is allowed only with "
            "--measure-stage-latency";
        return false;
    }
    if (parsed.kline_min_consecutive_bars != 0U &&
        parsed.kline_windows_ms.empty()) {
        *error =
            "--kline-min-consecutive-bars requires --kline-windows-ms";
        return false;
    }
    *output = std::move(parsed);
    return true;
}

[[nodiscard]] bool ReadSecret(
    const std::filesystem::path& path,
    std::string* output,
    std::string* error) {
    FileDescriptor fd(::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
    if (!fd.valid()) {
        *error = "cannot open user-name file";
        return false;
    }
    struct stat metadata {};
    if (::fstat(fd.get(), &metadata) != 0 || !S_ISREG(metadata.st_mode) ||
        metadata.st_size <= 0 || metadata.st_size > 4096) {
        *error = "user-name file metadata is invalid";
        return false;
    }
    std::string value(static_cast<std::size_t>(metadata.st_size), '\0');
    std::size_t offset = 0U;
    while (offset < value.size()) {
        const ssize_t count = ::read(
            fd.get(), value.data() + offset, value.size() - offset);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            *error = "cannot read complete user-name file";
            return false;
        }
        offset += static_cast<std::size_t>(count);
    }
    while (!value.empty() &&
           (value.back() == '\n' || value.back() == '\r')) {
        value.pop_back();
    }
    if (value.empty() || value.find('\0') != std::string::npos) {
        *error = "user-name file content is invalid";
        return false;
    }
    *output = std::move(value);
    return true;
}

[[nodiscard]] bool ResolveReaderCpus(
    const Options& options,
    std::vector<std::uint32_t>* output,
    std::string* error) {
    if (output == nullptr || error == nullptr) {
        return false;
    }
    cpu_set_t allowed;
    CPU_ZERO(&allowed);
    if (::sched_getaffinity(0, sizeof(allowed), &allowed) != 0) {
        *error =
            "cannot read inherited CPU affinity for final scan: errno=" +
            std::to_string(errno);
        return false;
    }

    std::vector<std::uint32_t> resolved;
    resolved.reserve(options.intraday_scan_workers);
    if (options.intraday_reader_cpus_set) {
        for (const std::uint32_t cpu : options.intraday_reader_cpus) {
            if (cpu >= static_cast<std::uint32_t>(CPU_SETSIZE) ||
                CPU_ISSET(static_cast<int>(cpu), &allowed) == 0) {
                *error =
                    "--intraday-reader-cpus includes a CPU outside the "
                    "process inherited affinity mask";
                return false;
            }
            resolved.push_back(cpu);
        }
    } else {
        for (int cpu = 0;
             cpu < CPU_SETSIZE &&
             resolved.size() <
                 static_cast<std::size_t>(
                     options.intraday_scan_workers);
             ++cpu) {
            if (CPU_ISSET(cpu, &allowed) != 0) {
                resolved.push_back(static_cast<std::uint32_t>(cpu));
            }
        }
    }
    if (resolved.size() !=
        static_cast<std::size_t>(options.intraday_scan_workers)) {
        *error =
            "final scan requires one distinct allowed CPU per scan worker";
        return false;
    }
    *output = std::move(resolved);
    return true;
}

[[nodiscard]] bool PinCurrentThread(
    std::uint32_t cpu,
    int* error_number) noexcept {
    if (error_number != nullptr) {
        *error_number = 0;
    }
    if (cpu >= static_cast<std::uint32_t>(CPU_SETSIZE)) {
        if (error_number != nullptr) {
            *error_number = EINVAL;
        }
        return false;
    }
    cpu_set_t requested;
    CPU_ZERO(&requested);
    CPU_SET(static_cast<int>(cpu), &requested);
    if (::sched_setaffinity(0, sizeof(requested), &requested) != 0) {
        if (error_number != nullptr) {
            *error_number = errno;
        }
        return false;
    }
    return true;
}

template <typename T>
[[nodiscard]] T QuantileOfSorted(
    std::span<const T> sorted,
    long double quantile) noexcept {
    if (sorted.empty()) {
        return T{};
    }
    const long double position =
        quantile * static_cast<long double>(sorted.size() - 1U);
    const std::size_t index = static_cast<std::size_t>(std::ceil(position));
    return sorted[index];
}

template <typename T>
[[nodiscard]] T Quantile(std::vector<T> values, long double quantile) {
    std::sort(values.begin(), values.end());
    return QuantileOfSorted<T>(values, quantile);
}

void WriteJsonString(std::ostream& output, std::string_view value) {
    output.put('"');
    for (const unsigned char byte : value) {
        switch (byte) {
            case '"': output << "\\\""; break;
            case '\\': output << "\\\\"; break;
            case '\n': output << "\\n"; break;
            case '\r': output << "\\r"; break;
            case '\t': output << "\\t"; break;
            default:
                if (byte < 0x20U) {
                    output << "\\u00" << std::hex << std::setw(2)
                           << std::setfill('0')
                           << static_cast<unsigned int>(byte) << std::dec
                           << std::setfill(' ');
                } else {
                    output.put(static_cast<char>(byte));
                }
                break;
        }
    }
    output.put('"');
}

template <typename T>
void WriteDistributionJson(
    std::ostream& output,
    std::vector<T> values) {
    std::sort(values.begin(), values.end());
    long double sum = 0.0L;
    for (const T value : values) {
        sum += static_cast<long double>(value);
    }
    output << "{\"count\":" << values.size();
    if (values.empty()) {
        output << ",\"min\":0,\"p50\":0,\"p90\":0,\"p95\":0,"
                  "\"p99\":0,\"p999\":0,\"max\":0,\"mean\":0}";
        return;
    }
    output << ",\"min\":" << values.front()
           << ",\"p50\":" << QuantileOfSorted<T>(values, 0.50L)
           << ",\"p90\":" << QuantileOfSorted<T>(values, 0.90L)
           << ",\"p95\":" << QuantileOfSorted<T>(values, 0.95L)
           << ",\"p99\":" << QuantileOfSorted<T>(values, 0.99L)
           << ",\"p999\":" << QuantileOfSorted<T>(values, 0.999L)
           << ",\"max\":" << values.back()
           << ",\"mean\":" << std::fixed << std::setprecision(3)
           << (sum / static_cast<long double>(values.size()))
           << std::defaultfloat << '}';
}

void WriteLatencyQuantileJson(
    std::ostream& output,
    const runtime::RealtimeLatencyQuantileV1& value) {
    output << "{\"estimate\":" << value.estimate_ns
           << ",\"lower_bound\":" << value.lower_bound_ns
           << ",\"upper_bound\":" << value.upper_bound_ns
           << ",\"clipped_below\":"
           << (value.clipped_below ? "true" : "false")
           << ",\"clipped_above\":"
           << (value.clipped_above ? "true" : "false") << '}';
}

void WriteStageLatencyDistributionJson(
    std::ostream& output,
    const runtime::RealtimeLatencyDistributionV1& value) {
    output << "{\"count\":" << value.samples
           << ",\"invalid_samples\":" << value.invalid_samples
           << ",\"below_histogram_range\":"
           << value.below_histogram_range
           << ",\"above_histogram_range\":"
           << value.above_histogram_range
           << ",\"min\":" << value.minimum_ns
           << ",\"max\":" << value.maximum_ns
           << ",\"mean\":" << value.mean_ns
           << ",\"histogram_minimum\":"
           << value.histogram_minimum_ns
           << ",\"histogram_maximum\":"
           << value.histogram_maximum_ns
           << ",\"histogram_bucket_width\":"
           << value.histogram_bucket_width_ns
           << ",\"sum_saturated\":"
           << (value.sum_saturated ? "true" : "false")
           << ",\"p50\":";
    WriteLatencyQuantileJson(output, value.p50);
    output << ",\"p90\":";
    WriteLatencyQuantileJson(output, value.p90);
    output << ",\"p95\":";
    WriteLatencyQuantileJson(output, value.p95);
    output << ",\"p99\":";
    WriteLatencyQuantileJson(output, value.p99);
    output << ",\"p999\":";
    WriteLatencyQuantileJson(output, value.p999);
    output << '}';
}

[[nodiscard]] std::size_t EventKindIndex(
    market::MarketEventKindV1 kind) noexcept {
    const std::uint8_t raw = static_cast<std::uint8_t>(kind);
    return raw == 0U || raw > kEventKindCount
               ? kEventKindCount
               : static_cast<std::size_t>(raw - 1U);
}

[[nodiscard]] bool SnapshotKind(market::MarketEventKindV1 kind) noexcept {
    return kind == market::MarketEventKindV1::kShanghaiSnapshot ||
           kind == market::MarketEventKindV1::kShenzhenSnapshot;
}

struct IntradayScanOrdinalRange final {
    std::size_t begin = 0U;
    std::size_t end = 0U;
    std::uint64_t expected_records = 0U;
};

enum class IntradayScanFailure : std::uint8_t {
    kNone = 0U,
    kNotStarted,
    kAffinity,
    kClock,
    kSummary,
    kOpenCursor,
    kReadCursor,
    kInvalidTerminalPage,
    kInvalidRecord,
    kInvalidOrder,
    kInvalidBoundary,
    kInvalidWatermark,
    kCounterOverflow,
    kAllocation,
    kUnexpected,
};

struct IntradayScanShardResult final {
    IntradayScanFailure failure = IntradayScanFailure::kNotStarted;
    market::IntradayInstrumentStoreQueryErrorV1 query_error =
        market::IntradayInstrumentStoreQueryErrorV1::kNone;
    int affinity_error_number = 0;
    std::uint64_t elapsed_ns = 0U;
    std::uint64_t record_count = 0U;
    std::array<
        std::uint64_t,
        market::kIntradayInstrumentStoreSourceCountV1>
        source_counts{};
    std::array<std::uint64_t, kEventKindCount> kind_counts{};
    std::uint64_t ingress_sequence_sum = 0U;
    std::uint64_t ingress_sequence_xor = 0U;
    std::uint32_t first_instrument_id = 0U;
    std::uint64_t first_ingress_sequence = 0U;
    std::uint32_t last_instrument_id = 0U;
    std::uint64_t last_ingress_sequence = 0U;
    bool have_record = false;
    bool observed_terminal_page = false;
};

[[nodiscard]] std::string_view IntradayScanFailureName(
    IntradayScanFailure failure) noexcept {
    switch (failure) {
        case IntradayScanFailure::kNone: return "none";
        case IntradayScanFailure::kNotStarted: return "not_started";
        case IntradayScanFailure::kAffinity: return "affinity";
        case IntradayScanFailure::kClock: return "clock";
        case IntradayScanFailure::kSummary: return "summary";
        case IntradayScanFailure::kOpenCursor: return "open_cursor";
        case IntradayScanFailure::kReadCursor: return "read_cursor";
        case IntradayScanFailure::kInvalidTerminalPage:
            return "invalid_terminal_page";
        case IntradayScanFailure::kInvalidRecord:
            return "invalid_record";
        case IntradayScanFailure::kInvalidOrder:
            return "invalid_order";
        case IntradayScanFailure::kInvalidBoundary:
            return "invalid_boundary";
        case IntradayScanFailure::kInvalidWatermark:
            return "invalid_watermark";
        case IntradayScanFailure::kCounterOverflow:
            return "counter_overflow";
        case IntradayScanFailure::kAllocation: return "allocation";
        case IntradayScanFailure::kUnexpected: return "unexpected";
    }
    return "unknown";
}

[[nodiscard]] std::uint64_t DenseSequenceSumModuloU64(
    std::uint64_t count) noexcept {
    std::uint64_t left = count;
    std::uint64_t right = count + 1U;
    if ((left & 1U) == 0U) {
        left /= 2U;
    } else {
        right /= 2U;
    }
    return left * right;
}

[[nodiscard]] std::uint64_t DenseSequenceXor(
    std::uint64_t count) noexcept {
    switch (count & 3U) {
        case 0U: return count;
        case 1U: return 1U;
        case 2U: return count + 1U;
        case 3U: return 0U;
    }
    return 0U;
}

[[nodiscard]] bool BuildIntradayScanRanges(
    const market::IntradayInstrumentStoreGenerationV1& generation,
    std::uint32_t worker_count,
    std::vector<IntradayScanOrdinalRange>* output,
    std::string* error) {
    if (output == nullptr || error == nullptr || worker_count == 0U) {
        return false;
    }
    output->clear();
    output->reserve(worker_count);
    const std::size_t instrument_count = generation.instrument_count();
    if (worker_count == 1U) {
        output->push_back(IntradayScanOrdinalRange{
            0U, instrument_count, generation.record_count()});
        return true;
    }

    std::vector<std::uint64_t> prefix(instrument_count + 1U, 0U);
    for (std::size_t ordinal = 0U;
         ordinal < instrument_count;
         ++ordinal) {
        market::IntradayInstrumentSummaryV1 summary{};
        const auto query_error =
            generation.SummaryAt(ordinal, &summary);
        if (query_error !=
            market::IntradayInstrumentStoreQueryErrorV1::kNone) {
            *error =
                "cannot build record-balanced ordinal scan ranges: " +
                std::string(
                    market::IntradayInstrumentStoreQueryErrorNameV1(
                        query_error));
            return false;
        }
        if (prefix[ordinal] >
            std::numeric_limits<std::uint64_t>::max() -
                summary.record_count) {
            *error = "ordinal scan range record prefix overflow";
            return false;
        }
        prefix[ordinal + 1U] =
            prefix[ordinal] + summary.record_count;
    }
    if (prefix.back() != generation.record_count()) {
        *error = "ordinal scan range summary total mismatch";
        return false;
    }

    std::vector<std::size_t> boundaries(
        static_cast<std::size_t>(worker_count) + 1U, 0U);
    boundaries.back() = instrument_count;
    const std::uint64_t quotient =
        generation.record_count() / worker_count;
    const std::uint64_t remainder =
        generation.record_count() % worker_count;
    for (std::uint32_t worker = 1U;
         worker < worker_count;
         ++worker) {
        const std::uint64_t worker_u64 =
            static_cast<std::uint64_t>(worker);
        const std::uint64_t target =
            quotient * worker_u64 +
            std::min(worker_u64, remainder);
        const auto boundary =
            std::lower_bound(prefix.begin(), prefix.end(), target);
        boundaries[worker] = static_cast<std::size_t>(
            boundary - prefix.begin());
    }
    for (std::uint32_t worker = 0U;
         worker < worker_count;
         ++worker) {
        const std::size_t begin = boundaries[worker];
        const std::size_t end = boundaries[worker + 1U];
        if (begin > end || end > instrument_count) {
            *error = "ordinal scan range partition is invalid";
            return false;
        }
        output->push_back(IntradayScanOrdinalRange{
            begin, end, prefix[end] - prefix[begin]});
    }
    return true;
}

void ScanIntradayOrdinalRange(
    const market::IntradayInstrumentStoreGenerationV1& generation,
    const IntradayScanOrdinalRange& range,
    std::size_t batch_capacity,
    std::uint32_t reader_cpu,
    IntradayScanShardResult* result) noexcept {
    if (result == nullptr) {
        return;
    }
    *result = IntradayScanShardResult{};
    try {
        if (!PinCurrentThread(
                reader_cpu, &result->affinity_error_number)) {
            result->failure = IntradayScanFailure::kAffinity;
            return;
        }
        std::vector<const market::RealtimeHistoryRecordV1*> batch(
            batch_capacity);

        std::uint64_t start_ns = 0U;
        if (!ClockNs(CLOCK_MONOTONIC, &start_ns)) {
            result->failure = IntradayScanFailure::kClock;
            return;
        }

        std::uint32_t minimum_instrument_id = 0U;
        std::uint32_t maximum_instrument_id = 0U;
        if (range.begin != range.end) {
            market::IntradayInstrumentSummaryV1 first{};
            market::IntradayInstrumentSummaryV1 last{};
            const auto first_error =
                generation.SummaryAt(range.begin, &first);
            const auto last_error =
                generation.SummaryAt(range.end - 1U, &last);
            if (first_error !=
                    market::IntradayInstrumentStoreQueryErrorV1::kNone ||
                last_error !=
                    market::IntradayInstrumentStoreQueryErrorV1::kNone ||
                first.instrument_id == 0U ||
                last.instrument_id < first.instrument_id) {
                result->query_error =
                    first_error !=
                            market::IntradayInstrumentStoreQueryErrorV1::
                                kNone
                        ? first_error
                        : last_error;
                result->failure = IntradayScanFailure::kSummary;
                return;
            }
            minimum_instrument_id = first.instrument_id;
            maximum_instrument_id = last.instrument_id;
        }

        market::IntradayInstrumentScanOptionsV1 scan_options{};
        scan_options.ingress_sequence_end_exclusive =
            generation.watermark().ingress_sequence_exclusive;
        std::unique_ptr<market::IntradayUniverseCursorV1> cursor;
        result->query_error = generation.OpenUniverseRangeCursor(
            range.begin, range.end, scan_options, &cursor);
        if (result->query_error !=
                market::IntradayInstrumentStoreQueryErrorV1::kNone ||
            cursor == nullptr) {
            result->failure = IntradayScanFailure::kOpenCursor;
            return;
        }

        std::uint32_t previous_instrument_id = 0U;
        std::uint64_t previous_instrument_ingress = 0U;
        bool have_previous_record = false;
        for (;;) {
            std::size_t written =
                std::numeric_limits<std::size_t>::max();
            result->query_error = cursor->ReadBatch(
                std::span<
                    const market::RealtimeHistoryRecordV1*>(
                    batch.data(), batch.size()),
                &written);
            if (result->query_error !=
                market::IntradayInstrumentStoreQueryErrorV1::kNone) {
                result->failure = IntradayScanFailure::kReadCursor;
                return;
            }
            if (written > batch.size()) {
                result->failure =
                    IntradayScanFailure::kInvalidTerminalPage;
                return;
            }
            if (written == 0U) {
                if (!cursor->done()) {
                    result->failure =
                        IntradayScanFailure::kInvalidTerminalPage;
                    return;
                }
                result->observed_terminal_page = true;
                break;
            }

            for (std::size_t index = 0U; index < written; ++index) {
                const market::RealtimeHistoryRecordV1* const record =
                    batch[index];
                if (record == nullptr ||
                    record->instrument_id() == 0U) {
                    result->failure =
                        IntradayScanFailure::kInvalidRecord;
                    return;
                }
                if (have_previous_record &&
                    (record->instrument_id() <
                         previous_instrument_id ||
                     (record->instrument_id() ==
                          previous_instrument_id &&
                      record->ingress_sequence() <=
                          previous_instrument_ingress))) {
                    result->failure =
                        IntradayScanFailure::kInvalidOrder;
                    return;
                }
                if (range.begin == range.end ||
                    record->instrument_id() <
                        minimum_instrument_id ||
                    record->instrument_id() >
                        maximum_instrument_id) {
                    result->failure =
                        IntradayScanFailure::kInvalidBoundary;
                    return;
                }
                if (record->ingress_sequence() == 0U ||
                    record->ingress_sequence() >=
                        generation.watermark()
                            .ingress_sequence_exclusive ||
                    static_cast<std::size_t>(
                        record->source_slot()) >=
                        generation.watermark().sources.size()) {
                    result->failure =
                        IntradayScanFailure::kInvalidWatermark;
                    return;
                }
                const std::size_t source =
                    static_cast<std::size_t>(
                        record->source_slot());
                const market::RealtimeSourceWatermarkV1&
                    source_watermark =
                        generation.watermark().sources[source];
                if (record->source_stream_id() !=
                        source_watermark.source_stream_id ||
                    record->source_sequence() == 0U ||
                    record->source_sequence() >=
                        source_watermark.sequence_exclusive) {
                    result->failure =
                        IntradayScanFailure::kInvalidWatermark;
                    return;
                }
                const std::size_t kind_index =
                    EventKindIndex(record->kind());
                if (kind_index >= result->kind_counts.size()) {
                    result->failure =
                        IntradayScanFailure::kInvalidRecord;
                    return;
                }
                if (result->source_counts[source] ==
                        std::numeric_limits<std::uint64_t>::max() ||
                    result->kind_counts[kind_index] ==
                        std::numeric_limits<std::uint64_t>::max() ||
                    result->record_count ==
                        std::numeric_limits<std::uint64_t>::max()) {
                    result->failure =
                        IntradayScanFailure::kCounterOverflow;
                    return;
                }
                ++result->source_counts[source];
                ++result->kind_counts[kind_index];
                ++result->record_count;
                result->ingress_sequence_sum +=
                    record->ingress_sequence();
                result->ingress_sequence_xor ^=
                    record->ingress_sequence();
                if (!result->have_record) {
                    result->first_instrument_id =
                        record->instrument_id();
                    result->first_ingress_sequence =
                        record->ingress_sequence();
                    result->have_record = true;
                }
                result->last_instrument_id =
                    record->instrument_id();
                result->last_ingress_sequence =
                    record->ingress_sequence();
                previous_instrument_id =
                    record->instrument_id();
                previous_instrument_ingress =
                    record->ingress_sequence();
                have_previous_record = true;
            }
        }

        std::uint64_t end_ns = 0U;
        if (!ClockNs(CLOCK_MONOTONIC, &end_ns) ||
            end_ns < start_ns) {
            result->failure = IntradayScanFailure::kClock;
            return;
        }
        result->elapsed_ns = end_ns - start_ns;
        result->failure = IntradayScanFailure::kNone;
    } catch (const std::bad_alloc&) {
        result->failure = IntradayScanFailure::kAllocation;
    } catch (...) {
        result->failure = IntradayScanFailure::kUnexpected;
    }
}

void ValidateWatermark(
    const market::RealtimeHistoryWatermarkV1& watermark,
    const market::InstrumentRegistryV1& registry,
    AcceptanceState* state) {
    std::uint64_t sum = 0U;
    for (const market::RealtimeSourceWatermarkV1& source :
         watermark.sources) {
        if (source.sequence_exclusive == 0U ||
            sum > std::numeric_limits<std::uint64_t>::max() -
                      (source.sequence_exclusive - 1U)) {
            Fail(state, "invalid or overflowing source watermark");
            return;
        }
        sum += source.sequence_exclusive - 1U;
    }
    if (watermark.ingress_sequence_exclusive == 0U ||
        watermark.ingress_sequence_exclusive - 1U != sum ||
        watermark.registry_version != registry.registry_version() ||
        watermark.registry_sha256 != registry.registry_sha256()) {
        Fail(state, "store watermark identity/prefix invariant failed");
    }
}

void ValidateGenerationAndCollect(
    const std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>& generation,
    const market::InstrumentRegistryV1& registry,
    std::uint64_t read_realtime_ns,
    std::uint64_t read_monotonic_ns,
    bool expected_coverage_from_open,
    AcceptanceState* state,
    SampleRow* sample) {
    if (generation == nullptr) {
        Fail(state, "published store handle is null");
        return;
    }
    ValidateWatermark(generation->watermark(), registry, state);
    if (!state->valid) {
        return;
    }
    const auto entries = registry.entries();
    if (generation->coverage_from_open() != expected_coverage_from_open ||
        generation->instrument_count() != entries.size() ||
        state->last_seen_head_by_universe_index.size() != entries.size()) {
        Fail(
            state,
            "store generation coverage flag or fixed universe is incorrect");
        return;
    }
    if (generation->watermark().recv_monotonic_cut_ns > read_monotonic_ns) {
        Fail(state, "store generation cut is in the monotonic-clock future");
        return;
    }
    sample->generation_age_ns =
        read_monotonic_ns - generation->watermark().recv_monotonic_cut_ns;
    state->generation_age_ns.Add(sample->generation_age_ns);

    const market::RealtimeHistoryRecordV1* global_head = nullptr;
    std::vector<std::uint64_t> local_recv_ages;
    std::vector<std::int64_t> local_event_ages;
    std::uint64_t summary_record_count = 0U;
    for (std::size_t index = 0U; index < entries.size(); ++index) {
        market::IntradayInstrumentSummaryV1 row{};
        const market::IntradayInstrumentStoreQueryErrorV1 query_error =
            generation->SummaryAt(index, &row);
        if (query_error !=
                market::IntradayInstrumentStoreQueryErrorV1::kNone ||
            row.instrument_id != entries[index].instrument_id) {
            Fail(state, "store universe ordering/identity mismatch");
            return;
        }
        std::uint64_t source_record_count = 0U;
        for (const std::uint64_t count : row.source_record_counts) {
            if (source_record_count >
                std::numeric_limits<std::uint64_t>::max() - count) {
                Fail(state, "store summary source count overflow");
                return;
            }
            source_record_count += count;
        }
        if (source_record_count != row.record_count ||
            summary_record_count >
                std::numeric_limits<std::uint64_t>::max() -
                    row.record_count) {
            Fail(state, "store summary record count invariant failed");
            return;
        }
        summary_record_count += row.record_count;
        ++state->universe_rows_checked;

        const auto valid_latest =
            [&generation, read_monotonic_ns, &row](
                const market::RealtimeHistoryRecordV1* record,
                bool expect_snapshot) {
                if (record == nullptr) {
                    return true;
                }
                const std::size_t source =
                    static_cast<std::size_t>(record->source_slot());
                return record->instrument_id() == row.instrument_id &&
                       EventKindIndex(record->kind()) <
                           kEventKindCount &&
                       SnapshotKind(record->kind()) == expect_snapshot &&
                       record->ingress_sequence() != 0U &&
                       record->ingress_sequence() <
                           generation->watermark()
                               .ingress_sequence_exclusive &&
                       source < generation->watermark().sources.size() &&
                       record->source_sequence() != 0U &&
                       record->source_sequence() <
                           generation->watermark()
                               .sources[source]
                               .sequence_exclusive &&
                       record->source_stream_id() ==
                           generation->watermark()
                               .sources[source]
                               .source_stream_id &&
                       record->recv_monotonic_ns() >= 0 &&
                       static_cast<std::uint64_t>(
                           record->recv_monotonic_ns()) <=
                           read_monotonic_ns;
            };
        if (!valid_latest(row.latest_snapshot, true) ||
            !valid_latest(row.latest_tick, false) ||
            (row.record_count == 0U &&
             (row.latest_snapshot != nullptr ||
              row.latest_tick != nullptr)) ||
            (row.record_count != 0U &&
             row.latest_snapshot == nullptr &&
             row.latest_tick == nullptr)) {
            Fail(state, "store latest-record metadata invariant failed");
            return;
        }
        const market::RealtimeHistoryRecordV1* head =
            row.latest_snapshot;
        if (head == nullptr ||
            (row.latest_tick != nullptr &&
             head->ingress_sequence() <
                 row.latest_tick->ingress_sequence())) {
            head = row.latest_tick;
        }
        if (head == nullptr) {
            continue;
        }
        if (global_head == nullptr ||
            global_head->ingress_sequence() < head->ingress_sequence()) {
            global_head = head;
        }
        const std::size_t kind_index = EventKindIndex(head->kind());
        if (kind_index < state->event_kinds_seen.size()) {
            state->event_kinds_seen[kind_index] = true;
        }
        if (head->ingress_sequence() >
            state->last_seen_head_by_universe_index[index]) {
            state->last_seen_head_by_universe_index[index] =
                head->ingress_sequence();
            const std::uint64_t recv_age = read_monotonic_ns -
                static_cast<std::uint64_t>(head->recv_monotonic_ns());
            state->updated_head_recv_age_ns.Add(recv_age);
            local_recv_ages.push_back(recv_age);
            ++state->updated_heads;
            ++sample->updated_heads;
            if (head->event_time_ns() > 0 &&
                read_realtime_ns <= static_cast<std::uint64_t>(
                    std::numeric_limits<std::int64_t>::max())) {
                const std::int64_t event_age =
                    static_cast<std::int64_t>(read_realtime_ns) -
                    head->event_time_ns();
                state->updated_head_event_age_ns.Add(event_age);
                local_event_ages.push_back(event_age);
                ++state->event_time_samples;
            }
        }
    }
    if (summary_record_count != generation->record_count() ||
        generation->watermark().ingress_sequence_exclusive == 0U ||
        summary_record_count !=
            generation->watermark().ingress_sequence_exclusive - 1U) {
        Fail(state, "store generation summary total invariant failed");
        return;
    }
    if (global_head == nullptr) {
        Fail(state, "published store generation contains no market head");
        return;
    }
    sample->global_head_recv_age_ns = read_monotonic_ns -
        static_cast<std::uint64_t>(global_head->recv_monotonic_ns());
    state->global_head_recv_age_ns.Add(sample->global_head_recv_age_ns);
    if (global_head->event_time_ns() > 0 &&
        read_realtime_ns <= static_cast<std::uint64_t>(
            std::numeric_limits<std::int64_t>::max())) {
        sample->global_head_event_age_ns =
            static_cast<std::int64_t>(read_realtime_ns) -
            global_head->event_time_ns();
        state->global_head_event_age_ns.Add(
            sample->global_head_event_age_ns);
    }
    if (!local_recv_ages.empty()) {
        sample->updated_recv_age_p50_ns =
            Quantile(local_recv_ages, 0.50L);
        sample->updated_recv_age_p99_ns =
            Quantile(std::move(local_recv_ages), 0.99L);
    }
    if (!local_event_ages.empty()) {
        sample->updated_event_age_p50_ns =
            Quantile(local_event_ages, 0.50L);
        sample->updated_event_age_p99_ns =
            Quantile(std::move(local_event_ages), 0.99L);
    }
}

void MeasurePerInstrumentDirectReads(
    const market::IntradayInstrumentStoreGenerationV1& generation,
    std::size_t batch_capacity,
    std::uint32_t reader_cpu,
    AcceptanceState* state) noexcept {
    if (state == nullptr || batch_capacity == 0U) {
        return;
    }
    state->per_instrument_direct_read_enabled = true;
    state->per_instrument_direct_read_cpu = reader_cpu;
    try {
        int affinity_error_number = 0;
        if (!PinCurrentThread(reader_cpu, &affinity_error_number)) {
            Fail(
                state,
                "cannot pin per-instrument direct-read thread to CPU: errno=" +
                    std::to_string(affinity_error_number));
            return;
        }

        const std::size_t instrument_count = generation.instrument_count();
        std::vector<const market::RealtimeHistoryRecordV1*> batch(
            batch_capacity);
        state->per_instrument_direct_read_timings.clear();
        state->per_instrument_direct_read_timings.reserve(instrument_count);
        state->per_instrument_direct_read_all_total_ns.values.clear();
        state->per_instrument_direct_read_all_total_ns.values.reserve(
            instrument_count);
        state->per_instrument_direct_read_nonempty_total_ns.values.clear();
        state->per_instrument_direct_read_nonempty_total_ns.values.reserve(
            instrument_count);
        state->per_instrument_direct_read_nonempty_read_ns.values.clear();
        state->per_instrument_direct_read_nonempty_read_ns.values.reserve(
            instrument_count);
        state->per_instrument_direct_read_nonempty_ns_per_record.values.clear();
        state->per_instrument_direct_read_nonempty_ns_per_record.values.reserve(
            instrument_count);

        std::uint64_t pass_start_ns = 0U;
        if (!ClockNs(CLOCK_MONOTONIC, &pass_start_ns)) {
            Fail(state, "cannot read per-instrument pass start clock");
            return;
        }
        for (std::size_t ordinal = 0U;
             ordinal < instrument_count;
             ++ordinal) {
            market::IntradayInstrumentSummaryV1 summary{};
            const market::IntradayInstrumentStoreQueryErrorV1 summary_error =
                generation.SummaryAt(ordinal, &summary);
            if (summary_error !=
                    market::IntradayInstrumentStoreQueryErrorV1::kNone ||
                summary.instrument_id == 0U) {
                Fail(
                    state,
                    "cannot resolve per-instrument direct-read summary at " +
                        std::to_string(ordinal));
                return;
            }

            std::uint64_t open_start_ns = 0U;
            if (!ClockNs(CLOCK_MONOTONIC, &open_start_ns)) {
                Fail(state, "cannot read instrument cursor-open start clock");
                return;
            }
            std::unique_ptr<market::IntradayInstrumentCursorV1> cursor;
            const market::IntradayInstrumentStoreQueryErrorV1 open_error =
                generation.OpenInstrumentCursor(
                    summary.instrument_id, {}, &cursor);
            std::uint64_t open_end_ns = 0U;
            if (!ClockNs(CLOCK_MONOTONIC, &open_end_ns) ||
                open_end_ns < open_start_ns) {
                Fail(state, "cannot read instrument cursor-open end clock");
                return;
            }
            if (open_error !=
                    market::IntradayInstrumentStoreQueryErrorV1::kNone ||
                cursor == nullptr) {
                Fail(
                    state,
                    "cannot open per-instrument full-history cursor for " +
                        std::to_string(summary.instrument_id) + ": " +
                        std::string(
                            market::IntradayInstrumentStoreQueryErrorNameV1(
                                open_error)));
                return;
            }

            std::uint64_t records = 0U;
            for (;;) {
                std::size_t written = 0U;
                const market::IntradayInstrumentStoreQueryErrorV1 read_error =
                    cursor->ReadBatch(batch, &written);
                if (read_error !=
                    market::IntradayInstrumentStoreQueryErrorV1::kNone) {
                    Fail(
                        state,
                        "cannot read per-instrument full-history cursor for " +
                            std::to_string(summary.instrument_id) + ": " +
                            std::string(
                                market::
                                    IntradayInstrumentStoreQueryErrorNameV1(
                                        read_error)));
                    return;
                }
                if (written == 0U) {
                    if (!cursor->done()) {
                        Fail(
                            state,
                            "per-instrument cursor returned an empty "
                            "nonterminal page");
                        return;
                    }
                    break;
                }
                if (records >
                    std::numeric_limits<std::uint64_t>::max() - written) {
                    Fail(state, "per-instrument record count overflow");
                    return;
                }
                records += static_cast<std::uint64_t>(written);
            }
            std::uint64_t read_end_ns = 0U;
            if (!ClockNs(CLOCK_MONOTONIC, &read_end_ns) ||
                read_end_ns < open_end_ns) {
                Fail(state, "cannot read instrument cursor-read end clock");
                return;
            }
            if (records != summary.record_count) {
                Fail(
                    state,
                    "per-instrument cursor count differs from generation "
                    "summary for " +
                        std::to_string(summary.instrument_id));
                return;
            }

            const std::uint64_t open_ns = open_end_ns - open_start_ns;
            const std::uint64_t read_ns = read_end_ns - open_end_ns;
            const std::uint64_t total_ns = read_end_ns - open_start_ns;
            if (state->per_instrument_direct_read_records >
                    std::numeric_limits<std::uint64_t>::max() - records ||
                state->per_instrument_direct_read_open_ns >
                    std::numeric_limits<std::uint64_t>::max() - open_ns ||
                state->per_instrument_direct_read_read_ns >
                    std::numeric_limits<std::uint64_t>::max() - read_ns ||
                state->per_instrument_direct_read_total_ns >
                    std::numeric_limits<std::uint64_t>::max() - total_ns) {
                Fail(state, "per-instrument direct-read total overflow");
                return;
            }
            state->per_instrument_direct_read_records += records;
            state->per_instrument_direct_read_open_ns += open_ns;
            state->per_instrument_direct_read_read_ns += read_ns;
            state->per_instrument_direct_read_total_ns += total_ns;
            state->per_instrument_direct_read_all_total_ns.Add(total_ns);
            if (records != 0U) {
                ++state->per_instrument_direct_read_nonempty;
                state->per_instrument_direct_read_nonempty_total_ns.Add(
                    total_ns);
                state->per_instrument_direct_read_nonempty_read_ns.Add(
                    read_ns);
                state->per_instrument_direct_read_nonempty_ns_per_record.Add(
                    read_ns / records);
            }
            state->per_instrument_direct_read_timings.push_back(
                AcceptanceState::PerInstrumentDirectReadTiming{
                    ordinal,
                    summary.instrument_id,
                    records,
                    open_ns,
                    read_ns,
                    total_ns});
        }
        std::uint64_t pass_end_ns = 0U;
        if (!ClockNs(CLOCK_MONOTONIC, &pass_end_ns) ||
            pass_end_ns < pass_start_ns) {
            Fail(state, "cannot read per-instrument pass end clock");
            return;
        }
        state->per_instrument_direct_read_pass_ns =
            pass_end_ns - pass_start_ns;
        if (state->per_instrument_direct_read_timings.size() !=
                instrument_count ||
            state->per_instrument_direct_read_records !=
                generation.record_count() ||
            state->per_instrument_direct_read_total_ns >
                state->per_instrument_direct_read_pass_ns) {
            Fail(state, "per-instrument direct-read accounting mismatch");
        }
    } catch (const std::bad_alloc&) {
        Fail(state, "per-instrument direct-read allocation failed");
    } catch (const std::exception&) {
        Fail(state, "per-instrument direct-read unexpected exception");
    } catch (...) {
        Fail(state, "per-instrument direct-read unknown exception");
    }
}

void ValidateFinalStore(
    const Options& options,
    const runtime::RealtimePipelineCutResultV1& cut,
    const runtime::RealtimePipelineSnapshotV1& snapshot,
    const market::InstrumentRegistryV1& registry,
    AcceptanceState* state) {
    if (snapshot.store.maximum_session_records !=
            options.intraday_store_maximum_records ||
        snapshot.store.maximum_session_accounted_bytes !=
            options.intraday_store_memory_bytes) {
        Fail(state, "intraday store snapshot configuration mismatch");
        return;
    }
    if (snapshot.store.coverage_lost ||
        snapshot.store.coverage_from_open !=
            options.intraday_store_from_open) {
        Fail(state, "required intraday store coverage contract failed");
        return;
    }
    if (cut.store_generation == nullptr) {
        Fail(state, "required intraday store omitted final generation");
        return;
    }
    const market::IntradayInstrumentStoreGenerationV1& generation =
        *cut.store_generation;
    if (cut.factor_generation == nullptr ||
        cut.factor_generation->input_store().get() !=
            cut.store_generation.get() ||
        generation.instrument_count() != registry.size() ||
        generation.record_count() != snapshot.store.appended_records ||
        generation.accounted_record_bytes() !=
            snapshot.store.accounted_record_bytes ||
        generation.allocated_index_bytes() !=
            snapshot.store.allocated_index_bytes ||
        generation.coverage_from_open() !=
            snapshot.store.coverage_from_open ||
        snapshot.store.coverage_from_open !=
            options.intraday_store_from_open ||
        generation.watermark().generation !=
            snapshot.store.latest_generation) {
        Fail(state, "final intraday store generation/snapshot contract failed");
        return;
    }
    ValidateWatermark(generation.watermark(), registry, state);
    if (!state->valid) {
        return;
    }

    try {
        std::vector<std::uint32_t> reader_cpus;
        std::string scan_error;
        if (!ResolveReaderCpus(
                options, &reader_cpus, &scan_error)) {
            Fail(state, std::move(scan_error));
            return;
        }
        if (!options.per_instrument_read_timings_csv.empty()) {
            MeasurePerInstrumentDirectReads(
                generation,
                static_cast<std::size_t>(
                    options.intraday_scan_batch_records),
                reader_cpus.front(),
                state);
            if (!state->valid) {
                return;
            }
        }

        std::uint64_t partition_start_ns = 0U;
        std::uint64_t partition_end_ns = 0U;
        if (!ClockNs(CLOCK_MONOTONIC, &partition_start_ns)) {
            Fail(state, "cannot read intraday scan partition start clock");
            return;
        }
        std::vector<IntradayScanOrdinalRange> ranges;
        if (!BuildIntradayScanRanges(
                generation,
                options.intraday_scan_workers,
                &ranges,
                &scan_error)) {
            Fail(state, std::move(scan_error));
            return;
        }
        if (!ClockNs(CLOCK_MONOTONIC, &partition_end_ns) ||
            partition_end_ns < partition_start_ns) {
            Fail(state, "cannot read intraday scan partition end clock");
            return;
        }
        state->intraday_scan_partition_ns =
            partition_end_ns - partition_start_ns;
        state->intraday_reader_cpus = reader_cpus;

        std::vector<IntradayScanShardResult> results(ranges.size());
        std::vector<std::jthread> readers;
        readers.reserve(ranges.size());
        std::uint64_t scan_start_ns = 0U;
        if (!ClockNs(CLOCK_MONOTONIC, &scan_start_ns)) {
            Fail(state, "cannot read intraday full-scan start clock");
            return;
        }
        for (std::size_t index = 0U; index < ranges.size(); ++index) {
            readers.emplace_back(
                [&generation,
                 &ranges,
                 &reader_cpus,
                 &results,
                 &options,
                 index]() noexcept {
                    IntradayScanShardResult local;
                    ScanIntradayOrdinalRange(
                        generation,
                        ranges[index],
                        static_cast<std::size_t>(
                            options.intraday_scan_batch_records),
                        reader_cpus[index],
                        &local);
                    results[index] = std::move(local);
                });
        }
        readers.clear();
        std::uint64_t scan_end_ns = 0U;
        if (!ClockNs(CLOCK_MONOTONIC, &scan_end_ns) ||
            scan_end_ns < scan_start_ns) {
            Fail(state, "cannot read intraday full-scan end clock");
            return;
        }

        std::array<
            std::uint64_t,
            market::kIntradayInstrumentStoreSourceCountV1>
            source_counts{};
        std::array<std::uint64_t, kEventKindCount> kind_counts{};
        std::uint64_t scanned_records = 0U;
        std::uint64_t ingress_sequence_sum = 0U;
        std::uint64_t ingress_sequence_xor = 0U;
        std::uint32_t previous_shard_last_instrument_id = 0U;
        bool have_previous_nonempty_shard = false;
        state->intraday_scan_ordinal_begins.clear();
        state->intraday_scan_ordinal_ends.clear();
        state->intraday_scan_shard_records.clear();
        state->intraday_scan_shard_ns.clear();
        state->intraday_scan_shard_last_instrument_ids.clear();
        state->intraday_scan_shard_last_ingress_sequences.clear();
        state->intraday_scan_ordinal_begins.reserve(ranges.size());
        state->intraday_scan_ordinal_ends.reserve(ranges.size());
        state->intraday_scan_shard_records.reserve(ranges.size());
        state->intraday_scan_shard_ns.reserve(ranges.size());
        state->intraday_scan_shard_last_instrument_ids.reserve(
            ranges.size());
        state->intraday_scan_shard_last_ingress_sequences.reserve(
            ranges.size());
        for (std::size_t index = 0U; index < results.size(); ++index) {
            const IntradayScanShardResult& result = results[index];
            if (result.failure != IntradayScanFailure::kNone ||
                !result.observed_terminal_page) {
                std::string message =
                    "intraday full-scan shard " +
                    std::to_string(index) + " failed: " +
                    std::string(
                        IntradayScanFailureName(result.failure));
                if (result.query_error !=
                    market::IntradayInstrumentStoreQueryErrorV1::kNone) {
                    message += " query=";
                    message +=
                        market::IntradayInstrumentStoreQueryErrorNameV1(
                            result.query_error);
                }
                if (result.affinity_error_number != 0) {
                    message += " errno=" +
                        std::to_string(
                            result.affinity_error_number);
                }
                Fail(state, std::move(message));
                return;
            }
            if (result.record_count !=
                ranges[index].expected_records) {
                Fail(
                    state,
                    "intraday full-scan shard record total mismatch");
                return;
            }
            if (result.have_record &&
                have_previous_nonempty_shard &&
                result.first_instrument_id <=
                    previous_shard_last_instrument_id) {
                Fail(
                    state,
                    "intraday ordinal-range shard ordering invariant "
                    "failed");
                return;
            }
            if (result.have_record) {
                previous_shard_last_instrument_id =
                    result.last_instrument_id;
                have_previous_nonempty_shard = true;
            }
            if (scanned_records >
                std::numeric_limits<std::uint64_t>::max() -
                    result.record_count) {
                Fail(state, "intraday full-scan record count overflow");
                return;
            }
            scanned_records += result.record_count;
            ingress_sequence_sum += result.ingress_sequence_sum;
            ingress_sequence_xor ^= result.ingress_sequence_xor;
            for (std::size_t source = 0U;
                 source < source_counts.size();
                 ++source) {
                if (source_counts[source] >
                    std::numeric_limits<std::uint64_t>::max() -
                        result.source_counts[source]) {
                    Fail(state, "intraday source count overflow");
                    return;
                }
                source_counts[source] +=
                    result.source_counts[source];
            }
            for (std::size_t kind = 0U;
                 kind < kind_counts.size();
                 ++kind) {
                if (kind_counts[kind] >
                    std::numeric_limits<std::uint64_t>::max() -
                        result.kind_counts[kind]) {
                    Fail(state, "intraday event-kind count overflow");
                    return;
                }
                kind_counts[kind] += result.kind_counts[kind];
                if (result.kind_counts[kind] != 0U) {
                    state->event_kinds_seen[kind] = true;
                }
            }
            state->intraday_scan_ordinal_begins.push_back(
                ranges[index].begin);
            state->intraday_scan_ordinal_ends.push_back(
                ranges[index].end);
            state->intraday_scan_shard_records.push_back(
                result.record_count);
            state->intraday_scan_shard_ns.push_back(
                result.elapsed_ns);
            state->intraday_scan_shard_last_instrument_ids.push_back(
                result.last_instrument_id);
            state->intraday_scan_shard_last_ingress_sequences.push_back(
                result.last_ingress_sequence);
        }

        if (scanned_records == 0U) {
            Fail(state, "intraday full scan did not terminate nonempty");
            return;
        }
        std::uint64_t expected_total = 0U;
        for (std::size_t source = 0U;
             source < source_counts.size();
             ++source) {
            const std::uint64_t exclusive =
                generation.watermark().sources[source]
                    .sequence_exclusive;
            if (exclusive == 0U ||
                source_counts[source] != exclusive - 1U ||
                expected_total >
                    std::numeric_limits<std::uint64_t>::max() -
                        source_counts[source]) {
                Fail(state, "intraday four-source totals mismatch");
                return;
            }
            expected_total += source_counts[source];
        }
        const std::uint64_t ingress_exclusive =
            generation.watermark().ingress_sequence_exclusive;
        if (ingress_exclusive == 0U ||
            scanned_records != ingress_exclusive - 1U ||
            scanned_records != expected_total ||
            scanned_records != generation.record_count()) {
            Fail(state, "intraday full-scan total mismatch");
            return;
        }
        if (ingress_sequence_sum !=
                DenseSequenceSumModuloU64(scanned_records) ||
            ingress_sequence_xor !=
                DenseSequenceXor(scanned_records)) {
            Fail(state, "intraday full-scan dense ingress fingerprint mismatch");
            return;
        }
        state->intraday_full_scan_records = scanned_records;
        state->intraday_full_scan_ns = scan_end_ns - scan_start_ns;
        if (state->intraday_scan_partition_ns >
            std::numeric_limits<std::uint64_t>::max() -
                state->intraday_full_scan_ns) {
            Fail(state, "intraday full-scan total duration overflow");
            return;
        }
        state->intraday_scan_total_ns =
            state->intraday_scan_partition_ns +
            state->intraday_full_scan_ns;
        state->intraday_scan_ingress_sum =
            ingress_sequence_sum;
        state->intraday_scan_ingress_xor =
            ingress_sequence_xor;
        state->intraday_scan_kind_counts = kind_counts;
        state->intraday_scan_last_instrument_id =
            previous_shard_last_instrument_id;
        for (auto iterator = results.rbegin();
             iterator != results.rend();
             ++iterator) {
            if (iterator->have_record) {
                state->intraday_scan_last_ingress_sequence =
                    iterator->last_ingress_sequence;
                break;
            }
        }
    } catch (const std::bad_alloc&) {
        Fail(state, "intraday full-scan allocation failed");
    } catch (const std::exception&) {
        Fail(state, "intraday full-scan unexpected exception");
    } catch (...) {
        Fail(state, "intraday full-scan unknown exception");
    }
}

[[nodiscard]] bool WriteSamples(
    const std::filesystem::path& path,
    const std::vector<SampleRow>& samples,
    std::string* error) {
    std::ofstream output(path, std::ios::out | std::ios::trunc);
    if (!output.is_open()) {
        *error = "cannot create samples CSV";
        return false;
    }
    output
        << "elapsed_ns,accepted,decoded,accepted_delta,decoded_delta,"
           "source0_delta,source1_delta,source2_delta,source3_delta,"
           "generation,ingress_prefix,cut_latency_ns,acquire_p50_ns,"
           "acquire_p99_ns,generation_age_ns,global_head_recv_age_ns,"
           "global_head_event_age_ns,updated_heads,"
           "updated_recv_age_p50_ns,updated_recv_age_p99_ns,"
           "updated_event_age_p50_ns,updated_event_age_p99_ns\n";
    for (const SampleRow& row : samples) {
        output << row.elapsed_ns << ',' << row.accepted << ',' << row.decoded
               << ',' << row.accepted_delta << ',' << row.decoded_delta;
        for (const std::uint64_t delta : row.source_delta) {
            output << ',' << delta;
        }
        output << ',' << row.generation << ',' << row.ingress_prefix << ','
               << row.cut_latency_ns << ',' << row.acquire_p50_ns << ','
               << row.acquire_p99_ns << ',' << row.generation_age_ns << ','
               << row.global_head_recv_age_ns << ','
               << row.global_head_event_age_ns << ',' << row.updated_heads
               << ',' << row.updated_recv_age_p50_ns << ','
               << row.updated_recv_age_p99_ns << ','
               << row.updated_event_age_p50_ns << ','
               << row.updated_event_age_p99_ns << '\n';
    }
    output.flush();
    if (!output.good()) {
        *error = "samples CSV write failed";
        return false;
    }
    return true;
}

[[nodiscard]] std::string RegistryBytesHex(
    std::span<const std::byte> bytes) {
    constexpr char digits[] = "0123456789abcdef";
    std::string result(bytes.size() * 2U, '0');
    for (std::size_t index = 0U; index < bytes.size(); ++index) {
        const unsigned int value =
            std::to_integer<unsigned int>(bytes[index]);
        result[index * 2U] = digits[(value >> 4U) & 0x0fU];
        result[index * 2U + 1U] = digits[value & 0x0fU];
    }
    return result;
}

[[nodiscard]] bool WritePerInstrumentDirectReadTimings(
    const std::filesystem::path& path,
    const AcceptanceState& state,
    const market::InstrumentRegistryV1& registry,
    std::string* error) {
    if (error == nullptr) {
        return false;
    }
    std::ofstream output(path, std::ios::out | std::ios::trunc);
    if (!output.is_open()) {
        *error = "cannot create per-instrument read-timings CSV";
        return false;
    }
    output
        << "ordinal,instrument_id,market,security_id_source_hex,"
           "security_id_hex,record_count,open_ns,read_ns,total_ns,"
           "read_ns_per_record,read_records_per_second\n";
    for (const AcceptanceState::PerInstrumentDirectReadTiming& timing :
         state.per_instrument_direct_read_timings) {
        const market::InstrumentRegistryLookupResultV1 lookup =
            registry.LookupById(timing.instrument_id);
        if (!lookup.known()) {
            *error =
                "per-instrument timing references an unknown instrument";
            return false;
        }
        const long double ns_per_record = timing.record_count == 0U
            ? 0.0L
            : static_cast<long double>(timing.read_ns) /
                  static_cast<long double>(timing.record_count);
        const long double records_per_second = timing.read_ns == 0U
            ? 0.0L
            : static_cast<long double>(timing.record_count) *
                  static_cast<long double>(kNanosecondsPerSecond) /
                  static_cast<long double>(timing.read_ns);
        output << timing.ordinal << ',' << timing.instrument_id << ','
               << static_cast<unsigned int>(lookup.entry->key.market) << ','
               << RegistryBytesHex(
                      lookup.entry->key.security_id_source)
               << ',' << RegistryBytesHex(lookup.entry->key.security_id)
               << ',' << timing.record_count << ',' << timing.open_ns << ','
               << timing.read_ns << ',' << timing.total_ns << ','
               << std::fixed << std::setprecision(3) << ns_per_record << ','
               << records_per_second << std::defaultfloat << '\n';
    }
    output.flush();
    if (!output.good()) {
        *error = "per-instrument read-timings CSV write failed";
        return false;
    }
    return true;
}

[[nodiscard]] bool AllKindsSeen(
    const std::array<bool, kEventKindCount>& seen) noexcept {
    return std::all_of(seen.begin(), seen.end(), [](bool value) {
        return value;
    });
}

[[nodiscard]] std::string RegistryBytesText(
    std::span<const std::byte> bytes) {
    std::string text;
    text.reserve(bytes.size());
    for (const std::byte value : bytes) {
        text.push_back(static_cast<char>(std::to_integer<unsigned char>(value)));
    }
    return text;
}

[[nodiscard]] bool KLineEventOrderLess(
    const market::KLineEventOrderV1& lhs,
    const market::KLineEventOrderV1& rhs) noexcept {
    if (lhs.event_time_ns_since_midnight !=
        rhs.event_time_ns_since_midnight) {
        return lhs.event_time_ns_since_midnight <
               rhs.event_time_ns_since_midnight;
    }
    if (lhs.event_sequence != rhs.event_sequence) {
        return lhs.event_sequence < rhs.event_sequence;
    }
    if (lhs.source_sequence != rhs.source_sequence) {
        return lhs.source_sequence < rhs.source_sequence;
    }
    return lhs.ingress_sequence < rhs.ingress_sequence;
}

[[nodiscard]] bool ValidateKLineBar(
    const Options& options,
    const market::KLineWindowSpecV1& window,
    std::uint32_t instrument_id,
    const market::KLineBarV1& bar) noexcept {
    if (bar.trade_date != options.trade_date ||
        bar.instrument_id != instrument_id ||
        bar.window_id != window.window_id ||
        bar.window_duration_ns != window.duration_ns ||
        bar.window_duration_ns == 0U ||
        bar.window_start_ns_since_midnight >=
            market::kKLineNanosecondsPerDayV1 ||
        bar.window_start_ns_since_midnight % bar.window_duration_ns != 0U ||
        bar.window_start_ns_since_midnight >
            std::numeric_limits<std::uint64_t>::max() -
                bar.window_duration_ns ||
        bar.window_end_ns_since_midnight !=
            bar.window_start_ns_since_midnight +
                bar.window_duration_ns ||
        bar.window_end_unix_ns < bar.window_start_unix_ns ||
        static_cast<std::uint64_t>(
            bar.window_end_unix_ns - bar.window_start_unix_ns) !=
                bar.window_duration_ns ||
        bar.open_price_p6 <= 0 || bar.high_price_p6 <= 0 ||
        bar.low_price_p6 <= 0 || bar.close_price_p6 <= 0 ||
        bar.low_price_p6 > bar.high_price_p6 ||
        bar.open_price_p6 < bar.low_price_p6 ||
        bar.open_price_p6 > bar.high_price_p6 ||
        bar.close_price_p6 < bar.low_price_p6 ||
        bar.close_price_p6 > bar.high_price_p6 ||
        bar.trade_count == 0U || bar.revision != bar.trade_count ||
        bar.first_trade.event_time_ns_since_midnight <
            bar.window_start_ns_since_midnight ||
        bar.first_trade.event_time_ns_since_midnight >=
            bar.window_end_ns_since_midnight ||
        bar.last_trade.event_time_ns_since_midnight <
            bar.window_start_ns_since_midnight ||
        bar.last_trade.event_time_ns_since_midnight >=
            bar.window_end_ns_since_midnight ||
        bar.first_trade.event_sequence == 0U ||
        bar.first_trade.source_sequence == 0U ||
        bar.first_trade.ingress_sequence == 0U ||
        bar.last_trade.event_sequence == 0U ||
        bar.last_trade.source_sequence == 0U ||
        bar.last_trade.ingress_sequence == 0U ||
        KLineEventOrderLess(bar.last_trade, bar.first_trade)) {
        return false;
    }
    return true;
}

void ValidateKLines(
    const Options& options,
    const runtime::RealtimePipelineCutResultV1& final_cut,
    const market::InstrumentRegistryV1& registry,
    KLineAcceptance* output,
    AcceptanceState* state) {
    if (output == nullptr || state == nullptr) {
        return;
    }
    output->enabled = !options.kline_windows_ms.empty();
    if (!output->enabled) {
        if (final_cut.kline_enabled || final_cut.kline_generation != nullptr) {
            Fail(state, "disabled KLine unexpectedly published a generation");
        }
        return;
    }
    if (!final_cut.kline_enabled || final_cut.kline_generation == nullptr ||
        final_cut.kline_generation->input_store().get() !=
            final_cut.store_generation.get() ||
        final_cut.kline_generation->coverage_from_open() !=
            options.intraday_store_from_open) {
        Fail(state, "KLine/store atomic generation contract failed");
        return;
    }

    const market::RealtimeKLineGenerationV1& generation =
        *final_cut.kline_generation;
    const std::span<const market::KLineWindowSpecV1> windows =
        generation.windows();
    if (windows.size() != options.kline_windows_ms.size()) {
        Fail(state, "KLine generation exposed the wrong window count");
        return;
    }
    output->generation_bar_count = generation.bar_count();
    output->windows.reserve(windows.size());
    for (std::size_t index = 0U; index < windows.size(); ++index) {
        const std::uint64_t expected_duration_ns =
            static_cast<std::uint64_t>(options.kline_windows_ms[index]) *
            market::kKLineNanosecondsPerMillisecondV1;
        if (windows[index].window_id != options.kline_windows_ms[index] ||
            windows[index].duration_ns != expected_duration_ns) {
            Fail(state, "KLine generation exposed the wrong window spec");
            return;
        }
        KLineWindowAcceptance summary{};
        summary.window_id = windows[index].window_id;
        summary.duration_ns = windows[index].duration_ns;
        output->windows.push_back(std::move(summary));
    }

    std::uint64_t bars_read = 0U;
    std::uint64_t longest_consecutive_bars = 0U;
    std::array<market::KLineBarV1, 256U> batch{};
    for (std::size_t window_index = 0U;
         window_index < windows.size();
         ++window_index) {
        KLineWindowAcceptance& summary = output->windows[window_index];
        for (const market::InstrumentRegistryEntryV1& entry :
             registry.entries()) {
            std::unique_ptr<market::KLineCursorV1> cursor;
            const market::KLineQueryErrorV1 open_error =
                generation.OpenInstrumentCursor(
                    entry.instrument_id,
                    windows[window_index].window_id,
                    &cursor);
            if (open_error == market::KLineQueryErrorV1::kNotFound) {
                continue;
            }
            if (open_error != market::KLineQueryErrorV1::kNone ||
                cursor == nullptr) {
                Fail(state, "cannot open a published KLine series");
                return;
            }

            std::vector<market::KLineBarV1> instrument_bars;
            for (;;) {
                std::size_t written = 0U;
                const market::KLineQueryErrorV1 read_error =
                    cursor->ReadBatch(batch, &written);
                if (read_error != market::KLineQueryErrorV1::kNone) {
                    Fail(state, "cannot read a published KLine series");
                    return;
                }
                if (written == 0U) {
                    if (!cursor->done()) {
                        Fail(state, "empty KLine batch did not finish cursor");
                        return;
                    }
                    break;
                }
                instrument_bars.insert(
                    instrument_bars.end(),
                    batch.begin(),
                    batch.begin() + static_cast<std::ptrdiff_t>(written));
            }
            if (instrument_bars.empty()) {
                Fail(state, "published KLine series was empty");
                return;
            }
            ++summary.instruments_with_bars;

            std::size_t run_begin = 0U;
            std::size_t best_begin = 0U;
            std::size_t best_end = 1U;
            for (std::size_t index = 0U;
                 index < instrument_bars.size();
                 ++index) {
                const market::KLineBarV1& bar = instrument_bars[index];
                if (!ValidateKLineBar(
                        options,
                        windows[window_index],
                        entry.instrument_id,
                        bar)) {
                    Fail(state, "published KLine bar invariant failed");
                    return;
                }
                if (index != 0U &&
                    instrument_bars[index - 1U]
                            .window_start_ns_since_midnight >=
                        bar.window_start_ns_since_midnight) {
                    Fail(state, "KLine series is not strictly ascending");
                    return;
                }
                if (summary.trades >
                    std::numeric_limits<std::uint64_t>::max() -
                        bar.trade_count) {
                    Fail(state, "KLine trade-count summary overflowed");
                    return;
                }
                summary.trades += bar.trade_count;
                if (index != 0U &&
                    instrument_bars[index - 1U]
                                .window_start_ns_since_midnight +
                            windows[window_index].duration_ns !=
                        bar.window_start_ns_since_midnight) {
                    run_begin = index;
                }
                if (index + 1U - run_begin > best_end - best_begin) {
                    best_begin = run_begin;
                    best_end = index + 1U;
                }
            }

            const std::uint64_t instrument_bar_count =
                static_cast<std::uint64_t>(instrument_bars.size());
            if (summary.bars >
                    std::numeric_limits<std::uint64_t>::max() -
                        instrument_bar_count ||
                bars_read >
                    std::numeric_limits<std::uint64_t>::max() -
                        instrument_bar_count) {
                Fail(state, "KLine bar-count summary overflowed");
                return;
            }
            summary.bars += instrument_bar_count;
            bars_read += instrument_bar_count;

            const std::uint64_t best_run =
                static_cast<std::uint64_t>(best_end - best_begin);
            if (best_run > summary.longest_consecutive_bars) {
                summary.longest_consecutive_bars = best_run;
                summary.representative_instrument_id = entry.instrument_id;
                summary.representative_bars.assign(
                    instrument_bars.begin() +
                        static_cast<std::ptrdiff_t>(best_begin),
                    instrument_bars.begin() +
                        static_cast<std::ptrdiff_t>(best_end));
            }
            longest_consecutive_bars = std::max(
                longest_consecutive_bars,
                summary.longest_consecutive_bars);
        }
    }

    if (bars_read == 0U || bars_read != generation.bar_count()) {
        Fail(state, "KLine cursor count did not match generation bar count");
        return;
    }
    if (options.kline_min_consecutive_bars != 0U &&
        longest_consecutive_bars <
            options.kline_min_consecutive_bars) {
        Fail(state, "KLine consecutive-bar acceptance gate failed");
    }
}

[[nodiscard]] int Run(const Options& options) {
    std::vector<std::uint32_t> validated_reader_cpus;
    std::string error;
    if (!ResolveReaderCpus(
            options, &validated_reader_cpus, &error)) {
        std::cerr << "accept-realtime-pipeline: " << error << '\n';
        return 2;
    }

    std::string user_name;
    if (!ReadSecret(options.user_name_file, &user_name, &error)) {
        std::cerr << "accept-realtime-pipeline: " << error << '\n';
        return 2;
    }

    FileDescriptor registry_directory_fd(::open(
        options.registry_directory.c_str(),
        O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
    if (!registry_directory_fd.valid()) {
        std::cerr << "accept-realtime-pipeline: cannot open registry directory\n";
        return 2;
    }
    market::InstrumentRegistryFileOptionsV1 registry_options{};
    registry_options.directory_fd = registry_directory_fd.get();
    registry_options.file_name = options.registry_file;
    registry_options.expected_owner_uid =
        static_cast<std::uint32_t>(::getuid());
    registry_options.expected_registry_version = options.registry_version;
    registry_options.expected_registry_sha256 = options.registry_sha256;
    market::InstrumentRegistryFileResultV1 registry_result =
        market::LoadInstrumentRegistryFileV1(registry_options);
    if (!registry_result.ok()) {
        std::cerr << "accept-realtime-pipeline: registry load failed: "
                  << market::InstrumentRegistryFileErrorNameV1(
                         registry_result.error)
                  << " line=" << registry_result.line
                  << " errno=" << registry_result.system_error_number << '\n';
        return 2;
    }

    common::Identity128 run_id{};
    int entropy_error = 0;
    if (!common::GenerateIdentity128(&run_id, &entropy_error)) {
        std::cerr << "accept-realtime-pipeline: run-id failed errno="
                  << entropy_error << '\n';
        return 2;
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
    config.kline.trade_date = options.trade_date;
    config.kline.windows.reserve(options.kline_windows_ms.size());
    for (const std::uint32_t duration_ms : options.kline_windows_ms) {
        config.kline.windows.push_back(market::KLineWindowSpecV1{
            duration_ms,
            static_cast<std::uint64_t>(duration_ms) *
                market::kKLineNanosecondsPerMillisecondV1});
    }
    config.enforce_receive_trade_date = true;
    config.measure_stage_latency = options.measure_stage_latency;
    config.sdk.enabled = true;
    config.sdk.library_path = options.sdk_library;
    config.sdk.server_address = options.server_address;
    config.sdk.user_name = std::move(user_name);
    config.sdk.log_prefix = options.sdk_log_prefix.string();
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
        const std::uint64_t reorder_guard =
            tick_source_count +
            static_cast<std::uint64_t>(config.store_worker_count);
        if (decoder_pending >
                std::numeric_limits<std::uint64_t>::max() -
                    history_pending - reorder_guard ||
            options.ipc_tick_ring_records <
                decoder_pending + history_pending + reorder_guard) {
            std::cerr
                << "accept-realtime-pipeline: --ipc-tick-ring-records "
                   "must cover the maximum in-flight mixed-tick reorder "
                   "span (minimum="
                << decoder_pending + history_pending + reorder_guard
                << ")\n";
            return 2;
        }
        ipc::RealtimeSharedServiceConfigV1 ipc_config{};
        ipc_config.run_id = run_id;
        ipc_config.trade_date = options.trade_date;
        ipc_config.registry = registry_result.registry.get();
        ipc_config.kline_windows = config.kline.windows;
        ipc_config.tick_ring_capacity = options.ipc_tick_ring_records;
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
                << "accept-realtime-pipeline: IPC service create failed: "
                << ipc::RealtimeSharedServiceCreateErrorNameV1(ipc_error)
                << " errno=" << ipc_system_error << '\n';
            return 2;
        }
        config.applied_record_sink = ipc_service;
    }
#endif

    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    const runtime::RealtimePipelineCreateErrorV1 create_error =
        runtime::RealtimePipelineV1::Create(
            config, &pipeline, &detail);
    if (create_error != runtime::RealtimePipelineCreateErrorV1::kNone ||
        pipeline == nullptr) {
        std::cerr << "accept-realtime-pipeline: create failed: "
                  << runtime::RealtimePipelineCreateErrorNameV1(create_error)
                  << (detail.empty() ? "" : ": ") << detail << '\n';
        return 2;
    }
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
    if (ipc_service != nullptr) {
        int ipc_system_error = 0;
        if (!ipc_service->Start(&ipc_system_error)) {
            std::cerr
                << "accept-realtime-pipeline: IPC control start failed: "
                   "errno="
                << ipc_system_error << '\n';
            pipeline->StopAndDrain();
            ipc_service->MarkFailed();
            return 2;
        }
        std::cerr
            << "accept-realtime-pipeline: IPC active: socket="
            << ipc_service->control_socket_path()
            << " mapping_bytes=" << ipc_service->mapping_bytes()
            << '\n';
    }
#endif

    AcceptanceState state{};
    state.last_seen_head_by_universe_index.resize(
        registry_result.registry->size(), 0U);
    const runtime::RealtimePipelineSnapshotV1 initial = pipeline->Snapshot();
    runtime::RealtimePipelineSnapshotV1 previous = initial;
    std::uint64_t start_monotonic_ns = 0U;
    std::uint64_t end_monotonic_ns = 0U;
    if (!ClockNs(CLOCK_MONOTONIC, &start_monotonic_ns)) {
        pipeline->StopAndDrain();
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
        if (ipc_service != nullptr) {
            ipc_service->MarkFailed();
        }
#endif
        std::cerr << "accept-realtime-pipeline: cannot read start clock\n";
        return 2;
    }
    const auto start = std::chrono::steady_clock::now();
    const auto deadline = start + std::chrono::seconds(options.duration_seconds);
    const auto interval =
        std::chrono::milliseconds(options.generation_interval_ms);
    const auto timeout =
        std::chrono::milliseconds(options.generation_timeout_ms);
    auto next = start + interval;
    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1> last_store;

    while (next <= deadline) {
        std::this_thread::sleep_until(next);
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
        if (ipc_service != nullptr && ipc_service->failed()) {
            Fail(&state, "IPC service lost coverage");
            break;
        }
#endif
        if (pipeline->fatal()) {
            Fail(&state, "pipeline became fatal during the measurement window");
            break;
        }
        std::uint64_t cut_start_ns = 0U;
        std::uint64_t cut_end_ns = 0U;
        if (!ClockNs(CLOCK_MONOTONIC, &cut_start_ns)) {
            Fail(&state, "cannot read cut start clock");
            break;
        }
        const runtime::RealtimePipelineCutResultV1 cut =
            pipeline->CutAndPublishGeneration(timeout);
        if (!ClockNs(CLOCK_MONOTONIC, &cut_end_ns)) {
            Fail(&state, "cannot read cut end clock");
            break;
        }
        if (!cut.published()) {
            Fail(
                &state,
                "generation cut failed: " +
                    std::string(runtime::RealtimePipelineCutErrorNameV1(
                        cut.error)));
            break;
        }
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
        if (ipc_service != nullptr && cut.kline_enabled &&
            (cut.kline_generation == nullptr ||
             !ipc_service->PublishKLineGeneration(
                 *cut.kline_generation))) {
            Fail(&state, "IPC KLine publication failed");
            break;
        }
#endif
        SampleRow sample{};
        sample.cut_latency_ns = cut_end_ns - cut_start_ns;
        state.cut_latency_ns.Add(sample.cut_latency_ns);
        ++state.generations;

        std::vector<std::uint64_t> local_acquire;
        local_acquire.reserve(options.acquire_repetitions);
        std::shared_ptr<
            const market::IntradayInstrumentStoreGenerationV1> acquired;
        for (std::uint32_t repetition = 0U;
             repetition < options.acquire_repetitions;
             ++repetition) {
            std::uint64_t before_ns = 0U;
            std::uint64_t after_ns = 0U;
            if (!ClockNs(CLOCK_MONOTONIC, &before_ns)) {
                Fail(&state, "cannot read store acquire start clock");
                break;
            }
            acquired = pipeline->AcquireLatestStoreGeneration();
            if (!ClockNs(CLOCK_MONOTONIC, &after_ns)) {
                Fail(&state, "cannot read store acquire end clock");
                break;
            }
            const std::uint64_t latency = after_ns - before_ns;
            local_acquire.push_back(latency);
            state.acquire_latency_ns.Add(latency);
        }
        if (!state.valid) {
            break;
        }
        if (acquired.get() != cut.store_generation.get()) {
            Fail(
                &state,
                "AcquireLatestStoreGeneration returned wrong handle");
            break;
        }
        sample.acquire_p50_ns = Quantile(local_acquire, 0.50L);
        sample.acquire_p99_ns = Quantile(std::move(local_acquire), 0.99L);

        const std::shared_ptr<const factor::RealtimeFactorGenerationV1> factor =
            pipeline->AcquireLatestFactorGeneration();
        if (factor == nullptr ||
            factor.get() != cut.factor_generation.get() ||
            factor->input_store().get() != cut.store_generation.get() ||
            factor->points().size() != registry_result.registry->size()) {
            Fail(&state, "factor/store atomic generation contract failed");
            break;
        }

        std::uint64_t read_realtime_ns = 0U;
        std::uint64_t read_monotonic_ns = 0U;
        if (!ClockNs(CLOCK_REALTIME, &read_realtime_ns) ||
            !ClockNs(CLOCK_MONOTONIC, &read_monotonic_ns)) {
            Fail(&state, "cannot read store observation clocks");
            break;
        }
        ValidateGenerationAndCollect(
            cut.store_generation,
            *registry_result.registry,
            read_realtime_ns,
            read_monotonic_ns,
            options.intraday_store_from_open,
            &state,
            &sample);
        if (!state.valid) {
            break;
        }

        const runtime::RealtimePipelineSnapshotV1 snapshot =
            pipeline->Snapshot();
        sample.elapsed_ns = read_monotonic_ns - start_monotonic_ns;
        sample.accepted = snapshot.accepted_messages;
        sample.decoded = snapshot.decoded_messages;
        sample.accepted_delta = snapshot.accepted_messages -
            previous.accepted_messages;
        sample.decoded_delta = snapshot.decoded_messages -
            previous.decoded_messages;
        for (std::size_t source = 0U;
             source < sample.source_delta.size();
             ++source) {
            sample.source_delta[source] = snapshot.source_sequences[source] -
                previous.source_sequences[source];
        }
        sample.generation = cut.store_generation->watermark().generation;
        sample.ingress_prefix =
            cut.store_generation->watermark().ingress_sequence_exclusive - 1U;
        if (sample.ingress_prefix > snapshot.decoded_messages ||
            snapshot.rejected_messages != 0U || snapshot.fatal) {
            Fail(&state, "snapshot prefix/rejection/fatal invariant failed");
            break;
        }
        previous = snapshot;
        last_store = cut.store_generation;
        state.samples.push_back(sample);
        next += interval;
    }

    if (std::chrono::steady_clock::now() < deadline && state.valid) {
        std::this_thread::sleep_until(deadline);
    }
    std::uint64_t final_start_ns = 0U;
    std::uint64_t final_end_ns = 0U;
    static_cast<void>(ClockNs(CLOCK_MONOTONIC, &final_start_ns));
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
    if (ipc_service != nullptr) {
        ipc_service->MarkDraining();
    }
#endif
    const runtime::RealtimePipelineCutResultV1 final_cut =
        pipeline->StopAndPublishFinalGeneration(timeout);
    static_cast<void>(ClockNs(CLOCK_MONOTONIC, &final_end_ns));
    if (final_end_ns >= final_start_ns) {
        state.cut_latency_ns.Add(final_end_ns - final_start_ns);
    }
    if (!final_cut.published()) {
        Fail(
            &state,
            "final generation failed: " +
                std::string(runtime::RealtimePipelineCutErrorNameV1(
                    final_cut.error)));
    } else {
        last_store = final_cut.store_generation;
#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
        if (ipc_service != nullptr && final_cut.kline_enabled &&
            (final_cut.kline_generation == nullptr ||
             !ipc_service->PublishKLineGeneration(
                 *final_cut.kline_generation))) {
            Fail(&state, "final IPC KLine publication failed");
        }
#endif
    }
    const runtime::RealtimePipelineSnapshotV1 final_snapshot =
        pipeline->Snapshot();
    const runtime::RealtimePipelineStageLatencySnapshotV1 stage_latency =
        pipeline->LatencySnapshot();
    KLineAcceptance kline_acceptance{};
    kline_acceptance.enabled = !options.kline_windows_ms.empty();
    if (!ClockNs(CLOCK_MONOTONIC, &end_monotonic_ns)) {
        end_monotonic_ns = final_end_ns;
    }
    if (final_cut.published()) {
        ValidateFinalStore(
            options,
            final_cut,
            final_snapshot,
            *registry_result.registry,
            &state);
        if (pipeline->AcquireLatestKLineGeneration().get() !=
            final_cut.kline_generation.get()) {
            Fail(&state, "AcquireLatestKLineGeneration returned wrong handle");
        }
        ValidateKLines(
            options,
            final_cut,
            *registry_result.registry,
            &kline_acceptance,
            &state);
    }
    const std::uint64_t measured_window_ns =
        end_monotonic_ns >= start_monotonic_ns
            ? end_monotonic_ns - start_monotonic_ns
            : 0U;
    const std::uint64_t required_window_ns =
        static_cast<std::uint64_t>(options.duration_seconds) *
        kNanosecondsPerSecond;
    if (measured_window_ns < required_window_ns ||
        final_snapshot.accepted_messages == 0U ||
        final_snapshot.accepted_messages != final_snapshot.decoded_messages ||
        final_snapshot.accepted_messages !=
            final_snapshot.store.appended_records ||
        final_snapshot.rejected_messages != 0U || final_snapshot.fatal ||
        !final_snapshot.stopped || last_store == nullptr ||
        (options.intraday_store_from_open &&
         !AllKindsSeen(state.event_kinds_seen))) {
        Fail(&state, "final duration/count/state/event-kind acceptance gate failed");
    }
    if (options.measure_stage_latency) {
        std::uint64_t callback_observations = 0U;
        std::uint64_t append_observations = 0U;
        for (std::size_t source = 0U;
             source < stage_latency.callback_samples_by_source.size();
             ++source) {
            callback_observations +=
                stage_latency.callback_samples_by_source[source];
            append_observations +=
                stage_latency.append_samples_by_source[source];
        }
        const auto completely_accounted = [](
            const runtime::RealtimeLatencyDistributionV1& distribution,
            std::uint64_t expected) noexcept {
            return distribution.samples <= expected &&
                   distribution.invalid_samples ==
                       expected - distribution.samples;
        };
        if (!stage_latency.enabled ||
            callback_observations != final_snapshot.accepted_messages ||
            append_observations != final_snapshot.store.appended_records ||
            !completely_accounted(
                stage_latency.sdk_local_to_callback_success,
                callback_observations) ||
            !completely_accounted(
                stage_latency.callback_entry_to_success,
                callback_observations) ||
            !completely_accounted(
                stage_latency.sdk_local_to_append_complete,
                append_observations) ||
            !completely_accounted(
                stage_latency.callback_entry_to_append_complete,
                append_observations) ||
            !completely_accounted(
                stage_latency.callback_entry_to_inprocess_latest_read,
                append_observations) ||
            !completely_accounted(
                stage_latency.append_call,
                append_observations)) {
            Fail(&state, "stage-latency observation accounting failed");
        }
    }

#if defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V1)
    if (ipc_service != nullptr) {
        if (state.valid &&
            !ipc_service->MarkStoppedClean(
                final_snapshot.tick_stream_sequence)) {
            Fail(&state, "IPC final tick prefix is incomplete");
        }
        if (!state.valid) {
            ipc_service->MarkFailed();
        }
    }
#endif

    const std::filesystem::path report_temporary =
        options.report_json.string() + ".tmp";
    const std::filesystem::path samples_temporary =
        options.samples_csv.string() + ".tmp";
    const std::filesystem::path per_instrument_temporary =
        options.per_instrument_read_timings_csv.empty()
            ? std::filesystem::path{}
            : std::filesystem::path(
                  options.per_instrument_read_timings_csv.string() + ".tmp");
    if (!WriteSamples(samples_temporary, state.samples, &error)) {
        std::cerr << "accept-realtime-pipeline: " << error << '\n';
        return 2;
    }
    if (!per_instrument_temporary.empty() &&
        !WritePerInstrumentDirectReadTimings(
            per_instrument_temporary,
            state,
            *registry_result.registry,
            &error)) {
        std::cerr << "accept-realtime-pipeline: " << error << '\n';
        return 2;
    }
    std::ofstream report(report_temporary, std::ios::out | std::ios::trunc);
    if (!report.is_open()) {
        std::cerr << "accept-realtime-pipeline: cannot create report JSON\n";
        return 2;
    }
    const std::uint64_t accepted_in_window =
        final_snapshot.accepted_messages - initial.accepted_messages;
    const long double seconds =
        static_cast<long double>(measured_window_ns) /
        static_cast<long double>(kNanosecondsPerSecond);
    const long double accepted_per_second = seconds > 0.0L
        ? static_cast<long double>(accepted_in_window) / seconds
        : 0.0L;
    const long double intraday_full_scan_records_per_second =
        state.intraday_full_scan_ns > 0U
            ? static_cast<long double>(
                  state.intraday_full_scan_records) *
                  static_cast<long double>(kNanosecondsPerSecond) /
                  static_cast<long double>(
                      state.intraday_full_scan_ns)
            : 0.0L;
    const std::size_t reported_scan_shards = std::min({
        state.intraday_reader_cpus.size(),
        state.intraday_scan_ordinal_begins.size(),
        state.intraday_scan_ordinal_ends.size(),
        state.intraday_scan_shard_records.size(),
        state.intraday_scan_shard_ns.size(),
        state.intraday_scan_shard_last_instrument_ids.size(),
        state.intraday_scan_shard_last_ingress_sequences.size()});
    report << "{\n  \"schema_version\":1,\n"
           << "  \"passed\":" << (state.valid ? "true" : "false")
           << ",\n  \"first_error\":";
    WriteJsonString(report, state.first_error);
    report << ",\n  \"requested_window_seconds\":"
           << options.duration_seconds
           << ",\n  \"measured_window_ns\":" << measured_window_ns
           << ",\n  \"generation_interval_ms\":"
           << options.generation_interval_ms
           << ",\n  \"store_workers\":"
           << options.instrument_store_workers
           << ",\n  \"registry_version\":"
           << registry_result.registry->registry_version()
           << ",\n  \"registry_sha256\":\""
           << common::Sha256Hex(registry_result.registry->registry_sha256())
           << "\",\n  \"registry_entries\":"
           << registry_result.registry->size()
           << ",\n  \"sdk_library\":";
    WriteJsonString(report, options.sdk_library.string());
    report << ",\n  \"server_address\":";
    WriteJsonString(report, options.server_address);
    report << ",\n  \"kline\":{\"enabled\":"
           << (kline_acceptance.enabled ? "true" : "false")
           << ",\"generation_bars\":"
           << kline_acceptance.generation_bar_count
           << ",\"minimum_consecutive_bars\":"
           << options.kline_min_consecutive_bars
           << ",\"windows\":[";
    for (std::size_t window_index = 0U;
         window_index < kline_acceptance.windows.size();
         ++window_index) {
        if (window_index != 0U) {
            report.put(',');
        }
        const KLineWindowAcceptance& window =
            kline_acceptance.windows[window_index];
        report << "{\"window_id\":" << window.window_id
               << ",\"duration_ns\":\"" << window.duration_ns
               << "\",\"bars\":" << window.bars
               << ",\"instruments_with_bars\":"
               << window.instruments_with_bars
               << ",\"trades\":\"" << window.trades
               << "\",\"longest_consecutive_bars\":"
               << window.longest_consecutive_bars
               << ",\"representative_instrument_id\":"
               << window.representative_instrument_id
               << ",\"representative_security_id\":";
        const market::InstrumentRegistryLookupResultV1 lookup =
            registry_result.registry->LookupById(
                window.representative_instrument_id);
        WriteJsonString(
            report,
            lookup.known()
                ? RegistryBytesText(lookup.entry->key.security_id)
                : std::string{});
        report << ",\"representative_bars\":[";
        for (std::size_t bar_index = 0U;
             bar_index < window.representative_bars.size();
             ++bar_index) {
            if (bar_index != 0U) {
                report.put(',');
            }
            const market::KLineBarV1& bar =
                window.representative_bars[bar_index];
            report << "{\"start_ns_since_midnight\":\""
                   << bar.window_start_ns_since_midnight
                   << "\",\"end_ns_since_midnight\":\""
                   << bar.window_end_ns_since_midnight
                   << "\",\"open_price_p6\":"
                   << bar.open_price_p6
                   << ",\"high_price_p6\":"
                   << bar.high_price_p6
                   << ",\"low_price_p6\":"
                   << bar.low_price_p6
                   << ",\"close_price_p6\":"
                   << bar.close_price_p6
                   << ",\"volume_raw\":\"" << bar.volume_raw
                   << "\",\"volume_scale\":"
                   << static_cast<unsigned int>(bar.volume_scale)
                   << ",\"quantity_unit\":"
                   << static_cast<unsigned int>(bar.quantity_unit)
                   << ",\"trade_count\":\"" << bar.trade_count
                   << "\",\"revision\":\"" << bar.revision
                   << "\",\"first_event_ns_since_midnight\":\""
                   << bar.first_trade.event_time_ns_since_midnight
                   << "\",\"last_event_ns_since_midnight\":\""
                   << bar.last_trade.event_time_ns_since_midnight
                   << "\"}";
        }
        report << "]}";
    }
    report << "]},\n  \"intraday_store\":{\"record_limit\":"
           << final_snapshot.store.maximum_session_records
           << ",\"byte_limit\":"
           << final_snapshot.store
                  .maximum_session_accounted_bytes
           << ",\"records\":"
           << final_snapshot.store.appended_records
           << ",\"record_bytes\":"
           << final_snapshot.store.accounted_record_bytes
           << ",\"index_bytes\":"
           << final_snapshot.store.allocated_index_bytes
           << ",\"allocated_segments\":"
           << final_snapshot.store.allocated_segments
           << ",\"failed_appends\":"
           << final_snapshot.store.failed_appends
           << ",\"latest_generation\":"
           << final_snapshot.store.latest_generation
           << ",\"full_scan_records\":"
           << state.intraday_full_scan_records
           << ",\"full_scan_ns\":"
           << state.intraday_full_scan_ns
           << ",\"records_per_second\":"
           << std::fixed << std::setprecision(3)
           << intraday_full_scan_records_per_second
           << std::defaultfloat
           << ",\"segment_target_bytes\":"
           << (static_cast<std::uint64_t>(
                   options.intraday_store_segment_kib) *
               1024U)
           << ",\"maximum_batch_records\":"
           << options.intraday_store_batch_records
           << ",\"scan_batch_records\":"
           << options.intraday_scan_batch_records
           << ",\"scan_workers\":"
           << options.intraday_scan_workers
           << ",\"scan_partition_ns\":"
           << state.intraday_scan_partition_ns
           << ",\"scan_total_ns\":"
           << state.intraday_scan_total_ns
           << ",\"ingress_sequence_sum_modulo_u64\":\""
           << state.intraday_scan_ingress_sum
           << "\",\"ingress_sequence_xor\":\""
           << state.intraday_scan_ingress_xor
           << "\",\"last_instrument_id\":"
           << state.intraday_scan_last_instrument_id
           << ",\"last_ingress_sequence\":\""
           << state.intraday_scan_last_ingress_sequence
           << '"'
           << ",\"reader_cpus\":[";
    for (std::size_t index = 0U;
         index < state.intraday_reader_cpus.size();
         ++index) {
        if (index != 0U) {
            report.put(',');
        }
        report << state.intraday_reader_cpus[index];
    }
    report << "],\"event_kind_counts\":[";
    for (std::size_t kind = 0U;
         kind < state.intraday_scan_kind_counts.size();
         ++kind) {
        if (kind != 0U) {
            report.put(',');
        }
        report << state.intraday_scan_kind_counts[kind];
    }
    report << "],\"scan_shards\":[";
    for (std::size_t index = 0U;
         index < reported_scan_shards;
         ++index) {
        if (index != 0U) {
            report.put(',');
        }
        report << "{\"cpu\":"
               << state.intraday_reader_cpus[index]
               << ",\"ordinal_begin\":"
               << state.intraday_scan_ordinal_begins[index]
               << ",\"ordinal_end\":"
               << state.intraday_scan_ordinal_ends[index]
               << ",\"records\":"
               << state.intraday_scan_shard_records[index]
               << ",\"elapsed_ns\":"
               << state.intraday_scan_shard_ns[index]
               << ",\"last_instrument_id\":"
               << state.intraday_scan_shard_last_instrument_ids[index]
               << ",\"last_ingress_sequence\":\""
               << state
                      .intraday_scan_shard_last_ingress_sequences[index]
               << '"'
               << '}';
    }
    report << ']'
           << ",\"coverage_from_open\":"
           << (final_snapshot.store.coverage_from_open
                   ? "true"
                   : "false")
           << ",\"coverage_lost\":"
           << (final_snapshot.store.coverage_lost
                   ? "true"
                   : "false")
           << "},\n  \"per_instrument_direct_read\":{\"enabled\":"
           << (state.per_instrument_direct_read_enabled
                   ? "true"
                   : "false")
           << ",\"clock\":\"CLOCK_MONOTONIC\""
           << ",\"reader_cpu\":"
           << state.per_instrument_direct_read_cpu
           << ",\"batch_records\":"
           << options.intraday_scan_batch_records
           << ",\"instruments\":"
           << state.per_instrument_direct_read_timings.size()
           << ",\"nonempty_instruments\":"
           << state.per_instrument_direct_read_nonempty
           << ",\"records\":"
           << state.per_instrument_direct_read_records
           << ",\"pass_wall_ns\":"
           << state.per_instrument_direct_read_pass_ns
           << ",\"sum_open_ns\":"
           << state.per_instrument_direct_read_open_ns
           << ",\"sum_read_ns\":"
           << state.per_instrument_direct_read_read_ns
           << ",\"sum_total_ns\":"
           << state.per_instrument_direct_read_total_ns
           << ",\"all_instrument_total_ns\":";
    WriteDistributionJson(
        report,
        state.per_instrument_direct_read_all_total_ns.values);
    report << ",\"nonempty_instrument_total_ns\":";
    WriteDistributionJson(
        report,
        state.per_instrument_direct_read_nonempty_total_ns.values);
    report << ",\"nonempty_instrument_read_ns\":";
    WriteDistributionJson(
        report,
        state.per_instrument_direct_read_nonempty_read_ns.values);
    report << ",\"nonempty_instrument_read_ns_per_record\":";
    WriteDistributionJson(
        report,
        state.per_instrument_direct_read_nonempty_ns_per_record.values);
    report
           << ",\"timing_boundary\":"
              "\"post-drain immutable terminal generation; per instrument from immediately before OpenInstrumentCursor through the terminal ReadBatch; excludes SDK wait, generation cut/build/publication, Acquire, file output and the later validating universe scan\""
           << ",\"read_work\":"
              "\"streams the complete cursor into a preallocated pointer batch and counts returned records; no payload-field consumer computation is included\""
           << "},\n  \"counts\":{\"accepted\":"
           << final_snapshot.accepted_messages
           << ",\"decoded\":" << final_snapshot.decoded_messages
           << ",\"appended\":"
           << final_snapshot.store.appended_records
           << ",\"ignored\":" << final_snapshot.ignored_messages
           << ",\"post_cut\":" << final_snapshot.post_cut_messages
           << ",\"rejected\":" << final_snapshot.rejected_messages
           << ",\"generations_sampled\":" << state.generations
           << ",\"last_published_generation\":"
           << final_snapshot.last_published_generation
           << ",\"updated_instrument_heads\":" << state.updated_heads
           << ",\"event_time_samples\":" << state.event_time_samples
           << ",\"universe_rows_checked\":"
           << state.universe_rows_checked << "},\n"
           << "  \"source_sequences\":[";
    for (std::size_t source = 0U;
         source < final_snapshot.source_sequences.size();
         ++source) {
        if (source != 0U) {
            report.put(',');
        }
        report << final_snapshot.source_sequences[source];
    }
    report << "],\n  \"event_kinds_seen\":[";
    for (std::size_t kind = 0U; kind < state.event_kinds_seen.size(); ++kind) {
        if (kind != 0U) {
            report.put(',');
        }
        report << (state.event_kinds_seen[kind] ? "true" : "false");
    }
    report << "],\n  \"event_kind_coverage_required\":"
           << (options.intraday_store_from_open ? "true" : "false")
           << ",\n  \"all_event_kinds_seen\":"
           << (AllKindsSeen(state.event_kinds_seen) ? "true" : "false")
           << ",\n  \"throughput\":{\"accepted_per_second\":"
           << std::fixed << std::setprecision(3) << accepted_per_second
           << ",\"decoded_per_second\":" << accepted_per_second
           << std::defaultfloat << "},\n"
           << "  \"latency_ns\":{\n"
           << "    \"generation_cut_and_factor\":";
    WriteDistributionJson(report, state.cut_latency_ns.values);
    report << ",\n    \"store_acquire_call\":";
    WriteDistributionJson(report, state.acquire_latency_ns.values);
    report << ",\n    \"generation_cut_to_read\":";
    WriteDistributionJson(report, state.generation_age_ns.values);
    report << ",\n    \"global_market_head_recv_to_read\":";
    WriteDistributionJson(report, state.global_head_recv_age_ns.values);
    report << ",\n    \"global_market_head_event_to_read\":";
    WriteDistributionJson(report, state.global_head_event_age_ns.values);
    report << ",\n    \"updated_instrument_head_recv_to_read\":";
    WriteDistributionJson(report, state.updated_head_recv_age_ns.values);
    report << ",\n    \"updated_instrument_head_event_to_read\":";
    WriteDistributionJson(report, state.updated_head_event_age_ns.values);
    report << "\n  },\n  \"stage_latency_ns\":{\n"
           << "    \"schema_version\":2,\n"
           << "    \"enabled\":"
           << (stage_latency.enabled ? "true" : "false")
           << ",\n    \"sdk_local_time_trade_date\":"
           << stage_latency.sdk_local_time_trade_date
           << ",\n    \"callback_samples_by_source\":[";
    for (std::size_t source = 0U;
         source < stage_latency.callback_samples_by_source.size();
         ++source) {
        if (source != 0U) {
            report.put(',');
        }
        report << stage_latency.callback_samples_by_source[source];
    }
    report << "],\n    \"append_samples_by_source\":[";
    for (std::size_t source = 0U;
         source < stage_latency.append_samples_by_source.size();
         ++source) {
        if (source != 0U) {
            report.put(',');
        }
        report << stage_latency.append_samples_by_source[source];
    }
    report << "],\n    \"sdk_local_to_callback_success\":";
    WriteStageLatencyDistributionJson(
        report, stage_latency.sdk_local_to_callback_success);
    report << ",\n    \"sdk_local_to_append_complete\":";
    WriteStageLatencyDistributionJson(
        report, stage_latency.sdk_local_to_append_complete);
    report << ",\n    \"callback_entry_to_success\":";
    WriteStageLatencyDistributionJson(
        report, stage_latency.callback_entry_to_success);
    report << ",\n    \"callback_entry_to_append_complete\":";
    WriteStageLatencyDistributionJson(
        report, stage_latency.callback_entry_to_append_complete);
    report << ",\n    \"callback_entry_to_inprocess_latest_read\":";
    WriteStageLatencyDistributionJson(
        report,
        stage_latency.callback_entry_to_inprocess_latest_read);
    report << ",\n    \"append_call\":";
    WriteStageLatencyDistributionJson(report, stage_latency.append_call);
    report << "\n  },\n  \"latency_semantics\":{\n"
           << "    \"store_acquire_call\":"
              "\"CLOCK_MONOTONIC around atomic shared store acquisition\",\n"
           << "    \"recv_to_read\":"
              "\"read CLOCK_MONOTONIC minus record recv_monotonic_ns; local process freshness including queue, decode, store barrier and publication interval\",\n"
           << "    \"event_to_read\":"
              "\"read CLOCK_REALTIME minus decoded exchange event_time_ns; end-to-end market freshness including upstream/feed/network/process/publication\",\n"
           << "    \"sdk_local_to_callback_success\":"
              "\"successful callback admission completion CLOCK_REALTIME minus MDLMessageHead::LocalTime projected onto sdk_local_time_trade_date at fixed UTC+08; signed and contaminated by upstream delay and realtime clock offset; source resolution is 1 ms\",\n"
           << "    \"sdk_local_to_append_complete\":"
              "\"first CLOCK_REALTIME observation after IntradayInstrumentStoreV1::Append returned success minus the same projected SDK LocalTime\",\n"
           << "    \"callback_entry_to_success\":"
              "\"same-host CLOCK_MONOTONIC from callback entry observation through owned copy, decoder enqueue, optional WAL enqueue, and admission-lock release\",\n"
           << "    \"callback_entry_to_append_complete\":"
              "\"same-host CLOCK_MONOTONIC from callback entry observation to first observation after store Append returned success; includes decoder and history queues\",\n"
           << "    \"callback_entry_to_inprocess_latest_read\":"
              "\"same-host CLOCK_MONOTONIC from callback entry observation until an immediate allocation-free acquire-read through the in-process latest model has returned and verified the exact Store-owned record pointer and ingress sequence; excludes immutable generation cut/acquire and runs before the optional IPC sink\",\n"
           << "    \"append_call\":"
              "\"CLOCK_MONOTONIC from immediately before successful-path input/route validation to the first observation after IntradayInstrumentStoreV1::Append returns; a tight upper bound for the call that excludes post-return histogram aggregation\",\n"
           << "    \"measurement_perturbation\":"
              "\"diagnostic mode adds clock reads and atomic histogram updates; completion timestamps are captured before histogram aggregation, but later messages can observe the instrumentation load\"\n"
           << "  }\n}\n";
    report.flush();
    if (!report.good()) {
        std::cerr << "accept-realtime-pipeline: report JSON write failed\n";
        return 2;
    }
    report.close();
    std::error_code rename_error;
    std::filesystem::rename(samples_temporary, options.samples_csv, rename_error);
    if (rename_error) {
        std::cerr << "accept-realtime-pipeline: cannot publish samples CSV\n";
        return 2;
    }
    if (!per_instrument_temporary.empty()) {
        std::filesystem::rename(
            per_instrument_temporary,
            options.per_instrument_read_timings_csv,
            rename_error);
        if (rename_error) {
            std::cerr
                << "accept-realtime-pipeline: cannot publish per-instrument "
                   "read-timings CSV\n";
            return 2;
        }
    }
    std::filesystem::rename(report_temporary, options.report_json, rename_error);
    if (rename_error) {
        std::cerr << "accept-realtime-pipeline: cannot publish report JSON\n";
        return 2;
    }
    std::cout << "{\"passed\":" << (state.valid ? "true" : "false")
              << ",\"measured_window_ns\":" << measured_window_ns
              << ",\"accepted\":" << final_snapshot.accepted_messages
              << ",\"decoded\":" << final_snapshot.decoded_messages
              << ",\"accepted_per_second\":" << std::fixed
              << std::setprecision(3) << accepted_per_second
              << ",\"generations\":" << state.generations
              << ",\"intraday_store\":{\"records\":"
              << final_snapshot.store.appended_records
              << ",\"record_bytes\":"
              << final_snapshot.store.accounted_record_bytes
              << ",\"index_bytes\":"
              << final_snapshot.store.allocated_index_bytes
              << ",\"byte_limit\":"
              << final_snapshot.store
                     .maximum_session_accounted_bytes
              << ",\"full_scan_records\":"
              << state.intraday_full_scan_records
              << ",\"full_scan_ns\":"
              << state.intraday_full_scan_ns
              << ",\"scan_total_ns\":"
              << state.intraday_scan_total_ns
              << ",\"records_per_second\":"
              << intraday_full_scan_records_per_second
              << ",\"scan_batch_records\":"
              << options.intraday_scan_batch_records
              << ",\"scan_workers\":"
              << options.intraday_scan_workers
              << ",\"coverage_from_open\":"
              << (final_snapshot.store.coverage_from_open
                      ? "true"
                      : "false")
              << ",\"coverage_lost\":"
              << (final_snapshot.store.coverage_lost
                      ? "true"
                      : "false")
              << "},\"per_instrument_direct_read\":{\"enabled\":"
              << (state.per_instrument_direct_read_enabled
                      ? "true"
                      : "false")
              << ",\"instruments\":"
              << state.per_instrument_direct_read_timings.size()
              << ",\"nonempty_instruments\":"
              << state.per_instrument_direct_read_nonempty
              << ",\"records\":"
              << state.per_instrument_direct_read_records
              << ",\"pass_wall_ns\":"
              << state.per_instrument_direct_read_pass_ns
              << ",\"sum_open_ns\":"
              << state.per_instrument_direct_read_open_ns
              << ",\"sum_read_ns\":"
              << state.per_instrument_direct_read_read_ns
              << ",\"sum_total_ns\":"
              << state.per_instrument_direct_read_total_ns
              << "},\"kline\":{\"enabled\":"
              << (kline_acceptance.enabled ? "true" : "false")
              << ",\"generation_bars\":"
              << kline_acceptance.generation_bar_count
              << ",\"windows\":[";
    for (std::size_t window_index = 0U;
         window_index < kline_acceptance.windows.size();
         ++window_index) {
        if (window_index != 0U) {
            std::cout.put(',');
        }
        const KLineWindowAcceptance& window =
            kline_acceptance.windows[window_index];
        std::cout << "{\"window_id\":" << window.window_id
                  << ",\"bars\":" << window.bars
                  << ",\"instruments_with_bars\":"
                  << window.instruments_with_bars
                  << ",\"trades\":\"" << window.trades
                  << "\",\"longest_consecutive_bars\":"
                  << window.longest_consecutive_bars
                  << ",\"representative_instrument_id\":"
                  << window.representative_instrument_id << '}';
    }
    std::cout << "]},\"stage_latency\":{\"enabled\":"
              << (stage_latency.enabled ? "true" : "false")
              << ",\"sdk_local_to_callback_p50_ns\":"
              << stage_latency.sdk_local_to_callback_success.p50.estimate_ns
              << ",\"sdk_local_to_callback_p99_ns\":"
              << stage_latency.sdk_local_to_callback_success.p99.estimate_ns
              << ",\"sdk_local_to_append_p50_ns\":"
              << stage_latency.sdk_local_to_append_complete.p50.estimate_ns
              << ",\"sdk_local_to_append_p99_ns\":"
              << stage_latency.sdk_local_to_append_complete.p99.estimate_ns
              << ",\"callback_work_p50_ns\":"
              << stage_latency.callback_entry_to_success.p50.estimate_ns
              << ",\"callback_work_p99_ns\":"
              << stage_latency.callback_entry_to_success.p99.estimate_ns
              << ",\"callback_entry_to_append_p50_ns\":"
              << stage_latency.callback_entry_to_append_complete.p50.estimate_ns
              << ",\"callback_entry_to_append_p99_ns\":"
              << stage_latency.callback_entry_to_append_complete.p99.estimate_ns
              << ",\"append_call_p50_ns\":"
              << stage_latency.append_call.p50.estimate_ns
              << ",\"append_call_p99_ns\":"
              << stage_latency.append_call.p99.estimate_ns
              << "}}\n";
    return state.valid ? 0 : 1;
}

}  // namespace

int main(int argc, char* argv[]) {
    Options options{};
    bool help = false;
    std::string error;
    if (!ParseOptions(argc, argv, &options, &help, &error)) {
        std::cerr << "accept-realtime-pipeline: " << error << '\n';
        PrintUsage(std::cerr);
        return 2;
    }
    if (help) {
        PrintUsage(std::cout);
        return 0;
    }
    try {
        return Run(options);
    } catch (const std::exception& exception) {
        std::cerr << "accept-realtime-pipeline: fatal: "
                  << exception.what() << '\n';
        return 2;
    } catch (...) {
        std::cerr << "accept-realtime-pipeline: unknown fatal error\n";
        return 2;
    }
}
