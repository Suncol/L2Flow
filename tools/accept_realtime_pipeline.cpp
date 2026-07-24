#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/market/instrument_registry_loader_v1.h"
#include "l2flow/runtime/realtime_pipeline_v1.h"

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
    std::uint32_t duration_seconds = 600U;
    std::uint32_t generation_interval_ms = 1000U;
    std::uint32_t generation_timeout_ms = 10'000U;
    std::uint32_t instrument_store_workers = 4U;
    std::uint32_t acquire_repetitions = 32U;
    std::uint64_t intraday_store_maximum_records = 0U;
    std::uint64_t intraday_store_memory_bytes = 0U;
    std::uint32_t intraday_store_chunk_records = 1024U;
    std::uint32_t intraday_store_batch_records = 64U * 1024U;
    std::uint32_t intraday_scan_batch_records = 1024U;
    std::uint32_t intraday_scan_workers = 1U;
    std::vector<std::uint32_t> intraday_reader_cpus;
    bool intraday_scan_batch_records_set = false;
    bool intraday_reader_cpus_set = false;
    bool intraday_store_from_open = false;
    bool intraday_store_maximum_records_set = false;
    bool intraday_store_memory_set = false;
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
        << "  --intraday-store-from-open    require continuous coverage from "
           "market open\n"
        << "Optional:\n"
        << "  --duration-seconds N          default 600\n"
        << "  --generation-interval-ms N    default 1000\n"
        << "  --generation-timeout-ms N     default 10000\n"
        << "  --instrument-store-workers N  default 4\n"
        << "  --acquire-repetitions N       default 32\n"
        << "  --intraday-store-chunk-records N\n"
        << "                                1..65536, default 1024\n"
        << "  --intraday-store-batch-records N\n"
        << "                                Store ReadBatch upper bound; "
           "1..1048576, default 65536\n"
        << "  --intraday-scan-batch-records N\n"
        << "                                actual final-scan page; "
           "1..Store upper bound, default min(1024, Store upper bound)\n"
        << "  --intraday-scan-workers N     independent ordinal-range readers; "
           "1..256, default 1\n"
        << "  --intraday-reader-cpus LIST   one allowed CPU per scan worker; "
           "default first allowed CPUs\n";
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
        } else if (option == "--intraday-store-chunk-records") {
            if (!ParseU32(
                    value, &parsed.intraday_store_chunk_records) ||
                parsed.intraday_store_chunk_records == 0U ||
                static_cast<std::size_t>(
                    parsed.intraday_store_chunk_records) >
                    market::
                        kIntradayInstrumentStoreMaximumChunkRecordsV1) {
                *error =
                    "--intraday-store-chunk-records must be 1..65536";
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
        parsed.duration_seconds > 86'400U ||
        parsed.generation_interval_ms > 60'000U ||
        parsed.generation_timeout_ms > 600'000U ||
        parsed.instrument_store_workers > 256U ||
        parsed.acquire_repetitions > 10'000U ||
        parsed.intraday_scan_workers > 256U) {
        *error = "required option missing or option is out of range";
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
        !parsed.intraday_store_memory_set ||
        !parsed.intraday_store_from_open) {
        *error =
            "store-only acceptance requires explicit positive "
            "--intraday-store-max-records, --intraday-store-memory-gib, "
            "and --intraday-store-from-open";
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
    if (!generation->coverage_from_open() ||
        generation->instrument_count() != entries.size() ||
        state->last_seen_head_by_universe_index.size() != entries.size()) {
        Fail(
            state,
            "store generation does not contain the exact from-open universe");
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
        !snapshot.store.coverage_from_open ||
        !options.intraday_store_from_open) {
        Fail(state, "required intraday store lost from-open coverage");
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

[[nodiscard]] bool AllKindsSeen(
    const std::array<bool, kEventKindCount>& seen) noexcept {
    return std::all_of(seen.begin(), seen.end(), [](bool value) {
        return value;
    });
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
    config.intraday_store.chunk_record_capacity =
        static_cast<std::size_t>(options.intraday_store_chunk_records);
    config.intraday_store.maximum_session_records =
        options.intraday_store_maximum_records;
    config.intraday_store.maximum_session_accounted_bytes =
        options.intraday_store_memory_bytes;
    config.intraday_store.maximum_records_per_batch =
        static_cast<std::size_t>(options.intraday_store_batch_records);
    config.intraday_store.coverage_from_open =
        options.intraday_store_from_open;
    config.enforce_receive_trade_date = true;
    config.sdk.enabled = true;
    config.sdk.library_path = options.sdk_library;
    config.sdk.server_address = options.server_address;
    config.sdk.user_name = std::move(user_name);
    config.sdk.log_prefix = options.sdk_log_prefix.string();
    config.sdk.message_encoding = datayes::mdl::MDLEID_BINARY;
    config.sdk.merge_message = false;

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

    AcceptanceState state{};
    state.last_seen_head_by_universe_index.resize(
        registry_result.registry->size(), 0U);
    const runtime::RealtimePipelineSnapshotV1 initial = pipeline->Snapshot();
    runtime::RealtimePipelineSnapshotV1 previous = initial;
    std::uint64_t start_monotonic_ns = 0U;
    std::uint64_t end_monotonic_ns = 0U;
    if (!ClockNs(CLOCK_MONOTONIC, &start_monotonic_ns)) {
        pipeline->StopAndDrain();
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
    }
    const runtime::RealtimePipelineSnapshotV1 final_snapshot =
        pipeline->Snapshot();
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
        !AllKindsSeen(state.event_kinds_seen)) {
        Fail(&state, "final duration/count/state/five-kind acceptance gate failed");
    }

    const std::filesystem::path report_temporary =
        options.report_json.string() + ".tmp";
    const std::filesystem::path samples_temporary =
        options.samples_csv.string() + ".tmp";
    if (!WriteSamples(samples_temporary, state.samples, &error)) {
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
    report << ",\n  \"intraday_store\":{\"record_limit\":"
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
           << ",\"allocated_chunks\":"
           << final_snapshot.store.allocated_chunks
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
           << ",\"chunk_records\":"
           << options.intraday_store_chunk_records
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
           << "},\n  \"counts\":{\"accepted\":"
           << final_snapshot.accepted_messages
           << ",\"decoded\":" << final_snapshot.decoded_messages
           << ",\"appended\":"
           << final_snapshot.store.appended_records
           << ",\"ignored\":" << final_snapshot.ignored_messages
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
    report << "],\n  \"throughput\":{\"accepted_per_second\":"
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
    report << "\n  },\n  \"latency_semantics\":{\n"
           << "    \"store_acquire_call\":"
              "\"CLOCK_MONOTONIC around atomic shared store acquisition\",\n"
           << "    \"recv_to_read\":"
              "\"read CLOCK_MONOTONIC minus record recv_monotonic_ns; local process freshness including queue, decode, store barrier and publication interval\",\n"
           << "    \"event_to_read\":"
              "\"read CLOCK_REALTIME minus decoded exchange event_time_ns; end-to-end market freshness including upstream/feed/network/process/publication\"\n"
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
