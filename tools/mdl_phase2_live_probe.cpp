#include "l2flow/baseline/vendor_baseline.h"
#include "l2flow/build_manifest.h"
#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/ingress/byte_ring.h"
#include "l2flow/ingress/callback_handler.h"
#include "l2flow/ingress/capture_clock.h"
#include "l2flow/ingress/capture_metrics.h"
#include "l2flow/ingress/clock_epoch.h"
#include "l2flow/ingress/raw_capture_worker.h"
#include "l2flow/ingress/raw_ingress_config.h"
#include "l2flow/ingress/raw_posix_io.h"
#include "l2flow/ingress/raw_reader.h"
#include "l2flow/ingress/raw_recovery.h"
#include "l2flow/ingress/raw_schema.h"
#include "l2flow/ingress/raw_v1.h"
#include "l2flow/ingress/raw_wal_writer.h"
#include "l2flow/ops/fatal_latch.h"
#include "l2flow/sdk/endpoint_contract.h"
#include "l2flow/sdk/sdk_runtime.h"
#include "l2flow/sdk/subscription_manifest.h"

#include "mdl_sys_msg.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <exception>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <cerrno>
#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

namespace baseline = l2flow::baseline;
namespace common = l2flow::common;
namespace ingress = l2flow::ingress;
namespace mdl = datayes::mdl;
namespace sdk = l2flow::sdk;

namespace {

constexpr std::uint64_t kMebibyte = 1024ULL * 1024ULL;
constexpr std::uint64_t kGibibyte = 1024ULL * 1024ULL * 1024ULL;
constexpr std::uint32_t kMaximumMessageBytes = 16U * 1024U * 1024U;
constexpr std::uint64_t kMaximumFramedRecordBytes =
    static_cast<std::uint64_t>(kMaximumMessageBytes) +
    static_cast<std::uint64_t>(ingress::kRawV1RecordHeaderBytes) +
    static_cast<std::uint64_t>(ingress::kRawV1RecordAlignment - 1U) +
    static_cast<std::uint64_t>(ingress::kRawV1RecordTrailerBytes);
constexpr std::uint64_t kDefaultRingBytes = 64ULL * kMebibyte;
constexpr std::uint64_t kDefaultMaximumRawBytes = kGibibyte;
constexpr std::string_view kLocalClientLabel =
    "l2flow-local-phase2-live-probe";

volatile std::sig_atomic_t g_stop_requested = 0;

extern "C" void HandleStopSignal(int) {
    g_stop_requested = 1;
}

struct Options final {
    std::filesystem::path library;
    std::filesystem::path output_directory;
    sdk::IngressKind ingress_kind = sdk::IngressKind::ShSnapshot;
    bool ingress_kind_set = false;
    std::string address = "127.0.0.1:9112";
    std::string sdk_log_prefix =
        "/tmp/l2flow-mdl-phase2-live-probe";
    std::uint32_t logon_timeout_seconds = 15U;
    std::uint32_t monitor_seconds = 60U;
    std::uint32_t minimum_market_messages_per_key = 1U;
    std::uint32_t capture_date = 0U;
    std::uint64_t ring_bytes = kDefaultRingBytes;
    std::uint64_t maximum_raw_bytes = kDefaultMaximumRawBytes;
};

class FileDescriptor final {
public:
    FileDescriptor() noexcept = default;
    explicit FileDescriptor(int value) noexcept : value_(value) {}
    ~FileDescriptor() {
        Reset();
    }

    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;

    FileDescriptor(FileDescriptor&& other) noexcept
        : value_(std::exchange(other.value_, -1)) {}

    FileDescriptor& operator=(FileDescriptor&& other) noexcept {
        if (this != &other) {
            Reset(std::exchange(other.value_, -1));
        }
        return *this;
    }

    [[nodiscard]] int get() const noexcept {
        return value_;
    }

    [[nodiscard]] bool valid() const noexcept {
        return value_ >= 0;
    }

    [[nodiscard]] int release() noexcept {
        return std::exchange(value_, -1);
    }

    void Reset(int value = -1) noexcept {
        if (value_ >= 0) {
            static_cast<void>(::close(value_));
        }
        value_ = value;
    }

private:
    int value_ = -1;
};

void PrintUsage(std::ostream& output) {
    output
        << "Usage: mdl-phase2-live-probe --library PATH --output-dir PATH\n"
        << "       --ingress-kind KIND [options]\n"
        << "  --ingress-kind KIND       sh-snapshot, sh-tick, sz-snapshot,"
           " or sz-tick\n"
        << "  --address HOST:PORT       feeder endpoint"
           " (default 127.0.0.1:9112)\n"
        << "  --sdk-log-prefix PATH     SDK log prefix"
           " (default /tmp/l2flow-mdl-phase2-live-probe)\n"
        << "  --logon-timeout-seconds N wait for LogonResponse, 1..60"
           " (default 15)\n"
        << "  --monitor-seconds N       capture duration after logon, 1..3600"
           " (default 60)\n"
        << "  --minimum-market-messages-per-key N"
           " required for every configured key, 0..100000 (default 1)\n"
        << "  --capture-date YYYYMMDD   Raw capture date"
           " (default local calendar date)\n"
        << "  --ring-bytes N            Phase-2 ByteRing bytes, 32 MiB..4 GiB"
           " (default 64 MiB)\n"
        << "  --maximum-raw-bytes N     fail-stop framed-byte threshold,"
           " 16 MiB..16 GiB (default 1 GiB); at most one record overshoot\n"
        << "  --help                    show this help\n\n"
        << "The output directory must be absolute and must not exist. The probe"
           " writes\n"
        << "segment-00000001.raw and durable.journal, then validates both with"
           " the\n"
        << "Phase-2 reader and recovery analyzer. It uses a fixed non-secret"
           " local\n"
        << "client label and never accepts a token on the command line. This is"
           " an\n"
        << "isolated live verification tool, not a production Raw namespace"
           " cutover.\n";
}

void AppendError(std::string* destination, std::string_view message) {
    if (message.empty()) {
        return;
    }
    if (!destination->empty()) {
        destination->append("; ");
    }
    destination->append(message);
}

void AppendErrorNoThrow(
    std::string* destination,
    std::string_view message) noexcept {
    try {
        AppendError(destination, message);
    } catch (...) {
        // Diagnostics must never prevent SDK quiescence or worker joining.
    }
}

void AppendExceptionNoThrow(
    std::string* destination,
    std::string_view prefix,
    const std::exception& exception) noexcept {
    try {
        std::string message(prefix);
        message.append(exception.what());
        AppendError(destination, message);
    } catch (...) {
        AppendErrorNoThrow(destination, prefix);
    }
}

std::string SafeDiagnostic(std::string message) {
    constexpr std::size_t kMaximumBytes = 4096U;
    if (message.size() > kMaximumBytes) {
        message.resize(kMaximumBytes);
        message.append("...");
    }
    for (char& character : message) {
        const unsigned char value =
            static_cast<unsigned char>(character);
        if (value < 0x20U || value == 0x7fU) {
            character = ' ';
        }
    }
    return message;
}

bool TakeValue(int argc,
               char* argv[],
               int* index,
               std::string_view option,
               std::string_view* value,
               std::string* error) {
    if (*index + 1 >= argc) {
        *error = std::string(option) + " requires a value";
        return false;
    }
    ++*index;
    *value = argv[*index];
    if (value->empty()) {
        *error = std::string(option) + " does not accept an empty value";
        return false;
    }
    return true;
}

template <typename Integer>
bool ParseUnsigned(std::string_view value, Integer* output) noexcept {
    static_assert(std::is_unsigned_v<Integer>);
    Integer parsed = 0;
    const std::from_chars_result result =
        std::from_chars(
            value.data(), value.data() + value.size(), parsed, 10);
    if (result.ec != std::errc{} ||
        result.ptr != value.data() + value.size()) {
        return false;
    }
    *output = parsed;
    return true;
}

bool IsGregorianDate(std::uint32_t value) noexcept {
    if (value < 10000101U || value > 99991231U) {
        return false;
    }
    const std::uint32_t year = value / 10000U;
    const std::uint32_t month = (value / 100U) % 100U;
    const std::uint32_t day = value % 100U;
    if (month == 0U || month > 12U || day == 0U) {
        return false;
    }
    static constexpr std::array<std::uint32_t, 12U> kDays{{
        31U, 28U, 31U, 30U, 31U, 30U,
        31U, 31U, 30U, 31U, 30U, 31U,
    }};
    std::uint32_t maximum = kDays[month - 1U];
    const bool leap =
        year % 4U == 0U &&
        (year % 100U != 0U || year % 400U == 0U);
    if (month == 2U && leap) {
        maximum = 29U;
    }
    return day <= maximum;
}

bool LocalCaptureDate(std::uint32_t* output, std::string* error) {
    struct timespec now {};
    if (::clock_gettime(CLOCK_REALTIME, &now) != 0) {
        *error = "cannot read CLOCK_REALTIME for the default capture date";
        return false;
    }
    const std::time_t seconds = static_cast<std::time_t>(now.tv_sec);
    struct tm local {};
    if (::localtime_r(&seconds, &local) == nullptr) {
        *error = "cannot convert the default capture date";
        return false;
    }
    const int year = local.tm_year + 1900;
    const int month = local.tm_mon + 1;
    const int day = local.tm_mday;
    if (year < 1000 || year > 9999 || month < 1 || month > 12 ||
        day < 1 || day > 31) {
        *error = "local calendar date is outside the Raw V1 range";
        return false;
    }
    *output = static_cast<std::uint32_t>(
        year * 10000 + month * 100 + day);
    return true;
}

bool IsSafeJsonAtom(std::string_view value) noexcept {
    if (value.empty() || value.find('\0') != std::string_view::npos) {
        return false;
    }
    return std::none_of(
        value.begin(), value.end(), [](unsigned char character) noexcept {
            return character < 0x20U || character == 0x7fU ||
                   character == static_cast<unsigned char>('"') ||
                   character == static_cast<unsigned char>('\\');
        });
}

bool IsNormalizedAbsoluteNonRoot(
    const std::filesystem::path& path) noexcept {
    try {
        return path.is_absolute() && path != path.root_path() &&
               path.lexically_normal() == path;
    } catch (...) {
        return false;
    }
}

bool ParseOptions(int argc,
                  char* argv[],
                  Options* options,
                  bool* show_help,
                  std::string* error) {
    std::set<std::string_view> seen;
    for (int index = 1; index < argc; ++index) {
        const std::string_view option(argv[index]);
        if (option == "--help") {
            if (argc != 2) {
                *error = "--help must be used without other options";
                return false;
            }
            *show_help = true;
            return true;
        }
        if (option != "--library" &&
            option != "--output-dir" &&
            option != "--ingress-kind" &&
            option != "--address" &&
            option != "--sdk-log-prefix" &&
            option != "--logon-timeout-seconds" &&
            option != "--monitor-seconds" &&
            option != "--minimum-market-messages-per-key" &&
            option != "--capture-date" &&
            option != "--ring-bytes" &&
            option != "--maximum-raw-bytes") {
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
        if (option == "--library") {
            options->library = std::string(value);
        } else if (option == "--output-dir") {
            options->output_directory = std::string(value);
        } else if (option == "--ingress-kind") {
            if (!sdk::ParseIngressKind(value, &options->ingress_kind)) {
                *error = "--ingress-kind is unknown";
                return false;
            }
            options->ingress_kind_set = true;
        } else if (option == "--address") {
            options->address = value;
        } else if (option == "--sdk-log-prefix") {
            options->sdk_log_prefix = value;
        } else if (option == "--logon-timeout-seconds") {
            if (!ParseUnsigned(value, &options->logon_timeout_seconds) ||
                options->logon_timeout_seconds == 0U ||
                options->logon_timeout_seconds > 60U) {
                *error = "--logon-timeout-seconds must be from 1 through 60";
                return false;
            }
        } else if (option == "--monitor-seconds") {
            if (!ParseUnsigned(value, &options->monitor_seconds) ||
                options->monitor_seconds == 0U ||
                options->monitor_seconds > 3600U) {
                *error = "--monitor-seconds must be from 1 through 3600";
                return false;
            }
        } else if (option == "--minimum-market-messages-per-key") {
            if (!ParseUnsigned(
                    value,
                    &options->minimum_market_messages_per_key) ||
                options->minimum_market_messages_per_key > 100000U) {
                *error =
                    "--minimum-market-messages-per-key must be at most 100000";
                return false;
            }
        } else if (option == "--capture-date") {
            if (!ParseUnsigned(value, &options->capture_date) ||
                !IsGregorianDate(options->capture_date)) {
                *error = "--capture-date must be a valid YYYYMMDD date";
                return false;
            }
        } else if (option == "--ring-bytes") {
            if (!ParseUnsigned(value, &options->ring_bytes) ||
                options->ring_bytes < 32ULL * kMebibyte ||
                options->ring_bytes > 4ULL * kGibibyte) {
                *error = "--ring-bytes must be from 32 MiB through 4 GiB";
                return false;
            }
        } else if (option == "--maximum-raw-bytes") {
            if (!ParseUnsigned(value, &options->maximum_raw_bytes) ||
                options->maximum_raw_bytes < 16ULL * kMebibyte ||
                options->maximum_raw_bytes > 16ULL * kGibibyte) {
                *error =
                    "--maximum-raw-bytes must be from 16 MiB through 16 GiB";
                return false;
            }
        }
    }

    if (options->library.empty() ||
        !IsNormalizedAbsoluteNonRoot(options->library)) {
        *error = "--library must be an absolute normalized non-root path";
        return false;
    }
    if (options->output_directory.empty() ||
        !IsNormalizedAbsoluteNonRoot(options->output_directory)) {
        *error = "--output-dir must be an absolute normalized non-root path";
        return false;
    }
    if (!options->ingress_kind_set) {
        *error = "--ingress-kind is required";
        return false;
    }
    if (!IsSafeJsonAtom(options->address)) {
        *error = "--address contains unsupported JSON/control bytes";
        return false;
    }
    if (options->sdk_log_prefix.empty() ||
        options->sdk_log_prefix.find('\0') != std::string::npos) {
        *error = "--sdk-log-prefix is empty or contains NUL";
        return false;
    }
    if (options->capture_date == 0U &&
        !LocalCaptureDate(&options->capture_date, error)) {
        return false;
    }
    return true;
}

std::string_view ShortIngressKind(sdk::IngressKind kind) {
    switch (kind) {
    case sdk::IngressKind::ShSnapshot:
        return "sh-snapshot";
    case sdk::IngressKind::ShTick:
        return "sh-tick";
    case sdk::IngressKind::SzSnapshot:
        return "sz-snapshot";
    case sdk::IngressKind::SzTick:
        return "sz-tick";
    }
    throw std::invalid_argument("unknown ingress kind");
}

std::string EndpointContractBytes(const Options& options) {
    std::string result =
        "{\"schema_version\":1,\"ingress_kind\":\"";
    result.append(ShortIngressKind(options.ingress_kind));
    result.append(
        "\",\"name\":\"phase2-live-probe\","
        "\"resolved_server_address\":\"");
    result.append(options.address);
    result.append(
        "\",\"message_encoding\":1,\"merge_message\":false,"
        "\"send_mac_auth\":false,\"server_select\":false}");
    return result;
}

struct RequiredKeyStatus final {
    sdk::MessageKey key{};
    bool ok = false;
    bool failed = false;
    std::uint64_t market_messages = 0U;
};

struct LiveTrackerSnapshot final {
    bool logon_received = false;
    bool malformed = false;
    std::uint32_t return_code = 0U;
    std::uint64_t accepted_callback_records = 0U;
    std::vector<RequiredKeyStatus> required;

    [[nodiscard]] bool subscriptions_ok() const noexcept {
        return logon_received && !malformed &&
               return_code == mdl::MDLEC_OK &&
               std::all_of(
                   required.begin(), required.end(),
                   [](const RequiredKeyStatus& status) noexcept {
                       return status.ok && !status.failed;
                   });
    }

    [[nodiscard]] bool minimum_met(
        std::uint64_t minimum) const noexcept {
        return std::all_of(
            required.begin(), required.end(),
            [minimum](const RequiredKeyStatus& status) noexcept {
                return status.market_messages >= minimum;
            });
    }
};

class LiveTracker final {
public:
    explicit LiveTracker(std::vector<sdk::MessageKey> required) {
        statuses_.reserve(required.size());
        for (const sdk::MessageKey& key : required) {
            statuses_.push_back(RequiredKeyStatus{key});
        }
    }

    void Observe(const mdl::MDLMessage* message) noexcept {
        try {
            ObserveImpl(message);
        } catch (...) {
            std::lock_guard<std::mutex> lock(mutex_);
            malformed_ = true;
            condition_.notify_all();
        }
    }

    LiveTrackerSnapshot WaitForLogon(
        std::chrono::seconds timeout,
        const l2flow::ops::FatalLatch& fatal) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        std::unique_lock<std::mutex> lock(mutex_);
        while (!logon_received_ && !malformed_ && !fatal.tripped() &&
               g_stop_requested == 0) {
            if (condition_.wait_until(lock, deadline) ==
                std::cv_status::timeout) {
                break;
            }
        }
        return SnapshotLocked();
    }

    LiveTrackerSnapshot Monitor(
        std::chrono::seconds duration,
        const l2flow::ops::FatalLatch& fatal,
        const ingress::CaptureMetrics& metrics,
        std::uint64_t maximum_raw_bytes,
        bool* raw_limit_exceeded) {
        const auto deadline = std::chrono::steady_clock::now() + duration;
        std::unique_lock<std::mutex> lock(mutex_);
        for (;;) {
            const bool limit =
                metrics.Snapshot().captured_framed_wal_bytes >
                maximum_raw_bytes;
            if (limit || malformed_ || fatal.tripped() ||
                g_stop_requested != 0 ||
                std::chrono::steady_clock::now() >= deadline) {
                *raw_limit_exceeded = limit;
                break;
            }
            static constexpr auto kPoll =
                std::chrono::milliseconds(100);
            const auto next = std::min(
                deadline, std::chrono::steady_clock::now() + kPoll);
            static_cast<void>(condition_.wait_until(lock, next));
        }
        return SnapshotLocked();
    }

    [[nodiscard]] LiveTrackerSnapshot Snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return SnapshotLocked();
    }

private:
    void ObserveImpl(const mdl::MDLMessage* message) {
        if (message == nullptr || message->GetHead() == nullptr) {
            throw std::runtime_error("SDK delivered a null message/head");
        }
        const mdl::MDLMessageHead* const head = message->GetHead();
        if (head->MessageSize < head->HeadSize) {
            throw std::runtime_error("SDK delivered an invalid message size");
        }

        std::lock_guard<std::mutex> lock(mutex_);
        ++accepted_callback_records_;
        for (RequiredKeyStatus& status : statuses_) {
            if (head->ServiceID == status.key.service_id &&
                head->ServiceVersion == status.key.service_version &&
                head->MessageID == status.key.message_id) {
                ++status.market_messages;
            }
        }

        if (head->ServiceID != mdl::MDLSID_MDL_SYS ||
            head->MessageID !=
                mdl::mdl_sys_msg::LogonResponse::MessageID) {
            condition_.notify_all();
            return;
        }
        if (head->MessageSize - head->HeadSize <
            sizeof(mdl::mdl_sys_msg::LogonResponse)) {
            throw std::runtime_error("truncated LogonResponse");
        }
        const char* const body = message->GetBody();
        if (body == nullptr) {
            throw std::runtime_error("null LogonResponse body");
        }
        const auto* const response =
            reinterpret_cast<const mdl::mdl_sys_msg::LogonResponse*>(body);
        constexpr std::uint32_t kMaximumServices = 256U;
        constexpr std::uint32_t kMaximumMessages = 4096U;
        if (response->Services.Length > kMaximumServices) {
            throw std::runtime_error("LogonResponse service list is too large");
        }

        return_code_ = response->ReturnCode;
        for (std::uint32_t service_index = 0U;
             service_index < response->Services.Length;
             ++service_index) {
            const auto* const service =
                response->Services[service_index];
            if (service == nullptr ||
                service->Messages.Length > kMaximumMessages) {
                malformed_ = true;
                continue;
            }
            for (std::uint32_t message_index = 0U;
                 message_index < service->Messages.Length;
                 ++message_index) {
                const auto* const result =
                    service->Messages[message_index];
                if (result == nullptr) {
                    malformed_ = true;
                    continue;
                }
                for (RequiredKeyStatus& status : statuses_) {
                    if (service->ServiceID == status.key.service_id &&
                        service->ServiceVersion ==
                            status.key.service_version &&
                        result->MessageID == status.key.message_id) {
                        if (result->MessageStatus == mdl::MDLEC_OK) {
                            status.ok = true;
                        } else {
                            status.failed = true;
                        }
                    }
                }
            }
        }
        logon_received_ = true;
        condition_.notify_all();
    }

    [[nodiscard]] LiveTrackerSnapshot SnapshotLocked() const {
        LiveTrackerSnapshot result;
        result.logon_received = logon_received_;
        result.malformed = malformed_;
        result.return_code = return_code_;
        result.accepted_callback_records = accepted_callback_records_;
        result.required = statuses_;
        return result;
    }

    mutable std::mutex mutex_;
    std::condition_variable condition_;
    std::vector<RequiredKeyStatus> statuses_;
    bool logon_received_ = false;
    bool malformed_ = false;
    std::uint32_t return_code_ = 0U;
    std::uint64_t accepted_callback_records_ = 0U;
};

class RawCallbackRouter final : public mdl::MessageHandlerBase {
public:
    RawCallbackRouter(
        ingress::CallbackHandler& handler,
        LiveTracker& tracker,
        const ingress::CaptureMetrics& metrics,
        std::uint64_t maximum_raw_bytes) noexcept
        : handler_(handler),
          tracker_(tracker),
          metrics_(metrics),
          maximum_raw_bytes_(maximum_raw_bytes) {}

    void OnMessage(
        mdl::Subscriber*,
        const mdl::MDLMessage* message) noexcept override {
        const std::uint64_t before = handler_.captured_sequence();
        handler_.OnMDLAPIMessage(message);
        const std::uint64_t after = handler_.captured_sequence();
        if (after == before + 1U) {
            tracker_.Observe(message);
            if (metrics_.Snapshot().captured_framed_wal_bytes >
                maximum_raw_bytes_) {
                // Bound threshold overshoot to the one record that crossed
                // it. Later callbacks are rejected while the control thread
                // notices the threshold and shuts down the SDK.
                handler_.BeginStopping();
            }
        }
    }

private:
    ingress::CallbackHandler& handler_;
    LiveTracker& tracker_;
    const ingress::CaptureMetrics& metrics_;
    std::uint64_t maximum_raw_bytes_;
};

void OnRawCaptureFailure(
    void* context,
    ingress::RawCaptureFatalSignal signal) noexcept {
    auto* const fatal = static_cast<l2flow::ops::FatalLatch*>(context);
    const l2flow::ops::FatalReason reason =
        signal == ingress::RawCaptureFatalSignal::kRingCorruption
            ? l2flow::ops::FatalReason::RING_CORRUPTION
            : l2flow::ops::FatalReason::RAW_WAL_IO;
    static_cast<void>(fatal->trip(reason));
}

bool CreateOutputDirectory(
    const std::filesystem::path& path,
    FileDescriptor* directory,
    std::string* error) {
    if (::mkdir(path.c_str(), 0700) != 0) {
        *error = errno == EEXIST
                     ? "output directory already exists"
                     : std::string("cannot create output directory: ") +
                           std::strerror(errno);
        return false;
    }
    const int raw_directory = ::open(
        path.c_str(),
        O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW | O_NOCTTY);
    if (raw_directory < 0) {
        *error = std::string("cannot open output directory: ") +
                 std::strerror(errno);
        return false;
    }
    directory->Reset(raw_directory);

    struct stat status {};
    if (::fstat(directory->get(), &status) != 0 ||
        !S_ISDIR(status.st_mode) || status.st_uid != ::geteuid() ||
        (status.st_mode & 0777U) != 0700U) {
        *error = "output directory is not an owner-only retained directory";
        return false;
    }
    return true;
}

bool CreateOutputFile(int directory_fd,
                      const char* name,
                      FileDescriptor* output,
                      std::string* error) {
    int raw_fd = -1;
    do {
        raw_fd = ::openat(
            directory_fd,
            name,
            O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW | O_NOCTTY,
            0600);
    } while (raw_fd < 0 && errno == EINTR);
    if (raw_fd < 0) {
        *error = std::string("cannot create ") + name + ": " +
                 std::strerror(errno);
        return false;
    }
    output->Reset(raw_fd);
    if (::fchmod(output->get(), 0600) != 0) {
        *error = std::string("cannot set owner-only mode on ") + name +
                 ": " + std::strerror(errno);
        return false;
    }
    return true;
}

bool ReadBoundedFileAt(
    int directory_fd,
    const char* name,
    std::uint64_t maximum_size,
    std::shared_ptr<const std::vector<std::byte>>* output,
    std::string* error) {
    int raw_fd = -1;
    do {
        raw_fd = ::openat(
            directory_fd,
            name,
            O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK | O_NOCTTY);
    } while (raw_fd < 0 && errno == EINTR);
    if (raw_fd < 0) {
        *error = std::string("cannot open ") + name + " for validation: " +
                 std::strerror(errno);
        return false;
    }
    FileDescriptor fd(raw_fd);

    struct stat before {};
    if (::fstat(fd.get(), &before) != 0 || !S_ISREG(before.st_mode) ||
        before.st_size < 0) {
        *error = std::string("cannot inspect ") + name +
                 " for validation";
        return false;
    }
    const std::uint64_t size = static_cast<std::uint64_t>(before.st_size);
    if (size > maximum_size ||
        size > static_cast<std::uint64_t>(
                   std::numeric_limits<std::size_t>::max())) {
        *error = std::string(name) + " exceeds the validation size bound";
        return false;
    }

    auto bytes = std::make_shared<std::vector<std::byte>>(
        static_cast<std::size_t>(size));
    std::size_t offset = 0U;
    while (offset < bytes->size()) {
        const ssize_t count = ::pread(
            fd.get(),
            bytes->data() + offset,
            bytes->size() - offset,
            static_cast<off_t>(offset));
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            *error = std::string("cannot read complete ") + name;
            return false;
        }
        offset += static_cast<std::size_t>(count);
    }
    struct stat after {};
    if (::fstat(fd.get(), &after) != 0 ||
        after.st_dev != before.st_dev || after.st_ino != before.st_ino ||
        after.st_size != before.st_size) {
        *error = std::string(name) + " changed while being validated";
        return false;
    }
    *output = std::move(bytes);
    return true;
}

bool ParseRequiredDigest(
    std::string_view value,
    common::Sha256Digest* output,
    std::string_view label,
    std::string* error) {
    std::string parse_error;
    if (!common::ParseSha256Hex(value, output, &parse_error)) {
        *error = std::string("cannot parse ") + std::string(label) +
                 " SHA-256: " + parse_error;
        return false;
    }
    return true;
}

bool LoadClockIdentity(
    ingress::RawV1Identity* host_uuid,
    ingress::RawV1Identity* linux_boot_id,
    ingress::ClockEpoch* epoch,
    std::string* error) {
    ingress::ClockEpochInputs inputs;
    if (!ingress::ReadClockEpochInputs(
            "/etc/machine-id",
            "/proc/sys/kernel/random/boot_id",
            "CLOCK_MONOTONIC_RAW+CLOCK_REALTIME",
            &inputs,
            error)) {
        return false;
    }
    if (!common::ParseIdentity128Hex(inputs.host_uuid, host_uuid) ||
        !common::ParseCanonicalUuid128(
            inputs.linux_boot_id, linux_boot_id)) {
        *error = "host or boot identity is not canonical";
        return false;
    }
    try {
        *epoch = ingress::ComputeClockEpoch(inputs);
    } catch (const std::exception& exception) {
        *error = std::string("cannot compute clock epoch: ") +
                 exception.what();
        return false;
    }
    return true;
}

bool BuildRawConfiguration(
    const Options& options,
    std::string_view endpoint_contract_sha256,
    ingress::RawIngressConfig* config,
    std::string* error) {
    ingress::RawIngressConfig value =
        ingress::DefaultRawIngressConfig(options.ingress_kind);
    value.endpoint_contract_sha256 = endpoint_contract_sha256;
    value.credential_name = "local-feeder-label";
    value.sdk_log_prefix = options.sdk_log_prefix;
    value.metrics_textfile_path =
        (options.output_directory / "metrics.prom").string();
    value.max_message_bytes = kMaximumMessageBytes;
    value.ring_capacity_bytes = options.ring_bytes;
    value.raw_root = options.output_directory.string();
    value.reserve_domain_id = "phase2-live-probe";
    value.reserve_coordinator_socket =
        "/tmp/l2flow-phase2-live-probe.sock";
    value.canonical_clock_source_config =
        "CLOCK_MONOTONIC_RAW+CLOCK_REALTIME";
    const std::string validation = ingress::ValidateRawIngressConfig(value);
    if (!validation.empty()) {
        *error = "invalid Phase-2 live configuration: " + validation;
        return false;
    }
    *config = std::move(value);
    return true;
}

bool BuildWriterConfig(
    const Options& options,
    const ingress::RawIngressConfig& raw_config,
    std::string_view endpoint_contract_sha256,
    ingress::LinuxCaptureClock* clock,
    ingress::RawWalWriterConfig* writer_config,
    ingress::SegmentHeaderV1* segment_header,
    std::string* error) {
    ingress::SegmentHeaderV1 segment;
    const sdk::IngressSpec& spec =
        sdk::GetIngressSpec(options.ingress_kind);
    segment.source_stream_id = spec.source_stream_id;
    segment.capture_date = options.capture_date;
    if (!common::GenerateIdentity128(&segment.stream_day_id, nullptr)) {
        *error = "cannot generate the Raw stream-day identity";
        return false;
    }
    segment.segment_sequence = 1U;
    segment.segment_base_wal_pos = 0U;
    segment.first_ingress_sequence = 1U;
    try {
        segment.created_realtime_ns = clock->RealtimeNanoseconds();
        segment.created_monotonic_ns = clock->MonotonicRawNanoseconds();
    } catch (const std::exception& exception) {
        *error = std::string("cannot read Raw creation clocks: ") +
                 exception.what();
        return false;
    }

    ingress::ClockEpoch epoch;
    if (!LoadClockIdentity(
            &segment.host_uuid,
            &segment.linux_boot_id,
            &epoch,
            error)) {
        return false;
    }
    segment.clock_epoch_algorithm = 1U;
    segment.clock_epoch_digest = epoch.digest;
    segment.clock_epoch_label = epoch.value;

    if (!ParseRequiredDigest(
            baseline::ApprovedVendorBaseline().sdk_archive_sha256,
            &segment.sdk_archive_sha256,
            "SDK archive",
            error)) {
        return false;
    }
    if (!common::ComputeFileSha256(
            options.library,
            &segment.libmdl_api_sha256,
            error,
            baseline::kMaximumSdkSharedLibraryBytes)) {
        return false;
    }
    if (!ParseRequiredDigest(
            endpoint_contract_sha256,
            &segment.endpoint_contract_sha256,
            "endpoint contract",
            error)) {
        return false;
    }
    try {
        if (!ParseRequiredDigest(
                ingress::RawIngressConfigSha256(raw_config),
                &segment.config_sha256,
                "Raw config",
                error)) {
            return false;
        }
    } catch (const std::exception& exception) {
        *error = std::string("cannot hash the Raw config: ") +
                 exception.what();
        return false;
    }
    segment.raw_schema_sha256 = ingress::RawSchemaSha256Digest();
    if (!ParseRequiredDigest(
            l2flow::build_manifest::kSha256,
            &segment.build_manifest_sha256,
            "build manifest",
            error)) {
        return false;
    }

    ingress::DurableJournalHeaderV1 journal;
    journal.capture_date = segment.capture_date;
    journal.source_stream_id = segment.source_stream_id;
    journal.stream_day_id = segment.stream_day_id;
    journal.raw_schema_sha256 = segment.raw_schema_sha256;
    journal.created_host_uuid = segment.host_uuid;
    journal.created_linux_boot_id = segment.linux_boot_id;
    journal.created_clock_epoch_algorithm = segment.clock_epoch_algorithm;
    journal.created_clock_epoch_digest = segment.clock_epoch_digest;
    journal.created_clock_epoch_label = segment.clock_epoch_label;

    ingress::RawWalWriterConfig result;
    if (ingress::EncodeSegmentHeaderV1(
            segment, &result.segment_header_wire) !=
            ingress::RawV1Error::kNone ||
        ingress::EncodeDurableJournalHeaderV1(
            journal, &result.journal_header_wire) !=
            ingress::RawV1Error::kNone) {
        *error = "cannot encode the Phase-2 Raw headers";
        return false;
    }
    result.source_stream_id = segment.source_stream_id;
    result.capture_date = segment.capture_date;
    result.segment_sequence = segment.segment_sequence;
    result.segment_base_wal_pos = segment.segment_base_wal_pos;
    result.first_ingress_sequence = segment.first_ingress_sequence;
    result.initial_durable_ingress_sequence = 0U;
    if (!common::GenerateIdentity128(&result.writer_instance, nullptr)) {
        *error = "cannot generate the Raw writer identity";
        return false;
    }
    *writer_config = std::move(result);
    *segment_header = segment;
    return true;
}

struct SdkStopResult final {
    bool clean = false;
    bool safe_to_unwind = false;
};

SdkStopResult StopAndReleaseSdk(
    std::unique_ptr<sdk::SdkManager>* manager,
    std::unique_ptr<sdk::SdkSubscriber>* subscriber,
    ingress::CallbackHandler* handler,
    std::string* error) noexcept {
    handler->BeginStopping();
    bool shutdown_complete = false;
    if (*manager != nullptr) {
        try {
            (*manager)->Shutdown();
            shutdown_complete = true;
        } catch (const std::exception& exception) {
            AppendExceptionNoThrow(
                error,
                "IOManager Shutdown threw: ",
                exception);
        } catch (...) {
            AppendErrorNoThrow(error, "IOManager Shutdown threw");
        }
    } else {
        shutdown_complete = true;
    }

    const bool callback_quiesced =
        handler->Quiesce(std::chrono::seconds(5));
    if (!callback_quiesced) {
        AppendErrorNoThrow(error, "Raw callback handler did not quiesce");
    }
    if (!shutdown_complete || !callback_quiesced) {
        // The adapter forbids release before Shutdown. Retain both wrappers,
        // and tell the caller not to unwind callback-owned state while the
        // vendor could still enter it.
        static_cast<void>(subscriber->release());
        static_cast<void>(manager->release());
        return {false, false};
    }

    bool success = true;
    if (*subscriber != nullptr) {
        std::string release_error;
        if (!(*subscriber)->Release(&release_error)) {
            AppendErrorNoThrow(error, release_error);
            success = false;
        }
        subscriber->reset();
    }
    if (*manager != nullptr) {
        std::string release_error;
        if (!(*manager)->Release(&release_error)) {
            AppendErrorNoThrow(error, release_error);
            success = false;
        }
        manager->reset();
    }
    return {success, true};
}

class RawWorkerThreadSession final {
public:
    explicit RawWorkerThreadSession(
        ingress::RawCaptureWorker& worker)
        : worker_(worker),
          thread_([this]() noexcept {
              result_.store(
                  worker_.Run(), std::memory_order_release);
          }) {}

    ~RawWorkerThreadSession() noexcept {
        StopAndJoin();
    }

    RawWorkerThreadSession(const RawWorkerThreadSession&) = delete;
    RawWorkerThreadSession& operator=(
        const RawWorkerThreadSession&) = delete;

    void StopAndJoin() noexcept {
        worker_.StopAndDrain();
        if (!thread_.joinable()) {
            return;
        }
        try {
            thread_.join();
        } catch (...) {
            // The worker owns references to stack state. Continuing to unwind
            // after a failed join cannot be made memory-safe.
            std::terminate();
        }
    }

    [[nodiscard]] bool result() const noexcept {
        return result_.load(std::memory_order_acquire);
    }

private:
    ingress::RawCaptureWorker& worker_;
    std::atomic<bool> result_{false};
    std::thread thread_;
};

struct ValidationResult final {
    ingress::RawSegmentScanResult scan{};
    ingress::RawRecoveryPlanV1 recovery{};
    std::vector<std::uint64_t> raw_required_counts;
    std::uint64_t segment_bytes = 0U;
    std::uint64_t journal_bytes = 0U;
};

bool ValidateRawArtifacts(
    int output_directory_fd,
    const Options& options,
    const sdk::IngressSpec& spec,
    const ingress::CaptureMetricsSnapshot& callback,
    const ingress::RawCaptureWorkerSnapshot& capture,
    const ingress::RawWalWriterSnapshot& wal,
    const LiveTrackerSnapshot& tracker,
    ValidationResult* validation,
    std::string* error) {
    const std::uint64_t maximum_segment =
        options.maximum_raw_bytes + ingress::kRawV1SegmentHeaderBytes +
        kMaximumFramedRecordBytes;
    std::shared_ptr<const std::vector<std::byte>> segment;
    std::shared_ptr<const std::vector<std::byte>> journal;
    if (!ReadBoundedFileAt(
            output_directory_fd,
            "segment-00000001.raw",
            maximum_segment,
            &segment,
            error) ||
        !ReadBoundedFileAt(
            output_directory_fd,
            "durable.journal",
            16U * kMebibyte,
            &journal,
            error)) {
        return false;
    }

    validation->segment_bytes = segment->size();
    validation->journal_bytes = journal->size();
    validation->scan = ingress::ScanRawSegmentV1(
        segment, static_cast<std::uint64_t>(segment->size()));
    if (!validation->scan.ok()) {
        *error = "Raw validating reader rejected the sealed segment at " +
                 std::to_string(validation->scan.error_offset) +
                 " with reader error " +
                 std::to_string(
                     static_cast<unsigned int>(validation->scan.error));
        return false;
    }
    if (validation->scan.records.size() != callback.captured_records ||
        tracker.accepted_callback_records != callback.captured_records ||
        capture.append.records != callback.captured_records ||
        capture.durable.records != callback.captured_records) {
        *error = "callback, tracker, Raw reader, append, and durable record "
                 "counts do not reconcile";
        return false;
    }

    validation->raw_required_counts.assign(spec.required.size(), 0U);
    for (std::size_t index = 0U;
         index < validation->scan.records.size();
         ++index) {
        const ingress::RawRecordHeaderV1& record =
            validation->scan.records[index].header();
        if (record.source_stream_id != spec.source_stream_id ||
            record.capture_date != options.capture_date ||
            record.ingress_sequence != index + 1U) {
            *error = "Raw record namespace or ingress sequence is inconsistent";
            return false;
        }
        for (std::size_t key_index = 0U;
             key_index < spec.required.size();
             ++key_index) {
            const sdk::MessageKey& key = spec.required[key_index];
            if (record.vendor_service_id == key.service_id &&
                record.vendor_service_version == key.service_version &&
                record.vendor_message_id == key.message_id) {
                ++validation->raw_required_counts[key_index];
            }
        }
    }
    for (std::size_t index = 0U; index < spec.required.size(); ++index) {
        if (validation->raw_required_counts[index] !=
            tracker.required[index].market_messages) {
            *error = "live callback and Raw reader market-message counts differ";
            return false;
        }
    }

    ingress::RawRecoveryInputV1 recovery_input;
    recovery_input.journal = journal;
    recovery_input.segments.push_back({segment});
    validation->recovery =
        ingress::AnalyzeRawRecoveryV1(recovery_input);
    if (!validation->recovery.ok() ||
        !validation->recovery.has_accepted_cursor ||
        validation->recovery.accepted_journal_size != journal->size() ||
        validation->recovery.accepted_cursor.segment_sequence != 1U ||
        validation->recovery.accepted_cursor.global_wal_pos !=
            wal.durable.global_wal_pos ||
        validation->recovery.accepted_cursor.ingress_sequence !=
            wal.durable.ingress_sequence ||
        validation->recovery.accepted_cursor.segment_offset !=
            wal.durable.segment_offset ||
        (validation->recovery.accepted_cursor.marker_flags &
         ingress::kRawWalSegmentSealedFlag) == 0U ||
        validation->recovery.segments.size() != 1U ||
        !validation->recovery.segments.front().sealed ||
        validation->recovery.segments.front().tail !=
            ingress::RawRecoverySegmentTailV1::kNone) {
        *error = "Raw recovery analyzer did not accept the exact sealed "
                 "journal/segment frontier";
        return false;
    }
    return true;
}

void WriteJsonString(std::ostream& output, std::string_view value) {
    output.put('"');
    for (const unsigned char character : value) {
        switch (character) {
        case '"':
            output << "\\\"";
            break;
        case '\\':
            output << "\\\\";
            break;
        case '\n':
            output << "\\n";
            break;
        case '\r':
            output << "\\r";
            break;
        case '\t':
            output << "\\t";
            break;
        default:
            if (character < 0x20U) {
                static constexpr char kHex[] = "0123456789abcdef";
                output << "\\u00"
                       << kHex[(character >> 4U) & 0x0fU]
                       << kHex[character & 0x0fU];
            } else {
                output.put(static_cast<char>(character));
            }
            break;
        }
    }
    output.put('"');
}

bool WaitForWorkerStartup(
    const ingress::RawCaptureWorker& worker,
    std::chrono::seconds timeout) noexcept {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!worker.startup_complete() &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    return worker.startup_complete() && worker.startup_succeeded();
}

bool InvalidMetrics(
    const ingress::CaptureMetricsSnapshot& snapshot) noexcept {
    return snapshot.callback_reentry != 0U ||
           snapshot.callback_exceptions != 0U ||
           snapshot.callbacks_after_fatal != 0U ||
           snapshot.ring_overflow != 0U ||
           snapshot.callback_inflight ||
           std::any_of(
               snapshot.invalid_messages.begin(),
               snapshot.invalid_messages.end(),
               [](std::uint64_t value) noexcept { return value != 0U; });
}

void PrintSuccessJson(
    const Options& options,
    const sdk::IngressSpec& spec,
    const LiveTrackerSnapshot& tracker,
    const ingress::CaptureMetricsSnapshot& callback,
    const ingress::RawCaptureWorkerSnapshot& capture,
    const ingress::RawWalWriterSnapshot& wal,
    const ingress::RawCaptureReconciliation& reconciliation,
    const ValidationResult& validation) {
    std::cout << "{\"passed\":true,\"ingress_kind\":";
    WriteJsonString(std::cout, ShortIngressKind(options.ingress_kind));
    std::cout << ",\"source_stream_id\":" << spec.source_stream_id
              << ",\"capture_date\":" << options.capture_date
              << ",\"output_directory\":";
    WriteJsonString(std::cout, options.output_directory.string());
    std::cout
        << ",\"logon_return_code\":" << tracker.return_code
        << ",\"required_subscriptions\":[";
    for (std::size_t index = 0U;
         index < tracker.required.size();
         ++index) {
        if (index != 0U) {
            std::cout << ',';
        }
        const RequiredKeyStatus& status = tracker.required[index];
        std::cout
            << "{\"service_id\":"
            << static_cast<unsigned int>(status.key.service_id)
            << ",\"service_version\":" << status.key.service_version
            << ",\"message_id\":" << status.key.message_id
            << ",\"status\":\"ok\",\"live_messages\":"
            << status.market_messages
            << ",\"raw_records\":"
            << validation.raw_required_counts[index] << '}';
    }
    std::cout
        << "]"
        << ",\"monitor_seconds\":" << options.monitor_seconds
        << ",\"callback_records\":" << callback.captured_records
        << ",\"callback_vendor_bytes\":"
        << callback.captured_vendor_bytes
        << ",\"callback_framed_raw_bytes\":"
        << callback.captured_framed_wal_bytes
        << ",\"callbacks_after_stop\":"
        << callback.callbacks_after_stop
        << ",\"append_records\":" << capture.append.records
        << ",\"durable_records\":" << capture.durable.records
        << ",\"append_global_wal_pos\":"
        << wal.append.global_wal_pos
        << ",\"durable_global_wal_pos\":"
        << wal.durable.global_wal_pos
        << ",\"segment_bytes\":" << validation.segment_bytes
        << ",\"journal_bytes\":" << validation.journal_bytes
        << ",\"reader_records\":"
        << validation.scan.records.size()
        << ",\"recovery_accepted_journal_bytes\":"
        << validation.recovery.accepted_journal_size
        << ",\"recovery_sealed\":true"
        << ",\"append_reconciliation_exact\":"
        << (reconciliation.append_exact() ? "true" : "false")
        << ",\"durable_reconciliation_exact\":"
        << (reconciliation.durable_exact() ? "true" : "false")
        << "}\n";
}

int Run(const Options& options) {
    const std::string manifest_error = sdk::ValidateIngressSpecs();
    if (!manifest_error.empty()) {
        std::cerr << "mdl-phase2-live-probe: invalid subscription manifest: "
                  << SafeDiagnostic(manifest_error) << '\n';
        return 1;
    }
    const sdk::IngressSpec& spec =
        sdk::GetIngressSpec(options.ingress_kind);
    const std::string endpoint_bytes = EndpointContractBytes(options);
    const std::string endpoint_sha256 = common::Sha256Hex(
        common::ComputeSha256(endpoint_bytes));
    std::string error;
    const auto endpoint = sdk::VerifyEndpointContractBytes(
        endpoint_bytes,
        endpoint_sha256,
        options.ingress_kind,
        &error);
    if (endpoint == nullptr) {
        std::cerr << "mdl-phase2-live-probe: "
                  << SafeDiagnostic(error) << '\n';
        return 1;
    }

    std::shared_ptr<sdk::SdkFactory> factory =
        sdk::LoadApprovedSdkFactory(options.library, &error);
    if (factory == nullptr) {
        std::cerr << "mdl-phase2-live-probe: "
                  << SafeDiagnostic(error) << '\n';
        return 1;
    }

    ingress::RawIngressConfig raw_config;
    if (!BuildRawConfiguration(
            options, endpoint_sha256, &raw_config, &error)) {
        std::cerr << "mdl-phase2-live-probe: "
                  << SafeDiagnostic(error) << '\n';
        return 1;
    }
    ingress::LinuxCaptureClock capture_clock;
    ingress::RawWalWriterConfig writer_config;
    ingress::SegmentHeaderV1 segment_header;
    if (!BuildWriterConfig(
            options,
            raw_config,
            endpoint_sha256,
            &capture_clock,
            &writer_config,
            &segment_header,
            &error)) {
        std::cerr << "mdl-phase2-live-probe: "
                  << SafeDiagnostic(error) << '\n';
        return 1;
    }

    FileDescriptor output_directory;
    if (!CreateOutputDirectory(
            options.output_directory, &output_directory, &error)) {
        std::cerr << "mdl-phase2-live-probe: "
                  << SafeDiagnostic(error) << '\n';
        return 1;
    }
    FileDescriptor segment_fd;
    FileDescriptor journal_fd;
    if (!CreateOutputFile(
            output_directory.get(),
            "segment-00000001.raw",
            &segment_fd,
            &error) ||
        !CreateOutputFile(
            output_directory.get(),
            "durable.journal",
            &journal_fd,
            &error)) {
        std::cerr << "mdl-phase2-live-probe: "
                  << SafeDiagnostic(error) << '\n';
        return 1;
    }
    std::unique_ptr<ingress::RawWalIo> io =
        ingress::AdoptPosixRawWalIo(
            segment_fd.get(), journal_fd.get(), &error);
    if (io == nullptr) {
        std::cerr << "mdl-phase2-live-probe: "
                  << SafeDiagnostic(error) << '\n';
        return 1;
    }
    static_cast<void>(segment_fd.release());
    static_cast<void>(journal_fd.release());

    ingress::RawWalWriter writer(
        std::move(writer_config), std::move(io));
    if (!writer.Initialize()) {
        const ingress::RawWalFailure failure = writer.failure();
        std::cerr
            << "mdl-phase2-live-probe: Raw writer initialization failed: "
            << static_cast<unsigned int>(failure.kind) << '/'
            << failure.error_number << '\n';
        return 1;
    }

    ingress::ByteRing ring(
        static_cast<std::size_t>(options.ring_bytes),
        kMaximumMessageBytes);
    l2flow::ops::FatalLatch fatal;
    ingress::CaptureMetrics metrics;
    ingress::CallbackHandlerConfig handler_config;
    handler_config.source_stream_id = spec.source_stream_id;
    handler_config.market_service_id = spec.market_service_id;
    handler_config.max_message_bytes = kMaximumMessageBytes;
    handler_config.capture_date = options.capture_date;
    handler_config.first_ingress_sequence = 1U;
    ingress::CallbackHandler handler(
        handler_config, ring, capture_clock, fatal, metrics);
    handler.SetConnectionEpochHint(1U);
    LiveTracker tracker(spec.required);
    RawCallbackRouter callback_router(
        handler,
        tracker,
        metrics,
        options.maximum_raw_bytes);

    ingress::RawCaptureWorkerConfig worker_config;
    worker_config.failure_sink = {&OnRawCaptureFailure, &fatal};
    ingress::RawCaptureWorker worker(worker_config, ring, writer);
    RawWorkerThreadSession worker_session(worker);
    if (!WaitForWorkerStartup(worker, std::chrono::seconds(5))) {
        worker_session.StopAndJoin();
        std::cerr
            << "mdl-phase2-live-probe: Raw capture worker failed to start\n";
        return 1;
    }

    std::unique_ptr<sdk::SdkManager> manager;
    std::unique_ptr<sdk::SdkSubscriber> subscriber;
    LiveTrackerSnapshot live_snapshot;
    bool raw_limit_exceeded = false;
    bool connected = false;
    std::string failure;
    try {
        manager = factory->Create(
            raw_config.work_threads, raw_config.io_threads);
        if (manager == nullptr) {
            failure = "SdkFactory::Create returned null";
        } else {
            manager->EnableLog(options.sdk_log_prefix, false);
            subscriber = manager->CreateSubscriber(
                &callback_router, false);
            if (subscriber == nullptr) {
                failure = "SdkManager::CreateSubscriber returned null";
            } else {
                subscriber->SetServerAddress(
                    endpoint->resolved_server_address());
                subscriber->SetUserName(kLocalClientLabel);
                subscriber->SetHeartbeatInterval(
                    raw_config.heartbeat_interval_seconds);
                subscriber->SetHeartbeatTimeout(
                    raw_config.heartbeat_timeout_seconds);
                subscriber->SetMessageEncoding(
                    endpoint->message_encoding());
                subscriber->EnableMergeMessage(
                    endpoint->merge_message());
                subscriber->SetSendMacAuth(
                    endpoint->send_mac_auth());
                subscriber->EnableServerSelect(
                    endpoint->server_select());
                for (const sdk::MessageKey& key : spec.required) {
                    subscriber->AddSubscription(key);
                }
                const std::string connect_error = subscriber->Connect();
                if (!connect_error.empty()) {
                    failure = "SDK Connect failed: " + connect_error;
                } else {
                    connected = true;
                    live_snapshot = tracker.WaitForLogon(
                        std::chrono::seconds(
                            options.logon_timeout_seconds),
                        fatal);
                    if (!live_snapshot.logon_received) {
                        failure = "timed out waiting for LogonResponse";
                    } else if (live_snapshot.malformed) {
                        failure = "received a malformed LogonResponse";
                    } else if (!live_snapshot.subscriptions_ok()) {
                        failure =
                            "LogonResponse did not approve every required "
                            "subscription";
                    } else {
                        live_snapshot = tracker.Monitor(
                            std::chrono::seconds(options.monitor_seconds),
                            fatal,
                            metrics,
                            options.maximum_raw_bytes,
                            &raw_limit_exceeded);
                    }
                }
            }
        }
    } catch (const std::exception& exception) {
        AppendExceptionNoThrow(
            &failure, "SDK call threw: ", exception);
    } catch (...) {
        AppendErrorNoThrow(
            &failure, "SDK call threw an unknown exception");
    }

    const SdkStopResult sdk_stop = StopAndReleaseSdk(
        &manager, &subscriber, &handler, &failure);
    if (!sdk_stop.clean) {
        AppendErrorNoThrow(
            &failure, "SDK shutdown/release did not complete cleanly");
    }
    worker_session.StopAndJoin();
    if (!sdk_stop.safe_to_unwind) {
        std::cerr
            << "mdl-phase2-live-probe: SDK shutdown or callback quiescence "
               "failed; artifacts may be incomplete; terminating without "
               "unwinding callback-owned state\n"
            << std::flush;
        // The retained vendor objects may still hold callback_router. Avoid
        // destroying its stack-owned handler/ring/metrics references.
        ::_exit(1);
    }

    live_snapshot = tracker.Snapshot();
    const ingress::CaptureMetricsSnapshot callback = metrics.Snapshot();
    const ingress::RawCaptureWorkerSnapshot capture = worker.Snapshot();
    const ingress::RawWalWriterSnapshot wal = writer.Snapshot();
    const ingress::RawCaptureReconciliation reconciliation =
        worker.Reconcile(callback);

    if (failure.empty() && !connected) {
        failure = "SDK connection was not established";
    }
    if (failure.empty() && g_stop_requested != 0) {
        failure = "capture was interrupted by a stop signal";
    }
    if (failure.empty() && raw_limit_exceeded) {
        failure = "capture exceeded --maximum-raw-bytes";
    }
    if (failure.empty() && fatal.tripped()) {
        failure = "Phase-2 capture fatal latch tripped with reason " +
                  std::to_string(
                      static_cast<unsigned int>(fatal.reason()));
    }
    if (failure.empty() && InvalidMetrics(callback)) {
        failure = "Phase-2 callback metrics contain a fatal/invalid event";
    }
    if (failure.empty() && !live_snapshot.subscriptions_ok()) {
        failure = "required live subscription evidence is incomplete";
    }
    if (failure.empty() &&
        !live_snapshot.minimum_met(
            options.minimum_market_messages_per_key)) {
        failure =
            "minimum live market-message count was not reached for every "
            "required key";
    }
    if (failure.empty() &&
        (!worker_session.result() ||
         capture.failure_kind !=
             ingress::RawCaptureWorkerFailureKind::kNone ||
         !capture.stop_requested || !capture.finished ||
         !reconciliation.exact() || ring.used_bytes() != 0U ||
         !wal.initialized || !wal.sealed || !wal.closed || wal.fatal ||
         wal.append != wal.durable)) {
        failure = "Raw worker, WAL seal, or exact reconciliation failed";
    }

    ValidationResult validation;
    if (failure.empty() &&
        !ValidateRawArtifacts(
            output_directory.get(),
            options,
            spec,
            callback,
            capture,
            wal,
            live_snapshot,
            &validation,
            &failure)) {
        if (failure.empty()) {
            failure = "Raw artifact validation failed";
        }
    }

    if (!failure.empty()) {
        std::cerr
            << "mdl-phase2-live-probe: "
            << SafeDiagnostic(std::move(failure))
            << "; artifacts retained at "
            << options.output_directory.string() << '\n';
        return 1;
    }

    PrintSuccessJson(
        options,
        spec,
        live_snapshot,
        callback,
        capture,
        wal,
        reconciliation,
        validation);
    return 0;
}

}  // namespace

int main(int argc, char* argv[]) {
    Options options;
    bool show_help = false;
    std::string error;
    if (!ParseOptions(
            argc, argv, &options, &show_help, &error)) {
        std::cerr << "mdl-phase2-live-probe: "
                  << SafeDiagnostic(std::move(error)) << '\n';
        PrintUsage(std::cerr);
        return 2;
    }
    if (show_help) {
        PrintUsage(std::cout);
        return 0;
    }
    if (std::signal(SIGINT, &HandleStopSignal) == SIG_ERR ||
        std::signal(SIGTERM, &HandleStopSignal) == SIG_ERR) {
        std::cerr
            << "mdl-phase2-live-probe: cannot install stop signal handlers\n";
        return 2;
    }
    try {
        return Run(options);
    } catch (const std::exception& exception) {
        std::cerr << "mdl-phase2-live-probe: fatal: "
                  << SafeDiagnostic(exception.what()) << '\n';
        return 2;
    } catch (...) {
        std::cerr << "mdl-phase2-live-probe: unknown fatal exception\n";
        return 2;
    }
}
