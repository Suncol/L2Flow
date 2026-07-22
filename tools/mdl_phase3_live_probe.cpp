#include "l2flow/baseline/vendor_baseline.h"
#include "l2flow/build_manifest.h"
#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/control/control_checkpoint_posix_store.h"
#include "l2flow/control/control_production_controller.h"
#include "l2flow/control/control_record_posix_sink.h"
#include "l2flow/ingress/capture_clock.h"
#include "l2flow/ingress/clock_epoch.h"
#include "l2flow/ingress/raw_clean_stop_gate.h"
#include "l2flow/ingress/raw_control_file.h"
#include "l2flow/ingress/raw_manifest_store.h"
#include "l2flow/ingress/raw_namespace.h"
#include "l2flow/ingress/raw_production_runtime.h"
#include "l2flow/ingress/raw_reader.h"
#include "l2flow/ingress/raw_recovery_maintenance_report_v1.h"
#include "l2flow/ingress/raw_recovery_posix.h"
#include "l2flow/ingress/raw_replay.h"
#include "l2flow/ingress/raw_reserve_coordinator.h"
#include "l2flow/ingress/raw_schema.h"
#include "l2flow/ingress/raw_sealed_certificate_v1.h"
#include "l2flow/sdk/endpoint_contract.h"
#include "l2flow/sdk/sdk_runtime.h"
#include "l2flow/sdk/subscription_manifest.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
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
namespace control = l2flow::control;
namespace ingress = l2flow::ingress;
namespace sdk = l2flow::sdk;

namespace {

constexpr std::uint64_t kMebibyte = UINT64_C(1024) * UINT64_C(1024);
constexpr std::uint64_t kSegmentTargetBytes = UINT64_C(512) * kMebibyte;
constexpr std::uint32_t kMaximumMessageBytes = 16U * 1024U * 1024U;
constexpr std::string_view kLocalClientLabel =
    "l2flow-local-phase3-live-probe";

volatile std::sig_atomic_t g_stop_requested = 0;

extern "C" void HandleStopSignal(int) {
    g_stop_requested = 1;
}

class FileDescriptor final {
public:
    explicit FileDescriptor(int value = -1) noexcept : value_(value) {}
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
    void Reset(int value = -1) noexcept {
        if (value_ >= 0) {
            static_cast<void>(::close(value_));
        }
        value_ = value;
    }

private:
    int value_ = -1;
};

struct Options final {
    std::filesystem::path library;
    std::filesystem::path output_directory;
    sdk::IngressKind ingress_kind = sdk::IngressKind::SzTick;
    bool ingress_kind_set = false;
    std::string address = "127.0.0.1:9112";
    std::uint32_t capture_date = 0U;
    std::uint32_t monitor_seconds = 10U;
    std::uint32_t segment_max_age_seconds = 3600U;
};

struct OutputDirectories final {
    std::filesystem::path raw_path;
    std::filesystem::path derived_path;
    std::filesystem::path checkpoint_path;
    FileDescriptor root;
    FileDescriptor raw;
    FileDescriptor derived;
    FileDescriptor checkpoint;
};

struct PreparedRuntimeInputs final {
    ingress::RawIngressAppConfigV1 app{};
    ingress::RawReserveFreshScaffoldingV1 registration{};
    ingress::SegmentHeaderV1 segment{};
    ingress::RawWalWriterConfig writer{};
    ingress::RawPosixWalStreamBackendOptionsV1 backend{};
    ingress::RawSegmentArtifactOptionsV1 artifacts{};
    ingress::RawWalStreamLimitsV1 stream_limits{};
};

template <std::size_t Size>
std::array<std::byte, Size> Pattern(std::uint8_t seed) noexcept {
    std::array<std::byte, Size> result{};
    for (std::size_t index = 0U; index < Size; ++index) {
        result[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(seed + index));
    }
    return result;
}

std::uint64_t ClockNow(clockid_t clock) noexcept {
    struct timespec value {};
    if (::clock_gettime(clock, &value) != 0 ||
        value.tv_sec < 0 || value.tv_nsec < 0) {
        return 0U;
    }
    const std::uint64_t seconds =
        static_cast<std::uint64_t>(value.tv_sec);
    if (seconds >
        (std::numeric_limits<std::uint64_t>::max() -
         static_cast<std::uint64_t>(value.tv_nsec)) /
            UINT64_C(1'000'000'000)) {
        return 0U;
    }
    return seconds * UINT64_C(1'000'000'000) +
        static_cast<std::uint64_t>(value.tv_nsec);
}

std::uint64_t RealtimeNow(void*) noexcept {
    return ClockNow(CLOCK_REALTIME);
}

std::uint64_t MonotonicNow(void*) noexcept {
    return ClockNow(CLOCK_MONOTONIC);
}

bool LocalCaptureDate(std::uint32_t* output, std::string* error) noexcept {
    const std::time_t now = std::time(nullptr);
    struct tm local {};
    if (now == static_cast<std::time_t>(-1) ||
        ::localtime_r(&now, &local) == nullptr) {
        *error = "cannot obtain the local capture date";
        return false;
    }
    *output = static_cast<std::uint32_t>(local.tm_year + 1900) * 10000U +
        static_cast<std::uint32_t>(local.tm_mon + 1) * 100U +
        static_cast<std::uint32_t>(local.tm_mday);
    return true;
}

template <typename Integer>
bool ParseUnsigned(std::string_view text, Integer* output) noexcept {
    static_assert(std::is_unsigned_v<Integer>);
    Integer value = 0;
    const auto result = std::from_chars(
        text.data(), text.data() + text.size(), value);
    if (result.ec != std::errc{} ||
        result.ptr != text.data() + text.size()) {
        return false;
    }
    *output = value;
    return true;
}

bool SafeJsonAtom(std::string_view text) noexcept {
    return !text.empty() &&
        std::none_of(text.begin(), text.end(), [](unsigned char value) {
            return value < 0x20U || value == 0x7fU ||
                   value == static_cast<unsigned char>('"') ||
                   value == static_cast<unsigned char>('\\');
        });
}

bool NormalizedAbsolute(const std::filesystem::path& path) noexcept {
    try {
        return path.is_absolute() && path != path.root_path() &&
            path.lexically_normal() == path;
    } catch (...) {
        return false;
    }
}

std::string_view KindSlug(sdk::IngressKind kind) {
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

void PrintUsage() {
    std::cout
        << "Usage: mdl-phase3-live-probe --library PATH --output-dir PATH "
           "--ingress-kind KIND [options]\n"
        << "  --ingress-kind KIND   sh-snapshot, sh-tick, sz-snapshot, or "
           "sz-tick\n"
        << "  --address HOST:PORT   feeder endpoint (default 127.0.0.1:9112)\n"
        << "  --monitor-seconds N   live READY monitoring duration, 1..300 "
           "(default 10)\n"
        << "  --capture-date DATE   YYYYMMDD (default local date)\n"
        << "  --segment-max-age-seconds N  test-only rotation age, 1..3600 "
           "(default 3600)\n"
        << "The output directory must be an absolute path which does not "
           "exist. No credential is accepted on the command line. The "
           "probe runs two real SDK generations separated by sealed-Raw "
           "production recovery and checkpoint restore.\n";
}

bool ParseOptions(
    int argc,
    char* argv[],
    Options* options,
    bool* help,
    std::string* error) {
    for (int index = 1; index < argc; ++index) {
        const std::string_view option(argv[index]);
        if (option == "--help") {
            *help = true;
            return argc == 2;
        }
        if (index + 1 >= argc) {
            *error = std::string(option) + " requires a value";
            return false;
        }
        const std::string_view value(argv[++index]);
        if (option == "--library") {
            options->library = std::string(value);
        } else if (option == "--output-dir") {
            options->output_directory = std::string(value);
        } else if (option == "--ingress-kind") {
            if (!sdk::ParseIngressKind(value, &options->ingress_kind)) {
                *error = "unknown --ingress-kind";
                return false;
            }
            options->ingress_kind_set = true;
        } else if (option == "--address") {
            options->address = value;
        } else if (option == "--monitor-seconds") {
            if (!ParseUnsigned(value, &options->monitor_seconds) ||
                options->monitor_seconds == 0U ||
                options->monitor_seconds > 300U) {
                *error = "--monitor-seconds must be from 1 through 300";
                return false;
            }
        } else if (option == "--capture-date") {
            if (!ParseUnsigned(value, &options->capture_date)) {
                *error = "--capture-date must be YYYYMMDD";
                return false;
            }
        } else if (option == "--segment-max-age-seconds") {
            if (!ParseUnsigned(value, &options->segment_max_age_seconds) ||
                options->segment_max_age_seconds == 0U ||
                options->segment_max_age_seconds > 3600U) {
                *error =
                    "--segment-max-age-seconds must be from 1 through 3600";
                return false;
            }
        } else {
            *error = "unknown option: " + std::string(option);
            return false;
        }
    }
    if (!options->ingress_kind_set ||
        !NormalizedAbsolute(options->library) ||
        !NormalizedAbsolute(options->output_directory) ||
        !SafeJsonAtom(options->address)) {
        *error = "required paths/kind/address are missing or unsafe";
        return false;
    }
    if (options->capture_date == 0U &&
        !LocalCaptureDate(&options->capture_date, error)) {
        return false;
    }
    return true;
}

int OpenDirectoryAt(int parent, const char* name) noexcept {
    for (;;) {
        const int fd = ::openat(
            parent, name,
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        if (fd >= 0 || errno != EINTR) {
            return fd;
        }
    }
}

bool CreateOutputDirectories(
    const Options& options,
    OutputDirectories* output,
    std::string* error) {
    if (::mkdir(options.output_directory.c_str(), 0700) != 0) {
        *error = "cannot create the exclusive output directory";
        return false;
    }
    output->root.Reset(::open(
        options.output_directory.c_str(),
        O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
    if (!output->root.valid() || ::fchmod(output->root.get(), 0700) != 0) {
        *error = "cannot retain the private output directory";
        return false;
    }
    for (const char* name : {"raw", "derived", "checkpoints"}) {
        if (::mkdirat(output->root.get(), name, 0700) != 0) {
            *error = std::string("cannot create output/") + name;
            return false;
        }
    }
    output->raw.Reset(OpenDirectoryAt(output->root.get(), "raw"));
    output->derived.Reset(OpenDirectoryAt(output->root.get(), "derived"));
    output->checkpoint.Reset(
        OpenDirectoryAt(output->root.get(), "checkpoints"));
    if (!output->raw.valid() || !output->derived.valid() ||
        !output->checkpoint.valid() ||
        ::fchmod(output->raw.get(), 0700) != 0 ||
        ::fchmod(output->derived.get(), 0700) != 0 ||
        ::fchmod(output->checkpoint.get(), 0700) != 0 ||
        ::fsync(output->root.get()) != 0) {
        *error = "cannot retain and sync private output subdirectories";
        return false;
    }
    output->raw_path = options.output_directory / "raw";
    output->derived_path = options.output_directory / "derived";
    output->checkpoint_path = options.output_directory / "checkpoints";
    return true;
}

std::string EndpointBytes(const Options& options) {
    std::string result =
        "{\"schema_version\":1,\"ingress_kind\":\"";
    result.append(KindSlug(options.ingress_kind));
    result.append(
        "\",\"name\":\"phase3-live-probe\","
        "\"resolved_server_address\":\"");
    result.append(options.address);
    result.append(
        "\",\"message_encoding\":1,\"merge_message\":false,"
        "\"send_mac_auth\":false,\"server_select\":false}");
    return result;
}

bool ParseDigest(
    std::string_view text,
    common::Sha256Digest* output,
    std::string_view label,
    std::string* error) {
    std::string detail;
    if (!common::ParseSha256Hex(text, output, &detail)) {
        *error = "cannot parse " + std::string(label) + " digest: " + detail;
        return false;
    }
    return true;
}

bool PrepareRuntimeInputs(
    const Options& options,
    const OutputDirectories& directories,
    const std::shared_ptr<const sdk::VerifiedEndpointContract>& endpoint,
    std::string_view endpoint_sha256,
    PreparedRuntimeInputs* output,
    std::string* error) {
    const sdk::IngressSpec& spec = sdk::GetIngressSpec(options.ingress_kind);
    ingress::RawIngressConfig stable =
        ingress::DefaultRawIngressConfig(options.ingress_kind);
    stable.endpoint_contract_sha256 = endpoint_sha256;
    stable.credential_name = "local-feeder-label";
    stable.sdk_log_prefix =
        (options.output_directory / "sdk-phase3-live").string();
    stable.metrics_textfile_path =
        (options.output_directory / "metrics.prom").string();
    stable.max_message_bytes = kMaximumMessageBytes;
    stable.ring_capacity_bytes = UINT64_C(128) * kMebibyte;
    stable.raw_root = directories.raw_path.string();
    stable.segment_target_bytes = kSegmentTargetBytes;
    // The probe must not manufacture an age rotation merely because a
    // deliberately expensive high-volume catch-up takes longer than its
    // short live observation window. Rotation has independent tests; this
    // runner keeps one segment so Raw-to-Phase-3 count reconciliation is
    // directly inspectable after stop.
    stable.segment_max_age_seconds = options.segment_max_age_seconds;
    stable.sync_bytes = UINT64_C(4) * kMebibyte;
    stable.reserve_domain_id = "phase3-live-probe";
    stable.reserve_coordinator_socket =
        "/tmp/l2flow-phase3-live-probe-" +
        std::to_string(static_cast<unsigned long>(::getpid())) + ".sock";
    stable.emergency_reserve_bytes = UINT64_C(512) * kMebibyte;
    stable.canonical_clock_source_config =
        "CLOCK_MONOTONIC_RAW+CLOCK_REALTIME";
    const std::string config_error =
        ingress::ValidateRawIngressConfig(stable);
    if (!config_error.empty()) {
        *error = "invalid live Raw configuration: " + config_error;
        return false;
    }

    ingress::ClockEpochInputs epoch_inputs;
    if (!ingress::ReadClockEpochInputs(
            "/etc/machine-id",
            "/proc/sys/kernel/random/boot_id",
            stable.canonical_clock_source_config,
            &epoch_inputs,
            error)) {
        return false;
    }
    ingress::ClockEpoch epoch;
    try {
        epoch = ingress::ComputeClockEpoch(epoch_inputs);
    } catch (const std::exception& exception) {
        *error = "cannot compute clock epoch: " +
            std::string(exception.what());
        return false;
    }

    ingress::LinuxCaptureClock creation_clock;
    ingress::SegmentHeaderV1 segment;
    segment.source_stream_id = spec.source_stream_id;
    segment.capture_date = options.capture_date;
    if (!common::GenerateIdentity128(&segment.stream_day_id, nullptr)) {
        *error = "cannot generate stream-day identity";
        return false;
    }
    segment.segment_sequence = 1U;
    segment.segment_base_wal_pos = 0U;
    segment.first_ingress_sequence = 1U;
    try {
        segment.created_realtime_ns = creation_clock.RealtimeNanoseconds();
        segment.created_monotonic_ns =
            creation_clock.MonotonicRawNanoseconds();
    } catch (const std::exception& exception) {
        *error = "cannot obtain segment creation clocks: " +
            std::string(exception.what());
        return false;
    }
    if (!common::ParseIdentity128Hex(
            epoch_inputs.host_uuid, &segment.host_uuid) ||
        !common::ParseCanonicalUuid128(
            epoch_inputs.linux_boot_id, &segment.linux_boot_id)) {
        *error = "host/boot identities are invalid";
        return false;
    }
    segment.clock_epoch_algorithm = stable.clock_epoch_algorithm_version;
    segment.clock_epoch_digest = epoch.digest;
    segment.clock_epoch_label = epoch.value;
    if (!ParseDigest(
            baseline::ApprovedVendorBaseline().sdk_archive_sha256,
            &segment.sdk_archive_sha256,
            "SDK archive",
            error) ||
        !common::ComputeFileSha256(
            options.library,
            &segment.libmdl_api_sha256,
            error,
            baseline::kMaximumSdkSharedLibraryBytes) ||
        !ParseDigest(
            endpoint_sha256,
            &segment.endpoint_contract_sha256,
            "endpoint contract",
            error) ||
        !ParseDigest(
            ingress::RawIngressConfigSha256(stable),
            &segment.config_sha256,
            "Raw configuration",
            error) ||
        !ParseDigest(
            l2flow::build_manifest::kSha256,
            &segment.build_manifest_sha256,
            "build manifest",
            error)) {
        return false;
    }
    segment.raw_schema_sha256 = ingress::RawSchemaSha256Digest();

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

    ingress::RawWalWriterConfig writer;
    if (ingress::EncodeSegmentHeaderV1(
            segment, &writer.segment_header_wire) !=
            ingress::RawV1Error::kNone ||
        ingress::EncodeDurableJournalHeaderV1(
            journal, &writer.journal_header_wire) !=
            ingress::RawV1Error::kNone ||
        !common::GenerateIdentity128(&writer.writer_instance, nullptr)) {
        *error = "cannot encode fresh Raw headers/identity";
        return false;
    }
    writer.source_stream_id = segment.source_stream_id;
    writer.capture_date = segment.capture_date;
    writer.segment_sequence = 1U;
    writer.segment_base_wal_pos = 0U;
    writer.first_ingress_sequence = 1U;
    writer.initial_durable_ingress_sequence = 0U;
    writer.initialization_mode = ingress::RawWalInitializationMode::kFreshJournal;
    writer.headers_already_persisted = false;

    ingress::RawIngressRuntimeState recovered;
    recovered.source_stream_id = segment.source_stream_id;
    recovered.capture_date = segment.capture_date;
    recovered.stream_day_id = segment.stream_day_id;
    recovered.recovered_next_ingress_sequence = 1U;
    recovered.append = {
        1U,
        ingress::kRawV1SegmentHeaderBytes,
        0U,
        ingress::kRawV1SegmentHeaderBytes};
    recovered.durable = recovered.append;
    recovered.writer_instance = writer.writer_instance;
    recovered.current_segment_sequence = 1U;
    recovered.clock_epoch_algorithm_version =
        stable.clock_epoch_algorithm_version;
    recovered.clock_epoch_digest = epoch.digest;
    recovered.clock_epoch_label = epoch.value;

    ingress::RawReserveFreshScaffoldingV1 registration;
    registration.key.route.source_stream_id = segment.source_stream_id;
    registration.key.route.capture_date = segment.capture_date;
    registration.key.stream_day_id = segment.stream_day_id;
    if (!common::GenerateIdentity128(
            &registration.key.recovery_attempt_id, nullptr)) {
        *error = "cannot generate recovery-attempt identity";
        return false;
    }
    registration.writer_instance = writer.writer_instance;
    registration.recovery_intent =
        ingress::ReserveRecoveryIntentV1::kResumeConnect;
    registration.scaffolding_allocation_cap =
        UINT64_C(1024) * kMebibyte;
    registration.safe_stop_template_id = 1U;

    ingress::RawRecordLayoutV1 maximum_record{};
    if (ingress::ComputeRawRecordLayoutV1(
            stable.max_message_bytes - ingress::kVendorMessageHeadBytes,
            &maximum_record) != ingress::RawV1Error::kNone) {
        *error = "cannot compute maximum Raw record layout";
        return false;
    }

    output->app.stable = std::move(stable);
    output->app.endpoint = endpoint;
    output->app.credential_token = std::string(kLocalClientLabel);
    output->app.recovered = recovered;
    output->app.connect_generation = 1U;
    output->registration = registration;
    output->segment = segment;
    output->writer = writer;
    output->backend.writer_instance = writer.writer_instance;
    output->backend.segment_preallocation_bytes = kSegmentTargetBytes;
    output->backend.realtime_now = &RealtimeNow;
    output->backend.monotonic_now = &MonotonicNow;
    output->artifacts.expected_raw_schema_sha256 =
        segment.raw_schema_sha256;
    output->artifacts.sample_record_interval =
        output->app.stable.sparse_index_every_records;
    output->artifacts.sample_raw_bytes_interval =
        output->app.stable.sparse_index_every_bytes;
    output->artifacts.maximum_segment_bytes = kSegmentTargetBytes;
    output->stream_limits.segment_target_bytes = kSegmentTargetBytes;
    output->stream_limits.segment_max_age_ns =
        static_cast<std::uint64_t>(
            output->app.stable.segment_max_age_seconds) *
        UINT64_C(1'000'000'000);
    output->stream_limits.maximum_record_bytes = maximum_record.record_size;
    output->stream_limits.monotonic_now = &MonotonicNow;
    return true;
}

ingress::ReserveCoordinatorStateV1 MakeCoordinatorState(int raw_fd) {
    struct stat status {};
    ingress::ReserveCoordinatorStateV1 state{};
    if (::fstat(raw_fd, &status) != 0) {
        return state;
    }
    state.header.reserve_state_uuid = Pattern<16U>(0x11U);
    state.header.quota_identity_sha256 = Pattern<32U>(0x31U);
    state.header.mount_identity_sha256 = Pattern<32U>(0x51U);
    state.header.device_id = static_cast<std::uint64_t>(status.st_dev);
    state.header.declared_releasable_bytes =
        UINT64_C(4) * UINT64_C(1024) * kMebibyte;
    state.header.allocation_quantum_bytes = 4096U;
    state.header.declared_inode_reserve_count = 4096U;
    state.header.byte_probe_version = 1U;
    state.header.inode_probe_version = 1U;
    state.header.inode_inventory_sha256 = Pattern<32U>(0x71U);
    state.header.safe_stop_catalog_sha256 = Pattern<32U>(0x91U);
    ingress::ReserveStateSlotV1 slot{};
    slot.coordinator_state = ingress::ReserveCoordinatorPhaseV1::kProvisioned;
    slot.generation = 1U;
    slot.reserve_state_uuid = state.header.reserve_state_uuid;
    state.slots[0U] = slot;
    state.slots[1U] = slot;
    state.selected_slot = 0U;
    return state;
}

ingress::RawReserveCoordinatorLeaseMarkerV1 MakeCoordinatorMarker(
    const ingress::ReserveCoordinatorStateV1& state) {
    ingress::RawReserveCoordinatorLeaseMarkerV1 marker{};
    marker.coordinator_identity = Pattern<16U>(0x05U);
    marker.device_id = state.header.device_id;
    marker.quota_identity_sha256 = state.header.quota_identity_sha256;
    marker.mount_identity_sha256 = state.header.mount_identity_sha256;
    return marker;
}

std::uint64_t CountRegularFiles(const std::filesystem::path& directory) {
    std::uint64_t result = 0U;
    std::error_code error;
    for (std::filesystem::directory_iterator iterator(directory, error), end;
         !error && iterator != end;
         iterator.increment(error)) {
        if (iterator->is_regular_file(error) && !error) {
            ++result;
        }
    }
    return error ? 0U : result;
}

std::shared_ptr<const std::vector<std::byte>> ReadSegment(
    int directory_fd,
    std::uint32_t segment_sequence,
    std::uint64_t maximum_bytes,
    std::string* error) {
    std::array<char, 32U> name{};
    const int formatted = std::snprintf(
        name.data(), name.size(), "segment-%08u.raw", segment_sequence);
    if (formatted <= 0 ||
        static_cast<std::size_t>(formatted) >= name.size()) {
        *error = "cannot format the Raw segment filename";
        return nullptr;
    }
    FileDescriptor fd(::openat(
        directory_fd,
        name.data(),
        O_RDONLY | O_NOFOLLOW | O_CLOEXEC));
    struct stat status {};
    if (!fd.valid() || ::fstat(fd.get(), &status) != 0 ||
        !S_ISREG(status.st_mode) || status.st_size < 0 ||
        static_cast<std::uint64_t>(status.st_size) > maximum_bytes) {
        *error = "cannot inspect a sealed Raw segment";
        return nullptr;
    }
    auto bytes = std::make_shared<std::vector<std::byte>>(
        static_cast<std::size_t>(status.st_size));
    std::size_t offset = 0U;
    while (offset < bytes->size()) {
        const ssize_t count = ::pread(
            fd.get(), bytes->data() + offset,
            bytes->size() - offset, static_cast<off_t>(offset));
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            *error = "cannot read a complete sealed Raw segment";
            return nullptr;
        }
        offset += static_cast<std::size_t>(count);
    }
    return bytes;
}

std::shared_ptr<const std::vector<std::byte>> ReadSegmentPrefix(
    int directory_fd,
    std::uint32_t segment_sequence,
    std::uint64_t logical_bytes,
    std::string* error) {
    if (logical_bytes < ingress::kRawV1SegmentHeaderBytes ||
        logical_bytes > kSegmentTargetBytes ||
        logical_bytes >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max())) {
        *error = "invalid Raw open-segment logical prefix";
        return nullptr;
    }
    std::array<char, 32U> name{};
    const int formatted = std::snprintf(
        name.data(), name.size(), "segment-%08u.raw", segment_sequence);
    if (formatted <= 0 ||
        static_cast<std::size_t>(formatted) >= name.size()) {
        *error = "cannot format the Raw open-segment filename";
        return nullptr;
    }
    FileDescriptor fd(::openat(
        directory_fd,
        name.data(),
        O_RDONLY | O_NOFOLLOW | O_CLOEXEC));
    struct stat status {};
    if (!fd.valid() || ::fstat(fd.get(), &status) != 0 ||
        !S_ISREG(status.st_mode) || status.st_size < 0 ||
        static_cast<std::uint64_t>(status.st_size) < logical_bytes) {
        *error = "cannot inspect the Raw open-segment prefix";
        return nullptr;
    }
    auto bytes = std::make_shared<std::vector<std::byte>>(
        static_cast<std::size_t>(logical_bytes));
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
            *error = "cannot read the complete Raw open-segment prefix";
            return nullptr;
        }
        offset += static_cast<std::size_t>(count);
    }
    return bytes;
}

struct RecoveredGenerationV1 final {
    ingress::RawProductionRuntimeBuildResultV1 runtime{};
    common::Identity128 writer_instance{};
    ingress::SegmentHeaderV1 open_segment{};
    std::uint64_t recovery_analyzed_journal_size = 0U;
    std::uint64_t recovery_retained_journal_size = 0U;
    std::uint32_t recovered_from_segment_sequence = 0U;
    std::uint32_t opened_segment_sequence = 0U;
    bool recovery_plan_clean = false;
    bool recovery_report_built = false;
};

bool PrepareRecoveredGeneration(
    const Options& options,
    const OutputDirectories& directories,
    const PreparedRuntimeInputs& generation_one,
    ingress::RawReserveRegistryCoordinatorV1& coordinator,
    const std::shared_ptr<sdk::SdkFactory>& sdk_factory,
    RecoveredGenerationV1* output,
    std::string* error) {
    const std::string stream_slug(KindSlug(options.ingress_kind));
    auto stream_directory = ingress::OpenExistingRawStreamDirectory(
        directories.raw_path.string(),
        generation_one.segment.source_stream_id,
        generation_one.segment.capture_date,
        stream_slug,
        error);
    if (stream_directory == nullptr) {
        return false;
    }

    ingress::RawReserveExistingAnchorRecoveryV1 registration{};
    registration.key.route.source_stream_id =
        generation_one.segment.source_stream_id;
    registration.key.route.capture_date =
        generation_one.segment.capture_date;
    registration.key.stream_day_id =
        generation_one.segment.stream_day_id;
    if (!common::GenerateIdentity128(
            &registration.key.recovery_attempt_id, nullptr) ||
        !common::GenerateIdentity128(
            &registration.writer_instance, nullptr)) {
        *error = "cannot generate generation-2 recovery identities";
        return false;
    }
    registration.recovery_intent =
        ingress::ReserveRecoveryIntentV1::kResumeConnect;
    registration.safe_stop_template_id =
        generation_one.registration.safe_stop_template_id;
    if (coordinator.RegisterExistingAnchorRecovering(
            registration, error) !=
        ingress::RawReserveCoordinatorErrorV1::kNone) {
        return false;
    }

    auto lease = ingress::AcquireRawWriterLeaseAtV1(
        stream_directory->descriptor(),
        registration.key.route.source_stream_id,
        registration.key.route.capture_date,
        registration.key.recovery_attempt_id,
        error);
    if (lease == nullptr) {
        return false;
    }
    ingress::RawPosixRecoveryLimitsV1 recovery_limits;
    recovery_limits.max_segment_bytes = kSegmentTargetBytes;
    recovery_limits.max_total_segment_bytes =
        UINT64_C(4) * kSegmentTargetBytes;
    auto recovery_session = ingress::LoadRawPosixRecoverySessionV1(
        *lease, recovery_limits, error);
    if (recovery_session == nullptr ||
        recovery_session->input().journal == nullptr ||
        recovery_session->input().journal->size() <
            ingress::kRawV1JournalHeaderBytes ||
        recovery_session->input().segments.empty() ||
        recovery_session->input().segments.back().bytes == nullptr) {
        if (error->empty()) {
            *error = "cannot load the generation-1 sealed Raw recovery image";
        }
        return false;
    }
    const std::uint64_t analyzed_journal_size =
        recovery_session->input().journal->size();
    ingress::RawV1JournalHeaderWire journal_header{};
    std::copy_n(
        recovery_session->input().journal->begin(),
        journal_header.size(),
        journal_header.begin());
    ingress::SegmentHeaderV1 terminal_segment{};
    const auto& terminal_bytes =
        recovery_session->input().segments.back().bytes;
    if (terminal_bytes->size() < ingress::kRawV1SegmentHeaderBytes ||
        ingress::DecodeSegmentHeaderV1(
            std::span<const std::byte>(
                terminal_bytes->data(),
                ingress::kRawV1SegmentHeaderBytes),
            &terminal_segment) != ingress::RawV1Error::kNone) {
        *error = "cannot decode the generation-1 terminal Raw segment";
        return false;
    }
    const ingress::RawRecoveryPlanV1 recovery_plan =
        ingress::AnalyzeRawRecoveryV1(recovery_session->input());
    const bool completely_sealed =
        recovery_plan.ok() && recovery_plan.has_accepted_cursor &&
        !recovery_plan.segments.empty() &&
        std::all_of(
            recovery_plan.segments.begin(),
            recovery_plan.segments.end(),
            [](const ingress::RawRecoverySegmentPlanV1& segment) {
                return segment.sealed && segment.has_accepted_marker;
            }) &&
        (recovery_plan.accepted_cursor.marker_flags &
         ingress::kRawV1SegmentSealed) != 0U &&
        recovery_plan.accepted_cursor.segment_sequence ==
            terminal_segment.segment_sequence;
    if (!completely_sealed) {
        *error = "generation-1 Raw is not an exact completely sealed recovery frontier";
        return false;
    }
    const ingress::RawRecoveryExecutionResultV1 recovery_execution =
        ingress::ExecuteRawRecoveryPlanV1(
            recovery_plan, *recovery_session);
    if (recovery_execution.failure !=
            ingress::RawRecoveryExecutionFailureV1::kNone ||
        !recovery_execution.cursor_publishable ||
        recovery_execution.recovered_cursor.segment_sequence !=
            terminal_segment.segment_sequence ||
        (recovery_execution.recovered_cursor.marker_flags &
         ingress::kRawV1SegmentSealed) == 0U) {
        *error = "generation-1 sealed Raw recovery execution failed";
        return false;
    }

    ingress::RawManifestV1 closed_manifest{};
    const ingress::RawManifestNamespaceV1 raw_namespace{
        terminal_segment.capture_date,
        terminal_segment.source_stream_id,
        terminal_segment.stream_day_id};
    if (ingress::LoadCurrentRawManifestAt(
            stream_directory->descriptor(),
            raw_namespace,
            generation_one.backend.maximum_manifest_bytes,
            &closed_manifest,
            nullptr,
            nullptr,
            error) != ingress::RawManifestStoreError::kNone ||
        closed_manifest.open_entry.has_value() ||
        closed_manifest.closed_entries.empty() ||
        closed_manifest.closed_entries.back().segment_sequence !=
            terminal_segment.segment_sequence) {
        if (error->empty()) {
            *error = "generation-1 closed Raw manifest is invalid";
        }
        return false;
    }

    ingress::RawWalWriterSnapshot generation_one_terminal{};
    generation_one_terminal.append = {
        recovery_execution.recovered_cursor.global_wal_pos,
        recovery_execution.recovered_cursor.ingress_sequence,
        recovery_execution.recovered_cursor.segment_offset};
    generation_one_terminal.durable = generation_one_terminal.append;
    generation_one_terminal.journal_logical_size =
        recovery_execution.retained_journal_size;
    generation_one_terminal.initialized = true;
    generation_one_terminal.sealed = true;
    generation_one_terminal.closed = true;
    ingress::RawWalSinkIdentityV1 generation_one_identity{};
    generation_one_identity.writer_instance =
        generation_one.writer.writer_instance;
    generation_one_identity.stream_day_id = terminal_segment.stream_day_id;
    generation_one_identity.source_stream_id =
        terminal_segment.source_stream_id;
    generation_one_identity.capture_date = terminal_segment.capture_date;
    generation_one_identity.segment_sequence =
        terminal_segment.segment_sequence;
    generation_one_identity.segment_base_wal_pos =
        terminal_segment.segment_base_wal_pos;
    generation_one_identity.first_ingress_sequence =
        terminal_segment.first_ingress_sequence;
    std::unique_ptr<ingress::BuiltSealedRawCertificateV1>
        sealed_certificate;
    if (ingress::BuildSealedRawCertificateCapabilityV1(
            journal_header,
            closed_manifest,
            generation_one_identity,
            generation_one_terminal,
            &sealed_certificate) !=
            ingress::SealedRawCertificateV1Error::kNone ||
        sealed_certificate == nullptr) {
        *error = "cannot revalidate the published generation-1 sealed certificate";
        return false;
    }

    ingress::RawRecoveredClosedWalStateV1 recovered_closed{};
    recovered_closed.closed_manifest = closed_manifest;
    recovered_closed.journal_header_wire = journal_header;
    recovered_closed.terminal_segment = terminal_segment;
    recovered_closed.accepted_sealed_cursor =
        generation_one_terminal.durable;
    recovered_closed.journal_logical_size =
        recovery_execution.retained_journal_size;

    recovery_session.reset();
    ingress::RawPosixWalStreamBackendOptionsV1 backend =
        generation_one.backend;
    backend.writer_instance = registration.writer_instance;
    auto recovering_stream =
        ingress::CreateRecoveredClosedRawPosixStreamV1(
            std::move(lease),
            std::move(recovered_closed),
            backend,
            generation_one.artifacts,
            generation_one.stream_limits,
            ClockNow(CLOCK_MONOTONIC),
            coordinator,
            registration.key,
            stream_slug,
            error);
    if (recovering_stream == nullptr) {
        return false;
    }
    const ingress::RawWalSinkIdentityV1 open_identity =
        recovering_stream->identity();
    const ingress::RawWalWriterSnapshot open_snapshot =
        recovering_stream->Snapshot();
    if (!open_snapshot.initialized || open_snapshot.sealed ||
        open_snapshot.closed || open_snapshot.fatal ||
        open_snapshot.append != open_snapshot.durable ||
        open_identity.writer_instance != registration.writer_instance ||
        open_identity.segment_sequence !=
            terminal_segment.segment_sequence + 1U) {
        *error = "generation-2 recovered Raw open boundary is invalid";
        return false;
    }

    ingress::RawManifestV1 open_manifest{};
    if (ingress::LoadCurrentRawManifestAt(
            recovering_stream->stream_directory_descriptor(),
            raw_namespace,
            backend.maximum_manifest_bytes,
            &open_manifest,
            nullptr,
            nullptr,
            error,
            &closed_manifest) != ingress::RawManifestStoreError::kNone ||
        !open_manifest.open_entry.has_value()) {
        if (error->empty()) {
            *error = "generation-2 open Raw manifest is invalid";
        }
        return false;
    }
    auto open_segment_bytes = ReadSegmentPrefix(
        recovering_stream->stream_directory_descriptor(),
        open_identity.segment_sequence,
        open_snapshot.durable.segment_offset,
        error);
    if (open_segment_bytes == nullptr ||
        ingress::DecodeSegmentHeaderV1(
            std::span<const std::byte>(
                open_segment_bytes->data(),
                ingress::kRawV1SegmentHeaderBytes),
            &output->open_segment) != ingress::RawV1Error::kNone) {
        return false;
    }

    const ingress::RawManifestSegmentEntryV1& frontier =
        closed_manifest.closed_entries.back();
    ingress::RecoveryMaintenanceNewOpenPredecessorV1 predecessor{};
    predecessor.terminal.kind =
        ingress::RecoveryMaintenancePreviousTerminalKindV1::kSealed;
    predecessor.terminal.marker_bytes = frontier.accepted_marker_bytes;
    predecessor.terminal.marker_sha256 = frontier.accepted_marker_sha256;
    predecessor.terminal.segment_sequence = frontier.segment_sequence;
    predecessor.terminal.segment_sha256 = frontier.segment_sha256;
    predecessor.reopens_sealed_raw_certificate_sha256 =
        sealed_certificate->certificate_sha256();
    std::unique_ptr<ingress::BuiltRecoveryMaintenanceReportV1>
        recovery_report;
    if (ingress::BuildResumedOpenRecoveryMaintenanceReportV1(
            registration.key,
            journal_header,
            recovery_plan,
            recovery_execution,
            analyzed_journal_size,
            open_manifest,
            std::move(open_segment_bytes),
            open_identity,
            open_snapshot,
            &predecessor,
            &recovery_report) !=
            ingress::RecoveryMaintenanceReportV1Error::kNone ||
        recovery_report == nullptr) {
        *error = "cannot build the generation-2 RESUMED_OPEN report";
        return false;
    }
    output->recovery_report_built = true;

    ingress::RawLiveTailPosixAttachGateV1 source_gate{};
    source_gate.writer_instance = open_identity.writer_instance;
    source_gate.stream_day_id = open_identity.stream_day_id;
    source_gate.recovered_durable = recovery_execution.recovered_cursor;
    auto live_tail_source = ingress::OpenRawLiveTailPosixSource(
        recovering_stream->stream_directory_descriptor(),
        open_identity.source_stream_id,
        open_identity.capture_date,
        source_gate,
        {},
        error);
    if (live_tail_source == nullptr) {
        return false;
    }
    ingress::RawLiveTailAttachV1 live_tail_attach{};
    live_tail_attach.writer_instance = open_identity.writer_instance;
    live_tail_attach.stream_day_id = open_identity.stream_day_id;
    live_tail_attach.source_stream_id = open_identity.source_stream_id;
    live_tail_attach.capture_date = open_identity.capture_date;
    live_tail_attach.segment_sequence = open_identity.segment_sequence;
    live_tail_attach.global_wal_pos = open_snapshot.append.global_wal_pos;
    live_tail_attach.segment_offset = open_snapshot.append.segment_offset;
    live_tail_attach.next_ingress_sequence =
        open_snapshot.append.ingress_sequence + 1U;
    auto clean_stop_gate = ingress::CreateRawPosixCleanStopGateV1(
        recovering_stream->lease(),
        coordinator,
        registration.key,
        stream_slug,
        backend.maximum_manifest_bytes,
        error);
    if (clean_stop_gate == nullptr) {
        return false;
    }

    ingress::RawIngressAppConfigV1 app = generation_one.app;
    app.recovered.source_stream_id = open_identity.source_stream_id;
    app.recovered.capture_date = open_identity.capture_date;
    app.recovered.stream_day_id = open_identity.stream_day_id;
    app.recovered.recovered_next_ingress_sequence =
        open_snapshot.append.ingress_sequence + 1U;
    app.recovered.append = {
        open_identity.segment_sequence,
        open_snapshot.append.global_wal_pos,
        open_snapshot.append.ingress_sequence,
        open_snapshot.append.segment_offset};
    app.recovered.durable = {
        open_identity.segment_sequence,
        open_snapshot.durable.global_wal_pos,
        open_snapshot.durable.ingress_sequence,
        open_snapshot.durable.segment_offset};
    app.recovered.writer_instance = open_identity.writer_instance;
    app.recovered.current_segment_sequence = open_identity.segment_sequence;
    app.recovered.clock_epoch_algorithm_version =
        output->open_segment.clock_epoch_algorithm;
    app.recovered.clock_epoch_digest = output->open_segment.clock_epoch_digest;
    app.recovered.clock_epoch_label = output->open_segment.clock_epoch_label;
    app.connect_generation = 2U;

    ingress::RawIngressAppOptionsV1 app_options;
    app_options.tail_consumer_mode =
        ingress::RawIngressTailConsumerModeV1::kExternalAuthoritativeConsumer;
    app_options.callback_quiesce_timeout = std::chrono::seconds(10);
    app_options.sdk_log_runtime_prefix =
        (options.output_directory / "sdk-phase3-live-generation-2").string();
    output->runtime =
        ingress::RawExistingRouteProductionRuntimeFactoryV1::
            ActivateRecoveredClosed(
                std::move(recovering_stream),
                *recovery_report,
                coordinator,
                registration.key,
                stream_slug,
                std::move(app),
                sdk_factory,
                std::make_unique<ingress::LinuxCaptureClock>(),
                std::move(live_tail_source),
                live_tail_attach,
                std::move(clean_stop_gate),
                app_options,
                nullptr);
    if (!output->runtime.ok()) {
        *error = "generation-2 recovered production activation failed";
        return false;
    }
    output->writer_instance = open_identity.writer_instance;
    output->recovery_analyzed_journal_size = analyzed_journal_size;
    output->recovery_retained_journal_size =
        recovery_execution.retained_journal_size;
    output->recovered_from_segment_sequence = terminal_segment.segment_sequence;
    output->opened_segment_sequence = open_identity.segment_sequence;
    output->recovery_plan_clean = !recovery_execution.mutated;
    return true;
}

struct RestartValidation final {
    bool checkpoint_loaded = false;
    bool boundary_found = false;
    bool decoder_restored = false;
    std::uint64_t raw_records = 0U;
    std::uint64_t append_wal = 0U;
    std::uint64_t durable_wal = 0U;
};

RestartValidation ValidateRestart(
    const OutputDirectories& directories,
    const PreparedRuntimeInputs& inputs,
    const control::ControlDecoderSnapshotV1& terminal_decoder,
    std::string* error) {
    RestartValidation result;
    const auto slug = ingress::CanonicalRawStreamSlugV1(
        inputs.segment.source_stream_id);
    if (!slug.has_value()) {
        *error = "canonical Raw stream slug is unavailable";
        return result;
    }
    auto stream = ingress::OpenExistingRawStreamDirectory(
        directories.raw_path.string(),
        inputs.segment.source_stream_id,
        inputs.segment.capture_date,
        std::string(*slug),
        error);
    if (stream == nullptr) {
        return result;
    }
    auto control_reader = ingress::OpenRawControlFile(
        stream->descriptor(),
        inputs.segment.source_stream_id,
        inputs.segment.capture_date,
        error);
    ingress::RawControlSnapshot raw_control{};
    if (control_reader == nullptr ||
        !control_reader->Read(&raw_control, nullptr)) {
        *error = "cannot read terminal Raw control for restart";
        return result;
    }
    result.append_wal = raw_control.append_global_wal_pos;
    result.durable_wal = raw_control.durable_global_wal_pos;
    control::ControlCheckpointPosixLoadResultV1 loaded =
        control::LoadLatestControlCheckpointV1At(
            directories.checkpoint.get(), raw_control, error);
    if (!loaded.ok() || !loaded.checkpoint.has_value()) {
        return result;
    }
    result.checkpoint_loaded = true;
    std::vector<ingress::RawSegmentScanResult> scans;
    try {
        scans.reserve(raw_control.segment_sequence);
    } catch (...) {
        *error = "cannot reserve restart Raw scan chain";
        return result;
    }
    std::uint64_t expected_base_wal = 0U;
    std::uint64_t expected_first_ingress = 1U;
    for (std::uint32_t sequence = 1U;
         sequence <= raw_control.segment_sequence;
         ++sequence) {
        auto bytes = ReadSegment(
            stream->descriptor(), sequence, kSegmentTargetBytes, error);
        if (bytes == nullptr) {
            return result;
        }
        ingress::RawSegmentScanResult scan =
            ingress::ScanRawSegmentV1(bytes, bytes->size());
        if (!scan.ok() || scan.segment.segment_sequence != sequence ||
            scan.segment.segment_base_wal_pos != expected_base_wal ||
            scan.segment.first_ingress_sequence != expected_first_ingress) {
            *error = "terminal Raw segment chain failed restart validation";
            return result;
        }
        if (scan.records.size() >
            std::numeric_limits<std::uint64_t>::max() - result.raw_records) {
            *error = "terminal Raw record count overflow";
            return result;
        }
        result.raw_records += scan.records.size();
        expected_base_wal = scan.validated_end_wal_pos;
        expected_first_ingress = result.raw_records + 1U;
        scans.push_back(std::move(scan));
    }
    if (expected_base_wal != raw_control.append_global_wal_pos ||
        result.raw_records != raw_control.append_ingress_sequence) {
        *error = "sealed Raw scan chain does not reach terminal control";
        return result;
    }
    const control::ControlDecoderSnapshotV1& checkpoint_state =
        loaded.checkpoint->state;
    const ingress::RawSegmentScanResult* boundary_scan = nullptr;
    const ingress::RawRecordView* boundary_view = nullptr;
    for (const ingress::RawSegmentScanResult& scan : scans) {
        const auto match = std::find_if(
            scan.records.begin(), scan.records.end(),
            [&checkpoint_state](const ingress::RawRecordView& view) {
                return view.header().ingress_sequence ==
                           checkpoint_state.processed_ingress_sequence &&
                       view.record_end_wal_pos() ==
                           checkpoint_state.processed_record_end_wal_pos;
            });
        if (match != scan.records.end()) {
            boundary_scan = &scan;
            boundary_view = &*match;
            break;
        }
    }
    if (boundary_scan == nullptr || boundary_view == nullptr) {
        *error = "checkpoint boundary is absent from sealed Raw";
        return result;
    }
    result.boundary_found = true;
    ingress::RawReplaySegmentContext context;
    context.source_stream_id = boundary_scan->segment.source_stream_id;
    context.capture_date = boundary_scan->segment.capture_date;
    context.stream_day_id = boundary_scan->segment.stream_day_id;
    context.segment_sequence = boundary_scan->segment.segment_sequence;
    context.segment_base_wal_pos =
        boundary_scan->segment.segment_base_wal_pos;
    context.config_sha256 = boundary_scan->segment.config_sha256;
    context.raw_schema_sha256 = boundary_scan->segment.raw_schema_sha256;
    context.clock_epoch.algorithm =
        boundary_scan->segment.clock_epoch_algorithm;
    context.clock_epoch.digest = boundary_scan->segment.clock_epoch_digest;
    context.clock_epoch.label = boundary_scan->segment.clock_epoch_label;
    ingress::RawReplayRecord boundary{
        *boundary_view, context, ingress::RawReplayProvenance::kDurable};

    control::ControlDecoderConfigV1 decoder_config;
    decoder_config.source_stream_id = inputs.segment.source_stream_id;
    decoder_config.capture_date = inputs.segment.capture_date;
    decoder_config.stream_day_id = inputs.segment.stream_day_id;
    decoder_config.stable_config_sha256 = inputs.segment.config_sha256;
    decoder_config.required =
        sdk::GetIngressSpec(inputs.app.stable.kind).required;
    if (inputs.app.stable.include_optional_index) {
        decoder_config.optional =
            sdk::GetIngressSpec(inputs.app.stable.kind).optional;
    }
    std::unique_ptr<control::ControlDecoderV1> restored;
    const control::ControlDecoderCreateErrorV1 restore =
        control::ControlDecoderV1::Restore(
            std::move(decoder_config),
            *loaded.checkpoint,
            boundary,
            raw_control,
            &restored);
    if (restore != control::ControlDecoderCreateErrorV1::kNone ||
        restored == nullptr) {
        *error = "checkpoint decoder restore rejected the real Raw boundary";
        return result;
    }
    const control::ControlDecoderSnapshotV1 restored_state =
        restored->Snapshot();
    result.decoder_restored =
        restored_state.state_sha256 == terminal_decoder.state_sha256 &&
        restored_state.processed_ingress_sequence ==
            terminal_decoder.processed_ingress_sequence &&
        restored_state.processed_record_end_wal_pos ==
            terminal_decoder.processed_record_end_wal_pos &&
        restored_state.counters == terminal_decoder.counters;
    if (!result.decoder_restored) {
        *error = "restored decoder differs from the terminal live decoder";
    }
    return result;
}

int Run(const Options& options) {
    const std::string manifest_error = sdk::ValidateIngressSpecs();
    if (!manifest_error.empty()) {
        std::cerr << "phase3-live: invalid subscription manifest: "
                  << manifest_error << '\n';
        return 1;
    }
    OutputDirectories directories;
    std::string error;
    if (!CreateOutputDirectories(options, &directories, &error)) {
        std::cerr << "phase3-live: " << error << '\n';
        return 1;
    }
    const std::string endpoint_bytes = EndpointBytes(options);
    const std::string endpoint_sha256 = common::Sha256Hex(
        common::ComputeSha256(endpoint_bytes));
    const auto endpoint = sdk::VerifyEndpointContractBytes(
        endpoint_bytes,
        endpoint_sha256,
        options.ingress_kind,
        &error);
    if (endpoint == nullptr) {
        std::cerr << "phase3-live: " << error << '\n';
        return 1;
    }
    std::shared_ptr<sdk::SdkFactory> sdk_factory =
        sdk::LoadApprovedSdkFactory(options.library, &error);
    if (sdk_factory == nullptr) {
        std::cerr << "phase3-live: " << error << '\n';
        return 1;
    }
    PreparedRuntimeInputs inputs;
    if (!PrepareRuntimeInputs(
            options,
            directories,
            endpoint,
            endpoint_sha256,
            &inputs,
            &error)) {
        std::cerr << "phase3-live: " << error << '\n';
        return 1;
    }

    const ingress::ReserveCoordinatorStateV1 bootstrap =
        MakeCoordinatorState(directories.raw.get());
    ingress::RawReserveCoordinatorErrorV1 coordinator_error =
        ingress::RawReserveCoordinatorErrorV1::kNone;
    auto coordinator =
        ingress::PublishFreshRawReserveRegistryCoordinatorAtV1(
            directories.raw.get(),
            MakeCoordinatorMarker(bootstrap),
            bootstrap,
            &coordinator_error,
            &error);
    if (coordinator == nullptr ||
        coordinator->RegisterFreshScaffolding(
            inputs.registration, &error) !=
            ingress::RawReserveCoordinatorErrorV1::kNone) {
        std::cerr << "phase3-live: coordinator setup failed: "
                  << error << '\n';
        return 1;
    }

    ingress::RawIngressAppOptionsV1 app_options;
    app_options.tail_consumer_mode =
        ingress::RawIngressTailConsumerModeV1::
            kExternalAuthoritativeConsumer;
    app_options.callback_quiesce_timeout = std::chrono::seconds(10);
    app_options.sdk_log_runtime_prefix =
        (options.output_directory / "sdk-phase3-live").string();
    ingress::RawProductionRuntimeBuildResultV1 runtime =
        ingress::RawExistingRouteProductionRuntimeFactoryV1::
            ActivateFreshRegistered(
                directories.raw_path.string(),
                KindSlug(options.ingress_kind),
                inputs.registration,
                *coordinator,
                inputs.writer,
                inputs.backend,
                inputs.artifacts,
                inputs.stream_limits,
                inputs.app,
                sdk_factory,
                std::make_unique<ingress::LinuxCaptureClock>(),
                {},
                app_options,
                nullptr,
                &error);
    if (!runtime.ok()) {
        std::cerr << "phase3-live: Raw production activation failed: "
                  << error << " (failure="
                  << static_cast<unsigned int>(runtime.failure)
                  << ", fresh="
                  << static_cast<unsigned int>(runtime.fresh_route_failure)
                  << ", fail_stop="
                  << (runtime.requires_fail_stop() ? "true" : "false")
                  << ")\n";
        return 1;
    }
    std::unique_ptr<control::ControlRecordPosixSinkV1> sink;
    const control::ControlRecordPosixSinkCreateErrorV1 sink_error =
        control::CreateControlRecordPosixSinkV1At(
            directories.derived.get(), {}, &sink, &error);
    if (sink_error !=
            control::ControlRecordPosixSinkCreateErrorV1::kNone ||
        sink == nullptr) {
        std::cerr << "phase3-live: derived sink creation failed: "
                  << error << '\n';
        return 1;
    }

    const sdk::IngressSpec& spec = sdk::GetIngressSpec(options.ingress_kind);
    control::ControlProductionControllerConfigV1 controller_config;
    controller_config.decoder.source_stream_id = inputs.segment.source_stream_id;
    controller_config.decoder.capture_date = inputs.segment.capture_date;
    controller_config.decoder.stream_day_id = inputs.segment.stream_day_id;
    controller_config.decoder.stable_config_sha256 =
        inputs.segment.config_sha256;
    controller_config.decoder.required = spec.required;
    if (inputs.app.stable.include_optional_index) {
        controller_config.decoder.optional = spec.optional;
    }
    controller_config.worker.writer_instance = inputs.writer.writer_instance;
    controller_config.worker.connect_generation = inputs.app.connect_generation;
    controller_config.worker.record_publish_timeout_ns =
        UINT64_C(2'000'000'000);
    controller_config.worker.final_catch_up_timeout_ns =
        UINT64_C(300'000'000'000);
    controller_config.startup_timeout_ns = UINT64_C(10'000'000'000);
    controller_config.checkpoint_directory_fd = directories.checkpoint.get();
    controller_config.replay_limits.max_segment_bytes = kSegmentTargetBytes;
    controller_config.replay_limits.max_total_bytes =
        UINT64_C(4) * kSegmentTargetBytes;

    std::unique_ptr<control::ControlProductionControllerV1> controller;
    const control::ControlProductionControllerCreateErrorV1 create =
        control::ControlProductionControllerV1::Create(
            controller_config,
            std::move(runtime.runtime),
            std::move(sink),
            &controller,
            &error);
    if (create !=
            control::ControlProductionControllerCreateErrorV1::kNone ||
        controller == nullptr) {
        std::cerr << "phase3-live: controller preparation failed: "
                  << error << " (create="
                  << static_cast<unsigned int>(create) << ")\n";
        return 1;
    }
    std::cerr
        << "{\"stage\":\"prepared\",\"pre_connect_replay\":true,"
           "\"checkpoint_discovery\":true,\"authoritative_tail\":true}\n"
        << std::flush;
    if (!controller->Initialize(&error)) {
        std::cerr << "phase3-live: SDK/controller initialization failed: "
                  << error << '\n';
        return 1;
    }
    std::cerr
        << "{\"stage\":\"connected\",\"phase3_worker_pre_connect\":true,"
           "\"sdk_generation\":1}\n"
        << std::flush;

    control::ControlReadinessGateConfigV1 readiness_config;
    readiness_config.maximum_decoder_lag_bytes =
        UINT64_C(128) * kMebibyte;
    readiness_config.maximum_durability_lag_bytes =
        UINT64_C(128) * kMebibyte;
    readiness_config.heartbeat_timeout_ns = UINT64_C(10'000'000'000);
    bool ready_seen = false;
    for (std::uint32_t second = 1U;
         second <= options.monitor_seconds && g_stop_requested == 0;
         ++second) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        const control::ControlProductionReadinessSampleV1 readiness =
            controller->EvaluateReadiness(
                readiness_config,
                std::nullopt,
                ClockNow(CLOCK_MONOTONIC),
                ClockNow(CLOCK_REALTIME));
        const control::ControlDecoderSnapshotV1 decoder =
            controller->DecoderSnapshot();
        const control::ControlProductionControllerSnapshotV1 snapshot =
            controller->Snapshot();
        ready_seen = ready_seen || readiness.gate.ready;
        std::cerr
            << "{\"stage\":\"monitor\",\"sdk_generation\":1,\"second\":"
            << second
            << ",\"ready\":" << (readiness.gate.ready ? "true" : "false")
            << ",\"ready_reason\":"
            << static_cast<unsigned int>(readiness.gate.reason)
            << ",\"processed_records\":"
            << decoder.counters.processed_records
            << ",\"emitted_control_records\":"
            << decoder.counters.emitted_control_records
            << ",\"logon_success\":"
            << decoder.counters.logon_success
            << ",\"subscription_responses\":"
            << decoder.counters.subscription_responses
            << ",\"required_first_seen_mask\":"
            << decoder.required_first_seen_mask
            << ",\"processed_ingress_sequence\":"
            << decoder.processed_ingress_sequence
            << ",\"decoder_lag_bytes\":"
            << readiness.gate.decoder_lag_bytes
            << ",\"durability_lag_bytes\":"
            << readiness.gate.durability_lag_bytes
            << ",\"controller_state\":"
            << static_cast<unsigned int>(snapshot.state) << "}\n"
            << std::flush;
        if (snapshot.state ==
                control::ControlProductionControllerStateV1::kFailed) {
            break;
        }
    }

    std::string generation_one_stop_error;
    const bool generation_one_stopped =
        controller->Stop(&generation_one_stop_error);
    const control::ControlProductionControllerSnapshotV1
        generation_one_snapshot =
        controller->Snapshot();
    const control::ControlDecoderSnapshotV1 generation_one_decoder =
        controller->DecoderSnapshot();
    std::string generation_one_restart_error;
    RestartValidation generation_one_restart = ValidateRestart(
        directories,
        inputs,
        generation_one_decoder,
        &generation_one_restart_error);
    const std::uint64_t generation_one_derived_files =
        CountRegularFiles(directories.derived_path);
    const std::uint64_t generation_one_checkpoint_files =
        CountRegularFiles(directories.checkpoint_path);
    const std::uint64_t required_mask = spec.required.size() >= 64U
        ? std::numeric_limits<std::uint64_t>::max()
        : (UINT64_C(1) << spec.required.size()) - 1U;
    const bool generation_one_exact =
        generation_one_restart.raw_records ==
            generation_one_decoder.counters.processed_records &&
        generation_one_decoder.processed_ingress_sequence ==
            generation_one_restart.raw_records &&
        generation_one_restart.append_wal ==
            generation_one_restart.durable_wal &&
        generation_one_restart.append_wal ==
            generation_one_decoder.processed_record_end_wal_pos;
    const bool generation_one_passed =
        generation_one_stopped && ready_seen &&
        generation_one_snapshot.state ==
            control::ControlProductionControllerStateV1::kStopped &&
        generation_one_snapshot.worker_failure ==
            control::ControlLiveWorkerFailureV1::kNone &&
        generation_one_snapshot.checkpoint_publication_attempted &&
        generation_one_snapshot.checkpoint_published &&
        generation_one_decoder.counters.logon_success >= 1U &&
        generation_one_decoder.counters.emitted_control_records >= 1U &&
        (generation_one_decoder.required_first_seen_mask & required_mask) ==
            required_mask &&
        generation_one_decoder.decoder_evidence_ready &&
        !generation_one_decoder.poisoned &&
        generation_one_derived_files >= 1U &&
        generation_one_checkpoint_files >= 1U &&
        generation_one_restart.checkpoint_loaded &&
        generation_one_restart.boundary_found &&
        generation_one_restart.decoder_restored &&
        generation_one_exact;
    std::cerr
        << "{\"stage\":\"generation_stopped\",\"sdk_generation\":1,"
           "\"passed\":"
        << (generation_one_passed ? "true" : "false")
        << ",\"raw_records\":" << generation_one_restart.raw_records
        << ",\"phase3_processed_records\":"
        << generation_one_decoder.counters.processed_records
        << ",\"checkpoint_published\":"
        << (generation_one_snapshot.checkpoint_published ? "true" : "false")
        << ",\"restart_decoder_restored\":"
        << (generation_one_restart.decoder_restored ? "true" : "false")
        << ",\"worker_failure\":"
        << static_cast<unsigned int>(generation_one_snapshot.worker_failure)
        << ",\"worker_process_error\":"
        << static_cast<unsigned int>(
               generation_one_snapshot.worker_process_error)
        << ",\"worker_live_tail_error\":"
        << static_cast<unsigned int>(
               generation_one_snapshot.worker_live_tail_error)
        << "}\n"
        << std::flush;
    if (!generation_one_passed || g_stop_requested != 0) {
        std::cerr
            << "phase3-live: generation 1 did not establish the required "
               "sealed/checkpoint recovery frontier: "
            << generation_one_stop_error << ' '
            << generation_one_restart_error << '\n';
        return 1;
    }

    // Release the generation-1 controller, derived writer and retained Raw
    // lease before the production recovery path acquires the generation-2
    // writer lease. The feeder process remains untouched.
    controller.reset();

    RecoveredGenerationV1 recovered_generation;
    std::string recovery_error;
    if (!PrepareRecoveredGeneration(
            options,
            directories,
            inputs,
            *coordinator,
            sdk_factory,
            &recovered_generation,
            &recovery_error)) {
        std::cerr
            << "phase3-live: generation-2 production recovery failed: "
            << recovery_error << " (runtime_failure="
            << static_cast<unsigned int>(
                   recovered_generation.runtime.failure)
            << ", report_error="
            << static_cast<unsigned int>(
                   recovered_generation.runtime.recovery_report_error)
            << ", coordinator_error="
            << static_cast<unsigned int>(
                   recovered_generation.runtime.coordinator_error)
            << ")\n";
        return 1;
    }
    std::cerr
        << "{\"stage\":\"recovered_active\",\"sdk_generation\":2,"
           "\"recovery_plan_clean\":"
        << (recovered_generation.recovery_plan_clean ? "true" : "false")
        << ",\"recovery_report_built\":"
        << (recovered_generation.recovery_report_built ? "true" : "false")
        << ",\"recovered_from_segment\":"
        << recovered_generation.recovered_from_segment_sequence
        << ",\"opened_segment\":"
        << recovered_generation.opened_segment_sequence
        << ",\"analyzed_journal_size\":"
        << recovered_generation.recovery_analyzed_journal_size
        << ",\"retained_journal_size\":"
        << recovered_generation.recovery_retained_journal_size
        << "}\n"
        << std::flush;

    std::unique_ptr<control::ControlRecordPosixSinkV1> generation_two_sink;
    std::string generation_two_error;
    const control::ControlRecordPosixSinkCreateErrorV1
        generation_two_sink_error =
            control::CreateControlRecordPosixSinkV1At(
                directories.derived.get(),
                {},
                &generation_two_sink,
                &generation_two_error);
    if (generation_two_sink_error !=
            control::ControlRecordPosixSinkCreateErrorV1::kNone ||
        generation_two_sink == nullptr) {
        std::cerr
            << "phase3-live: generation-2 derived sink creation failed: "
            << generation_two_error << '\n';
        return 1;
    }
    control::ControlProductionControllerConfigV1 generation_two_config =
        controller_config;
    generation_two_config.worker.writer_instance =
        recovered_generation.writer_instance;
    generation_two_config.worker.connect_generation = 2U;
    std::unique_ptr<control::ControlProductionControllerV1>
        generation_two_controller;
    const control::ControlProductionControllerCreateErrorV1
        generation_two_create =
            control::ControlProductionControllerV1::Create(
                generation_two_config,
                std::move(recovered_generation.runtime.runtime),
                std::move(generation_two_sink),
                &generation_two_controller,
                &generation_two_error);
    if (generation_two_create !=
            control::ControlProductionControllerCreateErrorV1::kNone ||
        generation_two_controller == nullptr) {
        std::cerr
            << "phase3-live: generation-2 controller preparation failed: "
            << generation_two_error << " (create="
            << static_cast<unsigned int>(generation_two_create) << ")\n";
        return 1;
    }
    const control::ControlProductionControllerSnapshotV1
        generation_two_prepared = generation_two_controller->Snapshot();
    std::cerr
        << "{\"stage\":\"prepared\",\"sdk_generation\":2,"
           "\"pre_connect_replay\":true,\"checkpoint_loaded\":"
        << (generation_two_prepared.checkpoint_loaded ? "true" : "false")
        << ",\"checkpoint_restored\":"
        << (generation_two_prepared.checkpoint_restored ? "true" : "false")
        << ",\"replayed_suffix_records\":"
        << generation_two_prepared.replayed_records << "}\n"
        << std::flush;
    if (!generation_two_prepared.checkpoint_loaded ||
        !generation_two_prepared.checkpoint_restored ||
        !generation_two_controller->Initialize(&generation_two_error)) {
        std::cerr
            << "phase3-live: generation-2 checkpoint restore/SDK "
               "initialization failed: "
            << generation_two_error << '\n';
        return 1;
    }
    std::cerr
        << "{\"stage\":\"connected\",\"phase3_worker_pre_connect\":true,"
           "\"sdk_generation\":2}\n"
        << std::flush;

    bool generation_two_ready_seen = false;
    for (std::uint32_t second = 1U;
         second <= options.monitor_seconds && g_stop_requested == 0;
         ++second) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        const control::ControlProductionReadinessSampleV1 readiness =
            generation_two_controller->EvaluateReadiness(
                readiness_config,
                std::nullopt,
                ClockNow(CLOCK_MONOTONIC),
                ClockNow(CLOCK_REALTIME));
        const control::ControlDecoderSnapshotV1 decoder =
            generation_two_controller->DecoderSnapshot();
        const control::ControlProductionControllerSnapshotV1 snapshot =
            generation_two_controller->Snapshot();
        generation_two_ready_seen =
            generation_two_ready_seen || readiness.gate.ready;
        std::cerr
            << "{\"stage\":\"monitor\",\"sdk_generation\":2,\"second\":"
            << second
            << ",\"ready\":" << (readiness.gate.ready ? "true" : "false")
            << ",\"ready_reason\":"
            << static_cast<unsigned int>(readiness.gate.reason)
            << ",\"processed_records\":"
            << decoder.counters.processed_records
            << ",\"emitted_control_records\":"
            << decoder.counters.emitted_control_records
            << ",\"logon_success\":" << decoder.counters.logon_success
            << ",\"subscription_responses\":"
            << decoder.counters.subscription_responses
            << ",\"required_first_seen_mask\":"
            << decoder.required_first_seen_mask
            << ",\"processed_ingress_sequence\":"
            << decoder.processed_ingress_sequence
            << ",\"decoder_lag_bytes\":"
            << readiness.gate.decoder_lag_bytes
            << ",\"durability_lag_bytes\":"
            << readiness.gate.durability_lag_bytes
            << ",\"controller_state\":"
            << static_cast<unsigned int>(snapshot.state) << "}\n"
            << std::flush;
        if (snapshot.state ==
            control::ControlProductionControllerStateV1::kFailed) {
            break;
        }
    }

    std::string generation_two_stop_error;
    const bool generation_two_stopped =
        generation_two_controller->Stop(&generation_two_stop_error);
    const control::ControlProductionControllerSnapshotV1 final_snapshot =
        generation_two_controller->Snapshot();
    const control::ControlDecoderSnapshotV1 final_decoder =
        generation_two_controller->DecoderSnapshot();
    std::string restart_error;
    RestartValidation restart = ValidateRestart(
        directories, inputs, final_decoder, &restart_error);
    const std::uint64_t derived_files =
        CountRegularFiles(directories.derived_path);
    const std::uint64_t checkpoint_files =
        CountRegularFiles(directories.checkpoint_path);
    const bool exact_raw_to_phase3 =
        restart.raw_records == final_decoder.counters.processed_records &&
        final_decoder.processed_ingress_sequence == restart.raw_records &&
        restart.append_wal == restart.durable_wal &&
        restart.append_wal == final_decoder.processed_record_end_wal_pos;
    const bool generation_two_live_suffix =
        final_decoder.counters.processed_records >
            generation_one_decoder.counters.processed_records &&
        final_decoder.counters.logon_success >
            generation_one_decoder.counters.logon_success &&
        restart.append_wal > generation_one_restart.append_wal &&
        derived_files > generation_one_derived_files;
    const bool passed =
        generation_one_passed && generation_two_stopped &&
        generation_two_ready_seen && generation_two_live_suffix &&
        final_snapshot.state ==
            control::ControlProductionControllerStateV1::kStopped &&
        final_snapshot.worker_failure ==
            control::ControlLiveWorkerFailureV1::kNone &&
        final_snapshot.worker_process_error ==
            control::ControlProcessErrorV1::kNone &&
        final_snapshot.worker_live_tail_error ==
            ingress::RawLiveTailError::kNone &&
        final_snapshot.checkpoint_loaded &&
        final_snapshot.checkpoint_restored &&
        final_snapshot.checkpoint_publication_attempted &&
        final_snapshot.checkpoint_published &&
        final_decoder.counters.logon_success >= 2U &&
        final_decoder.counters.emitted_control_records >
            generation_one_decoder.counters.emitted_control_records &&
        (final_decoder.required_first_seen_mask & required_mask) ==
            required_mask &&
        final_decoder.decoder_evidence_ready &&
        !final_decoder.poisoned &&
        derived_files > generation_one_derived_files &&
        checkpoint_files > generation_one_checkpoint_files &&
        restart.checkpoint_loaded && restart.boundary_found &&
        restart.decoder_restored && exact_raw_to_phase3;
    std::cout
        << "{\"passed\":" << (passed ? "true" : "false")
        << ",\"ingress_kind\":\"" << KindSlug(options.ingress_kind)
        << "\",\"source_stream_id\":" << spec.source_stream_id
        << ",\"capture_date\":" << options.capture_date
        << ",\"output_directory\":\""
        << options.output_directory.string()
        << "\",\"sdk_generations_tested\":2"
        << ",\"generation_1_passed\":"
        << (generation_one_passed ? "true" : "false")
        << ",\"generation_1_ready_seen\":"
        << (ready_seen ? "true" : "false")
        << ",\"generation_1_raw_records\":"
        << generation_one_restart.raw_records
        << ",\"generation_1_checkpoint_published\":"
        << (generation_one_snapshot.checkpoint_published ? "true" : "false")
        << ",\"generation_2_ready_seen\":"
        << (generation_two_ready_seen ? "true" : "false")
        << ",\"generation_2_checkpoint_loaded\":"
        << (final_snapshot.checkpoint_loaded ? "true" : "false")
        << ",\"generation_2_checkpoint_restored\":"
        << (final_snapshot.checkpoint_restored ? "true" : "false")
        << ",\"generation_2_live_suffix\":"
        << (generation_two_live_suffix ? "true" : "false")
        << ",\"ready_seen\":"
        << (generation_two_ready_seen ? "true" : "false")
        << ",\"raw_records\":" << restart.raw_records
        << ",\"phase3_processed_records\":"
        << final_decoder.counters.processed_records
        << ",\"phase3_emitted_control_records\":"
        << final_decoder.counters.emitted_control_records
        << ",\"logon_success\":"
        << final_decoder.counters.logon_success
        << ",\"subscription_responses\":"
        << final_decoder.counters.subscription_responses
        << ",\"derived_files\":" << derived_files
        << ",\"checkpoint_files\":" << checkpoint_files
        << ",\"checkpoint_published\":"
        << (final_snapshot.checkpoint_published ? "true" : "false")
        << ",\"restart_checkpoint_loaded\":"
        << (restart.checkpoint_loaded ? "true" : "false")
        << ",\"restart_boundary_found\":"
        << (restart.boundary_found ? "true" : "false")
        << ",\"restart_decoder_restored\":"
        << (restart.decoder_restored ? "true" : "false")
        << ",\"append_global_wal_pos\":" << restart.append_wal
        << ",\"durable_global_wal_pos\":" << restart.durable_wal
        << ",\"exact_raw_to_phase3\":"
        << (exact_raw_to_phase3 ? "true" : "false")
        << ",\"worker_failure\":"
        << static_cast<unsigned int>(final_snapshot.worker_failure)
        << ",\"worker_process_error\":"
        << static_cast<unsigned int>(final_snapshot.worker_process_error)
        << ",\"worker_live_tail_error\":"
        << static_cast<unsigned int>(final_snapshot.worker_live_tail_error)
        << ",\"stop_error\":\""
        << (passed ? "" : generation_two_stop_error)
        << "\",\"restart_error\":\""
        << (passed ? "" : restart_error) << "\"}\n";
    return passed ? 0 : 1;
}

}  // namespace

int main(int argc, char* argv[]) {
    Options options;
    bool help = false;
    std::string error;
    if (!ParseOptions(argc, argv, &options, &help, &error)) {
        std::cerr << "mdl-phase3-live-probe: " << error << '\n';
        PrintUsage();
        return 2;
    }
    if (help) {
        PrintUsage();
        return 0;
    }
    struct sigaction action {};
    action.sa_handler = &HandleStopSignal;
    ::sigemptyset(&action.sa_mask);
    if (::sigaction(SIGINT, &action, nullptr) != 0 ||
        ::sigaction(SIGTERM, &action, nullptr) != 0) {
        std::cerr << "mdl-phase3-live-probe: cannot install signal handlers\n";
        return 2;
    }
    try {
        return Run(options);
    } catch (const std::exception& exception) {
        std::cerr << "mdl-phase3-live-probe: fatal: "
                  << exception.what() << '\n';
    } catch (...) {
        std::cerr << "mdl-phase3-live-probe: unknown fatal exception\n";
    }
    return 1;
}
