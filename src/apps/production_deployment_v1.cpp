#include "l2flow/apps/production_deployment_v1.h"

#include "l2flow/apps/production_service_v1.h"
#include "l2flow/build_manifest.h"
#include "l2flow/canonical/canonical_normalizer_v1.h"
#include "l2flow/canonical/canonical_segment_v1.h"
#include "l2flow/canonical/source_frontier_posix_v1.h"
#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/control/control_decoder.h"
#include "l2flow/ingress/clock_epoch.h"
#include "l2flow/ingress/raw_ingress_config.h"
#include "l2flow/ingress/raw_production_runtime.h"
#include "l2flow/ingress/raw_schema.h"
#include "l2flow/market/instrument_history_v1.h"
#include "l2flow/market/instrument_registry_loader_v1.h"
#include "l2flow/ops/credential.h"
#include "l2flow/ops/stable_output_prefix.h"
#include "l2flow/route/production_route_controller_v1.h"
#include "l2flow/route/production_route_v1.h"
#include "l2flow/runtime/production_source_pipeline_v1.h"
#include "l2flow/sdk/endpoint_contract.h"
#include "l2flow/sdk/sdk_runtime.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <limits>
#include <map>
#include <new>
#include <optional>
#include <pthread.h>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace l2flow::apps {
namespace {

constexpr std::array<l2flow::sdk::IngressKind,
                     kProductionDeploymentSourceCountV1>
    kSourceKinds{{
        l2flow::sdk::IngressKind::ShSnapshot,
        l2flow::sdk::IngressKind::ShTick,
        l2flow::sdk::IngressKind::SzSnapshot,
        l2flow::sdk::IngressKind::SzTick,
    }};

constexpr std::array<std::string_view,
                     kProductionDeploymentSourceCountV1>
    kSourceSlugs{{
        "sh-snapshot", "sh-tick", "sz-snapshot", "sz-tick"}};

[[nodiscard]] ProductionDeploymentLoadResultV1 Failure(
    ProductionDeploymentErrorV1 error,
    std::string diagnostic,
    std::size_t line = 0U,
    int system_error_number = 0) noexcept {
    ProductionDeploymentLoadResultV1 result{};
    result.error = error;
    result.system_error_number = system_error_number;
    result.line = line;
    try {
        result.diagnostic = std::move(diagnostic);
    } catch (...) {
    }
    return result;
}

[[nodiscard]] bool HasNul(std::string_view value) noexcept {
    return value.find('\0') != std::string_view::npos;
}

[[nodiscard]] bool IsLowerHex(std::string_view value) noexcept {
    return std::all_of(value.begin(), value.end(), [](char character) {
        return (character >= '0' && character <= '9') ||
               (character >= 'a' && character <= 'f');
    });
}

[[nodiscard]] bool DigestNonzero(
    const l2flow::common::Sha256Digest& value) noexcept {
    return std::any_of(value.begin(), value.end(), [](std::byte byte) {
        return byte != std::byte{0U};
    });
}

[[nodiscard]] bool ValidIdentity(
    const l2flow::common::Identity128& value) noexcept {
    return !l2flow::common::IsZeroIdentity(value);
}

[[nodiscard]] bool ParseDigest(
    std::string_view value,
    l2flow::common::Sha256Digest* output) noexcept {
    if (output == nullptr || value.size() != 64U || !IsLowerHex(value)) {
        return false;
    }
    std::string ignored;
    return l2flow::common::ParseSha256Hex(value, output, &ignored) &&
           DigestNonzero(*output);
}

[[nodiscard]] bool ParseIdentity(
    std::string_view value,
    l2flow::common::Identity128* output) noexcept {
    if (output == nullptr || value.size() != 32U || !IsLowerHex(value)) {
        return false;
    }
    l2flow::common::Identity128 candidate{};
    for (std::size_t index = 0U; index < candidate.size(); ++index) {
        const auto nibble = [](char character) noexcept -> unsigned int {
            return character >= '0' && character <= '9'
                ? static_cast<unsigned int>(character - '0')
                : static_cast<unsigned int>(character - 'a') + 10U;
        };
        candidate[index] = static_cast<std::byte>(
            (nibble(value[index * 2U]) << 4U) |
            nibble(value[index * 2U + 1U]));
    }
    if (!ValidIdentity(candidate)) {
        return false;
    }
    *output = candidate;
    return true;
}

template <typename Integer>
[[nodiscard]] bool ParseUnsigned(
    std::string_view value,
    Integer* output) noexcept {
    static_assert(std::is_unsigned_v<Integer>);
    if (output == nullptr || value.empty() ||
        (value.size() > 1U && value.front() == '0')) {
        return false;
    }
    Integer candidate = 0;
    const auto parsed = std::from_chars(
        value.data(), value.data() + value.size(), candidate, 10);
    if (parsed.ec != std::errc{} ||
        parsed.ptr != value.data() + value.size()) {
        return false;
    }
    *output = candidate;
    return true;
}

[[nodiscard]] bool ParseSize(
    std::string_view value,
    std::size_t* output) noexcept {
    std::uint64_t candidate = 0U;
    if (!ParseUnsigned(value, &candidate) ||
        candidate > static_cast<std::uint64_t>(
                        std::numeric_limits<std::size_t>::max())) {
        return false;
    }
    *output = static_cast<std::size_t>(candidate);
    return true;
}

[[nodiscard]] bool ParseBool(
    std::string_view value,
    bool* output) noexcept {
    if (output == nullptr || (value != "true" && value != "false")) {
        return false;
    }
    *output = value == "true";
    return true;
}

[[nodiscard]] bool ValidCalendarDate(std::uint32_t value) noexcept {
    const std::uint32_t year = value / 10000U;
    const std::uint32_t month = (value / 100U) % 100U;
    const std::uint32_t day = value % 100U;
    if (year < 1992U || month == 0U || month > 12U || day == 0U) {
        return false;
    }
    constexpr std::array<std::uint32_t, 12U> kDays{{
        31U, 28U, 31U, 30U, 31U, 30U,
        31U, 31U, 30U, 31U, 30U, 31U}};
    std::uint32_t maximum = kDays[month - 1U];
    const bool leap =
        (year % 4U == 0U && year % 100U != 0U) || year % 400U == 0U;
    if (month == 2U && leap) {
        maximum = 29U;
    }
    return day <= maximum;
}

[[nodiscard]] constexpr std::uint64_t
MaximumCanonicalCapacityRecordsPerSink(
    std::uint64_t record_bytes) noexcept {
    constexpr std::uint64_t prefix =
        l2flow::canonical::kCanonicalSegmentDataOffsetV1;
    const std::uint64_t uint64_limit =
        (std::numeric_limits<std::uint64_t>::max() - prefix) / record_bytes;
    const std::uint64_t size_limit =
        (static_cast<std::uint64_t>(
             std::numeric_limits<std::size_t>::max()) -
         prefix) /
        record_bytes;
    const std::uint64_t offset_limit =
        (static_cast<std::uint64_t>(std::numeric_limits<off_t>::max()) -
         prefix) /
        record_bytes;
    return std::min({uint64_limit, size_limit, offset_limit});
}

[[nodiscard]] bool ValidAbsoluteNormalizedPath(
    const std::filesystem::path& path) noexcept {
    try {
        return !path.empty() && path.is_absolute() &&
               !HasNul(path.native()) && path.lexically_normal() == path;
    } catch (...) {
        return false;
    }
}

[[nodiscard]] bool ValidSimpleName(std::string_view value) noexcept {
    return !value.empty() && value.size() <= 255U && !HasNul(value) &&
           value != "." && value != ".." &&
           value.find('/') == std::string_view::npos;
}

[[nodiscard]] std::vector<std::string> ExpectedKeys() {
    std::vector<std::string> keys{
        "mode",
        "sdk_library_path",
        "credential_path",
        "credential_name",
        "raw_root",
        "frontier_root",
        "canonical_root",
        "route_root",
        "instrument_registry_file",
        "instrument_registry_version",
        "instrument_registry_sha256",
        "capture_date",
        "trade_date",
        "route_generation",
        "route_previous_generation",
        "route_instance",
        "coordinator_identity",
        "coordinator_device_id",
        "coordinator_quota_sha256",
        "coordinator_mount_sha256",
        "clock_source_config",
        "raw.heartbeat_interval_seconds",
        "raw.heartbeat_timeout_seconds",
        "raw.include_optional_index",
        "raw.max_message_bytes",
        "raw.ring_capacity_bytes",
        "raw.ring_stall_budget_seconds",
        "raw.segment_target_bytes",
        "raw.segment_max_age_seconds",
        "raw.sync_interval_milliseconds",
        "raw.sync_bytes",
        "raw.sparse_index_every_records",
        "raw.sparse_index_every_bytes",
        "raw.reserve_domain_id",
        "raw.reserve_coordinator_socket",
        "raw.reserve_ack_timeout_milliseconds",
        "raw.emergency_reserve_bytes",
        "raw.maximum_manifest_bytes",
        "raw.live.max_segments",
        "raw.live.max_segment_bytes",
        "raw.live.max_journal_markers",
        "raw.live.max_control_reattach_attempts",
        "canonical.snapshot_capacity_records_per_sink",
        "canonical.tick_capacity_records_per_sink",
        "canonical.quality_capacity_records_per_sink",
        "canonical.control_capacity_records_per_sink",
        "history.physical_workers",
        "history.queue_capacity",
        "history.maximum_inflight_per_source",
        "history.chunk_record_capacity",
        "history.maximum_records_per_query",
        "history.maximum_records_per_shard",
        "history.maximum_instruments_per_shard",
        "history.maximum_payload_bytes_per_shard",
        "service.activation_timeout_ms",
        "service.drain_timeout_ms",
        "service.writer_idle_heartbeat_interval_ms",
        "service.writer_heartbeat_timeout_ms",
    };
    constexpr std::array<std::string_view, 13U> suffixes{{
        "kind",
        "stream_slug",
        "endpoint_path",
        "endpoint_sha256",
        "sdk_log_prefix",
        "metrics_path",
        "stream_day_id",
        "recovery_attempt_id",
        "writer_instance",
        "source_generation",
        "canonical_generation",
        "connect_generation",
        "scaffolding_allocation_cap",
    }};
    for (std::size_t source = 0U; source < kSourceKinds.size(); ++source) {
        const std::string prefix = "source." + std::to_string(source) + ".";
        for (std::string_view suffix : suffixes) {
            keys.push_back(prefix + std::string(suffix));
        }
        keys.push_back(prefix + "safe_stop_template_id");
    }
    return keys;
}

[[nodiscard]] bool SameFileVersion(
    const struct stat& left,
    const struct stat& right) noexcept {
    return left.st_dev == right.st_dev && left.st_ino == right.st_ino &&
           left.st_mode == right.st_mode && left.st_uid == right.st_uid &&
           left.st_nlink == right.st_nlink && left.st_size == right.st_size &&
           left.st_mtim.tv_sec == right.st_mtim.tv_sec &&
           left.st_mtim.tv_nsec == right.st_mtim.tv_nsec &&
           left.st_ctim.tv_sec == right.st_ctim.tv_sec &&
           left.st_ctim.tv_nsec == right.st_ctim.tv_nsec;
}

class OwnedFd final {
public:
    OwnedFd() noexcept = default;
    explicit OwnedFd(int value) noexcept : value_(value) {}
    ~OwnedFd() {
        if (value_ >= 0) {
            static_cast<void>(::close(value_));
        }
    }
    OwnedFd(const OwnedFd&) = delete;
    OwnedFd& operator=(const OwnedFd&) = delete;
    OwnedFd(OwnedFd&& other) noexcept
        : value_(std::exchange(other.value_, -1)) {}
    OwnedFd& operator=(OwnedFd&& other) noexcept {
        if (this != &other) {
            if (value_ >= 0) {
                static_cast<void>(::close(value_));
            }
            value_ = std::exchange(other.value_, -1);
        }
        return *this;
    }
    [[nodiscard]] int get() const noexcept { return value_; }
    [[nodiscard]] bool valid() const noexcept { return value_ >= 0; }

private:
    int value_ = -1;
};

[[nodiscard]] std::string SourceKey(
    std::size_t source,
    std::string_view suffix) {
    return "source." + std::to_string(source) + "." + std::string(suffix);
}

}  // namespace

std::string_view ProductionDeploymentErrorNameV1(
    ProductionDeploymentErrorV1 error) noexcept {
    switch (error) {
        case ProductionDeploymentErrorV1::kNone: return "none";
        case ProductionDeploymentErrorV1::kInvalidArgument:
            return "invalid_argument";
        case ProductionDeploymentErrorV1::kInvalidDirectory:
            return "invalid_directory";
        case ProductionDeploymentErrorV1::kUnsafeDirectory:
            return "unsafe_directory";
        case ProductionDeploymentErrorV1::kOpenFailed: return "open_failed";
        case ProductionDeploymentErrorV1::kUnsafeFile: return "unsafe_file";
        case ProductionDeploymentErrorV1::kReadFailed: return "read_failed";
        case ProductionDeploymentErrorV1::kChangedDuringRead:
            return "changed_during_read";
        case ProductionDeploymentErrorV1::kDigestMismatch:
            return "digest_mismatch";
        case ProductionDeploymentErrorV1::kInvalidText: return "invalid_text";
        case ProductionDeploymentErrorV1::kUnknownField:
            return "unknown_field";
        case ProductionDeploymentErrorV1::kDuplicateField:
            return "duplicate_field";
        case ProductionDeploymentErrorV1::kMissingField:
            return "missing_field";
        case ProductionDeploymentErrorV1::kInvalidValue:
            return "invalid_value";
        case ProductionDeploymentErrorV1::kInvalidTopology:
            return "invalid_topology";
        case ProductionDeploymentErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "invalid_production_deployment_error";
}

ProductionDeploymentLoadResultV1 ParseProductionDeploymentManifestV1(
    std::string_view exact_bytes,
    const l2flow::common::Sha256Digest& expected_sha256) noexcept {
    try {
        if (exact_bytes.empty() ||
            exact_bytes.size() > kProductionDeploymentMaximumBytesV1 ||
            HasNul(exact_bytes) || !DigestNonzero(expected_sha256)) {
            return Failure(
                ProductionDeploymentErrorV1::kInvalidArgument,
                "manifest bytes or SHA-256 pin are invalid");
        }
        if (l2flow::common::ComputeSha256(exact_bytes) != expected_sha256) {
            return Failure(
                ProductionDeploymentErrorV1::kDigestMismatch,
                "manifest SHA-256 does not match the deployment pin");
        }
        if (exact_bytes.back() != '\n' || exact_bytes.find('\r') !=
                                                std::string_view::npos) {
            return Failure(
                ProductionDeploymentErrorV1::kInvalidText,
                "manifest must use LF lines and end with LF");
        }

        const std::vector<std::string> expected_vector = ExpectedKeys();
        const std::set<std::string> expected(
            expected_vector.begin(), expected_vector.end());
        std::map<std::string, std::string> fields;
        std::size_t offset = 0U;
        std::size_t line = 1U;
        while (offset < exact_bytes.size()) {
            const std::size_t end = exact_bytes.find('\n', offset);
            if (end == std::string_view::npos || end - offset > 4096U) {
                return Failure(
                    ProductionDeploymentErrorV1::kInvalidText,
                    "manifest line is missing or exceeds 4096 bytes", line);
            }
            const std::string_view text = exact_bytes.substr(offset, end - offset);
            offset = end + 1U;
            if (line == 1U) {
                if (text != kProductionDeploymentMagicV1) {
                    return Failure(
                        ProductionDeploymentErrorV1::kInvalidText,
                        "manifest magic/version is invalid", line);
                }
                ++line;
                continue;
            }
            if (text.empty()) {
                return Failure(
                    ProductionDeploymentErrorV1::kInvalidText,
                    "blank manifest lines are forbidden", line);
            }
            const std::size_t tab = text.find('\t');
            if (tab == std::string_view::npos || tab == 0U ||
                tab + 1U >= text.size() ||
                text.find('\t', tab + 1U) != std::string_view::npos) {
                return Failure(
                    ProductionDeploymentErrorV1::kInvalidText,
                    "each manifest line must contain exactly one TAB", line);
            }
            const std::string key(text.substr(0U, tab));
            const std::string value(text.substr(tab + 1U));
            if (!std::all_of(
                    key.begin(), key.end(), [](unsigned char character) {
                        return (character >= 'a' && character <= 'z') ||
                               (character >= '0' && character <= '9') ||
                               character == '_' || character == '.';
                    }) ||
                !std::all_of(
                    value.begin(), value.end(), [](unsigned char character) {
                        return character >= 0x20U && character != 0x7fU;
                    })) {
                return Failure(
                    ProductionDeploymentErrorV1::kInvalidText,
                    "manifest key/value contains a forbidden byte", line);
            }
            if (!expected.contains(key)) {
                return Failure(
                    ProductionDeploymentErrorV1::kUnknownField,
                    "manifest contains an unknown field", line);
            }
            if (!fields.emplace(key, value).second) {
                return Failure(
                    ProductionDeploymentErrorV1::kDuplicateField,
                    "manifest contains a duplicate field", line);
            }
            ++line;
        }
        for (const std::string& key : expected_vector) {
            if (!fields.contains(key)) {
                return Failure(
                    ProductionDeploymentErrorV1::kMissingField,
                    "manifest is missing required field: " + key);
            }
        }

        const auto value = [&fields](std::string_view key) -> std::string_view {
            return fields.at(std::string(key));
        };
        ProductionDeploymentV1 deployment{};
        deployment.manifest_sha256 = expected_sha256;
        if (value("mode") != "fresh") {
            return Failure(
                ProductionDeploymentErrorV1::kInvalidValue,
                "only mode=fresh is supported");
        }
        deployment.sdk_library_path = value("sdk_library_path");
        deployment.credential_path = value("credential_path");
        deployment.credential_name = value("credential_name");
        deployment.raw_root = value("raw_root");
        deployment.frontier_root = value("frontier_root");
        deployment.canonical_root = value("canonical_root");
        deployment.route_root = value("route_root");
        deployment.instrument_registry_file = value("instrument_registry_file");
        deployment.clock_source_config = value("clock_source_config");
        deployment.reserve_domain_id = value("raw.reserve_domain_id");
        deployment.reserve_coordinator_socket =
            value("raw.reserve_coordinator_socket");

        const auto invalid = [](std::string diagnostic) {
            return Failure(
                ProductionDeploymentErrorV1::kInvalidValue,
                std::move(diagnostic));
        };
        if (!ParseDigest(
                value("instrument_registry_sha256"),
                &deployment.instrument_registry_sha256) ||
            !ParseIdentity(
                value("route_instance"), &deployment.route_instance) ||
            !ParseIdentity(
                value("coordinator_identity"),
                &deployment.coordinator_identity) ||
            !ParseDigest(
                value("coordinator_quota_sha256"),
                &deployment.coordinator_quota_sha256) ||
            !ParseDigest(
                value("coordinator_mount_sha256"),
                &deployment.coordinator_mount_sha256)) {
            return invalid("manifest contains an invalid identity or digest");
        }

#define L2FLOW_PARSE_FIELD(KEY, MEMBER)                                      \
    if (!ParseUnsigned(value(KEY), &deployment.MEMBER)) {                    \
        return invalid("invalid unsigned decimal field: " KEY);             \
    }
#define L2FLOW_PARSE_SIZE_FIELD(KEY, MEMBER)                                 \
    if (!ParseSize(value(KEY), &deployment.MEMBER)) {                        \
        return invalid("invalid size field: " KEY);                         \
    }
        L2FLOW_PARSE_FIELD("instrument_registry_version",
                           instrument_registry_version)
        L2FLOW_PARSE_FIELD("capture_date", capture_date)
        L2FLOW_PARSE_FIELD("trade_date", trade_date)
        L2FLOW_PARSE_FIELD("route_generation", route_generation)
        L2FLOW_PARSE_FIELD("route_previous_generation",
                           route_previous_generation)
        L2FLOW_PARSE_FIELD("coordinator_device_id", coordinator_device_id)
        L2FLOW_PARSE_FIELD("raw.heartbeat_interval_seconds",
                           raw_heartbeat_interval_seconds)
        L2FLOW_PARSE_FIELD("raw.heartbeat_timeout_seconds",
                           raw_heartbeat_timeout_seconds)
        if (!ParseBool(
                value("raw.include_optional_index"),
                &deployment.raw_include_optional_index)) {
            return invalid("invalid boolean field: raw.include_optional_index");
        }
        if (deployment.raw_include_optional_index) {
            return invalid(
                "raw.include_optional_index must be false: the production "
                "source pipeline does not consume optional index messages");
        }
        L2FLOW_PARSE_FIELD("raw.max_message_bytes", raw_max_message_bytes)
        L2FLOW_PARSE_FIELD("raw.ring_capacity_bytes", raw_ring_capacity_bytes)
        L2FLOW_PARSE_FIELD("raw.ring_stall_budget_seconds",
                           raw_ring_stall_budget_seconds)
        L2FLOW_PARSE_FIELD("raw.segment_target_bytes", raw_segment_target_bytes)
        L2FLOW_PARSE_FIELD("raw.segment_max_age_seconds",
                           raw_segment_max_age_seconds)
        L2FLOW_PARSE_FIELD("raw.sync_interval_milliseconds",
                           raw_sync_interval_milliseconds)
        L2FLOW_PARSE_FIELD("raw.sync_bytes", raw_sync_bytes)
        L2FLOW_PARSE_FIELD("raw.sparse_index_every_records",
                           raw_sparse_index_every_records)
        L2FLOW_PARSE_FIELD("raw.sparse_index_every_bytes",
                           raw_sparse_index_every_bytes)
        L2FLOW_PARSE_FIELD("raw.reserve_ack_timeout_milliseconds",
                           reserve_ack_timeout_milliseconds)
        L2FLOW_PARSE_FIELD("raw.emergency_reserve_bytes", emergency_reserve_bytes)
        L2FLOW_PARSE_SIZE_FIELD("raw.maximum_manifest_bytes",
                                raw_maximum_manifest_bytes)
        L2FLOW_PARSE_FIELD("raw.live.max_segments", raw_live_max_segments)
        L2FLOW_PARSE_FIELD("raw.live.max_segment_bytes",
                           raw_live_max_segment_bytes)
        L2FLOW_PARSE_SIZE_FIELD("raw.live.max_journal_markers",
                                raw_live_max_journal_markers)
        L2FLOW_PARSE_FIELD("raw.live.max_control_reattach_attempts",
                           raw_live_max_control_reattach_attempts)
        L2FLOW_PARSE_FIELD("canonical.snapshot_capacity_records_per_sink",
                           canonical_snapshot_capacity_records_per_sink)
        L2FLOW_PARSE_FIELD("canonical.tick_capacity_records_per_sink",
                           canonical_tick_capacity_records_per_sink)
        L2FLOW_PARSE_FIELD("canonical.quality_capacity_records_per_sink",
                           canonical_quality_capacity_records_per_sink)
        L2FLOW_PARSE_FIELD("canonical.control_capacity_records_per_sink",
                           canonical_control_capacity_records_per_sink)
        L2FLOW_PARSE_FIELD("history.physical_workers", history_physical_workers)
        L2FLOW_PARSE_SIZE_FIELD("history.queue_capacity",
                                history_queue_capacity)
        L2FLOW_PARSE_SIZE_FIELD("history.maximum_inflight_per_source",
                                history_maximum_inflight_per_source)
        L2FLOW_PARSE_SIZE_FIELD("history.chunk_record_capacity",
                                history_chunk_record_capacity)
        L2FLOW_PARSE_SIZE_FIELD("history.maximum_records_per_query",
                                history_maximum_records_per_query)
        L2FLOW_PARSE_FIELD("history.maximum_records_per_shard",
                           history_maximum_records_per_shard)
        L2FLOW_PARSE_FIELD("history.maximum_instruments_per_shard",
                           history_maximum_instruments_per_shard)
        L2FLOW_PARSE_FIELD("history.maximum_payload_bytes_per_shard",
                           history_maximum_payload_bytes_per_shard)
        L2FLOW_PARSE_FIELD("service.activation_timeout_ms", activation_timeout_ms)
        L2FLOW_PARSE_FIELD("service.drain_timeout_ms", drain_timeout_ms)
        L2FLOW_PARSE_FIELD("service.writer_idle_heartbeat_interval_ms",
                           writer_idle_heartbeat_interval_ms)
        L2FLOW_PARSE_FIELD("service.writer_heartbeat_timeout_ms",
                           writer_heartbeat_timeout_ms)
#undef L2FLOW_PARSE_SIZE_FIELD
#undef L2FLOW_PARSE_FIELD

        for (std::size_t source = 0U; source < deployment.sources.size();
             ++source) {
            ProductionDeploymentSourceV1& target = deployment.sources[source];
            target.kind = kSourceKinds[source];
            const auto source_value = [&value, source](std::string_view suffix) {
                return value(SourceKey(source, suffix));
            };
            if (source_value("kind") !=
                    l2flow::sdk::ToString(kSourceKinds[source]) ||
                source_value("stream_slug") != kSourceSlugs[source]) {
                return Failure(
                    ProductionDeploymentErrorV1::kInvalidTopology,
                    "source slot kind/slug does not match the fixed topology");
            }
            target.stream_slug = source_value("stream_slug");
            target.endpoint_path = source_value("endpoint_path");
            target.sdk_log_prefix = source_value("sdk_log_prefix");
            target.metrics_path = source_value("metrics_path");
            if (!ParseDigest(
                    source_value("endpoint_sha256"),
                    &target.endpoint_sha256) ||
                !ParseIdentity(
                    source_value("stream_day_id"),
                    &target.stream_day_id) ||
                !ParseIdentity(
                    source_value("recovery_attempt_id"),
                    &target.recovery_attempt_id) ||
                !ParseIdentity(
                    source_value("writer_instance"),
                    &target.writer_instance) ||
                !ParseUnsigned(
                    source_value("source_generation"),
                    &target.source_generation) ||
                !ParseUnsigned(
                    source_value("canonical_generation"),
                    &target.canonical_generation) ||
                !ParseUnsigned(
                    source_value("connect_generation"),
                    &target.connect_generation) ||
                !ParseUnsigned(
                    source_value("scaffolding_allocation_cap"),
                    &target.scaffolding_allocation_cap) ||
                !ParseUnsigned(
                    source_value("safe_stop_template_id"),
                    &target.safe_stop_template_id)) {
                return invalid("source field contains an invalid value");
            }
        }

        const std::array<std::filesystem::path, 9U> absolute_paths{{
            deployment.sdk_library_path,
            deployment.credential_path,
            deployment.raw_root,
            deployment.frontier_root,
            deployment.canonical_root,
            deployment.route_root,
            deployment.sources[0U].endpoint_path,
            deployment.sources[1U].endpoint_path,
            deployment.sources[2U].endpoint_path,
        }};
        for (const auto& path : absolute_paths) {
            if (!ValidAbsoluteNormalizedPath(path)) {
                return invalid("manifest path is not normalized and absolute");
            }
        }
        if (!ValidAbsoluteNormalizedPath(deployment.sources[3U].endpoint_path) ||
            !ValidSimpleName(deployment.instrument_registry_file) ||
            deployment.credential_name.empty() ||
            deployment.credential_name.size() > 255U ||
            HasNul(deployment.credential_name) ||
            deployment.clock_source_config !=
                kProductionClockSourceConfigV1 ||
            deployment.reserve_domain_id.empty() ||
            deployment.reserve_coordinator_socket.empty() ||
            !ValidAbsoluteNormalizedPath(deployment.reserve_coordinator_socket)) {
            return invalid(
                "manifest contains an invalid name, path, or fixed clock source");
        }
        for (const auto& source : deployment.sources) {
            if (!ValidAbsoluteNormalizedPath(source.sdk_log_prefix) ||
                !ValidAbsoluteNormalizedPath(source.metrics_path)) {
                return invalid("source log/metrics path is not normalized and absolute");
            }
        }

        if (!ValidCalendarDate(deployment.capture_date) ||
            !ValidCalendarDate(deployment.trade_date) ||
            deployment.instrument_registry_version == 0U ||
            deployment.route_generation != 1U ||
            deployment.route_previous_generation != 0U ||
            deployment.coordinator_device_id == 0U ||
            deployment.raw_heartbeat_interval_seconds == 0U ||
            deployment.raw_heartbeat_timeout_seconds <=
                deployment.raw_heartbeat_interval_seconds ||
            deployment.raw_max_message_bytes <= 23U ||
            deployment.raw_ring_capacity_bytes == 0U ||
            deployment.raw_ring_stall_budget_seconds == 0U ||
            deployment.raw_segment_target_bytes == 0U ||
            deployment.raw_segment_max_age_seconds == 0U ||
            deployment.raw_sync_interval_milliseconds == 0U ||
            deployment.raw_sync_bytes == 0U ||
            deployment.raw_sparse_index_every_records == 0U ||
            deployment.raw_sparse_index_every_records >
                l2flow::ingress::kRawIndexV1DefaultRecordInterval ||
            deployment.raw_sparse_index_every_bytes == 0U ||
            deployment.raw_sparse_index_every_bytes >
                l2flow::ingress::kRawIndexV1DefaultRawBytesInterval ||
            deployment.reserve_ack_timeout_milliseconds == 0U ||
            deployment.emergency_reserve_bytes == 0U ||
            deployment.raw_maximum_manifest_bytes == 0U ||
            deployment.raw_live_max_segments == 0U ||
            deployment.raw_live_max_segments >
                l2flow::ingress::kRawLiveTailPosixAbsoluteMaxSegments ||
            deployment.raw_live_max_segment_bytes <
                deployment.raw_segment_target_bytes ||
            deployment.raw_live_max_journal_markers == 0U ||
            deployment.raw_live_max_control_reattach_attempts == 0U ||
            deployment.canonical_snapshot_capacity_records_per_sink == 0U ||
            deployment.canonical_snapshot_capacity_records_per_sink >
                MaximumCanonicalCapacityRecordsPerSink(
                    l2flow::canonical::kCanonicalSnapshotRecordBytesV1) ||
            deployment.canonical_tick_capacity_records_per_sink == 0U ||
            deployment.canonical_tick_capacity_records_per_sink >
                MaximumCanonicalCapacityRecordsPerSink(
                    l2flow::canonical::kCanonicalTickRecordBytesV1) ||
            deployment.canonical_quality_capacity_records_per_sink == 0U ||
            deployment.canonical_quality_capacity_records_per_sink >
                MaximumCanonicalCapacityRecordsPerSink(
                    l2flow::canonical::kCanonicalQualityRecordBytesV1) ||
            deployment.canonical_control_capacity_records_per_sink == 0U ||
            deployment.canonical_control_capacity_records_per_sink >
                MaximumCanonicalCapacityRecordsPerSink(
                    l2flow::canonical::kCanonicalControlRecordBytesV1) ||
            deployment.history_physical_workers == 0U ||
            deployment.history_physical_workers > 16U ||
            deployment.history_queue_capacity == 0U ||
            deployment.history_queue_capacity ==
                std::numeric_limits<std::size_t>::max() ||
            deployment.history_maximum_inflight_per_source == 0U ||
            deployment.history_chunk_record_capacity == 0U ||
            deployment.history_maximum_records_per_query == 0U ||
            deployment.history_maximum_records_per_query ==
                std::numeric_limits<std::size_t>::max() ||
            deployment.history_maximum_records_per_shard == 0U ||
            deployment.history_maximum_instruments_per_shard == 0U ||
            deployment.history_maximum_payload_bytes_per_shard == 0U ||
            deployment.activation_timeout_ms == 0U ||
            deployment.activation_timeout_ms > 600'000U ||
            deployment.drain_timeout_ms == 0U ||
            deployment.drain_timeout_ms > 600'000U ||
            deployment.writer_idle_heartbeat_interval_ms == 0U ||
            deployment.writer_idle_heartbeat_interval_ms >
                std::numeric_limits<std::uint64_t>::max() /
                    UINT64_C(1'000'000) ||
            deployment.writer_heartbeat_timeout_ms > 600'000U ||
            deployment.writer_heartbeat_timeout_ms <=
                deployment.writer_idle_heartbeat_interval_ms) {
            return invalid("manifest scalar constraints are not satisfied");
        }
        for (const auto& source : deployment.sources) {
            if (source.source_generation == 0U ||
                source.canonical_generation == 0U ||
                source.connect_generation == 0U ||
                source.scaffolding_allocation_cap == 0U ||
                source.safe_stop_template_id == 0U) {
                return invalid("source generation/capacity fields must be nonzero");
            }
        }

        ProductionDeploymentLoadResultV1 result{};
        result.deployment = std::move(deployment);
        return result;
    } catch (const std::bad_alloc&) {
        return Failure(
            ProductionDeploymentErrorV1::kResourceExhausted,
            "manifest parsing exhausted memory");
    } catch (...) {
        return Failure(
            ProductionDeploymentErrorV1::kInvalidText,
            "manifest parsing failed unexpectedly");
    }
}

ProductionDeploymentLoadResultV1 LoadProductionDeploymentManifestV1(
    const std::filesystem::path& deployment_directory,
    const l2flow::common::Sha256Digest& expected_sha256) noexcept {
    try {
        if (!ValidAbsoluteNormalizedPath(deployment_directory) ||
            !DigestNonzero(expected_sha256)) {
            return Failure(
                ProductionDeploymentErrorV1::kInvalidArgument,
                "deployment directory or SHA-256 pin is invalid");
        }
        const int directory_fd = ::open(
            deployment_directory.c_str(),
            O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
        if (directory_fd < 0) {
            return Failure(
                ProductionDeploymentErrorV1::kInvalidDirectory,
                "deployment directory cannot be opened", 0U, errno);
        }
        OwnedFd directory(directory_fd);
        struct stat directory_status {};
        if (::fstat(directory.get(), &directory_status) != 0) {
            return Failure(
                ProductionDeploymentErrorV1::kInvalidDirectory,
                "deployment directory cannot be inspected", 0U, errno);
        }
        if (!S_ISDIR(directory_status.st_mode) ||
            directory_status.st_uid != ::geteuid() ||
            (directory_status.st_mode & 07777U) != 0700U) {
            return Failure(
                ProductionDeploymentErrorV1::kUnsafeDirectory,
                "deployment directory must be effective-UID-owned mode 0700");
        }

        const std::string file_name(kProductionDeploymentFileNameV1);
        const int file_fd = ::openat(
            directory.get(),
            file_name.c_str(),
            O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
        if (file_fd < 0) {
            return Failure(
                ProductionDeploymentErrorV1::kOpenFailed,
                "deployment manifest cannot be opened", 0U, errno);
        }
        OwnedFd file(file_fd);
        struct stat before {};
        if (::fstat(file.get(), &before) != 0) {
            return Failure(
                ProductionDeploymentErrorV1::kOpenFailed,
                "deployment manifest cannot be inspected", 0U, errno);
        }
        if (!S_ISREG(before.st_mode) || before.st_uid != ::geteuid() ||
            (before.st_mode & 07777U) != 0600U || before.st_nlink != 1U ||
            before.st_size <= 0 ||
            static_cast<std::uint64_t>(before.st_size) >
                kProductionDeploymentMaximumBytesV1) {
            return Failure(
                ProductionDeploymentErrorV1::kUnsafeFile,
                "deployment manifest must be a singly linked effective-UID-owned mode 0600 regular file");
        }
        const std::size_t size = static_cast<std::size_t>(before.st_size);
        std::string bytes(size, '\0');
        std::size_t read_bytes = 0U;
        while (read_bytes < bytes.size()) {
            const ssize_t count = ::pread(
                file.get(),
                bytes.data() + read_bytes,
                bytes.size() - read_bytes,
                static_cast<off_t>(read_bytes));
            if (count < 0 && errno == EINTR) {
                continue;
            }
            if (count <= 0) {
                return Failure(
                    ProductionDeploymentErrorV1::kReadFailed,
                    "deployment manifest could not be read exactly",
                    0U,
                    count < 0 ? errno : 0);
            }
            read_bytes += static_cast<std::size_t>(count);
        }
        std::array<char, 1U> extra{};
        ssize_t extra_count = 0;
        do {
            extra_count = ::pread(
                file.get(), extra.data(), extra.size(), before.st_size);
        } while (extra_count < 0 && errno == EINTR);
        struct stat after {};
        if (extra_count != 0 || ::fstat(file.get(), &after) != 0 ||
            !SameFileVersion(before, after)) {
            return Failure(
                ProductionDeploymentErrorV1::kChangedDuringRead,
                "deployment manifest changed during its exact read");
        }
        return ParseProductionDeploymentManifestV1(bytes, expected_sha256);
    } catch (const std::bad_alloc&) {
        return Failure(
            ProductionDeploymentErrorV1::kResourceExhausted,
            "deployment manifest loading exhausted memory");
    } catch (...) {
        return Failure(
            ProductionDeploymentErrorV1::kInvalidArgument,
            "deployment manifest loading failed unexpectedly");
    }
}

ProductionRouterArgumentResultV1 ParseProductionRouterArgumentsV1(
    std::span<const std::string_view> arguments) noexcept {
    ProductionRouterArgumentResultV1 result{};
    try {
        for (const std::string_view argument : arguments) {
            if (HasNul(argument)) {
                result.diagnostic = "command line contains a NUL byte";
                return result;
            }
        }
        if (arguments.size() == 1U && arguments.front() == "--help") {
            result.arguments.show_help = true;
            result.ok = true;
            return result;
        }
        std::set<std::string_view> seen;
        bool has_directory = false;
        bool has_digest = false;
        for (std::size_t index = 0U; index < arguments.size(); ++index) {
            const std::string_view option = arguments[index];
            if (option == "--check") {
                if (!seen.insert(option).second) {
                    result.diagnostic = "duplicate option: --check";
                    return result;
                }
                result.arguments.check_only = true;
                continue;
            }
            if (option != "--deployment-dir" &&
                option != "--manifest-sha256" &&
                option != "--run-seconds" &&
                option != "--evidence-json") {
                result.diagnostic = "unknown option or positional argument";
                return result;
            }
            if (!seen.insert(option).second) {
                result.diagnostic = "duplicate command-line option";
                return result;
            }
            if (index + 1U >= arguments.size() ||
                arguments[index + 1U].starts_with("--")) {
                result.diagnostic = "command-line option is missing its value";
                return result;
            }
            const std::string_view value = arguments[++index];
            if (option == "--deployment-dir") {
                result.arguments.deployment_directory = value;
                has_directory = true;
            } else if (option == "--manifest-sha256") {
                if (!ParseDigest(value, &result.arguments.manifest_sha256)) {
                    result.diagnostic =
                        "--manifest-sha256 must be 64 lowercase hexadecimal digits and nonzero";
                    return result;
                }
                has_digest = true;
            } else if (option == "--run-seconds") {
                if (!ParseUnsigned(value, &result.arguments.run_seconds) ||
                    result.arguments.run_seconds == 0U ||
                    result.arguments.run_seconds > 86400U) {
                    result.diagnostic =
                        "--run-seconds must be an integer in [1,86400]";
                    return result;
                }
            } else {
                result.arguments.evidence_json = value;
            }
        }
        if (!has_directory || !has_digest) {
            result.diagnostic =
                "--deployment-dir and --manifest-sha256 are required";
            return result;
        }
        if (!ValidAbsoluteNormalizedPath(
                result.arguments.deployment_directory)) {
            result.diagnostic =
                "--deployment-dir must be a normalized absolute path";
            return result;
        }
        const bool has_run_seconds = result.arguments.run_seconds != 0U;
        const bool has_evidence = !result.arguments.evidence_json.empty();
        if (has_run_seconds != has_evidence) {
            result.diagnostic =
                "--run-seconds and --evidence-json must be supplied together";
            return result;
        }
        if (result.arguments.check_only && has_run_seconds) {
            result.diagnostic =
                "--check cannot be combined with bounded runtime options";
            return result;
        }
        if (has_evidence && !ValidAbsoluteNormalizedPath(
                                result.arguments.evidence_json)) {
            result.diagnostic =
                "--evidence-json must be a normalized absolute path";
            return result;
        }
        result.ok = true;
        return result;
    } catch (...) {
        try {
            result.diagnostic = "command-line parsing failed unexpectedly";
        } catch (...) {
        }
        return result;
    }
}

std::string ProductionRouterUsageV1() {
    return
        "Usage: mdl-production-router --deployment-dir <absolute-0700-dir> "
        "--manifest-sha256 <64-lowercase-hex> [--check | "
        "--run-seconds <1..86400> --evidence-json <absolute-path>]\n\n"
        "Reads the fixed production-v1.tsv file (mode 0600, no symlink) and "
        "starts the fresh-only four-source production route.\n"
        "--check validates the pinned manifest and non-mutating deployment "
        "inputs without loading vendor code or changing Raw/Canonical/route "
        "state. Existing INIT/RECOVERING/ACTIVE routes require the separate "
        "recovery/takeover workflow and are rejected here.\n\n"
        "Bounded mode starts its CLOCK_MONOTONIC_RAW window only after the "
        "route is authoritatively ACTIVE, samples the in-process service "
        "snapshot once per second, and then performs the normal durable "
        "revocation/drain stop. The evidence path must be a direct child of "
        "the deployment directory and is created fresh with mode 0600.\n\n"
        "Manifest template: configs/production-v1.example.tsv\n"
        "Copy it as production-v1.tsv into an effective-UID-owned mode-0700 "
        "deployment directory, replace every placeholder and every sample "
        "path/name/hash/identity/date/device/generation/capacity/timeout, and "
        "chmod the file "
        "0600. The template is a syntax fixture, not deployment sizing. Keep "
        "the credential token only in the "
        "separate mode-0400 credential_path file, never in the manifest. "
        "After the final byte is fixed, obtain the required command-line pin "
        "with: sha256sum production-v1.tsv\n";
}

namespace {

struct ProductionStaticInputsV1 final {
    OwnedFd deployment_directory;
    OwnedFd raw_directory;
    OwnedFd frontier_directory;
    OwnedFd canonical_directory;
    OwnedFd route_directory;
    std::unique_ptr<l2flow::market::InstrumentRegistryV1> registry;
    std::array<
        std::shared_ptr<const l2flow::sdk::VerifiedEndpointContract>,
        kProductionDeploymentSourceCountV1>
        endpoints{};
    std::array<std::unique_ptr<l2flow::ops::StableOutputPrefix>,
               kProductionDeploymentSourceCountV1>
        sdk_logs{};
    std::string credential_token;
    l2flow::ingress::ClockEpochInputs clock_inputs{};
    l2flow::ingress::ClockEpoch clock_epoch{};
    l2flow::common::Identity128 host_uuid{};
    l2flow::common::Identity128 linux_boot_id{};
};

[[nodiscard]] bool OpenPrivateDirectory(
    const std::filesystem::path& path,
    OwnedFd* output,
    std::string* diagnostic) noexcept {
    if (output == nullptr || !ValidAbsoluteNormalizedPath(path)) {
        if (diagnostic != nullptr) {
            *diagnostic = "private directory path is invalid";
        }
        return false;
    }
    const int descriptor = ::open(
        path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (descriptor < 0) {
        if (diagnostic != nullptr) {
            *diagnostic = "private directory cannot be opened";
        }
        return false;
    }
    OwnedFd candidate(descriptor);
    struct stat status {};
    if (::fstat(candidate.get(), &status) != 0 ||
        !S_ISDIR(status.st_mode) || status.st_uid != ::geteuid() ||
        (status.st_mode & 07777U) != 0700U) {
        if (diagnostic != nullptr) {
            *diagnostic =
                "private directory must be effective-UID-owned mode 0700";
        }
        return false;
    }
    *output = std::move(candidate);
    return true;
}

[[nodiscard]] bool FreshRouteNamespaceAppearsEmpty(
    int route_directory_fd,
    std::string* diagnostic) noexcept {
    constexpr std::array<std::string_view, 5U> kArtifacts{{
        l2flow::route::kProductionRouteFilenameV1,
        l2flow::route::kProductionRouteTemporaryFilenameV1,
        l2flow::route::kProductionRouteOwnerLockFilenameV1,
        l2flow::route::kProductionRouteOwnerMetadataFilenameV1,
        l2flow::route::kProductionRouteOwnerMetadataTemporaryFilenameV1,
    }};
    for (const std::string_view artifact : kArtifacts) {
        struct stat status {};
        int inspected = -1;
        do {
            inspected = ::fstatat(
                route_directory_fd,
                artifact.data(),
                &status,
                AT_SYMLINK_NOFOLLOW);
        } while (inspected != 0 && errno == EINTR);
        if (inspected == 0) {
            if (diagnostic != nullptr) {
                *diagnostic = "fresh route namespace already contains " +
                    std::string(artifact);
            }
            return false;
        }
        if (errno != ENOENT) {
            if (diagnostic != nullptr) {
                *diagnostic = "fresh route namespace cannot be inspected: " +
                    std::string(artifact);
            }
            return false;
        }
    }
    return true;
}

[[nodiscard]] bool FreshNamedTargetAppearsAbsent(
    int directory_fd,
    const std::string& name,
    std::string_view target_class,
    std::string* diagnostic) noexcept {
    struct stat status {};
    int inspected = -1;
    do {
        inspected = ::fstatat(
            directory_fd, name.data(), &status, AT_SYMLINK_NOFOLLOW);
    } while (inspected != 0 && errno == EINTR);
    if (inspected == 0) {
        if (diagnostic != nullptr) {
            *diagnostic = "fresh " + std::string(target_class) +
                " target already exists: " + std::string(name);
        }
        return false;
    }
    if (errno != ENOENT) {
        if (diagnostic != nullptr) {
            *diagnostic = "fresh " + std::string(target_class) +
                " target cannot be inspected: " + std::string(name);
        }
        return false;
    }
    return true;
}

[[nodiscard]] bool FreshRawStreamTargetAppearsAbsent(
    int raw_directory_fd,
    std::uint32_t source_stream_id,
    std::uint32_t capture_date,
    std::string_view stream_slug,
    std::string* diagnostic) noexcept {
    const std::string date_name =
        "capture_date=" + std::to_string(capture_date);
    struct stat date_status {};
    int inspected = -1;
    do {
        inspected = ::fstatat(
            raw_directory_fd,
            date_name.c_str(),
            &date_status,
            AT_SYMLINK_NOFOLLOW);
    } while (inspected != 0 && errno == EINTR);
    if (inspected != 0) {
        if (errno == ENOENT) {
            return true;
        }
        if (diagnostic != nullptr) {
            *diagnostic = "Raw capture-date namespace cannot be inspected";
        }
        return false;
    }
    if (!S_ISDIR(date_status.st_mode) ||
        date_status.st_uid != ::geteuid() ||
        (date_status.st_mode & 07777U) != 0700U) {
        if (diagnostic != nullptr) {
            *diagnostic = "Raw capture-date namespace is not a private directory";
        }
        return false;
    }
    int date_fd = -1;
    do {
        date_fd = ::openat(
            raw_directory_fd,
            date_name.c_str(),
            O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    } while (date_fd < 0 && errno == EINTR);
    if (date_fd < 0) {
        if (diagnostic != nullptr) {
            *diagnostic = "Raw capture-date namespace cannot be opened";
        }
        return false;
    }
    OwnedFd retained_date(date_fd);
    struct stat opened_status {};
    if (::fstat(retained_date.get(), &opened_status) != 0 ||
        opened_status.st_dev != date_status.st_dev ||
        opened_status.st_ino != date_status.st_ino) {
        if (diagnostic != nullptr) {
            *diagnostic = "Raw capture-date namespace identity changed";
        }
        return false;
    }
    const std::string stream_name = "stream=" +
        std::to_string(source_stream_id) + "-" + std::string(stream_slug);
    return FreshNamedTargetAppearsAbsent(
        retained_date.get(), stream_name, "Raw stream", diagnostic);
}

[[nodiscard]] bool FreshGenerationTargetsAppearAbsent(
    const ProductionDeploymentV1& deployment,
    int raw_directory_fd,
    int frontier_directory_fd,
    int canonical_directory_fd,
    std::string* diagnostic) {
    constexpr std::array<std::string_view, 2U> kShardedFamilies{{
        "snapshot", "tick"}};
    constexpr std::array<std::string_view, 2U> kSingletonFamilies{{
        "quality", "control"}};
    for (std::size_t source = 0U; source < deployment.sources.size();
         ++source) {
        const auto& source_config = deployment.sources[source];
        const auto& spec = l2flow::sdk::GetIngressSpec(source_config.kind);
        if (!FreshRawStreamTargetAppearsAbsent(
                raw_directory_fd,
                spec.source_stream_id,
                deployment.capture_date,
                source_config.stream_slug,
                diagnostic)) {
            return false;
        }
        const std::string frontier_name = source_config.stream_slug + "-g" +
            std::to_string(source_config.source_generation) + ".frontier";
        if (!FreshNamedTargetAppearsAbsent(
                frontier_directory_fd,
                frontier_name,
                "SourceFrontier",
                diagnostic)) {
            return false;
        }
        const std::string stem_prefix = source_config.stream_slug + "-g" +
            std::to_string(source_config.canonical_generation) + "-";
        const auto check_canonical_pair = [&](std::string_view family,
                                              std::uint32_t shard) {
            const std::string stem = stem_prefix + std::string(family) +
                "-" + std::to_string(shard);
            return FreshNamedTargetAppearsAbsent(
                       canonical_directory_fd,
                       stem + ".clog",
                       "Canonical segment",
                       diagnostic) &&
                FreshNamedTargetAppearsAbsent(
                       canonical_directory_fd,
                       stem + ".manifest",
                       "Canonical manifest",
                       diagnostic) &&
                FreshNamedTargetAppearsAbsent(
                       canonical_directory_fd,
                       stem + ".manifest.tmp",
                       "Canonical manifest temporary",
                       diagnostic);
        };
        for (std::uint32_t shard = 0U; shard < 16U; ++shard) {
            for (const std::string_view family : kShardedFamilies) {
                if (!check_canonical_pair(family, shard)) {
                    return false;
                }
            }
        }
        for (const std::string_view family : kSingletonFamilies) {
            if (!check_canonical_pair(family, 0U)) {
                return false;
            }
        }
    }
    return true;
}

[[nodiscard]] bool ParseUuidText(
    std::string_view text,
    l2flow::common::Identity128* output) noexcept {
    std::array<char, 32U> compact{};
    std::size_t count = 0U;
    for (char character : text) {
        if (character == '-') {
            continue;
        }
        if (count >= compact.size()) {
            return false;
        }
        compact[count++] = character;
    }
    return count == compact.size() &&
           ParseIdentity(
               std::string_view(compact.data(), compact.size()), output);
}

[[nodiscard]] bool PathWithin(
    const std::filesystem::path& root,
    const std::filesystem::path& candidate) {
    auto root_component = root.begin();
    auto candidate_component = candidate.begin();
    for (; root_component != root.end();
         ++root_component, ++candidate_component) {
        if (candidate_component == candidate.end() ||
            *root_component != *candidate_component) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] bool ValidateOutputPathSeparation(
    const std::filesystem::path& deployment_directory,
    const ProductionDeploymentV1& deployment,
    std::string* diagnostic) {
    std::vector<std::filesystem::path> protected_roots{
        deployment_directory,
        deployment.raw_root,
        deployment.frontier_root,
        deployment.canonical_root,
        deployment.route_root,
    };
    std::vector<std::filesystem::path> protected_files{
        deployment.sdk_library_path,
        deployment.credential_path,
    };
    for (const auto& source : deployment.sources) {
        protected_files.emplace_back(source.endpoint_path);
    }
    std::vector<std::filesystem::path> outputs;
    outputs.reserve(deployment.sources.size() * 2U);
    std::set<std::filesystem::path> log_parents;
    std::set<std::filesystem::path> metrics_parents;
    for (const auto& source : deployment.sources) {
        const std::filesystem::path log(source.sdk_log_prefix);
        const std::filesystem::path metrics(source.metrics_path);
        if (!log_parents.insert(log.parent_path()).second ||
            !metrics_parents.insert(metrics.parent_path()).second) {
            if (diagnostic != nullptr) {
                *diagnostic =
                    "each source requires a distinct SDK-log and metrics directory";
            }
            return false;
        }
        outputs.push_back(log);
        outputs.push_back(metrics);
    }
    for (std::size_t index = 0U; index < outputs.size(); ++index) {
        for (std::size_t prior = 0U; prior < index; ++prior) {
            if (outputs[index] == outputs[prior]) {
                if (diagnostic != nullptr) {
                    *diagnostic = "production output paths must be distinct";
                }
                return false;
            }
        }
        for (const auto& root : protected_roots) {
            if (PathWithin(root, outputs[index])) {
                if (diagnostic != nullptr) {
                    *diagnostic =
                        "SDK-log/metrics output may not be inside a protected production root";
                }
                return false;
            }
        }
        if (std::find(
                protected_files.begin(), protected_files.end(), outputs[index]) !=
            protected_files.end()) {
            if (diagnostic != nullptr) {
                *diagnostic =
                    "SDK-log/metrics output aliases a protected input path";
            }
            return false;
        }
    }
    return true;
}

[[nodiscard]] bool ReadCoordinatorMarker(
    int raw_directory_fd,
    const ProductionDeploymentV1& deployment,
    std::string* diagnostic) noexcept {
    const int descriptor = ::openat(
        raw_directory_fd,
        l2flow::ingress::kRawReserveCoordinatorLeaseFilename,
        O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    if (descriptor < 0) {
        if (diagnostic != nullptr) {
            *diagnostic = "reserve coordinator lease marker cannot be opened";
        }
        return false;
    }
    OwnedFd file(descriptor);
    struct stat before {};
    if (::fstat(file.get(), &before) != 0 ||
        !S_ISREG(before.st_mode) || before.st_uid != ::geteuid() ||
        before.st_nlink != 1U ||
        (before.st_mode & 07777U) != 0600U ||
        before.st_size != static_cast<off_t>(
                              l2flow::ingress::
                                  kRawReserveCoordinatorLeaseMarkerBytes)) {
        if (diagnostic != nullptr) {
            *diagnostic = "reserve coordinator lease marker is unsafe";
        }
        return false;
    }
    l2flow::ingress::RawReserveCoordinatorLeaseMarkerWireV1 wire{};
    std::size_t offset = 0U;
    while (offset < wire.size()) {
        const ssize_t count = ::pread(
            file.get(),
            wire.data() + offset,
            wire.size() - offset,
            static_cast<off_t>(offset));
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            if (diagnostic != nullptr) {
                *diagnostic = "reserve coordinator lease marker read failed";
            }
            return false;
        }
        offset += static_cast<std::size_t>(count);
    }
    struct stat after {};
    l2flow::ingress::RawReserveCoordinatorLeaseMarkerV1 observed{};
    if (::fstat(file.get(), &after) != 0 ||
        !SameFileVersion(before, after) ||
        !l2flow::ingress::DecodeRawReserveCoordinatorLeaseMarkerV1(
            wire, &observed) ||
        observed.coordinator_identity != deployment.coordinator_identity ||
        observed.device_id != deployment.coordinator_device_id ||
        observed.quota_identity_sha256 !=
            deployment.coordinator_quota_sha256 ||
        observed.mount_identity_sha256 !=
            deployment.coordinator_mount_sha256) {
        if (diagnostic != nullptr) {
            *diagnostic =
                "reserve coordinator lease marker does not match its pins";
        }
        return false;
    }
    return true;
}

[[nodiscard]] bool BuildAndValidateStableRawConfig(
    const ProductionDeploymentV1& deployment,
    std::size_t source,
    l2flow::ingress::RawIngressConfig* output,
    std::string* diagnostic) {
    if (output == nullptr || source >= deployment.sources.size()) {
        return false;
    }
    const auto& source_config = deployment.sources[source];
    l2flow::ingress::RawIngressConfig stable =
        l2flow::ingress::DefaultRawIngressConfig(source_config.kind);
    // This vendor SDK owns one process-global MessageDispatcher even when
    // DllCreateIOManager returns multiple managers.  Its latency-report path
    // computes a dispatcher index globally and then applies that index to the
    // receiving manager's local work-thread vector.  Different vector sizes
    // therefore permit an out-of-bounds _WorkThreadItem selection.  The
    // Formal production creates one physical manager and one physical
    // Subscriber, followed by an in-process four-lane callback fanout. Select
    // the maximum work/IO thread counts required by any source so every Raw
    // callback lane is valid in that single vendor dispatcher.
    stable.work_threads = 0;
    stable.io_threads = 0;
    for (const l2flow::sdk::IngressKind kind : kSourceKinds) {
        stable.work_threads = std::max(
            stable.work_threads,
            l2flow::sdk::GetIngressSpec(kind).default_work_threads);
        stable.io_threads = std::max(
            stable.io_threads,
            l2flow::sdk::GetIngressSpec(kind).default_io_threads);
    }
    stable.endpoint_contract_sha256 =
        l2flow::common::Sha256Hex(source_config.endpoint_sha256);
    stable.credential_name = deployment.credential_name;
    stable.sdk_log_prefix = source_config.sdk_log_prefix;
    stable.metrics_textfile_path = source_config.metrics_path;
    stable.heartbeat_interval_seconds =
        deployment.raw_heartbeat_interval_seconds;
    stable.heartbeat_timeout_seconds = deployment.raw_heartbeat_timeout_seconds;
    stable.include_optional_index = deployment.raw_include_optional_index;
    stable.max_message_bytes = deployment.raw_max_message_bytes;
    stable.ring_capacity_bytes = deployment.raw_ring_capacity_bytes;
    stable.ring_stall_budget_seconds =
        deployment.raw_ring_stall_budget_seconds;
    stable.raw_root = deployment.raw_root;
    stable.segment_target_bytes = deployment.raw_segment_target_bytes;
    stable.segment_max_age_seconds = deployment.raw_segment_max_age_seconds;
    stable.sync_interval_milliseconds =
        deployment.raw_sync_interval_milliseconds;
    stable.sync_bytes = deployment.raw_sync_bytes;
    stable.sparse_index_every_records =
        deployment.raw_sparse_index_every_records;
    stable.sparse_index_every_bytes =
        deployment.raw_sparse_index_every_bytes;
    stable.reserve_domain_id = deployment.reserve_domain_id;
    stable.reserve_coordinator_socket =
        deployment.reserve_coordinator_socket;
    stable.reserve_ack_timeout_milliseconds =
        deployment.reserve_ack_timeout_milliseconds;
    stable.emergency_reserve_bytes = deployment.emergency_reserve_bytes;
    stable.canonical_clock_source_config = deployment.clock_source_config;
    const std::string stable_error =
        l2flow::ingress::ValidateRawIngressConfig(stable);
    if (!stable_error.empty()) {
        if (diagnostic != nullptr) {
            *diagnostic = "Raw stable configuration rejected: " + stable_error;
        }
        return false;
    }
    *output = std::move(stable);
    return true;
}

[[nodiscard]] bool PrepareStaticInputs(
    const std::filesystem::path& deployment_directory,
    const ProductionDeploymentV1& deployment,
    ProductionStaticInputsV1* output,
    std::string* diagnostic) noexcept {
    if (output == nullptr) {
        return false;
    }
    try {
        ProductionStaticInputsV1 candidate;
        if (!OpenPrivateDirectory(
                deployment_directory,
                &candidate.deployment_directory,
                diagnostic) ||
            !OpenPrivateDirectory(
                deployment.raw_root,
                &candidate.raw_directory,
                diagnostic) ||
            !OpenPrivateDirectory(
                deployment.frontier_root,
                &candidate.frontier_directory,
                diagnostic) ||
            !OpenPrivateDirectory(
                deployment.canonical_root,
                &candidate.canonical_directory,
                diagnostic) ||
            !OpenPrivateDirectory(
                deployment.route_root,
                &candidate.route_directory,
                diagnostic)) {
            return false;
        }
        const std::array<int, 5U> directory_descriptors{{
            candidate.deployment_directory.get(),
            candidate.raw_directory.get(),
            candidate.frontier_directory.get(),
            candidate.canonical_directory.get(),
            candidate.route_directory.get(),
        }};
        std::array<struct stat, directory_descriptors.size()> directory_stats{};
        for (std::size_t index = 0U; index < directory_descriptors.size();
             ++index) {
            if (::fstat(directory_descriptors[index], &directory_stats[index]) !=
                0) {
                if (diagnostic != nullptr) {
                    *diagnostic = "production directory identity cannot be read";
                }
                return false;
            }
            for (std::size_t prior = 0U; prior < index; ++prior) {
                if (directory_stats[index].st_dev ==
                        directory_stats[prior].st_dev &&
                    directory_stats[index].st_ino ==
                        directory_stats[prior].st_ino) {
                    if (diagnostic != nullptr) {
                        *diagnostic =
                            "deployment/Raw/frontier/Canonical/route directories must be distinct inodes";
                    }
                    return false;
                }
            }
        }
        // This is an early point-in-time operator check only.  The formal
        // fresh publisher repeats the same fixed-name absence proof under its
        // process gate and directory flock immediately before owner/Active
        // publication, so a race cannot turn this observation into authority.
        if (!FreshRouteNamespaceAppearsEmpty(
                candidate.route_directory.get(), diagnostic)) {
            return false;
        }
        if (!FreshGenerationTargetsAppearAbsent(
                deployment,
                candidate.raw_directory.get(),
                candidate.frontier_directory.get(),
                candidate.canonical_directory.get(),
                diagnostic)) {
            return false;
        }
        if (!ValidateOutputPathSeparation(
                deployment_directory, deployment, diagnostic)) {
            return false;
        }
        for (std::size_t source = 0U; source < deployment.sources.size();
             ++source) {
            l2flow::ingress::RawIngressConfig stable{};
            if (!BuildAndValidateStableRawConfig(
                    deployment, source, &stable, diagnostic)) {
                return false;
            }
            candidate.sdk_logs[source] =
                l2flow::ops::OpenStableOutputPrefix(
                    deployment.sources[source].sdk_log_prefix,
                    diagnostic);
            if (candidate.sdk_logs[source] == nullptr) {
                return false;
            }
        }
        struct stat raw_status {};
        if (::fstat(candidate.raw_directory.get(), &raw_status) != 0 ||
            static_cast<std::uint64_t>(raw_status.st_dev) !=
                deployment.coordinator_device_id ||
            !ReadCoordinatorMarker(
                candidate.raw_directory.get(), deployment, diagnostic)) {
            if (diagnostic != nullptr && diagnostic->empty()) {
                *diagnostic =
                    "Raw root device does not match the coordinator pin";
            }
            return false;
        }

        l2flow::market::InstrumentRegistryFileOptionsV1 registry_options{};
        registry_options.directory_fd = candidate.deployment_directory.get();
        registry_options.file_name = deployment.instrument_registry_file;
        registry_options.expected_owner_uid =
            static_cast<std::uint32_t>(::geteuid());
        registry_options.expected_registry_version =
            deployment.instrument_registry_version;
        registry_options.expected_registry_sha256 =
            deployment.instrument_registry_sha256;
        auto registry = l2flow::market::LoadInstrumentRegistryFileV1(
            registry_options);
        if (!registry.ok()) {
            if (diagnostic != nullptr) {
                *diagnostic = "instrument registry file was rejected: " +
                    std::string(l2flow::market::InstrumentRegistryFileErrorNameV1(
                        registry.error));
            }
            return false;
        }
        candidate.registry = std::move(registry.registry);

        for (std::size_t source = 0U; source < deployment.sources.size();
             ++source) {
            const auto& source_config = deployment.sources[source];
            candidate.endpoints[source] =
                l2flow::sdk::LoadVerifiedEndpointContractFile(
                    source_config.endpoint_path,
                    l2flow::common::Sha256Hex(source_config.endpoint_sha256),
                    source_config.kind,
                    diagnostic);
            if (candidate.endpoints[source] == nullptr) {
                return false;
            }
        }

        l2flow::ops::CredentialPolicy credential_policy{};
        credential_policy.expected_owner = ::geteuid();
        const l2flow::ops::CredentialResult credential =
            l2flow::ops::ReadCredentialFile(
                deployment.credential_path, credential_policy);
        if (!credential.ok()) {
            if (diagnostic != nullptr) {
                *diagnostic = credential.error.empty()
                    ? "credential file was rejected"
                    : credential.error;
            }
            return false;
        }
        candidate.credential_token = credential.token;

        if (!l2flow::ingress::ReadClockEpochInputs(
                "/etc/machine-id",
                "/proc/sys/kernel/random/boot_id",
                deployment.clock_source_config,
                &candidate.clock_inputs,
                diagnostic) ||
            !ParseUuidText(
                candidate.clock_inputs.host_uuid, &candidate.host_uuid) ||
            !ParseUuidText(
                candidate.clock_inputs.linux_boot_id,
                &candidate.linux_boot_id)) {
            if (diagnostic != nullptr && diagnostic->empty()) {
                *diagnostic = "host or Linux boot UUID is invalid";
            }
            return false;
        }
        candidate.clock_epoch =
            l2flow::ingress::ComputeClockEpoch(candidate.clock_inputs);

        std::error_code sdk_status_error;
        if (!std::filesystem::is_regular_file(
                deployment.sdk_library_path, sdk_status_error) ||
            sdk_status_error) {
            if (diagnostic != nullptr) {
                *diagnostic =
                    "operator-selected SDK library path does not name an "
                    "existing regular file";
            }
            return false;
        }
        *output = std::move(candidate);
        if (diagnostic != nullptr) {
            diagnostic->clear();
        }
        return true;
    } catch (const std::exception& exception) {
        if (diagnostic != nullptr) {
            try {
                *diagnostic = std::string("static production preflight failed: ") +
                    exception.what();
            } catch (...) {
            }
        }
        return false;
    } catch (...) {
        if (diagnostic != nullptr) {
            try {
                *diagnostic = "static production preflight failed unexpectedly";
            } catch (...) {
            }
        }
        return false;
    }
}

}  // namespace

namespace {

[[nodiscard]] std::uint64_t PosixClockNow(clockid_t clock_id) noexcept {
    struct timespec value {};
    if (::clock_gettime(clock_id, &value) != 0 || value.tv_sec < 0 ||
        value.tv_nsec < 0) {
        return 0U;
    }
    const std::uint64_t seconds = static_cast<std::uint64_t>(value.tv_sec);
    if (seconds >
        (std::numeric_limits<std::uint64_t>::max() -
         static_cast<std::uint64_t>(value.tv_nsec)) /
            UINT64_C(1'000'000'000)) {
        return 0U;
    }
    return seconds * UINT64_C(1'000'000'000) +
           static_cast<std::uint64_t>(value.tv_nsec);
}

[[nodiscard]] std::uint64_t RawRealtimeNow(void*) noexcept {
    return PosixClockNow(CLOCK_REALTIME);
}

// control.page writer liveness is explicitly in host CLOCK_MONOTONIC and is
// checked against that clock by ProductionAggregateRuntimeV1.
[[nodiscard]] std::uint64_t RawHeartbeatMonotonicNow(void*) noexcept {
    return PosixClockNow(CLOCK_MONOTONIC);
}

// Raw capture receive timestamps, segment-age decisions and immutable
// segment creation provenance share the configured CLOCK_MONOTONIC_RAW epoch.
[[nodiscard]] std::uint64_t RawStreamEpochNow(void*) noexcept {
    return PosixClockNow(CLOCK_MONOTONIC_RAW);
}

[[nodiscard]] bool ToSignedTime(
    std::uint64_t value,
    std::int64_t* output) noexcept {
    if (output == nullptr ||
        value > static_cast<std::uint64_t>(
                    std::numeric_limits<std::int64_t>::max())) {
        return false;
    }
    *output = static_cast<std::int64_t>(value);
    return true;
}

[[nodiscard]] bool BuildDigest(
    std::string_view hex,
    l2flow::common::Sha256Digest* output) noexcept {
    std::string ignored;
    return l2flow::common::ParseSha256Hex(hex, output, &ignored) &&
           DigestNonzero(*output);
}

[[nodiscard]] l2flow::ingress::RawReserveCoordinatorLeaseMarkerV1
CoordinatorMarker(const ProductionDeploymentV1& deployment) noexcept {
    l2flow::ingress::RawReserveCoordinatorLeaseMarkerV1 marker{};
    marker.coordinator_identity = deployment.coordinator_identity;
    marker.device_id = deployment.coordinator_device_id;
    marker.quota_identity_sha256 = deployment.coordinator_quota_sha256;
    marker.mount_identity_sha256 = deployment.coordinator_mount_sha256;
    return marker;
}

[[nodiscard]] l2flow::ingress::RawReserveFreshScaffoldingV1 Registration(
    const ProductionDeploymentV1& deployment,
    std::size_t source) {
    const auto& source_config = deployment.sources.at(source);
    const auto& spec = l2flow::sdk::GetIngressSpec(source_config.kind);
    l2flow::ingress::RawReserveFreshScaffoldingV1 registration{};
    registration.key.route.source_stream_id = spec.source_stream_id;
    registration.key.route.capture_date = deployment.capture_date;
    registration.key.stream_day_id = source_config.stream_day_id;
    registration.key.recovery_attempt_id =
        source_config.recovery_attempt_id;
    registration.writer_instance = source_config.writer_instance;
    registration.recovery_intent =
        l2flow::ingress::ReserveRecoveryIntentV1::kResumeConnect;
    registration.scaffolding_allocation_cap =
        source_config.scaffolding_allocation_cap;
    registration.safe_stop_template_id =
        source_config.safe_stop_template_id;
    return registration;
}

[[nodiscard]] l2flow::ingress::ReserveStateEntryV1
ExpectedFreshScaffoldingEntry(
    const l2flow::ingress::RawReserveFreshScaffoldingV1& registration)
    noexcept {
    l2flow::ingress::ReserveStateEntryV1 entry{};
    entry.source_stream_id = registration.key.route.source_stream_id;
    entry.capture_date = registration.key.route.capture_date;
    entry.stream_day_id = registration.key.stream_day_id;
    entry.registry_status =
        l2flow::ingress::ReserveRegistryStatusV1::kScaffolding;
    entry.recovery_origin =
        l2flow::ingress::ReserveRecoveryOriginV1::kFreshInit;
    entry.recovery_intent =
        l2flow::ingress::ReserveRecoveryIntentV1::kResumeConnect;
    entry.writer_instance = registration.writer_instance;
    entry.executor_or_recovery_attempt =
        registration.key.recovery_attempt_id;
    entry.grant_bytes = registration.scaffolding_allocation_cap;
    entry.safe_stop_template_id = registration.safe_stop_template_id;
    return entry;
}

[[nodiscard]] bool ExactFreshScaffoldingSet(
    const ProductionDeploymentV1& deployment,
    const l2flow::ingress::ReserveCoordinatorStateV1& state) noexcept {
    if (state.selected_slot >= state.slots.size()) {
        return false;
    }
    const auto& slot = state.slots[state.selected_slot];
    if (slot.coordinator_state !=
            l2flow::ingress::ReserveCoordinatorPhaseV1::kProvisioned ||
        slot.entry_count != deployment.sources.size() ||
        slot.entry_count > slot.entries.size()) {
        return false;
    }
    std::array<bool, kProductionDeploymentSourceCountV1> matched{};
    for (std::size_t entry_index = 0U;
         entry_index < static_cast<std::size_t>(slot.entry_count);
         ++entry_index) {
        bool found = false;
        for (std::size_t source = 0U; source < deployment.sources.size();
             ++source) {
            if (matched[source]) {
                continue;
            }
            if (slot.entries[entry_index] == ExpectedFreshScaffoldingEntry(
                    Registration(deployment, source))) {
                matched[source] = true;
                found = true;
                break;
            }
        }
        if (!found) {
            return false;
        }
    }
    return std::all_of(matched.begin(), matched.end(), [](bool value) {
        return value;
    });
}

class DeploymentSourceLifetimeV1 final
    : public l2flow::apps::ProductionSourceLifetimeV1 {
public:
    std::unique_ptr<l2flow::canonical::SourceFrontierPosixMappingV1>
        frontier;
    std::unique_ptr<l2flow::canonical::SourceFrontierPosixRoleLeaseV1>
        producer_lease;
    std::unique_ptr<l2flow::canonical::SourceFrontierPosixRoleLeaseV1>
        processor_lease;
    std::unique_ptr<l2flow::ops::StableOutputPrefix> sdk_log;
    std::shared_ptr<l2flow::ingress::RawReserveRegistryCoordinatorV1>
        coordinator;
};

struct SourceAssemblyV1 final {
    std::unique_ptr<DeploymentSourceLifetimeV1> lifetime;
    std::unique_ptr<l2flow::control::ControlDecoderV1> control_decoder;
    std::unique_ptr<l2flow::canonical::CanonicalNormalizerV1> normalizer;
    std::vector<l2flow::runtime::ProductionCanonicalSinkV1> sinks;
    l2flow::runtime::ProductionSourcePipelineConfigV1 pipeline_config{};
    l2flow::ingress::RawReserveFreshScaffoldingV1 registration{};
    l2flow::ingress::RawWalWriterConfig writer_config{};
    l2flow::ingress::RawPosixWalStreamBackendOptionsV1 backend_options{};
    l2flow::ingress::RawSegmentArtifactOptionsV1 artifact_options{};
    l2flow::ingress::RawWalStreamLimitsV1 stream_limits{};
    l2flow::ingress::RawIngressAppConfigV1 raw_app_config{};
    l2flow::ingress::RawIngressAppOptionsV1 raw_app_options{};
    l2flow::ingress::RawLiveTailPosixLimitsV1 live_tail_limits{};
};

[[nodiscard]] bool AddCanonicalSink(
    const ProductionDeploymentV1& deployment,
    const ProductionStaticInputsV1& static_inputs,
    const l2flow::canonical::ClockEpochIdentityV1& clock_epoch,
    const l2flow::common::Sha256Digest& normalizer_build_sha256,
    const l2flow::common::Sha256Digest& normalizer_config_sha256,
    std::size_t source,
    l2flow::canonical::CanonicalFamilyV1 family,
    std::uint32_t shard,
    l2flow::canonical::CanonicalEventTypeV1 event_type,
    std::uint32_t record_size,
    std::int64_t created_realtime_ns,
    std::int64_t created_monotonic_ns,
    std::vector<l2flow::runtime::ProductionCanonicalSinkV1>* sinks,
    std::string* diagnostic) {
    const auto& source_config = deployment.sources.at(source);
    const auto& spec = l2flow::sdk::GetIngressSpec(source_config.kind);
    l2flow::canonical::CanonicalSegmentDescriptorV1 descriptor{};
    descriptor.event_type = event_type;
    descriptor.record_size = record_size;
    descriptor.source_stream_id = spec.source_stream_id;
    descriptor.shard = shard;
    descriptor.trade_date = deployment.trade_date;
    descriptor.origin_capture_date = deployment.capture_date;
    descriptor.origin_stream_day_id = source_config.stream_day_id;
    descriptor.origin_source_writer_instance = source_config.writer_instance;
    descriptor.origin_source_generation = source_config.source_generation;
    descriptor.clock_epoch = clock_epoch;
    descriptor.schema_sha256 =
        l2flow::canonical::CanonicalSchemaDescriptorSha256V1();
    descriptor.dtype_sha256 =
        l2flow::canonical::CanonicalDtypeDescriptorSha256V1();
    descriptor.registry_version = deployment.instrument_registry_version;
    descriptor.registry_sha256 = deployment.instrument_registry_sha256;
    descriptor.normalizer_build_sha256 = normalizer_build_sha256;
    descriptor.normalizer_config_sha256 = normalizer_config_sha256;
    descriptor.generation = source_config.canonical_generation;
    descriptor.segment_sequence = 1U;
    std::string family_name;
    switch (family) {
        case l2flow::canonical::CanonicalFamilyV1::kSnapshot:
            family_name = "snapshot";
            descriptor.capacity_records =
                deployment.canonical_snapshot_capacity_records_per_sink;
            break;
        case l2flow::canonical::CanonicalFamilyV1::kTick:
            family_name = "tick";
            descriptor.capacity_records =
                deployment.canonical_tick_capacity_records_per_sink;
            break;
        case l2flow::canonical::CanonicalFamilyV1::kQuality:
            family_name = "quality";
            descriptor.capacity_records =
                deployment.canonical_quality_capacity_records_per_sink;
            break;
        case l2flow::canonical::CanonicalFamilyV1::kControl:
            family_name = "control";
            descriptor.capacity_records =
                deployment.canonical_control_capacity_records_per_sink;
            break;
    }
    const std::string stem = source_config.stream_slug + "-g" +
        std::to_string(source_config.canonical_generation) + "-" +
        family_name + "-" + std::to_string(shard);
    // Canonical opens and fsyncs the parent with O_NOFOLLOW.  Making the
    // retained fd symlink an intermediate component (via the final "/.")
    // preserves the dirfd identity while allowing that final secure open;
    // using bare /proc/self/fd/<n> as the parent would fail with ENOTDIR.
    const std::filesystem::path retained_directory(
        "/proc/self/fd/" +
        std::to_string(static_inputs.canonical_directory.get()) + "/.");
    l2flow::canonical::CanonicalSegmentCreateOptionsV1 options{};
    options.segment_path = retained_directory / (stem + ".clog");
    options.manifest_path = retained_directory / (stem + ".manifest");
    options.descriptor = descriptor;
    options.created_realtime_ns = created_realtime_ns;
    options.created_monotonic_ns = created_monotonic_ns;
    std::unique_ptr<l2flow::canonical::CanonicalSegmentWriterV1> writer;
    const auto error = l2flow::canonical::CanonicalSegmentWriterV1::Create(
        options, &writer);
    if (error != l2flow::canonical::CanonicalSegmentErrorV1::kNone ||
        writer == nullptr ||
        writer->AdvanceProcessedRaw(
            0U, l2flow::ingress::kRawV1SegmentHeaderBytes) !=
            l2flow::canonical::CanonicalSegmentErrorV1::kNone) {
        if (diagnostic != nullptr) {
            *diagnostic = "Canonical sink creation failed: " +
                std::string(
                    l2flow::canonical::CanonicalSegmentErrorNameV1(error));
        }
        return false;
    }
    sinks->push_back(l2flow::runtime::ProductionCanonicalSinkV1{
        family, shard, std::move(writer)});
    return true;
}

[[nodiscard]] bool BuildSourceAssembly(
    const ProductionDeploymentV1& deployment,
    ProductionStaticInputsV1& static_inputs,
    const std::shared_ptr<l2flow::ingress::RawReserveRegistryCoordinatorV1>&
        coordinator,
    const l2flow::common::Sha256Digest& build_sha256,
    std::size_t source,
    SourceAssemblyV1* output,
    std::string* diagnostic) {
    if (output == nullptr || coordinator == nullptr) {
        return false;
    }
    const auto& source_config = deployment.sources.at(source);
    const auto& spec = l2flow::sdk::GetIngressSpec(source_config.kind);
    SourceAssemblyV1 candidate{};
    candidate.registration = Registration(deployment, source);
    candidate.lifetime = std::make_unique<DeploymentSourceLifetimeV1>();
    candidate.lifetime->coordinator = coordinator;
    candidate.lifetime->sdk_log = std::move(static_inputs.sdk_logs[source]);
    if (candidate.lifetime->sdk_log == nullptr) {
        if (diagnostic != nullptr) {
            *diagnostic = "preflight SDK log lease is unavailable";
        }
        return false;
    }

    l2flow::canonical::ClockEpochIdentityV1 clock_epoch{};
    clock_epoch.algorithm = 1U;
    clock_epoch.digest = static_inputs.clock_epoch.digest;
    clock_epoch.label = static_inputs.clock_epoch.value;
    l2flow::canonical::SourceFrontierConfigV1 frontier_config{};
    frontier_config.source_stream_id = spec.source_stream_id;
    frontier_config.capture_date = deployment.capture_date;
    frontier_config.stream_day_id = source_config.stream_day_id;
    frontier_config.clock_epoch = clock_epoch;
    frontier_config.writer_instance = source_config.writer_instance;
    frontier_config.generation = source_config.source_generation;
    frontier_config.initial_global_wal_pos =
        l2flow::ingress::kRawV1SegmentHeaderBytes;
    frontier_config.initial_state =
        l2flow::canonical::SourceStateV1::kHealthy;
    const std::string frontier_name = source_config.stream_slug + "-g" +
        std::to_string(source_config.source_generation) + ".frontier";
    l2flow::canonical::SourceFrontierPosixFileOptionsV1 frontier_options{};
    frontier_options.directory_fd = static_inputs.frontier_directory.get();
    frontier_options.file_name = frontier_name;
    frontier_options.expected_owner_uid =
        static_cast<std::uint32_t>(::geteuid());
    auto frontier_result =
        l2flow::canonical::SourceFrontierPosixMappingV1::Create(
            frontier_options, frontier_config, &candidate.lifetime->frontier);
    if (!frontier_result) {
        if (diagnostic != nullptr) {
            *diagnostic = "SourceFrontier creation failed: " +
                std::string(l2flow::canonical::SourceFrontierPosixErrorNameV1(
                    frontier_result.error));
        }
        return false;
    }
    frontier_result = candidate.lifetime->frontier->AcquireRole(
        l2flow::canonical::SourceFrontierWriterRoleV1::kProducer,
        &candidate.lifetime->producer_lease);
    if (!frontier_result) {
        if (diagnostic != nullptr) {
            *diagnostic = "SourceFrontier producer lease unavailable";
        }
        return false;
    }
    frontier_result = candidate.lifetime->frontier->AcquireRole(
        l2flow::canonical::SourceFrontierWriterRoleV1::kProcessor,
        &candidate.lifetime->processor_lease);
    if (!frontier_result) {
        if (diagnostic != nullptr) {
            *diagnostic = "SourceFrontier processor lease unavailable";
        }
        return false;
    }

    l2flow::ingress::RawIngressConfig stable{};
    if (!BuildAndValidateStableRawConfig(
            deployment, source, &stable, diagnostic)) {
        return false;
    }
    l2flow::common::Sha256Digest stable_config_sha256{};
    if (!BuildDigest(
            l2flow::ingress::RawIngressConfigSha256(stable),
            &stable_config_sha256)) {
        if (diagnostic != nullptr) {
            *diagnostic = "Raw stable configuration digest is invalid";
        }
        return false;
    }

    l2flow::control::ControlDecoderConfigV1 control_config{};
    control_config.source_stream_id = spec.source_stream_id;
    control_config.capture_date = deployment.capture_date;
    control_config.stream_day_id = source_config.stream_day_id;
    control_config.stable_config_sha256 = stable_config_sha256;
    control_config.required = spec.required;
    if (deployment.raw_include_optional_index) {
        control_config.optional = spec.optional;
    }
    if (l2flow::control::ControlDecoderV1::Create(
            std::move(control_config), &candidate.control_decoder) !=
            l2flow::control::ControlDecoderCreateErrorV1::kNone) {
        if (diagnostic != nullptr) {
            *diagnostic = "control decoder creation failed";
        }
        return false;
    }

    l2flow::canonical::CanonicalNormalizerConfigV1 normalizer_config{};
    normalizer_config.capture_date = deployment.capture_date;
    normalizer_config.trade_date = deployment.trade_date;
    normalizer_config.source_stream_id = spec.source_stream_id;
    normalizer_config.stream_day_id = source_config.stream_day_id;
    normalizer_config.shard_count = 16U;
    normalizer_config.instrument_registry = static_inputs.registry.get();
    normalizer_config.decoder_limits.maximum_body_bytes =
        static_cast<std::size_t>(deployment.raw_max_message_bytes -
                                 l2flow::ingress::kVendorMessageHeadBytes);
    normalizer_config.sequence_policy.policy_version = 1U;
    if (l2flow::canonical::CanonicalNormalizerV1::Create(
            std::move(normalizer_config), &candidate.normalizer) !=
            l2flow::canonical::CanonicalNormalizerCreateErrorV1::kNone) {
        if (diagnostic != nullptr) {
            *diagnostic = "Canonical normalizer creation failed";
        }
        return false;
    }

    std::int64_t created_realtime_ns = 0;
    std::int64_t created_monotonic_ns = 0;
    if (!ToSignedTime(RawRealtimeNow(nullptr), &created_realtime_ns) ||
        !ToSignedTime(RawStreamEpochNow(nullptr), &created_monotonic_ns)) {
        if (diagnostic != nullptr) {
            *diagnostic = "host clocks are unavailable";
        }
        return false;
    }
    candidate.sinks.reserve(34U);
    for (std::uint32_t shard = 0U; shard < 16U; ++shard) {
        if (!AddCanonicalSink(
                deployment,
                static_inputs,
                clock_epoch,
                build_sha256,
                deployment.manifest_sha256,
                source,
                l2flow::canonical::CanonicalFamilyV1::kSnapshot,
                shard,
                l2flow::canonical::CanonicalEventTypeV1::kSnapshot,
                static_cast<std::uint32_t>(
                    l2flow::canonical::kCanonicalSnapshotRecordBytesV1),
                created_realtime_ns,
                created_monotonic_ns,
                &candidate.sinks,
                diagnostic) ||
            !AddCanonicalSink(
                deployment,
                static_inputs,
                clock_epoch,
                build_sha256,
                deployment.manifest_sha256,
                source,
                l2flow::canonical::CanonicalFamilyV1::kTick,
                shard,
                l2flow::canonical::CanonicalEventTypeV1::kTick,
                static_cast<std::uint32_t>(
                    l2flow::canonical::kCanonicalTickRecordBytesV1),
                created_realtime_ns,
                created_monotonic_ns,
                &candidate.sinks,
                diagnostic)) {
            return false;
        }
    }
    if (!AddCanonicalSink(
            deployment,
            static_inputs,
            clock_epoch,
            build_sha256,
            deployment.manifest_sha256,
            source,
            l2flow::canonical::CanonicalFamilyV1::kQuality,
            0U,
            l2flow::canonical::CanonicalEventTypeV1::kQuality,
            static_cast<std::uint32_t>(
                l2flow::canonical::kCanonicalQualityRecordBytesV1),
            created_realtime_ns,
            created_monotonic_ns,
            &candidate.sinks,
            diagnostic) ||
        !AddCanonicalSink(
            deployment,
            static_inputs,
            clock_epoch,
            build_sha256,
            deployment.manifest_sha256,
            source,
            l2flow::canonical::CanonicalFamilyV1::kControl,
            0U,
            l2flow::canonical::CanonicalEventTypeV1::kControl,
            static_cast<std::uint32_t>(
                l2flow::canonical::kCanonicalControlRecordBytesV1),
            created_realtime_ns,
            created_monotonic_ns,
            &candidate.sinks,
            diagnostic)) {
        return false;
    }

    candidate.pipeline_config.source_slot = static_cast<std::uint8_t>(source);
    candidate.pipeline_config.ingress_kind = source_config.kind;
    candidate.pipeline_config.trade_date = deployment.trade_date;
    candidate.pipeline_config.source_generation =
        source_config.source_generation;
    candidate.pipeline_config.canonical_generation =
        source_config.canonical_generation;
    candidate.pipeline_config.normalizer_build_sha256 = build_sha256;
    candidate.pipeline_config.normalizer_config_sha256 =
        deployment.manifest_sha256;

    candidate.raw_app_config.stable = stable;
    candidate.raw_app_config.endpoint = static_inputs.endpoints[source];
    candidate.raw_app_config.credential_token =
        static_inputs.credential_token;
    candidate.raw_app_config.connect_generation =
        source_config.connect_generation;
    auto& runtime = candidate.raw_app_config.recovered;
    runtime.source_stream_id = spec.source_stream_id;
    runtime.capture_date = deployment.capture_date;
    runtime.stream_day_id = source_config.stream_day_id;
    runtime.recovered_next_ingress_sequence = 1U;
    runtime.append.segment_sequence = 1U;
    runtime.append.global_wal_pos = l2flow::ingress::kRawV1SegmentHeaderBytes;
    runtime.append.ingress_sequence = 0U;
    runtime.append.segment_offset = l2flow::ingress::kRawV1SegmentHeaderBytes;
    runtime.durable = runtime.append;
    runtime.writer_instance = source_config.writer_instance;
    runtime.current_segment_sequence = 1U;
    runtime.clock_epoch_algorithm_version = stable.clock_epoch_algorithm_version;
    runtime.clock_epoch_digest = static_inputs.clock_epoch.digest;
    runtime.clock_epoch_label = static_inputs.clock_epoch.value;

    l2flow::ingress::SegmentHeaderV1 segment{};
    segment.source_stream_id = spec.source_stream_id;
    segment.capture_date = deployment.capture_date;
    segment.stream_day_id = source_config.stream_day_id;
    segment.segment_sequence = 1U;
    segment.segment_base_wal_pos = 0U;
    segment.first_ingress_sequence = 1U;
    segment.created_realtime_ns = static_cast<std::uint64_t>(created_realtime_ns);
    segment.created_monotonic_ns =
        static_cast<std::uint64_t>(created_monotonic_ns);
    segment.host_uuid = static_inputs.host_uuid;
    segment.linux_boot_id = static_inputs.linux_boot_id;
    segment.clock_epoch_algorithm = stable.clock_epoch_algorithm_version;
    segment.clock_epoch_digest = static_inputs.clock_epoch.digest;
    segment.clock_epoch_label = static_inputs.clock_epoch.value;
    // The production SDK policy is deliberately path-only.  No archive is
    // used and no library digest is computed, so both legacy fixed-width Raw
    // provenance fields are zero (unavailable) rather than carrying a false
    // baseline archive or a value that could be mistaken for an approval pin.
    segment.sdk_archive_sha256 = {};
    segment.libmdl_api_sha256 = {};
    segment.endpoint_contract_sha256 = source_config.endpoint_sha256;
    segment.config_sha256 = stable_config_sha256;
    segment.raw_schema_sha256 = l2flow::ingress::RawSchemaSha256Digest();
    segment.build_manifest_sha256 = build_sha256;

    l2flow::ingress::DurableJournalHeaderV1 journal{};
    journal.capture_date = segment.capture_date;
    journal.source_stream_id = segment.source_stream_id;
    journal.stream_day_id = segment.stream_day_id;
    journal.raw_schema_sha256 = segment.raw_schema_sha256;
    journal.created_host_uuid = segment.host_uuid;
    journal.created_linux_boot_id = segment.linux_boot_id;
    journal.created_clock_epoch_algorithm = segment.clock_epoch_algorithm;
    journal.created_clock_epoch_digest = segment.clock_epoch_digest;
    journal.created_clock_epoch_label = segment.clock_epoch_label;
    if (l2flow::ingress::EncodeSegmentHeaderV1(
            segment, &candidate.writer_config.segment_header_wire) !=
            l2flow::ingress::RawV1Error::kNone ||
        l2flow::ingress::EncodeDurableJournalHeaderV1(
            journal, &candidate.writer_config.journal_header_wire) !=
            l2flow::ingress::RawV1Error::kNone) {
        if (diagnostic != nullptr) {
            *diagnostic = "fresh Raw headers could not be encoded";
        }
        return false;
    }
    candidate.writer_config.source_stream_id = spec.source_stream_id;
    candidate.writer_config.capture_date = deployment.capture_date;
    candidate.writer_config.segment_sequence = 1U;
    candidate.writer_config.segment_base_wal_pos = 0U;
    candidate.writer_config.first_ingress_sequence = 1U;
    candidate.writer_config.initial_durable_ingress_sequence = 0U;
    candidate.writer_config.writer_instance = source_config.writer_instance;
    candidate.writer_config.initialization_mode =
        l2flow::ingress::RawWalInitializationMode::kFreshJournal;
    candidate.writer_config.headers_already_persisted = false;

    candidate.backend_options.writer_instance = source_config.writer_instance;
    candidate.backend_options.segment_preallocation_bytes =
        deployment.raw_segment_target_bytes;
    candidate.backend_options.maximum_manifest_bytes =
        deployment.raw_maximum_manifest_bytes;
    candidate.backend_options.realtime_now = &RawRealtimeNow;
    candidate.backend_options.monotonic_now = &RawHeartbeatMonotonicNow;
    candidate.artifact_options.expected_raw_schema_sha256 =
        segment.raw_schema_sha256;
    candidate.artifact_options.sample_record_interval =
        deployment.raw_sparse_index_every_records;
    candidate.artifact_options.sample_raw_bytes_interval =
        deployment.raw_sparse_index_every_bytes;
    candidate.artifact_options.maximum_segment_bytes =
        deployment.raw_live_max_segment_bytes;
    l2flow::ingress::RawRecordLayoutV1 maximum_record{};
    if (l2flow::ingress::ComputeRawRecordLayoutV1(
            deployment.raw_max_message_bytes -
                l2flow::ingress::kVendorMessageHeadBytes,
            &maximum_record) != l2flow::ingress::RawV1Error::kNone ||
        deployment.raw_segment_max_age_seconds >
            std::numeric_limits<std::uint64_t>::max() /
                UINT64_C(1'000'000'000)) {
        if (diagnostic != nullptr) {
            *diagnostic = "Raw maximum record or segment age is invalid";
        }
        return false;
    }
    candidate.stream_limits.segment_target_bytes =
        deployment.raw_segment_target_bytes;
    candidate.stream_limits.segment_max_age_ns =
        static_cast<std::uint64_t>(deployment.raw_segment_max_age_seconds) *
        UINT64_C(1'000'000'000);
    candidate.stream_limits.maximum_record_bytes = maximum_record.record_size;
    candidate.stream_limits.monotonic_now = &RawStreamEpochNow;

    candidate.raw_app_options.sdk_log_runtime_prefix =
        candidate.lifetime->sdk_log->stable_prefix();
    candidate.raw_app_options.source_frontier =
        candidate.lifetime->frontier->page();
    candidate.raw_app_options.frontier_writer_instance =
        source_config.writer_instance;
    candidate.raw_app_options.frontier_generation =
        source_config.source_generation;
    candidate.raw_app_options.writer_idle_heartbeat_interval_ns =
        deployment.writer_idle_heartbeat_interval_ms * UINT64_C(1'000'000);
    candidate.live_tail_limits.max_segments =
        deployment.raw_live_max_segments;
    candidate.live_tail_limits.max_segment_bytes =
        deployment.raw_live_max_segment_bytes;
    candidate.live_tail_limits.max_journal_markers =
        deployment.raw_live_max_journal_markers;
    candidate.live_tail_limits.max_control_reattach_attempts =
        deployment.raw_live_max_control_reattach_attempts;
    *output = std::move(candidate);
    return true;
}

[[nodiscard]] l2flow::route::ProductionRouteManifestV1 BuildRouteManifest(
    const ProductionDeploymentV1& deployment,
    const l2flow::common::Sha256Digest& build_sha256,
    const l2flow::canonical::ClockEpochIdentityV1& clock_epoch) {
    l2flow::route::ProductionRouteManifestV1 manifest{};
    manifest.state = l2flow::route::ProductionRouteStateV1::kActive;
    manifest.generation = deployment.route_generation;
    manifest.previous_generation = deployment.route_previous_generation;
    manifest.route_instance = deployment.route_instance;
    manifest.trade_date = deployment.trade_date;
    manifest.registry_version = deployment.instrument_registry_version;
    manifest.registry_sha256 = deployment.instrument_registry_sha256;
    manifest.schema_sha256 =
        l2flow::canonical::CanonicalSchemaDescriptorSha256V1();
    manifest.build_sha256 = build_sha256;
    manifest.config_sha256 = deployment.manifest_sha256;
    for (std::size_t source = 0U; source < deployment.sources.size();
         ++source) {
        const auto& config = deployment.sources[source];
        const auto& spec = l2flow::sdk::GetIngressSpec(config.kind);
        auto& route_source = manifest.sources[source];
        route_source.source_stream_id = spec.source_stream_id;
        route_source.capture_date = deployment.capture_date;
        route_source.stream_day_id = config.stream_day_id;
        route_source.writer_instance = config.writer_instance;
        route_source.source_generation = config.source_generation;
        route_source.canonical_generation = config.canonical_generation;
        route_source.clock_epoch_identity_sha256 =
            l2flow::route::ComputeProductionRouteClockEpochIdentitySha256V1(
                clock_epoch);
    }
    manifest.endpoints.canonical = deployment.canonical_root;
    return manifest;
}

[[nodiscard]] bool BuildProductionService(
    const ProductionDeploymentV1& deployment,
    ProductionStaticInputsV1* static_inputs,
    std::unique_ptr<l2flow::apps::ProductionServiceV1>* output,
    bool* fail_stop_required,
    std::string* diagnostic) {
    if (static_inputs == nullptr || output == nullptr ||
        fail_stop_required == nullptr) {
        return false;
    }
    output->reset();
    *fail_stop_required = false;

    l2flow::common::Sha256Digest build_sha256{};
    if (!BuildDigest(l2flow::build_manifest::kSha256, &build_sha256)) {
        if (diagnostic != nullptr) {
            *diagnostic = "compiled build digest is invalid";
        }
        return false;
    }

    // The manifest path is the complete operator SDK selection.  Static
    // preflight checked only that it names an existing regular file.  Startup
    // uses that exact path and applies no SDK identity/approval validation.
    std::shared_ptr<l2flow::sdk::SdkFactory> sdk_factory =
        l2flow::sdk::LoadOperatorSelectedSdkFactory(
            deployment.sdk_library_path,
            diagnostic);
    if (sdk_factory == nullptr) {
        return false;
    }

    l2flow::ingress::RawReserveCoordinatorErrorV1 coordinator_error =
        l2flow::ingress::RawReserveCoordinatorErrorV1::kNone;
    auto coordinator =
        l2flow::ingress::AttachRawReserveRegistryCoordinatorAtV1(
            static_inputs->raw_directory.get(),
            CoordinatorMarker(deployment),
            &coordinator_error,
            diagnostic);
    if (coordinator == nullptr) {
        if (diagnostic != nullptr && diagnostic->empty()) {
            *diagnostic = "reserve coordinator attach failed: " +
                std::string(l2flow::ingress::RawReserveCoordinatorErrorNameV1(
                    coordinator_error));
        }
        return false;
    }
    std::array<
        std::unique_ptr<l2flow::ingress::RawReserveAuthorizedActionV1>,
        kProductionDeploymentSourceCountV1>
        scaffolding_actions{};
    for (std::size_t source = 0U; source < deployment.sources.size();
         ++source) {
        const auto registration = Registration(deployment, source);
        scaffolding_actions[source] = coordinator->AcquireAction(
            registration.key,
            l2flow::ingress::ReserveRegistryStatusV1::kScaffolding,
            &coordinator_error,
            diagnostic);
        const auto& action = scaffolding_actions[source];
        if (action == nullptr ||
            action->key() != registration.key ||
            action->required_status() !=
                l2flow::ingress::ReserveRegistryStatusV1::kScaffolding ||
            action->recovery_intent() !=
                l2flow::ingress::ReserveRecoveryIntentV1::kResumeConnect ||
            action->token().writer_instance_id !=
                registration.writer_instance ||
            action->token().recovery_attempt_id !=
                registration.key.recovery_attempt_id ||
            !action->ValidateLatest(diagnostic)) {
            if (diagnostic != nullptr && diagnostic->empty()) {
                *diagnostic =
                    "fresh-only startup requires four exact SCAFFOLDING entries";
            }
            return false;
        }
    }
    try {
        if (!ExactFreshScaffoldingSet(deployment, coordinator->state())) {
            if (diagnostic != nullptr) {
                *diagnostic =
                    "fresh-only startup requires exactly four provisioned "
                    "fresh SCAFFOLDING entries matching the deployment";
            }
            return false;
        }
    } catch (...) {
        if (diagnostic != nullptr) {
            *diagnostic = "reserve coordinator state validation failed";
        }
        return false;
    }

    l2flow::market::InstrumentHistoryRuntimeConfigV1 history_config{};
    history_config.source_stream_ids =
        l2flow::route::kProductionRouteSourceStreamIdsV1;
    history_config.physical_worker_count =
        deployment.history_physical_workers;
    history_config.queue_capacity = deployment.history_queue_capacity;
    history_config.maximum_inflight_per_source =
        deployment.history_maximum_inflight_per_source;
    history_config.chunk_record_capacity =
        deployment.history_chunk_record_capacity;
    history_config.maximum_records_per_query =
        deployment.history_maximum_records_per_query;
    history_config.maximum_records_per_logical_shard =
        deployment.history_maximum_records_per_shard;
    history_config.maximum_instruments_per_logical_shard =
        deployment.history_maximum_instruments_per_shard;
    history_config.maximum_owned_payload_bytes_per_logical_shard =
        deployment.history_maximum_payload_bytes_per_shard;
    std::unique_ptr<l2flow::market::InstrumentHistoryRuntimeV1> history;
    const auto history_error =
        l2flow::market::InstrumentHistoryRuntimeV1::Create(
            history_config, &history);
    if (history_error !=
            l2flow::market::InstrumentHistoryCreateErrorV1::kNone ||
        history == nullptr) {
        if (diagnostic != nullptr) {
            *diagnostic = "instrument history creation failed: " +
                std::string(
                    l2flow::market::InstrumentHistoryCreateErrorNameV1(
                        history_error));
        }
        return false;
    }

    l2flow::canonical::ClockEpochIdentityV1 clock_epoch{};
    clock_epoch.algorithm = 1U;
    clock_epoch.digest = static_inputs->clock_epoch.digest;
    clock_epoch.label = static_inputs->clock_epoch.value;
    auto route_manifest =
        BuildRouteManifest(deployment, build_sha256, clock_epoch);
    const auto manifest_error =
        l2flow::route::ValidateProductionRouteManifestV1(route_manifest);
    if (manifest_error !=
        l2flow::route::ProductionRouteManifestErrorV1::kNone) {
        if (diagnostic != nullptr) {
            *diagnostic = "production route manifest rejected: " +
                std::string(
                    l2flow::route::ProductionRouteManifestErrorNameV1(
                        manifest_error));
        }
        return false;
    }
    std::unique_ptr<l2flow::route::ProductionRouteControllerV1>
        route_controller;
    try {
        route_controller = std::make_unique<
            l2flow::route::ProductionRouteControllerV1>(
            static_inputs->route_directory.get(), std::move(route_manifest),
            l2flow::route::ProductionRouteControllerPolicyV1::kFreshOnly,
            l2flow::route::ProductionRoutePathIdentityGuardV1{
                static_inputs->canonical_directory.get(),
                deployment.route_root,
                deployment.canonical_root});
    } catch (...) {
        if (diagnostic != nullptr) {
            *diagnostic = "production route controller allocation failed";
        }
        return false;
    }

    std::array<SourceAssemblyV1, kProductionDeploymentSourceCountV1>
        assemblies{};
    // The following factories create the formal generation's frontier and
    // Canonical artifacts.  Any failure from this point consumes the fresh
    // namespace and requires explicit reprovision/recovery; it is never an
    // in-place retry.  The shared reserve actions above fence coordinator
    // transitions throughout these non-Raw mutations.
    *fail_stop_required = true;
    for (std::size_t source = 0U; source < assemblies.size(); ++source) {
        if (!BuildSourceAssembly(
                deployment,
                *static_inputs,
                coordinator,
                build_sha256,
                source,
                &assemblies[source],
                diagnostic)) {
            return false;
        }
    }

    // Raw activation acquires a fresh action of its own and repeats the exact
    // registration check while that action's generation gate is held.  Drop
    // these preparation guards before requesting the exclusive transition.
    for (auto& action : scaffolding_actions) {
        action.reset();
    }

    l2flow::apps::ProductionServiceInputsV1 service_inputs{};
    service_inputs.registry = std::move(static_inputs->registry);
    service_inputs.history = std::move(history);
    service_inputs.route_controller = std::move(route_controller);
    for (std::size_t source = 0U; source < assemblies.size(); ++source) {
        SourceAssemblyV1& assembly = assemblies[source];
        auto raw_result = l2flow::ingress::
            RawExistingRouteProductionRuntimeFactoryV1::
                ActivateFreshRegisteredAt(
                    static_inputs->raw_directory.get(),
                    deployment.raw_root,
                    deployment.sources[source].stream_slug,
                    assembly.registration,
                    *coordinator,
                    assembly.writer_config,
                    assembly.backend_options,
                    assembly.artifact_options,
                    assembly.stream_limits,
                    assembly.raw_app_config,
                    sdk_factory,
                    std::make_unique<l2flow::ingress::LinuxCaptureClock>(),
                    assembly.live_tail_limits,
                    assembly.raw_app_options,
                    nullptr,
                    diagnostic);
        if (!raw_result.ok()) {
            *fail_stop_required =
                *fail_stop_required || raw_result.requires_fail_stop();
            if (diagnostic != nullptr && diagnostic->empty()) {
                *diagnostic = "fresh Raw SCAFFOLDING activation failed";
            }
            return false;
        }
        *fail_stop_required = true;
        std::unique_ptr<l2flow::ingress::RawProductionRuntimeV1> raw_runtime =
            std::move(raw_result.runtime);
        std::unique_ptr<l2flow::ingress::RawLiveTail> pipeline_tail =
            raw_runtime->TakeFreshPipelineLiveTail();
        if (pipeline_tail == nullptr) {
            if (diagnostic != nullptr) {
                *diagnostic = "fresh Raw runtime did not provide a pipeline tail";
            }
            return false;
        }
        const auto pipeline_error =
            l2flow::runtime::ProductionSourcePipelineV1::Create(
                assembly.pipeline_config,
                std::move(pipeline_tail),
                std::move(assembly.control_decoder),
                std::move(assembly.normalizer),
                assembly.lifetime->frontier->page(),
                std::move(assembly.sinks),
                service_inputs.history.get(),
                &service_inputs.pipelines[source]);
        if (pipeline_error !=
                l2flow::runtime::ProductionSourceCreateErrorV1::kNone ||
            service_inputs.pipelines[source] == nullptr) {
            if (diagnostic != nullptr) {
                *diagnostic = "production source pipeline creation failed: " +
                    std::string(
                        l2flow::runtime::ProductionSourceCreateErrorNameV1(
                            pipeline_error));
            }
            return false;
        }
        service_inputs.captures[source] =
            l2flow::apps::OwnRawProductionCaptureRuntimeV1(
                std::move(raw_runtime));
        if (service_inputs.captures[source] == nullptr) {
            if (diagnostic != nullptr) {
                *diagnostic = "Raw capture ownership adapter allocation failed";
            }
            return false;
        }
        service_inputs.source_lifetimes[source] =
            std::move(assembly.lifetime);
    }

    l2flow::apps::ProductionServiceConfigV1 service_config{};
    service_config.aggregate.writer_heartbeat_timeout =
        std::chrono::milliseconds(deployment.writer_heartbeat_timeout_ms);
    service_config.activation_timeout =
        std::chrono::milliseconds(deployment.activation_timeout_ms);
    service_config.drain_timeout =
        std::chrono::milliseconds(deployment.drain_timeout_ms);
    l2flow::runtime::ProductionAggregateCreateErrorV1 aggregate_error =
        l2flow::runtime::ProductionAggregateCreateErrorV1::kNone;
    const auto service_error = l2flow::apps::ProductionServiceV1::Create(
        service_config,
        std::move(service_inputs),
        output,
        &aggregate_error);
    if (service_error !=
            l2flow::apps::ProductionServiceCreateErrorV1::kNone ||
        *output == nullptr) {
        if (diagnostic != nullptr) {
            *diagnostic = "production service creation failed: " +
                std::string(
                    l2flow::apps::ProductionServiceCreateErrorNameV1(
                        service_error));
        }
        return false;
    }
    *fail_stop_required = false;
    return true;
}

class ScopedTerminationSignalBlock final {
public:
    ScopedTerminationSignalBlock() noexcept {
        if (::sigemptyset(&wait_set_) != 0 ||
            ::sigaddset(&wait_set_, SIGINT) != 0 ||
            ::sigaddset(&wait_set_, SIGTERM) != 0) {
            error_number_ = errno;
            return;
        }
        const int result = ::pthread_sigmask(SIG_BLOCK, &wait_set_, &old_set_);
        if (result != 0) {
            error_number_ = result;
            return;
        }
        blocked_ = true;
    }

    ScopedTerminationSignalBlock(const ScopedTerminationSignalBlock&) = delete;
    ScopedTerminationSignalBlock& operator=(
        const ScopedTerminationSignalBlock&) = delete;

    ~ScopedTerminationSignalBlock() {
        if (blocked_) {
            static_cast<void>(
                ::pthread_sigmask(SIG_SETMASK, &old_set_, nullptr));
        }
    }

    [[nodiscard]] bool ok() const noexcept { return blocked_; }
    [[nodiscard]] int error_number() const noexcept { return error_number_; }

    // Returns SIGINT/SIGTERM when consumed, zero on timeout, and -1 on an
    // unexpected sigtimedwait failure. All runtime threads inherit the block,
    // so the process entry thread is the sole lifecycle signal consumer.
    [[nodiscard]] int Wait(std::uint32_t timeout_milliseconds,
                           int* error_number) const noexcept {
        if (error_number != nullptr) {
            *error_number = 0;
        }
        struct timespec timeout {};
        timeout.tv_sec = static_cast<time_t>(timeout_milliseconds / 1000U);
        timeout.tv_nsec = static_cast<long>(
            (timeout_milliseconds % 1000U) * 1000000U);
        int result = 0;
        do {
            result = ::sigtimedwait(&wait_set_, nullptr, &timeout);
        } while (result < 0 && errno == EINTR);
        if (result < 0 && errno == EAGAIN) {
            return 0;
        }
        if (result < 0 && error_number != nullptr) {
            *error_number = errno;
        }
        return result;
    }

private:
    sigset_t wait_set_{};
    sigset_t old_set_{};
    int error_number_ = 0;
    bool blocked_ = false;
};

[[nodiscard]] bool AggregateFatal(
    const ProductionServiceSnapshotV1& snapshot) noexcept {
    return snapshot.aggregate.state ==
               l2flow::runtime::ProductionAggregateStateV1::kFatal ||
           snapshot.aggregate.fatal_reason_code != 0U;
}

[[nodiscard]] int StopForTermination(
    ProductionServiceV1* service,
    bool fatal,
    bool internal_wait_failure) noexcept {
    if (service == nullptr) {
        return 70;
    }
    const ProductionServiceStopResultV1 stopped = service->Stop();
    if (fatal) {
        std::cerr << "production route entered Fatal state";
        if (stopped.ok()) {
            std::cerr << " and was stopped cleanly\n";
        } else {
            std::cerr << "; shutdown failed: "
                      << ProductionServiceStopErrorNameV1(stopped.error)
                      << '\n';
        }
        return 1;
    }
    if (internal_wait_failure) {
        std::cerr << "production signal wait failed";
        if (stopped.ok()) {
            std::cerr << "; route was stopped cleanly\n";
        } else {
            std::cerr << "; shutdown failed: "
                      << ProductionServiceStopErrorNameV1(stopped.error)
                      << '\n';
        }
        return 70;
    }
    if (!stopped.ok()) {
        std::cerr << "production shutdown failed: "
                  << ProductionServiceStopErrorNameV1(stopped.error) << '\n';
        return 1;
    }
    std::cout << "production route stopped cleanly\n";
    return 0;
}

[[nodiscard]] std::string_view ProductionServiceStateText(
    ProductionServiceStateV1 state) noexcept {
    switch (state) {
        case ProductionServiceStateV1::kReady: return "ready";
        case ProductionServiceStateV1::kStarting: return "starting";
        case ProductionServiceStateV1::kActive: return "active";
        case ProductionServiceStateV1::kStopping: return "stopping";
        case ProductionServiceStateV1::kStopped: return "stopped";
        case ProductionServiceStateV1::kFailed: return "failed";
    }
    return "invalid";
}

[[nodiscard]] std::string_view ProductionAggregateStateText(
    l2flow::runtime::ProductionAggregateStateV1 state) noexcept {
    using State = l2flow::runtime::ProductionAggregateStateV1;
    switch (state) {
        case State::kRunning: return "running";
        case State::kDraining: return "draining";
        case State::kStopping: return "stopping";
        case State::kStopped: return "stopped";
        case State::kFatal: return "fatal";
    }
    return "invalid";
}

[[nodiscard]] std::string_view RawIngressAppStateText(
    l2flow::ingress::RawIngressAppState state) noexcept {
    using State = l2flow::ingress::RawIngressAppState;
    switch (state) {
        case State::kConstructed: return "constructed";
        case State::kInitializing: return "initializing";
        case State::kRunning: return "running";
        case State::kStopping: return "stopping";
        case State::kStopped: return "stopped";
    }
    return "invalid";
}

[[nodiscard]] std::string_view RawCaptureFailureText(
    l2flow::ingress::RawCaptureWorkerFailureKind kind) noexcept {
    using Kind = l2flow::ingress::RawCaptureWorkerFailureKind;
    switch (kind) {
        case Kind::kNone: return "none";
        case Kind::kRunAlreadyStarted: return "run_already_started";
        case Kind::kWriterNotReady: return "writer_not_ready";
        case Kind::kRingCorruption: return "ring_corruption";
        case Kind::kWriterAppend: return "writer_append";
        case Kind::kWriterFlush: return "writer_flush";
        case Kind::kWriterSeal: return "writer_seal";
        case Kind::kProgressOverflow: return "progress_overflow";
        case Kind::kClockRegression: return "clock_regression";
        case Kind::kSourceFrontier: return "source_frontier";
    }
    return "invalid";
}

[[nodiscard]] std::string_view RawWalFailureText(
    l2flow::ingress::RawWalFailureKind kind) noexcept {
    using Kind = l2flow::ingress::RawWalFailureKind;
    switch (kind) {
        case Kind::kNone: return "none";
        case Kind::kInvalidState: return "invalid_state";
        case Kind::kInvalidConfiguration: return "invalid_configuration";
        case Kind::kInvalidRecord: return "invalid_record";
        case Kind::kCursorOverflow: return "cursor_overflow";
        case Kind::kSegmentWrite: return "segment_write";
        case Kind::kSegmentSync: return "segment_sync";
        case Kind::kSegmentTruncate: return "segment_truncate";
        case Kind::kJournalWrite: return "journal_write";
        case Kind::kJournalSync: return "journal_sync";
        case Kind::kClose: return "close";
        case Kind::kCommitObserver: return "commit_observer";
        case Kind::kRotationPolicy: return "rotation_policy";
        case Kind::kRotationArtifact: return "rotation_artifact";
        case Kind::kRotationFactory: return "rotation_factory";
        case Kind::kManifestPublish: return "manifest_publish";
        case Kind::kControlPublish: return "control_publish";
    }
    return "invalid";
}

void WriteJsonString(std::ostream& output, std::string_view value) {
    constexpr char kHex[] = "0123456789abcdef";
    output.put('"');
    for (const unsigned char character : value) {
        switch (character) {
            case '"': output << "\\\""; break;
            case '\\': output << "\\\\"; break;
            case '\b': output << "\\b"; break;
            case '\f': output << "\\f"; break;
            case '\n': output << "\\n"; break;
            case '\r': output << "\\r"; break;
            case '\t': output << "\\t"; break;
            default:
                if (character < 0x20U) {
                    output << "\\u00" << kHex[character >> 4U]
                           << kHex[character & 0x0FU];
                } else {
                    output.put(static_cast<char>(character));
                }
                break;
        }
    }
    output.put('"');
}

void WriteRouteControllerResult(
    std::ostream& output,
    const l2flow::route::ProductionRouteControllerResultV1& result) {
    output << "{\"error\":";
    WriteJsonString(
        output,
        l2flow::route::ProductionRouteControllerErrorNameV1(result.error));
    output << ",\"system_error_number\":" << result.system_error_number
           << ",\"already_complete\":"
           << (result.already_complete ? "true" : "false")
           << ",\"owner_lease_error\":"
           << static_cast<unsigned int>(result.owner_lease_error)
           << ",\"publish\":{\"error\":";
    WriteJsonString(
        output,
        l2flow::route::ProductionRouteStoreErrorNameV1(
            result.publish_result.error));
    output << ",\"manifest_error\":"
           << static_cast<unsigned int>(result.publish_result.manifest_error)
           << ",\"disposition\":"
           << static_cast<unsigned int>(result.publish_result.disposition)
           << ",\"generation\":" << result.publish_result.generation
           << ",\"state\":"
           << static_cast<unsigned int>(result.publish_result.state)
           << ",\"encoded_sha256\":";
    WriteJsonString(
        output,
        l2flow::common::Sha256Hex(result.publish_result.encoded_sha256));
    output << ",\"file_synced\":"
           << (result.publish_result.file_synced ? "true" : "false")
           << ",\"renamed\":"
           << (result.publish_result.renamed ? "true" : "false")
           << ",\"directory_synced\":"
           << (result.publish_result.directory_synced ? "true" : "false")
           << "}}";
}

void WriteCaptureStopEvidence(
    std::ostream& output,
    const ProductionCaptureRuntimeV1::SnapshotV1& evidence) {
    const auto& callback = evidence.callback;
    const auto& capture = evidence.capture;
    const auto& reconciliation = evidence.reconciliation;
    const auto& wal = evidence.wal;
    output << "{\"available\":"
           << (evidence.available ? "true" : "false")
           << ",\"fatal\":" << (evidence.fatal ? "true" : "false")
           << ",\"state\":";
    WriteJsonString(output, RawIngressAppStateText(evidence.state));
    output << ",\"callback\":{\"invocations\":"
           << callback.callback_invocations
           << ",\"captured_records\":" << callback.captured_records
           << ",\"captured_vendor_bytes\":"
           << callback.captured_vendor_bytes
           << ",\"captured_framed_wal_bytes\":"
           << callback.captured_framed_wal_bytes
           << ",\"captured_ingress_sequence\":"
           << callback.captured_ingress_sequence
           << ",\"callback_reentry\":" << callback.callback_reentry
           << ",\"callback_exceptions\":"
           << callback.callback_exceptions
           << ",\"callbacks_after_stop\":"
           << callback.callbacks_after_stop
           << ",\"callbacks_after_fatal\":"
           << callback.callbacks_after_fatal
           << ",\"ring_overflow\":" << callback.ring_overflow
           << ",\"callback_inflight\":"
           << (callback.callback_inflight ? "true" : "false")
           << "},\"capture\":{\"failure_kind\":";
    WriteJsonString(output, RawCaptureFailureText(capture.failure_kind));
    output << ",\"writer_failure_kind\":";
    WriteJsonString(output, RawWalFailureText(capture.writer_failure.kind));
    output << ",\"writer_failure_errno\":"
           << capture.writer_failure.error_number
           << ",\"stop_requested\":"
           << (capture.stop_requested ? "true" : "false")
           << ",\"startup_complete\":"
           << (capture.startup_complete ? "true" : "false")
           << ",\"startup_succeeded\":"
           << (capture.startup_succeeded ? "true" : "false")
           << ",\"finished\":"
           << (capture.finished ? "true" : "false")
           << ",\"append\":{\"records\":" << capture.append.records
           << ",\"vendor_bytes\":" << capture.append.vendor_bytes
           << ",\"framed_wal_bytes\":"
           << capture.append.framed_wal_bytes
           << ",\"last_ingress_sequence\":"
           << capture.append.last_ingress_sequence
           << "},\"durable\":{\"records\":"
           << capture.durable.records
           << ",\"vendor_bytes\":" << capture.durable.vendor_bytes
           << ",\"framed_wal_bytes\":"
           << capture.durable.framed_wal_bytes
           << ",\"last_ingress_sequence\":"
           << capture.durable.last_ingress_sequence
           << "}},\"reconciliation\":{\"callback_records\":"
           << reconciliation.callback_records
           << ",\"append_records\":" << reconciliation.append_records
           << ",\"durable_records\":" << reconciliation.durable_records
           << ",\"callback_vendor_bytes\":"
           << reconciliation.callback_vendor_bytes
           << ",\"append_vendor_bytes\":"
           << reconciliation.append_vendor_bytes
           << ",\"durable_vendor_bytes\":"
           << reconciliation.durable_vendor_bytes
           << ",\"exact\":"
           << (reconciliation.exact() ? "true" : "false")
           << "},\"wal\":{\"append\":{\"global_wal_pos\":"
           << wal.append.global_wal_pos
           << ",\"ingress_sequence\":" << wal.append.ingress_sequence
           << ",\"segment_offset\":" << wal.append.segment_offset
           << "},\"durable\":{\"global_wal_pos\":"
           << wal.durable.global_wal_pos
           << ",\"ingress_sequence\":" << wal.durable.ingress_sequence
           << ",\"segment_offset\":" << wal.durable.segment_offset
           << "},\"journal_logical_size\":"
           << wal.journal_logical_size
           << ",\"initialized\":"
           << (wal.initialized ? "true" : "false")
           << ",\"sealed\":" << (wal.sealed ? "true" : "false")
           << ",\"closed\":" << (wal.closed ? "true" : "false")
           << ",\"fatal\":" << (wal.fatal ? "true" : "false")
           << "},\"wal_failure\":{\"kind\":";
    WriteJsonString(output, RawWalFailureText(evidence.wal_failure.kind));
    output << ",\"errno\":" << evidence.wal_failure.error_number
           << "}}";
}

void WriteProductionSnapshot(
    std::ostream& output,
    const ProductionServiceSnapshotV1& snapshot) {
    output << "{\"service_state\":";
    WriteJsonString(output, ProductionServiceStateText(snapshot.state));
    output << ",\"capture_started\":[";
    for (std::size_t source = 0U;
         source < snapshot.capture_started.size(); ++source) {
        if (source != 0U) {
            output.put(',');
        }
        output << (snapshot.capture_started[source] ? "true" : "false");
    }
    const auto& aggregate = snapshot.aggregate;
    output << "],\"aggregate\":{\"state\":";
    WriteJsonString(output, ProductionAggregateStateText(aggregate.state));
    output << ",\"worker_exited\":[";
    for (std::size_t source = 0U;
         source < aggregate.worker_exited.size(); ++source) {
        if (source != 0U) {
            output.put(',');
        }
        output << (aggregate.worker_exited[source] ? "true" : "false");
    }
    output << "],\"active_route_published\":"
           << (aggregate.active_route_published ? "true" : "false")
           << ",\"active_route_publication_uncertain\":"
           << (aggregate.active_route_publication_uncertain ? "true" : "false")
           << ",\"fatal_route_published\":"
           << (aggregate.fatal_route_published ? "true" : "false")
           << ",\"drain_route_revoked\":"
           << (aggregate.drain_route_revoked ? "true" : "false")
           << ",\"route_operation_hook_active\":"
           << (aggregate.route_operation_hook_active ? "true" : "false")
           << ",\"source_frontier_busy\":[";
    for (std::size_t source = 0U;
         source < aggregate.source_frontier_busy.size(); ++source) {
        if (source != 0U) {
            output.put(',');
        }
        output << (aggregate.source_frontier_busy[source] ? "true" : "false");
    }
    output << "],\"fatal_reason_code\":"
           << aggregate.fatal_reason_code
           << ",\"last_active_publish\":";
    WriteRouteControllerResult(output, aggregate.last_active_publish);
    output << ",\"last_fatal_publish\":";
    WriteRouteControllerResult(output, aggregate.last_fatal_publish);
    output << ",\"sources\":[";
    for (std::size_t source = 0U; source < aggregate.sources.size();
         ++source) {
        if (source != 0U) {
            output.put(',');
        }
        const auto& value = aggregate.sources[source];
        const auto& history = value.history_frontier;
        output << "{\"source_slot\":" << source
               << ",\"source_slug\":";
        WriteJsonString(output, kSourceSlugs[source]);
        output << ",\"raw_records\":" << value.raw_records
               << ",\"market_records\":" << value.market_records
               << ",\"control_records\":" << value.control_records
               << ",\"no_output_records\":" << value.no_output_records
               << ",\"decode_quality_records\":"
               << value.decode_quality_records
               << ",\"segment_transitions\":"
               << value.segment_transitions
               << ",\"history_submissions\":"
               << value.history_submissions
               << ",\"history_frontier\":{\"submitted_ticket\":"
               << history.submitted_ticket
               << ",\"acknowledged_ticket\":"
               << history.acknowledged_ticket
               << ",\"submitted_source_sequence\":"
               << history.submitted_source_sequence
               << ",\"acknowledged_source_sequence\":"
               << history.acknowledged_source_sequence
               << ",\"completed_out_of_order\":"
               << history.completed_out_of_order
               << ",\"fatal\":"
               << (history.fatal ? "true" : "false")
               << "},\"history_pending\":"
               << (value.history_pending ? "true" : "false")
               << ",\"history_draining\":"
               << (value.history_draining ? "true" : "false")
               << ",\"ended\":" << (value.ended ? "true" : "false")
               << ",\"fatal\":" << (value.fatal ? "true" : "false")
               << ",\"terminal\":{\"kind\":"
               << static_cast<unsigned int>(value.terminal.kind)
               << ",\"failure\":";
        WriteJsonString(
            output,
            l2flow::runtime::ProductionSourceFailureNameV1(
                value.terminal.failure));
        output << ",\"raw_tail_error\":"
               << static_cast<unsigned int>(value.terminal.raw_tail_error)
               << ",\"raw_adapter_error\":"
               << static_cast<unsigned int>(value.terminal.raw_adapter_error)
               << ",\"frontier_error\":"
               << static_cast<unsigned int>(value.terminal.frontier_error)
               << ",\"control_error\":"
               << static_cast<unsigned int>(value.terminal.control_error)
               << ",\"decode_error\":"
               << static_cast<unsigned int>(value.terminal.decode_error)
               << ",\"retain_error\":"
               << static_cast<unsigned int>(value.terminal.retain_error)
               << ",\"canonical_error\":"
               << static_cast<unsigned int>(value.terminal.canonical_error)
               << ",\"history_create_error\":"
               << static_cast<unsigned int>(value.terminal.history_create_error)
               << ",\"history_submit_error\":"
               << static_cast<unsigned int>(value.terminal.history_submit_error)
               << ",\"history_barrier_error\":"
               << static_cast<unsigned int>(value.terminal.history_barrier_error)
               << ",\"ingress_sequence\":"
               << value.terminal.ingress_sequence
               << ",\"wal_pos\":" << value.terminal.wal_pos << "}}";
    }
    output << "]}}";
}

struct ProductionMonitorSampleV1 final {
    std::uint64_t elapsed_monotonic_raw_ns = 0U;
    std::uint64_t realtime_ns = 0U;
    ProductionServiceSnapshotV1 snapshot{};
};

struct ProductionMonitorEvidenceV1 final {
    std::uint32_t requested_run_seconds = 0U;
    std::string deployment_directory;
    std::string evidence_path;
    std::string manifest_sha256;
    std::string sdk_library_path;
    std::uint64_t active_start_monotonic_raw_ns = 0U;
    std::uint64_t active_start_realtime_ns = 0U;
    std::uint64_t monitor_end_monotonic_raw_ns = 0U;
    std::uint64_t monitor_end_realtime_ns = 0U;
    bool completed_window = false;
    bool fatal_observed = false;
    bool left_active = false;
    bool signal_wait_failed = false;
    bool clock_failed = false;
    int termination_signal = 0;
    std::string_view outcome = "not_started";
    ProductionServiceStartResultV1 start{};
    std::vector<ProductionMonitorSampleV1> samples;
    bool final_active_snapshot_valid = false;
    ProductionServiceSnapshotV1 final_active_snapshot{};
    ProductionServiceStopResultV1 stop{};
    ProductionServiceSnapshotV1 post_stop_snapshot{};
};

[[nodiscard]] bool SnapshotIsAuthoritativelyActive(
    const ProductionServiceSnapshotV1& snapshot) noexcept {
    if (snapshot.state != ProductionServiceStateV1::kActive ||
        snapshot.aggregate.state !=
            l2flow::runtime::ProductionAggregateStateV1::kRunning ||
        !snapshot.aggregate.active_route_published ||
        snapshot.aggregate.active_route_publication_uncertain ||
        snapshot.aggregate.fatal_route_published ||
        snapshot.aggregate.drain_route_revoked ||
        AggregateFatal(snapshot)) {
        return false;
    }
    for (std::size_t source = 0U;
         source < kProductionDeploymentSourceCountV1; ++source) {
        if (!snapshot.capture_started[source] ||
            snapshot.aggregate.worker_exited[source] ||
            snapshot.aggregate.sources[source].ended ||
            snapshot.aggregate.sources[source].fatal ||
            snapshot.aggregate.sources[source].history_frontier.fatal) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] bool ReserveEvidenceFile(
    int deployment_directory_fd,
    const std::filesystem::path& deployment_directory,
    const std::filesystem::path& evidence_path,
    OwnedFd* output,
    std::string* diagnostic) noexcept {
    if (output == nullptr || deployment_directory_fd < 0 ||
        evidence_path.parent_path() != deployment_directory ||
        evidence_path.filename().empty()) {
        if (diagnostic != nullptr) {
            try {
                *diagnostic =
                    "evidence path must be a direct child of the deployment directory";
            } catch (...) {
            }
        }
        return false;
    }
    try {
        const std::string filename = evidence_path.filename().string();
        const int descriptor = ::openat(
            deployment_directory_fd,
            filename.c_str(),
            O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
            S_IRUSR | S_IWUSR);
        if (descriptor < 0) {
            if (diagnostic != nullptr) {
                *diagnostic = "fresh evidence file creation failed (errno " +
                    std::to_string(errno) + ")";
            }
            return false;
        }
        struct stat metadata {};
        const bool safe = ::fstat(descriptor, &metadata) == 0 &&
            S_ISREG(metadata.st_mode) && metadata.st_uid == ::geteuid() &&
            metadata.st_nlink == 1 &&
            (metadata.st_mode & 07777U) == (S_IRUSR | S_IWUSR) &&
            metadata.st_size == 0;
        if (!safe || ::fsync(deployment_directory_fd) != 0) {
            const int saved_errno = errno;
            static_cast<void>(::close(descriptor));
            static_cast<void>(::unlinkat(
                deployment_directory_fd, filename.c_str(), 0));
            if (diagnostic != nullptr) {
                *diagnostic =
                    "fresh evidence file did not satisfy the secure creation contract (errno " +
                    std::to_string(saved_errno) + ")";
            }
            return false;
        }
        *output = OwnedFd(descriptor);
        if (diagnostic != nullptr) {
            diagnostic->clear();
        }
        return true;
    } catch (...) {
        if (diagnostic != nullptr) {
            try {
                *diagnostic = "fresh evidence file creation failed unexpectedly";
            } catch (...) {
            }
        }
        return false;
    }
}

[[nodiscard]] std::string EncodeMonitorEvidence(
    const ProductionMonitorEvidenceV1& evidence) {
    std::ostringstream output;
    output << "{\"schema\":\"l2flow.production-router-monitor.v1\""
           << ",\"requested_run_seconds\":"
           << evidence.requested_run_seconds
           << ",\"sample_clock\":\"CLOCK_MONOTONIC_RAW\""
           << ",\"sample_interval_seconds\":1"
           << ",\"deployment_directory\":";
    WriteJsonString(output, evidence.deployment_directory);
    output << ",\"evidence_path\":";
    WriteJsonString(output, evidence.evidence_path);
    output << ",\"manifest_sha256\":";
    WriteJsonString(output, evidence.manifest_sha256);
    output << ",\"sdk_library_path\":";
    WriteJsonString(output, evidence.sdk_library_path);
    output << ",\"sdk_acceptance_policy\":"
              "\"operator_path_exists_and_is_regular_file_only\""
           << ",\"sdk_digest_is_acceptance_gate\":false"
           << ",\"history_access\":\"in_process_only\""
           << ",\"latest_state_v1_wired\":false"
           << ",\"factor_runtime_wired\":false"
           << ",\"parquet_publication_wired\":false"
           << ",\"external_history_state_factor_endpoint\":false"
           << ",\"active_start_monotonic_raw_ns\":"
           << evidence.active_start_monotonic_raw_ns
           << ",\"active_start_realtime_ns\":"
           << evidence.active_start_realtime_ns
           << ",\"monitor_end_monotonic_raw_ns\":"
           << evidence.monitor_end_monotonic_raw_ns
           << ",\"monitor_end_realtime_ns\":"
           << evidence.monitor_end_realtime_ns
           << ",\"observed_monotonic_raw_ns\":"
           << (evidence.monitor_end_monotonic_raw_ns >=
                       evidence.active_start_monotonic_raw_ns
                   ? evidence.monitor_end_monotonic_raw_ns -
                         evidence.active_start_monotonic_raw_ns
                   : 0U)
           << ",\"completed_window\":"
           << (evidence.completed_window ? "true" : "false")
           << ",\"fatal_observed\":"
           << (evidence.fatal_observed ? "true" : "false")
           << ",\"left_active\":"
           << (evidence.left_active ? "true" : "false")
           << ",\"signal_wait_failed\":"
           << (evidence.signal_wait_failed ? "true" : "false")
           << ",\"clock_failed\":"
           << (evidence.clock_failed ? "true" : "false")
           << ",\"termination_signal\":"
           << evidence.termination_signal << ",\"outcome\":";
    WriteJsonString(output, evidence.outcome);
    output << ",\"start\":{\"error\":";
    WriteJsonString(
        output, ProductionServiceStartErrorNameV1(evidence.start.error));
    output << ",\"source_slot\":"
           << static_cast<unsigned int>(evidence.start.source_slot)
           << ",\"already_active\":"
           << (evidence.start.already_active ? "true" : "false")
           << ",\"diagnostic\":";
    WriteJsonString(output, evidence.start.diagnostic);
    output << ",\"barrier_error\":";
    WriteJsonString(
        output,
        l2flow::runtime::ProductionAggregateBarrierErrorNameV1(
            evidence.start.barrier.error));
    output << ",\"barrier_source_slot\":"
           << static_cast<unsigned int>(evidence.start.barrier.source_slot)
           << ",\"publication_error\":";
    WriteJsonString(
        output,
        l2flow::runtime::ProductionAggregatePublishErrorNameV1(
            evidence.start.publication.error));
    output << ",\"publication_source_slot\":"
           << static_cast<unsigned int>(
                  evidence.start.publication.source_slot)
           << ",\"route\":";
    WriteRouteControllerResult(output, evidence.start.publication.route);
    output << "},\"samples\":[";
    for (std::size_t index = 0U; index < evidence.samples.size(); ++index) {
        if (index != 0U) {
            output.put(',');
        }
        const auto& sample = evidence.samples[index];
        output << "{\"sample_index\":" << index
               << ",\"elapsed_monotonic_raw_ns\":"
               << sample.elapsed_monotonic_raw_ns
               << ",\"realtime_ns\":" << sample.realtime_ns
               << ",\"snapshot\":";
        WriteProductionSnapshot(output, sample.snapshot);
        output.put('}');
    }
    output << "],\"final_active_snapshot_valid\":"
           << (evidence.final_active_snapshot_valid ? "true" : "false")
           << ",\"final_active_snapshot\":";
    WriteProductionSnapshot(output, evidence.final_active_snapshot);
    output << ",\"stop\":{\"error\":";
    WriteJsonString(
        output, ProductionServiceStopErrorNameV1(evidence.stop.error));
    output << ",\"already_stopped\":"
           << (evidence.stop.already_stopped ? "true" : "false")
           << ",\"begin_drain_error\":";
    WriteJsonString(
        output,
        l2flow::runtime::ProductionAggregateBeginDrainErrorNameV1(
            evidence.stop.begin_drain.error));
    output << ",\"route_revocation_required\":"
           << (evidence.stop.begin_drain.route_revocation_required
                   ? "true" : "false")
           << ",\"begin_drain_route\":";
    WriteRouteControllerResult(output, evidence.stop.begin_drain.route);
    output << ",\"drain_error\":";
    WriteJsonString(
        output,
        l2flow::runtime::ProductionAggregateDrainWaitErrorNameV1(
            evidence.stop.drain.error));
    output << ",\"drain_source_slot\":"
           << static_cast<unsigned int>(evidence.stop.drain.source_slot)
           << ",\"capture_prepared\":[";
    for (std::size_t source = 0U;
         source < evidence.stop.capture_prepared.size(); ++source) {
        if (source != 0U) {
            output.put(',');
        }
        output << (evidence.stop.capture_prepared[source]
                       ? "true"
                       : "false");
    }
    output << "]"
           << ",\"capture_stopped\":[";
    for (std::size_t source = 0U;
         source < evidence.stop.capture_stopped.size(); ++source) {
        if (source != 0U) {
            output.put(',');
        }
        output << (evidence.stop.capture_stopped[source] ? "true" : "false");
    }
    output << "],\"capture_diagnostics\":[";
    for (std::size_t source = 0U;
         source < evidence.stop.capture_diagnostics.size(); ++source) {
        if (source != 0U) {
            output.put(',');
        }
        WriteJsonString(output, evidence.stop.capture_diagnostics[source]);
    }
    output << "],\"capture_evidence\":[";
    for (std::size_t source = 0U;
         source < evidence.stop.capture_evidence.size(); ++source) {
        if (source != 0U) {
            output.put(',');
        }
        WriteCaptureStopEvidence(
            output, evidence.stop.capture_evidence[source]);
    }
    output << "]},\"post_stop_snapshot\":";
    WriteProductionSnapshot(output, evidence.post_stop_snapshot);
    output << "}\n";
    return output.str();
}

[[nodiscard]] bool WriteEvidenceFile(
    int descriptor,
    std::string_view bytes,
    std::string* diagnostic) noexcept {
    if (descriptor < 0 || bytes.empty()) {
        if (diagnostic != nullptr) {
            try {
                *diagnostic = "evidence bytes or descriptor are invalid";
            } catch (...) {
            }
        }
        return false;
    }
    std::size_t written = 0U;
    while (written < bytes.size()) {
        const ssize_t count = ::pwrite(
            descriptor,
            bytes.data() + written,
            bytes.size() - written,
            static_cast<off_t>(written));
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            if (diagnostic != nullptr) {
                try {
                    *diagnostic = "evidence write failed (errno " +
                        std::to_string(count < 0 ? errno : 0) + ")";
                } catch (...) {
                }
            }
            return false;
        }
        written += static_cast<std::size_t>(count);
    }
    if (::fsync(descriptor) != 0) {
        if (diagnostic != nullptr) {
            try {
                *diagnostic = "evidence fsync failed (errno " +
                    std::to_string(errno) + ")";
            } catch (...) {
            }
        }
        return false;
    }
    struct stat metadata {};
    if (::fstat(descriptor, &metadata) != 0 || metadata.st_size < 0 ||
        static_cast<std::uint64_t>(metadata.st_size) != bytes.size()) {
        if (diagnostic != nullptr) {
            try {
                *diagnostic = "evidence durable size verification failed";
            } catch (...) {
            }
        }
        return false;
    }
    if (diagnostic != nullptr) {
        diagnostic->clear();
    }
    return true;
}

}  // namespace

int RunProductionRouterV1(int argc, const char* const argv[]) noexcept {
    if (argc < 0 || (argc > 0 && (argv == nullptr || argv[0] == nullptr))) {
        std::cerr << "invalid argc/argv\n";
        return 64;
    }
    try {
        std::vector<std::string_view> arguments;
        arguments.reserve(argc > 1 ? static_cast<std::size_t>(argc - 1) : 0U);
        for (int index = 1; index < argc; ++index) {
            if (argv[index] == nullptr) {
                std::cerr << "argv contains a null entry\n";
                return 64;
            }
            arguments.emplace_back(argv[index]);
        }
        const ProductionRouterArgumentResultV1 parsed =
            ParseProductionRouterArgumentsV1(arguments);
        if (!parsed.ok) {
            std::cerr << "invalid command line: " << parsed.diagnostic << '\n'
                      << ProductionRouterUsageV1();
            return 64;
        }
        if (parsed.arguments.show_help) {
            std::cout << ProductionRouterUsageV1();
            return 0;
        }
        const ProductionDeploymentLoadResultV1 loaded =
            LoadProductionDeploymentManifestV1(
                parsed.arguments.deployment_directory,
                parsed.arguments.manifest_sha256);
        if (!loaded.ok()) {
            std::cerr << "deployment manifest rejected: "
                      << ProductionDeploymentErrorNameV1(loaded.error);
            if (!loaded.diagnostic.empty()) {
                std::cerr << ": " << loaded.diagnostic;
            }
            if (loaded.line != 0U) {
                std::cerr << " (line " << loaded.line << ')';
            }
            std::cerr << '\n';
            return 78;
        }
        ProductionStaticInputsV1 static_inputs{};
        std::string static_diagnostic;
        if (!PrepareStaticInputs(
                parsed.arguments.deployment_directory,
                loaded.deployment,
                &static_inputs,
                &static_diagnostic)) {
            std::cerr << "production static preflight failed";
            if (!static_diagnostic.empty()) {
                std::cerr << ": " << static_diagnostic;
            }
            std::cerr << '\n';
            return 78;
        }
        if (parsed.arguments.check_only) {
            std::cout
                << "production deployment manifest and static inputs are "
                   "valid; no target fresh-generation artifact was present "
                   "at this check instant (normal startup revalidates at "
                   "each exclusive creator gate)\n";
            return 0;
        }

        const bool bounded_run = parsed.arguments.run_seconds != 0U;
        OwnedFd evidence_file;
        ProductionMonitorEvidenceV1 monitor_evidence{};
        if (bounded_run) {
            std::string evidence_diagnostic;
            if (!ReserveEvidenceFile(
                    static_inputs.deployment_directory.get(),
                    parsed.arguments.deployment_directory,
                    parsed.arguments.evidence_json,
                    &evidence_file,
                    &evidence_diagnostic)) {
                std::cerr << "production monitor evidence reservation failed";
                if (!evidence_diagnostic.empty()) {
                    std::cerr << ": " << evidence_diagnostic;
                }
                std::cerr << "; no production generation was created\n";
                return 78;
            }
            monitor_evidence.requested_run_seconds =
                parsed.arguments.run_seconds;
            monitor_evidence.deployment_directory =
                parsed.arguments.deployment_directory.string();
            monitor_evidence.evidence_path =
                parsed.arguments.evidence_json.string();
            monitor_evidence.manifest_sha256 = l2flow::common::Sha256Hex(
                parsed.arguments.manifest_sha256);
            monitor_evidence.sdk_library_path =
                loaded.deployment.sdk_library_path.string();
            monitor_evidence.samples.reserve(
                static_cast<std::size_t>(parsed.arguments.run_seconds) + 1U);
        }

        ScopedTerminationSignalBlock termination_signals;
        if (!termination_signals.ok()) {
            std::cerr << "production signal mask setup failed (errno "
                      << termination_signals.error_number() << ")\n";
            return 70;
        }
        int signal_wait_error = 0;
        if (termination_signals.Wait(0U, &signal_wait_error) > 0) {
            std::cout << "production startup cancelled before mutation\n";
            return 0;
        }
        if (signal_wait_error != 0) {
            std::cerr << "production signal wait failed before startup\n";
            return 70;
        }

        std::unique_ptr<ProductionServiceV1> service;
        bool fail_stop_required = false;
        std::string build_diagnostic;
        bool service_built = false;
        try {
            service_built = BuildProductionService(
                loaded.deployment,
                &static_inputs,
                &service,
                &fail_stop_required,
                &build_diagnostic);
        } catch (...) {
            std::cerr << "production composition failed unexpectedly";
            if (fail_stop_required) {
                std::cerr << " after formal fresh generation provisioning "
                             "began; explicit reprovision or "
                             "recovery/takeover is required";
            }
            std::cerr << '\n';
            return fail_stop_required ? 70 : 78;
        }
        if (!service_built) {
            if (fail_stop_required) {
                std::cerr
                    << "production composition failed after formal fresh "
                       "generation provisioning began; this generation must "
                       "not be retried in place and explicit reprovision or "
                       "recovery/takeover is required";
                if (!build_diagnostic.empty()) {
                    std::cerr << ": " << build_diagnostic;
                }
                std::cerr << '\n';
                return 70;
            }
            std::cerr << "production composition failed";
            if (!build_diagnostic.empty()) {
                std::cerr << ": " << build_diagnostic;
            }
            std::cerr << '\n';
            return 78;
        }

        // Build may hash large files and create the formal fresh generation.
        // Consume a signal which arrived during that interval before any SDK
        // Connect is allowed to run.  Ready-state Stop only tears down local
        // workers; the already-consumed namespace still requires recovery.
        signal_wait_error = 0;
        const int post_build_signal =
            termination_signals.Wait(0U, &signal_wait_error);
        if (post_build_signal == SIGINT || post_build_signal == SIGTERM ||
            post_build_signal < 0 || signal_wait_error != 0) {
            const ProductionServiceStopResultV1 stopped = service->Stop();
            if (post_build_signal == SIGINT || post_build_signal == SIGTERM) {
                std::cerr << "production startup was cancelled after formal "
                             "fresh provisioning and before SDK Connect";
            } else {
                std::cerr << "production signal wait failed after formal "
                             "fresh provisioning and before SDK Connect";
            }
            if (!stopped.ok()) {
                std::cerr << "; local teardown failed: "
                          << ProductionServiceStopErrorNameV1(stopped.error);
            }
            std::cerr << "; explicit reprovision or recovery/takeover is "
                         "required\n";
            return 70;
        }

        ProductionServiceStartResultV1 start_result{};
        std::atomic<bool> start_done{false};
        std::thread start_thread;
        try {
            start_thread = std::thread([&service, &start_result,
                                        &start_done]() noexcept {
                start_result = service->Start();
                start_done.store(true, std::memory_order_release);
            });
        } catch (...) {
            // Never call an unbounded vendor Connect synchronously from the
            // sole signal-consumer thread after thread creation has failed.
            // No capture has started, so Ready-state Stop is local teardown.
            const ProductionServiceStopResultV1 stopped = service->Stop();
            std::cerr << "production startup thread creation failed";
            if (!stopped.ok()) {
                std::cerr << "; local teardown failed: "
                          << ProductionServiceStopErrorNameV1(stopped.error);
            }
            std::cerr << "; explicit reprovision or recovery/takeover is "
                         "required\n";
            return 70;
        }

        bool termination_requested = false;
        bool fatal_observed = false;
        bool signal_wait_failed = false;
        bool stop_called = false;
        ProductionServiceStopResultV1 concurrent_stop{};
        while (!start_done.load(std::memory_order_acquire)) {
            const ProductionServiceSnapshotV1 snapshot = service->Snapshot();
            if (AggregateFatal(snapshot)) {
                fatal_observed = true;
                concurrent_stop = service->Stop();
                stop_called = true;
                break;
            }
            const int received = termination_signals.Wait(
                100U, &signal_wait_error);
            if (received == SIGINT || received == SIGTERM) {
                termination_requested = true;
            } else if (received < 0) {
                signal_wait_failed = true;
                termination_requested = true;
            }
            // Publish cancellation immediately. Stop() sets the service latch
            // before waiting for the lifecycle mutex, so Start() cannot begin
            // another SDK Connect after the current unbounded call returns.
            // An external supervisor still owns the hard deadline.
            if (termination_requested) {
                concurrent_stop = service->Stop();
                stop_called = true;
                break;
            }
        }
        if (start_thread.joinable()) {
            start_thread.join();
        }

        if (stop_called) {
            if (!concurrent_stop.ok()) {
                std::cerr << "production startup stop failed: "
                          << ProductionServiceStopErrorNameV1(
                                 concurrent_stop.error)
                          << "; explicit reprovision or recovery/takeover is "
                             "required\n";
                return 70;
            }
            if (start_result.ok() && !fatal_observed) {
                if (signal_wait_failed) {
                    std::cerr << "production signal wait failed after the "
                                 "route reached ACTIVE; route was stopped "
                                 "cleanly\n";
                    return 70;
                }
                std::cout << "production route reached ACTIVE and stopped "
                             "cleanly during startup termination\n";
                return 0;
            }
            std::cerr << "production startup did not reach an authoritative "
                         "ACTIVE result";
            if (fatal_observed) {
                std::cerr << " because aggregate Fatal was observed";
            } else {
                std::cerr << ": "
                          << ProductionServiceStartErrorNameV1(
                                 start_result.error);
            }
            std::cerr << "; the formal fresh generation must not be retried "
                         "in place and explicit reprovision or "
                         "recovery/takeover is required\n";
            return 70;
        }
        if (!start_result.ok()) {
            const ProductionServiceStopResultV1 stopped = service->Stop();
            std::cerr << "production startup failed: "
                      << ProductionServiceStartErrorNameV1(start_result.error);
            if (start_result.source_slot != UINT8_MAX) {
                std::cerr << " (source slot "
                          << static_cast<unsigned int>(
                                 start_result.source_slot)
                          << ')';
            }
            if (!stopped.ok()) {
                std::cerr << "; shutdown failed: "
                          << ProductionServiceStopErrorNameV1(stopped.error);
            }
            std::cerr << "; the formal fresh generation must not be retried "
                         "in place and explicit reprovision or "
                         "recovery/takeover is required\n";
            return 70;
        }
        if (termination_requested) {
            return StopForTermination(
                service.get(), false, signal_wait_failed);
        }

        if (bounded_run) {
            monitor_evidence.start = start_result;
        }

        // Active is already authoritative on disk at this point.  Flush the
        // operator-visible handoff before entering the long-running loop.
        std::cout << "production four-source route is ACTIVE\n" << std::flush;
        if (bounded_run) {
            constexpr std::uint64_t kNanosecondsPerSecond =
                UINT64_C(1'000'000'000);
            const std::uint64_t requested_duration_ns =
                static_cast<std::uint64_t>(
                    parsed.arguments.run_seconds) * kNanosecondsPerSecond;
            monitor_evidence.active_start_monotonic_raw_ns =
                PosixClockNow(CLOCK_MONOTONIC_RAW);
            monitor_evidence.active_start_realtime_ns =
                PosixClockNow(CLOCK_REALTIME);
            std::uint64_t next_sample_elapsed_ns = kNanosecondsPerSecond;

            ProductionServiceSnapshotV1 snapshot = service->Snapshot();
            if (monitor_evidence.active_start_monotonic_raw_ns == 0U) {
                monitor_evidence.clock_failed = true;
                monitor_evidence.outcome = "clock_failed_before_window";
            } else {
                monitor_evidence.samples.push_back(ProductionMonitorSampleV1{
                    0U,
                    monitor_evidence.active_start_realtime_ns,
                    snapshot,
                });
                if (AggregateFatal(snapshot)) {
                    monitor_evidence.fatal_observed = true;
                    monitor_evidence.outcome = "fatal_at_window_start";
                } else if (!SnapshotIsAuthoritativelyActive(snapshot)) {
                    monitor_evidence.left_active = true;
                    monitor_evidence.outcome =
                        "not_authoritatively_active_at_window_start";
                }
            }

            while (!monitor_evidence.clock_failed &&
                   !monitor_evidence.fatal_observed &&
                   !monitor_evidence.left_active &&
                   monitor_evidence.termination_signal == 0 &&
                   !monitor_evidence.signal_wait_failed &&
                   !monitor_evidence.completed_window) {
                snapshot = service->Snapshot();
                const std::uint64_t now =
                    PosixClockNow(CLOCK_MONOTONIC_RAW);
                const std::uint64_t realtime_now =
                    PosixClockNow(CLOCK_REALTIME);
                if (now == 0U ||
                    now < monitor_evidence.active_start_monotonic_raw_ns) {
                    monitor_evidence.clock_failed = true;
                    monitor_evidence.outcome =
                        "monotonic_raw_clock_failed_or_regressed";
                    break;
                }
                const std::uint64_t elapsed =
                    now - monitor_evidence.active_start_monotonic_raw_ns;
                if (AggregateFatal(snapshot)) {
                    monitor_evidence.fatal_observed = true;
                    monitor_evidence.outcome = "fatal_during_window";
                    monitor_evidence.samples.push_back(
                        ProductionMonitorSampleV1{
                            elapsed, realtime_now, snapshot});
                    monitor_evidence.monitor_end_monotonic_raw_ns = now;
                    monitor_evidence.monitor_end_realtime_ns = realtime_now;
                    break;
                }
                if (!SnapshotIsAuthoritativelyActive(snapshot)) {
                    monitor_evidence.left_active = true;
                    monitor_evidence.outcome =
                        "authoritative_active_contract_lost_during_window";
                    monitor_evidence.samples.push_back(
                        ProductionMonitorSampleV1{
                            elapsed, realtime_now, snapshot});
                    monitor_evidence.monitor_end_monotonic_raw_ns = now;
                    monitor_evidence.monitor_end_realtime_ns = realtime_now;
                    break;
                }
                if (elapsed >= requested_duration_ns) {
                    monitor_evidence.completed_window = true;
                    monitor_evidence.final_active_snapshot_valid = true;
                    monitor_evidence.final_active_snapshot = snapshot;
                    monitor_evidence.monitor_end_monotonic_raw_ns = now;
                    monitor_evidence.monitor_end_realtime_ns = realtime_now;
                    monitor_evidence.outcome = "window_completed";
                    break;
                }
                if (elapsed >= next_sample_elapsed_ns) {
                    monitor_evidence.samples.push_back(
                        ProductionMonitorSampleV1{
                            elapsed, realtime_now, snapshot});
                    const std::uint64_t completed_seconds =
                        elapsed / kNanosecondsPerSecond;
                    next_sample_elapsed_ns =
                        (completed_seconds + 1U) * kNanosecondsPerSecond;
                }

                signal_wait_error = 0;
                const int received = termination_signals.Wait(
                    100U, &signal_wait_error);
                if (received == SIGINT || received == SIGTERM) {
                    monitor_evidence.termination_signal = received;
                    monitor_evidence.outcome =
                        "termination_signal_before_window_completed";
                } else if (received < 0 || signal_wait_error != 0) {
                    monitor_evidence.signal_wait_failed = true;
                    monitor_evidence.outcome =
                        "signal_wait_failed_during_window";
                }
            }

            if (monitor_evidence.monitor_end_monotonic_raw_ns == 0U) {
                monitor_evidence.monitor_end_monotonic_raw_ns =
                    PosixClockNow(CLOCK_MONOTONIC_RAW);
                monitor_evidence.monitor_end_realtime_ns =
                    PosixClockNow(CLOCK_REALTIME);
            }
            monitor_evidence.stop = service->Stop();
            monitor_evidence.post_stop_snapshot = service->Snapshot();
            if (monitor_evidence.completed_window &&
                monitor_evidence.stop.ok()) {
                monitor_evidence.outcome =
                    "window_completed_and_route_stopped_cleanly";
            } else if (!monitor_evidence.stop.ok()) {
                monitor_evidence.outcome =
                    "monitor_ended_but_production_stop_failed";
            }

            bool evidence_written = false;
            std::string evidence_diagnostic;
            try {
                const std::string evidence_bytes =
                    EncodeMonitorEvidence(monitor_evidence);
                evidence_written = WriteEvidenceFile(
                    evidence_file.get(),
                    evidence_bytes,
                    &evidence_diagnostic);
            } catch (...) {
                evidence_diagnostic =
                    "monitor evidence encoding failed unexpectedly";
            }
            if (!evidence_written) {
                std::cerr << "production monitor evidence write failed";
                if (!evidence_diagnostic.empty()) {
                    std::cerr << ": " << evidence_diagnostic;
                }
                std::cerr << '\n';
            }
            if (monitor_evidence.completed_window &&
                monitor_evidence.stop.ok() && evidence_written) {
                std::cout << "production route completed "
                          << parsed.arguments.run_seconds
                          << " seconds of authoritative ACTIVE monitoring, "
                             "stopped cleanly, and persisted evidence\n";
                return 0;
            }
            if (monitor_evidence.fatal_observed ||
                monitor_evidence.left_active ||
                !monitor_evidence.stop.ok()) {
                std::cerr << "production bounded monitor failed: "
                          << monitor_evidence.outcome << '\n';
                return 1;
            }
            std::cerr << "production bounded monitor did not complete: "
                      << monitor_evidence.outcome << '\n';
            return 70;
        }
        for (;;) {
            const ProductionServiceSnapshotV1 snapshot = service->Snapshot();
            if (AggregateFatal(snapshot)) {
                return StopForTermination(service.get(), true, false);
            }
            if (snapshot.state != ProductionServiceStateV1::kActive) {
                const ProductionServiceStopResultV1 stopped = service->Stop();
                std::cerr << "production service left ACTIVE unexpectedly";
                if (!stopped.ok()) {
                    std::cerr << "; shutdown failed: "
                              << ProductionServiceStopErrorNameV1(
                                     stopped.error);
                }
                std::cerr << '\n';
                return 1;
            }
            const int received = termination_signals.Wait(
                100U, &signal_wait_error);
            if (received == SIGINT || received == SIGTERM) {
                return StopForTermination(service.get(), false, false);
            }
            if (received < 0) {
                return StopForTermination(service.get(), false, true);
            }
        }
    } catch (...) {
        std::cerr << "production router failed unexpectedly\n";
        return 70;
    }
}

}  // namespace l2flow::apps
