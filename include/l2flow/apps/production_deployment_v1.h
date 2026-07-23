#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/sdk/subscription_manifest.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <span>
#include <string>
#include <string_view>

namespace l2flow::apps {

inline constexpr std::string_view kProductionDeploymentMagicV1 =
    "L2FLOW_PRODUCTION_DEPLOYMENT_V1";
inline constexpr std::string_view kProductionDeploymentFileNameV1 =
    "production-v1.tsv";
// Raw receive/segment timestamps use CLOCK_MONOTONIC_RAW together with
// CLOCK_REALTIME.  Writer heartbeat liveness deliberately uses host
// CLOCK_MONOTONIC and is not part of this persisted clock epoch identity.
inline constexpr std::string_view kProductionClockSourceConfigV1 =
    "CLOCK_REALTIME+CLOCK_MONOTONIC_RAW:V1";
inline constexpr std::size_t kProductionDeploymentMaximumBytesV1 =
    256U * 1024U;
inline constexpr std::size_t kProductionDeploymentSourceCountV1 = 4U;

struct ProductionDeploymentSourceV1 final {
    l2flow::sdk::IngressKind kind =
        l2flow::sdk::IngressKind::ShSnapshot;
    std::string stream_slug;
    std::string endpoint_path;
    l2flow::common::Sha256Digest endpoint_sha256{};
    std::string sdk_log_prefix;
    std::string metrics_path;
    l2flow::common::Identity128 stream_day_id{};
    l2flow::common::Identity128 recovery_attempt_id{};
    l2flow::common::Identity128 writer_instance{};
    std::uint64_t source_generation = 0U;
    std::uint64_t canonical_generation = 0U;
    std::uint64_t connect_generation = 0U;
    std::uint64_t scaffolding_allocation_cap = 0U;
    std::uint64_t safe_stop_template_id = 0U;
};

// Every field is read from one exact hash-pinned manifest except values whose
// provenance is stronger when derived by the executable: build/schema hashes,
// host/boot identity and the manifest digest itself.  The SDK library path is
// an operator authorization: V1 checks only that it names an existing regular
// file, then uses that exact path.  It deliberately does not apply a snapshot,
// archive, baseline, size, digest, ELF/ABI or runtime-lifecycle approval gate.
// There are no implicit recovery or existing-ACTIVE modes in V1.
struct ProductionDeploymentV1 final {
    l2flow::common::Sha256Digest manifest_sha256{};

    std::filesystem::path sdk_library_path;
    std::string credential_path;
    std::string credential_name;

    std::string raw_root;
    std::string frontier_root;
    std::string canonical_root;
    std::string route_root;
    std::string instrument_registry_file;
    std::uint64_t instrument_registry_version = 0U;
    l2flow::common::Sha256Digest instrument_registry_sha256{};

    std::uint32_t capture_date = 0U;
    std::uint32_t trade_date = 0U;
    std::uint64_t route_generation = 0U;
    std::uint64_t route_previous_generation = 0U;
    l2flow::common::Identity128 route_instance{};

    l2flow::common::Identity128 coordinator_identity{};
    std::uint64_t coordinator_device_id = 0U;
    l2flow::common::Sha256Digest coordinator_quota_sha256{};
    l2flow::common::Sha256Digest coordinator_mount_sha256{};
    std::string clock_source_config;

    std::uint32_t raw_heartbeat_interval_seconds = 0U;
    std::uint32_t raw_heartbeat_timeout_seconds = 0U;
    bool raw_include_optional_index = false;
    std::uint32_t raw_max_message_bytes = 0U;
    std::uint64_t raw_ring_capacity_bytes = 0U;
    std::uint32_t raw_ring_stall_budget_seconds = 0U;
    std::uint64_t raw_segment_target_bytes = 0U;
    std::uint32_t raw_segment_max_age_seconds = 0U;
    std::uint32_t raw_sync_interval_milliseconds = 0U;
    std::uint64_t raw_sync_bytes = 0U;
    std::uint64_t raw_sparse_index_every_records = 0U;
    std::uint64_t raw_sparse_index_every_bytes = 0U;
    std::string reserve_domain_id;
    std::string reserve_coordinator_socket;
    std::uint32_t reserve_ack_timeout_milliseconds = 0U;
    std::uint64_t emergency_reserve_bytes = 0U;
    std::size_t raw_maximum_manifest_bytes = 0U;
    std::uint32_t raw_live_max_segments = 0U;
    std::uint64_t raw_live_max_segment_bytes = 0U;
    std::size_t raw_live_max_journal_markers = 0U;
    std::uint32_t raw_live_max_control_reattach_attempts = 0U;

    // Canonical families have materially different record sizes and
    // cardinalities.  In particular, quality/control are unsharded while
    // snapshot/tick have one sink per logical shard, so a single shared
    // capacity either exhausts the unsharded quality sink or grossly
    // over-allocates every snapshot segment.
    std::uint64_t canonical_snapshot_capacity_records_per_sink = 0U;
    std::uint64_t canonical_tick_capacity_records_per_sink = 0U;
    std::uint64_t canonical_quality_capacity_records_per_sink = 0U;
    std::uint64_t canonical_control_capacity_records_per_sink = 0U;
    std::uint32_t history_physical_workers = 0U;
    std::size_t history_queue_capacity = 0U;
    std::size_t history_maximum_inflight_per_source = 0U;
    std::size_t history_chunk_record_capacity = 0U;
    std::size_t history_maximum_records_per_query = 0U;
    std::uint64_t history_maximum_records_per_shard = 0U;
    std::uint32_t history_maximum_instruments_per_shard = 0U;
    std::uint64_t history_maximum_payload_bytes_per_shard = 0U;

    std::uint64_t activation_timeout_ms = 0U;
    std::uint64_t drain_timeout_ms = 0U;
    std::uint64_t writer_idle_heartbeat_interval_ms = 0U;
    std::uint64_t writer_heartbeat_timeout_ms = 0U;

    std::array<ProductionDeploymentSourceV1,
               kProductionDeploymentSourceCountV1>
        sources{};
};

enum class ProductionDeploymentErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kInvalidDirectory,
    kUnsafeDirectory,
    kOpenFailed,
    kUnsafeFile,
    kReadFailed,
    kChangedDuringRead,
    kDigestMismatch,
    kInvalidText,
    kUnknownField,
    kDuplicateField,
    kMissingField,
    kInvalidValue,
    kInvalidTopology,
    kResourceExhausted,
};

[[nodiscard]] std::string_view ProductionDeploymentErrorNameV1(
    ProductionDeploymentErrorV1 error) noexcept;

struct ProductionDeploymentLoadResultV1 final {
    ProductionDeploymentErrorV1 error =
        ProductionDeploymentErrorV1::kNone;
    int system_error_number = 0;
    std::size_t line = 0U;
    std::string diagnostic;
    ProductionDeploymentV1 deployment{};

    [[nodiscard]] bool ok() const noexcept {
        return error == ProductionDeploymentErrorV1::kNone;
    }
};

// Pure exact-byte parser used by tests and by the secure file loader.  The
// expected digest is mandatory and is checked before field parsing.
[[nodiscard]] ProductionDeploymentLoadResultV1
ParseProductionDeploymentManifestV1(
    std::string_view exact_bytes,
    const l2flow::common::Sha256Digest& expected_sha256) noexcept;

// Opens one fixed manifest name below an absolute, effective-UID-owned 0700
// directory.  The file must be an effective-UID-owned, singly linked 0600
// regular file and is read through one O_NOFOLLOW descriptor with stable
// metadata before/after the bounded read.
[[nodiscard]] ProductionDeploymentLoadResultV1
LoadProductionDeploymentManifestV1(
    const std::filesystem::path& deployment_directory,
    const l2flow::common::Sha256Digest& expected_sha256) noexcept;

struct ProductionRouterArgumentsV1 final {
    std::filesystem::path deployment_directory;
    l2flow::common::Sha256Digest manifest_sha256{};
    // Zero preserves the normal supervisor-owned, unbounded service mode.
    // A nonzero value starts only after Start() returns authoritative Active
    // and requires evidence_json in the deployment directory.
    std::uint32_t run_seconds = 0U;
    std::filesystem::path evidence_json;
    bool check_only = false;
    bool fast_plane_shadow = false;
    bool show_help = false;
};

struct ProductionRouterArgumentResultV1 final {
    ProductionRouterArgumentsV1 arguments{};
    bool ok = false;
    std::string diagnostic;
};

[[nodiscard]] ProductionRouterArgumentResultV1
ParseProductionRouterArgumentsV1(
    std::span<const std::string_view> arguments) noexcept;

[[nodiscard]] std::string ProductionRouterUsageV1();

// The process entry point.  --check performs the exact manifest/security and
// non-mutating deployment preflight, but deliberately does not load vendor
// code, acquire SourceFrontier roles, mutate SCAFFOLDING, create Canonical
// files, or publish a route.  Normal mode is fresh-only and fail-closed.
int RunProductionRouterV1(int argc, const char* const argv[]) noexcept;

}  // namespace l2flow::apps
