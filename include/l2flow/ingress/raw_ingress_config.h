#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/sdk/subscription_manifest.h"

#include <cstdint>
#include <string>

namespace l2flow::ingress {

// These are operator starting points from docs/design.md, not limits inferred
// from the vendor protocol or storage device.
inline constexpr std::uint64_t kDefaultRawSegmentTargetBytes =
    4ULL * 1024ULL * 1024ULL * 1024ULL;
inline constexpr std::uint32_t kDefaultRawSegmentMaxAgeSeconds = 5U * 60U;
inline constexpr std::uint32_t kDefaultRawSyncIntervalMilliseconds = 10U;
inline constexpr std::uint64_t kDefaultRawSyncBytes =
    4ULL * 1024ULL * 1024ULL;
inline constexpr std::uint64_t kDefaultRawSparseIndexEveryRecords = 4096U;
inline constexpr std::uint64_t kDefaultRawSparseIndexEveryBytes =
    4ULL * 1024ULL * 1024ULL;
inline constexpr std::uint32_t kDefaultRawRingStallBudgetSeconds = 5U;
inline constexpr std::uint32_t kDefaultRawReserveAckTimeoutMilliseconds =
    5000U;
inline constexpr std::uint64_t kDefaultRawEmergencyReserveBytes =
    50ULL * 1024ULL * 1024ULL * 1024ULL;

// Stable Phase-2 effective configuration. Endpoint address, encoding, merge,
// MAC-auth and server-select values are deliberately not copied here: their
// sole identity is the hash-pinned Phase-1 endpoint contract.
//
// This type also deliberately has no capture date, first ingress sequence,
// stream-day/writer identity, segment/cursor state, or Phase-1 shadow path.
// Those values are either recovered runtime state or are obsolete in the
// Phase-2 production path.
struct RawIngressConfig final {
    l2flow::sdk::IngressKind kind =
        l2flow::sdk::IngressKind::ShSnapshot;

    std::string endpoint_contract_sha256;
    std::string credential_name;
    std::string sdk_log_prefix;
    std::string metrics_textfile_path;

    int work_threads = 0;
    int io_threads = 0;
    std::uint32_t heartbeat_interval_seconds = 10U;
    std::uint32_t heartbeat_timeout_seconds = 30U;
    bool include_optional_index = false;

    std::uint32_t max_message_bytes = 16U * 1024U * 1024U;
    std::uint64_t ring_capacity_bytes =
        512ULL * 1024ULL * 1024ULL;
    std::uint32_t ring_stall_budget_seconds =
        kDefaultRawRingStallBudgetSeconds;

    std::string raw_root;
    std::uint16_t raw_format_version = 1U;
    std::string raw_schema_sha256;

    std::uint64_t segment_target_bytes =
        kDefaultRawSegmentTargetBytes;
    std::uint32_t segment_max_age_seconds =
        kDefaultRawSegmentMaxAgeSeconds;
    std::uint32_t sync_interval_milliseconds =
        kDefaultRawSyncIntervalMilliseconds;
    std::uint64_t sync_bytes = kDefaultRawSyncBytes;
    std::uint64_t sparse_index_every_records =
        kDefaultRawSparseIndexEveryRecords;
    std::uint64_t sparse_index_every_bytes =
        kDefaultRawSparseIndexEveryBytes;

    std::string reserve_domain_id;
    std::string reserve_coordinator_socket;
    std::uint32_t reserve_ack_timeout_milliseconds =
        kDefaultRawReserveAckTimeoutMilliseconds;
    std::uint64_t emergency_reserve_bytes =
        kDefaultRawEmergencyReserveBytes;

    // Only the stable algorithm/configuration inputs belong to this hash
    // domain. Per-boot host/boot inputs and the resulting epoch are runtime
    // state.
    std::uint32_t clock_epoch_algorithm_version = 1U;
    std::string canonical_clock_source_config;
};

// Launch inputs that are intentionally outside config_sha256. The configured
// capture date selects the logical route to discover/recover; it is not
// allowed to override any recovered identity or cursor. In particular, no
// API below accepts credential_token while constructing canonical bytes.
struct RawIngressProcessInputs final {
    std::string endpoint_contract_path;
    std::string credential_token;
    std::uint32_t configured_capture_date = 0U;
};

struct RawIngressRuntimeCursor final {
    std::uint32_t segment_sequence = 0U;
    std::uint64_t global_wal_pos = 0U;
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t segment_offset = 0U;
};

// Recovered/startup state has a typed home separate from RawIngressConfig.
// It is written to runtime artifacts by their own codecs; this structure is
// not a durable wire schema and is never an input to config_sha256.
struct RawIngressRuntimeState final {
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    l2flow::common::Identity128 stream_day_id{};
    std::uint64_t recovered_next_ingress_sequence = 0U;
    RawIngressRuntimeCursor append{};
    RawIngressRuntimeCursor durable{};
    l2flow::common::Identity128 writer_instance{};
    std::uint32_t current_segment_sequence = 0U;
    std::uint32_t clock_epoch_algorithm_version = 0U;
    l2flow::common::Sha256Digest clock_epoch_digest{};
    std::uint64_t clock_epoch_label = 0U;
};

// Populates documented Phase-2 starting values and per-stream SDK thread
// defaults. Required deployment identities and paths remain empty so a default
// object cannot accidentally pass validation.
[[nodiscard]] RawIngressConfig DefaultRawIngressConfig(
    l2flow::sdk::IngressKind kind);

// An empty result means valid. Validation is deliberately independent of
// filesystem existence: secure open/name-to-inode checks belong to startup.
[[nodiscard]] std::string ValidateRawIngressConfig(
    const RawIngressConfig& config);
[[nodiscard]] std::string ValidateRawIngressProcessInputs(
    const RawIngressProcessInputs& inputs);
[[nodiscard]] std::string ValidateRawIngressRuntimeState(
    const RawIngressConfig& config,
    const RawIngressRuntimeState& state);
// Service-level RESUME_CONNECT preflight. A recovered/fresh runtime may
// connect only for the explicitly configured capture date; old-date
// RECOVER_SEAL_ONLY maintenance must not pass through this gate.
[[nodiscard]] std::string
ValidateRawIngressResumeConnectInputs(
    const RawIngressConfig& config,
    const RawIngressProcessInputs& inputs,
    const RawIngressRuntimeState& state);

// Deterministic, versioned, compact UTF-8 JSON used only as the stable
// config_sha256 input. Both functions throw std::invalid_argument when config
// validation fails. These bytes are not a RunManifest or durable wire schema.
[[nodiscard]] std::string CanonicalRawIngressConfig(
    const RawIngressConfig& config);
[[nodiscard]] std::string RawIngressConfigSha256(
    const RawIngressConfig& config);

}  // namespace l2flow::ingress
