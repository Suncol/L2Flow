#pragma once

#include "l2flow/sdk/subscription_manifest.h"

#include "mdl_api_types.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

namespace l2flow::sdk {

inline constexpr std::uint32_t kDefaultMaxMessageBytes = 16U * 1024U * 1024U;
inline constexpr std::uint64_t kDefaultMinimumRingBytes = 512ULL * 1024ULL * 1024ULL;

struct EndpointContract {
    std::string name;
    std::string resolved_server_address;
    std::string contract_sha256;
    datayes::mdl::MDLMessageEncoding message_encoding =
        datayes::mdl::MDLEID_UNDEFINED;
    bool merge_message = false;
    // Optional forces callers to state that this value came from a reviewed
    // endpoint contract instead of relying on a market-based default.
    std::optional<bool> send_mac_auth;
    bool server_select = false;
};

struct IngressConfig {
    IngressKind kind = IngressKind::ShSnapshot;
    EndpointContract endpoint;

    std::string credential_name;
    // Runtime-only secret. It is validated but deliberately omitted from the
    // canonical representation and every report.
    std::string token;
    std::string sdk_log_prefix;
    std::string shadow_capture_path;
    std::string metrics_textfile_path;

    int work_threads = 0;
    int io_threads = 0;
    std::uint32_t heartbeat_interval_seconds = 10U;
    std::uint32_t heartbeat_timeout_seconds = 30U;
    bool include_optional_index = false;

    std::uint32_t max_message_bytes = kDefaultMaxMessageBytes;
    std::uint64_t ring_capacity_bytes = kDefaultMinimumRingBytes;
    std::uint32_t capture_date = 0U;
    std::uint64_t first_ingress_sequence = 1U;
};

// Constructs a config with per-stream thread defaults but no endpoint,
// credential, token, date, or output path.
IngressConfig DefaultIngressConfig(IngressKind kind);

// Returns an empty string when the configuration is safe to use.
std::string ValidateIngressConfig(const IngressConfig& config);

// Stable, sorted JSON used as the input to config_sha256. Secret token bytes
// are never included.
std::string CanonicalIngressConfig(const IngressConfig& config);
std::string IngressConfigSha256(const IngressConfig& config);

// ceil(peak_bytes_per_second * stall_seconds * 3 / 2), bounded below by
// minimum_bytes. Returns nullopt on overflow or zero inputs.
std::optional<std::uint64_t> RecommendedRingCapacity(
    std::uint64_t peak_bytes_per_second,
    std::uint64_t stall_seconds,
    std::uint64_t minimum_bytes = kDefaultMinimumRingBytes) noexcept;

}  // namespace l2flow::sdk
