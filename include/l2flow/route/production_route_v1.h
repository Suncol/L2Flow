#pragma once

#include "l2flow/canonical/canonical_schema_v1.h"
#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace l2flow::route {

inline constexpr std::uint16_t kProductionRouteWireVersionV1 = 1U;
inline constexpr std::size_t kProductionRouteSourceCountV1 = 4U;
inline constexpr std::size_t kProductionRouteEndpointCountV1 = 4U;
inline constexpr std::size_t kProductionRouteMaximumEndpointBytesV1 = 4096U;
inline constexpr std::size_t kProductionRouteMaximumEncodedBytesV1 =
    64U * 1024U;
inline constexpr std::array<std::uint32_t,
                            kProductionRouteSourceCountV1>
    kProductionRouteSourceStreamIdsV1{{1001U, 1002U, 2001U, 2002U}};

// kInvalid is the fail-closed default and is never encodable or publishable.
// A kFatal manifest is durable revocation evidence: it is decodable for audit,
// but an authoritative reader must never expose its endpoints as active.
enum class ProductionRouteStateV1 : std::uint8_t {
    kInvalid = 0U,
    kActive = 1U,
    kFatal = 2U,
};

struct ProductionRouteSourceV1 final {
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    l2flow::common::Identity128 stream_day_id{};
    l2flow::common::Identity128 writer_instance{};
    std::uint64_t source_generation = 0U;
    // Exact Canonical namespace generation consumed through this route.
    // It is independent of both the Raw control generation and the
    // SourceFrontier generation, so it is carried explicitly.
    std::uint64_t canonical_generation = 0U;
    // Per-source lower-bound anchor at Active publication: Raw is durable and
    // Canonical is processed through this ingress sequence/WAL position.
    // Zero/zero is the explicit pre-record sentinel; otherwise both fields
    // must be nonzero.
    std::uint64_t durable_ingress_sequence = 0U;
    std::uint64_t durable_global_wal_pos = 0U;
    l2flow::common::Sha256Digest clock_epoch_identity_sha256{};

    [[nodiscard]] friend constexpr bool operator==(
        const ProductionRouteSourceV1&,
        const ProductionRouteSourceV1&) noexcept = default;
};

struct ProductionRouteEndpointsV1 final {
    // V1 binds local, normalized, absolute paths.  A future URI/remote route
    // requires a new wire version rather than weakening this validation.
    // canonical is required.  Empty history/state/factor explicitly mean
    // that this generation does not publish that cross-process capability;
    // in-process history is injected directly by ProductionServiceV1 and is
    // never represented by a fictitious filesystem endpoint.
    std::string canonical;
    std::string history;
    std::string state;
    std::string factor;

    [[nodiscard]] friend bool operator==(
        const ProductionRouteEndpointsV1&,
        const ProductionRouteEndpointsV1&) noexcept = default;
};

struct ProductionRouteManifestV1 final {
    std::uint16_t wire_version = kProductionRouteWireVersionV1;
    ProductionRouteStateV1 state = ProductionRouteStateV1::kInvalid;
    // Active routes cannot use UINT64_MAX: one generation is reserved for a
    // fail-closed fatal successor even if no further active route is possible.
    std::uint64_t generation = 0U;
    // Zero only for the first published route.  A store additionally verifies
    // that this equals the generation it is replacing.
    std::uint64_t previous_generation = 0U;
    l2flow::common::Identity128 route_instance{};
    std::uint32_t trade_date = 0U;
    // Active routes require zero; fatal routes require a nonzero stable code.
    std::uint32_t fatal_reason_code = 0U;
    std::uint64_t registry_version = 0U;
    l2flow::common::Sha256Digest registry_sha256{};
    l2flow::common::Sha256Digest schema_sha256{};
    l2flow::common::Sha256Digest build_sha256{};
    l2flow::common::Sha256Digest config_sha256{};
    std::array<ProductionRouteSourceV1,
               kProductionRouteSourceCountV1>
        sources{};
    ProductionRouteEndpointsV1 endpoints{};

    // This is structural Active state, not a process-liveness proof.  A
    // cross-process consumer must use ReadLiveProductionRouteV1At and retain
    // its guard; a static manifest can outlive its owner after SIGKILL.
    [[nodiscard]] bool authoritative() const noexcept {
        return state == ProductionRouteStateV1::kActive;
    }

    [[nodiscard]] friend bool operator==(
        const ProductionRouteManifestV1&,
        const ProductionRouteManifestV1&) noexcept = default;
};

enum class ProductionRouteManifestErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kUnsupportedVersion,
    kInvalidState,
    kInvalidGeneration,
    kInvalidIdentity,
    kInvalidDate,
    kInvalidFatalReason,
    kInvalidRegistry,
    kInvalidDigest,
    kInvalidSourceSet,
    kInvalidSource,
    kInvalidEndpoint,
    kDuplicateEndpoint,
    kEncodedSizeExceeded,
    kTruncated,
    kTrailingBytes,
    kInvalidMagic,
    kInvalidReservedField,
    kLengthMismatch,
    kDigestMismatch,
    kAllocationFailure,
};

[[nodiscard]] std::string_view ProductionRouteManifestErrorNameV1(
    ProductionRouteManifestErrorV1 error) noexcept;

// Domain-separated V1 identity hash.  It covers the canonical little-endian
// algorithm value and every byte of the full digest.  The nonidentity label
// is deliberately ignored, matching ClockEpochIdentityV1 equality.
[[nodiscard]] l2flow::common::Sha256Digest
ComputeProductionRouteClockEpochIdentitySha256V1(
    const l2flow::canonical::ClockEpochIdentityV1& identity) noexcept;

[[nodiscard]] ProductionRouteManifestErrorV1
ValidateProductionRouteManifestV1(
    const ProductionRouteManifestV1& manifest) noexcept;

// Deterministic little-endian V1 codec.  The final 32 bytes are a
// domain-separated SHA-256 over every preceding byte.  Encode changes output
// only on success; Decode changes output only after digest and semantic
// validation both succeed.
[[nodiscard]] ProductionRouteManifestErrorV1
EncodeProductionRouteManifestV1(
    const ProductionRouteManifestV1& manifest,
    std::vector<std::byte>* output) noexcept;

[[nodiscard]] ProductionRouteManifestErrorV1
DecodeProductionRouteManifestV1(
    std::span<const std::byte> encoded,
    ProductionRouteManifestV1* output) noexcept;

}  // namespace l2flow::route
