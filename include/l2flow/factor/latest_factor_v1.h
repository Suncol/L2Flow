#pragma once

#include "l2flow/canonical/canonical_schema_v1.h"
#include "l2flow/common/sha256.h"
#include "l2flow/factor/latest_factor_c_api_v1.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace l2flow::factor {

inline constexpr std::size_t kLatestFactorSlotBytesV1 = 4096U;
inline constexpr std::size_t kLatestFactorSlotAlignmentV1 = 64U;
inline constexpr std::uint32_t kLatestFactorSlotMagicV1 =
    0x3143464cU;  // "LFC1" as little-endian bytes.
inline constexpr std::uint16_t kLatestFactorSlotVersionV1 = 1U;

// No process may reinterpret the payload as ordinary mutable fields. The
// supported GCC/Clang platform ABI accesses every 64-bit word through
// __atomic builtins after a lock-free host preflight; the seqlock supplies
// snapshot consistency. Portable C++ object-model or TSan claims are not
// inferred from this platform-specific shared-memory contract.
struct alignas(kLatestFactorSlotAlignmentV1) LatestFactorSlotV1 final {
    std::array<std::byte, kLatestFactorSlotBytesV1> bytes{};
};
static_assert(sizeof(LatestFactorSlotV1) == 4096U);
static_assert(alignof(LatestFactorSlotV1) == 64U);

struct LatestFactorValueV1 final {
    l2flow::common::Sha256Digest factor_id_sha256{};
    l2flow::common::Sha256Digest factor_version_sha256{};
    std::uint32_t instrument_id = 0U;
    std::int64_t asof_ns = 0;
    double value = 0.0;
    bool valid = false;
    std::uint64_t input_quality_flags = 0U;
    std::uint64_t watermark_set_id = 0U;
    l2flow::common::Sha256Digest input_identity_sha256{};
    std::uint64_t calculation_latency_ns = 0U;
    l2flow::canonical::ClockEpochIdentityV1 clock_epoch{};
};

enum class LatestFactorErrorV1 : std::uint8_t {
    kNone = L2FLOW_LATEST_FACTOR_NONE_V1,
    kNullArgument = L2FLOW_LATEST_FACTOR_NULL_ARGUMENT_V1,
    kInvalidStorage = L2FLOW_LATEST_FACTOR_INVALID_STORAGE_V1,
    kUnsupportedHost = L2FLOW_LATEST_FACTOR_UNSUPPORTED_HOST_V1,
    kAtomicsNotLockFree = L2FLOW_LATEST_FACTOR_ATOMICS_NOT_LOCK_FREE_V1,
    kInvalidValue = L2FLOW_LATEST_FACTOR_INVALID_VALUE_V1,
    kEmpty = L2FLOW_LATEST_FACTOR_EMPTY_V1,
    kBusy = L2FLOW_LATEST_FACTOR_BUSY_V1,
    kSequenceExhausted = L2FLOW_LATEST_FACTOR_SEQUENCE_EXHAUSTED_V1,
    kSlotIdentityMismatch =
        L2FLOW_LATEST_FACTOR_SLOT_IDENTITY_MISMATCH_V1,
    kOldOutput = L2FLOW_LATEST_FACTOR_OLD_OUTPUT_V1,
    kOutputConflict = L2FLOW_LATEST_FACTOR_OUTPUT_CONFLICT_V1,
    kCorruptSlot = L2FLOW_LATEST_FACTOR_CORRUPT_SLOT_V1,
    kAlreadyInitialized = L2FLOW_LATEST_FACTOR_ALREADY_INITIALIZED_V1,
};

enum class LatestFactorPublishDispositionV1 : std::uint8_t {
    kPublished = L2FLOW_LATEST_FACTOR_PUBLISHED_V1,
    kIdempotent = L2FLOW_LATEST_FACTOR_IDEMPOTENT_V1,
};

struct LatestFactorPublishResultV1 final {
    LatestFactorErrorV1 error = LatestFactorErrorV1::kNone;
    LatestFactorPublishDispositionV1 disposition =
        LatestFactorPublishDispositionV1::kPublished;

    [[nodiscard]] bool ok() const noexcept {
        return error == LatestFactorErrorV1::kNone;
    }
};

[[nodiscard]] std::string_view LatestFactorErrorNameV1(
    LatestFactorErrorV1 error) noexcept;

[[nodiscard]] bool LatestFactorAtomicsLockFreeV1() noexcept;

[[nodiscard]] std::string_view LatestFactorSchemaDescriptorV1() noexcept;
[[nodiscard]] l2flow::common::Sha256Digest
LatestFactorSchemaDescriptorSha256V1() noexcept;

[[nodiscard]] LatestFactorErrorV1 InitializeLatestFactorSlotV1(
    LatestFactorSlotV1* slot) noexcept;

[[nodiscard]] LatestFactorPublishResultV1 PublishLatestFactorV1(
    LatestFactorSlotV1* slot,
    const LatestFactorValueV1& value) noexcept;

[[nodiscard]] LatestFactorErrorV1 ReadLatestFactorV1(
    const LatestFactorSlotV1& slot,
    LatestFactorValueV1* value,
    std::uint64_t* stable_sequence = nullptr) noexcept;

}  // namespace l2flow::factor
