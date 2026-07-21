#pragma once

#include "l2flow/ingress/raw_v1.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace l2flow::ingress {

inline constexpr std::uint32_t kRunManifestV1SchemaVersion = 1U;
// SHA-256 over the exact UTF-8 bytes of schemas/run_manifest_v1.json,
// including its single trailing LF.
inline constexpr std::size_t kRunManifestV1SchemaBytes = 12613U;
inline constexpr std::string_view
    kRunManifestV1SchemaSha256Hex =
        "42690fc3a767d1af7805c6c2cd21c9c"
        "e57dbe245fd45a13462260294abe7914f";
inline constexpr RawV1Digest kRunManifestV1SchemaSha256{
    std::byte{0x42}, std::byte{0x69}, std::byte{0x0f},
    std::byte{0xc3}, std::byte{0xa7}, std::byte{0x67},
    std::byte{0xd1}, std::byte{0xaf}, std::byte{0x78},
    std::byte{0x05}, std::byte{0xc6}, std::byte{0xc2},
    std::byte{0xcd}, std::byte{0x21}, std::byte{0xc9},
    std::byte{0xce}, std::byte{0x57}, std::byte{0xdb},
    std::byte{0xe2}, std::byte{0x45}, std::byte{0xfd},
    std::byte{0x45}, std::byte{0xa1}, std::byte{0x34},
    std::byte{0x62}, std::byte{0x26}, std::byte{0x02},
    std::byte{0x94}, std::byte{0xab}, std::byte{0xe7},
    std::byte{0x91}, std::byte{0x4f}};
inline constexpr std::size_t kRunManifestV1MaximumBytes =
    4U * 1024U * 1024U;
inline constexpr std::size_t kRunManifestV1MaximumRawInputs = 16U;
inline constexpr std::size_t
    kRunManifestV1MaximumSegmentsPerInput = 100'000U;
inline constexpr std::size_t
    kRunManifestV1MaximumClockTransitionsPerInput = 100'000U;
inline constexpr std::size_t kRunManifestV1MaximumFaultRules = 1024U;
// One emergency finalization cycle may create at most one continuation
// segment. Multiple cycles are represented by distinct manifests rather than
// weakening the singular cycle/grant identity below.
inline constexpr std::size_t
    kRunManifestV1MaximumContinuationSegments = 1U;

enum class RunManifestModeV1 : std::uint8_t {
    kLive = 0U,
    kReplay,
};

enum class RunManifestSourceRevisionStatusV1 : std::uint8_t {
    kAvailable = 0U,
    kUnavailable,
};

enum class RunManifestDurabilityPolicyV1 : std::uint8_t {
    kDurableOnly = 0U,
    kIncludesRecoveredAppendOnly,
};

struct RunManifestVendorV1 final {
    std::uint32_t sdk_version = 0U;
    RawV1Digest sdk_archive_sha256{};
    RawV1Digest libmdl_api_sha256{};
    std::string elf_build_id;
};

struct RunManifestBuildV1 final {
    RawV1Digest build_manifest_sha256{};
    RunManifestSourceRevisionStatusV1
        source_revision_status =
            RunManifestSourceRevisionStatusV1::
                kUnavailable;
    std::optional<std::string> source_revision;
    std::string compiler;
    std::string cxx_flags;
    std::optional<std::string> python_version;
    RawV1Digest dependency_lock_sha256{};
};

struct RunManifestConfigurationV1 final {
    RawV1Digest config_sha256{};
    RawV1Digest endpoint_contract_sha256{};
    std::string registry_version;
    RawV1Digest registry_sha256{};
    RawV1Digest raw_schema_sha256{};
    std::optional<RawV1Digest> canonical_schema_sha256;
    std::optional<RawV1Digest> dtype_sha256;
    std::uint32_t shard_count = 0U;
};

struct RunManifestClockTransitionV1 final {
    std::uint64_t record_start_wal_pos = 0U;
    std::uint32_t algorithm = 0U;
    RawV1Digest digest{};
};

struct RunManifestReserveFinalizationV1 final {
    RawV1Identity reserve_state_uuid{};
    RawV1Identity finalization_cycle_id{};
    RawV1Digest immutable_grant_sha256{};
    std::optional<std::string> maintenance_report_locator;
    std::optional<RawV1Digest> maintenance_report_sha256;
    std::optional<std::string> finalization_archive_locator;
    std::optional<RawV1Digest>
        finalization_archive_manifest_sha256;
    std::vector<std::uint32_t>
        continuation_segment_sequences;
};

struct RunManifestRawRangeV1 final {
    std::uint64_t first_record_start_wal_pos = 0U;
    std::uint64_t last_record_end_wal_pos = 0U;
    std::uint64_t first_ingress_sequence = 0U;
    std::uint64_t last_ingress_sequence = 0U;
};

struct RunManifestRawInputV1 final {
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    RawV1Identity stream_day_id{};
    RawV1Digest durable_journal_header_sha256{};
    RunManifestDurabilityPolicyV1 durability_policy =
        RunManifestDurabilityPolicyV1::kDurableOnly;
    std::optional<RunManifestRawRangeV1> range;
    DurableMarkerV1 durable_marker{};
    RawV1DurableMarkerWire durable_marker_bytes{};
    std::vector<RawV1Digest> segment_sha256;
    std::vector<RunManifestClockTransitionV1>
        clock_epoch_transitions;
    std::optional<std::string> append_only_reason;

    bool synthetic = false;
    std::optional<std::string> synthetic_schema;
    std::optional<RawV1Identity> parent_run_id;
    std::optional<RawV1Digest> parent_raw_identity_sha256;
    std::optional<RawV1Digest> fault_rule_sha256;
    std::optional<std::uint64_t> fault_seed;

    RunManifestReserveFinalizationV1
        reserve_finalization;
};

struct RunManifestFactorV1 final {
    std::string factor_group;
    RawV1Digest factor_code_sha256{};
    RawV1Digest factor_config_sha256{};
    RawV1Digest state_schema_sha256{};
};

struct RunManifestFaultRuleV1 final {
    RawV1Digest rule_sha256{};
    std::string canonical_rule;
};

struct RunManifestFaultInjectionV1 final {
    bool enabled = false;
    std::optional<std::uint64_t> seed;
    std::vector<RunManifestFaultRuleV1> rules;
};

struct RunManifestV1 final {
    std::uint32_t schema_version =
        kRunManifestV1SchemaVersion;
    RawV1Identity run_id{};
    RunManifestModeV1 mode =
        RunManifestModeV1::kLive;
    std::uint32_t capture_date = 0U;
    std::optional<std::uint32_t> trade_date;
    RawV1Identity host_uuid{};
    RawV1Identity linux_boot_id{};
    std::uint64_t clock_epoch_label = 0U;
    std::uint32_t clock_epoch_algorithm = 0U;
    RawV1Digest clock_epoch_digest{};

    RunManifestVendorV1 vendor{};
    RunManifestBuildV1 build{};
    RunManifestConfigurationV1 configuration{};
    std::vector<RunManifestRawInputV1> raw_inputs;
    std::optional<RunManifestFactorV1> factor;
    RunManifestFaultInjectionV1 fault_injection{};
};

enum class RunManifestV1Error : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kUnsupportedSchema,
    kInvalidMode,
    kInvalidDate,
    kInvalidIdentity,
    kInvalidDigest,
    kInvalidString,
    kInvalidSourceRevision,
    kInvalidConfiguration,
    kInvalidInputCount,
    kDuplicateNamespace,
    kInvalidDurabilityPolicy,
    kInvalidRange,
    kInvalidMarker,
    kMarkerMismatch,
    kInvalidSegmentSet,
    kInvalidClockTransitions,
    kInvalidSyntheticProvenance,
    kInvalidReserveFinalization,
    kInvalidFactor,
    kInvalidFaultInjection,
    kEncodedSizeExceeded,
    kAllocationFailure,
};

[[nodiscard]] std::string_view RunManifestV1ErrorName(
    RunManifestV1Error error) noexcept;

[[nodiscard]] RunManifestV1Error ValidateRunManifestV1(
    const RunManifestV1& manifest) noexcept;

// RFC 8785 JCS for the frozen typed model. uint64 values are decimal
// strings; 128-bit identities and SHA-256 digests are fixed-width lowercase
// hexadecimal; absent values are JSON null; no BOM or trailing newline is
// emitted. Output changes only on success.
[[nodiscard]] RunManifestV1Error EncodeRunManifestV1Jcs(
    const RunManifestV1& manifest,
    std::string* output) noexcept;

class BuiltRunManifestV1 final {
public:
    ~BuiltRunManifestV1() = default;
    BuiltRunManifestV1(const BuiltRunManifestV1&) = delete;
    BuiltRunManifestV1& operator=(
        const BuiltRunManifestV1&) = delete;
    BuiltRunManifestV1(BuiltRunManifestV1&&) = delete;
    BuiltRunManifestV1& operator=(
        BuiltRunManifestV1&&) = delete;

    [[nodiscard]] const RunManifestV1& model()
        const noexcept {
        return model_;
    }
    [[nodiscard]] std::string_view canonical_jcs()
        const noexcept {
        return canonical_jcs_;
    }
    [[nodiscard]] const RawV1Digest& sha256()
        const noexcept {
        return sha256_;
    }

private:
    friend RunManifestV1Error BuildRunManifestV1(
        RunManifestV1,
        std::unique_ptr<BuiltRunManifestV1>*) noexcept;

    BuiltRunManifestV1(
        RunManifestV1 model,
        std::string canonical_jcs,
        RawV1Digest sha256) noexcept;

    RunManifestV1 model_{};
    std::string canonical_jcs_;
    RawV1Digest sha256_{};
};

[[nodiscard]] RunManifestV1Error BuildRunManifestV1(
    RunManifestV1 model,
    std::unique_ptr<BuiltRunManifestV1>* output) noexcept;

}  // namespace l2flow::ingress
