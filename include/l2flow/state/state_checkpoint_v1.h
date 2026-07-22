#pragma once

#include "l2flow/state/latest_state_v1.h"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace l2flow::state {

inline constexpr std::uint32_t kLatestStateCheckpointMagicV1 =
    0x3150434cU;  // "LCP1" as little-endian bytes.
inline constexpr std::uint16_t kLatestStateCheckpointVersionV1 = 1U;
inline constexpr std::size_t kLatestStateCheckpointHeaderBytesV1 = 512U;

enum class LatestStateInputFamilyV1 : std::uint16_t {
    kSnapshot = 1U,
    kTickQuality = 2U,
};

// The caller may set durability_barrier_satisfied only with one exact barrier for every
// consumed source/family namespace in the state image.  The current durable
// value must come from the validated Raw journal/control authority; Canonical
// processed/frontier progress is not a durable authority.
struct LatestStateDurabilityBarrierV1 final {
    LatestStateInputFamilyV1 family =
        LatestStateInputFamilyV1::kSnapshot;
    std::uint32_t shard_id = 0U;
    LatestStateCanonicalOriginV1 origin{};
    std::uint64_t max_consumed_shard_event_id = 0U;
    std::uint64_t max_consumed_origin_ingress_sequence = 0U;
    std::uint64_t max_consumed_origin_wal_end_pos = 0U;
    std::uint64_t current_raw_durable_global_wal_pos = 0U;

    [[nodiscard]] friend constexpr bool operator==(
        const LatestStateDurabilityBarrierV1&,
        const LatestStateDurabilityBarrierV1&) noexcept = default;
};

struct LatestStateCheckpointOptionsV1 final {
    // Caller assertion that every writer for every supplied slot was fenced
    // before EncodeLatestStateCheckpointV1 began and remains quiescent until
    // it returns.  This is what makes a multi-slot capture one common cut;
    // per-slot seqlocks alone provide only individually stable images.  The
    // local codec cannot verify the external writer lease/fence.
    bool writer_quiesced = false;

    // This is a caller assertion, not a non-forgeable proof created by the
    // codec.  The codec checks exact namespaces and numeric barriers; a
    // production publisher must additionally retain/authenticate the Raw
    // authority receipt outside this local V1 image.  Durable status also
    // requires writer_quiesced above.
    bool durability_barrier_satisfied = false;
    std::span<const LatestStateDurabilityBarrierV1> durability_barriers{};
};

struct LatestStateCheckpointSlotRefV1 final {
    std::uint32_t instrument_id = 0U;
    const LatestStateSlotV1* slot = nullptr;
};

struct LatestStateCheckpointTargetV1 final {
    std::uint32_t instrument_id = 0U;
    LatestStateSlotV1* slot = nullptr;
};

// Stored slot images have their seqlock word normalized to zero.  It is not a
// logical value and therefore does not participate in content identity.
struct LatestStateCheckpointEntryV1 final {
    std::uint32_t instrument_id = 0U;
    bool initialized = false;
    LatestStateSlotV1 normalized_slot{};
};

struct LatestStateCheckpointV1 final {
    LatestStateConfigV1 config{};
    bool writer_quiesced = false;
    bool durability_barrier_satisfied = false;
    std::vector<LatestStateDurabilityBarrierV1> durability_barriers;
    std::vector<LatestStateCheckpointEntryV1> entries;
    l2flow::common::Sha256Digest logical_state_sha256{};
    l2flow::common::Sha256Digest payload_sha256{};
};

enum class LatestStateCheckpointErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullArgument,
    kUnsupportedHost,
    kInvalidConfiguration,
    kInvalidInput,
    kReadFailed,
    kDuplicateInstrument,
    kInvalidDurabilityProof,
    kSizeOverflow,
    kCorruptWire,
    kHashMismatch,
    kCrcMismatch,
    kIdentityMismatch,
    kTargetNotEmpty,
    kWriterBusy,
    kAllocationFailed,
};

[[nodiscard]] std::string_view LatestStateCheckpointErrorNameV1(
    LatestStateCheckpointErrorV1 error) noexcept;

// Computes the deterministic logical state hash over schema/registry/shard
// config, sorted instrument IDs, initialization state and validated slot
// content.  Seqlock, state-generation/writer-owner mechanics and display-only
// clock labels are normalized away; complete source lineage, full clock
// digest, cursors, quality, validity and payload remain.  The checkpoint
// payload hash still binds the unnormalized retained identities and labels.
[[nodiscard]] LatestStateCheckpointErrorV1
ComputeLatestStateLogicalHashV1(
    const LatestStateConfigV1& config,
    std::span<const LatestStateCheckpointSlotRefV1> slots,
    l2flow::common::Sha256Digest* digest);

// Produces one deterministic little-endian wire image.  With the default
// options it is explicitly a non-durable recovery-acceleration image.  A
// durable image requires both an externally enforced writer-quiesced common
// cut and the exact Raw durability barriers described above.
[[nodiscard]] LatestStateCheckpointErrorV1 EncodeLatestStateCheckpointV1(
    const LatestStateConfigV1& config,
    std::span<const LatestStateCheckpointSlotRefV1> slots,
    const LatestStateCheckpointOptionsV1& options,
    std::vector<std::byte>* wire);

[[nodiscard]] LatestStateCheckpointErrorV1 DecodeLatestStateCheckpointV1(
    std::span<const std::byte> wire,
    LatestStateCheckpointV1* checkpoint);

// Restores only into an exact, all-zero target set.  Initialized slots receive
// a fresh even sequence (2); the checkpoint's normalized logical content and
// hash are unchanged.  Atomic switching of a complete SHM table generation is
// caller-owned and occurs only after this function succeeds.
[[nodiscard]] LatestStateCheckpointErrorV1 RestoreLatestStateCheckpointV1(
    const LatestStateCheckpointV1& checkpoint,
    const LatestStateConfigV1& expected_config,
    std::span<const LatestStateCheckpointTargetV1> targets);

}  // namespace l2flow::state
