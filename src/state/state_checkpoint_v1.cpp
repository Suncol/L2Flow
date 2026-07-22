#include "l2flow/state/state_checkpoint_v1.h"

#include "l2flow/common/crc32c.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <new>
#include <tuple>
#include <utility>

namespace l2flow::state {
namespace {

namespace canonical = l2flow::canonical;
namespace common = l2flow::common;

constexpr std::uint16_t kDurabilityBarrierSatisfiedFlag = 1U << 0U;
constexpr std::uint16_t kWriterQuiescedFlag = 1U << 1U;
constexpr std::uint16_t kCheckpointFlagsMask =
    kDurabilityBarrierSatisfiedFlag | kWriterQuiescedFlag;
constexpr std::size_t kBarrierBytes = 248U;
constexpr std::size_t kEntryBytes = 8U + kLatestStateSlotBytesV1;
constexpr std::size_t kCheckpointCrcOffset = 264U;
constexpr std::size_t kWordBytes = sizeof(std::uint64_t);
constexpr std::size_t kSlotWords = kLatestStateSlotBytesV1 / kWordBytes;
constexpr std::uint32_t kStableReadAttempts = 4096U;
constexpr std::size_t kStateGenerationOffset = 24U;
constexpr std::size_t kSnapshotClockLabelOffset = 120U;
constexpr std::size_t kTickClockLabelOffset = 2400U;
constexpr std::size_t kStateWriterInstanceOffset = 2544U;
constexpr std::string_view kLogicalHashDomain =
    "L2FLOW_LATEST_STATE_LOGICAL_CONTENT_V1\0";

namespace header_offset {
constexpr std::size_t kMagic = 0U;
constexpr std::size_t kVersion = 4U;
constexpr std::size_t kFlags = 6U;
constexpr std::size_t kHeaderBytes = 8U;
constexpr std::size_t kSlotBytes = 12U;
constexpr std::size_t kSlotCount = 16U;
constexpr std::size_t kBarrierCount = 24U;
constexpr std::size_t kEntryBytes = 32U;
constexpr std::size_t kBarrierBytes = 36U;
constexpr std::size_t kStateGeneration = 40U;
constexpr std::size_t kShardId = 48U;
constexpr std::size_t kShardCount = 52U;
constexpr std::size_t kRegistryVersion = 56U;
constexpr std::size_t kCanonicalSchemaSha256 = 64U;
constexpr std::size_t kCanonicalDtypeSha256 = 96U;
constexpr std::size_t kRegistrySha256 = 128U;
constexpr std::size_t kLatestStateSchemaSha256 = 160U;
constexpr std::size_t kLogicalStateSha256 = 192U;
constexpr std::size_t kPayloadSha256 = 224U;
constexpr std::size_t kWireBytes = 256U;
constexpr std::size_t kCrc32c = kCheckpointCrcOffset;
constexpr std::size_t kStateWriterInstance = 268U;
constexpr std::size_t kReserved = 284U;
}  // namespace header_offset

namespace barrier_offset {
constexpr std::size_t kFamily = 0U;
constexpr std::size_t kReserved16 = 2U;
constexpr std::size_t kShardId = 4U;
constexpr std::size_t kCaptureDate = 8U;
constexpr std::size_t kSourceStreamId = 12U;
constexpr std::size_t kStreamDayId = 16U;
constexpr std::size_t kSourceWriterInstance = 32U;
constexpr std::size_t kSourceGeneration = 48U;
constexpr std::size_t kCanonicalGeneration = 56U;
constexpr std::size_t kClockAlgorithm = 64U;
constexpr std::size_t kReserved32 = 68U;
constexpr std::size_t kClockLabel = 72U;
constexpr std::size_t kClockDigest = 80U;
constexpr std::size_t kCanonicalSchemaSha256 = 112U;
constexpr std::size_t kCanonicalDtypeSha256 = 144U;
constexpr std::size_t kRegistryVersion = 176U;
constexpr std::size_t kRegistrySha256 = 184U;
constexpr std::size_t kMaxConsumedShardEventId = 216U;
constexpr std::size_t kMaxConsumedIngressSequence = 224U;
constexpr std::size_t kMaxConsumedWal = 232U;
constexpr std::size_t kCurrentRawDurableWal = 240U;
}  // namespace barrier_offset

static_assert(kSlotWords == 512U);
static_assert(barrier_offset::kCurrentRawDurableWal + 8U == kBarrierBytes);

template <typename T>
void StoreScalar(
    std::span<std::byte> bytes,
    std::size_t offset,
    T value) noexcept {
    static_assert(std::is_integral_v<T>);
    std::memcpy(
        bytes.data() + static_cast<std::ptrdiff_t>(offset),
        &value,
        sizeof(value));
}

template <typename T>
T LoadScalar(
    std::span<const std::byte> bytes,
    std::size_t offset) noexcept {
    static_assert(std::is_integral_v<T>);
    T value{};
    std::memcpy(
        &value,
        bytes.data() + static_cast<std::ptrdiff_t>(offset),
        sizeof(value));
    return value;
}

template <std::size_t Size>
void StoreBytes(
    std::span<std::byte> bytes,
    std::size_t offset,
    const std::array<std::byte, Size>& value) noexcept {
    std::memcpy(
        bytes.data() + static_cast<std::ptrdiff_t>(offset),
        value.data(),
        Size);
}

template <std::size_t Size>
std::array<std::byte, Size> LoadBytes(
    std::span<const std::byte> bytes,
    std::size_t offset) noexcept {
    std::array<std::byte, Size> value{};
    std::memcpy(
        value.data(),
        bytes.data() + static_cast<std::ptrdiff_t>(offset),
        Size);
    return value;
}

bool BytesAreZero(std::span<const std::byte> bytes) noexcept {
    return std::all_of(
        bytes.begin(), bytes.end(),
        [](std::byte byte) noexcept { return byte == std::byte{0U}; });
}

void StoreSlotSequence(
    LatestStateSlotV1* slot,
    std::uint64_t sequence) noexcept {
    std::memcpy(slot->bytes.data(), &sequence, sizeof(sequence));
}

std::uint64_t LoadSlotWord(
    const LatestStateSlotV1& slot,
    std::size_t index) noexcept {
    std::uint64_t value = 0U;
    std::memcpy(
        &value,
        slot.bytes.data() +
            static_cast<std::ptrdiff_t>(index * kWordBytes),
        kWordBytes);
    return value;
}

std::uint64_t* SharedSlotWords(LatestStateSlotV1* slot) noexcept {
    return reinterpret_cast<std::uint64_t*>(slot->bytes.data());
}

bool OriginSameIdentity(
    const LatestStateCanonicalOriginV1& left,
    const LatestStateCanonicalOriginV1& right) noexcept {
    return left.capture_date == right.capture_date &&
           left.source_stream_id == right.source_stream_id &&
           left.stream_day_id == right.stream_day_id &&
           left.source_writer_instance == right.source_writer_instance &&
           left.source_generation == right.source_generation &&
           left.canonical_generation == right.canonical_generation &&
           left.clock_epoch.algorithm == right.clock_epoch.algorithm &&
           left.clock_epoch.digest == right.clock_epoch.digest &&
           left.canonical_schema_sha256 ==
               right.canonical_schema_sha256 &&
           left.canonical_dtype_sha256 ==
               right.canonical_dtype_sha256 &&
           left.registry_version == right.registry_version &&
           left.registry_sha256 == right.registry_sha256;
}

bool BarrierKeyLess(
    const LatestStateDurabilityBarrierV1& left,
    const LatestStateDurabilityBarrierV1& right) noexcept {
    return std::tie(
               left.origin.source_stream_id,
               left.origin.capture_date,
               left.family,
               left.shard_id,
               left.origin.stream_day_id,
               left.origin.source_writer_instance,
               left.origin.source_generation,
               left.origin.canonical_generation,
               left.origin.clock_epoch.algorithm,
               left.origin.clock_epoch.digest,
               left.origin.canonical_schema_sha256,
               left.origin.canonical_dtype_sha256,
               left.origin.registry_version,
               left.origin.registry_sha256) <
           std::tie(
               right.origin.source_stream_id,
               right.origin.capture_date,
               right.family,
               right.shard_id,
               right.origin.stream_day_id,
               right.origin.source_writer_instance,
               right.origin.source_generation,
               right.origin.canonical_generation,
               right.origin.clock_epoch.algorithm,
               right.origin.clock_epoch.digest,
               right.origin.canonical_schema_sha256,
               right.origin.canonical_dtype_sha256,
               right.origin.registry_version,
               right.origin.registry_sha256);
}

bool SameBarrierKey(
    const LatestStateDurabilityBarrierV1& left,
    const LatestStateDurabilityBarrierV1& right) noexcept {
    return left.family == right.family &&
           left.shard_id == right.shard_id &&
           OriginSameIdentity(left.origin, right.origin);
}

bool BarrierValid(
    const LatestStateConfigV1& config,
    const LatestStateDurabilityBarrierV1& barrier) noexcept {
    return (barrier.family == LatestStateInputFamilyV1::kSnapshot ||
            barrier.family == LatestStateInputFamilyV1::kTickQuality) &&
           barrier.shard_id == config.shard_id &&
           LatestStateOriginValidV1(barrier.origin) &&
           barrier.origin.canonical_schema_sha256 ==
               config.canonical_schema_sha256 &&
           barrier.origin.canonical_dtype_sha256 ==
               config.canonical_dtype_sha256 &&
           barrier.origin.registry_version == config.registry_version &&
           barrier.origin.registry_sha256 == config.registry_sha256 &&
           barrier.max_consumed_shard_event_id != 0U &&
           barrier.max_consumed_origin_ingress_sequence != 0U &&
           barrier.max_consumed_origin_wal_end_pos != 0U &&
           barrier.current_raw_durable_global_wal_pos >=
               barrier.max_consumed_origin_wal_end_pos;
}

LatestStateCheckpointErrorV1 StableNormalizedEntry(
    const LatestStateCheckpointSlotRefV1& source,
    const LatestStateConfigV1& config,
    LatestStateCheckpointEntryV1* entry,
    LatestStateValueV1* value) noexcept {
    if (source.instrument_id == 0U || source.slot == nullptr ||
        entry == nullptr || value == nullptr) {
        return LatestStateCheckpointErrorV1::kInvalidInput;
    }
    LatestStateSlotV1 image;
    const std::uint32_t copy_result =
        l2flow_latest_state_copy_stable_v1(
            source.slot->bytes.data(), source.slot->bytes.size(),
            image.bytes.data(), image.bytes.size(), kStableReadAttempts);
    if (copy_result != 0U) {
        return LatestStateCheckpointErrorV1::kReadFailed;
    }

    entry->instrument_id = source.instrument_id;
    entry->initialized = !BytesAreZero(image.bytes);
    entry->normalized_slot = image;
    StoreSlotSequence(&entry->normalized_slot, 0U);
    *value = {};
    if (!entry->initialized) {
        if (!BytesAreZero(entry->normalized_slot.bytes)) {
            return LatestStateCheckpointErrorV1::kInvalidInput;
        }
        return LatestStateCheckpointErrorV1::kNone;
    }

    // Decode the exact image copied above.  Reading the live slot a second
    // time could pair checkpoint bytes from generation/cursor A with
    // durability requirements derived from a later publication B.
    const LatestStateErrorV1 read_error =
        ReadLatestStateSlotV1(image, value);
    if (read_error != LatestStateErrorV1::kNone ||
        value->config != config ||
        value->snapshot.header.instrument_id != source.instrument_id) {
        return LatestStateCheckpointErrorV1::kInvalidInput;
    }
    return LatestStateCheckpointErrorV1::kNone;
}

LatestStateCheckpointErrorV1 BuildEntries(
    const LatestStateConfigV1& config,
    std::span<const LatestStateCheckpointSlotRefV1> slots,
    std::vector<LatestStateCheckpointEntryV1>* entries,
    std::vector<LatestStateValueV1>* values) {
    try {
        std::vector<LatestStateCheckpointSlotRefV1> sorted(
            slots.begin(), slots.end());
        std::sort(
            sorted.begin(), sorted.end(),
            [](const LatestStateCheckpointSlotRefV1& left,
               const LatestStateCheckpointSlotRefV1& right) noexcept {
                return left.instrument_id < right.instrument_id;
            });
        for (std::size_t index = 0U; index < sorted.size(); ++index) {
            if (sorted[index].instrument_id == 0U ||
                sorted[index].slot == nullptr) {
                return LatestStateCheckpointErrorV1::kInvalidInput;
            }
            if (index != 0U &&
                sorted[index - 1U].instrument_id ==
                    sorted[index].instrument_id) {
                return LatestStateCheckpointErrorV1::kDuplicateInstrument;
            }
        }
        entries->resize(sorted.size());
        values->resize(sorted.size());
        for (std::size_t index = 0U; index < sorted.size(); ++index) {
            const LatestStateCheckpointErrorV1 error = StableNormalizedEntry(
                sorted[index], config, &(*entries)[index],
                &(*values)[index]);
            if (error != LatestStateCheckpointErrorV1::kNone) {
                return error;
            }
        }
    } catch (const std::bad_alloc&) {
        return LatestStateCheckpointErrorV1::kAllocationFailed;
    }
    return LatestStateCheckpointErrorV1::kNone;
}

void MergeRequirement(
    LatestStateDurabilityBarrierV1 requirement,
    std::vector<LatestStateDurabilityBarrierV1>* requirements) {
    const auto iterator = std::find_if(
        requirements->begin(), requirements->end(),
        [&](const LatestStateDurabilityBarrierV1& existing) noexcept {
            return SameBarrierKey(existing, requirement);
        });
    if (iterator == requirements->end()) {
        requirements->push_back(std::move(requirement));
        return;
    }
    iterator->max_consumed_origin_wal_end_pos = std::max(
        iterator->max_consumed_origin_wal_end_pos,
        requirement.max_consumed_origin_wal_end_pos);
    iterator->max_consumed_shard_event_id = std::max(
        iterator->max_consumed_shard_event_id,
        requirement.max_consumed_shard_event_id);
    iterator->max_consumed_origin_ingress_sequence = std::max(
        iterator->max_consumed_origin_ingress_sequence,
        requirement.max_consumed_origin_ingress_sequence);
}

LatestStateCheckpointErrorV1 BuildRequirements(
    const LatestStateConfigV1& config,
    std::span<const LatestStateValueV1> values,
    std::span<const LatestStateCheckpointEntryV1> entries,
    std::vector<LatestStateDurabilityBarrierV1>* requirements) {
    try {
        requirements->clear();
        for (std::size_t index = 0U; index < entries.size(); ++index) {
            if (!entries[index].initialized) {
                continue;
            }
            LatestStateDurabilityBarrierV1 snapshot;
            snapshot.family = LatestStateInputFamilyV1::kSnapshot;
            snapshot.shard_id = config.shard_id;
            snapshot.origin = values[index].origin;
            snapshot.max_consumed_shard_event_id =
                values[index].snapshot.header.shard_event_id;
            snapshot.max_consumed_origin_ingress_sequence =
                values[index].snapshot.header.origin_ingress_sequence;
            snapshot.max_consumed_origin_wal_end_pos =
                values[index].snapshot.header.origin_wal_end_pos;
            MergeRequirement(std::move(snapshot), requirements);

            if (values[index].tick_quality_initialized) {
                LatestStateDurabilityBarrierV1 tick;
                tick.family = LatestStateInputFamilyV1::kTickQuality;
                tick.shard_id = config.shard_id;
                tick.origin = values[index].tick_origin;
                tick.max_consumed_shard_event_id =
                    values[index].tick_shard_event_id;
                tick.max_consumed_origin_ingress_sequence =
                    values[index].tick_origin_ingress_sequence;
                tick.max_consumed_origin_wal_end_pos =
                    values[index].tick_origin_wal_end_pos;
                MergeRequirement(std::move(tick), requirements);
            }
        }
        std::sort(
            requirements->begin(), requirements->end(), BarrierKeyLess);
    } catch (const std::bad_alloc&) {
        return LatestStateCheckpointErrorV1::kAllocationFailed;
    }
    return LatestStateCheckpointErrorV1::kNone;
}

LatestStateCheckpointErrorV1 ValidateAndSortBarriers(
    const LatestStateConfigV1& config,
    bool writer_quiesced,
    bool durability_barrier_satisfied,
    std::span<const LatestStateDurabilityBarrierV1> supplied,
    std::span<const LatestStateDurabilityBarrierV1> requirements,
    std::vector<LatestStateDurabilityBarrierV1>* sorted) {
    if (durability_barrier_satisfied && !writer_quiesced) {
        return LatestStateCheckpointErrorV1::kInvalidDurabilityProof;
    }
    if (!durability_barrier_satisfied) {
        if (!supplied.empty()) {
            return LatestStateCheckpointErrorV1::kInvalidDurabilityProof;
        }
        sorted->clear();
        return LatestStateCheckpointErrorV1::kNone;
    }
    if (requirements.empty() || supplied.size() != requirements.size()) {
        return LatestStateCheckpointErrorV1::kInvalidDurabilityProof;
    }
    try {
        sorted->assign(supplied.begin(), supplied.end());
        std::sort(sorted->begin(), sorted->end(), BarrierKeyLess);
    } catch (const std::bad_alloc&) {
        return LatestStateCheckpointErrorV1::kAllocationFailed;
    }
    for (std::size_t index = 0U; index < sorted->size(); ++index) {
        if (!BarrierValid(config, (*sorted)[index]) ||
            !SameBarrierKey((*sorted)[index], requirements[index]) ||
            (*sorted)[index].max_consumed_shard_event_id !=
                requirements[index].max_consumed_shard_event_id ||
            (*sorted)[index].max_consumed_origin_ingress_sequence !=
                requirements[index]
                    .max_consumed_origin_ingress_sequence ||
            (*sorted)[index].max_consumed_origin_wal_end_pos !=
                requirements[index].max_consumed_origin_wal_end_pos ||
            (index != 0U &&
             SameBarrierKey((*sorted)[index - 1U], (*sorted)[index]))) {
            return LatestStateCheckpointErrorV1::kInvalidDurabilityProof;
        }
    }
    return LatestStateCheckpointErrorV1::kNone;
}

bool HashUpdate(
    common::Sha256Hasher* hasher,
    std::span<const std::byte> bytes) noexcept {
    return hasher->Update(bytes);
}

template <typename T>
bool HashScalar(common::Sha256Hasher* hasher, T value) noexcept {
    static_assert(std::is_integral_v<T>);
    return HashUpdate(
        hasher,
        std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(&value), sizeof(value)));
}

LatestStateCheckpointErrorV1 HashEntries(
    const LatestStateConfigV1& config,
    std::span<const LatestStateCheckpointEntryV1> entries,
    common::Sha256Digest* digest) noexcept {
    common::Sha256Hasher hasher;
    const auto domain_bytes = std::span<const std::byte>(
        reinterpret_cast<const std::byte*>(kLogicalHashDomain.data()),
        kLogicalHashDomain.size());
    if (!hasher.Update(domain_bytes) ||
        !HashScalar(&hasher, config.shard_id) ||
        !HashScalar(&hasher, config.shard_count) ||
        !HashUpdate(&hasher, config.canonical_schema_sha256) ||
        !HashUpdate(&hasher, config.canonical_dtype_sha256) ||
        !HashScalar(&hasher, config.registry_version) ||
        !HashUpdate(&hasher, config.registry_sha256)) {
        return LatestStateCheckpointErrorV1::kSizeOverflow;
    }
    const common::Sha256Digest state_schema =
        LatestStateSchemaDescriptorSha256V1();
    if (!HashUpdate(&hasher, state_schema) ||
        !HashScalar(
            &hasher, static_cast<std::uint64_t>(entries.size()))) {
        return LatestStateCheckpointErrorV1::kSizeOverflow;
    }
    for (const LatestStateCheckpointEntryV1& entry : entries) {
        const std::uint32_t initialized = entry.initialized ? 1U : 0U;
        LatestStateSlotV1 logical_slot = entry.normalized_slot;
        // Seqlock and state-writer generation/owner are publication mechanics,
        // not market-state content.  Labels are display/index aids, not clock
        // identity.  Full source lineage, clock algorithm/digest, cursors,
        // quality, validity and payload remain in the logical hash.
        std::uint64_t zero_u64 = 0U;
        std::memcpy(
            logical_slot.bytes.data() +
                static_cast<std::ptrdiff_t>(kStateGenerationOffset),
            &zero_u64, sizeof(zero_u64));
        std::memcpy(
            logical_slot.bytes.data() +
                static_cast<std::ptrdiff_t>(kSnapshotClockLabelOffset),
            &zero_u64, sizeof(zero_u64));
        std::memcpy(
            logical_slot.bytes.data() +
                static_cast<std::ptrdiff_t>(kTickClockLabelOffset),
            &zero_u64, sizeof(zero_u64));
        std::memset(
            logical_slot.bytes.data() +
                static_cast<std::ptrdiff_t>(kStateWriterInstanceOffset),
            0, common::Identity128{}.size());
        if (!HashScalar(&hasher, entry.instrument_id) ||
            !HashScalar(&hasher, initialized) ||
            !HashUpdate(&hasher, logical_slot.bytes)) {
            return LatestStateCheckpointErrorV1::kSizeOverflow;
        }
    }
    if (!hasher.Finalize(digest)) {
        return LatestStateCheckpointErrorV1::kSizeOverflow;
    }
    return LatestStateCheckpointErrorV1::kNone;
}

void EncodeBarrier(
    const LatestStateDurabilityBarrierV1& value,
    std::span<std::byte> bytes) noexcept {
    StoreScalar<std::uint16_t>(
        bytes, barrier_offset::kFamily,
        static_cast<std::uint16_t>(value.family));
    StoreScalar<std::uint32_t>(
        bytes, barrier_offset::kShardId, value.shard_id);
    StoreScalar<std::uint32_t>(
        bytes, barrier_offset::kCaptureDate, value.origin.capture_date);
    StoreScalar<std::uint32_t>(
        bytes, barrier_offset::kSourceStreamId,
        value.origin.source_stream_id);
    StoreBytes(bytes, barrier_offset::kStreamDayId, value.origin.stream_day_id);
    StoreBytes(
        bytes, barrier_offset::kSourceWriterInstance,
        value.origin.source_writer_instance);
    StoreScalar<std::uint64_t>(
        bytes, barrier_offset::kSourceGeneration,
        value.origin.source_generation);
    StoreScalar<std::uint64_t>(
        bytes, barrier_offset::kCanonicalGeneration,
        value.origin.canonical_generation);
    StoreScalar<std::uint32_t>(
        bytes, barrier_offset::kClockAlgorithm,
        value.origin.clock_epoch.algorithm);
    StoreScalar<std::uint64_t>(
        bytes, barrier_offset::kClockLabel,
        value.origin.clock_epoch.label);
    StoreBytes(
        bytes, barrier_offset::kClockDigest,
        value.origin.clock_epoch.digest);
    StoreBytes(
        bytes, barrier_offset::kCanonicalSchemaSha256,
        value.origin.canonical_schema_sha256);
    StoreBytes(
        bytes, barrier_offset::kCanonicalDtypeSha256,
        value.origin.canonical_dtype_sha256);
    StoreScalar<std::uint64_t>(
        bytes, barrier_offset::kRegistryVersion,
        value.origin.registry_version);
    StoreBytes(
        bytes, barrier_offset::kRegistrySha256,
        value.origin.registry_sha256);
    StoreScalar<std::uint64_t>(
        bytes, barrier_offset::kMaxConsumedShardEventId,
        value.max_consumed_shard_event_id);
    StoreScalar<std::uint64_t>(
        bytes, barrier_offset::kMaxConsumedIngressSequence,
        value.max_consumed_origin_ingress_sequence);
    StoreScalar<std::uint64_t>(
        bytes, barrier_offset::kMaxConsumedWal,
        value.max_consumed_origin_wal_end_pos);
    StoreScalar<std::uint64_t>(
        bytes, barrier_offset::kCurrentRawDurableWal,
        value.current_raw_durable_global_wal_pos);
}

LatestStateDurabilityBarrierV1 DecodeBarrier(
    std::span<const std::byte> bytes) noexcept {
    LatestStateDurabilityBarrierV1 value;
    value.family = static_cast<LatestStateInputFamilyV1>(
        LoadScalar<std::uint16_t>(bytes, barrier_offset::kFamily));
    value.shard_id = LoadScalar<std::uint32_t>(
        bytes, barrier_offset::kShardId);
    value.origin.capture_date = LoadScalar<std::uint32_t>(
        bytes, barrier_offset::kCaptureDate);
    value.origin.source_stream_id = LoadScalar<std::uint32_t>(
        bytes, barrier_offset::kSourceStreamId);
    value.origin.stream_day_id = LoadBytes<16U>(
        bytes, barrier_offset::kStreamDayId);
    value.origin.source_writer_instance = LoadBytes<16U>(
        bytes, barrier_offset::kSourceWriterInstance);
    value.origin.source_generation = LoadScalar<std::uint64_t>(
        bytes, barrier_offset::kSourceGeneration);
    value.origin.canonical_generation = LoadScalar<std::uint64_t>(
        bytes, barrier_offset::kCanonicalGeneration);
    value.origin.clock_epoch.algorithm = LoadScalar<std::uint32_t>(
        bytes, barrier_offset::kClockAlgorithm);
    value.origin.clock_epoch.label = LoadScalar<std::uint64_t>(
        bytes, barrier_offset::kClockLabel);
    value.origin.clock_epoch.digest = LoadBytes<32U>(
        bytes, barrier_offset::kClockDigest);
    value.origin.canonical_schema_sha256 = LoadBytes<32U>(
        bytes, barrier_offset::kCanonicalSchemaSha256);
    value.origin.canonical_dtype_sha256 = LoadBytes<32U>(
        bytes, barrier_offset::kCanonicalDtypeSha256);
    value.origin.registry_version = LoadScalar<std::uint64_t>(
        bytes, barrier_offset::kRegistryVersion);
    value.origin.registry_sha256 = LoadBytes<32U>(
        bytes, barrier_offset::kRegistrySha256);
    value.max_consumed_shard_event_id = LoadScalar<std::uint64_t>(
        bytes, barrier_offset::kMaxConsumedShardEventId);
    value.max_consumed_origin_ingress_sequence =
        LoadScalar<std::uint64_t>(
            bytes, barrier_offset::kMaxConsumedIngressSequence);
    value.max_consumed_origin_wal_end_pos = LoadScalar<std::uint64_t>(
        bytes, barrier_offset::kMaxConsumedWal);
    value.current_raw_durable_global_wal_pos = LoadScalar<std::uint64_t>(
        bytes, barrier_offset::kCurrentRawDurableWal);
    return value;
}

std::uint32_t ComputeCheckpointCrc(
    std::span<const std::byte> wire) noexcept {
    common::Crc32cState crc;
    crc.Update(wire.first(kCheckpointCrcOffset));
    const std::array<std::byte, 4U> zero{};
    crc.Update(zero);
    crc.Update(wire.subspan(kCheckpointCrcOffset + zero.size()));
    return crc.Finalize();
}

LatestStateCheckpointErrorV1 ValidateNormalizedEntry(
    const LatestStateConfigV1& config,
    const LatestStateCheckpointEntryV1& entry,
    LatestStateValueV1* value) noexcept {
    if (entry.instrument_id == 0U ||
        LoadSlotWord(entry.normalized_slot, 0U) != 0U) {
        return LatestStateCheckpointErrorV1::kCorruptWire;
    }
    if (!entry.initialized) {
        return BytesAreZero(entry.normalized_slot.bytes)
                   ? LatestStateCheckpointErrorV1::kNone
                   : LatestStateCheckpointErrorV1::kCorruptWire;
    }

    LatestStateSlotV1 validation = entry.normalized_slot;
    StoreSlotSequence(&validation, 2U);
    const LatestStateErrorV1 read_error =
        ReadLatestStateSlotV1(validation, value);
    if (read_error != LatestStateErrorV1::kNone ||
        value->config != config ||
        value->snapshot.header.instrument_id != entry.instrument_id) {
        return LatestStateCheckpointErrorV1::kCorruptWire;
    }
    value->slot_sequence = 0U;
    return LatestStateCheckpointErrorV1::kNone;
}

}  // namespace

std::string_view LatestStateCheckpointErrorNameV1(
    LatestStateCheckpointErrorV1 error) noexcept {
    switch (error) {
        case LatestStateCheckpointErrorV1::kNone:
            return "none";
        case LatestStateCheckpointErrorV1::kNullArgument:
            return "null_argument";
        case LatestStateCheckpointErrorV1::kUnsupportedHost:
            return "unsupported_host";
        case LatestStateCheckpointErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case LatestStateCheckpointErrorV1::kInvalidInput:
            return "invalid_input";
        case LatestStateCheckpointErrorV1::kReadFailed:
            return "read_failed";
        case LatestStateCheckpointErrorV1::kDuplicateInstrument:
            return "duplicate_instrument";
        case LatestStateCheckpointErrorV1::kInvalidDurabilityProof:
            return "invalid_durability_proof";
        case LatestStateCheckpointErrorV1::kSizeOverflow:
            return "size_overflow";
        case LatestStateCheckpointErrorV1::kCorruptWire:
            return "corrupt_wire";
        case LatestStateCheckpointErrorV1::kHashMismatch:
            return "hash_mismatch";
        case LatestStateCheckpointErrorV1::kCrcMismatch:
            return "crc_mismatch";
        case LatestStateCheckpointErrorV1::kIdentityMismatch:
            return "identity_mismatch";
        case LatestStateCheckpointErrorV1::kTargetNotEmpty:
            return "target_not_empty";
        case LatestStateCheckpointErrorV1::kWriterBusy:
            return "writer_busy";
        case LatestStateCheckpointErrorV1::kAllocationFailed:
            return "allocation_failed";
    }
    return "unknown";
}

LatestStateCheckpointErrorV1 ComputeLatestStateLogicalHashV1(
    const LatestStateConfigV1& config,
    std::span<const LatestStateCheckpointSlotRefV1> slots,
    common::Sha256Digest* digest) {
    if (digest == nullptr) {
        return LatestStateCheckpointErrorV1::kNullArgument;
    }
    if (!LatestStateHostSupportedV1()) {
        return LatestStateCheckpointErrorV1::kUnsupportedHost;
    }
    if (!LatestStateConfigValidV1(config)) {
        return LatestStateCheckpointErrorV1::kInvalidConfiguration;
    }
    std::vector<LatestStateCheckpointEntryV1> entries;
    std::vector<LatestStateValueV1> values;
    const LatestStateCheckpointErrorV1 build_error =
        BuildEntries(config, slots, &entries, &values);
    if (build_error != LatestStateCheckpointErrorV1::kNone) {
        return build_error;
    }
    common::Sha256Digest result{};
    const LatestStateCheckpointErrorV1 hash_error =
        HashEntries(config, entries, &result);
    if (hash_error == LatestStateCheckpointErrorV1::kNone) {
        *digest = result;
    }
    return hash_error;
}

LatestStateCheckpointErrorV1 EncodeLatestStateCheckpointV1(
    const LatestStateConfigV1& config,
    std::span<const LatestStateCheckpointSlotRefV1> slots,
    const LatestStateCheckpointOptionsV1& options,
    std::vector<std::byte>* wire) {
    if (wire == nullptr) {
        return LatestStateCheckpointErrorV1::kNullArgument;
    }
    wire->clear();
    if (!LatestStateHostSupportedV1()) {
        return LatestStateCheckpointErrorV1::kUnsupportedHost;
    }
    if (!LatestStateConfigValidV1(config)) {
        return LatestStateCheckpointErrorV1::kInvalidConfiguration;
    }
    if (options.durability_barrier_satisfied &&
        !options.writer_quiesced) {
        return LatestStateCheckpointErrorV1::kInvalidDurabilityProof;
    }

    std::vector<LatestStateCheckpointEntryV1> entries;
    std::vector<LatestStateValueV1> values;
    LatestStateCheckpointErrorV1 error =
        BuildEntries(config, slots, &entries, &values);
    if (error != LatestStateCheckpointErrorV1::kNone) {
        return error;
    }
    std::vector<LatestStateDurabilityBarrierV1> requirements;
    error = BuildRequirements(config, values, entries, &requirements);
    if (error != LatestStateCheckpointErrorV1::kNone) {
        return error;
    }
    std::vector<LatestStateDurabilityBarrierV1> barriers;
    error = ValidateAndSortBarriers(
        config, options.writer_quiesced,
        options.durability_barrier_satisfied,
        options.durability_barriers, requirements, &barriers);
    if (error != LatestStateCheckpointErrorV1::kNone) {
        return error;
    }

    common::Sha256Digest logical_sha{};
    error = HashEntries(config, entries, &logical_sha);
    if (error != LatestStateCheckpointErrorV1::kNone) {
        return error;
    }
    if (barriers.size() >
            (std::numeric_limits<std::size_t>::max() -
             kLatestStateCheckpointHeaderBytesV1) /
                kBarrierBytes ||
        entries.size() >
            (std::numeric_limits<std::size_t>::max() -
             kLatestStateCheckpointHeaderBytesV1 -
             barriers.size() * kBarrierBytes) /
                kEntryBytes) {
        return LatestStateCheckpointErrorV1::kSizeOverflow;
    }
    const std::size_t wire_size =
        kLatestStateCheckpointHeaderBytesV1 +
        barriers.size() * kBarrierBytes +
        entries.size() * kEntryBytes;
    try {
        wire->assign(wire_size, std::byte{0U});
    } catch (const std::bad_alloc&) {
        return LatestStateCheckpointErrorV1::kAllocationFailed;
    }

    std::span<std::byte> output(*wire);
    std::size_t cursor = kLatestStateCheckpointHeaderBytesV1;
    for (const LatestStateDurabilityBarrierV1& barrier : barriers) {
        EncodeBarrier(barrier, output.subspan(cursor, kBarrierBytes));
        cursor += kBarrierBytes;
    }
    for (const LatestStateCheckpointEntryV1& entry : entries) {
        StoreScalar<std::uint32_t>(output, cursor, entry.instrument_id);
        StoreScalar<std::uint32_t>(
            output, cursor + 4U, entry.initialized ? 1U : 0U);
        std::memcpy(
            output.data() + static_cast<std::ptrdiff_t>(cursor + 8U),
            entry.normalized_slot.bytes.data(),
            entry.normalized_slot.bytes.size());
        cursor += kEntryBytes;
    }
    const common::Sha256Digest payload_sha = common::ComputeSha256(
        std::span<const std::byte>(output).subspan(
            kLatestStateCheckpointHeaderBytesV1));

    StoreScalar<std::uint32_t>(
        output, header_offset::kMagic, kLatestStateCheckpointMagicV1);
    StoreScalar<std::uint16_t>(
        output, header_offset::kVersion, kLatestStateCheckpointVersionV1);
    StoreScalar<std::uint16_t>(
        output, header_offset::kFlags,
        static_cast<std::uint16_t>(
            (options.durability_barrier_satisfied ?
                 kDurabilityBarrierSatisfiedFlag : 0U) |
            (options.writer_quiesced ? kWriterQuiescedFlag : 0U)));
    StoreScalar<std::uint32_t>(
        output, header_offset::kHeaderBytes,
        static_cast<std::uint32_t>(kLatestStateCheckpointHeaderBytesV1));
    StoreScalar<std::uint32_t>(
        output, header_offset::kSlotBytes,
        static_cast<std::uint32_t>(kLatestStateSlotBytesV1));
    StoreScalar<std::uint64_t>(
        output, header_offset::kSlotCount,
        static_cast<std::uint64_t>(entries.size()));
    StoreScalar<std::uint64_t>(
        output, header_offset::kBarrierCount,
        static_cast<std::uint64_t>(barriers.size()));
    StoreScalar<std::uint32_t>(
        output, header_offset::kEntryBytes,
        static_cast<std::uint32_t>(kEntryBytes));
    StoreScalar<std::uint32_t>(
        output, header_offset::kBarrierBytes,
        static_cast<std::uint32_t>(kBarrierBytes));
    StoreScalar<std::uint64_t>(
        output, header_offset::kStateGeneration,
        config.state_generation);
    StoreBytes(
        output, header_offset::kStateWriterInstance,
        config.state_writer_instance);
    StoreScalar<std::uint32_t>(
        output, header_offset::kShardId, config.shard_id);
    StoreScalar<std::uint32_t>(
        output, header_offset::kShardCount, config.shard_count);
    StoreScalar<std::uint64_t>(
        output, header_offset::kRegistryVersion,
        config.registry_version);
    StoreBytes(
        output, header_offset::kCanonicalSchemaSha256,
        config.canonical_schema_sha256);
    StoreBytes(
        output, header_offset::kCanonicalDtypeSha256,
        config.canonical_dtype_sha256);
    StoreBytes(
        output, header_offset::kRegistrySha256,
        config.registry_sha256);
    StoreBytes(
        output, header_offset::kLatestStateSchemaSha256,
        LatestStateSchemaDescriptorSha256V1());
    StoreBytes(
        output, header_offset::kLogicalStateSha256, logical_sha);
    StoreBytes(output, header_offset::kPayloadSha256, payload_sha);
    StoreScalar<std::uint64_t>(
        output, header_offset::kWireBytes,
        static_cast<std::uint64_t>(wire_size));
    StoreScalar<std::uint32_t>(
        output, header_offset::kCrc32c,
        ComputeCheckpointCrc(output));
    return LatestStateCheckpointErrorV1::kNone;
}

LatestStateCheckpointErrorV1 DecodeLatestStateCheckpointV1(
    std::span<const std::byte> wire,
    LatestStateCheckpointV1* checkpoint) {
    if (checkpoint == nullptr) {
        return LatestStateCheckpointErrorV1::kNullArgument;
    }
    if (!LatestStateHostSupportedV1()) {
        return LatestStateCheckpointErrorV1::kUnsupportedHost;
    }
    if (wire.size() < kLatestStateCheckpointHeaderBytesV1) {
        return LatestStateCheckpointErrorV1::kCorruptWire;
    }
    const std::uint16_t flags = LoadScalar<std::uint16_t>(
        wire, header_offset::kFlags);
    if (LoadScalar<std::uint32_t>(wire, header_offset::kMagic) !=
            kLatestStateCheckpointMagicV1 ||
        LoadScalar<std::uint16_t>(wire, header_offset::kVersion) !=
            kLatestStateCheckpointVersionV1 ||
        (flags & ~kCheckpointFlagsMask) != 0U ||
        LoadScalar<std::uint32_t>(wire, header_offset::kHeaderBytes) !=
            kLatestStateCheckpointHeaderBytesV1 ||
        LoadScalar<std::uint32_t>(wire, header_offset::kSlotBytes) !=
            kLatestStateSlotBytesV1 ||
        LoadScalar<std::uint32_t>(wire, header_offset::kEntryBytes) !=
            kEntryBytes ||
        LoadScalar<std::uint32_t>(wire, header_offset::kBarrierBytes) !=
            kBarrierBytes ||
        LoadScalar<std::uint64_t>(wire, header_offset::kWireBytes) !=
            wire.size() ||
        !BytesAreZero(wire.subspan(
            header_offset::kReserved,
            kLatestStateCheckpointHeaderBytesV1 -
                header_offset::kReserved))) {
        return LatestStateCheckpointErrorV1::kCorruptWire;
    }
    if (LoadScalar<std::uint32_t>(wire, header_offset::kCrc32c) !=
        ComputeCheckpointCrc(wire)) {
        return LatestStateCheckpointErrorV1::kCrcMismatch;
    }

    const std::uint64_t slot_count_u64 = LoadScalar<std::uint64_t>(
        wire, header_offset::kSlotCount);
    const std::uint64_t barrier_count_u64 = LoadScalar<std::uint64_t>(
        wire, header_offset::kBarrierCount);
    if (slot_count_u64 > std::numeric_limits<std::size_t>::max() ||
        barrier_count_u64 > std::numeric_limits<std::size_t>::max()) {
        return LatestStateCheckpointErrorV1::kSizeOverflow;
    }
    const std::size_t slot_count =
        static_cast<std::size_t>(slot_count_u64);
    const std::size_t barrier_count =
        static_cast<std::size_t>(barrier_count_u64);
    if (barrier_count >
            (wire.size() - kLatestStateCheckpointHeaderBytesV1) /
                kBarrierBytes ||
        slot_count >
            (wire.size() - kLatestStateCheckpointHeaderBytesV1 -
             barrier_count * kBarrierBytes) /
                kEntryBytes ||
        kLatestStateCheckpointHeaderBytesV1 +
                barrier_count * kBarrierBytes +
                slot_count * kEntryBytes !=
            wire.size()) {
        return LatestStateCheckpointErrorV1::kCorruptWire;
    }

    LatestStateCheckpointV1 value;
    value.config.state_generation = LoadScalar<std::uint64_t>(
        wire, header_offset::kStateGeneration);
    value.config.state_writer_instance = LoadBytes<16U>(
        wire, header_offset::kStateWriterInstance);
    value.config.shard_id = LoadScalar<std::uint32_t>(
        wire, header_offset::kShardId);
    value.config.shard_count = LoadScalar<std::uint32_t>(
        wire, header_offset::kShardCount);
    value.config.registry_version = LoadScalar<std::uint64_t>(
        wire, header_offset::kRegistryVersion);
    value.config.canonical_schema_sha256 = LoadBytes<32U>(
        wire, header_offset::kCanonicalSchemaSha256);
    value.config.canonical_dtype_sha256 = LoadBytes<32U>(
        wire, header_offset::kCanonicalDtypeSha256);
    value.config.registry_sha256 = LoadBytes<32U>(
        wire, header_offset::kRegistrySha256);
    value.writer_quiesced = (flags & kWriterQuiescedFlag) != 0U;
    value.durability_barrier_satisfied =
        (flags & kDurabilityBarrierSatisfiedFlag) != 0U;
    value.logical_state_sha256 = LoadBytes<32U>(
        wire, header_offset::kLogicalStateSha256);
    value.payload_sha256 = LoadBytes<32U>(
        wire, header_offset::kPayloadSha256);
    if (!LatestStateConfigValidV1(value.config) ||
        LoadBytes<32U>(wire, header_offset::kLatestStateSchemaSha256) !=
            LatestStateSchemaDescriptorSha256V1()) {
        return LatestStateCheckpointErrorV1::kIdentityMismatch;
    }
    const common::Sha256Digest actual_payload_sha = common::ComputeSha256(
        wire.subspan(kLatestStateCheckpointHeaderBytesV1));
    if (actual_payload_sha != value.payload_sha256) {
        return LatestStateCheckpointErrorV1::kHashMismatch;
    }

    try {
        value.durability_barriers.resize(barrier_count);
        value.entries.resize(slot_count);
    } catch (const std::bad_alloc&) {
        return LatestStateCheckpointErrorV1::kAllocationFailed;
    }
    std::size_t cursor = kLatestStateCheckpointHeaderBytesV1;
    for (std::size_t index = 0U; index < barrier_count; ++index) {
        const auto encoded = wire.subspan(cursor, kBarrierBytes);
        if (LoadScalar<std::uint16_t>(
                encoded, barrier_offset::kReserved16) != 0U ||
            LoadScalar<std::uint32_t>(
                encoded, barrier_offset::kReserved32) != 0U) {
            return LatestStateCheckpointErrorV1::kCorruptWire;
        }
        value.durability_barriers[index] = DecodeBarrier(encoded);
        cursor += kBarrierBytes;
    }
    std::vector<LatestStateValueV1> values;
    try {
        values.resize(slot_count);
    } catch (const std::bad_alloc&) {
        return LatestStateCheckpointErrorV1::kAllocationFailed;
    }
    for (std::size_t index = 0U; index < slot_count; ++index) {
        LatestStateCheckpointEntryV1& entry = value.entries[index];
        entry.instrument_id = LoadScalar<std::uint32_t>(wire, cursor);
        const std::uint32_t initialized =
            LoadScalar<std::uint32_t>(wire, cursor + 4U);
        if (initialized > 1U ||
            (index != 0U &&
             value.entries[index - 1U].instrument_id >=
                 entry.instrument_id)) {
            return LatestStateCheckpointErrorV1::kCorruptWire;
        }
        entry.initialized = initialized == 1U;
        std::memcpy(
            entry.normalized_slot.bytes.data(),
            wire.data() + static_cast<std::ptrdiff_t>(cursor + 8U),
            entry.normalized_slot.bytes.size());
        const LatestStateCheckpointErrorV1 entry_error =
            ValidateNormalizedEntry(value.config, entry, &values[index]);
        if (entry_error != LatestStateCheckpointErrorV1::kNone) {
            return entry_error;
        }
        cursor += kEntryBytes;
    }
    common::Sha256Digest actual_logical{};
    LatestStateCheckpointErrorV1 error =
        HashEntries(value.config, value.entries, &actual_logical);
    if (error != LatestStateCheckpointErrorV1::kNone) {
        return error;
    }
    if (actual_logical != value.logical_state_sha256) {
        return LatestStateCheckpointErrorV1::kHashMismatch;
    }

    std::vector<LatestStateDurabilityBarrierV1> requirements;
    error = BuildRequirements(
        value.config, values, value.entries, &requirements);
    if (error != LatestStateCheckpointErrorV1::kNone) {
        return error;
    }
    std::vector<LatestStateDurabilityBarrierV1> sorted;
    error = ValidateAndSortBarriers(
        value.config, value.writer_quiesced,
        value.durability_barrier_satisfied,
        value.durability_barriers, requirements, &sorted);
    if (error != LatestStateCheckpointErrorV1::kNone ||
        sorted != value.durability_barriers) {
        return error == LatestStateCheckpointErrorV1::kNone
                   ? LatestStateCheckpointErrorV1::kCorruptWire
                   : error;
    }

    *checkpoint = std::move(value);
    return LatestStateCheckpointErrorV1::kNone;
}

LatestStateCheckpointErrorV1 RestoreLatestStateCheckpointV1(
    const LatestStateCheckpointV1& checkpoint,
    const LatestStateConfigV1& expected_config,
    std::span<const LatestStateCheckpointTargetV1> targets) {
    if (!LatestStateHostSupportedV1()) {
        return LatestStateCheckpointErrorV1::kUnsupportedHost;
    }
    if (!LatestStateConfigValidV1(expected_config) ||
        checkpoint.config != expected_config) {
        return LatestStateCheckpointErrorV1::kIdentityMismatch;
    }
    if (checkpoint.durability_barrier_satisfied &&
        !checkpoint.writer_quiesced) {
        return LatestStateCheckpointErrorV1::kInvalidDurabilityProof;
    }
    if (targets.size() != checkpoint.entries.size()) {
        return LatestStateCheckpointErrorV1::kInvalidInput;
    }

    std::vector<LatestStateCheckpointTargetV1> sorted_targets;
    std::vector<LatestStateValueV1> values;
    try {
        sorted_targets.assign(targets.begin(), targets.end());
        values.resize(checkpoint.entries.size());
        std::sort(
            sorted_targets.begin(), sorted_targets.end(),
            [](const LatestStateCheckpointTargetV1& left,
               const LatestStateCheckpointTargetV1& right) noexcept {
                return left.instrument_id < right.instrument_id;
            });
    } catch (const std::bad_alloc&) {
        return LatestStateCheckpointErrorV1::kAllocationFailed;
    }

    for (std::size_t index = 0U; index < sorted_targets.size(); ++index) {
        if (sorted_targets[index].slot == nullptr ||
            sorted_targets[index].instrument_id !=
                checkpoint.entries[index].instrument_id ||
            (index != 0U &&
             checkpoint.entries[index - 1U].instrument_id >=
                 checkpoint.entries[index].instrument_id) ||
            (index != 0U &&
             sorted_targets[index - 1U].instrument_id ==
                 sorted_targets[index].instrument_id)) {
            return LatestStateCheckpointErrorV1::kInvalidInput;
        }
        const LatestStateCheckpointErrorV1 validation_error =
            ValidateNormalizedEntry(
                checkpoint.config, checkpoint.entries[index],
                &values[index]);
        if (validation_error != LatestStateCheckpointErrorV1::kNone) {
            return validation_error;
        }
        LatestStateSlotV1 current;
        const std::uint32_t copy_result =
            l2flow_latest_state_copy_stable_v1(
                sorted_targets[index].slot->bytes.data(),
                sorted_targets[index].slot->bytes.size(),
                current.bytes.data(), current.bytes.size(),
                kStableReadAttempts);
        if (copy_result != 0U || !BytesAreZero(current.bytes)) {
            return LatestStateCheckpointErrorV1::kTargetNotEmpty;
        }
    }
    common::Sha256Digest logical{};
    LatestStateCheckpointErrorV1 hash_error =
        HashEntries(checkpoint.config, checkpoint.entries, &logical);
    if (hash_error != LatestStateCheckpointErrorV1::kNone) {
        return hash_error;
    }
    if (logical != checkpoint.logical_state_sha256) {
        return LatestStateCheckpointErrorV1::kHashMismatch;
    }
    std::vector<LatestStateDurabilityBarrierV1> requirements;
    LatestStateCheckpointErrorV1 barrier_error = BuildRequirements(
        checkpoint.config, values, checkpoint.entries, &requirements);
    if (barrier_error != LatestStateCheckpointErrorV1::kNone) {
        return barrier_error;
    }
    std::vector<LatestStateDurabilityBarrierV1> sorted_barriers;
    barrier_error = ValidateAndSortBarriers(
        checkpoint.config, checkpoint.writer_quiesced,
        checkpoint.durability_barrier_satisfied,
        checkpoint.durability_barriers, requirements, &sorted_barriers);
    if (barrier_error != LatestStateCheckpointErrorV1::kNone ||
        sorted_barriers != checkpoint.durability_barriers) {
        return barrier_error == LatestStateCheckpointErrorV1::kNone
                   ? LatestStateCheckpointErrorV1::kInvalidDurabilityProof
                   : barrier_error;
    }

    std::size_t acquired = 0U;
    for (; acquired < sorted_targets.size(); ++acquired) {
        std::uint64_t expected = 0U;
        std::uint64_t* const words =
            SharedSlotWords(sorted_targets[acquired].slot);
        if (!__atomic_compare_exchange_n(
                &words[0], &expected, 1U, false,
                __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE)) {
            for (std::size_t rollback = 0U; rollback < acquired; ++rollback) {
                std::uint64_t* const rollback_words =
                    SharedSlotWords(sorted_targets[rollback].slot);
                __atomic_store_n(
                    &rollback_words[0], 0U, __ATOMIC_RELEASE);
            }
            return LatestStateCheckpointErrorV1::kWriterBusy;
        }
    }

    for (std::size_t index = 0U; index < sorted_targets.size(); ++index) {
        std::uint64_t* const target_words =
            SharedSlotWords(sorted_targets[index].slot);
        const LatestStateSlotV1& source =
            checkpoint.entries[index].normalized_slot;
        for (std::size_t word = 1U; word < kSlotWords; ++word) {
            __atomic_store_n(
                &target_words[word], LoadSlotWord(source, word),
                __ATOMIC_RELAXED);
        }
    }
    // Do not make any restored slot readable until every payload is present.
    // The caller still owns the table-generation switch across shards/files.
    for (std::size_t index = 0U; index < sorted_targets.size(); ++index) {
        std::uint64_t* const target_words =
            SharedSlotWords(sorted_targets[index].slot);
        const std::uint64_t sequence =
            checkpoint.entries[index].initialized ? 2U : 0U;
        __atomic_store_n(&target_words[0], sequence, __ATOMIC_RELEASE);
    }
    return LatestStateCheckpointErrorV1::kNone;
}

}  // namespace l2flow::state
