#include "l2flow/state/latest_state_v1.h"

#include "l2flow/control/quality_flags_v1.h"

#include <algorithm>
#include <bit>
#include <cstring>
#include <limits>
#include <new>
#include <utility>
#include <vector>

namespace {

namespace canonical = l2flow::canonical;
namespace common = l2flow::common;
namespace control = l2flow::control;
namespace state = l2flow::state;

constexpr std::size_t kWordBytes = sizeof(std::uint64_t);
constexpr std::size_t kSlotWords =
    state::kLatestStateSlotBytesV1 / kWordBytes;
constexpr std::uint32_t kStableReadAttempts = 4096U;

namespace slot_offset {
constexpr std::size_t kSequence = 0U;
constexpr std::size_t kMagic = 8U;
constexpr std::size_t kSlotSize = 12U;
constexpr std::size_t kSchemaVersion = 16U;
constexpr std::size_t kReserved16 = 18U;
constexpr std::size_t kInstrumentId = 20U;
constexpr std::size_t kStateGeneration = 24U;
constexpr std::size_t kShardId = 32U;
constexpr std::size_t kShardCount = 36U;
constexpr std::size_t kTickQualityFlags = 40U;
constexpr std::size_t kCaptureDate = 48U;
constexpr std::size_t kSourceStreamId = 52U;
constexpr std::size_t kSourceGeneration = 56U;
constexpr std::size_t kCanonicalGeneration = 64U;
constexpr std::size_t kRegistryVersion = 72U;
constexpr std::size_t kStreamDayId = 80U;
constexpr std::size_t kSourceWriterInstance = 96U;
constexpr std::size_t kClockAlgorithm = 112U;
constexpr std::size_t kReserved32 = 116U;
constexpr std::size_t kClockLabel = 120U;
constexpr std::size_t kClockDigest = 128U;
constexpr std::size_t kCanonicalSchemaSha256 = 160U;
constexpr std::size_t kCanonicalDtypeSha256 = 192U;
constexpr std::size_t kRegistrySha256 = 224U;
constexpr std::size_t kSnapshotRecord = 256U;
constexpr std::size_t kTickCaptureDate = 2304U;
constexpr std::size_t kTickSourceStreamId = 2308U;
constexpr std::size_t kTickSourceGeneration = 2312U;
constexpr std::size_t kTickCanonicalGeneration = 2320U;
constexpr std::size_t kTickShardEventId = 2328U;
constexpr std::size_t kTickOriginIngressSequence = 2336U;
constexpr std::size_t kTickOriginWalEndPos = 2344U;
constexpr std::size_t kTickRecvMonotonicNs = 2352U;
constexpr std::size_t kTickStreamDayId = 2360U;
constexpr std::size_t kTickSourceWriterInstance = 2376U;
constexpr std::size_t kTickClockAlgorithm = 2392U;
constexpr std::size_t kTickInitialized = 2396U;
constexpr std::size_t kTickClockLabel = 2400U;
constexpr std::size_t kTickClockDigest = 2408U;
constexpr std::size_t kTickCanonicalSchemaSha256 = 2440U;
constexpr std::size_t kTickCanonicalDtypeSha256 = 2472U;
constexpr std::size_t kTickRegistryVersion = 2504U;
constexpr std::size_t kTickRegistrySha256 = 2512U;
constexpr std::size_t kTickEnd = 2544U;
constexpr std::size_t kStateWriterInstance = 2544U;
constexpr std::size_t kReservedTail = 2560U;
}  // namespace slot_offset

static_assert(kSlotWords == 512U);
static_assert(slot_offset::kSnapshotRecord +
                  sizeof(canonical::CanonicalSnapshotRecordV1) ==
              slot_offset::kTickCaptureDate);

constexpr std::string_view kLatestStateSchemaDescriptor =
    "L2FLOW_LATEST_STATE_SLOT_V1\n"
    "byte_order=little\n"
    "size=4096\n"
    "alignment=64\n"
    "0:u64:seqlock\n"
    "8:u32:magic\n"
    "12:u32:slot_size\n"
    "16:u16:schema_version\n"
    "18:u16:reserved_zero\n"
    "20:u32:instrument_id\n"
    "24:u64:state_generation\n"
    "32:u32:shard_id\n"
    "36:u32:shard_count\n"
    "40:u64:tick_quality_flags\n"
    "48:u32:origin_capture_date\n"
    "52:u32:source_stream_id\n"
    "56:u64:origin_source_generation\n"
    "64:u64:canonical_generation\n"
    "72:u64:registry_version\n"
    "80:bytes16:origin_stream_day_id\n"
    "96:bytes16:origin_source_writer_instance\n"
    "112:u32:clock_epoch_algorithm\n"
    "116:u32:reserved_zero\n"
    "120:u64:clock_epoch_label\n"
    "128:bytes32:clock_epoch_digest\n"
    "160:bytes32:canonical_schema_sha256\n"
    "192:bytes32:canonical_dtype_sha256\n"
    "224:bytes32:registry_sha256\n"
    "256:bytes2048:canonical_snapshot_record_v1\n"
    "2304:u32:tick_origin_capture_date\n"
    "2308:u32:tick_source_stream_id\n"
    "2312:u64:tick_source_generation\n"
    "2320:u64:tick_canonical_generation\n"
    "2328:u64:tick_shard_event_id\n"
    "2336:u64:tick_origin_ingress_sequence\n"
    "2344:u64:tick_origin_wal_end_pos\n"
    "2352:i64:tick_recv_monotonic_ns\n"
    "2360:bytes16:tick_origin_stream_day_id\n"
    "2376:bytes16:tick_origin_source_writer_instance\n"
    "2392:u32:tick_clock_epoch_algorithm\n"
    "2396:u32:tick_initialized\n"
    "2400:u64:tick_clock_epoch_label\n"
    "2408:bytes32:tick_clock_epoch_digest\n"
    "2440:bytes32:tick_canonical_schema_sha256\n"
    "2472:bytes32:tick_canonical_dtype_sha256\n"
    "2504:u64:tick_registry_version\n"
    "2512:bytes32:tick_registry_sha256\n"
    "2544:bytes16:state_writer_instance\n"
    "2560:bytes1536:reserved_zero\n";

template <typename T>
void StoreScalar(
    state::LatestStateSlotV1* image,
    std::size_t offset,
    T value) noexcept {
    static_assert(std::is_integral_v<T>);
    std::memcpy(
        image->bytes.data() + static_cast<std::ptrdiff_t>(offset),
        &value,
        sizeof(value));
}

template <typename T>
T LoadScalar(
    const state::LatestStateSlotV1& image,
    std::size_t offset) noexcept {
    static_assert(std::is_integral_v<T>);
    T value{};
    std::memcpy(
        &value,
        image.bytes.data() + static_cast<std::ptrdiff_t>(offset),
        sizeof(value));
    return value;
}

template <std::size_t Size>
void StoreBytes(
    state::LatestStateSlotV1* image,
    std::size_t offset,
    const std::array<std::byte, Size>& value) noexcept {
    std::memcpy(
        image->bytes.data() + static_cast<std::ptrdiff_t>(offset),
        value.data(),
        Size);
}

template <std::size_t Size>
std::array<std::byte, Size> LoadBytes(
    const state::LatestStateSlotV1& image,
    std::size_t offset) noexcept {
    std::array<std::byte, Size> value{};
    std::memcpy(
        value.data(),
        image.bytes.data() + static_cast<std::ptrdiff_t>(offset),
        Size);
    return value;
}

template <std::size_t Size>
bool IsAllZero(const std::array<std::byte, Size>& value) noexcept {
    return std::all_of(
        value.begin(), value.end(),
        [](std::byte byte) noexcept { return byte == std::byte{0U}; });
}

bool RangeIsZero(
    const state::LatestStateSlotV1& image,
    std::size_t offset,
    std::size_t size) noexcept {
    return std::all_of(
        image.bytes.begin() + static_cast<std::ptrdiff_t>(offset),
        image.bytes.begin() + static_cast<std::ptrdiff_t>(offset + size),
        [](std::byte byte) noexcept { return byte == std::byte{0U}; });
}

bool WholeImageIsZero(const state::LatestStateSlotV1& image) noexcept {
    return RangeIsZero(image, 0U, image.bytes.size());
}

bool ClockIdentityExact(
    const canonical::ClockEpochIdentityV1& left,
    const canonical::ClockEpochIdentityV1& right) noexcept {
    return left.algorithm == right.algorithm && left.digest == right.digest;
}

bool SameImmutableOrigin(
    const state::LatestStateSnapshotOriginV1& left,
    const state::LatestStateSnapshotOriginV1& right) noexcept {
    return left.capture_date == right.capture_date &&
           left.source_stream_id == right.source_stream_id &&
           left.stream_day_id == right.stream_day_id &&
           left.source_writer_instance == right.source_writer_instance &&
           left.source_generation == right.source_generation &&
           left.canonical_generation == right.canonical_generation &&
           ClockIdentityExact(left.clock_epoch, right.clock_epoch) &&
           left.canonical_schema_sha256 ==
               right.canonical_schema_sha256 &&
           left.canonical_dtype_sha256 ==
               right.canonical_dtype_sha256 &&
           left.registry_version == right.registry_version &&
           left.registry_sha256 == right.registry_sha256;
}

bool ConfigMatchesOrigin(
    const state::LatestStateConfigV1& config,
    const state::LatestStateSnapshotOriginV1& origin) noexcept {
    return config.canonical_schema_sha256 ==
               origin.canonical_schema_sha256 &&
           config.canonical_dtype_sha256 ==
               origin.canonical_dtype_sha256 &&
           config.registry_version == origin.registry_version &&
           config.registry_sha256 == origin.registry_sha256;
}

bool SnapshotBytesEqual(
    const canonical::CanonicalSnapshotRecordV1& left,
    const canonical::CanonicalSnapshotRecordV1& right) noexcept {
    return std::memcmp(&left, &right, sizeof(left)) == 0;
}

std::uint64_t ReadLocalWord(
    const state::LatestStateSlotV1& image,
    std::size_t word_index) noexcept {
    std::uint64_t value = 0U;
    std::memcpy(
        &value,
        image.bytes.data() +
            static_cast<std::ptrdiff_t>(word_index * kWordBytes),
        kWordBytes);
    return value;
}

std::uint64_t* SharedWords(state::LatestStateSlotV1* slot) noexcept {
    return reinterpret_cast<std::uint64_t*>(slot->bytes.data());
}

state::LatestStateErrorV1 CopyStableImage(
    const state::LatestStateSlotV1& slot,
    state::LatestStateSlotV1* image) noexcept {
    const std::uint32_t result = l2flow_latest_state_copy_stable_v1(
        slot.bytes.data(), slot.bytes.size(),
        image->bytes.data(), image->bytes.size(), kStableReadAttempts);
    switch (result) {
        case 0U:
            return state::LatestStateErrorV1::kNone;
        case 1U:
            return state::LatestStateErrorV1::kUnsupportedHost;
        case 2U:
            return state::LatestStateErrorV1::kReadBusy;
        default:
            return state::LatestStateErrorV1::kCorruptSlot;
    }
}

// Publishes a fully encoded local image only if the stable sequence observed by
// the caller still owns the slot.  Every shared payload word is atomic; no
// concurrent reader performs a non-atomic memcpy from shared storage.
state::LatestStateErrorV1 PublishEncodedImage(
    state::LatestStateSlotV1* slot,
    std::uint64_t expected_sequence,
    const state::LatestStateSlotV1& desired) noexcept {
    if ((expected_sequence & 1U) != 0U ||
        expected_sequence >
            std::numeric_limits<std::uint64_t>::max() - 2U) {
        return state::LatestStateErrorV1::kSequenceOverflow;
    }

    std::uint64_t expected = expected_sequence;
    const std::uint64_t odd = expected_sequence + 1U;
    std::uint64_t* const words = SharedWords(slot);
    if (!__atomic_compare_exchange_n(
            &words[0], &expected, odd, false,
            __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE)) {
        return state::LatestStateErrorV1::kWriterBusy;
    }

    for (std::size_t index = 1U; index < kSlotWords; ++index) {
        const std::uint64_t value = ReadLocalWord(desired, index);
        __atomic_store_n(&words[index], value, __ATOMIC_RELAXED);
    }
    __atomic_store_n(&words[0], expected_sequence + 2U, __ATOMIC_RELEASE);
    return state::LatestStateErrorV1::kNone;
}

void EncodeValue(
    const state::LatestStateValueV1& value,
    state::LatestStateSlotV1* image) noexcept {
    *image = {};
    StoreScalar<std::uint64_t>(
        image, slot_offset::kSequence, value.slot_sequence);
    StoreScalar<std::uint32_t>(
        image, slot_offset::kMagic, state::kLatestStateSlotMagicV1);
    StoreScalar<std::uint32_t>(
        image, slot_offset::kSlotSize,
        static_cast<std::uint32_t>(state::kLatestStateSlotBytesV1));
    StoreScalar<std::uint16_t>(
        image, slot_offset::kSchemaVersion,
        state::kLatestStateSlotSchemaVersionV1);
    StoreScalar<std::uint32_t>(
        image, slot_offset::kInstrumentId,
        value.snapshot.header.instrument_id);
    StoreScalar<std::uint64_t>(
        image, slot_offset::kStateGeneration,
        value.config.state_generation);
    StoreBytes(
        image, slot_offset::kStateWriterInstance,
        value.config.state_writer_instance);
    StoreScalar<std::uint32_t>(
        image, slot_offset::kShardId, value.config.shard_id);
    StoreScalar<std::uint32_t>(
        image, slot_offset::kShardCount, value.config.shard_count);
    StoreScalar<std::uint64_t>(
        image, slot_offset::kTickQualityFlags,
        value.tick_quality_flags);
    StoreScalar<std::uint32_t>(
        image, slot_offset::kCaptureDate, value.origin.capture_date);
    StoreScalar<std::uint32_t>(
        image, slot_offset::kSourceStreamId,
        value.origin.source_stream_id);
    StoreScalar<std::uint64_t>(
        image, slot_offset::kSourceGeneration,
        value.origin.source_generation);
    StoreScalar<std::uint64_t>(
        image, slot_offset::kCanonicalGeneration,
        value.origin.canonical_generation);
    StoreScalar<std::uint64_t>(
        image, slot_offset::kRegistryVersion,
        value.config.registry_version);
    StoreBytes(
        image, slot_offset::kStreamDayId,
        value.origin.stream_day_id);
    StoreBytes(
        image, slot_offset::kSourceWriterInstance,
        value.origin.source_writer_instance);
    StoreScalar<std::uint32_t>(
        image, slot_offset::kClockAlgorithm,
        value.origin.clock_epoch.algorithm);
    StoreScalar<std::uint64_t>(
        image, slot_offset::kClockLabel,
        value.origin.clock_epoch.label);
    StoreBytes(
        image, slot_offset::kClockDigest,
        value.origin.clock_epoch.digest);
    StoreBytes(
        image, slot_offset::kCanonicalSchemaSha256,
        value.config.canonical_schema_sha256);
    StoreBytes(
        image, slot_offset::kCanonicalDtypeSha256,
        value.config.canonical_dtype_sha256);
    StoreBytes(
        image, slot_offset::kRegistrySha256,
        value.config.registry_sha256);
    std::memcpy(
        image->bytes.data() +
            static_cast<std::ptrdiff_t>(slot_offset::kSnapshotRecord),
        &value.snapshot,
        sizeof(value.snapshot));
    if (value.tick_quality_initialized) {
        StoreScalar<std::uint32_t>(
            image, slot_offset::kTickCaptureDate,
            value.tick_origin.capture_date);
        StoreScalar<std::uint32_t>(
            image, slot_offset::kTickSourceStreamId,
            value.tick_origin.source_stream_id);
        StoreScalar<std::uint64_t>(
            image, slot_offset::kTickSourceGeneration,
            value.tick_origin.source_generation);
        StoreScalar<std::uint64_t>(
            image, slot_offset::kTickCanonicalGeneration,
            value.tick_origin.canonical_generation);
        StoreScalar<std::uint64_t>(
            image, slot_offset::kTickShardEventId,
            value.tick_shard_event_id);
        StoreScalar<std::uint64_t>(
            image, slot_offset::kTickOriginIngressSequence,
            value.tick_origin_ingress_sequence);
        StoreScalar<std::uint64_t>(
            image, slot_offset::kTickOriginWalEndPos,
            value.tick_origin_wal_end_pos);
        StoreScalar<std::int64_t>(
            image, slot_offset::kTickRecvMonotonicNs,
            value.tick_recv_monotonic_ns);
        StoreBytes(
            image, slot_offset::kTickStreamDayId,
            value.tick_origin.stream_day_id);
        StoreBytes(
            image, slot_offset::kTickSourceWriterInstance,
            value.tick_origin.source_writer_instance);
        StoreScalar<std::uint32_t>(
            image, slot_offset::kTickClockAlgorithm,
            value.tick_origin.clock_epoch.algorithm);
        StoreScalar<std::uint32_t>(
            image, slot_offset::kTickInitialized, 1U);
        StoreScalar<std::uint64_t>(
            image, slot_offset::kTickClockLabel,
            value.tick_origin.clock_epoch.label);
        StoreBytes(
            image, slot_offset::kTickClockDigest,
            value.tick_origin.clock_epoch.digest);
        StoreBytes(
            image, slot_offset::kTickCanonicalSchemaSha256,
            value.tick_origin.canonical_schema_sha256);
        StoreBytes(
            image, slot_offset::kTickCanonicalDtypeSha256,
            value.tick_origin.canonical_dtype_sha256);
        StoreScalar<std::uint64_t>(
            image, slot_offset::kTickRegistryVersion,
            value.tick_origin.registry_version);
        StoreBytes(
            image, slot_offset::kTickRegistrySha256,
            value.tick_origin.registry_sha256);
    }
}

state::LatestStateErrorV1 DecodeImage(
    const state::LatestStateSlotV1& image,
    state::LatestStateValueV1* output) noexcept {
    if (WholeImageIsZero(image)) {
        return state::LatestStateErrorV1::kUninitialized;
    }

    const std::uint64_t sequence = LoadScalar<std::uint64_t>(
        image, slot_offset::kSequence);
    if (sequence == 0U || (sequence & 1U) != 0U ||
        LoadScalar<std::uint32_t>(image, slot_offset::kMagic) !=
            state::kLatestStateSlotMagicV1 ||
        LoadScalar<std::uint32_t>(image, slot_offset::kSlotSize) !=
            state::kLatestStateSlotBytesV1 ||
        LoadScalar<std::uint16_t>(image, slot_offset::kSchemaVersion) !=
            state::kLatestStateSlotSchemaVersionV1 ||
        LoadScalar<std::uint16_t>(image, slot_offset::kReserved16) != 0U ||
        LoadScalar<std::uint32_t>(image, slot_offset::kReserved32) != 0U ||
        !RangeIsZero(
            image, slot_offset::kReservedTail,
            state::kLatestStateSlotBytesV1 -
                slot_offset::kReservedTail)) {
        return state::LatestStateErrorV1::kCorruptSlot;
    }

    state::LatestStateValueV1 value;
    value.slot_sequence = sequence;
    value.config.state_generation = LoadScalar<std::uint64_t>(
        image, slot_offset::kStateGeneration);
    value.config.state_writer_instance = LoadBytes<16U>(
        image, slot_offset::kStateWriterInstance);
    value.config.shard_id = LoadScalar<std::uint32_t>(
        image, slot_offset::kShardId);
    value.config.shard_count = LoadScalar<std::uint32_t>(
        image, slot_offset::kShardCount);
    value.tick_quality_flags = LoadScalar<std::uint64_t>(
        image, slot_offset::kTickQualityFlags);
    value.origin.capture_date = LoadScalar<std::uint32_t>(
        image, slot_offset::kCaptureDate);
    value.origin.source_stream_id = LoadScalar<std::uint32_t>(
        image, slot_offset::kSourceStreamId);
    value.origin.source_generation = LoadScalar<std::uint64_t>(
        image, slot_offset::kSourceGeneration);
    value.origin.canonical_generation = LoadScalar<std::uint64_t>(
        image, slot_offset::kCanonicalGeneration);
    value.config.registry_version = LoadScalar<std::uint64_t>(
        image, slot_offset::kRegistryVersion);
    value.origin.stream_day_id = LoadBytes<16U>(
        image, slot_offset::kStreamDayId);
    value.origin.source_writer_instance = LoadBytes<16U>(
        image, slot_offset::kSourceWriterInstance);
    value.origin.clock_epoch.algorithm = LoadScalar<std::uint32_t>(
        image, slot_offset::kClockAlgorithm);
    value.origin.clock_epoch.label = LoadScalar<std::uint64_t>(
        image, slot_offset::kClockLabel);
    value.origin.clock_epoch.digest = LoadBytes<32U>(
        image, slot_offset::kClockDigest);
    value.config.canonical_schema_sha256 = LoadBytes<32U>(
        image, slot_offset::kCanonicalSchemaSha256);
    value.config.canonical_dtype_sha256 = LoadBytes<32U>(
        image, slot_offset::kCanonicalDtypeSha256);
    value.config.registry_sha256 = LoadBytes<32U>(
        image, slot_offset::kRegistrySha256);
    value.origin.canonical_schema_sha256 =
        value.config.canonical_schema_sha256;
    value.origin.canonical_dtype_sha256 =
        value.config.canonical_dtype_sha256;
    value.origin.registry_version = value.config.registry_version;
    value.origin.registry_sha256 = value.config.registry_sha256;
    std::memcpy(
        &value.snapshot,
        image.bytes.data() +
            static_cast<std::ptrdiff_t>(slot_offset::kSnapshotRecord),
        sizeof(value.snapshot));

    const std::uint32_t tick_initialized = LoadScalar<std::uint32_t>(
        image, slot_offset::kTickInitialized);
    if (tick_initialized > 1U) {
        return state::LatestStateErrorV1::kCorruptSlot;
    }
    value.tick_quality_initialized = tick_initialized == 1U;
    if (value.tick_quality_initialized) {
        value.tick_origin.capture_date = LoadScalar<std::uint32_t>(
            image, slot_offset::kTickCaptureDate);
        value.tick_origin.source_stream_id = LoadScalar<std::uint32_t>(
            image, slot_offset::kTickSourceStreamId);
        value.tick_origin.source_generation = LoadScalar<std::uint64_t>(
            image, slot_offset::kTickSourceGeneration);
        value.tick_origin.canonical_generation = LoadScalar<std::uint64_t>(
            image, slot_offset::kTickCanonicalGeneration);
        value.tick_shard_event_id = LoadScalar<std::uint64_t>(
            image, slot_offset::kTickShardEventId);
        value.tick_origin_ingress_sequence = LoadScalar<std::uint64_t>(
            image, slot_offset::kTickOriginIngressSequence);
        value.tick_origin_wal_end_pos = LoadScalar<std::uint64_t>(
            image, slot_offset::kTickOriginWalEndPos);
        value.tick_recv_monotonic_ns = LoadScalar<std::int64_t>(
            image, slot_offset::kTickRecvMonotonicNs);
        value.tick_origin.stream_day_id = LoadBytes<16U>(
            image, slot_offset::kTickStreamDayId);
        value.tick_origin.source_writer_instance = LoadBytes<16U>(
            image, slot_offset::kTickSourceWriterInstance);
        value.tick_origin.clock_epoch.algorithm = LoadScalar<std::uint32_t>(
            image, slot_offset::kTickClockAlgorithm);
        value.tick_origin.clock_epoch.label = LoadScalar<std::uint64_t>(
            image, slot_offset::kTickClockLabel);
        value.tick_origin.clock_epoch.digest = LoadBytes<32U>(
            image, slot_offset::kTickClockDigest);
        value.tick_origin.canonical_schema_sha256 = LoadBytes<32U>(
            image, slot_offset::kTickCanonicalSchemaSha256);
        value.tick_origin.canonical_dtype_sha256 = LoadBytes<32U>(
            image, slot_offset::kTickCanonicalDtypeSha256);
        value.tick_origin.registry_version = LoadScalar<std::uint64_t>(
            image, slot_offset::kTickRegistryVersion);
        value.tick_origin.registry_sha256 = LoadBytes<32U>(
            image, slot_offset::kTickRegistrySha256);
    } else if (value.tick_quality_flags != 0U ||
               !RangeIsZero(
                   image, slot_offset::kTickCaptureDate,
                   slot_offset::kTickEnd -
                       slot_offset::kTickCaptureDate)) {
        return state::LatestStateErrorV1::kCorruptSlot;
    }

    if (!state::LatestStateConfigValidV1(value.config) ||
        !state::LatestStateOriginValidV1(value.origin) ||
        !ConfigMatchesOrigin(value.config, value.origin) ||
        (value.tick_quality_flags &
         ~canonical::kCanonicalQualityFlagsMaskV1) != 0U ||
        canonical::ValidateCanonicalSnapshotRecordV1(value.snapshot) !=
            canonical::CanonicalValidationErrorV1::kNone ||
        value.snapshot.header.instrument_id !=
            LoadScalar<std::uint32_t>(
                image, slot_offset::kInstrumentId) ||
        value.snapshot.header.source_stream_id !=
            value.origin.source_stream_id ||
        value.snapshot.header.instrument_id % value.config.shard_count !=
            value.config.shard_id ||
        (value.tick_quality_initialized &&
         (!state::LatestStateOriginValidV1(value.tick_origin) ||
          !ConfigMatchesOrigin(value.config, value.tick_origin) ||
          value.tick_shard_event_id == 0U ||
          value.tick_origin_ingress_sequence == 0U ||
          value.tick_origin_wal_end_pos == 0U ||
          value.tick_recv_monotonic_ns < 0))) {
        return state::LatestStateErrorV1::kCorruptSlot;
    }

    *output = value;
    return state::LatestStateErrorV1::kNone;
}

state::LatestStateErrorV1 ValidateReadOptions(
    const state::LatestStateReadOptionsV1& options) noexcept {
    if (!canonical::ClockEpochIdentityV1Valid(options.now_clock_epoch)) {
        return state::LatestStateErrorV1::kClockEpochMismatch;
    }
    for (const std::int64_t threshold :
         options.stale_policy.max_age_ns_by_phase) {
        if (threshold < 0) {
            return state::LatestStateErrorV1::kInvalidStalePolicy;
        }
    }
    return state::LatestStateErrorV1::kNone;
}

}  // namespace

extern "C" bool l2flow_latest_state_atomic_u64_lock_free_v1() noexcept {
    alignas(64) std::uint64_t probe = 0U;
    return __atomic_always_lock_free(sizeof(probe), nullptr) &&
           __atomic_is_lock_free(sizeof(probe), &probe);
}

extern "C" std::uint32_t l2flow_latest_state_copy_stable_v1(
    const void* slot_bytes,
    std::size_t slot_size,
    void* output_bytes,
    std::size_t output_size,
    std::uint32_t max_attempts) noexcept {
    const std::uintptr_t source_address =
        reinterpret_cast<std::uintptr_t>(slot_bytes);
    const std::uintptr_t output_address =
        reinterpret_cast<std::uintptr_t>(output_bytes);
    const bool address_overflow =
        source_address >
            std::numeric_limits<std::uintptr_t>::max() - slot_size ||
        output_address >
            std::numeric_limits<std::uintptr_t>::max() - output_size;
    const bool overlaps = !address_overflow &&
        source_address < output_address + output_size &&
        output_address < source_address + slot_size;
    if (slot_bytes == nullptr || output_bytes == nullptr ||
        slot_size != state::kLatestStateSlotBytesV1 ||
        output_size != state::kLatestStateSlotBytesV1 ||
        max_attempts == 0U ||
        address_overflow || overlaps ||
        (source_address %
         state::kLatestStateSlotAlignmentV1) != 0U ||
        std::endian::native != std::endian::little ||
        !l2flow_latest_state_atomic_u64_lock_free_v1()) {
        return 1U;
    }

    const auto* const source =
        reinterpret_cast<const std::uint64_t*>(slot_bytes);
    auto* const output = static_cast<std::byte*>(output_bytes);
    for (std::uint32_t attempt = 0U; attempt < max_attempts; ++attempt) {
        const std::uint64_t first =
            __atomic_load_n(&source[0], __ATOMIC_ACQUIRE);
        if ((first & 1U) != 0U) {
            continue;
        }
        for (std::size_t index = 1U; index < kSlotWords; ++index) {
            const std::uint64_t value =
                __atomic_load_n(&source[index], __ATOMIC_RELAXED);
            std::memcpy(
                output + static_cast<std::ptrdiff_t>(index * kWordBytes),
                &value,
                kWordBytes);
        }
        const std::uint64_t second =
            __atomic_load_n(&source[0], __ATOMIC_SEQ_CST);
        if (first == second && (second & 1U) == 0U) {
            std::memcpy(output, &second, kWordBytes);
            return 0U;
        }
    }
    return 2U;
}

namespace l2flow::state {

std::string_view LatestStateErrorNameV1(LatestStateErrorV1 error) noexcept {
    switch (error) {
        case LatestStateErrorV1::kNone:
            return "none";
        case LatestStateErrorV1::kNullArgument:
            return "null_argument";
        case LatestStateErrorV1::kUnsupportedHost:
            return "unsupported_host";
        case LatestStateErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case LatestStateErrorV1::kInvalidOrigin:
            return "invalid_origin";
        case LatestStateErrorV1::kInvalidRecord:
            return "invalid_record";
        case LatestStateErrorV1::kInvalidQualityFlags:
            return "invalid_quality_flags";
        case LatestStateErrorV1::kUninitialized:
            return "uninitialized";
        case LatestStateErrorV1::kCorruptSlot:
            return "corrupt_slot";
        case LatestStateErrorV1::kIdentityMismatch:
            return "identity_mismatch";
        case LatestStateErrorV1::kInstrumentMismatch:
            return "instrument_mismatch";
        case LatestStateErrorV1::kStaleCursor:
            return "stale_cursor";
        case LatestStateErrorV1::kConflictingDuplicate:
            return "conflicting_duplicate";
        case LatestStateErrorV1::kWriterBusy:
            return "writer_busy";
        case LatestStateErrorV1::kSequenceOverflow:
            return "sequence_overflow";
        case LatestStateErrorV1::kReadBusy:
            return "read_busy";
        case LatestStateErrorV1::kClockEpochMismatch:
            return "clock_epoch_mismatch";
        case LatestStateErrorV1::kClockRegression:
            return "clock_regression";
        case LatestStateErrorV1::kInvalidStalePolicy:
            return "invalid_stale_policy";
        case LatestStateErrorV1::kNotFound:
            return "not_found";
        case LatestStateErrorV1::kOutputSizeMismatch:
            return "output_size_mismatch";
        case LatestStateErrorV1::kDuplicateInstrument:
            return "duplicate_instrument";
        case LatestStateErrorV1::kAllocationFailed:
            return "allocation_failed";
    }
    return "unknown";
}

bool LatestStateHostSupportedV1() noexcept {
    return std::endian::native == std::endian::little &&
           l2flow_latest_state_atomic_u64_lock_free_v1();
}

bool LatestStateConfigValidV1(
    const LatestStateConfigV1& config) noexcept {
    return config.state_generation != 0U &&
           !IsAllZero(config.state_writer_instance) &&
           config.shard_count != 0U &&
           config.shard_id < config.shard_count &&
           config.canonical_schema_sha256 ==
               canonical::CanonicalSchemaDescriptorSha256V1() &&
           config.canonical_dtype_sha256 ==
               canonical::CanonicalDtypeDescriptorSha256V1() &&
           config.registry_version != 0U &&
           !IsAllZero(config.registry_sha256);
}

bool LatestStateOriginValidV1(
    const LatestStateSnapshotOriginV1& origin) noexcept {
    return origin.capture_date != 0U &&
           origin.source_stream_id != 0U &&
           !IsAllZero(origin.stream_day_id) &&
           !IsAllZero(origin.source_writer_instance) &&
           origin.source_generation != 0U &&
           origin.canonical_generation != 0U &&
           canonical::ClockEpochIdentityV1Valid(origin.clock_epoch) &&
           origin.canonical_schema_sha256 ==
               canonical::CanonicalSchemaDescriptorSha256V1() &&
           origin.canonical_dtype_sha256 ==
               canonical::CanonicalDtypeDescriptorSha256V1() &&
           origin.registry_version != 0U &&
           !IsAllZero(origin.registry_sha256);
}

std::string_view LatestStateSchemaDescriptorV1() noexcept {
    return kLatestStateSchemaDescriptor;
}

common::Sha256Digest LatestStateSchemaDescriptorSha256V1() noexcept {
    return common::ComputeSha256(kLatestStateSchemaDescriptor);
}

LatestStateErrorV1 ReadLatestStateSlotV1(
    const LatestStateSlotV1& slot,
    LatestStateValueV1* output) noexcept {
    if (output == nullptr) {
        return LatestStateErrorV1::kNullArgument;
    }
    if (!LatestStateHostSupportedV1()) {
        return LatestStateErrorV1::kUnsupportedHost;
    }
    LatestStateSlotV1 image;
    const LatestStateErrorV1 copy_error = CopyStableImage(slot, &image);
    if (copy_error != LatestStateErrorV1::kNone) {
        return copy_error;
    }
    LatestStateValueV1 value;
    const LatestStateErrorV1 decode_error = DecodeImage(image, &value);
    if (decode_error != LatestStateErrorV1::kNone) {
        return decode_error;
    }
    *output = value;
    return LatestStateErrorV1::kNone;
}

LatestStateErrorV1 PublishLatestSnapshotV1(
    const LatestStateConfigV1& config,
    const LatestStateSnapshotOriginV1& origin,
    const canonical::CanonicalSnapshotRecordV1& snapshot,
    LatestStateSlotV1* slot,
    LatestStateUpdateDispositionV1* disposition) noexcept {
    if (slot == nullptr) {
        return LatestStateErrorV1::kNullArgument;
    }
    if (!LatestStateHostSupportedV1()) {
        return LatestStateErrorV1::kUnsupportedHost;
    }
    if (!LatestStateConfigValidV1(config)) {
        return LatestStateErrorV1::kInvalidConfiguration;
    }
    if (!LatestStateOriginValidV1(origin) ||
        !ConfigMatchesOrigin(config, origin) ||
        origin.source_stream_id != snapshot.header.source_stream_id) {
        return LatestStateErrorV1::kInvalidOrigin;
    }
    if (canonical::ValidateCanonicalSnapshotRecordV1(snapshot) !=
        canonical::CanonicalValidationErrorV1::kNone) {
        return LatestStateErrorV1::kInvalidRecord;
    }
    if (snapshot.header.instrument_id % config.shard_count !=
        config.shard_id) {
        return LatestStateErrorV1::kInstrumentMismatch;
    }

    LatestStateSlotV1 current_image;
    const LatestStateErrorV1 copy_error =
        CopyStableImage(*slot, &current_image);
    if (copy_error != LatestStateErrorV1::kNone) {
        return copy_error;
    }

    LatestStateValueV1 desired;
    std::uint64_t expected_sequence =
        LoadScalar<std::uint64_t>(current_image, slot_offset::kSequence);
    if (WholeImageIsZero(current_image)) {
        desired.slot_sequence = expected_sequence;
        desired.config = config;
        desired.origin = origin;
        desired.snapshot = snapshot;
    } else {
        LatestStateValueV1 current;
        const LatestStateErrorV1 decode_error =
            DecodeImage(current_image, &current);
        if (decode_error != LatestStateErrorV1::kNone) {
            return decode_error;
        }
        if (current.config != config ||
            !SameImmutableOrigin(current.origin, origin)) {
            return LatestStateErrorV1::kIdentityMismatch;
        }
        if (current.snapshot.header.instrument_id !=
            snapshot.header.instrument_id) {
            return LatestStateErrorV1::kInstrumentMismatch;
        }

        const bool same_cursors =
            current.snapshot.header.shard_event_id ==
                snapshot.header.shard_event_id &&
            current.snapshot.header.origin_ingress_sequence ==
                snapshot.header.origin_ingress_sequence &&
            current.snapshot.header.origin_wal_end_pos ==
                snapshot.header.origin_wal_end_pos;
        if (same_cursors) {
            if (!SnapshotBytesEqual(current.snapshot, snapshot)) {
                return LatestStateErrorV1::kConflictingDuplicate;
            }
            if (disposition != nullptr) {
                *disposition = LatestStateUpdateDispositionV1::kIdempotent;
            }
            return LatestStateErrorV1::kNone;
        }
        if (snapshot.header.shard_event_id <=
                current.snapshot.header.shard_event_id ||
            snapshot.header.origin_ingress_sequence <=
                current.snapshot.header.origin_ingress_sequence ||
            snapshot.header.origin_wal_end_pos <=
                current.snapshot.header.origin_wal_end_pos) {
            return LatestStateErrorV1::kStaleCursor;
        }
        desired = current;
        desired.origin = origin;
        desired.snapshot = snapshot;
        desired.slot_sequence = expected_sequence;
    }

    LatestStateSlotV1 desired_image;
    EncodeValue(desired, &desired_image);
    const LatestStateErrorV1 publish_error = PublishEncodedImage(
        slot, expected_sequence, desired_image);
    if (publish_error == LatestStateErrorV1::kNone &&
        disposition != nullptr) {
        *disposition = LatestStateUpdateDispositionV1::kPublished;
    }
    return publish_error;
}

LatestStateErrorV1 PublishLatestTickQualityV1(
    const LatestStateConfigV1& config,
    const LatestStateTickQualityUpdateV1& update,
    LatestStateSlotV1* slot) noexcept {
    if (slot == nullptr) {
        return LatestStateErrorV1::kNullArgument;
    }
    if ((update.quality_flags &
         ~canonical::kCanonicalQualityFlagsMaskV1) != 0U) {
        return LatestStateErrorV1::kInvalidQualityFlags;
    }
    if (!LatestStateConfigValidV1(config)) {
        return LatestStateErrorV1::kInvalidConfiguration;
    }
    if (update.instrument_id == 0U ||
        update.instrument_id % config.shard_count != config.shard_id) {
        return LatestStateErrorV1::kInstrumentMismatch;
    }
    if (!LatestStateOriginValidV1(update.origin) ||
        !ConfigMatchesOrigin(config, update.origin) ||
        update.shard_event_id == 0U ||
        update.origin_ingress_sequence == 0U ||
        update.origin_wal_end_pos == 0U ||
        update.recv_monotonic_ns < 0) {
        return LatestStateErrorV1::kInvalidOrigin;
    }

    LatestStateSlotV1 current_image;
    const LatestStateErrorV1 copy_error =
        CopyStableImage(*slot, &current_image);
    if (copy_error != LatestStateErrorV1::kNone) {
        return copy_error;
    }
    LatestStateValueV1 current;
    const LatestStateErrorV1 decode_error =
        DecodeImage(current_image, &current);
    if (decode_error != LatestStateErrorV1::kNone) {
        return decode_error;
    }
    if (current.config != config) {
        return LatestStateErrorV1::kIdentityMismatch;
    }
    if (current.snapshot.header.instrument_id != update.instrument_id) {
        return LatestStateErrorV1::kInstrumentMismatch;
    }
    if (current.tick_quality_initialized) {
        if (!SameImmutableOrigin(current.tick_origin, update.origin)) {
            return LatestStateErrorV1::kIdentityMismatch;
        }
        const bool same_cursors =
            current.tick_shard_event_id == update.shard_event_id &&
            current.tick_origin_ingress_sequence ==
                update.origin_ingress_sequence &&
            current.tick_origin_wal_end_pos == update.origin_wal_end_pos;
        if (same_cursors) {
            if (current.tick_recv_monotonic_ns !=
                    update.recv_monotonic_ns ||
                current.tick_quality_flags != update.quality_flags) {
                return LatestStateErrorV1::kConflictingDuplicate;
            }
            return LatestStateErrorV1::kNone;
        }
        if (update.shard_event_id <= current.tick_shard_event_id ||
            update.origin_ingress_sequence <=
                current.tick_origin_ingress_sequence ||
            update.origin_wal_end_pos <=
                current.tick_origin_wal_end_pos ||
            update.recv_monotonic_ns < current.tick_recv_monotonic_ns) {
            return LatestStateErrorV1::kStaleCursor;
        }
    }

    current.tick_quality_initialized = true;
    current.tick_origin = update.origin;
    current.tick_shard_event_id = update.shard_event_id;
    current.tick_origin_ingress_sequence =
        update.origin_ingress_sequence;
    current.tick_origin_wal_end_pos = update.origin_wal_end_pos;
    current.tick_recv_monotonic_ns = update.recv_monotonic_ns;
    current.tick_quality_flags = update.quality_flags;
    current.slot_sequence = LoadScalar<std::uint64_t>(
        current_image, slot_offset::kSequence);
    LatestStateSlotV1 desired_image;
    EncodeValue(current, &desired_image);
    return PublishEncodedImage(
        slot, current.slot_sequence, desired_image);
}

LatestStateErrorV1 ReadLatestStateV1(
    const LatestStateSlotV1& slot,
    const LatestStateReadOptionsV1& options,
    LatestStateReadResultV1* output) noexcept {
    if (output == nullptr) {
        return LatestStateErrorV1::kNullArgument;
    }
    const LatestStateErrorV1 options_error = ValidateReadOptions(options);
    if (options_error != LatestStateErrorV1::kNone) {
        return options_error;
    }

    LatestStateReadResultV1 result;
    const LatestStateErrorV1 read_error =
        ReadLatestStateSlotV1(slot, &result.value);
    if (read_error != LatestStateErrorV1::kNone) {
        return read_error;
    }
    if (!ClockIdentityExact(
            result.value.origin.clock_epoch,
            options.now_clock_epoch)) {
        return LatestStateErrorV1::kClockEpochMismatch;
    }
    if (options.now_monotonic_ns <
        result.value.snapshot.header.recv_monotonic_ns) {
        return LatestStateErrorV1::kClockRegression;
    }

    result.age_ns = options.now_monotonic_ns -
                    result.value.snapshot.header.recv_monotonic_ns;
    const std::size_t phase_index = static_cast<std::size_t>(
        result.value.snapshot.payload.phase);
    if (phase_index >= kLatestStatePhaseCountV1) {
        return LatestStateErrorV1::kCorruptSlot;
    }
    const std::int64_t threshold =
        options.stale_policy.max_age_ns_by_phase[phase_index];
    result.snapshot_stale = result.age_ns > threshold;
    result.effective_snapshot_quality_flags =
        result.value.snapshot.header.quality_flags;
    if (result.snapshot_stale) {
        result.effective_snapshot_quality_flags |= control::QualityBit(
            control::QualityFlagV1::kSnapshotStale);
    }
    *output = result;
    return LatestStateErrorV1::kNone;
}

LatestStateErrorV1 BatchReadLatestStateV1(
    std::span<const LatestStateBatchRequestV1> requests,
    const LatestStateReadOptionsV1& options,
    std::span<LatestStateBatchResultV1> output) noexcept {
    if (requests.size() != output.size()) {
        return LatestStateErrorV1::kOutputSizeMismatch;
    }
    const LatestStateErrorV1 options_error = ValidateReadOptions(options);
    if (options_error != LatestStateErrorV1::kNone) {
        return options_error;
    }
    for (std::size_t index = 0U; index < requests.size(); ++index) {
        output[index] = {};
        output[index].instrument_id = requests[index].instrument_id;
        if (requests[index].slot == nullptr ||
            requests[index].instrument_id == 0U) {
            output[index].error = LatestStateErrorV1::kNotFound;
            continue;
        }
        output[index].error = ReadLatestStateV1(
            *requests[index].slot, options, &output[index].latest);
        if (output[index].error == LatestStateErrorV1::kNone &&
            output[index].latest.value.snapshot.header.instrument_id !=
                requests[index].instrument_id) {
            output[index].error = LatestStateErrorV1::kInstrumentMismatch;
            output[index].latest = {};
        }
    }
    return LatestStateErrorV1::kNone;
}

class LatestStateLocalQueryV1::Impl final {
public:
    explicit Impl(std::vector<LatestStateQueryEntryV1> entries)
        : entries_(std::move(entries)) {}

    [[nodiscard]] const LatestStateQueryEntryV1* Find(
        std::uint32_t instrument_id) const noexcept {
        const auto iterator = std::lower_bound(
            entries_.begin(), entries_.end(), instrument_id,
            [](const LatestStateQueryEntryV1& entry,
               std::uint32_t value) noexcept {
                return entry.instrument_id < value;
            });
        if (iterator == entries_.end() ||
            iterator->instrument_id != instrument_id) {
            return nullptr;
        }
        return &*iterator;
    }

    std::vector<LatestStateQueryEntryV1> entries_;
};

LatestStateLocalQueryV1::LatestStateLocalQueryV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

LatestStateLocalQueryV1::~LatestStateLocalQueryV1() = default;

LatestStateErrorV1 LatestStateLocalQueryV1::Create(
    std::span<const LatestStateQueryEntryV1> entries,
    std::unique_ptr<LatestStateLocalQueryV1>* query) {
    if (query == nullptr) {
        return LatestStateErrorV1::kNullArgument;
    }
    query->reset();
    std::vector<LatestStateQueryEntryV1> copy;
    try {
        copy.assign(entries.begin(), entries.end());
        std::sort(
            copy.begin(), copy.end(),
            [](const LatestStateQueryEntryV1& left,
               const LatestStateQueryEntryV1& right) noexcept {
                return left.instrument_id < right.instrument_id;
            });
    } catch (const std::bad_alloc&) {
        return LatestStateErrorV1::kAllocationFailed;
    }
    for (std::size_t index = 0U; index < copy.size(); ++index) {
        if (copy[index].instrument_id == 0U || copy[index].slot == nullptr) {
            return LatestStateErrorV1::kInvalidConfiguration;
        }
        if (index != 0U &&
            copy[index - 1U].instrument_id == copy[index].instrument_id) {
            return LatestStateErrorV1::kDuplicateInstrument;
        }
    }
    try {
        auto impl = std::make_unique<Impl>(std::move(copy));
        query->reset(new LatestStateLocalQueryV1(std::move(impl)));
    } catch (const std::bad_alloc&) {
        return LatestStateErrorV1::kAllocationFailed;
    }
    return LatestStateErrorV1::kNone;
}

LatestStateErrorV1 LatestStateLocalQueryV1::GetLatest(
    std::uint32_t instrument_id,
    const LatestStateReadOptionsV1& options,
    LatestStateReadResultV1* output) const noexcept {
    if (output == nullptr) {
        return LatestStateErrorV1::kNullArgument;
    }
    const LatestStateQueryEntryV1* const entry = impl_->Find(instrument_id);
    if (entry == nullptr) {
        return LatestStateErrorV1::kNotFound;
    }
    const LatestStateErrorV1 error =
        ReadLatestStateV1(*entry->slot, options, output);
    if (error == LatestStateErrorV1::kNone &&
        output->value.snapshot.header.instrument_id != instrument_id) {
        *output = {};
        return LatestStateErrorV1::kInstrumentMismatch;
    }
    return error;
}

LatestStateErrorV1 LatestStateLocalQueryV1::BatchGetLatest(
    std::span<const std::uint32_t> instrument_ids,
    const LatestStateReadOptionsV1& options,
    std::span<LatestStateBatchResultV1> output) const noexcept {
    if (instrument_ids.size() != output.size()) {
        return LatestStateErrorV1::kOutputSizeMismatch;
    }
    const LatestStateErrorV1 options_error = ValidateReadOptions(options);
    if (options_error != LatestStateErrorV1::kNone) {
        return options_error;
    }
    for (std::size_t index = 0U; index < instrument_ids.size(); ++index) {
        output[index] = {};
        output[index].instrument_id = instrument_ids[index];
        const LatestStateQueryEntryV1* const entry =
            impl_->Find(instrument_ids[index]);
        if (entry == nullptr) {
            output[index].error = LatestStateErrorV1::kNotFound;
            continue;
        }
        output[index].error = ReadLatestStateV1(
            *entry->slot, options, &output[index].latest);
        if (output[index].error == LatestStateErrorV1::kNone &&
            output[index].latest.value.snapshot.header.instrument_id !=
                instrument_ids[index]) {
            output[index].error = LatestStateErrorV1::kInstrumentMismatch;
            output[index].latest = {};
        }
    }
    return LatestStateErrorV1::kNone;
}

LatestStateErrorV1 LatestStateLocalQueryV1::ScanMarket(
    canonical::CanonicalMarketV1 market,
    const LatestStateReadOptionsV1& options,
    std::span<LatestStateBatchResultV1> output,
    std::size_t* written) const noexcept {
    if (written == nullptr || market == canonical::CanonicalMarketV1::kUnknown) {
        return LatestStateErrorV1::kNullArgument;
    }
    *written = 0U;
    const LatestStateErrorV1 options_error = ValidateReadOptions(options);
    if (options_error != LatestStateErrorV1::kNone) {
        return options_error;
    }
    bool overflow = false;
    for (const LatestStateQueryEntryV1& entry : impl_->entries_) {
        LatestStateReadResultV1 latest;
        const LatestStateErrorV1 error =
            ReadLatestStateV1(*entry.slot, options, &latest);
        if (error == LatestStateErrorV1::kUninitialized) {
            continue;
        }
        if (error != LatestStateErrorV1::kNone) {
            return error;
        }
        if (latest.value.snapshot.header.instrument_id !=
            entry.instrument_id) {
            return LatestStateErrorV1::kInstrumentMismatch;
        }
        if (latest.value.snapshot.header.market != market) {
            continue;
        }
        if (*written == output.size()) {
            overflow = true;
            continue;
        }
        output[*written] = {};
        output[*written].instrument_id = entry.instrument_id;
        output[*written].error = LatestStateErrorV1::kNone;
        output[*written].latest = latest;
        ++*written;
    }
    return overflow ? LatestStateErrorV1::kOutputSizeMismatch
                    : LatestStateErrorV1::kNone;
}

}  // namespace l2flow::state
