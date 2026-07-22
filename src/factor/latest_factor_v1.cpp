#include "l2flow/factor/latest_factor_v1.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>

namespace l2flow::factor {
namespace {

constexpr std::size_t kWordBytes = sizeof(std::uint64_t);
constexpr std::size_t kWordCount = kLatestFactorSlotBytesV1 / kWordBytes;
constexpr std::size_t kMaximumReadAttempts = 1000U;

constexpr std::size_t kSequenceOffset = 0U;
constexpr std::size_t kMagicOffset = 8U;
constexpr std::size_t kSlotSizeOffset = 12U;
constexpr std::size_t kSchemaVersionOffset = 16U;
constexpr std::size_t kInitializedOffset = 18U;
constexpr std::size_t kValidityOffset = 19U;
constexpr std::size_t kFactorIdOffset = 24U;
constexpr std::size_t kFactorVersionOffset = 56U;
constexpr std::size_t kInstrumentIdOffset = 88U;
constexpr std::size_t kAsofOffset = 96U;
constexpr std::size_t kValueOffset = 104U;
constexpr std::size_t kQualityOffset = 112U;
constexpr std::size_t kWatermarkSetIdOffset = 120U;
constexpr std::size_t kInputIdentityOffset = 128U;
constexpr std::size_t kCalculationLatencyOffset = 160U;
constexpr std::size_t kClockAlgorithmOffset = 168U;
constexpr std::size_t kClockDigestOffset = 176U;
constexpr std::size_t kClockLabelOffset = 208U;
constexpr std::size_t kUsedBytes = 216U;

constexpr std::string_view kLatestFactorSchemaDescriptor =
    "l2flow.factor.latest-slot.v1|endian=little|slot_size=4096|alignment=64|"
    "atomic_words=u64|seqlock:u8@0|magic:u4@8|slot_size:u4@12|"
    "schema_version:u2@16|initialized:u1@18|valid:u1@19|reserved:V4@20|"
    "factor_id_sha256:V32@24|factor_version_sha256:V32@56|"
    "instrument_id:u4@88|reserved:V4@92|asof_ns:i8@96|value:f8@104|"
    "input_quality_flags:u8@112|watermark_set_id:u8@120|"
    "input_identity_sha256:V32@128|calculation_latency_ns:u8@160|"
    "clock_epoch_algorithm:u4@168|reserved:V4@172|"
    "clock_epoch_digest:V32@176|clock_epoch_label:u8@208|"
    "reserved:V3880@216|invalid_value=positive-zero|"
    "ordering=slot-identity+asof|idempotence-excludes="
    "watermark_set_id,calculation_latency_ns,clock_epoch_label";

using WordImage = std::array<std::uint64_t, kWordCount>;

static_assert(kLatestFactorSlotBytesV1 % kWordBytes == 0U);
static_assert(kUsedBytes % kWordBytes == 0U);

[[nodiscard]] bool DigestNonzero(
    const l2flow::common::Sha256Digest& digest) noexcept {
    return std::any_of(
        digest.begin(), digest.end(),
        [](std::byte value) { return value != std::byte{0U}; });
}

[[nodiscard]] bool StorageValid(
    const void* storage,
    std::size_t storage_size) noexcept {
    return storage != nullptr && storage_size == kLatestFactorSlotBytesV1 &&
           reinterpret_cast<std::uintptr_t>(storage) %
                   kLatestFactorSlotAlignmentV1 ==
               0U;
}

[[nodiscard]] bool RangesOverlap(
    const void* left,
    std::size_t left_size,
    const void* right,
    std::size_t right_size) noexcept {
    if (left == nullptr || right == nullptr || left_size == 0U ||
        right_size == 0U) {
        return false;
    }
    const std::uintptr_t left_begin =
        reinterpret_cast<std::uintptr_t>(left);
    const std::uintptr_t right_begin =
        reinterpret_cast<std::uintptr_t>(right);
    if (left_begin >
            std::numeric_limits<std::uintptr_t>::max() - left_size ||
        right_begin >
            std::numeric_limits<std::uintptr_t>::max() - right_size) {
        return true;
    }
    return left_begin < right_begin + right_size &&
           right_begin < left_begin + left_size;
}

[[nodiscard]] std::uint64_t* WordPointer(
    void* storage,
    std::size_t word_index) noexcept {
    auto* bytes = static_cast<std::byte*>(storage);
    return reinterpret_cast<std::uint64_t*>(bytes + word_index * kWordBytes);
}

[[nodiscard]] const std::uint64_t* WordPointer(
    const void* storage,
    std::size_t word_index) noexcept {
    const auto* bytes = static_cast<const std::byte*>(storage);
    return reinterpret_cast<const std::uint64_t*>(
        bytes + word_index * kWordBytes);
}

[[nodiscard]] std::uint64_t AtomicLoadWord(
    const void* storage,
    std::size_t word_index,
    int ordering) noexcept {
    return __atomic_load_n(WordPointer(storage, word_index), ordering);
}

void AtomicStoreWord(
    void* storage,
    std::size_t word_index,
    std::uint64_t value,
    int ordering) noexcept {
    __atomic_store_n(WordPointer(storage, word_index), value, ordering);
}

[[nodiscard]] std::span<std::byte> ImageBytes(WordImage* image) noexcept {
    return std::as_writable_bytes(std::span(image->data(), image->size()));
}

[[nodiscard]] std::span<const std::byte> ImageBytes(
    const WordImage& image) noexcept {
    return std::as_bytes(std::span(image.data(), image.size()));
}

template <typename Value>
void StoreImageValue(
    WordImage* image,
    std::size_t offset,
    const Value& value) noexcept {
    std::memcpy(ImageBytes(image).data() + offset, &value, sizeof(value));
}

template <typename Value>
[[nodiscard]] Value LoadImageValue(
    const WordImage& image,
    std::size_t offset) noexcept {
    Value value{};
    std::memcpy(&value, ImageBytes(image).data() + offset, sizeof(value));
    return value;
}

void StoreImageDigest(
    WordImage* image,
    std::size_t offset,
    const l2flow::common::Sha256Digest& digest) noexcept {
    std::memcpy(
        ImageBytes(image).data() + offset, digest.data(), digest.size());
}

[[nodiscard]] l2flow::common::Sha256Digest LoadImageDigest(
    const WordImage& image,
    std::size_t offset) noexcept {
    l2flow::common::Sha256Digest digest{};
    std::memcpy(
        digest.data(), ImageBytes(image).data() + offset, digest.size());
    return digest;
}

[[nodiscard]] bool ValueValid(const LatestFactorValueV1& value) noexcept {
    return DigestNonzero(value.factor_id_sha256) &&
           DigestNonzero(value.factor_version_sha256) &&
           value.instrument_id != 0U && value.asof_ns > 0 &&
           (!value.valid || std::isfinite(value.value)) &&
           (value.valid ||
            std::bit_cast<std::uint64_t>(value.value) == 0U) &&
           (value.input_quality_flags &
            ~l2flow::canonical::kCanonicalQualityFlagsMaskV1) == 0U &&
           value.watermark_set_id != 0U &&
           DigestNonzero(value.input_identity_sha256) &&
           l2flow::canonical::ClockEpochIdentityV1Valid(value.clock_epoch);
}

[[nodiscard]] WordImage Encode(const LatestFactorValueV1& source) noexcept {
    LatestFactorValueV1 value = source;
    if (!value.valid) {
        value.value = 0.0;
    }
    WordImage image{};
    StoreImageValue(&image, kMagicOffset, kLatestFactorSlotMagicV1);
    const std::uint32_t slot_size =
        static_cast<std::uint32_t>(kLatestFactorSlotBytesV1);
    StoreImageValue(&image, kSlotSizeOffset, slot_size);
    StoreImageValue(
        &image, kSchemaVersionOffset, kLatestFactorSlotVersionV1);
    const std::uint8_t initialized = 1U;
    const std::uint8_t validity = value.valid ? 1U : 0U;
    StoreImageValue(&image, kInitializedOffset, initialized);
    StoreImageValue(&image, kValidityOffset, validity);
    StoreImageDigest(&image, kFactorIdOffset, value.factor_id_sha256);
    StoreImageDigest(
        &image, kFactorVersionOffset, value.factor_version_sha256);
    StoreImageValue(&image, kInstrumentIdOffset, value.instrument_id);
    StoreImageValue(&image, kAsofOffset, value.asof_ns);
    const std::uint64_t value_bits = std::bit_cast<std::uint64_t>(value.value);
    StoreImageValue(&image, kValueOffset, value_bits);
    StoreImageValue(
        &image, kQualityOffset, value.input_quality_flags);
    StoreImageValue(
        &image, kWatermarkSetIdOffset, value.watermark_set_id);
    StoreImageDigest(
        &image, kInputIdentityOffset, value.input_identity_sha256);
    StoreImageValue(
        &image, kCalculationLatencyOffset,
        value.calculation_latency_ns);
    StoreImageValue(
        &image, kClockAlgorithmOffset, value.clock_epoch.algorithm);
    StoreImageDigest(
        &image, kClockDigestOffset, value.clock_epoch.digest);
    StoreImageValue(&image, kClockLabelOffset, value.clock_epoch.label);
    return image;
}

[[nodiscard]] LatestFactorErrorV1 Decode(
    const WordImage& image,
    LatestFactorValueV1* output) noexcept {
    if (LoadImageValue<std::uint32_t>(image, kMagicOffset) == 0U &&
        LoadImageValue<std::uint8_t>(image, kInitializedOffset) == 0U) {
        return std::all_of(
                   image.begin() + 1, image.end(),
                   [](std::uint64_t value) { return value == 0U; })
                   ? LatestFactorErrorV1::kEmpty
                   : LatestFactorErrorV1::kCorruptSlot;
    }
    if (LoadImageValue<std::uint32_t>(image, kMagicOffset) !=
            kLatestFactorSlotMagicV1 ||
        LoadImageValue<std::uint32_t>(image, kSlotSizeOffset) !=
            kLatestFactorSlotBytesV1 ||
        LoadImageValue<std::uint16_t>(image, kSchemaVersionOffset) !=
            kLatestFactorSlotVersionV1 ||
        LoadImageValue<std::uint8_t>(image, kInitializedOffset) != 1U ||
        LoadImageValue<std::uint8_t>(image, kValidityOffset) > 1U) {
        return LatestFactorErrorV1::kCorruptSlot;
    }
    const auto bytes = ImageBytes(image);
    const auto range_nonzero = [&bytes](
                                   std::size_t begin,
                                   std::size_t end) noexcept {
        return std::any_of(
            bytes.begin() + static_cast<std::ptrdiff_t>(begin),
            bytes.begin() + static_cast<std::ptrdiff_t>(end),
            [](std::byte value) { return value != std::byte{0U}; });
    };
    // Internal padding and all bytes outside the frozen used prefix must stay
    // zero; otherwise a future producer could smuggle unversioned semantics.
    if (range_nonzero(20U, 24U) || range_nonzero(92U, 96U) ||
        range_nonzero(172U, 176U) || std::any_of(
            bytes.begin() + static_cast<std::ptrdiff_t>(kUsedBytes),
            bytes.end(),
            [](std::byte value) { return value != std::byte{0U}; })) {
        return LatestFactorErrorV1::kCorruptSlot;
    }
    LatestFactorValueV1 candidate{};
    candidate.factor_id_sha256 = LoadImageDigest(image, kFactorIdOffset);
    candidate.factor_version_sha256 =
        LoadImageDigest(image, kFactorVersionOffset);
    candidate.instrument_id =
        LoadImageValue<std::uint32_t>(image, kInstrumentIdOffset);
    candidate.asof_ns =
        LoadImageValue<std::int64_t>(image, kAsofOffset);
    candidate.value = std::bit_cast<double>(
        LoadImageValue<std::uint64_t>(image, kValueOffset));
    candidate.valid =
        LoadImageValue<std::uint8_t>(image, kValidityOffset) != 0U;
    candidate.input_quality_flags =
        LoadImageValue<std::uint64_t>(image, kQualityOffset);
    candidate.watermark_set_id =
        LoadImageValue<std::uint64_t>(image, kWatermarkSetIdOffset);
    candidate.input_identity_sha256 =
        LoadImageDigest(image, kInputIdentityOffset);
    candidate.calculation_latency_ns =
        LoadImageValue<std::uint64_t>(image, kCalculationLatencyOffset);
    candidate.clock_epoch.algorithm =
        LoadImageValue<std::uint32_t>(image, kClockAlgorithmOffset);
    candidate.clock_epoch.digest =
        LoadImageDigest(image, kClockDigestOffset);
    candidate.clock_epoch.label =
        LoadImageValue<std::uint64_t>(image, kClockLabelOffset);
    if (!ValueValid(candidate)) {
        return LatestFactorErrorV1::kCorruptSlot;
    }
    *output = candidate;
    return LatestFactorErrorV1::kNone;
}

[[nodiscard]] bool SameSlotIdentity(
    const LatestFactorValueV1& left,
    const LatestFactorValueV1& right) noexcept {
    return left.factor_id_sha256 == right.factor_id_sha256 &&
           left.factor_version_sha256 == right.factor_version_sha256 &&
           left.instrument_id == right.instrument_id;
}

[[nodiscard]] bool SameIdempotentOutput(
    const LatestFactorValueV1& left,
    const LatestFactorValueV1& right) noexcept {
    return SameSlotIdentity(left, right) && left.asof_ns == right.asof_ns &&
           left.input_identity_sha256 == right.input_identity_sha256 &&
           left.valid == right.valid &&
           std::bit_cast<std::uint64_t>(left.value) ==
               std::bit_cast<std::uint64_t>(right.value) &&
           left.input_quality_flags == right.input_quality_flags &&
           left.clock_epoch.algorithm == right.clock_epoch.algorithm &&
           left.clock_epoch.digest == right.clock_epoch.digest;
    // Run-local watermark ID, calculation latency and display-only clock label
    // are intentionally not stable replay identity fields.
}

void ToC(
    const LatestFactorValueV1& input,
    l2flow_latest_factor_value_v1* output) noexcept {
    std::memcpy(
        output->factor_id_sha256,
        input.factor_id_sha256.data(),
        input.factor_id_sha256.size());
    std::memcpy(
        output->factor_version_sha256,
        input.factor_version_sha256.data(),
        input.factor_version_sha256.size());
    output->instrument_id = input.instrument_id;
    output->asof_ns = input.asof_ns;
    output->value = input.value;
    output->valid = input.valid ? 1U : 0U;
    std::fill(std::begin(output->reserved0), std::end(output->reserved0), 0U);
    output->input_quality_flags = input.input_quality_flags;
    output->watermark_set_id = input.watermark_set_id;
    std::memcpy(
        output->input_identity_sha256,
        input.input_identity_sha256.data(),
        input.input_identity_sha256.size());
    output->calculation_latency_ns = input.calculation_latency_ns;
    output->clock_epoch_algorithm = input.clock_epoch.algorithm;
    std::memcpy(
        output->clock_epoch_digest,
        input.clock_epoch.digest.data(),
        input.clock_epoch.digest.size());
    output->clock_epoch_label = input.clock_epoch.label;
}

[[nodiscard]] bool FromC(
    const l2flow_latest_factor_value_v1& input,
    LatestFactorValueV1* output) noexcept {
    if (input.valid > 1U ||
        (input.valid == 0U &&
         std::bit_cast<std::uint64_t>(input.value) != 0U) ||
        std::any_of(
            std::begin(input.reserved0),
            std::end(input.reserved0),
            [](std::uint8_t value) { return value != 0U; })) {
        return false;
    }
    LatestFactorValueV1 candidate{};
    std::memcpy(
        candidate.factor_id_sha256.data(),
        input.factor_id_sha256,
        candidate.factor_id_sha256.size());
    std::memcpy(
        candidate.factor_version_sha256.data(),
        input.factor_version_sha256,
        candidate.factor_version_sha256.size());
    candidate.instrument_id = input.instrument_id;
    candidate.asof_ns = input.asof_ns;
    candidate.value = input.value;
    candidate.valid = input.valid != 0U;
    if (!candidate.valid) {
        candidate.value = 0.0;
    }
    candidate.input_quality_flags = input.input_quality_flags;
    candidate.watermark_set_id = input.watermark_set_id;
    std::memcpy(
        candidate.input_identity_sha256.data(),
        input.input_identity_sha256,
        candidate.input_identity_sha256.size());
    candidate.calculation_latency_ns = input.calculation_latency_ns;
    candidate.clock_epoch.algorithm = input.clock_epoch_algorithm;
    std::memcpy(
        candidate.clock_epoch.digest.data(),
        input.clock_epoch_digest,
        candidate.clock_epoch.digest.size());
    candidate.clock_epoch.label = input.clock_epoch_label;
    if (!ValueValid(candidate)) {
        return false;
    }
    *output = candidate;
    return true;
}

}  // namespace

std::string_view LatestFactorErrorNameV1(
    LatestFactorErrorV1 error) noexcept {
    switch (error) {
        case LatestFactorErrorV1::kNone:
            return "none";
        case LatestFactorErrorV1::kNullArgument:
            return "null_argument";
        case LatestFactorErrorV1::kInvalidStorage:
            return "invalid_storage";
        case LatestFactorErrorV1::kUnsupportedHost:
            return "unsupported_host";
        case LatestFactorErrorV1::kAtomicsNotLockFree:
            return "atomics_not_lock_free";
        case LatestFactorErrorV1::kInvalidValue:
            return "invalid_value";
        case LatestFactorErrorV1::kEmpty:
            return "empty";
        case LatestFactorErrorV1::kBusy:
            return "busy";
        case LatestFactorErrorV1::kSequenceExhausted:
            return "sequence_exhausted";
        case LatestFactorErrorV1::kSlotIdentityMismatch:
            return "slot_identity_mismatch";
        case LatestFactorErrorV1::kOldOutput:
            return "old_output";
        case LatestFactorErrorV1::kOutputConflict:
            return "output_conflict";
        case LatestFactorErrorV1::kCorruptSlot:
            return "corrupt_slot";
        case LatestFactorErrorV1::kAlreadyInitialized:
            return "already_initialized";
    }
    return "invalid_latest_factor_error";
}

bool LatestFactorAtomicsLockFreeV1() noexcept {
    alignas(8) std::uint64_t probe = 0U;
    return __atomic_is_lock_free(sizeof(probe), &probe);
}

std::string_view LatestFactorSchemaDescriptorV1() noexcept {
    return kLatestFactorSchemaDescriptor;
}

l2flow::common::Sha256Digest
LatestFactorSchemaDescriptorSha256V1() noexcept {
    return l2flow::common::ComputeSha256(kLatestFactorSchemaDescriptor);
}

LatestFactorErrorV1 InitializeLatestFactorSlotV1(
    LatestFactorSlotV1* slot) noexcept {
    const int error = l2flow_latest_factor_initialize_v1(
        slot, slot == nullptr ? 0U : sizeof(*slot));
    return static_cast<LatestFactorErrorV1>(error);
}

LatestFactorPublishResultV1 PublishLatestFactorV1(
    LatestFactorSlotV1* slot,
    const LatestFactorValueV1& value) noexcept {
    LatestFactorPublishResultV1 result{};
    l2flow_latest_factor_value_v1 wire{};
    ToC(value, &wire);
    std::uint8_t disposition = 0U;
    result.error = static_cast<LatestFactorErrorV1>(
        l2flow_latest_factor_publish_v1(
            slot, slot == nullptr ? 0U : sizeof(*slot),
            &wire, &disposition));
    if (result.error == LatestFactorErrorV1::kNone) {
        result.disposition =
            static_cast<LatestFactorPublishDispositionV1>(disposition);
    }
    return result;
}

LatestFactorErrorV1 ReadLatestFactorV1(
    const LatestFactorSlotV1& slot,
    LatestFactorValueV1* value,
    std::uint64_t* stable_sequence) noexcept {
    if (value == nullptr) {
        return LatestFactorErrorV1::kNullArgument;
    }
    l2flow_latest_factor_value_v1 wire{};
    const int error = l2flow_latest_factor_read_v1(
        &slot, sizeof(slot), &wire, stable_sequence);
    if (error != L2FLOW_LATEST_FACTOR_NONE_V1) {
        return static_cast<LatestFactorErrorV1>(error);
    }
    LatestFactorValueV1 candidate{};
    if (!FromC(wire, &candidate)) {
        return LatestFactorErrorV1::kCorruptSlot;
    }
    *value = candidate;
    return LatestFactorErrorV1::kNone;
}

}  // namespace l2flow::factor

extern "C" int l2flow_latest_factor_atomics_lock_free_v1(void) {
    return l2flow::factor::LatestFactorAtomicsLockFreeV1() ? 1 : 0;
}

extern "C" int l2flow_latest_factor_initialize_v1(
    void* storage,
    std::size_t storage_size) {
    using namespace l2flow::factor;
    if (!StorageValid(storage, storage_size)) {
        return L2FLOW_LATEST_FACTOR_INVALID_STORAGE_V1;
    }
    if (!l2flow::canonical::CanonicalHostIsLittleEndianV1()) {
        return L2FLOW_LATEST_FACTOR_UNSUPPORTED_HOST_V1;
    }
    if (!LatestFactorAtomicsLockFreeV1()) {
        return L2FLOW_LATEST_FACTOR_ATOMICS_NOT_LOCK_FREE_V1;
    }
    for (std::size_t index = 0U; index < kWordCount; ++index) {
        if (AtomicLoadWord(storage, index, __ATOMIC_ACQUIRE) != 0U) {
            return L2FLOW_LATEST_FACTOR_ALREADY_INITIALIZED_V1;
        }
    }
    return L2FLOW_LATEST_FACTOR_NONE_V1;
}

extern "C" int l2flow_latest_factor_publish_v1(
    void* storage,
    std::size_t storage_size,
    const l2flow_latest_factor_value_v1* value,
    std::uint8_t* disposition) {
    using namespace l2flow::factor;
    if (value == nullptr || disposition == nullptr) {
        return L2FLOW_LATEST_FACTOR_NULL_ARGUMENT_V1;
    }
    if (!StorageValid(storage, storage_size)) {
        return L2FLOW_LATEST_FACTOR_INVALID_STORAGE_V1;
    }
    if (RangesOverlap(storage, storage_size, value, sizeof(*value)) ||
        RangesOverlap(
            storage, storage_size, disposition, sizeof(*disposition))) {
        return L2FLOW_LATEST_FACTOR_INVALID_STORAGE_V1;
    }
    if (RangesOverlap(
            value, sizeof(*value), disposition, sizeof(*disposition))) {
        return L2FLOW_LATEST_FACTOR_INVALID_VALUE_V1;
    }
    *disposition = 0U;
    if (!l2flow::canonical::CanonicalHostIsLittleEndianV1()) {
        return L2FLOW_LATEST_FACTOR_UNSUPPORTED_HOST_V1;
    }
    if (!LatestFactorAtomicsLockFreeV1()) {
        return L2FLOW_LATEST_FACTOR_ATOMICS_NOT_LOCK_FREE_V1;
    }
    LatestFactorValueV1 candidate{};
    if (!FromC(*value, &candidate)) {
        return L2FLOW_LATEST_FACTOR_INVALID_VALUE_V1;
    }

    std::uint64_t sequence =
        AtomicLoadWord(storage, kSequenceOffset / kWordBytes, __ATOMIC_ACQUIRE);
    if ((sequence & 1U) != 0U) {
        return L2FLOW_LATEST_FACTOR_BUSY_V1;
    }
    if (sequence > std::numeric_limits<std::uint64_t>::max() - 2U) {
        return L2FLOW_LATEST_FACTOR_SEQUENCE_EXHAUSTED_V1;
    }
    const std::uint64_t odd = sequence + 1U;
    if (!__atomic_compare_exchange_n(
            WordPointer(storage, kSequenceOffset / kWordBytes),
            &sequence,
            odd,
            false,
            __ATOMIC_ACQ_REL,
            __ATOMIC_ACQUIRE)) {
        return L2FLOW_LATEST_FACTOR_BUSY_V1;
    }

    WordImage existing_image{};
    for (std::size_t index = 1U; index < kWordCount; ++index) {
        existing_image[index] =
            AtomicLoadWord(storage, index, __ATOMIC_RELAXED);
    }
    LatestFactorValueV1 existing{};
    const LatestFactorErrorV1 existing_error =
        Decode(existing_image, &existing);
    int decision = L2FLOW_LATEST_FACTOR_NONE_V1;
    std::uint8_t selected_disposition =
        L2FLOW_LATEST_FACTOR_PUBLISHED_V1;
    if (existing_error != LatestFactorErrorV1::kEmpty &&
        existing_error != LatestFactorErrorV1::kNone) {
        decision = L2FLOW_LATEST_FACTOR_CORRUPT_SLOT_V1;
    } else if (existing_error == LatestFactorErrorV1::kNone) {
        if (!SameSlotIdentity(existing, candidate)) {
            decision = L2FLOW_LATEST_FACTOR_SLOT_IDENTITY_MISMATCH_V1;
        } else if (candidate.asof_ns < existing.asof_ns) {
            decision = L2FLOW_LATEST_FACTOR_OLD_OUTPUT_V1;
        } else if (candidate.asof_ns == existing.asof_ns) {
            if (SameIdempotentOutput(existing, candidate)) {
                selected_disposition = L2FLOW_LATEST_FACTOR_IDEMPOTENT_V1;
            } else {
                decision = L2FLOW_LATEST_FACTOR_OUTPUT_CONFLICT_V1;
            }
        }
    }

    if (decision == L2FLOW_LATEST_FACTOR_NONE_V1 &&
        selected_disposition == L2FLOW_LATEST_FACTOR_PUBLISHED_V1) {
        const WordImage encoded = Encode(candidate);
        for (std::size_t index = 1U; index < kWordCount; ++index) {
            AtomicStoreWord(
                storage, index, encoded[index], __ATOMIC_RELAXED);
        }
    }
    const bool payload_changed =
        decision == L2FLOW_LATEST_FACTOR_NONE_V1 &&
        selected_disposition == L2FLOW_LATEST_FACTOR_PUBLISHED_V1;
    AtomicStoreWord(
        storage,
        kSequenceOffset / kWordBytes,
        payload_changed ? sequence + 2U : sequence,
        __ATOMIC_RELEASE);
    if (decision != L2FLOW_LATEST_FACTOR_NONE_V1) {
        return decision;
    }
    *disposition = selected_disposition;
    return L2FLOW_LATEST_FACTOR_NONE_V1;
}

extern "C" int l2flow_latest_factor_read_v1(
    const void* storage,
    std::size_t storage_size,
    l2flow_latest_factor_value_v1* value,
    std::uint64_t* stable_sequence) {
    using namespace l2flow::factor;
    if (value == nullptr) {
        return L2FLOW_LATEST_FACTOR_NULL_ARGUMENT_V1;
    }
    if (!StorageValid(storage, storage_size)) {
        return L2FLOW_LATEST_FACTOR_INVALID_STORAGE_V1;
    }
    if (RangesOverlap(storage, storage_size, value, sizeof(*value)) ||
        (stable_sequence != nullptr &&
         (RangesOverlap(
              storage, storage_size, stable_sequence,
              sizeof(*stable_sequence)) ||
          RangesOverlap(
              value, sizeof(*value), stable_sequence,
              sizeof(*stable_sequence))))) {
        return L2FLOW_LATEST_FACTOR_INVALID_STORAGE_V1;
    }
    if (!l2flow::canonical::CanonicalHostIsLittleEndianV1()) {
        return L2FLOW_LATEST_FACTOR_UNSUPPORTED_HOST_V1;
    }
    if (!LatestFactorAtomicsLockFreeV1()) {
        return L2FLOW_LATEST_FACTOR_ATOMICS_NOT_LOCK_FREE_V1;
    }
    for (std::size_t attempt = 0U; attempt < kMaximumReadAttempts; ++attempt) {
        const std::uint64_t before = AtomicLoadWord(
            storage, kSequenceOffset / kWordBytes, __ATOMIC_ACQUIRE);
        if ((before & 1U) != 0U) {
            continue;
        }
        WordImage image{};
        for (std::size_t index = 1U; index < kWordCount; ++index) {
            image[index] = AtomicLoadWord(storage, index, __ATOMIC_RELAXED);
        }
        const std::uint64_t after = AtomicLoadWord(
            storage, kSequenceOffset / kWordBytes, __ATOMIC_SEQ_CST);
        if (before != after || (after & 1U) != 0U) {
            continue;
        }
        LatestFactorValueV1 decoded{};
        const LatestFactorErrorV1 error = Decode(image, &decoded);
        if (error != LatestFactorErrorV1::kNone) {
            return static_cast<int>(error);
        }
        ToC(decoded, value);
        if (stable_sequence != nullptr) {
            *stable_sequence = after;
        }
        return L2FLOW_LATEST_FACTOR_NONE_V1;
    }
    return L2FLOW_LATEST_FACTOR_BUSY_V1;
}

extern "C" const char* l2flow_latest_factor_schema_descriptor_v1(
    std::size_t* byte_count) {
    if (byte_count != nullptr) {
        *byte_count =
            l2flow::factor::LatestFactorSchemaDescriptorV1().size();
    }
    return l2flow::factor::LatestFactorSchemaDescriptorV1().data();
}

extern "C" int l2flow_latest_factor_schema_sha256_v1(
    std::uint8_t output[32]) {
    if (output == nullptr) {
        return L2FLOW_LATEST_FACTOR_NULL_ARGUMENT_V1;
    }
    const auto digest =
        l2flow::factor::LatestFactorSchemaDescriptorSha256V1();
    std::memcpy(output, digest.data(), digest.size());
    return L2FLOW_LATEST_FACTOR_NONE_V1;
}
