#include "l2flow/realtime/native_sequence_recovery_v1.h"

#include "mdl_shl2_msg.h"
#include "mdl_szl2_msg.h"

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace l2flow::realtime {
namespace {

namespace sh = datayes::mdl::mdl_shl2_msg;
namespace sz = datayes::mdl::mdl_szl2_msg;

constexpr std::uint16_t kCoreServiceVersion = 101U;
constexpr std::uint32_t kInvalidIndex =
    std::numeric_limits<std::uint32_t>::max();

#define L2FLOW_NATIVE_ASSERT_OFFSET(type, member, expected)                  \
    static_assert(offsetof(type, member) == (expected))
#define L2FLOW_NATIVE_ASSERT_MESSAGE_KEY(type, service, version, message)    \
    static_assert(type::ServiceID == (service));                             \
    static_assert(type::ServiceVer == (version));                            \
    static_assert(type::MessageID == (message))

L2FLOW_NATIVE_ASSERT_MESSAGE_KEY(sh::SHL2MarketData, 4, 101, 4);
L2FLOW_NATIVE_ASSERT_MESSAGE_KEY(sh::NGTSTick, 4, 101, 24);
L2FLOW_NATIVE_ASSERT_MESSAGE_KEY(sz::Snapshot300111_v2, 6, 101, 28);
L2FLOW_NATIVE_ASSERT_MESSAGE_KEY(sz::Order300192_v2, 6, 101, 33);
L2FLOW_NATIVE_ASSERT_MESSAGE_KEY(
    sz::Transaction300191_v2, 6, 101, 36);
static_assert(
    l2flow::sdk::kProductionMessageKeysV1[0] ==
    l2flow::sdk::MessageKey{4U, 101U, 4U});
static_assert(
    l2flow::sdk::kProductionMessageKeysV1[1] ==
    l2flow::sdk::MessageKey{4U, 101U, 24U});
static_assert(
    l2flow::sdk::kProductionMessageKeysV1[2] ==
    l2flow::sdk::MessageKey{6U, 101U, 28U});
static_assert(
    l2flow::sdk::kProductionMessageKeysV1[3] ==
    l2flow::sdk::MessageKey{6U, 101U, 33U});
static_assert(
    l2flow::sdk::kProductionMessageKeysV1[4] ==
    l2flow::sdk::MessageKey{6U, 101U, 36U});
static_assert(
    l2flow::sdk::kForbiddenCombinedTickMessageKeyV1 ==
    l2flow::sdk::MessageKey{6U, 101U, 53U});

static_assert(sizeof(sh::NGTSTick) == 70U);
L2FLOW_NATIVE_ASSERT_OFFSET(sh::NGTSTick, BizIndex, 0U);
L2FLOW_NATIVE_ASSERT_OFFSET(sh::NGTSTick, Channel, 8U);
static_assert(sizeof(sz::Order300192_v2) == 58U);
L2FLOW_NATIVE_ASSERT_OFFSET(sz::Order300192_v2, ChannelNo, 0U);
L2FLOW_NATIVE_ASSERT_OFFSET(sz::Order300192_v2, ApplSeqNum, 4U);
static_assert(sizeof(sz::Transaction300191_v2) == 70U);
L2FLOW_NATIVE_ASSERT_OFFSET(
    sz::Transaction300191_v2, ChannelNo, 0U);
L2FLOW_NATIVE_ASSERT_OFFSET(
    sz::Transaction300191_v2, ApplSeqNum, 4U);

#undef L2FLOW_NATIVE_ASSERT_OFFSET
#undef L2FLOW_NATIVE_ASSERT_MESSAGE_KEY

template <typename Unsigned>
[[nodiscard]] Unsigned LoadLittleEndian(
    std::span<const std::byte> bytes,
    std::size_t offset) noexcept {
    static_assert(std::is_unsigned_v<Unsigned>);
    Unsigned value = 0U;
    for (std::size_t index = 0U; index < sizeof(Unsigned); ++index) {
        const auto octet = static_cast<Unsigned>(
            std::to_integer<unsigned char>(bytes[offset + index]));
        const auto shift =
            static_cast<unsigned int>(index * 8U);
        value |= static_cast<Unsigned>(octet << shift);
    }
    return value;
}

[[nodiscard]] bool IsKey(
    const l2flow::sdk::MessageKey& key,
    std::uint8_t service,
    std::uint16_t version,
    std::uint16_t message) noexcept {
    return key.service_id == service &&
           key.service_version == version &&
           key.message_id == message;
}

[[nodiscard]] bool IsTrackedMessageIgnoringVersion(
    const l2flow::sdk::MessageKey& key) noexcept {
    return (key.service_id == 4U && key.message_id == 24U) ||
           (key.service_id == 6U &&
            (key.message_id == 33U || key.message_id == 36U));
}

[[nodiscard]] bool IsSnapshotIgnoringVersion(
    const l2flow::sdk::MessageKey& key) noexcept {
    return (key.service_id == 4U && key.message_id == 4U) ||
           (key.service_id == 6U && key.message_id == 28U);
}

[[nodiscard]] bool IsKnownMarket(
    NativeSequenceMarketV1 market) noexcept {
    return market == NativeSequenceMarketV1::kShanghai ||
           market == NativeSequenceMarketV1::kShenzhen;
}

[[nodiscard]] bool IsValidDescriptor(
    const NativeSequenceDescriptorV1& descriptor) noexcept {
    if (!IsKnownMarket(descriptor.domain.market) ||
        descriptor.sequence == 0U ||
        descriptor.sequence >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::int64_t>::max())) {
        return false;
    }
    return descriptor.domain.market !=
               NativeSequenceMarketV1::kShanghai ||
           (descriptor.domain.channel != 0U &&
            descriptor.domain.channel <=
                static_cast<std::uint32_t>(
                    std::numeric_limits<std::int32_t>::max()));
}

[[nodiscard]] bool IsValidMessageKeyForDomain(
    const NativeSequenceChannelV1& domain,
    const l2flow::sdk::MessageKey& key) noexcept {
    if (domain.market == NativeSequenceMarketV1::kShanghai) {
        return IsKey(key, 4U, kCoreServiceVersion, 24U);
    }
    if (domain.market == NativeSequenceMarketV1::kShenzhen) {
        return IsKey(key, 6U, kCoreServiceVersion, 33U) ||
               IsKey(key, 6U, kCoreServiceVersion, 36U);
    }
    return false;
}

[[nodiscard]] bool IsValidRecordClass(
    NativeSequenceRecoveryRecordClassV1 record_class) noexcept {
    return record_class ==
               NativeSequenceRecoveryRecordClassV1::kTarget ||
           record_class ==
               NativeSequenceRecoveryRecordClassV1::kFiltered;
}

[[nodiscard]] bool IsValidOriginPolicy(
    NativeSequenceOriginPolicyV1 policy) noexcept {
    return policy == NativeSequenceOriginPolicyV1::kExplicitOrigin ||
           policy ==
               NativeSequenceOriginPolicyV1::kBoundedProcessStart;
}

[[nodiscard]] bool IsValidChannelDomain(
    const NativeSequenceChannelV1& domain) noexcept {
    return IsKnownMarket(domain.market) &&
           (domain.market != NativeSequenceMarketV1::kShanghai ||
            (domain.channel != 0U &&
             domain.channel <=
                 static_cast<std::uint32_t>(
                     std::numeric_limits<std::int32_t>::max())));
}

[[nodiscard]] std::uint64_t Mix64(std::uint64_t value) noexcept {
    value ^= value >> 30U;
    value *= UINT64_C(0xbf58476d1ce4e5b9);
    value ^= value >> 27U;
    value *= UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31U);
}

void IncrementCounter(std::uint64_t* value) noexcept {
    if (*value != std::numeric_limits<std::uint64_t>::max()) {
        ++*value;
    }
}

[[nodiscard]] bool ComputeTableCapacity(
    std::size_t item_capacity,
    std::size_t* output) noexcept {
    if (output == nullptr || item_capacity == 0U ||
        item_capacity >
            std::numeric_limits<std::size_t>::max() / 2U) {
        return false;
    }
    const std::size_t target = item_capacity * 2U;
    std::size_t capacity = 1U;
    while (capacity < target) {
        if (capacity >
            std::numeric_limits<std::size_t>::max() / 2U) {
            return false;
        }
        capacity *= 2U;
    }
    if (capacity >=
        static_cast<std::size_t>(
            std::numeric_limits<std::uint32_t>::max())) {
        return false;
    }
    *output = capacity;
    return true;
}

}  // namespace

std::string_view NativeSequenceExtractErrorNameV1(
    NativeSequenceExtractErrorV1 error) noexcept {
    switch (error) {
        case NativeSequenceExtractErrorV1::kNone:
            return "none";
        case NativeSequenceExtractErrorV1::kNotTracked:
            return "not_tracked";
        case NativeSequenceExtractErrorV1::kNullOutput:
            return "null_output";
        case NativeSequenceExtractErrorV1::kUnsupportedMessage:
            return "unsupported_message";
        case NativeSequenceExtractErrorV1::kUnsupportedServiceVersion:
            return "unsupported_service_version";
        case NativeSequenceExtractErrorV1::kTruncated:
            return "truncated";
        case NativeSequenceExtractErrorV1::kInvalidSequence:
            return "invalid_sequence";
        case NativeSequenceExtractErrorV1::kInvalidChannel:
            return "invalid_channel";
    }
    return "unknown";
}

NativeSequenceExtractErrorV1 ExtractNativeSequenceV1(
    const l2flow::sdk::MessageKey& key,
    std::span<const std::byte> body,
    NativeSequenceDescriptorV1* output) noexcept {
    if (output == nullptr) {
        return NativeSequenceExtractErrorV1::kNullOutput;
    }
    *output = NativeSequenceDescriptorV1{};

    if ((IsTrackedMessageIgnoringVersion(key) ||
         IsSnapshotIgnoringVersion(key)) &&
        key.service_version != kCoreServiceVersion) {
        return NativeSequenceExtractErrorV1::
            kUnsupportedServiceVersion;
    }
    if (IsKey(key, 4U, kCoreServiceVersion, 4U) ||
        IsKey(key, 6U, kCoreServiceVersion, 28U)) {
        return NativeSequenceExtractErrorV1::kNotTracked;
    }

    if (IsKey(key, 4U, kCoreServiceVersion, 24U)) {
        constexpr std::size_t required =
            offsetof(sh::NGTSTick, Channel) +
            sizeof(sh::NGTSTick::Channel);
        if (body.size() < required) {
            return NativeSequenceExtractErrorV1::kTruncated;
        }
        const auto sequence_bits =
            LoadLittleEndian<std::uint64_t>(
                body, offsetof(sh::NGTSTick, BizIndex));
        const auto sequence =
            std::bit_cast<std::int64_t>(sequence_bits);
        const auto channel_bits =
            LoadLittleEndian<std::uint32_t>(
                body, offsetof(sh::NGTSTick, Channel));
        const auto channel =
            std::bit_cast<std::int32_t>(channel_bits);
        if (sequence <= 0) {
            return NativeSequenceExtractErrorV1::kInvalidSequence;
        }
        if (channel <= 0) {
            return NativeSequenceExtractErrorV1::kInvalidChannel;
        }
        output->domain.market =
            NativeSequenceMarketV1::kShanghai;
        output->domain.channel =
            static_cast<std::uint32_t>(channel);
        output->sequence =
            static_cast<std::uint64_t>(sequence);
        return NativeSequenceExtractErrorV1::kNone;
    }

    if (IsKey(key, 6U, kCoreServiceVersion, 33U) ||
        IsKey(key, 6U, kCoreServiceVersion, 36U)) {
        constexpr std::size_t required =
            offsetof(sz::Order300192_v2, ApplSeqNum) +
            sizeof(sz::Order300192_v2::ApplSeqNum);
        if (body.size() < required) {
            return NativeSequenceExtractErrorV1::kTruncated;
        }
        const auto channel =
            LoadLittleEndian<std::uint32_t>(
                body, offsetof(sz::Order300192_v2, ChannelNo));
        const auto sequence_bits =
            LoadLittleEndian<std::uint64_t>(
                body, offsetof(sz::Order300192_v2, ApplSeqNum));
        const auto sequence =
            std::bit_cast<std::int64_t>(sequence_bits);
        if (sequence <= 0) {
            return NativeSequenceExtractErrorV1::kInvalidSequence;
        }
        output->domain.market =
            NativeSequenceMarketV1::kShenzhen;
        output->domain.channel = channel;
        output->sequence =
            static_cast<std::uint64_t>(sequence);
        return NativeSequenceExtractErrorV1::kNone;
    }

    return NativeSequenceExtractErrorV1::kUnsupportedMessage;
}

std::string_view NativeSequenceRecoveryCreateErrorNameV1(
    NativeSequenceRecoveryCreateErrorV1 error) noexcept {
    switch (error) {
        case NativeSequenceRecoveryCreateErrorV1::kNone:
            return "none";
        case NativeSequenceRecoveryCreateErrorV1::kNullOutput:
            return "null_output";
        case NativeSequenceRecoveryCreateErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case NativeSequenceRecoveryCreateErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

std::string_view NativeSequenceRecoveryChannelStateNameV1(
    NativeSequenceRecoveryChannelStateV1 state) noexcept {
    switch (state) {
        case NativeSequenceRecoveryChannelStateV1::kHealthy:
            return "healthy";
        case NativeSequenceRecoveryChannelStateV1::kCatchingUp:
            return "catching_up";
        case NativeSequenceRecoveryChannelStateV1::kRepairing:
            return "repairing";
        case NativeSequenceRecoveryChannelStateV1::kFrozenConflict:
            return "frozen_conflict";
        case NativeSequenceRecoveryChannelStateV1::kFrozenResource:
            return "frozen_resource";
        case NativeSequenceRecoveryChannelStateV1::kBootstrapping:
            return "bootstrapping";
    }
    return "unknown";
}

std::string_view NativeSequenceRecoveryFreezeReasonNameV1(
    NativeSequenceRecoveryFreezeReasonV1 reason) noexcept {
    switch (reason) {
        case NativeSequenceRecoveryFreezeReasonV1::kNone:
            return "none";
        case NativeSequenceRecoveryFreezeReasonV1::kPayloadConflict:
            return "payload_conflict";
        case NativeSequenceRecoveryFreezeReasonV1::
            kPendingEntryCapacity:
            return "pending_entry_capacity";
        case NativeSequenceRecoveryFreezeReasonV1::
            kPerChannelEntryCapacity:
            return "per_channel_entry_capacity";
        case NativeSequenceRecoveryFreezeReasonV1::kPayloadCapacity:
            return "payload_capacity";
        case NativeSequenceRecoveryFreezeReasonV1::
            kReorderWindowExceeded:
            return "reorder_window_exceeded";
        case NativeSequenceRecoveryFreezeReasonV1::
            kDuplicateVerificationUnavailable:
            return "duplicate_verification_unavailable";
        case NativeSequenceRecoveryFreezeReasonV1::
            kInternalInvariant:
            return "internal_invariant";
        case NativeSequenceRecoveryFreezeReasonV1::
            kReorderBoundExceeded:
            return "reorder_bound_exceeded";
        case NativeSequenceRecoveryFreezeReasonV1::
            kInputSealedIncomplete:
            return "input_sealed_incomplete";
    }
    return "unknown";
}

std::string_view NativeSequenceRecoveryObserveDispositionNameV1(
    NativeSequenceRecoveryObserveDispositionV1 disposition) noexcept {
    switch (disposition) {
        case NativeSequenceRecoveryObserveDispositionV1::kAccepted:
            return "accepted";
        case NativeSequenceRecoveryObserveDispositionV1::kGapOpened:
            return "gap_opened";
        case NativeSequenceRecoveryObserveDispositionV1::kGapExtended:
            return "gap_extended";
        case NativeSequenceRecoveryObserveDispositionV1::kBackfill:
            return "backfill";
        case NativeSequenceRecoveryObserveDispositionV1::
            kDuplicatePending:
            return "duplicate_pending";
        case NativeSequenceRecoveryObserveDispositionV1::
            kDuplicateCertified:
            return "duplicate_certified";
        case NativeSequenceRecoveryObserveDispositionV1::
            kDuplicateOutsideRetention:
            return "duplicate_outside_retention";
        case NativeSequenceRecoveryObserveDispositionV1::kBeforeOrigin:
            return "before_origin";
        case NativeSequenceRecoveryObserveDispositionV1::
            kPayloadConflict:
            return "payload_conflict";
        case NativeSequenceRecoveryObserveDispositionV1::kChannelFrozen:
            return "channel_frozen";
        case NativeSequenceRecoveryObserveDispositionV1::kResourceFrozen:
            return "resource_frozen";
        case NativeSequenceRecoveryObserveDispositionV1::kChannelCapacity:
            return "channel_capacity";
    }
    return "unknown";
}

std::string_view NativeSequenceRecoveryApplyDispositionNameV1(
    NativeSequenceRecoveryApplyDispositionV1 disposition) noexcept {
    switch (disposition) {
        case NativeSequenceRecoveryApplyDispositionV1::kApplied:
            return "applied";
        case NativeSequenceRecoveryApplyDispositionV1::kExactDuplicate:
            return "exact_duplicate";
        case NativeSequenceRecoveryApplyDispositionV1::
            kDuplicateOutsideRetention:
            return "duplicate_outside_retention";
        case NativeSequenceRecoveryApplyDispositionV1::
            kPayloadConflict:
            return "payload_conflict";
        case NativeSequenceRecoveryApplyDispositionV1::
            kResourceFrozen:
            return "resource_frozen";
        case NativeSequenceRecoveryApplyDispositionV1::kChannelFrozen:
            return "channel_frozen";
        case NativeSequenceRecoveryApplyDispositionV1::kChannelCapacity:
            return "channel_capacity";
    }
    return "unknown";
}

class NativeSequenceRecoveryCoordinatorV1::Impl final {
public:
    class CanonicalPayloadSlab final {
    public:
        struct FreeRange final {
            std::size_t offset = 0U;
            std::size_t size = 0U;
        };

        CanonicalPayloadSlab(
            std::size_t capacity_bytes,
            std::size_t maximum_allocations)
            : storage_(
                  std::make_unique_for_overwrite<std::byte[]>(
                      capacity_bytes)),
              capacity_bytes_(capacity_bytes),
              free_ranges_(maximum_allocations + 1U) {
            free_ranges_[0] = FreeRange{0U, capacity_bytes_};
            free_range_count_ = 1U;
        }

        CanonicalPayloadSlab(const CanonicalPayloadSlab&) = delete;
        CanonicalPayloadSlab& operator=(
            const CanonicalPayloadSlab&) = delete;

        [[nodiscard]] bool Allocate(
            std::size_t size,
            std::size_t* output_offset) noexcept {
            if (output_offset == nullptr || size == 0U ||
                size > capacity_bytes_) {
                return false;
            }
            for (std::size_t index = 0U;
                 index < free_range_count_;
                 ++index) {
                FreeRange& range = free_ranges_[index];
                if (range.size < size) {
                    continue;
                }
                *output_offset = range.offset;
                range.offset += size;
                range.size -= size;
                if (range.size == 0U) {
                    EraseRange(index);
                }
                return true;
            }
            return false;
        }

        [[nodiscard]] bool Release(
            std::size_t offset,
            std::size_t size) noexcept {
            if (size == 0U || offset > capacity_bytes_ ||
                size > capacity_bytes_ - offset) {
                return false;
            }
            std::size_t position = 0U;
            while (position < free_range_count_ &&
                   free_ranges_[position].offset < offset) {
                ++position;
            }
            if ((position != 0U &&
                 free_ranges_[position - 1U].offset +
                         free_ranges_[position - 1U].size >
                     offset) ||
                (position < free_range_count_ &&
                 offset + size > free_ranges_[position].offset)) {
                return false;
            }
            const bool joins_prior =
                position != 0U &&
                free_ranges_[position - 1U].offset +
                        free_ranges_[position - 1U].size ==
                    offset;
            const bool joins_next =
                position < free_range_count_ &&
                offset + size == free_ranges_[position].offset;
            if (joins_prior && joins_next) {
                FreeRange& prior = free_ranges_[position - 1U];
                prior.size += size + free_ranges_[position].size;
                EraseRange(position);
                return true;
            }
            if (joins_prior) {
                free_ranges_[position - 1U].size += size;
                return true;
            }
            if (joins_next) {
                free_ranges_[position].offset = offset;
                free_ranges_[position].size += size;
                return true;
            }
            if (free_range_count_ >= free_ranges_.size()) {
                return false;
            }
            for (std::size_t index = free_range_count_;
                 index > position;
                 --index) {
                free_ranges_[index] = free_ranges_[index - 1U];
            }
            free_ranges_[position] = FreeRange{offset, size};
            ++free_range_count_;
            return true;
        }

        [[nodiscard]] std::span<std::byte> MutableBytes(
            std::size_t offset,
            std::size_t size) noexcept {
            if (offset > capacity_bytes_ ||
                size > capacity_bytes_ - offset) {
                return {};
            }
            return {storage_.get() + offset, size};
        }

        [[nodiscard]] std::span<const std::byte> Bytes(
            std::size_t offset,
            std::size_t size) const noexcept {
            if (offset > capacity_bytes_ ||
                size > capacity_bytes_ - offset) {
                return {};
            }
            return {storage_.get() + offset, size};
        }

        void ResetFreeTail(std::size_t used_bytes) noexcept {
            if (used_bytes >= capacity_bytes_) {
                free_range_count_ = 0U;
                return;
            }
            free_ranges_[0] = FreeRange{
                used_bytes, capacity_bytes_ - used_bytes};
            free_range_count_ = 1U;
        }

        [[nodiscard]] std::size_t capacity_bytes() const noexcept {
            return capacity_bytes_;
        }

    private:
        void EraseRange(std::size_t position) noexcept {
            for (std::size_t index = position + 1U;
                 index < free_range_count_;
                 ++index) {
                free_ranges_[index - 1U] = free_ranges_[index];
            }
            --free_range_count_;
        }

        std::unique_ptr<std::byte[]> storage_;
        std::size_t capacity_bytes_ = 0U;
        std::vector<FreeRange> free_ranges_;
        std::size_t free_range_count_ = 0U;
    };

    struct Channel final {
        bool occupied = false;
        bool initialized = false;
        bool frozen = false;
        NativeSequenceChannelV1 domain{};
        NativeSequenceRecoveryFreezeReasonV1 freeze_reason =
            NativeSequenceRecoveryFreezeReasonV1::kNone;
        std::uint64_t origin_sequence = 0U;
        std::uint64_t certified_sequence = 0U;
        std::uint64_t observed_contiguous_sequence = 0U;
        std::uint64_t highest_observed_sequence = 0U;
        std::uint64_t unique_sequences = 0U;
        std::uint64_t duplicate_arrivals = 0U;
        std::uint64_t exact_duplicate_applications = 0U;
        std::uint64_t unverified_duplicate_applications = 0U;
        std::uint64_t duplicate_outside_retention = 0U;
        std::uint64_t conflicts = 0U;
        std::uint64_t filtered_sequences_certified = 0U;
        std::uint64_t channel_correction_epoch = 1U;
        std::uint64_t provisional_min_sequence = 0U;
        std::size_t bootstrap_observed_entries = 0U;
        // Number of unique observed positions in
        // (observed_contiguous_sequence, highest_observed_sequence].
        // Maintaining this incrementally keeps gap status O(1), including
        // while a long repair suffix is arriving.
        std::size_t observed_above_contiguous = 0U;
        std::size_t pending_entries = 0U;
    };

    struct Entry final {
        bool occupied = false;
        bool sequence_observed = false;
        bool pending = false;
        bool retained = false;
        std::uint32_t generation = 0U;
        std::uint32_t free_next = kInvalidIndex;
        std::uint32_t hash_next = kInvalidIndex;
        std::uint32_t channel_slot = kInvalidIndex;
        std::uint64_t sequence = 0U;
        l2flow::sdk::MessageKey message_key{};
        NativeSequenceRecoveryRecordClassV1 record_class =
            NativeSequenceRecoveryRecordClassV1::kTarget;
        std::uint64_t observe_calls = 0U;
        std::uint64_t apply_calls = 0U;
        std::uint64_t applied_cookie = 0U;
        bool applied_cookie_owned = false;
        std::size_t canonical_payload_offset = 0U;
        std::size_t canonical_payload_size = 0U;
    };

    struct RetentionReference final {
        std::uint32_t entry_index = kInvalidIndex;
        std::uint32_t generation = 0U;
    };

    Impl(
        NativeSequenceRecoveryConfigV1 config,
        std::size_t channel_table_capacity,
        std::size_t entry_bucket_capacity,
        std::size_t entry_capacity)
        : config_(config),
          channels_(channel_table_capacity),
          entry_buckets_(entry_bucket_capacity, kInvalidIndex),
          entries_(entry_capacity),
          canonical_payload_slab_(
              config.maximum_total_canonical_payload_bytes,
              entry_capacity),
          retention_queue_(
              config.certified_duplicate_retention_entries),
          occupied_channel_slots_(
              config.maximum_channels, kInvalidIndex) {
        for (std::size_t index = 0U; index < entries_.size(); ++index) {
            entries_[index].free_next =
                index + 1U < entries_.size()
                    ? static_cast<std::uint32_t>(index + 1U)
                    : kInvalidIndex;
        }
        if (!entries_.empty()) {
            free_entry_head_ = 0U;
        }
    }

    ~Impl() {
        for (Entry& entry : entries_) {
            if (!entry.applied_cookie_owned) {
                continue;
            }
            if (config_.release_applied_cookie != nullptr) {
                config_.release_applied_cookie(
                    config_.release_applied_cookie_context,
                    entry.applied_cookie);
            }
            entry.applied_cookie_owned = false;
            entry.applied_cookie = 0U;
        }
    }

    [[nodiscard]] std::uint32_t FindChannel(
        const NativeSequenceChannelV1& domain) const noexcept {
        const std::size_t mask = channels_.size() - 1U;
        std::size_t slot = static_cast<std::size_t>(
            Mix64(
                (static_cast<std::uint64_t>(domain.channel) << 8U) |
                static_cast<std::uint64_t>(domain.market))) &
            mask;
        for (std::size_t probe = 0U; probe < channels_.size(); ++probe) {
            const Channel& channel = channels_[slot];
            if (!channel.occupied) {
                return kInvalidIndex;
            }
            if (channel.domain == domain) {
                return static_cast<std::uint32_t>(slot);
            }
            slot = (slot + 1U) & mask;
        }
        return kInvalidIndex;
    }

    [[nodiscard]] std::uint32_t FindOrCreateChannel(
        const NativeSequenceChannelV1& domain) noexcept {
        const std::size_t mask = channels_.size() - 1U;
        std::size_t slot = static_cast<std::size_t>(
            Mix64(
                (static_cast<std::uint64_t>(domain.channel) << 8U) |
                static_cast<std::uint64_t>(domain.market))) &
            mask;
        for (std::size_t probe = 0U; probe < channels_.size(); ++probe) {
            Channel& channel = channels_[slot];
            if (!channel.occupied) {
                if (channel_count_ >= config_.maximum_channels) {
                    IncrementCounter(&channel_capacity_failures_);
                    return kInvalidIndex;
                }
                channel = Channel{};
                channel.occupied = true;
                channel.domain = domain;
                if (config_.origin_policy ==
                    NativeSequenceOriginPolicyV1::kExplicitOrigin) {
                    const std::uint64_t checkpoint =
                        config_.trusted_checkpoint_sequence != 0U
                            ? config_.trusted_checkpoint_sequence
                            : config_.expected_origin_sequence - 1U;
                    channel.initialized = true;
                    channel.origin_sequence =
                        config_.expected_origin_sequence;
                    channel.certified_sequence = checkpoint;
                    channel.observed_contiguous_sequence = checkpoint;
                    channel.highest_observed_sequence = checkpoint;
                }
                occupied_channel_slots_[channel_count_] =
                    static_cast<std::uint32_t>(slot);
                ++channel_count_;
                return static_cast<std::uint32_t>(slot);
            }
            if (channel.domain == domain) {
                return static_cast<std::uint32_t>(slot);
            }
            slot = (slot + 1U) & mask;
        }
        IncrementCounter(&channel_capacity_failures_);
        return kInvalidIndex;
    }

    [[nodiscard]] NativeSequenceRecoveryChannelStateV1 ChannelState(
        const Channel& channel) const noexcept {
        if (channel.frozen) {
            if (channel.freeze_reason ==
                NativeSequenceRecoveryFreezeReasonV1::
                    kPayloadConflict) {
                return NativeSequenceRecoveryChannelStateV1::
                    kFrozenConflict;
            }
            return NativeSequenceRecoveryChannelStateV1::
                kFrozenResource;
        }
        if (!channel.initialized) {
            return NativeSequenceRecoveryChannelStateV1::
                kBootstrapping;
        }
        if (channel.observed_contiguous_sequence <
            channel.highest_observed_sequence) {
            return NativeSequenceRecoveryChannelStateV1::kRepairing;
        }
        if (channel.certified_sequence <
            channel.highest_observed_sequence) {
            return NativeSequenceRecoveryChannelStateV1::kCatchingUp;
        }
        return NativeSequenceRecoveryChannelStateV1::kHealthy;
    }

    [[nodiscard]] std::uint32_t FindEntry(
        std::uint32_t channel_slot,
        std::uint64_t sequence) const noexcept {
        const std::size_t bucket = EntryBucket(channel_slot, sequence);
        std::uint32_t index = entry_buckets_[bucket];
        std::size_t traversed = 0U;
        while (index != kInvalidIndex && traversed < entries_.size()) {
            const Entry& entry = entries_[index];
            if (entry.occupied &&
                entry.channel_slot == channel_slot &&
                entry.sequence == sequence) {
                return index;
            }
            index = entry.hash_next;
            ++traversed;
        }
        return kInvalidIndex;
    }

    [[nodiscard]] std::size_t EntryBucket(
        std::uint32_t channel_slot,
        std::uint64_t sequence) const noexcept {
        const auto combined =
            sequence ^
            (static_cast<std::uint64_t>(channel_slot) *
             UINT64_C(0x9e3779b97f4a7c15));
        return static_cast<std::size_t>(Mix64(combined)) &
               (entry_buckets_.size() - 1U);
    }

    void InsertEntryIntoHash(std::uint32_t entry_index) noexcept {
        Entry& entry = entries_[entry_index];
        const std::size_t bucket =
            EntryBucket(entry.channel_slot, entry.sequence);
        entry.hash_next = entry_buckets_[bucket];
        entry_buckets_[bucket] = entry_index;
    }

    [[nodiscard]] bool RemoveEntryFromHash(
        std::uint32_t entry_index) noexcept {
        Entry& entry = entries_[entry_index];
        const std::size_t bucket =
            EntryBucket(entry.channel_slot, entry.sequence);
        std::uint32_t* link = &entry_buckets_[bucket];
        std::size_t traversed = 0U;
        while (*link != kInvalidIndex &&
               traversed < entries_.size()) {
            if (*link == entry_index) {
                *link = entries_[*link].hash_next;
                return true;
            }
            link = &entries_[*link].hash_next;
            ++traversed;
        }
        return false;
    }

    [[nodiscard]] std::uint32_t AllocateEntry(
        std::uint32_t channel_slot,
        std::uint64_t sequence,
        const l2flow::sdk::MessageKey& message_key,
        NativeSequenceRecoveryRecordClassV1 record_class) noexcept {
        if (free_entry_head_ == kInvalidIndex) {
            return kInvalidIndex;
        }
        const std::uint32_t index = free_entry_head_;
        Entry& entry = entries_[index];
        free_entry_head_ = entry.free_next;

        std::uint32_t generation = entry.generation + 1U;
        if (generation == 0U) {
            generation = 1U;
        }
        entry = Entry{};
        entry.occupied = true;
        entry.pending = true;
        entry.generation = generation;
        entry.channel_slot = channel_slot;
        entry.sequence = sequence;
        entry.message_key = message_key;
        entry.record_class = record_class;
        InsertEntryIntoHash(index);

        ++pending_entries_;
        ++channels_[channel_slot].pending_entries;
        return index;
    }

    void ReleaseEntry(std::uint32_t entry_index) noexcept {
        Entry& entry = entries_[entry_index];
        if (!entry.occupied) {
            return;
        }
        if (entry.applied_cookie_owned) {
            if (config_.release_applied_cookie != nullptr) {
                config_.release_applied_cookie(
                    config_.release_applied_cookie_context,
                    entry.applied_cookie);
            }
            entry.applied_cookie_owned = false;
            entry.applied_cookie = 0U;
        }
        const std::uint32_t channel_slot = entry.channel_slot;
        if (entry.pending) {
            if (pending_entries_ > 0U) {
                --pending_entries_;
            }
            if (channel_slot < channels_.size() &&
                channels_[channel_slot].pending_entries > 0U) {
                --channels_[channel_slot].pending_entries;
            }
        }
        if (entry.retained && retained_entries_ > 0U) {
            --retained_entries_;
        }
        if (entry.canonical_payload_size <=
            canonical_payload_bytes_) {
            canonical_payload_bytes_ -=
                entry.canonical_payload_size;
        } else {
            canonical_payload_bytes_ = 0U;
            canonical_payload_storage_corrupt_ = true;
        }
        if (entry.canonical_payload_size != 0U &&
            !canonical_payload_slab_.Release(
                entry.canonical_payload_offset,
                entry.canonical_payload_size)) {
            canonical_payload_storage_corrupt_ = true;
        }
        const std::uint32_t generation = entry.generation;
        static_cast<void>(RemoveEntryFromHash(entry_index));
        entry = Entry{};
        entry.generation = generation;
        entry.free_next = free_entry_head_;
        free_entry_head_ = entry_index;
    }

    [[nodiscard]] bool ReferenceIsLiveRetained(
        const RetentionReference& reference) const noexcept {
        if (reference.entry_index >= entries_.size()) {
            return false;
        }
        const Entry& entry = entries_[reference.entry_index];
        return entry.occupied && entry.retained &&
               entry.generation == reference.generation;
    }

    [[nodiscard]] bool RetainedEntryPinned(
        const Entry& entry) const noexcept {
        return entry.record_class ==
                   NativeSequenceRecoveryRecordClassV1::kTarget &&
               DuplicateVerificationGap(entry) != 0U;
    }

    [[nodiscard]] std::uint64_t DuplicateVerificationGap(
        const Entry& entry) const noexcept {
        if (entry.record_class !=
                NativeSequenceRecoveryRecordClassV1::kTarget ||
            entry.observe_calls <= 1U) {
            return 0U;
        }
        const std::uint64_t checked_arrivals =
            std::max<std::uint64_t>(entry.apply_calls, 1U);
        return entry.observe_calls > checked_arrivals
                   ? entry.observe_calls - checked_arrivals
                   : 0U;
    }

    [[nodiscard]] bool HasBlockingDuplicateVerification(
        std::uint32_t channel_slot,
        std::uint64_t through_sequence) const noexcept {
        const Channel& channel = channels_[channel_slot];
        if (channel.unverified_duplicate_applications == 0U) {
            return false;
        }
        for (const Entry& entry : entries_) {
            if (entry.occupied &&
                entry.channel_slot == channel_slot &&
                entry.sequence <= through_sequence &&
                DuplicateVerificationGap(entry) != 0U) {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] RetentionReference PopRetentionFront() noexcept {
        RetentionReference output{};
        if (retention_queue_count_ == 0U) {
            return output;
        }
        output = retention_queue_[retention_queue_head_];
        retention_queue_head_ =
            (retention_queue_head_ + 1U) %
            retention_queue_.size();
        --retention_queue_count_;
        return output;
    }

    void PushRetentionBack(
        RetentionReference reference) noexcept {
        const std::size_t position =
            (retention_queue_head_ + retention_queue_count_) %
            retention_queue_.size();
        retention_queue_[position] = reference;
        ++retention_queue_count_;
    }

    [[nodiscard]] bool MakeRetentionQueueSlot() noexcept {
        if (retention_queue_count_ < retention_queue_.size()) {
            return true;
        }
        const std::size_t attempts = retention_queue_count_;
        for (std::size_t attempt = 0U;
             attempt < attempts;
             ++attempt) {
            const RetentionReference reference =
                PopRetentionFront();
            if (!ReferenceIsLiveRetained(reference)) {
                return true;
            }
            Entry& candidate = entries_[reference.entry_index];
            if (RetainedEntryPinned(candidate)) {
                PushRetentionBack(reference);
                continue;
            }
            ReleaseEntry(reference.entry_index);
            return true;
        }
        return retention_queue_count_ <
               retention_queue_.size();
    }

    void FinishPendingAndRetain(
        std::uint32_t entry_index) noexcept {
        Entry& entry = entries_[entry_index];
        Channel& channel = channels_[entry.channel_slot];
        if (entry.pending) {
            entry.pending = false;
            if (pending_entries_ > 0U) {
                --pending_entries_;
            }
            if (channel.pending_entries > 0U) {
                --channel.pending_entries;
            }
        }
        if (retention_queue_.empty()) {
            ReleaseEntry(entry_index);
            return;
        }
        if (!MakeRetentionQueueSlot()) {
            // Every retained entry has an observed target duplicate whose
            // applied payload has not arrived yet. Preserve those proofs and
            // release this newly certified, currently unpinned entry.
            ReleaseEntry(entry_index);
            return;
        }
        entry.retained = true;
        ++retained_entries_;
        PushRetentionBack(
            RetentionReference{entry_index, entry.generation});
    }

    void FreezeChannel(
        std::uint32_t channel_slot,
        NativeSequenceRecoveryFreezeReasonV1 reason) noexcept {
        Channel& channel = channels_[channel_slot];
        if (channel.frozen) {
            return;
        }
        channel.frozen = true;
        channel.freeze_reason = reason;
        ++frozen_channel_count_;
        for (std::size_t index = 0U; index < entries_.size(); ++index) {
            if (entries_[index].occupied &&
                entries_[index].channel_slot == channel_slot) {
                ReleaseEntry(static_cast<std::uint32_t>(index));
            }
        }
    }

    [[nodiscard]] bool UnseenSequenceWithinBackwardBound(
        std::uint32_t channel_slot,
        std::uint64_t sequence) noexcept {
        Channel& channel = channels_[channel_slot];
        if (config_.origin_policy !=
                NativeSequenceOriginPolicyV1::kBoundedProcessStart ||
            channel.highest_observed_sequence == 0U ||
            sequence >= channel.highest_observed_sequence) {
            return true;
        }
        const std::uint64_t displacement =
            channel.highest_observed_sequence - sequence;
        if (displacement <=
            config_.maximum_backward_displacement) {
            return true;
        }
        FreezeChannel(
            channel_slot,
            NativeSequenceRecoveryFreezeReasonV1::
                kReorderBoundExceeded);
        return false;
    }

    [[nodiscard]] bool EstablishBootstrapOrigin(
        std::uint32_t channel_slot) noexcept {
        Channel& channel = channels_[channel_slot];
        if (channel.initialized ||
            channel.provisional_min_sequence == 0U ||
            channel.highest_observed_sequence <
                channel.provisional_min_sequence ||
            channel.bootstrap_observed_entries == 0U) {
            return false;
        }
        channel.initialized = true;
        channel.origin_sequence =
            channel.provisional_min_sequence;
        channel.certified_sequence =
            channel.origin_sequence - 1U;
        channel.observed_contiguous_sequence =
            channel.origin_sequence - 1U;
        channel.observed_above_contiguous =
            channel.bootstrap_observed_entries;

        while (channel.observed_contiguous_sequence <
               channel.highest_observed_sequence) {
            const std::uint64_t next_sequence =
                channel.observed_contiguous_sequence + 1U;
            const std::uint32_t next_index =
                FindEntry(channel_slot, next_sequence);
            if (next_index == kInvalidIndex ||
                !entries_[next_index].sequence_observed) {
                break;
            }
            if (channel.observed_above_contiguous == 0U) {
                FreezeChannel(
                    channel_slot,
                    NativeSequenceRecoveryFreezeReasonV1::
                        kInternalInvariant);
                return false;
            }
            --channel.observed_above_contiguous;
            ++channel.observed_contiguous_sequence;
        }
        return true;
    }

    [[nodiscard]] bool EnforceEstablishedGapBound(
        std::uint32_t channel_slot) noexcept {
        Channel& channel = channels_[channel_slot];
        if (config_.origin_policy !=
                NativeSequenceOriginPolicyV1::kBoundedProcessStart ||
            !channel.initialized || channel.frozen ||
            channel.observed_contiguous_sequence >=
                channel.highest_observed_sequence) {
            return !channel.frozen;
        }
        const std::uint64_t expected_sequence =
            channel.observed_contiguous_sequence + 1U;
        if (channel.highest_observed_sequence <= expected_sequence ||
            channel.highest_observed_sequence - expected_sequence <=
                config_.maximum_backward_displacement) {
            return true;
        }
        FreezeChannel(
            channel_slot,
            NativeSequenceRecoveryFreezeReasonV1::
                kReorderBoundExceeded);
        return false;
    }

    [[nodiscard]] bool SequenceFitsWindow(
        const Channel& channel,
        std::uint64_t sequence) const noexcept {
        if (!channel.initialized ||
            sequence <= channel.certified_sequence) {
            return true;
        }
        return sequence - channel.certified_sequence <=
               config_.maximum_reorder_span;
    }

    [[nodiscard]] NativeSequenceRecoveryObserveDispositionV1
    ClassifyNewObservedSequence(
        const Channel& channel,
        std::uint64_t sequence) const noexcept {
        if (!channel.initialized) {
            return NativeSequenceRecoveryObserveDispositionV1::
                kAccepted;
        }
        if (channel.observed_contiguous_sequence <
            channel.highest_observed_sequence) {
            if (sequence <= channel.highest_observed_sequence) {
                return NativeSequenceRecoveryObserveDispositionV1::
                    kBackfill;
            }
            return NativeSequenceRecoveryObserveDispositionV1::
                kGapExtended;
        }
        if (sequence ==
            channel.highest_observed_sequence + 1U) {
            return NativeSequenceRecoveryObserveDispositionV1::
                kAccepted;
        }
        return NativeSequenceRecoveryObserveDispositionV1::
            kGapOpened;
    }

    [[nodiscard]] bool RegisterObservedSequence(
        std::uint32_t channel_slot,
        std::uint32_t entry_index) noexcept {
        Channel& channel = channels_[channel_slot];
        Entry& entry = entries_[entry_index];
        entry.sequence_observed = true;
        ++entry.observe_calls;
        IncrementCounter(&channel.unique_sequences);

        if (!channel.initialized) {
            if (channel.bootstrap_observed_entries ==
                std::numeric_limits<std::size_t>::max()) {
                FreezeChannel(
                    channel_slot,
                    NativeSequenceRecoveryFreezeReasonV1::
                        kInternalInvariant);
                return false;
            }
            ++channel.bootstrap_observed_entries;
            if (channel.provisional_min_sequence == 0U ||
                entry.sequence < channel.provisional_min_sequence) {
                channel.provisional_min_sequence = entry.sequence;
            }
            if (entry.sequence > channel.highest_observed_sequence) {
                channel.highest_observed_sequence = entry.sequence;
            }
            const std::uint64_t observed_span =
                channel.highest_observed_sequence -
                channel.provisional_min_sequence;
            if (observed_span <
                config_.maximum_backward_displacement) {
                return true;
            }
            if (!EstablishBootstrapOrigin(channel_slot)) {
                return false;
            }
            if (channel.highest_observed_sequence -
                    channel.certified_sequence >
                config_.maximum_reorder_span) {
                FreezeChannel(
                    channel_slot,
                    NativeSequenceRecoveryFreezeReasonV1::
                        kReorderWindowExceeded);
                return false;
            }
            return EnforceEstablishedGapBound(channel_slot);
        }
        const bool gap_before =
            channel.observed_contiguous_sequence <
            channel.highest_observed_sequence;
        if (entry.sequence > channel.highest_observed_sequence) {
            channel.highest_observed_sequence = entry.sequence;
        }
        if (entry.sequence ==
            channel.observed_contiguous_sequence + 1U) {
            ++channel.observed_contiguous_sequence;
            while (channel.observed_contiguous_sequence <
                   channel.highest_observed_sequence) {
                const std::uint64_t next_sequence =
                    channel.observed_contiguous_sequence + 1U;
                const std::uint32_t next_index =
                    FindEntry(channel_slot, next_sequence);
                if (next_index == kInvalidIndex ||
                    !entries_[next_index].sequence_observed) {
                    break;
                }
                if (channel.observed_above_contiguous == 0U) {
                    FreezeChannel(
                        channel_slot,
                        NativeSequenceRecoveryFreezeReasonV1::
                            kInternalInvariant);
                    return false;
                }
                --channel.observed_above_contiguous;
                ++channel.observed_contiguous_sequence;
            }
        } else if (
            entry.sequence >
            channel.observed_contiguous_sequence) {
            if (channel.observed_above_contiguous ==
                std::numeric_limits<std::size_t>::max()) {
                FreezeChannel(
                    channel_slot,
                    NativeSequenceRecoveryFreezeReasonV1::
                        kInternalInvariant);
                return false;
            }
            ++channel.observed_above_contiguous;
        }
        if (gap_before &&
            channel.observed_contiguous_sequence ==
                channel.highest_observed_sequence) {
            if (channel.channel_correction_epoch ==
                std::numeric_limits<std::uint64_t>::max()) {
                FreezeChannel(
                    channel_slot,
                    NativeSequenceRecoveryFreezeReasonV1::
                        kInternalInvariant);
                return false;
            }
            ++channel.channel_correction_epoch;
        }
        return EnforceEstablishedGapBound(channel_slot);
    }

    [[nodiscard]] NativeSequenceRecoveryTokenV1 TokenFor(
        std::uint32_t entry_index) const noexcept {
        const Entry& entry = entries_[entry_index];
        NativeSequenceRecoveryTokenV1 token{};
        token.channel_slot = entry.channel_slot;
        token.entry_index = entry_index;
        token.entry_generation = entry.generation;
        token.sequence = entry.sequence;
        return token;
    }

    void FillObserveResult(
        const Channel& channel,
        NativeSequenceRecoveryObserveDispositionV1 disposition,
        NativeSequenceRecoveryTokenV1 token,
        NativeSequenceRecoveryObserveResultV1* output) const noexcept {
        output->disposition = disposition;
        output->token = token;
        output->channel_state = ChannelState(channel);
        output->origin_sequence = channel.origin_sequence;
        output->certified_sequence = channel.certified_sequence;
        output->observed_contiguous_sequence =
            channel.observed_contiguous_sequence;
        output->highest_observed_sequence =
            channel.highest_observed_sequence;
    }

    void FillApplyResult(
        const Channel& channel,
        const Entry* entry,
        NativeSequenceRecoveryApplyDispositionV1 disposition,
        NativeSequenceRecoveryApplyResultV1* output) const noexcept {
        output->disposition = disposition;
        output->channel_state = ChannelState(channel);
        output->freeze_reason = channel.freeze_reason;
        if (entry != nullptr) {
            output->canonical_applied_cookie =
                entry->applied_cookie;
            output->observed_arrivals = entry->observe_calls;
            output->applied_arrivals = entry->apply_calls;
        }
    }

    [[nodiscard]] bool HasEntryCapacity(
        const Channel& channel) const noexcept {
        return pending_entries_ <
                   config_.maximum_pending_entries &&
               channel.pending_entries <
                   config_.maximum_pending_entries_per_channel &&
               free_entry_head_ != kInvalidIndex;
    }

    [[nodiscard]] NativeSequenceRecoveryFreezeReasonV1
    EntryCapacityReason(const Channel& channel) const noexcept {
        if (channel.pending_entries >=
            config_.maximum_pending_entries_per_channel) {
            return NativeSequenceRecoveryFreezeReasonV1::
                kPerChannelEntryCapacity;
        }
        return NativeSequenceRecoveryFreezeReasonV1::
            kPendingEntryCapacity;
    }

    [[nodiscard]] NativeSequenceRecoveryApplyErrorV1
    StoreApplication(
        std::uint32_t channel_slot,
        std::uint32_t entry_index,
        const l2flow::sdk::MessageKey& message_key,
        std::span<const std::byte> canonical_payload,
        std::uint64_t applied_cookie,
        NativeSequenceRecoveryApplyResultV1* output) noexcept {
        Channel& channel = channels_[channel_slot];
        Entry& entry = entries_[entry_index];

        if (!(entry.message_key == message_key) ||
            entry.record_class !=
                NativeSequenceRecoveryRecordClassV1::kTarget) {
            IncrementCounter(&channel.conflicts);
            FreezeChannel(
                channel_slot,
                NativeSequenceRecoveryFreezeReasonV1::
                    kPayloadConflict);
            FillApplyResult(
                channel,
                nullptr,
                NativeSequenceRecoveryApplyDispositionV1::
                    kPayloadConflict,
                output);
            return NativeSequenceRecoveryApplyErrorV1::kNone;
        }
        if (entry.apply_calls ==
            std::numeric_limits<std::uint64_t>::max()) {
            FreezeChannel(
                channel_slot,
                NativeSequenceRecoveryFreezeReasonV1::
                    kInternalInvariant);
            FillApplyResult(
                channel,
                nullptr,
                NativeSequenceRecoveryApplyDispositionV1::
                    kResourceFrozen,
                output);
            return NativeSequenceRecoveryApplyErrorV1::kNone;
        }

        if (entry.apply_calls == 0U) {
            if (canonical_payload.size() >
                    config_.
                        maximum_canonical_payload_bytes_per_entry ||
                canonical_payload_bytes_ >
                    config_.maximum_total_canonical_payload_bytes ||
                canonical_payload.size() >
                    config_.maximum_total_canonical_payload_bytes -
                        canonical_payload_bytes_) {
                FreezeChannel(
                    channel_slot,
                    NativeSequenceRecoveryFreezeReasonV1::
                        kPayloadCapacity);
                FillApplyResult(
                    channel,
                    nullptr,
                    NativeSequenceRecoveryApplyDispositionV1::
                        kResourceFrozen,
                    output);
                return NativeSequenceRecoveryApplyErrorV1::kNone;
            }
            std::size_t payload_offset = 0U;
            if (canonical_payload_storage_corrupt_ ||
                !AllocateCanonicalPayload(
                    canonical_payload.size(), &payload_offset)) {
                FreezeChannel(
                    channel_slot,
                    canonical_payload_storage_corrupt_
                        ? NativeSequenceRecoveryFreezeReasonV1::
                              kInternalInvariant
                        : NativeSequenceRecoveryFreezeReasonV1::
                              kPayloadCapacity);
                FillApplyResult(
                    channel,
                    nullptr,
                    NativeSequenceRecoveryApplyDispositionV1::
                        kResourceFrozen,
                    output);
                return NativeSequenceRecoveryApplyErrorV1::kNone;
            }
            std::span<std::byte> destination =
                canonical_payload_slab_.MutableBytes(
                    payload_offset, canonical_payload.size());
            if (destination.size() != canonical_payload.size()) {
                canonical_payload_storage_corrupt_ = true;
                FreezeChannel(
                    channel_slot,
                    NativeSequenceRecoveryFreezeReasonV1::
                        kInternalInvariant);
                FillApplyResult(
                    channel,
                    nullptr,
                    NativeSequenceRecoveryApplyDispositionV1::
                        kResourceFrozen,
                    output);
                return NativeSequenceRecoveryApplyErrorV1::kNone;
            }
            std::copy(
                canonical_payload.begin(),
                canonical_payload.end(),
                destination.begin());
            entry.canonical_payload_offset = payload_offset;
            entry.canonical_payload_size = canonical_payload.size();
            canonical_payload_bytes_ += canonical_payload.size();
            entry.applied_cookie = applied_cookie;
            entry.applied_cookie_owned = true;
            const std::uint64_t gap_before =
                DuplicateVerificationGap(entry);
            ++entry.apply_calls;
            const std::uint64_t gap_after =
                DuplicateVerificationGap(entry);
            if (gap_after <= gap_before &&
                gap_before - gap_after <=
                    channel.unverified_duplicate_applications) {
                channel.unverified_duplicate_applications -=
                    gap_before - gap_after;
            } else {
                FreezeChannel(
                    channel_slot,
                    NativeSequenceRecoveryFreezeReasonV1::
                        kInternalInvariant);
                FillApplyResult(
                    channel,
                    nullptr,
                    NativeSequenceRecoveryApplyDispositionV1::
                        kResourceFrozen,
                    output);
                return NativeSequenceRecoveryApplyErrorV1::kNone;
            }
            FillApplyResult(
                channel,
                &entry,
                NativeSequenceRecoveryApplyDispositionV1::kApplied,
                output);
            return NativeSequenceRecoveryApplyErrorV1::kNone;
        }

        const std::span<const std::byte> stored_payload =
            canonical_payload_slab_.Bytes(
                entry.canonical_payload_offset,
                entry.canonical_payload_size);
        const bool exact =
            stored_payload.size() == canonical_payload.size() &&
            std::equal(
                stored_payload.begin(),
                stored_payload.end(),
                canonical_payload.begin());
        if (!exact) {
            IncrementCounter(&channel.conflicts);
            FreezeChannel(
                channel_slot,
                NativeSequenceRecoveryFreezeReasonV1::
                    kPayloadConflict);
            FillApplyResult(
                channel,
                nullptr,
                NativeSequenceRecoveryApplyDispositionV1::
                    kPayloadConflict,
                output);
            return NativeSequenceRecoveryApplyErrorV1::kNone;
        }
        const std::uint64_t gap_before =
            DuplicateVerificationGap(entry);
        ++entry.apply_calls;
        const std::uint64_t gap_after =
            DuplicateVerificationGap(entry);
        if (gap_after <= gap_before &&
            gap_before - gap_after <=
                channel.unverified_duplicate_applications) {
            channel.unverified_duplicate_applications -=
                gap_before - gap_after;
        } else {
            FreezeChannel(
                channel_slot,
                NativeSequenceRecoveryFreezeReasonV1::
                    kInternalInvariant);
            FillApplyResult(
                channel,
                nullptr,
                NativeSequenceRecoveryApplyDispositionV1::
                    kResourceFrozen,
                output);
            return NativeSequenceRecoveryApplyErrorV1::kNone;
        }
        IncrementCounter(
            &channel.exact_duplicate_applications);
        FillApplyResult(
            channel,
            &entry,
            NativeSequenceRecoveryApplyDispositionV1::
                kExactDuplicate,
            output);
        return NativeSequenceRecoveryApplyErrorV1::kNone;
    }

    [[nodiscard]] bool AllocateCanonicalPayload(
        std::size_t size,
        std::size_t* output_offset) noexcept {
        if (canonical_payload_slab_.Allocate(size, output_offset)) {
            return true;
        }
        if (size > canonical_payload_slab_.capacity_bytes() ||
            canonical_payload_bytes_ >
                canonical_payload_slab_.capacity_bytes() - size) {
            return false;
        }

        // Variable-size generic payloads can fragment the fixed backing
        // store. Compact only on that cold failure path. The coordinator is
        // single-owner, so no reader can retain a payload span while offsets
        // move. Production's fixed 336-byte payload never needs this path.
        std::size_t next_offset = 0U;
        std::size_t source_cursor = 0U;
        for (;;) {
            Entry* selected = nullptr;
            std::size_t selected_offset =
                canonical_payload_slab_.capacity_bytes();
            for (Entry& candidate : entries_) {
                if (!candidate.occupied ||
                    candidate.canonical_payload_size == 0U ||
                    candidate.canonical_payload_offset < source_cursor ||
                    candidate.canonical_payload_offset >=
                        selected_offset) {
                    continue;
                }
                selected = &candidate;
                selected_offset = candidate.canonical_payload_offset;
            }
            if (selected == nullptr) {
                break;
            }
            Entry& candidate = *selected;
            const std::span<const std::byte> source =
                canonical_payload_slab_.Bytes(
                    selected_offset,
                    candidate.canonical_payload_size);
            std::span<std::byte> destination =
                canonical_payload_slab_.MutableBytes(
                    next_offset,
                    candidate.canonical_payload_size);
            if (source.size() != candidate.canonical_payload_size ||
                destination.size() !=
                    candidate.canonical_payload_size) {
                canonical_payload_storage_corrupt_ = true;
                return false;
            }
            if (selected_offset != next_offset) {
                std::memmove(
                    destination.data(),
                    source.data(),
                    candidate.canonical_payload_size);
                candidate.canonical_payload_offset = next_offset;
            }
            source_cursor = selected_offset +
                            candidate.canonical_payload_size;
            next_offset += candidate.canonical_payload_size;
        }
        if (next_offset != canonical_payload_bytes_) {
            canonical_payload_storage_corrupt_ = true;
            return false;
        }
        canonical_payload_slab_.ResetFreeTail(next_offset);
        return canonical_payload_slab_.Allocate(size, output_offset);
    }

    NativeSequenceRecoveryConfigV1 config_{};
    std::vector<Channel> channels_;
    std::vector<std::uint32_t> entry_buckets_;
    std::vector<Entry> entries_;
    CanonicalPayloadSlab canonical_payload_slab_;
    std::vector<RetentionReference> retention_queue_;
    // Dense list of occupied open-addressing slots. Polling readiness over
    // actual channels, rather than the capacity-sized hash table, keeps the
    // normal one/few-channel cost independent of maximum_channels.
    std::vector<std::uint32_t> occupied_channel_slots_;
    std::uint32_t free_entry_head_ = kInvalidIndex;
    std::size_t retention_queue_head_ = 0U;
    std::size_t retention_queue_count_ = 0U;
    std::size_t poll_cursor_ = 0U;
    std::size_t channel_count_ = 0U;
    std::size_t frozen_channel_count_ = 0U;
    std::size_t pending_entries_ = 0U;
    std::size_t retained_entries_ = 0U;
    std::size_t canonical_payload_bytes_ = 0U;
    std::uint64_t channel_capacity_failures_ = 0U;
    std::uint64_t invalid_observations_ = 0U;
    bool canonical_payload_storage_corrupt_ = false;
    bool input_sealed_ = false;
};

NativeSequenceRecoveryCoordinatorV1::
    NativeSequenceRecoveryCoordinatorV1(
        std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

NativeSequenceRecoveryCoordinatorV1::
    ~NativeSequenceRecoveryCoordinatorV1() = default;

NativeSequenceRecoveryCreateErrorV1
NativeSequenceRecoveryCoordinatorV1::Create(
    NativeSequenceRecoveryConfigV1 config,
    std::unique_ptr<NativeSequenceRecoveryCoordinatorV1>* output)
    noexcept {
    if (output == nullptr) {
        return NativeSequenceRecoveryCreateErrorV1::kNullOutput;
    }
    output->reset();

    constexpr std::uint64_t maximum_native_sequence =
        static_cast<std::uint64_t>(
            std::numeric_limits<std::int64_t>::max());
    const bool explicit_origin =
        config.origin_policy ==
        NativeSequenceOriginPolicyV1::kExplicitOrigin;
    const bool bounded_process_start =
        config.origin_policy ==
        NativeSequenceOriginPolicyV1::kBoundedProcessStart;
    const bool invalid_explicit_baseline =
        explicit_origin &&
        (config.expected_origin_sequence == 0U ||
         config.expected_origin_sequence > maximum_native_sequence ||
         config.trusted_checkpoint_sequence >= maximum_native_sequence ||
         (config.trusted_checkpoint_sequence != 0U &&
          config.trusted_checkpoint_sequence <
              config.expected_origin_sequence - 1U));
    const bool invalid_process_start =
        bounded_process_start &&
        (config.expected_origin_sequence != 1U ||
         config.trusted_checkpoint_sequence != 0U ||
         config.maximum_backward_displacement >=
             config.maximum_reorder_span ||
         config.maximum_backward_displacement >=
             static_cast<std::uint64_t>(
                 config.maximum_pending_entries_per_channel));
    if (config.maximum_channels == 0U ||
        config.maximum_pending_entries == 0U ||
        config.maximum_pending_entries_per_channel == 0U ||
        config.maximum_pending_entries_per_channel >
            config.maximum_pending_entries ||
        config.maximum_canonical_payload_bytes_per_entry == 0U ||
        config.maximum_total_canonical_payload_bytes == 0U ||
        config.maximum_canonical_payload_bytes_per_entry >
            config.maximum_total_canonical_payload_bytes ||
        config.maximum_reorder_span == 0U ||
        !IsValidOriginPolicy(config.origin_policy) ||
        invalid_explicit_baseline ||
        invalid_process_start ||
        (explicit_origin &&
         config.maximum_backward_displacement != 0U) ||
        config.certified_duplicate_retention_entries >
            std::numeric_limits<std::size_t>::max() -
                config.maximum_pending_entries) {
        return NativeSequenceRecoveryCreateErrorV1::
            kInvalidConfiguration;
    }

    const std::size_t entry_capacity =
        config.maximum_pending_entries +
        config.certified_duplicate_retention_entries;
    if (entry_capacity == 0U ||
        entry_capacity >=
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max())) {
        return NativeSequenceRecoveryCreateErrorV1::
            kInvalidConfiguration;
    }
    std::size_t channel_table_capacity = 0U;
    std::size_t entry_bucket_capacity = 0U;
    if (!ComputeTableCapacity(
            config.maximum_channels,
            &channel_table_capacity) ||
        !ComputeTableCapacity(
            entry_capacity,
            &entry_bucket_capacity)) {
        return NativeSequenceRecoveryCreateErrorV1::
            kInvalidConfiguration;
    }

    try {
        auto impl = std::make_unique<Impl>(
            config,
            channel_table_capacity,
            entry_bucket_capacity,
            entry_capacity);
        auto coordinator =
            std::unique_ptr<NativeSequenceRecoveryCoordinatorV1>(
                new NativeSequenceRecoveryCoordinatorV1(
                    std::move(impl)));
        *output = std::move(coordinator);
    } catch (...) {
        return NativeSequenceRecoveryCreateErrorV1::
            kResourceExhausted;
    }
    return NativeSequenceRecoveryCreateErrorV1::kNone;
}

NativeSequenceRecoveryRegisterOriginErrorV1
NativeSequenceRecoveryCoordinatorV1::RegisterTrustedOrigin(
    const NativeSequenceChannelV1& domain,
    std::uint64_t origin,
    std::uint64_t checkpoint) noexcept {
    constexpr std::uint64_t maximum_native_sequence =
        static_cast<std::uint64_t>(
            std::numeric_limits<std::int64_t>::max());
    if (!IsValidChannelDomain(domain) || origin == 0U ||
        origin > maximum_native_sequence ||
        checkpoint >= maximum_native_sequence) {
        return NativeSequenceRecoveryRegisterOriginErrorV1::
            kInvalidInput;
    }
    const std::uint64_t effective_checkpoint =
        checkpoint == 0U ? origin - 1U : checkpoint;
    if (effective_checkpoint < origin - 1U) {
        return NativeSequenceRecoveryRegisterOriginErrorV1::
            kInvalidInput;
    }
    if (impl_->input_sealed_) {
        return NativeSequenceRecoveryRegisterOriginErrorV1::
            kInputSealed;
    }
    if (impl_->FindChannel(domain) != kInvalidIndex) {
        return NativeSequenceRecoveryRegisterOriginErrorV1::
            kChannelAlreadyObserved;
    }
    const std::uint32_t channel_slot =
        impl_->FindOrCreateChannel(domain);
    if (channel_slot == kInvalidIndex) {
        return NativeSequenceRecoveryRegisterOriginErrorV1::
            kChannelCapacity;
    }
    NativeSequenceRecoveryCoordinatorV1::Impl::Channel& channel =
        impl_->channels_[channel_slot];
    channel.initialized = true;
    channel.origin_sequence = origin;
    channel.certified_sequence = effective_checkpoint;
    channel.observed_contiguous_sequence = effective_checkpoint;
    channel.highest_observed_sequence = effective_checkpoint;
    return NativeSequenceRecoveryRegisterOriginErrorV1::kNone;
}

NativeSequenceRecoveryObserveErrorV1
NativeSequenceRecoveryCoordinatorV1::Observe(
    const NativeSequenceDescriptorV1& descriptor,
    const l2flow::sdk::MessageKey& message_key,
    NativeSequenceRecoveryRecordClassV1 record_class,
    NativeSequenceRecoveryObserveResultV1* output) noexcept {
    if (output == nullptr) {
        IncrementCounter(&impl_->invalid_observations_);
        return NativeSequenceRecoveryObserveErrorV1::kNullOutput;
    }
    *output = NativeSequenceRecoveryObserveResultV1{};
    if (!IsValidDescriptor(descriptor) ||
        !IsValidMessageKeyForDomain(
            descriptor.domain, message_key) ||
        !IsValidRecordClass(record_class)) {
        IncrementCounter(&impl_->invalid_observations_);
        return NativeSequenceRecoveryObserveErrorV1::kInvalidInput;
    }
    if (impl_->input_sealed_) {
        return NativeSequenceRecoveryObserveErrorV1::kInputSealed;
    }

    const std::uint32_t channel_slot =
        impl_->FindOrCreateChannel(descriptor.domain);
    if (channel_slot == kInvalidIndex) {
        output->disposition =
            NativeSequenceRecoveryObserveDispositionV1::
                kChannelCapacity;
        return NativeSequenceRecoveryObserveErrorV1::kNone;
    }
    Impl::Channel& channel = impl_->channels_[channel_slot];
    if (channel.frozen) {
        impl_->FillObserveResult(
            channel,
            NativeSequenceRecoveryObserveDispositionV1::
                kChannelFrozen,
            NativeSequenceRecoveryTokenV1{},
            output);
        return NativeSequenceRecoveryObserveErrorV1::kNone;
    }

    const std::uint32_t existing_index =
        impl_->FindEntry(channel_slot, descriptor.sequence);
    if (existing_index != kInvalidIndex) {
        Impl::Entry& entry = impl_->entries_[existing_index];
        if (!(entry.message_key == message_key) ||
            entry.record_class != record_class) {
            IncrementCounter(&channel.conflicts);
            impl_->FreezeChannel(
                channel_slot,
                NativeSequenceRecoveryFreezeReasonV1::
                    kPayloadConflict);
            impl_->FillObserveResult(
                channel,
                NativeSequenceRecoveryObserveDispositionV1::
                    kPayloadConflict,
                NativeSequenceRecoveryTokenV1{},
                output);
            return NativeSequenceRecoveryObserveErrorV1::kNone;
        }

        if (!entry.sequence_observed) {
            if (!impl_->UnseenSequenceWithinBackwardBound(
                    channel_slot, descriptor.sequence)) {
                impl_->FillObserveResult(
                    channel,
                    NativeSequenceRecoveryObserveDispositionV1::
                        kResourceFrozen,
                    NativeSequenceRecoveryTokenV1{},
                    output);
                return NativeSequenceRecoveryObserveErrorV1::kNone;
            }
            if (channel.initialized &&
                descriptor.sequence < channel.origin_sequence) {
                impl_->ReleaseEntry(existing_index);
                impl_->FillObserveResult(
                    channel,
                    NativeSequenceRecoveryObserveDispositionV1::
                        kBeforeOrigin,
                    NativeSequenceRecoveryTokenV1{},
                    output);
                return NativeSequenceRecoveryObserveErrorV1::kNone;
            }
            if (!impl_->SequenceFitsWindow(
                    channel, descriptor.sequence)) {
                impl_->FreezeChannel(
                    channel_slot,
                    NativeSequenceRecoveryFreezeReasonV1::
                        kReorderWindowExceeded);
                impl_->FillObserveResult(
                    channel,
                    NativeSequenceRecoveryObserveDispositionV1::
                        kResourceFrozen,
                    NativeSequenceRecoveryTokenV1{},
                    output);
                return NativeSequenceRecoveryObserveErrorV1::kNone;
            }
            const auto disposition =
                impl_->ClassifyNewObservedSequence(
                    channel, descriptor.sequence);
            if (!impl_->RegisterObservedSequence(
                    channel_slot, existing_index)) {
                impl_->FillObserveResult(
                    channel,
                    NativeSequenceRecoveryObserveDispositionV1::
                        kResourceFrozen,
                    NativeSequenceRecoveryTokenV1{},
                    output);
                return NativeSequenceRecoveryObserveErrorV1::kNone;
            }
            const NativeSequenceRecoveryTokenV1 token =
                record_class ==
                        NativeSequenceRecoveryRecordClassV1::kTarget
                    ? impl_->TokenFor(existing_index)
                    : NativeSequenceRecoveryTokenV1{};
            impl_->FillObserveResult(
                channel, disposition, token, output);
            return NativeSequenceRecoveryObserveErrorV1::kNone;
        }

        if (entry.observe_calls ==
            std::numeric_limits<std::uint64_t>::max()) {
            impl_->FreezeChannel(
                channel_slot,
                NativeSequenceRecoveryFreezeReasonV1::
                    kInternalInvariant);
            impl_->FillObserveResult(
                channel,
                NativeSequenceRecoveryObserveDispositionV1::
                    kResourceFrozen,
                NativeSequenceRecoveryTokenV1{},
                output);
            return NativeSequenceRecoveryObserveErrorV1::kNone;
        }
        const std::uint64_t gap_before =
            impl_->DuplicateVerificationGap(entry);
        ++entry.observe_calls;
        const std::uint64_t gap_after =
            impl_->DuplicateVerificationGap(entry);
        if (gap_after < gap_before ||
            gap_after - gap_before >
                std::numeric_limits<std::uint64_t>::max() -
                    channel.unverified_duplicate_applications) {
            impl_->FreezeChannel(
                channel_slot,
                NativeSequenceRecoveryFreezeReasonV1::
                    kInternalInvariant);
            impl_->FillObserveResult(
                channel,
                NativeSequenceRecoveryObserveDispositionV1::
                    kResourceFrozen,
                NativeSequenceRecoveryTokenV1{},
                output);
            return NativeSequenceRecoveryObserveErrorV1::kNone;
        }
        channel.unverified_duplicate_applications +=
            gap_after - gap_before;
        IncrementCounter(&channel.duplicate_arrivals);
        impl_->FillObserveResult(
            channel,
            entry.retained
                ? NativeSequenceRecoveryObserveDispositionV1::
                      kDuplicateCertified
                : NativeSequenceRecoveryObserveDispositionV1::
                      kDuplicatePending,
            NativeSequenceRecoveryTokenV1{},
            output);
        return NativeSequenceRecoveryObserveErrorV1::kNone;
    }

    if (!impl_->UnseenSequenceWithinBackwardBound(
            channel_slot, descriptor.sequence)) {
        impl_->FillObserveResult(
            channel,
            NativeSequenceRecoveryObserveDispositionV1::
                kResourceFrozen,
            NativeSequenceRecoveryTokenV1{},
            output);
        return NativeSequenceRecoveryObserveErrorV1::kNone;
    }
    if (channel.initialized &&
        descriptor.sequence < channel.origin_sequence) {
        impl_->FillObserveResult(
            channel,
            NativeSequenceRecoveryObserveDispositionV1::
                kBeforeOrigin,
            NativeSequenceRecoveryTokenV1{},
            output);
        return NativeSequenceRecoveryObserveErrorV1::kNone;
    }
    if (channel.initialized &&
        descriptor.sequence <= channel.certified_sequence) {
        IncrementCounter(
            &channel.duplicate_outside_retention);
        impl_->FreezeChannel(
            channel_slot,
            NativeSequenceRecoveryFreezeReasonV1::
                kDuplicateVerificationUnavailable);
        impl_->FillObserveResult(
            channel,
            NativeSequenceRecoveryObserveDispositionV1::kResourceFrozen,
            NativeSequenceRecoveryTokenV1{},
            output);
        return NativeSequenceRecoveryObserveErrorV1::kNone;
    }
    if (!impl_->SequenceFitsWindow(channel, descriptor.sequence)) {
        impl_->FreezeChannel(
            channel_slot,
            NativeSequenceRecoveryFreezeReasonV1::
                kReorderWindowExceeded);
        impl_->FillObserveResult(
            channel,
            NativeSequenceRecoveryObserveDispositionV1::
                kResourceFrozen,
            NativeSequenceRecoveryTokenV1{},
            output);
        return NativeSequenceRecoveryObserveErrorV1::kNone;
    }
    if (!impl_->HasEntryCapacity(channel)) {
        const auto reason = impl_->EntryCapacityReason(channel);
        impl_->FreezeChannel(channel_slot, reason);
        impl_->FillObserveResult(
            channel,
            NativeSequenceRecoveryObserveDispositionV1::
                kResourceFrozen,
            NativeSequenceRecoveryTokenV1{},
            output);
        return NativeSequenceRecoveryObserveErrorV1::kNone;
    }

    const auto disposition =
        impl_->ClassifyNewObservedSequence(
            channel, descriptor.sequence);
    const std::uint32_t entry_index = impl_->AllocateEntry(
        channel_slot,
        descriptor.sequence,
        message_key,
        record_class);
    if (entry_index == kInvalidIndex) {
        impl_->FreezeChannel(
            channel_slot,
            NativeSequenceRecoveryFreezeReasonV1::
                kPendingEntryCapacity);
        impl_->FillObserveResult(
            channel,
            NativeSequenceRecoveryObserveDispositionV1::
                kResourceFrozen,
            NativeSequenceRecoveryTokenV1{},
            output);
        return NativeSequenceRecoveryObserveErrorV1::kNone;
    }
    if (!impl_->RegisterObservedSequence(
            channel_slot, entry_index)) {
        impl_->FillObserveResult(
            channel,
            NativeSequenceRecoveryObserveDispositionV1::
                kResourceFrozen,
            NativeSequenceRecoveryTokenV1{},
            output);
        return NativeSequenceRecoveryObserveErrorV1::kNone;
    }
    const NativeSequenceRecoveryTokenV1 token =
        record_class ==
                NativeSequenceRecoveryRecordClassV1::kTarget
            ? impl_->TokenFor(entry_index)
            : NativeSequenceRecoveryTokenV1{};
    impl_->FillObserveResult(
        channel, disposition, token, output);
    return NativeSequenceRecoveryObserveErrorV1::kNone;
}

NativeSequenceRecoveryApplyErrorV1
NativeSequenceRecoveryCoordinatorV1::MarkTargetApplied(
    const NativeSequenceDescriptorV1& descriptor,
    const l2flow::sdk::MessageKey& message_key,
    std::span<const std::byte> canonical_payload,
    std::uint64_t applied_cookie,
    NativeSequenceRecoveryApplyResultV1* output) noexcept {
    if (output == nullptr) {
        return NativeSequenceRecoveryApplyErrorV1::kNullOutput;
    }
    *output = NativeSequenceRecoveryApplyResultV1{};
    if (!IsValidDescriptor(descriptor) ||
        !IsValidMessageKeyForDomain(
            descriptor.domain, message_key) ||
        canonical_payload.empty()) {
        return NativeSequenceRecoveryApplyErrorV1::kInvalidInput;
    }
    if (impl_->input_sealed_) {
        return NativeSequenceRecoveryApplyErrorV1::kInputSealed;
    }

    const std::uint32_t channel_slot =
        impl_->FindOrCreateChannel(descriptor.domain);
    if (channel_slot == kInvalidIndex) {
        output->disposition =
            NativeSequenceRecoveryApplyDispositionV1::
                kChannelCapacity;
        return NativeSequenceRecoveryApplyErrorV1::kNone;
    }
    Impl::Channel& channel = impl_->channels_[channel_slot];
    if (channel.frozen) {
        impl_->FillApplyResult(
            channel,
            nullptr,
            NativeSequenceRecoveryApplyDispositionV1::
                kChannelFrozen,
            output);
        return NativeSequenceRecoveryApplyErrorV1::kNone;
    }

    std::uint32_t entry_index =
        impl_->FindEntry(channel_slot, descriptor.sequence);
    if (entry_index == kInvalidIndex) {
        if (channel.initialized &&
            descriptor.sequence <= channel.certified_sequence) {
            IncrementCounter(
                &channel.duplicate_outside_retention);
            impl_->FreezeChannel(
                channel_slot,
                NativeSequenceRecoveryFreezeReasonV1::
                    kDuplicateVerificationUnavailable);
            impl_->FillApplyResult(
                channel,
                nullptr,
                NativeSequenceRecoveryApplyDispositionV1::
                    kResourceFrozen,
                output);
            return NativeSequenceRecoveryApplyErrorV1::kNone;
        }
        if (channel.initialized &&
            descriptor.sequence < channel.origin_sequence) {
            impl_->FillApplyResult(
                channel,
                nullptr,
                NativeSequenceRecoveryApplyDispositionV1::
                    kDuplicateOutsideRetention,
                output);
            return NativeSequenceRecoveryApplyErrorV1::kNone;
        }
        if (!impl_->SequenceFitsWindow(
                channel, descriptor.sequence)) {
            impl_->FreezeChannel(
                channel_slot,
                NativeSequenceRecoveryFreezeReasonV1::
                    kReorderWindowExceeded);
            impl_->FillApplyResult(
                channel,
                nullptr,
                NativeSequenceRecoveryApplyDispositionV1::
                    kResourceFrozen,
                output);
            return NativeSequenceRecoveryApplyErrorV1::kNone;
        }
        if (!impl_->HasEntryCapacity(channel)) {
            const auto reason =
                impl_->EntryCapacityReason(channel);
            impl_->FreezeChannel(channel_slot, reason);
            impl_->FillApplyResult(
                channel,
                nullptr,
                NativeSequenceRecoveryApplyDispositionV1::
                    kResourceFrozen,
                output);
            return NativeSequenceRecoveryApplyErrorV1::kNone;
        }
        entry_index = impl_->AllocateEntry(
            channel_slot,
            descriptor.sequence,
            message_key,
            NativeSequenceRecoveryRecordClassV1::kTarget);
        if (entry_index == kInvalidIndex) {
            impl_->FreezeChannel(
                channel_slot,
                NativeSequenceRecoveryFreezeReasonV1::
                    kPendingEntryCapacity);
            impl_->FillApplyResult(
                channel,
                nullptr,
                NativeSequenceRecoveryApplyDispositionV1::
                    kResourceFrozen,
                output);
            return NativeSequenceRecoveryApplyErrorV1::kNone;
        }
    }

    return impl_->StoreApplication(
        channel_slot,
        entry_index,
        message_key,
        canonical_payload,
        applied_cookie,
        output);
}

NativeSequenceRecoveryApplyErrorV1
NativeSequenceRecoveryCoordinatorV1::MarkTargetApplied(
    const NativeSequenceRecoveryTokenV1& token,
    const l2flow::sdk::MessageKey& message_key,
    std::span<const std::byte> canonical_payload,
    std::uint64_t applied_cookie,
    NativeSequenceRecoveryApplyResultV1* output) noexcept {
    if (output == nullptr) {
        return NativeSequenceRecoveryApplyErrorV1::kNullOutput;
    }
    *output = NativeSequenceRecoveryApplyResultV1{};
    if (impl_->input_sealed_) {
        return NativeSequenceRecoveryApplyErrorV1::kInputSealed;
    }
    if (!token.valid() || canonical_payload.empty() ||
        token.channel_slot >= impl_->channels_.size()) {
        return NativeSequenceRecoveryApplyErrorV1::kInvalidToken;
    }
    Impl::Channel& channel =
        impl_->channels_[token.channel_slot];
    if (!channel.occupied) {
        return NativeSequenceRecoveryApplyErrorV1::kInvalidToken;
    }
    if (channel.frozen) {
        impl_->FillApplyResult(
            channel,
            nullptr,
            NativeSequenceRecoveryApplyDispositionV1::
                kChannelFrozen,
            output);
        return NativeSequenceRecoveryApplyErrorV1::kChannelFrozen;
    }
    if (token.entry_index >= impl_->entries_.size()) {
        return NativeSequenceRecoveryApplyErrorV1::kInvalidToken;
    }
    const Impl::Entry& entry =
        impl_->entries_[token.entry_index];
    if (!entry.occupied ||
        entry.channel_slot != token.channel_slot ||
        entry.sequence != token.sequence ||
        entry.generation != token.entry_generation) {
        return NativeSequenceRecoveryApplyErrorV1::kInvalidToken;
    }
    if (entry.record_class !=
        NativeSequenceRecoveryRecordClassV1::kTarget) {
        return NativeSequenceRecoveryApplyErrorV1::kWrongRecordClass;
    }
    if (!IsValidMessageKeyForDomain(
            channel.domain, message_key)) {
        return NativeSequenceRecoveryApplyErrorV1::kInvalidInput;
    }
    return impl_->StoreApplication(
        token.channel_slot,
        token.entry_index,
        message_key,
        canonical_payload,
        applied_cookie,
        output);
}

NativeSequenceRecoveryPollErrorV1
NativeSequenceRecoveryCoordinatorV1::PollCertified(
    NativeSequenceCertifiedReadyV1* output) noexcept {
    if (output == nullptr) {
        return NativeSequenceRecoveryPollErrorV1::kNullOutput;
    }
    *output = NativeSequenceCertifiedReadyV1{};
    const std::size_t channel_count = impl_->channel_count_;
    if (channel_count == 0U) {
        return NativeSequenceRecoveryPollErrorV1::kNotReady;
    }
    for (std::size_t inspected = 0U;
         inspected < channel_count;
         ++inspected) {
        const std::size_t dense_index =
            (impl_->poll_cursor_ + inspected) %
            channel_count;
        const std::uint32_t slot =
            impl_->occupied_channel_slots_[dense_index];
        if (slot == kInvalidIndex ||
            slot >= impl_->channels_.size()) {
            return NativeSequenceRecoveryPollErrorV1::kNotReady;
        }
        Impl::Channel& channel = impl_->channels_[slot];
        if (!channel.occupied || channel.frozen ||
            !channel.initialized) {
            continue;
        }

        while (channel.certified_sequence <
               channel.observed_contiguous_sequence) {
            const std::uint64_t next_sequence =
                channel.certified_sequence + 1U;
            if (impl_->HasBlockingDuplicateVerification(
                    slot,
                    next_sequence)) {
                break;
            }
            const std::uint32_t entry_index =
                impl_->FindEntry(
                    slot,
                    next_sequence);
            if (entry_index == kInvalidIndex) {
                impl_->FreezeChannel(
                    slot,
                    NativeSequenceRecoveryFreezeReasonV1::
                        kInternalInvariant);
                break;
            }
            Impl::Entry& entry = impl_->entries_[entry_index];
            if (!entry.sequence_observed || !entry.pending) {
                impl_->FreezeChannel(
                    slot,
                    NativeSequenceRecoveryFreezeReasonV1::
                        kInternalInvariant);
                break;
            }
            if (entry.record_class ==
                NativeSequenceRecoveryRecordClassV1::kFiltered) {
                output->token = impl_->TokenFor(entry_index);
                output->descriptor.domain = channel.domain;
                output->descriptor.sequence = entry.sequence;
                output->message_key = entry.message_key;
                output->record_class =
                    NativeSequenceRecoveryRecordClassV1::kFiltered;
                impl_->poll_cursor_ =
                    (dense_index + 1U) % channel_count;
                return NativeSequenceRecoveryPollErrorV1::kNone;
            }
            if (entry.observe_calls == 0U ||
                entry.apply_calls < entry.observe_calls) {
                break;
            }
            output->token = impl_->TokenFor(entry_index);
            output->descriptor.domain = channel.domain;
            output->descriptor.sequence = entry.sequence;
            output->message_key = entry.message_key;
            output->record_class =
                NativeSequenceRecoveryRecordClassV1::kTarget;
            output->applied_cookie = entry.applied_cookie;
            impl_->poll_cursor_ =
                (dense_index + 1U) % channel_count;
            return NativeSequenceRecoveryPollErrorV1::kNone;
        }
    }
    impl_->poll_cursor_ =
        (impl_->poll_cursor_ + 1U) % channel_count;
    return NativeSequenceRecoveryPollErrorV1::kNotReady;
}

NativeSequenceRecoveryCommitErrorV1
NativeSequenceRecoveryCoordinatorV1::CommitCertified(
    const NativeSequenceRecoveryTokenV1& token) noexcept {
    if (!token.valid() ||
        token.channel_slot >= impl_->channels_.size() ||
        token.entry_index >= impl_->entries_.size()) {
        return NativeSequenceRecoveryCommitErrorV1::kInvalidToken;
    }
    Impl::Channel& channel =
        impl_->channels_[token.channel_slot];
    if (!channel.occupied) {
        return NativeSequenceRecoveryCommitErrorV1::kInvalidToken;
    }
    if (channel.frozen) {
        return NativeSequenceRecoveryCommitErrorV1::kChannelFrozen;
    }
    Impl::Entry& entry = impl_->entries_[token.entry_index];
    if (!entry.occupied ||
        entry.channel_slot != token.channel_slot ||
        entry.sequence != token.sequence ||
        entry.generation != token.entry_generation) {
        return NativeSequenceRecoveryCommitErrorV1::kInvalidToken;
    }
    if (entry.sequence != channel.certified_sequence + 1U) {
        return NativeSequenceRecoveryCommitErrorV1::kNotNext;
    }
    if (!entry.sequence_observed || entry.observe_calls == 0U ||
        !entry.pending) {
        return NativeSequenceRecoveryCommitErrorV1::kNotApplied;
    }
    if (entry.record_class ==
        NativeSequenceRecoveryRecordClassV1::kFiltered) {
        channel.certified_sequence = entry.sequence;
        IncrementCounter(
            &channel.filtered_sequences_certified);
        impl_->FinishPendingAndRetain(token.entry_index);
        return NativeSequenceRecoveryCommitErrorV1::kNone;
    }
    if (entry.record_class !=
        NativeSequenceRecoveryRecordClassV1::kTarget) {
        return NativeSequenceRecoveryCommitErrorV1::
            kWrongRecordClass;
    }
    if (impl_->HasBlockingDuplicateVerification(
            token.channel_slot, entry.sequence)) {
        return NativeSequenceRecoveryCommitErrorV1::kNotApplied;
    }
    if (entry.apply_calls < entry.observe_calls) {
        return NativeSequenceRecoveryCommitErrorV1::kNotApplied;
    }
    channel.certified_sequence = entry.sequence;
    // A successful commit transfers the canonical cookie back to the caller
    // that received it in the ready token. Retention keeps only canonical
    // bytes, never the caller's full-payload lease.
    entry.applied_cookie_owned = false;
    entry.applied_cookie = 0U;
    impl_->FinishPendingAndRetain(token.entry_index);
    return NativeSequenceRecoveryCommitErrorV1::kNone;
}

NativeSequenceRecoverySealErrorV1
NativeSequenceRecoveryCoordinatorV1::SealInput() noexcept {
    if (impl_->input_sealed_) {
        return NativeSequenceRecoverySealErrorV1::kAlreadySealed;
    }
    impl_->input_sealed_ = true;

    for (std::size_t dense_index = 0U;
         dense_index < impl_->channel_count_;
         ++dense_index) {
        const std::uint32_t channel_slot =
            impl_->occupied_channel_slots_[dense_index];
        if (channel_slot == kInvalidIndex ||
            channel_slot >= impl_->channels_.size()) {
            continue;
        }
        Impl::Channel& channel = impl_->channels_[channel_slot];
        if (!channel.occupied || channel.frozen) {
            continue;
        }
        if (!channel.initialized &&
            !impl_->EstablishBootstrapOrigin(channel_slot)) {
            impl_->FreezeChannel(
                channel_slot,
                NativeSequenceRecoveryFreezeReasonV1::
                    kInputSealedIncomplete);
            continue;
        }
        if (channel.observed_contiguous_sequence <
            channel.highest_observed_sequence) {
            impl_->FreezeChannel(
                channel_slot,
                NativeSequenceRecoveryFreezeReasonV1::
                    kInputSealedIncomplete);
            continue;
        }

        bool incomplete = false;
        for (const Impl::Entry& entry : impl_->entries_) {
            if (!entry.occupied ||
                entry.channel_slot != channel_slot) {
                continue;
            }
            if (!entry.sequence_observed ||
                (entry.record_class ==
                     NativeSequenceRecoveryRecordClassV1::kTarget &&
                 entry.apply_calls != entry.observe_calls)) {
                incomplete = true;
                break;
            }
        }
        if (incomplete) {
            impl_->FreezeChannel(
                channel_slot,
                NativeSequenceRecoveryFreezeReasonV1::
                    kInputSealedIncomplete);
        }
    }
    return NativeSequenceRecoverySealErrorV1::kNone;
}

bool NativeSequenceRecoveryCoordinatorV1::ChannelSnapshot(
    const NativeSequenceChannelV1& domain,
    NativeSequenceRecoveryChannelSnapshotV1* output) const noexcept {
    if (output == nullptr) {
        return false;
    }
    *output = NativeSequenceRecoveryChannelSnapshotV1{};
    const std::uint32_t channel_slot =
        impl_->FindChannel(domain);
    if (channel_slot == kInvalidIndex) {
        return false;
    }
    const Impl::Channel& channel =
        impl_->channels_[channel_slot];
    output->domain = channel.domain;
    output->state = impl_->ChannelState(channel);
    output->freeze_reason = channel.freeze_reason;
    output->origin_sequence = channel.origin_sequence;
    output->certified_sequence = channel.certified_sequence;
    output->observed_contiguous_sequence =
        channel.observed_contiguous_sequence;
    output->highest_observed_sequence =
        channel.highest_observed_sequence;
    output->unique_sequences = channel.unique_sequences;
    output->duplicate_arrivals = channel.duplicate_arrivals;
    output->exact_duplicate_applications =
        channel.exact_duplicate_applications;
    output->unverified_duplicate_applications =
        channel.unverified_duplicate_applications;
    output->duplicate_outside_retention =
        channel.duplicate_outside_retention;
    output->conflicts = channel.conflicts;
    output->filtered_sequences_certified =
        channel.filtered_sequences_certified;
    output->channel_correction_epoch =
        channel.channel_correction_epoch;
    output->pending_entries = channel.pending_entries;
    output->coverage_from_sequence_one =
        channel.initialized && channel.origin_sequence == 1U;

    if (channel.initialized &&
        channel.highest_observed_sequence >
            channel.observed_contiguous_sequence) {
        const std::uint64_t span =
            channel.highest_observed_sequence -
            channel.observed_contiguous_sequence;
        const std::uint64_t observed_above_contiguous =
            static_cast<std::uint64_t>(
                channel.observed_above_contiguous);
        output->missing_sequences =
            observed_above_contiguous <= span
                ? span - observed_above_contiguous
                : 0U;
    }
    return true;
}

NativeSequenceRecoverySnapshotV1
NativeSequenceRecoveryCoordinatorV1::Snapshot() const noexcept {
    NativeSequenceRecoverySnapshotV1 output{};
    output.maximum_channels = impl_->config_.maximum_channels;
    output.channel_count = impl_->channel_count_;
    output.frozen_channel_count =
        impl_->frozen_channel_count_;
    output.pending_entries = impl_->pending_entries_;
    output.retained_certified_entries =
        impl_->retained_entries_;
    output.canonical_payload_bytes =
        impl_->canonical_payload_bytes_;
    output.channel_capacity_failures =
        impl_->channel_capacity_failures_;
    output.invalid_observations =
        impl_->invalid_observations_;
    return output;
}

}  // namespace l2flow::realtime
