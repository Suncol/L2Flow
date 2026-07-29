#include "l2flow/realtime/owned_ingress_message_v1.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <mutex>
#include <new>
#include <utility>

namespace l2flow::realtime {
namespace {

inline constexpr std::size_t kMinimumSizeClassBytes = 128U;
inline constexpr std::size_t kMaximumSizeClassBytes =
    32U * 1024U * 1024U;
inline constexpr std::size_t kSizeClassCount = 19U;
static_assert(
    kMinimumSizeClassBytes << (kSizeClassCount - 1U) ==
    kMaximumSizeClassBytes);
static_assert(sizeof(OwnedIngressMessageV1) <= kMinimumSizeClassBytes);
static_assert(
    alignof(OwnedIngressMessageV1) <= alignof(std::max_align_t));

[[nodiscard]] bool IsZeroIdentity(
    const l2flow::common::Identity128& identity) noexcept {
    return std::all_of(
        identity.begin(), identity.end(),
        [](std::byte value) { return value == std::byte{0U}; });
}

[[nodiscard]] bool IsValidMetadata(
    const OwnedIngressMetadataV1& metadata) noexcept {
    return !IsZeroIdentity(metadata.run_id) &&
           metadata.global_ingress_sequence != 0U &&
           metadata.source_sequence != 0U &&
           metadata.global_ingress_sequence !=
               std::numeric_limits<std::uint64_t>::max() &&
           metadata.source_sequence !=
               std::numeric_limits<std::uint64_t>::max() &&
           metadata.tick_stream_sequence !=
               std::numeric_limits<std::uint64_t>::max() &&
           metadata.tick_stream_sequence <=
               metadata.global_ingress_sequence;
}

[[nodiscard]] bool TickStreamSequenceMatchesSource(
    OwnedIngressSourceV1 source,
    std::uint64_t tick_stream_sequence) noexcept {
    switch (source) {
        case OwnedIngressSourceV1::kShanghaiSnapshot:
        case OwnedIngressSourceV1::kShenzhenSnapshot:
            return tick_stream_sequence == 0U;
        case OwnedIngressSourceV1::kShanghaiTick:
        case OwnedIngressSourceV1::kShenzhenTick:
            return tick_stream_sequence != 0U;
    }
    return false;
}

[[nodiscard]] bool IsValidMaximumMessageBytes(
    std::uint32_t maximum_message_bytes) noexcept {
    return maximum_message_bytes >= l2flow::sdk::kVendorHeadBytes &&
           maximum_message_bytes <=
               kOwnedIngressMaximumMessageBytesV1;
}

[[nodiscard]] std::size_t SizeClassBytes(
    std::size_t index) noexcept {
    return kMinimumSizeClassBytes << index;
}

[[nodiscard]] bool FindSizeClass(
    std::size_t required_bytes,
    std::uint8_t* output) noexcept {
    std::size_t class_bytes = kMinimumSizeClassBytes;
    for (std::size_t index = 0U; index < kSizeClassCount; ++index) {
        if (required_bytes <= class_bytes) {
            *output = static_cast<std::uint8_t>(index);
            return true;
        }
        class_bytes <<= 1U;
    }
    return false;
}

[[nodiscard]] bool PoolConfigValid(
    const OwnedIngressMessagePoolConfigV1& config,
    std::uint8_t* maximum_size_class,
    std::size_t* maximum_pool_bytes) noexcept {
    if (!IsValidMaximumMessageBytes(config.maximum_message_bytes) ||
        config.maximum_inflight_messages == 0U ||
        config.maximum_inflight_messages >
            kOwnedIngressMaximumInflightMessagesV1 ||
        (config.prewarm_message_bytes == 0U) !=
            (config.prewarm_message_count == 0U) ||
        config.prewarm_message_bytes > config.maximum_message_bytes ||
        config.prewarm_message_count >
            config.maximum_inflight_messages) {
        return false;
    }
    if (config.prewarm_message_count != 0U &&
        !IsValidMaximumMessageBytes(
            config.prewarm_message_bytes)) {
        return false;
    }

    if (config.prewarm_message_count != 0U) {
        const std::size_t prewarm_body_bytes =
            static_cast<std::size_t>(
                config.prewarm_message_bytes) -
            l2flow::sdk::kVendorHeadBytes;
        std::uint8_t prewarm_size_class = 0U;
        if (!FindSizeClass(
                sizeof(OwnedIngressMessageV1) +
                    prewarm_body_bytes,
                &prewarm_size_class)) {
            return false;
        }
        const std::size_t prewarm_block_bytes =
            SizeClassBytes(prewarm_size_class);
        if (config.prewarm_message_count >
            kOwnedIngressMaximumPrewarmBytesV1 /
                prewarm_block_bytes) {
            return false;
        }
    }

    const std::size_t maximum_body_bytes =
        static_cast<std::size_t>(config.maximum_message_bytes) -
        l2flow::sdk::kVendorHeadBytes;
    const std::size_t maximum_required_bytes =
        sizeof(OwnedIngressMessageV1) + maximum_body_bytes;
    if (!FindSizeClass(maximum_required_bytes, maximum_size_class)) {
        return false;
    }
    const std::size_t maximum_block_bytes =
        SizeClassBytes(*maximum_size_class);
    if (config.maximum_inflight_messages >
        std::numeric_limits<std::size_t>::max() /
            maximum_block_bytes) {
        return false;
    }
    *maximum_pool_bytes =
        config.maximum_inflight_messages * maximum_block_bytes;
    return true;
}

}  // namespace

class OwnedIngressMessagePoolStateV1 final {
public:
    OwnedIngressMessagePoolStateV1(
        OwnedIngressMessagePoolConfigV1 config,
        std::uint8_t maximum_size_class,
        std::size_t maximum_pool_bytes) noexcept
        : config_(config),
          maximum_size_class_(maximum_size_class),
          maximum_pool_bytes_(maximum_pool_bytes) {}

    OwnedIngressMessagePoolStateV1(
        const OwnedIngressMessagePoolStateV1&) = delete;
    OwnedIngressMessagePoolStateV1& operator=(
        const OwnedIngressMessagePoolStateV1&) = delete;

    [[nodiscard]] bool Prewarm() noexcept {
        if (config_.prewarm_message_count == 0U) {
            return true;
        }

        const std::size_t prewarm_body_bytes =
            static_cast<std::size_t>(
                config_.prewarm_message_bytes) -
            l2flow::sdk::kVendorHeadBytes;
        std::uint8_t size_class = 0U;
        if (!FindSizeClass(
                sizeof(OwnedIngressMessageV1) +
                    prewarm_body_bytes,
                &size_class) ||
            size_class > maximum_size_class_) {
            return false;
        }

        const std::size_t block_bytes =
            SizeClassBytes(size_class);
        std::lock_guard<std::mutex> lock(mutex_);
        for (std::size_t index = 0U;
             index < config_.prewarm_message_count;
             ++index) {
            void* const block =
                ::operator new(block_bytes, std::nothrow);
            if (block == nullptr) {
                return false;
            }
            // Creation runs before SDK Connect. Touch every byte now so the
            // first opening-burst memcpy cannot inherit demand-zero page
            // faults from an otherwise only virtually allocated block.
            std::memset(block, 0, block_bytes);
            PushFreeBlockLocked(size_class, block);
            ++allocated_blocks_;
            allocated_bytes_ += block_bytes;
        }
        return true;
    }

    [[nodiscard]] OwnedIngressMessageErrorV1 Acquire(
        const OwnedIngressMessageInspectionV1& inspection,
        const OwnedIngressMetadataV1& metadata,
        OwnedIngressMessageHandleV1* output) noexcept {
        if (output == nullptr) {
            return OwnedIngressMessageErrorV1::kNullOutput;
        }
        output->reset();
        if (!IsValidMetadata(metadata)) {
            return OwnedIngressMessageErrorV1::kInvalidMetadata;
        }
        if (!InspectionValid(inspection)) {
            return OwnedIngressMessageErrorV1::kInvalidInspection;
        }
        if (!TickStreamSequenceMatchesSource(
                inspection.source(), metadata.tick_stream_sequence)) {
            return OwnedIngressMessageErrorV1::kInvalidMetadata;
        }
        if (inspection.wire_size_ > config_.maximum_message_bytes) {
            return OwnedIngressMessageErrorV1::kMessageTooLarge;
        }

        const std::size_t required_bytes =
            sizeof(OwnedIngressMessageV1) + inspection.body_.size();
        std::uint8_t requested_size_class = 0U;
        if (!FindSizeClass(required_bytes, &requested_size_class) ||
            requested_size_class > maximum_size_class_) {
            return OwnedIngressMessageErrorV1::kMessageTooLarge;
        }

        void* block = nullptr;
        std::uint8_t actual_size_class = requested_size_class;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (retired_) {
                return OwnedIngressMessageErrorV1::kPoolExhausted;
            }
            if (active_messages_ >=
                config_.maximum_inflight_messages) {
                return OwnedIngressMessageErrorV1::kPoolExhausted;
            }

            for (std::size_t index = requested_size_class;
                 index <= maximum_size_class_;
                 ++index) {
                if (free_lists_[index].head != nullptr) {
                    actual_size_class =
                        static_cast<std::uint8_t>(index);
                    block = PopFreeBlockLocked(index);
                    break;
                }
            }

            if (block == nullptr) {
                while (allocated_blocks_ >=
                       config_.maximum_inflight_messages) {
                    if (!EvictOneFreeBlockLocked()) {
                        return OwnedIngressMessageErrorV1::
                            kPoolExhausted;
                    }
                }

                const std::size_t allocation_bytes =
                    SizeClassBytes(requested_size_class);
                while (allocated_bytes_ >
                       maximum_pool_bytes_ - allocation_bytes) {
                    if (!EvictOneFreeBlockLocked()) {
                        return OwnedIngressMessageErrorV1::
                            kPoolExhausted;
                    }
                }
                block = ::operator new(
                    allocation_bytes, std::nothrow);
                if (block == nullptr) {
                    return OwnedIngressMessageErrorV1::
                        kResourceExhausted;
                }
                ++allocated_blocks_;
                allocated_bytes_ += allocation_bytes;
                actual_size_class = requested_size_class;
            }

            ++active_messages_;
            AddLifetimeReference();
        }

        auto* const allocation_bytes =
            static_cast<std::byte*>(block);
        std::byte* const body_data =
            allocation_bytes + sizeof(OwnedIngressMessageV1);
        auto* const message = new (block) OwnedIngressMessageV1(
            metadata,
            inspection,
            this,
            body_data,
            actual_size_class);
        if (!inspection.body_.empty()) {
            std::memcpy(
                message->mutable_body_data(),
                inspection.body_.data(),
                inspection.body_.size());
        }
        *output = OwnedIngressMessageHandleV1(
            message,
            OwnedIngressMessageHandleV1::AdoptReference{});
        return OwnedIngressMessageErrorV1::kNone;
    }

    void Recycle(OwnedIngressMessageV1* message) noexcept {
        const std::size_t size_class = message->size_class_index_;
        void* const block = message;
        message->~OwnedIngressMessageV1();

        bool delete_block = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (active_messages_ == 0U ||
                size_class > maximum_size_class_) {
                std::terminate();
            }
            --active_messages_;
            if (retired_) {
                const std::size_t block_bytes =
                    SizeClassBytes(size_class);
                if (allocated_blocks_ == 0U ||
                    allocated_bytes_ < block_bytes) {
                    std::terminate();
                }
                --allocated_blocks_;
                allocated_bytes_ -= block_bytes;
                delete_block = true;
            } else {
                PushFreeBlockLocked(size_class, block);
            }
        }
        if (delete_block) {
            ::operator delete(block);
        }
        ReleaseLifetimeReference();
    }

    void Retire() noexcept {
        std::array<void*, kSizeClassCount> detached{};
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (retired_) {
                return;
            }
            retired_ = true;
            for (std::size_t index = 0U;
                 index <= maximum_size_class_;
                 ++index) {
                detached[index] = free_lists_[index].head;
                const std::size_t count = free_lists_[index].count;
                if (allocated_blocks_ < count) {
                    std::terminate();
                }
                allocated_blocks_ -= count;
                allocated_bytes_ -=
                    count * SizeClassBytes(index);
                cached_blocks_ -= count;
                free_lists_[index] = {};
            }
        }

        for (std::size_t index = 0U;
             index <= maximum_size_class_;
             ++index) {
            void* block = detached[index];
            while (block != nullptr) {
                void* next = nullptr;
                std::memcpy(&next, block, sizeof(next));
                ::operator delete(block);
                block = next;
            }
        }
    }

    [[nodiscard]] OwnedIngressMessagePoolSnapshotV1 Snapshot()
        const noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        OwnedIngressMessagePoolSnapshotV1 result{};
        result.maximum_message_bytes =
            config_.maximum_message_bytes;
        result.maximum_inflight_messages =
            config_.maximum_inflight_messages;
        result.prewarm_message_bytes =
            config_.prewarm_message_bytes;
        result.prewarm_message_count =
            config_.prewarm_message_count;
        result.active_messages = active_messages_;
        result.allocated_blocks = allocated_blocks_;
        result.cached_blocks = cached_blocks_;
        result.allocated_bytes = allocated_bytes_;
        result.retired = retired_;
        return result;
    }

    void AddLifetimeReference() noexcept {
        const std::size_t previous =
            lifetime_references_.fetch_add(
                1U, std::memory_order_relaxed);
        if (previous ==
            std::numeric_limits<std::size_t>::max()) {
            std::terminate();
        }
    }

    void ReleaseLifetimeReference() noexcept {
        const std::size_t previous =
            lifetime_references_.fetch_sub(
                1U, std::memory_order_acq_rel);
        if (previous == 0U) {
            std::terminate();
        }
        if (previous == 1U) {
            delete this;
        }
    }

private:
    struct FreeList final {
        void* head = nullptr;
        std::size_t count = 0U;
    };

    ~OwnedIngressMessagePoolStateV1() {
        if (!retired_ || active_messages_ != 0U ||
            allocated_blocks_ != 0U || cached_blocks_ != 0U ||
            allocated_bytes_ != 0U) {
            std::terminate();
        }
    }

    [[nodiscard]] bool InspectionValid(
        const OwnedIngressMessageInspectionV1& inspection)
        const noexcept {
        if (!inspection.valid_ ||
            inspection.wire_size_ <
                l2flow::sdk::kVendorHeadBytes ||
            inspection.wire_size_ >
                kOwnedIngressMaximumMessageBytesV1 ||
            inspection.body_.size() !=
                static_cast<std::size_t>(inspection.wire_size_) -
                    l2flow::sdk::kVendorHeadBytes) {
            return false;
        }
        const l2flow::sdk::VendorHeadView head(
            inspection.vendor_head_bytes_);
        OwnedIngressSourceV1 classified_source =
            OwnedIngressSourceV1::kShanghaiSnapshot;
        return head.head_size() == l2flow::sdk::kVendorHeadBytes &&
               head.message_size() == inspection.wire_size_ &&
               head.message_encoding() ==
                   static_cast<std::uint8_t>(
                       datayes::mdl::MDLEID_BINARY) &&
               inspection.key_ == l2flow::sdk::MessageKey{
                   head.service_id(),
                   head.service_version(),
                   head.message_id()} &&
               inspection.source_slot() <
                   kOwnedIngressSourceCountV1 &&
               ClassifyOwnedIngressMessageKeyV1(
                   inspection.key_, &classified_source) ==
                   OwnedIngressKeyErrorV1::kNone &&
               inspection.source_ == classified_source;
    }

    [[nodiscard]] void* PopFreeBlockLocked(
        std::size_t size_class) noexcept {
        FreeList& list = free_lists_[size_class];
        void* const block = list.head;
        std::memcpy(&list.head, block, sizeof(list.head));
        --list.count;
        --cached_blocks_;
        return block;
    }

    void PushFreeBlockLocked(
        std::size_t size_class,
        void* block) noexcept {
        FreeList& list = free_lists_[size_class];
        std::memcpy(block, &list.head, sizeof(list.head));
        list.head = block;
        ++list.count;
        ++cached_blocks_;
    }

    [[nodiscard]] bool EvictOneFreeBlockLocked() noexcept {
        for (std::size_t reverse = maximum_size_class_ + 1U;
             reverse != 0U;
             --reverse) {
            const std::size_t index = reverse - 1U;
            if (free_lists_[index].head == nullptr) {
                continue;
            }
            void* const block = PopFreeBlockLocked(index);
            const std::size_t block_bytes = SizeClassBytes(index);
            --allocated_blocks_;
            allocated_bytes_ -= block_bytes;
            ::operator delete(block);
            return true;
        }
        return false;
    }

    OwnedIngressMessagePoolConfigV1 config_{};
    std::uint8_t maximum_size_class_ = 0U;
    std::size_t maximum_pool_bytes_ = 0U;
    mutable std::mutex mutex_;
    std::array<FreeList, kSizeClassCount> free_lists_{};
    std::atomic<std::size_t> lifetime_references_{1U};
    std::size_t active_messages_ = 0U;
    std::size_t allocated_blocks_ = 0U;
    std::size_t cached_blocks_ = 0U;
    std::size_t allocated_bytes_ = 0U;
    bool retired_ = false;
};

std::string_view OwnedIngressKeyErrorNameV1(
    OwnedIngressKeyErrorV1 error) noexcept {
    switch (error) {
        case OwnedIngressKeyErrorV1::kNone:
            return "none";
        case OwnedIngressKeyErrorV1::kNullOutput:
            return "null_output";
        case OwnedIngressKeyErrorV1::kUnsupported:
            return "unsupported";
        case OwnedIngressKeyErrorV1::kForbiddenCombinedTick:
            return "forbidden_combined_tick";
    }
    return "unknown";
}

OwnedIngressKeyErrorV1 ClassifyOwnedIngressMessageKeyV1(
    const l2flow::sdk::MessageKey& key,
    OwnedIngressSourceV1* output) noexcept {
    if (output == nullptr) {
        return OwnedIngressKeyErrorV1::kNullOutput;
    }
    if (key == kForbiddenShenzhenCombinedTickKeyV1) {
        return OwnedIngressKeyErrorV1::kForbiddenCombinedTick;
    }

    OwnedIngressSourceV1 source =
        OwnedIngressSourceV1::kShanghaiSnapshot;
    if (key == kRequiredOwnedIngressMessageKeysV1[0U]) {
        source = OwnedIngressSourceV1::kShanghaiSnapshot;
    } else if (key == kRequiredOwnedIngressMessageKeysV1[1U]) {
        source = OwnedIngressSourceV1::kShanghaiTick;
    } else if (key == kRequiredOwnedIngressMessageKeysV1[2U]) {
        source = OwnedIngressSourceV1::kShenzhenSnapshot;
    } else if (key == kRequiredOwnedIngressMessageKeysV1[3U] ||
               key == kRequiredOwnedIngressMessageKeysV1[4U]) {
        source = OwnedIngressSourceV1::kShenzhenTick;
    } else {
        return OwnedIngressKeyErrorV1::kUnsupported;
    }

    *output = source;
    return OwnedIngressKeyErrorV1::kNone;
}

std::string_view OwnedIngressMessageErrorNameV1(
    OwnedIngressMessageErrorV1 error) noexcept {
    switch (error) {
        case OwnedIngressMessageErrorV1::kNone:
            return "none";
        case OwnedIngressMessageErrorV1::kNullOutput:
            return "null_output";
        case OwnedIngressMessageErrorV1::kInvalidPoolConfiguration:
            return "invalid_pool_configuration";
        case OwnedIngressMessageErrorV1::kInvalidMetadata:
            return "invalid_metadata";
        case OwnedIngressMessageErrorV1::kInvalidMaximumMessageBytes:
            return "invalid_maximum_message_bytes";
        case OwnedIngressMessageErrorV1::kNullMessage:
            return "null_message";
        case OwnedIngressMessageErrorV1::kSdkAccess:
            return "sdk_access";
        case OwnedIngressMessageErrorV1::kNullHead:
            return "null_head";
        case OwnedIngressMessageErrorV1::kWrongHeadSize:
            return "wrong_head_size";
        case OwnedIngressMessageErrorV1::kMessageSmallerThanHead:
            return "message_smaller_than_head";
        case OwnedIngressMessageErrorV1::kMessageTooLarge:
            return "message_too_large";
        case OwnedIngressMessageErrorV1::kUnexpectedMessageEncoding:
            return "unexpected_message_encoding";
        case OwnedIngressMessageErrorV1::kUnsupportedMessage:
            return "unsupported_message";
        case OwnedIngressMessageErrorV1::kForbiddenCombinedTick:
            return "forbidden_combined_tick";
        case OwnedIngressMessageErrorV1::kNullBody:
            return "null_body";
        case OwnedIngressMessageErrorV1::kInvalidInspection:
            return "invalid_inspection";
        case OwnedIngressMessageErrorV1::kPoolExhausted:
            return "pool_exhausted";
        case OwnedIngressMessageErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

OwnedIngressMessageErrorV1 InspectOwnedIngressMessageV1(
    const datayes::mdl::MDLMessage* message,
    std::uint32_t maximum_message_bytes,
    OwnedIngressMessageInspectionV1* output) noexcept {
    if (output == nullptr) {
        return OwnedIngressMessageErrorV1::kNullOutput;
    }
    *output = {};
    if (!IsValidMaximumMessageBytes(maximum_message_bytes)) {
        return OwnedIngressMessageErrorV1::
            kInvalidMaximumMessageBytes;
    }
    if (message == nullptr) {
        return OwnedIngressMessageErrorV1::kNullMessage;
    }

    const datayes::mdl::MDLMessageHead* vendor_head = nullptr;
    try {
        vendor_head = message->GetHead();
    } catch (...) {
        return OwnedIngressMessageErrorV1::kSdkAccess;
    }
    if (vendor_head == nullptr) {
        return OwnedIngressMessageErrorV1::kNullHead;
    }

    OwnedIngressMessageInspectionV1 candidate;
    std::memcpy(
        candidate.vendor_head_bytes_.data(),
        vendor_head,
        candidate.vendor_head_bytes_.size());
    const l2flow::sdk::VendorHeadView head(
        candidate.vendor_head_bytes_);

    // Preserve callback admission semantics: unsupported tuples are ignored
    // based on their copied catalog key before the production schema is
    // validated. The explicitly forbidden combined Shenzhen tick remains a
    // hard rejection regardless of its other header fields.
    candidate.key_ = {
        head.service_id(),
        head.service_version(),
        head.message_id()};
    const OwnedIngressKeyErrorV1 key_error =
        ClassifyOwnedIngressMessageKeyV1(
            candidate.key_, &candidate.source_);
    if (key_error ==
        OwnedIngressKeyErrorV1::kForbiddenCombinedTick) {
        return OwnedIngressMessageErrorV1::
            kForbiddenCombinedTick;
    }
    if (key_error != OwnedIngressKeyErrorV1::kNone) {
        return OwnedIngressMessageErrorV1::kUnsupportedMessage;
    }

    if (head.head_size() != l2flow::sdk::kVendorHeadBytes) {
        return OwnedIngressMessageErrorV1::kWrongHeadSize;
    }
    if (head.message_size() <
        static_cast<std::uint32_t>(head.head_size())) {
        return OwnedIngressMessageErrorV1::kMessageSmallerThanHead;
    }
    if (head.message_size() > maximum_message_bytes) {
        return OwnedIngressMessageErrorV1::kMessageTooLarge;
    }
    if (head.message_encoding() !=
        static_cast<std::uint8_t>(datayes::mdl::MDLEID_BINARY)) {
        return OwnedIngressMessageErrorV1::
            kUnexpectedMessageEncoding;
    }

    const std::size_t body_size =
        static_cast<std::size_t>(head.message_size()) -
        l2flow::sdk::kVendorHeadBytes;
    if (body_size != 0U) {
        const char* body_pointer = nullptr;
        try {
            body_pointer = message->GetBody();
        } catch (...) {
            return OwnedIngressMessageErrorV1::kSdkAccess;
        }
        if (body_pointer == nullptr) {
            return OwnedIngressMessageErrorV1::kNullBody;
        }
        candidate.body_ = std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(body_pointer),
            body_size);
    }
    candidate.wire_size_ = head.message_size();
    candidate.valid_ = true;
    *output = candidate;
    return OwnedIngressMessageErrorV1::kNone;
}

OwnedIngressMessageHandleV1::OwnedIngressMessageHandleV1(
    const OwnedIngressMessageHandleV1& other) noexcept
    : message_(other.message_) {
    if (message_ != nullptr) {
        message_->AddReference();
    }
}

OwnedIngressMessageHandleV1&
OwnedIngressMessageHandleV1::operator=(
    const OwnedIngressMessageHandleV1& other) noexcept {
    if (this != &other) {
        OwnedIngressMessageHandleV1 candidate(other);
        swap(candidate);
    }
    return *this;
}

OwnedIngressMessageHandleV1::OwnedIngressMessageHandleV1(
    OwnedIngressMessageHandleV1&& other) noexcept
    : message_(std::exchange(other.message_, nullptr)) {}

OwnedIngressMessageHandleV1&
OwnedIngressMessageHandleV1::operator=(
    OwnedIngressMessageHandleV1&& other) noexcept {
    if (this != &other) {
        reset();
        message_ = std::exchange(other.message_, nullptr);
    }
    return *this;
}

OwnedIngressMessageHandleV1::~OwnedIngressMessageHandleV1() {
    reset();
}

void OwnedIngressMessageHandleV1::reset() noexcept {
    const OwnedIngressMessageV1* const releasing =
        std::exchange(message_, nullptr);
    if (releasing != nullptr) {
        releasing->ReleaseReference();
    }
}

void OwnedIngressMessageHandleV1::swap(
    OwnedIngressMessageHandleV1& other) noexcept {
    std::swap(message_, other.message_);
}

const OwnedIngressMessageV1&
OwnedIngressMessageHandleV1::operator*() const noexcept {
    return *message_;
}

OwnedIngressMessageV1::OwnedIngressMessageV1(
    const OwnedIngressMetadataV1& metadata,
    const OwnedIngressMessageInspectionV1& inspection,
    OwnedIngressMessagePoolStateV1* pool_state,
    std::byte* body_data,
    std::uint8_t size_class_index) noexcept
    : metadata_(metadata),
      pool_state_(pool_state),
      body_data_(body_data),
      vendor_head_bytes_(inspection.vendor_head_bytes()),
      key_(inspection.key()),
      body_size_(static_cast<std::uint32_t>(
          inspection.body().size())),
      source_(inspection.source()),
      size_class_index_(size_class_index) {}

std::span<const std::byte> OwnedIngressMessageV1::body()
    const noexcept {
    return std::span<const std::byte>(
        body_data(), static_cast<std::size_t>(body_size_));
}

void OwnedIngressMessageV1::AddReference() const noexcept {
    std::uint32_t current =
        references_.load(std::memory_order_relaxed);
    for (;;) {
        if (current ==
            std::numeric_limits<std::uint32_t>::max()) {
            std::terminate();
        }
        if (references_.compare_exchange_weak(
                current,
                current + 1U,
                std::memory_order_relaxed,
                std::memory_order_relaxed)) {
            return;
        }
    }
}

void OwnedIngressMessageV1::ReleaseReference() const noexcept {
    const std::uint32_t previous =
        references_.fetch_sub(1U, std::memory_order_acq_rel);
    if (previous == 0U) {
        std::terminate();
    }
    if (previous == 1U) {
        auto* const mutable_message =
            const_cast<OwnedIngressMessageV1*>(this);
        pool_state_->Recycle(mutable_message);
    }
}

std::byte* OwnedIngressMessageV1::mutable_body_data() noexcept {
    return body_data_;
}

const std::byte* OwnedIngressMessageV1::body_data() const noexcept {
    return body_data_;
}

OwnedIngressMessagePoolV1::~OwnedIngressMessagePoolV1() {
    if (state_ != nullptr) {
        state_->Retire();
        state_->ReleaseLifetimeReference();
        state_ = nullptr;
    }
}

OwnedIngressMessageErrorV1 OwnedIngressMessagePoolV1::Create(
    OwnedIngressMessagePoolConfigV1 config,
    std::unique_ptr<OwnedIngressMessagePoolV1>* output) noexcept {
    if (output == nullptr) {
        return OwnedIngressMessageErrorV1::kNullOutput;
    }
    output->reset();

    std::uint8_t maximum_size_class = 0U;
    std::size_t maximum_pool_bytes = 0U;
    if (!PoolConfigValid(
            config,
            &maximum_size_class,
            &maximum_pool_bytes)) {
        return OwnedIngressMessageErrorV1::
            kInvalidPoolConfiguration;
    }

    auto* const state = new (std::nothrow)
        OwnedIngressMessagePoolStateV1(
            config, maximum_size_class, maximum_pool_bytes);
    if (state == nullptr) {
        return OwnedIngressMessageErrorV1::kResourceExhausted;
    }
    if (!state->Prewarm()) {
        state->Retire();
        state->ReleaseLifetimeReference();
        return OwnedIngressMessageErrorV1::kResourceExhausted;
    }
    auto* const pool = new (std::nothrow)
        OwnedIngressMessagePoolV1(state);
    if (pool == nullptr) {
        state->Retire();
        state->ReleaseLifetimeReference();
        return OwnedIngressMessageErrorV1::kResourceExhausted;
    }
    output->reset(pool);
    return OwnedIngressMessageErrorV1::kNone;
}

OwnedIngressMessageErrorV1 OwnedIngressMessagePoolV1::Acquire(
    const OwnedIngressMessageInspectionV1& inspection,
    const OwnedIngressMetadataV1& metadata,
    OwnedIngressMessageHandleV1* output) noexcept {
    return state_->Acquire(inspection, metadata, output);
}

OwnedIngressMessagePoolSnapshotV1
OwnedIngressMessagePoolV1::Snapshot() const noexcept {
    return state_->Snapshot();
}

}  // namespace l2flow::realtime
