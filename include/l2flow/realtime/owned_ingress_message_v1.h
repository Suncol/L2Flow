#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/sdk/market_message_catalog_v1.h"
#include "l2flow/sdk/vendor_head_view.h"

#include "mdl_api.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>

namespace l2flow::realtime {

inline constexpr std::size_t kOwnedIngressSourceCountV1 = 2U;
inline constexpr std::size_t kRequiredOwnedIngressMessageCountV1 =
    l2flow::sdk::kProductionMessageCountV1;
inline constexpr std::uint32_t kOwnedIngressMaximumMessageBytesV1 =
    16U * 1024U * 1024U;
inline constexpr std::size_t
    kOwnedIngressMaximumInflightMessagesV1 = 10'000'000U;
inline constexpr std::size_t kOwnedIngressMaximumPrewarmBytesV1 =
    256U * 1024U * 1024U;
static_assert(kRequiredOwnedIngressMessageCountV1 == 3U);

// These are the only market sources admitted by production. The two Shenzhen
// Tick message types deliberately share one serial owner so their callback
// order is retained.
enum class OwnedIngressSourceV1 : std::uint8_t {
    kShanghaiTick = 0U,
    kShenzhenTick = 1U,
};

inline constexpr const auto& kRequiredOwnedIngressMessageKeysV1 =
    l2flow::sdk::kProductionMessageKeysV1;

inline constexpr const l2flow::sdk::MessageKey&
    kForbiddenShenzhenCombinedTickKeyV1 =
        l2flow::sdk::kForbiddenCombinedTickMessageKeyV1;

enum class OwnedIngressKeyErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kUnsupported,
    kForbiddenCombinedTick,
};

[[nodiscard]] std::string_view OwnedIngressKeyErrorNameV1(
    OwnedIngressKeyErrorV1 error) noexcept;

// Returns a source only for one of kRequiredOwnedIngressMessageKeysV1.
// 6.101.53 is distinguished from other unsupported messages so a merged
// Shenzhen feed cannot be enabled accidentally.
[[nodiscard]] OwnedIngressKeyErrorV1 ClassifyOwnedIngressMessageKeyV1(
    const l2flow::sdk::MessageKey& key,
    OwnedIngressSourceV1* output) noexcept;

// Sequence values are assigned by the single serialized subscription
// callback. They describe the dense prefix committed directly to the raw
// source-by-Tick-worker queues, not vendor event time. A shard admission
// failure does not commit its candidate sequence and fails that instrument
// closed.
// global_ingress_sequence is retained only as an arrival identity and repair
// handshake; it is not a public live cursor or an Event/KLine ordering key.
// UINT64_MAX is reserved as the exhaustion sentinel.
struct OwnedIngressMetadataV1 final {
    l2flow::common::Identity128 run_id{};
    std::uint64_t global_ingress_sequence = 0U;
    std::uint64_t source_sequence = 0U;
    std::uint64_t recv_realtime_ns = 0U;
    std::uint64_t recv_monotonic_ns = 0U;
};

enum class OwnedIngressMessageErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidPoolConfiguration,
    kInvalidMetadata,
    kInvalidMaximumMessageBytes,
    kNullMessage,
    kSdkAccess,
    kNullHead,
    kWrongHeadSize,
    kMessageSmallerThanHead,
    kMessageTooLarge,
    kUnexpectedMessageEncoding,
    kUnsupportedMessage,
    kForbiddenCombinedTick,
    kNullBody,
    kInvalidInspection,
    kPoolExhausted,
    kResourceExhausted,
};

[[nodiscard]] std::string_view OwnedIngressMessageErrorNameV1(
    OwnedIngressMessageErrorV1 error) noexcept;

class OwnedIngressMessagePoolStateV1;

// A non-owning, callback-lifetime view produced by exactly one SDK head read
// and, for a non-empty body, exactly one SDK body read. Acquire must be called
// before the vendor callback returns. It copies these already classified
// bytes into pool ownership without calling the SDK again.
class OwnedIngressMessageInspectionV1 final {
public:
    OwnedIngressMessageInspectionV1() noexcept = default;

    [[nodiscard]] explicit operator bool() const noexcept {
        return valid_;
    }
    [[nodiscard]] OwnedIngressSourceV1 source() const noexcept {
        return source_;
    }
    [[nodiscard]] std::uint8_t source_slot() const noexcept {
        return static_cast<std::uint8_t>(source_);
    }
    [[nodiscard]] const l2flow::sdk::MessageKey& key() const noexcept {
        return key_;
    }
    [[nodiscard]] const l2flow::sdk::VendorHeadBytes&
    vendor_head_bytes() const noexcept {
        return vendor_head_bytes_;
    }
    [[nodiscard]] l2flow::sdk::VendorHeadView vendor_head()
        const noexcept {
        return l2flow::sdk::VendorHeadView(vendor_head_bytes_);
    }
    [[nodiscard]] std::span<const std::byte> body() const noexcept {
        return body_;
    }
    [[nodiscard]] std::size_t wire_size() const noexcept {
        return wire_size_;
    }

private:
    friend OwnedIngressMessageErrorV1 InspectOwnedIngressMessageV1(
        const datayes::mdl::MDLMessage*,
        std::uint32_t,
        OwnedIngressMessageInspectionV1*) noexcept;
    friend class OwnedIngressMessagePoolStateV1;

    bool valid_ = false;
    OwnedIngressSourceV1 source_ = OwnedIngressSourceV1::kShanghaiTick;
    l2flow::sdk::MessageKey key_{};
    l2flow::sdk::VendorHeadBytes vendor_head_bytes_{};
    std::span<const std::byte> body_{};
    std::uint32_t wire_size_ = 0U;
};

[[nodiscard]] OwnedIngressMessageErrorV1 InspectOwnedIngressMessageV1(
    const datayes::mdl::MDLMessage* message,
    std::uint32_t maximum_message_bytes,
    OwnedIngressMessageInspectionV1* output) noexcept;

class OwnedIngressMessageV1;

// Intrusive immutable message ownership. Copies add a reference to the same
// pooled block; moves only transfer that reference. No per-message shared_ptr
// control block is allocated.
class OwnedIngressMessageHandleV1 final {
public:
    OwnedIngressMessageHandleV1() noexcept = default;
    OwnedIngressMessageHandleV1(
        const OwnedIngressMessageHandleV1& other) noexcept;
    OwnedIngressMessageHandleV1& operator=(
        const OwnedIngressMessageHandleV1& other) noexcept;
    OwnedIngressMessageHandleV1(
        OwnedIngressMessageHandleV1&& other) noexcept;
    OwnedIngressMessageHandleV1& operator=(
        OwnedIngressMessageHandleV1&& other) noexcept;
    ~OwnedIngressMessageHandleV1();

    void reset() noexcept;
    void swap(OwnedIngressMessageHandleV1& other) noexcept;

    [[nodiscard]] const OwnedIngressMessageV1* get() const noexcept {
        return message_;
    }
    [[nodiscard]] const OwnedIngressMessageV1* operator->()
        const noexcept {
        return message_;
    }
    [[nodiscard]] const OwnedIngressMessageV1& operator*()
        const noexcept;
    [[nodiscard]] explicit operator bool() const noexcept {
        return message_ != nullptr;
    }

private:
    struct AdoptReference final {};

    explicit OwnedIngressMessageHandleV1(
        const OwnedIngressMessageV1* message,
        AdoptReference) noexcept
        : message_(message) {}

    friend class OwnedIngressMessagePoolStateV1;

    const OwnedIngressMessageV1* message_ = nullptr;
};

// Immutable ownership boundary between the vendor callback and Tick-worker
// consumers. Object storage and body bytes occupy one
// size-class pool block; the body starts immediately after this object.
class OwnedIngressMessageV1 final {
public:
    OwnedIngressMessageV1(const OwnedIngressMessageV1&) = delete;
    OwnedIngressMessageV1& operator=(
        const OwnedIngressMessageV1&) = delete;
    OwnedIngressMessageV1(OwnedIngressMessageV1&&) = delete;
    OwnedIngressMessageV1& operator=(
        OwnedIngressMessageV1&&) = delete;

    [[nodiscard]] const l2flow::common::Identity128& run_id()
        const noexcept {
        return metadata_.run_id;
    }
    [[nodiscard]] std::uint64_t global_ingress_sequence()
        const noexcept {
        return metadata_.global_ingress_sequence;
    }
    [[nodiscard]] std::uint64_t source_sequence() const noexcept {
        return metadata_.source_sequence;
    }
    [[nodiscard]] std::uint64_t recv_realtime_ns() const noexcept {
        return metadata_.recv_realtime_ns;
    }
    [[nodiscard]] std::uint64_t recv_monotonic_ns() const noexcept {
        return metadata_.recv_monotonic_ns;
    }
    [[nodiscard]] const OwnedIngressMetadataV1& metadata()
        const noexcept {
        return metadata_;
    }

    [[nodiscard]] OwnedIngressSourceV1 source() const noexcept {
        return source_;
    }
    [[nodiscard]] std::uint8_t source_slot() const noexcept {
        return static_cast<std::uint8_t>(source_);
    }
    [[nodiscard]] const l2flow::sdk::MessageKey& key() const noexcept {
        return key_;
    }

    [[nodiscard]] const l2flow::sdk::VendorHeadBytes&
    vendor_head_bytes() const noexcept {
        return vendor_head_bytes_;
    }
    [[nodiscard]] l2flow::sdk::VendorHeadView vendor_head()
        const noexcept {
        return l2flow::sdk::VendorHeadView(vendor_head_bytes_);
    }
    [[nodiscard]] std::span<const std::byte> body() const noexcept;
    [[nodiscard]] std::size_t wire_size() const noexcept {
        return l2flow::sdk::kVendorHeadBytes +
               static_cast<std::size_t>(body_size_);
    }

private:
    friend class OwnedIngressMessageHandleV1;
    friend class OwnedIngressMessagePoolStateV1;

    OwnedIngressMessageV1(
        const OwnedIngressMetadataV1& metadata,
        const OwnedIngressMessageInspectionV1& inspection,
        OwnedIngressMessagePoolStateV1* pool_state,
        std::byte* body_data,
        std::uint8_t size_class_index) noexcept;
    ~OwnedIngressMessageV1() = default;

    void AddReference() const noexcept;
    void ReleaseReference() const noexcept;
    [[nodiscard]] std::byte* mutable_body_data() noexcept;
    [[nodiscard]] const std::byte* body_data() const noexcept;

    OwnedIngressMetadataV1 metadata_{};
    OwnedIngressMessagePoolStateV1* pool_state_ = nullptr;
    std::byte* body_data_ = nullptr;
    l2flow::sdk::VendorHeadBytes vendor_head_bytes_{};
    l2flow::sdk::MessageKey key_{};
    std::uint32_t body_size_ = 0U;
    mutable std::atomic<std::uint32_t> references_{1U};
    OwnedIngressSourceV1 source_ = OwnedIngressSourceV1::kShanghaiTick;
    std::uint8_t size_class_index_ = 0U;
};

struct OwnedIngressMessagePoolConfigV1 final {
    std::uint32_t maximum_message_bytes = 0U;
    std::size_t maximum_inflight_messages = 0U;
    // Creation-time hot-set reservation. Both fields are zero to disable it,
    // or both are nonzero. Blocks are allocated and physically touched before
    // SDK Connect. At most prewarm_message_count simultaneously retained
    // messages no larger than prewarm_message_bytes avoid both allocator
    // entry and first-touch page faults in the callback. Larger legal
    // messages use the bounded size-class fallback.
    std::uint32_t prewarm_message_bytes = 0U;
    std::size_t prewarm_message_count = 0U;
    // Enables the callback-oriented free-cache path. Acquire calls must not
    // overlap when this is true (they may migrate between threads when an
    // external mutex provides that serialization). Recycle remains safe from
    // any number of threads. Keep false for the general multi-acquirer pool.
    bool serialized_acquire = false;
};

struct OwnedIngressMessagePoolSnapshotV1 final {
    std::uint32_t maximum_message_bytes = 0U;
    std::size_t maximum_inflight_messages = 0U;
    std::uint32_t prewarm_message_bytes = 0U;
    std::size_t prewarm_message_count = 0U;
    bool serialized_acquire = false;
    std::size_t active_messages = 0U;
    std::size_t allocated_blocks = 0U;
    std::size_t cached_blocks = 0U;
    std::size_t allocated_bytes = 0U;
    bool retired = false;
};

// Bounded, thread-safe size-class storage. Acquire is allocation-free after a
// suitable block has been cached. The default configuration supports
// concurrent acquirers. The explicit serialized-acquire mode instead requires
// externally non-overlapping Acquire calls and uses an acquirer-private cache;
// Recycle and Snapshot remain thread-safe. Destroying the pool stops new
// acquisition and frees idle blocks; blocks still referenced by handles
// remain valid and are deleted safely by whichever thread performs their
// final release.
class OwnedIngressMessagePoolV1 final {
public:
    OwnedIngressMessagePoolV1(
        const OwnedIngressMessagePoolV1&) = delete;
    OwnedIngressMessagePoolV1& operator=(
        const OwnedIngressMessagePoolV1&) = delete;
    OwnedIngressMessagePoolV1(OwnedIngressMessagePoolV1&&) = delete;
    OwnedIngressMessagePoolV1& operator=(
        OwnedIngressMessagePoolV1&&) = delete;
    ~OwnedIngressMessagePoolV1();

    [[nodiscard]] static OwnedIngressMessageErrorV1 Create(
        OwnedIngressMessagePoolConfigV1 config,
        std::unique_ptr<OwnedIngressMessagePoolV1>* output) noexcept;

    [[nodiscard]] OwnedIngressMessageErrorV1 Acquire(
        const OwnedIngressMessageInspectionV1& inspection,
        const OwnedIngressMetadataV1& metadata,
        OwnedIngressMessageHandleV1* output) noexcept;

    // Idempotently stops new block reservation and frees idle blocks. Existing
    // handles remain valid and release their blocks safely. In the default
    // multi-acquirer mode, an Acquire which reserved an active block before
    // Retire may finish constructing and return its handle after Retire has
    // returned; the retained lifetime reference keeps that operation safe.
    // An Acquire that has not reserved a block observes retirement and returns
    // kPoolExhausted. Serialized mode closes its acquire gate before returning.
    void Retire() noexcept;

    [[nodiscard]] OwnedIngressMessagePoolSnapshotV1 Snapshot()
        const noexcept;

private:
    explicit OwnedIngressMessagePoolV1(
        OwnedIngressMessagePoolStateV1* state) noexcept
        : state_(state) {}

    OwnedIngressMessagePoolStateV1* state_ = nullptr;
};

}  // namespace l2flow::realtime
