#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/sdk/market_message_catalog_v1.h"
#include "l2flow/sdk/vendor_head_view.h"

#include "mdl_api.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

namespace l2flow::realtime {

inline constexpr std::size_t kOwnedIngressSourceCountV1 = 4U;
inline constexpr std::size_t kRequiredOwnedIngressMessageCountV1 =
    l2flow::sdk::kProductionMessageCountV1;
static_assert(kRequiredOwnedIngressMessageCountV1 == 5U);

// These are the only market messages admitted by the production realtime
// ingress.  Shanghai and Shenzhen each have a snapshot source; the two
// Shenzhen tick message types deliberately share one serial source.
enum class OwnedIngressSourceV1 : std::uint8_t {
    kShanghaiSnapshot = 0U,
    kShanghaiTick = 1U,
    kShenzhenSnapshot = 2U,
    kShenzhenTick = 3U,
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
// callback. They describe prefixes owned by this process, not vendor event
// time and not WAL durability. A successful message owns both sequence
// values exactly once; a rejected callback must not advance either counter.
// UINT64_MAX is reserved as the exhaustion sentinel so an exclusive
// generation cut can always be represented without wraparound.
struct OwnedIngressMetadataV1 final {
    l2flow::common::Identity128 run_id{};
    std::uint64_t global_ingress_sequence = 0U;
    std::uint64_t source_sequence = 0U;
    std::uint64_t recv_realtime_ns = 0U;
    std::uint64_t recv_monotonic_ns = 0U;
};

enum class OwnedIngressCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
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
    kResourceExhausted,
};

[[nodiscard]] std::string_view OwnedIngressCreateErrorNameV1(
    OwnedIngressCreateErrorV1 error) noexcept;

// Immutable ownership boundary between the vendor callback and every
// downstream consumer. The callback copies the packed vendor head once and
// the declared body once. Decoder and optional WAL consumers share the same
// const object; audit-sink state is not part of this schema.
class OwnedIngressMessageV1 final {
public:
    OwnedIngressMessageV1(const OwnedIngressMessageV1&) = delete;
    OwnedIngressMessageV1& operator=(
        const OwnedIngressMessageV1&) = delete;
    OwnedIngressMessageV1(OwnedIngressMessageV1&&) = delete;
    OwnedIngressMessageV1& operator=(
        OwnedIngressMessageV1&&) = delete;
    ~OwnedIngressMessageV1() = default;

    [[nodiscard]] static OwnedIngressCreateErrorV1 Create(
        const datayes::mdl::MDLMessage* message,
        const OwnedIngressMetadataV1& metadata,
        std::uint32_t maximum_message_bytes,
        std::shared_ptr<const OwnedIngressMessageV1>* output) noexcept;

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
    [[nodiscard]] std::span<const std::byte> body() const noexcept {
        return body_;
    }
    [[nodiscard]] std::size_t wire_size() const noexcept {
        return vendor_head_bytes_.size() + body_.size();
    }

private:
    explicit OwnedIngressMessageV1(
        OwnedIngressMetadataV1 metadata) noexcept;

    OwnedIngressMetadataV1 metadata_{};
    OwnedIngressSourceV1 source_ =
        OwnedIngressSourceV1::kShanghaiSnapshot;
    l2flow::sdk::MessageKey key_{};
    l2flow::sdk::VendorHeadBytes vendor_head_bytes_{};
    std::vector<std::byte> body_;
};

using OwnedIngressMessageHandleV1 =
    std::shared_ptr<const OwnedIngressMessageV1>;

}  // namespace l2flow::realtime
