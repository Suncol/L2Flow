#pragma once

#include "mdl_api.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace l2flow::sdk {

enum class IngressKind : std::uint8_t {
    ShSnapshot = 0,
    ShTick = 1,
    SzSnapshot = 2,
    SzTick = 3,
};

struct MessageKey {
    std::uint8_t service_id = 0;
    std::uint16_t service_version = 0;
    std::uint16_t message_id = 0;

    friend constexpr bool operator==(const MessageKey&,
                                     const MessageKey&) = default;
};

struct IngressSpec {
    IngressKind kind = IngressKind::ShSnapshot;
    std::string_view service_name;
    std::uint32_t source_stream_id = 0;
    std::uint8_t market_service_id = 0;
    int default_work_threads = 0;
    int default_io_threads = 0;
    std::vector<MessageKey> required;
    std::vector<MessageKey> optional;
    std::vector<MessageKey> forbidden;
};

// Returns one of four immutable production stream definitions. The Shenzhen
// order and transaction messages deliberately share one SzTick definition.
const IngressSpec& GetIngressSpec(IngressKind kind);
const std::vector<IngressSpec>& AllIngressSpecs();

// Validates uniqueness, market ownership, and the explicit 6.53 prohibition.
// An empty string means the built-in manifest is internally consistent.
std::string ValidateIngressSpecs();

// Adds only message-level subscriptions. Field filters are intentionally not
// part of the production API. Optional index streams are opt-in.
void AddConfiguredSubscriptions(datayes::mdl::Subscriber& subscriber,
                                const IngressSpec& spec,
                                bool include_optional_indices);

std::string_view ToString(IngressKind kind);
bool ParseIngressKind(std::string_view text, IngressKind* result);

// Returns the frozen SDK fixed-portion lower bound for one of the five
// required market messages. Dynamic strings/lists still require independent
// checked-offset decoding; this function only decides whether a captured
// record is large enough to be a candidate "first legal required record".
[[nodiscard]] std::optional<std::size_t>
RequiredMessageFixedBodyBytes(const MessageKey& key) noexcept;

}  // namespace l2flow::sdk
