#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace l2flow::sdk {

struct MessageKey final {
    std::uint8_t service_id = 0U;
    std::uint16_t service_version = 0U;
    std::uint16_t message_id = 0U;

    friend constexpr bool operator==(
        const MessageKey&, const MessageKey&) = default;
};

inline constexpr std::size_t kProductionMessageCountV1 = 3U;

// The single production fact catalog. Snapshots are intentionally absent:
// FAST Tick history is the only recovery authority. Both the physical SDK
// subscription and the owned-ingress classifier consume this array, so their
// tuple sets cannot silently drift apart.
inline constexpr std::array<MessageKey, kProductionMessageCountV1>
    kProductionMessageKeysV1{{
        {4U, 101U, 24U},
        {6U, 101U, 33U},
        {6U, 101U, 36U},
    }};

inline constexpr MessageKey kForbiddenCombinedTickMessageKeyV1{
    6U, 101U, 53U};

}  // namespace l2flow::sdk
