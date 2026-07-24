#pragma once

#include "l2flow/sdk/market_message_catalog_v1.h"

#include <array>
#include <cstddef>

namespace l2flow::sdk {

class SdkSubscriber;

inline constexpr std::size_t kProductionSubscriptionCountV1 =
    kProductionMessageCountV1;

// The one production Subscriber owns exactly these five market message
// tuples.  The order is stable so tests and diagnostics can compare the SDK
// calls without set normalization.
[[nodiscard]] const std::array<MessageKey,
                               kProductionSubscriptionCountV1>&
ProductionSubscriptionKeysV1() noexcept;

[[nodiscard]] bool IsRequiredProductionSubscriptionV1(
    const MessageKey& key) noexcept;

// 6.101.53 is intentionally distinguished from other unsupported tuples: it
// is the vendor CombinedTick stream and must never enter the production data
// chain alongside the independent order and transaction feeds.
[[nodiscard]] bool IsForbiddenProductionSubscriptionV1(
    const MessageKey& key) noexcept;

// Adds exactly ProductionSubscriptionKeysV1(), once each, to one physical
// Subscriber.  There is no optional-index or CombinedTick switch.
void AddProductionSubscriptionsV1(SdkSubscriber& subscriber);

}  // namespace l2flow::sdk
