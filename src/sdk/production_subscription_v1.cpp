#include "l2flow/sdk/production_subscription_v1.h"

#include "l2flow/sdk/sdk_runtime.h"

#include <algorithm>

namespace l2flow::sdk {
const std::array<MessageKey, kProductionSubscriptionCountV1>&
ProductionSubscriptionKeysV1() noexcept {
    return kProductionMessageKeysV1;
}

bool IsRequiredProductionSubscriptionV1(
    const MessageKey& key) noexcept {
    return std::find(
               kProductionMessageKeysV1.begin(),
               kProductionMessageKeysV1.end(),
               key) != kProductionMessageKeysV1.end();
}

bool IsForbiddenProductionSubscriptionV1(
    const MessageKey& key) noexcept {
    return key == kForbiddenCombinedTickMessageKeyV1;
}

void AddProductionSubscriptionsV1(SdkSubscriber& subscriber) {
    for (const MessageKey& key : kProductionMessageKeysV1) {
        subscriber.AddSubscription(key);
    }
}

}  // namespace l2flow::sdk
