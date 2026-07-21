#pragma once

#include "l2flow/sdk/subscription_manifest.h"

#include <cstddef>
#include <span>

namespace l2flow::ingress {

// Phase-1/2 bounds-only validator shared by the Phase-2 observational gate
// and the Phase-3 authoritative control decoder. It validates only the five
// required message layouts and their dynamic descriptor ranges; it does not
// publish decoded business fields.
[[nodiscard]] bool ValidateRequiredMarketBodyBounds(
    const l2flow::sdk::MessageKey& key,
    std::span<const std::byte> body) noexcept;

}  // namespace l2flow::ingress
