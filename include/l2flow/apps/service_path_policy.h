#pragma once

#include <optional>
#include <string>
#include <string_view>

namespace l2flow::apps {

struct IngressServiceConfig;

// Validates that every service path is absolute and unambiguous, and that
// writable outputs cannot alias each other or any immutable/runtime input.
// `resolved_credential_path` supplies the systemd-resolved credential path;
// an explicit config credential path is checked automatically.
// Production startup keeps `require_existing_parents` true so bind-mount
// aliases are decidable. The pure CLI parser may set it false before any
// deployment filesystem is available; startup always repeats the strict form.
//
// Returns an empty string on success. Diagnostics deliberately identify only
// the violated policy and never echo a path.
std::string ValidateIngressServicePathPolicy(
    const IngressServiceConfig& config,
    std::optional<std::string_view> resolved_credential_path =
        std::nullopt,
    bool require_existing_parents = true);

}  // namespace l2flow::apps
