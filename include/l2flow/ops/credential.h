#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>

#include <sys/types.h>

namespace l2flow::ops {

struct CredentialPolicy {
    uid_t expected_owner = 0;
    std::size_t max_bytes = 64U * 1024U;
    bool require_mode_0400 = true;
};

struct CredentialResult {
    std::string token;
    std::string error;

    [[nodiscard]] bool ok() const noexcept { return error.empty(); }
};

// Reads an absolute-path regular, non-symlink credential file without ever
// including its contents in an error. One trailing LF or CRLF is removed.
CredentialResult ReadCredentialFile(
    const std::string& path,
    const CredentialPolicy& policy = CredentialPolicy{});

// Resolves $CREDENTIALS_DIRECTORY/<name>. Names containing a slash, "." or
// ".." are rejected. The returned path is not opened by this function.
std::optional<std::string> ResolveSystemdCredentialPath(
    std::string_view name,
    std::string* error);

}  // namespace l2flow::ops
