#pragma once

#include "l2flow/sdk/ingress_config.h"

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::sdk {

inline constexpr std::size_t kMaximumEndpointContractBytes = 4096U;

// Immutable proof that every endpoint field was parsed from the exact bytes
// named by contract_sha256. The constructor is private so a caller cannot
// retain an approved hash while substituting a different address, encoding,
// merge, MAC-auth, or server-selection value.
class VerifiedEndpointContract final {
public:
    VerifiedEndpointContract(
        const VerifiedEndpointContract&) = delete;
    VerifiedEndpointContract& operator=(
        const VerifiedEndpointContract&) = delete;
    VerifiedEndpointContract(
        VerifiedEndpointContract&&) = delete;
    VerifiedEndpointContract& operator=(
        VerifiedEndpointContract&&) = delete;
    ~VerifiedEndpointContract() = default;

    [[nodiscard]] IngressKind ingress_kind() const noexcept {
        return ingress_kind_;
    }
    [[nodiscard]] const std::string& name() const noexcept {
        return contract_.name;
    }
    [[nodiscard]] const std::string&
    resolved_server_address() const noexcept {
        return contract_.resolved_server_address;
    }
    [[nodiscard]] const std::string&
    contract_sha256() const noexcept {
        return contract_.contract_sha256;
    }
    [[nodiscard]] datayes::mdl::MDLMessageEncoding
    message_encoding() const noexcept {
        return contract_.message_encoding;
    }
    [[nodiscard]] bool merge_message() const noexcept {
        return contract_.merge_message;
    }
    [[nodiscard]] bool send_mac_auth() const noexcept {
        return *contract_.send_mac_auth;
    }
    [[nodiscard]] bool server_select() const noexcept {
        return contract_.server_select;
    }

    // Phase-1 compatibility bridge. The returned value is a copy; mutating it
    // cannot alter this verified capability.
    [[nodiscard]] EndpointContract CopyValue() const;

private:
    friend std::shared_ptr<const VerifiedEndpointContract>
    VerifyEndpointContractBytes(
        std::string_view,
        std::string_view,
        IngressKind,
        std::string*) noexcept;

    VerifiedEndpointContract(
        IngressKind ingress_kind,
        EndpointContract contract) noexcept;

    IngressKind ingress_kind_;
    EndpointContract contract_;
};

// Hashes and strictly parses one bounded in-memory document, returning an
// immutable verified capability. This is also the test/embedded-bytes entry
// point; it applies the same parser and hash gate as the file loader.
[[nodiscard]] std::shared_ptr<const VerifiedEndpointContract>
VerifyEndpointContractBytes(
    std::string_view exact_bytes,
    std::string_view expected_sha256,
    IngressKind expected_kind,
    std::string* error = nullptr) noexcept;

// Secure file-snapshot counterpart of VerifyEndpointContractBytes().
[[nodiscard]] std::shared_ptr<const VerifiedEndpointContract>
LoadVerifiedEndpointContractFile(
    const std::string& path,
    std::string_view expected_sha256,
    IngressKind expected_kind,
    std::string* error = nullptr) noexcept;

// Opens and consumes one version-1 endpoint-contract JSON document. The file
// is opened without following its final symlink and is then checked, read,
// hashed, and parsed through that one regular-file descriptor. The expected
// hash must be exactly 64 lowercase hexadecimal digits.
//
// On success, `contract` receives the parsed values and the SHA-256 of the
// exact file bytes in EndpointContract::contract_sha256. On failure,
// `contract` is left unchanged and `error` receives a non-secret diagnostic
// when it is non-null.
bool LoadEndpointContractFile(
    const std::string& path,
    std::string_view expected_sha256,
    IngressKind expected_kind,
    EndpointContract* contract,
    std::string* error) noexcept;

}  // namespace l2flow::sdk
