#include "l2flow/ingress/raw_schema.h"

#include <cstdint>
#include <optional>
#include <string>
#include <utility>

namespace l2flow::ingress {
namespace {

void SetError(
    std::string* error,
    std::string message) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        *error = std::move(message);
    } catch (...) {
        // Verification failure must remain noexcept even under allocation
        // pressure while formatting its diagnostic.
    }
}

}  // namespace

const l2flow::common::Sha256Digest&
RawSchemaSha256Digest() noexcept {
    return kFrozenRawSchemaSha256;
}

std::string_view RawSchemaSha256Hex() noexcept {
    return kFrozenRawSchemaSha256Hex;
}

bool VerifyRawSchemaFile(
    const std::filesystem::path& path,
    std::string* error) noexcept {
    l2flow::common::Sha256Digest actual{};
    std::string hash_error;
    if (!l2flow::common::ComputeFileSha256(
            path,
            &actual,
            &hash_error,
            std::optional<std::uint64_t>{
                static_cast<std::uint64_t>(
                    kFrozenRawSchemaBytes)})) {
        SetError(error, std::move(hash_error));
        return false;
    }
    if (actual != kFrozenRawSchemaSha256) {
        SetError(error, "Raw V1 schema SHA-256 mismatch");
        return false;
    }
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

}  // namespace l2flow::ingress
