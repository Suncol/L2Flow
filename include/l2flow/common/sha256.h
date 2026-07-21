#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace l2flow::common {

using Sha256Digest = std::array<std::byte, 32>;

// Allocation-free incremental SHA-256. Update rejects a message whose bit
// length cannot be represented by SHA-256's 64-bit length field. Finalize is
// one-shot; after it succeeds, both Update and a second Finalize fail without
// changing caller-owned output.
class Sha256Hasher final {
public:
    Sha256Hasher() noexcept = default;

    Sha256Hasher(const Sha256Hasher&) = delete;
    Sha256Hasher& operator=(const Sha256Hasher&) = delete;
    Sha256Hasher(Sha256Hasher&&) = delete;
    Sha256Hasher& operator=(Sha256Hasher&&) = delete;

    [[nodiscard]] bool Update(
        std::span<const std::byte> bytes) noexcept;
    [[nodiscard]] bool Finalize(
        Sha256Digest* digest) noexcept;
    [[nodiscard]] std::uint64_t total_bytes() const noexcept {
        return total_bytes_;
    }
    [[nodiscard]] bool finalized() const noexcept {
        return finalized_;
    }

private:
    void Transform(
        std::span<const std::byte> block) noexcept;

    std::array<std::uint32_t, 8> state_{{
        0x6a09e667U,
        0xbb67ae85U,
        0x3c6ef372U,
        0xa54ff53aU,
        0x510e527fU,
        0x9b05688cU,
        0x1f83d9abU,
        0x5be0cd19U}};
    std::array<std::byte, 64> buffer_{};
    std::size_t buffer_size_ = 0U;
    std::uint64_t total_bytes_ = 0U;
    bool finalized_ = false;
};

// Computes SHA-256 over an in-memory byte sequence.
Sha256Digest ComputeSha256(std::span<const std::byte> bytes) noexcept;
Sha256Digest ComputeSha256(std::string_view text) noexcept;

// Opens one O_NOFOLLOW|O_NONBLOCK regular-file descriptor and streams its
// exact, metadata-stable bytes through SHA-256. On failure, `digest` is left
// unchanged and `error` receives a path-free reason when it is non-null.
// maximum_size, when set, is enforced before any file bytes are read.
bool ComputeFileSha256(const std::filesystem::path& path,
                       Sha256Digest* digest,
                       std::string* error,
                       std::optional<std::uint64_t> maximum_size =
                           std::nullopt) noexcept;

// Duplicates one already-open regular-file descriptor with close-on-exec and
// hashes it using pread, so the caller's file offset is unchanged. The same
// exact-size snapshot and metadata-stability checks as ComputeFileSha256 are
// applied. On failure, `digest` is left unchanged.
bool ComputeFileSha256ForOpenFd(
    int fd,
    Sha256Digest* digest,
    std::string* error,
    std::optional<std::uint64_t> maximum_size =
        std::nullopt) noexcept;

std::string Sha256Hex(const Sha256Digest& digest);

// Requires exactly 64 hexadecimal characters. Both cases are accepted; output
// is always normalized by Sha256Hex().
bool ParseSha256Hex(std::string_view text,
                    Sha256Digest* digest,
                    std::string* error) noexcept;

}  // namespace l2flow::common
