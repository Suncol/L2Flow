#include "l2flow/common/sha256.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <exception>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <utility>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace l2flow::common {
namespace {

constexpr std::array<std::uint32_t, 64> kRoundConstants = {
    0x428a2f98U, 0x71374491U, 0xb5c0fbcfU, 0xe9b5dba5U,
    0x3956c25bU, 0x59f111f1U, 0x923f82a4U, 0xab1c5ed5U,
    0xd807aa98U, 0x12835b01U, 0x243185beU, 0x550c7dc3U,
    0x72be5d74U, 0x80deb1feU, 0x9bdc06a7U, 0xc19bf174U,
    0xe49b69c1U, 0xefbe4786U, 0x0fc19dc6U, 0x240ca1ccU,
    0x2de92c6fU, 0x4a7484aaU, 0x5cb0a9dcU, 0x76f988daU,
    0x983e5152U, 0xa831c66dU, 0xb00327c8U, 0xbf597fc7U,
    0xc6e00bf3U, 0xd5a79147U, 0x06ca6351U, 0x14292967U,
    0x27b70a85U, 0x2e1b2138U, 0x4d2c6dfcU, 0x53380d13U,
    0x650a7354U, 0x766a0abbU, 0x81c2c92eU, 0x92722c85U,
    0xa2bfe8a1U, 0xa81a664bU, 0xc24b8b70U, 0xc76c51a3U,
    0xd192e819U, 0xd6990624U, 0xf40e3585U, 0x106aa070U,
    0x19a4c116U, 0x1e376c08U, 0x2748774cU, 0x34b0bcb5U,
    0x391c0cb3U, 0x4ed8aa4aU, 0x5b9cca4fU, 0x682e6ff3U,
    0x748f82eeU, 0x78a5636fU, 0x84c87814U, 0x8cc70208U,
    0x90befffaU, 0xa4506cebU, 0xbef9a3f7U, 0xc67178f2U};

std::uint32_t LoadBigEndian32(const std::byte* bytes) noexcept {
    return (std::to_integer<std::uint32_t>(bytes[0]) << 24U) |
           (std::to_integer<std::uint32_t>(bytes[1]) << 16U) |
           (std::to_integer<std::uint32_t>(bytes[2]) << 8U) |
           std::to_integer<std::uint32_t>(bytes[3]);
}

void StoreBigEndian32(std::uint32_t value, std::byte* output) noexcept {
    output[0] = static_cast<std::byte>((value >> 24U) & 0xffU);
    output[1] = static_cast<std::byte>((value >> 16U) & 0xffU);
    output[2] = static_cast<std::byte>((value >> 8U) & 0xffU);
    output[3] = static_cast<std::byte>(value & 0xffU);
}

void StoreBigEndian64(std::uint64_t value, std::byte* output) noexcept {
    for (std::size_t index = 0; index < 8U; ++index) {
        const unsigned int shift =
            static_cast<unsigned int>((7U - index) * 8U);
        output[index] =
            static_cast<std::byte>((value >> shift) & 0xffU);
    }
}

int HexNibble(char character) noexcept {
    if (character >= '0' && character <= '9') {
        return character - '0';
    }
    if (character >= 'a' && character <= 'f') {
        return character - 'a' + 10;
    }
    if (character >= 'A' && character <= 'F') {
        return character - 'A' + 10;
    }
    return -1;
}

void SetError(std::string* error, std::string message) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        *error = std::move(message);
    } catch (...) {
        // Error reporting must not turn a failed preflight into an exception.
    }
}

class OwnedFd final {
public:
    explicit OwnedFd(int fd = -1) noexcept : fd_(fd) {}

    ~OwnedFd() {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
    }

    OwnedFd(const OwnedFd&) = delete;
    OwnedFd& operator=(const OwnedFd&) = delete;

    [[nodiscard]] int get() const noexcept {
        return fd_;
    }

private:
    int fd_;
};

[[nodiscard]] bool SameHashSourceMetadata(
    const struct stat& before,
    const struct stat& after) noexcept {
    return before.st_dev == after.st_dev &&
           before.st_ino == after.st_ino &&
           before.st_mode == after.st_mode &&
           before.st_size == after.st_size &&
           before.st_mtim.tv_sec == after.st_mtim.tv_sec &&
           before.st_mtim.tv_nsec == after.st_mtim.tv_nsec &&
           before.st_ctim.tv_sec == after.st_ctim.tv_sec &&
           before.st_ctim.tv_nsec == after.st_ctim.tv_nsec;
}

[[nodiscard]] bool HashOpenRegularFd(
    int fd,
    Sha256Digest* digest,
    std::string* error,
    std::optional<std::uint64_t> maximum_size) {
    struct stat before {};
    if (::fstat(fd, &before) != 0) {
        SetError(
            error,
            std::string("cannot inspect SHA-256 source: ") +
                std::strerror(errno));
        return false;
    }
    if (!S_ISREG(before.st_mode) || before.st_size < 0) {
        SetError(error, "SHA-256 source is not a regular file");
        return false;
    }

    const std::uint64_t size =
        static_cast<std::uint64_t>(before.st_size);
    if (maximum_size.has_value() && size > *maximum_size) {
        SetError(
            error,
            "SHA-256 source exceeds the configured size bound");
        return false;
    }

    Sha256Hasher state;
    std::array<std::byte, 64U * 1024U> buffer{};
    std::uint64_t offset = 0U;
    while (offset < size) {
        const std::uint64_t remaining = size - offset;
        const std::size_t requested =
            remaining <
                    static_cast<std::uint64_t>(buffer.size())
                ? static_cast<std::size_t>(remaining)
                : buffer.size();
        ssize_t count = -1;
        do {
            count = ::pread(
                fd,
                buffer.data(),
                requested,
                static_cast<off_t>(offset));
        } while (count < 0 && errno == EINTR);
        if (count < 0) {
            SetError(
                error,
                std::string("cannot read SHA-256 source: ") +
                    std::strerror(errno));
            return false;
        }
        if (count == 0) {
            SetError(error, "SHA-256 source ended during hashing");
            return false;
        }
        const std::size_t unsigned_count =
            static_cast<std::size_t>(count);
        if (!state.Update(std::span<const std::byte>(
                buffer.data(), unsigned_count))) {
            SetError(error, "SHA-256 source is too large");
            return false;
        }
        offset += static_cast<std::uint64_t>(unsigned_count);
    }

    std::byte extra{};
    ssize_t extra_count = -1;
    do {
        extra_count = ::pread(
            fd,
            &extra,
            1U,
            static_cast<off_t>(size));
    } while (extra_count < 0 && errno == EINTR);
    if (extra_count < 0) {
        SetError(
            error,
            std::string("cannot confirm SHA-256 source length: ") +
                std::strerror(errno));
        return false;
    }
    if (extra_count != 0) {
        SetError(
            error,
            "SHA-256 source changed length during hashing");
        return false;
    }

    struct stat after {};
    if (::fstat(fd, &after) != 0) {
        SetError(
            error,
            std::string("cannot reinspect SHA-256 source: ") +
                std::strerror(errno));
        return false;
    }
    if (!SameHashSourceMetadata(before, after)) {
        SetError(error, "SHA-256 source changed during hashing");
        return false;
    }

    Sha256Digest computed{};
    if (!state.Finalize(&computed)) {
        SetError(error, "cannot finalize SHA-256 source");
        return false;
    }
    *digest = computed;
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

}  // namespace

bool Sha256Hasher::Update(
    std::span<const std::byte> bytes) noexcept {
    constexpr std::uint64_t maximum_bytes =
        std::numeric_limits<std::uint64_t>::max() / 8U;
    if (finalized_ ||
        bytes.size() >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint64_t>::max()) ||
        static_cast<std::uint64_t>(bytes.size()) >
            maximum_bytes - total_bytes_) {
        return false;
    }
    total_bytes_ += static_cast<std::uint64_t>(bytes.size());

    if (buffer_size_ != 0U) {
        const std::size_t copied =
            std::min(bytes.size(), buffer_.size() - buffer_size_);
        std::copy_n(
            bytes.begin(), copied, buffer_.begin() + buffer_size_);
        buffer_size_ += copied;
        bytes = bytes.subspan(copied);
        if (buffer_size_ == buffer_.size()) {
            Transform(buffer_);
            buffer_size_ = 0U;
        } else {
            // copied consumed the whole input: if bytes had contained enough
            // data to fill the block, buffer_size_ would be buffer_.size().
            // Returning here preserves the existing partial block. Falling
            // through would copy an empty span at buffer_[0] and incorrectly
            // reset buffer_size_ to zero.
            return true;
        }
    }

    while (bytes.size() >= buffer_.size()) {
        Transform(bytes.first(buffer_.size()));
        bytes = bytes.subspan(buffer_.size());
    }

    std::copy(bytes.begin(), bytes.end(), buffer_.begin());
    buffer_size_ = bytes.size();
    return true;
}

bool Sha256Hasher::Finalize(
    Sha256Digest* digest) noexcept {
    if (digest == nullptr || finalized_) {
        return false;
    }
    const std::uint64_t bit_length = total_bytes_ * 8U;
    buffer_[buffer_size_++] = std::byte{0x80};

    if (buffer_size_ > 56U) {
        std::fill(
            buffer_.begin() + buffer_size_,
            buffer_.end(),
            std::byte{0});
        Transform(buffer_);
        buffer_size_ = 0U;
    }

    std::fill(
        buffer_.begin() + buffer_size_,
        buffer_.begin() + 56U,
        std::byte{0});
    StoreBigEndian64(bit_length, buffer_.data() + 56U);
    Transform(buffer_);

    Sha256Digest computed{};
    for (std::size_t index = 0U;
         index < state_.size();
         ++index) {
        StoreBigEndian32(
            state_[index], computed.data() + index * 4U);
    }
    *digest = computed;
    finalized_ = true;
    return true;
}

void Sha256Hasher::Transform(
    std::span<const std::byte> block) noexcept {
    std::array<std::uint32_t, 64> words{};
    for (std::size_t index = 0U; index < 16U; ++index) {
        words[index] =
            LoadBigEndian32(block.data() + index * 4U);
    }
    for (std::size_t index = 16U;
         index < words.size();
         ++index) {
        const std::uint32_t s0 =
            std::rotr(words[index - 15U], 7) ^
            std::rotr(words[index - 15U], 18) ^
            (words[index - 15U] >> 3U);
        const std::uint32_t s1 =
            std::rotr(words[index - 2U], 17) ^
            std::rotr(words[index - 2U], 19) ^
            (words[index - 2U] >> 10U);
        words[index] =
            words[index - 16U] + s0 +
            words[index - 7U] + s1;
    }

    std::uint32_t a = state_[0];
    std::uint32_t b = state_[1];
    std::uint32_t c = state_[2];
    std::uint32_t d = state_[3];
    std::uint32_t e = state_[4];
    std::uint32_t f = state_[5];
    std::uint32_t g = state_[6];
    std::uint32_t h = state_[7];

    for (std::size_t index = 0U;
         index < words.size();
         ++index) {
        const std::uint32_t sigma1 =
            std::rotr(e, 6) ^
            std::rotr(e, 11) ^
            std::rotr(e, 25);
        const std::uint32_t choose =
            (e & f) ^ (~e & g);
        const std::uint32_t temporary1 =
            h + sigma1 + choose +
            kRoundConstants[index] + words[index];
        const std::uint32_t sigma0 =
            std::rotr(a, 2) ^
            std::rotr(a, 13) ^
            std::rotr(a, 22);
        const std::uint32_t majority =
            (a & b) ^ (a & c) ^ (b & c);
        const std::uint32_t temporary2 =
            sigma0 + majority;

        h = g;
        g = f;
        f = e;
        e = d + temporary1;
        d = c;
        c = b;
        b = a;
        a = temporary1 + temporary2;
    }

    state_[0] += a;
    state_[1] += b;
    state_[2] += c;
    state_[3] += d;
    state_[4] += e;
    state_[5] += f;
    state_[6] += g;
    state_[7] += h;
}

Sha256Digest ComputeSha256(std::span<const std::byte> bytes) noexcept {
    Sha256Hasher state;
    Sha256Digest digest{};
    if (!state.Update(bytes) ||
        !state.Finalize(&digest)) {
        return {};
    }
    return digest;
}

Sha256Digest ComputeSha256(std::string_view text) noexcept {
    return ComputeSha256(std::as_bytes(std::span{text.data(), text.size()}));
}

bool ComputeFileSha256(const std::filesystem::path& path,
                       Sha256Digest* digest,
                       std::string* error,
                       std::optional<std::uint64_t>
                           maximum_size) noexcept {
    if (digest == nullptr) {
        SetError(error, "SHA-256 output pointer is null");
        return false;
    }

    try {
        if (path.empty() ||
            path.native().find('\0') !=
                std::string::npos) {
            SetError(
                error,
                "SHA-256 source path is empty or contains NUL");
            return false;
        }
        int fd = -1;
        do {
            fd = ::open(
                path.c_str(),
                O_RDONLY | O_CLOEXEC | O_NOFOLLOW |
                    O_NONBLOCK);
        } while (fd < 0 && errno == EINTR);
        if (fd < 0) {
            SetError(
                error,
                std::string("cannot open SHA-256 source: ") +
                    std::strerror(errno));
            return false;
        }
        const OwnedFd source(fd);
        return HashOpenRegularFd(
            source.get(), digest, error, maximum_size);
    } catch (const std::exception& exception) {
        static_cast<void>(exception);
        SetError(
            error,
            "exception while hashing file");
        return false;
    } catch (...) {
        SetError(error,
                 "unknown exception while hashing file");
        return false;
    }
}

bool ComputeFileSha256ForOpenFd(
    int fd,
    Sha256Digest* digest,
    std::string* error,
    std::optional<std::uint64_t> maximum_size) noexcept {
    if (digest == nullptr) {
        SetError(error, "SHA-256 output pointer is null");
        return false;
    }
    if (fd < 0) {
        SetError(error, "SHA-256 source descriptor is invalid");
        return false;
    }

    try {
        int duplicate = -1;
        do {
            duplicate = ::fcntl(fd, F_DUPFD_CLOEXEC, 0);
        } while (duplicate < 0 && errno == EINTR);
        if (duplicate < 0) {
            SetError(
                error,
                std::string("cannot duplicate SHA-256 source: ") +
                    std::strerror(errno));
            return false;
        }
        const OwnedFd source(duplicate);
        return HashOpenRegularFd(
            source.get(), digest, error, maximum_size);
    } catch (const std::exception& exception) {
        static_cast<void>(exception);
        SetError(error, "exception while hashing open file");
        return false;
    } catch (...) {
        SetError(error, "unknown exception while hashing open file");
        return false;
    }
}

std::string Sha256Hex(const Sha256Digest& digest) {
    constexpr char kHex[] = "0123456789abcdef";
    std::string result;
    result.resize(digest.size() * 2U);
    for (std::size_t index = 0; index < digest.size(); ++index) {
        const unsigned int value =
            std::to_integer<unsigned int>(digest[index]);
        result[index * 2U] = kHex[(value >> 4U) & 0x0fU];
        result[index * 2U + 1U] = kHex[value & 0x0fU];
    }
    return result;
}

bool ParseSha256Hex(std::string_view text,
                    Sha256Digest* digest,
                    std::string* error) noexcept {
    if (digest == nullptr) {
        SetError(error, "SHA-256 output pointer is null");
        return false;
    }
    if (text.size() != digest->size() * 2U) {
        SetError(error, "SHA-256 text must contain exactly 64 hex characters");
        return false;
    }

    Sha256Digest parsed{};
    for (std::size_t index = 0; index < parsed.size(); ++index) {
        const int high = HexNibble(text[index * 2U]);
        const int low = HexNibble(text[index * 2U + 1U]);
        if (high < 0 || low < 0) {
            SetError(error, "SHA-256 text contains a non-hex character");
            return false;
        }
        parsed[index] =
            static_cast<std::byte>((high << 4) | low);
    }
    *digest = parsed;
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

}  // namespace l2flow::common
