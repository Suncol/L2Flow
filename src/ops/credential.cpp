#include "l2flow/ops/credential.h"

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>
#include <utility>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace l2flow::ops {
namespace {

class FileDescriptor {
public:
    explicit FileDescriptor(int value) noexcept : value_(value) {}
    ~FileDescriptor() {
        if (value_ >= 0) {
            static_cast<void>(::close(value_));
        }
    }
    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;

    [[nodiscard]] int get() const noexcept { return value_; }

private:
    int value_;
};

std::string ErrnoMessage(const char* operation, int error_number) {
    return std::string(operation) + " failed: " +
           std::strerror(error_number);
}

}  // namespace

CredentialResult ReadCredentialFile(const std::string& path,
                                    const CredentialPolicy& policy) {
    CredentialResult result;
    if (path.empty() ||
        path.find('\0') != std::string::npos ||
        !std::filesystem::path(path).is_absolute()) {
        result.error =
            "credential path must be absolute and contain no NUL byte";
        return result;
    }
    if (policy.max_bytes == 0U ||
        policy.max_bytes >
            static_cast<std::size_t>(std::numeric_limits<ssize_t>::max())) {
        result.error = "credential max_bytes is outside the supported range";
        return result;
    }

    const int raw_fd = ::open(
        path.c_str(),
        O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK | O_NOCTTY);
    if (raw_fd < 0) {
        result.error = ErrnoMessage("open credential", errno);
        return result;
    }
    FileDescriptor fd(raw_fd);

    struct stat metadata {};
    if (::fstat(fd.get(), &metadata) != 0) {
        result.error = ErrnoMessage("stat credential", errno);
        return result;
    }
    if (!S_ISREG(metadata.st_mode)) {
        result.error = "credential is not a regular file";
        return result;
    }
    if (metadata.st_uid != policy.expected_owner) {
        result.error = "credential owner does not match policy";
        return result;
    }
    const mode_t permission_bits = metadata.st_mode & 07777;
    if (policy.require_mode_0400 && permission_bits != 0400) {
        result.error = "credential permissions must be exactly 0400";
        return result;
    }
    if (metadata.st_size < 0 ||
        static_cast<std::uintmax_t>(metadata.st_size) >
            static_cast<std::uintmax_t>(policy.max_bytes)) {
        result.error = "credential exceeds the configured size limit";
        return result;
    }

    result.token.resize(static_cast<std::size_t>(metadata.st_size));
    std::size_t offset = 0U;
    while (offset < result.token.size()) {
        const ssize_t count = ::read(
            fd.get(), result.token.data() + offset,
            result.token.size() - offset);
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            result.token.clear();
            result.error = ErrnoMessage("read credential", errno);
            return result;
        }
        if (count == 0) {
            result.token.clear();
            result.error = "credential was truncated while being read";
            return result;
        }
        offset += static_cast<std::size_t>(count);
    }

    char extra = '\0';
    for (;;) {
        const ssize_t count = ::read(fd.get(), &extra, 1U);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count < 0) {
            result.token.clear();
            result.error = ErrnoMessage("verify credential length", errno);
            return result;
        }
        if (count != 0) {
            result.token.clear();
            result.error = "credential grew while being read";
            return result;
        }
        break;
    }

    if (!result.token.empty() && result.token.back() == '\n') {
        result.token.pop_back();
        if (!result.token.empty() && result.token.back() == '\r') {
            result.token.pop_back();
        }
    }
    if (result.token.empty()) {
        result.error = "credential token is empty";
        return result;
    }
    if (result.token.find('\0') != std::string::npos) {
        result.token.clear();
        result.error = "credential token contains a NUL byte";
        return result;
    }
    return result;
}

std::optional<std::string> ResolveSystemdCredentialPath(
    std::string_view name,
    std::string* error) {
    if (error != nullptr) {
        error->clear();
    }
    if (name.empty() || name == "." || name == ".." ||
        name.find('/') != std::string_view::npos ||
        name.find('\0') != std::string_view::npos) {
        if (error != nullptr) {
            *error = "credential name must be one path component";
        }
        return std::nullopt;
    }
    const char* directory = std::getenv("CREDENTIALS_DIRECTORY");
    if (directory == nullptr || *directory == '\0') {
        if (error != nullptr) {
            *error = "CREDENTIALS_DIRECTORY is not set";
        }
        return std::nullopt;
    }
    std::string path(directory);
    if (!std::filesystem::path(path).is_absolute()) {
        if (error != nullptr) {
            *error = "CREDENTIALS_DIRECTORY must be an absolute path";
        }
        return std::nullopt;
    }
    if (path.back() != '/') {
        path.push_back('/');
    }
    path.append(name);
    return path;
}

}  // namespace l2flow::ops
