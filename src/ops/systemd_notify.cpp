#include "l2flow/ops/systemd_notify.h"

#include <cerrno>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <string>

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace l2flow::ops {

NotifyResult NotifySystemd(std::string_view state) noexcept {
    NotifyResult result;
    try {
        const char* configured = std::getenv("NOTIFY_SOCKET");
        if (configured == nullptr || *configured == '\0') {
            result.status = NotifyStatus::NotConfigured;
            return result;
        }
        if (state.empty()) {
            result.status = NotifyStatus::Error;
            result.error = "systemd notification state is empty";
            return result;
        }
        const std::string path(configured);
        if (path.size() >= sizeof(sockaddr_un::sun_path)) {
            result.status = NotifyStatus::Error;
            result.error = "NOTIFY_SOCKET path is too long";
            return result;
        }

        const int fd = ::socket(
            AF_UNIX,
            SOCK_DGRAM | SOCK_CLOEXEC | SOCK_NONBLOCK,
            0);
        if (fd < 0) {
            result.status = NotifyStatus::Error;
            result.error =
                std::string("socket failed: ") + std::strerror(errno);
            return result;
        }

        struct sockaddr_un address {};
        address.sun_family = AF_UNIX;
        socklen_t address_length = 0;
        if (path.front() == '@') {
            address.sun_path[0] = '\0';
            std::memcpy(address.sun_path + 1, path.data() + 1,
                        path.size() - 1U);
            address_length = static_cast<socklen_t>(
                offsetof(sockaddr_un, sun_path) + path.size());
        } else {
            std::memcpy(address.sun_path, path.data(), path.size());
            address.sun_path[path.size()] = '\0';
            address_length = static_cast<socklen_t>(
                offsetof(sockaddr_un, sun_path) + path.size() + 1U);
        }

        ssize_t sent = 0;
        do {
            sent = ::sendto(
                fd,
                state.data(),
                state.size(),
                MSG_NOSIGNAL | MSG_DONTWAIT,
                reinterpret_cast<const struct sockaddr*>(&address),
                address_length);
        } while (sent < 0 && errno == EINTR);
        const int send_error = sent < 0 ? errno : 0;
        static_cast<void>(::close(fd));
        if (sent < 0) {
            result.status = NotifyStatus::Error;
            result.error = std::string("sendto NOTIFY_SOCKET failed: ") +
                           std::strerror(send_error);
            return result;
        }
        if (static_cast<std::size_t>(sent) != state.size()) {
            result.status = NotifyStatus::Error;
            result.error =
                "sendto NOTIFY_SOCKET returned a short datagram";
            return result;
        }
        result.status = NotifyStatus::Sent;
        return result;
    } catch (...) {
        result.status = NotifyStatus::Error;
        result.error = "systemd notification raised an internal exception";
        return result;
    }
}

}  // namespace l2flow::ops
