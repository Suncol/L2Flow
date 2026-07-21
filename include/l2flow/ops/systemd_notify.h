#pragma once

#include <string>
#include <string_view>

namespace l2flow::ops {

enum class NotifyStatus {
    Sent,
    NotConfigured,
    Error,
};

struct NotifyResult {
    NotifyStatus status = NotifyStatus::NotConfigured;
    std::string error;
};

// Sends one datagram to NOTIFY_SOCKET, including Linux abstract sockets
// represented by an environment value beginning with '@'.
NotifyResult NotifySystemd(std::string_view state) noexcept;

}  // namespace l2flow::ops
