#pragma once

#include "l2flow/control/control_live_worker.h"

#include <cstdint>
#include <memory>
#include <string>

namespace l2flow::control {

using ControlRecordPosixMonotonicNowV1 =
    std::uint64_t (*)(void* context) noexcept;

struct ControlRecordPosixSinkOptionsV1 final {
    ControlRecordPosixMonotonicNowV1 monotonic_now = nullptr;
    void* monotonic_clock_context = nullptr;
};

enum class ControlRecordPosixSinkCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kUnsafeDirectory,
    kDirectoryBusy,
    kAllocationFailure,
};

// One durable canonical file per idempotency key. The sink retains an
// independently opened owner-only directory and a nonblocking exclusive
// flock for its entire lifetime, enforcing one production derived writer.
class ControlRecordPosixSinkV1 final : public ControlRecordSinkV1 {
public:
    ~ControlRecordPosixSinkV1() override;

    ControlRecordPosixSinkV1(const ControlRecordPosixSinkV1&) = delete;
    ControlRecordPosixSinkV1& operator=(
        const ControlRecordPosixSinkV1&) = delete;
    ControlRecordPosixSinkV1(ControlRecordPosixSinkV1&&) = delete;
    ControlRecordPosixSinkV1& operator=(
        ControlRecordPosixSinkV1&&) = delete;

    [[nodiscard]] ControlRecordPublishResultV1 Publish(
        const ControlRecordV1& record,
        const ControlRecordWireV1& canonical_wire,
        std::uint64_t deadline_monotonic_ns) noexcept override;
    [[nodiscard]] std::uint64_t MonotonicNowNs() const noexcept;

private:
    friend struct ControlRecordPosixSinkCreateAccessV1;

    ControlRecordPosixSinkV1(
        int directory_fd,
        std::uint64_t directory_device,
        std::uint64_t directory_inode,
        ControlRecordPosixSinkOptionsV1 options) noexcept;

    int directory_fd_ = -1;
    std::uint64_t directory_device_ = 0U;
    std::uint64_t directory_inode_ = 0U;
    ControlRecordPosixSinkOptionsV1 options_{};
};

[[nodiscard]] ControlRecordPosixSinkCreateErrorV1
CreateControlRecordPosixSinkV1At(
    int retained_directory_fd,
    ControlRecordPosixSinkOptionsV1 options,
    std::unique_ptr<ControlRecordPosixSinkV1>* output,
    std::string* diagnostic = nullptr) noexcept;

[[nodiscard]] bool ControlRecordV1Filename(
    const ControlRecordV1& record,
    std::string* filename) noexcept;

}  // namespace l2flow::control
