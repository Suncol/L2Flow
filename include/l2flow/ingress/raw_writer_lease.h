#pragma once

#include "l2flow/common/identity128.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>

namespace l2flow::ingress {

inline constexpr std::size_t kRawWriterLeaseMarkerBytes = 64U;
inline constexpr std::uint16_t kRawWriterLeaseVersion = 1U;
inline constexpr std::array<std::byte, 8U> kRawWriterLeaseMagic{
    std::byte{'L'}, std::byte{'2'}, std::byte{'R'}, std::byte{'W'},
    std::byte{'L'}, std::byte{'1'}, std::byte{0}, std::byte{0}};
inline constexpr char kRawWriterLeaseFilename[] =
    "writer.lease";
inline constexpr char kRawWriterLeaseTemporaryFilename[] =
    ".writer.lease.raw-writer-lease.tmp";
inline constexpr std::string_view
    kRawWriterLeaseAttemptTemporaryPrefix =
        ".writer.lease.raw-writer-lease-v1.";

namespace raw_writer_lease_offset {
inline constexpr std::size_t kMagic = 0U;
inline constexpr std::size_t kVersion = 8U;
inline constexpr std::size_t kMarkerSize = 10U;
inline constexpr std::size_t kSourceStreamId = 12U;
inline constexpr std::size_t kCaptureDate = 16U;
inline constexpr std::size_t kReserved = 20U;
inline constexpr std::size_t kCrc32c = 60U;
}  // namespace raw_writer_lease_offset

struct RawWriterLeaseMarkerV1 final {
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    std::uint32_t crc32c = 0U;
};

[[nodiscard]] bool EncodeRawWriterLeaseMarkerV1(
    const RawWriterLeaseMarkerV1& marker,
    std::array<std::byte, kRawWriterLeaseMarkerBytes>* wire) noexcept;
[[nodiscard]] bool DecodeRawWriterLeaseMarkerV1(
    std::span<const std::byte> wire,
    RawWriterLeaseMarkerV1* marker) noexcept;

class RawWriterLease final {
public:
    ~RawWriterLease();

    RawWriterLease(const RawWriterLease&) = delete;
    RawWriterLease& operator=(const RawWriterLease&) = delete;
    RawWriterLease(RawWriterLease&&) = delete;
    RawWriterLease& operator=(RawWriterLease&&) = delete;

    [[nodiscard]] int descriptor() const noexcept {
        return lease_fd_;
    }
    [[nodiscard]] int directory_descriptor() const noexcept {
        return directory_fd_;
    }
    [[nodiscard]] std::uint32_t source_stream_id() const noexcept {
        return marker_.source_stream_id;
    }
    [[nodiscard]] std::uint32_t capture_date() const noexcept {
        return marker_.capture_date;
    }

private:
    friend std::unique_ptr<RawWriterLease>
    AcquireRawWriterLeaseAt(
        int,
        std::uint32_t,
        std::uint32_t,
        std::string*) noexcept;
    friend std::unique_ptr<RawWriterLease>
    AcquireRawWriterLeaseAtV1(
        int,
        std::uint32_t,
        std::uint32_t,
        const l2flow::common::Identity128&,
        std::string*) noexcept;

    RawWriterLease(
        int directory_fd,
        int lease_fd,
        RawWriterLeaseMarkerV1 marker) noexcept;

    int directory_fd_ = -1;
    int lease_fd_ = -1;
    RawWriterLeaseMarkerV1 marker_{};
};

// directory_fd remains owned by the caller. The returned object retains a
// duplicate directory descriptor and an exclusive nonblocking flock on the
// final lease inode. Missing leases use one deterministic typed temporary
// name and a NOREPLACE publication; ambiguous final+tmp state fails closed.
[[nodiscard]] std::unique_ptr<RawWriterLease>
AcquireRawWriterLeaseAt(
    int directory_fd,
    std::uint32_t source_stream_id,
    std::uint32_t capture_date,
    std::string* error = nullptr) noexcept;

// Production creation/adoption path. The one typed temporary name is derived
// from the durable recovery-attempt identity; a different/second attempt tmp
// is ambiguous and rejected. Existing fixed final leases remain attachable
// regardless of the current attempt, but final+any typed tmp is fatal.
[[nodiscard]] std::string
RawWriterLeaseAttemptTemporaryFilenameV1(
    const l2flow::common::Identity128&
        recovery_attempt);

[[nodiscard]] std::unique_ptr<RawWriterLease>
AcquireRawWriterLeaseAtV1(
    int directory_fd,
    std::uint32_t source_stream_id,
    std::uint32_t capture_date,
    const l2flow::common::Identity128&
        recovery_attempt,
    std::string* error = nullptr) noexcept;

}  // namespace l2flow::ingress
