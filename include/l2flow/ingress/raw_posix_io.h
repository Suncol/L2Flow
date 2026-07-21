#pragma once

#include "l2flow/ingress/raw_reserve_coordinator.h"
#include "l2flow/ingress/raw_wal_writer.h"

#include <memory>
#include <string>
#include <string_view>

namespace l2flow::ingress {

// Validates and adopts two already-open retained descriptors. Each descriptor
// must be an owner-only, singly-linked regular file opened O_RDWR without
// O_APPEND. Ownership transfers only on success.
[[nodiscard]] std::unique_ptr<RawWalIo> AdoptPosixRawWalIo(
    int segment_fd,
    int journal_fd,
    std::string* error = nullptr) noexcept;

// Production target-bound adoption. In addition to the writable-fd checks,
// this requires the exact canonical segment name and durable.journal to name
// the supplied inodes in one retained private stream-day directory. The
// returned RawWalIo retains a duplicate of that directory and implements
// RawReserveMutationTargetProviderV1. Ownership of segment_fd/journal_fd
// transfers only on success; stream_directory_fd always remains caller-owned.
[[nodiscard]] std::unique_ptr<RawWalIo>
AdoptTargetBoundPosixRawWalIo(
    int stream_directory_fd,
    std::string_view canonical_segment_filename,
    int segment_fd,
    int journal_fd,
    std::string* error = nullptr) noexcept;

}  // namespace l2flow::ingress
