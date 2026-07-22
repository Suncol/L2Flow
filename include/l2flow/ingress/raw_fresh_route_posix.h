#pragma once

#include "l2flow/ingress/raw_wal_stream_posix.h"

#include <cstdint>
#include <string>
#include <string_view>

namespace l2flow::ingress {

enum class RawFreshRoutePosixFailureV1 : std::uint8_t {
    kNone = 0U,
    kInvalidInput,
    kScaffoldingAuthorization,
    kStreamDirectoryScaffold,
    kMaintenanceDirectoryScaffold,
    kWriterLease,
    kJournalAnchor,
    kInitPublication,
    kActiveStream,
    kAllocationFailure,
};

[[nodiscard]] std::string_view
RawFreshRoutePosixFailureV1Name(
    RawFreshRoutePosixFailureV1 failure) noexcept;

struct RawFreshRoutePosixResultV1 final {
    RawFreshRoutePosixFailureV1 failure =
        RawFreshRoutePosixFailureV1::kNone;
    RawReserveCoordinatorErrorV1 coordinator_failure =
        RawReserveCoordinatorErrorV1::kNone;
    RawFreshActivePosixStreamResultV1 active{};
    bool stream_directory_durable = false;
    bool maintenance_directory_durable = false;
    bool writer_lease_durable = false;
    bool journal_anchor_durable = false;
    bool init_publication_attempted = false;

    [[nodiscard]] bool ok() const noexcept {
        return failure ==
                   RawFreshRoutePosixFailureV1::kNone &&
               active.ok();
    }

    // An INIT publication error is conservatively indeterminate because the
    // state-slot barrier may have completed before an error was observed.
    // Once INIT publication is attempted, any later failure requires process
    // fail-stop and takeover/recovery rather than an in-process retry.
    [[nodiscard]] bool requires_fail_stop() const noexcept {
        return !ok() &&
               (init_publication_attempted ||
                active.requires_fail_stop());
    }
};

// Completes an already durable SCAFFOLDING registration through the first
// ACTIVE-bound Raw sink. Registration and route-absence discovery are
// intentionally outside this function: callers must choose/generate the
// identities only after the coordinator's bounded route scan.
//
// Ordering:
//   SCAFFOLDING action
//   -> stream-day directory
//   -> empty maintenance directory
//   -> writer lease
//   -> journal anchor
//   -> durable INIT
//   -> CreateFreshActiveRawPosixStreamV1().
//
// Every pre-INIT mutation is performed while the same shared SCAFFOLDING OFD
// action is retained. No Subscriber is created and Connect is never called.
[[nodiscard]] RawFreshRoutePosixResultV1
CompleteRegisteredFreshRawRouteV1(
    const std::string& raw_root,
    std::string_view stream_slug,
    const RawReserveFreshScaffoldingV1& registration,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawWalWriterConfig logical_writer_config,
    RawPosixWalStreamBackendOptionsV1 backend_options,
    RawSegmentArtifactOptionsV1 artifact_options,
    RawWalStreamLimitsV1 stream_limits,
    std::string* error = nullptr) noexcept;

// Retained-authority production variant. retained_raw_root_fd must identify
// the same directory inode already retained by coordinator. The configured
// Raw-root pathname is deliberately not accepted or reopened here.
[[nodiscard]] RawFreshRoutePosixResultV1
CompleteRegisteredFreshRawRouteAtV1(
    int retained_raw_root_fd,
    std::string_view stream_slug,
    const RawReserveFreshScaffoldingV1& registration,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawWalWriterConfig logical_writer_config,
    RawPosixWalStreamBackendOptionsV1 backend_options,
    RawSegmentArtifactOptionsV1 artifact_options,
    RawWalStreamLimitsV1 stream_limits,
    std::string* error = nullptr) noexcept;

}  // namespace l2flow::ingress
