#pragma once

#include "l2flow/ingress/raw_ingress_app.h"
#include "l2flow/ingress/raw_reserve_coordinator.h"
#include "l2flow/ingress/raw_writer_lease.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

namespace l2flow::ingress {

class RawActiveBoundWalSinkV1;

enum class RawCleanStopGateFailureV1 : std::uint8_t {
    kNone = 0U,
    kInvalidConfiguration,
    kAlreadyFailed,
    kEvidenceMismatch,
    kAuthorizationRejected,
    kTargetMismatch,
    kJournalEvidenceInvalid,
    kManifestEvidenceInvalid,
    kCertificateBuildFailed,
    kCertificatePublishFailed,
    kCoordinatorUnregisterFailed,
    kAllocationFailure,
};

[[nodiscard]] std::string_view
RawCleanStopGateFailureV1Name(
    RawCleanStopGateFailureV1 failure) noexcept;

// Concrete normal-ACTIVE clean-stop barrier. The gate acquires one exact
// writer-generation/route capability before reading terminal evidence,
// reloads the durable journal header and last marker from their retained
// canonical inode, reloads the closed manifest, builds the non-forgeable
// SealedRawCertificate capability, publishes/reuses it, and finally consumes
// its retained-fd receipt to unregister the route.
//
// The referenced writer lease must outlive this gate. A failed attempt is
// deliberately not retryable in-process: restart must fence the writer and
// enter RECOVERING before adopting any publication crash window.
class RawPosixCleanStopGateV1 final
    : public RawIngressCleanStopGateV1 {
public:
    ~RawPosixCleanStopGateV1() override;

    RawPosixCleanStopGateV1(
        const RawPosixCleanStopGateV1&) = delete;
    RawPosixCleanStopGateV1& operator=(
        const RawPosixCleanStopGateV1&) = delete;
    RawPosixCleanStopGateV1(
        RawPosixCleanStopGateV1&&) = delete;
    RawPosixCleanStopGateV1& operator=(
        RawPosixCleanStopGateV1&&) = delete;

    [[nodiscard]] bool Complete(
        const RawIngressCleanStopEvidenceV1&
            evidence) noexcept override;

    [[nodiscard]] RawCleanStopGateFailureV1
    failure() const noexcept {
        return static_cast<RawCleanStopGateFailureV1>(
            failure_.load(std::memory_order_acquire));
    }
    [[nodiscard]] std::string diagnostic() const;

private:
    friend std::unique_ptr<RawPosixCleanStopGateV1>
    CreateRawPosixCleanStopGateV1(
        RawWriterLease&,
        RawReserveRegistryCoordinatorV1&,
        RawReserveRegistryEntryKeyV1,
        std::string_view,
        std::size_t,
        std::string*) noexcept;

    RawPosixCleanStopGateV1(
        RawWriterLease& lease,
        int retained_journal_fd,
        std::shared_ptr<
            RawReserveRegistryCoordinatorV1> coordinator,
        RawReserveRegistryEntryKeyV1 key,
        std::string stream_slug,
        std::size_t maximum_manifest_bytes) noexcept;

    void Fail(
        RawCleanStopGateFailureV1 failure,
        std::string_view diagnostic) noexcept;

    RawWriterLease& lease_;
    int retained_journal_fd_ = -1;
    std::shared_ptr<
        RawReserveRegistryCoordinatorV1> coordinator_;
    RawReserveRegistryEntryKeyV1 key_{};
    std::string stream_slug_;
    std::size_t maximum_manifest_bytes_ = 0U;

    mutable std::mutex mutex_;
    bool attempted_ = false;
    bool completed_ = false;
    std::string diagnostic_;
    std::atomic<std::uint8_t> failure_{
        static_cast<std::uint8_t>(
            RawCleanStopGateFailureV1::kNone)};
};

[[nodiscard]] std::unique_ptr<RawPosixCleanStopGateV1>
CreateRawPosixCleanStopGateV1(
    RawWriterLease& lease,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawReserveRegistryEntryKeyV1 key,
    std::string_view stream_slug,
    std::size_t maximum_manifest_bytes,
    std::string* error = nullptr) noexcept;

// Convenience composition for an owning production sink. It verifies the
// exact ACTIVE key/writer/target binding before borrowing the sink-owned
// locked writer lease. The sink must outlive the returned gate.
[[nodiscard]] std::unique_ptr<RawPosixCleanStopGateV1>
CreateRawPosixCleanStopGateV1(
    RawActiveBoundWalSinkV1& active_sink,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawReserveRegistryEntryKeyV1 key,
    std::string_view stream_slug,
    std::size_t maximum_manifest_bytes,
    std::string* error = nullptr) noexcept;

}  // namespace l2flow::ingress
