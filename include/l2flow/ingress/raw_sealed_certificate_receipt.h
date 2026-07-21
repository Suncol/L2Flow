#pragma once

#include "l2flow/ingress/raw_reserve_coordinator.h"
#include "l2flow/ingress/raw_v1.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include <unistd.h>

namespace l2flow::ingress {

class RawWriterLease;
class BuiltSealedRawCertificateV1;
struct SealedRawCertificatePublishResultV1;
class RecoveryMaintenanceReportStoreAccessV1;

[[nodiscard]] SealedRawCertificatePublishResultV1
PublishSealedRawCertificateV1(
    const RawWriterLease& lease,
    std::unique_ptr<RawReserveAuthorizedActionV1>
        authorized_action,
    const BuiltSealedRawCertificateV1& certificate,
    std::string* diagnostic) noexcept;

// Non-forgeable process-local proof that an exact SealedRawCertificateV1
// completed its file-fsync, NOREPLACE publication, maintenance-directory
// fsync and retained-fd readback while the matching ACTIVE generation action
// was held. A receipt is single-use and is not a durable artifact itself.
class SealedRawCertificateReceiptV1 final {
public:
    ~SealedRawCertificateReceiptV1() {
        if (certificate_fd_ >= 0) {
            static_cast<void>(::close(certificate_fd_));
        }
        if (maintenance_directory_fd_ >= 0) {
            static_cast<void>(
                ::close(maintenance_directory_fd_));
        }
    }

    SealedRawCertificateReceiptV1(
        const SealedRawCertificateReceiptV1&) = delete;
    SealedRawCertificateReceiptV1& operator=(
        const SealedRawCertificateReceiptV1&) = delete;
    SealedRawCertificateReceiptV1(
        SealedRawCertificateReceiptV1&&) = delete;
    SealedRawCertificateReceiptV1& operator=(
        SealedRawCertificateReceiptV1&&) = delete;

private:
    friend SealedRawCertificatePublishResultV1
    PublishSealedRawCertificateV1(
        const RawWriterLease&,
        std::unique_ptr<
            RawReserveAuthorizedActionV1>,
        const BuiltSealedRawCertificateV1&,
        std::string*) noexcept;
    friend class RawReserveRegistryCoordinatorV1;

    SealedRawCertificateReceiptV1(
        RawReserveRegistryEntryKeyV1 key,
        RawReserveGenerationActionTokenV1 token,
        RawReserveMutationTargetAnchorV1 target,
        RawV1Digest certificate_sha256,
        std::string filename,
        int maintenance_directory_fd,
        int certificate_fd) noexcept
        : key_(std::move(key)),
          token_(std::move(token)),
          target_(std::move(target)),
          certificate_sha256_(certificate_sha256),
          filename_(std::move(filename)),
          maintenance_directory_fd_(
              maintenance_directory_fd),
          certificate_fd_(certificate_fd) {}

    RawReserveRegistryEntryKeyV1 key_{};
    RawReserveGenerationActionTokenV1 token_{};
    RawReserveMutationTargetAnchorV1 target_;
    RawV1Digest certificate_sha256_{};
    std::string filename_;
    int maintenance_directory_fd_ = -1;
    int certificate_fd_ = -1;
    bool consumed_ = false;
};

// Separate process-local proof for the ordinary recovery terminal path.
// Unlike SealedRawCertificateReceiptV1, this capability can never authorize
// an ACTIVE unregister. It binds the exact RECOVERING+RECOVER_SEAL_ONLY
// attempt/writer/target to the durable certificate barrier and retained
// inode identities. The only next consumer is the terminal maintenance
// report publisher.
class SealedRawRecoveryTerminalReceiptV1 final {
public:
    ~SealedRawRecoveryTerminalReceiptV1() {
        if (certificate_fd_ >= 0) {
            static_cast<void>(::close(certificate_fd_));
        }
        if (maintenance_directory_fd_ >= 0) {
            static_cast<void>(
                ::close(maintenance_directory_fd_));
        }
    }

    SealedRawRecoveryTerminalReceiptV1(
        const SealedRawRecoveryTerminalReceiptV1&) =
        delete;
    SealedRawRecoveryTerminalReceiptV1& operator=(
        const SealedRawRecoveryTerminalReceiptV1&) =
        delete;
    SealedRawRecoveryTerminalReceiptV1(
        SealedRawRecoveryTerminalReceiptV1&&) = delete;
    SealedRawRecoveryTerminalReceiptV1& operator=(
        SealedRawRecoveryTerminalReceiptV1&&) = delete;

private:
    friend SealedRawCertificatePublishResultV1
    PublishSealedRawCertificateV1(
        const RawWriterLease&,
        std::unique_ptr<
            RawReserveAuthorizedActionV1>,
        const BuiltSealedRawCertificateV1&,
        std::string*) noexcept;
    friend class RecoveryMaintenanceReportStoreAccessV1;
    friend class RawReserveRegistryCoordinatorV1;

    SealedRawRecoveryTerminalReceiptV1(
        RawReserveRegistryEntryKeyV1 key,
        RawReserveGenerationActionTokenV1 token,
        RawReserveMutationTargetAnchorV1 target,
        RawV1Identity writer_instance,
        RawV1Digest certificate_sha256,
        std::string filename,
        std::uint64_t maintenance_device,
        std::uint64_t maintenance_inode,
        std::uint64_t certificate_device,
        std::uint64_t certificate_inode,
        int maintenance_directory_fd,
        int certificate_fd) noexcept
        : key_(std::move(key)),
          token_(std::move(token)),
          target_(std::move(target)),
          writer_instance_(writer_instance),
          certificate_sha256_(certificate_sha256),
          filename_(std::move(filename)),
          maintenance_device_(maintenance_device),
          maintenance_inode_(maintenance_inode),
          certificate_device_(certificate_device),
          certificate_inode_(certificate_inode),
          maintenance_directory_fd_(
              maintenance_directory_fd),
          certificate_fd_(certificate_fd) {}

    RawReserveRegistryEntryKeyV1 key_{};
    RawReserveGenerationActionTokenV1 token_{};
    RawReserveMutationTargetAnchorV1 target_;
    RawV1Identity writer_instance_{};
    RawV1Digest certificate_sha256_{};
    std::string filename_;
    std::uint64_t maintenance_device_ = 0U;
    std::uint64_t maintenance_inode_ = 0U;
    std::uint64_t certificate_device_ = 0U;
    std::uint64_t certificate_inode_ = 0U;
    int maintenance_directory_fd_ = -1;
    int certificate_fd_ = -1;
    bool consumed_ = false;
};

}  // namespace l2flow::ingress
