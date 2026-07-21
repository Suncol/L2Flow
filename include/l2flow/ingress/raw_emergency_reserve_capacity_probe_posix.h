#pragma once

#include "l2flow/ingress/raw_emergency_reserve_posix.h"

#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace l2flow::ingress {

// The order is part of the V1 quota-vector commitment.  A domain may contain
// at most one entry of each kind and entries must be strictly increasing.
enum class RawEmergencyReserveQuotaKindV1 : std::uint8_t {
    kUser = 1U,
    kGroup = 2U,
    kProject = 3U,
};

struct RawEmergencyReserveQuotaSubjectV1 final {
    RawEmergencyReserveQuotaKindV1 kind =
        RawEmergencyReserveQuotaKindV1::kUser;
    std::uint32_t id = 0U;

    friend bool operator==(
        const RawEmergencyReserveQuotaSubjectV1&,
        const RawEmergencyReserveQuotaSubjectV1&) = default;
};

inline constexpr std::string_view
    kRawEmergencyReserveQuotaIdentityDomainV1 =
        "L2FLOW_RAW_QUOTA_VECTOR_V1";

// Hashes:
//   domain bytes || 0x00 || vector_count:u8 ||
//   repeated(kind:u8 || id:u32-le)
// The input must contain one to three strictly kind-sorted subjects.
[[nodiscard]] bool ComputeRawEmergencyReserveQuotaIdentitySha256V1(
    std::span<const RawEmergencyReserveQuotaSubjectV1> subjects,
    ReserveHeaderDigestV1* digest,
    std::string* error = nullptr) noexcept;

enum class RawEmergencyReserveFilesystemModelV1 : std::uint8_t {
    kExt4 = 1U,
    kXfs = 2U,
};

struct RawEmergencyReserveFilesystemObservationV1 final {
    std::uint64_t filesystem_id = 0U;
    std::uint64_t filesystem_type = 0U;
    std::uint64_t fragment_size = 0U;
    std::uint64_t blocks_available = 0U;
    std::uint64_t blocks_free = 0U;
    std::uint64_t inodes_available = 0U;
    std::uint64_t inodes_free = 0U;
    bool read_only = true;
};

struct RawEmergencyReserveQuotaObservationV1 final {
    // Linux generic quota byte limits are normalized to bytes by the system
    // backend; XFS basic-block limits are likewise normalized to bytes.
    std::uint64_t byte_hard_limit = 0U;
    std::uint64_t byte_soft_limit = 0U;
    std::uint64_t bytes_used = 0U;
    std::uint64_t inode_hard_limit = 0U;
    std::uint64_t inode_soft_limit = 0U;
    std::uint64_t inodes_used = 0U;
    bool byte_limits_valid = false;
    bool byte_usage_valid = false;
    bool inode_limits_valid = false;
    bool inode_usage_valid = false;
    bool accounting_enabled = false;
    bool enforcement_enabled = false;
};

// Injectable only at the syscall boundary.  The default implementation uses
// fstatvfs/fstatfs, quotactl_fd and FS_IOC_FSGETXATTR, all against an already
// opened descriptor.  XFS enforcement is proven with Q_XGETQSTATV.  Linux's
// generic VFS Q_GETQUOTA does not expose the ext4 enforcement-on bit, so the
// default ext4 backend deliberately reports enforcement as unproven and the
// bind fails closed.  An ext4-specific injected implementation may return an
// enforcement proof only when a separate privileged preflight/drill has
// established it.  Tests can inject deterministic observations without
// weakening identity checks or reserve-file validation.
class RawEmergencyReserveCapacityProbePosixOpsV1 {
public:
    virtual ~RawEmergencyReserveCapacityProbePosixOpsV1() = default;

    [[nodiscard]] virtual bool ObserveFilesystem(
        int fd,
        RawEmergencyReserveFilesystemObservationV1* observation,
        int* system_error) noexcept = 0;

    [[nodiscard]] virtual bool ObserveQuota(
        int fd,
        RawEmergencyReserveFilesystemModelV1 filesystem_model,
        const RawEmergencyReserveQuotaSubjectV1& subject,
        RawEmergencyReserveQuotaObservationV1* observation,
        int* system_error) noexcept = 0;

    [[nodiscard]] virtual bool ObserveProjectId(
        int fd,
        std::uint32_t* project_id,
        bool* project_inherit,
        int* system_error) noexcept = 0;
};

struct RawEmergencyReserveCapacityProbePosixConfigV1 final {
    RawEmergencyReserveFilesystemModelV1 filesystem_model =
        RawEmergencyReserveFilesystemModelV1::kExt4;
    ReserveHeaderDigestV1 mount_identity_sha256{};
    std::vector<RawEmergencyReserveQuotaSubjectV1> quota_subjects{};
    std::uint16_t byte_probe_version = 0U;
    std::uint16_t inode_probe_version = 0U;
};

class RawEmergencyReserveCapacityProbePosixV1 final
    : public RawEmergencyReserveCapacityProbeV1 {
public:
    ~RawEmergencyReserveCapacityProbePosixV1() override;

    RawEmergencyReserveCapacityProbePosixV1(
        const RawEmergencyReserveCapacityProbePosixV1&) = delete;
    RawEmergencyReserveCapacityProbePosixV1& operator=(
        const RawEmergencyReserveCapacityProbePosixV1&) = delete;

    [[nodiscard]] bool Observe(
        int retained_raw_root_fd,
        const RawEmergencyReserveCapacityProbeRequestV1& request,
        RawEmergencyReserveCapacityObservationV1* observation,
        std::string* error) noexcept override;

private:
    friend std::unique_ptr<
        RawEmergencyReserveCapacityProbePosixV1>
    BindRawEmergencyReserveCapacityProbePosixV1At(
        int,
        const RawEmergencyReserveCapacityProbePosixConfigV1&,
        RawEmergencyReserveCapacityProbePosixOpsV1*,
        std::string*) noexcept;

    RawEmergencyReserveCapacityProbePosixV1(
        RawEmergencyReserveCapacityProbePosixConfigV1 config,
        RawEmergencyReserveCapacityProbePosixOpsV1* operations,
        std::uint64_t root_device,
        std::uint64_t root_inode,
        std::uint32_t root_uid,
        std::uint32_t root_gid,
        std::uint64_t filesystem_id,
        std::uint64_t filesystem_type,
        ReserveHeaderDigestV1 quota_identity_sha256) noexcept;

    RawEmergencyReserveCapacityProbePosixConfigV1 config_{};
    RawEmergencyReserveCapacityProbePosixOpsV1* operations_ = nullptr;
    std::uint64_t root_device_ = 0U;
    std::uint64_t root_inode_ = 0U;
    std::uint32_t root_uid_ = 0U;
    std::uint32_t root_gid_ = 0U;
    std::uint64_t filesystem_id_ = 0U;
    std::uint64_t filesystem_type_ = 0U;
    ReserveHeaderDigestV1 quota_identity_sha256_{};
};

// Captures the exact retained directory's device/inode/filesystem identity,
// validates owner-only Raw-root metadata and the configured quota vector, and
// returns a probe permanently bound to those facts.  `operations == nullptr`
// selects the real Linux backend.  The returned probe does not own or reopen
// a pathname; every Observe call must supply a descriptor for the same inode.
[[nodiscard]] std::unique_ptr<
    RawEmergencyReserveCapacityProbePosixV1>
BindRawEmergencyReserveCapacityProbePosixV1At(
    int retained_raw_root_fd,
    const RawEmergencyReserveCapacityProbePosixConfigV1& config,
    RawEmergencyReserveCapacityProbePosixOpsV1* operations = nullptr,
    std::string* error = nullptr) noexcept;

}  // namespace l2flow::ingress
