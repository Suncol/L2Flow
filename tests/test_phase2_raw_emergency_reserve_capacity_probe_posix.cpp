#include "l2flow/common/identity128.h"
#include "l2flow/ingress/raw_emergency_reserve_capacity_probe_posix.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace {

namespace common = l2flow::common;
namespace ingress = l2flow::ingress;

class TestContext final {
public:
    void Expect(bool condition, std::string_view description) {
        if (!condition) {
            ++failures_;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    [[nodiscard]] int failures() const noexcept {
        return failures_;
    }

private:
    int failures_ = 0;
};

class TempDirectory final {
public:
    TempDirectory() {
        std::array<char, 64U> pattern{};
        const char prefix[] =
            "/tmp/l2flow-capacity-probe-XXXXXX";
        std::copy(
            std::begin(prefix),
            std::end(prefix),
            pattern.begin());
        char* created = ::mkdtemp(pattern.data());
        if (created != nullptr) {
            path_ = created;
            fd_ = ::open(
                created,
                O_RDONLY | O_DIRECTORY | O_CLOEXEC |
                    O_NOFOLLOW);
        }
    }

    ~TempDirectory() {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
        if (!path_.empty()) {
            std::error_code ignored;
            static_cast<void>(
                std::filesystem::remove_all(path_, ignored));
        }
    }

    TempDirectory(const TempDirectory&) = delete;
    TempDirectory& operator=(const TempDirectory&) = delete;

    [[nodiscard]] int fd() const noexcept {
        return fd_;
    }

    [[nodiscard]] const std::string& path() const noexcept {
        return path_;
    }

private:
    std::string path_{};
    int fd_ = -1;
};

template <std::size_t Size>
std::array<std::byte, Size> Pattern(std::uint8_t first) {
    std::array<std::byte, Size> output{};
    for (std::size_t index = 0U; index < Size; ++index) {
        output[index] = static_cast<std::byte>(
            static_cast<unsigned int>(first) +
            static_cast<unsigned int>(index));
    }
    return output;
}

class FakeOps final
    : public ingress::
          RawEmergencyReserveCapacityProbePosixOpsV1 {
public:
    bool ObserveFilesystem(
        int,
        ingress::RawEmergencyReserveFilesystemObservationV1*
            observation,
        int* system_error) noexcept override {
        if (observation == nullptr || system_error == nullptr ||
            fail_filesystem) {
            if (system_error != nullptr) {
                *system_error = EIO;
            }
            return false;
        }
        *system_error = 0;
        *observation = filesystem;
        return true;
    }

    bool ObserveQuota(
        int,
        ingress::RawEmergencyReserveFilesystemModelV1,
        const ingress::RawEmergencyReserveQuotaSubjectV1&
            subject,
        ingress::RawEmergencyReserveQuotaObservationV1*
            observation,
        int* system_error) noexcept override {
        last_subject = subject;
        if (observation == nullptr || system_error == nullptr ||
            fail_quota) {
            if (system_error != nullptr) {
                *system_error = ESRCH;
            }
            return false;
        }
        *system_error = 0;
        *observation =
            group_override &&
                    subject.kind ==
                        ingress::
                            RawEmergencyReserveQuotaKindV1::
                                kGroup
                ? group_quota
                : quota;
        return true;
    }

    bool ObserveProjectId(
        int,
        std::uint32_t* observed_project_id,
        bool* project_inherit,
        int* system_error) noexcept override {
        if (observed_project_id == nullptr ||
            project_inherit == nullptr ||
            system_error == nullptr ||
            fail_project) {
            if (system_error != nullptr) {
                *system_error = EOPNOTSUPP;
            }
            return false;
        }
        *system_error = 0;
        *observed_project_id = project_id;
        *project_inherit = inherit;
        return true;
    }

    ingress::RawEmergencyReserveFilesystemObservationV1
        filesystem{
            .filesystem_id = 0x12345678U,
            .filesystem_type = 0xef53U,
            .fragment_size = 4096U,
            .blocks_available = 4096U,
            .blocks_free = 4096U,
            .inodes_available = 1024U,
            .inodes_free = 1024U,
            .read_only = false};
    ingress::RawEmergencyReserveQuotaObservationV1 quota{
        .byte_hard_limit = 1ULL << 34U,
        .byte_soft_limit = 1ULL << 33U,
        .bytes_used = 4096U,
        .inode_hard_limit = 1ULL << 24U,
        .inode_soft_limit = 1ULL << 23U,
        .inodes_used = 8U,
        .byte_limits_valid = true,
        .byte_usage_valid = true,
        .inode_limits_valid = true,
        .inode_usage_valid = true,
        .accounting_enabled = true,
        .enforcement_enabled = true};
    ingress::RawEmergencyReserveQuotaObservationV1
        group_quota = quota;
    ingress::RawEmergencyReserveQuotaSubjectV1 last_subject{};
    std::uint32_t project_id = 0U;
    bool inherit = true;
    bool fail_filesystem = false;
    bool fail_quota = false;
    bool fail_project = false;
    bool group_override = false;
};

[[nodiscard]] bool WriteAll(
    int fd,
    std::span<const std::byte> bytes) noexcept {
    std::size_t offset = 0U;
    while (offset < bytes.size()) {
        const ssize_t written = ::pwrite(
            fd,
            bytes.data() + offset,
            bytes.size() - offset,
            static_cast<off_t>(offset));
        if (written > 0) {
            offset += static_cast<std::size_t>(written);
            continue;
        }
        if (written < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

[[nodiscard]] std::uint64_t AllocatedBytes(
    int fd) noexcept {
    struct stat status {};
    if (::fstat(fd, &status) != 0 ||
        status.st_blocks < 0) {
        return 0U;
    }
    return static_cast<std::uint64_t>(
               status.st_blocks) *
           512U;
}

struct BoundFixture final {
    TempDirectory temporary{};
    FakeOps operations{};
    ingress::RawEmergencyReserveCapacityProbePosixConfigV1
        config{};
    ingress::ReserveHeaderDigestV1 quota_identity{};
    std::unique_ptr<
        ingress::RawEmergencyReserveCapacityProbePosixV1>
        probe{};

    BoundFixture() {
        config.filesystem_model =
            ingress::RawEmergencyReserveFilesystemModelV1::
                kExt4;
        config.mount_identity_sha256 = Pattern<32U>(0x40U);
        config.quota_subjects.push_back(
            ingress::RawEmergencyReserveQuotaSubjectV1{
                ingress::RawEmergencyReserveQuotaKindV1::
                    kUser,
                static_cast<std::uint32_t>(::geteuid())});
        config.byte_probe_version = 1U;
        config.inode_probe_version = 1U;
        std::string ignored;
        static_cast<void>(
            ingress::
                ComputeRawEmergencyReserveQuotaIdentitySha256V1(
                    config.quota_subjects,
                    &quota_identity,
                    &ignored));
        probe =
            ingress::
                BindRawEmergencyReserveCapacityProbePosixV1At(
                    temporary.fd(),
                    config,
                    &operations,
                    &ignored);
    }

    [[nodiscard]]
    ingress::RawEmergencyReserveCapacityProbeRequestV1
    Request(
        ingress::RawEmergencyReserveProbeStageV1 stage,
        std::uint64_t bytes,
        std::uint64_t inodes) const {
        struct stat status {};
        static_cast<void>(
            ::fstat(temporary.fd(), &status));
        ingress::RawEmergencyReserveCapacityProbeRequestV1
            request{};
        request.stage = stage;
        request.pool.reserve_state_uuid =
            Pattern<16U>(0x10U);
        request.pool.device_id =
            static_cast<std::uint64_t>(status.st_dev);
        request.pool.quota_identity_sha256 =
            quota_identity;
        request.pool.mount_identity_sha256 =
            config.mount_identity_sha256;
        request.byte_probe_version =
            config.byte_probe_version;
        request.inode_probe_version =
            config.inode_probe_version;
        request.required_bytes = bytes;
        request.required_inodes = inodes;
        return request;
    }
};

void CheckQuotaIdentity(TestContext* test) {
    const std::array<
        ingress::RawEmergencyReserveQuotaSubjectV1,
        3U>
        subjects{{
            {ingress::RawEmergencyReserveQuotaKindV1::kUser,
             12U},
            {ingress::RawEmergencyReserveQuotaKindV1::kGroup,
             34U},
            {ingress::RawEmergencyReserveQuotaKindV1::kProject,
             56U},
        }};
    ingress::ReserveHeaderDigestV1 first{};
    ingress::ReserveHeaderDigestV1 second{};
    std::string error;
    test->Expect(
        ingress::
            ComputeRawEmergencyReserveQuotaIdentitySha256V1(
                subjects, &first, &error) &&
            ingress::
                ComputeRawEmergencyReserveQuotaIdentitySha256V1(
                    subjects, &second, &error) &&
            first == second,
        "quota vector commitment is deterministic");
    test->Expect(
        common::Sha256Hex(first) ==
            "93f07c4455635476d0d843fb9e20eba5"
            "77f6d1411d722b43a8fc8aa803e754af",
        "quota vector canonical bytes match independent SHA-256 golden");
    auto changed = subjects;
    changed[2U].id += 1U;
    ingress::ReserveHeaderDigestV1 changed_digest{};
    const bool changed_hashed = ingress::
            ComputeRawEmergencyReserveQuotaIdentitySha256V1(
                changed, &changed_digest, &error);
    if (!changed_hashed || changed_digest == first) {
        std::cerr << "quota hash diagnostic: " << error << '\n';
    }
    test->Expect(
        changed_hashed && changed_digest != first,
        "quota id changes the domain commitment");
    auto unsorted = subjects;
    std::swap(unsorted[0U], unsorted[1U]);
    test->Expect(
        !ingress::
            ComputeRawEmergencyReserveQuotaIdentitySha256V1(
                unsorted, &changed_digest, &error),
        "unsorted quota vector is rejected");
    const std::array<
        ingress::RawEmergencyReserveQuotaSubjectV1,
        0U>
        empty{};
    test->Expect(
        !ingress::
            ComputeRawEmergencyReserveQuotaIdentitySha256V1(
                empty, &changed_digest, &error),
        "empty quota vector cannot claim quota proof");
}

void CheckCapacityAndIdentityGates(TestContext* test) {
    BoundFixture fixture;
    test->Expect(
        fixture.temporary.fd() >= 0 &&
            fixture.probe != nullptr,
        "fd-bound capacity probe initializes");
    if (fixture.probe == nullptr) {
        return;
    }
    auto request = fixture.Request(
        ingress::RawEmergencyReserveProbeStageV1::
            kBeforeProvision,
        8192U,
        8U);
    ingress::RawEmergencyReserveCapacityObservationV1
        observation{};
    std::string error;
    test->Expect(
        fixture.probe->Observe(
            fixture.temporary.fd(),
            request,
            &observation,
            &error),
        "filesystem and quota bytes/inodes pass together");
    test->Expect(
        observation.filesystem_bytes_proven &&
            observation.quota_bytes_proven &&
            observation.filesystem_inodes_proven &&
            observation.quota_inodes_proven &&
            observation.pool == request.pool,
        "successful observation binds all four dimensions and exact pool");

    auto wrong_device = request;
    wrong_device.pool.device_id += 1U;
    test->Expect(
        !fixture.probe->Observe(
            fixture.temporary.fd(),
            wrong_device,
            &observation,
            &error),
        "request device tamper is rejected");
    test->Expect(
        !observation.filesystem_bytes_proven &&
            !observation.quota_bytes_proven &&
            !observation.reserve_byte_charge_proven,
        "failed observation clears prior proof bits");
    auto wrong_quota = request;
    wrong_quota.pool.quota_identity_sha256[0U] ^=
        std::byte{1};
    test->Expect(
        !fixture.probe->Observe(
            fixture.temporary.fd(),
            wrong_quota,
            &observation,
            &error),
        "request quota-domain tamper is rejected");

    fixture.operations.filesystem.blocks_available = 1U;
    fixture.operations.filesystem.blocks_free = 1U;
    test->Expect(
        !fixture.probe->Observe(
            fixture.temporary.fd(),
            request,
            &observation,
            &error),
        "insufficient filesystem bytes fail closed");
    fixture.operations.filesystem.blocks_available = 4096U;
    fixture.operations.filesystem.blocks_free = 4096U;
    fixture.operations.quota.byte_hard_limit = 4096U;
    fixture.operations.quota.byte_soft_limit = 0U;
    fixture.operations.quota.bytes_used = 0U;
    test->Expect(
        !fixture.probe->Observe(
            fixture.temporary.fd(),
            request,
            &observation,
            &error),
        "insufficient quota bytes fail closed");
    fixture.operations.quota.byte_hard_limit = 1ULL << 34U;
    fixture.operations.filesystem.inodes_available = 1U;
    fixture.operations.filesystem.inodes_free = 1U;
    test->Expect(
        !fixture.probe->Observe(
            fixture.temporary.fd(),
            request,
            &observation,
            &error),
        "insufficient filesystem inodes fail closed");
    fixture.operations.filesystem.inodes_available = 1024U;
    fixture.operations.filesystem.inodes_free = 1024U;
    fixture.operations.quota.inode_hard_limit = 4U;
    fixture.operations.quota.inode_soft_limit = 0U;
    fixture.operations.quota.inodes_used = 0U;
    test->Expect(
        !fixture.probe->Observe(
            fixture.temporary.fd(),
            request,
            &observation,
            &error),
        "insufficient quota inodes fail closed");
    fixture.operations.quota.inode_hard_limit = 1ULL << 24U;
    fixture.operations.quota.byte_usage_valid = false;
    test->Expect(
        !fixture.probe->Observe(
            fixture.temporary.fd(),
            request,
            &observation,
            &error),
        "incomplete quota accounting proof is rejected");
    fixture.operations.quota.byte_usage_valid = true;
    fixture.operations.fail_quota = true;
    test->Expect(
        !fixture.probe->Observe(
            fixture.temporary.fd(),
            request,
            &observation,
            &error),
        "quota syscall failure is fail closed");
    fixture.operations.fail_quota = false;
    fixture.operations.filesystem.inodes_available =
        std::numeric_limits<std::uint64_t>::max();
    test->Expect(
        !fixture.probe->Observe(
            fixture.temporary.fd(),
            request,
            &observation,
            &error),
        "unknown filesystem inode capacity is fail closed");
    fixture.operations.filesystem.inodes_available = 1024U;
    fixture.operations.filesystem.fragment_size =
        std::numeric_limits<std::uint64_t>::max();
    fixture.operations.filesystem.blocks_available = 2U;
    fixture.operations.filesystem.blocks_free = 2U;
    test->Expect(
        !fixture.probe->Observe(
            fixture.temporary.fd(),
            request,
            &observation,
            &error),
        "filesystem byte multiplication overflow is rejected");
}

void CheckRootAndBindingTamper(TestContext* test) {
    BoundFixture fixture;
    TempDirectory other;
    test->Expect(
        fixture.probe != nullptr && other.fd() >= 0,
        "root tamper fixture initializes");
    if (fixture.probe == nullptr || other.fd() < 0) {
        return;
    }
    auto request = fixture.Request(
        ingress::RawEmergencyReserveProbeStageV1::
            kBeforeProvision,
        1U,
        1U);
    struct stat other_status {};
    static_cast<void>(::fstat(other.fd(), &other_status));
    request.pool.device_id =
        static_cast<std::uint64_t>(other_status.st_dev);
    ingress::RawEmergencyReserveCapacityObservationV1
        observation{};
    std::string error;
    test->Expect(
        !fixture.probe->Observe(
            other.fd(), request, &observation, &error),
        "different directory inode on the same device is rejected");

    auto original_request = fixture.Request(
        ingress::RawEmergencyReserveProbeStageV1::
            kBeforeProvision,
        1U,
        1U);
    fixture.operations.filesystem.filesystem_id += 1U;
    test->Expect(
        !fixture.probe->Observe(
            fixture.temporary.fd(),
            original_request,
            &observation,
            &error),
        "filesystem identity tamper is rejected");

    BoundFixture failure;
    failure.operations.fail_filesystem = true;
    test->Expect(
        failure.probe != nullptr,
        "failure probe was bound before injected failure");
    if (failure.probe != nullptr) {
        auto failure_request = failure.Request(
            ingress::RawEmergencyReserveProbeStageV1::
                kBeforeProvision,
            1U,
            1U);
        test->Expect(
            !failure.probe->Observe(
                failure.temporary.fd(),
                failure_request,
                &observation,
                &error),
            "filesystem syscall failure is fail closed");
    }
}

[[nodiscard]] bool InstallReserveArtifacts(
    const BoundFixture& fixture,
    const ingress::RawEmergencyReserveCapacityProbeRequestV1&
        request,
    int* data_fd,
    int* inode_fd,
    std::string* inode_name) {
    if (data_fd == nullptr || inode_fd == nullptr ||
        inode_name == nullptr ||
        ::mkdirat(
            fixture.temporary.fd(),
            ingress::kRawEmergencyReserveInodesDirectory,
            0700U) != 0) {
        return false;
    }
    *data_fd = ::openat(
        fixture.temporary.fd(),
        ingress::kRawEmergencyReserveDataFilename,
        O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC |
            O_NOFOLLOW,
        0600U);
    if (*data_fd < 0) {
        return false;
    }
    ingress::ReserveFileHeaderV1 data_header{};
    data_header.pool = request.pool;
    data_header.declared_bytes =
        ingress::kReserveHeaderV1Bytes;
    ingress::ReserveHeaderWireV1 data_wire{};
    if (ingress::EncodeReserveFileHeaderV1(
            data_header, &data_wire) !=
            ingress::ReserveHeaderV1Error::kNone ||
        !WriteAll(*data_fd, data_wire)) {
        return false;
    }

    int directory_fd = ::openat(
        fixture.temporary.fd(),
        ingress::kRawEmergencyReserveInodesDirectory,
        O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (directory_fd < 0 ||
        ingress::FormatReserveInodeFilenameV1(
            request.pool.reserve_state_uuid,
            0U,
            inode_name) !=
            ingress::ReserveHeaderV1Error::kNone) {
        if (directory_fd >= 0) {
            static_cast<void>(::close(directory_fd));
        }
        return false;
    }
    *inode_fd = ::openat(
        directory_fd,
        inode_name->c_str(),
        O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC |
            O_NOFOLLOW,
        0600U);
    static_cast<void>(::close(directory_fd));
    if (*inode_fd < 0) {
        return false;
    }
    ingress::ReserveInodeHeaderV1 inode_header{};
    inode_header.pool = request.pool;
    inode_header.inode_index = 0U;
    inode_header.declared_inode_reserve_count = 1U;
    ingress::ReserveHeaderWireV1 inode_wire{};
    return ingress::EncodeReserveInodeHeaderV1(
               inode_header, &inode_wire) ==
               ingress::ReserveHeaderV1Error::kNone &&
           WriteAll(*inode_fd, inode_wire);
}

void CheckReserveChargeProof(TestContext* test) {
    BoundFixture fixture;
    test->Expect(
        fixture.probe != nullptr,
        "reserve charge fixture binds");
    if (fixture.probe == nullptr) {
        return;
    }
    auto request = fixture.Request(
        ingress::RawEmergencyReserveProbeStageV1::kAttached,
        0U,
        2U);
    int data_fd = -1;
    int inode_fd = -1;
    std::string inode_name;
    const bool installed = InstallReserveArtifacts(
        fixture,
        request,
        &data_fd,
        &inode_fd,
        &inode_name);
    test->Expect(
        installed,
        "fd-relative reserve artifacts install");
    if (!installed) {
        if (data_fd >= 0) {
            static_cast<void>(::close(data_fd));
        }
        if (inode_fd >= 0) {
            static_cast<void>(::close(inode_fd));
        }
        return;
    }
    request.required_bytes =
        AllocatedBytes(data_fd) +
        AllocatedBytes(inode_fd);
    ingress::RawEmergencyReserveCapacityObservationV1
        observation{};
    std::string error;
    const bool charge_observed =
        request.required_bytes >=
            2U * ingress::kReserveHeaderV1Bytes &&
        fixture.probe->Observe(
                fixture.temporary.fd(),
                request,
                &observation,
                &error);
    if (!charge_observed) {
        std::cerr << "charge probe diagnostic: " << error << '\n';
    }
    test->Expect(
        charge_observed,
        "attached reserve charge is derived from validated allocated inodes");
    test->Expect(
        observation.reserve_byte_charge_proven &&
            observation.reserve_inode_charge_proven &&
            observation.proven_reserve_byte_charge ==
                request.required_bytes &&
            observation.proven_reserve_inode_charge == 2U,
        "charge observation reports actual st_blocks and inode count");

    auto underdeclared = request;
    underdeclared.required_bytes -= 1U;
    test->Expect(
        !fixture.probe->Observe(
            fixture.temporary.fd(),
            underdeclared,
            &observation,
            &error),
        "actual reserve overcharge is not accepted as a lower requested budget");
    auto overdeclared = request;
    overdeclared.required_bytes += 1U;
    test->Expect(
        !fixture.probe->Observe(
            fixture.temporary.fd(),
            overdeclared,
            &observation,
            &error),
        "requested reserve bytes without actual allocation are rejected");
    auto wrong_inodes = request;
    wrong_inodes.required_inodes += 1U;
    test->Expect(
        !fixture.probe->Observe(
            fixture.temporary.fd(),
            wrong_inodes,
            &observation,
            &error),
        "reserve inode charge must match exactly");

    const std::array<std::byte, 1U> corrupt{
        std::byte{0xff}};
    const ssize_t corrupted = ::pwrite(
        data_fd, corrupt.data(), corrupt.size(), 0);
    test->Expect(
        corrupted == 1,
        "reserve header tamper is written");
    test->Expect(
        !fixture.probe->Observe(
            fixture.temporary.fd(),
            request,
            &observation,
            &error),
        "reserve header tamper invalidates charge proof");
    static_cast<void>(::close(data_fd));
    static_cast<void>(::close(inode_fd));
}

void CheckReleasedAndUnknownInventory(TestContext* test) {
    BoundFixture released;
    test->Expect(
        released.probe != nullptr &&
            ::mkdirat(
                released.temporary.fd(),
                ingress::kRawEmergencyReserveInodesDirectory,
                0700U) == 0,
        "released reserve retains empty inode directory");
    if (released.probe != nullptr) {
        auto request = released.Request(
            ingress::RawEmergencyReserveProbeStageV1::
                kReleased,
            0U,
            0U);
        ingress::RawEmergencyReserveCapacityObservationV1
            observation{};
        std::string error;
        const bool released_observed =
            released.probe->Observe(
                released.temporary.fd(),
                request,
                &observation,
                &error);
        if (!released_observed) {
            std::cerr << "released probe diagnostic: "
                      << error << '\n';
        }
        test->Expect(
            released_observed &&
                observation.reserve_byte_charge_proven &&
                observation.reserve_inode_charge_proven &&
                observation.proven_reserve_byte_charge == 0U &&
                observation.proven_reserve_inode_charge == 0U,
            "released stage proves exact zero reserve charge");

        const int directory_fd = ::openat(
            released.temporary.fd(),
            ingress::kRawEmergencyReserveInodesDirectory,
            O_RDONLY | O_DIRECTORY | O_CLOEXEC |
                O_NOFOLLOW);
        const int unknown =
            directory_fd < 0
                ? -1
                : ::openat(
                      directory_fd,
                      "unknown.reserve",
                      O_WRONLY | O_CREAT | O_EXCL |
                          O_CLOEXEC | O_NOFOLLOW,
                      0600U);
        if (unknown >= 0) {
            static_cast<void>(::close(unknown));
        }
        if (directory_fd >= 0) {
            static_cast<void>(::close(directory_fd));
        }
        test->Expect(
            !released.probe->Observe(
                released.temporary.fd(),
                request,
                &observation,
                &error),
            "unknown inode inventory artifact fails closed");
    }

    BoundFixture symlink_attack;
    test->Expect(
        symlink_attack.probe != nullptr &&
            ::mkdirat(
                symlink_attack.temporary.fd(),
                ingress::kRawEmergencyReserveInodesDirectory,
                0700U) == 0 &&
            ::symlinkat(
                "/dev/null",
                symlink_attack.temporary.fd(),
                ingress::kRawEmergencyReserveDataFilename) == 0,
        "symlink attack fixture installs");
    if (symlink_attack.probe != nullptr) {
        auto request = symlink_attack.Request(
            ingress::RawEmergencyReserveProbeStageV1::
                kReleased,
            0U,
            0U);
        ingress::RawEmergencyReserveCapacityObservationV1
            observation{};
        std::string error;
        test->Expect(
            !symlink_attack.probe->Observe(
                symlink_attack.temporary.fd(),
                request,
                &observation,
                &error),
            "reserve data symlink is not mistaken for durable absence");
    }
}

void CheckInvalidBinding(TestContext* test) {
    TempDirectory temporary;
    FakeOps operations;
    ingress::RawEmergencyReserveCapacityProbePosixConfigV1
        config{};
    config.filesystem_model =
        ingress::RawEmergencyReserveFilesystemModelV1::kExt4;
    config.mount_identity_sha256 = Pattern<32U>(0x60U);
    config.quota_subjects.push_back(
        {ingress::RawEmergencyReserveQuotaKindV1::kUser,
         static_cast<std::uint32_t>(::geteuid()) + 1U});
    config.byte_probe_version = 1U;
    config.inode_probe_version = 1U;
    std::string error;
    test->Expect(
        ingress::
            BindRawEmergencyReserveCapacityProbePosixV1At(
                temporary.fd(),
                config,
                &operations,
                &error) == nullptr,
        "quota subject that does not own the Raw root is rejected");

    config.quota_subjects[0U].id =
        static_cast<std::uint32_t>(::geteuid());
    operations.quota.enforcement_enabled = false;
    test->Expect(
        ingress::
            BindRawEmergencyReserveCapacityProbePosixV1At(
                temporary.fd(),
                config,
                &operations,
                &error) == nullptr,
        "accounting without quota enforcement cannot bind");
}

void CheckProjectQuotaBinding(TestContext* test) {
    TempDirectory temporary;
    FakeOps operations;
    operations.project_id = 77U;
    ingress::RawEmergencyReserveCapacityProbePosixConfigV1
        config{};
    config.filesystem_model =
        ingress::RawEmergencyReserveFilesystemModelV1::kExt4;
    config.mount_identity_sha256 = Pattern<32U>(0x70U);
    config.quota_subjects.push_back(
        {ingress::RawEmergencyReserveQuotaKindV1::kProject,
         77U});
    config.byte_probe_version = 1U;
    config.inode_probe_version = 1U;
    std::string error;
    auto probe =
        ingress::BindRawEmergencyReserveCapacityProbePosixV1At(
            temporary.fd(), config, &operations, &error);
    test->Expect(
        probe != nullptr,
        "matching project id with inheritance binds");
    if (probe == nullptr) {
        return;
    }
    ingress::ReserveHeaderDigestV1 quota_identity{};
    static_cast<void>(
        ingress::
            ComputeRawEmergencyReserveQuotaIdentitySha256V1(
                config.quota_subjects,
                &quota_identity,
                &error));
    struct stat status {};
    static_cast<void>(::fstat(temporary.fd(), &status));
    ingress::RawEmergencyReserveCapacityProbeRequestV1 request{};
    request.stage =
        ingress::RawEmergencyReserveProbeStageV1::
            kBeforeProvision;
    request.pool.reserve_state_uuid = Pattern<16U>(0x30U);
    request.pool.device_id =
        static_cast<std::uint64_t>(status.st_dev);
    request.pool.quota_identity_sha256 = quota_identity;
    request.pool.mount_identity_sha256 =
        config.mount_identity_sha256;
    request.byte_probe_version = 1U;
    request.inode_probe_version = 1U;
    request.required_bytes = 1U;
    request.required_inodes = 1U;
    ingress::RawEmergencyReserveCapacityObservationV1
        observation{};
    operations.project_id = 78U;
    test->Expect(
        !probe->Observe(
            temporary.fd(),
            request,
            &observation,
            &error),
        "project-id replacement is rejected after binding");
    operations.project_id = 77U;
    operations.inherit = false;
    test->Expect(
        !probe->Observe(
            temporary.fd(),
            request,
            &observation,
            &error),
        "lost project inheritance is rejected");
}

void CheckQuotaVectorMinimum(TestContext* test) {
    TempDirectory temporary;
    FakeOps operations;
    operations.group_override = true;
    operations.group_quota.byte_hard_limit = 4096U;
    operations.group_quota.byte_soft_limit = 0U;
    operations.group_quota.bytes_used = 0U;
    operations.group_quota.inode_hard_limit = 4U;
    operations.group_quota.inode_soft_limit = 0U;
    operations.group_quota.inodes_used = 0U;
    ingress::RawEmergencyReserveCapacityProbePosixConfigV1
        config{};
    config.filesystem_model =
        ingress::RawEmergencyReserveFilesystemModelV1::kExt4;
    config.mount_identity_sha256 = Pattern<32U>(0x78U);
    config.quota_subjects = {
        {ingress::RawEmergencyReserveQuotaKindV1::kUser,
         static_cast<std::uint32_t>(::geteuid())},
        {ingress::RawEmergencyReserveQuotaKindV1::kGroup,
         static_cast<std::uint32_t>(::getegid())}};
    config.byte_probe_version = 1U;
    config.inode_probe_version = 1U;
    std::string error;
    auto probe =
        ingress::BindRawEmergencyReserveCapacityProbePosixV1At(
            temporary.fd(), config, &operations, &error);
    test->Expect(
        probe != nullptr,
        "overlapping user/group quota vector binds");
    if (probe == nullptr) {
        return;
    }
    ingress::ReserveHeaderDigestV1 quota_identity{};
    static_cast<void>(
        ingress::
            ComputeRawEmergencyReserveQuotaIdentitySha256V1(
                config.quota_subjects,
                &quota_identity,
                &error));
    struct stat status {};
    static_cast<void>(::fstat(temporary.fd(), &status));
    ingress::RawEmergencyReserveCapacityProbeRequestV1 request{};
    request.stage =
        ingress::RawEmergencyReserveProbeStageV1::
            kBeforeProvision;
    request.pool.reserve_state_uuid = Pattern<16U>(0x38U);
    request.pool.device_id =
        static_cast<std::uint64_t>(status.st_dev);
    request.pool.quota_identity_sha256 = quota_identity;
    request.pool.mount_identity_sha256 =
        config.mount_identity_sha256;
    request.byte_probe_version = 1U;
    request.inode_probe_version = 1U;
    request.required_bytes = 8192U;
    request.required_inodes = 8U;
    ingress::RawEmergencyReserveCapacityObservationV1
        observation{};
    test->Expect(
        !probe->Observe(
            temporary.fd(),
            request,
            &observation,
            &error),
        "effective capacity takes the minimum across every enabled quota subject");
}

}  // namespace

int main() {
    TestContext test;
    CheckQuotaIdentity(&test);
    CheckCapacityAndIdentityGates(&test);
    CheckRootAndBindingTamper(&test);
    CheckReserveChargeProof(&test);
    CheckReleasedAndUnknownInventory(&test);
    CheckInvalidBinding(&test);
    CheckProjectQuotaBinding(&test);
    CheckQuotaVectorMinimum(&test);
    if (test.failures() != 0) {
        std::cerr << test.failures()
                  << " capacity probe assertion(s) failed\n";
        return 1;
    }
    std::cout
        << "raw emergency reserve capacity probe tests passed\n";
    return 0;
}
