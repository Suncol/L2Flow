#include "l2flow/common/identity128.h"
#include "l2flow/ingress/raw_emergency_reserve_posix.h"
#include "l2flow/ingress/raw_reserve_coordinator.h"
#include "l2flow/ingress/reserve_emergency_transition_v1.h"

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <span>
#include <string>
#include <system_error>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace ingress = l2flow::ingress;

namespace {

struct TestContext final {
    void Expect(bool condition, const std::string& message) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << message << '\n';
        }
    }

    void ExpectError(
        ingress::RawEmergencyReservePosixErrorV1 actual,
        ingress::RawEmergencyReservePosixErrorV1 expected,
        const std::string& message) {
        if (actual != expected) {
            ++failures;
            std::cerr
                << "FAIL: " << message << " (expected "
                << ingress::RawEmergencyReservePosixErrorV1Name(
                       expected)
                << ", got "
                << ingress::RawEmergencyReservePosixErrorV1Name(
                       actual)
                << ")\n";
        }
    }

    int failures = 0;
};

class TempDirectory final {
public:
    TempDirectory() {
        char pattern[] = "/tmp/l2flow-reserve-inventory-XXXXXX";
        char* const created = ::mkdtemp(pattern);
        if (created == nullptr) {
            return;
        }
        path_ = created;
        fd_ = ::open(
            path_.c_str(),
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
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

    [[nodiscard]] bool valid() const noexcept {
        return fd_ >= 0;
    }
    [[nodiscard]] int fd() const noexcept {
        return fd_;
    }

private:
    std::string path_{};
    int fd_ = -1;
};

template <std::size_t Size>
std::array<std::byte, Size> Pattern(std::uint8_t first) {
    std::array<std::byte, Size> result{};
    for (std::size_t index = 0U; index < Size; ++index) {
        result[index] = static_cast<std::byte>(
            static_cast<unsigned int>(first) +
            static_cast<unsigned int>(index));
    }
    return result;
}

class ProvingProbe final
    : public ingress::RawEmergencyReserveCapacityProbeV1 {
public:
    bool Observe(
        int,
        const ingress::RawEmergencyReserveCapacityProbeRequestV1&
            request,
        ingress::RawEmergencyReserveCapacityObservationV1*
            observation,
        std::string*) noexcept override {
        if (observation == nullptr) {
            return false;
        }
        ingress::RawEmergencyReserveCapacityObservationV1 result{};
        result.pool = request.pool;
        result.byte_probe_version = request.byte_probe_version;
        result.inode_probe_version = request.inode_probe_version;
        result.filesystem_bytes_proven = true;
        result.quota_bytes_proven = prove_quota;
        result.filesystem_inodes_proven = true;
        result.quota_inodes_proven = prove_quota;
        result.filesystem_free_bytes = 1ULL << 40U;
        result.quota_free_bytes = 1ULL << 40U;
        result.filesystem_free_inodes = 1ULL << 32U;
        result.quota_free_inodes = 1ULL << 32U;
        if (request.stage !=
            ingress::RawEmergencyReserveProbeStageV1::
                kBeforeProvision) {
            result.reserve_byte_charge_proven = true;
            result.reserve_inode_charge_proven = true;
            result.proven_reserve_byte_charge =
                request.stage ==
                        ingress::RawEmergencyReserveProbeStageV1::
                            kReleased
                    ? 0U
                    : request.required_bytes;
            result.proven_reserve_inode_charge =
                request.stage ==
                        ingress::RawEmergencyReserveProbeStageV1::
                            kReleased
                    ? 0U
                    : request.required_inodes;
        }
        last_stage = request.stage;
        ++calls;
        *observation = result;
        return true;
    }

    bool prove_quota = true;
    std::uint32_t calls = 0U;
    ingress::RawEmergencyReserveProbeStageV1 last_stage =
        ingress::RawEmergencyReserveProbeStageV1::kAttached;
};

ingress::ReserveStateEntryV1 MakeRegistryEntry() {
    ingress::ReserveStateEntryV1 entry{};
    entry.source_stream_id = 1001U;
    entry.capture_date = 20260719U;
    entry.stream_day_id = Pattern<16U>(0x70U);
    entry.registry_status =
        ingress::ReserveRegistryStatusV1::kActive;
    entry.recovery_origin =
        ingress::ReserveRecoveryOriginV1::kFreshInit;
    entry.recovery_intent =
        ingress::ReserveRecoveryIntentV1::kResumeConnect;
    entry.writer_instance = Pattern<16U>(0x80U);
    entry.executor_or_recovery_attempt = Pattern<16U>(0x90U);
    entry.safe_stop_template_id = 17U;
    return entry;
}

ingress::ReserveCoordinatorStateV1 MakeBootstrap(
    int root_fd,
    std::uint8_t uuid_seed = 0x10U) {
    struct stat root_status {};
    static_cast<void>(::fstat(root_fd, &root_status));

    ingress::ReserveCoordinatorStateV1 state{};
    state.header.reserve_state_uuid =
        Pattern<16U>(uuid_seed);
    state.header.quota_identity_sha256 =
        Pattern<32U>(0x20U);
    state.header.mount_identity_sha256 =
        Pattern<32U>(0x40U);
    state.header.device_id =
        static_cast<std::uint64_t>(root_status.st_dev);
    state.header.declared_releasable_bytes = 1U << 20U;
    state.header.allocation_quantum_bytes = 4096U;
    state.header.declared_inode_reserve_count = 8U;
    state.header.byte_probe_version = 1U;
    state.header.inode_probe_version = 1U;
    state.header.safe_stop_catalog_sha256 =
        Pattern<32U>(0x50U);
    std::string error;
    static_cast<void>(
        ingress::ComputeRawEmergencyReserveInventorySha256V1(
            state.header,
            &state.header.inode_inventory_sha256,
            &error));

    ingress::ReserveStateSlotV1 slot{};
    slot.coordinator_state =
        ingress::ReserveCoordinatorPhaseV1::kProvisioned;
    slot.generation = 1U;
    slot.reserve_state_uuid =
        state.header.reserve_state_uuid;
    slot.entry_count = 1U;
    slot.entries[0U] = MakeRegistryEntry();
    state.slots[0U] = slot;
    state.slots[1U] = slot;
    state.selected_slot = 0U;
    return state;
}

std::string DataCandidateName(
    const ingress::ReserveStateV1Identity& uuid) {
    return ".reserve.data." +
           l2flow::common::Identity128Hex(uuid) +
           ".reserve-file-v1.tmp";
}

bool WritePartialCandidate(
    int root_fd,
    const std::string& name) {
    const int fd = ::openat(
        root_fd,
        name.c_str(),
        O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW |
            O_NONBLOCK | O_CLOEXEC,
        0600);
    if (fd < 0) {
        return false;
    }
    const std::array<std::byte, 13U> partial =
        Pattern<13U>(0xa0U);
    const ssize_t written =
        ::pwrite(fd, partial.data(), partial.size(), 0);
    const bool ok =
        written == static_cast<ssize_t>(partial.size()) &&
        ::fsync(fd) == 0 && ::fsync(root_fd) == 0;
    static_cast<void>(::close(fd));
    return ok;
}

struct InterruptContext final {
    ingress::RawEmergencyReserveMutationPointV1 target =
        ingress::RawEmergencyReserveMutationPointV1::
            kDataPublished;
    bool fired = false;
};

bool InterruptOnce(
    ingress::RawEmergencyReserveMutationPointV1 point,
    std::uint32_t,
    void* context) noexcept {
    auto* state = static_cast<InterruptContext*>(context);
    if (state != nullptr && !state->fired &&
        point == state->target) {
        state->fired = true;
        return false;
    }
    return true;
}

void SetAction(
    ingress::ReserveStateEntryV1* entry,
    std::size_t action_id,
    ingress::FinalizationActionKindV1 kind,
    std::uint32_t byte_cap_quanta,
    std::uint32_t inode_cap,
    std::uint8_t seed) {
    auto& receipt = entry->actions[action_id];
    receipt.action_id =
        static_cast<std::uint16_t>(action_id);
    receipt.action_kind = kind;
    receipt.action_state =
        ingress::FinalizationActionStateV1::kPending;
    receipt.byte_cap_quanta = byte_cap_quanta;
    receipt.inode_cap = inode_cap;
    receipt.object_plan_sha256 = Pattern<32U>(seed);

    auto& plan = entry->plans[action_id];
    plan.plan_version = 1U;
    plan.object_type = kind;
    plan.object_sequence =
        static_cast<std::uint32_t>(action_id + 1U);
    plan.range_start =
        static_cast<std::uint64_t>(action_id * 100U);
    plan.range_end_or_size =
        static_cast<std::uint64_t>((action_id + 1U) * 100U);
    plan.causal_id =
        Pattern<16U>(static_cast<std::uint8_t>(seed + 1U));
}

ingress::ReserveStateEntryV1 MakeGrant(
    const ingress::ReserveStateEntryV1& registry,
    const ingress::ReserveCoordinatorHeaderV1& header) {
    ingress::ReserveStateEntryV1 entry{};
    entry.source_stream_id = registry.source_stream_id;
    entry.capture_date = registry.capture_date;
    entry.stream_day_id = registry.stream_day_id;
    entry.ack_status = ingress::ReserveAckStatusV1::kAcked;
    entry.counter_validity =
        ingress::kReserveCounterValidityMask;
    entry.grant_status =
        ingress::ReserveGrantStatusV1::kPending;
    entry.grant_flags =
        ingress::kReserveGrantRawFinalization;
    entry.writer_instance = registry.writer_instance;
    entry.raw_counters.callback_published_records = 50U;
    entry.raw_counters.callback_published_vendor_bytes = 5000U;
    entry.raw_counters.append_global_wal_pos = 4000U;
    entry.raw_counters.append_ingress_sequence = 40U;
    entry.raw_counters.durable_global_wal_pos = 3000U;
    entry.raw_counters.durable_ingress_sequence = 30U;
    entry.raw_counters.queued_record_count = 10U;
    entry.raw_counters.queued_framed_wal_bytes = 1000U;
    entry.safe_stop_template_id =
        registry.safe_stop_template_id;
    SetAction(
        &entry,
        0U,
        ingress::FinalizationActionKindV1::
            kCurrentSegmentDrain,
        2U,
        1U,
        0xa0U);
    SetAction(
        &entry,
        1U,
        ingress::FinalizationActionKindV1::
            kFinalizationReport,
        3U,
        2U,
        0xb0U);
    entry.grant_bytes =
        5U * header.allocation_quantum_bytes;
    return entry;
}

bool TransitionToPrepared(
    ingress::RawReserveStateFileV1* state_file,
    std::string* error) {
    if (state_file == nullptr) {
        return false;
    }
    const auto& provisioned_state = state_file->state();
    const auto& provisioned =
        provisioned_state
            .slots[provisioned_state.selected_slot];
    ingress::ReserveReleaseIntentV1 intent_request{};
    intent_request.reason =
        ingress::ReserveReleaseReasonV1::kLowWatermark;
    intent_request.trigger =
        ingress::ReserveReleaseTriggerV1::kFilesystemBytes;
    intent_request.writer_set_sha256 = Pattern<32U>(0xc0U);
    ingress::ReserveStateSlotV1 intent{};
    if (ingress::BuildReserveReleasingIntentV1(
            provisioned_state.header,
            provisioned,
            intent_request,
            &intent) != ingress::ReserveStateV1Error::kNone ||
        state_file->PublishNext(
            intent, nullptr, error) !=
            ingress::RawReserveStatePosixError::kNone) {
        return false;
    }

    const auto& intent_state = state_file->state();
    const auto& selected_intent =
        intent_state.slots[intent_state.selected_slot];
    const std::array<ingress::ReserveStateEntryV1, 1U> grants{
        MakeGrant(
            selected_intent.entries[0U],
            intent_state.header)};
    ingress::ReserveReleasePreparedV1 request{};
    request.finalization_cycle_id = Pattern<16U>(0xd0U);
    request.grant_entries = grants;
    request.pre_release_fs_free_bytes = 1000000U;
    request.pre_release_quota_free_bytes = 900000U;
    request.pre_release_fs_free_inodes = 500U;
    request.pre_release_quota_free_inodes = 400U;
    request.expected_release_fs_bytes =
        intent_state.header.declared_releasable_bytes;
    request.expected_release_quota_bytes =
        intent_state.header.declared_releasable_bytes;
    request.expected_release_fs_inodes =
        intent_state.header.declared_inode_reserve_count +
        1U;
    request.expected_release_quota_inodes =
        intent_state.header.declared_inode_reserve_count +
        1U;
    request.reserved_margin_bytes =
        intent_state.header.allocation_quantum_bytes;
    request.reserved_margin_inodes = 1U;
    request.effective_min_fs_free_bytes =
        intent_state.header.declared_releasable_bytes;
    request.effective_min_quota_free_bytes =
        intent_state.header.declared_releasable_bytes;
    request.effective_min_fs_free_inodes =
        intent_state.header.declared_inode_reserve_count;
    request.effective_min_quota_free_inodes =
        intent_state.header.declared_inode_reserve_count;
    ingress::ReserveStateSlotV1 prepared{};
    return ingress::BuildReserveReleasingPreparedV1(
               intent_state.header,
               selected_intent,
               request,
               &prepared) ==
               ingress::ReserveStateV1Error::kNone &&
           state_file->PublishNext(
               prepared, nullptr, error) ==
               ingress::RawReserveStatePosixError::kNone;
}

void CheckCommitmentAndQuotaGate(TestContext* test) {
    TempDirectory temporary;
    test->Expect(temporary.valid(), "quota-gate temp root opens");
    if (!temporary.valid()) {
        return;
    }
    auto bootstrap = MakeBootstrap(temporary.fd());
    ingress::ReserveStateV1Digest recomputed{};
    std::string error;
    test->Expect(
        ingress::ComputeRawEmergencyReserveInventorySha256V1(
            bootstrap.header, &recomputed, &error) ==
            ingress::RawEmergencyReservePosixErrorV1::kNone,
        "inventory commitment computes");
    test->Expect(
        recomputed == bootstrap.header.inode_inventory_sha256,
        "inventory commitment is deterministic");

    ProvingProbe probe;
    probe.prove_quota = false;
    ingress::RawEmergencyReserveProvisionResultV1 result{};
    ingress::RawEmergencyReservePosixErrorV1 failure{};
    test->Expect(
        !ingress::ProvisionFreshRawEmergencyReserveV1(
            temporary.fd(),
            bootstrap,
            &probe,
            nullptr,
            &result,
            &failure,
            nullptr,
            &error),
        "statvfs-like probe without quota proof is rejected");
    test->ExpectError(
        failure,
        ingress::RawEmergencyReservePosixErrorV1::
            kCapacityProbeFailure,
        "missing quota proof classification");
}

void CheckShortCandidateAndProvision(TestContext* test) {
    TempDirectory temporary;
    test->Expect(temporary.valid(), "provision temp root opens");
    if (!temporary.valid()) {
        return;
    }
    const auto bootstrap = MakeBootstrap(temporary.fd());
    const std::string candidate =
        DataCandidateName(bootstrap.header.reserve_state_uuid);
    test->Expect(
        WritePartialCandidate(temporary.fd(), candidate),
        "recognized short data candidate is staged");

    ProvingProbe probe;
    ingress::RawEmergencyReserveProvisionResultV1 result{};
    ingress::RawEmergencyReservePosixErrorV1 failure{};
    ingress::ReserveStateV1Error codec_error{};
    std::string error;
    test->Expect(
        ingress::ProvisionFreshRawEmergencyReserveV1(
            temporary.fd(),
            bootstrap,
            &probe,
            nullptr,
            &result,
            &failure,
            &codec_error,
            &error),
        "recognized unpublished short candidate is cleaned and rebuilt");
    test->Expect(
        result.state_file != nullptr &&
            result.inventory != nullptr,
        "provision returns retained state and inventory capabilities");
    if (result.inventory != nullptr) {
        test->Expect(
            result.inventory->data_present() &&
                result.inventory
                        ->remaining_inode_prefix_count() ==
                    bootstrap.header
                        .declared_inode_reserve_count &&
                result.inventory->inode_descriptors().size() ==
                    bootstrap.header
                        .declared_inode_reserve_count,
            "provision retains fixed data and exact complete inventory");

        struct stat data_status {};
        ingress::ReserveHeaderWireV1 data_wire{};
        ingress::ReserveFileHeaderV1 data_header{};
        bool exact_charge =
            ::fstat(
                result.inventory->data_descriptor(),
                &data_status) == 0 &&
            data_status.st_blocks >= 0 &&
            data_status.st_size >= 0 &&
            ::pread(
                result.inventory->data_descriptor(),
                data_wire.data(),
                data_wire.size(),
                0) ==
                static_cast<ssize_t>(data_wire.size()) &&
            ingress::DecodeReserveFileHeaderV1(
                data_wire, &data_header) ==
                ingress::ReserveHeaderV1Error::kNone &&
            data_header.declared_bytes ==
                static_cast<std::uint64_t>(
                    data_status.st_size) &&
            data_header.declared_bytes <
                bootstrap.header
                    .declared_releasable_bytes;
        std::uint64_t allocated_bytes =
            exact_charge
                ? static_cast<std::uint64_t>(
                      data_status.st_blocks) *
                      512U
                : 0U;
        for (const int inode_fd :
             result.inventory->inode_descriptors()) {
            struct stat inode_status {};
            exact_charge =
                exact_charge &&
                ::fstat(inode_fd, &inode_status) == 0 &&
                inode_status.st_blocks >= 0;
            if (exact_charge) {
                allocated_bytes +=
                    static_cast<std::uint64_t>(
                        inode_status.st_blocks) *
                    512U;
            }
        }
        test->Expect(
            exact_charge &&
                allocated_bytes ==
                    bootstrap.header
                        .declared_releasable_bytes,
            "state releasable bytes equal data plus inode-reserve allocated bytes exactly");
    }

    ProvingProbe no_quota;
    no_quota.prove_quota = false;
    if (result.state_file != nullptr) {
        auto rejected =
            ingress::AttachRawEmergencyReserveInventoryV1(
                *result.state_file,
                &no_quota,
                &failure,
                &error);
        test->Expect(
            rejected == nullptr,
            "normal attach also rejects missing quota proof");

        test->Expect(
            WritePartialCandidate(
                temporary.fd(), candidate),
            "post-publication candidate contradiction is staged");
        rejected =
            ingress::AttachRawEmergencyReserveInventoryV1(
                *result.state_file,
                &probe,
                &failure,
                &error);
        test->Expect(
            rejected == nullptr,
            "normal attach rejects a candidate beside fixed state");
        test->ExpectError(
            failure,
            ingress::RawEmergencyReservePosixErrorV1::
                kCandidateConflict,
            "post-publication candidate contradiction classification");
    }
}

void CheckNonRepresentableReleasableChargeRejected(
    TestContext* test) {
    TempDirectory temporary;
    test->Expect(
        temporary.valid(),
        "nonrepresentable reserve charge temp root opens");
    if (!temporary.valid()) {
        return;
    }
    auto bootstrap = MakeBootstrap(temporary.fd());
    // st_blocks is expressed in 512-byte units.  This immutable total can
    // therefore never equal the sum of data and inode-reserve st_blocks,
    // even though every individual logical size remains constructible.
    ++bootstrap.header.declared_releasable_bytes;
    std::string error;
    test->Expect(
        ingress::ComputeRawEmergencyReserveInventorySha256V1(
            bootstrap.header,
            &bootstrap.header.inode_inventory_sha256,
            &error) ==
            ingress::RawEmergencyReservePosixErrorV1::kNone,
        "nonrepresentable charge bootstrap keeps a valid inventory commitment");

    ProvingProbe probe;
    ingress::RawEmergencyReserveProvisionResultV1 result{};
    ingress::RawEmergencyReservePosixErrorV1 failure{};
    ingress::ReserveStateV1Error codec_error{};
    test->Expect(
        !ingress::ProvisionFreshRawEmergencyReserveV1(
            temporary.fd(),
            bootstrap,
            &probe,
            nullptr,
            &result,
            &failure,
            &codec_error,
            &error) &&
            failure ==
                ingress::RawEmergencyReservePosixErrorV1::
                    kAllocationProofFailure &&
            result.state_file == nullptr &&
            result.inventory == nullptr,
        "provision rejects a lying probe when actual st_blocks cannot equal immutable releasable bytes");
}

void CheckDataPublishCrashAdoption(TestContext* test) {
    TempDirectory temporary;
    test->Expect(temporary.valid(), "crash-adoption temp root opens");
    if (!temporary.valid()) {
        return;
    }
    const auto bootstrap = MakeBootstrap(temporary.fd(), 0x31U);
    ProvingProbe probe;
    InterruptContext interrupt{};
    interrupt.target =
        ingress::RawEmergencyReserveMutationPointV1::
            kDataPublished;
    const ingress::RawEmergencyReserveMutationHooksV1 hooks{
        &InterruptOnce, &interrupt};
    ingress::RawEmergencyReserveProvisionResultV1 result{};
    ingress::RawEmergencyReservePosixErrorV1 failure{};
    std::string error;
    test->Expect(
        !ingress::ProvisionFreshRawEmergencyReserveV1(
            temporary.fd(),
            bootstrap,
            &probe,
            &hooks,
            &result,
            &failure,
            nullptr,
            &error),
        "data-publication crash seam interrupts provision");
    test->ExpectError(
        failure,
        ingress::RawEmergencyReservePosixErrorV1::
            kInjectedInterruption,
        "data-publication interruption classification");
    struct stat status {};
    test->Expect(
        ::fstatat(
            temporary.fd(),
            ingress::kRawEmergencyReserveDataFilename,
            &status,
            AT_SYMLINK_NOFOLLOW) == 0 &&
            ::fstatat(
                temporary.fd(),
                ingress::kRawReserveStateFilename,
                &status,
                AT_SYMLINK_NOFOLLOW) != 0 &&
            errno == ENOENT,
        "crash seam leaves data final before state final");

    test->Expect(
        ingress::ProvisionFreshRawEmergencyReserveV1(
            temporary.fd(),
            bootstrap,
            &probe,
            nullptr,
            &result,
            &failure,
            nullptr,
            &error),
        "retry rebuilds barriers and adopts exact state candidate");
}

void CheckWrongPathAndHole(TestContext* test) {
    {
        TempDirectory temporary;
        test->Expect(
            temporary.valid(), "wrong-path temp root opens");
        if (temporary.valid()) {
            const auto bootstrap =
                MakeBootstrap(temporary.fd(), 0x41U);
            test->Expect(
                ::symlinkat(
                    ".",
                    temporary.fd(),
                    ingress::
                        kRawEmergencyReserveInodesDirectory) == 0,
                "reserve-inodes symlink is staged");
            ProvingProbe probe;
            ingress::RawEmergencyReserveProvisionResultV1 result{};
            ingress::RawEmergencyReservePosixErrorV1 failure{};
            std::string error;
            test->Expect(
                !ingress::ProvisionFreshRawEmergencyReserveV1(
                    temporary.fd(),
                    bootstrap,
                    &probe,
                    nullptr,
                    &result,
                    &failure,
                    nullptr,
                    &error),
                "reserve-inodes symlink is rejected");
            test->ExpectError(
                failure,
                ingress::RawEmergencyReservePosixErrorV1::
                    kUnsafeInodeDirectory,
                "wrong directory type classification");
        }
    }

    TempDirectory temporary;
    test->Expect(temporary.valid(), "hole temp root opens");
    if (!temporary.valid()) {
        return;
    }
    const auto bootstrap = MakeBootstrap(temporary.fd(), 0x51U);
    ProvingProbe probe;
    ingress::RawEmergencyReserveProvisionResultV1 result{};
    ingress::RawEmergencyReservePosixErrorV1 failure{};
    std::string error;
    test->Expect(
        ingress::ProvisionFreshRawEmergencyReserveV1(
            temporary.fd(),
            bootstrap,
            &probe,
            nullptr,
            &result,
            &failure,
            nullptr,
            &error),
        "hole test provision succeeds");
    result.inventory.reset();

    const int inode_directory = ::openat(
        temporary.fd(),
        ingress::kRawEmergencyReserveInodesDirectory,
        O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    std::string hole_name;
    static_cast<void>(
        ingress::FormatReserveInodeFilenameV1(
            bootstrap.header.reserve_state_uuid,
            3U,
            &hole_name));
    test->Expect(
        inode_directory >= 0 &&
            ::unlinkat(
                inode_directory,
                hole_name.c_str(),
                0) == 0 &&
            ::fsync(inode_directory) == 0,
        "middle inventory index is removed");
    if (inode_directory >= 0) {
        static_cast<void>(::close(inode_directory));
    }
    auto rejected =
        ingress::AttachRawEmergencyReserveInventoryV1(
            *result.state_file,
            &probe,
            &failure,
            &error);
    test->Expect(
        rejected == nullptr,
        "inventory hole is rejected on secure attach");
    test->ExpectError(
        failure,
        ingress::RawEmergencyReservePosixErrorV1::
            kInventoryMismatch,
        "inventory hole classification");
}

void CheckDescendingReleaseRestart(TestContext* test) {
    TempDirectory temporary;
    test->Expect(temporary.valid(), "release temp root opens");
    if (!temporary.valid()) {
        return;
    }
    const auto bootstrap = MakeBootstrap(temporary.fd(), 0x61U);
    ProvingProbe probe;
    ingress::RawEmergencyReserveProvisionResultV1 result{};
    ingress::RawEmergencyReservePosixErrorV1 failure{};
    std::string error;
    test->Expect(
        ingress::ProvisionFreshRawEmergencyReserveV1(
            temporary.fd(),
            bootstrap,
            &probe,
            nullptr,
            &result,
            &failure,
            nullptr,
            &error),
        "release test provision succeeds");
    if (result.state_file == nullptr ||
        result.inventory == nullptr) {
        return;
    }
    result.inventory.reset();
    test->Expect(
        TransitionToPrepared(result.state_file.get(), &error),
        "state transitions durably to RELEASING_PREPARED");
    auto prepared =
        ingress::AttachRawEmergencyReserveInventoryV1(
            *result.state_file,
            &probe,
            &failure,
            &error);
    test->Expect(
        prepared != nullptr && prepared->data_present(),
        "PREPARED attach retains full reserve before release");
    if (prepared == nullptr) {
        return;
    }

    InterruptContext interrupt{};
    interrupt.target =
        ingress::RawEmergencyReserveMutationPointV1::
            kInodeReleased;
    const ingress::RawEmergencyReserveMutationHooksV1 hooks{
        &InterruptOnce, &interrupt};
    const auto first_release =
        ingress::ReleaseRawEmergencyReserveV1(
            *prepared, &probe, &hooks, &error);
    test->ExpectError(
        first_release,
        ingress::RawEmergencyReservePosixErrorV1::
            kInjectedInterruption,
        "release interruption occurs after first descending inode");
    test->Expect(
        !prepared->data_present() &&
            prepared->remaining_inode_prefix_count() ==
                bootstrap.header.declared_inode_reserve_count -
                    1U,
        "interrupted release retains exact remaining prefix");

    prepared.reset();
    auto restarted =
        ingress::AttachRawEmergencyReserveInventoryV1(
            *result.state_file,
            &probe,
            &failure,
            &error);
    test->Expect(
        restarted != nullptr &&
            !restarted->data_present() &&
            restarted->remaining_inode_prefix_count() ==
                bootstrap.header.declared_inode_reserve_count -
                    1U,
        "release restart adopts only the exact synced prefix");
    if (restarted == nullptr) {
        return;
    }
    test->ExpectError(
        ingress::ReleaseRawEmergencyReserveV1(
            *restarted, &probe, nullptr, &error),
        ingress::RawEmergencyReservePosixErrorV1::kNone,
        "release restart finishes descending unlink/close/dirsync");
    test->Expect(
        !restarted->data_present() &&
            restarted->remaining_inode_prefix_count() == 0U &&
            restarted->inode_descriptors().empty() &&
            probe.last_stage ==
                ingress::RawEmergencyReserveProbeStageV1::
                    kReleased,
        "completed release proves zero charge in all four dimensions");
}

void CheckRetainedNameToInodeBinding(TestContext* test) {
    TempDirectory temporary;
    test->Expect(
        temporary.valid(), "inode-replacement temp root opens");
    if (!temporary.valid()) {
        return;
    }
    const auto bootstrap = MakeBootstrap(temporary.fd(), 0x71U);
    ProvingProbe probe;
    ingress::RawEmergencyReserveProvisionResultV1 result{};
    ingress::RawEmergencyReservePosixErrorV1 failure{};
    std::string error;
    test->Expect(
        ingress::ProvisionFreshRawEmergencyReserveV1(
            temporary.fd(),
            bootstrap,
            &probe,
            nullptr,
            &result,
            &failure,
            nullptr,
            &error),
        "inode-replacement provision succeeds");
    if (result.state_file == nullptr ||
        result.inventory == nullptr) {
        return;
    }
    result.inventory.reset();
    test->Expect(
        TransitionToPrepared(result.state_file.get(), &error),
        "inode-replacement state reaches PREPARED");
    auto prepared =
        ingress::AttachRawEmergencyReserveInventoryV1(
            *result.state_file,
            &probe,
            &failure,
            &error);
    if (prepared == nullptr) {
        test->Expect(false, "inode-replacement PREPARED attach succeeds");
        return;
    }

    const int directory_fd =
        prepared->inode_directory_descriptor();
    const std::uint32_t replaced_index =
        bootstrap.header.declared_inode_reserve_count - 1U;
    std::string final_name;
    static_cast<void>(
        ingress::FormatReserveInodeFilenameV1(
            bootstrap.header.reserve_state_uuid,
            replaced_index,
            &final_name));
    const int original_fd = ::openat(
        directory_fd,
        final_name.c_str(),
        O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    ingress::ReserveHeaderWireV1 header_wire{};
    const ssize_t read_count =
        original_fd < 0
            ? -1
            : ::pread(
                  original_fd,
                  header_wire.data(),
                  header_wire.size(),
                  0);
    if (original_fd >= 0) {
        static_cast<void>(::close(original_fd));
    }
    const char replacement_name[] = ".replacement.reserve.tmp";
    const int replacement_fd = ::openat(
        directory_fd,
        replacement_name,
        O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW |
            O_NONBLOCK | O_CLOEXEC,
        0600);
    bool replaced = replacement_fd >= 0 &&
                    read_count ==
                        static_cast<ssize_t>(
                            header_wire.size()) &&
                    ::pwrite(
                        replacement_fd,
                        header_wire.data(),
                        header_wire.size(),
                        0) ==
                        static_cast<ssize_t>(
                            header_wire.size()) &&
                    ::posix_fallocate(
                        replacement_fd,
                        0,
                        static_cast<off_t>(
                            bootstrap.header
                                .allocation_quantum_bytes)) == 0 &&
                    ::fsync(replacement_fd) == 0 &&
                    ::renameat(
                        directory_fd,
                        replacement_name,
                        directory_fd,
                        final_name.c_str()) == 0 &&
                    ::fsync(directory_fd) == 0;
    if (replacement_fd >= 0) {
        static_cast<void>(::close(replacement_fd));
    }
    test->Expect(
        replaced,
        "same-header replacement inode is durably installed");
    if (!replaced) {
        return;
    }
    test->ExpectError(
        ingress::ReleaseRawEmergencyReserveV1(
            *prepared, &probe, nullptr, &error),
        ingress::RawEmergencyReservePosixErrorV1::
            kReleaseOrderViolation,
        "retained name-to-inode replacement is rejected before release");
    struct stat data_status {};
    test->Expect(
        ::fstatat(
            temporary.fd(),
            ingress::kRawEmergencyReserveDataFilename,
            &data_status,
            AT_SYMLINK_NOFOLLOW) == 0,
        "failed identity revalidation does not unlink data reserve");
}

void CheckCoordinatorPreparedReleaseComposition(
    TestContext* test) {
    TempDirectory temporary;
    test->Expect(
        temporary.valid(),
        "coordinator release temp root opens");
    if (!temporary.valid()) {
        return;
    }

    const auto bootstrap =
        MakeBootstrap(temporary.fd(), 0x31U);
    ProvingProbe probe;
    ingress::RawEmergencyReserveProvisionResultV1
        provisioned{};
    ingress::RawEmergencyReservePosixErrorV1
        reserve_failure{};
    std::string error;
    test->Expect(
        ingress::ProvisionFreshRawEmergencyReserveV1(
            temporary.fd(),
            bootstrap,
            &probe,
            nullptr,
            &provisioned,
            &reserve_failure,
            nullptr,
            &error),
        "coordinator release reserve provision succeeds");
    if (provisioned.state_file == nullptr ||
        provisioned.inventory == nullptr) {
        return;
    }
    const char date_name[] = "capture_date=20260719";
    const char route_name[] = "stream=1001-sh-snapshot";
    bool route_created =
        ::mkdirat(temporary.fd(), date_name, 0700) == 0;
    const int date_fd = ::openat(
        temporary.fd(),
        date_name,
        O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
            O_CLOEXEC);
    route_created =
        route_created && date_fd >= 0 &&
        ::fsync(temporary.fd()) == 0 &&
        ::mkdirat(date_fd, route_name, 0700) == 0 &&
        ::fsync(date_fd) == 0;
    if (date_fd >= 0) {
        static_cast<void>(::close(date_fd));
    }
    test->Expect(
        route_created,
        "coordinator finalization route is durably scaffolded while PROVISIONED");
    if (!route_created) {
        return;
    }
    // The coordinator becomes the sole state owner and must not retain a
    // second inventory descriptor set across physical release.
    provisioned.inventory.reset();
    provisioned.state_file.reset();

    ingress::RawReserveCoordinatorLeaseMarkerV1 marker{};
    marker.coordinator_identity =
        bootstrap.header.reserve_state_uuid;
    marker.device_id = bootstrap.header.device_id;
    marker.quota_identity_sha256 =
        bootstrap.header.quota_identity_sha256;
    marker.mount_identity_sha256 =
        bootstrap.header.mount_identity_sha256;
    ingress::RawReserveCoordinatorErrorV1
        coordinator_failure{};
    auto coordinator =
        ingress::AttachRawReserveRegistryCoordinatorAtV1(
            temporary.fd(),
            marker,
            &coordinator_failure,
            &error);
    test->Expect(
        coordinator != nullptr,
        "coordinator attaches provisioned reserve state");
    if (coordinator == nullptr) {
        return;
    }

    ingress::ReserveReleaseIntentV1 intent{};
    intent.reason =
        ingress::ReserveReleaseReasonV1::kLowWatermark;
    intent.trigger =
        ingress::ReserveReleaseTriggerV1::
            kFilesystemBytes;
    intent.writer_set_sha256 = Pattern<32U>(0xc1U);
    test->Expect(
        coordinator->PublishReleasingIntent(
            intent, &error) ==
            ingress::RawReserveCoordinatorErrorV1::kNone,
        "coordinator durably publishes RELEASING_INTENT");

    const auto intent_state = coordinator->state();
    if (intent_state.selected_slot >=
        intent_state.slots.size()) {
        test->Expect(false, "intent selected slot is valid");
        return;
    }
    const auto& selected_intent =
        intent_state.slots[intent_state.selected_slot];
    const std::array<
        ingress::ReserveStateEntryV1,
        1U>
        grants{MakeGrant(
            selected_intent.entries[0U],
            intent_state.header)};
    ingress::ReserveReleasePreparedV1 prepared{};
    prepared.finalization_cycle_id =
        Pattern<16U>(0xd1U);
    prepared.grant_entries = grants;
    prepared.pre_release_fs_free_bytes = 1'000'000U;
    prepared.pre_release_quota_free_bytes = 900'000U;
    prepared.pre_release_fs_free_inodes = 500U;
    prepared.pre_release_quota_free_inodes = 400U;
    prepared.expected_release_fs_bytes =
        intent_state.header.declared_releasable_bytes;
    prepared.expected_release_quota_bytes =
        intent_state.header.declared_releasable_bytes;
    prepared.expected_release_fs_inodes =
        intent_state.header.declared_inode_reserve_count +
        1U;
    prepared.expected_release_quota_inodes =
        intent_state.header.declared_inode_reserve_count +
        1U;
    prepared.reserved_margin_bytes =
        intent_state.header.allocation_quantum_bytes;
    prepared.reserved_margin_inodes = 1U;
    prepared.effective_min_fs_free_bytes =
        intent_state.header.declared_releasable_bytes;
    prepared.effective_min_quota_free_bytes =
        intent_state.header.declared_releasable_bytes;
    prepared.effective_min_fs_free_inodes =
        intent_state.header.declared_inode_reserve_count;
    prepared.effective_min_quota_free_inodes =
        intent_state.header.declared_inode_reserve_count;
    test->Expect(
        coordinator->PublishReleasingPrepared(
            prepared, &error) ==
            ingress::RawReserveCoordinatorErrorV1::kNone,
        "coordinator durably publishes RELEASING_PREPARED");

    InterruptContext interrupt{};
    interrupt.target =
        ingress::RawEmergencyReserveMutationPointV1::
            kInodeReleased;
    const ingress::RawEmergencyReserveMutationHooksV1
        hooks{&InterruptOnce, &interrupt};
    const auto interrupted =
        coordinator->ReleasePreparedAndPublishConsumed(
            &probe, &hooks, &error);
    test->Expect(
        interrupted.coordinator_error ==
                ingress::RawReserveCoordinatorErrorV1::
                    kReserveReleaseFailure &&
            interrupted.reserve_error ==
                ingress::RawEmergencyReservePosixErrorV1::
                    kInjectedInterruption &&
            !interrupted.consumed_state_published,
        "interrupted physical release stays durably PREPARED");
    const auto still_prepared = coordinator->state();
    test->Expect(
        still_prepared
                .slots[still_prepared.selected_slot]
                .coordinator_state ==
            ingress::ReserveCoordinatorPhaseV1::
                kReleasingPrepared,
        "interrupted coordinator state cannot roll back or skip to CONSUMED");

    const auto completed =
        coordinator->ReleasePreparedAndPublishConsumed(
            &probe, nullptr, &error);
    test->Expect(
        completed.ok(),
        "coordinator resumes exact release prefix and publishes CONSUMED");
    const auto consumed = coordinator->state();
    test->Expect(
        consumed.slots[consumed.selected_slot]
                .coordinator_state ==
            ingress::ReserveCoordinatorPhaseV1::kConsumed,
        "successful physical proof selects CONSUMED state");
    struct stat absent {};
    test->Expect(
        ::fstatat(
            temporary.fd(),
            ingress::kRawEmergencyReserveDataFilename,
            &absent,
            AT_SYMLINK_NOFOLLOW) != 0 &&
            errno == ENOENT,
        "CONSUMED publication follows absent data reserve");

    const auto consumed_slot =
        consumed.slots[consumed.selected_slot];
    const auto& consumed_entry =
        consumed_slot.entries[0U];
    ingress::ReserveFinalizationGrantKeyV1 grant_key{};
    grant_key.finalization_cycle_id =
        consumed_slot.finalization_cycle_id;
    grant_key.source_stream_id =
        consumed_entry.source_stream_id;
    grant_key.capture_date =
        consumed_entry.capture_date;
    grant_key.stream_day_id =
        consumed_entry.stream_day_id;
    grant_key.ack_status =
        consumed_entry.ack_status;
    grant_key.ack_writer_instance =
        consumed_entry.writer_instance;
    grant_key.safe_stop_template_id =
        consumed_entry.safe_stop_template_id;
    ingress::ReserveGrantActivationV1 activation{};
    activation.grant = grant_key;
    activation.executor_instance =
        consumed_entry.writer_instance;
    activation.filesystem_free_byte_baseline =
        800'000U;
    activation.quota_free_byte_baseline =
        700'000U;
    activation.filesystem_free_inode_baseline =
        300U;
    activation.quota_free_inode_baseline = 200U;
    test->Expect(
        coordinator->ActivateNextFinalizationGrant(
            activation, &error) ==
            ingress::RawReserveCoordinatorErrorV1::kNone,
        "coordinator durably activates and precharges first grant");

    const auto active_state = coordinator->state();
    const auto& active_slot =
        active_state.slots[active_state.selected_slot];
    const auto& action_receipt =
        active_slot.entries[0U].actions[0U];
    ingress::ReserveFinalizationActionKeyV1
        action_key{};
    action_key.grant = grant_key;
    action_key.action_id = 0U;
    action_key.action_kind =
        action_receipt.action_kind;
    action_key.object_plan_sha256 =
        action_receipt.object_plan_sha256;
    test->Expect(
        coordinator->DebitFinalizationAction(
            action_key, &error) ==
            ingress::RawReserveCoordinatorErrorV1::kNone,
        "coordinator durably debits the canonical first action");

    auto action =
        coordinator->AcquireDebitedFinalizationAction(
            action_key,
            "sh-snapshot",
            &coordinator_failure,
            &error);
    test->Expect(
        action != nullptr && action->ValidateLatest(&error),
        "DEBITED Raw action issues an exact shared-gate capability");
    if (action != nullptr) {
        ingress::ReserveStateV1Digest expected_grant{};
        const auto debited_state = coordinator->state();
        const auto& debited_slot =
            debited_state
                .slots[debited_state.selected_slot];
        test->Expect(
            ingress::
                    ComputeImmutableFinalizationGrantSha256V1(
                        debited_state.header,
                        debited_slot,
                        0U,
                        &expected_grant) ==
                    ingress::ReserveStateV1Error::kNone &&
                action->immutable_grant_sha256() ==
                    expected_grant &&
                action->byte_cap() ==
                    2U *
                        debited_state.header
                            .allocation_quantum_bytes &&
                action->inode_cap() == 1U &&
                action->route_directory_descriptor() >=
                    0 &&
                action->raw_root_descriptor() >= 0,
            "action capability binds immutable grant, caps and retained target");
    }
    auto wrong_domain =
        coordinator->AcquireDebitedScaffoldingAction(
            action_key,
            &coordinator_failure,
            &error);
    test->Expect(
        wrong_domain == nullptr,
        "RAW_FINALIZATION debit cannot be issued as domain scaffolding capability");
    action.reset();
    test->Expect(
        coordinator->CompleteFinalizationAction(
            action_key, &error) ==
            ingress::RawReserveCoordinatorErrorV1::kNone,
        "action completes only after its shared generation gate is released");
}

}  // namespace

int main() {
    TestContext test;
    CheckCommitmentAndQuotaGate(&test);
    CheckShortCandidateAndProvision(&test);
    CheckNonRepresentableReleasableChargeRejected(&test);
    CheckDataPublishCrashAdoption(&test);
    CheckWrongPathAndHole(&test);
    CheckDescendingReleaseRestart(&test);
    CheckRetainedNameToInodeBinding(&test);
    CheckCoordinatorPreparedReleaseComposition(&test);

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " raw emergency reserve POSIX test(s) failed\n";
        return 1;
    }
    std::cout
        << "raw emergency reserve POSIX lifecycle tests passed\n";
    return 0;
}
