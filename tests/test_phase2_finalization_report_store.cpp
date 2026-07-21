#include "l2flow/common/sha256.h"
#include "l2flow/ingress/finalization_report_store.h"
#include "l2flow/ingress/raw_reserve_state_posix.h"
#include "l2flow/ingress/raw_schema.h"
#include "l2flow/ingress/reserve_emergency_transition_v1.h"
#include "l2flow/ingress/scaffolding_finalization_report_store.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

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

    int failures = 0;
};

class FixedFinalizationCapacityProbe final
    : public ingress::
          RawEmergencyReserveCapacityProbeV1 {
public:
    [[nodiscard]] bool Observe(
        int retained_raw_root_fd,
        const ingress::
            RawEmergencyReserveCapacityProbeRequestV1&
            request,
        ingress::
            RawEmergencyReserveCapacityObservationV1*
            observation,
        std::string* error) noexcept override {
        if (retained_raw_root_fd < 0 ||
            observation == nullptr ||
            request.stage !=
                ingress::
                    RawEmergencyReserveProbeStageV1::
                        kFinalizationPostPublish) {
            if (error != nullptr) {
                *error =
                    "unexpected finalization capacity request";
            }
            return false;
        }
        observation->pool = request.pool;
        observation->byte_probe_version =
            request.byte_probe_version;
        observation->inode_probe_version =
            request.inode_probe_version;
        observation->filesystem_bytes_proven = true;
        observation->quota_bytes_proven = true;
        observation->filesystem_inodes_proven = true;
        observation->quota_inodes_proven = true;
        observation->filesystem_free_bytes =
            filesystem_free_bytes;
        observation->quota_free_bytes =
            quota_free_bytes;
        observation->filesystem_free_inodes =
            filesystem_free_inodes;
        observation->quota_free_inodes =
            quota_free_inodes;
        saw_request = true;
        return true;
    }

    std::uint64_t filesystem_free_bytes = 800'000U;
    std::uint64_t quota_free_bytes = 700'000U;
    std::uint64_t filesystem_free_inodes = 300U;
    std::uint64_t quota_free_inodes = 200U;
    bool saw_request = false;
};

class TempDirectory final {
public:
    TempDirectory() {
        char pattern[] =
            "/tmp/l2flow-finalization-store-XXXXXX";
        char* const created = ::mkdtemp(pattern);
        if (created == nullptr) {
            return;
        }
        path_ = created;
        fd_ = ::open(
            path_.c_str(),
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_CLOEXEC);
    }

    ~TempDirectory() {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
        if (!path_.empty()) {
            std::error_code ignored;
            static_cast<void>(
                std::filesystem::remove_all(
                    path_, ignored));
        }
    }

    TempDirectory(const TempDirectory&) = delete;
    TempDirectory& operator=(const TempDirectory&) =
        delete;

    [[nodiscard]] bool valid() const noexcept {
        return fd_ >= 0;
    }
    [[nodiscard]] int fd() const noexcept {
        return fd_;
    }

private:
    std::string path_;
    int fd_ = -1;
};

template <std::size_t Size>
std::array<std::byte, Size> Pattern(
    std::uint8_t first) {
    std::array<std::byte, Size> value{};
    for (std::size_t index = 0U;
         index < Size;
         ++index) {
        value[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                first +
                static_cast<std::uint8_t>(
                    index)));
    }
    return value;
}

void AppendU16Le(
    std::vector<std::byte>* output,
    std::uint16_t value) {
    output->push_back(
        static_cast<std::byte>(value & 0xffU));
    output->push_back(
        static_cast<std::byte>(
            (value >> 8U) & 0xffU));
}

void AppendU32Le(
    std::vector<std::byte>* output,
    std::uint32_t value) {
    for (unsigned shift = 0U;
         shift < 32U;
         shift += 8U) {
        output->push_back(
            static_cast<std::byte>(
                (value >> shift) & 0xffU));
    }
}

void AppendU64Le(
    std::vector<std::byte>* output,
    std::uint64_t value) {
    for (unsigned shift = 0U;
         shift < 64U;
         shift += 8U) {
        output->push_back(
            static_cast<std::byte>(
                (value >> shift) & 0xffU));
    }
}

template <std::size_t Size>
void AppendBytes(
    std::vector<std::byte>* output,
    const std::array<std::byte, Size>& value) {
    output->insert(
        output->end(), value.begin(), value.end());
}

ingress::ReserveStateV1Digest AbsentReportPlanHash(
    std::uint32_t source_stream_id,
    std::uint32_t capture_date,
    const ingress::ReserveStateV1Identity&
        stream_day_id,
    const ingress::ReserveStateV1Identity& cycle,
    std::uint16_t action_id,
    const ingress::FinalizationActionPlanV1& plan) {
    constexpr std::string_view domain =
        "L2FLOW_FINALIZATION_ACTION_PLAN_V1";
    std::vector<std::byte> encoded;
    encoded.insert(
        encoded.end(),
        reinterpret_cast<const std::byte*>(
            domain.data()),
        reinterpret_cast<const std::byte*>(
            domain.data() + domain.size()));
    encoded.push_back(std::byte{0});
    AppendU32Le(&encoded, source_stream_id);
    AppendU32Le(&encoded, capture_date);
    AppendBytes(&encoded, stream_day_id);
    AppendBytes(&encoded, cycle);
    AppendU16Le(&encoded, action_id);
    encoded.push_back(
        static_cast<std::byte>(plan.plan_version));
    encoded.push_back(
        static_cast<std::byte>(
            static_cast<std::uint8_t>(
                plan.object_type)));
    AppendU16Le(&encoded, plan.plan_flags);
    AppendU32Le(&encoded, plan.object_sequence);
    AppendU64Le(&encoded, plan.range_start);
    AppendU64Le(
        &encoded, plan.range_end_or_size);
    AppendBytes(&encoded, plan.causal_id);
    encoded.push_back(std::byte{0});
    return l2flow::common::ComputeSha256(
        std::span<const std::byte>(
            encoded.data(), encoded.size()));
}

bool MakeDirectoryAt(
    int parent_fd,
    const std::string& name,
    int* output_fd = nullptr) {
    if (::mkdirat(parent_fd, name.c_str(), 0700) != 0) {
        return false;
    }
    const int fd = ::openat(
        parent_fd,
        name.c_str(),
        O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
            O_CLOEXEC);
    if (fd < 0 || ::fsync(parent_fd) != 0) {
        if (fd >= 0) {
            static_cast<void>(::close(fd));
        }
        return false;
    }
    if (output_fd == nullptr) {
        static_cast<void>(::close(fd));
    } else {
        *output_fd = fd;
    }
    return true;
}

ingress::ReserveCoordinatorStateV1 MakeBootstrap(
    int root_fd,
    std::uint8_t uuid_seed) {
    struct stat root {};
    static_cast<void>(::fstat(root_fd, &root));
    ingress::ReserveCoordinatorStateV1 state{};
    state.header.reserve_state_uuid =
        Pattern<16U>(uuid_seed);
    state.header.quota_identity_sha256 =
        Pattern<32U>(0x20U);
    state.header.mount_identity_sha256 =
        Pattern<32U>(0x40U);
    state.header.device_id =
        static_cast<std::uint64_t>(root.st_dev);
    state.header.declared_releasable_bytes =
        1U << 20U;
    state.header.allocation_quantum_bytes = 4096U;
    state.header.declared_inode_reserve_count = 8U;
    state.header.byte_probe_version = 1U;
    state.header.inode_probe_version = 1U;
    state.header.inode_inventory_sha256 =
        Pattern<32U>(0x60U);
    state.header.safe_stop_catalog_sha256 =
        Pattern<32U>(0x80U);

    ingress::ReserveStateEntryV1 registry{};
    registry.source_stream_id = 1001U;
    registry.capture_date = 20260719U;
    registry.stream_day_id =
        Pattern<16U>(0xa0U);
    registry.registry_status =
        ingress::ReserveRegistryStatusV1::kActive;
    registry.recovery_origin =
        ingress::ReserveRecoveryOriginV1::kFreshInit;
    registry.recovery_intent =
        ingress::ReserveRecoveryIntentV1::
            kResumeConnect;
    registry.writer_instance =
        Pattern<16U>(0xc0U);
    registry.executor_or_recovery_attempt =
        Pattern<16U>(0xd0U);
    registry.safe_stop_template_id = 17U;

    ingress::ReserveStateSlotV1 slot{};
    slot.coordinator_state =
        ingress::ReserveCoordinatorPhaseV1::
            kProvisioned;
    slot.generation = 1U;
    slot.reserve_state_uuid =
        state.header.reserve_state_uuid;
    slot.entry_count = 1U;
    slot.entries[0U] = registry;
    state.slots[0U] = slot;
    state.slots[1U] = slot;
    return state;
}

void SetAction(
    ingress::ReserveStateEntryV1* entry,
    std::size_t index,
    ingress::FinalizationActionKindV1 kind,
    std::uint32_t byte_quanta,
    std::uint32_t inode_cap,
    const ingress::ReserveStateV1Identity& cycle) {
    auto& receipt = entry->actions[index];
    receipt.action_id =
        static_cast<std::uint16_t>(index);
    receipt.action_kind = kind;
    receipt.action_state =
        ingress::FinalizationActionStateV1::kPending;
    receipt.byte_cap_quanta = byte_quanta;
    receipt.inode_cap = inode_cap;
    receipt.object_plan_sha256 =
        Pattern<32U>(
            static_cast<std::uint8_t>(
                0x10U + index));
    auto& plan = entry->plans[index];
    plan.plan_version = 1U;
    plan.object_type = kind;
    plan.object_sequence =
        static_cast<std::uint32_t>(index + 1U);
    plan.range_start =
        static_cast<std::uint64_t>(index);
    plan.range_end_or_size =
        static_cast<std::uint64_t>(index + 1U);
    plan.causal_id = cycle;
}

void SetReportAction(
    ingress::ReserveStateEntryV1* entry,
    std::size_t index,
    const ingress::ReserveStateV1Identity& cycle,
    std::size_t maximum_bytes) {
    SetAction(
        entry,
        index,
        ingress::FinalizationActionKindV1::
            kFinalizationReport,
        16U,
        1U,
        cycle);
    auto& plan = entry->plans[index];
    plan.object_sequence = 0U;
    plan.range_start = 0U;
    plan.range_end_or_size = maximum_bytes;
    entry->actions[index].object_plan_sha256 =
        AbsentReportPlanHash(
            entry->source_stream_id,
            entry->capture_date,
            entry->stream_day_id,
            cycle,
            static_cast<std::uint16_t>(index),
            plan);
}

ingress::ReserveStateEntryV1 MakeAnchorGrant(
    const ingress::ReserveStateEntryV1& registry,
    const ingress::ReserveStateV1Identity& cycle,
    const ingress::RawV1Digest& journal_sha256,
    std::uint64_t quantum) {
    ingress::ReserveStateEntryV1 entry{};
    entry.source_stream_id = registry.source_stream_id;
    entry.capture_date = registry.capture_date;
    entry.stream_day_id = registry.stream_day_id;
    entry.ack_status =
        ingress::ReserveAckStatusV1::kAcked;
    entry.writer_instance = registry.writer_instance;
    entry.grant_status =
        ingress::ReserveGrantStatusV1::kPending;
    entry.grant_flags =
        ingress::kReserveGrantRawAnchorOnly;
    entry.anchor_only_payload.recovery_attempt_id =
        Pattern<16U>(0x31U);
    entry.anchor_only_payload.journal_header_sha256 =
        journal_sha256;
    entry.anchor_only_payload.origin_registry_status =
        ingress::ReserveRegistryStatusV1::kInit;
    entry.anchor_only_payload.recovery_origin =
        ingress::ReserveRecoveryOriginV1::kFreshInit;
    entry.anchor_only_payload.original_recovery_intent =
        ingress::ReserveRecoveryIntentV1::
            kResumeConnect;
    entry.anchor_only_payload.finalization_intent =
        ingress::ReserveRecoveryIntentV1::
            kRecoverSealOnly;
    entry.anchor_only_payload.required_action_bitmap =
        0x07U;
    entry.safe_stop_template_id =
        registry.safe_stop_template_id;
    SetAction(
        &entry,
        0U,
        ingress::FinalizationActionKindV1::
            kJournalAnchor,
        1U,
        1U,
        cycle);
    SetAction(
        &entry,
        1U,
        ingress::FinalizationActionKindV1::
            kEmptyAnchorTombstone,
        1U,
        1U,
        cycle);
    SetReportAction(
        &entry,
        2U,
        cycle,
        ingress::kFinalizationReportV1MaximumBytes);
    entry.grant_bytes = 18U * quantum;
    return entry;
}

ingress::ReserveStateEntryV1 MakeScaffoldingGrant(
    const ingress::ReserveStateEntryV1& registry,
    const ingress::ReserveStateV1Identity& cycle,
    const ingress::ReserveStateV1Identity&
        recovery_attempt,
    const ingress::RawV1Digest& snapshot,
    std::uint64_t quantum) {
    ingress::ReserveStateEntryV1 entry{};
    entry.source_stream_id = registry.source_stream_id;
    entry.capture_date = registry.capture_date;
    entry.stream_day_id = registry.stream_day_id;
    entry.ack_status =
        ingress::ReserveAckStatusV1::kAcked;
    entry.writer_instance = registry.writer_instance;
    entry.grant_status =
        ingress::ReserveGrantStatusV1::kPending;
    entry.grant_flags =
        ingress::kReserveGrantScaffoldingOnly;
    entry.scaffolding_payload.recovery_attempt_id =
        recovery_attempt;
    entry.scaffolding_payload.object_snapshot_sha256 =
        snapshot;
    entry.scaffolding_payload.observed_object_bitmap =
        0U;
    entry.scaffolding_payload.required_action_bitmap =
        0x7fU;
    entry.safe_stop_template_id =
        registry.safe_stop_template_id;
    SetAction(
        &entry,
        0U,
        ingress::FinalizationActionKindV1::
            kDirectoryLeaseScaffold,
        1U,
        1U,
        cycle);
    SetAction(
        &entry,
        1U,
        ingress::FinalizationActionKindV1::
            kJournalAnchor,
        1U,
        1U,
        cycle);
    SetAction(
        &entry,
        2U,
        ingress::FinalizationActionKindV1::
            kEmptyAnchorTombstone,
        1U,
        1U,
        cycle);
    SetReportAction(
        &entry,
        3U,
        cycle,
        ingress::
            kScaffoldingFinalizationReportV1MaximumBytes);
    entry.grant_bytes = 19U * quantum;
    return entry;
}

struct CoordinatorFixture final {
    std::shared_ptr<
        ingress::RawReserveRegistryCoordinatorV1>
        coordinator;
    ingress::ReserveFinalizationGrantKeyV1 grant_key{};
    ingress::ReserveFinalizationActionKeyV1 report_key{};
};

CoordinatorFixture BuildConsumedCoordinator(
    TestContext* test,
    int root_fd,
    const ingress::ReserveCoordinatorStateV1& bootstrap,
    const ingress::ReserveStateEntryV1& grant,
    const ingress::ReserveStateV1Identity& cycle) {
    CoordinatorFixture fixture{};
    ingress::RawReserveStatePosixError posix_error{};
    ingress::ReserveStateV1Error codec_error{};
    std::string error;
    auto state_file =
        ingress::PublishFreshRawReserveStateAtV1(
            root_fd,
            bootstrap,
            &posix_error,
            &codec_error,
            &error);
    test->Expect(
        state_file != nullptr,
        "bootstrap state publishes: " + error);
    if (state_file == nullptr) {
        return fixture;
    }

    const auto& provisioned_state =
        state_file->state();
    const auto& provisioned =
        provisioned_state.slots[
            provisioned_state.selected_slot];
    ingress::ReserveReleaseIntentV1 intent_request{};
    intent_request.reason =
        ingress::ReserveReleaseReasonV1::kLowWatermark;
    intent_request.trigger =
        ingress::ReserveReleaseTriggerV1::
            kFilesystemBytes;
    intent_request.writer_set_sha256 =
        Pattern<32U>(0x51U);
    ingress::ReserveStateSlotV1 intent{};
    bool built =
        ingress::BuildReserveReleasingIntentV1(
            bootstrap.header,
            provisioned,
            intent_request,
            &intent) ==
        ingress::ReserveStateV1Error::kNone;
    test->Expect(built, "INTENT state builds");
    if (!built ||
        state_file->PublishNext(
            intent, &codec_error, &error) !=
            ingress::RawReserveStatePosixError::kNone) {
        test->Expect(false, "INTENT state publishes: " + error);
        return fixture;
    }

    ingress::ReserveReleasePreparedV1 prepared_request{};
    prepared_request.finalization_cycle_id = cycle;
    prepared_request.grant_entries =
        std::span<const ingress::ReserveStateEntryV1>(
            &grant, 1U);
    prepared_request.pre_release_fs_free_bytes =
        1'000'000U;
    prepared_request.pre_release_quota_free_bytes =
        900'000U;
    prepared_request.pre_release_fs_free_inodes = 500U;
    prepared_request.pre_release_quota_free_inodes = 400U;
    prepared_request.expected_release_fs_bytes =
        bootstrap.header.declared_releasable_bytes;
    prepared_request.expected_release_quota_bytes =
        bootstrap.header.declared_releasable_bytes;
    prepared_request.expected_release_fs_inodes =
        bootstrap.header.declared_inode_reserve_count + 1U;
    prepared_request.expected_release_quota_inodes =
        bootstrap.header.declared_inode_reserve_count + 1U;
    prepared_request.reserved_margin_bytes =
        bootstrap.header.allocation_quantum_bytes;
    prepared_request.reserved_margin_inodes = 1U;
    prepared_request.effective_min_fs_free_bytes =
        bootstrap.header.declared_releasable_bytes;
    prepared_request.effective_min_quota_free_bytes =
        bootstrap.header.declared_releasable_bytes;
    prepared_request.effective_min_fs_free_inodes =
        bootstrap.header.declared_inode_reserve_count;
    prepared_request.effective_min_quota_free_inodes =
        bootstrap.header.declared_inode_reserve_count;
    ingress::ReserveStateSlotV1 prepared{};
    const ingress::ReserveStateV1Error prepared_error =
        ingress::BuildReserveReleasingPreparedV1(
            bootstrap.header,
            state_file->state().slots[
                state_file->state().selected_slot],
            prepared_request,
            &prepared);
    built =
        prepared_error ==
        ingress::ReserveStateV1Error::kNone;
    test->Expect(
        built,
        "PREPARED state builds: " +
            std::string(
                ingress::ReserveStateV1ErrorName(
                    prepared_error)));
    if (!built ||
        state_file->PublishNext(
            prepared, &codec_error, &error) !=
            ingress::RawReserveStatePosixError::kNone) {
        test->Expect(false, "PREPARED state publishes: " + error);
        return fixture;
    }
    ingress::ReserveStateSlotV1 consumed{};
    built = ingress::BuildReserveConsumedV1(
                bootstrap.header,
                state_file->state().slots[
                    state_file->state().selected_slot],
                &consumed) ==
            ingress::ReserveStateV1Error::kNone;
    test->Expect(built, "CONSUMED state builds");
    if (!built ||
        state_file->PublishNext(
            consumed, &codec_error, &error) !=
            ingress::RawReserveStatePosixError::kNone) {
        test->Expect(false, "CONSUMED state publishes: " + error);
        return fixture;
    }
    state_file.reset();

    ingress::RawReserveCoordinatorLeaseMarkerV1 marker{};
    marker.coordinator_identity =
        bootstrap.header.reserve_state_uuid;
    marker.device_id = bootstrap.header.device_id;
    marker.quota_identity_sha256 =
        bootstrap.header.quota_identity_sha256;
    marker.mount_identity_sha256 =
        bootstrap.header.mount_identity_sha256;
    ingress::RawReserveCoordinatorErrorV1 coordinator_error{};
    fixture.coordinator =
        ingress::AttachRawReserveRegistryCoordinatorAtV1(
            root_fd,
            marker,
            &coordinator_error,
            &error);
    test->Expect(
        fixture.coordinator != nullptr,
        "coordinator attaches exact CONSUMED state: " +
            error);
    if (fixture.coordinator == nullptr) {
        return fixture;
    }

    fixture.grant_key.finalization_cycle_id = cycle;
    fixture.grant_key.source_stream_id =
        grant.source_stream_id;
    fixture.grant_key.capture_date =
        grant.capture_date;
    fixture.grant_key.stream_day_id =
        grant.stream_day_id;
    fixture.grant_key.ack_status =
        grant.ack_status;
    fixture.grant_key.ack_writer_instance =
        grant.writer_instance;
    fixture.grant_key.safe_stop_template_id =
        grant.safe_stop_template_id;
    ingress::ReserveGrantActivationV1 activation{};
    activation.grant = fixture.grant_key;
    activation.executor_instance =
        grant.ack_status ==
                ingress::ReserveAckStatusV1::kAcked
            ? grant.writer_instance
            : Pattern<16U>(0xe0U);
    activation.filesystem_free_byte_baseline =
        800'000U;
    activation.quota_free_byte_baseline =
        700'000U;
    activation.filesystem_free_inode_baseline = 300U;
    activation.quota_free_inode_baseline = 200U;
    test->Expect(
        fixture.coordinator
                ->ActivateNextFinalizationGrant(
                    activation, &error) ==
            ingress::RawReserveCoordinatorErrorV1::kNone,
        "grant activates: " + error);

    const std::size_t report_index =
        [&]() {
            std::size_t count = 0U;
            while (count <
                       ingress::
                           kReserveStateV1ActionCapacity &&
                   grant.actions[count].action_kind !=
                       ingress::FinalizationActionKindV1::
                           kFinalizationReport) {
                ++count;
            }
            return count;
        }();
    for (std::size_t index = 0U;
         index < report_index;
         ++index) {
        ingress::ReserveFinalizationActionKeyV1 key{};
        key.grant = fixture.grant_key;
        key.action_id =
            static_cast<std::uint16_t>(index);
        key.action_kind =
            grant.actions[index].action_kind;
        key.object_plan_sha256 =
            grant.actions[index].object_plan_sha256;
        test->Expect(
            fixture.coordinator
                    ->DebitFinalizationAction(
                        key, &error) ==
                ingress::RawReserveCoordinatorErrorV1::
                    kNone,
            "predecessor action debits");
        test->Expect(
            fixture.coordinator
                    ->CompleteFinalizationAction(
                        key, &error) ==
                ingress::RawReserveCoordinatorErrorV1::
                    kNone,
            "predecessor action completes");
    }
    fixture.report_key.grant = fixture.grant_key;
    fixture.report_key.action_id =
        static_cast<std::uint16_t>(report_index);
    fixture.report_key.action_kind =
        grant.actions[report_index].action_kind;
    fixture.report_key.object_plan_sha256 =
        grant.actions[report_index].object_plan_sha256;
    test->Expect(
        fixture.coordinator->DebitFinalizationAction(
            fixture.report_key, &error) ==
            ingress::RawReserveCoordinatorErrorV1::kNone,
        "terminal report action debits");
    return fixture;
}

std::unique_ptr<ingress::BuiltEmptyAnchorTombstoneV1>
MakeTombstone(
    TestContext* test,
    std::uint32_t source_stream_id,
    std::uint32_t capture_date,
    const ingress::RawV1Identity& stream_day_id) {
    ingress::DurableJournalHeaderV1 header{};
    header.capture_date = capture_date;
    header.source_stream_id = source_stream_id;
    header.stream_day_id = stream_day_id;
    header.raw_schema_sha256 =
        ingress::kFrozenRawSchemaSha256;
    header.created_host_uuid =
        Pattern<16U>(0x11U);
    header.created_linux_boot_id =
        Pattern<16U>(0x21U);
    header.created_clock_epoch_algorithm = 1U;
    header.created_clock_epoch_digest =
        Pattern<32U>(0x31U);
    header.created_clock_epoch_label = 1U;
    ingress::EmptyAnchorObservationV1 observation{};
    static_cast<void>(
        ingress::EncodeDurableJournalHeaderV1(
            header,
            &observation.journal_header_bytes));
    observation.journal_logical_size =
        ingress::kRawV1JournalHeaderBytes;
    std::unique_ptr<
        ingress::BuiltEmptyAnchorTombstoneV1>
        tombstone;
    test->Expect(
        ingress::BuildEmptyAnchorTombstoneCapabilityV1(
            observation, &tombstone) ==
                ingress::EmptyAnchorTombstoneV1Error::kNone &&
            tombstone != nullptr,
        "empty-anchor capability builds");
    return tombstone;
}

struct InterruptHook final {
    ingress::FinalizationReportStoreMutationPointV1
        point =
            ingress::
                FinalizationReportStoreMutationPointV1::
                    kBeforePublishRename;
    bool fired = false;
};

bool InterruptOnce(
    ingress::FinalizationReportStoreMutationPointV1 point,
    void* context) noexcept {
    auto* const hook =
        static_cast<InterruptHook*>(context);
    if (hook != nullptr && !hook->fired &&
        point == hook->point) {
        hook->fired = true;
        return false;
    }
    return true;
}

std::unique_ptr<ingress::RawReserveFinalizationActionV1>
AcquireRouted(
    CoordinatorFixture& fixture,
    TestContext* test) {
    ingress::RawReserveCoordinatorErrorV1 failure{};
    std::string error;
    auto action =
        fixture.coordinator
            ->AcquireDebitedFinalizationAction(
                fixture.report_key,
                "sh-snapshot",
                &failure,
                &error);
    test->Expect(
        action != nullptr,
        "routed DEBITED action reacquires: " + error);
    return action;
}

std::unique_ptr<ingress::RawReserveFinalizationActionV1>
AcquireScaffolding(
    CoordinatorFixture& fixture,
    TestContext* test) {
    ingress::RawReserveCoordinatorErrorV1 failure{};
    std::string error;
    auto action =
        fixture.coordinator
            ->AcquireDebitedScaffoldingAction(
                fixture.report_key,
                &failure,
                &error);
    test->Expect(
        action != nullptr,
        "scaffolding DEBITED action reacquires: " +
            error);
    return action;
}

bool WriteFileAt(
    int parent_fd,
    const std::string& name,
    std::string_view bytes) {
    const int fd = ::openat(
        parent_fd,
        name.c_str(),
        O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW |
            O_CLOEXEC,
        0600);
    if (fd < 0) {
        return false;
    }
    const ssize_t written = ::pwrite(
        fd, bytes.data(), bytes.size(), 0);
    const bool ok =
        written == static_cast<ssize_t>(bytes.size()) &&
        ::fsync(fd) == 0 &&
        ::fsync(parent_fd) == 0;
    static_cast<void>(::close(fd));
    return ok;
}

void CheckRoutedStore(TestContext* test) {
    TempDirectory root;
    test->Expect(root.valid(), "routed temp root opens");
    if (!root.valid()) {
        return;
    }
    auto bootstrap =
        MakeBootstrap(root.fd(), 0x41U);
    for (auto& slot : bootstrap.slots) {
        auto& registry = slot.entries[0U];
        registry.registry_status =
            ingress::ReserveRegistryStatusV1::kInit;
        registry.recovery_origin =
            ingress::ReserveRecoveryOriginV1::kFreshInit;
        registry.recovery_intent =
            ingress::ReserveRecoveryIntentV1::
                kResumeConnect;
        registry.executor_or_recovery_attempt =
            Pattern<16U>(0x31U);
    }
    auto tombstone = MakeTombstone(
        test,
        bootstrap.slots[0U]
            .entries[0U].source_stream_id,
        bootstrap.slots[0U]
            .entries[0U].capture_date,
        bootstrap.slots[0U]
            .entries[0U].stream_day_id);
    if (tombstone == nullptr) {
        return;
    }
    const auto cycle = Pattern<16U>(0x61U);
    const auto grant = MakeAnchorGrant(
        bootstrap.slots[0U].entries[0U],
        cycle,
        tombstone->model().journal_header_sha256,
        bootstrap.header.allocation_quantum_bytes);
    CoordinatorFixture fixture =
        BuildConsumedCoordinator(
            test, root.fd(), bootstrap, grant, cycle);
    if (fixture.coordinator == nullptr) {
        return;
    }

    int date_fd = -1;
    int route_fd = -1;
    int maintenance_fd = -1;
    const std::string date_name =
        "capture_date=" +
        std::to_string(grant.capture_date);
    const std::string route_name =
        "stream=" +
        std::to_string(grant.source_stream_id) +
        "-sh-snapshot";
    bool directories =
        MakeDirectoryAt(
            root.fd(), date_name, &date_fd) &&
        MakeDirectoryAt(
            date_fd, route_name, &route_fd) &&
        MakeDirectoryAt(
            route_fd, "maintenance", &maintenance_fd);
    test->Expect(
        directories,
        "routed maintenance chain is pre-created");
    if (!directories) {
        if (maintenance_fd >= 0) {
            static_cast<void>(::close(maintenance_fd));
        }
        if (route_fd >= 0) {
            static_cast<void>(::close(route_fd));
        }
        if (date_fd >= 0) {
            static_cast<void>(::close(date_fd));
        }
        return;
    }

    auto action = AcquireRouted(fixture, test);
    if (action == nullptr) {
        return;
    }
    ingress::FinalizationReportV1 model{};
    model.reserve_state_uuid =
        action->generation_token().reserve_state_uuid;
    model.finalization_cycle_id = cycle;
    model.namespace_identity =
        ingress::RawManifestNamespaceV1{
        grant.capture_date,
        grant.source_stream_id,
        grant.stream_day_id};
    model.ack_status =
        ingress::ReserveAckStatusV1::kAcked;
    model.ack_writer_instance =
        grant.writer_instance;
    model.immutable_grant_sha256 =
        action->immutable_grant_sha256();
    model.tail_classification =
        ingress::FinalizationReportTailClassificationV1::
            kNone;
    model.tail_classification_valid = true;
    model.gap_classification =
        ingress::FinalizationReportGapClassificationV1::
            kNone;
    model.gap_classification_valid = true;
    model.result =
        ingress::FinalizationReportResultV1::
            kEmptyAnchorOnly;
    model.journal_header_sha256 =
        tombstone->model().journal_header_sha256;
    model.marker_count = 0U;
    model.empty_anchor_tombstone_sha256 =
        tombstone->tombstone_sha256();
    std::unique_ptr<
        ingress::BuiltFinalizationReportV1>
        built;
    test->Expect(
        ingress::BuildFinalizationReportCapabilityV1(
            model,
            ingress::kReserveGrantRawAnchorOnly,
            nullptr,
            tombstone.get(),
            &built) ==
                ingress::FinalizationReportV1Error::kNone &&
            built != nullptr,
        "routed report capability binds exact grant");
    if (built == nullptr) {
        return;
    }

    InterruptHook interruption{};
    const ingress::FinalizationReportStoreHooksV1 hooks{
        &InterruptOnce, &interruption};
    std::string diagnostic;
    auto interrupted =
        ingress::PublishFinalizationReportV1(
            std::move(action),
            *built,
            &hooks,
            &diagnostic);
    test->Expect(
        interrupted.error ==
                ingress::FinalizationReportStoreErrorV1::
                    kInjectedInterruption &&
            interruption.fired &&
            interrupted.receipt == nullptr,
        "fault before NOREPLACE leaves resumable complete tmp");

    action = AcquireRouted(fixture, test);
    auto adopted = ingress::PublishFinalizationReportV1(
        std::move(action),
        *built,
        nullptr,
        &diagnostic);
    test->Expect(
        adopted.ok() &&
            adopted.disposition ==
                ingress::
                    FinalizationReportStoreDispositionV1::
                        kAdoptedCompleteTemporary &&
            adopted.receipt->Validate(&diagnostic),
        "restart adopts complete tmp and returns valid receipt");
    const auto state_after_store =
        fixture.coordinator->state();
    const auto& selected =
        state_after_store.slots[
            state_after_store.selected_slot];
    test->Expect(
        selected.entries[0U]
                .actions[fixture.report_key.action_id]
                .action_state ==
            ingress::FinalizationActionStateV1::kDebited,
        "publisher does not mutate coordinator receipt state");

    FixedFinalizationCapacityProbe insufficient_capacity;
    insufficient_capacity.filesystem_free_bytes = 0U;
    insufficient_capacity.quota_free_bytes = 0U;
    insufficient_capacity.filesystem_free_inodes = 0U;
    insufficient_capacity.quota_free_inodes = 0U;
    std::string rejected_completion_error;
    test->Expect(
        fixture.coordinator
                ->CompleteFinalizationReport(
                    std::move(adopted.receipt),
                    &insufficient_capacity,
                    &rejected_completion_error) ==
            ingress::RawReserveCoordinatorErrorV1::
                kCapacityProbeFailure,
        "post-publication capacity drop beyond the immutable grant rejects DONE");
    const auto state_after_capacity_rejection =
        fixture.coordinator->state();
    const auto& capacity_rejected_slot =
        state_after_capacity_rejection.slots[
            state_after_capacity_rejection.selected_slot];
    test->Expect(
        capacity_rejected_slot.entries[0U]
                .grant_status ==
            ingress::ReserveGrantStatusV1::kActive &&
            capacity_rejected_slot.entries[0U]
                    .actions[
                        fixture.report_key.action_id]
                    .action_state ==
                ingress::
                    FinalizationActionStateV1::kDebited,
        "failed capacity proof leaves durable report receipt DEBITED");

    action = AcquireRouted(fixture, test);
    auto replay_candidate =
        ingress::PublishFinalizationReportV1(
            std::move(action), *built);
    test->Expect(
        replay_candidate.ok() &&
            replay_candidate.receipt->Validate(),
        "a fresh exact receipt is available for replay rejection");

    const std::string temporary_name =
        "." + std::string(built->filename()) +
        std::string(
            ingress::
                kFinalizationReportV1TemporarySuffix);
    test->Expect(
        WriteFileAt(
            maintenance_fd,
            temporary_name,
            built->canonical_jcs()),
        "identical tmp is injected beside final");
    action = AcquireRouted(fixture, test);
    auto cleaned = ingress::PublishFinalizationReportV1(
        std::move(action), *built);
    test->Expect(
        cleaned.ok() &&
            cleaned.disposition ==
                ingress::
                    FinalizationReportStoreDispositionV1::
                        kAcceptedExistingFinalAndRemovedIdenticalTemporary &&
            cleaned.receipt->Validate(),
        "DEBITED action cleans exact final+tmp pair and dirsyncs");

    test->Expect(
        WriteFileAt(
            maintenance_fd,
            temporary_name,
            "partial"),
        "conflicting partial tmp is injected");
    action = AcquireRouted(fixture, test);
    auto conflict =
        ingress::PublishFinalizationReportV1(
            std::move(action), *built);
    test->Expect(
        conflict.error ==
                ingress::FinalizationReportStoreErrorV1::
                    kCandidateConflict &&
            conflict.receipt == nullptr,
        "partial/conflicting tmp fails closed without overwrite");

    std::string transition_error;
    test->Expect(
        ::unlinkat(
            maintenance_fd,
            temporary_name.c_str(),
            0) == 0 &&
            ::fsync(maintenance_fd) == 0,
        "test removes injected conflict after fail-closed proof");
    FixedFinalizationCapacityProbe capacity_probe;
    test->Expect(
        fixture.coordinator
                ->CompleteFinalizationReport(
                    std::move(cleaned.receipt),
                    &capacity_probe,
                    &transition_error) ==
            ingress::RawReserveCoordinatorErrorV1::kNone,
        "typed routed receipt atomically completes report and grant: " +
            transition_error);
    const auto completed_state =
        fixture.coordinator->state();
    const auto& completed_slot =
        completed_state.slots[
            completed_state.selected_slot];
    test->Expect(
        capacity_probe.saw_request &&
            completed_slot.entries[0U].grant_status ==
                ingress::ReserveGrantStatusV1::kDone &&
            completed_slot.entries[0U]
                    .actions[
                        fixture.report_key.action_id]
                    .action_state ==
                ingress::
                    FinalizationActionStateV1::
                        kComplete &&
            completed_slot.entries[0U]
                    .maintenance_report_sha256 ==
                built->report_sha256(),
        "routed COMPLETE + report hash + DONE share one durable slot");

    transition_error.clear();
    const auto stale_completion =
        fixture.coordinator
            ->CompleteFinalizationReport(
                std::move(replay_candidate.receipt),
                &capacity_probe,
                &transition_error);
    test->Expect(
        stale_completion ==
            ingress::RawReserveCoordinatorErrorV1::
                kActionGenerationChanged,
        "a second routed receipt cannot replay DONE");
    ingress::RawReserveCoordinatorErrorV1 stale_failure{};
    auto stale =
        fixture.coordinator
            ->AcquireDebitedFinalizationAction(
                fixture.report_key,
                "sh-snapshot",
                &stale_failure,
                &transition_error);
    test->Expect(
        stale == nullptr,
        "COMPLETE/stale report action cannot be reissued");

    static_cast<void>(::close(maintenance_fd));
    static_cast<void>(::close(route_fd));
    static_cast<void>(::close(date_fd));
}

ingress::ScaffoldingObjectPostStateV1 PostState(
    ingress::ScaffoldingObjectRoleV1 role) {
    switch (role) {
    case ingress::ScaffoldingObjectRoleV1::
        kCaptureDirectory:
    case ingress::ScaffoldingObjectRoleV1::
        kStreamDirectory:
    case ingress::ScaffoldingObjectRoleV1::
        kMaintenanceDirectory:
        return ingress::
            ScaffoldingObjectPostStateV1::
                kValidDirectory;
    case ingress::ScaffoldingObjectRoleV1::
        kWriterLeaseTemporary:
    case ingress::ScaffoldingObjectRoleV1::
        kJournalTemporary:
        return ingress::
            ScaffoldingObjectPostStateV1::kAbsent;
    case ingress::ScaffoldingObjectRoleV1::
        kWriterLeaseFinal:
        return ingress::
            ScaffoldingObjectPostStateV1::kValidFile;
    case ingress::ScaffoldingObjectRoleV1::
        kJournalFinal:
        return ingress::
            ScaffoldingObjectPostStateV1::
                kHeaderOnlyJournal;
    }
    return ingress::ScaffoldingObjectPostStateV1::kAbsent;
}

void CheckScaffoldingStore(TestContext* test) {
    TempDirectory root;
    test->Expect(
        root.valid(), "scaffolding temp root opens");
    if (!root.valid()) {
        return;
    }
    auto bootstrap =
        MakeBootstrap(root.fd(), 0x71U);
    auto tombstone = MakeTombstone(
        test,
        bootstrap.slots[0U]
            .entries[0U].source_stream_id,
        bootstrap.slots[0U]
            .entries[0U].capture_date,
        bootstrap.slots[0U]
            .entries[0U].stream_day_id);
    if (tombstone == nullptr) {
        return;
    }
    const auto cycle = Pattern<16U>(0x81U);
    const auto recovery_attempt =
        Pattern<16U>(0x91U);
    const auto snapshot = Pattern<32U>(0xa1U);
    for (auto& slot : bootstrap.slots) {
        auto& registry = slot.entries[0U];
        registry.registry_status =
            ingress::ReserveRegistryStatusV1::
                kScaffolding;
        registry.recovery_origin =
            ingress::ReserveRecoveryOriginV1::kFreshInit;
        registry.recovery_intent =
            ingress::ReserveRecoveryIntentV1::
                kResumeConnect;
        registry.executor_or_recovery_attempt =
            recovery_attempt;
        registry.grant_bytes =
            19U *
            bootstrap.header.allocation_quantum_bytes;
    }
    const auto grant = MakeScaffoldingGrant(
        bootstrap.slots[0U].entries[0U],
        cycle,
        recovery_attempt,
        snapshot,
        bootstrap.header.allocation_quantum_bytes);
    CoordinatorFixture fixture =
        BuildConsumedCoordinator(
            test, root.fd(), bootstrap, grant, cycle);
    if (fixture.coordinator == nullptr) {
        return;
    }

    int audit_fd = -1;
    int reports_fd = -1;
    const bool directories =
        MakeDirectoryAt(
            root.fd(), "reserve-audit", &audit_fd) &&
        MakeDirectoryAt(
            audit_fd,
            "emergency-reports",
            &reports_fd);
    test->Expect(
        directories,
        "domain emergency-report chain is pre-created");
    if (!directories) {
        if (reports_fd >= 0) {
            static_cast<void>(::close(reports_fd));
        }
        if (audit_fd >= 0) {
            static_cast<void>(::close(audit_fd));
        }
        return;
    }

    auto action = AcquireScaffolding(fixture, test);
    if (action == nullptr) {
        return;
    }
    ingress::ScaffoldingFinalizationReportV1 model{};
    model.reserve_state_uuid =
        action->generation_token().reserve_state_uuid;
    model.finalization_cycle_id = cycle;
    model.planned_namespace =
        ingress::RawManifestNamespaceV1{
        grant.capture_date,
        grant.source_stream_id,
        grant.stream_day_id};
    model.planned_recovery_attempt_id =
        recovery_attempt;
    model.immutable_grant_sha256 =
        action->immutable_grant_sha256();
    model.object_snapshot_sha256 = snapshot;
    model.observed_object_bitmap = 0U;
    model.required_action_bitmap = 0x7fU;
    for (std::uint8_t wire = 1U;
         wire <= 7U;
         ++wire) {
        const auto role =
            static_cast<
                ingress::ScaffoldingObjectRoleV1>(
                wire);
        ingress::ScaffoldingObjectStateV1 object{};
        object.role = role;
        object.start_state =
            ingress::ScaffoldingObjectStartStateV1::
                kAbsent;
        object.post_state = PostState(role);
        object.barriers.object_synced =
            object.post_state !=
            ingress::ScaffoldingObjectPostStateV1::
                kAbsent;
        object.barriers.parent_directory_synced =
            true;
        object.barriers.retained_fd_revalidated =
            true;
        model.objects.push_back(object);
        ingress::ScaffoldingActionResultV1
            action_result{};
        action_result.action_id =
            static_cast<std::uint8_t>(wire - 1U);
        action_result.action_kind =
            ingress::ScaffoldingActionKindV1::
                kRevalidatePostState;
        action_result.object_role = role;
        action_result.post_state =
            object.post_state;
        action_result.completed = true;
        model.actions.push_back(action_result);
    }
    model.journal_header_sha256 =
        tombstone->model().journal_header_sha256;
    model.index_absent = true;
    model.manifest_absent = true;
    model.control_absent = true;
    model.empty_anchor_tombstone_sha256 =
        tombstone->tombstone_sha256();
    std::unique_ptr<
        ingress::BuiltScaffoldingFinalizationReportV1>
        built;
    test->Expect(
        ingress::
            BuildScaffoldingFinalizationReportCapabilityV1(
                model,
                ingress::kReserveGrantScaffoldingOnly,
                *tombstone,
                &built) ==
                ingress::
                    ScaffoldingFinalizationReportV1Error::
                        kNone &&
            built != nullptr,
        "scaffolding report capability binds exact grant");
    if (built == nullptr) {
        return;
    }

    InterruptHook interruption{};
    interruption.point =
        ingress::
            FinalizationReportStoreMutationPointV1::
                kBeforeParentSync;
    const ingress::FinalizationReportStoreHooksV1 hooks{
        &InterruptOnce, &interruption};
    auto interrupted =
        ingress::PublishScaffoldingFinalizationReportV1(
            std::move(action), *built, &hooks);
    test->Expect(
        interrupted.error ==
                ingress::FinalizationReportStoreErrorV1::
                    kInjectedInterruption &&
            interruption.fired,
        "fault after scaffolding rename preserves restart window");

    action = AcquireScaffolding(fixture, test);
    auto accepted =
        ingress::PublishScaffoldingFinalizationReportV1(
            std::move(action), *built);
    test->Expect(
        accepted.ok() &&
            accepted.disposition ==
                ingress::
                    FinalizationReportStoreDispositionV1::
                        kAcceptedExistingFinal &&
            accepted.receipt->Validate(),
        "restart re-syncs and accepts domain final");
    struct stat absent {};
    test->Expect(
        ::fstatat(
            root.fd(),
            "capture_date=20260719",
            &absent,
            AT_SYMLINK_NOFOLLOW) != 0 &&
            errno == ENOENT,
        "scaffolding report publisher creates no stream route");

    FixedFinalizationCapacityProbe capacity_probe;
    std::string completion_error;
    test->Expect(
        fixture.coordinator
                ->CompleteScaffoldingFinalizationReport(
                    std::move(accepted.receipt),
                    &capacity_probe,
                    &completion_error) ==
            ingress::RawReserveCoordinatorErrorV1::kNone,
        "typed scaffolding receipt atomically completes report and grant: " +
            completion_error);
    const auto completed_state =
        fixture.coordinator->state();
    const auto& completed_slot =
        completed_state.slots[
            completed_state.selected_slot];
    test->Expect(
        capacity_probe.saw_request &&
            completed_slot.entries[0U].grant_status ==
                ingress::ReserveGrantStatusV1::kDone &&
            completed_slot.entries[0U]
                    .maintenance_report_sha256 ==
                built->report_sha256(),
        "scaffolding COMPLETE + report hash + DONE share one durable slot");

    static_cast<void>(::close(reports_fd));
    static_cast<void>(::close(audit_fd));
}

}  // namespace

int main() {
    TestContext test;
    CheckRoutedStore(&test);
    CheckScaffoldingStore(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " finalization report store test(s) failed\n";
        return 1;
    }
    std::cout
        << "finalization report POSIX store tests passed\n";
    return 0;
}
