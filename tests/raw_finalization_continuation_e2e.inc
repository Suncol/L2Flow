class ContinuationTempDirectory final {
public:
    ContinuationTempDirectory() {
        char pattern[] =
            "/tmp/l2flow-finalization-continuation-XXXXXX";
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

    ~ContinuationTempDirectory() {
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

    ContinuationTempDirectory(
        const ContinuationTempDirectory&) = delete;
    ContinuationTempDirectory& operator=(
        const ContinuationTempDirectory&) = delete;

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

[[nodiscard]] bool ContinuationPwriteAll(
    int descriptor,
    std::uint64_t offset,
    std::span<const std::byte> bytes) {
    std::size_t completed = 0U;
    while (completed < bytes.size()) {
        const ssize_t count = ::pwrite(
            descriptor,
            bytes.data() + completed,
            bytes.size() - completed,
            static_cast<off_t>(offset + completed));
        if (count > 0) {
            completed +=
                static_cast<std::size_t>(count);
            continue;
        }
        if (count < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

[[nodiscard]] bool ContinuationReadAll(
    int descriptor,
    std::vector<std::byte>* bytes) {
    if (descriptor < 0 || bytes == nullptr) {
        return false;
    }
    struct stat status {};
    if (::fstat(descriptor, &status) != 0 ||
        status.st_size < 0) {
        return false;
    }
    try {
        bytes->resize(
            static_cast<std::size_t>(
                status.st_size));
    } catch (...) {
        return false;
    }
    std::size_t completed = 0U;
    while (completed < bytes->size()) {
        const ssize_t count = ::pread(
            descriptor,
            bytes->data() + completed,
            bytes->size() - completed,
            static_cast<off_t>(completed));
        if (count > 0) {
            completed +=
                static_cast<std::size_t>(count);
            continue;
        }
        if (count < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

[[nodiscard]] int ContinuationOpenFileAt(
    int directory_fd,
    const char* name) {
    return ::openat(
        directory_fd,
        name,
        O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW |
            O_NONBLOCK | O_CLOEXEC,
        0600);
}

[[nodiscard]] bool ContinuationMakeDirectoryAt(
    int parent_fd,
    const std::string& name,
    int* output) {
    if (output == nullptr ||
        ::mkdirat(parent_fd, name.c_str(), 0700) != 0) {
        return false;
    }
    const int descriptor = ::openat(
        parent_fd,
        name.c_str(),
        O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
            O_NONBLOCK | O_CLOEXEC);
    if (descriptor < 0 || ::fsync(parent_fd) != 0) {
        if (descriptor >= 0) {
            static_cast<void>(::close(descriptor));
        }
        return false;
    }
    *output = descriptor;
    return true;
}

[[nodiscard]] ingress::DurableMarkerV1
ContinuationMarker(
    const ingress::RawEmergencyWriterAckFactsV1& facts,
    const ingress::RawWalCursor& cursor,
    std::uint32_t flags) {
    ingress::DurableMarkerV1 marker{};
    marker.source_stream_id =
        facts.writer.source_stream_id;
    marker.segment_sequence =
        facts.writer.segment_sequence;
    marker.durable_global_wal_pos =
        cursor.global_wal_pos;
    marker.durable_ingress_sequence =
        cursor.ingress_sequence;
    marker.durable_segment_offset =
        cursor.segment_offset;
    marker.marker_flags = flags;
    return marker;
}

struct ContinuationMutationFault final {
    ingress::RawFinalizationContinuationMutationV1
        selected =
            ingress::
                RawFinalizationContinuationMutationV1::
                    kContinuationTemporaryCreate;
    bool enabled = false;
    bool fired = false;
    std::size_t matching_admissions_before_failure = 0U;

    static int Inject(
        void* context,
        ingress::RawFinalizationContinuationMutationV1
            mutation) noexcept {
        auto* const self =
            static_cast<ContinuationMutationFault*>(
                context);
        if (self != nullptr && self->enabled &&
            !self->fired &&
            mutation == self->selected) {
            if (self->matching_admissions_before_failure !=
                0U) {
                --self
                      ->matching_admissions_before_failure;
                return 0;
            }
            self->fired = true;
            return EIO;
        }
        return 0;
    }
};

[[nodiscard]] std::uint64_t
ContinuationFixedClock(void* context) noexcept {
    return context == nullptr
               ? 0U
               : *static_cast<std::uint64_t*>(
                     context);
}

enum class ContinuationCompletionProbeFault {
    kNone,
    kMissingFilesystemBytes,
    kMissingQuotaBytes,
    kMissingFilesystemInodes,
    kMissingQuotaInodes,
    kFilesystemBytesOverCap,
    kQuotaBytesOverCap,
    kFilesystemInodesOverCap,
    kQuotaInodesOverCap,
    kCorruptStateBeforePublish,
    kDropCoordinatorLeaseBeforeRootSync,
};

class ContinuationCapacityProbe final
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
                    "unexpected continuation capacity probe";
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
            UINT64_C(1) << 30U;
        observation->quota_free_bytes =
            UINT64_C(1) << 30U;
        observation->filesystem_free_inodes =
            UINT64_C(1) << 20U;
        observation->quota_free_inodes =
            UINT64_C(1) << 20U;
        switch (fault) {
            case ContinuationCompletionProbeFault::kNone:
                break;
            case ContinuationCompletionProbeFault::
                kMissingFilesystemBytes:
                observation->filesystem_bytes_proven =
                    false;
                break;
            case ContinuationCompletionProbeFault::
                kMissingQuotaBytes:
                observation->quota_bytes_proven = false;
                break;
            case ContinuationCompletionProbeFault::
                kMissingFilesystemInodes:
                observation->filesystem_inodes_proven =
                    false;
                break;
            case ContinuationCompletionProbeFault::
                kMissingQuotaInodes:
                observation->quota_inodes_proven = false;
                break;
            case ContinuationCompletionProbeFault::
                kFilesystemBytesOverCap:
                observation->filesystem_free_bytes = 0U;
                break;
            case ContinuationCompletionProbeFault::
                kQuotaBytesOverCap:
                observation->quota_free_bytes = 0U;
                break;
            case ContinuationCompletionProbeFault::
                kFilesystemInodesOverCap:
                observation->filesystem_free_inodes = 0U;
                break;
            case ContinuationCompletionProbeFault::
                kQuotaInodesOverCap:
                observation->quota_free_inodes = 0U;
                break;
            case ContinuationCompletionProbeFault::
                kCorruptStateBeforePublish: {
                const int descriptor = ::openat(
                    retained_raw_root_fd,
                    ingress::kRawReserveStateFilename,
                    O_RDWR | O_NOFOLLOW | O_NONBLOCK |
                        O_CLOEXEC);
                std::byte value{};
                const bool changed =
                    descriptor >= 0 &&
                    ::pread(
                        descriptor,
                        &value,
                        1U,
                        0) == 1 &&
                    (value ^= std::byte{0x01U},
                     ::pwrite(
                         descriptor,
                         &value,
                         1U,
                         0) == 1);
                if (descriptor >= 0) {
                    static_cast<void>(
                        ::close(descriptor));
                }
                if (!changed) {
                    if (error != nullptr) {
                        *error =
                            "cannot inject reserve.state publication race";
                    }
                    return false;
                }
                fault_injected = true;
                break;
            }
            case ContinuationCompletionProbeFault::
                kDropCoordinatorLeaseBeforeRootSync:
                ingress::
                    RawFinalizationContinuationCoordinatorTestPeerV1::
                        DropLease(coordinator);
                fault_injected = coordinator != nullptr;
                if (!fault_injected) {
                    if (error != nullptr) {
                        *error =
                            "cannot inject coordinator root-sync failure";
                    }
                    return false;
                }
                break;
        }
        last_request = request;
        observed = true;
        return true;
    }

    ContinuationCompletionProbeFault fault =
        ContinuationCompletionProbeFault::kNone;
    ingress::RawReserveRegistryCoordinatorV1*
        coordinator = nullptr;
    bool observed = false;
    bool fault_injected = false;
    ingress::RawEmergencyReserveCapacityProbeRequestV1
        last_request{};
};

void SetContinuationAction(
    ingress::ReserveStateEntryV1* entry,
    std::size_t index,
    ingress::FinalizationActionKindV1 kind,
    std::uint32_t byte_quanta,
    std::uint32_t inode_cap,
    const ingress::ReserveStateV1Identity& cycle,
    std::uint32_t object_sequence,
    std::uint64_t range_start,
    std::uint64_t range_end_or_size) {
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
                0x20U + index));

    auto& plan = entry->plans[index];
    plan.plan_version = 1U;
    plan.object_type = kind;
    plan.object_sequence = object_sequence;
    plan.range_start = range_start;
    plan.range_end_or_size = range_end_or_size;
    plan.causal_id = cycle;
}

[[nodiscard]] ingress::ReserveCoordinatorStateV1
MakeContinuationBootstrap(
    int root_fd,
    const ingress::RawEmergencyWriterAckFactsV1& facts) {
    ingress::ReserveCoordinatorStateV1 state{};
    struct stat root {};
    if (::fstat(root_fd, &root) != 0) {
        return state;
    }
    state.header.reserve_state_uuid =
        Pattern<16U>(0x91U);
    state.header.quota_identity_sha256 =
        Pattern<32U>(0x31U);
    state.header.mount_identity_sha256 =
        Pattern<32U>(0x51U);
    state.header.device_id =
        static_cast<std::uint64_t>(root.st_dev);
    state.header.declared_releasable_bytes =
        UINT64_C(1) << 20U;
    state.header.allocation_quantum_bytes = 4096U;
    state.header.declared_inode_reserve_count = 64U;
    state.header.byte_probe_version = 1U;
    state.header.inode_probe_version = 1U;
    state.header.inode_inventory_sha256 =
        Pattern<32U>(0x71U);
    state.header.safe_stop_catalog_sha256 =
        Pattern<32U>(0x81U);

    ingress::ReserveStateEntryV1 registry{};
    registry.source_stream_id =
        facts.writer.source_stream_id;
    registry.capture_date =
        facts.writer.capture_date;
    registry.stream_day_id =
        facts.writer.stream_day_id;
    registry.registry_status =
        ingress::ReserveRegistryStatusV1::kActive;
    registry.recovery_origin =
        ingress::ReserveRecoveryOriginV1::kFreshInit;
    registry.recovery_intent =
        ingress::ReserveRecoveryIntentV1::
            kResumeConnect;
    registry.writer_instance =
        facts.writer.writer_instance;
    registry.executor_or_recovery_attempt =
        facts.writer.writer_instance;
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

[[nodiscard]] ingress::ReserveStateEntryV1
MakeContinuationGrant(
    const ingress::ReserveStateEntryV1& registry,
    const ingress::ReserveStateV1Identity& cycle,
    const ingress::RawEmergencyWriterAckFactsV1& facts) {
    constexpr std::uint64_t kContinuationAllocation =
        16384U;
    ingress::ReserveStateEntryV1 grant{};
    grant.source_stream_id =
        registry.source_stream_id;
    grant.capture_date = registry.capture_date;
    grant.stream_day_id = registry.stream_day_id;
    grant.ack_status =
        ingress::ReserveAckStatusV1::kAcked;
    grant.counter_validity =
        ingress::kReserveCounterValidityMask;
    grant.grant_status =
        ingress::ReserveGrantStatusV1::kPending;
    grant.grant_flags =
        ingress::kReserveGrantRawFinalization;
    grant.writer_instance = registry.writer_instance;
    grant.safe_stop_template_id =
        registry.safe_stop_template_id;
    grant.continuation_allocation_cap =
        kContinuationAllocation;
    grant.raw_counters.callback_published_records =
        facts.callback.captured_records;
    grant.raw_counters.callback_published_vendor_bytes =
        facts.callback.captured_vendor_bytes;
    grant.raw_counters.append_global_wal_pos =
        facts.wal.append.global_wal_pos;
    grant.raw_counters.append_ingress_sequence =
        facts.wal.append.ingress_sequence;
    grant.raw_counters.durable_global_wal_pos =
        facts.wal.durable.global_wal_pos;
    grant.raw_counters.durable_ingress_sequence =
        facts.wal.durable.ingress_sequence;
    grant.raw_counters.queued_record_count =
        facts.queued_record_count;
    grant.raw_counters.queued_framed_wal_bytes =
        facts.queued_framed_wal_bytes;

    SetContinuationAction(
        &grant,
        0U,
        ingress::FinalizationActionKindV1::
            kCurrentSegmentDrain,
        1U,
        1U,
        cycle,
        facts.writer.segment_sequence,
        facts.writer.segment_base_wal_pos,
        facts.wal.append.segment_offset);
    SetContinuationAction(
        &grant,
        1U,
        ingress::FinalizationActionKindV1::
            kJournalGrowth,
        1U,
        1U,
        cycle,
        facts.writer.segment_sequence,
        facts.wal.journal_logical_size,
        ingress::kRawV1DurableMarkerBytes);
    SetContinuationAction(
        &grant,
        2U,
        ingress::FinalizationActionKindV1::kIndex,
        1U,
        1U,
        cycle,
        facts.writer.segment_sequence,
        0U,
        1U);
    SetContinuationAction(
        &grant,
        3U,
        ingress::FinalizationActionKindV1::kManifest,
        1U,
        1U,
        cycle,
        facts.writer.segment_sequence,
        0U,
        1U);
    SetContinuationAction(
        &grant,
        4U,
        ingress::FinalizationActionKindV1::
            kContinuation,
        6U,
        2U,
        cycle,
        facts.writer.segment_sequence + 1U,
        facts.wal.append.global_wal_pos,
        kContinuationAllocation);
    SetContinuationAction(
        &grant,
        5U,
        ingress::FinalizationActionKindV1::kIndex,
        1U,
        1U,
        cycle,
        facts.writer.segment_sequence + 1U,
        0U,
        1U);
    SetContinuationAction(
        &grant,
        6U,
        ingress::FinalizationActionKindV1::kManifest,
        1U,
        1U,
        cycle,
        facts.writer.segment_sequence + 1U,
        0U,
        1U);
    SetContinuationAction(
        &grant,
        7U,
        ingress::FinalizationActionKindV1::
            kFinalizationReport,
        1U,
        1U,
        cycle,
        0U,
        0U,
        ingress::kFinalizationReportV1MaximumBytes);
    grant.grant_bytes = 13U * 4096U;
    return grant;
}

struct ContinuationE2eFixture final {
    ~ContinuationE2eFixture() {
        if (route_fd >= 0) {
            static_cast<void>(::close(route_fd));
        }
    }

    ContinuationTempDirectory directory;
    ingress::RawIngressAppConfigV1 config;
    std::unique_ptr<EmptyLiveSource> live_source;
    std::unique_ptr<ingress::RawIngressApp> app;
    SlowRecordingPreparedSink* sink = nullptr;
    std::shared_ptr<
        ingress::RawReserveRegistryCoordinatorV1>
        coordinator;
    ingress::ReserveFinalizationActionKeyV1
        continuation_key{};
    ingress::RawEmergencyWriterAckFactsV1 facts{};
    std::unique_ptr<
        ingress::RawFinalizationContinuationPosixV1>
        session;
    std::unique_ptr<ingress::RawWriterLease>
        original_writer_lease;
    ContinuationMutationFault fault;
    std::uint64_t realtime = 7000U;
    std::uint64_t monotonic = 6000U;
    int route_fd = -1;
    std::string route_slug;
};

[[nodiscard]]
ingress::RawFinalizationContinuationPosixOptionsV1
ContinuationOptions(
    ContinuationE2eFixture* fixture) {
    ingress::RawFinalizationContinuationPosixOptionsV1
        options{};
    options.artifact_options
        .expected_raw_schema_sha256 =
        ingress::kFrozenRawSchemaSha256;
    options.artifact_options.maximum_segment_bytes =
        1U << 20U;
    if (fixture != nullptr) {
        options.realtime_now =
            &ContinuationFixedClock;
        options.realtime_clock_context =
            &fixture->realtime;
        options.monotonic_now =
            &ContinuationFixedClock;
        options.monotonic_clock_context =
            &fixture->monotonic;
        options.mutation_hook =
            &ContinuationMutationFault::Inject;
        options.mutation_hook_context =
            &fixture->fault;
    }
    return options;
}

[[nodiscard]] bool PublishContinuationOldBoundary(
    ContinuationE2eFixture* fixture,
    std::string* error) {
    if (fixture == nullptr || fixture->route_fd < 0 ||
        fixture->sink == nullptr) {
        return false;
    }
    const auto& facts = fixture->facts;
    ingress::SegmentHeaderV1 header{};
    header.source_stream_id =
        facts.writer.source_stream_id;
    header.capture_date = facts.writer.capture_date;
    header.stream_day_id = facts.writer.stream_day_id;
    header.segment_sequence =
        facts.writer.segment_sequence;
    header.segment_base_wal_pos =
        facts.writer.segment_base_wal_pos;
    header.first_ingress_sequence =
        facts.writer.first_ingress_sequence;
    header.created_realtime_ns = 1000U;
    header.created_monotonic_ns = 900U;
    header.host_uuid = Pattern<16U>(0x11U);
    header.linux_boot_id = Pattern<16U>(0x21U);
    header.clock_epoch_algorithm =
        fixture->config.recovered
            .clock_epoch_algorithm_version;
    header.clock_epoch_digest =
        fixture->config.recovered
            .clock_epoch_digest;
    header.clock_epoch_label =
        fixture->config.recovered
            .clock_epoch_label;
    header.sdk_archive_sha256 =
        Pattern<32U>(0x41U);
    header.libmdl_api_sha256 =
        Pattern<32U>(0x61U);
    if (!l2flow::common::ParseSha256Hex(
            fixture->config.stable
                .endpoint_contract_sha256,
            &header.endpoint_contract_sha256,
            error)) {
        return false;
    }
    header.config_sha256 = Pattern<32U>(0x81U);
    header.raw_schema_sha256 =
        ingress::kFrozenRawSchemaSha256;
    header.build_manifest_sha256 =
        Pattern<32U>(0xa1U);

    ingress::RawV1SegmentHeaderWire header_wire{};
    if (ingress::EncodeSegmentHeaderV1(
            header, &header_wire) !=
        ingress::RawV1Error::kNone) {
        return false;
    }
    std::vector<std::byte> segment(
        header_wire.begin(), header_wire.end());
    const auto records =
        fixture->sink->appended_records();
    for (const auto& record : records) {
        segment.insert(
            segment.end(),
            record.begin(),
            record.end());
    }
    if (records.size() !=
            facts.capture.append.records ||
        segment.size() !=
            facts.wal.append.segment_offset) {
        return false;
    }

    const std::string segment_name =
        "segment-00000001.raw";
    const int segment_fd = ContinuationOpenFileAt(
        fixture->route_fd,
        segment_name.c_str());
    if (segment_fd < 0 ||
        !ContinuationPwriteAll(
            segment_fd, 0U, segment) ||
        ::fsync(segment_fd) != 0 ||
        ::fsync(fixture->route_fd) != 0) {
        if (segment_fd >= 0) {
            static_cast<void>(::close(segment_fd));
        }
        return false;
    }

    ingress::DurableJournalHeaderV1 journal_header{};
    journal_header.capture_date =
        facts.writer.capture_date;
    journal_header.source_stream_id =
        facts.writer.source_stream_id;
    journal_header.stream_day_id =
        facts.writer.stream_day_id;
    journal_header.raw_schema_sha256 =
        header.raw_schema_sha256;
    journal_header.created_host_uuid =
        header.host_uuid;
    journal_header.created_linux_boot_id =
        header.linux_boot_id;
    journal_header.created_clock_epoch_algorithm =
        header.clock_epoch_algorithm;
    journal_header.created_clock_epoch_digest =
        header.clock_epoch_digest;
    journal_header.created_clock_epoch_label =
        header.clock_epoch_label;
    ingress::RawV1JournalHeaderWire journal_wire{};
    ingress::RawV1DurableMarkerWire durable_wire{};
    ingress::RawV1DurableMarkerWire seal_wire{};
    if (ingress::EncodeDurableJournalHeaderV1(
            journal_header, &journal_wire) !=
            ingress::RawV1Error::kNone ||
        ingress::EncodeDurableMarkerV1(
            ContinuationMarker(
                facts, facts.wal.durable, 0U),
            &durable_wire) !=
            ingress::RawV1Error::kNone ||
        ingress::EncodeDurableMarkerV1(
            ContinuationMarker(
                facts,
                facts.wal.append,
                ingress::kRawV1SegmentSealed),
            &seal_wire) !=
            ingress::RawV1Error::kNone) {
        static_cast<void>(::close(segment_fd));
        return false;
    }
    const int journal_fd = ContinuationOpenFileAt(
        fixture->route_fd,
        ingress::kRawJournalFilename);
    const bool journal_ready =
        journal_fd >= 0 &&
        ContinuationPwriteAll(
            journal_fd, 0U, journal_wire) &&
        ContinuationPwriteAll(
            journal_fd,
            facts.wal.journal_logical_size -
                ingress::kRawV1DurableMarkerBytes,
            durable_wire) &&
        ContinuationPwriteAll(
            journal_fd,
            facts.wal.journal_logical_size,
            seal_wire) &&
        ::fsync(journal_fd) == 0 &&
        ::fsync(fixture->route_fd) == 0;
    if (!journal_ready) {
        if (journal_fd >= 0) {
            static_cast<void>(::close(journal_fd));
        }
        static_cast<void>(::close(segment_fd));
        return false;
    }
    static_cast<void>(::close(journal_fd));

    ingress::RawSegmentArtifactOptionsV1 options{};
    options.expected_raw_schema_sha256 =
        header.raw_schema_sha256;
    options.maximum_segment_bytes = 1U << 20U;
    const auto plan =
        ingress::
            PrepareRawSegmentArtifactPlanForPosixFdV1(
                fixture->route_fd,
                segment_fd,
                seal_wire,
                options);
    ingress::RawSegmentArtifactCausalProofV1 proof{};
    proof.segment_truncated_and_synced_to_logical_end =
        true;
    proof.accepted_seal_marker_journal_synced = true;
    proof.synced_segment_logical_end_offset =
        segment.size();
    proof.synced_segment_sha256 =
        l2flow::common::ComputeSha256(
            std::span<const std::byte>(
                segment.data(), segment.size()));
    proof.journal_synced_sealed_marker_bytes =
        seal_wire;
    const auto published =
        ingress::BuildAndPublishRawSegmentArtifactAtV1(
            fixture->route_fd,
            segment_fd,
            seal_wire,
            options,
            proof);
    static_cast<void>(::close(segment_fd));
    if (!plan.ok() || !published.ok() ||
        published.metadata.segment_sha256 !=
            plan.metadata.segment_sha256) {
        return false;
    }

    const std::array<
        ingress::RawSealedSegmentMetadataV1,
        1U>
        metadata{published.metadata};
    ingress::RawManifestV1 manifest{};
    std::string manifest_bytes;
    if (ingress::BuildRecoveredClosedRawManifestV1(
            metadata,
            nullptr,
            &manifest) !=
            ingress::RawManifestTransitionErrorV1::kNone ||
        ingress::EncodeRawManifestJcs(
            manifest,
            &manifest_bytes) !=
            ingress::RawManifestV1Error::kNone) {
        return false;
    }
    const int manifest_fd = ContinuationOpenFileAt(
        fixture->route_fd,
        ingress::kRawManifestCurrentFilename);
    const bool manifest_ready =
        manifest_fd >= 0 &&
        ContinuationPwriteAll(
            manifest_fd,
            0U,
            std::as_bytes(
                std::span<const char>(
                    manifest_bytes.data(),
                    manifest_bytes.size()))) &&
        ::fsync(manifest_fd) == 0 &&
        ::fsync(fixture->route_fd) == 0;
    if (manifest_fd >= 0) {
        static_cast<void>(::close(manifest_fd));
    }
    return manifest_ready;
}

[[nodiscard]] bool PublishContinuationGrant(
    ContinuationE2eFixture* fixture,
    std::unique_ptr<
        ingress::RawReserveFinalizationActionV1>*
        action,
    std::string* error) {
    if (fixture == nullptr || action == nullptr) {
        return false;
    }
    const ingress::ReserveStateV1Identity cycle =
        Pattern<16U>(0xc1U);
    const auto bootstrap =
        MakeContinuationBootstrap(
            fixture->directory.fd(),
            fixture->facts);
    const auto& initial_slot =
        bootstrap.slots[bootstrap.selected_slot];
    const auto grant =
        MakeContinuationGrant(
            initial_slot.entries[0U],
            cycle,
            fixture->facts);

    ingress::RawReserveStatePosixError posix_error{};
    ingress::ReserveStateV1Error codec_error{};
    auto state_file =
        ingress::PublishFreshRawReserveStateAtV1(
            fixture->directory.fd(),
            bootstrap,
            &posix_error,
            &codec_error,
            error);
    if (state_file == nullptr) {
        return false;
    }

    ingress::ReserveReleaseIntentV1 intent_request{};
    intent_request.reason =
        ingress::ReserveReleaseReasonV1::kLowWatermark;
    intent_request.trigger =
        ingress::ReserveReleaseTriggerV1::
            kFilesystemBytes;
    intent_request.writer_set_sha256 =
        Pattern<32U>(0xd1U);
    ingress::ReserveStateSlotV1 intent{};
    if (ingress::BuildReserveReleasingIntentV1(
            bootstrap.header,
            state_file->state().slots[
                state_file->state().selected_slot],
            intent_request,
            &intent) !=
            ingress::ReserveStateV1Error::kNone ||
        state_file->PublishNext(
            intent, &codec_error, error) !=
            ingress::RawReserveStatePosixError::kNone) {
        return false;
    }

    ingress::ReserveReleasePreparedV1 request{};
    request.finalization_cycle_id = cycle;
    request.grant_entries =
        std::span<const ingress::ReserveStateEntryV1>(
            &grant, 1U);
    request.pre_release_fs_free_bytes =
        UINT64_C(1) << 30U;
    request.pre_release_quota_free_bytes =
        UINT64_C(1) << 30U;
    request.pre_release_fs_free_inodes =
        UINT64_C(1) << 20U;
    request.pre_release_quota_free_inodes =
        UINT64_C(1) << 20U;
    request.expected_release_fs_bytes =
        bootstrap.header.declared_releasable_bytes;
    request.expected_release_quota_bytes =
        bootstrap.header.declared_releasable_bytes;
    request.expected_release_fs_inodes =
        bootstrap.header
            .declared_inode_reserve_count +
        1U;
    request.expected_release_quota_inodes =
        bootstrap.header
            .declared_inode_reserve_count +
        1U;
    request.reserved_margin_bytes =
        bootstrap.header.allocation_quantum_bytes;
    request.reserved_margin_inodes = 1U;
    request.effective_min_fs_free_bytes =
        bootstrap.header.declared_releasable_bytes;
    request.effective_min_quota_free_bytes =
        bootstrap.header.declared_releasable_bytes;
    request.effective_min_fs_free_inodes =
        bootstrap.header
            .declared_inode_reserve_count;
    request.effective_min_quota_free_inodes =
        bootstrap.header
            .declared_inode_reserve_count;
    ingress::ReserveStateSlotV1 prepared{};
    if (ingress::BuildReserveReleasingPreparedV1(
            bootstrap.header,
            state_file->state().slots[
                state_file->state().selected_slot],
            request,
            &prepared) !=
            ingress::ReserveStateV1Error::kNone ||
        state_file->PublishNext(
            prepared, &codec_error, error) !=
            ingress::RawReserveStatePosixError::kNone) {
        return false;
    }
    ingress::ReserveStateSlotV1 consumed{};
    if (ingress::BuildReserveConsumedV1(
            bootstrap.header,
            state_file->state().slots[
                state_file->state().selected_slot],
            &consumed) !=
            ingress::ReserveStateV1Error::kNone ||
        state_file->PublishNext(
            consumed, &codec_error, error) !=
            ingress::RawReserveStatePosixError::kNone) {
        return false;
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
    ingress::RawReserveCoordinatorErrorV1
        coordinator_error{};
    fixture->coordinator =
        ingress::AttachRawReserveRegistryCoordinatorAtV1(
            fixture->directory.fd(),
            marker,
            &coordinator_error,
            error);
    if (fixture->coordinator == nullptr) {
        return false;
    }

    ingress::ReserveFinalizationGrantKeyV1 grant_key{};
    grant_key.finalization_cycle_id = cycle;
    grant_key.source_stream_id = grant.source_stream_id;
    grant_key.capture_date = grant.capture_date;
    grant_key.stream_day_id = grant.stream_day_id;
    grant_key.ack_status = grant.ack_status;
    grant_key.ack_writer_instance =
        grant.writer_instance;
    grant_key.safe_stop_template_id =
        grant.safe_stop_template_id;
    ingress::ReserveGrantActivationV1 activation{};
    activation.grant = grant_key;
    activation.executor_instance =
        grant.writer_instance;
    activation.filesystem_free_byte_baseline =
        UINT64_C(1) << 30U;
    activation.quota_free_byte_baseline =
        UINT64_C(1) << 30U;
    activation.filesystem_free_inode_baseline =
        UINT64_C(1) << 20U;
    activation.quota_free_inode_baseline =
        UINT64_C(1) << 20U;
    if (fixture->coordinator
            ->ActivateNextFinalizationGrant(
                activation, error) !=
        ingress::RawReserveCoordinatorErrorV1::kNone) {
        return false;
    }

    for (std::size_t index = 0U;
         index < 4U;
         ++index) {
        ingress::ReserveFinalizationActionKeyV1 key{};
        key.grant = grant_key;
        key.action_id =
            static_cast<std::uint16_t>(index);
        key.action_kind =
            grant.actions[index].action_kind;
        key.object_plan_sha256 =
            grant.actions[index].object_plan_sha256;
        if (fixture->coordinator
                    ->DebitFinalizationAction(
                        key, error) !=
                ingress::
                    RawReserveCoordinatorErrorV1::kNone ||
            fixture->coordinator
                    ->CompleteFinalizationAction(
                        key, error) !=
                ingress::
                    RawReserveCoordinatorErrorV1::kNone) {
            return false;
        }
    }

    fixture->continuation_key.grant = grant_key;
    fixture->continuation_key.action_id = 4U;
    fixture->continuation_key.action_kind =
        ingress::FinalizationActionKindV1::
            kContinuation;
    fixture->continuation_key.object_plan_sha256 =
        grant.actions[4U].object_plan_sha256;
    if (fixture->coordinator
            ->DebitFinalizationAction(
                fixture->continuation_key,
                error) !=
        ingress::RawReserveCoordinatorErrorV1::kNone) {
        return false;
    }
    *action =
        fixture->coordinator
            ->AcquireDebitedFinalizationAction(
                fixture->continuation_key,
                fixture->route_slug,
                &coordinator_error,
                error);
    return *action != nullptr;
}

[[nodiscard]] std::unique_ptr<
    ContinuationE2eFixture>
BuildContinuationE2eFixture(
    TestContext* test,
    std::optional<
        ingress::RawFinalizationContinuationMutationV1>
        fault,
    bool invalidate_ack_before_begin = false) {
    auto fixture =
        std::make_unique<ContinuationE2eFixture>();
    if (!fixture->directory.valid()) {
        test->Expect(
            false,
            "continuation temp Raw root is available");
        return nullptr;
    }
    fixture->config = MakeConfig();
    const auto canonical =
        ingress::CanonicalRawStreamSlugV1(
            fixture->config.recovered
                .source_stream_id);
    if (!canonical.has_value()) {
        test->Expect(
            false,
            "continuation source has frozen route slug");
        return nullptr;
    }
    fixture->route_slug = std::string(*canonical);

    int date_fd = -1;
    const std::string date_name =
        "capture_date=" +
        std::to_string(
            fixture->config.recovered.capture_date);
    const std::string route_name =
        "stream=" +
        std::to_string(
            fixture->config.recovered
                .source_stream_id) +
        "-" + fixture->route_slug;
    if (!ContinuationMakeDirectoryAt(
            fixture->directory.fd(),
            date_name,
            &date_fd) ||
        !ContinuationMakeDirectoryAt(
            date_fd,
            route_name,
            &fixture->route_fd)) {
        if (date_fd >= 0) {
            static_cast<void>(::close(date_fd));
        }
        test->Expect(
            false,
            "continuation canonical route is created");
        return nullptr;
    }
    static_cast<void>(::close(date_fd));
    std::string error;
    fixture->original_writer_lease =
        ingress::AcquireRawWriterLeaseAt(
            fixture->route_fd,
            fixture->config.recovered
                .source_stream_id,
            fixture->config.recovered.capture_date,
            &error);
    if (fixture->original_writer_lease == nullptr) {
        test->Expect(
            false,
            "continuation fixture acquires the original writer lease: " +
                error);
        return nullptr;
    }

    fixture->live_source =
        std::make_unique<EmptyLiveSource>();
    if (fixture->live_source == nullptr) {
        return nullptr;
    }
    auto tail = MakeTail(
        fixture->config,
        fixture->live_source.get());
    auto events = std::make_shared<SdkEvents>();
    auto sink =
        std::make_unique<
            SlowRecordingPreparedSink>(
                fixture->config.recovered);
    fixture->sink = sink.get();
    fixture->app =
        std::make_unique<ingress::RawIngressApp>(
            fixture->config,
            std::make_shared<FakeFactory>(events),
            std::make_unique<FixedClock>(),
            std::move(sink),
            std::move(tail),
            std::make_unique<
                RecordingCleanStopGate>());
    if (!fixture->app->Initialize(&error)) {
        test->Expect(
            false,
            "continuation app initializes: " + error);
        return nullptr;
    }
    const sdk::IngressSpec& spec =
        sdk::GetIngressSpec(
            fixture->config.stable.kind);
    ShutdownMessage shutdown_message(
        spec.required.front());
    events->shutdown_message = &shutdown_message;
    events->shutdown_message_count = 32U;
    auto ack =
        fixture->app->BeginEmergencyStop(&error);
    events->shutdown_message = nullptr;
    if (ack == nullptr ||
        !ack->facts().exact() ||
        ack->facts().queued_record_count == 0U) {
        test->Expect(
            false,
            "real emergency ACK freezes a non-empty suffix: " +
                error);
        return nullptr;
    }
    fixture->facts = ack->facts();
    if (!PublishContinuationOldBoundary(
            fixture.get(), &error)) {
        test->Expect(
            false,
            "old drain/index/closed-manifest boundary publishes: " +
                error);
        return nullptr;
    }

    std::unique_ptr<
        ingress::RawReserveFinalizationActionV1>
        action;
    if (!PublishContinuationGrant(
            fixture.get(), &action, &error)) {
        test->Expect(
            false,
            "exact ACKED continuation grant is acquired: " +
                error);
        return nullptr;
    }

    if (invalidate_ack_before_begin) {
        static_cast<void>(
            fixture->app->Stop(&error));
    }
    if (fault.has_value()) {
        fixture->fault.selected = *fault;
        fixture->fault.enabled = true;
    }
    const auto options =
        ContinuationOptions(fixture.get());
    ingress::RawFinalizationContinuationFailureV1
        failure{};
    fixture->session =
        ingress::RawFinalizationContinuationPosixV1::
            Begin(
                std::move(action),
                std::move(ack),
                options,
                &failure,
                &error);
    if (invalidate_ack_before_begin) {
        test->Expect(
            fixture->session == nullptr &&
                failure ==
                    ingress::
                        RawFinalizationContinuationFailureV1::
                            kNoLiveAck,
            "invalidated/replaced emergency ACK is rejected");
        return fixture;
    }
    if (fixture->session == nullptr) {
        test->Expect(
            false,
            "continuation Begin consumes exact capabilities: " +
                error);
        return nullptr;
    }
    return fixture;
}

[[nodiscard]] bool ValidateContinuationReceiptAndBytes(
    TestContext* test,
    ContinuationE2eFixture* fixture,
    const ingress::RawFinalizationContinuationReceiptV1&
        receipt) {
    std::string error;
    const std::string segment_name =
        "segment-00000002.raw";
    const int descriptor = ::openat(
        fixture->route_fd,
        segment_name.c_str(),
        O_RDONLY | O_NOFOLLOW | O_NONBLOCK |
            O_CLOEXEC);
    std::vector<std::byte> bytes;
    const bool read =
        descriptor >= 0 &&
        ContinuationReadAll(descriptor, &bytes);
    if (descriptor >= 0) {
        static_cast<void>(::close(descriptor));
    }
    auto shared =
        std::make_shared<
            const std::vector<std::byte>>(
                bytes);
    const auto scan =
        ingress::ScanRawSegmentV1(
            shared, bytes.size());
    const bool valid =
        receipt.Validate(&error) &&
        read &&
        scan.ok() &&
        scan.segment.segment_flags ==
            ingress::kRawV1FinalizationContinuation &&
        scan.segment.segment_sequence == 2U &&
        scan.records.size() ==
            fixture->facts.queued_record_count &&
        receipt.frozen_record_count() ==
            fixture->facts.queued_record_count &&
        receipt.frozen_framed_wal_bytes() ==
            fixture->facts.queued_framed_wal_bytes &&
        receipt.final_cursor().ingress_sequence ==
            fixture->facts.callback
                .captured_ingress_sequence &&
        receipt.final_cursor().segment_offset ==
            bytes.size() &&
        !scan.records.empty() &&
        scan.records.front()
                .header()
                .ingress_sequence ==
            fixture->facts.wal.append
                    .ingress_sequence +
                1U &&
        scan.records.back()
                .header()
                .ingress_sequence ==
            fixture->facts.callback
                .captured_ingress_sequence;
    test->Expect(
        valid,
        "receipt binds exact continuation records, framed bytes, ingress range, and sealed cursor");
    return valid;
}

void RunContinuationFaultScenario(
    TestContext* test,
    ingress::RawFinalizationContinuationMutationV1
        mutation,
    std::string_view label) {
    auto fixture =
        BuildContinuationE2eFixture(
            test, mutation);
    if (fixture == nullptr ||
        fixture->session == nullptr) {
        return;
    }
    std::string error;
    const bool first =
        fixture->session->CreateOrAdopt(&error);
    fixture->fault.enabled = false;
    const bool retried =
        fixture->session->CreateOrAdopt(&error);
    auto receipt =
        retried
            ? fixture->session->TakeReceipt(&error)
            : nullptr;
    test->Expect(
        !first && fixture->fault.fired &&
            retried && receipt != nullptr,
        std::string(label) +
            " crash prefix is adopted by the same live session");
    if (receipt != nullptr) {
        static_cast<void>(
            ValidateContinuationReceiptAndBytes(
                test, fixture.get(), *receipt));
    }
}

struct ReadyContinuationReceipt final {
    std::unique_ptr<ContinuationE2eFixture> fixture;
    std::unique_ptr<
        ingress::RawFinalizationContinuationReceiptV1>
        receipt;
};

[[nodiscard]] std::unique_ptr<
    ReadyContinuationReceipt>
BuildReadyContinuationReceipt(TestContext* test) {
    auto ready =
        std::make_unique<ReadyContinuationReceipt>();
    ready->fixture =
        BuildContinuationE2eFixture(
            test, std::nullopt);
    if (ready->fixture == nullptr ||
        ready->fixture->session == nullptr) {
        return nullptr;
    }
    std::string error;
    if (!ready->fixture->session
             ->CreateOrAdopt(&error)) {
        test->Expect(
            false,
            "continuation completion fixture publishes: " +
                error);
        return nullptr;
    }
    ready->receipt =
        ready->fixture->session->TakeReceipt(&error);
    if (ready->receipt == nullptr ||
        !ready->receipt->Validate(&error)) {
        test->Expect(
            false,
            "continuation completion fixture retains a valid receipt: " +
                error);
        return nullptr;
    }
    return ready;
}

[[nodiscard]] bool ContinuationActionIs(
    const ingress::ReserveCoordinatorStateV1& state,
    const ingress::ReserveFinalizationActionKeyV1& key,
    ingress::FinalizationActionStateV1 expected) {
    if (state.selected_slot >= state.slots.size()) {
        return false;
    }
    const auto& slot =
        state.slots[state.selected_slot];
    for (std::size_t index = 0U;
         index <
         static_cast<std::size_t>(slot.entry_count);
         ++index) {
        const auto& entry = slot.entries[index];
        if (entry.source_stream_id ==
                key.grant.source_stream_id &&
            entry.capture_date ==
                key.grant.capture_date &&
            entry.stream_day_id ==
                key.grant.stream_day_id &&
            key.action_id <
                ingress::
                    kReserveStateV1ActionCapacity) {
            const auto& action =
                entry.actions[
                    static_cast<std::size_t>(
                        key.action_id)];
            return action.action_kind ==
                       key.action_kind &&
                   action.object_plan_sha256 ==
                       key.object_plan_sha256 &&
                   action.action_state == expected;
        }
    }
    return false;
}

[[nodiscard]] bool ContinuationActionIs(
    const ContinuationE2eFixture& fixture,
    ingress::FinalizationActionStateV1 expected) {
    return fixture.coordinator != nullptr &&
           ContinuationActionIs(
               fixture.coordinator->state(),
               fixture.continuation_key,
               expected);
}

[[nodiscard]] bool FlipContinuationArtifactByte(
    int route_fd,
    const char* name,
    std::uint64_t offset) {
    const int descriptor = ::openat(
        route_fd,
        name,
        O_RDWR | O_NOFOLLOW | O_NONBLOCK |
            O_CLOEXEC);
    std::byte value{};
    const bool changed =
        descriptor >= 0 &&
        ::pread(
            descriptor,
            &value,
            1U,
            static_cast<off_t>(offset)) == 1 &&
        (value ^= std::byte{0x01U},
         ::pwrite(
             descriptor,
             &value,
             1U,
             static_cast<off_t>(offset)) == 1) &&
        ::fsync(descriptor) == 0;
    if (descriptor >= 0) {
        static_cast<void>(::close(descriptor));
    }
    return changed;
}

[[nodiscard]] bool ReplaceContinuationArtifactName(
    int route_fd,
    const char* name) {
    const std::string displaced =
        std::string(".displaced-") + name;
    if (::renameat(
            route_fd,
            name,
            route_fd,
            displaced.c_str()) != 0) {
        return false;
    }
    const int descriptor =
        ContinuationOpenFileAt(route_fd, name);
    const std::array<std::byte, 1U> replacement{
        std::byte{0x5aU}};
    const bool published =
        descriptor >= 0 &&
        ContinuationPwriteAll(
            descriptor, 0U, replacement) &&
        ::fsync(descriptor) == 0 &&
        ::fsync(route_fd) == 0;
    if (descriptor >= 0) {
        static_cast<void>(::close(descriptor));
    }
    return published;
}

[[nodiscard]] bool ReplaceContinuationCanonicalRoute(
    ContinuationE2eFixture* fixture) {
    if (fixture == nullptr) {
        return false;
    }
    const std::string date_name =
        "capture_date=" +
        std::to_string(
            fixture->config.recovered.capture_date);
    const std::string route_name =
        "stream=" +
        std::to_string(
            fixture->config.recovered
                .source_stream_id) +
        "-" + fixture->route_slug;
    const int date_fd = ::openat(
        fixture->directory.fd(),
        date_name.c_str(),
        O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
            O_NONBLOCK | O_CLOEXEC);
    if (date_fd < 0) {
        return false;
    }
    const std::string displaced =
        ".displaced-" + route_name;
    const bool replaced =
        ::renameat(
            date_fd,
            route_name.c_str(),
            date_fd,
            displaced.c_str()) == 0 &&
        ::mkdirat(
            date_fd,
            route_name.c_str(),
            0700) == 0 &&
        ::fsync(date_fd) == 0;
    static_cast<void>(::close(date_fd));
    return replaced;
}

[[nodiscard]] bool PublishContinuationStateRace(
    ReadyContinuationReceipt* ready,
    std::string* error) {
    if (ready == nullptr ||
        ready->fixture == nullptr) {
        return false;
    }
    ingress::RawReserveStatePosixError
        posix_error{};
    ingress::ReserveStateV1Error codec_error{};
    auto state_file =
        ingress::AttachRawReserveStateAtV1(
            ready->fixture->directory.fd(),
            &posix_error,
            &codec_error,
            error);
    if (state_file == nullptr) {
        return false;
    }
    const auto& state = state_file->state();
    if (state.selected_slot >=
        state.slots.size()) {
        return false;
    }
    ingress::ReserveStateSlotV1 failed{};
    return ingress::BuildReserveFailDebitedActionV1(
               state.header,
               state.slots[state.selected_slot],
               ready->fixture->continuation_key,
               &failed) ==
               ingress::ReserveStateV1Error::kNone &&
           state_file->PublishNext(
               failed,
               &codec_error,
               error) ==
               ingress::RawReserveStatePosixError::kNone;
}

void TestContinuationCompletionInputAndBindingNegatives(
    TestContext* test) {
    {
        auto ready =
            BuildReadyContinuationReceipt(test);
        if (ready != nullptr) {
            std::string error;
            const auto rejected =
                ready->fixture->coordinator
                    ->CompleteFinalizationContinuation(
                        std::move(ready->receipt),
                        nullptr,
                        &error);
            ContinuationCapacityProbe probe;
            const auto repeated =
                ready->fixture->coordinator
                    ->CompleteFinalizationContinuation(
                        std::move(ready->receipt),
                        &probe,
                        &error);
            test->Expect(
                rejected ==
                        ingress::
                            RawReserveCoordinatorErrorV1::
                                kInvalidArgument &&
                    repeated ==
                        ingress::
                            RawReserveCoordinatorErrorV1::
                                kInvalidArgument &&
                    ready->receipt == nullptr &&
                    !probe.observed &&
                    ContinuationActionIs(
                        *ready->fixture,
                        ingress::
                            FinalizationActionStateV1::
                                kDebited),
                "null probe consumes the receipt and repeated/null receipt use cannot advance COMPLETE");
        }
    }

    using Receipt =
        ingress::RawFinalizationContinuationReceiptV1;
    using Mutation = void (*)(Receipt*) noexcept;
    struct BindingMutation final {
        Mutation mutate = nullptr;
        const char* label = nullptr;
    };
    const std::array<BindingMutation, 8U>
        mutations{{
            {&ingress::
                 RawFinalizationContinuationTestPeerV1::
                     MutateKey,
             "key"},
            {&ingress::
                 RawFinalizationContinuationTestPeerV1::
                     MutateActionKind,
             "action kind"},
            {&ingress::
                 RawFinalizationContinuationTestPeerV1::
                     MutatePlan,
             "plan"},
            {&ingress::
                 RawFinalizationContinuationTestPeerV1::
                     MutateImmutableGrantHash,
             "immutable grant hash"},
            {&ingress::
                 RawFinalizationContinuationTestPeerV1::
                     MutateByteCap,
             "byte cap"},
            {&ingress::
                 RawFinalizationContinuationTestPeerV1::
                     MutateInodeCap,
             "inode cap"},
            {&ingress::
                 RawFinalizationContinuationTestPeerV1::
                     MutateExecutor,
             "executor"},
            {&ingress::
                 RawFinalizationContinuationTestPeerV1::
                     MutateDebitGeneration,
             "debit generation"},
        }};
    for (const BindingMutation& mutation :
         mutations) {
        auto ready =
            BuildReadyContinuationReceipt(test);
        if (ready == nullptr ||
            mutation.mutate == nullptr) {
            continue;
        }
        mutation.mutate(ready->receipt.get());
        ContinuationCapacityProbe probe;
        std::string error;
        const auto result =
            ready->fixture->coordinator
                ->CompleteFinalizationContinuation(
                    std::move(ready->receipt),
                    &probe,
                    &error);
        test->Expect(
            result ==
                    ingress::
                        RawReserveCoordinatorErrorV1::
                            kActionGenerationChanged &&
                ready->receipt == nullptr &&
                !probe.observed &&
                ContinuationActionIs(
                    *ready->fixture,
                    ingress::
                        FinalizationActionStateV1::
                            kDebited),
            std::string("receipt ") +
                mutation.label +
                " mismatch is consumed and cannot advance COMPLETE");
    }

    {
        auto ready =
            BuildReadyContinuationReceipt(test);
        if (ready != nullptr) {
            std::string error;
            const bool raced =
                PublishContinuationStateRace(
                    ready.get(), &error);
            ContinuationCapacityProbe probe;
            const auto result =
                ready->fixture->coordinator
                    ->CompleteFinalizationContinuation(
                        std::move(ready->receipt),
                        &probe,
                        &error);
            test->Expect(
                raced &&
                    result ==
                        ingress::
                            RawReserveCoordinatorErrorV1::
                                kActionGenerationChanged &&
                    ready->receipt == nullptr &&
                    !probe.observed &&
                    ContinuationActionIs(
                        *ready->fixture,
                        ingress::
                            FinalizationActionStateV1::
                                kFailed),
                "durable generation race invalidates the receipt before artifact/capacity publication");
        }
    }
}

void TestContinuationCompletionArtifactAndSuffixNegatives(
    TestContext* test) {
    struct ArtifactMutation final {
        const char* name = nullptr;
        std::uint64_t offset = 0U;
        const char* label = nullptr;
    };
    const std::array<ArtifactMutation, 5U>
        artifact_mutations{{
            {"segment-00000001.raw",
             ingress::kRawV1SegmentHeaderBytes,
             "old sealed segment"},
            {"segment-00000001.idx",
             0U,
             "old sealed index"},
            {ingress::kRawJournalFilename,
             0U,
             "journal"},
            {"segment-00000002.raw",
             ingress::kRawV1SegmentHeaderBytes,
             "continuation body"},
            {ingress::kRawManifestCurrentFilename,
             0U,
             "open manifest"},
        }};
    for (const ArtifactMutation& mutation :
         artifact_mutations) {
        auto ready =
            BuildReadyContinuationReceipt(test);
        if (ready == nullptr) {
            continue;
        }
        const bool changed =
            FlipContinuationArtifactByte(
                ready->fixture->route_fd,
                mutation.name,
                mutation.offset);
        ContinuationCapacityProbe probe;
        std::string error;
        const auto result =
            ready->fixture->coordinator
                ->CompleteFinalizationContinuation(
                    std::move(ready->receipt),
                    &probe,
                    &error);
        test->Expect(
            changed &&
                result ==
                    ingress::
                        RawReserveCoordinatorErrorV1::
                            kTargetIdentityChanged &&
                ready->receipt == nullptr &&
                !probe.observed &&
                ContinuationActionIs(
                    *ready->fixture,
                    ingress::
                        FinalizationActionStateV1::
                            kDebited),
            std::string("retained ") +
                mutation.label +
                " byte mutation is detected before COMPLETE");
    }

    {
        auto ready =
            BuildReadyContinuationReceipt(test);
        if (ready != nullptr) {
            const bool replaced =
                ReplaceContinuationArtifactName(
                    ready->fixture->route_fd,
                    "segment-00000002.raw");
            ContinuationCapacityProbe probe;
            std::string error;
            const auto result =
                ready->fixture->coordinator
                    ->CompleteFinalizationContinuation(
                        std::move(ready->receipt),
                        &probe,
                        &error);
            test->Expect(
                replaced &&
                    result ==
                        ingress::
                            RawReserveCoordinatorErrorV1::
                                kTargetIdentityChanged &&
                    ready->receipt == nullptr &&
                    !probe.observed &&
                    ContinuationActionIs(
                        *ready->fixture,
                        ingress::
                            FinalizationActionStateV1::
                                kDebited),
                "same-name continuation inode replacement is detected before COMPLETE");
        }
    }

    {
        auto ready =
            BuildReadyContinuationReceipt(test);
        if (ready != nullptr) {
            const bool replaced =
                ReplaceContinuationCanonicalRoute(
                    ready->fixture.get());
            ContinuationCapacityProbe probe;
            std::string error;
            const auto result =
                ready->fixture->coordinator
                    ->CompleteFinalizationContinuation(
                        std::move(ready->receipt),
                        &probe,
                        &error);
            test->Expect(
                replaced &&
                    result !=
                        ingress::
                            RawReserveCoordinatorErrorV1::
                                kNone &&
                    ready->receipt == nullptr &&
                    !probe.observed &&
                    ContinuationActionIs(
                        *ready->fixture,
                        ingress::
                            FinalizationActionStateV1::
                                kDebited),
                "canonical route same-name inode replacement invalidates the receipt before COMPLETE");
        }
    }

    using Receipt =
        ingress::RawFinalizationContinuationReceiptV1;
    using Mutation = void (*)(Receipt*) noexcept;
    struct SuffixMutation final {
        Mutation mutate = nullptr;
        const char* label = nullptr;
    };
    const std::array<SuffixMutation, 7U>
        suffix_mutations{{
            {&ingress::
                 RawFinalizationContinuationTestPeerV1::
                     MutateFrozenRecordCount,
             "frozen record count"},
            {&ingress::
                 RawFinalizationContinuationTestPeerV1::
                     MutateFrozenFramedBytes,
             "frozen framed bytes"},
            {&ingress::
                 RawFinalizationContinuationTestPeerV1::
                     MutateFinalCursor,
             "terminal cursor"},
            {&ingress::
                 RawFinalizationContinuationTestPeerV1::
                     ExceedObservedByteCap,
             "allocation byte cap"},
            {&ingress::
                 RawFinalizationContinuationTestPeerV1::
                     MutateAuthorizedInodeCap,
             "allocation inode cap"},
            {&ingress::
                 RawFinalizationContinuationTestPeerV1::
                     MutateRetainedWriterSnapshot,
             "writer lease snapshot"},
            {&ingress::
                 RawFinalizationContinuationTestPeerV1::
                     DropRetainedWriterLease,
             "writer lease ownership"},
        }};
    for (const SuffixMutation& mutation :
         suffix_mutations) {
        auto ready =
            BuildReadyContinuationReceipt(test);
        if (ready == nullptr ||
            mutation.mutate == nullptr) {
            continue;
        }
        mutation.mutate(ready->receipt.get());
        ContinuationCapacityProbe probe;
        std::string error;
        const auto result =
            ready->fixture->coordinator
                ->CompleteFinalizationContinuation(
                    std::move(ready->receipt),
                    &probe,
                    &error);
        test->Expect(
            result !=
                    ingress::
                        RawReserveCoordinatorErrorV1::
                            kNone &&
                ready->receipt == nullptr &&
                !probe.observed &&
                ContinuationActionIs(
                    *ready->fixture,
                    ingress::
                        FinalizationActionStateV1::
                            kDebited),
            std::string("receipt ") +
                mutation.label +
                " mismatch fails closed before COMPLETE");
    }
}

void TestContinuationCompletionCapacityAndBarrierNegatives(
    TestContext* test) {
    struct ProbeFailure final {
        ContinuationCompletionProbeFault fault =
            ContinuationCompletionProbeFault::kNone;
        const char* label = nullptr;
    };
    const std::array<ProbeFailure, 8U>
        failures{{
            {ContinuationCompletionProbeFault::
                 kMissingFilesystemBytes,
             "filesystem byte proof"},
            {ContinuationCompletionProbeFault::
                 kMissingQuotaBytes,
             "quota byte proof"},
            {ContinuationCompletionProbeFault::
                 kMissingFilesystemInodes,
             "filesystem inode proof"},
            {ContinuationCompletionProbeFault::
                 kMissingQuotaInodes,
             "quota inode proof"},
            {ContinuationCompletionProbeFault::
                 kFilesystemBytesOverCap,
             "filesystem byte drop"},
            {ContinuationCompletionProbeFault::
                 kQuotaBytesOverCap,
             "quota byte drop"},
            {ContinuationCompletionProbeFault::
                 kFilesystemInodesOverCap,
             "filesystem inode drop"},
            {ContinuationCompletionProbeFault::
                 kQuotaInodesOverCap,
             "quota inode drop"},
        }};
    for (const ProbeFailure& failure : failures) {
        auto ready =
            BuildReadyContinuationReceipt(test);
        if (ready == nullptr) {
            continue;
        }
        ContinuationCapacityProbe probe;
        probe.fault = failure.fault;
        std::string error;
        const auto result =
            ready->fixture->coordinator
                ->CompleteFinalizationContinuation(
                    std::move(ready->receipt),
                    &probe,
                    &error);
        test->Expect(
            result ==
                    ingress::
                        RawReserveCoordinatorErrorV1::
                            kCapacityProbeFailure &&
                ready->receipt == nullptr &&
                probe.observed &&
                ContinuationActionIs(
                    *ready->fixture,
                    ingress::
                        FinalizationActionStateV1::
                            kDebited),
            std::string("missing/excess ") +
                failure.label +
                " consumes receipt without COMPLETE");
    }

    {
        auto ready =
            BuildReadyContinuationReceipt(test);
        if (ready != nullptr) {
            ContinuationCapacityProbe probe;
            probe.fault =
                ContinuationCompletionProbeFault::
                    kCorruptStateBeforePublish;
            std::string error;
            const auto result =
                ready->fixture->coordinator
                    ->CompleteFinalizationContinuation(
                        std::move(ready->receipt),
                        &probe,
                        &error);
            test->Expect(
                result ==
                        ingress::
                            RawReserveCoordinatorErrorV1::
                                kStatePublishFailure &&
                    ready->receipt == nullptr &&
                    probe.observed &&
                    probe.fault_injected &&
                    ContinuationActionIs(
                        *ready->fixture,
                        ingress::
                            FinalizationActionStateV1::
                                kDebited),
                "state wire race makes PublishNext fail without cached or durable COMPLETE");
        }
    }

    {
        auto ready =
            BuildReadyContinuationReceipt(test);
        if (ready != nullptr) {
            ContinuationCapacityProbe probe;
            probe.fault =
                ContinuationCompletionProbeFault::
                    kDropCoordinatorLeaseBeforeRootSync;
            probe.coordinator =
                ready->fixture->coordinator.get();
            std::string error;
            const auto result =
                ready->fixture->coordinator
                    ->CompleteFinalizationContinuation(
                        std::move(ready->receipt),
                        &probe,
                        &error);
            test->Expect(
                result ==
                        ingress::
                            RawReserveCoordinatorErrorV1::
                                kRootSyncFailure &&
                    ready->receipt == nullptr &&
                    probe.observed &&
                    probe.fault_injected &&
                    ContinuationActionIs(
                        *ready->fixture,
                        ingress::
                            FinalizationActionStateV1::
                                kComplete),
                "root fsync failure is fail-stop after the exact COMPLETE state publication");
        }
    }
}

[[nodiscard]] std::unique_ptr<
    ingress::RawReserveFinalizationActionV1>
HandOffContinuationToReplacement(
    TestContext* test,
    ContinuationE2eFixture* fixture,
    const ingress::ReserveStateV1Identity&
        replacement_executor,
    std::string* error) {
    if (fixture == nullptr ||
        fixture->coordinator == nullptr) {
        return nullptr;
    }

    // Model the original process disappearing: its action gate, in-memory
    // ring, Raw sink, and writer lease all go away before the durable
    // ACTIVE->ACTIVE executor handoff.
    fixture->session.reset();
    fixture->app.reset();
    fixture->sink = nullptr;
    fixture->original_writer_lease.reset();
    fixture->fault.enabled = false;

    const auto replaced =
        fixture->coordinator
            ->ReplaceDebitedContinuationExecutor(
                fixture->continuation_key,
                fixture->route_slug,
                replacement_executor,
                error);
    ingress::RawReserveCoordinatorErrorV1
        acquire_failure{};
    auto action =
        replaced ==
                ingress::
                    RawReserveCoordinatorErrorV1::kNone
            ? fixture->coordinator
                  ->AcquireDebitedFinalizationAction(
                      fixture->continuation_key,
                      fixture->route_slug,
                      &acquire_failure,
                      error)
            : nullptr;
    test->Expect(
        replaced ==
                ingress::
                    RawReserveCoordinatorErrorV1::kNone &&
            action != nullptr &&
            action->executor_instance() ==
                replacement_executor,
        "replacement handoff reacquires the unchanged DEBITED continuation under the new executor");
    return action;
}

[[nodiscard]] std::unique_ptr<
    ingress::RawFinalizationContinuationPosixV1>
BeginReplacementContinuation(
    TestContext* test,
    ContinuationE2eFixture* fixture,
    std::unique_ptr<
        ingress::RawReserveFinalizationActionV1>&&
        action,
    std::string* error) {
    ingress::RawFinalizationContinuationFailureV1
        failure{};
    auto session =
        ingress::RawFinalizationContinuationPosixV1::
            BeginReplacementSealOnly(
                std::move(action),
                ContinuationOptions(fixture),
                &failure,
                error);
    test->Expect(
        session != nullptr,
        "replacement seal-only session adopts the durable candidate without a ring: " +
            (error == nullptr ? std::string{} : *error));
    return session;
}

void TestContinuationReplacementCoordinator(
    TestContext* test) {
    {
        auto ready =
            BuildReadyContinuationReceipt(test);
        if (ready != nullptr) {
            ready->receipt.reset();
            ready->fixture->session.reset();
            const auto before =
                ready->fixture->coordinator->state();
            const auto replacement =
                Pattern<16U>(0xe1U);
            std::string error;
            const auto result =
                ready->fixture->coordinator
                    ->ReplaceDebitedContinuationExecutor(
                        ready->fixture->continuation_key,
                        ready->fixture->route_slug,
                        replacement,
                        &error);
            const auto after =
                ready->fixture->coordinator->state();
            bool exact_transition = false;
            if (before.selected_slot <
                    before.slots.size() &&
                after.selected_slot <
                    after.slots.size()) {
                const auto& old_slot =
                    before.slots[
                        before.selected_slot];
                const auto& new_slot =
                    after.slots[
                        after.selected_slot];
                if (old_slot.active_entry_index <
                        old_slot.entry_count &&
                    new_slot.active_entry_index ==
                        old_slot.active_entry_index) {
                    auto expected =
                        old_slot.entries[
                            old_slot.active_entry_index];
                    expected
                        .executor_or_recovery_attempt =
                        replacement;
                    exact_transition =
                        new_slot.generation ==
                            old_slot.generation + 1U &&
                        new_slot.entries[
                            new_slot
                                .active_entry_index] ==
                            expected &&
                        new_slot
                                .active_fs_free_inode_baseline ==
                            old_slot
                                .active_fs_free_inode_baseline &&
                        new_slot
                                .active_quota_free_inode_baseline ==
                            old_slot
                                .active_quota_free_inode_baseline;
                }
            }
            ingress::RawReserveCoordinatorErrorV1
                acquire_failure{};
            auto action =
                ready->fixture->coordinator
                    ->AcquireDebitedFinalizationAction(
                        ready->fixture
                            ->continuation_key,
                        ready->fixture->route_slug,
                        &acquire_failure,
                        &error);
            test->Expect(
                result ==
                        ingress::
                            RawReserveCoordinatorErrorV1::
                                kNone &&
                    exact_transition &&
                    action != nullptr &&
                    action->executor_instance() ==
                        replacement &&
                    action->debit_generation() ==
                        before
                            .slots[
                                before.selected_slot]
                            .entries[
                                before
                                    .slots[
                                        before
                                            .selected_slot]
                                    .active_entry_index]
                            .actions[
                                ready->fixture
                                    ->continuation_key
                                    .action_id]
                            .debit_generation,
                "replacement handoff changes only current executor and preserves the original DEBITED plan/caps/baselines");
        }
    }

    {
        auto fixture =
            BuildContinuationE2eFixture(
                test, std::nullopt);
        if (fixture != nullptr &&
            fixture->session != nullptr) {
            fixture->session.reset();
            const auto before =
                fixture->coordinator->state();
            std::string error;
            const auto result =
                fixture->coordinator
                    ->ReplaceDebitedContinuationExecutor(
                        fixture->continuation_key,
                        fixture->route_slug,
                        Pattern<16U>(0xe2U),
                        &error);
            test->Expect(
                result !=
                        ingress::
                            RawReserveCoordinatorErrorV1::
                                kNone &&
                    fixture->coordinator->state() ==
                        before,
                "replacement executor is not published when the deterministic continuation candidate is absent");
        }
    }

    {
        auto ready =
            BuildReadyContinuationReceipt(test);
        if (ready != nullptr) {
            ready->receipt.reset();
            ready->fixture->session.reset();
            const int temporary =
                ContinuationOpenFileAt(
                    ready->fixture->route_fd,
                    ".segment-00000002.raw.raw-segment.tmp");
            const bool conflict =
                temporary >= 0 &&
                ::fsync(temporary) == 0 &&
                ::fsync(
                    ready->fixture->route_fd) == 0;
            if (temporary >= 0) {
                static_cast<void>(
                    ::close(temporary));
            }
            const auto before =
                ready->fixture->coordinator->state();
            std::string error;
            const auto result =
                ready->fixture->coordinator
                    ->ReplaceDebitedContinuationExecutor(
                        ready->fixture
                            ->continuation_key,
                        ready->fixture->route_slug,
                        Pattern<16U>(0xe3U),
                        &error);
            test->Expect(
                conflict &&
                    result !=
                        ingress::
                            RawReserveCoordinatorErrorV1::
                                kNone &&
                    ready->fixture->coordinator
                            ->state() ==
                        before,
                "coexisting continuation final and temporary are a causal conflict, not a replacement handoff");
        }
    }

    {
        auto ready =
            BuildReadyContinuationReceipt(test);
        if (ready != nullptr) {
            ready->receipt.reset();
            ready->fixture->session.reset();
            const int higher =
                ContinuationOpenFileAt(
                    ready->fixture->route_fd,
                    "segment-00000003.raw");
            const bool conflict =
                higher >= 0 &&
                ::fsync(higher) == 0 &&
                ::fsync(
                    ready->fixture->route_fd) == 0;
            if (higher >= 0) {
                static_cast<void>(
                    ::close(higher));
            }
            const auto before =
                ready->fixture->coordinator->state();
            std::string error;
            const auto result =
                ready->fixture->coordinator
                    ->ReplaceDebitedContinuationExecutor(
                        ready->fixture
                            ->continuation_key,
                        ready->fixture->route_slug,
                        Pattern<16U>(0xe5U),
                        &error);
            test->Expect(
                conflict &&
                    result !=
                        ingress::
                            RawReserveCoordinatorErrorV1::
                                kNone &&
                    ready->fixture->coordinator
                            ->state() ==
                        before,
                "a higher Raw segment candidate blocks replacement executor publication");
        }
    }

    {
        auto ready =
            BuildReadyContinuationReceipt(test);
        if (ready != nullptr) {
            ready->receipt.reset();
            ready->fixture->session.reset();
            const bool temporary_only =
                ::renameat(
                    ready->fixture->route_fd,
                    "segment-00000002.raw",
                    ready->fixture->route_fd,
                    ".segment-00000002.raw.raw-segment.tmp") ==
                    0 &&
                ::fsync(
                    ready->fixture->route_fd) == 0;
            const auto replacement =
                Pattern<16U>(0xe4U);
            std::string error;
            const auto result =
                ready->fixture->coordinator
                    ->ReplaceDebitedContinuationExecutor(
                        ready->fixture
                            ->continuation_key,
                        ready->fixture->route_slug,
                        replacement,
                        &error);
            const auto state =
                ready->fixture->coordinator->state();
            const auto& slot =
                state.slots[state.selected_slot];
            test->Expect(
                temporary_only &&
                    result ==
                        ingress::
                            RawReserveCoordinatorErrorV1::
                                kNone &&
                    slot.active_entry_index <
                        slot.entry_count &&
                    slot.entries[
                            slot.active_entry_index]
                            .executor_or_recovery_attempt ==
                        replacement,
                "a sole safe deterministic continuation temporary permits replacement without re-debit");
        }
    }
}

void TestContinuationReplacementSealOnlyE2e(
    TestContext* test) {
    {
        auto fixture =
            BuildContinuationE2eFixture(
                test,
                ingress::
                    RawFinalizationContinuationMutationV1::
                        kContinuationRecordWrite);
        if (fixture != nullptr &&
            fixture->session != nullptr) {
            fixture->fault
                .matching_admissions_before_failure =
                1U;
            std::string error;
            const bool original_failed =
                !fixture->session
                     ->CreateOrAdopt(&error) &&
                fixture->fault.fired;
            auto action =
                original_failed
                    ? HandOffContinuationToReplacement(
                          test,
                          fixture.get(),
                          Pattern<16U>(0xf1U),
                          &error)
                    : nullptr;
            auto replacement =
                action != nullptr
                    ? BeginReplacementContinuation(
                          test,
                          fixture.get(),
                          std::move(action),
                          &error)
                    : nullptr;
            auto competitor_during_session =
                replacement != nullptr
                    ? ingress::AcquireRawWriterLeaseAt(
                          fixture->route_fd,
                          fixture->config.recovered
                              .source_stream_id,
                          fixture->config.recovered
                              .capture_date,
                          &error)
                    : nullptr;
            const bool created =
                replacement != nullptr &&
                replacement->CreateOrAdopt(&error);
            auto receipt =
                created
                    ? replacement->TakeReceipt(&error)
                    : nullptr;
            auto competitor_during_receipt =
                receipt != nullptr
                    ? ingress::AcquireRawWriterLeaseAt(
                          fixture->route_fd,
                          fixture->config.recovered
                              .source_stream_id,
                          fixture->config.recovered
                              .capture_date,
                          &error)
                    : nullptr;

            const int descriptor = ::openat(
                fixture->route_fd,
                "segment-00000002.raw",
                O_RDONLY | O_NOFOLLOW | O_NONBLOCK |
                    O_CLOEXEC);
            std::vector<std::byte> bytes;
            const bool read =
                descriptor >= 0 &&
                ContinuationReadAll(
                    descriptor, &bytes);
            if (descriptor >= 0) {
                static_cast<void>(
                    ::close(descriptor));
            }
            auto immutable =
                std::make_shared<
                    const std::vector<std::byte>>(
                        bytes);
            const auto scan =
                ingress::ScanRawSegmentV1(
                    immutable, bytes.size());
            const bool exact_partial =
                read && receipt != nullptr &&
                receipt->Validate(&error) &&
                receipt->completion_mode() ==
                    ingress::
                        RawFinalizationContinuationCompletionModeV1::
                            kReplacementSealOnly &&
                receipt->recovered_record_count() ==
                    1U &&
                receipt->recovered_record_count() <
                    receipt->frozen_record_count() &&
                receipt
                        ->recovered_framed_wal_bytes() ==
                    bytes.size() -
                        ingress::
                            kRawV1SegmentHeaderBytes &&
                scan.ok() &&
                scan.records.size() == 1U &&
                receipt->final_cursor()
                        .segment_offset ==
                    bytes.size();
            ContinuationCapacityProbe probe;
            const auto completed =
                exact_partial
                    ? fixture->coordinator
                          ->CompleteFinalizationContinuation(
                              std::move(receipt),
                              &probe,
                              &error)
                    : ingress::
                          RawReserveCoordinatorErrorV1::
                              kInvalidArgument;
            auto competitor_after_complete =
                completed ==
                        ingress::
                            RawReserveCoordinatorErrorV1::
                                kNone
                    ? ingress::AcquireRawWriterLeaseAt(
                          fixture->route_fd,
                          fixture->config.recovered
                              .source_stream_id,
                          fixture->config.recovered
                              .capture_date,
                          &error)
                    : nullptr;
            test->Expect(
                original_failed &&
                    replacement != nullptr &&
                    competitor_during_session ==
                        nullptr &&
                    created && exact_partial &&
                    competitor_during_receipt ==
                        nullptr &&
                    completed ==
                        ingress::
                            RawReserveCoordinatorErrorV1::
                                kNone &&
                    probe.observed &&
                    competitor_after_complete !=
                        nullptr &&
                    ContinuationActionIs(
                        *fixture,
                        ingress::
                            FinalizationActionStateV1::
                                kComplete),
                "replacement seals exactly the one-record durable prefix, reports the bounded gap, and holds writer.lease through typed COMPLETE");
        }
    }

    {
        auto fixture =
            BuildContinuationE2eFixture(
                test, std::nullopt);
        if (fixture != nullptr &&
            fixture->session != nullptr) {
            std::string error;
            const bool sealed_orphan =
                fixture->session
                    ->CreateOrAdopt(&error);
            auto action =
                sealed_orphan
                    ? HandOffContinuationToReplacement(
                          test,
                          fixture.get(),
                          Pattern<16U>(0xf2U),
                          &error)
                    : nullptr;
            auto replacement =
                action != nullptr
                    ? BeginReplacementContinuation(
                          test,
                          fixture.get(),
                          std::move(action),
                          &error)
                    : nullptr;
            const bool adopted =
                replacement != nullptr &&
                replacement->CreateOrAdopt(&error);
            auto receipt =
                adopted
                    ? replacement->TakeReceipt(&error)
                    : nullptr;
            const bool exact =
                receipt != nullptr &&
                receipt->Validate(&error) &&
                receipt->completion_mode() ==
                    ingress::
                        RawFinalizationContinuationCompletionModeV1::
                            kReplacementSealOnly &&
                receipt->recovered_record_count() ==
                    receipt->frozen_record_count() &&
                receipt
                        ->recovered_framed_wal_bytes() ==
                    receipt
                        ->frozen_framed_wal_bytes();
            ContinuationCapacityProbe probe;
            const auto completed =
                exact
                    ? fixture->coordinator
                          ->CompleteFinalizationContinuation(
                              std::move(receipt),
                              &probe,
                              &error)
                    : ingress::
                          RawReserveCoordinatorErrorV1::
                              kInvalidArgument;
            test->Expect(
                sealed_orphan && adopted && exact &&
                    completed ==
                        ingress::
                            RawReserveCoordinatorErrorV1::
                                kNone &&
                    probe.observed,
                "replacement idempotently adopts an already sealed continuation orphan and completes its unchanged DEBITED receipt");
        }
    }

    {
        auto fixture =
            BuildContinuationE2eFixture(
                test,
                ingress::
                    RawFinalizationContinuationMutationV1::
                        kHeaderMarkerWrite);
        if (fixture != nullptr &&
            fixture->session != nullptr) {
            std::string error;
            const bool candidate_published =
                !fixture->session
                     ->CreateOrAdopt(&error) &&
                fixture->fault.fired;
            auto action =
                candidate_published
                    ? HandOffContinuationToReplacement(
                          test,
                          fixture.get(),
                          Pattern<16U>(0xf3U),
                          &error)
                    : nullptr;
            const bool removed =
                action != nullptr &&
                ::unlinkat(
                    fixture->route_fd,
                    "segment-00000002.raw",
                    0) == 0 &&
                ::fsync(fixture->route_fd) == 0;
            ingress::
                RawFinalizationContinuationFailureV1
                    failure{};
            auto replacement =
                removed
                    ? ingress::
                          RawFinalizationContinuationPosixV1::
                              BeginReplacementSealOnly(
                                  std::move(action),
                                  ContinuationOptions(
                                      fixture.get()),
                                  &failure,
                                  &error)
                    : nullptr;
            struct stat absent {};
            errno = 0;
            test->Expect(
                candidate_published && removed &&
                    replacement == nullptr &&
                    failure ==
                        ingress::
                            RawFinalizationContinuationFailureV1::
                                kContinuationNotRequired &&
                    ::fstatat(
                        fixture->route_fd,
                        "segment-00000002.raw",
                        &absent,
                        AT_SYMLINK_NOFOLLOW) != 0 &&
                    errno == ENOENT &&
                    ContinuationActionIs(
                        *fixture,
                        ingress::
                            FinalizationActionStateV1::
                                kDebited),
                "replacement refuses to create the first continuation inode when the handed-off candidate disappears");
        }
    }

    {
        auto fixture =
            BuildContinuationE2eFixture(
                test,
                ingress::
                    RawFinalizationContinuationMutationV1::
                        kHeaderMarkerWrite);
        if (fixture != nullptr &&
            fixture->session != nullptr) {
            std::string error;
            const bool candidate_published =
                !fixture->session
                     ->CreateOrAdopt(&error) &&
                fixture->fault.fired;
            auto action =
                candidate_published
                    ? HandOffContinuationToReplacement(
                          test,
                          fixture.get(),
                          Pattern<16U>(0xf4U),
                          &error)
                    : nullptr;
            const int temporary =
                action != nullptr
                    ? ContinuationOpenFileAt(
                          fixture->route_fd,
                          ".segment-00000002.raw.raw-segment.tmp")
                    : -1;
            const bool doubled =
                temporary >= 0 &&
                ::fsync(temporary) == 0 &&
                ::fsync(fixture->route_fd) == 0;
            if (temporary >= 0) {
                static_cast<void>(
                    ::close(temporary));
            }
            ingress::
                RawFinalizationContinuationFailureV1
                    failure{};
            auto replacement =
                doubled
                    ? ingress::
                          RawFinalizationContinuationPosixV1::
                              BeginReplacementSealOnly(
                                  std::move(action),
                                  ContinuationOptions(
                                      fixture.get()),
                                  &failure,
                                  &error)
                    : nullptr;
            test->Expect(
                candidate_published && doubled &&
                    replacement == nullptr &&
                    failure ==
                        ingress::
                            RawFinalizationContinuationFailureV1::
                                kNamespaceConflict &&
                    ContinuationActionIs(
                        *fixture,
                        ingress::
                            FinalizationActionStateV1::
                                kDebited),
                "replacement rejects a final-plus-tmp causal conflict without changing the DEBITED action");
        }
    }

    {
        auto fixture =
            BuildContinuationE2eFixture(
                test, std::nullopt);
        if (fixture != nullptr &&
            fixture->session != nullptr) {
            std::string error;
            const bool sealed_orphan =
                fixture->session
                    ->CreateOrAdopt(&error);
            auto action =
                sealed_orphan
                    ? HandOffContinuationToReplacement(
                          test,
                          fixture.get(),
                          Pattern<16U>(0xf5U),
                          &error)
                    : nullptr;
            auto replacement =
                action != nullptr
                    ? BeginReplacementContinuation(
                          test,
                          fixture.get(),
                          std::move(action),
                          &error)
                    : nullptr;
            const bool adopted =
                replacement != nullptr &&
                replacement->CreateOrAdopt(&error);
            auto receipt =
                adopted
                    ? replacement->TakeReceipt(&error)
                    : nullptr;
            const bool initially_valid =
                receipt != nullptr &&
                receipt->Validate(&error);
            const bool tampered =
                initially_valid &&
                FlipContinuationArtifactByte(
                    fixture->route_fd,
                    "segment-00000002.raw",
                    ingress::
                        kRawV1SegmentHeaderBytes);
            ContinuationCapacityProbe probe;
            const auto completed =
                tampered
                    ? fixture->coordinator
                          ->CompleteFinalizationContinuation(
                              std::move(receipt),
                              &probe,
                              &error)
                    : ingress::
                          RawReserveCoordinatorErrorV1::
                              kInvalidArgument;
            auto lease_after_rejection =
                completed ==
                        ingress::
                            RawReserveCoordinatorErrorV1::
                                kTargetIdentityChanged
                    ? ingress::AcquireRawWriterLeaseAt(
                          fixture->route_fd,
                          fixture->config.recovered
                              .source_stream_id,
                          fixture->config.recovered
                              .capture_date,
                          &error)
                    : nullptr;
            test->Expect(
                sealed_orphan && adopted &&
                    initially_valid && tampered &&
                    completed ==
                        ingress::
                            RawReserveCoordinatorErrorV1::
                                kTargetIdentityChanged &&
                    receipt == nullptr &&
                    !probe.observed &&
                    lease_after_rejection != nullptr &&
                    ContinuationActionIs(
                        *fixture,
                        ingress::
                            FinalizationActionStateV1::
                                kDebited),
                "replacement receipt tampering is rejected before capacity/COMPLETE and releases its writer lease only when the receipt is consumed");
        }
    }
}

void TestFinalizationContinuationPosixE2e(
    TestContext* test) {
    static_assert(
        !std::is_default_constructible_v<
            ingress::
                RawFinalizationContinuationReceiptV1>);
    static_assert(
        !std::is_copy_constructible_v<
            ingress::
                RawFinalizationContinuationReceiptV1>);
    static_assert(
        !std::is_move_constructible_v<
            ingress::
                RawFinalizationContinuationReceiptV1>);

    {
        auto invalid =
            BuildContinuationE2eFixture(
                test, std::nullopt, true);
        if (invalid != nullptr) {
            struct stat absent {};
            errno = 0;
            test->Expect(
                invalid->session == nullptr &&
                    ::fstatat(
                        invalid->route_fd,
                        "segment-00000002.raw",
                        &absent,
                        AT_SYMLINK_NOFOLLOW) != 0 &&
                    errno == ENOENT,
                "bad ACK rejection performs no continuation namespace mutation");
        }
    }

    RunContinuationFaultScenario(
        test,
        ingress::
            RawFinalizationContinuationMutationV1::
                kHeaderMarkerWrite,
        "header-marker");
    RunContinuationFaultScenario(
        test,
        ingress::
            RawFinalizationContinuationMutationV1::
                kOpenManifestRename,
        "open-manifest");
    RunContinuationFaultScenario(
        test,
        ingress::
            RawFinalizationContinuationMutationV1::
                kContinuationRecordWrite,
        "record");
    RunContinuationFaultScenario(
        test,
        ingress::
            RawFinalizationContinuationMutationV1::
                kContinuationSealMarkerWrite,
        "seal-marker");

    {
        auto fixture =
            BuildContinuationE2eFixture(
                test, std::nullopt);
        if (fixture != nullptr &&
            fixture->session != nullptr) {
            std::string error;
            const bool created =
                fixture->session
                    ->CreateOrAdopt(&error);
            auto receipt =
                created
                    ? fixture->session
                          ->TakeReceipt(&error)
                    : nullptr;
            const bool valid =
                receipt != nullptr &&
                ValidateContinuationReceiptAndBytes(
                    test,
                    fixture.get(),
                    *receipt);
            const auto generic =
                fixture->coordinator
                    ->CompleteFinalizationAction(
                        fixture->continuation_key,
                        &error);
            const bool generic_preserved_debit =
                ContinuationActionIs(
                    *fixture,
                    ingress::
                        FinalizationActionStateV1::
                            kDebited);
            ContinuationCapacityProbe probe;
            const auto complete =
                valid
                    ? fixture->coordinator
                          ->CompleteFinalizationContinuation(
                              std::move(receipt),
                              &probe,
                              &error)
                    : ingress::
                          RawReserveCoordinatorErrorV1::
                              kInvalidArgument;
            test->Expect(
                created && valid &&
                    generic ==
                        ingress::
                            RawReserveCoordinatorErrorV1::
                                kInvalidArgument &&
                    generic_preserved_debit &&
                    complete ==
                        ingress::
                            RawReserveCoordinatorErrorV1::
                                kNone &&
                    receipt == nullptr &&
                    probe.observed &&
                    probe.last_request.stage ==
                        ingress::
                            RawEmergencyReserveProbeStageV1::
                                kFinalizationPostPublish &&
                    probe.last_request.required_bytes ==
                        13U * 4096U &&
                    probe.last_request.required_inodes ==
                        9U &&
                    ContinuationActionIs(
                        *fixture,
                        ingress::
                            FinalizationActionStateV1::
                                kComplete),
                "only the consumed typed receipt publishes continuation COMPLETE after exact four-dimensional capacity revalidation");
        }
    }

    {
        auto fixture =
            BuildContinuationE2eFixture(
                test, std::nullopt);
        if (fixture != nullptr &&
            fixture->session != nullptr) {
            std::string error;
            const bool created =
                fixture->session
                    ->CreateOrAdopt(&error);
            auto receipt =
                created
                    ? fixture->session
                          ->TakeReceipt(&error)
                    : nullptr;
            const int descriptor = ::openat(
                fixture->route_fd,
                "segment-00000002.raw",
                O_RDWR | O_NOFOLLOW | O_NONBLOCK |
                    O_CLOEXEC);
            std::byte changed{};
            const bool tampered =
                receipt != nullptr &&
                descriptor >= 0 &&
                ::pread(
                    descriptor,
                    &changed,
                    1U,
                    static_cast<off_t>(
                        ingress::
                            kRawV1SegmentHeaderBytes)) ==
                    1 &&
                (changed ^= std::byte{0x01U},
                 ::pwrite(
                     descriptor,
                     &changed,
                     1U,
                     static_cast<off_t>(
                         ingress::
                             kRawV1SegmentHeaderBytes)) ==
                     1) &&
                ::fsync(descriptor) == 0;
            if (descriptor >= 0) {
                static_cast<void>(
                    ::close(descriptor));
            }
            test->Expect(
                created && tampered &&
                    receipt != nullptr &&
                    !receipt->Validate(&error),
                "receipt revalidation rejects sealed continuation body tampering");
        }
    }

    TestContinuationCompletionInputAndBindingNegatives(
        test);
    TestContinuationCompletionArtifactAndSuffixNegatives(
        test);
    TestContinuationCompletionCapacityAndBarrierNegatives(
        test);
    TestContinuationReplacementCoordinator(test);
    TestContinuationReplacementSealOnlyE2e(test);
}
