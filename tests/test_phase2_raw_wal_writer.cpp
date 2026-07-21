#include "l2flow/ingress/raw_v1.h"
#include "l2flow/ingress/raw_recovery.h"
#include "l2flow/ingress/raw_wal_writer.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace ingress = l2flow::ingress;

namespace {

struct TestContext final {
    void Expect(bool condition, const std::string& description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    int failures = 0;
};

enum class OperationKind : std::uint8_t {
    kWrite = 0U,
    kSync,
    kTruncate,
    kClose,
};

struct Operation final {
    OperationKind kind = OperationKind::kWrite;
    ingress::RawWalFile file =
        ingress::RawWalFile::kSegment;
    std::uint64_t offset = 0U;
    std::size_t requested_bytes = 0U;
    std::size_t completed_bytes = 0U;
    int error_number = 0;
};

struct WriteAction final {
    std::size_t maximum_bytes =
        std::numeric_limits<std::size_t>::max();
    int error_number = 0;
    bool zero_success = false;
};

struct MemoryIoState final {
    std::vector<std::byte> segment;
    std::vector<std::byte> journal;
    std::vector<Operation> operations;
    std::vector<WriteAction> write_actions;
    std::size_t next_write_action = 0U;
    std::size_t default_maximum_write =
        std::numeric_limits<std::size_t>::max();
    std::vector<int> segment_sync_results;
    std::vector<int> journal_sync_results;
    std::vector<int> truncate_results;
    std::vector<int> segment_close_results;
    std::vector<int> journal_close_results;
    std::size_t next_segment_sync = 0U;
    std::size_t next_journal_sync = 0U;
    std::size_t next_truncate = 0U;
    std::size_t next_segment_close = 0U;
    std::size_t next_journal_close = 0U;
};

int NextResult(
    const std::vector<int>& results,
    std::size_t* next) {
    if (*next >= results.size()) {
        return 0;
    }
    return results[(*next)++];
}

class MemoryRawWalIo final : public ingress::RawWalIo {
public:
    explicit MemoryRawWalIo(
        std::shared_ptr<MemoryIoState> state)
        : state_(std::move(state)) {}

    ingress::RawWalWriteResult WritevSome(
        ingress::RawWalFile file,
        std::uint64_t offset,
        std::span<const ingress::RawWalIoVector>
            vectors) noexcept override {
        std::size_t requested = 0U;
        for (const ingress::RawWalIoVector& vector :
             vectors) {
            if (vector.bytes.size() >
                std::numeric_limits<std::size_t>::max() -
                    requested) {
                state_->operations.push_back(
                    {OperationKind::kWrite,
                     file,
                     offset,
                     requested,
                     0U,
                     EOVERFLOW});
                return {0U, EOVERFLOW};
            }
            requested += vector.bytes.size();
        }

        WriteAction action;
        action.maximum_bytes =
            state_->default_maximum_write;
        if (state_->next_write_action <
            state_->write_actions.size()) {
            action = state_->write_actions[
                state_->next_write_action++];
        }
        if (action.error_number != 0) {
            state_->operations.push_back(
                {OperationKind::kWrite,
                 file,
                 offset,
                 requested,
                 0U,
                 action.error_number});
            return {0U, action.error_number};
        }
        if (action.zero_success) {
            state_->operations.push_back(
                {OperationKind::kWrite,
                 file,
                 offset,
                 requested,
                 0U,
                 0});
            return {};
        }

        const std::size_t completed =
            std::min(requested, action.maximum_bytes);
        if (offset >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max()) ||
            completed >
                std::numeric_limits<std::size_t>::max() -
                    static_cast<std::size_t>(offset)) {
            state_->operations.push_back(
                {OperationKind::kWrite,
                 file,
                 offset,
                 requested,
                 0U,
                 EOVERFLOW});
            return {0U, EOVERFLOW};
        }
        std::vector<std::byte>& output =
            file == ingress::RawWalFile::kSegment
                ? state_->segment
                : state_->journal;
        const std::size_t start =
            static_cast<std::size_t>(offset);
        const std::size_t end = start + completed;
        if (output.size() < end) {
            output.resize(end);
        }

        std::size_t destination = start;
        std::size_t remaining = completed;
        for (const ingress::RawWalIoVector& vector :
             vectors) {
            if (remaining == 0U) {
                break;
            }
            const std::size_t count =
                std::min(remaining, vector.bytes.size());
            std::copy_n(
                vector.bytes.begin(),
                count,
                output.begin() +
                    static_cast<std::ptrdiff_t>(
                        destination));
            destination += count;
            remaining -= count;
        }
        state_->operations.push_back(
            {OperationKind::kWrite,
             file,
             offset,
             requested,
             completed,
             0});
        return {completed, 0};
    }

    int Fdatasync(
        ingress::RawWalFile file) noexcept override {
        int result = 0;
        if (file == ingress::RawWalFile::kSegment) {
            result = NextResult(
                state_->segment_sync_results,
                &state_->next_segment_sync);
        } else {
            result = NextResult(
                state_->journal_sync_results,
                &state_->next_journal_sync);
        }
        state_->operations.push_back(
            {OperationKind::kSync,
             file,
             0U,
             0U,
             0U,
             result});
        return result;
    }

    int Truncate(
        ingress::RawWalFile file,
        std::uint64_t logical_size) noexcept override {
        const int result = NextResult(
            state_->truncate_results,
            &state_->next_truncate);
        state_->operations.push_back(
            {OperationKind::kTruncate,
             file,
             logical_size,
             0U,
             0U,
             result});
        if (result == 0 &&
            file == ingress::RawWalFile::kSegment &&
            logical_size <=
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max())) {
            state_->segment.resize(
                static_cast<std::size_t>(logical_size));
        }
        return result;
    }

    int Close(
        ingress::RawWalFile file) noexcept override {
        int result = 0;
        if (file == ingress::RawWalFile::kSegment) {
            result = NextResult(
                state_->segment_close_results,
                &state_->next_segment_close);
        } else {
            result = NextResult(
                state_->journal_close_results,
                &state_->next_journal_close);
        }
        state_->operations.push_back(
            {OperationKind::kClose,
             file,
             0U,
             0U,
             0U,
             result});
        return result;
    }

private:
    std::shared_ptr<MemoryIoState> state_;
};

void FillIdentity(
    ingress::RawV1Identity* identity,
    std::uint8_t seed) {
    for (std::size_t index = 0U;
         index < identity->size();
         ++index) {
        (*identity)[index] =
            static_cast<std::byte>(
                static_cast<std::uint8_t>(
                    seed +
                    static_cast<std::uint8_t>(index)));
    }
}

void FillDigest(
    ingress::RawV1Digest* digest,
    std::uint8_t seed) {
    for (std::size_t index = 0U;
         index < digest->size();
         ++index) {
        (*digest)[index] =
            static_cast<std::byte>(
                static_cast<std::uint8_t>(
                    seed +
                    static_cast<std::uint8_t>(index)));
    }
}

ingress::RawWalWriterConfig MakeConfig() {
    ingress::SegmentHeaderV1 segment;
    segment.source_stream_id = 2002U;
    segment.capture_date = 20260718U;
    FillIdentity(&segment.stream_day_id, 0x10U);
    segment.segment_sequence = 1U;
    segment.segment_base_wal_pos = 0U;
    segment.first_ingress_sequence = 1U;
    segment.created_realtime_ns = 1000U;
    segment.created_monotonic_ns = 2000U;
    FillIdentity(&segment.host_uuid, 0x30U);
    FillIdentity(&segment.linux_boot_id, 0x50U);
    segment.clock_epoch_algorithm = 1U;
    FillDigest(&segment.clock_epoch_digest, 0x70U);
    segment.clock_epoch_label = 0x1234U;
    FillDigest(&segment.sdk_archive_sha256, 0x80U);
    FillDigest(&segment.libmdl_api_sha256, 0x90U);
    FillDigest(
        &segment.endpoint_contract_sha256, 0xa0U);
    FillDigest(&segment.config_sha256, 0xb0U);
    FillDigest(&segment.raw_schema_sha256, 0xc0U);
    FillDigest(&segment.build_manifest_sha256, 0xd0U);

    ingress::DurableJournalHeaderV1 journal;
    journal.capture_date = segment.capture_date;
    journal.source_stream_id =
        segment.source_stream_id;
    journal.stream_day_id = segment.stream_day_id;
    journal.raw_schema_sha256 =
        segment.raw_schema_sha256;
    journal.created_host_uuid = segment.host_uuid;
    journal.created_linux_boot_id =
        segment.linux_boot_id;
    journal.created_clock_epoch_algorithm =
        segment.clock_epoch_algorithm;
    journal.created_clock_epoch_digest =
        segment.clock_epoch_digest;
    journal.created_clock_epoch_label =
        segment.clock_epoch_label;

    ingress::RawWalWriterConfig config;
    const ingress::RawV1Error segment_error =
        ingress::EncodeSegmentHeaderV1(
            segment, &config.segment_header_wire);
    const ingress::RawV1Error journal_error =
        ingress::EncodeDurableJournalHeaderV1(
            journal, &config.journal_header_wire);
    if (segment_error != ingress::RawV1Error::kNone ||
        journal_error != ingress::RawV1Error::kNone) {
        throw std::runtime_error(
            "Raw V1 fixture header encoding failed");
    }
    config.source_stream_id =
        segment.source_stream_id;
    config.capture_date = segment.capture_date;
    config.segment_sequence =
        segment.segment_sequence;
    config.segment_base_wal_pos =
        segment.segment_base_wal_pos;
    config.first_ingress_sequence =
        segment.first_ingress_sequence;
    config.initial_durable_ingress_sequence = 0U;
    return config;
}

ingress::SegmentHeaderV1 DecodeSegmentConfig(
    const ingress::RawWalWriterConfig& config) {
    ingress::SegmentHeaderV1 header;
    if (ingress::DecodeSegmentHeaderV1(
            config.segment_header_wire,
            &header) != ingress::RawV1Error::kNone) {
        throw std::runtime_error(
            "Raw V1 fixture segment decoding failed");
    }
    return header;
}

ingress::RawWalWriterConfig MakeRotatedConfig(
    const ingress::RawWalWriterConfig& previous,
    const ingress::RawWalRotationPlan& plan) {
    if (!plan.ok()) {
        throw std::runtime_error(
            "Raw WAL rotation fixture received invalid plan");
    }
    ingress::SegmentHeaderV1 header =
        DecodeSegmentConfig(previous);
    header.segment_sequence =
        plan.next_segment_sequence;
    header.segment_flags = plan.next_segment_flags;
    header.segment_base_wal_pos =
        plan.next_segment_base_wal_pos;
    header.first_ingress_sequence =
        plan.next_first_ingress_sequence;
    header.created_realtime_ns += 1U;
    header.created_monotonic_ns += 1U;

    ingress::RawWalWriterConfig config = previous;
    if (ingress::EncodeSegmentHeaderV1(
            header,
            &config.segment_header_wire) !=
        ingress::RawV1Error::kNone) {
        throw std::runtime_error(
            "Raw WAL rotated header encoding failed");
    }
    config.segment_sequence =
        plan.next_segment_sequence;
    config.segment_base_wal_pos =
        plan.next_segment_base_wal_pos;
    config.first_ingress_sequence =
        plan.next_first_ingress_sequence;
    config.initial_durable_ingress_sequence =
        plan.initial_durable_ingress_sequence;
    config.initialization_mode =
        ingress::RawWalInitializationMode::
            kExistingJournal;
    config.existing_journal = plan.existing_journal;
    return config;
}

void StoreU16(
    std::uint16_t value,
    std::byte* output) {
    output[0] =
        static_cast<std::byte>(value & 0xffU);
    output[1] =
        static_cast<std::byte>((value >> 8U) & 0xffU);
}

void StoreU32(
    std::uint32_t value,
    std::byte* output) {
    for (std::size_t index = 0U; index < 4U; ++index) {
        output[index] =
            static_cast<std::byte>(
                (value >>
                 static_cast<unsigned int>(index * 8U)) &
                0xffU);
    }
}

void StoreU64(
    std::uint64_t value,
    std::byte* output) {
    for (std::size_t index = 0U; index < 8U; ++index) {
        output[index] =
            static_cast<std::byte>(
                (value >>
                 static_cast<unsigned int>(index * 8U)) &
                0xffU);
    }
}

std::array<std::byte, ingress::kVendorMessageHeadBytes>
MakeHead(
    std::size_t body_size,
    std::uint64_t vendor_sequence) {
    std::array<
        std::byte,
        ingress::kVendorMessageHeadBytes> head{};
    head[0] = static_cast<std::byte>(
        ingress::kVendorMessageHeadBytes);
    StoreU32(
        static_cast<std::uint32_t>(
            ingress::kVendorMessageHeadBytes +
            body_size),
        head.data() + 1U);
    head[5] = std::byte{1U};
    head[6] = std::byte{6U};
    StoreU16(101U, head.data() + 7U);
    StoreU16(36U, head.data() + 9U);
    StoreU32(93000123U, head.data() + 11U);
    StoreU64(vendor_sequence, head.data() + 15U);
    return head;
}

struct RecordFixture final {
    ingress::CaptureMetaV1 meta;
    std::array<
        std::byte,
        ingress::kVendorMessageHeadBytes> head;
    std::vector<std::byte> body;

    [[nodiscard]] ingress::RawWalRecordInputV1 input()
        const noexcept {
        return {meta, head, body};
    }
};

class RecordingCommitObserver final
    : public ingress::RawWalCommitObserver {
public:
    bool OnSegmentOpened(
        std::span<const std::byte> wire) noexcept override {
        ++open_calls;
        opened.assign(wire.begin(), wire.end());
        return !fail_open;
    }

    bool OnRecordCommitted(
        std::span<const std::byte> wire,
        std::uint64_t segment_offset,
        std::uint64_t global_wal_pos) noexcept override {
        ++record_calls;
        record.assign(wire.begin(), wire.end());
        record_segment_offset = segment_offset;
        record_global_wal_pos = global_wal_pos;
        return !fail_record;
    }

    bool OnSegmentSealed(
        std::span<const std::byte> wire,
        const ingress::RawWalCursor& cursor) noexcept override {
        ++seal_calls;
        seal.assign(wire.begin(), wire.end());
        sealed_cursor = cursor;
        return !fail_seal;
    }

    bool fail_open = false;
    bool fail_record = false;
    bool fail_seal = false;
    std::size_t open_calls = 0U;
    std::size_t record_calls = 0U;
    std::size_t seal_calls = 0U;
    std::vector<std::byte> opened;
    std::vector<std::byte> record;
    std::vector<std::byte> seal;
    std::uint64_t record_segment_offset = 0U;
    std::uint64_t record_global_wal_pos = 0U;
    ingress::RawWalCursor sealed_cursor{};
};

RecordFixture MakeRecord(
    std::uint64_t ingress_sequence,
    std::size_t body_size = 13U) {
    RecordFixture record;
    record.meta.source_stream_id = 2002U;
    record.meta.connection_epoch_hint = 7U;
    record.meta.ingress_sequence = ingress_sequence;
    record.meta.recv_realtime_ns =
        1'000'000U + ingress_sequence;
    record.meta.recv_monotonic_ns =
        2'000'000U + ingress_sequence;
    record.meta.capture_date = 20260718U;
    record.meta.flags = 0U;
    record.head =
        MakeHead(body_size, 9000U + ingress_sequence);
    record.body.resize(body_size);
    for (std::size_t index = 0U;
         index < record.body.size();
         ++index) {
        record.body[index] =
            static_cast<std::byte>(
                static_cast<std::uint8_t>(
                    index * 7U + 3U));
    }
    return record;
}

std::vector<Operation> BarrierOperations(
    const MemoryIoState& state,
    std::size_t begin) {
    std::vector<Operation> result;
    for (std::size_t index = begin;
         index < state.operations.size();
         ++index) {
        if (state.operations[index].kind !=
            OperationKind::kWrite) {
            result.push_back(state.operations[index]);
        }
    }
    return result;
}

ingress::DurableMarkerV1 DecodeMarker(
    const std::vector<std::byte>& journal,
    std::size_t marker_index) {
    const std::size_t offset =
        ingress::kRawV1JournalHeaderBytes +
        marker_index *
            ingress::kRawV1DurableMarkerBytes;
    ingress::DurableMarkerV1 marker;
    if (offset > journal.size() ||
        journal.size() - offset <
            ingress::kRawV1DurableMarkerBytes ||
        ingress::DecodeDurableMarkerV1(
            std::span<const std::byte>(
                journal.data() + offset,
                ingress::kRawV1DurableMarkerBytes),
            &marker) != ingress::RawV1Error::kNone) {
        throw std::runtime_error(
            "Raw V1 fixture marker decoding failed");
    }
    return marker;
}

void TestHappyOrderingAndSeal(TestContext* test) {
    const auto state =
        std::make_shared<MemoryIoState>();
    state->default_maximum_write = 37U;
    state->write_actions.push_back(
        {0U, EINTR, false});
    ingress::RawWalWriter writer(
        MakeConfig(),
        std::make_unique<MemoryRawWalIo>(state));

    test->Expect(
        writer.Initialize(),
        "initialization retries EINTR and completes");
    ingress::RawWalWriterSnapshot snapshot =
        writer.Snapshot();
    test->Expect(
        snapshot.initialized &&
            !snapshot.fatal &&
            snapshot.append ==
                ingress::RawWalCursor{4096U, 0U, 4096U} &&
            snapshot.durable == snapshot.append &&
            snapshot.journal_logical_size == 4144U,
        "header-only marker publishes exclusive header cursor");
    const std::vector<Operation> startup_barriers =
        BarrierOperations(*state, 0U);
    test->Expect(
        startup_barriers.size() == 3U &&
            startup_barriers[0].kind ==
                OperationKind::kSync &&
            startup_barriers[0].file ==
                ingress::RawWalFile::kJournal &&
            startup_barriers[1].file ==
                ingress::RawWalFile::kSegment &&
            startup_barriers[2].file ==
                ingress::RawWalFile::kJournal,
        "startup orders journal anchor, segment, then marker barriers");
    const ingress::DurableMarkerV1 initial =
        DecodeMarker(state->journal, 0U);
    test->Expect(
        initial.durable_global_wal_pos == 4096U &&
            initial.durable_segment_offset == 4096U &&
            initial.durable_ingress_sequence == 0U &&
            initial.marker_flags == 0U,
        "first marker is the required header-only marker");

    const RecordFixture record = MakeRecord(1U);
    const std::size_t before_append_ops =
        state->operations.size();
    test->Expect(
        writer.AppendRecord(record.input()),
        "one complete record appends through short writes");
    snapshot = writer.Snapshot();
    test->Expect(
        snapshot.append.segment_offset >
            ingress::kRawV1SegmentHeaderBytes &&
            snapshot.append.global_wal_pos ==
                snapshot.append.segment_offset &&
            snapshot.append.ingress_sequence == 1U &&
            snapshot.durable.segment_offset == 4096U,
        "append advances only the append exclusive cursor");

    const Operation& final_append_operation =
        state->operations.back();
    test->Expect(
        final_append_operation.kind ==
                OperationKind::kWrite &&
            final_append_operation.file ==
                ingress::RawWalFile::kSegment &&
            final_append_operation.offset + 16U ==
                snapshot.append.segment_offset &&
            final_append_operation.completed_bytes == 16U,
        "the 16-byte trailer is the final record write");
    const std::span<const std::byte> record_wire(
        state->segment.data() +
            ingress::kRawV1SegmentHeaderBytes,
        state->segment.size() -
            ingress::kRawV1SegmentHeaderBytes);
    const ingress::RawRecordNamespaceV1 record_namespace{
        2002U, 20260718U};
    test->Expect(
        ingress::ValidateRawRecordV1(
            record_wire, &record_namespace) ==
            ingress::RawV1Error::kNone,
        "writer output validates with the independent Raw V1 decoder");
    test->Expect(
        state->operations.size() >
            before_append_ops + 1U,
        "short writes exercise the explicit-offset loop");

    const std::size_t before_flush_ops =
        state->operations.size();
    test->Expect(
        writer.FlushDurable(),
        "runtime durability batch completes");
    snapshot = writer.Snapshot();
    test->Expect(
        snapshot.durable == snapshot.append,
        "durable cursor publishes only after journal sync");
    const std::vector<Operation> flush_barriers =
        BarrierOperations(*state, before_flush_ops);
    test->Expect(
        flush_barriers.size() == 2U &&
            flush_barriers[0].file ==
                ingress::RawWalFile::kSegment &&
            flush_barriers[1].file ==
                ingress::RawWalFile::kJournal,
        "runtime barrier is segment sync then journal sync");
    const ingress::DurableMarkerV1 runtime =
        DecodeMarker(state->journal, 1U);
    test->Expect(
        runtime.durable_segment_offset ==
                snapshot.durable.segment_offset &&
            runtime.durable_global_wal_pos ==
                snapshot.durable.global_wal_pos &&
            runtime.durable_ingress_sequence == 1U &&
            runtime.marker_flags == 0U,
        "runtime marker records exact exclusive cursors");

    state->truncate_results = {EINTR, 0};
    const std::size_t before_seal_ops =
        state->operations.size();
    test->Expect(
        writer.SealAndClose(),
        "clean close retries truncate EINTR and seals");
    snapshot = writer.Snapshot();
    test->Expect(
        snapshot.sealed &&
            snapshot.closed &&
            !snapshot.fatal &&
            state->segment.size() ==
                snapshot.append.segment_offset,
        "clean close truncates to logical end and closes");
    const ingress::DurableMarkerV1 sealed =
        DecodeMarker(state->journal, 2U);
    test->Expect(
        sealed.marker_flags ==
                ingress::kRawV1SegmentSealed &&
            sealed.durable_segment_offset ==
                snapshot.append.segment_offset &&
            sealed.durable_ingress_sequence == 1U,
        "final marker is authoritative SEGMENT_SEALED");
    const std::vector<Operation> seal_barriers =
        BarrierOperations(*state, before_seal_ops);
    test->Expect(
        seal_barriers.size() == 6U &&
            seal_barriers[0].kind ==
                OperationKind::kTruncate &&
            seal_barriers[0].error_number == EINTR &&
            seal_barriers[1].kind ==
                OperationKind::kTruncate &&
            seal_barriers[2].kind ==
                OperationKind::kSync &&
            seal_barriers[2].file ==
                ingress::RawWalFile::kSegment &&
            seal_barriers[3].kind ==
                OperationKind::kSync &&
            seal_barriers[3].file ==
                ingress::RawWalFile::kJournal &&
            seal_barriers[4].kind ==
                OperationKind::kClose &&
            seal_barriers[5].kind ==
                OperationKind::kClose,
        "seal orders truncate, segment sync, marker sync, and closes");
}

void TestTrailerFailureDoesNotPublish(TestContext* test) {
    const auto state =
        std::make_shared<MemoryIoState>();
    ingress::RawWalWriter writer(
        MakeConfig(),
        std::make_unique<MemoryRawWalIo>(state));
    test->Expect(writer.Initialize(), "fault fixture initializes");
    state->write_actions = {
        {std::numeric_limits<std::size_t>::max(), 0, false},
        {0U, EIO, false}};
    state->next_write_action = 0U;

    const RecordFixture record = MakeRecord(1U);
    test->Expect(
        !writer.AppendRecord(record.input()),
        "trailer write error fails the record");
    const ingress::RawWalWriterSnapshot snapshot =
        writer.Snapshot();
    test->Expect(
        snapshot.fatal &&
            snapshot.append ==
                ingress::RawWalCursor{4096U, 0U, 4096U} &&
            snapshot.durable == snapshot.append &&
            writer.failure().kind ==
                ingress::RawWalFailureKind::kSegmentWrite &&
            writer.failure().error_number == EIO,
        "partial record never advances append or durable cursor");
    const std::size_t operation_count =
        state->operations.size();
    test->Expect(
        !writer.FlushDurable() &&
            state->operations.size() == operation_count,
        "fatal latch prevents later durability mutation");
}

void TestZeroWriteIsFatal(TestContext* test) {
    const auto state =
        std::make_shared<MemoryIoState>();
    ingress::RawWalWriter writer(
        MakeConfig(),
        std::make_unique<MemoryRawWalIo>(state));
    test->Expect(writer.Initialize(), "zero-write fixture initializes");
    state->write_actions = {{0U, 0, true}};
    state->next_write_action = 0U;
    const RecordFixture record = MakeRecord(1U);
    test->Expect(
        !writer.AppendRecord(record.input()) &&
            writer.failure().kind ==
                ingress::RawWalFailureKind::kSegmentWrite &&
            writer.failure().error_number == EIO,
        "zero-byte success on a nonempty write is fatal");
}

void TestSegmentSyncFailureDoesNotWriteMarker(
    TestContext* test) {
    const auto state =
        std::make_shared<MemoryIoState>();
    ingress::RawWalWriter writer(
        MakeConfig(),
        std::make_unique<MemoryRawWalIo>(state));
    test->Expect(writer.Initialize(), "sync fault fixture initializes");
    const RecordFixture record = MakeRecord(1U);
    test->Expect(writer.AppendRecord(record.input()), "record appends");
    const std::size_t journal_before =
        state->journal.size();
    state->segment_sync_results = {EIO};
    state->next_segment_sync = 0U;
    test->Expect(
        !writer.FlushDurable(),
        "segment sync failure aborts durability batch");
    const ingress::RawWalWriterSnapshot snapshot =
        writer.Snapshot();
    test->Expect(
        writer.failure().kind ==
                ingress::RawWalFailureKind::kSegmentSync &&
            snapshot.durable.segment_offset == 4096U &&
            snapshot.append.segment_offset > 4096U &&
            state->journal.size() == journal_before,
        "marker is not written and durable does not advance");
}

void TestPartialMarkerFailureDoesNotPublish(
    TestContext* test) {
    const auto state =
        std::make_shared<MemoryIoState>();
    ingress::RawWalWriter writer(
        MakeConfig(),
        std::make_unique<MemoryRawWalIo>(state));
    test->Expect(writer.Initialize(), "marker fault fixture initializes");
    const RecordFixture record = MakeRecord(1U);
    test->Expect(writer.AppendRecord(record.input()), "record appends");
    const std::size_t journal_before =
        state->journal.size();
    state->write_actions = {
        {7U, 0, false},
        {0U, ENOSPC, false}};
    state->next_write_action = 0U;
    test->Expect(
        !writer.FlushDurable(),
        "partial marker followed by ENOSPC is fatal");
    const ingress::RawWalWriterSnapshot snapshot =
        writer.Snapshot();
    test->Expect(
        writer.failure().kind ==
                ingress::RawWalFailureKind::kJournalWrite &&
            writer.failure().error_number == ENOSPC &&
            snapshot.durable.segment_offset == 4096U &&
            snapshot.journal_logical_size ==
                journal_before &&
            state->journal.size() == journal_before + 7U,
        "partial marker is not published as a logical marker");
}

void TestJournalSyncFailureDoesNotPublish(
    TestContext* test) {
    const auto state =
        std::make_shared<MemoryIoState>();
    ingress::RawWalWriter writer(
        MakeConfig(),
        std::make_unique<MemoryRawWalIo>(state));
    test->Expect(writer.Initialize(), "journal fault fixture initializes");
    const RecordFixture record = MakeRecord(1U);
    test->Expect(writer.AppendRecord(record.input()), "record appends");
    state->journal_sync_results = {EIO};
    state->next_journal_sync = 0U;
    test->Expect(
        !writer.FlushDurable(),
        "journal sync failure is fatal");
    const ingress::RawWalWriterSnapshot snapshot =
        writer.Snapshot();
    test->Expect(
        writer.failure().kind ==
                ingress::RawWalFailureKind::kJournalSync &&
            snapshot.durable.segment_offset == 4096U &&
            snapshot.append.segment_offset > 4096U &&
            snapshot.journal_logical_size == 4192U &&
            state->journal.size() == 4192U,
        "complete unsynced marker bytes do not publish durable cursor");
}

void TestInvalidRecordLatchesBeforeIo(TestContext* test) {
    const auto state =
        std::make_shared<MemoryIoState>();
    ingress::RawWalWriter writer(
        MakeConfig(),
        std::make_unique<MemoryRawWalIo>(state));
    test->Expect(writer.Initialize(), "invalid fixture initializes");
    RecordFixture record = MakeRecord(2U);
    const std::size_t operations_before =
        state->operations.size();
    test->Expect(
        !writer.AppendRecord(record.input()) &&
            writer.failure().kind ==
                ingress::RawWalFailureKind::kInvalidRecord &&
            state->operations.size() ==
                operations_before,
        "sequence gap fails before any segment mutation");
}

void ExpectInitializationRejectedBeforeIo(
    TestContext* test,
    ingress::RawWalWriterConfig config,
    const std::string& description) {
    const auto state =
        std::make_shared<MemoryIoState>();
    ingress::RawWalWriter writer(
        std::move(config),
        std::make_unique<MemoryRawWalIo>(state));
    test->Expect(
        !writer.Initialize() &&
            writer.failure().kind ==
                ingress::RawWalFailureKind::
                    kInvalidConfiguration &&
            state->operations.empty(),
        description);
}

ingress::RawWalWriterSnapshot MakeSealedSnapshot(
    std::uint64_t global_wal_pos,
    std::uint64_t ingress_sequence,
    std::uint64_t segment_offset,
    std::uint64_t journal_logical_size = 4192U) {
    ingress::RawWalWriterSnapshot snapshot;
    snapshot.append = {
        global_wal_pos,
        ingress_sequence,
        segment_offset};
    snapshot.durable = snapshot.append;
    snapshot.journal_logical_size =
        journal_logical_size;
    snapshot.initialized = true;
    snapshot.sealed = true;
    snapshot.closed = true;
    return snapshot;
}

void TestNormalRotationChain(TestContext* test) {
    const auto state =
        std::make_shared<MemoryIoState>();
    const ingress::RawWalWriterConfig config1 =
        MakeConfig();
    ingress::RawWalWriter writer1(
        config1,
        std::make_unique<MemoryRawWalIo>(state));
    test->Expect(
        writer1.Initialize(),
        "rotation segment 1 initializes");
    const RecordFixture record1 = MakeRecord(1U);
    test->Expect(
        writer1.AppendRecord(record1.input()) &&
            writer1.SealAndClose(),
        "rotation segment 1 appends and seals");
    const ingress::RawWalWriterSnapshot sealed1 =
        writer1.Snapshot();
    const ingress::SegmentHeaderV1 header1 =
        DecodeSegmentConfig(config1);
    const ingress::RawWalRotationPlan rotation1 =
        ingress::PlanRawWalRotation(
            header1, sealed1);
    test->Expect(
        rotation1.ok() &&
            rotation1.next_segment_sequence == 2U &&
            rotation1.next_segment_flags == 0U &&
            rotation1.next_segment_base_wal_pos ==
                sealed1.durable.global_wal_pos &&
            rotation1.next_first_ingress_sequence == 2U &&
            rotation1
                    .initial_durable_ingress_sequence ==
                1U &&
            rotation1.existing_journal
                    .journal_append_offset ==
                sealed1.journal_logical_size,
        "sealed segment 1 derives an exact normal rotation plan");

    const std::vector<std::byte> segment1 =
        state->segment;
    const std::vector<std::byte> journal_anchor(
        state->journal.begin(),
        state->journal.begin() +
            static_cast<std::ptrdiff_t>(
                ingress::kRawV1JournalHeaderBytes));
    const std::uint64_t segment2_marker_offset =
        sealed1.journal_logical_size;
    const std::size_t segment2_operation_begin =
        state->operations.size();
    state->segment.clear();

    const ingress::RawWalWriterConfig config2 =
        MakeRotatedConfig(config1, rotation1);
    ingress::RawWalWriter writer2(
        config2,
        std::make_unique<MemoryRawWalIo>(state));
    test->Expect(
        writer2.Initialize(),
        "rotation segment 2 attaches to the verified journal");
    bool journal_prefix_rewritten = false;
    bool header_only_marker_appended = false;
    for (std::size_t index = segment2_operation_begin;
         index < state->operations.size();
         ++index) {
        const Operation& operation =
            state->operations[index];
        if (operation.kind == OperationKind::kWrite &&
            operation.file ==
                ingress::RawWalFile::kJournal) {
            journal_prefix_rewritten =
                journal_prefix_rewritten ||
                operation.offset <
                    segment2_marker_offset;
            header_only_marker_appended =
                header_only_marker_appended ||
                operation.offset ==
                    segment2_marker_offset;
        }
    }
    test->Expect(
        !journal_prefix_rewritten &&
            header_only_marker_appended &&
            std::equal(
                journal_anchor.begin(),
                journal_anchor.end(),
                state->journal.begin()),
        "existing-journal initialization never rewrites its header");

    ingress::SegmentHeaderV1 decoded2;
    test->Expect(
        ingress::DecodeSegmentHeaderV1(
            std::span<const std::byte>(
                state->segment.data(),
                ingress::kRawV1SegmentHeaderBytes),
            &decoded2) == ingress::RawV1Error::kNone &&
            decoded2.segment_sequence == 2U &&
            decoded2.segment_flags == 0U &&
            decoded2.segment_base_wal_pos ==
                sealed1.durable.global_wal_pos &&
            decoded2.first_ingress_sequence == 2U,
        "segment 2 header is a contiguous normal segment");
    const std::size_t segment2_marker_index =
        static_cast<std::size_t>(
            (segment2_marker_offset -
             ingress::kRawV1JournalHeaderBytes) /
            ingress::kRawV1DurableMarkerBytes);
    const ingress::DurableMarkerV1 segment2_initial =
        DecodeMarker(
            state->journal, segment2_marker_index);
    test->Expect(
        segment2_initial.segment_sequence == 2U &&
            segment2_initial.durable_segment_offset ==
                ingress::kRawV1SegmentHeaderBytes &&
            segment2_initial.durable_global_wal_pos ==
                decoded2.segment_base_wal_pos +
                    ingress::kRawV1SegmentHeaderBytes &&
            segment2_initial
                    .durable_ingress_sequence ==
                sealed1.durable.ingress_sequence &&
            segment2_initial.marker_flags == 0U,
        "segment 2 starts with the required header-only marker");

    const RecordFixture record2 = MakeRecord(2U);
    test->Expect(
        writer2.AppendRecord(record2.input()) &&
            writer2.SealAndClose(),
        "rotation segment 2 appends and seals");
    const ingress::RawWalWriterSnapshot sealed2 =
        writer2.Snapshot();
    const ingress::RawWalRotationPlan rotation2 =
        ingress::PlanRawWalRotation(
            decoded2, sealed2);
    test->Expect(
        rotation2.ok() &&
            rotation2.next_segment_sequence == 3U &&
            rotation2.next_segment_base_wal_pos ==
                decoded2.segment_base_wal_pos +
                    sealed2.durable.segment_offset &&
            rotation2.next_first_ingress_sequence == 3U,
        "segment 2 derives a continuous segment 3 plan");

    const std::vector<std::byte> segment2 =
        state->segment;
    const std::uint64_t segment3_marker_offset =
        sealed2.journal_logical_size;
    state->segment.clear();
    const ingress::RawWalWriterConfig config3 =
        MakeRotatedConfig(config2, rotation2);
    ingress::RawWalWriter writer3(
        config3,
        std::make_unique<MemoryRawWalIo>(state));
    test->Expect(
        writer3.Initialize(),
        "rotation segment 3 attaches to the same journal");
    const RecordFixture record3 = MakeRecord(3U);
    test->Expect(
        writer3.AppendRecord(record3.input()) &&
            writer3.SealAndClose(),
        "rotation segment 3 appends and seals");
    const ingress::RawWalWriterSnapshot sealed3 =
        writer3.Snapshot();
    const std::vector<std::byte> segment3 =
        state->segment;
    const ingress::SegmentHeaderV1 header3 =
        DecodeSegmentConfig(config3);
    const std::size_t segment3_marker_index =
        static_cast<std::size_t>(
            (segment3_marker_offset -
             ingress::kRawV1JournalHeaderBytes) /
            ingress::kRawV1DurableMarkerBytes);
    const ingress::DurableMarkerV1 segment3_initial =
        DecodeMarker(
            state->journal, segment3_marker_index);
    test->Expect(
        header3.segment_flags == 0U &&
            header3.segment_base_wal_pos ==
                decoded2.segment_base_wal_pos +
                    static_cast<std::uint64_t>(
                        segment2.size()) &&
            segment3_initial.segment_sequence == 3U &&
            segment3_initial.durable_global_wal_pos ==
                header3.segment_base_wal_pos +
                    ingress::kRawV1SegmentHeaderBytes &&
            segment3_initial
                    .durable_ingress_sequence ==
                sealed2.durable.ingress_sequence,
        "third segment preserves exact base and ingress continuity");

    ingress::RawRecoveryInputV1 recovery_input;
    recovery_input.journal =
        std::make_shared<const std::vector<std::byte>>(
            state->journal);
    recovery_input.segments.push_back(
        {std::make_shared<
            const std::vector<std::byte>>(segment1)});
    recovery_input.segments.push_back(
        {std::make_shared<
            const std::vector<std::byte>>(segment2)});
    recovery_input.segments.push_back(
        {std::make_shared<
            const std::vector<std::byte>>(segment3)});
    const ingress::RawRecoveryPlanV1 recovery =
        ingress::AnalyzeRawRecoveryV1(recovery_input);
    test->Expect(
        recovery.ok() &&
            recovery.accepted_journal_size ==
                sealed3.journal_logical_size &&
            recovery.has_accepted_cursor &&
            recovery.accepted_cursor.segment_sequence ==
                3U &&
            recovery.accepted_cursor.marker_flags ==
                ingress::kRawV1SegmentSealed &&
            recovery.segments.size() == 3U,
        "recovery accepts the complete three-segment marker chain");
}

void TestRotationConfigurationRejections(
    TestContext* test) {
    ingress::SegmentHeaderV1 previous_header =
        DecodeSegmentConfig(MakeConfig());
    const ingress::RawWalWriterSnapshot sealed =
        MakeSealedSnapshot(5000U, 1U, 5000U);
    const ingress::RawWalRotationPlan plan =
        ingress::PlanRawWalRotation(
            previous_header, sealed);
    test->Expect(
        plan.ok(),
        "configuration rejection fixture has a valid plan");
    const ingress::RawWalWriterConfig base =
        MakeRotatedConfig(MakeConfig(), plan);

    ingress::RawWalWriterConfig gap = base;
    ingress::SegmentHeaderV1 gap_header =
        DecodeSegmentConfig(gap);
    gap_header.segment_sequence += 1U;
    gap.segment_sequence =
        gap_header.segment_sequence;
    test->Expect(
        ingress::EncodeSegmentHeaderV1(
            gap_header,
            &gap.segment_header_wire) ==
            ingress::RawV1Error::kNone,
        "gap fixture header encodes");
    ExpectInitializationRejectedBeforeIo(
        test,
        std::move(gap),
        "existing-journal initialization rejects a segment gap");

    ingress::RawWalWriterConfig wrong_base = base;
    ingress::SegmentHeaderV1 wrong_base_header =
        DecodeSegmentConfig(wrong_base);
    wrong_base_header.segment_base_wal_pos += 8U;
    wrong_base.segment_base_wal_pos =
        wrong_base_header.segment_base_wal_pos;
    test->Expect(
        ingress::EncodeSegmentHeaderV1(
            wrong_base_header,
            &wrong_base.segment_header_wire) ==
            ingress::RawV1Error::kNone,
        "wrong-base fixture header encodes");
    ExpectInitializationRejectedBeforeIo(
        test,
        std::move(wrong_base),
        "existing-journal initialization rejects a wrong base");

    ingress::RawWalWriterConfig wrong_offset = base;
    wrong_offset.existing_journal
        .journal_append_offset += 1U;
    ExpectInitializationRejectedBeforeIo(
        test,
        std::move(wrong_offset),
        "existing-journal initialization rejects an unaligned append offset");

    ingress::RawWalWriterConfig wrong_first = base;
    ingress::SegmentHeaderV1 wrong_first_header =
        DecodeSegmentConfig(wrong_first);
    wrong_first_header.first_ingress_sequence += 1U;
    wrong_first.first_ingress_sequence =
        wrong_first_header.first_ingress_sequence;
    test->Expect(
        ingress::EncodeSegmentHeaderV1(
            wrong_first_header,
            &wrong_first.segment_header_wire) ==
            ingress::RawV1Error::kNone,
        "wrong-first fixture header encodes");
    ExpectInitializationRejectedBeforeIo(
        test,
        std::move(wrong_first),
        "rotation rejects first-ingress mismatch");

    ingress::RawWalWriterConfig wrong_initial = base;
    wrong_initial.initial_durable_ingress_sequence += 1U;
    ExpectInitializationRejectedBeforeIo(
        test,
        std::move(wrong_initial),
        "rotation rejects initial-durable mismatch");

    ingress::RawWalWriterConfig continuation = base;
    ingress::SegmentHeaderV1 continuation_header =
        DecodeSegmentConfig(continuation);
    continuation_header.segment_flags =
        ingress::kRawV1FinalizationContinuation;
    FillIdentity(
        &continuation_header.reserve_state_uuid,
        0xe0U);
    FillIdentity(
        &continuation_header.finalization_cycle_id,
        0xf0U);
    FillDigest(
        &continuation_header.immutable_grant_sha256,
        0x20U);
    test->Expect(
        ingress::EncodeSegmentHeaderV1(
            continuation_header,
            &continuation.segment_header_wire) ==
            ingress::RawV1Error::kNone,
        "continuation fixture is schema-valid");
    ExpectInitializationRejectedBeforeIo(
        test,
        std::move(continuation),
        "normal rotation never creates a finalization continuation");
}

void TestRotationPlanRejections(TestContext* test) {
    ingress::SegmentHeaderV1 header =
        DecodeSegmentConfig(MakeConfig());
    ingress::RawWalWriterSnapshot snapshot =
        MakeSealedSnapshot(4096U, 0U, 4096U);
    snapshot.closed = false;
    test->Expect(
        ingress::PlanRawWalRotation(
            header, snapshot).error ==
            ingress::RawWalRotationError::
                kSnapshotNotSealed,
        "rotation plan requires a closed sealed snapshot");

    snapshot =
        MakeSealedSnapshot(4096U, 0U, 4096U, 4097U);
    test->Expect(
        ingress::PlanRawWalRotation(
            header, snapshot).error ==
            ingress::RawWalRotationError::
                kJournalOffsetInvalid,
        "rotation plan rejects an unaligned journal end");

    header.segment_sequence =
        std::numeric_limits<std::uint32_t>::max();
    header.segment_base_wal_pos = 4096U;
    header.first_ingress_sequence = 2U;
    snapshot =
        MakeSealedSnapshot(8192U, 1U, 4096U);
    test->Expect(
        ingress::PlanRawWalRotation(
            header, snapshot).error ==
            ingress::RawWalRotationError::
                kSegmentSequenceOverflow,
        "rotation plan rejects uint32 segment overflow");

    header.segment_sequence = 2U;
    header.segment_base_wal_pos =
        std::numeric_limits<std::uint64_t>::max() -
        ingress::kRawV1SegmentHeaderBytes;
    snapshot = MakeSealedSnapshot(
        std::numeric_limits<std::uint64_t>::max(),
        1U,
        ingress::kRawV1SegmentHeaderBytes);
    test->Expect(
        ingress::PlanRawWalRotation(
            header, snapshot).error ==
            ingress::RawWalRotationError::
                kWalPositionOverflow,
        "rotation plan reserves address space for the next header");

    header = DecodeSegmentConfig(MakeConfig());
    snapshot = MakeSealedSnapshot(
        ingress::kRawV1SegmentHeaderBytes,
        std::numeric_limits<std::uint64_t>::max(),
        ingress::kRawV1SegmentHeaderBytes);
    test->Expect(
        ingress::PlanRawWalRotation(
            header, snapshot).error ==
            ingress::RawWalRotationError::
                kIngressSequenceOverflow,
        "rotation plan rejects uint64 ingress overflow");

    header.segment_sequence = 2U;
    header.segment_flags =
        ingress::kRawV1FinalizationContinuation;
    header.segment_base_wal_pos = 4096U;
    header.first_ingress_sequence = 2U;
    FillIdentity(&header.reserve_state_uuid, 0xe0U);
    FillIdentity(&header.finalization_cycle_id, 0xf0U);
    FillDigest(&header.immutable_grant_sha256, 0x20U);
    snapshot = MakeSealedSnapshot(8192U, 1U, 4096U);
    test->Expect(
        ingress::PlanRawWalRotation(
            header, snapshot).error ==
            ingress::RawWalRotationError::
                kFinalizationContinuationUnsupported,
        "normal rotation helper rejects finalization continuation input");
}

void TestFactoryPersistedHeadersAreNotRewritten(
    TestContext* test) {
    const auto state =
        std::make_shared<MemoryIoState>();
    ingress::RawWalWriterConfig config = MakeConfig();
    config.headers_already_persisted = true;
    state->segment.assign(
        config.segment_header_wire.begin(),
        config.segment_header_wire.end());
    state->journal.assign(
        config.journal_header_wire.begin(),
        config.journal_header_wire.end());

    ingress::RawWalWriter writer(
        config,
        std::make_unique<MemoryRawWalIo>(state));
    test->Expect(
        writer.Initialize(),
        "factory-persisted writer initializes");
    bool rewrote_segment_header = false;
    bool rewrote_journal_header = false;
    bool wrote_initial_marker = false;
    for (const Operation& operation : state->operations) {
        if (operation.kind != OperationKind::kWrite) {
            continue;
        }
        rewrote_segment_header =
            rewrote_segment_header ||
            (operation.file ==
                 ingress::RawWalFile::kSegment &&
             operation.offset <
                 ingress::kRawV1SegmentHeaderBytes);
        rewrote_journal_header =
            rewrote_journal_header ||
            (operation.file ==
                 ingress::RawWalFile::kJournal &&
             operation.offset <
                 ingress::kRawV1JournalHeaderBytes);
        wrote_initial_marker =
            wrote_initial_marker ||
            (operation.file ==
                 ingress::RawWalFile::kJournal &&
             operation.offset ==
                 ingress::kRawV1JournalHeaderBytes);
    }
    test->Expect(
        !rewrote_segment_header &&
            !rewrote_journal_header &&
            wrote_initial_marker,
        "published factory headers are never rewritten before initial marker");
    test->Expect(
        writer.SealAndClose(),
        "factory-persisted empty segment seals cleanly");
}

void TestRecoveredOpenIsSealOnly(
    TestContext* test) {
    const auto recovered_state =
        std::make_shared<MemoryIoState>();
    const ingress::RawWalWriterConfig original_config =
        MakeConfig();
    ingress::RawWalWriterSnapshot recovered_snapshot;
    {
        ingress::RawWalWriter original(
            original_config,
            std::make_unique<MemoryRawWalIo>(
                recovered_state));
        const RecordFixture record = MakeRecord(1U);
        test->Expect(
            original.Initialize() &&
                original.AppendRecord(record.input()) &&
                original.FlushDurable(),
            "recovered-seal fixture creates one accepted open-segment record");
        recovered_snapshot = original.Snapshot();
        test->Expect(
            recovered_snapshot.initialized &&
                !recovered_snapshot.sealed &&
                recovered_snapshot.append ==
                    recovered_snapshot.durable &&
                recovered_snapshot.durable
                        .ingress_sequence == 1U,
            "recovered-seal fixture ends at an exact durable open boundary");
    }

    ingress::RawWalWriterConfig resume_config =
        original_config;
    resume_config.initialization_mode =
        ingress::RawWalInitializationMode::
            kRecoveredSealOnly;
    resume_config.initial_durable_ingress_sequence =
        recovered_snapshot.durable.ingress_sequence;
    resume_config.headers_already_persisted = true;
    resume_config.recovered_open.journal_append_offset =
        recovered_snapshot.journal_logical_size;
    resume_config.recovered_open.recovered_cursor =
        recovered_snapshot.durable;

    const auto misuse_state =
        std::make_shared<MemoryIoState>(
            *recovered_state);
    const std::size_t misuse_segment_size =
        misuse_state->segment.size();
    const std::size_t misuse_journal_size =
        misuse_state->journal.size();
    {
        ingress::RawWalWriter misuse(
            resume_config,
            std::make_unique<MemoryRawWalIo>(
                misuse_state));
        const RecordFixture next_record = MakeRecord(2U);
        test->Expect(
            misuse.Initialize() &&
                !misuse.AppendRecord(next_record.input()) &&
                misuse.failure().kind ==
                    ingress::RawWalFailureKind::
                        kInvalidState &&
                misuse_state->segment.size() ==
                    misuse_segment_size &&
                misuse_state->journal.size() ==
                    misuse_journal_size,
            "recovered-open mode rejects record append without mutating either file");
    }

    const std::size_t initialize_operation_begin =
        recovered_state->operations.size();
    ingress::RawWalWriter resumed(
        resume_config,
        std::make_unique<MemoryRawWalIo>(
            recovered_state));
    test->Expect(
        resumed.Initialize() &&
            recovered_state->operations.size() ==
                initialize_operation_begin &&
            resumed.Snapshot().append ==
                recovered_snapshot.durable &&
            resumed.Snapshot().durable ==
                recovered_snapshot.durable &&
            resumed.Snapshot().journal_logical_size ==
                recovered_snapshot.journal_logical_size,
        "recovered-open initialization publishes the accepted cursor with zero I/O");
    test->Expect(
        resumed.SealAndClose(),
        "recovered-open mode seals the old segment");
    const ingress::RawWalWriterSnapshot sealed =
        resumed.Snapshot();
    const std::size_t seal_marker_index =
        static_cast<std::size_t>(
            (recovered_snapshot.journal_logical_size -
             ingress::kRawV1JournalHeaderBytes) /
            ingress::kRawV1DurableMarkerBytes);
    const ingress::DurableMarkerV1 seal =
        DecodeMarker(
            recovered_state->journal,
            seal_marker_index);
    test->Expect(
        sealed.sealed &&
            sealed.closed &&
            sealed.append == recovered_snapshot.durable &&
            sealed.durable == recovered_snapshot.durable &&
            sealed.journal_logical_size ==
                recovered_snapshot.journal_logical_size +
                    ingress::kRawV1DurableMarkerBytes &&
            seal.segment_sequence ==
                resume_config.segment_sequence &&
            seal.durable_global_wal_pos ==
                recovered_snapshot.durable
                    .global_wal_pos &&
            seal.durable_ingress_sequence ==
                recovered_snapshot.durable
                    .ingress_sequence &&
            seal.durable_segment_offset ==
                recovered_snapshot.durable
                    .segment_offset &&
            seal.marker_flags ==
                ingress::kRawV1SegmentSealed,
        "recovered seal appends one exact SEGMENT_SEALED marker without moving the cursor");

    ingress::RawWalWriterConfig invalid = resume_config;
    invalid.headers_already_persisted = false;
    ExpectInitializationRejectedBeforeIo(
        test,
        invalid,
        "recovered-open mode requires retained persisted headers");
    invalid = resume_config;
    invalid.recovered_open.recovered_cursor
        .global_wal_pos += 1U;
    ExpectInitializationRejectedBeforeIo(
        test,
        invalid,
        "recovered-open mode rejects a non-contiguous global cursor");
    invalid = resume_config;
    invalid.recovered_open.accepted_marker_flags =
        ingress::kRawV1SegmentSealed;
    ExpectInitializationRejectedBeforeIo(
        test,
        invalid,
        "recovered-open mode rejects an already sealed accepted marker");
    invalid = resume_config;
    invalid.recovered_open.journal_append_offset -= 1U;
    ExpectInitializationRejectedBeforeIo(
        test,
        invalid,
        "recovered-open mode rejects an unaligned journal frontier");
}

void TestCommitObserverBoundaries(TestContext* test) {
    const auto state =
        std::make_shared<MemoryIoState>();
    RecordingCommitObserver observer;
    ingress::RawWalWriterConfig config = MakeConfig();
    config.commit_observer = &observer;
    ingress::RawWalWriter writer(
        config,
        std::make_unique<MemoryRawWalIo>(state));

    test->Expect(
        writer.Initialize() &&
            observer.open_calls == 1U &&
            observer.opened ==
                std::vector<std::byte>(
                    config.segment_header_wire.begin(),
                    config.segment_header_wire.end()),
        "commit observer sees the exact header after the initial marker barrier");
    const RecordFixture record = MakeRecord(1U);
    test->Expect(
        writer.AppendRecord(record.input()) &&
            observer.record_calls == 1U &&
            observer.record_segment_offset ==
                ingress::kRawV1SegmentHeaderBytes &&
            observer.record_global_wal_pos ==
                ingress::kRawV1SegmentHeaderBytes &&
            ingress::ValidateRawRecordV1(
                observer.record, nullptr) ==
                ingress::RawV1Error::kNone,
        "commit observer sees an independently valid record only after its trailer");
    test->Expect(
        writer.SealAndClose() &&
            observer.seal_calls == 1U &&
            observer.seal.size() ==
                ingress::kRawV1DurableMarkerBytes,
        "commit observer sees one exact journal-synced seal marker");
    ingress::DurableMarkerV1 marker{};
    test->Expect(
        ingress::DecodeDurableMarkerV1(
            observer.seal, &marker) ==
                ingress::RawV1Error::kNone &&
            marker.marker_flags ==
                ingress::kRawV1SegmentSealed &&
            observer.sealed_cursor ==
                writer.Snapshot().durable,
        "seal observer cursor is bound to the accepted marker");
}

void TestCommitObserverFailureFailStops(
    TestContext* test) {
    const auto state =
        std::make_shared<MemoryIoState>();
    RecordingCommitObserver observer;
    observer.fail_record = true;
    ingress::RawWalWriterConfig config = MakeConfig();
    config.commit_observer = &observer;
    ingress::RawWalWriter writer(
        config,
        std::make_unique<MemoryRawWalIo>(state));
    test->Expect(
        writer.Initialize(),
        "observer failure fixture initializes");
    const ingress::RawWalCursor before =
        writer.Snapshot().append;
    const RecordFixture record = MakeRecord(1U);
    test->Expect(
        !writer.AppendRecord(record.input()) &&
            observer.record_calls == 1U &&
            writer.failure().kind ==
                ingress::RawWalFailureKind::
                    kCommitObserver &&
            writer.Snapshot().append == before,
        "artifact observer failure latches fatal before append publication");
    test->Expect(
        state->segment.size() > before.segment_offset,
        "failed observer leaves the complete physical suffix for recovery classification");
}

}  // namespace

int main() {
    TestContext test;
    TestHappyOrderingAndSeal(&test);
    TestTrailerFailureDoesNotPublish(&test);
    TestZeroWriteIsFatal(&test);
    TestSegmentSyncFailureDoesNotWriteMarker(&test);
    TestPartialMarkerFailureDoesNotPublish(&test);
    TestJournalSyncFailureDoesNotPublish(&test);
    TestInvalidRecordLatchesBeforeIo(&test);
    TestNormalRotationChain(&test);
    TestRotationConfigurationRejections(&test);
    TestRotationPlanRejections(&test);
    TestFactoryPersistedHeadersAreNotRewritten(&test);
    TestRecoveredOpenIsSealOnly(&test);
    TestCommitObserverBoundaries(&test);
    TestCommitObserverFailureFailStops(&test);

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Phase 2 Raw WAL writer tests failed\n";
        return 1;
    }
    std::cout << "Phase 2 Raw WAL writer tests passed\n";
    return 0;
}
