#include "l2flow/control/control_checkpoint_posix_store.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace control = l2flow::control;
namespace ingress = l2flow::ingress;

namespace {

struct TestContext final {
    void Expect(bool condition, std::string_view message) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << message << '\n';
        }
    }

    int failures = 0;
};

class TemporaryDirectory final {
public:
    explicit TemporaryDirectory(std::string_view prefix) {
        std::array<char, 96U> buffer{};
        const std::string pattern =
            "/tmp/" + std::string(prefix) + "-XXXXXX";
        if (pattern.size() + 1U > buffer.size()) {
            throw std::runtime_error("temporary pattern is too long");
        }
        std::copy(pattern.begin(), pattern.end(), buffer.begin());
        char* const created = ::mkdtemp(buffer.data());
        if (created == nullptr) {
            throw std::runtime_error("mkdtemp failed");
        }
        path_ = created;
        if (::chmod(path_.c_str(), 0700U) != 0) {
            throw std::runtime_error("chmod failed");
        }
        descriptor_ = ::open(
            path_.c_str(),
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        if (descriptor_ < 0) {
            throw std::runtime_error("directory open failed");
        }
    }

    ~TemporaryDirectory() {
        if (descriptor_ >= 0) {
            static_cast<void>(::close(descriptor_));
        }
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    TemporaryDirectory(const TemporaryDirectory&) = delete;
    TemporaryDirectory& operator=(
        const TemporaryDirectory&) = delete;

    [[nodiscard]] int descriptor() const noexcept {
        return descriptor_;
    }

private:
    std::string path_;
    int descriptor_ = -1;
};

template <std::size_t Size>
void Fill(
    std::array<std::byte, Size>* output,
    std::uint8_t seed) {
    for (std::size_t index = 0U; index < Size; ++index) {
        (*output)[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                seed + static_cast<std::uint8_t>(index)));
    }
}

control::ControlDecoderCheckpointV1 MakeCheckpoint(
    std::uint8_t config_seed = 0x40U,
    std::uint64_t processed_sequence = 1U,
    std::uint64_t processed_start = 4096U,
    std::uint64_t processed_end = 4224U,
    std::uint32_t source_stream_id = 9001U,
    std::uint8_t stream_day_seed = 0x10U,
    std::uint32_t capture_date = 20260721U) {
    if (processed_sequence == 0U ||
        processed_sequence ==
            std::numeric_limits<std::uint64_t>::max()) {
        throw std::runtime_error("invalid checkpoint sequence");
    }
    control::ControlDecoderCheckpointV1 checkpoint;
    control::ControlDecoderSnapshotV1& state = checkpoint.state;
    state.source_stream_id = source_stream_id;
    state.capture_date = capture_date;
    Fill(&state.stream_day_id, stream_day_seed);
    Fill(&state.stable_config_sha256, config_seed);
    state.subscriptions.push_back(
        control::ControlSubscriptionStateV1{
            l2flow::sdk::MessageKey{4U, 101U, 24U},
            control::SubscriptionPolicyV1::kRequired,
            0U,
            false});
    state.requested_manifest_sha256 =
        control::ComputeRequestedSubscriptionManifestSha256V1(
            state.subscriptions);
    state.counters.processed_records = processed_sequence;
    state.next_ingress_sequence = processed_sequence + 1U;
    state.processed_ingress_sequence = processed_sequence;
    state.processed_record_start_wal_pos = processed_start;
    state.processed_record_end_wal_pos = processed_end;
    if (!control::ComputeControlDecoderStateSha256V1(
            state, &state.state_sha256)) {
        throw std::runtime_error("state hash failed");
    }
    return checkpoint;
}

ingress::RawControlSnapshot MakeFrontier(
    const control::ControlDecoderCheckpointV1& checkpoint) {
    ingress::RawControlSnapshot frontier;
    Fill(&frontier.writer_instance, 0x70U);
    frontier.stream_day_id = checkpoint.state.stream_day_id;
    frontier.source_stream_id = checkpoint.state.source_stream_id;
    frontier.capture_date = checkpoint.state.capture_date;
    frontier.segment_sequence = 1U;
    frontier.append_global_wal_pos =
        checkpoint.state.processed_record_end_wal_pos + 128U;
    frontier.append_ingress_sequence =
        checkpoint.state.processed_ingress_sequence + 1U;
    frontier.append_segment_offset =
        frontier.append_global_wal_pos;
    frontier.durable_global_wal_pos =
        checkpoint.state.processed_record_end_wal_pos;
    frontier.durable_ingress_sequence =
        checkpoint.state.processed_ingress_sequence;
    frontier.durable_segment_offset =
        frontier.durable_global_wal_pos;
    frontier.clock_epoch_label = 3U;
    frontier.heartbeat_monotonic_ns = 100U;
    return frontier;
}

[[nodiscard]] bool NameExists(
    int directory_fd,
    std::string_view name) {
    const std::string owned(name);
    struct stat status {};
    return ::fstatat(
               directory_fd,
               owned.c_str(),
               &status,
               AT_SYMLINK_NOFOLLOW) == 0;
}

[[nodiscard]] bool WriteAll(
    int descriptor,
    const std::vector<std::byte>& bytes) {
    std::size_t completed = 0U;
    while (completed < bytes.size()) {
        const ssize_t result = ::pwrite(
            descriptor,
            bytes.data() + completed,
            bytes.size() - completed,
            static_cast<off_t>(completed));
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            return false;
        }
        const std::size_t progress =
            static_cast<std::size_t>(result);
        if (progress > bytes.size() - completed) {
            return false;
        }
        completed += progress;
    }
    return true;
}

[[nodiscard]] bool WriteNamedFile(
    int directory_fd,
    std::string_view name,
    const std::vector<std::byte>& bytes) {
    const std::string owned_name(name);
    const int descriptor = ::openat(
        directory_fd,
        owned_name.c_str(),
        O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
        0600U);
    if (descriptor < 0) {
        return false;
    }
    const bool result =
        ::fchmod(descriptor, 0600U) == 0 &&
        WriteAll(descriptor, bytes) &&
        ::fsync(descriptor) == 0;
    static_cast<void>(::close(descriptor));
    return result;
}

void TestPublishIdempotenceConflictAndTamper(
    TestContext* test) {
    TemporaryDirectory directory(
        "l2flow-control-checkpoint-store");
    const control::ControlDecoderCheckpointV1 checkpoint =
        MakeCheckpoint();
    const ingress::RawControlSnapshot frontier =
        MakeFrontier(checkpoint);
    std::vector<std::byte> encoded;
    test->Expect(
        control::EncodeControlDecoderCheckpointV1(
            checkpoint, &encoded) ==
            control::ControlCheckpointV1Error::kNone,
        "fixture checkpoint encodes");

    control::ControlCheckpointPosixPublishResultV1 first =
        control::PublishControlCheckpointV1At(
            directory.descriptor(), encoded, frontier);
    test->Expect(
        first.ok() && first.receipt != nullptr &&
            first.disposition ==
                control::ControlCheckpointPosixDispositionV1::
                    kPublishedNew &&
            first.file_synced && first.directory_synced,
        "new checkpoint crosses both POSIX durability barriers");
    test->Expect(
        first.receipt != nullptr &&
            first.receipt->Validate(),
        "new checkpoint returns a valid retained-fd receipt");
    test->Expect(
        first.filename.find("d20260721-s9001-n") !=
                std::string::npos &&
            first.filename.find("-i1-e4224.bin") !=
                std::string::npos,
        "immutable final name includes namespace and processed cursors");

    control::ControlCheckpointPosixPublishResultV1 second =
        control::PublishControlCheckpointV1At(
            directory.descriptor(), checkpoint, frontier);
    test->Expect(
        second.ok() && second.receipt != nullptr &&
            second.disposition ==
                control::ControlCheckpointPosixDispositionV1::
                    kAcceptedExistingFinal &&
            second.filename == first.filename &&
            second.checkpoint_sha256 == first.checkpoint_sha256,
        "byte-identical existing final is idempotently accepted");

    const control::ControlDecoderCheckpointV1 conflicting =
        MakeCheckpoint(0x41U);
    control::ControlCheckpointPosixPublishResultV1 conflict =
        control::PublishControlCheckpointV1At(
            directory.descriptor(), conflicting, frontier);
    test->Expect(
        !conflict.ok() &&
            conflict.error ==
                control::ControlCheckpointPosixStoreErrorV1::
                    kCandidateConflict,
        "different valid bytes at the same immutable boundary conflict");
    test->Expect(
        first.receipt != nullptr &&
            first.receipt->Validate(),
        "conflicting publication does not overwrite the accepted final");

    ingress::RawControlSnapshot behind = frontier;
    behind.append_global_wal_pos =
        checkpoint.state.processed_record_start_wal_pos;
    behind.durable_global_wal_pos =
        checkpoint.state.processed_record_start_wal_pos;
    behind.append_segment_offset =
        checkpoint.state.processed_record_start_wal_pos;
    behind.durable_segment_offset =
        checkpoint.state.processed_record_start_wal_pos;
    behind.append_ingress_sequence =
        checkpoint.state.processed_ingress_sequence;
    behind.durable_ingress_sequence =
        checkpoint.state.processed_ingress_sequence;
    control::ControlCheckpointPosixPublishResultV1 past =
        control::PublishControlCheckpointV1At(
            directory.descriptor(), checkpoint, behind);
    test->Expect(
        !past.ok() &&
            past.error ==
                control::ControlCheckpointPosixStoreErrorV1::
                    kCheckpointPastDurableFrontier,
        "checkpoint past either Raw durable cursor is rejected before I/O");

    ingress::RawControlSnapshot sequence_behind = frontier;
    sequence_behind.append_global_wal_pos =
        sequence_behind.durable_global_wal_pos;
    sequence_behind.append_segment_offset =
        sequence_behind.durable_segment_offset;
    sequence_behind.append_ingress_sequence = 0U;
    sequence_behind.durable_ingress_sequence = 0U;
    const control::ControlCheckpointPosixPublishResultV1
        sequence_past = control::PublishControlCheckpointV1At(
            directory.descriptor(), checkpoint, sequence_behind);
    test->Expect(
        !sequence_past.ok() &&
            sequence_past.error ==
                control::ControlCheckpointPosixStoreErrorV1::
                    kCheckpointPastDurableFrontier,
        "checkpoint past the Raw durable ingress sequence is rejected");

    ingress::RawControlSnapshot same_sequence_gap = frontier;
    same_sequence_gap.segment_sequence = 2U;
    same_sequence_gap.append_global_wal_pos =
        checkpoint.state.processed_record_end_wal_pos +
        ingress::kRawV1SegmentHeaderBytes +
        ingress::kRawV1RecordAlignment;
    same_sequence_gap.durable_global_wal_pos =
        same_sequence_gap.append_global_wal_pos;
    same_sequence_gap.append_segment_offset =
        ingress::kRawV1SegmentHeaderBytes;
    same_sequence_gap.durable_segment_offset =
        same_sequence_gap.append_segment_offset;
    same_sequence_gap.append_ingress_sequence =
        checkpoint.state.processed_ingress_sequence;
    same_sequence_gap.durable_ingress_sequence =
        same_sequence_gap.append_ingress_sequence;
    const control::ControlCheckpointPosixPublishResultV1
        gap_rejected = control::PublishControlCheckpointV1At(
            directory.descriptor(), checkpoint, same_sequence_gap);
    test->Expect(
        !gap_rejected.ok() &&
            gap_rejected.error ==
                control::ControlCheckpointPosixStoreErrorV1::
                    kCheckpointPastDurableFrontier,
        "same-sequence durable WAL movement cannot be an arbitrary aligned gap");

    ingress::RawControlSnapshot header_only = frontier;
    header_only.segment_sequence = 2U;
    header_only.append_global_wal_pos =
        checkpoint.state.processed_record_end_wal_pos +
        ingress::kRawV1SegmentHeaderBytes;
    header_only.durable_global_wal_pos =
        header_only.append_global_wal_pos;
    header_only.append_segment_offset =
        ingress::kRawV1SegmentHeaderBytes;
    header_only.durable_segment_offset =
        header_only.append_segment_offset;
    header_only.append_ingress_sequence =
        checkpoint.state.processed_ingress_sequence;
    header_only.durable_ingress_sequence =
        header_only.append_ingress_sequence;
    const control::ControlCheckpointPosixPublishResultV1
        header_accepted = control::PublishControlCheckpointV1At(
            directory.descriptor(), checkpoint, header_only);
    test->Expect(
        header_accepted.ok() &&
            header_accepted.disposition ==
                control::ControlCheckpointPosixDispositionV1::
                    kAcceptedExistingFinal,
        "one durable segment header may advance WAL without advancing ingress sequence");

    ingress::RawControlSnapshot implausible_records = frontier;
    constexpr std::uint64_t kMinimumRecordBytes =
        ingress::kRawV1RecordHeaderBytes +
        ingress::kRawV1RecordTrailerBytes;
    implausible_records.append_global_wal_pos =
        checkpoint.state.processed_record_end_wal_pos +
        kMinimumRecordBytes;
    implausible_records.durable_global_wal_pos =
        implausible_records.append_global_wal_pos;
    implausible_records.append_segment_offset =
        implausible_records.append_global_wal_pos;
    implausible_records.durable_segment_offset =
        implausible_records.append_segment_offset;
    implausible_records.append_ingress_sequence =
        checkpoint.state.processed_ingress_sequence + 2U;
    implausible_records.durable_ingress_sequence =
        implausible_records.append_ingress_sequence;
    const control::ControlCheckpointPosixPublishResultV1
        records_rejected = control::PublishControlCheckpointV1At(
            directory.descriptor(), checkpoint, implausible_records);
    test->Expect(
        !records_rejected.ok() &&
            records_rejected.error ==
                control::ControlCheckpointPosixStoreErrorV1::
                    kCheckpointPastDurableFrontier,
        "durable sequence advance is bounded by the minimum Raw record size");

    ingress::RawControlSnapshot wrong_namespace = frontier;
    wrong_namespace.capture_date += 1U;
    const control::ControlCheckpointPosixPublishResultV1
        namespace_result = control::PublishControlCheckpointV1At(
            directory.descriptor(), checkpoint, wrong_namespace);
    test->Expect(
        !namespace_result.ok() &&
            namespace_result.error ==
                control::ControlCheckpointPosixStoreErrorV1::
                    kNamespaceMismatch,
        "Raw frontier from another namespace cannot authorize publication");

    ingress::RawControlSnapshot fatal = frontier;
    fatal.fatal_state = 1U;
    const control::ControlCheckpointPosixPublishResultV1 fatal_result =
        control::PublishControlCheckpointV1At(
            directory.descriptor(), checkpoint, fatal);
    test->Expect(
        !fatal_result.ok() &&
            fatal_result.error ==
                control::ControlCheckpointPosixStoreErrorV1::
                    kInvalidRawFrontier,
        "fatal Raw snapshot cannot authorize a durable checkpoint receipt");

    ingress::RawControlSnapshot impossible = frontier;
    impossible.append_global_wal_pos =
        impossible.append_segment_offset - 1U;
    const control::ControlCheckpointPosixPublishResultV1
        impossible_result = control::PublishControlCheckpointV1At(
            directory.descriptor(), checkpoint, impossible);
    test->Expect(
        !impossible_result.ok() &&
            impossible_result.error ==
                control::ControlCheckpointPosixStoreErrorV1::
                    kInvalidRawFrontier,
        "arithmetically impossible Raw global/segment cursor is rejected");

    control::ControlDecoderCheckpointV1 unaligned = checkpoint;
    unaligned.state.processed_record_end_wal_pos += 1U;
    test->Expect(
        control::ComputeControlDecoderStateSha256V1(
            unaligned.state,
            &unaligned.state.state_sha256),
        "test recomputes an otherwise model-valid unaligned checkpoint");
    const control::ControlCheckpointPosixPublishResultV1
        unaligned_result = control::PublishControlCheckpointV1At(
            directory.descriptor(), unaligned, frontier);
    test->Expect(
        !unaligned_result.ok() &&
            unaligned_result.error ==
                control::ControlCheckpointPosixStoreErrorV1::
                    kInvalidCheckpoint,
        "store rejects arithmetically impossible unaligned Raw cursor");

    if (first.receipt != nullptr) {
        const int writable = ::openat(
            directory.descriptor(),
            first.filename.c_str(),
            O_RDWR | O_NOFOLLOW | O_CLOEXEC);
        test->Expect(writable >= 0, "test opens final for tamper injection");
        if (writable >= 0) {
            std::byte first_byte{};
            const ssize_t read = ::pread(writable, &first_byte, 1U, 0);
            const std::byte original = first_byte;
            first_byte ^= std::byte{0xffU};
            const ssize_t written =
                ::pwrite(writable, &first_byte, 1U, 0);
            const int sync_result = ::fsync(writable);
            test->Expect(
                read == 1 && written == 1 && sync_result == 0,
                "test injects a same-inode byte mutation");
            test->Expect(
                !first.receipt->Validate(),
                "receipt validation detects same-name same-inode byte tamper");
            const ssize_t restored =
                ::pwrite(writable, &original, 1U, 0);
            const int restore_sync = ::fsync(writable);
            static_cast<void>(::close(writable));
            test->Expect(
                restored == 1 && restore_sync == 0,
                "test restores the original checkpoint bytes");
        }
        test->Expect(
            !first.receipt->Validate(),
            "receipt barrier identity stays invalid after bytes are restored");
    }
}

void TestPartialTemporaryFailsClosed(TestContext* test) {
    TemporaryDirectory directory(
        "l2flow-control-checkpoint-partial");
    const control::ControlDecoderCheckpointV1 checkpoint =
        MakeCheckpoint();
    const ingress::RawControlSnapshot frontier =
        MakeFrontier(checkpoint);
    std::string final_name;
    std::string temporary_name;
    test->Expect(
        control::ControlCheckpointV1Filename(
            checkpoint, &final_name) ==
                control::ControlCheckpointPosixStoreErrorV1::kNone &&
            control::ControlCheckpointV1TemporaryFilename(
                checkpoint, &temporary_name) ==
                control::ControlCheckpointPosixStoreErrorV1::kNone,
        "fixture derives deterministic checkpoint names");
    const int partial = ::openat(
        directory.descriptor(),
        temporary_name.c_str(),
        O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
        0600U);
    test->Expect(partial >= 0, "test creates partial O_EXCL temporary");
    if (partial >= 0) {
        const std::byte byte{0x11U};
        test->Expect(
            ::pwrite(partial, &byte, 1U, 0) == 1 &&
                ::fsync(partial) == 0,
            "test persists partial temporary");
        static_cast<void>(::close(partial));
    }

    control::ControlCheckpointPosixPublishResultV1 result =
        control::PublishControlCheckpointV1At(
            directory.descriptor(), checkpoint, frontier);
    test->Expect(
        !result.ok(),
        "partial deterministic temporary fails closed");
    test->Expect(
        NameExists(directory.descriptor(), temporary_name) &&
            !NameExists(directory.descriptor(), final_name),
        "store neither deletes partial temporary nor publishes a final");
}

void TestCompleteTemporaryIsAdopted(TestContext* test) {
    TemporaryDirectory directory(
        "l2flow-control-checkpoint-complete");
    const control::ControlDecoderCheckpointV1 checkpoint =
        MakeCheckpoint();
    const ingress::RawControlSnapshot frontier =
        MakeFrontier(checkpoint);
    std::vector<std::byte> encoded;
    std::string final_name;
    std::string temporary_name;
    test->Expect(
        control::EncodeControlDecoderCheckpointV1(
            checkpoint, &encoded) ==
                control::ControlCheckpointV1Error::kNone &&
            control::ControlCheckpointV1Filename(
                checkpoint, &final_name) ==
                control::ControlCheckpointPosixStoreErrorV1::kNone &&
            control::ControlCheckpointV1TemporaryFilename(
                checkpoint, &temporary_name) ==
                control::ControlCheckpointPosixStoreErrorV1::kNone,
        "complete temporary fixture is canonical");
    const int temporary = ::openat(
        directory.descriptor(),
        temporary_name.c_str(),
        O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
        0600U);
    test->Expect(
        temporary >= 0,
        "test creates complete crash-window temporary");
    if (temporary >= 0) {
        test->Expect(
            WriteAll(temporary, encoded) &&
                ::fsync(temporary) == 0,
            "test persists complete crash-window temporary");
        static_cast<void>(::close(temporary));
    }

    control::ControlCheckpointPosixPublishResultV1 result =
        control::PublishControlCheckpointV1At(
            directory.descriptor(), encoded, frontier);
    test->Expect(
        result.ok() && result.receipt != nullptr &&
            result.disposition ==
                control::ControlCheckpointPosixDispositionV1::
                    kAdoptedCompleteTemporary &&
            result.receipt->Validate(),
        "complete exact temporary is adopted through NOREPLACE and dirsync");
    test->Expect(
        NameExists(directory.descriptor(), final_name) &&
            !NameExists(directory.descriptor(), temporary_name),
        "adoption leaves the sole immutable final mapping");
}

void TestRestartDiscoverySelectsLatest(TestContext* test) {
    TemporaryDirectory directory(
        "l2flow-control-checkpoint-restart");
    const control::ControlDecoderCheckpointV1 first_checkpoint =
        MakeCheckpoint();
    const control::ControlDecoderCheckpointV1 latest_checkpoint =
        MakeCheckpoint(0x40U, 2U, 4224U, 4352U);
    const ingress::RawControlSnapshot frontier =
        MakeFrontier(latest_checkpoint);
    control::ControlCheckpointPosixPublishResultV1 first =
        control::PublishControlCheckpointV1At(
            directory.descriptor(), first_checkpoint, frontier);
    control::ControlCheckpointPosixPublishResultV1 latest =
        control::PublishControlCheckpointV1At(
            directory.descriptor(), latest_checkpoint, frontier);
    test->Expect(
        first.ok() && latest.ok(),
        "restart fixture publishes two increasing immutable finals");

    const std::vector<std::byte> note{
        std::byte{'k'}, std::byte{'e'}, std::byte{'e'}, std::byte{'p'}};
    test->Expect(
        WriteNamedFile(
            directory.descriptor(), "operator-note", note),
        "restart fixture creates unrelated file");
    std::string ignored_temporary;
    test->Expect(
        control::ControlCheckpointV1TemporaryFilename(
            latest_checkpoint, &ignored_temporary) ==
            control::ControlCheckpointPosixStoreErrorV1::kNone,
        "restart fixture derives temporary name");
    const std::vector<std::byte> partial{std::byte{0x33U}};
    test->Expect(
        WriteNamedFile(
            directory.descriptor(),
            ignored_temporary,
            partial),
        "restart fixture creates ignored partial temporary");

    control::ControlCheckpointPosixLoadResultV1 loaded =
        control::LoadLatestControlCheckpointV1At(
            directory.descriptor(), frontier);
    test->Expect(
        loaded.ok() && loaded.checkpoint.has_value() &&
            loaded.filename == latest.filename &&
            loaded.checkpoint->state.processed_ingress_sequence == 2U &&
            loaded.checkpoint->state.processed_record_end_wal_pos ==
                4352U &&
            loaded.observed_final_candidate_count == 2U &&
            loaded.namespace_candidate_count == 2U,
        "restart discovery selects the unique candidate maximal in both cursors");
    test->Expect(
        loaded.checkpoint_sha256 == latest.checkpoint_sha256 &&
            !loaded.encoded_checkpoint.empty(),
        "restart discovery returns exact owned wire/model/hash");
    test->Expect(
        NameExists(directory.descriptor(), "operator-note") &&
            NameExists(
                directory.descriptor(), ignored_temporary),
        "restart discovery preserves unrelated files and temporary evidence");
}

void TestRestartRejectsBadFinals(TestContext* test) {
    const control::ControlDecoderCheckpointV1 checkpoint =
        MakeCheckpoint();
    const ingress::RawControlSnapshot frontier =
        MakeFrontier(checkpoint);
    std::string final_name;
    test->Expect(
        control::ControlCheckpointV1Filename(
            checkpoint, &final_name) ==
            control::ControlCheckpointPosixStoreErrorV1::kNone,
        "bad-final fixture derives exact target name");

    {
        TemporaryDirectory directory(
            "l2flow-control-checkpoint-bad-wire");
        std::vector<std::byte> bad_wire(
            control::kControlCheckpointV1HeaderBytes +
                control::kControlCheckpointV1TrailerBytes,
            std::byte{0x5aU});
        test->Expect(
            WriteNamedFile(
                directory.descriptor(), final_name, bad_wire),
            "bad-final fixture writes safe-mode corrupt bytes");
        const control::ControlCheckpointPosixLoadResultV1 loaded =
            control::LoadLatestControlCheckpointV1At(
                directory.descriptor(), frontier);
        test->Expect(
            !loaded.ok() &&
                loaded.error ==
                    control::ControlCheckpointPosixStoreErrorV1::
                        kInvalidCheckpoint,
            "restart rejects a canonical-name final with bad wire");
    }
    {
        TemporaryDirectory directory(
            "l2flow-control-checkpoint-malformed-name");
        const std::vector<std::byte> bytes{std::byte{0x01U}};
        test->Expect(
            WriteNamedFile(
                directory.descriptor(),
                "control-checkpoint-v1-not-valid.bin",
                bytes),
            "malformed-name fixture writes prefix-like file");
        const control::ControlCheckpointPosixLoadResultV1 loaded =
            control::LoadLatestControlCheckpointV1At(
                directory.descriptor(), frontier);
        test->Expect(
            !loaded.ok() &&
                loaded.error ==
                    control::ControlCheckpointPosixStoreErrorV1::
                        kMalformedCandidateName,
            "restart fails closed on malformed final grammar");
    }
}

void TestRestartNamespaceAndDurableBound(TestContext* test) {
    const control::ControlDecoderCheckpointV1 expected =
        MakeCheckpoint();
    const ingress::RawControlSnapshot expected_frontier =
        MakeFrontier(expected);
    {
        TemporaryDirectory directory(
            "l2flow-control-checkpoint-namespace");
        const control::ControlDecoderCheckpointV1 other_source =
            MakeCheckpoint(
                0x40U,
                1U,
                4096U,
                4224U,
                9002U,
                0x10U);
        const control::ControlDecoderCheckpointV1 other_date =
            MakeCheckpoint(
                0x40U,
                1U,
                4096U,
                4224U,
                9001U,
                0x10U,
                20260722U);
        const control::ControlDecoderCheckpointV1 other_stream_day =
            MakeCheckpoint(
                0x40U,
                1U,
                4096U,
                4224U,
                9001U,
                0x20U);
        const control::ControlCheckpointPosixPublishResultV1 published_source =
            control::PublishControlCheckpointV1At(
                directory.descriptor(),
                other_source,
                MakeFrontier(other_source));
        const control::ControlCheckpointPosixPublishResultV1 published_date =
            control::PublishControlCheckpointV1At(
                directory.descriptor(),
                other_date,
                MakeFrontier(other_date));
        const control::ControlCheckpointPosixPublishResultV1 published_stream_day =
            control::PublishControlCheckpointV1At(
                directory.descriptor(),
                other_stream_day,
                MakeFrontier(other_stream_day));
        test->Expect(
            published_source.ok() && published_date.ok() &&
                published_stream_day.ok(),
            "namespace fixture publishes one candidate differing in each identity component");
        const control::ControlCheckpointPosixLoadResultV1 loaded =
            control::LoadLatestControlCheckpointV1At(
                directory.descriptor(), expected_frontier);
        test->Expect(
            !loaded.ok() &&
                loaded.error ==
                    control::ControlCheckpointPosixStoreErrorV1::
                        kNoEligibleCheckpoint &&
                loaded.observed_final_candidate_count == 3U &&
                loaded.namespace_candidate_count == 0U,
            "restart requires equality of all three Raw namespace components");
    }
    {
        TemporaryDirectory directory(
            "l2flow-control-checkpoint-past-durable");
        const control::ControlDecoderCheckpointV1 later =
            MakeCheckpoint(0x40U, 2U, 4224U, 4352U);
        const ingress::RawControlSnapshot later_frontier =
            MakeFrontier(later);
        const control::ControlCheckpointPosixPublishResultV1 published =
            control::PublishControlCheckpointV1At(
                directory.descriptor(), later, later_frontier);
        test->Expect(
            published.ok(),
            "past-durable fixture publishes under its original frontier");
        const control::ControlCheckpointPosixLoadResultV1 loaded =
            control::LoadLatestControlCheckpointV1At(
                directory.descriptor(), expected_frontier);
        test->Expect(
            !loaded.ok() &&
                loaded.error ==
                    control::ControlCheckpointPosixStoreErrorV1::
                        kCheckpointPastDurableFrontier,
            "restart rejects a final beyond the current Raw durable frontier");
    }
}

void TestRestartRejectsSymlinkAndHardlink(TestContext* test) {
    const control::ControlDecoderCheckpointV1 checkpoint =
        MakeCheckpoint();
    const ingress::RawControlSnapshot frontier =
        MakeFrontier(checkpoint);
    std::string final_name;
    test->Expect(
        control::ControlCheckpointV1Filename(
            checkpoint, &final_name) ==
            control::ControlCheckpointPosixStoreErrorV1::kNone,
        "link fixtures derive exact final name");
    {
        TemporaryDirectory directory(
            "l2flow-control-checkpoint-symlink");
        const std::vector<std::byte> target_bytes{std::byte{0x01U}};
        test->Expect(
            WriteNamedFile(
                directory.descriptor(), "target", target_bytes) &&
                ::symlinkat(
                    "target",
                    directory.descriptor(),
                    final_name.c_str()) == 0,
            "symlink fixture creates exact-name symlink");
        const control::ControlCheckpointPosixLoadResultV1 loaded =
            control::LoadLatestControlCheckpointV1At(
                directory.descriptor(), frontier);
        test->Expect(
            !loaded.ok() &&
                loaded.error ==
                    control::ControlCheckpointPosixStoreErrorV1::
                        kUnsafeCandidate,
            "restart O_NOFOLLOW rejects exact-name symlink");
    }
    {
        TemporaryDirectory directory(
            "l2flow-control-checkpoint-hardlink");
        control::ControlCheckpointPosixPublishResultV1 published =
            control::PublishControlCheckpointV1At(
                directory.descriptor(), checkpoint, frontier);
        test->Expect(
            published.ok() &&
                ::linkat(
                    directory.descriptor(),
                    published.filename.c_str(),
                    directory.descriptor(),
                    "extra-hardlink",
                    0) == 0,
            "hardlink fixture adds a second name to final inode");
        const control::ControlCheckpointPosixLoadResultV1 loaded =
            control::LoadLatestControlCheckpointV1At(
                directory.descriptor(), frontier);
        test->Expect(
            !loaded.ok() &&
                loaded.error ==
                    control::ControlCheckpointPosixStoreErrorV1::
                        kUnsafeCandidate,
            "restart rejects a final inode with link count greater than one");
    }
}

void TestRestartRejectsCrossedCandidateCursors(TestContext* test) {
    TemporaryDirectory directory(
        "l2flow-control-checkpoint-crossed");
    const control::ControlDecoderCheckpointV1 dominant =
        MakeCheckpoint(0x40U, 3U, 4608U, 4736U);
    const control::ControlDecoderCheckpointV1 wal_later =
        MakeCheckpoint(0x40U, 1U, 4352U, 4480U);
    const control::ControlDecoderCheckpointV1 sequence_later =
        MakeCheckpoint(0x40U, 2U, 4224U, 4352U);
    const ingress::RawControlSnapshot frontier =
        MakeFrontier(dominant);
    const control::ControlCheckpointPosixPublishResultV1 maximum =
        control::PublishControlCheckpointV1At(
            directory.descriptor(), dominant, frontier);
    const control::ControlCheckpointPosixPublishResultV1 first =
        control::PublishControlCheckpointV1At(
            directory.descriptor(), wal_later, frontier);
    const control::ControlCheckpointPosixPublishResultV1 second =
        control::PublishControlCheckpointV1At(
            directory.descriptor(), sequence_later, frontier);
    test->Expect(
        maximum.ok() && first.ok() && second.ok(),
        "crossed fixture persists a maximum plus two crossed predecessors");
    const control::ControlCheckpointPosixLoadResultV1 loaded =
        control::LoadLatestControlCheckpointV1At(
            directory.descriptor(), frontier);
    test->Expect(
        !loaded.ok() &&
            loaded.error ==
                control::ControlCheckpointPosixStoreErrorV1::
                    kInconsistentCandidates &&
            loaded.namespace_candidate_count == 3U,
        "restart refuses crossed predecessors even below one dominant maximum");
}

void TestUnsafeCandidateTypesFailClosed(TestContext* test) {
    const control::ControlDecoderCheckpointV1 checkpoint =
        MakeCheckpoint();
    const ingress::RawControlSnapshot frontier =
        MakeFrontier(checkpoint);
    std::string final_name;
    std::string temporary_name;
    test->Expect(
        control::ControlCheckpointV1Filename(
            checkpoint, &final_name) ==
                control::ControlCheckpointPosixStoreErrorV1::kNone &&
            control::ControlCheckpointV1TemporaryFilename(
                checkpoint, &temporary_name) ==
                control::ControlCheckpointPosixStoreErrorV1::kNone,
        "unsafe-candidate fixtures derive deterministic names");

    {
        TemporaryDirectory directory(
            "l2flow-control-checkpoint-symlink");
        const int created = ::symlinkat(
            "untrusted-target",
            directory.descriptor(),
            final_name.c_str());
        test->Expect(
            created == 0,
            "test creates a final-name symlink candidate");
        const control::ControlCheckpointPosixPublishResultV1 result =
            control::PublishControlCheckpointV1At(
                directory.descriptor(), checkpoint, frontier);
        test->Expect(
            !result.ok() &&
                result.error ==
                    control::ControlCheckpointPosixStoreErrorV1::
                        kUnsafeCandidate &&
                result.observed_candidate_count == 1U &&
                NameExists(directory.descriptor(), final_name) &&
                !NameExists(directory.descriptor(), temporary_name),
            "final-name symlink is rejected without creating a temporary");
    }

    {
        TemporaryDirectory directory(
            "l2flow-control-checkpoint-fifo");
        const int created = ::mkfifoat(
            directory.descriptor(),
            temporary_name.c_str(),
            static_cast<mode_t>(0600U));
        test->Expect(
            created == 0,
            "test creates a temporary-name FIFO candidate");
        const control::ControlCheckpointPosixPublishResultV1 result =
            control::PublishControlCheckpointV1At(
                directory.descriptor(), checkpoint, frontier);
        test->Expect(
            !result.ok() &&
                result.error ==
                    control::ControlCheckpointPosixStoreErrorV1::
                        kUnsafeCandidate &&
                result.observed_candidate_count == 1U &&
                !NameExists(directory.descriptor(), final_name) &&
                NameExists(directory.descriptor(), temporary_name),
            "temporary-name FIFO is rejected without blocking or publication");
    }

    {
        TemporaryDirectory directory(
            "l2flow-control-checkpoint-hardlink");
        constexpr const char* kSourceName = "hardlink-source";
        const int source = ::openat(
            directory.descriptor(),
            kSourceName,
            O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
            0600U);
        test->Expect(
            source >= 0,
            "test creates a regular hard-link source");
        if (source >= 0) {
            static_cast<void>(::close(source));
        }
        const int linked = source >= 0
            ? ::linkat(
                  directory.descriptor(),
                  kSourceName,
                  directory.descriptor(),
                  final_name.c_str(),
                  0)
            : -1;
        test->Expect(
            linked == 0,
            "test gives the final candidate a second hard link");
        const control::ControlCheckpointPosixPublishResultV1 result =
            control::PublishControlCheckpointV1At(
                directory.descriptor(), checkpoint, frontier);
        test->Expect(
            !result.ok() &&
                result.error ==
                    control::ControlCheckpointPosixStoreErrorV1::
                        kUnsafeCandidate &&
                result.observed_candidate_count == 1U &&
                NameExists(directory.descriptor(), final_name) &&
                !NameExists(directory.descriptor(), temporary_name),
            "multiply-linked final candidate is rejected without mutation");
    }
}

void TestReceiptDetectsNameToInodeReplacement(TestContext* test) {
    TemporaryDirectory directory(
        "l2flow-control-checkpoint-replaced");
    const control::ControlDecoderCheckpointV1 checkpoint =
        MakeCheckpoint();
    const ingress::RawControlSnapshot frontier =
        MakeFrontier(checkpoint);
    std::vector<std::byte> encoded;
    test->Expect(
        control::EncodeControlDecoderCheckpointV1(
            checkpoint, &encoded) ==
            control::ControlCheckpointV1Error::kNone,
        "replacement fixture checkpoint encodes");
    control::ControlCheckpointPosixPublishResultV1 published =
        control::PublishControlCheckpointV1At(
            directory.descriptor(), checkpoint, frontier);
    test->Expect(
        published.ok() && published.receipt != nullptr &&
            published.receipt->Validate(),
        "replacement fixture starts with a valid retained-fd receipt");
    if (!published.ok() || published.receipt == nullptr) {
        return;
    }

    const std::string displaced_name =
        published.filename + ".displaced";
    const int renamed = ::renameat(
        directory.descriptor(),
        published.filename.c_str(),
        directory.descriptor(),
        displaced_name.c_str());
    test->Expect(
        renamed == 0,
        "test displaces the accepted final inode under another name");
    const int replacement = renamed == 0
        ? ::openat(
              directory.descriptor(),
              published.filename.c_str(),
              O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
              0600U)
        : -1;
    test->Expect(
        replacement >= 0,
        "test creates a new inode at the accepted final name");
    bool replacement_persisted = false;
    if (replacement >= 0) {
        replacement_persisted =
            WriteAll(replacement, encoded) &&
            ::fsync(replacement) == 0;
        static_cast<void>(::close(replacement));
    }
    struct stat displaced_status {};
    struct stat replacement_status {};
    const bool distinct_inodes =
        replacement_persisted &&
        ::fstatat(
            directory.descriptor(),
            displaced_name.c_str(),
            &displaced_status,
            AT_SYMLINK_NOFOLLOW) == 0 &&
        ::fstatat(
            directory.descriptor(),
            published.filename.c_str(),
            &replacement_status,
            AT_SYMLINK_NOFOLLOW) == 0 &&
        (displaced_status.st_dev != replacement_status.st_dev ||
         displaced_status.st_ino != replacement_status.st_ino) &&
        ::fsync(directory.descriptor()) == 0;
    test->Expect(
        distinct_inodes,
        "test persists byte-identical content under a distinct final inode");
    test->Expect(
        distinct_inodes && !published.receipt->Validate(),
        "retained receipt detects final-name to inode replacement");
}

void TestRestartDiscoveryRejectsCorruptFinal(TestContext* test) {
    TemporaryDirectory directory(
        "l2flow-control-checkpoint-discovery");
    const control::ControlDecoderCheckpointV1 checkpoint =
        MakeCheckpoint();
    const ingress::RawControlSnapshot frontier =
        MakeFrontier(checkpoint);
    control::ControlCheckpointPosixPublishResultV1 published =
        control::PublishControlCheckpointV1At(
            directory.descriptor(), checkpoint, frontier);
    test->Expect(
        published.ok() && published.receipt != nullptr,
        "restart discovery fixture publishes an immutable final");
    if (!published.ok() || published.receipt == nullptr) {
        return;
    }

    const control::ControlCheckpointPosixLoadResultV1 loaded =
        control::LoadLatestControlCheckpointV1At(
            directory.descriptor(), frontier);
    test->Expect(
        loaded.ok() && loaded.checkpoint.has_value() &&
            loaded.filename == published.filename &&
            loaded.checkpoint_sha256 == published.checkpoint_sha256 &&
            loaded.checkpoint->state.state_sha256 ==
                checkpoint.state.state_sha256 &&
            loaded.observed_final_candidate_count == 1U &&
            loaded.namespace_candidate_count == 1U,
        "restart discovery loads the unique canonical namespace candidate");

    const int writable = ::openat(
        directory.descriptor(),
        published.filename.c_str(),
        O_RDWR | O_NOFOLLOW | O_CLOEXEC);
    test->Expect(
        writable >= 0,
        "test opens the discovered final for corruption injection");
    bool corrupted = false;
    if (writable >= 0) {
        std::byte first_byte{};
        corrupted = ::pread(writable, &first_byte, 1U, 0) == 1;
        first_byte ^= std::byte{0xffU};
        corrupted = corrupted &&
            ::pwrite(writable, &first_byte, 1U, 0) == 1 &&
            ::fsync(writable) == 0;
        static_cast<void>(::close(writable));
    }
    test->Expect(
        corrupted,
        "test persists a same-name corrupt checkpoint final");
    const control::ControlCheckpointPosixLoadResultV1 rejected =
        control::LoadLatestControlCheckpointV1At(
            directory.descriptor(), frontier);
    test->Expect(
        corrupted && !rejected.ok() &&
            rejected.error ==
                control::ControlCheckpointPosixStoreErrorV1::
                    kInvalidCheckpoint &&
            rejected.observed_final_candidate_count == 1U &&
            rejected.namespace_candidate_count == 1U,
        "restart discovery fails closed on a corrupt in-namespace final");
}

}  // namespace

int main() {
    TestContext test;
    try {
        TestPublishIdempotenceConflictAndTamper(&test);
        TestPartialTemporaryFailsClosed(&test);
        TestCompleteTemporaryIsAdopted(&test);
        TestRestartDiscoverySelectsLatest(&test);
        TestRestartRejectsBadFinals(&test);
        TestRestartNamespaceAndDurableBound(&test);
        TestRestartRejectsSymlinkAndHardlink(&test);
        TestRestartRejectsCrossedCandidateCursors(&test);
        TestUnsafeCandidateTypesFailClosed(&test);
        TestReceiptDetectsNameToInodeReplacement(&test);
        TestRestartDiscoveryRejectsCorruptFinal(&test);
    } catch (const std::exception& exception) {
        std::cerr << "FAIL: unexpected exception: "
                  << exception.what() << '\n';
        ++test.failures;
    }
    if (test.failures == 0) {
        std::cout
            << "Phase3 control checkpoint POSIX store tests passed\n";
    }
    return test.failures == 0 ? 0 : 1;
}
