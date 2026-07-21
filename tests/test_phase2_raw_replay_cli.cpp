#include "l2flow/ingress/raw_schema.h"
#include "l2flow/ingress/raw_v1.h"

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#ifndef L2FLOW_RAW_REPLAY_EXE
#error "L2FLOW_RAW_REPLAY_EXE must name the built replay executable"
#endif

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

class TemporaryDirectory final {
public:
    TemporaryDirectory() {
        std::array<char, 64U> pattern{};
        constexpr std::string_view prefix =
            "/tmp/l2flow-raw-replay-cli-XXXXXX";
        std::memcpy(
            pattern.data(), prefix.data(), prefix.size());
        char* const created = ::mkdtemp(pattern.data());
        if (created != nullptr) {
            path_ = created;
        }
    }

    TemporaryDirectory(const TemporaryDirectory&) = delete;
    TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

    ~TemporaryDirectory() {
        for (const std::string& name : names_) {
            static_cast<void>(::unlink(
                (path_ + "/" + name).c_str()));
        }
        if (!path_.empty()) {
            static_cast<void>(::rmdir(path_.c_str()));
        }
    }

    [[nodiscard]] bool valid() const noexcept {
        return !path_.empty();
    }

    [[nodiscard]] const std::string& path() const noexcept {
        return path_;
    }

    void Remember(std::string name) {
        names_.push_back(std::move(name));
    }

private:
    std::string path_;
    std::vector<std::string> names_;
};

struct CommandResult final {
    int exit_code = -1;
    bool exited = false;
    std::string standard_output;
    std::string standard_error;
};

void StoreU16(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint16_t value) {
    bytes[offset] = static_cast<std::byte>(value & 0xffU);
    bytes[offset + 1U] =
        static_cast<std::byte>((value >> 8U) & 0xffU);
}

void StoreU32(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint32_t value) {
    for (std::size_t index = 0U; index < 4U; ++index) {
        bytes[offset + index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
}

void StoreU64(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint64_t value) {
    for (std::size_t index = 0U; index < 8U; ++index) {
        bytes[offset + index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
}

template <std::size_t Size>
void Fill(
    std::array<std::byte, Size>* bytes,
    std::uint8_t seed) {
    for (std::size_t index = 0U; index < Size; ++index) {
        (*bytes)[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                seed + static_cast<std::uint8_t>(index)));
    }
}

ingress::RawV1VendorHead MakeVendorHead(
    std::uint32_t body_size,
    std::uint8_t service_id,
    std::uint16_t service_version,
    std::uint16_t message_id,
    std::uint64_t sequence) {
    ingress::RawV1VendorHead head{};
    std::span<std::byte> bytes(head);
    head[0U] = static_cast<std::byte>(
        ingress::kVendorMessageHeadBytes);
    StoreU32(
        bytes,
        1U,
        static_cast<std::uint32_t>(
            ingress::kVendorMessageHeadBytes) +
            body_size);
    head[5U] = std::byte{1U};
    head[6U] = static_cast<std::byte>(service_id);
    StoreU16(bytes, 7U, service_version);
    StoreU16(bytes, 9U, message_id);
    StoreU32(bytes, 11U, 123U);
    StoreU64(bytes, 15U, sequence);
    return head;
}

struct RecordSpec final {
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t realtime_ns = 0U;
    std::uint64_t monotonic_ns = 0U;
    std::uint8_t service_id = 0U;
    std::uint16_t service_version = 0U;
    std::uint16_t message_id = 0U;
    std::byte body{};
};

struct SegmentFixture final {
    std::vector<std::byte> bytes;
    std::vector<std::uint64_t> record_ends;
};

SegmentFixture MakeSegment(
    std::uint32_t segment_sequence,
    std::uint64_t base_wal_pos,
    std::uint64_t first_ingress_sequence,
    std::uint8_t stream_day_seed,
    std::span<const RecordSpec> records,
    bool frozen_schema = true,
    bool continuation = false) {
    ingress::SegmentHeaderV1 segment;
    segment.source_stream_id = 1001U;
    segment.capture_date = 20260718U;
    Fill(&segment.stream_day_id, stream_day_seed);
    segment.segment_sequence = segment_sequence;
    if (continuation) {
        segment.segment_flags =
            ingress::kRawV1FinalizationContinuation;
        Fill(&segment.reserve_state_uuid, 171U);
        Fill(&segment.finalization_cycle_id, 181U);
        Fill(&segment.immutable_grant_sha256, 191U);
    }
    segment.segment_base_wal_pos = base_wal_pos;
    segment.first_ingress_sequence = first_ingress_sequence;
    segment.created_realtime_ns = 1U;
    segment.created_monotonic_ns = 1U;
    Fill(&segment.host_uuid, 21U);
    Fill(&segment.linux_boot_id, 41U);
    segment.clock_epoch_algorithm = 1U;
    Fill(&segment.clock_epoch_digest, 51U);
    segment.clock_epoch_label = 51U;
    Fill(&segment.sdk_archive_sha256, 61U);
    Fill(&segment.libmdl_api_sha256, 81U);
    Fill(&segment.endpoint_contract_sha256, 101U);
    Fill(&segment.config_sha256, 121U);
    segment.raw_schema_sha256 =
        ingress::RawSchemaSha256Digest();
    if (!frozen_schema) {
        segment.raw_schema_sha256[0U] ^= std::byte{1U};
    }
    Fill(&segment.build_manifest_sha256, 161U);

    ingress::RawV1SegmentHeaderWire header{};
    const ingress::RawV1Error header_error =
        ingress::EncodeSegmentHeaderV1(segment, &header);
    if (header_error != ingress::RawV1Error::kNone) {
        return {};
    }

    SegmentFixture fixture;
    fixture.bytes.assign(header.begin(), header.end());
    for (const RecordSpec& spec : records) {
        const std::array<std::byte, 1U> body{spec.body};
        ingress::RawRecordInputV1 input;
        input.meta.source_stream_id = segment.source_stream_id;
        input.meta.connection_epoch_hint = 1U;
        input.meta.ingress_sequence =
            spec.ingress_sequence;
        input.meta.recv_realtime_ns = spec.realtime_ns;
        input.meta.recv_monotonic_ns = spec.monotonic_ns;
        input.meta.capture_date = segment.capture_date;
        input.vendor_head = MakeVendorHead(
            static_cast<std::uint32_t>(body.size()),
            spec.service_id,
            spec.service_version,
            spec.message_id,
            spec.ingress_sequence);
        input.vendor_body = body;

        std::vector<std::byte> record;
        if (ingress::EncodeRawRecordV1(input, &record) !=
            ingress::RawV1Error::kNone) {
            return {};
        }
        fixture.bytes.insert(
            fixture.bytes.end(), record.begin(), record.end());
        fixture.record_ends.push_back(
            static_cast<std::uint64_t>(
                fixture.bytes.size()));
    }
    return fixture;
}

[[nodiscard]] bool WriteAll(
    int descriptor,
    std::span<const std::byte> bytes) {
    std::size_t offset = 0U;
    while (offset < bytes.size()) {
        const ssize_t result = ::write(
            descriptor,
            bytes.data() + offset,
            bytes.size() - offset);
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (result == 0) {
            return false;
        }
        offset += static_cast<std::size_t>(result);
    }
    return true;
}

[[nodiscard]] bool WriteFixture(
    TemporaryDirectory* directory,
    std::string name,
    std::span<const std::byte> bytes) {
    if (directory == nullptr || !directory->valid()) {
        return false;
    }
    const std::string path = directory->path() + "/" + name;
    const int descriptor = ::open(
        path.c_str(),
        O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
        0600);
    if (descriptor < 0) {
        return false;
    }
    const bool wrote = WriteAll(descriptor, bytes);
    const bool closed = ::close(descriptor) == 0;
    if (wrote && closed) {
        directory->Remember(std::move(name));
        return true;
    }
    static_cast<void>(::unlink(path.c_str()));
    return false;
}

[[nodiscard]] bool ReadAll(
    int descriptor,
    std::string* output) {
    std::array<char, 4096U> buffer{};
    for (;;) {
        const ssize_t result =
            ::read(descriptor, buffer.data(), buffer.size());
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (result == 0) {
            return true;
        }
        output->append(
            buffer.data(), static_cast<std::size_t>(result));
    }
}

CommandResult RunCommand(std::vector<std::string> arguments) {
    CommandResult result;
    arguments.insert(
        arguments.begin(), std::string(L2FLOW_RAW_REPLAY_EXE));
    std::vector<char*> argv;
    argv.reserve(arguments.size() + 1U);
    for (std::string& argument : arguments) {
        argv.push_back(argument.data());
    }
    argv.push_back(nullptr);

    std::array<int, 2U> output_pipe{{-1, -1}};
    std::array<int, 2U> error_pipe{{-1, -1}};
    if (::pipe2(output_pipe.data(), O_CLOEXEC) != 0 ||
        ::pipe2(error_pipe.data(), O_CLOEXEC) != 0) {
        if (output_pipe[0U] >= 0) {
            static_cast<void>(::close(output_pipe[0U]));
            static_cast<void>(::close(output_pipe[1U]));
        }
        return result;
    }

    const pid_t child = ::fork();
    if (child == 0) {
        static_cast<void>(::close(output_pipe[0U]));
        static_cast<void>(::close(error_pipe[0U]));
        if (::dup2(output_pipe[1U], STDOUT_FILENO) < 0 ||
            ::dup2(error_pipe[1U], STDERR_FILENO) < 0) {
            _exit(126);
        }
        static_cast<void>(::close(output_pipe[1U]));
        static_cast<void>(::close(error_pipe[1U]));
        ::execv(argv[0U], argv.data());
        _exit(127);
    }

    static_cast<void>(::close(output_pipe[1U]));
    static_cast<void>(::close(error_pipe[1U]));
    if (child < 0) {
        static_cast<void>(::close(output_pipe[0U]));
        static_cast<void>(::close(error_pipe[0U]));
        return result;
    }
    const bool output_ok =
        ReadAll(output_pipe[0U], &result.standard_output);
    const bool error_ok =
        ReadAll(error_pipe[0U], &result.standard_error);
    static_cast<void>(::close(output_pipe[0U]));
    static_cast<void>(::close(error_pipe[0U]));

    int status = 0;
    pid_t waited = -1;
    do {
        waited = ::waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    result.exited =
        waited == child && WIFEXITED(status) &&
        output_ok && error_ok;
    if (result.exited) {
        result.exit_code = WEXITSTATUS(status);
    }
    return result;
}

[[nodiscard]] std::size_t CountLinesBeginningWith(
    std::string_view text,
    std::string_view prefix) {
    std::size_t count = 0U;
    std::size_t offset = 0U;
    while (offset < text.size()) {
        const std::size_t end = text.find('\n', offset);
        const std::string_view line = text.substr(
            offset,
            end == std::string_view::npos
                ? text.size() - offset
                : end - offset);
        if (line.substr(0U, prefix.size()) == prefix) {
            ++count;
        }
        if (end == std::string_view::npos) {
            break;
        }
        offset = end + 1U;
    }
    return count;
}

void TestHelpAndParser(TestContext* test) {
    const CommandResult help = RunCommand({"--help"});
    test->Expect(
        help.exited && help.exit_code == 0 &&
            help.standard_error.empty() &&
            help.standard_output.find(
                "explicit exclusive durable logical end") !=
                std::string::npos &&
            help.standard_output.find(
                "does not business-decode") !=
                std::string::npos,
        "help documents explicit-frontier and metadata-only boundaries");

    const CommandResult missing_speed = RunCommand(
        {"--root", "/tmp", "--segment",
         "segment-00000001.raw", "4096",
         "--pace", "fixed"});
    test->Expect(
        missing_speed.exited &&
            missing_speed.exit_code == 2 &&
            missing_speed.standard_output.empty() &&
            missing_speed.standard_error.find(
                "--speed is required exactly") !=
                std::string::npos,
        "fixed pacing without an exact rational speed fails in argv parsing");

    const CommandResult reversed_range = RunCommand(
        {"--root", "/tmp", "--segment",
         "segment-00000001.raw", "4096",
         "--wal", "10:9"});
    test->Expect(
        reversed_range.exited &&
            reversed_range.exit_code == 2 &&
            reversed_range.standard_output.empty(),
        "reversed half-open range fails before filesystem access");

    const CommandResult append_only = RunCommand(
        {"--root", "/tmp", "--segment",
         "segment-00000001.raw", "4096",
         "--include-recovered-append-only"});
    test->Expect(
        append_only.exited &&
            append_only.exit_code == 2 &&
            append_only.standard_output.empty() &&
            append_only.standard_error.find("unknown option") !=
                std::string::npos,
        "the CLI does not expose unbound append-only provenance");
}

void TestStableMultiSegmentReplay(TestContext* test) {
    TemporaryDirectory directory;
    test->Expect(directory.valid(), "multi-segment temp root created");
    if (!directory.valid()) {
        return;
    }

    const std::array<RecordSpec, 1U> first_records{{
        {1U, 100U, 1000U, 2U, 7U, 41U, std::byte{0x51U}}}};
    const SegmentFixture first = MakeSegment(
        1U, 0U, 1U, 1U, first_records);
    const std::array<RecordSpec, 1U> second_records{{
        {2U, 200U, 1100U, 2U, 7U, 41U, std::byte{0x52U}}}};
    const SegmentFixture second = MakeSegment(
        2U,
        static_cast<std::uint64_t>(first.bytes.size()),
        2U,
        1U,
        second_records);
    test->Expect(
        !first.bytes.empty() && !second.bytes.empty() &&
            WriteFixture(
                &directory,
                "segment-00000001.raw",
                first.bytes) &&
            WriteFixture(
                &directory,
                "segment-00000002.raw",
                second.bytes),
        "two contiguous Raw V1 final fixtures written");

    const std::vector<std::string> arguments{
        "--root", directory.path(),
        "--segment", "segment-00000002.raw",
        std::to_string(second.bytes.size()),
        "--segment", "segment-00000001.raw",
        std::to_string(first.bytes.size()),
        "--stream-id", "1001",
        "--capture-date", "20260718",
        "--service-id", "2",
        "--service-version", "7",
        "--message-id", "41",
        "--wal", "4096:",
        "--ingress", "1:3",
        "--recv-realtime-ns", "100:201",
        "--pace", "fixed",
        "--speed", "1000000/1",
        "--seed", "99"};
    const CommandResult first_run = RunCommand(arguments);
    const CommandResult second_run = RunCommand(arguments);

    test->Expect(
        first_run.exited && first_run.exit_code == 0 &&
            first_run.standard_error.empty(),
        "real replay executable accepts validated multi-segment input");
    test->Expect(
        second_run.exited && second_run.exit_code == 0 &&
            first_run.standard_output ==
                second_run.standard_output,
        "record/provenance stdout is stable across identical runs");
    test->Expect(
        first_run.standard_output.find(
            "provenance=explicit_durable_frontier") !=
                std::string::npos &&
            first_run.standard_output.find(
                "business_decode=false") !=
                std::string::npos &&
            first_run.standard_output.find(
                "run_manifest_published=false") !=
                std::string::npos &&
            first_run.standard_output.find(
                "selected_record_count=2") !=
                std::string::npos &&
            CountLinesBeginningWith(
                first_run.standard_output, "input\t") == 2U &&
            CountLinesBeginningWith(
                first_run.standard_output, "record\t") == 2U &&
            first_run.standard_output.find(
                "segment_sequence=1") <
                first_run.standard_output.find(
                    "segment_sequence=2") &&
            first_run.standard_output.find(
                "provenance=durable") !=
                std::string::npos &&
            first_run.standard_output.find(
                "vendor_service_id=2") !=
                std::string::npos &&
            first_run.standard_output.find(
                "durable_prefix_sha256=") !=
                std::string::npos,
        "stdout carries ordered locator, metadata, digest and durability facts");
}

void TestExplicitFrontierHidesSuffix(TestContext* test) {
    TemporaryDirectory directory;
    const std::array<RecordSpec, 2U> records{{
        {1U, 100U, 1000U, 2U, 7U, 41U, std::byte{1U}},
        {2U, 200U, 1100U, 3U, 8U, 42U, std::byte{2U}}}};
    const SegmentFixture fixture =
        MakeSegment(1U, 0U, 1U, 2U, records);
    test->Expect(
        directory.valid() &&
            fixture.record_ends.size() == 2U &&
            WriteFixture(
                &directory,
                "segment-00000001.raw",
                fixture.bytes),
        "suffix fixture written");
    if (!directory.valid() ||
        fixture.record_ends.size() != 2U) {
        return;
    }

    const CommandResult result = RunCommand(
        {"--root", directory.path(),
         "--segment", "segment-00000001.raw",
         std::to_string(fixture.record_ends.front())});
    test->Expect(
        result.exited && result.exit_code == 0 &&
            result.standard_error.empty() &&
            CountLinesBeginningWith(
                result.standard_output, "record\t") == 1U &&
            result.standard_output.find(
                "ingress_sequence=1") !=
                std::string::npos &&
            result.standard_output.find(
                "ingress_sequence=2") ==
                std::string::npos,
        "complete physical suffix is invisible beyond explicit durable end");
}

void TestInvalidRawProducesNoStdout(TestContext* test) {
    const std::array<RecordSpec, 1U> records{{
        {1U, 100U, 1000U, 2U, 7U, 41U, std::byte{1U}}}};
    const SegmentFixture valid =
        MakeSegment(1U, 0U, 1U, 3U, records);

    {
        TemporaryDirectory directory;
        test->Expect(
            directory.valid() &&
                WriteFixture(
                    &directory,
                    "segment-00000001.raw",
                    valid.bytes),
            "mid-record fixture written");
        const CommandResult result = RunCommand(
            {"--root", directory.path(),
             "--segment", "segment-00000001.raw",
             std::to_string(valid.bytes.size() - 1U)});
        test->Expect(
            result.exited && result.exit_code == 3 &&
                result.standard_output.empty() &&
                result.standard_error.find(
                    "Raw V1 scan failed") !=
                    std::string::npos,
            "non-record-aligned durable logical end fails before stdout");
    }

    {
        TemporaryDirectory directory;
        std::vector<std::byte> corrupt = valid.bytes;
        corrupt[ingress::kRawV1SegmentHeaderBytes +
                ingress::kRawV1RecordHeaderBytes +
                ingress::kVendorMessageHeadBytes] ^=
            std::byte{0x80U};
        test->Expect(
            directory.valid() &&
                WriteFixture(
                    &directory,
                    "segment-00000001.raw",
                    corrupt),
            "CRC corruption fixture written");
        const CommandResult result = RunCommand(
            {"--root", directory.path(),
             "--segment", "segment-00000001.raw",
             std::to_string(corrupt.size())});
        test->Expect(
            result.exited && result.exit_code == 3 &&
                result.standard_output.empty() &&
                result.standard_error.find(
                    "payload CRC mismatch") !=
                    std::string::npos,
            "payload corruption is rejected by complete scan before stdout");
    }

    {
        TemporaryDirectory directory;
        const SegmentFixture wrong_schema =
            MakeSegment(1U, 0U, 1U, 4U, records, false);
        test->Expect(
            directory.valid() &&
                WriteFixture(
                    &directory,
                    "segment-00000001.raw",
                    wrong_schema.bytes),
            "valid-framing wrong-schema fixture written");
        const CommandResult result = RunCommand(
            {"--root", directory.path(),
             "--segment", "segment-00000001.raw",
             std::to_string(wrong_schema.bytes.size())});
        test->Expect(
            result.exited && result.exit_code == 3 &&
                result.standard_output.empty() &&
                result.standard_error.find(
                    "does not match the compiled digest") !=
                    std::string::npos,
            "CRC-valid segment for another schema identity fails closed");
    }

    {
        TemporaryDirectory directory;
        const SegmentFixture continuation = MakeSegment(
            1U, 0U, 1U, 6U, records, true, true);
        test->Expect(
            directory.valid() &&
                WriteFixture(
                    &directory,
                    "segment-00000001.raw",
                    continuation.bytes),
            "valid-framing continuation fixture written");
        const CommandResult result = RunCommand(
            {"--root", directory.path(),
             "--segment", "segment-00000001.raw",
             std::to_string(continuation.bytes.size())});
        test->Expect(
            result.exited && result.exit_code == 3 &&
                result.standard_output.empty() &&
                result.standard_error.find(
                    "requires bound reserve/report evidence") !=
                    std::string::npos,
            "continuation cannot bypass its missing DONE/report provenance");
    }
}

void TestSecureOpenAndSetContinuity(TestContext* test) {
    const std::array<RecordSpec, 1U> records{{
        {1U, 100U, 1000U, 2U, 7U, 41U, std::byte{1U}}}};
    const SegmentFixture first =
        MakeSegment(1U, 0U, 1U, 5U, records);

    {
        TemporaryDirectory directory;
        test->Expect(
            directory.valid() &&
                WriteFixture(
                    &directory, "source.raw", first.bytes),
            "symlink source fixture written");
        const std::string link_path =
            directory.path() + "/segment-00000001.raw";
        const bool linked =
            ::symlink("source.raw", link_path.c_str()) == 0;
        if (linked) {
            directory.Remember("segment-00000001.raw");
        }
        test->Expect(linked, "symlink fixture created");
        const CommandResult result = RunCommand(
            {"--root", directory.path(),
             "--segment", "segment-00000001.raw",
             std::to_string(first.bytes.size())});
        test->Expect(
            result.exited && result.exit_code == 3 &&
                result.standard_output.empty() &&
                result.standard_error.find(
                    "secure open rejected") !=
                    std::string::npos,
            "O_NOFOLLOW rejects a canonical-name symlink");
    }

    {
        TemporaryDirectory directory;
        test->Expect(
            directory.valid() &&
                WriteFixture(
                    &directory,
                    "segment-00000001.raw",
                    first.bytes),
            "hardlink source fixture written");
        const std::string extra =
            directory.path() + "/extra-link.raw";
        const bool linked =
            ::link(
                (directory.path() +
                 "/segment-00000001.raw").c_str(),
                extra.c_str()) == 0;
        if (linked) {
            directory.Remember("extra-link.raw");
        }
        test->Expect(linked, "extra hardlink fixture created");
        const CommandResult result = RunCommand(
            {"--root", directory.path(),
             "--segment", "segment-00000001.raw",
             std::to_string(first.bytes.size())});
        test->Expect(
            result.exited && result.exit_code == 3 &&
                result.standard_output.empty() &&
                result.standard_error.find(
                    "file safety or name-to-inode gate rejected") !=
                    std::string::npos,
            "singly-linked regular-file gate rejects a hardlinked segment");
    }

    {
        TemporaryDirectory directory;
        const std::array<RecordSpec, 1U> second_records{{
            {2U, 200U, 1100U, 2U, 7U, 41U, std::byte{2U}}}};
        const SegmentFixture noncontiguous = MakeSegment(
            2U,
            static_cast<std::uint64_t>(first.bytes.size()) + 8U,
            2U,
            5U,
            second_records);
        test->Expect(
            directory.valid() &&
                WriteFixture(
                    &directory,
                    "segment-00000001.raw",
                    first.bytes) &&
                WriteFixture(
                    &directory,
                    "segment-00000002.raw",
                    noncontiguous.bytes),
            "non-contiguous segment set written");
        const CommandResult result = RunCommand(
            {"--root", directory.path(),
             "--segment", "segment-00000001.raw",
             std::to_string(first.bytes.size()),
             "--segment", "segment-00000002.raw",
             std::to_string(noncontiguous.bytes.size())});
        test->Expect(
            result.exited && result.exit_code == 3 &&
                result.standard_output.empty() &&
                result.standard_error.find(
                    "WAL bases are not contiguous") !=
                    std::string::npos,
            "multi-segment replay rejects a WAL hole before output");
    }
}

}  // namespace

int main() {
    TestContext test;
    TestHelpAndParser(&test);
    TestStableMultiSegmentReplay(&test);
    TestExplicitFrontierHidesSuffix(&test);
    TestInvalidRawProducesNoStdout(&test);
    TestSecureOpenAndSetContinuity(&test);

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Phase 2 Raw replay CLI checks failed\n";
        return 1;
    }
    std::cout << "Phase 2 Raw replay CLI checks passed\n";
    return 0;
}
