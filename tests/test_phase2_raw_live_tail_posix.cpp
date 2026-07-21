#include "l2flow/ingress/raw_control_file.h"
#include "l2flow/ingress/raw_live_tail.h"
#include "l2flow/ingress/raw_live_tail_posix.h"
#include "l2flow/ingress/raw_namespace.h"
#include "l2flow/ingress/raw_v1.h"
#include "l2flow/ingress/raw_writer_lease.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace ingress = l2flow::ingress;

namespace {

int failures = 0;

void Expect(bool condition, std::string_view message) {
    if (!condition) {
        ++failures;
        std::cerr << "FAIL: " << message << '\n';
    }
}

template <std::size_t Size>
std::array<std::byte, Size> Pattern(std::uint8_t seed) {
    std::array<std::byte, Size> result{};
    for (std::size_t index = 0U; index < Size; ++index) {
        result[index] = static_cast<std::byte>(
            seed + static_cast<std::uint8_t>(index));
    }
    return result;
}

class TemporaryDirectory final {
public:
    TemporaryDirectory() {
        std::array<char, 80U> native{};
        const std::string pattern =
            "/tmp/l2flow-live-tail-posix-XXXXXX";
        std::copy(
            pattern.begin(), pattern.end(), native.begin());
        const char* const created =
            ::mkdtemp(native.data());
        if (created == nullptr) {
            throw std::runtime_error("mkdtemp failed");
        }
        path_ = created;
        if (::chmod(path_.c_str(), 0700) != 0) {
            throw std::runtime_error("chmod failed");
        }
        descriptor_ = ::open(
            path_.c_str(),
            O_RDONLY | O_DIRECTORY | O_NONBLOCK |
                O_CLOEXEC | O_NOATIME);
        if (descriptor_ < 0) {
            throw std::runtime_error(
                "cannot open temporary directory");
        }
    }

    ~TemporaryDirectory() {
        if (descriptor_ >= 0) {
            static_cast<void>(::close(descriptor_));
        }
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] int descriptor() const noexcept {
        return descriptor_;
    }

private:
    std::string path_;
    int descriptor_ = -1;
};

[[nodiscard]] bool WriteAllAt(
    int descriptor,
    std::uint64_t offset,
    std::span<const std::byte> bytes) {
    std::size_t completed = 0U;
    while (completed < bytes.size()) {
        const std::uint64_t current =
            offset +
            static_cast<std::uint64_t>(completed);
        if (current >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max())) {
            return false;
        }
        const ssize_t result = ::pwrite(
            descriptor,
            bytes.data() + completed,
            bytes.size() - completed,
            static_cast<off_t>(current));
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (result == 0) {
            return false;
        }
        completed += static_cast<std::size_t>(result);
    }
    return true;
}

[[nodiscard]] bool CreateRegular(
    int directory_fd,
    const char* name,
    std::span<const std::byte> bytes,
    std::uint64_t final_size) {
    const int descriptor = ::openat(
        directory_fd,
        name,
        O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW |
            O_NONBLOCK | O_CLOEXEC,
        0600);
    if (descriptor < 0) {
        return false;
    }
    const bool size_fits =
        final_size <=
        static_cast<std::uint64_t>(
            std::numeric_limits<off_t>::max());
    const bool result =
        size_fits &&
        ::ftruncate(
            descriptor,
            static_cast<off_t>(final_size)) == 0 &&
        WriteAllAt(descriptor, 0U, bytes) &&
        ::fdatasync(descriptor) == 0;
    static_cast<void>(::close(descriptor));
    return result;
}

ingress::SegmentHeaderV1 MakeSegmentHeader(
    std::uint32_t sequence,
    std::uint64_t base,
    std::uint64_t first_ingress) {
    ingress::SegmentHeaderV1 header;
    header.source_stream_id = 1001U;
    header.capture_date = 20260718U;
    header.stream_day_id = Pattern<16U>(0x10U);
    header.segment_sequence = sequence;
    header.segment_base_wal_pos = base;
    header.first_ingress_sequence = first_ingress;
    header.created_realtime_ns =
        100U + static_cast<std::uint64_t>(sequence);
    header.created_monotonic_ns =
        200U + static_cast<std::uint64_t>(sequence);
    header.host_uuid = Pattern<16U>(0x20U);
    header.linux_boot_id = Pattern<16U>(0x30U);
    header.clock_epoch_algorithm = 1U;
    header.clock_epoch_digest = Pattern<32U>(0x40U);
    header.clock_epoch_label = 7U;
    header.sdk_archive_sha256 = Pattern<32U>(0x50U);
    header.libmdl_api_sha256 = Pattern<32U>(0x60U);
    header.endpoint_contract_sha256 =
        Pattern<32U>(0x70U);
    header.config_sha256 = Pattern<32U>(0x80U);
    header.raw_schema_sha256 = Pattern<32U>(0x90U);
    header.build_manifest_sha256 =
        Pattern<32U>(0xa0U);
    return header;
}

std::vector<std::byte> MakeRecord(
    std::uint64_t ingress_sequence) {
    ingress::RawRecordInputV1 input;
    input.meta.source_stream_id = 1001U;
    input.meta.capture_date = 20260718U;
    input.meta.ingress_sequence = ingress_sequence;
    input.meta.recv_realtime_ns =
        1'000U + ingress_sequence;
    input.meta.recv_monotonic_ns =
        2'000U + ingress_sequence;
    input.vendor_head.fill(std::byte{0});
    input.vendor_head[0U] = std::byte{23};
    const auto store_le =
        [&input](
            std::size_t offset,
            std::uint64_t value,
            std::size_t width) {
            for (std::size_t index = 0U;
                 index < width;
                 ++index) {
                input.vendor_head[offset + index] =
                    static_cast<std::byte>(
                        (value >> (index * 8U)) &
                        UINT64_C(0xff));
            }
        };
    store_le(1U, 27U, 4U);
    input.vendor_head[5U] = std::byte{4};
    input.vendor_head[6U] = std::byte{1};
    store_le(7U, 2U, 2U);
    store_le(9U, 3U, 2U);
    store_le(11U, 5U, 4U);
    store_le(15U, 100U + ingress_sequence, 8U);
    const std::array<std::byte, 4U> body{
        std::byte{1},
        std::byte{2},
        std::byte{3},
        static_cast<std::byte>(ingress_sequence)};
    input.vendor_body = body;
    std::vector<std::byte> wire;
    Expect(
        ingress::EncodeRawRecordV1(
            input, &wire, nullptr) ==
            ingress::RawV1Error::kNone,
        "Raw record fixture encodes");
    return wire;
}

ingress::DurableMarkerV1 MakeMarker(
    std::uint32_t segment_sequence,
    std::uint64_t base,
    std::uint64_t offset,
    std::uint64_t ingress_sequence,
    std::uint32_t flags) {
    ingress::DurableMarkerV1 marker;
    marker.source_stream_id = 1001U;
    marker.segment_sequence = segment_sequence;
    marker.durable_global_wal_pos = base + offset;
    marker.durable_ingress_sequence =
        ingress_sequence;
    marker.durable_segment_offset = offset;
    marker.marker_flags = flags;
    return marker;
}

struct RawFixture final {
    ingress::SegmentHeaderV1 first_header{};
    ingress::SegmentHeaderV1 second_header{};
    std::uint64_t first_end = 0U;
    std::uint64_t second_end = 0U;
    std::uint64_t second_file_size = 0U;
};

[[nodiscard]] bool PublishRawFixture(
    int directory_fd,
    RawFixture* fixture) {
    const std::vector<std::byte> first_record =
        MakeRecord(1U);
    fixture->first_end =
        ingress::kRawV1SegmentHeaderBytes +
        first_record.size();
    fixture->first_header =
        MakeSegmentHeader(1U, 0U, 1U);
    fixture->second_header =
        MakeSegmentHeader(
            2U, fixture->first_end, 2U);
    const std::vector<std::byte> second_record =
        MakeRecord(2U);
    fixture->second_end =
        ingress::kRawV1SegmentHeaderBytes +
        second_record.size();
    fixture->second_file_size =
        fixture->second_end + 4096U;

    ingress::RawV1SegmentHeaderWire first_wire{};
    ingress::RawV1SegmentHeaderWire second_wire{};
    if (ingress::EncodeSegmentHeaderV1(
            fixture->first_header, &first_wire) !=
            ingress::RawV1Error::kNone ||
        ingress::EncodeSegmentHeaderV1(
            fixture->second_header, &second_wire) !=
            ingress::RawV1Error::kNone) {
        return false;
    }
    std::vector<std::byte> first_bytes(
        first_wire.begin(), first_wire.end());
    first_bytes.insert(
        first_bytes.end(),
        first_record.begin(),
        first_record.end());
    std::vector<std::byte> second_bytes(
        second_wire.begin(), second_wire.end());
    second_bytes.insert(
        second_bytes.end(),
        second_record.begin(),
        second_record.end());
    if (!CreateRegular(
            directory_fd,
            "segment-00000001.raw",
            first_bytes,
            fixture->first_end) ||
        !CreateRegular(
            directory_fd,
            "segment-00000002.raw",
            second_bytes,
            fixture->second_file_size)) {
        return false;
    }

    ingress::DurableJournalHeaderV1 journal_header;
    journal_header.capture_date = 20260718U;
    journal_header.source_stream_id = 1001U;
    journal_header.stream_day_id =
        fixture->first_header.stream_day_id;
    journal_header.raw_schema_sha256 =
        fixture->first_header.raw_schema_sha256;
    journal_header.created_host_uuid =
        fixture->first_header.host_uuid;
    journal_header.created_linux_boot_id =
        fixture->first_header.linux_boot_id;
    journal_header.created_clock_epoch_algorithm =
        fixture->first_header.clock_epoch_algorithm;
    journal_header.created_clock_epoch_digest =
        fixture->first_header.clock_epoch_digest;
    journal_header.created_clock_epoch_label =
        fixture->first_header.clock_epoch_label;
    ingress::RawV1JournalHeaderWire journal_wire{};
    if (ingress::EncodeDurableJournalHeaderV1(
            journal_header, &journal_wire) !=
        ingress::RawV1Error::kNone) {
        return false;
    }
    std::vector<std::byte> journal(
        journal_wire.begin(), journal_wire.end());
    const std::array<ingress::DurableMarkerV1, 5U>
        markers{
            MakeMarker(
                1U,
                0U,
                ingress::kRawV1SegmentHeaderBytes,
                0U,
                0U),
            MakeMarker(
                1U, 0U, fixture->first_end, 1U, 0U),
            MakeMarker(
                1U,
                0U,
                fixture->first_end,
                1U,
                ingress::kRawV1SegmentSealed),
            MakeMarker(
                2U,
                fixture->first_end,
                ingress::kRawV1SegmentHeaderBytes,
                1U,
                0U),
            MakeMarker(
                2U,
                fixture->first_end,
                fixture->second_end,
                2U,
                0U)};
    for (const ingress::DurableMarkerV1& marker :
         markers) {
        ingress::RawV1DurableMarkerWire marker_wire{};
        if (ingress::EncodeDurableMarkerV1(
                marker, &marker_wire) !=
            ingress::RawV1Error::kNone) {
            return false;
        }
        journal.insert(
            journal.end(),
            marker_wire.begin(),
            marker_wire.end());
    }
    return CreateRegular(
        directory_fd,
        ingress::kRawJournalFilename,
        journal,
        journal.size());
}

ingress::RawControlSnapshot MakeControl(
    const RawFixture& fixture,
    std::uint8_t writer_seed) {
    ingress::RawControlSnapshot control;
    control.writer_instance =
        Pattern<16U>(writer_seed);
    control.stream_day_id =
        fixture.first_header.stream_day_id;
    control.source_stream_id = 1001U;
    control.capture_date = 20260718U;
    control.segment_sequence = 2U;
    control.append_global_wal_pos =
        fixture.first_end + fixture.second_end;
    control.append_ingress_sequence = 2U;
    control.append_segment_offset = fixture.second_end;
    control.durable_global_wal_pos =
        control.append_global_wal_pos;
    control.durable_ingress_sequence = 2U;
    control.durable_segment_offset =
        fixture.second_end;
    control.clock_epoch_label = 7U;
    control.heartbeat_monotonic_ns = 10'000U;
    return control;
}

ingress::RawLiveTailPosixAttachGateV1 MakeAttachGate(
    const ingress::RawControlSnapshot& control) {
    ingress::RawLiveTailPosixAttachGateV1 gate;
    gate.writer_instance = control.writer_instance;
    gate.stream_day_id = control.stream_day_id;
    gate.recovered_durable = {
        control.segment_sequence,
        control.durable_global_wal_pos,
        control.durable_ingress_sequence,
        control.durable_segment_offset,
        0U};
    return gate;
}

void TestPosixTailRotationAndControlReattach() {
    TemporaryDirectory directory;
    std::string error;
    std::unique_ptr<ingress::RawWriterLease> lease =
        ingress::AcquireRawWriterLeaseAt(
            directory.descriptor(),
            1001U,
            20260718U,
            &error);
    Expect(lease != nullptr, "writer lease fixture opens");
    if (lease == nullptr) {
        return;
    }

    RawFixture fixture;
    Expect(
        PublishRawFixture(
            lease->directory_descriptor(), &fixture),
        "two-segment Raw fixture publishes");
    const ingress::RawControlSnapshot first_control =
        MakeControl(fixture, 0xb0U);
    std::unique_ptr<ingress::RawControlFileWriter>
        first_writer = ingress::CreateRawControlFile(
            *lease, first_control, &error);
    Expect(
        first_writer != nullptr,
        "first control inode publishes");
    if (first_writer == nullptr) {
        return;
    }

    std::unique_ptr<ingress::RawLiveTailPosixSource>
        source = ingress::OpenRawLiveTailPosixSource(
            lease->directory_descriptor(),
            1001U,
            20260718U,
            MakeAttachGate(first_control),
            {},
            &error);
    Expect(
        source != nullptr,
        "POSIX live-tail source securely attaches");
    if (source == nullptr) {
        std::cerr << error << '\n';
        return;
    }

    ingress::RawLiveSegmentInfo first_info;
    ingress::RawLiveSegmentInfo second_info;
    Expect(
        source->InspectSegment(1U, &first_info) == 0 &&
            first_info.sealed &&
            first_info.visible_end_offset ==
                fixture.first_end,
        "sealed segment logical end comes from journal seal");
    Expect(
        source->InspectSegment(2U, &second_info) == 0 &&
            !second_info.sealed &&
            second_info.visible_end_offset ==
                fixture.second_file_size,
        "open segment exposes file bound for control-page capping");

    const auto flags_are_read_only_noatime =
        [](int flags) {
            return flags >= 0 &&
                   (flags & O_ACCMODE) == O_RDONLY &&
                   (flags & O_APPEND) == 0 &&
                   (flags & O_NONBLOCK) != 0 &&
                   (flags & O_NOATIME) != 0;
        };
    Expect(
        flags_are_read_only_noatime(
            source->control_open_flags()) &&
            flags_are_read_only_noatime(
                source->journal_open_flags()) &&
            flags_are_read_only_noatime(
                source->segment_open_flags(1U)) &&
            flags_are_read_only_noatime(
                source->segment_open_flags(2U)) &&
            (source->directory_open_flags() &
             O_NOATIME) != 0 &&
            source->retained_segment_count() == 2U,
        "all retained read descriptors preserve secure flags");

    std::array<std::byte, 8U> short_buffer{};
    const ingress::RawLiveReadResult short_read =
        source->ReadSegmentSome(
            1U,
            fixture.first_end - 2U,
            short_buffer);
    Expect(
        short_read.error_number == 0 &&
            short_read.bytes_read == 2U,
        "one POSIX pread reports a real EOF short read");

    ingress::RawLiveTailAttachV1 attach;
    attach.writer_instance =
        first_control.writer_instance;
    attach.stream_day_id =
        first_control.stream_day_id;
    attach.source_stream_id = 1001U;
    attach.capture_date = 20260718U;
    attach.segment_sequence = 1U;
    attach.global_wal_pos =
        ingress::kRawV1SegmentHeaderBytes;
    attach.segment_offset =
        ingress::kRawV1SegmentHeaderBytes;
    attach.next_ingress_sequence = 1U;
    std::unique_ptr<ingress::RawLiveTail> tail;
    Expect(
        ingress::RawLiveTail::Attach(
            source.get(), attach, &tail) ==
            ingress::RawLiveTailError::kNone,
        "live tail attaches at recovered record boundary");
    if (tail == nullptr) {
        return;
    }
    const ingress::RawLiveTailStep first = tail->Next();
    const ingress::RawLiveTailStep transition =
        tail->Next();
    const ingress::RawLiveTailStep second = tail->Next();
    Expect(
        first.kind ==
                ingress::RawLiveTailStepKind::kRecord &&
            first.record->view.header().ingress_sequence ==
                1U &&
            transition.kind ==
                ingress::RawLiveTailStepKind::
                    kSegmentTransition &&
            transition.segment_transition.has_value() &&
            transition.segment_transition
                    ->previous_segment
                    .segment_sequence == 1U &&
            transition.segment_transition
                    ->next_segment.segment_sequence ==
                2U &&
            transition.segment_transition
                    ->next_data_begin_wal_pos ==
                fixture.second_header
                        .segment_base_wal_pos +
                    ingress::kRawV1SegmentHeaderBytes &&
            second.kind ==
                ingress::RawLiveTailStepKind::kRecord &&
            second.record->segment.segment_sequence == 2U &&
            second.record->view.header().ingress_sequence ==
                2U,
        "owned pread records cross only the journal-sealed boundary");
    Expect(
        tail->Next().kind ==
            ingress::RawLiveTailStepKind::kWouldBlock,
        "preallocated open tail is capped by append publication");

    const ingress::RawControlSnapshot second_control =
        MakeControl(fixture, 0xd0U);
    std::unique_ptr<ingress::RawControlFileWriter>
        replacement_writer =
            ingress::CreateRawControlFile(
                *lease, second_control, &error);
    Expect(
        replacement_writer != nullptr,
        "replacement control inode publishes");
    const ingress::RawLiveTailStep fenced = tail->Next();
    Expect(
        fenced.kind ==
                ingress::RawLiveTailStepKind::
                    kInstanceChanged &&
            fenced.error ==
                ingress::RawLiveTailError::
                    kInstanceChanged,
        "control pathname replacement safely reattaches and fences writer instance");

    ingress::RawControlSnapshot observed;
    std::uint64_t generation = 0U;
    Expect(
        source->ReadControl(&observed, &generation) ==
            ESTALE &&
            source->InspectSegment(2U, &second_info) ==
                ESTALE,
        "old source is permanently fenced until a new recovery-frontier attach gate is constructed");
}

void TestRetainedInodeAndStrictNameFailures() {
    TemporaryDirectory directory;
    std::string error;
    auto lease = ingress::AcquireRawWriterLeaseAt(
        directory.descriptor(),
        1001U,
        20260718U,
        &error);
    Expect(lease != nullptr, "strict-name fixture lease opens");
    if (lease == nullptr) {
        return;
    }
    RawFixture fixture;
    Expect(
        PublishRawFixture(
            lease->directory_descriptor(), &fixture),
        "strict-name Raw fixture publishes");
    auto writer = ingress::CreateRawControlFile(
        *lease,
        MakeControl(fixture, 0xb0U),
        &error);
    auto source = ingress::OpenRawLiveTailPosixSource(
        lease->directory_descriptor(),
        1001U,
        20260718U,
        MakeAttachGate(MakeControl(fixture, 0xb0U)),
        {},
        &error);
    Expect(
        writer != nullptr && source != nullptr,
        "strict-name source attaches");
    if (writer == nullptr || source == nullptr) {
        return;
    }

    ingress::RawLiveSegmentInfo info;
    Expect(
        source->InspectSegment(100'000'000U, &info) ==
            EINVAL,
        "sequence outside exact eight-digit grammar is rejected");

    Expect(
        ::linkat(
            lease->directory_descriptor(),
            "segment-00000001.raw",
            lease->directory_descriptor(),
            "segment-hardlink-fixture.raw",
            0) == 0,
        "hard-link fixture is created");
    Expect(
        source->InspectSegment(1U, &info) == EMLINK,
        "singly-linked invariant is revalidated on retained fd");
    static_cast<void>(::unlinkat(
        lease->directory_descriptor(),
        "segment-hardlink-fixture.raw",
        0));

    Expect(
        ::renameat(
            lease->directory_descriptor(),
            "segment-00000002.raw",
            lease->directory_descriptor(),
            "segment-replaced.raw") == 0 &&
            ::symlinkat(
                "segment-replaced.raw",
                lease->directory_descriptor(),
                "segment-00000002.raw") == 0,
        "final-name symlink replacement fixture is created");
    const int replacement_error =
        source->InspectSegment(2U, &info);
    Expect(
        replacement_error == EINVAL ||
            replacement_error == ESTALE ||
            replacement_error == ELOOP,
        "cached segment fd fails closed when final name is replaced");
}

}  // namespace

int main() {
    TestPosixTailRotationAndControlReattach();
    TestRetainedInodeAndStrictNameFailures();
    if (failures != 0) {
        std::cerr << failures
                  << " Phase 2 Raw POSIX live-tail tests failed\n";
        return 1;
    }
    std::cout
        << "Phase 2 Raw POSIX live-tail tests passed\n";
    return 0;
}
