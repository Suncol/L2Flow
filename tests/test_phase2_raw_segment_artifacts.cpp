#include "l2flow/common/sha256.h"
#include "l2flow/ingress/raw_index_v1.h"
#include "l2flow/ingress/raw_segment_artifacts.h"
#include "l2flow/ingress/raw_v1.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <sys/stat.h>
#include <unistd.h>

namespace ingress = l2flow::ingress;

namespace {

constexpr int kDirectoryFd = 10;
constexpr int kSegmentFd = 11;
constexpr int kArtifactFd = 20;
constexpr std::uint64_t kTestUserId = 42U;
constexpr std::uint64_t kTestDevice = 7U;

struct TestContext final {
    void Expect(bool condition, const std::string& description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    int failures = 0;
};

template <std::size_t Size>
void FillNonzero(
    std::array<std::byte, Size>* bytes,
    std::uint8_t seed) {
    for (std::size_t index = 0U;
         index < bytes->size();
         ++index) {
        (*bytes)[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                seed + static_cast<std::uint8_t>(index)));
    }
}

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
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        bytes[offset + index] = static_cast<std::byte>(
            (value >>
             static_cast<unsigned int>(index * 8U)) &
            0xffU);
    }
}

void StoreU64(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint64_t value) {
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        bytes[offset + index] = static_cast<std::byte>(
            (value >>
             static_cast<unsigned int>(index * 8U)) &
            0xffU);
    }
}

ingress::RawV1VendorHead MakeVendorHead(
    std::uint32_t body_size,
    std::uint64_t vendor_sequence) {
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
    head[5U] = std::byte{2U};
    head[6U] = std::byte{3U};
    StoreU16(bytes, 7U, 101U);
    StoreU16(bytes, 9U, 3001U);
    StoreU32(bytes, 11U, 0x12345678U);
    StoreU64(bytes, 15U, vendor_sequence);
    return head;
}

struct SegmentFixture final {
    std::shared_ptr<std::vector<std::byte>> bytes;
    ingress::RawV1Digest raw_schema_sha256{};
    ingress::RawV1DurableMarkerWire seal{};
};

SegmentFixture MakeSegment(std::uint64_t record_count = 5U) {
    SegmentFixture fixture;
    fixture.bytes =
        std::make_shared<std::vector<std::byte>>();
    ingress::SegmentHeaderV1 header{};
    header.source_stream_id = 1001U;
    header.capture_date = 20260718U;
    FillNonzero(&header.stream_day_id, 1U);
    header.segment_sequence = 1U;
    header.segment_base_wal_pos = 0U;
    header.first_ingress_sequence = 1U;
    header.created_realtime_ns = 1000U;
    header.created_monotonic_ns = 900U;
    FillNonzero(&header.host_uuid, 21U);
    FillNonzero(&header.linux_boot_id, 41U);
    header.clock_epoch_algorithm = 1U;
    FillNonzero(&header.clock_epoch_digest, 61U);
    header.clock_epoch_label = 77U;
    FillNonzero(&header.sdk_archive_sha256, 81U);
    FillNonzero(&header.libmdl_api_sha256, 101U);
    FillNonzero(&header.endpoint_contract_sha256, 121U);
    FillNonzero(&header.config_sha256, 141U);
    FillNonzero(&header.raw_schema_sha256, 161U);
    FillNonzero(&header.build_manifest_sha256, 181U);
    fixture.raw_schema_sha256 =
        header.raw_schema_sha256;

    ingress::RawV1SegmentHeaderWire header_wire{};
    if (ingress::EncodeSegmentHeaderV1(
            header, &header_wire) !=
        ingress::RawV1Error::kNone) {
        return fixture;
    }
    fixture.bytes->assign(
        header_wire.begin(), header_wire.end());
    for (std::uint64_t sequence = 1U;
         sequence <= record_count;
         ++sequence) {
        std::vector<std::byte> body(
            static_cast<std::size_t>(3U + sequence),
            static_cast<std::byte>(
                static_cast<std::uint8_t>(
                    10U + sequence)));
        ingress::RawRecordInputV1 input{};
        input.meta.source_stream_id =
            header.source_stream_id;
        input.meta.connection_epoch_hint = 9U;
        input.meta.ingress_sequence = sequence;
        input.meta.recv_realtime_ns =
            2000U + sequence;
        input.meta.recv_monotonic_ns =
            1500U + sequence;
        input.meta.capture_date = header.capture_date;
        input.vendor_head = MakeVendorHead(
            static_cast<std::uint32_t>(body.size()),
            900U + sequence);
        input.vendor_body = body;
        std::vector<std::byte> record;
        if (ingress::EncodeRawRecordV1(
                input, &record) !=
            ingress::RawV1Error::kNone) {
            continue;
        }
        fixture.bytes->insert(
            fixture.bytes->end(),
            record.begin(),
            record.end());
    }

    ingress::DurableMarkerV1 marker{};
    marker.source_stream_id = header.source_stream_id;
    marker.segment_sequence = header.segment_sequence;
    marker.durable_global_wal_pos =
        fixture.bytes->size();
    marker.durable_ingress_sequence = record_count;
    marker.durable_segment_offset =
        fixture.bytes->size();
    marker.marker_flags = ingress::kRawV1SegmentSealed;
    static_cast<void>(ingress::EncodeDurableMarkerV1(
        marker, &fixture.seal));
    return fixture;
}

ingress::RawSegmentArtifactOptionsV1 Options(
    const SegmentFixture& fixture) {
    ingress::RawSegmentArtifactOptionsV1 options{};
    options.expected_raw_schema_sha256 =
        fixture.raw_schema_sha256;
    options.sample_record_interval = 2U;
    options.sample_raw_bytes_interval =
        ingress::kRawIndexV1DefaultRawBytesInterval;
    options.maximum_segment_bytes = 1U << 20U;
    return options;
}

ingress::RawIndexBuilderV1 MakeIncrementalBuilder(
    const SegmentFixture& fixture,
    const ingress::RawSegmentArtifactOptionsV1& options) {
    std::shared_ptr<const std::vector<std::byte>> bytes =
        fixture.bytes;
    const ingress::RawSegmentScanResult scan =
        ingress::ScanRawSegmentV1(
            bytes, fixture.bytes->size());
    ingress::RawIndexHeaderV1 header{};
    header.capture_date = scan.segment.capture_date;
    header.source_stream_id =
        scan.segment.source_stream_id;
    header.stream_day_id = scan.segment.stream_day_id;
    header.segment_sequence =
        scan.segment.segment_sequence;
    header.segment_base_wal_pos =
        scan.segment.segment_base_wal_pos;
    header.raw_schema_sha256 =
        scan.segment.raw_schema_sha256;
    header.sample_record_interval =
        options.sample_record_interval;
    header.sample_raw_bytes_interval =
        options.sample_raw_bytes_interval;
    ingress::RawIndexBuilderV1 builder(header);
    for (const ingress::RawRecordView& record :
         scan.records) {
        ingress::RawIndexEntryV1 entry{};
        entry.ingress_sequence =
            record.header().ingress_sequence;
        entry.record_start_wal_pos =
            record.record_start_wal_pos();
        entry.record_end_wal_pos =
            record.record_end_wal_pos();
        entry.segment_file_offset =
            record.record_start_offset();
        entry.vendor_sequence_id =
            record.header().vendor_sequence_id;
        entry.recv_monotonic_ns =
            record.header().recv_monotonic_ns;
        entry.connection_epoch_hint =
            record.header().connection_epoch_hint;
        entry.vendor_service_version =
            record.header().vendor_service_version;
        entry.vendor_message_id =
            record.header().vendor_message_id;
        entry.vendor_service_id =
            record.header().vendor_service_id;
        static_cast<void>(builder.AddRecord(entry));
    }
    return builder;
}

ingress::RawSegmentArtifactCausalProofV1 Proof(
    const SegmentFixture& fixture) {
    return {
        .segment_truncated_and_synced_to_logical_end = true,
        .accepted_seal_marker_journal_synced = true,
        .synced_segment_logical_end_offset =
            fixture.bytes->size(),
        .synced_segment_sha256 =
            l2flow::common::ComputeSha256(
                std::span<const std::byte>(
                    fixture.bytes->data(),
                    fixture.bytes->size())),
        .journal_synced_sealed_marker_bytes =
            fixture.seal};
}

class MemoryArtifactIo final
    : public ingress::RawSegmentArtifactIoV1 {
public:
    explicit MemoryArtifactIo(
        std::vector<std::byte> segment_bytes)
        : segment(std::move(segment_bytes)) {}

    std::uint64_t EffectiveUserId() const noexcept override {
        ++calls;
        return kTestUserId;
    }

    int InspectDescriptor(
        int descriptor,
        ingress::RawSegmentArtifactFileInfoV1* info) noexcept
        override {
        ++calls;
        if (info == nullptr) {
            return EINVAL;
        }
        if (descriptor == kDirectoryFd) {
            *info = DirectoryInfo();
            return 0;
        }
        if (descriptor == kSegmentFd) {
            ++segment_inspections;
            *info = SegmentInfo();
            if (change_segment_after_first_inspection &&
                segment_inspections >= 2U) {
                ++info->change_nanoseconds;
            }
            return 0;
        }
        if (descriptor == kArtifactFd &&
            artifact_open) {
            *info = ArtifactInfo(
                OpenArtifactBytes().size(),
                artifact_writable);
            return 0;
        }
        return EBADF;
    }

    int InspectName(
        int,
        std::string_view name,
        ingress::RawSegmentArtifactFileInfoV1* info) noexcept
        override {
        ++calls;
        if (name == final_name && final.has_value()) {
            *info = ArtifactInfo(
                final->size(), false);
            info->open_flags = -1;
            return 0;
        }
        if (name == temporary_name &&
            temporary.has_value()) {
            *info = ArtifactInfo(
                temporary->size(), true);
            info->open_flags = -1;
            return 0;
        }
        return ENOENT;
    }

    ingress::RawSegmentArtifactOpenResultV1
    OpenExisting(
        int,
        std::string_view name,
        bool writable) noexcept override {
        ++calls;
        events.emplace_back(
            name == final_name
                ? "open-final"
                : "open-temporary");
        if (name == final_name && final.has_value()) {
            artifact_open = true;
            artifact_is_final = true;
            artifact_writable = writable;
            return {kArtifactFd, 0};
        }
        if (name == temporary_name &&
            temporary.has_value()) {
            artifact_open = true;
            artifact_is_final = false;
            artifact_writable = writable;
            return {kArtifactFd, 0};
        }
        return {-1, ENOENT};
    }

    ingress::RawSegmentArtifactOpenResultV1
    CreateExclusive(
        int,
        std::string_view name) noexcept override {
        ++calls;
        events.emplace_back("create-temporary");
        if (name != temporary_name ||
            temporary.has_value() ||
            final.has_value() ||
            Fail("create-temporary")) {
            return {-1, temporary.has_value() ||
                            final.has_value()
                        ? EEXIST
                        : EIO};
        }
        temporary.emplace();
        artifact_open = true;
        artifact_is_final = false;
        artifact_writable = true;
        return {kArtifactFd, 0};
    }

    ingress::RawSegmentArtifactIoStepV1 ReadSome(
        int descriptor,
        std::uint64_t offset,
        std::span<std::byte> output) noexcept override {
        ++calls;
        if (read_eintr_once) {
            read_eintr_once = false;
            return {0U, EINTR};
        }
        const std::vector<std::byte>* source = nullptr;
        if (descriptor == kSegmentFd) {
            source = &segment;
        } else if (
            descriptor == kArtifactFd &&
            artifact_open) {
            source = &OpenArtifactBytes();
        } else {
            return {0U, EBADF};
        }
        if (offset >= source->size()) {
            return {0U, 0};
        }
        const std::size_t available =
            source->size() -
            static_cast<std::size_t>(offset);
        const std::size_t count = std::min(
            {output.size(), available, maximum_read});
        std::copy_n(
            source->begin() +
                static_cast<std::ptrdiff_t>(offset),
            count,
            output.begin());
        if (descriptor == kSegmentFd) {
            segment_read_bytes += count;
        }
        return {count, 0};
    }

    ingress::RawSegmentArtifactIoStepV1 WriteSome(
        int descriptor,
        std::uint64_t offset,
        std::span<const std::byte> input) noexcept override {
        ++calls;
        events.emplace_back("write");
        if (write_eintr_once) {
            write_eintr_once = false;
            return {0U, EINTR};
        }
        if (descriptor != kArtifactFd ||
            !artifact_open ||
            artifact_is_final ||
            !artifact_writable ||
            !temporary.has_value()) {
            return {0U, EBADF};
        }
        if (write_zero_once) {
            write_zero_once = false;
            return {0U, 0};
        }
        const std::size_t count =
            std::min(input.size(), maximum_write);
        if (offset >
                std::numeric_limits<std::size_t>::max() ||
            count >
                std::numeric_limits<std::size_t>::max() -
                    static_cast<std::size_t>(offset)) {
            return {0U, EOVERFLOW};
        }
        const std::size_t start =
            static_cast<std::size_t>(offset);
        if (temporary->size() < start + count) {
            temporary->resize(start + count);
        }
        std::copy_n(
            input.begin(),
            count,
            temporary->begin() +
                static_cast<std::ptrdiff_t>(start));
        return {count, 0};
    }

    int SyncFile(int descriptor) noexcept override {
        ++calls;
        events.emplace_back("sync-file");
        return descriptor == kArtifactFd &&
                       !Fail("sync-file")
                   ? 0
                   : EIO;
    }

    int SyncDirectory(int descriptor) noexcept override {
        ++calls;
        events.emplace_back("sync-directory");
        return descriptor == kDirectoryFd &&
                       !Fail("sync-directory")
                   ? 0
                   : EIO;
    }

    int RenameNoReplace(
        int,
        std::string_view old_name,
        std::string_view new_name) noexcept override {
        ++calls;
        events.emplace_back("rename");
        if (Fail("rename")) {
            return EIO;
        }
        if (old_name != temporary_name ||
            new_name != final_name ||
            !temporary.has_value() ||
            final.has_value()) {
            return EEXIST;
        }
        final = std::move(temporary);
        temporary.reset();
        artifact_is_final = true;
        return 0;
    }

    int NameMatchesDescriptor(
        int,
        std::string_view name,
        int descriptor) noexcept override {
        ++calls;
        if (descriptor == kSegmentFd) {
            return name == segment_name &&
                           !segment_name_mismatch
                       ? 0
                       : ESTALE;
        }
        if (descriptor != kArtifactFd ||
            !artifact_open) {
            return EBADF;
        }
        if (name == final_name) {
            return artifact_is_final &&
                           final.has_value() &&
                           !final_name_mismatch
                       ? 0
                       : ESTALE;
        }
        if (name == temporary_name) {
            return !artifact_is_final &&
                           temporary.has_value()
                       ? 0
                       : ESTALE;
        }
        return ESTALE;
    }

    void Close(int descriptor) noexcept override {
        ++calls;
        if (descriptor == kArtifactFd) {
            artifact_open = false;
        }
    }

    static ingress::RawSegmentArtifactFileInfoV1
    DirectoryInfo() {
        ingress::RawSegmentArtifactFileInfoV1 info{};
        info.directory = true;
        info.owner_user_id = kTestUserId;
        info.permission_bits = 0700U;
        info.link_count = 2U;
        info.device = kTestDevice;
        info.inode = 1U;
        info.open_flags = O_RDONLY;
        info.descriptor_flags = FD_CLOEXEC;
        return info;
    }

    ingress::RawSegmentArtifactFileInfoV1
    SegmentInfo() const {
        ingress::RawSegmentArtifactFileInfoV1 info{};
        info.regular_file = true;
        info.owner_user_id = kTestUserId;
        info.permission_bits = 0600U;
        info.link_count = 1U;
        info.size = segment.size();
        info.device = kTestDevice;
        info.inode = 2U;
        info.open_flags = O_RDWR | O_NONBLOCK;
        info.descriptor_flags = FD_CLOEXEC;
        return info;
    }

    static ingress::RawSegmentArtifactFileInfoV1
    ArtifactInfo(std::size_t size, bool writable) {
        ingress::RawSegmentArtifactFileInfoV1 info{};
        info.regular_file = true;
        info.owner_user_id = kTestUserId;
        info.permission_bits = 0600U;
        info.link_count = 1U;
        info.size = size;
        info.device = kTestDevice;
        info.inode = 3U;
        info.open_flags =
            (writable ? O_RDWR : O_RDONLY) |
            O_NONBLOCK | O_NOATIME;
        info.descriptor_flags = FD_CLOEXEC;
        return info;
    }

    bool Fail(std::string_view event) {
        if (!failure_event.empty() &&
            failure_event == event &&
            !failure_consumed) {
            failure_consumed = true;
            return true;
        }
        return false;
    }

    std::vector<std::byte>& OpenArtifactBytes() {
        return artifact_is_final
                   ? *final
                   : *temporary;
    }
    const std::vector<std::byte>&
    OpenArtifactBytes() const {
        return artifact_is_final
                   ? *final
                   : *temporary;
    }

    mutable std::size_t calls = 0U;
    std::size_t segment_inspections = 0U;
    std::size_t segment_read_bytes = 0U;
    std::vector<std::string> events;
    std::vector<std::byte> segment;
    std::optional<std::vector<std::byte>> temporary;
    std::optional<std::vector<std::byte>> final;
    std::size_t maximum_read =
        std::numeric_limits<std::size_t>::max();
    std::size_t maximum_write =
        std::numeric_limits<std::size_t>::max();
    bool read_eintr_once = false;
    bool write_eintr_once = false;
    bool write_zero_once = false;
    bool change_segment_after_first_inspection = false;
    bool segment_name_mismatch = false;
    bool final_name_mismatch = false;
    std::string failure_event;
    bool failure_consumed = false;
    bool artifact_open = false;
    bool artifact_is_final = false;
    bool artifact_writable = false;
    const std::string segment_name =
        "segment-00000001.raw";
    const std::string final_name =
        "segment-00000001.idx";
    const std::string temporary_name =
        ".segment-00000001.idx.raw-index.tmp";
};

ingress::RawSegmentArtifactPlanV1 Prepare(
    const SegmentFixture& fixture,
    MemoryArtifactIo* io) {
    return ingress::PrepareRawSegmentArtifactPlanForFdV1(
        kDirectoryFd,
        kSegmentFd,
        fixture.seal,
        Options(fixture),
        *io);
}

void TestPurePlan(TestContext* test) {
    SegmentFixture fixture = MakeSegment();
    std::shared_ptr<const std::vector<std::byte>> immutable =
        fixture.bytes;
    const ingress::RawSegmentArtifactPlanV1 plan =
        ingress::BuildRawSegmentArtifactPlanV1(
            immutable, fixture.seal, Options(fixture));
    test->Expect(
        plan.ok() &&
            plan.metadata.record_count == 5U &&
            plan.metadata.actual_first_ingress_sequence ==
                std::optional<std::uint64_t>(1U) &&
            plan.metadata.actual_last_ingress_sequence ==
                std::optional<std::uint64_t>(5U) &&
            plan.metadata.segment_sha256 ==
                l2flow::common::ComputeSha256(
                    std::span<const std::byte>(
                        fixture.bytes->data(),
                        fixture.bytes->size())) &&
            !plan.retained_segment_fd_bound,
        "pure plan validates and hashes the exact logical segment");
    ingress::RawIndexFileV1 index{};
    test->Expect(
        ingress::DecodeRawIndexFileV1(
            plan.index_bytes, &index) ==
                ingress::RawIndexV1Error::kNone &&
            index.entries.size() == 3U &&
            index.entries[0U].ingress_sequence == 1U &&
            index.entries[1U].ingress_sequence == 3U &&
            index.entries[2U].ingress_sequence == 5U &&
            index.footer.accepted_segment_sealed_marker ==
                fixture.seal,
        "sparse index includes first, strict threshold, final and exact seal");

    ingress::RawSegmentArtifactOptionsV1 byte_options =
        Options(fixture);
    byte_options.sample_record_interval =
        ingress::kRawIndexV1DefaultRecordInterval;
    byte_options.sample_raw_bytes_interval = 200U;
    const auto byte_plan =
        ingress::BuildRawSegmentArtifactPlanV1(
            immutable, fixture.seal, byte_options);
    ingress::RawIndexFileV1 byte_index{};
    test->Expect(
        byte_plan.ok() &&
            ingress::DecodeRawIndexFileV1(
                byte_plan.index_bytes, &byte_index) ==
                ingress::RawIndexV1Error::kNone &&
            byte_index.entries.size() == 3U &&
            byte_index.entries[1U].ingress_sequence == 3U,
        "raw-byte threshold independently samples before the record ceiling");

    ingress::RawV1DurableMarkerWire wrong_seal =
        fixture.seal;
    ingress::DurableMarkerV1 marker{};
    static_cast<void>(ingress::DecodeDurableMarkerV1(
        wrong_seal, &marker));
    ++marker.durable_segment_offset;
    ++marker.durable_global_wal_pos;
    static_cast<void>(ingress::EncodeDurableMarkerV1(
        marker, &wrong_seal));
    const auto seal_rejected =
        ingress::BuildRawSegmentArtifactPlanV1(
            immutable, wrong_seal, Options(fixture));
    test->Expect(
        seal_rejected.failure ==
            ingress::RawSegmentArtifactFailureV1::
                kSealMarkerMismatch,
        "a valid but non-exact seal marker is rejected");

    auto corrupt =
        std::make_shared<std::vector<std::byte>>(
            *fixture.bytes);
    (*corrupt)[ingress::kRawV1SegmentHeaderBytes + 100U] ^=
        std::byte{1U};
    std::shared_ptr<const std::vector<std::byte>>
        corrupt_immutable = corrupt;
    const auto raw_rejected =
        ingress::BuildRawSegmentArtifactPlanV1(
            corrupt_immutable,
            fixture.seal,
            Options(fixture));
    test->Expect(
        raw_rejected.failure ==
            ingress::RawSegmentArtifactFailureV1::
                kRawScanInvalid,
        "planner never trusts an index to skip a corrupt Raw prefix");

    SegmentFixture empty = MakeSegment(0U);
    std::shared_ptr<const std::vector<std::byte>> empty_bytes =
        empty.bytes;
    const auto empty_plan =
        ingress::BuildRawSegmentArtifactPlanV1(
            empty_bytes, empty.seal, Options(empty));
    test->Expect(
        empty_plan.ok() &&
            empty_plan.metadata.record_count == 0U &&
            !empty_plan.metadata
                 .actual_first_ingress_sequence.has_value() &&
            !empty_plan.metadata
                 .actual_last_ingress_sequence.has_value(),
        "header-only sealed segment preserves null actual range");
}

void TestRetainedFdPreparation(TestContext* test) {
    SegmentFixture fixture = MakeSegment();
    MemoryArtifactIo io(*fixture.bytes);
    io.maximum_read = 17U;
    io.read_eintr_once = true;
    const auto plan = Prepare(fixture, &io);
    test->Expect(
        plan.ok() &&
            plan.retained_segment_fd_bound &&
            plan.retained_segment_snapshot.size ==
                fixture.bytes->size(),
        "retained fd loader handles EINTR/short reads and binds stable metadata");

    MemoryArtifactIo changed(*fixture.bytes);
    changed.change_segment_after_first_inspection = true;
    const auto rejected = Prepare(fixture, &changed);
    test->Expect(
        rejected.failure ==
            ingress::RawSegmentArtifactFailureV1::
                kSegmentChanged,
        "metadata change during bounded read fails closed");

    MemoryArtifactIo replaced(*fixture.bytes);
    replaced.segment_name_mismatch = true;
    const auto path_rejected = Prepare(fixture, &replaced);
    test->Expect(
        path_rejected.failure ==
            ingress::RawSegmentArtifactFailureV1::
                kSegmentPathMismatch,
        "retained segment must still be named by the exact final pathname");
}

void TestIncrementalWriterPlan(TestContext* test) {
    SegmentFixture fixture = MakeSegment();
    std::shared_ptr<const std::vector<std::byte>> immutable =
        fixture.bytes;
    const auto scanned =
        ingress::BuildRawSegmentArtifactPlanV1(
            immutable, fixture.seal, Options(fixture));
    test->Expect(
        scanned.ok(),
        "incremental fixture derives independently validated facts");
    const ingress::RawIndexBuilderV1 builder =
        MakeIncrementalBuilder(fixture, Options(fixture));
    auto incremental =
        ingress::BuildIncrementalRawSegmentArtifactPlanV1(
            scanned.metadata,
            builder,
            Options(fixture));
    test->Expect(
        incremental.ok() &&
            !incremental.retained_segment_fd_bound,
        "incremental digest/index evidence validates without a fd scan");
    MemoryArtifactIo io(*fixture.bytes);
    io.maximum_read = 19U;
    auto bound =
        ingress::BindRawSegmentArtifactPlanToFdV1(
            kDirectoryFd,
            kSegmentFd,
            std::move(incremental),
            io);
    test->Expect(
        bound.ok() &&
            bound.retained_segment_fd_bound &&
            io.segment_read_bytes ==
                ingress::kRawV1SegmentHeaderBytes &&
            io.segment_read_bytes < fixture.bytes->size(),
        "normal R6/R7 binding reads only the exact segment header");

    ingress::RawSealedSegmentMetadataV1 wrong_metadata =
        scanned.metadata;
    wrong_metadata.segment_sha256[0U] ^= std::byte{1U};
    auto wrong_hash =
        ingress::BuildIncrementalRawSegmentArtifactPlanV1(
            wrong_metadata,
            builder,
            Options(fixture));
    MemoryArtifactIo wrong_hash_io(*fixture.bytes);
    wrong_hash =
        ingress::BindRawSegmentArtifactPlanToFdV1(
            kDirectoryFd,
            kSegmentFd,
            std::move(wrong_hash),
            wrong_hash_io);
    wrong_hash_io.calls = 0U;
    const auto wrong_hash_publish =
        ingress::PublishRawSegmentArtifactPlanV1(
            kDirectoryFd,
            kSegmentFd,
            wrong_hash,
            Proof(fixture),
            wrong_hash_io);
    test->Expect(
        wrong_hash.ok() &&
            wrong_hash_publish.failure ==
            ingress::RawSegmentArtifactFailureV1::
                kCausalProofMissing &&
            wrong_hash_io.calls == 0U,
        "incremental hash must equal the exact synchronized causal proof");

    auto valid_again =
        ingress::BuildIncrementalRawSegmentArtifactPlanV1(
            scanned.metadata,
            builder,
            Options(fixture));
    MemoryArtifactIo wrong_header(*fixture.bytes);
    wrong_header.segment[0U] ^= std::byte{1U};
    const auto header_rejected =
        ingress::BindRawSegmentArtifactPlanToFdV1(
            kDirectoryFd,
            kSegmentFd,
            std::move(valid_again),
            wrong_header);
    test->Expect(
        header_rejected.failure ==
            ingress::RawSegmentArtifactFailureV1::
                kSegmentChanged,
        "incremental plan cannot bind to a different retained header");
}

void TestPublicationAndCrashWindows(TestContext* test) {
    SegmentFixture fixture = MakeSegment();
    MemoryArtifactIo io(*fixture.bytes);
    ingress::RawSegmentArtifactPlanV1 plan =
        Prepare(fixture, &io);
    test->Expect(plan.ok(), "publication fixture prepares");
    io.calls = 0U;
    io.events.clear();

    const auto missing_proof =
        ingress::PublishRawSegmentArtifactPlanV1(
            kDirectoryFd,
            kSegmentFd,
            plan,
            {},
            io);
    test->Expect(
        missing_proof.failure ==
                ingress::RawSegmentArtifactFailureV1::
                    kCausalProofMissing &&
            io.calls == 0U,
        "missing segment/seal barriers cause zero syscall");

    ingress::RawSegmentArtifactCausalProofV1 wrong_proof =
        Proof(fixture);
    ++wrong_proof.synced_segment_logical_end_offset;
    const auto mismatched_proof =
        ingress::PublishRawSegmentArtifactPlanV1(
            kDirectoryFd,
            kSegmentFd,
            plan,
            wrong_proof,
            io);
    test->Expect(
        mismatched_proof.failure ==
                ingress::RawSegmentArtifactFailureV1::
                    kCausalProofMissing &&
            io.calls == 0U,
        "causal proof is bound to the exact logical end and seal bytes");

    io.maximum_write = 7U;
    io.write_eintr_once = true;
    const auto published =
        ingress::PublishRawSegmentArtifactPlanV1(
            kDirectoryFd,
            kSegmentFd,
            plan,
            Proof(fixture),
            io);
    const auto sync =
        std::find(
            io.events.begin(), io.events.end(), "sync-file");
    const auto rename =
        std::find(
            io.events.begin(), io.events.end(), "rename");
    const auto dirsync =
        std::find(
            io.events.begin(),
            io.events.end(),
            "sync-directory");
    test->Expect(
        published.ok() &&
            published.disposition ==
                ingress::RawSegmentArtifactDispositionV1::
                    kPublishedNew &&
            published.namespace_mutated &&
            published.directory_synced &&
            io.final ==
                std::optional<std::vector<std::byte>>(
                    plan.index_bytes) &&
            sync < rename && rename < dirsync,
        "short/EINTR writes complete before file sync, NOREPLACE and actual-parent sync");

    const auto idempotent =
        ingress::PublishRawSegmentArtifactPlanV1(
            kDirectoryFd,
            kSegmentFd,
            plan,
            Proof(fixture),
            io);
    test->Expect(
        idempotent.ok() &&
            idempotent.disposition ==
                ingress::RawSegmentArtifactDispositionV1::
                    kAcceptedExistingFinal &&
            !idempotent.namespace_mutated &&
            idempotent.directory_synced,
        "exact existing final is read back and idempotently accepted");

    MemoryArtifactIo file_sync_crash(*fixture.bytes);
    file_sync_crash.failure_event = "sync-file";
    const auto sync_failed =
        ingress::PublishRawSegmentArtifactPlanV1(
            kDirectoryFd,
            kSegmentFd,
            plan,
            Proof(fixture),
            file_sync_crash);
    test->Expect(
        sync_failed.failure ==
                ingress::RawSegmentArtifactFailureV1::
                    kArtifactSync &&
            file_sync_crash.temporary ==
                std::optional<std::vector<std::byte>>(
                    plan.index_bytes) &&
            !file_sync_crash.final.has_value(),
        "file-sync crash window preserves the exact typed temporary");
    file_sync_crash.failure_event.clear();
    file_sync_crash.failure_consumed = false;
    const auto adopted =
        ingress::PublishRawSegmentArtifactPlanV1(
            kDirectoryFd,
            kSegmentFd,
            plan,
            Proof(fixture),
            file_sync_crash);
    test->Expect(
        adopted.ok() &&
            adopted.disposition ==
                ingress::RawSegmentArtifactDispositionV1::
                    kAdoptedCompleteTemporary &&
            file_sync_crash.final.has_value() &&
            !file_sync_crash.temporary.has_value(),
        "complete tmp is fsynced and adopted without regeneration");

    MemoryArtifactIo dir_sync_crash(*fixture.bytes);
    dir_sync_crash.failure_event = "sync-directory";
    const auto dir_failed =
        ingress::PublishRawSegmentArtifactPlanV1(
            kDirectoryFd,
            kSegmentFd,
            plan,
            Proof(fixture),
            dir_sync_crash);
    test->Expect(
        dir_failed.failure ==
                ingress::RawSegmentArtifactFailureV1::
                    kDirectorySync &&
            dir_failed.namespace_mutated &&
            dir_sync_crash.final ==
                std::optional<std::vector<std::byte>>(
                    plan.index_bytes) &&
            !dir_sync_crash.temporary.has_value(),
        "rename-before-dirsync crash window leaves one exact final");
    dir_sync_crash.failure_event.clear();
    dir_sync_crash.failure_consumed = false;
    const auto recovered_final =
        ingress::PublishRawSegmentArtifactPlanV1(
            kDirectoryFd,
            kSegmentFd,
            plan,
            Proof(fixture),
            dir_sync_crash);
    test->Expect(
        recovered_final.ok() &&
            recovered_final.disposition ==
                ingress::RawSegmentArtifactDispositionV1::
                    kAcceptedExistingFinal &&
            recovered_final.directory_synced,
        "retry re-establishes the missing directory barrier by exact final readback");
}

void TestConflictsAndFaultSeams(TestContext* test) {
    SegmentFixture fixture = MakeSegment();
    MemoryArtifactIo preparing(*fixture.bytes);
    const auto plan = Prepare(fixture, &preparing);
    test->Expect(plan.ok(), "conflict fixture prepares");

    MemoryArtifactIo partial(*fixture.bytes);
    partial.temporary =
        std::vector<std::byte>(
            plan.index_bytes.begin(),
            plan.index_bytes.begin() +
                static_cast<std::ptrdiff_t>(
                    plan.index_bytes.size() / 2U));
    const auto partial_result =
        ingress::PublishRawSegmentArtifactPlanV1(
            kDirectoryFd,
            kSegmentFd,
            plan,
            Proof(fixture),
            partial);
    test->Expect(
        partial_result.failure ==
                ingress::RawSegmentArtifactFailureV1::
                    kTemporaryConflict &&
            partial.temporary.has_value() &&
            !partial.final.has_value(),
        "partial typed tmp is preserved and never treated as causal proof");

    MemoryArtifactIo conflict(*fixture.bytes);
    conflict.final = plan.index_bytes;
    (*conflict.final)[0U] ^= std::byte{1U};
    const auto conflict_result =
        ingress::PublishRawSegmentArtifactPlanV1(
            kDirectoryFd,
            kSegmentFd,
            plan,
            Proof(fixture),
            conflict);
    test->Expect(
        conflict_result.failure ==
                ingress::RawSegmentArtifactFailureV1::
                    kFinalConflict &&
            !conflict_result.namespace_mutated,
        "same-size conflicting final is fatal and never overwritten");

    MemoryArtifactIo ambiguous(*fixture.bytes);
    ambiguous.final = plan.index_bytes;
    ambiguous.temporary = plan.index_bytes;
    const auto ambiguous_result =
        ingress::PublishRawSegmentArtifactPlanV1(
            kDirectoryFd,
            kSegmentFd,
            plan,
            Proof(fixture),
            ambiguous);
    test->Expect(
        ambiguous_result.failure ==
            ingress::RawSegmentArtifactFailureV1::
                kAmbiguousFinalAndTemporary,
        "simultaneous final/tmp state is rejected");

    MemoryArtifactIo zero_write(*fixture.bytes);
    zero_write.write_zero_once = true;
    const auto zero_result =
        ingress::PublishRawSegmentArtifactPlanV1(
            kDirectoryFd,
            kSegmentFd,
            plan,
            Proof(fixture),
            zero_write);
    test->Expect(
        zero_result.failure ==
                ingress::RawSegmentArtifactFailureV1::
                    kArtifactWrite &&
            zero_write.temporary.has_value() &&
            zero_write.temporary->empty(),
        "zero-byte nonempty write is fatal and leaves typed evidence");

    MemoryArtifactIo rename_failure(*fixture.bytes);
    rename_failure.failure_event = "rename";
    const auto rename_result =
        ingress::PublishRawSegmentArtifactPlanV1(
            kDirectoryFd,
            kSegmentFd,
            plan,
            Proof(fixture),
            rename_failure);
    test->Expect(
        rename_result.failure ==
                ingress::RawSegmentArtifactFailureV1::
                    kArtifactRename &&
            rename_failure.temporary ==
                std::optional<std::vector<std::byte>>(
                    plan.index_bytes) &&
            !rename_failure.final.has_value(),
        "NOREPLACE failure never overwrites or discards the tmp");

    MemoryArtifactIo inode_failure(*fixture.bytes);
    inode_failure.final_name_mismatch = true;
    const auto inode_result =
        ingress::PublishRawSegmentArtifactPlanV1(
            kDirectoryFd,
            kSegmentFd,
            plan,
            Proof(fixture),
            inode_failure);
    test->Expect(
        inode_result.failure ==
                ingress::RawSegmentArtifactFailureV1::
                    kNameToInodeMismatch &&
            inode_result.namespace_mutated &&
            inode_result.directory_synced,
        "post-publication name-to-inode mismatch fails after recorded barriers");
}

bool PwriteAll(
    int descriptor,
    std::span<const std::byte> bytes) {
    std::size_t completed = 0U;
    while (completed < bytes.size()) {
        const ssize_t count = ::pwrite(
            descriptor,
            bytes.data() + completed,
            bytes.size() - completed,
            static_cast<off_t>(completed));
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            return false;
        }
        completed += static_cast<std::size_t>(count);
    }
    return true;
}

void TestPosixComposition(TestContext* test) {
    SegmentFixture fixture = MakeSegment();
    std::array<char, 64U> path{};
    const char* pattern =
        "/tmp/l2flow-raw-index-test-XXXXXX";
    std::copy_n(
        pattern,
        std::strlen(pattern) + 1U,
        path.begin());
    char* created_directory = ::mkdtemp(path.data());
    test->Expect(
        created_directory != nullptr,
        "POSIX fixture directory is created");
    if (created_directory == nullptr) {
        return;
    }
    const int directory_fd = ::open(
        created_directory,
        O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    const int segment_fd =
        directory_fd < 0
            ? -1
            : ::openat(
                  directory_fd,
                  "segment-00000001.raw",
                  O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW |
                      O_NONBLOCK | O_CLOEXEC,
                  0600);
    const bool initialized =
        directory_fd >= 0 &&
        segment_fd >= 0 &&
        PwriteAll(segment_fd, *fixture.bytes) &&
        ::fsync(segment_fd) == 0 &&
        ::fsync(directory_fd) == 0;
    test->Expect(
        initialized,
        "POSIX retained segment has completed file/name barriers");
    if (initialized) {
        const auto scanned =
            ingress::BuildRawSegmentArtifactPlanV1(
                fixture.bytes,
                fixture.seal,
                Options(fixture));
        const ingress::RawIndexBuilderV1 builder =
            MakeIncrementalBuilder(
                fixture, Options(fixture));
        auto incremental =
            ingress::BuildIncrementalRawSegmentArtifactPlanV1(
                scanned.metadata,
                builder,
                Options(fixture));
        const auto first =
            ingress::
                BindAndPublishIncrementalRawSegmentArtifactAtV1(
                directory_fd,
                std::move(incremental),
                Proof(fixture));
        test->Expect(
            scanned.ok() &&
                first.ok() &&
                first.disposition ==
                    ingress::
                        RawSegmentArtifactDispositionV1::
                            kPublishedNew &&
                first.directory_synced,
            "normal POSIX composition binds only incremental evidence and publishes a new index");
        const auto second =
            ingress::BuildAndPublishRawSegmentArtifactAtV1(
                directory_fd,
                segment_fd,
                fixture.seal,
                Options(fixture),
                Proof(fixture));
        test->Expect(
            second.ok() &&
                second.disposition ==
                    ingress::
                        RawSegmentArtifactDispositionV1::
                            kAcceptedExistingFinal &&
                second.metadata.segment_sha256 ==
                    first.metadata.segment_sha256,
            "production POSIX composition accepts only its exact existing final");
    }
    if (directory_fd >= 0) {
        static_cast<void>(::unlinkat(
            directory_fd,
            ".segment-00000001.idx.raw-index.tmp",
            0));
        static_cast<void>(::unlinkat(
            directory_fd,
            "segment-00000001.idx",
            0));
        static_cast<void>(::unlinkat(
            directory_fd,
            "segment-00000001.raw",
            0));
    }
    if (segment_fd >= 0) {
        static_cast<void>(::close(segment_fd));
    }
    if (directory_fd >= 0) {
        static_cast<void>(::close(directory_fd));
    }
    static_cast<void>(::rmdir(created_directory));
}

}  // namespace

int main() {
    TestContext test;
    TestPurePlan(&test);
    TestRetainedFdPreparation(&test);
    TestIncrementalWriterPlan(&test);
    TestPublicationAndCrashWindows(&test);
    TestConflictsAndFaultSeams(&test);
    TestPosixComposition(&test);

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Phase 2 Raw segment artifact tests failed\n";
        return 1;
    }
    std::cout
        << "Phase 2 Raw segment artifact tests passed\n";
    return 0;
}
