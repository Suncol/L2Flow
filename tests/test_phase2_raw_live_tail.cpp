#include "l2flow/ingress/raw_live_tail.h"
#include "l2flow/ingress/raw_v1.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

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

struct SegmentFixture final {
    ingress::SegmentHeaderV1 header{};
    std::vector<std::byte> bytes;
    bool sealed = false;
};

class MemorySource final : public ingress::RawLiveTailSource {
public:
    int ReadControl(
        ingress::RawControlSnapshot* output,
        std::uint64_t* generation) noexcept override {
        if (control_error != 0) {
            return control_error;
        }
        *output = control;
        *generation = control_generation;
        return 0;
    }

    int InspectSegment(
        std::uint32_t sequence,
        ingress::RawLiveSegmentInfo* info) noexcept override {
        const auto found = segments.find(sequence);
        if (found == segments.end()) {
            return ENOENT;
        }
        info->header = found->second.header;
        info->visible_end_offset =
            static_cast<std::uint64_t>(
                found->second.bytes.size());
        info->sealed = found->second.sealed;
        return 0;
    }

    ingress::RawLiveReadResult ReadSegmentSome(
        std::uint32_t sequence,
        std::uint64_t offset,
        std::span<std::byte> output) noexcept override {
        if (eintr_once) {
            eintr_once = false;
            return {0U, EINTR};
        }
        const auto found = segments.find(sequence);
        if (found == segments.end() ||
            offset > found->second.bytes.size()) {
            return {0U, ENOENT};
        }
        const std::size_t start =
            static_cast<std::size_t>(offset);
        const std::size_t count = std::min({
            output.size(),
            found->second.bytes.size() - start,
            maximum_read});
        std::copy_n(
            found->second.bytes.begin() +
                static_cast<std::ptrdiff_t>(start),
            count,
            output.begin());
        return {count, 0};
    }

    ingress::RawControlSnapshot control{};
    std::uint64_t control_generation = 2U;
    int control_error = 0;
    bool eintr_once = false;
    std::size_t maximum_read =
        std::numeric_limits<std::size_t>::max();
    std::unordered_map<std::uint32_t, SegmentFixture> segments;
};

ingress::SegmentHeaderV1 MakeHeader(
    std::uint32_t sequence,
    std::uint64_t base,
    std::uint64_t first) {
    ingress::SegmentHeaderV1 header{};
    header.source_stream_id = 1001U;
    header.capture_date = 20260718U;
    header.stream_day_id = Pattern<16U>(0x10U);
    header.segment_sequence = sequence;
    header.segment_base_wal_pos = base;
    header.first_ingress_sequence = first;
    header.created_realtime_ns = 1U;
    header.created_monotonic_ns = 2U;
    header.host_uuid = Pattern<16U>(0x20U);
    header.linux_boot_id = Pattern<16U>(0x30U);
    header.clock_epoch_algorithm = 1U;
    header.clock_epoch_digest = Pattern<32U>(0x40U);
    header.clock_epoch_label = 3U;
    header.sdk_archive_sha256 = Pattern<32U>(0x50U);
    header.libmdl_api_sha256 = Pattern<32U>(0x60U);
    header.endpoint_contract_sha256 = Pattern<32U>(0x70U);
    header.config_sha256 = Pattern<32U>(0x80U);
    header.raw_schema_sha256 = Pattern<32U>(0x90U);
    header.build_manifest_sha256 = Pattern<32U>(0xa0U);
    return header;
}

std::vector<std::byte> EncodeRecord(
    std::uint64_t ingress_sequence) {
    ingress::RawRecordInputV1 input{};
    input.meta.source_stream_id = 1001U;
    input.meta.capture_date = 20260718U;
    input.meta.ingress_sequence = ingress_sequence;
    input.meta.recv_realtime_ns =
        1000U + ingress_sequence;
    input.meta.recv_monotonic_ns =
        2000U + ingress_sequence;
    input.vendor_head.fill(std::byte{0});
    input.vendor_head[0U] = std::byte{23};
    const auto put_le =
        [&input](std::size_t offset,
                 std::uint64_t value,
                 std::size_t width) {
            for (std::size_t index = 0U;
                 index < width;
                 ++index) {
                input.vendor_head[offset + index] =
                    static_cast<std::byte>(
                        (value >> (index * 8U)) &
                        0xffU);
            }
        };
    put_le(1U, 27U, 4U);
    input.vendor_head[5U] = std::byte{4};
    input.vendor_head[6U] = std::byte{1};
    put_le(7U, 2U, 2U);
    put_le(9U, 3U, 2U);
    put_le(11U, 5U, 4U);
    const std::uint64_t vendor_sequence =
        6U + ingress_sequence;
    put_le(15U, vendor_sequence, 8U);
    const std::array<std::byte, 4U> body{
        std::byte{1}, std::byte{2},
        std::byte{3}, std::byte{4}};
    input.vendor_body = body;
    std::vector<std::byte> wire;
    const auto encoded =
        ingress::EncodeRawRecordV1(input, &wire, nullptr);
    Expect(
        encoded == ingress::RawV1Error::kNone,
        "record fixture encodes");
    return wire;
}

SegmentFixture MakeSegment(
    std::uint32_t sequence,
    std::uint64_t base,
    std::uint64_t first,
    std::span<const std::uint64_t> records,
    bool sealed) {
    SegmentFixture fixture;
    fixture.header = MakeHeader(sequence, base, first);
    ingress::RawV1SegmentHeaderWire header_wire{};
    Expect(
        ingress::EncodeSegmentHeaderV1(
            fixture.header, &header_wire) ==
            ingress::RawV1Error::kNone,
        "segment fixture header encodes");
    fixture.bytes.assign(
        header_wire.begin(), header_wire.end());
    for (const std::uint64_t record : records) {
        const std::vector<std::byte> wire =
            EncodeRecord(record);
        fixture.bytes.insert(
            fixture.bytes.end(),
            wire.begin(),
            wire.end());
    }
    fixture.sealed = sealed;
    return fixture;
}

ingress::RawLiveTailAttachV1 MakeAttach() {
    ingress::RawLiveTailAttachV1 attach;
    attach.writer_instance = Pattern<16U>(0xc0U);
    attach.stream_day_id = Pattern<16U>(0x10U);
    attach.source_stream_id = 1001U;
    attach.capture_date = 20260718U;
    attach.segment_sequence = 1U;
    attach.global_wal_pos =
        ingress::kRawV1SegmentHeaderBytes;
    attach.segment_offset = ingress::kRawV1SegmentHeaderBytes;
    attach.next_ingress_sequence = 1U;
    return attach;
}

void ConfigureControl(
    MemorySource* source,
    const ingress::RawLiveTailAttachV1& attach,
    std::uint32_t sequence,
    std::uint64_t base,
    std::uint64_t offset,
    std::uint64_t append_ingress,
    std::uint64_t durable_global,
    std::uint64_t durable_ingress) {
    source->control.writer_instance =
        attach.writer_instance;
    source->control.stream_day_id =
        attach.stream_day_id;
    source->control.source_stream_id =
        attach.source_stream_id;
    source->control.capture_date =
        attach.capture_date;
    source->control.segment_sequence = sequence;
    source->control.append_segment_offset = offset;
    source->control.append_global_wal_pos =
        base + offset;
    source->control.append_ingress_sequence =
        append_ingress;
    source->control.durable_global_wal_pos =
        durable_global;
    source->control.durable_segment_offset =
        durable_global - base;
    source->control.durable_ingress_sequence =
        durable_ingress;
}

void TestOwnedShortReadsAndProvenance() {
    MemorySource source;
    const auto attach = MakeAttach();
    const std::array<std::uint64_t, 2U> records{1U, 2U};
    source.segments.emplace(
        1U,
        MakeSegment(1U, 0U, 1U, records, false));
    const std::uint64_t end =
        source.segments.at(1U).bytes.size();
    const std::uint64_t first_end =
        ingress::kRawV1SegmentHeaderBytes +
        EncodeRecord(1U).size();
    ConfigureControl(
        &source,
        attach,
        1U,
        0U,
        end,
        2U,
        first_end,
        1U);
    source.maximum_read = 7U;
    source.eintr_once = true;

    std::unique_ptr<ingress::RawLiveTail> tail;
    Expect(
        ingress::RawLiveTail::Attach(
            &source, attach, &tail) ==
            ingress::RawLiveTailError::kNone,
        "live tail attaches to recovered cursor");
    auto first = tail->Next();
    Expect(
        first.kind ==
                ingress::RawLiveTailStepKind::kRecord &&
            first.record->view.header().ingress_sequence ==
                1U &&
            first.record->provenance ==
                ingress::RawLiveRecordProvenance::kDurable,
        "short-read/EINTR loop returns owned durable record");
    const auto retained = first.record->view.vendor_body();
    auto second = tail->Next();
    Expect(
        second.kind ==
                ingress::RawLiveTailStepKind::kRecord &&
            second.record->view.header().ingress_sequence ==
                2U &&
            second.record->provenance ==
                ingress::RawLiveRecordProvenance::kAppendVisible,
        "record beyond durable snapshot is append-visible");
    Expect(
        retained.size() == 4U &&
            retained[0U] == std::byte{1},
        "advancing tail does not invalidate prior owned view");
    Expect(
        tail->Next().kind ==
            ingress::RawLiveTailStepKind::kWouldBlock,
        "open tail is nonblocking at append cursor");
}

void TestRotationAndInstanceFence() {
    MemorySource source;
    const auto attach = MakeAttach();
    const std::array<std::uint64_t, 1U> first_records{1U};
    source.segments.emplace(
        1U,
        MakeSegment(1U, 0U, 1U, first_records, true));
    const std::uint64_t base2 =
        source.segments.at(1U).bytes.size();
    ConfigureControl(
        &source,
        attach,
        1U,
        0U,
        base2,
        1U,
        base2,
        1U);

    std::unique_ptr<ingress::RawLiveTail> tail;
    Expect(
        ingress::RawLiveTail::Attach(
            &source, attach, &tail) ==
            ingress::RawLiveTailError::kNone,
        "rotation fixture attaches");
    const auto first = tail->Next();
    const auto sealed_before_next_control = tail->Next();
    const std::array<std::uint64_t, 1U> second_records{2U};
    source.segments.emplace(
        2U,
        MakeSegment(2U, base2, 2U, second_records, false));
    const std::uint64_t end2 =
        source.segments.at(2U).bytes.size();
    ConfigureControl(
        &source,
        attach,
        2U,
        base2,
        end2,
        2U,
        base2 + end2,
        2U);
    source.control_generation += 2U;
    const auto transition = tail->Next();
    const auto second = tail->Next();
    Expect(
        first.kind ==
                ingress::RawLiveTailStepKind::kRecord &&
            sealed_before_next_control.kind ==
                ingress::RawLiveTailStepKind::kWouldBlock &&
            transition.kind ==
                ingress::RawLiveTailStepKind::
                    kSegmentTransition &&
            transition.segment_transition.has_value() &&
            transition.segment_transition
                    ->writer_instance ==
                attach.writer_instance &&
            transition.segment_transition
                    ->previous_segment
                    .segment_sequence == 1U &&
            transition.segment_transition
                    ->previous_segment_end_offset ==
                base2 &&
            transition.segment_transition
                    ->next_segment.segment_sequence ==
                2U &&
            transition.segment_transition
                    ->next_segment
                    .segment_base_wal_pos ==
                base2 &&
            transition.segment_transition
                    ->next_data_begin_wal_pos ==
                base2 +
                    ingress::kRawV1SegmentHeaderBytes &&
            transition.segment_transition
                    ->next_ingress_sequence == 2U &&
            second.kind ==
                ingress::RawLiveTailStepKind::kRecord &&
            second.record->segment.segment_sequence == 2U &&
            second.record->view.header().ingress_sequence ==
                2U,
        "a sealed-current rotation window stays retryable and the tail later emits a validated header-frontier fact before the next segment record");

    source.control.writer_instance =
        Pattern<16U>(0xd0U);
    const auto fenced = tail->Next();
    Expect(
        fenced.kind ==
                ingress::RawLiveTailStepKind::kInstanceChanged &&
            fenced.error ==
                ingress::RawLiveTailError::kInstanceChanged,
        "writer instance replacement fences the old live tail");
}

void TestRotationIdentityAndBaseFailures() {
    const auto attach = MakeAttach();
    const std::array<std::uint64_t, 1U> first_records{1U};
    const std::array<std::uint64_t, 0U> no_records{};

    MemorySource gap;
    gap.segments.emplace(
        1U,
        MakeSegment(1U, 0U, 1U, first_records, true));
    const std::uint64_t expected_base =
        gap.segments.at(1U).bytes.size();
    gap.segments.emplace(
        2U,
        MakeSegment(
            2U,
            expected_base + 1U,
            2U,
            no_records,
            false));
    ConfigureControl(
        &gap,
        attach,
        2U,
        expected_base,
        ingress::kRawV1SegmentHeaderBytes,
        1U,
        expected_base +
            ingress::kRawV1SegmentHeaderBytes,
        1U);
    std::unique_ptr<ingress::RawLiveTail> gap_tail;
    Expect(
        ingress::RawLiveTail::Attach(
            &gap, attach, &gap_tail) ==
                ingress::RawLiveTailError::kNone &&
            gap_tail->Next().kind ==
                ingress::RawLiveTailStepKind::kRecord,
        "base-gap fixture reaches the sealed boundary");
    const auto gap_rejected = gap_tail->Next();
    Expect(
        gap_rejected.kind ==
                ingress::RawLiveTailStepKind::kError &&
            gap_rejected.error ==
                ingress::RawLiveTailError::
                    kSegmentOrderViolation &&
            !gap_rejected.segment_transition.has_value(),
        "next header with a one-byte base gap is rejected before transition publication");

    MemorySource foreign;
    foreign.segments.emplace(
        1U,
        MakeSegment(1U, 0U, 1U, first_records, true));
    const std::uint64_t base2 =
        foreign.segments.at(1U).bytes.size();
    foreign.segments.emplace(
        2U,
        MakeSegment(
            2U, base2, 2U, no_records, false));
    foreign.segments.at(2U)
        .header.stream_day_id[0U] ^=
        std::byte{0x7fU};
    ConfigureControl(
        &foreign,
        attach,
        2U,
        base2,
        ingress::kRawV1SegmentHeaderBytes,
        1U,
        base2 + ingress::kRawV1SegmentHeaderBytes,
        1U);
    std::unique_ptr<ingress::RawLiveTail> foreign_tail;
    Expect(
        ingress::RawLiveTail::Attach(
            &foreign, attach, &foreign_tail) ==
                ingress::RawLiveTailError::kNone &&
            foreign_tail->Next().kind ==
                ingress::RawLiveTailStepKind::kRecord,
        "foreign-header fixture reaches the sealed boundary");
    const auto foreign_rejected =
        foreign_tail->Next();
    Expect(
        foreign_rejected.kind ==
                ingress::RawLiveTailStepKind::kError &&
            foreign_rejected.error ==
                ingress::RawLiveTailError::
                    kSegmentOrderViolation &&
            !foreign_rejected
                 .segment_transition.has_value(),
        "foreign stream-day next header is rejected before transition publication");
}

void TestPublicationAndCorruptionRejections() {
    MemorySource source;
    const auto attach = MakeAttach();
    const std::array<std::uint64_t, 1U> records{1U};
    source.segments.emplace(
        1U,
        MakeSegment(1U, 0U, 1U, records, false));
    const std::uint64_t end =
        source.segments.at(1U).bytes.size();
    ConfigureControl(
        &source,
        attach,
        1U,
        0U,
        end - ingress::kRawV1RecordAlignment,
        1U,
        ingress::kRawV1SegmentHeaderBytes,
        0U);
    std::unique_ptr<ingress::RawLiveTail> unpublished;
    Expect(
        ingress::RawLiveTail::Attach(
            &source, attach, &unpublished) ==
            ingress::RawLiveTailError::kNone &&
            unpublished->Next().error ==
                ingress::RawLiveTailError::
                    kRecordPastPublication,
        "tail never reads a record whose trailer is outside append publication");

    MemorySource corrupt;
    corrupt.segments.emplace(
        1U,
        MakeSegment(1U, 0U, 1U, records, false));
    corrupt.segments.at(1U).bytes.back() ^=
        std::byte{0x01};
    ConfigureControl(
        &corrupt,
        attach,
        1U,
        0U,
        corrupt.segments.at(1U).bytes.size(),
        1U,
        ingress::kRawV1SegmentHeaderBytes,
        0U);
    std::unique_ptr<ingress::RawLiveTail> invalid;
    Expect(
        ingress::RawLiveTail::Attach(
            &corrupt, attach, &invalid) ==
            ingress::RawLiveTailError::kNone,
        "corrupt fixture attaches before record validation");
    const auto rejected = invalid->Next();
    Expect(
        rejected.kind ==
                ingress::RawLiveTailStepKind::kError &&
            rejected.error ==
                ingress::RawLiveTailError::kRecordInvalid &&
            rejected.reader_error ==
                ingress::RawReaderError::kTrailerInvalid,
        "live tail validates trailer before exposing record");
}

void TestControlGenerationMustBeCoherent() {
    MemorySource source;
    const auto attach = MakeAttach();
    const std::array<std::uint64_t, 0U> no_records{};
    source.segments.emplace(
        1U,
        MakeSegment(1U, 0U, 1U, no_records, false));
    ConfigureControl(
        &source,
        attach,
        1U,
        0U,
        ingress::kRawV1SegmentHeaderBytes,
        0U,
        ingress::kRawV1SegmentHeaderBytes,
        0U);

    source.control_generation = 3U;
    std::unique_ptr<ingress::RawLiveTail> rejected;
    Expect(
        ingress::RawLiveTail::Attach(
            &source, attach, &rejected) ==
            ingress::RawLiveTailError::kControlCursorInvalid,
        "attach rejects an odd in-progress control generation");

    source.control_generation = 4U;
    std::unique_ptr<ingress::RawLiveTail> tail;
    Expect(
        ingress::RawLiveTail::Attach(
            &source, attach, &tail) ==
                ingress::RawLiveTailError::kNone &&
            tail != nullptr,
        "attach accepts an even coherent control generation");
    if (tail != nullptr) {
        source.control_generation = 5U;
        const auto sample = tail->SampleControlFresh();
        Expect(
            !sample.ok() &&
                sample.error ==
                    ingress::RawLiveTailError::kControlCursorInvalid,
            "fresh sampling rejects a subsequently observed odd generation");
    }
}

}  // namespace

int main() {
    TestOwnedShortReadsAndProvenance();
    TestRotationAndInstanceFence();
    TestRotationIdentityAndBaseFailures();
    TestPublicationAndCorruptionRejections();
    TestControlGenerationMustBeCoherent();
    if (failures != 0) {
        std::cerr << failures
                  << " Phase 2 Raw live-tail tests failed\n";
        return 1;
    }
    std::cout << "Phase 2 Raw live-tail tests passed\n";
    return 0;
}
