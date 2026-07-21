#include "raw_v1_fuzz_harness.h"

#include "l2flow/ingress/raw_reader.h"
#include "l2flow/ingress/raw_recovery.h"
#include "l2flow/ingress/raw_v1.h"

#include <algorithm>
#include <array>
#include <cstdlib>
#include <limits>
#include <memory>
#include <new>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace l2flow::test {
namespace {

namespace ingress = l2flow::ingress;

[[noreturn]] void PropertyFailure() noexcept {
    std::abort();
}

void Require(bool condition) noexcept {
    if (!condition) {
        PropertyFailure();
    }
}

template <std::size_t Size>
void FillNonzero(
    std::array<std::byte, Size>* output,
    std::uint8_t seed) noexcept {
    Require(output != nullptr);
    for (std::size_t index = 0U; index < Size; ++index) {
        (*output)[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                seed + static_cast<std::uint8_t>(index)));
    }
}

void StoreU16(
    std::span<std::byte> output,
    std::size_t offset,
    std::uint16_t value) noexcept {
    output[offset] = static_cast<std::byte>(value & 0xffU);
    output[offset + 1U] =
        static_cast<std::byte>((value >> 8U) & 0xffU);
}

void StoreU32(
    std::span<std::byte> output,
    std::size_t offset,
    std::uint32_t value) noexcept {
    for (std::size_t index = 0U; index < 4U; ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        output[offset + index] = static_cast<std::byte>(
            (value >> shift) & 0xffU);
    }
}

void StoreU64(
    std::span<std::byte> output,
    std::size_t offset,
    std::uint64_t value) noexcept {
    for (std::size_t index = 0U; index < 8U; ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        output[offset + index] = static_cast<std::byte>(
            (value >> shift) & 0xffU);
    }
}

[[nodiscard]] std::uint64_t LoadSeedU64(
    std::span<const std::uint8_t> input) noexcept {
    std::uint64_t result = 0U;
    const std::size_t count =
        std::min(input.size(), sizeof(result));
    for (std::size_t index = 0U; index < count; ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        result |=
            static_cast<std::uint64_t>(input[index]) << shift;
    }
    return result;
}

[[nodiscard]] std::vector<std::byte> CopyBytes(
    std::span<const std::uint8_t> input) {
    std::vector<std::byte> result;
    result.reserve(input.size());
    for (const std::uint8_t value : input) {
        result.push_back(static_cast<std::byte>(value));
    }
    return result;
}

void Mutate(
    std::vector<std::byte>* wire,
    std::span<const std::uint8_t> seed) noexcept {
    Require(wire != nullptr);
    if (wire->empty()) {
        return;
    }
    const std::size_t mutation_bytes =
        std::min(seed.size(), std::size_t{192U});
    for (std::size_t index = 0U;
         index + 2U < mutation_bytes;
         index += 3U) {
        const std::size_t position =
            (static_cast<std::size_t>(seed[index]) * 257U +
             static_cast<std::size_t>(seed[index + 1U])) %
            wire->size();
        const std::uint8_t mask =
            seed[index + 2U] == 0U
                ? 1U
                : seed[index + 2U];
        (*wire)[position] ^= static_cast<std::byte>(mask);
    }
}

[[nodiscard]] ingress::RawV1VendorHead MakeVendorHead(
    std::uint32_t body_size) noexcept {
    ingress::RawV1VendorHead head{};
    std::span<std::byte> bytes(head);
    head[0] =
        static_cast<std::byte>(ingress::kVendorMessageHeadBytes);
    StoreU32(
        bytes,
        1U,
        static_cast<std::uint32_t>(
            ingress::kVendorMessageHeadBytes) +
            body_size);
    head[5] = std::byte{2U};
    head[6] = std::byte{3U};
    StoreU16(bytes, 7U, 101U);
    StoreU16(bytes, 9U, 3001U);
    StoreU32(bytes, 11U, 0x12345678U);
    StoreU64(bytes, 15U, 9001U);
    return head;
}

struct Fixture final {
    ingress::SegmentHeaderV1 segment{};
    ingress::RawV1SegmentHeaderWire segment_header{};
    ingress::RawV1JournalHeaderWire journal_header{};
    ingress::RawV1DurableMarkerWire anchor_marker{};
    ingress::RawV1DurableMarkerWire record_marker{};
    std::vector<std::byte> record;
    std::vector<std::byte> segment_bytes;
    std::vector<std::byte> journal_bytes;
};

[[nodiscard]] Fixture MakeFixture(
    std::span<const std::uint8_t> body_seed) {
    Fixture fixture;
    fixture.segment.source_stream_id = 1001U;
    fixture.segment.capture_date = 20260719U;
    FillNonzero(&fixture.segment.stream_day_id, 1U);
    fixture.segment.segment_sequence = 1U;
    fixture.segment.segment_base_wal_pos = 0U;
    fixture.segment.first_ingress_sequence = 1U;
    fixture.segment.created_realtime_ns = 1000U;
    fixture.segment.created_monotonic_ns = 500U;
    FillNonzero(&fixture.segment.host_uuid, 21U);
    FillNonzero(&fixture.segment.linux_boot_id, 41U);
    fixture.segment.clock_epoch_algorithm = 1U;
    FillNonzero(&fixture.segment.clock_epoch_digest, 61U);
    fixture.segment.clock_epoch_label = 7U;
    FillNonzero(&fixture.segment.sdk_archive_sha256, 81U);
    FillNonzero(&fixture.segment.libmdl_api_sha256, 101U);
    FillNonzero(
        &fixture.segment.endpoint_contract_sha256, 121U);
    FillNonzero(&fixture.segment.config_sha256, 141U);
    FillNonzero(&fixture.segment.raw_schema_sha256, 161U);
    FillNonzero(&fixture.segment.build_manifest_sha256, 181U);
    Require(
        ingress::EncodeSegmentHeaderV1(
            fixture.segment, &fixture.segment_header) ==
        ingress::RawV1Error::kNone);

    const std::size_t body_size =
        std::min(body_seed.size(), std::size_t{4096U});
    std::vector<std::byte> body;
    body.reserve(body_size);
    for (std::size_t index = 0U;
         index < body_size;
         ++index) {
        body.push_back(
            static_cast<std::byte>(body_seed[index]));
    }
    ingress::RawRecordInputV1 record_input;
    record_input.meta.source_stream_id =
        fixture.segment.source_stream_id;
    record_input.meta.connection_epoch_hint = 9U;
    record_input.meta.ingress_sequence = 1U;
    record_input.meta.recv_realtime_ns = 2000U;
    record_input.meta.recv_monotonic_ns = 1500U;
    record_input.meta.capture_date =
        fixture.segment.capture_date;
    record_input.vendor_head =
        MakeVendorHead(static_cast<std::uint32_t>(body.size()));
    record_input.vendor_body = body;
    Require(
        ingress::EncodeRawRecordV1(
            record_input, &fixture.record) ==
        ingress::RawV1Error::kNone);

    fixture.segment_bytes.assign(
        fixture.segment_header.begin(),
        fixture.segment_header.end());
    fixture.segment_bytes.insert(
        fixture.segment_bytes.end(),
        fixture.record.begin(),
        fixture.record.end());

    ingress::DurableJournalHeaderV1 journal;
    journal.capture_date = fixture.segment.capture_date;
    journal.source_stream_id = fixture.segment.source_stream_id;
    journal.stream_day_id = fixture.segment.stream_day_id;
    journal.raw_schema_sha256 =
        fixture.segment.raw_schema_sha256;
    FillNonzero(&journal.created_host_uuid, 31U);
    FillNonzero(&journal.created_linux_boot_id, 51U);
    journal.created_clock_epoch_algorithm = 1U;
    FillNonzero(&journal.created_clock_epoch_digest, 71U);
    journal.created_clock_epoch_label = 8U;
    Require(
        ingress::EncodeDurableJournalHeaderV1(
            journal, &fixture.journal_header) ==
        ingress::RawV1Error::kNone);

    ingress::DurableMarkerV1 marker;
    marker.source_stream_id = fixture.segment.source_stream_id;
    marker.segment_sequence = 1U;
    marker.durable_global_wal_pos =
        ingress::kRawV1SegmentHeaderBytes;
    marker.durable_ingress_sequence = 0U;
    marker.durable_segment_offset =
        ingress::kRawV1SegmentHeaderBytes;
    Require(
        ingress::EncodeDurableMarkerV1(
            marker, &fixture.anchor_marker) ==
        ingress::RawV1Error::kNone);
    marker.durable_global_wal_pos =
        static_cast<std::uint64_t>(
            fixture.segment_bytes.size());
    marker.durable_ingress_sequence = 1U;
    marker.durable_segment_offset =
        static_cast<std::uint64_t>(
            fixture.segment_bytes.size());
    Require(
        ingress::EncodeDurableMarkerV1(
            marker, &fixture.record_marker) ==
        ingress::RawV1Error::kNone);

    fixture.journal_bytes.assign(
        fixture.journal_header.begin(),
        fixture.journal_header.end());
    fixture.journal_bytes.insert(
        fixture.journal_bytes.end(),
        fixture.anchor_marker.begin(),
        fixture.anchor_marker.end());
    fixture.journal_bytes.insert(
        fixture.journal_bytes.end(),
        fixture.record_marker.begin(),
        fixture.record_marker.end());
    return fixture;
}

template <typename Wire>
void RequireWireEqual(
    const Wire& encoded,
    std::span<const std::byte> input) noexcept {
    Require(
        encoded.size() == input.size() &&
        std::equal(
            encoded.begin(), encoded.end(), input.begin()));
}

void CheckSegmentCodec(
    std::span<const std::byte> wire) noexcept {
    ingress::SegmentHeaderV1 value;
    const ingress::RawV1Error decoded =
        ingress::DecodeSegmentHeaderV1(wire, &value);
    Require(
        decoded == ingress::ValidateSegmentHeaderV1(wire));
    if (decoded == ingress::RawV1Error::kNone) {
        ingress::RawV1SegmentHeaderWire encoded{};
        Require(
            ingress::EncodeSegmentHeaderV1(value, &encoded) ==
            ingress::RawV1Error::kNone);
        RequireWireEqual(encoded, wire);
    }
}

void CheckRecordHeaderCodec(
    std::span<const std::byte> wire) noexcept {
    ingress::RawRecordHeaderV1 value;
    const ingress::RawV1Error decoded =
        ingress::DecodeRawRecordHeaderV1(wire, &value);
    Require(
        decoded == ingress::ValidateRawRecordHeaderV1(wire));
    if (decoded == ingress::RawV1Error::kNone) {
        ingress::RawV1RecordHeaderWire encoded{};
        Require(
            ingress::EncodeRawRecordHeaderV1(value, &encoded) ==
            ingress::RawV1Error::kNone);
        RequireWireEqual(encoded, wire);
    }
}

void CheckTrailerCodec(
    std::span<const std::byte> wire) noexcept {
    ingress::RawRecordTrailerV1 value;
    const ingress::RawV1Error decoded =
        ingress::DecodeRawRecordTrailerV1(wire, &value);
    Require(
        decoded == ingress::ValidateRawRecordTrailerV1(wire));
    if (decoded == ingress::RawV1Error::kNone) {
        ingress::RawV1RecordTrailerWire encoded{};
        Require(
            ingress::EncodeRawRecordTrailerV1(value, &encoded) ==
            ingress::RawV1Error::kNone);
        RequireWireEqual(encoded, wire);
    }
}

void CheckJournalCodec(
    std::span<const std::byte> wire) noexcept {
    ingress::DurableJournalHeaderV1 value;
    const ingress::RawV1Error decoded =
        ingress::DecodeDurableJournalHeaderV1(wire, &value);
    Require(
        decoded ==
        ingress::ValidateDurableJournalHeaderV1(wire));
    if (decoded == ingress::RawV1Error::kNone) {
        ingress::RawV1JournalHeaderWire encoded{};
        Require(
            ingress::EncodeDurableJournalHeaderV1(
                value, &encoded) ==
            ingress::RawV1Error::kNone);
        RequireWireEqual(encoded, wire);
    }
}

void CheckMarkerCodec(
    std::span<const std::byte> wire) noexcept {
    ingress::DurableMarkerV1 value;
    const ingress::RawV1Error decoded =
        ingress::DecodeDurableMarkerV1(wire, &value);
    Require(
        decoded == ingress::ValidateDurableMarkerV1(wire));
    if (decoded == ingress::RawV1Error::kNone) {
        ingress::RawV1DurableMarkerWire encoded{};
        Require(
            ingress::EncodeDurableMarkerV1(value, &encoded) ==
            ingress::RawV1Error::kNone);
        RequireWireEqual(encoded, wire);
    }
}

void CheckRecordCodec(
    std::span<const std::byte> wire) noexcept {
    ingress::RawRecordViewV1 value;
    const ingress::RawV1Error decoded =
        ingress::DecodeRawRecordV1(wire, &value);
    Require(decoded == ingress::ValidateRawRecordV1(wire));
    if (decoded != ingress::RawV1Error::kNone) {
        return;
    }
    ingress::RawRecordInputV1 input;
    input.meta.source_stream_id = value.header.source_stream_id;
    input.meta.connection_epoch_hint =
        value.header.connection_epoch_hint;
    input.meta.ingress_sequence =
        value.header.ingress_sequence;
    input.meta.recv_realtime_ns =
        value.header.recv_realtime_ns;
    input.meta.recv_monotonic_ns =
        value.header.recv_monotonic_ns;
    input.meta.capture_date = value.header.capture_date;
    input.meta.flags = value.header.flags;
    std::copy(
        value.vendor_head.begin(),
        value.vendor_head.end(),
        input.vendor_head.begin());
    input.vendor_body = value.vendor_body;
    std::vector<std::byte> encoded;
    Require(
        ingress::EncodeRawRecordV1(input, &encoded) ==
        ingress::RawV1Error::kNone);
    Require(
        encoded.size() == wire.size() &&
        std::equal(
            encoded.begin(), encoded.end(), wire.begin()));
}

void DispatchCodec(
    std::uint8_t selector,
    std::span<const std::byte> wire) noexcept {
    switch (selector % 6U) {
        case 0U:
            CheckSegmentCodec(wire);
            break;
        case 1U:
            CheckRecordHeaderCodec(wire);
            break;
        case 2U:
            CheckTrailerCodec(wire);
            break;
        case 3U:
            CheckJournalCodec(wire);
            break;
        case 4U:
            CheckMarkerCodec(wire);
            break;
        case 5U:
            CheckRecordCodec(wire);
            break;
    }
}

void CheckScan(
    const std::shared_ptr<const std::vector<std::byte>>& bytes,
    std::uint64_t durable_limit) noexcept {
    const ingress::RawSegmentScanResult result =
        ingress::ScanRawSegmentV1(bytes, durable_limit);
    if (bytes == nullptr) {
        Require(
            result.error ==
            ingress::RawReaderError::kNullBuffer);
        return;
    }
    Require(
        result.validated_end_offset <=
        static_cast<std::uint64_t>(bytes->size()));
    if (result.ok()) {
        Require(result.validated_end_offset == durable_limit);
    }
    std::uint64_t previous_end =
        ingress::kRawV1SegmentHeaderBytes;
    std::uint64_t previous_sequence = 0U;
    for (const ingress::RawRecordView& record :
         result.records) {
        Require(
            record.record_start_offset() == previous_end &&
            record.record_start_offset() <
                record.record_end_offset() &&
            record.record_end_offset() <=
                result.validated_end_offset &&
            record.vendor_head().size() ==
                ingress::kVendorMessageHeadBytes &&
            record.vendor_body().size() ==
                record.metadata().vendor_body_size);
        if (previous_sequence != 0U) {
            Require(
                previous_sequence !=
                    std::numeric_limits<std::uint64_t>::max() &&
                record.metadata().ingress_sequence ==
                    previous_sequence + 1U);
        }
        const std::size_t start =
            static_cast<std::size_t>(
                record.record_start_offset());
        const std::size_t length =
            static_cast<std::size_t>(
                record.record_end_offset() -
                record.record_start_offset());
        Require(
            ingress::ValidateRawRecordV1(
                std::span<const std::byte>(
                    bytes->data() + start, length)) ==
            ingress::RawV1Error::kNone);
        previous_end = record.record_end_offset();
        previous_sequence =
            record.metadata().ingress_sequence;
    }
}

[[nodiscard]] bool SameRecoveryPlan(
    const ingress::RawRecoveryPlanV1& left,
    const ingress::RawRecoveryPlanV1& right) noexcept {
    if (left.fatal != right.fatal ||
        left.codec_error != right.codec_error ||
        left.reader_error != right.reader_error ||
        left.evidence_offset != right.evidence_offset ||
        left.accepted_journal_size !=
            right.accepted_journal_size ||
        left.has_accepted_cursor !=
            right.has_accepted_cursor ||
        left.accepted_cursor.segment_sequence !=
            right.accepted_cursor.segment_sequence ||
        left.accepted_cursor.global_wal_pos !=
            right.accepted_cursor.global_wal_pos ||
        left.accepted_cursor.ingress_sequence !=
            right.accepted_cursor.ingress_sequence ||
        left.accepted_cursor.segment_offset !=
            right.accepted_cursor.segment_offset ||
        left.accepted_cursor.marker_flags !=
            right.accepted_cursor.marker_flags ||
        left.journal_tail != right.journal_tail ||
        left.initial_anchor != right.initial_anchor ||
        left.r11_orphan != right.r11_orphan ||
        left.segments.size() != right.segments.size()) {
        return false;
    }
    for (std::size_t index = 0U;
         index < left.segments.size();
         ++index) {
        const auto& a = left.segments[index];
        const auto& b = right.segments[index];
        if (a.segment_sequence != b.segment_sequence ||
            a.segment_base_wal_pos != b.segment_base_wal_pos ||
            a.has_accepted_marker != b.has_accepted_marker ||
            a.accepted_marker_wire != b.accepted_marker_wire ||
            a.durable_end_offset != b.durable_end_offset ||
            a.validated_logical_end_offset !=
                b.validated_logical_end_offset ||
            a.validated_last_ingress_sequence !=
                b.validated_last_ingress_sequence ||
            a.append_only_begin_offset !=
                b.append_only_begin_offset ||
            a.append_only_end_offset !=
                b.append_only_end_offset ||
            a.tail_begin_offset != b.tail_begin_offset ||
            a.tail_end_offset != b.tail_end_offset ||
            a.tail != b.tail ||
            a.sealed != b.sealed) {
            return false;
        }
    }
    return true;
}

void CheckRecovery(
    const ingress::RawRecoveryInputV1& input) noexcept {
    std::vector<std::byte> journal_before;
    if (input.journal != nullptr) {
        journal_before = *input.journal;
    }
    std::vector<std::vector<std::byte>> segments_before;
    segments_before.reserve(input.segments.size());
    for (const auto& segment : input.segments) {
        segments_before.push_back(
            segment.bytes == nullptr
                ? std::vector<std::byte>{}
                : *segment.bytes);
    }

    const ingress::RawRecoveryPlanV1 first =
        ingress::AnalyzeRawRecoveryV1(input);
    const ingress::RawRecoveryPlanV1 second =
        ingress::AnalyzeRawRecoveryV1(input);
    Require(SameRecoveryPlan(first, second));
    if (input.journal != nullptr) {
        Require(
            *input.journal == journal_before &&
            first.accepted_journal_size <=
                static_cast<std::uint64_t>(
                    input.journal->size()));
    }
    for (std::size_t index = 0U;
         index < input.segments.size();
         ++index) {
        if (input.segments[index].bytes != nullptr) {
            Require(
                *input.segments[index].bytes ==
                segments_before[index]);
        }
    }
    if (!first.ok()) {
        return;
    }
    std::uint32_t previous_sequence = 0U;
    for (const auto& segment : first.segments) {
        Require(
            segment.segment_sequence > previous_sequence &&
            segment.durable_end_offset <=
                segment.validated_logical_end_offset &&
            segment.append_only_begin_offset <=
                segment.append_only_end_offset &&
            segment.append_only_end_offset <=
                segment.validated_logical_end_offset &&
            segment.tail_begin_offset <=
                segment.tail_end_offset);
        previous_sequence = segment.segment_sequence;
    }
}

void RunDirectCodec(
    std::span<const std::uint8_t> seed) {
    if (seed.empty()) {
        const std::span<const std::byte> empty;
        for (std::uint8_t selector = 0U;
             selector < 6U;
             ++selector) {
            DispatchCodec(selector, empty);
        }
        return;
    }
    const std::vector<std::byte> wire =
        CopyBytes(seed.subspan(1U));
    DispatchCodec(seed.front(), wire);
}

void RunCanonicalCodec(
    std::span<const std::uint8_t> seed) {
    const Fixture fixture = MakeFixture({});
    const std::uint8_t selector =
        seed.empty() ? 0U : seed.front();
    std::vector<std::byte> wire;
    switch (selector % 6U) {
        case 0U:
            wire.assign(
                fixture.segment_header.begin(),
                fixture.segment_header.end());
            break;
        case 1U:
            wire.assign(
                fixture.record.begin(),
                fixture.record.begin() +
                    static_cast<std::ptrdiff_t>(
                        ingress::kRawV1RecordHeaderBytes));
            break;
        case 2U:
            wire.assign(
                fixture.record.end() -
                    static_cast<std::ptrdiff_t>(
                        ingress::kRawV1RecordTrailerBytes),
                fixture.record.end());
            break;
        case 3U:
            wire.assign(
                fixture.journal_header.begin(),
                fixture.journal_header.end());
            break;
        case 4U:
            wire.assign(
                fixture.record_marker.begin(),
                fixture.record_marker.end());
            break;
        case 5U:
            wire = fixture.record;
            break;
    }
    const std::span<const std::uint8_t> mutation_seed =
        seed.empty() ? seed : seed.subspan(1U);
    Mutate(&wire, mutation_seed);
    DispatchCodec(selector, wire);
}

void RunRecordCodec(
    std::span<const std::uint8_t> seed) {
    const std::size_t body_size =
        std::min(seed.size(), std::size_t{512U});
    const Fixture fixture = MakeFixture(seed.first(body_size));
    std::vector<std::byte> wire = fixture.record;
    Mutate(&wire, seed.subspan(body_size));
    CheckRecordCodec(wire);
}

void RunDirectReader(
    std::span<const std::uint8_t> seed) {
    if (seed.empty()) {
        CheckScan(nullptr, 0U);
        return;
    }
    const std::uint64_t durable = LoadSeedU64(seed);
    auto mutable_bytes = std::make_shared<std::vector<std::byte>>(
        CopyBytes(seed.subspan(
            std::min(seed.size(), sizeof(durable)))));
    std::shared_ptr<const std::vector<std::byte>> bytes =
        mutable_bytes;
    CheckScan(bytes, durable);
}

void RunCanonicalReader(
    std::span<const std::uint8_t> seed) {
    Fixture fixture = MakeFixture(seed.first(
        std::min(seed.size(), std::size_t{128U})));
    const std::uint8_t selector =
        seed.empty() ? 0U : seed.front();
    const std::span<const std::uint8_t> mutation_seed =
        seed.empty() ? seed : seed.subspan(1U);
    Mutate(&fixture.segment_bytes, mutation_seed);
    std::uint64_t durable =
        static_cast<std::uint64_t>(
            fixture.segment_bytes.size());
    switch (selector % 6U) {
        case 0U:
            break;
        case 1U:
            durable = ingress::kRawV1SegmentHeaderBytes;
            break;
        case 2U:
            durable = ingress::kRawV1SegmentHeaderBytes - 1U;
            break;
        case 3U:
            ++durable;
            break;
        case 4U:
            if (durable != 0U) {
                --durable;
            }
            break;
        case 5U:
            durable = std::numeric_limits<std::uint64_t>::max();
            break;
    }
    auto mutable_bytes =
        std::make_shared<std::vector<std::byte>>(
            std::move(fixture.segment_bytes));
    std::shared_ptr<const std::vector<std::byte>> bytes =
        mutable_bytes;
    CheckScan(bytes, durable);
}

void RunDirectRecovery(
    std::span<const std::uint8_t> seed) {
    ingress::RawRecoveryInputV1 input;
    if (seed.empty()) {
        CheckRecovery(input);
        return;
    }
    const std::uint8_t flags = seed.front();
    seed = seed.subspan(1U);
    const std::size_t segment_count =
        static_cast<std::size_t>((flags >> 2U) & 0x03U);
    const std::size_t journal_size =
        seed.empty()
            ? 0U
            : static_cast<std::size_t>(seed.front()) %
                  (seed.size() + 1U);
    if ((flags & 0x01U) == 0U) {
        auto journal =
            std::make_shared<std::vector<std::byte>>(
                CopyBytes(seed.first(journal_size)));
        input.journal = std::move(journal);
    }
    seed = seed.subspan(journal_size);
    for (std::size_t index = 0U;
         index < segment_count;
         ++index) {
        if (((flags >> (index + 4U)) & 0x01U) != 0U) {
            input.segments.push_back({nullptr});
            continue;
        }
        const std::size_t remaining_segments =
            segment_count - index;
        const std::size_t size =
            remaining_segments == 0U
                ? 0U
                : seed.size() / remaining_segments;
        auto segment =
            std::make_shared<std::vector<std::byte>>(
                CopyBytes(seed.first(size)));
        input.segments.push_back({std::move(segment)});
        seed = seed.subspan(size);
    }
    CheckRecovery(input);
}

void RunCanonicalRecovery(
    std::span<const std::uint8_t> seed) {
    Fixture fixture = MakeFixture(seed.first(
        std::min(seed.size(), std::size_t{128U})));
    const std::uint8_t selector =
        seed.empty() ? 0U : seed.front();
    const std::span<const std::uint8_t> mutation_seed =
        seed.empty() ? seed : seed.subspan(1U);
    ingress::RawRecoveryInputV1 input;
    switch (selector % 8U) {
        case 0U:
            break;
        case 1U:
            fixture.journal_bytes.resize(
                ingress::kRawV1JournalHeaderBytes - 1U);
            break;
        case 2U:
            Mutate(&fixture.journal_bytes, mutation_seed);
            break;
        case 3U:
            fixture.segment_bytes.resize(
                ingress::kRawV1SegmentHeaderBytes - 1U);
            break;
        case 4U:
            Mutate(&fixture.segment_bytes, mutation_seed);
            break;
        case 5U:
            fixture.journal_bytes.push_back(std::byte{0x5aU});
            break;
        case 6U:
            fixture.segment_bytes.insert(
                fixture.segment_bytes.end(),
                64U,
                std::byte{0});
            break;
        case 7U:
            break;
    }
    input.journal =
        std::make_shared<const std::vector<std::byte>>(
            std::move(fixture.journal_bytes));
    input.segments.push_back(
        {std::make_shared<const std::vector<std::byte>>(
            std::move(fixture.segment_bytes))});
    if (selector % 8U == 7U) {
        input.segments.push_back({nullptr});
    }
    CheckRecovery(input);
}

void RunOwnedReaderAndLayout(
    std::span<const std::uint8_t> seed) {
    ingress::RawRecordLayoutV1 layout{};
    const std::uint64_t value = LoadSeedU64(seed);
    const std::size_t body_size =
        value >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max())
            ? std::numeric_limits<std::size_t>::max()
            : static_cast<std::size_t>(value);
    static_cast<void>(
        ingress::ComputeRawRecordLayoutV1(body_size, &layout));
    Require(
        ingress::ComputeRawRecordLayoutV1(
            body_size, nullptr) ==
        ingress::RawV1Error::kNullOutput);

    const Fixture fixture = MakeFixture(seed.first(
        std::min(seed.size(), std::size_t{128U})));
    auto mutable_record =
        std::make_shared<std::vector<std::byte>>(
            fixture.record);
    std::shared_ptr<const std::vector<std::byte>> record =
        mutable_record;
    const std::uint64_t offset =
        seed.empty()
            ? ingress::kRawV1SegmentHeaderBytes
            : value;
    const std::uint64_t sequence =
        seed.size() < 2U ? 1U : value;
    const ingress::RawOwnedRecordResult result =
        ingress::DecodeOwnedRawRecordV1(
            record,
            fixture.segment,
            offset,
            sequence);
    if (result.ok()) {
        Require(
            result.record->record_start_offset() == offset &&
            result.record->metadata().ingress_sequence ==
                sequence &&
            result.record->record_end_offset() >
                result.record->record_start_offset());
    }
}

void RunBounded(
    std::span<const std::uint8_t> input) {
    if (input.empty()) {
        RunDirectCodec({});
        RunDirectReader({});
        RunDirectRecovery({});
        return;
    }
    const std::uint8_t mode = input.front() & 0x07U;
    const std::span<const std::uint8_t> seed =
        input.subspan(1U);
    switch (mode) {
        case 0U:
            RunDirectCodec(seed);
            break;
        case 1U:
            RunCanonicalCodec(seed);
            break;
        case 2U:
            RunRecordCodec(seed);
            break;
        case 3U:
            RunDirectReader(seed);
            break;
        case 4U:
            RunCanonicalReader(seed);
            break;
        case 5U:
            RunDirectRecovery(seed);
            break;
        case 6U:
            RunCanonicalRecovery(seed);
            break;
        case 7U:
            RunOwnedReaderAndLayout(seed);
            break;
    }
}

}  // namespace

void RunRawV1FuzzInput(
    std::span<const std::uint8_t> input) noexcept {
    if (input.size() > kRawV1FuzzMaxInputBytes) {
        return;
    }
    try {
        RunBounded(input);
    } catch (const std::bad_alloc&) {
        // Harness fixture allocation is outside the production API under
        // test. Resource-exhaustion injection belongs in dedicated tests.
    } catch (const std::length_error&) {
        // The input is bounded, but standard-library implementation limits
        // may be smaller than the nominal bound.
    }
}

}  // namespace l2flow::test
