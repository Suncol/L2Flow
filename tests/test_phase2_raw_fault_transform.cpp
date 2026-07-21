#include "l2flow/ingress/raw_fault_transform.h"
#include "l2flow/ingress/raw_reader.h"
#include "l2flow/ingress/raw_v1.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace ingress = l2flow::ingress;

namespace {

struct TestContext final {
    void Expect(
        bool condition,
        const std::string& description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    int failures = 0;
};

void StoreU16(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint16_t value) {
    bytes[offset] =
        static_cast<std::byte>(value & 0xffU);
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

std::uint64_t LoadU64(
    std::span<const std::byte> bytes,
    std::size_t offset) {
    std::uint64_t result = 0U;
    for (std::size_t index = 0U; index < 8U; ++index) {
        result |=
            static_cast<std::uint64_t>(
                std::to_integer<std::uint8_t>(
                    bytes[offset + index]))
            << (index * 8U);
    }
    return result;
}

template <std::size_t Size>
void Fill(
    std::array<std::byte, Size>* bytes,
    std::uint8_t seed) {
    if (bytes == nullptr) {
        return;
    }
    for (std::size_t index = 0U;
         index < bytes->size();
         ++index) {
        (*bytes)[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                seed +
                static_cast<std::uint8_t>(index)));
    }
}

ingress::RawV1VendorHead MakeVendorHead(
    std::uint32_t body_size,
    std::uint64_t sequence) {
    ingress::RawV1VendorHead head{};
    std::span<std::byte> bytes(head);
    head[0] = static_cast<std::byte>(
        ingress::kVendorMessageHeadBytes);
    StoreU32(
        bytes,
        1U,
        static_cast<std::uint32_t>(
            ingress::kVendorMessageHeadBytes) +
            body_size);
    head[5] = std::byte{1U};
    head[6] = std::byte{2U};
    StoreU16(bytes, 7U, 7U);
    StoreU16(bytes, 9U, 41U);
    StoreU32(bytes, 11U, 1234U);
    StoreU64(bytes, 15U, sequence);
    return head;
}

struct BuiltSegment final {
    std::shared_ptr<std::vector<std::byte>> bytes;
    ingress::RawSegmentScanResult scan;
    std::vector<std::size_t> starts;
    std::vector<std::size_t> ends;
};

BuiltSegment BuildSegment(
    const std::vector<std::size_t>& body_sizes) {
    BuiltSegment built;
    ingress::SegmentHeaderV1 segment;
    segment.source_stream_id = 1001U;
    segment.capture_date = 20260718U;
    Fill(&segment.stream_day_id, 1U);
    segment.segment_sequence = 1U;
    segment.segment_base_wal_pos = 0U;
    segment.first_ingress_sequence = 1U;
    segment.created_realtime_ns = 100U;
    segment.created_monotonic_ns = 50U;
    Fill(&segment.host_uuid, 21U);
    Fill(&segment.linux_boot_id, 41U);
    segment.clock_epoch_algorithm = 1U;
    Fill(&segment.clock_epoch_digest, 61U);
    segment.clock_epoch_label = 1U;
    Fill(&segment.sdk_archive_sha256, 81U);
    Fill(&segment.libmdl_api_sha256, 101U);
    Fill(&segment.endpoint_contract_sha256, 121U);
    Fill(&segment.config_sha256, 141U);
    Fill(&segment.raw_schema_sha256, 161U);
    Fill(&segment.build_manifest_sha256, 181U);

    ingress::RawV1SegmentHeaderWire header{};
    if (ingress::EncodeSegmentHeaderV1(
            segment, &header) !=
        ingress::RawV1Error::kNone) {
        return built;
    }
    built.bytes =
        std::make_shared<std::vector<std::byte>>(
            header.begin(), header.end());

    for (std::size_t index = 0U;
         index < body_sizes.size();
         ++index) {
        std::vector<std::byte> body(body_sizes[index]);
        for (std::size_t body_index = 0U;
             body_index < body.size();
             ++body_index) {
            body[body_index] = static_cast<std::byte>(
                static_cast<std::uint8_t>(
                    10U + index + body_index));
        }
        ingress::RawRecordInputV1 input;
        input.meta.source_stream_id =
            segment.source_stream_id;
        input.meta.connection_epoch_hint = 9U;
        input.meta.ingress_sequence =
            static_cast<std::uint64_t>(index) + 1U;
        input.meta.recv_realtime_ns =
            1000U + static_cast<std::uint64_t>(index);
        input.meta.recv_monotonic_ns =
            900U + static_cast<std::uint64_t>(index);
        input.meta.capture_date = segment.capture_date;
        input.vendor_head = MakeVendorHead(
            static_cast<std::uint32_t>(body.size()),
            100U + static_cast<std::uint64_t>(index));
        input.vendor_body = body;

        std::vector<std::byte> record;
        if (ingress::EncodeRawRecordV1(
                input, &record) !=
            ingress::RawV1Error::kNone) {
            return built;
        }
        built.starts.push_back(built.bytes->size());
        built.bytes->insert(
            built.bytes->end(),
            record.begin(),
            record.end());
        built.ends.push_back(built.bytes->size());
    }

    std::shared_ptr<const std::vector<std::byte>> immutable =
        built.bytes;
    built.scan = ingress::ScanRawSegmentV1(
        std::move(immutable),
        static_cast<std::uint64_t>(
            built.bytes->size()));
    return built;
}

std::vector<ingress::RawReplayRecord> MakeReplayRecords(
    const ingress::RawSegmentScanResult& scan) {
    ingress::RawReplaySegmentContext context;
    context.source_stream_id =
        scan.segment.source_stream_id;
    context.capture_date = scan.segment.capture_date;
    context.stream_day_id = scan.segment.stream_day_id;
    context.segment_sequence =
        scan.segment.segment_sequence;
    context.clock_epoch.algorithm =
        scan.segment.clock_epoch_algorithm;
    context.clock_epoch.digest =
        scan.segment.clock_epoch_digest;
    context.clock_epoch.label =
        scan.segment.clock_epoch_label;

    std::vector<ingress::RawReplayRecord> result;
    result.reserve(scan.records.size());
    for (const ingress::RawRecordView& view :
         scan.records) {
        result.push_back(
            ingress::RawReplayRecord{
                view,
                context,
                ingress::RawReplayProvenance::kDurable});
    }
    return result;
}

ingress::InjectedRawPlanIdentity MakePlanIdentity() {
    ingress::InjectedRawPlanIdentity identity;
    Fill(&identity.run_id, 201U);
    Fill(&identity.synthetic_namespace_id, 221U);
    Fill(&identity.raw_schema_sha256, 11U);
    Fill(
        &identity.injected_schema_identity_sha256,
        51U);
    Fill(&identity.parent_raw_identity_sha256, 91U);
    Fill(&identity.fault_rule_sha256, 131U);
    return identity;
}

bool SameLocator(
    const ingress::InjectedRawParentLocator& left,
    const ingress::InjectedRawParentLocator& right) {
    return left.capture_date == right.capture_date &&
        left.source_stream_id == right.source_stream_id &&
        left.stream_day_id == right.stream_day_id &&
        left.ingress_sequence == right.ingress_sequence &&
        left.record_start_wal_pos ==
            right.record_start_wal_pos &&
        left.record_end_wal_pos ==
            right.record_end_wal_pos &&
        left.occurrence == right.occurrence;
}

bool SamePlanRecord(
    const ingress::InjectedRawPlanRecord& left,
    const ingress::InjectedRawPlanRecord& right) {
    const bool mutation_equal =
        (!left.mutation.has_value() &&
         !right.mutation.has_value()) ||
        (left.mutation.has_value() &&
         right.mutation.has_value() &&
         left.mutation->kind == right.mutation->kind &&
         left.mutation->vendor_body_offset ==
             right.mutation->vendor_body_offset &&
         left.mutation->before ==
             right.mutation->before &&
         left.mutation->after ==
             right.mutation->after);
    return left.synthetic_ingress_sequence ==
               right.synthetic_ingress_sequence &&
        left.capture_meta.source_stream_id ==
            right.capture_meta.source_stream_id &&
        left.capture_meta.connection_epoch_hint ==
            right.capture_meta.connection_epoch_hint &&
        left.capture_meta.ingress_sequence ==
            right.capture_meta.ingress_sequence &&
        left.capture_meta.recv_realtime_ns ==
            right.capture_meta.recv_realtime_ns &&
        left.capture_meta.recv_monotonic_ns ==
            right.capture_meta.recv_monotonic_ns &&
        left.capture_meta.capture_date ==
            right.capture_meta.capture_date &&
        left.capture_meta.flags ==
            right.capture_meta.flags &&
        left.vendor_head == right.vendor_head &&
        left.vendor_body == right.vendor_body &&
        SameLocator(left.parent, right.parent) &&
        left.parent_provenance ==
            right.parent_provenance &&
        mutation_equal;
}

void TestLogicalTransform(TestContext* test) {
    BuiltSegment built = BuildSegment({4U, 4U, 4U});
    test->Expect(
        built.scan.ok() &&
            built.scan.records.size() == 3U,
        "logical source is a validating-reader result");
    if (!built.scan.ok() ||
        built.scan.records.size() != 3U) {
        return;
    }
    const std::vector<ingress::RawReplayRecord> input =
        MakeReplayRecords(built.scan);

    ingress::RawLogicalFaultRule rule;
    rule.seed = 0x123456789abcdef0ULL;
    rule.duplicate_one_in = 1U;
    rule.duplicate_additional_copies = 1U;
    rule.mutate_one_in = 1U;
    rule.reorder_window = 3U;
    rule.mutation =
        ingress::RawLogicalMutationKind::
            kVendorBodyByteXor;
    rule.mutation_xor_mask = std::byte{0x5aU};

    const ingress::InjectedRawPlanIdentity identity =
        MakePlanIdentity();
    const ingress::RawLogicalFaultLimits limits;
    ingress::InjectedRawTransformPlan first;
    ingress::InjectedRawTransformPlan second;
    const ingress::RawLogicalFaultError first_error =
        ingress::BuildRawLogicalFaultPlan(
            input, identity, rule, limits, &first);
    const ingress::RawLogicalFaultError second_error =
        ingress::BuildRawLogicalFaultPlan(
            input, identity, rule, limits, &second);
    test->Expect(
        first_error == ingress::RawLogicalFaultError::kNone &&
            second_error ==
                ingress::RawLogicalFaultError::kNone,
        "logical duplicate/mutation/reorder plan builds");
    test->Expect(
        first.identity.plan_magic ==
                ingress::kInjectedRawUnframedPlanMagic &&
            first.identity.synthetic &&
            first.identity.raw_schema_sha256 !=
                first.identity.
                    injected_schema_identity_sha256,
        "logical output has a distinct synthetic plan/schema identity");
    test->Expect(
        first.records.size() == 6U &&
            first.dropped_locators.empty(),
        "selected duplicates produce bounded output");
    const std::array<
        std::pair<std::uint64_t, std::uint32_t>,
        6U>
        stable_order{{
            {2U, 1U},
            {1U, 1U},
            {1U, 2U},
            {3U, 1U},
            {2U, 2U},
            {3U, 2U},
        }};
    const std::array<std::uint64_t, 3U>
        stable_mutation_offsets{0U, 3U, 0U};
    if (first.records.size() == stable_order.size()) {
        for (std::size_t index = 0U;
             index < stable_order.size();
             ++index) {
            const ingress::InjectedRawPlanRecord& record =
                first.records[index];
            const std::uint64_t parent_index =
                record.parent.ingress_sequence - 1U;
            test->Expect(
                record.parent.ingress_sequence ==
                        stable_order[index].first &&
                    record.parent.occurrence ==
                        stable_order[index].second &&
                    record.mutation.has_value() &&
                    parent_index <
                        stable_mutation_offsets.size() &&
                    record.mutation->
                            vendor_body_offset ==
                        stable_mutation_offsets[
                            static_cast<std::size_t>(
                                parent_index)],
                "stable SplitMix64 selection/order golden at output " +
                    std::to_string(index));
        }
    }
    test->Expect(
        first.records.size() == second.records.size(),
        "same explicit seed produces the same record count");
    if (first.records.size() == second.records.size()) {
        for (std::size_t index = 0U;
             index < first.records.size();
             ++index) {
            test->Expect(
                SamePlanRecord(
                    first.records[index],
                    second.records[index]),
                "same seed is byte-for-byte deterministic at output " +
                    std::to_string(index));
        }
    }

    std::map<std::uint64_t, std::set<std::uint32_t>>
        occurrences;
    bool sequence_ids_preserved = true;
    bool every_record_mutated_once = true;
    for (std::size_t index = 0U;
         index < first.records.size();
         ++index) {
        const ingress::InjectedRawPlanRecord& record =
            first.records[index];
        test->Expect(
            record.synthetic_ingress_sequence ==
                    static_cast<std::uint64_t>(index) + 1U &&
                record.capture_meta.ingress_sequence ==
                    record.synthetic_ingress_sequence,
            "synthetic sequence is contiguous after reorder");
        occurrences[record.parent.ingress_sequence].insert(
            record.parent.occurrence);
        const std::uint64_t expected_vendor_sequence =
            99U + record.parent.ingress_sequence;
        sequence_ids_preserved =
            sequence_ids_preserved &&
            LoadU64(record.vendor_head, 15U) ==
                expected_vendor_sequence;
        every_record_mutated_once =
            every_record_mutated_once &&
            record.mutation.has_value() &&
            record.mutation->kind ==
                ingress::RawLogicalMutationKind::
                    kVendorBodyByteXor &&
            record.mutation->vendor_body_offset <
                record.vendor_body.size() &&
            (record.mutation->before ^
                 rule.mutation_xor_mask) ==
                record.mutation->after &&
            record.vendor_body[
                static_cast<std::size_t>(
                    record.mutation->
                        vendor_body_offset)] ==
                record.mutation->after;
    }
    test->Expect(
        occurrences.size() == 3U &&
            std::all_of(
                occurrences.begin(),
                occurrences.end(),
                [](const auto& item) {
                    return item.second ==
                        std::set<std::uint32_t>{1U, 2U};
                }),
        "each duplicate carries an unambiguous parent occurrence");
    test->Expect(
        sequence_ids_preserved,
        "logical body mutation preserves vendor SequenceID bytes");
    test->Expect(
        every_record_mutated_once,
        "logical mutation records exact before/after evidence");

    // Each independently shuffled chunk contains exactly the same parents as
    // its pre-shuffle chunk, proving the configured displacement bound.
    const std::array<std::multiset<std::uint64_t>, 2U>
        expected_chunks{
            std::multiset<std::uint64_t>{1U, 1U, 2U},
            std::multiset<std::uint64_t>{2U, 3U, 3U}};
    for (std::size_t chunk = 0U; chunk < 2U; ++chunk) {
        std::multiset<std::uint64_t> actual;
        for (std::size_t offset = 0U; offset < 3U;
             ++offset) {
            actual.insert(
                first.records[chunk * 3U + offset].
                    parent.ingress_sequence);
        }
        test->Expect(
            actual == expected_chunks[chunk],
            "reorder never crosses its configured window");
    }
}

void TestLogicalDropAndFailures(TestContext* test) {
    BuiltSegment built = BuildSegment({2U, 2U, 2U});
    if (!built.scan.ok()) {
        test->Expect(false, "logical failure fixture builds");
        return;
    }
    std::vector<ingress::RawReplayRecord> input =
        MakeReplayRecords(built.scan);
    const ingress::InjectedRawPlanIdentity identity =
        MakePlanIdentity();
    const ingress::RawLogicalFaultLimits limits;

    ingress::RawLogicalFaultRule drop;
    drop.seed = 7U;
    drop.drop_one_in = 1U;
    ingress::InjectedRawTransformPlan plan;
    test->Expect(
        ingress::BuildRawLogicalFaultPlan(
            input, identity, drop, limits, &plan) ==
                ingress::RawLogicalFaultError::kNone &&
            plan.records.empty() &&
            plan.dropped_locators.size() == input.size() &&
            std::all_of(
                plan.dropped_locators.begin(),
                plan.dropped_locators.end(),
                [](const auto& locator) {
                    return locator.occurrence == 1U;
                }),
        "drop writes an explicit run-level parent locator set");

    ingress::RawLogicalFaultLimits small = limits;
    small.max_output_records = 5U;
    ingress::RawLogicalFaultRule duplicate;
    duplicate.duplicate_one_in = 1U;
    duplicate.duplicate_additional_copies = 1U;
    test->Expect(
        ingress::BuildRawLogicalFaultPlan(
            input, identity, duplicate, small, &plan) ==
            ingress::RawLogicalFaultError::
                kResourceLimitExceeded,
        "logical output count is resource bounded");

    ingress::RawLogicalFaultRule unknown_algorithm;
    unknown_algorithm.algorithm =
        static_cast<
            ingress::RawLogicalSelectionAlgorithm>(255U);
    test->Expect(
        ingress::BuildRawLogicalFaultPlan(
            input,
            identity,
            unknown_algorithm,
            limits,
            &plan) ==
            ingress::RawLogicalFaultError::
                kUnknownAlgorithm,
        "unknown logical random algorithm fails closed");

    ingress::RawLogicalFaultRule unknown_mutation;
    unknown_mutation.mutate_one_in = 1U;
    unknown_mutation.mutation =
        static_cast<ingress::RawLogicalMutationKind>(255U);
    test->Expect(
        ingress::BuildRawLogicalFaultPlan(
            input,
            identity,
            unknown_mutation,
            limits,
            &plan) ==
            ingress::RawLogicalFaultError::
                kUnknownMutation,
        "unknown logical mutation fails closed");

    ingress::InjectedRawPlanIdentity alias = identity;
    alias.injected_schema_identity_sha256 =
        alias.raw_schema_sha256;
    test->Expect(
        ingress::BuildRawLogicalFaultPlan(
            input, alias, drop, limits, &plan) ==
            ingress::RawLogicalFaultError::
                kRawAndInjectedSchemaIdentityAlias,
        "Injected plan cannot reuse the RawV1 schema identity");

    std::vector<ingress::RawReplayRecord> duplicate_input{
        input.front(), input.front()};
    test->Expect(
        ingress::BuildRawLogicalFaultPlan(
            duplicate_input,
            identity,
            drop,
            limits,
            &plan) ==
            ingress::RawLogicalFaultError::
                kDuplicateParentLocator,
        "duplicate unchecked parent locators fail closed");

    input.front().segment.source_stream_id = 9999U;
    test->Expect(
        ingress::BuildRawLogicalFaultPlan(
            input, identity, drop, limits, &plan) ==
            ingress::RawLogicalFaultError::
                kInvalidValidatedInputContext,
        "replay context must match the validated RawRecordView");
    input = MakeReplayRecords(built.scan);
    input.front().provenance =
        static_cast<ingress::RawReplayProvenance>(255U);
    test->Expect(
        ingress::BuildRawLogicalFaultPlan(
            input, identity, drop, limits, &plan) ==
            ingress::RawLogicalFaultError::
                kInvalidValidatedInputContext,
        "unknown parent durability provenance fails closed");

    ingress::InjectedRawPlanIdentity wrong_magic = identity;
    wrong_magic.plan_magic[0U] = std::byte{'R'};
    input = MakeReplayRecords(built.scan);
    test->Expect(
        ingress::BuildRawLogicalFaultPlan(
            input, wrong_magic, drop, limits, &plan) ==
            ingress::RawLogicalFaultError::
                kInvalidPlanIdentity,
        "unknown synthetic plan identity fails closed");

    BuiltSegment empty = BuildSegment({0U});
    std::vector<ingress::RawReplayRecord> empty_input =
        MakeReplayRecords(empty.scan);
    ingress::RawLogicalFaultRule mutate;
    mutate.mutate_one_in = 1U;
    mutate.mutation =
        ingress::RawLogicalMutationKind::
            kVendorBodyByteXor;
    mutate.mutation_xor_mask = std::byte{1U};
    test->Expect(
        ingress::BuildRawLogicalFaultPlan(
            empty_input,
            identity,
            mutate,
            limits,
            &plan) ==
            ingress::RawLogicalFaultError::
                kEmptyMutationTarget,
        "selected body mutation rejects an empty body");
}

bool ExactRewriteOnly(
    const std::vector<std::byte>& source,
    const std::vector<std::byte>& derived,
    std::size_t begin,
    std::size_t end,
    std::byte mask) {
    if (source.size() != derived.size()) {
        return false;
    }
    for (std::size_t index = 0U;
         index < source.size();
         ++index) {
        const std::byte expected =
            index >= begin && index < end
                ? source[index] ^ mask
                : source[index];
        if (derived[index] != expected) {
            return false;
        }
    }
    return true;
}

void TestPhysicalFixtures(TestContext* test) {
    BuiltSegment built = BuildSegment({4U, 4U, 4U});
    test->Expect(
        built.scan.ok() &&
            built.scan.records.size() == 3U,
        "physical source is valid before deriving corruption");
    if (!built.scan.ok() ||
        built.scan.records.size() != 3U) {
        return;
    }

    const std::uint64_t durable_end =
        built.scan.records.front().record_end_offset();
    const std::uint64_t logical_end =
        static_cast<std::uint64_t>(built.bytes->size());
    const std::uint64_t second_body =
        built.scan.records[1].record_start_offset() +
        ingress::kRawV1RecordHeaderBytes +
        ingress::kVendorMessageHeadBytes;
    std::shared_ptr<const std::vector<std::byte>> source =
        built.bytes;
    const std::vector<std::byte> source_snapshot =
        *built.bytes;

    ingress::RawPhysicalCorruptionRequest append_request;
    append_request.source_bytes = source;
    append_request.segment_state =
        ingress::RawPhysicalSegmentState::kHighestOpen;
    append_request.durable_end_offset = durable_end;
    append_request.logical_end_offset = logical_end;
    append_request.rewrite_offset = second_body;
    append_request.rewrite_length = 1U;
    append_request.xor_mask = std::byte{0x80U};

    ingress::RawPhysicalCorruptionFixture append_fixture;
    const ingress::RawPhysicalFaultError append_error =
        ingress::CreateRawPhysicalCorruptionFixture(
            append_request,
            ingress::RawPhysicalFaultLimits{},
            &append_fixture);
    test->Expect(
        append_error ==
                ingress::RawPhysicalFaultError::kNone &&
            append_fixture.derived_bytes != nullptr,
        "append-only physical corruption fixture builds");
    if (append_fixture.derived_bytes != nullptr) {
        test->Expect(
            *built.bytes == source_snapshot &&
                ExactRewriteOnly(
                    source_snapshot,
                    *append_fixture.derived_bytes,
                    static_cast<std::size_t>(second_body),
                    static_cast<std::size_t>(
                        second_body + 1U),
                    append_request.xor_mask),
            "fixture preserves source and rewrites only the exact range");
        test->Expect(
            append_fixture.oracle.original_durability ==
                    ingress::
                        RawPhysicalOriginalDurability::
                            kHighestOpenAppendOnly &&
                append_fixture.oracle.
                    recovery_expectation ==
                    ingress::
                        RawPhysicalRecoveryExpectation::
                            kTruncatedInvalidTail &&
                append_fixture.oracle.
                    durable_reader_expectation ==
                    ingress::
                        RawPhysicalDurableReaderExpectation::
                            kHiddenByDurableBoundary &&
                append_fixture.oracle.
                    expected_invalid_tail_begin_offset ==
                    built.scan.records[1].
                        record_start_offset() &&
                append_fixture.oracle.
                    expected_invalid_tail_end_offset ==
                    logical_end &&
                append_fixture.oracle.
                    validating_reader_error ==
                    ingress::RawReaderError::
                        kPayloadCrcMismatch,
            "append-only oracle gives exact invalid-tail and hidden boundary");
        const ingress::RawSegmentScanResult durable_scan =
            ingress::ScanRawSegmentV1(
                append_fixture.derived_bytes,
                durable_end);
        test->Expect(
            durable_scan.ok(),
            "durable-only reader cannot expose marker-after corruption");
    }

    const std::uint64_t first_body =
        built.scan.records[0].record_start_offset() +
        ingress::kRawV1RecordHeaderBytes +
        ingress::kVendorMessageHeadBytes;
    ingress::RawPhysicalCorruptionRequest durable_request =
        append_request;
    durable_request.rewrite_offset = first_body;
    ingress::RawPhysicalCorruptionFixture durable_fixture;
    test->Expect(
        ingress::CreateRawPhysicalCorruptionFixture(
            durable_request,
            ingress::RawPhysicalFaultLimits{},
            &durable_fixture) ==
                ingress::RawPhysicalFaultError::kNone &&
            durable_fixture.oracle.original_durability ==
                ingress::
                    RawPhysicalOriginalDurability::
                        kHighestOpenDurable &&
            durable_fixture.oracle.recovery_expectation ==
                ingress::
                    RawPhysicalRecoveryExpectation::
                        kFatalRawCorruption &&
            durable_fixture.oracle.
                    durable_reader_expectation ==
                ingress::
                    RawPhysicalDurableReaderExpectation::
                        kRejectCorruption,
        "corruption inside open durable prefix is fatal");

    ingress::RawPhysicalCorruptionRequest sealed_request =
        append_request;
    sealed_request.segment_state =
        ingress::RawPhysicalSegmentState::kSealed;
    sealed_request.durable_end_offset = logical_end;
    sealed_request.rewrite_offset = second_body;
    ingress::RawPhysicalCorruptionFixture sealed_fixture;
    test->Expect(
        ingress::CreateRawPhysicalCorruptionFixture(
            sealed_request,
            ingress::RawPhysicalFaultLimits{},
            &sealed_fixture) ==
                ingress::RawPhysicalFaultError::kNone &&
            sealed_fixture.oracle.original_durability ==
                ingress::
                    RawPhysicalOriginalDurability::
                        kSealedDurable &&
            sealed_fixture.oracle.recovery_expectation ==
                ingress::
                    RawPhysicalRecoveryExpectation::
                        kFatalRawCorruption,
        "corruption anywhere in a sealed logical segment is fatal");

    ingress::RawPhysicalCorruptionRequest overlap_request =
        append_request;
    overlap_request.rewrite_offset = durable_end - 1U;
    overlap_request.rewrite_length = 2U;
    ingress::RawPhysicalCorruptionFixture overlap_fixture;
    test->Expect(
        ingress::CreateRawPhysicalCorruptionFixture(
            overlap_request,
            ingress::RawPhysicalFaultLimits{},
            &overlap_fixture) ==
                ingress::RawPhysicalFaultError::kNone &&
            overlap_fixture.oracle.original_durability ==
                ingress::
                    RawPhysicalOriginalDurability::
                        kHighestOpenDurableBoundaryOverlap &&
            overlap_fixture.oracle.recovery_expectation ==
                ingress::
                    RawPhysicalRecoveryExpectation::
                        kFatalRawCorruption,
        "range crossing durable boundary remains fatal");
}

void TestPhysicalFailures(TestContext* test) {
    BuiltSegment built = BuildSegment({2U, 2U});
    if (!built.scan.ok()) {
        test->Expect(false, "physical failure fixture builds");
        return;
    }
    ingress::RawPhysicalCorruptionRequest request;
    request.source_bytes = built.bytes;
    request.segment_state =
        ingress::RawPhysicalSegmentState::kHighestOpen;
    request.durable_end_offset =
        built.scan.records.front().record_end_offset();
    request.logical_end_offset =
        static_cast<std::uint64_t>(built.bytes->size());
    request.rewrite_offset =
        built.scan.records.back().record_start_offset() +
        ingress::kRawV1RecordHeaderBytes +
        ingress::kVendorMessageHeadBytes;
    request.rewrite_length = 1U;
    request.xor_mask = std::byte{1U};

    ingress::RawPhysicalCorruptionFixture fixture;
    ingress::RawPhysicalCorruptionRequest unknown_state =
        request;
    unknown_state.segment_state =
        static_cast<ingress::RawPhysicalSegmentState>(255U);
    test->Expect(
        ingress::CreateRawPhysicalCorruptionFixture(
            unknown_state,
            ingress::RawPhysicalFaultLimits{},
            &fixture) ==
            ingress::RawPhysicalFaultError::
                kUnknownSegmentState,
        "unknown physical segment state fails closed");

    ingress::RawPhysicalCorruptionRequest unknown_rewrite =
        request;
    unknown_rewrite.rewrite_kind =
        static_cast<ingress::RawPhysicalRewriteKind>(255U);
    test->Expect(
        ingress::CreateRawPhysicalCorruptionFixture(
            unknown_rewrite,
            ingress::RawPhysicalFaultLimits{},
            &fixture) ==
            ingress::RawPhysicalFaultError::
                kUnknownRewriteKind,
        "unknown physical rewrite operation fails closed");

    ingress::RawPhysicalCorruptionRequest zero = request;
    zero.xor_mask = std::byte{0U};
    test->Expect(
        ingress::CreateRawPhysicalCorruptionFixture(
            zero,
            ingress::RawPhysicalFaultLimits{},
            &fixture) ==
            ingress::RawPhysicalFaultError::kZeroRewriteMask,
        "zero physical rewrite fails closed");

    ingress::RawPhysicalFaultLimits small;
    small.max_source_bytes =
        static_cast<std::uint64_t>(built.bytes->size()) - 1U;
    test->Expect(
        ingress::CreateRawPhysicalCorruptionFixture(
            request, small, &fixture) ==
            ingress::RawPhysicalFaultError::
                kResourceLimitExceeded,
        "physical source copy is resource bounded");

    ingress::RawPhysicalCorruptionRequest past = request;
    past.rewrite_offset = past.logical_end_offset;
    test->Expect(
        ingress::CreateRawPhysicalCorruptionFixture(
            past,
            ingress::RawPhysicalFaultLimits{},
            &fixture) ==
            ingress::RawPhysicalFaultError::
                kInvalidRewriteRange,
        "physical rewrite cannot exceed validated logical bytes");

    ingress::RawPhysicalCorruptionRequest overflow = request;
    overflow.rewrite_offset =
        std::numeric_limits<std::uint64_t>::max();
    overflow.rewrite_length = 2U;
    test->Expect(
        ingress::CreateRawPhysicalCorruptionFixture(
            overflow,
            ingress::RawPhysicalFaultLimits{},
            &fixture) ==
            ingress::RawPhysicalFaultError::
                kArithmeticOverflow,
        "physical rewrite range uses checked arithmetic");

    auto corrupt_source =
        std::make_shared<std::vector<std::byte>>(
            *built.bytes);
    (*corrupt_source)[0U] ^= std::byte{1U};
    ingress::RawPhysicalCorruptionRequest invalid = request;
    invalid.source_bytes = corrupt_source;
    test->Expect(
        ingress::CreateRawPhysicalCorruptionFixture(
            invalid,
            ingress::RawPhysicalFaultLimits{},
            &fixture) ==
            ingress::RawPhysicalFaultError::
                kOriginalRawInvalid,
        "physical fixture rejects an already-invalid source");
}

}  // namespace

int main() {
    TestContext test;
    TestLogicalTransform(&test);
    TestLogicalDropAndFailures(&test);
    TestPhysicalFixtures(&test);
    TestPhysicalFailures(&test);

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Phase 2 Raw fault-transform tests failed\n";
        return 1;
    }
    std::cout
        << "Phase 2 Raw fault-transform tests passed\n";
    return 0;
}
