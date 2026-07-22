#include "l2flow/canonical/canonical_control_adapter_v1.h"
#include "l2flow/control/quality_flags_v1.h"

#include <cstddef>
#include <cstdint>
#include <iostream>

namespace canonical = l2flow::canonical;
namespace control = l2flow::control;

namespace {

void Fill(l2flow::common::Sha256Digest* digest, std::uint8_t seed) {
    for (std::size_t index = 0U; index < digest->size(); ++index) {
        (*digest)[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(seed + index));
    }
}

canonical::CanonicalRawContextV1 Raw() {
    canonical::CanonicalRawContextV1 raw{};
    raw.capture_date = 20260723U;
    raw.trade_date = 20260722U;
    raw.source_stream_id = 9U;
    raw.stream_day_id[0] = std::byte{1U};
    raw.source_writer_instance[0] = std::byte{3U};
    raw.source_generation = 4U;
    raw.origin_ingress_sequence = 7U;
    raw.origin_wal_end_pos = 8192U;
    raw.authoritative_connection_epoch = 0U;
    raw.clock_epoch.algorithm = 1U;
    raw.clock_epoch.digest[0] = std::byte{2U};
    return raw;
}

control::ControlRecordV1 Connecting() {
    control::ControlRecordV1 record{};
    record.flags = control::kControlRecordAddressHashPresent;
    record.control_type = control::ControlTypeV1::kConnecting;
    record.source_stream_id = 9U;
    record.capture_date = 20260723U;
    record.stream_day_id[0] = std::byte{1U};
    record.vendor_service_id = 1U;
    record.vendor_service_version = 101U;
    record.vendor_message_id = 1U;
    record.origin_ingress_sequence = 7U;
    record.origin_record_end_wal_pos = 8192U;
    record.quality_flags = control::QualityBit(
        control::QualityFlagV1::kSessionUnknown);
    record.required_count = 1U;
    Fill(&record.address_sha256, 3U);
    Fill(&record.control_state_sha256, 9U);
    return record;
}

control::ControlRecordV1 UnsupportedVersionDecodeError() {
    control::ControlRecordV1 record = Connecting();
    record.flags = 0U;
    record.control_type = control::ControlTypeV1::kDecodeError;
    record.decode_error = 0x0103U;
    record.vendor_service_version = 0U;
    record.quality_flags =
        control::QualityBit(control::QualityFlagV1::kSessionUnknown) |
        control::QualityBit(control::QualityFlagV1::kSchemaUnknown);
    record.address_sha256 = {};
    return record;
}

}  // namespace

int main() {
    int failures = 0;
    const auto expect = [&](bool condition, const char* label) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << label << '\n';
        }
    };

    canonical::CanonicalControlRecordV1 output{};
    output.header.shard_event_id = 999U;
    const auto raw = Raw();
    const auto control_record = Connecting();
    expect(
        control::ValidateControlRecordV1(control_record) ==
            control::ControlRecordV1Error::kNone,
        "test oracle Phase-3 control record is valid");
    expect(
        canonical::MakeCanonicalControlRecordV1(
            raw, control_record, 1000, 2000, 4U, &output) ==
                canonical::CanonicalControlAdapterErrorV1::kNone,
        "field-wise control adapter succeeds");
    expect(
        output.header.event_type ==
                canonical::CanonicalEventTypeV1::kControl &&
            output.header.record_size ==
                canonical::kCanonicalControlRecordBytesV1 &&
            output.header.trade_date == raw.trade_date &&
            output.header.origin_wal_end_pos == raw.origin_wal_end_pos &&
            output.header.shard_event_id == 4U &&
            output.payload.control_type ==
                canonical::CanonicalControlTypeV1::kConnecting &&
            output.payload.address_sha256 == control_record.address_sha256 &&
            canonical::ValidateCanonicalControlRecordV1(output) ==
                canonical::CanonicalValidationErrorV1::kNone,
        "adapter preserves lineage, hashes and independent Canonical layout");

    const auto decode_error = UnsupportedVersionDecodeError();
    expect(
        control::ValidateControlRecordV1(decode_error) ==
            control::ControlRecordV1Error::kNone,
        "unsupported zero-version DecodeError is a valid Phase-3 fact");
    expect(
        canonical::MakeCanonicalControlRecordV1(
            raw, decode_error, 1001, 2001, 5U, &output) ==
                canonical::CanonicalControlAdapterErrorV1::kNone &&
            output.header.origin_service_version == 0U &&
            output.payload.control_type ==
                canonical::CanonicalControlTypeV1::kDecodeError &&
            output.payload.return_or_error_code == 0x0103U &&
            canonical::ValidateCanonicalControlRecordV1(output) ==
                canonical::CanonicalValidationErrorV1::kNone,
        "Canonical control preserves zero version and exact decode code");

    auto mismatched = control_record;
    mismatched.origin_ingress_sequence = 8U;
    const canonical::CanonicalControlRecordV1 sentinel = output;
    expect(
        canonical::MakeCanonicalControlRecordV1(
            raw, mismatched, 1000, 2000, 5U, &output) ==
                canonical::CanonicalControlAdapterErrorV1::
                    kContextMismatch &&
            output.header.shard_event_id == sentinel.header.shard_event_id,
        "namespace mismatch fails atomically without changing output");

    if (failures != 0) {
        std::cerr << failures << " control adapter assertion(s) failed\n";
        return 1;
    }
    std::cout << "phase5 canonical control adapter tests passed\n";
    return 0;
}
