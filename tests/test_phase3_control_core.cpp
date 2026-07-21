#include "l2flow/common/sha256.h"
#include "l2flow/control/api_decoder.h"
#include "l2flow/control/control_checkpoint_v1.h"
#include "l2flow/control/control_record_v1.h"
#include "l2flow/control/quality_flags_v1.h"
#include "l2flow/control/resubscribe_guard.h"
#include "l2flow/control/sys_decoder.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace control = l2flow::control;
namespace common = l2flow::common;

namespace {

constexpr std::uint16_t kApiVersion = 101U;
constexpr std::uint16_t kApiConnecting = 1U;
constexpr std::uint16_t kApiConnectError = 2U;
constexpr std::uint16_t kApiDisconnected = 3U;
constexpr std::uint16_t kSysVersion = 101U;
constexpr std::uint16_t kSysLogonResponse = 2U;
constexpr std::uint16_t kSysServiceStatus = 5U;
constexpr std::uint16_t kSysSessionStatus = 6U;
constexpr std::uint16_t kSysSubscribeResponse = 23U;

static_assert(control::kControlRecordV1Magic == 0x3152434cU);
static_assert(control::kControlRecordV1Version == 1U);
static_assert(control::kControlRecordV1HeaderBytes == 16U);
static_assert(control::kControlRecordV1Bytes == 256U);
static_assert(control::control_record_v1_offset::kStreamDayId == 28U);
static_assert(control::control_record_v1_offset::kRecordCrc32c == 248U);
static_assert(control::control_record_v1_offset::kReservedTail == 252U);
static_assert(
    control::kControlRecordV1SchemaSha256Hex ==
    "0a49233912fde159bd238b38b8bf2c3aca921432826f2ea7fa4168aaed74b14b");
static_assert(
    control::kControlCheckpointV1SchemaSha256Hex ==
    "90b70205e11edcc9f01ebf5cd7f6c0090e4d03420fde355b15a23305820147a2");

class TestContext final {
public:
    void Expect(bool condition, std::string_view description) {
        if (!condition) {
            ++failures_;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    [[nodiscard]] int failures() const noexcept {
        return failures_;
    }

private:
    int failures_ = 0;
};

void PutU16(std::span<std::byte> bytes,
            std::size_t offset,
            std::uint16_t value) {
    bytes[offset] = static_cast<std::byte>(value & 0xffU);
    bytes[offset + 1U] =
        static_cast<std::byte>((value >> 8U) & 0xffU);
}

void PutU32(std::span<std::byte> bytes,
            std::size_t offset,
            std::uint32_t value) {
    for (std::size_t index = 0U; index < 4U; ++index) {
        bytes[offset + index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
}

std::uint32_t GetU32(std::span<const std::byte> bytes,
                     std::size_t offset) {
    std::uint32_t value = 0U;
    for (std::size_t index = 0U; index < 4U; ++index) {
        value |= static_cast<std::uint32_t>(
                     std::to_integer<std::uint8_t>(
                         bytes[offset + index]))
                 << (index * 8U);
    }
    return value;
}

void PutU64(std::span<std::byte> bytes,
            std::size_t offset,
            std::uint64_t value) {
    for (std::size_t index = 0U; index < 8U; ++index) {
        bytes[offset + index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
}

void AppendU32(std::vector<std::byte>* bytes, std::uint32_t value) {
    for (std::size_t index = 0U; index < 4U; ++index) {
        bytes->push_back(static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU));
    }
}

void PutText(std::span<std::byte> bytes,
             std::size_t descriptor,
             std::size_t target,
             std::string_view text) {
    PutU16(
        bytes, descriptor, static_cast<std::uint16_t>(text.size()));
    PutU32(bytes,
           descriptor + 2U,
           static_cast<std::uint32_t>(target - descriptor));
    for (std::size_t index = 0U; index < text.size(); ++index) {
        bytes[target + index] = static_cast<std::byte>(
            static_cast<unsigned char>(text[index]));
    }
}

void PutList(std::span<std::byte> bytes,
             std::size_t descriptor,
             std::size_t target,
             std::uint32_t count) {
    PutU32(bytes, descriptor, count);
    PutU32(bytes,
           descriptor + 4U,
           static_cast<std::uint32_t>(target - descriptor));
}

template <std::size_t Size>
void Fill(std::array<std::byte, Size>* value, std::uint8_t seed) {
    for (std::size_t index = 0U; index < value->size(); ++index) {
        (*value)[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(seed + index));
    }
}

bool IsZeroDigest(const common::Sha256Digest& digest) {
    return std::all_of(
        digest.begin(), digest.end(), [](std::byte value) {
            return value == std::byte{0U};
        });
}

std::uint32_t IndependentCrc32c(std::span<const std::byte> bytes) {
    std::uint32_t crc = 0xffffffffU;
    for (const std::byte value : bytes) {
        crc ^= static_cast<std::uint32_t>(
            std::to_integer<std::uint8_t>(value));
        for (std::size_t bit = 0U; bit < 8U; ++bit) {
            const std::uint32_t mask = 0U - (crc & 1U);
            crc = (crc >> 1U) ^ (0x82f63b78U & mask);
        }
    }
    return ~crc;
}

control::ControlRecordV1 CompleteControlRecord() {
    control::ControlRecordV1 record;
    record.flags =
        control::kControlRecordResponseManifestHashPresent |
        control::kControlRecordRequiredFailure |
        control::kControlRecordOptionalFailure |
        control::kControlRecordNoncanonicalEmptyOffset;
    record.control_type = control::ControlTypeV1::kSubscriptionRejected;
    record.source_stream_id = 0x01020304U;
    record.capture_date = 20260721U;
    Fill(&record.stream_day_id, 0x10U);
    record.vendor_service_id = 2U;
    record.vendor_service_version = 101U;
    record.vendor_message_id = 23U;
    record.return_or_error_code = 0U;
    record.connection_epoch = 0x11223344U;
    record.subscription_epoch = 0x55667788U;
    record.origin_ingress_sequence = 0x0102030405060708ULL;
    record.origin_record_end_wal_pos = 0x1112131415161718ULL;
    record.quality_flags =
        control::QualityBit(
            control::QualityFlagV1::kNoncanonicalEmptyOffset) |
        control::QualityBit(control::QualityFlagV1::kSchemaUnknown) |
        control::QualityBit(control::QualityFlagV1::kSessionUnknown);
    record.required_count = 3U;
    record.required_ok_count = 1U;
    record.required_failed_count = 2U;
    record.optional_count = 2U;
    record.optional_ok_count = 1U;
    record.optional_failed_count = 1U;
    record.response_entry_count = 5U;
    Fill(&record.response_manifest_sha256, 0x30U);
    Fill(&record.control_state_sha256, 0x90U);
    return record;
}

control::ControlRecordWireV1 BuildGoldenControlRecord(
    const control::ControlRecordV1& record) {
    control::ControlRecordWireV1 wire{};
    std::span<std::byte> bytes(wire);
    PutU32(bytes, 0U, 0x3152434cU);
    PutU16(bytes, 4U, 1U);
    PutU16(bytes, 6U, 16U);
    PutU32(bytes, 8U, 256U);
    PutU32(bytes, 12U, record.flags);
    PutU16(
        bytes, 16U, static_cast<std::uint16_t>(record.control_type));
    PutU16(bytes, 18U, record.decode_error);
    PutU32(bytes, 20U, record.source_stream_id);
    PutU32(bytes, 24U, record.capture_date);
    std::copy(record.stream_day_id.begin(),
              record.stream_day_id.end(),
              bytes.begin() + 28U);
    bytes[44U] = static_cast<std::byte>(record.vendor_service_id);
    PutU16(bytes, 46U, record.vendor_service_version);
    PutU16(bytes, 48U, record.vendor_message_id);
    PutU32(bytes, 52U, record.return_or_error_code);
    PutU32(bytes, 56U, record.connection_epoch);
    PutU32(bytes, 60U, record.subscription_epoch);
    PutU64(bytes, 64U, record.origin_ingress_sequence);
    PutU64(bytes, 72U, record.origin_record_end_wal_pos);
    PutU64(bytes, 80U, record.quality_flags);
    PutU32(bytes, 88U, record.required_count);
    PutU32(bytes, 92U, record.required_ok_count);
    PutU32(bytes, 96U, record.required_failed_count);
    PutU32(bytes, 100U, record.optional_count);
    PutU32(bytes, 104U, record.optional_ok_count);
    PutU32(bytes, 108U, record.optional_failed_count);
    PutU32(bytes, 112U, record.response_entry_count);
    std::copy(record.response_manifest_sha256.begin(),
              record.response_manifest_sha256.end(),
              bytes.begin() + 120U);
    std::copy(record.address_sha256.begin(),
              record.address_sha256.end(),
              bytes.begin() + 152U);
    std::copy(record.error_text_sha256.begin(),
              record.error_text_sha256.end(),
              bytes.begin() + 184U);
    std::copy(record.control_state_sha256.begin(),
              record.control_state_sha256.end(),
              bytes.begin() + 216U);
    PutU32(bytes, 248U, IndependentCrc32c(bytes));
    return wire;
}

void TestControlRecordCodec(TestContext* test) {
    control::ControlRecordV1 expected = CompleteControlRecord();
    const control::ControlRecordWireV1 golden =
        BuildGoldenControlRecord(expected);
    expected.record_crc32c =
        GetU32(std::span<const std::byte>(golden), 248U);

    control::ControlRecordWireV1 encoded{};
    test->Expect(
        control::EncodeControlRecordV1(expected, &encoded) ==
            control::ControlRecordV1Error::kNone,
        "complete ControlRecordV1 encodes");
    test->Expect(encoded == golden,
                 "ControlRecordV1 matches independent 256-byte golden");

    control::ControlRecordV1 decoded;
    test->Expect(
        control::DecodeControlRecordV1(encoded, &decoded) ==
                control::ControlRecordV1Error::kNone &&
            decoded == expected,
        "ControlRecordV1 round-trips every field and namespace component");

    const std::array<std::pair<std::size_t, std::size_t>, 4U>
        reserved_ranges{{
            {45U, 1U},
            {50U, 2U},
            {116U, 4U},
            {252U, 4U},
        }};
    for (const auto& range : reserved_ranges) {
        test->Expect(
            std::all_of(
                encoded.begin() + range.first,
                encoded.begin() + range.first + range.second,
                [](std::byte value) {
                    return value == std::byte{0U};
                }),
            "all ControlRecordV1 reserved bytes encode as zero");
        control::ControlRecordWireV1 reserved = encoded;
        reserved[range.first] = std::byte{1U};
        test->Expect(
            control::DecodeControlRecordV1(reserved, &decoded) ==
                control::ControlRecordV1Error::kNonzeroReserved,
            "nonzero ControlRecordV1 reserved bytes are rejected");
    }

    control::ControlRecordWireV1 corrupted = encoded;
    corrupted[120U] ^= std::byte{1U};
    test->Expect(
        control::DecodeControlRecordV1(corrupted, &decoded) ==
            control::ControlRecordV1Error::kCrcMismatch,
        "ControlRecordV1 CRC detects a payload bit flip");

    control::ControlRecordV1 invalid = expected;
    invalid.source_stream_id = 0U;
    test->Expect(
        control::ValidateControlRecordV1(invalid) ==
            control::ControlRecordV1Error::kInvalidIdentity,
        "zero source stream rejects an incomplete namespace");
    invalid = expected;
    invalid.capture_date = 0U;
    test->Expect(
        control::ValidateControlRecordV1(invalid) ==
            control::ControlRecordV1Error::kInvalidIdentity,
        "zero capture date rejects an incomplete namespace");
    invalid = expected;
    invalid.stream_day_id = {};
    test->Expect(
        control::ValidateControlRecordV1(invalid) ==
            control::ControlRecordV1Error::kInvalidIdentity,
        "zero stream-day identity rejects an incomplete namespace");

    invalid = expected;
    invalid.vendor_message_id = kSysLogonResponse;
    test->Expect(
        control::ValidateControlRecordV1(invalid) ==
            control::ControlRecordV1Error::kInconsistentFields,
        "ControlRecord type rejects a different SYS message identity");
    invalid = expected;
    invalid.return_or_error_code = 5U;
    test->Expect(
        control::ValidateControlRecordV1(invalid) ==
            control::ControlRecordV1Error::kInconsistentFields,
        "subscription response rejects an unreachable top-level code");
    invalid = expected;
    invalid.vendor_service_version = kSysVersion - 1U;
    test->Expect(
        control::ValidateControlRecordV1(invalid) ==
            control::ControlRecordV1Error::kInconsistentFields,
        "successful control record requires the frozen service version");
    invalid = expected;
    invalid.flags |= control::kControlRecordAddressHashPresent;
    Fill(&invalid.address_sha256, 0x50U);
    test->Expect(
        control::ValidateControlRecordV1(invalid) ==
            control::ControlRecordV1Error::kInconsistentFields,
        "SYS response rejects an unreachable API address digest");
    invalid = expected;
    invalid.quality_flags |= control::QualityBit(
        control::QualityFlagV1::kVendorSequenceGap);
    test->Expect(
        control::ValidateControlRecordV1(invalid) ==
            control::ControlRecordV1Error::kInconsistentFields,
        "ControlRecord rejects quality bits its Phase-3 decoder cannot emit");

    invalid = expected;
    invalid.control_type = control::ControlTypeV1::kLogonFailure;
    invalid.vendor_message_id = kSysLogonResponse;
    invalid.return_or_error_code = 5U;
    invalid.flags = control::kControlRecordResponseManifestHashPresent;
    invalid.quality_flags = 0U;
    test->Expect(
        control::ValidateControlRecordV1(invalid) ==
            control::ControlRecordV1Error::kInconsistentFields,
        "a failed logon record cannot omit SESSION_UNKNOWN");

    invalid = expected;
    invalid.control_type = control::ControlTypeV1::kLogonSuccess;
    invalid.vendor_message_id = kSysLogonResponse;
    invalid.flags = control::kControlRecordResponseManifestHashPresent;
    invalid.quality_flags = control::QualityBit(
        control::QualityFlagV1::kSessionUnknown);
    test->Expect(
        control::ValidateControlRecordV1(invalid) ==
            control::ControlRecordV1Error::kInconsistentFields,
        "an unpoisoned successful logon cannot carry SESSION_UNKNOWN");

    invalid = expected;
    invalid.control_type = control::ControlTypeV1::kConnectError;
    invalid.vendor_service_id = 1U;
    invalid.vendor_message_id = kApiConnectError;
    invalid.flags = control::kControlRecordAddressHashPresent |
                    control::kControlRecordErrorTextHashPresent;
    invalid.response_entry_count = 0U;
    invalid.response_manifest_sha256 = {};
    Fill(&invalid.address_sha256, 0x50U);
    Fill(&invalid.error_text_sha256, 0x70U);
    invalid.quality_flags = control::QualityBit(
        control::QualityFlagV1::kSessionUnknown);
    test->Expect(
        control::ValidateControlRecordV1(invalid) ==
            control::ControlRecordV1Error::kInconsistentFields,
        "ConnectError cannot omit SOURCE_DISCONNECTED attribution");

    control::ControlRecordV1 decode_error = expected;
    decode_error.control_type = control::ControlTypeV1::kDecodeError;
    decode_error.decode_error = 0x0203U;
    decode_error.vendor_service_version = kSysVersion - 1U;
    decode_error.flags &=
        ~(control::kControlRecordResponseManifestHashPresent |
          control::kControlRecordNoncanonicalEmptyOffset);
    decode_error.response_entry_count = 0U;
    decode_error.response_manifest_sha256 = {};
    test->Expect(
        control::ValidateControlRecordV1(decode_error) ==
            control::ControlRecordV1Error::kNone,
        "reachable recognized-message SYS decode error validates");
    decode_error.decode_error = 0x0209U;
    test->Expect(
        control::ValidateControlRecordV1(decode_error) ==
            control::ControlRecordV1Error::kInconsistentFields,
        "resource-exhaustion code that never commits is not serializable");
}

std::vector<std::byte> OneTextBody(std::string_view text) {
    std::vector<std::byte> body(6U + text.size());
    PutText(std::span<std::byte>(body), 0U, 6U, text);
    return body;
}

std::vector<std::byte> TwoTextBody(std::string_view error,
                                   std::string_view address) {
    std::vector<std::byte> body(12U + error.size() + address.size());
    const std::size_t error_start = 12U;
    const std::size_t address_start = error_start + error.size();
    PutText(std::span<std::byte>(body), 0U, error_start, error);
    PutText(std::span<std::byte>(body), 6U, address_start, address);
    return body;
}

void TestApiDecoder(TestContext* test) {
    control::DecodedApiControlV1 decoded;
    const std::vector<std::byte> connecting = OneTextBody("tcp://feed");
    test->Expect(
        control::DecodeApiControlV1(
            kApiVersion, kApiConnecting, connecting, &decoded) ==
                control::ApiControlDecodeErrorV1::kNone &&
            decoded.control_type == control::ControlTypeV1::kConnecting &&
            decoded.address_present && !decoded.error_text_present &&
            !IsZeroDigest(decoded.address_sha256),
        "checked API decoder accepts an independently built Connecting body");

    const std::vector<std::byte> failure =
        TwoTextBody("refused", "tcp://feed");
    test->Expect(
        control::DecodeApiControlV1(
            kApiVersion, kApiConnectError, failure, &decoded) ==
                control::ApiControlDecodeErrorV1::kNone &&
            decoded.control_type ==
                control::ControlTypeV1::kConnectError &&
            decoded.address_present && decoded.error_text_present,
        "checked API decoder accepts ConnectError dynamic strings");
    test->Expect(
        control::DecodeApiControlV1(
            kApiVersion, kApiDisconnected, failure, &decoded) ==
                control::ApiControlDecodeErrorV1::kNone &&
            decoded.control_type ==
                control::ControlTypeV1::kDisconnected,
        "checked API decoder accepts Disconnected dynamic strings");

    std::vector<std::byte> out_of_bounds(7U);
    PutU16(std::span<std::byte>(out_of_bounds), 0U, 2U);
    PutU32(std::span<std::byte>(out_of_bounds), 2U, 6U);
    out_of_bounds[6U] = std::byte{0x78U};
    test->Expect(
        control::DecodeApiControlV1(
            kApiVersion,
            kApiConnecting,
            out_of_bounds,
            &decoded) == control::ApiControlDecodeErrorV1::kOffsetInvalid,
        "checked API decoder rejects an out-of-bounds string offset");

    std::vector<std::byte> overlap(15U);
    PutU16(std::span<std::byte>(overlap), 0U, 3U);
    PutU32(std::span<std::byte>(overlap), 2U, 12U);
    PutU16(std::span<std::byte>(overlap), 6U, 3U);
    PutU32(std::span<std::byte>(overlap), 8U, 6U);
    overlap[12U] = std::byte{0x62U};
    overlap[13U] = std::byte{0x61U};
    overlap[14U] = std::byte{0x64U};
    test->Expect(
        control::DecodeApiControlV1(
            kApiVersion, kApiConnectError, overlap, &decoded) ==
            control::ApiControlDecodeErrorV1::kRangeOverlap,
        "checked API decoder rejects aliased dynamic ranges");
}

std::vector<std::byte> LogonResponseBody() {
    std::vector<std::byte> body(56U);
    std::span<std::byte> bytes(body);
    PutText(bytes, 0U, 24U, "usr");
    PutText(bytes, 6U, 27U, "pw");
    PutList(bytes, 12U, 32U, 1U);
    PutU32(bytes, 20U, 0U);
    PutU32(bytes, 32U, 4U);
    PutU32(bytes, 36U, 101U);
    PutList(bytes, 40U, 48U, 1U);
    PutU32(bytes, 48U, 4U);
    PutU32(bytes, 52U, 0U);
    return body;
}

std::vector<std::byte> SubscribeResponseBody(
    std::span<const std::pair<std::uint32_t, std::uint32_t>> messages) {
    std::vector<std::byte> body(24U + messages.size() * 8U);
    std::span<std::byte> bytes(body);
    PutList(bytes, 0U, 8U, 1U);
    PutU32(bytes, 8U, 4U);
    PutU32(bytes, 12U, 101U);
    PutList(bytes,
            16U,
            24U,
            static_cast<std::uint32_t>(messages.size()));
    for (std::size_t index = 0U; index < messages.size(); ++index) {
        PutU32(bytes, 24U + index * 8U, messages[index].first);
        PutU32(bytes, 28U + index * 8U, messages[index].second);
    }
    return body;
}

std::vector<std::byte> ServiceStatusBody() {
    std::vector<std::byte> body(72U);
    std::span<std::byte> bytes(body);
    PutU32(bytes, 0U, 213234U);
    PutList(bytes, 44U, 60U, 1U);
    PutU64(bytes, 52U, 17U);
    PutU32(bytes, 60U, 4U);
    PutU32(bytes, 64U, 123U);
    PutU32(bytes, 68U, 7U);
    return body;
}

std::vector<std::byte> SessionStatusBody() {
    std::vector<std::byte> body(51U);
    std::span<std::byte> bytes(body);
    PutList(bytes, 0U, 8U, 1U);
    PutU32(bytes, 8U, 213234U);
    PutText(bytes, 12U, 44U, "peer");
    PutU32(bytes, 18U, 20260721U);
    PutU32(bytes, 22U, 93000000U);
    PutU32(bytes, 26U, 1U);
    PutU32(bytes, 30U, 1U);
    PutU32(bytes, 34U, 0U);
    PutText(bytes, 38U, 48U, "4.4");
    return body;
}

void TestSysDecoder(TestContext* test) {
    control::DecodedSysControlV1 decoded;
    const std::vector<std::byte> logon = LogonResponseBody();
    test->Expect(
        control::DecodeSysControlV1(
            kSysVersion, kSysLogonResponse, logon, &decoded) ==
                control::SysControlDecodeErrorV1::kNone &&
            decoded.control_type ==
                control::ControlTypeV1::kLogonSuccess &&
            decoded.response_manifest_present &&
            decoded.subscription_statuses.size() == 1U &&
            decoded.subscription_statuses[0U] ==
                control::SubscriptionStatusV1{4U, 101U, 4U, 0U},
        "checked SYS decoder accepts nested LogonResponse lists");

    const std::array<std::pair<std::uint32_t, std::uint32_t>, 2U>
        subscriptions = {{{24U, 0U}, {4U, 0U}}};
    const std::vector<std::byte> subscribe =
        SubscribeResponseBody(subscriptions);
    test->Expect(
        control::DecodeSysControlV1(
            kSysVersion,
            kSysSubscribeResponse,
            subscribe,
            &decoded) == control::SysControlDecodeErrorV1::kNone &&
            decoded.control_type ==
                control::ControlTypeV1::kSubscriptionAccepted &&
            decoded.subscription_statuses.size() == 2U &&
            decoded.subscription_statuses[0U].message_id == 4U &&
            decoded.subscription_statuses[1U].message_id == 24U,
        "checked SYS decoder canonicalizes valid subscription order");

    const std::vector<std::byte> service = ServiceStatusBody();
    test->Expect(
        control::DecodeSysControlV1(
            kSysVersion, kSysServiceStatus, service, &decoded) ==
                control::SysControlDecodeErrorV1::kNone &&
            decoded.control_type ==
                control::ControlTypeV1::kServiceStatus &&
            decoded.return_or_error_code == 213234U &&
            decoded.response_entry_count == 1U,
        "checked SYS decoder accepts ServiceStatus list framing");

    const std::vector<std::byte> session = SessionStatusBody();
    test->Expect(
        control::DecodeSysControlV1(
            kSysVersion, kSysSessionStatus, session, &decoded) ==
                control::SysControlDecodeErrorV1::kNone &&
            decoded.control_type ==
                control::ControlTypeV1::kSessionStatus &&
            decoded.response_entry_count == 1U,
        "checked SYS decoder accepts SessionStatus nested strings");

    std::vector<std::byte> malformed_list(8U);
    PutU32(std::span<std::byte>(malformed_list), 0U, 1U);
    PutU32(std::span<std::byte>(malformed_list), 4U, 4U);
    test->Expect(
        control::DecodeSysControlV1(
            kSysVersion,
            kSysSubscribeResponse,
            malformed_list,
            &decoded) == control::SysControlDecodeErrorV1::kOffsetInvalid,
        "checked SYS decoder rejects a list target inside fixed bytes");

    const std::array<std::pair<std::uint32_t, std::uint32_t>, 2U>
        duplicates = {{{4U, 0U}, {4U, 5U}}};
    const std::vector<std::byte> duplicate_body =
        SubscribeResponseBody(duplicates);
    test->Expect(
        control::DecodeSysControlV1(
            kSysVersion,
            kSysSubscribeResponse,
            duplicate_body,
            &decoded) ==
            control::SysControlDecodeErrorV1::kDuplicateSubscriptionKey,
        "checked SYS decoder rejects duplicate subscription keys");

    std::vector<std::byte> malformed_session = SessionStatusBody();
    PutU16(std::span<std::byte>(malformed_session), 12U, 1U);
    PutU32(std::span<std::byte>(malformed_session), 14U, 6U);
    test->Expect(
        control::DecodeSysControlV1(
            kSysVersion,
            kSysSessionStatus,
            malformed_session,
            &decoded) == control::SysControlDecodeErrorV1::kOffsetInvalid,
        "checked SYS decoder rejects a nested string inside client records");
}

common::Sha256Digest IndependentResponseManifest(
    std::span<const control::SubscriptionStatusV1> statuses) {
    constexpr std::string_view domain =
        "L2FLOW_PHASE3_SUBSCRIPTION_RESPONSE_MANIFEST_V1";
    std::vector<std::byte> tuples;
    AppendU32(&tuples, static_cast<std::uint32_t>(statuses.size()));
    for (const control::SubscriptionStatusV1& status : statuses) {
        AppendU32(&tuples, status.service_id);
        AppendU32(&tuples, status.service_version);
        AppendU32(&tuples, status.message_id);
        AppendU32(&tuples, status.status);
    }
    const std::span<const char> domain_chars(domain.data(), domain.size());
    const std::array<std::byte, 1U> separator = {std::byte{0U}};
    common::Sha256Digest digest{};
    common::Sha256Hasher hasher;
    if (!hasher.Update(std::as_bytes(domain_chars)) ||
        !hasher.Update(separator) ||
        !hasher.Update(std::span<const std::byte>(tuples)) ||
        !hasher.Finalize(&digest)) {
        return {};
    }
    return digest;
}

void TestManifestCanonicalization(TestContext* test) {
    const std::array<control::SubscriptionStatusV1, 2U> sorted = {{
        {4U, 101U, 4U, 0U},
        {4U, 101U, 24U, 5U},
    }};
    const common::Sha256Digest actual =
        control::ComputeSubscriptionResponseManifestSha256V1(sorted);
    test->Expect(
        !IsZeroDigest(actual) &&
            actual == IndependentResponseManifest(sorted),
        "canonical response manifest matches an independent byte builder");

    const std::array<control::SubscriptionStatusV1, 2U> unsorted = {{
        sorted[1U], sorted[0U],
    }};
    test->Expect(
        IsZeroDigest(
            control::ComputeSubscriptionResponseManifestSha256V1(
                unsorted)),
        "response manifest rejects noncanonical tuple ordering");

    const std::array<control::SubscriptionStatusV1, 2U> duplicate = {{
        sorted[0U],
        {4U, 101U, 4U, 9U},
    }};
    test->Expect(
        IsZeroDigest(
            control::ComputeSubscriptionResponseManifestSha256V1(
                duplicate)),
        "response manifest rejects duplicate subscription keys");
}

void TestResubscribeStructuralRefusal(TestContext* test) {
    control::ControlDecoderSnapshotV1 state;
    state.source_stream_id = 1001U;
    state.capture_date = 20260721U;
    Fill(&state.stream_day_id, 0x11U);
    Fill(&state.requested_manifest_sha256, 0x21U);
    state.session_phase = control::ControlSessionPhaseV1::kLoggedIn;
    state.logged_in = true;
    state.poisoned = false;

    common::Sha256Digest proposed{};
    Fill(&proposed, 0x41U);
    control::ResubscribeMaintenanceWindowV1 window;
    window.schema_version =
        control::kResubscribeMaintenanceWindowSchemaVersionV1;
    window.explicitly_outside_trading_interval = true;

    control::DurableResubscribeAuditReceiptV1 audit;
    audit.schema_version =
        control::kDurableResubscribeAuditSchemaVersionV1;
    audit.source_stream_id = state.source_stream_id;
    audit.capture_date = state.capture_date;
    audit.stream_day_id = state.stream_day_id;
    audit.old_manifest_sha256 = state.requested_manifest_sha256;
    audit.proposed_manifest_sha256 = proposed;
    Fill(&audit.actor_sha256, 0x61U);
    Fill(&audit.reason_sha256, 0x81U);
    Fill(&audit.audit_record_sha256, 0xa1U);
    audit.operation =
        control::ResubscribeOperationV1::kReplaceRequestedManifest;
    audit.file_barrier_complete = true;
    audit.actual_parent_directory_barrier_complete = true;

    control::ResubscribeAuthorizationV1 authorization;
    authorization.reviewed_operational_authorization = true;
    test->Expect(
        control::AuthorizeResubscribeV1(
            state, proposed, window, audit, authorization) ==
            control::ResubscribeGuardResultV1::
                kNoReplayableManifestTransition,
        "ReSubscribe V1 remains structurally refused after every guard passes");
}

}  // namespace

int main() {
    TestContext test;
    TestControlRecordCodec(&test);
    TestApiDecoder(&test);
    TestSysDecoder(&test);
    TestManifestCanonicalization(&test);
    TestResubscribeStructuralRefusal(&test);
    if (test.failures() != 0) {
        std::cerr << test.failures()
                  << " Phase 3 control-core assertion(s) failed\n";
        return 1;
    }
    std::cout << "Phase 3 control-core tests passed\n";
    return 0;
}
