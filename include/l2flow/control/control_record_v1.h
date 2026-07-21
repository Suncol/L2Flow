#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/control/phase3_schema_v1_generated.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace l2flow::control {

// Standalone Phase-3 control event. This is intentionally not the Phase-5
// CanonicalHeaderV1 + ControlPayloadV1 record, whose enclosing schema is not
// frozen yet. The explicit codec is portable and never persists C++ layout.
inline constexpr std::uint32_t kControlRecordV1Magic =
    phase3_schema_v1::kControlRecordV1Magic;
inline constexpr std::uint16_t kControlRecordV1Version =
    phase3_schema_v1::kControlRecordV1Version;
inline constexpr std::uint16_t kControlRecordV1HeaderBytes =
    phase3_schema_v1::kControlRecordV1HeaderBytes;
inline constexpr std::size_t kControlRecordV1Bytes =
    phase3_schema_v1::kControlRecordV1Bytes;
inline constexpr auto kControlRecordV1SchemaSha256 =
    phase3_schema_v1::kControlRecordV1SchemaSha256;
inline constexpr std::string_view kControlRecordV1SchemaSha256Hex =
    phase3_schema_v1::kControlRecordV1SchemaSha256Hex;

using ControlDigestV1 = l2flow::common::Sha256Digest;
using ControlRecordWireV1 =
    std::array<std::byte, kControlRecordV1Bytes>;

enum class ControlTypeV1 : std::uint16_t {
    kConnecting =
        phase3_schema_v1::control_type_v1_value::kConnecting,
    kConnectError =
        phase3_schema_v1::control_type_v1_value::kConnectError,
    kDisconnected =
        phase3_schema_v1::control_type_v1_value::kDisconnected,
    kLogonSuccess =
        phase3_schema_v1::control_type_v1_value::kLogonSuccess,
    kLogonFailure =
        phase3_schema_v1::control_type_v1_value::kLogonFailure,
    kSubscriptionAccepted = phase3_schema_v1::
        control_type_v1_value::kSubscriptionAccepted,
    kSubscriptionRejected = phase3_schema_v1::
        control_type_v1_value::kSubscriptionRejected,
    kServiceStatus =
        phase3_schema_v1::control_type_v1_value::kServiceStatus,
    kSessionStatus =
        phase3_schema_v1::control_type_v1_value::kSessionStatus,
    // A recognized API/SYS message could not be decoded safely. The Raw
    // record remains intact; the authoritative control state is poisoned.
    kDecodeError =
        phase3_schema_v1::control_type_v1_value::kDecodeError,
};

enum ControlRecordFlagV1 : std::uint32_t {
    kControlRecordResponseManifestHashPresent = phase3_schema_v1::
        control_record_flag_v1_value::kResponseManifestHashPresent,
    kControlRecordAddressHashPresent = phase3_schema_v1::
        control_record_flag_v1_value::kAddressHashPresent,
    kControlRecordErrorTextHashPresent = phase3_schema_v1::
        control_record_flag_v1_value::kErrorTextHashPresent,
    kControlRecordRequiredFailure = phase3_schema_v1::
        control_record_flag_v1_value::kRequiredFailure,
    kControlRecordOptionalFailure = phase3_schema_v1::
        control_record_flag_v1_value::kOptionalFailure,
    kControlRecordNoncanonicalEmptyOffset = phase3_schema_v1::
        control_record_flag_v1_value::kNoncanonicalEmptyOffset,
};

inline constexpr std::uint32_t kControlRecordFlagsMask =
    kControlRecordResponseManifestHashPresent |
    kControlRecordAddressHashPresent |
    kControlRecordErrorTextHashPresent |
    kControlRecordRequiredFailure |
    kControlRecordOptionalFailure |
    kControlRecordNoncanonicalEmptyOffset;

// Exact little-endian wire offsets.
namespace control_record_v1_offset =
    phase3_schema_v1::control_record_v1_offset;

struct ControlRecordV1 final {
    std::uint32_t flags = 0U;
    ControlTypeV1 control_type = ControlTypeV1::kDecodeError;
    std::uint16_t decode_error = 0U;
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    l2flow::common::Identity128 stream_day_id{};
    std::uint8_t vendor_service_id = 0U;
    std::uint16_t vendor_service_version = 0U;
    std::uint16_t vendor_message_id = 0U;
    std::uint32_t return_or_error_code = 0U;
    std::uint32_t connection_epoch = 0U;
    std::uint32_t subscription_epoch = 0U;
    std::uint64_t origin_ingress_sequence = 0U;
    std::uint64_t origin_record_end_wal_pos = 0U;
    std::uint64_t quality_flags = 0U;
    std::uint32_t required_count = 0U;
    std::uint32_t required_ok_count = 0U;
    std::uint32_t required_failed_count = 0U;
    std::uint32_t optional_count = 0U;
    std::uint32_t optional_ok_count = 0U;
    std::uint32_t optional_failed_count = 0U;
    std::uint32_t response_entry_count = 0U;
    ControlDigestV1 response_manifest_sha256{};
    ControlDigestV1 address_sha256{};
    ControlDigestV1 error_text_sha256{};
    ControlDigestV1 control_state_sha256{};
    std::uint32_t record_crc32c = 0U;

    [[nodiscard]] friend constexpr bool operator==(
        const ControlRecordV1&,
        const ControlRecordV1&) noexcept = default;
};

enum class ControlRecordV1Error : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidWireSize,
    kInvalidMagic,
    kUnsupportedVersion,
    kInvalidHeaderSize,
    kInvalidRecordSize,
    kUnknownFlags,
    kInvalidControlType,
    kInvalidIdentity,
    kInvalidOriginCursor,
    kInvalidCounts,
    kInvalidDecodeError,
    kInconsistentFields,
    kNonzeroReserved,
    kCrcMismatch,
};

[[nodiscard]] std::string_view ControlRecordV1ErrorName(
    ControlRecordV1Error error) noexcept;

[[nodiscard]] ControlRecordV1Error ValidateControlRecordV1(
    const ControlRecordV1& record) noexcept;
[[nodiscard]] ControlRecordV1Error EncodeControlRecordV1(
    const ControlRecordV1& record,
    ControlRecordWireV1* wire) noexcept;
[[nodiscard]] ControlRecordV1Error DecodeControlRecordV1(
    std::span<const std::byte> wire,
    ControlRecordV1* record) noexcept;

}  // namespace l2flow::control
