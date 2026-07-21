#pragma once

#include "l2flow/control/control_decoder.h"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace l2flow::control {

inline constexpr std::uint32_t kControlCheckpointV1Magic =
    phase3_schema_v1::kControlCheckpointV1Magic;
inline constexpr std::uint16_t kControlCheckpointV1Version =
    phase3_schema_v1::kControlCheckpointV1Version;
inline constexpr std::size_t kControlCheckpointV1HeaderBytes =
    phase3_schema_v1::kControlCheckpointV1HeaderBytes;
inline constexpr std::size_t kControlCheckpointV1EntryBytes =
    phase3_schema_v1::kControlCheckpointV1EntryBytes;
inline constexpr std::size_t kControlCheckpointV1TrailerBytes =
    phase3_schema_v1::kControlCheckpointV1TrailerBytes;
inline constexpr std::size_t kControlCheckpointV1MaximumEntries =
    phase3_schema_v1::kControlCheckpointV1MaximumEntries;
inline constexpr auto kControlCheckpointV1SchemaSha256 =
    phase3_schema_v1::kControlCheckpointV1SchemaSha256;
inline constexpr std::string_view
    kControlCheckpointV1SchemaSha256Hex =
        phase3_schema_v1::kControlCheckpointV1SchemaSha256Hex;

enum class ControlCheckpointV1Error : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidModel,
    kInvalidWireSize,
    kInvalidMagic,
    kUnsupportedVersion,
    kInvalidHeaderSize,
    kInvalidEntrySize,
    kInvalidTotalSize,
    kUnknownFlags,
    kNonzeroReserved,
    kHeaderCrcMismatch,
    kDigestMismatch,
    kResourceExhausted,
};

[[nodiscard]] std::string_view ControlCheckpointV1ErrorName(
    ControlCheckpointV1Error error) noexcept;

[[nodiscard]] ControlCheckpointV1Error
ValidateControlDecoderCheckpointV1(
    const ControlDecoderCheckpointV1& checkpoint) noexcept;
[[nodiscard]] ControlCheckpointV1Error
EncodeControlDecoderCheckpointV1(
    const ControlDecoderCheckpointV1& checkpoint,
    std::vector<std::byte>* wire) noexcept;
[[nodiscard]] ControlCheckpointV1Error
DecodeControlDecoderCheckpointV1(
    std::span<const std::byte> wire,
    ControlDecoderCheckpointV1* checkpoint) noexcept;

}  // namespace l2flow::control
