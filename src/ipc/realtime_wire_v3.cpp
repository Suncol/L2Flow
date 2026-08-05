#include "l2flow/ipc/realtime_wire_v3.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>

namespace l2flow::ipc {
namespace {

constexpr std::size_t kCursorInstrumentOffset = 16U;
constexpr std::size_t kCursorReservedOffset = 20U;
constexpr std::size_t kCursorSequenceOffset = 24U;
constexpr std::size_t kStatusMajorOffset = 4U;
constexpr std::size_t kStatusMinorOffset = 6U;
constexpr std::size_t kStatusDatasetOffset = 8U;
constexpr std::size_t kStatusRepairStateOffset = 9U;
constexpr std::size_t kStatusReservedOffset = 10U;
constexpr std::size_t kStatusInstrumentOffset = 12U;
constexpr std::size_t kStatusStableTailOffset = 16U;
constexpr std::size_t kStatusRepairThroughOffset = 24U;

template <typename Unsigned>
void WriteLittleEndian(
    Unsigned value,
    std::size_t offset,
    std::span<std::byte> output) noexcept {
    static_assert(std::is_unsigned_v<Unsigned>);
    for (std::size_t index = 0U; index < sizeof(Unsigned); ++index) {
        output[offset + index] = static_cast<std::byte>(
            value & static_cast<Unsigned>(0xffU));
        value >>= 8U;
    }
}

template <typename Unsigned>
[[nodiscard]] Unsigned ReadLittleEndian(
    std::size_t offset,
    std::span<const std::byte> input) noexcept {
    static_assert(std::is_unsigned_v<Unsigned>);
    std::uint64_t result = 0U;
    for (std::size_t index = 0U; index < sizeof(Unsigned); ++index) {
        result |= static_cast<std::uint64_t>(
                      std::to_integer<std::uint8_t>(input[offset + index]))
                  << (index * 8U);
    }
    return static_cast<Unsigned>(result);
}

template <typename Cursor>
[[nodiscard]] RealtimeWireCodecErrorV3 EncodeCursor(
    const Cursor& cursor,
    std::uint64_t sequence,
    RealtimeCursorBytesV3* output) noexcept {
    if (output == nullptr) {
        return RealtimeWireCodecErrorV3::kNullOutput;
    }
    output->fill(std::byte{0U});
    if (cursor.reserved != 0U) {
        return RealtimeWireCodecErrorV3::kReservedNonzero;
    }
    std::copy(cursor.session_id.begin(), cursor.session_id.end(),
              output->begin());
    WriteLittleEndian<std::uint32_t>(
        cursor.instrument_id, kCursorInstrumentOffset, *output);
    WriteLittleEndian<std::uint64_t>(
        sequence, kCursorSequenceOffset, *output);
    return RealtimeWireCodecErrorV3::kNone;
}

template <typename Cursor>
[[nodiscard]] RealtimeWireCodecErrorV3 DecodeCursor(
    std::span<const std::byte> input,
    Cursor* output,
    std::uint64_t Cursor::* sequence) noexcept {
    if (output == nullptr) {
        return RealtimeWireCodecErrorV3::kNullOutput;
    }
    *output = {};
    if (input.size() != kRealtimeCursorWireSizeV3) {
        return RealtimeWireCodecErrorV3::kInvalidSize;
    }
    if (ReadLittleEndian<std::uint32_t>(
            kCursorReservedOffset, input) != 0U) {
        return RealtimeWireCodecErrorV3::kReservedNonzero;
    }
    std::copy_n(input.begin(), output->session_id.size(),
                output->session_id.begin());
    output->instrument_id = ReadLittleEndian<std::uint32_t>(
        kCursorInstrumentOffset, input);
    output->*sequence = ReadLittleEndian<std::uint64_t>(
        kCursorSequenceOffset, input);
    return RealtimeWireCodecErrorV3::kNone;
}

[[nodiscard]] bool ValidDataset(std::uint8_t value) noexcept {
    return value >= static_cast<std::uint8_t>(
                        RealtimeDatasetV3::kFastTick) &&
           value <= static_cast<std::uint8_t>(
                        RealtimeDatasetV3::kKLine);
}

[[nodiscard]] bool ValidRepairState(std::uint8_t value) noexcept {
    return value <= static_cast<std::uint8_t>(
                        RealtimeRepairStateWireV3::kUnrecoverable);
}

}  // namespace

std::string_view RealtimeWireCodecErrorNameV3(
    RealtimeWireCodecErrorV3 error) noexcept {
    switch (error) {
        case RealtimeWireCodecErrorV3::kNone:
            return "none";
        case RealtimeWireCodecErrorV3::kNullOutput:
            return "null_output";
        case RealtimeWireCodecErrorV3::kInvalidSize:
            return "invalid_size";
        case RealtimeWireCodecErrorV3::kReservedNonzero:
            return "reserved_nonzero";
        case RealtimeWireCodecErrorV3::kInvalidMagic:
            return "invalid_magic";
        case RealtimeWireCodecErrorV3::kIncompatibleVersion:
            return "incompatible_version";
        case RealtimeWireCodecErrorV3::kInvalidDataset:
            return "invalid_dataset";
        case RealtimeWireCodecErrorV3::kInvalidRepairState:
            return "invalid_repair_state";
    }
    return "unknown";
}

RealtimeWireCodecErrorV3 EncodeFastTickCursorWireV3(
    const FastTickCursorWireV3& cursor,
    RealtimeCursorBytesV3* output) noexcept {
    return EncodeCursor(cursor, cursor.next_arrival_row, output);
}

RealtimeWireCodecErrorV3 EncodeEventChangeCursorWireV3(
    const EventChangeCursorWireV3& cursor,
    RealtimeCursorBytesV3* output) noexcept {
    return EncodeCursor(cursor, cursor.next_change_sequence, output);
}

RealtimeWireCodecErrorV3 EncodeKLineChangeCursorWireV3(
    const KLineChangeCursorWireV3& cursor,
    RealtimeCursorBytesV3* output) noexcept {
    return EncodeCursor(cursor, cursor.next_change_sequence, output);
}

RealtimeWireCodecErrorV3 DecodeFastTickCursorWireV3(
    std::span<const std::byte> input,
    FastTickCursorWireV3* output) noexcept {
    return DecodeCursor(input, output,
                        &FastTickCursorWireV3::next_arrival_row);
}

RealtimeWireCodecErrorV3 DecodeEventChangeCursorWireV3(
    std::span<const std::byte> input,
    EventChangeCursorWireV3* output) noexcept {
    return DecodeCursor(input, output,
                        &EventChangeCursorWireV3::next_change_sequence);
}

RealtimeWireCodecErrorV3 DecodeKLineChangeCursorWireV3(
    std::span<const std::byte> input,
    KLineChangeCursorWireV3* output) noexcept {
    return DecodeCursor(input, output,
                        &KLineChangeCursorWireV3::next_change_sequence);
}

RealtimeWireCodecErrorV3 EncodeStableStatusWireV3(
    const InstrumentStableStatusWireV3& status,
    RealtimeStableStatusBytesV3* output) noexcept {
    if (output == nullptr) {
        return RealtimeWireCodecErrorV3::kNullOutput;
    }
    output->fill(std::byte{0U});
    if (status.magic != kRealtimeWireMagicV3) {
        return RealtimeWireCodecErrorV3::kInvalidMagic;
    }
    if (status.major != kRealtimeWireMajorV3 ||
        status.minor > kRealtimeWireMinorV3) {
        return RealtimeWireCodecErrorV3::kIncompatibleVersion;
    }
    if (status.reserved != 0U) {
        return RealtimeWireCodecErrorV3::kReservedNonzero;
    }
    if (!ValidDataset(status.dataset)) {
        return RealtimeWireCodecErrorV3::kInvalidDataset;
    }
    if (!ValidRepairState(status.repair_state)) {
        return RealtimeWireCodecErrorV3::kInvalidRepairState;
    }
    std::transform(
        status.magic.begin(), status.magic.end(), output->begin(),
        [](std::uint8_t value) { return static_cast<std::byte>(value); });
    WriteLittleEndian<std::uint16_t>(
        status.major, kStatusMajorOffset, *output);
    WriteLittleEndian<std::uint16_t>(
        status.minor, kStatusMinorOffset, *output);
    (*output)[kStatusDatasetOffset] =
        static_cast<std::byte>(status.dataset);
    (*output)[kStatusRepairStateOffset] =
        static_cast<std::byte>(status.repair_state);
    WriteLittleEndian<std::uint32_t>(
        status.instrument_id, kStatusInstrumentOffset, *output);
    WriteLittleEndian<std::uint64_t>(
        status.stable_tail, kStatusStableTailOffset, *output);
    WriteLittleEndian<std::uint64_t>(
        status.repair_through_arrival_id,
        kStatusRepairThroughOffset, *output);
    return RealtimeWireCodecErrorV3::kNone;
}

RealtimeWireCodecErrorV3 DecodeStableStatusWireV3(
    std::span<const std::byte> input,
    InstrumentStableStatusWireV3* output) noexcept {
    if (output == nullptr) {
        return RealtimeWireCodecErrorV3::kNullOutput;
    }
    *output = {};
    if (input.size() != kRealtimeStableStatusWireSizeV3) {
        return RealtimeWireCodecErrorV3::kInvalidSize;
    }
    for (std::size_t index = 0U; index < kRealtimeWireMagicV3.size();
         ++index) {
        if (std::to_integer<std::uint8_t>(input[index]) !=
            kRealtimeWireMagicV3[index]) {
            return RealtimeWireCodecErrorV3::kInvalidMagic;
        }
    }
    const std::uint16_t major = ReadLittleEndian<std::uint16_t>(
        kStatusMajorOffset, input);
    const std::uint16_t minor = ReadLittleEndian<std::uint16_t>(
        kStatusMinorOffset, input);
    if (major != kRealtimeWireMajorV3 || minor > kRealtimeWireMinorV3) {
        return RealtimeWireCodecErrorV3::kIncompatibleVersion;
    }
    if (ReadLittleEndian<std::uint16_t>(
            kStatusReservedOffset, input) != 0U) {
        return RealtimeWireCodecErrorV3::kReservedNonzero;
    }
    const std::uint8_t dataset =
        std::to_integer<std::uint8_t>(input[kStatusDatasetOffset]);
    if (!ValidDataset(dataset)) {
        return RealtimeWireCodecErrorV3::kInvalidDataset;
    }
    const std::uint8_t repair_state = std::to_integer<std::uint8_t>(
        input[kStatusRepairStateOffset]);
    if (!ValidRepairState(repair_state)) {
        return RealtimeWireCodecErrorV3::kInvalidRepairState;
    }
    std::copy(kRealtimeWireMagicV3.begin(), kRealtimeWireMagicV3.end(),
              output->magic.begin());
    output->major = major;
    output->minor = minor;
    output->dataset = dataset;
    output->repair_state = repair_state;
    output->instrument_id = ReadLittleEndian<std::uint32_t>(
        kStatusInstrumentOffset, input);
    output->stable_tail = ReadLittleEndian<std::uint64_t>(
        kStatusStableTailOffset, input);
    output->repair_through_arrival_id =
        ReadLittleEndian<std::uint64_t>(
            kStatusRepairThroughOffset, input);
    return RealtimeWireCodecErrorV3::kNone;
}

}  // namespace l2flow::ipc
