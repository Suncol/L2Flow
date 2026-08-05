#pragma once

#include "l2flow/common/identity128.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <type_traits>

namespace l2flow::ipc {

inline constexpr std::array<std::uint8_t, 4U> kRealtimeWireMagicV3{
    'L', '2', 'F', '3'};
inline constexpr std::uint16_t kRealtimeWireMajorV3 = 3U;
inline constexpr std::uint16_t kRealtimeWireMinorV3 = 0U;
inline constexpr std::size_t kRealtimeCursorWireSizeV3 = 32U;
inline constexpr std::size_t kRealtimeStableStatusWireSizeV3 = 32U;

enum class RealtimeDatasetV3 : std::uint8_t {
    kFastTick = 1U,
    kDerivedEvent = 2U,
    kKLine = 3U,
};

enum class RealtimeRepairStateWireV3 : std::uint8_t {
    kLive = 0U,
    kRepairRequired,
    kRebuilding,
    kCatchingUp,
    kSourceConflict,
    kUnrecoverable,
};

// V3 intentionally has no global sequence or generation field. Each cursor
// belongs to exactly one session, dataset and instrument.
struct FastTickCursorWireV3 final {
    l2flow::common::Identity128 session_id{};
    std::uint32_t instrument_id = 0U;
    std::uint32_t reserved = 0U;
    std::uint64_t next_arrival_row = 1U;
};

struct EventChangeCursorWireV3 final {
    l2flow::common::Identity128 session_id{};
    std::uint32_t instrument_id = 0U;
    std::uint32_t reserved = 0U;
    std::uint64_t next_change_sequence = 1U;
};

struct KLineChangeCursorWireV3 final {
    l2flow::common::Identity128 session_id{};
    std::uint32_t instrument_id = 0U;
    std::uint32_t reserved = 0U;
    std::uint64_t next_change_sequence = 1U;
};

// Every integer is fixed-width and little-endian on the serialized boundary.
// C++ enums are kept out of the structures so the ABI is language-neutral.
struct InstrumentStableStatusWireV3 final {
    std::array<std::uint8_t, 4U> magic = kRealtimeWireMagicV3;
    std::uint16_t major = kRealtimeWireMajorV3;
    std::uint16_t minor = kRealtimeWireMinorV3;
    std::uint8_t dataset =
        static_cast<std::uint8_t>(RealtimeDatasetV3::kFastTick);
    std::uint8_t repair_state =
        static_cast<std::uint8_t>(RealtimeRepairStateWireV3::kLive);
    std::uint16_t reserved = 0U;
    std::uint32_t instrument_id = 0U;
    // Cardinality of the immutable root (Event rows or KLine bars), not a
    // CDC cursor, generation, or cross-plane position.
    std::uint64_t stable_tail = 0U;
    std::uint64_t repair_through_arrival_id = 0U;
};

using RealtimeCursorBytesV3 =
    std::array<std::byte, kRealtimeCursorWireSizeV3>;
using RealtimeStableStatusBytesV3 =
    std::array<std::byte, kRealtimeStableStatusWireSizeV3>;

enum class RealtimeWireCodecErrorV3 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidSize,
    kReservedNonzero,
    kInvalidMagic,
    kIncompatibleVersion,
    kInvalidDataset,
    kInvalidRepairState,
};

[[nodiscard]] std::string_view RealtimeWireCodecErrorNameV3(
    RealtimeWireCodecErrorV3 error) noexcept;

// Native structure layout is never copied onto the wire. These functions
// encode every integer explicitly as little-endian bytes, so their output is
// independent of host endianness, padding and enum ABI.
[[nodiscard]] RealtimeWireCodecErrorV3 EncodeFastTickCursorWireV3(
    const FastTickCursorWireV3& cursor,
    RealtimeCursorBytesV3* output) noexcept;
[[nodiscard]] RealtimeWireCodecErrorV3 EncodeEventChangeCursorWireV3(
    const EventChangeCursorWireV3& cursor,
    RealtimeCursorBytesV3* output) noexcept;
[[nodiscard]] RealtimeWireCodecErrorV3 EncodeKLineChangeCursorWireV3(
    const KLineChangeCursorWireV3& cursor,
    RealtimeCursorBytesV3* output) noexcept;

[[nodiscard]] RealtimeWireCodecErrorV3 DecodeFastTickCursorWireV3(
    std::span<const std::byte> input,
    FastTickCursorWireV3* output) noexcept;
[[nodiscard]] RealtimeWireCodecErrorV3 DecodeEventChangeCursorWireV3(
    std::span<const std::byte> input,
    EventChangeCursorWireV3* output) noexcept;
[[nodiscard]] RealtimeWireCodecErrorV3 DecodeKLineChangeCursorWireV3(
    std::span<const std::byte> input,
    KLineChangeCursorWireV3* output) noexcept;

[[nodiscard]] RealtimeWireCodecErrorV3 EncodeStableStatusWireV3(
    const InstrumentStableStatusWireV3& status,
    RealtimeStableStatusBytesV3* output) noexcept;
[[nodiscard]] RealtimeWireCodecErrorV3 DecodeStableStatusWireV3(
    std::span<const std::byte> input,
    InstrumentStableStatusWireV3* output) noexcept;

static_assert(std::is_standard_layout_v<FastTickCursorWireV3>);
static_assert(std::is_trivially_copyable_v<FastTickCursorWireV3>);
static_assert(std::is_standard_layout_v<EventChangeCursorWireV3>);
static_assert(std::is_trivially_copyable_v<EventChangeCursorWireV3>);
static_assert(std::is_standard_layout_v<KLineChangeCursorWireV3>);
static_assert(std::is_trivially_copyable_v<KLineChangeCursorWireV3>);
static_assert(std::is_standard_layout_v<InstrumentStableStatusWireV3>);
static_assert(std::is_trivially_copyable_v<InstrumentStableStatusWireV3>);
// These checks detect accidental ABI drift for in-process users. The explicit
// codecs above, rather than these sizeof values, define the serialized ABI.
static_assert(sizeof(FastTickCursorWireV3) == kRealtimeCursorWireSizeV3);
static_assert(sizeof(EventChangeCursorWireV3) == kRealtimeCursorWireSizeV3);
static_assert(sizeof(KLineChangeCursorWireV3) == kRealtimeCursorWireSizeV3);
static_assert(
    sizeof(InstrumentStableStatusWireV3) ==
    kRealtimeStableStatusWireSizeV3);

}  // namespace l2flow::ipc
