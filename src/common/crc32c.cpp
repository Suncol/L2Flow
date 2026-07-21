#include "l2flow/common/crc32c.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace l2flow::common {
namespace {

// Reflected representation of the Castagnoli polynomial 0x1EDC6F41.
constexpr std::uint32_t kReflectedPolynomial = 0x82f63b78U;

constexpr std::array<std::uint32_t, 256U> MakeCrc32cTable() noexcept {
    std::array<std::uint32_t, 256U> table{};
    for (std::size_t index = 0; index < table.size(); ++index) {
        std::uint32_t value = static_cast<std::uint32_t>(index);
        for (unsigned int bit = 0U; bit < 8U; ++bit) {
            value = (value & 1U) != 0U
                        ? (value >> 1U) ^ kReflectedPolynomial
                        : value >> 1U;
        }
        table[index] = value;
    }
    return table;
}

constexpr std::array<std::uint32_t, 256U> kCrc32cTable =
    MakeCrc32cTable();

}  // namespace

void Crc32cState::Reset() noexcept {
    state_ = kCrc32cInitial;
}

void Crc32cState::Update(std::span<const std::byte> bytes) noexcept {
    std::uint32_t state = state_;
    for (const std::byte byte : bytes) {
        const std::uint32_t table_index =
            (state ^ std::to_integer<std::uint32_t>(byte)) & 0xffU;
        state =
            kCrc32cTable[static_cast<std::size_t>(table_index)] ^
            (state >> 8U);
    }
    state_ = state;
}

void Crc32cState::Update(
    std::span<const std::uint8_t> bytes) noexcept {
    Update(std::as_bytes(bytes));
}

void Crc32cState::Update(std::string_view bytes) noexcept {
    Update(std::as_bytes(
        std::span<const char>(bytes.data(), bytes.size())));
}

std::uint32_t Crc32cState::Finalize() const noexcept {
    return state_ ^ kCrc32cXorOut;
}

std::uint32_t ComputeCrc32c(
    std::span<const std::byte> bytes) noexcept {
    Crc32cState state;
    state.Update(bytes);
    return state.Finalize();
}

std::uint32_t ComputeCrc32c(
    std::span<const std::uint8_t> bytes) noexcept {
    Crc32cState state;
    state.Update(bytes);
    return state.Finalize();
}

std::uint32_t ComputeCrc32c(std::string_view bytes) noexcept {
    Crc32cState state;
    state.Update(bytes);
    return state.Finalize();
}

}  // namespace l2flow::common
