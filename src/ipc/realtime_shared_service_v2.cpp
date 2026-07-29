#include "l2flow/ipc/realtime_shared_service_v2.h"

#include "l2flow/common/sha256.h"
#include "l2flow/ipc/realtime_wire_v2.h"
#include "l2flow/market/market_types_v1.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <span>
#include <string>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include <fcntl.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#ifndef F_SEAL_FUTURE_WRITE
#define F_SEAL_FUTURE_WRITE 0x0010
#endif

namespace l2flow::ipc {
namespace {

namespace common = l2flow::common;
namespace market = l2flow::market;
namespace realtime = l2flow::realtime;

constexpr std::uint64_t kPageBytes = 4096U;
constexpr std::size_t kHotPrefixAdvanceBudget = 64U;
constexpr std::size_t kControlPrefixAdvanceBudget = 4096U;

void SetSystemError(int* output, int value) noexcept {
    if (output != nullptr) {
        *output = value;
    }
}

bool CheckedAdd(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* output) noexcept {
    if (output == nullptr ||
        left > std::numeric_limits<std::uint64_t>::max() - right) {
        return false;
    }
    *output = left + right;
    return true;
}

bool CheckedMultiply(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* output) noexcept {
    if (output == nullptr ||
        (right != 0U &&
         left > std::numeric_limits<std::uint64_t>::max() / right)) {
        return false;
    }
    *output = left * right;
    return true;
}

bool AlignUp(
    std::uint64_t value,
    std::uint64_t alignment,
    std::uint64_t* output) noexcept {
    if (output == nullptr || alignment == 0U ||
        (alignment & (alignment - 1U)) != 0U) {
        return false;
    }
    const std::uint64_t remainder = value & (alignment - 1U);
    if (remainder == 0U) {
        *output = value;
        return true;
    }
    return CheckedAdd(value, alignment - remainder, output);
}

template <typename Integer>
std::atomic_ref<Integer> Atomic(Integer& value) noexcept {
    static_assert(std::is_integral_v<Integer>);
    return std::atomic_ref<Integer>(value);
}

template <typename Integer>
std::atomic_ref<Integer> Atomic(const Integer& value) noexcept {
    static_assert(std::is_integral_v<Integer>);
    return std::atomic_ref<Integer>(const_cast<Integer&>(value));
}

bool StateAcceptsPublication(std::uint32_t state) noexcept {
    return state ==
               static_cast<std::uint32_t>(
                   RealtimeServerStateV2::kInitializing) ||
           state ==
               static_cast<std::uint32_t>(
                   RealtimeServerStateV2::kActive) ||
           state ==
               static_cast<std::uint32_t>(
                   RealtimeServerStateV2::kDraining);
}

template <typename Slot, typename Payload>
bool PublishSlot(Slot* slot, const Payload& payload) noexcept {
    if (slot == nullptr ||
        sizeof(Payload) > sizeof(slot->payload_words)) {
        return false;
    }
    std::atomic_ref<std::uint64_t> tag = Atomic(slot->publish_tag);
    std::uint64_t stable = tag.load(std::memory_order_acquire);
    if ((stable & 1U) != 0U ||
        stable > std::numeric_limits<std::uint64_t>::max() - 2U) {
        return false;
    }
    if (!tag.compare_exchange_strong(
            stable,
            stable + 1U,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        return false;
    }
    constexpr std::size_t word_count =
        (sizeof(Payload) + sizeof(std::uint64_t) - 1U) /
        sizeof(std::uint64_t);
    static_assert(
        word_count <=
        std::tuple_size_v<decltype(slot->payload_words)>);
    std::array<std::uint64_t, word_count> words{};
    std::memcpy(words.data(), &payload, sizeof(payload));
    for (std::size_t index = 0U; index < words.size(); ++index) {
        Atomic(slot->payload_words[index])
            .store(words[index], std::memory_order_relaxed);
    }
    tag.store(stable + 2U, std::memory_order_release);
    return true;
}

bool TickSlotContainsSequence(
    const RealtimeWireTickSlotV2& slot,
    std::uint64_t expected_sequence) noexcept {
    constexpr std::size_t sequence_word =
        offsetof(
            RealtimeWireCommonRecordV2,
            tick_stream_sequence) /
        sizeof(std::uint64_t);
    static_assert(sequence_word < 56U);
    for (std::size_t attempt = 0U; attempt < 3U; ++attempt) {
        const std::uint64_t begin =
            Atomic(slot.publish_tag).load(std::memory_order_acquire);
        if (begin == 0U || (begin & 1U) != 0U) {
            return false;
        }
        const std::uint64_t sequence =
            Atomic(slot.payload_words[sequence_word])
                .load(std::memory_order_relaxed);
        std::atomic_thread_fence(std::memory_order_acq_rel);
        const std::uint64_t end =
            Atomic(slot.publish_tag).load(std::memory_order_acquire);
        if (begin == end && (end & 1U) == 0U) {
            return sequence == expected_sequence;
        }
    }
    return false;
}

void AtomicMaximum(
    std::uint64_t* destination,
    std::uint64_t value) noexcept {
    std::atomic_ref<std::uint64_t> target = Atomic(*destination);
    std::uint64_t current = target.load(std::memory_order_acquire);
    while (current < value &&
           !target.compare_exchange_weak(
               current,
               value,
               std::memory_order_release,
               std::memory_order_acquire)) {
    }
}

struct LayoutRegion final {
    RealtimeRegionKindV2 kind = RealtimeRegionKindV2::kReserved8;
    std::uint64_t offset = 0U;
    std::uint64_t length = 0U;
    std::uint64_t stride = 0U;
    std::uint64_t count = 0U;
    std::uint64_t capacity = 0U;
    std::uint32_t alignment = 0U;
};

bool AppendRegion(
    RealtimeRegionKindV2 kind,
    std::uint64_t stride,
    std::uint64_t count,
    std::uint32_t alignment,
    std::uint64_t* cursor,
    LayoutRegion* output) noexcept {
    if (cursor == nullptr || output == nullptr || stride == 0U ||
        alignment == 0U) {
        return false;
    }
    std::uint64_t offset = 0U;
    std::uint64_t length = 0U;
    std::uint64_t end = 0U;
    if (!AlignUp(
            *cursor,
            static_cast<std::uint64_t>(alignment),
            &offset) ||
        !CheckedMultiply(stride, count, &length) ||
        !CheckedAdd(offset, length, &end)) {
        return false;
    }
    output->kind = kind;
    output->offset = offset;
    output->length = length;
    output->stride = stride;
    output->count = count;
    output->capacity = count;
    output->alignment = alignment;
    *cursor = end;
    return true;
}

bool ValidSocketPath(const std::filesystem::path& path) noexcept {
    try {
        if (!path.is_absolute() || path.filename().empty() ||
            path.filename() == "." || path.filename() == "..") {
            return false;
        }
        const std::string native = path.string();
        sockaddr_un address{};
        return native.size() < sizeof(address.sun_path);
    } catch (...) {
        return false;
    }
}

bool MonotonicNowNanoseconds(std::uint64_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    timespec now{};
    if (::clock_gettime(CLOCK_MONOTONIC, &now) != 0 ||
        now.tv_sec < 0 || now.tv_nsec < 0) {
        return false;
    }
    constexpr std::uint64_t nanoseconds_per_second =
        1'000'000'000ULL;
    const std::uint64_t seconds =
        static_cast<std::uint64_t>(now.tv_sec);
    const std::uint64_t nanoseconds =
        static_cast<std::uint64_t>(now.tv_nsec);
    if (seconds >
        (std::numeric_limits<std::uint64_t>::max() - nanoseconds) /
            nanoseconds_per_second) {
        return false;
    }
    *output = seconds * nanoseconds_per_second + nanoseconds;
    return true;
}

void CloseDescriptor(int* descriptor) noexcept {
    if (descriptor != nullptr && *descriptor >= 0) {
        static_cast<void>(::close(*descriptor));
        *descriptor = -1;
    }
}

bool SendPacket(
    int socket_fd,
    const void* data,
    std::size_t bytes,
    int attached_fd) noexcept {
    if (socket_fd < 0 || data == nullptr || bytes == 0U) {
        return false;
    }
    iovec vector{};
    vector.iov_base = const_cast<void*>(data);
    vector.iov_len = bytes;
    msghdr message{};
    message.msg_iov = &vector;
    message.msg_iovlen = 1U;
    std::array<std::byte, CMSG_SPACE(sizeof(int))> control{};
    if (attached_fd >= 0) {
        message.msg_control = control.data();
        message.msg_controllen = control.size();
        cmsghdr* const rights = CMSG_FIRSTHDR(&message);
        if (rights == nullptr) {
            return false;
        }
        rights->cmsg_level = SOL_SOCKET;
        rights->cmsg_type = SCM_RIGHTS;
        rights->cmsg_len = CMSG_LEN(sizeof(int));
        std::memcpy(
            CMSG_DATA(rights), &attached_fd, sizeof(attached_fd));
    }
    for (;;) {
        const ssize_t sent =
            ::sendmsg(socket_fd, &message, MSG_NOSIGNAL);
        if (sent == static_cast<ssize_t>(bytes)) {
            return true;
        }
        if (sent >= 0) {
            return false;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            return false;
        }
        pollfd descriptor{};
        descriptor.fd = socket_fd;
        descriptor.events = static_cast<short>(POLLOUT);
        int ready = -1;
        do {
            ready = ::poll(&descriptor, 1U, 250);
        } while (ready < 0 && errno == EINTR);
        if (ready <= 0 ||
            (descriptor.revents &
             (POLLERR | POLLHUP | POLLNVAL)) != 0 ||
            (descriptor.revents & POLLOUT) == 0) {
            return false;
        }
    }
}

RealtimeWireDecimalV2 Decimal(
    const market::DecimalValueV1& value) noexcept {
    RealtimeWireDecimalV2 result{};
    result.raw = value.raw;
    result.normalized_p6 = value.normalized_p6;
    result.scale = value.scale;
    result.valid = value.valid ? 1U : 0U;
    result.is_null = value.is_null ? 1U : 0U;
    return result;
}

RealtimeWireQuantityV2 Quantity(
    const market::QuantityValueV1& value) noexcept {
    RealtimeWireQuantityV2 result{};
    result.raw = value.raw;
    result.scale = value.scale;
    result.valid = value.valid ? 1U : 0U;
    result.is_null = value.is_null ? 1U : 0U;
    return result;
}

bool Common(
    const market::RealtimeHistoryRecordV1& record,
    std::size_t ordinal,
    const market::DecodedMarketCommonV1& source,
    std::uint32_t record_bytes,
    RealtimeWireCommonRecordV2* output) noexcept {
    if (output == nullptr ||
        ordinal >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max()) ||
        source.instrument_id != record.instrument_id() ||
        source.ordinal != ordinal ||
        source.kind != record.kind() ||
        source.origin.source_stream_id !=
            record.source_stream_id() ||
        source.origin.source_sequence != record.source_sequence() ||
        static_cast<std::uint8_t>(source.quantity_unit) >
            static_cast<std::uint8_t>(
                market::QuantityUnitV1::kIndexUnit) ||
        static_cast<std::uint8_t>(source.security_type) >
            static_cast<std::uint8_t>(
                market::SecurityTypeV1::kOption) ||
        static_cast<std::uint8_t>(source.asset_scope) >
            static_cast<std::uint8_t>(
                market::AssetScopeV1::kOutsideDocumentedCore)) {
        return false;
    }
    const market::MarketV1 expected_market =
        record.kind() ==
                    market::MarketEventKindV1::kShanghaiSnapshot ||
                record.kind() ==
                    market::MarketEventKindV1::kShanghaiTick
            ? market::MarketV1::kShanghai
            : market::MarketV1::kShenzhen;
    if (source.market != expected_market) {
        return false;
    }
    RealtimeWireCommonRecordV2 result{};
    result.record_bytes = record_bytes;
    result.instrument_id = record.instrument_id();
    result.ordinal = static_cast<std::uint32_t>(ordinal);
    result.source_sequence = record.source_sequence();
    result.ingress_sequence = record.ingress_sequence();
    result.tick_stream_sequence = record.tick_stream_sequence();
    result.vendor_sequence_id = source.origin.vendor_sequence_id;
    result.event_time_unix_ns = record.event_time_ns();
    result.recv_realtime_ns = record.recv_realtime_ns();
    result.recv_monotonic_ns = record.recv_monotonic_ns();
    result.exchange_time_ns_since_midnight =
        source.exchange_time.nanoseconds_since_midnight;
    result.quality_flags = source.quality_flags;
    result.market_notices = source.market_notices;
    result.source_stream_id = record.source_stream_id();
    result.trade_date = source.origin.trade_date;
    result.vendor_local_time_raw =
        source.origin.vendor_local_time_raw;
    result.source_slot = record.source_slot();
    result.event_kind =
        static_cast<std::uint8_t>(record.kind());
    result.market = static_cast<std::uint8_t>(source.market);
    result.quantity_unit =
        static_cast<std::uint8_t>(source.quantity_unit);
    result.security_type =
        static_cast<std::uint8_t>(source.security_type);
    result.asset_scope =
        static_cast<std::uint8_t>(source.asset_scope);
    *output = result;
    return true;
}

void Book(
    const market::SnapshotBookV1& source,
    RealtimeWireSnapshotPayloadV2* output) noexcept {
    output->actual_bid_depth = source.actual_bid_depth;
    output->actual_ask_depth = source.actual_ask_depth;
    output->retained_bid_depth = source.retained_bid_depth;
    output->retained_ask_depth = source.retained_ask_depth;
    for (std::size_t index = 0U; index < source.bids.size(); ++index) {
        output->bids[index].price = Decimal(source.bids[index].price);
        output->bids[index].quantity =
            Quantity(source.bids[index].quantity);
        output->bids[index].order_count =
            source.bids[index].order_count;
        output->bids[index].order_count_valid =
            source.bids[index].order_count_valid ? 1U : 0U;
        output->asks[index].price = Decimal(source.asks[index].price);
        output->asks[index].quantity =
            Quantity(source.asks[index].quantity);
        output->asks[index].order_count =
            source.asks[index].order_count;
        output->asks[index].order_count_valid =
            source.asks[index].order_count_valid ? 1U : 0U;
    }
    output->bid1_queue.total_order_count =
        source.bid1_queue.total_order_count;
    output->bid1_queue.actual_revealed_count =
        source.bid1_queue.actual_revealed_count;
    output->bid1_queue.retained_count =
        source.bid1_queue.retained_count;
    output->ask1_queue.total_order_count =
        source.ask1_queue.total_order_count;
    output->ask1_queue.actual_revealed_count =
        source.ask1_queue.actual_revealed_count;
    output->ask1_queue.retained_count =
        source.ask1_queue.retained_count;
    for (std::size_t index = 0U;
         index < source.bid1_queue.quantities.size();
         ++index) {
        output->bid1_queue_quantities[index] =
            Quantity(source.bid1_queue.quantities[index]);
        output->ask1_queue_quantities[index] =
            Quantity(source.ask1_queue.quantities[index]);
    }
}

bool BookRepresentable(
    const market::SnapshotBookV1& book) noexcept {
    return book.retained_bid_depth <= book.bids.size() &&
           book.retained_ask_depth <= book.asks.size() &&
           book.bid1_queue.retained_count <=
               book.bid1_queue.quantities.size() &&
           book.ask1_queue.retained_count <=
               book.ask1_queue.quantities.size();
}

bool TickFieldsRepresentable(
    const market::TickFieldsV1& fields) noexcept {
    return static_cast<std::uint8_t>(fields.action) <=
               static_cast<std::uint8_t>(
                   market::TickActionV1::kStatus) &&
           static_cast<std::uint8_t>(fields.side) <=
               static_cast<std::uint8_t>(
                   market::SideV1::kLend) &&
           static_cast<std::uint8_t>(fields.order_type) <=
               static_cast<std::uint8_t>(
                   market::OrderTypeV1::kSameSideBest) &&
           static_cast<std::uint8_t>(fields.aggressor) <=
               static_cast<std::uint8_t>(
                   market::AggressorV1::kNeutral) &&
           static_cast<std::uint8_t>(fields.phase) <=
               static_cast<std::uint8_t>(
                   market::TradingPhaseV1::kEnd);
}

void TickFields(
    const market::TickFieldsV1& source,
    RealtimeWireTickPayloadV2* output) noexcept {
    output->validity_bitmap = source.validity_bitmap;
    output->action = static_cast<std::uint8_t>(source.action);
    output->side = static_cast<std::uint8_t>(source.side);
    output->order_type =
        static_cast<std::uint8_t>(source.order_type);
    output->aggressor =
        static_cast<std::uint8_t>(source.aggressor);
    output->phase = static_cast<std::uint8_t>(source.phase);
    output->price = Decimal(source.price);
    output->quantity = Quantity(source.quantity);
    output->trade_amount = Decimal(source.trade_amount);
    output->matched_quantity = Quantity(source.matched_quantity);
    output->primary_order_id = source.primary_order_id;
    output->buy_order_id = source.buy_order_id;
    output->sell_order_id = source.sell_order_id;
}

bool CopyRaw(
    const std::string& source,
    std::array<std::uint8_t, 32U>* destination,
    std::uint8_t* length,
    std::uint32_t omission_flag,
    std::uint32_t* projection_flags) noexcept {
    if (destination == nullptr || length == nullptr ||
        projection_flags == nullptr) {
        return false;
    }
    if (source.size() > destination->size()) {
        destination->fill(0U);
        *length = 0U;
        *projection_flags |= omission_flag;
        return true;
    }
    destination->fill(0U);
    std::transform(
        source.begin(),
        source.end(),
        destination->begin(),
        [](char value) noexcept {
            return static_cast<std::uint8_t>(
                static_cast<unsigned char>(value));
        });
    *length = static_cast<std::uint8_t>(source.size());
    return true;
}

bool ProjectTick(
    const market::RealtimeHistoryRecordV1& record,
    std::size_t ordinal,
    RealtimeWireTickPayloadV2* output) noexcept {
    if (output == nullptr || !market::IsTickEventKindV1(record.kind()) ||
        record.tick_stream_sequence() == 0U) {
        return false;
    }
    RealtimeWireTickPayloadV2 projected{};
    std::uint32_t projected_flags = 0U;
    const bool ok = std::visit(
        [&](const auto* event) noexcept -> bool {
            using Pointer = std::decay_t<decltype(event)>;
            using Event = std::remove_cv_t<
                std::remove_pointer_t<Pointer>>;
            if (event == nullptr) {
                return false;
            }
            if constexpr (
                std::is_same_v<Event, market::ShanghaiTickV1>) {
                if (record.kind() !=
                        market::MarketEventKindV1::kShanghaiTick ||
                    !TickFieldsRepresentable(event->fields) ||
                    !Common(
                        record,
                        ordinal,
                        event->common,
                        static_cast<std::uint32_t>(
                            sizeof(projected)),
                        &projected.common) ||
                    !CopyRaw(
                        event->raw_type,
                        &projected.raw_type,
                        &projected.raw_type_length,
                        kRealtimeWireTickRawTypeOmittedV2,
                        &projected_flags) ||
                    !CopyRaw(
                        event->raw_tick_flag,
                        &projected.raw_tick_flag,
                        &projected.raw_tick_flag_length,
                        kRealtimeWireTickRawTickFlagOmittedV2,
                        &projected_flags)) {
                    return false;
                }
                projected.channel =
                    static_cast<std::int64_t>(event->channel);
                projected.native_event_sequence =
                    event->business_index;
                TickFields(event->fields, &projected);
                return true;
            } else if constexpr (
                std::is_same_v<Event, market::ShenzhenOrderV1>) {
                if (record.kind() !=
                        market::MarketEventKindV1::kShenzhenOrder ||
                    !TickFieldsRepresentable(event->fields) ||
                    !Common(
                        record,
                        ordinal,
                        event->common,
                        static_cast<std::uint32_t>(
                            sizeof(projected)),
                        &projected.common)) {
                    return false;
                }
                projected.channel =
                    static_cast<std::int64_t>(event->channel);
                projected.native_event_sequence =
                    event->application_sequence;
                projected.source_raw_code_1 = event->raw_side;
                projected.source_raw_code_2 = event->raw_order_type;
                TickFields(event->fields, &projected);
                return true;
            } else if constexpr (
                std::is_same_v<
                    Event,
                    market::ShenzhenTransactionV1>) {
                if (record.kind() !=
                        market::MarketEventKindV1::
                            kShenzhenTransaction ||
                    !TickFieldsRepresentable(event->fields) ||
                    !Common(
                        record,
                        ordinal,
                        event->common,
                        static_cast<std::uint32_t>(
                            sizeof(projected)),
                        &projected.common)) {
                    return false;
                }
                projected.channel =
                    static_cast<std::int64_t>(event->channel);
                projected.native_event_sequence =
                    event->application_sequence;
                projected.source_raw_code_1 =
                    event->raw_execution_type;
                TickFields(event->fields, &projected);
                return true;
            } else {
                return false;
            }
        },
        record.event());
    if (!ok) {
        return false;
    }
    projected.projection_flags = projected_flags;
    *output = projected;
    return true;
}

bool ProjectSnapshot(
    const market::RealtimeHistoryRecordV1& record,
    std::size_t ordinal,
    RealtimeWireSnapshotPayloadV2* output) noexcept {
    if (output == nullptr ||
        !market::IsSnapshotEventKindV1(record.kind()) ||
        record.tick_stream_sequence() != 0U) {
        return false;
    }
    RealtimeWireSnapshotPayloadV2 projected{};
    const bool ok = std::visit(
        [&](const auto* event) noexcept -> bool {
            using Pointer = std::decay_t<decltype(event)>;
            using Event = std::remove_cv_t<
                std::remove_pointer_t<Pointer>>;
            if (event == nullptr) {
                return false;
            }
            if constexpr (
                std::is_same_v<Event, market::ShanghaiSnapshotV1>) {
                if (record.kind() !=
                        market::MarketEventKindV1::kShanghaiSnapshot ||
                    !BookRepresentable(event->book) ||
                    !Common(
                        record,
                        ordinal,
                        event->common,
                        static_cast<std::uint32_t>(
                            sizeof(projected)),
                        &projected.common)) {
                    return false;
                }
                projected.trade_count =
                    static_cast<std::int64_t>(event->trade_count);
                projected.image_status = event->image_status;
                projected.pre_close_price =
                    Decimal(event->pre_close_price);
                projected.open_price = Decimal(event->open_price);
                projected.high_price = Decimal(event->high_price);
                projected.low_price = Decimal(event->low_price);
                projected.last_price = Decimal(event->last_price);
                projected.close_price = Decimal(event->close_price);
                projected.trade_volume =
                    Quantity(event->trade_volume);
                projected.turnover = Decimal(event->turnover);
                projected.total_bid_quantity =
                    Quantity(event->total_bid_volume);
                projected.weighted_average_bid_price =
                    Decimal(event->weighted_average_bid_price);
                projected.total_ask_quantity =
                    Quantity(event->total_ask_volume);
                projected.weighted_average_ask_price =
                    Decimal(event->weighted_average_ask_price);
                projected.iopv = Decimal(event->iopv);
                Book(event->book, &projected);
                return true;
            } else if constexpr (
                std::is_same_v<Event, market::ShenzhenSnapshotV1>) {
                if (record.kind() !=
                        market::MarketEventKindV1::kShenzhenSnapshot ||
                    !BookRepresentable(event->book) ||
                    !Common(
                        record,
                        ordinal,
                        event->common,
                        static_cast<std::uint32_t>(
                            sizeof(projected)),
                        &projected.common)) {
                    return false;
                }
                projected.trade_count = event->trade_count;
                projected.channel = event->channel;
                projected.pre_close_price =
                    Decimal(event->pre_close_price);
                projected.open_price = Decimal(event->open_price);
                projected.high_price = Decimal(event->high_price);
                projected.low_price = Decimal(event->low_price);
                projected.last_price = Decimal(event->last_price);
                projected.trade_volume = Quantity(event->volume);
                projected.turnover = Decimal(event->turnover);
                projected.total_bid_quantity =
                    Quantity(event->total_bid_quantity);
                projected.weighted_average_bid_price =
                    Decimal(event->weighted_average_bid_price);
                projected.total_ask_quantity =
                    Quantity(event->total_ask_quantity);
                projected.weighted_average_ask_price =
                    Decimal(event->weighted_average_ask_price);
                projected.high_limit_price =
                    Decimal(event->high_limit_price);
                projected.low_limit_price =
                    Decimal(event->low_limit_price);
                projected.iopv = Decimal(event->iopv);
                projected.open_interest =
                    Quantity(event->open_interest);
                Book(event->book, &projected);
                return true;
            } else {
                return false;
            }
        },
        record.event());
    if (!ok) {
        return false;
    }
    *output = projected;
    return true;
}

bool ProjectKLine(
    std::uint64_t generation,
    const market::KLineBarV1& bar,
    RealtimeWireKLinePayloadV2* output) noexcept {
    if (output == nullptr || generation == 0U ||
        bar.instrument_id == 0U || bar.window_id == 0U ||
        static_cast<std::uint8_t>(bar.quantity_unit) >
            static_cast<std::uint8_t>(
                market::QuantityUnitV1::kIndexUnit)) {
        return false;
    }
    RealtimeWireKLinePayloadV2 projected{};
    projected.generation = generation;
    projected.trade_date = bar.trade_date;
    projected.instrument_id = bar.instrument_id;
    projected.window_id = bar.window_id;
    projected.window_duration_ns = bar.window_duration_ns;
    projected.window_start_ns_since_midnight =
        bar.window_start_ns_since_midnight;
    projected.window_end_ns_since_midnight =
        bar.window_end_ns_since_midnight;
    projected.window_start_unix_ns = bar.window_start_unix_ns;
    projected.window_end_unix_ns = bar.window_end_unix_ns;
    projected.open_price_p6 = bar.open_price_p6;
    projected.high_price_p6 = bar.high_price_p6;
    projected.low_price_p6 = bar.low_price_p6;
    projected.close_price_p6 = bar.close_price_p6;
    projected.volume_raw = bar.volume_raw;
    projected.trade_count = bar.trade_count;
    projected.revision = bar.revision;
    projected.first_event_time_ns_since_midnight =
        bar.first_trade.event_time_ns_since_midnight;
    projected.first_event_sequence =
        bar.first_trade.event_sequence;
    projected.first_source_sequence =
        bar.first_trade.source_sequence;
    projected.first_ingress_sequence =
        bar.first_trade.ingress_sequence;
    projected.last_event_time_ns_since_midnight =
        bar.last_trade.event_time_ns_since_midnight;
    projected.last_event_sequence = bar.last_trade.event_sequence;
    projected.last_source_sequence = bar.last_trade.source_sequence;
    projected.last_ingress_sequence = bar.last_trade.ingress_sequence;
    projected.volume_scale = bar.volume_scale;
    projected.quantity_unit =
        static_cast<std::uint8_t>(bar.quantity_unit);
    projected.present = 1U;
    *output = projected;
    return true;
}

bool SnapshotFactorEligible(
    const RealtimeWireSnapshotPayloadV2& snapshot) noexcept {
    return snapshot.last_price.valid != 0U &&
           snapshot.last_price.is_null == 0U &&
           snapshot.last_price.normalized_p6 > 0;
}

bool DigestNonzero(const common::Sha256Digest& digest) noexcept {
    return std::any_of(
        digest.begin(),
        digest.end(),
        [](std::byte value) noexcept {
            return value != std::byte{0U};
        });
}

void StoreDigest(
    RealtimeWireDigest256V2* destination,
    const common::Sha256Digest& source) noexcept {
    std::array<std::uint64_t, 4U> words{};
    std::memcpy(words.data(), source.data(), source.size());
    for (std::size_t index = 0U; index < words.size(); ++index) {
        Atomic(destination->words[index])
            .store(words[index], std::memory_order_relaxed);
    }
}

bool HashU16(
    common::Sha256Hasher* hasher,
    std::uint16_t value) noexcept {
    return hasher != nullptr &&
           hasher->Update(std::as_bytes(std::span(&value, 1U)));
}

bool HashU32(
    common::Sha256Hasher* hasher,
    std::uint32_t value) noexcept {
    return hasher != nullptr &&
           hasher->Update(std::as_bytes(std::span(&value, 1U)));
}

bool HashU64(
    common::Sha256Hasher* hasher,
    std::uint64_t value) noexcept {
    return hasher != nullptr &&
           hasher->Update(std::as_bytes(std::span(&value, 1U)));
}

bool ComputeLayoutDigest(
    const RealtimeSharedServiceConfigV2& config,
    std::span<const LayoutRegion, kRealtimeWireRegionCountV2> regions,
    std::uint64_t mapping_bytes,
    common::Sha256Digest* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    common::Sha256Hasher hasher;
    constexpr std::string_view domain =
        "l2flow.realtime-wire-v2.layout.v1";
    if (!hasher.Update(std::as_bytes(std::span(
            domain.data(), domain.size()))) ||
        !HashU16(&hasher, kRealtimeWireMajorV2) ||
        !HashU16(&hasher, kRealtimeWireMinorV2) ||
        !HashU32(
            &hasher,
            static_cast<std::uint32_t>(config.directory->capacity())) ||
        !HashU32(
            &hasher,
            static_cast<std::uint32_t>(
                config.kline_windows.size())) ||
        !HashU64(&hasher, config.tick_ring_capacity) ||
        !HashU64(&hasher, config.key_arena_bytes) ||
        !HashU64(&hasher, mapping_bytes)) {
        return false;
    }
    for (const LayoutRegion& region : regions) {
        if (!HashU32(
                &hasher,
                static_cast<std::uint32_t>(region.kind)) ||
            !HashU64(&hasher, region.offset) ||
            !HashU64(&hasher, region.length) ||
            !HashU64(&hasher, region.stride) ||
            !HashU64(&hasher, region.count) ||
            !HashU32(&hasher, region.alignment)) {
            return false;
        }
    }
    for (const market::KLineWindowSpecV1& window :
         config.kline_windows) {
        if (!HashU32(&hasher, window.window_id) ||
            !HashU64(&hasher, window.duration_ns)) {
            return false;
        }
    }
    common::Sha256Digest digest{};
    if (!hasher.Finalize(&digest) || !DigestNonzero(digest)) {
        return false;
    }
    *output = digest;
    return true;
}

}  // namespace

std::string_view RealtimeSharedServiceCreateErrorNameV2(
    RealtimeSharedServiceCreateErrorV2 error) noexcept {
    switch (error) {
        case RealtimeSharedServiceCreateErrorV2::kNone:
            return "none";
        case RealtimeSharedServiceCreateErrorV2::kNullOutput:
            return "null_output";
        case RealtimeSharedServiceCreateErrorV2::kInvalidConfiguration:
            return "invalid_configuration";
        case RealtimeSharedServiceCreateErrorV2::kDirectoryNotEmpty:
            return "directory_not_empty";
        case RealtimeSharedServiceCreateErrorV2::kLayoutOverflow:
            return "layout_overflow";
        case RealtimeSharedServiceCreateErrorV2::kMappingCreateFailed:
            return "mapping_create_failed";
        case RealtimeSharedServiceCreateErrorV2::kReadOnlyHandleFailed:
            return "read_only_handle_failed";
        case RealtimeSharedServiceCreateErrorV2::kSealFailed:
            return "seal_failed";
        case RealtimeSharedServiceCreateErrorV2::kSocketCreateFailed:
            return "socket_create_failed";
        case RealtimeSharedServiceCreateErrorV2::kSocketPathExists:
            return "socket_path_exists";
        case RealtimeSharedServiceCreateErrorV2::kSocketBindFailed:
            return "socket_bind_failed";
        case RealtimeSharedServiceCreateErrorV2::kResourceExhausted:
            return "resource_exhausted";
        case RealtimeSharedServiceCreateErrorV2::kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

class RealtimeSharedMarketServiceV2::Impl final {
public:
    explicit Impl(RealtimeSharedServiceConfigV2 config)
        : config_(std::move(config)) {}

    ~Impl() {
        StopControl();
        SafeUnlinkSocket();
        CloseDescriptor(&listener_fd_);
        CloseDescriptor(&stop_event_fd_);
        CloseDescriptor(&prefix_event_fd_);
        CloseDescriptor(&read_only_fd_);
        if (mapping_ != MAP_FAILED) {
            static_cast<void>(::munmap(
                mapping_, static_cast<std::size_t>(mapping_bytes_)));
        }
        CloseDescriptor(&memfd_);
    }

    [[nodiscard]] RealtimeSharedServiceCreateErrorV2 Initialize(
        int* system_error_number) noexcept {
        SetSystemError(system_error_number, 0);
        if constexpr (std::endian::native != std::endian::little) {
            return RealtimeSharedServiceCreateErrorV2::
                kInvalidConfiguration;
        }
        if (config_.directory == nullptr ||
            common::IsZeroIdentity(config_.run_id) ||
            config_.session_epoch == 0U ||
            config_.directory->session_epoch() !=
                config_.session_epoch ||
            config_.trade_date == 0U ||
            config_.directory->capacity() == 0U ||
            config_.directory->capacity() >=
                static_cast<std::size_t>(
                    std::numeric_limits<std::uint32_t>::max()) ||
            config_.tick_ring_capacity == 0U ||
            config_.key_arena_bytes == 0U ||
            config_.maximum_mapping_bytes <
                sizeof(RealtimeWireHeaderV2) ||
            !ValidSocketPath(config_.control_socket_path) ||
            config_.kline_windows.size() >
                market::kKLineMaximumWindowsV1) {
            return RealtimeSharedServiceCreateErrorV2::
                kInvalidConfiguration;
        }
        for (std::size_t index = 0U;
             index < config_.kline_windows.size();
             ++index) {
            if (config_.kline_windows[index].window_id == 0U ||
                config_.kline_windows[index].duration_ns == 0U) {
                return RealtimeSharedServiceCreateErrorV2::
                    kInvalidConfiguration;
            }
            for (std::size_t prior = 0U; prior < index; ++prior) {
                if (config_.kline_windows[prior].window_id ==
                    config_.kline_windows[index].window_id) {
                    return RealtimeSharedServiceCreateErrorV2::
                        kInvalidConfiguration;
                }
            }
        }
        std::sort(
            config_.kline_windows.begin(),
            config_.kline_windows.end(),
            [](const market::KLineWindowSpecV1& left,
               const market::KLineWindowSpecV1& right) noexcept {
                return left.window_id < right.window_id;
            });

        std::shared_ptr<
            const market::ObservedInstrumentCatalogSnapshotV2>
            initial_catalog;
        if (config_.directory->AcquireSnapshot(&initial_catalog) !=
                market::ObservedInstrumentDirectoryErrorV2::kNone ||
            initial_catalog == nullptr ||
            initial_catalog->session_epoch() !=
                config_.session_epoch ||
            initial_catalog->capacity() !=
                config_.directory->capacity() ||
            initial_catalog->catalog_scope() !=
                market::ObservedInstrumentCatalogScopeV2::
                    kObservedOnly ||
            initial_catalog->coverage_complete()) {
            return RealtimeSharedServiceCreateErrorV2::
                kInvalidConfiguration;
        }
        if (initial_catalog->bound_count() != 0U ||
            initial_catalog->available_count() != 0U ||
            initial_catalog->snapshot_available_count() != 0U ||
            initial_catalog->tick_available_count() != 0U ||
            initial_catalog->factor_eligible_count() != 0U) {
            return RealtimeSharedServiceCreateErrorV2::
                kDirectoryNotEmpty;
        }
        initial_catalog_generation_ =
            initial_catalog->catalog_generation();
        initial_data_state_generation_ =
            initial_catalog->data_state_generation();
        initial_catalog_digest_ =
            initial_catalog->catalog_digest();

        const std::uint64_t capacity =
            static_cast<std::uint64_t>(
                config_.directory->capacity());
        const std::uint64_t window_count =
            static_cast<std::uint64_t>(
                config_.kline_windows.size());
        std::uint64_t logical_kline_count = 0U;
        std::uint64_t physical_kline_count = 0U;
        if (!CheckedMultiply(
                capacity,
                window_count,
                &logical_kline_count) ||
            !CheckedMultiply(
                logical_kline_count,
                kRealtimeKLineTableCountV2,
                &physical_kline_count)) {
            return RealtimeSharedServiceCreateErrorV2::
                kLayoutOverflow;
        }

        std::uint64_t cursor = sizeof(RealtimeWireHeaderV2);
        if (!AppendRegion(
                RealtimeRegionKindV2::kInstrumentRows,
                sizeof(RealtimeWireInstrumentV2),
                capacity,
                alignof(RealtimeWireInstrumentV2),
                &cursor,
                &regions_[0U]) ||
            !AppendRegion(
                RealtimeRegionKindV2::kInstrumentKeyArena,
                1U,
                config_.key_arena_bytes,
                1U,
                &cursor,
                &regions_[1U]) ||
            !AppendRegion(
                RealtimeRegionKindV2::kKLineWindows,
                sizeof(RealtimeWireKLineWindowV2),
                window_count,
                alignof(RealtimeWireKLineWindowV2),
                &cursor,
                &regions_[2U]) ||
            !AppendRegion(
                RealtimeRegionKindV2::kLatestSnapshots,
                sizeof(RealtimeWireSnapshotSlotV2),
                capacity,
                alignof(RealtimeWireSnapshotSlotV2),
                &cursor,
                &regions_[3U]) ||
            !AppendRegion(
                RealtimeRegionKindV2::kLatestTicks,
                sizeof(RealtimeWireTickSlotV2),
                capacity,
                alignof(RealtimeWireTickSlotV2),
                &cursor,
                &regions_[4U]) ||
            !AppendRegion(
                RealtimeRegionKindV2::kLatestKLines,
                sizeof(RealtimeWireKLineSlotV2),
                physical_kline_count,
                alignof(RealtimeWireKLineSlotV2),
                &cursor,
                &regions_[5U]) ||
            !AppendRegion(
                RealtimeRegionKindV2::kTickRingSlots,
                sizeof(RealtimeWireTickSlotV2),
                config_.tick_ring_capacity,
                alignof(RealtimeWireTickSlotV2),
                &cursor,
                &regions_[6U]) ||
            !AlignUp(cursor, kPageBytes, &mapping_bytes_) ||
            mapping_bytes_ > config_.maximum_mapping_bytes ||
            mapping_bytes_ >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max()) ||
            mapping_bytes_ >
                static_cast<std::uint64_t>(
                    std::numeric_limits<off_t>::max())) {
            return RealtimeSharedServiceCreateErrorV2::
                kLayoutOverflow;
        }
        regions_[7U].kind = RealtimeRegionKindV2::kReserved8;
        regions_[7U].offset = mapping_bytes_;
        regions_[7U].alignment = 1U;
        regions_[8U].kind = RealtimeRegionKindV2::kReserved9;
        regions_[8U].offset = mapping_bytes_;
        regions_[8U].alignment = 1U;

        if (!ComputeLayoutDigest(
                config_, regions_, mapping_bytes_, &layout_digest_)) {
            return RealtimeSharedServiceCreateErrorV2::
                kUnexpectedFailure;
        }

        memfd_ = ::memfd_create(
            "l2flow-realtime-v2",
            MFD_CLOEXEC | MFD_ALLOW_SEALING);
        if (memfd_ < 0 ||
            ::ftruncate(memfd_, static_cast<off_t>(mapping_bytes_)) !=
                0) {
            SetSystemError(system_error_number, errno);
            return RealtimeSharedServiceCreateErrorV2::
                kMappingCreateFailed;
        }
        mapping_ = ::mmap(
            nullptr,
            static_cast<std::size_t>(mapping_bytes_),
            PROT_READ | PROT_WRITE,
            MAP_SHARED,
            memfd_,
            0);
        if (mapping_ == MAP_FAILED) {
            SetSystemError(system_error_number, errno);
            return RealtimeSharedServiceCreateErrorV2::
                kMappingCreateFailed;
        }
        std::memset(
            mapping_, 0, static_cast<std::size_t>(mapping_bytes_));

        const std::string proc_path =
            "/proc/self/fd/" + std::to_string(memfd_);
        read_only_fd_ = ::open(
            proc_path.c_str(), O_RDONLY | O_CLOEXEC);
        if (read_only_fd_ < 0) {
            SetSystemError(system_error_number, errno);
            return RealtimeSharedServiceCreateErrorV2::
                kReadOnlyHandleFailed;
        }
        constexpr int seals =
            F_SEAL_GROW | F_SEAL_SHRINK |
            F_SEAL_FUTURE_WRITE | F_SEAL_SEAL;
        if (::fcntl(memfd_, F_ADD_SEALS, seals) != 0) {
            SetSystemError(system_error_number, errno);
            return RealtimeSharedServiceCreateErrorV2::kSealFailed;
        }

        header_ = static_cast<RealtimeWireHeaderV2*>(mapping_);
        InitializeHeader();
        InitializeWindows();
        try {
            ring_locks_ = std::make_unique<std::atomic_flag[]>(
                static_cast<std::size_t>(
                    config_.tick_ring_capacity));
            for (std::uint64_t index = 0U;
                 index < config_.tick_ring_capacity;
                 ++index) {
                ring_locks_[static_cast<std::size_t>(index)].clear(
                    std::memory_order_relaxed);
            }
        } catch (...) {
            return RealtimeSharedServiceCreateErrorV2::
                kResourceExhausted;
        }
        return BindSocket(system_error_number);
    }

    [[nodiscard]] bool Start(int* system_error_number) noexcept {
        SetSystemError(system_error_number, 0);
        if (header_ == nullptr || listener_fd_ < 0 ||
            stop_event_fd_ < 0 || prefix_event_fd_ < 0 ||
            control_thread_.joinable() ||
            control_stop_requested_.load(std::memory_order_acquire) ||
            (Atomic(header_->flags).load(std::memory_order_acquire) &
             kRealtimeHeaderCoverageLostV2) != 0U) {
            SetSystemError(system_error_number, EINVAL);
            return false;
        }
        std::uint32_t expected =
            static_cast<std::uint32_t>(
                RealtimeServerStateV2::kInitializing);
        if (!Atomic(header_->server_state)
                 .compare_exchange_strong(
                     expected,
                     static_cast<std::uint32_t>(
                         RealtimeServerStateV2::kActive),
                     std::memory_order_release,
                     std::memory_order_acquire)) {
            SetSystemError(system_error_number, EINVAL);
            return false;
        }
        UpdateHeartbeatNow();
        if (failed()) {
            SetSystemError(system_error_number, EIO);
            return false;
        }
        try {
            control_thread_ =
                std::thread([this] { ControlLoop(); });
            return true;
        } catch (...) {
            MarkFailed();
            SetSystemError(system_error_number, EAGAIN);
            return false;
        }
    }

    [[nodiscard]] bool PublishObservedInstrumentBinding(
        const market::ObservedInstrumentBindResultV2& binding)
        noexcept {
        if (header_ == nullptr || !binding.newly_bound ||
            !binding.entry.bound() ||
            binding.entry.binding_state !=
                market::ObservedInstrumentBindingStateV2::
                    kBoundNoData ||
            binding.entry.has_snapshot || binding.entry.has_tick ||
            binding.entry.factor_eligible ||
            binding.entry.first_ingress_sequence != 0U ||
            binding.entry.last_ingress_sequence != 0U ||
            binding.entry.first_capture_sequence == 0U ||
            binding.entry.ordinal >=
                config_.directory->capacity() ||
            binding.entry.instrument_id !=
                binding.entry.ordinal + 1U ||
            binding.bound_count != binding.entry.ordinal + 1U ||
            binding.catalog_generation == 0U ||
            !DigestNonzero(binding.catalog_digest) ||
            binding.entry.key.market == market::MarketV1::kUnknown ||
            binding.entry.key.security_id.empty() ||
            binding.entry.key.security_id_source.size() >
                static_cast<std::size_t>(
                    std::numeric_limits<std::uint32_t>::max()) ||
            binding.entry.key.security_id.size() >
                static_cast<std::size_t>(
                    std::numeric_limits<std::uint32_t>::max()) ||
            static_cast<std::uint8_t>(
                binding.entry.metadata.quantity_unit) >
                static_cast<std::uint8_t>(
                    market::QuantityUnitV1::kIndexUnit) ||
            static_cast<std::uint8_t>(
                binding.entry.metadata.security_type) >
                static_cast<std::uint8_t>(
                    market::SecurityTypeV1::kOption) ||
            static_cast<std::uint8_t>(
                binding.entry.metadata.asset_scope) >
                static_cast<std::uint8_t>(
                    market::AssetScopeV1::
                        kOutsideDocumentedCore) ||
            !StateAcceptsPublication(
                Atomic(header_->server_state)
                    .load(std::memory_order_acquire))) {
            ReportFailure(
                "binding_precondition",
                binding.entry.ordinal,
                binding.entry.instrument_id,
                0U);
            MarkCoverageLost();
            return false;
        }

        const std::uint64_t source_bytes =
            static_cast<std::uint64_t>(
                binding.entry.key.security_id_source.size());
        const std::uint64_t id_bytes =
            static_cast<std::uint64_t>(
                binding.entry.key.security_id.size());
        const std::uint64_t used =
            key_arena_used_.load(std::memory_order_acquire);
        std::uint64_t id_offset = 0U;
        std::uint64_t end = 0U;
        if (!CheckedAdd(used, source_bytes, &id_offset) ||
            !CheckedAdd(id_offset, id_bytes, &end) ||
            end > config_.key_arena_bytes) {
            ReportFailure(
                "key_arena_exhausted",
                binding.entry.ordinal,
                binding.entry.instrument_id,
                binding.entry.first_capture_sequence);
            MarkCoverageLost();
            return false;
        }

        // The binding sink is single-writer. Key bytes are copied before the
        // immutable row, and the row is release-published before the catalog
        // status cut advances.
        std::byte* const arena = key_arena();
        if (source_bytes != 0U) {
            std::memcpy(
                arena + used,
                binding.entry.key.security_id_source.data(),
                static_cast<std::size_t>(source_bytes));
        }
        std::memcpy(
            arena + id_offset,
            binding.entry.key.security_id.data(),
            static_cast<std::size_t>(id_bytes));

        RealtimeWireInstrumentV2& row =
            instrument_rows()[binding.entry.ordinal];
        std::uint64_t row_stable = 0U;
        if (!AcquireSeqcount(
                &row.publish_tag, &row_stable, true)) {
            ReportFailure(
                "binding_row_busy",
                binding.entry.ordinal,
                binding.entry.instrument_id,
                binding.entry.first_capture_sequence);
            MarkCoverageLost();
            return false;
        }
        StoreBoundIdentityRow(
            &row, binding, used, id_offset);
        ReleaseSeqcount(&row.publish_tag, row_stable);

        std::uint64_t status_stable = 0U;
        if (!AcquireSeqcount(
                &header_->status_publish_tag,
                &status_stable,
                false)) {
            ReportFailure(
                "binding_status_busy",
                binding.entry.ordinal,
                binding.entry.instrument_id,
                binding.entry.first_capture_sequence);
            MarkCoverageLost();
            return false;
        }
        const std::uint32_t old_bound =
            Atomic(header_->bound_count)
                .load(std::memory_order_relaxed);
        const std::uint64_t old_generation =
            Atomic(header_->catalog_generation)
                .load(std::memory_order_relaxed);
        const bool coherent =
            old_bound == binding.entry.ordinal &&
            binding.bound_count == old_bound + 1U &&
            old_generation !=
                std::numeric_limits<std::uint64_t>::max() &&
            binding.catalog_generation == old_generation + 1U &&
            HeaderCountsValidLocked();
        if (coherent) {
            Atomic(header_->catalog_generation)
                .store(
                    binding.catalog_generation,
                    std::memory_order_relaxed);
            StoreDigest(
                &header_->catalog_digest,
                binding.catalog_digest);
            Atomic(header_->bound_count)
                .store(
                    static_cast<std::uint32_t>(
                        binding.bound_count),
                    // Point readers acquire this field directly. The release
                    // imports the already release-published immutable row and
                    // its preceding key-arena copies without touching the
                    // global status seqcount.
                    std::memory_order_release);
            key_arena_used_.store(end, std::memory_order_release);
        }
        ReleaseSeqcount(
            &header_->status_publish_tag, status_stable);
        if (!coherent) {
            ReportFailure(
                "binding_catalog_cut",
                binding.entry.ordinal,
                binding.entry.instrument_id,
                binding.entry.first_capture_sequence);
            MarkCoverageLost();
            return false;
        }
        return true;
    }

    [[nodiscard]] bool PublishApplied(
        std::size_t ordinal,
        const market::RealtimeHistoryRecordV1& record) noexcept {
        if (header_ == nullptr ||
            ordinal >= config_.directory->capacity() ||
            record.instrument_id() != ordinal + 1U ||
            record.ingress_sequence() == 0U ||
            !StateAcceptsPublication(
                Atomic(header_->server_state)
                    .load(std::memory_order_acquire))) {
            ReportFailure(
                "applied_precondition",
                ordinal,
                record.instrument_id(),
                record.ingress_sequence());
            MarkCoverageLost();
            return false;
        }
        const RealtimeWireInstrumentV2& identity =
            instrument_rows()[ordinal];
        if (Atomic(identity.instrument_id)
                    .load(std::memory_order_acquire) !=
                record.instrument_id() ||
            Atomic(identity.ordinal).load(std::memory_order_acquire) !=
                ordinal) {
            ReportFailure(
                "applied_identity",
                ordinal,
                record.instrument_id(),
                record.ingress_sequence());
            MarkCoverageLost();
            return false;
        }

        std::uint32_t availability_flag = 0U;
        bool factor_eligible = false;
        if (market::IsSnapshotEventKindV1(record.kind())) {
            RealtimeWireSnapshotPayloadV2 payload{};
            if (!ProjectSnapshot(record, ordinal, &payload) ||
                !PublishSlot(&snapshot_slots()[ordinal], payload)) {
                ReportFailure(
                    "snapshot_projection_or_slot",
                    ordinal,
                    record.instrument_id(),
                    record.ingress_sequence());
                MarkCoverageLost();
                return false;
            }
            availability_flag =
                kRealtimeInstrumentHasSnapshotV2;
            factor_eligible = SnapshotFactorEligible(payload);
        } else if (market::IsTickEventKindV1(record.kind())) {
            RealtimeWireTickPayloadV2 payload{};
            if (!ProjectTick(record, ordinal, &payload) ||
                !PublishRing(payload) ||
                !PublishSlot(&latest_tick_slots()[ordinal], payload)) {
                ReportFailure(
                    "tick_projection_or_slot",
                    ordinal,
                    record.instrument_id(),
                    record.ingress_sequence());
                MarkCoverageLost();
                return false;
            }
            availability_flag = kRealtimeInstrumentHasTickV2;
        } else {
            ReportFailure(
                "applied_kind",
                ordinal,
                record.instrument_id(),
                record.ingress_sequence());
            MarkCoverageLost();
            return false;
        }
        if (!UpdateAppliedRow(
                ordinal,
                record.instrument_id(),
                record.ingress_sequence(),
                availability_flag,
                factor_eligible)) {
            ReportFailure(
                "applied_row",
                ordinal,
                record.instrument_id(),
                record.ingress_sequence());
            MarkCoverageLost();
            return false;
        }
        Atomic(header_->published_records)
            .fetch_add(1U, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool PublishProcessingProgress(
        realtime::ProcessingProgressV2 progress) noexcept {
        if (header_ == nullptr || !progress.valid() ||
            !StateAcceptsPublication(
                Atomic(header_->server_state)
                    .load(std::memory_order_acquire))) {
            MarkCoverageLost();
            return false;
        }
        std::uint64_t stable = 0U;
        if (!AcquireSeqcount(
                &header_->status_publish_tag, &stable, false)) {
            MarkCoverageLost();
            return false;
        }
        const std::uint64_t old_accepted =
            Atomic(header_->accepted_sequence)
                .load(std::memory_order_relaxed);
        const std::uint64_t old_durable =
            Atomic(header_->durable_sequence)
                .load(std::memory_order_relaxed);
        const std::uint64_t old_applied =
            Atomic(header_->applied_sequence)
                .load(std::memory_order_relaxed);
        const std::uint64_t accepted =
            std::max(old_accepted, progress.accepted_sequence);
        const std::uint64_t durable =
            std::max(old_durable, progress.durable_sequence);
        const std::uint64_t applied =
            std::max(old_applied, progress.applied_sequence);
        const bool coherent =
            durable <= accepted && applied <= accepted &&
            HeaderCountsValidLocked();
        if (coherent) {
            Atomic(header_->accepted_sequence)
                .store(accepted, std::memory_order_relaxed);
            Atomic(header_->durable_sequence)
                .store(durable, std::memory_order_relaxed);
            Atomic(header_->applied_sequence)
                .store(applied, std::memory_order_relaxed);
        }
        ReleaseSeqcount(&header_->status_publish_tag, stable);
        if (!coherent) {
            MarkCoverageLost();
        }
        return coherent;
    }

    void MarkCoverageLost() noexcept {
        if (header_ == nullptr) {
            return;
        }
        Atomic(header_->flags)
            .fetch_or(
                kRealtimeHeaderCoverageLostV2,
                std::memory_order_release);
        Atomic(header_->server_state)
            .store(
                static_cast<std::uint32_t>(
                    RealtimeServerStateV2::kFailed),
                std::memory_order_release);
    }

    [[nodiscard]] bool PublishKLineGeneration(
        const market::RealtimeKLineGenerationV1& generation)
        noexcept {
        if (kline_publication_in_progress_.test_and_set(
                std::memory_order_acquire)) {
            MarkCoverageLost();
            return false;
        }
        struct FlagGuard final {
            std::atomic_flag& flag;
            ~FlagGuard() { flag.clear(std::memory_order_release); }
        } guard{kline_publication_in_progress_};

        const market::RealtimeHistoryWatermarkV1& watermark =
            generation.watermark();
        const auto& catalog = watermark.catalog_snapshot;
        if (header_ == nullptr || config_.kline_windows.empty() ||
            !StateAcceptsPublication(
                Atomic(header_->server_state)
                    .load(std::memory_order_acquire)) ||
            watermark.run_id != config_.run_id ||
            watermark.trade_date != config_.trade_date ||
            watermark.generation == 0U ||
            catalog == nullptr ||
            catalog->session_epoch() != config_.session_epoch ||
            catalog->capacity() != config_.directory->capacity() ||
            catalog->catalog_scope() !=
                market::ObservedInstrumentCatalogScopeV2::
                    kObservedOnly ||
            catalog->coverage_complete() ||
            !watermark.processing_progress.valid() ||
            generation.windows().size() !=
                config_.kline_windows.size()) {
            MarkCoverageLost();
            return false;
        }
        const std::span<const market::KLineWindowSpecV1>
            generation_windows = generation.windows();
        for (std::size_t index = 0U;
             index < generation_windows.size();
             ++index) {
            if (generation_windows[index].window_id !=
                    config_.kline_windows[index].window_id ||
                generation_windows[index].duration_ns !=
                    config_.kline_windows[index].duration_ns) {
                MarkCoverageLost();
                return false;
            }
        }
        const std::uint64_t generation_number =
            watermark.generation;
        const std::uint64_t completed =
            Atomic(header_->kline_generation)
                .load(std::memory_order_acquire);
        const std::uint64_t visible_catalog_generation =
            Atomic(header_->catalog_generation)
                .load(std::memory_order_acquire);
        const std::uint32_t visible_bound_count =
            Atomic(header_->bound_count)
                .load(std::memory_order_acquire);
        if (completed ==
                std::numeric_limits<std::uint64_t>::max() ||
            generation_number != completed + 1U ||
            catalog->catalog_generation() >
                visible_catalog_generation ||
            catalog->bound_count() > visible_bound_count) {
            MarkCoverageLost();
            return false;
        }

        std::vector<std::uint8_t> ordinal_has_kline;
        try {
            ordinal_has_kline.assign(
                catalog->bound_count(), 0U);
        } catch (...) {
            MarkCoverageLost();
            return false;
        }
        const std::uint64_t logical_slot_count =
            static_cast<std::uint64_t>(
                config_.directory->capacity()) *
            static_cast<std::uint64_t>(
                config_.kline_windows.size());
        const std::uint64_t table_index =
            generation_number % kRealtimeKLineTableCountV2;
        const std::uint64_t table_offset =
            table_index * logical_slot_count;

        for (std::size_t ordinal = 0U;
             ordinal < catalog->bound_count();
             ++ordinal) {
            market::ObservedInstrumentEntryViewV2 entry{};
            if (catalog->EntryAt(ordinal, &entry) !=
                    market::ObservedInstrumentDirectoryErrorV2::
                        kNone ||
                !entry.bound() ||
                entry.instrument_id != ordinal + 1U ||
                Atomic(instrument_rows()[ordinal].instrument_id)
                        .load(std::memory_order_acquire) !=
                    entry.instrument_id) {
                MarkCoverageLost();
                return false;
            }
            for (std::size_t window_index = 0U;
                 window_index < config_.kline_windows.size();
                 ++window_index) {
                const market::KLineWindowSpecV1& window =
                    config_.kline_windows[window_index];
                market::KLineBarV1 bar{};
                const market::KLineQueryErrorV1 error =
                    generation.GetLatestBar(
                        entry.instrument_id,
                        window.window_id,
                        &bar);
                RealtimeWireKLinePayloadV2 payload{};
                if (error == market::KLineQueryErrorV1::kNotFound) {
                    payload.generation = generation_number;
                    payload.trade_date = config_.trade_date;
                    payload.instrument_id = entry.instrument_id;
                    payload.window_id = window.window_id;
                    payload.window_duration_ns =
                        window.duration_ns;
                } else if (
                    error != market::KLineQueryErrorV1::kNone ||
                    bar.trade_date != config_.trade_date ||
                    bar.instrument_id != entry.instrument_id ||
                    bar.window_id != window.window_id ||
                    bar.window_duration_ns != window.duration_ns ||
                    !ProjectKLine(
                        generation_number, bar, &payload)) {
                    MarkCoverageLost();
                    return false;
                } else {
                    ordinal_has_kline[ordinal] = 1U;
                }
                const std::uint64_t slot_index =
                    table_offset +
                    static_cast<std::uint64_t>(ordinal) *
                        static_cast<std::uint64_t>(
                            config_.kline_windows.size()) +
                    static_cast<std::uint64_t>(window_index);
                if (!PublishSlot(
                        &kline_slots()[static_cast<std::size_t>(
                            slot_index)],
                        payload)) {
                    MarkCoverageLost();
                    return false;
                }
            }
        }

        bool coherent =
            Atomic(header_->kline_generation)
                    .load(std::memory_order_acquire) ==
                completed &&
            StateAcceptsPublication(
                Atomic(header_->server_state)
                    .load(std::memory_order_acquire));
        if (coherent) {
            // Every inactive-table slot is already release-published. Commit
            // the table before advertising HAS_KLINE on a newly eligible
            // row: point readers intentionally ignore status_publish_tag, so
            // the opposite order could make them select the old table for a
            // newly flagged row. Seeing the new table just before the flag is
            // benign TYPE_UNAVAILABLE; seeing a flag without its table is
            // never allowed.
            Atomic(header_->kline_generation)
                .store(
                    generation_number,
                    std::memory_order_release);
        }
        bool changed = false;
        for (std::size_t ordinal = 0U;
             coherent && ordinal < ordinal_has_kline.size();
             ++ordinal) {
            if (ordinal_has_kline[ordinal] == 0U) {
                continue;
            }
            RealtimeWireInstrumentV2& row =
                instrument_rows()[ordinal];
            const std::uint32_t state =
                Atomic(row.binding_state)
                    .load(std::memory_order_acquire);
            const std::uint32_t flags =
                Atomic(row.availability_flags)
                    .load(std::memory_order_acquire);
            if (state !=
                    static_cast<std::uint32_t>(
                        RealtimeInstrumentBindingStateV2::
                            kAvailable) ||
                Atomic(row.instrument_id)
                        .load(std::memory_order_relaxed) !=
                    ordinal + 1U) {
                coherent = false;
                break;
            }
            if ((flags &
                 kRealtimeInstrumentHasKLineV2) != 0U) {
                continue;
            }
            std::uint64_t row_stable = 0U;
            if (!AcquireSeqcount(
                    &row.publish_tag, &row_stable, false)) {
                coherent = false;
                break;
            }
            const std::uint32_t locked_flags =
                Atomic(row.availability_flags)
                    .load(std::memory_order_relaxed);
            if (Atomic(row.binding_state)
                        .load(std::memory_order_relaxed) !=
                    static_cast<std::uint32_t>(
                        RealtimeInstrumentBindingStateV2::
                            kAvailable) ||
                Atomic(row.instrument_id)
                        .load(std::memory_order_relaxed) !=
                    ordinal + 1U) {
                ReleaseSeqcount(&row.publish_tag, row_stable);
                coherent = false;
                break;
            }
            if ((locked_flags &
                 kRealtimeInstrumentHasKLineV2) == 0U) {
                Atomic(row.availability_flags)
                    .store(
                        locked_flags |
                            kRealtimeInstrumentHasKLineV2,
                        std::memory_order_relaxed);
                changed = true;
            }
            ReleaseSeqcount(&row.publish_tag, row_stable);
        }
        if (coherent && changed) {
            // HAS_KLINE is not one of the selection-count dimensions, so its
            // rows can be release-published independently after the table
            // commit. Hold the global status writer only for the short
            // version bump; a full-capacity KLine generation must not stall
            // concurrent durable/applied progress publishers.
            std::uint64_t status_stable = 0U;
            const bool status_locked = AcquireSeqcount(
                &header_->status_publish_tag,
                &status_stable,
                false);
            if (!status_locked) {
                coherent = false;
            }
            const std::uint64_t data_generation =
                coherent
                    ? Atomic(header_->data_state_generation)
                          .load(std::memory_order_relaxed)
                    : 0U;
            if (coherent &&
                (!HeaderCountsValidLocked() ||
                 data_generation ==
                     std::numeric_limits<std::uint64_t>::max())) {
                coherent = false;
            }
            if (coherent) {
                Atomic(header_->data_state_generation)
                    .store(
                        data_generation + 1U,
                        std::memory_order_relaxed);
            }
            if (status_locked) {
                ReleaseSeqcount(
                    &header_->status_publish_tag,
                    status_stable);
            }
        }
        if (!coherent) {
            MarkCoverageLost();
        }
        return coherent;
    }

    void MarkDraining() noexcept {
        if (header_ == nullptr) {
            return;
        }
        std::uint32_t expected =
            static_cast<std::uint32_t>(
                RealtimeServerStateV2::kActive);
        static_cast<void>(
            Atomic(header_->server_state)
                .compare_exchange_strong(
                    expected,
                    static_cast<std::uint32_t>(
                        RealtimeServerStateV2::kDraining),
                    std::memory_order_release,
                    std::memory_order_acquire));
    }

    [[nodiscard]] bool MarkStoppedClean(
        std::uint64_t final_admitted_tick_sequence) noexcept {
        while (header_ != nullptr &&
               AdvanceContiguousTickPrefix(
                   kControlPrefixAdvanceBudget)) {
        }
        if (header_ == nullptr ||
            (Atomic(header_->flags).load(std::memory_order_acquire) &
             kRealtimeHeaderCoverageLostV2) != 0U ||
            Atomic(header_->tick_highest_published_sequence)
                    .load(std::memory_order_acquire) !=
                final_admitted_tick_sequence ||
            Atomic(header_->tick_contiguous_published_sequence)
                    .load(std::memory_order_acquire) !=
                final_admitted_tick_sequence) {
            MarkCoverageLost();
            return false;
        }
        std::uint32_t expected =
            static_cast<std::uint32_t>(
                RealtimeServerStateV2::kDraining);
        if (!Atomic(header_->server_state)
                 .compare_exchange_strong(
                     expected,
                     static_cast<std::uint32_t>(
                         RealtimeServerStateV2::kStoppedClean),
                     std::memory_order_release,
                     std::memory_order_acquire)) {
            MarkCoverageLost();
            return false;
        }
        if ((Atomic(header_->flags).load(std::memory_order_acquire) &
             kRealtimeHeaderCoverageLostV2) != 0U ||
            Atomic(header_->server_state)
                    .load(std::memory_order_acquire) !=
                static_cast<std::uint32_t>(
                    RealtimeServerStateV2::kStoppedClean)) {
            MarkCoverageLost();
            return false;
        }
        return true;
    }

    void MarkFailed() noexcept { MarkCoverageLost(); }

    void StopControl() noexcept {
        std::lock_guard<std::mutex> lock(control_stop_mutex_);
        if (header_ != nullptr) {
            const std::uint32_t state =
                Atomic(header_->server_state)
                    .load(std::memory_order_acquire);
            if (state ==
                    static_cast<std::uint32_t>(
                        RealtimeServerStateV2::kInitializing) ||
                state ==
                    static_cast<std::uint32_t>(
                        RealtimeServerStateV2::kActive) ||
                state ==
                    static_cast<std::uint32_t>(
                        RealtimeServerStateV2::kDraining)) {
                MarkCoverageLost();
            }
        }
        bool expected = false;
        if (!control_stop_requested_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            if (control_thread_.joinable()) {
                control_thread_.join();
            }
            return;
        }
        if (stop_event_fd_ >= 0) {
            const std::uint64_t one = 1U;
            ssize_t written = -1;
            do {
                written =
                    ::write(stop_event_fd_, &one, sizeof(one));
            } while (written < 0 && errno == EINTR);
            if (written != static_cast<ssize_t>(sizeof(one)) &&
                !(written < 0 && errno == EAGAIN)) {
                MarkCoverageLost();
            }
        }
        if (control_thread_.joinable()) {
            control_thread_.join();
        }
    }

    [[nodiscard]] std::uint64_t mapping_bytes() const noexcept {
        return mapping_bytes_;
    }

    [[nodiscard]] std::uint64_t key_arena_used_bytes()
        const noexcept {
        return key_arena_used_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool failed() const noexcept {
        return header_ == nullptr ||
               Atomic(header_->server_state)
                       .load(std::memory_order_acquire) ==
                   static_cast<std::uint32_t>(
                       RealtimeServerStateV2::kFailed) ||
               (Atomic(header_->flags)
                    .load(std::memory_order_acquire) &
                kRealtimeHeaderCoverageLostV2) != 0U;
    }

    [[nodiscard]] const std::filesystem::path& socket_path()
        const noexcept {
        return config_.control_socket_path;
    }

private:
    [[nodiscard]] bool AcquireSeqcount(
        std::uint64_t* tag_address,
        std::uint64_t* stable_output,
        bool require_zero) noexcept {
        if (tag_address == nullptr || stable_output == nullptr) {
            return false;
        }
        std::atomic_ref<std::uint64_t> tag = Atomic(*tag_address);
        std::size_t contention_count = 0U;
        while (header_ != nullptr &&
               StateAcceptsPublication(
                   Atomic(header_->server_state)
                       .load(std::memory_order_acquire))) {
            std::uint64_t stable =
                tag.load(std::memory_order_acquire);
            if (require_zero && stable != 0U) {
                return false;
            }
            if ((stable & 1U) != 0U) {
                ++contention_count;
                if ((contention_count & 63U) == 0U) {
                    std::this_thread::yield();
                }
                continue;
            }
            if (stable >
                std::numeric_limits<std::uint64_t>::max() - 2U) {
                return false;
            }
            if (tag.compare_exchange_weak(
                    stable,
                    stable + 1U,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                *stable_output = stable;
                return true;
            }
            ++contention_count;
            if ((contention_count & 63U) == 0U) {
                std::this_thread::yield();
            }
        }
        return false;
    }

    void ReleaseSeqcount(
        std::uint64_t* tag_address,
        std::uint64_t stable) noexcept {
        Atomic(*tag_address).store(
            stable + 2U, std::memory_order_release);
    }

    [[nodiscard]] bool HeaderCountsValidLocked() const noexcept {
        return RealtimeWireCountsValidV2(
                   header_->capacity,
                   Atomic(header_->bound_count)
                       .load(std::memory_order_relaxed),
                   Atomic(header_->available_count)
                       .load(std::memory_order_relaxed),
                   Atomic(header_->snapshot_available_count)
                       .load(std::memory_order_relaxed),
                   Atomic(header_->tick_available_count)
                       .load(std::memory_order_relaxed),
                   Atomic(header_->factor_eligible_count)
                       .load(std::memory_order_relaxed)) &&
               RealtimeWireProcessingSequencesValidV2(
                   Atomic(header_->accepted_sequence)
                       .load(std::memory_order_relaxed),
                   Atomic(header_->durable_sequence)
                       .load(std::memory_order_relaxed),
                   Atomic(header_->applied_sequence)
                       .load(std::memory_order_relaxed));
    }

    void StoreBoundIdentityRow(
        RealtimeWireInstrumentV2* row,
        const market::ObservedInstrumentBindResultV2& binding,
        std::uint64_t source_offset,
        std::uint64_t id_offset) noexcept {
        Atomic(row->instrument_id)
            .store(
                binding.entry.instrument_id,
                std::memory_order_relaxed);
        Atomic(row->ordinal)
            .store(
                static_cast<std::uint32_t>(
                    binding.entry.ordinal),
                std::memory_order_relaxed);
        Atomic(row->binding_state)
            .store(
                static_cast<std::uint32_t>(
                    RealtimeInstrumentBindingStateV2::
                        kBoundNoData),
                std::memory_order_relaxed);
        Atomic(row->availability_flags)
            .store(0U, std::memory_order_relaxed);
        Atomic(row->market)
            .store(
                static_cast<std::uint8_t>(
                    binding.entry.key.market),
                std::memory_order_relaxed);
        Atomic(row->quantity_unit)
            .store(
                static_cast<std::uint8_t>(
                    binding.entry.metadata.quantity_unit),
                std::memory_order_relaxed);
        Atomic(row->security_type)
            .store(
                static_cast<std::uint8_t>(
                    binding.entry.metadata.security_type),
                std::memory_order_relaxed);
        Atomic(row->asset_scope)
            .store(
                static_cast<std::uint8_t>(
                    binding.entry.metadata.asset_scope),
                std::memory_order_relaxed);
        Atomic(row->security_id_source_offset)
            .store(source_offset, std::memory_order_relaxed);
        Atomic(row->security_id_offset)
            .store(id_offset, std::memory_order_relaxed);
        Atomic(row->security_id_source_length)
            .store(
                static_cast<std::uint32_t>(
                    binding.entry.key.security_id_source.size()),
                std::memory_order_relaxed);
        Atomic(row->security_id_length)
            .store(
                static_cast<std::uint32_t>(
                    binding.entry.key.security_id.size()),
                std::memory_order_relaxed);
    }

    [[nodiscard]] bool UpdateAppliedRow(
        std::size_t ordinal,
        std::uint32_t instrument_id,
        std::uint64_t ingress_sequence,
        std::uint32_t availability_flag,
        bool factor_eligible) noexcept {
        RealtimeWireInstrumentV2& row =
            instrument_rows()[ordinal];
        const std::uint32_t observed_state =
            Atomic(row.binding_state).load(std::memory_order_acquire);
        const std::uint32_t observed_flags =
            Atomic(row.availability_flags)
                .load(std::memory_order_acquire);
        std::uint32_t expected_flags =
            observed_flags | availability_flag;
        if (availability_flag ==
            kRealtimeInstrumentHasSnapshotV2) {
            if (factor_eligible) {
                expected_flags |=
                    kRealtimeInstrumentFactorEligibleV2;
            } else {
                expected_flags &=
                    ~kRealtimeInstrumentFactorEligibleV2;
            }
        }
        const bool predicted_transition =
            observed_state !=
                static_cast<std::uint32_t>(
                    RealtimeInstrumentBindingStateV2::kAvailable) ||
            expected_flags != observed_flags;

        std::uint64_t status_stable = 0U;
        bool status_locked = false;
        if (predicted_transition) {
            status_locked = AcquireSeqcount(
                &header_->status_publish_tag,
                &status_stable,
                false);
            if (!status_locked) {
                return false;
            }
        }
        std::uint64_t row_stable = 0U;
        if (!AcquireSeqcount(
                &row.publish_tag, &row_stable, false)) {
            if (status_locked) {
                ReleaseSeqcount(
                    &header_->status_publish_tag,
                    status_stable);
            }
            return false;
        }

        const std::uint32_t state =
            Atomic(row.binding_state)
                .load(std::memory_order_relaxed);
        const std::uint32_t old_flags =
            Atomic(row.availability_flags)
                .load(std::memory_order_relaxed);
        const std::uint64_t old_first =
            Atomic(row.first_ingress_sequence)
                .load(std::memory_order_relaxed);
        const std::uint64_t old_last =
            Atomic(row.last_ingress_sequence)
                .load(std::memory_order_relaxed);
        std::uint32_t new_flags =
            old_flags | availability_flag;
        if (availability_flag ==
            kRealtimeInstrumentHasSnapshotV2) {
            if (factor_eligible) {
                new_flags |=
                    kRealtimeInstrumentFactorEligibleV2;
            } else {
                new_flags &=
                    ~kRealtimeInstrumentFactorEligibleV2;
            }
        }
        const bool old_available =
            state ==
            static_cast<std::uint32_t>(
                RealtimeInstrumentBindingStateV2::kAvailable);
        const bool transition =
            !old_available || new_flags != old_flags;
        bool coherent =
            Atomic(row.instrument_id)
                    .load(std::memory_order_relaxed) ==
                instrument_id &&
            Atomic(row.ordinal).load(std::memory_order_relaxed) ==
                ordinal &&
            (state ==
                 static_cast<std::uint32_t>(
                     RealtimeInstrumentBindingStateV2::
                         kBoundNoData) ||
             old_available) &&
            RealtimeWireAvailabilityFlagsValidV2(new_flags) &&
            (!transition || status_locked) &&
            (!old_available ||
             (old_first != 0U && old_last >= old_first));

        std::uint32_t available_count = 0U;
        std::uint32_t snapshot_count = 0U;
        std::uint32_t tick_count = 0U;
        std::uint32_t factor_count = 0U;
        std::uint64_t data_generation = 0U;
        if (coherent && transition) {
            const std::uint32_t bound_count =
                Atomic(header_->bound_count)
                    .load(std::memory_order_relaxed);
            available_count =
                Atomic(header_->available_count)
                    .load(std::memory_order_relaxed);
            snapshot_count =
                Atomic(header_->snapshot_available_count)
                    .load(std::memory_order_relaxed);
            tick_count =
                Atomic(header_->tick_available_count)
                    .load(std::memory_order_relaxed);
            factor_count =
                Atomic(header_->factor_eligible_count)
                    .load(std::memory_order_relaxed);
            data_generation =
                Atomic(header_->data_state_generation)
                    .load(std::memory_order_relaxed);
            if (!HeaderCountsValidLocked() ||
                ordinal >= bound_count ||
                data_generation ==
                    std::numeric_limits<std::uint64_t>::max()) {
                coherent = false;
            } else {
                if (!old_available) {
                    ++available_count;
                }
                if ((old_flags &
                         kRealtimeInstrumentHasSnapshotV2) == 0U &&
                    (new_flags &
                         kRealtimeInstrumentHasSnapshotV2) != 0U) {
                    ++snapshot_count;
                }
                if ((old_flags &
                         kRealtimeInstrumentHasTickV2) == 0U &&
                    (new_flags &
                         kRealtimeInstrumentHasTickV2) != 0U) {
                    ++tick_count;
                }
                const bool old_factor =
                    (old_flags &
                     kRealtimeInstrumentFactorEligibleV2) != 0U;
                const bool new_factor =
                    (new_flags &
                     kRealtimeInstrumentFactorEligibleV2) != 0U;
                if (!old_factor && new_factor) {
                    ++factor_count;
                } else if (old_factor && !new_factor) {
                    if (factor_count == 0U) {
                        coherent = false;
                    } else {
                        --factor_count;
                    }
                }
                coherent =
                    coherent &&
                    RealtimeWireCountsValidV2(
                        header_->capacity,
                        bound_count,
                        available_count,
                        snapshot_count,
                        tick_count,
                        factor_count);
            }
        }
        if (coherent) {
            const std::uint64_t first =
                old_first == 0U
                    ? ingress_sequence
                    : std::min(old_first, ingress_sequence);
            const std::uint64_t last =
                std::max(old_last, ingress_sequence);
            Atomic(row.binding_state)
                .store(
                    static_cast<std::uint32_t>(
                        RealtimeInstrumentBindingStateV2::
                            kAvailable),
                    std::memory_order_relaxed);
            Atomic(row.availability_flags)
                .store(new_flags, std::memory_order_relaxed);
            Atomic(row.first_ingress_sequence)
                .store(first, std::memory_order_relaxed);
            Atomic(row.last_ingress_sequence)
                .store(last, std::memory_order_relaxed);
            if (transition) {
                Atomic(header_->available_count)
                    .store(
                        available_count,
                        std::memory_order_relaxed);
                Atomic(header_->snapshot_available_count)
                    .store(
                        snapshot_count,
                        std::memory_order_relaxed);
                Atomic(header_->tick_available_count)
                    .store(
                        tick_count,
                        std::memory_order_relaxed);
                Atomic(header_->factor_eligible_count)
                    .store(
                        factor_count,
                        std::memory_order_relaxed);
                Atomic(header_->data_state_generation)
                    .store(
                        data_generation + 1U,
                        std::memory_order_relaxed);
            }
        }
        ReleaseSeqcount(&row.publish_tag, row_stable);
        if (status_locked) {
            ReleaseSeqcount(
                &header_->status_publish_tag, status_stable);
        }
        return coherent;
    }

    [[nodiscard]] bool PublishRing(
        const RealtimeWireTickPayloadV2& payload) noexcept {
        const std::uint64_t sequence =
            payload.common.tick_stream_sequence;
        if (sequence == 0U || ring_locks_ == nullptr) {
            return false;
        }
        // Production config makes the ring at least as large as the explicit
        // pipeline applied-dispatch window. Catch the publication prefix up
        // before reusing a cell; if a genuine missing tick still keeps this
        // sequence outside the ring window, fail closed instead of silently
        // overwriting evidence needed to close that gap.
        for (;;) {
            const std::uint64_t contiguous =
                Atomic(
                    header_->
                        tick_contiguous_published_sequence)
                    .load(std::memory_order_acquire);
            if (sequence <= contiguous) {
                return false;
            }
            if (sequence - contiguous <=
                config_.tick_ring_capacity) {
                break;
            }
            const std::uint64_t before = contiguous;
            static_cast<void>(AdvanceContiguousTickPrefix(
                kControlPrefixAdvanceBudget));
            if (Atomic(
                    header_->
                        tick_contiguous_published_sequence)
                    .load(std::memory_order_acquire) == before) {
                return false;
            }
        }
        const std::uint64_t index =
            (sequence - 1U) % config_.tick_ring_capacity;
        std::atomic_flag& lock =
            ring_locks_[static_cast<std::size_t>(index)];
        if (lock.test_and_set(std::memory_order_acquire)) {
            return false;
        }
        RealtimeWireTickSlotV2& slot =
            ring_slots()[static_cast<std::size_t>(index)];
        bool published = false;
        const std::uint64_t stable =
            Atomic(slot.publish_tag).load(std::memory_order_acquire);
        std::uint64_t existing_sequence = 0U;
        if (stable != 0U && (stable & 1U) == 0U) {
            constexpr std::size_t sequence_word =
                offsetof(
                    RealtimeWireCommonRecordV2,
                    tick_stream_sequence) /
                sizeof(std::uint64_t);
            existing_sequence =
                Atomic(slot.payload_words[sequence_word])
                    .load(std::memory_order_acquire);
        }
        const std::uint64_t contiguous =
            Atomic(
                header_->tick_contiguous_published_sequence)
                .load(std::memory_order_acquire);
        if ((stable & 1U) == 0U &&
            existing_sequence < sequence &&
            (existing_sequence == 0U ||
             existing_sequence <= contiguous) &&
            PublishSlot(&slot, payload)) {
            published = true;
        }
        lock.clear(std::memory_order_release);
        if (!published) {
            return false;
        }
        AtomicMaximum(
            &header_->tick_highest_published_sequence,
            sequence);
        if (AdvanceContiguousTickPrefix(kHotPrefixAdvanceBudget) &&
            !RequestPrefixAdvance()) {
            return false;
        }
        return true;
    }

    // True means the finite budget was exhausted and another pass may make
    // immediate progress.
    [[nodiscard]] bool AdvanceContiguousTickPrefix(
        std::size_t budget) noexcept {
        if (header_ == nullptr || budget == 0U) {
            return budget == 0U;
        }
        std::atomic_ref<std::uint64_t> contiguous =
            Atomic(header_->tick_contiguous_published_sequence);
        std::uint64_t current =
            contiguous.load(std::memory_order_acquire);
        std::size_t advanced = 0U;
        while (advanced < budget) {
            if (current ==
                std::numeric_limits<std::uint64_t>::max()) {
                return false;
            }
            const std::uint64_t expected = current + 1U;
            if (Atomic(header_->tick_highest_published_sequence)
                    .load(std::memory_order_acquire) < expected) {
                return false;
            }
            const std::uint64_t index =
                (expected - 1U) % config_.tick_ring_capacity;
            if (!TickSlotContainsSequence(
                    ring_slots()[static_cast<std::size_t>(index)],
                    expected)) {
                return false;
            }
            if (!contiguous.compare_exchange_weak(
                    current,
                    expected,
                    std::memory_order_release,
                    std::memory_order_acquire)) {
                continue;
            }
            current = expected;
            ++advanced;
        }
        return true;
    }

    [[nodiscard]] bool RequestPrefixAdvance() noexcept {
        bool expected = false;
        if (!prefix_notification_pending_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            return true;
        }
        const std::uint64_t one = 1U;
        ssize_t written = -1;
        do {
            written =
                ::write(prefix_event_fd_, &one, sizeof(one));
        } while (written < 0 && errno == EINTR);
        if (written == static_cast<ssize_t>(sizeof(one))) {
            return true;
        }
        prefix_notification_pending_.store(
            false, std::memory_order_release);
        return false;
    }

    void InitializeHeader() noexcept {
        header_->magic = kRealtimeShmMagicV2;
        header_->abi_major = kRealtimeWireMajorV2;
        header_->abi_minor = kRealtimeWireMinorV2;
        header_->header_bytes =
            static_cast<std::uint32_t>(
                sizeof(RealtimeWireHeaderV2));
        header_->endian_marker = kRealtimeLittleEndianMarkerV2;
        header_->server_state =
            static_cast<std::uint32_t>(
                RealtimeServerStateV2::kInitializing);
        header_->total_mapping_bytes = mapping_bytes_;
        for (std::size_t index = 0U;
             index < config_.run_id.size();
             ++index) {
            header_->run_id[index] =
                std::to_integer<std::uint8_t>(
                    config_.run_id[index]);
        }
        header_->session_epoch = config_.session_epoch;
        header_->trade_date = config_.trade_date;
        header_->flags =
            config_.kline_windows.empty()
                ? 0U
                : kRealtimeHeaderKLineEnabledV2;
        header_->capacity =
            static_cast<std::uint32_t>(
                config_.directory->capacity());
        header_->window_count =
            static_cast<std::uint32_t>(
                config_.kline_windows.size());
        header_->catalog_scope =
            static_cast<std::uint32_t>(
                RealtimeCatalogScopeV2::kObservedOnly);
        header_->coverage_complete = 0U;
        StoreDigest(&header_->layout_digest, layout_digest_);
        header_->catalog_generation =
            initial_catalog_generation_;
        header_->data_state_generation =
            initial_data_state_generation_;
        StoreDigest(
            &header_->catalog_digest,
            initial_catalog_digest_);
        header_->region_count =
            static_cast<std::uint32_t>(
                kRealtimeWireRegionCountV2);
        header_->region_descriptor_bytes =
            static_cast<std::uint32_t>(
                sizeof(RealtimeWireRegionDescriptorV2));
        for (std::size_t index = 0U;
             index < regions_.size();
             ++index) {
            const LayoutRegion& source = regions_[index];
            RealtimeWireRegionDescriptorV2& destination =
                header_->regions[index];
            destination.kind =
                static_cast<std::uint32_t>(source.kind);
            destination.schema_major = kRealtimeWireMajorV2;
            destination.schema_minor = kRealtimeWireMinorV2;
            destination.offset = source.offset;
            destination.length = source.length;
            destination.element_stride = source.stride;
            destination.element_count = source.count;
            destination.capacity = source.capacity;
            destination.alignment = source.alignment;
        }
    }

    void InitializeWindows() noexcept {
        for (std::size_t index = 0U;
             index < config_.kline_windows.size();
             ++index) {
            window_rows()[index].window_id =
                config_.kline_windows[index].window_id;
            window_rows()[index].duration_ns =
                config_.kline_windows[index].duration_ns;
        }
    }

    [[nodiscard]] RealtimeSharedServiceCreateErrorV2 BindSocket(
        int* system_error_number) noexcept {
        const std::filesystem::path parent =
            config_.control_socket_path.parent_path();
        int directory_flags = O_RDONLY | O_DIRECTORY | O_CLOEXEC;
#ifdef O_NOFOLLOW
        directory_flags |= O_NOFOLLOW;
#endif
        const int directory_fd =
            ::open(parent.c_str(), directory_flags);
        struct stat directory_stat {};
        const bool directory_ok =
            directory_fd >= 0 &&
            ::fstat(directory_fd, &directory_stat) == 0 &&
            S_ISDIR(directory_stat.st_mode) &&
            directory_stat.st_uid == ::geteuid() &&
            (directory_stat.st_mode & 0077) == 0;
        const int directory_error =
            directory_fd < 0 ? errno : EACCES;
        if (directory_fd >= 0) {
            static_cast<void>(::close(directory_fd));
        }
        if (!directory_ok) {
            SetSystemError(system_error_number, directory_error);
            return RealtimeSharedServiceCreateErrorV2::
                kInvalidConfiguration;
        }
        struct stat existing {};
        if (::lstat(config_.control_socket_path.c_str(), &existing) ==
            0) {
            SetSystemError(system_error_number, EEXIST);
            return RealtimeSharedServiceCreateErrorV2::
                kSocketPathExists;
        }
        if (errno != ENOENT) {
            SetSystemError(system_error_number, errno);
            return RealtimeSharedServiceCreateErrorV2::
                kSocketBindFailed;
        }

        listener_fd_ = ::socket(
            AF_UNIX,
            SOCK_SEQPACKET | SOCK_NONBLOCK | SOCK_CLOEXEC,
            0);
        if (listener_fd_ < 0) {
            SetSystemError(system_error_number, errno);
            return RealtimeSharedServiceCreateErrorV2::
                kSocketCreateFailed;
        }
        sockaddr_un address{};
        address.sun_family = AF_UNIX;
        const std::string path =
            config_.control_socket_path.string();
        std::memcpy(
            address.sun_path, path.c_str(), path.size() + 1U);
        const mode_t old_mask = ::umask(0077);
        const int bind_result = ::bind(
            listener_fd_,
            reinterpret_cast<const sockaddr*>(&address),
            static_cast<socklen_t>(
                offsetof(sockaddr_un, sun_path) +
                path.size() + 1U));
        static_cast<void>(::umask(old_mask));
        if (bind_result != 0) {
            SetSystemError(system_error_number, errno);
            return RealtimeSharedServiceCreateErrorV2::
                kSocketBindFailed;
        }
        struct stat socket_stat {};
        if (::lstat(path.c_str(), &socket_stat) != 0 ||
            !S_ISSOCK(socket_stat.st_mode) ||
            socket_stat.st_uid != ::geteuid()) {
            SetSystemError(
                system_error_number,
                errno == 0 ? EACCES : errno);
            return RealtimeSharedServiceCreateErrorV2::
                kSocketBindFailed;
        }
        socket_device_ = socket_stat.st_dev;
        socket_inode_ = socket_stat.st_ino;
        socket_bound_ = true;
        if (::chmod(path.c_str(), 0600) != 0 ||
            ::listen(listener_fd_, 16) != 0) {
            SetSystemError(system_error_number, errno);
            SafeUnlinkSocket();
            return RealtimeSharedServiceCreateErrorV2::
                kSocketBindFailed;
        }
        stop_event_fd_ =
            ::eventfd(0U, EFD_CLOEXEC | EFD_NONBLOCK);
        if (stop_event_fd_ < 0) {
            SetSystemError(system_error_number, errno);
            return RealtimeSharedServiceCreateErrorV2::
                kSocketCreateFailed;
        }
        prefix_event_fd_ =
            ::eventfd(0U, EFD_CLOEXEC | EFD_NONBLOCK);
        if (prefix_event_fd_ < 0) {
            SetSystemError(system_error_number, errno);
            return RealtimeSharedServiceCreateErrorV2::
                kSocketCreateFailed;
        }
        return RealtimeSharedServiceCreateErrorV2::kNone;
    }

    void SafeUnlinkSocket() noexcept {
        if (!socket_bound_) {
            return;
        }
        struct stat current {};
        if (::lstat(
                config_.control_socket_path.c_str(),
                &current) == 0 &&
            current.st_dev == socket_device_ &&
            current.st_ino == socket_inode_ &&
            S_ISSOCK(current.st_mode)) {
            static_cast<void>(
                ::unlink(config_.control_socket_path.c_str()));
        }
        socket_bound_ = false;
    }

    void UpdateHeartbeatNow() noexcept {
        std::uint64_t now = 0U;
        if (header_ == nullptr ||
            !MonotonicNowNanoseconds(&now)) {
            MarkFailed();
            return;
        }
        Atomic(header_->heartbeat_monotonic_ns)
            .store(now, std::memory_order_release);
    }

    void ControlLoop() noexcept {
        std::array<pollfd, 3U> descriptors{};
        descriptors[0U].fd = listener_fd_;
        descriptors[0U].events = static_cast<short>(POLLIN);
        descriptors[1U].fd = stop_event_fd_;
        descriptors[1U].events = static_cast<short>(POLLIN);
        descriptors[2U].fd = prefix_event_fd_;
        descriptors[2U].events = static_cast<short>(POLLIN);
        while (!control_stop_requested_.load(
            std::memory_order_acquire)) {
            const int ready =
                ::poll(descriptors.data(), descriptors.size(), 1000);
            if (ready < 0) {
                if (errno == EINTR) {
                    continue;
                }
                MarkFailed();
                return;
            }
            if (ready == 0) {
                UpdateHeartbeatNow();
                continue;
            }
            if ((descriptors[1U].revents & POLLIN) != 0) {
                return;
            }
            if ((descriptors[0U].revents &
                 (POLLERR | POLLHUP | POLLNVAL)) != 0 ||
                (descriptors[1U].revents &
                 (POLLERR | POLLHUP | POLLNVAL)) != 0 ||
                (descriptors[2U].revents &
                 (POLLERR | POLLHUP | POLLNVAL)) != 0) {
                MarkFailed();
                return;
            }
            if ((descriptors[2U].revents & POLLIN) != 0) {
                std::uint64_t notifications = 0U;
                ssize_t read_result = -1;
                do {
                    read_result = ::read(
                        prefix_event_fd_,
                        &notifications,
                        sizeof(notifications));
                } while (read_result < 0 && errno == EINTR);
                if (read_result !=
                    static_cast<ssize_t>(
                        sizeof(notifications))) {
                    MarkFailed();
                    return;
                }
                prefix_notification_pending_.store(
                    false, std::memory_order_release);
                if (AdvanceContiguousTickPrefix(
                        kControlPrefixAdvanceBudget) &&
                    !RequestPrefixAdvance()) {
                    MarkFailed();
                    return;
                }
            }
            if ((descriptors[0U].revents & POLLIN) != 0) {
                for (;;) {
                    const int client = ::accept4(
                        listener_fd_,
                        nullptr,
                        nullptr,
                        SOCK_NONBLOCK | SOCK_CLOEXEC);
                    if (client < 0) {
                        if (errno == EAGAIN ||
                            errno == EWOULDBLOCK) {
                            break;
                        }
                        if (errno == EINTR) {
                            continue;
                        }
                        MarkFailed();
                        return;
                    }
                    HandleClient(client);
                    static_cast<void>(::close(client));
                }
            }
            UpdateHeartbeatNow();
        }
    }

    void HandleClient(int client) noexcept {
        ucred credentials{};
        socklen_t credentials_size =
            static_cast<socklen_t>(sizeof(credentials));
        if (::getsockopt(
                client,
                SOL_SOCKET,
                SO_PEERCRED,
                &credentials,
                &credentials_size) != 0 ||
            credentials_size != sizeof(credentials) ||
            credentials.uid != ::geteuid()) {
            return;
        }
        pollfd descriptor{};
        descriptor.fd = client;
        descriptor.events = static_cast<short>(POLLIN);
        int ready = -1;
        do {
            ready = ::poll(&descriptor, 1U, 250);
        } while (ready < 0 && errno == EINTR);
        if (ready <= 0 || (descriptor.revents & POLLIN) == 0) {
            return;
        }
        RealtimeControlRequestV2 request{};
        const ssize_t received = ::recv(
            client, &request, sizeof(request), MSG_TRUNC);
        RealtimeControlResponseV2 response{};
        response.magic = kRealtimeControlMagicV2;
        response.protocol_major = kRealtimeWireMajorV2;
        response.protocol_minor = kRealtimeWireMinorV2;
        response.message_bytes =
            static_cast<std::uint32_t>(sizeof(response));
        response.request_id = request.request_id;
        response.session_epoch = config_.session_epoch;
        response.total_mapping_bytes = mapping_bytes_;
        bool send_fd = false;
        if (received !=
                static_cast<ssize_t>(sizeof(request)) ||
            request.magic != kRealtimeControlMagicV2 ||
            request.message_bytes != sizeof(request) ||
            request.opcode !=
                static_cast<std::uint16_t>(
                    RealtimeControlOpcodeV2::kGetSession) ||
            request.flags != 0U || request.reserved0 != 0U ||
            request.reserved1 != 0U) {
            response.status =
                static_cast<std::uint16_t>(
                    RealtimeControlStatusV2::kInvalidRequest);
        } else if (
            request.protocol_major != kRealtimeWireMajorV2 ||
            request.protocol_minor != kRealtimeWireMinorV2) {
            response.status =
                static_cast<std::uint16_t>(
                    RealtimeControlStatusV2::
                        kUnsupportedVersion);
        } else if (failed()) {
            response.status =
                static_cast<std::uint16_t>(
                    RealtimeControlStatusV2::kUnavailable);
        } else {
            response.status =
                static_cast<std::uint16_t>(
                    RealtimeControlStatusV2::kOk);
            send_fd = true;
        }
        static_cast<void>(SendPacket(
            client,
            &response,
            sizeof(response),
            send_fd ? read_only_fd_ : -1));
    }

    [[nodiscard]] RealtimeWireInstrumentV2* instrument_rows()
        const noexcept {
        return reinterpret_cast<RealtimeWireInstrumentV2*>(
            static_cast<std::byte*>(mapping_) +
            regions_[0U].offset);
    }

    [[nodiscard]] std::byte* key_arena() const noexcept {
        return static_cast<std::byte*>(mapping_) +
               regions_[1U].offset;
    }

    [[nodiscard]] RealtimeWireKLineWindowV2* window_rows()
        const noexcept {
        return reinterpret_cast<RealtimeWireKLineWindowV2*>(
            static_cast<std::byte*>(mapping_) +
            regions_[2U].offset);
    }

    [[nodiscard]] RealtimeWireSnapshotSlotV2* snapshot_slots()
        const noexcept {
        return reinterpret_cast<RealtimeWireSnapshotSlotV2*>(
            static_cast<std::byte*>(mapping_) +
            regions_[3U].offset);
    }

    [[nodiscard]] RealtimeWireTickSlotV2* latest_tick_slots()
        const noexcept {
        return reinterpret_cast<RealtimeWireTickSlotV2*>(
            static_cast<std::byte*>(mapping_) +
            regions_[4U].offset);
    }

    [[nodiscard]] RealtimeWireKLineSlotV2* kline_slots()
        const noexcept {
        return reinterpret_cast<RealtimeWireKLineSlotV2*>(
            static_cast<std::byte*>(mapping_) +
            regions_[5U].offset);
    }

    [[nodiscard]] RealtimeWireTickSlotV2* ring_slots()
        const noexcept {
        return reinterpret_cast<RealtimeWireTickSlotV2*>(
            static_cast<std::byte*>(mapping_) +
            regions_[6U].offset);
    }

    void ReportFailure(
        const char* reason,
        std::size_t ordinal,
        std::uint32_t instrument_id,
        std::uint64_t sequence) noexcept {
        if (failure_reported_.test_and_set(
                std::memory_order_relaxed)) {
            return;
        }
        std::fprintf(
            stderr,
            "l2flow-ipc-v2: publication failed reason=%s "
            "ordinal=%zu instrument_id=%u sequence=%llu\n",
            reason == nullptr ? "unknown" : reason,
            ordinal,
            instrument_id,
            static_cast<unsigned long long>(sequence));
    }

    RealtimeSharedServiceConfigV2 config_;
    std::array<LayoutRegion, kRealtimeWireRegionCountV2> regions_{};
    common::Sha256Digest layout_digest_{};
    common::Sha256Digest initial_catalog_digest_{};
    std::uint64_t initial_catalog_generation_ = 0U;
    std::uint64_t initial_data_state_generation_ = 0U;

    int memfd_ = -1;
    int read_only_fd_ = -1;
    void* mapping_ = MAP_FAILED;
    std::uint64_t mapping_bytes_ = 0U;
    RealtimeWireHeaderV2* header_ = nullptr;
    std::atomic<std::uint64_t> key_arena_used_{0U};
    std::unique_ptr<std::atomic_flag[]> ring_locks_;
    std::atomic_flag failure_reported_ = ATOMIC_FLAG_INIT;
    std::atomic_flag kline_publication_in_progress_ =
        ATOMIC_FLAG_INIT;

    int listener_fd_ = -1;
    int stop_event_fd_ = -1;
    int prefix_event_fd_ = -1;
    dev_t socket_device_ = 0;
    ino_t socket_inode_ = 0;
    bool socket_bound_ = false;
    std::atomic<bool> prefix_notification_pending_{false};
    std::atomic<bool> control_stop_requested_{false};
    std::mutex control_stop_mutex_;
    std::thread control_thread_;
};

RealtimeSharedMarketServiceV2::RealtimeSharedMarketServiceV2(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

RealtimeSharedMarketServiceV2::~RealtimeSharedMarketServiceV2() =
    default;

RealtimeSharedServiceCreateErrorV2
RealtimeSharedMarketServiceV2::Create(
    RealtimeSharedServiceConfigV2 config,
    std::shared_ptr<RealtimeSharedMarketServiceV2>* output,
    int* system_error_number) noexcept {
    if (output == nullptr) {
        return RealtimeSharedServiceCreateErrorV2::kNullOutput;
    }
    output->reset();
    try {
        auto impl = std::make_unique<Impl>(std::move(config));
        const RealtimeSharedServiceCreateErrorV2 error =
            impl->Initialize(system_error_number);
        if (error != RealtimeSharedServiceCreateErrorV2::kNone) {
            return error;
        }
        output->reset(
            new RealtimeSharedMarketServiceV2(std::move(impl)));
        return RealtimeSharedServiceCreateErrorV2::kNone;
    } catch (const std::bad_alloc&) {
        return RealtimeSharedServiceCreateErrorV2::
            kResourceExhausted;
    } catch (...) {
        return RealtimeSharedServiceCreateErrorV2::
            kUnexpectedFailure;
    }
}

bool RealtimeSharedMarketServiceV2::Start(
    int* system_error_number) noexcept {
    return impl_ != nullptr &&
           impl_->Start(system_error_number);
}

bool RealtimeSharedMarketServiceV2::
    PublishObservedInstrumentBinding(
        const market::ObservedInstrumentBindResultV2& binding)
        noexcept {
    return impl_ != nullptr &&
           impl_->PublishObservedInstrumentBinding(binding);
}

bool RealtimeSharedMarketServiceV2::PublishApplied(
    std::size_t ordinal,
    const market::RealtimeHistoryRecordV1& record) noexcept {
    return impl_ != nullptr &&
           impl_->PublishApplied(ordinal, record);
}

bool RealtimeSharedMarketServiceV2::PublishProcessingProgress(
    realtime::ProcessingProgressV2 progress) noexcept {
    return impl_ != nullptr &&
           impl_->PublishProcessingProgress(progress);
}

void RealtimeSharedMarketServiceV2::MarkCoverageLost() noexcept {
    if (impl_ != nullptr) {
        impl_->MarkCoverageLost();
    }
}

bool RealtimeSharedMarketServiceV2::PublishKLineGeneration(
    const market::RealtimeKLineGenerationV1& generation) noexcept {
    return impl_ != nullptr &&
           impl_->PublishKLineGeneration(generation);
}

void RealtimeSharedMarketServiceV2::MarkDraining() noexcept {
    if (impl_ != nullptr) {
        impl_->MarkDraining();
    }
}

bool RealtimeSharedMarketServiceV2::MarkStoppedClean(
    std::uint64_t final_admitted_tick_sequence) noexcept {
    return impl_ != nullptr &&
           impl_->MarkStoppedClean(final_admitted_tick_sequence);
}

void RealtimeSharedMarketServiceV2::MarkFailed() noexcept {
    if (impl_ != nullptr) {
        impl_->MarkFailed();
    }
}

void RealtimeSharedMarketServiceV2::StopControl() noexcept {
    if (impl_ != nullptr) {
        impl_->StopControl();
    }
}

std::uint64_t RealtimeSharedMarketServiceV2::mapping_bytes()
    const noexcept {
    return impl_ == nullptr ? 0U : impl_->mapping_bytes();
}

std::uint64_t
RealtimeSharedMarketServiceV2::key_arena_used_bytes()
    const noexcept {
    return impl_ == nullptr
               ? 0U
               : impl_->key_arena_used_bytes();
}

bool RealtimeSharedMarketServiceV2::failed() const noexcept {
    return impl_ == nullptr || impl_->failed();
}

const std::filesystem::path&
RealtimeSharedMarketServiceV2::control_socket_path()
    const noexcept {
    return impl_->socket_path();
}

}  // namespace l2flow::ipc
