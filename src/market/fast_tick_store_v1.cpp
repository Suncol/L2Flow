#include "l2flow/market/fast_tick_store_v1.h"

#include "l2flow/market/kline_types_v1.h"

#include <algorithm>
#include <atomic>
#include <limits>
#include <new>
#include <type_traits>
#include <utility>

namespace l2flow::market {
namespace {

[[nodiscard]] bool ZeroIdentity(
    const l2flow::common::Identity128& identity) noexcept {
    return std::all_of(
        identity.begin(), identity.end(),
        [](std::byte value) noexcept { return value == std::byte{0U}; });
}

[[nodiscard]] bool ValidTradeDate(std::uint32_t value) noexcept {
    const std::uint32_t year = value / 10'000U;
    const std::uint32_t month = (value / 100U) % 100U;
    const std::uint32_t day = value % 100U;
    if (year < 1992U || year > 2200U || month == 0U ||
        month > 12U || day == 0U) {
        return false;
    }
    constexpr std::uint32_t days_by_month[12U] = {
        31U, 28U, 31U, 30U, 31U, 30U,
        31U, 31U, 30U, 31U, 30U, 31U};
    std::uint32_t maximum_day = days_by_month[month - 1U];
    const bool leap =
        (year % 4U == 0U && year % 100U != 0U) ||
        year % 400U == 0U;
    if (month == 2U && leap) {
        maximum_day = 29U;
    }
    return day <= maximum_day;
}

[[nodiscard]] bool ValidCompact(
    const CompactFastTickV1& compact) noexcept {
    if (compact.source_stream_id == 0U ||
        compact.source_sequence == 0U || compact.arrival_id == 0U ||
        compact.arrival_id ==
            std::numeric_limits<std::uint64_t>::max() ||
        !ValidTradeDate(compact.trade_date) ||
        compact.instrument_id == 0U ||
        compact.ordinal >= static_cast<std::size_t>(
            std::numeric_limits<std::uint32_t>::max()) ||
        compact.instrument_id !=
            static_cast<std::uint32_t>(compact.ordinal + 1U) ||
        compact.business_sequence.channel <= 0 ||
        compact.business_sequence.value <= 0 ||
        !IsTickEventKindV1(compact.kind)) {
        return false;
    }
    if (compact.source == FastTickSourceV1::kShanghaiTick) {
        return compact.market == MarketV1::kShanghai &&
               compact.kind == MarketEventKindV1::kShanghaiTick;
    }
    if (compact.source == FastTickSourceV1::kShenzhenTick) {
        return compact.market == MarketV1::kShenzhen &&
               (compact.kind == MarketEventKindV1::kShenzhenOrder ||
                compact.kind ==
                    MarketEventKindV1::kShenzhenTransaction);
    }
    return false;
}

void FillCommon(
    const DecodedMarketCommonV1& common,
    FastTickSourceV1 source,
    std::uint64_t arrival_id,
    CompactFastTickV1* output) noexcept {
    output->source = source;
    output->market = common.market;
    output->kind = common.kind;
    output->source_stream_id = common.origin.source_stream_id;
    output->source_sequence = common.origin.source_sequence;
    output->arrival_id = arrival_id;
    output->vendor_sequence_id = common.origin.vendor_sequence_id;
    output->trade_date = common.origin.trade_date;
    output->instrument_id = common.instrument_id;
    output->ordinal = common.ordinal;
    output->event_time_ns_since_midnight =
        common.exchange_time.nanoseconds_since_midnight;
    output->event_time_unix_ns = common.exchange_time.unix_nanoseconds;
    output->recv_realtime_ns = common.origin.recv_realtime_ns;
    output->recv_monotonic_ns = common.origin.recv_monotonic_ns;
    output->vendor_local_time_raw =
        common.vendor_local_time.raw_hhmmssmmm;
    output->vendor_local_time_ns_since_midnight =
        common.vendor_local_time.nanoseconds_since_midnight;
    output->quantity_unit = common.quantity_unit;
    output->quality_flags = common.quality_flags;
    output->market_notices = common.market_notices;
    output->event_time_valid = common.exchange_time.valid;
    output->event_time_unix_ns_valid =
        common.exchange_time.unix_nanoseconds_valid;
    output->vendor_local_time_valid = common.vendor_local_time.valid;
}

void FillFields(
    const TickFieldsV1& fields,
    CompactFastTickV1* output) noexcept {
    output->action = fields.action;
    output->side = fields.side;
    output->order_type = fields.order_type;
    output->aggressor = fields.aggressor;
    output->phase = fields.phase;
    output->price_p6 = fields.price.normalized_p6;
    output->trade_amount_p6 = fields.trade_amount.normalized_p6;
    output->quantity_raw = fields.quantity.raw;
    output->matched_quantity_raw = fields.matched_quantity.raw;
    output->primary_order_id = fields.primary_order_id;
    output->buy_order_id = fields.buy_order_id;
    output->sell_order_id = fields.sell_order_id;
    output->quantity_scale = fields.quantity.scale;
    output->validity_bitmap = fields.validity_bitmap;
}

[[nodiscard]] bool DecodedMatchesCompact(
    const DecodedFastTickV1& decoded,
    const CompactFastTickV1& compact) noexcept {
    return std::visit(
        [&compact](const auto& value) noexcept {
            using Event = std::decay_t<decltype(value)>;
            CompactFastTickV1 expected{};
            FillCommon(
                value.common,
                compact.source,
                compact.arrival_id,
                &expected);
            FillFields(value.fields, &expected);
            if constexpr (std::is_same_v<Event, ShanghaiTickV1>) {
                expected.business_sequence = BusinessSequenceV1{
                    value.channel, value.business_index};
            } else if constexpr (
                std::is_same_v<Event, ShenzhenOrderV1>) {
                if (value.channel > static_cast<std::uint32_t>(
                                        std::numeric_limits<
                                            std::int32_t>::max())) {
                    return false;
                }
                expected.business_sequence = BusinessSequenceV1{
                    static_cast<std::int32_t>(value.channel),
                    value.application_sequence};
                expected.raw_code_1 = value.raw_side;
                expected.raw_code_2 = value.raw_order_type;
            } else {
                if (value.channel > static_cast<std::uint32_t>(
                                        std::numeric_limits<
                                            std::int32_t>::max())) {
                    return false;
                }
                expected.business_sequence = BusinessSequenceV1{
                    static_cast<std::int32_t>(value.channel),
                    value.application_sequence};
                expected.raw_code_1 = value.raw_execution_type;
            }
            return expected == compact;
        },
        decoded);
}

}  // namespace

std::string_view FastTickProjectionErrorNameV1(
    FastTickProjectionErrorV1 error) noexcept {
    switch (error) {
        case FastTickProjectionErrorV1::kNone:
            return "none";
        case FastTickProjectionErrorV1::kNullOutput:
            return "null_output";
        case FastTickProjectionErrorV1::kNotTick:
            return "not_tick";
        case FastTickProjectionErrorV1::kSourceMismatch:
            return "source_mismatch";
        case FastTickProjectionErrorV1::kInvalidIdentity:
            return "invalid_identity";
        case FastTickProjectionErrorV1::kInvalidBusinessSequence:
            return "invalid_business_sequence";
    }
    return "unknown";
}

FastTickProjectionErrorV1 ProjectFastTickV1(
    DecodedMarketEventV1&& decoded,
    FastTickSourceV1 source,
    std::uint64_t arrival_id,
    DecodedFastTickV1* owned_tick,
    CompactFastTickV1* compact) noexcept {
    if (owned_tick == nullptr || compact == nullptr) {
        return FastTickProjectionErrorV1::kNullOutput;
    }
    if (arrival_id == 0U ||
        arrival_id == std::numeric_limits<std::uint64_t>::max()) {
        return FastTickProjectionErrorV1::kInvalidIdentity;
    }

    CompactFastTickV1 projected{};
    if (auto* tick = std::get_if<ShanghaiTickV1>(&decoded);
        tick != nullptr) {
        if (source != FastTickSourceV1::kShanghaiTick ||
            tick->common.market != MarketV1::kShanghai ||
            tick->common.kind != MarketEventKindV1::kShanghaiTick) {
            return FastTickProjectionErrorV1::kSourceMismatch;
        }
        FillCommon(tick->common, source, arrival_id, &projected);
        FillFields(tick->fields, &projected);
        projected.business_sequence =
            BusinessSequenceV1{tick->channel, tick->business_index};
        if (!ValidCompact(projected)) {
            return tick->channel <= 0 || tick->business_index <= 0
                       ? FastTickProjectionErrorV1::
                             kInvalidBusinessSequence
                       : FastTickProjectionErrorV1::kInvalidIdentity;
        }
        DecodedFastTickV1 moved(std::move(*tick));
        *compact = projected;
        *owned_tick = std::move(moved);
        return FastTickProjectionErrorV1::kNone;
    }

    if (auto* order = std::get_if<ShenzhenOrderV1>(&decoded);
        order != nullptr) {
        if (source != FastTickSourceV1::kShenzhenTick ||
            order->common.market != MarketV1::kShenzhen ||
            order->common.kind != MarketEventKindV1::kShenzhenOrder ||
            order->channel >
                static_cast<std::uint32_t>(
                    std::numeric_limits<std::int32_t>::max())) {
            return FastTickProjectionErrorV1::kSourceMismatch;
        }
        FillCommon(order->common, source, arrival_id, &projected);
        FillFields(order->fields, &projected);
        projected.business_sequence = BusinessSequenceV1{
            static_cast<std::int32_t>(order->channel),
            order->application_sequence};
        projected.raw_code_1 = order->raw_side;
        projected.raw_code_2 = order->raw_order_type;
        if (!ValidCompact(projected)) {
            return order->channel == 0U ||
                           order->application_sequence <= 0
                       ? FastTickProjectionErrorV1::
                             kInvalidBusinessSequence
                       : FastTickProjectionErrorV1::kInvalidIdentity;
        }
        DecodedFastTickV1 moved(std::move(*order));
        *compact = projected;
        *owned_tick = std::move(moved);
        return FastTickProjectionErrorV1::kNone;
    }

    if (auto* transaction =
            std::get_if<ShenzhenTransactionV1>(&decoded);
        transaction != nullptr) {
        if (source != FastTickSourceV1::kShenzhenTick ||
            transaction->common.market != MarketV1::kShenzhen ||
            transaction->common.kind !=
                MarketEventKindV1::kShenzhenTransaction ||
            transaction->channel >
                static_cast<std::uint32_t>(
                    std::numeric_limits<std::int32_t>::max())) {
            return FastTickProjectionErrorV1::kSourceMismatch;
        }
        FillCommon(
            transaction->common, source, arrival_id, &projected);
        FillFields(transaction->fields, &projected);
        projected.business_sequence = BusinessSequenceV1{
            static_cast<std::int32_t>(transaction->channel),
            transaction->application_sequence};
        projected.raw_code_1 = transaction->raw_execution_type;
        if (!ValidCompact(projected)) {
            return transaction->channel == 0U ||
                           transaction->application_sequence <= 0
                       ? FastTickProjectionErrorV1::
                             kInvalidBusinessSequence
                       : FastTickProjectionErrorV1::kInvalidIdentity;
        }
        DecodedFastTickV1 moved(std::move(*transaction));
        *compact = projected;
        *owned_tick = std::move(moved);
        return FastTickProjectionErrorV1::kNone;
    }
    return FastTickProjectionErrorV1::kNotTick;
}

bool IsKLineTradeV1(const CompactFastTickV1& tick) noexcept {
    constexpr std::uint32_t required =
        kTickPriceValidV1 | kTickQuantityValidV1 |
        kTickExchangeTimeValidV1;
    const bool supported_source =
        (tick.source == FastTickSourceV1::kShanghaiTick &&
         tick.market == MarketV1::kShanghai &&
         tick.kind == MarketEventKindV1::kShanghaiTick) ||
        (tick.source == FastTickSourceV1::kShenzhenTick &&
         tick.market == MarketV1::kShenzhen &&
         tick.kind == MarketEventKindV1::kShenzhenTransaction);
    return supported_source && tick.action == TickActionV1::kTrade &&
           (tick.validity_bitmap & required) == required &&
           tick.price_p6 > 0 && tick.quantity_raw > 0 &&
           tick.event_time_valid &&
           tick.event_time_unix_ns_valid &&
           tick.event_time_ns_since_midnight <
               kKLineNanosecondsPerDayV1;
}

bool SameFastTickPayloadV1(
    const CompactFastTickV1& lhs,
    const CompactFastTickV1& rhs) noexcept {
    return lhs.market == rhs.market && lhs.kind == rhs.kind &&
           lhs.trade_date == rhs.trade_date &&
           lhs.instrument_id == rhs.instrument_id &&
           lhs.ordinal == rhs.ordinal &&
           lhs.business_sequence == rhs.business_sequence &&
           lhs.event_time_ns_since_midnight ==
               rhs.event_time_ns_since_midnight &&
           lhs.event_time_unix_ns == rhs.event_time_unix_ns &&
           lhs.vendor_local_time_raw == rhs.vendor_local_time_raw &&
           lhs.vendor_local_time_ns_since_midnight ==
               rhs.vendor_local_time_ns_since_midnight &&
           lhs.action == rhs.action && lhs.side == rhs.side &&
           lhs.order_type == rhs.order_type &&
           lhs.aggressor == rhs.aggressor && lhs.phase == rhs.phase &&
           lhs.price_p6 == rhs.price_p6 &&
           lhs.trade_amount_p6 == rhs.trade_amount_p6 &&
           lhs.quantity_raw == rhs.quantity_raw &&
           lhs.matched_quantity_raw == rhs.matched_quantity_raw &&
           lhs.primary_order_id == rhs.primary_order_id &&
           lhs.buy_order_id == rhs.buy_order_id &&
           lhs.sell_order_id == rhs.sell_order_id &&
           lhs.quantity_scale == rhs.quantity_scale &&
           lhs.quantity_unit == rhs.quantity_unit &&
           lhs.validity_bitmap == rhs.validity_bitmap &&
           lhs.quality_flags == rhs.quality_flags &&
           lhs.market_notices == rhs.market_notices &&
           lhs.raw_code_1 == rhs.raw_code_1 &&
           lhs.raw_code_2 == rhs.raw_code_2 &&
           lhs.event_time_valid == rhs.event_time_valid &&
           lhs.event_time_unix_ns_valid ==
               rhs.event_time_unix_ns_valid &&
           lhs.vendor_local_time_valid ==
               rhs.vendor_local_time_valid;
}

FastTickRecordV1::FastTickRecordV1(
    std::uint64_t instrument_tick_sequence,
    CompactFastTickV1 compact,
    DecodedFastTickV1&& decoded) noexcept
    : instrument_tick_sequence_(instrument_tick_sequence),
      compact_(compact),
      decoded_(std::move(decoded)) {}

std::string_view FastTickStoreCreateErrorNameV1(
    FastTickStoreCreateErrorV1 error) noexcept {
    switch (error) {
        case FastTickStoreCreateErrorV1::kNone:
            return "none";
        case FastTickStoreCreateErrorV1::kNullOutput:
            return "null_output";
        case FastTickStoreCreateErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case FastTickStoreCreateErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

std::string_view FastTickStoreAppendErrorNameV1(
    FastTickStoreAppendErrorV1 error) noexcept {
    switch (error) {
        case FastTickStoreAppendErrorV1::kNone:
            return "none";
        case FastTickStoreAppendErrorV1::kInvalidInput:
            return "invalid_input";
        case FastTickStoreAppendErrorV1::kWrongWorker:
            return "wrong_worker";
        case FastTickStoreAppendErrorV1::kCoverageLost:
            return "coverage_lost";
        case FastTickStoreAppendErrorV1::kRecordCapacity:
            return "record_capacity";
        case FastTickStoreAppendErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

std::string_view FastTickStoreQueryErrorNameV1(
    FastTickStoreQueryErrorV1 error) noexcept {
    switch (error) {
        case FastTickStoreQueryErrorV1::kNone:
            return "none";
        case FastTickStoreQueryErrorV1::kNullOutput:
            return "null_output";
        case FastTickStoreQueryErrorV1::kInvalidArgument:
            return "invalid_argument";
        case FastTickStoreQueryErrorV1::kNotFound:
            return "not_found";
        case FastTickStoreQueryErrorV1::kBatchLimitExceeded:
            return "batch_limit_exceeded";
        case FastTickStoreQueryErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

class FastTickStoreChunkV1 final {
public:
    explicit FastTickStoreChunkV1(std::size_t capacity)
        : capacity_(capacity),
          storage_(::operator new(
              sizeof(FastTickRecordV1) * capacity_,
              std::align_val_t{alignof(FastTickRecordV1)})) {}

    FastTickStoreChunkV1(const FastTickStoreChunkV1&) = delete;
    FastTickStoreChunkV1& operator=(
        const FastTickStoreChunkV1&) = delete;

    ~FastTickStoreChunkV1() {
        const std::size_t count =
            published_.load(std::memory_order_relaxed);
        for (std::size_t index = 0U; index < count; ++index) {
            At(index)->~FastTickRecordV1();
        }
        ::operator delete(
            storage_,
            std::align_val_t{alignof(FastTickRecordV1)});
    }

    [[nodiscard]] FastTickRecordV1* At(
        std::size_t index) noexcept {
        auto* const bytes = static_cast<std::byte*>(storage_);
        return std::launder(reinterpret_cast<FastTickRecordV1*>(
            bytes + index * sizeof(FastTickRecordV1)));
    }

    [[nodiscard]] const FastTickRecordV1* At(
        std::size_t index) const noexcept {
        const auto* const bytes =
            static_cast<const std::byte*>(storage_);
        return std::launder(
            reinterpret_cast<const FastTickRecordV1*>(
                bytes + index * sizeof(FastTickRecordV1)));
    }

    std::size_t capacity_ = 0U;
    void* storage_ = nullptr;
    std::atomic<std::size_t> published_{0U};
    std::atomic<FastTickStoreChunkV1*> next_{nullptr};
};

class FastTickStoreV1::Impl final {
public:

    struct InstrumentRow final {
        std::atomic<FastTickStoreChunkV1*> head{nullptr};
        FastTickStoreChunkV1* writer_tail = nullptr;
        std::size_t writer_offset = 0U;
        std::vector<FastTickStoreChunkV1*> chunks;
        std::size_t next_chunk = 0U;
        std::uint64_t record_limit = 0U;
        std::uint64_t used_records = 0U;
        std::atomic<const FastTickRecordV1*> latest{nullptr};
        std::atomic<std::uint64_t> published_tail{0U};
        std::atomic<bool> coverage_complete{true};
    };

    struct WorkerPool final {
        std::vector<std::unique_ptr<FastTickStoreChunkV1>> chunks;
    };

    explicit Impl(FastTickStoreConfigV1 config)
        : config_(std::move(config)),
          rows_(std::make_unique<InstrumentRow[]>(
              config_.instrument_count)),
          workers_(config_.worker_count) {}

    [[nodiscard]] FastTickStoreChunkV1* AcquireChunk(
        std::size_t ordinal) noexcept {
        InstrumentRow& row = rows_[ordinal];
        if (row.next_chunk >= row.chunks.size()) {
            return nullptr;
        }
        FastTickStoreChunkV1* const result =
            row.chunks[row.next_chunk];
        ++row.next_chunk;
        allocated_chunks_.fetch_add(1U, std::memory_order_relaxed);
        return result;
    }

    FastTickStoreConfigV1 config_{};
    std::unique_ptr<InstrumentRow[]> rows_;
    std::vector<WorkerPool> workers_;
    std::uint64_t chunk_capacity_ = 0U;
    std::atomic<std::uint64_t> appended_records_{0U};
    std::atomic<std::uint64_t> failed_appends_{0U};
    std::atomic<std::uint64_t> allocated_chunks_{0U};
    std::atomic<bool> any_coverage_lost_{false};
};

class FastTickCursorV1::Impl final {
public:
    const FastTickStoreChunkV1* chunk = nullptr;
    std::size_t offset = 0U;
    std::uint64_t next_row = 1U;
    std::uint64_t target = 0U;
    std::size_t maximum_records_per_read = 0U;
};

FastTickCursorV1::FastTickCursorV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

FastTickCursorV1::FastTickCursorV1(FastTickCursorV1&&) noexcept =
    default;
FastTickCursorV1& FastTickCursorV1::operator=(
    FastTickCursorV1&&) noexcept = default;
FastTickCursorV1::~FastTickCursorV1() = default;

FastTickStoreQueryErrorV1 FastTickCursorV1::ReadBatch(
    std::span<const FastTickRecordV1*> output,
    std::size_t* written) noexcept {
    if (written == nullptr) {
        return FastTickStoreQueryErrorV1::kNullOutput;
    }
    *written = 0U;
    if (impl_ == nullptr || output.empty()) {
        return FastTickStoreQueryErrorV1::kInvalidArgument;
    }
    if (output.size() > impl_->maximum_records_per_read) {
        return FastTickStoreQueryErrorV1::kBatchLimitExceeded;
    }
    while (*written < output.size() &&
           impl_->next_row <= impl_->target) {
        if (impl_->chunk == nullptr) {
            return FastTickStoreQueryErrorV1::kResourceExhausted;
        }
        const std::size_t published = impl_->chunk->published_.load(
            std::memory_order_acquire);
        if (impl_->offset >= published) {
            impl_->chunk = impl_->chunk->next_.load(
                std::memory_order_acquire);
            impl_->offset = 0U;
            continue;
        }
        output[*written] = impl_->chunk->At(impl_->offset);
        ++(*written);
        ++impl_->offset;
        ++impl_->next_row;
    }
    return FastTickStoreQueryErrorV1::kNone;
}

bool FastTickCursorV1::done() const noexcept {
    return impl_ == nullptr || impl_->next_row > impl_->target;
}

std::uint64_t FastTickCursorV1::next_arrival_row() const noexcept {
    return impl_ == nullptr ? 0U : impl_->next_row;
}

std::uint64_t FastTickCursorV1::target_tail() const noexcept {
    return impl_ == nullptr ? 0U : impl_->target;
}

FastTickStoreV1::FastTickStoreV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

FastTickStoreV1::~FastTickStoreV1() = default;

FastTickStoreCreateErrorV1 FastTickStoreV1::Create(
    FastTickStoreConfigV1 config,
    std::unique_ptr<FastTickStoreV1>* output) noexcept {
    if (output == nullptr) {
        return FastTickStoreCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (ZeroIdentity(config.session_id) ||
        !ValidTradeDate(config.trade_date) ||
        config.instrument_count == 0U || config.worker_count == 0U ||
        config.instrument_count > static_cast<std::size_t>(
            std::numeric_limits<std::uint32_t>::max()) ||
        config.worker_count > config.instrument_count ||
        config.maximum_session_records < config.instrument_count ||
        config.maximum_session_records ==
            std::numeric_limits<std::uint64_t>::max() ||
        config.records_per_chunk == 0U ||
        config.records_per_chunk >
            kFastTickMaximumRecordsPerChunkV1 ||
        config.maximum_records_per_read == 0U ||
        config.tick_routes.size() != config.instrument_count ||
        (!config.instrument_record_capacities.empty() &&
         config.instrument_record_capacities.size() !=
             config.instrument_count)) {
        return FastTickStoreCreateErrorV1::kInvalidConfiguration;
    }
    for (std::size_t ordinal = 0U;
         ordinal < config.tick_routes.size(); ++ordinal) {
        if (config.tick_routes[ordinal] >= config.worker_count ||
            ordinal >= static_cast<std::size_t>(
                           std::numeric_limits<std::uint32_t>::max())) {
            return FastTickStoreCreateErrorV1::kInvalidConfiguration;
        }
    }
    if (config.instrument_record_capacities.empty()) {
        try {
            config.instrument_record_capacities.resize(
                config.instrument_count);
        } catch (...) {
            return FastTickStoreCreateErrorV1::kResourceExhausted;
        }
        const std::uint64_t base =
            config.maximum_session_records / config.instrument_count;
        const std::uint64_t remainder =
            config.maximum_session_records % config.instrument_count;
        for (std::size_t ordinal = 0U;
             ordinal < config.instrument_count; ++ordinal) {
            config.instrument_record_capacities[ordinal] =
                base + (ordinal < remainder ? 1U : 0U);
        }
    } else {
        std::uint64_t sum = 0U;
        for (std::uint64_t capacity :
             config.instrument_record_capacities) {
            if (capacity == 0U ||
                sum > std::numeric_limits<std::uint64_t>::max() -
                          capacity) {
                return FastTickStoreCreateErrorV1::
                    kInvalidConfiguration;
            }
            sum += capacity;
        }
        if (sum != config.maximum_session_records) {
            return FastTickStoreCreateErrorV1::kInvalidConfiguration;
        }
    }
    try {
        auto impl = std::make_unique<Impl>(std::move(config));
        for (std::size_t ordinal = 0U;
             ordinal < impl->config_.instrument_count; ++ordinal) {
            Impl::InstrumentRow& row = impl->rows_[ordinal];
            row.record_limit =
                impl->config_.instrument_record_capacities[ordinal];
            const std::uint64_t records_per_chunk =
                impl->config_.records_per_chunk;
            const std::uint64_t chunk_count =
                row.record_limit / records_per_chunk +
                (row.record_limit % records_per_chunk == 0U ? 0U : 1U);
            if (chunk_count > static_cast<std::uint64_t>(
                                  std::numeric_limits<std::size_t>::max())) {
                return FastTickStoreCreateErrorV1::kResourceExhausted;
            }
            row.chunks.reserve(static_cast<std::size_t>(chunk_count));
            Impl::WorkerPool& pool = impl->workers_[
                impl->config_.tick_routes[ordinal]];
            std::uint64_t remaining = row.record_limit;
            while (remaining != 0U) {
                const std::uint64_t capacity = std::min<std::uint64_t>(
                    remaining, impl->config_.records_per_chunk);
                auto chunk = std::make_unique<FastTickStoreChunkV1>(
                    static_cast<std::size_t>(capacity));
                row.chunks.push_back(chunk.get());
                pool.chunks.push_back(std::move(chunk));
                ++impl->chunk_capacity_;
                remaining -= capacity;
            }
        }
        output->reset(new FastTickStoreV1(std::move(impl)));
        return FastTickStoreCreateErrorV1::kNone;
    } catch (...) {
        return FastTickStoreCreateErrorV1::kResourceExhausted;
    }
}

FastTickStoreQueryErrorV1 FastTickStoreV1::ResolveRoute(
    std::size_t ordinal,
    std::uint32_t instrument_id,
    FastTickRouteTokenV1* output) const noexcept {
    if (output == nullptr) {
        return FastTickStoreQueryErrorV1::kNullOutput;
    }
    *output = {};
    if (impl_ == nullptr || ordinal >= impl_->config_.instrument_count ||
        instrument_id == 0U ||
        instrument_id != static_cast<std::uint32_t>(ordinal + 1U)) {
        return FastTickStoreQueryErrorV1::kInvalidArgument;
    }
    output->instrument_id = instrument_id;
    output->ordinal = ordinal;
    output->worker = impl_->config_.tick_routes[ordinal];
    return FastTickStoreQueryErrorV1::kNone;
}

FastTickStoreAppendErrorV1 FastTickStoreV1::Append(
    std::uint32_t worker,
    const FastTickRouteTokenV1& route,
    CompactFastTickV1 compact,
    DecodedFastTickV1&& decoded,
    const FastTickRecordV1** appended_record) noexcept {
    if (appended_record != nullptr) {
        *appended_record = nullptr;
    }
    if (impl_ == nullptr || !ValidCompact(compact) ||
        compact.trade_date != impl_->config_.trade_date ||
        route.ordinal >= impl_->config_.instrument_count ||
        route.instrument_id != compact.instrument_id ||
        route.instrument_id !=
            static_cast<std::uint32_t>(route.ordinal + 1U) ||
        !DecodedMatchesCompact(decoded, compact)) {
        return FastTickStoreAppendErrorV1::kInvalidInput;
    }
    if (worker >= impl_->config_.worker_count ||
        route.worker != worker ||
        impl_->config_.tick_routes[route.ordinal] != worker) {
        return FastTickStoreAppendErrorV1::kWrongWorker;
    }
    Impl::InstrumentRow& row = impl_->rows_[route.ordinal];
    if (!row.coverage_complete.load(std::memory_order_acquire)) {
        return FastTickStoreAppendErrorV1::kCoverageLost;
    }
    const FastTickRecordV1* const prior =
        row.latest.load(std::memory_order_acquire);
    if (prior != nullptr &&
        compact.arrival_id <= prior->compact().arrival_id) {
        row.coverage_complete.store(false, std::memory_order_release);
        impl_->any_coverage_lost_.store(true, std::memory_order_release);
        impl_->failed_appends_.fetch_add(1U, std::memory_order_relaxed);
        return FastTickStoreAppendErrorV1::kInvalidInput;
    }

    if (row.used_records >= row.record_limit) {
        row.coverage_complete.store(false, std::memory_order_release);
        impl_->any_coverage_lost_.store(true, std::memory_order_release);
        impl_->failed_appends_.fetch_add(1U, std::memory_order_relaxed);
        return FastTickStoreAppendErrorV1::kRecordCapacity;
    }
    if (row.writer_tail == nullptr ||
        row.writer_offset >= row.writer_tail->capacity_) {
        FastTickStoreChunkV1* const chunk =
            impl_->AcquireChunk(route.ordinal);
        if (chunk == nullptr) {
            row.coverage_complete.store(false, std::memory_order_release);
            impl_->any_coverage_lost_.store(
                true, std::memory_order_release);
            impl_->failed_appends_.fetch_add(
                1U, std::memory_order_relaxed);
            return FastTickStoreAppendErrorV1::kResourceExhausted;
        }
        if (row.writer_tail == nullptr) {
            row.head.store(chunk, std::memory_order_release);
        } else {
            row.writer_tail->next_.store(
                chunk, std::memory_order_release);
        }
        row.writer_tail = chunk;
        row.writer_offset = 0U;
    }

    const std::uint64_t sequence =
        row.published_tail.load(std::memory_order_relaxed) + 1U;
    FastTickRecordV1* const record =
        row.writer_tail->At(row.writer_offset);
    ::new (static_cast<void*>(record)) FastTickRecordV1(
        sequence, compact, std::move(decoded));
    ++row.writer_offset;
    row.writer_tail->published_.store(
        row.writer_offset, std::memory_order_release);
    row.published_tail.store(sequence, std::memory_order_release);
    // latest is the publication handshake used by derived workers. Keeping
    // it after the history tail guarantees PublishedThrough can never make a
    // derived row visible before the corresponding FAST cursor row.
    row.latest.store(record, std::memory_order_release);
    ++row.used_records;
    impl_->appended_records_.fetch_add(1U, std::memory_order_relaxed);
    if (appended_record != nullptr) {
        *appended_record = record;
    }
    return FastTickStoreAppendErrorV1::kNone;
}

FastTickStoreQueryErrorV1 FastTickStoreV1::Latest(
    std::uint32_t instrument_id,
    const FastTickRecordV1** output) const noexcept {
    if (output == nullptr) {
        return FastTickStoreQueryErrorV1::kNullOutput;
    }
    *output = nullptr;
    if (impl_ == nullptr || instrument_id == 0U ||
        instrument_id > impl_->config_.instrument_count) {
        return FastTickStoreQueryErrorV1::kInvalidArgument;
    }
    const FastTickRecordV1* const latest =
        impl_->rows_[instrument_id - 1U].latest.load(
            std::memory_order_acquire);
    if (latest == nullptr) {
        return FastTickStoreQueryErrorV1::kNotFound;
    }
    *output = latest;
    return FastTickStoreQueryErrorV1::kNone;
}

FastTickStoreQueryErrorV1 FastTickStoreV1::Status(
    std::uint32_t instrument_id,
    FastTickInstrumentStatusV1* output) const noexcept {
    if (output == nullptr) {
        return FastTickStoreQueryErrorV1::kNullOutput;
    }
    *output = {};
    if (impl_ == nullptr || instrument_id == 0U ||
        instrument_id > impl_->config_.instrument_count) {
        return FastTickStoreQueryErrorV1::kInvalidArgument;
    }
    const Impl::InstrumentRow& row =
        impl_->rows_[instrument_id - 1U];
    output->instrument_id = instrument_id;
    output->record_capacity = row.record_limit;
    output->coverage_start = 1U;
    output->coverage_from_open = impl_->config_.coverage_from_open;
    output->coverage_complete = row.coverage_complete.load(
        std::memory_order_acquire);
    // latest is stored after the append-only tail. Sampling it directly
    // yields either the prior complete publication or the new one without
    // spinning while a writer is descheduled between the two stores.
    const FastTickRecordV1* const latest = row.latest.load(
        std::memory_order_acquire);
    if (latest != nullptr) {
        output->published_tail = latest->instrument_tick_sequence();
        output->latest_arrival_id = latest->compact().arrival_id;
        output->latest_source_anchor =
            latest->compact().business_sequence;
    }
    return FastTickStoreQueryErrorV1::kNone;
}

bool FastTickStoreV1::PublishedThrough(
    std::uint32_t instrument_id,
    std::uint64_t arrival_id) const noexcept {
    if (impl_ == nullptr || instrument_id == 0U ||
        instrument_id > impl_->config_.instrument_count ||
        arrival_id == 0U) {
        return false;
    }
    const FastTickRecordV1* const latest =
        impl_->rows_[instrument_id - 1U].latest.load(
            std::memory_order_acquire);
    return latest != nullptr &&
           latest->compact().arrival_id >= arrival_id;
}

FastTickStoreQueryErrorV1 FastTickStoreV1::OpenCursor(
    std::uint32_t instrument_id,
    std::uint64_t next_arrival_row,
    std::unique_ptr<FastTickCursorV1>* output) const noexcept {
    if (output == nullptr) {
        return FastTickStoreQueryErrorV1::kNullOutput;
    }
    output->reset();
    if (impl_ == nullptr || instrument_id == 0U ||
        instrument_id > impl_->config_.instrument_count ||
        next_arrival_row == 0U) {
        return FastTickStoreQueryErrorV1::kInvalidArgument;
    }
    const Impl::InstrumentRow& row =
        impl_->rows_[instrument_id - 1U];
    const std::uint64_t target = row.published_tail.load(
        std::memory_order_acquire);
    if (target == std::numeric_limits<std::uint64_t>::max() ||
        next_arrival_row > target + 1U) {
        return FastTickStoreQueryErrorV1::kInvalidArgument;
    }
    try {
        auto cursor = std::make_unique<FastTickCursorV1::Impl>();
        cursor->chunk = row.head.load(std::memory_order_acquire);
        cursor->next_row = next_arrival_row;
        cursor->target = target;
        cursor->maximum_records_per_read =
            impl_->config_.maximum_records_per_read;

        std::uint64_t skip = next_arrival_row - 1U;
        while (skip != 0U && cursor->chunk != nullptr) {
            const std::size_t count = cursor->chunk->published_.load(
                std::memory_order_acquire);
            if (skip < count) {
                cursor->offset = static_cast<std::size_t>(skip);
                skip = 0U;
                break;
            }
            skip -= count;
            cursor->chunk = cursor->chunk->next_.load(
                std::memory_order_acquire);
        }
        if (skip != 0U) {
            return FastTickStoreQueryErrorV1::kResourceExhausted;
        }
        output->reset(new FastTickCursorV1(std::move(cursor)));
        return FastTickStoreQueryErrorV1::kNone;
    } catch (...) {
        return FastTickStoreQueryErrorV1::kResourceExhausted;
    }
}

FastTickStoreQueryErrorV1 FastTickStoreV1::CopyCompactHistory(
    std::uint32_t instrument_id,
    std::vector<CompactFastTickV1>* output,
    std::uint64_t* captured_tail) const noexcept {
    if (output == nullptr) {
        return FastTickStoreQueryErrorV1::kNullOutput;
    }
    output->clear();
    if (captured_tail != nullptr) {
        *captured_tail = 0U;
    }
    std::unique_ptr<FastTickCursorV1> cursor;
    const FastTickStoreQueryErrorV1 open = OpenCursor(
        instrument_id, 1U, &cursor);
    if (open != FastTickStoreQueryErrorV1::kNone) {
        return open;
    }
    try {
        if (cursor->target_tail() >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max())) {
            return FastTickStoreQueryErrorV1::kResourceExhausted;
        }
        output->reserve(
            static_cast<std::size_t>(cursor->target_tail()));
        const std::size_t batch_capacity = std::min<std::size_t>(
            impl_->config_.maximum_records_per_read, 4096U);
        std::vector<const FastTickRecordV1*> batch(batch_capacity);
        while (!cursor->done()) {
            std::size_t written = 0U;
            const FastTickStoreQueryErrorV1 read = cursor->ReadBatch(
                batch, &written);
            if (read != FastTickStoreQueryErrorV1::kNone) {
                output->clear();
                return read;
            }
            for (std::size_t index = 0U; index < written; ++index) {
                output->push_back(batch[index]->compact());
            }
        }
        if (captured_tail != nullptr) {
            *captured_tail = cursor->target_tail();
        }
        return FastTickStoreQueryErrorV1::kNone;
    } catch (...) {
        output->clear();
        return FastTickStoreQueryErrorV1::kResourceExhausted;
    }
}

void FastTickStoreV1::MarkCoverageLost(
    std::uint32_t instrument_id) noexcept {
    if (impl_ == nullptr || instrument_id == 0U ||
        instrument_id > impl_->config_.instrument_count) {
        return;
    }
    impl_->rows_[instrument_id - 1U].coverage_complete.store(
        false, std::memory_order_release);
    impl_->any_coverage_lost_.store(true, std::memory_order_release);
}

FastTickStoreSnapshotV1 FastTickStoreV1::Snapshot() const noexcept {
    FastTickStoreSnapshotV1 result{};
    if (impl_ == nullptr) {
        return result;
    }
    result.maximum_session_records =
        impl_->config_.maximum_session_records;
    result.appended_records = impl_->appended_records_.load(
        std::memory_order_acquire);
    result.failed_appends = impl_->failed_appends_.load(
        std::memory_order_acquire);
    result.allocated_chunks = impl_->allocated_chunks_.load(
        std::memory_order_acquire);
    result.chunk_capacity = impl_->chunk_capacity_;
    result.any_coverage_lost = impl_->any_coverage_lost_.load(
        std::memory_order_acquire);
    return result;
}

const FastTickStoreConfigV1& FastTickStoreV1::config() const noexcept {
    return impl_->config_;
}

}  // namespace l2flow::market
