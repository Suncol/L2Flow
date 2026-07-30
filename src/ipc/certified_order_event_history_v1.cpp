#include "l2flow/ipc/certified_order_event_history_v1.h"

#include "l2flow/ipc/certified_order_event_journal_v1.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include <sys/mman.h>
#include <unistd.h>

namespace l2flow::ipc {

// This storage is an intentional Linux/GCC implementation contract, not a
// portable ISO C++ allocator: mmap supplies one aligned raw region, individual
// prefix elements begin lifetime through construct_at, and the resulting
// adjacent prefix is exposed as a contiguous span on this target ABI.
struct CertifiedOrderEventHistoryJournalStorageV1 final {
    explicit CertifiedOrderEventHistoryJournalStorageV1(
        std::size_t capacity_value) noexcept
        : capacity(capacity_value),
          mapped_bytes(
              capacity_value *
              sizeof(InstrumentDerivedEventV1)) {
        const long system_page_size = ::sysconf(_SC_PAGESIZE);
        if (system_page_size <= 0) {
            return;
        }
        page_size_ =
            static_cast<std::size_t>(system_page_size);
        if (mapped_bytes >
            std::numeric_limits<std::size_t>::max() -
                (page_size_ - 1U)) {
            page_size_ = 0U;
            return;
        }
        reserved_bytes_ =
            ((mapped_bytes + page_size_ - 1U) /
             page_size_) *
            page_size_;

        int flags = MAP_PRIVATE | MAP_ANONYMOUS;
#if defined(MAP_NORESERVE)
        // The inaccessible mapping reserves only one stable virtual address
        // range. Writable chunks replace it on demand without MAP_NORESERVE,
        // allowing strict Linux commit accounting to reject a chunk before
        // the stateful projection core is mutated.
        flags |= MAP_NORESERVE;
#endif
        void* const mapping = ::mmap(
            nullptr,
            reserved_bytes_,
            PROT_NONE,
            flags,
            -1,
            0);
        if (mapping == MAP_FAILED) {
            ResetSizes();
            return;
        }
        if (reinterpret_cast<std::uintptr_t>(mapping) %
                alignof(InstrumentDerivedEventV1) !=
            0U) {
            static_cast<void>(
                ::munmap(mapping, reserved_bytes_));
            ResetSizes();
            return;
        }
        mapping_ = mapping;
        reservation_active_ = true;
    }

    CertifiedOrderEventHistoryJournalStorageV1(
        const CertifiedOrderEventHistoryJournalStorageV1&) =
        delete;
    CertifiedOrderEventHistoryJournalStorageV1& operator=(
        const CertifiedOrderEventHistoryJournalStorageV1&) =
        delete;

    ~CertifiedOrderEventHistoryJournalStorageV1() {
        while (constructed_count != 0U) {
            --constructed_count;
            std::destroy_at(Slot(constructed_count));
        }
        if (reservation_active_) {
            static_cast<void>(
                ::munmap(mapping_, reserved_bytes_));
        }
    }

    [[nodiscard]] bool valid() const noexcept {
        return reservation_active_ &&
               mapped_bytes != 0U &&
               reserved_bytes_ != 0U;
    }

    [[nodiscard]] bool EnsureWritable(
        std::size_t required_count) noexcept {
        if (!valid() || required_count > capacity) {
            return false;
        }
        const std::size_t required_bytes =
            required_count *
            sizeof(InstrumentDerivedEventV1);
        if (required_bytes <= committed_bytes_) {
            return true;
        }

        constexpr std::size_t kPreferredCommitBytes =
            64U * 1024U * 1024U;
        std::size_t commit_granularity = page_size_;
        if (page_size_ <= kPreferredCommitBytes &&
            kPreferredCommitBytes <=
                std::numeric_limits<std::size_t>::max() -
                    (page_size_ - 1U)) {
            commit_granularity =
                ((kPreferredCommitBytes + page_size_ - 1U) /
                 page_size_) *
                page_size_;
        }
        std::size_t target_bytes = reserved_bytes_;
        if (required_bytes <=
            std::numeric_limits<std::size_t>::max() -
                (commit_granularity - 1U)) {
            target_bytes =
                ((required_bytes + commit_granularity - 1U) /
                 commit_granularity) *
                commit_granularity;
        }
        target_bytes =
            std::min(target_bytes, reserved_bytes_);

        const std::uintptr_t target_address =
            reinterpret_cast<std::uintptr_t>(mapping_) +
            committed_bytes_;
        void* const target =
            reinterpret_cast<void*>(target_address);
        const std::size_t commit_bytes =
            target_bytes - committed_bytes_;
        void* const committed = ::mmap(
            target,
            commit_bytes,
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED,
            -1,
            0);
        if (committed == MAP_FAILED) {
            return false;
        }
        if (committed != target) {
            static_cast<void>(
                ::munmap(committed, commit_bytes));
            return false;
        }
        committed_bytes_ = target_bytes;
        return true;
    }

    [[nodiscard]] InstrumentDerivedEventV1*
    ConstructNext(std::size_t index) noexcept {
        if (index != constructed_count ||
            index >= capacity ||
            (index + 1U) *
                    sizeof(InstrumentDerivedEventV1) >
                committed_bytes_) {
            return nullptr;
        }
        InstrumentDerivedEventV1* const event =
            std::construct_at(Slot(index));
        ++constructed_count;
        return event;
    }

    [[nodiscard]] InstrumentDerivedEventV1*
    events() noexcept {
        return static_cast<InstrumentDerivedEventV1*>(
            mapping_);
    }

    [[nodiscard]] const InstrumentDerivedEventV1*
    events() const noexcept {
        return static_cast<const InstrumentDerivedEventV1*>(
            mapping_);
    }

private:
    void ResetSizes() noexcept {
        mapped_bytes = 0U;
        reserved_bytes_ = 0U;
        page_size_ = 0U;
    }

    [[nodiscard]] InstrumentDerivedEventV1*
    Slot(std::size_t index) noexcept {
        const std::uintptr_t address =
            reinterpret_cast<std::uintptr_t>(mapping_) +
            index * sizeof(InstrumentDerivedEventV1);
        return reinterpret_cast<InstrumentDerivedEventV1*>(
            address);
    }

public:
    std::size_t capacity = 0U;
    std::size_t mapped_bytes = 0U;

private:
    void* mapping_ = nullptr;
    bool reservation_active_ = false;
    std::size_t page_size_ = 0U;
    std::size_t reserved_bytes_ = 0U;
    std::size_t committed_bytes_ = 0U;
    std::size_t constructed_count = 0U;
};

namespace {

template <typename Event>
inline constexpr bool kLosslessEventMoveV1 =
    std::is_nothrow_move_constructible_v<Event>;

static_assert(kLosslessEventMoveV1<
              market::ShanghaiOrderRevisionEventV1>);
static_assert(kLosslessEventMoveV1<market::ShanghaiTradeEventV1>);
static_assert(kLosslessEventMoveV1<market::ShanghaiCancelEventV1>);
static_assert(kLosslessEventMoveV1<market::ShanghaiStatusEventV1>);
static_assert(kLosslessEventMoveV1<
              market::ShenzhenOrderRevisionEventV1>);
static_assert(kLosslessEventMoveV1<market::ShenzhenTradeEventV1>);
static_assert(kLosslessEventMoveV1<market::ShenzhenCancelEventV1>);
static_assert(
    std::is_nothrow_default_constructible_v<
        InstrumentDerivedEventV1>);

[[nodiscard]] bool ValidConfig(
    const CertifiedOrderEventHistoryConfigV1& config) noexcept {
    if (config.trade_date == 0U ||
        config.maximum_shanghai_order_states == 0U ||
        config.maximum_shenzhen_order_states == 0U ||
        config.maximum_events == 0U) {
        return false;
    }
    if (config.maximum_events >
        std::numeric_limits<std::size_t>::max() /
            sizeof(InstrumentDerivedEventV1)) {
        return false;
    }
    if (config.maximum_events >
        static_cast<std::size_t>(
            std::numeric_limits<std::ptrdiff_t>::max()) /
            sizeof(InstrumentDerivedEventV1)) {
        return false;
    }
    if constexpr (
        std::numeric_limits<std::size_t>::max() >=
        std::numeric_limits<std::uint64_t>::max()) {
        return config.maximum_events <
               std::numeric_limits<std::uint64_t>::max();
    }
    return true;
}

template <typename SourceEvent>
[[nodiscard]] bool AppendLosslessEvents(
    std::vector<SourceEvent>* source,
    CertifiedOrderEventHistoryJournalStorageV1* storage,
    std::size_t* event_count,
    std::uint64_t* next_sequence) {
    for (SourceEvent& source_event : *source) {
        InstrumentDerivedEventV1* const destination =
            storage->ConstructNext(*event_count);
        if (destination == nullptr) {
            return false;
        }
        destination->derived_event_sequence = *next_sequence;
        std::visit(
            [&destination](auto& event) {
                using Event =
                    std::decay_t<decltype(event)>;
                destination->payload.template emplace<Event>(
                    std::move(event));
            },
            source_event);
        ++(*event_count);
        ++(*next_sequence);
    }
    return true;
}

[[nodiscard]] bool RemainingCapacity(
    std::size_t current,
    std::size_t required,
    std::size_t capacity) noexcept {
    return current <= capacity &&
           required <= capacity - current;
}

}  // namespace

class CertifiedOrderEventHistoryV1::Impl final {
public:
    explicit Impl(
        CertifiedOrderEventHistoryConfigV1 config) noexcept
        : config_(config) {}

    [[nodiscard]] CertifiedOrderEventHistoryErrorV1
    Initialize() {
        if (config_.external_journal != nullptr) {
            const CertifiedOrderEventJournalSessionV1 session =
                config_.external_journal->session();
            if (session.trade_date != config_.trade_date ||
                session.event_capacity <
                    static_cast<std::uint64_t>(
                        config_.maximum_events) ||
                config_.external_journal
                        ->canonical_apply_frontier() !=
                    0U ||
                config_.external_journal
                        ->published_event_sequence() !=
                    0U ||
                config_.external_journal->failed()) {
                return CertifiedOrderEventHistoryErrorV1::
                    kInvalidConfiguration;
            }
        }
        const auto shanghai_error =
            market::ShanghaiOrderEventAggregatorV1::Create(
                {config_.trade_date,
                 config_.maximum_shanghai_order_states},
                &shanghai_);
        if (shanghai_error !=
            market::ShanghaiOrderAggregatorCreateErrorV1::kNone) {
            return shanghai_error ==
                           market::
                               ShanghaiOrderAggregatorCreateErrorV1::
                                   kResourceExhausted
                       ? CertifiedOrderEventHistoryErrorV1::
                             kResourceExhausted
                       : CertifiedOrderEventHistoryErrorV1::
                             kAggregationError;
        }
        const auto shenzhen_error =
            market::ShenzhenOrderEventProjectorV1::Create(
                {config_.trade_date,
                 config_.maximum_shenzhen_order_states},
                &shenzhen_);
        if (shenzhen_error !=
            market::ShenzhenOrderProjectorCreateErrorV1::kNone) {
            shanghai_.reset();
            return shenzhen_error ==
                           market::
                               ShenzhenOrderProjectorCreateErrorV1::
                                   kResourceExhausted
                       ? CertifiedOrderEventHistoryErrorV1::
                             kResourceExhausted
                       : CertifiedOrderEventHistoryErrorV1::
                             kAggregationError;
        }

        storage_ = std::make_shared<
            CertifiedOrderEventHistoryJournalStorageV1>(
            config_.maximum_events);
        if (!storage_->valid()) {
            storage_.reset();
            shenzhen_.reset();
            shanghai_.reset();
            return CertifiedOrderEventHistoryErrorV1::
                kResourceExhausted;
        }
        return CertifiedOrderEventHistoryErrorV1::kNone;
    }

    [[nodiscard]] CertifiedOrderEventHistoryErrorV1 Consume(
        const RealtimeWireTickPayloadV2& input,
        std::uint64_t canonical_apply_sequence) noexcept {
        if (failed_.load(std::memory_order_acquire)) {
            return CertifiedOrderEventHistoryErrorV1::kFailed;
        }
        if (canonical_apply_sequence == 0U ||
            canonical_apply_sequence ==
                std::numeric_limits<std::uint64_t>::max() ||
            (last_canonical_apply_sequence_ != 0U &&
             canonical_apply_sequence <=
                 last_canonical_apply_sequence_)) {
            return Fail(
                CertifiedOrderEventHistoryErrorV1::
                    kCanonicalSequence);
        }
        if (published_generation_ ==
            std::numeric_limits<std::uint64_t>::max()) {
            return Fail(
                CertifiedOrderEventHistoryErrorV1::
                    kEventCapacity);
        }

        try {
            const auto kind =
                static_cast<market::MarketEventKindV1>(
                    input.common.event_kind);
            if (kind ==
                market::MarketEventKindV1::kShanghaiTick) {
                return ConsumeShanghai(
                    input, canonical_apply_sequence);
            }
            if (kind ==
                    market::MarketEventKindV1::kShenzhenOrder ||
                kind ==
                    market::MarketEventKindV1::
                        kShenzhenTransaction) {
                return ConsumeShenzhen(
                    input, canonical_apply_sequence);
            }
            last_wire_projection_ =
                WireOrderEventProjectionResultV2::
                    kNotTargetEvent;
            return Fail(
                CertifiedOrderEventHistoryErrorV1::
                    kWireProjectionError);
        } catch (const std::bad_alloc&) {
            return Fail(
                CertifiedOrderEventHistoryErrorV1::
                    kResourceExhausted);
        } catch (...) {
            return Fail(
                CertifiedOrderEventHistoryErrorV1::kFailed);
        }
    }

    [[nodiscard]] std::shared_ptr<
        const CertifiedOrderEventHistoryJournalStorageV1>
    AcquireStorage() const noexcept {
        return storage_;
    }

    [[nodiscard]] CertifiedOrderEventHistoryGenerationV1
    AcquirePublishedGeneration() const noexcept {
        const std::lock_guard<std::mutex> lock(
            publication_mutex_);
        const CertifiedOrderEventHistoryGenerationV1 result =
            visible_generation_;
        return result;
    }

    [[nodiscard]] bool failed() const noexcept {
        return failed_.load(std::memory_order_acquire);
    }

    [[nodiscard]] CertifiedOrderEventHistoryErrorV1
    last_error() const noexcept {
        return last_error_.load(std::memory_order_acquire);
    }

    [[nodiscard]] WireOrderEventProjectionResultV2
    last_wire_projection_result() const noexcept {
        return last_wire_projection_;
    }

    [[nodiscard]] market::ShanghaiOrderAggregatorConsumeErrorV1
    last_shanghai_error() const noexcept {
        return last_shanghai_error_;
    }

    [[nodiscard]] market::ShenzhenOrderProjectorConsumeErrorV1
    last_shenzhen_error() const noexcept {
        return last_shenzhen_error_;
    }

    [[nodiscard]] CertifiedOrderEventJournalPublishErrorV1
    last_external_journal_error() const noexcept {
        return last_external_journal_error_;
    }

    [[nodiscard]] const CertifiedOrderEventHistoryConfigV1&
    config() const noexcept {
        return config_;
    }

private:
    [[nodiscard]] CertifiedOrderEventHistoryErrorV1
    ConsumeShanghai(
        const RealtimeWireTickPayloadV2& wire,
        std::uint64_t canonical_apply_sequence) {
        market::ShanghaiOrderEventInputV1 input{};
        last_wire_projection_ =
            ProjectShanghaiOrderEventInputFromWireV2(
                wire, &input);
        if (last_wire_projection_ !=
                WireOrderEventProjectionResultV2::kProjected ||
            input.trade_date != config_.trade_date) {
            return Fail(
                CertifiedOrderEventHistoryErrorV1::
                    kWireProjectionError);
        }

        std::int64_t* native_frontier = nullptr;
        if (!CheckNativeMonotonic(
                &shanghai_native_by_channel_,
                static_cast<std::int64_t>(input.channel),
                input.anchor.native_event_sequence,
                &native_frontier)) {
            return Fail(
                CertifiedOrderEventHistoryErrorV1::
                    kNativeSequenceRegression);
        }

        std::size_t maximum_output = 0U;
        switch (input.action) {
            case market::TickActionV1::kAdd:
                maximum_output = 1U;
                break;
            case market::TickActionV1::kCancel:
                maximum_output = 2U;
                break;
            case market::TickActionV1::kTrade:
                maximum_output = 3U;
                break;
            case market::TickActionV1::kStatus:
                if (input.phase_valid &&
                    input.phase ==
                        market::TradingPhaseV1::kEnd) {
                    if (shanghai_->order_count() ==
                        std::numeric_limits<
                            std::size_t>::max()) {
                        return Fail(
                            CertifiedOrderEventHistoryErrorV1::
                                kEventCapacity);
                    }
                    maximum_output =
                        shanghai_->order_count() + 1U;
                } else {
                    maximum_output = 1U;
                }
                break;
            case market::TickActionV1::kUnknown:
                return Fail(
                    CertifiedOrderEventHistoryErrorV1::
                        kWireProjectionError);
        }
        if (!RemainingCapacity(
                event_count_,
                maximum_output,
                config_.maximum_events)) {
            return Fail(
                CertifiedOrderEventHistoryErrorV1::
                    kEventCapacity);
        }
        if (!storage_->EnsureWritable(
                event_count_ + maximum_output)) {
            return Fail(
                CertifiedOrderEventHistoryErrorV1::
                    kResourceExhausted);
        }
        if (config_.external_journal != nullptr) {
            last_external_journal_error_ =
                config_.external_journal->EnsureWritable(
                    static_cast<std::uint64_t>(
                        event_count_ + maximum_output));
            if (last_external_journal_error_ !=
                CertifiedOrderEventJournalPublishErrorV1::kNone) {
                return Fail(
                    CertifiedOrderEventHistoryErrorV1::
                        kExternalJournalError);
            }
        }

        const std::size_t event_begin = event_count_;
        shanghai_events_.clear();
        last_shanghai_error_ =
            shanghai_->ConsumeCanonical(
                input,
                canonical_apply_sequence,
                &shanghai_events_);
        if (last_shanghai_error_ !=
            market::ShanghaiOrderAggregatorConsumeErrorV1::
                kNone) {
            return Fail(
                CertifiedOrderEventHistoryErrorV1::
                    kAggregationError);
        }
        if (shanghai_events_.size() > maximum_output ||
            !RemainingCapacity(
                event_count_,
                shanghai_events_.size(),
                config_.maximum_events)) {
            return Fail(
                CertifiedOrderEventHistoryErrorV1::
                    kEventCapacity);
        }

        if (!AppendLosslessEvents(
                &shanghai_events_,
                storage_.get(),
                &event_count_,
                &next_derived_event_sequence_)) {
            return Fail(
                CertifiedOrderEventHistoryErrorV1::
                    kEventCapacity);
        }
        if (config_.external_journal != nullptr) {
            last_external_journal_error_ =
                config_.external_journal->PublishCanonicalTick(
                    canonical_apply_sequence,
                    static_cast<std::uint64_t>(
                        shanghai_->order_count()),
                    static_cast<std::uint64_t>(
                        shenzhen_->order_count()),
                    std::span<const InstrumentDerivedEventV1>(
                        storage_->events() + event_begin,
                        event_count_ - event_begin));
            if (last_external_journal_error_ !=
                CertifiedOrderEventJournalPublishErrorV1::kNone) {
                return Fail(
                    CertifiedOrderEventHistoryErrorV1::
                        kExternalJournalError);
            }
        }
        *native_frontier = input.anchor.native_event_sequence;
        return Publish(
            market::MarketV1::kShanghai,
            static_cast<std::int64_t>(input.channel),
            input.anchor,
            canonical_apply_sequence);
    }

    [[nodiscard]] CertifiedOrderEventHistoryErrorV1
    ConsumeShenzhen(
        const RealtimeWireTickPayloadV2& wire,
        std::uint64_t canonical_apply_sequence) {
        market::ShenzhenOrderEventInputV1 input{};
        last_wire_projection_ =
            ProjectShenzhenOrderEventInputFromWireV2(
                wire, &input);
        if (last_wire_projection_ !=
                WireOrderEventProjectionResultV2::kProjected ||
            input.trade_date != config_.trade_date) {
            return Fail(
                CertifiedOrderEventHistoryErrorV1::
                    kWireProjectionError);
        }

        std::int64_t* native_frontier = nullptr;
        if (!CheckNativeMonotonic(
                &shenzhen_native_by_channel_,
                static_cast<std::int64_t>(input.channel),
                input.anchor.native_event_sequence,
                &native_frontier)) {
            return Fail(
                CertifiedOrderEventHistoryErrorV1::
                    kNativeSequenceRegression);
        }

        std::size_t maximum_output = 0U;
        switch (input.action) {
            case market::TickActionV1::kAdd:
                maximum_output = 1U;
                break;
            case market::TickActionV1::kCancel:
                maximum_output = 2U;
                break;
            case market::TickActionV1::kTrade:
                maximum_output = 3U;
                break;
            case market::TickActionV1::kStatus:
            case market::TickActionV1::kUnknown:
                return Fail(
                    CertifiedOrderEventHistoryErrorV1::
                        kWireProjectionError);
        }
        if (!RemainingCapacity(
                event_count_,
                maximum_output,
                config_.maximum_events)) {
            return Fail(
                CertifiedOrderEventHistoryErrorV1::
                    kEventCapacity);
        }
        if (!storage_->EnsureWritable(
                event_count_ + maximum_output)) {
            return Fail(
                CertifiedOrderEventHistoryErrorV1::
                    kResourceExhausted);
        }
        if (config_.external_journal != nullptr) {
            last_external_journal_error_ =
                config_.external_journal->EnsureWritable(
                    static_cast<std::uint64_t>(
                        event_count_ + maximum_output));
            if (last_external_journal_error_ !=
                CertifiedOrderEventJournalPublishErrorV1::kNone) {
                return Fail(
                    CertifiedOrderEventHistoryErrorV1::
                        kExternalJournalError);
            }
        }

        const std::size_t event_begin = event_count_;
        shenzhen_events_.clear();
        last_shenzhen_error_ =
            shenzhen_->ConsumeCanonical(
                input,
                canonical_apply_sequence,
                &shenzhen_events_);
        if (last_shenzhen_error_ !=
            market::ShenzhenOrderProjectorConsumeErrorV1::
                kNone) {
            return Fail(
                CertifiedOrderEventHistoryErrorV1::
                    kAggregationError);
        }
        if (shenzhen_events_.size() > maximum_output ||
            !RemainingCapacity(
                event_count_,
                shenzhen_events_.size(),
                config_.maximum_events)) {
            return Fail(
                CertifiedOrderEventHistoryErrorV1::
                    kEventCapacity);
        }

        if (!AppendLosslessEvents(
                &shenzhen_events_,
                storage_.get(),
                &event_count_,
                &next_derived_event_sequence_)) {
            return Fail(
                CertifiedOrderEventHistoryErrorV1::
                    kEventCapacity);
        }
        if (config_.external_journal != nullptr) {
            last_external_journal_error_ =
                config_.external_journal->PublishCanonicalTick(
                    canonical_apply_sequence,
                    static_cast<std::uint64_t>(
                        shanghai_->order_count()),
                    static_cast<std::uint64_t>(
                        shenzhen_->order_count()),
                    std::span<const InstrumentDerivedEventV1>(
                        storage_->events() + event_begin,
                        event_count_ - event_begin));
            if (last_external_journal_error_ !=
                CertifiedOrderEventJournalPublishErrorV1::kNone) {
                return Fail(
                    CertifiedOrderEventHistoryErrorV1::
                        kExternalJournalError);
            }
        }
        *native_frontier = input.anchor.native_event_sequence;
        return Publish(
            market::MarketV1::kShenzhen,
            static_cast<std::int64_t>(input.channel),
            input.anchor,
            canonical_apply_sequence);
    }

    template <typename Map>
    [[nodiscard]] bool CheckNativeMonotonic(
        Map* frontiers,
        std::int64_t channel,
        std::int64_t native_sequence,
        std::int64_t** output) {
        auto [position, inserted] =
            frontiers->try_emplace(channel, 0);
        if (!inserted &&
            native_sequence <= position->second) {
            return false;
        }
        *output = &position->second;
        return true;
    }

    template <typename Anchor>
    [[nodiscard]] CertifiedOrderEventHistoryErrorV1 Publish(
        market::MarketV1 market_value,
        std::int64_t channel,
        const Anchor& anchor,
        std::uint64_t canonical_apply_sequence) noexcept {
        ++published_generation_;
        last_canonical_apply_sequence_ =
            canonical_apply_sequence;
        CertifiedOrderEventHistoryGenerationV1 publication{};
        publication.generation = published_generation_;
        publication.input_frontier.canonical_apply_sequence =
            canonical_apply_sequence;
        publication.input_frontier.market = market_value;
        publication.input_frontier.channel = channel;
        publication.input_frontier.native_event_sequence =
            anchor.native_event_sequence;
        publication.input_frontier.source_sequence =
            anchor.source_sequence;
        publication.input_frontier.ingress_sequence =
            anchor.ingress_sequence;
        publication.input_frontier.tick_stream_sequence =
            anchor.tick_stream_sequence;
        publication.derived_event_sequence_exclusive =
            next_derived_event_sequence_;
        publication.event_count = event_count_;
        publication.shanghai_order_state_count =
            shanghai_->order_count();
        publication.shenzhen_order_state_count =
            shenzhen_->order_count();
        {
            const std::lock_guard<std::mutex> lock(
                publication_mutex_);
            visible_generation_ = publication;
        }
        last_error_.store(
            CertifiedOrderEventHistoryErrorV1::kNone,
            std::memory_order_release);
        return CertifiedOrderEventHistoryErrorV1::kNone;
    }

    [[nodiscard]] CertifiedOrderEventHistoryErrorV1 Fail(
        CertifiedOrderEventHistoryErrorV1 error) noexcept {
        last_error_.store(error, std::memory_order_release);
        failed_.store(true, std::memory_order_release);
        return error;
    }

    CertifiedOrderEventHistoryConfigV1 config_;
    std::unique_ptr<market::ShanghaiOrderEventAggregatorV1>
        shanghai_;
    std::unique_ptr<market::ShenzhenOrderEventProjectorV1>
        shenzhen_;
    std::shared_ptr<
        CertifiedOrderEventHistoryJournalStorageV1>
        storage_;
    std::map<std::int64_t, std::int64_t>
        shanghai_native_by_channel_;
    std::map<std::int64_t, std::int64_t>
        shenzhen_native_by_channel_;
    std::vector<market::ShanghaiOrderEventV1>
        shanghai_events_;
    std::vector<market::ShenzhenOrderEventV1>
        shenzhen_events_;
    std::size_t event_count_ = 0U;
    std::uint64_t next_derived_event_sequence_ = 1U;
    std::uint64_t last_canonical_apply_sequence_ = 0U;
    std::uint64_t published_generation_ = 0U;
    // This mutex protects only one fixed-size generation metadata copy; it
    // never covers projection or journal construction.
    mutable std::mutex publication_mutex_;
    CertifiedOrderEventHistoryGenerationV1
        visible_generation_{};
    std::atomic<CertifiedOrderEventHistoryErrorV1>
        last_error_{
            CertifiedOrderEventHistoryErrorV1::kNone};
    std::atomic<bool> failed_{false};
    WireOrderEventProjectionResultV2
        last_wire_projection_ =
            WireOrderEventProjectionResultV2::kProjected;
    market::ShanghaiOrderAggregatorConsumeErrorV1
        last_shanghai_error_ =
            market::ShanghaiOrderAggregatorConsumeErrorV1::kNone;
    market::ShenzhenOrderProjectorConsumeErrorV1
        last_shenzhen_error_ =
            market::ShenzhenOrderProjectorConsumeErrorV1::kNone;
    CertifiedOrderEventJournalPublishErrorV1
        last_external_journal_error_ =
            CertifiedOrderEventJournalPublishErrorV1::kNone;
};

std::string_view CertifiedOrderEventHistoryErrorNameV1(
    CertifiedOrderEventHistoryErrorV1 error) noexcept {
    switch (error) {
        case CertifiedOrderEventHistoryErrorV1::kNone:
            return "none";
        case CertifiedOrderEventHistoryErrorV1::kNullOutput:
            return "null_output";
        case CertifiedOrderEventHistoryErrorV1::
            kInvalidConfiguration:
            return "invalid_configuration";
        case CertifiedOrderEventHistoryErrorV1::
            kCanonicalSequence:
            return "canonical_sequence";
        case CertifiedOrderEventHistoryErrorV1::
            kNativeSequenceRegression:
            return "native_sequence_regression";
        case CertifiedOrderEventHistoryErrorV1::
            kWireProjectionError:
            return "wire_projection_error";
        case CertifiedOrderEventHistoryErrorV1::
            kAggregationError:
            return "aggregation_error";
        case CertifiedOrderEventHistoryErrorV1::
            kEventCapacity:
            return "event_capacity";
        case CertifiedOrderEventHistoryErrorV1::
            kExternalJournalError:
            return "external_journal_error";
        case CertifiedOrderEventHistoryErrorV1::
            kResourceExhausted:
            return "resource_exhausted";
        case CertifiedOrderEventHistoryErrorV1::kFailed:
            return "failed";
    }
    return "unknown";
}

bool CertifiedOrderEventHistorySnapshotV1::valid() const noexcept {
    return storage_ != nullptr && storage_->valid() &&
           generation_.event_count <= storage_->capacity;
}

CertifiedOrderEventHistoryGenerationV1
CertifiedOrderEventHistorySnapshotV1::generation() const noexcept {
    return valid() ? generation_
                   : CertifiedOrderEventHistoryGenerationV1{};
}

std::span<const InstrumentDerivedEventV1>
CertifiedOrderEventHistorySnapshotV1::events() const noexcept {
    if (!valid()) {
        return {};
    }
    if (generation_.event_count == 0U) {
        return {};
    }
    return {
        storage_->events(),
        generation_.event_count};
}

CertifiedOrderEventHistoryV1::CertifiedOrderEventHistoryV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

CertifiedOrderEventHistoryV1::~CertifiedOrderEventHistoryV1() =
    default;

CertifiedOrderEventHistoryErrorV1
CertifiedOrderEventHistoryV1::Create(
    CertifiedOrderEventHistoryConfigV1 config,
    std::unique_ptr<CertifiedOrderEventHistoryV1>* output)
    noexcept {
    if (output == nullptr) {
        return CertifiedOrderEventHistoryErrorV1::kNullOutput;
    }
    output->reset();
    if (!ValidConfig(config)) {
        return CertifiedOrderEventHistoryErrorV1::
            kInvalidConfiguration;
    }
    try {
        auto impl = std::make_unique<Impl>(config);
        const CertifiedOrderEventHistoryErrorV1 error =
            impl->Initialize();
        if (error !=
            CertifiedOrderEventHistoryErrorV1::kNone) {
            return error;
        }
        output->reset(new CertifiedOrderEventHistoryV1(
            std::move(impl)));
        return CertifiedOrderEventHistoryErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return CertifiedOrderEventHistoryErrorV1::
            kResourceExhausted;
    } catch (...) {
        return CertifiedOrderEventHistoryErrorV1::
            kResourceExhausted;
    }
}

CertifiedOrderEventHistoryErrorV1
CertifiedOrderEventHistoryV1::AppendCertifiedTick(
    const RealtimeWireTickPayloadV2& input,
    std::uint64_t canonical_apply_sequence) noexcept {
    return impl_ == nullptr
               ? CertifiedOrderEventHistoryErrorV1::kFailed
               : impl_->Consume(
                     input, canonical_apply_sequence);
}

CertifiedOrderEventHistoryErrorV1
CertifiedOrderEventHistoryV1::AcquireGeneration(
    CertifiedOrderEventHistorySnapshotV1* output) const noexcept {
    if (output == nullptr) {
        return CertifiedOrderEventHistoryErrorV1::kNullOutput;
    }
    output->storage_.reset();
    output->generation_ = {};
    if (impl_ == nullptr) {
        return CertifiedOrderEventHistoryErrorV1::kFailed;
    }
    output->storage_ = impl_->AcquireStorage();
    if (output->storage_ != nullptr) {
        output->generation_ =
            impl_->AcquirePublishedGeneration();
    }
    return output->storage_ == nullptr
               ? CertifiedOrderEventHistoryErrorV1::kFailed
               : CertifiedOrderEventHistoryErrorV1::kNone;
}

bool CertifiedOrderEventHistoryV1::failed() const noexcept {
    return impl_ == nullptr || impl_->failed();
}

CertifiedOrderEventHistoryErrorV1
CertifiedOrderEventHistoryV1::last_error() const noexcept {
    return impl_ == nullptr
               ? CertifiedOrderEventHistoryErrorV1::kFailed
               : impl_->last_error();
}

WireOrderEventProjectionResultV2
CertifiedOrderEventHistoryV1::
    last_wire_projection_result() const noexcept {
    return impl_ == nullptr
               ? WireOrderEventProjectionResultV2::
                     kInvalidEnvelope
               : impl_->last_wire_projection_result();
}

market::ShanghaiOrderAggregatorConsumeErrorV1
CertifiedOrderEventHistoryV1::
    last_shanghai_error() const noexcept {
    return impl_ == nullptr
               ? market::
                     ShanghaiOrderAggregatorConsumeErrorV1::
                         kFailed
               : impl_->last_shanghai_error();
}

market::ShenzhenOrderProjectorConsumeErrorV1
CertifiedOrderEventHistoryV1::
    last_shenzhen_error() const noexcept {
    return impl_ == nullptr
               ? market::ShenzhenOrderProjectorConsumeErrorV1::
                     kFailed
               : impl_->last_shenzhen_error();
}

CertifiedOrderEventJournalPublishErrorV1
CertifiedOrderEventHistoryV1::
    last_external_journal_error() const noexcept {
    return impl_ == nullptr
               ? CertifiedOrderEventJournalPublishErrorV1::kFailed
               : impl_->last_external_journal_error();
}

const CertifiedOrderEventHistoryConfigV1&
CertifiedOrderEventHistoryV1::config() const noexcept {
    static const CertifiedOrderEventHistoryConfigV1 empty{};
    return impl_ == nullptr ? empty : impl_->config();
}

}  // namespace l2flow::ipc
