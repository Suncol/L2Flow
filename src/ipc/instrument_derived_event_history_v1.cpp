#include "l2flow/ipc/instrument_derived_event_history_v1.h"

#include "l2flow/ipc/order_event_wire_adapter_v2.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <new>
#include <type_traits>
#include <utility>

namespace l2flow::ipc {
namespace {

[[nodiscard]] bool RawCheckpointEqual(
    const InstrumentRawEventHistoryCheckpointV2& lhs,
    const InstrumentRawEventHistoryCheckpointV2& rhs) noexcept {
    // The public raw-history implementation value-initializes and completely
    // populates this C object. Exact byte equality is intentional here: a
    // derived update may continue only from the exact EOF boundary which
    // committed the in-memory order state.
    static_assert(std::is_trivially_copyable_v<
                  InstrumentRawEventHistoryCheckpointV2>);
    return std::memcmp(&lhs, &rhs, sizeof(lhs)) == 0;
}

[[nodiscard]] bool DerivedCheckpointEqual(
    const InstrumentDerivedEventCheckpointV1& lhs,
    const InstrumentDerivedEventCheckpointV1& rhs) noexcept {
    return RawCheckpointEqual(
               lhs.raw_checkpoint, rhs.raw_checkpoint) &&
           lhs.derived_event_sequence_exclusive ==
               rhs.derived_event_sequence_exclusive &&
           lhs.order_state_count == rhs.order_state_count &&
           lhs.instrument_id == rhs.instrument_id &&
           lhs.trade_date == rhs.trade_date &&
           lhs.market == rhs.market &&
           lhs.finalized == rhs.finalized;
}

[[nodiscard]] bool IsAbsoluteUnixPath(
    const std::string& value) noexcept {
    return !value.empty() && value.front() == '/' &&
           value.find('\0') == std::string::npos;
}

template <std::size_t Size>
[[nodiscard]] bool CByteArrayAnyNonzero(
    const std::uint8_t (&value)[Size]) noexcept {
    return std::any_of(
        value,
        value + Size,
        [](std::uint8_t byte) noexcept { return byte != 0U; });
}

[[nodiscard]] bool ExpectedSessionCanonical(
    const l2flow_shm_session_info_v2& session) noexcept {
    return CByteArrayAnyNonzero(session.run_id) &&
           CByteArrayAnyNonzero(session.catalog_digest) &&
           session.session_epoch != 0U && session.trade_date != 0U &&
           session.capacity != 0U &&
           session.catalog_scope ==
               static_cast<std::uint32_t>(
                   L2FLOW_CATALOG_DECLARED_DAILY_A_SHARE_V2) &&
           session.coverage_complete == 1U &&
           session.catalog_generation == 1U &&
           session.bound_count == session.capacity &&
           session.catalog_trade_date == session.trade_date &&
           session.reserved_catalog == 0U &&
           session.catalog_version != 0U;
}

template <typename SourceVariant>
[[nodiscard]] bool AppendMarketEvents(
    SourceVariant* source,
    std::uint64_t* next_sequence,
    std::vector<InstrumentDerivedEventV1>* output,
    bool source_tick_identity_valid) {
    if (source == nullptr || next_sequence == nullptr ||
        output == nullptr) {
        return false;
    }
    if (source->size() >
        std::numeric_limits<std::uint64_t>::max() -
            *next_sequence) {
        return false;
    }
    if (source_tick_identity_valid && !source->empty() &&
        source->size() - 1U >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max())) {
        return false;
    }
    output->reserve(output->size() + source->size());
    for (std::size_t index = 0U; index < source->size(); ++index) {
        auto& source_event = (*source)[index];
        InstrumentDerivedEventV1 event{};
        event.derived_event_sequence = *next_sequence;
        event.source_tick_event_ordinal =
            source_tick_identity_valid
                ? static_cast<std::uint32_t>(index)
                : 0U;
        event.source_tick_event_ordinal_valid =
            source_tick_identity_valid;
        std::visit(
            [&event](auto& value) {
                using Event = std::decay_t<decltype(value)>;
                event.payload.template emplace<Event>(
                    std::move(value));
            },
            source_event);
        output->emplace_back(std::move(event));
        ++(*next_sequence);
    }
    return true;
}

}  // namespace

class InstrumentDerivedEventHistorySessionV1::Impl final {
public:
    explicit Impl(
        InstrumentDerivedEventHistoryConfigV1 config) noexcept
        : config_(std::move(config)) {}

    [[nodiscard]] InstrumentDerivedEventHistoryErrorV1
    InitializeCore() noexcept {
        if (config_.market == market::MarketV1::kShanghai) {
            const market::ShanghaiOrderAggregatorCreateErrorV1 error =
                market::ShanghaiOrderEventAggregatorV1::Create(
                    {config_.expected_session.trade_date,
                     config_.maximum_order_states},
                    &shanghai_);
            if (error ==
                market::ShanghaiOrderAggregatorCreateErrorV1::kNone) {
                return InstrumentDerivedEventHistoryErrorV1::kNone;
            }
            return error ==
                           market::ShanghaiOrderAggregatorCreateErrorV1::
                               kResourceExhausted
                       ? InstrumentDerivedEventHistoryErrorV1::
                             kResourceExhausted
                       : InstrumentDerivedEventHistoryErrorV1::
                             kInvalidConfiguration;
        }
        const market::ShenzhenOrderProjectorCreateErrorV1 error =
            market::ShenzhenOrderEventProjectorV1::Create(
                {config_.expected_session.trade_date,
                 config_.maximum_order_states},
                &shenzhen_);
        if (error ==
            market::ShenzhenOrderProjectorCreateErrorV1::kNone) {
            return InstrumentDerivedEventHistoryErrorV1::kNone;
        }
        return error ==
                       market::ShenzhenOrderProjectorCreateErrorV1::
                           kResourceExhausted
                   ? InstrumentDerivedEventHistoryErrorV1::
                         kResourceExhausted
                   : InstrumentDerivedEventHistoryErrorV1::
                         kInvalidConfiguration;
    }

    [[nodiscard]] InstrumentDerivedEventHistoryErrorV1 BeginFull(
        std::uint64_t expected_generation,
        std::uint32_t requested_raw_page_records) noexcept {
        if (failed_) {
            return InstrumentDerivedEventHistoryErrorV1::kFailed;
        }
        if (read_active_ || ever_started_ || has_checkpoint_ ||
            finalized_ || requested_raw_page_records == 0U) {
            return InstrumentDerivedEventHistoryErrorV1::kInvalidState;
        }
        return OpenRawRead(
            nullptr,
            expected_generation,
            requested_raw_page_records);
    }

    [[nodiscard]] InstrumentDerivedEventHistoryErrorV1 BeginUpdate(
        const InstrumentDerivedEventCheckpointV1& base,
        std::uint64_t expected_generation,
        std::uint32_t requested_raw_page_records) noexcept {
        if (failed_) {
            return InstrumentDerivedEventHistoryErrorV1::kFailed;
        }
        if (read_active_ || !has_checkpoint_ || finalized_ ||
            requested_raw_page_records == 0U) {
            return InstrumentDerivedEventHistoryErrorV1::kInvalidState;
        }
        if (!DerivedCheckpointEqual(base, checkpoint_)) {
            return InstrumentDerivedEventHistoryErrorV1::
                kCheckpointMismatch;
        }
        return OpenRawRead(
            &base.raw_checkpoint,
            expected_generation,
            requested_raw_page_records);
    }

    [[nodiscard]] InstrumentDerivedEventHistoryErrorV1 ReadPage(
        InstrumentDerivedEventHistoryPageV1* output) noexcept {
        if (output == nullptr) {
            return InstrumentDerivedEventHistoryErrorV1::kNullOutput;
        }
        output->events.clear();
        output->raw_page_index = 0U;
        output->cumulative_raw_event_count = 0U;
        output->first_ingress_sequence = 0U;
        output->last_ingress_sequence = 0U;
        output->first_tick_stream_sequence = 0U;
        output->last_tick_stream_sequence = 0U;
        output->eof = false;
        if (failed_) {
            return InstrumentDerivedEventHistoryErrorV1::kFailed;
        }
        if (!read_active_) {
            return InstrumentDerivedEventHistoryErrorV1::kInvalidState;
        }

        InstrumentRawEventHistoryPageViewV2 raw_page;
        last_raw_error_ = raw_cursor_.ReadPage(&raw_page);
        if (last_raw_error_ !=
            InstrumentRawEventHistoryErrorV2::kNone) {
            FailClose();
            return InstrumentDerivedEventHistoryErrorV1::
                kRawHistoryError;
        }
        output->raw_page_index = raw_page.page_index();
        output->cumulative_raw_event_count =
            raw_page.cumulative_record_count();
        output->first_ingress_sequence =
            raw_page.first_ingress_sequence();
        output->last_ingress_sequence =
            raw_page.last_ingress_sequence();
        output->first_tick_stream_sequence =
            raw_page.first_tick_stream_sequence();
        output->last_tick_stream_sequence =
            raw_page.last_tick_stream_sequence();

        if (raw_page.eof()) {
            InstrumentRawEventHistoryCheckpointV2 raw_checkpoint{};
            last_raw_error_ =
                raw_cursor_.VerifiedCheckpoint(&raw_checkpoint);
            if (last_raw_error_ !=
                InstrumentRawEventHistoryErrorV2::kNone) {
                FailClose();
                return InstrumentDerivedEventHistoryErrorV1::
                    kRawHistoryError;
            }
            checkpoint_ = MakeCheckpoint(raw_checkpoint);
            has_checkpoint_ = true;
            read_active_ = false;
            output->eof = true;
            raw_cursor_.Reset();
            raw_session_.Reset();
            return InstrumentDerivedEventHistoryErrorV1::kNone;
        }

        try {
            const auto records = raw_page.records();
            if (records.size() >
                std::numeric_limits<std::size_t>::max() / 3U) {
                FailClose();
                return InstrumentDerivedEventHistoryErrorV1::
                    kResourceExhausted;
            }
            output->events.reserve(records.size() * 3U);
            for (const RealtimeWireTickPayloadV2& record : records) {
                if (record.common.instrument_id !=
                        config_.instrument_id ||
                    record.common.trade_date !=
                        config_.expected_session.trade_date) {
                    output->events.clear();
                    FailClose();
                    return InstrumentDerivedEventHistoryErrorV1::
                        kWireProjectionError;
                }
                const auto error = ConsumeRecord(
                    record, &output->events);
                if (error !=
                    InstrumentDerivedEventHistoryErrorV1::kNone) {
                    output->events.clear();
                    FailClose();
                    return error;
                }
            }
            return InstrumentDerivedEventHistoryErrorV1::kNone;
        } catch (const std::bad_alloc&) {
            output->events.clear();
            FailClose();
            return InstrumentDerivedEventHistoryErrorV1::
                kResourceExhausted;
        } catch (...) {
            output->events.clear();
            FailClose();
            return InstrumentDerivedEventHistoryErrorV1::kFailed;
        }
    }

    [[nodiscard]] InstrumentDerivedEventHistoryErrorV1
    VerifiedCheckpoint(
        InstrumentDerivedEventCheckpointV1* output) const noexcept {
        if (output == nullptr) {
            return InstrumentDerivedEventHistoryErrorV1::kNullOutput;
        }
        *output = {};
        if (failed_) {
            return InstrumentDerivedEventHistoryErrorV1::kFailed;
        }
        if (read_active_ || !has_checkpoint_) {
            return InstrumentDerivedEventHistoryErrorV1::kInvalidState;
        }
        *output = checkpoint_;
        return InstrumentDerivedEventHistoryErrorV1::kNone;
    }

    [[nodiscard]] InstrumentDerivedEventHistoryErrorV1
    FinalizeTradingDay(
        std::vector<InstrumentDerivedEventV1>* output) noexcept {
        if (output == nullptr) {
            return InstrumentDerivedEventHistoryErrorV1::kNullOutput;
        }
        output->clear();
        if (failed_) {
            return InstrumentDerivedEventHistoryErrorV1::kFailed;
        }
        if (read_active_ || !has_checkpoint_ || finalized_) {
            return InstrumentDerivedEventHistoryErrorV1::kInvalidState;
        }
        try {
            if (config_.market == market::MarketV1::kShanghai) {
                shanghai_events_.clear();
                const market::ShanghaiOrderAggregatorConsumeErrorV1 error =
                    shanghai_->Finalize({}, &shanghai_events_);
                if (error !=
                    market::ShanghaiOrderAggregatorConsumeErrorV1::kNone) {
                    FailClose();
                    return InstrumentDerivedEventHistoryErrorV1::
                        kAggregationError;
                }
                if (!AppendMarketEvents(
                        &shanghai_events_,
                        &next_sequence_,
                        output,
                        false)) {
                    FailClose();
                    return InstrumentDerivedEventHistoryErrorV1::
                        kResourceExhausted;
                }
            } else {
                shenzhen_events_.clear();
                const market::ShenzhenOrderProjectorConsumeErrorV1 error =
                    shenzhen_->Finalize({}, &shenzhen_events_);
                if (error !=
                    market::ShenzhenOrderProjectorConsumeErrorV1::kNone) {
                    FailClose();
                    return InstrumentDerivedEventHistoryErrorV1::
                        kAggregationError;
                }
                if (!AppendMarketEvents(
                        &shenzhen_events_,
                        &next_sequence_,
                        output,
                        false)) {
                    FailClose();
                    return InstrumentDerivedEventHistoryErrorV1::
                        kResourceExhausted;
                }
            }
            finalized_ = true;
            checkpoint_.derived_event_sequence_exclusive =
                next_sequence_;
            checkpoint_.order_state_count = order_state_count();
            checkpoint_.finalized = true;
            return InstrumentDerivedEventHistoryErrorV1::kNone;
        } catch (const std::bad_alloc&) {
            output->clear();
            FailClose();
            return InstrumentDerivedEventHistoryErrorV1::
                kResourceExhausted;
        } catch (...) {
            output->clear();
            FailClose();
            return InstrumentDerivedEventHistoryErrorV1::kFailed;
        }
    }

    [[nodiscard]] bool read_active() const noexcept {
        return read_active_;
    }
    [[nodiscard]] bool failed() const noexcept {
        return failed_;
    }
    [[nodiscard]] bool finalized() const noexcept {
        return finalized_;
    }
    [[nodiscard]] std::uint64_t next_sequence() const noexcept {
        return next_sequence_;
    }
    [[nodiscard]] std::size_t order_state_count() const noexcept {
        return config_.market == market::MarketV1::kShanghai
                   ? shanghai_->order_count()
                   : shenzhen_->order_count();
    }
    [[nodiscard]] InstrumentRawEventHistoryErrorV2
    last_raw_error() const noexcept {
        return last_raw_error_;
    }
    [[nodiscard]] const InstrumentDerivedEventHistoryConfigV1&
    config() const noexcept {
        return config_;
    }

    void CloseForDestruction() noexcept {
        if (read_active_) {
            failed_ = true;
        }
        raw_cursor_.Reset();
        raw_session_.Reset();
        read_active_ = false;
    }

private:
    [[nodiscard]] InstrumentDerivedEventHistoryErrorV1 OpenRawRead(
        const InstrumentRawEventHistoryCheckpointV2* base,
        std::uint64_t expected_generation,
        std::uint32_t requested_raw_page_records) noexcept {
        InstrumentRawEventHistorySessionV2 raw_session;
        last_raw_error_ = InstrumentRawEventHistorySessionV2::Open(
            config_.control_socket_path.c_str(),
            config_.expected_session,
            expected_generation,
            config_.timeout_ms,
            &raw_session);
        if (last_raw_error_ !=
            InstrumentRawEventHistoryErrorV2::kNone) {
            return InstrumentDerivedEventHistoryErrorV1::
                kRawHistoryError;
        }
        InstrumentRawEventHistoryEndpointV2 target{};
        last_raw_error_ = raw_session.Target(&target);
        if (last_raw_error_ !=
                InstrumentRawEventHistoryErrorV2::kNone ||
            target.trade_date !=
                config_.expected_session.trade_date) {
            if (last_raw_error_ ==
                InstrumentRawEventHistoryErrorV2::kNone) {
                last_raw_error_ =
                    InstrumentRawEventHistoryErrorV2::kProtocolError;
            }
            return InstrumentDerivedEventHistoryErrorV1::
                kRawHistoryError;
        }
        InstrumentRawEventHistoryCursorV2 raw_cursor;
        last_raw_error_ =
            base == nullptr
                ? raw_session.OpenFull(
                      config_.instrument_id,
                      requested_raw_page_records,
                      &raw_cursor)
                : raw_session.OpenUpdate(
                      config_.instrument_id,
                      requested_raw_page_records,
                      *base,
                      &raw_cursor);
        if (last_raw_error_ !=
            InstrumentRawEventHistoryErrorV2::kNone) {
            return InstrumentDerivedEventHistoryErrorV1::
                kRawHistoryError;
        }
        raw_session_ = std::move(raw_session);
        raw_cursor_ = std::move(raw_cursor);
        read_active_ = true;
        ever_started_ = true;
        return InstrumentDerivedEventHistoryErrorV1::kNone;
    }

    [[nodiscard]] InstrumentDerivedEventCheckpointV1
    MakeCheckpoint(
        const InstrumentRawEventHistoryCheckpointV2& raw) const noexcept {
        InstrumentDerivedEventCheckpointV1 checkpoint{};
        checkpoint.raw_checkpoint = raw;
        checkpoint.derived_event_sequence_exclusive =
            next_sequence_;
        checkpoint.order_state_count = order_state_count();
        checkpoint.instrument_id = config_.instrument_id;
        checkpoint.trade_date =
            config_.expected_session.trade_date;
        checkpoint.market = config_.market;
        checkpoint.finalized = finalized_;
        return checkpoint;
    }

    [[nodiscard]] InstrumentDerivedEventHistoryErrorV1 ConsumeRecord(
        const RealtimeWireTickPayloadV2& record,
        std::vector<InstrumentDerivedEventV1>* output) {
        if (config_.market == market::MarketV1::kShanghai) {
            market::ShanghaiOrderEventInputV1 input{};
            if (ProjectShanghaiOrderEventInputFromWireV2(
                    record, &input) !=
                WireOrderEventProjectionResultV2::kProjected) {
                return InstrumentDerivedEventHistoryErrorV1::
                    kWireProjectionError;
            }
            shanghai_events_.clear();
            const market::ShanghaiOrderAggregatorConsumeErrorV1 error =
                shanghai_->Consume(input, &shanghai_events_);
            if (error !=
                market::ShanghaiOrderAggregatorConsumeErrorV1::kNone) {
                return InstrumentDerivedEventHistoryErrorV1::
                    kAggregationError;
            }
            return AppendMarketEvents(
                       &shanghai_events_,
                       &next_sequence_,
                       output,
                       true)
                       ? InstrumentDerivedEventHistoryErrorV1::kNone
                       : InstrumentDerivedEventHistoryErrorV1::
                             kResourceExhausted;
        }

        market::ShenzhenOrderEventInputV1 input{};
        if (ProjectShenzhenOrderEventInputFromWireV2(
                record, &input) !=
            WireOrderEventProjectionResultV2::kProjected) {
            return InstrumentDerivedEventHistoryErrorV1::
                kWireProjectionError;
        }
        shenzhen_events_.clear();
        const market::ShenzhenOrderProjectorConsumeErrorV1 error =
            shenzhen_->Consume(input, &shenzhen_events_);
        if (error !=
            market::ShenzhenOrderProjectorConsumeErrorV1::kNone) {
            return InstrumentDerivedEventHistoryErrorV1::
                kAggregationError;
        }
        return AppendMarketEvents(
                   &shenzhen_events_,
                   &next_sequence_,
                   output,
                   true)
                   ? InstrumentDerivedEventHistoryErrorV1::kNone
                   : InstrumentDerivedEventHistoryErrorV1::
                         kResourceExhausted;
    }

    void FailClose() noexcept {
        failed_ = true;
        raw_cursor_.Reset();
        raw_session_.Reset();
        read_active_ = false;
    }

    InstrumentDerivedEventHistoryConfigV1 config_;
    std::unique_ptr<market::ShanghaiOrderEventAggregatorV1>
        shanghai_;
    std::unique_ptr<market::ShenzhenOrderEventProjectorV1>
        shenzhen_;
    std::vector<market::ShanghaiOrderEventV1> shanghai_events_;
    std::vector<market::ShenzhenOrderEventV1> shenzhen_events_;
    InstrumentRawEventHistorySessionV2 raw_session_;
    InstrumentRawEventHistoryCursorV2 raw_cursor_;
    InstrumentDerivedEventCheckpointV1 checkpoint_{};
    std::uint64_t next_sequence_ = 1U;
    InstrumentRawEventHistoryErrorV2 last_raw_error_ =
        InstrumentRawEventHistoryErrorV2::kNone;
    bool read_active_ = false;
    bool ever_started_ = false;
    bool has_checkpoint_ = false;
    bool failed_ = false;
    bool finalized_ = false;
};

std::string_view InstrumentDerivedEventHistoryErrorNameV1(
    InstrumentDerivedEventHistoryErrorV1 error) noexcept {
    switch (error) {
        case InstrumentDerivedEventHistoryErrorV1::kNone:
            return "none";
        case InstrumentDerivedEventHistoryErrorV1::kNullOutput:
            return "null_output";
        case InstrumentDerivedEventHistoryErrorV1::
            kInvalidConfiguration:
            return "invalid_configuration";
        case InstrumentDerivedEventHistoryErrorV1::kInvalidState:
            return "invalid_state";
        case InstrumentDerivedEventHistoryErrorV1::
            kCheckpointMismatch:
            return "checkpoint_mismatch";
        case InstrumentDerivedEventHistoryErrorV1::
            kRawHistoryError:
            return "raw_history_error";
        case InstrumentDerivedEventHistoryErrorV1::
            kWireProjectionError:
            return "wire_projection_error";
        case InstrumentDerivedEventHistoryErrorV1::
            kAggregationError:
            return "aggregation_error";
        case InstrumentDerivedEventHistoryErrorV1::
            kResourceExhausted:
            return "resource_exhausted";
        case InstrumentDerivedEventHistoryErrorV1::kFailed:
            return "failed";
    }
    return "unknown";
}

InstrumentDerivedEventHistorySessionV1::
    InstrumentDerivedEventHistorySessionV1(
        std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

InstrumentDerivedEventHistorySessionV1::
    ~InstrumentDerivedEventHistorySessionV1() {
    if (impl_ != nullptr) {
        impl_->CloseForDestruction();
    }
}

InstrumentDerivedEventHistoryErrorV1
InstrumentDerivedEventHistorySessionV1::Create(
    InstrumentDerivedEventHistoryConfigV1 config,
    std::unique_ptr<InstrumentDerivedEventHistorySessionV1>* output)
    noexcept {
    if (output == nullptr) {
        return InstrumentDerivedEventHistoryErrorV1::kNullOutput;
    }
    output->reset();
    if (!IsAbsoluteUnixPath(config.control_socket_path) ||
        !ExpectedSessionCanonical(config.expected_session) ||
        config.instrument_id == 0U ||
        config.instrument_id > config.expected_session.bound_count ||
        (config.market != market::MarketV1::kShanghai &&
         config.market != market::MarketV1::kShenzhen) ||
        config.maximum_order_states == 0U) {
        return InstrumentDerivedEventHistoryErrorV1::
            kInvalidConfiguration;
    }
    try {
        auto impl = std::make_unique<Impl>(std::move(config));
        const InstrumentDerivedEventHistoryErrorV1 error =
            impl->InitializeCore();
        if (error !=
            InstrumentDerivedEventHistoryErrorV1::kNone) {
            return error;
        }
        output->reset(new InstrumentDerivedEventHistorySessionV1(
            std::move(impl)));
        return InstrumentDerivedEventHistoryErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return InstrumentDerivedEventHistoryErrorV1::
            kResourceExhausted;
    } catch (...) {
        return InstrumentDerivedEventHistoryErrorV1::
            kResourceExhausted;
    }
}

InstrumentDerivedEventHistoryErrorV1
InstrumentDerivedEventHistorySessionV1::BeginFull(
    std::uint64_t expected_generation,
    std::uint32_t requested_raw_page_records) noexcept {
    return impl_ == nullptr
               ? InstrumentDerivedEventHistoryErrorV1::kFailed
               : impl_->BeginFull(
                     expected_generation,
                     requested_raw_page_records);
}

InstrumentDerivedEventHistoryErrorV1
InstrumentDerivedEventHistorySessionV1::BeginUpdate(
    const InstrumentDerivedEventCheckpointV1& base,
    std::uint64_t expected_generation,
    std::uint32_t requested_raw_page_records) noexcept {
    return impl_ == nullptr
               ? InstrumentDerivedEventHistoryErrorV1::kFailed
               : impl_->BeginUpdate(
                     base,
                     expected_generation,
                     requested_raw_page_records);
}

InstrumentDerivedEventHistoryErrorV1
InstrumentDerivedEventHistorySessionV1::ReadPage(
    InstrumentDerivedEventHistoryPageV1* output) noexcept {
    return impl_ == nullptr
               ? InstrumentDerivedEventHistoryErrorV1::kFailed
               : impl_->ReadPage(output);
}

InstrumentDerivedEventHistoryErrorV1
InstrumentDerivedEventHistorySessionV1::VerifiedCheckpoint(
    InstrumentDerivedEventCheckpointV1* output) const noexcept {
    return impl_ == nullptr
               ? InstrumentDerivedEventHistoryErrorV1::kFailed
               : impl_->VerifiedCheckpoint(output);
}

InstrumentDerivedEventHistoryErrorV1
InstrumentDerivedEventHistorySessionV1::FinalizeTradingDay(
    std::vector<InstrumentDerivedEventV1>* output) noexcept {
    return impl_ == nullptr
               ? InstrumentDerivedEventHistoryErrorV1::kFailed
               : impl_->FinalizeTradingDay(output);
}

bool InstrumentDerivedEventHistorySessionV1::read_active()
    const noexcept {
    return impl_ != nullptr && impl_->read_active();
}

bool InstrumentDerivedEventHistorySessionV1::failed() const noexcept {
    return impl_ == nullptr || impl_->failed();
}

bool InstrumentDerivedEventHistorySessionV1::finalized()
    const noexcept {
    return impl_ != nullptr && impl_->finalized();
}

std::uint64_t
InstrumentDerivedEventHistorySessionV1::
    next_derived_event_sequence() const noexcept {
    return impl_ == nullptr ? 0U : impl_->next_sequence();
}

std::size_t
InstrumentDerivedEventHistorySessionV1::order_state_count()
    const noexcept {
    return impl_ == nullptr ? 0U : impl_->order_state_count();
}

InstrumentRawEventHistoryErrorV2
InstrumentDerivedEventHistorySessionV1::last_raw_error()
    const noexcept {
    return impl_ == nullptr
               ? InstrumentRawEventHistoryErrorV2::kClosed
               : impl_->last_raw_error();
}

const InstrumentDerivedEventHistoryConfigV1&
InstrumentDerivedEventHistorySessionV1::config() const noexcept {
    static const InstrumentDerivedEventHistoryConfigV1 empty{};
    return impl_ == nullptr ? empty : impl_->config();
}

}  // namespace l2flow::ipc
