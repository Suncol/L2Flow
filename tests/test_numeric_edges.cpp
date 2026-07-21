#include "l2mock/l2_mock.h"

#include "mdl_cffexl2_msg.h"
#include "mdl_dcel2_msg.h"
#include "mdl_gfexl2_msg.h"
#include "mdl_shfel2_msg.h"
#include "mdl_shl2_msg.h"
#include "mdl_szl2_msg.h"

#include <algorithm>
#include <chrono>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace mdl = datayes::mdl;
namespace mock = datayes::mdl::mock;
namespace cf = datayes::mdl::mdl_cffexl2_msg;
namespace dc = datayes::mdl::mdl_dcel2_msg;
namespace gf = datayes::mdl::mdl_gfexl2_msg;
namespace sf = datayes::mdl::mdl_shfel2_msg;
namespace sh = datayes::mdl::mdl_shl2_msg;
namespace sz = datayes::mdl::mdl_szl2_msg;

namespace {

const std::chrono::milliseconds kTimeout(5000);

class CheckedHandler : public mdl::MessageHandler {
public:
    std::vector<std::string> Errors() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return errors_;
    }

public:
    void Fail(const std::string& message) {
        errors_.push_back(message);
    }

    void Check(bool condition, const std::string& message) {
        if (!condition) {
            Fail(message);
        }
    }

    template <typename T>
    const T* Body(const mdl::MDLMessage* message, const char* label) {
        if (message == nullptr || message->GetHead() == nullptr ||
            message->GetBody() == nullptr) {
            Fail(std::string(label) + ": null message/head/body");
            return nullptr;
        }
        const mdl::MDLMessageHead* const head = message->GetHead();
        if (head->ServiceID != T::ServiceID ||
            head->ServiceVersion != T::ServiceVer ||
            head->MessageID != T::MessageID) {
            Fail(std::string(label) + ": header/type key mismatch");
            return nullptr;
        }
        if (head->MessageSize < head->HeadSize ||
            static_cast<std::size_t>(head->MessageSize - head->HeadSize) <
                sizeof(T) ||
            message->GetBodySize() <
                static_cast<std::uint32_t>(sizeof(T))) {
            Fail(std::string(label) + ": body is smaller than SDK type");
            return nullptr;
        }
        return reinterpret_cast<const T*>(message->GetBody());
    }

    mutable std::mutex mutex_;
    std::vector<std::string> errors_;
};

template <typename Handler, typename Subscribe>
bool RunScenario(const mock::Config& config,
                 Handler* handler,
                 Subscribe subscribe,
                 const std::string& name,
                 std::vector<std::string>* errors) {
    const std::string validation = mock::ValidateConfig(config);
    if (!validation.empty()) {
        errors->push_back(name + ": ValidateConfig rejected the scenario: " +
                          validation);
        return false;
    }

    mdl::IOManagerPtr manager;
    try {
        manager = mock::CreateIOManager(config);
        if (manager.IsNull()) {
            errors->push_back(name + ": CreateIOManager returned null");
            return false;
        }
        mdl::SubscriberPtr subscriber =
            manager->CreateSubscriber(handler, false);
        if (subscriber.IsNull()) {
            errors->push_back(name + ": CreateSubscriber returned null");
            manager->Shutdown();
            return false;
        }
        subscribe(subscriber.Get());
        const char* const connect_error = subscriber->Connect();
        if (connect_error == nullptr || connect_error[0] != '\0') {
            errors->push_back(
                name + ": Connect failed: " +
                (connect_error == nullptr ? std::string("<null>")
                                          : std::string(connect_error)));
            manager->Shutdown();
            return false;
        }

        const std::chrono::steady_clock::time_point deadline =
            std::chrono::steady_clock::now() + kTimeout;
        while (!handler->Done() &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        const bool done = handler->Done();
        manager->Shutdown();
        if (!done) {
            errors->push_back(name + ": timed out waiting for observations");
        }
        const mock::Statistics statistics =
            mock::GetStatistics(manager.Get());
        if (statistics.callback_errors != 0) {
            errors->push_back(name + ": callback_errors is non-zero");
        }
        return done;
    } catch (const std::exception& error) {
        if (!manager.IsNull()) {
            manager->Shutdown();
        }
        errors->push_back(name + ": threw std::exception: " + error.what());
        return false;
    } catch (...) {
        if (!manager.IsNull()) {
            manager->Shutdown();
        }
        errors->push_back(name + ": threw an unknown exception");
        return false;
    }
}

mock::Config BaseConfig() {
    mock::Config config;
    config.messages_per_second = 20000;
    config.book_depth = 1;
    config.orders_per_level = 1;
    config.callback_threads = 1;
    config.callback_queue_capacity = 64;
    config.clock_mode = mock::ClockMode::SimulatedTradingDay;
    config.simulated_start_date = 20260105;
    config.simulated_start_time = 93000000;
    config.simulated_min_step_ms = 0;
    config.simulated_max_step_ms = 1;
    config.instruments.clear();
    return config;
}

mock::Instrument Instrument(std::uint8_t service_id,
                            const std::string& security_id,
                            std::int64_t reference_milli,
                            std::int64_t tick_milli,
                            std::uint32_t lot_size) {
    mock::Instrument instrument;
    instrument.service_id = service_id;
    instrument.security_id = security_id;
    instrument.reference_price_milli = reference_milli;
    instrument.tick_size_milli = tick_milli;
    instrument.lot_size = lot_size;
    instrument.option = false;
    return instrument;
}

struct WeightedBookResult {
    std::uint64_t quantity = 0;
    std::int64_t minimum = 0;
    std::int64_t maximum = 0;
    std::int64_t average = 0;
    bool valid = false;
};

template <typename Object, typename Value>
Value CopyMember(const Object* object, Value Object::* member) {
    Value value;
    std::memcpy(&value, &(object->*member), sizeof(value));
    return value;
}

template <typename Level, typename Price, typename Size, typename Orders>
WeightedBookResult CheckSzBook(
    const mdl::MDLListT<Level>& levels,
    Price price,
    Size size,
    Orders orders,
    std::int64_t tick,
    std::uint32_t expected_depth,
    std::uint32_t expected_orders,
    const char* label,
    CheckedHandler* checks) {
    WeightedBookResult result;
    checks->Check(
        levels.Length == expected_depth,
        std::string(label) + ": wrong depth");
    if (levels.Length != expected_depth) {
        return result;
    }

    std::vector<std::pair<std::int64_t, std::uint64_t> > book;
    book.reserve(expected_depth);
    for (std::uint32_t index = 0; index < expected_depth; ++index) {
        const Level* const level = levels[index];
        if (level == nullptr) {
            checks->Fail(
                std::string(label) + ": null level " +
                std::to_string(index));
            return result;
        }
        const std::int64_t level_price =
            CopyMember(level, price).m_Value;
        const std::uint64_t level_size =
            CopyMember(level, size);
        const auto& queue = level->*orders;
        checks->Check(
            level_price > 0 && level_price % tick == 0,
            std::string(label) + ": price is not positive/tick-aligned");
        checks->Check(
            level_size > 0,
            std::string(label) + ": level size is not positive");
        checks->Check(
            level->NumOrders == expected_orders,
            std::string(label) + ": NumOrders is wrong");
        checks->Check(
            queue.Length == expected_orders,
            std::string(label) + ": nested queue length is wrong");

        std::uint64_t queue_sum = 0;
        if (queue.Length == expected_orders) {
            for (std::uint32_t order_index = 0;
                 order_index < expected_orders;
                 ++order_index) {
                const typename Level::OrdersItem* const order =
                    queue[order_index];
                if (order == nullptr) {
                    checks->Fail(
                        std::string(label) + ": null nested order");
                    continue;
                }
                checks->Check(
                    order->OrderQty > 0,
                    std::string(label) +
                        ": nested OrderQty is not positive");
                checks->Check(
                    order->OrderQty % 1000000U == 0U,
                    std::string(label) +
                        ": nested OrderQty is not on the lot grid");
                queue_sum += order->OrderQty;
            }
        }
        checks->Check(
            queue_sum == level_size,
            std::string(label) +
                ": nested OrderQty sum does not match level size");
        book.push_back(std::make_pair(level_price, level_size));
    }

    if (book.empty()) {
        return result;
    }
    result.minimum = book.front().first;
    result.maximum = book.front().first;
    for (const auto& level : book) {
        result.minimum = std::min(result.minimum, level.first);
        result.maximum = std::max(result.maximum, level.first);
        if (result.quantity >
            std::numeric_limits<std::uint64_t>::max() - level.second) {
            checks->Fail(std::string(label) + ": quantity sum overflow");
            return result;
        }
        result.quantity += level.second;
    }

    // Subtracting the minimum price before multiplying is mathematically
    // exact and keeps this intentionally extreme test inside uint64_t.
    std::uint64_t weighted_delta = 0;
    for (const auto& level : book) {
        const std::uint64_t delta =
            static_cast<std::uint64_t>(level.first - result.minimum);
        if (delta != 0 &&
            level.second >
                std::numeric_limits<std::uint64_t>::max() / delta) {
            checks->Fail(std::string(label) + ": weighted delta overflow");
            return result;
        }
        const std::uint64_t product = delta * level.second;
        if (weighted_delta >
            std::numeric_limits<std::uint64_t>::max() - product) {
            checks->Fail(
                std::string(label) + ": weighted delta sum overflow");
            return result;
        }
        weighted_delta += product;
    }
    if (result.quantity == 0) {
        checks->Fail(std::string(label) + ": total quantity is zero");
        return result;
    }
    result.average =
        result.minimum +
        static_cast<std::int64_t>(weighted_delta / result.quantity);
    result.valid = true;
    return result;
}

class ShenzhenExtremeHandler final : public CheckedHandler {
public:
    bool Done() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return count_ >= kTarget;
    }

    void OnMDLSZL2Message(const mdl::MDLMessage* message) override {
        if (message == nullptr || message->GetHead() == nullptr ||
            message->GetHead()->MessageID != sz::MarketData::MessageID) {
            return;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        if (count_ >= kTarget) {
            return;
        }
        ++count_;
        const sz::MarketData* const body =
            Body<sz::MarketData>(message, "SZ MarketData");
        if (body == nullptr) {
            return;
        }

        const WeightedBookResult offers = CheckSzBook(
            body->OfferPriceLevel,
            &sz::MarketData::OfferPriceLevelItem::OfferPx,
            &sz::MarketData::OfferPriceLevelItem::OfferSize,
            &sz::MarketData::OfferPriceLevelItem::Orders,
            kTick,
            kDepth,
            kOrders,
            "SZ offer book",
            this);
        const WeightedBookResult bids = CheckSzBook(
            body->BidPriceLevel,
            &sz::MarketData::BidPriceLevelItem::BidPx,
            &sz::MarketData::BidPriceLevelItem::BidSize,
            &sz::MarketData::BidPriceLevelItem::Orders,
            kTick,
            kDepth,
            kOrders,
            "SZ bid book",
            this);

        if (offers.valid) {
            Check(
                body->TotalOfferQty == offers.quantity,
                "SZ TotalOfferQty does not reconcile with the book");
            Check(
                body->WeightedAvgOfferPx.m_Value == offers.average,
                "SZ WeightedAvgOfferPx is not exact");
            Check(
                body->WeightedAvgOfferPx.m_Value >= offers.minimum &&
                    body->WeightedAvgOfferPx.m_Value <= offers.maximum,
                "SZ WeightedAvgOfferPx is outside the offer book");
        }
        if (bids.valid) {
            Check(
                body->TotalBidQty == bids.quantity,
                "SZ TotalBidQty does not reconcile with the book");
            Check(
                body->WeightedAvgBidPx.m_Value == bids.average,
                "SZ WeightedAvgBidPx is not exact");
            Check(
                body->WeightedAvgBidPx.m_Value >= bids.minimum &&
                    body->WeightedAvgBidPx.m_Value <= bids.maximum,
                "SZ WeightedAvgBidPx is outside the bid book");
        }
    }

private:
    static const std::uint32_t kDepth = 50;
    static const std::uint32_t kOrders = 100;
    static const std::int64_t kTick = 1000;
    static const std::uint32_t kTarget = 6;
    std::uint32_t count_ = 0;
};

template <typename T>
void CheckDceOrder(const mdl::MDLMessage* message,
                   const char* label,
                   CheckedHandler* checks) {
    const T* const body = checks->Body<T>(message, label);
    if (body == nullptr) {
        return;
    }
    checks->Check(
        body->BidOrders.Length == 2,
        std::string(label) + ": BidOrders length is not two");
    checks->Check(
        body->AskOrders.Length == 2,
        std::string(label) + ": AskOrders length is not two");
    if (body->BidOrders.Length == 2) {
        for (std::uint32_t index = 0; index < 2; ++index) {
            const typename T::BidOrdersItem* const order =
                body->BidOrders[index];
            checks->Check(
                order != nullptr && order->OrderQty > 0 &&
                    order->OrderQty % 5 == 0,
                std::string(label) +
                    ": BidOrders OrderQty is not a positive multiple of 5");
        }
    }
    if (body->AskOrders.Length == 2) {
        for (std::uint32_t index = 0; index < 2; ++index) {
            const typename T::AskOrdersItem* const order =
                body->AskOrders[index];
            checks->Check(
                order != nullptr && order->OrderQty > 0 &&
                    order->OrderQty % 5 == 0,
                std::string(label) +
                    ": AskOrders OrderQty is not a positive multiple of 5");
        }
    }
}

class DceGfOrderHandler final : public CheckedHandler {
public:
    bool Done() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return dce_count_ >= kTarget && gf_count_ >= kTarget;
    }

    void OnMDLDCEL2Message(const mdl::MDLMessage* message) override {
        if (message == nullptr || message->GetHead() == nullptr ||
            message->GetHead()->MessageID != dc::FutureOrder::MessageID) {
            return;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        if (dce_count_ >= kTarget) {
            return;
        }
        ++dce_count_;
        CheckDceOrder<dc::FutureOrder>(
            message, "DCE FutureOrder", this);
    }

    void OnMDLGFEXL2Message(const mdl::MDLMessage* message) override {
        if (message == nullptr || message->GetHead() == nullptr ||
            message->GetHead()->MessageID != gf::FutureOrder::MessageID) {
            return;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        if (gf_count_ >= kTarget) {
            return;
        }
        ++gf_count_;
        CheckDceOrder<gf::FutureOrder>(
            message, "GFEX FutureOrder", this);
    }

private:
    static const std::uint32_t kTarget = 12;
    std::uint32_t dce_count_ = 0;
    std::uint32_t gf_count_ = 0;
};

template <typename T, typename BidList, typename AskList>
void CheckInt32Snapshot(const mdl::MDLMessage* message,
                        BidList T::* bids_member,
                        AskList T::* asks_member,
                        const char* label,
                        CheckedHandler* checks) {
    const T* const body = checks->Body<T>(message, label);
    if (body == nullptr) {
        return;
    }
    checks->Check(
        body->Volume > 0 && body->Volume <= INT_MAX &&
            body->Volume % 429496 == 0,
        std::string(label) +
            ": cumulative Volume is not positive/int32/lot-aligned");
    const BidList& bids = body->*bids_member;
    const AskList& asks = body->*asks_member;
    checks->Check(
        bids.Length == 1,
        std::string(label) + ": bid book depth is not one");
    checks->Check(
        asks.Length == 1,
        std::string(label) + ": ask book depth is not one");
    if (bids.Length == 1) {
        const typename BidList::value_type* const bid = bids[0];
        checks->Check(
            bid != nullptr && bid->Volume > 0 &&
                bid->Volume <= INT_MAX &&
                bid->Volume % 429496 == 0,
            std::string(label) +
                ": bid Volume is not positive/int32/lot-aligned");
    }
    if (asks.Length == 1) {
        const typename AskList::value_type* const ask = asks[0];
        checks->Check(
            ask != nullptr && ask->Volume > 0 &&
                ask->Volume <= INT_MAX &&
                ask->Volume % 429496 == 0,
            std::string(label) +
                ": ask Volume is not positive/int32/lot-aligned");
    }
}

class Int32BoundaryHandler final : public CheckedHandler {
public:
    bool Done() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return cffex_count_ >= kTarget && ctp_count_ >= kTarget &&
               crude_count_ >= kTarget;
    }

    void OnMDLCFFEXL2Message(const mdl::MDLMessage* message) override {
        if (message == nullptr || message->GetHead() == nullptr ||
            message->GetHead()->MessageID != cf::Future::MessageID) {
            return;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        if (cffex_count_ >= kTarget) {
            return;
        }
        ++cffex_count_;
        CheckInt32Snapshot(
            message,
            &cf::Future::BidPriceLevel,
            &cf::Future::AskPriceLevel,
            "CFFEX Future",
            this);
    }

    void OnMDLSHFEL2Message(const mdl::MDLMessage* message) override {
        if (message == nullptr || message->GetHead() == nullptr) {
            return;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        if (message->GetHead()->MessageID == sf::CTPFuture::MessageID &&
            ctp_count_ < kTarget) {
            ++ctp_count_;
            CheckInt32Snapshot(
                message,
                &sf::CTPFuture::BidBook,
                &sf::CTPFuture::AskBook,
                "SHFE CTPFuture",
                this);
        } else if (
            message->GetHead()->MessageID == sf::CrudeFuture::MessageID &&
            crude_count_ < kTarget) {
            ++crude_count_;
            CheckInt32Snapshot(
                message,
                &sf::CrudeFuture::BidBook,
                &sf::CrudeFuture::AskBook,
                "SHFE CrudeFuture",
                this);
        }
    }

private:
    static const std::uint32_t kTarget = 8;
    std::uint32_t cffex_count_ = 0;
    std::uint32_t ctp_count_ = 0;
    std::uint32_t crude_count_ = 0;
};

template <typename T>
void CheckShanghaiTrade(const mdl::MDLMessage* message,
                        const char* label,
                        CheckedHandler* checks,
                        std::int64_t* maximum_money) {
    const T* const body = checks->Body<T>(message, label);
    if (body == nullptr) {
        return;
    }
    const std::int64_t price = body->TradPrice.m_Value;
    const std::int64_t raw_volume = body->TradVolume.m_Value;
    checks->Check(
        price > 0 && price % 1000 == 0,
        std::string(label) + ": TradPrice is not tick-aligned");
    checks->Check(
        raw_volume > 0 && raw_volume % 1000 == 0,
        std::string(label) + ": TradVolume has the wrong scale");
    if (raw_volume <= 0 || raw_volume % 1000 != 0) {
        return;
    }
    const std::int64_t quantity = raw_volume / 1000;
    checks->Check(
        quantity % 1000000 == 0,
        std::string(label) + ": quantity is not lot-aligned");
    checks->Check(
        quantity <= std::numeric_limits<std::int64_t>::max() / 100 &&
            price <= std::numeric_limits<std::int64_t>::max() /
                         (quantity * 100),
        std::string(label) +
            ": price/quantity cannot be multiplied safely");
    if (quantity <= std::numeric_limits<std::int64_t>::max() / 100 &&
        price <= std::numeric_limits<std::int64_t>::max() /
                     (quantity * 100)) {
        const std::int64_t expected = price * quantity * 100;
        const std::int64_t actual = body->TradeMoney.m_Value;
        checks->Check(
            actual == expected,
            std::string(label) +
                ": TradeMoney raw value is not price*quantity*100");
        *maximum_money = std::max(*maximum_money, actual);
    }
}

class ShanghaiMoneyHandler final : public CheckedHandler {
public:
    bool Done() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return v1_count_ >= kTarget && v2_count_ >= kTarget;
    }

    std::int64_t MaximumMoney() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return maximum_money_;
    }

    void OnMDLSHL2Message(const mdl::MDLMessage* message) override {
        if (message == nullptr || message->GetHead() == nullptr) {
            return;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        if (message->GetHead()->MessageID ==
                sh::SHL2Transaction::MessageID &&
            v1_count_ < kTarget) {
            ++v1_count_;
            CheckShanghaiTrade<sh::SHL2Transaction>(
                message,
                "SH SHL2Transaction",
                this,
                &maximum_money_);
        } else if (
            message->GetHead()->MessageID ==
                sh::SHL2Transaction2::MessageID &&
            v2_count_ < kTarget) {
            ++v2_count_;
            CheckShanghaiTrade<sh::SHL2Transaction2>(
                message,
                "SH SHL2Transaction2",
                this,
                &maximum_money_);
        }
    }

private:
    // At this deliberately extreme price/lot, only a small number of trades
    // fit in the finite cumulative-turnover field before coherent suppression.
    static const std::uint32_t kTarget = 1;
    std::uint32_t v1_count_ = 0;
    std::uint32_t v2_count_ = 0;
    std::int64_t maximum_money_ = 0;
};

struct DceCumulativeSample {
    std::int64_t last_price = 0;
    std::int64_t last_volume = 0;
    std::int64_t volume = 0;
    std::int64_t turnover = 0;
};

class DceCumulativeSaturationHandler final : public CheckedHandler {
public:
    bool Done() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return count_ >= 2;
    }

    void OnMDLDCEL2Message(const mdl::MDLMessage* message) override {
        if (message == nullptr || message->GetHead() == nullptr ||
            message->GetHead()->MessageID != dc::Future::MessageID) {
            return;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        if (count_ >= 2) {
            return;
        }
        const dc::Future* const body =
            Body<dc::Future>(message, "DCE cumulative saturation");
        if (body == nullptr) {
            return;
        }
        DceCumulativeSample sample;
        sample.last_price = body->LastPrice.m_Value;
        sample.last_volume = body->LastVolume;
        sample.volume = body->Volume;
        sample.turnover = body->Turnover.m_Value;
        samples_[count_++] = sample;
        if (count_ != 2) {
            return;
        }

        const DceCumulativeSample& first = samples_[0];
        const DceCumulativeSample& second = samples_[1];
        Check(
            first.last_volume == 1000000 &&
                first.volume == first.last_volume,
            "DCE saturation: first accepted trade is not exactly one lot");
        Check(
            first.turnover ==
                first.last_price * first.last_volume,
            "DCE saturation: first turnover does not reconcile");
        Check(
            second.last_volume == 0,
            "DCE saturation: unrepresentable next lot is reported as traded");
        Check(
            second.volume == first.volume &&
                second.turnover == first.turnover &&
                second.last_price == first.last_price,
            "DCE saturation: frozen cumulative snapshot is incoherent");
    }

private:
    std::uint32_t count_ = 0;
    DceCumulativeSample samples_[2];
};

struct DailySnapshot {
    std::uint32_t date = 0;
    std::int64_t open = 0;
    std::int64_t high = 0;
    std::int64_t low = 0;
    std::int64_t last = 0;
    std::int32_t volume = 0;
    std::int64_t turnover = 0;
};

class RolloverAggregateHandler final : public CheckedHandler {
public:
    bool Done() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return have_new_day_;
    }

    void OnMDLCFFEXL2Message(const mdl::MDLMessage* message) override {
        if (message == nullptr || message->GetHead() == nullptr ||
            message->GetHead()->MessageID != cf::Future::MessageID) {
            return;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        if (have_new_day_) {
            return;
        }
        const cf::Future* const body =
            Body<cf::Future>(message, "rollover CFFEX Future");
        if (body == nullptr) {
            return;
        }
        DailySnapshot current;
        current.date = body->ActionDay.m_Value;
        current.open = body->OpenPrice.m_Value;
        current.high = body->HighPrice.m_Value;
        current.low = body->LowPrice.m_Value;
        current.last = body->LastPrice.m_Value;
        current.volume = body->Volume;
        current.turnover = body->Turnover.m_Value;

        if (current.date == kOldDate) {
            last_old_day_ = current;
            have_old_day_ = true;
            return;
        }
        if (!have_old_day_) {
            Fail("rollover: new day arrived before an old-day observation");
            have_new_day_ = true;
            return;
        }

        first_new_day_ = current;
        have_new_day_ = true;
        Check(
            first_new_day_.date == kNewDate,
            "rollover: Friday did not advance to Monday");
        Check(
            last_old_day_.volume >= 51,
            "rollover: did not accumulate the guaranteed old-day volume");
        Check(
            first_new_day_.volume > 0 &&
                first_new_day_.volume <= 50,
            "rollover: new-day Volume carries old-day volume");
        Check(
            first_new_day_.turnover ==
                first_new_day_.last * first_new_day_.volume,
            "rollover: new-day Turnover is not based only on its first trade");
        Check(
            first_new_day_.open == last_old_day_.last,
            "rollover: new-day OpenPrice is not the prior last price");
        Check(
            first_new_day_.high ==
                    std::max(first_new_day_.open, first_new_day_.last) &&
                first_new_day_.low ==
                    std::min(first_new_day_.open, first_new_day_.last),
            "rollover: new-day OHLC did not restart consistently");
    }

private:
    static const std::uint32_t kOldDate = 20260109;
    static const std::uint32_t kNewDate = 20260112;
    bool have_old_day_ = false;
    bool have_new_day_ = false;
    DailySnapshot last_old_day_;
    DailySnapshot first_new_day_;
};

void AppendErrors(const std::string& scenario,
                  const std::vector<std::string>& source,
                  std::vector<std::string>* destination) {
    for (const std::string& error : source) {
        destination->push_back(scenario + ": " + error);
    }
}

void TestShenzhenExtreme(std::vector<std::string>* errors) {
    mock::Config config = BaseConfig();
    config.book_depth = 50;
    config.orders_per_level = 100;
    config.instruments.push_back(Instrument(
        mdl::MDLSID_MDL_SZL2,
        "SZEDGE",
        100000000,
        1000,
        1000000));

    ShenzhenExtremeHandler handler;
    RunScenario(
        config,
        &handler,
        [](mdl::Subscriber* subscriber) {
            subscriber->SubcribeMessage<sz::MarketData>();
        },
        "SZ extreme weighted book",
        errors);
    AppendErrors(
        "SZ extreme weighted book", handler.Errors(), errors);
}

void TestDceGfOrderLots(std::vector<std::string>* errors) {
    mock::Config config = BaseConfig();
    config.orders_per_level = 2;
    config.instruments.push_back(Instrument(
        mdl::MDLSID_MDL_DCEL2, "DCEEDGE", 500000, 100, 5));
    config.instruments.push_back(Instrument(
        mdl::MDLSID_MDL_GFEXL2, "GFEEDGE", 600000, 100, 5));

    DceGfOrderHandler handler;
    RunScenario(
        config,
        &handler,
        [](mdl::Subscriber* subscriber) {
            subscriber->SubcribeMessage<dc::FutureOrder>();
            subscriber->SubcribeMessage<gf::FutureOrder>();
        },
        "DCE/GFEX order lot grid",
        errors);
    AppendErrors(
        "DCE/GFEX order lot grid", handler.Errors(), errors);
}

void TestInt32BookBoundary(std::vector<std::string>* errors) {
    mock::Config config = BaseConfig();
    config.book_depth = 1;
    config.orders_per_level = 100;
    config.instruments.push_back(Instrument(
        mdl::MDLSID_MDL_CFFEXL2,
        "CFFEDGE",
        500000,
        100,
        429496));
    config.instruments.push_back(Instrument(
        mdl::MDLSID_MDL_SHFEL2,
        "SHFEDGE",
        600000,
        100,
        429496));

    const std::uint64_t maximum_level_volume =
        UINT64_C(429496) * UINT64_C(100) * UINT64_C(50);
    if (maximum_level_volume > static_cast<std::uint64_t>(INT_MAX) ||
        static_cast<std::uint64_t>(INT_MAX) - maximum_level_volume > 5000U) {
        errors->push_back(
            "int32 boundary setup is not close to the legal int32 maximum");
    }

    Int32BoundaryHandler handler;
    RunScenario(
        config,
        &handler,
        [](mdl::Subscriber* subscriber) {
            subscriber->SubcribeMessage<cf::Future>();
            subscriber->SubcribeMessage<sf::CTPFuture>();
            subscriber->SubcribeMessage<sf::CrudeFuture>();
        },
        "CFFEX/SHFE int32 boundary",
        errors);
    AppendErrors(
        "CFFEX/SHFE int32 boundary", handler.Errors(), errors);
}

void TestShanghaiMoneyBoundary(std::vector<std::string>* errors) {
    mock::Config config = BaseConfig();
    config.seed = UINT64_C(0x53484d4f4e4559);
    config.book_depth = 1;
    config.orders_per_level = 1;
    config.instruments.push_back(Instrument(
        mdl::MDLSID_MDL_SHL2,
        "SHEDGE",
        838000000,
        1000,
        1000000));

    const std::int64_t theoretical_price = 921800000;
    const std::int64_t theoretical_quantity = 100000000;
    const std::int64_t theoretical_money =
        theoretical_price * theoretical_quantity * 100;
    const long double fraction =
        static_cast<long double>(theoretical_money) /
        static_cast<long double>(
            std::numeric_limits<std::int64_t>::max());
    if (fraction < 0.999L) {
        errors->push_back(
            "SH money boundary setup is not close to int64 maximum");
    }

    ShanghaiMoneyHandler handler;
    RunScenario(
        config,
        &handler,
        [](mdl::Subscriber* subscriber) {
            subscriber->SubcribeMessage<sh::SHL2Transaction>();
            subscriber->SubcribeMessage<sh::SHL2Transaction2>();
        },
        "SH TradeMoney boundary",
        errors);
    AppendErrors(
        "SH TradeMoney boundary", handler.Errors(), errors);
    if (handler.MaximumMoney() <= 0) {
        errors->push_back(
            "SH TradeMoney boundary: no positive TradeMoney was observed");
    }
}

void TestDceCumulativeSaturation(std::vector<std::string>* errors) {
    mock::Config invalid = BaseConfig();
    invalid.instruments.push_back(Instrument(
        mdl::MDLSID_MDL_DCEL2,
        "DCEREJ",
        INT64_C(3900000000000),
        INT64_C(1000000),
        1000000));
    if (mock::ValidateConfig(invalid).find("cumulative turnover") ==
        std::string::npos) {
        errors->push_back(
            "DCE saturation: config without one-lot turnover room "
            "was not rejected");
    }

    mock::Config config = BaseConfig();
    config.seed = UINT64_C(0x444345534154);
    config.book_depth = 1;
    config.orders_per_level = 1;
    config.instruments.push_back(Instrument(
        mdl::MDLSID_MDL_DCEL2,
        "DCESAT",
        INT64_C(3800000000000),
        INT64_C(1000000),
        1000000));

    DceCumulativeSaturationHandler handler;
    RunScenario(
        config,
        &handler,
        [](mdl::Subscriber* subscriber) {
            subscriber->SubcribeMessage<dc::Future>();
        },
        "DCE cumulative saturation",
        errors);
    AppendErrors(
        "DCE cumulative saturation", handler.Errors(), errors);
}

void TestDailyRollover(std::vector<std::string>* errors) {
    mock::Config config = BaseConfig();
    config.seed = UINT64_C(0x524f4c4c4f5645);
    config.messages_per_second = 5000;
    config.simulated_start_date = 20260109;
    config.simulated_start_time = 145959949;
    config.simulated_min_step_ms = 1;
    config.simulated_max_step_ms = 1;
    config.instruments.push_back(Instrument(
        mdl::MDLSID_MDL_CFFEXL2, "ROLLOVER", 500000, 100, 1));

    RolloverAggregateHandler handler;
    RunScenario(
        config,
        &handler,
        [](mdl::Subscriber* subscriber) {
            subscriber->SubcribeMessage<cf::Future>();
        },
        "derivative daily rollover",
        errors);
    AppendErrors(
        "derivative daily rollover", handler.Errors(), errors);
}

} // namespace

int main() {
    std::vector<std::string> errors;
    TestShenzhenExtreme(&errors);
    TestDceGfOrderLots(&errors);
    TestInt32BookBoundary(&errors);
    TestShanghaiMoneyBoundary(&errors);
    TestDceCumulativeSaturation(&errors);
    TestDailyRollover(&errors);

    if (!errors.empty()) {
        for (const std::string& error : errors) {
            std::cerr << "FAIL: " << error << '\n';
        }
        std::cerr << errors.size()
                  << " numeric edge contract failure(s)\n";
        return 1;
    }
    std::cout << "numeric edge contracts passed\n";
    return 0;
}
