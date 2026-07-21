#include "l2mock/l2_mock.h"

#include "mdl_cffexl2_msg.h"
#include "mdl_czcel2_msg.h"
#include "mdl_dcel2_msg.h"
#include "mdl_gfexl2_msg.h"
#include "mdl_shfel2_msg.h"
#include "mdl_shl2_msg.h"
#include "mdl_szl2_msg.h"

#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <type_traits>
#include <vector>

namespace mdl = datayes::mdl;
namespace mock = datayes::mdl::mock;
namespace sh = datayes::mdl::mdl_shl2_msg;
namespace sz = datayes::mdl::mdl_szl2_msg;
namespace cf = datayes::mdl::mdl_cffexl2_msg;
namespace sf = datayes::mdl::mdl_shfel2_msg;
namespace cz = datayes::mdl::mdl_czcel2_msg;
namespace dc = datayes::mdl::mdl_dcel2_msg;
namespace gf = datayes::mdl::mdl_gfexl2_msg;

namespace {

const std::uint32_t kBookDepth = 3;
const std::uint32_t kOrdersPerLevel = 2;
const std::uint32_t kSimulatedDate = 20240229;
const std::size_t kExpectedKeyCount = 28;

struct Key {
    std::uint8_t service_id;
    std::uint16_t service_version;
    std::uint16_t message_id;

    bool operator<(const Key& other) const {
        return std::tie(service_id, service_version, message_id) <
               std::tie(
                   other.service_id,
                   other.service_version,
                   other.message_id);
    }
};

Key MakeKey(const mdl::MDLMessageHead& head) {
    return Key{head.ServiceID, head.ServiceVersion, head.MessageID};
}

Key MakeKey(const mock::MessageKey& key) {
    return Key{key.service_id, key.service_version, key.message_id};
}

std::string KeyText(const Key& key) {
    std::ostringstream out;
    out << "SID=" << static_cast<unsigned int>(key.service_id)
        << "/VER=" << key.service_version << "/MID=" << key.message_id;
    return out.str();
}

std::uint64_t Pow10(std::uint32_t placement) {
    std::uint64_t value = 1;
    for (std::uint32_t i = 0; i < placement; ++i) {
        value *= 10;
    }
    return value;
}

bool IsSessionTime(const mdl::MDLTime& time) {
    if (!time.IsValid()) {
        return false;
    }
    const std::uint32_t value = time.m_Value;
    return (value >= 93000000U && value <= 113000000U) ||
           (value >= 130000000U && value <= 150000000U);
}

struct ExpectedInstrument {
    const char* security_id;
    std::int64_t reference_milli;
    std::int64_t tick_milli;
    std::int64_t lot_size;
};

ExpectedInstrument ExpectedFor(const Key& key) {
    switch (key.service_id) {
    case mdl::MDLSID_MDL_SHL2:
        return ExpectedInstrument{"SH0001", 100000, 10, 100};
    case mdl::MDLSID_MDL_SZL2:
        return ExpectedInstrument{"SZ0001", 120000, 10, 100};
    case mdl::MDLSID_MDL_CFFEXL2:
        return key.message_id == cf::Option::MessageID
                   ? ExpectedInstrument{"CFF_O", 600000, 100, 5}
                   : ExpectedInstrument{"CFF_F", 500000, 100, 5};
    case mdl::MDLSID_MDL_SHFEL2:
        return key.message_id == sf::CTPOption::MessageID ||
                       key.message_id == sf::CrudeOption::MessageID
                   ? ExpectedInstrument{"SHF_O", 800000, 100, 5}
                   : ExpectedInstrument{"SHF_F", 700000, 100, 5};
    case mdl::MDLSID_MDL_CZCEL2:
        return key.message_id == cz::CTPOption::MessageID
                   ? ExpectedInstrument{"CZE_O", 1000000, 100, 5}
                   : ExpectedInstrument{"CZE_F", 900000, 100, 5};
    case mdl::MDLSID_MDL_DCEL2:
        return key.message_id == dc::Option::MessageID ||
                       key.message_id == dc::OptionOrder::MessageID
                   ? ExpectedInstrument{"DCE_O", 1200000, 100, 5}
                   : ExpectedInstrument{"DCE_F", 1100000, 100, 5};
    case mdl::MDLSID_MDL_GFEXL2:
        return key.message_id == gf::Option::MessageID ||
                       key.message_id == gf::OptionOrder::MessageID
                   ? ExpectedInstrument{"GFE_O", 1400000, 100, 5}
                   : ExpectedInstrument{"GFE_F", 1300000, 100, 5};
    default:
        return ExpectedInstrument{"", 0, 1, 1};
    }
}

class MessageContext {
public:
    MessageContext(
        const mdl::MDLMessage* message,
        std::vector<std::string>* errors)
        : message_(message), errors_(errors) {
        if (message_ == nullptr) {
            Fail("callback supplied a null MDLMessage");
            return;
        }
        head_ = message_->GetHead();
        if (head_ == nullptr) {
            Fail("MDLMessage::GetHead() returned null");
            return;
        }
        key_ = MakeKey(*head_);
        label_ = KeyText(key_);

        Check(
            head_->HeadSize == sizeof(mdl::MDLMessageHead),
            "HeadSize is not sizeof(MDLMessageHead)");
        Check(
            head_->MessageEncoding == mdl::MDLEID_BINARY,
            "MessageEncoding is not MDLEID_BINARY");
        Check(head_->ServiceVersion == 101, "service version is not 101");
        Check(head_->SequenceID != 0, "SequenceID is zero");
        Check(
            IsSessionTime(head_->LocalTime),
            "LocalTime is invalid or outside the simulated trading session");

        if (head_->MessageSize < head_->HeadSize) {
            Fail("MessageSize is smaller than HeadSize");
            return;
        }
        body_size_ =
            static_cast<std::size_t>(head_->MessageSize - head_->HeadSize);
        body_ = message_->GetBody();
        if (body_ == nullptr) {
            Fail("GetBody() returned null");
            return;
        }
        Check(
            message_->GetBodySize() == body_size_,
            "GetBodySize() disagrees with the wire header");
        usable_ = true;
    }

    bool usable() const {
        return usable_;
    }

    const mdl::MDLMessageHead* head() const {
        return head_;
    }

    const Key& key() const {
        return key_;
    }

    void Check(bool condition, const std::string& detail) {
        if (!condition) {
            Fail(detail);
        }
    }

    void CheckTime(const mdl::MDLTime& time, const char* field_name) {
        Check(time.IsValid(), std::string(field_name) + " is not valid");
        Check(
            IsSessionTime(time),
            std::string(field_name) +
                " is outside 09:30-11:30 and 13:00-15:00");
        if (head_ != nullptr) {
            Check(
                time.m_Value == head_->LocalTime.m_Value,
                std::string(field_name) + " differs from header LocalTime");
        }
    }

    void CheckDates(
        const mdl::MDLDate& action_day,
        const mdl::MDLDate& trading_day) {
        Check(action_day.IsValid(), "ActionDay is invalid");
        Check(trading_day.IsValid(), "TradDay is invalid");
        Check(
            action_day.m_Value == trading_day.m_Value,
            "ActionDay and TradDay differ");
        Check(
            action_day.m_Value == kSimulatedDate,
            "ActionDay does not equal the configured simulated_start_date");
    }

    template <typename T>
    const T* Body() {
        if (!usable_) {
            return nullptr;
        }
        Check(
            static_cast<int>(head_->ServiceID) ==
                static_cast<int>(T::ServiceID),
            "body type ServiceID disagrees with the header");
        Check(
            head_->ServiceVersion == T::ServiceVer,
            "body type ServiceVer disagrees with the header");
        Check(
            head_->MessageID == T::MessageID,
            "body type MessageID disagrees with the header");
        if (body_size_ < sizeof(T)) {
            Fail("body is smaller than the fixed SDK payload");
            return nullptr;
        }
        fixed_size_ = sizeof(T);
        return reinterpret_cast<const T*>(body_);
    }

    std::string String(
        const mdl::MDLAnsiString& field,
        const char* field_name) {
        if (!InBody(&field, sizeof(field))) {
            Fail(std::string(field_name) + " string descriptor is out of body");
            return std::string();
        }
        if (field.Length == 0) {
            Check(
                field.Offset == 0,
                std::string(field_name) +
                    " empty string must have relative Offset 0");
            return std::string();
        }
        if (field.Offset == 0) {
            Fail(
                std::string(field_name) +
                " non-empty string has relative Offset 0");
            return std::string();
        }

        const std::uintptr_t descriptor =
            reinterpret_cast<std::uintptr_t>(&field);
        if (field.Offset >
            std::numeric_limits<std::uintptr_t>::max() - descriptor) {
            Fail(std::string(field_name) + " relative offset overflows");
            return std::string();
        }
        const std::uintptr_t data_address = descriptor + field.Offset;
        const char* const data = reinterpret_cast<const char*>(data_address);
        const std::size_t bytes = static_cast<std::size_t>(field.Length) + 1U;
        if (!InBody(data, bytes)) {
            Fail(
                std::string(field_name) +
                " relative data range is outside the body");
            return std::string();
        }
        Check(
            data_address >=
                reinterpret_cast<std::uintptr_t>(body_) + fixed_size_,
            std::string(field_name) +
                " relative offset points into the fixed payload");
        Check(
            data[field.Length] == '\0',
            std::string(field_name) + " lacks its trailing NUL");
        Check(
            field.c_str() == data,
            std::string(field_name) +
                " c_str() did not resolve the relative offset");
        const std::string value(data, field.Length);
        Check(
            field.std_str() == value,
            std::string(field_name) +
                " std_str() did not resolve the relative bytes");
        return value;
    }

    void CheckString(
        const mdl::MDLAnsiString& field,
        const char* field_name,
        const std::string& expected) {
        const std::string actual = String(field, field_name);
        Check(
            actual == expected,
            std::string(field_name) + " is '" + actual +
                "', expected '" + expected + "'");
    }

    template <typename Item>
    const Item* List(
        const mdl::MDLListT<Item>& field,
        std::uint32_t expected_count,
        const char* field_name) {
        if (!InBody(&field, sizeof(mdl::MDLList))) {
            Fail(std::string(field_name) + " list descriptor is out of body");
            return nullptr;
        }
        Check(
            field.Length == expected_count,
            std::string(field_name) + " has Length " +
                std::to_string(field.Length) + ", expected " +
                std::to_string(expected_count));
        if (field.Length == 0) {
            Check(
                field.Offset == 0,
                std::string(field_name) +
                    " empty list must have relative Offset 0");
            return nullptr;
        }
        if (field.Offset == 0) {
            Fail(
                std::string(field_name) +
                " non-empty list has relative Offset 0");
            return nullptr;
        }
        if (sizeof(Item) != 0 &&
            field.Length >
                std::numeric_limits<std::size_t>::max() / sizeof(Item)) {
            Fail(std::string(field_name) + " list byte size overflows");
            return nullptr;
        }

        const std::uintptr_t descriptor =
            reinterpret_cast<std::uintptr_t>(&field);
        if (field.Offset >
            std::numeric_limits<std::uintptr_t>::max() - descriptor) {
            Fail(std::string(field_name) + " relative offset overflows");
            return nullptr;
        }
        const std::uintptr_t data_address = descriptor + field.Offset;
        const Item* const data =
            reinterpret_cast<const Item*>(data_address);
        const std::size_t bytes =
            static_cast<std::size_t>(field.Length) * sizeof(Item);
        if (!InBody(data, bytes)) {
            Fail(
                std::string(field_name) +
                " relative item range is outside the body");
            return nullptr;
        }
        Check(
            data_address >=
                reinterpret_cast<std::uintptr_t>(body_) + fixed_size_,
            std::string(field_name) +
                " relative offset points into the fixed payload");
        Check(
            field[0] == data,
            std::string(field_name) +
                " operator[] did not resolve the relative offset");
        Check(
            field.begin() == data,
            std::string(field_name) +
                " begin() did not resolve the relative offset");
        Check(
            field.end() == data + field.Length,
            std::string(field_name) +
                " end() did not resolve the relative range");
        return field.Length == expected_count ? data : nullptr;
    }

    template <std::uint32_t Placement>
    std::int64_t PriceMilli(
        const mdl::MDLDoubleT<Placement>& value,
        const char* field_name,
        std::int64_t tick_milli,
        bool positive = true) {
        const std::int64_t raw = value.m_Value;
        Check(
            raw != mdl::MDLDoubleT<Placement>::s_NullValue,
            std::string(field_name) + " is null");
        if (raw == mdl::MDLDoubleT<Placement>::s_NullValue) {
            return 0;
        }
        Check(
            value.GetDecimalPlace() == Placement,
            std::string(field_name) + " reports the wrong decimal placement");
        Check(
            value.GetDecimalShift() == Pow10(Placement),
            std::string(field_name) + " reports the wrong decimal shift");
        const long double expected =
            static_cast<long double>(raw) /
            static_cast<long double>(Pow10(Placement));
        Check(
            std::fabs(
                static_cast<long double>(value.GetDouble()) - expected) <
                1e-9L,
            std::string(field_name) +
                " GetDouble() disagrees with its fixed-point raw value");

        std::int64_t milli = 0;
        if constexpr (Placement >= 3) {
            const std::int64_t factor =
                static_cast<std::int64_t>(Pow10(Placement - 3));
            Check(
                raw % factor == 0,
                std::string(field_name) +
                    " cannot be represented as integer thousandths");
            milli = raw / factor;
        } else {
            milli =
                raw * static_cast<std::int64_t>(Pow10(3 - Placement));
        }
        if (positive) {
            Check(milli > 0, std::string(field_name) + " is not positive");
        }
        if (tick_milli > 0) {
            Check(
                milli % tick_milli == 0,
                std::string(field_name) +
                    " is not aligned to the configured tick");
        }
        return milli;
    }

    template <std::uint32_t Placement>
    std::int64_t PriceMilli(
        const mdl::MDLFloatT<Placement>& value,
        const char* field_name,
        std::int64_t tick_milli,
        bool positive = true) {
        const std::int64_t raw = value.m_Value;
        Check(
            raw != mdl::MDLFloatT<Placement>::s_NullValue,
            std::string(field_name) + " is null");
        if (raw == mdl::MDLFloatT<Placement>::s_NullValue) {
            return 0;
        }
        Check(
            value.GetDecimalPlace() == Placement,
            std::string(field_name) + " reports the wrong decimal placement");
        Check(
            static_cast<std::uint64_t>(value.GetDecimalShift()) ==
                Pow10(Placement),
            std::string(field_name) + " reports the wrong decimal shift");
        const long double expected =
            static_cast<long double>(raw) /
            static_cast<long double>(Pow10(Placement));
        Check(
            std::fabs(
                static_cast<long double>(value.GetFloat()) - expected) <
                1e-4L,
            std::string(field_name) +
                " GetFloat() disagrees with its fixed-point raw value");

        std::int64_t milli = 0;
        if constexpr (Placement >= 3) {
            const std::int64_t factor =
                static_cast<std::int64_t>(Pow10(Placement - 3));
            Check(
                raw % factor == 0,
                std::string(field_name) +
                    " cannot be represented as integer thousandths");
            milli = raw / factor;
        } else {
            milli =
                raw * static_cast<std::int64_t>(Pow10(3 - Placement));
        }
        if (positive) {
            Check(milli > 0, std::string(field_name) + " is not positive");
        }
        if (tick_milli > 0) {
            Check(
                milli % tick_milli == 0,
                std::string(field_name) +
                    " is not aligned to the configured tick");
        }
        return milli;
    }

private:
    bool InBody(const void* pointer, std::size_t bytes) const {
        if (body_ == nullptr) {
            return false;
        }
        const std::uintptr_t begin =
            reinterpret_cast<std::uintptr_t>(body_);
        const std::uintptr_t pointer_value =
            reinterpret_cast<std::uintptr_t>(pointer);
        if (pointer_value < begin) {
            return false;
        }
        const std::uintptr_t difference = pointer_value - begin;
        return difference <= body_size_ &&
               bytes <= body_size_ - static_cast<std::size_t>(difference);
    }

    void Fail(const std::string& detail) {
        errors_->push_back(
            (label_.empty() ? std::string("unknown-key") : label_) +
            ": " + detail);
    }

    const mdl::MDLMessage* message_ = nullptr;
    const mdl::MDLMessageHead* head_ = nullptr;
    const char* body_ = nullptr;
    std::size_t body_size_ = 0;
    std::size_t fixed_size_ = 0;
    std::vector<std::string>* errors_;
    Key key_{0, 0, 0};
    std::string label_;
    bool usable_ = false;
};

void CheckOhlc(
    MessageContext* context,
    std::int64_t open,
    std::int64_t high,
    std::int64_t low,
    std::int64_t last) {
    context->Check(high >= open, "HighPrice is below OpenPrice");
    context->Check(high >= last, "HighPrice is below LastPrice");
    context->Check(low <= open, "LowPrice is above OpenPrice");
    context->Check(low <= last, "LowPrice is above LastPrice");
    context->Check(high >= low, "HighPrice is below LowPrice");
}

void CheckBookOrder(
    MessageContext* context,
    const std::vector<std::int64_t>& bids,
    const std::vector<std::int64_t>& asks,
    std::int64_t tick_milli) {
    context->Check(
        bids.size() == kBookDepth, "bid book depth is not configured depth");
    context->Check(
        asks.size() == kBookDepth, "ask book depth is not configured depth");
    for (std::size_t i = 1; i < bids.size(); ++i) {
        context->Check(
            bids[i - 1] > bids[i], "bid prices are not strictly decreasing");
        context->Check(
            bids[i - 1] - bids[i] == tick_milli,
            "adjacent bid prices are not one configured tick apart");
    }
    for (std::size_t i = 1; i < asks.size(); ++i) {
        context->Check(
            asks[i - 1] < asks[i], "ask prices are not strictly increasing");
        context->Check(
            asks[i] - asks[i - 1] == tick_milli,
            "adjacent ask prices are not one configured tick apart");
    }
    if (!bids.empty() && !asks.empty()) {
        context->Check(
            bids.front() < asks.front(), "best bid is not below best ask");
    }
}

void CheckCumulative(
    MessageContext* context,
    std::int64_t volume,
    std::int64_t turnover_milli,
    std::int64_t low,
    std::int64_t high,
    std::int64_t reported_average_milli = -1) {
    context->Check(volume > 0, "cumulative Volume is not positive");
    context->Check(
        turnover_milli > 0, "cumulative Turnover is not positive");
    if (volume <= 0 || turnover_milli <= 0) {
        return;
    }
    const std::int64_t average = turnover_milli / volume;
    context->Check(
        average >= low && average <= high,
        "Turnover/Volume average is outside LowPrice..HighPrice");
    if (reported_average_milli >= 0) {
        context->Check(
            reported_average_milli == average,
            "reported AveragePrice does not reconcile with Turnover/Volume");
    }
}

template <typename T>
void CheckEventStrings(
    MessageContext* context,
    const T& body,
    const ExpectedInstrument& expected) {
    context->CheckString(body.MDStreamID, "MDStreamID", "");
    context->CheckString(
        body.SecurityID, "SecurityID", expected.security_id);
    context->CheckString(body.SecurityIDSource, "SecurityIDSource", "");
}

class CollectingHandler final : public mdl::MessageHandlerBase {
public:
    void OnMessage(
        mdl::Subscriber*,
        const mdl::MDLMessage* message) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (complete_) {
            return;
        }
        callback_threads_.insert(std::this_thread::get_id());

        MessageContext context(message, &errors_);
        if (!context.usable()) {
            condition_.notify_all();
            return;
        }
        if (context.head()->SequenceID <= last_sequence_) {
            context.Check(false, "SequenceID is not strictly increasing");
        }
        last_sequence_ = context.head()->SequenceID;

        try {
            Validate(&context);
        } catch (const std::exception& error) {
            errors_.push_back(
                KeyText(context.key()) +
                ": validator threw std::exception: " + error.what());
        } catch (...) {
            errors_.push_back(
                KeyText(context.key()) +
                ": validator threw an unknown exception");
        }
        seen_.insert(context.key());
        ++callbacks_;
        if (seen_.size() == kExpectedKeyCount) {
            complete_ = true;
        }
        condition_.notify_all();
    }

    bool WaitForAll(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return condition_.wait_for(lock, timeout, [this] {
            return complete_;
        });
    }

    std::set<Key> Seen() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return seen_;
    }

    std::vector<std::string> Errors() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return errors_;
    }

    std::size_t CallbackThreadCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return callback_threads_.size();
    }

    std::size_t CallbackCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return callbacks_;
    }

private:
    void Validate(MessageContext* context);
    void ValidateShanghai(MessageContext* context);
    void ValidateShenzhen(MessageContext* context);

    template <typename T>
    void ValidateCffex(MessageContext* context);

    template <typename T>
    void ValidateShfe(MessageContext* context);

    template <typename T>
    void ValidateCzce(MessageContext* context);

    template <typename T>
    void ValidateDceLikeSnapshot(MessageContext* context);

    template <typename T>
    void ValidateDceLikeOrder(MessageContext* context);

    template <typename T>
    void ValidateShenzhenSnapshot(MessageContext* context);

    mutable std::mutex mutex_;
    std::condition_variable condition_;
    std::set<Key> seen_;
    std::vector<std::string> errors_;
    std::set<std::thread::id> callback_threads_;
    std::uint64_t last_sequence_ = 0;
    std::size_t callbacks_ = 0;
    bool complete_ = false;

    std::int64_t sh_bid_order_ = 0;
    std::int64_t sh_ask_order_ = 0;
    std::int64_t sz_bid_application_ = 0;
    std::int64_t sz_ask_application_ = 0;
    std::uint32_t sz_legacy_bid_record_ = 0;
    std::uint32_t sz_legacy_ask_record_ = 0;
};

void CollectingHandler::ValidateShanghai(MessageContext* context) {
    const ExpectedInstrument expected = ExpectedFor(context->key());
    switch (context->head()->MessageID) {
    case sh::Order::MessageID: {
        const sh::Order* const body = context->Body<sh::Order>();
        if (body == nullptr) {
            return;
        }
        context->CheckTime(body->OrderTime, "OrderTime");
        context->CheckString(
            body->SecurityID, "SecurityID", expected.security_id);
        context->CheckString(body->OrderType, "OrderType", "");
        const std::string side =
            context->String(body->OrderBSFlag, "OrderBSFlag");
        context->Check(
            side == "B" || side == "S",
            "OrderBSFlag is neither B nor S");
        context->PriceMilli(
            body->OrderPrice,
            "OrderPrice",
            expected.tick_milli);
        context->Check(body->OrderNO > 0, "OrderNO is not positive");
        context->Check(
            body->OrderIndex > 0 && body->BizIndex > 0,
            "order indices are not positive");
        context->Check(
            body->OrderIndex == body->BizIndex,
            "OrderIndex and BizIndex do not reconcile");
        context->Check(
            body->Balance.m_Value > 0 &&
                body->Balance.m_Value % 1000 == 0,
            "Balance is not a positive integer quantity at scale 3");
        context->List(body->ExtraFields, 0, "ExtraFields");
        if (side == "B") {
            sh_bid_order_ = body->OrderNO;
        } else if (side == "S") {
            sh_ask_order_ = body->OrderNO;
        }
        return;
    }

    case sh::SHL2Transaction::MessageID: {
        const sh::SHL2Transaction* const body =
            context->Body<sh::SHL2Transaction>();
        if (body == nullptr) {
            return;
        }
        context->CheckTime(body->TradTime, "TradTime");
        context->CheckString(
            body->SecurityID, "SecurityID", expected.security_id);
        const std::string side =
            context->String(body->TradeBSFlag, "TradeBSFlag");
        context->Check(
            side == "B" || side == "S",
            "TradeBSFlag is neither B nor S");
        const std::int64_t price = context->PriceMilli(
            body->TradPrice, "TradPrice", expected.tick_milli);
        context->Check(
            body->TradVolume.m_Value > 0 &&
                body->TradVolume.m_Value % 1000 == 0,
            "TradVolume is not a positive integer quantity at scale 3");
        const std::int64_t quantity =
            body->TradVolume.m_Value / 1000;
        context->Check(
            body->TradeMoney.m_Value ==
                price * quantity * 100,
            "TradeMoney does not equal price times quantity at scale 5");
        context->Check(
            sh_bid_order_ != 0 && sh_ask_order_ != 0,
            "trade arrived before both buy and sell orders");
        context->Check(
            body->TradeBuyNo == sh_bid_order_,
            "TradeBuyNo does not reference the emitted buy order");
        context->Check(
            body->TradeSellNo == sh_ask_order_,
            "TradeSellNo does not reference the emitted sell order");
        return;
    }

    case sh::SHL2Transaction2::MessageID: {
        const sh::SHL2Transaction2* const body =
            context->Body<sh::SHL2Transaction2>();
        if (body == nullptr) {
            return;
        }
        context->CheckTime(body->TradTime, "TradTime");
        context->CheckString(
            body->SecurityID, "SecurityID", expected.security_id);
        const std::string side =
            context->String(body->TradeBSFlag, "TradeBSFlag");
        context->Check(
            side == "B" || side == "S",
            "TradeBSFlag is neither B nor S");
        const std::int64_t price = context->PriceMilli(
            body->TradPrice, "TradPrice", expected.tick_milli);
        context->Check(
            body->TradVolume.m_Value > 0 &&
                body->TradVolume.m_Value % 1000 == 0,
            "TradVolume is not a positive integer quantity at scale 3");
        const std::int64_t quantity =
            body->TradVolume.m_Value / 1000;
        context->Check(
            body->TradeMoney.m_Value ==
                price * quantity * 100,
            "TradeMoney does not equal price times quantity at scale 5");
        context->Check(
            sh_bid_order_ != 0 && sh_ask_order_ != 0,
            "trade arrived before both buy and sell orders");
        context->Check(
            body->TradeBuyNo == sh_bid_order_,
            "TradeBuyNo does not reference the emitted buy order");
        context->Check(
            body->TradeSellNo == sh_ask_order_,
            "TradeSellNo does not reference the emitted sell order");
        context->Check(body->BizIndex > 0, "BizIndex is not positive");
        context->List(body->ExtraFields, 0, "ExtraFields");
        return;
    }

    case sh::SHL2MarketData::MessageID: {
        typedef sh::SHL2MarketData T;
        typedef T::BidLevelsItem Bid;
        typedef Bid::NOrdersItem BidOrder;
        typedef T::SellLevelsItem Ask;
        typedef Ask::NoOrdersItem AskOrder;

        const T* const body = context->Body<T>();
        if (body == nullptr) {
            return;
        }
        context->CheckTime(body->UpdateTime, "UpdateTime");
        context->CheckString(
            body->SecurityID, "SecurityID", expected.security_id);
        context->CheckString(
            body->InstruStatus, "InstruStatus", "T");
        context->Check(
            body->PreCloPrice.m_Value == expected.reference_milli,
            "PreCloPrice does not equal configured reference price");
        const std::int64_t open = context->PriceMilli(
            body->OpenPrice, "OpenPrice", expected.tick_milli);
        const std::int64_t high = context->PriceMilli(
            body->HighPrice, "HighPrice", expected.tick_milli);
        const std::int64_t low = context->PriceMilli(
            body->LowPrice, "LowPrice", expected.tick_milli);
        const std::int64_t last = context->PriceMilli(
            body->LastPrice, "LastPrice", expected.tick_milli);
        CheckOhlc(context, open, high, low, last);
        context->Check(
            body->ClosePrice.IsNull(), "ClosePrice should be null");
        context->Check(body->IOPV.IsNull(), "IOPV should be null");
        context->Check(
            body->TradVolume.m_Value > 0 &&
                body->TradVolume.m_Value % 1000 == 0,
            "TradVolume is not a positive integer quantity at scale 3");
        context->Check(
            body->Turnover.m_Value > 0 &&
                body->Turnover.m_Value % 100 == 0,
            "Turnover is not valid at scale 5");
        const std::int64_t traded_volume =
            body->TradVolume.m_Value / 1000;
        const std::int64_t turnover_milli =
            body->Turnover.m_Value / 100;
        CheckCumulative(
            context, traded_volume, turnover_milli, low, high);

        const Bid* const bids =
            context->List(body->BidLevels, kBookDepth, "BidLevels");
        const Ask* const asks =
            context->List(body->SellLevels, kBookDepth, "SellLevels");
        std::vector<std::int64_t> bid_prices;
        std::vector<std::int64_t> ask_prices;
        std::int64_t total_bid_raw = 0;
        std::int64_t total_ask_raw = 0;
        std::int64_t weighted_bid = 0;
        std::int64_t weighted_ask = 0;
        std::uint32_t bid_orders = 0;
        std::uint32_t ask_orders = 0;

        if (bids != nullptr) {
            for (std::uint32_t i = 0; i < kBookDepth; ++i) {
                const std::int64_t price = context->PriceMilli(
                    bids[i].OrderPrice,
                    "BidLevels[].OrderPrice",
                    expected.tick_milli);
                bid_prices.push_back(price);
                context->Check(
                    bids[i].OrderVol.m_Value > 0 &&
                        bids[i].OrderVol.m_Value % 1000 == 0,
                    "BidLevels[].OrderVol has invalid scale");
                context->Check(
                    bids[i].OrderNum == kOrdersPerLevel,
                    "BidLevels[].OrderNum is wrong");
                const BidOrder* const orders = context->List(
                    bids[i].NOrders,
                    kOrdersPerLevel,
                    "BidLevels[].NOrders");
                std::int64_t queue_raw = 0;
                if (orders != nullptr) {
                    for (std::uint32_t j = 0;
                         j < kOrdersPerLevel;
                         ++j) {
                        context->Check(
                            orders[j].OrderQty.m_Value > 0 &&
                                orders[j].OrderQty.m_Value % 1000 == 0,
                            "bid nested OrderQty has invalid scale");
                        queue_raw += orders[j].OrderQty.m_Value;
                    }
                }
                context->Check(
                    queue_raw == bids[i].OrderVol.m_Value,
                    "bid nested queue does not sum to level volume");
                total_bid_raw += bids[i].OrderVol.m_Value;
                weighted_bid +=
                    price * (bids[i].OrderVol.m_Value / 1000);
                bid_orders += bids[i].OrderNum;
            }
        }
        if (asks != nullptr) {
            for (std::uint32_t i = 0; i < kBookDepth; ++i) {
                const std::int64_t price = context->PriceMilli(
                    asks[i].OrderPrice,
                    "SellLevels[].OrderPrice",
                    expected.tick_milli);
                ask_prices.push_back(price);
                context->Check(
                    asks[i].OrderVol.m_Value > 0 &&
                        asks[i].OrderVol.m_Value % 1000 == 0,
                    "SellLevels[].OrderVol has invalid scale");
                context->Check(
                    asks[i].OrderNum == kOrdersPerLevel,
                    "SellLevels[].OrderNum is wrong");
                const AskOrder* const orders = context->List(
                    asks[i].NoOrders,
                    kOrdersPerLevel,
                    "SellLevels[].NoOrders");
                std::int64_t queue_raw = 0;
                if (orders != nullptr) {
                    for (std::uint32_t j = 0;
                         j < kOrdersPerLevel;
                         ++j) {
                        context->Check(
                            orders[j].OrderQty.m_Value > 0 &&
                                orders[j].OrderQty.m_Value % 1000 == 0,
                            "ask nested OrderQty has invalid scale");
                        queue_raw += orders[j].OrderQty.m_Value;
                    }
                }
                context->Check(
                    queue_raw == asks[i].OrderVol.m_Value,
                    "ask nested queue does not sum to level volume");
                total_ask_raw += asks[i].OrderVol.m_Value;
                weighted_ask +=
                    price * (asks[i].OrderVol.m_Value / 1000);
                ask_orders += asks[i].OrderNum;
            }
        }
        CheckBookOrder(
            context, bid_prices, ask_prices, expected.tick_milli);
        context->Check(
            body->TotalBidVol.m_Value == total_bid_raw,
            "TotalBidVol does not reconcile with bid levels");
        context->Check(
            body->TotalAskVol.m_Value == total_ask_raw,
            "TotalAskVol does not reconcile with ask levels");
        context->Check(
            body->TotBidNum == bid_orders &&
                body->TotSellNum == ask_orders,
            "total order counts do not reconcile with levels");
        context->Check(
            body->BidNum == kBookDepth &&
                body->SellNum == kBookDepth,
            "reported book level counts are wrong");
        if (total_bid_raw > 0) {
            const std::int64_t quantity = total_bid_raw / 1000;
            context->Check(
                body->WAvgBidPri.m_Value == weighted_bid / quantity &&
                    body->AltWAvgBidPri.m_Value ==
                        weighted_bid / quantity,
                "weighted bid prices do not reconcile");
        }
        if (total_ask_raw > 0) {
            const std::int64_t quantity = total_ask_raw / 1000;
            context->Check(
                body->WAvgAskPri.m_Value == weighted_ask / quantity &&
                    body->AltWAvgAskPri.m_Value ==
                        weighted_ask / quantity,
                "weighted ask prices do not reconcile");
        }
        return;
    }
    default:
        context->Check(false, "unexpected Shanghai MID");
        return;
    }
}

template <typename T>
void CollectingHandler::ValidateShenzhenSnapshot(
    MessageContext* context) {
    typedef typename T::BidPriceLevelItem Bid;
    typedef typename Bid::OrdersItem BidOrder;
    typedef typename T::AskPriceLevelItem Ask;
    typedef typename Ask::OrdersItem AskOrder;

    const ExpectedInstrument expected = ExpectedFor(context->key());
    const T* const body = context->Body<T>();
    if (body == nullptr) {
        return;
    }
    context->CheckTime(body->UpdateTime, "UpdateTime");
    CheckEventStrings(context, *body, expected);
    context->CheckString(
        body->TradingPhaseCode, "TradingPhaseCode", "T");
    context->Check(
        body->PreCloPrice.m_Value ==
            expected.reference_milli * 10,
        "PreCloPrice does not use scale 4 for configured reference");

    const std::int64_t open = context->PriceMilli(
        body->OpenPrice, "OpenPrice", expected.tick_milli);
    const std::int64_t high = context->PriceMilli(
        body->HighPrice, "HighPrice", expected.tick_milli);
    const std::int64_t low = context->PriceMilli(
        body->LowPrice, "LowPrice", expected.tick_milli);
    const std::int64_t last = context->PriceMilli(
        body->LastPrice, "LastPrice", expected.tick_milli);
    CheckOhlc(context, open, high, low, last);
    context->Check(
        body->DifPrice1.m_Value ==
            body->LastPrice.m_Value -
                expected.reference_milli * 1000,
        "DifPrice1 does not equal LastPrice minus PreClose");
    context->Check(
        body->DifPrice2.m_Value ==
            body->LastPrice.m_Value - body->OpenPrice.m_Value,
        "DifPrice2 does not equal LastPrice minus OpenPrice");
    context->Check(body->Volume > 0, "Volume is not positive");
    context->Check(
        body->Turnover.m_Value > 0 &&
            body->Turnover.m_Value % 10 == 0,
        "Turnover does not use the expected scale 4");
    const std::int64_t turnover_milli =
        body->Turnover.m_Value / 10;
    CheckCumulative(
        context, body->Volume, turnover_milli, low, high);
    context->Check(
        body->HighLimitPrice.m_Value > body->LowLimitPrice.m_Value,
        "HighLimitPrice is not above LowLimitPrice");
    context->Check(body->PE1.IsNull(), "PE1 should be null");
    context->Check(body->PE2.IsNull(), "PE2 should be null");
    context->Check(body->IOPV.IsNull(), "IOPV should be null");

    const Bid* const bids = context->List(
        body->BidPriceLevel, kBookDepth, "BidPriceLevel");
    const Ask* const asks = context->List(
        body->AskPriceLevel, kBookDepth, "AskPriceLevel");
    std::vector<std::int64_t> bid_prices;
    std::vector<std::int64_t> ask_prices;
    std::int64_t total_bid = 0;
    std::int64_t total_ask = 0;
    std::int64_t weighted_bid = 0;
    std::int64_t weighted_ask = 0;

    if (bids != nullptr) {
        for (std::uint32_t i = 0; i < kBookDepth; ++i) {
            const std::int64_t price = context->PriceMilli(
                bids[i].Price,
                "BidPriceLevel[].Price",
                expected.tick_milli);
            bid_prices.push_back(price);
            context->Check(
                bids[i].Volume > 0,
                "BidPriceLevel[].Volume is not positive");
            context->Check(
                bids[i].NumOrders == kOrdersPerLevel,
                "BidPriceLevel[].NumOrders is wrong");
            const BidOrder* const orders = context->List(
                bids[i].Orders,
                kOrdersPerLevel,
                "BidPriceLevel[].Orders");
            std::int64_t queue = 0;
            if (orders != nullptr) {
                for (std::uint32_t j = 0;
                     j < kOrdersPerLevel;
                     ++j) {
                    context->Check(
                        orders[j].OrderQty > 0,
                        "bid nested OrderQty is not positive");
                    queue += orders[j].OrderQty;
                }
            }
            context->Check(
                queue == bids[i].Volume,
                "bid nested queue does not sum to level Volume");
            total_bid += bids[i].Volume;
            weighted_bid += price * bids[i].Volume;
        }
    }
    if (asks != nullptr) {
        for (std::uint32_t i = 0; i < kBookDepth; ++i) {
            const std::int64_t price = context->PriceMilli(
                asks[i].Price,
                "AskPriceLevel[].Price",
                expected.tick_milli);
            ask_prices.push_back(price);
            context->Check(
                asks[i].Volume > 0,
                "AskPriceLevel[].Volume is not positive");
            context->Check(
                asks[i].NumOrders == kOrdersPerLevel,
                "AskPriceLevel[].NumOrders is wrong");
            const AskOrder* const orders = context->List(
                asks[i].Orders,
                kOrdersPerLevel,
                "AskPriceLevel[].Orders");
            std::int64_t queue = 0;
            if (orders != nullptr) {
                for (std::uint32_t j = 0;
                     j < kOrdersPerLevel;
                     ++j) {
                    context->Check(
                        orders[j].OrderQty > 0,
                        "ask nested OrderQty is not positive");
                    queue += orders[j].OrderQty;
                }
            }
            context->Check(
                queue == asks[i].Volume,
                "ask nested queue does not sum to level Volume");
            total_ask += asks[i].Volume;
            weighted_ask += price * asks[i].Volume;
        }
    }
    CheckBookOrder(
        context, bid_prices, ask_prices, expected.tick_milli);
    context->Check(
        body->TotalBidQty == total_bid,
        "TotalBidQty does not reconcile with bid levels");
    context->Check(
        body->TotalOfferQty == total_ask,
        "TotalOfferQty does not reconcile with ask levels");
    if (total_bid > 0) {
        context->Check(
            body->WeightedAvgBidPx.m_Value ==
                (weighted_bid / total_bid) * 1000,
            "WeightedAvgBidPx does not reconcile with bid levels");
    }
    if (total_ask > 0) {
        context->Check(
            body->WeightedAvgOfferPx.m_Value ==
                (weighted_ask / total_ask) * 1000,
            "WeightedAvgOfferPx does not reconcile with ask levels");
    }

    if constexpr (std::is_same<T, sz::Snapshot300111_v3>::value) {
        context->Check(
            body->WeightedAvgPx.m_Value ==
                (turnover_milli / body->Volume) * 1000,
            "WeightedAvgPx does not reconcile with Turnover/Volume");
        context->Check(
            body->WeightedAvgPxPreClose.m_Value ==
                expected.reference_milli * 1000,
            "WeightedAvgPxPreClose has the wrong scale/value");
        context->Check(
            body->WeightedAvgPxChangeBP.IsNull(),
            "WeightedAvgPxChangeBP should be null without authoritative semantics");
        context->List(body->ExtendFields, 0, "ExtendFields");
    }
}

void CollectingHandler::ValidateShenzhen(MessageContext* context) {
    const ExpectedInstrument expected = ExpectedFor(context->key());
    switch (context->head()->MessageID) {
    case sz::Order::MessageID: {
        const sz::Order* const body = context->Body<sz::Order>();
        if (body == nullptr) {
            return;
        }
        context->CheckTime(body->OrderEntryTime, "OrderEntryTime");
        context->CheckString(
            body->SecurityID, "SecurityID", expected.security_id);
        context->CheckString(body->OrderKind, "OrderKind", "");
        const std::string side =
            context->String(body->FunctionCode, "FunctionCode");
        context->Check(
            side == "B" || side == "S",
            "FunctionCode is neither B nor S");
        context->PriceMilli(
            body->Price, "Price", expected.tick_milli);
        context->Check(body->OrderQty > 0, "OrderQty is not positive");
        context->Check(body->RecNo > 0, "RecNo is not positive");
        context->Check(body->SetNo > 0, "SetNo is not positive");
        if (side == "B") {
            sz_legacy_bid_record_ = body->RecNo;
        } else {
            sz_legacy_ask_record_ = body->RecNo;
        }
        return;
    }

    case sz::Trade::MessageID: {
        const sz::Trade* const body = context->Body<sz::Trade>();
        if (body == nullptr) {
            return;
        }
        context->CheckTime(body->TradTime, "TradTime");
        context->CheckString(
            body->SecurityID, "SecurityID", expected.security_id);
        context->CheckString(body->OrderKind, "OrderKind", "");
        const std::string side =
            context->String(body->FunctionCode, "FunctionCode");
        context->Check(
            side == "B" || side == "S",
            "FunctionCode is neither B nor S");
        context->PriceMilli(
            body->Price, "Price", expected.tick_milli);
        context->Check(body->TradeQty > 0, "TradeQty is not positive");
        context->Check(body->RecNo > 0, "RecNo is not positive");
        context->Check(
            body->BuyOrderRecNo != 0 &&
                body->SellOrderRecNo != 0,
            "legacy trade order references are zero");
        context->Check(
            body->BuyOrderRecNo != body->SellOrderRecNo,
            "legacy trade buy/sell references are identical");
        context->Check(
            sz_legacy_bid_record_ != 0 &&
                body->BuyOrderRecNo == sz_legacy_bid_record_,
            "legacy BuyOrderRecNo does not reference the emitted buy order");
        context->Check(
            sz_legacy_ask_record_ != 0 &&
                body->SellOrderRecNo == sz_legacy_ask_record_,
            "legacy SellOrderRecNo does not reference the emitted sell order");
        return;
    }

    case sz::Order300192_v2::MessageID: {
        const sz::Order300192_v2* const body =
            context->Body<sz::Order300192_v2>();
        if (body == nullptr) {
            return;
        }
        context->CheckTime(body->TransactTime, "TransactTime");
        CheckEventStrings(context, *body, expected);
        context->PriceMilli(
            body->Price, "Price", expected.tick_milli);
        context->Check(body->OrderQty > 0, "OrderQty is not positive");
        context->Check(
            body->ApplSeqNum > 0, "ApplSeqNum is not positive");
        context->Check(
            body->Side == mdl::SF_BUY ||
                body->Side == mdl::SF_SELL,
            "Side is neither SF_BUY nor SF_SELL");
        if (body->Side == mdl::SF_BUY) {
            sz_bid_application_ = body->ApplSeqNum;
        } else if (body->Side == mdl::SF_SELL) {
            sz_ask_application_ = body->ApplSeqNum;
        }
        return;
    }

    case sz::Transaction300191_v2::MessageID: {
        const sz::Transaction300191_v2* const body =
            context->Body<sz::Transaction300191_v2>();
        if (body == nullptr) {
            return;
        }
        context->CheckTime(body->TransactTime, "TransactTime");
        CheckEventStrings(context, *body, expected);
        context->PriceMilli(
            body->LastPx, "LastPx", expected.tick_milli);
        context->Check(body->LastQty > 0, "LastQty is not positive");
        context->Check(
            sz_bid_application_ != 0 &&
                sz_ask_application_ != 0,
            "transaction arrived before both v2 orders");
        context->Check(
            body->BidApplSeqNum == sz_bid_application_,
            "BidApplSeqNum does not reference the emitted buy order");
        context->Check(
            body->OfferApplSeqNum == sz_ask_application_,
            "OfferApplSeqNum does not reference the emitted sell order");
        context->Check(
            body->ApplSeqNum > body->BidApplSeqNum &&
                body->ApplSeqNum > body->OfferApplSeqNum,
            "trade application sequence does not follow its orders");
        return;
    }

    case sz::CombinedTick::MessageID: {
        const sz::CombinedTick* const body =
            context->Body<sz::CombinedTick>();
        if (body == nullptr) {
            return;
        }
        context->CheckTime(body->TransactTime, "TransactTime");
        CheckEventStrings(context, *body, expected);
        context->PriceMilli(
            body->Price, "Price", expected.tick_milli);
        context->Check(body->Qty > 0, "Qty is not positive");
        context->Check(
            body->BidApplSeqNum == sz_bid_application_,
            "CombinedTick BidApplSeqNum does not reference buy order");
        context->Check(
            body->OfferApplSeqNum == sz_ask_application_,
            "CombinedTick OfferApplSeqNum does not reference sell order");
        context->Check(
            body->ApplSeqNum > body->BidApplSeqNum &&
                body->ApplSeqNum > body->OfferApplSeqNum,
            "CombinedTick application sequence does not follow orders");
        return;
    }

    case sz::Snapshot300111_v2::MessageID:
        ValidateShenzhenSnapshot<sz::Snapshot300111_v2>(context);
        return;

    case sz::Snapshot300111_v3::MessageID:
        ValidateShenzhenSnapshot<sz::Snapshot300111_v3>(context);
        return;

    case sz::MarketData::MessageID: {
        typedef sz::MarketData T;
        typedef T::BidPriceLevelItem Bid;
        typedef Bid::OrdersItem BidOrder;
        typedef T::OfferPriceLevelItem Ask;
        typedef Ask::OrdersItem AskOrder;

        const T* const body = context->Body<T>();
        if (body == nullptr) {
            return;
        }
        context->CheckTime(body->DataTimeStamp, "DataTimeStamp");
        context->CheckString(
            body->SecurityID, "SecurityID", expected.security_id);
        context->CheckString(
            body->EndOfDayMaker, "EndOfDayMaker", "");
        context->Check(
            body->PreClosePx.m_Value == expected.reference_milli,
            "PreClosePx does not equal configured reference price");
        const std::int64_t open = context->PriceMilli(
            body->OpenPx, "OpenPx", expected.tick_milli);
        const std::int64_t high = context->PriceMilli(
            body->HighPx, "HighPx", expected.tick_milli);
        const std::int64_t low = context->PriceMilli(
            body->LowPx, "LowPx", expected.tick_milli);
        const std::int64_t last = context->PriceMilli(
            body->LastPx, "LastPx", expected.tick_milli);
        CheckOhlc(context, open, high, low, last);
        CheckCumulative(
            context,
            static_cast<std::int64_t>(body->TotalVolumeTrade),
            body->TotalValueTrade.m_Value,
            low,
            high);

        const Bid* const bids = context->List(
            body->BidPriceLevel, kBookDepth, "BidPriceLevel");
        const Ask* const asks = context->List(
            body->OfferPriceLevel, kBookDepth, "OfferPriceLevel");
        std::vector<std::int64_t> bid_prices;
        std::vector<std::int64_t> ask_prices;
        std::uint64_t total_bid = 0;
        std::uint64_t total_ask = 0;
        std::int64_t weighted_bid = 0;
        std::int64_t weighted_ask = 0;
        if (bids != nullptr) {
            for (std::uint32_t i = 0; i < kBookDepth; ++i) {
                const std::int64_t price = context->PriceMilli(
                    bids[i].BidPx,
                    "BidPriceLevel[].BidPx",
                    expected.tick_milli);
                bid_prices.push_back(price);
                context->Check(
                    bids[i].BidSize > 0,
                    "BidPriceLevel[].BidSize is not positive");
                context->Check(
                    bids[i].NumOrders == kOrdersPerLevel,
                    "BidPriceLevel[].NumOrders is wrong");
                const BidOrder* const orders = context->List(
                    bids[i].Orders,
                    kOrdersPerLevel,
                    "BidPriceLevel[].Orders");
                std::uint64_t queue = 0;
                if (orders != nullptr) {
                    for (std::uint32_t j = 0;
                         j < kOrdersPerLevel;
                         ++j) {
                        context->Check(
                            orders[j].OrderQty > 0,
                            "bid nested OrderQty is not positive");
                        queue += orders[j].OrderQty;
                    }
                }
                context->Check(
                    queue == bids[i].BidSize,
                    "bid nested queue does not sum to BidSize");
                total_bid += bids[i].BidSize;
                weighted_bid +=
                    price * static_cast<std::int64_t>(bids[i].BidSize);
            }
        }
        if (asks != nullptr) {
            for (std::uint32_t i = 0; i < kBookDepth; ++i) {
                const std::int64_t price = context->PriceMilli(
                    asks[i].OfferPx,
                    "OfferPriceLevel[].OfferPx",
                    expected.tick_milli);
                ask_prices.push_back(price);
                context->Check(
                    asks[i].OfferSize > 0,
                    "OfferPriceLevel[].OfferSize is not positive");
                context->Check(
                    asks[i].NumOrders == kOrdersPerLevel,
                    "OfferPriceLevel[].NumOrders is wrong");
                const AskOrder* const orders = context->List(
                    asks[i].Orders,
                    kOrdersPerLevel,
                    "OfferPriceLevel[].Orders");
                std::uint64_t queue = 0;
                if (orders != nullptr) {
                    for (std::uint32_t j = 0;
                         j < kOrdersPerLevel;
                         ++j) {
                        context->Check(
                            orders[j].OrderQty > 0,
                            "ask nested OrderQty is not positive");
                        queue += orders[j].OrderQty;
                    }
                }
                context->Check(
                    queue == asks[i].OfferSize,
                    "ask nested queue does not sum to OfferSize");
                total_ask += asks[i].OfferSize;
                weighted_ask +=
                    price * static_cast<std::int64_t>(asks[i].OfferSize);
            }
        }
        CheckBookOrder(
            context, bid_prices, ask_prices, expected.tick_milli);
        context->Check(
            body->TotalBidQty == total_bid,
            "TotalBidQty does not reconcile with bid levels");
        context->Check(
            body->TotalOfferQty == total_ask,
            "TotalOfferQty does not reconcile with ask levels");
        if (total_bid > 0) {
            context->Check(
                body->WeightedAvgBidPx.m_Value ==
                    weighted_bid /
                        static_cast<std::int64_t>(total_bid),
                "WeightedAvgBidPx does not reconcile");
        }
        if (total_ask > 0) {
            context->Check(
                body->WeightedAvgOfferPx.m_Value ==
                    weighted_ask /
                        static_cast<std::int64_t>(total_ask),
                "WeightedAvgOfferPx does not reconcile");
        }
        return;
    }
    default:
        context->Check(false, "unexpected Shenzhen MID");
        return;
    }
}

template <typename T>
void CollectingHandler::ValidateCffex(MessageContext* context) {
    typedef typename T::BidPriceLevelItem Bid;
    typedef typename T::AskPriceLevelItem Ask;
    const ExpectedInstrument expected = ExpectedFor(context->key());
    const T* const body = context->Body<T>();
    if (body == nullptr) {
        return;
    }
    context->CheckDates(body->ActionDay, body->TradDay);
    context->CheckTime(body->UpdateTime, "UpdateTime");
    context->CheckString(
        body->InstruID, "InstruID", expected.security_id);
    context->Check(
        body->PreCloPrice.m_Value == expected.reference_milli &&
            body->PreSetPrice.m_Value == expected.reference_milli,
        "PreCloPrice/PreSetPrice do not equal configured reference");
    const std::int64_t open = context->PriceMilli(
        body->OpenPrice, "OpenPrice", expected.tick_milli);
    const std::int64_t high = context->PriceMilli(
        body->HighPrice, "HighPrice", expected.tick_milli);
    const std::int64_t low = context->PriceMilli(
        body->LowPrice, "LowPrice", expected.tick_milli);
    const std::int64_t last = context->PriceMilli(
        body->LastPrice, "LastPrice", expected.tick_milli);
    CheckOhlc(context, open, high, low, last);
    context->Check(body->ClosePrice.IsNull(), "ClosePrice should be null");
    context->Check(body->SetPrice.IsNull(), "SetPrice should be null");
    context->Check(body->CurrDelta.IsNull(), "CurrDelta should be null");
    context->Check(body->PreDelta.IsNull(), "PreDelta should be null");
    const std::int64_t average = context->PriceMilli(
        body->AveragePrice, "AveragePrice", 0);
    CheckCumulative(
        context,
        body->Volume,
        body->Turnover.m_Value,
        low,
        high,
        average);

    const Bid* const bids = context->List(
        body->BidPriceLevel, kBookDepth, "BidPriceLevel");
    const Ask* const asks = context->List(
        body->AskPriceLevel, kBookDepth, "AskPriceLevel");
    std::vector<std::int64_t> bid_prices;
    std::vector<std::int64_t> ask_prices;
    if (bids != nullptr) {
        for (std::uint32_t i = 0; i < kBookDepth; ++i) {
            bid_prices.push_back(context->PriceMilli(
                bids[i].Price,
                "BidPriceLevel[].Price",
                expected.tick_milli));
            context->Check(
                bids[i].Volume > 0,
                "BidPriceLevel[].Volume is not positive");
        }
    }
    if (asks != nullptr) {
        for (std::uint32_t i = 0; i < kBookDepth; ++i) {
            ask_prices.push_back(context->PriceMilli(
                asks[i].Price,
                "AskPriceLevel[].Price",
                expected.tick_milli));
            context->Check(
                asks[i].Volume > 0,
                "AskPriceLevel[].Volume is not positive");
        }
    }
    CheckBookOrder(
        context, bid_prices, ask_prices, expected.tick_milli);
}

template <typename T>
void CollectingHandler::ValidateShfe(MessageContext* context) {
    typedef typename T::BidBookItem Bid;
    typedef typename T::AskBookItem Ask;
    const ExpectedInstrument expected = ExpectedFor(context->key());
    const T* const body = context->Body<T>();
    if (body == nullptr) {
        return;
    }
    context->CheckDates(body->ActionDay, body->TradDay);
    context->CheckTime(body->UpdateTime, "UpdateTime");
    context->CheckString(
        body->InstruID, "InstruID", expected.security_id);
    context->Check(
        body->PreCloPrice.m_Value == expected.reference_milli &&
            body->PreSetPrice.m_Value == expected.reference_milli,
        "PreCloPrice/PreSetPrice do not equal configured reference");
    const std::int64_t open = context->PriceMilli(
        body->OpenPrice, "OpenPrice", expected.tick_milli);
    const std::int64_t high = context->PriceMilli(
        body->HighPrice, "HighPrice", expected.tick_milli);
    const std::int64_t low = context->PriceMilli(
        body->LowPrice, "LowPrice", expected.tick_milli);
    const std::int64_t last = context->PriceMilli(
        body->LastPrice, "LastPrice", expected.tick_milli);
    CheckOhlc(context, open, high, low, last);
    context->Check(body->ClosePrice.IsNull(), "ClosePrice should be null");
    context->Check(body->SetPrice.IsNull(), "SetPrice should be null");
    context->Check(body->CurrDelta.IsNull(), "CurrDelta should be null");
    context->Check(body->PreDelta.IsNull(), "PreDelta should be null");
    const std::int64_t average = context->PriceMilli(
        body->AveragePrice, "AveragePrice", 0);
    CheckCumulative(
        context,
        body->Volume,
        body->Turnover.m_Value,
        low,
        high,
        average);

    const Bid* const bids =
        context->List(body->BidBook, kBookDepth, "BidBook");
    const Ask* const asks =
        context->List(body->AskBook, kBookDepth, "AskBook");
    std::vector<std::int64_t> bid_prices;
    std::vector<std::int64_t> ask_prices;
    if (bids != nullptr) {
        for (std::uint32_t i = 0; i < kBookDepth; ++i) {
            bid_prices.push_back(context->PriceMilli(
                bids[i].Price,
                "BidBook[].Price",
                expected.tick_milli));
            context->Check(
                bids[i].Volume > 0,
                "BidBook[].Volume is not positive");
        }
    }
    if (asks != nullptr) {
        for (std::uint32_t i = 0; i < kBookDepth; ++i) {
            ask_prices.push_back(context->PriceMilli(
                asks[i].Price,
                "AskBook[].Price",
                expected.tick_milli));
            context->Check(
                asks[i].Volume > 0,
                "AskBook[].Volume is not positive");
        }
    }
    CheckBookOrder(
        context, bid_prices, ask_prices, expected.tick_milli);
    context->List(body->ExtraFields, 0, "ExtraFields");
}

template <typename T>
void CollectingHandler::ValidateCzce(MessageContext* context) {
    typedef typename T::BidBookItem Bid;
    typedef typename T::AskBookItem Ask;
    const ExpectedInstrument expected = ExpectedFor(context->key());
    const T* const body = context->Body<T>();
    if (body == nullptr) {
        return;
    }
    context->CheckDates(body->ActionDay, body->TradDay);
    context->CheckTime(body->UpdateTime, "UpdateTime");
    context->CheckString(
        body->InstruID, "InstruID", expected.security_id);
    context->Check(
        body->PreCloPrice.m_Value == expected.reference_milli &&
            body->PreSetPrice.m_Value == expected.reference_milli,
        "PreCloPrice/PreSetPrice do not equal configured reference");
    const std::int64_t open = context->PriceMilli(
        body->OpenPrice, "OpenPrice", expected.tick_milli);
    const std::int64_t high = context->PriceMilli(
        body->HighPrice, "HighPrice", expected.tick_milli);
    const std::int64_t low = context->PriceMilli(
        body->LowPrice, "LowPrice", expected.tick_milli);
    const std::int64_t last = context->PriceMilli(
        body->LastPrice, "LastPrice", expected.tick_milli);
    CheckOhlc(context, open, high, low, last);
    context->Check(
        body->LifeHighPrice.m_Value == body->HighPrice.m_Value &&
            body->LifeLowPrice.m_Value == body->LowPrice.m_Value,
        "LifeHighPrice/LifeLowPrice do not match cumulative high/low");
    context->Check(body->ClosePrice.IsNull(), "ClosePrice should be null");
    context->Check(body->SetPrice.IsNull(), "SetPrice should be null");
    const std::int64_t average = context->PriceMilli(
        body->AveragePrice, "AveragePrice", 0);
    CheckCumulative(
        context,
        body->Volume,
        body->Turnover.m_Value,
        low,
        high,
        average);

    const Bid* const bids =
        context->List(body->BidBook, kBookDepth, "BidBook");
    const Ask* const asks =
        context->List(body->AskBook, kBookDepth, "AskBook");
    std::vector<std::int64_t> bid_prices;
    std::vector<std::int64_t> ask_prices;
    std::int64_t total_bid = 0;
    std::int64_t total_ask = 0;
    std::int64_t weighted_bid = 0;
    std::int64_t weighted_ask = 0;
    if (bids != nullptr) {
        for (std::uint32_t i = 0; i < kBookDepth; ++i) {
            const std::int64_t price = context->PriceMilli(
                bids[i].Price, "BidBook[].Price", expected.tick_milli);
            bid_prices.push_back(price);
            context->Check(
                bids[i].Volume > 0,
                "BidBook[].Volume is not positive");
            context->Check(
                bids[i].Num == static_cast<std::int32_t>(kOrdersPerLevel),
                "BidBook[].Num is wrong");
            total_bid += bids[i].Volume;
            weighted_bid += price * bids[i].Volume;
        }
    }
    if (asks != nullptr) {
        for (std::uint32_t i = 0; i < kBookDepth; ++i) {
            const std::int64_t price = context->PriceMilli(
                asks[i].Price, "AskBook[].Price", expected.tick_milli);
            ask_prices.push_back(price);
            context->Check(
                asks[i].Volume > 0,
                "AskBook[].Volume is not positive");
            context->Check(
                asks[i].Num == static_cast<std::int32_t>(kOrdersPerLevel),
                "AskBook[].Num is wrong");
            total_ask += asks[i].Volume;
            weighted_ask += price * asks[i].Volume;
        }
    }
    CheckBookOrder(
        context, bid_prices, ask_prices, expected.tick_milli);
    context->Check(
        body->BuyVolume == total_bid,
        "BuyVolume does not reconcile with BidBook");
    context->Check(
        body->SellVolume == total_ask,
        "SellVolume does not reconcile with AskBook");
    if (total_bid > 0) {
        context->Check(
            body->AvgBuyPrice.m_Value == weighted_bid / total_bid,
            "AvgBuyPrice does not reconcile with BidBook");
    }
    if (total_ask > 0) {
        context->Check(
            body->AvgSellPrice.m_Value == weighted_ask / total_ask,
            "AvgSellPrice does not reconcile with AskBook");
    }
}

template <typename T>
void CollectingHandler::ValidateDceLikeSnapshot(
    MessageContext* context) {
    typedef typename T::BidBookItem Bid;
    typedef typename T::AskBookItem Ask;
    const ExpectedInstrument expected = ExpectedFor(context->key());
    const T* const body = context->Body<T>();
    if (body == nullptr) {
        return;
    }
    context->CheckDates(body->ActionDay, body->TradDay);
    context->CheckTime(body->UpdateTime, "UpdateTime");
    context->CheckString(
        body->InstruID, "InstruID", expected.security_id);
    context->Check(
        body->PreCloPrice.m_Value == expected.reference_milli &&
            body->PreSetPrice.m_Value == expected.reference_milli,
        "PreCloPrice/PreSetPrice do not equal configured reference");
    const std::int64_t open = context->PriceMilli(
        body->OpenPrice, "OpenPrice", expected.tick_milli);
    const std::int64_t high = context->PriceMilli(
        body->HighPrice, "HighPrice", expected.tick_milli);
    const std::int64_t low = context->PriceMilli(
        body->LowPrice, "LowPrice", expected.tick_milli);
    const std::int64_t last = context->PriceMilli(
        body->LastPrice, "LastPrice", expected.tick_milli);
    CheckOhlc(context, open, high, low, last);
    context->Check(
        body->LifeHighPrice.m_Value == body->HighPrice.m_Value &&
            body->LifeLowPrice.m_Value == body->LowPrice.m_Value,
        "LifeHighPrice/LifeLowPrice do not match cumulative high/low");
    context->Check(body->ClosePrice.IsNull(), "ClosePrice should be null");
    context->Check(body->SetPrice.IsNull(), "SetPrice should be null");
    context->Check(body->LastVolume > 0, "LastVolume is not positive");
    context->Check(
        body->OpenIntChg == body->OpenInt - body->PreOpenInt,
        "OpenIntChg does not reconcile");
    const std::int64_t average = context->PriceMilli(
        body->AveragePrice, "AveragePrice", 0);
    CheckCumulative(
        context,
        body->Volume,
        body->Turnover.m_Value,
        low,
        high,
        average);

    const Bid* const bids =
        context->List(body->BidBook, kBookDepth, "BidBook");
    const Ask* const asks =
        context->List(body->AskBook, kBookDepth, "AskBook");
    std::vector<std::int64_t> bid_prices;
    std::vector<std::int64_t> ask_prices;
    std::int64_t total_bid = 0;
    std::int64_t total_ask = 0;
    std::int64_t weighted_bid = 0;
    std::int64_t weighted_ask = 0;
    if (bids != nullptr) {
        for (std::uint32_t i = 0; i < kBookDepth; ++i) {
            const std::int64_t price = context->PriceMilli(
                bids[i].Price, "BidBook[].Price", expected.tick_milli);
            bid_prices.push_back(price);
            context->Check(
                bids[i].Volume > 0,
                "BidBook[].Volume is not positive");
            context->Check(
                bids[i].DerVolume == 0,
                "BidBook[].DerVolume is not zero");
            total_bid += bids[i].Volume;
            weighted_bid += price * bids[i].Volume;
        }
    }
    if (asks != nullptr) {
        for (std::uint32_t i = 0; i < kBookDepth; ++i) {
            const std::int64_t price = context->PriceMilli(
                asks[i].Price, "AskBook[].Price", expected.tick_milli);
            ask_prices.push_back(price);
            context->Check(
                asks[i].Volume > 0,
                "AskBook[].Volume is not positive");
            context->Check(
                asks[i].DerVolume == 0,
                "AskBook[].DerVolume is not zero");
            total_ask += asks[i].Volume;
            weighted_ask += price * asks[i].Volume;
        }
    }
    CheckBookOrder(
        context, bid_prices, ask_prices, expected.tick_milli);
    context->Check(
        body->BuyVolume == total_bid,
        "BuyVolume does not reconcile with BidBook");
    context->Check(
        body->SellVolume == total_ask,
        "SellVolume does not reconcile with AskBook");
    if (total_bid > 0) {
        context->Check(
            body->AvgBuyPrice.m_Value == weighted_bid / total_bid,
            "AvgBuyPrice does not reconcile with BidBook");
    }
    if (total_ask > 0) {
        context->Check(
            body->AvgSellPrice.m_Value == weighted_ask / total_ask,
            "AvgSellPrice does not reconcile with AskBook");
    }
}

template <typename T>
void CollectingHandler::ValidateDceLikeOrder(
    MessageContext* context) {
    typedef typename T::BidOrdersItem Bid;
    typedef typename T::AskOrdersItem Ask;
    const ExpectedInstrument expected = ExpectedFor(context->key());
    const T* const body = context->Body<T>();
    if (body == nullptr) {
        return;
    }
    context->CheckDates(body->ActionDay, body->TradDay);
    context->CheckTime(body->UpdateTime, "UpdateTime");
    context->CheckString(
        body->InstruID, "InstruID", expected.security_id);
    const std::int64_t bid_price = context->PriceMilli(
        body->BidPrice, "BidPrice", expected.tick_milli);
    const std::int64_t ask_price = context->PriceMilli(
        body->AskPrice, "AskPrice", expected.tick_milli);
    context->Check(
        bid_price < ask_price, "BidPrice is not below AskPrice");
    context->Check(
        ask_price - bid_price == 2 * expected.tick_milli,
        "order queue spread is not two configured ticks");

    const Bid* const bids = context->List(
        body->BidOrders, kOrdersPerLevel, "BidOrders");
    const Ask* const asks = context->List(
        body->AskOrders, kOrdersPerLevel, "AskOrders");
    std::int64_t total_bid = 0;
    std::int64_t total_ask = 0;
    if (bids != nullptr) {
        for (std::uint32_t i = 0; i < kOrdersPerLevel; ++i) {
            context->Check(
                bids[i].OrderQty > 0,
                "BidOrders[].OrderQty is not positive");
            context->Check(
                bids[i].OrderQty % expected.lot_size == 0,
                "BidOrders[].OrderQty is not aligned to lot_size");
            total_bid += bids[i].OrderQty;
        }
    }
    if (asks != nullptr) {
        for (std::uint32_t i = 0; i < kOrdersPerLevel; ++i) {
            context->Check(
                asks[i].OrderQty > 0,
                "AskOrders[].OrderQty is not positive");
            context->Check(
                asks[i].OrderQty % expected.lot_size == 0,
                "AskOrders[].OrderQty is not aligned to lot_size");
            total_ask += asks[i].OrderQty;
        }
    }
    context->Check(
        total_bid > 0 && total_bid == total_ask,
        "bid/ask aggregate order queues do not reconcile");
}

void CollectingHandler::Validate(MessageContext* context) {
    switch (context->head()->ServiceID) {
    case mdl::MDLSID_MDL_SHL2:
        ValidateShanghai(context);
        return;

    case mdl::MDLSID_MDL_SZL2:
        ValidateShenzhen(context);
        return;

    case mdl::MDLSID_MDL_CFFEXL2:
        if (context->head()->MessageID == cf::Future::MessageID) {
            ValidateCffex<cf::Future>(context);
        } else if (context->head()->MessageID == cf::Option::MessageID) {
            ValidateCffex<cf::Option>(context);
        } else {
            context->Check(false, "unexpected CFFEX MID");
        }
        return;

    case mdl::MDLSID_MDL_SHFEL2:
        switch (context->head()->MessageID) {
        case sf::CTPFuture::MessageID:
            ValidateShfe<sf::CTPFuture>(context);
            return;
        case sf::CTPOption::MessageID:
            ValidateShfe<sf::CTPOption>(context);
            return;
        case sf::CrudeFuture::MessageID:
            ValidateShfe<sf::CrudeFuture>(context);
            return;
        case sf::CrudeOption::MessageID:
            ValidateShfe<sf::CrudeOption>(context);
            return;
        default:
            context->Check(false, "unexpected SHFE MID");
            return;
        }

    case mdl::MDLSID_MDL_CZCEL2:
        if (context->head()->MessageID == cz::CTPFuture::MessageID) {
            ValidateCzce<cz::CTPFuture>(context);
        } else if (context->head()->MessageID == cz::CTPOption::MessageID) {
            ValidateCzce<cz::CTPOption>(context);
        } else {
            context->Check(false, "unexpected CZCE MID");
        }
        return;

    case mdl::MDLSID_MDL_DCEL2:
        switch (context->head()->MessageID) {
        case dc::Future::MessageID:
            ValidateDceLikeSnapshot<dc::Future>(context);
            return;
        case dc::Option::MessageID:
            ValidateDceLikeSnapshot<dc::Option>(context);
            return;
        case dc::FutureOrder::MessageID:
            ValidateDceLikeOrder<dc::FutureOrder>(context);
            return;
        case dc::OptionOrder::MessageID:
            ValidateDceLikeOrder<dc::OptionOrder>(context);
            return;
        default:
            context->Check(false, "unexpected DCE MID");
            return;
        }

    case mdl::MDLSID_MDL_GFEXL2:
        switch (context->head()->MessageID) {
        case gf::Future::MessageID:
            ValidateDceLikeSnapshot<gf::Future>(context);
            return;
        case gf::Option::MessageID:
            ValidateDceLikeSnapshot<gf::Option>(context);
            return;
        case gf::FutureOrder::MessageID:
            ValidateDceLikeOrder<gf::FutureOrder>(context);
            return;
        case gf::OptionOrder::MessageID:
            ValidateDceLikeOrder<gf::OptionOrder>(context);
            return;
        default:
            context->Check(false, "unexpected GFEX MID");
            return;
        }

    default:
        context->Check(false, "unexpected service ID");
        return;
    }
}

mock::Instrument Instrument(
    std::uint8_t service_id,
    const char* security_id,
    std::int64_t reference_milli,
    std::int64_t tick_milli,
    std::uint32_t lot_size,
    bool option = false) {
    mock::Instrument instrument;
    instrument.service_id = service_id;
    instrument.security_id = security_id;
    instrument.reference_price_milli = reference_milli;
    instrument.tick_size_milli = tick_milli;
    instrument.lot_size = lot_size;
    instrument.option = option;
    return instrument;
}

mock::Config TestConfig() {
    mock::Config config;
    config.seed = 0x746573745f6d7367ULL;
    config.messages_per_second = 0;
    config.book_depth = kBookDepth;
    config.orders_per_level = kOrdersPerLevel;
    config.callback_threads = 1;
    config.callback_queue_capacity = 64;
    config.backpressure = mock::BackpressurePolicy::Block;
    config.clock_mode = mock::ClockMode::SimulatedTradingDay;
    config.simulated_start_date = kSimulatedDate;
    config.simulated_start_time = 93000000U;
    config.simulated_min_step_ms = 1;
    config.simulated_max_step_ms = 1;
    config.trading_phase_code = "T";

    config.instruments.push_back(Instrument(
        mdl::MDLSID_MDL_SHL2, "SH0001", 100000, 10, 100));
    config.instruments.push_back(Instrument(
        mdl::MDLSID_MDL_SZL2, "SZ0001", 120000, 10, 100));

    config.instruments.push_back(Instrument(
        mdl::MDLSID_MDL_CFFEXL2, "CFF_F", 500000, 100, 5));
    config.instruments.push_back(Instrument(
        mdl::MDLSID_MDL_CFFEXL2, "CFF_O", 600000, 100, 5, true));
    config.instruments.push_back(Instrument(
        mdl::MDLSID_MDL_SHFEL2, "SHF_F", 700000, 100, 5));
    config.instruments.push_back(Instrument(
        mdl::MDLSID_MDL_SHFEL2, "SHF_O", 800000, 100, 5, true));
    config.instruments.push_back(Instrument(
        mdl::MDLSID_MDL_CZCEL2, "CZE_F", 900000, 100, 5));
    config.instruments.push_back(Instrument(
        mdl::MDLSID_MDL_CZCEL2, "CZE_O", 1000000, 100, 5, true));
    config.instruments.push_back(Instrument(
        mdl::MDLSID_MDL_DCEL2, "DCE_F", 1100000, 100, 5));
    config.instruments.push_back(Instrument(
        mdl::MDLSID_MDL_DCEL2, "DCE_O", 1200000, 100, 5, true));
    config.instruments.push_back(Instrument(
        mdl::MDLSID_MDL_GFEXL2, "GFE_F", 1300000, 100, 5));
    config.instruments.push_back(Instrument(
        mdl::MDLSID_MDL_GFEXL2, "GFE_O", 1400000, 100, 5, true));
    return config;
}

} // namespace

int main() {
    std::vector<std::string> setup_errors;
    const mock::Config config = TestConfig();
    const std::string config_error = mock::ValidateConfig(config);
    if (!config_error.empty()) {
        std::cerr << "FAIL: test Config is invalid: " << config_error << '\n';
        return 1;
    }

    const std::vector<mock::SupportedMessage>& supported =
        mock::SupportedMessages();
    if (supported.size() != kExpectedKeyCount) {
        std::cerr << "FAIL: SupportedMessages() contains "
                  << supported.size() << " keys; expected "
                  << kExpectedKeyCount << '\n';
        return 1;
    }
    std::set<Key> expected_keys;
    for (const mock::SupportedMessage& descriptor : supported) {
        if (!expected_keys.insert(MakeKey(descriptor.key)).second) {
            setup_errors.push_back(
                "SupportedMessages() contains duplicate " +
                KeyText(MakeKey(descriptor.key)));
        }
    }

    CollectingHandler handler;
    mdl::IOManagerPtr manager;
    mdl::SubscriberPtr subscriber;
    try {
        manager = mock::CreateIOManager(config);
        if (manager.IsNull()) {
            std::cerr << "FAIL: CreateIOManager() returned null\n";
            return 1;
        }
        if (!mock::IsMockIOManager(manager.Get())) {
            std::cerr << "FAIL: factory did not return a mock IOManager\n";
            return 1;
        }

        // false is the direct-callback path; callback_threads=1 makes the
        // runtime configuration explicitly single-worker as well.
        subscriber = manager->CreateSubscriber(&handler, false);
        if (subscriber.IsNull()) {
            std::cerr << "FAIL: CreateSubscriber() returned null\n";
            manager->Shutdown();
            return 1;
        }
        mock::SubscribeAll(subscriber.Get());
        const char* const connect_error = subscriber->Connect();
        if (connect_error == nullptr || connect_error[0] != '\0') {
            std::cerr << "FAIL: Connect() returned: "
                      << (connect_error == nullptr ? "<null>" : connect_error)
                      << '\n';
            manager->Shutdown();
            return 1;
        }
    } catch (const std::exception& error) {
        std::cerr << "FAIL: setup threw std::exception: "
                  << error.what() << '\n';
        if (!manager.IsNull()) {
            manager->Shutdown();
        }
        return 1;
    }

    const std::chrono::milliseconds timeout(5000);
    const bool received_all = handler.WaitForAll(timeout);
    manager->Shutdown();
    const mock::Statistics statistics =
        mock::GetStatistics(manager.Get());

    const std::set<Key> seen = handler.Seen();
    std::vector<std::string> errors = handler.Errors();
    errors.insert(errors.end(), setup_errors.begin(), setup_errors.end());

    if (!received_all) {
        errors.push_back(
            "timeout after 5000 ms while waiting for all 28 message keys");
    }
    for (const Key& expected : expected_keys) {
        if (seen.count(expected) == 0) {
            errors.push_back("missing callback for " + KeyText(expected));
        }
    }
    for (const Key& actual : seen) {
        if (expected_keys.count(actual) == 0) {
            errors.push_back("unexpected callback for " + KeyText(actual));
        }
    }
    if (seen.size() != kExpectedKeyCount) {
        errors.push_back(
            "received " + std::to_string(seen.size()) +
            " unique keys instead of 28");
    }
    if (handler.CallbackThreadCount() != 1) {
        errors.push_back(
            "direct callbacks used " +
            std::to_string(handler.CallbackThreadCount()) +
            " threads instead of exactly one");
    }
    if (handler.CallbackCount() < kExpectedKeyCount) {
        errors.push_back(
            "only " + std::to_string(handler.CallbackCount()) +
            " callbacks were validated");
    }
    if (statistics.delivered < kExpectedKeyCount) {
        errors.push_back(
            "Statistics::delivered is below 28");
    }
    if (statistics.dropped != 0) {
        errors.push_back("direct callback test unexpectedly dropped messages");
    }
    if (statistics.callback_errors != 0) {
        errors.push_back("runtime reported callback_errors");
    }
    if (statistics.queue_high_watermark != 0) {
        errors.push_back(
            "direct callback path unexpectedly used the callback queue");
    }
    if (statistics.active_subscribers != 0) {
        errors.push_back(
            "Shutdown left active_subscribers nonzero");
    }

    subscriber.Reset();
    manager.Reset();

    if (!errors.empty()) {
        std::cerr << "message invariant test failed with "
                  << errors.size() << " error(s):\n";
        for (const std::string& error : errors) {
            std::cerr << "  - " << error << '\n';
        }
        std::cerr << "received " << seen.size()
                  << "/28 keys across " << handler.CallbackCount()
                  << " validated callback(s)\n";
        return 1;
    }

    std::cout << "message invariant checks passed: received all "
              << seen.size() << " keys across "
              << handler.CallbackCount()
              << " direct callback(s)\n";
    return 0;
}
