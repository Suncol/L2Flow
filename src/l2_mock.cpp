#include "l2mock/l2_mock.h"

#include "mdl_cffexl2_msg.h"
#include "mdl_czcel2_msg.h"
#include "mdl_dcel2_msg.h"
#include "mdl_gfexl2_msg.h"
#include "mdl_shfel2_msg.h"
#include "mdl_shl2_msg.h"
#include "mdl_szl2_msg.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <cstddef>
#include <ctime>
#include <cstring>
#include <deque>
#include <iomanip>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace datayes {
namespace mdl {
namespace mock {
namespace {

namespace sh = mdl_shl2_msg;
namespace sz = mdl_szl2_msg;
namespace cf = mdl_cffexl2_msg;
namespace sf = mdl_shfel2_msg;
namespace cz = mdl_czcel2_msg;
namespace dc = mdl_dcel2_msg;
namespace gf = mdl_gfexl2_msg;

struct KeyLess {
    bool operator()(const MessageKey& lhs, const MessageKey& rhs) const {
        return std::tie(lhs.service_id, lhs.service_version, lhs.message_id) <
               std::tie(rhs.service_id, rhs.service_version, rhs.message_id);
    }
};

bool SameKey(const MessageKey& lhs, const MessageKey& rhs) {
    return lhs.service_id == rhs.service_id &&
           lhs.service_version == rhs.service_version &&
           lhs.message_id == rhs.message_id;
}

template <typename T>
MessageKey KeyOf() {
    MessageKey key;
    key.service_id = static_cast<uint8_t>(T::ServiceID);
    key.service_version = static_cast<uint16_t>(T::ServiceVer);
    key.message_id = static_cast<uint16_t>(T::MessageID);
    return key;
}

template <typename T>
SupportedMessage Supported(MessageRole role, const char* name) {
    SupportedMessage value;
    value.key = KeyOf<T>();
    value.role = role;
    value.cpp_type = name;
    return value;
}

const std::vector<SupportedMessage>& SupportedMessageStorage() {
    static const std::vector<SupportedMessage> messages = {
        Supported<sh::SHL2Transaction>(MessageRole::Trade, "mdl_shl2_msg::SHL2Transaction"),
        Supported<sh::SHL2MarketData>(MessageRole::Snapshot, "mdl_shl2_msg::SHL2MarketData"),
        Supported<sh::SHL2Transaction2>(MessageRole::Trade, "mdl_shl2_msg::SHL2Transaction2"),
        Supported<sh::Order>(MessageRole::Order, "mdl_shl2_msg::Order"),

        Supported<sz::Trade>(MessageRole::Trade, "mdl_szl2_msg::Trade"),
        Supported<sz::Order>(MessageRole::Order, "mdl_szl2_msg::Order"),
        Supported<sz::MarketData>(MessageRole::Snapshot, "mdl_szl2_msg::MarketData"),
        Supported<sz::Snapshot300111_v2>(MessageRole::Snapshot, "mdl_szl2_msg::Snapshot300111_v2"),
        Supported<sz::Order300192_v2>(MessageRole::Order, "mdl_szl2_msg::Order300192_v2"),
        Supported<sz::Transaction300191_v2>(MessageRole::Trade, "mdl_szl2_msg::Transaction300191_v2"),
        Supported<sz::Snapshot300111_v3>(MessageRole::Snapshot, "mdl_szl2_msg::Snapshot300111_v3"),
        Supported<sz::CombinedTick>(MessageRole::Trade, "mdl_szl2_msg::CombinedTick"),

        Supported<cf::Future>(MessageRole::Snapshot, "mdl_cffexl2_msg::Future"),
        Supported<cf::Option>(MessageRole::Snapshot, "mdl_cffexl2_msg::Option"),

        Supported<sf::CTPFuture>(MessageRole::Snapshot, "mdl_shfel2_msg::CTPFuture"),
        Supported<sf::CTPOption>(MessageRole::Snapshot, "mdl_shfel2_msg::CTPOption"),
        Supported<sf::CrudeFuture>(MessageRole::Snapshot, "mdl_shfel2_msg::CrudeFuture"),
        Supported<sf::CrudeOption>(MessageRole::Snapshot, "mdl_shfel2_msg::CrudeOption"),

        Supported<cz::CTPFuture>(MessageRole::Snapshot, "mdl_czcel2_msg::CTPFuture"),
        Supported<cz::CTPOption>(MessageRole::Snapshot, "mdl_czcel2_msg::CTPOption"),

        Supported<dc::Future>(MessageRole::Snapshot, "mdl_dcel2_msg::Future"),
        Supported<dc::Option>(MessageRole::Snapshot, "mdl_dcel2_msg::Option"),
        Supported<dc::FutureOrder>(MessageRole::Order, "mdl_dcel2_msg::FutureOrder"),
        Supported<dc::OptionOrder>(MessageRole::Order, "mdl_dcel2_msg::OptionOrder"),

        Supported<gf::Future>(MessageRole::Snapshot, "mdl_gfexl2_msg::Future"),
        Supported<gf::Option>(MessageRole::Snapshot, "mdl_gfexl2_msg::Option"),
        Supported<gf::FutureOrder>(MessageRole::Order, "mdl_gfexl2_msg::FutureOrder"),
        Supported<gf::OptionOrder>(MessageRole::Order, "mdl_gfexl2_msg::OptionOrder")
    };
    return messages;
}

bool IsSupportedService(uint8_t service_id) {
    switch (service_id) {
    case MDLSID_MDL_SHL2:
    case MDLSID_MDL_SZL2:
    case MDLSID_MDL_CFFEXL2:
    case MDLSID_MDL_SHFEL2:
    case MDLSID_MDL_CZCEL2:
    case MDLSID_MDL_DCEL2:
    case MDLSID_MDL_GFEXL2:
        return true;
    default:
        return false;
    }
}

bool IsCashService(uint8_t service_id) {
    return service_id == MDLSID_MDL_SHL2 || service_id == MDLSID_MDL_SZL2;
}

uint32_t Pow10(uint32_t placement) {
    uint32_t result = 1;
    for (uint32_t i = 0; i < placement; ++i) {
        result *= 10;
    }
    return result;
}

uint32_t TimeRawToMillis(uint32_t raw) {
    const uint32_t hour = raw / 10000000U;
    const uint32_t minute = (raw / 100000U) % 100U;
    const uint32_t second = (raw / 1000U) % 100U;
    const uint32_t millis = raw % 1000U;
    return ((hour * 60U + minute) * 60U + second) * 1000U + millis;
}

uint32_t MillisToTimeRaw(uint32_t millis) {
    millis %= 24U * 60U * 60U * 1000U;
    const uint32_t hour = millis / 3600000U;
    millis %= 3600000U;
    const uint32_t minute = millis / 60000U;
    millis %= 60000U;
    const uint32_t second = millis / 1000U;
    return hour * 10000000U + minute * 100000U + second * 1000U + millis % 1000U;
}

bool IsValidTimeRaw(uint32_t raw) {
    MDLTime time;
    time.m_Value = raw;
    return time.IsValid();
}

bool IsSimulatedSessionTime(uint32_t raw) {
    if (!IsValidTimeRaw(raw)) {
        return false;
    }
    const uint32_t millis = TimeRawToMillis(raw);
    const uint32_t morning_open = TimeRawToMillis(93000000U);
    const uint32_t morning_close = TimeRawToMillis(113000000U);
    const uint32_t afternoon_open = TimeRawToMillis(130000000U);
    const uint32_t afternoon_close = TimeRawToMillis(150000000U);
    return (millis >= morning_open && millis <= morning_close) ||
           (millis >= afternoon_open && millis <= afternoon_close);
}

bool IsLeapYear(uint32_t year) {
    return year % 4U == 0U && (year % 100U != 0U || year % 400U == 0U);
}

uint32_t DaysInMonth(uint32_t year, uint32_t month) {
    static const uint32_t days[] = {
        31U, 28U, 31U, 30U, 31U, 30U,
        31U, 31U, 30U, 31U, 30U, 31U};
    if (month == 0U || month > 12U) {
        return 0U;
    }
    if (month == 2U && IsLeapYear(year)) {
        return 29U;
    }
    return days[month - 1U];
}

bool IsGregorianDate(uint32_t raw) {
    const uint32_t year = raw / 10000U;
    const uint32_t month = (raw / 100U) % 100U;
    const uint32_t day = raw % 100U;
    return year >= 1U && year <= 9999U &&
           day >= 1U && day <= DaysInMonth(year, month);
}

// 0 = Sunday, 1 = Monday, ..., 6 = Saturday.
uint32_t GregorianWeekday(uint32_t raw) {
    static const uint32_t month_offsets[] = {
        0U, 3U, 2U, 5U, 0U, 3U, 5U, 1U, 4U, 6U, 2U, 4U};
    int64_t year = static_cast<int64_t>(raw / 10000U);
    const uint32_t month = (raw / 100U) % 100U;
    const uint32_t day = raw % 100U;
    if (month < 3U) {
        --year;
    }
    const int64_t weekday =
        (year + year / 4U - year / 100U + year / 400U +
         static_cast<int64_t>(month_offsets[month - 1U]) + day) %
        7;
    return static_cast<uint32_t>(weekday);
}

uint32_t NextGregorianDate(uint32_t raw) {
    uint32_t year = raw / 10000U;
    uint32_t month = (raw / 100U) % 100U;
    uint32_t day = raw % 100U;
    if (day < DaysInMonth(year, month)) {
        ++day;
    } else {
        day = 1U;
        if (month < 12U) {
            ++month;
        } else {
            if (year == 9999U) {
                throw std::overflow_error(
                    "simulated date exceeds MDLDate year 9999");
            }
            month = 1U;
            ++year;
        }
    }
    return year * 10000U + month * 100U + day;
}

int64_t SaturatingMultiply(int64_t lhs, int64_t rhs, int64_t limit) {
    if (lhs <= 0 || rhs <= 0) {
        return 0;
    }
    if (lhs > limit / rhs) {
        return limit;
    }
    return lhs * rhs;
}

int64_t SaturatingAdd(int64_t lhs, int64_t rhs, int64_t limit) {
    if (rhs > 0 && lhs > limit - rhs) {
        return limit;
    }
    return lhs + rhs;
}

template <typename Derived, typename Interface>
class AtomicRefCounted : public Interface {
public:
    AtomicRefCounted() : ref_count_(1) {}

    void AddRef() override {
        ref_count_.fetch_add(1, std::memory_order_relaxed);
    }

    int ReleaseRef() override {
        const int value = ref_count_.fetch_sub(1, std::memory_order_acq_rel) - 1;
        if (value == 0) {
            delete static_cast<Derived*>(this);
        }
        return value;
    }

protected:
    ~AtomicRefCounted() = default;

private:
    std::atomic<int> ref_count_;
};

class WireBody {
public:
    explicit WireBody(size_t fixed_size) : bytes_(fixed_size, 0) {}

    size_t size() const {
        return bytes_.size();
    }

    const std::vector<char>& bytes() const {
        return bytes_;
    }

    std::vector<char> TakeBytes() {
        return std::move(bytes_);
    }

    template <typename T>
    void Put(size_t offset, const T& value) {
        if (offset > bytes_.size() || sizeof(T) > bytes_.size() - offset) {
            throw std::logic_error("wire body scalar write out of range");
        }
        std::memcpy(bytes_.data() + offset, &value, sizeof(T));
    }

    void PutDouble(size_t offset, int64_t raw) {
        Put<int64_t>(offset, raw);
    }

    void PutFloat(size_t offset, int32_t raw) {
        Put<int32_t>(offset, raw);
    }

    void SetString(size_t field_offset, const std::string& value) {
        if (value.empty()) {
            Put<uint16_t>(field_offset + offsetof(MDLString, Length), 0);
            Put<uint32_t>(field_offset + offsetof(MDLString, Offset), 0);
            return;
        }
        if (value.size() > std::numeric_limits<uint16_t>::max()) {
            throw std::length_error("MDL string exceeds uint16 length");
        }
        const size_t data_offset = bytes_.size();
        if (data_offset < field_offset ||
            data_offset - field_offset > std::numeric_limits<uint32_t>::max()) {
            throw std::length_error("MDL string relative offset exceeds uint32");
        }
        bytes_.insert(bytes_.end(), value.begin(), value.end());
        bytes_.push_back('\0');
        Put<uint16_t>(field_offset + offsetof(MDLString, Length),
                      static_cast<uint16_t>(value.size()));
        Put<uint32_t>(field_offset + offsetof(MDLString, Offset),
                      static_cast<uint32_t>(data_offset - field_offset));
    }

    template <typename Item>
    size_t AppendList(size_t field_offset, uint32_t count) {
        const size_t data_offset = bytes_.size();
        const size_t data_size = static_cast<size_t>(count) * sizeof(Item);
        if (count != 0 && data_size / sizeof(Item) != count) {
            throw std::length_error("MDL list size overflow");
        }
        if (data_offset < field_offset ||
            data_offset - field_offset > std::numeric_limits<uint32_t>::max()) {
            throw std::length_error("MDL list relative offset exceeds uint32");
        }
        bytes_.resize(data_offset + data_size, 0);
        Put<uint32_t>(field_offset + offsetof(MDLList, Length), count);
        Put<uint32_t>(field_offset + offsetof(MDLList, Offset),
                      count == 0 ? 0U : static_cast<uint32_t>(data_offset - field_offset));
        return data_offset;
    }

private:
    std::vector<char> bytes_;
};

class OwningMessage final : public AtomicRefCounted<OwningMessage, MDLMessage> {
public:
    OwningMessage(const MessageKey& key,
                  const MDLTime& local_time,
                  uint64_t sequence,
                  std::vector<char> body)
        : body_(std::move(body)) {
        std::memset(&head_, 0, sizeof(head_));
        if (body_.size() > std::numeric_limits<uint32_t>::max() - sizeof(MDLMessageHead)) {
            throw std::length_error("MDL message exceeds uint32 MessageSize");
        }
        head_.HeadSize = static_cast<uint8_t>(sizeof(MDLMessageHead));
        head_.MessageSize =
            static_cast<uint32_t>(sizeof(MDLMessageHead) + body_.size());
        head_.MessageEncoding = MDLEID_BINARY;
        head_.ServiceID = key.service_id;
        head_.ServiceVersion = key.service_version;
        head_.MessageID = key.message_id;
        head_.LocalTime = local_time;
        head_.SequenceID = sequence;
    }

    OwningMessage(const OwningMessage& other)
        : AtomicRefCounted<OwningMessage, MDLMessage>(),
          head_(other.head_),
          body_(other.body_) {}

    MDLMessageHead* GetHead() const override {
        return const_cast<MDLMessageHead*>(&head_);
    }

    char* GetBody() const override {
        return body_.empty() ? nullptr : const_cast<char*>(body_.data());
    }

    MDLMessage* _Copy() const override {
        return new OwningMessage(*this);
    }

private:
    MDLMessageHead head_;
    std::vector<char> body_;
};

MDLMessagePtr MakeMessage(const MessageKey& key,
                          const MDLTime& local_time,
                          uint64_t sequence,
                          WireBody body) {
    MDLMessage* raw =
        new OwningMessage(key, local_time, sequence, body.TakeBytes());
    return PtrFromReturn(raw);
}

struct Stamp {
    MDLDate date;
    MDLTime event_time;
    MDLTime local_time;
    uint32_t event_millis = 0;
};

void LocalCalendar(std::tm* result, std::chrono::system_clock::time_point now) {
    const std::time_t value = std::chrono::system_clock::to_time_t(now);
#ifdef _WIN32
    localtime_s(result, &value);
#else
    localtime_r(&value, result);
#endif
}

MDLDate DateFromTm(const std::tm& tm_value) {
    MDLDate date;
    date.m_Value = static_cast<uint32_t>((tm_value.tm_year + 1900) * 10000 +
                                         (tm_value.tm_mon + 1) * 100 +
                                         tm_value.tm_mday);
    return date;
}

uint32_t MillisFromTm(const std::tm& tm_value, uint32_t subsecond_ms) {
    return static_cast<uint32_t>(
        ((tm_value.tm_hour * 60 + tm_value.tm_min) * 60 + tm_value.tm_sec) * 1000 +
        subsecond_ms);
}

class EventClock {
public:
    EventClock(const Config& config, std::mt19937_64* random)
        : config_(config),
          random_(random),
          simulated_date_(config.simulated_start_date),
          simulated_millis_(TimeRawToMillis(config.simulated_start_time)) {
    }

    Stamp Next(uint32_t previous_date, uint32_t previous_millis) {
        if (config_.clock_mode == ClockMode::SimulatedTradingDay) {
            return NextSimulated();
        }
        return NextRealtime(previous_date, previous_millis);
    }

private:
    Stamp NextRealtime(uint32_t previous_date, uint32_t previous_millis) {
        const auto now = std::chrono::system_clock::now();
        const auto since_epoch = now.time_since_epoch();
        const uint32_t subsecond = static_cast<uint32_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(since_epoch).count() %
            1000);
        std::tm local;
        LocalCalendar(&local, now);
        const MDLDate date = DateFromTm(local);
        const uint32_t local_millis = MillisFromTm(local, subsecond);
        const uint32_t max_lag = std::min(config_.max_realtime_lag_ms, local_millis);
        std::uniform_int_distribution<uint32_t> lag_distribution(0, max_lag);
        uint32_t event_millis = local_millis - lag_distribution(*random_);
        if (date.m_Value == previous_date && event_millis < previous_millis) {
            event_millis = std::min(previous_millis, local_millis);
        }

        Stamp stamp;
        stamp.date = date;
        stamp.event_time.m_Value = MillisToTimeRaw(event_millis);
        stamp.local_time.m_Value = MillisToTimeRaw(local_millis);
        stamp.event_millis = event_millis;
        return stamp;
    }

    Stamp NextSimulated() {
        std::uniform_int_distribution<uint32_t> step_distribution(
            config_.simulated_min_step_ms, config_.simulated_max_step_ms);
        const uint32_t step = step_distribution(*random_);
        const uint32_t day_end = TimeRawToMillis(150000000U);
        if (step > day_end - std::min(simulated_millis_, day_end)) {
            AdvanceTradingDay();
        } else {
            simulated_millis_ += step;
        }

        const uint32_t morning_close = TimeRawToMillis(113000000U);
        const uint32_t afternoon_open = TimeRawToMillis(130000000U);
        if (simulated_millis_ > morning_close && simulated_millis_ < afternoon_open) {
            simulated_millis_ = afternoon_open;
        }
        if (simulated_millis_ > day_end) {
            AdvanceTradingDay();
        }

        Stamp stamp;
        stamp.date.m_Value = simulated_date_;
        stamp.event_time.m_Value = MillisToTimeRaw(simulated_millis_);
        stamp.local_time = stamp.event_time;
        stamp.event_millis = simulated_millis_;
        return stamp;
    }

    void AdvanceTradingDay() {
        do {
            simulated_date_ = NextGregorianDate(simulated_date_);
        } while (GregorianWeekday(simulated_date_) == 0U ||
                 GregorianWeekday(simulated_date_) == 6U);
        simulated_millis_ = TimeRawToMillis(93000000U);
    }

    const Config& config_;
    std::mt19937_64* random_;
    uint32_t simulated_date_;
    uint32_t simulated_millis_;
};

struct InstrumentState {
    Instrument spec;
    int64_t reference = 0;
    int64_t tick = 1;
    int64_t lower_limit = 0;
    int64_t upper_limit = 0;
    int64_t open = 0;
    int64_t high = 0;
    int64_t low = 0;
    int64_t last = 0;
    int64_t volume = 0;
    int64_t turnover_milli = 0;
    int64_t open_interest = 10000;
    uint64_t trades = 0;
    uint64_t order_counter = 0;
    uint64_t trade_counter = 0;
    uint64_t application_counter = 0;
    uint64_t sequence_base = 0;
    int64_t last_bid_order = 0;
    int64_t last_ask_order = 0;
    uint32_t channel = 1;
    uint32_t book_depth = 10;
    uint32_t last_event_date = 0;
    uint32_t last_event_millis = 0;
};

struct EventData {
    MessageKey key;
    MessageRole role = MessageRole::Snapshot;
    std::string security_id;
    Stamp stamp;
    bool emit = true;
    int side = SF_NA;
    int64_t price_milli = 0;
    int64_t quantity = 0;
    int64_t order_id = 0;
    int64_t application_sequence = 0;
    int64_t bid_order_id = 0;
    int64_t ask_order_id = 0;
};

struct Descriptor {
    size_t state_index = 0;
    MessageKey key;
    MessageRole role = MessageRole::Snapshot;
    int forced_side = SF_NA;
};

void AddDescriptor(std::vector<Descriptor>* descriptors,
                   size_t state_index,
                   const MessageKey& key,
                   MessageRole role,
                   int forced_side = SF_NA) {
    Descriptor descriptor;
    descriptor.state_index = state_index;
    descriptor.key = key;
    descriptor.role = role;
    descriptor.forced_side = forced_side;
    descriptors->push_back(descriptor);
}

void AddCashDescriptors(uint8_t service_id,
                        size_t state_index,
                        std::vector<Descriptor>* descriptors) {
    if (service_id == MDLSID_MDL_SHL2) {
        AddDescriptor(descriptors, state_index, KeyOf<sh::Order>(), MessageRole::Order, SF_BUY);
        AddDescriptor(descriptors, state_index, KeyOf<sh::Order>(), MessageRole::Order, SF_SELL);
        AddDescriptor(descriptors, state_index, KeyOf<sh::SHL2Transaction2>(), MessageRole::Trade);
        AddDescriptor(descriptors, state_index, KeyOf<sh::SHL2Transaction>(), MessageRole::Trade);
        AddDescriptor(descriptors, state_index, KeyOf<sh::SHL2MarketData>(), MessageRole::Snapshot);
        return;
    }

    AddDescriptor(descriptors, state_index, KeyOf<sz::Order300192_v2>(), MessageRole::Order, SF_BUY);
    AddDescriptor(descriptors, state_index, KeyOf<sz::Order300192_v2>(), MessageRole::Order, SF_SELL);
    AddDescriptor(descriptors, state_index, KeyOf<sz::Transaction300191_v2>(), MessageRole::Trade);
    AddDescriptor(descriptors, state_index, KeyOf<sz::CombinedTick>(), MessageRole::Trade);
    AddDescriptor(descriptors, state_index, KeyOf<sz::Snapshot300111_v2>(), MessageRole::Snapshot);
    AddDescriptor(descriptors, state_index, KeyOf<sz::Snapshot300111_v3>(), MessageRole::Snapshot);
    AddDescriptor(descriptors, state_index, KeyOf<sz::Order>(), MessageRole::Order, SF_BUY);
    AddDescriptor(descriptors, state_index, KeyOf<sz::Order>(), MessageRole::Order, SF_SELL);
    AddDescriptor(descriptors, state_index, KeyOf<sz::Trade>(), MessageRole::Trade);
    AddDescriptor(descriptors, state_index, KeyOf<sz::MarketData>(), MessageRole::Snapshot);
}

void AddDerivativeDescriptors(const Instrument& instrument,
                              size_t state_index,
                              std::vector<Descriptor>* descriptors) {
    const bool option = instrument.option;
    switch (instrument.service_id) {
    case MDLSID_MDL_CFFEXL2:
        AddDescriptor(descriptors, state_index,
                      option ? KeyOf<cf::Option>() : KeyOf<cf::Future>(),
                      MessageRole::Snapshot);
        break;
    case MDLSID_MDL_SHFEL2:
        AddDescriptor(descriptors, state_index,
                      option ? KeyOf<sf::CTPOption>() : KeyOf<sf::CTPFuture>(),
                      MessageRole::Snapshot);
        AddDescriptor(descriptors, state_index,
                      option ? KeyOf<sf::CrudeOption>() : KeyOf<sf::CrudeFuture>(),
                      MessageRole::Snapshot);
        break;
    case MDLSID_MDL_CZCEL2:
        AddDescriptor(descriptors, state_index,
                      option ? KeyOf<cz::CTPOption>() : KeyOf<cz::CTPFuture>(),
                      MessageRole::Snapshot);
        break;
    case MDLSID_MDL_DCEL2:
        AddDescriptor(descriptors, state_index,
                      option ? KeyOf<dc::OptionOrder>() : KeyOf<dc::FutureOrder>(),
                      MessageRole::Order);
        AddDescriptor(descriptors, state_index,
                      option ? KeyOf<dc::Option>() : KeyOf<dc::Future>(),
                      MessageRole::Snapshot);
        break;
    case MDLSID_MDL_GFEXL2:
        AddDescriptor(descriptors, state_index,
                      option ? KeyOf<gf::OptionOrder>() : KeyOf<gf::FutureOrder>(),
                      MessageRole::Order);
        AddDescriptor(descriptors, state_index,
                      option ? KeyOf<gf::Option>() : KeyOf<gf::Future>(),
                      MessageRole::Snapshot);
        break;
    default:
        throw std::logic_error("unsupported derivative service");
    }
}

int64_t RandomQuantity(const InstrumentState& state, std::mt19937_64* random) {
    std::uniform_int_distribution<int64_t> units(1, IsCashService(state.spec.service_id) ? 100 : 50);
    return static_cast<int64_t>(state.spec.lot_size) * units(*random);
}

int64_t CumulativeVolumeLimit(const InstrumentState& state) {
    int64_t limit =
        (state.spec.service_id == MDLSID_MDL_CFFEXL2 ||
         state.spec.service_id == MDLSID_MDL_SHFEL2)
            ? static_cast<int64_t>(INT_MAX)
            : 1000000000000LL;
    const int64_t lot = static_cast<int64_t>(state.spec.lot_size);
    return (limit / lot) * lot;
}

int64_t CumulativeTurnoverLimit(const InstrumentState& state) {
    // Internal turnover uses scale 3.  SH emits it at scale 5 (x100), while
    // SZ's tightest supported cumulative field uses scale 4 (x10).
    if (state.spec.service_id == MDLSID_MDL_SHL2) {
        return LLONG_MAX / 100;
    }
    if (state.spec.service_id == MDLSID_MDL_SZL2) {
        return LLONG_MAX / 10;
    }
    return LLONG_MAX / 2;
}

int64_t AccumulateTrade(InstrumentState* state,
                        int64_t price_milli,
                        int64_t requested_quantity) {
    const int64_t lot = static_cast<int64_t>(state->spec.lot_size);
    const int64_t volume_room =
        CumulativeVolumeLimit(*state) - state->volume;
    const int64_t turnover_room =
        CumulativeTurnoverLimit(*state) - state->turnover_milli;
    int64_t accepted = std::min(requested_quantity, volume_room);
    accepted = std::min(accepted, turnover_room / price_milli);
    accepted = (accepted / lot) * lot;
    if (accepted <= 0) {
        return 0;
    }
    state->volume += accepted;
    state->turnover_milli += price_milli * accepted;
    return accepted;
}

int64_t ClampPrice(const InstrumentState& state, int64_t price) {
    const int64_t margin =
        state.tick * static_cast<int64_t>(std::max<uint32_t>(2, state.book_depth + 2));
    const int64_t lower = std::min(state.upper_limit, state.lower_limit + margin);
    const int64_t upper = std::max(lower, state.upper_limit - margin);
    return std::max(lower, std::min(price, upper));
}

class Generator {
public:
    explicit Generator(const Config& config)
        : config_(config),
          random_(config.seed),
          clock_(config_, &random_) {
        const std::vector<Instrument> universe =
            config.instruments.empty()
                ? MakeSyntheticUniverse(config.shanghai_instruments,
                                        config.shenzhen_instruments,
                                        config.derivatives_per_market,
                                        config.include_derivatives)
                : config.instruments;
        states_.reserve(universe.size());
        for (size_t i = 0; i < universe.size(); ++i) {
            InstrumentState state;
            state.spec = universe[i];
            state.reference = universe[i].reference_price_milli;
            state.tick = universe[i].tick_size_milli;
            const int64_t limit_bps = IsCashService(universe[i].service_id) ? 1000 : 2000;
            const int64_t raw_lower =
                state.reference - state.reference * limit_bps / 10000;
            const int64_t raw_upper =
                state.reference + state.reference * limit_bps / 10000;
            state.lower_limit = std::max<int64_t>(
                state.tick,
                ((raw_lower + state.tick - 1) / state.tick) * state.tick);
            state.upper_limit = (raw_upper / state.tick) * state.tick;
            state.book_depth = config.book_depth;
            state.open = ClampPrice(state, state.reference);
            state.high = state.open;
            state.low = state.open;
            state.last = state.open;
            state.open_interest = 10000 + static_cast<int64_t>(i % 5000);
            state.channel = static_cast<uint32_t>(i % 32U + 1U);
            state.sequence_base = static_cast<uint64_t>(i + 1U) << 40U;
            states_.push_back(state);

            if (IsCashService(universe[i].service_id)) {
                AddCashDescriptors(universe[i].service_id, i, &descriptors_);
            } else {
                AddDerivativeDescriptors(universe[i], i, &descriptors_);
            }
        }
        if (descriptors_.empty()) {
            throw std::invalid_argument("mock universe produces no message streams");
        }
    }

    struct Result {
        MDLMessagePtr message;
        std::string security_id;
    };

    Result Next() {
        for (;;) {
            const Descriptor descriptor = descriptors_[descriptor_index_];
            descriptor_index_ =
                (descriptor_index_ + 1) % descriptors_.size();
            InstrumentState& state = states_[descriptor.state_index];
            EventData event = Mutate(state, descriptor);
            if (!event.emit) {
                continue;
            }
            WireBody body = BuildBody(state, event);
            ++sequence_;

            Result result;
            result.security_id = event.security_id;
            result.message = MakeMessage(
                descriptor.key,
                event.stamp.local_time,
                sequence_,
                std::move(body));
            return result;
        }
    }

private:
    EventData Mutate(InstrumentState& state, const Descriptor& descriptor) {
        EventData event;
        event.key = descriptor.key;
        event.role = descriptor.role;
        event.security_id = state.spec.security_id;
        const uint32_t previous_event_date = state.last_event_date;
        event.stamp =
            clock_.Next(state.last_event_date, state.last_event_millis);
        if (previous_event_date != 0U &&
            previous_event_date != event.stamp.date.m_Value) {
            // Daily market aggregates restart when the synthetic calendar
            // advances.  Stream/order identifiers remain monotonic.
            state.open = state.last;
            state.high = state.last;
            state.low = state.last;
            state.volume = 0;
            state.turnover_milli = 0;
            state.trades = 0;
        }
        state.last_event_date = event.stamp.date.m_Value;
        state.last_event_millis = event.stamp.event_millis;

        std::uniform_int_distribution<int> side_distribution(0, 1);
        event.side = descriptor.forced_side == SF_NA
                         ? (side_distribution(random_) == 0 ? SF_BUY : SF_SELL)
                         : descriptor.forced_side;
        event.quantity = RandomQuantity(state, &random_);

        if (descriptor.role == MessageRole::Order) {
            ++state.order_counter;
            ++state.application_counter;
            std::uniform_int_distribution<int64_t> level_distribution(1, config_.book_depth);
            const int64_t level = level_distribution(random_);
            event.price_milli = ClampPrice(
                state,
                state.last + (event.side == SF_BUY ? -level : level) * state.tick);
            const uint64_t raw_id =
                state.sequence_base + state.application_counter;
            event.order_id = static_cast<int64_t>(raw_id & 0x7fffffffffffffffULL);
            event.application_sequence = event.order_id;
            if (event.side == SF_BUY) {
                state.last_bid_order = event.order_id;
            } else {
                state.last_ask_order = event.order_id;
            }
        } else if (descriptor.role == MessageRole::Trade) {
            std::uniform_int_distribution<int64_t> movement(-2, 2);
            const int64_t trade_price = ClampPrice(
                state, state.last + movement(random_) * state.tick);
            const int64_t accepted =
                AccumulateTrade(&state, trade_price, event.quantity);
            if (accepted <= 0) {
                // A finite SDK cumulative field cannot represent another lot.
                // Suppress explicit trade payloads until the daily rollover
                // instead of publishing a trade omitted from the totals.
                event.emit = false;
                return event;
            }
            event.quantity = accepted;
            if (state.last_bid_order == 0) {
                ++state.order_counter;
                ++state.application_counter;
                state.last_bid_order = static_cast<int64_t>(
                    state.sequence_base + state.application_counter);
            }
            if (state.last_ask_order == 0) {
                ++state.order_counter;
                ++state.application_counter;
                state.last_ask_order = static_cast<int64_t>(
                    state.sequence_base + state.application_counter);
            }
            ++state.trade_counter;
            ++state.application_counter;
            event.application_sequence = static_cast<int64_t>(
                state.sequence_base + state.application_counter);
            ++state.trades;
            state.last = trade_price;
            state.high = std::max(state.high, state.last);
            state.low = std::min(state.low, state.last);
            event.price_milli = state.last;
            event.bid_order_id = state.last_bid_order;
            event.ask_order_id = state.last_ask_order;
        } else {
            if (!IsCashService(state.spec.service_id)) {
                std::uniform_int_distribution<int64_t> movement(-1, 1);
                const int64_t trade_price = ClampPrice(
                    state, state.last + movement(random_) * state.tick);
                const int64_t accepted =
                    AccumulateTrade(&state, trade_price, event.quantity);
                event.quantity = accepted;
                if (accepted > 0) {
                    ++state.trades;
                    state.last = trade_price;
                    state.high = std::max(state.high, state.last);
                    state.low = std::min(state.low, state.last);
                    std::uniform_int_distribution<int> open_interest_move(
                        -2, 2);
                    state.open_interest = std::max<int64_t>(
                        0,
                        state.open_interest +
                            open_interest_move(random_));
                }
            }
            event.price_milli = state.last;
            event.bid_order_id = state.last_bid_order;
            event.ask_order_id = state.last_ask_order;
        }
        return event;
    }

    WireBody BuildBody(InstrumentState& state, const EventData& event);

    const Config& config_;
    std::mt19937_64 random_;
    EventClock clock_;
    std::vector<InstrumentState> states_;
    std::vector<Descriptor> descriptors_;
    size_t descriptor_index_ = 0;
    uint64_t sequence_ = 0;
};

int64_t ScaleFromMilli(int64_t value_milli, uint32_t placement) {
    if (placement == 3) {
        return value_milli;
    }
    if (placement > 3) {
        const int64_t factor = Pow10(placement - 3);
        if (value_milli > 0 && value_milli > (LLONG_MAX / 2) / factor) {
            return LLONG_MAX / 2;
        }
        if (value_milli < 0 && value_milli < (LLONG_MIN / 2) / factor) {
            return LLONG_MIN / 2;
        }
        return value_milli * factor;
    }
    return value_milli / static_cast<int64_t>(Pow10(3 - placement));
}

int32_t FloatPrice(int64_t price_milli) {
    if (price_milli <= 0 || price_milli >= INT_MAX) {
        throw std::overflow_error("price cannot be represented by MDLFloatT<3>");
    }
    return static_cast<int32_t>(price_milli);
}

int64_t Volume3(int64_t quantity) {
    return SaturatingMultiply(quantity, 1000, LLONG_MAX / 2);
}

int64_t Turnover5(const InstrumentState& state) {
    return SaturatingMultiply(state.turnover_milli, 100, LLONG_MAX);
}

int64_t Turnover4(const InstrumentState& state) {
    return SaturatingMultiply(state.turnover_milli, 10, LLONG_MAX);
}

int64_t TradeMoney5(int64_t price_milli, int64_t quantity) {
    if (price_milli <= 0 || quantity <= 0 ||
        quantity > LLONG_MAX / 100 ||
        price_milli > LLONG_MAX / (quantity * 100)) {
        throw std::overflow_error(
            "trade money cannot be represented by MDLDoubleT<5>");
    }
    return price_milli * quantity * 100;
}

uint32_t NarrowU32(uint64_t value) {
    return static_cast<uint32_t>(
        std::min<uint64_t>(value, std::numeric_limits<uint32_t>::max()));
}

int32_t NarrowI32(uint64_t value) {
    return static_cast<int32_t>(
        std::min<uint64_t>(value, static_cast<uint64_t>(INT_MAX)));
}

int32_t ExactPositiveI32(int64_t value, const char* field_name) {
    if (value <= 0 || value > INT_MAX) {
        throw std::overflow_error(
            std::string(field_name) + " cannot be represented by int32");
    }
    return static_cast<int32_t>(value);
}

struct BookLevel {
    int64_t price_milli = 0;
    int64_t quantity = 0;
    uint32_t orders = 0;
};

std::vector<BookLevel> MakeBook(const InstrumentState& state,
                                bool bid,
                                const Config& config,
                                std::mt19937_64* random) {
    std::vector<BookLevel> result;
    result.reserve(config.book_depth);
    std::uniform_int_distribution<int64_t> units(1, 50);
    for (uint32_t i = 0; i < config.book_depth; ++i) {
        BookLevel level;
        const int64_t distance = static_cast<int64_t>(i + 1) * state.tick;
        level.price_milli = state.last + (bid ? -distance : distance);
        level.orders = config.orders_per_level;
        level.quantity =
            static_cast<int64_t>(state.spec.lot_size) *
            units(*random) * static_cast<int64_t>(level.orders);
        result.push_back(level);
    }
    return result;
}

int64_t TotalBookQuantity(const std::vector<BookLevel>& levels) {
    int64_t result = 0;
    for (const BookLevel& level : levels) {
        result = SaturatingAdd(result, level.quantity, LLONG_MAX / 1000);
    }
    return result;
}

int64_t WeightedBookPrice(const std::vector<BookLevel>& levels) {
    int64_t divisor = 0;
    for (const BookLevel& level : levels) {
        divisor = std::gcd(divisor, level.quantity);
    }
    if (divisor <= 0) {
        return 0;
    }

    int64_t reduced_quantity = 0;
    int64_t reduced_weighted = 0;
    bool exact = true;
    for (const BookLevel& level : levels) {
        const int64_t weight = level.quantity / divisor;
        if (weight <= 0 ||
            level.price_milli > LLONG_MAX / weight) {
            exact = false;
            break;
        }
        const int64_t product = level.price_milli * weight;
        if (reduced_weighted > LLONG_MAX - product ||
            reduced_quantity > LLONG_MAX - weight) {
            exact = false;
            break;
        }
        reduced_weighted += product;
        reduced_quantity += weight;
    }
    if (exact && reduced_quantity != 0) {
        return reduced_weighted / reduced_quantity;
    }

    long double weighted = 0.0L;
    long double quantity = 0.0L;
    for (const BookLevel& level : levels) {
        weighted += static_cast<long double>(level.price_milli) *
                    static_cast<long double>(level.quantity);
        quantity += static_cast<long double>(level.quantity);
    }
    if (quantity <= 0.0L) {
        return 0;
    }
    const long double average = weighted / quantity;
    if (average >= static_cast<long double>(LLONG_MAX)) {
        return LLONG_MAX;
    }
    return static_cast<int64_t>(average);
}

void PutCommonShanghaiTransaction(WireBody* body,
                                  const InstrumentState& state,
                                  const EventData& event,
                                  bool version_two) {
    if (version_two) {
        using T = sh::SHL2Transaction2;
        body->Put<int32_t>(offsetof(T, DataStatus), 0);
        body->Put<int32_t>(offsetof(T, TradeIndex), NarrowI32(state.trade_counter));
        body->Put<int32_t>(offsetof(T, TradeChan), static_cast<int32_t>(state.channel));
        body->Put<MDLTime>(offsetof(T, TradTime), event.stamp.event_time);
        body->PutFloat(offsetof(T, TradPrice), FloatPrice(event.price_milli));
        body->PutDouble(offsetof(T, TradVolume), Volume3(event.quantity));
        body->PutDouble(
            offsetof(T, TradeMoney), TradeMoney5(event.price_milli, event.quantity));
        body->Put<int64_t>(offsetof(T, TradeBuyNo), event.bid_order_id);
        body->Put<int64_t>(offsetof(T, TradeSellNo), event.ask_order_id);
        body->Put<int64_t>(offsetof(T, BizIndex),
                           static_cast<int64_t>(state.trade_counter));
        body->SetString(offsetof(T, SecurityID), event.security_id);
        body->SetString(
            offsetof(T, TradeBSFlag), event.side == SF_BUY ? "B" : "S");
        body->AppendList<T::ExtraFieldsItem>(offsetof(T, ExtraFields), 0);
        return;
    }

    using T = sh::SHL2Transaction;
    body->Put<int32_t>(offsetof(T, DataStatus), 0);
    body->Put<int32_t>(offsetof(T, TradeIndex), NarrowI32(state.trade_counter));
    body->Put<int32_t>(offsetof(T, TradeChan), static_cast<int32_t>(state.channel));
    body->Put<MDLTime>(offsetof(T, TradTime), event.stamp.event_time);
    body->PutFloat(offsetof(T, TradPrice), FloatPrice(event.price_milli));
    body->PutDouble(offsetof(T, TradVolume), Volume3(event.quantity));
    body->PutDouble(
        offsetof(T, TradeMoney), TradeMoney5(event.price_milli, event.quantity));
    body->Put<int64_t>(offsetof(T, TradeBuyNo), event.bid_order_id);
    body->Put<int64_t>(offsetof(T, TradeSellNo), event.ask_order_id);
    body->SetString(offsetof(T, SecurityID), event.security_id);
    body->SetString(
        offsetof(T, TradeBSFlag), event.side == SF_BUY ? "B" : "S");
}

WireBody BuildShanghaiOrder(const Config& config,
                             const InstrumentState& state,
                             const EventData& event) {
    using T = sh::Order;
    WireBody body(sizeof(T));
    body.Put<int32_t>(offsetof(T, DataStatus), 0);
    body.Put<int32_t>(offsetof(T, OrderIndex), NarrowI32(state.order_counter));
    body.Put<int32_t>(offsetof(T, OrderChannel), static_cast<int32_t>(state.channel));
    body.Put<MDLTime>(offsetof(T, OrderTime), event.stamp.event_time);
    body.Put<int64_t>(offsetof(T, OrderNO), event.order_id);
    body.PutFloat(offsetof(T, OrderPrice), FloatPrice(event.price_milli));
    body.PutDouble(offsetof(T, Balance), Volume3(event.quantity));
    body.Put<int64_t>(offsetof(T, BizIndex),
                      static_cast<int64_t>(state.order_counter));
    body.SetString(offsetof(T, SecurityID), event.security_id);
    body.SetString(offsetof(T, OrderType),
                   config.mock_order_type == 0
                       ? std::string()
                       : std::to_string(config.mock_order_type));
    body.SetString(
        offsetof(T, OrderBSFlag), event.side == SF_BUY ? "B" : "S");
    body.AppendList<T::ExtraFieldsItem>(offsetof(T, ExtraFields), 0);
    return body;
}

WireBody BuildShanghaiSnapshot(const Config& config,
                                const InstrumentState& state,
                                const EventData& event,
                                std::mt19937_64* random) {
    using T = sh::SHL2MarketData;
    using Bid = T::BidLevelsItem;
    using BidOrder = Bid::NOrdersItem;
    using Ask = T::SellLevelsItem;
    using AskOrder = Ask::NoOrdersItem;

    const std::vector<BookLevel> bids = MakeBook(state, true, config, random);
    const std::vector<BookLevel> asks = MakeBook(state, false, config, random);
    const int64_t total_bid = TotalBookQuantity(bids);
    const int64_t total_ask = TotalBookQuantity(asks);

    WireBody body(sizeof(T));
    body.Put<MDLTime>(offsetof(T, UpdateTime), event.stamp.event_time);
    body.Put<int32_t>(offsetof(T, ImageStatus), 0);
    body.PutFloat(offsetof(T, PreCloPrice), FloatPrice(state.reference));
    body.PutFloat(offsetof(T, OpenPrice), FloatPrice(state.open));
    body.PutFloat(offsetof(T, HighPrice), FloatPrice(state.high));
    body.PutFloat(offsetof(T, LowPrice), FloatPrice(state.low));
    body.PutFloat(offsetof(T, LastPrice), FloatPrice(state.last));
    body.PutFloat(offsetof(T, ClosePrice), MDLFloatT<3>::s_NullValue);
    body.Put<uint32_t>(offsetof(T, TradNumber), NarrowU32(state.trades));
    body.PutDouble(offsetof(T, TradVolume), Volume3(state.volume));
    body.PutDouble(offsetof(T, Turnover), Turnover5(state));
    body.PutDouble(offsetof(T, TotalBidVol), Volume3(total_bid));
    body.PutFloat(offsetof(T, WAvgBidPri), FloatPrice(WeightedBookPrice(bids)));
    body.PutFloat(offsetof(T, AltWAvgBidPri), FloatPrice(WeightedBookPrice(bids)));
    body.PutDouble(offsetof(T, TotalAskVol), Volume3(total_ask));
    body.PutFloat(offsetof(T, WAvgAskPri), FloatPrice(WeightedBookPrice(asks)));
    body.PutFloat(offsetof(T, AltWAvgAskPri), FloatPrice(WeightedBookPrice(asks)));
    body.Put<uint32_t>(offsetof(T, TotBidNum),
                       config.book_depth * config.orders_per_level);
    body.Put<uint32_t>(offsetof(T, TotSellNum),
                       config.book_depth * config.orders_per_level);
    body.Put<uint32_t>(offsetof(T, BidNum), config.book_depth);
    body.Put<uint32_t>(offsetof(T, SellNum), config.book_depth);
    body.PutFloat(offsetof(T, YieldToMatu), MDLFloatT<4>::s_NullValue);
    body.PutFloat(offsetof(T, IOPV), MDLFloatT<3>::s_NullValue);
    body.SetString(offsetof(T, SecurityID), event.security_id);
    body.SetString(offsetof(T, InstruStatus), config.trading_phase_code);

    const size_t bid_start =
        body.AppendList<Bid>(offsetof(T, BidLevels), config.book_depth);
    for (uint32_t i = 0; i < config.book_depth; ++i) {
        const size_t item = bid_start + static_cast<size_t>(i) * sizeof(Bid);
        body.Put<uint32_t>(item + offsetof(Bid, PriLevOpera), 0);
        body.PutFloat(item + offsetof(Bid, OrderPrice),
                      FloatPrice(bids[i].price_milli));
        body.PutDouble(item + offsetof(Bid, OrderVol), Volume3(bids[i].quantity));
        body.Put<uint32_t>(item + offsetof(Bid, OrderNum), bids[i].orders);
        const size_t orders = body.AppendList<BidOrder>(
            item + offsetof(Bid, NOrders), bids[i].orders);
        for (uint32_t j = 0; j < bids[i].orders; ++j) {
            const size_t order = orders + static_cast<size_t>(j) * sizeof(BidOrder);
            body.Put<uint32_t>(order + offsetof(BidOrder, OrderQueOper), 0);
            body.Put<uint32_t>(
                order + offsetof(BidOrder, OrderQueID), j + 1);
            body.PutDouble(order + offsetof(BidOrder, OrderQty),
                           Volume3(bids[i].quantity / bids[i].orders));
        }
    }

    const size_t ask_start =
        body.AppendList<Ask>(offsetof(T, SellLevels), config.book_depth);
    for (uint32_t i = 0; i < config.book_depth; ++i) {
        const size_t item = ask_start + static_cast<size_t>(i) * sizeof(Ask);
        body.Put<uint32_t>(item + offsetof(Ask, PriLevOpera), 0);
        body.PutFloat(item + offsetof(Ask, OrderPrice),
                      FloatPrice(asks[i].price_milli));
        body.PutDouble(item + offsetof(Ask, OrderVol), Volume3(asks[i].quantity));
        body.Put<uint32_t>(item + offsetof(Ask, OrderNum), asks[i].orders);
        const size_t orders = body.AppendList<AskOrder>(
            item + offsetof(Ask, NoOrders), asks[i].orders);
        for (uint32_t j = 0; j < asks[i].orders; ++j) {
            const size_t order = orders + static_cast<size_t>(j) * sizeof(AskOrder);
            body.Put<uint32_t>(order + offsetof(AskOrder, OrderQueOper), 0);
            body.Put<uint32_t>(
                order + offsetof(AskOrder, OrderQueID), j + 1);
            body.PutDouble(order + offsetof(AskOrder, OrderQty),
                           Volume3(asks[i].quantity / asks[i].orders));
        }
    }
    return body;
}

WireBody BuildShenzhenLegacyOrder(const InstrumentState& state,
                                   const EventData& event) {
    using T = sz::Order;
    WireBody body(sizeof(T));
    body.Put<uint32_t>(
        offsetof(T, RecNo),
        static_cast<uint32_t>(static_cast<uint64_t>(event.order_id)));
    body.Put<MDLTime>(offsetof(T, OrderEntryTime), event.stamp.event_time);
    body.PutDouble(offsetof(T, Price), event.price_milli);
    body.Put<uint32_t>(offsetof(T, OrderQty), NarrowU32(event.quantity));
    body.Put<uint32_t>(offsetof(T, SetNo), state.channel);
    body.SetString(offsetof(T, SecurityID), event.security_id);
    body.SetString(offsetof(T, OrderKind), std::string());
    body.SetString(
        offsetof(T, FunctionCode), event.side == SF_BUY ? "B" : "S");
    return body;
}

WireBody BuildShenzhenLegacyTrade(const InstrumentState& state,
                                   const EventData& event) {
    using T = sz::Trade;
    WireBody body(sizeof(T));
    body.Put<uint32_t>(offsetof(T, RecNo), NarrowU32(state.trade_counter));
    body.Put<uint32_t>(offsetof(T, BuyOrderRecNo),
                       static_cast<uint32_t>(
                           static_cast<uint64_t>(event.bid_order_id)));
    body.Put<uint32_t>(offsetof(T, SellOrderRecNo),
                       static_cast<uint32_t>(
                           static_cast<uint64_t>(event.ask_order_id)));
    body.Put<MDLTime>(offsetof(T, TradTime), event.stamp.event_time);
    body.PutDouble(offsetof(T, Price), event.price_milli);
    body.Put<uint32_t>(offsetof(T, TradeQty), NarrowU32(event.quantity));
    body.Put<uint32_t>(offsetof(T, SetNo), state.channel);
    body.SetString(offsetof(T, SecurityID), event.security_id);
    body.SetString(offsetof(T, OrderKind), std::string());
    body.SetString(
        offsetof(T, FunctionCode), event.side == SF_BUY ? "B" : "S");
    return body;
}

WireBody BuildShenzhenLegacySnapshot(const Config& config,
                                      const InstrumentState& state,
                                      const EventData& event,
                                      std::mt19937_64* random) {
    using T = sz::MarketData;
    using Bid = T::BidPriceLevelItem;
    using BidOrder = Bid::OrdersItem;
    using Ask = T::OfferPriceLevelItem;
    using AskOrder = Ask::OrdersItem;

    const std::vector<BookLevel> bids = MakeBook(state, true, config, random);
    const std::vector<BookLevel> asks = MakeBook(state, false, config, random);
    WireBody body(sizeof(T));
    body.Put<MDLTime>(offsetof(T, DataTimeStamp), event.stamp.event_time);
    body.PutDouble(offsetof(T, PreClosePx), state.reference);
    body.PutDouble(offsetof(T, OpenPx), state.open);
    body.PutDouble(offsetof(T, HighPx), state.high);
    body.PutDouble(offsetof(T, LowPx), state.low);
    body.PutDouble(offsetof(T, LastPx), state.last);
    body.Put<uint64_t>(offsetof(T, NumTrades), state.trades);
    body.Put<uint64_t>(offsetof(T, TotalVolumeTrade),
                       static_cast<uint64_t>(state.volume));
    body.PutDouble(offsetof(T, TotalValueTrade), state.turnover_milli);
    body.Put<uint64_t>(offsetof(T, TotalOfferQty),
                       static_cast<uint64_t>(TotalBookQuantity(asks)));
    body.PutDouble(offsetof(T, WeightedAvgOfferPx), WeightedBookPrice(asks));
    body.Put<uint64_t>(offsetof(T, TotalBidQty),
                       static_cast<uint64_t>(TotalBookQuantity(bids)));
    body.PutDouble(offsetof(T, WeightedAvgBidPx), WeightedBookPrice(bids));
    body.SetString(offsetof(T, SecurityID), event.security_id);
    body.SetString(offsetof(T, EndOfDayMaker), std::string());

    const size_t ask_start = body.AppendList<Ask>(
        offsetof(T, OfferPriceLevel), config.book_depth);
    for (uint32_t i = 0; i < config.book_depth; ++i) {
        const size_t item = ask_start + static_cast<size_t>(i) * sizeof(Ask);
        body.PutDouble(item + offsetof(Ask, OfferPx), asks[i].price_milli);
        body.Put<uint64_t>(item + offsetof(Ask, OfferSize),
                           static_cast<uint64_t>(asks[i].quantity));
        body.Put<uint32_t>(item + offsetof(Ask, NumOrders), asks[i].orders);
        const size_t orders = body.AppendList<AskOrder>(
            item + offsetof(Ask, Orders), asks[i].orders);
        for (uint32_t j = 0; j < asks[i].orders; ++j) {
            const size_t order = orders + static_cast<size_t>(j) * sizeof(AskOrder);
            body.Put<uint32_t>(order + offsetof(AskOrder, OrderQty),
                               NarrowU32(asks[i].quantity / asks[i].orders));
        }
    }

    const size_t bid_start = body.AppendList<Bid>(
        offsetof(T, BidPriceLevel), config.book_depth);
    for (uint32_t i = 0; i < config.book_depth; ++i) {
        const size_t item = bid_start + static_cast<size_t>(i) * sizeof(Bid);
        body.PutDouble(item + offsetof(Bid, BidPx), bids[i].price_milli);
        body.Put<uint64_t>(item + offsetof(Bid, BidSize),
                           static_cast<uint64_t>(bids[i].quantity));
        body.Put<uint32_t>(item + offsetof(Bid, NumOrders), bids[i].orders);
        const size_t orders = body.AppendList<BidOrder>(
            item + offsetof(Bid, Orders), bids[i].orders);
        for (uint32_t j = 0; j < bids[i].orders; ++j) {
            const size_t order = orders + static_cast<size_t>(j) * sizeof(BidOrder);
            body.Put<uint32_t>(order + offsetof(BidOrder, OrderQty),
                               NarrowU32(bids[i].quantity / bids[i].orders));
        }
    }
    return body;
}

WireBody BuildShenzhenV2Order(const Config& config,
                               const InstrumentState& state,
                               const EventData& event) {
    using T = sz::Order300192_v2;
    WireBody body(sizeof(T));
    body.Put<uint32_t>(offsetof(T, ChannelNo), state.channel);
    body.Put<int64_t>(offsetof(T, ApplSeqNum), event.application_sequence);
    body.PutDouble(offsetof(T, Price), ScaleFromMilli(event.price_milli, 4));
    body.Put<int64_t>(offsetof(T, OrderQty), event.quantity);
    body.Put<int32_t>(offsetof(T, Side), event.side);
    body.Put<MDLTime>(offsetof(T, TransactTime), event.stamp.event_time);
    body.Put<int32_t>(offsetof(T, OrdType), config.mock_order_type);
    body.SetString(offsetof(T, MDStreamID), std::string());
    body.SetString(offsetof(T, SecurityID), event.security_id);
    body.SetString(offsetof(T, SecurityIDSource), std::string());
    return body;
}

WireBody BuildShenzhenV2Transaction(const Config& config,
                                     const InstrumentState& state,
                                     const EventData& event) {
    using T = sz::Transaction300191_v2;
    WireBody body(sizeof(T));
    body.Put<uint32_t>(offsetof(T, ChannelNo), state.channel);
    body.Put<int64_t>(offsetof(T, ApplSeqNum), event.application_sequence);
    body.Put<int64_t>(offsetof(T, BidApplSeqNum), event.bid_order_id);
    body.Put<int64_t>(offsetof(T, OfferApplSeqNum), event.ask_order_id);
    body.PutDouble(offsetof(T, LastPx), ScaleFromMilli(event.price_milli, 4));
    body.Put<int64_t>(offsetof(T, LastQty), event.quantity);
    body.Put<int32_t>(offsetof(T, ExecType), config.mock_exec_type);
    body.Put<MDLTime>(offsetof(T, TransactTime), event.stamp.event_time);
    body.SetString(offsetof(T, MDStreamID), std::string());
    body.SetString(offsetof(T, SecurityID), event.security_id);
    body.SetString(offsetof(T, SecurityIDSource), std::string());
    return body;
}

WireBody BuildShenzhenCombinedTick(const Config& config,
                                    const InstrumentState& state,
                                    const EventData& event) {
    using T = sz::CombinedTick;
    WireBody body(sizeof(T));
    body.Put<uint32_t>(offsetof(T, ChannelNo), state.channel);
    body.Put<int64_t>(offsetof(T, ApplSeqNum), event.application_sequence);
    body.Put<MDLTime>(offsetof(T, TransactTime), event.stamp.event_time);
    body.Put<int32_t>(offsetof(T, Type), config.mock_exec_type);
    body.Put<int64_t>(offsetof(T, BidApplSeqNum), event.bid_order_id);
    body.Put<int64_t>(offsetof(T, OfferApplSeqNum), event.ask_order_id);
    body.PutDouble(offsetof(T, Price), ScaleFromMilli(event.price_milli, 4));
    body.Put<int64_t>(offsetof(T, Qty), event.quantity);
    body.SetString(offsetof(T, MDStreamID), std::string());
    body.SetString(offsetof(T, SecurityID), event.security_id);
    body.SetString(offsetof(T, SecurityIDSource), std::string());
    return body;
}

template <typename T>
WireBody BuildShenzhenSnapshot(const Config& config,
                                const InstrumentState& state,
                                const EventData& event,
                                std::mt19937_64* random) {
    using Bid = typename T::BidPriceLevelItem;
    using BidOrder = typename Bid::OrdersItem;
    using Ask = typename T::AskPriceLevelItem;
    using AskOrder = typename Ask::OrdersItem;

    const std::vector<BookLevel> bids = MakeBook(state, true, config, random);
    const std::vector<BookLevel> asks = MakeBook(state, false, config, random);
    const int64_t total_bid = TotalBookQuantity(bids);
    const int64_t total_ask = TotalBookQuantity(asks);
    const int64_t weighted_bid = WeightedBookPrice(bids);
    const int64_t weighted_ask = WeightedBookPrice(asks);

    WireBody body(sizeof(T));
    body.Put<MDLTime>(offsetof(T, UpdateTime), event.stamp.event_time);
    body.Put<uint32_t>(offsetof(T, ChannelNo), state.channel);
    body.PutDouble(offsetof(T, PreCloPrice), ScaleFromMilli(state.reference, 4));
    body.Put<int64_t>(offsetof(T, TurnNum), static_cast<int64_t>(state.trades));
    body.Put<int64_t>(offsetof(T, Volume), state.volume);
    body.PutDouble(offsetof(T, Turnover), Turnover4(state));
    body.PutDouble(offsetof(T, LastPrice), ScaleFromMilli(state.last, 6));
    body.PutDouble(offsetof(T, OpenPrice), ScaleFromMilli(state.open, 6));
    body.PutDouble(offsetof(T, HighPrice), ScaleFromMilli(state.high, 6));
    body.PutDouble(offsetof(T, LowPrice), ScaleFromMilli(state.low, 6));
    body.PutDouble(offsetof(T, DifPrice1),
                   ScaleFromMilli(state.last - state.reference, 6));
    body.PutDouble(offsetof(T, DifPrice2),
                   ScaleFromMilli(state.last - state.open, 6));
    body.PutDouble(offsetof(T, PE1), MDLDoubleT<6>::s_NullValue);
    body.PutDouble(offsetof(T, PE2), MDLDoubleT<6>::s_NullValue);
    body.PutDouble(offsetof(T, PreCloseIOPV), MDLDoubleT<6>::s_NullValue);
    body.PutDouble(offsetof(T, IOPV), MDLDoubleT<6>::s_NullValue);
    body.Put<int64_t>(offsetof(T, TotalOfferQty), total_ask);
    body.PutDouble(offsetof(T, WeightedAvgOfferPx),
                   ScaleFromMilli(weighted_ask, 6));
    body.Put<int64_t>(offsetof(T, TotalBidQty), total_bid);
    body.PutDouble(offsetof(T, WeightedAvgBidPx),
                   ScaleFromMilli(weighted_bid, 6));
    body.PutDouble(offsetof(T, HighLimitPrice),
                   ScaleFromMilli(state.upper_limit, 6));
    body.PutDouble(offsetof(T, LowLimitPrice),
                   ScaleFromMilli(state.lower_limit, 6));
    body.Put<int64_t>(offsetof(T, OpenInt), 0);
    body.PutDouble(offsetof(T, OptPremiumRatio), MDLDoubleT<6>::s_NullValue);
    body.SetString(offsetof(T, MDStreamID), std::string());
    body.SetString(offsetof(T, SecurityID), event.security_id);
    body.SetString(offsetof(T, SecurityIDSource), std::string());
    body.SetString(offsetof(T, TradingPhaseCode), config.trading_phase_code);

    if constexpr (std::is_same<T, sz::Snapshot300111_v3>::value) {
        const int64_t weighted =
            state.volume == 0 ? state.last : state.turnover_milli / state.volume;
        body.PutDouble(offsetof(T, WeightedAvgPx), ScaleFromMilli(weighted, 6));
        body.PutDouble(offsetof(T, WeightedAvgPxPreClose),
                       ScaleFromMilli(state.reference, 6));
        // The supplied SDK does not define the calculation semantics of this
        // basis-point field.  Null is safer than a contradictory numeric zero.
        body.PutDouble(
            offsetof(T, WeightedAvgPxChangeBP),
            MDLDoubleT<6>::s_NullValue);
    }

    const size_t bid_start = body.AppendList<Bid>(
        offsetof(T, BidPriceLevel), config.book_depth);
    for (uint32_t i = 0; i < config.book_depth; ++i) {
        const size_t item = bid_start + static_cast<size_t>(i) * sizeof(Bid);
        body.Put<int64_t>(item + offsetof(Bid, Volume), bids[i].quantity);
        body.PutDouble(item + offsetof(Bid, Price),
                       ScaleFromMilli(bids[i].price_milli, 6));
        body.Put<uint32_t>(item + offsetof(Bid, NumOrders), bids[i].orders);
        const size_t orders = body.AppendList<BidOrder>(
            item + offsetof(Bid, Orders), bids[i].orders);
        for (uint32_t j = 0; j < bids[i].orders; ++j) {
            const size_t order = orders + static_cast<size_t>(j) * sizeof(BidOrder);
            body.Put<int64_t>(order + offsetof(BidOrder, OrderQty),
                              bids[i].quantity / bids[i].orders);
        }
    }

    const size_t ask_start = body.AppendList<Ask>(
        offsetof(T, AskPriceLevel), config.book_depth);
    for (uint32_t i = 0; i < config.book_depth; ++i) {
        const size_t item = ask_start + static_cast<size_t>(i) * sizeof(Ask);
        body.Put<int64_t>(item + offsetof(Ask, Volume), asks[i].quantity);
        body.PutDouble(item + offsetof(Ask, Price),
                       ScaleFromMilli(asks[i].price_milli, 6));
        body.Put<uint32_t>(item + offsetof(Ask, NumOrders), asks[i].orders);
        const size_t orders = body.AppendList<AskOrder>(
            item + offsetof(Ask, Orders), asks[i].orders);
        for (uint32_t j = 0; j < asks[i].orders; ++j) {
            const size_t order = orders + static_cast<size_t>(j) * sizeof(AskOrder);
            body.Put<int64_t>(order + offsetof(AskOrder, OrderQty),
                              asks[i].quantity / asks[i].orders);
        }
    }

    if constexpr (std::is_same<T, sz::Snapshot300111_v3>::value) {
        body.template AppendList<typename T::ExtendFieldsItem>(
            offsetof(T, ExtendFields), 0);
    }
    return body;
}

int64_t AveragePriceMilli(const InstrumentState& state) {
    return state.volume == 0 ? state.last : state.turnover_milli / state.volume;
}

template <typename T>
WireBody BuildCffexSnapshot(const Config& config,
                             const InstrumentState& state,
                             const EventData& event,
                             std::mt19937_64* random) {
    using Bid = typename T::BidPriceLevelItem;
    using Ask = typename T::AskPriceLevelItem;
    const std::vector<BookLevel> bids = MakeBook(state, true, config, random);
    const std::vector<BookLevel> asks = MakeBook(state, false, config, random);

    WireBody body(sizeof(T));
    body.Put<MDLDate>(offsetof(T, ActionDay), event.stamp.date);
    body.Put<MDLDate>(offsetof(T, TradDay), event.stamp.date);
    body.Put<MDLTime>(offsetof(T, UpdateTime), event.stamp.event_time);
    body.PutDouble(offsetof(T, LastPrice), state.last);
    body.PutDouble(offsetof(T, HighPrice), state.high);
    body.PutDouble(offsetof(T, LowPrice), state.low);
    body.PutDouble(offsetof(T, OpenPrice), state.open);
    body.Put<int32_t>(offsetof(T, Volume),
                      static_cast<int32_t>(state.volume));
    body.PutDouble(offsetof(T, Turnover), state.turnover_milli);
    body.PutDouble(offsetof(T, OpenInt), Volume3(state.open_interest));
    body.PutDouble(offsetof(T, PreOpenInt), Volume3(10000));
    body.PutDouble(offsetof(T, AveragePrice), AveragePriceMilli(state));
    body.PutDouble(offsetof(T, ClosePrice), MDLDoubleT<3>::s_NullValue);
    body.PutDouble(offsetof(T, SetPrice), MDLDoubleT<3>::s_NullValue);
    body.PutDouble(offsetof(T, PreCloPrice), state.reference);
    body.PutDouble(offsetof(T, PreSetPrice), state.reference);
    body.PutDouble(offsetof(T, CurrDelta), MDLDoubleT<3>::s_NullValue);
    body.PutDouble(offsetof(T, PreDelta), MDLDoubleT<3>::s_NullValue);
    body.PutDouble(offsetof(T, ULimitPrice), state.upper_limit);
    body.PutDouble(offsetof(T, LLimitPrice), state.lower_limit);
    body.SetString(offsetof(T, InstruID), event.security_id);

    const size_t bid_start =
        body.template AppendList<Bid>(offsetof(T, BidPriceLevel), config.book_depth);
    for (uint32_t i = 0; i < config.book_depth; ++i) {
        const size_t item = bid_start + static_cast<size_t>(i) * sizeof(Bid);
        body.Put<int32_t>(item + offsetof(Bid, Volume),
                          ExactPositiveI32(
                              bids[i].quantity, "CFFEX bid volume"));
        body.PutDouble(item + offsetof(Bid, Price), bids[i].price_milli);
    }
    const size_t ask_start =
        body.template AppendList<Ask>(offsetof(T, AskPriceLevel), config.book_depth);
    for (uint32_t i = 0; i < config.book_depth; ++i) {
        const size_t item = ask_start + static_cast<size_t>(i) * sizeof(Ask);
        body.Put<int32_t>(item + offsetof(Ask, Volume),
                          ExactPositiveI32(
                              asks[i].quantity, "CFFEX ask volume"));
        body.PutDouble(item + offsetof(Ask, Price), asks[i].price_milli);
    }
    return body;
}

template <typename T>
WireBody BuildShfeSnapshot(const Config& config,
                            const InstrumentState& state,
                            const EventData& event,
                            std::mt19937_64* random) {
    using Bid = typename T::BidBookItem;
    using Ask = typename T::AskBookItem;
    const std::vector<BookLevel> bids = MakeBook(state, true, config, random);
    const std::vector<BookLevel> asks = MakeBook(state, false, config, random);

    WireBody body(sizeof(T));
    body.PutDouble(offsetof(T, LastPrice), state.last);
    body.PutDouble(offsetof(T, PreSetPrice), state.reference);
    body.PutDouble(offsetof(T, OpenPrice), state.open);
    body.PutDouble(offsetof(T, HighPrice), state.high);
    body.PutDouble(offsetof(T, LowPrice), state.low);
    body.PutDouble(offsetof(T, Turnover), state.turnover_milli);
    body.PutDouble(offsetof(T, OpenInt), Volume3(state.open_interest));
    body.PutDouble(offsetof(T, SetPrice), MDLDoubleT<3>::s_NullValue);
    body.PutDouble(offsetof(T, ULimitPrice), state.upper_limit);
    body.PutDouble(offsetof(T, LLimitPrice), state.lower_limit);
    body.Put<MDLDate>(offsetof(T, TradDay), event.stamp.date);
    body.PutDouble(offsetof(T, PreCloPrice), state.reference);
    body.Put<int32_t>(offsetof(T, Volume),
                      static_cast<int32_t>(state.volume));
    body.PutDouble(offsetof(T, ClosePrice), MDLDoubleT<3>::s_NullValue);
    body.PutDouble(offsetof(T, PreDelta), MDLDoubleT<3>::s_NullValue);
    body.PutDouble(offsetof(T, CurrDelta), MDLDoubleT<3>::s_NullValue);
    body.Put<MDLTime>(offsetof(T, UpdateTime), event.stamp.event_time);
    body.PutDouble(offsetof(T, PreOpenInt), Volume3(10000));
    body.PutDouble(offsetof(T, AveragePrice), AveragePriceMilli(state));
    body.Put<MDLDate>(offsetof(T, ActionDay), event.stamp.date);
    body.SetString(offsetof(T, InstruID), event.security_id);

    const size_t bid_start =
        body.template AppendList<Bid>(offsetof(T, BidBook), config.book_depth);
    for (uint32_t i = 0; i < config.book_depth; ++i) {
        const size_t item = bid_start + static_cast<size_t>(i) * sizeof(Bid);
        body.Put<int32_t>(item + offsetof(Bid, Volume),
                          ExactPositiveI32(
                              bids[i].quantity, "SHFE bid volume"));
        body.PutDouble(item + offsetof(Bid, Price), bids[i].price_milli);
    }
    const size_t ask_start =
        body.template AppendList<Ask>(offsetof(T, AskBook), config.book_depth);
    for (uint32_t i = 0; i < config.book_depth; ++i) {
        const size_t item = ask_start + static_cast<size_t>(i) * sizeof(Ask);
        body.Put<int32_t>(item + offsetof(Ask, Volume),
                          ExactPositiveI32(
                              asks[i].quantity, "SHFE ask volume"));
        body.PutDouble(item + offsetof(Ask, Price), asks[i].price_milli);
    }
    body.template AppendList<typename T::ExtraFieldsItem>(
        offsetof(T, ExtraFields), 0);
    return body;
}

template <typename T>
WireBody BuildCzceSnapshot(const Config& config,
                            const InstrumentState& state,
                            const EventData& event,
                            std::mt19937_64* random) {
    using Bid = typename T::BidBookItem;
    using Ask = typename T::AskBookItem;
    const std::vector<BookLevel> bids = MakeBook(state, true, config, random);
    const std::vector<BookLevel> asks = MakeBook(state, false, config, random);

    WireBody body(sizeof(T));
    body.Put<MDLDate>(offsetof(T, ActionDay), event.stamp.date);
    body.Put<MDLDate>(offsetof(T, TradDay), event.stamp.date);
    body.Put<MDLTime>(offsetof(T, UpdateTime), event.stamp.event_time);
    body.PutDouble(offsetof(T, LastPrice), state.last);
    body.PutDouble(offsetof(T, HighPrice), state.high);
    body.PutDouble(offsetof(T, LowPrice), state.low);
    body.PutDouble(offsetof(T, OpenPrice), state.open);
    body.Put<int64_t>(offsetof(T, Volume), state.volume);
    body.PutDouble(offsetof(T, Turnover), state.turnover_milli);
    body.Put<int64_t>(offsetof(T, OpenInt), state.open_interest);
    body.PutDouble(offsetof(T, AveragePrice), AveragePriceMilli(state));
    body.PutDouble(offsetof(T, ClosePrice), MDLDoubleT<3>::s_NullValue);
    body.PutDouble(offsetof(T, SetPrice), MDLDoubleT<3>::s_NullValue);
    body.Put<int64_t>(offsetof(T, PreOpenInt), 10000);
    body.PutDouble(offsetof(T, PreCloPrice), state.reference);
    body.PutDouble(offsetof(T, PreSetPrice), state.reference);
    body.Put<int64_t>(offsetof(T, BuyVolume), TotalBookQuantity(bids));
    body.Put<int64_t>(offsetof(T, SellVolume), TotalBookQuantity(asks));
    body.PutDouble(offsetof(T, AvgBuyPrice), WeightedBookPrice(bids));
    body.PutDouble(offsetof(T, AvgSellPrice), WeightedBookPrice(asks));
    body.Put<int64_t>(offsetof(T, DerBuyVolume), 0);
    body.Put<int64_t>(offsetof(T, DerSellVolume), 0);
    body.PutDouble(offsetof(T, ULimitPrice), state.upper_limit);
    body.PutDouble(offsetof(T, LLimitPrice), state.lower_limit);
    body.PutDouble(offsetof(T, LifeHighPrice), state.high);
    body.PutDouble(offsetof(T, LifeLowPrice), state.low);
    body.SetString(offsetof(T, InstruID), event.security_id);

    const size_t bid_start =
        body.template AppendList<Bid>(offsetof(T, BidBook), config.book_depth);
    for (uint32_t i = 0; i < config.book_depth; ++i) {
        const size_t item = bid_start + static_cast<size_t>(i) * sizeof(Bid);
        body.Put<int64_t>(item + offsetof(Bid, Volume), bids[i].quantity);
        body.PutDouble(item + offsetof(Bid, Price), bids[i].price_milli);
        body.Put<int32_t>(item + offsetof(Bid, Num),
                          static_cast<int32_t>(bids[i].orders));
    }
    const size_t ask_start =
        body.template AppendList<Ask>(offsetof(T, AskBook), config.book_depth);
    for (uint32_t i = 0; i < config.book_depth; ++i) {
        const size_t item = ask_start + static_cast<size_t>(i) * sizeof(Ask);
        body.Put<int64_t>(item + offsetof(Ask, Volume), asks[i].quantity);
        body.PutDouble(item + offsetof(Ask, Price), asks[i].price_milli);
        body.Put<int32_t>(item + offsetof(Ask, Num),
                          static_cast<int32_t>(asks[i].orders));
    }
    return body;
}

template <typename T>
WireBody BuildDceLikeSnapshot(const Config& config,
                               const InstrumentState& state,
                               const EventData& event,
                               std::mt19937_64* random) {
    using Bid = typename T::BidBookItem;
    using Ask = typename T::AskBookItem;
    const std::vector<BookLevel> bids = MakeBook(state, true, config, random);
    const std::vector<BookLevel> asks = MakeBook(state, false, config, random);

    WireBody body(sizeof(T));
    body.Put<MDLDate>(offsetof(T, ActionDay), event.stamp.date);
    body.Put<MDLDate>(offsetof(T, TradDay), event.stamp.date);
    body.Put<MDLTime>(offsetof(T, UpdateTime), event.stamp.event_time);
    body.PutDouble(offsetof(T, LastPrice), state.last);
    body.PutDouble(offsetof(T, HighPrice), state.high);
    body.PutDouble(offsetof(T, LowPrice), state.low);
    body.PutDouble(offsetof(T, OpenPrice), state.open);
    body.Put<int64_t>(offsetof(T, LastVolume), event.quantity);
    body.Put<int64_t>(offsetof(T, Volume), state.volume);
    body.PutDouble(offsetof(T, Turnover), state.turnover_milli);
    body.Put<int64_t>(offsetof(T, OpenInt), state.open_interest);
    body.Put<int64_t>(offsetof(T, PreOpenInt), 10000);
    body.Put<int64_t>(offsetof(T, OpenIntChg), state.open_interest - 10000);
    body.PutDouble(offsetof(T, AveragePrice), AveragePriceMilli(state));
    body.PutDouble(offsetof(T, ClosePrice), MDLDoubleT<3>::s_NullValue);
    body.PutDouble(offsetof(T, SetPrice), MDLDoubleT<3>::s_NullValue);
    body.PutDouble(offsetof(T, PreSetPrice), state.reference);
    body.PutDouble(offsetof(T, PreCloPrice), state.reference);
    body.Put<int64_t>(offsetof(T, BuyVolume), TotalBookQuantity(bids));
    body.Put<int64_t>(offsetof(T, SellVolume), TotalBookQuantity(asks));
    body.PutDouble(offsetof(T, AvgBuyPrice), WeightedBookPrice(bids));
    body.PutDouble(offsetof(T, AvgSellPrice), WeightedBookPrice(asks));
    body.PutDouble(offsetof(T, ULimitPrice), state.upper_limit);
    body.PutDouble(offsetof(T, LLimitPrice), state.lower_limit);
    body.PutDouble(offsetof(T, LifeHighPrice), state.high);
    body.PutDouble(offsetof(T, LifeLowPrice), state.low);
    body.SetString(offsetof(T, InstruID), event.security_id);

    const size_t bid_start =
        body.template AppendList<Bid>(offsetof(T, BidBook), config.book_depth);
    for (uint32_t i = 0; i < config.book_depth; ++i) {
        const size_t item = bid_start + static_cast<size_t>(i) * sizeof(Bid);
        body.PutDouble(item + offsetof(Bid, Price), bids[i].price_milli);
        body.Put<int64_t>(item + offsetof(Bid, Volume), bids[i].quantity);
        body.Put<int32_t>(item + offsetof(Bid, DerVolume), 0);
    }
    const size_t ask_start =
        body.template AppendList<Ask>(offsetof(T, AskBook), config.book_depth);
    for (uint32_t i = 0; i < config.book_depth; ++i) {
        const size_t item = ask_start + static_cast<size_t>(i) * sizeof(Ask);
        body.PutDouble(item + offsetof(Ask, Price), asks[i].price_milli);
        body.Put<int64_t>(item + offsetof(Ask, Volume), asks[i].quantity);
        body.Put<int32_t>(item + offsetof(Ask, DerVolume), 0);
    }
    return body;
}

template <typename T>
WireBody BuildDceLikeOrder(const Config& config,
                            const InstrumentState& state,
                            const EventData& event) {
    using Bid = typename T::BidOrdersItem;
    using Ask = typename T::AskOrdersItem;
    const int64_t bid_price = state.last - state.tick;
    const int64_t ask_price = state.last + state.tick;
    WireBody body(sizeof(T));
    body.Put<MDLDate>(offsetof(T, ActionDay), event.stamp.date);
    body.Put<MDLDate>(offsetof(T, TradDay), event.stamp.date);
    body.Put<MDLTime>(offsetof(T, UpdateTime), event.stamp.event_time);
    body.PutDouble(offsetof(T, BidPrice), bid_price);
    body.PutDouble(offsetof(T, AskPrice), ask_price);
    body.SetString(offsetof(T, InstruID), event.security_id);

    const size_t bid_start = body.template AppendList<Bid>(
        offsetof(T, BidOrders), config.orders_per_level);
    const size_t ask_start = body.template AppendList<Ask>(
        offsetof(T, AskOrders), config.orders_per_level);
    const int64_t each =
        static_cast<int64_t>(state.spec.lot_size) *
        std::max<int64_t>(
            1,
            event.quantity /
                static_cast<int64_t>(state.spec.lot_size) /
                static_cast<int64_t>(config.orders_per_level));
    for (uint32_t i = 0; i < config.orders_per_level; ++i) {
        body.Put<int64_t>(
            bid_start + static_cast<size_t>(i) * sizeof(Bid) +
                offsetof(Bid, OrderQty),
            each);
        body.Put<int64_t>(
            ask_start + static_cast<size_t>(i) * sizeof(Ask) +
                offsetof(Ask, OrderQty),
            each);
    }
    return body;
}

WireBody Generator::BuildBody(InstrumentState& state, const EventData& event) {
    switch (event.key.service_id) {
    case MDLSID_MDL_SHL2:
        switch (event.key.message_id) {
        case sh::SHL2Transaction::MessageID: {
            WireBody body(sizeof(sh::SHL2Transaction));
            PutCommonShanghaiTransaction(&body, state, event, false);
            return body;
        }
        case sh::SHL2MarketData::MessageID:
            return BuildShanghaiSnapshot(config_, state, event, &random_);
        case sh::SHL2Transaction2::MessageID: {
            WireBody body(sizeof(sh::SHL2Transaction2));
            PutCommonShanghaiTransaction(&body, state, event, true);
            return body;
        }
        case sh::Order::MessageID:
            return BuildShanghaiOrder(config_, state, event);
        default:
            break;
        }
        break;

    case MDLSID_MDL_SZL2:
        switch (event.key.message_id) {
        case sz::Trade::MessageID:
            return BuildShenzhenLegacyTrade(state, event);
        case sz::Order::MessageID:
            return BuildShenzhenLegacyOrder(state, event);
        case sz::MarketData::MessageID:
            return BuildShenzhenLegacySnapshot(config_, state, event, &random_);
        case sz::Snapshot300111_v2::MessageID:
            return BuildShenzhenSnapshot<sz::Snapshot300111_v2>(
                config_, state, event, &random_);
        case sz::Order300192_v2::MessageID:
            return BuildShenzhenV2Order(config_, state, event);
        case sz::Transaction300191_v2::MessageID:
            return BuildShenzhenV2Transaction(config_, state, event);
        case sz::Snapshot300111_v3::MessageID:
            return BuildShenzhenSnapshot<sz::Snapshot300111_v3>(
                config_, state, event, &random_);
        case sz::CombinedTick::MessageID:
            return BuildShenzhenCombinedTick(config_, state, event);
        default:
            break;
        }
        break;

    case MDLSID_MDL_CFFEXL2:
        if (event.key.message_id == cf::Future::MessageID) {
            return BuildCffexSnapshot<cf::Future>(
                config_, state, event, &random_);
        }
        if (event.key.message_id == cf::Option::MessageID) {
            return BuildCffexSnapshot<cf::Option>(
                config_, state, event, &random_);
        }
        break;

    case MDLSID_MDL_SHFEL2:
        switch (event.key.message_id) {
        case sf::CTPFuture::MessageID:
            return BuildShfeSnapshot<sf::CTPFuture>(
                config_, state, event, &random_);
        case sf::CTPOption::MessageID:
            return BuildShfeSnapshot<sf::CTPOption>(
                config_, state, event, &random_);
        case sf::CrudeFuture::MessageID:
            return BuildShfeSnapshot<sf::CrudeFuture>(
                config_, state, event, &random_);
        case sf::CrudeOption::MessageID:
            return BuildShfeSnapshot<sf::CrudeOption>(
                config_, state, event, &random_);
        default:
            break;
        }
        break;

    case MDLSID_MDL_CZCEL2:
        if (event.key.message_id == cz::CTPFuture::MessageID) {
            return BuildCzceSnapshot<cz::CTPFuture>(
                config_, state, event, &random_);
        }
        if (event.key.message_id == cz::CTPOption::MessageID) {
            return BuildCzceSnapshot<cz::CTPOption>(
                config_, state, event, &random_);
        }
        break;

    case MDLSID_MDL_DCEL2:
        switch (event.key.message_id) {
        case dc::Future::MessageID:
            return BuildDceLikeSnapshot<dc::Future>(
                config_, state, event, &random_);
        case dc::Option::MessageID:
            return BuildDceLikeSnapshot<dc::Option>(
                config_, state, event, &random_);
        case dc::FutureOrder::MessageID:
            return BuildDceLikeOrder<dc::FutureOrder>(config_, state, event);
        case dc::OptionOrder::MessageID:
            return BuildDceLikeOrder<dc::OptionOrder>(config_, state, event);
        default:
            break;
        }
        break;

    case MDLSID_MDL_GFEXL2:
        switch (event.key.message_id) {
        case gf::Future::MessageID:
            return BuildDceLikeSnapshot<gf::Future>(
                config_, state, event, &random_);
        case gf::Option::MessageID:
            return BuildDceLikeSnapshot<gf::Option>(
                config_, state, event, &random_);
        case gf::FutureOrder::MessageID:
            return BuildDceLikeOrder<gf::FutureOrder>(config_, state, event);
        case gf::OptionOrder::MessageID:
            return BuildDceLikeOrder<gf::OptionOrder>(config_, state, event);
        default:
            break;
        }
        break;
    default:
        break;
    }
    throw std::logic_error("generator descriptor has no payload builder");
}

class Runtime;
class MockSubscriber;

struct ActiveCallbackFrame {
    MockSubscriber* subscriber;
    ActiveCallbackFrame* previous;
};

struct ActiveRuntimeFrame {
    Runtime* runtime;
    ActiveRuntimeFrame* previous;
};

thread_local ActiveRuntimeFrame* active_runtime_frame = nullptr;
thread_local ActiveCallbackFrame* active_callback_frame = nullptr;

class RuntimeThreadScope {
public:
    explicit RuntimeThreadScope(Runtime* runtime)
        : frame_{runtime, active_runtime_frame} {
        active_runtime_frame = &frame_;
    }

    ~RuntimeThreadScope() {
        active_runtime_frame = frame_.previous;
    }

private:
    ActiveRuntimeFrame frame_;
};

bool IsActiveRuntime(const Runtime* runtime) {
    for (ActiveRuntimeFrame* frame = active_runtime_frame;
         frame != nullptr;
         frame = frame->previous) {
        if (frame->runtime == runtime) {
            return true;
        }
    }
    return false;
}

bool GlobMatch(const std::string& pattern, const std::string& value) {
    size_t pattern_index = 0;
    size_t value_index = 0;
    size_t star = std::string::npos;
    size_t star_value = 0;
    while (value_index < value.size()) {
        if (pattern_index < pattern.size() &&
            (pattern[pattern_index] == '?' ||
             pattern[pattern_index] == value[value_index])) {
            ++pattern_index;
            ++value_index;
        } else if (pattern_index < pattern.size() &&
                   pattern[pattern_index] == '*') {
            star = pattern_index++;
            star_value = value_index;
        } else if (star != std::string::npos) {
            pattern_index = star + 1;
            value_index = ++star_value;
        } else {
            return false;
        }
    }
    while (pattern_index < pattern.size() && pattern[pattern_index] == '*') {
        ++pattern_index;
    }
    return pattern_index == pattern.size();
}

void AppendJsonString(
    std::ostringstream* output, const std::string& value) {
    static const char hex[] = "0123456789abcdef";
    *output << '"';
    for (unsigned char character : value) {
        switch (character) {
        case '"':
            *output << "\\\"";
            break;
        case '\\':
            *output << "\\\\";
            break;
        case '\b':
            *output << "\\b";
            break;
        case '\f':
            *output << "\\f";
            break;
        case '\n':
            *output << "\\n";
            break;
        case '\r':
            *output << "\\r";
            break;
        case '\t':
            *output << "\\t";
            break;
        default:
            if (character < 0x20U) {
                *output << "\\u00"
                        << hex[(character >> 4U) & 0x0fU]
                        << hex[character & 0x0fU];
            } else {
                *output << static_cast<char>(character);
            }
            break;
        }
    }
    *output << '"';
}

struct SubscriptionRule {
    bool all_values = false;
    std::map<std::string, std::vector<std::string> > field_patterns;
};

class MockSubscriber final
    : public AtomicRefCounted<MockSubscriber, Subscriber> {
public:
    MockSubscriber(std::shared_ptr<Runtime> runtime,
                   MessageHandlerBase* handler,
                   bool multithread_callback)
        : runtime_(std::move(runtime)),
          handler_(handler),
          multithread_callback_(multithread_callback) {}

    ~MockSubscriber();

    void AddSubscription(uint8_t service_id,
                         uint16_t service_version,
                         uint16_t message_id) override {
        std::lock_guard<std::mutex> lock(mutex_);
        const MessageKey key{
            service_id, service_version, message_id};
        if (subscriptions_.find(key) == subscriptions_.end()) {
            SubscriptionRule rule;
            rule.all_values = true;
            subscriptions_.emplace(key, std::move(rule));
        }
    }

    void AddSubscriptionByFieldValues(uint8_t service_id,
                                      uint16_t service_version,
                                      uint16_t message_id,
                                      const char* field_name,
                                      const char** field_values,
                                      uint32_t field_value_count) override {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<std::string> additions;
        for (uint32_t i = 0; i < field_value_count; ++i) {
            if (field_values != nullptr &&
                field_values[i] != nullptr) {
                additions.emplace_back(field_values[i]);
            }
        }
        std::sort(additions.begin(), additions.end());
        additions.erase(
            std::unique(additions.begin(), additions.end()),
            additions.end());

        const MessageKey key{
            service_id, service_version, message_id};
        auto subscription = subscriptions_.find(key);
        if (additions.empty()) {
            if (subscription == subscriptions_.end()) {
                SubscriptionRule rule;
                rule.all_values = true;
                subscriptions_.emplace(key, std::move(rule));
            }
            return;
        }

        const std::string name = field_name == nullptr ? "" : field_name;
        if (subscription == subscriptions_.end()) {
            SubscriptionRule rule;
            rule.field_patterns.emplace(name, std::move(additions));
            subscriptions_.emplace(key, std::move(rule));
            return;
        }

        SubscriptionRule& rule = subscription->second;
        if (!rule.field_patterns.empty() &&
            rule.field_patterns.begin()->first != name) {
            return;
        }
        rule.all_values = false;
        std::vector<std::string>& patterns = rule.field_patterns[name];
        patterns.insert(
            patterns.end(), additions.begin(), additions.end());
        std::sort(patterns.begin(), patterns.end());
        patterns.erase(
            std::unique(patterns.begin(), patterns.end()),
            patterns.end());
    }

    void DelSubscription(uint8_t service_id,
                         uint16_t service_version,
                         uint16_t message_id) override {
        std::lock_guard<std::mutex> lock(mutex_);
        subscriptions_.erase(
            MessageKey{service_id, service_version, message_id});
    }

    void DelSubscriptionByFieldValues(uint8_t service_id,
                                      uint16_t service_version,
                                      uint16_t message_id,
                                      const char* field_name,
                                      const char** field_values,
                                      uint32_t field_value_count) override {
        std::lock_guard<std::mutex> lock(mutex_);
        const MessageKey key{service_id, service_version, message_id};
        auto subscription = subscriptions_.find(key);
        if (subscription == subscriptions_.end()) {
            return;
        }
        if (subscription->second.all_values ||
            field_value_count == 0) {
            return;
        }
        const std::string name = field_name == nullptr ? "" : field_name;
        auto field = subscription->second.field_patterns.find(name);
        if (field == subscription->second.field_patterns.end()) {
            return;
        }
        for (uint32_t i = 0; i < field_value_count; ++i) {
            if (field_values == nullptr || field_values[i] == nullptr) {
                continue;
            }
            const std::string value(field_values[i]);
            field->second.erase(
                std::remove(field->second.begin(), field->second.end(), value),
                field->second.end());
        }
        if (field->second.empty()) {
            subscription->second.field_patterns.erase(field);
        }
        if (subscription->second.field_patterns.empty()) {
            subscriptions_.erase(subscription);
        }
    }

    void ClearSubscriptions() override {
        std::lock_guard<std::mutex> lock(mutex_);
        subscriptions_.clear();
    }

    void SetHeartbeatInterval(uint32_t interval) override {
        heartbeat_interval_.store(interval, std::memory_order_relaxed);
    }

    uint32_t GetHeartbeatInterval() override {
        return heartbeat_interval_.load(std::memory_order_relaxed);
    }

    void SetHeartbeatTimeout(uint32_t timeout) override {
        heartbeat_timeout_.store(timeout, std::memory_order_relaxed);
    }

    uint32_t GetHeartbeatTimeout() override {
        return heartbeat_timeout_.load(std::memory_order_relaxed);
    }

    void SetUserName(const char* user_name) override {
        std::lock_guard<std::mutex> lock(mutex_);
        user_name_ = user_name == nullptr ? "" : user_name;
    }

    const char* GetUserName() override {
        std::lock_guard<std::mutex> lock(mutex_);
        return user_name_.c_str();
    }

    void SetPassword(const char* password) override {
        std::lock_guard<std::mutex> lock(mutex_);
        password_ = password == nullptr ? "" : password;
    }

    const char* GetPassword() override {
        std::lock_guard<std::mutex> lock(mutex_);
        return password_.c_str();
    }

    void SetMessageEncoding(MDLMessageEncoding encoding) override {
        encoding_.store(static_cast<int>(encoding), std::memory_order_relaxed);
    }

    MDLMessageEncoding GetMessageEncoding() override {
        return static_cast<MDLMessageEncoding>(
            encoding_.load(std::memory_order_relaxed));
    }

    void SetServerAddress(const char* address) override {
        std::lock_guard<std::mutex> lock(mutex_);
        server_address_ = address == nullptr ? "" : address;
    }

    const char* GetServerAddress() override {
        std::lock_guard<std::mutex> lock(mutex_);
        return server_address_.c_str();
    }

    bool PostRequest(MDLMessage*) override {
        return false;
    }

    bool SendRequest(MDLMessage*) override {
        return false;
    }

    const char* Connect() override;

    void SetReadBufferSize(int value) override {
        read_buffer_size_.store(value, std::memory_order_relaxed);
    }

    void SetSendMacAuth(bool send) override {
        send_mac_auth_.store(send, std::memory_order_relaxed);
    }

    void EnableServerSelect(bool enable) override {
        server_select_.store(enable, std::memory_order_relaxed);
    }

    void ReSubscribe() override {}

    const char* GetSubscription() override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (subscriptions_.empty()) {
            subscription_text_ = "null\n";
            return subscription_text_.c_str();
        }
        std::ostringstream output;
        bool first = true;
        output << '[';
        for (const auto& entry : subscriptions_) {
            if (entry.second.all_values) {
                if (!first) {
                    output << ',';
                }
                first = false;
                output << "{\"mid\":" << entry.first.message_id
                       << ",\"sid\":"
                       << static_cast<unsigned>(
                              entry.first.service_id)
                       << '}';
                continue;
            }
            for (const auto& field :
                 entry.second.field_patterns) {
                if (!first) {
                    output << ',';
                }
                first = false;
                output << "{\"fieldname\":";
                AppendJsonString(&output, field.first);
                output << ",\"fieldvalues\":[";
                for (size_t i = 0; i < field.second.size(); ++i) {
                    if (i != 0U) {
                        output << ',';
                    }
                    AppendJsonString(&output, field.second[i]);
                }
                output << "],\"mid\":" << entry.first.message_id
                       << ",\"sid\":"
                       << static_cast<unsigned>(
                              entry.first.service_id)
                       << '}';
            }
        }
        output << "]\n";
        subscription_text_ = output.str();
        return subscription_text_.c_str();
    }

    void EnableMergeMessage(bool enable) override {
        merge_message_.store(enable, std::memory_order_relaxed);
    }

    bool Matches(
        const MessageKey& key,
        const std::optional<std::string>& instrument) const {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto subscription = subscriptions_.find(key);
        if (subscription == subscriptions_.end()) {
            return false;
        }
        const SubscriptionRule& rule = subscription->second;
        if (rule.all_values) {
            return true;
        }
        if (!instrument.has_value()) {
            return false;
        }
        const char* identifier_field = nullptr;
        switch (key.service_id) {
        case MDLSID_MDL_SHL2:
        case MDLSID_MDL_SZL2:
            identifier_field = "SecurityID";
            break;
        case MDLSID_MDL_CFFEXL2:
        case MDLSID_MDL_SHFEL2:
        case MDLSID_MDL_CZCEL2:
        case MDLSID_MDL_DCEL2:
        case MDLSID_MDL_GFEXL2:
            identifier_field = "InstruID";
            break;
        default:
            return false;
        }
        for (const auto& field : rule.field_patterns) {
            if (field.first != identifier_field) {
                continue;
            }
            for (const std::string& pattern : field.second) {
                if (GlobMatch(pattern, *instrument)) {
                    return true;
                }
            }
        }
        return false;
    }

    bool MarkConnected() {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        if (connected_) {
            return false;
        }
        connected_ = true;
        if (++connection_generation_ == 0U) {
            ++connection_generation_;
        }
        return true;
    }

    bool MarkDisconnected(uint64_t* generation = nullptr) {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        if (generation != nullptr) {
            *generation = connection_generation_;
        }
        if (!connected_) {
            return false;
        }
        connected_ = false;
        return true;
    }

    bool SnapshotConnection(uint64_t* generation) const {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        if (!connected_) {
            return false;
        }
        *generation = connection_generation_;
        return true;
    }

    MessageHandlerBase* handler() const {
        return handler_;
    }

    bool multithread_callback() const {
        return multithread_callback_;
    }

    bool TryBeginCallback(uint64_t generation) {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        if (!connected_ || generation != connection_generation_) {
            return false;
        }
        ++callbacks_in_flight_[generation];
        return true;
    }

    void EndCallback(uint64_t generation) {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        auto callbacks = callbacks_in_flight_.find(generation);
        if (callbacks == callbacks_in_flight_.end()) {
            return;
        }
        if (--callbacks->second == 0U) {
            callbacks_in_flight_.erase(callbacks);
            callbacks_finished_.notify_all();
        }
    }

    void WaitForCallbacks(uint64_t generation) {
        // A callback cannot synchronously wait for arbitrary other callbacks:
        // two handlers stopping each other would deadlock.  The owner thread
        // can call IOManager::Shutdown() as the final synchronization point.
        if (active_callback_frame != nullptr) {
            return;
        }
        std::unique_lock<std::mutex> lock(callback_mutex_);
        callbacks_finished_.wait(
            lock, [this, generation] {
                return callbacks_in_flight_.find(generation) ==
                       callbacks_in_flight_.end();
            });
    }

    void Disconnect();

private:
    std::shared_ptr<Runtime> runtime_;
    MessageHandlerBase* handler_;
    bool multithread_callback_;
    mutable std::mutex mutex_;
    std::map<MessageKey, SubscriptionRule, KeyLess> subscriptions_;
    std::string user_name_;
    std::string password_;
    std::string server_address_;
    std::string subscription_text_;
    std::string connect_result_;
    std::atomic<uint32_t> heartbeat_interval_{0};
    std::atomic<uint32_t> heartbeat_timeout_{0};
    std::atomic<int> encoding_{MDLEID_BINARY};
    std::atomic<int> read_buffer_size_{0};
    std::atomic<bool> send_mac_auth_{false};
    std::atomic<bool> server_select_{false};
    std::atomic<bool> merge_message_{false};
    mutable std::mutex callback_mutex_;
    std::condition_variable callbacks_finished_;
    bool connected_ = false;
    uint64_t connection_generation_ = 0;
    std::map<uint64_t, uint64_t> callbacks_in_flight_;
};

std::optional<std::string> ReadIdentifierAt(
    const MDLMessage* message,
    size_t fixed_body_size,
    size_t field_offset) {
    if (message == nullptr || message->GetHead() == nullptr ||
        message->GetBody() == nullptr) {
        return std::nullopt;
    }
    const MDLMessageHead* const head = message->GetHead();
    if (head->MessageSize < head->HeadSize) {
        return std::nullopt;
    }
    const size_t body_size =
        static_cast<size_t>(head->MessageSize - head->HeadSize);
    if (body_size < fixed_body_size ||
        field_offset > body_size ||
        sizeof(MDLString) > body_size - field_offset) {
        return std::nullopt;
    }

    MDLString field;
    std::memcpy(
        &field, message->GetBody() + field_offset, sizeof(field));
    if (field.Length == 0U) {
        return std::string();
    }
    if (field.Offset == 0U ||
        static_cast<size_t>(field.Offset) > body_size - field_offset) {
        return std::nullopt;
    }
    const size_t data_offset =
        field_offset + static_cast<size_t>(field.Offset);
    if (static_cast<size_t>(field.Length) > body_size - data_offset) {
        return std::nullopt;
    }
    return std::string(
        message->GetBody() + data_offset,
        static_cast<size_t>(field.Length));
}

template <typename T>
std::optional<std::string> ReadSecurityID(const MDLMessage* message) {
    return ReadIdentifierAt(
        message, sizeof(T), offsetof(T, SecurityID));
}

template <typename T>
std::optional<std::string> ReadInstruID(const MDLMessage* message) {
    return ReadIdentifierAt(
        message, sizeof(T), offsetof(T, InstruID));
}

std::optional<std::string> ExtractIdentifier(
    const MDLMessage* message, const MessageKey& key) {
    if (key.service_version != 101U) {
        return std::nullopt;
    }
    switch (key.service_id) {
    case MDLSID_MDL_SHL2:
        switch (key.message_id) {
        case sh::SHL2Transaction::MessageID:
            return ReadSecurityID<sh::SHL2Transaction>(message);
        case sh::SHL2MarketData::MessageID:
            return ReadSecurityID<sh::SHL2MarketData>(message);
        case sh::SHL2Transaction2::MessageID:
            return ReadSecurityID<sh::SHL2Transaction2>(message);
        case sh::Order::MessageID:
            return ReadSecurityID<sh::Order>(message);
        default:
            return std::nullopt;
        }

    case MDLSID_MDL_SZL2:
        switch (key.message_id) {
        case sz::Trade::MessageID:
            return ReadSecurityID<sz::Trade>(message);
        case sz::Order::MessageID:
            return ReadSecurityID<sz::Order>(message);
        case sz::MarketData::MessageID:
            return ReadSecurityID<sz::MarketData>(message);
        case sz::Snapshot300111_v2::MessageID:
            return ReadSecurityID<sz::Snapshot300111_v2>(message);
        case sz::Order300192_v2::MessageID:
            return ReadSecurityID<sz::Order300192_v2>(message);
        case sz::Transaction300191_v2::MessageID:
            return ReadSecurityID<sz::Transaction300191_v2>(message);
        case sz::Snapshot300111_v3::MessageID:
            return ReadSecurityID<sz::Snapshot300111_v3>(message);
        case sz::CombinedTick::MessageID:
            return ReadSecurityID<sz::CombinedTick>(message);
        default:
            return std::nullopt;
        }

    case MDLSID_MDL_CFFEXL2:
        if (key.message_id == cf::Future::MessageID) {
            return ReadInstruID<cf::Future>(message);
        }
        if (key.message_id == cf::Option::MessageID) {
            return ReadInstruID<cf::Option>(message);
        }
        return std::nullopt;

    case MDLSID_MDL_SHFEL2:
        switch (key.message_id) {
        case sf::CTPFuture::MessageID:
            return ReadInstruID<sf::CTPFuture>(message);
        case sf::CTPOption::MessageID:
            return ReadInstruID<sf::CTPOption>(message);
        case sf::CrudeFuture::MessageID:
            return ReadInstruID<sf::CrudeFuture>(message);
        case sf::CrudeOption::MessageID:
            return ReadInstruID<sf::CrudeOption>(message);
        default:
            return std::nullopt;
        }

    case MDLSID_MDL_CZCEL2:
        if (key.message_id == cz::CTPFuture::MessageID) {
            return ReadInstruID<cz::CTPFuture>(message);
        }
        if (key.message_id == cz::CTPOption::MessageID) {
            return ReadInstruID<cz::CTPOption>(message);
        }
        return std::nullopt;

    case MDLSID_MDL_DCEL2:
        switch (key.message_id) {
        case dc::Future::MessageID:
            return ReadInstruID<dc::Future>(message);
        case dc::Option::MessageID:
            return ReadInstruID<dc::Option>(message);
        case dc::FutureOrder::MessageID:
            return ReadInstruID<dc::FutureOrder>(message);
        case dc::OptionOrder::MessageID:
            return ReadInstruID<dc::OptionOrder>(message);
        default:
            return std::nullopt;
        }

    case MDLSID_MDL_GFEXL2:
        switch (key.message_id) {
        case gf::Future::MessageID:
            return ReadInstruID<gf::Future>(message);
        case gf::Option::MessageID:
            return ReadInstruID<gf::Option>(message);
        case gf::FutureOrder::MessageID:
            return ReadInstruID<gf::FutureOrder>(message);
        case gf::OptionOrder::MessageID:
            return ReadInstruID<gf::OptionOrder>(message);
        default:
            return std::nullopt;
        }

    default:
        return std::nullopt;
    }
}

class Runtime : public std::enable_shared_from_this<Runtime> {
public:
    explicit Runtime(const Config& config)
        : config_(config),
          generator_(config_) {}

    ~Runtime() {
        Shutdown();
    }

    bool AddSubscriber(MockSubscriber* subscriber) {
        std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
        if (stopping_.load(std::memory_order_acquire)) {
            return false;
        }
        subscriber->AddRef();
        std::lock_guard<std::mutex> subscribers_lock(subscribers_mutex_);
        subscribers_.push_back(subscriber);
        return true;
    }

    std::string Connect(MockSubscriber* subscriber) {
        if (subscriber->handler() == nullptr) {
            return "mock subscriber requires a non-null MessageHandlerBase";
        }
        std::unique_lock<std::mutex> lock(lifecycle_mutex_);
        if (stopping_.load(std::memory_order_acquire)) {
            return "mock IOManager has been shut down";
        }
        const bool newly_connected = subscriber->MarkConnected();
        if (newly_connected) {
            active_subscribers_.fetch_add(1, std::memory_order_relaxed);
        }
        if (!engine_.joinable()) {
            try {
                for (uint32_t i = 0; i < config_.callback_threads; ++i) {
                    callback_workers_.emplace_back(
                        &Runtime::CallbackLoop, this);
                }
                engine_ = std::thread(&Runtime::EngineLoop, this);
            } catch (...) {
                if (newly_connected && subscriber->MarkDisconnected()) {
                    active_subscribers_.fetch_sub(
                        1, std::memory_order_relaxed);
                }
                stopping_.store(true, std::memory_order_release);
                lock.unlock();
                queue_not_empty_.notify_all();
                queue_not_full_.notify_all();
                FinishShutdown();
                return "mock IOManager could not start its worker threads";
            }
        }
        return std::string();
    }

    void Disconnect(MockSubscriber* subscriber) {
        uint64_t disconnected_generation = 0;
        {
            std::lock_guard<std::mutex> lock(lifecycle_mutex_);
            if (subscriber->MarkDisconnected(
                    &disconnected_generation)) {
                active_subscribers_.fetch_sub(1, std::memory_order_relaxed);
            }
        }
        subscriber->WaitForCallbacks(disconnected_generation);
    }

    void Shutdown() {
        {
            std::lock_guard<std::mutex> lock(lifecycle_mutex_);
            stopping_.store(true, std::memory_order_release);
        }
        queue_not_empty_.notify_all();
        queue_not_full_.notify_all();

        // Joining the current engine/callback thread would deadlock.  Complete
        // teardown on a short-lived coordinator while retaining this Runtime.
        // Normal owner-thread Shutdown remains synchronous.
        if (IsActiveRuntime(this)) {
            bool expected = false;
            if (shutdown_coordinator_started_.compare_exchange_strong(
                    expected, true, std::memory_order_acq_rel)) {
                const std::shared_ptr<Runtime> self = shared_from_this();
                std::thread([self] { self->FinishShutdown(); }).detach();
            }
            return;
        }
        FinishShutdown();
    }

    Statistics statistics() const {
        Statistics value;
        value.generated = generated_.load(std::memory_order_relaxed);
        value.delivered = delivered_.load(std::memory_order_relaxed);
        value.filtered = filtered_.load(std::memory_order_relaxed);
        value.dropped = dropped_.load(std::memory_order_relaxed);
        value.callback_errors =
            callback_errors_.load(std::memory_order_relaxed);
        value.queue_high_watermark =
            queue_high_watermark_.load(std::memory_order_relaxed);
        value.active_subscribers =
            active_subscribers_.load(std::memory_order_relaxed);
        return value;
    }

    void Publish(const MDLMessage* message, bool asynchronous) {
        if (message == nullptr || message->GetHead() == nullptr) {
            return;
        }
        if (!BeginExternalOperation()) {
            return;
        }
        RuntimeThreadScope runtime_scope(this);
        try {
            PublishWhileActive(message, asynchronous);
        } catch (...) {
            EndExternalOperation();
            throw;
        }
        EndExternalOperation();
    }

private:
    bool BeginExternalOperation() {
        std::lock_guard<std::mutex> lock(operation_mutex_);
        if (stopping_.load(std::memory_order_acquire)) {
            return false;
        }
        ++active_external_operations_;
        return true;
    }

    void EndExternalOperation() {
        std::lock_guard<std::mutex> lock(operation_mutex_);
        if (--active_external_operations_ == 0U) {
            operation_condition_.notify_all();
        }
    }

    void PublishWhileActive(const MDLMessage* message, bool asynchronous) {
        const MDLMessageHead* head = message->GetHead();
        const MessageKey key{
            head->ServiceID, head->ServiceVersion, head->MessageID};
        MDLMessagePtr owned = message->Copy();
        if (owned.IsNull()) {
            callback_errors_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        const std::optional<std::string> identifier =
            ExtractIdentifier(owned.Get(), key);
        std::vector<Target> targets = Interested(key, identifier);
        if (targets.empty()) {
            filtered_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        generated_.fetch_add(1, std::memory_order_relaxed);
        for (const Target& target : targets) {
            if (asynchronous) {
                Enqueue(target, owned);
            } else {
                Invoke(target, owned);
            }
        }
    }

    void FinishShutdown() {
        {
            std::unique_lock<std::mutex> lock(shutdown_mutex_);
            if (shutdown_complete_) {
                return;
            }
            if (shutdown_in_progress_) {
                shutdown_complete_condition_.wait(
                    lock, [this] { return shutdown_complete_; });
                return;
            }
            shutdown_in_progress_ = true;
        }

        if (engine_.joinable() &&
            engine_.get_id() != std::this_thread::get_id()) {
            engine_.join();
        }

        {
            std::unique_lock<std::mutex> lock(operation_mutex_);
            operation_condition_.wait(
                lock, [this] { return active_external_operations_ == 0U; });
        }

        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            queue_stopping_ = true;
        }
        queue_not_empty_.notify_all();
        queue_not_full_.notify_all();
        for (std::thread& worker : callback_workers_) {
            if (worker.joinable() && worker.get_id() != std::this_thread::get_id()) {
                worker.join();
            }
        }
        callback_workers_.clear();

        std::vector<MockSubscriber*> subscribers;
        {
            std::lock_guard<std::mutex> lock(subscribers_mutex_);
            subscribers.swap(subscribers_);
        }
        for (MockSubscriber* subscriber : subscribers) {
            subscriber->MarkDisconnected();
            subscriber->ReleaseRef();
        }
        active_subscribers_.store(0, std::memory_order_relaxed);

        {
            std::lock_guard<std::mutex> lock(shutdown_mutex_);
            shutdown_complete_ = true;
        }
        shutdown_complete_condition_.notify_all();
    }

    struct Target {
        MockSubscriber* subscriber = nullptr;
        uint64_t connection_generation = 0;
    };

    struct Task {
        Target target;
        MDLMessagePtr message;
    };

    std::vector<Target> Interested(
        const MessageKey& key,
        const std::optional<std::string>& instrument) {
        std::vector<Target> targets;
        std::lock_guard<std::mutex> lock(subscribers_mutex_);
        for (MockSubscriber* subscriber : subscribers_) {
            if (!subscriber->Matches(key, instrument)) {
                continue;
            }
            uint64_t connection_generation = 0;
            if (subscriber->SnapshotConnection(
                    &connection_generation)) {
                Target target;
                target.subscriber = subscriber;
                target.connection_generation = connection_generation;
                targets.push_back(target);
            }
        }
        return targets;
    }

    void EngineLoop() {
        RuntimeThreadScope runtime_scope(this);
        auto next_deadline = std::chrono::steady_clock::now();
        bool pacing_active = false;
        uint32_t empty_iterations = 0;
        while (!stopping_.load(std::memory_order_acquire)) {
            Generator::Result result;
            try {
                result = generator_.Next();
            } catch (...) {
                callback_errors_.fetch_add(1, std::memory_order_relaxed);
                stopping_.store(true, std::memory_order_release);
                break;
            }
            const MDLMessageHead* head = result.message->GetHead();
            const MessageKey key{
                head->ServiceID, head->ServiceVersion, head->MessageID};
            std::vector<Target> targets =
                Interested(key, std::optional<std::string>(
                                    result.security_id));
            if (targets.empty()) {
                pacing_active = false;
                filtered_.fetch_add(1, std::memory_order_relaxed);
                if (++empty_iterations >= 1024) {
                    empty_iterations = 0;
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                continue;
            }
            empty_iterations = 0;

            if (config_.messages_per_second != 0) {
                const auto interval =
                    std::chrono::duration_cast<
                        std::chrono::steady_clock::duration>(
                        std::chrono::duration<double>(
                            1.0 /
                            static_cast<double>(
                                config_.messages_per_second)));
                const auto now = std::chrono::steady_clock::now();
                if (!pacing_active) {
                    next_deadline = now;
                    pacing_active = true;
                }
                next_deadline += interval;
                if (next_deadline > now) {
                    std::this_thread::sleep_until(next_deadline);
                }
            }
            if (stopping_.load(std::memory_order_acquire)) {
                break;
            }

            generated_.fetch_add(1, std::memory_order_relaxed);
            for (const Target& target : targets) {
                if (target.subscriber->multithread_callback()) {
                    Enqueue(target, result.message);
                } else {
                    Invoke(target, result.message);
                }
            }
        }
    }

    void Enqueue(const Target& target, const MDLMessagePtr& message) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        const bool called_from_callback =
            active_callback_frame != nullptr;
        if (config_.backpressure == BackpressurePolicy::Block &&
            !called_from_callback) {
            queue_not_full_.wait(lock, [this] {
                return stopping_.load(std::memory_order_acquire) ||
                       queue_.size() < config_.callback_queue_capacity;
            });
        }
        if (stopping_.load(std::memory_order_acquire)) {
            return;
        }
        if (queue_.size() >= config_.callback_queue_capacity) {
            dropped_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        Task task;
        task.target = target;
        task.message = message;
        queue_.push_back(task);
        const uint64_t size = queue_.size();
        uint64_t high = queue_high_watermark_.load(std::memory_order_relaxed);
        while (size > high &&
               !queue_high_watermark_.compare_exchange_weak(
                   high, size, std::memory_order_relaxed)) {
        }
        lock.unlock();
        queue_not_empty_.notify_one();
    }

    void CallbackLoop() {
        RuntimeThreadScope runtime_scope(this);
        for (;;) {
            Task task;
            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                queue_not_empty_.wait(lock, [this] {
                    return queue_stopping_ || !queue_.empty();
                });
                if (queue_.empty() && queue_stopping_) {
                    return;
                }
                task = std::move(queue_.front());
                queue_.pop_front();
            }
            queue_not_full_.notify_one();
            Invoke(task.target, task.message);
        }
    }

    void Invoke(const Target& target, const MDLMessagePtr& message) {
        MockSubscriber* const subscriber = target.subscriber;
        std::unique_lock<std::recursive_mutex> serial_lock(
            serialized_callbacks_mutex_, std::defer_lock);
        if (!subscriber->multithread_callback()) {
            serial_lock.lock();
        }
        if (!subscriber->TryBeginCallback(
                target.connection_generation)) {
            return;
        }
        ActiveCallbackFrame callback_frame{
            subscriber, active_callback_frame};
        active_callback_frame = &callback_frame;
        try {
            MessageHandlerBase* handler = subscriber->handler();
            if (handler != nullptr) {
                handler->OnMessage(subscriber, message.Get());
                delivered_.fetch_add(1, std::memory_order_relaxed);
            }
        } catch (...) {
            callback_errors_.fetch_add(1, std::memory_order_relaxed);
        }
        active_callback_frame = callback_frame.previous;
        subscriber->EndCallback(target.connection_generation);
    }

    Config config_;
    Generator generator_;
    mutable std::mutex lifecycle_mutex_;
    mutable std::mutex subscribers_mutex_;
    std::vector<MockSubscriber*> subscribers_;
    std::thread engine_;
    std::vector<std::thread> callback_workers_;
    std::atomic<bool> stopping_{false};
    std::atomic<bool> shutdown_coordinator_started_{false};
    std::mutex shutdown_mutex_;
    std::condition_variable shutdown_complete_condition_;
    bool shutdown_in_progress_ = false;
    bool shutdown_complete_ = false;
    std::mutex operation_mutex_;
    std::condition_variable operation_condition_;
    uint64_t active_external_operations_ = 0;

    // A Runtime-wide recursive gate preserves synchronous re-entry while
    // preventing two serialized subscribers from taking each other's
    // callback locks in opposite order during nested SyncPublish calls.
    std::recursive_mutex serialized_callbacks_mutex_;
    std::mutex queue_mutex_;
    std::condition_variable queue_not_empty_;
    std::condition_variable queue_not_full_;
    std::deque<Task> queue_;
    bool queue_stopping_ = false;

    std::atomic<uint64_t> generated_{0};
    std::atomic<uint64_t> delivered_{0};
    std::atomic<uint64_t> filtered_{0};
    std::atomic<uint64_t> dropped_{0};
    std::atomic<uint64_t> callback_errors_{0};
    std::atomic<uint64_t> queue_high_watermark_{0};
    std::atomic<uint64_t> active_subscribers_{0};
};

MockSubscriber::~MockSubscriber() {
    Disconnect();
}

const char* MockSubscriber::Connect() {
    connect_result_ = runtime_->Connect(this);
    return connect_result_.c_str();
}

void MockSubscriber::Disconnect() {
    if (runtime_) {
        runtime_->Disconnect(this);
    }
}

class MockPublisher final
    : public AtomicRefCounted<MockPublisher, Publisher> {
public:
    explicit MockPublisher(PublisherType type) : type_(type) {}

    PublisherType GetType() override {
        return type_;
    }

    const char* Listen() override {
        result_.clear();
        return result_.c_str();
    }

    void SetListenAddress(const char* address) override {
        address_ = address == nullptr ? "" : address;
    }

    const char* GetListenAddress() override {
        return address_.c_str();
    }

private:
    PublisherType type_;
    std::string address_;
    std::string result_;
};

class MockAutoSubscriber final
    : public AtomicRefCounted<MockAutoSubscriber, AutoSubscriber> {
public:
    MockAutoSubscriber(std::shared_ptr<Runtime> runtime,
                       MessageHandlerBase* handler,
                       bool multithread_callback)
        : runtime_(std::move(runtime)),
          handler_(handler),
          multithread_callback_(multithread_callback) {}

    ~MockAutoSubscriber() {
        Stop();
    }

    void SetURL(const char* url) override {
        std::lock_guard<std::mutex> lock(state_mutex_);
        url_ = url == nullptr ? "" : url;
    }

    void SetToken(const char* token) override {
        std::lock_guard<std::mutex> lock(state_mutex_);
        token_ = token == nullptr ? "" : token;
    }

    void SetOptionsJson(const char* options_json) override {
        std::lock_guard<std::mutex> lock(state_mutex_);
        options_json_ = options_json == nullptr ? "" : options_json;
    }

    void SetDailyCheckTime(int value) override {
        std::lock_guard<std::mutex> lock(state_mutex_);
        daily_check_time_ = value;
    }

    void SetQueryTimeoutMs(int value) override {
        std::lock_guard<std::mutex> lock(state_mutex_);
        query_timeout_ms_ = value;
    }

    const char* Start() override {
        std::lock_guard<std::mutex> start_lock(start_mutex_);
        SubscriberPtr subscriber;
        {
            std::lock_guard<std::mutex> state_lock(state_mutex_);
            if (subscriber_.IsNull()) {
                MockSubscriber* raw = new MockSubscriber(
                    runtime_, handler_, multithread_callback_);
                runtime_->AddSubscriber(raw);
                subscriber_ = PtrFromReturn<Subscriber>(raw);
                SubscribeAll(subscriber_.Get());
            }
            subscriber = subscriber_;
        }
        const char* const connect_result = subscriber->Connect();
        result_ = connect_result == nullptr
                      ? "mock subscriber Connect returned null"
                      : connect_result;
        return result_.c_str();
    }

    void Stop() override {
        SubscriberPtr subscriber;
        {
            std::lock_guard<std::mutex> state_lock(state_mutex_);
            subscriber = subscriber_;
        }
        if (!subscriber.IsNull()) {
            static_cast<MockSubscriber*>(subscriber.Get())->Disconnect();
        }
    }

private:
    std::shared_ptr<Runtime> runtime_;
    MessageHandlerBase* handler_;
    bool multithread_callback_;
    std::mutex state_mutex_;
    std::mutex start_mutex_;
    SubscriberPtr subscriber_;
    std::string url_;
    std::string token_;
    std::string options_json_;
    std::string result_;
    int daily_check_time_ = 0;
    int query_timeout_ms_ = 0;
};

class MockIOManager final
    : public AtomicRefCounted<MockIOManager, IOManager> {
public:
    explicit MockIOManager(const Config& config)
        : runtime_(std::make_shared<Runtime>(config)) {}

    ~MockIOManager() {
        runtime_->Shutdown();
    }

    AutoSubscriberPtr CreateAutoSubscriber(
        MessageHandlerBase* handler,
        bool multithread_callback = false) override {
        AutoSubscriber* raw =
            new MockAutoSubscriber(runtime_, handler, multithread_callback);
        return PtrFromReturn(raw);
    }

    bool RegisterMessage(uint8_t service_id,
                         uint16_t service_version,
                         uint16_t message_id) override {
        const MessageKey key{service_id, service_version, message_id};
        const auto& messages = SupportedMessageStorage();
        return std::any_of(
            messages.begin(), messages.end(),
            [&key](const SupportedMessage& message) {
                return SameKey(key, message.key);
            });
    }

    void AsyncResponse(Publisher*,
                       RefCounted*,
                       const MDLMessage* message) override {
        runtime_->Publish(message, true);
    }

    void AsyncPublish(const MDLMessage* message) override {
        runtime_->Publish(message, true);
    }

    void SyncPublish(const MDLMessage* message) override {
        runtime_->Publish(message, false);
    }

    void EnableLog(const char*, bool) override {}

    void Shutdown() override {
        runtime_->Shutdown();
    }

    void SetWorkersProxy(IOManager*) override {}

    Statistics statistics() const {
        return runtime_->statistics();
    }

protected:
    Publisher* _CreatePublisher(PublisherType type) override {
        return new MockPublisher(type);
    }

    Publisher* _GetPublisherByType(PublisherType type) override {
        return new MockPublisher(type);
    }

    Subscriber* _CreateSubscriber(
        MessageHandlerBase* handler,
        bool multithread_callback) override {
        MockSubscriber* subscriber =
            new MockSubscriber(runtime_, handler, multithread_callback);
        runtime_->AddSubscriber(subscriber);
        return subscriber;
    }

private:
    std::shared_ptr<Runtime> runtime_;
};

bool IsSevenBitAscii(const std::string& value) {
    for (unsigned char character : value) {
        if (character == 0 || character > 0x7fU) {
            return false;
        }
    }
    return true;
}

} // namespace

std::string ValidateConfig(const Config& config) {
    if (sizeof(MDLMessageHead) > std::numeric_limits<uint8_t>::max()) {
        return "MDLMessageHead cannot be represented by HeadSize";
    }
    if (config.book_depth == 0 || config.book_depth > 50) {
        return "book_depth must be in [1, 50]";
    }
    if (config.orders_per_level == 0 || config.orders_per_level > 100) {
        return "orders_per_level must be in [1, 100]";
    }
    if (config.callback_threads == 0 || config.callback_threads > 256) {
        return "callback_threads must be in [1, 256]";
    }
    if (config.callback_queue_capacity == 0 ||
        config.callback_queue_capacity > 10000000U) {
        return "callback_queue_capacity must be in [1, 10000000]";
    }
    if (config.backpressure != BackpressurePolicy::Block &&
        config.backpressure != BackpressurePolicy::DropNewest) {
        return "backpressure is not a supported BackpressurePolicy value";
    }
    if (config.clock_mode != ClockMode::Realtime &&
        config.clock_mode != ClockMode::SimulatedTradingDay) {
        return "clock_mode is not a supported ClockMode value";
    }
    if (config.max_realtime_lag_ms >= 24U * 60U * 60U * 1000U) {
        return "max_realtime_lag_ms must be less than one day";
    }
    if (config.simulated_min_step_ms > config.simulated_max_step_ms) {
        return "simulated_min_step_ms must not exceed simulated_max_step_ms";
    }
    if (!IsGregorianDate(config.simulated_start_date)) {
        return "simulated_start_date must be a valid Gregorian yyyymmdd date";
    }
    if (config.simulated_start_date / 10000U == 9999U) {
        return "simulated_start_date must be before year 9999 so it can roll "
               "to a following MDLDate";
    }
    const uint32_t simulated_weekday =
        GregorianWeekday(config.simulated_start_date);
    if (simulated_weekday == 0U || simulated_weekday == 6U) {
        return "simulated_start_date must be a Monday-Friday date";
    }
    if (!IsSimulatedSessionTime(config.simulated_start_time)) {
        return "simulated_start_time must be a valid hhmmssmmm time in "
               "09:30-11:30 or 13:00-15:00";
    }
    if (config.trading_phase_code.size() >
            std::numeric_limits<uint16_t>::max() ||
        !IsSevenBitAscii(config.trading_phase_code)) {
        return "trading_phase_code must be a 7-bit ASCII MDLAnsiString";
    }
    if (config.instruments.empty()) {
        if (config.shanghai_instruments > 100000U ||
            config.shenzhen_instruments > 100000U) {
            return "synthetic cash instrument counts must not exceed 100000";
        }
        if (config.include_derivatives &&
            config.derivatives_per_market > 10000U) {
            return "derivatives_per_market must not exceed 10000";
        }
        const uint64_t total =
            static_cast<uint64_t>(config.shanghai_instruments) +
            config.shenzhen_instruments +
            (config.include_derivatives
                 ? static_cast<uint64_t>(config.derivatives_per_market) * 5U
                 : 0U);
        if (total == 0) {
            return "the synthetic universe must contain at least one instrument";
        }
        return std::string();
    }
    if (config.instruments.size() > 250000U) {
        return "custom instruments must not exceed 250000 entries";
    }

    std::set<std::pair<uint8_t, std::string> > unique;
    for (size_t i = 0; i < config.instruments.size(); ++i) {
        const Instrument& instrument = config.instruments[i];
        const std::string prefix =
            "instruments[" + std::to_string(i) + "]: ";
        if (!IsSupportedService(instrument.service_id)) {
            return prefix + "service_id is not one of the seven L2 services";
        }
        if (instrument.security_id.empty() ||
            instrument.security_id.size() >
                std::numeric_limits<uint16_t>::max() ||
            !IsSevenBitAscii(instrument.security_id)) {
            return prefix +
                   "security_id must be non-empty 7-bit ASCII and fit uint16";
        }
        if (instrument.reference_price_milli <= 0) {
            return prefix + "reference_price_milli must be positive";
        }
        if (instrument.reference_price_milli > LLONG_MAX / 10000) {
            return prefix +
                   "reference_price_milli is too large for safe fixed-point math";
        }
        if (instrument.tick_size_milli <= 0) {
            return prefix + "tick_size_milli must be positive";
        }
        if (instrument.tick_size_milli >
            LLONG_MAX /
                static_cast<int64_t>(config.book_depth + 2U)) {
            return prefix + "tick_size_milli is too large";
        }
        if (instrument.reference_price_milli %
                instrument.tick_size_milli !=
            0) {
            return prefix +
                   "reference_price_milli must align to tick_size_milli";
        }
        if (instrument.lot_size == 0 || instrument.lot_size > 1000000U) {
            return prefix + "lot_size must be in [1, 1000000]";
        }
        if (instrument.service_id == MDLSID_MDL_CFFEXL2 ||
            instrument.service_id == MDLSID_MDL_SHFEL2) {
            const uint64_t maximum_level_quantity =
                static_cast<uint64_t>(instrument.lot_size) *
                config.orders_per_level * 50U;
            if (maximum_level_quantity >
                static_cast<uint64_t>(INT_MAX)) {
                return prefix +
                       "lot_size and orders_per_level can overflow the "
                       "SDK int32 book volume";
            }
        }
        if (instrument.service_id == MDLSID_MDL_SHL2 &&
            instrument.reference_price_milli >= INT_MAX / 2) {
            return prefix +
                   "Shanghai prices must fit the SDK's MDLFloatT<3>";
        }
        if (instrument.service_id == MDLSID_MDL_SHL2) {
            const int64_t maximum_trade_price =
                instrument.reference_price_milli +
                instrument.reference_price_milli / 10;
            const int64_t maximum_trade_quantity =
                static_cast<int64_t>(instrument.lot_size) * 100;
            if (maximum_trade_quantity > LLONG_MAX / 100 ||
                maximum_trade_price >
                    LLONG_MAX / (maximum_trade_quantity * 100)) {
                return prefix +
                       "reference_price_milli and lot_size can overflow "
                       "SH TradeMoney at scale 5";
            }
        }
        if (instrument.service_id == MDLSID_MDL_SZL2) {
            const int64_t maximum_trade_price =
                instrument.reference_price_milli +
                instrument.reference_price_milli / 10;
            if (maximum_trade_price >
                (LLONG_MAX / 10) /
                    static_cast<int64_t>(instrument.lot_size)) {
                return prefix +
                       "reference_price_milli and lot_size leave no room "
                       "for one Shenzhen lot in cumulative turnover";
            }
        }
        if (!IsCashService(instrument.service_id)) {
            const int64_t maximum_trade_price =
                instrument.reference_price_milli +
                instrument.reference_price_milli / 5;
            if (maximum_trade_price >
                (LLONG_MAX / 2) /
                    static_cast<int64_t>(instrument.lot_size)) {
                return prefix +
                       "reference_price_milli and lot_size leave no room "
                       "for one derivative lot in cumulative turnover";
            }
        }
        const int64_t limit_bps =
            IsCashService(instrument.service_id) ? 1000 : 2000;
        const int64_t price_room =
            instrument.reference_price_milli * limit_bps / 10000;
        const int64_t required_room =
            instrument.tick_size_milli *
            static_cast<int64_t>(config.book_depth + 2U);
        if (price_room <= required_room) {
            return prefix +
                   "reference price is too small for the requested depth/tick";
        }
        if (!unique.insert(
                 std::make_pair(instrument.service_id, instrument.security_id))
                 .second) {
            return prefix + "duplicate service_id/security_id";
        }
    }
    return std::string();
}

std::vector<Instrument> MakeSyntheticUniverse(
    uint32_t shanghai_count,
    uint32_t shenzhen_count,
    uint32_t derivatives_per_market,
    bool include_derivatives) {
    if (shanghai_count > 100000U || shenzhen_count > 100000U) {
        throw std::invalid_argument(
            "synthetic cash instrument counts must not exceed 100000");
    }
    if (include_derivatives && derivatives_per_market > 10000U) {
        throw std::invalid_argument(
            "derivatives_per_market must not exceed 10000");
    }
    std::vector<Instrument> result;
    const uint64_t total =
        static_cast<uint64_t>(shanghai_count) + shenzhen_count +
        (include_derivatives
             ? static_cast<uint64_t>(derivatives_per_market) * 5U
             : 0U);
    if (total > std::numeric_limits<size_t>::max()) {
        throw std::length_error("synthetic universe size overflow");
    }
    result.reserve(static_cast<size_t>(total));

    for (uint32_t i = 0; i < shanghai_count; ++i) {
        Instrument instrument;
        instrument.service_id = MDLSID_MDL_SHL2;
        std::ostringstream id;
        id << std::setw(6) << std::setfill('0') << 600000U + i;
        instrument.security_id = id.str();
        instrument.reference_price_milli =
            10000 +
            static_cast<int64_t>(
                (static_cast<uint64_t>(i) * 7919U) % 19000U) *
                10;
        instrument.tick_size_milli = 10;
        instrument.lot_size = 100;
        result.push_back(instrument);
    }
    for (uint32_t i = 0; i < shenzhen_count; ++i) {
        Instrument instrument;
        instrument.service_id = MDLSID_MDL_SZL2;
        std::ostringstream id;
        id << std::setw(6) << std::setfill('0') << i + 1U;
        instrument.security_id = id.str();
        instrument.reference_price_milli =
            10000 +
            static_cast<int64_t>(
                (static_cast<uint64_t>(i) * 7877U) % 19000U) *
                10;
        instrument.tick_size_milli = 10;
        instrument.lot_size = 100;
        result.push_back(instrument);
    }

    if (include_derivatives) {
        const std::pair<uint8_t, const char*> markets[] = {
            {MDLSID_MDL_CFFEXL2, "CFF"},
            {MDLSID_MDL_SHFEL2, "SHF"},
            {MDLSID_MDL_CZCEL2, "CZE"},
            {MDLSID_MDL_DCEL2, "DCE"},
            {MDLSID_MDL_GFEXL2, "GFE"}
        };
        for (const auto& market : markets) {
            for (uint32_t i = 0; i < derivatives_per_market; ++i) {
                Instrument instrument;
                instrument.service_id = market.first;
                std::ostringstream id;
                id << market.second << std::setw(5) << std::setfill('0')
                   << i + 1U;
                instrument.security_id = id.str();
                instrument.reference_price_milli =
                    500000 +
                    static_cast<int64_t>(
                        (static_cast<uint64_t>(i) * 12347U) % 45000U) *
                        100;
                instrument.tick_size_milli = 100;
                instrument.lot_size = 1;
                instrument.option = (i % 2U) != 0;
                result.push_back(instrument);
            }
        }
    }
    return result;
}

const std::vector<SupportedMessage>& SupportedMessages() {
    return SupportedMessageStorage();
}

void SubscribeAll(Subscriber* subscriber) {
    if (subscriber == nullptr) {
        return;
    }
    for (const SupportedMessage& message : SupportedMessageStorage()) {
        subscriber->AddSubscription(
            message.key.service_id,
            message.key.service_version,
            message.key.message_id);
    }
}

IOManagerPtr CreateIOManager(const Config& config) {
    const std::string error = ValidateConfig(config);
    if (!error.empty()) {
        throw std::invalid_argument("invalid L2 mock config: " + error);
    }
    IOManager* manager = new MockIOManager(config);
    return PtrFromReturn(manager);
}

bool IsMockIOManager(const IOManager* manager) {
    return dynamic_cast<const MockIOManager*>(manager) != nullptr;
}

Statistics GetStatistics(const IOManager* manager) {
    const MockIOManager* mock_manager =
        dynamic_cast<const MockIOManager*>(manager);
    return mock_manager == nullptr ? Statistics() : mock_manager->statistics();
}

} // namespace mock
} // namespace mdl
} // namespace datayes
