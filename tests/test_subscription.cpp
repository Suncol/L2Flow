#include "l2mock/l2_mock.h"

#include "mdl_cffexl2_msg.h"
#include "mdl_czcel2_msg.h"
#include "mdl_dcel2_msg.h"
#include "mdl_gfexl2_msg.h"
#include "mdl_shfel2_msg.h"
#include "mdl_shl2_msg.h"
#include "mdl_szl2_msg.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <tuple>

namespace mdl = datayes::mdl;
namespace mock = datayes::mdl::mock;
namespace sh = datayes::mdl::mdl_shl2_msg;
namespace sz = datayes::mdl::mdl_szl2_msg;
namespace cffex = datayes::mdl::mdl_cffexl2_msg;
namespace shfe = datayes::mdl::mdl_shfel2_msg;
namespace czce = datayes::mdl::mdl_czcel2_msg;
namespace dce = datayes::mdl::mdl_dcel2_msg;
namespace gfex = datayes::mdl::mdl_gfexl2_msg;

namespace {

typedef std::tuple<unsigned int, unsigned int, unsigned int> MessageKey;
typedef std::tuple<unsigned int, unsigned int, std::string> ObservationKey;

struct TestContext {
    void Expect(bool condition, const std::string& description) {
        if (condition) {
            return;
        }
        ++failures;
        std::cerr << "FAIL: " << description << '\n';
    }

    int failures = 0;
};

template <typename Predicate>
bool WaitFor(Predicate predicate, std::chrono::milliseconds timeout) {
    const std::chrono::steady_clock::time_point deadline =
        std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return predicate();
}

class RecordingHandler final : public mdl::MessageHandler {
public:
    template <typename Body>
    void Allow() {
        std::lock_guard<std::mutex> lock(mutex_);
        allowed_.insert(MessageKey(
            static_cast<unsigned int>(Body::ServiceID),
            static_cast<unsigned int>(Body::ServiceVer),
            static_cast<unsigned int>(Body::MessageID)));
    }

    uint64_t Count(uint8_t service_id,
                   uint16_t message_id,
                   const std::string& identifier) const {
        std::lock_guard<std::mutex> lock(mutex_);
        const ObservationKey key(
            static_cast<unsigned int>(service_id),
            static_cast<unsigned int>(message_id),
            identifier);
        const std::map<ObservationKey, uint64_t>::const_iterator found =
            observations_.find(key);
        return found == observations_.end() ? 0 : found->second;
    }

    uint64_t CountMessage(uint8_t service_id, uint16_t message_id) const {
        std::lock_guard<std::mutex> lock(mutex_);
        uint64_t result = 0;
        for (std::map<ObservationKey, uint64_t>::const_iterator
                 item = observations_.begin();
             item != observations_.end();
             ++item) {
            if (std::get<0>(item->first) == service_id &&
                std::get<1>(item->first) == message_id) {
                result += item->second;
            }
        }
        return result;
    }

    uint64_t RoutedTo(uint8_t service_id) const {
        std::lock_guard<std::mutex> lock(mutex_);
        const std::map<unsigned int, uint64_t>::const_iterator found =
            routed_to_.find(static_cast<unsigned int>(service_id));
        return found == routed_to_.end() ? 0 : found->second;
    }

    uint64_t total() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return total_;
    }

    uint64_t routing_errors() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return routing_errors_;
    }

    uint64_t unexpected_messages() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return unexpected_messages_;
    }

    uint64_t malformed_messages() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return malformed_messages_;
    }

    void OnMDLSHL2Message(const mdl::MDLMessage* message) override {
        Record(mdl::MDLSID_MDL_SHL2, message);
    }

    void OnMDLSZL2Message(const mdl::MDLMessage* message) override {
        Record(mdl::MDLSID_MDL_SZL2, message);
    }

    void OnMDLCFFEXL2Message(const mdl::MDLMessage* message) override {
        Record(mdl::MDLSID_MDL_CFFEXL2, message);
    }

    void OnMDLSHFEL2Message(const mdl::MDLMessage* message) override {
        Record(mdl::MDLSID_MDL_SHFEL2, message);
    }

    void OnMDLCZCEL2Message(const mdl::MDLMessage* message) override {
        Record(mdl::MDLSID_MDL_CZCEL2, message);
    }

    void OnMDLDCEL2Message(const mdl::MDLMessage* message) override {
        Record(mdl::MDLSID_MDL_DCEL2, message);
    }

    void OnMDLGFEXL2Message(const mdl::MDLMessage* message) override {
        Record(mdl::MDLSID_MDL_GFEXL2, message);
    }

private:
    template <typename Body>
    static bool ReadIdentifier(
        const mdl::MDLMessage* message,
        mdl::MDLAnsiString Body::* member,
        std::string* identifier) {
        if (message == NULL || message->GetHead() == NULL ||
            message->GetBody() == NULL) {
            return false;
        }
        const mdl::MDLMessageHead* const head = message->GetHead();
        if (head->MessageSize < head->HeadSize ||
            static_cast<std::size_t>(head->MessageSize - head->HeadSize) <
                sizeof(Body)) {
            return false;
        }
        const Body* const body =
            reinterpret_cast<const Body*>(message->GetBody());
        *identifier = (body->*member).std_str();
        return !identifier->empty();
    }

    static bool ExtractIdentifier(const mdl::MDLMessage* message,
                                  std::string* identifier) {
        if (message == NULL || message->GetHead() == NULL) {
            return false;
        }
        const mdl::MDLMessageHead* const head = message->GetHead();
        switch (head->ServiceID) {
        case mdl::MDLSID_MDL_SHL2:
            switch (head->MessageID) {
            case sh::SHL2MarketData::MessageID:
                return ReadIdentifier<sh::SHL2MarketData>(
                    message, &sh::SHL2MarketData::SecurityID, identifier);
            case sh::Order::MessageID:
                return ReadIdentifier<sh::Order>(
                    message, &sh::Order::SecurityID, identifier);
            default:
                return false;
            }
        case mdl::MDLSID_MDL_SZL2:
            if (head->MessageID == sz::MarketData::MessageID) {
                return ReadIdentifier<sz::MarketData>(
                    message, &sz::MarketData::SecurityID, identifier);
            }
            return false;
        case mdl::MDLSID_MDL_CFFEXL2:
            if (head->MessageID == cffex::Future::MessageID) {
                return ReadIdentifier<cffex::Future>(
                    message, &cffex::Future::InstruID, identifier);
            }
            return false;
        case mdl::MDLSID_MDL_SHFEL2:
            if (head->MessageID == shfe::CTPFuture::MessageID) {
                return ReadIdentifier<shfe::CTPFuture>(
                    message, &shfe::CTPFuture::InstruID, identifier);
            }
            return false;
        case mdl::MDLSID_MDL_CZCEL2:
            if (head->MessageID == czce::CTPFuture::MessageID) {
                return ReadIdentifier<czce::CTPFuture>(
                    message, &czce::CTPFuture::InstruID, identifier);
            }
            return false;
        case mdl::MDLSID_MDL_DCEL2:
            if (head->MessageID == dce::Future::MessageID) {
                return ReadIdentifier<dce::Future>(
                    message, &dce::Future::InstruID, identifier);
            }
            return false;
        case mdl::MDLSID_MDL_GFEXL2:
            if (head->MessageID == gfex::Future::MessageID) {
                return ReadIdentifier<gfex::Future>(
                    message, &gfex::Future::InstruID, identifier);
            }
            return false;
        default:
            return false;
        }
    }

    void Record(uint8_t expected_service_id,
                const mdl::MDLMessage* message) {
        bool routing_error = false;
        bool unexpected = false;
        bool malformed = false;
        unsigned int service_id = 0;
        unsigned int service_version = 0;
        unsigned int message_id = 0;
        std::string identifier("<malformed>");

        if (message == NULL || message->GetHead() == NULL) {
            malformed = true;
        } else {
            const mdl::MDLMessageHead* const head = message->GetHead();
            service_id = head->ServiceID;
            service_version = head->ServiceVersion;
            message_id = head->MessageID;
            routing_error = service_id != expected_service_id;
            malformed = !ExtractIdentifier(message, &identifier);
        }

        std::lock_guard<std::mutex> lock(mutex_);
        ++total_;
        ++routed_to_[static_cast<unsigned int>(expected_service_id)];
        if (routing_error) {
            ++routing_errors_;
        }
        if (allowed_.find(
                MessageKey(service_id, service_version, message_id)) ==
            allowed_.end()) {
            unexpected = true;
        }
        if (unexpected) {
            ++unexpected_messages_;
        }
        if (malformed) {
            ++malformed_messages_;
        }
        ++observations_[ObservationKey(service_id, message_id, identifier)];
    }

    mutable std::mutex mutex_;
    std::set<MessageKey> allowed_;
    std::map<ObservationKey, uint64_t> observations_;
    std::map<unsigned int, uint64_t> routed_to_;
    uint64_t total_ = 0;
    uint64_t routing_errors_ = 0;
    uint64_t unexpected_messages_ = 0;
    uint64_t malformed_messages_ = 0;
};

mock::Instrument Instrument(uint8_t service_id,
                            const std::string& identifier) {
    mock::Instrument instrument;
    instrument.service_id = service_id;
    instrument.security_id = identifier;
    instrument.reference_price_milli =
        service_id == mdl::MDLSID_MDL_SHL2 ||
                service_id == mdl::MDLSID_MDL_SZL2
            ? 100000
            : 1000000;
    instrument.tick_size_milli =
        service_id == mdl::MDLSID_MDL_SHL2 ||
                service_id == mdl::MDLSID_MDL_SZL2
            ? 10
            : 100;
    instrument.lot_size =
        service_id == mdl::MDLSID_MDL_SHL2 ||
                service_id == mdl::MDLSID_MDL_SZL2
            ? 100
            : 1;
    instrument.option = false;
    return instrument;
}

void WaitForInFlightCallback() {
    // Callbacks in this test are synchronous. A subscription mutation can race
    // only with the one target set already selected by the generator.
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
}

} // namespace

int main() {
    TestContext test;

    mock::Config config;
    config.seed = 0x5355425343524942ULL;
    config.messages_per_second = 20000;
    config.book_depth = 3;
    config.orders_per_level = 2;
    config.callback_threads = 1;
    config.callback_queue_capacity = 64;
    config.backpressure = mock::BackpressurePolicy::Block;
    config.clock_mode = mock::ClockMode::SimulatedTradingDay;
    config.simulated_min_step_ms = 1;
    config.simulated_max_step_ms = 1;
    config.instruments.push_back(
        Instrument(mdl::MDLSID_MDL_SHL2, "600001"));
    config.instruments.push_back(
        Instrument(mdl::MDLSID_MDL_SHL2, "601001"));
    config.instruments.push_back(
        Instrument(mdl::MDLSID_MDL_SZL2, "000001"));
    config.instruments.push_back(
        Instrument(mdl::MDLSID_MDL_CFFEXL2, "IFMOCK1"));
    config.instruments.push_back(
        Instrument(mdl::MDLSID_MDL_CFFEXL2, "IHMOCK1"));
    config.instruments.push_back(
        Instrument(mdl::MDLSID_MDL_SHFEL2, "SHFUT1"));
    config.instruments.push_back(
        Instrument(mdl::MDLSID_MDL_CZCEL2, "CZFUT1"));
    config.instruments.push_back(
        Instrument(mdl::MDLSID_MDL_DCEL2, "DCFUT1"));
    config.instruments.push_back(
        Instrument(mdl::MDLSID_MDL_GFEXL2, "GFFUT1"));

    const std::string validation_error = mock::ValidateConfig(config);
    test.Expect(validation_error.empty(),
                "custom subscription-test config is valid: " +
                    validation_error);
    if (!validation_error.empty()) {
        return 1;
    }

    RecordingHandler first_handler;
    first_handler.Allow<sh::SHL2MarketData>();
    first_handler.Allow<sz::MarketData>();
    first_handler.Allow<cffex::Future>();
    first_handler.Allow<shfe::CTPFuture>();
    first_handler.Allow<czce::CTPFuture>();
    first_handler.Allow<dce::Future>();
    first_handler.Allow<gfex::Future>();

    RecordingHandler second_handler;
    second_handler.Allow<sh::Order>();

    mdl::IOManagerPtr manager;
    try {
        manager = mock::CreateIOManager(config);
    } catch (const std::exception& error) {
        std::cerr << "FAIL: CreateIOManager threw: " << error.what() << '\n';
        return 1;
    }
    test.Expect(!manager.IsNull(), "mock IOManager is created");
    test.Expect(mock::IsMockIOManager(manager.Get()),
                "factory returns a mock IOManager");
    if (manager.IsNull()) {
        return 1;
    }

    mdl::SubscriberPtr first =
        manager->CreateSubscriber(&first_handler, false);
    mdl::SubscriberPtr second =
        manager->CreateSubscriber(&second_handler, false);
    test.Expect(!first.IsNull(), "first subscriber is created");
    test.Expect(!second.IsNull(), "second subscriber is created");
    if (first.IsNull() || second.IsNull()) {
        manager->Shutdown();
        return 1;
    }

    const char* shanghai_patterns[] = {"60000?", "601*"};
    first->SubcribeMessageByFieldValues<sh::SHL2MarketData>(
        "SecurityID", shanghai_patterns, 2);
    const char* cffex_patterns[] = {"IF*"};
    first->SubcribeMessageByFieldValues<cffex::Future>(
        "InstruID", cffex_patterns, 1);
    first->SubcribeMessage<sz::MarketData>();
    first->SubcribeMessage<shfe::CTPFuture>();
    first->SubcribeMessage<czce::CTPFuture>();
    first->SubcribeMessage<dce::Future>();
    first->SubcribeMessage<gfex::Future>();

    // This subscriber intentionally has one exact message subscription and no
    // field filter. Any other key delivered to it is a subscription failure.
    second->SubcribeMessage<sh::Order>();

    const std::string first_connect(first->Connect());
    test.Expect(first_connect.empty(), "first Connect succeeds");
    test.Expect(mock::GetStatistics(manager.Get()).active_subscribers == 1,
                "one connected subscriber is active");
    const std::string first_connect_again(first->Connect());
    test.Expect(first_connect_again.empty(),
                "repeated first Connect is idempotent");
    test.Expect(mock::GetStatistics(manager.Get()).active_subscribers == 1,
                "repeated Connect does not double-count first subscriber");

    const std::string second_connect(second->Connect());
    test.Expect(second_connect.empty(), "second Connect succeeds");
    test.Expect(mock::GetStatistics(manager.Get()).active_subscribers == 2,
                "two connected subscribers are active");
    const std::string second_connect_again(second->Connect());
    test.Expect(second_connect_again.empty(),
                "repeated second Connect is idempotent");
    test.Expect(mock::GetStatistics(manager.Get()).active_subscribers == 2,
                "repeated Connect does not double-count second subscriber");

    const bool initial_delivery = WaitFor(
        [&first_handler, &second_handler] {
            return
                first_handler.Count(
                    mdl::MDLSID_MDL_SHL2,
                    sh::SHL2MarketData::MessageID,
                    "600001") >= 3 &&
                first_handler.Count(
                    mdl::MDLSID_MDL_SHL2,
                    sh::SHL2MarketData::MessageID,
                    "601001") >= 3 &&
                first_handler.Count(
                    mdl::MDLSID_MDL_SZL2,
                    sz::MarketData::MessageID,
                    "000001") >= 3 &&
                first_handler.Count(
                    mdl::MDLSID_MDL_CFFEXL2,
                    cffex::Future::MessageID,
                    "IFMOCK1") >= 3 &&
                first_handler.Count(
                    mdl::MDLSID_MDL_SHFEL2,
                    shfe::CTPFuture::MessageID,
                    "SHFUT1") >= 3 &&
                first_handler.Count(
                    mdl::MDLSID_MDL_CZCEL2,
                    czce::CTPFuture::MessageID,
                    "CZFUT1") >= 3 &&
                first_handler.Count(
                    mdl::MDLSID_MDL_DCEL2,
                    dce::Future::MessageID,
                    "DCFUT1") >= 3 &&
                first_handler.Count(
                    mdl::MDLSID_MDL_GFEXL2,
                    gfex::Future::MessageID,
                    "GFFUT1") >= 3 &&
                second_handler.CountMessage(
                    mdl::MDLSID_MDL_SHL2,
                    sh::Order::MessageID) >= 6;
        },
        std::chrono::seconds(3));
    test.Expect(initial_delivery,
                "both subscribers receive every initially selected stream");

    test.Expect(
        first_handler.Count(
            mdl::MDLSID_MDL_CFFEXL2,
            cffex::Future::MessageID,
            "IHMOCK1") == 0,
        "InstruID IF* wildcard excludes IHMOCK1");
    test.Expect(first_handler.routing_errors() == 0,
                "MessageHandler routes every SID to its matching callback");
    test.Expect(first_handler.unexpected_messages() == 0,
                "first subscriber receives only exact subscribed message keys");
    test.Expect(second_handler.unexpected_messages() == 0,
                "second subscriber receives only the SH order key");
    test.Expect(first_handler.malformed_messages() == 0 &&
                    second_handler.malformed_messages() == 0,
                "subscribed messages expose the expected identifier fields");

    const uint8_t routed_services[] = {
        mdl::MDLSID_MDL_SHL2,
        mdl::MDLSID_MDL_SZL2,
        mdl::MDLSID_MDL_CFFEXL2,
        mdl::MDLSID_MDL_SHFEL2,
        mdl::MDLSID_MDL_CZCEL2,
        mdl::MDLSID_MDL_DCEL2,
        mdl::MDLSID_MDL_GFEXL2
    };
    for (std::size_t i = 0;
         i < sizeof(routed_services) / sizeof(routed_services[0]);
         ++i) {
        test.Expect(first_handler.RoutedTo(routed_services[i]) != 0,
                    "SID routing callback was exercised for service " +
                        std::to_string(routed_services[i]));
    }

    mock::Statistics statistics = mock::GetStatistics(manager.Get());
    test.Expect(statistics.generated != 0, "generated statistic advances");
    test.Expect(statistics.delivered != 0, "delivered statistic advances");
    test.Expect(statistics.filtered != 0,
                "unsubscribed generator streams increment filtered");
    test.Expect(statistics.active_subscribers == 2,
                "statistics retain two active subscribers");

    // Remove one of two SecurityID wildcard patterns. The key remains
    // subscribed through the other pattern.
    const char* remove_601[] = {"601*"};
    first->DelSubscriptionByFieldValues(
        sh::SHL2MarketData::ServiceID,
        sh::SHL2MarketData::ServiceVer,
        sh::SHL2MarketData::MessageID,
        "SecurityID",
        remove_601,
        1);
    first->ReSubscribe();
    WaitForInFlightCallback();
    const uint64_t stopped_601 = first_handler.Count(
        mdl::MDLSID_MDL_SHL2,
        sh::SHL2MarketData::MessageID,
        "601001");
    const uint64_t continuing_600 = first_handler.Count(
        mdl::MDLSID_MDL_SHL2,
        sh::SHL2MarketData::MessageID,
        "600001");
    const bool remaining_pattern_delivers = WaitFor(
        [&first_handler, continuing_600] {
            return first_handler.Count(
                       mdl::MDLSID_MDL_SHL2,
                       sh::SHL2MarketData::MessageID,
                       "600001") >=
                   continuing_600 + 3;
        },
        std::chrono::seconds(2));
    test.Expect(remaining_pattern_delivers,
                "remaining SecurityID wildcard continues after partial delete");
    test.Expect(
        first_handler.Count(
            mdl::MDLSID_MDL_SHL2,
            sh::SHL2MarketData::MessageID,
            "601001") == stopped_601,
        "DelSubscriptionByFieldValues stops only the removed wildcard");

    // Removing the last pattern removes the complete message rule.
    const char* remove_600[] = {"60000?"};
    first->DelSubscriptionByFieldValues(
        sh::SHL2MarketData::ServiceID,
        sh::SHL2MarketData::ServiceVer,
        sh::SHL2MarketData::MessageID,
        "SecurityID",
        remove_600,
        1);
    first->ReSubscribe();
    WaitForInFlightCallback();
    const uint64_t stopped_shanghai = first_handler.CountMessage(
        mdl::MDLSID_MDL_SHL2,
        sh::SHL2MarketData::MessageID);
    const uint64_t shenzhen_before = first_handler.CountMessage(
        mdl::MDLSID_MDL_SZL2,
        sz::MarketData::MessageID);
    test.Expect(
        WaitFor(
            [&first_handler, shenzhen_before] {
                return first_handler.CountMessage(
                           mdl::MDLSID_MDL_SZL2,
                           sz::MarketData::MessageID) >=
                       shenzhen_before + 3;
            },
            std::chrono::seconds(2)),
        "other subscriptions continue after the last SH pattern is deleted");
    test.Expect(
        first_handler.CountMessage(
            mdl::MDLSID_MDL_SHL2,
            sh::SHL2MarketData::MessageID) == stopped_shanghai,
        "removing the last field pattern removes the message subscription");

    // Delete a complete field-filtered subscription by exact SID/version/MID.
    first->DelSubscription(
        cffex::Future::ServiceID,
        cffex::Future::ServiceVer,
        cffex::Future::MessageID);
    first->ReSubscribe();
    WaitForInFlightCallback();
    const uint64_t stopped_if = first_handler.Count(
        mdl::MDLSID_MDL_CFFEXL2,
        cffex::Future::MessageID,
        "IFMOCK1");
    const uint64_t dce_before = first_handler.CountMessage(
        mdl::MDLSID_MDL_DCEL2,
        dce::Future::MessageID);
    test.Expect(
        WaitFor(
            [&first_handler, dce_before] {
                return first_handler.CountMessage(
                           mdl::MDLSID_MDL_DCEL2,
                           dce::Future::MessageID) >=
                       dce_before + 3;
            },
            std::chrono::seconds(2)),
        "other subscriptions continue after DelSubscription");
    test.Expect(
        first_handler.Count(
            mdl::MDLSID_MDL_CFFEXL2,
            cffex::Future::MessageID,
            "IFMOCK1") == stopped_if,
        "DelSubscription stops the exact CFFEX message key");

    // Clear only the first subscriber. The independent second subscriber must
    // keep receiving its exact SH order stream.
    first->ClearSubscriptions();
    first->ReSubscribe();
    WaitForInFlightCallback();
    const uint64_t first_stopped = first_handler.total();
    const uint64_t second_continuing = second_handler.total();
    test.Expect(
        WaitFor(
            [&second_handler, second_continuing] {
                return second_handler.total() >= second_continuing + 6;
            },
            std::chrono::seconds(2)),
        "second subscriber continues after first ClearSubscriptions");
    test.Expect(first_handler.total() == first_stopped,
                "ClearSubscriptions stops callbacks to only that subscriber");

    // Add a subscription back and call ReSubscribe to exercise the complete
    // dynamic subscription cycle.
    const uint64_t shenzhen_stopped = first_handler.CountMessage(
        mdl::MDLSID_MDL_SZL2,
        sz::MarketData::MessageID);
    first->SubcribeMessage<sz::MarketData>();
    first->ReSubscribe();
    test.Expect(
        WaitFor(
            [&first_handler, shenzhen_stopped] {
                return first_handler.CountMessage(
                           mdl::MDLSID_MDL_SZL2,
                           sz::MarketData::MessageID) >=
                       shenzhen_stopped + 3;
            },
            std::chrono::seconds(2)),
        "subscription added after clear resumes through ReSubscribe");

    manager->Shutdown();
    manager->Shutdown();

    const uint64_t callbacks_after_shutdown =
        first_handler.total() + second_handler.total();
    const mock::Statistics final_statistics =
        mock::GetStatistics(manager.Get());
    test.Expect(final_statistics.active_subscribers == 0,
                "Shutdown clears active_subscribers");
    test.Expect(final_statistics.generated != 0,
                "final generated statistic is nonzero");
    test.Expect(final_statistics.delivered == callbacks_after_shutdown,
                "delivered statistic equals callbacks observed by both handlers");
    test.Expect(final_statistics.filtered != 0,
                "final filtered statistic is nonzero");
    test.Expect(final_statistics.dropped == 0,
                "synchronous subscribers do not drop queue entries");
    test.Expect(final_statistics.callback_errors == 0,
                "callbacks complete without runtime errors");

    const uint64_t stopped_total = callbacks_after_shutdown;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    test.Expect(first_handler.total() + second_handler.total() == stopped_total,
                "no callback is delivered after Shutdown returns");

    const std::string reconnect_after_shutdown(first->Connect());
    const std::string reconnect_after_shutdown_again(first->Connect());
    test.Expect(!reconnect_after_shutdown.empty(),
                "Connect after Shutdown returns an error");
    test.Expect(!reconnect_after_shutdown_again.empty(),
                "repeated Connect after Shutdown remains an error");
    test.Expect(mock::GetStatistics(manager.Get()).active_subscribers == 0,
                "failed reconnects do not reactivate subscribers");
    test.Expect(first_handler.total() + second_handler.total() == stopped_total,
                "failed reconnects do not restart callbacks");

    second.Reset();
    first.Reset();
    manager.Reset();

    if (test.failures != 0) {
        std::cerr << "subscription test failed with " << test.failures
                  << " error(s)\n";
        return 1;
    }

    std::cout
        << "subscription, wildcard, lifecycle, statistics, and SID routing "
           "checks passed\n";
    return 0;
}
