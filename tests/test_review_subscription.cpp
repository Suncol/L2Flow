#include "l2mock/l2_mock.h"

#include "mdl_cffexl2_msg.h"
#include "mdl_czcel2_msg.h"
#include "mdl_dcel2_msg.h"
#include "mdl_gfexl2_msg.h"
#include "mdl_shfel2_msg.h"
#include "mdl_shl2_msg.h"
#include "mdl_szl2_msg.h"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <vector>

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

struct Key {
    std::uint8_t service_id;
    std::uint16_t service_version;
    std::uint16_t message_id;

    bool operator<(const Key& other) const {
        if (service_id != other.service_id) {
            return service_id < other.service_id;
        }
        if (service_version != other.service_version) {
            return service_version < other.service_version;
        }
        return message_id < other.message_id;
    }
};

Key MakeKey(const mdl::MDLMessageHead& head) {
    return Key{head.ServiceID, head.ServiceVersion, head.MessageID};
}

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

mock::Instrument Instrument(std::uint8_t service_id,
                            const std::string& identifier,
                            bool option = false) {
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
            : 5;
    instrument.option = option;
    return instrument;
}

mock::Config FullCoreConfig(bool capture_source) {
    mock::Config config;
    config.seed = capture_source ? 0x5245564945575f31ULL
                                 : 0x5245564945575f32ULL;
    config.messages_per_second = capture_source ? 0 : 1;
    config.book_depth = 2;
    config.orders_per_level = 2;
    config.callback_threads = 2;
    config.callback_queue_capacity = 256;
    config.backpressure = mock::BackpressurePolicy::Block;
    config.clock_mode = mock::ClockMode::SimulatedTradingDay;
    config.simulated_min_step_ms = 1;
    config.simulated_max_step_ms = 1;

    const std::string suffix = capture_source ? "_MATCH" : "_DUMMY";
    config.instruments.push_back(
        Instrument(mdl::MDLSID_MDL_SHL2, "SH" + suffix));
    config.instruments.push_back(
        Instrument(mdl::MDLSID_MDL_SZL2, "SZ" + suffix));
    config.instruments.push_back(
        Instrument(mdl::MDLSID_MDL_CFFEXL2, "CFF_F" + suffix));
    config.instruments.push_back(
        Instrument(mdl::MDLSID_MDL_CFFEXL2, "CFF_O" + suffix, true));
    config.instruments.push_back(
        Instrument(mdl::MDLSID_MDL_SHFEL2, "SHF_F" + suffix));
    config.instruments.push_back(
        Instrument(mdl::MDLSID_MDL_SHFEL2, "SHF_O" + suffix, true));
    config.instruments.push_back(
        Instrument(mdl::MDLSID_MDL_CZCEL2, "CZE_F" + suffix));
    config.instruments.push_back(
        Instrument(mdl::MDLSID_MDL_CZCEL2, "CZE_O" + suffix, true));
    config.instruments.push_back(
        Instrument(mdl::MDLSID_MDL_DCEL2, "DCE_F" + suffix));
    config.instruments.push_back(
        Instrument(mdl::MDLSID_MDL_DCEL2, "DCE_O" + suffix, true));
    config.instruments.push_back(
        Instrument(mdl::MDLSID_MDL_GFEXL2, "GFE_F" + suffix));
    config.instruments.push_back(
        Instrument(mdl::MDLSID_MDL_GFEXL2, "GFE_O" + suffix, true));
    return config;
}

class CaptureHandler final : public mdl::MessageHandlerBase {
public:
    void OnMessage(mdl::Subscriber*,
                   const mdl::MDLMessage* message) override {
        if (message == nullptr || message->GetHead() == nullptr) {
            return;
        }
        const Key key = MakeKey(*message->GetHead());
        std::lock_guard<std::mutex> lock(mutex_);
        if (samples_.find(key) == samples_.end()) {
            samples_[key] = message->Copy();
            sample_added_.notify_all();
        }
    }

    bool WaitFor(std::size_t count) {
        std::unique_lock<std::mutex> lock(mutex_);
        return sample_added_.wait_for(
            lock,
            std::chrono::seconds(5),
            [this, count] { return samples_.size() >= count; });
    }

    std::map<Key, mdl::MDLMessagePtr> Samples() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return samples_;
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable sample_added_;
    std::map<Key, mdl::MDLMessagePtr> samples_;
};

class CountingHandler final : public mdl::MessageHandlerBase {
public:
    void OnMessage(mdl::Subscriber*,
                   const mdl::MDLMessage* message) override {
        if (message == nullptr || message->GetHead() == nullptr) {
            return;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        ++counts_[MakeKey(*message->GetHead())];
        ++total_;
        callback_arrived_.notify_all();
    }

    bool WaitForTotal(std::uint64_t count) {
        std::unique_lock<std::mutex> lock(mutex_);
        return callback_arrived_.wait_for(
            lock,
            std::chrono::seconds(5),
            [this, count] { return total_ >= count; });
    }

    std::uint64_t Count(const Key& key) const {
        std::lock_guard<std::mutex> lock(mutex_);
        const std::map<Key, std::uint64_t>::const_iterator found =
            counts_.find(key);
        return found == counts_.end() ? 0 : found->second;
    }

    std::uint64_t total() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return total_;
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable callback_arrived_;
    std::map<Key, std::uint64_t> counts_;
    std::uint64_t total_ = 0;
};

bool IsOptionKey(const Key& key) {
    switch (key.service_id) {
    case mdl::MDLSID_MDL_CFFEXL2:
        return key.message_id == cffex::Option::MessageID;
    case mdl::MDLSID_MDL_SHFEL2:
        return key.message_id == shfe::CTPOption::MessageID ||
               key.message_id == shfe::CrudeOption::MessageID;
    case mdl::MDLSID_MDL_CZCEL2:
        return key.message_id == czce::CTPOption::MessageID;
    case mdl::MDLSID_MDL_DCEL2:
        return key.message_id == dce::Option::MessageID ||
               key.message_id == dce::OptionOrder::MessageID;
    case mdl::MDLSID_MDL_GFEXL2:
        return key.message_id == gfex::Option::MessageID ||
               key.message_id == gfex::OptionOrder::MessageID;
    default:
        return false;
    }
}

std::string IdentifierFor(const Key& key) {
    switch (key.service_id) {
    case mdl::MDLSID_MDL_SHL2:
        return "SH_MATCH";
    case mdl::MDLSID_MDL_SZL2:
        return "SZ_MATCH";
    case mdl::MDLSID_MDL_CFFEXL2:
        return IsOptionKey(key) ? "CFF_O_MATCH" : "CFF_F_MATCH";
    case mdl::MDLSID_MDL_SHFEL2:
        return IsOptionKey(key) ? "SHF_O_MATCH" : "SHF_F_MATCH";
    case mdl::MDLSID_MDL_CZCEL2:
        return IsOptionKey(key) ? "CZE_O_MATCH" : "CZE_F_MATCH";
    case mdl::MDLSID_MDL_DCEL2:
        return IsOptionKey(key) ? "DCE_O_MATCH" : "DCE_F_MATCH";
    case mdl::MDLSID_MDL_GFEXL2:
        return IsOptionKey(key) ? "GFE_O_MATCH" : "GFE_F_MATCH";
    default:
        return std::string();
    }
}

const char* IdentifierField(const Key& key) {
    return key.service_id == mdl::MDLSID_MDL_SHL2 ||
                   key.service_id == mdl::MDLSID_MDL_SZL2
               ? "SecurityID"
               : "InstruID";
}

const char* WrongIdentifierField(const Key& key) {
    return key.service_id == mdl::MDLSID_MDL_SHL2 ||
                   key.service_id == mdl::MDLSID_MDL_SZL2
               ? "InstruID"
               : "SecurityID";
}

std::map<Key, mdl::MDLMessagePtr> CaptureEverySupportedMessage(
    TestContext* test) {
    const mock::Config config = FullCoreConfig(true);
    const std::string validation_error = mock::ValidateConfig(config);
    test->Expect(validation_error.empty(),
                 "capture config is valid: " + validation_error);
    if (!validation_error.empty()) {
        return std::map<Key, mdl::MDLMessagePtr>();
    }

    CaptureHandler handler;
    mdl::IOManagerPtr manager = mock::CreateIOManager(config);
    mdl::SubscriberPtr subscriber =
        manager->CreateSubscriber(&handler, false);
    test->Expect(!subscriber.IsNull(), "capture subscriber is created");
    if (subscriber.IsNull()) {
        manager->Shutdown();
        return std::map<Key, mdl::MDLMessagePtr>();
    }
    mock::SubscribeAll(subscriber.Get());
    const char* const connect_result = subscriber->Connect();
    test->Expect(connect_result != nullptr && connect_result[0] == '\0',
                 "capture subscriber connects");
    if (connect_result == nullptr || connect_result[0] != '\0') {
        manager->Shutdown();
        return std::map<Key, mdl::MDLMessagePtr>();
    }

    const std::size_t expected = mock::SupportedMessages().size();
    test->Expect(handler.WaitFor(expected),
                 "one native packed sample is captured for every supported key");
    manager->Shutdown();
    const std::map<Key, mdl::MDLMessagePtr> samples = handler.Samples();
    test->Expect(samples.size() == expected,
                 "captured sample set has exactly every supported key");
    return samples;
}

void CheckSubscriptionJson(TestContext* test,
                           mdl::IOManager* manager) {
    CountingHandler handler;
    mdl::SubscriberPtr subscriber =
        manager->CreateSubscriber(&handler, false);
    test->Expect(!subscriber.IsNull(), "JSON probe subscriber is created");
    if (subscriber.IsNull()) {
        return;
    }

    test->Expect(std::string(subscriber->GetSubscription()) == "null\n",
                 "empty GetSubscription matches SDK JsonCpp null schema");

    subscriber->SubcribeMessage<sh::Order>();
    test->Expect(
        std::string(subscriber->GetSubscription()) ==
            "[{\"mid\":19,\"sid\":4}]\n",
        "all-values GetSubscription uses exact SDK sid/mid schema");

    subscriber->ClearSubscriptions();
    const char* field_values[] = {
        "IF2408", "quote\"slash\\line\nutf8-中"};
    subscriber->AddSubscriptionByFieldValues(
        cffex::Future::ServiceID,
        cffex::Future::ServiceVer,
        cffex::Future::MessageID,
        "Instru\"ID\\x",
        field_values,
        2);
    test->Expect(
        std::string(subscriber->GetSubscription()) ==
            "[{\"fieldname\":\"Instru\\\"ID\\\\x\","
            "\"fieldvalues\":[\"IF2408\","
            "\"quote\\\"slash\\\\line\\nutf8-中\"],"
            "\"mid\":1,\"sid\":21}]\n",
        "field-filter GetSubscription preserves the exact SDK schema and "
        "JSON escaping");

    subscriber->ClearSubscriptions();
    subscriber->SubcribeMessage<cffex::Future>();
    subscriber->SubcribeMessage<sz::MarketData>();
    subscriber->SubcribeMessage<sh::Order>();
    test->Expect(
        std::string(subscriber->GetSubscription()) ==
            "[{\"mid\":19,\"sid\":4},{\"mid\":4,\"sid\":6},"
            "{\"mid\":1,\"sid\":21}]\n",
        "GetSubscription sorts entries by SDK sid/mid order");

    subscriber->ClearSubscriptions();
    const char* first_values[] = {"B", "A"};
    const char* second_values[] = {"2", "1"};
    const char* repeated_values[] = {"C", "A", "C"};
    subscriber->AddSubscriptionByFieldValues(
        cffex::Future::ServiceID,
        cffex::Future::ServiceVer,
        cffex::Future::MessageID,
        "FirstField",
        first_values,
        2);
    subscriber->AddSubscriptionByFieldValues(
        cffex::Future::ServiceID,
        cffex::Future::ServiceVer,
        cffex::Future::MessageID,
        "SecondField",
        second_values,
        2);
    subscriber->AddSubscriptionByFieldValues(
        cffex::Future::ServiceID,
        cffex::Future::ServiceVer,
        cffex::Future::MessageID,
        "FirstField",
        repeated_values,
        3);
    test->Expect(
        std::string(subscriber->GetSubscription()) ==
            "[{\"fieldname\":\"FirstField\","
            "\"fieldvalues\":[\"A\",\"B\",\"C\"],"
            "\"mid\":1,\"sid\":21}]\n",
        "SDK retains the first field, unions values, deduplicates, and sorts");

    subscriber->AddSubscription(
        cffex::Future::ServiceID,
        cffex::Future::ServiceVer,
        cffex::Future::MessageID);
    test->Expect(
        std::string(subscriber->GetSubscription()) ==
            "[{\"fieldname\":\"FirstField\","
            "\"fieldvalues\":[\"A\",\"B\",\"C\"],"
            "\"mid\":1,\"sid\":21}]\n",
        "all-values AddSubscription does not override an existing filter");

    subscriber->ClearSubscriptions();
    subscriber->SubcribeMessage<cffex::Future>();
    const char* replacement[] = {"IF*"};
    subscriber->AddSubscriptionByFieldValues(
        cffex::Future::ServiceID,
        cffex::Future::ServiceVer,
        cffex::Future::MessageID,
        "InstruID",
        replacement,
        1);
    test->Expect(
        std::string(subscriber->GetSubscription()) ==
            "[{\"fieldname\":\"InstruID\","
            "\"fieldvalues\":[\"IF*\"],\"mid\":1,\"sid\":21}]\n",
        "a nonempty field filter replaces an all-values rule");

    subscriber->ClearSubscriptions();
    subscriber->AddSubscriptionByFieldValues(
        cffex::Future::ServiceID,
        cffex::Future::ServiceVer,
        cffex::Future::MessageID,
        "InstruID",
        nullptr,
        0);
    test->Expect(
        std::string(subscriber->GetSubscription()) ==
            "[{\"mid\":1,\"sid\":21}]\n",
        "an empty field-value add creates the SDK all-values rule");

    subscriber->ClearSubscriptions();
    const char* delete_source[] = {"C", "A", "B"};
    subscriber->AddSubscriptionByFieldValues(
        cffex::Future::ServiceID,
        cffex::Future::ServiceVer,
        cffex::Future::MessageID,
        "InstruID",
        delete_source,
        3);
    const char* delete_b[] = {"B"};
    subscriber->DelSubscriptionByFieldValues(
        cffex::Future::ServiceID,
        cffex::Future::ServiceVer,
        cffex::Future::MessageID,
        "WrongField",
        delete_b,
        1);
    subscriber->DelSubscriptionByFieldValues(
        cffex::Future::ServiceID,
        cffex::Future::ServiceVer,
        cffex::Future::MessageID,
        "InstruID",
        delete_b,
        1);
    subscriber->DelSubscriptionByFieldValues(
        cffex::Future::ServiceID,
        cffex::Future::ServiceVer,
        cffex::Future::MessageID,
        "InstruID",
        nullptr,
        0);
    test->Expect(
        std::string(subscriber->GetSubscription()) ==
            "[{\"fieldname\":\"InstruID\","
            "\"fieldvalues\":[\"A\",\"C\"],\"mid\":1,\"sid\":21}]\n",
        "field deletion ignores wrong/empty requests and removes exact values");

    const char* delete_remaining[] = {"A", "C"};
    subscriber->DelSubscriptionByFieldValues(
        cffex::Future::ServiceID,
        cffex::Future::ServiceVer,
        cffex::Future::MessageID,
        "InstruID",
        delete_remaining,
        2);
    test->Expect(
        std::string(subscriber->GetSubscription()) == "null\n",
        "deleting the last field values removes the message rule");
}

void CheckExplicitPublicationRouting(
    TestContext* test,
    const std::map<Key, mdl::MDLMessagePtr>& samples) {
    const mock::Config config = FullCoreConfig(false);
    const std::string validation_error = mock::ValidateConfig(config);
    test->Expect(validation_error.empty(),
                 "playback config is valid: " + validation_error);
    if (!validation_error.empty()) {
        return;
    }

    CountingHandler matching_handler;
    CountingHandler rejecting_handler;
    CountingHandler wrong_field_handler;
    mdl::IOManagerPtr manager = mock::CreateIOManager(config);
    CheckSubscriptionJson(test, manager.Get());

    mdl::SubscriberPtr matching =
        manager->CreateSubscriber(&matching_handler, true);
    mdl::SubscriberPtr rejecting =
        manager->CreateSubscriber(&rejecting_handler, true);
    mdl::SubscriberPtr wrong_field =
        manager->CreateSubscriber(&wrong_field_handler, true);
    test->Expect(
        !matching.IsNull() && !rejecting.IsNull() &&
            !wrong_field.IsNull(),
        "matching, rejecting, and wrong-field subscribers are created");
    if (matching.IsNull() || rejecting.IsNull() ||
        wrong_field.IsNull()) {
        manager->Shutdown();
        return;
    }

    for (std::map<Key, mdl::MDLMessagePtr>::const_iterator sample =
             samples.begin();
         sample != samples.end();
         ++sample) {
        const std::string identifier = IdentifierFor(sample->first);
        const char* matching_value[] = {identifier.c_str()};
        const char* rejecting_value[] = {"NEVER_MATCH_THIS_IDENTIFIER"};
        matching->AddSubscriptionByFieldValues(
            sample->first.service_id,
            sample->first.service_version,
            sample->first.message_id,
            IdentifierField(sample->first),
            matching_value,
            1);
        rejecting->AddSubscriptionByFieldValues(
            sample->first.service_id,
            sample->first.service_version,
            sample->first.message_id,
            IdentifierField(sample->first),
            rejecting_value,
            1);
        wrong_field->AddSubscriptionByFieldValues(
            sample->first.service_id,
            sample->first.service_version,
            sample->first.message_id,
            WrongIdentifierField(sample->first),
            matching_value,
            1);
    }

    const char* const matching_connect = matching->Connect();
    const char* const rejecting_connect = rejecting->Connect();
    const char* const wrong_field_connect = wrong_field->Connect();
    test->Expect(
        matching_connect != nullptr && matching_connect[0] == '\0' &&
            rejecting_connect != nullptr && rejecting_connect[0] == '\0' &&
            wrong_field_connect != nullptr &&
            wrong_field_connect[0] == '\0',
        "playback subscribers connect");
    if (matching_connect == nullptr || matching_connect[0] != '\0' ||
        rejecting_connect == nullptr || rejecting_connect[0] != '\0' ||
        wrong_field_connect == nullptr ||
        wrong_field_connect[0] != '\0') {
        manager->Shutdown();
        return;
    }

    for (std::map<Key, mdl::MDLMessagePtr>::const_iterator sample =
             samples.begin();
         sample != samples.end();
         ++sample) {
        manager->SyncPublish(sample->second.Get());
    }
    test->Expect(
        matching_handler.total() == samples.size(),
        "SyncPublish matches the body SecurityID/InstruID for all 28 keys");
    test->Expect(
        rejecting_handler.total() == 0,
        "SyncPublish rejects nonmatching body identifiers for all 28 keys");
    test->Expect(
        wrong_field_handler.total() == 0,
        "SyncPublish rejects matching values under the wrong identifier field "
        "for all 28 keys");

    for (std::map<Key, mdl::MDLMessagePtr>::const_iterator sample =
             samples.begin();
         sample != samples.end();
         ++sample) {
        manager->AsyncPublish(sample->second.Get());
    }
    const std::uint64_t expected_callbacks =
        static_cast<std::uint64_t>(samples.size()) * 2U;
    test->Expect(
        matching_handler.WaitForTotal(expected_callbacks),
        "AsyncPublish delivers every matching explicit publication");
    manager->Shutdown();

    test->Expect(
        matching_handler.total() == expected_callbacks,
        "matching subscriber receives exactly one sync and one async callback "
        "per supported key");
    test->Expect(
        rejecting_handler.total() == 0,
        "nonmatching SecurityID/InstruID subscriber receives no explicit "
        "sync or async callback");
    test->Expect(
        wrong_field_handler.total() == 0,
        "wrong-field subscriber receives no explicit sync or async callback");
    for (std::map<Key, mdl::MDLMessagePtr>::const_iterator sample =
             samples.begin();
         sample != samples.end();
         ++sample) {
        test->Expect(
            matching_handler.Count(sample->first) == 2,
            "explicit sync/async publication routes twice for SID " +
                std::to_string(sample->first.service_id) + " MID " +
                std::to_string(sample->first.message_id));
    }
}

} // namespace

int main() {
    TestContext test;
    const std::map<Key, mdl::MDLMessagePtr> samples =
        CaptureEverySupportedMessage(&test);
    if (!samples.empty()) {
        CheckExplicitPublicationRouting(&test, samples);
    }

    if (test.failures != 0) {
        std::cerr << "review subscription regression test failed with "
                  << test.failures << " error(s)\n";
        return 1;
    }

    std::cout
        << "SDK subscription JSON and explicit publication identifier routing "
           "checks passed\n";
    return 0;
}
