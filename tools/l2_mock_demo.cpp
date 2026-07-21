#include "l2mock/l2_mock.h"

#include "mdl_cffexl2_msg.h"
#include "mdl_czcel2_msg.h"
#include "mdl_dcel2_msg.h"
#include "mdl_gfexl2_msg.h"
#include "mdl_shfel2_msg.h"
#include "mdl_shl2_msg.h"
#include "mdl_szl2_msg.h"

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>
#include <thread>

namespace mdl = datayes::mdl;
namespace mock = datayes::mdl::mock;

namespace {

const std::size_t kServiceCount = 7;
const uint8_t kServiceIds[kServiceCount] = {
    mdl::MDLSID_MDL_SHL2,
    mdl::MDLSID_MDL_SZL2,
    mdl::MDLSID_MDL_CFFEXL2,
    mdl::MDLSID_MDL_SHFEL2,
    mdl::MDLSID_MDL_CZCEL2,
    mdl::MDLSID_MDL_DCEL2,
    mdl::MDLSID_MDL_GFEXL2
};
const char* const kServiceNames[kServiceCount] = {
    "shl2", "szl2", "cffexl2", "shfel2", "czcel2", "dcel2", "gfexl2"
};

struct Options {
    uint32_t duration_seconds = 10;
    uint64_t rate = 100000;
    uint32_t shanghai_instruments = 2500;
    uint32_t shenzhen_instruments = 2500;
    uint32_t futures_per_market = 64;
    uint64_t seed = 0x4c32464c4f57ULL;
};

void PrintUsage(const char* program) {
    std::cout
        << "Usage: " << program << " [options]\n"
        << "  --duration SECONDS  Run duration (default: 10)\n"
        << "  --rate MESSAGES     Target messages/second; 0 is unpaced (default: 100000)\n"
        << "  --sh COUNT          Synthetic Shanghai instruments (default: 2500)\n"
        << "  --sz COUNT          Synthetic Shenzhen instruments (default: 2500)\n"
        << "  --futures COUNT     Synthetic instruments per derivatives market;\n"
        << "                      0 disables derivatives (default: 64)\n"
        << "  --seed UINT64       Deterministic random seed (decimal or 0x-prefixed)\n"
        << "  --help              Show this help\n";
}

bool ParseUnsigned(const std::string& text, uint64_t* value) {
    if (text.empty() || text[0] == '-') {
        return false;
    }
    errno = 0;
    char* end = NULL;
    const bool hexadecimal =
        text.size() > 2 && text[0] == '0' &&
        (text[1] == 'x' || text[1] == 'X');
    const unsigned long long parsed =
        std::strtoull(text.c_str(), &end, hexadecimal ? 16 : 10);
    if (errno == ERANGE || end == text.c_str() || *end != '\0') {
        return false;
    }
    *value = static_cast<uint64_t>(parsed);
    return true;
}

bool AssignUint32(
    const std::string& option,
    const std::string& text,
    uint32_t* destination,
    std::string* error) {
    uint64_t value = 0;
    if (!ParseUnsigned(text, &value) ||
        value > std::numeric_limits<uint32_t>::max()) {
        *error = option + " expects an unsigned 32-bit integer";
        return false;
    }
    *destination = static_cast<uint32_t>(value);
    return true;
}

bool ParseOptions(
    int argc,
    char* argv[],
    Options* options,
    bool* show_help,
    std::string* error) {
    *show_help = false;
    for (int i = 1; i < argc; ++i) {
        std::string option(argv[i]);
        std::string value;
        const std::string::size_type equal = option.find('=');
        if (equal != std::string::npos) {
            value = option.substr(equal + 1);
            option.resize(equal);
        }

        if (option == "--help" || option == "-h") {
            *show_help = true;
            return true;
        }
        if (option != "--duration" && option != "--rate" &&
            option != "--sh" && option != "--sz" &&
            option != "--futures" && option != "--seed") {
            *error = "unknown option: " + option;
            return false;
        }
        if (equal == std::string::npos) {
            if (++i >= argc) {
                *error = "missing value for " + option;
                return false;
            }
            value = argv[i];
        }

        if (option == "--duration") {
            if (!AssignUint32(option, value, &options->duration_seconds, error) ||
                options->duration_seconds == 0) {
                if (error->empty()) {
                    *error = "--duration must be at least one second";
                }
                return false;
            }
        } else if (option == "--rate") {
            if (!ParseUnsigned(value, &options->rate)) {
                *error = "--rate expects an unsigned 64-bit integer";
                return false;
            }
        } else if (option == "--sh") {
            if (!AssignUint32(
                    option, value, &options->shanghai_instruments, error)) {
                return false;
            }
        } else if (option == "--sz") {
            if (!AssignUint32(
                    option, value, &options->shenzhen_instruments, error)) {
                return false;
            }
        } else if (option == "--futures") {
            if (!AssignUint32(
                    option, value, &options->futures_per_market, error)) {
                return false;
            }
        } else {
            if (!ParseUnsigned(value, &options->seed)) {
                *error = "--seed expects an unsigned 64-bit integer";
                return false;
            }
        }
    }
    return true;
}

int ServiceIndex(uint8_t service_id) {
    for (std::size_t i = 0; i < kServiceCount; ++i) {
        if (kServiceIds[i] == service_id) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

struct HandlerSnapshot {
    std::array<uint64_t, kServiceCount> by_service;
    uint64_t total = 0;
    uint64_t parsed = 0;
    uint64_t parse_errors = 0;
    uint64_t time_checks = 0;
    uint64_t invalid_times = 0;
};

class CountingHandler : public mdl::MessageHandler {
public:
    CountingHandler() {
        for (std::size_t i = 0; i < kServiceCount; ++i) {
            by_service_[i].store(0, std::memory_order_relaxed);
        }
    }

    HandlerSnapshot Snapshot() const {
        HandlerSnapshot result;
        for (std::size_t i = 0; i < kServiceCount; ++i) {
            result.by_service[i] =
                by_service_[i].load(std::memory_order_relaxed);
            result.total += result.by_service[i];
        }
        result.parsed = parsed_.load(std::memory_order_relaxed);
        result.parse_errors = parse_errors_.load(std::memory_order_relaxed);
        result.time_checks = time_checks_.load(std::memory_order_relaxed);
        result.invalid_times = invalid_times_.load(std::memory_order_relaxed);
        return result;
    }

    void OnMDLSHL2Message(const mdl::MDLMessage* message) override {
        Handle(mdl::MDLSID_MDL_SHL2, message);
    }

    void OnMDLSZL2Message(const mdl::MDLMessage* message) override {
        Handle(mdl::MDLSID_MDL_SZL2, message);
    }

    void OnMDLCFFEXL2Message(const mdl::MDLMessage* message) override {
        Handle(mdl::MDLSID_MDL_CFFEXL2, message);
    }

    void OnMDLSHFEL2Message(const mdl::MDLMessage* message) override {
        Handle(mdl::MDLSID_MDL_SHFEL2, message);
    }

    void OnMDLCZCEL2Message(const mdl::MDLMessage* message) override {
        Handle(mdl::MDLSID_MDL_CZCEL2, message);
    }

    void OnMDLDCEL2Message(const mdl::MDLMessage* message) override {
        Handle(mdl::MDLSID_MDL_DCEL2, message);
    }

    void OnMDLGFEXL2Message(const mdl::MDLMessage* message) override {
        Handle(mdl::MDLSID_MDL_GFEXL2, message);
    }

private:
    void CheckTime(const mdl::MDLTime& time) {
        time_checks_.fetch_add(1, std::memory_order_relaxed);
        if (time.IsNull() || !time.IsValid()) {
            invalid_times_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    bool ValidateString(
        const char* body,
        std::size_t body_size,
        const mdl::MDLAnsiString& value) const {
        const uintptr_t body_address =
            reinterpret_cast<uintptr_t>(body);
        const uintptr_t field_address =
            reinterpret_cast<uintptr_t>(&value);
        if (field_address < body_address) {
            return false;
        }
        const std::size_t field_offset =
            static_cast<std::size_t>(field_address - body_address);
        if (field_offset > body_size ||
            sizeof(value) > body_size - field_offset) {
            return false;
        }
        if (value.Length == 0) {
            return true;
        }
        if (value.Offset == 0 ||
            value.Offset > body_size - field_offset) {
            return false;
        }
        const std::size_t string_offset = field_offset + value.Offset;
        return value.Length <= body_size - string_offset;
    }

    template <typename Body>
    bool Inspect(
        const mdl::MDLMessage* message,
        const mdl::MDLMessageHead* head,
        mdl::MDLTime Body::* time_member,
        mdl::MDLAnsiString Body::* identifier_member) {
        if (head->ServiceID != Body::ServiceID ||
            head->ServiceVersion != Body::ServiceVer ||
            head->MessageID != Body::MessageID ||
            head->MessageSize < head->HeadSize) {
            return false;
        }
        const std::size_t body_size =
            static_cast<std::size_t>(head->MessageSize - head->HeadSize);
        const char* const bytes = message->GetBody();
        if (bytes == NULL || body_size < sizeof(Body)) {
            return false;
        }
        const Body* const body = reinterpret_cast<const Body*>(bytes);
        CheckTime(body->*time_member);
        return ValidateString(bytes, body_size, body->*identifier_member);
    }

    bool ParseShanghai(
        const mdl::MDLMessage* message,
        const mdl::MDLMessageHead* head) {
        switch (head->MessageID) {
        case mdl::mdl_shl2_msg::SHL2Transaction::MessageID:
            return Inspect<mdl::mdl_shl2_msg::SHL2Transaction>(
                message, head,
                &mdl::mdl_shl2_msg::SHL2Transaction::TradTime,
                &mdl::mdl_shl2_msg::SHL2Transaction::SecurityID);
        case mdl::mdl_shl2_msg::SHL2MarketData::MessageID:
            return Inspect<mdl::mdl_shl2_msg::SHL2MarketData>(
                message, head,
                &mdl::mdl_shl2_msg::SHL2MarketData::UpdateTime,
                &mdl::mdl_shl2_msg::SHL2MarketData::SecurityID);
        case mdl::mdl_shl2_msg::SHL2Transaction2::MessageID:
            return Inspect<mdl::mdl_shl2_msg::SHL2Transaction2>(
                message, head,
                &mdl::mdl_shl2_msg::SHL2Transaction2::TradTime,
                &mdl::mdl_shl2_msg::SHL2Transaction2::SecurityID);
        case mdl::mdl_shl2_msg::Order::MessageID:
            return Inspect<mdl::mdl_shl2_msg::Order>(
                message, head,
                &mdl::mdl_shl2_msg::Order::OrderTime,
                &mdl::mdl_shl2_msg::Order::SecurityID);
        default:
            return false;
        }
    }

    bool ParseShenzhen(
        const mdl::MDLMessage* message,
        const mdl::MDLMessageHead* head) {
        switch (head->MessageID) {
        case mdl::mdl_szl2_msg::Trade::MessageID:
            return Inspect<mdl::mdl_szl2_msg::Trade>(
                message, head, &mdl::mdl_szl2_msg::Trade::TradTime,
                &mdl::mdl_szl2_msg::Trade::SecurityID);
        case mdl::mdl_szl2_msg::Order::MessageID:
            return Inspect<mdl::mdl_szl2_msg::Order>(
                message, head, &mdl::mdl_szl2_msg::Order::OrderEntryTime,
                &mdl::mdl_szl2_msg::Order::SecurityID);
        case mdl::mdl_szl2_msg::MarketData::MessageID:
            return Inspect<mdl::mdl_szl2_msg::MarketData>(
                message, head, &mdl::mdl_szl2_msg::MarketData::DataTimeStamp,
                &mdl::mdl_szl2_msg::MarketData::SecurityID);
        case mdl::mdl_szl2_msg::Snapshot300111_v2::MessageID:
            return Inspect<mdl::mdl_szl2_msg::Snapshot300111_v2>(
                message, head,
                &mdl::mdl_szl2_msg::Snapshot300111_v2::UpdateTime,
                &mdl::mdl_szl2_msg::Snapshot300111_v2::SecurityID);
        case mdl::mdl_szl2_msg::Snapshot300111_v3::MessageID:
            return Inspect<mdl::mdl_szl2_msg::Snapshot300111_v3>(
                message, head,
                &mdl::mdl_szl2_msg::Snapshot300111_v3::UpdateTime,
                &mdl::mdl_szl2_msg::Snapshot300111_v3::SecurityID);
        case mdl::mdl_szl2_msg::Order300192_v2::MessageID:
            return Inspect<mdl::mdl_szl2_msg::Order300192_v2>(
                message, head,
                &mdl::mdl_szl2_msg::Order300192_v2::TransactTime,
                &mdl::mdl_szl2_msg::Order300192_v2::SecurityID);
        case mdl::mdl_szl2_msg::Order300592_v2::MessageID:
            return Inspect<mdl::mdl_szl2_msg::Order300592_v2>(
                message, head,
                &mdl::mdl_szl2_msg::Order300592_v2::TransactTime,
                &mdl::mdl_szl2_msg::Order300592_v2::SecurityID);
        case mdl::mdl_szl2_msg::Order300792_v2::MessageID:
            return Inspect<mdl::mdl_szl2_msg::Order300792_v2>(
                message, head,
                &mdl::mdl_szl2_msg::Order300792_v2::TransactTime,
                &mdl::mdl_szl2_msg::Order300792_v2::SecurityID);
        case mdl::mdl_szl2_msg::Transaction300191_v2::MessageID:
            return Inspect<mdl::mdl_szl2_msg::Transaction300191_v2>(
                message, head,
                &mdl::mdl_szl2_msg::Transaction300191_v2::TransactTime,
                &mdl::mdl_szl2_msg::Transaction300191_v2::SecurityID);
        case mdl::mdl_szl2_msg::Transaction300591_v2::MessageID:
            return Inspect<mdl::mdl_szl2_msg::Transaction300591_v2>(
                message, head,
                &mdl::mdl_szl2_msg::Transaction300591_v2::TransactTime,
                &mdl::mdl_szl2_msg::Transaction300591_v2::SecurityID);
        case mdl::mdl_szl2_msg::Transaction300791_v2::MessageID:
            return Inspect<mdl::mdl_szl2_msg::Transaction300791_v2>(
                message, head,
                &mdl::mdl_szl2_msg::Transaction300791_v2::TransactTime,
                &mdl::mdl_szl2_msg::Transaction300791_v2::SecurityID);
        case mdl::mdl_szl2_msg::CombinedTick::MessageID:
            return Inspect<mdl::mdl_szl2_msg::CombinedTick>(
                message, head,
                &mdl::mdl_szl2_msg::CombinedTick::TransactTime,
                &mdl::mdl_szl2_msg::CombinedTick::SecurityID);
        default:
            return false;
        }
    }

    bool ParseCffex(
        const mdl::MDLMessage* message,
        const mdl::MDLMessageHead* head) {
        switch (head->MessageID) {
        case mdl::mdl_cffexl2_msg::Future::MessageID:
            return Inspect<mdl::mdl_cffexl2_msg::Future>(
                message, head, &mdl::mdl_cffexl2_msg::Future::UpdateTime,
                &mdl::mdl_cffexl2_msg::Future::InstruID);
        case mdl::mdl_cffexl2_msg::Option::MessageID:
            return Inspect<mdl::mdl_cffexl2_msg::Option>(
                message, head, &mdl::mdl_cffexl2_msg::Option::UpdateTime,
                &mdl::mdl_cffexl2_msg::Option::InstruID);
        default:
            return false;
        }
    }

    bool ParseShfe(
        const mdl::MDLMessage* message,
        const mdl::MDLMessageHead* head) {
        switch (head->MessageID) {
        case mdl::mdl_shfel2_msg::CTPFuture::MessageID:
            return Inspect<mdl::mdl_shfel2_msg::CTPFuture>(
                message, head, &mdl::mdl_shfel2_msg::CTPFuture::UpdateTime,
                &mdl::mdl_shfel2_msg::CTPFuture::InstruID);
        case mdl::mdl_shfel2_msg::CTPOption::MessageID:
            return Inspect<mdl::mdl_shfel2_msg::CTPOption>(
                message, head, &mdl::mdl_shfel2_msg::CTPOption::UpdateTime,
                &mdl::mdl_shfel2_msg::CTPOption::InstruID);
        case mdl::mdl_shfel2_msg::CrudeFuture::MessageID:
            return Inspect<mdl::mdl_shfel2_msg::CrudeFuture>(
                message, head, &mdl::mdl_shfel2_msg::CrudeFuture::UpdateTime,
                &mdl::mdl_shfel2_msg::CrudeFuture::InstruID);
        case mdl::mdl_shfel2_msg::CrudeOption::MessageID:
            return Inspect<mdl::mdl_shfel2_msg::CrudeOption>(
                message, head, &mdl::mdl_shfel2_msg::CrudeOption::UpdateTime,
                &mdl::mdl_shfel2_msg::CrudeOption::InstruID);
        default:
            return false;
        }
    }

    bool ParseCzce(
        const mdl::MDLMessage* message,
        const mdl::MDLMessageHead* head) {
        switch (head->MessageID) {
        case mdl::mdl_czcel2_msg::CTPFuture::MessageID:
            return Inspect<mdl::mdl_czcel2_msg::CTPFuture>(
                message, head, &mdl::mdl_czcel2_msg::CTPFuture::UpdateTime,
                &mdl::mdl_czcel2_msg::CTPFuture::InstruID);
        case mdl::mdl_czcel2_msg::CTPOption::MessageID:
            return Inspect<mdl::mdl_czcel2_msg::CTPOption>(
                message, head, &mdl::mdl_czcel2_msg::CTPOption::UpdateTime,
                &mdl::mdl_czcel2_msg::CTPOption::InstruID);
        default:
            return false;
        }
    }

    bool ParseDce(
        const mdl::MDLMessage* message,
        const mdl::MDLMessageHead* head) {
        switch (head->MessageID) {
        case mdl::mdl_dcel2_msg::Future::MessageID:
            return Inspect<mdl::mdl_dcel2_msg::Future>(
                message, head, &mdl::mdl_dcel2_msg::Future::UpdateTime,
                &mdl::mdl_dcel2_msg::Future::InstruID);
        case mdl::mdl_dcel2_msg::Option::MessageID:
            return Inspect<mdl::mdl_dcel2_msg::Option>(
                message, head, &mdl::mdl_dcel2_msg::Option::UpdateTime,
                &mdl::mdl_dcel2_msg::Option::InstruID);
        case mdl::mdl_dcel2_msg::FutureOrder::MessageID:
            return Inspect<mdl::mdl_dcel2_msg::FutureOrder>(
                message, head, &mdl::mdl_dcel2_msg::FutureOrder::UpdateTime,
                &mdl::mdl_dcel2_msg::FutureOrder::InstruID);
        case mdl::mdl_dcel2_msg::OptionOrder::MessageID:
            return Inspect<mdl::mdl_dcel2_msg::OptionOrder>(
                message, head, &mdl::mdl_dcel2_msg::OptionOrder::UpdateTime,
                &mdl::mdl_dcel2_msg::OptionOrder::InstruID);
        default:
            return false;
        }
    }

    bool ParseGfex(
        const mdl::MDLMessage* message,
        const mdl::MDLMessageHead* head) {
        switch (head->MessageID) {
        case mdl::mdl_gfexl2_msg::Future::MessageID:
            return Inspect<mdl::mdl_gfexl2_msg::Future>(
                message, head, &mdl::mdl_gfexl2_msg::Future::UpdateTime,
                &mdl::mdl_gfexl2_msg::Future::InstruID);
        case mdl::mdl_gfexl2_msg::Option::MessageID:
            return Inspect<mdl::mdl_gfexl2_msg::Option>(
                message, head, &mdl::mdl_gfexl2_msg::Option::UpdateTime,
                &mdl::mdl_gfexl2_msg::Option::InstruID);
        case mdl::mdl_gfexl2_msg::FutureOrder::MessageID:
            return Inspect<mdl::mdl_gfexl2_msg::FutureOrder>(
                message, head, &mdl::mdl_gfexl2_msg::FutureOrder::UpdateTime,
                &mdl::mdl_gfexl2_msg::FutureOrder::InstruID);
        case mdl::mdl_gfexl2_msg::OptionOrder::MessageID:
            return Inspect<mdl::mdl_gfexl2_msg::OptionOrder>(
                message, head, &mdl::mdl_gfexl2_msg::OptionOrder::UpdateTime,
                &mdl::mdl_gfexl2_msg::OptionOrder::InstruID);
        default:
            return false;
        }
    }

    void Handle(uint8_t expected_service, const mdl::MDLMessage* message) {
        const int index = ServiceIndex(expected_service);
        if (index >= 0) {
            by_service_[static_cast<std::size_t>(index)].fetch_add(
                1, std::memory_order_relaxed);
        }
        if (message == NULL) {
            parse_errors_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        const mdl::MDLMessageHead* const head = message->GetHead();
        if (head == NULL || head->ServiceID != expected_service ||
            head->HeadSize < sizeof(mdl::MDLMessageHead) ||
            head->MessageSize < head->HeadSize) {
            parse_errors_.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        CheckTime(head->LocalTime);
        bool valid = false;
        switch (expected_service) {
        case mdl::MDLSID_MDL_SHL2:
            valid = ParseShanghai(message, head);
            break;
        case mdl::MDLSID_MDL_SZL2:
            valid = ParseShenzhen(message, head);
            break;
        case mdl::MDLSID_MDL_CFFEXL2:
            valid = ParseCffex(message, head);
            break;
        case mdl::MDLSID_MDL_SHFEL2:
            valid = ParseShfe(message, head);
            break;
        case mdl::MDLSID_MDL_CZCEL2:
            valid = ParseCzce(message, head);
            break;
        case mdl::MDLSID_MDL_DCEL2:
            valid = ParseDce(message, head);
            break;
        case mdl::MDLSID_MDL_GFEXL2:
            valid = ParseGfex(message, head);
            break;
        default:
            break;
        }
        if (valid) {
            parsed_.fetch_add(1, std::memory_order_relaxed);
        } else {
            parse_errors_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    std::array<std::atomic<uint64_t>, kServiceCount> by_service_;
    std::atomic<uint64_t> parsed_{0};
    std::atomic<uint64_t> parse_errors_{0};
    std::atomic<uint64_t> time_checks_{0};
    std::atomic<uint64_t> invalid_times_{0};
};

void PrintStatistics(
    const char* label,
    double elapsed_seconds,
    const mdl::IOManager* manager,
    const CountingHandler& handler) {
    const mock::Statistics statistics = mock::GetStatistics(manager);
    const HandlerSnapshot callbacks = handler.Snapshot();
    const double generated_rate =
        elapsed_seconds > 0.0
            ? static_cast<double>(statistics.generated) / elapsed_seconds
            : 0.0;

    std::cout << std::fixed << std::setprecision(3)
              << label
              << " elapsed_s=" << elapsed_seconds
              << " generated=" << statistics.generated
              << " delivered=" << statistics.delivered
              << " filtered=" << statistics.filtered
              << " dropped=" << statistics.dropped
              << " callback_errors=" << statistics.callback_errors
              << " queue_high_watermark=" << statistics.queue_high_watermark
              << " active_subscribers=" << statistics.active_subscribers
              << " generated_per_s=" << std::setprecision(1) << generated_rate
              << " callbacks=" << callbacks.total
              << " parsed=" << callbacks.parsed
              << " parse_errors=" << callbacks.parse_errors
              << " time_checks=" << callbacks.time_checks
              << " invalid_times=" << callbacks.invalid_times;
    for (std::size_t i = 0; i < kServiceCount; ++i) {
        std::cout << " " << kServiceNames[i] << "=" << callbacks.by_service[i];
    }
    std::cout << "\n";
}

} // namespace

int main(int argc, char* argv[]) {
    Options options;
    bool show_help = false;
    std::string error;
    if (!ParseOptions(argc, argv, &options, &show_help, &error)) {
        std::cerr << "error: " << error << "\n";
        PrintUsage(argv[0]);
        return 2;
    }
    if (show_help) {
        PrintUsage(argv[0]);
        return 0;
    }

    mock::Config config;
    config.seed = options.seed;
    config.messages_per_second = options.rate;
    config.shanghai_instruments = options.shanghai_instruments;
    config.shenzhen_instruments = options.shenzhen_instruments;
    config.derivatives_per_market = options.futures_per_market;
    config.include_derivatives = options.futures_per_market != 0;

    error = mock::ValidateConfig(config);
    if (!error.empty()) {
        std::cerr << "invalid mock configuration: " << error << "\n";
        return 3;
    }

    CountingHandler handler;
    mdl::IOManagerPtr manager = mock::CreateIOManager(config);
    if (manager.IsNull() || !mock::IsMockIOManager(manager.Get())) {
        std::cerr << "failed to create the mock IOManager\n";
        return 4;
    }

    mdl::SubscriberPtr subscriber = manager->CreateSubscriber(&handler, true);
    if (subscriber.IsNull()) {
        std::cerr << "failed to create the mock Subscriber\n";
        manager->Shutdown();
        manager.Reset();
        return 5;
    }

    mock::SubscribeAll(subscriber.Get());

    error = subscriber->Connect();
    if (!error.empty()) {
        std::cerr << "mock Subscriber::Connect failed: " << error << "\n";
        manager->Shutdown();
        subscriber.Reset();
        manager.Reset();
        return 6;
    }

    std::cout << "started"
              << " duration_s=" << options.duration_seconds
              << " target_rate=" << options.rate
              << " sh=" << options.shanghai_instruments
              << " sz=" << options.shenzhen_instruments
              << " futures_per_market=" << options.futures_per_market
              << " seed=" << options.seed
              << " supported_messages=" << mock::SupportedMessages().size()
              << "\n";

    typedef std::chrono::steady_clock Clock;
    const Clock::time_point started = Clock::now();
    const Clock::time_point deadline =
        started + std::chrono::seconds(options.duration_seconds);
    Clock::time_point next_report = started + std::chrono::seconds(1);

    while (Clock::now() < deadline) {
        const Clock::time_point wake =
            next_report < deadline ? next_report : deadline;
        std::this_thread::sleep_until(wake);
        const double elapsed =
            std::chrono::duration<double>(Clock::now() - started).count();
        PrintStatistics("statistics", elapsed, manager.Get(), handler);
        next_report += std::chrono::seconds(1);
    }

    manager->Shutdown();
    const double elapsed =
        std::chrono::duration<double>(Clock::now() - started).count();
    PrintStatistics("final", elapsed, manager.Get(), handler);

    const mock::Statistics statistics = mock::GetStatistics(manager.Get());
    const HandlerSnapshot callbacks = handler.Snapshot();
    const bool ok =
        statistics.callback_errors == 0 &&
        callbacks.parse_errors == 0 &&
        callbacks.invalid_times == 0 &&
        (statistics.generated == 0 || callbacks.total != 0);

    subscriber.Reset();
    manager.Reset();
    return ok ? 0 : 7;
}
