#include "l2flow/sdk/sdk_runtime.h"
#include "l2flow/sdk/subscription_manifest.h"

#include "mdl_shl2_msg.h"
#include "mdl_szl2_msg.h"
#include "mdl_sys_msg.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace mdl = datayes::mdl;
namespace sdk = l2flow::sdk;
namespace sh = datayes::mdl::mdl_shl2_msg;
namespace sz = datayes::mdl::mdl_szl2_msg;

namespace {

struct Options final {
    std::filesystem::path library;
    std::filesystem::path capture_csv;
    std::string address = "127.0.0.1:9112";
    std::string sdk_log_prefix = "/tmp/l2flow-mdl-sdk-feeder-probe";
    std::uint32_t timeout_seconds = 10U;
    std::uint32_t market_timeout_seconds = 60U;
    std::uint32_t monitor_seconds = 0U;
    std::uint32_t minimum_market_messages = 1U;
    std::uint32_t maximum_captured_records = 100000U;
};

void PrintUsage(std::ostream& output) {
    output
        << "Usage: mdl-sdk-feeder-probe --library PATH [options]\n"
        << "  --address HOST:PORT       feeder SDK endpoint"
           " (default 127.0.0.1:9112)\n"
        << "  --sdk-log-prefix PATH     SDK log prefix"
           " (default /tmp/l2flow-mdl-sdk-feeder-probe)\n"
        << "  --timeout-seconds N       wait for LogonResponse, 1..60"
           " (default 10)\n"
        << "  --market-timeout-seconds N"
           " wait until the minimum arrives, 1..3600 (default 60)\n"
        << "  --monitor-seconds N       always monitor for N seconds, 1..3600"
           " (disabled by default)\n"
        << "  --minimum-market-messages N"
           " required data messages, 0..100000 (default 1)\n"
        << "  --maximum-captured-records N"
           " capture capacity, 1..10000000 (default 100000)\n"
        << "  --capture-csv PATH        write normalized captured records after"
           " clean SDK shutdown\n"
        << "  --help                    show this help\n\n"
        << "The probe uses a fixed non-secret local client label, binary"
           " encoding,\n"
        << "and the five required L2Flow subscriptions. It never accepts a"
           " token on the command line. It reports copied market-message heads;\n"
        << "the target endpoint must be a trusted local feeder. Use a minimum"
           " of 0 only\n"
        << "for a control-plane-only login/subscription check.\n";
}

bool TakeValue(int argc,
               char* argv[],
               int* index,
               std::string_view option,
               std::string_view* value,
               std::string* error) {
    if (*index + 1 >= argc) {
        *error = std::string(option) + " requires a value";
        return false;
    }
    ++*index;
    *value = argv[*index];
    if (value->empty()) {
        *error = std::string(option) + " does not accept an empty value";
        return false;
    }
    return true;
}

bool ParseOptions(int argc,
                  char* argv[],
                  Options* options,
                  bool* show_help,
                  std::string* error) {
    std::set<std::string_view> seen;
    for (int index = 1; index < argc; ++index) {
        const std::string_view option(argv[index]);
        if (option == "--help") {
            if (argc != 2) {
                *error = "--help must be used without other options";
                return false;
            }
            *show_help = true;
            return true;
        }
        if (option != "--library" &&
            option != "--address" &&
            option != "--sdk-log-prefix" &&
            option != "--timeout-seconds" &&
            option != "--market-timeout-seconds" &&
            option != "--monitor-seconds" &&
            option != "--capture-csv" &&
            option != "--minimum-market-messages" &&
            option != "--maximum-captured-records") {
            *error = "unknown option: " + std::string(option);
            return false;
        }
        if (!seen.insert(option).second) {
            *error = "duplicate option: " + std::string(option);
            return false;
        }

        std::string_view value;
        if (!TakeValue(
                argc, argv, &index, option, &value, error)) {
            return false;
        }
        if (option == "--library") {
            options->library = std::string(value);
        } else if (option == "--address") {
            options->address = value;
        } else if (option == "--sdk-log-prefix") {
            options->sdk_log_prefix = value;
        } else if (option == "--capture-csv") {
            options->capture_csv = std::string(value);
        } else if (
            option == "--timeout-seconds" ||
            option == "--market-timeout-seconds" ||
            option == "--monitor-seconds" ||
            option == "--minimum-market-messages" ||
            option == "--maximum-captured-records") {
            std::uint32_t parsed = 0U;
            const std::from_chars_result result =
                std::from_chars(
                    value.data(),
                    value.data() + value.size(),
                    parsed,
                    10);
            if (result.ec != std::errc{} ||
                result.ptr != value.data() + value.size()) {
                *error = std::string(option) +
                         " must be an unsigned decimal integer";
                return false;
            }
            if (option == "--timeout-seconds") {
                if (parsed == 0U || parsed > 60U) {
                    *error =
                        "--timeout-seconds must be from 1 through 60";
                    return false;
                }
                options->timeout_seconds = parsed;
            } else if (option == "--market-timeout-seconds") {
                if (parsed == 0U || parsed > 3600U) {
                    *error =
                        "--market-timeout-seconds must be from 1 through 3600";
                    return false;
                }
                options->market_timeout_seconds = parsed;
            } else if (option == "--monitor-seconds") {
                if (parsed == 0U || parsed > 3600U) {
                    *error =
                        "--monitor-seconds must be from 1 through 3600";
                    return false;
                }
                options->monitor_seconds = parsed;
            } else if (option == "--minimum-market-messages") {
                if (parsed > 100000U) {
                    *error =
                        "--minimum-market-messages must be at most 100000";
                    return false;
                }
                options->minimum_market_messages = parsed;
            } else {
                if (parsed == 0U || parsed > 10000000U) {
                    *error =
                        "--maximum-captured-records must be from 1 through "
                        "10000000";
                    return false;
                }
                options->maximum_captured_records = parsed;
            }
        }
    }
    if (options->library.empty()) {
        *error = "--library is required";
        return false;
    }
    if (!options->capture_csv.empty() &&
        options->capture_csv.native().find('\0') != std::string::npos) {
        *error = "--capture-csv contains NUL";
        return false;
    }
    if (options->minimum_market_messages >
        options->maximum_captured_records) {
        *error =
            "--minimum-market-messages exceeds --maximum-captured-records";
        return false;
    }
    return true;
}

std::vector<sdk::MessageKey> RequiredMessages() {
    const std::string manifest_error = sdk::ValidateIngressSpecs();
    if (!manifest_error.empty()) {
        throw std::runtime_error(
            "invalid built-in subscription manifest: " + manifest_error);
    }

    std::vector<sdk::MessageKey> result;
    for (const sdk::IngressSpec& spec : sdk::AllIngressSpecs()) {
        result.insert(
            result.end(), spec.required.begin(), spec.required.end());
    }
    return result;
}

struct MarketMessageSample final {
    std::uint8_t head_size = 0U;
    std::uint32_t message_size = 0U;
    std::uint8_t message_encoding = 0U;
    std::uint8_t service_id = 0U;
    std::uint16_t service_version = 0U;
    std::uint16_t message_id = 0U;
    std::uint32_t local_time = 0U;
    std::uint64_t sequence_id = 0U;
};

struct CapturedMarketRecord final {
    MarketMessageSample head;
    std::string message_type;
    std::string event_time;
    std::string security_id;
    std::string md_stream_id;
    std::string security_id_source;
    std::string trading_phase_code;
    std::string event_type;
    std::string tick_bs_flag;
    std::string channel_no;
    std::string application_sequence;
    std::string bid_application_sequence;
    std::string offer_application_sequence;
    std::string side;
    std::string order_type;
    std::string execution_type;
    std::string price;
    std::string quantity;
    std::string trade_count;
    std::string volume;
    std::string turnover;
    std::string pre_close_price;
    std::string open_price;
    std::string high_price;
    std::string low_price;
    std::string last_price;
    std::string total_bid_quantity;
    std::string weighted_average_bid_price;
    std::string total_offer_quantity;
    std::string weighted_average_offer_price;
};

std::string FormatTime(std::uint32_t value) {
    std::string digits = std::to_string(value);
    if (digits.size() < 9U) {
        digits.insert(0U, 9U - digits.size(), '0');
    }
    if (digits.size() != 9U) {
        return {};
    }
    return digits.substr(0U, 2U) + ":" +
           digits.substr(2U, 2U) + ":" +
           digits.substr(4U, 2U) + "." +
           digits.substr(6U, 3U);
}

bool FormatTime(const mdl::MDLTime& value, std::string* output) {
    if (value.IsNull()) {
        output->clear();
        return true;
    }
    if (!value.IsValid()) {
        return false;
    }
    *output = FormatTime(value.m_Value);
    return !output->empty();
}

std::string FormatFixed(std::int64_t raw, std::uint32_t decimal_places) {
    std::uint64_t magnitude = static_cast<std::uint64_t>(raw);
    const bool negative = raw < 0;
    if (negative) {
        magnitude = 0U - magnitude;
    }
    std::string digits = std::to_string(magnitude);
    if (decimal_places != 0U) {
        if (digits.size() <= decimal_places) {
            digits.insert(
                0U, decimal_places + 1U - digits.size(), '0');
        }
        digits.insert(digits.size() - decimal_places, 1U, '.');
    }
    if (negative) {
        digits.insert(0U, 1U, '-');
    }
    return digits;
}

template <typename Fixed>
std::string FormatFixed(const Fixed& value) {
    if (value.IsNull()) {
        return {};
    }
    return FormatFixed(
        static_cast<std::int64_t>(value.m_Value),
        value.GetDecimalPlace());
}

bool CopyBoundedString(const char* body,
                       std::size_t body_size,
                       const mdl::MDLAnsiString& value,
                       std::string* output) {
    const std::uintptr_t body_address =
        reinterpret_cast<std::uintptr_t>(body);
    const std::uintptr_t field_address =
        reinterpret_cast<std::uintptr_t>(&value);
    if (field_address < body_address) {
        return false;
    }
    const std::size_t field_offset =
        static_cast<std::size_t>(field_address - body_address);
    if (field_offset > body_size ||
        sizeof(value) > body_size - field_offset) {
        return false;
    }
    if (value.Length == 0U) {
        output->clear();
        return true;
    }
    if (value.Offset == 0U ||
        value.Offset > body_size - field_offset) {
        return false;
    }
    const std::size_t string_offset =
        field_offset + value.Offset;
    if (value.Length > body_size - string_offset) {
        return false;
    }
    output->assign(body + string_offset, value.Length);
    return true;
}

template <typename Body>
const Body* CheckedBody(const mdl::MDLMessage* message,
                        const mdl::MDLMessageHead* head,
                        std::size_t* body_size) {
    if (head->MessageSize < head->HeadSize) {
        return nullptr;
    }
    *body_size = static_cast<std::size_t>(
        head->MessageSize - head->HeadSize);
    const char* const bytes = message->GetBody();
    if (bytes == nullptr || *body_size < sizeof(Body)) {
        return nullptr;
    }
    return reinterpret_cast<const Body*>(bytes);
}

bool DecodeMarketRecord(const mdl::MDLMessage* message,
                        const mdl::MDLMessageHead* head,
                        CapturedMarketRecord* output) {
    output->head = MarketMessageSample{
        head->HeadSize,
        head->MessageSize,
        head->MessageEncoding,
        head->ServiceID,
        head->ServiceVersion,
        head->MessageID,
        head->LocalTime.m_Value,
        head->SequenceID};

    std::size_t body_size = 0U;
    if (head->ServiceID == sh::SHL2MarketData::ServiceID &&
        head->ServiceVersion == sh::SHL2MarketData::ServiceVer &&
        head->MessageID == sh::SHL2MarketData::MessageID) {
        const sh::SHL2MarketData* const body =
            CheckedBody<sh::SHL2MarketData>(message, head, &body_size);
        if (body == nullptr ||
            !FormatTime(body->UpdateTime, &output->event_time) ||
            !CopyBoundedString(message->GetBody(), body_size,
                               body->SecurityID, &output->security_id) ||
            !CopyBoundedString(message->GetBody(), body_size,
                               body->InstruStatus, &output->event_type)) {
            return false;
        }
        output->message_type = "SHL2MarketData";
        output->last_price = FormatFixed(body->LastPrice);
        output->pre_close_price = FormatFixed(body->PreCloPrice);
        output->open_price = FormatFixed(body->OpenPrice);
        output->high_price = FormatFixed(body->HighPrice);
        output->low_price = FormatFixed(body->LowPrice);
        output->trade_count = std::to_string(body->TradNumber);
        output->volume = FormatFixed(body->TradVolume);
        output->turnover = FormatFixed(body->Turnover);
        output->total_bid_quantity = FormatFixed(body->TotalBidVol);
        output->weighted_average_bid_price = FormatFixed(body->WAvgBidPri);
        output->total_offer_quantity = FormatFixed(body->TotalAskVol);
        output->weighted_average_offer_price = FormatFixed(body->WAvgAskPri);
        return true;
    }
    if (head->ServiceID == sh::NGTSTick::ServiceID &&
        head->ServiceVersion == sh::NGTSTick::ServiceVer &&
        head->MessageID == sh::NGTSTick::MessageID) {
        const sh::NGTSTick* const body =
            CheckedBody<sh::NGTSTick>(message, head, &body_size);
        if (body == nullptr ||
            !FormatTime(body->TickTime, &output->event_time) ||
            !CopyBoundedString(message->GetBody(), body_size,
                               body->SecurityID, &output->security_id) ||
            !CopyBoundedString(message->GetBody(), body_size,
                               body->Type, &output->event_type) ||
            !CopyBoundedString(message->GetBody(), body_size,
                               body->TickBSFlag, &output->tick_bs_flag)) {
            return false;
        }
        output->message_type = "NGTSTick";
        output->application_sequence = std::to_string(body->BizIndex);
        output->channel_no = std::to_string(body->Channel);
        output->bid_application_sequence =
            std::to_string(body->BuyOrderNO);
        output->offer_application_sequence =
            std::to_string(body->SellOrderNO);
        output->price = FormatFixed(body->Price);
        output->quantity = std::to_string(body->Qty);
        output->turnover = FormatFixed(body->TradeMoney);
        return true;
    }
    if (head->ServiceID == sz::Snapshot300111_v2::ServiceID &&
        head->ServiceVersion == sz::Snapshot300111_v2::ServiceVer &&
        head->MessageID == sz::Snapshot300111_v2::MessageID) {
        const sz::Snapshot300111_v2* const body =
            CheckedBody<sz::Snapshot300111_v2>(
                message, head, &body_size);
        if (body == nullptr ||
            !FormatTime(body->UpdateTime, &output->event_time) ||
            !CopyBoundedString(message->GetBody(), body_size,
                               body->MDStreamID, &output->md_stream_id) ||
            !CopyBoundedString(message->GetBody(), body_size,
                               body->SecurityID, &output->security_id) ||
            !CopyBoundedString(message->GetBody(), body_size,
                               body->SecurityIDSource,
                               &output->security_id_source) ||
            !CopyBoundedString(message->GetBody(), body_size,
                               body->TradingPhaseCode,
                               &output->trading_phase_code)) {
            return false;
        }
        output->message_type = "Snapshot300111_v2";
        output->channel_no = std::to_string(body->ChannelNo);
        output->trade_count = std::to_string(body->TurnNum);
        output->volume = std::to_string(body->Volume);
        output->turnover = FormatFixed(body->Turnover);
        output->pre_close_price = FormatFixed(body->PreCloPrice);
        output->open_price = FormatFixed(body->OpenPrice);
        output->high_price = FormatFixed(body->HighPrice);
        output->low_price = FormatFixed(body->LowPrice);
        output->last_price = FormatFixed(body->LastPrice);
        output->total_bid_quantity = std::to_string(body->TotalBidQty);
        output->weighted_average_bid_price =
            FormatFixed(body->WeightedAvgBidPx);
        output->total_offer_quantity = std::to_string(body->TotalOfferQty);
        output->weighted_average_offer_price =
            FormatFixed(body->WeightedAvgOfferPx);
        return true;
    }
    if (head->ServiceID == sz::Order300192_v2::ServiceID &&
        head->ServiceVersion == sz::Order300192_v2::ServiceVer &&
        head->MessageID == sz::Order300192_v2::MessageID) {
        const sz::Order300192_v2* const body =
            CheckedBody<sz::Order300192_v2>(message, head, &body_size);
        if (body == nullptr ||
            !FormatTime(body->TransactTime, &output->event_time) ||
            !CopyBoundedString(message->GetBody(), body_size,
                               body->MDStreamID, &output->md_stream_id) ||
            !CopyBoundedString(message->GetBody(), body_size,
                               body->SecurityID, &output->security_id) ||
            !CopyBoundedString(message->GetBody(), body_size,
                               body->SecurityIDSource,
                               &output->security_id_source)) {
            return false;
        }
        output->message_type = "Order300192_v2";
        output->channel_no = std::to_string(body->ChannelNo);
        output->application_sequence = std::to_string(body->ApplSeqNum);
        output->price = FormatFixed(body->Price);
        output->quantity = std::to_string(body->OrderQty);
        output->side = std::to_string(body->Side);
        output->order_type = std::to_string(body->OrdType);
        return true;
    }
    if (head->ServiceID == sz::Transaction300191_v2::ServiceID &&
        head->ServiceVersion == sz::Transaction300191_v2::ServiceVer &&
        head->MessageID == sz::Transaction300191_v2::MessageID) {
        const sz::Transaction300191_v2* const body =
            CheckedBody<sz::Transaction300191_v2>(
                message, head, &body_size);
        if (body == nullptr ||
            !FormatTime(body->TransactTime, &output->event_time) ||
            !CopyBoundedString(message->GetBody(), body_size,
                               body->MDStreamID, &output->md_stream_id) ||
            !CopyBoundedString(message->GetBody(), body_size,
                               body->SecurityID, &output->security_id) ||
            !CopyBoundedString(message->GetBody(), body_size,
                               body->SecurityIDSource,
                               &output->security_id_source)) {
            return false;
        }
        output->message_type = "Transaction300191_v2";
        output->channel_no = std::to_string(body->ChannelNo);
        output->application_sequence = std::to_string(body->ApplSeqNum);
        output->bid_application_sequence =
            std::to_string(body->BidApplSeqNum);
        output->offer_application_sequence =
            std::to_string(body->OfferApplSeqNum);
        output->price = FormatFixed(body->LastPx);
        output->quantity = std::to_string(body->LastQty);
        output->execution_type = std::to_string(body->ExecType);
        return true;
    }
    return false;
}

struct SubscriptionStatusSample final {
    sdk::MessageKey key;
    bool ok = false;
    bool failed = false;
};

struct ProbeSnapshot final {
    bool logon_received = false;
    bool malformed_response = false;
    std::uint32_t return_code = 0U;
    std::size_t required_ok = 0U;
    std::size_t required_failed = 0U;
    std::uint64_t market_messages = 0U;
    std::uint64_t dropped_market_records = 0U;
    bool capture_overflow = false;
    std::vector<SubscriptionStatusSample> subscription_statuses;
    std::vector<MarketMessageSample> market_samples;
    std::vector<CapturedMarketRecord> market_records;
};

class ProbeHandler final : public mdl::MessageHandlerBase {
public:
    explicit ProbeHandler(
        std::vector<sdk::MessageKey> expected,
        std::size_t maximum_captured_records)
        : expected_(std::move(expected)),
          maximum_captured_records_(maximum_captured_records),
          required_ok_(expected_.size(), false),
          required_failed_(expected_.size(), false) {}

    void OnMessage(
        mdl::Subscriber*,
        const mdl::MDLMessage* message) noexcept override {
        try {
            HandleMessage(message);
        } catch (...) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                malformed_response_ = true;
                logon_received_ = true;
            }
            condition_.notify_all();
        }
    }

    ProbeSnapshot WaitForLogon(
        std::chrono::seconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        static_cast<void>(
            condition_.wait_for(
                lock,
                timeout,
                [this] { return logon_received_; }));
        return SnapshotLocked();
    }

    ProbeSnapshot WaitForMarketMessages(
        std::uint64_t minimum,
        std::chrono::seconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        static_cast<void>(
            condition_.wait_for(
                lock,
                timeout,
                [this, minimum] {
                    return market_messages_ >= minimum ||
                           malformed_response_;
                }));
        return SnapshotLocked();
    }

    ProbeSnapshot MonitorMarketMessages(
        std::chrono::seconds duration) {
        std::unique_lock<std::mutex> lock(mutex_);
        static_cast<void>(
            condition_.wait_for(
                lock,
                duration,
                [this] {
                    return malformed_response_ || capture_overflow_;
                }));
        return SnapshotLocked();
    }

    ProbeSnapshot Snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return SnapshotLocked();
    }

private:
    void HandleMessage(const mdl::MDLMessage* message) {
        if (message == nullptr) {
            throw std::runtime_error("SDK delivered a null message");
        }
        const mdl::MDLMessageHead* const head =
            message->GetHead();
        if (head == nullptr) {
            throw std::runtime_error(
                "SDK delivered a message with a null head");
        }

        const auto expected_market_message =
            std::find_if(
                expected_.begin(),
                expected_.end(),
                [head](const sdk::MessageKey& expected) {
                    return head->ServiceID == expected.service_id &&
                           head->ServiceVersion ==
                               expected.service_version &&
                           head->MessageID == expected.message_id;
                });
        if (expected_market_message != expected_.end()) {
            CapturedMarketRecord record;
            if (!DecodeMarketRecord(message, head, &record)) {
                throw std::runtime_error(
                    "SDK delivered a malformed required market message");
            }
            {
                std::lock_guard<std::mutex> lock(mutex_);
                ++market_messages_;
                constexpr std::size_t kMaximumSamples = 16U;
                if (market_samples_.size() < kMaximumSamples) {
                    market_samples_.push_back(record.head);
                }
                if (market_records_.size() < maximum_captured_records_) {
                    market_records_.push_back(std::move(record));
                } else {
                    ++dropped_market_records_;
                    capture_overflow_ = true;
                }
            }
            condition_.notify_all();
            return;
        }
        if (head->ServiceID != mdl::MDLSID_MDL_SYS ||
            head->MessageID !=
                mdl::mdl_sys_msg::LogonResponse::MessageID) {
            return;
        }

        const std::uint32_t head_bytes = head->HeadSize;
        if (head->MessageSize < head_bytes ||
            head->MessageSize - head_bytes <
                sizeof(mdl::mdl_sys_msg::LogonResponse)) {
            throw std::runtime_error(
                "SDK delivered a truncated LogonResponse");
        }
        const char* const raw_body = message->GetBody();
        if (raw_body == nullptr) {
            throw std::runtime_error(
                "SDK delivered a LogonResponse with a null body");
        }
        const auto* const response =
            reinterpret_cast<
                const mdl::mdl_sys_msg::LogonResponse*>(
                    raw_body);

        constexpr std::uint32_t kMaximumServices = 256U;
        constexpr std::uint32_t kMaximumMessages = 4096U;
        if (response->Services.Length > kMaximumServices) {
            throw std::runtime_error(
                "LogonResponse service list exceeds probe bound");
        }

        std::lock_guard<std::mutex> lock(mutex_);
        return_code_ = response->ReturnCode;
        for (std::uint32_t service_index = 0U;
             service_index < response->Services.Length;
             ++service_index) {
            const auto* const service =
                response->Services[service_index];
            if (service == nullptr ||
                service->Messages.Length > kMaximumMessages) {
                malformed_response_ = true;
                continue;
            }
            for (std::uint32_t message_index = 0U;
                 message_index < service->Messages.Length;
                 ++message_index) {
                const auto* const status =
                    service->Messages[message_index];
                if (status == nullptr) {
                    malformed_response_ = true;
                    continue;
                }
                for (std::size_t expected_index = 0U;
                     expected_index < expected_.size();
                     ++expected_index) {
                    const sdk::MessageKey& expected =
                        expected_[expected_index];
                    if (service->ServiceID !=
                            expected.service_id ||
                        service->ServiceVersion !=
                            expected.service_version ||
                        status->MessageID !=
                            expected.message_id) {
                        continue;
                    }
                    if (status->MessageStatus == mdl::MDLEC_OK) {
                        required_ok_[expected_index] = true;
                    } else {
                        required_failed_[expected_index] = true;
                    }
                }
            }
        }
        logon_received_ = true;
        condition_.notify_all();
    }

    ProbeSnapshot SnapshotLocked() const {
        ProbeSnapshot result;
        result.logon_received = logon_received_;
        result.malformed_response = malformed_response_;
        result.return_code = return_code_;
        result.required_ok =
            static_cast<std::size_t>(
                std::count(
                    required_ok_.begin(),
                    required_ok_.end(),
                    true));
        result.required_failed =
            static_cast<std::size_t>(
                std::count(
                    required_failed_.begin(),
                    required_failed_.end(),
                    true));
        result.market_messages = market_messages_;
        result.dropped_market_records = dropped_market_records_;
        result.capture_overflow = capture_overflow_;
        result.subscription_statuses.reserve(expected_.size());
        for (std::size_t index = 0U;
             index < expected_.size();
             ++index) {
            result.subscription_statuses.push_back(
                SubscriptionStatusSample{
                    expected_[index],
                    required_ok_[index],
                    required_failed_[index]});
        }
        result.market_samples = market_samples_;
        result.market_records = market_records_;
        return result;
    }

    const std::vector<sdk::MessageKey> expected_;
    const std::size_t maximum_captured_records_;
    mutable std::mutex mutex_;
    std::condition_variable condition_;
    std::vector<bool> required_ok_;
    std::vector<bool> required_failed_;
    bool logon_received_ = false;
    bool malformed_response_ = false;
    bool capture_overflow_ = false;
    std::uint32_t return_code_ = 0U;
    std::uint64_t market_messages_ = 0U;
    std::uint64_t dropped_market_records_ = 0U;
    std::vector<MarketMessageSample> market_samples_;
    std::vector<CapturedMarketRecord> market_records_;
};

void AppendError(std::string* destination, std::string message) {
    if (message.empty()) {
        return;
    }
    if (!destination->empty()) {
        destination->append("; ");
    }
    destination->append(std::move(message));
}

bool StopAndRelease(
    std::unique_ptr<sdk::SdkManager>* manager,
    std::unique_ptr<sdk::SdkSubscriber>* subscriber,
    std::string* error) noexcept {
    if (*manager == nullptr) {
        return true;
    }
    try {
        (*manager)->Shutdown();
    } catch (const std::exception& exception) {
        AppendError(
            error,
            std::string("IOManager Shutdown threw: ") +
                exception.what());
        static_cast<void>(subscriber->release());
        static_cast<void>(manager->release());
        return false;
    } catch (...) {
        AppendError(
            error,
            "IOManager Shutdown threw an unknown exception");
        static_cast<void>(subscriber->release());
        static_cast<void>(manager->release());
        return false;
    }

    bool success = true;
    if (*subscriber != nullptr) {
        std::string release_error;
        if (!(*subscriber)->Release(&release_error)) {
            AppendError(error, std::move(release_error));
            success = false;
        }
        subscriber->reset();
    }
    std::string release_error;
    if (!(*manager)->Release(&release_error)) {
        AppendError(error, std::move(release_error));
        success = false;
    }
    manager->reset();
    return success;
}

std::string SafeDiagnostic(std::string message) {
    constexpr std::size_t kMaximumBytes = 4096U;
    if (message.size() > kMaximumBytes) {
        message.resize(kMaximumBytes);
        message.append("...");
    }
    for (char& character : message) {
        const unsigned char value =
            static_cast<unsigned char>(character);
        if (value < 0x20U || value == 0x7fU) {
            character = ' ';
        }
    }
    return message;
}

void WriteCsvValue(std::ostream& output, std::string_view value) {
    const bool quoted =
        value.find_first_of(",\"\r\n") != std::string_view::npos;
    if (!quoted) {
        output << value;
        return;
    }
    output.put('"');
    for (const char character : value) {
        if (character == '"') {
            output.put('"');
        }
        output.put(character);
    }
    output.put('"');
}

bool WriteCaptureCsv(
    const std::filesystem::path& path,
    const std::vector<CapturedMarketRecord>& records,
    std::string* error) {
    std::error_code filesystem_error;
    const bool exists = std::filesystem::exists(path, filesystem_error);
    if (filesystem_error) {
        *error = "cannot inspect capture CSV path: " +
                 filesystem_error.message();
        return false;
    }
    if (exists) {
        *error = "refusing to overwrite existing capture CSV: " +
                 path.string();
        return false;
    }

    std::ofstream output(path, std::ios::out | std::ios::trunc);
    if (!output.is_open()) {
        *error = "cannot create capture CSV: " + path.string();
        return false;
    }
    output
        << "CaptureIndex,ServiceID,ServiceVersion,MessageID,MessageType,"
           "HeadSize,MessageSize,MessageEncoding,LocalTime,SeqNo,EventTime,"
           "SecurityID,MDStreamID,SecurityIDSource,TradingPhaseCode,"
           "EventType,TickBSFlag,ChannelNo,ApplicationSequence,"
           "BidApplicationSequence,OfferApplicationSequence,Side,OrderType,"
           "ExecutionType,Price,Quantity,TradeCount,Volume,Turnover,"
           "PreClosePrice,OpenPrice,HighPrice,LowPrice,LastPrice,"
           "TotalBidQuantity,WeightedAverageBidPrice,TotalOfferQuantity,"
           "WeightedAverageOfferPrice\n";

    for (std::size_t index = 0U; index < records.size(); ++index) {
        const CapturedMarketRecord& record = records[index];
        output << index << ','
               << static_cast<unsigned int>(record.head.service_id) << ','
               << record.head.service_version << ','
               << record.head.message_id << ',';
        WriteCsvValue(output, record.message_type);
        output << ','
               << static_cast<unsigned int>(record.head.head_size) << ','
               << record.head.message_size << ','
               << static_cast<unsigned int>(
                      record.head.message_encoding)
               << ',';
        WriteCsvValue(output, FormatTime(record.head.local_time));
        output << ',' << record.head.sequence_id;

        const auto field = [&output](std::string_view value) {
            output.put(',');
            WriteCsvValue(output, value);
        };
        field(record.event_time);
        field(record.security_id);
        field(record.md_stream_id);
        field(record.security_id_source);
        field(record.trading_phase_code);
        field(record.event_type);
        field(record.tick_bs_flag);
        field(record.channel_no);
        field(record.application_sequence);
        field(record.bid_application_sequence);
        field(record.offer_application_sequence);
        field(record.side);
        field(record.order_type);
        field(record.execution_type);
        field(record.price);
        field(record.quantity);
        field(record.trade_count);
        field(record.volume);
        field(record.turnover);
        field(record.pre_close_price);
        field(record.open_price);
        field(record.high_price);
        field(record.low_price);
        field(record.last_price);
        field(record.total_bid_quantity);
        field(record.weighted_average_bid_price);
        field(record.total_offer_quantity);
        field(record.weighted_average_offer_price);
        output.put('\n');
        if (!output) {
            *error = "failed while writing capture CSV: " + path.string();
            return false;
        }
    }
    output.close();
    if (!output) {
        *error = "failed to close capture CSV: " + path.string();
        return false;
    }
    error->clear();
    return true;
}

int Run(const Options& options) {
    const std::vector<sdk::MessageKey> expected =
        RequiredMessages();
    ProbeHandler handler(
        expected,
        static_cast<std::size_t>(options.maximum_captured_records));

    std::string failure;
    std::string loader_error;
    std::shared_ptr<sdk::SdkFactory> factory =
        sdk::LoadApprovedSdkFactory(
            options.library, &loader_error);
    if (factory == nullptr) {
        std::cerr << "mdl-sdk-feeder-probe: "
                  << SafeDiagnostic(loader_error) << '\n';
        return 1;
    }

    std::unique_ptr<sdk::SdkManager> manager;
    std::unique_ptr<sdk::SdkSubscriber> subscriber;
    ProbeSnapshot snapshot;
    try {
        manager = factory->Create(1, 1);
        if (manager == nullptr) {
            failure = "SdkFactory::Create returned null";
        } else {
            manager->EnableLog(options.sdk_log_prefix, false);
            subscriber =
                manager->CreateSubscriber(&handler, false);
            if (subscriber == nullptr) {
                failure =
                    "SdkManager::CreateSubscriber returned null";
            } else {
                subscriber->SetServerAddress(options.address);
                subscriber->SetUserName(
                    "l2flow-local-feeder-probe");
                subscriber->SetHeartbeatInterval(10U);
                subscriber->SetHeartbeatTimeout(30U);
                subscriber->SetMessageEncoding(
                    mdl::MDLEID_BINARY);
                subscriber->EnableMergeMessage(false);
                subscriber->SetSendMacAuth(false);
                subscriber->EnableServerSelect(false);
                for (const sdk::MessageKey& key : expected) {
                    subscriber->AddSubscription(key);
                }

                const std::string connect_error =
                    subscriber->Connect();
                if (!connect_error.empty()) {
                    failure =
                        "SDK Connect failed: " + connect_error;
                } else {
                    snapshot = handler.WaitForLogon(
                        std::chrono::seconds(
                            options.timeout_seconds));
                    if (!snapshot.logon_received) {
                        failure =
                            "timed out waiting for LogonResponse";
                    } else if (snapshot.malformed_response) {
                        failure = "received a malformed LogonResponse";
                    } else if (
                        snapshot.return_code != mdl::MDLEC_OK) {
                        failure =
                            "LogonResponse returned code " +
                            std::to_string(snapshot.return_code);
                    } else if (
                        snapshot.required_failed != 0U ||
                        snapshot.required_ok != expected.size()) {
                        failure =
                            "required subscription status mismatch: ok=" +
                            std::to_string(snapshot.required_ok) +
                            " failed=" +
                            std::to_string(
                                snapshot.required_failed) +
                            " expected=" +
                            std::to_string(expected.size());
                    } else {
                        if (options.monitor_seconds != 0U) {
                            snapshot = handler.MonitorMarketMessages(
                                std::chrono::seconds(
                                    options.monitor_seconds));
                        } else {
                            snapshot = handler.WaitForMarketMessages(
                                options.minimum_market_messages,
                                std::chrono::seconds(
                                    options.market_timeout_seconds));
                        }
                        if (snapshot.malformed_response) {
                            failure =
                                "received a malformed SDK message while "
                                "waiting for market data";
                        } else if (snapshot.capture_overflow) {
                            failure = "market capture capacity exceeded: "
                                      "maximum=" +
                                      std::to_string(
                                          options.maximum_captured_records) +
                                      " dropped=" +
                                      std::to_string(
                                          snapshot.dropped_market_records);
                        } else if (
                            snapshot.market_messages <
                            options.minimum_market_messages) {
                            failure =
                                "timed out waiting for required market "
                                "messages: observed=" +
                                std::to_string(
                                    snapshot.market_messages) +
                                " required=" +
                                std::to_string(
                                    options.minimum_market_messages) +
                                " wait_seconds=" +
                                std::to_string(
                                    options.monitor_seconds == 0U
                                        ? options.market_timeout_seconds
                                        : options.monitor_seconds);
                        }
                    }
                }
            }
        }
    } catch (const std::exception& exception) {
        failure =
            std::string("SDK call threw: ") +
            exception.what();
    } catch (...) {
        failure = "SDK call threw an unknown exception";
    }

    std::string cleanup_error;
    if (!StopAndRelease(
            &manager, &subscriber, &cleanup_error)) {
        AppendError(&failure, std::move(cleanup_error));
    }
    snapshot = handler.Snapshot();
    if (failure.empty() && snapshot.capture_overflow) {
        failure = "market capture capacity exceeded after wait: maximum="
                  + std::to_string(options.maximum_captured_records) +
                  " dropped=" +
                  std::to_string(snapshot.dropped_market_records);
    }
    if (failure.empty() && !options.capture_csv.empty()) {
        std::string csv_error;
        if (!WriteCaptureCsv(
                options.capture_csv,
                snapshot.market_records,
                &csv_error)) {
            failure = std::move(csv_error);
        }
    }

    if (!failure.empty()) {
        std::cerr << "mdl-sdk-feeder-probe: "
                  << SafeDiagnostic(std::move(failure))
                  << '\n';
        return 1;
    }

    std::cout
        << "{\"connected\":true"
        << ",\"logon_return_code\":"
        << snapshot.return_code
        << ",\"required_subscriptions_ok\":"
        << snapshot.required_ok
        << ",\"required_subscriptions_failed\":"
        << snapshot.required_failed
        << ",\"subscription_statuses\":[";
    for (std::size_t index = 0U;
         index < snapshot.subscription_statuses.size();
         ++index) {
        if (index != 0U) {
            std::cout << ',';
        }
        const SubscriptionStatusSample& status =
            snapshot.subscription_statuses[index];
        std::cout
            << "{\"service_id\":"
            << static_cast<unsigned int>(status.key.service_id)
            << ",\"service_version\":"
            << status.key.service_version
            << ",\"message_id\":"
            << status.key.message_id
            << ",\"status\":\""
            << (status.ok ? "ok" :
                (status.failed ? "failed" : "missing"))
            << "\"}";
    }
    std::cout
        << "]"
        << ",\"market_messages_observed\":"
        << snapshot.market_messages
        << ",\"monitor_seconds\":"
        << options.monitor_seconds
        << ",\"capture_capacity\":"
        << options.maximum_captured_records
        << ",\"captured_market_records\":"
        << snapshot.market_records.size()
        << ",\"dropped_market_records\":"
        << snapshot.dropped_market_records
        << ",\"capture_csv_written\":"
        << (!options.capture_csv.empty() ? "true" : "false")
        << ",\"samples\":[";
    for (std::size_t index = 0U;
         index < snapshot.market_samples.size();
         ++index) {
        if (index != 0U) {
            std::cout << ',';
        }
        const MarketMessageSample& sample =
            snapshot.market_samples[index];
        std::cout
            << "{\"head_size\":"
            << static_cast<unsigned int>(sample.head_size)
            << ",\"message_size\":"
            << sample.message_size
            << ",\"message_encoding\":"
            << static_cast<unsigned int>(sample.message_encoding)
            << ",\"service_id\":"
            << static_cast<unsigned int>(sample.service_id)
            << ",\"service_version\":"
            << sample.service_version
            << ",\"message_id\":"
            << sample.message_id
            << ",\"local_time\":"
            << sample.local_time
            << ",\"sequence_id\":"
            << sample.sequence_id
            << '}';
    }
    std::cout
        << "]"
        << "}\n";
    return 0;
}

}  // namespace

int main(int argc, char* argv[]) {
    Options options;
    bool show_help = false;
    std::string error;
    if (!ParseOptions(
            argc, argv, &options, &show_help, &error)) {
        std::cerr << "mdl-sdk-feeder-probe: "
                  << SafeDiagnostic(std::move(error))
                  << '\n';
        PrintUsage(std::cerr);
        return 2;
    }
    if (show_help) {
        PrintUsage(std::cout);
        return 0;
    }
    try {
        return Run(options);
    } catch (const std::exception& exception) {
        std::cerr
            << "mdl-sdk-feeder-probe: fatal: "
            << SafeDiagnostic(exception.what())
            << '\n';
        return 2;
    } catch (...) {
        std::cerr
            << "mdl-sdk-feeder-probe: unknown fatal exception\n";
        return 2;
    }
}
