#include "l2flow/ingress/raw_reader.h"
#include "l2flow/sdk/subscription_manifest.h"

#include "mdl_api.h"
#include "mdl_shl2_msg.h"
#include "mdl_szl2_msg.h"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace {

namespace ingress = l2flow::ingress;
namespace mdl = datayes::mdl;
namespace sh = datayes::mdl::mdl_shl2_msg;
namespace sz = datayes::mdl::mdl_szl2_msg;

constexpr std::uint64_t kMaximumSegmentBytes =
    UINT64_C(4) * UINT64_C(1024) * UINT64_C(1024) * UINT64_C(1024);

struct Input final {
    std::filesystem::path relative_path;
    std::uint64_t durable_end = 0U;
};

struct Options final {
    std::filesystem::path root;
    std::filesystem::path output;
    std::vector<Input> inputs;
    std::uint64_t monotonic_begin = 0U;
    std::uint64_t monotonic_end = 0U;
};

struct CapturedMarketRecord final {
    std::uint8_t head_size = 0U;
    std::uint32_t message_size = 0U;
    std::uint8_t message_encoding = 0U;
    std::uint8_t service_id = 0U;
    std::uint16_t service_version = 0U;
    std::uint16_t message_id = 0U;
    std::uint32_t local_time = 0U;
    std::uint64_t sequence_id = 0U;
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

[[nodiscard]] bool ParseU64(std::string_view text,
                            std::uint64_t* output) noexcept {
    if (output == nullptr || text.empty()) {
        return false;
    }
    std::uint64_t value = 0U;
    const auto parsed = std::from_chars(
        text.data(), text.data() + text.size(), value, 10);
    if (parsed.ec != std::errc{} ||
        parsed.ptr != text.data() + text.size()) {
        return false;
    }
    *output = value;
    return true;
}

[[nodiscard]] bool SafeRelativePath(const std::filesystem::path& path) {
    if (path.empty() || path.is_absolute() || path.filename().empty()) {
        return false;
    }
    for (const auto& component : path) {
        if (component == "." || component == ".." || component.empty()) {
            return false;
        }
    }
    return path.native().find('\0') == std::string::npos;
}

[[nodiscard]] bool ParseOptions(int argc,
                                const char* const argv[],
                                Options* output,
                                std::string* error) {
    if (output == nullptr || error == nullptr || argc < 0 ||
        (argc > 0 && argv == nullptr)) {
        return false;
    }
    Options parsed;
    bool root_seen = false;
    bool output_seen = false;
    bool range_seen = false;
    for (int index = 1; index < argc; ++index) {
        if (argv[index] == nullptr) {
            *error = "argv contains null";
            return false;
        }
        const std::string_view option(argv[index]);
        if (option == "--help") {
            *error = "help";
            return false;
        }
        if (index + 1 >= argc || argv[index + 1] == nullptr) {
            *error = "option is missing a value";
            return false;
        }
        const std::string_view value(argv[++index]);
        if (option == "--root" && !root_seen) {
            parsed.root = value;
            root_seen = true;
        } else if (option == "--output" && !output_seen) {
            parsed.output = value;
            output_seen = true;
        } else if (option == "--monotonic-range" && !range_seen) {
            const std::size_t separator = value.find(':');
            if (separator == std::string_view::npos ||
                value.find(':', separator + 1U) != std::string_view::npos ||
                !ParseU64(value.substr(0U, separator),
                          &parsed.monotonic_begin) ||
                !ParseU64(value.substr(separator + 1U),
                          &parsed.monotonic_end) ||
                parsed.monotonic_begin >= parsed.monotonic_end) {
                *error = "invalid --monotonic-range";
                return false;
            }
            range_seen = true;
        } else if (option == "--segment") {
            if (index + 1 >= argc || argv[index + 1] == nullptr) {
                *error = "--segment requires PATH and DURABLE_END";
                return false;
            }
            Input input;
            input.relative_path = value;
            if (!SafeRelativePath(input.relative_path) ||
                !ParseU64(argv[++index], &input.durable_end) ||
                input.durable_end < ingress::kRawV1SegmentHeaderBytes ||
                input.durable_end > kMaximumSegmentBytes) {
                *error = "invalid --segment input";
                return false;
            }
            parsed.inputs.push_back(std::move(input));
        } else {
            *error = "unknown or duplicate option";
            return false;
        }
    }
    if (!root_seen || !output_seen || !range_seen || parsed.inputs.empty() ||
        !parsed.root.is_absolute() || !parsed.output.is_absolute() ||
        parsed.root.native().find('\0') != std::string::npos ||
        parsed.output.native().find('\0') != std::string::npos) {
        *error = "required arguments are missing or invalid";
        return false;
    }
    *output = std::move(parsed);
    error->clear();
    return true;
}

[[nodiscard]] bool ReadInput(const Options& options,
                             const Input& input,
                             std::shared_ptr<const std::vector<std::byte>>*
                                 output,
                             std::string* error) {
    const std::filesystem::path path = options.root / input.relative_path;
    const int descriptor = ::open(
        path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    if (descriptor < 0) {
        *error = "cannot open Raw segment: " + path.string();
        return false;
    }
    struct Close final {
        int fd;
        ~Close() { static_cast<void>(::close(fd)); }
    } close{descriptor};
    struct stat metadata {};
    if (::fstat(descriptor, &metadata) != 0 ||
        !S_ISREG(metadata.st_mode) || metadata.st_size < 0 ||
        input.durable_end > static_cast<std::uint64_t>(metadata.st_size) ||
        input.durable_end >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max())) {
        *error = "Raw segment metadata/durable end is invalid: " +
                 path.string();
        return false;
    }
    auto bytes = std::make_shared<std::vector<std::byte>>(
        static_cast<std::size_t>(input.durable_end));
    std::size_t offset = 0U;
    while (offset < bytes->size()) {
        const ssize_t count = ::pread(
            descriptor,
            bytes->data() + offset,
            bytes->size() - offset,
            static_cast<off_t>(offset));
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            *error = "Raw segment durable prefix read failed: " +
                     path.string();
            return false;
        }
        offset += static_cast<std::size_t>(count);
    }
    *output = std::move(bytes);
    return true;
}

std::string FormatTime(std::uint32_t value) {
    std::string digits = std::to_string(value);
    if (digits.size() < 9U) {
        digits.insert(0U, 9U - digits.size(), '0');
    }
    if (digits.size() != 9U) {
        return {};
    }
    return digits.substr(0U, 2U) + ":" + digits.substr(2U, 2U) +
           ":" + digits.substr(4U, 2U) + "." + digits.substr(6U, 3U);
}

[[nodiscard]] bool FormatTime(const mdl::MDLTime& value,
                              std::string* output) {
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
            digits.insert(0U, decimal_places + 1U - digits.size(), '0');
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
        static_cast<std::int64_t>(value.m_Value), value.GetDecimalPlace());
}

[[nodiscard]] bool CopyBoundedString(
    std::span<const std::byte> body,
    const mdl::MDLAnsiString& value,
    std::string* output) {
    const std::uintptr_t body_address =
        reinterpret_cast<std::uintptr_t>(body.data());
    const std::uintptr_t field_address =
        reinterpret_cast<std::uintptr_t>(&value);
    if (field_address < body_address) {
        return false;
    }
    const std::size_t field_offset =
        static_cast<std::size_t>(field_address - body_address);
    if (field_offset > body.size() ||
        sizeof(value) > body.size() - field_offset) {
        return false;
    }
    if (value.Length == 0U) {
        output->clear();
        return true;
    }
    if (value.Offset == 0U || value.Offset > body.size() - field_offset) {
        return false;
    }
    const std::size_t string_offset = field_offset + value.Offset;
    if (value.Length > body.size() - string_offset) {
        return false;
    }
    output->assign(
        reinterpret_cast<const char*>(body.data() + string_offset),
        value.Length);
    return true;
}

template <typename Body>
[[nodiscard]] const Body* CheckedBody(std::span<const std::byte> body) {
    if (body.size() < sizeof(Body)) {
        return nullptr;
    }
    return reinterpret_cast<const Body*>(body.data());
}

[[nodiscard]] bool DecodeMarketRecord(
    const ingress::RawRecordView& input,
    CapturedMarketRecord* output) {
    if (output == nullptr) {
        return false;
    }
    const auto& head = input.header();
    const std::span<const std::byte> body_bytes = input.vendor_body();
    output->head_size = head.vendor_head_size;
    output->message_size = head.vendor_message_size;
    output->message_encoding = head.vendor_message_encoding;
    output->service_id = head.vendor_service_id;
    output->service_version = head.vendor_service_version;
    output->message_id = head.vendor_message_id;
    output->local_time = head.vendor_local_time_raw;
    output->sequence_id = head.vendor_sequence_id;

    if (head.vendor_service_id == sh::SHL2MarketData::ServiceID &&
        head.vendor_service_version == sh::SHL2MarketData::ServiceVer &&
        head.vendor_message_id == sh::SHL2MarketData::MessageID) {
        const auto* body = CheckedBody<sh::SHL2MarketData>(body_bytes);
        if (body == nullptr || !FormatTime(body->UpdateTime, &output->event_time) ||
            !CopyBoundedString(body_bytes, body->SecurityID,
                               &output->security_id) ||
            !CopyBoundedString(body_bytes, body->InstruStatus,
                               &output->event_type)) {
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
    if (head.vendor_service_id == sh::NGTSTick::ServiceID &&
        head.vendor_service_version == sh::NGTSTick::ServiceVer &&
        head.vendor_message_id == sh::NGTSTick::MessageID) {
        const auto* body = CheckedBody<sh::NGTSTick>(body_bytes);
        if (body == nullptr || !FormatTime(body->TickTime, &output->event_time) ||
            !CopyBoundedString(body_bytes, body->SecurityID,
                               &output->security_id) ||
            !CopyBoundedString(body_bytes, body->Type, &output->event_type) ||
            !CopyBoundedString(body_bytes, body->TickBSFlag,
                               &output->tick_bs_flag)) {
            return false;
        }
        output->message_type = "NGTSTick";
        output->application_sequence = std::to_string(body->BizIndex);
        output->channel_no = std::to_string(body->Channel);
        output->bid_application_sequence = std::to_string(body->BuyOrderNO);
        output->offer_application_sequence =
            std::to_string(body->SellOrderNO);
        output->price = FormatFixed(body->Price);
        output->quantity = std::to_string(body->Qty);
        output->turnover = FormatFixed(body->TradeMoney);
        return true;
    }
    if (head.vendor_service_id == sz::Snapshot300111_v2::ServiceID &&
        head.vendor_service_version == sz::Snapshot300111_v2::ServiceVer &&
        head.vendor_message_id == sz::Snapshot300111_v2::MessageID) {
        const auto* body = CheckedBody<sz::Snapshot300111_v2>(body_bytes);
        if (body == nullptr || !FormatTime(body->UpdateTime, &output->event_time) ||
            !CopyBoundedString(body_bytes, body->MDStreamID,
                               &output->md_stream_id) ||
            !CopyBoundedString(body_bytes, body->SecurityID,
                               &output->security_id) ||
            !CopyBoundedString(body_bytes, body->SecurityIDSource,
                               &output->security_id_source) ||
            !CopyBoundedString(body_bytes, body->TradingPhaseCode,
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
    if (head.vendor_service_id == sz::Order300192_v2::ServiceID &&
        head.vendor_service_version == sz::Order300192_v2::ServiceVer &&
        head.vendor_message_id == sz::Order300192_v2::MessageID) {
        const auto* body = CheckedBody<sz::Order300192_v2>(body_bytes);
        if (body == nullptr ||
            !FormatTime(body->TransactTime, &output->event_time) ||
            !CopyBoundedString(body_bytes, body->MDStreamID,
                               &output->md_stream_id) ||
            !CopyBoundedString(body_bytes, body->SecurityID,
                               &output->security_id) ||
            !CopyBoundedString(body_bytes, body->SecurityIDSource,
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
    if (head.vendor_service_id == sz::Transaction300191_v2::ServiceID &&
        head.vendor_service_version == sz::Transaction300191_v2::ServiceVer &&
        head.vendor_message_id == sz::Transaction300191_v2::MessageID) {
        const auto* body = CheckedBody<sz::Transaction300191_v2>(body_bytes);
        if (body == nullptr ||
            !FormatTime(body->TransactTime, &output->event_time) ||
            !CopyBoundedString(body_bytes, body->MDStreamID,
                               &output->md_stream_id) ||
            !CopyBoundedString(body_bytes, body->SecurityID,
                               &output->security_id) ||
            !CopyBoundedString(body_bytes, body->SecurityIDSource,
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

[[nodiscard]] bool IsRequiredMarketKey(
    const ingress::RawRecordHeaderV1& head) {
    const l2flow::sdk::MessageKey key{
        head.vendor_service_id,
        head.vendor_service_version,
        head.vendor_message_id};
    for (const auto& spec : l2flow::sdk::AllIngressSpecs()) {
        if (std::find(spec.required.begin(), spec.required.end(), key) !=
            spec.required.end()) {
            return true;
        }
    }
    return false;
}

void WriteCsvValue(std::ostream& output, std::string_view value) {
    if (value.find_first_of(",\"\r\n") == std::string_view::npos) {
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

void WriteHeader(std::ostream& output) {
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
}

void WriteRecord(std::ostream& output,
                 std::uint64_t index,
                 const CapturedMarketRecord& record) {
    output << index << ',' << static_cast<unsigned int>(record.service_id)
           << ',' << record.service_version << ',' << record.message_id
           << ',';
    WriteCsvValue(output, record.message_type);
    output << ',' << static_cast<unsigned int>(record.head_size) << ','
           << record.message_size << ','
           << static_cast<unsigned int>(record.message_encoding) << ',';
    WriteCsvValue(output, FormatTime(record.local_time));
    output << ',' << record.sequence_id;
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
}

[[nodiscard]] bool Export(const Options& options, std::string* error) {
    std::error_code filesystem_error;
    if (std::filesystem::exists(options.output, filesystem_error) ||
        filesystem_error) {
        *error = "output exists or cannot be inspected";
        return false;
    }
    const std::filesystem::path temporary =
        options.output.string() + ".tmp";
    if (std::filesystem::exists(temporary, filesystem_error) ||
        filesystem_error) {
        *error = "temporary output exists or cannot be inspected";
        return false;
    }
    std::ofstream output(temporary, std::ios::out | std::ios::trunc);
    if (!output.is_open()) {
        *error = "cannot create temporary output";
        return false;
    }
    WriteHeader(output);
    std::uint64_t scanned_records = 0U;
    std::uint64_t selected_records = 0U;
    for (const Input& input : options.inputs) {
        std::shared_ptr<const std::vector<std::byte>> bytes;
        if (!ReadInput(options, input, &bytes, error)) {
            return false;
        }
        ingress::RawSegmentScanResult scan =
            ingress::ScanRawSegmentV1(bytes, input.durable_end);
        if (!scan.ok() ||
            scan.validated_end_offset != input.durable_end) {
            *error = "Raw segment validation failed at offset " +
                     std::to_string(scan.error_offset);
            return false;
        }
        for (const ingress::RawRecordView& record : scan.records) {
            ++scanned_records;
            const auto& head = record.header();
            if (head.recv_monotonic_ns < options.monotonic_begin ||
                head.recv_monotonic_ns >= options.monotonic_end) {
                continue;
            }
            if (!IsRequiredMarketKey(head)) {
                continue;
            }
            CapturedMarketRecord decoded;
            if (!DecodeMarketRecord(record, &decoded)) {
                *error = "required market Raw record failed normalized decode";
                return false;
            }
            WriteRecord(output, selected_records, decoded);
            ++selected_records;
        }
    }
    output.flush();
    if (!output.good()) {
        *error = "capture CSV write failed";
        return false;
    }
    output.close();
    if (::rename(temporary.c_str(), options.output.c_str()) != 0) {
        *error = "cannot publish capture CSV";
        return false;
    }
    std::cout << "{\"scanned_raw_records\":" << scanned_records
              << ",\"selected_market_records\":" << selected_records
              << ",\"monotonic_begin\":" << options.monotonic_begin
              << ",\"monotonic_end\":" << options.monotonic_end
              << "}\n";
    return true;
}

constexpr std::string_view kUsage =
    "Usage: export-raw-feeder-capture --root ABSOLUTE_RAW_ROOT "
    "--segment RELATIVE_PATH DURABLE_END [--segment ...] "
    "--monotonic-range BEGIN:END --output ABSOLUTE_CSV\n";

}  // namespace

int main(int argc, const char* const argv[]) {
    Options options;
    std::string error;
    if (!ParseOptions(argc, argv, &options, &error)) {
        if (error != "help") {
            std::cerr << "export-raw-feeder-capture: " << error << '\n';
        }
        std::cerr << kUsage;
        return error == "help" ? 0 : 64;
    }
    if (!Export(options, &error)) {
        std::cerr << "export-raw-feeder-capture: " << error << '\n';
        return 70;
    }
    return 0;
}
