#include "l2flow/market/market_decoder.h"
#include "l2flow/market/market_types_v1.h"
#include "l2flow/recovery/startup_replay_v1.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <map>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace market = l2flow::market;
namespace recovery = l2flow::recovery;

namespace {

struct TestContext final {
    void Expect(bool condition, std::string_view detail) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << detail << '\n';
        }
    }
    int failures = 0;
};

class TempDirectory final {
public:
    TempDirectory() {
        const auto nonce =
            std::chrono::steady_clock::now().time_since_epoch().count();
        path_ = std::filesystem::temp_directory_path() /
                ("l2flow_csv_replay_" + std::to_string(nonce));
        std::filesystem::create_directories(path_);
    }
    ~TempDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }
    [[nodiscard]] const std::filesystem::path& path() const {
        return path_;
    }

private:
    std::filesystem::path path_;
};

std::vector<std::string> ShanghaiSnapshotColumns() {
    std::vector<std::string> result{
        "UpdateTime", "SecurityID", "ImageStatus",
        "PreCloPrice", "OpenPrice", "HighPrice", "LowPrice",
        "LastPrice", "ClosePrice", "InstruStatus", "TradNumber",
        "TradVolume", "Turnover", "TotalBidVol", "WAvgBidPri",
        "AltWAvgBidPri", "TotalAskVol", "WAvgAskPri",
        "AltWAvgAskPri", "EtfBuyNumber", "EtfBuyVolume",
        "EtfBuyMoney", "EtfSellNumber", "EtfSellVolume",
        "ETFSellMoney", "YieldToMatu", "TotWarExNum",
        "WarLowerPri", "WarUpperPri", "WiDBuyNum", "WiDBuyVol",
        "WiDBuyMon", "WiDSellNum", "WiDSellVol", "WiDSellMon",
        "TotBidNum", "TotSellNum", "MaxBidDur", "MaxSellDur",
        "BidNum", "SellNum", "IOPV"};
    for (int index = 1; index <= 10; ++index) {
        result.push_back("AskPrice" + std::to_string(index));
        result.push_back("AskVolume" + std::to_string(index));
    }
    for (int index = 1; index <= 10; ++index) {
        result.push_back("BidPrice" + std::to_string(index));
        result.push_back("BidVolume" + std::to_string(index));
    }
    for (int index = 1; index <= 10; ++index) {
        result.push_back("NumOrdersB" + std::to_string(index));
    }
    for (int index = 1; index <= 10; ++index) {
        result.push_back("NumOrdersS" + std::to_string(index));
    }
    result.push_back("LocalTime");
    result.push_back("SeqNo");
    return result;
}

std::vector<std::string> QueueColumns(std::string time_column) {
    std::vector<std::string> result{
        std::move(time_column), "SecurityID", "ImageStatus", "Side",
        "NoPriceLevel", "PrcLvlOperator", "Price", "Volume",
        "NumOrders", "NoOrders"};
    for (int index = 1; index <= 50; ++index) {
        result.push_back("OrderQty" + std::to_string(index));
    }
    result.push_back("LocalTime");
    result.push_back("SeqNo");
    return result;
}

std::vector<std::string> ShenzhenSnapshotColumns() {
    std::vector<std::string> result{
        "UpdateTime", "MDStreamID", "SecurityID",
        "SecurityIDSource", "TradingPhaseCode", "PreCloPrice",
        "TurnNum", "Volume", "Turnover", "LastPrice", "OpenPrice",
        "HighPrice", "LowPrice", "DifPrice1", "DifPrice2", "PE1",
        "PE2", "PreCloseIOPV", "IOPV", "TotalBidQty",
        "WeightedAvgBidPx", "TotalOfferQty", "WeightedAvgOfferPx",
        "HighLimitPrice", "LowLimitPrice", "OpenInt",
        "OptPremiumRatio"};
    for (int index = 1; index <= 10; ++index) {
        result.push_back("AskPrice" + std::to_string(index));
        result.push_back("AskVolume" + std::to_string(index));
    }
    for (int index = 1; index <= 10; ++index) {
        result.push_back("BidPrice" + std::to_string(index));
        result.push_back("BidVolume" + std::to_string(index));
    }
    for (int index = 1; index <= 10; ++index) {
        result.push_back("NumOrdersB" + std::to_string(index));
    }
    for (int index = 1; index <= 10; ++index) {
        result.push_back("NumOrdersS" + std::to_string(index));
    }
    result.push_back("LocalTime");
    result.push_back("SeqNo");
    return result;
}

const std::vector<std::string> kShanghaiTickColumns{
    "BizIndex", "Channel", "SecurityID", "TickTime", "Type",
    "BuyOrderNO", "SellOrderNO", "Price", "Qty", "TradeMoney",
    "TickBSFlag", "LocalTime", "SeqNo"};
const std::vector<std::string> kShenzhenOrderColumns{
    "ChannelNo", "ApplSeqNum", "MDStreamID", "SecurityID",
    "SecurityIDSource", "Price", "OrderQty", "Side",
    "TransactTime", "OrdType", "LocalTime", "SeqNo"};
const std::vector<std::string> kShenzhenTransactionColumns{
    "ChannelNo", "ApplSeqNum", "MDStreamID", "BidApplSeqNum",
    "OfferApplSeqNum", "SecurityID", "SecurityIDSource", "LastPx",
    "LastQty", "ExecType", "TransactTime", "LocalTime", "SeqNo"};

using Row = std::map<std::string, std::string>;

Row ZeroRow(const std::vector<std::string>& columns) {
    Row row;
    for (const std::string& column : columns) {
        row[column] = "0";
    }
    return row;
}

std::string EscapeCsv(std::string_view value) {
    if (value.find_first_of(",\"\r\n") == std::string_view::npos) {
        return std::string(value);
    }
    std::string result{"\""};
    for (char character : value) {
        if (character == '"') {
            result.push_back('"');
        }
        result.push_back(character);
    }
    result.push_back('"');
    return result;
}

std::string CsvLine(
    const std::vector<std::string>& columns,
    const Row& row) {
    std::string result;
    for (std::size_t index = 0U; index < columns.size(); ++index) {
        if (index != 0U) {
            result.push_back(',');
        }
        const auto found = row.find(columns[index]);
        result += EscapeCsv(
            found == row.end() ? std::string_view{}
                               : std::string_view(found->second));
    }
    result.push_back('\n');
    return result;
}

void WriteTable(
    const std::filesystem::path& path,
    const std::vector<std::string>& columns,
    const std::vector<Row>& rows,
    std::string_view suffix = {}) {
    std::ofstream output(path, std::ios::binary);
    Row header;
    for (const std::string& column : columns) {
        header[column] = column;
    }
    output << CsvLine(columns, header);
    for (const Row& row : rows) {
        output << CsvLine(columns, row);
    }
    output << suffix;
}

class DecodeSink final : public recovery::StartupReplaySinkV1 {
public:
    DecodeSink()
        : decoder_(market::MarketDecoderConfigV1{
              20260731U, 77U, {}}) {}

    bool CaptureTupleFence(
        const l2flow::sdk::MessageKey& key,
        std::string* detail) noexcept override {
        fences.push_back(key);
        if (!fence_hook) {
            return true;
        }
        try {
            return fence_hook(key, detail);
        } catch (...) {
            if (detail != nullptr) {
                *detail = "test fence hook threw";
            }
            return false;
        }
    }

    bool CooperativeCheckpoint(
        std::string* detail) noexcept override {
        ++cooperative_checkpoint_calls;
        if (!cooperative_checkpoint_hook) {
            return true;
        }
        try {
            return cooperative_checkpoint_hook(detail);
        } catch (...) {
            if (detail != nullptr) {
                *detail = "test cooperative checkpoint hook threw";
            }
            return false;
        }
    }

    bool Publish(
        const recovery::StartupReplayPublicationV1& publication,
        std::string* detail) noexcept override {
        if (publication.message == nullptr) {
            *detail = "null message";
            return false;
        }
        const auto* head = publication.message->GetHead();
        if (head == nullptr ||
            head->MessageSize < head->HeadSize ||
            publication.message->GetBody() == nullptr) {
            *detail = "invalid short-lived message";
            return false;
        }
        market::MarketMessageViewV1 view;
        view.source_stream_id = 77U;
        view.trade_date = 20260731U;
        view.source_sequence = ++source_sequence_;
        view.service_id = head->ServiceID;
        view.service_version = head->ServiceVersion;
        view.message_id = head->MessageID;
        view.message_encoding = head->MessageEncoding;
        view.vendor_local_time_raw = head->LocalTime.m_Value;
        view.vendor_sequence_id = head->SequenceID;
        view.recv_realtime_ns = 1;
        view.recv_monotonic_ns = 1;
        view.body = std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(
                publication.message->GetBody()),
            head->MessageSize - head->HeadSize);
        market::DecodedMarketEventV1 decoded;
        const market::MarketDecodeErrorV1 error =
            decoder_.Decode(view, &decoded);
        if (error != market::MarketDecodeErrorV1::kNone) {
            *detail = "market decode failed";
            return false;
        }
        events.push_back(std::move(decoded));
        notice_flags.push_back(publication.market_notice_flags);
        publication_keys.push_back(publication.key);
        publication_sequences.push_back(publication.csv_sequence);
        if (publish_hook) {
            try {
                publish_hook(publication);
            } catch (...) {
                *detail = "test publish hook threw";
                return false;
            }
        }
        return true;
    }

    std::vector<market::DecodedMarketEventV1> events;
    std::vector<std::uint64_t> notice_flags;
    std::vector<l2flow::sdk::MessageKey> publication_keys;
    std::vector<std::uint64_t> publication_sequences;
    std::vector<l2flow::sdk::MessageKey> fences;
    std::size_t cooperative_checkpoint_calls = 0U;
    std::function<bool(
        const l2flow::sdk::MessageKey&,
        std::string*)>
        fence_hook;
    std::function<bool(std::string*)>
        cooperative_checkpoint_hook;
    std::function<void(
        const recovery::StartupReplayPublicationV1&)>
        publish_hook;

private:
    market::MarketDecoderV1 decoder_;
    std::uint64_t source_sequence_ = 0U;
};

Row ShenzhenOrderRow(
    std::string channel,
    std::string application_sequence,
    std::string csv_sequence) {
    Row row = ZeroRow(kShenzhenOrderColumns);
    row["ChannelNo"] = std::move(channel);
    row["ApplSeqNum"] = std::move(application_sequence);
    row["MDStreamID"] = "011";
    row["SecurityID"] = "000001";
    row["SecurityIDSource"] = "102";
    row["Price"] = "10.0000";
    row["OrderQty"] = "100";
    row["Side"] = "1";
    row["TransactTime"] = "09:30:00.007";
    row["OrdType"] = "2";
    row["LocalTime"] = "09:30:00.008";
    row["SeqNo"] = std::move(csv_sequence);
    return row;
}

Row ShenzhenTransactionRow(
    std::string channel,
    std::string application_sequence,
    std::string csv_sequence) {
    Row row = ZeroRow(kShenzhenTransactionColumns);
    row["ChannelNo"] = std::move(channel);
    row["ApplSeqNum"] = std::move(application_sequence);
    row["MDStreamID"] = "011";
    row["BidApplSeqNum"] = "1";
    row["OfferApplSeqNum"] = "0";
    row["SecurityID"] = "000001";
    row["SecurityIDSource"] = "102";
    row["LastPx"] = "10.0000";
    row["LastQty"] = "100";
    row["ExecType"] = "F";
    row["TransactTime"] = "09:30:00.009";
    row["LocalTime"] = "09:30:00.010";
    row["SeqNo"] = std::move(csv_sequence);
    return row;
}

void ReplaceFirstInFile(
    const std::filesystem::path& path,
    std::string_view before,
    std::string_view after) {
    std::ifstream input(path, std::ios::binary);
    std::string bytes(
        (std::istreambuf_iterator<char>(input)),
        std::istreambuf_iterator<char>());
    const std::size_t position = bytes.find(before);
    if (position != std::string::npos) {
        bytes.replace(position, before.size(), after);
    }
    std::ofstream output(path, std::ios::binary);
    output << bytes;
}

void PopulateSuccessDirectory(const std::filesystem::path& directory) {
    const std::vector<std::string> sh_snapshot_columns =
        ShanghaiSnapshotColumns();
    Row sh_snapshot = ZeroRow(sh_snapshot_columns);
    sh_snapshot["UpdateTime"] = "09:30:00.001";
    sh_snapshot["SecurityID"] = "600000 ";
    sh_snapshot["ImageStatus"] = "1";
    sh_snapshot["InstruStatus"] = "TRADE";
    sh_snapshot["BidPrice1"] = "10.100";
    sh_snapshot["BidVolume1"] = "300.000";
    sh_snapshot["NumOrdersB1"] = "2";
    sh_snapshot["AskPrice1"] = "10.200";
    sh_snapshot["AskVolume1"] = "400.000";
    sh_snapshot["NumOrdersS1"] = "1";
    sh_snapshot["BidNum"] = "1";
    sh_snapshot["SellNum"] = "1";
    sh_snapshot["LocalTime"] = "09:30:00.002";
    sh_snapshot["SeqNo"] = "100";
    WriteTable(
        directory / "MarketData.csv",
        sh_snapshot_columns,
        {sh_snapshot});

    const std::vector<std::string> sh_queue_columns =
        QueueColumns("UpdateTime");
    Row sh_bid = ZeroRow(sh_queue_columns);
    sh_bid["UpdateTime"] = "09:30:00.001";
    sh_bid["SecurityID"] = "600000 ";
    sh_bid["ImageStatus"] = "1";
    sh_bid["Side"] = "B";
    sh_bid["NoPriceLevel"] = "1";
    sh_bid["Price"] = "10.100";
    sh_bid["Volume"] = "300.000";
    sh_bid["NumOrders"] = "2";
    sh_bid["NoOrders"] = "2";
    sh_bid["OrderQty1"] = "100.000";
    sh_bid["OrderQty2"] = "200.000";
    sh_bid["LocalTime"] = "09:30:00.002";
    sh_bid["SeqNo"] = "100";
    Row sh_ask = sh_bid;
    sh_ask["Side"] = "S";
    sh_ask["Price"] = "10.200";
    sh_ask["Volume"] = "400.000";
    sh_ask["NumOrders"] = "1";
    sh_ask["NoOrders"] = "1";
    sh_ask["OrderQty1"] = "400.000";
    sh_ask["SeqNo"] = "100";
    WriteTable(
        directory / "OrderQueue.csv",
        sh_queue_columns,
        {sh_bid, sh_ask});

    Row sh_tick = ZeroRow(kShanghaiTickColumns);
    sh_tick["BizIndex"] = "0001";
    sh_tick["Channel"] = "1";
    sh_tick["SecurityID"] = "600000 ";
    sh_tick["TickTime"] = "09:30:00.003";
    sh_tick["Type"] = "A";
    sh_tick["BuyOrderNO"] = "10";
    sh_tick["SellOrderNO"] = "0";
    sh_tick["Price"] = "10.100";
    sh_tick["Qty"] = "100";
    sh_tick["TradeMoney"] = "0.000";
    sh_tick["TickBSFlag"] = "B";
    sh_tick["LocalTime"] = "09:30:00.004";
    sh_tick["SeqNo"] = "101";
    WriteTable(
        directory / "mdl_4_24_0.csv",
        kShanghaiTickColumns,
        {sh_tick});

    const std::vector<std::string> sz_snapshot_columns =
        ShenzhenSnapshotColumns();
    Row sz_snapshot = ZeroRow(sz_snapshot_columns);
    sz_snapshot["UpdateTime"] = "09:30:00.005";
    sz_snapshot["MDStreamID"] = "010";
    sz_snapshot["SecurityID"] = "000001";
    sz_snapshot["SecurityIDSource"] = "102";
    sz_snapshot["TradingPhaseCode"] = "T0";
    sz_snapshot["PreCloPrice"] = "10.0000";
    sz_snapshot["BidPrice1"] = "9.999999";
    sz_snapshot["BidVolume1"] = "300";
    sz_snapshot["NumOrdersB1"] = "2";
    sz_snapshot["AskPrice1"] = "10.000001";
    sz_snapshot["AskVolume1"] = "400";
    sz_snapshot["NumOrdersS1"] = "1";
    sz_snapshot["LocalTime"] = "09:30:00.006";
    sz_snapshot["SeqNo"] = "200";
    WriteTable(
        directory / "mdl_6_28_0.csv",
        sz_snapshot_columns,
        {sz_snapshot});

    const std::vector<std::string> sz_queue_columns =
        QueueColumns("DataTimeStamp");
    Row sz_ask = ZeroRow(sz_queue_columns);
    sz_ask["DataTimeStamp"] = "09:30:00.005";
    sz_ask["SecurityID"] = "000001";
    sz_ask["ImageStatus"] = "1";
    sz_ask["Side"] = "S";
    sz_ask["NoPriceLevel"] = "1";
    sz_ask["Price"] = "10.000001";
    sz_ask["Volume"] = "400";
    sz_ask["NumOrders"] = "1";
    sz_ask["NoOrders"] = "1";
    sz_ask["OrderQty1"] = "400";
    sz_ask["LocalTime"] = "09:30:00.006";
    sz_ask["SeqNo"] = "200";
    Row sz_bid = sz_ask;
    sz_bid["Side"] = "B";
    sz_bid["Price"] = "9.999999";
    sz_bid["Volume"] = "300";
    sz_bid["NumOrders"] = "2";
    sz_bid["NoOrders"] = "2";
    sz_bid["OrderQty1"] = "100";
    sz_bid["OrderQty2"] = "200";
    WriteTable(
        directory / "mdl_6_28_1.csv",
        sz_queue_columns,
        {sz_ask});
    WriteTable(
        directory / "mdl_6_28_2.csv",
        sz_queue_columns,
        {sz_bid});

    Row sz_order = ZeroRow(kShenzhenOrderColumns);
    sz_order["ChannelNo"] = "201";
    sz_order["ApplSeqNum"] = "1";
    sz_order["MDStreamID"] = "011";
    sz_order["SecurityID"] = "000001";
    sz_order["SecurityIDSource"] = "102";
    sz_order["Price"] = "10.0000";
    sz_order["OrderQty"] = "100";
    sz_order["Side"] = "1";
    sz_order["TransactTime"] = "09:30:00.007";
    sz_order["OrdType"] = "2";
    sz_order["LocalTime"] = "09:30:00.008";
    sz_order["SeqNo"] = "300";
    WriteTable(
        directory / "mdl_6_33_0.csv",
        kShenzhenOrderColumns,
        {sz_order});

    Row sz_transaction = ZeroRow(kShenzhenTransactionColumns);
    sz_transaction["ChannelNo"] = "201";
    sz_transaction["ApplSeqNum"] = "2";
    sz_transaction["MDStreamID"] = "011";
    sz_transaction["BidApplSeqNum"] = "1";
    sz_transaction["OfferApplSeqNum"] = "0";
    sz_transaction["SecurityID"] = "000001";
    sz_transaction["SecurityIDSource"] = "102";
    sz_transaction["LastPx"] = "10.0000";
    sz_transaction["LastQty"] = "100";
    sz_transaction["ExecType"] = "F";
    sz_transaction["TransactTime"] = "09:30:00.009";
    sz_transaction["LocalTime"] = "09:30:00.010";
    sz_transaction["SeqNo"] = "301";
    WriteTable(
        directory / "mdl_6_36_0.csv",
        kShenzhenTransactionColumns,
        {sz_transaction});
}

recovery::StartupReplayResultV1 Replay(
    const std::filesystem::path& directory,
    recovery::StartupReplayMessageSetV1 messages,
    DecodeSink* sink) {
    recovery::StartupReplayConfigV1 config;
    config.directory = directory;
    config.enabled_messages = messages;
    recovery::MdlCsvStartupReplaySourceV1 source(std::move(config));
    return source.Replay(*sink);
}

recovery::StartupReplayResultV1 Replay(
    recovery::StartupReplayConfigV1 config,
    DecodeSink* sink) {
    recovery::MdlCsvStartupReplaySourceV1 source(std::move(config));
    return source.Replay(*sink);
}

void TestFiveMessagesAndQueues(TestContext* test) {
    TempDirectory directory;
    PopulateSuccessDirectory(directory.path());
    DecodeSink sink;
    const recovery::StartupReplayResultV1 result = Replay(
        directory.path(),
        recovery::StartupReplayMessageSetV1::kAll,
        &sink);
    test->Expect(result.ok(), "five-message replay succeeds");
    test->Expect(
        result.counts.shanghai_snapshots == 1U &&
            result.counts.shanghai_ticks == 1U &&
            result.counts.shenzhen_snapshots == 1U &&
            result.counts.shenzhen_orders == 1U &&
            result.counts.shenzhen_transactions == 1U,
        "all five message counters increment");
    test->Expect(sink.events.size() == 5U, "all five bodies decode");
    const auto fence_is =
        [&](std::size_t index,
            std::uint16_t service_id,
            std::uint16_t message_id) {
            return sink.fences.size() > index &&
                   sink.fences[index].service_id == service_id &&
                   sink.fences[index].service_version == 101U &&
                   sink.fences[index].message_id == message_id;
        };
    test->Expect(
        sink.fences.size() == 5U &&
            fence_is(0U, 4U, 4U) &&
            fence_is(1U, 4U, 24U) &&
            fence_is(2U, 6U, 28U) &&
            fence_is(3U, 6U, 33U) &&
            fence_is(4U, 6U, 36U),
        "logical tuple fences are captured once in file-capture order");
    const auto* sh_snapshot =
        sink.events.empty()
            ? nullptr
            : std::get_if<market::ShanghaiSnapshotV1>(
                  &sink.events[0U]);
    test->Expect(
        sh_snapshot != nullptr &&
            sh_snapshot->common.security_id == "600000 " &&
            sh_snapshot->book.bid1_queue.retained_count == 2U &&
            sh_snapshot->book.bid1_queue.quantities[0U].raw ==
                100000,
        "Shanghai queue joins and preserves textual/fixed values");
    const auto* sz_snapshot =
        sink.events.size() < 3U
            ? nullptr
            : std::get_if<market::ShenzhenSnapshotV1>(
                  &sink.events[2U]);
    const auto* sz_order =
        sink.events.size() < 4U
            ? nullptr
            : std::get_if<market::ShenzhenOrderV1>(
                  &sink.events[3U]);
    const auto* sz_transaction =
        sink.events.size() < 5U
            ? nullptr
            : std::get_if<market::ShenzhenTransactionV1>(
                  &sink.events[4U]);
    test->Expect(
        sz_snapshot != nullptr &&
            sz_snapshot->common.security_id_source == "102 " &&
            sz_snapshot->book.ask1_queue.retained_count == 1U &&
            sz_order != nullptr &&
            sz_order->common.security_id_source == "102 " &&
            sz_transaction != nullptr &&
            sz_transaction->common.security_id_source == "102 ",
        "Shenzhen CSV source 102 maps to the canonical four-byte SDK source while queues join");
    test->Expect(
        sink.notice_flags.size() >= 3U &&
            (sink.notice_flags[2U] &
             recovery::
                 kStartupReplayNoticeShenzhenSnapshotChannelUnavailableV1) !=
                0U,
        "missing Shenzhen ChannelNo carries mandatory notice");
}

void TestVendorBlankPreOpenBooksAndPaddedText(TestContext* test) {
    {
        TempDirectory directory;
        const std::vector<std::string> snapshot_columns =
            ShanghaiSnapshotColumns();
        Row snapshot = ZeroRow(snapshot_columns);
        snapshot["UpdateTime"] = "08:45:00.000";
        snapshot["SecurityID"] = "600648";
        snapshot["ImageStatus"] = "1";
        snapshot["InstruStatus"] = "START";
        snapshot["LocalTime"] = "08:45:00.456";
        snapshot["SeqNo"] = "1";
        for (std::size_t index = 1U; index <= 10U; ++index) {
            const std::string ordinal = std::to_string(index);
            snapshot.erase("AskPrice" + ordinal);
            snapshot.erase("AskVolume" + ordinal);
            snapshot.erase("BidPrice" + ordinal);
            snapshot.erase("BidVolume" + ordinal);
            snapshot.erase("NumOrdersB" + ordinal);
            snapshot.erase("NumOrdersS" + ordinal);
        }
        WriteTable(
            directory.path() / "MarketData.csv",
            snapshot_columns,
            {snapshot});

        const std::vector<std::string> queue_columns =
            QueueColumns("UpdateTime");
        Row bid = ZeroRow(queue_columns);
        bid["UpdateTime"] = "08:45:00.000";
        bid["SecurityID"] = "600648";
        bid["ImageStatus"] = "1";
        bid["Side"] = "B";
        bid["NoPriceLevel"] = "1";
        bid["LocalTime"] = "08:45:00.456";
        bid["SeqNo"] = "1";
        bid.erase("PrcLvlOperator");
        bid.erase("Price");
        bid.erase("Volume");
        bid.erase("NumOrders");
        bid.erase("NoOrders");
        for (std::size_t index = 1U; index <= 50U; ++index) {
            bid.erase("OrderQty" + std::to_string(index));
        }
        Row ask = bid;
        ask["Side"] = "S";
        WriteTable(
            directory.path() / "OrderQueue.csv",
            queue_columns,
            {bid, ask});

        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::
                kShanghaiSnapshot,
            &sink);
        const auto* decoded =
            sink.events.empty()
                ? nullptr
                : std::get_if<market::ShanghaiSnapshotV1>(
                      &sink.events.front());
        if (!result.ok()) {
            std::cerr << "Shanghai blank-book replay detail: "
                      << result.detail << '\n';
        }
        test->Expect(
            result.ok() &&
                result.counts.shanghai_snapshots == 1U &&
                decoded != nullptr &&
                decoded->book.actual_bid_depth == 0U &&
                decoded->book.actual_ask_depth == 0U &&
                decoded->book.retained_bid_depth == 0U &&
                decoded->book.retained_ask_depth == 0U,
            "vendor blank Shanghai pre-open depth and queue values map to an empty book");
    }
    {
        TempDirectory directory;
        const std::vector<std::string> snapshot_columns =
            ShanghaiSnapshotColumns();
        Row snapshot = ZeroRow(snapshot_columns);
        snapshot["UpdateTime"] = "09:15:00.000";
        snapshot["SecurityID"] = "603813";
        snapshot["ImageStatus"] = "1";
        snapshot["InstruStatus"] = "OCALL";
        snapshot["BidNum"] = "1";
        snapshot["SellNum"] = "1";
        snapshot["BidPrice1"] = "34.270";
        snapshot["BidVolume1"] = "100.000";
        snapshot["NumOrdersB1"] = "0";
        snapshot["BidPrice2"] = "0.000";
        snapshot["BidVolume2"] = "700.000";
        snapshot["NumOrdersB2"] = "0";
        snapshot["AskPrice1"] = "34.270";
        snapshot["AskVolume1"] = "100.000";
        snapshot["NumOrdersS1"] = "0";
        snapshot["AskPrice2"] = "0.000";
        snapshot["AskVolume2"] = "0.000";
        snapshot["NumOrdersS2"] = "0";
        snapshot["LocalTime"] = "09:15:00.001";
        snapshot["SeqNo"] = "1";
        for (std::size_t index = 3U; index <= 10U; ++index) {
            const std::string ordinal = std::to_string(index);
            snapshot.erase("AskPrice" + ordinal);
            snapshot.erase("AskVolume" + ordinal);
            snapshot.erase("BidPrice" + ordinal);
            snapshot.erase("BidVolume" + ordinal);
            snapshot.erase("NumOrdersB" + ordinal);
            snapshot.erase("NumOrdersS" + ordinal);
        }
        WriteTable(
            directory.path() / "MarketData.csv",
            snapshot_columns,
            {snapshot});
        WriteTable(
            directory.path() / "OrderQueue.csv",
            QueueColumns("UpdateTime"),
            {});

        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::
                kShanghaiSnapshot,
            &sink);
        const auto* decoded =
            sink.events.empty()
                ? nullptr
                : std::get_if<market::ShanghaiSnapshotV1>(
                      &sink.events.front());
        test->Expect(
            result.ok() && decoded != nullptr &&
                decoded->book.actual_bid_depth == 2U &&
                decoded->book.actual_ask_depth == 2U &&
                decoded->book.bids[1U].price.raw == 0 &&
                decoded->book.bids[1U].quantity.raw == 700000,
            "Shanghai flattened level presence, not BidNum/SellNum counters, restores dynamic depth");
    }
    {
        TempDirectory directory;
        const std::vector<std::string> snapshot_columns =
            ShenzhenSnapshotColumns();
        Row snapshot = ZeroRow(snapshot_columns);
        snapshot["UpdateTime"] = "08:15:00.000";
        snapshot["MDStreamID"] = "010";
        snapshot["SecurityID"] = "002853";
        snapshot["SecurityIDSource"] = "102 ";
        snapshot["TradingPhaseCode"] = "S0      ";
        snapshot["LocalTime"] = "08:15:00.690";
        snapshot["SeqNo"] = "1";
        for (std::size_t index = 1U; index <= 10U; ++index) {
            const std::string ordinal = std::to_string(index);
            snapshot.erase("AskPrice" + ordinal);
            snapshot.erase("AskVolume" + ordinal);
            snapshot.erase("BidPrice" + ordinal);
            snapshot.erase("BidVolume" + ordinal);
            snapshot.erase("NumOrdersB" + ordinal);
            snapshot.erase("NumOrdersS" + ordinal);
        }
        WriteTable(
            directory.path() / "mdl_6_28_0.csv",
            snapshot_columns,
            {snapshot});

        const std::vector<std::string> queue_columns =
            QueueColumns("DataTimeStamp");
        Row ask = ZeroRow(queue_columns);
        ask["DataTimeStamp"] = "08:15:00.000";
        ask["SecurityID"] = "002853";
        ask["ImageStatus"] = "1";
        ask["Side"] = "S";
        ask["NoPriceLevel"] = "1";
        ask["LocalTime"] = "08:15:00.690";
        ask["SeqNo"] = "1";
        ask.erase("PrcLvlOperator");
        ask.erase("Price");
        ask.erase("Volume");
        ask.erase("NumOrders");
        ask.erase("NoOrders");
        for (std::size_t index = 1U; index <= 50U; ++index) {
            ask.erase("OrderQty" + std::to_string(index));
        }
        Row bid = ask;
        bid["Side"] = "B";
        WriteTable(
            directory.path() / "mdl_6_28_1.csv",
            queue_columns,
            {ask});
        WriteTable(
            directory.path() / "mdl_6_28_2.csv",
            queue_columns,
            {bid});

        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::
                kShenzhenSnapshot,
            &sink);
        const auto* decoded =
            sink.events.empty()
                ? nullptr
                : std::get_if<market::ShenzhenSnapshotV1>(
                      &sink.events.front());
        if (!result.ok()) {
            std::cerr << "Shenzhen blank-book replay detail: "
                      << result.detail << '\n';
        }
        test->Expect(
            result.ok() &&
                result.counts.shenzhen_snapshots == 1U &&
                decoded != nullptr &&
                decoded->book.actual_bid_depth == 0U &&
                decoded->book.actual_ask_depth == 0U &&
                decoded->book.retained_bid_depth == 0U &&
                decoded->book.retained_ask_depth == 0U &&
                decoded->common.security_id_source == "102 ",
            "vendor blank Shenzhen pre-open book and right-padded fixed text are normalized");
    }
}

void TestPartialSuffixAndFailures(TestContext* test) {
    {
        TempDirectory directory;
        {
            std::ofstream output(
                directory.path() / "mdl_4_24_0.csv",
                std::ios::binary);
            output << "BizIndex,Channel";
        }
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShanghaiTick,
            &sink);
        test->Expect(
            result.error ==
                recovery::StartupReplayErrorV1::
                    kIncompleteBoundary,
            "unresolved partial header fails after one bounded extension");
    }
    {
        TempDirectory directory;
        Row tick = ZeroRow(kShanghaiTickColumns);
        tick["BizIndex"] = "1";
        tick["Channel"] = "1";
        tick["SecurityID"] = "600000";
        tick["TickTime"] = "09:30:00.001";
        tick["Type"] = "A";
        tick["TickBSFlag"] = "B";
        tick["LocalTime"] = "09:30:00.002";
        tick["SeqNo"] = "1";
        WriteTable(
            directory.path() / "mdl_4_24_0.csv",
            kShanghaiTickColumns,
            {tick},
            "2,1,partial");
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShanghaiTick,
            &sink);
        test->Expect(
            result.error ==
                    recovery::StartupReplayErrorV1::
                        kIncompleteBoundary &&
                result.counts.shanghai_ticks == 1U,
            "unresolved non-LF data suffix fails after the bounded extension");
    }
    {
        TempDirectory directory;
        Row first = ZeroRow(kShanghaiTickColumns);
        first["BizIndex"] = "1";
        first["Channel"] = "1";
        first["SecurityID"] = "600000";
        first["TickTime"] = "09:30:00.001";
        first["Type"] = "A";
        first["TickBSFlag"] = "B";
        first["LocalTime"] = "09:30:00.002";
        first["SeqNo"] = "1";
        Row second = first;
        second["BizIndex"] = "2";
        second["SeqNo"] = "2";
        std::string partial =
            CsvLine(kShanghaiTickColumns, second);
        partial.pop_back();
        WriteTable(
            directory.path() / "mdl_4_24_0.csv",
            kShanghaiTickColumns,
            {first},
            partial);
        bool completed = false;
        DecodeSink sink;
        sink.publish_hook =
            [&](const recovery::StartupReplayPublicationV1&) {
                if (!completed) {
                    std::ofstream output(
                        directory.path() / "mdl_4_24_0.csv",
                        std::ios::binary | std::ios::app);
                    output << '\n';
                    completed = true;
                }
            };
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShanghaiTick,
            &sink);
        test->Expect(
            result.ok() && completed &&
                result.counts.shanghai_ticks == 2U,
            "a completed tick boundary is consumed through the finite extension");
    }
    {
        TempDirectory directory;
        Row tick = ZeroRow(kShanghaiTickColumns);
        tick["BizIndex"] = "1";
        tick["Channel"] = "1";
        tick["SecurityID"] = "600000";
        tick["TickTime"] = "09:30:00.001";
        tick["Type"] = "A";
        tick["Price"] = "999999999999999999999.000";
        tick["TickBSFlag"] = "B";
        tick["LocalTime"] = "09:30:00.002";
        tick["SeqNo"] = "1";
        WriteTable(
            directory.path() / "mdl_4_24_0.csv",
            kShanghaiTickColumns,
            {tick});
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShanghaiTick,
            &sink);
        test->Expect(
            result.error ==
                recovery::StartupReplayErrorV1::kNumericOverflow,
            "fixed-point overflow fails closed");
    }
    {
        TempDirectory directory;
        std::vector<std::string> bad_columns = kShanghaiTickColumns;
        bad_columns.pop_back();
        WriteTable(
            directory.path() / "mdl_4_24_0.csv",
            bad_columns,
            {});
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShanghaiTick,
            &sink);
        test->Expect(
            result.error ==
                recovery::StartupReplayErrorV1::kSchemaMismatch,
            "missing header column fails strict schema validation");
    }
    {
        TempDirectory directory;
        Row header;
        for (const std::string& column : kShanghaiTickColumns) {
            header[column] = column;
        }
        WriteTable(
            directory.path() / "mdl_4_24_0.csv",
            kShanghaiTickColumns,
            {},
            CsvLine(kShanghaiTickColumns, header));
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShanghaiTick,
            &sink);
        test->Expect(
            result.error ==
                recovery::StartupReplayErrorV1::kDuplicateHeader,
            "repeated header row fails closed");
    }
    {
        TempDirectory directory;
        Row header;
        for (const std::string& column : kShanghaiTickColumns) {
            header[column] = column;
        }
        std::ofstream output(
            directory.path() / "mdl_4_24_0.csv",
            std::ios::binary);
        output << CsvLine(kShanghaiTickColumns, header);
        output << "1,2\n";
        output.close();
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShanghaiTick,
            &sink);
        test->Expect(
            result.error ==
                recovery::StartupReplayErrorV1::
                    kColumnCountMismatch,
            "wrong data column count fails closed");
    }
    {
        TempDirectory directory;
        Row tick = ZeroRow(kShanghaiTickColumns);
        tick["BizIndex"] = "1";
        tick["Channel"] = "1";
        tick["SecurityID"] = "600000";
        tick["TickTime"] = "09:30:00.001";
        tick["Type"] = "A";
        tick["TickBSFlag"] = "B";
        tick["LocalTime"] = "09:30:00.002";
        tick["SeqNo"] = "1";
        Row header;
        for (const std::string& column : kShanghaiTickColumns) {
            header[column] = column;
        }
        std::string vendor_row = CsvLine(kShanghaiTickColumns, tick);
        vendor_row.insert(vendor_row.size() - 1U, ",");
        std::ofstream output(
            directory.path() / "mdl_4_24_0.csv",
            std::ios::binary);
        output << CsvLine(kShanghaiTickColumns, header)
               << vendor_row;
        output.close();
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShanghaiTick,
            &sink);
        test->Expect(
            result.ok() && result.counts.shanghai_ticks == 1U,
            "one vendor trailing empty data column is accepted");
    }
    {
        TempDirectory directory;
        Row tick = ZeroRow(kShanghaiTickColumns);
        tick["BizIndex"] = "1";
        tick["Channel"] = "1";
        tick["SecurityID"] = "600000";
        tick["TickTime"] = "09:30:00.001";
        tick["Type"] = "A";
        tick["TickBSFlag"] = "B";
        tick["LocalTime"] = "09:30:00.002";
        tick["SeqNo"] = "1";
        Row header;
        for (const std::string& column : kShanghaiTickColumns) {
            header[column] = column;
        }
        std::string invalid_row = CsvLine(kShanghaiTickColumns, tick);
        invalid_row.insert(
            invalid_row.size() - 1U,
            ",unexpected");
        std::ofstream output(
            directory.path() / "mdl_4_24_0.csv",
            std::ios::binary);
        output << CsvLine(kShanghaiTickColumns, header)
               << invalid_row;
        output.close();
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShanghaiTick,
            &sink);
        test->Expect(
            result.error ==
                recovery::StartupReplayErrorV1::
                    kColumnCountMismatch,
            "a trailing non-empty undeclared data column fails closed");
    }
    {
        TempDirectory directory;
        Row tick = ZeroRow(kShanghaiTickColumns);
        tick["BizIndex"] = "1";
        tick["Channel"] = "1";
        tick["SecurityID"] = "600000";
        tick["TickTime"] = "25:00:00.000";
        tick["Type"] = "A";
        tick["TickBSFlag"] = "B";
        tick["LocalTime"] = "09:30:00.002";
        tick["SeqNo"] = "1";
        WriteTable(
            directory.path() / "mdl_4_24_0.csv",
            kShanghaiTickColumns,
            {tick});
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShanghaiTick,
            &sink);
        test->Expect(
            result.error ==
                recovery::StartupReplayErrorV1::kTimeInvalid,
            "invalid exchange time fails closed");
    }
    {
        TempDirectory directory;
        Row tick = ZeroRow(kShanghaiTickColumns);
        tick["BizIndex"] = "1";
        tick["Channel"] = "1";
        tick["SecurityID"] = "600000";
        tick["TickTime"] = "09:30:00.001";
        tick["Type"] = "A";
        tick["TickBSFlag"] = "B";
        tick["LocalTime"] = "09:30:00.002";
        tick["SeqNo"] = "1";
        WriteTable(
            directory.path() / "mdl_4_24_0.csv",
            kShanghaiTickColumns,
            {tick});
        recovery::StartupReplayConfigV1 too_small;
        too_small.directory = directory.path();
        too_small.enabled_messages =
            recovery::StartupReplayMessageSetV1::kShanghaiTick;
        too_small.maximum_message_bytes = 270U;
        DecodeSink rejected_sink;
        const auto rejected =
            Replay(std::move(too_small), &rejected_sink);
        test->Expect(
            rejected.error ==
                    recovery::StartupReplayErrorV1::
                        kInvalidConfiguration,
            "message bound must hold the largest fixed V4 wire body");
    }
    {
        TempDirectory directory;
        Row first = ZeroRow(kShanghaiTickColumns);
        first["BizIndex"] = "1";
        first["Channel"] = "1";
        first["SecurityID"] = "600000";
        first["TickTime"] = "09:30:00.001";
        first["Type"] = "A";
        first["TickBSFlag"] = "B";
        first["LocalTime"] = "09:30:00.002";
        first["SeqNo"] = "1";
        Row second = first;
        second["BizIndex"] = "2";
        second["SeqNo"] = "2";
        std::string partial =
            CsvLine(kShanghaiTickColumns, second);
        partial.pop_back();
        const std::filesystem::path file =
            directory.path() / "mdl_4_24_0.csv";
        WriteTable(
            file,
            kShanghaiTickColumns,
            {first},
            partial);
        bool truncated = false;
        DecodeSink sink;
        sink.publish_hook =
            [&](const recovery::StartupReplayPublicationV1&) {
                if (!truncated) {
                    std::filesystem::resize_file(
                        file,
                        std::filesystem::file_size(file) - 1U);
                    truncated = true;
                }
            };
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShanghaiTick,
            &sink);
        test->Expect(
            truncated &&
                result.error ==
                    recovery::StartupReplayErrorV1::kIo &&
                result.counts.shanghai_ticks == 1U,
            "retained descriptor shrink is detected before selecting an extension");
    }
    {
        TempDirectory directory;
        Row order = ZeroRow(kShenzhenOrderColumns);
        order["ChannelNo"] = "1";
        order["ApplSeqNum"] = "2";
        order["MDStreamID"] = "011";
        order["SecurityID"] = "000001";
        order["SecurityIDSource"] = "102";
        order["Side"] = "1";
        order["TransactTime"] = "09:30:00.001";
        order["OrdType"] = "2";
        order["LocalTime"] = "09:30:00.002";
        order["SeqNo"] = "1";
        WriteTable(
            directory.path() / "mdl_6_33_0.csv",
            kShenzhenOrderColumns,
            {order});
        WriteTable(
            directory.path() / "mdl_6_36_0.csv",
            kShenzhenTransactionColumns,
            {});
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShenzhenOrder,
            &sink);
        test->Expect(
            result.error ==
                recovery::StartupReplayErrorV1::kSequenceGap,
            "shared Shenzhen ApplSeqNum gap fails at EOF");
    }
}

void TestTupleFencesAndReservedValues(TestContext* test) {
    {
        TempDirectory directory;
        PopulateSuccessDirectory(directory.path());
        DecodeSink sink;
        sink.fence_hook =
            [](const l2flow::sdk::MessageKey&,
               std::string* detail) {
                *detail = "intentional fence rejection";
                return false;
            };
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShanghaiTick,
            &sink);
        test->Expect(
            result.error ==
                    recovery::StartupReplayErrorV1::kSinkRejected &&
                sink.fences.size() == 1U &&
                sink.events.empty(),
            "tuple fence rejection stops before file replay");
    }
    {
        TempDirectory directory;
        PopulateSuccessDirectory(directory.path());
        ReplaceFirstInFile(
            directory.path() / "OrderQueue.csv",
            ",600000 ,1,B,1,0,10.100,",
            ",600000 ,2,B,1,0,10.100,");
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShanghaiSnapshot,
            &sink);
        test->Expect(
            result.error ==
                recovery::StartupReplayErrorV1::kValueInvalid,
            "client Shanghai queue rejects ImageStatus 2");
    }
    {
        TempDirectory directory;
        PopulateSuccessDirectory(directory.path());
        ReplaceFirstInFile(
            directory.path() / "mdl_6_28_1.csv",
            ",000001,1,S,1,0,10.000001,",
            ",000001,1,S,1,9,10.000001,");
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShenzhenSnapshot,
            &sink);
        test->Expect(
            result.error ==
                recovery::StartupReplayErrorV1::kValueInvalid,
            "reserved Shenzhen PrcLvlOperator rejects nonzero");
    }
    {
        TempDirectory directory;
        PopulateSuccessDirectory(directory.path());
        ReplaceFirstInFile(
            directory.path() / "mdl_6_28_1.csv",
            ",000001,1,S,",
            ",000001,0,S,");
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShenzhenSnapshot,
            &sink);
        test->Expect(
            result.error ==
                recovery::StartupReplayErrorV1::kValueInvalid,
            "Shenzhen snapshot queue ImageStatus must be one");
    }
    {
        TempDirectory directory;
        PopulateSuccessDirectory(directory.path());
        ReplaceFirstInFile(
            directory.path() / "mdl_6_28_0.csv",
            ",010,000001,102,",
            ",999,000001,102,");
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShenzhenSnapshot,
            &sink);
        test->Expect(
            result.error ==
                recovery::StartupReplayErrorV1::kValueInvalid,
            "Shenzhen snapshot rejects undocumented MDStreamID");
    }
    {
        TempDirectory directory;
        PopulateSuccessDirectory(directory.path());
        ReplaceFirstInFile(
            directory.path() / "MarketData.csv",
            ",TRADE,",
            ",UNKNOWN,");
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShanghaiSnapshot,
            &sink);
        test->Expect(
            result.error ==
                recovery::StartupReplayErrorV1::kValueInvalid,
            "Shanghai snapshot rejects undocumented InstruStatus");
    }
    {
        TempDirectory directory;
        PopulateSuccessDirectory(directory.path());
        recovery::StartupReplayConfigV1 config;
        config.directory = directory.path();
        config.enabled_messages =
            recovery::StartupReplayMessageSetV1::kShanghaiSnapshot;
        config.maximum_message_bytes = 271U;
        DecodeSink sink;
        const auto result = Replay(std::move(config), &sink);
        test->Expect(
            result.error ==
                recovery::StartupReplayErrorV1::kMessageTooLarge,
            "maximum_message_bytes limits total SDK wire bytes");
    }
}

void TestFenceOrderingAndSnapshotTailJoin(TestContext* test) {
    const auto shanghai_tail_row =
        [](const std::vector<std::string>& columns,
           std::string sequence,
           std::string side = "B") {
            Row row = ZeroRow(columns);
            row["UpdateTime"] = "09:31:00.001";
            row["SecurityID"] = "600000 ";
            row["ImageStatus"] = "1";
            row["Side"] = std::move(side);
            row["NoPriceLevel"] = "1";
            row["LocalTime"] = "09:31:00.002";
            row["SeqNo"] = std::move(sequence);
            return row;
        };
    {
        TempDirectory directory;
        PopulateSuccessDirectory(directory.path());
        Row second = ZeroRow(kShanghaiTickColumns);
        second["BizIndex"] = "2";
        second["Channel"] = "1";
        second["SecurityID"] = "600000 ";
        second["TickTime"] = "09:30:00.011";
        second["Type"] = "A";
        second["BuyOrderNO"] = "11";
        second["Price"] = "10.100";
        second["Qty"] = "100";
        second["TickBSFlag"] = "B";
        second["LocalTime"] = "09:30:00.012";
        second["SeqNo"] = "102";
        bool appended = false;
        DecodeSink sink;
        sink.fence_hook =
            [&](const l2flow::sdk::MessageKey& key,
                std::string*) {
                if (key.service_id == 4U &&
                    key.message_id == 24U) {
                    std::ofstream output(
                        directory.path() / "mdl_4_24_0.csv",
                        std::ios::binary | std::ios::app);
                    output << CsvLine(kShanghaiTickColumns, second);
                    appended = true;
                }
                return true;
            };
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShanghaiTick,
            &sink);
        test->Expect(
            result.ok() && appended &&
                result.counts.shanghai_ticks == 2U,
            "tuple fence runs before the tuple prefix capture");
    }
    {
        TempDirectory directory;
        PopulateSuccessDirectory(directory.path());
        const std::filesystem::path root =
            directory.path() / "MarketData.csv";
        std::filesystem::resize_file(
            root, std::filesystem::file_size(root) - 1U);
        const std::vector<std::string> columns =
            ShanghaiSnapshotColumns();
        Row second = ZeroRow(columns);
        second["UpdateTime"] = "09:31:00.001";
        second["SecurityID"] = "600001 ";
        second["ImageStatus"] = "1";
        second["InstruStatus"] = "TRADE";
        second["LocalTime"] = "09:31:00.002";
        second["SeqNo"] = "200";
        bool completed = false;
        DecodeSink sink;
        sink.fence_hook =
            [&](const l2flow::sdk::MessageKey& key,
                std::string*) {
                if (key.service_id == 4U &&
                    key.message_id == 24U) {
                    std::ofstream output(
                        root,
                        std::ios::binary | std::ios::app);
                    output << '\n'
                           << CsvLine(columns, second);
                    completed = true;
                }
                return true;
            };
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::
                    kShanghaiSnapshot |
                recovery::StartupReplayMessageSetV1::kShanghaiTick,
            &sink);
        test->Expect(
            result.ok() && completed &&
                result.counts.shanghai_snapshots == 2U,
            "a selected root extension publishes every complete row in its finite descriptor cut");
    }
    {
        TempDirectory directory;
        PopulateSuccessDirectory(directory.path());
        const std::filesystem::path queue =
            directory.path() / "OrderQueue.csv";
        std::filesystem::resize_file(
            queue, std::filesystem::file_size(queue) - 1U);
        const std::vector<std::string> queue_columns =
            QueueColumns("UpdateTime");
        Row live_tail = ZeroRow(queue_columns);
        live_tail["UpdateTime"] = "09:31:00.001";
        live_tail["SecurityID"] = "600000 ";
        live_tail["ImageStatus"] = "1";
        live_tail["Side"] = "B";
        live_tail["NoPriceLevel"] = "1";
        live_tail["LocalTime"] = "09:31:00.002";
        live_tail["SeqNo"] = "200";
        bool completed = false;
        DecodeSink sink;
        sink.fence_hook =
            [&](const l2flow::sdk::MessageKey& key,
                std::string*) {
                if (key.service_id == 4U &&
                    key.message_id == 24U) {
                    std::ofstream output(
                        queue,
                        std::ios::binary | std::ios::app);
                    output << '\n'
                           << CsvLine(queue_columns, live_tail);
                    completed = true;
                }
                return true;
            };
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::
                    kShanghaiSnapshot |
                recovery::StartupReplayMessageSetV1::kShanghaiTick,
            &sink);
        const auto* snapshot =
            sink.events.empty()
                ? nullptr
                : std::get_if<market::ShanghaiSnapshotV1>(
                      &sink.events.front());
        test->Expect(
            result.ok() && completed &&
                result.counts.shanghai_snapshots == 1U &&
                snapshot != nullptr &&
                snapshot->book.ask1_queue.retained_count == 1U,
            "appended child closes its root and the remaining finite child extension is validated");
    }
    {
        TempDirectory directory;
        PopulateSuccessDirectory(directory.path());
        const std::filesystem::path asks =
            directory.path() / "mdl_6_28_1.csv";
        std::filesystem::resize_file(
            asks, std::filesystem::file_size(asks) - 1U);
        bool completed = false;
        DecodeSink sink;
        sink.fence_hook =
            [&](const l2flow::sdk::MessageKey& key,
                std::string*) {
                if (key.service_id == 6U &&
                    key.message_id == 33U) {
                    std::ofstream output(
                        asks,
                        std::ios::binary | std::ios::app);
                    output << '\n';
                    completed = true;
                }
                return true;
            };
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::
                    kShenzhenSnapshot |
                recovery::StartupReplayMessageSetV1::kShenzhenOrder,
            &sink);
        const auto* snapshot =
            sink.events.empty()
                ? nullptr
                : std::get_if<market::ShenzhenSnapshotV1>(
                      &sink.events.front());
        test->Expect(
            result.ok() && completed &&
                result.counts.shenzhen_snapshots == 1U &&
                snapshot != nullptr &&
                snapshot->book.ask1_queue.retained_count == 1U,
            "Shenzhen appended side child completes its initial root");
    }
    {
        TempDirectory directory;
        PopulateSuccessDirectory(directory.path());
        const std::filesystem::path queue =
            directory.path() / "OrderQueue.csv";
        std::filesystem::resize_file(
            queue, std::filesystem::file_size(queue) - 1U);
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShanghaiSnapshot,
            &sink);
        test->Expect(
            result.error ==
                    recovery::StartupReplayErrorV1::
                        kIncompleteBoundary &&
                sink.events.empty(),
            "nonzero root order count with an unresolved partial child fails closed at the boundary");
    }
    {
        TempDirectory directory;
        PopulateSuccessDirectory(directory.path());
        const std::vector<std::string> queue_columns =
            QueueColumns("UpdateTime");
        Row orphan = ZeroRow(queue_columns);
        orphan["UpdateTime"] = "09:31:00.001";
        orphan["SecurityID"] = "600000 ";
        orphan["ImageStatus"] = "1";
        orphan["Side"] = "B";
        orphan["NoPriceLevel"] = "1";
        orphan["LocalTime"] = "09:31:00.002";
        orphan["SeqNo"] = "200";
        {
            std::ofstream output(
                directory.path() / "OrderQueue.csv",
                std::ios::binary | std::ios::app);
            output << CsvLine(queue_columns, orphan);
        }
        const std::vector<std::string> root_columns =
            ShanghaiSnapshotColumns();
        Row appended_root = ZeroRow(root_columns);
        appended_root["UpdateTime"] = "09:31:00.001";
        appended_root["SecurityID"] = "600000 ";
        appended_root["ImageStatus"] = "1";
        appended_root["InstruStatus"] = "TRADE";
        appended_root["LocalTime"] = "09:31:00.002";
        appended_root["SeqNo"] = "200";
        bool appended = false;
        DecodeSink sink;
        sink.fence_hook =
            [&](const l2flow::sdk::MessageKey& key,
                std::string*) {
                if (key.service_id == 4U &&
                    key.message_id == 24U) {
                    std::ofstream output(
                        directory.path() / "MarketData.csv",
                        std::ios::binary | std::ios::app);
                    output << CsvLine(
                        root_columns, appended_root);
                    appended = true;
                }
                return true;
            };
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::
                    kShanghaiSnapshot |
                recovery::StartupReplayMessageSetV1::kShanghaiTick,
            &sink);
        test->Expect(
            result.ok() && appended &&
                result.counts.shanghai_snapshots == 1U,
            "a complete root appended after its fixed cut is not replayed and a validated child row beyond the final root cutoff is cropped");
    }
    {
        TempDirectory directory;
        PopulateSuccessDirectory(directory.path());
        {
            std::ofstream output(
                directory.path() / "OrderQueue.csv",
                std::ios::binary | std::ios::app);
            output << "not,a,valid,queue,row\n";
        }
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShanghaiSnapshot,
            &sink);
        test->Expect(
            result.error ==
                recovery::StartupReplayErrorV1::
                    kColumnCountMismatch,
            "cropped child tail is fully parsed and malformed rows fail");
    }
    {
        TempDirectory directory;
        PopulateSuccessDirectory(directory.path());
        const std::vector<std::string> columns =
            QueueColumns("UpdateTime");
        const Row tail =
            shanghai_tail_row(columns, "200");
        {
            std::ofstream output(
                directory.path() / "OrderQueue.csv",
                std::ios::binary | std::ios::app);
            output << CsvLine(columns, tail)
                   << CsvLine(columns, tail);
        }
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShanghaiSnapshot,
            &sink);
        test->Expect(
            result.error ==
                recovery::StartupReplayErrorV1::kJoinDuplicate,
            "duplicate child rows beyond the final root cutoff are still fully validated");
    }
    {
        TempDirectory directory;
        PopulateSuccessDirectory(directory.path());
        const std::vector<std::string> columns =
            QueueColumns("UpdateTime");
        std::string incomplete = CsvLine(
            columns, shanghai_tail_row(columns, "200"));
        incomplete.pop_back();
        {
            std::ofstream output(
                directory.path() / "OrderQueue.csv",
                std::ios::binary | std::ios::app);
            output << incomplete;
        }
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShanghaiSnapshot,
            &sink);
        test->Expect(
            result.error ==
                    recovery::StartupReplayErrorV1::
                        kIncompleteBoundary &&
                result.counts.shanghai_snapshots == 1U,
            "unresolved non-LF child tail fails after the bounded extension");
    }
}

void TestShenzhenZeroChannelAndMergeBounds(TestContext* test) {
    {
        TempDirectory directory;
        std::vector<Row> blocked_orders;
        blocked_orders.reserve(
            recovery::kStartupReplayCooperativeCheckpointRecordsV1 +
            32U);
        for (std::size_t index = 0U;
             index <
                 recovery::kStartupReplayCooperativeCheckpointRecordsV1 +
                     32U;
             ++index) {
            blocked_orders.push_back(ShenzhenOrderRow(
                std::to_string(index + 1U),
                "2",
                std::to_string(10'000U + index)));
        }
        WriteTable(
            directory.path() / "mdl_6_33_0.csv",
            kShenzhenOrderColumns,
            blocked_orders);
        WriteTable(
            directory.path() / "mdl_6_36_0.csv",
            kShenzhenTransactionColumns,
            {});
        recovery::StartupReplayConfigV1 config;
        config.directory = directory.path();
        config.enabled_messages =
            recovery::StartupReplayMessageSetV1::kShenzhenOrder;
        config.maximum_pending_messages = blocked_orders.size() + 1U;
        config.maximum_pending_bytes = 16U * 1024U * 1024U;
        DecodeSink sink;
        sink.cooperative_checkpoint_hook =
            [](std::string* detail) {
                if (detail != nullptr) {
                    *detail = "checkpoint stop";
                }
                return false;
            };
        const auto result = Replay(std::move(config), &sink);
        test->Expect(
            result.error ==
                    recovery::StartupReplayErrorV1::kSinkRejected &&
                result.detail == "checkpoint stop" &&
                sink.cooperative_checkpoint_calls == 1U &&
                sink.events.empty(),
            "bounded parser checkpoint can stop a long Shenzhen gap before any publication");
    }
    {
        TempDirectory directory;
        WriteTable(
            directory.path() / "mdl_6_33_0.csv",
            kShenzhenOrderColumns,
            {ShenzhenOrderRow("0", "1", "10")});
        WriteTable(
            directory.path() / "mdl_6_36_0.csv",
            kShenzhenTransactionColumns,
            {ShenzhenTransactionRow("0", "2", "11")});
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShenzhenOrder |
                recovery::StartupReplayMessageSetV1::
                    kShenzhenTransaction,
            &sink);
        const auto* order =
            sink.events.empty()
                ? nullptr
                : std::get_if<market::ShenzhenOrderV1>(
                      &sink.events[0U]);
        const auto* transaction =
            sink.events.size() < 2U
                ? nullptr
                : std::get_if<market::ShenzhenTransactionV1>(
                      &sink.events[1U]);
        test->Expect(
            result.ok() &&
                result.counts.shenzhen_orders == 1U &&
                result.counts.shenzhen_transactions == 1U &&
                order != nullptr && order->channel == 0U &&
                transaction != nullptr &&
                transaction->channel == 0U,
            "Shenzhen shared sequence permits ChannelNo zero");
    }
    {
        TempDirectory directory;
        std::vector<Row> orders;
        for (std::uint64_t sequence = 1U;
             sequence <= 64U;
             ++sequence) {
            orders.push_back(ShenzhenOrderRow(
                "0",
                std::to_string(sequence),
                std::to_string(1000U + sequence)));
        }
        WriteTable(
            directory.path() / "mdl_6_33_0.csv",
            kShenzhenOrderColumns,
            orders);
        WriteTable(
            directory.path() / "mdl_6_36_0.csv",
            kShenzhenTransactionColumns,
            {});
        recovery::StartupReplayConfigV1 config;
        config.directory = directory.path();
        config.enabled_messages =
            recovery::StartupReplayMessageSetV1::kShenzhenOrder;
        config.maximum_pending_messages = 1U;
        config.maximum_pending_bytes = 1U;
        DecodeSink sink;
        const auto result = Replay(std::move(config), &sink);
        test->Expect(
            result.ok() &&
                result.counts.shenzhen_orders == orders.size(),
            "long ready order run does not consume the gap budget");
    }
    {
        TempDirectory directory;
        WriteTable(
            directory.path() / "mdl_6_33_0.csv",
            kShenzhenOrderColumns,
            {ShenzhenOrderRow("1", "2", "1")});
        WriteTable(
            directory.path() / "mdl_6_36_0.csv",
            kShenzhenTransactionColumns,
            {});
        recovery::StartupReplayConfigV1 config;
        config.directory = directory.path();
        config.enabled_messages =
            recovery::StartupReplayMessageSetV1::kShenzhenOrder;
        config.maximum_pending_messages = 2U;
        config.maximum_pending_bytes = 1U;
        DecodeSink sink;
        const auto result = Replay(std::move(config), &sink);
        test->Expect(
            result.error ==
                recovery::StartupReplayErrorV1::kResourceExhausted,
            "blocked Shenzhen bodies obey maximum_pending_bytes");
    }
    {
        TempDirectory directory;
        WriteTable(
            directory.path() / "mdl_6_33_0.csv",
            kShenzhenOrderColumns,
            {ShenzhenOrderRow("7", "2", "100"),
             ShenzhenOrderRow("8", "1", "101"),
             ShenzhenOrderRow("9", "1", "102")});
        WriteTable(
            directory.path() / "mdl_6_36_0.csv",
            kShenzhenTransactionColumns,
            {ShenzhenTransactionRow("9", "2", "200"),
             ShenzhenTransactionRow("7", "1", "201")});
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShenzhenOrder |
                recovery::StartupReplayMessageSetV1::
                    kShenzhenTransaction,
            &sink);
        std::vector<std::uint64_t> order_publish_sequences;
        for (std::size_t index = 0U;
             index < sink.publication_keys.size();
             ++index) {
            if (sink.publication_keys[index].service_id == 6U &&
                sink.publication_keys[index].message_id == 33U) {
                order_publish_sequences.push_back(
                    sink.publication_sequences[index]);
            }
        }
        test->Expect(
            result.ok() &&
                order_publish_sequences ==
                    std::vector<std::uint64_t>{101U, 102U, 100U},
            "cross-channel Shenzhen gap repair can publish one tuple's physical SeqNo out of order");
    }
}

void TestCooperativeCheckpointBounds(TestContext* test) {
    TempDirectory directory;
    Row tick = ZeroRow(kShanghaiTickColumns);
    tick["BizIndex"] = "1";
    tick["Channel"] = "1";
    // The comma forces a quoted field, and the logical row crosses the fixed
    // 64 KiB input buffer without reaching a record terminator.
    tick["SecurityID"] =
        std::string(
            recovery::kStartupReplayCooperativeCheckpointBytesV1 +
                4U * 1024U,
            '6') +
        ",quoted";
    tick["TickTime"] = "09:30:00.001";
    tick["Type"] = "A";
    tick["TickBSFlag"] = "B";
    tick["LocalTime"] = "09:30:00.002";
    tick["SeqNo"] = "1";
    const std::filesystem::path input =
        directory.path() / "mdl_4_24_0.csv";
    WriteTable(input, kShanghaiTickColumns, {tick});

    DecodeSink sink;
    sink.cooperative_checkpoint_hook =
        [&sink](std::string* detail) {
            if (sink.cooperative_checkpoint_calls < 2U) {
                return true;
            }
            if (detail != nullptr) {
                *detail = "long-record checkpoint stop";
            }
            return false;
        };
    const auto result = Replay(
        directory.path(),
        recovery::StartupReplayMessageSetV1::kShanghaiTick,
        &sink);
    test->Expect(
        result.error ==
                recovery::StartupReplayErrorV1::kSinkRejected &&
            result.error_file == input && result.error_line == 2U &&
            result.detail == "long-record checkpoint stop" &&
            sink.cooperative_checkpoint_calls == 2U &&
            sink.events.empty(),
        "a quoted record crossing 64 KiB can be cancelled before publication with exact source context");
}

void TestAppendedBoundaryClosesSharedSequence(TestContext* test) {
    TempDirectory directory;
    const Row first = ShenzhenOrderRow("7", "1", "100");
    const Row boundary = ShenzhenOrderRow("7", "2", "101");
    std::string incomplete_order = CsvLine(
        kShenzhenOrderColumns, boundary);
    incomplete_order.pop_back();
    std::string incomplete_transaction = CsvLine(
        kShenzhenTransactionColumns,
        ShenzhenTransactionRow("7", "4", "103"));
    incomplete_transaction.pop_back();
    WriteTable(
        directory.path() / "mdl_6_33_0.csv",
        kShenzhenOrderColumns,
        {first},
        incomplete_order);
    WriteTable(
        directory.path() / "mdl_6_36_0.csv",
        kShenzhenTransactionColumns,
        {ShenzhenTransactionRow("7", "3", "102")},
        incomplete_transaction);

    bool appended = false;
    DecodeSink sink;
    sink.publish_hook =
        [&](const recovery::StartupReplayPublicationV1&
                publication) {
            if (!appended &&
                publication.key.service_id == 6U &&
                publication.key.message_id == 33U) {
                std::ofstream output(
                    directory.path() / "mdl_6_33_0.csv",
                    std::ios::binary | std::ios::app);
                output << '\n';
                std::ofstream transaction_output(
                    directory.path() / "mdl_6_36_0.csv",
                    std::ios::binary | std::ios::app);
                transaction_output << '\n';
                appended = true;
            }
        };
    const auto result = Replay(
        directory.path(),
        recovery::StartupReplayMessageSetV1::kShenzhenOrder |
            recovery::StartupReplayMessageSetV1::
                kShenzhenTransaction,
        &sink);
    test->Expect(
        result.ok() && appended &&
            result.counts.shenzhen_orders == 2U &&
            result.counts.shenzhen_transactions == 2U &&
            sink.events.size() == 4U,
        "two-sided finite extension is consumed completely after closing the initial shared-sequence gap");
}

void TestRetainedDescriptorDoesNotFollowReplacement(
    TestContext* test) {
    TempDirectory directory;
    const Row first = ShenzhenOrderRow("7", "1", "100");
    const Row boundary = ShenzhenOrderRow("7", "2", "101");
    std::string incomplete = CsvLine(
        kShenzhenOrderColumns, boundary);
    incomplete.pop_back();
    const std::filesystem::path active =
        directory.path() / "mdl_6_33_0.csv";
    const std::filesystem::path rotated =
        directory.path() / "mdl_6_33_0.rotated";
    WriteTable(
        active,
        kShenzhenOrderColumns,
        {first},
        incomplete);
    WriteTable(
        directory.path() / "mdl_6_36_0.csv",
        kShenzhenTransactionColumns,
        {ShenzhenTransactionRow("7", "3", "102")});

    bool replaced = false;
    DecodeSink sink;
    sink.publish_hook =
        [&](const recovery::StartupReplayPublicationV1&
                publication) {
            if (!replaced &&
                publication.key.service_id == 6U &&
                publication.key.message_id == 33U) {
                std::filesystem::rename(active, rotated);
                std::filesystem::copy_file(rotated, active);
                std::ofstream replacement(
                    active,
                    std::ios::binary | std::ios::app);
                replacement << '\n';
                replaced = true;
            }
        };
    const auto result = Replay(
        directory.path(),
        recovery::StartupReplayMessageSetV1::kShenzhenOrder,
        &sink);
    test->Expect(
        replaced &&
            result.error ==
                recovery::StartupReplayErrorV1::
                    kIncompleteBoundary &&
            result.counts.shenzhen_orders == 1U,
        "extension fstat/read stays on the retained descriptor and cannot follow a replacement path");
}

void TestRecoverySequenceIntegrity(TestContext* test) {
    {
        TempDirectory directory;
        Row first = ZeroRow(kShanghaiTickColumns);
        first["BizIndex"] = "1";
        first["Channel"] = "1";
        first["SecurityID"] = "600000";
        first["TickTime"] = "09:30:00.001";
        first["Type"] = "A";
        first["TickBSFlag"] = "B";
        first["LocalTime"] = "09:30:00.002";
        first["SeqNo"] = "10";
        Row second = first;
        second["BizIndex"] = "2";
        second["SeqNo"] = "10";
        WriteTable(
            directory.path() / "mdl_4_24_0.csv",
            kShanghaiTickColumns,
            {first, second});
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShanghaiTick,
            &sink);
        test->Expect(
            result.error ==
                recovery::StartupReplayErrorV1::kSequenceDuplicate,
            "Shanghai tick duplicate recovery SeqNo fails closed");
    }
    {
        TempDirectory directory;
        WriteTable(
            directory.path() / "mdl_6_33_0.csv",
            kShenzhenOrderColumns,
            {ShenzhenOrderRow("3", "1", "20"),
             ShenzhenOrderRow("3", "2", "19")});
        WriteTable(
            directory.path() / "mdl_6_36_0.csv",
            kShenzhenTransactionColumns,
            {});
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShenzhenOrder,
            &sink);
        test->Expect(
            result.error ==
                recovery::StartupReplayErrorV1::kSequenceDuplicate,
            "Shenzhen order decreasing recovery SeqNo fails closed");
    }
    {
        TempDirectory directory;
        WriteTable(
            directory.path() / "mdl_6_33_0.csv",
            kShenzhenOrderColumns,
            {ShenzhenOrderRow("3", "2", "20"),
             ShenzhenOrderRow("3", "1", "21")});
        WriteTable(
            directory.path() / "mdl_6_36_0.csv",
            kShenzhenTransactionColumns,
            {});
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::kShenzhenOrder,
            &sink);
        test->Expect(
            result.error ==
                recovery::StartupReplayErrorV1::kSequenceDuplicate,
            "Shenzhen native sequence decreases within one file fail closed");
    }
    {
        TempDirectory directory;
        WriteTable(
            directory.path() / "mdl_6_33_0.csv",
            kShenzhenOrderColumns,
            {ShenzhenOrderRow("3", "1", "10")});
        WriteTable(
            directory.path() / "mdl_6_36_0.csv",
            kShenzhenTransactionColumns,
            {ShenzhenTransactionRow("3", "2", "20"),
             ShenzhenTransactionRow("3", "3", "20")});
        DecodeSink sink;
        const auto result = Replay(
            directory.path(),
            recovery::StartupReplayMessageSetV1::
                kShenzhenTransaction,
            &sink);
        test->Expect(
            result.error ==
                recovery::StartupReplayErrorV1::kSequenceDuplicate,
            "Shenzhen transaction duplicate recovery SeqNo fails closed");
    }
}

}  // namespace

int main() {
    TestContext test;
    TestFiveMessagesAndQueues(&test);
    TestVendorBlankPreOpenBooksAndPaddedText(&test);
    TestPartialSuffixAndFailures(&test);
    TestTupleFencesAndReservedValues(&test);
    TestFenceOrderingAndSnapshotTailJoin(&test);
    TestShenzhenZeroChannelAndMergeBounds(&test);
    TestCooperativeCheckpointBounds(&test);
    TestAppendedBoundaryClosesSharedSequence(&test);
    TestRetainedDescriptorDoesNotFollowReplacement(&test);
    TestRecoverySequenceIntegrity(&test);
    if (test.failures != 0) {
        std::cerr << test.failures << " test(s) failed\n";
        return 1;
    }
    return 0;
}
