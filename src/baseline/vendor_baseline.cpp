#include "l2flow/baseline/vendor_baseline.h"

#include "l2flow/common/sealed_file_snapshot.h"
#include "l2flow/common/sha256.h"

#include "mdl_api.h"
#include "mdl_shl2_msg.h"
#include "mdl_szl2_msg.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dlfcn.h>
#include <elf.h>
#include <fstream>
#include <iomanip>
#include <limits>
#include <optional>
#include <sstream>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

namespace l2flow::baseline {
namespace {

namespace mdl = datayes::mdl;
namespace sys = datayes::mdl::mdl_sys_msg;
namespace sh = datayes::mdl::mdl_shl2_msg;
namespace sz = datayes::mdl::mdl_szl2_msg;

constexpr std::array<MemberLayout, 8> kMessageHeadMembers = {{
    {"HeadSize", 0},
    {"MessageSize", 1},
    {"MessageEncoding", 5},
    {"ServiceID", 6},
    {"ServiceVersion", 7},
    {"MessageID", 9},
    {"LocalTime", 11},
    {"SequenceID", 15},
}};

constexpr std::array<MemberLayout, 2> kStringMembers = {{
    {"Length", 0},
    {"Offset", 2},
}};

constexpr std::array<MemberLayout, 2> kListMembers = {{
    {"Length", 0},
    {"Offset", 4},
}};

constexpr std::array<MemberLayout, 4> kLogonResponseMembers = {{
    {"UserName", 0},
    {"Password", 6},
    {"Services", 12},
    {"ReturnCode", 20},
}};

constexpr std::array<MemberLayout, 3> kControlServiceMembers = {{
    {"ServiceID", 0},
    {"ServiceVersion", 4},
    {"Messages", 8},
}};

constexpr std::array<MemberLayout, 2> kControlMessageMembers = {{
    {"MessageID", 0},
    {"MessageStatus", 4},
}};

constexpr std::array<MemberLayout, 1> kSubscribeResponseMembers = {{
    {"Services", 0},
}};

constexpr std::array<MemberLayout, 44> kSh44Members = {{
    {"UpdateTime", 0},
    {"SecurityID", 4},
    {"ImageStatus", 10},
    {"PreCloPrice", 14},
    {"OpenPrice", 18},
    {"HighPrice", 22},
    {"LowPrice", 26},
    {"LastPrice", 30},
    {"ClosePrice", 34},
    {"InstruStatus", 38},
    {"TradNumber", 44},
    {"TradVolume", 48},
    {"Turnover", 56},
    {"TotalBidVol", 64},
    {"WAvgBidPri", 72},
    {"AltWAvgBidPri", 76},
    {"TotalAskVol", 80},
    {"WAvgAskPri", 88},
    {"AltWAvgAskPri", 92},
    {"EtfBuyNumber", 96},
    {"EtfBuyVolume", 100},
    {"EtfBuyMoney", 108},
    {"EtfSellNumber", 116},
    {"EtfSellVolume", 120},
    {"ETFSellMoney", 128},
    {"YieldToMatu", 136},
    {"TotWarExNum", 140},
    {"WarLowerPri", 148},
    {"WarUpperPri", 156},
    {"WiDBuyNum", 164},
    {"WiDBuyVol", 168},
    {"WiDBuyMon", 176},
    {"WiDSellNum", 184},
    {"WiDSellVol", 188},
    {"WiDSellMon", 196},
    {"TotBidNum", 204},
    {"TotSellNum", 208},
    {"MaxBidDur", 212},
    {"MaxSellDur", 216},
    {"BidNum", 220},
    {"SellNum", 224},
    {"BidLevels", 228},
    {"SellLevels", 236},
    {"IOPV", 244},
}};

constexpr std::array<MemberLayout, 5> kSh44BidLevelMembers = {{
    {"PriLevOpera", 0},
    {"OrderPrice", 4},
    {"OrderVol", 8},
    {"OrderNum", 16},
    {"NOrders", 20},
}};

constexpr std::array<MemberLayout, 3> kSh44NOrdersMembers = {{
    {"OrderQueOper", 0},
    {"OrderQueID", 4},
    {"OrderQty", 8},
}};

constexpr std::array<MemberLayout, 5> kSh44SellLevelMembers = {{
    {"PriLevOpera", 0},
    {"OrderPrice", 4},
    {"OrderVol", 8},
    {"OrderNum", 16},
    {"NoOrders", 20},
}};

constexpr std::array<MemberLayout, 3> kSh44NoOrdersMembers = {{
    {"OrderQueOper", 0},
    {"OrderQueID", 4},
    {"OrderQty", 8},
}};

constexpr std::array<MemberLayout, 11> kSh24Members = {{
    {"BizIndex", 0},
    {"Channel", 8},
    {"SecurityID", 12},
    {"TickTime", 18},
    {"Type", 22},
    {"BuyOrderNO", 28},
    {"SellOrderNO", 36},
    {"Price", 44},
    {"Qty", 48},
    {"TradeMoney", 56},
    {"TickBSFlag", 64},
}};

constexpr std::array<MemberLayout, 30> kSz28Members = {{
    {"UpdateTime", 0},
    {"ChannelNo", 4},
    {"MDStreamID", 8},
    {"SecurityID", 14},
    {"SecurityIDSource", 20},
    {"TradingPhaseCode", 26},
    {"PreCloPrice", 32},
    {"TurnNum", 40},
    {"Volume", 48},
    {"Turnover", 56},
    {"LastPrice", 64},
    {"OpenPrice", 72},
    {"HighPrice", 80},
    {"LowPrice", 88},
    {"DifPrice1", 96},
    {"DifPrice2", 104},
    {"PE1", 112},
    {"PE2", 120},
    {"PreCloseIOPV", 128},
    {"IOPV", 136},
    {"TotalOfferQty", 144},
    {"WeightedAvgOfferPx", 152},
    {"TotalBidQty", 160},
    {"WeightedAvgBidPx", 168},
    {"HighLimitPrice", 176},
    {"LowLimitPrice", 184},
    {"OpenInt", 192},
    {"OptPremiumRatio", 200},
    {"BidPriceLevel", 208},
    {"AskPriceLevel", 216},
}};

constexpr std::array<MemberLayout, 4> kSz28BidLevelMembers = {{
    {"Volume", 0},
    {"Price", 8},
    {"NumOrders", 16},
    {"Orders", 20},
}};

constexpr std::array<MemberLayout, 1> kSz28OrdersMembers = {{
    {"OrderQty", 0},
}};

constexpr std::array<MemberLayout, 4> kSz28AskLevelMembers = {{
    {"Volume", 0},
    {"Price", 8},
    {"NumOrders", 16},
    {"Orders", 20},
}};

constexpr std::array<MemberLayout, 1> kSz28AskOrdersMembers = {{
    {"OrderQty", 0},
}};

constexpr std::array<MemberLayout, 10> kSz33Members = {{
    {"ChannelNo", 0},
    {"ApplSeqNum", 4},
    {"MDStreamID", 12},
    {"SecurityID", 18},
    {"SecurityIDSource", 24},
    {"Price", 30},
    {"OrderQty", 38},
    {"Side", 46},
    {"TransactTime", 50},
    {"OrdType", 54},
}};

constexpr std::array<MemberLayout, 11> kSz36Members = {{
    {"ChannelNo", 0},
    {"ApplSeqNum", 4},
    {"MDStreamID", 12},
    {"BidApplSeqNum", 18},
    {"OfferApplSeqNum", 26},
    {"SecurityID", 34},
    {"SecurityIDSource", 40},
    {"LastPx", 46},
    {"LastQty", 54},
    {"ExecType", 62},
    {"TransactTime", 66},
}};

constexpr std::array<MemberLayout, 11> kCombinedTickMembers = {{
    {"ChannelNo", 0},
    {"ApplSeqNum", 4},
    {"MDStreamID", 12},
    {"SecurityID", 18},
    {"SecurityIDSource", 24},
    {"TransactTime", 30},
    {"Type", 34},
    {"BidApplSeqNum", 38},
    {"OfferApplSeqNum", 46},
    {"Price", 54},
    {"Qty", 62},
}};

constexpr std::array<TypeLayout, 24> kAbiTypes = {{
    {"MDLMessageHead", 23, 1, kMessageHeadMembers},
    {"MDLAnsiString", 6, 1, kStringMembers},
    {"MDLUTF8String", 6, 1, kStringMembers},
    {"MDLList", 8, 1, kListMembers},
    {"LogonResponse", 24, 1, kLogonResponseMembers},
    {"LogonResponse::ServicesItem",
     16,
     1,
     kControlServiceMembers},
    {"LogonResponse::ServicesItem::MessagesItem",
     8,
     1,
     kControlMessageMembers},
    {"SubscribeResponse", 8, 1, kSubscribeResponseMembers},
    {"SubscribeResponse::ServicesItem",
     16,
     1,
     kControlServiceMembers},
    {"SubscribeResponse::ServicesItem::MessagesItem",
     8,
     1,
     kControlMessageMembers},
    {"SHL2MarketData", 248, 1, kSh44Members},
    {"SHL2MarketData::BidLevelsItem",
     28,
     1,
     kSh44BidLevelMembers},
    {"SHL2MarketData::BidLevelsItem::NOrdersItem",
     16,
     1,
     kSh44NOrdersMembers},
    {"SHL2MarketData::SellLevelsItem",
     28,
     1,
     kSh44SellLevelMembers},
    {"SHL2MarketData::SellLevelsItem::NoOrdersItem",
     16,
     1,
     kSh44NoOrdersMembers},
    {"NGTSTick", 70, 1, kSh24Members},
    {"Snapshot300111_v2", 224, 1, kSz28Members},
    {"Snapshot300111_v2::BidPriceLevelItem",
     28,
     1,
     kSz28BidLevelMembers},
    {"Snapshot300111_v2::BidPriceLevelItem::OrdersItem",
     8,
     1,
     kSz28OrdersMembers},
    {"Snapshot300111_v2::AskPriceLevelItem",
     28,
     1,
     kSz28AskLevelMembers},
    {"Snapshot300111_v2::AskPriceLevelItem::OrdersItem",
     8,
     1,
     kSz28AskOrdersMembers},
    {"Order300192_v2", 58, 1, kSz33Members},
    {"Transaction300191_v2", 70, 1, kSz36Members},
    {"CombinedTick", 70, 1, kCombinedTickMembers},
}};

constexpr std::array<std::string_view, 7> kElfNeeded = {{
    "ld-linux-x86-64.so.2",
    "libc.so.6",
    "libgcc_s.so.1",
    "libm.so.6",
    "libpthread.so.0",
    "librt.so.1",
    "libstdc++.so.6",
}};

// Sorted bytewise.  These are the exact DT_VERNEED names, not strings found
// by scanning arbitrary payload bytes.
constexpr std::array<std::string_view, 25> kRequiredSymbolVersions = {{
    "CXXABI_1.3",
    "CXXABI_1.3.1",
    "CXXABI_1.3.3",
    "CXXABI_1.3.5",
    "GCC_3.0",
    "GLIBCXX_3.4",
    "GLIBCXX_3.4.10",
    "GLIBCXX_3.4.11",
    "GLIBCXX_3.4.14",
    "GLIBCXX_3.4.15",
    "GLIBCXX_3.4.18",
    "GLIBCXX_3.4.19",
    "GLIBCXX_3.4.5",
    "GLIBCXX_3.4.9",
    "GLIBC_2.12",
    "GLIBC_2.14",
    "GLIBC_2.15",
    "GLIBC_2.2.5",
    "GLIBC_2.3",
    "GLIBC_2.3.2",
    "GLIBC_2.3.4",
    "GLIBC_2.4",
    "GLIBC_2.7",
    "GLIBC_2.8",
    "GLIBC_2.9",
}};

constexpr std::array<NumericConstant, 20> kProtocolConstants = {{
    {"MDLSID_MDL_API", 1},
    {"MDLSID_MDL_SYS", 2},
    {"MDLSID_MDL_SHL2", 4},
    {"MDLSID_MDL_SZL2", 6},
    {"MDLVID_MDL_SYS", 101},
    {"MDLMID_MDL_SYS_Logon", 1},
    {"MDLMID_MDL_SYS_LogonResponse", 2},
    {"MDLMID_MDL_SYS_SubscribeRequest", 22},
    {"MDLMID_MDL_SYS_SubscribeResponse", 23},
    {"MDLEC_OK", 0},
    {"MDLEID_BINARY", 1},
    {"MDLEID_FAST", 2},
    {"MDLEID_JSON", 3},
    {"MDLEID_PROTOBUF", 4},
    {"MDLEID_CSV", 5},
    {"MDLEID_MKTDATA", 6},
    {"MDLEID_MKTPRO", 7},
    {"MDLEID_PACKAGE", 64},
    {"MDLEID_DEFLATE", 128},
    {"MDLEID_DEFLATE_PROTOBUF", 132},
}};

constexpr std::array<MessageContract, 8> kMessages = {{
    {"mdl_shl2_msg::SHL2MarketData",
     {4, 101, 4},
     SubscriptionPolicy::Required},
    {"mdl_shl2_msg::NGTSTick",
     {4, 101, 24},
     SubscriptionPolicy::Required},
    {"mdl_szl2_msg::Snapshot300111_v2",
     {6, 101, 28},
     SubscriptionPolicy::Required},
    {"mdl_szl2_msg::Order300192_v2",
     {6, 101, 33},
     SubscriptionPolicy::Required},
    {"mdl_szl2_msg::Transaction300191_v2",
     {6, 101, 36},
     SubscriptionPolicy::Required},
    {"mdl_shl2_msg::SHL2Index",
     {4, 101, 6},
     SubscriptionPolicy::Optional},
    {"mdl_szl2_msg::Snapshot309011_v2",
     {6, 101, 29},
     SubscriptionPolicy::Optional},
    {"mdl_szl2_msg::CombinedTick",
     {6, 101, 53},
     SubscriptionPolicy::Forbidden},
}};

constexpr VendorBaseline kApprovedBaseline = {
    2,
    213234,
    "23830887091d35875c653d97a4874f27f04b4cc36a69de37a952510d0701cc71",
    kElfNeeded,
    kRequiredSymbolVersions,
    "GLIBC_2.15",
    "GLIBCXX_3.4.19",
    "CXXABI_1.3.5",
    "GCC_3.0",
    kProtocolConstants,
    kAbiTypes,
    kMessages,
};

std::string PolicyName(SubscriptionPolicy policy) {
    switch (policy) {
    case SubscriptionPolicy::Required:
        return "required";
    case SubscriptionPolicy::Optional:
        return "optional";
    case SubscriptionPolicy::Forbidden:
        return "forbidden";
    }
    return "invalid";
}

void AppendJsonString(std::string* output, std::string_view value) {
    constexpr char kHex[] = "0123456789abcdef";
    output->push_back('"');
    for (const unsigned char character : value) {
        switch (character) {
        case '"':
            output->append("\\\"");
            break;
        case '\\':
            output->append("\\\\");
            break;
        case '\b':
            output->append("\\b");
            break;
        case '\f':
            output->append("\\f");
            break;
        case '\n':
            output->append("\\n");
            break;
        case '\r':
            output->append("\\r");
            break;
        case '\t':
            output->append("\\t");
            break;
        default:
            if (character < 0x20U) {
                output->append("\\u00");
                output->push_back(kHex[(character >> 4U) & 0x0fU]);
                output->push_back(kHex[character & 0x0fU]);
            } else {
                output->push_back(static_cast<char>(character));
            }
            break;
        }
    }
    output->push_back('"');
}

std::string RenderApprovedBaselineJson() {
    const VendorBaseline& baseline = kApprovedBaseline;
    std::ostringstream output;
    output << "{\n"
           << "  \"schema_version\": " << baseline.schema_version << ",\n"
           << "  \"vendor\": \"DataYes MDL\",\n"
           << "  \"sdk_version\": " << baseline.sdk_version << ",\n"
           << "  \"artifacts\": {\n"
           << "    \"sdk_archive_sha256\": \""
           << baseline.sdk_archive_sha256 << "\"\n"
           << "  },\n"
           << "  \"elf\": {\n"
           << "    \"class\": \"ELF64\",\n"
           << "    \"data\": \"little-endian\",\n"
           << "    \"type\": \"ET_DYN\",\n"
           << "    \"machine\": \"EM_X86_64\",\n"
           << "    \"soname\": null,\n"
           << "    \"dt_needed\": [\n";
    for (std::size_t index = 0; index < baseline.elf_needed.size(); ++index) {
        output << "      \"" << baseline.elf_needed[index] << "\"";
        output << (index + 1U == baseline.elf_needed.size() ? "\n" : ",\n");
    }
    output << "    ]\n"
           << "  },\n"
           << "  \"compiler_abi\": {\n"
           << "    \"required_symbol_versions\": [\n";
    for (std::size_t index = 0;
         index < baseline.required_symbol_versions.size();
         ++index) {
        output << "      \""
               << baseline.required_symbol_versions[index] << "\""
               << (index + 1U ==
                           baseline.required_symbol_versions.size()
                       ? "\n"
                       : ",\n");
    }
    output << "    ],\n"
           << "    \"max_required\": {\"glibc\": \""
           << baseline.glibc_version_max
           << "\", \"glibcxx\": \"" << baseline.glibcxx_version_max
           << "\", \"cxxabi\": \"" << baseline.cxxabi_version_max
           << "\", \"libgcc\": \"" << baseline.libgcc_version_max
           << "\"}\n"
           << "  },\n"
           << "  \"protocol_constants\": {\n";
    for (std::size_t index = 0;
         index < baseline.protocol_constants.size();
         ++index) {
        const NumericConstant& constant =
            baseline.protocol_constants[index];
        output << "    \"" << constant.name << "\": "
               << constant.value
               << (index + 1U == baseline.protocol_constants.size()
                       ? "\n"
                       : ",\n");
    }
    output << "  },\n"
           << "  \"abi\": [\n";
    for (std::size_t type_index = 0;
         type_index < baseline.abi_types.size();
         ++type_index) {
        const TypeLayout& type = baseline.abi_types[type_index];
        output << "    {\"name\": \"" << type.name << "\", \"size\": "
               << type.size << ", \"align\": " << type.alignment
               << ", \"members\": {";
        for (std::size_t member_index = 0;
             member_index < type.members.size();
             ++member_index) {
            if (member_index != 0U) {
                output << ", ";
            }
            output << "\"" << type.members[member_index].name << "\": "
                   << type.members[member_index].offset;
        }
        output << "}}";
        output << (type_index + 1U == baseline.abi_types.size() ? "\n"
                                                               : ",\n");
    }
    output << "  ],\n"
           << "  \"messages\": [\n";
    for (std::size_t index = 0; index < baseline.messages.size(); ++index) {
        const MessageContract& message = baseline.messages[index];
        output << "    {\"cpp_type\": \"" << message.cpp_type
               << "\", \"service_id\": "
               << static_cast<unsigned int>(message.key.service_id)
               << ", \"service_version\": " << message.key.service_version
               << ", \"message_id\": " << message.key.message_id
               << ", \"policy\": \"" << PolicyName(message.policy) << "\"}";
        output << (index + 1U == baseline.messages.size() ? "\n" : ",\n");
    }
    output << "  ]\n"
           << "}\n";
    return output.str();
}

void SetError(std::string* error, std::string message) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        *error = std::move(message);
    } catch (...) {
    }
}

bool CheckedAdd(std::uint64_t lhs,
                std::uint64_t rhs,
                std::uint64_t* result) noexcept {
    if (rhs > std::numeric_limits<std::uint64_t>::max() - lhs) {
        return false;
    }
    *result = lhs + rhs;
    return true;
}

bool CheckedMultiply(std::uint64_t lhs,
                     std::uint64_t rhs,
                     std::uint64_t* result) noexcept {
    if (lhs != 0U &&
        rhs > std::numeric_limits<std::uint64_t>::max() / lhs) {
        return false;
    }
    *result = lhs * rhs;
    return true;
}

class RandomAccessFile {
public:
    bool Open(const std::filesystem::path& path, std::string* error) {
        std::error_code size_error;
        const std::uintmax_t size =
            std::filesystem::file_size(path, size_error);
        if (size_error) {
            SetError(error,
                     "cannot stat ELF file " + path.string() + ": " +
                         size_error.message());
            return false;
        }
        if (size > std::numeric_limits<std::uint64_t>::max()) {
            SetError(error, "ELF file size is not representable");
            return false;
        }
        size_ = static_cast<std::uint64_t>(size);
        input_.open(path, std::ios::binary);
        if (!input_.is_open()) {
            SetError(error, "cannot open ELF file: " + path.string());
            return false;
        }
        return true;
    }

    std::uint64_t size() const noexcept {
        return size_;
    }

    bool Read(std::uint64_t offset,
              std::span<std::byte> output,
              std::string* error) {
        std::uint64_t end = 0;
        if (!CheckedAdd(offset,
                        static_cast<std::uint64_t>(output.size()),
                        &end) ||
            end > size_) {
            SetError(error, "ELF read exceeds file bounds");
            return false;
        }
        if (offset >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::streamoff>::max())) {
            SetError(error, "ELF offset exceeds stream range");
            return false;
        }
        input_.clear();
        input_.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
        if (!input_) {
            SetError(error, "cannot seek ELF file");
            return false;
        }
        if (!output.empty()) {
            input_.read(reinterpret_cast<char*>(output.data()),
                        static_cast<std::streamsize>(output.size()));
        }
        if (!input_) {
            SetError(error, "short read from ELF file");
            return false;
        }
        return true;
    }

    template <typename T>
    bool ReadObject(std::uint64_t offset, T* value, std::string* error) {
        return Read(offset,
                    std::as_writable_bytes(std::span<T>{value, 1U}),
                    error);
    }

private:
    std::ifstream input_;
    std::uint64_t size_ = 0;
};

std::uint64_t AlignFour(std::uint64_t value, bool* ok) noexcept {
    std::uint64_t adjusted = 0;
    if (!CheckedAdd(value, 3U, &adjusted)) {
        *ok = false;
        return 0;
    }
    return adjusted & ~std::uint64_t{3U};
}

std::string BytesHex(std::span<const std::byte> bytes) {
    constexpr char kHex[] = "0123456789abcdef";
    std::string result(bytes.size() * 2U, '\0');
    for (std::size_t index = 0; index < bytes.size(); ++index) {
        const unsigned int value =
            std::to_integer<unsigned int>(bytes[index]);
        result[index * 2U] = kHex[(value >> 4U) & 0x0fU];
        result[index * 2U + 1U] = kHex[value & 0x0fU];
    }
    return result;
}

bool ParseBuildId(RandomAccessFile* file,
                  const std::vector<Elf64_Phdr>& program_headers,
                  std::string* build_id,
                  std::string* error) {
    std::optional<std::string> found;
    for (const Elf64_Phdr& header : program_headers) {
        if (header.p_type != PT_NOTE) {
            continue;
        }
        constexpr std::uint64_t kMaximumNoteBytes = 16U * 1024U * 1024U;
        if (header.p_filesz > kMaximumNoteBytes) {
            SetError(error, "PT_NOTE exceeds safety limit");
            return false;
        }
        std::vector<std::byte> bytes(
            static_cast<std::size_t>(header.p_filesz));
        if (!file->Read(header.p_offset, bytes, error)) {
            return false;
        }
        std::uint64_t cursor = 0;
        while (cursor < bytes.size()) {
            if (bytes.size() - cursor < sizeof(Elf64_Nhdr)) {
                SetError(error, "truncated ELF note header");
                return false;
            }
            Elf64_Nhdr note{};
            std::memcpy(&note, bytes.data() + cursor, sizeof(note));
            cursor += sizeof(note);

            bool aligned = true;
            const std::uint64_t name_bytes =
                AlignFour(note.n_namesz, &aligned);
            const std::uint64_t description_bytes =
                AlignFour(note.n_descsz, &aligned);
            std::uint64_t after_name = 0;
            std::uint64_t after_description = 0;
            if (!aligned ||
                !CheckedAdd(cursor, name_bytes, &after_name) ||
                !CheckedAdd(after_name,
                            description_bytes,
                            &after_description) ||
                after_description > bytes.size()) {
                SetError(error, "ELF note lengths exceed PT_NOTE");
                return false;
            }

            const std::span<const std::byte> name{
                bytes.data() + cursor,
                static_cast<std::size_t>(note.n_namesz)};
            const std::span<const std::byte> description{
                bytes.data() + after_name,
                static_cast<std::size_t>(note.n_descsz)};
            const bool is_gnu =
                name.size() == 4U &&
                std::to_integer<unsigned char>(name[0]) == 'G' &&
                std::to_integer<unsigned char>(name[1]) == 'N' &&
                std::to_integer<unsigned char>(name[2]) == 'U' &&
                name[3] == std::byte{0};
            if (is_gnu && note.n_type == NT_GNU_BUILD_ID) {
                const std::string candidate = BytesHex(description);
                if (found.has_value()) {
                    SetError(error, "multiple GNU build-id notes are rejected");
                    return false;
                }
                found = candidate;
            }
            cursor = after_description;
        }
    }
    if (!found.has_value()) {
        SetError(error, "GNU build-id note is missing");
        return false;
    }
    *build_id = std::move(*found);
    return true;
}

bool VirtualAddressToFileOffset(
    std::uint64_t virtual_address,
    std::uint64_t length,
    const std::vector<Elf64_Phdr>& program_headers,
    std::uint64_t* file_offset) noexcept {
    for (const Elf64_Phdr& header : program_headers) {
        if (header.p_type != PT_LOAD ||
            virtual_address < header.p_vaddr) {
            continue;
        }
        const std::uint64_t delta = virtual_address - header.p_vaddr;
        if (delta > header.p_filesz ||
            length > header.p_filesz - delta) {
            continue;
        }
        return CheckedAdd(header.p_offset, delta, file_offset);
    }
    return false;
}

bool DynamicString(std::span<const std::byte> table,
                   std::uint64_t offset,
                   std::string* value,
                   std::string* error) {
    if (offset >= table.size()) {
        SetError(error, "dynamic string offset exceeds DT_STRSZ");
        return false;
    }
    const char* const begin =
        reinterpret_cast<const char*>(table.data() + offset);
    const std::size_t remaining =
        table.size() - static_cast<std::size_t>(offset);
    const void* const terminator = std::memchr(begin, '\0', remaining);
    if (terminator == nullptr) {
        SetError(error, "dynamic string is not NUL terminated");
        return false;
    }
    const char* const end = static_cast<const char*>(terminator);
    constexpr std::size_t kMaximumDynamicStringBytes = 4096U;
    if (static_cast<std::size_t>(end - begin) >
        kMaximumDynamicStringBytes) {
        SetError(error, "dynamic string exceeds 4096 bytes");
        return false;
    }
    value->assign(begin, end);
    return true;
}

template <typename Type>
bool ReadVirtualObject(
    RandomAccessFile* file,
    std::uint64_t virtual_address,
    const std::vector<Elf64_Phdr>& program_headers,
    Type* value,
    std::string* error) {
    std::uint64_t file_offset = 0U;
    if (!VirtualAddressToFileOffset(
            virtual_address,
            static_cast<std::uint64_t>(sizeof(Type)),
            program_headers,
            &file_offset)) {
        SetError(error,
                 "ELF virtual object is not backed by a PT_LOAD file range");
        return false;
    }
    return file->ReadObject(file_offset, value, error);
}

bool ParseVersionRequirements(
    RandomAccessFile* file,
    const std::vector<Elf64_Phdr>& program_headers,
    std::span<const std::byte> string_table,
    std::uint64_t first_need_address,
    std::uint64_t need_count,
    std::span<const std::string> needed_libraries,
    std::vector<std::string>* versions,
    std::string* error) {
    constexpr std::uint64_t kMaximumNeedEntries = 4096U;
    constexpr std::uint16_t kMaximumAuxEntries = 4096U;
    constexpr std::size_t kMaximumAggregateAuxEntries = 65536U;
    if (first_need_address == 0U || need_count == 0U ||
        need_count > kMaximumNeedEntries) {
        SetError(error, "DT_VERNEED metadata is missing or exceeds limits");
        return false;
    }

    versions->clear();
    std::uint64_t need_address = first_need_address;
    std::size_t aggregate_aux_entries = 0U;
    for (std::uint64_t need_index = 0U;
         need_index < need_count;
         ++need_index) {
        Elf64_Verneed need{};
        if (!ReadVirtualObject(file,
                               need_address,
                               program_headers,
                               &need,
                               error)) {
            return false;
        }
        if (need.vn_version != VER_NEED_CURRENT ||
            need.vn_cnt == 0U ||
            need.vn_cnt > kMaximumAuxEntries ||
            need.vn_aux == 0U) {
            SetError(error, "malformed ELF version-need entry");
            return false;
        }
        if (need.vn_cnt >
            kMaximumAggregateAuxEntries -
                aggregate_aux_entries) {
            SetError(
                error,
                "aggregate ELF version requirements exceed safety limit");
            return false;
        }
        aggregate_aux_entries += need.vn_cnt;

        std::string library;
        if (!DynamicString(
                string_table, need.vn_file, &library, error)) {
            return false;
        }
        if (library.empty() ||
            std::find(needed_libraries.begin(),
                      needed_libraries.end(),
                      library) == needed_libraries.end()) {
            SetError(error,
                     "ELF version requirement references an unknown library");
            return false;
        }

        std::uint64_t aux_address = 0U;
        if (!CheckedAdd(
                need_address, need.vn_aux, &aux_address)) {
            SetError(error, "ELF version auxiliary address overflows");
            return false;
        }
        for (std::uint16_t aux_index = 0U;
             aux_index < need.vn_cnt;
             ++aux_index) {
            Elf64_Vernaux auxiliary{};
            if (!ReadVirtualObject(file,
                                   aux_address,
                                   program_headers,
                                   &auxiliary,
                                   error)) {
                return false;
            }
            std::string version;
            if (!DynamicString(string_table,
                               auxiliary.vna_name,
                               &version,
                               error) ||
                version.empty()) {
                SetError(error, "empty ELF required symbol version");
                return false;
            }
            versions->push_back(std::move(version));

            const bool final_aux =
                aux_index + 1U == need.vn_cnt;
            if ((final_aux && auxiliary.vna_next != 0U) ||
                (!final_aux && auxiliary.vna_next == 0U)) {
                SetError(error,
                         "ELF version auxiliary chain/count mismatch");
                return false;
            }
            if (!final_aux &&
                !CheckedAdd(aux_address,
                            auxiliary.vna_next,
                            &aux_address)) {
                SetError(error,
                         "ELF version auxiliary chain overflows");
                return false;
            }
        }

        const bool final_need = need_index + 1U == need_count;
        if ((final_need && need.vn_next != 0U) ||
            (!final_need && need.vn_next == 0U)) {
            SetError(error, "ELF version-need chain/count mismatch");
            return false;
        }
        if (!final_need &&
            !CheckedAdd(
                need_address, need.vn_next, &need_address)) {
            SetError(error, "ELF version-need chain overflows");
            return false;
        }
    }
    std::sort(versions->begin(), versions->end());
    versions->erase(
        std::unique(versions->begin(), versions->end()),
        versions->end());
    return true;
}

bool ParseDynamic(RandomAccessFile* file,
                  const std::vector<Elf64_Phdr>& program_headers,
                  std::optional<std::string>* soname,
                  std::vector<std::string>* needed,
                  std::vector<std::string>* required_symbol_versions,
                  std::string* error) {
    const Elf64_Phdr* dynamic_header = nullptr;
    for (const Elf64_Phdr& header : program_headers) {
        if (header.p_type != PT_DYNAMIC) {
            continue;
        }
        if (dynamic_header != nullptr) {
            SetError(error, "multiple PT_DYNAMIC segments are rejected");
            return false;
        }
        dynamic_header = &header;
    }
    if (dynamic_header == nullptr) {
        SetError(error, "PT_DYNAMIC is missing");
        return false;
    }
    if (dynamic_header->p_filesz == 0U ||
        dynamic_header->p_filesz % sizeof(Elf64_Dyn) != 0U) {
        SetError(error, "PT_DYNAMIC has an invalid size");
        return false;
    }

    const std::uint64_t count =
        dynamic_header->p_filesz / sizeof(Elf64_Dyn);
    if (count > 65536U) {
        SetError(error, "PT_DYNAMIC entry count exceeds safety limit");
        return false;
    }
    std::vector<Elf64_Dyn> entries(static_cast<std::size_t>(count));
    if (!file->Read(
            dynamic_header->p_offset,
            std::as_writable_bytes(std::span{entries}),
            error)) {
        return false;
    }

    std::optional<std::uint64_t> string_table_address;
    std::optional<std::uint64_t> string_table_size;
    std::optional<std::uint64_t> soname_offset;
    std::optional<std::uint64_t> version_need_address;
    std::optional<std::uint64_t> version_need_count;
    std::vector<std::uint64_t> needed_offsets;
    bool saw_null = false;
    for (const Elf64_Dyn& entry : entries) {
        if (entry.d_tag == DT_NULL) {
            saw_null = true;
            break;
        }
        switch (entry.d_tag) {
        case DT_STRTAB:
            if (string_table_address.has_value()) {
                SetError(error, "duplicate DT_STRTAB");
                return false;
            }
            string_table_address = entry.d_un.d_ptr;
            break;
        case DT_STRSZ:
            if (string_table_size.has_value()) {
                SetError(error, "duplicate DT_STRSZ");
                return false;
            }
            string_table_size = entry.d_un.d_val;
            break;
        case DT_NEEDED: {
            constexpr std::size_t kMaximumNeededLibraries = 128U;
            if (needed_offsets.size() >=
                kMaximumNeededLibraries) {
                SetError(
                    error,
                    "DT_NEEDED entry count exceeds safety limit");
                return false;
            }
            needed_offsets.push_back(entry.d_un.d_val);
            break;
        }
        case DT_SONAME:
            if (soname_offset.has_value()) {
                SetError(error, "duplicate DT_SONAME");
                return false;
            }
            soname_offset = entry.d_un.d_val;
            break;
        case DT_VERNEED:
            if (version_need_address.has_value()) {
                SetError(error, "duplicate DT_VERNEED");
                return false;
            }
            version_need_address = entry.d_un.d_ptr;
            break;
        case DT_VERNEEDNUM:
            if (version_need_count.has_value()) {
                SetError(error, "duplicate DT_VERNEEDNUM");
                return false;
            }
            version_need_count = entry.d_un.d_val;
            break;
        default:
            break;
        }
    }
    if (!saw_null) {
        SetError(error, "PT_DYNAMIC lacks a DT_NULL terminator");
        return false;
    }
    if (!string_table_address.has_value() ||
        !string_table_size.has_value() ||
        *string_table_size == 0U ||
        *string_table_size > 64U * 1024U * 1024U) {
        SetError(error, "dynamic string table metadata is invalid");
        return false;
    }

    std::uint64_t string_table_offset = 0;
    if (!VirtualAddressToFileOffset(*string_table_address,
                                    *string_table_size,
                                    program_headers,
                                    &string_table_offset)) {
        SetError(error, "DT_STRTAB is not backed by a PT_LOAD file range");
        return false;
    }
    std::vector<std::byte> string_table(
        static_cast<std::size_t>(*string_table_size));
    if (!file->Read(string_table_offset, string_table, error)) {
        return false;
    }

    needed->clear();
    needed->reserve(needed_offsets.size());
    for (const std::uint64_t offset : needed_offsets) {
        std::string value;
        if (!DynamicString(string_table, offset, &value, error)) {
            return false;
        }
        if (value.empty()) {
            SetError(error, "empty DT_NEEDED entry is rejected");
            return false;
        }
        needed->push_back(std::move(value));
    }
    if (soname_offset.has_value()) {
        std::string value;
        if (!DynamicString(string_table, *soname_offset, &value, error)) {
            return false;
        }
        *soname = std::move(value);
    } else {
        soname->reset();
    }
    if (!version_need_address.has_value() ||
        !version_need_count.has_value()) {
        SetError(error, "DT_VERNEED/DT_VERNEEDNUM is missing");
        return false;
    }
    return ParseVersionRequirements(
        file,
        program_headers,
        string_table,
        *version_need_address,
        *version_need_count,
        *needed,
        required_symbol_versions,
        error);
}

bool ParseCompilerComment(
    RandomAccessFile* file,
    const Elf64_Ehdr& elf_header,
    std::string* comment_sha256,
    std::vector<std::string>* producers,
    std::string* error) {
    if (elf_header.e_shentsize != sizeof(Elf64_Shdr) ||
        elf_header.e_shnum == 0U ||
        elf_header.e_shnum == SHN_UNDEF ||
        elf_header.e_shstrndx == SHN_UNDEF ||
        elf_header.e_shstrndx == SHN_XINDEX ||
        elf_header.e_shstrndx >= elf_header.e_shnum) {
        SetError(error, "unsupported or malformed ELF section metadata");
        return false;
    }

    std::uint64_t table_bytes = 0U;
    std::uint64_t table_end = 0U;
    if (!CheckedMultiply(elf_header.e_shnum,
                         sizeof(Elf64_Shdr),
                         &table_bytes) ||
        !CheckedAdd(
            elf_header.e_shoff, table_bytes, &table_end) ||
        table_end > file->size()) {
        SetError(error, "ELF section header table exceeds file bounds");
        return false;
    }
    std::vector<Elf64_Shdr> sections(elf_header.e_shnum);
    if (!file->Read(
            elf_header.e_shoff,
            std::as_writable_bytes(std::span{sections}),
            error)) {
        return false;
    }

    const Elf64_Shdr& names_header =
        sections[elf_header.e_shstrndx];
    constexpr std::uint64_t kMaximumSectionNames = 16U * 1024U * 1024U;
    std::uint64_t names_end = 0U;
    if (names_header.sh_size == 0U ||
        names_header.sh_size > kMaximumSectionNames ||
        !CheckedAdd(names_header.sh_offset,
                    names_header.sh_size,
                    &names_end) ||
        names_end > file->size()) {
        SetError(error, "ELF section-name string table is invalid");
        return false;
    }
    std::vector<std::byte> section_names(
        static_cast<std::size_t>(names_header.sh_size));
    if (!file->Read(
            names_header.sh_offset, section_names, error)) {
        return false;
    }

    const Elf64_Shdr* comment_header = nullptr;
    for (const Elf64_Shdr& section : sections) {
        std::string name;
        if (!DynamicString(
                section_names, section.sh_name, &name, error)) {
            return false;
        }
        if (name != ".comment") {
            continue;
        }
        if (comment_header != nullptr) {
            SetError(error, "multiple .comment sections are rejected");
            return false;
        }
        comment_header = &section;
    }
    constexpr std::uint64_t kMaximumCommentBytes = 1024U * 1024U;
    if (comment_header == nullptr ||
        comment_header->sh_type != SHT_PROGBITS ||
        comment_header->sh_size == 0U ||
        comment_header->sh_size > kMaximumCommentBytes) {
        SetError(error, "ELF .comment section is missing or invalid");
        return false;
    }
    std::uint64_t comment_end = 0U;
    if (!CheckedAdd(comment_header->sh_offset,
                    comment_header->sh_size,
                    &comment_end) ||
        comment_end > file->size()) {
        SetError(error, "ELF .comment section exceeds file bounds");
        return false;
    }

    std::vector<std::byte> comment(
        static_cast<std::size_t>(comment_header->sh_size));
    if (!file->Read(comment_header->sh_offset, comment, error)) {
        return false;
    }
    *comment_sha256 =
        common::Sha256Hex(common::ComputeSha256(comment));

    producers->clear();
    std::size_t cursor = 0U;
    while (cursor < comment.size()) {
        const char* const begin =
            reinterpret_cast<const char*>(comment.data() + cursor);
        const std::size_t remaining = comment.size() - cursor;
        const void* const terminator =
            std::memchr(begin, '\0', remaining);
        if (terminator == nullptr) {
            SetError(error, "ELF .comment string is not NUL terminated");
            return false;
        }
        const char* const end =
            static_cast<const char*>(terminator);
        if (end != begin) {
            producers->emplace_back(begin, end);
        }
        cursor += static_cast<std::size_t>(end - begin) + 1U;
    }
    if (producers->empty()) {
        SetError(error, "ELF .comment contains no compiler producer");
        return false;
    }
    return true;
}

std::string Number(std::uint64_t value) {
    return std::to_string(value);
}

std::string JoinStrings(std::span<const std::string> values) {
    std::string result = "[";
    for (std::size_t index = 0; index < values.size(); ++index) {
        if (index != 0U) {
            result.append(",");
        }
        result.append(values[index]);
    }
    result.push_back(']');
    return result;
}

std::string JoinStringViews(std::span<const std::string_view> values) {
    std::string result = "[";
    for (std::size_t index = 0; index < values.size(); ++index) {
        if (index != 0U) {
            result.append(",");
        }
        result.append(values[index]);
    }
    result.push_back(']');
    return result;
}

bool ParseVersionComponents(std::string_view value,
                            std::string_view prefix,
                            std::vector<std::uint64_t>* components) {
    if (!value.starts_with(prefix) || value.size() == prefix.size()) {
        return false;
    }
    components->clear();
    std::size_t cursor = prefix.size();
    while (cursor < value.size()) {
        std::uint64_t component = 0U;
        const std::size_t start = cursor;
        while (cursor < value.size() && value[cursor] != '.') {
            const char character = value[cursor];
            if (character < '0' || character > '9') {
                return false;
            }
            const std::uint64_t digit =
                static_cast<std::uint64_t>(character - '0');
            if (component >
                (std::numeric_limits<std::uint64_t>::max() - digit) /
                    10U) {
                return false;
            }
            component = component * 10U + digit;
            ++cursor;
        }
        if (cursor == start) {
            return false;
        }
        components->push_back(component);
        if (cursor == value.size()) {
            break;
        }
        ++cursor;
        if (cursor == value.size()) {
            return false;
        }
    }
    return !components->empty();
}

std::string MaximumRequiredVersion(
    std::span<const std::string> versions,
    std::string_view prefix) {
    std::string maximum;
    std::vector<std::uint64_t> maximum_components;
    for (const std::string& version : versions) {
        if (!version.starts_with(prefix)) {
            continue;
        }
        std::vector<std::uint64_t> components;
        if (!ParseVersionComponents(
                version, prefix, &components)) {
            return "<malformed>";
        }
        if (maximum.empty() ||
            std::lexicographical_compare(
                maximum_components.begin(),
                maximum_components.end(),
                components.begin(),
                components.end())) {
            maximum = version;
            maximum_components = std::move(components);
        }
    }
    return maximum.empty() ? "<absent>" : maximum;
}

void AddCheck(std::vector<CheckResult>* checks,
              std::string id,
              std::string expected,
              std::string actual,
              bool passed,
              std::string detail = {}) {
    checks->push_back(CheckResult{
        std::move(id),
        passed,
        std::move(expected),
        std::move(actual),
        std::move(detail)});
}

void AddIntegerCheck(std::vector<CheckResult>* checks,
                     std::string id,
                     std::uint64_t expected,
                     std::uint64_t actual) {
    AddCheck(checks,
             std::move(id),
             Number(expected),
             Number(actual),
             expected == actual);
}

bool ChecksPassed(std::span<const CheckResult> checks) noexcept {
    return std::all_of(checks.begin(), checks.end(),
                       [](const CheckResult& check) {
                           return check.passed;
                       });
}

void AddFileHashCheck(const std::filesystem::path& path,
                      std::string_view expected,
                      std::string id,
                      std::vector<CheckResult>* checks,
                      std::optional<std::uint64_t>
                          maximum_size = std::nullopt) {
    common::Sha256Digest digest{};
    std::string error;
    if (!common::ComputeFileSha256(
            path,
            &digest,
            &error,
            maximum_size)) {
        AddCheck(checks,
                 std::move(id),
                 std::string(expected),
                 "<unavailable>",
                 false,
                 std::move(error));
        return;
    }
    const std::string actual = common::Sha256Hex(digest);
    AddCheck(checks,
             std::move(id),
             std::string(expected),
             actual,
             actual == expected);
}

void AddBaselineFileCheck(const std::filesystem::path& path,
                          std::vector<CheckResult>* checks) {
    std::string error;
    const bool passed = VerifyApprovedBaselineFile(path, &error);
    const common::Sha256Digest approved_digest =
        common::ComputeSha256(ApprovedVendorBaselineJson());
    const std::string approved_hex =
        common::Sha256Hex(approved_digest);
    AddCheck(checks,
             "baseline.file",
             approved_hex,
             passed ? approved_hex : "<unavailable>",
             passed,
             std::move(error));
}

void AddElfChecks(const std::filesystem::path& path,
                  std::vector<CheckResult>* checks) {
    ElfMetadata metadata;
    std::string error;
    if (!InspectElfFile(path, &metadata, &error)) {
        AddCheck(checks,
                 "elf.parse",
                 "valid constrained ELF64 shared object",
                 "<invalid>",
                 false,
                 std::move(error));
        return;
    }
    AddCheck(checks, "elf.parse", "valid", "valid", true);
    AddCheck(checks,
             "elf.file_size",
             "<=" + Number(kMaximumSdkSharedLibraryBytes),
             Number(metadata.file_size),
             metadata.file_size <= kMaximumSdkSharedLibraryBytes);
    AddIntegerCheck(checks, "elf.class", ELFCLASS64, metadata.elf_class);
    AddIntegerCheck(checks, "elf.data", ELFDATA2LSB, metadata.data_encoding);
    AddIntegerCheck(checks, "elf.type", ET_DYN, metadata.object_type);
    AddIntegerCheck(checks, "elf.machine", EM_X86_64, metadata.machine);
    AddCheck(checks,
             "elf.build_id",
             "<present>",
             metadata.build_id,
             !metadata.build_id.empty(),
             "observed for diagnostics; not pinned to one library build");
    AddCheck(checks,
             "elf.soname",
             "<absent>",
             metadata.soname.has_value() ? *metadata.soname : "<absent>",
             !metadata.soname.has_value());

    std::sort(metadata.needed.begin(), metadata.needed.end());
    std::vector<std::string_view> expected(
        kApprovedBaseline.elf_needed.begin(),
        kApprovedBaseline.elf_needed.end());
    std::sort(expected.begin(), expected.end());
    const bool needed_equal =
        metadata.needed.size() == expected.size() &&
        std::equal(metadata.needed.begin(),
                   metadata.needed.end(),
                   expected.begin(),
                   [](const std::string& lhs, std::string_view rhs) {
                       return lhs == rhs;
                   });
    AddCheck(checks,
             "elf.dt_needed",
             JoinStringViews(expected),
             JoinStrings(metadata.needed),
             needed_equal);

    const bool versions_equal =
        metadata.required_symbol_versions.size() ==
            kApprovedBaseline.required_symbol_versions.size() &&
        std::equal(
            metadata.required_symbol_versions.begin(),
            metadata.required_symbol_versions.end(),
            kApprovedBaseline.required_symbol_versions.begin(),
            [](const std::string& actual, std::string_view approved) {
                return actual == approved;
            });
    AddCheck(checks,
             "elf.required_symbol_versions",
             JoinStringViews(
                 kApprovedBaseline.required_symbol_versions),
             JoinStrings(metadata.required_symbol_versions),
             versions_equal);

    const auto add_maximum =
        [&metadata, checks](std::string id,
                            std::string_view prefix,
                            std::string_view expected_maximum) {
            const std::string actual = MaximumRequiredVersion(
                metadata.required_symbol_versions, prefix);
            AddCheck(checks,
                     std::move(id),
                     std::string(expected_maximum),
                     actual,
                     actual == expected_maximum);
        };
    add_maximum("elf.max_required.glibc",
                "GLIBC_",
                kApprovedBaseline.glibc_version_max);
    add_maximum("elf.max_required.glibcxx",
                "GLIBCXX_",
                kApprovedBaseline.glibcxx_version_max);
    add_maximum("elf.max_required.cxxabi",
                "CXXABI_",
                kApprovedBaseline.cxxabi_version_max);
    add_maximum("elf.max_required.libgcc",
                "GCC_",
                kApprovedBaseline.libgcc_version_max);
}

template <typename Type>
void AddTypeChecks(const TypeLayout& expected,
                   std::span<const MemberLayout> actual_members,
                   std::vector<CheckResult>* checks) {
    const std::string prefix = "abi." + std::string(expected.name);
    AddIntegerCheck(checks, prefix + ".size", expected.size, sizeof(Type));
    AddIntegerCheck(
        checks, prefix + ".align", expected.alignment, alignof(Type));
    AddCheck(checks,
             prefix + ".standard_layout",
             "true",
             std::is_standard_layout_v<Type> ? "true" : "false",
             std::is_standard_layout_v<Type>);
    if (actual_members.size() != expected.members.size()) {
        AddCheck(checks,
                 prefix + ".member_count",
                 Number(expected.members.size()),
                 Number(actual_members.size()),
                 false);
        return;
    }
    for (std::size_t index = 0; index < expected.members.size(); ++index) {
        const MemberLayout& expected_member = expected.members[index];
        const MemberLayout& actual_member = actual_members[index];
        const bool name_equal =
            expected_member.name == actual_member.name;
        AddCheck(checks,
                 prefix + "." + std::string(expected_member.name) +
                     ".offset",
                 Number(expected_member.offset),
                 name_equal ? Number(actual_member.offset)
                            : "<member-name-mismatch>",
                 name_equal &&
                     expected_member.offset == actual_member.offset);
    }
}

template <typename Message>
void AddMessageCheck(const MessageContract& expected,
                     std::vector<CheckResult>* checks) {
    const MessageKey actual{
        static_cast<std::uint8_t>(Message::ServiceID),
        static_cast<std::uint16_t>(Message::ServiceVer),
        static_cast<std::uint16_t>(Message::MessageID)};
    const auto text = [](const MessageKey& key) {
        return Number(key.service_id) + "." +
               Number(key.service_version) + "." +
               Number(key.message_id);
    };
    AddCheck(checks,
             "message." + std::string(expected.cpp_type) + "." +
                 PolicyName(expected.policy),
             text(expected.key),
             text(actual),
             expected.key == actual);
}

struct RuntimeProbeResult {
    bool attempted = false;
    std::vector<CheckResult> checks;
};

bool DisposeManager(mdl::IOManager* manager, std::string* error) noexcept {
    if (manager == nullptr) {
        return true;
    }
    try {
        manager->Shutdown();
    } catch (...) {
        SetError(error, "IOManager::Shutdown threw across the SDK ABI");
        // A throwing opaque Shutdown cannot prove callback/thread
        // convergence. Do not release or unload code underneath it.
        return false;
    }
    try {
        const int remaining = manager->ReleaseRef();
        if (remaining != 0) {
            SetError(error,
                     "IOManager factory reference did not release to zero");
            return false;
        }
    } catch (...) {
        SetError(error, "IOManager::ReleaseRef threw across the SDK ABI");
        return false;
    }
    return true;
}

RuntimeProbeResult ProbeRuntime(const std::filesystem::path& library) {
    RuntimeProbeResult result;
    result.attempted = true;

    void* const handle =
        ::dlopen(library.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (handle == nullptr) {
        const char* const message = ::dlerror();
        AddCheck(&result.checks,
                 "runtime.dlopen",
                 "success",
                 "failure",
                 false,
                 message == nullptr ? "dlopen failed without an error string"
                                    : message);
        return result;
    }
    AddCheck(&result.checks,
             "runtime.dlopen",
             "success",
             "success",
             true);

    ::dlerror();
    void* const symbol = ::dlsym(handle, "DllCreateIOManager");
    const char* const symbol_error = ::dlerror();
    if (symbol_error != nullptr || symbol == nullptr) {
        AddCheck(&result.checks,
                 "runtime.DllCreateIOManager.symbol",
                 "present",
                 "missing",
                 false,
                 symbol_error == nullptr ? "dlsym returned null"
                                         : symbol_error);
        ::dlclose(handle);
        return result;
    }
    AddCheck(&result.checks,
             "runtime.DllCreateIOManager.symbol",
             "present",
             "present",
             true);

    using CreateFunction =
        mdl::IOManager* (*)(std::uint32_t, int, int);
    static_assert(sizeof(CreateFunction) == sizeof(symbol));
    CreateFunction create = nullptr;
    std::memcpy(&create, &symbol, sizeof(create));

    constexpr std::uint32_t kWrongVersion = 213235U;
    mdl::IOManager* wrong = nullptr;
    std::string wrong_call_error;
    try {
        wrong = create(kWrongVersion, 1, 1);
    } catch (...) {
        wrong_call_error =
            "wrong-version DllCreateIOManager threw across the SDK ABI";
    }
    bool can_close = wrong_call_error.empty();
    if (wrong != nullptr) {
        std::string dispose_error;
        can_close = DisposeManager(wrong, &dispose_error);
        if (!dispose_error.empty()) {
            wrong_call_error.append(
                wrong_call_error.empty() ? dispose_error
                                         : "; " + dispose_error);
        }
    }
    AddCheck(&result.checks,
             "runtime.create_wrong_version",
             "null",
             wrong == nullptr && wrong_call_error.empty() ? "null"
                                                          : "non-null/error",
             wrong == nullptr && wrong_call_error.empty(),
             std::move(wrong_call_error));

    if (can_close) {
        mdl::IOManager* current = nullptr;
        std::string current_call_error;
        try {
            current = create(kApprovedBaseline.sdk_version, 1, 1);
        } catch (...) {
            current_call_error =
                "current-version DllCreateIOManager threw across the SDK ABI";
            // A throwing opaque factory may have created partial global
            // state or threads without returning a releasable manager.
            can_close = false;
        }
        bool disposed = false;
        if (current != nullptr) {
            disposed = DisposeManager(current, &current_call_error);
            can_close = disposed;
        } else {
            // A current-version factory returning null gives us no object
            // through which to prove that partial global state or threads
            // were cleaned up. Keep the loaded code resident.
            can_close = false;
        }
        AddCheck(
            &result.checks,
            "runtime.create_current_version",
            "non-null and safely released",
            current != nullptr && disposed
                ? "non-null and safely released"
                : "null/release failure",
            current != nullptr && disposed &&
                current_call_error.empty(),
            std::move(current_call_error));
    } else {
        AddCheck(
            &result.checks,
            "runtime.create_current_version",
            "non-null and safely released",
            "skipped after unsafe wrong-version call",
            false,
            "the prior factory call did not leave a provably safe "
            "library state");
    }

    if (can_close) {
        ::dlerror();
        const int close_result = ::dlclose(handle);
        const char* const close_error =
            close_result == 0 ? nullptr : ::dlerror();
        AddCheck(&result.checks,
                 "runtime.dlclose",
                 "success",
                 close_result == 0 ? "success" : "failure",
                 close_result == 0,
                 close_error == nullptr
                     ? std::string{}
                     : std::string(close_error));
    } else {
        // Unloading code while a vendor object might still exist is unsafe.
        AddCheck(&result.checks,
                 "runtime.dlclose",
                 "success",
                 "intentionally leaked handle after unsafe release",
                 false);
    }
    return result;
}

void AppendChecks(std::vector<CheckResult>* destination,
                  std::vector<CheckResult> source) {
    destination->insert(destination->end(),
                        std::make_move_iterator(source.begin()),
                        std::make_move_iterator(source.end()));
}

std::string StableFileDescriptorPath(int fd) {
    return "/proc/self/fd/" + std::to_string(fd);
}

}  // namespace

const VendorBaseline& ApprovedVendorBaseline() noexcept {
    return kApprovedBaseline;
}

const std::string& ApprovedVendorBaselineJson() {
    static const std::string json = RenderApprovedBaselineJson();
    return json;
}

bool VerifyApprovedBaselineFile(const std::filesystem::path& path,
                                std::string* error) noexcept {
    try {
        const std::string& approved = ApprovedVendorBaselineJson();
        common::SealedFileSnapshot snapshot;
        if (!common::CreateSealedFileSnapshot(
                path,
                &snapshot,
                error,
                static_cast<std::uint64_t>(
                    approved.size()))) {
            return false;
        }
        RandomAccessFile file;
        if (!file.Open(
                snapshot.proc_fd_path(),
                error)) {
            return false;
        }
        std::string contents(approved.size(), '\0');
        if (!file.Read(
                0U,
                std::as_writable_bytes(
                    std::span<char>(
                        contents.data(),
                        contents.size())),
                error)) {
            return false;
        }
        if (contents != approved) {
            SetError(error,
                     "baseline JSON does not exactly match compiled approved "
                     "constants");
            return false;
        }
        if (error != nullptr) {
            error->clear();
        }
        return true;
    } catch (const std::exception& exception) {
        SetError(error,
                 "exception while verifying baseline JSON: " +
                     std::string(exception.what()));
        return false;
    } catch (...) {
        SetError(error, "unknown exception while verifying baseline JSON");
        return false;
    }
}

bool InspectElfFile(const std::filesystem::path& path,
                    ElfMetadata* metadata,
                    std::string* error) noexcept {
    if (metadata == nullptr) {
        SetError(error, "ELF metadata output pointer is null");
        return false;
    }
    try {
        if constexpr (std::endian::native != std::endian::little) {
            SetError(error,
                     "this constrained ELF parser requires a little-endian "
                     "host");
            return false;
        }

        RandomAccessFile file;
        if (!file.Open(path, error)) {
            return false;
        }
        Elf64_Ehdr header{};
        if (!file.ReadObject(0, &header, error)) {
            return false;
        }
        if (std::memcmp(header.e_ident, ELFMAG, SELFMAG) != 0 ||
            header.e_ident[EI_CLASS] != ELFCLASS64 ||
            header.e_ident[EI_DATA] != ELFDATA2LSB ||
            header.e_ident[EI_VERSION] != EV_CURRENT ||
            header.e_version != EV_CURRENT ||
            header.e_ehsize != sizeof(Elf64_Ehdr) ||
            header.e_phentsize != sizeof(Elf64_Phdr) ||
            header.e_phnum == 0U ||
            header.e_phnum == PN_XNUM) {
            SetError(error, "unsupported or malformed ELF64 header");
            return false;
        }
        if (header.e_type != ET_DYN || header.e_machine != EM_X86_64) {
            SetError(error, "ELF is not an x86-64 ET_DYN object");
            return false;
        }

        std::uint64_t program_header_bytes = 0;
        std::uint64_t program_header_end = 0;
        if (!CheckedMultiply(header.e_phnum,
                             sizeof(Elf64_Phdr),
                             &program_header_bytes) ||
            !CheckedAdd(header.e_phoff,
                        program_header_bytes,
                        &program_header_end) ||
            program_header_end > file.size()) {
            SetError(error, "ELF program header table exceeds file bounds");
            return false;
        }
        std::vector<Elf64_Phdr> program_headers(header.e_phnum);
        if (!file.Read(
                header.e_phoff,
                std::as_writable_bytes(std::span{program_headers}),
                error)) {
            return false;
        }
        for (const Elf64_Phdr& program_header : program_headers) {
            std::uint64_t end = 0;
            if (!CheckedAdd(program_header.p_offset,
                            program_header.p_filesz,
                            &end) ||
                end > file.size()) {
                SetError(error,
                         "ELF program segment exceeds file bounds");
                return false;
            }
        }

        ElfMetadata parsed;
        parsed.file_size = file.size();
        parsed.elf_class = header.e_ident[EI_CLASS];
        parsed.data_encoding = header.e_ident[EI_DATA];
        parsed.object_type = header.e_type;
        parsed.machine = header.e_machine;
        std::string compiler_comment_sha256;
        std::vector<std::string> compiler_producers;
        if (!ParseBuildId(
                &file, program_headers, &parsed.build_id, error) ||
            !ParseDynamic(&file,
                          program_headers,
                          &parsed.soname,
                          &parsed.needed,
                          &parsed.required_symbol_versions,
                          error) ||
            !ParseCompilerComment(&file,
                                  header,
                                  &compiler_comment_sha256,
                                  &compiler_producers,
                          error)) {
            return false;
        }
        *metadata = std::move(parsed);
        if (error != nullptr) {
            error->clear();
        }
        return true;
    } catch (const std::exception& exception) {
        SetError(error,
                 "exception while parsing ELF: " +
                     std::string(exception.what()));
        return false;
    } catch (...) {
        SetError(error, "unknown exception while parsing ELF");
        return false;
    }
}

bool PreflightReport::passed() const noexcept {
    return !checks.empty() && ChecksPassed(checks);
}

std::vector<CheckResult> CheckCompiledVendorAbi() {
    std::vector<CheckResult> checks;
    AddCheck(&checks,
             "build.cxx_standard",
             ">=202002",
             Number(__cplusplus),
             __cplusplus >= 202002L);
    AddIntegerCheck(
        &checks, "build.pointer_size", 8U, sizeof(void*));
    AddCheck(&checks,
             "build.native_endian",
             "little",
             std::endian::native == std::endian::little ? "little" : "other",
             std::endian::native == std::endian::little);
    AddIntegerCheck(&checks,
                    "abi.MDL_VERSION",
                    kApprovedBaseline.sdk_version,
                    mdl::MDL_VERSION);

    const std::array<NumericConstant, 20> actual_constants = {{
        {"MDLSID_MDL_API",
         static_cast<std::uint64_t>(mdl::MDLSID_MDL_API)},
        {"MDLSID_MDL_SYS",
         static_cast<std::uint64_t>(mdl::MDLSID_MDL_SYS)},
        {"MDLSID_MDL_SHL2",
         static_cast<std::uint64_t>(mdl::MDLSID_MDL_SHL2)},
        {"MDLSID_MDL_SZL2",
         static_cast<std::uint64_t>(mdl::MDLSID_MDL_SZL2)},
        {"MDLVID_MDL_SYS",
         static_cast<std::uint64_t>(sys::MDLVID_MDL_SYS)},
        {"MDLMID_MDL_SYS_Logon",
         static_cast<std::uint64_t>(sys::MDLMID_MDL_SYS_Logon)},
        {"MDLMID_MDL_SYS_LogonResponse",
         static_cast<std::uint64_t>(
             sys::MDLMID_MDL_SYS_LogonResponse)},
        {"MDLMID_MDL_SYS_SubscribeRequest",
         static_cast<std::uint64_t>(
             sys::MDLMID_MDL_SYS_SubscribeRequest)},
        {"MDLMID_MDL_SYS_SubscribeResponse",
         static_cast<std::uint64_t>(
             sys::MDLMID_MDL_SYS_SubscribeResponse)},
        {"MDLEC_OK", static_cast<std::uint64_t>(mdl::MDLEC_OK)},
        {"MDLEID_BINARY",
         static_cast<std::uint64_t>(mdl::MDLEID_BINARY)},
        {"MDLEID_FAST",
         static_cast<std::uint64_t>(mdl::MDLEID_FAST)},
        {"MDLEID_JSON",
         static_cast<std::uint64_t>(mdl::MDLEID_JSON)},
        {"MDLEID_PROTOBUF",
         static_cast<std::uint64_t>(mdl::MDLEID_PROTOBUF)},
        {"MDLEID_CSV",
         static_cast<std::uint64_t>(mdl::MDLEID_CSV)},
        {"MDLEID_MKTDATA",
         static_cast<std::uint64_t>(mdl::MDLEID_MKTDATA)},
        {"MDLEID_MKTPRO",
         static_cast<std::uint64_t>(mdl::MDLEID_MKTPRO)},
        {"MDLEID_PACKAGE",
         static_cast<std::uint64_t>(mdl::MDLEID_PACKAGE)},
        {"MDLEID_DEFLATE",
         static_cast<std::uint64_t>(mdl::MDLEID_DEFLATE)},
        {"MDLEID_DEFLATE_PROTOBUF",
         static_cast<std::uint64_t>(
             mdl::MDLEID_DEFLATE_PROTOBUF)},
    }};
    for (std::size_t index = 0; index < kProtocolConstants.size(); ++index) {
        const NumericConstant& expected = kProtocolConstants[index];
        const NumericConstant& actual = actual_constants[index];
        AddCheck(&checks,
                 "constant." + std::string(expected.name),
                 Number(expected.value),
                 expected.name == actual.name
                     ? Number(actual.value)
                     : "<constant-name-mismatch>",
                 expected.name == actual.name &&
                     expected.value == actual.value);
    }

    const std::array<MemberLayout, 8> head_members = {{
        {"HeadSize", offsetof(mdl::MDLMessageHead, HeadSize)},
        {"MessageSize", offsetof(mdl::MDLMessageHead, MessageSize)},
        {"MessageEncoding",
         offsetof(mdl::MDLMessageHead, MessageEncoding)},
        {"ServiceID", offsetof(mdl::MDLMessageHead, ServiceID)},
        {"ServiceVersion",
         offsetof(mdl::MDLMessageHead, ServiceVersion)},
        {"MessageID", offsetof(mdl::MDLMessageHead, MessageID)},
        {"LocalTime", offsetof(mdl::MDLMessageHead, LocalTime)},
        {"SequenceID", offsetof(mdl::MDLMessageHead, SequenceID)},
    }};
    AddTypeChecks<mdl::MDLMessageHead>(
        kAbiTypes[0], head_members, &checks);
    const std::array<MemberLayout, 2> string_members = {{
        {"Length", offsetof(mdl::MDLString, Length)},
        {"Offset", offsetof(mdl::MDLString, Offset)},
    }};
    AddTypeChecks<mdl::MDLAnsiString>(
        kAbiTypes[1], string_members, &checks);
    AddTypeChecks<mdl::MDLUTF8String>(
        kAbiTypes[2], string_members, &checks);

    const std::array<MemberLayout, 2> list_members = {{
        {"Length", offsetof(mdl::MDLList, Length)},
        {"Offset", offsetof(mdl::MDLList, Offset)},
    }};
    AddTypeChecks<mdl::MDLList>(
        kAbiTypes[3], list_members, &checks);

    const std::array<MemberLayout, 4> logon_response_members = {{
        {"UserName", offsetof(sys::LogonResponse, UserName)},
        {"Password", offsetof(sys::LogonResponse, Password)},
        {"Services", offsetof(sys::LogonResponse, Services)},
        {"ReturnCode", offsetof(sys::LogonResponse, ReturnCode)},
    }};
    AddTypeChecks<sys::LogonResponse>(
        kAbiTypes[4], logon_response_members, &checks);
    const std::array<MemberLayout, 3> logon_service_members = {{
        {"ServiceID",
         offsetof(sys::LogonResponse::ServicesItem, ServiceID)},
        {"ServiceVersion",
         offsetof(sys::LogonResponse::ServicesItem, ServiceVersion)},
        {"Messages",
         offsetof(sys::LogonResponse::ServicesItem, Messages)},
    }};
    AddTypeChecks<sys::LogonResponse::ServicesItem>(
        kAbiTypes[5], logon_service_members, &checks);
    const std::array<MemberLayout, 2> logon_message_members = {{
        {"MessageID",
         offsetof(
             sys::LogonResponse::ServicesItem::MessagesItem,
             MessageID)},
        {"MessageStatus",
         offsetof(
             sys::LogonResponse::ServicesItem::MessagesItem,
             MessageStatus)},
    }};
    AddTypeChecks<
        sys::LogonResponse::ServicesItem::MessagesItem>(
        kAbiTypes[6], logon_message_members, &checks);

    const std::array<MemberLayout, 1> subscribe_response_members = {{
        {"Services", offsetof(sys::SubscribeResponse, Services)},
    }};
    AddTypeChecks<sys::SubscribeResponse>(
        kAbiTypes[7], subscribe_response_members, &checks);
    const std::array<MemberLayout, 3> subscribe_service_members = {{
        {"ServiceID",
         offsetof(sys::SubscribeResponse::ServicesItem, ServiceID)},
        {"ServiceVersion",
         offsetof(
             sys::SubscribeResponse::ServicesItem, ServiceVersion)},
        {"Messages",
         offsetof(sys::SubscribeResponse::ServicesItem, Messages)},
    }};
    AddTypeChecks<sys::SubscribeResponse::ServicesItem>(
        kAbiTypes[8], subscribe_service_members, &checks);
    const std::array<MemberLayout, 2> subscribe_message_members = {{
        {"MessageID",
         offsetof(
             sys::SubscribeResponse::ServicesItem::MessagesItem,
             MessageID)},
        {"MessageStatus",
         offsetof(
             sys::SubscribeResponse::ServicesItem::MessagesItem,
             MessageStatus)},
    }};
    AddTypeChecks<
        sys::SubscribeResponse::ServicesItem::MessagesItem>(
        kAbiTypes[9], subscribe_message_members, &checks);

    const std::array<MemberLayout, 44> sh44_members = {{
        {"UpdateTime", offsetof(sh::SHL2MarketData, UpdateTime)},
        {"SecurityID", offsetof(sh::SHL2MarketData, SecurityID)},
        {"ImageStatus", offsetof(sh::SHL2MarketData, ImageStatus)},
        {"PreCloPrice", offsetof(sh::SHL2MarketData, PreCloPrice)},
        {"OpenPrice", offsetof(sh::SHL2MarketData, OpenPrice)},
        {"HighPrice", offsetof(sh::SHL2MarketData, HighPrice)},
        {"LowPrice", offsetof(sh::SHL2MarketData, LowPrice)},
        {"LastPrice", offsetof(sh::SHL2MarketData, LastPrice)},
        {"ClosePrice", offsetof(sh::SHL2MarketData, ClosePrice)},
        {"InstruStatus", offsetof(sh::SHL2MarketData, InstruStatus)},
        {"TradNumber", offsetof(sh::SHL2MarketData, TradNumber)},
        {"TradVolume", offsetof(sh::SHL2MarketData, TradVolume)},
        {"Turnover", offsetof(sh::SHL2MarketData, Turnover)},
        {"TotalBidVol", offsetof(sh::SHL2MarketData, TotalBidVol)},
        {"WAvgBidPri", offsetof(sh::SHL2MarketData, WAvgBidPri)},
        {"AltWAvgBidPri",
         offsetof(sh::SHL2MarketData, AltWAvgBidPri)},
        {"TotalAskVol", offsetof(sh::SHL2MarketData, TotalAskVol)},
        {"WAvgAskPri", offsetof(sh::SHL2MarketData, WAvgAskPri)},
        {"AltWAvgAskPri",
         offsetof(sh::SHL2MarketData, AltWAvgAskPri)},
        {"EtfBuyNumber", offsetof(sh::SHL2MarketData, EtfBuyNumber)},
        {"EtfBuyVolume", offsetof(sh::SHL2MarketData, EtfBuyVolume)},
        {"EtfBuyMoney", offsetof(sh::SHL2MarketData, EtfBuyMoney)},
        {"EtfSellNumber", offsetof(sh::SHL2MarketData, EtfSellNumber)},
        {"EtfSellVolume", offsetof(sh::SHL2MarketData, EtfSellVolume)},
        {"ETFSellMoney", offsetof(sh::SHL2MarketData, ETFSellMoney)},
        {"YieldToMatu", offsetof(sh::SHL2MarketData, YieldToMatu)},
        {"TotWarExNum", offsetof(sh::SHL2MarketData, TotWarExNum)},
        {"WarLowerPri", offsetof(sh::SHL2MarketData, WarLowerPri)},
        {"WarUpperPri", offsetof(sh::SHL2MarketData, WarUpperPri)},
        {"WiDBuyNum", offsetof(sh::SHL2MarketData, WiDBuyNum)},
        {"WiDBuyVol", offsetof(sh::SHL2MarketData, WiDBuyVol)},
        {"WiDBuyMon", offsetof(sh::SHL2MarketData, WiDBuyMon)},
        {"WiDSellNum", offsetof(sh::SHL2MarketData, WiDSellNum)},
        {"WiDSellVol", offsetof(sh::SHL2MarketData, WiDSellVol)},
        {"WiDSellMon", offsetof(sh::SHL2MarketData, WiDSellMon)},
        {"TotBidNum", offsetof(sh::SHL2MarketData, TotBidNum)},
        {"TotSellNum", offsetof(sh::SHL2MarketData, TotSellNum)},
        {"MaxBidDur", offsetof(sh::SHL2MarketData, MaxBidDur)},
        {"MaxSellDur", offsetof(sh::SHL2MarketData, MaxSellDur)},
        {"BidNum", offsetof(sh::SHL2MarketData, BidNum)},
        {"SellNum", offsetof(sh::SHL2MarketData, SellNum)},
        {"BidLevels", offsetof(sh::SHL2MarketData, BidLevels)},
        {"SellLevels", offsetof(sh::SHL2MarketData, SellLevels)},
        {"IOPV", offsetof(sh::SHL2MarketData, IOPV)},
    }};
    AddTypeChecks<sh::SHL2MarketData>(
        kAbiTypes[10], sh44_members, &checks);
    const std::array<MemberLayout, 5> sh44_bid_level_members = {{
        {"PriLevOpera",
         offsetof(sh::SHL2MarketData::BidLevelsItem, PriLevOpera)},
        {"OrderPrice",
         offsetof(sh::SHL2MarketData::BidLevelsItem, OrderPrice)},
        {"OrderVol",
         offsetof(sh::SHL2MarketData::BidLevelsItem, OrderVol)},
        {"OrderNum",
         offsetof(sh::SHL2MarketData::BidLevelsItem, OrderNum)},
        {"NOrders",
         offsetof(sh::SHL2MarketData::BidLevelsItem, NOrders)},
    }};
    AddTypeChecks<sh::SHL2MarketData::BidLevelsItem>(
        kAbiTypes[11], sh44_bid_level_members, &checks);
    const std::array<MemberLayout, 3> sh44_order_members = {{
        {"OrderQueOper",
         offsetof(
             sh::SHL2MarketData::BidLevelsItem::NOrdersItem,
             OrderQueOper)},
        {"OrderQueID",
         offsetof(
             sh::SHL2MarketData::BidLevelsItem::NOrdersItem,
             OrderQueID)},
        {"OrderQty",
         offsetof(
             sh::SHL2MarketData::BidLevelsItem::NOrdersItem,
             OrderQty)},
    }};
    AddTypeChecks<sh::SHL2MarketData::BidLevelsItem::NOrdersItem>(
        kAbiTypes[12], sh44_order_members, &checks);

    const std::array<MemberLayout, 5> sh44_sell_level_members = {{
        {"PriLevOpera",
         offsetof(sh::SHL2MarketData::SellLevelsItem, PriLevOpera)},
        {"OrderPrice",
         offsetof(sh::SHL2MarketData::SellLevelsItem, OrderPrice)},
        {"OrderVol",
         offsetof(sh::SHL2MarketData::SellLevelsItem, OrderVol)},
        {"OrderNum",
         offsetof(sh::SHL2MarketData::SellLevelsItem, OrderNum)},
        {"NoOrders",
         offsetof(sh::SHL2MarketData::SellLevelsItem, NoOrders)},
    }};
    AddTypeChecks<sh::SHL2MarketData::SellLevelsItem>(
        kAbiTypes[13], sh44_sell_level_members, &checks);
    const std::array<MemberLayout, 3> sh44_no_order_members = {{
        {"OrderQueOper",
         offsetof(
             sh::SHL2MarketData::SellLevelsItem::NoOrdersItem,
             OrderQueOper)},
        {"OrderQueID",
         offsetof(
             sh::SHL2MarketData::SellLevelsItem::NoOrdersItem,
             OrderQueID)},
        {"OrderQty",
         offsetof(
             sh::SHL2MarketData::SellLevelsItem::NoOrdersItem,
             OrderQty)},
    }};
    AddTypeChecks<
        sh::SHL2MarketData::SellLevelsItem::NoOrdersItem>(
        kAbiTypes[14], sh44_no_order_members, &checks);

    const std::array<MemberLayout, 11> sh24_members = {{
        {"BizIndex", offsetof(sh::NGTSTick, BizIndex)},
        {"Channel", offsetof(sh::NGTSTick, Channel)},
        {"SecurityID", offsetof(sh::NGTSTick, SecurityID)},
        {"TickTime", offsetof(sh::NGTSTick, TickTime)},
        {"Type", offsetof(sh::NGTSTick, Type)},
        {"BuyOrderNO", offsetof(sh::NGTSTick, BuyOrderNO)},
        {"SellOrderNO", offsetof(sh::NGTSTick, SellOrderNO)},
        {"Price", offsetof(sh::NGTSTick, Price)},
        {"Qty", offsetof(sh::NGTSTick, Qty)},
        {"TradeMoney", offsetof(sh::NGTSTick, TradeMoney)},
        {"TickBSFlag", offsetof(sh::NGTSTick, TickBSFlag)},
    }};
    AddTypeChecks<sh::NGTSTick>(
        kAbiTypes[15], sh24_members, &checks);

    const std::array<MemberLayout, 30> sz28_members = {{
        {"UpdateTime", offsetof(sz::Snapshot300111_v2, UpdateTime)},
        {"ChannelNo", offsetof(sz::Snapshot300111_v2, ChannelNo)},
        {"MDStreamID",
         offsetof(sz::Snapshot300111_v2, MDStreamID)},
        {"SecurityID", offsetof(sz::Snapshot300111_v2, SecurityID)},
        {"SecurityIDSource",
         offsetof(sz::Snapshot300111_v2, SecurityIDSource)},
        {"TradingPhaseCode",
         offsetof(sz::Snapshot300111_v2, TradingPhaseCode)},
        {"PreCloPrice",
         offsetof(sz::Snapshot300111_v2, PreCloPrice)},
        {"TurnNum", offsetof(sz::Snapshot300111_v2, TurnNum)},
        {"Volume", offsetof(sz::Snapshot300111_v2, Volume)},
        {"Turnover", offsetof(sz::Snapshot300111_v2, Turnover)},
        {"LastPrice", offsetof(sz::Snapshot300111_v2, LastPrice)},
        {"OpenPrice", offsetof(sz::Snapshot300111_v2, OpenPrice)},
        {"HighPrice", offsetof(sz::Snapshot300111_v2, HighPrice)},
        {"LowPrice", offsetof(sz::Snapshot300111_v2, LowPrice)},
        {"DifPrice1", offsetof(sz::Snapshot300111_v2, DifPrice1)},
        {"DifPrice2", offsetof(sz::Snapshot300111_v2, DifPrice2)},
        {"PE1", offsetof(sz::Snapshot300111_v2, PE1)},
        {"PE2", offsetof(sz::Snapshot300111_v2, PE2)},
        {"PreCloseIOPV",
         offsetof(sz::Snapshot300111_v2, PreCloseIOPV)},
        {"IOPV", offsetof(sz::Snapshot300111_v2, IOPV)},
        {"TotalOfferQty",
         offsetof(sz::Snapshot300111_v2, TotalOfferQty)},
        {"WeightedAvgOfferPx",
         offsetof(sz::Snapshot300111_v2, WeightedAvgOfferPx)},
        {"TotalBidQty",
         offsetof(sz::Snapshot300111_v2, TotalBidQty)},
        {"WeightedAvgBidPx",
         offsetof(sz::Snapshot300111_v2, WeightedAvgBidPx)},
        {"HighLimitPrice",
         offsetof(sz::Snapshot300111_v2, HighLimitPrice)},
        {"LowLimitPrice",
         offsetof(sz::Snapshot300111_v2, LowLimitPrice)},
        {"OpenInt", offsetof(sz::Snapshot300111_v2, OpenInt)},
        {"OptPremiumRatio",
         offsetof(sz::Snapshot300111_v2, OptPremiumRatio)},
        {"BidPriceLevel",
         offsetof(sz::Snapshot300111_v2, BidPriceLevel)},
        {"AskPriceLevel",
         offsetof(sz::Snapshot300111_v2, AskPriceLevel)},
    }};
    AddTypeChecks<sz::Snapshot300111_v2>(
        kAbiTypes[16], sz28_members, &checks);
    const std::array<MemberLayout, 4> sz28_bid_level_members = {{
        {"Volume",
         offsetof(
             sz::Snapshot300111_v2::BidPriceLevelItem, Volume)},
        {"Price",
         offsetof(
             sz::Snapshot300111_v2::BidPriceLevelItem, Price)},
        {"NumOrders",
         offsetof(
             sz::Snapshot300111_v2::BidPriceLevelItem, NumOrders)},
        {"Orders",
         offsetof(
             sz::Snapshot300111_v2::BidPriceLevelItem, Orders)},
    }};
    AddTypeChecks<sz::Snapshot300111_v2::BidPriceLevelItem>(
        kAbiTypes[17], sz28_bid_level_members, &checks);
    const std::array<MemberLayout, 1> sz28_order_members = {{
        {"OrderQty",
         offsetof(
             sz::Snapshot300111_v2::BidPriceLevelItem::OrdersItem,
             OrderQty)},
    }};
    AddTypeChecks<
        sz::Snapshot300111_v2::BidPriceLevelItem::OrdersItem>(
        kAbiTypes[18], sz28_order_members, &checks);

    const std::array<MemberLayout, 4> sz28_ask_level_members = {{
        {"Volume",
         offsetof(
             sz::Snapshot300111_v2::AskPriceLevelItem, Volume)},
        {"Price",
         offsetof(
             sz::Snapshot300111_v2::AskPriceLevelItem, Price)},
        {"NumOrders",
         offsetof(
             sz::Snapshot300111_v2::AskPriceLevelItem, NumOrders)},
        {"Orders",
         offsetof(
             sz::Snapshot300111_v2::AskPriceLevelItem, Orders)},
    }};
    AddTypeChecks<sz::Snapshot300111_v2::AskPriceLevelItem>(
        kAbiTypes[19], sz28_ask_level_members, &checks);
    const std::array<MemberLayout, 1> sz28_ask_order_members = {{
        {"OrderQty",
         offsetof(
             sz::Snapshot300111_v2::AskPriceLevelItem::OrdersItem,
             OrderQty)},
    }};
    AddTypeChecks<
        sz::Snapshot300111_v2::AskPriceLevelItem::OrdersItem>(
        kAbiTypes[20], sz28_ask_order_members, &checks);

    const std::array<MemberLayout, 10> sz33_members = {{
        {"ChannelNo", offsetof(sz::Order300192_v2, ChannelNo)},
        {"ApplSeqNum", offsetof(sz::Order300192_v2, ApplSeqNum)},
        {"MDStreamID", offsetof(sz::Order300192_v2, MDStreamID)},
        {"SecurityID", offsetof(sz::Order300192_v2, SecurityID)},
        {"SecurityIDSource",
         offsetof(sz::Order300192_v2, SecurityIDSource)},
        {"Price", offsetof(sz::Order300192_v2, Price)},
        {"OrderQty", offsetof(sz::Order300192_v2, OrderQty)},
        {"Side", offsetof(sz::Order300192_v2, Side)},
        {"TransactTime", offsetof(sz::Order300192_v2, TransactTime)},
        {"OrdType", offsetof(sz::Order300192_v2, OrdType)},
    }};
    AddTypeChecks<sz::Order300192_v2>(
        kAbiTypes[21], sz33_members, &checks);

    const std::array<MemberLayout, 11> sz36_members = {{
        {"ChannelNo", offsetof(sz::Transaction300191_v2, ChannelNo)},
        {"ApplSeqNum",
         offsetof(sz::Transaction300191_v2, ApplSeqNum)},
        {"MDStreamID",
         offsetof(sz::Transaction300191_v2, MDStreamID)},
        {"BidApplSeqNum",
         offsetof(sz::Transaction300191_v2, BidApplSeqNum)},
        {"OfferApplSeqNum",
         offsetof(sz::Transaction300191_v2, OfferApplSeqNum)},
        {"SecurityID",
         offsetof(sz::Transaction300191_v2, SecurityID)},
        {"SecurityIDSource",
         offsetof(sz::Transaction300191_v2, SecurityIDSource)},
        {"LastPx", offsetof(sz::Transaction300191_v2, LastPx)},
        {"LastQty", offsetof(sz::Transaction300191_v2, LastQty)},
        {"ExecType", offsetof(sz::Transaction300191_v2, ExecType)},
        {"TransactTime",
         offsetof(sz::Transaction300191_v2, TransactTime)},
    }};
    AddTypeChecks<sz::Transaction300191_v2>(
        kAbiTypes[22], sz36_members, &checks);
    const std::array<MemberLayout, 11> combined_tick_members = {{
        {"ChannelNo", offsetof(sz::CombinedTick, ChannelNo)},
        {"ApplSeqNum", offsetof(sz::CombinedTick, ApplSeqNum)},
        {"MDStreamID", offsetof(sz::CombinedTick, MDStreamID)},
        {"SecurityID", offsetof(sz::CombinedTick, SecurityID)},
        {"SecurityIDSource",
         offsetof(sz::CombinedTick, SecurityIDSource)},
        {"TransactTime", offsetof(sz::CombinedTick, TransactTime)},
        {"Type", offsetof(sz::CombinedTick, Type)},
        {"BidApplSeqNum",
         offsetof(sz::CombinedTick, BidApplSeqNum)},
        {"OfferApplSeqNum",
         offsetof(sz::CombinedTick, OfferApplSeqNum)},
        {"Price", offsetof(sz::CombinedTick, Price)},
        {"Qty", offsetof(sz::CombinedTick, Qty)},
    }};
    AddTypeChecks<sz::CombinedTick>(
        kAbiTypes[23], combined_tick_members, &checks);

    AddMessageCheck<sh::SHL2MarketData>(kMessages[0], &checks);
    AddMessageCheck<sh::NGTSTick>(kMessages[1], &checks);
    AddMessageCheck<sz::Snapshot300111_v2>(kMessages[2], &checks);
    AddMessageCheck<sz::Order300192_v2>(kMessages[3], &checks);
    AddMessageCheck<sz::Transaction300191_v2>(kMessages[4], &checks);
    AddMessageCheck<sh::SHL2Index>(kMessages[5], &checks);
    AddMessageCheck<sz::Snapshot309011_v2>(kMessages[6], &checks);
    AddMessageCheck<sz::CombinedTick>(kMessages[7], &checks);
    return checks;
}

namespace {

PreflightReport RunApprovedLibraryRuntimePreflightAtStablePath(
    const std::filesystem::path& stable_library,
    std::uint64_t stable_library_size) {
    PreflightReport report;
    report.mode = "approved-library-runtime";
    AddCheck(&report.checks,
             "artifact.shared_library_snapshot",
             "sealed immutable regular file <= " +
                 Number(kMaximumSdkSharedLibraryBytes) + " bytes",
             Number(stable_library_size) + " bytes",
             stable_library_size <= kMaximumSdkSharedLibraryBytes);
    AddElfChecks(stable_library, &report.checks);
    AppendChecks(&report.checks, CheckCompiledVendorAbi());
    if (!ChecksPassed(report.checks)) {
        AddCheck(&report.checks,
                 "runtime.probe",
                 "attempted after validation",
                 "skipped",
                 false,
                 "snapshot, ELF, or compiled ABI validation failed");
        return report;
    }
    RuntimeProbeResult runtime = ProbeRuntime(stable_library);
    report.runtime_probe_attempted = runtime.attempted;
    AppendChecks(&report.checks, std::move(runtime.checks));
    return report;
}

PreflightReport StableLibraryOpenFailure(
    std::string mode,
    std::string detail,
    std::string_view runtime_expectation) {
    PreflightReport report;
    report.mode = std::move(mode);
    AddCheck(&report.checks,
             "artifact.shared_library_snapshot",
             "sealed immutable regular file <= " +
                 Number(kMaximumSdkSharedLibraryBytes) + " bytes",
             "<unavailable>",
             false,
             detail);
    AddCheck(&report.checks,
             "elf.parse",
             "valid constrained ELF64 shared object",
             "<unavailable>",
             false,
             std::move(detail));
    AppendChecks(&report.checks, CheckCompiledVendorAbi());
    AddCheck(&report.checks,
             "runtime.probe",
             std::string(runtime_expectation),
             "skipped",
             false,
             "the shared library could not be captured as one sealed "
             "immutable snapshot");
    return report;
}

}  // namespace

PreflightReport RunApprovedLibraryRuntimePreflight(
    const std::filesystem::path& shared_library) {
    common::SealedFileSnapshot snapshot;
    std::string error;
    if (!common::CreateSealedFileSnapshot(
            shared_library,
            &snapshot,
            &error,
            std::nullopt,
            kMaximumSdkSharedLibraryBytes)) {
        return StableLibraryOpenFailure(
            "approved-library-runtime",
            std::move(error),
            "attempted after validation");
    }
    return RunApprovedLibraryRuntimePreflightAtStablePath(
        snapshot.proc_fd_path(),
        snapshot.size());
}

PreflightReport RunApprovedLibraryRuntimePreflightForOpenFd(
    int fd) {
    common::SealedFileSnapshot snapshot;
    std::string error;
    if (!common::CreateSealedFileSnapshotFromOpenFd(
            fd,
            &snapshot,
            &error,
            std::nullopt,
            kMaximumSdkSharedLibraryBytes)) {
        return StableLibraryOpenFailure(
            "approved-library-runtime",
            std::move(error),
            "attempted after validation");
    }
    return RunApprovedLibraryRuntimePreflightAtStablePath(
        snapshot.proc_fd_path(),
        snapshot.size());
}

PreflightReport RunApprovedLibraryRuntimePreflightForSealedSnapshotFd(
    int fd) {
    std::uint64_t size = 0;
    std::string error;
    if (!common::ValidateSealedFileSnapshotFd(
            fd, &size, &error)) {
        return StableLibraryOpenFailure(
            "approved-library-runtime",
            std::move(error),
            "attempted after validation");
    }
    if (size > kMaximumSdkSharedLibraryBytes) {
        return StableLibraryOpenFailure(
            "approved-library-runtime",
            "sealed snapshot exceeds the SDK shared-library size bound",
            "attempted after validation");
    }
    return RunApprovedLibraryRuntimePreflightAtStablePath(
        StableFileDescriptorPath(fd),
        size);
}

PreflightReport RunVendorPreflight(const PreflightPaths& paths) {
    PreflightReport report;
    report.mode = "full-vendor-preflight";

    // Capture executable bytes first. Slow archive/baseline checks can no
    // longer race an in-place mutation of the source shared object.
    common::SealedFileSnapshot library_snapshot;
    std::string library_open_error;
    const bool library_snapshotted =
        common::CreateSealedFileSnapshot(
            paths.shared_library,
            &library_snapshot,
            &library_open_error,
            std::nullopt,
            kMaximumSdkSharedLibraryBytes);

    AddBaselineFileCheck(paths.baseline_json, &report.checks);
    AddFileHashCheck(paths.sdk_archive,
                     kApprovedBaseline.sdk_archive_sha256,
                     "artifact.sdk_archive_sha256",
                     &report.checks,
                     1U * 1024U * 1024U * 1024U);

    std::filesystem::path stable_library;
    if (!library_snapshotted) {
        AddCheck(&report.checks,
                 "artifact.shared_library_snapshot",
                 "sealed immutable regular file <= " +
                     Number(kMaximumSdkSharedLibraryBytes) + " bytes",
                 "<unavailable>",
                 false,
                 library_open_error);
        AddCheck(&report.checks,
                 "elf.parse",
                 "valid constrained ELF64 shared object",
                 "<unavailable>",
                 false,
                 std::move(library_open_error));
    } else {
        stable_library =
            library_snapshot.proc_fd_path();
        AddCheck(&report.checks,
                 "artifact.shared_library_snapshot",
                 "sealed immutable regular file <= " +
                     Number(kMaximumSdkSharedLibraryBytes) + " bytes",
                 Number(library_snapshot.size()) + " bytes",
                 library_snapshot.size() <=
                     kMaximumSdkSharedLibraryBytes);
        AddElfChecks(stable_library, &report.checks);
    }
    AppendChecks(&report.checks, CheckCompiledVendorAbi());
    if (!ChecksPassed(report.checks)) {
        AddCheck(&report.checks,
                 "runtime.probe",
                 "attempted after all validation",
                 "skipped",
                 false,
                 "baseline, archive, library snapshot, ELF, or ABI "
                 "validation failed");
        return report;
    }
    RuntimeProbeResult runtime = ProbeRuntime(stable_library);
    report.runtime_probe_attempted = runtime.attempted;
    AppendChecks(&report.checks, std::move(runtime.checks));
    return report;
}

std::string PreflightReportJson(const PreflightReport& report,
                                bool differences_only) {
    std::string output;
    output.reserve(report.checks.size() * 160U + 256U);
    output.append("{\n  \"schema_version\": 1,\n  \"mode\": ");
    AppendJsonString(&output, report.mode);
    output.append(",\n  \"passed\": ");
    output.append(report.passed() ? "true" : "false");
    output.append(",\n  \"runtime_probe_attempted\": ");
    output.append(report.runtime_probe_attempted ? "true" : "false");
    output.append(differences_only ? ",\n  \"differences\": [\n"
                                   : ",\n  \"checks\": [\n");

    bool first = true;
    for (const CheckResult& check : report.checks) {
        if (differences_only && check.passed) {
            continue;
        }
        if (!first) {
            output.append(",\n");
        }
        first = false;
        output.append("    {\"id\": ");
        AppendJsonString(&output, check.id);
        output.append(", \"passed\": ");
        output.append(check.passed ? "true" : "false");
        output.append(", \"expected\": ");
        AppendJsonString(&output, check.expected);
        output.append(", \"actual\": ");
        AppendJsonString(&output, check.actual);
        output.append(", \"detail\": ");
        AppendJsonString(&output, check.detail);
        output.push_back('}');
    }
    output.append("\n  ]\n}\n");
    return output;
}

}  // namespace l2flow::baseline
