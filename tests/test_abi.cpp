#include "l2mock/l2_mock.h"

#include "mdl_cffexl2_msg.h"
#include "mdl_czcel2_msg.h"
#include "mdl_dcel2_msg.h"
#include "mdl_gfexl2_msg.h"
#include "mdl_shfel2_msg.h"
#include "mdl_shl2_msg.h"
#include "mdl_szl2_msg.h"

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <set>
#include <tuple>
#include <type_traits>

namespace mdl = datayes::mdl;
namespace mock = datayes::mdl::mock;
namespace sh = datayes::mdl::mdl_shl2_msg;
namespace sz = datayes::mdl::mdl_szl2_msg;
namespace cffex = datayes::mdl::mdl_cffexl2_msg;
namespace shfe = datayes::mdl::mdl_shfel2_msg;
namespace czce = datayes::mdl::mdl_czcel2_msg;
namespace dce = datayes::mdl::mdl_dcel2_msg;
namespace gfex = datayes::mdl::mdl_gfexl2_msg;

#define ASSERT_PACKED_SIZE(Type, ExpectedSize)                              \
    static_assert(sizeof(Type) == (ExpectedSize),                          \
                  #Type " has an unexpected ABI size");                   \
    static_assert(alignof(Type) == 1, #Type " is no longer byte-packed")

#define ASSERT_MESSAGE_KEY(Type, ExpectedSid, ExpectedMid)                 \
    static_assert(static_cast<int>(Type::ServiceID) ==                     \
                      static_cast<int>(ExpectedSid),                        \
                  #Type " has an unexpected service ID");                 \
    static_assert(Type::ServiceVer == 101,                                 \
                  #Type " has an unexpected service version");            \
    static_assert(Type::MessageID == (ExpectedMid),                        \
                  #Type " has an unexpected message ID")

static_assert(mdl::MDL_VERSION == 213234, "tests target DataYes SDK 2.13.234");

static_assert(mdl::MDLSID_MDL_SHL2 == 4, "SHL2 SID changed");
static_assert(mdl::MDLSID_MDL_SZL2 == 6, "SZL2 SID changed");
static_assert(mdl::MDLSID_MDL_CFFEXL2 == 21, "CFFEXL2 SID changed");
static_assert(mdl::MDLSID_MDL_SHFEL2 == 22, "SHFEL2 SID changed");
static_assert(mdl::MDLSID_MDL_CZCEL2 == 23, "CZCEL2 SID changed");
static_assert(mdl::MDLSID_MDL_DCEL2 == 24, "DCEL2 SID changed");
static_assert(mdl::MDLSID_MDL_GFEXL2 == 26, "GFEXL2 SID changed");

static_assert(sh::MDLVID_MDL_SHL2 == 101, "SHL2 version changed");
static_assert(sz::MDLVID_MDL_SZL2 == 101, "SZL2 version changed");
static_assert(cffex::MDLVID_MDL_CFFEXL2 == 101, "CFFEXL2 version changed");
static_assert(shfe::MDLVID_MDL_SHFEL2 == 101, "SHFEL2 version changed");
static_assert(czce::MDLVID_MDL_CZCEL2 == 101, "CZCEL2 version changed");
static_assert(dce::MDLVID_MDL_DCEL2 == 101, "DCEL2 version changed");
static_assert(gfex::MDLVID_MDL_GFEXL2 == 101, "GFEXL2 version changed");

ASSERT_PACKED_SIZE(mdl::MDLDate, 4);
ASSERT_PACKED_SIZE(mdl::MDLTime, 4);
ASSERT_PACKED_SIZE(mdl::MDLString, 6);
ASSERT_PACKED_SIZE(mdl::MDLAnsiString, 6);
ASSERT_PACKED_SIZE(mdl::MDLUTF8String, 6);
ASSERT_PACKED_SIZE(mdl::MDLList, 8);
ASSERT_PACKED_SIZE(mdl::MDLListT<std::uint64_t>, 8);
ASSERT_PACKED_SIZE(mdl::MDLFloatT<3>, 4);
ASSERT_PACKED_SIZE(mdl::MDLDoubleT<3>, 8);

static_assert(mdl::MDLDate::s_NullValue == 100000000U,
              "MDLDate null sentinel changed");
static_assert(mdl::MDLTime::s_NullValue == 1000000000U,
              "MDLTime null sentinel changed");

static_assert(std::is_standard_layout<mdl::MDLMessageHead>::value,
              "MDLMessageHead must remain standard-layout");
ASSERT_PACKED_SIZE(mdl::MDLMessageHead, 23);
static_assert(offsetof(mdl::MDLMessageHead, HeadSize) == 0,
              "MDLMessageHead::HeadSize offset changed");
static_assert(offsetof(mdl::MDLMessageHead, MessageSize) == 1,
              "MDLMessageHead::MessageSize offset changed");
static_assert(offsetof(mdl::MDLMessageHead, MessageEncoding) == 5,
              "MDLMessageHead::MessageEncoding offset changed");
static_assert(offsetof(mdl::MDLMessageHead, ServiceID) == 6,
              "MDLMessageHead::ServiceID offset changed");
static_assert(offsetof(mdl::MDLMessageHead, ServiceVersion) == 7,
              "MDLMessageHead::ServiceVersion offset changed");
static_assert(offsetof(mdl::MDLMessageHead, MessageID) == 9,
              "MDLMessageHead::MessageID offset changed");
static_assert(offsetof(mdl::MDLMessageHead, LocalTime) == 11,
              "MDLMessageHead::LocalTime offset changed");
static_assert(offsetof(mdl::MDLMessageHead, SequenceID) == 15,
              "MDLMessageHead::SequenceID offset changed");

ASSERT_MESSAGE_KEY(sh::SHL2MarketData, mdl::MDLSID_MDL_SHL2, 4);
ASSERT_MESSAGE_KEY(sh::SHL2Transaction, mdl::MDLSID_MDL_SHL2, 3);
ASSERT_MESSAGE_KEY(sh::SHL2Transaction2, mdl::MDLSID_MDL_SHL2, 18);
ASSERT_MESSAGE_KEY(sh::Order, mdl::MDLSID_MDL_SHL2, 19);

ASSERT_MESSAGE_KEY(sz::Trade, mdl::MDLSID_MDL_SZL2, 1);
ASSERT_MESSAGE_KEY(sz::Order, mdl::MDLSID_MDL_SZL2, 2);
ASSERT_MESSAGE_KEY(sz::MarketData, mdl::MDLSID_MDL_SZL2, 4);
ASSERT_MESSAGE_KEY(sz::Snapshot300111_v2, mdl::MDLSID_MDL_SZL2, 28);
ASSERT_MESSAGE_KEY(sz::Order300192_v2, mdl::MDLSID_MDL_SZL2, 33);
ASSERT_MESSAGE_KEY(sz::Transaction300191_v2, mdl::MDLSID_MDL_SZL2, 36);
ASSERT_MESSAGE_KEY(sz::Snapshot300111_v3, mdl::MDLSID_MDL_SZL2, 39);
ASSERT_MESSAGE_KEY(sz::CombinedTick, mdl::MDLSID_MDL_SZL2, 53);

ASSERT_MESSAGE_KEY(cffex::Future, mdl::MDLSID_MDL_CFFEXL2, 1);
ASSERT_MESSAGE_KEY(cffex::Option, mdl::MDLSID_MDL_CFFEXL2, 2);
ASSERT_MESSAGE_KEY(shfe::CTPFuture, mdl::MDLSID_MDL_SHFEL2, 1);
ASSERT_MESSAGE_KEY(shfe::CTPOption, mdl::MDLSID_MDL_SHFEL2, 2);
ASSERT_MESSAGE_KEY(shfe::CrudeFuture, mdl::MDLSID_MDL_SHFEL2, 3);
ASSERT_MESSAGE_KEY(shfe::CrudeOption, mdl::MDLSID_MDL_SHFEL2, 4);
ASSERT_MESSAGE_KEY(czce::CTPFuture, mdl::MDLSID_MDL_CZCEL2, 1);
ASSERT_MESSAGE_KEY(czce::CTPOption, mdl::MDLSID_MDL_CZCEL2, 2);
ASSERT_MESSAGE_KEY(dce::Future, mdl::MDLSID_MDL_DCEL2, 1);
ASSERT_MESSAGE_KEY(dce::Option, mdl::MDLSID_MDL_DCEL2, 2);
ASSERT_MESSAGE_KEY(dce::FutureOrder, mdl::MDLSID_MDL_DCEL2, 7);
ASSERT_MESSAGE_KEY(dce::OptionOrder, mdl::MDLSID_MDL_DCEL2, 8);
ASSERT_MESSAGE_KEY(gfex::Future, mdl::MDLSID_MDL_GFEXL2, 1);
ASSERT_MESSAGE_KEY(gfex::Option, mdl::MDLSID_MDL_GFEXL2, 2);
ASSERT_MESSAGE_KEY(gfex::FutureOrder, mdl::MDLSID_MDL_GFEXL2, 7);
ASSERT_MESSAGE_KEY(gfex::OptionOrder, mdl::MDLSID_MDL_GFEXL2, 8);

ASSERT_PACKED_SIZE(sh::SHL2MarketData, 248);
ASSERT_PACKED_SIZE(sh::SHL2MarketData::BidLevelsItem, 28);
ASSERT_PACKED_SIZE(sh::SHL2MarketData::BidLevelsItem::NOrdersItem, 16);
ASSERT_PACKED_SIZE(sh::SHL2Transaction, 64);
ASSERT_PACKED_SIZE(sh::SHL2Transaction2, 80);
ASSERT_PACKED_SIZE(sh::Order, 70);

ASSERT_PACKED_SIZE(sz::Trade, 50);
ASSERT_PACKED_SIZE(sz::Order, 42);
ASSERT_PACKED_SIZE(sz::MarketData, 152);
ASSERT_PACKED_SIZE(sz::Snapshot300111_v2, 224);
ASSERT_PACKED_SIZE(sz::Snapshot300111_v2::BidPriceLevelItem, 28);
ASSERT_PACKED_SIZE(
    sz::Snapshot300111_v2::BidPriceLevelItem::OrdersItem, 8);
ASSERT_PACKED_SIZE(sz::Snapshot300111_v3, 256);
ASSERT_PACKED_SIZE(sz::Order300192_v2, 58);
ASSERT_PACKED_SIZE(sz::Transaction300191_v2, 70);
ASSERT_PACKED_SIZE(sz::CombinedTick, 70);

ASSERT_PACKED_SIZE(cffex::Future, 166);
ASSERT_PACKED_SIZE(cffex::Option, 166);
ASSERT_PACKED_SIZE(shfe::CTPFuture, 174);
ASSERT_PACKED_SIZE(shfe::CTPOption, 174);
ASSERT_PACKED_SIZE(shfe::CrudeFuture, 174);
ASSERT_PACKED_SIZE(shfe::CrudeOption, 174);
ASSERT_PACKED_SIZE(czce::CTPFuture, 218);
ASSERT_PACKED_SIZE(czce::CTPOption, 218);
ASSERT_PACKED_SIZE(dce::Future, 218);
ASSERT_PACKED_SIZE(dce::Option, 218);
ASSERT_PACKED_SIZE(dce::FutureOrder, 50);
ASSERT_PACKED_SIZE(dce::OptionOrder, 50);
ASSERT_PACKED_SIZE(gfex::Future, 218);
ASSERT_PACKED_SIZE(gfex::Option, 218);
ASSERT_PACKED_SIZE(gfex::FutureOrder, 50);
ASSERT_PACKED_SIZE(gfex::OptionOrder, 50);

#undef ASSERT_MESSAGE_KEY
#undef ASSERT_PACKED_SIZE

namespace {

typedef std::tuple<unsigned int, unsigned int, unsigned int> RuntimeKey;

struct ExpectedMessage {
    std::uint8_t service_id;
    std::uint16_t service_version;
    std::uint16_t message_id;
    const char* cpp_type;
};

#define EXPECTED_MESSAGE(Type)                                             \
    {static_cast<std::uint8_t>(Type::ServiceID),                           \
     static_cast<std::uint16_t>(Type::ServiceVer),                         \
     static_cast<std::uint16_t>(Type::MessageID), #Type}

const ExpectedMessage kExpectedMessages[] = {
    EXPECTED_MESSAGE(sh::SHL2Transaction),
    EXPECTED_MESSAGE(sh::SHL2MarketData),
    EXPECTED_MESSAGE(sh::SHL2Transaction2),
    EXPECTED_MESSAGE(sh::Order),
    EXPECTED_MESSAGE(sz::Trade),
    EXPECTED_MESSAGE(sz::Order),
    EXPECTED_MESSAGE(sz::MarketData),
    EXPECTED_MESSAGE(sz::Snapshot300111_v2),
    EXPECTED_MESSAGE(sz::Order300192_v2),
    EXPECTED_MESSAGE(sz::Transaction300191_v2),
    EXPECTED_MESSAGE(sz::Snapshot300111_v3),
    EXPECTED_MESSAGE(sz::CombinedTick),
    EXPECTED_MESSAGE(cffex::Future),
    EXPECTED_MESSAGE(cffex::Option),
    EXPECTED_MESSAGE(shfe::CTPFuture),
    EXPECTED_MESSAGE(shfe::CTPOption),
    EXPECTED_MESSAGE(shfe::CrudeFuture),
    EXPECTED_MESSAGE(shfe::CrudeOption),
    EXPECTED_MESSAGE(czce::CTPFuture),
    EXPECTED_MESSAGE(czce::CTPOption),
    EXPECTED_MESSAGE(dce::Future),
    EXPECTED_MESSAGE(dce::Option),
    EXPECTED_MESSAGE(dce::FutureOrder),
    EXPECTED_MESSAGE(dce::OptionOrder),
    EXPECTED_MESSAGE(gfex::Future),
    EXPECTED_MESSAGE(gfex::Option),
    EXPECTED_MESSAGE(gfex::FutureOrder),
    EXPECTED_MESSAGE(gfex::OptionOrder),
};

#undef EXPECTED_MESSAGE

const std::size_t kExpectedMessageCount =
    sizeof(kExpectedMessages) / sizeof(kExpectedMessages[0]);
static_assert(
    sizeof(kExpectedMessages) / sizeof(kExpectedMessages[0]) == 28,
    "the mock compatibility surface must contain exactly 28 message keys");

RuntimeKey MakeRuntimeKey(
    std::uint8_t service_id,
    std::uint16_t service_version,
    std::uint16_t message_id) {
    return RuntimeKey(
        static_cast<unsigned int>(service_id),
        static_cast<unsigned int>(service_version),
        static_cast<unsigned int>(message_id));
}

RuntimeKey MakeRuntimeKey(const mock::MessageKey& key) {
    return MakeRuntimeKey(
        key.service_id, key.service_version, key.message_id);
}

RuntimeKey MakeRuntimeKey(const ExpectedMessage& expected) {
    return MakeRuntimeKey(
        expected.service_id,
        expected.service_version,
        expected.message_id);
}

const ExpectedMessage* FindExpected(const mock::MessageKey& key) {
    const RuntimeKey wanted = MakeRuntimeKey(key);
    for (std::size_t index = 0; index < kExpectedMessageCount; ++index) {
        if (MakeRuntimeKey(kExpectedMessages[index]) == wanted) {
            return &kExpectedMessages[index];
        }
    }
    return NULL;
}

int ExpectedVersion(std::uint8_t service_id) {
    switch (service_id) {
    case mdl::MDLSID_MDL_SHL2:
        return sh::MDLVID_MDL_SHL2;
    case mdl::MDLSID_MDL_SZL2:
        return sz::MDLVID_MDL_SZL2;
    case mdl::MDLSID_MDL_CFFEXL2:
        return cffex::MDLVID_MDL_CFFEXL2;
    case mdl::MDLSID_MDL_SHFEL2:
        return shfe::MDLVID_MDL_SHFEL2;
    case mdl::MDLSID_MDL_CZCEL2:
        return czce::MDLVID_MDL_CZCEL2;
    case mdl::MDLSID_MDL_DCEL2:
        return dce::MDLVID_MDL_DCEL2;
    case mdl::MDLSID_MDL_GFEXL2:
        return gfex::MDLVID_MDL_GFEXL2;
    default:
        return -1;
    }
}

void PrintKey(const mock::MessageKey& key) {
    std::cerr << "SID=" << static_cast<unsigned int>(key.service_id)
              << ", version=" << key.service_version
              << ", MID=" << key.message_id;
}

void PrintKey(const ExpectedMessage& expected) {
    std::cerr << "SID=" << static_cast<unsigned int>(expected.service_id)
              << ", version=" << expected.service_version
              << ", MID=" << expected.message_id;
}

} // namespace

int main() {
    int failures = 0;
    const std::vector<mock::SupportedMessage>& supported =
        mock::SupportedMessages();

    if (supported.size() != kExpectedMessageCount) {
        std::cerr << "FAIL: mock::SupportedMessages() returned "
                  << supported.size() << " key(s); expected exactly "
                  << kExpectedMessageCount << '\n';
        ++failures;
    }

    std::set<RuntimeKey> seen;
    for (std::size_t index = 0; index < supported.size(); ++index) {
        const mock::MessageKey& key = supported[index].key;
        const int expected_version = ExpectedVersion(key.service_id);

        if (expected_version < 0) {
            std::cerr << "FAIL: SupportedMessages()[" << index
                      << "] declares an unexpected L2 service: ";
            PrintKey(key);
            std::cerr << '\n';
            ++failures;
        } else if (key.service_version !=
                   static_cast<std::uint16_t>(expected_version)) {
            std::cerr << "FAIL: SupportedMessages()[" << index
                      << "] has the wrong SDK service version: ";
            PrintKey(key);
            std::cerr << "; expected version=" << expected_version << '\n';
            ++failures;
        }

        if (key.message_id == 0) {
            std::cerr << "FAIL: SupportedMessages()[" << index
                      << "] uses reserved MID 0: ";
            PrintKey(key);
            std::cerr << '\n';
            ++failures;
        }

        const ExpectedMessage* const expected = FindExpected(key);
        if (expected == NULL) {
            std::cerr << "FAIL: SupportedMessages()[" << index
                      << "] is not in the expected 28-key SDK contract: ";
            PrintKey(key);
            std::cerr << '\n';
            ++failures;
        }

        const RuntimeKey runtime_key = MakeRuntimeKey(key);
        if (!seen.insert(runtime_key).second) {
            std::cerr << "FAIL: SupportedMessages()[" << index
                      << "] duplicates a key: ";
            PrintKey(key);
            std::cerr << '\n';
            ++failures;
        }
    }

    for (std::size_t index = 0; index < kExpectedMessageCount; ++index) {
        const ExpectedMessage& expected = kExpectedMessages[index];
        if (seen.count(MakeRuntimeKey(expected)) == 0) {
            std::cerr << "FAIL: SupportedMessages() omitted "
                      << expected.cpp_type << " (";
            PrintKey(expected);
            std::cerr << ")\n";
            ++failures;
        }
    }

    if (failures != 0) {
        std::cerr << "ABI/runtime contract test failed with " << failures
                  << " error(s)\n";
        return 1;
    }

    std::cout << "ABI/runtime contract checks passed for " << supported.size()
              << " supported message key(s)\n";
    return 0;
}
