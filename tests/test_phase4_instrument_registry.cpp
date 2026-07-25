#include "l2flow/common/sha256.h"
#include "l2flow/market/instrument_registry.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace common = l2flow::common;
namespace market = l2flow::market;

namespace {

class TestContext final {
public:
    void Expect(bool condition, std::string_view description) {
        if (!condition) {
            ++failures_;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    [[nodiscard]] int failures() const noexcept {
        return failures_;
    }

private:
    int failures_ = 0;
};

std::vector<std::byte> Bytes(std::string_view value) {
    const std::span<const char> characters(value.data(), value.size());
    const std::span<const std::byte> bytes =
        std::as_bytes(characters);
    return std::vector<std::byte>(bytes.begin(), bytes.end());
}

market::InstrumentRegistryEntryV1 ShanghaiEntry() {
    market::InstrumentRegistryEntryV1 entry;
    entry.instrument_id = 100U;
    entry.key.market = market::MarketV1::kShanghai;
    entry.key.security_id = Bytes("600000");
    entry.quantity_unit = market::QuantityUnitV1::kShare;
    entry.security_type = market::SecurityTypeV1::kEquity;
    entry.asset_scope = market::AssetScopeV1::kDocumentedCore;
    return entry;
}

market::InstrumentRegistryEntryV1 ShenzhenEntry() {
    market::InstrumentRegistryEntryV1 entry;
    entry.instrument_id = 200U;
    entry.key.market = market::MarketV1::kShenzhen;
    entry.key.security_id_source = Bytes("102");
    entry.key.security_id = Bytes("000001");
    entry.quantity_unit = market::QuantityUnitV1::kShare;
    entry.security_type = market::SecurityTypeV1::kEquity;
    entry.asset_scope = market::AssetScopeV1::kDocumentedCore;
    return entry;
}

void TestCanonicalRegistry(TestContext* test) {
    std::vector<market::InstrumentRegistryEntryV1> forward;
    forward.push_back(ShanghaiEntry());
    forward.push_back(ShenzhenEntry());
    std::vector<market::InstrumentRegistryEntryV1> reverse = forward;
    std::reverse(reverse.begin(), reverse.end());

    std::unique_ptr<market::InstrumentRegistryV1> first;
    std::unique_ptr<market::InstrumentRegistryV1> second;
    test->Expect(
        market::InstrumentRegistryV1::Create(7U, forward, &first) ==
                market::InstrumentRegistryCreateErrorV1::kNone &&
            market::InstrumentRegistryV1::Create(7U, reverse, &second) ==
                market::InstrumentRegistryCreateErrorV1::kNone,
        "valid registry is independent of input order");
    if (first == nullptr || second == nullptr) {
        return;
    }
    test->Expect(
        first->registry_sha256() == second->registry_sha256(),
        "canonical registry digest is order independent");
    test->Expect(
        common::Sha256Hex(first->registry_sha256()) ==
            "6b3af4fa28a616784bf879347df6b394d7d22dd4f6841f6a"
            "00228073f58e318c",
        "registry digest matches an independent little-endian golden");
    test->Expect(
        first->entries().size() == 2U &&
            first->entries()[0].key.market ==
                market::MarketV1::kShanghai &&
            first->entries()[1].key.market ==
                market::MarketV1::kShenzhen,
        "published entries use exact canonical key order");

    const market::InstrumentRegistryLookupResultV1 sh = first->Lookup(
        market::MarketV1::kShanghai, "", "600000");
    const market::InstrumentRegistryLookupResultV1 sz = first->Lookup(
        market::MarketV1::kShenzhen, "102", "000001");
    test->Expect(
        sh.known() && sh.instrument_id == 100U &&
            sh.registry_ordinal == 0U &&
            sh.quantity_unit == market::QuantityUnitV1::kShare &&
            sz.known() && sz.instrument_id == 200U &&
            sz.registry_ordinal == 1U,
        "exact byte keys resolve explicit metadata");
    const market::InstrumentRegistryLookupResultV1 unknown_key =
        first->Lookup(
            market::MarketV1::kShanghai, "", "600001");
    test->Expect(
        first->Lookup(
                 market::MarketV1::kShanghai, "", "600000 ")
                .error == market::InstrumentRegistryLookupErrorV1::
                              kUnknownInstrument &&
            first->Lookup(
                     market::MarketV1::kShenzhen, "", "000001")
                    .error == market::InstrumentRegistryLookupErrorV1::
                                  kUnknownInstrument &&
            unknown_key.registry_ordinal ==
                std::numeric_limits<std::size_t>::max(),
        "lookup never trims identifiers or guesses missing source bytes");
    const market::InstrumentRegistryLookupResultV1 by_id =
        first->LookupById(200U);
    const market::InstrumentRegistryLookupResultV1 zero_id =
        first->LookupById(0U);
    const market::InstrumentRegistryLookupResultV1 unknown_id =
        first->LookupById(201U);
    test->Expect(
        by_id.known() && by_id.registry_ordinal == 1U &&
            zero_id.error ==
                market::InstrumentRegistryLookupErrorV1::kZeroInstrumentId,
        "explicit instrument-id index is validated");
    test->Expect(
        zero_id.registry_ordinal ==
                std::numeric_limits<std::size_t>::max() &&
            unknown_id.error ==
                market::InstrumentRegistryLookupErrorV1::
                    kUnknownInstrument &&
            unknown_id.registry_ordinal ==
                std::numeric_limits<std::size_t>::max(),
        "failed id lookups retain the unknown ordinal sentinel");
}

void TestInstrumentIdOrdinal(TestContext* test) {
    market::InstrumentRegistryEntryV1 shanghai = ShanghaiEntry();
    shanghai.instrument_id = 900U;
    market::InstrumentRegistryEntryV1 shenzhen = ShenzhenEntry();
    shenzhen.instrument_id = 100U;
    const std::vector<market::InstrumentRegistryEntryV1> entries{
        shenzhen, shanghai};

    std::unique_ptr<market::InstrumentRegistryV1> registry;
    test->Expect(
        market::InstrumentRegistryV1::Create(
            8U, entries, &registry) ==
                market::InstrumentRegistryCreateErrorV1::kNone &&
            registry != nullptr,
        "registry with inverse key/id ordering creates");
    if (registry == nullptr) {
        return;
    }

    const auto shanghai_by_key = registry->Lookup(
        market::MarketV1::kShanghai, "", "600000");
    const auto shanghai_by_id = registry->LookupById(900U);
    const auto shenzhen_by_key = registry->Lookup(
        market::MarketV1::kShenzhen, "102", "000001");
    const auto shenzhen_by_id = registry->LookupById(100U);

    test->Expect(
        registry->entries()[0].instrument_id == 900U &&
            registry->entries()[1].instrument_id == 100U,
        "public entries remain in canonical byte-key order");
    test->Expect(
        shenzhen_by_key.known() &&
            shenzhen_by_key.registry_ordinal == 0U &&
            shenzhen_by_id.known() &&
            shenzhen_by_id.registry_ordinal == 0U &&
            shenzhen_by_key.entry == shenzhen_by_id.entry,
        "lowest instrument id has ordinal zero through both indexes");
    test->Expect(
        shanghai_by_key.known() &&
            shanghai_by_key.registry_ordinal == 1U &&
            shanghai_by_id.known() &&
            shanghai_by_id.registry_ordinal == 1U &&
            shanghai_by_key.entry == shanghai_by_id.entry,
        "key lookup maps canonical entry index to id-sorted ordinal");
}

void TestCreationFailures(TestContext* test) {
    std::vector<market::InstrumentRegistryEntryV1> entries;
    entries.push_back(ShanghaiEntry());
    std::unique_ptr<market::InstrumentRegistryV1> output;
    test->Expect(
        market::InstrumentRegistryV1::Create(0U, entries, &output) ==
                market::InstrumentRegistryCreateErrorV1::
                    kZeroRegistryVersion &&
            output == nullptr,
        "zero registry version fails closed");

    entries.push_back(ShanghaiEntry());
    entries.back().instrument_id = 101U;
    test->Expect(
        market::InstrumentRegistryV1::Create(1U, entries, &output) ==
                market::InstrumentRegistryCreateErrorV1::kDuplicateKey &&
            output == nullptr,
        "duplicate exact key is rejected");

    entries.clear();
    entries.push_back(ShanghaiEntry());
    entries.push_back(ShenzhenEntry());
    entries.back().instrument_id = entries.front().instrument_id;
    test->Expect(
        market::InstrumentRegistryV1::Create(1U, entries, &output) ==
                market::InstrumentRegistryCreateErrorV1::
                    kDuplicateInstrumentId &&
            output == nullptr,
        "duplicate explicit instrument id is rejected");

    entries.resize(1U);
    entries.front() = ShanghaiEntry();
    entries.front().key.market = market::MarketV1::kUnknown;
    test->Expect(
        market::InstrumentRegistryV1::Create(1U, entries, &output) ==
            market::InstrumentRegistryCreateErrorV1::kInvalidMarket,
        "unknown market cannot enter the registry");
}

}  // namespace

int main() {
    TestContext test;
    TestCanonicalRegistry(&test);
    TestInstrumentIdOrdinal(&test);
    TestCreationFailures(&test);
    if (test.failures() == 0) {
        std::cout << "phase4 instrument registry tests passed\n";
    }
    return test.failures() == 0 ? 0 : 1;
}
