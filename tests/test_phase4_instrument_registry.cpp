#include "l2flow/common/sha256.h"
#include "l2flow/market/instrument_registry.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
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
            sh.quantity_unit == market::QuantityUnitV1::kShare &&
            sz.known() && sz.instrument_id == 200U,
        "exact byte keys resolve explicit metadata");
    test->Expect(
        first->Lookup(
                 market::MarketV1::kShanghai, "", "600000 ")
                .error == market::InstrumentRegistryLookupErrorV1::
                              kUnknownInstrument &&
            first->Lookup(
                     market::MarketV1::kShenzhen, "", "000001")
                    .error == market::InstrumentRegistryLookupErrorV1::
                                  kUnknownInstrument,
        "lookup never trims identifiers or guesses missing source bytes");
    test->Expect(
        first->LookupById(200U).known() &&
            first->LookupById(0U).error ==
                market::InstrumentRegistryLookupErrorV1::kZeroInstrumentId,
        "explicit instrument-id index is validated");
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
    TestCreationFailures(&test);
    if (test.failures() == 0) {
        std::cout << "phase4 instrument registry tests passed\n";
    }
    return test.failures() == 0 ? 0 : 1;
}
