#include "l2flow/market/daily_instrument_catalog_loader_v2.h"
#include "l2flow/market/daily_instrument_catalog_v2.h"
#include "l2flow/market/instrument_runtime_state_v2.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <unistd.h>

namespace {

namespace market = l2flow::market;

std::vector<std::byte> Bytes(std::string_view text) {
    const std::span<const char> characters(text.data(), text.size());
    const std::span<const std::byte> bytes =
        std::as_bytes(characters);
    return {bytes.begin(), bytes.end()};
}

market::DailyInstrumentSourceEntryV2 Entry(
    market::MarketV1 market_value,
    std::string_view source,
    std::string_view security_id,
    std::string_view external = {}) {
    market::DailyInstrumentSourceEntryV2 result{};
    result.key.market = market_value;
    result.key.security_id_source = Bytes(source);
    result.key.security_id = Bytes(security_id);
    result.metadata.quantity_unit = market::QuantityUnitV1::kShare;
    result.metadata.security_type = market::SecurityTypeV1::kEquity;
    result.metadata.asset_scope =
        market::AssetScopeV1::kDocumentedCore;
    result.external_instrument_id = Bytes(external);
    return result;
}

struct Test final {
    int failures = 0;

    void Expect(bool condition, std::string_view message) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << message << '\n';
        }
    }
};

std::unique_ptr<market::DailyInstrumentCatalogV2> MakeCatalog(
    Test* test,
    std::span<const market::DailyInstrumentSourceEntryV2> input) {
    market::DailyInstrumentCatalogConfigV2 config{};
    config.trade_date = 20260730U;
    config.catalog_version = 17U;
    config.session_epoch = 91U;
    config.market_scope = market::kDailyCatalogMainlandScopeV2;
    config.coverage_complete = true;
    std::unique_ptr<market::DailyInstrumentCatalogV2> catalog;
    const auto error = market::DailyInstrumentCatalogV2::Create(
        config, input, &catalog);
    test->Expect(
        error == market::DailyInstrumentCatalogCreateErrorV2::kNone &&
            catalog != nullptr,
        "daily catalog creates");
    return catalog;
}

void TestCanonicalDenseIdentity(Test* test) {
    std::vector<market::DailyInstrumentSourceEntryV2> input{
        Entry(market::MarketV1::kShenzhen, "102", "000001", "sz"),
        Entry(market::MarketV1::kShanghai, "", "600001", "sh"),
        Entry(market::MarketV1::kShanghai, "", "510300", "filtered"),
        Entry(market::MarketV1::kShanghai, "", "600001", "sh"),
    };
    std::unique_ptr<market::DailyInstrumentCatalogV2> catalog =
        MakeCatalog(test, input);
    if (catalog == nullptr) {
        return;
    }
    test->Expect(
        catalog->instrument_count() == 2U &&
            catalog->filtered_non_a_share_count() == 1U,
        "non-A-share input is filtered and duplicate is coalesced");
    const auto* first = catalog->EntryAt(0U);
    const auto* second = catalog->EntryAt(1U);
    test->Expect(
        first != nullptr && second != nullptr &&
            first->instrument_id == 1U && first->ordinal == 0U &&
            second->instrument_id == 2U && second->ordinal == 1U &&
            first->key.market == market::MarketV1::kShanghai &&
            second->key.market == market::MarketV1::kShenzhen,
        "exact-key order assigns dense deterministic IDs");
    const market::InstrumentKeyViewV1 exact{
        market::MarketV1::kShenzhen,
        input[0].key.security_id_source,
        input[0].key.security_id};
    const auto known = catalog->Lookup(exact);
    test->Expect(
        known.known() && known.entry->instrument_id == 2U,
        "exact bytes resolve without normalization");
    const auto wrong_source_bytes = Bytes("103");
    const market::InstrumentKeyViewV1 wrong_source{
        market::MarketV1::kShenzhen,
        wrong_source_bytes,
        input[0].key.security_id};
    test->Expect(
        catalog->Lookup(wrong_source).error ==
            market::DailyInstrumentCatalogLookupErrorV2::
                kUnknownInstrument,
        "SecurityIDSource has no fallback mapping");

    std::vector<market::DailyInstrumentSourceEntryV2> reverse(
        input.rbegin(), input.rend());
    std::unique_ptr<market::DailyInstrumentCatalogV2> reversed =
        MakeCatalog(test, reverse);
    test->Expect(
        reversed != nullptr &&
            reversed->catalog_digest() == catalog->catalog_digest(),
        "digest and identity are input-order independent");
}

void TestCatalogMissAndRuntimeFreeze(Test* test) {
    const std::vector<market::DailyInstrumentSourceEntryV2> input{
        Entry(market::MarketV1::kShanghai, "", "600001"),
        Entry(market::MarketV1::kShenzhen, "102", "000001"),
    };
    std::unique_ptr<market::DailyInstrumentCatalogV2> catalog =
        MakeCatalog(test, input);
    if (catalog == nullptr) {
        return;
    }
    const auto miss_id = Bytes("600002");
    const market::InstrumentKeyViewV1 miss{
        market::MarketV1::kShanghai, {}, miss_id};
    test->Expect(
        catalog->Lookup(miss).error ==
            market::DailyInstrumentCatalogLookupErrorV2::
                kUnknownInstrument,
        "valid A-share absent from catalog is an explicit miss");

    std::unique_ptr<market::InstrumentRuntimeStateV2> runtime_state;
    const auto create_error =
        market::InstrumentRuntimeStateV2::Create(
            *catalog, &runtime_state);
    test->Expect(
        create_error ==
                market::InstrumentRuntimeStateErrorV2::kNone &&
            runtime_state != nullptr &&
            runtime_state->capacity() == catalog->instrument_count() &&
            runtime_state->trade_date() == catalog->trade_date() &&
            runtime_state->catalog_version() ==
                catalog->catalog_version(),
        "runtime state is preallocated from frozen dense identity");
    if (runtime_state == nullptr) {
        return;
    }

    std::shared_ptr<const market::DailyInstrumentCatalogSnapshotV2>
        snapshot;
    test->Expect(
        runtime_state->AcquireSnapshot(&snapshot) ==
                market::InstrumentRuntimeStateErrorV2::kNone &&
            snapshot != nullptr &&
            snapshot->catalog_scope() ==
                market::InstrumentCatalogScopeV2::
                    kDeclaredDailyAShare &&
            snapshot->coverage_complete() &&
            snapshot->bound_count() == catalog->instrument_count() &&
            snapshot->catalog_digest() == catalog->catalog_digest(),
        "initial runtime snapshot declares every catalog identity bound");
}

void TestConflictingDuplicateRejected(Test* test) {
    std::vector<market::DailyInstrumentSourceEntryV2> input{
        Entry(market::MarketV1::kShanghai, "", "600001"),
        Entry(market::MarketV1::kShanghai, "", "600001"),
    };
    input[1].metadata.quantity_unit = market::QuantityUnitV1::kLot;
    market::DailyInstrumentCatalogConfigV2 config{};
    config.trade_date = 20260730U;
    config.catalog_version = 1U;
    config.session_epoch = 1U;
    config.market_scope = market::kDailyCatalogMainlandScopeV2;
    config.coverage_complete = true;
    std::unique_ptr<market::DailyInstrumentCatalogV2> catalog;
    test->Expect(
        market::DailyInstrumentCatalogV2::Create(
            config, input, &catalog) ==
            market::DailyInstrumentCatalogCreateErrorV2::
                kDuplicateMetadataConflict,
        "duplicate exact key with conflicting metadata is rejected");
}

void TestRealtimeExactKeyContract(Test* test) {
    market::DailyInstrumentCatalogConfigV2 config{};
    config.trade_date = 20260730U;
    config.catalog_version = 1U;
    config.session_epoch = 1U;
    config.market_scope = market::kDailyCatalogMainlandScopeV2;
    config.coverage_complete = true;
    const auto create_error = [&config](
        market::DailyInstrumentSourceEntryV2 entry) {
        std::unique_ptr<market::DailyInstrumentCatalogV2> catalog;
        return market::DailyInstrumentCatalogV2::Create(
            config,
            std::span<const market::DailyInstrumentSourceEntryV2>(
                &entry, 1U),
            &catalog);
    };

    test->Expect(
        create_error(Entry(
            market::MarketV1::kShanghai, "101", "600001")) ==
            market::DailyInstrumentCatalogCreateErrorV2::kInvalidKey,
        "Shanghai catalog key rejects an unreachable source component");
    test->Expect(
        create_error(Entry(
            market::MarketV1::kShenzhen, "", "000001")) ==
            market::DailyInstrumentCatalogCreateErrorV2::kInvalidKey,
        "Shenzhen catalog key requires exact SecurityIDSource bytes");
    auto nonprintable =
        Entry(market::MarketV1::kShenzhen, "102", "000001");
    nonprintable.key.security_id_source[1U] = std::byte{0U};
    test->Expect(
        create_error(std::move(nonprintable)) ==
            market::DailyInstrumentCatalogCreateErrorV2::kInvalidKey,
        "catalog rejects key bytes that callback extraction cannot produce");
}

class ScopedTempDirectory final {
public:
    ScopedTempDirectory() {
        std::array<char, 64U> pattern{};
        constexpr std::string_view prefix =
            "/tmp/l2flow-daily-catalog-v2-XXXXXX";
        std::copy(prefix.begin(), prefix.end(), pattern.begin());
        if (::mkdtemp(pattern.data()) != nullptr) {
            path_ = pattern.data();
        }
    }

    ScopedTempDirectory(const ScopedTempDirectory&) = delete;
    ScopedTempDirectory& operator=(const ScopedTempDirectory&) = delete;
    ~ScopedTempDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] bool valid() const noexcept {
        return !path_.empty();
    }

    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

bool WriteFile(
    const std::filesystem::path& path,
    std::string_view content) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(
        content.data(), static_cast<std::streamsize>(content.size()));
    output.close();
    return static_cast<bool>(output);
}

void TestStrictFileLoader(Test* test) {
    ScopedTempDirectory temporary;
    test->Expect(temporary.valid(), "create loader temp directory");
    if (!temporary.valid()) {
        return;
    }
    constexpr std::string_view valid_file =
        "L2FLOW_DAILY_INSTRUMENT_CATALOG_V2\t20260730\t17\t"
        "sh+sz\tcomplete\n"
        "sz\t31303220\t303030303031\tshare\tequity\t"
        "documented_core\t-\n"
        "sh\t-\t363030303031\tshare\tequity\t"
        "documented_core\t7368\n";
    const std::filesystem::path valid_path =
        temporary.path() / "daily.catalog";
    test->Expect(
        WriteFile(valid_path, valid_file),
        "write strict daily catalog fixture");

    market::DailyInstrumentCatalogFileOptionsV2 options{};
    options.path = valid_path;
    options.expected_trade_date = 20260730U;
    options.expected_catalog_version = 17U;
    options.session_epoch = 91U;
    market::DailyInstrumentCatalogFileResultV2 loaded =
        market::LoadDailyInstrumentCatalogFileV2(options);
    test->Expect(
        loaded.ok() && loaded.source_row_count == 2U &&
            loaded.catalog->instrument_count() == 2U &&
            loaded.catalog->trade_date() == 20260730U &&
            loaded.catalog->catalog_version() == 17U,
        "strict loader freezes the declared complete daily catalog");
    if (loaded.ok()) {
        const auto exact_source = Bytes("102 ");
        const auto exact_id = Bytes("000001");
        const market::InstrumentKeyViewV1 key{
            market::MarketV1::kShenzhen,
            exact_source,
            exact_id};
        test->Expect(
            loaded.catalog->Lookup(key).known(),
            "loader preserves exact SecurityIDSource bytes");
    }

    market::DailyInstrumentCatalogFileOptionsV2 mismatch = options;
    mismatch.expected_catalog_version = 18U;
    test->Expect(
        market::LoadDailyInstrumentCatalogFileV2(mismatch).error ==
            market::DailyInstrumentCatalogFileErrorV2::
                kCatalogVersionMismatch,
        "loader rejects a declared version mismatch");

    const std::filesystem::path uppercase_path =
        temporary.path() / "uppercase.catalog";
    std::string uppercase(valid_file);
    uppercase.replace(
        uppercase.find("363030303031"), 12U, "36303030303A");
    test->Expect(
        WriteFile(uppercase_path, uppercase),
        "write noncanonical hex fixture");
    options.path = uppercase_path;
    test->Expect(
        market::LoadDailyInstrumentCatalogFileV2(options).error ==
            market::DailyInstrumentCatalogFileErrorV2::kInvalidHex,
        "loader rejects uppercase hex instead of normalizing it");

    const std::filesystem::path no_newline_path =
        temporary.path() / "no-newline.catalog";
    test->Expect(
        WriteFile(
            no_newline_path,
            valid_file.substr(0U, valid_file.size() - 1U)),
        "write missing-newline fixture");
    options.path = no_newline_path;
    test->Expect(
        market::LoadDailyInstrumentCatalogFileV2(options).error ==
            market::DailyInstrumentCatalogFileErrorV2::
                kMissingFinalNewline,
        "loader rejects a missing final newline");

    const std::filesystem::path symlink_path =
        temporary.path() / "daily.symlink";
    std::error_code symlink_error;
    std::filesystem::create_symlink(
        valid_path, symlink_path, symlink_error);
    test->Expect(!symlink_error, "create loader symlink fixture");
    if (!symlink_error) {
        options.path = symlink_path;
        test->Expect(
            market::LoadDailyInstrumentCatalogFileV2(options).error ==
                market::DailyInstrumentCatalogFileErrorV2::
                    kSymlinkRejected,
            "loader rejects symlinks fail-closed");
    }

    options.path = "relative.catalog";
    test->Expect(
        market::LoadDailyInstrumentCatalogFileV2(options).error ==
            market::DailyInstrumentCatalogFileErrorV2::
                kPathNotAbsolute,
        "loader requires an absolute operator-owned path");
}

}  // namespace

int main() {
    Test test;
    TestCanonicalDenseIdentity(&test);
    TestCatalogMissAndRuntimeFreeze(&test);
    TestConflictingDuplicateRejected(&test);
    TestRealtimeExactKeyContract(&test);
    TestStrictFileLoader(&test);
    if (test.failures != 0) {
        std::cerr << test.failures << " daily catalog test(s) failed\n";
        return 1;
    }
    std::cout << "daily instrument catalog tests passed\n";
    return 0;
}
