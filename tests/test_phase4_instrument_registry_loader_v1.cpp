#include "l2flow/market/instrument_registry_loader_v1.h"

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace market = l2flow::market;

namespace {

struct TestContext final {
    void Expect(bool condition, std::string_view message) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << message << '\n';
        }
    }
    int failures = 0;
};

class TemporaryDirectory final {
public:
    TemporaryDirectory() {
        std::array<char, 64U> pattern{};
        const char* prefix = "/tmp/l2flow-registry-loader-XXXXXX";
        std::memcpy(pattern.data(), prefix, std::strlen(prefix) + 1U);
        char* const created = ::mkdtemp(pattern.data());
        if (created == nullptr) {
            return;
        }
        path_ = created;
        descriptor_ = ::open(
            path_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    }

    ~TemporaryDirectory() {
        if (descriptor_ >= 0) {
            static_cast<void>(::unlinkat(descriptor_, "registry.tsv", 0));
            static_cast<void>(::close(descriptor_));
        }
        if (!path_.empty()) {
            static_cast<void>(::rmdir(path_.c_str()));
        }
    }

    TemporaryDirectory(const TemporaryDirectory&) = delete;
    TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

    [[nodiscard]] bool ok() const noexcept { return descriptor_ >= 0; }
    [[nodiscard]] int descriptor() const noexcept { return descriptor_; }

    [[nodiscard]] bool Write(std::string_view bytes, mode_t mode = 0600) {
        static_cast<void>(::unlinkat(descriptor_, "registry.tsv", 0));
        const int file = ::openat(
            descriptor_, "registry.tsv",
            O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, mode);
        if (file < 0) {
            return false;
        }
        if (::fchmod(file, mode) != 0) {
            static_cast<void>(::close(file));
            return false;
        }
        std::size_t offset = 0U;
        while (offset < bytes.size()) {
            const ssize_t written = ::write(
                file, bytes.data() + offset, bytes.size() - offset);
            if (written < 0 && errno == EINTR) {
                continue;
            }
            if (written <= 0) {
                static_cast<void>(::close(file));
                return false;
            }
            offset += static_cast<std::size_t>(written);
        }
        const bool synced = ::fsync(file) == 0;
        const bool closed = ::close(file) == 0;
        return synced && closed;
    }

private:
    std::string path_;
    int descriptor_ = -1;
};

std::vector<std::byte> Bytes(std::string_view text) {
    const std::span<const char> chars(text.data(), text.size());
    const std::span<const std::byte> bytes = std::as_bytes(chars);
    return {bytes.begin(), bytes.end()};
}

std::unique_ptr<market::InstrumentRegistryV1> ExpectedRegistry(
    TestContext* test) {
    std::array<market::InstrumentRegistryEntryV1, 2U> entries{};
    entries[0].instrument_id = 1U;
    entries[0].key.market = market::MarketV1::kShanghai;
    entries[0].key.security_id = Bytes("600000");
    entries[0].quantity_unit = market::QuantityUnitV1::kShare;
    entries[0].security_type = market::SecurityTypeV1::kEquity;
    entries[0].asset_scope = market::AssetScopeV1::kDocumentedCore;

    entries[1].instrument_id = 2U;
    entries[1].key.market = market::MarketV1::kShenzhen;
    entries[1].key.security_id_source = Bytes("102");
    entries[1].key.security_id = Bytes("000001");
    entries[1].quantity_unit = market::QuantityUnitV1::kLot;
    entries[1].security_type = market::SecurityTypeV1::kBond;
    entries[1].asset_scope = market::AssetScopeV1::kOutsideDocumentedCore;

    std::unique_ptr<market::InstrumentRegistryV1> registry;
    const auto error = market::InstrumentRegistryV1::Create(
        7U, entries, &registry);
    test->Expect(
        error == market::InstrumentRegistryCreateErrorV1::kNone &&
            registry != nullptr,
        "expected registry fixture is valid");
    return registry;
}

market::InstrumentRegistryFileOptionsV1 Options(
    int directory,
    const market::InstrumentRegistryV1& expected) {
    market::InstrumentRegistryFileOptionsV1 options{};
    options.directory_fd = directory;
    options.file_name = "registry.tsv";
    options.expected_owner_uid = static_cast<std::uint32_t>(::getuid());
    options.expected_registry_version = expected.registry_version();
    options.expected_registry_sha256 = expected.registry_sha256();
    return options;
}

constexpr std::string_view kValidFile =
    "L2FLOW_INSTRUMENT_REGISTRY_V1\t7\n"
    "2\tsz\t313032\t303030303031\tlot\tbond\toutside_documented_core\n"
    "1\tsh\t-\t363030303030\tshare\tequity\tdocumented_core\n";

void TestValidPinnedLoad(TestContext* test) {
    TemporaryDirectory directory;
    auto expected = ExpectedRegistry(test);
    test->Expect(directory.ok() && expected != nullptr, "valid fixture opens");
    if (!directory.ok() || expected == nullptr) {
        return;
    }
    test->Expect(directory.Write(kValidFile), "valid registry file writes");
    auto result = market::LoadInstrumentRegistryFileV1(
        Options(directory.descriptor(), *expected));
    test->Expect(
        result.ok() && result.registry->size() == 2U &&
            result.registry->registry_sha256() == expected->registry_sha256(),
        "strict loader accepts canonical rows independent of input order");
    if (result.ok()) {
        const auto sh = result.registry->Lookup(
            market::MarketV1::kShanghai, "", "600000");
        const auto sz = result.registry->Lookup(
            market::MarketV1::kShenzhen, "102", "000001");
        test->Expect(
            sh.known() && sh.instrument_id == 1U &&
                sz.known() && sz.instrument_id == 2U,
            "loaded opaque keys resolve to fixed instrument ids");
    }
}

void TestIdentityAndGrammarFailures(TestContext* test) {
    TemporaryDirectory directory;
    auto expected = ExpectedRegistry(test);
    if (!directory.ok() || expected == nullptr) {
        test->Expect(false, "failure fixture opens");
        return;
    }
    auto options = Options(directory.descriptor(), *expected);

    test->Expect(directory.Write(kValidFile), "identity file writes");
    options.expected_registry_sha256[0] ^= std::byte{0x01U};
    auto result = market::LoadInstrumentRegistryFileV1(options);
    test->Expect(
        result.error ==
                market::InstrumentRegistryFileErrorV1::kRegistrySha256Mismatch &&
            result.registry == nullptr,
        "content is rejected when the mandatory canonical digest pin differs");

    options = Options(directory.descriptor(), *expected);
    test->Expect(
        directory.Write(
            "L2FLOW_INSTRUMENT_REGISTRY_V1\t7\n"
            "01\tsh\t-\t363030303030\tshare\tequity\tdocumented_core\n"),
        "noncanonical number file writes");
    result = market::LoadInstrumentRegistryFileV1(options);
    test->Expect(
        result.error == market::InstrumentRegistryFileErrorV1::kInvalidNumber &&
            result.line == 2U,
        "leading-zero instrument ids are rejected at the exact line");

    test->Expect(
        directory.Write(
            "L2FLOW_INSTRUMENT_REGISTRY_V1\t7\n"
            "1\tsh\t-\t36303030303A\tshare\tequity\tdocumented_core\n"),
        "uppercase hex file writes");
    result = market::LoadInstrumentRegistryFileV1(options);
    test->Expect(
        result.error == market::InstrumentRegistryFileErrorV1::kInvalidHex &&
            result.line == 2U,
        "uppercase/noncanonical identifier hex is rejected");

    std::string without_newline(kValidFile);
    without_newline.pop_back();
    test->Expect(directory.Write(without_newline), "unterminated file writes");
    result = market::LoadInstrumentRegistryFileV1(options);
    test->Expect(
        result.error ==
            market::InstrumentRegistryFileErrorV1::kMissingFinalNewline,
        "missing final LF is rejected");
}

void TestFilesystemAndDuplicateFailures(TestContext* test) {
    TemporaryDirectory directory;
    auto expected = ExpectedRegistry(test);
    if (!directory.ok() || expected == nullptr) {
        test->Expect(false, "filesystem fixture opens");
        return;
    }
    auto options = Options(directory.descriptor(), *expected);
    test->Expect(directory.Write(kValidFile, 0644), "unsafe mode file writes");
    auto result = market::LoadInstrumentRegistryFileV1(options);
    test->Expect(
        result.error == market::InstrumentRegistryFileErrorV1::kUnsafeFileMode,
        "group/world-readable registry files are rejected");

    test->Expect(
        directory.Write(
            "L2FLOW_INSTRUMENT_REGISTRY_V1\t7\n"
            "1\tsh\t-\t363030303030\tshare\tequity\tdocumented_core\n"
            "2\tsh\t-\t363030303030\tshare\tequity\tdocumented_core\n"),
        "duplicate key file writes");
    result = market::LoadInstrumentRegistryFileV1(options);
    test->Expect(
        result.error == market::InstrumentRegistryFileErrorV1::kRegistryRejected &&
            result.registry_error ==
                market::InstrumentRegistryCreateErrorV1::kDuplicateKey &&
            result.registry == nullptr,
        "canonical registry validation rejects duplicate opaque keys");
}

}  // namespace

int main() {
    TestContext test;
    TestValidPinnedLoad(&test);
    TestIdentityAndGrammarFailures(&test);
    TestFilesystemAndDuplicateFailures(&test);
    if (test.failures != 0) {
        std::cerr << test.failures << " registry loader test(s) failed\n";
        return 1;
    }
    std::cout << "instrument registry loader tests passed\n";
    return 0;
}
