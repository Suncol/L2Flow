#include "l2flow/baseline/vendor_baseline.h"
#include "l2flow/common/sha256.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <elf.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace baseline = l2flow::baseline;
namespace common = l2flow::common;

namespace {

class TestContext {
public:
    void Expect(bool condition, const std::string& description) {
        if (condition) {
            return;
        }
        ++failures_;
        std::cerr << "FAIL: " << description << '\n';
    }

    int failures() const noexcept {
        return failures_;
    }

private:
    int failures_ = 0;
};

class TemporaryDirectory {
public:
    TemporaryDirectory() {
        const auto ticks =
            std::chrono::steady_clock::now().time_since_epoch().count();
        path_ = std::filesystem::temp_directory_path() /
                ("l2flow-phase0-" + std::to_string(::getpid()) + "-" +
                 std::to_string(ticks));
        std::filesystem::create_directories(path_);
    }

    ~TemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

bool IsRepositoryRoot(const std::filesystem::path& candidate) {
    const bool has_baseline =
        std::filesystem::is_regular_file(
            candidate / "configs/vendor_baseline.json");
#if defined(L2FLOW_SANITIZER_BUILD)
    return has_baseline;
#else
    return has_baseline &&
           std::filesystem::is_regular_file(
               candidate /
               "mdl_sdk_2_13_234/libs/linux/libmdl_api.so");
#endif
}

std::optional<std::filesystem::path> SearchParents(
    std::filesystem::path candidate) {
    std::error_code canonical_error;
    candidate =
        std::filesystem::weakly_canonical(candidate, canonical_error);
    if (canonical_error) {
        return std::nullopt;
    }
    for (int level = 0; level < 12; ++level) {
        if (IsRepositoryRoot(candidate)) {
            return candidate;
        }
        const std::filesystem::path parent = candidate.parent_path();
        if (parent == candidate || parent.empty()) {
            break;
        }
        candidate = parent;
    }
    return std::nullopt;
}

std::optional<std::filesystem::path> FindRepositoryRoot() {
    if (const auto from_working_directory =
            SearchParents(std::filesystem::current_path());
        from_working_directory.has_value()) {
        return from_working_directory;
    }
    const std::filesystem::path source_file(__FILE__);
    if (source_file.is_absolute()) {
        return SearchParents(source_file.parent_path());
    }
    return std::nullopt;
}

bool WriteBytes(const std::filesystem::path& path,
                std::span<const std::byte> bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    if (!output.is_open()) {
        return false;
    }
    if (!bytes.empty()) {
        output.write(reinterpret_cast<const char*>(bytes.data()),
                     static_cast<std::streamsize>(bytes.size()));
    }
    output.flush();
    return static_cast<bool>(output);
}

bool WriteText(const std::filesystem::path& path, std::string_view text) {
    return WriteBytes(path, std::as_bytes(std::span{text.data(), text.size()}));
}

const baseline::CheckResult* FindCheck(
    const baseline::PreflightReport& report,
    std::string_view id) {
    const auto found = std::find_if(
        report.checks.begin(),
        report.checks.end(),
        [id](const baseline::CheckResult& check) {
            return check.id == id;
        });
    return found == report.checks.end() ? nullptr : &*found;
}

const baseline::TypeLayout* FindTypeLayout(
    const baseline::VendorBaseline& vendor,
    std::string_view name) {
    const auto found = std::find_if(
        vendor.abi_types.begin(),
        vendor.abi_types.end(),
        [name](const baseline::TypeLayout& type) {
            return type.name == name;
        });
    return found == vendor.abi_types.end() ? nullptr : &*found;
}

const baseline::MemberLayout* FindMemberLayout(
    const baseline::TypeLayout& type,
    std::string_view name) {
    const auto found = std::find_if(
        type.members.begin(),
        type.members.end(),
        [name](const baseline::MemberLayout& member) {
            return member.name == name;
        });
    return found == type.members.end() ? nullptr : &*found;
}

void ExpectMember(const baseline::VendorBaseline& vendor,
                  std::string_view type_name,
                  std::string_view member_name,
                  std::size_t expected_offset,
                  TestContext* test) {
    const baseline::TypeLayout* type =
        FindTypeLayout(vendor, type_name);
    const baseline::MemberLayout* member =
        type == nullptr
            ? nullptr
            : FindMemberLayout(*type, member_name);
    test->Expect(
        member != nullptr && member->offset == expected_offset,
        "baseline freezes " + std::string(type_name) + "::" +
            std::string(member_name) + " at offset " +
            std::to_string(expected_offset));
}

void CheckSha256(const TemporaryDirectory& temporary, TestContext* test) {
    struct Vector {
        std::string_view input;
        std::string_view expected;
    };
    constexpr std::array<Vector, 3> kVectors = {{
        {"",
         "e3b0c44298fc1c149afbf4c8996fb924"
         "27ae41e4649b934ca495991b7852b855"},
        {"abc",
         "ba7816bf8f01cfea414140de5dae2223"
         "b00361a396177a9cb410ff61f20015ad"},
        {"abcdbcdecdefdefgefghfghighijhijk"
         "ijkljklmklmnlmnomnopnopq",
         "248d6a61d20638b8e5c026930c3e6039"
         "a33ce45964ff2167f6ecedd419db06c1"},
    }};
    for (const Vector& vector : kVectors) {
        const std::string actual =
            common::Sha256Hex(common::ComputeSha256(vector.input));
        test->Expect(actual == vector.expected,
                     "SHA-256 known vector matches: " +
                         std::string(vector.input));
    }

    const std::filesystem::path file = temporary.path() / "abc.bin";
    test->Expect(WriteText(file, "abc"), "writes SHA-256 file fixture");
    common::Sha256Digest file_digest{};
    std::string error;
    test->Expect(common::ComputeFileSha256(file, &file_digest, &error),
                 "file SHA-256 succeeds: " + error);
    test->Expect(
        common::Sha256Hex(file_digest) == kVectors[1].expected,
        "streaming file SHA-256 matches the in-memory vector");

    common::Sha256Digest bounded_digest{};
    bounded_digest.fill(std::byte{0x5a});
    const common::Sha256Digest unchanged_digest =
        bounded_digest;
    test->Expect(
        !common::ComputeFileSha256(
            file,
            &bounded_digest,
            &error,
            2U) &&
            bounded_digest == unchanged_digest,
        "file SHA-256 rejects an over-bound source before changing output");
    test->Expect(
        common::ComputeFileSha256(
            file,
            &bounded_digest,
            &error,
            3U) &&
            common::Sha256Hex(bounded_digest) ==
                kVectors[1].expected,
        "file SHA-256 accepts an exact size bound");

    const std::filesystem::path symlink =
        temporary.path() / "abc-link.bin";
    std::error_code symlink_error;
    std::filesystem::create_symlink(
        file, symlink, symlink_error);
    test->Expect(
        !symlink_error,
        "creates SHA-256 symlink fixture");
    if (!symlink_error) {
        test->Expect(
            !common::ComputeFileSha256(
                symlink,
                &bounded_digest,
                &error),
            "path-based SHA-256 refuses a symbolic link");
    }

    const std::filesystem::path fifo =
        temporary.path() / "hash-source.fifo";
    test->Expect(
        ::mkfifo(fifo.c_str(), S_IRUSR | S_IWUSR) == 0,
        "creates SHA-256 FIFO fixture");
    test->Expect(
        !common::ComputeFileSha256(
            fifo,
            &bounded_digest,
            &error),
        "path-based SHA-256 rejects a FIFO without blocking");

    const int file_fd =
        ::open(file.c_str(), O_RDONLY | O_CLOEXEC);
    test->Expect(
        file_fd >= 0,
        "opens SHA-256 descriptor fixture");
    if (file_fd >= 0) {
        test->Expect(
            ::lseek(file_fd, 1, SEEK_SET) == 1,
            "sets the caller-owned SHA-256 descriptor offset");
        common::Sha256Digest fd_digest{};
        test->Expect(
            common::ComputeFileSha256ForOpenFd(
                file_fd,
                &fd_digest,
                &error,
                3U) &&
                common::Sha256Hex(fd_digest) ==
                    kVectors[1].expected,
            "descriptor SHA-256 hashes exact regular-file bytes");
        test->Expect(
            ::lseek(file_fd, 0, SEEK_CUR) == 1,
            "descriptor SHA-256 preserves the caller's file offset");
        static_cast<void>(::close(file_fd));
    }

    common::Sha256Digest parsed{};
    test->Expect(
        common::ParseSha256Hex(
            "BA7816BF8F01CFEA414140DE5DAE2223"
            "B00361A396177A9CB410FF61F20015AD",
            &parsed,
            &error),
        "uppercase SHA-256 text parses");
    test->Expect(
        common::Sha256Hex(parsed) == kVectors[1].expected,
        "parsed SHA-256 is normalized to lowercase");
    test->Expect(
        !common::ParseSha256Hex("abc", &parsed, &error),
        "short SHA-256 text is rejected");
    test->Expect(
        !common::ParseSha256Hex(
            "ga7816bf8f01cfea414140de5dae2223"
            "b00361a396177a9cb410ff61f20015ad",
            &parsed,
            &error),
        "non-hex SHA-256 text is rejected");
    test->Expect(
        !common::ComputeFileSha256(
            temporary.path() / "missing.bin", &file_digest, &error),
        "missing file SHA-256 fails closed");
}

void CheckFrozenBaseline(const std::filesystem::path& root,
                         const TemporaryDirectory& temporary,
                         TestContext* test) {
    std::string error;
    const std::filesystem::path approved =
        root / "configs/vendor_baseline.json";
    test->Expect(
        baseline::VerifyApprovedBaselineFile(approved, &error),
        "checked-in baseline exactly matches compiled constants: " + error);

    const std::filesystem::path exact =
        temporary.path() / "exact-baseline.json";
    test->Expect(
        WriteText(exact, baseline::ApprovedVendorBaselineJson()),
        "writes exact baseline fixture");
    test->Expect(
        baseline::VerifyApprovedBaselineFile(exact, &error),
        "exact baseline fixture is accepted");

    const std::filesystem::path changed =
        temporary.path() / "changed-baseline.json";
    const std::string changed_contents =
        baseline::ApprovedVendorBaselineJson() + " ";
    test->Expect(WriteText(changed, changed_contents),
                 "writes modified baseline fixture");
    test->Expect(
        !baseline::VerifyApprovedBaselineFile(changed, &error),
        "even whitespace modification to frozen baseline is rejected");
    test->Expect(
        !baseline::VerifyApprovedBaselineFile(
            temporary.path() / "missing-baseline.json", &error),
        "missing baseline is rejected");

    const baseline::VendorBaseline& approved_constants =
        baseline::ApprovedVendorBaseline();
    test->Expect(
        approved_constants.shared_library_sha256 ==
                "09bd58282d6f758bfb737b628f5c51daa591a60f31d4081992679fcbc2e2cfc5" &&
            approved_constants.shared_library_size == 242357680U,
        "the originally approved shared-library hash and size stay frozen");

    constexpr std::array<baseline::NumericConstant, 20>
        kExpectedProtocolConstants = {{
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
    test->Expect(
        approved_constants.protocol_constants.size() ==
            kExpectedProtocolConstants.size(),
        "baseline freezes exactly the runner-relevant protocol constants");
    if (approved_constants.protocol_constants.size() ==
        kExpectedProtocolConstants.size()) {
        for (std::size_t index = 0;
             index < kExpectedProtocolConstants.size();
             ++index) {
            const baseline::NumericConstant& expected =
                kExpectedProtocolConstants[index];
            const baseline::NumericConstant& actual =
                approved_constants.protocol_constants[index];
            test->Expect(
                actual.name == expected.name &&
                    actual.value == expected.value,
                "protocol constant is independently frozen: " +
                    std::string(expected.name));
        }
    }

    struct ExpectedTypeCoverage {
        std::string_view name;
        std::size_t size;
        std::size_t member_count;
    };
    constexpr std::array<ExpectedTypeCoverage, 24>
        kExpectedTypes = {{
            {"MDLMessageHead", 23, 8},
            {"MDLAnsiString", 6, 2},
            {"MDLUTF8String", 6, 2},
            {"MDLList", 8, 2},
            {"LogonResponse", 24, 4},
            {"LogonResponse::ServicesItem", 16, 3},
            {"LogonResponse::ServicesItem::MessagesItem", 8, 2},
            {"SubscribeResponse", 8, 1},
            {"SubscribeResponse::ServicesItem", 16, 3},
            {"SubscribeResponse::ServicesItem::MessagesItem", 8, 2},
            {"SHL2MarketData", 248, 44},
            {"SHL2MarketData::BidLevelsItem", 28, 5},
            {"SHL2MarketData::BidLevelsItem::NOrdersItem", 16, 3},
            {"SHL2MarketData::SellLevelsItem", 28, 5},
            {"SHL2MarketData::SellLevelsItem::NoOrdersItem", 16, 3},
            {"NGTSTick", 70, 11},
            {"Snapshot300111_v2", 224, 30},
            {"Snapshot300111_v2::BidPriceLevelItem", 28, 4},
            {"Snapshot300111_v2::BidPriceLevelItem::OrdersItem", 8, 1},
            {"Snapshot300111_v2::AskPriceLevelItem", 28, 4},
            {"Snapshot300111_v2::AskPriceLevelItem::OrdersItem", 8, 1},
            {"Order300192_v2", 58, 10},
            {"Transaction300191_v2", 70, 11},
            {"CombinedTick", 70, 11},
        }};
    test->Expect(approved_constants.abi_types.size() ==
                     kExpectedTypes.size(),
                 "baseline freezes exactly 24 ABI structures");
    if (approved_constants.abi_types.size() ==
        kExpectedTypes.size()) {
        for (std::size_t index = 0; index < kExpectedTypes.size();
             ++index) {
            const ExpectedTypeCoverage& expected =
                kExpectedTypes[index];
            const baseline::TypeLayout& actual =
                approved_constants.abi_types[index];
            test->Expect(
                actual.name == expected.name &&
                    actual.size == expected.size &&
                    actual.alignment == 1U &&
                    actual.members.size() == expected.member_count,
                "ABI type has complete frozen coverage: " +
                    std::string(expected.name));
            std::set<std::string_view> member_names;
            for (const baseline::MemberLayout& member :
                 actual.members) {
                test->Expect(
                    member_names.insert(member.name).second,
                    "ABI member names are unique in " +
                        std::string(expected.name));
            }
        }
    }

    ExpectMember(
        approved_constants, "MDLList", "Offset", 4, test);
    ExpectMember(
        approved_constants, "LogonResponse", "ReturnCode", 20, test);
    ExpectMember(approved_constants,
                 "LogonResponse::ServicesItem",
                 "Messages",
                 8,
                 test);
    ExpectMember(approved_constants,
                 "LogonResponse::ServicesItem::MessagesItem",
                 "MessageStatus",
                 4,
                 test);
    ExpectMember(
        approved_constants, "SubscribeResponse", "Services", 0, test);
    ExpectMember(approved_constants,
                 "SubscribeResponse::ServicesItem::MessagesItem",
                 "MessageStatus",
                 4,
                 test);
    ExpectMember(
        approved_constants, "SHL2MarketData", "PreCloPrice", 14, test);
    ExpectMember(
        approved_constants, "SHL2MarketData", "SellNum", 224, test);
    ExpectMember(approved_constants,
                 "SHL2MarketData::SellLevelsItem",
                 "NoOrders",
                 20,
                 test);
    ExpectMember(approved_constants,
                 "Snapshot300111_v2",
                 "OptPremiumRatio",
                 200,
                 test);
    ExpectMember(approved_constants,
                 "Snapshot300111_v2::AskPriceLevelItem",
                 "Orders",
                 20,
                 test);
    ExpectMember(
        approved_constants, "Order300192_v2", "Side", 46, test);
    ExpectMember(approved_constants,
                 "Transaction300191_v2",
                 "ExecType",
                 62,
                 test);

    std::size_t required = 0;
    std::size_t optional = 0;
    std::size_t forbidden = 0;
    bool saw_ngts = false;
    bool saw_combined_tick_forbidden = false;
    std::set<std::array<unsigned int, 3>> unique_keys;
    for (const baseline::MessageContract& message :
         approved_constants.messages) {
        const std::array<unsigned int, 3> key = {
            message.key.service_id,
            message.key.service_version,
            message.key.message_id};
        test->Expect(unique_keys.insert(key).second,
                     "baseline message keys are unique");
        switch (message.policy) {
        case baseline::SubscriptionPolicy::Required:
            ++required;
            break;
        case baseline::SubscriptionPolicy::Optional:
            ++optional;
            break;
        case baseline::SubscriptionPolicy::Forbidden:
            ++forbidden;
            break;
        }
        if (message.key == baseline::MessageKey{4, 101, 24}) {
            saw_ngts =
                message.policy ==
                baseline::SubscriptionPolicy::Required;
        }
        if (message.key == baseline::MessageKey{6, 101, 53}) {
            saw_combined_tick_forbidden =
                message.policy ==
                baseline::SubscriptionPolicy::Forbidden;
        }
    }
    test->Expect(required == 5U && optional == 2U && forbidden == 1U,
                 "message manifest freezes 5 required, 2 optional, "
                 "and 1 forbidden key");
    test->Expect(saw_ngts, "Shanghai 4.24 is a required message");
    test->Expect(saw_combined_tick_forbidden,
                 "Shenzhen 6.53 is explicitly forbidden");
}

void CheckElf(const std::filesystem::path& library,
              const TemporaryDirectory& temporary,
              TestContext* test) {
    baseline::ElfMetadata metadata;
    std::string error;
#if !defined(L2FLOW_SANITIZER_BUILD)
    test->Expect(baseline::InspectElfFile(library, &metadata, &error),
                 "approved libmdl_api.so parses as constrained ELF: " +
                     error);
    const baseline::VendorBaseline& approved =
        baseline::ApprovedVendorBaseline();
    test->Expect(metadata.file_size == approved.shared_library_size,
                 "ELF file size matches baseline");
    test->Expect(metadata.elf_class == ELFCLASS64 &&
                     metadata.data_encoding == ELFDATA2LSB &&
                     metadata.object_type == ET_DYN &&
                     metadata.machine == EM_X86_64,
                 "ELF class/data/type/machine match baseline");
    test->Expect(metadata.build_id == approved.elf_build_id,
                 "GNU build ID matches baseline");
    test->Expect(!metadata.soname.has_value(),
                 "approved library has no DT_SONAME");

    std::sort(metadata.needed.begin(), metadata.needed.end());
    std::vector<std::string> expected;
    expected.reserve(approved.elf_needed.size());
    for (const std::string_view name : approved.elf_needed) {
        expected.emplace_back(name);
    }
    std::sort(expected.begin(), expected.end());
    test->Expect(metadata.needed == expected,
                 "DT_NEEDED set matches the exact frozen set");
    test->Expect(
        metadata.compiler_comment_sha256 ==
            approved.compiler_comment_sha256,
        ".comment compiler evidence hash matches baseline");
    test->Expect(
        metadata.compiler_producers.size() ==
            approved.compiler_producers.size() &&
            std::equal(metadata.compiler_producers.begin(),
                       metadata.compiler_producers.end(),
                       approved.compiler_producers.begin()),
        "all compiler producer strings match in recorded order");
    test->Expect(
        metadata.required_symbol_versions.size() ==
            approved.required_symbol_versions.size() &&
            std::equal(metadata.required_symbol_versions.begin(),
                       metadata.required_symbol_versions.end(),
                       approved.required_symbol_versions.begin()),
        "DT_VERNEED symbol-version set matches exactly");
#else
    static_cast<void>(library);
#endif

    const std::filesystem::path bad = temporary.path() / "bad.so";
    test->Expect(WriteText(bad, "not-an-elf"),
                 "writes malformed ELF fixture");
    test->Expect(!baseline::InspectElfFile(bad, &metadata, &error),
                 "malformed ELF is rejected");

    Elf64_Ehdr truncated{};
    std::memcpy(truncated.e_ident, ELFMAG, SELFMAG);
    truncated.e_ident[EI_CLASS] = ELFCLASS64;
    truncated.e_ident[EI_DATA] = ELFDATA2LSB;
    truncated.e_ident[EI_VERSION] = EV_CURRENT;
    truncated.e_version = EV_CURRENT;
    truncated.e_type = ET_DYN;
    truncated.e_machine = EM_X86_64;
    truncated.e_ehsize = sizeof(Elf64_Ehdr);
    truncated.e_phentsize = sizeof(Elf64_Phdr);
    truncated.e_phnum = 1;
    truncated.e_phoff = sizeof(Elf64_Ehdr);
    const std::filesystem::path truncated_path =
        temporary.path() / "truncated-headers.so";
    test->Expect(
        WriteBytes(
            truncated_path,
            std::as_bytes(std::span{&truncated, std::size_t{1}})),
        "writes header-only ELF fixture");
    test->Expect(
        !baseline::InspectElfFile(truncated_path, &metadata, &error),
        "ELF with an out-of-file program header table is rejected");
}

void CheckCompiledAbi(TestContext* test) {
    const std::vector<baseline::CheckResult> checks =
        baseline::CheckCompiledVendorAbi();
    test->Expect(!checks.empty(), "compiled ABI produces checks");
    bool all_passed = true;
    bool saw_ngts_layout = false;
    bool saw_utf8_layout = false;
    bool saw_required_ngts_key = false;
    bool saw_forbidden_combined_key = false;
    std::set<std::string> check_ids;
    for (const baseline::CheckResult& check : checks) {
        test->Expect(check_ids.insert(check.id).second,
                     "compiled ABI check IDs are unique: " + check.id);
        all_passed = all_passed && check.passed;
        saw_ngts_layout =
            saw_ngts_layout ||
            check.id == "abi.NGTSTick.size";
        saw_utf8_layout =
            saw_utf8_layout ||
            check.id == "abi.MDLUTF8String.size";
        saw_required_ngts_key =
            saw_required_ngts_key ||
            check.id ==
                "message.mdl_shl2_msg::NGTSTick.required";
        saw_forbidden_combined_key =
            saw_forbidden_combined_key ||
            check.id ==
                "message.mdl_szl2_msg::CombinedTick.forbidden";
        if (!check.passed) {
            std::cerr << "ABI DIFF: " << check.id << " expected="
                      << check.expected << " actual=" << check.actual
                      << " detail=" << check.detail << '\n';
        }
    }
    test->Expect(all_passed, "all compiled ABI checks match baseline");
    test->Expect(saw_ngts_layout && saw_required_ngts_key,
                 "ABI checks include required Shanghai 4.24");
    test->Expect(saw_utf8_layout,
                 "ABI checks include MDLUTF8String");
    test->Expect(saw_forbidden_combined_key,
                 "ABI checks include forbidden Shenzhen 6.53 key");

    const baseline::VendorBaseline& approved =
        baseline::ApprovedVendorBaseline();
    for (const baseline::NumericConstant& constant :
         approved.protocol_constants) {
        const std::string id =
            "constant." + std::string(constant.name);
        test->Expect(
            check_ids.contains(id),
            "compiled ABI checks protocol constant " +
                std::string(constant.name));
    }
    for (const baseline::TypeLayout& type : approved.abi_types) {
        const std::string prefix =
            "abi." + std::string(type.name);
        test->Expect(
            check_ids.contains(prefix + ".size") &&
                check_ids.contains(prefix + ".align") &&
                check_ids.contains(prefix + ".standard_layout"),
            "compiled ABI checks size/alignment/layout for " +
                std::string(type.name));
        for (const baseline::MemberLayout& member : type.members) {
            const std::string id =
                prefix + "." + std::string(member.name) + ".offset";
            test->Expect(
                check_ids.contains(id),
                "compiled ABI checks member offset " +
                    std::string(type.name) + "::" +
                    std::string(member.name));
        }
    }
}

void CheckRuntimeAndFailureGates(
    const std::filesystem::path& root,
    const std::filesystem::path& library,
    const TemporaryDirectory& temporary,
    TestContext* test) {
#if !defined(L2FLOW_SANITIZER_BUILD)
    {
        const baseline::PreflightPaths approved_paths{
            root / "configs/vendor_baseline.json",
            root / "mdl_sdk_2_13_234.tar.gz",
            library};
        const baseline::PreflightReport runtime =
            baseline::RunVendorPreflight(
                approved_paths);
        if (!runtime.passed()) {
            for (const baseline::CheckResult& check : runtime.checks) {
                if (!check.passed) {
                    std::cerr << "RUNTIME DIFF: " << check.id
                              << " expected=" << check.expected
                              << " actual=" << check.actual
                              << " detail=" << check.detail << '\n';
                }
            }
        }
        test->Expect(
            runtime.runtime_probe_attempted,
            "runtime probe is attempted after the complete artifact gate");
        const baseline::CheckResult* archive =
            FindCheck(
                runtime,
                "artifact.sdk_archive_sha256");
        test->Expect(
            runtime.passed() &&
                archive != nullptr &&
                archive->passed,
            "approved baseline, archive, library, ABI, and runtime "
            "factory gate passes");
        const baseline::CheckResult* wrong =
            FindCheck(runtime, "runtime.create_wrong_version");
        const baseline::CheckResult* current =
            FindCheck(runtime, "runtime.create_current_version");
        test->Expect(wrong != nullptr && wrong->passed,
                     "wrong SDK version returns null");
        test->Expect(
            current != nullptr && current->passed,
            "current SDK version returns an object and releases safely");
    }
#endif

    const std::filesystem::path unapproved_library =
        temporary.path() / "unapproved-libmdl_api.so";
    test->Expect(
        WriteText(
            unapproved_library,
            "not the approved library"),
        "writes wrong-sized unapproved library fixture");
    const baseline::PreflightReport unapproved_report =
        baseline::RunApprovedLibraryRuntimePreflight(
            unapproved_library);
    test->Expect(
        !unapproved_report.passed(),
        "wrong-sized library fails component preflight");
    test->Expect(
        !unapproved_report.runtime_probe_attempted,
        "wrong-sized library is never dlopen'ed");
    const baseline::CheckResult* unapproved_identity =
        FindCheck(
            unapproved_report,
            "artifact.shared_library_sha256");
    test->Expect(
        unapproved_identity != nullptr &&
            !unapproved_identity->passed &&
            unapproved_identity->actual == "<unavailable>",
        "wrong-sized shared-library identity is unavailable "
        "before hashing");

    const std::filesystem::path symlink_library =
        temporary.path() / "symlink-libmdl_api.so";
    std::error_code symlink_error;
    std::filesystem::create_symlink(
        library, symlink_library, symlink_error);
    test->Expect(
        !symlink_error,
        "creates shared-library symlink rejection fixture");
    if (!symlink_error) {
        const baseline::PreflightReport symlink_report =
            baseline::RunApprovedLibraryRuntimePreflight(
                symlink_library);
        test->Expect(
            !symlink_report.passed() &&
                !symlink_report.runtime_probe_attempted,
            "component preflight never hashes or loads through a symlink");
        const baseline::CheckResult* symlink_hash =
            FindCheck(
                symlink_report,
                "artifact.shared_library_sha256");
        test->Expect(
            symlink_hash != nullptr &&
                !symlink_hash->passed &&
                symlink_hash->actual == "<unavailable>",
            "symlink rejection is an explicit stable-open failure");
    }

    const baseline::PreflightReport invalid_fd =
        baseline::RunApprovedLibraryRuntimePreflightForOpenFd(-1);
    test->Expect(
        !invalid_fd.passed() &&
            !invalid_fd.runtime_probe_attempted,
        "invalid pre-opened descriptors fail before runtime loading");

    baseline::PreflightPaths missing_archive_paths{
        root / "configs/vendor_baseline.json",
        temporary.path() / "missing-sdk-archive.tar.gz",
        library};
    const baseline::PreflightReport missing_archive =
        baseline::RunVendorPreflight(missing_archive_paths);
    test->Expect(!missing_archive.passed(),
                 "missing SDK archive fails full startup preflight");
    test->Expect(!missing_archive.runtime_probe_attempted,
                 "missing SDK archive prevents dlopen");
    const baseline::CheckResult* missing_archive_hash =
        FindCheck(missing_archive, "artifact.sdk_archive_sha256");
    test->Expect(
        missing_archive_hash != nullptr &&
            !missing_archive_hash->passed &&
            missing_archive_hash->actual == "<unavailable>",
        "missing archive is an explicit hash-check failure");

    const std::filesystem::path oversized_archive =
        temporary.path() / "oversized-sdk-archive.tar.gz";
    const int oversized_fd =
        ::open(
            oversized_archive.c_str(),
            O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC,
            S_IRUSR | S_IWUSR);
    const off_t oversized_length =
        static_cast<off_t>(
            1ULL * 1024ULL * 1024ULL * 1024ULL + 1ULL);
    const bool wrote_sparse_archive =
        oversized_fd >= 0 &&
        ::ftruncate(oversized_fd, oversized_length) == 0;
    if (oversized_fd >= 0) {
        static_cast<void>(::close(oversized_fd));
    }
    test->Expect(
        wrote_sparse_archive,
        "creates sparse over-bound SDK archive fixture");
    if (wrote_sparse_archive) {
        baseline::PreflightPaths oversized_archive_paths{
            root / "configs/vendor_baseline.json",
            oversized_archive,
            library};
        const baseline::PreflightReport oversized_report =
            baseline::RunVendorPreflight(
                oversized_archive_paths);
        const baseline::CheckResult* oversized_hash =
            FindCheck(
                oversized_report,
                "artifact.sdk_archive_sha256");
        test->Expect(
            oversized_hash != nullptr &&
                !oversized_hash->passed &&
                oversized_hash->actual == "<unavailable>" &&
                oversized_hash->detail.find("size bound") !=
                    std::string::npos &&
                !oversized_report.runtime_probe_attempted,
            "over-one-GiB archive is rejected before hashing or dlopen");
    }

    const std::filesystem::path bad_archive =
        temporary.path() / "bad-sdk-archive.tar.gz";
    test->Expect(WriteText(bad_archive, "wrong archive"),
                 "writes wrong-hash archive fixture");
    baseline::PreflightPaths bad_archive_paths{
        root / "configs/vendor_baseline.json", bad_archive, library};
    const baseline::PreflightReport wrong_archive =
        baseline::RunVendorPreflight(bad_archive_paths);
    test->Expect(!wrong_archive.passed(),
                 "wrong-hash SDK archive fails full startup preflight");
    test->Expect(!wrong_archive.runtime_probe_attempted,
                 "wrong-hash SDK archive prevents dlopen");
    const baseline::CheckResult* wrong_archive_hash =
        FindCheck(wrong_archive, "artifact.sdk_archive_sha256");
    test->Expect(
        wrong_archive_hash != nullptr &&
            !wrong_archive_hash->passed &&
            wrong_archive_hash->actual != "<unavailable>",
        "wrong archive content produces an explicit digest mismatch");

    const std::string diff_json =
        baseline::PreflightReportJson(wrong_archive, true);
    test->Expect(diff_json.find("\"differences\"") != std::string::npos,
                 "diff JSON uses the differences field");
    test->Expect(
        diff_json.find("artifact.sdk_archive_sha256") !=
            std::string::npos,
        "diff JSON contains the failed archive check");
    test->Expect(
        diff_json.find("\"passed\": true") == std::string::npos,
        "diff-only JSON omits passing checks");

    baseline::PreflightReport escape_report;
    escape_report.mode = "quote\"slash\\newline\n";
    escape_report.checks.push_back(
        {"escape", false, "\t", "\r", "control\ncharacters"});
    const std::string escaped =
        baseline::PreflightReportJson(escape_report);
    test->Expect(
        escaped.find("quote\\\"slash\\\\newline\\n") !=
            std::string::npos &&
            escaped.find("control\\ncharacters") !=
                std::string::npos,
        "JSON report escapes quotes, slashes, and control characters");
}

}  // namespace

int main() {
    TestContext test;
    const std::optional<std::filesystem::path> root =
        FindRepositoryRoot();
    test.Expect(root.has_value(), "locates the L2Flow repository root");
    if (!root.has_value()) {
        return 1;
    }

    TemporaryDirectory temporary;
    const std::filesystem::path library =
        *root / "mdl_sdk_2_13_234/libs/linux/libmdl_api.so";
    CheckSha256(temporary, &test);
    CheckFrozenBaseline(*root, temporary, &test);
    CheckElf(library, temporary, &test);
    CheckCompiledAbi(&test);
    CheckRuntimeAndFailureGates(
        *root, library, temporary, &test);

    if (test.failures() != 0) {
        std::cerr << "Phase 0 baseline test failed with "
                  << test.failures() << " failure(s)\n";
        return 1;
    }
    std::cout
        << "Phase 0 baseline checks passed: SHA-256 vectors, immutable "
           "baseline, constrained ELF, ABI/message contracts, and "
           "fail-closed artifact gates";
#if defined(L2FLOW_SANITIZER_BUILD)
    std::cout << " (vendor runtime probe intentionally omitted)";
#else
    std::cout << ", including the safe SDK runtime factory probe";
#endif
    std::cout << '\n';
    return 0;
}
