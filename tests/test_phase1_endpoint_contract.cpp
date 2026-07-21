#include "l2flow/common/sha256.h"
#include "l2flow/sdk/endpoint_contract.h"

#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace common = l2flow::common;
namespace sdk = l2flow::sdk;

namespace {

struct TestContext {
    void Expect(bool condition, const std::string& description) {
        if (condition) {
            return;
        }
        ++failures;
        std::cerr << "FAIL: " << description << '\n';
    }

    int failures = 0;
};

class ScopedFd {
public:
    explicit ScopedFd(int fd = -1) noexcept : fd_(fd) {}
    ~ScopedFd() {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
    }

    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;

    [[nodiscard]] int get() const noexcept { return fd_; }

private:
    int fd_;
};

class TempDirectory {
public:
    TempDirectory() {
        char pattern[] = "/tmp/l2flow-endpoint-contract-XXXXXX";
        const char* created = ::mkdtemp(pattern);
        if (created == nullptr) {
            throw std::runtime_error(
                std::string("mkdtemp failed: ") + std::strerror(errno));
        }
        path_ = created;
    }

    ~TempDirectory() {
        std::error_code ignored;
        static_cast<void>(std::filesystem::remove_all(path_, ignored));
    }

    TempDirectory(const TempDirectory&) = delete;
    TempDirectory& operator=(const TempDirectory&) = delete;

    [[nodiscard]] std::string Child(std::string_view name) const {
        return path_ + "/" + std::string(name);
    }

private:
    std::string path_;
};

void ThrowErrno(const char* operation) {
    throw std::runtime_error(
        std::string(operation) + " failed: " + std::strerror(errno));
}

void WriteAll(int fd, std::string_view contents) {
    std::size_t offset = 0U;
    while (offset < contents.size()) {
        const ssize_t count = ::write(
            fd, contents.data() + offset, contents.size() - offset);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count < 0) {
            ThrowErrno("write fixture");
        }
        if (count == 0) {
            throw std::runtime_error("write fixture made no progress");
        }
        offset += static_cast<std::size_t>(count);
    }
}

void CreateFile(const std::string& path, std::string_view contents) {
    const int raw_fd = ::open(
        path.c_str(),
        O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC,
        0600);
    if (raw_fd < 0) {
        ThrowErrno("open fixture");
    }
    ScopedFd fd(raw_fd);
    WriteAll(fd.get(), contents);
}

std::string Sha256(std::string_view bytes) {
    return common::Sha256Hex(common::ComputeSha256(bytes));
}

const std::string& ValidJson() {
    static const std::string value =
        "{\"schema_version\":1,"
        "\"ingress_kind\":\"sh-snapshot\","
        "\"name\":\"sh_snapshot_prod\","
        "\"resolved_server_address\":\"tcp://127.0.0.1:9001\","
        "\"message_encoding\":7,"
        "\"merge_message\":true,"
        "\"send_mac_auth\":false,"
        "\"server_select\":false}";
    return value;
}

sdk::EndpointContract SentinelContract() {
    sdk::EndpointContract value;
    value.name = "sentinel-name";
    value.resolved_server_address = "sentinel-address";
    value.contract_sha256 = std::string(64U, 'f');
    value.message_encoding = datayes::mdl::MDLEID_BINARY;
    value.merge_message = false;
    value.send_mac_auth = true;
    value.server_select = true;
    return value;
}

bool SameContract(const sdk::EndpointContract& left,
                  const sdk::EndpointContract& right) {
    return left.name == right.name &&
           left.resolved_server_address ==
               right.resolved_server_address &&
           left.contract_sha256 == right.contract_sha256 &&
           left.message_encoding == right.message_encoding &&
           left.merge_message == right.merge_message &&
           left.send_mac_auth == right.send_mac_auth &&
           left.server_select == right.server_select;
}

std::string ReplaceOnce(std::string input,
                        std::string_view needle,
                        std::string_view replacement) {
    const std::size_t position = input.find(needle);
    if (position == std::string::npos) {
        throw std::runtime_error("test replacement needle not found");
    }
    input.replace(position, needle.size(), replacement);
    return input;
}

std::size_t rejected_file_index = 0U;

void ExpectRejectedContents(TestContext* test,
                            const TempDirectory& temporary,
                            std::string_view label,
                            const std::string& contents,
                            sdk::IngressKind expected_kind =
                                sdk::IngressKind::ShSnapshot) {
    const std::string path =
        temporary.Child("bad-" + std::to_string(rejected_file_index++));
    CreateFile(path, contents);

    const sdk::EndpointContract sentinel = SentinelContract();
    sdk::EndpointContract output = sentinel;
    std::string error = "stale error";
    const bool loaded = sdk::LoadEndpointContractFile(
        path, Sha256(contents), expected_kind, &output, &error);
    test->Expect(!loaded, std::string(label) + " is rejected");
    test->Expect(!error.empty(),
                 std::string(label) + " returns a diagnostic");
    test->Expect(
        SameContract(output, sentinel),
        std::string(label) + " leaves the output contract unchanged");
}

void CheckValidContract(TestContext* test,
                        const TempDirectory& temporary) {
    const std::string path = temporary.Child("valid");
    CreateFile(path, ValidJson());
    const std::string digest = Sha256(ValidJson());

    sdk::EndpointContract contract = SentinelContract();
    std::string error = "stale";
    test->Expect(
        sdk::LoadEndpointContractFile(
            path,
            digest,
            sdk::IngressKind::ShSnapshot,
            &contract,
            &error),
        "valid endpoint contract loads");
    test->Expect(error.empty(), "valid endpoint contract clears stale error");
    test->Expect(contract.name == "sh_snapshot_prod",
                 "contract name is parsed exactly");
    test->Expect(
        contract.resolved_server_address == "tcp://127.0.0.1:9001",
        "resolved server address is parsed exactly");
    test->Expect(contract.contract_sha256 == digest,
                 "contract stores the exact raw-file SHA-256");
    test->Expect(
        contract.message_encoding == datayes::mdl::MDLEID_MKTPRO,
        "message encoding is parsed as the approved vendor enum");
    test->Expect(contract.merge_message,
                 "merge_message true is parsed");
    test->Expect(
        contract.send_mac_auth.has_value() &&
            !*contract.send_mac_auth,
        "send_mac_auth false remains explicit");
    test->Expect(!contract.server_select,
                 "server_select false is parsed");

    const std::string reordered =
        " \n{\n"
        " \"server_select\": true,\n"
        " \"send_mac_auth\": true,\n"
        " \"merge_message\": false,\n"
        " \"message_encoding\": 132,\n"
        " \"resolved_server_address\": \"host:1\",\n"
        " \"name\": \"sz_tick_prod\",\n"
        " \"ingress_kind\": \"mdl-ingress-sz-tick\",\n"
        " \"schema_version\": 1\n"
        "}\r\n";
    const std::string reordered_path = temporary.Child("reordered");
    CreateFile(reordered_path, reordered);
    test->Expect(
        sdk::LoadEndpointContractFile(
            reordered_path,
            Sha256(reordered),
            sdk::IngressKind::SzTick,
            &contract,
            &error),
        "field order and JSON whitespace do not change the contract");
    test->Expect(
        contract.message_encoding ==
            datayes::mdl::MDLEID_DEFLATE_PROTOBUF &&
            !contract.merge_message &&
            contract.send_mac_auth.has_value() &&
            *contract.send_mac_auth &&
            contract.server_select,
        "reordered contract preserves all booleans and encoding");

    const std::string utf8_name =
        std::string("endpoint-") +
        static_cast<char>(0xe7) +
        static_cast<char>(0xab) +
        static_cast<char>(0xaf) +
        static_cast<char>(0xe7) +
        static_cast<char>(0x82) +
        static_cast<char>(0xb9);
    const std::string utf8_json = ReplaceOnce(
        ValidJson(),
        "\"name\":\"sh_snapshot_prod\"",
        "\"name\":\"" + utf8_name + "\"");
    const std::string utf8_path = temporary.Child("utf8");
    CreateFile(utf8_path, utf8_json);
    test->Expect(
        sdk::LoadEndpointContractFile(
            utf8_path,
            Sha256(utf8_json),
            sdk::IngressKind::ShSnapshot,
            &contract,
            &error) &&
            contract.name == utf8_name,
        "well-formed unescaped UTF-8 string bytes are preserved");
}

void CheckAllApprovedEncodings(TestContext* test,
                               const TempDirectory& temporary) {
    struct EncodingCase {
        std::uint64_t number;
        datayes::mdl::MDLMessageEncoding expected;
    };
    const std::array<EncodingCase, 10> cases = {{
        {1U, datayes::mdl::MDLEID_BINARY},
        {2U, datayes::mdl::MDLEID_FAST},
        {3U, datayes::mdl::MDLEID_JSON},
        {4U, datayes::mdl::MDLEID_PROTOBUF},
        {5U, datayes::mdl::MDLEID_CSV},
        {6U, datayes::mdl::MDLEID_MKTDATA},
        {7U, datayes::mdl::MDLEID_MKTPRO},
        {64U, datayes::mdl::MDLEID_PACKAGE},
        {128U, datayes::mdl::MDLEID_DEFLATE},
        {132U, datayes::mdl::MDLEID_DEFLATE_PROTOBUF},
    }};
    for (std::size_t index = 0U; index < cases.size(); ++index) {
        const EncodingCase& item = cases[index];
        const std::string json = ReplaceOnce(
            ValidJson(),
            "\"message_encoding\":7",
            "\"message_encoding\":" + std::to_string(item.number));
        const std::string path =
            temporary.Child("encoding-" + std::to_string(index));
        CreateFile(path, json);
        sdk::EndpointContract contract;
        std::string error;
        test->Expect(
            sdk::LoadEndpointContractFile(
                path,
                Sha256(json),
                sdk::IngressKind::ShSnapshot,
                &contract,
                &error) &&
                contract.message_encoding == item.expected,
            "approved message encoding " +
                std::to_string(item.number) + " is accepted exactly");
    }
}

void CheckSchemaShapeAndTypes(TestContext* test,
                              const TempDirectory& temporary) {
    std::vector<std::pair<std::string, std::string>> invalid = {
        {
            "unknown field",
            ReplaceOnce(
                ValidJson(), "\"schema_version\":1",
                "\"unknown\":0,\"schema_version\":1"),
        },
        {
            "duplicate field",
            ReplaceOnce(
                ValidJson(), "\"schema_version\":1",
                "\"schema_version\":1,\"schema_version\":1"),
        },
        {
            "missing schema_version",
            ReplaceOnce(ValidJson(), "\"schema_version\":1,", ""),
        },
        {
            "missing ingress_kind",
            ReplaceOnce(
                ValidJson(), "\"ingress_kind\":\"sh-snapshot\",", ""),
        },
        {
            "missing name",
            ReplaceOnce(
                ValidJson(), "\"name\":\"sh_snapshot_prod\",", ""),
        },
        {
            "missing resolved address",
            ReplaceOnce(
                ValidJson(),
                "\"resolved_server_address\":\"tcp://127.0.0.1:9001\",",
                ""),
        },
        {
            "missing message_encoding",
            ReplaceOnce(ValidJson(), "\"message_encoding\":7,", ""),
        },
        {
            "missing merge_message",
            ReplaceOnce(ValidJson(), "\"merge_message\":true,", ""),
        },
        {
            "missing send_mac_auth",
            ReplaceOnce(ValidJson(), "\"send_mac_auth\":false,", ""),
        },
        {
            "missing server_select",
            ReplaceOnce(ValidJson(), ",\"server_select\":false", ""),
        },
        {
            "schema_version string",
            ReplaceOnce(
                ValidJson(), "\"schema_version\":1",
                "\"schema_version\":\"1\""),
        },
        {
            "ingress_kind boolean",
            ReplaceOnce(
                ValidJson(), "\"ingress_kind\":\"sh-snapshot\"",
                "\"ingress_kind\":true"),
        },
        {
            "name number",
            ReplaceOnce(
                ValidJson(), "\"name\":\"sh_snapshot_prod\"",
                "\"name\":1"),
        },
        {
            "resolved address null",
            ReplaceOnce(
                ValidJson(),
                "\"resolved_server_address\":\"tcp://127.0.0.1:9001\"",
                "\"resolved_server_address\":null"),
        },
        {
            "message_encoding string",
            ReplaceOnce(
                ValidJson(), "\"message_encoding\":7",
                "\"message_encoding\":\"7\""),
        },
        {
            "merge_message number",
            ReplaceOnce(
                ValidJson(), "\"merge_message\":true",
                "\"merge_message\":1"),
        },
        {
            "send_mac_auth string",
            ReplaceOnce(
                ValidJson(), "\"send_mac_auth\":false",
                "\"send_mac_auth\":\"false\""),
        },
        {
            "server_select null",
            ReplaceOnce(
                ValidJson(), "\"server_select\":false",
                "\"server_select\":null"),
        },
        {
            "unsupported schema",
            ReplaceOnce(
                ValidJson(), "\"schema_version\":1",
                "\"schema_version\":2"),
        },
        {
            "schema leading zero",
            ReplaceOnce(
                ValidJson(), "\"schema_version\":1",
                "\"schema_version\":01"),
        },
        {
            "negative encoding",
            ReplaceOnce(
                ValidJson(), "\"message_encoding\":7",
                "\"message_encoding\":-1"),
        },
        {
            "floating encoding",
            ReplaceOnce(
                ValidJson(), "\"message_encoding\":7",
                "\"message_encoding\":7.0"),
        },
        {
            "overflow encoding",
            ReplaceOnce(
                ValidJson(), "\"message_encoding\":7",
                "\"message_encoding\":18446744073709551616"),
        },
        {
            "undefined encoding",
            ReplaceOnce(
                ValidJson(), "\"message_encoding\":7",
                "\"message_encoding\":0"),
        },
        {
            "unapproved encoding",
            ReplaceOnce(
                ValidJson(), "\"message_encoding\":7",
                "\"message_encoding\":8"),
        },
        {
            "empty name",
            ReplaceOnce(
                ValidJson(), "\"name\":\"sh_snapshot_prod\"",
                "\"name\":\"\""),
        },
        {
            "empty address",
            ReplaceOnce(
                ValidJson(),
                "\"resolved_server_address\":\"tcp://127.0.0.1:9001\"",
                "\"resolved_server_address\":\"\""),
        },
        {
            "unknown ingress kind",
            ReplaceOnce(
                ValidJson(), "\"ingress_kind\":\"sh-snapshot\"",
                "\"ingress_kind\":\"unknown\""),
        },
        {"empty object", "{}"},
        {"array root", "[]"},
    };

    for (const auto& item : invalid) {
        ExpectRejectedContents(
            test, temporary, item.first, item.second);
    }

    ExpectRejectedContents(
        test,
        temporary,
        "ingress kind mismatch",
        ValidJson(),
        sdk::IngressKind::ShTick);
}

void CheckStrictStringsAndTail(TestContext* test,
                               const TempDirectory& temporary) {
    const std::vector<std::pair<std::string, std::string>> invalid = {
        {
            "escaped value",
            ReplaceOnce(
                ValidJson(), "\"name\":\"sh_snapshot_prod\"",
                "\"name\":\"sh\\u005fsnapshot\""),
        },
        {
            "escaped key",
            ReplaceOnce(
                ValidJson(), "\"name\":\"sh_snapshot_prod\"",
                "\"na\\u006de\":\"sh_snapshot_prod\""),
        },
        {
            "raw newline in string",
            ReplaceOnce(
                ValidJson(), "\"name\":\"sh_snapshot_prod\"",
                "\"name\":\"sh\nsnapshot\""),
        },
        {
            "raw DEL in string",
            ReplaceOnce(
                ValidJson(), "\"name\":\"sh_snapshot_prod\"",
                std::string("\"name\":\"sh") +
                    static_cast<char>(0x7f) + "snapshot\""),
        },
        {
            "UTF-8 C1 control in string",
            ReplaceOnce(
                ValidJson(), "\"name\":\"sh_snapshot_prod\"",
                std::string("\"name\":\"sh") +
                    static_cast<char>(0xc2) +
                    static_cast<char>(0x80) + "snapshot\""),
        },
        {
            "invalid UTF-8 continuation",
            ReplaceOnce(
                ValidJson(), "\"name\":\"sh_snapshot_prod\"",
                std::string("\"name\":\"sh") +
                    static_cast<char>(0x80) + "snapshot\""),
        },
        {
            "overlong UTF-8 encoding",
            ReplaceOnce(
                ValidJson(), "\"name\":\"sh_snapshot_prod\"",
                std::string("\"name\":\"sh") +
                    static_cast<char>(0xc0) +
                    static_cast<char>(0xaf) + "snapshot\""),
        },
        {
            "UTF-8 surrogate",
            ReplaceOnce(
                ValidJson(), "\"name\":\"sh_snapshot_prod\"",
                std::string("\"name\":\"sh") +
                    static_cast<char>(0xed) +
                    static_cast<char>(0xa0) +
                    static_cast<char>(0x80) + "snapshot\""),
        },
        {
            "non-whitespace control outside string",
            std::string(1U, static_cast<char>(0x01)) + ValidJson(),
        },
        {"trailing object", ValidJson() + "{}"},
        {"trailing scalar", ValidJson() + " true"},
        {
            "trailing comma",
            ReplaceOnce(
                ValidJson(), ",\"server_select\":false}",
                ",\"server_select\":false,}"),
        },
        {
            "missing comma",
            ReplaceOnce(
                ValidJson(),
                ",\"name\":\"sh_snapshot_prod\"",
                "\"name\":\"sh_snapshot_prod\""),
        },
        {"unterminated object", ValidJson().substr(0U, ValidJson().size() - 1U)},
    };
    for (const auto& item : invalid) {
        ExpectRejectedContents(
            test, temporary, item.first, item.second);
    }

    const std::string sensitive_marker = "sensitive-endpoint-marker";
    const std::string sensitive_json = ReplaceOnce(
        ValidJson(),
        "\"name\":\"sh_snapshot_prod\"",
        "\"name\":\"" + sensitive_marker + "\\x\"");
    const std::string sensitive_path = temporary.Child("sensitive-error");
    CreateFile(sensitive_path, sensitive_json);
    sdk::EndpointContract output = SentinelContract();
    std::string error;
    test->Expect(
        !sdk::LoadEndpointContractFile(
            sensitive_path,
            Sha256(sensitive_json),
            sdk::IngressKind::ShSnapshot,
            &output,
            &error) &&
            error.find(sensitive_marker) == std::string::npos &&
            error.find(sensitive_path) == std::string::npos,
        "parse diagnostic exposes neither path nor contract contents");

    std::string with_nul = ReplaceOnce(
        ValidJson(), "\"name\":\"sh_snapshot_prod\"",
        "\"name\":\"sh_snapshot_prod\"");
    const std::size_t name_position =
        with_nul.find("sh_snapshot_prod");
    with_nul.insert(name_position + 2U, 1U, '\0');
    ExpectRejectedContents(
        test, temporary, "NUL in string", with_nul);
}

void CheckHashContract(TestContext* test,
                       const TempDirectory& temporary) {
    const std::string path = temporary.Child("hash-contract");
    CreateFile(path, ValidJson());
    const std::string digest = Sha256(ValidJson());
    sdk::EndpointContract sentinel = SentinelContract();
    std::string error;

    const std::array<std::string, 4> invalid_hashes = {{
        std::string(63U, 'a'),
        std::string(65U, 'a'),
        std::string(64U, 'A'),
        std::string(63U, 'a') + "g",
    }};
    for (const std::string& hash : invalid_hashes) {
        sdk::EndpointContract output = sentinel;
        test->Expect(
            !sdk::LoadEndpointContractFile(
                path,
                hash,
                sdk::IngressKind::ShSnapshot,
                &output,
                &error),
            "invalid expected SHA-256 format is rejected");
        test->Expect(
            SameContract(output, sentinel),
            "invalid expected SHA-256 leaves output unchanged");
    }

    sdk::EndpointContract output = sentinel;
    std::string mismatch = digest;
    mismatch[0] = mismatch[0] == '0' ? '1' : '0';
    test->Expect(
        !sdk::LoadEndpointContractFile(
            path,
            mismatch,
            sdk::IngressKind::ShSnapshot,
            &output,
            &error),
        "well-formed but mismatched SHA-256 is rejected");
    test->Expect(
        SameContract(output, sentinel),
        "SHA-256 mismatch leaves output unchanged");
    test->Expect(
        error.find(path) == std::string::npos &&
            error.find("sh_snapshot_prod") == std::string::npos &&
            error.find("tcp://127.0.0.1:9001") ==
                std::string::npos,
        "hash diagnostic exposes neither path nor contract contents");

    const auto unknown_kind = static_cast<sdk::IngressKind>(
        std::numeric_limits<std::uint8_t>::max());
    test->Expect(
        !sdk::LoadEndpointContractFile(
            path, digest, unknown_kind, &output, &error),
        "unknown expected ingress kind is rejected");
    test->Expect(
        !sdk::LoadEndpointContractFile(
            path,
            digest,
            sdk::IngressKind::ShSnapshot,
            nullptr,
            &error),
        "null endpoint contract output is rejected");

    sdk::EndpointContract without_error;
    test->Expect(
        sdk::LoadEndpointContractFile(
            path,
            digest,
            sdk::IngressKind::ShSnapshot,
            &without_error,
            nullptr),
        "null diagnostic output is accepted on success");
    test->Expect(
        !sdk::LoadEndpointContractFile(
            path,
            std::string(64U, '0'),
            sdk::IngressKind::ShSnapshot,
            &without_error,
            nullptr),
        "null diagnostic output is accepted on failure");
}

bool RenameRetry(const std::string& from,
                 const std::string& to) noexcept {
    int result = -1;
    do {
        result = ::rename(from.c_str(), to.c_str());
    } while (result != 0 && errno == EINTR);
    return result == 0;
}

void CheckPathReplacementRace(TestContext* test,
                              const TempDirectory& temporary) {
    const std::string live = temporary.Child("race-live");
    const std::string other = temporary.Child("race-other");
    const std::string transit = temporary.Child("race-transit");
    CreateFile(live, ValidJson());
    const std::string unapproved = ReplaceOnce(
        ValidJson(),
        "\"name\":\"sh_snapshot_prod\"",
        "\"name\":\"do-not-leak-marker\"");
    CreateFile(other, unapproved);

    std::atomic<bool> rename_failed{false};
    std::jthread writer([&](std::stop_token stop) {
        while (!stop.stop_requested()) {
            if (!RenameRetry(live, transit) ||
                !RenameRetry(other, live) ||
                !RenameRetry(transit, other)) {
                rename_failed.store(true, std::memory_order_relaxed);
                return;
            }
            std::this_thread::yield();
        }
    });

    const std::string approved_sha = Sha256(ValidJson());
    const sdk::EndpointContract sentinel = SentinelContract();
    for (std::size_t iteration = 0U; iteration < 2000U; ++iteration) {
        sdk::EndpointContract output = sentinel;
        std::string error;
        const bool loaded = sdk::LoadEndpointContractFile(
            live,
            approved_sha,
            sdk::IngressKind::ShSnapshot,
            &output,
            &error);
        if (loaded) {
            test->Expect(
                output.name == "sh_snapshot_prod" &&
                    output.resolved_server_address ==
                        "tcp://127.0.0.1:9001" &&
                    output.contract_sha256 == approved_sha,
                "path replacement race can only accept the complete "
                "approved inode");
        } else {
            test->Expect(
                SameContract(output, sentinel),
                "path replacement race failure leaves output unchanged");
            test->Expect(
                error.find("do-not-leak-marker") ==
                        std::string::npos &&
                    error.find(live) == std::string::npos,
                "path replacement race diagnostic does not expose "
                "contents or path");
        }
    }
    writer.request_stop();
    writer.join();
    test->Expect(
        !rename_failed.load(std::memory_order_relaxed),
        "path replacement fixture completed every atomic rename");
}

void CheckFileSafetyAndBounds(TestContext* test,
                              const TempDirectory& temporary) {
    const std::string target = temporary.Child("symlink-target");
    const std::string symlink = temporary.Child("symlink");
    CreateFile(target, ValidJson());
    if (::symlink(target.c_str(), symlink.c_str()) != 0) {
        ThrowErrno("create symlink fixture");
    }

    sdk::EndpointContract sentinel = SentinelContract();
    sdk::EndpointContract output = sentinel;
    std::string error;
    test->Expect(
        !sdk::LoadEndpointContractFile(
            symlink,
            Sha256(ValidJson()),
            sdk::IngressKind::ShSnapshot,
            &output,
            &error),
        "final-component symlink is rejected");
    test->Expect(
        SameContract(output, sentinel),
        "symlink rejection leaves output unchanged");

    const std::string fifo = temporary.Child("fifo");
    if (::mkfifo(fifo.c_str(), 0600) != 0) {
        ThrowErrno("create FIFO fixture");
    }
    test->Expect(
        !sdk::LoadEndpointContractFile(
            fifo,
            Sha256(ValidJson()),
            sdk::IngressKind::ShSnapshot,
            &output,
            &error) &&
            error.find("regular") != std::string::npos,
        "FIFO is rejected without blocking");

    test->Expect(
        !sdk::LoadEndpointContractFile(
            temporary.Child("missing"),
            Sha256(ValidJson()),
            sdk::IngressKind::ShSnapshot,
            &output,
            &error),
        "missing endpoint contract is rejected");
    test->Expect(
        !sdk::LoadEndpointContractFile(
            temporary.Child("."),
            Sha256(ValidJson()),
            sdk::IngressKind::ShSnapshot,
            &output,
            &error),
        "directory endpoint contract is rejected");

    std::string nul_path = target;
    nul_path.push_back('\0');
    nul_path.append("ignored");
    test->Expect(
        !sdk::LoadEndpointContractFile(
            nul_path,
            Sha256(ValidJson()),
            sdk::IngressKind::ShSnapshot,
            &output,
            &error),
        "path containing NUL is rejected instead of being truncated");

    const std::string exact_path = temporary.Child("exact-4096");
    std::string exact = ValidJson();
    exact.append(
        sdk::kMaximumEndpointContractBytes - exact.size(), ' ');
    CreateFile(exact_path, exact);
    test->Expect(
        sdk::LoadEndpointContractFile(
            exact_path,
            Sha256(exact),
            sdk::IngressKind::ShSnapshot,
            &output,
            &error),
        "endpoint contract exactly at 4096-byte limit is accepted");

    const std::string oversized_path = temporary.Child("oversized");
    std::string oversized(
        sdk::kMaximumEndpointContractBytes + 1U, ' ');
    CreateFile(oversized_path, oversized);
    test->Expect(
        !sdk::LoadEndpointContractFile(
            oversized_path,
            Sha256(oversized),
            sdk::IngressKind::ShSnapshot,
            &output,
            &error) &&
            error.find("4096") != std::string::npos,
        "endpoint contract above 4096-byte limit is rejected before parse");

    const std::string empty_path = temporary.Child("empty");
    CreateFile(empty_path, "");
    test->Expect(
        !sdk::LoadEndpointContractFile(
            empty_path,
            Sha256(""),
            sdk::IngressKind::ShSnapshot,
            &output,
            &error),
        "empty endpoint contract is rejected");

    // procfs exposes this regular pseudo-file with st_size == 0 but supplies
    // bytes when read. It deterministically exercises the no-growth check
    // without a racy writer thread.
    struct stat proc_status {};
    if (::stat("/proc/self/status", &proc_status) == 0 &&
        S_ISREG(proc_status.st_mode) && proc_status.st_size == 0) {
        test->Expect(
            !sdk::LoadEndpointContractFile(
                "/proc/self/status",
                std::string(64U, '0'),
                sdk::IngressKind::ShSnapshot,
                &output,
                &error) &&
                error.find("grew") != std::string::npos,
            "bytes beyond the fstat size are rejected as file growth");
    }
}

void CheckImmutableVerifiedCapability(TestContext* test) {
    const std::string digest = Sha256(ValidJson());
    std::string error = "stale";
    const auto verified =
        sdk::VerifyEndpointContractBytes(
            ValidJson(),
            digest,
            sdk::IngressKind::ShSnapshot,
            &error);
    test->Expect(
        verified != nullptr && error.empty(),
        "exact endpoint bytes produce an immutable verified capability");
    if (verified == nullptr) {
        return;
    }
    test->Expect(
        verified->ingress_kind() ==
                sdk::IngressKind::ShSnapshot &&
            verified->contract_sha256() == digest &&
            verified->resolved_server_address() ==
                "tcp://127.0.0.1:9001" &&
            verified->message_encoding() ==
                datayes::mdl::MDLEID_MKTPRO &&
            verified->merge_message() &&
            !verified->send_mac_auth() &&
            !verified->server_select(),
        "verified capability exposes only fields parsed from its hashed bytes");

    sdk::EndpointContract mutable_copy =
        verified->CopyValue();
    mutable_copy.resolved_server_address =
        "tcp://foreign.invalid:1";
    mutable_copy.message_encoding =
        datayes::mdl::MDLEID_BINARY;
    test->Expect(
        verified->resolved_server_address() ==
                "tcp://127.0.0.1:9001" &&
            verified->message_encoding() ==
                datayes::mdl::MDLEID_MKTPRO,
        "mutating a Phase-1 compatibility copy cannot alter the verified capability");

    const auto wrong_hash =
        sdk::VerifyEndpointContractBytes(
            ValidJson(),
            std::string(64U, '0'),
            sdk::IngressKind::ShSnapshot,
            &error);
    test->Expect(
        wrong_hash == nullptr && !error.empty(),
        "verified capability cannot be constructed with unrelated hash bytes");

    std::string oversized(
        sdk::kMaximumEndpointContractBytes + 1U, ' ');
    const auto too_large =
        sdk::VerifyEndpointContractBytes(
            oversized,
            Sha256(oversized),
            sdk::IngressKind::ShSnapshot,
            &error);
    test->Expect(
        too_large == nullptr,
        "in-memory endpoint verification enforces the file-size bound");
}

}  // namespace

int main() {
    try {
        TestContext test;
        TempDirectory temporary;
        CheckValidContract(&test, temporary);
        CheckAllApprovedEncodings(&test, temporary);
        CheckSchemaShapeAndTypes(&test, temporary);
        CheckStrictStringsAndTail(&test, temporary);
        CheckHashContract(&test, temporary);
        CheckPathReplacementRace(&test, temporary);
        CheckFileSafetyAndBounds(&test, temporary);
        CheckImmutableVerifiedCapability(&test);

        if (test.failures != 0) {
            std::cerr << test.failures
                      << " endpoint-contract assertion(s) failed\n";
            return 1;
        }
        std::cout
            << "Phase 1 endpoint-contract strict parser checks passed\n";
        return 0;
    } catch (const std::exception& exception) {
        std::cerr << "unexpected test exception: " << exception.what()
                  << '\n';
        return 2;
    }
}
