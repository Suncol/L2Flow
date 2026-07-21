#include "l2flow/ops/credential.h"
#include "l2flow/ops/systemd_notify.h"
#include "l2flow/sdk/subscription_manifest.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

namespace mdl = datayes::mdl;
namespace ops = l2flow::ops;
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

struct LiteralKey {
    std::uint8_t service_id = 0;
    std::uint16_t service_version = 0;
    std::uint16_t message_id = 0;

    friend constexpr bool operator==(const LiteralKey&,
                                     const LiteralKey&) = default;
};

LiteralKey CopyKey(const sdk::MessageKey& key) {
    return {key.service_id, key.service_version, key.message_id};
}

std::vector<LiteralKey> CopyKeys(const std::vector<sdk::MessageKey>& keys) {
    std::vector<LiteralKey> result;
    result.reserve(keys.size());
    for (const sdk::MessageKey& key : keys) {
        result.push_back(CopyKey(key));
    }
    return result;
}

struct ExpectedIngress {
    sdk::IngressKind kind;
    std::string_view service_name;
    std::uint32_t source_stream_id;
    std::uint8_t market_service_id;
    int work_threads;
    int io_threads;
    std::vector<LiteralKey> required;
    std::vector<LiteralKey> optional;
    std::vector<LiteralKey> forbidden;
};

const std::array<ExpectedIngress, 4>& ApprovedIngresses() {
    // These are approved SDK 2.13.234 literals. They deliberately do not use
    // the production KeyOf/type constants, so a vendor-header drift cannot
    // rewrite both the implementation and its test oracle.
    static const std::array<ExpectedIngress, 4> expected = {{
        {
            sdk::IngressKind::ShSnapshot,
            "mdl-ingress-sh-snapshot",
            1001U,
            4U,
            2,
            1,
            {{4U, 101U, 4U}},
            {{4U, 101U, 6U}},
            {},
        },
        {
            sdk::IngressKind::ShTick,
            "mdl-ingress-sh-tick",
            1002U,
            4U,
            4,
            1,
            {{4U, 101U, 24U}},
            {},
            {},
        },
        {
            sdk::IngressKind::SzSnapshot,
            "mdl-ingress-sz-snapshot",
            2001U,
            6U,
            2,
            1,
            {{6U, 101U, 28U}},
            {{6U, 101U, 29U}},
            {{6U, 101U, 53U}},
        },
        {
            sdk::IngressKind::SzTick,
            "mdl-ingress-sz-tick",
            2002U,
            6U,
            4,
            1,
            {
                {6U, 101U, 33U},
                {6U, 101U, 36U},
            },
            {},
            {{6U, 101U, 53U}},
        },
    }};
    return expected;
}

class RecordingSubscriber final : public mdl::Subscriber {
public:
    void AddRef() override {}
    int ReleaseRef() override { return 1; }

    void AddSubscription(std::uint8_t service_id,
                         std::uint16_t service_version,
                         std::uint16_t message_id) override {
        added.push_back({service_id, service_version, message_id});
    }

    void AddSubscriptionByFieldValues(std::uint8_t,
                                      std::uint16_t,
                                      std::uint16_t,
                                      const char*,
                                      const char**,
                                      std::uint32_t) override {
        ++field_filter_adds;
    }

    void DelSubscription(std::uint8_t,
                         std::uint16_t,
                         std::uint16_t) override {}

    void DelSubscriptionByFieldValues(std::uint8_t,
                                      std::uint16_t,
                                      std::uint16_t,
                                      const char*,
                                      const char**,
                                      std::uint32_t) override {}

    void ClearSubscriptions() override {}
    void SetHeartbeatInterval(std::uint32_t) override {}
    std::uint32_t GetHeartbeatInterval() override { return 0U; }
    void SetHeartbeatTimeout(std::uint32_t) override {}
    std::uint32_t GetHeartbeatTimeout() override { return 0U; }
    void SetUserName(const char*) override {}
    const char* GetUserName() override { return ""; }
    void SetPassword(const char*) override {}
    const char* GetPassword() override { return ""; }
    void SetMessageEncoding(mdl::MDLMessageEncoding) override {}
    mdl::MDLMessageEncoding GetMessageEncoding() override {
        return mdl::MDLEID_BINARY;
    }
    void SetServerAddress(const char*) override {}
    const char* GetServerAddress() override { return ""; }
    bool PostRequest(mdl::MDLMessage*) override { return false; }
    bool SendRequest(mdl::MDLMessage*) override { return false; }
    const char* Connect() override { return ""; }
    void SetReadBufferSize(int) override {}
    void SetSendMacAuth(bool) override {}
    void EnableServerSelect(bool) override {}
    void ReSubscribe() override {}
    const char* GetSubscription() override { return "[]"; }
    void EnableMergeMessage(bool) override {}

    std::vector<LiteralKey> added;
    std::size_t field_filter_adds = 0U;
};

void CheckIngressManifests(TestContext* test) {
    test->Expect(sdk::ValidateIngressSpecs().empty(),
                 "built-in ingress manifest validates");

    const std::vector<sdk::IngressSpec>& actual = sdk::AllIngressSpecs();
    const std::array<ExpectedIngress, 4>& expected = ApprovedIngresses();
    test->Expect(actual.size() == expected.size(),
                 "manifest contains exactly four ingress streams");
    if (actual.size() != expected.size()) {
        return;
    }

    for (std::size_t index = 0; index < expected.size(); ++index) {
        const sdk::IngressSpec& observed = actual[index];
        const ExpectedIngress& approved = expected[index];
        const std::string prefix =
            "manifest[" + std::to_string(index) + "] ";

        test->Expect(observed.kind == approved.kind,
                     prefix + "kind and stream order are approved");
        test->Expect(observed.service_name == approved.service_name,
                     prefix + "service name is approved");
        test->Expect(observed.source_stream_id == approved.source_stream_id,
                     prefix + "source stream id is approved");
        test->Expect(observed.market_service_id == approved.market_service_id,
                     prefix + "market service id is approved");
        test->Expect(observed.default_work_threads == approved.work_threads,
                     prefix + "work-thread default is approved");
        test->Expect(observed.default_io_threads == approved.io_threads,
                     prefix + "IO-thread default is approved");
        test->Expect(CopyKeys(observed.required) == approved.required,
                     prefix + "required subscriptions and order are exact");
        test->Expect(CopyKeys(observed.optional) == approved.optional,
                     prefix + "optional subscriptions and order are exact");
        test->Expect(CopyKeys(observed.forbidden) == approved.forbidden,
                     prefix + "forbidden subscriptions are exact");

        const sdk::IngressSpec& looked_up =
            sdk::GetIngressSpec(approved.kind);
        test->Expect(&looked_up == &observed,
                     prefix + "kind lookup returns the canonical spec");
        test->Expect(sdk::ToString(approved.kind) == approved.service_name,
                     prefix + "kind string is stable");
    }

    // The approved order is semantically important: Shenzhen order then
    // transaction must be registered on one Subscriber in that order.
    test->Expect(
        expected[3].required ==
            std::vector<LiteralKey>({
                {6U, 101U, 33U},
                {6U, 101U, 36U},
            }),
        "SZ 6.33 and 6.36 share the one ordered SZ-tick manifest");

    for (const sdk::IngressSpec& spec : actual) {
        const std::vector<LiteralKey> required = CopyKeys(spec.required);
        const std::vector<LiteralKey> optional = CopyKeys(spec.optional);
        const LiteralKey combined_tick{6U, 101U, 53U};
        test->Expect(
            std::find(required.begin(), required.end(), combined_tick) ==
                    required.end() &&
                std::find(optional.begin(), optional.end(), combined_tick) ==
                    optional.end(),
            std::string(spec.service_name) +
                " never enables forbidden SZ 6.53");
    }
}

void CheckSubscriptionOperations(TestContext* test) {
    const std::vector<sdk::IngressSpec>& specs = sdk::AllIngressSpecs();
    const std::array<ExpectedIngress, 4>& expected = ApprovedIngresses();
    if (specs.size() != expected.size()) {
        test->Expect(false,
                     "subscription operation test requires four manifests");
        return;
    }

    for (std::size_t index = 0; index < specs.size(); ++index) {
        const std::string prefix =
            std::string(expected[index].service_name) + " ";

        RecordingSubscriber required_only;
        sdk::AddConfiguredSubscriptions(required_only, specs[index], false);
        test->Expect(required_only.added == expected[index].required,
                     prefix +
                         "default path adds required messages only, in order");
        test->Expect(required_only.field_filter_adds == 0U,
                     prefix + "default path never uses a field filter");

        RecordingSubscriber with_optional;
        sdk::AddConfiguredSubscriptions(with_optional, specs[index], true);
        std::vector<LiteralKey> expected_calls = expected[index].required;
        expected_calls.insert(expected_calls.end(),
                              expected[index].optional.begin(),
                              expected[index].optional.end());
        test->Expect(with_optional.added == expected_calls,
                     prefix +
                         "opt-in path appends optional messages after required");
        test->Expect(with_optional.field_filter_adds == 0U,
                     prefix + "opt-in path never uses a field filter");

        const LiteralKey combined_tick{6U, 101U, 53U};
        test->Expect(
            std::find(with_optional.added.begin(), with_optional.added.end(),
                      combined_tick) == with_optional.added.end(),
            prefix + "subscription calls never include SZ 6.53");
    }
}

void CheckIngressKindParsing(TestContext* test) {
    struct ParseCase {
        std::string_view text;
        sdk::IngressKind expected;
    };
    const std::array<ParseCase, 8> cases = {{
        {"sh-snapshot", sdk::IngressKind::ShSnapshot},
        {"mdl-ingress-sh-snapshot", sdk::IngressKind::ShSnapshot},
        {"sh-tick", sdk::IngressKind::ShTick},
        {"mdl-ingress-sh-tick", sdk::IngressKind::ShTick},
        {"sz-snapshot", sdk::IngressKind::SzSnapshot},
        {"mdl-ingress-sz-snapshot", sdk::IngressKind::SzSnapshot},
        {"sz-tick", sdk::IngressKind::SzTick},
        {"mdl-ingress-sz-tick", sdk::IngressKind::SzTick},
    }};
    for (const ParseCase& item : cases) {
        sdk::IngressKind parsed = sdk::IngressKind::ShSnapshot;
        test->Expect(sdk::ParseIngressKind(item.text, &parsed) &&
                         parsed == item.expected,
                     std::string("parse approved ingress name ") +
                         std::string(item.text));
    }

    sdk::IngressKind unchanged = sdk::IngressKind::SzTick;
    test->Expect(!sdk::ParseIngressKind("unknown", &unchanged) &&
                     unchanged == sdk::IngressKind::SzTick,
                 "unknown ingress text fails without changing the output");
    test->Expect(!sdk::ParseIngressKind("", &unchanged),
                 "empty ingress text is rejected");
    test->Expect(!sdk::ParseIngressKind("sh-snapshot", nullptr),
                 "null ingress parse output is rejected");

    const sdk::IngressKind unknown =
        static_cast<sdk::IngressKind>(std::numeric_limits<std::uint8_t>::max());
    bool lookup_threw = false;
    try {
        static_cast<void>(sdk::GetIngressSpec(unknown));
    } catch (const std::invalid_argument&) {
        lookup_threw = true;
    } catch (...) {
        test->Expect(false,
                     "unknown ingress kind throws std::invalid_argument only");
    }
    test->Expect(lookup_threw,
                 "unknown ingress enum cannot fall back to another stream");

    bool string_threw = false;
    try {
        static_cast<void>(sdk::ToString(unknown));
    } catch (const std::invalid_argument&) {
        string_threw = true;
    } catch (...) {
        test->Expect(false,
                     "unknown kind string throws std::invalid_argument only");
    }
    test->Expect(string_threw, "unknown ingress enum has no string fallback");
}

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
        char pattern[] = "/tmp/l2flow-phase1-ops-XXXXXX";
        const char* created = ::mkdtemp(pattern);
        if (created == nullptr) {
            throw std::runtime_error(std::string("mkdtemp failed: ") +
                                     std::strerror(errno));
        }
        path_ = created;
    }

    ~TempDirectory() {
        std::error_code ignored;
        static_cast<void>(std::filesystem::remove_all(path_, ignored));
    }

    TempDirectory(const TempDirectory&) = delete;
    TempDirectory& operator=(const TempDirectory&) = delete;

    [[nodiscard]] const std::string& path() const noexcept { return path_; }

    [[nodiscard]] std::string Child(std::string_view name) const {
        return path_ + "/" + std::string(name);
    }

private:
    std::string path_;
};

void ThrowErrno(const char* operation) {
    throw std::runtime_error(std::string(operation) + " failed: " +
                             std::strerror(errno));
}

void WriteAll(int fd, std::string_view contents) {
    std::size_t offset = 0U;
    while (offset < contents.size()) {
        const ssize_t count =
            ::write(fd, contents.data() + offset, contents.size() - offset);
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

void CreateFile(const std::string& path,
                std::string_view contents,
                mode_t mode) {
    const int raw_fd =
        ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, mode);
    if (raw_fd < 0) {
        ThrowErrno("open fixture");
    }
    ScopedFd fd(raw_fd);
    if (::fchmod(fd.get(), mode) != 0) {
        ThrowErrno("chmod fixture");
    }
    WriteAll(fd.get(), contents);
}

ops::CredentialPolicy CredentialPolicyForCurrentUser(std::size_t max_bytes) {
    ops::CredentialPolicy policy;
    policy.expected_owner = ::getuid();
    policy.max_bytes = max_bytes;
    policy.require_mode_0400 = true;
    return policy;
}

void CheckCredentialSuccessAndNewlines(TestContext* test,
                                       const TempDirectory& temporary) {
    const std::string plain = temporary.Child("plain");
    const std::string lf = temporary.Child("lf");
    const std::string crlf = temporary.Child("crlf");
    CreateFile(plain, "secret-token", 0400);
    CreateFile(lf, "secret-token\n", 0400);
    CreateFile(crlf, "secret-token\r\n", 0400);

    const ops::CredentialPolicy policy =
        CredentialPolicyForCurrentUser(64U);
    for (const std::string& path : {plain, lf, crlf}) {
        const ops::CredentialResult result =
            ops::ReadCredentialFile(path, policy);
        test->Expect(result.ok(), path + " 0400 credential is accepted");
        test->Expect(result.token == "secret-token",
                     path + " strips at most one LF/CRLF terminator");
        test->Expect(result.error.empty(),
                     path + " successful read has no error");
    }
}

void CheckCredentialOwnershipAndPermissions(
    TestContext* test,
    const TempDirectory& temporary) {
    const std::string secure = temporary.Child("secure");
    const std::string permissive = temporary.Child("permissive");
    const std::string special = temporary.Child("special");
    CreateFile(secure, "owner-secret", 0400);
    CreateFile(permissive, "mode-secret", 0600);
    CreateFile(special, "special-secret", 0400);
    if (::chmod(special.c_str(), 04400) != 0) {
        ThrowErrno("set credential special mode bit");
    }

    ops::CredentialPolicy policy = CredentialPolicyForCurrentUser(64U);
    const uid_t current = ::getuid();
    policy.expected_owner = current == 0U ? 1U : 0U;
    const ops::CredentialResult wrong_owner =
        ops::ReadCredentialFile(secure, policy);
    test->Expect(!wrong_owner.ok() && wrong_owner.token.empty(),
                 "credential owner mismatch is rejected without a token");
    test->Expect(wrong_owner.error.find("owner-secret") == std::string::npos,
                 "owner error never exposes credential contents");

    policy = CredentialPolicyForCurrentUser(64U);
    const ops::CredentialResult wrong_mode =
        ops::ReadCredentialFile(permissive, policy);
    test->Expect(!wrong_mode.ok() && wrong_mode.token.empty(),
                 "credential mode 0600 is rejected when exact 0400 is required");
    test->Expect(wrong_mode.error.find("mode-secret") == std::string::npos,
                 "permission error never exposes credential contents");

    const ops::CredentialResult special_mode =
        ops::ReadCredentialFile(special, policy);
    test->Expect(!special_mode.ok() && special_mode.token.empty(),
                 "credential set-user-ID bit is rejected by exact 0400 policy");
    test->Expect(
        special_mode.error.find("special-secret") == std::string::npos,
        "special-mode error never exposes credential contents");

    policy.require_mode_0400 = false;
    const ops::CredentialResult relaxed =
        ops::ReadCredentialFile(permissive, policy);
    test->Expect(relaxed.ok() && relaxed.token == "mode-secret",
                 "explicit relaxed policy accepts a regular 0600 file");
}

void CheckCredentialSizesAndContents(TestContext* test,
                                     const TempDirectory& temporary) {
    const std::string exact = temporary.Child("exact");
    const std::string newline_exact = temporary.Child("newline-exact");
    const std::string too_large = temporary.Child("too-large");
    const std::string with_nul = temporary.Child("with-nul");
    const std::string empty = temporary.Child("empty");
    const std::string only_newline = temporary.Child("only-newline");

    CreateFile(exact, "12345678", 0400);
    CreateFile(newline_exact, "1234567\n", 0400);
    CreateFile(too_large, "TOO-LONG-SECRET", 0400);
    const std::string nul_token("abc\0def", 7U);
    CreateFile(with_nul, nul_token, 0400);
    CreateFile(empty, "", 0400);
    CreateFile(only_newline, "\n", 0400);

    ops::CredentialPolicy policy = CredentialPolicyForCurrentUser(8U);
    const ops::CredentialResult exact_result =
        ops::ReadCredentialFile(exact, policy);
    test->Expect(exact_result.ok() && exact_result.token == "12345678",
                 "credential exactly at max_bytes is accepted");

    const ops::CredentialResult newline_result =
        ops::ReadCredentialFile(newline_exact, policy);
    test->Expect(newline_result.ok() && newline_result.token == "1234567",
                 "raw file exactly at max_bytes is checked before LF trimming");

    policy.max_bytes = 7U;
    const ops::CredentialResult raw_too_large =
        ops::ReadCredentialFile(newline_exact, policy);
    test->Expect(!raw_too_large.ok() && raw_too_large.token.empty(),
                 "newline does not evade the raw credential size limit");

    policy.max_bytes = 8U;
    const ops::CredentialResult large_result =
        ops::ReadCredentialFile(too_large, policy);
    test->Expect(!large_result.ok() && large_result.token.empty(),
                 "credential one or more bytes over max_bytes is rejected");
    test->Expect(
        large_result.error.find("TOO-LONG-SECRET") == std::string::npos,
        "size error never exposes credential contents");

    policy.max_bytes = 64U;
    const ops::CredentialResult nul_result =
        ops::ReadCredentialFile(with_nul, policy);
    test->Expect(!nul_result.ok() && nul_result.token.empty(),
                 "credential containing an embedded NUL is rejected");

    const ops::CredentialResult empty_result =
        ops::ReadCredentialFile(empty, policy);
    test->Expect(!empty_result.ok() && empty_result.token.empty(),
                 "empty credential is rejected");
    const ops::CredentialResult newline_only_result =
        ops::ReadCredentialFile(only_newline, policy);
    test->Expect(!newline_only_result.ok() &&
                     newline_only_result.token.empty(),
                 "credential empty after newline trimming is rejected");

    policy.max_bytes = 0U;
    test->Expect(!ops::ReadCredentialFile(exact, policy).ok(),
                 "zero credential size limit is invalid");
    policy.max_bytes = std::numeric_limits<std::size_t>::max();
    test->Expect(!ops::ReadCredentialFile(exact, policy).ok(),
                 "credential size limit beyond ssize_t is invalid");
}

void CheckCredentialFileTypes(TestContext* test,
                              const TempDirectory& temporary) {
    const std::string target = temporary.Child("target");
    const std::string link = temporary.Child("link");
    const std::string fifo = temporary.Child("fifo");
    CreateFile(target, "symlink-secret", 0400);
    if (::symlink(target.c_str(), link.c_str()) != 0) {
        ThrowErrno("create credential symlink");
    }

    const ops::CredentialPolicy policy =
        CredentialPolicyForCurrentUser(64U);
    const ops::CredentialResult symlink_result =
        ops::ReadCredentialFile(link, policy);
    test->Expect(!symlink_result.ok() && symlink_result.token.empty(),
                 "credential symlink is rejected by the opened-file policy");
    test->Expect(
        symlink_result.error.find("symlink-secret") == std::string::npos,
        "symlink error never exposes target contents");

    const ops::CredentialResult directory_result =
        ops::ReadCredentialFile(temporary.path(), policy);
    test->Expect(!directory_result.ok() && directory_result.token.empty(),
                 "credential must be a regular file");
    if (::mkfifo(fifo.c_str(), 0600) != 0) {
        ThrowErrno("create credential fifo");
    }
    const auto fifo_start = std::chrono::steady_clock::now();
    const ops::CredentialResult fifo_result =
        ops::ReadCredentialFile(fifo, policy);
    const auto fifo_elapsed =
        std::chrono::steady_clock::now() - fifo_start;
    test->Expect(
        !fifo_result.ok() && fifo_result.token.empty() &&
            fifo_elapsed < std::chrono::seconds(1),
        "credential FIFO without a writer is rejected without blocking");
    test->Expect(!ops::ReadCredentialFile("", policy).ok(),
                 "empty credential path is rejected");
}

class ScopedEnvironment {
public:
    ScopedEnvironment(std::string name, std::optional<std::string> value)
        : name_(std::move(name)) {
        const char* existing = std::getenv(name_.c_str());
        if (existing != nullptr) {
            previous_ = std::string(existing);
        }
        const int status =
            value.has_value()
                ? ::setenv(name_.c_str(), value->c_str(), 1)
                : ::unsetenv(name_.c_str());
        if (status != 0) {
            ThrowErrno("set test environment");
        }
    }

    ~ScopedEnvironment() {
        if (previous_.has_value()) {
            static_cast<void>(
                ::setenv(name_.c_str(), previous_->c_str(), 1));
        } else {
            static_cast<void>(::unsetenv(name_.c_str()));
        }
    }

    ScopedEnvironment(const ScopedEnvironment&) = delete;
    ScopedEnvironment& operator=(const ScopedEnvironment&) = delete;

private:
    std::string name_;
    std::optional<std::string> previous_;
};

void CheckCredentialPathResolution(TestContext* test,
                                   const TempDirectory& temporary) {
    {
        ScopedEnvironment directory(
            "CREDENTIALS_DIRECTORY",
            std::optional<std::string>(temporary.path()));
        std::string error("stale");
        const std::optional<std::string> path =
            ops::ResolveSystemdCredentialPath("market-token", &error);
        test->Expect(path.has_value() &&
                         *path == temporary.Child("market-token") &&
                         error.empty(),
                     "systemd credential name resolves under its directory");

        for (const std::string_view invalid :
             {std::string_view(), std::string_view("."),
              std::string_view(".."), std::string_view("a/b"),
              std::string_view("a\0b", 3U)}) {
            error.clear();
            test->Expect(
                !ops::ResolveSystemdCredentialPath(invalid, &error)
                     .has_value() &&
                    !error.empty(),
                "invalid credential component is rejected");
        }
    }
    {
        ScopedEnvironment directory("CREDENTIALS_DIRECTORY", std::nullopt);
        std::string error;
        test->Expect(
            !ops::ResolveSystemdCredentialPath("market-token", &error)
                 .has_value() &&
                !error.empty(),
            "unset CREDENTIALS_DIRECTORY fails closed");
        test->Expect(
            !ops::ResolveSystemdCredentialPath("market-token", nullptr)
                 .has_value(),
            "unset credential directory is safe with a null error output");
    }
}

int BindFilesystemDatagram(const std::string& path) {
    const int fd = ::socket(AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        ThrowErrno("create filesystem notify socket");
    }

    struct sockaddr_un address {};
    address.sun_family = AF_UNIX;
    if (path.size() >= sizeof(address.sun_path)) {
        static_cast<void>(::close(fd));
        throw std::runtime_error("filesystem notify fixture path too long");
    }
    std::memcpy(address.sun_path, path.data(), path.size());
    address.sun_path[path.size()] = '\0';
    const socklen_t length = static_cast<socklen_t>(
        offsetof(sockaddr_un, sun_path) + path.size() + 1U);
    if (::bind(fd, reinterpret_cast<const struct sockaddr*>(&address),
               length) != 0) {
        const int bind_error = errno;
        static_cast<void>(::close(fd));
        if (bind_error == EPERM || bind_error == EACCES) {
            return -1;
        }
        errno = bind_error;
        ThrowErrno("bind filesystem notify socket");
    }
    return fd;
}

int BindAbstractDatagram(std::string_view name) {
    const int fd = ::socket(AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        ThrowErrno("create abstract notify socket");
    }

    struct sockaddr_un address {};
    address.sun_family = AF_UNIX;
    if (name.empty() || name.size() + 1U > sizeof(address.sun_path)) {
        static_cast<void>(::close(fd));
        throw std::runtime_error("abstract notify fixture name is invalid");
    }
    address.sun_path[0] = '\0';
    std::memcpy(address.sun_path + 1, name.data(), name.size());
    const socklen_t length = static_cast<socklen_t>(
        offsetof(sockaddr_un, sun_path) + 1U + name.size());
    if (::bind(fd, reinterpret_cast<const struct sockaddr*>(&address),
               length) != 0) {
        const int bind_error = errno;
        static_cast<void>(::close(fd));
        if (bind_error == EPERM || bind_error == EACCES) {
            return -1;
        }
        errno = bind_error;
        ThrowErrno("bind abstract notify socket");
    }
    return fd;
}

std::optional<std::string> ReceiveDatagram(int fd) {
    struct pollfd descriptor {};
    descriptor.fd = fd;
    descriptor.events = POLLIN;
    int ready = 0;
    do {
        ready = ::poll(&descriptor, 1U, 2000);
    } while (ready < 0 && errno == EINTR);
    if (ready <= 0 || (descriptor.revents & POLLIN) == 0) {
        return std::nullopt;
    }

    std::array<char, 4096> buffer {};
    ssize_t count = 0;
    do {
        count = ::recv(fd, buffer.data(), buffer.size(), 0);
    } while (count < 0 && errno == EINTR);
    if (count < 0) {
        ThrowErrno("receive notify datagram");
    }
    return std::string(buffer.data(), static_cast<std::size_t>(count));
}

void CheckNotifyNotConfiguredAndErrors(TestContext* test,
                                       const TempDirectory& temporary) {
    {
        ScopedEnvironment socket("NOTIFY_SOCKET", std::nullopt);
        const ops::NotifyResult result = ops::NotifySystemd("READY=1");
        test->Expect(result.status == ops::NotifyStatus::NotConfigured &&
                         result.error.empty(),
                     "unset NOTIFY_SOCKET is an explicit no-op");
    }
    {
        ScopedEnvironment socket("NOTIFY_SOCKET",
                                 std::optional<std::string>(""));
        const ops::NotifyResult result = ops::NotifySystemd("READY=1");
        test->Expect(result.status == ops::NotifyStatus::NotConfigured &&
                         result.error.empty(),
                     "empty NOTIFY_SOCKET is an explicit no-op");
    }
    {
        ScopedEnvironment socket(
            "NOTIFY_SOCKET",
            std::optional<std::string>(temporary.Child("missing.sock")));
        const ops::NotifyResult result = ops::NotifySystemd("READY=1");
        test->Expect(result.status == ops::NotifyStatus::Error &&
                         !result.error.empty(),
                     "configured but missing notify socket reports an error");
        const ops::NotifyResult empty = ops::NotifySystemd("");
        test->Expect(empty.status == ops::NotifyStatus::Error &&
                         !empty.error.empty(),
                     "empty notification state is rejected");
    }
    {
        const std::size_t path_capacity =
            sizeof(static_cast<sockaddr_un*>(nullptr)->sun_path);
        ScopedEnvironment socket(
            "NOTIFY_SOCKET",
            std::optional<std::string>(std::string(path_capacity, 'x')));
        const ops::NotifyResult result = ops::NotifySystemd("READY=1");
        test->Expect(result.status == ops::NotifyStatus::Error &&
                         !result.error.empty(),
                     "overlong notify socket path is rejected before sendto");
    }
}

void CheckFilesystemNotify(TestContext* test,
                           const TempDirectory& temporary) {
    const std::string socket_path = temporary.Child("notify.sock");
    const int bound = BindFilesystemDatagram(socket_path);
    if (bound < 0) {
        std::cout << "SKIP: sandbox forbids filesystem AF_UNIX bind\n";
        return;
    }
    ScopedFd receiver(bound);
    ScopedEnvironment socket(
        "NOTIFY_SOCKET", std::optional<std::string>(socket_path));

    const std::string state = "READY=1\nSTATUS=phase1-shadow";
    const ops::NotifyResult result = ops::NotifySystemd(state);
    test->Expect(result.status == ops::NotifyStatus::Sent &&
                     result.error.empty(),
                 "filesystem systemd notification reports Sent");
    const std::optional<std::string> received =
        ReceiveDatagram(receiver.get());
    test->Expect(received.has_value() && *received == state,
                 "filesystem notify socket receives the exact datagram");
}

void CheckAbstractNotify(TestContext* test) {
    const std::string name =
        "l2flow-phase1-" +
        std::to_string(static_cast<unsigned long long>(::getpid()));
    const int bound = BindAbstractDatagram(name);
    if (bound < 0) {
        std::cout << "SKIP: sandbox forbids abstract AF_UNIX bind\n";
        return;
    }
    ScopedFd receiver(bound);
    ScopedEnvironment socket(
        "NOTIFY_SOCKET", std::optional<std::string>("@" + name));

    const std::string state = "STATUS=abstract-socket\nREADY=1";
    const ops::NotifyResult result = ops::NotifySystemd(state);
    test->Expect(result.status == ops::NotifyStatus::Sent &&
                     result.error.empty(),
                 "Linux abstract systemd notification reports Sent");
    const std::optional<std::string> received =
        ReceiveDatagram(receiver.get());
    test->Expect(received.has_value() && *received == state,
                 "abstract notify socket receives the exact datagram");
}

}  // namespace

int main() {
    static_assert(
        noexcept(ops::NotifySystemd(std::string_view())),
        "sd_notify boundary must not allow exceptions to cross the caller");

    TestContext test;
    try {
        CheckIngressManifests(&test);
        CheckSubscriptionOperations(&test);
        CheckIngressKindParsing(&test);

        TempDirectory temporary;
        CheckCredentialSuccessAndNewlines(&test, temporary);
        CheckCredentialOwnershipAndPermissions(&test, temporary);
        CheckCredentialSizesAndContents(&test, temporary);
        CheckCredentialFileTypes(&test, temporary);
        CheckCredentialPathResolution(&test, temporary);

        CheckNotifyNotConfiguredAndErrors(&test, temporary);
        CheckFilesystemNotify(&test, temporary);
        CheckAbstractNotify(&test);
    } catch (const std::exception& error) {
        std::cerr << "FAIL: test fixture raised: " << error.what() << '\n';
        ++test.failures;
    } catch (...) {
        std::cerr << "FAIL: test fixture raised a non-standard exception\n";
        ++test.failures;
    }

    if (test.failures != 0) {
        std::cerr << "phase1 subscription/credential/systemd test failed with "
                  << test.failures << " error(s)\n";
        return 1;
    }
    std::cout
        << "phase1 subscription operations, credential security, and systemd "
           "notify checks passed\n";
    return 0;
}
