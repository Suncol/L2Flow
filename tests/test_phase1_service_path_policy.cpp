#include "l2flow/apps/ingress_service.h"
#include "l2flow/apps/service_path_policy.h"
#include "l2flow/ops/stable_output_prefix.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>

#include <fcntl.h>
#include <unistd.h>

namespace apps = l2flow::apps;
namespace ops = l2flow::ops;
namespace sdk = l2flow::sdk;

namespace {

struct TestContext final {
    void Expect(
        bool condition,
        const std::string& description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    int failures = 0;
};

class TempDirectory final {
public:
    TempDirectory() {
        std::array<char, 64U> pattern{};
        constexpr std::string_view value =
            "/tmp/l2flow-service-paths-XXXXXX";
        static_assert(value.size() < pattern.size());
        std::copy(
            value.begin(), value.end(), pattern.begin());
        const char* const created =
            ::mkdtemp(pattern.data());
        if (created == nullptr) {
            throw std::runtime_error(
                std::string("mkdtemp failed: ") +
                std::strerror(errno));
        }
        path_ = created;
        for (const std::string_view directory :
             {"inputs", "shadow", "metrics", "sdk-logs"}) {
            std::filesystem::create_directory(
                Child(directory));
        }
    }

    ~TempDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] std::string Child(
        std::string_view name) const {
        return path_ + "/" + std::string(name);
    }

private:
    std::string path_;
};

void CreateFile(const std::string& path) {
    const int fd =
        ::open(
            path.c_str(),
            O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC,
            0600);
    if (fd < 0) {
        throw std::runtime_error(
            std::string("open fixture failed: ") +
            std::strerror(errno));
    }
    if (::close(fd) != 0) {
        throw std::runtime_error(
            std::string("close fixture failed: ") +
            std::strerror(errno));
    }
}

apps::IngressServiceConfig ValidPaths(
    const TempDirectory& temporary) {
    apps::IngressServiceConfig config =
        apps::DefaultIngressServiceConfig(
            sdk::IngressKind::SzTick);
    config.preflight_paths.baseline_json =
        temporary.Child("inputs/baseline.json");
    config.preflight_paths.sdk_archive =
        temporary.Child("inputs/sdk.tar.gz");
    config.preflight_paths.shared_library =
        temporary.Child("inputs/libmdl_api.so");
    config.endpoint_contract_path =
        temporary.Child("inputs/endpoint.json");
    config.ingress.shadow_capture_path =
        temporary.Child("shadow/capture.bin");
    config.ingress.metrics_textfile_path =
        temporary.Child("metrics/service.prom");
    config.ingress.sdk_log_prefix =
        temporary.Child("sdk-logs/service");
    config.credential_path =
        temporary.Child("inputs/credential");
    return config;
}

void CheckDistinctAndLexicalAliases(
    TestContext* test,
    const TempDirectory& temporary) {
    apps::IngressServiceConfig config =
        ValidPaths(temporary);
    test->Expect(
        apps::ValidateIngressServicePathPolicy(
            config).empty(),
        "distinct absolute service paths pass");

    config.ingress.metrics_textfile_path =
        config.ingress.shadow_capture_path;
    test->Expect(
        !apps::ValidateIngressServicePathPolicy(
             config).empty(),
        "metrics and shadow cannot be the same path");

    config = ValidPaths(temporary);
    config.ingress.shadow_capture_path =
        temporary.Child("Capture.Output");
    config.ingress.metrics_textfile_path =
        temporary.Child("capture.output");
    test->Expect(
        !apps::ValidateIngressServicePathPolicy(
             config).empty(),
        "case-fold-equivalent names in one parent are conservatively "
        "rejected");

    config = ValidPaths(temporary);
    config.ingress.metrics_textfile_path =
        temporary.Child("sub/../shadow.capture");
    test->Expect(
        !apps::ValidateIngressServicePathPolicy(
             config).empty(),
        "dot-component aliases are rejected");

    config = ValidPaths(temporary);
    config.ingress.metrics_textfile_path =
        "relative.prom";
    test->Expect(
        !apps::ValidateIngressServicePathPolicy(
             config).empty(),
        "relative service paths are rejected");

    config = ValidPaths(temporary);
    config.ingress.metrics_textfile_path =
        temporary.Child("missing-parent/service.prom");
    test->Expect(
        !apps::ValidateIngressServicePathPolicy(
             config).empty(),
        "production path policy requires every parent to preexist");

    config = ValidPaths(temporary);
    config.ingress.metrics_textfile_path =
        temporary.Child("\xc3\xa9.prom");
    test->Expect(
        !apps::ValidateIngressServicePathPolicy(
             config).empty(),
        "non-ASCII service basenames are rejected so case-fold aliases "
        "remain decidable");

    config = ValidPaths(temporary);
    config.ingress.shadow_capture_path =
        temporary.Child(
            ops::kSdkLogDirectoryMarkerFilename);
    test->Expect(
        !apps::ValidateIngressServicePathPolicy(
             config).empty(),
        "shadow cannot use the reserved SDK log marker basename");

    config = ValidPaths(temporary);
    config.ingress.metrics_textfile_path =
        temporary.Child(
            ops::kSdkLogDirectoryMarkerFilename);
    test->Expect(
        !apps::ValidateIngressServicePathPolicy(
             config).empty(),
        "metrics cannot use the reserved SDK log marker basename");

    config = ValidPaths(temporary);
    config.ingress.metrics_textfile_path =
        temporary.Child(
            ".L2FLOW-SDK-LOG-DIRECTORY-V1");
    test->Expect(
        !apps::ValidateIngressServicePathPolicy(
             config).empty(),
        "ASCII case variants of the SDK log marker remain reserved");
}

void CheckProtectedInputs(
    TestContext* test,
    const TempDirectory& temporary) {
    apps::IngressServiceConfig config =
        ValidPaths(temporary);
    config.ingress.shadow_capture_path =
        config.endpoint_contract_path;
    test->Expect(
        !apps::ValidateIngressServicePathPolicy(
             config).empty(),
        "shadow cannot overwrite the endpoint contract");

    config = ValidPaths(temporary);
    config.ingress.metrics_textfile_path =
        config.preflight_paths.shared_library.string();
    test->Expect(
        !apps::ValidateIngressServicePathPolicy(
             config).empty(),
        "metrics cannot overwrite the vendor library");

    config = ValidPaths(temporary);
    config.ingress.shadow_capture_path =
        *config.credential_path;
    test->Expect(
        !apps::ValidateIngressServicePathPolicy(
             config).empty(),
        "shadow cannot overwrite an explicit credential");

    config = ValidPaths(temporary);
    config.credential_path.reset();
    test->Expect(
        !apps::ValidateIngressServicePathPolicy(
             config,
             config.ingress.metrics_textfile_path)
             .empty(),
        "metrics cannot overwrite a resolved systemd credential");
}

void CheckSdkLogNamespaceIsolation(
    TestContext* test,
    const TempDirectory& temporary) {
    apps::IngressServiceConfig config =
        ValidPaths(temporary);
    config.ingress.shadow_capture_path =
        config.ingress.sdk_log_prefix + ".log";
    test->Expect(
        !apps::ValidateIngressServicePathPolicy(
             config).empty(),
        "shadow cannot alias the SDK primary rolling-log name");

    config = ValidPaths(temporary);
    config.ingress.metrics_textfile_path =
        config.ingress.sdk_log_prefix + ".trace.log";
    test->Expect(
        !apps::ValidateIngressServicePathPolicy(
             config).empty(),
        "metrics cannot alias the SDK trace rolling-log name");

    config = ValidPaths(temporary);
    config.ingress.sdk_log_prefix =
        temporary.Child("inputs/vendor-log");
    test->Expect(
        !apps::ValidateIngressServicePathPolicy(
             config).empty(),
        "the SDK rolling-log namespace cannot share a protected-input "
        "directory");
}

void CheckFilesystemAliases(
    TestContext* test,
    const TempDirectory& temporary) {
    const std::string original =
        temporary.Child("existing-original");
    const std::string hardlink =
        temporary.Child("existing-hardlink");
    CreateFile(original);
    if (::link(original.c_str(), hardlink.c_str()) != 0) {
        throw std::runtime_error(
            std::string("link fixture failed: ") +
            std::strerror(errno));
    }

    apps::IngressServiceConfig config =
        ValidPaths(temporary);
    config.ingress.shadow_capture_path = original;
    config.ingress.metrics_textfile_path = hardlink;
    const std::string hardlink_error =
        apps::ValidateIngressServicePathPolicy(
            config);
    test->Expect(
        !hardlink_error.empty(),
        "existing hardlink aliases are rejected");
    test->Expect(
        hardlink_error.find(original) ==
                std::string::npos &&
            hardlink_error.find(hardlink) ==
                std::string::npos,
        "path-policy diagnostics do not echo paths");

    const std::string real_directory =
        temporary.Child("real-directory");
    const std::string alias_directory =
        temporary.Child("alias-directory");
    std::filesystem::create_directory(real_directory);
    if (::symlink(
            real_directory.c_str(),
            alias_directory.c_str()) != 0) {
        throw std::runtime_error(
            std::string("symlink fixture failed: ") +
            std::strerror(errno));
    }
    config = ValidPaths(temporary);
    config.ingress.shadow_capture_path =
        real_directory + "/capture";
    config.ingress.metrics_textfile_path =
        alias_directory + "/capture";
    test->Expect(
        !apps::ValidateIngressServicePathPolicy(
             config).empty(),
        "resolved parent-directory aliases are rejected");
}

}  // namespace

int main() {
    TestContext test;
    try {
        TempDirectory temporary;
        CheckDistinctAndLexicalAliases(
            &test, temporary);
        CheckProtectedInputs(&test, temporary);
        CheckSdkLogNamespaceIsolation(
            &test, temporary);
        CheckFilesystemAliases(&test, temporary);
    } catch (const std::exception& exception) {
        std::cerr << "FAIL: setup raised: "
                  << exception.what() << '\n';
        ++test.failures;
    }

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " service path-policy test(s) failed\n";
        return 1;
    }
    std::cout
        << "service path policy prevents writable/protected aliases\n";
    return 0;
}
