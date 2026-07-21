#include "l2flow/apps/ingress_service.h"
#include "l2flow/common/sha256.h"
#include "l2flow/ingress/ingress_app.h"
#include "l2flow/ops/metrics_worker.h"
#include "l2flow/ops/stable_output_prefix.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace apps = l2flow::apps;
namespace common = l2flow::common;
namespace ingress = l2flow::ingress;
namespace ops = l2flow::ops;
namespace sdk = l2flow::sdk;
namespace mdl = datayes::mdl;

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

struct ServiceSdkState final {
    std::string log_prefix;
    bool log_console = true;
    int work_threads = 0;
    int io_threads = 0;
    std::size_t enable_log_calls = 0U;
    std::size_t shutdown_calls = 0U;
    std::size_t subscriber_release_calls = 0U;
    std::size_t manager_release_calls = 0U;
};

class ServiceTestClock final : public ingress::CaptureClock {
public:
    std::uint64_t MonotonicRawNanoseconds() override {
        return monotonic_++;
    }

    std::uint64_t RealtimeNanoseconds() override {
        return realtime_++;
    }

private:
    std::uint64_t monotonic_ = 1000U;
    std::uint64_t realtime_ = 2000U;
};

class ServiceTestOutput final
    : public ingress::ShadowCaptureOutput {
public:
    ingress::ShadowOutputWriteResult WriteSome(
        std::span<const std::byte> bytes) noexcept override {
        return {bytes.size(), 0};
    }

    int Fdatasync() noexcept override {
        return 0;
    }

    int Close() noexcept override {
        return 0;
    }
};

class ServiceTestSubscriber final : public sdk::SdkSubscriber {
public:
    explicit ServiceTestSubscriber(
        std::shared_ptr<ServiceSdkState> state)
        : state_(std::move(state)) {}

    void SetServerAddress(std::string_view) override {}
    void SetUserName(std::string_view) override {}
    void SetHeartbeatInterval(std::uint32_t) override {}
    void SetHeartbeatTimeout(std::uint32_t) override {}
    void SetMessageEncoding(
        mdl::MDLMessageEncoding) override {}
    void EnableMergeMessage(bool) override {}
    void SetSendMacAuth(bool) override {}
    void EnableServerSelect(bool) override {}
    void AddSubscription(const sdk::MessageKey&) override {}

    std::string Connect() override {
        return {};
    }

    bool Release(std::string* error) noexcept override {
        if (!released_) {
            released_ = true;
            ++state_->subscriber_release_calls;
        }
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

private:
    std::shared_ptr<ServiceSdkState> state_;
    bool released_ = false;
};

class ServiceTestManager final : public sdk::SdkManager {
public:
    explicit ServiceTestManager(
        std::shared_ptr<ServiceSdkState> state)
        : state_(std::move(state)) {}

    void EnableLog(
        std::string_view prefix,
        bool console) override {
        state_->log_prefix = prefix;
        state_->log_console = console;
        ++state_->enable_log_calls;
    }

    std::unique_ptr<sdk::SdkSubscriber> CreateSubscriber(
        mdl::MessageHandlerBase*,
        bool) override {
        return std::make_unique<ServiceTestSubscriber>(
            state_);
    }

    void Shutdown() override {
        ++state_->shutdown_calls;
    }

    bool Release(std::string* error) noexcept override {
        if (!released_) {
            released_ = true;
            ++state_->manager_release_calls;
        }
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

private:
    std::shared_ptr<ServiceSdkState> state_;
    bool released_ = false;
};

class ServiceTestFactory final : public sdk::SdkFactory {
public:
    explicit ServiceTestFactory(
        std::shared_ptr<ServiceSdkState> state)
        : state_(std::move(state)) {}

    std::unique_ptr<sdk::SdkManager> Create(
        int work_threads,
        int io_threads) override {
        state_->work_threads = work_threads;
        state_->io_threads = io_threads;
        return std::make_unique<ServiceTestManager>(state_);
    }

private:
    std::shared_ptr<ServiceSdkState> state_;
};

struct MetricsPublication final {
    std::string path;
    std::string contents;
};

class RecordingMetricsBackend final
    : public ops::MetricsPublishBackend {
public:
    bool Publish(
        const std::string& path,
        std::string_view contents,
        std::string* error) override {
        {
            const std::lock_guard<std::mutex> lock(mutex_);
            publications_.push_back(
                MetricsPublication{
                    path, std::string(contents)});
        }
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

    [[nodiscard]] std::vector<MetricsPublication>
    Publications() const {
        const std::lock_guard<std::mutex> lock(mutex_);
        return publications_;
    }

private:
    mutable std::mutex mutex_;
    std::vector<MetricsPublication> publications_;
};

class TemporaryCredential final {
public:
    TemporaryCredential() {
        std::array<char, 64U> pattern{};
        const std::string value =
            "/tmp/l2flow-service-config-XXXXXX";
        std::copy(
            value.begin(), value.end(), pattern.begin());
        char* const created =
            ::mkdtemp(pattern.data());
        if (created == nullptr) {
            throw std::runtime_error(
                std::string("mkdtemp failed: ") +
                std::strerror(errno));
        }
        directory_ = created;
        shadow_directory_ =
            directory_ + "/shadow";
        metrics_directory_ =
            directory_ + "/metrics";
        sdk_log_directory_ =
            directory_ + "/sdk-logs";
        for (const std::string* child :
             {&shadow_directory_,
              &metrics_directory_,
              &sdk_log_directory_}) {
            if (::mkdir(child->c_str(), 0700) != 0) {
                throw std::runtime_error(
                    std::string(
                        "mkdir output fixture failed: ") +
                    std::strerror(errno));
            }
        }
        path_ =
            directory_ + "/mdl_sz_tick_token";

        constexpr std::string_view token =
            "runtime-systemd-secret\n";
        WriteExclusiveFile(path_, token);
        if (::chmod(path_.c_str(), 0400) != 0) {
            throw std::runtime_error(
                std::string(
                    "finalize credential failed: ") +
                std::strerror(errno));
        }

        contract_path_ =
            directory_ + "/sz_tick_endpoint.json";
        constexpr std::string_view endpoint_contract =
            "{\"schema_version\":1,"
            "\"ingress_kind\":\"sz-tick\","
            "\"name\":\"sz_tick_prod\","
            "\"resolved_server_address\":"
            "\"tcp://feed.example:1234\","
            "\"message_encoding\":7,"
            "\"merge_message\":true,"
            "\"send_mac_auth\":false,"
            "\"server_select\":true}";
        WriteExclusiveFile(
            contract_path_, endpoint_contract);
        contract_sha256_ = common::Sha256Hex(
            common::ComputeSha256(endpoint_contract));

        sdk_log_marker_path_ =
            sdk_log_directory_ + "/" +
            std::string(
                l2flow::ops::
                    kSdkLogDirectoryMarkerFilename);
        WriteExclusiveFile(
            sdk_log_marker_path_,
            l2flow::ops::kSdkLogDirectoryMarkerContent);
        if (::chmod(
                sdk_log_marker_path_.c_str(),
                0444) != 0) {
            throw std::runtime_error(
                std::string(
                    "finalize SDK log marker failed: ") +
                std::strerror(errno));
        }
    }

    ~TemporaryCredential() {
        if (!contract_path_.empty()) {
            static_cast<void>(
                ::unlink(contract_path_.c_str()));
        }
        if (!path_.empty()) {
            static_cast<void>(::unlink(path_.c_str()));
        }
        if (!sdk_log_marker_path_.empty()) {
            static_cast<void>(
                ::unlink(sdk_log_marker_path_.c_str()));
        }
        if (!sdk_log_directory_.empty()) {
            static_cast<void>(
                ::rmdir(sdk_log_directory_.c_str()));
        }
        if (!metrics_directory_.empty()) {
            static_cast<void>(
                ::rmdir(metrics_directory_.c_str()));
        }
        if (!shadow_directory_.empty()) {
            static_cast<void>(
                ::rmdir(shadow_directory_.c_str()));
        }
        if (!directory_.empty()) {
            static_cast<void>(::rmdir(directory_.c_str()));
        }
    }

    TemporaryCredential(
        const TemporaryCredential&) = delete;
    TemporaryCredential& operator=(
        const TemporaryCredential&) = delete;

    [[nodiscard]] const std::string& directory() const {
        return directory_;
    }

    [[nodiscard]] const std::string& path() const {
        return path_;
    }

    [[nodiscard]] const std::string& contract_path() const {
        return contract_path_;
    }

    [[nodiscard]] const std::string& contract_sha256() const {
        return contract_sha256_;
    }

    [[nodiscard]] const std::string& shadow_directory() const {
        return shadow_directory_;
    }

    [[nodiscard]] const std::string& metrics_directory() const {
        return metrics_directory_;
    }

    [[nodiscard]] const std::string& sdk_log_directory() const {
        return sdk_log_directory_;
    }

private:
    static void WriteExclusiveFile(
        const std::string& path,
        std::string_view contents) {
        const int descriptor =
            ::open(
                path.c_str(),
                O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC,
                0600);
        if (descriptor < 0) {
            throw std::runtime_error(
                std::string("open fixture failed: ") +
                std::strerror(errno));
        }

        std::size_t offset = 0U;
        while (offset < contents.size()) {
            const ssize_t written =
                ::write(
                    descriptor,
                    contents.data() + offset,
                    contents.size() - offset);
            if (written < 0 && errno == EINTR) {
                continue;
            }
            if (written <= 0) {
                const int write_error = errno;
                static_cast<void>(::close(descriptor));
                throw std::runtime_error(
                    std::string("write fixture failed: ") +
                    std::strerror(write_error));
            }
            offset += static_cast<std::size_t>(written);
        }
        if (::close(descriptor) != 0) {
            throw std::runtime_error(
                std::string("close fixture failed: ") +
                std::strerror(errno));
        }
    }

    std::string directory_;
    std::string path_;
    std::string contract_path_;
    std::string contract_sha256_;
    std::string shadow_directory_;
    std::string metrics_directory_;
    std::string sdk_log_directory_;
    std::string sdk_log_marker_path_;
};

class ScopedEnvironment final {
public:
    ScopedEnvironment(
        std::string name,
        std::optional<std::string> value)
        : name_(std::move(name)) {
        const char* const previous =
            std::getenv(name_.c_str());
        if (previous != nullptr) {
            previous_ = std::string(previous);
        }
        const int result =
            value.has_value()
                ? ::setenv(
                      name_.c_str(),
                      value->c_str(),
                      1)
                : ::unsetenv(name_.c_str());
        if (result != 0) {
            throw std::runtime_error(
                std::string(
                    "environment update failed: ") +
                std::strerror(errno));
        }
    }

    ~ScopedEnvironment() {
        if (previous_.has_value()) {
            static_cast<void>(::setenv(
                name_.c_str(),
                previous_->c_str(),
                1));
        } else {
            static_cast<void>(
                ::unsetenv(name_.c_str()));
        }
    }

    ScopedEnvironment(
        const ScopedEnvironment&) = delete;
    ScopedEnvironment& operator=(
        const ScopedEnvironment&) = delete;

private:
    std::string name_;
    std::optional<std::string> previous_;
};

class ScopedStandardErrorCapture final {
public:
    ScopedStandardErrorCapture()
        : previous_(std::cerr.rdbuf(output_.rdbuf())) {}

    ~ScopedStandardErrorCapture() {
        std::cerr.rdbuf(previous_);
    }

    ScopedStandardErrorCapture(
        const ScopedStandardErrorCapture&) = delete;
    ScopedStandardErrorCapture& operator=(
        const ScopedStandardErrorCapture&) = delete;

    [[nodiscard]] std::string text() const {
        return output_.str();
    }

private:
    std::ostringstream output_;
    std::streambuf* previous_;
};

std::vector<std::string> CompleteArguments() {
    return {
        "--baseline",
        "/approved/vendor_baseline.json",
        "--archive",
        "/approved/mdl_sdk.tar.gz",
        "--library",
        "/approved/libmdl_api.so",
        "--endpoint-contract",
        "/approved/sz-tick.endpoint.json",
        "--endpoint-contract-sha256",
        std::string(64U, 'a'),
        "--credential-name",
        "mdl_sz_tick_token",
        "--credential-path",
        "/run/credentials/mdl_sz_tick_token",
        "--capture-date",
        "20260717",
        "--shadow-path",
        "/data/shadow/sz-tick.capture",
        "--sdk-log-prefix",
        "/var/log/mdl/sz-tick",
        "--metrics-path",
        "/run/prometheus-textfile/mdl-sz-tick.prom",
        "--ring-bytes",
        "600000000",
        "--max-message-bytes",
        "8388608",
        "--include-optional-index",
        "true",
    };
}

apps::IngressServiceParseResult Parse(
    sdk::IngressKind kind,
    const std::vector<std::string>& arguments) {
    std::vector<std::string_view> views;
    views.reserve(arguments.size());
    for (const std::string& argument : arguments) {
        views.emplace_back(argument);
    }
    return apps::ParseIngressServiceArguments(
        kind, views);
}

void RemoveOption(
    std::vector<std::string>* arguments,
    std::string_view option) {
    const auto found = std::find(
        arguments->begin(), arguments->end(), option);
    if (found == arguments->end()) {
        return;
    }
    const auto after = std::next(found);
    arguments->erase(
        found,
        after == arguments->end()
            ? after
            : std::next(after));
}

void SetOptionValue(
    std::vector<std::string>* arguments,
    std::string_view option,
    std::string value) {
    const auto found = std::find(
        arguments->begin(), arguments->end(), option);
    if (found != arguments->end() &&
        std::next(found) != arguments->end()) {
        *std::next(found) = std::move(value);
    }
}

void SetTemporaryOutputPaths(
    std::vector<std::string>* arguments,
    const TemporaryCredential& temporary) {
    SetOptionValue(
        arguments,
        "--shadow-path",
        temporary.shadow_directory() +
            "/capture.bin");
    SetOptionValue(
        arguments,
        "--metrics-path",
        temporary.metrics_directory() +
            "/service.prom");
    SetOptionValue(
        arguments,
        "--sdk-log-prefix",
        temporary.sdk_log_directory() +
            "/service");
}

int RunService(
    const std::vector<std::string>& arguments,
    std::string* standard_error) {
    std::vector<const char*> argv;
    argv.reserve(arguments.size() + 1U);
    argv.push_back("mdl-ingress-sz-tick");
    for (const std::string& argument : arguments) {
        argv.push_back(argument.c_str());
    }

    ScopedStandardErrorCapture capture;
    const int result =
        apps::RunIngressService(
            sdk::IngressKind::SzTick,
            static_cast<int>(argv.size()),
            argv.data());
    *standard_error = capture.text();
    return result;
}

void CheckStableServiceGlueAndFinalMetricsDrain(
    TestContext* test) {
    TemporaryCredential temporary;
    std::vector<std::string> arguments =
        CompleteArguments();
    SetTemporaryOutputPaths(&arguments, temporary);

    apps::IngressServiceParseResult parsed =
        Parse(sdk::IngressKind::SzTick, arguments);
    test->Expect(
        parsed.success,
        "stable-prefix service glue fixture parses");
    if (!parsed.success) {
        return;
    }

    sdk::IngressConfig config =
        std::move(parsed.config.ingress);
    config.endpoint.name = "test-endpoint";
    config.endpoint.resolved_server_address =
        "tcp://127.0.0.1:18001";
    config.endpoint.contract_sha256 =
        temporary.contract_sha256();
    config.endpoint.message_encoding =
        mdl::MDLEID_BINARY;
    config.endpoint.merge_message = true;
    config.endpoint.send_mac_auth = false;
    config.endpoint.server_select = true;
    config.token = "service-glue-secret";
    config.max_message_bytes = 128U;
    config.ring_capacity_bytes = 1024U;

    const std::string config_error =
        sdk::ValidateIngressConfig(config);
    test->Expect(
        config_error.empty(),
        "service glue fixture is a valid ingress config: " +
            config_error);
    if (!config_error.empty()) {
        return;
    }

    std::string lease_error;
    std::unique_ptr<ops::StableOutputPrefix> sdk_log_lease =
        ops::OpenStableOutputPrefix(
            config.sdk_log_prefix, &lease_error);
    test->Expect(
        sdk_log_lease != nullptr,
        "service acquires a stable SDK log prefix lease: " +
            lease_error);
    if (sdk_log_lease == nullptr) {
        return;
    }
    const std::string stable_prefix =
        sdk_log_lease->stable_prefix();
    test->Expect(
        stable_prefix != config.sdk_log_prefix &&
            stable_prefix.starts_with("/proc/self/fd/") &&
            stable_prefix.ends_with("/service"),
        "stable SDK log prefix retains the configured basename "
        "under an fd-anchored directory");

    const std::string config_sha256 =
        sdk::IngressConfigSha256(config);
    const auto sdk_state =
        std::make_shared<ServiceSdkState>();
    const auto metrics_backend =
        std::make_shared<RecordingMetricsBackend>();
    ops::MetricsWorker metrics_worker(
        config.metrics_textfile_path,
        metrics_backend);

    std::string final_metrics;
    bool final_snapshot_submitted = false;
    {
        ingress::IngressAppOptions options;
        options.callback_quiesce_timeout =
            std::chrono::milliseconds(500);
        options.sdk_log_runtime_prefix =
            stable_prefix;
        ingress::IngressApp app(
            config,
            std::make_shared<ServiceTestFactory>(
                sdk_state),
            std::make_unique<ServiceTestClock>(),
            std::make_unique<ServiceTestOutput>(),
            std::move(options));

        std::string error;
        const bool initialized = app.Initialize(&error);
        test->Expect(
            initialized,
            "service glue initializes IngressApp through "
            "the fake SDK: " +
                error);
        if (initialized) {
            const bool stopped = app.Stop(&error);
            test->Expect(
                stopped &&
                    app.state() ==
                        ingress::IngressAppState::Stopped &&
                    app.reconciliation().exact(),
                "service glue stops and reconciles before "
                "rendering final metrics: " +
                    error);
            if (stopped) {
                final_metrics =
                    app.prometheus_metrics();
                final_snapshot_submitted =
                    metrics_worker.Submit(
                        final_metrics);
                test->Expect(
                    final_snapshot_submitted,
                    "post-stop metrics snapshot is accepted "
                    "before worker drain");
            }
        }
    }

    metrics_worker.StopAndJoin();
    const ops::MetricsWorkerSnapshot worker_snapshot =
        metrics_worker.Snapshot();
    const std::vector<MetricsPublication> publications =
        metrics_backend->Publications();

    test->Expect(
        sdk_state->enable_log_calls == 1U &&
            sdk_state->log_prefix == stable_prefix &&
            !sdk_state->log_console &&
            sdk_state->work_threads ==
                config.work_threads &&
            sdk_state->io_threads ==
                config.io_threads,
        "IngressApp passes only the stable leased prefix "
        "to the SDK manager");
    test->Expect(
        sdk_state->shutdown_calls == 1U &&
            sdk_state->subscriber_release_calls == 1U &&
            sdk_state->manager_release_calls == 1U,
        "fake SDK completes the production shutdown order");
    test->Expect(
        final_snapshot_submitted &&
            publications.size() == 1U &&
            publications.front().path ==
                config.metrics_textfile_path &&
            publications.front().contents ==
                final_metrics &&
            worker_snapshot.submitted == 1U &&
            worker_snapshot.completed == 1U &&
            worker_snapshot.failed == 0U &&
            worker_snapshot.dropped == 0U &&
            metrics_worker.TakeLastError().empty(),
        "worker shutdown drains the exact final post-stop "
        "metrics snapshot");
    test->Expect(
        final_metrics.find(config_sha256) !=
                std::string::npos &&
            final_metrics.find(stable_prefix) ==
                std::string::npos &&
            final_metrics.find(config.token) ==
                std::string::npos,
        "runtime path anchoring preserves canonical config "
        "identity and final metrics secrecy");
}

void CheckFourKindDefaults(TestContext* test) {
    struct Expected final {
        sdk::IngressKind kind;
        int work_threads;
        int io_threads;
        std::uint32_t source_stream_id;
    };
    constexpr std::array<Expected, 4U> expected{{
        {sdk::IngressKind::ShSnapshot, 2, 1, 1001U},
        {sdk::IngressKind::ShTick, 4, 1, 1002U},
        {sdk::IngressKind::SzSnapshot, 2, 1, 2001U},
        {sdk::IngressKind::SzTick, 4, 1, 2002U},
    }};

    for (const Expected& item : expected) {
        const apps::IngressServiceConfig defaults =
            apps::DefaultIngressServiceConfig(item.kind);
        const sdk::IngressSpec& spec =
            sdk::GetIngressSpec(item.kind);
        test->Expect(
            defaults.ingress.kind == item.kind &&
                defaults.ingress.work_threads ==
                    item.work_threads &&
                defaults.ingress.io_threads ==
                    item.io_threads &&
                spec.source_stream_id ==
                    item.source_stream_id,
            "service defaults preserve the selected stream identity");
        test->Expect(
            defaults.ingress.ring_capacity_bytes ==
                    sdk::kDefaultMinimumRingBytes &&
                defaults.ingress.max_message_bytes ==
                    sdk::kDefaultMaxMessageBytes &&
                !defaults.ingress.endpoint.send_mac_auth
                     .has_value() &&
                !defaults.credential_path.has_value(),
            "shared defaults remain safe and do not invent "
            "endpoint or credential policy");

        const apps::IngressServiceParseResult parsed =
            Parse(item.kind, CompleteArguments());
        test->Expect(
            parsed.success &&
                parsed.config.ingress.kind == item.kind &&
                parsed.config.ingress.work_threads ==
                    item.work_threads &&
                parsed.config.ingress.io_threads ==
                    item.io_threads,
            "the common parser retains each binary's "
            "compile-time kind defaults");
    }
}

void CheckCompleteMapping(TestContext* test) {
    const apps::IngressServiceParseResult parsed =
        Parse(
            sdk::IngressKind::SzTick,
            CompleteArguments());
    test->Expect(
        parsed.success && !parsed.show_help &&
            parsed.error.empty(),
        "a complete explicit service configuration parses");
    if (!parsed.success) {
        return;
    }

    const apps::IngressServiceConfig& config =
        parsed.config;
    test->Expect(
        config.preflight_paths.baseline_json ==
                "/approved/vendor_baseline.json" &&
            config.preflight_paths.sdk_archive ==
                "/approved/mdl_sdk.tar.gz" &&
            config.preflight_paths.shared_library ==
                "/approved/libmdl_api.so",
        "all three full-preflight paths map exactly");
    test->Expect(
        config.endpoint_contract_path ==
                "/approved/sz-tick.endpoint.json" &&
            config.endpoint_contract_sha256 ==
                std::string(64U, 'a'),
        "reviewed endpoint contract path and expected hash map exactly");
    test->Expect(
        config.ingress.endpoint.name.empty() &&
            config.ingress.endpoint
                .resolved_server_address.empty() &&
            config.ingress.endpoint
                .contract_sha256.empty() &&
            config.ingress.endpoint.message_encoding ==
                datayes::mdl::MDLEID_UNDEFINED &&
            !config.ingress.endpoint.merge_message &&
            !config.ingress.endpoint.send_mac_auth.has_value() &&
            !config.ingress.endpoint.server_select,
        "the pure parser does not manufacture endpoint fields before "
        "the pinned contract file is loaded");
    test->Expect(
        config.ingress.capture_date == 20260717U &&
            config.ingress.ring_capacity_bytes ==
                600000000U &&
            config.ingress.max_message_bytes ==
                8388608U &&
            config.ingress.metrics_textfile_path ==
                "/run/prometheus-textfile/mdl-sz-tick.prom" &&
            config.ingress.include_optional_index,
        "date, capacity, message limit, metrics path, and optional index "
        "map exactly");
    test->Expect(
        config.credential_path ==
                std::optional<std::string>(
                    "/run/credentials/mdl_sz_tick_token") &&
            config.ingress.token.empty(),
        "parsing maps only the credential path and never accepts a token");
}

void CheckOptionalDefaults(TestContext* test) {
    std::vector<std::string> arguments =
        CompleteArguments();
    RemoveOption(&arguments, "--credential-path");
    RemoveOption(
        &arguments, "--include-optional-index");
    RemoveOption(&arguments, "--ring-bytes");
    RemoveOption(
        &arguments, "--max-message-bytes");

    const apps::IngressServiceParseResult parsed =
        Parse(
            sdk::IngressKind::ShSnapshot,
            arguments);
    test->Expect(
        parsed.success &&
            !parsed.config.credential_path.has_value() &&
            parsed.config.ingress.endpoint.name.empty() &&
            !parsed.config.ingress.endpoint.send_mac_auth
                 .has_value() &&
            !parsed.config.ingress.include_optional_index &&
            parsed.config.ingress.ring_capacity_bytes ==
                sdk::kDefaultMinimumRingBytes &&
            parsed.config.ingress.max_message_bytes ==
                sdk::kDefaultMaxMessageBytes,
        "optional CLI fields retain documented defaults "
        "and select systemd credentials");

    arguments = CompleteArguments();
    RemoveOption(
        &arguments, "--endpoint-contract-sha256");
    const apps::IngressServiceParseResult missing_hash =
        Parse(sdk::IngressKind::ShSnapshot, arguments);
    test->Expect(
        !missing_hash.success &&
            missing_hash.error.find(
                "--endpoint-contract-sha256") !=
                std::string::npos,
        "the expected endpoint contract hash must always be explicit");
}

void CheckStrictRejection(TestContext* test) {
    std::vector<std::string> arguments =
        CompleteArguments();
    arguments.push_back("--baseline");
    arguments.push_back("/second/baseline");
    test->Expect(
        !Parse(sdk::IngressKind::SzTick, arguments)
             .success,
        "duplicate options are rejected");

    arguments = CompleteArguments();
    arguments.push_back("--unknown");
    arguments.push_back("top-secret-accidental-value");
    const apps::IngressServiceParseResult unknown =
        Parse(sdk::IngressKind::SzTick, arguments);
    test->Expect(
        !unknown.success &&
            unknown.error.find(
                "top-secret-accidental-value") ==
                std::string::npos,
        "unknown options are rejected without echoing following values");

    const std::vector<std::string> missing_value{
        "--baseline", "--archive", "/archive"};
    test->Expect(
        !Parse(
             sdk::IngressKind::SzTick,
             missing_value)
             .success,
        "an option followed by another option has a missing value");

    arguments = CompleteArguments();
    SetOptionValue(
        &arguments,
        "--ring-bytes",
        "18446744073709551616");
    test->Expect(
        !Parse(sdk::IngressKind::SzTick, arguments)
             .success,
        "uint64 overflow is rejected");

    arguments = CompleteArguments();
    SetOptionValue(
        &arguments,
        "--ring-bytes",
        "536870911");
    test->Expect(
        !Parse(sdk::IngressKind::SzTick, arguments)
             .success,
        "the production service rejects rings below 512 MiB");

    arguments = CompleteArguments();
    SetOptionValue(
        &arguments,
        "--ring-bytes",
        "536870912");
    test->Expect(
        Parse(sdk::IngressKind::SzTick, arguments)
            .success,
        "the exact 512 MiB production ring boundary is accepted");

    for (const std::string_view invalid :
         {"+536870912", "-536870912", " 536870912",
          "536870912 ", "536870912bytes"}) {
        arguments = CompleteArguments();
        SetOptionValue(
            &arguments,
            "--ring-bytes",
            std::string(invalid));
        test->Expect(
            !Parse(sdk::IngressKind::SzTick, arguments)
                 .success,
            "ring capacity accepts only a complete unsigned decimal");
    }

    arguments = CompleteArguments();
    SetOptionValue(
        &arguments,
        "--capture-date",
        "4294967296");
    test->Expect(
        !Parse(sdk::IngressKind::SzTick, arguments)
             .success,
        "uint32 overflow is rejected");

    for (const std::string_view invalid_date :
         {"10101", "020260717", "100001231"}) {
        arguments = CompleteArguments();
        SetOptionValue(
            &arguments,
            "--capture-date",
            std::string(invalid_date));
        test->Expect(
            !Parse(sdk::IngressKind::SzTick, arguments)
                 .success,
            "capture date requires exactly eight decimal YYYYMMDD digits");
    }

    constexpr std::array<std::string_view, 6U>
        forbidden_endpoint_overrides{{
            "--endpoint-name",
            "--endpoint-address",
            "--encoding",
            "--merge-message",
            "--send-mac-auth",
            "--server-select",
        }};
    for (const std::string_view option :
         forbidden_endpoint_overrides) {
        arguments = CompleteArguments();
        arguments.emplace_back(option);
        arguments.emplace_back("attacker-controlled-override");
        const apps::IngressServiceParseResult rejected =
            Parse(
                sdk::IngressKind::SzTick,
                arguments);
        test->Expect(
            !rejected.success &&
                rejected.error.find("unknown") !=
                    std::string::npos &&
                rejected.error.find(
                    "attacker-controlled-override") ==
                    std::string::npos,
            "individual endpoint fields cannot override the pinned "
            "endpoint contract");
    }

    arguments = CompleteArguments();
    SetOptionValue(
        &arguments,
        "--endpoint-contract-sha256",
        "not-a-sha256");
    test->Expect(
        !Parse(sdk::IngressKind::SzTick, arguments)
             .success,
        "a malformed endpoint contract hash is rejected");

    arguments = CompleteArguments();
    SetOptionValue(
        &arguments,
        "--endpoint-contract-sha256",
        std::string(64U, 'A'));
    test->Expect(
        !Parse(sdk::IngressKind::SzTick, arguments)
             .success,
        "an uppercase endpoint contract hash is rejected");

    arguments = CompleteArguments();
    SetOptionValue(
        &arguments,
        "--endpoint-contract",
        "relative/endpoint-contract.json");
    test->Expect(
        !Parse(sdk::IngressKind::SzTick, arguments)
             .success,
        "the endpoint contract path must be absolute");

    arguments = CompleteArguments();
    SetOptionValue(
        &arguments,
        "--metrics-path",
        "/data/shadow/sz-tick.capture");
    test->Expect(
        !Parse(sdk::IngressKind::SzTick, arguments)
             .success,
        "metrics cannot replace the shadow capture path");

    arguments = CompleteArguments();
    SetOptionValue(
        &arguments,
        "--shadow-path",
        "/approved/libmdl_api.so");
    test->Expect(
        !Parse(sdk::IngressKind::SzTick, arguments)
             .success,
        "writable outputs cannot alias preflight inputs");

    arguments = CompleteArguments();
    SetOptionValue(
        &arguments,
        "--shadow-path",
        "/var/log/mdl/sz-tick.log");
    test->Expect(
        !Parse(sdk::IngressKind::SzTick, arguments)
             .success,
        "shadow cannot alias a derived SDK rolling-log path");

    arguments = CompleteArguments();
    SetOptionValue(
        &arguments,
        "--metrics-path",
        "/var/log/mdl/sz-tick.trace.log");
    test->Expect(
        !Parse(sdk::IngressKind::SzTick, arguments)
             .success,
        "metrics cannot alias a derived SDK trace-log path");

    arguments = CompleteArguments();
    SetOptionValue(
        &arguments,
        "--sdk-log-prefix",
        "/approved/sdk-log");
    test->Expect(
        !Parse(sdk::IngressKind::SzTick, arguments)
             .success,
        "SDK rolling logs require a directory isolated from preflight "
        "inputs");

    arguments = CompleteArguments();
    SetOptionValue(
        &arguments,
        "--include-optional-index",
        "TRUE");
    test->Expect(
        !Parse(sdk::IngressKind::SzTick, arguments)
             .success,
        "booleans use exact lowercase true or false");

    arguments = CompleteArguments();
    SetOptionValue(
        &arguments,
        "--endpoint-contract",
        std::string("/approved/prod\0bad", 18U));
    test->Expect(
        !Parse(sdk::IngressKind::SzTick, arguments)
             .success,
        "embedded NUL bytes are rejected");

    const std::array<std::string_view, 1U> help{{
        "--help",
    }};
    const apps::IngressServiceParseResult help_result =
        apps::ParseIngressServiceArguments(
            sdk::IngressKind::ShTick, help);
    test->Expect(
        help_result.show_help &&
            !help_result.success &&
            help_result.error.empty(),
        "standalone help is distinguished from a parse failure");

    const std::string usage =
        apps::IngressServiceUsage(
            sdk::IngressKind::SzTick);
    test->Expect(
        usage.find("runtime-only-secret") ==
                std::string::npos &&
            usage.find("tcp://feed.example:1234") ==
                std::string::npos,
        "usage contains syntax and defaults, not runtime values");
}

void CheckWatchdogMapping(TestContext* test) {
    apps::WatchdogSettings settings;
    std::string error;
    test->Expect(
        apps::ParseWatchdogEnvironment(
            std::nullopt,
            std::optional<std::string_view>("invalid"),
            &settings,
            &error) &&
            !settings.enabled,
        "WATCHDOG_USEC is irrelevant without NOTIFY_SOCKET");
    test->Expect(
        apps::ParseWatchdogEnvironment(
            std::optional<std::string_view>(
                "/run/systemd/notify"),
            std::nullopt,
            &settings,
            &error) &&
            !settings.enabled,
        "an absent WATCHDOG_USEC cleanly disables watchdog notifications");
    test->Expect(
        apps::ParseWatchdogEnvironment(
            std::optional<std::string_view>(
                "/run/systemd/notify"),
            std::optional<std::string_view>("11"),
            &settings,
            &error) &&
            settings.enabled &&
            settings.notify_interval ==
                std::chrono::microseconds(5),
        "an odd WATCHDOG_USEC maps to a period strictly below half");
    test->Expect(
        apps::ParseWatchdogEnvironment(
            std::optional<std::string_view>(
                "/run/systemd/notify"),
            std::optional<std::string_view>("10"),
            &settings,
            &error) &&
            settings.enabled &&
            settings.notify_interval ==
                std::chrono::microseconds(4),
        "an even WATCHDOG_USEC also maps strictly below half");

    for (const std::string_view invalid :
         {"", "0", "1", "2", "+10", "10us",
          "18446744073709551615"}) {
        test->Expect(
            !apps::ParseWatchdogEnvironment(
                std::optional<std::string_view>(
                    "/run/systemd/notify"),
                std::optional<std::string_view>(invalid),
                &settings,
                &error),
            "malformed, zero or unrepresentable WATCHDOG_USEC is rejected");
    }

    const std::string notify_with_nul(
        "/run\0notify", 11U);
    test->Expect(
        !apps::ParseWatchdogEnvironment(
            std::optional<std::string_view>(
                notify_with_nul),
            std::optional<std::string_view>("10"),
            &settings,
            &error),
        "NOTIFY_SOCKET with an embedded NUL is rejected");
    test->Expect(
        !apps::ParseWatchdogEnvironment(
            std::optional<std::string_view>(
                "/run/systemd/notify"),
            std::optional<std::string_view>("10"),
            nullptr,
            &error),
        "a null watchdog output is rejected");
}

void CheckReadinessDecisions(TestContext* test) {
    using Decision = apps::ReadinessDecision;
    test->Expect(
        apps::EvaluateReadiness(
            false, 0U, 0U, 0U, 0U) ==
            Decision::Waiting,
        "absence of control and market evidence remains not ready");
    test->Expect(
        apps::EvaluateReadiness(
            false, 0U, 3U, 0U, 0U) ==
            Decision::BecameReady,
        "complete first evidence causes exactly one READY transition");
    test->Expect(
        apps::EvaluateReadiness(
            true, 3U, 3U, 0U, 0U) ==
            Decision::RemainsReady,
        "stable complete evidence retains readiness");
    test->Expect(
        apps::EvaluateReadiness(
            false, 0U, 0U, 1U, 0U) ==
                Decision::FailedBeforeReady &&
            apps::EvaluateReadiness(
                false, 0U, 0U, 0U, 0x2U) ==
                Decision::FailedBeforeReady &&
            apps::EvaluateReadiness(
                false, 0U, 3U, 1U, 0U) ==
                Decision::FailedBeforeReady &&
            apps::EvaluateReadiness(
                false, 0U, 3U, 0U, 0x2U) ==
                Decision::FailedBeforeReady,
        "failed logon and non-OK required status take precedence "
        "over the first READY");
    test->Expect(
        apps::EvaluateReadiness(
            true, 3U, 0U, 0U, 0U) ==
                Decision::LostAfterReady &&
            apps::EvaluateReadiness(
                true, 3U, 3U, 1U, 0U) ==
                Decision::LostAfterReady &&
            apps::EvaluateReadiness(
                true, 3U, 3U, 0U, 0x1U) ==
                Decision::LostAfterReady &&
            apps::EvaluateReadiness(
                true, 3U, 4U, 0U, 0U) ==
                Decision::LostAfterReady,
        "current, cumulative, or replacement-generation readiness loss "
        "after READY is terminal");
}

void CheckCredentialSourcePolicy(TestContext* test) {
    TemporaryCredential credential;
    ScopedEnvironment credentials_directory(
        "CREDENTIALS_DIRECTORY",
        credential.directory());
    ScopedEnvironment notify_socket(
        "NOTIFY_SOCKET", std::nullopt);
    ScopedEnvironment watchdog_usec(
        "WATCHDOG_USEC", std::nullopt);

    std::vector<std::string> arguments =
        CompleteArguments();
    SetTemporaryOutputPaths(
        &arguments, credential);
    RemoveOption(&arguments, "--credential-path");
    SetOptionValue(
        &arguments,
        "--endpoint-contract",
        credential.contract_path());
    SetOptionValue(
        &arguments,
        "--endpoint-contract-sha256",
        credential.contract_sha256());
    SetOptionValue(
        &arguments,
        "--baseline",
        credential.directory() +
            "/runtime-systemd-secret");
    SetOptionValue(
        &arguments,
        "--archive",
        credential.directory() + "/missing-archive");
    SetOptionValue(
        &arguments,
        "--library",
        credential.directory() + "/missing-library");

    std::string standard_error;
    const int systemd_result =
        RunService(arguments, &standard_error);
    test->Expect(
        systemd_result == 1 &&
            standard_error.find(
                "full vendor preflight failed") !=
                std::string::npos &&
            standard_error.find(
                "credential loading failed") ==
                std::string::npos,
        "an effective-UID-owned 0400 systemd credential "
        "passes credential policy and reaches full preflight");
    test->Expect(
        standard_error.find(
            "runtime-systemd-secret") ==
            std::string::npos,
        "service failures never print credential bytes");

    if (::chmod(credential.path().c_str(), 0600) != 0) {
        throw std::runtime_error(
            std::string("chmod credential failed: ") +
            std::strerror(errno));
    }
    const int wrong_mode_result =
        RunService(arguments, &standard_error);
    test->Expect(
        wrong_mode_result == 1 &&
            standard_error.find(
                "credential loading failed") !=
                std::string::npos &&
            standard_error.find(
                "runtime-systemd-secret") ==
                std::string::npos,
        "systemd credentials require exact 0400 mode without leaking bytes");
    if (::chmod(credential.path().c_str(), 0400) != 0) {
        throw std::runtime_error(
            std::string("restore credential mode failed: ") +
            std::strerror(errno));
    }

    if (::geteuid() != 0) {
        arguments = CompleteArguments();
        SetTemporaryOutputPaths(
            &arguments, credential);
        SetOptionValue(
            &arguments,
            "--endpoint-contract",
            credential.contract_path());
        SetOptionValue(
            &arguments,
            "--endpoint-contract-sha256",
            credential.contract_sha256());
        SetOptionValue(
            &arguments,
            "--credential-path",
            credential.path());
        SetOptionValue(
            &arguments,
            "--baseline",
            credential.directory() +
                "/missing-baseline");
        SetOptionValue(
            &arguments,
            "--archive",
            credential.directory() +
                "/missing-archive");
        SetOptionValue(
            &arguments,
            "--library",
            credential.directory() +
                "/missing-library");
        const int explicit_result =
            RunService(arguments, &standard_error);
        test->Expect(
            explicit_result == 1 &&
                standard_error.find(
                    "credential loading failed") !=
                    std::string::npos,
            "an explicit credential file remains root-owned-only");
        test->Expect(
            standard_error.find(
                "runtime-systemd-secret") ==
                std::string::npos,
            "explicit credential rejection never prints its bytes");
    }
}

}  // namespace

int main() {
    TestContext test;
    try {
        CheckFourKindDefaults(&test);
        CheckCompleteMapping(&test);
        CheckOptionalDefaults(&test);
        CheckStrictRejection(&test);
        CheckWatchdogMapping(&test);
        CheckReadinessDecisions(&test);
        CheckStableServiceGlueAndFinalMetricsDrain(&test);
        CheckCredentialSourcePolicy(&test);
    } catch (const std::exception& exception) {
        std::cerr << "FAIL: test setup raised: "
                  << exception.what() << '\n';
        ++test.failures;
    }

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " phase-1 service config test(s) failed\n";
        return 1;
    }
    std::cout
        << "phase-1 service config checks passed\n";
    return 0;
}
