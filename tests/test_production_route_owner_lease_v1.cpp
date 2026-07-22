#include "l2flow/route/production_route_controller_v1.h"
#include "l2flow/route/production_route_owner_lease_v1.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <linux/filter.h>
#include <linux/seccomp.h>
#include <signal.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

namespace canonical = l2flow::canonical;
namespace route = l2flow::route;

namespace {

struct TestContext final {
    int failures = 0;

    void Check(bool condition, std::string_view expression, int line) {
        if (!condition) {
            ++failures;
            std::cerr << "line " << line << ": " << expression
                      << " failed\n";
        }
    }
};

#define CHECK(test, expression) \
    (test)->Check((expression), #expression, __LINE__)

template <std::size_t Size>
std::array<std::byte, Size> Filled(std::uint8_t seed) {
    std::array<std::byte, Size> output{};
    for (std::size_t index = 0U; index < output.size(); ++index) {
        output[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(seed + index));
    }
    return output;
}

route::ProductionRouteManifestV1 MakeManifest() {
    route::ProductionRouteManifestV1 manifest{};
    manifest.state = route::ProductionRouteStateV1::kActive;
    manifest.generation = 1U;
    manifest.route_instance = Filled<16U>(0x11U);
    manifest.trade_date = 20260723U;
    manifest.registry_version = 9U;
    manifest.registry_sha256 = Filled<32U>(0x21U);
    manifest.schema_sha256 =
        canonical::CanonicalSchemaDescriptorSha256V1();
    manifest.build_sha256 = Filled<32U>(0x41U);
    manifest.config_sha256 = Filled<32U>(0x51U);
    for (std::size_t index = 0U; index < manifest.sources.size(); ++index) {
        auto& source = manifest.sources[index];
        source.source_stream_id =
            route::kProductionRouteSourceStreamIdsV1[index];
        source.capture_date = manifest.trade_date;
        source.stream_day_id = Filled<16U>(
            static_cast<std::uint8_t>(0x61U + index));
        source.writer_instance = Filled<16U>(
            static_cast<std::uint8_t>(0x71U + index));
        source.source_generation = 10U + index;
        source.canonical_generation = 20U + index;
        source.durable_ingress_sequence = 100U + index;
        source.durable_global_wal_pos = 1000U + index;
        canonical::ClockEpochIdentityV1 clock{};
        clock.algorithm = 1U;
        clock.digest = Filled<32U>(
            static_cast<std::uint8_t>(0x81U + index));
        source.clock_epoch_identity_sha256 =
            route::ComputeProductionRouteClockEpochIdentitySha256V1(clock);
    }
    manifest.endpoints.canonical = "/run/l2flow/canonical-live";
    manifest.endpoints.history = "/run/l2flow/history-live";
    manifest.endpoints.state = "/run/l2flow/state-live";
    manifest.endpoints.factor = "/run/l2flow/factor-live";
    return manifest;
}

class TemporaryDirectory final {
public:
    TemporaryDirectory() {
        std::array<char, 64U> pattern{};
        constexpr std::string_view prefix =
            "/tmp/l2flow-route-owner-test-XXXXXX";
        std::copy(prefix.begin(), prefix.end(), pattern.begin());
        char* const created = ::mkdtemp(pattern.data());
        if (created != nullptr) {
            path_ = created;
            static_cast<void>(::chmod(path_.c_str(), 0700));
            descriptor_ = ::open(
                path_.c_str(),
                O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
        }
    }

    ~TemporaryDirectory() {
        if (descriptor_ >= 0) {
            static_cast<void>(::close(descriptor_));
        }
        if (!path_.empty()) {
            std::error_code ignored;
            std::filesystem::remove_all(path_, ignored);
        }
    }

    [[nodiscard]] bool valid() const noexcept {
        return descriptor_ >= 0;
    }
    [[nodiscard]] int fd() const noexcept {
        return descriptor_;
    }
    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
    int descriptor_ = -1;
};

[[nodiscard]] route::ProductionRoutePathIdentityGuardV1 PathGuard(
    const TemporaryDirectory& route_directory,
    const TemporaryDirectory& canonical_directory) {
    return route::ProductionRoutePathIdentityGuardV1{
        canonical_directory.fd(),
        route_directory.path().string(),
        canonical_directory.path().string()};
}

[[nodiscard]] route::ProductionRoutePathIdentityGuardV1 PathGuard(
    const TemporaryDirectory& directory) {
    return PathGuard(directory, directory);
}

[[nodiscard]] bool ReplaceDirectoryPath(
    const std::filesystem::path& path,
    const std::filesystem::path& displaced) noexcept {
    if (::rename(path.c_str(), displaced.c_str()) != 0) {
        return false;
    }
    if (::mkdir(path.c_str(), 0700) == 0) {
        return true;
    }
    static_cast<void>(::rename(displaced.c_str(), path.c_str()));
    return false;
}

[[nodiscard]] bool RestoreDirectoryPath(
    const std::filesystem::path& path,
    const std::filesystem::path& displaced) noexcept {
    return ::rmdir(path.c_str()) == 0 &&
           ::rename(displaced.c_str(), path.c_str()) == 0;
}

[[nodiscard]] bool WriteByte(int descriptor) noexcept {
    const std::byte value{0x5aU};
    for (;;) {
        const ssize_t written = ::write(descriptor, &value, 1U);
        if (written == 1) {
            return true;
        }
        if (written < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
}

[[nodiscard]] bool ReadByte(int descriptor) noexcept {
    std::byte value{};
    for (;;) {
        const ssize_t count = ::read(descriptor, &value, 1U);
        if (count == 1) {
            return value == std::byte{0x5aU};
        }
        if (count < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
}

[[nodiscard]] bool WriteMarker(
    int descriptor,
    std::byte marker) noexcept {
    for (;;) {
        const ssize_t written = ::write(descriptor, &marker, 1U);
        if (written == 1) {
            return true;
        }
        if (written < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
}

[[nodiscard]] bool ReadMarker(
    int descriptor,
    std::byte* marker) noexcept {
    if (marker == nullptr) {
        return false;
    }
    for (;;) {
        const ssize_t count = ::read(descriptor, marker, 1U);
        if (count == 1) {
            return true;
        }
        if (count < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
}

[[nodiscard]] bool ExternalReaderObservesLiveRoute(
    int directory_fd) noexcept {
    int result_pipe[2]{};
    if (::pipe2(result_pipe, O_CLOEXEC) != 0) {
        return false;
    }
    const pid_t child = ::fork();
    if (child == 0) {
        static_cast<void>(::close(result_pipe[0]));
        const auto live =
            route::ReadLiveProductionRouteV1At(directory_fd);
        ::_exit(live.authoritative() && WriteByte(result_pipe[1])
                    ? 0
                    : 3);
    }
    static_cast<void>(::close(result_pipe[1]));
    if (child < 0) {
        static_cast<void>(::close(result_pipe[0]));
        return false;
    }
    const bool observed = ReadByte(result_pipe[0]);
    static_cast<void>(::close(result_pipe[0]));
    int status = 0;
    return ::waitpid(child, &status, 0) == child && observed &&
           WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

[[nodiscard]] bool CreateArtifact(
    int directory_fd,
    std::string_view name) noexcept {
    const int descriptor = ::openat(
        directory_fd, name.data(),
        O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
        0600);
    if (descriptor < 0) {
        return false;
    }
    const bool written = WriteByte(descriptor);
    const bool synced = written && ::fsync(descriptor) == 0;
    const bool closed = ::close(descriptor) == 0;
    return synced && closed && ::fsync(directory_fd) == 0;
}

[[nodiscard]] bool DenyPidFdOpenForCurrentProcess() noexcept {
#if defined(__linux__) && defined(SYS_pidfd_open)
    const struct sock_filter instructions[] = {
        BPF_STMT(
            BPF_LD | BPF_W | BPF_ABS,
            static_cast<std::uint32_t>(
                offsetof(struct seccomp_data, nr))),
        BPF_JUMP(
            BPF_JMP | BPF_JEQ | BPF_K,
            static_cast<std::uint32_t>(SYS_pidfd_open),
            0U,
            1U),
        BPF_STMT(
            BPF_RET | BPF_K,
            SECCOMP_RET_ERRNO |
                (static_cast<std::uint32_t>(EPERM) & SECCOMP_RET_DATA)),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW),
    };
    struct sock_fprog program {};
    program.len = static_cast<unsigned short>(
        sizeof(instructions) / sizeof(instructions[0]));
    program.filter = const_cast<struct sock_filter*>(instructions);
    return ::prctl(PR_SET_NO_NEW_PRIVS, 1UL, 0UL, 0UL, 0UL) == 0 &&
           ::prctl(
               PR_SET_SECCOMP,
               static_cast<unsigned long>(SECCOMP_MODE_FILTER),
               &program) == 0;
#else
    return false;
#endif
}

[[nodiscard]] bool NamedEntryMissing(
    int directory_fd,
    std::string_view name) noexcept {
    struct stat status {};
    return ::fstatat(
               directory_fd,
               name.data(),
               &status,
               AT_SYMLINK_NOFOLLOW) != 0 &&
           errno == ENOENT;
}

[[nodiscard]] bool FreshOwnerArtifactsMissing(int directory_fd) noexcept {
    return NamedEntryMissing(
               directory_fd,
               route::kProductionRouteOwnerLockFilenameV1) &&
           NamedEntryMissing(
               directory_fd,
               route::kProductionRouteOwnerMetadataFilenameV1) &&
           NamedEntryMissing(
               directory_fd,
               route::kProductionRouteOwnerMetadataTemporaryFilenameV1);
}

struct BlockingFreshPublish final {
    std::mutex mutex;
    std::condition_variable condition;
    bool entered = false;
    bool release = false;

    static bool Hook(
        void* context,
        route::ProductionRouteStoreOperationV1 operation) noexcept {
        auto* const state = static_cast<BlockingFreshPublish*>(context);
        if (state == nullptr ||
            operation !=
                route::ProductionRouteStoreOperationV1::
                    kAfterTemporaryOpen) {
            return true;
        }
        std::unique_lock<std::mutex> lock(state->mutex);
        state->entered = true;
        state->condition.notify_all();
        state->condition.wait(
            lock, [state]() noexcept { return state->release; });
        return true;
    }
};

struct OneShotStoreFault final {
    route::ProductionRouteStoreOperationV1 operation =
        route::ProductionRouteStoreOperationV1::kAfterRename;
    bool fired = false;

    static bool Hook(
        void* context,
        route::ProductionRouteStoreOperationV1 operation) noexcept {
        auto* const fault = static_cast<OneShotStoreFault*>(context);
        if (fault == nullptr || fault->fired ||
            fault->operation != operation) {
            return true;
        }
        fault->fired = true;
        return false;
    }
};

[[nodiscard]] bool ReplaceCurrentRoute(
    int directory_fd,
    const route::ProductionRouteManifestV1& manifest) noexcept {
    constexpr std::string_view replacement_name =
        ".production-route-v1.controller-conflict";
    std::vector<std::byte> encoded;
    if (route::EncodeProductionRouteManifestV1(manifest, &encoded) !=
        route::ProductionRouteManifestErrorV1::kNone) {
        return false;
    }
    const int descriptor = ::openat(
        directory_fd, replacement_name.data(),
        O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
        0600);
    if (descriptor < 0) {
        return false;
    }
    std::size_t offset = 0U;
    bool written = true;
    while (offset < encoded.size()) {
        const ssize_t count = ::pwrite(
            descriptor, encoded.data() + offset,
            encoded.size() - offset,
            static_cast<off_t>(offset));
        if (count > 0) {
            offset += static_cast<std::size_t>(count);
            continue;
        }
        if (count < 0 && errno == EINTR) {
            continue;
        }
        written = false;
        break;
    }
    const bool file_synced = written && ::fsync(descriptor) == 0;
    const bool closed = ::close(descriptor) == 0;
    if (!file_synced || !closed) {
        static_cast<void>(::unlinkat(
            directory_fd, replacement_name.data(), 0));
        return false;
    }
    if (::renameat(
            directory_fd, replacement_name.data(),
            directory_fd,
            route::kProductionRouteFilenameV1.data()) != 0) {
        static_cast<void>(::unlinkat(
            directory_fd, replacement_name.data(), 0));
        return false;
    }
    return ::fsync(directory_fd) == 0;
}

void TestFreshRejectsExistingRouteArtifacts(TestContext* test) {
    TemporaryDirectory current;
    CHECK(test, current.valid());
    if (current.valid()) {
        CHECK(test, route::PublishProductionRouteManifestV1At(
                        current.fd(), MakeManifest()).ok());
        route::ProductionRouteControllerV1 controller(
            current.fd(), MakeManifest(),
            route::ProductionRouteControllerPolicyV1::kFreshOnly,
            PathGuard(current));
        const auto rejected = controller.PublishActive();
        CHECK(test, rejected.error ==
                        route::ProductionRouteControllerErrorV1::
                            kStoreFailure);
        CHECK(test, rejected.publish_result.error ==
                        route::ProductionRouteStoreErrorV1::
                            kFreshArtifactPresent);
        CHECK(test, FreshOwnerArtifactsMissing(current.fd()));
    }

    TemporaryDirectory temporary;
    CHECK(test, temporary.valid());
    if (temporary.valid()) {
        CHECK(test, CreateArtifact(
                        temporary.fd(),
                        route::kProductionRouteTemporaryFilenameV1));
        route::ProductionRouteControllerV1 controller(
            temporary.fd(), MakeManifest(),
            route::ProductionRouteControllerPolicyV1::kFreshOnly,
            PathGuard(temporary));
        const auto rejected = controller.PublishActive();
        CHECK(test, rejected.error ==
                        route::ProductionRouteControllerErrorV1::
                            kStoreFailure);
        CHECK(test, rejected.publish_result.error ==
                        route::ProductionRouteStoreErrorV1::
                            kFreshArtifactPresent);
        CHECK(test, NamedEntryMissing(
                        temporary.fd(),
                        route::kProductionRouteFilenameV1));
        CHECK(test, FreshOwnerArtifactsMissing(temporary.fd()));
    }
}

void TestFreshRejectsExistingOwnerArtifacts(TestContext* test) {
    constexpr std::array<std::string_view, 3U> artifacts{{
        route::kProductionRouteOwnerLockFilenameV1,
        route::kProductionRouteOwnerMetadataFilenameV1,
        route::kProductionRouteOwnerMetadataTemporaryFilenameV1}};
    for (const std::string_view artifact : artifacts) {
        TemporaryDirectory directory;
        CHECK(test, directory.valid());
        if (!directory.valid()) {
            continue;
        }
        CHECK(test, CreateArtifact(directory.fd(), artifact));
        route::ProductionRouteControllerV1 controller(
            directory.fd(), MakeManifest(),
            route::ProductionRouteControllerPolicyV1::kFreshOnly,
            PathGuard(directory));
        const auto rejected = controller.PublishActive();
        CHECK(test, rejected.error ==
                        route::ProductionRouteControllerErrorV1::
                            kOwnerLeaseFailure);
        CHECK(test, rejected.owner_lease_error ==
                        route::ProductionRouteOwnerLeaseErrorV1::
                            kFreshArtifactPresent);
        CHECK(test, rejected.publish_result.error ==
                        route::ProductionRouteStoreErrorV1::
                            kFreshPreparationFailure);
        CHECK(test, NamedEntryMissing(
                        directory.fd(),
                        route::kProductionRouteFilenameV1));
        CHECK(test, NamedEntryMissing(
                        directory.fd(),
                        route::kProductionRouteTemporaryFilenameV1));
    }
}

void TestFreshRejectsReplacedPublicationPaths(TestContext* test) {
    for (const bool replace_route : {true, false}) {
        TemporaryDirectory route_directory;
        TemporaryDirectory canonical_directory;
        CHECK(test, route_directory.valid());
        CHECK(test, canonical_directory.valid());
        if (!route_directory.valid() || !canonical_directory.valid()) {
            continue;
        }
        const std::filesystem::path& replaced = replace_route
            ? route_directory.path()
            : canonical_directory.path();
        std::filesystem::path displaced = replaced;
        displaced += ".retained";
        const bool path_replaced =
            ReplaceDirectoryPath(replaced, displaced);
        CHECK(test, path_replaced);
        if (!path_replaced) {
            continue;
        }
        {
            route::ProductionRouteControllerV1 controller(
                route_directory.fd(), MakeManifest(),
                route::ProductionRouteControllerPolicyV1::kFreshOnly,
                PathGuard(route_directory, canonical_directory));
            const auto rejected = controller.PublishActive();
            CHECK(test, rejected.error ==
                            route::ProductionRouteControllerErrorV1::
                                kPathIdentityMismatch);
            CHECK(test, rejected.system_error_number == ESTALE);
            CHECK(test, rejected.publish_result.error ==
                            route::ProductionRouteStoreErrorV1::
                                kFreshPreparationFailure);
            CHECK(test, NamedEntryMissing(
                            route_directory.fd(),
                            route::kProductionRouteFilenameV1));
            CHECK(test, NamedEntryMissing(
                            route_directory.fd(),
                            route::kProductionRouteTemporaryFilenameV1));
            CHECK(test, FreshOwnerArtifactsMissing(route_directory.fd()));
        }
        CHECK(test, RestoreDirectoryPath(replaced, displaced));
    }
}

void TestFreshOwnerPrecedesActiveWithoutAuthority(TestContext* test) {
    TemporaryDirectory directory;
    CHECK(test, directory.valid());
    if (!directory.valid()) {
        return;
    }
    BlockingFreshPublish blocked;
    route::ProductionRouteStoreOptionsV1 options{};
    options.operation_hook = &BlockingFreshPublish::Hook;
    options.operation_hook_context = &blocked;
    route::ProductionRouteControllerV1 controller(
        directory.fd(), MakeManifest(),
        route::ProductionRouteControllerPolicyV1::kFreshOnly,
        PathGuard(directory));
    route::ProductionRouteControllerResultV1 published{};
    std::thread publisher([&]() {
        published = controller.PublishActive(options);
    });
    bool entered = false;
    {
        std::unique_lock<std::mutex> lock(blocked.mutex);
        entered = blocked.condition.wait_for(
            lock, std::chrono::seconds{2},
            [&blocked]() noexcept { return blocked.entered; });
    }
    CHECK(test, entered);
    if (entered) {
        CHECK(test, NamedEntryMissing(
                        directory.fd(),
                        route::kProductionRouteFilenameV1));
        CHECK(test, !NamedEntryMissing(
                        directory.fd(),
                        route::kProductionRouteTemporaryFilenameV1));
        CHECK(test, !NamedEntryMissing(
                        directory.fd(),
                        route::kProductionRouteOwnerMetadataFilenameV1));
        const auto live =
            route::ReadLiveProductionRouteV1At(directory.fd());
        CHECK(test, !live.authoritative());
        CHECK(test, live.error ==
                        route::ProductionRouteOwnerLeaseErrorV1::
                            kRouteNotActive);
    }
    {
        std::lock_guard<std::mutex> lock(blocked.mutex);
        blocked.release = true;
    }
    blocked.condition.notify_all();
    publisher.join();
    CHECK(test, published.authoritative());
    const auto repeated = controller.PublishActive();
    CHECK(test, repeated.authoritative());
    CHECK(test, repeated.already_complete);
}

void TestFreshRenameUncertaintyConvergesAndCanPublishFatal(
    TestContext* test) {
    constexpr std::array<route::ProductionRouteStoreOperationV1, 4U>
        operations{{
            route::ProductionRouteStoreOperationV1::kAfterRename,
            route::ProductionRouteStoreOperationV1::kBeforeDirectorySync,
            route::ProductionRouteStoreOperationV1::kAfterDirectorySync,
            route::ProductionRouteStoreOperationV1::kBeforeReadback}};

    for (const route::ProductionRouteStoreOperationV1 operation :
         operations) {
        TemporaryDirectory directory;
        CHECK(test, directory.valid());
        if (!directory.valid()) {
            continue;
        }
        const route::ProductionRouteManifestV1 active = MakeManifest();
        route::ProductionRouteControllerV1 controller(
            directory.fd(), active,
            route::ProductionRouteControllerPolicyV1::kFreshOnly,
            PathGuard(directory));
        OneShotStoreFault fault;
        fault.operation = operation;
        route::ProductionRouteStoreOptionsV1 options;
        options.operation_hook = &OneShotStoreFault::Hook;
        options.operation_hook_context = &fault;

        const auto uncertain = controller.PublishActive(options);
        CHECK(test, fault.fired);
        CHECK(test, uncertain.error ==
                        route::ProductionRouteControllerErrorV1::
                            kStoreFailure);
        CHECK(test, uncertain.publish_result.error ==
                        route::ProductionRouteStoreErrorV1::
                            kInjectedFailure);
        CHECK(test, uncertain.publish_result.renamed);
        CHECK(test, !uncertain.authoritative());
        CHECK(test, ExternalReaderObservesLiveRoute(directory.fd()));

        const auto converged = controller.PublishActive();
        CHECK(test, converged.authoritative());
        CHECK(test, !converged.already_complete);
        CHECK(test, converged.publish_result.disposition ==
                        route::ProductionRoutePublishDispositionV1::
                            kAcceptedExisting);
        CHECK(test, converged.publish_result.file_synced);
        CHECK(test, converged.publish_result.directory_synced);

        const std::uint32_t reason_code =
            9100U + static_cast<std::uint32_t>(operation);
        const auto fatal = controller.PublishFatal(reason_code);
        CHECK(test, fatal.ok());
        CHECK(test, fatal.publish_result.state ==
                        route::ProductionRouteStateV1::kFatal);
        const auto persisted =
            route::ReadProductionRouteManifestV1At(directory.fd());
        CHECK(test, persisted.error ==
                        route::ProductionRouteStoreErrorV1::kRouteFatal);
        CHECK(test, persisted.manifest != nullptr);
        if (persisted.manifest != nullptr) {
            CHECK(test, persisted.manifest->fatal_reason_code ==
                            reason_code);
        }
    }
}

void TestFreshRenameUncertaintyRejectsConflictingCurrent(
    TestContext* test) {
    TemporaryDirectory directory;
    CHECK(test, directory.valid());
    if (!directory.valid()) {
        return;
    }
    const route::ProductionRouteManifestV1 active = MakeManifest();
    route::ProductionRouteControllerV1 controller(
        directory.fd(), active,
        route::ProductionRouteControllerPolicyV1::kFreshOnly,
        PathGuard(directory));
    OneShotStoreFault fault;
    route::ProductionRouteStoreOptionsV1 options;
    options.operation_hook = &OneShotStoreFault::Hook;
    options.operation_hook_context = &fault;
    const auto uncertain = controller.PublishActive(options);
    CHECK(test, fault.fired);
    CHECK(test, uncertain.publish_result.renamed);

    route::ProductionRouteManifestV1 conflict = active;
    conflict.endpoints.history = "/run/l2flow/history-conflict";
    CHECK(test, ReplaceCurrentRoute(directory.fd(), conflict));

    const auto rejected = controller.PublishActive();
    CHECK(test, rejected.error ==
                    route::ProductionRouteControllerErrorV1::kStoreFailure);
    CHECK(test, rejected.publish_result.error ==
                    route::ProductionRouteStoreErrorV1::
                        kGenerationConflict);
    CHECK(test, !rejected.authoritative());
    const auto fatal = controller.PublishFatal(9201U);
    CHECK(test, fatal.error ==
                    route::ProductionRouteControllerErrorV1::
                        kActiveNotPublished);
    const auto persisted =
        route::ReadProductionRouteManifestV1At(directory.fd());
    CHECK(test, persisted.ok());
    CHECK(test, persisted.manifest != nullptr);
    if (persisted.manifest != nullptr) {
        CHECK(test, *persisted.manifest == conflict);
    }
}

void TestFreshCrossProcessRace(TestContext* test) {
    TemporaryDirectory directory;
    CHECK(test, directory.valid());
    if (!directory.valid()) {
        return;
    }
    int ready[2]{};
    int start[2]{};
    int results[2]{};
    CHECK(test, ::pipe2(ready, O_CLOEXEC) == 0);
    CHECK(test, ::pipe2(start, O_CLOEXEC) == 0);
    CHECK(test, ::pipe2(results, O_CLOEXEC) == 0);

    const auto child_main = [&](std::uint8_t identity_seed) noexcept {
        static_cast<void>(::close(ready[0]));
        static_cast<void>(::close(start[1]));
        static_cast<void>(::close(results[0]));
        const int independent_directory = ::open(
            directory.path().c_str(),
            O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
        if (independent_directory < 0 || !WriteByte(ready[1]) ||
            !ReadByte(start[0])) {
            ::_exit(10);
        }
        auto manifest = MakeManifest();
        manifest.route_instance = Filled<16U>(identity_seed);
        route::ProductionRouteControllerV1 controller(
            independent_directory, std::move(manifest),
            route::ProductionRouteControllerPolicyV1::kFreshOnly,
            route::ProductionRoutePathIdentityGuardV1{
                independent_directory,
                directory.path().string(),
                directory.path().string()});
        static_cast<void>(::close(independent_directory));
        const auto result = controller.PublishActive();
        const std::byte marker = result.authoritative()
            ? std::byte{0x57U}
            : result.error ==
                          route::ProductionRouteControllerErrorV1::
                              kStoreFailure &&
                      result.publish_result.error ==
                          route::ProductionRouteStoreErrorV1::
                              kFreshArtifactPresent
                ? std::byte{0x52U}
                : std::byte{0x58U};
        const bool reported = WriteMarker(results[1], marker);
        ::_exit(reported ? 0 : 11);
    };

    const pid_t first = ::fork();
    CHECK(test, first >= 0);
    if (first == 0) {
        child_main(0xa1U);
    }
    const pid_t second = ::fork();
    CHECK(test, second >= 0);
    if (second == 0) {
        child_main(0xb1U);
    }
    static_cast<void>(::close(ready[1]));
    static_cast<void>(::close(start[0]));
    static_cast<void>(::close(results[1]));
    if (first < 0 || second < 0) {
        if (first > 0) {
            static_cast<void>(::kill(first, SIGKILL));
            static_cast<void>(::waitpid(first, nullptr, 0));
        }
        if (second > 0) {
            static_cast<void>(::kill(second, SIGKILL));
            static_cast<void>(::waitpid(second, nullptr, 0));
        }
        return;
    }
    CHECK(test, ReadByte(ready[0]));
    CHECK(test, ReadByte(ready[0]));
    CHECK(test, WriteByte(start[1]));
    CHECK(test, WriteByte(start[1]));
    std::array<std::byte, 2U> markers{};
    CHECK(test, ReadMarker(results[0], &markers[0U]));
    CHECK(test, ReadMarker(results[0], &markers[1U]));
    int first_status = 0;
    int second_status = 0;
    CHECK(test, ::waitpid(first, &first_status, 0) == first);
    CHECK(test, ::waitpid(second, &second_status, 0) == second);
    CHECK(test, WIFEXITED(first_status) && WEXITSTATUS(first_status) == 0);
    CHECK(test, WIFEXITED(second_status) && WEXITSTATUS(second_status) == 0);
    CHECK(test, static_cast<std::size_t>(std::count(
                    markers.begin(), markers.end(), std::byte{0x57U})) == 1U);
    CHECK(test, static_cast<std::size_t>(std::count(
                    markers.begin(), markers.end(), std::byte{0x52U})) == 1U);
}

void TestCrossProcessCrashRevocation(TestContext* test) {
    TemporaryDirectory directory;
    CHECK(test, directory.valid());
    if (!directory.valid()) {
        return;
    }
    int ready[2]{};
    CHECK(test, ::pipe2(ready, O_CLOEXEC) == 0);
    const pid_t child = ::fork();
    CHECK(test, child >= 0);
    if (child == 0) {
        static_cast<void>(::close(ready[0]));
        route::ProductionRouteControllerV1 controller(
            directory.fd(), MakeManifest());
        const auto published = controller.PublishActive();
        if (!published.authoritative() || !WriteByte(ready[1])) {
            ::_exit(2);
        }
        for (;;) {
            static_cast<void>(::pause());
        }
    }
    static_cast<void>(::close(ready[1]));
    if (child < 0 || !ReadByte(ready[0])) {
        static_cast<void>(::close(ready[0]));
        if (child > 0) {
            static_cast<void>(::kill(child, SIGKILL));
            static_cast<void>(::waitpid(child, nullptr, 0));
        }
        CHECK(test, false);
        return;
    }
    static_cast<void>(::close(ready[0]));

    route::LiveProductionRouteReadResultV1 live =
        route::ReadLiveProductionRouteV1At(directory.fd());
    CHECK(test, live.authoritative());
    CHECK(test, live.guard != nullptr);
    if (live.guard != nullptr) {
        CHECK(test, live.guard->Validate() ==
                        route::ProductionRouteOwnerLeaseErrorV1::kNone);
    }

    CHECK(test, ::kill(child, SIGKILL) == 0);
    int status = 0;
    CHECK(test, ::waitpid(child, &status, 0) == child);
    CHECK(test, WIFSIGNALED(status));
    CHECK(test, WTERMSIG(status) == SIGKILL);
    CHECK(test, live.guard != nullptr &&
                    live.guard->Validate() ==
                        route::ProductionRouteOwnerLeaseErrorV1::
                            kOwnerMissing);

    const route::ProductionRouteReadResultV1 audit =
        route::ReadProductionRouteManifestV1At(directory.fd());
    CHECK(test, audit.authoritative());
    live = route::ReadLiveProductionRouteV1At(directory.fd());
    CHECK(test, !live.authoritative());
    CHECK(test, live.error ==
                    route::ProductionRouteOwnerLeaseErrorV1::kOwnerMissing);
}

void TestSameProcessCapabilityBoundary(TestContext* test) {
    TemporaryDirectory directory;
    CHECK(test, directory.valid());
    if (!directory.valid()) {
        return;
    }
    route::ProductionRouteControllerV1 controller(
        directory.fd(), MakeManifest());
    CHECK(test, controller.PublishActive().authoritative());

    route::LiveProductionRouteReadResultV1 local =
        route::ReadLiveProductionRouteV1At(directory.fd());
    CHECK(test, !local.authoritative());
    CHECK(test, local.error ==
                    route::ProductionRouteOwnerLeaseErrorV1::kInvalidArgument);

    int result_pipe[2]{};
    CHECK(test, ::pipe2(result_pipe, O_CLOEXEC) == 0);
    const pid_t child = ::fork();
    CHECK(test, child >= 0);
    if (child == 0) {
        static_cast<void>(::close(result_pipe[0]));
        const auto external =
            route::ReadLiveProductionRouteV1At(directory.fd());
        ::_exit(external.authoritative() && WriteByte(result_pipe[1])
                    ? 0
                    : 3);
    }
    static_cast<void>(::close(result_pipe[1]));
    const bool child_observed_live = child > 0 && ReadByte(result_pipe[0]);
    static_cast<void>(::close(result_pipe[0]));
    int status = 0;
    if (child > 0) {
        CHECK(test, ::waitpid(child, &status, 0) == child);
    }
    CHECK(test, child_observed_live);
    CHECK(test, child > 0 && WIFEXITED(status) && WEXITSTATUS(status) == 0);

    route::ProductionRouteControllerV1 competing(
        directory.fd(), MakeManifest());
    const auto rejected = competing.PublishActive();
    CHECK(test, rejected.error ==
                    route::ProductionRouteControllerErrorV1::
                        kOwnerLeaseFailure);
    CHECK(test, rejected.owner_lease_error ==
                    route::ProductionRouteOwnerLeaseErrorV1::
                        kHeldByAnotherOwner);

    int verification_pipe[2]{};
    CHECK(test, ::pipe2(verification_pipe, O_CLOEXEC) == 0);
    const pid_t verifier = ::fork();
    CHECK(test, verifier >= 0);
    if (verifier == 0) {
        static_cast<void>(::close(verification_pipe[0]));
        const auto external =
            route::ReadLiveProductionRouteV1At(directory.fd());
        ::_exit(external.authoritative() && WriteByte(verification_pipe[1])
                    ? 0
                    : 4);
    }
    static_cast<void>(::close(verification_pipe[1]));
    const bool lock_survived =
        verifier > 0 && ReadByte(verification_pipe[0]);
    static_cast<void>(::close(verification_pipe[0]));
    status = 0;
    if (verifier > 0) {
        CHECK(test, ::waitpid(verifier, &status, 0) == verifier);
    }
    CHECK(test, lock_survived);
    CHECK(test, verifier > 0 && WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

void TestPidFdPolicyDenialPrecedesOwnerPublication(TestContext* test) {
    TemporaryDirectory directory;
    CHECK(test, directory.valid());
    if (!directory.valid()) {
        return;
    }

    const pid_t child = ::fork();
    CHECK(test, child >= 0);
    if (child == 0) {
        if (!DenyPidFdOpenForCurrentProcess()) {
            ::_exit(2);
        }
        route::ProductionRouteControllerV1 controller(
            directory.fd(), MakeManifest());
        const auto published = controller.PublishActive();
        const bool rejected =
            published.error == route::ProductionRouteControllerErrorV1::
                                   kOwnerLeaseFailure &&
            published.owner_lease_error ==
                route::ProductionRouteOwnerLeaseErrorV1::kUnsupported &&
            !published.authoritative();
        ::_exit(rejected ? 0 : 3);
    }

    int status = 0;
    if (child > 0) {
        CHECK(test, ::waitpid(child, &status, 0) == child);
    }
    CHECK(test, child > 0 && WIFEXITED(status) && WEXITSTATUS(status) == 0);
    const auto route_read =
        route::ReadProductionRouteManifestV1At(directory.fd());
    CHECK(test, route_read.error ==
                    route::ProductionRouteStoreErrorV1::kRouteNotFound);
    CHECK(test, NamedEntryMissing(
                    directory.fd(),
                    route::kProductionRouteOwnerLockFilenameV1));
    CHECK(test, NamedEntryMissing(
                    directory.fd(),
                    route::kProductionRouteOwnerMetadataFilenameV1));
}

}  // namespace

int main() {
    TestContext test;
    TestFreshRejectsExistingRouteArtifacts(&test);
    TestFreshRejectsExistingOwnerArtifacts(&test);
    TestFreshRejectsReplacedPublicationPaths(&test);
    TestFreshOwnerPrecedesActiveWithoutAuthority(&test);
    TestFreshRenameUncertaintyConvergesAndCanPublishFatal(&test);
    TestFreshRenameUncertaintyRejectsConflictingCurrent(&test);
    TestFreshCrossProcessRace(&test);
    TestCrossProcessCrashRevocation(&test);
    TestSameProcessCapabilityBoundary(&test);
    TestPidFdPolicyDenialPrecedesOwnerPublication(&test);
    if (test.failures != 0) {
        std::cerr << test.failures << " route owner lease checks failed\n";
        return 1;
    }
    std::cout << "production route owner lease checks passed\n";
    return 0;
}
