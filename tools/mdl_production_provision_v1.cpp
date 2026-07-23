#include "l2flow/apps/production_deployment_v1.h"
#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/ingress/raw_emergency_reserve_posix.h"
#include "l2flow/ingress/raw_reserve_coordinator.h"
#include "l2flow/sdk/subscription_manifest.h"

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace {

namespace apps = l2flow::apps;
namespace common = l2flow::common;
namespace ingress = l2flow::ingress;
namespace sdk = l2flow::sdk;

class OwnedFd final {
public:
    explicit OwnedFd(int value = -1) noexcept : value_(value) {}
    ~OwnedFd() {
        if (value_ >= 0) {
            static_cast<void>(::close(value_));
        }
    }
    OwnedFd(const OwnedFd&) = delete;
    OwnedFd& operator=(const OwnedFd&) = delete;
    OwnedFd(OwnedFd&& other) noexcept
        : value_(std::exchange(other.value_, -1)) {}
    OwnedFd& operator=(OwnedFd&& other) noexcept {
        if (this != &other) {
            if (value_ >= 0) {
                static_cast<void>(::close(value_));
            }
            value_ = std::exchange(other.value_, -1);
        }
        return *this;
    }
    [[nodiscard]] int get() const noexcept { return value_; }

private:
    int value_;
};

struct Options final {
    std::filesystem::path deployment_directory;
    common::Sha256Digest manifest_sha256{};
};

[[nodiscard]] bool ParseOptions(
    std::span<const std::string_view> arguments,
    Options* output,
    std::string* error) {
    if (output == nullptr || error == nullptr) {
        return false;
    }
    bool directory_seen = false;
    bool digest_seen = false;
    Options options{};
    for (std::size_t index = 0U; index < arguments.size(); ++index) {
        const std::string_view option = arguments[index];
        if (option != "--deployment-dir" &&
            option != "--manifest-sha256") {
            *error = "unknown option";
            return false;
        }
        if (index + 1U >= arguments.size()) {
            *error = "option is missing its value";
            return false;
        }
        const std::string_view value = arguments[++index];
        if (option == "--deployment-dir") {
            if (directory_seen) {
                *error = "duplicate --deployment-dir";
                return false;
            }
            directory_seen = true;
            options.deployment_directory = value;
        } else {
            if (digest_seen) {
                *error = "duplicate --manifest-sha256";
                return false;
            }
            digest_seen = true;
            std::string parse_error;
            if (!common::ParseSha256Hex(
                    value, &options.manifest_sha256, &parse_error)) {
                *error = "invalid --manifest-sha256";
                return false;
            }
        }
    }
    if (!directory_seen || !digest_seen ||
        !options.deployment_directory.is_absolute() ||
        options.deployment_directory.lexically_normal() !=
            options.deployment_directory) {
        *error = "normalized absolute deployment directory and digest are required";
        return false;
    }
    *output = std::move(options);
    return true;
}

[[nodiscard]] bool AddChecked(
    std::uint64_t value,
    std::uint64_t* accumulator) noexcept {
    if (accumulator == nullptr ||
        value > std::numeric_limits<std::uint64_t>::max() - *accumulator) {
        return false;
    }
    *accumulator += value;
    return true;
}

[[nodiscard]] common::Sha256Digest CatalogIdentity(
    const common::Sha256Digest& manifest_sha256) {
    constexpr std::string_view domain =
        "L2FLOW_PRODUCTION_LIVE_SAFE_STOP_CATALOG_DECLARATION_V1";
    common::Sha256Hasher hasher;
    const auto domain_bytes = std::as_bytes(std::span(domain));
    common::Sha256Digest result{};
    if (!hasher.Update(domain_bytes) ||
        !hasher.Update(manifest_sha256) ||
        !hasher.Finalize(&result)) {
        return {};
    }
    return result;
}

[[nodiscard]] ingress::RawReserveFreshScaffoldingV1 Registration(
    const apps::ProductionDeploymentV1& deployment,
    std::size_t source) {
    const auto& configured = deployment.sources.at(source);
    ingress::RawReserveFreshScaffoldingV1 result{};
    result.key.route.source_stream_id =
        sdk::GetIngressSpec(configured.kind).source_stream_id;
    result.key.route.capture_date = deployment.capture_date;
    result.key.stream_day_id = configured.stream_day_id;
    result.key.recovery_attempt_id = configured.recovery_attempt_id;
    result.writer_instance = configured.writer_instance;
    result.recovery_intent =
        ingress::ReserveRecoveryIntentV1::kResumeConnect;
    result.scaffolding_allocation_cap =
        configured.scaffolding_allocation_cap;
    result.safe_stop_template_id = configured.safe_stop_template_id;
    return result;
}

[[nodiscard]] bool ExactScaffolding(
    const apps::ProductionDeploymentV1& deployment,
    const ingress::ReserveCoordinatorStateV1& state) {
    if (state.selected_slot >= state.slots.size()) {
        return false;
    }
    const auto& slot = state.slots[state.selected_slot];
    if (slot.coordinator_state !=
            ingress::ReserveCoordinatorPhaseV1::kProvisioned ||
        slot.entry_count != deployment.sources.size()) {
        return false;
    }
    for (std::size_t source = 0U; source < deployment.sources.size(); ++source) {
        const auto expected = Registration(deployment, source);
        bool found = false;
        for (std::size_t index = 0U; index < slot.entry_count; ++index) {
            const auto& entry = slot.entries[index];
            if (entry.source_stream_id == expected.key.route.source_stream_id &&
                entry.capture_date == expected.key.route.capture_date &&
                entry.stream_day_id == expected.key.stream_day_id) {
                found =
                    entry.registry_status ==
                        ingress::ReserveRegistryStatusV1::kScaffolding &&
                    entry.recovery_origin ==
                        ingress::ReserveRecoveryOriginV1::kFreshInit &&
                    entry.recovery_intent == expected.recovery_intent &&
                    entry.writer_instance == expected.writer_instance &&
                    entry.executor_or_recovery_attempt ==
                        expected.key.recovery_attempt_id &&
                    entry.grant_bytes ==
                        expected.scaffolding_allocation_cap &&
                    entry.safe_stop_template_id ==
                        expected.safe_stop_template_id;
                break;
            }
        }
        if (!found) {
            return false;
        }
    }
    return true;
}

}  // namespace

int main(int argc, char* argv[]) {
    try {
        std::vector<std::string_view> arguments;
        for (int index = 1; index < argc; ++index) {
            if (argv[index] == nullptr) {
                std::cerr << "null command-line argument\n";
                return 64;
            }
            arguments.emplace_back(argv[index]);
        }
        Options options{};
        std::string error;
        if (!ParseOptions(arguments, &options, &error)) {
            std::cerr
                << "Usage: mdl-production-provision-v1 --deployment-dir DIR "
                   "--manifest-sha256 HEX\n"
                << error << '\n';
            return 64;
        }
        auto loaded = apps::LoadProductionDeploymentManifestV1(
            options.deployment_directory, options.manifest_sha256);
        if (!loaded.ok()) {
            std::cerr << "deployment manifest rejected: "
                      << loaded.diagnostic << '\n';
            return 1;
        }
        const auto& deployment = loaded.deployment;
        OwnedFd raw(::open(
            deployment.raw_root.c_str(),
            O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
        struct stat status {};
        if (raw.get() < 0 || ::fstat(raw.get(), &status) != 0 ||
            !S_ISDIR(status.st_mode) || status.st_uid != ::geteuid() ||
            (status.st_mode & 07777U) != 0700U ||
            static_cast<std::uint64_t>(status.st_dev) !=
                deployment.coordinator_device_id) {
            std::cerr << "Raw root is not the manifest-pinned private directory\n";
            return 1;
        }

        ingress::ReserveCoordinatorStateV1 bootstrap{};
        if (!common::GenerateIdentity128(&bootstrap.header.reserve_state_uuid)) {
            std::cerr << "cannot generate reserve-state UUID\n";
            return 1;
        }
        bootstrap.header.quota_identity_sha256 =
            deployment.coordinator_quota_sha256;
        bootstrap.header.mount_identity_sha256 =
            deployment.coordinator_mount_sha256;
        bootstrap.header.device_id = deployment.coordinator_device_id;
        for (const auto& source : deployment.sources) {
            if (!AddChecked(
                    source.scaffolding_allocation_cap,
                    &bootstrap.header.declared_releasable_bytes)) {
                std::cerr << "scaffolding caps overflow declared reserve bytes\n";
                return 1;
            }
        }
        bootstrap.header.allocation_quantum_bytes = 4096U;
        bootstrap.header.declared_inode_reserve_count = 64U;
        bootstrap.header.byte_probe_version = 1U;
        bootstrap.header.inode_probe_version = 1U;
        bootstrap.header.safe_stop_catalog_sha256 =
            CatalogIdentity(deployment.manifest_sha256);
        if (ingress::ComputeRawEmergencyReserveInventorySha256V1(
                bootstrap.header,
                &bootstrap.header.inode_inventory_sha256,
                &error) !=
            ingress::RawEmergencyReservePosixErrorV1::kNone) {
            std::cerr << "reserve inventory commitment failed: " << error << '\n';
            return 1;
        }
        ingress::ReserveStateSlotV1 slot{};
        slot.coordinator_state =
            ingress::ReserveCoordinatorPhaseV1::kProvisioned;
        slot.generation = 1U;
        slot.reserve_state_uuid = bootstrap.header.reserve_state_uuid;
        bootstrap.slots[0U] = slot;
        bootstrap.slots[1U] = slot;
        bootstrap.selected_slot = 0U;

        ingress::RawReserveCoordinatorLeaseMarkerV1 marker{};
        marker.coordinator_identity = deployment.coordinator_identity;
        marker.device_id = deployment.coordinator_device_id;
        marker.quota_identity_sha256 = deployment.coordinator_quota_sha256;
        marker.mount_identity_sha256 = deployment.coordinator_mount_sha256;

        ingress::RawReserveCoordinatorErrorV1 coordinator_error =
            ingress::RawReserveCoordinatorErrorV1::kNone;
        auto coordinator =
            ingress::PublishFreshRawReserveRegistryCoordinatorAtV1(
                raw.get(), marker, bootstrap, &coordinator_error, &error);
        if (coordinator == nullptr) {
            std::cerr << "fresh coordinator publication failed: "
                      << ingress::RawReserveCoordinatorErrorNameV1(
                             coordinator_error)
                      << ": " << error << '\n';
            return 1;
        }
        for (std::size_t source = 0U;
             source < deployment.sources.size(); ++source) {
            coordinator_error = coordinator->RegisterFreshScaffolding(
                Registration(deployment, source), &error);
            if (coordinator_error !=
                ingress::RawReserveCoordinatorErrorV1::kNone) {
                std::cerr << "SCAFFOLDING registration failed for source "
                          << source << ": "
                          << ingress::RawReserveCoordinatorErrorNameV1(
                                 coordinator_error)
                          << ": " << error << '\n';
                return 1;
            }
        }
        const auto state = coordinator->state();
        if (!ExactScaffolding(deployment, state)) {
            std::cerr << "published coordinator state failed exact readback\n";
            return 1;
        }
        std::cout
            << "{\"passed\":true,\"capture_date\":"
            << deployment.capture_date
            << ",\"entry_count\":4,\"selected_generation\":"
            << state.slots[state.selected_slot].generation
            << ",\"raw_device_id\":" << deployment.coordinator_device_id
            << ",\"declared_releasable_bytes\":"
            << bootstrap.header.declared_releasable_bytes
            << ",\"physical_emergency_reserve_inventory_provisioned\":false}"
            << '\n';
        return 0;
    } catch (const std::exception& exception) {
        std::cerr << "production provision failed: " << exception.what() << '\n';
        return 1;
    } catch (...) {
        std::cerr << "production provision failed unexpectedly\n";
        return 1;
    }
}
