#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace l2flow::baseline {

// Operational resource bound for capturing a candidate SDK shared object.
// This is deliberately a maximum rather than an exact artifact size so a
// compatible vendor library can be upgraded without changing the baseline.
inline constexpr std::uint64_t kMaximumSdkSharedLibraryBytes =
    1ULL * 1024ULL * 1024ULL * 1024ULL;

struct MessageKey {
    std::uint8_t service_id = 0;
    std::uint16_t service_version = 0;
    std::uint16_t message_id = 0;

    friend bool operator==(const MessageKey&, const MessageKey&) = default;
};

enum class SubscriptionPolicy {
    Required,
    Optional,
    Forbidden,
};

struct MessageContract {
    std::string_view cpp_type;
    MessageKey key;
    SubscriptionPolicy policy = SubscriptionPolicy::Required;
};

struct MemberLayout {
    std::string_view name;
    std::size_t offset = 0;
};

struct TypeLayout {
    std::string_view name;
    std::size_t size = 0;
    std::size_t alignment = 0;
    std::span<const MemberLayout> members;
};

struct NumericConstant {
    std::string_view name;
    std::uint64_t value = 0;
};

struct VendorBaseline {
    std::uint32_t schema_version = 0;
    std::uint32_t sdk_version = 0;
    std::string_view sdk_archive_sha256;
    std::span<const std::string_view> elf_needed;
    // Imported symbol-version compatibility contract. The four maxima are
    // the compatibility ceilings implied by DT_VERNEED.
    std::span<const std::string_view> required_symbol_versions;
    std::string_view glibc_version_max;
    std::string_view glibcxx_version_max;
    std::string_view cxxabi_version_max;
    std::string_view libgcc_version_max;
    // Numeric protocol constants are frozen independently of the vendor
    // headers so an enum/literal drift is reported before the SDK is loaded.
    std::span<const NumericConstant> protocol_constants;
    std::span<const TypeLayout> abi_types;
    std::span<const MessageContract> messages;
};

// Approved constants are compiled into the executable. The JSON file is an
// exact, human-readable rendering of these constants, not a source of values
// that an attacker or accidental edit can relax at runtime.
const VendorBaseline& ApprovedVendorBaseline() noexcept;
const std::string& ApprovedVendorBaselineJson();

bool VerifyApprovedBaselineFile(const std::filesystem::path& path,
                                std::string* error) noexcept;

struct ElfMetadata {
    std::uint64_t file_size = 0;
    std::uint8_t elf_class = 0;
    std::uint8_t data_encoding = 0;
    std::uint16_t object_type = 0;
    std::uint16_t machine = 0;
    std::string build_id;
    std::optional<std::string> soname;
    std::vector<std::string> needed;
    std::vector<std::string> required_symbol_versions;
};

// Parses constrained ELF64 structure, notes, and dynamic metadata/version
// requirements. It never invokes a shell command or loads the object.
bool InspectElfFile(const std::filesystem::path& path,
                    ElfMetadata* metadata,
                    std::string* error) noexcept;

struct CheckResult {
    std::string id;
    bool passed = false;
    std::string expected;
    std::string actual;
    std::string detail;
};

struct PreflightReport {
    std::string mode;
    bool runtime_probe_attempted = false;
    std::vector<CheckResult> checks;

    bool passed() const noexcept;
};

struct PreflightPaths {
    std::filesystem::path baseline_json;
    std::filesystem::path sdk_archive;
    std::filesystem::path shared_library;
};

// Full startup gate. Runtime loading is attempted only after the immutable
// baseline file, archive, safely captured shared library, ELF compatibility,
// and compiled ABI all pass. A missing SDK archive is therefore an
// intentional hard failure.
PreflightReport RunVendorPreflight(const PreflightPaths& paths);

// Component gate for exercising DllCreateIOManager when the SDK archive is not
// present in a developer checkout. It safely snapshots and parses the
// candidate shared library and checks the compiled ABI before dlopen().
PreflightReport RunApprovedLibraryRuntimePreflight(
    const std::filesystem::path& shared_library);

// Variant for a caller that already owns a regular-file descriptor. The
// function copies the descriptor's bytes into a sealed immutable memfd before
// any size, ELF, ABI, or runtime check. It never closes or changes the caller's fd.
PreflightReport RunApprovedLibraryRuntimePreflightForOpenFd(int fd);

// Loader-only continuation for a descriptor that already satisfies the
// SealedFileSnapshot invariants. The caller retains the descriptor, and every
// preflight operation uses that exact immutable fd. This permits final dlopen
// to retain and use the very same checked snapshot without making a second
// copy.
PreflightReport RunApprovedLibraryRuntimePreflightForSealedSnapshotFd(
    int fd);

// Checks MDL_VERSION, required layouts/offsets, and required/optional/forbidden
// message keys against numeric constants compiled independently of the headers.
std::vector<CheckResult> CheckCompiledVendorAbi();

// `differences_only` emits only failed checks and is the command-line diff
// representation used by mdl_abi_preflight --diff-only.
std::string PreflightReportJson(const PreflightReport& report,
                                bool differences_only = false);

}  // namespace l2flow::baseline
