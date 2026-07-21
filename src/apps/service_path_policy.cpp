#include "l2flow/apps/service_path_policy.h"

#include "l2flow/apps/ingress_service.h"
#include "l2flow/ops/metrics_textfile.h"
#include "l2flow/ops/stable_output_prefix.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace l2flow::apps {
namespace {

enum class PathAccess {
    Writable,
    SdkLogPrefix,
    ProtectedInput,
};

struct CheckedPath final {
    PathAccess access = PathAccess::ProtectedInput;
    std::filesystem::path lexical;
    std::filesystem::path resolved;
    bool exists = false;
};

bool ContainsAmbiguousComponent(
    const std::filesystem::path& path) {
    for (const std::filesystem::path& component : path) {
        if (component == "." || component == "..") {
            return true;
        }
    }
    return false;
}

bool IsPortableFilename(
    const std::filesystem::path& path) {
    const std::string filename =
        path.filename().native();
    return !filename.empty() &&
           std::all_of(
               filename.begin(),
               filename.end(),
               [](char character) {
                   const unsigned char value =
                       static_cast<unsigned char>(
                           character);
                   return (value >=
                               static_cast<unsigned char>('a') &&
                           value <=
                               static_cast<unsigned char>('z')) ||
                          (value >=
                               static_cast<unsigned char>('A') &&
                           value <=
                               static_cast<unsigned char>('Z')) ||
                          (value >=
                               static_cast<unsigned char>('0') &&
                           value <=
                               static_cast<unsigned char>('9')) ||
                          value ==
                              static_cast<unsigned char>('.') ||
                          value ==
                              static_cast<unsigned char>('_') ||
                          value ==
                              static_cast<unsigned char>('-');
               });
}

std::optional<CheckedPath> CheckPath(
    PathAccess access,
    const std::filesystem::path& path,
    bool require_existing_parent,
    std::string* error) {
    const std::string native = path.native();
    if (native.empty() ||
        native.find('\0') != std::string::npos ||
        !path.is_absolute() ||
        !path.has_filename() ||
        ContainsAmbiguousComponent(path) ||
        !IsPortableFilename(path)) {
        *error =
            "service paths must be absolute, NUL-free file names "
            "with portable ASCII basenames and no dot components";
        return std::nullopt;
    }
    if (access == PathAccess::Writable &&
        (l2flow::ops::
             IsSdkLogDirectoryMarkerFilename(
                 path.filename().native()) ||
         l2flow::ops::
             IsPrometheusTextfileLeaseFilename(
                 path.filename().native()))) {
        *error =
            "writable service path uses a reserved output-control name";
        return std::nullopt;
    }
    if (require_existing_parent) {
        std::error_code parent_error;
        const bool parent_is_directory =
            std::filesystem::is_directory(
                path.parent_path(),
                parent_error);
        if (parent_error ||
            !parent_is_directory) {
            *error =
                "every service path parent must already be a directory";
            return std::nullopt;
        }
    }

    CheckedPath checked;
    checked.access = access;
    checked.lexical = path.lexically_normal();

    std::error_code canonical_error;
    checked.resolved =
        std::filesystem::weakly_canonical(
            checked.lexical, canonical_error);
    if (canonical_error || checked.resolved.empty()) {
        *error =
            "service path cannot be resolved safely";
        return std::nullopt;
    }

    std::error_code exists_error;
    checked.exists =
        std::filesystem::exists(
            checked.lexical, exists_error);
    if (exists_error) {
        *error =
            "service path metadata cannot be inspected safely";
        return std::nullopt;
    }
    return checked;
}

bool SameExistingObject(
    const CheckedPath& left,
    const CheckedPath& right,
    std::string* error) {
    if (!left.exists || !right.exists) {
        return false;
    }
    std::error_code equivalent_error;
    const bool equivalent =
        std::filesystem::equivalent(
            left.lexical,
            right.lexical,
            equivalent_error);
    if (equivalent_error) {
        *error =
            "service path identity cannot be inspected safely";
        return true;
    }
    return equivalent;
}

bool EqualFilenameConservatively(
    const std::filesystem::path& left,
    const std::filesystem::path& right) {
    const std::string left_name =
        left.filename().native();
    const std::string right_name =
        right.filename().native();
    if (left_name.size() != right_name.size()) {
        return false;
    }
    for (std::size_t index = 0U;
         index < left_name.size();
         ++index) {
        const unsigned char left_byte =
            static_cast<unsigned char>(left_name[index]);
        const unsigned char right_byte =
            static_cast<unsigned char>(right_name[index]);
        if (left_byte == right_byte) {
            continue;
        }
        if (left_byte >=
                static_cast<unsigned char>('A') &&
            left_byte <=
                static_cast<unsigned char>('Z')) {
            const unsigned char folded =
                static_cast<unsigned char>(
                    left_byte -
                    static_cast<unsigned char>('A') +
                    static_cast<unsigned char>('a'));
            if (folded == right_byte) {
                continue;
            }
        }
        if (right_byte >=
                static_cast<unsigned char>('A') &&
            right_byte <=
                static_cast<unsigned char>('Z')) {
            const unsigned char folded =
                static_cast<unsigned char>(
                    right_byte -
                    static_cast<unsigned char>('A') +
                    static_cast<unsigned char>('a'));
            if (folded == left_byte) {
                continue;
            }
        }
        return false;
    }
    return true;
}

bool IsMissingPathError(
    const std::error_code& error) {
    return error ==
               std::errc::no_such_file_or_directory ||
           error ==
               std::errc::not_a_directory;
}

bool SameExistingParentEntry(
    const CheckedPath& left,
    const CheckedPath& right,
    std::string* error) {
    if (!EqualFilenameConservatively(
            left.lexical, right.lexical)) {
        return false;
    }
    const std::filesystem::path left_parent =
        left.lexical.parent_path();
    const std::filesystem::path right_parent =
        right.lexical.parent_path();
    if (left_parent == right_parent ||
        left.resolved.parent_path() ==
            right.resolved.parent_path()) {
        return true;
    }
    std::error_code left_error;
    const bool left_exists =
        std::filesystem::is_directory(
            left_parent, left_error);
    std::error_code right_error;
    const bool right_exists =
        std::filesystem::is_directory(
            right_parent, right_error);
    if ((left_error &&
         !IsMissingPathError(left_error)) ||
        (right_error &&
         !IsMissingPathError(right_error))) {
        *error =
            "service parent identity cannot be inspected safely";
        return true;
    }
    if (!left_exists || !right_exists) {
        return false;
    }
    std::error_code equivalent_error;
    const bool equivalent =
        std::filesystem::equivalent(
            left_parent,
            right_parent,
            equivalent_error);
    if (equivalent_error) {
        *error =
            "service parent identity cannot be inspected safely";
        return true;
    }
    return equivalent;
}

bool ParentsAlias(
    const CheckedPath& left,
    const CheckedPath& right,
    std::string* error) {
    const std::filesystem::path left_lexical =
        left.lexical.parent_path();
    const std::filesystem::path right_lexical =
        right.lexical.parent_path();
    const std::filesystem::path left_resolved =
        left.resolved.parent_path();
    const std::filesystem::path right_resolved =
        right.resolved.parent_path();
    if (left_lexical == right_lexical ||
        left_resolved == right_resolved) {
        return true;
    }
    std::error_code left_error;
    const bool left_exists =
        std::filesystem::is_directory(
            left_lexical, left_error);
    std::error_code right_error;
    const bool right_exists =
        std::filesystem::is_directory(
            right_lexical, right_error);
    if ((left_error &&
         !IsMissingPathError(left_error)) ||
        (right_error &&
         !IsMissingPathError(right_error))) {
        *error =
            "service parent identity cannot be inspected safely";
        return true;
    }
    if (!left_exists || !right_exists) {
        return false;
    }
    std::error_code equivalent_error;
    const bool equivalent =
        std::filesystem::equivalent(
            left_lexical,
            right_lexical,
            equivalent_error);
    if (equivalent_error) {
        *error =
            "service parent identity cannot be inspected safely";
        return true;
    }
    return equivalent;
}

bool Aliases(
    const CheckedPath& left,
    const CheckedPath& right,
    std::string* error) {
    return left.lexical == right.lexical ||
           left.resolved == right.resolved ||
           SameExistingObject(left, right, error) ||
           SameExistingParentEntry(left, right, error);
}

}  // namespace

std::string ValidateIngressServicePathPolicy(
    const IngressServiceConfig& config,
    std::optional<std::string_view>
        resolved_credential_path,
    bool require_existing_parents) {
    struct InputPath final {
        PathAccess access;
        std::filesystem::path path;
    };

    std::vector<InputPath> inputs;
    inputs.reserve(9U);
    inputs.push_back(
        {PathAccess::Writable,
         config.ingress.shadow_capture_path});
    inputs.push_back(
        {PathAccess::Writable,
         config.ingress.metrics_textfile_path});
    inputs.push_back(
        {PathAccess::SdkLogPrefix,
         config.ingress.sdk_log_prefix});
    inputs.push_back(
        {PathAccess::ProtectedInput,
         config.endpoint_contract_path});
    inputs.push_back(
        {PathAccess::ProtectedInput,
         config.preflight_paths.baseline_json});
    inputs.push_back(
        {PathAccess::ProtectedInput,
         config.preflight_paths.sdk_archive});
    inputs.push_back(
        {PathAccess::ProtectedInput,
         config.preflight_paths.shared_library});
    if (resolved_credential_path.has_value()) {
        inputs.push_back(
            {PathAccess::ProtectedInput,
             std::string(*resolved_credential_path)});
    } else if (config.credential_path.has_value()) {
        inputs.push_back(
            {PathAccess::ProtectedInput,
             *config.credential_path});
    }

    std::vector<CheckedPath> checked;
    checked.reserve(inputs.size());
    std::string error;
    for (const InputPath& input : inputs) {
        std::optional<CheckedPath> item =
            CheckPath(
                input.access,
                input.path,
                require_existing_parents,
                &error);
        if (!item.has_value()) {
            return error;
        }
        checked.push_back(std::move(*item));
    }

    for (std::size_t left = 0U;
         left < checked.size();
         ++left) {
        for (std::size_t right = left + 1U;
             right < checked.size();
             ++right) {
            if (checked[left].access !=
                    PathAccess::Writable &&
                checked[left].access !=
                    PathAccess::SdkLogPrefix &&
                checked[right].access !=
                    PathAccess::Writable &&
                checked[right].access !=
                    PathAccess::SdkLogPrefix) {
                continue;
            }
            error.clear();
            if ((checked[left].access ==
                     PathAccess::SdkLogPrefix ||
                 checked[right].access ==
                     PathAccess::SdkLogPrefix) &&
                ParentsAlias(
                    checked[left],
                    checked[right],
                    &error)) {
                if (!error.empty()) {
                    return error;
                }
                return
                    "SDK log directory must be isolated from every "
                    "other service path";
            }
            if (!Aliases(
                    checked[left],
                    checked[right],
                    &error)) {
                continue;
            }
            if (!error.empty()) {
                return error;
            }
            if (checked[left].access !=
                    PathAccess::ProtectedInput &&
                checked[right].access !=
                    PathAccess::ProtectedInput) {
                return
                    "writable service paths must not alias one another";
            }
            return
                "writable service path must not alias a protected input";
        }
    }
    return {};
}

}  // namespace l2flow::apps
