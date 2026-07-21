#include "l2flow/baseline/vendor_baseline.h"

#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace {

struct Options {
    l2flow::baseline::PreflightPaths paths{
        "configs/vendor_baseline.json",
        "mdl_sdk_2_13_234.tar.gz",
        "mdl_sdk_2_13_234/libs/linux/libmdl_api.so"};
    std::optional<std::filesystem::path> output;
    bool differences_only = false;
};

void PrintUsage(std::ostream& output) {
    output
        << "Usage: mdl_abi_preflight [options]\n"
        << "  --baseline PATH   immutable approved baseline JSON\n"
        << "  --archive PATH    original SDK archive (required)\n"
        << "  --library PATH    libmdl_api.so path\n"
        << "  --output PATH     write JSON report instead of stdout\n"
        << "  --diff-only       emit only checks that differ from baseline\n"
        << "  --help            show this help\n\n"
        << "The full preflight intentionally fails if the original SDK archive is\n"
        << "missing. It never loads libmdl_api.so until every artifact, ELF, and\n"
        << "compiled ABI check has passed.\n";
}

bool TakePath(int argc,
              char** argv,
              int* index,
              std::filesystem::path* path,
              std::string* error) {
    if (*index + 1 >= argc) {
        *error = std::string(argv[*index]) + " requires a path";
        return false;
    }
    ++*index;
    *path = argv[*index];
    if (path->empty()) {
        *error = std::string(argv[*index - 1]) +
                 " does not accept an empty path";
        return false;
    }
    return true;
}

bool ParseOptions(int argc,
                  char** argv,
                  Options* options,
                  bool* show_help,
                  std::string* error) {
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument == "--help") {
            *show_help = true;
            return true;
        }
        if (argument == "--diff-only") {
            options->differences_only = true;
            continue;
        }
        if (argument == "--baseline") {
            if (!TakePath(argc,
                          argv,
                          &index,
                          &options->paths.baseline_json,
                          error)) {
                return false;
            }
            continue;
        }
        if (argument == "--archive") {
            if (!TakePath(argc,
                          argv,
                          &index,
                          &options->paths.sdk_archive,
                          error)) {
                return false;
            }
            continue;
        }
        if (argument == "--library") {
            if (!TakePath(argc,
                          argv,
                          &index,
                          &options->paths.shared_library,
                          error)) {
                return false;
            }
            continue;
        }
        if (argument == "--output") {
            std::filesystem::path output_path;
            if (!TakePath(
                    argc, argv, &index, &output_path, error)) {
                return false;
            }
            options->output = std::move(output_path);
            continue;
        }
        *error = "unknown option: " + std::string(argument);
        return false;
    }
    return true;
}

bool WriteReport(const Options& options,
                 std::string_view report,
                 std::string* error) {
    if (!options.output.has_value()) {
        std::cout << report;
        return static_cast<bool>(std::cout);
    }
    std::ofstream output(*options.output,
                         std::ios::binary | std::ios::trunc);
    if (!output.is_open()) {
        *error = "cannot open report output: " +
                 options.output->string();
        return false;
    }
    output.write(report.data(),
                 static_cast<std::streamsize>(report.size()));
    output.flush();
    if (!output) {
        *error = "cannot write complete report: " +
                 options.output->string();
        return false;
    }
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        Options options;
        bool show_help = false;
        std::string error;
        if (!ParseOptions(
                argc, argv, &options, &show_help, &error)) {
            std::cerr << "mdl_abi_preflight: " << error << '\n';
            PrintUsage(std::cerr);
            return 2;
        }
        if (show_help) {
            PrintUsage(std::cout);
            return 0;
        }

        const l2flow::baseline::PreflightReport result =
            l2flow::baseline::RunVendorPreflight(options.paths);
        const std::string report =
            l2flow::baseline::PreflightReportJson(
                result, options.differences_only);
        if (!WriteReport(options, report, &error)) {
            std::cerr << "mdl_abi_preflight: " << error << '\n';
            return 2;
        }
        return result.passed() ? 0 : 1;
    } catch (const std::exception& exception) {
        std::cerr << "mdl_abi_preflight: fatal exception: "
                  << exception.what() << '\n';
        return 2;
    } catch (...) {
        std::cerr << "mdl_abi_preflight: unknown fatal exception\n";
        return 2;
    }
}
