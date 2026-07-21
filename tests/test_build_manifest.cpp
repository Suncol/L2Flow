#include "l2flow/build_manifest.h"
#include "l2flow/common/sha256.h"

#include <array>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <string_view>

namespace {

class TestContext final {
public:
    void Expect(bool condition, std::string_view description) {
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

std::string ReadFile(const char* path, TestContext* test) {
    std::ifstream input(path, std::ios::binary);
    test->Expect(input.is_open(), "opens configured build manifest");
    if (!input.is_open()) {
        return {};
    }
    std::string contents{
        std::istreambuf_iterator<char>(input),
        std::istreambuf_iterator<char>()};
    test->Expect(!input.bad(), "reads the complete build manifest");
    return contents;
}

bool Contains(std::string_view text, std::string_view value) {
    return text.find(value) != std::string_view::npos;
}

bool IsLowercaseSha256(std::string_view value) {
    if (value.size() != 64U) {
        return false;
    }
    for (const char character : value) {
        const unsigned char byte =
            static_cast<unsigned char>(character);
        if (std::isdigit(byte) == 0 &&
            (character < 'a' || character > 'f')) {
            return false;
        }
    }
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    TestContext test;
    test.Expect(argc == 3,
                "test receives manifest and SDK baseline paths");
    if (argc != 3) {
        return 1;
    }

    const std::string manifest = ReadFile(argv[1], &test);
    const std::string baseline = ReadFile(argv[2], &test);
    const std::string_view expected = l2flow::build_manifest::kJson;

    test.Expect(l2flow::build_manifest::kSchemaVersion == 1U,
                "generated header exposes manifest schema version 1");
    test.Expect(l2flow::build_manifest::kCxxStandard == 20U,
                "generated header exposes the production C++ standard");
    test.Expect(manifest == expected,
                "manifest file is byte-exact with generated header");
    const std::string actual_manifest_sha =
        l2flow::common::Sha256Hex(
            l2flow::common::ComputeSha256(
                std::string_view(manifest)));
    test.Expect(
        actual_manifest_sha == l2flow::build_manifest::kSha256,
        "configured manifest SHA-256 matches exact file bytes");
    test.Expect(IsLowercaseSha256(l2flow::build_manifest::kSha256),
                "manifest SHA-256 is canonical lowercase hex");

    const std::string actual_baseline_sha =
        l2flow::common::Sha256Hex(
            l2flow::common::ComputeSha256(
                std::string_view(baseline)));
    test.Expect(
        actual_baseline_sha ==
            l2flow::build_manifest::kSdkBaselineSha256,
        "manifest pins the exact SDK baseline JSON SHA-256");
    test.Expect(
        Contains(
            manifest,
            "\"sha256\": \"" + actual_baseline_sha + "\""),
        "SDK baseline hash field matches the checked file");

    constexpr std::array<std::string_view, 18U> required_fields = {{
        "\"schema_version\": 1",
        "\"project\": {",
        "\"name\": \"L2Flow\"",
        "\"version\": \"0.1.0\"",
        "\"compiler\": {",
        "\"id\": \"",
        "\"path\": \"",
        "\"cxx_standard\": 20",
        "\"system\": {",
        "\"processor\": \"",
        "\"build_type\": \"",
        "\"effective_flags\": {",
        "\"strict_compile\": [",
        "\"sanitizer\": {",
        "\"sdk_baseline\": {",
        "\"path\": \"configs/vendor_baseline.json\"",
        "\"source_revision\": {",
        "\"status\": \"",
    }};
    for (const std::string_view field : required_fields) {
        test.Expect(Contains(manifest, field),
                    std::string("manifest contains field: ") +
                        std::string(field));
    }

    test.Expect(
        Contains(
            manifest,
            "\"strict_compile\": [\"-Wall\", \"-Wextra\", "
            "\"-Wpedantic\", \"-Wconversion\", \"-Wshadow\", "
            "\"-Werror\"]"),
        "strict Phase 0/1 compile flags are recorded exactly");
    if (l2flow::build_manifest::kSanitizerMode == "none") {
        test.Expect(
            Contains(manifest, "\"mode\": \"none\"") &&
                Contains(manifest, "\"compile\": []") &&
                Contains(manifest, "\"link\": []"),
            "non-sanitized build records exact empty sanitizer flags");
    } else if (
        l2flow::build_manifest::kSanitizerMode ==
        "address+undefined") {
        test.Expect(
            Contains(manifest, "\"mode\": \"address+undefined\"") &&
                Contains(
                    manifest,
                    "\"compile\": [\"-fno-omit-frame-pointer\", "
                    "\"-fsanitize=address,undefined\"]") &&
                Contains(
                    manifest,
                    "\"link\": [\"-fno-omit-frame-pointer\", "
                    "\"-fsanitize=address,undefined\"]"),
            "ASan+UBSan build records exact compile and link flags");
    } else if (l2flow::build_manifest::kSanitizerMode == "thread") {
        test.Expect(
            Contains(manifest, "\"mode\": \"thread\"") &&
                Contains(
                    manifest,
                    "\"compile\": [\"-fno-omit-frame-pointer\", "
                    "\"-fsanitize=thread\"]") &&
                Contains(
                    manifest,
                    "\"link\": [\"-fno-omit-frame-pointer\", "
                    "\"-fsanitize=thread\"]"),
            "TSan build records exact compile and link flags");
    } else {
        test.Expect(false, "manifest rejects unknown sanitizer modes");
    }
    test.Expect(
        !Contains(manifest, "\"id\": \"\"") &&
            !Contains(manifest, "\"version\": \"\"") &&
            !Contains(manifest, "\"path\": \"\"") &&
            !Contains(manifest, "\"name\": \"\"") &&
            !Contains(manifest, "\"processor\": \"\"") &&
            !Contains(manifest, "\"build_type\": \"\""),
        "compiler, project, system, and build fields are non-empty");
    test.Expect(
        !Contains(manifest, "\"timestamp\"") &&
            !Contains(manifest, "\"generated_at\"") &&
            !Contains(manifest, "\"generated_time\""),
        "manifest contains no unstable generation timestamp");

    if (l2flow::build_manifest::kSourceRevisionStatus ==
        "unavailable") {
        test.Expect(
            l2flow::build_manifest::kSourceRevision.empty() &&
                Contains(manifest, "\"revision\": null"),
            "missing Git revision is explicit and never fabricated");
    } else {
        test.Expect(
            l2flow::build_manifest::kSourceRevisionStatus ==
                    "available" &&
                l2flow::build_manifest::kSourceRevision.size() ==
                    40U,
            "available source revision is a full Git object id");
    }

    if (test.failures() != 0) {
        std::cerr << test.failures()
                  << " build manifest checks failed\n";
        return 1;
    }
    std::cout << "build manifest contract passed\n";
    return 0;
}
