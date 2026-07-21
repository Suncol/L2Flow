#include "l2flow/common/identity128.h"
#include "l2flow/ingress/raw_manifest_store.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace {

namespace ingress = l2flow::ingress;

int failures = 0;

void Expect(bool condition, std::string_view message) {
    if (!condition) {
        ++failures;
        std::cerr << "FAIL: " << message << '\n';
    }
}

template <std::size_t Size>
std::array<std::byte, Size> Pattern(std::uint8_t start) {
    std::array<std::byte, Size> value{};
    for (std::size_t index = 0U; index < Size; ++index) {
        value[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                start + static_cast<std::uint8_t>(index)));
    }
    return value;
}

ingress::RawManifestNamespaceV1 MakeNamespace() {
    ingress::RawManifestNamespaceV1 value{};
    value.capture_date = 20260718U;
    value.source_stream_id = 1001U;
    value.stream_day_id = Pattern<16U>(0x01U);
    return value;
}

ingress::RawManifestSegmentEntryV1 MakeOpenEntry() {
    ingress::RawManifestSegmentEntryV1 entry{};
    entry.namespace_identity = MakeNamespace();
    entry.segment_sequence = 1U;
    entry.state = ingress::RawManifestSegmentStateV1::kOpen;
    entry.segment_base_wal_pos = 0U;
    entry.segment_logical_length =
        ingress::kRawV1SegmentHeaderBytes;
    entry.segment_sha256 = Pattern<32U>(0x10U);
    entry.record_count = 0U;
    entry.next_expected_first_ingress_sequence = 1U;

    ingress::DurableMarkerV1 marker{};
    marker.source_stream_id =
        entry.namespace_identity.source_stream_id;
    marker.segment_sequence = 1U;
    marker.durable_global_wal_pos =
        ingress::kRawV1SegmentHeaderBytes;
    marker.durable_ingress_sequence = 0U;
    marker.durable_segment_offset =
        ingress::kRawV1SegmentHeaderBytes;
    marker.marker_flags = 0U;
    Expect(
        ingress::EncodeDurableMarkerV1(
            marker, &entry.accepted_marker_bytes) ==
            ingress::RawV1Error::kNone,
        "header-only accepted marker encodes");
    entry.accepted_marker_sha256 =
        ingress::ComputeAcceptedMarkerSha256(
            entry.accepted_marker_bytes);

    entry.host_uuid = Pattern<16U>(0x21U);
    entry.linux_boot_id = Pattern<16U>(0x31U);
    entry.clock_epoch_algorithm = 1U;
    entry.clock_epoch_digest = Pattern<32U>(0x41U);
    entry.clock_epoch_label =
        std::numeric_limits<std::uint64_t>::max();
    entry.sdk_archive_sha256 = Pattern<32U>(0x61U);
    entry.libmdl_api_sha256 = Pattern<32U>(0x81U);
    entry.endpoint_contract_sha256 = Pattern<32U>(0xa1U);
    entry.config_sha256 = Pattern<32U>(0xc1U);
    entry.raw_schema_sha256 = Pattern<32U>(0x01U);
    entry.build_manifest_sha256 = Pattern<32U>(0x21U);
    return entry;
}

bool SetPrefix(ingress::RawManifestV1* manifest) {
    ingress::RawV1Digest digest{};
    const ingress::RawManifestV1Error result =
        ingress::ComputeClosedPrefix(*manifest, &digest);
    if (result != ingress::RawManifestV1Error::kNone) {
        ++failures;
        std::cerr << "FAIL: cannot compute test manifest prefix: "
                  << ingress::RawManifestV1ErrorName(result)
                  << '\n';
        return false;
    }
    manifest->closed_prefix_sha256 = digest;
    return true;
}

ingress::RawManifestV1 MakeManifest(
    std::uint64_t generation,
    bool with_open) {
    ingress::RawManifestV1 manifest{};
    manifest.manifest_generation = generation;
    manifest.namespace_identity = MakeNamespace();
    manifest.closed_entry_count = 0U;
    if (with_open) {
        manifest.open_entry = MakeOpenEntry();
    }
    static_cast<void>(SetPrefix(&manifest));
    return manifest;
}

std::string Encode(const ingress::RawManifestV1& manifest) {
    std::string bytes;
    const ingress::RawManifestV1Error result =
        ingress::EncodeRawManifestJcs(manifest, &bytes);
    Expect(
        result == ingress::RawManifestV1Error::kNone,
        "test manifest encodes");
    return bytes;
}

std::string ReplaceOnce(
    std::string input,
    std::string_view needle,
    std::string_view replacement) {
    const std::size_t offset = input.find(needle);
    if (offset == std::string::npos) {
        ++failures;
        std::cerr << "FAIL: mutation needle was not found\n";
        return input;
    }
    input.replace(offset, needle.size(), replacement);
    return input;
}

class TemporaryDirectory final {
public:
    TemporaryDirectory() {
        std::array<char, 64U> pattern{};
        constexpr std::string_view prefix =
            "/tmp/l2flow-raw-manifest-store-XXXXXX";
        std::copy(prefix.begin(), prefix.end(), pattern.begin());
        char* const created = ::mkdtemp(pattern.data());
        if (created == nullptr) {
            return;
        }
        path_ = created;
        static_cast<void>(::chmod(path_.c_str(), 0700));
        descriptor_ = ::open(
            path_.c_str(),
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC | O_NOATIME);
    }

    ~TemporaryDirectory() {
        if (descriptor_ >= 0) {
            for (const char* const name : {
                     ingress::kRawManifestTemporaryFilename,
                     ingress::kRawManifestCurrentFilename,
                     "manifest-hardlink",
                     ingress::kRawWriterLeaseTemporaryFilename,
                     ingress::kRawWriterLeaseFilename}) {
                static_cast<void>(
                    ::unlinkat(descriptor_, name, 0));
            }
            static_cast<void>(::close(descriptor_));
        }
        if (!path_.empty()) {
            static_cast<void>(::rmdir(path_.c_str()));
        }
    }

    TemporaryDirectory(const TemporaryDirectory&) = delete;
    TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

    [[nodiscard]] int descriptor() const noexcept {
        return descriptor_;
    }

private:
    std::string path_;
    int descriptor_ = -1;
};

bool WriteAllAt(
    int directory_fd,
    const char* name,
    std::string_view bytes) {
    const int fd = ::openat(
        directory_fd,
        name,
        O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW |
            O_NONBLOCK | O_CLOEXEC,
        0600);
    if (fd < 0) {
        return false;
    }
    static_cast<void>(::fchmod(fd, 0600));
    std::size_t complete = 0U;
    bool okay = true;
    while (complete < bytes.size()) {
        const ssize_t result = ::pwrite(
            fd,
            bytes.data() + complete,
            bytes.size() - complete,
            static_cast<off_t>(complete));
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            okay = false;
            break;
        }
        complete += static_cast<std::size_t>(result);
    }
    if (::fsync(fd) != 0) {
        okay = false;
    }
    if (::close(fd) != 0) {
        okay = false;
    }
    return okay;
}

void TestFilenameContract() {
    Expect(
        ingress::ClassifyRawManifestFilename("manifest.json") ==
            ingress::RawManifestFilenameKind::kCurrent,
        "the one frozen current manifest filename is accepted");
    for (const std::string_view name : {
             "Manifest.json",
             "./manifest.json",
             "manifest.json/",
             "manifest-00000000000000000001.json",
             "manifest-history.json",
             ingress::kRawManifestTemporaryFilename}) {
        Expect(
            ingress::ClassifyRawManifestFilename(name) ==
                ingress::RawManifestFilenameKind::kInvalid,
            "unfrozen historical/alias/temp manifest name is rejected");
    }
}

void TestStrictParser() {
    const ingress::RawManifestV1 source =
        MakeManifest(2U, true);
    const std::string canonical = Encode(source);
    ingress::RawManifestV1 parsed{};
    ingress::RawManifestV1Error model_error =
        ingress::RawManifestV1Error::kAllocationFailure;
    Expect(
        ingress::ParseRawManifestJcs(
            canonical, &parsed, &model_error) ==
            ingress::RawManifestStoreError::kNone,
        "complete canonical manifest strictly parses");
    Expect(
        model_error == ingress::RawManifestV1Error::kNone &&
            Encode(parsed) == canonical,
        "strict parser round-trips exact canonical bytes");

    ingress::RawManifestV1 continuation{};
    continuation.manifest_generation = 7U;
    continuation.namespace_identity = MakeNamespace();
    ingress::RawManifestSegmentEntryV1 continuation_entry =
        MakeOpenEntry();
    continuation_entry.state =
        ingress::RawManifestSegmentStateV1::kClosed;
    continuation_entry.segment_flags =
        ingress::kRawV1FinalizationContinuation;
    continuation_entry.reserve_state_uuid =
        Pattern<16U>(0x51U);
    continuation_entry.finalization_cycle_id =
        Pattern<16U>(0x71U);
    continuation_entry.immutable_grant_sha256 =
        Pattern<32U>(0x91U);
    continuation_entry.maintenance_report_locator =
        "maintenance/finalization-" +
        l2flow::common::Identity128Hex(
            continuation_entry.finalization_cycle_id) +
        ".json";
    continuation_entry.archive_locator =
        "reserve-audit/finalization-" +
        l2flow::common::Identity128Hex(
            continuation_entry.reserve_state_uuid) +
        "-" +
        l2flow::common::Identity128Hex(
            continuation_entry.finalization_cycle_id) +
        "/";
    ingress::DurableMarkerV1 sealed_marker{};
    sealed_marker.source_stream_id =
        continuation.namespace_identity.source_stream_id;
    sealed_marker.segment_sequence = 1U;
    sealed_marker.durable_global_wal_pos =
        ingress::kRawV1SegmentHeaderBytes;
    sealed_marker.durable_ingress_sequence = 0U;
    sealed_marker.durable_segment_offset =
        ingress::kRawV1SegmentHeaderBytes;
    sealed_marker.marker_flags =
        ingress::kRawV1SegmentSealed;
    Expect(
        ingress::EncodeDurableMarkerV1(
            sealed_marker,
            &continuation_entry.accepted_marker_bytes) ==
            ingress::RawV1Error::kNone,
        "continuation seal marker encodes");
    continuation_entry.accepted_marker_sha256 =
        ingress::ComputeAcceptedMarkerSha256(
            continuation_entry.accepted_marker_bytes);
    continuation.closed_entries.push_back(
        std::move(continuation_entry));
    continuation.closed_entry_count = 1U;
    static_cast<void>(SetPrefix(&continuation));
    const std::string continuation_bytes =
        Encode(continuation);
    Expect(
        ingress::ParseRawManifestJcs(
            continuation_bytes, &parsed, &model_error) ==
            ingress::RawManifestStoreError::kNone &&
            Encode(parsed) == continuation_bytes,
        "strict parser decodes deterministic continuation locators and nullable fields");

    const auto rejects_json =
        [&](std::string bytes, std::string_view description) {
            ingress::RawManifestV1 unchanged = MakeManifest(99U, false);
            const std::string before = Encode(unchanged);
            const ingress::RawManifestStoreError result =
                ingress::ParseRawManifestJcs(
                    bytes, &unchanged, nullptr);
            Expect(
                result != ingress::RawManifestStoreError::kNone,
                description);
            Expect(
                Encode(unchanged) == before,
                "parse failure leaves model output unchanged");
        };

    rejects_json(" " + canonical, "leading whitespace is rejected");
    rejects_json(canonical + "\n", "trailing newline is rejected");
    rejects_json(
        std::string("\xef\xbb\xbf", 3U) + canonical,
        "UTF-8 BOM is rejected");
    rejects_json(
        ReplaceOnce(
            canonical,
            "{\"closed_entries\":",
            "{\"unknown\":0,\"closed_entries\":"),
        "unknown top-level member is rejected");
    rejects_json(
        ReplaceOnce(
            canonical,
            "{\"closed_entries\":",
            "{\"closed_entries\":[],\"closed_entries\":"),
        "duplicate top-level member is rejected");
    rejects_json(
        ReplaceOnce(
            canonical,
            ",\"open_entry\":",
            ",\"missing_open_entry\":"),
        "missing required member is rejected");
    rejects_json(
        ReplaceOnce(
            canonical,
            "\"manifest_generation\":\"2\"",
            "\"manifest_generation\":2"),
        "uint64 JSON number instead of decimal string is rejected");
    rejects_json(
        ReplaceOnce(
            canonical,
            "\"manifest_generation\":\"2\"",
            "\"manifest_generation\":\"02\""),
        "uint64 decimal string with leading zero is rejected");
    rejects_json(
        ReplaceOnce(
            canonical,
            "\"schema_version\":1",
            "\"schema_version\":\"1\""),
        "u32 member with wrong JSON type is rejected");
    rejects_json(
        ReplaceOnce(
            canonical,
            "\"stream_day_id\":\"01",
            "\"stream_day_id\":\"A1"),
        "uppercase/noncanonical identity hex is rejected");
    rejects_json("", "empty manifest is invalid JSON");
    for (std::size_t length = 1U;
         length < canonical.size();
         length +=
             std::max<std::size_t>(
                 1U, canonical.size() / 37U)) {
        rejects_json(
            canonical.substr(0U, length),
            "truncated canonical manifest is rejected");
    }

    const std::string escaped =
        ReplaceOnce(
            canonical,
            "\"stream_day_id\":\"0",
            "\"stream_day_id\":\"\\u0030");
    Expect(
        ingress::ParseRawManifestJcs(
            escaped, &parsed, nullptr) ==
            ingress::RawManifestStoreError::kNonCanonicalJson,
        "semantically equivalent noncanonical string escape is rejected");

    std::string wrong_prefix = canonical;
    const std::size_t prefix =
        wrong_prefix.find("\"closed_prefix_sha256\":\"");
    Expect(prefix != std::string::npos, "closed prefix is found");
    if (prefix != std::string::npos) {
        const std::size_t digit =
            prefix +
            std::string_view(
                "\"closed_prefix_sha256\":\"").size();
        wrong_prefix[digit] =
            wrong_prefix[digit] == '0' ? '1' : '0';
        model_error = ingress::RawManifestV1Error::kNone;
        Expect(
            ingress::ParseRawManifestJcs(
                wrong_prefix, &parsed, &model_error) ==
                ingress::RawManifestStoreError::kInvalidModel &&
                model_error ==
                    ingress::RawManifestV1Error::
                        kClosedPrefixMismatch,
            "canonical syntax with false closed frontier is rejected");
    }
}

void TestFileStoreAndPublication() {
    TemporaryDirectory directory;
    Expect(
        directory.descriptor() >= 0,
        "temporary manifest directory opens");
    if (directory.descriptor() < 0) {
        return;
    }
    std::string error;
    std::unique_ptr<ingress::RawWriterLease> lease =
        ingress::AcquireRawWriterLeaseAt(
            directory.descriptor(),
            MakeNamespace().source_stream_id,
            MakeNamespace().capture_date,
            &error);
    Expect(lease != nullptr, "Raw writer lease is acquired");
    if (lease == nullptr) {
        std::cerr << "lease error: " << error << '\n';
        return;
    }

    constexpr std::size_t kMaximumBytes = 1024U * 1024U;
    ingress::RawManifestV1 generation_one =
        MakeManifest(1U, false);
    ingress::RawManifestV1Error model_error =
        ingress::RawManifestV1Error::kNone;
    Expect(
        ingress::PublishCurrentRawManifest(
            *lease,
            MakeNamespace(),
            generation_one,
            kMaximumBytes,
            &model_error,
            &error) == ingress::RawManifestStoreError::kNone,
        "first manifest uses durable no-replace publication");
    if (!error.empty()) {
        std::cerr << "first publish error: " << error << '\n';
    }

    struct stat status {};
    Expect(
        ::fstatat(
            directory.descriptor(),
            ingress::kRawManifestCurrentFilename,
            &status,
            AT_SYMLINK_NOFOLLOW) == 0 &&
            S_ISREG(status.st_mode) &&
            status.st_uid == ::geteuid() &&
            status.st_nlink == 1 &&
            (status.st_mode & 0777U) == 0600U,
        "published manifest is exact owner-only regular inode");
    Expect(
        ::fstatat(
            directory.descriptor(),
            ingress::kRawManifestTemporaryFilename,
            &status,
            AT_SYMLINK_NOFOLLOW) != 0 &&
            errno == ENOENT,
        "successful publication consumes typed temporary");

    ingress::RawManifestV1 loaded{};
    std::string loaded_bytes;
    Expect(
        ingress::LoadCurrentRawManifestAt(
            directory.descriptor(),
            MakeNamespace(),
            kMaximumBytes,
            &loaded,
            &loaded_bytes,
            &model_error,
            &error) == ingress::RawManifestStoreError::kNone &&
            loaded.manifest_generation == 1U &&
            loaded_bytes == Encode(generation_one),
        "strict retained-fd readback returns exact generation one");

    ingress::RawManifestNamespaceV1 wrong_namespace =
        MakeNamespace();
    wrong_namespace.stream_day_id[0U] ^= std::byte{0x01};
    Expect(
        ingress::LoadCurrentRawManifestAt(
            directory.descriptor(),
            wrong_namespace,
            kMaximumBytes,
            &loaded,
            nullptr,
            nullptr,
            &error) ==
            ingress::RawManifestStoreError::kNamespaceMismatch,
        "load binds manifest to authoritative stream-day identity");
    Expect(
        ingress::LoadCurrentRawManifestAt(
            directory.descriptor(),
            MakeNamespace(),
            8U,
            &loaded,
            nullptr,
            nullptr,
            &error) ==
            ingress::RawManifestStoreError::kFileTooLarge,
        "caller-supplied manifest read bound is enforced");

    ingress::RawManifestV1 generation_two =
        MakeManifest(2U, true);
    Expect(
        ingress::PublishCurrentRawManifest(
            *lease,
            MakeNamespace(),
            generation_two,
            kMaximumBytes,
            &model_error,
            &error) == ingress::RawManifestStoreError::kNone,
        "append-only successor atomically replaces current manifest");
    Expect(
        ingress::LoadCurrentRawManifestAt(
            directory.descriptor(),
            MakeNamespace(),
            kMaximumBytes,
            &loaded,
            &loaded_bytes,
            nullptr,
            &error) == ingress::RawManifestStoreError::kNone &&
            loaded.manifest_generation == 2U &&
            loaded_bytes == Encode(generation_two),
        "replacement generation strictly reads back");

    Expect(
        ingress::PublishCurrentRawManifest(
            *lease,
            MakeNamespace(),
            generation_one,
            kMaximumBytes,
            &model_error,
            &error) == ingress::RawManifestStoreError::kInvalidModel &&
            model_error ==
                ingress::RawManifestV1Error::kGenerationRegression,
        "generation rollback is rejected before filesystem mutation");
    Expect(
        ingress::LoadCurrentRawManifestAt(
            directory.descriptor(),
            MakeNamespace(),
            kMaximumBytes,
            &loaded,
            nullptr,
            nullptr,
            &error) == ingress::RawManifestStoreError::kNone &&
            loaded.manifest_generation == 2U,
        "failed rollback preserves current generation");

    ingress::RawManifestV1 generation_three = generation_two;
    generation_three.manifest_generation = 3U;
    Expect(
        WriteAllAt(
            directory.descriptor(),
            ingress::kRawManifestTemporaryFilename,
            "{}"),
        "invalid interrupted typed temporary is created");
    Expect(
        ingress::PublishCurrentRawManifest(
            *lease,
            MakeNamespace(),
            generation_three,
            kMaximumBytes,
            nullptr,
            &error) ==
            ingress::RawManifestStoreError::kAmbiguousTemporary,
        "partial/mismatching typed temporary fails closed");
    Expect(
        ::unlinkat(
            directory.descriptor(),
            ingress::kRawManifestTemporaryFilename,
            0) == 0 &&
            ::fsync(directory.descriptor()) == 0,
        "test removes rejected temporary with parent barrier");

    const std::string generation_three_bytes =
        Encode(generation_three);
    Expect(
        WriteAllAt(
            directory.descriptor(),
            ingress::kRawManifestTemporaryFilename,
            generation_three_bytes),
        "complete interrupted typed temporary is created");
    Expect(
        ingress::PublishCurrentRawManifest(
            *lease,
            MakeNamespace(),
            generation_three,
            kMaximumBytes,
            nullptr,
            &error) == ingress::RawManifestStoreError::kNone,
        "byte-identical complete interrupted temporary is adopted");
    Expect(
        ingress::LoadCurrentRawManifestAt(
            directory.descriptor(),
            MakeNamespace(),
            kMaximumBytes,
            &loaded,
            &loaded_bytes,
            nullptr,
            &error) == ingress::RawManifestStoreError::kNone &&
            loaded.manifest_generation == 3U &&
            loaded_bytes == generation_three_bytes,
        "adopted candidate becomes exact current bytes");
    Expect(
        ingress::LoadCurrentRawManifestAt(
            directory.descriptor(),
            MakeNamespace(),
            kMaximumBytes,
            &loaded,
            nullptr,
            &model_error,
            &error,
            &generation_two) ==
            ingress::RawManifestStoreError::kNone,
        "load validates a trusted append-only predecessor");
    ingress::RawManifestV1 future_predecessor =
        generation_three;
    future_predecessor.manifest_generation = 4U;
    Expect(
        ingress::LoadCurrentRawManifestAt(
            directory.descriptor(),
            MakeNamespace(),
            kMaximumBytes,
            &loaded,
            nullptr,
            &model_error,
            &error,
            &future_predecessor) ==
            ingress::RawManifestStoreError::kInvalidModel &&
            model_error ==
                ingress::RawManifestV1Error::kGenerationRegression,
        "load rejects a current file that regresses a trusted predecessor");

    Expect(
        ingress::PublishCurrentRawManifest(
            *lease,
            MakeNamespace(),
            generation_three,
            kMaximumBytes,
            nullptr,
            &error) == ingress::RawManifestStoreError::kNone,
        "byte-identical current publication is idempotent");

    Expect(
        ::fchmodat(
            directory.descriptor(),
            ingress::kRawManifestCurrentFilename,
            04600,
            0) == 0,
        "test adds an unsafe special mode bit");
    Expect(
        ingress::LoadCurrentRawManifestAt(
            directory.descriptor(),
            MakeNamespace(),
            kMaximumBytes,
            &loaded,
            nullptr,
            nullptr,
            &error) ==
            ingress::RawManifestStoreError::kUnsafeFile,
        "special mode bits are rejected despite owner-only rw bits");
    Expect(
        ::fchmodat(
            directory.descriptor(),
            ingress::kRawManifestCurrentFilename,
            0600,
            0) == 0,
        "test restores exact manifest mode");

    Expect(
        ::linkat(
            directory.descriptor(),
            ingress::kRawManifestCurrentFilename,
            directory.descriptor(),
            "manifest-hardlink",
            0) == 0,
        "test creates a second hard link");
    Expect(
        ingress::LoadCurrentRawManifestAt(
            directory.descriptor(),
            MakeNamespace(),
            kMaximumBytes,
            &loaded,
            nullptr,
            nullptr,
            &error) ==
            ingress::RawManifestStoreError::kUnsafeFile,
        "multiply-linked current manifest is rejected");
    Expect(
        ::unlinkat(
            directory.descriptor(),
            "manifest-hardlink",
            0) == 0 &&
            ::fsync(directory.descriptor()) == 0,
        "test removes manifest hard link with parent barrier");
}

}  // namespace

int main() {
    TestFilenameContract();
    TestStrictParser();
    TestFileStoreAndPublication();
    if (failures != 0) {
        std::cerr << failures
                  << " Phase 2 Raw manifest store tests failed\n";
        return 1;
    }
    std::cout << "Phase 2 Raw manifest store tests passed\n";
    return 0;
}
