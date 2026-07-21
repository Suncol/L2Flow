#include "l2flow/ingress/finalization_archive_v1.h"

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/ingress/empty_anchor_tombstone_v1.h"
#include "l2flow/ingress/finalization_report_v1.h"
#include "l2flow/ingress/raw_manifest_store.h"
#include "l2flow/ingress/raw_namespace.h"
#include "l2flow/ingress/raw_recovery_maintenance_report_v1.h"
#include "l2flow/ingress/raw_sealed_certificate_v1.h"
#include "l2flow/ingress/raw_wal_stream_posix.h"
#include "l2flow/ingress/scaffolding_finalization_report_v1.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace l2flow::ingress {
namespace {

static_assert(
    kFinalizationArchiveV1MaximumRawManifestBytes ==
    kRawWalStreamDefaultMaximumManifestBytesV1);
inline constexpr std::size_t
    kMaximumAllDoneArchiveEvidenceBytesV1 =
        kReserveStateV1EntryCapacity *
        (kFinalizationArchiveV1MaximumRawManifestBytes +
         kFinalizationReportV1MaximumBytes +
         kRecoveryMaintenanceReportV1MaximumBytes +
         2U * kSealedRawCertificateV1MaximumBytes);
static_assert(
    kFinalizationArchiveV1MaximumTotalArtifactBytes >=
    kMaximumAllDoneArchiveEvidenceBytesV1);

template <std::size_t Size>
[[nodiscard]] bool IsZero(
    const std::array<std::byte, Size>& value) noexcept {
    return std::all_of(
        value.begin(),
        value.end(),
        [](std::byte byte) noexcept {
            return byte == std::byte{0};
        });
}

[[nodiscard]] bool SameNamespace(
    const RawManifestNamespaceV1& left,
    const RawManifestNamespaceV1& right) noexcept {
    return left.capture_date == right.capture_date &&
           left.source_stream_id ==
               right.source_stream_id &&
           left.stream_day_id == right.stream_day_id;
}

[[nodiscard]] bool SameNamespace(
    const RawManifestNamespaceV1& left,
    const EmptyAnchorNamespaceV1& right) noexcept {
    return left.capture_date == right.capture_date &&
           left.source_stream_id ==
               right.source_stream_id &&
           left.stream_day_id == right.stream_day_id;
}

[[nodiscard]] RawManifestNamespaceV1 NamespaceOf(
    const ReserveStateEntryV1& entry) noexcept {
    return RawManifestNamespaceV1{
        .capture_date = entry.capture_date,
        .source_stream_id = entry.source_stream_id,
        .stream_day_id = entry.stream_day_id};
}

[[nodiscard]] RawManifestNamespaceV1 NamespaceOf(
    const EmptyAnchorNamespaceV1& value) noexcept {
    return RawManifestNamespaceV1{
        .capture_date = value.capture_date,
        .source_stream_id = value.source_stream_id,
        .stream_day_id = value.stream_day_id};
}

[[nodiscard]] bool ValidNamespace(
    const RawManifestNamespaceV1& value) noexcept {
    return value.capture_date != 0U &&
           value.source_stream_id != 0U &&
           !l2flow::common::IsZeroIdentity(
               value.stream_day_id);
}

[[nodiscard]] std::string_view ArtifactTypeName(
    FinalizationArchiveArtifactTypeV1 value) noexcept {
    switch (value) {
    case FinalizationArchiveArtifactTypeV1::
        kFinalizationReport:
        return "FINALIZATION_REPORT";
    case FinalizationArchiveArtifactTypeV1::
        kScaffoldingFinalizationReport:
        return "SCAFFOLDING_FINALIZATION_REPORT";
    case FinalizationArchiveArtifactTypeV1::
        kPreexistingRecoveryReport:
        return "PREEXISTING_RECOVERY_REPORT";
    case FinalizationArchiveArtifactTypeV1::
        kRawManifest:
        return "RAW_MANIFEST";
    case FinalizationArchiveArtifactTypeV1::
        kSealedRawCertificate:
        return "SEALED_RAW_CERTIFICATE";
    case FinalizationArchiveArtifactTypeV1::
        kEmptyAnchorTombstone:
        return "EMPTY_ANCHOR_TOMBSTONE";
    }
    return {};
}

[[nodiscard]] std::string_view ArtifactStem(
    FinalizationArchiveArtifactTypeV1 value) noexcept {
    switch (value) {
    case FinalizationArchiveArtifactTypeV1::
        kFinalizationReport:
        return "finalization-report";
    case FinalizationArchiveArtifactTypeV1::
        kScaffoldingFinalizationReport:
        return "scaffolding-finalization-report";
    case FinalizationArchiveArtifactTypeV1::
        kPreexistingRecoveryReport:
        return "preexisting-recovery-report";
    case FinalizationArchiveArtifactTypeV1::
        kRawManifest:
        return "raw-manifest";
    case FinalizationArchiveArtifactTypeV1::
        kSealedRawCertificate:
        return "sealed-raw-certificate";
    case FinalizationArchiveArtifactTypeV1::
        kEmptyAnchorTombstone:
        return "empty-anchor-tombstone";
    }
    return {};
}

[[nodiscard]] bool IsArtifactType(
    FinalizationArchiveArtifactTypeV1 value) noexcept {
    return !ArtifactTypeName(value).empty();
}

[[nodiscard]] std::size_t MaximumArtifactBytes(
    FinalizationArchiveArtifactTypeV1 value) noexcept {
    switch (value) {
    case FinalizationArchiveArtifactTypeV1::
        kFinalizationReport:
        return kFinalizationReportV1MaximumBytes;
    case FinalizationArchiveArtifactTypeV1::
        kScaffoldingFinalizationReport:
        return kScaffoldingFinalizationReportV1MaximumBytes;
    case FinalizationArchiveArtifactTypeV1::
        kPreexistingRecoveryReport:
        return kRecoveryMaintenanceReportV1MaximumBytes;
    case FinalizationArchiveArtifactTypeV1::
        kRawManifest:
        return kFinalizationArchiveV1MaximumRawManifestBytes;
    case FinalizationArchiveArtifactTypeV1::
        kSealedRawCertificate:
        return kSealedRawCertificateV1MaximumBytes;
    case FinalizationArchiveArtifactTypeV1::
        kEmptyAnchorTombstone:
        return kEmptyAnchorTombstoneV1MaximumBytes;
    }
    return 0U;
}

[[nodiscard]] bool IsSafeLocator(
    std::string_view value) noexcept {
    if (value.empty() ||
        value.size() >
            kFinalizationArchiveV1MaximumSourceLocatorBytes ||
        value.front() == '/' || value.back() == '/') {
        return false;
    }
    std::size_t component_begin = 0U;
    for (std::size_t index = 0U;
         index <= value.size();
         ++index) {
        if (index != value.size() &&
            value[index] != '/') {
            const char character = value[index];
            const bool safe =
                (character >= 'a' &&
                 character <= 'z') ||
                (character >= 'A' &&
                 character <= 'Z') ||
                (character >= '0' &&
                 character <= '9') ||
                character == '-' ||
                character == '_' ||
                character == '.' ||
                character == '=';
            if (!safe) {
                return false;
            }
            continue;
        }
        const std::string_view component =
            value.substr(
                component_begin,
                index - component_begin);
        if (component.empty() ||
            component == "." ||
            component == "..") {
            return false;
        }
        component_begin = index + 1U;
    }
    return true;
}

[[nodiscard]] std::string_view Basename(
    std::string_view locator) noexcept {
    const std::size_t slash = locator.rfind('/');
    return slash == std::string_view::npos
               ? locator
               : locator.substr(slash + 1U);
}

[[nodiscard]] bool ParseDecimalU32(
    std::string_view value,
    std::uint32_t* output) noexcept {
    if (output == nullptr || value.empty() ||
        (value.size() > 1U && value.front() == '0')) {
        return false;
    }
    std::uint64_t parsed = 0U;
    for (const char character : value) {
        if (character < '0' || character > '9') {
            return false;
        }
        parsed =
            parsed * 10U +
            static_cast<std::uint64_t>(
                character - '0');
        if (parsed >
            std::numeric_limits<std::uint32_t>::max()) {
            return false;
        }
    }
    *output = static_cast<std::uint32_t>(parsed);
    return true;
}

[[nodiscard]] bool IsCleanupLocatorShape(
    FinalizationArchiveArtifactTypeV1 type,
    std::string_view locator,
    const RawManifestNamespaceV1&
        namespace_identity) noexcept {
    if (type ==
        FinalizationArchiveArtifactTypeV1::
            kScaffoldingFinalizationReport) {
        constexpr std::string_view prefix =
            "emergency-reports/";
        return locator.starts_with(prefix) &&
               locator.find(
                   '/', prefix.size()) ==
                   std::string_view::npos;
    }
    if (type !=
            FinalizationArchiveArtifactTypeV1::
                kFinalizationReport &&
        type !=
            FinalizationArchiveArtifactTypeV1::
                kPreexistingRecoveryReport) {
        return true;
    }
    const std::size_t first = locator.find('/');
    const std::size_t second =
        first == std::string_view::npos
            ? std::string_view::npos
            : locator.find('/', first + 1U);
    const std::size_t third =
        second == std::string_view::npos
            ? std::string_view::npos
            : locator.find('/', second + 1U);
    if (first == std::string_view::npos ||
        second == std::string_view::npos ||
        third == std::string_view::npos ||
        locator.find('/', third + 1U) !=
            std::string_view::npos) {
        return false;
    }
    const std::string_view capture =
        locator.substr(0U, first);
    const std::string_view stream =
        locator.substr(
            first + 1U,
            second - first - 1U);
    if (!capture.starts_with("capture_date=") ||
        !stream.starts_with("stream=") ||
        locator.substr(
            second + 1U,
            third - second - 1U) !=
            "maintenance") {
        return false;
    }
    std::uint32_t parsed_capture = 0U;
    if (!ParseDecimalU32(
            capture.substr(
                std::string_view(
                    "capture_date=")
                    .size()),
            &parsed_capture) ||
        parsed_capture !=
            namespace_identity.capture_date) {
        return false;
    }
    const std::string_view stream_tail =
        stream.substr(
            std::string_view("stream=").size());
    const std::size_t slug_separator =
        stream_tail.find('-');
    std::uint32_t parsed_stream = 0U;
    const auto canonical_slug =
        CanonicalRawStreamSlugV1(
            namespace_identity.source_stream_id);
    return slug_separator != std::string_view::npos &&
           slug_separator != 0U &&
           slug_separator + 1U < stream_tail.size() &&
           ParseDecimalU32(
               stream_tail.substr(0U, slug_separator),
               &parsed_stream) &&
           parsed_stream ==
               namespace_identity.source_stream_id &&
           canonical_slug.has_value() &&
           stream_tail.substr(slug_separator + 1U) ==
               *canonical_slug;
}

[[nodiscard]] bool BuildArchivePath(
    std::size_t index,
    FinalizationArchiveArtifactTypeV1 type,
    std::string* output) {
    if (output == nullptr ||
        index >=
            kFinalizationArchiveV1MaximumArtifacts ||
        !IsArtifactType(type)) {
        return false;
    }
    std::array<char, 3U> digits{
        static_cast<char>(
            '0' + (index / 100U) % 10U),
        static_cast<char>(
            '0' + (index / 10U) % 10U),
        static_cast<char>('0' + index % 10U)};
    std::string path;
    path.reserve(64U);
    path.append(kFinalizationArchiveV1EvidenceDirectory);
    path.push_back('/');
    path.append(digits.data(), digits.size());
    path.push_back('-');
    path.append(ArtifactStem(type));
    path.append(".json");
    output->swap(path);
    return true;
}

void AppendQuoted(
    std::string* output,
    std::string_view value) {
    output->push_back('"');
    output->append(value);
    output->push_back('"');
}

void AppendU32(
    std::string* output,
    std::uint32_t value) {
    std::array<char, 10U> bytes{};
    const auto result = std::to_chars(
        bytes.data(),
        bytes.data() + bytes.size(),
        value);
    if (result.ec != std::errc{}) {
        throw std::bad_alloc();
    }
    output->append(bytes.data(), result.ptr);
}

void AppendQuotedU64(
    std::string* output,
    std::uint64_t value) {
    std::array<char, 20U> bytes{};
    const auto result = std::to_chars(
        bytes.data(),
        bytes.data() + bytes.size(),
        value);
    if (result.ec != std::errc{}) {
        throw std::bad_alloc();
    }
    output->push_back('"');
    output->append(bytes.data(), result.ptr);
    output->push_back('"');
}

template <std::size_t Size>
void AppendHex(
    std::string* output,
    const std::array<std::byte, Size>& value) {
    static constexpr std::string_view kDigits =
        "0123456789abcdef";
    output->push_back('"');
    for (const std::byte byte : value) {
        const unsigned octet =
            std::to_integer<unsigned>(byte);
        output->push_back(
            kDigits[(octet >> 4U) & 0x0fU]);
        output->push_back(kDigits[octet & 0x0fU]);
    }
    output->push_back('"');
}

class Parser final {
public:
    explicit Parser(std::string_view input) noexcept
        : input_(input) {}

    [[nodiscard]] bool Consume(
        std::string_view token) noexcept {
        if (input_.substr(position_, token.size()) !=
            token) {
            return false;
        }
        position_ += token.size();
        return true;
    }

    [[nodiscard]] bool ParseU32(
        std::uint32_t* output) noexcept {
        if (output == nullptr ||
            position_ >= input_.size() ||
            input_[position_] < '0' ||
            input_[position_] > '9') {
            return false;
        }
        const std::size_t begin = position_;
        if (input_[position_] == '0' &&
            position_ + 1U < input_.size() &&
            input_[position_ + 1U] >= '0' &&
            input_[position_ + 1U] <= '9') {
            return false;
        }
        while (position_ < input_.size() &&
               input_[position_] >= '0' &&
               input_[position_] <= '9') {
            ++position_;
        }
        std::uint32_t value = 0U;
        const auto result = std::from_chars(
            input_.data() + begin,
            input_.data() + position_,
            value);
        if (result.ec != std::errc{} ||
            result.ptr != input_.data() + position_) {
            return false;
        }
        *output = value;
        return true;
    }

    [[nodiscard]] bool ParseQuotedU64(
        std::uint64_t* output) noexcept {
        if (output == nullptr || !Consume("\"") ||
            position_ >= input_.size() ||
            input_[position_] < '0' ||
            input_[position_] > '9') {
            return false;
        }
        const std::size_t begin = position_;
        if (input_[position_] == '0' &&
            position_ + 1U < input_.size() &&
            input_[position_ + 1U] != '"') {
            return false;
        }
        while (position_ < input_.size() &&
               input_[position_] >= '0' &&
               input_[position_] <= '9') {
            ++position_;
        }
        std::uint64_t value = 0U;
        const auto result = std::from_chars(
            input_.data() + begin,
            input_.data() + position_,
            value);
        if (result.ec != std::errc{} ||
            result.ptr != input_.data() + position_ ||
            !Consume("\"")) {
            return false;
        }
        *output = value;
        return true;
    }

    [[nodiscard]] bool ParseQuoted(
        std::string* output) {
        if (output == nullptr || !Consume("\"")) {
            return false;
        }
        const std::size_t begin = position_;
        while (position_ < input_.size() &&
               input_[position_] != '"') {
            const unsigned char byte =
                static_cast<unsigned char>(
                    input_[position_]);
            if (byte < 0x20U ||
                input_[position_] == '\\') {
                return false;
            }
            ++position_;
        }
        if (position_ >= input_.size()) {
            return false;
        }
        try {
            output->assign(
                input_.data() + begin,
                position_ - begin);
        } catch (...) {
            return false;
        }
        ++position_;
        return true;
    }

    template <std::size_t Size>
    [[nodiscard]] bool ParseHex(
        std::array<std::byte, Size>* output) noexcept {
        if (output == nullptr || !Consume("\"") ||
            input_.size() - position_ <
                Size * 2U + 1U) {
            return false;
        }
        std::array<std::byte, Size> value{};
        for (std::size_t index = 0U;
             index < Size;
             ++index) {
            const int high =
                HexNibble(
                    input_[position_ + index * 2U]);
            const int low =
                HexNibble(
                    input_[position_ +
                           index * 2U + 1U]);
            if (high < 0 || low < 0) {
                return false;
            }
            value[index] = static_cast<std::byte>(
                static_cast<unsigned>(
                    high * 16 + low));
        }
        position_ += Size * 2U;
        if (!Consume("\"")) {
            return false;
        }
        *output = value;
        return true;
    }

    [[nodiscard]] bool done() const noexcept {
        return position_ == input_.size();
    }

private:
    [[nodiscard]] static int HexNibble(
        char value) noexcept {
        if (value >= '0' && value <= '9') {
            return value - '0';
        }
        if (value >= 'a' && value <= 'f') {
            return 10 + value - 'a';
        }
        return -1;
    }

    std::string_view input_;
    std::size_t position_ = 0U;
};

[[nodiscard]] bool ParseArtifactType(
    Parser* parser,
    FinalizationArchiveArtifactTypeV1*
        output) {
    if (parser == nullptr || output == nullptr) {
        return false;
    }
    std::string text;
    if (!parser->ParseQuoted(&text)) {
        return false;
    }
    for (std::uint8_t wire = 1U; wire <= 6U;
         ++wire) {
        const auto value =
            static_cast<
                FinalizationArchiveArtifactTypeV1>(
                wire);
        if (text == ArtifactTypeName(value)) {
            *output = value;
            return true;
        }
    }
    return false;
}

[[nodiscard]] bool HasCompletedPreexistingAction(
    const ReserveStateEntryV1& entry) noexcept {
    return std::any_of(
        entry.actions.begin(),
        entry.actions.end(),
        [](const FinalizationActionReceiptV1&
               action) noexcept {
            return action.action_kind ==
                       FinalizationActionKindV1::
                           kPreexistingRecoveryReport &&
                   action.action_state ==
                       FinalizationActionStateV1::
                           kComplete;
        });
}

using ParsedArtifactModel = std::variant<
    FinalizationReportV1,
    ScaffoldingFinalizationReportV1,
    RecoveryMaintenanceReportV1,
    RawManifestV1,
    SealedRawCertificateV1,
    EmptyAnchorTombstoneV1>;

struct ArtifactDraft final {
    FinalizationArchiveArtifactTypeV1 type =
        FinalizationArchiveArtifactTypeV1::
            kFinalizationReport;
    std::string source_locator;
    std::string exact_bytes;
    RawV1Digest sha256{};
    RawManifestNamespaceV1 namespace_identity{};
    ParsedArtifactModel parsed =
        FinalizationReportV1{};
    bool referenced = false;
};

[[nodiscard]] FinalizationArchiveV1Error
ParseArtifact(
    const FinalizationArchiveArtifactInputV1& input,
    ArtifactDraft* output) {
    if (output == nullptr ||
        !IsArtifactType(input.artifact_type)) {
        return FinalizationArchiveV1Error::
            kInvalidArtifactType;
    }
    if (input.exact_bytes.empty() ||
        input.exact_bytes.size() >
            MaximumArtifactBytes(
                input.artifact_type)) {
        return FinalizationArchiveV1Error::
            kArtifactSizeExceeded;
    }

    ArtifactDraft draft{};
    draft.type = input.artifact_type;
    draft.exact_bytes = input.exact_bytes;
    draft.sha256 = l2flow::common::ComputeSha256(
        std::string_view(draft.exact_bytes));
    std::string expected_filename;

    switch (draft.type) {
    case FinalizationArchiveArtifactTypeV1::
        kFinalizationReport: {
        FinalizationReportV1 value{};
        if (ParseFinalizationReportV1Jcs(
                draft.exact_bytes,
                &value) !=
            FinalizationReportV1Error::kNone ||
            FinalizationReportV1Filename(
                value, &expected_filename) !=
                FinalizationReportV1Error::kNone) {
            return FinalizationArchiveV1Error::
                kArtifactNotCanonical;
        }
        draft.namespace_identity =
            value.namespace_identity;
        draft.parsed = std::move(value);
        break;
    }
    case FinalizationArchiveArtifactTypeV1::
        kScaffoldingFinalizationReport: {
        ScaffoldingFinalizationReportV1 value{};
        if (ParseScaffoldingFinalizationReportV1Jcs(
                draft.exact_bytes,
                &value) !=
            ScaffoldingFinalizationReportV1Error::
                kNone ||
            ScaffoldingFinalizationReportV1Filename(
                value, &expected_filename) !=
                ScaffoldingFinalizationReportV1Error::
                    kNone) {
            return FinalizationArchiveV1Error::
                kArtifactNotCanonical;
        }
        draft.namespace_identity =
            value.planned_namespace;
        draft.parsed = std::move(value);
        break;
    }
    case FinalizationArchiveArtifactTypeV1::
        kPreexistingRecoveryReport: {
        RecoveryMaintenanceReportV1 value{};
        if (ParseRecoveryMaintenanceReportV1Jcs(
                draft.exact_bytes,
                &value) !=
            RecoveryMaintenanceReportV1Error::kNone ||
            RecoveryMaintenanceReportV1Filename(
                value, &expected_filename) !=
                RecoveryMaintenanceReportV1Error::
                    kNone) {
            return FinalizationArchiveV1Error::
                kArtifactNotCanonical;
        }
        draft.namespace_identity =
            value.namespace_identity;
        draft.parsed = std::move(value);
        break;
    }
    case FinalizationArchiveArtifactTypeV1::
        kRawManifest: {
        RawManifestV1 value{};
        if (ParseRawManifestJcs(
                draft.exact_bytes,
                &value) !=
            RawManifestStoreError::kNone) {
            return FinalizationArchiveV1Error::
                kArtifactNotCanonical;
        }
        expected_filename =
            kRawManifestCurrentFilename;
        draft.namespace_identity =
            value.namespace_identity;
        draft.parsed = std::move(value);
        break;
    }
    case FinalizationArchiveArtifactTypeV1::
        kSealedRawCertificate: {
        SealedRawCertificateV1 value{};
        if (ParseSealedRawCertificateV1Jcs(
                draft.exact_bytes,
                &value) !=
            SealedRawCertificateV1Error::kNone ||
            SealedRawCertificateV1Filename(
                value, &expected_filename) !=
                SealedRawCertificateV1Error::kNone) {
            return FinalizationArchiveV1Error::
                kArtifactNotCanonical;
        }
        draft.namespace_identity =
            value.namespace_identity;
        draft.parsed = std::move(value);
        break;
    }
    case FinalizationArchiveArtifactTypeV1::
        kEmptyAnchorTombstone: {
        EmptyAnchorTombstoneV1 value{};
        if (ParseEmptyAnchorTombstoneV1Jcs(
                draft.exact_bytes,
                &value) !=
            EmptyAnchorTombstoneV1Error::kNone ||
            EmptyAnchorTombstoneV1Filename(
                value, &expected_filename) !=
                EmptyAnchorTombstoneV1Error::kNone) {
            return FinalizationArchiveV1Error::
                kArtifactNotCanonical;
        }
        draft.namespace_identity =
            NamespaceOf(value.namespace_identity);
        draft.parsed = std::move(value);
        break;
    }
    }

    if (!ValidNamespace(draft.namespace_identity)) {
        return FinalizationArchiveV1Error::
            kArtifactNamespaceMismatch;
    }
    const bool routed_report =
        draft.type ==
            FinalizationArchiveArtifactTypeV1::
                kFinalizationReport ||
        draft.type ==
            FinalizationArchiveArtifactTypeV1::
                kPreexistingRecoveryReport;
    const bool scaffolding_report =
        draft.type ==
        FinalizationArchiveArtifactTypeV1::
            kScaffoldingFinalizationReport;
    if (routed_report) {
        const auto canonical_stream_slug =
            CanonicalRawStreamSlugV1(
                draft.namespace_identity
                    .source_stream_id);
        if (!input.source_locator.empty() ||
            !canonical_stream_slug.has_value() ||
            input.source_stream_slug !=
                *canonical_stream_slug) {
            return FinalizationArchiveV1Error::
                kInvalidSourceLocator;
        }
        draft.source_locator =
            "capture_date=" +
            std::to_string(
                draft.namespace_identity.capture_date) +
            "/stream=" +
            std::to_string(
                draft.namespace_identity
                    .source_stream_id) +
            "-" +
            std::string(*canonical_stream_slug) +
            "/maintenance/" + expected_filename;
    } else if (scaffolding_report) {
        if (!input.source_locator.empty() ||
            !input.source_stream_slug.empty()) {
            return FinalizationArchiveV1Error::
                kInvalidSourceLocator;
        }
        draft.source_locator =
            "emergency-reports/" + expected_filename;
    } else {
        if (!input.source_stream_slug.empty() ||
            !IsSafeLocator(input.source_locator)) {
            return FinalizationArchiveV1Error::
                kInvalidSourceLocator;
        }
        draft.source_locator = input.source_locator;
    }
    if (!IsSafeLocator(draft.source_locator)) {
        return FinalizationArchiveV1Error::
            kInvalidSourceLocator;
    }
    if (!IsCleanupLocatorShape(
            draft.type,
            draft.source_locator,
            draft.namespace_identity)) {
        return FinalizationArchiveV1Error::
            kInvalidSourceLocator;
    }
    if (Basename(draft.source_locator) !=
        expected_filename) {
        return FinalizationArchiveV1Error::
            kArtifactFilenameMismatch;
    }
    *output = std::move(draft);
    return FinalizationArchiveV1Error::kNone;
}

[[nodiscard]] bool DraftLess(
    const ArtifactDraft& left,
    const ArtifactDraft& right) {
    if (left.type != right.type) {
        return static_cast<std::uint8_t>(left.type) <
               static_cast<std::uint8_t>(right.type);
    }
    if (left.source_locator != right.source_locator) {
        return left.source_locator <
               right.source_locator;
    }
    return std::lexicographical_compare(
        left.sha256.begin(),
        left.sha256.end(),
        right.sha256.begin(),
        right.sha256.end());
}

[[nodiscard]] FinalizationArchiveV1Error
NormalizeArtifacts(
    std::span<
        const FinalizationArchiveArtifactInputV1>
        inputs,
    std::vector<ArtifactDraft>* output) {
    if (output == nullptr) {
        return FinalizationArchiveV1Error::
            kInvalidArgument;
    }
    if (inputs.size() >
        kFinalizationArchiveV1MaximumArtifacts) {
        return FinalizationArchiveV1Error::
            kArtifactLimitExceeded;
    }
    std::vector<ArtifactDraft> values;
    values.reserve(inputs.size());
    std::uint64_t total_bytes = 0U;
    for (const auto& input : inputs) {
        ArtifactDraft value{};
        const FinalizationArchiveV1Error error =
            ParseArtifact(input, &value);
        if (error !=
            FinalizationArchiveV1Error::kNone) {
            return error;
        }
        if (value.exact_bytes.size() >
            kFinalizationArchiveV1MaximumTotalArtifactBytes -
                total_bytes) {
            return FinalizationArchiveV1Error::
                kTotalSizeExceeded;
        }
        total_bytes += value.exact_bytes.size();
        values.push_back(std::move(value));
    }
    std::sort(values.begin(), values.end(), DraftLess);

    std::vector<ArtifactDraft> normalized;
    normalized.reserve(values.size());
    for (auto& value : values) {
        if (!normalized.empty() &&
            normalized.back().type == value.type &&
            normalized.back().source_locator ==
                value.source_locator) {
            if (normalized.back().sha256 !=
                    value.sha256 ||
                normalized.back().exact_bytes !=
                    value.exact_bytes) {
                return FinalizationArchiveV1Error::
                    kConflictingArtifact;
            }
            continue;
        }
        normalized.push_back(std::move(value));
    }
    output->swap(normalized);
    return FinalizationArchiveV1Error::kNone;
}

template <typename Predicate>
[[nodiscard]] FinalizationArchiveV1Error
FindUnique(
    std::vector<ArtifactDraft>* artifacts,
    FinalizationArchiveArtifactTypeV1 type,
    Predicate predicate,
    FinalizationArchiveV1Error missing_error,
    ArtifactDraft** output) noexcept {
    if (artifacts == nullptr || output == nullptr) {
        return FinalizationArchiveV1Error::
            kInvalidArgument;
    }
    ArtifactDraft* found = nullptr;
    for (ArtifactDraft& artifact : *artifacts) {
        if (artifact.type != type ||
            !predicate(artifact)) {
            continue;
        }
        if (found != nullptr) {
            return FinalizationArchiveV1Error::
                kConflictingArtifact;
        }
        found = &artifact;
    }
    if (found == nullptr) {
        return missing_error;
    }
    found->referenced = true;
    *output = found;
    return FinalizationArchiveV1Error::kNone;
}

[[nodiscard]] FinalizationArchiveV1Error
RequireSidecar(
    std::vector<ArtifactDraft>* artifacts,
    FinalizationArchiveArtifactTypeV1 type,
    const RawManifestNamespaceV1& expected_namespace,
    const RawV1Digest& expected_hash) noexcept {
    if (IsZero(expected_hash)) {
        return FinalizationArchiveV1Error::
            kMissingSidecar;
    }
    ArtifactDraft* artifact = nullptr;
    const FinalizationArchiveV1Error error =
        FindUnique(
            artifacts,
            type,
            [&](const ArtifactDraft& value) noexcept {
                return SameNamespace(
                           value.namespace_identity,
                           expected_namespace) &&
                       value.sha256 == expected_hash;
            },
            FinalizationArchiveV1Error::
                kMissingSidecar,
            &artifact);
    if (error !=
        FinalizationArchiveV1Error::kNone) {
        return error;
    }
    if (type ==
        FinalizationArchiveArtifactTypeV1::
            kSealedRawCertificate) {
        const auto* certificate =
            std::get_if<SealedRawCertificateV1>(
                &artifact->parsed);
        if (certificate == nullptr ||
            !SameNamespace(
                certificate->namespace_identity,
                expected_namespace)) {
            return FinalizationArchiveV1Error::
                kArtifactNamespaceMismatch;
        }
    } else {
        const auto* tombstone =
            std::get_if<EmptyAnchorTombstoneV1>(
                &artifact->parsed);
        if (tombstone == nullptr ||
            !SameNamespace(
                expected_namespace,
                tombstone->namespace_identity)) {
            return FinalizationArchiveV1Error::
                kArtifactNamespaceMismatch;
        }
    }
    return FinalizationArchiveV1Error::kNone;
}

[[nodiscard]] FinalizationArchiveV1Error
RequireRecoverySidecars(
    std::vector<ArtifactDraft>* artifacts,
    const RecoveryMaintenanceReportV1& report) noexcept {
    switch (report.result) {
    case RecoveryMaintenanceResultV1::kSealedRaw:
        return RequireSidecar(
            artifacts,
            FinalizationArchiveArtifactTypeV1::
                kSealedRawCertificate,
            report.namespace_identity,
            report.current_sealed_raw_certificate_sha256);
    case RecoveryMaintenanceResultV1::kEmptyAnchorOnly:
        return RequireSidecar(
            artifacts,
            FinalizationArchiveArtifactTypeV1::
                kEmptyAnchorTombstone,
            report.namespace_identity,
            report.current_empty_anchor_tombstone_sha256);
    case RecoveryMaintenanceResultV1::kResumedOpen:
        if (!IsZero(
                report.open_boundary
                    .reopens_empty_tombstone_sha256)) {
            const FinalizationArchiveV1Error error =
                RequireSidecar(
                    artifacts,
                    FinalizationArchiveArtifactTypeV1::
                        kEmptyAnchorTombstone,
                    report.namespace_identity,
                    report.open_boundary
                        .reopens_empty_tombstone_sha256);
            if (error !=
                FinalizationArchiveV1Error::kNone) {
                return error;
            }
        }
        if (!IsZero(
                report.open_boundary
                    .reopens_sealed_raw_certificate_sha256)) {
            return RequireSidecar(
                artifacts,
                FinalizationArchiveArtifactTypeV1::
                    kSealedRawCertificate,
                report.namespace_identity,
                report.open_boundary
                    .reopens_sealed_raw_certificate_sha256);
        }
        return FinalizationArchiveV1Error::kNone;
    }
    return FinalizationArchiveV1Error::
        kArtifactNotCanonical;
}

[[nodiscard]] FinalizationArchiveV1Error
RequireManifest(
    std::vector<ArtifactDraft>* artifacts,
    const FinalizationReportV1& report) noexcept {
    if (!report.manifest_frontier.has_value()) {
        return FinalizationArchiveV1Error::
            kFrontierMismatch;
    }
    ArtifactDraft* artifact = nullptr;
    const FinalizationArchiveV1Error error =
        FindUnique(
            artifacts,
            FinalizationArchiveArtifactTypeV1::
                kRawManifest,
            [&](const ArtifactDraft& value) noexcept {
                return SameNamespace(
                    value.namespace_identity,
                    report.namespace_identity);
            },
            FinalizationArchiveV1Error::
                kMissingManifest,
            &artifact);
    if (error !=
        FinalizationArchiveV1Error::kNone) {
        return error;
    }
    const auto* manifest =
        std::get_if<RawManifestV1>(
            &artifact->parsed);
    const auto& frontier =
        *report.manifest_frontier;
    if (manifest == nullptr ||
        manifest->open_entry.has_value() ||
        manifest->closed_entry_count !=
            frontier.closed_entry_count ||
        manifest->closed_prefix_sha256 !=
            frontier.closed_prefix_sha256 ||
        manifest->closed_entries.empty()) {
        return FinalizationArchiveV1Error::
            kFrontierMismatch;
    }
    const RawManifestSegmentEntryV1& last =
        manifest->closed_entries.back();
    if (last.segment_sequence !=
            frontier.frontier_segment_sequence ||
        last.segment_sha256 !=
            frontier.frontier_segment_sha256 ||
        last.accepted_marker_sha256 !=
            frontier.frontier_accepted_seal_sha256) {
        return FinalizationArchiveV1Error::
            kFrontierMismatch;
    }
    return FinalizationArchiveV1Error::kNone;
}

[[nodiscard]] FinalizationArchiveV1Error
CrossValidateArtifacts(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& slot,
    std::vector<ArtifactDraft>* artifacts) noexcept {
    if (artifacts == nullptr) {
        return FinalizationArchiveV1Error::
            kInvalidArgument;
    }
    for (std::size_t index = 0U;
         index < slot.entry_count;
         ++index) {
        const ReserveStateEntryV1& entry =
            slot.entries[index];
        const RawManifestNamespaceV1 expected_namespace =
            NamespaceOf(entry);
        ReserveStateV1Digest grant_hash{};
        if (ComputeImmutableFinalizationGrantSha256V1(
                header,
                slot,
                index,
                &grant_hash) !=
            ReserveStateV1Error::kNone) {
            return FinalizationArchiveV1Error::
                kGrantMismatch;
        }

        if (entry.grant_flags ==
            kReserveGrantScaffoldingOnly) {
            ArtifactDraft* artifact = nullptr;
            const FinalizationArchiveV1Error find_error =
                FindUnique(
                    artifacts,
                    FinalizationArchiveArtifactTypeV1::
                        kScaffoldingFinalizationReport,
                    [&](const ArtifactDraft&
                            value) noexcept {
                        return SameNamespace(
                            value.namespace_identity,
                            expected_namespace);
                    },
                    FinalizationArchiveV1Error::
                        kMissingFinalReport,
                    &artifact);
            if (find_error !=
                FinalizationArchiveV1Error::kNone) {
                return find_error;
            }
            const auto* report =
                std::get_if<
                    ScaffoldingFinalizationReportV1>(
                    &artifact->parsed);
            if (report == nullptr ||
                report->reserve_state_uuid !=
                    header.reserve_state_uuid ||
                report->finalization_cycle_id !=
                    slot.finalization_cycle_id ||
                report->immutable_grant_sha256 !=
                    grant_hash ||
                report->planned_recovery_attempt_id !=
                    entry.scaffolding_payload
                        .recovery_attempt_id ||
                report->object_snapshot_sha256 !=
                    entry.scaffolding_payload
                        .object_snapshot_sha256 ||
                report->observed_object_bitmap !=
                    entry.scaffolding_payload
                        .observed_object_bitmap ||
                report->required_action_bitmap !=
                    entry.scaffolding_payload
                        .required_action_bitmap ||
                artifact->sha256 !=
                    entry.maintenance_report_sha256) {
                return FinalizationArchiveV1Error::
                    kGrantMismatch;
            }
            const FinalizationArchiveV1Error sidecar =
                RequireSidecar(
                    artifacts,
                    FinalizationArchiveArtifactTypeV1::
                        kEmptyAnchorTombstone,
                    expected_namespace,
                    report
                        ->empty_anchor_tombstone_sha256);
            if (sidecar !=
                FinalizationArchiveV1Error::kNone) {
                return sidecar;
            }
            continue;
        }

        ArtifactDraft* artifact = nullptr;
        const FinalizationArchiveV1Error find_error =
            FindUnique(
                artifacts,
                FinalizationArchiveArtifactTypeV1::
                    kFinalizationReport,
                [&](const ArtifactDraft&
                        value) noexcept {
                    return SameNamespace(
                        value.namespace_identity,
                        expected_namespace);
                },
                FinalizationArchiveV1Error::
                    kMissingFinalReport,
                &artifact);
        if (find_error !=
            FinalizationArchiveV1Error::kNone) {
            return find_error;
        }
        const auto* report =
            std::get_if<FinalizationReportV1>(
                &artifact->parsed);
        if (report == nullptr ||
            report->reserve_state_uuid !=
                header.reserve_state_uuid ||
            report->finalization_cycle_id !=
                slot.finalization_cycle_id ||
            report->immutable_grant_sha256 !=
                grant_hash ||
            report->ack_status != entry.ack_status ||
            report->ack_writer_instance !=
                entry.writer_instance ||
            artifact->sha256 !=
                entry.maintenance_report_sha256) {
            return FinalizationArchiveV1Error::
                kGrantMismatch;
        }

        if (entry.grant_flags ==
            kReserveGrantRawFinalization) {
            if (report->result !=
                FinalizationReportResultV1::
                    kSealedRaw) {
                return FinalizationArchiveV1Error::
                    kGrantMismatch;
            }
            FinalizationArchiveV1Error dependency =
                RequireManifest(artifacts, *report);
            if (dependency !=
                FinalizationArchiveV1Error::kNone) {
                return dependency;
            }
            dependency = RequireSidecar(
                artifacts,
                FinalizationArchiveArtifactTypeV1::
                    kSealedRawCertificate,
                expected_namespace,
                report
                    ->sealed_raw_certificate_sha256);
            if (dependency !=
                FinalizationArchiveV1Error::kNone) {
                return dependency;
            }
        } else if (
            entry.grant_flags ==
            kReserveGrantRawAnchorOnly) {
            if (report->result !=
                FinalizationReportResultV1::
                    kEmptyAnchorOnly) {
                return FinalizationArchiveV1Error::
                    kGrantMismatch;
            }
            const FinalizationArchiveV1Error dependency =
                RequireSidecar(
                    artifacts,
                    FinalizationArchiveArtifactTypeV1::
                        kEmptyAnchorTombstone,
                    expected_namespace,
                    report
                        ->empty_anchor_tombstone_sha256);
            if (dependency !=
                FinalizationArchiveV1Error::kNone) {
                return dependency;
            }
        } else {
            return FinalizationArchiveV1Error::
                kGrantMismatch;
        }

        const bool requires_preexisting =
            HasCompletedPreexistingAction(entry);
        if (requires_preexisting !=
            report->preexisting_recovery_report
                .has_value()) {
            return FinalizationArchiveV1Error::
                kMissingPreexistingReport;
        }
        if (report->preexisting_recovery_report
                .has_value()) {
            const auto& reference =
                *report->preexisting_recovery_report;
            ArtifactDraft* recovery_artifact = nullptr;
            const FinalizationArchiveV1Error recovery_error =
                FindUnique(
                    artifacts,
                    FinalizationArchiveArtifactTypeV1::
                        kPreexistingRecoveryReport,
                    [&](const ArtifactDraft&
                            value) noexcept {
                        if (!SameNamespace(
                                value
                                    .namespace_identity,
                                expected_namespace) ||
                            value.sha256 !=
                                reference
                                    .report_sha256) {
                            return false;
                        }
                        const auto* recovery =
                            std::get_if<
                                RecoveryMaintenanceReportV1>(
                                &value.parsed);
                        return recovery != nullptr &&
                               recovery
                                       ->recovery_attempt_id ==
                                   reference
                                       .recovery_attempt_id;
                    },
                    FinalizationArchiveV1Error::
                        kMissingPreexistingReport,
                    &recovery_artifact);
            if (recovery_error !=
                FinalizationArchiveV1Error::kNone) {
                return recovery_error;
            }
            const auto* recovery =
                std::get_if<
                    RecoveryMaintenanceReportV1>(
                    &recovery_artifact->parsed);
            if (recovery == nullptr) {
                return FinalizationArchiveV1Error::
                    kArtifactNotCanonical;
            }
            const FinalizationArchiveV1Error
                sidecar_error =
                    RequireRecoverySidecars(
                        artifacts, *recovery);
            if (sidecar_error !=
                FinalizationArchiveV1Error::kNone) {
                return sidecar_error;
            }
        }
    }

    if (std::any_of(
            artifacts->begin(),
            artifacts->end(),
            [](const ArtifactDraft&
                   artifact) noexcept {
                return !artifact.referenced;
            })) {
        return FinalizationArchiveV1Error::
            kUnexpectedArtifact;
    }
    return FinalizationArchiveV1Error::kNone;
}

[[nodiscard]] bool ParseArtifactObject(
    Parser* parser,
    FinalizationArchiveArtifactV1* output) {
    if (parser == nullptr || output == nullptr) {
        return false;
    }
    FinalizationArchiveArtifactV1 value{};
    if (!parser->Consume("{\"archive_path\":") ||
        !parser->ParseQuoted(&value.archive_path) ||
        !parser->Consume(",\"artifact_type\":") ||
        !ParseArtifactType(
            parser, &value.artifact_type) ||
        !parser->Consume(",\"byte_count\":") ||
        !parser->ParseQuotedU64(&value.byte_count) ||
        !parser->Consume(",\"capture_date\":") ||
        !parser->ParseU32(
            &value.namespace_identity.capture_date) ||
        !parser->Consume(",\"sha256\":") ||
        !parser->ParseHex(&value.sha256) ||
        !parser->Consume(",\"source_locator\":") ||
        !parser->ParseQuoted(&value.source_locator) ||
        !parser->Consume(",\"source_stream_id\":") ||
        !parser->ParseU32(
            &value.namespace_identity
                 .source_stream_id) ||
        !parser->Consume(",\"stream_day_id\":") ||
        !parser->ParseHex(
            &value.namespace_identity.stream_day_id) ||
        !parser->Consume("}")) {
        return false;
    }
    *output = std::move(value);
    return true;
}

}  // namespace

std::string_view FinalizationArchiveV1ErrorName(
    FinalizationArchiveV1Error error) noexcept {
    switch (error) {
    case FinalizationArchiveV1Error::kNone:
        return "NONE";
    case FinalizationArchiveV1Error::kNullOutput:
        return "NULL_OUTPUT";
    case FinalizationArchiveV1Error::kInvalidArgument:
        return "INVALID_ARGUMENT";
    case FinalizationArchiveV1Error::kInvalidStateHeader:
        return "INVALID_STATE_HEADER";
    case FinalizationArchiveV1Error::kStateNotConsumed:
        return "STATE_NOT_CONSUMED";
    case FinalizationArchiveV1Error::kStateNotAllDone:
        return "STATE_NOT_ALL_DONE";
    case FinalizationArchiveV1Error::kInvalidStateSlot:
        return "INVALID_STATE_SLOT";
    case FinalizationArchiveV1Error::kStateEncodingFailure:
        return "STATE_ENCODING_FAILURE";
    case FinalizationArchiveV1Error::kInvalidIdentity:
        return "INVALID_IDENTITY";
    case FinalizationArchiveV1Error::kInvalidArtifactType:
        return "INVALID_ARTIFACT_TYPE";
    case FinalizationArchiveV1Error::kInvalidSourceLocator:
        return "INVALID_SOURCE_LOCATOR";
    case FinalizationArchiveV1Error::kArtifactLimitExceeded:
        return "ARTIFACT_LIMIT_EXCEEDED";
    case FinalizationArchiveV1Error::kArtifactSizeExceeded:
        return "ARTIFACT_SIZE_EXCEEDED";
    case FinalizationArchiveV1Error::kTotalSizeExceeded:
        return "TOTAL_SIZE_EXCEEDED";
    case FinalizationArchiveV1Error::kArtifactNotCanonical:
        return "ARTIFACT_NOT_CANONICAL";
    case FinalizationArchiveV1Error::
        kArtifactNamespaceMismatch:
        return "ARTIFACT_NAMESPACE_MISMATCH";
    case FinalizationArchiveV1Error::
        kArtifactIdentityMismatch:
        return "ARTIFACT_IDENTITY_MISMATCH";
    case FinalizationArchiveV1Error::kArtifactHashMismatch:
        return "ARTIFACT_HASH_MISMATCH";
    case FinalizationArchiveV1Error::
        kArtifactFilenameMismatch:
        return "ARTIFACT_FILENAME_MISMATCH";
    case FinalizationArchiveV1Error::kDuplicateArtifact:
        return "DUPLICATE_ARTIFACT";
    case FinalizationArchiveV1Error::kConflictingArtifact:
        return "CONFLICTING_ARTIFACT";
    case FinalizationArchiveV1Error::kMissingFinalReport:
        return "MISSING_FINAL_REPORT";
    case FinalizationArchiveV1Error::
        kMissingPreexistingReport:
        return "MISSING_PREEXISTING_REPORT";
    case FinalizationArchiveV1Error::kMissingManifest:
        return "MISSING_MANIFEST";
    case FinalizationArchiveV1Error::kMissingSidecar:
        return "MISSING_SIDECAR";
    case FinalizationArchiveV1Error::kUnexpectedArtifact:
        return "UNEXPECTED_ARTIFACT";
    case FinalizationArchiveV1Error::kGrantMismatch:
        return "GRANT_MISMATCH";
    case FinalizationArchiveV1Error::kFrontierMismatch:
        return "FRONTIER_MISMATCH";
    case FinalizationArchiveV1Error::kInvalidArchivePath:
        return "INVALID_ARCHIVE_PATH";
    case FinalizationArchiveV1Error::kInvalidCanonicalJson:
        return "INVALID_CANONICAL_JSON";
    case FinalizationArchiveV1Error::kEncodedSizeExceeded:
        return "ENCODED_SIZE_EXCEEDED";
    case FinalizationArchiveV1Error::kAllocationFailure:
        return "ALLOCATION_FAILURE";
    }
    return "UNKNOWN";
}

FinalizationArchiveV1Error
ValidateFinalizationArchiveV1(
    const FinalizationArchiveV1& archive) noexcept {
    if (archive.schema_version !=
        kFinalizationArchiveV1SchemaVersion) {
        return FinalizationArchiveV1Error::
            kInvalidArgument;
    }
    if (l2flow::common::IsZeroIdentity(
            archive.reserve_state_uuid) ||
        l2flow::common::IsZeroIdentity(
            archive.finalization_cycle_id) ||
        IsZero(archive.reserve_state_schema_sha256) ||
        IsZero(archive.immutable_state_header_sha256) ||
        IsZero(archive.all_done_slot_sha256) ||
        IsZero(archive.state_sha256)) {
        return FinalizationArchiveV1Error::
            kInvalidIdentity;
    }
    if (archive.reserve_state_schema_sha256 !=
        kReserveStateV1SchemaSha256) {
        return FinalizationArchiveV1Error::
            kInvalidStateHeader;
    }
    if (archive.all_done_generation == 0U) {
        return FinalizationArchiveV1Error::
            kStateNotAllDone;
    }
    if (archive.immutable_state_header_path !=
            kFinalizationArchiveV1StateHeaderFilename ||
        archive.all_done_slot_path !=
            kFinalizationArchiveV1AllDoneSlotFilename) {
        return FinalizationArchiveV1Error::
            kInvalidArchivePath;
    }
    if (archive.artifact_count !=
            archive.artifacts.size() ||
        archive.artifacts.size() >
            kFinalizationArchiveV1MaximumArtifacts) {
        return FinalizationArchiveV1Error::
            kArtifactLimitExceeded;
    }
    std::uint64_t total_bytes = 0U;
    std::string previous_locator;
    FinalizationArchiveArtifactTypeV1 previous_type =
        FinalizationArchiveArtifactTypeV1::
            kFinalizationReport;
    bool have_previous = false;
    for (std::size_t index = 0U;
         index < archive.artifacts.size();
         ++index) {
        const FinalizationArchiveArtifactV1& artifact =
            archive.artifacts[index];
        std::string expected_path;
        try {
            if (!BuildArchivePath(
                    index,
                    artifact.artifact_type,
                    &expected_path)) {
                return FinalizationArchiveV1Error::
                    kInvalidArchivePath;
            }
        } catch (...) {
            return FinalizationArchiveV1Error::
                kAllocationFailure;
        }
        if (artifact.archive_path != expected_path) {
            return FinalizationArchiveV1Error::
                kInvalidArchivePath;
        }
        if (!IsArtifactType(artifact.artifact_type)) {
            return FinalizationArchiveV1Error::
                kInvalidArtifactType;
        }
        if (!IsSafeLocator(artifact.source_locator)) {
            return FinalizationArchiveV1Error::
                kInvalidSourceLocator;
        }
        if (!ValidNamespace(
                artifact.namespace_identity)) {
            return FinalizationArchiveV1Error::
                kArtifactNamespaceMismatch;
        }
        if (!IsCleanupLocatorShape(
                artifact.artifact_type,
                artifact.source_locator,
                artifact.namespace_identity)) {
            return FinalizationArchiveV1Error::
                kInvalidSourceLocator;
        }
        if (artifact.byte_count == 0U ||
            artifact.byte_count >
                MaximumArtifactBytes(
                    artifact.artifact_type) ||
            IsZero(artifact.sha256)) {
            return FinalizationArchiveV1Error::
                kArtifactSizeExceeded;
        }
        if (artifact.byte_count >
            kFinalizationArchiveV1MaximumTotalArtifactBytes -
                total_bytes) {
            return FinalizationArchiveV1Error::
                kTotalSizeExceeded;
        }
        total_bytes += artifact.byte_count;
        if (have_previous) {
            const auto type_wire =
                static_cast<std::uint8_t>(
                    artifact.artifact_type);
            const auto previous_wire =
                static_cast<std::uint8_t>(
                    previous_type);
            if (type_wire < previous_wire ||
                (type_wire == previous_wire &&
                 artifact.source_locator <=
                     previous_locator)) {
                return FinalizationArchiveV1Error::
                    kDuplicateArtifact;
            }
        }
        previous_type = artifact.artifact_type;
        previous_locator = artifact.source_locator;
        have_previous = true;
    }
    return FinalizationArchiveV1Error::kNone;
}

FinalizationArchiveV1Error
EncodeFinalizationArchiveV1Jcs(
    const FinalizationArchiveV1& archive,
    std::string* output) noexcept {
    if (output == nullptr) {
        return FinalizationArchiveV1Error::kNullOutput;
    }
    const FinalizationArchiveV1Error validation =
        ValidateFinalizationArchiveV1(archive);
    if (validation !=
        FinalizationArchiveV1Error::kNone) {
        return validation;
    }
    try {
        std::string encoded;
        encoded.reserve(
            1024U + archive.artifacts.size() * 1024U);
        encoded.append("{\"all_done_generation\":");
        AppendQuotedU64(
            &encoded, archive.all_done_generation);
        encoded.append(",\"all_done_slot_path\":");
        AppendQuoted(
            &encoded, archive.all_done_slot_path);
        encoded.append(",\"all_done_slot_sha256\":");
        AppendHex(
            &encoded, archive.all_done_slot_sha256);
        encoded.append(",\"artifact_count\":");
        AppendQuotedU64(
            &encoded, archive.artifact_count);
        encoded.append(",\"artifacts\":[");
        for (std::size_t index = 0U;
             index < archive.artifacts.size();
             ++index) {
            if (index != 0U) {
                encoded.push_back(',');
            }
            const auto& artifact =
                archive.artifacts[index];
            encoded.append("{\"archive_path\":");
            AppendQuoted(
                &encoded, artifact.archive_path);
            encoded.append(",\"artifact_type\":");
            AppendQuoted(
                &encoded,
                ArtifactTypeName(
                    artifact.artifact_type));
            encoded.append(",\"byte_count\":");
            AppendQuotedU64(
                &encoded, artifact.byte_count);
            encoded.append(",\"capture_date\":");
            AppendU32(
                &encoded,
                artifact.namespace_identity
                    .capture_date);
            encoded.append(",\"sha256\":");
            AppendHex(&encoded, artifact.sha256);
            encoded.append(",\"source_locator\":");
            AppendQuoted(
                &encoded, artifact.source_locator);
            encoded.append(",\"source_stream_id\":");
            AppendU32(
                &encoded,
                artifact.namespace_identity
                    .source_stream_id);
            encoded.append(",\"stream_day_id\":");
            AppendHex(
                &encoded,
                artifact.namespace_identity
                    .stream_day_id);
            encoded.push_back('}');
        }
        encoded.append("],\"finalization_cycle_id\":");
        AppendHex(
            &encoded, archive.finalization_cycle_id);
        encoded.append(
            ",\"immutable_state_header_path\":");
        AppendQuoted(
            &encoded,
            archive.immutable_state_header_path);
        encoded.append(
            ",\"immutable_state_header_sha256\":");
        AppendHex(
            &encoded,
            archive.immutable_state_header_sha256);
        encoded.append(
            ",\"reserve_state_schema_sha256\":");
        AppendHex(
            &encoded,
            archive.reserve_state_schema_sha256);
        encoded.append(",\"reserve_state_uuid\":");
        AppendHex(
            &encoded, archive.reserve_state_uuid);
        encoded.append(",\"schema_version\":");
        AppendU32(&encoded, archive.schema_version);
        encoded.append(",\"state_sha256\":");
        AppendHex(&encoded, archive.state_sha256);
        encoded.push_back('}');
        if (encoded.size() >
            kFinalizationArchiveV1MaximumManifestBytes) {
            return FinalizationArchiveV1Error::
                kEncodedSizeExceeded;
        }
        output->swap(encoded);
        return FinalizationArchiveV1Error::kNone;
    } catch (...) {
        return FinalizationArchiveV1Error::
            kAllocationFailure;
    }
}

FinalizationArchiveV1Error
ParseFinalizationArchiveV1Jcs(
    std::string_view exact_bytes,
    FinalizationArchiveV1* output) noexcept {
    if (output == nullptr) {
        return FinalizationArchiveV1Error::kNullOutput;
    }
    if (exact_bytes.empty() ||
        exact_bytes.size() >
            kFinalizationArchiveV1MaximumManifestBytes) {
        return FinalizationArchiveV1Error::
            kEncodedSizeExceeded;
    }
    try {
        Parser parser(exact_bytes);
        FinalizationArchiveV1 value{};
        if (!parser.Consume(
                "{\"all_done_generation\":") ||
            !parser.ParseQuotedU64(
                &value.all_done_generation) ||
            !parser.Consume(
                ",\"all_done_slot_path\":") ||
            !parser.ParseQuoted(
                &value.all_done_slot_path) ||
            !parser.Consume(
                ",\"all_done_slot_sha256\":") ||
            !parser.ParseHex(
                &value.all_done_slot_sha256) ||
            !parser.Consume(",\"artifact_count\":") ||
            !parser.ParseQuotedU64(
                &value.artifact_count) ||
            value.artifact_count >
                kFinalizationArchiveV1MaximumArtifacts ||
            !parser.Consume(",\"artifacts\":[")) {
            return FinalizationArchiveV1Error::
                kInvalidCanonicalJson;
        }
        value.artifacts.reserve(
            static_cast<std::size_t>(
                value.artifact_count));
        for (std::uint64_t index = 0U;
             index < value.artifact_count;
             ++index) {
            if (index != 0U && !parser.Consume(",")) {
                return FinalizationArchiveV1Error::
                    kInvalidCanonicalJson;
            }
            FinalizationArchiveArtifactV1 artifact{};
            if (!ParseArtifactObject(
                    &parser, &artifact)) {
                return FinalizationArchiveV1Error::
                    kInvalidCanonicalJson;
            }
            value.artifacts.push_back(
                std::move(artifact));
        }
        if (!parser.Consume(
                "],\"finalization_cycle_id\":") ||
            !parser.ParseHex(
                &value.finalization_cycle_id) ||
            !parser.Consume(
                ",\"immutable_state_header_path\":") ||
            !parser.ParseQuoted(
                &value.immutable_state_header_path) ||
            !parser.Consume(
                ",\"immutable_state_header_sha256\":") ||
            !parser.ParseHex(
                &value
                     .immutable_state_header_sha256) ||
            !parser.Consume(
                ",\"reserve_state_schema_sha256\":") ||
            !parser.ParseHex(
                &value
                     .reserve_state_schema_sha256) ||
            !parser.Consume(
                ",\"reserve_state_uuid\":") ||
            !parser.ParseHex(
                &value.reserve_state_uuid) ||
            !parser.Consume(",\"schema_version\":") ||
            !parser.ParseU32(&value.schema_version) ||
            !parser.Consume(",\"state_sha256\":") ||
            !parser.ParseHex(&value.state_sha256) ||
            !parser.Consume("}") ||
            !parser.done()) {
            return FinalizationArchiveV1Error::
                kInvalidCanonicalJson;
        }
        const FinalizationArchiveV1Error validation =
            ValidateFinalizationArchiveV1(value);
        if (validation !=
            FinalizationArchiveV1Error::kNone) {
            return validation;
        }
        std::string canonical;
        const FinalizationArchiveV1Error encode =
            EncodeFinalizationArchiveV1Jcs(
                value, &canonical);
        if (encode !=
                FinalizationArchiveV1Error::kNone ||
            canonical != exact_bytes) {
            return FinalizationArchiveV1Error::
                kInvalidCanonicalJson;
        }
        *output = std::move(value);
        return FinalizationArchiveV1Error::kNone;
    } catch (...) {
        return FinalizationArchiveV1Error::
            kAllocationFailure;
    }
}

FinalizationArchiveV1Error
FinalizationArchiveV1DirectoryName(
    const FinalizationArchiveV1& archive,
    std::string* output) noexcept {
    if (output == nullptr) {
        return FinalizationArchiveV1Error::kNullOutput;
    }
    if (l2flow::common::IsZeroIdentity(
            archive.reserve_state_uuid) ||
        l2flow::common::IsZeroIdentity(
            archive.finalization_cycle_id)) {
        return FinalizationArchiveV1Error::
            kInvalidIdentity;
    }
    try {
        std::string value = "finalization-";
        value.append(
            l2flow::common::Identity128Hex(
                archive.reserve_state_uuid));
        value.push_back('-');
        value.append(
            l2flow::common::Identity128Hex(
                archive.finalization_cycle_id));
        output->swap(value);
        return FinalizationArchiveV1Error::kNone;
    } catch (...) {
        return FinalizationArchiveV1Error::
            kAllocationFailure;
    }
}

FinalizationArchiveV1Error
FinalizationArchiveV1TemporaryDirectoryName(
    const FinalizationArchiveV1& archive,
    std::string* output) noexcept {
    if (output == nullptr) {
        return FinalizationArchiveV1Error::kNullOutput;
    }
    try {
        std::string final_name;
        const FinalizationArchiveV1Error error =
            FinalizationArchiveV1DirectoryName(
                archive, &final_name);
        if (error !=
            FinalizationArchiveV1Error::kNone) {
            return error;
        }
        std::string value;
        value.reserve(final_name.size() + 35U);
        value.push_back('.');
        value.append(final_name);
        value.append(
            ".finalization-archive-v1.tmp");
        output->swap(value);
        return FinalizationArchiveV1Error::kNone;
    } catch (...) {
        return FinalizationArchiveV1Error::
            kAllocationFailure;
    }
}

FinalizationArchiveV1Error
BuildFinalizationArchiveCapabilityV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& all_done_slot,
    std::span<
        const FinalizationArchiveArtifactInputV1>
        artifacts,
    std::unique_ptr<BuiltFinalizationArchiveV1>*
        output) noexcept {
    if (output == nullptr) {
        return FinalizationArchiveV1Error::kNullOutput;
    }
    try {
        ReserveStateV1HeaderWire header_wire{};
        const ReserveStateV1Error header_error =
            EncodeReserveCoordinatorHeaderV1(
                header, &header_wire);
        if (header_error !=
            ReserveStateV1Error::kNone) {
            return FinalizationArchiveV1Error::
                kInvalidStateHeader;
        }
        if (all_done_slot.coordinator_state !=
            ReserveCoordinatorPhaseV1::kConsumed) {
            return FinalizationArchiveV1Error::
                kStateNotConsumed;
        }
        if (all_done_slot.reserve_state_uuid !=
                header.reserve_state_uuid ||
            l2flow::common::IsZeroIdentity(
                all_done_slot.finalization_cycle_id) ||
            all_done_slot.entry_count >
                kReserveStateV1EntryCapacity ||
            all_done_slot.active_entry_index !=
                kReserveStateV1NoActiveEntry) {
            return FinalizationArchiveV1Error::
                kInvalidStateSlot;
        }
        const std::uint16_t expected_completed =
            all_done_slot.entry_count == 0U
                ? std::uint16_t{0U}
                : static_cast<std::uint16_t>(
                      (1U << all_done_slot.entry_count) -
                      1U);
        if (all_done_slot.completed_bitmap !=
            expected_completed) {
            return FinalizationArchiveV1Error::
                kStateNotAllDone;
        }
        for (std::size_t index = 0U;
             index < all_done_slot.entry_count;
             ++index) {
            if (all_done_slot.entries[index]
                    .grant_status !=
                ReserveGrantStatusV1::kDone) {
                return FinalizationArchiveV1Error::
                    kStateNotAllDone;
            }
        }
        ReserveStateV1SlotWire slot_wire{};
        const ReserveStateV1Error slot_error =
            EncodeReserveStateSlotV1(
                header, all_done_slot, &slot_wire);
        if (slot_error !=
            ReserveStateV1Error::kNone) {
            return FinalizationArchiveV1Error::
                kStateEncodingFailure;
        }

        std::vector<ArtifactDraft> drafts;
        FinalizationArchiveV1Error error =
            NormalizeArtifacts(artifacts, &drafts);
        if (error !=
            FinalizationArchiveV1Error::kNone) {
            return error;
        }
        error = CrossValidateArtifacts(
            header, all_done_slot, &drafts);
        if (error !=
            FinalizationArchiveV1Error::kNone) {
            return error;
        }

        FinalizationArchiveV1 model{};
        model.reserve_state_uuid =
            header.reserve_state_uuid;
        model.finalization_cycle_id =
            all_done_slot.finalization_cycle_id;
        model.reserve_state_schema_sha256 =
            header.schema_sha256;
        model.all_done_generation =
            all_done_slot.generation;
        model.immutable_state_header_sha256 =
            l2flow::common::ComputeSha256(
                std::span<const std::byte>(
                    header_wire));
        model.all_done_slot_sha256 =
            l2flow::common::ComputeSha256(
                std::span<const std::byte>(
                    slot_wire));
        l2flow::common::Sha256Hasher state_hasher;
        if (!state_hasher.Update(
                std::span<const std::byte>(
                    header_wire)) ||
            !state_hasher.Update(
                std::span<const std::byte>(
                    slot_wire)) ||
            !state_hasher.Finalize(
                &model.state_sha256)) {
            return FinalizationArchiveV1Error::
                kStateEncodingFailure;
        }

        std::vector<
            BuiltFinalizationArchiveArtifactV1>
            built_artifacts;
        built_artifacts.reserve(drafts.size());
        model.artifacts.reserve(drafts.size());
        for (std::size_t index = 0U;
             index < drafts.size();
             ++index) {
            ArtifactDraft& draft = drafts[index];
            FinalizationArchiveArtifactV1 artifact{};
            if (!BuildArchivePath(
                    index,
                    draft.type,
                    &artifact.archive_path)) {
                return FinalizationArchiveV1Error::
                    kInvalidArchivePath;
            }
            artifact.artifact_type = draft.type;
            artifact.byte_count =
                draft.exact_bytes.size();
            artifact.namespace_identity =
                draft.namespace_identity;
            artifact.sha256 = draft.sha256;
            artifact.source_locator =
                draft.source_locator;
            model.artifacts.push_back(artifact);
            BuiltFinalizationArchiveArtifactV1
                built_artifact(
                    std::move(artifact),
                    std::move(draft.exact_bytes));
            built_artifacts.push_back(
                std::move(built_artifact));
        }
        model.artifact_count =
            model.artifacts.size();
        error = ValidateFinalizationArchiveV1(model);
        if (error !=
            FinalizationArchiveV1Error::kNone) {
            return error;
        }
        std::string canonical_jcs;
        error = EncodeFinalizationArchiveV1Jcs(
            model, &canonical_jcs);
        if (error !=
            FinalizationArchiveV1Error::kNone) {
            return error;
        }
        const RawV1Digest manifest_sha256 =
            l2flow::common::ComputeSha256(
                std::string_view(canonical_jcs));
        std::string directory_name;
        error = FinalizationArchiveV1DirectoryName(
            model, &directory_name);
        if (error !=
            FinalizationArchiveV1Error::kNone) {
            return error;
        }
        std::string temporary_name;
        error =
            FinalizationArchiveV1TemporaryDirectoryName(
                model, &temporary_name);
        if (error !=
            FinalizationArchiveV1Error::kNone) {
            return error;
        }
        std::string directory_locator =
            directory_name;
        directory_locator.push_back('/');
        auto result = std::unique_ptr<
            BuiltFinalizationArchiveV1>(
            new BuiltFinalizationArchiveV1(
                std::move(model),
                std::move(canonical_jcs),
                manifest_sha256,
                std::move(directory_name),
                std::move(directory_locator),
                std::move(temporary_name),
                header_wire,
                slot_wire,
                std::move(built_artifacts)));
        *output = std::move(result);
        return FinalizationArchiveV1Error::kNone;
    } catch (...) {
        return FinalizationArchiveV1Error::
            kAllocationFailure;
    }
}

}  // namespace l2flow::ingress
