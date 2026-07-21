#include "l2flow/ingress/scaffolding_finalization_report_v1.h"

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <new>
#include <string>
#include <string_view>
#include <utility>

namespace l2flow::ingress {
namespace {

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
    const EmptyAnchorNamespaceV1& right) noexcept {
    return left.capture_date == right.capture_date &&
           left.source_stream_id == right.source_stream_id &&
           left.stream_day_id == right.stream_day_id;
}

[[nodiscard]] std::string_view RoleName(
    ScaffoldingObjectRoleV1 role) noexcept {
    switch (role) {
    case ScaffoldingObjectRoleV1::kCaptureDirectory:
        return "CAPTURE_DIRECTORY";
    case ScaffoldingObjectRoleV1::kStreamDirectory:
        return "STREAM_DIRECTORY";
    case ScaffoldingObjectRoleV1::kWriterLeaseTemporary:
        return "WRITER_LEASE_TMP";
    case ScaffoldingObjectRoleV1::kWriterLeaseFinal:
        return "WRITER_LEASE_FINAL";
    case ScaffoldingObjectRoleV1::kMaintenanceDirectory:
        return "MAINTENANCE_DIRECTORY";
    case ScaffoldingObjectRoleV1::kJournalTemporary:
        return "JOURNAL_TMP";
    case ScaffoldingObjectRoleV1::kJournalFinal:
        return "JOURNAL_FINAL";
    }
    return {};
}

[[nodiscard]] std::string_view StartStateName(
    ScaffoldingObjectStartStateV1 state) noexcept {
    switch (state) {
    case ScaffoldingObjectStartStateV1::kAbsent:
        return "ABSENT";
    case ScaffoldingObjectStartStateV1::
        kValidFinalOrCompleteTemporary:
        return "VALID_FINAL_OR_COMPLETE_TMP";
    case ScaffoldingObjectStartStateV1::
        kRecognizedPartialTemporary:
        return "RECOGNIZED_PARTIAL_TMP";
    }
    return {};
}

[[nodiscard]] std::string_view PostStateName(
    ScaffoldingObjectPostStateV1 state) noexcept {
    switch (state) {
    case ScaffoldingObjectPostStateV1::kAbsent:
        return "ABSENT";
    case ScaffoldingObjectPostStateV1::kValidDirectory:
        return "VALID_DIRECTORY";
    case ScaffoldingObjectPostStateV1::kValidFile:
        return "VALID_FILE";
    case ScaffoldingObjectPostStateV1::kHeaderOnlyJournal:
        return "HEADER_ONLY_JOURNAL";
    }
    return {};
}

[[nodiscard]] std::string_view ActionName(
    ScaffoldingActionKindV1 action) noexcept {
    switch (action) {
    case ScaffoldingActionKindV1::
        kEnsureOrValidateParent:
        return "ENSURE_OR_VALIDATE_PARENT";
    case ScaffoldingActionKindV1::
        kCompleteOrRebuildTemporary:
        return "COMPLETE_OR_REBUILD_TMP";
    case ScaffoldingActionKindV1::kAdoptNoReplace:
        return "ADOPT_NOREPLACE";
    case ScaffoldingActionKindV1::
        kCleanupRecognizedPartial:
        return "CLEANUP_RECOGNIZED_PARTIAL_TMP";
    case ScaffoldingActionKindV1::kSyncObject:
        return "SYNC_OBJECT";
    case ScaffoldingActionKindV1::kSyncParent:
        return "SYNC_PARENT";
    case ScaffoldingActionKindV1::
        kRevalidatePostState:
        return "REVALIDATE_POST_STATE";
    }
    return {};
}

[[nodiscard]] ScaffoldingObjectPostStateV1 ExpectedPostState(
    ScaffoldingObjectRoleV1 role) noexcept {
    switch (role) {
    case ScaffoldingObjectRoleV1::kCaptureDirectory:
    case ScaffoldingObjectRoleV1::kStreamDirectory:
    case ScaffoldingObjectRoleV1::kMaintenanceDirectory:
        return ScaffoldingObjectPostStateV1::kValidDirectory;
    case ScaffoldingObjectRoleV1::kWriterLeaseTemporary:
    case ScaffoldingObjectRoleV1::kJournalTemporary:
        return ScaffoldingObjectPostStateV1::kAbsent;
    case ScaffoldingObjectRoleV1::kWriterLeaseFinal:
        return ScaffoldingObjectPostStateV1::kValidFile;
    case ScaffoldingObjectRoleV1::kJournalFinal:
        return ScaffoldingObjectPostStateV1::
            kHeaderOnlyJournal;
    }
    return ScaffoldingObjectPostStateV1::kAbsent;
}

[[nodiscard]] std::uint8_t StartStateWire(
    ScaffoldingObjectStartStateV1 state) noexcept {
    switch (state) {
    case ScaffoldingObjectStartStateV1::kAbsent:
        return 0U;
    case ScaffoldingObjectStartStateV1::
        kValidFinalOrCompleteTemporary:
        return 1U;
    case ScaffoldingObjectStartStateV1::
        kRecognizedPartialTemporary:
        return 2U;
    }
    return 0xffU;
}

void AppendU32(std::string* output, std::uint32_t value) {
    std::array<char, 10U> bytes{};
    const auto result = std::to_chars(
        bytes.data(), bytes.data() + bytes.size(), value);
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
        bytes.data(), bytes.data() + bytes.size(), value);
    if (result.ec != std::errc{}) {
        throw std::bad_alloc();
    }
    output->push_back('"');
    output->append(bytes.data(), result.ptr);
    output->push_back('"');
}

void AppendQuoted(
    std::string* output,
    std::string_view value) {
    output->push_back('"');
    output->append(value);
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
        output->push_back(kDigits[(octet >> 4U) & 0x0fU]);
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
        if (input_.substr(position_, token.size()) != token) {
            return false;
        }
        position_ += token.size();
        return true;
    }

    [[nodiscard]] bool ParseBool(bool* output) noexcept {
        if (output == nullptr) {
            return false;
        }
        if (Consume("true")) {
            *output = true;
            return true;
        }
        if (Consume("false")) {
            *output = false;
            return true;
        }
        return false;
    }

    [[nodiscard]] bool ParseU32(
        std::uint32_t* output) noexcept {
        if (output == nullptr || position_ >= input_.size() ||
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

    template <std::size_t Size>
    [[nodiscard]] bool ParseHex(
        std::array<std::byte, Size>* output) noexcept {
        if (output == nullptr || !Consume("\"") ||
            input_.size() - position_ < Size * 2U + 1U) {
            return false;
        }
        std::array<std::byte, Size> value{};
        for (std::size_t index = 0U; index < Size; ++index) {
            const int high =
                HexNibble(input_[position_ + index * 2U]);
            const int low =
                HexNibble(input_[position_ + index * 2U + 1U]);
            if (high < 0 || low < 0) {
                return false;
            }
            value[index] = static_cast<std::byte>(
                static_cast<unsigned>(high * 16 + low));
        }
        position_ += Size * 2U;
        if (!Consume("\"")) {
            return false;
        }
        *output = value;
        return true;
    }

    [[nodiscard]] bool ParseQuoted(
        std::string_view* output) noexcept {
        if (output == nullptr || !Consume("\"")) {
            return false;
        }
        const std::size_t begin = position_;
        while (position_ < input_.size() &&
               input_[position_] != '"') {
            const unsigned char byte =
                static_cast<unsigned char>(input_[position_]);
            if (byte < 0x20U || input_[position_] == '\\') {
                return false;
            }
            ++position_;
        }
        if (position_ >= input_.size()) {
            return false;
        }
        *output = input_.substr(begin, position_ - begin);
        ++position_;
        return true;
    }

    [[nodiscard]] bool done() const noexcept {
        return position_ == input_.size();
    }

private:
    [[nodiscard]] static int HexNibble(char value) noexcept {
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

[[nodiscard]] bool ParseRole(
    Parser* parser,
    ScaffoldingObjectRoleV1* output) noexcept {
    std::string_view text;
    if (parser == nullptr || output == nullptr ||
        !parser->ParseQuoted(&text)) {
        return false;
    }
    for (std::uint8_t wire = 1U; wire <= 7U; ++wire) {
        const auto value =
            static_cast<ScaffoldingObjectRoleV1>(wire);
        if (text == RoleName(value)) {
            *output = value;
            return true;
        }
    }
    return false;
}

[[nodiscard]] bool ParseStartState(
    Parser* parser,
    ScaffoldingObjectStartStateV1* output) noexcept {
    std::string_view text;
    if (parser == nullptr || output == nullptr ||
        !parser->ParseQuoted(&text)) {
        return false;
    }
    for (std::uint8_t wire = 1U; wire <= 3U; ++wire) {
        const auto value =
            static_cast<ScaffoldingObjectStartStateV1>(wire);
        if (text == StartStateName(value)) {
            *output = value;
            return true;
        }
    }
    return false;
}

[[nodiscard]] bool ParsePostState(
    Parser* parser,
    ScaffoldingObjectPostStateV1* output) noexcept {
    std::string_view text;
    if (parser == nullptr || output == nullptr ||
        !parser->ParseQuoted(&text)) {
        return false;
    }
    for (std::uint8_t wire = 1U; wire <= 4U; ++wire) {
        const auto value =
            static_cast<ScaffoldingObjectPostStateV1>(wire);
        if (text == PostStateName(value)) {
            *output = value;
            return true;
        }
    }
    return false;
}

[[nodiscard]] bool ParseAction(
    Parser* parser,
    ScaffoldingActionKindV1* output) noexcept {
    std::string_view text;
    if (parser == nullptr || output == nullptr ||
        !parser->ParseQuoted(&text)) {
        return false;
    }
    for (std::uint8_t wire = 1U; wire <= 7U; ++wire) {
        const auto value =
            static_cast<ScaffoldingActionKindV1>(wire);
        if (text == ActionName(value)) {
            *output = value;
            return true;
        }
    }
    return false;
}

[[nodiscard]] bool ParseActionObject(
    Parser* parser,
    ScaffoldingActionResultV1* output) noexcept {
    std::uint32_t action_id = 0U;
    ScaffoldingActionResultV1 value{};
    if (parser == nullptr || output == nullptr ||
        !parser->Consume("{\"action_id\":") ||
        !parser->ParseU32(&action_id) ||
        action_id > 255U ||
        !parser->Consume(",\"action_kind\":") ||
        !ParseAction(parser, &value.action_kind) ||
        !parser->Consume(",\"completed\":") ||
        !parser->ParseBool(&value.completed) ||
        !parser->Consume(",\"object_role\":") ||
        !ParseRole(parser, &value.object_role) ||
        !parser->Consume(",\"post_state\":") ||
        !ParsePostState(parser, &value.post_state) ||
        !parser->Consume("}")) {
        return false;
    }
    value.action_id =
        static_cast<std::uint8_t>(action_id);
    *output = value;
    return true;
}

[[nodiscard]] bool ParseObjectState(
    Parser* parser,
    ScaffoldingObjectStateV1* output) noexcept {
    ScaffoldingObjectStateV1 value{};
    if (parser == nullptr || output == nullptr ||
        !parser->Consume(
            "{\"barriers\":{\"object_synced\":") ||
        !parser->ParseBool(
            &value.barriers.object_synced) ||
        !parser->Consume(
            ",\"parent_directory_synced\":") ||
        !parser->ParseBool(
            &value.barriers.parent_directory_synced) ||
        !parser->Consume(
            ",\"retained_fd_revalidated\":") ||
        !parser->ParseBool(
            &value.barriers.retained_fd_revalidated) ||
        !parser->Consume("},\"post_state\":") ||
        !ParsePostState(parser, &value.post_state) ||
        !parser->Consume(",\"role\":") ||
        !ParseRole(parser, &value.role) ||
        !parser->Consume(",\"start_state\":") ||
        !ParseStartState(parser, &value.start_state) ||
        !parser->Consume("}")) {
        return false;
    }
    *output = value;
    return true;
}

[[nodiscard]] const ScaffoldingObjectStateV1*
FindObject(
    const ScaffoldingFinalizationReportV1& report,
    ScaffoldingObjectRoleV1 role) noexcept {
    const auto found = std::find_if(
        report.objects.begin(),
        report.objects.end(),
        [role](const ScaffoldingObjectStateV1& object) {
            return object.role == role;
        });
    return found == report.objects.end() ? nullptr : &*found;
}

}  // namespace

std::string_view
ScaffoldingFinalizationReportV1ErrorName(
    ScaffoldingFinalizationReportV1Error error) noexcept {
    switch (error) {
    case ScaffoldingFinalizationReportV1Error::kNone:
        return "none";
    case ScaffoldingFinalizationReportV1Error::kNullOutput:
        return "null_output";
    case ScaffoldingFinalizationReportV1Error::kInvalidArgument:
        return "invalid_argument";
    case ScaffoldingFinalizationReportV1Error::kInvalidNamespace:
        return "invalid_namespace";
    case ScaffoldingFinalizationReportV1Error::kInvalidIdentity:
        return "invalid_identity";
    case ScaffoldingFinalizationReportV1Error::kInvalidSnapshot:
        return "invalid_snapshot";
    case ScaffoldingFinalizationReportV1Error::
        kInvalidObjectVector:
        return "invalid_object_vector";
    case ScaffoldingFinalizationReportV1Error::
        kInvalidActionVector:
        return "invalid_action_vector";
    case ScaffoldingFinalizationReportV1Error::kInvalidPostState:
        return "invalid_post_state";
    case ScaffoldingFinalizationReportV1Error::kNotHeaderOnly:
        return "not_header_only";
    case ScaffoldingFinalizationReportV1Error::
        kGrantKindMismatch:
        return "grant_kind_mismatch";
    case ScaffoldingFinalizationReportV1Error::kSidecarMismatch:
        return "sidecar_mismatch";
    case ScaffoldingFinalizationReportV1Error::
        kInvalidCanonicalJson:
        return "invalid_canonical_json";
    case ScaffoldingFinalizationReportV1Error::kFilenameInvalid:
        return "filename_invalid";
    case ScaffoldingFinalizationReportV1Error::
        kEncodedSizeExceeded:
        return "encoded_size_exceeded";
    case ScaffoldingFinalizationReportV1Error::kAllocationFailure:
        return "allocation_failure";
    }
    return "unknown";
}

ScaffoldingFinalizationReportV1Error
ValidateScaffoldingFinalizationReportV1(
    const ScaffoldingFinalizationReportV1& report) noexcept {
    if (report.schema_version !=
        kScaffoldingFinalizationReportV1SchemaVersion) {
        return ScaffoldingFinalizationReportV1Error::
            kInvalidArgument;
    }
    if (report.planned_namespace.capture_date == 0U ||
        report.planned_namespace.source_stream_id == 0U ||
        common::IsZeroIdentity(
            report.planned_namespace.stream_day_id)) {
        return ScaffoldingFinalizationReportV1Error::
            kInvalidNamespace;
    }
    if (common::IsZeroIdentity(report.reserve_state_uuid) ||
        common::IsZeroIdentity(
            report.finalization_cycle_id) ||
        common::IsZeroIdentity(
            report.planned_recovery_attempt_id) ||
        IsZero(report.immutable_grant_sha256) ||
        IsZero(report.journal_header_sha256) ||
        IsZero(report.empty_anchor_tombstone_sha256)) {
        return ScaffoldingFinalizationReportV1Error::
            kInvalidIdentity;
    }
    if (IsZero(report.object_snapshot_sha256) ||
        report.required_action_bitmap == 0U) {
        return ScaffoldingFinalizationReportV1Error::
            kInvalidSnapshot;
    }
    if (report.marker_count != 0U ||
        report.segment_count != 0U ||
        report.record_count != 0U ||
        !report.index_absent ||
        !report.manifest_absent ||
        !report.control_absent) {
        return ScaffoldingFinalizationReportV1Error::
            kNotHeaderOnly;
    }
    if (report.objects.size() != 7U ||
        report.objects.size() >
            kScaffoldingFinalizationReportV1MaximumObjects) {
        return ScaffoldingFinalizationReportV1Error::
            kInvalidObjectVector;
    }
    std::uint64_t expected_observed_bitmap = 0U;
    for (std::size_t index = 0U;
         index < report.objects.size();
         ++index) {
        const auto& object = report.objects[index];
        const auto expected_role =
            static_cast<ScaffoldingObjectRoleV1>(
                static_cast<std::uint8_t>(index + 1U));
        if (object.role != expected_role ||
            RoleName(object.role).empty() ||
            StartStateName(object.start_state).empty() ||
            PostStateName(object.post_state).empty()) {
            return ScaffoldingFinalizationReportV1Error::
                kInvalidObjectVector;
        }
        const std::uint8_t start_wire =
            StartStateWire(object.start_state);
        if (start_wire > 2U) {
            return ScaffoldingFinalizationReportV1Error::
                kInvalidObjectVector;
        }
        expected_observed_bitmap |=
            static_cast<std::uint64_t>(start_wire)
            << static_cast<unsigned>(index * 4U);
        if (object.post_state !=
                ExpectedPostState(object.role) ||
            !object.barriers.parent_directory_synced ||
            !object.barriers.retained_fd_revalidated ||
            (object.post_state ==
                 ScaffoldingObjectPostStateV1::kAbsent
                 ? object.barriers.object_synced
                 : !object.barriers.object_synced)) {
            return ScaffoldingFinalizationReportV1Error::
                kInvalidPostState;
        }
    }
    if (report.observed_object_bitmap !=
        expected_observed_bitmap) {
        return ScaffoldingFinalizationReportV1Error::
            kInvalidObjectVector;
    }

    if (report.actions.empty() ||
        report.actions.size() >
            kScaffoldingFinalizationReportV1MaximumActions) {
        return ScaffoldingFinalizationReportV1Error::
            kInvalidActionVector;
    }
    std::uint64_t action_bitmap = 0U;
    std::uint8_t previous_id = 0U;
    bool has_previous = false;
    for (const auto& action : report.actions) {
        if (action.action_id >= 16U ||
            ActionName(action.action_kind).empty() ||
            RoleName(action.object_role).empty() ||
            PostStateName(action.post_state).empty() ||
            !action.completed ||
            (has_previous &&
             action.action_id <= previous_id)) {
            return ScaffoldingFinalizationReportV1Error::
                kInvalidActionVector;
        }
        const ScaffoldingObjectStateV1* const object =
            FindObject(report, action.object_role);
        if (object == nullptr ||
            object->post_state != action.post_state) {
            return ScaffoldingFinalizationReportV1Error::
                kInvalidActionVector;
        }
        action_bitmap |=
            std::uint64_t{1U} << action.action_id;
        previous_id = action.action_id;
        has_previous = true;
    }
    if (action_bitmap != report.required_action_bitmap) {
        return ScaffoldingFinalizationReportV1Error::
            kInvalidActionVector;
    }
    return ScaffoldingFinalizationReportV1Error::kNone;
}

ScaffoldingFinalizationReportV1Error
EncodeScaffoldingFinalizationReportV1Jcs(
    const ScaffoldingFinalizationReportV1& report,
    std::string* output) noexcept {
    if (output == nullptr) {
        return ScaffoldingFinalizationReportV1Error::
            kNullOutput;
    }
    const auto validation =
        ValidateScaffoldingFinalizationReportV1(report);
    if (validation !=
        ScaffoldingFinalizationReportV1Error::kNone) {
        return validation;
    }
    try {
        std::string encoded;
        encoded.reserve(8192U);
        encoded.append("{\"actions\":[");
        for (std::size_t index = 0U;
             index < report.actions.size();
             ++index) {
            if (index != 0U) {
                encoded.push_back(',');
            }
            const auto& action = report.actions[index];
            encoded.append("{\"action_id\":");
            AppendU32(&encoded, action.action_id);
            encoded.append(",\"action_kind\":");
            AppendQuoted(
                &encoded, ActionName(action.action_kind));
            encoded.append(",\"completed\":true");
            encoded.append(",\"object_role\":");
            AppendQuoted(
                &encoded, RoleName(action.object_role));
            encoded.append(",\"post_state\":");
            AppendQuoted(
                &encoded, PostStateName(action.post_state));
            encoded.push_back('}');
        }
        encoded.append("],\"capture_date\":");
        AppendU32(
            &encoded, report.planned_namespace.capture_date);
        encoded.append(",\"control_absent\":true");
        encoded.append(
            ",\"empty_anchor_tombstone_sha256\":");
        AppendHex(
            &encoded,
            report.empty_anchor_tombstone_sha256);
        encoded.append(",\"finalization_cycle_id\":");
        AppendHex(
            &encoded, report.finalization_cycle_id);
        encoded.append(",\"immutable_grant_sha256\":");
        AppendHex(
            &encoded, report.immutable_grant_sha256);
        encoded.append(",\"index_absent\":true");
        encoded.append(",\"journal_header_sha256\":");
        AppendHex(&encoded, report.journal_header_sha256);
        encoded.append(",\"manifest_absent\":true");
        encoded.append(",\"marker_count\":");
        AppendQuotedU64(&encoded, report.marker_count);
        encoded.append(",\"object_snapshot_sha256\":");
        AppendHex(
            &encoded, report.object_snapshot_sha256);
        encoded.append(",\"objects\":[");
        for (std::size_t index = 0U;
             index < report.objects.size();
             ++index) {
            if (index != 0U) {
                encoded.push_back(',');
            }
            const auto& object = report.objects[index];
            encoded.append(
                "{\"barriers\":{\"object_synced\":");
            encoded.append(
                object.barriers.object_synced
                    ? "true"
                    : "false");
            encoded.append(
                ",\"parent_directory_synced\":true"
                ",\"retained_fd_revalidated\":true}");
            encoded.append(",\"post_state\":");
            AppendQuoted(
                &encoded, PostStateName(object.post_state));
            encoded.append(",\"role\":");
            AppendQuoted(
                &encoded, RoleName(object.role));
            encoded.append(",\"start_state\":");
            AppendQuoted(
                &encoded,
                StartStateName(object.start_state));
            encoded.push_back('}');
        }
        encoded.append("],\"observed_object_bitmap\":");
        AppendQuotedU64(
            &encoded, report.observed_object_bitmap);
        encoded.append(
            ",\"planned_recovery_attempt_id\":");
        AppendHex(
            &encoded,
            report.planned_recovery_attempt_id);
        encoded.append(",\"record_count\":");
        AppendQuotedU64(&encoded, report.record_count);
        encoded.append(",\"required_action_bitmap\":");
        AppendQuotedU64(
            &encoded, report.required_action_bitmap);
        encoded.append(",\"reserve_state_uuid\":");
        AppendHex(&encoded, report.reserve_state_uuid);
        encoded.append(
            ",\"result\":\"EMPTY_ANCHOR_ONLY\""
            ",\"schema_version\":");
        AppendU32(&encoded, report.schema_version);
        encoded.append(
            ",\"sealed_raw_certificate_sha256\":null"
            ",\"segment_count\":");
        AppendQuotedU64(&encoded, report.segment_count);
        encoded.append(",\"source_stream_id\":");
        AppendU32(
            &encoded,
            report.planned_namespace.source_stream_id);
        encoded.append(",\"stream_day_id\":");
        AppendHex(
            &encoded,
            report.planned_namespace.stream_day_id);
        encoded.push_back('}');
        if (encoded.size() >
            kScaffoldingFinalizationReportV1MaximumBytes) {
            return ScaffoldingFinalizationReportV1Error::
                kEncodedSizeExceeded;
        }
        output->swap(encoded);
        return ScaffoldingFinalizationReportV1Error::kNone;
    } catch (const std::bad_alloc&) {
        return ScaffoldingFinalizationReportV1Error::
            kAllocationFailure;
    }
}

ScaffoldingFinalizationReportV1Error
ParseScaffoldingFinalizationReportV1Jcs(
    std::string_view exact_bytes,
    ScaffoldingFinalizationReportV1* output) noexcept {
    if (output == nullptr) {
        return ScaffoldingFinalizationReportV1Error::
            kNullOutput;
    }
    if (exact_bytes.empty() ||
        exact_bytes.size() >
            kScaffoldingFinalizationReportV1MaximumBytes) {
        return exact_bytes.size() >
                       kScaffoldingFinalizationReportV1MaximumBytes
                   ? ScaffoldingFinalizationReportV1Error::
                         kEncodedSizeExceeded
                   : ScaffoldingFinalizationReportV1Error::
                         kInvalidCanonicalJson;
    }
    try {
        Parser parser(exact_bytes);
        ScaffoldingFinalizationReportV1 value{};
        if (!parser.Consume("{\"actions\":[")) {
            return ScaffoldingFinalizationReportV1Error::
                kInvalidCanonicalJson;
        }
        if (!parser.Consume("]")) {
            for (;;) {
                if (value.actions.size() >=
                    kScaffoldingFinalizationReportV1MaximumActions) {
                    return
                        ScaffoldingFinalizationReportV1Error::
                            kInvalidCanonicalJson;
                }
                ScaffoldingActionResultV1 action{};
                if (!ParseActionObject(&parser, &action)) {
                    return
                        ScaffoldingFinalizationReportV1Error::
                            kInvalidCanonicalJson;
                }
                value.actions.push_back(action);
                if (parser.Consume("]")) {
                    break;
                }
                if (!parser.Consume(",")) {
                    return
                        ScaffoldingFinalizationReportV1Error::
                            kInvalidCanonicalJson;
                }
            }
        }
        if (!parser.Consume(",\"capture_date\":") ||
            !parser.ParseU32(
                &value.planned_namespace.capture_date) ||
            !parser.Consume(",\"control_absent\":") ||
            !parser.ParseBool(&value.control_absent) ||
            !parser.Consume(
                ",\"empty_anchor_tombstone_sha256\":") ||
            !parser.ParseHex(
                &value.empty_anchor_tombstone_sha256) ||
            !parser.Consume(
                ",\"finalization_cycle_id\":") ||
            !parser.ParseHex(
                &value.finalization_cycle_id) ||
            !parser.Consume(
                ",\"immutable_grant_sha256\":") ||
            !parser.ParseHex(
                &value.immutable_grant_sha256) ||
            !parser.Consume(",\"index_absent\":") ||
            !parser.ParseBool(&value.index_absent) ||
            !parser.Consume(
                ",\"journal_header_sha256\":") ||
            !parser.ParseHex(
                &value.journal_header_sha256) ||
            !parser.Consume(",\"manifest_absent\":") ||
            !parser.ParseBool(&value.manifest_absent) ||
            !parser.Consume(",\"marker_count\":") ||
            !parser.ParseQuotedU64(&value.marker_count) ||
            !parser.Consume(
                ",\"object_snapshot_sha256\":") ||
            !parser.ParseHex(
                &value.object_snapshot_sha256) ||
            !parser.Consume(",\"objects\":[")) {
            return ScaffoldingFinalizationReportV1Error::
                kInvalidCanonicalJson;
        }
        if (!parser.Consume("]")) {
            for (;;) {
                if (value.objects.size() >=
                    kScaffoldingFinalizationReportV1MaximumObjects) {
                    return
                        ScaffoldingFinalizationReportV1Error::
                            kInvalidCanonicalJson;
                }
                ScaffoldingObjectStateV1 object{};
                if (!ParseObjectState(&parser, &object)) {
                    return
                        ScaffoldingFinalizationReportV1Error::
                            kInvalidCanonicalJson;
                }
                value.objects.push_back(object);
                if (parser.Consume("]")) {
                    break;
                }
                if (!parser.Consume(",")) {
                    return
                        ScaffoldingFinalizationReportV1Error::
                            kInvalidCanonicalJson;
                }
            }
        }
        std::string_view result;
        if (!parser.Consume(
                ",\"observed_object_bitmap\":") ||
            !parser.ParseQuotedU64(
                &value.observed_object_bitmap) ||
            !parser.Consume(
                ",\"planned_recovery_attempt_id\":") ||
            !parser.ParseHex(
                &value.planned_recovery_attempt_id) ||
            !parser.Consume(",\"record_count\":") ||
            !parser.ParseQuotedU64(&value.record_count) ||
            !parser.Consume(
                ",\"required_action_bitmap\":") ||
            !parser.ParseQuotedU64(
                &value.required_action_bitmap) ||
            !parser.Consume(",\"reserve_state_uuid\":") ||
            !parser.ParseHex(&value.reserve_state_uuid) ||
            !parser.Consume(",\"result\":") ||
            !parser.ParseQuoted(&result) ||
            result != "EMPTY_ANCHOR_ONLY" ||
            !parser.Consume(",\"schema_version\":") ||
            !parser.ParseU32(&value.schema_version) ||
            !parser.Consume(
                ",\"sealed_raw_certificate_sha256\":null"
                ",\"segment_count\":") ||
            !parser.ParseQuotedU64(&value.segment_count) ||
            !parser.Consume(",\"source_stream_id\":") ||
            !parser.ParseU32(
                &value.planned_namespace.source_stream_id) ||
            !parser.Consume(",\"stream_day_id\":") ||
            !parser.ParseHex(
                &value.planned_namespace.stream_day_id) ||
            !parser.Consume("}") || !parser.done()) {
            return ScaffoldingFinalizationReportV1Error::
                kInvalidCanonicalJson;
        }
        const auto validation =
            ValidateScaffoldingFinalizationReportV1(value);
        if (validation !=
            ScaffoldingFinalizationReportV1Error::kNone) {
            return validation;
        }
        std::string canonical;
        const auto encoded =
            EncodeScaffoldingFinalizationReportV1Jcs(
                value, &canonical);
        if (encoded !=
            ScaffoldingFinalizationReportV1Error::kNone) {
            return encoded;
        }
        if (canonical != exact_bytes) {
            return ScaffoldingFinalizationReportV1Error::
                kInvalidCanonicalJson;
        }
        *output = std::move(value);
        return ScaffoldingFinalizationReportV1Error::kNone;
    } catch (const std::bad_alloc&) {
        return ScaffoldingFinalizationReportV1Error::
            kAllocationFailure;
    }
}

ScaffoldingFinalizationReportV1Error
ScaffoldingFinalizationReportV1Filename(
    const ScaffoldingFinalizationReportV1& report,
    std::string* output) noexcept {
    if (output == nullptr) {
        return ScaffoldingFinalizationReportV1Error::
            kNullOutput;
    }
    if (common::IsZeroIdentity(
            report.finalization_cycle_id) ||
        IsZero(report.immutable_grant_sha256)) {
        return ScaffoldingFinalizationReportV1Error::
            kFilenameInvalid;
    }
    try {
        std::string filename("scaffolding-");
        filename.append(
            common::Identity128Hex(
                report.finalization_cycle_id));
        filename.push_back('-');
        filename.append(
            common::Sha256Hex(
                report.immutable_grant_sha256));
        filename.append(".json");
        if (filename.size() != 114U) {
            return ScaffoldingFinalizationReportV1Error::
                kFilenameInvalid;
        }
        output->swap(filename);
        return ScaffoldingFinalizationReportV1Error::kNone;
    } catch (const std::bad_alloc&) {
        return ScaffoldingFinalizationReportV1Error::
            kAllocationFailure;
    }
}

ScaffoldingFinalizationReportV1Error
BuildScaffoldingFinalizationReportCapabilityV1(
    const ScaffoldingFinalizationReportV1& report,
    std::uint8_t immutable_grant_flags,
    const BuiltEmptyAnchorTombstoneV1& empty_tombstone,
    std::unique_ptr<
        BuiltScaffoldingFinalizationReportV1>*
        output) noexcept {
    if (output == nullptr) {
        return ScaffoldingFinalizationReportV1Error::
            kNullOutput;
    }
    const auto validation =
        ValidateScaffoldingFinalizationReportV1(report);
    if (validation !=
        ScaffoldingFinalizationReportV1Error::kNone) {
        return validation;
    }
    if (immutable_grant_flags !=
        kReserveGrantScaffoldingOnly) {
        return ScaffoldingFinalizationReportV1Error::
            kGrantKindMismatch;
    }
    const auto& tombstone = empty_tombstone.model();
    if (!SameNamespace(
            report.planned_namespace,
            tombstone.namespace_identity) ||
        report.journal_header_sha256 !=
            tombstone.journal_header_sha256 ||
        tombstone.marker_count != 0U ||
        tombstone.segment_count != 0U ||
        tombstone.record_count != 0U ||
        empty_tombstone.tombstone_sha256() !=
            report.empty_anchor_tombstone_sha256) {
        return ScaffoldingFinalizationReportV1Error::
            kSidecarMismatch;
    }
    try {
        std::string canonical;
        auto error =
            EncodeScaffoldingFinalizationReportV1Jcs(
                report, &canonical);
        if (error !=
            ScaffoldingFinalizationReportV1Error::kNone) {
            return error;
        }
        std::string filename;
        error = ScaffoldingFinalizationReportV1Filename(
            report, &filename);
        if (error !=
            ScaffoldingFinalizationReportV1Error::kNone) {
            return error;
        }
        const RawV1Digest digest =
            common::ComputeSha256(canonical);
        auto built = std::unique_ptr<
            BuiltScaffoldingFinalizationReportV1>(
            new BuiltScaffoldingFinalizationReportV1(
                report,
                std::move(canonical),
                std::move(filename),
                digest));
        output->swap(built);
        return ScaffoldingFinalizationReportV1Error::kNone;
    } catch (const std::bad_alloc&) {
        return ScaffoldingFinalizationReportV1Error::
            kAllocationFailure;
    }
}

}  // namespace l2flow::ingress
