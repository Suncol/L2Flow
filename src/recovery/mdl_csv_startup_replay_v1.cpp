#include "l2flow/recovery/startup_replay_v1.h"

#include "mdl_shl2_msg.h"
#include "mdl_szl2_msg.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <new>
#include <optional>
#include <set>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#if defined(__linux__)
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace l2flow::recovery {
namespace {

namespace mdl = datayes::mdl;
namespace sh = datayes::mdl::mdl_shl2_msg;
namespace sz = datayes::mdl::mdl_szl2_msg;

constexpr std::uint16_t kServiceVersion = 101U;
constexpr std::size_t kMaximumQueueItems = 50U;
constexpr std::size_t kPublishedDepth = 10U;
constexpr std::string_view kShenzhenSdkSecurityIdSource = "102 ";

static_assert(sizeof(mdl::MDLMessageHead) == 23U);
static_assert(sizeof(sh::SHL2MarketData) == 248U);
static_assert(sizeof(sh::NGTSTick) == 70U);
static_assert(sizeof(sz::Snapshot300111_v2) == 224U);
static_assert(sizeof(sz::Order300192_v2) == 58U);
static_assert(sizeof(sz::Transaction300191_v2) == 70U);

class StableInputFile final {
public:
#if defined(__linux__)
    explicit StableInputFile(int descriptor) noexcept
        : descriptor_(descriptor) {}
    ~StableInputFile() {
        if (descriptor_ >= 0) {
            static_cast<void>(::close(descriptor_));
        }
    }

    StableInputFile(const StableInputFile&) = delete;
    StableInputFile& operator=(const StableInputFile&) = delete;

    [[nodiscard]] int descriptor() const noexcept {
        return descriptor_;
    }

    bool CurrentSize(
        std::uint64_t* output,
        std::string* detail) const {
        struct stat status {};
        if (::fstat(descriptor_, &status) != 0) {
            if (detail != nullptr) {
                *detail = std::strerror(errno);
            }
            return false;
        }
        if (!S_ISREG(status.st_mode) || status.st_size < 0) {
            if (detail != nullptr) {
                *detail = "opened CSV is no longer a regular file";
            }
            return false;
        }
        *output = static_cast<std::uint64_t>(status.st_size);
        return true;
    }

private:
    int descriptor_ = -1;
#else
    explicit StableInputFile(int) noexcept {}
    [[nodiscard]] int descriptor() const noexcept {
        return -1;
    }
    bool CurrentSize(std::uint64_t*, std::string* detail) const {
        if (detail != nullptr) {
            *detail =
                "stable CSV descriptors require Linux in replay v1";
        }
        return false;
    }
#endif
};

struct InputFile final {
    std::filesystem::path path;
    std::uint64_t prefix_bytes = 0U;
    std::shared_ptr<StableInputFile> stable;
};

struct CsvRecord final {
    std::vector<std::string> fields;
    std::uint64_t line = 0U;
    std::uint64_t start_offset = 0U;
    std::uint64_t end_offset = 0U;
};

class CsvRecordScanAccount final {
public:
    CsvRecordScanAccount(
        bool enabled,
        const std::size_t* record_bytes,
        std::uint64_t* total_bytes) noexcept
        : enabled_(enabled),
          record_bytes_(record_bytes),
          total_bytes_(total_bytes) {}

    CsvRecordScanAccount(const CsvRecordScanAccount&) = delete;
    CsvRecordScanAccount& operator=(const CsvRecordScanAccount&) = delete;

    ~CsvRecordScanAccount() noexcept {
        if (!enabled_ || record_bytes_ == nullptr ||
            total_bytes_ == nullptr) {
            return;
        }
        const std::uint64_t bytes =
            static_cast<std::uint64_t>(*record_bytes_);
        *total_bytes_ =
            bytes > std::numeric_limits<std::uint64_t>::max() -
                        *total_bytes_
                ? std::numeric_limits<std::uint64_t>::max()
                : *total_bytes_ + bytes;
    }

private:
    bool enabled_;
    const std::size_t* record_bytes_;
    std::uint64_t* total_bytes_;
};

struct TransparentStringHash final {
    using is_transparent = void;

    [[nodiscard]] std::size_t operator()(
        std::string_view value) const noexcept {
        return std::hash<std::string_view>{}(value);
    }
    [[nodiscard]] std::size_t operator()(
        const std::string& value) const noexcept {
        return (*this)(std::string_view(value));
    }
};

struct TransparentStringEqual final {
    using is_transparent = void;

    [[nodiscard]] bool operator()(
        std::string_view left,
        std::string_view right) const noexcept {
        return left == right;
    }
};

bool IsValidUtf8(std::string_view value) noexcept {
    const auto* data =
        reinterpret_cast<const unsigned char*>(value.data());
    std::size_t index = 0U;
    while (index < value.size()) {
        const unsigned char first = data[index];
        if (first == 0U) {
            return false;
        }
        if (first <= 0x7fU) {
            ++index;
            continue;
        }
        std::size_t continuation_count = 0U;
        std::uint32_t code_point = 0U;
        if (first >= 0xc2U && first <= 0xdfU) {
            continuation_count = 1U;
            code_point = static_cast<std::uint32_t>(first & 0x1fU);
        } else if (first >= 0xe0U && first <= 0xefU) {
            continuation_count = 2U;
            code_point = static_cast<std::uint32_t>(first & 0x0fU);
        } else if (first >= 0xf0U && first <= 0xf4U) {
            continuation_count = 3U;
            code_point = static_cast<std::uint32_t>(first & 0x07U);
        } else {
            return false;
        }
        if (continuation_count > value.size() - index - 1U) {
            return false;
        }
        for (std::size_t offset = 1U;
             offset <= continuation_count;
             ++offset) {
            const unsigned char next = data[index + offset];
            if ((next & 0xc0U) != 0x80U) {
                return false;
            }
            code_point =
                (code_point << 6U) |
                static_cast<std::uint32_t>(next & 0x3fU);
        }
        if ((continuation_count == 2U && code_point < 0x800U) ||
            (continuation_count == 3U && code_point < 0x10000U) ||
            code_point > 0x10ffffU ||
            (code_point >= 0xd800U && code_point <= 0xdfffU)) {
            return false;
        }
        index += continuation_count + 1U;
    }
    return true;
}

bool IsAscii(std::string_view value) noexcept {
    return std::all_of(
        value.begin(), value.end(), [](char character) {
            const unsigned char byte =
                static_cast<unsigned char>(character);
            return byte != 0U && byte <= 0x7fU;
        });
}

class ReplayState final {
public:
    ReplayState(
        const StartupReplayConfigV1& replay_config,
        StartupReplaySinkV1& replay_sink) noexcept
        : config(replay_config), sink(replay_sink) {}

    bool Fail(
        StartupReplayErrorV1 error,
        const std::filesystem::path& file,
        std::uint64_t line,
        std::string detail) {
        if (result.error == StartupReplayErrorV1::kNone) {
            result.error = error;
            result.error_file = file;
            result.error_line = line;
            result.detail = std::move(detail);
        }
        return false;
    }

    StartupReplaySinkCallResultV1 ObserveCompleteRecord(
        const std::filesystem::path& file,
        std::uint64_t line,
        std::optional<std::chrono::steady_clock::time_point>
            deadline = std::nullopt) {
        ++records_since_cooperative_checkpoint_;
        if (records_since_cooperative_checkpoint_ <
            kStartupReplayCooperativeCheckpointRecordsV1) {
            return StartupReplaySinkCallResultV1::kAccepted;
        }
        records_since_cooperative_checkpoint_ = 0U;

        if (deadline.has_value()) {
            return CooperativeCheckpointUntil(
                file, line, *deadline);
        }
        return CooperativeCheckpoint(file, line)
                   ? StartupReplaySinkCallResultV1::kAccepted
                   : StartupReplaySinkCallResultV1::kRejected;
    }

    bool CooperativeCheckpoint(
        const std::filesystem::path& file,
        std::uint64_t line) {
        std::string detail;
        if (sink.CooperativeCheckpoint(&detail)) {
            return true;
        }
        return Fail(
            StartupReplayErrorV1::kSinkRejected,
            file,
            line,
            detail.empty()
                ? "startup replay cooperative checkpoint was rejected"
                : std::move(detail));
    }

    StartupReplaySinkCallResultV1 CooperativeCheckpointUntil(
        const std::filesystem::path& file,
        std::uint64_t line,
        std::chrono::steady_clock::time_point deadline) {
        std::string detail;
        const StartupReplaySinkCallResultV1 checkpoint_result =
            sink.CooperativeCheckpointUntil(deadline, &detail);
        if (checkpoint_result !=
            StartupReplaySinkCallResultV1::kRejected) {
            return checkpoint_result;
        }
        static_cast<void>(Fail(
            StartupReplayErrorV1::kSinkRejected,
            file,
            line,
            detail.empty()
                ? "startup replay cooperative checkpoint was rejected"
                : std::move(detail)));
        return StartupReplaySinkCallResultV1::kRejected;
    }

    const StartupReplayConfigV1& config;
    StartupReplaySinkV1& sink;
    StartupReplayResultV1 result{};

private:
    std::size_t records_since_cooperative_checkpoint_ = 0U;
};

class CsvRecordReader final {
public:
    CsvRecordReader(
        const InputFile& input,
        std::size_t maximum_record_bytes,
        ReplayState* state)
        : input_(input),
          maximum_record_bytes_(maximum_record_bytes),
          state_(state),
          prefix_end_(input.prefix_bytes),
          remaining_(input.prefix_bytes) {
        if (input_.stable == nullptr ||
            input_.stable->descriptor() < 0) {
            static_cast<void>(state_->Fail(
                StartupReplayErrorV1::kFileOpen,
                input_.path,
                0U,
                "captured CSV descriptor is unavailable"));
        }
    }

    [[nodiscard]] bool usable() const noexcept {
        return input_.stable != nullptr &&
               input_.stable->descriptor() >= 0 &&
               state_->result.error == StartupReplayErrorV1::kNone;
    }

    bool Next(
        CsvRecord* output,
        bool* available,
        std::size_t maximum_scan_bytes =
            std::numeric_limits<std::size_t>::max()) {
        if (output == nullptr || available == nullptr) {
            return state_->Fail(
                StartupReplayErrorV1::kUnexpectedFailure,
                input_.path,
                physical_line_,
                "internal null CSV output");
        }
        *available = false;
        output->fields.clear();
        output->line = physical_line_;
        output->start_offset = consumed_offset_;
        output->end_offset = consumed_offset_;
        if (remaining_ == 0U) {
            return true;
        }
        const std::uint64_t record_start_offset = consumed_offset_;
        const std::uint64_t record_start_line = physical_line_;

        enum class FieldState : std::uint8_t {
            kStart,
            kUnquoted,
            kQuoted,
            kQuoteClosed,
        };
        FieldState field_state = FieldState::kStart;
        std::string field;
        std::size_t record_bytes = 0U;
        const CsvRecordScanAccount scan_account(
            scan_accounting_enabled_,
            &record_bytes,
            &bytes_read_);
        const std::size_t effective_maximum_bytes =
            std::min(maximum_record_bytes_, maximum_scan_bytes);

        const auto finish_field = [&]() {
            output->fields.push_back(std::move(field));
            field.clear();
            field_state = FieldState::kStart;
        };
        const auto finish_record = [&]() -> bool {
            finish_field();
            for (const std::string& value : output->fields) {
                if (!IsValidUtf8(value)) {
                    return state_->Fail(
                        StartupReplayErrorV1::kUtf8Invalid,
                        input_.path,
                        output->line,
                        "record contains invalid UTF-8 or NUL");
                }
            }
            if (scan_accounting_enabled_ &&
                complete_records_read_ !=
                    std::numeric_limits<std::uint64_t>::max()) {
                ++complete_records_read_;
            }
            const StartupReplaySinkCallResultV1 record_checkpoint =
                state_->ObserveCompleteRecord(
                    input_.path,
                    output->line,
                    cooperative_deadline_);
            if (record_checkpoint ==
                StartupReplaySinkCallResultV1::kDeadline) {
                checkpoint_deadline_expired_ = true;
                return false;
            }
            if (record_checkpoint ==
                StartupReplaySinkCallResultV1::kRejected) {
                return false;
            }
            output->start_offset = record_start_offset;
            output->end_offset = consumed_offset_;
            *available = true;
            return true;
        };

        while (remaining_ != 0U) {
            if (record_bytes == effective_maximum_bytes) {
                return state_->Fail(
                    maximum_scan_bytes < maximum_record_bytes_
                        ? StartupReplayErrorV1::kResourceExhausted
                        : StartupReplayErrorV1::kLineTooLong,
                    input_.path,
                    output->line,
                    maximum_scan_bytes < maximum_record_bytes_
                        ? "Shenzhen boundary alignment scan byte "
                          "limit was exhausted within a record"
                        : "CSV logical record exceeds "
                          "maximum_record_bytes");
            }
            char character = '\0';
            if (!ReadByte(
                    &character,
                    output->line,
                    effective_maximum_bytes - record_bytes)) {
                return false;
            }
            ++record_bytes;

            if (field_state == FieldState::kQuoted) {
                if (character == '"') {
                    field_state = FieldState::kQuoteClosed;
                } else {
                    field.push_back(character);
                    if (character == '\n') {
                        ++physical_line_;
                    }
                }
                continue;
            }

            if (field_state == FieldState::kQuoteClosed) {
                if (character == '"') {
                    field.push_back('"');
                    field_state = FieldState::kQuoted;
                    continue;
                }
                if (character == ',') {
                    finish_field();
                    continue;
                }
                if (character == '\n') {
                    ++physical_line_;
                    return finish_record();
                }
                if (character == '\r') {
                    if (!ConsumeRecordLf(
                            &record_bytes,
                            output->line,
                            record_start_offset,
                            record_start_line,
                            maximum_scan_bytes)) {
                        return state_->result.error ==
                               StartupReplayErrorV1::kNone;
                    }
                    ++physical_line_;
                    return finish_record();
                }
                return state_->Fail(
                    StartupReplayErrorV1::kCsvMalformed,
                    input_.path,
                    output->line,
                    "character after closing quote is not a delimiter");
            }

            if (field_state == FieldState::kStart &&
                character == '"') {
                field_state = FieldState::kQuoted;
                continue;
            }
            if (character == '"') {
                return state_->Fail(
                    StartupReplayErrorV1::kCsvMalformed,
                    input_.path,
                    output->line,
                    "quote occurs inside an unquoted field");
            }
            if (character == ',') {
                finish_field();
                continue;
            }
            if (character == '\n') {
                ++physical_line_;
                return finish_record();
            }
            if (character == '\r') {
                if (!ConsumeRecordLf(
                        &record_bytes,
                        output->line,
                        record_start_offset,
                        record_start_line,
                        maximum_scan_bytes)) {
                    return state_->result.error ==
                           StartupReplayErrorV1::kNone;
                }
                ++physical_line_;
                return finish_record();
            }
            field.push_back(character);
            field_state = FieldState::kUnquoted;
        }

        // A vendor process may be appending the last row. Do not emit it until
        // its LF is inside the current available prefix; a later refresh
        // restarts parsing from this record's first byte.
        output->fields.clear();
        MarkPartial(record_start_offset, record_start_line);
        return true;
    }

    bool ExtendToCurrent(bool* extended) {
        if (extended == nullptr) {
            return state_->Fail(
                StartupReplayErrorV1::kUnexpectedFailure,
                input_.path,
                physical_line_,
                "internal null extension result");
        }
        *extended = false;
        if (extension_attempted_) {
            return true;
        }
        extension_attempted_ = true;
        return RefreshAvailableEnd(extended);
    }

    bool RefreshAvailableEnd(bool* extended) {
        if (extended == nullptr) {
            return state_->Fail(
                StartupReplayErrorV1::kUnexpectedFailure,
                input_.path,
                physical_line_,
                "internal null refresh result");
        }
        *extended = false;
        std::uint64_t current_size = 0U;
        std::string detail;
        if (input_.stable == nullptr ||
            !input_.stable->CurrentSize(&current_size, &detail)) {
            return state_->Fail(
                StartupReplayErrorV1::kFileStat,
                input_.path,
                physical_line_,
                detail.empty()
                    ? "cannot re-stat captured CSV descriptor"
                    : std::move(detail));
        }
        if (current_size > state_->config.maximum_file_bytes) {
            return state_->Fail(
                StartupReplayErrorV1::kFileTooLarge,
                input_.path,
                physical_line_,
                "extended CSV byte prefix exceeds maximum_file_bytes");
        }
        if (current_size < prefix_end_) {
            return state_->Fail(
                StartupReplayErrorV1::kIo,
                input_.path,
                physical_line_,
                "file became shorter than its captured byte prefix");
        }
        if (current_size == prefix_end_) {
            return true;
        }
        const std::uint64_t restart_offset =
            partial_suffix_ ? partial_start_offset_ : consumed_offset_;
        const std::uint64_t restart_line =
            partial_suffix_ ? partial_start_line_ : physical_line_;
        prefix_end_ = current_size;
        consumed_offset_ = restart_offset;
        next_read_offset_ = restart_offset;
        remaining_ = current_size - restart_offset;
        physical_line_ = restart_line;
        buffer_index_ = 0U;
        buffer_size_ = 0U;
        partial_suffix_ = false;
        *extended = true;
        return true;
    }

    [[nodiscard]] bool has_incomplete_suffix() const noexcept {
        return partial_suffix_;
    }

    [[nodiscard]] bool extension_attempted() const noexcept {
        return extension_attempted_;
    }

    [[nodiscard]] std::uint64_t incomplete_line() const noexcept {
        return partial_start_line_;
    }

    [[nodiscard]] std::uint64_t incomplete_offset() const noexcept {
        return partial_start_offset_;
    }

    [[nodiscard]] std::uint64_t next_record_offset() const noexcept {
        return partial_suffix_ ? partial_start_offset_ : consumed_offset_;
    }

    [[nodiscard]] bool has_available_bytes() const noexcept {
        return remaining_ != 0U;
    }

    [[nodiscard]] std::uint64_t bytes_read() const noexcept {
        return bytes_read_;
    }

    [[nodiscard]] std::uint64_t complete_records_read() const noexcept {
        return complete_records_read_;
    }

    void BeginScanAccounting() noexcept {
        bytes_read_ = 0U;
        complete_records_read_ = 0U;
        scan_accounting_enabled_ = true;
    }

    void SetCooperativeDeadline(
        std::optional<std::chrono::steady_clock::time_point>
            deadline) noexcept {
        cooperative_deadline_ = deadline;
        checkpoint_deadline_expired_ = false;
    }

    [[nodiscard]] bool checkpoint_deadline_expired() const noexcept {
        return checkpoint_deadline_expired_;
    }

    [[nodiscard]] std::uint64_t available_end() const noexcept {
        return prefix_end_;
    }

private:
    bool ReadByte(
        char* output,
        std::uint64_t checkpoint_line,
        std::size_t maximum_prefetch_bytes) {
        if (output == nullptr || remaining_ == 0U) {
            return false;
        }
        if (buffer_index_ == buffer_size_) {
            // A single logical record can be many MiB and repeatedly grow its
            // field strings without ever reaching the record-count gate.
            // Check before each bounded pread so online recovery gets a
            // cancellation/backpressure point at most one 64 KiB chunk away.
            if (cooperative_deadline_.has_value()) {
                const StartupReplaySinkCallResultV1 checkpoint =
                    state_->CooperativeCheckpointUntil(
                        input_.path,
                        checkpoint_line,
                        *cooperative_deadline_);
                if (checkpoint ==
                    StartupReplaySinkCallResultV1::kDeadline) {
                    checkpoint_deadline_expired_ = true;
                    return false;
                }
                if (checkpoint ==
                    StartupReplaySinkCallResultV1::kRejected) {
                    return false;
                }
            } else if (!state_->CooperativeCheckpoint(
                           input_.path, checkpoint_line)) {
                return false;
            }
            const std::uint64_t requested_u64 = std::min(
                remaining_,
                static_cast<std::uint64_t>(std::min(
                    buffer_.size(), maximum_prefetch_bytes)));
#if defined(__linux__)
            std::size_t received = 0U;
            while (received <
                   static_cast<std::size_t>(requested_u64)) {
                const ssize_t count = ::pread(
                    input_.stable->descriptor(),
                    buffer_.data() + received,
                    static_cast<std::size_t>(requested_u64) - received,
                    static_cast<off_t>(
                        next_read_offset_ + received));
                if (count < 0 && errno == EINTR) {
                    continue;
                }
                if (count <= 0) {
                    return state_->Fail(
                        StartupReplayErrorV1::kIo,
                        input_.path,
                        physical_line_,
                        count == 0
                            ? "file became shorter than its captured byte prefix"
                            : "cannot read captured CSV descriptor: " +
                                  std::string(std::strerror(errno)));
                }
                received += static_cast<std::size_t>(count);
            }
#else
            return state_->Fail(
                StartupReplayErrorV1::kInvalidConfiguration,
                input_.path,
                physical_line_,
                "stable CSV descriptors require Linux in replay v1");
#endif
            next_read_offset_ += requested_u64;
            buffer_index_ = 0U;
            buffer_size_ =
                static_cast<std::size_t>(requested_u64);
        }
        *output = buffer_[buffer_index_++];
        --remaining_;
        ++consumed_offset_;
        return true;
    }

    bool ConsumeRecordLf(
        std::size_t* record_bytes,
        std::uint64_t record_line,
        std::uint64_t record_start_offset,
        std::uint64_t record_start_line,
        std::size_t maximum_scan_bytes) {
        if (remaining_ == 0U) {
            // CR without the LF in the captured prefix is an incomplete
            // suffix, not a malformed committed record.
            MarkPartial(record_start_offset, record_start_line);
            return false;
        }
        const std::size_t effective_maximum_bytes =
            std::min(maximum_record_bytes_, maximum_scan_bytes);
        if (*record_bytes == effective_maximum_bytes) {
            static_cast<void>(state_->Fail(
                maximum_scan_bytes < maximum_record_bytes_
                    ? StartupReplayErrorV1::kResourceExhausted
                    : StartupReplayErrorV1::kLineTooLong,
                input_.path,
                record_line,
                maximum_scan_bytes < maximum_record_bytes_
                    ? "Shenzhen boundary alignment scan byte limit "
                      "was exhausted within a CRLF terminator"
                    : "CSV logical record exceeds "
                      "maximum_record_bytes"));
            return false;
        }
        char next = '\0';
        if (!ReadByte(
                &next,
                record_line,
                effective_maximum_bytes - *record_bytes)) {
            return false;
        }
        ++(*record_bytes);
        if (next != '\n') {
            return state_->Fail(
                StartupReplayErrorV1::kCsvMalformed,
                input_.path,
                record_line,
                "bare CR is not a valid record terminator");
        }
        return true;
    }

    void MarkPartial(
        std::uint64_t offset,
        std::uint64_t line) noexcept {
        partial_suffix_ = true;
        partial_start_offset_ = offset;
        partial_start_line_ = line;
    }

    const InputFile& input_;
    std::size_t maximum_record_bytes_;
    ReplayState* state_;
    std::uint64_t prefix_end_ = 0U;
    std::uint64_t remaining_;
    std::uint64_t consumed_offset_ = 0U;
    std::uint64_t next_read_offset_ = 0U;
    std::uint64_t physical_line_ = 1U;
    bool partial_suffix_ = false;
    bool extension_attempted_ = false;
    std::uint64_t partial_start_offset_ = 0U;
    std::uint64_t partial_start_line_ = 1U;
    std::uint64_t bytes_read_ = 0U;
    std::uint64_t complete_records_read_ = 0U;
    bool scan_accounting_enabled_ = false;
    std::optional<std::chrono::steady_clock::time_point>
        cooperative_deadline_;
    bool checkpoint_deadline_expired_ = false;
    std::array<
        char,
        kStartupReplayCooperativeCheckpointBytesV1>
        buffer_{};
    std::size_t buffer_index_ = 0U;
    std::size_t buffer_size_ = 0U;
};

class CsvTable final {
public:
    CsvTable(
        const InputFile& input,
        std::vector<std::string> required_columns,
        std::vector<std::string> optional_columns,
        ReplayState* state)
        : input_(input),
          reader_(input, state->config.maximum_record_bytes, state),
          required_columns_(std::move(required_columns)),
          optional_columns_(std::move(optional_columns)),
          state_(state) {}

    bool Initialize() {
        if (!reader_.usable()) {
            return false;
        }
        CsvRecord header;
        bool available = false;
        if (!reader_.Next(&header, &available)) {
            return false;
        }
        if (!available && reader_.has_incomplete_suffix()) {
            bool extended = false;
            if (!reader_.ExtendToCurrent(&extended)) {
                return false;
            }
            if (extended && !reader_.Next(&header, &available)) {
                return false;
            }
            if (!available && reader_.has_incomplete_suffix()) {
                return state_->Fail(
                    StartupReplayErrorV1::kIncompleteBoundary,
                    input_.path,
                    reader_.incomplete_line(),
                    "CSV header is still incomplete after the bounded extension");
            }
        }
        if (!available) {
            return state_->Fail(
                StartupReplayErrorV1::kHeaderMissing,
                input_.path,
                1U,
                "CSV has no complete LF-terminated header");
        }
        if (!header.fields.empty() &&
            header.fields.front().size() >= 3U &&
            static_cast<unsigned char>(header.fields.front()[0U]) ==
                0xefU &&
            static_cast<unsigned char>(header.fields.front()[1U]) ==
                0xbbU &&
            static_cast<unsigned char>(header.fields.front()[2U]) ==
                0xbfU) {
            header.fields.front().erase(0U, 3U);
        }
        std::set<std::string> unique;
        for (std::size_t index = 0U;
             index < header.fields.size();
             ++index) {
            const std::string& name = header.fields[index];
            if (name.empty() || !IsAscii(name)) {
                return state_->Fail(
                    StartupReplayErrorV1::kSchemaMismatch,
                    input_.path,
                    header.line,
                    "header names must be non-empty ASCII");
            }
            if (!unique.insert(name).second) {
                return state_->Fail(
                    StartupReplayErrorV1::kDuplicateHeader,
                    input_.path,
                    header.line,
                    "duplicate header column: " + name);
            }
            index_by_name_.emplace(name, index);
        }
        for (const std::string& required : required_columns_) {
            if (index_by_name_.find(required) ==
                index_by_name_.end()) {
                return state_->Fail(
                    StartupReplayErrorV1::kSchemaMismatch,
                    input_.path,
                    header.line,
                    "missing required column: " + required);
            }
        }
        std::set<std::string> permitted(
            required_columns_.begin(), required_columns_.end());
        permitted.insert(
            optional_columns_.begin(), optional_columns_.end());
        for (const std::string& actual : header.fields) {
            if (permitted.find(actual) == permitted.end()) {
                return state_->Fail(
                    StartupReplayErrorV1::kSchemaMismatch,
                    input_.path,
                    header.line,
                    "unexpected column: " + actual);
            }
        }
        if (header.fields.size() < required_columns_.size() ||
            header.fields.size() >
                required_columns_.size() + optional_columns_.size()) {
            return state_->Fail(
                StartupReplayErrorV1::kSchemaMismatch,
                input_.path,
                header.line,
                "header has an invalid number of columns");
        }
        header_end_offset_ = header.end_offset;
        header_ = std::move(header.fields);
        return true;
    }

    bool Next(
        CsvRecord* output,
        bool* available,
        std::size_t maximum_scan_bytes =
            std::numeric_limits<std::size_t>::max()) {
        if (!reader_.Next(
                output, available, maximum_scan_bytes)) {
            return false;
        }
        if (!*available) {
            return true;
        }
        // The production MDL snapshot/queue backup writer terminates data
        // rows with a delimiter even though its header does not declare an
        // additional column.  Treat exactly one trailing empty field as the
        // writer's row terminator.  Any non-empty or additional field remains
        // a hard schema failure below.
        if (output->fields.size() == header_.size() + 1U &&
            output->fields.back().empty()) {
            output->fields.pop_back();
        }
        if (output->fields == header_) {
            return state_->Fail(
                StartupReplayErrorV1::kDuplicateHeader,
                input_.path,
                output->line,
                "header row is repeated in the data");
        }
        if (output->fields.size() != header_.size()) {
            std::ostringstream detail;
            detail << "row has " << output->fields.size()
                   << " columns; header has " << header_.size();
            return state_->Fail(
                StartupReplayErrorV1::kColumnCountMismatch,
                input_.path,
                output->line,
                detail.str());
        }
        return true;
    }

    bool ExtendToCurrent(bool* extended) {
        return reader_.ExtendToCurrent(extended);
    }

    bool RefreshAvailableEnd(bool* extended) {
        return reader_.RefreshAvailableEnd(extended);
    }

    [[nodiscard]] bool has_incomplete_suffix() const noexcept {
        return reader_.has_incomplete_suffix();
    }

    [[nodiscard]] bool extension_attempted() const noexcept {
        return reader_.extension_attempted();
    }

    [[nodiscard]] std::uint64_t incomplete_line() const noexcept {
        return reader_.incomplete_line();
    }

    [[nodiscard]] std::uint64_t incomplete_offset() const noexcept {
        return reader_.incomplete_offset();
    }

    [[nodiscard]] std::uint64_t next_record_offset() const noexcept {
        return reader_.next_record_offset();
    }

    [[nodiscard]] bool has_available_bytes() const noexcept {
        return reader_.has_available_bytes();
    }

    [[nodiscard]] std::uint64_t bytes_read() const noexcept {
        return reader_.bytes_read();
    }

    [[nodiscard]] std::uint64_t complete_records_read() const noexcept {
        return reader_.complete_records_read();
    }

    void BeginScanAccounting() noexcept {
        reader_.BeginScanAccounting();
    }

    void SetCooperativeDeadline(
        std::optional<std::chrono::steady_clock::time_point>
            deadline) noexcept {
        reader_.SetCooperativeDeadline(deadline);
    }

    [[nodiscard]] bool checkpoint_deadline_expired() const noexcept {
        return reader_.checkpoint_deadline_expired();
    }

    [[nodiscard]] std::uint64_t available_end() const noexcept {
        return reader_.available_end();
    }

    [[nodiscard]] std::uint64_t header_end_offset() const noexcept {
        return header_end_offset_;
    }

    [[nodiscard]] std::string_view Get(
        const CsvRecord& record,
        std::string_view column) const {
        const auto found = index_by_name_.find(column);
        if (found == index_by_name_.end()) {
            return {};
        }
        return record.fields[found->second];
    }

    [[nodiscard]] bool HasColumn(std::string_view column) const {
        return index_by_name_.find(column) !=
               index_by_name_.end();
    }

    [[nodiscard]] const InputFile& input() const noexcept {
        return input_;
    }

private:
    const InputFile& input_;
    CsvRecordReader reader_;
    std::vector<std::string> required_columns_;
    std::vector<std::string> optional_columns_;
    ReplayState* state_;
    std::vector<std::string> header_;
    std::uint64_t header_end_offset_ = 0U;
    std::unordered_map<
        std::string,
        std::size_t,
        TransparentStringHash,
        TransparentStringEqual>
        index_by_name_;
};

bool ExtendIncompleteTableOnce(
    CsvTable* table,
    bool* extended,
    ReplayState* state) {
    if (table == nullptr || extended == nullptr) {
        return state->Fail(
            StartupReplayErrorV1::kUnexpectedFailure,
            {},
            0U,
            "internal null incomplete-boundary state");
    }
    *extended = false;
    if (!table->has_incomplete_suffix()) {
        return true;
    }
    if (table->extension_attempted() ||
        !table->ExtendToCurrent(extended)) {
        if (state->result.error != StartupReplayErrorV1::kNone) {
            return false;
        }
        return state->Fail(
            StartupReplayErrorV1::kIncompleteBoundary,
            table->input().path,
            table->incomplete_line(),
            "CSV data record is incomplete after the bounded extension");
    }
    if (!*extended) {
        return state->Fail(
            StartupReplayErrorV1::kIncompleteBoundary,
            table->input().path,
            table->incomplete_line(),
            "CSV data record is incomplete after the bounded extension");
    }
    return true;
}

std::vector<std::string> ShanghaiSnapshotColumns() {
    std::vector<std::string> result{
        "UpdateTime", "SecurityID", "ImageStatus",
        "PreCloPrice", "OpenPrice", "HighPrice", "LowPrice",
        "LastPrice", "ClosePrice", "InstruStatus", "TradNumber",
        "TradVolume", "Turnover", "TotalBidVol", "WAvgBidPri",
        "AltWAvgBidPri", "TotalAskVol", "WAvgAskPri",
        "AltWAvgAskPri", "EtfBuyNumber", "EtfBuyVolume",
        "EtfBuyMoney", "EtfSellNumber", "EtfSellVolume",
        "ETFSellMoney", "YieldToMatu", "TotWarExNum",
        "WarLowerPri", "WarUpperPri", "WiDBuyNum", "WiDBuyVol",
        "WiDBuyMon", "WiDSellNum", "WiDSellVol", "WiDSellMon",
        "TotBidNum", "TotSellNum", "MaxBidDur", "MaxSellDur",
        "BidNum", "SellNum", "IOPV"};
    for (std::size_t index = 1U; index <= kPublishedDepth; ++index) {
        result.push_back("AskPrice" + std::to_string(index));
        result.push_back("AskVolume" + std::to_string(index));
    }
    for (std::size_t index = 1U; index <= kPublishedDepth; ++index) {
        result.push_back("BidPrice" + std::to_string(index));
        result.push_back("BidVolume" + std::to_string(index));
    }
    for (std::size_t index = 1U; index <= kPublishedDepth; ++index) {
        result.push_back("NumOrdersB" + std::to_string(index));
    }
    for (std::size_t index = 1U; index <= kPublishedDepth; ++index) {
        result.push_back("NumOrdersS" + std::to_string(index));
    }
    result.push_back("LocalTime");
    result.push_back("SeqNo");
    return result;
}

std::vector<std::string> QueueColumns(
    std::string first_time_column) {
    std::vector<std::string> result{
        std::move(first_time_column), "SecurityID", "ImageStatus",
        "Side", "NoPriceLevel", "PrcLvlOperator", "Price",
        "Volume", "NumOrders", "NoOrders"};
    for (std::size_t index = 1U;
         index <= kMaximumQueueItems;
         ++index) {
        result.push_back("OrderQty" + std::to_string(index));
    }
    result.push_back("LocalTime");
    result.push_back("SeqNo");
    return result;
}

std::vector<std::string> ShanghaiTickColumns() {
    return {
        "BizIndex", "Channel", "SecurityID", "TickTime", "Type",
        "BuyOrderNO", "SellOrderNO", "Price", "Qty", "TradeMoney",
        "TickBSFlag", "LocalTime", "SeqNo"};
}

std::vector<std::string> ShenzhenSnapshotColumns() {
    std::vector<std::string> result{
        "UpdateTime", "MDStreamID", "SecurityID",
        "SecurityIDSource", "TradingPhaseCode", "PreCloPrice",
        "TurnNum", "Volume", "Turnover", "LastPrice", "OpenPrice",
        "HighPrice", "LowPrice", "DifPrice1", "DifPrice2", "PE1",
        "PE2", "PreCloseIOPV", "IOPV", "TotalBidQty",
        "WeightedAvgBidPx", "TotalOfferQty", "WeightedAvgOfferPx",
        "HighLimitPrice", "LowLimitPrice", "OpenInt",
        "OptPremiumRatio"};
    for (std::size_t index = 1U; index <= kPublishedDepth; ++index) {
        result.push_back("AskPrice" + std::to_string(index));
        result.push_back("AskVolume" + std::to_string(index));
    }
    for (std::size_t index = 1U; index <= kPublishedDepth; ++index) {
        result.push_back("BidPrice" + std::to_string(index));
        result.push_back("BidVolume" + std::to_string(index));
    }
    for (std::size_t index = 1U; index <= kPublishedDepth; ++index) {
        result.push_back("NumOrdersB" + std::to_string(index));
    }
    for (std::size_t index = 1U; index <= kPublishedDepth; ++index) {
        result.push_back("NumOrdersS" + std::to_string(index));
    }
    result.push_back("LocalTime");
    result.push_back("SeqNo");
    return result;
}

std::vector<std::string> ShenzhenOrderColumns() {
    return {
        "ChannelNo", "ApplSeqNum", "MDStreamID", "SecurityID",
        "SecurityIDSource", "Price", "OrderQty", "Side",
        "TransactTime", "OrdType", "LocalTime", "SeqNo"};
}

std::vector<std::string> ShenzhenTransactionColumns() {
    return {
        "ChannelNo", "ApplSeqNum", "MDStreamID",
        "BidApplSeqNum", "OfferApplSeqNum", "SecurityID",
        "SecurityIDSource", "LastPx", "LastQty", "ExecType",
        "TransactTime", "LocalTime", "SeqNo"};
}

bool ParseUnsigned(
    std::string_view text,
    std::uint64_t maximum,
    std::uint64_t* output,
    bool* overflow = nullptr) noexcept {
    if (overflow != nullptr) {
        *overflow = false;
    }
    if (output == nullptr || text.empty()) {
        return false;
    }
    std::uint64_t value = 0U;
    for (char character : text) {
        if (character < '0' || character > '9') {
            return false;
        }
        const std::uint64_t digit =
            static_cast<std::uint64_t>(character - '0');
        if (value > (maximum - digit) / 10U) {
            if (overflow != nullptr) {
                *overflow = true;
            }
            return false;
        }
        value = value * 10U + digit;
    }
    *output = value;
    return true;
}

bool ParseFixed(
    std::string_view text,
    std::uint8_t scale,
    std::int64_t maximum_magnitude,
    std::int64_t null_value,
    std::int64_t* output,
    bool* overflow) noexcept {
    if (output == nullptr || overflow == nullptr) {
        return false;
    }
    *overflow = false;
    if (text.empty()) {
        *output = null_value;
        return true;
    }
    bool negative = false;
    std::size_t index = 0U;
    if (text[index] == '-') {
        negative = true;
        ++index;
        if (index == text.size()) {
            return false;
        }
    }
    std::uint64_t whole = 0U;
    std::size_t whole_digits = 0U;
    while (index < text.size() && text[index] != '.') {
        const char character = text[index];
        if (character < '0' || character > '9') {
            return false;
        }
        const std::uint64_t digit =
            static_cast<std::uint64_t>(character - '0');
        if (whole >
            (static_cast<std::uint64_t>(maximum_magnitude) - digit) /
                10U) {
            *overflow = true;
            return false;
        }
        whole = whole * 10U + digit;
        ++whole_digits;
        ++index;
    }
    if (whole_digits == 0U) {
        return false;
    }
    std::uint64_t fraction = 0U;
    std::size_t fraction_digits = 0U;
    if (index < text.size()) {
        ++index;
        if (index == text.size()) {
            return false;
        }
        while (index < text.size()) {
            const char character = text[index++];
            if (character < '0' || character > '9') {
                return false;
            }
            if (fraction_digits >= scale) {
                return false;
            }
            fraction = fraction * 10U +
                       static_cast<std::uint64_t>(character - '0');
            ++fraction_digits;
        }
    }
    std::uint64_t multiplier = 1U;
    for (std::size_t count = 0U; count < scale; ++count) {
        multiplier *= 10U;
    }
    while (fraction_digits < scale) {
        fraction *= 10U;
        ++fraction_digits;
    }
    const std::uint64_t maximum =
        static_cast<std::uint64_t>(maximum_magnitude);
    if (whole > (maximum - fraction) / multiplier) {
        *overflow = true;
        return false;
    }
    const std::uint64_t magnitude = whole * multiplier + fraction;
    const std::int64_t signed_magnitude =
        static_cast<std::int64_t>(magnitude);
    *output = negative ? -signed_magnitude : signed_magnitude;
    return true;
}

bool ParseTime(std::string_view text, std::uint32_t* output) noexcept {
    if (output == nullptr || text.size() != 12U ||
        text[2U] != ':' || text[5U] != ':' || text[8U] != '.') {
        return false;
    }
    const std::array<std::size_t, 9U> digit_indexes{
        0U, 1U, 3U, 4U, 6U, 7U, 9U, 10U, 11U};
    for (std::size_t index : digit_indexes) {
        if (text[index] < '0' || text[index] > '9') {
            return false;
        }
    }
    const std::uint32_t hour =
        static_cast<std::uint32_t>(
            (text[0U] - '0') * 10 + (text[1U] - '0'));
    const std::uint32_t minute =
        static_cast<std::uint32_t>(
            (text[3U] - '0') * 10 + (text[4U] - '0'));
    const std::uint32_t second =
        static_cast<std::uint32_t>(
            (text[6U] - '0') * 10 + (text[7U] - '0'));
    const std::uint32_t millisecond =
        static_cast<std::uint32_t>(
            (text[9U] - '0') * 100 +
            (text[10U] - '0') * 10 +
            (text[11U] - '0'));
    if (hour >= 24U || minute >= 60U || second >= 60U) {
        return false;
    }
    *output =
        hour * 10'000'000U + minute * 100'000U +
        second * 1'000U + millisecond;
    return true;
}

class RowParser final {
public:
    RowParser(
        const CsvTable& table,
        const CsvRecord& record,
        ReplayState* state) noexcept
        : table_(table), record_(record), state_(state) {}

    bool Ascii(
        std::string_view column,
        std::string* output,
        bool allow_empty = false) {
        const std::string_view value = table_.Get(record_, column);
        if ((!allow_empty && value.empty()) || !IsAscii(value)) {
            return Invalid(
                StartupReplayErrorV1::kValueInvalid,
                column,
                "must be an ASCII string");
        }
        output->assign(value);
        return true;
    }

    bool RightSpacePaddedAscii(
        std::string_view column,
        std::string* output) {
        std::string_view value = table_.Get(record_, column);
        if (value.empty() || !IsAscii(value)) {
            return Invalid(
                StartupReplayErrorV1::kValueInvalid,
                column,
                "must be a non-empty ASCII string");
        }
        while (!value.empty() && value.back() == ' ') {
            value.remove_suffix(1U);
        }
        if (value.empty()) {
            return Invalid(
                StartupReplayErrorV1::kValueInvalid,
                column,
                "must contain a non-space ASCII value");
        }
        output->assign(value);
        return true;
    }

    bool U32(std::string_view column, std::uint32_t* output) {
        std::uint64_t value = 0U;
        bool overflow = false;
        if (!ParseUnsigned(
                table_.Get(record_, column),
                std::numeric_limits<std::uint32_t>::max(),
                &value,
                &overflow)) {
            return Invalid(
                overflow ? StartupReplayErrorV1::kNumericOverflow
                         : StartupReplayErrorV1::kValueInvalid,
                column,
                "must be an unsigned 32-bit integer");
        }
        *output = static_cast<std::uint32_t>(value);
        return true;
    }

    bool PositiveU32(
        std::string_view column,
        std::uint32_t* output) {
        return U32(column, output) &&
               (*output != 0U ||
                Invalid(
                    StartupReplayErrorV1::kValueInvalid,
                    column,
                    "must be greater than zero"));
    }

    bool OptionalU32(
        std::string_view column,
        std::uint32_t* output) {
        if (table_.Get(record_, column).empty()) {
            *output = 0U;
            return true;
        }
        return U32(column, output);
    }

    bool NonnegativeI64(
        std::string_view column,
        std::int64_t* output) {
        std::uint64_t value = 0U;
        bool overflow = false;
        if (!ParseUnsigned(
                table_.Get(record_, column),
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::int64_t>::max()),
                &value,
                &overflow)) {
            return Invalid(
                overflow ? StartupReplayErrorV1::kNumericOverflow
                         : StartupReplayErrorV1::kValueInvalid,
                column,
                "must be a non-negative signed 64-bit integer");
        }
        *output = static_cast<std::int64_t>(value);
        return true;
    }

    bool PositiveI64(
        std::string_view column,
        std::int64_t* output) {
        return NonnegativeI64(column, output) &&
               (*output != 0 ||
                Invalid(
                    StartupReplayErrorV1::kValueInvalid,
                    column,
                    "must be greater than zero"));
    }

    bool OptionalNonnegativeI64(
        std::string_view column,
        std::int64_t* output) {
        if (table_.Get(record_, column).empty()) {
            *output = 0;
            return true;
        }
        return NonnegativeI64(column, output);
    }

    bool PositiveU64(
        std::string_view column,
        std::uint64_t* output) {
        bool overflow = false;
        if (!ParseUnsigned(
                table_.Get(record_, column),
                std::numeric_limits<std::uint64_t>::max(),
                output,
                &overflow)) {
            return Invalid(
                overflow ? StartupReplayErrorV1::kNumericOverflow
                         : StartupReplayErrorV1::kValueInvalid,
                column,
                "must be an unsigned 64-bit integer");
        }
        if (*output == 0U) {
            return Invalid(
                StartupReplayErrorV1::kValueInvalid,
                column,
                "must be greater than zero");
        }
        return true;
    }

    bool Time(std::string_view column, std::uint32_t* output) {
        if (!ParseTime(table_.Get(record_, column), output)) {
            return Invalid(
                StartupReplayErrorV1::kTimeInvalid,
                column,
                "must be exactly HH:MM:SS.mmm with a valid clock time");
        }
        return true;
    }

    bool Fixed32(
        std::string_view column,
        std::uint8_t scale,
        std::int32_t* output) {
        std::int64_t value = 0;
        bool overflow = false;
        if (!ParseFixed(
                table_.Get(record_, column),
                scale,
                std::numeric_limits<std::int32_t>::max(),
                std::numeric_limits<std::int32_t>::min(),
                &value,
                &overflow)) {
            return Invalid(
                overflow ? StartupReplayErrorV1::kNumericOverflow
                         : StartupReplayErrorV1::kValueInvalid,
                column,
                "is not an exactly representable fixed-point int32");
        }
        *output = static_cast<std::int32_t>(value);
        return true;
    }

    bool Fixed64(
        std::string_view column,
        std::uint8_t scale,
        std::int64_t* output) {
        bool overflow = false;
        if (!ParseFixed(
                table_.Get(record_, column),
                scale,
                std::numeric_limits<std::int64_t>::max(),
                std::numeric_limits<std::int64_t>::min(),
                output,
                &overflow)) {
            return Invalid(
                overflow ? StartupReplayErrorV1::kNumericOverflow
                         : StartupReplayErrorV1::kValueInvalid,
                column,
                "is not an exactly representable fixed-point int64");
        }
        return true;
    }

    bool OptionalFixed32(
        std::string_view column,
        std::uint8_t scale,
        std::int32_t* output) {
        if (table_.Get(record_, column).empty()) {
            *output = 0;
            return true;
        }
        return Fixed32(column, scale, output);
    }

    bool OptionalFixed64(
        std::string_view column,
        std::uint8_t scale,
        std::int64_t* output) {
        if (table_.Get(record_, column).empty()) {
            *output = 0;
            return true;
        }
        return Fixed64(column, scale, output);
    }

    bool OptionalTailFixed64(
        std::string_view column,
        std::uint8_t scale,
        std::int64_t* output) {
        return OptionalFixed64(column, scale, output);
    }

    bool EncodedAsciiI32(
        std::string_view column,
        std::string_view permitted,
        std::int32_t* output) {
        const std::string_view text = table_.Get(record_, column);
        std::int32_t value = 0;
        if (text.size() == 1U &&
            permitted.find(text.front()) != std::string_view::npos) {
            value = static_cast<std::int32_t>(
                static_cast<unsigned char>(text.front()));
        } else {
            std::uint64_t parsed = 0U;
            bool overflow = false;
            if (!ParseUnsigned(
                    text,
                    static_cast<std::uint64_t>(
                        std::numeric_limits<std::int32_t>::max()),
                    &parsed,
                    &overflow)) {
                return Invalid(
                    overflow
                        ? StartupReplayErrorV1::kNumericOverflow
                        : StartupReplayErrorV1::kValueInvalid,
                    column,
                    "is neither an allowed character nor its ASCII code");
            }
            value = static_cast<std::int32_t>(parsed);
            if (value > 127 ||
                permitted.find(static_cast<char>(value)) ==
                    std::string_view::npos) {
                return Invalid(
                    StartupReplayErrorV1::kValueInvalid,
                    column,
                    "contains an unsupported ASCII code");
            }
        }
        *output = value;
        return true;
    }

private:
    bool Invalid(
        StartupReplayErrorV1 error,
        std::string_view column,
        std::string_view reason) {
        return state_->Fail(
            error,
            table_.input().path,
            record_.line,
            std::string(column) + " " + std::string(reason));
    }

    const CsvTable& table_;
    const CsvRecord& record_;
    ReplayState* state_;
};

class BodyWriter final {
public:
    explicit BodyWriter(std::size_t fixed_bytes)
        : bytes_(fixed_bytes, std::byte{0U}) {}

    bool StoreU16(std::size_t offset, std::uint16_t value) {
        return StoreUnsigned(offset, value, 2U);
    }
    bool StoreU32(std::size_t offset, std::uint32_t value) {
        return StoreUnsigned(offset, value, 4U);
    }
    bool StoreI32(std::size_t offset, std::int32_t value) {
        return StoreU32(offset, static_cast<std::uint32_t>(value));
    }
    bool StoreU64(std::size_t offset, std::uint64_t value) {
        return StoreUnsigned(offset, value, 8U);
    }
    bool StoreI64(std::size_t offset, std::int64_t value) {
        return StoreU64(offset, static_cast<std::uint64_t>(value));
    }

    bool AddString(std::size_t descriptor, std::string_view value) {
        if (value.size() >
                std::numeric_limits<std::uint16_t>::max() ||
            descriptor > bytes_.size() ||
            6U > bytes_.size() - descriptor) {
            return false;
        }
        if (value.empty()) {
            return StoreU16(descriptor, 0U) &&
                   StoreU32(descriptor + 2U, 0U);
        }
        const std::size_t start = bytes_.size();
        if (start < descriptor ||
            start - descriptor >
                std::numeric_limits<std::uint32_t>::max()) {
            return false;
        }
        bytes_.reserve(bytes_.size() + value.size() + 1U);
        for (char character : value) {
            bytes_.push_back(static_cast<std::byte>(
                static_cast<unsigned char>(character)));
        }
        bytes_.push_back(std::byte{0U});
        return StoreU16(
                   descriptor,
                   static_cast<std::uint16_t>(value.size())) &&
               StoreU32(
                   descriptor + 2U,
                   static_cast<std::uint32_t>(start - descriptor));
    }

    bool AddList(
        std::size_t descriptor,
        std::uint32_t count,
        std::size_t item_bytes,
        std::size_t* start) {
        if (start == nullptr || descriptor > bytes_.size() ||
            8U > bytes_.size() - descriptor) {
            return false;
        }
        if (count == 0U) {
            *start = descriptor;
            return StoreU32(descriptor, 0U) &&
                   StoreU32(descriptor + 4U, 0U);
        }
        if (item_bytes != 0U &&
            static_cast<std::size_t>(count) >
                std::numeric_limits<std::size_t>::max() /
                    item_bytes) {
            return false;
        }
        const std::size_t list_start = bytes_.size();
        if (list_start < descriptor ||
            list_start - descriptor >
                std::numeric_limits<std::uint32_t>::max()) {
            return false;
        }
        const std::size_t append =
            static_cast<std::size_t>(count) * item_bytes;
        bytes_.resize(bytes_.size() + append, std::byte{0U});
        *start = list_start;
        return StoreU32(descriptor, count) &&
               StoreU32(
                   descriptor + 4U,
                   static_cast<std::uint32_t>(
                       list_start - descriptor));
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return bytes_.size();
    }
    [[nodiscard]] std::vector<std::byte> Take() && {
        return std::move(bytes_);
    }

private:
    bool StoreUnsigned(
        std::size_t offset,
        std::uint64_t value,
        std::size_t width) {
        if (offset > bytes_.size() ||
            width > bytes_.size() - offset) {
            return false;
        }
        for (std::size_t index = 0U; index < width; ++index) {
            bytes_[offset + index] = static_cast<std::byte>(
                (value >> (index * 8U)) & 0xffU);
        }
        return true;
    }

    std::vector<std::byte> bytes_;
};

class CallbackLifetimeMessage final : public mdl::MDLMessage {
public:
    CallbackLifetimeMessage(
        l2flow::sdk::MessageKey key,
        std::uint32_t local_time,
        std::uint64_t sequence,
        std::vector<std::byte>* body) noexcept
        : body_(body) {
        head_.HeadSize = static_cast<std::uint8_t>(
            sizeof(mdl::MDLMessageHead));
        head_.MessageSize = static_cast<std::uint32_t>(
            sizeof(mdl::MDLMessageHead) + body_->size());
        head_.MessageEncoding = mdl::MDLEID_BINARY;
        head_.ServiceID = key.service_id;
        head_.ServiceVersion = key.service_version;
        head_.MessageID = key.message_id;
        head_.LocalTime.m_Value = local_time;
        head_.SequenceID = sequence;
    }

    mdl::MDLMessageHead* GetHead() const override {
        return const_cast<mdl::MDLMessageHead*>(&head_);
    }
    char* GetBody() const override {
        return reinterpret_cast<char*>(body_->data());
    }
    void AddRef() override {}
    int ReleaseRef() override { return 1; }

protected:
    mdl::MDLMessage* _Copy() const override { return nullptr; }

private:
    mdl::MDLMessageHead head_{};
    std::vector<std::byte>* body_;
};

struct PendingMessage final {
    l2flow::sdk::MessageKey key{};
    const std::filesystem::path* file = nullptr;
    std::uint64_t line = 0U;
    std::uint64_t csv_sequence = 0U;
    std::uint32_t local_time = 0U;
    std::uint64_t notice_flags = 0U;
    bool publish = true;
    std::vector<std::byte> body;
};

enum class PendingPublishResult : std::uint8_t {
    kPublished = 0U,
    kFailed,
    kDeadline,
};

PendingPublishResult PublishMessageUntil(
    PendingMessage* pending,
    ReplayState* state,
    const std::chrono::steady_clock::time_point* deadline) {
    if (pending == nullptr) {
        static_cast<void>(state->Fail(
            StartupReplayErrorV1::kUnexpectedFailure,
            {},
            0U,
            "internal null pending message"));
        return PendingPublishResult::kFailed;
    }
    if (pending->body.size() >
            std::numeric_limits<std::uint32_t>::max() -
                sizeof(mdl::MDLMessageHead)) {
        static_cast<void>(state->Fail(
            StartupReplayErrorV1::kMessageTooLarge,
            *pending->file,
            pending->line,
            "reconstructed MDL message exceeds configured size"));
        return PendingPublishResult::kFailed;
    }
    const std::size_t wire_size =
        sizeof(mdl::MDLMessageHead) + pending->body.size();
    if (wire_size > state->config.maximum_message_bytes) {
        static_cast<void>(state->Fail(
            StartupReplayErrorV1::kMessageTooLarge,
            *pending->file,
            pending->line,
            "reconstructed MDL total wire size exceeds "
            "maximum_message_bytes"));
        return PendingPublishResult::kFailed;
    }
    if (!pending->publish) {
        return PendingPublishResult::kPublished;
    }
    CallbackLifetimeMessage message(
        pending->key,
        pending->local_time,
        pending->csv_sequence,
        &pending->body);
    const StartupReplayPublicationV1 publication{
        &message,
        pending->key,
        pending->file,
        pending->line,
        pending->csv_sequence,
        kStartupReplayProvenanceCsvV1,
        pending->notice_flags};
    std::string detail;
    const StartupReplaySinkCallResultV1 sink_result =
        deadline != nullptr
            ? state->sink.PublishUntil(
                  publication, *deadline, &detail)
            : state->sink.Publish(publication, &detail)
                  ? StartupReplaySinkCallResultV1::kAccepted
                  : StartupReplaySinkCallResultV1::kRejected;
    if (sink_result == StartupReplaySinkCallResultV1::kDeadline) {
        return PendingPublishResult::kDeadline;
    }
    if (sink_result == StartupReplaySinkCallResultV1::kRejected) {
        if (detail.empty()) {
            detail = "startup replay sink rejected publication";
        }
        static_cast<void>(state->Fail(
            StartupReplayErrorV1::kSinkRejected,
            *pending->file,
            pending->line,
            std::move(detail)));
        return PendingPublishResult::kFailed;
    }
    if (pending->key.service_id == 4U &&
        pending->key.message_id == 4U) {
        ++state->result.counts.shanghai_snapshots;
    } else if (pending->key.service_id == 4U &&
               pending->key.message_id == 24U) {
        ++state->result.counts.shanghai_ticks;
    } else if (pending->key.service_id == 6U &&
               pending->key.message_id == 28U) {
        ++state->result.counts.shenzhen_snapshots;
    } else if (pending->key.service_id == 6U &&
               pending->key.message_id == 33U) {
        ++state->result.counts.shenzhen_orders;
    } else if (pending->key.service_id == 6U &&
               pending->key.message_id == 36U) {
        ++state->result.counts.shenzhen_transactions;
    }
    return PendingPublishResult::kPublished;
}

bool PublishMessage(
    PendingMessage* pending,
    ReplayState* state) {
    return PublishMessageUntil(pending, state, nullptr) ==
           PendingPublishResult::kPublished;
}

bool EnsureWriter(
    bool success,
    const CsvTable& table,
    const CsvRecord& record,
    ReplayState* state) {
    if (success) {
        return true;
    }
    return state->Fail(
        StartupReplayErrorV1::kMessageTooLarge,
        table.input().path,
        record.line,
        "reconstructed MDL relative body layout overflowed");
}

struct QueueRow final {
    const std::filesystem::path* file = nullptr;
    std::uint64_t line = 0U;
    std::uint64_t sequence = 0U;
    std::uint32_t exchange_time = 0U;
    std::uint32_t local_time = 0U;
    std::string security_id;
    std::uint32_t image_status = 0U;
    char side = '\0';
    std::uint32_t price_level = 0U;
    std::uint32_t level_operator = 0U;
    std::int64_t price = 0;
    std::int64_t volume = 0;
    std::uint32_t total_order_count = 0U;
    std::uint32_t revealed_count = 0U;
    std::vector<std::int64_t> quantities;
};

bool ParseQueueRow(
    const CsvTable& table,
    const CsvRecord& record,
    std::string_view time_column,
    std::uint8_t price_scale,
    std::uint8_t quantity_scale,
    std::optional<char> required_side,
    QueueRow* output,
    ReplayState* state) {
    if (output == nullptr) {
        return state->Fail(
            StartupReplayErrorV1::kUnexpectedFailure,
            table.input().path,
            record.line,
            "internal null queue row");
    }
    RowParser parse(table, record, state);
    QueueRow decoded;
    decoded.file = &table.input().path;
    decoded.line = record.line;
    std::string side;
    if (!parse.Time(time_column, &decoded.exchange_time) ||
        !parse.Ascii("SecurityID", &decoded.security_id) ||
        !parse.U32("ImageStatus", &decoded.image_status) ||
        !parse.Ascii("Side", &side) ||
        !parse.U32("NoPriceLevel", &decoded.price_level) ||
        !parse.OptionalU32(
            "PrcLvlOperator", &decoded.level_operator) ||
        !parse.OptionalU32(
            "NumOrders", &decoded.total_order_count) ||
        !parse.OptionalU32("NoOrders", &decoded.revealed_count) ||
        !parse.Time("LocalTime", &decoded.local_time) ||
        !parse.PositiveU64("SeqNo", &decoded.sequence)) {
        return false;
    }
    if (price_scale == 3U) {
        std::int32_t price = 0;
        if (!parse.OptionalFixed32(
                "Price", price_scale, &price)) {
            return false;
        }
        decoded.price = price;
    } else if (!parse.OptionalFixed64(
                   "Price", price_scale, &decoded.price)) {
        return false;
    }
    if (quantity_scale == 0U) {
        if (!parse.OptionalNonnegativeI64(
                "Volume", &decoded.volume)) {
            return false;
        }
    } else if (!parse.OptionalFixed64(
                   "Volume", quantity_scale, &decoded.volume)) {
        return false;
    }
    if (decoded.volume < 0) {
        return state->Fail(
            StartupReplayErrorV1::kValueInvalid,
            table.input().path,
            record.line,
            "Volume must not be negative or null");
    }
    if (side.size() != 1U ||
        (side.front() != 'B' && side.front() != 'S')) {
        return state->Fail(
            StartupReplayErrorV1::kValueInvalid,
            table.input().path,
            record.line,
            "Side must be exactly B or S");
    }
    decoded.side = side.front();
    if (required_side.has_value() &&
        decoded.side != *required_side) {
        return state->Fail(
            StartupReplayErrorV1::kJoinMismatch,
            table.input().path,
            record.line,
            std::string("queue file requires Side=") +
                *required_side);
    }
    if (required_side.has_value() &&
        decoded.level_operator != 0U) {
        return state->Fail(
            StartupReplayErrorV1::kValueInvalid,
            table.input().path,
            record.line,
            "Shenzhen snapshot PrcLvlOperator is reserved and must be zero");
    }
    if (required_side.has_value() &&
        decoded.image_status != 1U) {
        return state->Fail(
            StartupReplayErrorV1::kValueInvalid,
            table.input().path,
            record.line,
            "Shenzhen snapshot queue ImageStatus must be 1");
    }
    if (!required_side.has_value() &&
        decoded.image_status != 1U &&
        decoded.image_status != 3U) {
        return state->Fail(
            StartupReplayErrorV1::kValueInvalid,
            table.input().path,
            record.line,
            "Shanghai queue ImageStatus must be 1 or 3");
    }
    if (!required_side.has_value() &&
        decoded.level_operator != 0U) {
        return state->Fail(
            StartupReplayErrorV1::kValueInvalid,
            table.input().path,
            record.line,
            "Shanghai client queue PrcLvlOperator must be zero");
    }
    if (decoded.price_level != 1U) {
        return state->Fail(
            StartupReplayErrorV1::kJoinMismatch,
            table.input().path,
            record.line,
            "NoPriceLevel must be exactly 1");
    }
    const std::uint32_t expected_revealed = std::min(
        decoded.total_order_count,
        static_cast<std::uint32_t>(kMaximumQueueItems));
    if (decoded.revealed_count != expected_revealed) {
        return state->Fail(
            StartupReplayErrorV1::kJoinMismatch,
            table.input().path,
            record.line,
            "NoOrders must equal min(NumOrders, 50)");
    }
    decoded.quantities.reserve(decoded.revealed_count);
    for (std::size_t index = 1U;
         index <= kMaximumQueueItems;
         ++index) {
        const std::string column =
            "OrderQty" + std::to_string(index);
        std::int64_t quantity = 0;
        if (index <= decoded.revealed_count) {
            if (quantity_scale == 0U) {
                if (!parse.NonnegativeI64(column, &quantity)) {
                    return false;
                }
            } else if (!parse.Fixed64(
                           column, quantity_scale, &quantity)) {
                return false;
            }
            if (quantity < 0) {
                return state->Fail(
                    StartupReplayErrorV1::kValueInvalid,
                    table.input().path,
                    record.line,
                    column + " must not be negative");
            }
            decoded.quantities.push_back(quantity);
        } else {
            // V4 fixes 50 physical columns but does not specify whether
            // inactive cells are blank or zero.  Validate any supplied text
            // without assigning it observed semantics.
            if (quantity_scale == 0U) {
                const std::string_view text = table.Get(record, column);
                if (!text.empty() &&
                    !parse.NonnegativeI64(column, &quantity)) {
                    return false;
                }
            } else if (!parse.OptionalTailFixed64(
                           column, quantity_scale, &quantity)) {
                return false;
            }
            if (quantity < 0) {
                return state->Fail(
                    StartupReplayErrorV1::kValueInvalid,
                    table.input().path,
                    record.line,
                    column + " must not be negative");
            }
        }
    }
    *output = std::move(decoded);
    return true;
}

class QueueStream final {
public:
    QueueStream(
        CsvTable* table,
        std::string time_column,
        std::uint8_t price_scale,
        std::uint8_t quantity_scale,
        std::optional<char> required_side,
        ReplayState* state)
        : table_(table),
          time_column_(std::move(time_column)),
          price_scale_(price_scale),
          quantity_scale_(quantity_scale),
          required_side_(required_side),
          state_(state) {}

    bool Peek(const QueueRow** output) {
        if (!lookahead_.has_value() && !at_end_) {
            bool available = false;
            if (!table_->Next(&record_, &available)) {
                return false;
            }
            if (!available) {
                at_end_ = true;
            } else {
                QueueRow row;
                if (!ParseQueueRow(
                        *table_,
                        record_,
                        time_column_,
                        price_scale_,
                        quantity_scale_,
                        required_side_,
                        &row,
                        state_)) {
                    return false;
                }
                if (have_previous_ &&
                    row.sequence < previous_sequence_) {
                    return state_->Fail(
                        StartupReplayErrorV1::kJoinMismatch,
                        *row.file,
                        row.line,
                        "queue SeqNo decreases in file order");
                }
                previous_sequence_ = row.sequence;
                have_previous_ = true;
                lookahead_ = std::move(row);
            }
        }
        *output = lookahead_.has_value() ? &*lookahead_ : nullptr;
        return true;
    }

    [[nodiscard]] QueueRow Take() {
        QueueRow result = std::move(*lookahead_);
        lookahead_.reset();
        return result;
    }

    bool ExtendToCurrent(bool* extended) {
        if (lookahead_.has_value() || !at_end_) {
            return state_->Fail(
                StartupReplayErrorV1::kUnexpectedFailure,
                table_->input().path,
                record_.line,
                "queue extension requested before captured-prefix EOF");
        }
        if (!table_->ExtendToCurrent(extended)) {
            return false;
        }
        if (*extended) {
            at_end_ = false;
        }
        return true;
    }

    [[nodiscard]] bool has_incomplete_suffix() const noexcept {
        return table_->has_incomplete_suffix();
    }

    [[nodiscard]] bool extension_attempted() const noexcept {
        return table_->extension_attempted();
    }

    [[nodiscard]] std::uint64_t incomplete_line() const noexcept {
        return table_->incomplete_line();
    }

    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return table_->input().path;
    }

private:
    CsvTable* table_;
    std::string time_column_;
    std::uint8_t price_scale_;
    std::uint8_t quantity_scale_;
    std::optional<char> required_side_;
    ReplayState* state_;
    std::optional<QueueRow> lookahead_;
    CsvRecord record_;
    bool at_end_ = false;
    bool have_previous_ = false;
    std::uint64_t previous_sequence_ = 0U;
};

struct QueuePair final {
    std::optional<QueueRow> bid;
    std::optional<QueueRow> ask;
};

bool AddQueueRow(
    QueueRow row,
    QueuePair* pair,
    ReplayState* state) {
    std::optional<QueueRow>* destination =
        row.side == 'B' ? &pair->bid : &pair->ask;
    if (destination->has_value()) {
        return state->Fail(
            StartupReplayErrorV1::kJoinDuplicate,
            *row.file,
            row.line,
            std::string("duplicate queue Side=") + row.side +
                " for SeqNo");
    }
    *destination = std::move(row);
    return true;
}

bool CollectJoinedQueues(
    std::uint64_t root_sequence,
    QueueStream* stream,
    QueuePair* pair,
    ReplayState* state) {
    for (;;) {
        const QueueRow* next = nullptr;
        if (!stream->Peek(&next)) {
            return false;
        }
        if (next == nullptr || next->sequence > root_sequence) {
            return true;
        }
        if (next->sequence < root_sequence) {
            return state->Fail(
                StartupReplayErrorV1::kJoinOrphan,
                *next->file,
                next->line,
                "queue SeqNo has no matching earlier snapshot row");
        }
        QueueRow joined = stream->Take();
        if (!AddQueueRow(std::move(joined), pair, state)) {
            return false;
        }
    }
}

bool CollectJoinedQueueSide(
    std::uint64_t root_sequence,
    QueueStream* stream,
    std::optional<QueueRow>* output,
    ReplayState* state) {
    const QueueRow* next = nullptr;
    if (!stream->Peek(&next)) {
        return false;
    }
    if (next == nullptr || next->sequence > root_sequence) {
        return true;
    }
    if (next->sequence < root_sequence) {
        return state->Fail(
            StartupReplayErrorV1::kJoinOrphan,
            *next->file,
            next->line,
            "queue SeqNo has no matching earlier snapshot row");
    }
    *output = stream->Take();
    return true;
}

bool CollectAppendedQueuesForRoot(
    std::uint64_t root_sequence,
    QueueStream* stream,
    QueuePair* pair,
    ReplayState* state) {
    bool extended = false;
    if (!stream->ExtendToCurrent(&extended)) {
        return false;
    }
    if (!extended) {
        if (stream->has_incomplete_suffix()) {
            return state->Fail(
                StartupReplayErrorV1::kIncompleteBoundary,
                stream->path(),
                stream->incomplete_line(),
                "snapshot child record is incomplete after the bounded extension");
        }
        return true;
    }
    for (;;) {
        const QueueRow* next = nullptr;
        if (!stream->Peek(&next)) {
            return false;
        }
        if (next == nullptr) {
            return true;
        }
        if (next->sequence > root_sequence) {
            return true;
        }
        if (next->sequence < root_sequence) {
            return state->Fail(
                StartupReplayErrorV1::kJoinOrphan,
                *next->file,
                next->line,
                "appended queue row precedes the snapshot cutoff");
        }
        QueueRow joined = stream->Take();
        if (!AddQueueRow(std::move(joined), pair, state)) {
            return false;
        }
    }
}

bool FinishQueueTail(
    QueueStream* stream,
    bool have_root,
    std::uint64_t last_root_sequence,
    bool allow_opposite_sides_at_same_sequence,
    ReplayState* state) {
    std::optional<std::uint64_t> tail_sequence;
    bool have_bid = false;
    bool have_ask = false;
    for (;;) {
        const QueueRow* next = nullptr;
        if (!stream->Peek(&next)) {
            return false;
        }
        if (next == nullptr) {
            if (!stream->has_incomplete_suffix()) {
                return true;
            }
            if (stream->extension_attempted()) {
                return state->Fail(
                    StartupReplayErrorV1::kIncompleteBoundary,
                    stream->path(),
                    stream->incomplete_line(),
                    "snapshot child record is incomplete after the bounded extension");
            }
            bool extended = false;
            if (!stream->ExtendToCurrent(&extended)) {
                return false;
            }
            if (!extended) {
                return state->Fail(
                    StartupReplayErrorV1::kIncompleteBoundary,
                    stream->path(),
                    stream->incomplete_line(),
                    "snapshot child record is incomplete after the bounded extension");
            }
            continue;
        }

        QueueRow row = stream->Take();
        if (!have_root || row.sequence <= last_root_sequence) {
            return state->Fail(
                StartupReplayErrorV1::kJoinOrphan,
                *row.file,
                row.line,
                "queue SeqNo has no matching snapshot row at or before the final root cutoff");
        }

        if (!tail_sequence.has_value() ||
            row.sequence != *tail_sequence) {
            tail_sequence = row.sequence;
            have_bid = false;
            have_ask = false;
        } else if (!allow_opposite_sides_at_same_sequence) {
            return state->Fail(
                StartupReplayErrorV1::kJoinDuplicate,
                *row.file,
                row.line,
                "duplicate queue SeqNo in one-sided snapshot child");
        }
        bool* have_side = row.side == 'B' ? &have_bid : &have_ask;
        if (*have_side) {
            return state->Fail(
                StartupReplayErrorV1::kJoinDuplicate,
                *row.file,
                row.line,
                std::string("duplicate queue Side=") + row.side +
                    " in cropped snapshot tail");
        }
        *have_side = true;
    }
}

struct DepthLevel final {
    std::int64_t price = 0;
    std::int64_t volume = 0;
    std::uint32_t order_count = 0U;
};

bool ValidateQueueAgainstRoot(
    const QueueRow& queue,
    std::string_view root_security_id,
    std::uint32_t root_exchange_time,
    std::uint32_t root_local_time,
    std::uint32_t root_image_status,
    const DepthLevel& level,
    ReplayState* state) {
    if (queue.security_id != root_security_id) {
        return state->Fail(
            StartupReplayErrorV1::kJoinMismatch,
            *queue.file,
            queue.line,
            "queue SecurityID differs from snapshot");
    }
    if (queue.exchange_time != root_exchange_time ||
        queue.local_time != root_local_time) {
        return state->Fail(
            StartupReplayErrorV1::kJoinMismatch,
            *queue.file,
            queue.line,
            "queue exchange/local time differs from snapshot");
    }
    if (queue.image_status != root_image_status) {
        return state->Fail(
            StartupReplayErrorV1::kJoinMismatch,
            *queue.file,
            queue.line,
            "queue ImageStatus differs from snapshot");
    }
    if (queue.price != level.price ||
        queue.volume != level.volume ||
        queue.total_order_count != level.order_count) {
        return state->Fail(
            StartupReplayErrorV1::kJoinMismatch,
            *queue.file,
            queue.line,
            "queue price/volume/count differs from snapshot best level");
    }
    return true;
}

bool RequireQueueWhenOrdersExist(
    const std::optional<QueueRow>& queue,
    const DepthLevel& level,
    char side,
    const CsvTable& root_table,
    const CsvRecord& root_record,
    ReplayState* state) {
    if (level.order_count != 0U && !queue.has_value()) {
        return state->Fail(
            StartupReplayErrorV1::kJoinMismatch,
            root_table.input().path,
            root_record.line,
            std::string("missing Side=") + side +
                " best-level queue for nonzero NumOrders");
    }
    return true;
}

bool BuildShanghaiSnapshot(
    const CsvTable& table,
    const CsvRecord& record,
    QueuePair queues,
    PendingMessage* output,
    ReplayState* state) {
    RowParser parse(table, record, state);
    std::uint32_t update_time = 0U;
    std::uint32_t local_time = 0U;
    std::uint64_t sequence = 0U;
    std::string security_id;
    std::string instrument_status;
    std::uint32_t image_u32 = 0U;
    std::uint32_t bid_feed_count = 0U;
    std::uint32_t ask_feed_count = 0U;
    if (!parse.Time("UpdateTime", &update_time) ||
        !parse.Ascii("SecurityID", &security_id) ||
        !parse.U32("ImageStatus", &image_u32) ||
        !parse.Ascii(
            "InstruStatus", &instrument_status, true) ||
        !parse.Time("LocalTime", &local_time) ||
        !parse.PositiveU64("SeqNo", &sequence)) {
        return false;
    }
    if (image_u32 >
        static_cast<std::uint32_t>(
            std::numeric_limits<std::int32_t>::max())) {
        return state->Fail(
            StartupReplayErrorV1::kNumericOverflow,
            table.input().path,
            record.line,
            "ImageStatus does not fit SDK int32");
    }
    if (image_u32 != 1U && image_u32 != 3U) {
        return state->Fail(
            StartupReplayErrorV1::kValueInvalid,
            table.input().path,
            record.line,
            "Shanghai client snapshot ImageStatus must be 1 or 3");
    }
    constexpr std::array<std::string_view, 22U>
        kDocumentedInstrumentStatuses{
            "START", "OCALL", "TRADE", "SUSP", "CCALL", "CLOSE",
            "ENDTR", "HALT", "ADD", "BETW", "BREAK", "DEL",
            "FCALL", "ICALL", "IOBB", "IPOBB", "OOBB", "OPOBB",
            "NOTRD", "POSTR", "PRETR", "VOLA"};
    if (!instrument_status.empty() &&
        std::find(
            kDocumentedInstrumentStatuses.begin(),
            kDocumentedInstrumentStatuses.end(),
            instrument_status) ==
            kDocumentedInstrumentStatuses.end()) {
        return state->Fail(
            StartupReplayErrorV1::kValueInvalid,
            table.input().path,
            record.line,
            "Shanghai InstruStatus is not a documented V4 value");
    }

    BodyWriter writer(sizeof(sh::SHL2MarketData));
    bool wire_ok =
        writer.StoreU32(0U, update_time) &&
        writer.StoreI32(10U, static_cast<std::int32_t>(image_u32));
    const auto fixed32 = [&](std::string_view name,
                             std::size_t offset,
                             std::uint8_t scale) {
        std::int32_t value = 0;
        return parse.Fixed32(name, scale, &value) &&
               writer.StoreI32(offset, value);
    };
    const auto fixed64 = [&](std::string_view name,
                             std::size_t offset,
                             std::uint8_t scale) {
        std::int64_t value = 0;
        return parse.Fixed64(name, scale, &value) &&
               writer.StoreI64(offset, value);
    };
    const auto u32 = [&](std::string_view name, std::size_t offset) {
        std::uint32_t value = 0U;
        return parse.U32(name, &value) &&
               writer.StoreU32(offset, value);
    };
    wire_ok =
        wire_ok &&
        fixed32("PreCloPrice", 14U, 3U) &&
        fixed32("OpenPrice", 18U, 3U) &&
        fixed32("HighPrice", 22U, 3U) &&
        fixed32("LowPrice", 26U, 3U) &&
        fixed32("LastPrice", 30U, 3U) &&
        fixed32("ClosePrice", 34U, 3U) &&
        u32("TradNumber", 44U) &&
        fixed64("TradVolume", 48U, 3U) &&
        fixed64("Turnover", 56U, 5U) &&
        fixed64("TotalBidVol", 64U, 3U) &&
        fixed32("WAvgBidPri", 72U, 3U) &&
        fixed32("AltWAvgBidPri", 76U, 3U) &&
        fixed64("TotalAskVol", 80U, 3U) &&
        fixed32("WAvgAskPri", 88U, 3U) &&
        fixed32("AltWAvgAskPri", 92U, 3U) &&
        u32("EtfBuyNumber", 96U) &&
        fixed64("EtfBuyVolume", 100U, 3U) &&
        fixed64("EtfBuyMoney", 108U, 5U) &&
        u32("EtfSellNumber", 116U) &&
        fixed64("EtfSellVolume", 120U, 3U) &&
        fixed64("ETFSellMoney", 128U, 5U) &&
        fixed32("YieldToMatu", 136U, 4U) &&
        fixed64("TotWarExNum", 140U, 3U) &&
        fixed64("WarLowerPri", 148U, 3U) &&
        fixed64("WarUpperPri", 156U, 5U) &&
        u32("WiDBuyNum", 164U) &&
        fixed64("WiDBuyVol", 168U, 3U) &&
        fixed64("WiDBuyMon", 176U, 5U) &&
        u32("WiDSellNum", 184U) &&
        fixed64("WiDSellVol", 188U, 3U) &&
        fixed64("WiDSellMon", 196U, 5U) &&
        u32("TotBidNum", 204U) &&
        u32("TotSellNum", 208U) &&
        u32("MaxBidDur", 212U) &&
        u32("MaxSellDur", 216U) &&
        parse.U32("BidNum", &bid_feed_count) &&
        writer.StoreU32(220U, bid_feed_count) &&
        parse.U32("SellNum", &ask_feed_count) &&
        writer.StoreU32(224U, ask_feed_count) &&
        fixed32("IOPV", 244U, 3U);
    if (!wire_ok ||
        !writer.AddString(4U, security_id) ||
        !writer.AddString(38U, instrument_status)) {
        return state->result.error == StartupReplayErrorV1::kNone
                   ? EnsureWriter(false, table, record, state)
                   : false;
    }

    std::array<DepthLevel, kPublishedDepth> bids{};
    std::array<DepthLevel, kPublishedDepth> asks{};
    std::array<bool, kPublishedDepth> bid_present{};
    std::array<bool, kPublishedDepth> ask_present{};
    for (std::size_t index = 0U; index < kPublishedDepth; ++index) {
        const std::string ordinal = std::to_string(index + 1U);
        const bool bid_price_present =
            !table.Get(record, "BidPrice" + ordinal).empty();
        const bool bid_volume_present =
            !table.Get(record, "BidVolume" + ordinal).empty();
        const bool bid_orders_present =
            !table.Get(record, "NumOrdersB" + ordinal).empty();
        const bool ask_price_present =
            !table.Get(record, "AskPrice" + ordinal).empty();
        const bool ask_volume_present =
            !table.Get(record, "AskVolume" + ordinal).empty();
        const bool ask_orders_present =
            !table.Get(record, "NumOrdersS" + ordinal).empty();
        const bool any_bid =
            bid_price_present || bid_volume_present ||
            bid_orders_present;
        const bool any_ask =
            ask_price_present || ask_volume_present ||
            ask_orders_present;
        if ((any_bid &&
             !(bid_price_present && bid_volume_present &&
               bid_orders_present)) ||
            (any_ask &&
             !(ask_price_present && ask_volume_present &&
               ask_orders_present))) {
            return state->Fail(
                StartupReplayErrorV1::kValueInvalid,
                table.input().path,
                record.line,
                "flattened Shanghai depth item is only partially present");
        }
        bid_present[index] = any_bid;
        ask_present[index] = any_ask;
        std::int32_t bid_price = 0;
        std::int32_t ask_price = 0;
        if (!parse.OptionalFixed32(
                "BidPrice" + ordinal, 3U, &bid_price) ||
            !parse.OptionalFixed64(
                "BidVolume" + ordinal, 3U,
                &bids[index].volume) ||
            !parse.OptionalU32(
                "NumOrdersB" + ordinal,
                &bids[index].order_count) ||
            !parse.OptionalFixed32(
                "AskPrice" + ordinal, 3U, &ask_price) ||
            !parse.OptionalFixed64(
                "AskVolume" + ordinal, 3U,
                &asks[index].volume) ||
            !parse.OptionalU32(
                "NumOrdersS" + ordinal,
                &asks[index].order_count)) {
            return false;
        }
        bids[index].price = bid_price;
        asks[index].price = ask_price;
    }
    if (!RequireQueueWhenOrdersExist(
            queues.bid, bids.front(), 'B',
            table, record, state) ||
        !RequireQueueWhenOrdersExist(
            queues.ask, asks.front(), 'S',
            table, record, state)) {
        return false;
    }
    if (queues.bid.has_value() &&
        !ValidateQueueAgainstRoot(
            *queues.bid,
            security_id,
            update_time,
            local_time,
            image_u32,
            bids.front(),
            state)) {
        return false;
    }
    if (queues.ask.has_value() &&
        !ValidateQueueAgainstRoot(
            *queues.ask,
            security_id,
            update_time,
            local_time,
            image_u32,
            asks.front(),
            state)) {
        return false;
    }

    const auto infer_flattened_depth =
        [&](std::span<const bool> present,
            std::string_view side,
            std::size_t* depth) {
            bool empty_seen = false;
            *depth = 0U;
            for (std::size_t index = 0U;
                 index < present.size();
                 ++index) {
                if (!present[index]) {
                    empty_seen = true;
                    continue;
                }
                if (empty_seen) {
                    return state->Fail(
                        StartupReplayErrorV1::kValueInvalid,
                        table.input().path,
                        record.line,
                        std::string(side) +
                            " flattened depth contains an item after an empty slot");
                }
                *depth = index + 1U;
            }
            return true;
        };
    std::size_t bid_depth = 0U;
    std::size_t ask_depth = 0U;
    if (!infer_flattened_depth(
            bid_present, "bid", &bid_depth) ||
        !infer_flattened_depth(
            ask_present, "ask", &ask_depth)) {
        return false;
    }
    std::size_t bid_start = 0U;
    std::size_t ask_start = 0U;
    if (!writer.AddList(
            228U,
            static_cast<std::uint32_t>(bid_depth),
            sizeof(sh::SHL2MarketData::BidLevelsItem),
            &bid_start) ||
        !writer.AddList(
            236U,
            static_cast<std::uint32_t>(ask_depth),
            sizeof(sh::SHL2MarketData::SellLevelsItem),
            &ask_start)) {
        return EnsureWriter(false, table, record, state);
    }
    const auto write_side =
        [&](std::size_t list_start,
            std::span<const DepthLevel> levels,
            const std::optional<QueueRow>& queue) {
            for (std::size_t index = 0U;
                 index < levels.size();
                 ++index) {
                const std::size_t item =
                    list_start + index * 28U;
                const std::uint32_t level_operator =
                    index == 0U && queue.has_value()
                        ? queue->level_operator
                        : 0U;
                if (!writer.StoreU32(item, level_operator) ||
                    !writer.StoreI32(
                        item + 4U,
                        static_cast<std::int32_t>(
                            levels[index].price)) ||
                    !writer.StoreI64(
                        item + 8U, levels[index].volume) ||
                    !writer.StoreU32(
                        item + 16U,
                        levels[index].order_count)) {
                    return false;
                }
                const std::span<const std::int64_t> quantities =
                    index == 0U && queue.has_value()
                        ? std::span<const std::int64_t>(
                              queue->quantities)
                        : std::span<const std::int64_t>{};
                std::size_t queue_start = 0U;
                if (!writer.AddList(
                        item + 20U,
                        static_cast<std::uint32_t>(
                            quantities.size()),
                        16U,
                        &queue_start)) {
                    return false;
                }
                for (std::size_t queue_index = 0U;
                     queue_index < quantities.size();
                     ++queue_index) {
                    if (!writer.StoreI64(
                            queue_start +
                                queue_index * 16U + 8U,
                            quantities[queue_index])) {
                        return false;
                    }
                }
            }
            return true;
        };
    if (!write_side(
            bid_start,
            std::span<const DepthLevel>(bids).first(bid_depth),
            queues.bid) ||
        !write_side(
            ask_start,
            std::span<const DepthLevel>(asks).first(ask_depth),
            queues.ask)) {
        return EnsureWriter(false, table, record, state);
    }
    PendingMessage pending;
    pending.key = {4U, kServiceVersion, 4U};
    pending.file = &table.input().path;
    pending.line = record.line;
    pending.csv_sequence = sequence;
    pending.local_time = local_time;
    if ((queues.bid.has_value() &&
         !queues.bid->quantities.empty()) ||
        (queues.ask.has_value() &&
         !queues.ask->quantities.empty())) {
        pending.notice_flags |=
            kStartupReplayNoticeShanghaiOrderQueueMetadataUnavailableV1;
    }
    pending.body = std::move(writer).Take();
    *output = std::move(pending);
    return true;
}

bool BuildShenzhenSnapshot(
    const CsvTable& table,
    const CsvRecord& record,
    std::optional<QueueRow> bid_queue,
    std::optional<QueueRow> ask_queue,
    PendingMessage* output,
    ReplayState* state) {
    RowParser parse(table, record, state);
    std::uint32_t update_time = 0U;
    std::uint32_t local_time = 0U;
    std::uint64_t sequence = 0U;
    std::uint32_t channel = 0U;
    std::string md_stream_id;
    std::string security_id;
    std::string security_id_source;
    std::string trading_phase;
    if (!parse.Time("UpdateTime", &update_time) ||
        !parse.RightSpacePaddedAscii(
            "MDStreamID", &md_stream_id) ||
        !parse.Ascii("SecurityID", &security_id) ||
        !parse.RightSpacePaddedAscii(
            "SecurityIDSource", &security_id_source) ||
        !parse.RightSpacePaddedAscii(
            "TradingPhaseCode", &trading_phase) ||
        !parse.Time("LocalTime", &local_time) ||
        !parse.PositiveU64("SeqNo", &sequence)) {
        return false;
    }
    if (table.HasColumn("ChannelNo") &&
        !parse.U32("ChannelNo", &channel)) {
        return false;
    }
    if (md_stream_id != "010" &&
        md_stream_id != "020" &&
        md_stream_id != "030") {
        return state->Fail(
            StartupReplayErrorV1::kValueInvalid,
            table.input().path,
            record.line,
            "Shenzhen snapshot MDStreamID must be 010, 020, or 030");
    }
    if (security_id_source != "102") {
        return state->Fail(
            StartupReplayErrorV1::kValueInvalid,
            table.input().path,
            record.line,
            "Shenzhen SecurityIDSource must be 102");
    }
    if (trading_phase.size() != 2U ||
        std::string_view("SOTBCEHAV").find(trading_phase[0U]) ==
            std::string_view::npos ||
        (trading_phase[1U] != '0' &&
         trading_phase[1U] != '1')) {
        return state->Fail(
            StartupReplayErrorV1::kValueInvalid,
            table.input().path,
            record.line,
            "TradingPhaseCode must contain a documented phase and 0/1");
    }

    BodyWriter writer(sizeof(sz::Snapshot300111_v2));
    bool wire_ok =
        writer.StoreU32(0U, update_time) &&
        writer.StoreU32(4U, channel);
    const auto fixed64 = [&](std::string_view name,
                             std::size_t offset,
                             std::uint8_t scale) {
        std::int64_t value = 0;
        return parse.Fixed64(name, scale, &value) &&
               writer.StoreI64(offset, value);
    };
    const auto nonnegative_i64 =
        [&](std::string_view name, std::size_t offset) {
            std::int64_t value = 0;
            return parse.NonnegativeI64(name, &value) &&
                   writer.StoreI64(offset, value);
        };
    wire_ok =
        wire_ok &&
        fixed64("PreCloPrice", 32U, 4U) &&
        nonnegative_i64("TurnNum", 40U) &&
        nonnegative_i64("Volume", 48U) &&
        fixed64("Turnover", 56U, 4U) &&
        fixed64("LastPrice", 64U, 6U) &&
        fixed64("OpenPrice", 72U, 6U) &&
        fixed64("HighPrice", 80U, 6U) &&
        fixed64("LowPrice", 88U, 6U) &&
        fixed64("DifPrice1", 96U, 6U) &&
        fixed64("DifPrice2", 104U, 6U) &&
        fixed64("PE1", 112U, 6U) &&
        fixed64("PE2", 120U, 6U) &&
        fixed64("PreCloseIOPV", 128U, 6U) &&
        fixed64("IOPV", 136U, 6U) &&
        nonnegative_i64("TotalOfferQty", 144U) &&
        fixed64("WeightedAvgOfferPx", 152U, 6U) &&
        nonnegative_i64("TotalBidQty", 160U) &&
        fixed64("WeightedAvgBidPx", 168U, 6U) &&
        fixed64("HighLimitPrice", 176U, 6U) &&
        fixed64("LowLimitPrice", 184U, 6U) &&
        nonnegative_i64("OpenInt", 192U) &&
        fixed64("OptPremiumRatio", 200U, 6U);
    if (!wire_ok ||
        !writer.AddString(8U, md_stream_id) ||
        !writer.AddString(14U, security_id) ||
        !writer.AddString(20U, kShenzhenSdkSecurityIdSource) ||
        !writer.AddString(26U, trading_phase)) {
        return state->result.error == StartupReplayErrorV1::kNone
                   ? EnsureWriter(false, table, record, state)
                   : false;
    }

    std::array<DepthLevel, kPublishedDepth> bids{};
    std::array<DepthLevel, kPublishedDepth> asks{};
    std::array<bool, kPublishedDepth> bid_present{};
    std::array<bool, kPublishedDepth> ask_present{};
    for (std::size_t index = 0U; index < kPublishedDepth; ++index) {
        const std::string ordinal = std::to_string(index + 1U);
        const bool bid_price_present =
            !table.Get(record, "BidPrice" + ordinal).empty();
        const bool bid_volume_present =
            !table.Get(record, "BidVolume" + ordinal).empty();
        const bool bid_orders_present =
            !table.Get(record, "NumOrdersB" + ordinal).empty();
        const bool ask_price_present =
            !table.Get(record, "AskPrice" + ordinal).empty();
        const bool ask_volume_present =
            !table.Get(record, "AskVolume" + ordinal).empty();
        const bool ask_orders_present =
            !table.Get(record, "NumOrdersS" + ordinal).empty();
        const bool any_bid =
            bid_price_present || bid_volume_present ||
            bid_orders_present;
        const bool any_ask =
            ask_price_present || ask_volume_present ||
            ask_orders_present;
        if ((any_bid &&
             !(bid_price_present && bid_volume_present &&
               bid_orders_present)) ||
            (any_ask &&
             !(ask_price_present && ask_volume_present &&
               ask_orders_present))) {
            return state->Fail(
                StartupReplayErrorV1::kValueInvalid,
                table.input().path,
                record.line,
                "flattened Shenzhen depth item is only partially present");
        }
        bid_present[index] = any_bid;
        ask_present[index] = any_ask;
        if (!parse.OptionalFixed64(
                "BidPrice" + ordinal, 6U,
                &bids[index].price) ||
            !parse.OptionalNonnegativeI64(
                "BidVolume" + ordinal,
                &bids[index].volume) ||
            !parse.OptionalU32(
                "NumOrdersB" + ordinal,
                &bids[index].order_count) ||
            !parse.OptionalFixed64(
                "AskPrice" + ordinal, 6U,
                &asks[index].price) ||
            !parse.OptionalNonnegativeI64(
                "AskVolume" + ordinal,
                &asks[index].volume) ||
            !parse.OptionalU32(
                "NumOrdersS" + ordinal,
                &asks[index].order_count)) {
            return false;
        }
    }
    if (!RequireQueueWhenOrdersExist(
            bid_queue, bids.front(), 'B',
            table, record, state) ||
        !RequireQueueWhenOrdersExist(
            ask_queue, asks.front(), 'S',
            table, record, state)) {
        return false;
    }
    if (bid_queue.has_value() &&
        !ValidateQueueAgainstRoot(
            *bid_queue,
            security_id,
            update_time,
            local_time,
            1U,
            bids.front(),
            state)) {
        return false;
    }
    if (ask_queue.has_value() &&
        !ValidateQueueAgainstRoot(
            *ask_queue,
            security_id,
            update_time,
            local_time,
            1U,
            asks.front(),
            state)) {
        return false;
    }

    std::size_t bid_start = 0U;
    std::size_t ask_start = 0U;
    const auto infer_depth =
        [&](std::span<const bool> present,
            std::string_view side_name,
            std::size_t* depth) {
            bool inactive_seen = false;
            *depth = 0U;
            for (std::size_t index = 0U;
                 index < present.size();
                 ++index) {
                if (!present[index]) {
                    inactive_seen = true;
                    continue;
                }
                if (inactive_seen) {
                    return state->Fail(
                        StartupReplayErrorV1::kValueInvalid,
                        table.input().path,
                        record.line,
                        std::string(side_name) +
                            " depth contains a populated level after an "
                            "empty level");
                }
                *depth = index + 1U;
            }
            return true;
        };
    std::size_t bid_depth = 0U;
    std::size_t ask_depth = 0U;
    if (!infer_depth(bid_present, "bid", &bid_depth) ||
        !infer_depth(ask_present, "ask", &ask_depth)) {
        return false;
    }
    if (!writer.AddList(
            208U,
            static_cast<std::uint32_t>(bid_depth),
            sizeof(sz::Snapshot300111_v2::BidPriceLevelItem),
            &bid_start) ||
        !writer.AddList(
            216U,
            static_cast<std::uint32_t>(ask_depth),
            sizeof(sz::Snapshot300111_v2::AskPriceLevelItem),
            &ask_start)) {
        return EnsureWriter(false, table, record, state);
    }
    const auto write_side =
        [&](std::size_t list_start,
            std::span<const DepthLevel> levels,
            const std::optional<QueueRow>& queue) {
            for (std::size_t index = 0U;
                 index < levels.size();
                 ++index) {
                const std::size_t item =
                    list_start + index * 28U;
                if (!writer.StoreI64(
                        item, levels[index].volume) ||
                    !writer.StoreI64(
                        item + 8U, levels[index].price) ||
                    !writer.StoreU32(
                        item + 16U,
                        levels[index].order_count)) {
                    return false;
                }
                const std::span<const std::int64_t> quantities =
                    index == 0U && queue.has_value()
                        ? std::span<const std::int64_t>(
                              queue->quantities)
                        : std::span<const std::int64_t>{};
                std::size_t queue_start = 0U;
                if (!writer.AddList(
                        item + 20U,
                        static_cast<std::uint32_t>(
                            quantities.size()),
                        8U,
                        &queue_start)) {
                    return false;
                }
                for (std::size_t queue_index = 0U;
                     queue_index < quantities.size();
                     ++queue_index) {
                    if (!writer.StoreI64(
                            queue_start + queue_index * 8U,
                            quantities[queue_index])) {
                        return false;
                    }
                }
            }
            return true;
        };
    if (!write_side(
            bid_start,
            std::span<const DepthLevel>(bids).first(bid_depth),
            bid_queue) ||
        !write_side(
            ask_start,
            std::span<const DepthLevel>(asks).first(ask_depth),
            ask_queue)) {
        return EnsureWriter(false, table, record, state);
    }
    PendingMessage pending;
    pending.key = {6U, kServiceVersion, 28U};
    pending.file = &table.input().path;
    pending.line = record.line;
    pending.csv_sequence = sequence;
    pending.local_time = local_time;
    if (!table.HasColumn("ChannelNo")) {
        pending.notice_flags |=
            kStartupReplayNoticeShenzhenSnapshotChannelUnavailableV1;
    }
    pending.body = std::move(writer).Take();
    *output = std::move(pending);
    return true;
}

bool BuildShanghaiTick(
    const CsvTable& table,
    const CsvRecord& record,
    PendingMessage* output,
    std::int32_t* channel_output,
    std::int64_t* business_index_output,
    ReplayState* state) {
    RowParser parse(table, record, state);
    std::int64_t business_index = 0;
    std::uint32_t channel_u32 = 0U;
    std::uint32_t tick_time = 0U;
    std::uint32_t local_time = 0U;
    std::uint64_t sequence = 0U;
    std::int64_t buy_order = 0;
    std::int64_t sell_order = 0;
    std::int64_t quantity = 0;
    std::int32_t price = 0;
    std::int64_t trade_money = 0;
    std::string security_id;
    std::string type;
    std::string tick_flag;
    if (!parse.PositiveI64("BizIndex", &business_index) ||
        !parse.PositiveU32("Channel", &channel_u32) ||
        !parse.Ascii("SecurityID", &security_id) ||
        !parse.Time("TickTime", &tick_time) ||
        !parse.Ascii("Type", &type) ||
        !parse.NonnegativeI64("BuyOrderNO", &buy_order) ||
        !parse.NonnegativeI64("SellOrderNO", &sell_order) ||
        !parse.Fixed32("Price", 3U, &price) ||
        !parse.NonnegativeI64("Qty", &quantity) ||
        !parse.Fixed64("TradeMoney", 3U, &trade_money) ||
        !parse.Ascii("TickBSFlag", &tick_flag) ||
        !parse.Time("LocalTime", &local_time) ||
        !parse.PositiveU64("SeqNo", &sequence)) {
        return false;
    }
    if (channel_u32 >
        static_cast<std::uint32_t>(
            std::numeric_limits<std::int32_t>::max())) {
        return state->Fail(
            StartupReplayErrorV1::kNumericOverflow,
            table.input().path,
            record.line,
            "Channel does not fit SDK int32");
    }
    if (type.size() != 1U ||
        std::string_view("ADST").find(type.front()) ==
            std::string_view::npos) {
        return state->Fail(
            StartupReplayErrorV1::kValueInvalid,
            table.input().path,
            record.line,
            "Type must be A, D, S, or T");
    }
    bool flag_valid = false;
    if (type == "A" || type == "D") {
        flag_valid = tick_flag == "B" || tick_flag == "S";
    } else if (type == "T") {
        flag_valid =
            tick_flag == "B" || tick_flag == "S" ||
            tick_flag == "N";
    } else {
        static constexpr std::array<std::string_view, 7U>
            kPhases{
                "START", "OCALL", "TRADE", "SUSP",
                "CCALL", "CLOSE", "ENDTR"};
        flag_valid = std::find(
                         kPhases.begin(), kPhases.end(),
                         std::string_view(tick_flag)) !=
                     kPhases.end();
    }
    if (!flag_valid) {
        return state->Fail(
            StartupReplayErrorV1::kValueInvalid,
            table.input().path,
            record.line,
            "TickBSFlag is invalid for Type");
    }

    BodyWriter writer(sizeof(sh::NGTSTick));
    if (!writer.StoreI64(0U, business_index) ||
        !writer.StoreI32(
            8U, static_cast<std::int32_t>(channel_u32)) ||
        !writer.StoreU32(18U, tick_time) ||
        !writer.StoreI64(28U, buy_order) ||
        !writer.StoreI64(36U, sell_order) ||
        !writer.StoreI32(44U, price) ||
        !writer.StoreI64(48U, quantity) ||
        !writer.StoreI64(56U, trade_money) ||
        !writer.AddString(12U, security_id) ||
        !writer.AddString(22U, type) ||
        !writer.AddString(64U, tick_flag)) {
        return EnsureWriter(false, table, record, state);
    }
    PendingMessage pending;
    pending.key = {4U, kServiceVersion, 24U};
    pending.file = &table.input().path;
    pending.line = record.line;
    pending.csv_sequence = sequence;
    pending.local_time = local_time;
    pending.body = std::move(writer).Take();
    *output = std::move(pending);
    *channel_output = static_cast<std::int32_t>(channel_u32);
    *business_index_output = business_index;
    return true;
}

bool BuildShenzhenOrder(
    const CsvTable& table,
    const CsvRecord& record,
    bool publish,
    PendingMessage* output,
    std::uint32_t* channel_output,
    std::uint64_t* application_sequence_output,
    ReplayState* state) {
    RowParser parse(table, record, state);
    std::uint32_t channel = 0U;
    std::int64_t application_sequence = 0;
    std::int64_t price = 0;
    std::int64_t quantity = 0;
    std::int32_t side = 0;
    std::uint32_t transact_time = 0U;
    std::int32_t order_type = 0;
    std::uint32_t local_time = 0U;
    std::uint64_t csv_sequence = 0U;
    std::string md_stream_id;
    std::string security_id;
    std::string security_id_source;
    if (!parse.U32("ChannelNo", &channel) ||
        !parse.PositiveI64(
            "ApplSeqNum", &application_sequence) ||
        !parse.RightSpacePaddedAscii(
            "MDStreamID", &md_stream_id) ||
        !parse.Ascii("SecurityID", &security_id) ||
        !parse.RightSpacePaddedAscii(
            "SecurityIDSource", &security_id_source) ||
        !parse.Fixed64("Price", 4U, &price) ||
        !parse.NonnegativeI64("OrderQty", &quantity) ||
        !parse.EncodedAsciiI32("Side", "12GF", &side) ||
        !parse.Time("TransactTime", &transact_time) ||
        !parse.EncodedAsciiI32("OrdType", "12U", &order_type) ||
        !parse.Time("LocalTime", &local_time) ||
        !parse.PositiveU64("SeqNo", &csv_sequence)) {
        return false;
    }
    if (md_stream_id != "011" &&
        md_stream_id != "021" &&
        md_stream_id != "041") {
        return state->Fail(
            StartupReplayErrorV1::kValueInvalid,
            table.input().path,
            record.line,
            "Shenzhen order MDStreamID must be 011, 021, or 041");
    }
    if (security_id_source != "102") {
        return state->Fail(
            StartupReplayErrorV1::kValueInvalid,
            table.input().path,
            record.line,
            "Shenzhen SecurityIDSource must be 102");
    }
    BodyWriter writer(sizeof(sz::Order300192_v2));
    if (!writer.StoreU32(0U, channel) ||
        !writer.StoreI64(4U, application_sequence) ||
        !writer.StoreI64(30U, price) ||
        !writer.StoreI64(38U, quantity) ||
        !writer.StoreI32(46U, side) ||
        !writer.StoreU32(50U, transact_time) ||
        !writer.StoreI32(54U, order_type) ||
        !writer.AddString(12U, md_stream_id) ||
        !writer.AddString(18U, security_id) ||
        !writer.AddString(24U, kShenzhenSdkSecurityIdSource)) {
        return EnsureWriter(false, table, record, state);
    }
    PendingMessage pending;
    pending.key = {6U, kServiceVersion, 33U};
    pending.file = &table.input().path;
    pending.line = record.line;
    pending.csv_sequence = csv_sequence;
    pending.local_time = local_time;
    pending.publish = publish;
    pending.body = std::move(writer).Take();
    *output = std::move(pending);
    *channel_output = channel;
    *application_sequence_output =
        static_cast<std::uint64_t>(application_sequence);
    return true;
}

bool BuildShenzhenTransaction(
    const CsvTable& table,
    const CsvRecord& record,
    bool publish,
    PendingMessage* output,
    std::uint32_t* channel_output,
    std::uint64_t* application_sequence_output,
    ReplayState* state) {
    RowParser parse(table, record, state);
    std::uint32_t channel = 0U;
    std::int64_t application_sequence = 0;
    std::int64_t bid_application_sequence = 0;
    std::int64_t offer_application_sequence = 0;
    std::int64_t last_price = 0;
    std::int64_t last_quantity = 0;
    std::int32_t execution_type = 0;
    std::uint32_t transact_time = 0U;
    std::uint32_t local_time = 0U;
    std::uint64_t csv_sequence = 0U;
    std::string md_stream_id;
    std::string security_id;
    std::string security_id_source;
    if (!parse.U32("ChannelNo", &channel) ||
        !parse.PositiveI64(
            "ApplSeqNum", &application_sequence) ||
        !parse.RightSpacePaddedAscii(
            "MDStreamID", &md_stream_id) ||
        !parse.NonnegativeI64(
            "BidApplSeqNum", &bid_application_sequence) ||
        !parse.NonnegativeI64(
            "OfferApplSeqNum", &offer_application_sequence) ||
        !parse.Ascii("SecurityID", &security_id) ||
        !parse.RightSpacePaddedAscii(
            "SecurityIDSource", &security_id_source) ||
        !parse.Fixed64("LastPx", 4U, &last_price) ||
        !parse.NonnegativeI64("LastQty", &last_quantity) ||
        !parse.EncodedAsciiI32("ExecType", "4F", &execution_type) ||
        !parse.Time("TransactTime", &transact_time) ||
        !parse.Time("LocalTime", &local_time) ||
        !parse.PositiveU64("SeqNo", &csv_sequence)) {
        return false;
    }
    if (md_stream_id != "011" &&
        md_stream_id != "021" &&
        md_stream_id != "041") {
        return state->Fail(
            StartupReplayErrorV1::kValueInvalid,
            table.input().path,
            record.line,
            "Shenzhen transaction MDStreamID must be 011, 021, or 041");
    }
    if (security_id_source != "102") {
        return state->Fail(
            StartupReplayErrorV1::kValueInvalid,
            table.input().path,
            record.line,
            "Shenzhen SecurityIDSource must be 102");
    }
    BodyWriter writer(sizeof(sz::Transaction300191_v2));
    if (!writer.StoreU32(0U, channel) ||
        !writer.StoreI64(4U, application_sequence) ||
        !writer.StoreI64(18U, bid_application_sequence) ||
        !writer.StoreI64(26U, offer_application_sequence) ||
        !writer.StoreI64(46U, last_price) ||
        !writer.StoreI64(54U, last_quantity) ||
        !writer.StoreI32(62U, execution_type) ||
        !writer.StoreU32(66U, transact_time) ||
        !writer.AddString(12U, md_stream_id) ||
        !writer.AddString(34U, security_id) ||
        !writer.AddString(40U, kShenzhenSdkSecurityIdSource)) {
        return EnsureWriter(false, table, record, state);
    }
    PendingMessage pending;
    pending.key = {6U, kServiceVersion, 36U};
    pending.file = &table.input().path;
    pending.line = record.line;
    pending.csv_sequence = csv_sequence;
    pending.local_time = local_time;
    pending.publish = publish;
    pending.body = std::move(writer).Take();
    *output = std::move(pending);
    *channel_output = channel;
    *application_sequence_output =
        static_cast<std::uint64_t>(application_sequence);
    return true;
}

bool ReplayShanghaiSnapshots(
    const InputFile& root_input,
    const InputFile& queue_input,
    ReplayState* state) {
    CsvTable root(
        root_input,
        ShanghaiSnapshotColumns(),
        {},
        state);
    CsvTable queue(
        queue_input,
        QueueColumns("UpdateTime"),
        {},
        state);
    if (!root.Initialize() || !queue.Initialize()) {
        return false;
    }
    QueueStream queue_stream(
        &queue, "UpdateTime", 3U, 3U, std::nullopt, state);
    bool have_previous_root = false;
    std::uint64_t previous_root_sequence = 0U;
    CsvRecord record;
    for (;;) {
        bool available = false;
        if (!root.Next(&record, &available)) {
            return false;
        }
        if (!available) {
            bool extended = false;
            if (!ExtendIncompleteTableOnce(
                    &root, &extended, state)) {
                return false;
            }
            if (extended) {
                continue;
            }
            break;
        }
        RowParser parse(root, record, state);
        std::uint64_t sequence = 0U;
        if (!parse.PositiveU64("SeqNo", &sequence)) {
            return false;
        }
        if (have_previous_root &&
            sequence <= previous_root_sequence) {
            return state->Fail(
                StartupReplayErrorV1::kSequenceDuplicate,
                root_input.path,
                record.line,
                "snapshot SeqNo must increase strictly in file order");
        }
        previous_root_sequence = sequence;
        have_previous_root = true;
        QueuePair queues;
        if (!CollectJoinedQueues(
                sequence, &queue_stream, &queues, state)) {
            return false;
        }
        std::uint32_t bid_orders = 0U;
        std::uint32_t ask_orders = 0U;
        if (!parse.OptionalU32("NumOrdersB1", &bid_orders) ||
            !parse.OptionalU32("NumOrdersS1", &ask_orders)) {
            return false;
        }
        const bool missing_required_queue =
            (bid_orders != 0U && !queues.bid.has_value()) ||
            (ask_orders != 0U && !queues.ask.has_value());
        if (missing_required_queue) {
            const QueueRow* next = nullptr;
            if (!queue_stream.Peek(&next)) {
                return false;
            }
            if (next == nullptr) {
                if (!CollectAppendedQueuesForRoot(
                        sequence,
                        &queue_stream,
                        &queues,
                        state)) {
                    return false;
                }
            }
        }
        PendingMessage pending;
        if (!BuildShanghaiSnapshot(
                root, record, std::move(queues),
                &pending, state) ||
            !PublishMessage(&pending, state)) {
            return false;
        }
    }
    return FinishQueueTail(
        &queue_stream,
        have_previous_root,
        previous_root_sequence,
        true,
        state);
}

bool ReplayShenzhenSnapshots(
    const InputFile& root_input,
    const InputFile& ask_input,
    const InputFile& bid_input,
    ReplayState* state) {
    CsvTable root(
        root_input,
        ShenzhenSnapshotColumns(),
        {"ChannelNo"},
        state);
    CsvTable asks(
        ask_input,
        QueueColumns("DataTimeStamp"),
        {},
        state);
    CsvTable bids(
        bid_input,
        QueueColumns("DataTimeStamp"),
        {},
        state);
    if (!root.Initialize() ||
        !asks.Initialize() ||
        !bids.Initialize()) {
        return false;
    }
    QueueStream ask_stream(
        &asks, "DataTimeStamp", 6U, 0U, 'S', state);
    QueueStream bid_stream(
        &bids, "DataTimeStamp", 6U, 0U, 'B', state);
    bool have_previous_root = false;
    std::uint64_t previous_root_sequence = 0U;
    CsvRecord record;
    for (;;) {
        bool available = false;
        if (!root.Next(&record, &available)) {
            return false;
        }
        if (!available) {
            bool extended = false;
            if (!ExtendIncompleteTableOnce(
                    &root, &extended, state)) {
                return false;
            }
            if (extended) {
                continue;
            }
            break;
        }
        RowParser parse(root, record, state);
        std::uint64_t sequence = 0U;
        if (!parse.PositiveU64("SeqNo", &sequence)) {
            return false;
        }
        if (have_previous_root &&
            sequence <= previous_root_sequence) {
            return state->Fail(
                StartupReplayErrorV1::kSequenceDuplicate,
                root_input.path,
                record.line,
                "snapshot SeqNo must increase strictly in file order");
        }
        previous_root_sequence = sequence;
        have_previous_root = true;
        QueuePair queues;
        if (!CollectJoinedQueueSide(
                sequence, &ask_stream, &queues.ask, state) ||
            !CollectJoinedQueueSide(
                sequence, &bid_stream, &queues.bid, state)) {
            return false;
        }
        const QueueRow* duplicate = nullptr;
        if (!ask_stream.Peek(&duplicate)) {
            return false;
        }
        const bool ask_at_captured_eof = duplicate == nullptr;
        if (duplicate != nullptr &&
            duplicate->sequence == sequence) {
            return state->Fail(
                StartupReplayErrorV1::kJoinDuplicate,
                *duplicate->file,
                duplicate->line,
                "duplicate ask queue SeqNo");
        }
        if (!bid_stream.Peek(&duplicate)) {
            return false;
        }
        const bool bid_at_captured_eof = duplicate == nullptr;
        if (duplicate != nullptr &&
            duplicate->sequence == sequence) {
            return state->Fail(
                StartupReplayErrorV1::kJoinDuplicate,
                *duplicate->file,
                duplicate->line,
                "duplicate bid queue SeqNo");
        }
        std::uint32_t ask_orders = 0U;
        std::uint32_t bid_orders = 0U;
        if (!parse.OptionalU32("NumOrdersS1", &ask_orders) ||
            !parse.OptionalU32("NumOrdersB1", &bid_orders)) {
            return false;
        }
        if (ask_orders != 0U &&
            !queues.ask.has_value() &&
            ask_at_captured_eof) {
            if (!CollectAppendedQueuesForRoot(
                    sequence,
                    &ask_stream,
                    &queues,
                    state)) {
                return false;
            }
        }
        if (bid_orders != 0U &&
            !queues.bid.has_value() &&
            bid_at_captured_eof) {
            if (!CollectAppendedQueuesForRoot(
                    sequence,
                    &bid_stream,
                    &queues,
                    state)) {
                return false;
            }
        }
        PendingMessage pending;
        if (!BuildShenzhenSnapshot(
                root,
                record,
                std::move(queues.bid),
                std::move(queues.ask),
                &pending,
                state) ||
            !PublishMessage(&pending, state)) {
            return false;
        }
    }
    return FinishQueueTail(
               &ask_stream,
               have_previous_root,
               previous_root_sequence,
               false,
               state) &&
           FinishQueueTail(
               &bid_stream,
               have_previous_root,
               previous_root_sequence,
               false,
               state);
}

bool ReplayShanghaiTicks(
    const InputFile& input,
    ReplayState* state) {
    CsvTable table(input, ShanghaiTickColumns(), {}, state);
    if (!table.Initialize()) {
        return false;
    }
    std::map<std::int32_t, std::int64_t> next_by_channel;
    std::optional<std::uint64_t> previous_recovery_sequence;
    CsvRecord record;
    for (;;) {
        bool available = false;
        if (!table.Next(&record, &available)) {
            return false;
        }
        if (!available) {
            bool extended = false;
            if (!ExtendIncompleteTableOnce(
                    &table, &extended, state)) {
                return false;
            }
            if (extended) {
                continue;
            }
            return true;
        }
        PendingMessage pending;
        std::int32_t channel = 0;
        std::int64_t business_index = 0;
        if (!BuildShanghaiTick(
                table,
                record,
                &pending,
                &channel,
                &business_index,
                state)) {
            return false;
        }
        if (previous_recovery_sequence.has_value() &&
            pending.csv_sequence <=
                *previous_recovery_sequence) {
            return state->Fail(
                StartupReplayErrorV1::kSequenceDuplicate,
                input.path,
                record.line,
                "Shanghai tick recovery SeqNo must increase strictly");
        }
        previous_recovery_sequence = pending.csv_sequence;
        auto [found, inserted] =
            next_by_channel.try_emplace(channel, 1);
        if (business_index != found->second) {
            std::ostringstream detail;
            detail << "Channel " << channel
                   << " expected BizIndex " << found->second
                   << " but read " << business_index;
            return state->Fail(
                StartupReplayErrorV1::kSequenceGap,
                input.path,
                record.line,
                detail.str());
        }
        if (found->second ==
            std::numeric_limits<std::int64_t>::max()) {
            return state->Fail(
                StartupReplayErrorV1::kNumericOverflow,
                input.path,
                record.line,
                "BizIndex sequence cannot advance without overflow");
        }
        ++found->second;
        if (!PublishMessage(&pending, state)) {
            return false;
        }
    }
}

struct ShenzhenTickCandidate final {
    PendingMessage pending;
    std::uint32_t channel = 0U;
    std::uint64_t application_sequence = 0U;
    std::uint64_t record_start_offset = 0U;
    std::uint64_t record_end_offset = 0U;
    bool completes_mandatory_partial = false;
};

struct ShenzhenChannelPending final {
    std::uint64_t next_sequence = 1U;
    std::map<std::uint64_t, PendingMessage> messages;
};

enum class InsertShenzhenPendingResult : std::uint8_t {
    kInserted = 0U,
    kFailed,
    kDeadlineExpired,
};

InsertShenzhenPendingResult InsertShenzhenPending(
    std::uint32_t channel,
    std::uint64_t application_sequence,
    PendingMessage pending,
    std::map<std::uint32_t, ShenzhenChannelPending>* channels,
    std::size_t* pending_count,
    std::size_t* pending_bytes,
    const std::chrono::steady_clock::time_point* deadline,
    ReplayState* state) {
    const auto deadline_expired = [&]() noexcept {
        return deadline != nullptr &&
               std::chrono::steady_clock::now() >= *deadline;
    };
    ShenzhenChannelPending& channel_state = (*channels)[channel];
    if (application_sequence < channel_state.next_sequence ||
        channel_state.messages.find(application_sequence) !=
            channel_state.messages.end()) {
        static_cast<void>(state->Fail(
            StartupReplayErrorV1::kSequenceDuplicate,
            *pending.file,
            pending.line,
            "duplicate or already-published ChannelNo/ApplSeqNum"));
        return InsertShenzhenPendingResult::kFailed;
    }
    if (application_sequence == channel_state.next_sequence) {
        if (deadline_expired()) {
            return InsertShenzhenPendingResult::kDeadlineExpired;
        }
        const PendingPublishResult publication =
            PublishMessageUntil(&pending, state, deadline);
        if (publication == PendingPublishResult::kDeadline) {
            return InsertShenzhenPendingResult::kDeadlineExpired;
        }
        if (publication == PendingPublishResult::kFailed) {
            return InsertShenzhenPendingResult::kFailed;
        }
        if (channel_state.next_sequence ==
            std::numeric_limits<std::uint64_t>::max()) {
            static_cast<void>(state->Fail(
                StartupReplayErrorV1::kNumericOverflow,
                *pending.file,
                pending.line,
                "ApplSeqNum sequence cannot advance without overflow"));
            return InsertShenzhenPendingResult::kFailed;
        }
        ++channel_state.next_sequence;
    } else {
        if (*pending_count >=
            state->config.maximum_pending_messages) {
            static_cast<void>(state->Fail(
                StartupReplayErrorV1::kResourceExhausted,
                *pending.file,
                pending.line,
                "Shenzhen cross-file gap window exceeds "
                "maximum_pending_messages"));
            return InsertShenzhenPendingResult::kFailed;
        }
        if (*pending_bytes >
                state->config.maximum_pending_bytes ||
            pending.body.size() >
                state->config.maximum_pending_bytes -
                    *pending_bytes) {
            static_cast<void>(state->Fail(
                StartupReplayErrorV1::kResourceExhausted,
                *pending.file,
                pending.line,
                "Shenzhen cross-file gap window exceeds "
                "maximum_pending_bytes"));
            return InsertShenzhenPendingResult::kFailed;
        }
        *pending_bytes += pending.body.size();
        channel_state.messages.emplace(
            application_sequence, std::move(pending));
        ++(*pending_count);
    }
    for (;;) {
        if (deadline_expired()) {
            return InsertShenzhenPendingResult::kDeadlineExpired;
        }
        auto found = channel_state.messages.find(
            channel_state.next_sequence);
        if (found == channel_state.messages.end()) {
            return InsertShenzhenPendingResult::kInserted;
        }
        PendingMessage ready = std::move(found->second);
        channel_state.messages.erase(found);
        --(*pending_count);
        *pending_bytes -= ready.body.size();
        const PendingPublishResult publication =
            PublishMessageUntil(&ready, state, deadline);
        if (publication == PendingPublishResult::kDeadline) {
            return InsertShenzhenPendingResult::kDeadlineExpired;
        }
        if (publication == PendingPublishResult::kFailed) {
            return InsertShenzhenPendingResult::kFailed;
        }
        if (channel_state.next_sequence ==
            std::numeric_limits<std::uint64_t>::max()) {
            static_cast<void>(state->Fail(
                StartupReplayErrorV1::kNumericOverflow,
                *ready.file,
                ready.line,
                "ApplSeqNum sequence cannot advance without overflow"));
            return InsertShenzhenPendingResult::kFailed;
        }
        ++channel_state.next_sequence;
    }
}

std::uint64_t ShenzhenExpectedSequence(
    const std::map<std::uint32_t, ShenzhenChannelPending>& channels,
    std::uint32_t channel) {
    const auto found = channels.find(channel);
    return found == channels.end()
               ? 1U
               : found->second.next_sequence;
}

bool LoadShenzhenOrderCandidate(
    CsvTable* table,
    bool publish,
    bool completes_mandatory_partial,
    std::size_t maximum_scan_bytes,
    std::optional<ShenzhenTickCandidate>* candidate,
    bool* done,
    std::optional<std::uint64_t>* previous_recovery_sequence,
    std::map<std::uint32_t, std::uint64_t>*
        previous_native_sequence,
    ReplayState* state) {
    if (*done || candidate->has_value()) {
        return true;
    }
    CsvRecord record;
    bool available = false;
    if (!table->Next(
            &record, &available, maximum_scan_bytes)) {
        return false;
    }
    if (!available) {
        *done = true;
        return true;
    }
    ShenzhenTickCandidate next;
    if (!BuildShenzhenOrder(
            *table,
            record,
            publish,
            &next.pending,
            &next.channel,
            &next.application_sequence,
            state)) {
        return false;
    }
    if (previous_recovery_sequence->has_value() &&
        next.pending.csv_sequence <=
            **previous_recovery_sequence) {
        return state->Fail(
            StartupReplayErrorV1::kSequenceDuplicate,
            table->input().path,
            record.line,
            "Shenzhen order recovery SeqNo must increase strictly");
    }
    const auto previous =
        previous_native_sequence->find(next.channel);
    if (previous != previous_native_sequence->end() &&
        next.application_sequence <= previous->second) {
        return state->Fail(
            StartupReplayErrorV1::kSequenceDuplicate,
            table->input().path,
            record.line,
            "Shenzhen order ApplSeqNum decreases within its file/channel");
    }
    *previous_recovery_sequence = next.pending.csv_sequence;
    (*previous_native_sequence)[next.channel] =
        next.application_sequence;
    next.record_start_offset = record.start_offset;
    next.record_end_offset = record.end_offset;
    next.completes_mandatory_partial =
        completes_mandatory_partial;
    *candidate = std::move(next);
    return true;
}

bool LoadShenzhenTransactionCandidate(
    CsvTable* table,
    bool publish,
    bool completes_mandatory_partial,
    std::size_t maximum_scan_bytes,
    std::optional<ShenzhenTickCandidate>* candidate,
    bool* done,
    std::optional<std::uint64_t>* previous_recovery_sequence,
    std::map<std::uint32_t, std::uint64_t>*
        previous_native_sequence,
    ReplayState* state) {
    if (*done || candidate->has_value()) {
        return true;
    }
    CsvRecord record;
    bool available = false;
    if (!table->Next(
            &record, &available, maximum_scan_bytes)) {
        return false;
    }
    if (!available) {
        *done = true;
        return true;
    }
    ShenzhenTickCandidate next;
    if (!BuildShenzhenTransaction(
            *table,
            record,
            publish,
            &next.pending,
            &next.channel,
            &next.application_sequence,
            state)) {
        return false;
    }
    if (previous_recovery_sequence->has_value() &&
        next.pending.csv_sequence <=
            **previous_recovery_sequence) {
        return state->Fail(
            StartupReplayErrorV1::kSequenceDuplicate,
            table->input().path,
            record.line,
            "Shenzhen transaction recovery SeqNo must increase strictly");
    }
    const auto previous =
        previous_native_sequence->find(next.channel);
    if (previous != previous_native_sequence->end() &&
        next.application_sequence <= previous->second) {
        return state->Fail(
            StartupReplayErrorV1::kSequenceDuplicate,
            table->input().path,
            record.line,
            "Shenzhen transaction ApplSeqNum decreases within its file/channel");
    }
    *previous_recovery_sequence = next.pending.csv_sequence;
    (*previous_native_sequence)[next.channel] =
        next.application_sequence;
    next.record_start_offset = record.start_offset;
    next.record_end_offset = record.end_offset;
    next.completes_mandatory_partial =
        completes_mandatory_partial;
    *candidate = std::move(next);
    return true;
}

bool ReplayShenzhenTicks(
    const InputFile& order_input,
    const InputFile& transaction_input,
    bool publish_orders,
    bool publish_transactions,
    ReplayState* state) {
    StartupReplayBoundaryAlignmentStatsV1& alignment =
        state->result.boundary_alignment;
    alignment.phase =
        StartupReplayBoundaryAlignmentPhaseV1::kInitialReplay;
    alignment.initial_order_cut = order_input.prefix_bytes;
    alignment.initial_transaction_cut =
        transaction_input.prefix_bytes;
    alignment.order_available_end = order_input.prefix_bytes;
    alignment.transaction_available_end =
        transaction_input.prefix_bytes;
    state->sink.ObserveBoundaryAlignment(alignment);

    CsvTable orders(
        order_input, ShenzhenOrderColumns(), {}, state);
    CsvTable transactions(
        transaction_input,
        ShenzhenTransactionColumns(),
        {},
        state);
    if (!orders.Initialize() || !transactions.Initialize()) {
        return false;
    }
    alignment.sealed_order_cut = orders.header_end_offset();
    alignment.sealed_transaction_cut =
        transactions.header_end_offset();
    alignment.order_available_end = orders.available_end();
    alignment.transaction_available_end =
        transactions.available_end();
    state->sink.ObserveBoundaryAlignment(alignment);
    bool orders_done = false;
    bool transactions_done = false;
    std::optional<ShenzhenTickCandidate> order_candidate;
    std::optional<ShenzhenTickCandidate> transaction_candidate;
    std::optional<std::uint64_t> previous_order_recovery_sequence;
    std::optional<std::uint64_t>
        previous_transaction_recovery_sequence;
    std::map<std::uint32_t, std::uint64_t>
        previous_order_native_sequence;
    std::map<std::uint32_t, std::uint64_t>
        previous_transaction_native_sequence;
    std::map<std::uint32_t, ShenzhenChannelPending> channels;
    std::size_t pending_count = 0U;
    std::size_t pending_bytes = 0U;
    bool aligning = false;
    bool order_mandatory_partial = false;
    bool transaction_mandatory_partial = false;
    bool first_alignment_poll = true;
    using AlignmentClock = std::chrono::steady_clock;
    AlignmentClock::time_point alignment_started{};
    AlignmentClock::time_point alignment_deadline{};
    std::chrono::milliseconds alignment_poll_delay(
        state->config.boundary_alignment_initial_poll_ms);

    const auto saturating_add = [](
                                    std::uint64_t left,
                                    std::uint64_t right) noexcept {
        return right > std::numeric_limits<std::uint64_t>::max() - left
                   ? std::numeric_limits<std::uint64_t>::max()
                   : left + right;
    };
    const auto update_alignment_scan = [&]() {
        if (!aligning) {
            return;
        }
        alignment.order_scanned_bytes =
            orders.bytes_read();
        alignment.transaction_scanned_bytes =
            transactions.bytes_read();
        alignment.scanned_records = saturating_add(
            orders.complete_records_read(),
            transactions.complete_records_read());
    };

    const auto update_waited = [&]() {
        if (!aligning) {
            return;
        }
        const auto elapsed = std::chrono::duration_cast<
            std::chrono::milliseconds>(
            AlignmentClock::now() - alignment_started);
        alignment.waited_ms = elapsed.count() <= 0
                                  ? 0U
                                  : static_cast<std::uint64_t>(
                                        elapsed.count());
    };
    const auto update_alignment_observation = [&]() {
        alignment.pending_messages =
            static_cast<std::uint64_t>(pending_count);
        alignment.pending_channels = 0U;
        alignment.representative_missing_channel = 0U;
        alignment.representative_missing_appl_seq = 0U;
        for (const auto& [channel, channel_state] : channels) {
            if (channel_state.messages.empty()) {
                continue;
            }
            if (alignment.pending_channels == 0U) {
                alignment.representative_missing_channel = channel;
                alignment.representative_missing_appl_seq =
                    channel_state.next_sequence;
            }
            ++alignment.pending_channels;
        }
        alignment.peak_pending_messages = std::max(
            alignment.peak_pending_messages,
            static_cast<std::uint64_t>(pending_count));
        alignment.order_available_end = orders.available_end();
        alignment.transaction_available_end =
            transactions.available_end();
        alignment.order_mandatory_partial =
            order_mandatory_partial;
        alignment.transaction_mandatory_partial =
            transaction_mandatory_partial;
    };
    const auto alignment_closed = [&]() noexcept {
        return pending_count == 0U &&
               !order_mandatory_partial &&
               !transaction_mandatory_partial;
    };
    const auto fail_alignment_timeout = [&]() {
        update_waited();
        update_alignment_scan();
        update_alignment_observation();
        const PendingMessage* first_pending = nullptr;
        std::uint32_t missing_channel = 0U;
        std::uint64_t missing_sequence = 0U;
        std::size_t pending_channels = 0U;
        for (const auto& [channel, channel_state] : channels) {
            if (channel_state.messages.empty()) {
                continue;
            }
            ++pending_channels;
            if (first_pending == nullptr) {
                first_pending = &channel_state.messages.begin()->second;
                missing_channel = channel;
                missing_sequence = channel_state.next_sequence;
            }
        }
        std::ostringstream detail;
        detail << "waited_ms=" << alignment.waited_ms
               << " polls=" << alignment.poll_count
               << " pending_messages=" << pending_count
               << " pending_channels=" << pending_channels;
        if (first_pending != nullptr) {
            detail << " representative_missing_channel="
                   << missing_channel
                   << " representative_missing_appl_seq="
                   << missing_sequence;
        }
        detail << " order_initial_cut="
               << alignment.initial_order_cut
               << " order_sealed_cut="
               << alignment.sealed_order_cut
               << " order_available_end=" << orders.available_end()
               << " transaction_initial_cut="
               << alignment.initial_transaction_cut
               << " transaction_sealed_cut="
               << alignment.sealed_transaction_cut
               << " transaction_available_end="
               << transactions.available_end()
               << " order_extension_bytes="
               << alignment.order_extension_bytes
               << " transaction_extension_bytes="
               << alignment.transaction_extension_bytes
               << " scanned_records="
               << alignment.scanned_records
               << " order_scanned_bytes="
               << alignment.order_scanned_bytes
               << " transaction_scanned_bytes="
               << alignment.transaction_scanned_bytes
               << " order_partial="
               << (order_mandatory_partial ? "true" : "false")
               << " transaction_partial="
               << (transaction_mandatory_partial ? "true" : "false");
        const std::filesystem::path& error_file =
            first_pending != nullptr
                ? *first_pending->file
                : (order_mandatory_partial
                       ? orders.input().path
                       : transactions.input().path);
        const std::uint64_t error_line =
            first_pending != nullptr
                ? first_pending->line
                : (order_mandatory_partial
                       ? orders.incomplete_line()
                       : transactions.incomplete_line());
        return state->Fail(
            StartupReplayErrorV1::kBoundaryAlignmentTimeout,
            error_file,
            error_line,
            detail.str());
    };
    const auto closes_gap_or_is_invalid =
        [&](const ShenzhenTickCandidate& candidate) {
            const auto found = channels.find(candidate.channel);
            if (found == channels.end()) {
                return false;
            }
            return candidate.application_sequence <
                       found->second.next_sequence ||
                   found->second.messages.find(
                       candidate.application_sequence) !=
                       found->second.messages.end() ||
                   (candidate.application_sequence ==
                        found->second.next_sequence &&
                    !found->second.messages.empty());
        };
    const auto file_can_still_contain_missing =
        [&](const std::map<std::uint32_t, std::uint64_t>&
                previous_native_sequence) {
            for (const auto& [channel, channel_state] : channels) {
                if (channel_state.messages.empty()) {
                    continue;
                }
                const auto previous =
                    previous_native_sequence.find(channel);
                if (previous == previous_native_sequence.end() ||
                    previous->second < channel_state.next_sequence) {
                    return true;
                }
            }
            return false;
        };
    const auto alignment_scan_exhausted = [&]() noexcept {
        if (alignment.scanned_records >
            state->config.maximum_boundary_alignment_records) {
            return true;
        }
        return alignment.order_scanned_bytes >
                   state->config.maximum_boundary_alignment_bytes ||
               alignment.transaction_scanned_bytes >
                   state->config.maximum_boundary_alignment_bytes -
                       std::min(
                           alignment.order_scanned_bytes,
                           state->config
                               .maximum_boundary_alignment_bytes);
    };

    for (;;) {
        for (;;) {
            if (aligning &&
                state->config.boundary_alignment_timeout_ms != 0U &&
                AlignmentClock::now() >= alignment_deadline) {
                return fail_alignment_timeout();
            }
            const bool order_initial_work =
                !aligning &&
                orders.next_record_offset() <
                    alignment.initial_order_cut;
            const bool transaction_initial_work =
                !aligning &&
                transactions.next_record_offset() <
                    alignment.initial_transaction_cut;
            const bool order_alignment_work =
                aligning &&
                (order_mandatory_partial ||
                 file_can_still_contain_missing(
                     previous_order_native_sequence));
            const bool transaction_alignment_work =
                aligning &&
                (transaction_mandatory_partial ||
                 file_can_still_contain_missing(
                     previous_transaction_native_sequence));
            const bool existing_order_candidate_is_decisive =
                aligning && order_candidate.has_value() &&
                (order_candidate->completes_mandatory_partial ||
                 closes_gap_or_is_invalid(*order_candidate));
            const bool existing_transaction_candidate_is_decisive =
                aligning && transaction_candidate.has_value() &&
                (transaction_candidate
                     ->completes_mandatory_partial ||
                 closes_gap_or_is_invalid(
                     *transaction_candidate));
            const bool have_existing_decisive_candidate =
                existing_order_candidate_is_decisive ||
                existing_transaction_candidate_is_decisive;
            const bool load_order =
                !order_candidate.has_value() && !orders_done &&
                !have_existing_decisive_candidate &&
                (order_initial_work || order_alignment_work);
            const bool load_transaction =
                !transaction_candidate.has_value() &&
                !transactions_done &&
                !have_existing_decisive_candidate &&
                (transaction_initial_work ||
                 transaction_alignment_work);
            const auto remaining_scan_bytes = [&]() {
                update_alignment_scan();
                if (alignment_scan_exhausted()) {
                    return std::size_t{0U};
                }
                const std::uint64_t used =
                    alignment.order_scanned_bytes +
                    alignment.transaction_scanned_bytes;
                const std::uint64_t remaining =
                    state->config.maximum_boundary_alignment_bytes -
                    used;
                return remaining >
                               static_cast<std::uint64_t>(
                                   std::numeric_limits<
                                       std::size_t>::max())
                           ? std::numeric_limits<std::size_t>::max()
                           : static_cast<std::size_t>(remaining);
            };
            const auto fail_scan_resource = [&] (
                                                const CsvTable& table) {
                update_alignment_scan();
                std::ostringstream detail;
                detail << "Shenzhen boundary alignment scanned "
                       << alignment.scanned_records
                       << " records and "
                       << alignment.order_scanned_bytes
                       << "+"
                       << alignment.transaction_scanned_bytes
                       << " bytes";
                return state->Fail(
                    StartupReplayErrorV1::kResourceExhausted,
                    table.input().path,
                    table.incomplete_line(),
                    detail.str());
            };
            const auto load_scan_budget = [&]() {
                return aligning
                           ? remaining_scan_bytes()
                           : std::numeric_limits<std::size_t>::max();
            };
            if (load_order) {
                if (aligning && orders.has_available_bytes() &&
                    (alignment.scanned_records >=
                         state->config
                             .maximum_boundary_alignment_records ||
                     remaining_scan_bytes() == 0U)) {
                    return fail_scan_resource(orders);
                }
                if (!LoadShenzhenOrderCandidate(
                        &orders,
                        publish_orders,
                        order_mandatory_partial,
                        load_scan_budget(),
                        &order_candidate,
                        &orders_done,
                        &previous_order_recovery_sequence,
                        &previous_order_native_sequence,
                        state)) {
                    update_alignment_scan();
                    if (aligning &&
                        orders.checkpoint_deadline_expired()) {
                        return fail_alignment_timeout();
                    }
                    return false;
                }
                if (aligning) {
                    update_alignment_scan();
                    if (alignment_scan_exhausted()) {
                        return fail_scan_resource(orders);
                    }
                }
            }
            const bool order_candidate_is_decisive =
                aligning && order_candidate.has_value() &&
                (order_candidate->completes_mandatory_partial ||
                 closes_gap_or_is_invalid(*order_candidate));
            const bool load_transaction_now =
                load_transaction && !order_candidate_is_decisive;
            if (load_transaction_now) {
                if (aligning && transactions.has_available_bytes() &&
                    (alignment.scanned_records >=
                         state->config
                             .maximum_boundary_alignment_records ||
                     remaining_scan_bytes() == 0U)) {
                    return fail_scan_resource(transactions);
                }
                if (!LoadShenzhenTransactionCandidate(
                        &transactions,
                        publish_transactions,
                        transaction_mandatory_partial,
                        load_scan_budget(),
                        &transaction_candidate,
                        &transactions_done,
                        &previous_transaction_recovery_sequence,
                        &previous_transaction_native_sequence,
                        state)) {
                    update_alignment_scan();
                    if (aligning &&
                        transactions
                            .checkpoint_deadline_expired()) {
                        return fail_alignment_timeout();
                    }
                    return false;
                }
                if (aligning) {
                    update_alignment_scan();
                    if (alignment_scan_exhausted()) {
                        return fail_scan_resource(transactions);
                    }
                }
            }
            if (!order_candidate.has_value() &&
                !transaction_candidate.has_value()) {
                break;
            }
            const bool order_closes_or_is_invalid =
                order_candidate.has_value() &&
                closes_gap_or_is_invalid(*order_candidate);
            const bool transaction_closes_or_is_invalid =
                transaction_candidate.has_value() &&
                closes_gap_or_is_invalid(*transaction_candidate);
            const bool order_required =
                order_candidate.has_value() &&
                ((!aligning &&
                 order_candidate->record_start_offset <
                      alignment.initial_order_cut) ||
                 (aligning &&
                  (order_candidate->completes_mandatory_partial ||
                   order_closes_or_is_invalid ||
                   file_can_still_contain_missing(
                       previous_order_native_sequence))));
            const bool transaction_required =
                transaction_candidate.has_value() &&
                ((!aligning &&
                  transaction_candidate->record_start_offset <
                      alignment.initial_transaction_cut) ||
                 (aligning &&
                  (transaction_candidate
                       ->completes_mandatory_partial ||
                   transaction_closes_or_is_invalid ||
                   file_can_still_contain_missing(
                       previous_transaction_native_sequence))));
            if (!order_required && !transaction_required) {
                // Both retained files have already advanced past every
                // current missing native sequence. Keep their parsed heads
                // provisional and wait for the opposite writer instead of
                // moving the sealed CSV seam along a fast live tail.
                break;
            }
            bool take_order = false;
            if (order_closes_or_is_invalid) {
                take_order = true;
            } else if (transaction_closes_or_is_invalid) {
                take_order = false;
            } else if (order_candidate.has_value() &&
                       order_candidate
                           ->completes_mandatory_partial) {
                take_order = true;
            } else if (transaction_candidate.has_value() &&
                       transaction_candidate
                           ->completes_mandatory_partial) {
                take_order = false;
            } else if (order_required && !transaction_required) {
                take_order = true;
            } else if (!order_required && transaction_required) {
                take_order = false;
            } else {
                const std::uint64_t order_gap =
                    order_candidate->application_sequence -
                    ShenzhenExpectedSequence(
                        channels, order_candidate->channel);
                const std::uint64_t transaction_gap =
                    transaction_candidate->application_sequence -
                    ShenzhenExpectedSequence(
                        channels,
                        transaction_candidate->channel);
                take_order = order_gap <= transaction_gap;
            }

            ShenzhenTickCandidate selected =
                take_order
                    ? std::move(*order_candidate)
                    : std::move(*transaction_candidate);
            if (take_order) {
                order_candidate.reset();
            } else {
                transaction_candidate.reset();
            }
            if (selected.record_end_offset <
                selected.record_start_offset) {
                return state->Fail(
                    StartupReplayErrorV1::kUnexpectedFailure,
                    *selected.pending.file,
                    selected.pending.line,
                    "Shenzhen candidate has an invalid byte range");
            }
            const std::uint64_t initial_cut =
                take_order
                    ? alignment.initial_order_cut
                    : alignment.initial_transaction_cut;
            const std::uint64_t extension_bytes =
                selected.record_end_offset <= initial_cut
                    ? 0U
                    : selected.record_end_offset -
                          std::max(
                              selected.record_start_offset,
                              initial_cut);
            if (aligning) {
                const std::uint64_t used_bytes =
                    alignment.order_extension_bytes;
                if (alignment.transaction_extension_bytes >
                        state->config.maximum_boundary_alignment_bytes ||
                    used_bytes >
                        state->config.maximum_boundary_alignment_bytes -
                            alignment.transaction_extension_bytes ||
                    extension_bytes >
                        state->config.maximum_boundary_alignment_bytes -
                            used_bytes -
                            alignment.transaction_extension_bytes ||
                    alignment.extension_records >=
                        state->config.maximum_boundary_alignment_records) {
                    return state->Fail(
                        StartupReplayErrorV1::kResourceExhausted,
                        *selected.pending.file,
                        selected.pending.line,
                        "Shenzhen boundary alignment exceeds its "
                        "record/byte limit");
                }
                if (state->config.boundary_alignment_timeout_ms != 0U &&
                    AlignmentClock::now() >= alignment_deadline) {
                    return fail_alignment_timeout();
                }
            }
            const std::chrono::steady_clock::time_point*
                insert_deadline =
                    aligning &&
                            state->config
                                    .boundary_alignment_timeout_ms !=
                                0U
                        ? &alignment_deadline
                        : nullptr;
            const InsertShenzhenPendingResult insert_result =
                InsertShenzhenPending(
                    selected.channel,
                    selected.application_sequence,
                    std::move(selected.pending),
                    &channels,
                    &pending_count,
                    &pending_bytes,
                    insert_deadline,
                    state);
            if (insert_result ==
                InsertShenzhenPendingResult::kDeadlineExpired) {
                return fail_alignment_timeout();
            }
            if (insert_result ==
                InsertShenzhenPendingResult::kFailed) {
                return false;
            }
            if (take_order) {
                alignment.sealed_order_cut =
                    selected.record_end_offset;
                if (selected.completes_mandatory_partial) {
                    order_mandatory_partial = false;
                }
                if (aligning) {
                    alignment.order_extension_bytes +=
                        extension_bytes;
                }
            } else {
                alignment.sealed_transaction_cut =
                    selected.record_end_offset;
                if (selected.completes_mandatory_partial) {
                    transaction_mandatory_partial = false;
                }
                if (aligning) {
                    alignment.transaction_extension_bytes +=
                        extension_bytes;
                }
            }
            if (aligning) {
                ++alignment.extension_records;
                update_alignment_observation();
                if (state->config.boundary_alignment_timeout_ms != 0U &&
                    AlignmentClock::now() >= alignment_deadline) {
                    return fail_alignment_timeout();
                }
                if (alignment_closed()) {
                    update_waited();
                    update_alignment_scan();
                    alignment.phase =
                        StartupReplayBoundaryAlignmentPhaseV1::
                            kSealed;
                    alignment.sealed = true;
                    state->sink.ObserveBoundaryAlignment(alignment);
                    return true;
                }
            }
        }

        if (aligning && !alignment_closed()) {
            update_alignment_scan();
            const bool record_budget_spent =
                alignment.scanned_records >=
                state->config.maximum_boundary_alignment_records;
            const bool byte_budget_spent =
                alignment.order_scanned_bytes >=
                    state->config.maximum_boundary_alignment_bytes ||
                alignment.transaction_scanned_bytes >=
                    state->config.maximum_boundary_alignment_bytes -
                        std::min(
                            alignment.order_scanned_bytes,
                            state->config
                                .maximum_boundary_alignment_bytes);
            if (record_budget_spent || byte_budget_spent) {
                std::ostringstream detail;
                detail << "Shenzhen boundary alignment exhausted its "
                          "scan budget with "
                       << alignment.scanned_records
                       << " records and "
                       << alignment.order_scanned_bytes
                       << "+"
                       << alignment.transaction_scanned_bytes
                       << " bytes before closure";
                return state->Fail(
                    StartupReplayErrorV1::kResourceExhausted,
                    order_mandatory_partial
                        ? orders.input().path
                        : transactions.input().path,
                    order_mandatory_partial
                        ? orders.incomplete_line()
                        : transactions.incomplete_line(),
                    detail.str());
            }
        }

        if (!aligning) {
            order_mandatory_partial =
                orders.has_incomplete_suffix() &&
                orders.incomplete_offset() <
                    alignment.initial_order_cut;
            transaction_mandatory_partial =
                transactions.has_incomplete_suffix() &&
                transactions.incomplete_offset() <
                    alignment.initial_transaction_cut;
            if (alignment_closed()) {
                alignment.phase =
                    StartupReplayBoundaryAlignmentPhaseV1::kSealed;
                alignment.sealed = true;
                state->sink.ObserveBoundaryAlignment(alignment);
                return true;
            }
            aligning = true;
            alignment.attempted = true;
            alignment.phase =
                StartupReplayBoundaryAlignmentPhaseV1::
                    kTailAligning;
            alignment_started = AlignmentClock::now();
            alignment_deadline =
                alignment_started + std::chrono::milliseconds(
                    state->config.boundary_alignment_timeout_ms);
            const std::optional<AlignmentClock::time_point>
                cooperative_deadline =
                    state->config
                                .boundary_alignment_timeout_ms != 0U
                        ? std::optional<AlignmentClock::time_point>(
                              alignment_deadline)
                        : std::nullopt;
            orders.SetCooperativeDeadline(cooperative_deadline);
            transactions.SetCooperativeDeadline(
                cooperative_deadline);
            orders.BeginScanAccounting();
            transactions.BeginScanAccounting();
            update_alignment_observation();
            state->sink.ObserveBoundaryAlignment(alignment);
            // Header completion may already have exposed data bytes beyond an
            // initial cut. Re-enter selection once so only rows needed for an
            // existing initial obligation are considered before polling.
            continue;
        }
        if (alignment_closed()) {
            update_waited();
            update_alignment_scan();
            alignment.phase =
                StartupReplayBoundaryAlignmentPhaseV1::kSealed;
            alignment.sealed = true;
            state->sink.ObserveBoundaryAlignment(alignment);
            return true;
        }

        if (!first_alignment_poll) {
            if (state->config.boundary_alignment_timeout_ms == 0U ||
                AlignmentClock::now() >= alignment_deadline) {
                return fail_alignment_timeout();
            }
            const auto remaining = alignment_deadline -
                                   AlignmentClock::now();
            if (remaining <= AlignmentClock::duration::zero()) {
                return fail_alignment_timeout();
            }
            const auto sleep_duration = std::min(
                std::chrono::duration_cast<AlignmentClock::duration>(
                    alignment_poll_delay),
                remaining);
            std::this_thread::sleep_for(sleep_duration);
            const std::uint64_t doubled_poll_ms =
                static_cast<std::uint64_t>(
                    alignment_poll_delay.count()) * 2U;
            alignment_poll_delay = std::chrono::milliseconds(
                std::min(
                    doubled_poll_ms,
                    state->config
                        .boundary_alignment_maximum_poll_ms));
            if (state->config.boundary_alignment_timeout_ms != 0U &&
                AlignmentClock::now() >= alignment_deadline) {
                return fail_alignment_timeout();
            }
        }
        const std::filesystem::path& checkpoint_file =
            order_mandatory_partial
                ? orders.input().path
                : transactions.input().path;
        const std::uint64_t checkpoint_line =
            order_mandatory_partial
                ? orders.incomplete_line()
                : transactions.incomplete_line();
        if (state->config.boundary_alignment_timeout_ms != 0U) {
            const StartupReplaySinkCallResultV1 checkpoint =
                state->CooperativeCheckpointUntil(
                    checkpoint_file,
                    checkpoint_line,
                    alignment_deadline);
            if (checkpoint ==
                StartupReplaySinkCallResultV1::kDeadline) {
                return fail_alignment_timeout();
            }
            if (checkpoint ==
                StartupReplaySinkCallResultV1::kRejected) {
                return false;
            }
        } else if (!state->CooperativeCheckpoint(
                       checkpoint_file, checkpoint_line)) {
            return false;
        }
        if (state->config.boundary_alignment_timeout_ms != 0U &&
            AlignmentClock::now() >= alignment_deadline) {
            return fail_alignment_timeout();
        }

        update_waited();
        update_alignment_observation();
        ++alignment.poll_count;
        state->sink.ObserveBoundaryAlignment(alignment);
        if (state->config.boundary_alignment_timeout_ms != 0U &&
            AlignmentClock::now() >= alignment_deadline) {
            return fail_alignment_timeout();
        }
        bool orders_extended = false;
        bool transactions_extended = false;
        if (!orders.RefreshAvailableEnd(&orders_extended)) {
            return false;
        }
        update_alignment_observation();
        if (!transactions.RefreshAvailableEnd(
                &transactions_extended)) {
            return false;
        }
        update_alignment_observation();
        if (state->config.boundary_alignment_timeout_ms != 0U &&
            AlignmentClock::now() >= alignment_deadline) {
            return fail_alignment_timeout();
        }
        first_alignment_poll = false;
        orders_done = !orders_extended;
        transactions_done = !transactions_extended;
        if (orders_extended || transactions_extended) {
            continue;
        }
    }
}

struct CapturedInputs final {
    std::optional<InputFile> shanghai_snapshot;
    std::optional<InputFile> shanghai_queue;
    std::optional<InputFile> shanghai_tick;
    std::optional<InputFile> shenzhen_snapshot;
    std::optional<InputFile> shenzhen_ask_queue;
    std::optional<InputFile> shenzhen_bid_queue;
    std::optional<InputFile> shenzhen_order;
    std::optional<InputFile> shenzhen_transaction;
};

struct ResolvedInput final {
    std::filesystem::path path;
};

bool ResolveInput(
    const std::filesystem::path& directory,
    std::span<const std::string_view> aliases,
    ResolvedInput* output,
    ReplayState* state) {
    std::optional<std::filesystem::path> selected;
    for (const std::string_view alias : aliases) {
        const std::filesystem::path candidate =
            directory / std::string(alias);
        std::error_code error;
        const bool exists = std::filesystem::exists(candidate, error);
        if (error) {
            return state->Fail(
                StartupReplayErrorV1::kFileStat,
                candidate,
                0U,
                "cannot stat CSV path: " + error.message());
        }
        if (!exists) {
            continue;
        }
        if (selected.has_value()) {
            return state->Fail(
                StartupReplayErrorV1::kAliasConflict,
                candidate,
                0U,
                "more than one alias exists for the same CSV table");
        }
        selected = candidate;
    }
    if (!selected.has_value()) {
        return state->Fail(
            StartupReplayErrorV1::kFileMissing,
            directory / std::string(aliases.front()),
            0U,
            "required CSV file is missing");
    }
    output->path = std::move(*selected);
    return true;
}

bool CaptureResolvedInput(
    const ResolvedInput& resolved,
    InputFile* output,
    ReplayState* state) {
#if defined(__linux__)
    int flags = O_RDONLY | O_CLOEXEC | O_NONBLOCK;
#if defined(O_NOFOLLOW)
    flags |= O_NOFOLLOW;
#endif
    const int descriptor =
        ::open(resolved.path.c_str(), flags);
    if (descriptor < 0) {
        return state->Fail(
            StartupReplayErrorV1::kFileOpen,
            resolved.path,
            0U,
            "cannot open CSV with a stable descriptor: " +
                std::string(std::strerror(errno)));
    }
    std::shared_ptr<StableInputFile> stable;
    try {
        stable = std::make_shared<StableInputFile>(descriptor);
    } catch (...) {
        static_cast<void>(::close(descriptor));
        throw;
    }
    std::uint64_t bytes = 0U;
    std::string detail;
    if (!stable->CurrentSize(&bytes, &detail)) {
        return state->Fail(
            StartupReplayErrorV1::kFileStat,
            resolved.path,
            0U,
            detail.empty()
                ? "cannot fstat opened CSV descriptor"
                : std::move(detail));
    }
    if (bytes > state->config.maximum_file_bytes) {
        return state->Fail(
            StartupReplayErrorV1::kFileTooLarge,
            resolved.path,
            0U,
            "CSV byte prefix exceeds maximum_file_bytes");
    }
    output->path = resolved.path;
    output->prefix_bytes = bytes;
    output->stable = std::move(stable);
    return true;
#else
    static_cast<void>(output);
    return state->Fail(
        StartupReplayErrorV1::kInvalidConfiguration,
        resolved.path,
        0U,
        "stable CSV descriptors require Linux in replay v1");
#endif
}

bool CaptureFence(
    l2flow::sdk::MessageKey key,
    const ResolvedInput& root,
    ReplayState* state) {
    std::string detail;
    if (state->sink.CaptureTupleFence(key, &detail)) {
        return true;
    }
    if (detail.empty()) {
        detail = "startup replay sink rejected tuple fence";
    }
    return state->Fail(
        StartupReplayErrorV1::kSinkRejected,
        root.path,
        0U,
        std::move(detail));
}

bool CaptureInputs(
    CapturedInputs* inputs,
    ReplayState* state) {
    const StartupReplayMessageSetV1 enabled =
        state->config.enabled_messages;
    const auto resolve = [&](std::optional<ResolvedInput>* destination,
                             std::initializer_list<std::string_view>
                                 aliases) {
        ResolvedInput input;
        const std::span<const std::string_view> alias_span(
            aliases.begin(), aliases.size());
        if (!ResolveInput(
                state->config.directory,
                alias_span,
                &input,
                state)) {
            return false;
        }
        *destination = std::move(input);
        return true;
    };
    std::optional<ResolvedInput> shanghai_snapshot;
    std::optional<ResolvedInput> shanghai_queue;
    std::optional<ResolvedInput> shanghai_tick;
    std::optional<ResolvedInput> shenzhen_snapshot;
    std::optional<ResolvedInput> shenzhen_ask_queue;
    std::optional<ResolvedInput> shenzhen_bid_queue;
    std::optional<ResolvedInput> shenzhen_order;
    std::optional<ResolvedInput> shenzhen_transaction;

    if (StartupReplayContainsV1(
            enabled,
            StartupReplayMessageSetV1::kShanghaiSnapshot) &&
        (!resolve(
             &shanghai_snapshot,
             {"mdl_4_4_0.csv", "MarketData.csv"}) ||
         !resolve(
             &shanghai_queue,
             {"mdl_4_4_1.csv", "OrderQueue.csv"}))) {
        return false;
    }
    if (StartupReplayContainsV1(
            enabled,
            StartupReplayMessageSetV1::kShanghaiTick) &&
        !resolve(
            &shanghai_tick,
            {"mdl_4_24_0.csv"})) {
        return false;
    }
    if (StartupReplayContainsV1(
            enabled,
            StartupReplayMessageSetV1::kShenzhenSnapshot) &&
        (!resolve(
             &shenzhen_snapshot,
             {"mdl_6_28_0.csv"}) ||
         !resolve(
             &shenzhen_ask_queue,
             {"mdl_6_28_1.csv"}) ||
         !resolve(
             &shenzhen_bid_queue,
             {"mdl_6_28_2.csv"}))) {
        return false;
    }
    const bool need_shenzhen_ticks =
        StartupReplayContainsV1(
            enabled,
            StartupReplayMessageSetV1::kShenzhenOrder) ||
        StartupReplayContainsV1(
            enabled,
            StartupReplayMessageSetV1::kShenzhenTransaction);
    if (need_shenzhen_ticks &&
        (!resolve(
             &shenzhen_order,
             {"mdl_6_33_0.csv"}) ||
         !resolve(
             &shenzhen_transaction,
             {"mdl_6_36_0.csv"}))) {
        return false;
    }

    const auto capture =
        [&](const ResolvedInput& resolved,
            std::optional<InputFile>* destination) {
            InputFile input;
            if (!CaptureResolvedInput(
                    resolved, &input, state)) {
                return false;
            }
            *destination = std::move(input);
            return true;
        };
    if (shanghai_snapshot.has_value() &&
        (!CaptureFence(
             {4U, kServiceVersion, 4U},
             *shanghai_snapshot,
             state) ||
         !capture(
             *shanghai_queue,
             &inputs->shanghai_queue) ||
         !capture(
             *shanghai_snapshot,
             &inputs->shanghai_snapshot))) {
        return false;
    }
    if (shanghai_tick.has_value() &&
        (!CaptureFence(
             {4U, kServiceVersion, 24U},
             *shanghai_tick,
             state) ||
         !capture(
             *shanghai_tick,
             &inputs->shanghai_tick))) {
        return false;
    }
    if (shenzhen_snapshot.has_value() &&
        (!CaptureFence(
             {6U, kServiceVersion, 28U},
             *shenzhen_snapshot,
             state) ||
         !capture(
             *shenzhen_ask_queue,
             &inputs->shenzhen_ask_queue) ||
         !capture(
             *shenzhen_bid_queue,
             &inputs->shenzhen_bid_queue) ||
         !capture(
             *shenzhen_snapshot,
             &inputs->shenzhen_snapshot))) {
        return false;
    }
    if (shenzhen_order.has_value() &&
        (!CaptureFence(
             {6U, kServiceVersion, 33U},
             *shenzhen_order,
             state) ||
         !CaptureFence(
             {6U, kServiceVersion, 36U},
             *shenzhen_order,
             state) ||
         !capture(
             *shenzhen_order,
             &inputs->shenzhen_order) ||
         !capture(
             *shenzhen_transaction,
             &inputs->shenzhen_transaction))) {
        return false;
    }
    return true;
}

}  // namespace

std::string_view StartupReplayErrorNameV1(
    StartupReplayErrorV1 error) noexcept {
    switch (error) {
        case StartupReplayErrorV1::kNone:
            return "none";
        case StartupReplayErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case StartupReplayErrorV1::kFileMissing:
            return "file_missing";
        case StartupReplayErrorV1::kAliasConflict:
            return "alias_conflict";
        case StartupReplayErrorV1::kFileOpen:
            return "file_open";
        case StartupReplayErrorV1::kFileStat:
            return "file_stat";
        case StartupReplayErrorV1::kFileTooLarge:
            return "file_too_large";
        case StartupReplayErrorV1::kIo:
            return "io";
        case StartupReplayErrorV1::kUtf8Invalid:
            return "utf8_invalid";
        case StartupReplayErrorV1::kCsvMalformed:
            return "csv_malformed";
        case StartupReplayErrorV1::kIncompleteBoundary:
            return "incomplete_boundary";
        case StartupReplayErrorV1::kLineTooLong:
            return "line_too_long";
        case StartupReplayErrorV1::kHeaderMissing:
            return "header_missing";
        case StartupReplayErrorV1::kDuplicateHeader:
            return "duplicate_header";
        case StartupReplayErrorV1::kSchemaMismatch:
            return "schema_mismatch";
        case StartupReplayErrorV1::kColumnCountMismatch:
            return "column_count_mismatch";
        case StartupReplayErrorV1::kValueInvalid:
            return "value_invalid";
        case StartupReplayErrorV1::kNumericOverflow:
            return "numeric_overflow";
        case StartupReplayErrorV1::kTimeInvalid:
            return "time_invalid";
        case StartupReplayErrorV1::kJoinDuplicate:
            return "join_duplicate";
        case StartupReplayErrorV1::kJoinMismatch:
            return "join_mismatch";
        case StartupReplayErrorV1::kJoinOrphan:
            return "join_orphan";
        case StartupReplayErrorV1::kSequenceDuplicate:
            return "sequence_duplicate";
        case StartupReplayErrorV1::kSequenceGap:
            return "sequence_gap";
        case StartupReplayErrorV1::kMessageTooLarge:
            return "message_too_large";
        case StartupReplayErrorV1::kSinkRejected:
            return "sink_rejected";
        case StartupReplayErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case StartupReplayErrorV1::kUnexpectedFailure:
            return "unexpected_failure";
        case StartupReplayErrorV1::kBoundaryAlignmentTimeout:
            return "boundary_alignment_timeout";
    }
    return "unexpected_failure";
}

std::string_view StartupReplayBoundaryAlignmentPhaseNameV1(
    StartupReplayBoundaryAlignmentPhaseV1 phase) noexcept {
    switch (phase) {
        case StartupReplayBoundaryAlignmentPhaseV1::kNotApplicable:
            return "NOT_APPLICABLE";
        case StartupReplayBoundaryAlignmentPhaseV1::kInitialReplay:
            return "INITIAL_REPLAY";
        case StartupReplayBoundaryAlignmentPhaseV1::kTailAligning:
            return "TAIL_ALIGNING";
        case StartupReplayBoundaryAlignmentPhaseV1::kSealed:
            return "SEALED";
    }
    return "NOT_APPLICABLE";
}

MdlCsvStartupReplaySourceV1::MdlCsvStartupReplaySourceV1(
    StartupReplayConfigV1 config)
    : config_(std::move(config)) {}

MdlCsvStartupReplaySourceV1::~MdlCsvStartupReplaySourceV1() = default;

StartupReplayResultV1 MdlCsvStartupReplaySourceV1::Replay(
    StartupReplaySinkV1& sink) noexcept {
    ReplayState state(config_, sink);
    try {
        const std::uint32_t enabled_bits =
            static_cast<std::uint32_t>(config_.enabled_messages);
        const std::uint32_t all_bits =
            static_cast<std::uint32_t>(
                StartupReplayMessageSetV1::kAll);
        if (config_.directory.empty() ||
            (enabled_bits & ~all_bits) != 0U ||
            config_.maximum_file_bytes == 0U ||
            config_.maximum_record_bytes == 0U ||
            config_.maximum_message_bytes <
                sizeof(mdl::MDLMessageHead) +
                    sizeof(sh::SHL2MarketData) ||
            config_.maximum_pending_messages == 0U ||
            config_.maximum_pending_bytes == 0U ||
            config_.boundary_alignment_timeout_ms > 86'400'000U ||
            config_.boundary_alignment_initial_poll_ms == 0U ||
            config_.boundary_alignment_initial_poll_ms > 60'000U ||
            config_.boundary_alignment_maximum_poll_ms <
                config_.boundary_alignment_initial_poll_ms ||
            config_.boundary_alignment_maximum_poll_ms > 60'000U ||
            config_.maximum_boundary_alignment_records == 0U ||
            config_.maximum_boundary_alignment_bytes == 0U) {
            static_cast<void>(state.Fail(
                StartupReplayErrorV1::kInvalidConfiguration,
                config_.directory,
                0U,
                "startup replay configuration is invalid"));
            return std::move(state.result);
        }

        CapturedInputs inputs;
        if (!CaptureInputs(&inputs, &state)) {
            return std::move(state.result);
        }
        if (inputs.shanghai_snapshot.has_value() &&
            !ReplayShanghaiSnapshots(
                *inputs.shanghai_snapshot,
                *inputs.shanghai_queue,
                &state)) {
            return std::move(state.result);
        }
        if (inputs.shanghai_tick.has_value() &&
            !ReplayShanghaiTicks(
                *inputs.shanghai_tick, &state)) {
            return std::move(state.result);
        }
        if (inputs.shenzhen_snapshot.has_value() &&
            !ReplayShenzhenSnapshots(
                *inputs.shenzhen_snapshot,
                *inputs.shenzhen_ask_queue,
                *inputs.shenzhen_bid_queue,
                &state)) {
            return std::move(state.result);
        }
        if (inputs.shenzhen_order.has_value()) {
            const bool publish_orders = StartupReplayContainsV1(
                config_.enabled_messages,
                StartupReplayMessageSetV1::kShenzhenOrder);
            const bool publish_transactions =
                StartupReplayContainsV1(
                    config_.enabled_messages,
                    StartupReplayMessageSetV1::
                        kShenzhenTransaction);
            if (!ReplayShenzhenTicks(
                    *inputs.shenzhen_order,
                    *inputs.shenzhen_transaction,
                    publish_orders,
                    publish_transactions,
                    &state)) {
                return std::move(state.result);
            }
        }
        return std::move(state.result);
    } catch (const std::bad_alloc&) {
        static_cast<void>(state.Fail(
            StartupReplayErrorV1::kResourceExhausted,
            {},
            0U,
            "allocation failed during CSV startup replay"));
        return std::move(state.result);
    } catch (const std::exception& exception) {
        static_cast<void>(state.Fail(
            StartupReplayErrorV1::kUnexpectedFailure,
            {},
            0U,
            exception.what()));
        return std::move(state.result);
    } catch (...) {
        static_cast<void>(state.Fail(
            StartupReplayErrorV1::kUnexpectedFailure,
            {},
            0U,
            "unknown exception during CSV startup replay"));
        return std::move(state.result);
    }
}

}  // namespace l2flow::recovery
