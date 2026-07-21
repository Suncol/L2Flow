#include "l2flow/sdk/endpoint_contract.h"

#include "l2flow/common/sha256.h"

#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace l2flow::sdk {
namespace {

class FileDescriptor {
public:
    explicit FileDescriptor(int value) noexcept : value_(value) {}
    ~FileDescriptor() {
        if (value_ >= 0) {
            static_cast<void>(::close(value_));
        }
    }

    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;

    [[nodiscard]] int get() const noexcept { return value_; }

private:
    int value_;
};

void SetError(std::string* error, std::string message) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        *error = std::move(message);
    } catch (...) {
        // A failed diagnostic allocation must not escape this preflight.
    }
}

void ClearError(std::string* error) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        error->clear();
    } catch (...) {
        // std::string::clear is normally non-throwing; preserve noexcept.
    }
}

std::string ErrnoMessage(const char* operation, int error_number) {
    return std::string(operation) + " failed: " +
           std::strerror(error_number);
}

bool IsLowercaseSha256(std::string_view text) noexcept {
    if (text.size() != 64U) {
        return false;
    }
    for (const char character : text) {
        const bool decimal = character >= '0' && character <= '9';
        const bool lowercase_hex = character >= 'a' && character <= 'f';
        if (!decimal && !lowercase_hex) {
            return false;
        }
    }
    return true;
}

bool IsKnownIngressKind(IngressKind kind) noexcept {
    switch (kind) {
    case IngressKind::ShSnapshot:
    case IngressKind::ShTick:
    case IngressKind::SzSnapshot:
    case IngressKind::SzTick:
        return true;
    }
    return false;
}

bool ReadStableFile(const std::string& path,
                    std::string* contents,
                    std::string* error) {
    if (path.empty() || path.find('\0') != std::string::npos) {
        *error = "endpoint contract path is empty or contains NUL";
        return false;
    }

    static_assert(O_CLOEXEC != 0);
    static_assert(O_NOFOLLOW != 0);
    static_assert(O_NONBLOCK != 0);
    int raw_fd = -1;
    do {
        raw_fd = ::open(
            path.c_str(),
            O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK | O_NOCTTY);
    } while (raw_fd < 0 && errno == EINTR);
    if (raw_fd < 0) {
        *error = ErrnoMessage("open endpoint contract", errno);
        return false;
    }
    FileDescriptor fd(raw_fd);

    struct stat before {};
    int stat_result = -1;
    do {
        stat_result = ::fstat(fd.get(), &before);
    } while (stat_result != 0 && errno == EINTR);
    if (stat_result != 0) {
        *error = ErrnoMessage("stat endpoint contract", errno);
        return false;
    }
    if (!S_ISREG(before.st_mode)) {
        *error = "endpoint contract is not a regular file";
        return false;
    }
    if (before.st_size < 0 ||
        static_cast<std::uintmax_t>(before.st_size) >
            static_cast<std::uintmax_t>(kMaximumEndpointContractBytes)) {
        *error = "endpoint contract exceeds 4096 bytes";
        return false;
    }

    std::string bytes(static_cast<std::size_t>(before.st_size), '\0');
    std::size_t offset = 0U;
    while (offset < bytes.size()) {
        const ssize_t count =
            ::read(fd.get(), bytes.data() + offset, bytes.size() - offset);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count < 0) {
            *error = ErrnoMessage("read endpoint contract", errno);
            return false;
        }
        if (count == 0) {
            *error = "endpoint contract was truncated while being read";
            return false;
        }
        offset += static_cast<std::size_t>(count);
    }

    char extra = '\0';
    for (;;) {
        const ssize_t count = ::read(fd.get(), &extra, 1U);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count < 0) {
            *error =
                ErrnoMessage("verify endpoint contract length", errno);
            return false;
        }
        if (count != 0) {
            *error = "endpoint contract grew while being read";
            return false;
        }
        break;
    }

    struct stat after {};
    do {
        stat_result = ::fstat(fd.get(), &after);
    } while (stat_result != 0 && errno == EINTR);
    if (stat_result != 0) {
        *error = ErrnoMessage("restat endpoint contract", errno);
        return false;
    }
    if (after.st_dev != before.st_dev || after.st_ino != before.st_ino ||
        after.st_size != before.st_size) {
        *error = "endpoint contract changed size while being read";
        return false;
    }

    *contents = std::move(bytes);
    return true;
}

class StrictJsonParser {
public:
    explicit StrictJsonParser(std::string_view input) noexcept
        : input_(input) {}

    bool Parse(EndpointContract* contract,
               IngressKind expected_kind,
               std::string* error) {
        SkipWhitespace();
        if (!Consume('{')) {
            return Fail("endpoint contract must be a JSON object", error);
        }
        SkipWhitespace();
        if (Peek('}')) {
            return Fail("endpoint contract object is empty", error);
        }

        ParsedFields fields;
        for (;;) {
            std::string key;
            if (!ParseString(&key, error)) {
                return false;
            }
            SkipWhitespace();
            if (!Consume(':')) {
                return Fail("expected ':' after endpoint contract field",
                            error);
            }
            SkipWhitespace();
            if (!ParseField(key, &fields, error)) {
                return false;
            }
            SkipWhitespace();
            if (Consume('}')) {
                break;
            }
            if (!Consume(',')) {
                return Fail(
                    "expected ',' or '}' after endpoint contract field",
                    error);
            }
            SkipWhitespace();
            if (Peek('}')) {
                return Fail("trailing comma is not permitted", error);
            }
        }

        SkipWhitespace();
        if (position_ != input_.size()) {
            return Fail("trailing content after endpoint contract object",
                        error);
        }
        if (fields.seen != kAllFields) {
            return Fail("endpoint contract is missing a required field",
                        error);
        }

        IngressKind parsed_kind = IngressKind::ShSnapshot;
        if (!ParseIngressKind(fields.ingress_kind, &parsed_kind)) {
            return Fail("endpoint contract ingress_kind is unknown", error);
        }
        if (parsed_kind != expected_kind) {
            return Fail(
                "endpoint contract ingress_kind does not match the service",
                error);
        }
        if (fields.name.empty()) {
            return Fail("endpoint contract name must not be empty", error);
        }
        if (fields.resolved_server_address.empty()) {
            return Fail(
                "endpoint contract resolved_server_address must not be empty",
                error);
        }

        datayes::mdl::MDLMessageEncoding encoding =
            datayes::mdl::MDLEID_UNDEFINED;
        if (!ParseMessageEncoding(fields.message_encoding, &encoding)) {
            return Fail(
                "endpoint contract message_encoding is not approved",
                error);
        }

        EndpointContract parsed;
        parsed.name = std::move(fields.name);
        parsed.resolved_server_address =
            std::move(fields.resolved_server_address);
        parsed.message_encoding = encoding;
        parsed.merge_message = fields.merge_message;
        parsed.send_mac_auth = fields.send_mac_auth;
        parsed.server_select = fields.server_select;
        *contract = std::move(parsed);
        return true;
    }

private:
    enum Field : std::uint16_t {
        kSchemaVersion = 1U << 0U,
        kIngressKind = 1U << 1U,
        kName = 1U << 2U,
        kResolvedServerAddress = 1U << 3U,
        kMessageEncoding = 1U << 4U,
        kMergeMessage = 1U << 5U,
        kSendMacAuth = 1U << 6U,
        kServerSelect = 1U << 7U,
    };

    static constexpr std::uint16_t kAllFields =
        kSchemaVersion | kIngressKind | kName |
        kResolvedServerAddress | kMessageEncoding |
        kMergeMessage | kSendMacAuth | kServerSelect;

    struct ParsedFields {
        std::uint16_t seen = 0U;
        std::string ingress_kind;
        std::string name;
        std::string resolved_server_address;
        std::uint64_t message_encoding = 0U;
        bool merge_message = false;
        bool send_mac_auth = false;
        bool server_select = false;
    };

    void SkipWhitespace() noexcept {
        while (position_ < input_.size()) {
            const char character = input_[position_];
            if (character != ' ' && character != '\t' &&
                character != '\n' && character != '\r') {
                break;
            }
            ++position_;
        }
    }

    [[nodiscard]] bool Peek(char expected) const noexcept {
        return position_ < input_.size() &&
               input_[position_] == expected;
    }

    bool Consume(char expected) noexcept {
        if (!Peek(expected)) {
            return false;
        }
        ++position_;
        return true;
    }

    bool Fail(std::string_view reason, std::string* error) const {
        *error = std::string(reason) + " at byte " +
                 std::to_string(position_);
        return false;
    }

    bool ParseString(std::string* result, std::string* error) {
        if (!Consume('"')) {
            return Fail("expected a JSON string", error);
        }
        const std::size_t begin = position_;
        while (position_ < input_.size()) {
            const unsigned char character =
                static_cast<unsigned char>(input_[position_]);
            if (character == static_cast<unsigned char>('"')) {
                result->assign(
                    input_.substr(begin, position_ - begin));
                ++position_;
                return true;
            }
            if (character == static_cast<unsigned char>('\\')) {
                return Fail(
                    "JSON string escapes are not permitted", error);
            }
            if (character < 0x20U || character == 0x7fU) {
                return Fail(
                    "control characters are not permitted in JSON strings",
                    error);
            }
            if (character < 0x80U) {
                ++position_;
                continue;
            }
            if (!ConsumeUtf8CodePoint(error)) {
                return false;
            }
        }
        return Fail("unterminated JSON string", error);
    }

    bool ConsumeUtf8CodePoint(std::string* error) {
        const auto byte_at = [this](std::size_t offset) {
            return static_cast<unsigned char>(
                input_[position_ + offset]);
        };
        const auto is_continuation = [](unsigned char byte) {
            return byte >= 0x80U && byte <= 0xbfU;
        };
        const std::size_t remaining = input_.size() - position_;
        const unsigned char first = byte_at(0U);

        if (first >= 0xc2U && first <= 0xdfU) {
            if (remaining < 2U || !is_continuation(byte_at(1U))) {
                return Fail("invalid UTF-8 in JSON string", error);
            }
            if (first == 0xc2U && byte_at(1U) <= 0x9fU) {
                return Fail(
                    "control characters are not permitted in JSON strings",
                    error);
            }
            position_ += 2U;
            return true;
        }
        if (first >= 0xe0U && first <= 0xefU) {
            if (remaining < 3U ||
                !is_continuation(byte_at(1U)) ||
                !is_continuation(byte_at(2U))) {
                return Fail("invalid UTF-8 in JSON string", error);
            }
            const unsigned char second = byte_at(1U);
            if ((first == 0xe0U && second < 0xa0U) ||
                (first == 0xedU && second > 0x9fU)) {
                return Fail("invalid UTF-8 in JSON string", error);
            }
            position_ += 3U;
            return true;
        }
        if (first >= 0xf0U && first <= 0xf4U) {
            if (remaining < 4U ||
                !is_continuation(byte_at(1U)) ||
                !is_continuation(byte_at(2U)) ||
                !is_continuation(byte_at(3U))) {
                return Fail("invalid UTF-8 in JSON string", error);
            }
            const unsigned char second = byte_at(1U);
            if ((first == 0xf0U && second < 0x90U) ||
                (first == 0xf4U && second > 0x8fU)) {
                return Fail("invalid UTF-8 in JSON string", error);
            }
            position_ += 4U;
            return true;
        }
        return Fail("invalid UTF-8 in JSON string", error);
    }

    bool ParseUnsigned(std::uint64_t* result, std::string* error) {
        const std::size_t begin = position_;
        if (position_ >= input_.size() ||
            input_[position_] < '0' || input_[position_] > '9') {
            return Fail("expected an unsigned decimal integer", error);
        }
        if (input_[position_] == '0' &&
            position_ + 1U < input_.size() &&
            input_[position_ + 1U] >= '0' &&
            input_[position_ + 1U] <= '9') {
            return Fail("leading zero in JSON integer", error);
        }
        while (position_ < input_.size() &&
               input_[position_] >= '0' &&
               input_[position_] <= '9') {
            ++position_;
        }
        const std::string_view digits =
            input_.substr(begin, position_ - begin);
        std::uint64_t parsed = 0U;
        const auto converted = std::from_chars(
            digits.data(), digits.data() + digits.size(), parsed);
        if (converted.ec == std::errc::result_out_of_range ||
            converted.ptr != digits.data() + digits.size()) {
            return Fail("JSON integer is outside uint64 range", error);
        }
        *result = parsed;
        return true;
    }

    bool ParseBoolean(bool* result, std::string* error) {
        constexpr std::string_view true_text = "true";
        constexpr std::string_view false_text = "false";
        if (input_.substr(position_, true_text.size()) == true_text) {
            position_ += true_text.size();
            *result = true;
            return true;
        }
        if (input_.substr(position_, false_text.size()) == false_text) {
            position_ += false_text.size();
            *result = false;
            return true;
        }
        return Fail("expected a JSON boolean", error);
    }

    bool MarkSeen(Field field,
                  ParsedFields* fields,
                  std::string* error) {
        const auto bit = static_cast<std::uint16_t>(field);
        if ((fields->seen & bit) != 0U) {
            return Fail("duplicate endpoint contract field", error);
        }
        fields->seen = static_cast<std::uint16_t>(fields->seen | bit);
        return true;
    }

    bool ParseField(const std::string& key,
                    ParsedFields* fields,
                    std::string* error) {
        if (key == "schema_version") {
            if (!MarkSeen(kSchemaVersion, fields, error)) {
                return false;
            }
            std::uint64_t version = 0U;
            if (!ParseUnsigned(&version, error)) {
                return false;
            }
            if (version != 1U) {
                return Fail(
                    "unsupported endpoint contract schema_version", error);
            }
            return true;
        }
        if (key == "ingress_kind") {
            return MarkSeen(kIngressKind, fields, error) &&
                   ParseString(&fields->ingress_kind, error);
        }
        if (key == "name") {
            return MarkSeen(kName, fields, error) &&
                   ParseString(&fields->name, error);
        }
        if (key == "resolved_server_address") {
            return MarkSeen(kResolvedServerAddress, fields, error) &&
                   ParseString(
                       &fields->resolved_server_address, error);
        }
        if (key == "message_encoding") {
            return MarkSeen(kMessageEncoding, fields, error) &&
                   ParseUnsigned(&fields->message_encoding, error);
        }
        if (key == "merge_message") {
            return MarkSeen(kMergeMessage, fields, error) &&
                   ParseBoolean(&fields->merge_message, error);
        }
        if (key == "send_mac_auth") {
            return MarkSeen(kSendMacAuth, fields, error) &&
                   ParseBoolean(&fields->send_mac_auth, error);
        }
        if (key == "server_select") {
            return MarkSeen(kServerSelect, fields, error) &&
                   ParseBoolean(&fields->server_select, error);
        }
        return Fail("unknown endpoint contract field", error);
    }

    static bool ParseMessageEncoding(
        std::uint64_t value,
        datayes::mdl::MDLMessageEncoding* result) noexcept {
        switch (value) {
        case 1U:
            *result = datayes::mdl::MDLEID_BINARY;
            return true;
        case 2U:
            *result = datayes::mdl::MDLEID_FAST;
            return true;
        case 3U:
            *result = datayes::mdl::MDLEID_JSON;
            return true;
        case 4U:
            *result = datayes::mdl::MDLEID_PROTOBUF;
            return true;
        case 5U:
            *result = datayes::mdl::MDLEID_CSV;
            return true;
        case 6U:
            *result = datayes::mdl::MDLEID_MKTDATA;
            return true;
        case 7U:
            *result = datayes::mdl::MDLEID_MKTPRO;
            return true;
        case 0x40U:
            *result = datayes::mdl::MDLEID_PACKAGE;
            return true;
        case 0x80U:
            *result = datayes::mdl::MDLEID_DEFLATE;
            return true;
        case 0x84U:
            *result = datayes::mdl::MDLEID_DEFLATE_PROTOBUF;
            return true;
        default:
            return false;
        }
    }

    std::string_view input_;
    std::size_t position_ = 0U;
};

}  // namespace

VerifiedEndpointContract::VerifiedEndpointContract(
    IngressKind ingress_kind,
    EndpointContract contract) noexcept
    : ingress_kind_(ingress_kind),
      contract_(std::move(contract)) {}

EndpointContract VerifiedEndpointContract::CopyValue() const {
    return contract_;
}

std::shared_ptr<const VerifiedEndpointContract>
VerifyEndpointContractBytes(
    std::string_view exact_bytes,
    std::string_view expected_sha256,
    IngressKind expected_kind,
    std::string* error) noexcept {
    if (!IsLowercaseSha256(expected_sha256)) {
        SetError(
            error,
            "expected endpoint contract SHA-256 must be 64 lowercase "
            "hexadecimal digits");
        return nullptr;
    }
    if (!IsKnownIngressKind(expected_kind)) {
        SetError(error, "expected ingress kind is unknown");
        return nullptr;
    }
    if (exact_bytes.size() > kMaximumEndpointContractBytes) {
        SetError(error, "endpoint contract exceeds 4096 bytes");
        return nullptr;
    }

    try {
        const std::string actual_sha256 = common::Sha256Hex(
            common::ComputeSha256(exact_bytes));
        if (actual_sha256 != expected_sha256) {
            SetError(
                error,
                "endpoint contract SHA-256 does not match expected");
            return nullptr;
        }

        EndpointContract parsed;
        std::string local_error;
        StrictJsonParser parser(exact_bytes);
        if (!parser.Parse(
                &parsed, expected_kind, &local_error)) {
            SetError(error, std::move(local_error));
            return nullptr;
        }
        parsed.contract_sha256 = actual_sha256;
        auto verified = std::shared_ptr<
            const VerifiedEndpointContract>(
            new VerifiedEndpointContract(
                expected_kind, std::move(parsed)));
        ClearError(error);
        return verified;
    } catch (const std::exception& exception) {
        try {
            SetError(
                error,
                std::string(
                    "endpoint contract verification failure: ") +
                    exception.what());
        } catch (...) {
            SetError(
                error,
                "endpoint contract verification failure");
        }
        return nullptr;
    } catch (...) {
        SetError(
            error,
            "unknown endpoint contract verification failure");
        return nullptr;
    }
}

std::shared_ptr<const VerifiedEndpointContract>
LoadVerifiedEndpointContractFile(
    const std::string& path,
    std::string_view expected_sha256,
    IngressKind expected_kind,
    std::string* error) noexcept {
    if (!IsLowercaseSha256(expected_sha256)) {
        SetError(
            error,
            "expected endpoint contract SHA-256 must be 64 lowercase "
            "hexadecimal digits");
        return nullptr;
    }
    if (!IsKnownIngressKind(expected_kind)) {
        SetError(error, "expected ingress kind is unknown");
        return nullptr;
    }

    try {
        std::string contents;
        std::string local_error;
        if (!ReadStableFile(path, &contents, &local_error)) {
            SetError(error, std::move(local_error));
            return nullptr;
        }
        return VerifyEndpointContractBytes(
            contents,
            expected_sha256,
            expected_kind,
            error);
    } catch (const std::exception& exception) {
        try {
            SetError(
                error,
                std::string("endpoint contract load failure: ") +
                    exception.what());
        } catch (...) {
            SetError(error, "endpoint contract load failure");
        }
        return nullptr;
    } catch (...) {
        SetError(error, "unknown endpoint contract load failure");
        return nullptr;
    }
}

bool LoadEndpointContractFile(
    const std::string& path,
    std::string_view expected_sha256,
    IngressKind expected_kind,
    EndpointContract* contract,
    std::string* error) noexcept {
    if (contract == nullptr) {
        SetError(error, "endpoint contract output pointer is null");
        return false;
    }
    try {
        const auto verified =
            LoadVerifiedEndpointContractFile(
                path,
                expected_sha256,
                expected_kind,
                error);
        if (verified == nullptr) {
            return false;
        }
        static_assert(
            std::is_nothrow_move_assignable_v<EndpointContract>);
        *contract = verified->CopyValue();
        ClearError(error);
        return true;
    } catch (const std::exception& exception) {
        try {
            SetError(
                error,
                std::string("endpoint contract load failure: ") +
                    exception.what());
        } catch (...) {
            SetError(error, "endpoint contract load failure");
        }
        return false;
    } catch (...) {
        SetError(error, "unknown endpoint contract load failure");
        return false;
    }
}

}  // namespace l2flow::sdk
