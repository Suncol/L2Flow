#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/ingress/raw_reader.h"
#include "l2flow/ingress/raw_replay.h"
#include "l2flow/ingress/raw_schema.h"
#include "l2flow/ingress/raw_v1.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace ingress = l2flow::ingress;

namespace {

constexpr std::uint64_t kMaximumSegmentBytes =
    UINT64_C(4) * UINT64_C(1024) * UINT64_C(1024) *
    UINT64_C(1024);
constexpr std::uint64_t kMaximumTotalSegmentBytes =
    UINT64_C(8) * UINT64_C(1024) * UINT64_C(1024) *
    UINT64_C(1024);
constexpr std::size_t kMaximumInputs = 100'000U;
constexpr std::size_t kMaximumPathBytes = 4096U;
constexpr std::size_t kMaximumComponentBytes = 255U;

constexpr std::string_view kUsage =
    "Usage:\n"
    "  l2flow-raw-replay --root ROOT"
    " --segment RELATIVE_RAW_PATH DURABLE_LOGICAL_END"
    " [--segment ...] [options]\n"
    "\n"
    "Required input:\n"
    "  --root ROOT"
    "      Owner-only retained directory. Symlink path components are"
    " rejected.\n"
    "  --segment PATH BYTES\n"
    "      Canonical final name segment-NNNNNNNN.raw beneath ROOT and its\n"
    "      explicit exclusive durable logical end in segment-file bytes."
    " Repeatable.\n"
    "\n"
    "Phase 2 Raw metadata filters (repeat exact selectors as needed):\n"
    "  --stream-id N\n"
    "  --capture-date N\n"
    "  --service-id N\n"
    "  --service-version N\n"
    "  --message-id N\n"
    "  --wal BEGIN:END\n"
    "  --ingress BEGIN:END\n"
    "  --recv-realtime-ns BEGIN:END\n"
    "      Ranges are half-open. Either endpoint may be empty, but not both.\n"
    "\n"
    "Pacing and provenance:\n"
    "  --pace fast|original|fixed    Default: fast.\n"
    "  --speed NUMERATOR/DENOMINATOR Required exactly with --pace fixed.\n"
    "  --seed N                      Provenance-only deterministic seed.\n"
    "  --help\n"
    "\n"
    "Safety boundary:\n"
    "  This command never infers durability from a pathname, Raw manifest,"
    " or\n"
    "  physical file length. Each caller-supplied durable logical end must"
    " be a\n"
    "  complete Raw V1 record boundary and the whole selected prefix is"
    " validated\n"
    "  before stdout is produced. Bytes after that end are invisible. This"
    " target\n"
    "  does not admit recovered append-only input and does not business-decode"
    " the\n"
    "  opaque vendor head/body bytes. One invocation accepts one contiguous\n"
    "  stream-day namespace. FINALIZATION_CONTINUATION segments are rejected\n"
    "  because this interface cannot bind their DONE reserve/report evidence.\n"
    "  It does not publish RunManifestV1 because this\n"
    "  interface has no bound durable-journal header and accepted marker;\n"
    "  stdout is diagnostic metadata, not a durable run-manifest artifact.\n";

class UniqueFd final {
public:
    UniqueFd() noexcept = default;
    explicit UniqueFd(int fd) noexcept : fd_(fd) {}
    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;
    UniqueFd(UniqueFd&& other) noexcept
        : fd_(std::exchange(other.fd_, -1)) {}
    UniqueFd& operator=(UniqueFd&& other) noexcept {
        if (this != &other) {
            Reset();
            fd_ = std::exchange(other.fd_, -1);
        }
        return *this;
    }
    ~UniqueFd() {
        Reset();
    }

    [[nodiscard]] int get() const noexcept {
        return fd_;
    }
    [[nodiscard]] bool valid() const noexcept {
        return fd_ >= 0;
    }

private:
    void Reset() noexcept {
        if (fd_ >= 0) {
            const int closing = std::exchange(fd_, -1);
            static_cast<void>(::close(closing));
        }
    }

    int fd_ = -1;
};

struct InputSpec final {
    std::string relative_path;
    std::uint64_t durable_limit = 0U;
};

struct Options final {
    std::string root;
    bool root_seen = false;
    std::vector<InputSpec> inputs;
    ingress::RawReplayFilter filter;
    ingress::RawReplayRunSettings settings;
    bool pace_seen = false;
    bool speed_seen = false;
    bool seed_seen = false;
    bool help = false;
};

struct LoadedInput final {
    std::uint64_t durable_limit = 0U;
    l2flow::common::Sha256Digest prefix_sha256{};
    ingress::RawSegmentScanResult scan;
};

enum class ParseResult : std::uint8_t {
    kOk = 0U,
    kHelp,
    kError,
};

[[nodiscard]] bool WriteAll(
    int descriptor,
    std::string_view bytes) noexcept {
    std::size_t offset = 0U;
    while (offset < bytes.size()) {
        const std::size_t available = bytes.size() - offset;
        const std::size_t chunk = std::min(
            available,
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max()));
        const ssize_t written = ::write(
            descriptor, bytes.data() + offset, chunk);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (written == 0) {
            return false;
        }
        offset += static_cast<std::size_t>(written);
    }
    return true;
}

void Report(std::string_view message) noexcept {
    static_cast<void>(WriteAll(STDERR_FILENO, "l2flow-raw-replay: "));
    static_cast<void>(WriteAll(STDERR_FILENO, message));
    static_cast<void>(WriteAll(STDERR_FILENO, "\n"));
}

template <typename Value>
[[nodiscard]] bool ParseUnsigned(
    std::string_view text,
    Value* output) noexcept {
    static_assert(std::is_unsigned_v<Value>);
    if (output == nullptr || text.empty()) {
        return false;
    }
    Value value = 0U;
    const char* const begin = text.data();
    const char* const end = begin + text.size();
    const std::from_chars_result parsed =
        std::from_chars(begin, end, value, 10);
    if (parsed.ec != std::errc{} || parsed.ptr != end) {
        return false;
    }
    *output = value;
    return true;
}

[[nodiscard]] bool ParseRange(
    std::string_view text,
    ingress::RawReplayHalfOpenRange* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    const std::size_t separator = text.find(':');
    if (separator == std::string_view::npos ||
        text.find(':', separator + 1U) != std::string_view::npos) {
        return false;
    }
    const std::string_view begin = text.substr(0U, separator);
    const std::string_view end = text.substr(separator + 1U);
    if (begin.empty() && end.empty()) {
        return false;
    }

    ingress::RawReplayHalfOpenRange parsed;
    std::uint64_t value = 0U;
    if (!begin.empty()) {
        if (!ParseUnsigned(begin, &value)) {
            return false;
        }
        parsed.begin = value;
    }
    if (!end.empty()) {
        if (!ParseUnsigned(end, &value)) {
            return false;
        }
        parsed.end = value;
    }
    if (parsed.begin.has_value() && parsed.end.has_value() &&
        *parsed.begin > *parsed.end) {
        return false;
    }
    *output = parsed;
    return true;
}

[[nodiscard]] bool ParseSpeed(
    std::string_view text,
    std::uint64_t* numerator,
    std::uint64_t* denominator) noexcept {
    if (numerator == nullptr || denominator == nullptr) {
        return false;
    }
    const std::size_t separator = text.find('/');
    if (separator == std::string_view::npos ||
        text.find('/', separator + 1U) != std::string_view::npos) {
        return false;
    }
    std::uint64_t parsed_numerator = 0U;
    std::uint64_t parsed_denominator = 0U;
    if (!ParseUnsigned(
            text.substr(0U, separator), &parsed_numerator) ||
        !ParseUnsigned(
            text.substr(separator + 1U), &parsed_denominator) ||
        parsed_numerator == 0U || parsed_denominator == 0U) {
        return false;
    }
    *numerator = parsed_numerator;
    *denominator = parsed_denominator;
    return true;
}

template <typename Value>
[[nodiscard]] bool PushSelector(
    int argc,
    char** argv,
    int* index,
    std::vector<Value>* output) {
    if (index == nullptr || output == nullptr ||
        *index + 1 >= argc || argv[*index + 1] == nullptr) {
        return false;
    }
    Value value = 0U;
    if (!ParseUnsigned(
            std::string_view(argv[*index + 1]), &value)) {
        return false;
    }
    output->push_back(value);
    ++*index;
    return true;
}

[[nodiscard]] ParseResult ParseArguments(
    int argc,
    char** argv,
    Options* options) {
    if (options == nullptr || argc <= 0 || argv == nullptr) {
        Report("invalid argv");
        return ParseResult::kError;
    }

    for (int index = 1; index < argc; ++index) {
        if (argv[index] == nullptr) {
            Report("argv contains a null argument");
            return ParseResult::kError;
        }
        const std::string_view argument(argv[index]);
        if (argument == "--help") {
            options->help = true;
            continue;
        }
        if (argument == "--root") {
            if (options->root_seen || index + 1 >= argc ||
                argv[index + 1] == nullptr) {
                Report("--root must occur exactly once with a value");
                return ParseResult::kError;
            }
            options->root_seen = true;
            options->root = argv[++index];
            continue;
        }
        if (argument == "--segment") {
            if (index + 2 >= argc || argv[index + 1] == nullptr ||
                argv[index + 2] == nullptr ||
                options->inputs.size() >= kMaximumInputs) {
                Report(
                    "--segment requires a relative path and durable limit");
                return ParseResult::kError;
            }
            InputSpec input;
            input.relative_path = argv[++index];
            if (!ParseUnsigned(
                    std::string_view(argv[++index]),
                    &input.durable_limit)) {
                Report("--segment durable limit must be decimal uint64");
                return ParseResult::kError;
            }
            options->inputs.push_back(std::move(input));
            continue;
        }
        if (argument == "--stream-id") {
            if (!PushSelector(
                    argc, argv, &index,
                    &options->filter.source_stream_ids)) {
                Report("--stream-id requires a decimal uint32");
                return ParseResult::kError;
            }
            continue;
        }
        if (argument == "--capture-date") {
            if (!PushSelector(
                    argc, argv, &index,
                    &options->filter.capture_dates)) {
                Report("--capture-date requires a decimal uint32");
                return ParseResult::kError;
            }
            continue;
        }
        if (argument == "--service-id") {
            if (!PushSelector(
                    argc, argv, &index,
                    &options->filter.vendor_service_ids)) {
                Report("--service-id requires a decimal uint8");
                return ParseResult::kError;
            }
            continue;
        }
        if (argument == "--service-version") {
            if (!PushSelector(
                    argc, argv, &index,
                    &options->filter.vendor_service_versions)) {
                Report("--service-version requires a decimal uint16");
                return ParseResult::kError;
            }
            continue;
        }
        if (argument == "--message-id") {
            if (!PushSelector(
                    argc, argv, &index,
                    &options->filter.vendor_message_ids)) {
                Report("--message-id requires a decimal uint16");
                return ParseResult::kError;
            }
            continue;
        }
        if (argument == "--wal" ||
            argument == "--ingress" ||
            argument == "--recv-realtime-ns") {
            if (index + 1 >= argc || argv[index + 1] == nullptr) {
                Report("range option requires BEGIN:END");
                return ParseResult::kError;
            }
            ingress::RawReplayHalfOpenRange* destination =
                argument == "--wal"
                    ? &options->filter.wal
                    : (argument == "--ingress"
                           ? &options->filter.ingress_sequence
                           : &options->filter.recv_realtime_ns);
            if ((destination->begin.has_value() ||
                 destination->end.has_value()) ||
                !ParseRange(
                    std::string_view(argv[++index]), destination)) {
                Report(
                    "each range may occur once and must be a valid"
                    " half-open BEGIN:END");
                return ParseResult::kError;
            }
            continue;
        }
        if (argument == "--pace") {
            if (options->pace_seen || index + 1 >= argc ||
                argv[index + 1] == nullptr) {
                Report("--pace may occur once and requires a value");
                return ParseResult::kError;
            }
            options->pace_seen = true;
            const std::string_view value(argv[++index]);
            if (value == "fast") {
                options->settings.pace =
                    ingress::RawReplayPace::kAsFastAsPossible;
            } else if (value == "original") {
                options->settings.pace =
                    ingress::RawReplayPace::kOriginalMonotonic;
            } else if (value == "fixed") {
                options->settings.pace =
                    ingress::RawReplayPace::kFixedMultiplier;
            } else {
                Report("--pace must be fast, original, or fixed");
                return ParseResult::kError;
            }
            continue;
        }
        if (argument == "--speed") {
            if (options->speed_seen || index + 1 >= argc ||
                argv[index + 1] == nullptr ||
                !ParseSpeed(
                    std::string_view(argv[index + 1]),
                    &options->settings.speed_numerator,
                    &options->settings.speed_denominator)) {
                Report(
                    "--speed may occur once and requires nonzero NUM/DEN");
                return ParseResult::kError;
            }
            options->speed_seen = true;
            ++index;
            continue;
        }
        if (argument == "--seed") {
            if (options->seed_seen || index + 1 >= argc ||
                argv[index + 1] == nullptr ||
                !ParseUnsigned(
                    std::string_view(argv[index + 1]),
                    &options->settings.determinism_seed)) {
                Report("--seed may occur once and requires decimal uint64");
                return ParseResult::kError;
            }
            options->seed_seen = true;
            ++index;
            continue;
        }

        Report("unknown option");
        return ParseResult::kError;
    }

    if (options->help) {
        if (argc != 2) {
            Report("--help must be used alone");
            return ParseResult::kError;
        }
        return ParseResult::kHelp;
    }
    if (!options->root_seen || options->root.empty()) {
        Report("exactly one nonempty --root is required");
        return ParseResult::kError;
    }
    if (options->inputs.empty()) {
        Report("at least one --segment is required");
        return ParseResult::kError;
    }
    const bool fixed =
        options->settings.pace ==
        ingress::RawReplayPace::kFixedMultiplier;
    if (fixed != options->speed_seen) {
        Report("--speed is required exactly when --pace fixed is selected");
        return ParseResult::kError;
    }
    return ParseResult::kOk;
}

[[nodiscard]] bool IsPrivateDirectory(
    int descriptor) noexcept {
    struct stat status {};
    return descriptor >= 0 &&
        ::fstat(descriptor, &status) == 0 &&
        S_ISDIR(status.st_mode) &&
        status.st_uid == ::geteuid() &&
        (status.st_mode & 0077U) == 0U &&
        (status.st_mode & 0500U) == 0500U;
}

[[nodiscard]] bool IsSafePathComponent(
    std::string_view component) noexcept {
    return !component.empty() && component != "." &&
        component != ".." &&
        component.size() <= kMaximumComponentBytes &&
        component.find('\0') == std::string_view::npos;
}

[[nodiscard]] int OpenDirectoryComponent(
    int parent,
    std::string_view component) noexcept {
    if (!IsSafePathComponent(component)) {
        errno = EINVAL;
        return -1;
    }
    std::array<char, kMaximumComponentBytes + 1U> name{};
    std::memcpy(name.data(), component.data(), component.size());
    for (;;) {
        const int descriptor = ::openat(
            parent,
            name.data(),
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_NONBLOCK |
                O_CLOEXEC);
        if (descriptor >= 0 || errno != EINTR) {
            return descriptor;
        }
    }
}

[[nodiscard]] UniqueFd OpenRetainedRoot(
    std::string_view path) noexcept {
    if (path.empty() || path.size() > kMaximumPathBytes ||
        path.find('\0') != std::string_view::npos) {
        errno = EINVAL;
        return {};
    }
    const bool absolute = path.front() == '/';
    UniqueFd current(::open(
        absolute ? "/" : ".",
        O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_NONBLOCK |
            O_CLOEXEC));
    if (!current.valid()) {
        return {};
    }

    if (path == ".") {
        if (!IsPrivateDirectory(current.get())) {
            errno = EPERM;
            return {};
        }
        return current;
    }

    std::size_t offset = absolute ? 1U : 0U;
    if (offset == path.size()) {
        errno = EPERM;
        return {};
    }
    while (offset < path.size()) {
        const std::size_t separator = path.find('/', offset);
        const std::size_t end = separator == std::string_view::npos
            ? path.size()
            : separator;
        const std::string_view component =
            path.substr(offset, end - offset);
        const int next =
            OpenDirectoryComponent(current.get(), component);
        if (next < 0) {
            return {};
        }
        current = UniqueFd(next);
        if (separator == std::string_view::npos) {
            break;
        }
        offset = separator + 1U;
        if (offset == path.size()) {
            errno = EINVAL;
            return {};
        }
    }
    if (!IsPrivateDirectory(current.get())) {
        errno = EPERM;
        return {};
    }
    return current;
}

[[nodiscard]] bool ParseCanonicalSegmentName(
    std::string_view name,
    std::uint32_t* sequence) noexcept {
    constexpr std::string_view prefix = "segment-";
    constexpr std::string_view suffix = ".raw";
    if (sequence == nullptr || name.size() != 20U ||
        name.substr(0U, prefix.size()) != prefix ||
        name.substr(16U, suffix.size()) != suffix) {
        return false;
    }
    std::uint32_t value = 0U;
    if (!ParseUnsigned(name.substr(8U, 8U), &value) ||
        value == 0U || value > 99'999'999U) {
        return false;
    }
    *sequence = value;
    return true;
}

[[nodiscard]] bool SameFileSnapshot(
    const struct stat& left,
    const struct stat& right) noexcept {
    return left.st_dev == right.st_dev &&
        left.st_ino == right.st_ino &&
        left.st_uid == right.st_uid &&
        left.st_mode == right.st_mode &&
        left.st_nlink == right.st_nlink &&
        left.st_size == right.st_size &&
        left.st_mtim.tv_sec == right.st_mtim.tv_sec &&
        left.st_mtim.tv_nsec == right.st_mtim.tv_nsec &&
        left.st_ctim.tv_sec == right.st_ctim.tv_sec &&
        left.st_ctim.tv_nsec == right.st_ctim.tv_nsec;
}

[[nodiscard]] bool SameNamedFile(
    int parent,
    std::string_view name,
    const struct stat& expected) noexcept {
    if (!IsSafePathComponent(name)) {
        return false;
    }
    std::array<char, kMaximumComponentBytes + 1U> owned_name{};
    std::memcpy(owned_name.data(), name.data(), name.size());
    struct stat named {};
    return ::fstatat(
               parent,
               owned_name.data(),
               &named,
               AT_SYMLINK_NOFOLLOW) == 0 &&
        SameFileSnapshot(named, expected) &&
        S_ISREG(named.st_mode);
}

[[nodiscard]] bool DuplicateFd(
    int descriptor,
    UniqueFd* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    for (;;) {
        const int duplicate =
            ::fcntl(descriptor, F_DUPFD_CLOEXEC, 0);
        if (duplicate >= 0) {
            *output = UniqueFd(duplicate);
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

[[nodiscard]] bool ResolveSegmentParent(
    int root,
    std::string_view relative_path,
    UniqueFd* parent,
    std::string_view* basename) noexcept {
    if (parent == nullptr || basename == nullptr ||
        relative_path.empty() ||
        relative_path.size() > kMaximumPathBytes ||
        relative_path.front() == '/' ||
        relative_path.back() == '/' ||
        relative_path.find('\0') != std::string_view::npos ||
        !DuplicateFd(root, parent)) {
        errno = EINVAL;
        return false;
    }

    std::size_t offset = 0U;
    for (;;) {
        const std::size_t separator =
            relative_path.find('/', offset);
        if (separator == std::string_view::npos) {
            *basename = relative_path.substr(offset);
            return IsSafePathComponent(*basename);
        }
        const std::string_view component =
            relative_path.substr(offset, separator - offset);
        const int next =
            OpenDirectoryComponent(parent->get(), component);
        if (next < 0) {
            return false;
        }
        *parent = UniqueFd(next);
        if (!IsPrivateDirectory(parent->get())) {
            errno = EPERM;
            return false;
        }
        offset = separator + 1U;
    }
}

[[nodiscard]] bool ReadExactPrefix(
    int descriptor,
    std::span<std::byte> output) noexcept {
    std::size_t offset = 0U;
    while (offset < output.size()) {
        if (offset >
            static_cast<std::size_t>(
                std::numeric_limits<off_t>::max())) {
            errno = EOVERFLOW;
            return false;
        }
        const std::size_t available = output.size() - offset;
        const std::size_t chunk = std::min(
            available,
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max()));
        const ssize_t received = ::pread(
            descriptor,
            output.data() + offset,
            chunk,
            static_cast<off_t>(offset));
        if (received < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (received == 0) {
            errno = EIO;
            return false;
        }
        offset += static_cast<std::size_t>(received);
    }
    return true;
}

[[nodiscard]] bool LoadInput(
    int root,
    const InputSpec& input,
    std::uint64_t* total_bytes,
    LoadedInput* output,
    std::string* reason) {
    if (total_bytes == nullptr || output == nullptr ||
        reason == nullptr) {
        return false;
    }
    UniqueFd parent;
    std::string_view basename;
    if (!ResolveSegmentParent(
            root, input.relative_path, &parent, &basename)) {
        *reason = "path resolution rejected";
        return false;
    }
    std::uint32_t filename_sequence = 0U;
    if (!ParseCanonicalSegmentName(
            basename, &filename_sequence)) {
        *reason = "filename is not canonical final Raw";
        return false;
    }

    std::array<char, 21U> owned_name{};
    std::memcpy(owned_name.data(), basename.data(), basename.size());
    int opened = -1;
    for (;;) {
        opened = ::openat(
            parent.get(),
            owned_name.data(),
            O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC |
                O_NOATIME);
        if (opened >= 0 || errno != EINTR) {
            break;
        }
    }
    UniqueFd descriptor(opened);
    if (!descriptor.valid()) {
        *reason = "secure open rejected";
        return false;
    }

    struct stat before {};
    if (::fstat(descriptor.get(), &before) != 0 ||
        !S_ISREG(before.st_mode) ||
        before.st_uid != ::geteuid() ||
        before.st_nlink != static_cast<nlink_t>(1) ||
        (before.st_mode & 0777U) != 0600U ||
        before.st_size < 0 ||
        !SameNamedFile(parent.get(), basename, before)) {
        *reason = "file safety or name-to-inode gate rejected";
        return false;
    }
    const std::uint64_t physical_size =
        static_cast<std::uint64_t>(before.st_size);
    if (physical_size > kMaximumSegmentBytes ||
        input.durable_limit <
            static_cast<std::uint64_t>(
                ingress::kRawV1SegmentHeaderBytes) ||
        input.durable_limit > physical_size ||
        input.durable_limit > kMaximumSegmentBytes ||
        *total_bytes >
            kMaximumTotalSegmentBytes - input.durable_limit ||
        input.durable_limit >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max())) {
        *reason = "file size or durable logical end is out of bounds";
        return false;
    }

    auto mutable_bytes =
        std::make_shared<std::vector<std::byte>>(
            static_cast<std::size_t>(input.durable_limit));
    if (!ReadExactPrefix(descriptor.get(), *mutable_bytes)) {
        *reason = "prefix read failed";
        return false;
    }

    struct stat after {};
    if (::fstat(descriptor.get(), &after) != 0 ||
        !SameFileSnapshot(before, after) ||
        !SameNamedFile(parent.get(), basename, after)) {
        *reason = "file changed during snapshot";
        return false;
    }

    const std::shared_ptr<const std::vector<std::byte>> bytes =
        mutable_bytes;
    ingress::RawSegmentScanResult scan =
        ingress::ScanRawSegmentV1(bytes, input.durable_limit);
    if (!scan.ok()) {
        *reason = std::string("Raw V1 scan failed: ") +
            std::string(
                ingress::RawV1ErrorName(scan.codec_error)) +
            "/" +
            std::to_string(
                static_cast<unsigned int>(scan.error));
        return false;
    }
    if (scan.segment.segment_sequence != filename_sequence) {
        *reason = "filename sequence does not match segment header";
        return false;
    }
    if (scan.segment.raw_schema_sha256 !=
        ingress::RawSchemaSha256Digest()) {
        *reason =
            "segment Raw schema identity does not match the compiled digest";
        return false;
    }
    if (scan.segment.segment_flags != 0U) {
        *reason =
            "finalization continuation requires bound reserve/report evidence";
        return false;
    }

    LoadedInput loaded;
    loaded.durable_limit = input.durable_limit;
    loaded.prefix_sha256 = l2flow::common::ComputeSha256(
        std::span<const std::byte>(*mutable_bytes));
    loaded.scan = std::move(scan);
    *total_bytes += input.durable_limit;
    *output = std::move(loaded);
    reason->clear();
    return true;
}

[[nodiscard]] bool CheckedIncrement(
    std::uint32_t value,
    std::uint32_t* output) noexcept {
    if (output == nullptr ||
        value == std::numeric_limits<std::uint32_t>::max()) {
        return false;
    }
    *output = value + 1U;
    return true;
}

[[nodiscard]] bool CheckedIncrement(
    std::uint64_t value,
    std::uint64_t* output) noexcept {
    if (output == nullptr ||
        value == std::numeric_limits<std::uint64_t>::max()) {
        return false;
    }
    *output = value + 1U;
    return true;
}

[[nodiscard]] bool ValidateAndOrderInputs(
    std::vector<LoadedInput>* inputs,
    std::string* reason) {
    if (inputs == nullptr || reason == nullptr || inputs->empty()) {
        return false;
    }
    std::sort(
        inputs->begin(),
        inputs->end(),
        [](const LoadedInput& left, const LoadedInput& right) {
            return left.scan.segment.segment_sequence <
                right.scan.segment.segment_sequence;
        });

    for (std::size_t index = 1U; index < inputs->size(); ++index) {
        const ingress::RawSegmentScanResult& previous =
            (*inputs)[index - 1U].scan;
        const ingress::RawSegmentScanResult& current =
            (*inputs)[index].scan;
        if (current.segment.source_stream_id !=
                previous.segment.source_stream_id ||
            current.segment.capture_date !=
                previous.segment.capture_date ||
            current.segment.stream_day_id !=
                previous.segment.stream_day_id) {
            *reason =
                "one invocation may contain only one stream-day namespace";
            return false;
        }
        std::uint32_t expected_segment_sequence = 0U;
        if (!CheckedIncrement(
                previous.segment.segment_sequence,
                &expected_segment_sequence) ||
            current.segment.segment_sequence !=
                expected_segment_sequence) {
            *reason = "segment sequence is duplicated or non-contiguous";
            return false;
        }
        if (current.segment.segment_base_wal_pos !=
            previous.validated_end_wal_pos) {
            *reason = "segment WAL bases are not contiguous";
            return false;
        }
        std::uint64_t expected_ingress =
            previous.segment.first_ingress_sequence;
        if (!previous.records.empty() &&
            !CheckedIncrement(
                previous.records.back().header().
                    ingress_sequence,
                &expected_ingress)) {
            *reason = "ingress sequence cannot continue after uint64 max";
            return false;
        }
        if (current.segment.first_ingress_sequence !=
            expected_ingress) {
            *reason = "segment ingress ranges are not contiguous";
            return false;
        }
    }
    reason->clear();
    return true;
}

template <typename Value>
void SortUnique(std::vector<Value>* values) {
    std::sort(values->begin(), values->end());
    values->erase(
        std::unique(values->begin(), values->end()),
        values->end());
}

void NormalizeSelectors(ingress::RawReplayFilter* filter) {
    SortUnique(&filter->source_stream_ids);
    SortUnique(&filter->capture_dates);
    SortUnique(&filter->vendor_service_ids);
    SortUnique(&filter->vendor_service_versions);
    SortUnique(&filter->vendor_message_ids);
}

template <typename Value>
requires std::is_unsigned_v<Value>
void AppendUnsigned(std::string* output, Value value) {
    std::array<char, std::numeric_limits<Value>::digits10 + 3U>
        buffer{};
    const std::to_chars_result encoded =
        std::to_chars(
            buffer.data(),
            buffer.data() + buffer.size(),
            value,
            10);
    if (encoded.ec != std::errc{}) {
        throw std::system_error(
            std::make_error_code(encoded.ec));
    }
    output->append(
        buffer.data(),
        static_cast<std::size_t>(encoded.ptr - buffer.data()));
}

void AppendField(
    std::string* output,
    std::string_view name,
    std::string_view value) {
    output->push_back('\t');
    output->append(name);
    output->push_back('=');
    output->append(value);
}

template <typename Value>
requires std::is_unsigned_v<Value>
void AppendField(
    std::string* output,
    std::string_view name,
    Value value) {
    output->push_back('\t');
    output->append(name);
    output->push_back('=');
    AppendUnsigned(output, value);
}

[[nodiscard]] std::string_view PaceName(
    ingress::RawReplayPace pace) noexcept {
    switch (pace) {
        case ingress::RawReplayPace::kAsFastAsPossible:
            return "fast";
        case ingress::RawReplayPace::kFixedMultiplier:
            return "fixed";
        case ingress::RawReplayPace::kOriginalMonotonic:
            return "original";
    }
    return "invalid";
}

[[nodiscard]] bool EmitHeader(
    const Options& options,
    std::size_t input_count,
    std::size_t record_count) {
    std::string line = "raw_replay_v1";
    AppendField(
        &line, "provenance", "explicit_durable_frontier");
    AppendField(&line, "business_decode", "false");
    AppendField(&line, "run_manifest_published", "false");
    AppendField(&line, "append_only", "rejected");
    AppendField(&line, "pace", PaceName(options.settings.pace));
    AppendField(
        &line, "speed_numerator",
        options.settings.speed_numerator);
    AppendField(
        &line, "speed_denominator",
        options.settings.speed_denominator);
    AppendField(
        &line, "determinism_seed",
        options.settings.determinism_seed);
    AppendField(&line, "input_count", input_count);
    AppendField(&line, "selected_record_count", record_count);
    line.push_back('\n');
    return WriteAll(STDOUT_FILENO, line);
}

[[nodiscard]] bool EmitInput(
    std::size_t index,
    const LoadedInput& input) {
    const ingress::SegmentHeaderV1& segment = input.scan.segment;
    std::string line = "input";
    AppendField(&line, "index", index);
    AppendField(
        &line, "source_stream_id",
        segment.source_stream_id);
    AppendField(&line, "capture_date", segment.capture_date);
    AppendField(
        &line,
        "stream_day_id",
        l2flow::common::Identity128Hex(segment.stream_day_id));
    AppendField(
        &line, "segment_sequence",
        segment.segment_sequence);
    AppendField(
        &line, "segment_flags",
        segment.segment_flags);
    AppendField(
        &line, "durable_logical_end_offset",
        input.durable_limit);
    AppendField(
        &line, "durable_logical_end_wal_pos",
        input.scan.validated_end_wal_pos);
    AppendField(
        &line, "durable_prefix_sha256",
        l2flow::common::Sha256Hex(input.prefix_sha256));
    AppendField(
        &line, "validated_record_count",
        input.scan.records.size());
    line.push_back('\n');
    return WriteAll(STDOUT_FILENO, line);
}

[[nodiscard]] bool EmitRecord(
    const ingress::RawReplayRecord& record) {
    const ingress::RawRecordView& view = record.view;
    const ingress::RawRecordHeaderV1& header = view.header();
    std::string line = "record";
    AppendField(
        &line, "source_stream_id",
        record.segment.source_stream_id);
    AppendField(
        &line, "capture_date",
        record.segment.capture_date);
    AppendField(
        &line,
        "stream_day_id",
        l2flow::common::Identity128Hex(
            record.segment.stream_day_id));
    AppendField(
        &line, "segment_sequence",
        record.segment.segment_sequence);
    AppendField(
        &line, "ingress_sequence",
        header.ingress_sequence);
    AppendField(
        &line, "record_start_wal_pos",
        view.record_start_wal_pos());
    AppendField(
        &line, "record_end_wal_pos",
        view.record_end_wal_pos());
    AppendField(
        &line, "recv_realtime_ns",
        header.recv_realtime_ns);
    AppendField(
        &line, "recv_monotonic_ns",
        header.recv_monotonic_ns);
    AppendField(
        &line, "vendor_service_id",
        static_cast<std::uint32_t>(
            header.vendor_service_id));
    AppendField(
        &line, "vendor_service_version",
        static_cast<std::uint32_t>(
            header.vendor_service_version));
    AppendField(
        &line, "vendor_message_id",
        static_cast<std::uint32_t>(
            header.vendor_message_id));
    AppendField(
        &line, "vendor_message_encoding",
        static_cast<std::uint32_t>(
            header.vendor_message_encoding));
    AppendField(
        &line, "vendor_sequence_id",
        header.vendor_sequence_id);
    AppendField(
        &line, "vendor_local_time_raw",
        header.vendor_local_time_raw);
    AppendField(
        &line, "vendor_message_size",
        header.vendor_message_size);
    AppendField(
        &line, "payload_crc32c",
        header.payload_crc32c);
    AppendField(&line, "provenance", "durable");
    line.push_back('\n');
    return WriteAll(STDOUT_FILENO, line);
}

void AppendLocator(
    std::string* line,
    std::string_view prefix,
    const ingress::RawReplayLocator& locator) {
    AppendField(
        line,
        std::string(prefix) + "_segment_stream_id",
        locator.source_stream_id);
    AppendField(
        line,
        std::string(prefix) + "_ingress_sequence",
        locator.ingress_sequence);
    AppendField(
        line,
        std::string(prefix) + "_record_start_wal_pos",
        locator.record_start_wal_pos);
    AppendField(
        line,
        std::string(prefix) + "_record_end_wal_pos",
        locator.record_end_wal_pos);
}

[[nodiscard]] bool EmitBoundary(
    std::string_view kind,
    const ingress::RawReplayBoundary& boundary) {
    std::string line = "boundary";
    AppendField(&line, "kind", kind);
    AppendLocator(&line, "previous", boundary.previous);
    AppendLocator(&line, "current", boundary.current);
    AppendField(
        &line, "previous_recv_monotonic_ns",
        boundary.previous_recv_monotonic_ns);
    AppendField(
        &line, "current_recv_monotonic_ns",
        boundary.current_recv_monotonic_ns);
    AppendField(
        &line, "previous_clock_algorithm",
        boundary.previous_clock_epoch.algorithm);
    AppendField(
        &line,
        "previous_clock_digest",
        l2flow::common::Sha256Hex(
            boundary.previous_clock_epoch.digest));
    AppendField(
        &line, "current_clock_algorithm",
        boundary.current_clock_epoch.algorithm);
    AppendField(
        &line,
        "current_clock_digest",
        l2flow::common::Sha256Hex(
            boundary.current_clock_epoch.digest));
    line.push_back('\n');
    return WriteAll(STDOUT_FILENO, line);
}

[[nodiscard]] bool EmitEnd(
    std::uint64_t records,
    std::uint64_t epoch_boundaries,
    std::uint64_t monotonic_regressions) {
    std::string line = "end";
    AppendField(&line, "record_count", records);
    AppendField(
        &line, "clock_epoch_boundary_count",
        epoch_boundaries);
    AppendField(
        &line, "monotonic_regression_count",
        monotonic_regressions);
    line.push_back('\n');
    return WriteAll(STDOUT_FILENO, line);
}

[[nodiscard]] int Run(const Options& parsed_options) {
    Options options = parsed_options;
    NormalizeSelectors(&options.filter);

    UniqueFd root = OpenRetainedRoot(options.root);
    if (!root.valid()) {
        Report("retained root secure-open/private-directory gate failed");
        return 3;
    }

    std::vector<LoadedInput> loaded;
    loaded.reserve(options.inputs.size());
    std::uint64_t total_bytes = 0U;
    for (std::size_t index = 0U;
         index < options.inputs.size();
         ++index) {
        LoadedInput input;
        std::string reason;
        if (!LoadInput(
                root.get(),
                options.inputs[index],
                &total_bytes,
                &input,
                &reason)) {
            Report(
                std::string("input ") + std::to_string(index) +
                " rejected: " + reason);
            return 3;
        }
        loaded.push_back(std::move(input));
    }

    std::string ordering_reason;
    if (!ValidateAndOrderInputs(&loaded, &ordering_reason)) {
        Report(std::string("input set rejected: ") + ordering_reason);
        return 3;
    }

    std::vector<ingress::RawReplaySegmentInput> replay_inputs;
    replay_inputs.reserve(loaded.size());
    for (const LoadedInput& input : loaded) {
        replay_inputs.push_back(
            ingress::RawReplaySegmentInput{
                &input.scan,
                ingress::RawReplayScanExtent::kDurableOnly,
                0U});
    }

    ingress::SteadyRawReplayClock clock;
    ingress::ThreadRawReplaySleeper sleeper;
    ingress::RawReplayClock* const clock_pointer =
        options.settings.pace ==
                ingress::RawReplayPace::kAsFastAsPossible
            ? nullptr
            : &clock;
    ingress::RawReplaySleeper* const sleeper_pointer =
        options.settings.pace ==
                ingress::RawReplayPace::kAsFastAsPossible
            ? nullptr
            : &sleeper;

    std::unique_ptr<ingress::RawReplayEngine> engine;
    const ingress::RawReplayError creation =
        ingress::RawReplayEngine::Create(
            replay_inputs,
            options.filter,
            options.settings,
            clock_pointer,
            sleeper_pointer,
            &engine);
    if (creation != ingress::RawReplayError::kNone ||
        engine == nullptr) {
        Report(
            std::string("replay construction failed: ") +
            std::string(
                ingress::RawReplayErrorName(creation)));
        return 3;
    }

    if (!EmitHeader(
            options,
            loaded.size(),
            engine->selected_record_count())) {
        Report("stdout write failed");
        return 4;
    }
    for (std::size_t index = 0U; index < loaded.size(); ++index) {
        if (!EmitInput(index, loaded[index])) {
            Report("stdout write failed");
            return 4;
        }
    }

    std::uint64_t record_count = 0U;
    std::uint64_t epoch_boundary_count = 0U;
    std::uint64_t monotonic_regression_count = 0U;
    for (;;) {
        ingress::RawReplayStep step = engine->Next();
        switch (step.kind) {
            case ingress::RawReplayStepKind::kRecord:
                if (!step.record.has_value() ||
                    record_count ==
                        std::numeric_limits<std::uint64_t>::max()) {
                    Report("replay returned an invalid record step");
                    return 4;
                }
                if (!EmitRecord(*step.record)) {
                    Report("stdout write failed");
                    return 4;
                }
                ++record_count;
                break;
            case ingress::RawReplayStepKind::kClockEpochBoundary:
                if (!step.boundary.has_value() ||
                    epoch_boundary_count ==
                        std::numeric_limits<std::uint64_t>::max()) {
                    Report("replay returned an invalid epoch boundary");
                    return 4;
                }
                if (!EmitBoundary(
                        "clock_epoch", *step.boundary)) {
                    Report("stdout write failed");
                    return 4;
                }
                ++epoch_boundary_count;
                break;
            case ingress::RawReplayStepKind::kMonotonicRegression:
                if (!step.boundary.has_value() ||
                    monotonic_regression_count ==
                        std::numeric_limits<std::uint64_t>::max()) {
                    Report(
                        "replay returned an invalid monotonic boundary");
                    return 4;
                }
                if (!EmitBoundary(
                        "monotonic_regression",
                        *step.boundary)) {
                    Report("stdout write failed");
                    return 4;
                }
                ++monotonic_regression_count;
                break;
            case ingress::RawReplayStepKind::kEnd:
                if (record_count !=
                    static_cast<std::uint64_t>(
                        engine->selected_record_count())) {
                    Report("replay selected/emitted count mismatch");
                    return 4;
                }
                if (!EmitEnd(
                        record_count,
                        epoch_boundary_count,
                        monotonic_regression_count)) {
                    Report("stdout write failed");
                    return 4;
                }
                return 0;
            case ingress::RawReplayStepKind::kError:
                Report(
                    std::string("replay failed: ") +
                    std::string(
                        ingress::RawReplayErrorName(step.error)));
                return 4;
            case ingress::RawReplayStepKind::kPaused:
                Report("unexpected paused replay state");
                return 4;
        }
    }
}

}  // namespace

int main(int argc, char** argv) {
    try {
        static_cast<void>(std::signal(SIGPIPE, SIG_IGN));
        Options options;
        const ParseResult parsed =
            ParseArguments(argc, argv, &options);
        if (parsed == ParseResult::kHelp) {
            return WriteAll(STDOUT_FILENO, kUsage) ? 0 : 4;
        }
        if (parsed != ParseResult::kOk) {
            static_cast<void>(WriteAll(STDERR_FILENO, kUsage));
            return 2;
        }
        return Run(options);
    } catch (const std::bad_alloc&) {
        Report("resource exhausted");
        return 4;
    } catch (const std::exception&) {
        Report("unexpected internal failure");
        return 4;
    } catch (...) {
        Report("unexpected non-standard failure");
        return 4;
    }
}
