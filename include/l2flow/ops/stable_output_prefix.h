#pragma once

#include <memory>
#include <string>
#include <string_view>

namespace l2flow::ops {

inline constexpr std::string_view
kSdkLogDirectoryMarkerFilename =
    ".l2flow-sdk-log-directory-v1";
inline constexpr std::string_view
kSdkLogDirectoryMarkerContent =
    "l2flow-sdk-log-directory-v1\n";

// Conservatively reserves every portable-ASCII case variant so the marker
// namespace remains safe on case-insensitive or case-folding filesystems.
[[nodiscard]] constexpr bool
IsSdkLogDirectoryMarkerFilename(
    std::string_view candidate) noexcept {
    if (candidate.size() !=
        kSdkLogDirectoryMarkerFilename.size()) {
        return false;
    }
    for (std::size_t index = 0U;
         index < candidate.size();
         ++index) {
        unsigned char observed =
            static_cast<unsigned char>(
                candidate[index]);
        unsigned char expected =
            static_cast<unsigned char>(
                kSdkLogDirectoryMarkerFilename[index]);
        if (observed >=
                static_cast<unsigned char>('A') &&
            observed <=
                static_cast<unsigned char>('Z')) {
            observed = static_cast<unsigned char>(
                observed -
                static_cast<unsigned char>('A') +
                static_cast<unsigned char>('a'));
        }
        if (expected >=
                static_cast<unsigned char>('A') &&
            expected <=
                static_cast<unsigned char>('Z')) {
            expected = static_cast<unsigned char>(
                expected -
                static_cast<unsigned char>('A') +
                static_cast<unsigned char>('a'));
        }
        if (observed != expected) {
            return false;
        }
    }
    return true;
}

// Holds an opened output directory so an SDK which appends a suffix to a
// filename prefix cannot be redirected by renaming or replacing an ancestor.
// The stable prefix preserves the basename supplied to
// OpenStableOutputPrefix and is valid only while this lease is alive.
class StableOutputPrefix final {
public:
    ~StableOutputPrefix();

    StableOutputPrefix(const StableOutputPrefix&) = delete;
    StableOutputPrefix& operator=(const StableOutputPrefix&) = delete;
    StableOutputPrefix(StableOutputPrefix&&) = delete;
    StableOutputPrefix& operator=(StableOutputPrefix&&) = delete;

    [[nodiscard]] const std::string& stable_prefix() const noexcept {
        return stable_prefix_;
    }

private:
    friend std::unique_ptr<StableOutputPrefix>
    OpenStableOutputPrefix(
        const std::string& absolute_prefix,
        std::string* error) noexcept;

    StableOutputPrefix(
        int directory_fd,
        int marker_fd,
        std::string stable_prefix) noexcept;

    int directory_fd_;
    int marker_fd_;
    std::string stable_prefix_;
};

// Opens every existing parent directory of absolute_prefix from "/" using
// openat(O_DIRECTORY | O_NOFOLLOW), then returns a lease exposing
// /proc/self/fd/<directory-fd>/<original-basename>.
//
// Empty, NUL-containing, "."/".." and empty path components are rejected.
// Trusted ancestor owners are the effective uid, uid 0, and the owner observed
// on the namespace root. A group/world-writable ancestor is accepted only when
// sticky and its selected child has a trusted owner. The final parent must be
// owned by the effective uid and must not be group/world writable.
//
// The final parent must also contain kSdkLogDirectoryMarkerFilename as a
// non-symlink, singly linked regular file with exact mode 0444, a trusted
// owner, and byte-exact kSdkLogDirectoryMarkerContent. This reserved marker
// declares that the pre-existing directory is dedicated to SDK logs. The
// returned lease also holds a nonblocking exclusive flock on that marker, so
// a second cooperating service cannot lease the same directory concurrently.
//
// The function does not create directories. Errors use fixed classifications
// and never contain the supplied prefix.
[[nodiscard]] std::unique_ptr<StableOutputPrefix>
OpenStableOutputPrefix(
    const std::string& absolute_prefix,
    std::string* error) noexcept;

}  // namespace l2flow::ops
