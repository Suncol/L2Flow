#include "l2flow/ops/stable_output_prefix.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace ops = l2flow::ops;

namespace {

struct TestContext final {
    void Expect(
        bool condition,
        const std::string& description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    int failures = 0;
};

[[noreturn]] void ThrowErrno(const char* operation) {
    throw std::runtime_error(
        std::string(operation) + " failed: " +
        std::strerror(errno));
}

class ScopedFd final {
public:
    explicit ScopedFd(int fd = -1) noexcept : fd_(fd) {}

    ~ScopedFd() {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
    }

    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;

    [[nodiscard]] int get() const noexcept {
        return fd_;
    }

private:
    int fd_;
};

class TempDirectory final {
public:
    TempDirectory() {
        std::array<char, 64U> pattern{};
        constexpr std::string_view value =
            "/tmp/l2flow-stable-prefix-XXXXXX";
        static_assert(value.size() < pattern.size());
        std::copy(
            value.begin(),
            value.end(),
            pattern.begin());
        const char* const created =
            ::mkdtemp(pattern.data());
        if (created == nullptr) {
            ThrowErrno("mkdtemp");
        }
        path_ = created;
    }

    ~TempDirectory() {
        std::error_code ignored;
        static_cast<void>(
            std::filesystem::remove_all(path_, ignored));
    }

    TempDirectory(const TempDirectory&) = delete;
    TempDirectory& operator=(const TempDirectory&) = delete;

    [[nodiscard]] const std::string& path() const noexcept {
        return path_;
    }

    [[nodiscard]] std::string Child(
        std::string_view name) const {
        return path_ + "/" + std::string(name);
    }

private:
    std::string path_;
};

void MakeDirectory(
    const std::string& path,
    mode_t mode = 0700) {
    if (::mkdir(path.c_str(), mode) != 0) {
        ThrowErrno("mkdir");
    }
    if (::chmod(path.c_str(), mode) != 0) {
        ThrowErrno("chmod directory");
    }
}

[[nodiscard]] ScopedFd CreateFile(
    const std::string& path) {
    int fd = -1;
    do {
        fd = ::open(
            path.c_str(),
            O_WRONLY | O_CREAT | O_EXCL |
                O_CLOEXEC | O_NOFOLLOW | O_NOCTTY,
            0600);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
        ThrowErrno("create through stable prefix");
    }
    return ScopedFd(fd);
}

void WriteAll(
    int fd,
    std::string_view contents) {
    std::size_t offset = 0U;
    while (offset < contents.size()) {
        const ssize_t count =
            ::write(
                fd,
                contents.data() + offset,
                contents.size() - offset);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            ThrowErrno("write marker");
        }
        offset += static_cast<std::size_t>(count);
    }
}

void InstallMarker(
    const std::string& directory,
    std::string_view contents =
        ops::kSdkLogDirectoryMarkerContent,
    mode_t mode = 0444) {
    const std::string marker_path =
        directory + "/" +
        std::string(
            ops::kSdkLogDirectoryMarkerFilename);
    int fd = -1;
    do {
        fd = ::open(
            marker_path.c_str(),
            O_WRONLY | O_CREAT | O_EXCL |
                O_CLOEXEC | O_NOFOLLOW | O_NOCTTY,
            0600);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
        ThrowErrno("create marker");
    }
    const ScopedFd marker(fd);
    WriteAll(marker.get(), contents);
    if (::fchmod(marker.get(), mode) != 0) {
        ThrowErrno("chmod marker");
    }
}

[[nodiscard]] bool ExistsWithoutFollowing(
    const std::string& path) {
    struct stat metadata {};
    return ::lstat(path.c_str(), &metadata) == 0;
}

void CheckSafeCreation(TestContext* context) {
    TempDirectory directory;
    InstallMarker(directory.path());
    const std::string original_prefix =
        directory.Child("sdk-log");
    std::string error = "stale";
    std::unique_ptr<ops::StableOutputPrefix> lease =
        ops::OpenStableOutputPrefix(
            original_prefix,
            &error);

    context->Expect(
        lease != nullptr,
        "private owner-controlled parent is accepted");
    context->Expect(
        error.empty(),
        "successful open clears the error");
    if (lease == nullptr) {
        return;
    }
    context->Expect(
        lease->stable_prefix().ends_with("/sdk-log"),
        "stable prefix preserves the original basename");

    {
        const ScopedFd output =
            CreateFile(
                lease->stable_prefix() + ".log");
        context->Expect(
            output.get() >= 0,
            "stable prefix can create an SDK-style suffixed file");
    }
    context->Expect(
        ExistsWithoutFollowing(
            original_prefix + ".log"),
        "stable creation lands in the requested parent");
}

void CheckSymlinkRejection(TestContext* context) {
    TempDirectory directory;
    const std::string real =
        directory.Child("real");
    const std::string link =
        directory.Child("link");
    MakeDirectory(real);
    if (::symlink(real.c_str(), link.c_str()) != 0) {
        ThrowErrno("symlink");
    }

    std::string error;
    const std::unique_ptr<ops::StableOutputPrefix> lease =
        ops::OpenStableOutputPrefix(
            link + "/sdk",
            &error);
    context->Expect(
        lease == nullptr,
        "symbolic-link parent is rejected");
    context->Expect(
        !error.empty(),
        "symbolic-link rejection reports a fixed error");
    context->Expect(
        error.find(directory.path()) == std::string::npos &&
            error.find("link") == std::string::npos,
        "symbolic-link error discloses no input path");
}

void CheckUnsafeFinalParentRejection(
    TestContext* context) {
    TempDirectory directory;
    if (::chmod(directory.path().c_str(), 0770) != 0) {
        ThrowErrno("chmod unsafe final parent");
    }

    std::string error;
    const std::unique_ptr<ops::StableOutputPrefix> lease =
        ops::OpenStableOutputPrefix(
            directory.Child("sdk"),
            &error);
    context->Expect(
        lease == nullptr,
        "group-writable final parent is rejected");
    context->Expect(
        !error.empty(),
        "unsafe final parent reports an error");

    if (::chmod(directory.path().c_str(), 0700) != 0) {
        ThrowErrno("restore final parent mode");
    }
}

void CheckUnsafeNonFinalAncestorRejection(
    TestContext* context) {
    TempDirectory directory;
    const std::string unsafe =
        directory.Child("unsafe");
    const std::string final =
        unsafe + "/final";
    MakeDirectory(unsafe);
    MakeDirectory(final);
    if (::chmod(unsafe.c_str(), 0777) != 0) {
        ThrowErrno("chmod unsafe ancestor");
    }

    std::string error;
    const std::unique_ptr<ops::StableOutputPrefix> lease =
        ops::OpenStableOutputPrefix(
            final + "/sdk",
            &error);
    context->Expect(
        lease == nullptr,
        "non-sticky writable non-final ancestor is rejected");
    context->Expect(
        !error.empty(),
        "unsafe non-final ancestor reports an error");
}

void CheckAmbiguousAndMissingPaths(
    TestContext* context) {
    TempDirectory directory;
    const std::array<std::string, 5U> invalid{
        directory.path() + "/./sdk",
        directory.path() + "/../sdk",
        directory.path() + "//sdk",
        directory.path() + "/sdk/",
        "relative/sdk",
    };
    for (const std::string& prefix : invalid) {
        std::string error;
        const std::unique_ptr<ops::StableOutputPrefix> lease =
            ops::OpenStableOutputPrefix(prefix, &error);
        context->Expect(
            lease == nullptr && !error.empty(),
            "ambiguous or relative prefix is rejected");
        context->Expect(
            error.find(directory.path()) == std::string::npos,
            "syntax error discloses no input path");
    }

    const std::string missing =
        directory.Child("missing");
    std::string error;
    const std::unique_ptr<ops::StableOutputPrefix> lease =
        ops::OpenStableOutputPrefix(
            missing + "/sdk",
            &error);
    context->Expect(
        lease == nullptr,
        "missing parent is rejected instead of created");
    context->Expect(
        !ExistsWithoutFollowing(missing),
        "factory does not create a missing parent");
}

void CheckLeaseAnchoringAndLifetime(
    TestContext* context) {
    TempDirectory directory;
    const std::string selected =
        directory.Child("selected");
    const std::string moved =
        directory.Child("moved");
    MakeDirectory(selected);
    InstallMarker(selected);

    std::string error;
    std::unique_ptr<ops::StableOutputPrefix> lease =
        ops::OpenStableOutputPrefix(
            selected + "/sdk",
            &error);
    context->Expect(
        lease != nullptr,
        "lease setup succeeds");
    if (lease == nullptr) {
        return;
    }
    const std::string stable_prefix =
        lease->stable_prefix();

    if (::rename(selected.c_str(), moved.c_str()) != 0) {
        ThrowErrno("rename leased directory");
    }
    MakeDirectory(selected);
    {
        const ScopedFd output =
            CreateFile(stable_prefix + ".anchored");
        context->Expect(
            output.get() >= 0,
            "stable path remains usable after parent replacement");
    }
    context->Expect(
        ExistsWithoutFollowing(
            moved + "/sdk.anchored"),
        "lease remains anchored to the opened directory");
    context->Expect(
        !ExistsWithoutFollowing(
            selected + "/sdk.anchored"),
        "replacement path does not redirect the lease");

    lease.reset();
    errno = 0;
    int fd = -1;
    do {
        fd = ::open(
            (stable_prefix + ".after-close").c_str(),
            O_WRONLY | O_CREAT | O_EXCL |
                O_CLOEXEC | O_NOFOLLOW | O_NOCTTY,
            0600);
    } while (fd < 0 && errno == EINTR);
    context->Expect(
        fd < 0,
        "stable path expires when the lease is destroyed");
    if (fd >= 0) {
        static_cast<void>(::close(fd));
    }
    context->Expect(
        !ExistsWithoutFollowing(
            moved + "/sdk.after-close") &&
            !ExistsWithoutFollowing(
                selected + "/sdk.after-close"),
        "expired lease creates no output");
}

void CheckMarkerPolicy(TestContext* context) {
    {
        TempDirectory directory;
        std::string error;
        const std::unique_ptr<ops::StableOutputPrefix> lease =
            ops::OpenStableOutputPrefix(
                directory.Child("sdk"),
                &error);
        context->Expect(
            lease == nullptr && !error.empty(),
            "missing directory marker is rejected");
        context->Expect(
            !ExistsWithoutFollowing(
                directory.Child(
                    ops::kSdkLogDirectoryMarkerFilename)),
            "factory never creates a missing directory marker");
    }

    {
        TempDirectory directory;
        const std::string target =
            directory.Child("marker-target");
        {
            const ScopedFd file = CreateFile(target);
            WriteAll(
                file.get(),
                ops::kSdkLogDirectoryMarkerContent);
            if (::fchmod(file.get(), 0444) != 0) {
                ThrowErrno("chmod symlink marker target");
            }
        }
        const std::string marker =
            directory.Child(
                ops::kSdkLogDirectoryMarkerFilename);
        if (::symlink(target.c_str(), marker.c_str()) != 0) {
            ThrowErrno("symlink directory marker");
        }
        std::string error;
        const std::unique_ptr<ops::StableOutputPrefix> lease =
            ops::OpenStableOutputPrefix(
                directory.Child("sdk"),
                &error);
        context->Expect(
            lease == nullptr && !error.empty(),
            "symbolic-link directory marker is rejected");
    }

    {
        TempDirectory directory;
        std::string wrong(
            ops::kSdkLogDirectoryMarkerContent.size(),
            'x');
        InstallMarker(directory.path(), wrong);
        std::string error;
        const std::unique_ptr<ops::StableOutputPrefix> lease =
            ops::OpenStableOutputPrefix(
                directory.Child("sdk"),
                &error);
        context->Expect(
            lease == nullptr && !error.empty(),
            "wrong directory marker content is rejected");
    }

    {
        TempDirectory directory;
        InstallMarker(
            directory.path(),
            ops::kSdkLogDirectoryMarkerContent,
            0644);
        std::string error;
        const std::unique_ptr<ops::StableOutputPrefix> lease =
            ops::OpenStableOutputPrefix(
                directory.Child("sdk"),
                &error);
        context->Expect(
            lease == nullptr && !error.empty(),
            "directory marker mode other than exact 0444 is rejected");
    }

    {
        TempDirectory directory;
        InstallMarker(directory.path());
        const std::string marker =
            directory.Child(
                ops::kSdkLogDirectoryMarkerFilename);
        if (::link(
                marker.c_str(),
                directory.Child("marker-hard-link").c_str()) != 0) {
            ThrowErrno("hard-link directory marker");
        }
        std::string error;
        const std::unique_ptr<ops::StableOutputPrefix> lease =
            ops::OpenStableOutputPrefix(
                directory.Child("sdk"),
                &error);
        context->Expect(
            lease == nullptr && !error.empty(),
            "multiply linked directory marker is rejected");
    }
}

void CheckExclusiveDirectoryLease(TestContext* context) {
    TempDirectory directory;
    InstallMarker(directory.path());
    std::string error;
    std::unique_ptr<ops::StableOutputPrefix> first =
        ops::OpenStableOutputPrefix(
            directory.Child("first"),
            &error);
    context->Expect(
        first != nullptr,
        "first service acquires the SDK log directory lease");

    std::unique_ptr<ops::StableOutputPrefix> second =
        ops::OpenStableOutputPrefix(
            directory.Child("second"),
            &error);
    context->Expect(
        second == nullptr && !error.empty(),
        "a concurrent service cannot lease the same SDK log directory");

    first.reset();
    second =
        ops::OpenStableOutputPrefix(
            directory.Child("second"),
            &error);
    context->Expect(
        second != nullptr && error.empty(),
        "destroying the first lease releases the SDK log directory");
}

}  // namespace

int main() {
    TestContext context;
    try {
        CheckSafeCreation(&context);
        CheckSymlinkRejection(&context);
        CheckUnsafeFinalParentRejection(&context);
        CheckUnsafeNonFinalAncestorRejection(&context);
        CheckAmbiguousAndMissingPaths(&context);
        CheckLeaseAnchoringAndLifetime(&context);
        CheckMarkerPolicy(&context);
        CheckExclusiveDirectoryLease(&context);
    } catch (const std::exception& exception) {
        std::cerr << "UNCAUGHT: " << exception.what() << '\n';
        return 1;
    }

    if (context.failures != 0) {
        std::cerr << context.failures
                  << " stable output prefix checks failed\n";
        return 1;
    }
    std::cout
        << "stable output prefix lease is path-race resistant\n";
    return 0;
}
