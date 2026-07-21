#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::ops {

inline constexpr std::string_view kPrometheusTextfileMagic =
    "# l2flow_metrics_textfile_version 1\n";
inline constexpr std::size_t kMaximumPrometheusTextfileBodyBytes =
    1024U * 1024U - kPrometheusTextfileMagic.size();
inline constexpr std::string_view
kPrometheusTextfileLeaseFilenamePrefix =
    ".l2flow-metrics-lease-v1-";

[[nodiscard]] constexpr bool
IsPrometheusTextfileLeaseFilename(
    std::string_view candidate) noexcept {
    if (candidate.size() <
        kPrometheusTextfileLeaseFilenamePrefix.size()) {
        return false;
    }
    for (std::size_t index = 0U;
         index <
         kPrometheusTextfileLeaseFilenamePrefix.size();
         ++index) {
        unsigned char observed =
            static_cast<unsigned char>(
                candidate[index]);
        unsigned char expected =
            static_cast<unsigned char>(
                kPrometheusTextfileLeaseFilenamePrefix[index]);
        if (observed >=
                static_cast<unsigned char>('A') &&
            observed <=
                static_cast<unsigned char>('Z')) {
            observed = static_cast<unsigned char>(
                observed -
                static_cast<unsigned char>('A') +
                static_cast<unsigned char>('a'));
        }
        if (observed != expected) {
            return false;
        }
    }
    return true;
}

// Holds a private sidecar flock for the lifetime of one production metrics
// worker. This prevents two cooperating services from silently alternating
// replacements of the same textfile.
class PrometheusTextfileLease final {
public:
    ~PrometheusTextfileLease();

    PrometheusTextfileLease(
        const PrometheusTextfileLease&) = delete;
    PrometheusTextfileLease& operator=(
        const PrometheusTextfileLease&) = delete;
    PrometheusTextfileLease(
        PrometheusTextfileLease&&) = delete;
    PrometheusTextfileLease& operator=(
        PrometheusTextfileLease&&) = delete;

    // Publishes relative to the retained destination-directory descriptor.
    // Renaming or replacing the original pathname therefore cannot redirect a
    // live worker away from the namespace protected by this lease.
    [[nodiscard]] bool Publish(
        std::string_view contents,
        std::string* error) noexcept;

private:
    friend std::unique_ptr<PrometheusTextfileLease>
    AcquirePrometheusTextfileLease(
        const std::string&,
        std::string*) noexcept;

    PrometheusTextfileLease(
        int directory_fd,
        int lock_fd,
        std::string basename) noexcept;

    int directory_fd_;
    int lock_fd_;
    std::string basename_;
};

// Validates the destination directory and acquires a persistent, typed 0600
// sidecar lock derived from the case-folded portable-ASCII target basename.
// The sidecar remains on disk for reuse. Errors never contain `path`.
[[nodiscard]] std::unique_ptr<PrometheusTextfileLease>
AcquirePrometheusTextfileLease(
    const std::string& path,
    std::string* error) noexcept;

// Atomically replaces one Prometheus textfile with owner-only permissions.
// Every absolute parent component is opened without following symbolic links.
// The owner-controlled destination directory fd anchors temporary creation and
// replacement. An existing target must be a private, singly linked regular file
// owned by the effective uid and must accept a non-blocking exclusive flock.
// Metric paths and contents are never included in an error.
// The publisher prepends kPrometheusTextfileMagic. An existing target must
// carry the same marker, preventing a stopped shadow capture or another
// owner-only file from being retyped by rename. `contents` is bounded so the
// complete marked file is at most one MiB.
[[nodiscard]] bool PublishPrometheusTextfile(
    const std::string& path,
    std::string_view contents,
    std::string* error) noexcept;

}  // namespace l2flow::ops
