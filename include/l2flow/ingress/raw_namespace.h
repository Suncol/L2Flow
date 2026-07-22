#pragma once

#include "l2flow/ingress/raw_v1.h"
#include "l2flow/ingress/raw_wal_writer.h"
#include "l2flow/ingress/raw_writer_lease.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace l2flow::ingress {

struct RawFreshStateAuthorizationV1;
class RawFreshMutationAuthorizationGateV1;

inline constexpr char kRawJournalFilename[] =
    "durable.journal";
inline constexpr char kRawJournalTemporaryFilename[] =
    ".durable.journal.raw-journal.tmp";
inline constexpr char kRawFirstSegmentFilename[] =
    "segment-00000001.raw";
inline constexpr char kRawFirstSegmentTemporaryFilename[] =
    ".segment-00000001.raw.raw-segment.tmp";

// The durable Raw namespace uses the four frozen short route slugs from
// docs/design.md, not the longer executable/service names accepted by the
// Phase-1 CLI parser:
//
//   1001 -> sh-snapshot
//   1002 -> sh-tick
//   2001 -> sz-snapshot
//   2002 -> sz-tick
//
// Production route composition and retained evidence must use this mapping
// so a syntactically valid caller-selected slug cannot redirect one
// source_stream_id into a second namespace. Generic low-level namespace test
// helpers remain capable of exercising arbitrary IDs and slugs.
[[nodiscard]] std::optional<std::string_view>
CanonicalRawStreamSlugV1(
    std::uint32_t source_stream_id) noexcept;

[[nodiscard]] bool IsCanonicalRawStreamRouteV1(
    std::uint32_t source_stream_id,
    std::string_view stream_slug) noexcept;

class RawStreamDirectory final {
public:
    ~RawStreamDirectory();

    RawStreamDirectory(const RawStreamDirectory&) = delete;
    RawStreamDirectory& operator=(const RawStreamDirectory&) = delete;
    RawStreamDirectory(RawStreamDirectory&&) = delete;
    RawStreamDirectory& operator=(RawStreamDirectory&&) = delete;

    [[nodiscard]] int descriptor() const noexcept {
        return directory_fd_;
    }
    [[nodiscard]] std::uint32_t source_stream_id() const noexcept {
        return source_stream_id_;
    }
    [[nodiscard]] std::uint32_t capture_date() const noexcept {
        return capture_date_;
    }

private:
    friend std::unique_ptr<RawStreamDirectory>
    OpenOrCreateRawStreamDirectory(
        const std::string&,
        std::uint32_t,
        std::uint32_t,
        const std::string&,
        std::string*) noexcept;
    friend std::unique_ptr<RawStreamDirectory>
    OpenOrCreateRawStreamDirectoryAt(
        int,
        std::uint32_t,
        std::uint32_t,
        const std::string&,
        std::string*) noexcept;
    friend std::unique_ptr<RawStreamDirectory>
    OpenOrCreateAuthorizedFreshRawStreamDirectory(
        const std::string&,
        std::uint32_t,
        std::uint32_t,
        const std::string&,
        const RawFreshStateAuthorizationV1&,
        const RawFreshMutationAuthorizationGateV1&,
        std::string*) noexcept;
    friend std::unique_ptr<RawStreamDirectory>
    OpenOrCreateAuthorizedFreshRawStreamDirectoryAt(
        int,
        std::uint32_t,
        std::uint32_t,
        const std::string&,
        const RawFreshStateAuthorizationV1&,
        const RawFreshMutationAuthorizationGateV1&,
        std::string*) noexcept;
    friend std::unique_ptr<RawStreamDirectory>
    OpenExistingRawStreamDirectory(
        const std::string&,
        std::uint32_t,
        std::uint32_t,
        const std::string&,
        std::string*) noexcept;

    RawStreamDirectory(
        int directory_fd,
        std::uint32_t source_stream_id,
        std::uint32_t capture_date) noexcept;

    int directory_fd_ = -1;
    std::uint32_t source_stream_id_ = 0U;
    std::uint32_t capture_date_ = 0U;
};

// Opens an absolute, no-symlink Raw root and durably creates/validates
// capture_date=YYYYMMDD/stream=<id>-<slug>. The final Raw root, date, and
// stream directories must be service-UID-owned and private.
[[nodiscard]] std::unique_ptr<RawStreamDirectory>
OpenOrCreateRawStreamDirectory(
    const std::string& raw_root,
    std::uint32_t source_stream_id,
    std::uint32_t capture_date,
    const std::string& stream_slug,
    std::string* error = nullptr) noexcept;

// Retained-authority variant. No pathname component above capture_date is
// resolved: the function duplicates retained_raw_root_fd with openat(".") and
// creates every descendant relative to that exact inode. The retained input
// remains caller-owned.
[[nodiscard]] std::unique_ptr<RawStreamDirectory>
OpenOrCreateRawStreamDirectoryAt(
    int retained_raw_root_fd,
    std::uint32_t source_stream_id,
    std::uint32_t capture_date,
    const std::string& stream_slug,
    std::string* error = nullptr) noexcept;

enum class RawFreshRegistryStageV1 : std::uint8_t {
    kScaffolding = 1U,
    kInit = 2U,
};

// These are the durable coordinator facts to which a fresh namespace mutation
// is bound.  The recovery-attempt identity is deliberately independent of the
// stream-day identity.
struct RawFreshStateAuthorizationV1 final {
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    RawV1Identity stream_day_id{};
    RawV1Identity recovery_attempt{};
    RawFreshRegistryStageV1 registry_stage =
        RawFreshRegistryStageV1::kScaffolding;
    std::uint64_t durable_state_generation = 0U;
};

// Service-owned authorization seam.  A production implementation must retain
// an independently opened shared OFD generation-gate lock for the whole
// namespace call, and Authorizes() must verify the exact durable registry
// generation and stage while that lock is held.  raw_namespace does not claim
// to implement the coordinator or its OFD gate.
class RawFreshMutationAuthorizationGateV1 {
public:
    virtual ~RawFreshMutationAuthorizationGateV1() = default;

    [[nodiscard]] virtual bool Authorizes(
        const RawFreshStateAuthorizationV1& facts) const noexcept = 0;
};

// Production fresh-route entry point. The caller retains an independently
// opened shared OFD generation gate for the entire call; authorization is
// revalidated before any parent mkdir/fsync sequence begins.
[[nodiscard]] std::unique_ptr<RawStreamDirectory>
OpenOrCreateAuthorizedFreshRawStreamDirectory(
    const std::string& raw_root,
    std::uint32_t source_stream_id,
    std::uint32_t capture_date,
    const std::string& stream_slug,
    const RawFreshStateAuthorizationV1& authorization,
    const RawFreshMutationAuthorizationGateV1&
        authorization_gate,
    std::string* error = nullptr) noexcept;

// Production retained-authority counterpart. Authorization is checked before
// any mkdir/fsync, and all namespace mutation is anchored beneath the supplied
// Raw-root inode rather than reopening a configured pathname.
[[nodiscard]] std::unique_ptr<RawStreamDirectory>
OpenOrCreateAuthorizedFreshRawStreamDirectoryAt(
    int retained_raw_root_fd,
    std::uint32_t source_stream_id,
    std::uint32_t capture_date,
    const std::string& stream_slug,
    const RawFreshStateAuthorizationV1& authorization,
    const RawFreshMutationAuthorizationGateV1& authorization_gate,
    std::string* error = nullptr) noexcept;

// The first mutation inside a newly authorized stream-day directory. The
// complete directory must contain no entry other than an already-created,
// exact empty maintenance directory from the same SCAFFOLDING retry. The
// function creates/adopts owner-only maintenance/, fsyncs the directory and
// stream-day parent, and revalidates name-to-inode identity. It must run
// before writer.lease or durable.journal publication.
[[nodiscard]] bool
CreateOrAdoptAuthorizedFreshRawMaintenanceDirectoryV1(
    const RawStreamDirectory& stream_directory,
    const RawFreshStateAuthorizationV1& authorization,
    const RawFreshMutationAuthorizationGateV1&
        authorization_gate,
    std::string* error = nullptr) noexcept;

// Existing-anchor/recovery entry point. It performs secure no-symlink opens
// and validation only: no mkdir, fsync, cleanup, or other namespace mutation.
[[nodiscard]] std::unique_ptr<RawStreamDirectory>
OpenExistingRawStreamDirectory(
    const std::string& raw_root,
    std::uint32_t source_stream_id,
    std::uint32_t capture_date,
    const std::string& stream_slug,
    std::string* error = nullptr) noexcept;

class RawFreshJournalAnchor final {
public:
    ~RawFreshJournalAnchor();

    RawFreshJournalAnchor(const RawFreshJournalAnchor&) = delete;
    RawFreshJournalAnchor& operator=(
        const RawFreshJournalAnchor&) = delete;
    RawFreshJournalAnchor(RawFreshJournalAnchor&&) = delete;
    RawFreshJournalAnchor& operator=(
        RawFreshJournalAnchor&&) = delete;

    [[nodiscard]] const RawFreshStateAuthorizationV1&
    scaffolding_authorization() const noexcept {
        return scaffolding_authorization_;
    }

private:
    friend std::unique_ptr<RawFreshJournalAnchor>
    PublishFreshRawJournalAnchor(
        const RawWriterLease&,
        const RawV1JournalHeaderWire&,
        const RawFreshStateAuthorizationV1&,
        const RawFreshMutationAuthorizationGateV1&,
        std::string*) noexcept;
    friend std::unique_ptr<class RawBootstrapFiles>
    CreateInitialRawSegment(
        const RawWriterLease&,
        RawFreshJournalAnchor&,
        const RawV1SegmentHeaderWire&,
        const RawFreshStateAuthorizationV1&,
        const RawFreshMutationAuthorizationGateV1&,
        std::uint64_t,
        std::string*) noexcept;

    RawFreshJournalAnchor(
        int directory_fd,
        int journal_fd,
        RawV1JournalHeaderWire journal_header,
        RawFreshStateAuthorizationV1
            scaffolding_authorization) noexcept;

    [[nodiscard]] int ReleaseJournalFd() noexcept;

    int directory_fd_ = -1;
    int journal_fd_ = -1;
    RawV1JournalHeaderWire journal_header_{};
    RawFreshStateAuthorizationV1
        scaffolding_authorization_{};
};

class RawBootstrapFiles final {
public:
    ~RawBootstrapFiles();

    RawBootstrapFiles(const RawBootstrapFiles&) = delete;
    RawBootstrapFiles& operator=(const RawBootstrapFiles&) = delete;
    RawBootstrapFiles(RawBootstrapFiles&&) = delete;
    RawBootstrapFiles& operator=(RawBootstrapFiles&&) = delete;

    [[nodiscard]] int ReleaseSegmentFd() noexcept;
    [[nodiscard]] int ReleaseJournalFd() noexcept;

private:
    friend std::unique_ptr<RawBootstrapFiles>
    CreateInitialRawSegment(
        const RawWriterLease&,
        RawFreshJournalAnchor&,
        const RawV1SegmentHeaderWire&,
        const RawFreshStateAuthorizationV1&,
        const RawFreshMutationAuthorizationGateV1&,
        std::uint64_t,
        std::string*) noexcept;
    friend std::unique_ptr<RawBootstrapFiles>
    CreateRotatedRawBootstrap(
        const RawWriterLease&,
        const RawV1SegmentHeaderWire&,
        const RawV1JournalHeaderWire&,
        const RawWalExistingJournalInit&,
        std::uint64_t,
        std::string*) noexcept;

    RawBootstrapFiles(
        int segment_fd,
        int journal_fd) noexcept;

    int segment_fd_ = -1;
    int journal_fd_ = -1;
};

// Under an exact durable SCAFFOLDING authorization, inventory the complete
// stream directory through O_NOATIME and publish only the journal header
// anchor.  This API cannot create a segment or append a marker.
[[nodiscard]] std::unique_ptr<RawFreshJournalAnchor>
PublishFreshRawJournalAnchor(
    const RawWriterLease& lease,
    const RawV1JournalHeaderWire& journal_header,
    const RawFreshStateAuthorizationV1&
        scaffolding_authorization,
    const RawFreshMutationAuthorizationGateV1&
        authorization_gate,
    std::string* error = nullptr) noexcept;

// This is intentionally a separate call.  It requires an exact durable INIT
// authorization for the same route, stream-day, and recovery attempt at a
// strictly higher state generation than the retained SCAFFOLDING anchor.  The
// complete directory is inventoried again before the typed segment mutation.
// No durable marker is written here; RawWalWriter::Initialize appends and
// synchronizes the mandatory header-only marker on the returned descriptors.
[[nodiscard]] std::unique_ptr<RawBootstrapFiles>
CreateInitialRawSegment(
    const RawWriterLease& lease,
    RawFreshJournalAnchor& journal_anchor,
    const RawV1SegmentHeaderWire& segment_header,
    const RawFreshStateAuthorizationV1& init_authorization,
    const RawFreshMutationAuthorizationGateV1&
        authorization_gate,
    std::uint64_t segment_preallocation_bytes,
    std::string* error = nullptr) noexcept;

// Opens and validates the existing journal through its retained final inode,
// including the exact preceding sealed marker, then publishes one next normal
// segment from a deterministic typed temporary. The returned descriptors are
// ready for a RawWalWriter configured with kExistingJournal and
// headers_already_persisted=true.
[[nodiscard]] std::unique_ptr<RawBootstrapFiles>
CreateRotatedRawBootstrap(
    const RawWriterLease& lease,
    const RawV1SegmentHeaderWire& next_segment_header,
    const RawV1JournalHeaderWire& expected_journal_header,
    const RawWalExistingJournalInit& existing_journal,
    std::uint64_t segment_preallocation_bytes,
    std::string* error = nullptr) noexcept;

}  // namespace l2flow::ingress
