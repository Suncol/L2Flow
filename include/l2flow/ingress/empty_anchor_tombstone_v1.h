#pragma once

#include "l2flow/ingress/raw_v1.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace l2flow::ingress {

inline constexpr std::uint32_t
    kEmptyAnchorTombstoneV1SchemaVersion = 1U;
inline constexpr std::size_t
    kEmptyAnchorTombstoneV1MaximumBytes = 4096U;

// Updated together with schemas/empty_anchor_tombstone_v1.json.  These
// constants are verified byte-for-byte by test_phase2_raw_schema.
inline constexpr std::size_t
    kEmptyAnchorTombstoneV1SchemaBytes = 2087U;
inline constexpr std::string_view
    kEmptyAnchorTombstoneV1SchemaSha256Hex =
        "02b5a2f35c950bcf17e655ba640bdb06"
        "14063aaaff98ce6fb059d05f363137f1";

struct EmptyAnchorNamespaceV1 final {
    std::uint32_t capture_date = 0U;
    std::uint32_t source_stream_id = 0U;
    RawV1Identity stream_day_id{};
};

// Evidence supplied by the namespace scanner.  The codec checks all scalar
// facts and validates the exact Raw V1 journal header.  A POSIX publisher
// must additionally revalidate the retained journal/directory descriptors
// while holding the applicable coordinator authorization.
struct EmptyAnchorObservationV1 final {
    RawV1JournalHeaderWire journal_header_bytes{};
    std::uint64_t journal_logical_size = 0U;
    std::uint64_t marker_count = 0U;
    std::uint64_t segment_count = 0U;
    std::uint64_t record_count = 0U;
};

// Self-contained certificate of the immutable first-anchor state.  It
// deliberately excludes recovery attempts, reserve cycles, executors, and
// report/archive locators.
struct EmptyAnchorTombstoneV1 final {
    std::uint32_t schema_version =
        kEmptyAnchorTombstoneV1SchemaVersion;
    EmptyAnchorNamespaceV1 namespace_identity{};
    RawV1Digest journal_header_sha256{};
    std::uint64_t marker_count = 0U;
    std::uint64_t segment_count = 0U;
    std::uint64_t record_count = 0U;
};

enum class EmptyAnchorTombstoneV1Error : std::uint8_t;

class BuiltEmptyAnchorTombstoneV1 final {
public:
    ~BuiltEmptyAnchorTombstoneV1() = default;

    BuiltEmptyAnchorTombstoneV1(
        const BuiltEmptyAnchorTombstoneV1&) = delete;
    BuiltEmptyAnchorTombstoneV1& operator=(
        const BuiltEmptyAnchorTombstoneV1&) = delete;
    BuiltEmptyAnchorTombstoneV1(
        BuiltEmptyAnchorTombstoneV1&&) = delete;
    BuiltEmptyAnchorTombstoneV1& operator=(
        BuiltEmptyAnchorTombstoneV1&&) = delete;

    [[nodiscard]] const EmptyAnchorTombstoneV1&
    model() const noexcept {
        return model_;
    }
    [[nodiscard]] std::string_view
    canonical_jcs() const noexcept {
        return canonical_jcs_;
    }
    [[nodiscard]] std::string_view
    filename() const noexcept {
        return filename_;
    }
    [[nodiscard]] const RawV1Digest&
    tombstone_sha256() const noexcept {
        return tombstone_sha256_;
    }

private:
    friend EmptyAnchorTombstoneV1Error
    BuildEmptyAnchorTombstoneCapabilityV1(
        const EmptyAnchorObservationV1&,
        std::unique_ptr<
            BuiltEmptyAnchorTombstoneV1>*) noexcept;

    BuiltEmptyAnchorTombstoneV1(
        EmptyAnchorTombstoneV1 model,
        std::string canonical_jcs,
        std::string filename,
        RawV1Digest tombstone_sha256) noexcept
        : model_(std::move(model)),
          canonical_jcs_(std::move(canonical_jcs)),
          filename_(std::move(filename)),
          tombstone_sha256_(tombstone_sha256) {}

    EmptyAnchorTombstoneV1 model_{};
    std::string canonical_jcs_;
    std::string filename_;
    RawV1Digest tombstone_sha256_{};
};

enum class EmptyAnchorTombstoneV1Error : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kUnsupportedSchemaVersion,
    kInvalidNamespace,
    kInvalidJournalHeader,
    kNotEmptyAnchor,
    kHashInvalid,
    kInvalidCanonicalJson,
    kFilenameInvalid,
    kEncodedSizeExceeded,
    kAllocationFailure,
};

[[nodiscard]] std::string_view
EmptyAnchorTombstoneV1ErrorName(
    EmptyAnchorTombstoneV1Error error) noexcept;

[[nodiscard]] EmptyAnchorTombstoneV1Error
BuildEmptyAnchorTombstoneV1(
    const EmptyAnchorObservationV1& observation,
    EmptyAnchorTombstoneV1* output) noexcept;

// Freezes the validated model, exact canonical bytes, deterministic basename,
// and content digest into a move-only publication input.
[[nodiscard]] EmptyAnchorTombstoneV1Error
BuildEmptyAnchorTombstoneCapabilityV1(
    const EmptyAnchorObservationV1& observation,
    std::unique_ptr<
        BuiltEmptyAnchorTombstoneV1>* output) noexcept;

[[nodiscard]] EmptyAnchorTombstoneV1Error
ValidateEmptyAnchorTombstoneV1(
    const EmptyAnchorTombstoneV1& tombstone) noexcept;

// Emits an RFC 8785-compatible, ASCII-key canonical object. uint64 values
// are quoted canonical decimal strings; identities and digests are
// fixed-width lowercase hexadecimal. No BOM/newline is emitted.
[[nodiscard]] EmptyAnchorTombstoneV1Error
EncodeEmptyAnchorTombstoneV1Jcs(
    const EmptyAnchorTombstoneV1& tombstone,
    std::string* output) noexcept;

// Accepts only the exact canonical form emitted above. Unknown, missing,
// duplicate, reordered, non-canonical, or trailing fields are rejected.
[[nodiscard]] EmptyAnchorTombstoneV1Error
ParseEmptyAnchorTombstoneV1Jcs(
    std::string_view exact_bytes,
    EmptyAnchorTombstoneV1* output) noexcept;

// empty-anchor-<32-lowercase-hex-stream-day-id>.json
[[nodiscard]] EmptyAnchorTombstoneV1Error
EmptyAnchorTombstoneV1Filename(
    const EmptyAnchorTombstoneV1& tombstone,
    std::string* output) noexcept;

}  // namespace l2flow::ingress
