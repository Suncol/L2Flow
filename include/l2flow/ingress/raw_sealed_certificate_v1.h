#pragma once

#include "l2flow/ingress/raw_manifest_v1.h"
#include "l2flow/ingress/raw_wal_writer.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace l2flow::ingress {

inline constexpr std::uint32_t
    kSealedRawCertificateV1SchemaVersion = 1U;
inline constexpr std::size_t
    kSealedRawCertificateV1MaximumBytes = 8192U;
inline constexpr std::size_t
    kSealedRawCertificateV1SchemaBytes = 5246U;
inline constexpr std::string_view
    kSealedRawCertificateV1SchemaSha256Hex =
        "c559c191e53e9a56d357f9314ebb2e39"
        "8fe5ba32523e7e25ccafb92df73e9534";

struct SealedRawCertificateCursorV1 final {
    std::uint32_t segment_sequence = 0U;
    std::uint64_t global_wal_pos = 0U;
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t segment_offset = 0U;
};

// Self-contained terminal-frontier certificate. It deliberately contains no
// recovery attempt, writer/executor instance, reserve cycle, report locator,
// or mutable whole-manifest hash. Its bytes are determined solely by the
// validated Raw namespace, journal header, terminal closed entry, exact
// accepted seal marker, and immutable closed frontier.
struct SealedRawCertificateV1 final {
    std::uint32_t schema_version =
        kSealedRawCertificateV1SchemaVersion;
    RawManifestNamespaceV1 namespace_identity{};
    RawV1Digest journal_header_sha256{};

    SealedRawCertificateCursorV1 terminal_append_cursor{};
    SealedRawCertificateCursorV1 terminal_durable_cursor{};

    std::uint32_t last_segment_sequence = 0U;
    std::uint32_t last_segment_flags = 0U;
    std::uint64_t last_segment_base_wal_pos = 0U;
    std::uint64_t last_segment_logical_length = 0U;
    RawV1Digest last_segment_sha256{};

    RawV1DurableMarkerWire accepted_sealed_marker_bytes{};
    RawV1Digest accepted_sealed_marker_sha256{};

    std::uint64_t closed_entry_count = 0U;
    RawV1Digest closed_prefix_sha256{};
};

enum class SealedRawCertificateV1Error : std::uint8_t;

class BuiltSealedRawCertificateV1 final {
public:
    ~BuiltSealedRawCertificateV1() = default;

    BuiltSealedRawCertificateV1(
        const BuiltSealedRawCertificateV1&) = delete;
    BuiltSealedRawCertificateV1& operator=(
        const BuiltSealedRawCertificateV1&) = delete;
    BuiltSealedRawCertificateV1(
        BuiltSealedRawCertificateV1&&) = delete;
    BuiltSealedRawCertificateV1& operator=(
        BuiltSealedRawCertificateV1&&) = delete;

    [[nodiscard]] const SealedRawCertificateV1&
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
    certificate_sha256() const noexcept {
        return certificate_sha256_;
    }

private:
    friend SealedRawCertificateV1Error
    BuildSealedRawCertificateCapabilityV1(
        const RawV1JournalHeaderWire&,
        const RawManifestV1&,
        const RawWalSinkIdentityV1&,
        const RawWalWriterSnapshot&,
        std::unique_ptr<
            BuiltSealedRawCertificateV1>*) noexcept;

    BuiltSealedRawCertificateV1(
        SealedRawCertificateV1 model,
        std::string canonical_jcs,
        std::string filename,
        RawV1Digest certificate_sha256) noexcept
        : model_(std::move(model)),
          canonical_jcs_(std::move(canonical_jcs)),
          filename_(std::move(filename)),
          certificate_sha256_(certificate_sha256) {}

    SealedRawCertificateV1 model_{};
    std::string canonical_jcs_;
    std::string filename_;
    RawV1Digest certificate_sha256_{};
};

enum class SealedRawCertificateV1Error : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kUnsupportedSchemaVersion,
    kInvalidNamespace,
    kInvalidJournalHeader,
    kJournalNamespaceMismatch,
    kInvalidManifest,
    kManifestNotTerminal,
    kInvalidTerminalMarker,
    kTerminalCursorMismatch,
    kTerminalSegmentMismatch,
    kHashMismatch,
    kInvalidCanonicalJson,
    kFilenameInvalid,
    kEncodedSizeExceeded,
    kAllocationFailure,
};

[[nodiscard]] std::string_view
SealedRawCertificateV1ErrorName(
    SealedRawCertificateV1Error error) noexcept;

// Constructs and cross-validates the certificate from already completed
// clean-stop barriers. `manifest` must contain only closed entries. The final
// sink snapshot and identity are proofs that the terminal writer sealed and
// closed at the exact marker-bound cursor; they are not encoded into the
// certificate as mutable runtime authority.
[[nodiscard]] SealedRawCertificateV1Error
BuildSealedRawCertificateV1(
    const RawV1JournalHeaderWire& journal_header_bytes,
    const RawManifestV1& manifest,
    const RawWalSinkIdentityV1& final_sink_identity,
    const RawWalWriterSnapshot& final_wal,
    SealedRawCertificateV1* output) noexcept;

// Produces the only certificate capability accepted by the POSIX publisher.
// Its private construction freezes the fully cross-validated model together
// with its exact canonical bytes, deterministic basename and content hash.
// Parsed or caller-assembled models remain useful for read-only validation
// but cannot authorize publication.
[[nodiscard]] SealedRawCertificateV1Error
BuildSealedRawCertificateCapabilityV1(
    const RawV1JournalHeaderWire& journal_header_bytes,
    const RawManifestV1& manifest,
    const RawWalSinkIdentityV1& final_sink_identity,
    const RawWalWriterSnapshot& final_wal,
    std::unique_ptr<
        BuiltSealedRawCertificateV1>* output) noexcept;

[[nodiscard]] SealedRawCertificateV1Error
ValidateSealedRawCertificateV1(
    const SealedRawCertificateV1& certificate) noexcept;

// RFC 8785-compatible canonical JSON for this ASCII-key, integer/string-only
// schema. uint64 values are decimal strings; identities/digests/wire bytes
// are fixed-width lowercase hexadecimal. No BOM or trailing newline is
// emitted. Output changes only on success.
[[nodiscard]] SealedRawCertificateV1Error
EncodeSealedRawCertificateV1Jcs(
    const SealedRawCertificateV1& certificate,
    std::string* output) noexcept;

// Strictly parses the exact canonical form emitted above. Reordered,
// duplicate, unknown or missing fields; non-canonical decimal/hex; trailing
// bytes; and redundant frontier/marker facts that disagree with the terminal
// model are rejected. Output changes only on success.
[[nodiscard]] SealedRawCertificateV1Error
ParseSealedRawCertificateV1Jcs(
    std::string_view exact_bytes,
    SealedRawCertificateV1* output) noexcept;

// Returns the deterministic basename:
// sealed-raw-<stream-day-id>-<closed-prefix-sha256>.json
[[nodiscard]] SealedRawCertificateV1Error
SealedRawCertificateV1Filename(
    const SealedRawCertificateV1& certificate,
    std::string* output) noexcept;

}  // namespace l2flow::ingress
