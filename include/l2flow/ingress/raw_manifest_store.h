#pragma once

#include "l2flow/ingress/raw_manifest_v1.h"
#include "l2flow/ingress/raw_writer_lease.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace l2flow::ingress {

// RawManifestV1 freezes one mutable current pathname. The Phase 2 contract
// does not define a historical-manifest pathname grammar; closed history is
// carried by the append-only closed_entries/frontier inside this file.
inline constexpr char kRawManifestCurrentFilename[] = "manifest.json";

// This is an implementation-private, deterministic typed publication
// candidate. It is never a valid reader-visible or historical manifest name.
inline constexpr char kRawManifestTemporaryFilename[] =
    ".manifest.json.raw-manifest-v1.tmp";

enum class RawManifestFilenameKind : std::uint8_t {
    kInvalid = 0U,
    kCurrent,
};

[[nodiscard]] RawManifestFilenameKind ClassifyRawManifestFilename(
    std::string_view name) noexcept;

enum class RawManifestStoreError : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kUnsupportedFilename,
    kUnsafeDirectory,
    kNotFound,
    kUnsafeFile,
    kFileTooLarge,
    kReadFailure,
    kInvalidJson,
    kNonCanonicalJson,
    kInvalidModel,
    kNamespaceMismatch,
    kAmbiguousTemporary,
    kWriteFailure,
    kSyncFailure,
    kPublishConflict,
    kReadbackFailure,
    kAllocationFailure,
};

[[nodiscard]] std::string_view RawManifestStoreErrorName(
    RawManifestStoreError error) noexcept;

// Strictly decodes the exact RawManifestV1 JCS representation emitted by
// EncodeRawManifestJcs. It rejects whitespace, duplicate/unknown/missing
// members, non-canonical escaping/order, wrong JSON types, non-canonical
// decimal strings, and non-lowercase/fixed-width hexadecimal. The decoded
// model is validated and then re-encoded; the input must be byte-identical to
// that canonical encoding. Outputs are changed only on success.
[[nodiscard]] RawManifestStoreError ParseRawManifestJcs(
    std::string_view bytes,
    RawManifestV1* manifest,
    RawManifestV1Error* model_error = nullptr) noexcept;

// Reads current `manifest.json` through a retained O_NOATIME directory alias
// and one O_RDONLY|O_NOFOLLOW|O_NOATIME descriptor. The file must be a stable,
// owner-only, singly-linked regular file. maximum_bytes is a caller-supplied
// operational bound and must be non-zero. The namespace must match the
// journal/segment identity already established by the caller. When
// append_only_predecessor is supplied, the loaded current manifest must also
// be a generation-increasing append-only successor of that trusted frontier.
[[nodiscard]] RawManifestStoreError LoadCurrentRawManifestAt(
    int retained_directory_fd,
    const RawManifestNamespaceV1& expected_namespace,
    std::size_t maximum_bytes,
    RawManifestV1* manifest,
    std::string* canonical_bytes = nullptr,
    RawManifestV1Error* model_error = nullptr,
    std::string* error = nullptr,
    const RawManifestV1* append_only_predecessor = nullptr) noexcept;

// Publishes one new current manifest while the caller retains the namespace's
// exclusive RawWriterLease. A missing current name is published with
// RENAME_NOREPLACE; an existing current name is replaced atomically only
// after its decoded manifest proves the new model is an append-only,
// generation-increasing successor. The sequence is:
//
//   create typed O_EXCL temp -> write -> fsync(temp)
//   -> atomic no-replace/replace -> fsync(directory) -> strict readback
//
// A complete byte-identical temp from an interrupted retry may be adopted.
// Any partial, mismatching, unsafe, or final+unexpected-temp state fails
// closed and is not cleaned up by this layer.
[[nodiscard]] RawManifestStoreError PublishCurrentRawManifest(
    const RawWriterLease& lease,
    const RawManifestNamespaceV1& expected_namespace,
    const RawManifestV1& manifest,
    std::size_t maximum_bytes,
    RawManifestV1Error* model_error = nullptr,
    std::string* error = nullptr) noexcept;

}  // namespace l2flow::ingress
