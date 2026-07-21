#include "l2flow/ingress/injected_raw_reader.h"

#include "l2flow/common/crc32c.h"

#include <algorithm>
#include <limits>
#include <map>
#include <new>
#include <set>
#include <tuple>
#include <utility>

namespace l2flow::ingress {
namespace {

[[nodiscard]] std::uint32_t LoadU32Le(
    std::span<const std::byte> bytes,
    std::size_t offset) noexcept {
    std::uint32_t value = 0U;
    for (std::size_t index = 0U; index < 4U; ++index) {
        value |=
            static_cast<std::uint32_t>(
                std::to_integer<std::uint8_t>(
                    bytes[offset + index]))
            << (index * 8U);
    }
    return value;
}

[[nodiscard]] bool IsZero(
    std::span<const std::byte> bytes) noexcept {
    return std::all_of(
        bytes.begin(), bytes.end(), [](std::byte value) {
            return value == std::byte{0U};
        });
}

[[nodiscard]] bool U64ToSize(
    std::uint64_t value,
    std::size_t* result) noexcept {
    if (result == nullptr) {
        return false;
    }
    if constexpr (sizeof(std::size_t) <
                  sizeof(std::uint64_t)) {
        if (value >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max())) {
            return false;
        }
    }
    *result = static_cast<std::size_t>(value);
    return true;
}

void SetError(
    InjectedRawSegmentScanResult* result,
    InjectedRawV1Error error,
    std::uint64_t error_offset) noexcept {
    if (result == nullptr) {
        return;
    }
    result->error = error;
    result->error_offset = error_offset;
}

using ParentBaseKey = std::tuple<
    std::uint32_t,
    std::uint32_t,
    RawV1Identity,
    std::uint64_t,
    std::uint64_t,
    std::uint64_t>;

[[nodiscard]] ParentBaseKey MakeParentBaseKey(
    const InjectedRawParentLocator& locator) {
    return ParentBaseKey{
        locator.capture_date,
        locator.source_stream_id,
        locator.stream_day_id,
        locator.ingress_sequence,
        locator.record_start_wal_pos,
        locator.record_end_wal_pos};
}

void IncludeParentRange(
    const InjectedRawParentLocator& locator,
    bool* has_parent,
    std::uint64_t* parent_wal_begin,
    std::uint64_t* parent_wal_end) noexcept {
    if (has_parent == nullptr ||
        parent_wal_begin == nullptr ||
        parent_wal_end == nullptr) {
        return;
    }
    if (!*has_parent) {
        *has_parent = true;
        *parent_wal_begin = locator.record_start_wal_pos;
        *parent_wal_end = locator.record_end_wal_pos;
        return;
    }
    *parent_wal_begin = std::min(
        *parent_wal_begin, locator.record_start_wal_pos);
    *parent_wal_end = std::max(
        *parent_wal_end, locator.record_end_wal_pos);
}

[[nodiscard]] bool VendorHeadMatches(
    std::span<const std::byte> vendor_head,
    std::uint32_t vendor_message_size) noexcept {
    return vendor_head.size() == kVendorMessageHeadBytes &&
        std::to_integer<std::uint8_t>(vendor_head[0U]) ==
            kVendorMessageHeadBytes &&
        LoadU32Le(vendor_head, 1U) == vendor_message_size;
}

}  // namespace

struct InjectedRawRecordViewFactory final {
    [[nodiscard]] static InjectedRawRecordView Make(
        const std::shared_ptr<const std::vector<std::byte>>& owner,
        const InjectedRawRecordHeaderV1& header,
        const InjectedRawParentLocator& parent,
        std::optional<InjectedRawMutationEvidence> mutation,
        std::span<const std::byte> vendor_head,
        std::span<const std::byte> vendor_body,
        std::uint64_t record_start_offset,
        std::uint64_t record_end_offset) noexcept {
        return InjectedRawRecordView{
            owner,
            header,
            parent,
            std::move(mutation),
            vendor_head,
            vendor_body,
            record_start_offset,
            record_end_offset};
    }
};

InjectedRawRecordView::InjectedRawRecordView(
    std::shared_ptr<const std::vector<std::byte>> owner,
    InjectedRawRecordHeaderV1 header,
    InjectedRawParentLocator parent,
    std::optional<InjectedRawMutationEvidence> mutation,
    std::span<const std::byte> vendor_head,
    std::span<const std::byte> vendor_body,
    std::uint64_t record_start_offset,
    std::uint64_t record_end_offset) noexcept
    : owner_(std::move(owner)),
      header_(header),
      parent_(parent),
      mutation_(std::move(mutation)),
      vendor_head_(vendor_head),
      vendor_body_(vendor_body),
      record_start_offset_(record_start_offset),
      record_end_offset_(record_end_offset) {}

InjectedRawSegmentScanResult ScanInjectedRawSegmentV1(
    std::shared_ptr<const std::vector<std::byte>> buffer)
    noexcept {
    InjectedRawSegmentScanResult result;
    if (buffer == nullptr) {
        SetError(
            &result, InjectedRawV1Error::kNullBuffer, 0U);
        return result;
    }
    if (buffer->size() <
        kInjectedRawV1SegmentHeaderBytes) {
        SetError(
            &result,
            InjectedRawV1Error::kInvalidWireSize,
            static_cast<std::uint64_t>(buffer->size()));
        return result;
    }
    if (buffer->size() >
        kInjectedRawV1MaximumLogicalBytes) {
        SetError(
            &result,
            InjectedRawV1Error::kResourceLimitExceeded,
            0U);
        return result;
    }

    const std::span<const std::byte> all(
        buffer->data(), buffer->size());
    const InjectedRawV1Error segment_error =
        DecodeInjectedRawSegmentHeaderV1(
            all.first(kInjectedRawV1SegmentHeaderBytes),
            &result.segment);
    if (segment_error != InjectedRawV1Error::kNone) {
        SetError(&result, segment_error, 0U);
        return result;
    }
    if (result.segment.logical_size != buffer->size()) {
        SetError(
            &result, InjectedRawV1Error::kInvalidSize, 0U);
        return result;
    }

    std::size_t dropped_offset = 0U;
    std::size_t record_count = 0U;
    std::size_t dropped_count = 0U;
    if (!U64ToSize(
            result.segment.dropped_locators_offset,
            &dropped_offset) ||
        !U64ToSize(
            result.segment.record_count, &record_count) ||
        !U64ToSize(
            result.segment.dropped_locator_count,
            &dropped_count)) {
        SetError(
            &result, InjectedRawV1Error::kSizeOverflow, 0U);
        return result;
    }

    std::size_t cursor =
        kInjectedRawV1SegmentHeaderBytes;
    try {
        result.records.reserve(record_count);
        result.dropped_locators.reserve(dropped_count);
        std::map<ParentBaseKey, std::set<std::uint32_t>>
            occurrences;
        std::set<ParentBaseKey> dropped_keys;
        bool has_parent = false;
        std::uint64_t parent_wal_begin = 0U;
        std::uint64_t parent_wal_end = 0U;

        for (std::size_t index = 0U;
             index < record_count;
             ++index) {
            if (cursor > dropped_offset ||
                dropped_offset - cursor <
                    kInjectedRawV1RecordHeaderBytes) {
                SetError(
                    &result,
                    InjectedRawV1Error::kInvalidSize,
                    static_cast<std::uint64_t>(cursor));
                return result;
            }
            InjectedRawRecordHeaderV1 header;
            const InjectedRawV1Error header_error =
                DecodeInjectedRawRecordHeaderV1(
                    all.subspan(
                        cursor,
                        kInjectedRawV1RecordHeaderBytes),
                    &header);
            if (header_error !=
                InjectedRawV1Error::kNone) {
                SetError(
                    &result,
                    header_error,
                    static_cast<std::uint64_t>(cursor));
                return result;
            }
            const std::uint64_t expected_sequence =
                static_cast<std::uint64_t>(index) + 1U;
            if (header.synthetic_ingress_sequence !=
                expected_sequence) {
                SetError(
                    &result,
                    InjectedRawV1Error::kInvalidSequence,
                    static_cast<std::uint64_t>(cursor));
                return result;
            }
            const std::size_t record_size =
                static_cast<std::size_t>(header.record_size);
            if (record_size > dropped_offset - cursor) {
                SetError(
                    &result,
                    InjectedRawV1Error::kInvalidSize,
                    static_cast<std::uint64_t>(cursor));
                return result;
            }
            InjectedRawRecordLayoutV1 layout{};
            const InjectedRawV1Error layout_error =
                ComputeInjectedRawRecordLayoutV1(
                    header.vendor_body_size, &layout);
            if (layout_error !=
                    InjectedRawV1Error::kNone ||
                layout.record_size != header.record_size) {
                SetError(
                    &result,
                    layout_error ==
                            InjectedRawV1Error::kNone
                        ? InjectedRawV1Error::kInvalidSize
                        : layout_error,
                    static_cast<std::uint64_t>(cursor));
                return result;
            }

            const std::size_t parent_offset =
                cursor + kInjectedRawV1RecordHeaderBytes;
            const std::size_t vendor_head_offset =
                parent_offset +
                kInjectedRawV1ParentLocatorBytes;
            const std::size_t vendor_body_offset =
                vendor_head_offset +
                kVendorMessageHeadBytes;
            const std::size_t padding_offset =
                vendor_body_offset +
                static_cast<std::size_t>(
                    header.vendor_body_size);
            const std::size_t trailer_offset =
                padding_offset +
                static_cast<std::size_t>(
                    layout.padding_size);
            if (trailer_offset +
                    kInjectedRawV1RecordTrailerBytes !=
                cursor + record_size) {
                SetError(
                    &result,
                    InjectedRawV1Error::kInvalidSize,
                    static_cast<std::uint64_t>(cursor));
                return result;
            }

            InjectedRawParentLocatorV1 parent;
            const InjectedRawV1Error parent_error =
                DecodeInjectedRawParentLocatorV1(
                    all.subspan(
                        parent_offset,
                        kInjectedRawV1ParentLocatorBytes),
                    &parent);
            if (parent_error !=
                InjectedRawV1Error::kNone) {
                SetError(
                    &result,
                    parent_error,
                    static_cast<std::uint64_t>(
                        parent_offset));
                return result;
            }
            if (header.source_stream_id !=
                    parent.locator.source_stream_id ||
                header.capture_date !=
                    parent.locator.capture_date) {
                SetError(
                    &result,
                    InjectedRawV1Error::
                        kInvalidParentLocator,
                    static_cast<std::uint64_t>(
                        parent_offset));
                return result;
            }

            const std::span<const std::byte> vendor_head =
                all.subspan(
                    vendor_head_offset,
                    kVendorMessageHeadBytes);
            const std::span<const std::byte> vendor_body =
                all.subspan(
                    vendor_body_offset,
                    header.vendor_body_size);
            if (!VendorHeadMatches(
                    vendor_head,
                    header.vendor_message_size)) {
                SetError(
                    &result,
                    InjectedRawV1Error::kInvalidVendorHead,
                    static_cast<std::uint64_t>(
                        vendor_head_offset));
                return result;
            }
            const std::uint32_t payload_crc =
                l2flow::common::ComputeCrc32c(
                    all.subspan(
                        parent_offset,
                        kInjectedRawV1ParentLocatorBytes +
                            header.vendor_message_size));
            if (payload_crc != header.payload_crc32c) {
                SetError(
                    &result,
                    InjectedRawV1Error::
                        kPayloadCrcMismatch,
                    static_cast<std::uint64_t>(
                        parent_offset));
                return result;
            }
            if (!IsZero(all.subspan(
                    padding_offset, layout.padding_size))) {
                SetError(
                    &result,
                    InjectedRawV1Error::kNonzeroPadding,
                    static_cast<std::uint64_t>(
                        padding_offset));
                return result;
            }

            std::optional<InjectedRawMutationEvidence>
                mutation;
            if (header.mutation_kind !=
                RawLogicalMutationKind::kNone) {
                const std::size_t mutation_offset =
                    static_cast<std::size_t>(
                        header.
                            mutation_vendor_body_offset);
                if (mutation_offset >= vendor_body.size() ||
                    vendor_body[mutation_offset] !=
                        header.mutation_after) {
                    SetError(
                        &result,
                        InjectedRawV1Error::
                            kInvalidMutation,
                        static_cast<std::uint64_t>(
                            vendor_body_offset));
                    return result;
                }
                mutation = InjectedRawMutationEvidence{
                    header.mutation_kind,
                    header.mutation_vendor_body_offset,
                    header.mutation_before,
                    header.mutation_after};
            }

            InjectedRawRecordTrailerV1 trailer;
            const InjectedRawV1Error trailer_error =
                DecodeInjectedRawRecordTrailerV1(
                    all.subspan(
                        trailer_offset,
                        kInjectedRawV1RecordTrailerBytes),
                    &trailer);
            if (trailer_error !=
                    InjectedRawV1Error::kNone ||
                trailer.record_size != header.record_size ||
                trailer.synthetic_ingress_sequence !=
                    header.synthetic_ingress_sequence) {
                SetError(
                    &result,
                    InjectedRawV1Error::kTrailerMismatch,
                    static_cast<std::uint64_t>(
                        trailer_offset));
                return result;
            }

            auto& parent_occurrences =
                occurrences[MakeParentBaseKey(
                    parent.locator)];
            if (!parent_occurrences.insert(
                    parent.locator.occurrence)
                    .second) {
                SetError(
                    &result,
                    InjectedRawV1Error::
                        kDuplicateParentLocator,
                    static_cast<std::uint64_t>(
                        parent_offset));
                return result;
            }
            IncludeParentRange(
                parent.locator,
                &has_parent,
                &parent_wal_begin,
                &parent_wal_end);
            const std::uint64_t record_start =
                static_cast<std::uint64_t>(cursor);
            cursor += record_size;
            result.records.push_back(
                InjectedRawRecordViewFactory::Make(
                    buffer,
                    header,
                    parent.locator,
                    std::move(mutation),
                    vendor_head,
                    vendor_body,
                    record_start,
                    static_cast<std::uint64_t>(cursor)));
        }

        if (cursor != dropped_offset) {
            SetError(
                &result,
                InjectedRawV1Error::kDroppedTableMismatch,
                static_cast<std::uint64_t>(cursor));
            return result;
        }
        for (std::size_t index = 0U;
             index < dropped_count;
             ++index) {
            InjectedRawParentLocatorV1 dropped;
            const InjectedRawV1Error dropped_error =
                DecodeInjectedRawParentLocatorV1(
                    all.subspan(
                        cursor,
                        kInjectedRawV1ParentLocatorBytes),
                    &dropped);
            if (dropped_error !=
                InjectedRawV1Error::kNone) {
                SetError(
                    &result,
                    dropped_error,
                    static_cast<std::uint64_t>(cursor));
                return result;
            }
            const ParentBaseKey key =
                MakeParentBaseKey(dropped.locator);
            if (dropped.locator.occurrence != 1U ||
                occurrences.contains(key)) {
                SetError(
                    &result,
                    InjectedRawV1Error::
                        kInvalidParentOccurrence,
                    static_cast<std::uint64_t>(cursor));
                return result;
            }
            if (!dropped_keys.insert(key).second) {
                SetError(
                    &result,
                    InjectedRawV1Error::
                        kDuplicateParentLocator,
                    static_cast<std::uint64_t>(cursor));
                return result;
            }
            result.dropped_locators.push_back(
                dropped.locator);
            IncludeParentRange(
                dropped.locator,
                &has_parent,
                &parent_wal_begin,
                &parent_wal_end);
            cursor += kInjectedRawV1ParentLocatorBytes;
        }
        if (cursor != buffer->size()) {
            SetError(
                &result,
                InjectedRawV1Error::kDroppedTableMismatch,
                static_cast<std::uint64_t>(cursor));
            return result;
        }

        for (const auto& [key, values] : occurrences) {
            static_cast<void>(key);
            std::uint32_t expected = 1U;
            for (std::uint32_t occurrence : values) {
                if (occurrence != expected) {
                    SetError(
                        &result,
                        InjectedRawV1Error::
                            kInvalidParentOccurrence,
                        result.segment.
                            dropped_locators_offset);
                    return result;
                }
                if (expected ==
                    std::numeric_limits<std::uint32_t>::max()) {
                    if (std::next(values.find(occurrence)) !=
                        values.end()) {
                        SetError(
                            &result,
                            InjectedRawV1Error::
                                kInvalidParentOccurrence,
                            result.segment.
                                dropped_locators_offset);
                        return result;
                    }
                } else {
                    ++expected;
                }
            }
        }
        if ((has_parent &&
             (result.segment.parent_wal_begin !=
                  parent_wal_begin ||
              result.segment.parent_wal_end !=
                  parent_wal_end)) ||
            (!has_parent &&
             (result.segment.parent_wal_begin != 0U ||
              result.segment.parent_wal_end != 0U))) {
            SetError(
                &result,
                InjectedRawV1Error::kInvalidParentLocator,
                0U);
            return result;
        }
    } catch (const std::bad_alloc&) {
        SetError(
            &result,
            InjectedRawV1Error::kAllocationFailure,
            static_cast<std::uint64_t>(cursor));
        return result;
    } catch (...) {
        SetError(
            &result,
            InjectedRawV1Error::kAllocationFailure,
            static_cast<std::uint64_t>(cursor));
        return result;
    }
    return result;
}

}  // namespace l2flow::ingress
