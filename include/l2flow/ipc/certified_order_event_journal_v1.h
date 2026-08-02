#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/ipc/certified_order_event_wire_v1.h"
#include "l2flow/ipc/instrument_derived_event_history_v1.h"

#include <cstdint>
#include <memory>
#include <span>
#include <string_view>

namespace l2flow::ipc {

struct CertifiedOrderEventJournalConfigV1 final {
    common::Identity128 run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
    std::uint64_t event_capacity = 0U;
    // Zero derives the exact checked full-day mapping from event_capacity.
    // A nonzero value is an additional operator bound; there is no arbitrary
    // production cap below a correctly configured full-day capacity.
    std::uint64_t maximum_mapping_bytes = 0U;
    // Physical backing is reserved only as publication approaches a chunk.
    std::uint64_t lazy_commit_chunk_bytes =
        64ULL * 1024ULL * 1024ULL;
};

struct CertifiedOrderEventJournalSessionV1 final {
    common::Identity128 run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
    std::uint64_t event_capacity = 0U;
    std::uint64_t total_mapping_bytes = 0U;

    [[nodiscard]] friend bool operator==(
        const CertifiedOrderEventJournalSessionV1&,
        const CertifiedOrderEventJournalSessionV1&) noexcept =
        default;
};

enum class CertifiedOrderEventJournalCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kLayoutOverflow,
    kMappingCreateFailed,
    kReadOnlyHandleFailed,
    kSealFailed,
    kResourceExhausted,
    kUnexpectedFailure,
};

enum class CertifiedOrderEventJournalPublishErrorV1
    : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kCanonicalSequence,
    kEventCapacity,
    kBackingCommitFailed,
    kProjectionError,
    kPublicationInvariant,
    kFailed,
};

[[nodiscard]] std::string_view
CertifiedOrderEventJournalCreateErrorNameV1(
    CertifiedOrderEventJournalCreateErrorV1 error) noexcept;
[[nodiscard]] std::string_view
CertifiedOrderEventJournalPublishErrorNameV1(
    CertifiedOrderEventJournalPublishErrorV1 error) noexcept;

// Serial writer. Readers never enter this object. A fallible backing-space
// reservation is performed by EnsureWritable before a stateful Event
// projection. PublishCanonicalTick then performs only bounded validation and
// atomic stores. Any error permanently freezes writes while the previously
// published header prefix remains readable.
class CertifiedOrderEventJournalProducerV1 final {
public:
    CertifiedOrderEventJournalProducerV1(
        const CertifiedOrderEventJournalProducerV1&) = delete;
    CertifiedOrderEventJournalProducerV1& operator=(
        const CertifiedOrderEventJournalProducerV1&) = delete;
    CertifiedOrderEventJournalProducerV1(
        CertifiedOrderEventJournalProducerV1&&) = delete;
    CertifiedOrderEventJournalProducerV1& operator=(
        CertifiedOrderEventJournalProducerV1&&) = delete;
    ~CertifiedOrderEventJournalProducerV1();

    [[nodiscard]] static CertifiedOrderEventJournalCreateErrorV1
    Create(
        CertifiedOrderEventJournalConfigV1 config,
        std::shared_ptr<CertifiedOrderEventJournalProducerV1>*
            output,
        int* system_error_number = nullptr) noexcept;

    [[nodiscard]] CertifiedOrderEventJournalPublishErrorV1
    EnsureWritable(std::uint64_t required_event_count) noexcept;

    [[nodiscard]] CertifiedOrderEventJournalPublishErrorV1
    PublishCanonicalTick(
        std::uint64_t canonical_apply_sequence,
        std::uint64_t shanghai_order_state_count,
        std::uint64_t shenzhen_order_state_count,
        std::span<const InstrumentDerivedEventV1> events) noexcept;

    [[nodiscard]] bool DuplicateReadOnlyDescriptor(
        int* output,
        int* system_error_number = nullptr) const noexcept;

    [[nodiscard]] CertifiedOrderEventJournalSessionV1 session()
        const noexcept;
    [[nodiscard]] std::uint64_t canonical_apply_frontier()
        const noexcept;
    [[nodiscard]] std::uint64_t published_event_sequence()
        const noexcept;
    [[nodiscard]] std::uint64_t committed_mapping_bytes()
        const noexcept;
    // Called by the CERTIFIED worker at the exact successful online-recovery
    // prefix barrier, before the control socket is exposed. This is a
    // monotonic transition and remains false for ordinary from-open startup.
    [[nodiscard]] bool MarkStartupPrefixRecovered() noexcept;
    [[nodiscard]] std::uint32_t coverage_flags() const noexcept;
    [[nodiscard]] bool failed() const noexcept;

private:
    class Impl;
    explicit CertifiedOrderEventJournalProducerV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::ipc
