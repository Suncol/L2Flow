#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>

namespace l2flow::market {

class ObservedInstrumentDirectoryV2;
class RealtimeHistoryRecordV1;

// The live read model deliberately has only two record classes.  "Tick"
// retains the existing mixed meaning: Shanghai tick, Shenzhen order, or
// Shenzhen transaction.  It must not be interpreted as "latest trade".
enum class RealtimeLatestRecordKindV1 : std::uint8_t {
    kSnapshot = 0U,
    kTick,
};

enum class RealtimeLatestRecordStatusV1 : std::uint8_t {
    kAvailable = 0U,
    kBoundNoTypeData,
    kUnbound,
    kInvalidInstrumentId,
};

enum class RealtimeLatestReadModelCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kResourceExhausted,
};

enum class RealtimeLatestPublishErrorV1 : std::uint8_t {
    kNone = 0U,
    kCoverageLost,
    kNullRecord,
    kInvalidOrdinal,
    kUnboundInstrument,
    kInstrumentMismatch,
    kInvalidRecordKind,
    kIngressConflict,
};

enum class RealtimeLatestQueryErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidArgument,
    kUnavailable,
    kCoverageLost,
};

[[nodiscard]] std::string_view
RealtimeLatestReadModelCreateErrorNameV1(
    RealtimeLatestReadModelCreateErrorV1 error) noexcept;
[[nodiscard]] std::string_view RealtimeLatestPublishErrorNameV1(
    RealtimeLatestPublishErrorV1 error) noexcept;
[[nodiscard]] std::string_view RealtimeLatestQueryErrorNameV1(
    RealtimeLatestQueryErrorV1 error) noexcept;

// A result borrows one immutable record from the intraday Store arena.  The
// record and its exact typed payload remain valid only while the Store
// session owning this read model remains alive.  External-language adapters
// must project/copy the record into their wire representation before that
// owner can be destroyed.
struct RealtimeLatestRecordViewV1 final {
    std::uint32_t instrument_id = 0U;
    RealtimeLatestRecordStatusV1 status =
        RealtimeLatestRecordStatusV1::kInvalidInstrumentId;
    const RealtimeHistoryRecordV1* record = nullptr;

    [[nodiscard]] bool available() const noexcept {
        return status == RealtimeLatestRecordStatusV1::kAvailable &&
               record != nullptr;
    }
};

// Allocation occurs only in Create.  PublishApplied is invoked by the one
// permanent instrument owner after the matching Store/KLine applied boundary;
// concurrent publishers for one directory ordinal are outside this contract.
// Queries are read-only, allocation-free acquire loads.  A batch is coherent
// per returned row, not a cross-instrument generation cut; consumers needing
// one exact market-wide prefix must use IntradayInstrumentStoreGenerationV1.
// This is a latest-point cache: a slower polling consumer is not guaranteed to
// observe every intermediate record.
//
// The stable directory must outlive this object. The Store session must
// outlive this object and every borrowed record returned by it.
class RealtimeLatestReadModelV1 final {
public:
    RealtimeLatestReadModelV1(
        const RealtimeLatestReadModelV1&) = delete;
    RealtimeLatestReadModelV1& operator=(
        const RealtimeLatestReadModelV1&) = delete;
    RealtimeLatestReadModelV1(
        RealtimeLatestReadModelV1&&) = delete;
    RealtimeLatestReadModelV1& operator=(
        RealtimeLatestReadModelV1&&) = delete;
    ~RealtimeLatestReadModelV1();

    [[nodiscard]] static RealtimeLatestReadModelCreateErrorV1 Create(
        const ObservedInstrumentDirectoryV2* directory,
        std::unique_ptr<RealtimeLatestReadModelV1>* output) noexcept;

    // An older ingress sequence is ignored defensively: kNone is returned
    // with updated=false and the current latest pointer does not regress.
    // The fixed-owner production topology normally publishes one slot in
    // increasing ingress order. updated may be null.
    [[nodiscard]] RealtimeLatestPublishErrorV1 PublishApplied(
        std::size_t ordinal,
        const RealtimeHistoryRecordV1* record,
        bool* updated = nullptr) noexcept;

    [[nodiscard]] RealtimeLatestQueryErrorV1 GetLatest(
        RealtimeLatestRecordKindV1 kind,
        std::uint32_t instrument_id,
        RealtimeLatestRecordViewV1* output) const noexcept;

    // output.size() must equal instrument_ids.size().  Results preserve input
    // order and report unknown/unobserved instruments independently, so a
    // partially available batch is still kNone.
    [[nodiscard]] RealtimeLatestQueryErrorV1 GetLatestRecords(
        RealtimeLatestRecordKindV1 kind,
        std::span<const std::uint32_t> instrument_ids,
        std::span<RealtimeLatestRecordViewV1> output) const noexcept;

    [[nodiscard]] RealtimeLatestQueryErrorV1 GetLatestSnapshot(
        std::uint32_t instrument_id,
        RealtimeLatestRecordViewV1* output) const noexcept;
    [[nodiscard]] RealtimeLatestQueryErrorV1 GetLatestSnapshots(
        std::span<const std::uint32_t> instrument_ids,
        std::span<RealtimeLatestRecordViewV1> output) const noexcept;

    [[nodiscard]] RealtimeLatestQueryErrorV1 GetLatestTick(
        std::uint32_t instrument_id,
        RealtimeLatestRecordViewV1* output) const noexcept;
    [[nodiscard]] RealtimeLatestQueryErrorV1 GetLatestTicks(
        std::span<const std::uint32_t> instrument_ids,
        std::span<RealtimeLatestRecordViewV1> output) const noexcept;

    // Coverage loss is sticky.  Once marked, later publication is rejected
    // and queries fail closed instead of presenting an incomplete prefix as a
    // trustworthy latest value.
    void MarkCoverageLost() noexcept;
    [[nodiscard]] bool coverage_lost() const noexcept;

private:
    class Impl;
    explicit RealtimeLatestReadModelV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::market
