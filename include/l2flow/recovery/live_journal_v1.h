#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/realtime/ingress_capture_v1.h"
#include "l2flow/sdk/market_message_catalog_v1.h"

#include "mdl_api.h"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace l2flow::recovery {

inline constexpr std::uint64_t kLiveJournalMinimumSegmentBytesV1 =
    1ULL * 1024ULL * 1024ULL;
inline constexpr std::uint64_t kLiveJournalMaximumSegmentBytesV1 =
    4ULL * 1024ULL * 1024ULL * 1024ULL;
inline constexpr std::size_t kLiveJournalMaximumQueueRecordsV1 =
    4'194'304U;

enum class LiveJournalErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidConfiguration,
    kDirectoryCreateFailed,
    kDirectoryOpenFailed,
    kDirectoryNotEmpty,
    kSegmentCreateFailed,
    kSegmentOpenFailed,
    kWriteFailed,
    kSyncFailed,
    kCapacityExhausted,
    kQueueExhausted,
    kInvalidCapture,
    kSequenceExhausted,
    kCorruptRecord,
    kStopped,
    kResourceExhausted,
    kUnexpectedFailure,
};

[[nodiscard]] std::string_view LiveJournalErrorNameV1(
    LiveJournalErrorV1 error) noexcept;

struct LiveJournalConfigV1 final {
    std::filesystem::path directory;
    l2flow::common::Identity128 run_id{};
    std::uint32_t trade_date = 0U;
    std::uint32_t maximum_message_bytes = 16U * 1024U * 1024U;
    std::uint64_t segment_maximum_bytes =
        256ULL * 1024ULL * 1024ULL;
    std::uint64_t maximum_total_bytes =
        512ULL * 1024ULL * 1024ULL * 1024ULL;
    std::size_t queue_capacity_records = 65'536U;
    std::size_t sync_batch_records = 256U;
    std::chrono::milliseconds sync_interval{2};
};

enum class LiveJournalStateV1 : std::uint8_t {
    kWriting = 1U,
    kStopping = 2U,
    kStopped = 3U,
    kFailed = 4U,
};

struct LiveJournalSnapshotV1 final {
    LiveJournalStateV1 state = LiveJournalStateV1::kFailed;
    LiveJournalErrorV1 error = LiveJournalErrorV1::kNone;
    int system_error_number = 0;
    std::uint64_t accepted_serial = 0U;
    // A record is committed only after every byte through this frontier has
    // passed fdatasync.  Shadow recovery must never read beyond it.
    std::uint64_t committed_serial = 0U;
    std::uint64_t reserved_bytes = 0U;
    std::uint64_t committed_bytes = 0U;
    std::uint64_t segment_count = 0U;
    std::size_t queue_depth = 0U;
    std::size_t queue_high_water = 0U;
    std::array<std::uint64_t, l2flow::sdk::kProductionMessageCountV1>
        accepted_tuple_serials{};

    [[nodiscard]] bool healthy() const noexcept {
        return state != LiveJournalStateV1::kFailed &&
               error == LiveJournalErrorV1::kNone;
    }
};

struct LiveJournalTupleFenceV1 final {
    l2flow::sdk::MessageKey key{};
    std::uint64_t accepted_global_serial = 0U;
    std::uint64_t accepted_tuple_serial = 0U;
};

// One CRC- and digest-verified record returned by the sequential reader.  It
// reconstructs the exact vendor MDLMessage bytes while retaining the callback
// clocks and both journal order identities outside that wire message.
class MdlLiveJournalRecordV1 final : public datayes::mdl::MDLMessage {
public:
    MdlLiveJournalRecordV1(
        const MdlLiveJournalRecordV1&) = delete;
    MdlLiveJournalRecordV1& operator=(
        const MdlLiveJournalRecordV1&) = delete;
    MdlLiveJournalRecordV1(MdlLiveJournalRecordV1&&) = delete;
    MdlLiveJournalRecordV1& operator=(MdlLiveJournalRecordV1&&) = delete;
    ~MdlLiveJournalRecordV1() = default;

    void AddRef() override {}
    int ReleaseRef() override { return 1; }
    [[nodiscard]] datayes::mdl::MDLMessageHead* GetHead()
        const override;
    [[nodiscard]] char* GetBody() const override;
    [[nodiscard]] datayes::mdl::MDLMessage* _Copy() const override {
        return nullptr;
    }

    [[nodiscard]] std::uint64_t global_serial() const noexcept {
        return global_serial_;
    }
    [[nodiscard]] std::uint64_t tuple_serial() const noexcept {
        return tuple_serial_;
    }
    [[nodiscard]] std::uint64_t recv_realtime_ns() const noexcept {
        return recv_realtime_ns_;
    }
    [[nodiscard]] std::uint64_t recv_monotonic_ns() const noexcept {
        return recv_monotonic_ns_;
    }
    [[nodiscard]] const l2flow::sdk::MessageKey& key() const noexcept {
        return key_;
    }
    [[nodiscard]] bool native_sequence_valid() const noexcept {
        return native_sequence_valid_;
    }
    [[nodiscard]] std::uint8_t native_market() const noexcept {
        return native_market_;
    }
    [[nodiscard]] std::uint32_t native_channel() const noexcept {
        return native_channel_;
    }
    [[nodiscard]] std::uint64_t native_sequence() const noexcept {
        return native_sequence_;
    }
    [[nodiscard]] std::size_t wire_size() const noexcept;

private:
    friend class MdlLiveJournalReaderV1;
    MdlLiveJournalRecordV1() = default;

    datayes::mdl::MDLMessageHead head_{};
    std::vector<std::byte> body_;
    l2flow::sdk::MessageKey key_{};
    std::uint64_t global_serial_ = 0U;
    std::uint64_t tuple_serial_ = 0U;
    std::uint64_t recv_realtime_ns_ = 0U;
    std::uint64_t recv_monotonic_ns_ = 0U;
    std::uint64_t native_sequence_ = 0U;
    std::uint32_t native_channel_ = 0U;
    std::uint8_t native_market_ = 0U;
    bool native_sequence_valid_ = false;
};

enum class LiveJournalReadDispositionV1 : std::uint8_t {
    kRecord = 0U,
    kTimeout,
    kEnd,
    kFailed,
};

struct LiveJournalReadResultV1 final {
    LiveJournalReadDispositionV1 disposition =
        LiveJournalReadDispositionV1::kFailed;
    LiveJournalErrorV1 error = LiveJournalErrorV1::kNone;
    int system_error_number = 0;
    std::unique_ptr<MdlLiveJournalRecordV1> record;

    [[nodiscard]] bool has_record() const noexcept {
        return disposition == LiveJournalReadDispositionV1::kRecord &&
               record != nullptr;
    }
};

class MdlLiveJournalReaderV1 final {
public:
    MdlLiveJournalReaderV1(const MdlLiveJournalReaderV1&) = delete;
    MdlLiveJournalReaderV1& operator=(
        const MdlLiveJournalReaderV1&) = delete;
    MdlLiveJournalReaderV1(MdlLiveJournalReaderV1&&) = delete;
    MdlLiveJournalReaderV1& operator=(
        MdlLiveJournalReaderV1&&) = delete;
    ~MdlLiveJournalReaderV1();

    // Reads exactly the next global callback serial.  It waits only for the
    // journal's durable committed frontier, never for its accepted frontier.
    [[nodiscard]] LiveJournalReadResultV1 ReadNext(
        std::chrono::steady_clock::time_point deadline) noexcept;
    [[nodiscard]] std::uint64_t next_serial() const noexcept;

private:
    class Impl;
    friend class MdlLiveJournalV1;
    explicit MdlLiveJournalReaderV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

class MdlLiveJournalV1 final
    : public l2flow::realtime::RealtimeIngressCaptureSinkV1,
      public std::enable_shared_from_this<MdlLiveJournalV1> {
public:
    MdlLiveJournalV1(const MdlLiveJournalV1&) = delete;
    MdlLiveJournalV1& operator=(const MdlLiveJournalV1&) = delete;
    MdlLiveJournalV1(MdlLiveJournalV1&&) = delete;
    MdlLiveJournalV1& operator=(MdlLiveJournalV1&&) = delete;
    ~MdlLiveJournalV1() override;

    [[nodiscard]] static LiveJournalErrorV1 Create(
        LiveJournalConfigV1 config,
        std::shared_ptr<MdlLiveJournalV1>* output,
        int* system_error_number = nullptr) noexcept;

    [[nodiscard]] bool Capture(
        const l2flow::realtime::RealtimeIngressCaptureInputV1& input)
        noexcept override;

    // Linearizes the CSV tuple cutoff with callback copies under the same
    // small queue mutex used to assign tuple_serial.
    [[nodiscard]] bool CaptureTupleFence(
        const l2flow::sdk::MessageKey& key,
        LiveJournalTupleFenceV1* output) const noexcept;

    [[nodiscard]] bool CreateReader(
        std::unique_ptr<MdlLiveJournalReaderV1>* output) noexcept;
    [[nodiscard]] LiveJournalSnapshotV1 Snapshot() const noexcept;

    // Stops new captures, drains and fdatasyncs every accepted record, then
    // joins the writer.  It is idempotent.  A false result means the journal
    // failed before the accepted frontier became durable.
    [[nodiscard]] bool StopAndFlush() noexcept;

private:
    class Impl;
    friend class MdlLiveJournalReaderV1;
    explicit MdlLiveJournalV1(std::shared_ptr<Impl> impl) noexcept;
    std::shared_ptr<Impl> impl_;
};

}  // namespace l2flow::recovery
