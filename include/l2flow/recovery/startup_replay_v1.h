#pragma once

#include "l2flow/sdk/market_message_catalog_v1.h"

#include "mdl_api.h"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <string>
#include <string_view>

namespace l2flow::recovery {

// A CSV replay publication is recovery provenance, never a live SDK callback.
inline constexpr std::uint64_t kStartupReplayProvenanceCsvV1 = 1ULL << 0U;

// The V4 CSV description does not contain ChannelNo in mdl_6_28_0.  When that
// optional column is absent the reconstructed SDK field is exactly zero and
// this notice is mandatory; callers must not infer a channel from another
// stream.
inline constexpr std::uint64_t
    kStartupReplayNoticeShenzhenSnapshotChannelUnavailableV1 =
        1ULL << 0U;

// V4 OrderQueue.csv exposes OrderQty but not the SDK queue item's
// OrderQueOper/OrderQueID.  Reconstructed values are deterministically zero
// and this notice prevents a caller from treating those zeroes as observed
// exchange data.
inline constexpr std::uint64_t
    kStartupReplayNoticeShanghaiOrderQueueMetadataUnavailableV1 =
        1ULL << 1U;

enum class StartupReplayMessageSetV1 : std::uint32_t {
    kNone = 0U,
    kShanghaiSnapshot = 1U << 0U,
    kShanghaiTick = 1U << 1U,
    kShenzhenSnapshot = 1U << 2U,
    kShenzhenOrder = 1U << 3U,
    kShenzhenTransaction = 1U << 4U,
    kAll = (1U << 5U) - 1U,
};

[[nodiscard]] constexpr StartupReplayMessageSetV1 operator|(
    StartupReplayMessageSetV1 left,
    StartupReplayMessageSetV1 right) noexcept {
    return static_cast<StartupReplayMessageSetV1>(
        static_cast<std::uint32_t>(left) |
        static_cast<std::uint32_t>(right));
}

[[nodiscard]] constexpr bool StartupReplayContainsV1(
    StartupReplayMessageSetV1 set,
    StartupReplayMessageSetV1 value) noexcept {
    return (static_cast<std::uint32_t>(set) &
            static_cast<std::uint32_t>(value)) ==
           static_cast<std::uint32_t>(value);
}

enum class StartupReplayErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidConfiguration,
    kFileMissing,
    kAliasConflict,
    kFileOpen,
    kFileStat,
    kFileTooLarge,
    kIo,
    kUtf8Invalid,
    kCsvMalformed,
    kIncompleteBoundary,
    kLineTooLong,
    kHeaderMissing,
    kDuplicateHeader,
    kSchemaMismatch,
    kColumnCountMismatch,
    kValueInvalid,
    kNumericOverflow,
    kTimeInvalid,
    kJoinDuplicate,
    kJoinMismatch,
    kJoinOrphan,
    kSequenceDuplicate,
    kSequenceGap,
    kMessageTooLarge,
    kSinkRejected,
    kResourceExhausted,
    kUnexpectedFailure,
};

[[nodiscard]] std::string_view StartupReplayErrorNameV1(
    StartupReplayErrorV1 error) noexcept;

struct StartupReplayConfigV1 final {
    std::filesystem::path directory;
    StartupReplayMessageSetV1 enabled_messages =
        StartupReplayMessageSetV1::kAll;

    // Each path is opened once and all reads stay on that retained descriptor.
    // The initial fstat fixes its normal prefix. An incomplete header/data
    // record or an initial cross-file root/child/native-sequence gap may take
    // one later fstat on the same descriptor. Once that finite extension is
    // selected, every complete row in it is consumed and validated; a record
    // still lacking LF/CRLF at its end fails kIncompleteBoundary. Growth past
    // the file's final selected prefix (initial, or the one later fstat) belongs
    // to the live side of the closed handoff.
    std::uint64_t maximum_file_bytes =
        std::numeric_limits<std::uint64_t>::max();
    std::size_t maximum_record_bytes = 16U * 1024U * 1024U;
    // Total SDK wire bytes, including MDLMessageHead.
    std::size_t maximum_message_bytes = 16U * 1024U * 1024U;
    std::size_t maximum_pending_messages = 2'000'000U;
    std::size_t maximum_pending_bytes = 512U * 1024U * 1024U;
};

struct StartupReplayPublicationV1 final {
    // Valid only for the dynamic extent of StartupReplaySinkV1::Publish.
    // The sink may synchronously copy its head/body bytes, but must not retain
    // this pointer (MDLMessage::Copy is unsupported) or any pointer obtained
    // from it.
    const datayes::mdl::MDLMessage* message = nullptr;
    l2flow::sdk::MessageKey key{};
    // Like message, this borrowed path is valid only during Publish.
    const std::filesystem::path* source_file = nullptr;
    std::uint64_t source_line = 0U;
    // Must be the nonzero SeqNo reconstructed into
    // message->GetHead()->SequenceID. The source contract requires this
    // recovery identity to increase strictly in each physical CSV file.
    // Shenzhen 6.33/6.36 native-sequence gap repair may delay an earlier
    // per-file row behind a later row from another channel, so Publish call
    // order is not a monotonic-SeqNo contract. It is not exchange-native
    // continuity; BizIndex/ApplSeqNum carry that stronger meaning for the
    // documented tick messages.
    std::uint64_t csv_sequence = 0U;
    std::uint64_t provenance_flags = kStartupReplayProvenanceCsvV1;
    std::uint64_t market_notice_flags = 0U;
};

class StartupReplaySinkV1 {
public:
    StartupReplaySinkV1() = default;
    StartupReplaySinkV1(const StartupReplaySinkV1&) = delete;
    StartupReplaySinkV1& operator=(const StartupReplaySinkV1&) = delete;
    virtual ~StartupReplaySinkV1() = default;

    // Records a callback-buffer serial fence before any CSV in a logical tuple
    // is opened/captured. Snapshot children are captured next and their root
    // is captured last, making the root prefix the tuple cutoff. A callback
    // in the capture interval is deduplicated when its retained identity is
    // present; otherwise the sink's tuple cutoff check must prove it is beyond
    // the captured prefix or fail closed. The default keeps standalone parser
    // sinks source-compatible.
    [[nodiscard]] virtual bool CaptureTupleFence(
        const l2flow::sdk::MessageKey& key,
        std::string* detail) noexcept {
        static_cast<void>(key);
        if (detail != nullptr) {
            detail->clear();
        }
        return true;
    }

    // A false return stops replay.  detail, when non-null, is sink-owned
    // diagnostic text copied into StartupReplayResultV1.
    [[nodiscard]] virtual bool Publish(
        const StartupReplayPublicationV1& publication,
        std::string* detail) noexcept = 0;
};

struct StartupReplayCountsV1 final {
    std::uint64_t shanghai_snapshots = 0U;
    std::uint64_t shanghai_ticks = 0U;
    std::uint64_t shenzhen_snapshots = 0U;
    std::uint64_t shenzhen_orders = 0U;
    std::uint64_t shenzhen_transactions = 0U;

    [[nodiscard]] std::uint64_t total() const noexcept {
        return shanghai_snapshots + shanghai_ticks +
               shenzhen_snapshots + shenzhen_orders +
               shenzhen_transactions;
    }
};

struct StartupReplayResultV1 final {
    StartupReplayErrorV1 error = StartupReplayErrorV1::kNone;
    StartupReplayCountsV1 counts{};
    std::filesystem::path error_file;
    std::uint64_t error_line = 0U;
    std::string detail;

    [[nodiscard]] bool ok() const noexcept {
        return error == StartupReplayErrorV1::kNone;
    }
};

class StartupReplaySourceV1 {
public:
    StartupReplaySourceV1() = default;
    StartupReplaySourceV1(const StartupReplaySourceV1&) = delete;
    StartupReplaySourceV1& operator=(const StartupReplaySourceV1&) = delete;
    virtual ~StartupReplaySourceV1() = default;

    // Replay, CaptureTupleFence, and Publish are synchronous, single-threaded
    // calls for one Replay invocation. Publications carry a nonzero identity
    // that is unique and strictly increasing in its physical source file.
    // Sink deduplication must use the exact (tuple, csv_sequence) identity
    // rather than assuming Publish call order is monotonic; native exchange
    // ordering is source-specific.
    [[nodiscard]] virtual StartupReplayResultV1 Replay(
        StartupReplaySinkV1& sink) noexcept = 0;
};

class MdlCsvStartupReplaySourceV1 final : public StartupReplaySourceV1 {
public:
    explicit MdlCsvStartupReplaySourceV1(StartupReplayConfigV1 config);
    ~MdlCsvStartupReplaySourceV1() override;

    MdlCsvStartupReplaySourceV1(
        const MdlCsvStartupReplaySourceV1&) = delete;
    MdlCsvStartupReplaySourceV1& operator=(
        const MdlCsvStartupReplaySourceV1&) = delete;
    MdlCsvStartupReplaySourceV1(
        MdlCsvStartupReplaySourceV1&&) = delete;
    MdlCsvStartupReplaySourceV1& operator=(
        MdlCsvStartupReplaySourceV1&&) = delete;

    [[nodiscard]] StartupReplayResultV1 Replay(
        StartupReplaySinkV1& sink) noexcept override;

    [[nodiscard]] const StartupReplayConfigV1& config() const noexcept {
        return config_;
    }

private:
    StartupReplayConfigV1 config_;
};

}  // namespace l2flow::recovery
