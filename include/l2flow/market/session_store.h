#pragma once

#include "l2flow/common/identity128.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace l2flow::market {

// The production topology has four independent TCP sources.  This store
// deliberately exposes four per-source histories rather than manufacturing a
// cross-source order from receive timestamps or bare source sequences.
inline constexpr std::size_t kSessionStoreStreamCountV1 = 4U;

struct SessionStreamKeyV1 final {
    std::uint32_t capture_date = 0U;
    std::uint32_t source_stream_id = 0U;
    l2flow::common::Identity128 stream_day_id{};

    [[nodiscard]] friend constexpr bool operator==(
        const SessionStreamKeyV1&,
        const SessionStreamKeyV1&) noexcept = default;
};

enum class SessionRetentionModeV1 : std::uint8_t {
    // The default policy: retain every accepted event for this store's exact
    // capture-date/stream-day session, subject only to the explicit hard
    // limits below.  Reaching a limit rejects the new event; it never evicts
    // an old event to make room.
    kFullSession = 0U,
    // On a successful append to one stream, evict only that stream's prefix
    // whose recv_monotonic_ns is strictly older than the inclusive window.
    // No timestamp from another stream advances this stream's window.
    kRecvMonotonicWindow = 1U,
};

struct SessionStoreConfigV1 final {
    std::array<SessionStreamKeyV1, kSessionStoreStreamCountV1>
        streams{};
    SessionRetentionModeV1 retention_mode =
        SessionRetentionModeV1::kFullSession;
    // Must be zero for kFullSession and nonzero for
    // kRecvMonotonicWindow.
    std::uint64_t recv_monotonic_window_ns = 0U;
    // Aggregate resident limits across all four streams.  Both are hard
    // admission limits and must be nonzero.  Record payload accounting is
    // supplied explicitly on SessionOwnedRecordV1 because a generic C++ type
    // cannot introspect its transitively owned heap allocation.
    std::uint64_t max_records = 0U;
    std::uint64_t max_payload_bytes = 0U;
    // Immutable snapshots share chunks.  Snapshot construction seals the
    // bounded open chunk and copies chunk descriptors, rather than copying
    // the complete history while holding the writer mutex.
    std::size_t chunk_record_capacity = 1024U;
};

enum class SessionStoreCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidStreamKey,
    kDuplicateSourceStream,
    kMixedCaptureDate,
    kInvalidRetention,
    kInvalidLimits,
    kInvalidChunkCapacity,
    kResourceExhausted,
};

enum class SessionAppendErrorV1 : std::uint8_t {
    kNone = 0U,
    kUnknownSourceStream,
    kStreamContextMismatch,
    kInvalidSourceSequence,
    kSourceSequenceNotIncreasing,
    kRecvMonotonicRegression,
    kMaxRecordsExceeded,
    kMaxPayloadBytesExceeded,
    kResourceExhausted,
    kCount,
};

inline constexpr std::size_t kSessionAppendErrorCountV1 =
    static_cast<std::size_t>(SessionAppendErrorV1::kCount);

enum class SessionSnapshotErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kResourceExhausted,
};

[[nodiscard]] std::string_view SessionStoreCreateErrorNameV1(
    SessionStoreCreateErrorV1 error) noexcept;
[[nodiscard]] std::string_view SessionAppendErrorNameV1(
    SessionAppendErrorV1 error) noexcept;
[[nodiscard]] std::string_view SessionSnapshotErrorNameV1(
    SessionSnapshotErrorV1 error) noexcept;

struct SessionStreamCountersV1 final {
    std::uint64_t accepted_records = 0U;
    std::uint64_t accepted_payload_bytes = 0U;
    std::uint64_t evicted_records = 0U;
    std::uint64_t evicted_payload_bytes = 0U;
    std::uint64_t rejected_records = 0U;
    std::uint64_t rejected_payload_bytes = 0U;
};

struct SessionStoreCountersV1 final {
    std::uint64_t append_attempts = 0U;
    std::uint64_t accepted_records = 0U;
    std::uint64_t accepted_payload_bytes = 0U;
    std::uint64_t evicted_records = 0U;
    std::uint64_t evicted_payload_bytes = 0U;
    std::uint64_t rejected_records = 0U;
    std::uint64_t rejected_payload_bytes = 0U;
    std::array<std::uint64_t, kSessionAppendErrorCountV1>
        rejected_by_error{};
    std::uint64_t snapshot_attempts = 0U;
    std::uint64_t successful_snapshots = 0U;
    std::uint64_t failed_snapshots = 0U;
    // Diagnostic counters saturate at UINT64_MAX.  Resident counts never
    // saturate: hard admission checks keep them within configured bounds.
    bool saturated = false;
};

struct SessionStreamWatermarkV1 final {
    SessionStreamKeyV1 key{};
    SessionStreamCountersV1 counters{};
    std::uint64_t resident_records = 0U;
    std::uint64_t resident_payload_bytes = 0U;
    std::uint64_t high_water_records = 0U;
    std::uint64_t high_water_payload_bytes = 0U;
    std::uint64_t first_accepted_source_sequence = 0U;
    std::uint64_t first_accepted_recv_monotonic_ns = 0U;
    std::uint64_t latest_accepted_source_sequence = 0U;
    std::uint64_t latest_accepted_recv_monotonic_ns = 0U;
    std::uint64_t oldest_resident_source_sequence = 0U;
    std::uint64_t oldest_resident_recv_monotonic_ns = 0U;
    // For window retention this is the inclusive lower bound computed from
    // the latest successfully accepted event in this stream.  It is zero for
    // full-session retention and before the window reaches monotonic origin.
    std::uint64_t retention_floor_recv_monotonic_ns = 0U;
    bool has_accepted_record = false;
    bool has_resident_record = false;
};

struct SessionStoreWatermarkV1 final {
    std::array<SessionStreamWatermarkV1,
               kSessionStoreStreamCountV1>
        streams{};
    SessionStoreCountersV1 counters{};
    std::uint64_t resident_records = 0U;
    std::uint64_t resident_payload_bytes = 0U;
    std::uint64_t high_water_records = 0U;
    std::uint64_t high_water_payload_bytes = 0U;
};

struct SessionAppendResultV1 final {
    SessionAppendErrorV1 error = SessionAppendErrorV1::kNone;
    std::uint64_t evicted_records = 0U;
    std::uint64_t evicted_payload_bytes = 0U;
    // Compact post-attempt watermarks keep the append hot path independent
    // of the larger diagnostic Watermark() snapshot.
    std::uint64_t store_resident_records = 0U;
    std::uint64_t store_resident_payload_bytes = 0U;
    std::uint64_t stream_resident_records = 0U;
    std::uint64_t stream_resident_payload_bytes = 0U;
    std::uint64_t stream_latest_source_sequence = 0U;
    std::uint64_t stream_latest_recv_monotonic_ns = 0U;
    bool source_stream_known = false;

    [[nodiscard]] bool accepted() const noexcept {
        return error == SessionAppendErrorV1::kNone;
    }
};

// OwnedEvent must transitively own every byte needed by later factor
// computation.  A span, string_view, SDK body pointer, or other borrowed view
// is not an OwnedEvent.  The store moves the value into immutable shared
// storage before Append returns.
template <typename OwnedEvent>
struct SessionOwnedRecordV1 final {
    SessionStreamKeyV1 stream{};
    // This is the per-source callback/Raw order.  It must be nonzero and
    // strictly increasing, but need not be contiguous because API/SYS records
    // may occupy intervening ingress_sequence values.
    std::uint64_t source_sequence = 0U;
    // Only this same-context receive monotonic timestamp drives optional
    // retention.  Exchange timestamps and realtime clocks are never used.
    std::uint64_t recv_monotonic_ns = 0U;
    std::uint64_t owned_payload_bytes = 0U;
    OwnedEvent event;
};

template <typename OwnedEvent>
class SessionStoreV1;

template <typename OwnedEvent>
class SessionChunkViewV1;

template <typename OwnedEvent>
class SessionRecordHandleV1;

template <typename OwnedEvent>
class SessionStreamSnapshotV1;

template <typename OwnedEvent>
class SessionStoreSnapshotV1;

namespace session_store_detail {

template <typename OwnedEvent>
class SessionChunkV1 final {
public:
    using record_type = SessionOwnedRecordV1<OwnedEvent>;

private:
    friend class ::l2flow::market::SessionStoreV1<OwnedEvent>;
    friend class ::l2flow::market::SessionChunkViewV1<OwnedEvent>;
    friend class ::l2flow::market::SessionRecordHandleV1<OwnedEvent>;

    std::vector<record_type> records_;
};

}  // namespace session_store_detail

// A record handle owns its immutable chunk.  get()/operator-> remain stable
// while the handle is retained, independently of later appends, retention,
// snapshot destruction, or store destruction.
template <typename OwnedEvent>
class SessionRecordHandleV1 final {
public:
    using record_type = SessionOwnedRecordV1<OwnedEvent>;

    SessionRecordHandleV1() noexcept = default;

    [[nodiscard]] const record_type* get() const noexcept {
        if (chunk_ == nullptr || index_ >= chunk_->records_.size()) {
            return nullptr;
        }
        return &chunk_->records_[index_];
    }

    [[nodiscard]] const record_type* operator->() const noexcept {
        return get();
    }

    [[nodiscard]] const record_type& operator*() const noexcept {
        return *get();
    }

    [[nodiscard]] explicit operator bool() const noexcept {
        return get() != nullptr;
    }

private:
    using chunk_type =
        session_store_detail::SessionChunkV1<OwnedEvent>;
    using chunk_ptr = std::shared_ptr<const chunk_type>;

    friend class SessionChunkViewV1<OwnedEvent>;

    SessionRecordHandleV1(
        chunk_ptr chunk,
        std::size_t index) noexcept
        : chunk_(std::move(chunk)), index_(index) {}

    chunk_ptr chunk_;
    std::size_t index_ = 0U;
};

// A copied chunk view owns the immutable chunk.  Retaining either the chunk
// view or a RecordAt() handle keeps every referenced record stable.
template <typename OwnedEvent>
class SessionChunkViewV1 final {
public:
    using record_type = SessionOwnedRecordV1<OwnedEvent>;
    using record_handle_type = SessionRecordHandleV1<OwnedEvent>;

    SessionChunkViewV1() noexcept = default;

    [[nodiscard]] std::size_t size() const noexcept {
        return end_ - begin_;
    }

    [[nodiscard]] bool empty() const noexcept {
        return size() == 0U;
    }

    [[nodiscard]] record_handle_type RecordAt(
        std::size_t index) const noexcept {
        if (chunk_ == nullptr || index >= size()) {
            return {};
        }
        return record_handle_type{chunk_, begin_ + index};
    }

    // References passed to visitor remain valid for the complete call.  A
    // caller that needs a longer lifetime should retain RecordAt() instead.
    template <typename Visitor>
    void VisitRecords(Visitor&& visitor) const {
        auto&& callable = visitor;
        if (chunk_ == nullptr) {
            return;
        }
        for (std::size_t index = begin_; index < end_; ++index) {
            callable(chunk_->records_[index]);
        }
    }

private:
    using chunk_type =
        session_store_detail::SessionChunkV1<OwnedEvent>;
    using chunk_ptr = std::shared_ptr<const chunk_type>;

    friend class SessionStoreV1<OwnedEvent>;
    friend class SessionStreamSnapshotV1<OwnedEvent>;

    SessionChunkViewV1(
        chunk_ptr chunk,
        std::size_t begin,
        std::size_t end) noexcept
        : chunk_(std::move(chunk)), begin_(begin), end_(end) {}

    chunk_ptr chunk_;
    std::size_t begin_ = 0U;
    std::size_t end_ = 0U;
};

template <typename OwnedEvent>
class SessionStreamSnapshotV1 final {
public:
    using chunk_view_type = SessionChunkViewV1<OwnedEvent>;
    using record_type = SessionOwnedRecordV1<OwnedEvent>;
    using record_handle_type = SessionRecordHandleV1<OwnedEvent>;

    [[nodiscard]] const SessionStreamWatermarkV1& watermark()
        const noexcept {
        return watermark_;
    }

    [[nodiscard]] std::size_t chunk_count() const noexcept {
        return chunks_.size();
    }

    [[nodiscard]] std::uint64_t record_count() const noexcept {
        return watermark_.resident_records;
    }

    [[nodiscard]] chunk_view_type ChunkAt(
        std::size_t index) const noexcept {
        return index < chunks_.size() ? chunks_[index]
                                      : chunk_view_type{};
    }

    [[nodiscard]] record_handle_type RecordAt(
        std::uint64_t index) const noexcept {
        if (index >= record_count()) {
            return {};
        }
        std::uint64_t offset = index;
        for (const chunk_view_type& chunk : chunks_) {
            const std::uint64_t chunk_size =
                static_cast<std::uint64_t>(chunk.size());
            if (offset < chunk_size) {
                return chunk.RecordAt(
                    static_cast<std::size_t>(offset));
            }
            offset -= chunk_size;
        }
        return {};
    }

    template <typename Visitor>
    void VisitRecords(Visitor&& visitor) const {
        auto&& callable = visitor;
        for (const chunk_view_type& chunk : chunks_) {
            chunk.VisitRecords(callable);
        }
    }

private:
    friend class SessionStoreV1<OwnedEvent>;
    friend class SessionStoreSnapshotV1<OwnedEvent>;

    SessionStreamWatermarkV1 watermark_{};
    std::vector<chunk_view_type> chunks_;
};

template <typename OwnedEvent>
class SessionStoreSnapshotV1 final {
public:
    using stream_snapshot_type =
        SessionStreamSnapshotV1<OwnedEvent>;

    [[nodiscard]] const SessionStoreWatermarkV1& watermark()
        const noexcept {
        return watermark_;
    }

    [[nodiscard]] const stream_snapshot_type* StreamAt(
        std::size_t index) const noexcept {
        return index < streams_.size() ? &streams_[index] : nullptr;
    }

    [[nodiscard]] const stream_snapshot_type* FindStream(
        const SessionStreamKeyV1& key) const noexcept {
        for (const stream_snapshot_type& stream : streams_) {
            if (stream.watermark().key == key) {
                return &stream;
            }
        }
        return nullptr;
    }

    void Swap(SessionStoreSnapshotV1& other) noexcept {
        using std::swap;
        swap(watermark_, other.watermark_);
        for (std::size_t index = 0U;
             index < streams_.size();
             ++index) {
            swap(streams_[index].watermark_,
                 other.streams_[index].watermark_);
            streams_[index].chunks_.swap(
                other.streams_[index].chunks_);
        }
    }

private:
    friend class SessionStoreV1<OwnedEvent>;

    SessionStoreWatermarkV1 watermark_{};
    std::array<stream_snapshot_type,
               kSessionStoreStreamCountV1>
        streams_{};
};

// Thread-safe, owned, append-only-per-stream session storage.  Append calls
// are serialized only for admission and publication.  A returned snapshot is
// immutable and readers traverse it without taking the store mutex while
// later appends continue.  Stable snapshots can intentionally pin evicted
// chunks; hard limits describe the store's resident logical set, while the
// caller controls the number and lifetime of snapshots it retains.
template <typename OwnedEvent>
class SessionStoreV1 final {
public:
    using event_type = OwnedEvent;
    using record_type = SessionOwnedRecordV1<OwnedEvent>;
    using record_handle_type = SessionRecordHandleV1<OwnedEvent>;
    using snapshot_type = SessionStoreSnapshotV1<OwnedEvent>;

    static_assert(
        std::is_nothrow_move_constructible_v<OwnedEvent>,
        "SessionStoreV1 requires a non-throwing movable owned event");
    static_assert(
        std::is_nothrow_destructible_v<OwnedEvent>,
        "SessionStoreV1 requires a non-throwing owned event destructor");

    SessionStoreV1(const SessionStoreV1&) = delete;
    SessionStoreV1& operator=(const SessionStoreV1&) = delete;
    SessionStoreV1(SessionStoreV1&&) = delete;
    SessionStoreV1& operator=(SessionStoreV1&&) = delete;
    ~SessionStoreV1() = default;

    [[nodiscard]] static SessionStoreCreateErrorV1 Create(
        SessionStoreConfigV1 config,
        std::unique_ptr<SessionStoreV1>* output) noexcept {
        if (output == nullptr) {
            return SessionStoreCreateErrorV1::kNullOutput;
        }
        output->reset();

        const SessionStoreCreateErrorV1 validation =
            ValidateConfig(config);
        if (validation != SessionStoreCreateErrorV1::kNone) {
            return validation;
        }
        try {
            output->reset(new SessionStoreV1(std::move(config)));
            return SessionStoreCreateErrorV1::kNone;
        } catch (...) {
            return SessionStoreCreateErrorV1::kResourceExhausted;
        }
    }

    [[nodiscard]] SessionAppendResultV1 Append(
        record_type record) noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        AddCounter(&counters_.append_attempts, 1U);

        const std::size_t stream_index =
            FindExactStreamLocked(record.stream);
        if (stream_index == kInvalidStreamIndex) {
            const std::size_t source_index =
                FindSourceStreamLocked(
                    record.stream.source_stream_id);
            const SessionAppendErrorV1 error =
                source_index == kInvalidStreamIndex
                    ? SessionAppendErrorV1::kUnknownSourceStream
                    : SessionAppendErrorV1::
                          kStreamContextMismatch;
            RejectLocked(
                error,
                record.stream,
                record.owned_payload_bytes);
            return ResultLocked(error, source_index);
        }

        StreamState& stream = streams_[stream_index];
        if (record.source_sequence == 0U) {
            RejectLocked(
                SessionAppendErrorV1::kInvalidSourceSequence,
                record.stream,
                record.owned_payload_bytes);
            return ResultLocked(
                SessionAppendErrorV1::kInvalidSourceSequence,
                stream_index);
        }
        if (stream.has_accepted &&
            record.source_sequence <=
                stream.latest_source_sequence) {
            RejectLocked(
                SessionAppendErrorV1::
                    kSourceSequenceNotIncreasing,
                record.stream,
                record.owned_payload_bytes);
            return ResultLocked(
                SessionAppendErrorV1::
                    kSourceSequenceNotIncreasing,
                stream_index);
        }
        if (stream.has_accepted &&
            record.recv_monotonic_ns <
                stream.latest_recv_monotonic_ns) {
            RejectLocked(
                SessionAppendErrorV1::
                    kRecvMonotonicRegression,
                record.stream,
                record.owned_payload_bytes);
            return ResultLocked(
                SessionAppendErrorV1::
                    kRecvMonotonicRegression,
                stream_index);
        }

        const EvictionPlan eviction =
            PlanEvictionLocked(stream, record.recv_monotonic_ns);
        const std::uint64_t resident_after_eviction =
            resident_records_ - eviction.records;
        if (resident_after_eviction >= config_.max_records) {
            RejectLocked(
                SessionAppendErrorV1::kMaxRecordsExceeded,
                record.stream,
                record.owned_payload_bytes);
            return ResultLocked(
                SessionAppendErrorV1::kMaxRecordsExceeded,
                stream_index);
        }
        const std::uint64_t payload_after_eviction =
            resident_payload_bytes_ - eviction.payload_bytes;
        if (record.owned_payload_bytes >
            config_.max_payload_bytes - payload_after_eviction) {
            RejectLocked(
                SessionAppendErrorV1::
                    kMaxPayloadBytesExceeded,
                record.stream,
                record.owned_payload_bytes);
            return ResultLocked(
                SessionAppendErrorV1::
                    kMaxPayloadBytesExceeded,
                stream_index);
        }

        const std::uint64_t accepted_source_sequence =
            record.source_sequence;
        const std::uint64_t accepted_recv_monotonic_ns =
            record.recv_monotonic_ns;
        const std::uint64_t accepted_payload_bytes =
            record.owned_payload_bytes;
        try {
            CommitAppendLocked(
                stream,
                std::move(record),
                eviction);
        } catch (...) {
            RejectLocked(
                SessionAppendErrorV1::kResourceExhausted,
                config_.streams[stream_index],
                accepted_payload_bytes);
            return ResultLocked(
                SessionAppendErrorV1::kResourceExhausted,
                stream_index);
        }

        PublishAcceptedLocked(
            stream,
            accepted_source_sequence,
            accepted_recv_monotonic_ns,
            accepted_payload_bytes,
            eviction);
        SessionAppendResultV1 result = ResultLocked(
            SessionAppendErrorV1::kNone,
            stream_index);
        result.evicted_records = eviction.records;
        result.evicted_payload_bytes = eviction.payload_bytes;
        return result;
    }

    [[nodiscard]] SessionStoreWatermarkV1 Watermark()
        const noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        return WatermarkLocked();
    }

    // Seals each bounded open chunk and copies only immutable chunk
    // descriptors.  On failure, output is unchanged; representation-only
    // sealing already completed for an earlier stream may remain in effect.
    [[nodiscard]] SessionSnapshotErrorV1 Snapshot(
        snapshot_type* output) noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        AddCounter(&counters_.snapshot_attempts, 1U);
        if (output == nullptr) {
            AddCounter(&counters_.failed_snapshots, 1U);
            return SessionSnapshotErrorV1::kNullOutput;
        }

        snapshot_type candidate;
        try {
            for (std::size_t stream_index = 0U;
                 stream_index < streams_.size();
                 ++stream_index) {
                BuildStreamSnapshotLocked(
                    streams_[stream_index],
                    &candidate.streams_[stream_index]);
            }
        } catch (...) {
            AddCounter(&counters_.failed_snapshots, 1U);
            return SessionSnapshotErrorV1::kResourceExhausted;
        }

        AddCounter(&counters_.successful_snapshots, 1U);
        candidate.watermark_ = WatermarkLocked();
        for (std::size_t index = 0U;
             index < candidate.streams_.size();
             ++index) {
            candidate.streams_[index].watermark_ =
                candidate.watermark_.streams[index];
        }
        output->Swap(candidate);
        return SessionSnapshotErrorV1::kNone;
    }

    [[nodiscard]] const SessionStoreConfigV1& config()
        const noexcept {
        return config_;
    }

private:
    using chunk_type =
        session_store_detail::SessionChunkV1<OwnedEvent>;
    using mutable_chunk_ptr = std::shared_ptr<chunk_type>;
    using chunk_ptr = std::shared_ptr<const chunk_type>;
    using chunk_view_type = SessionChunkViewV1<OwnedEvent>;

    struct ChunkSlice final {
        chunk_ptr chunk;
        std::size_t begin = 0U;
        std::size_t end = 0U;
    };

    struct StreamState final {
        std::deque<ChunkSlice> sealed;
        mutable_chunk_ptr active;
        std::size_t active_begin = 0U;
        SessionStreamCountersV1 counters{};
        std::uint64_t resident_records = 0U;
        std::uint64_t resident_payload_bytes = 0U;
        std::uint64_t high_water_records = 0U;
        std::uint64_t high_water_payload_bytes = 0U;
        std::uint64_t first_source_sequence = 0U;
        std::uint64_t first_recv_monotonic_ns = 0U;
        std::uint64_t latest_source_sequence = 0U;
        std::uint64_t latest_recv_monotonic_ns = 0U;
        bool has_accepted = false;
    };

    struct EvictionPlan final {
        std::size_t sealed_chunks = 0U;
        std::size_t sealed_front_begin = 0U;
        std::size_t active_begin = 0U;
        std::uint64_t records = 0U;
        std::uint64_t payload_bytes = 0U;
        bool sealed_front_partial = false;
    };

    static constexpr std::size_t kInvalidStreamIndex =
        std::numeric_limits<std::size_t>::max();

    explicit SessionStoreV1(SessionStoreConfigV1 config) noexcept
        : config_(std::move(config)) {}

    [[nodiscard]] static SessionStoreCreateErrorV1 ValidateConfig(
        const SessionStoreConfigV1& config) noexcept {
        if (config.max_records == 0U ||
            config.max_payload_bytes == 0U) {
            return SessionStoreCreateErrorV1::kInvalidLimits;
        }
        if (config.chunk_record_capacity == 0U ||
            static_cast<std::uint64_t>(
                config.chunk_record_capacity) >
                config.max_records) {
            return SessionStoreCreateErrorV1::
                kInvalidChunkCapacity;
        }
        switch (config.retention_mode) {
            case SessionRetentionModeV1::kFullSession:
                if (config.recv_monotonic_window_ns != 0U) {
                    return SessionStoreCreateErrorV1::
                        kInvalidRetention;
                }
                break;
            case SessionRetentionModeV1::kRecvMonotonicWindow:
                if (config.recv_monotonic_window_ns == 0U) {
                    return SessionStoreCreateErrorV1::
                        kInvalidRetention;
                }
                break;
            default:
                return SessionStoreCreateErrorV1::
                    kInvalidRetention;
        }

        const std::uint32_t capture_date =
            config.streams.front().capture_date;
        for (std::size_t index = 0U;
             index < config.streams.size();
             ++index) {
            const SessionStreamKeyV1& key = config.streams[index];
            if (key.capture_date == 0U ||
                key.source_stream_id == 0U ||
                l2flow::common::IsZeroIdentity(
                    key.stream_day_id)) {
                return SessionStoreCreateErrorV1::
                    kInvalidStreamKey;
            }
            if (key.capture_date != capture_date) {
                return SessionStoreCreateErrorV1::
                    kMixedCaptureDate;
            }
            for (std::size_t prior = 0U;
                 prior < index;
                 ++prior) {
                if (config.streams[prior].source_stream_id ==
                    key.source_stream_id) {
                    return SessionStoreCreateErrorV1::
                        kDuplicateSourceStream;
                }
            }
        }
        return SessionStoreCreateErrorV1::kNone;
    }

    void AddCounter(
        std::uint64_t* counter,
        std::uint64_t amount) const noexcept {
        if (*counter >
            std::numeric_limits<std::uint64_t>::max() - amount) {
            *counter = std::numeric_limits<std::uint64_t>::max();
            counters_.saturated = true;
            return;
        }
        *counter += amount;
    }

    [[nodiscard]] std::size_t FindExactStreamLocked(
        const SessionStreamKeyV1& key) const noexcept {
        for (std::size_t index = 0U;
             index < config_.streams.size();
             ++index) {
            if (config_.streams[index] == key) {
                return index;
            }
        }
        return kInvalidStreamIndex;
    }

    [[nodiscard]] std::size_t FindSourceStreamLocked(
        std::uint32_t source_stream_id) const noexcept {
        for (std::size_t index = 0U;
             index < config_.streams.size();
             ++index) {
            if (config_.streams[index].source_stream_id ==
                source_stream_id) {
                return index;
            }
        }
        return kInvalidStreamIndex;
    }

    void RejectLocked(
        SessionAppendErrorV1 error,
        const SessionStreamKeyV1& key,
        std::uint64_t payload_bytes) const noexcept {
        AddCounter(&counters_.rejected_records, 1U);
        AddCounter(
            &counters_.rejected_payload_bytes,
            payload_bytes);
        const std::size_t reason = static_cast<std::size_t>(error);
        if (reason < counters_.rejected_by_error.size()) {
            AddCounter(
                &counters_.rejected_by_error[reason], 1U);
        }

        std::size_t stream_index = FindExactStreamLocked(key);
        if (stream_index == kInvalidStreamIndex) {
            stream_index =
                FindSourceStreamLocked(key.source_stream_id);
        }
        if (stream_index != kInvalidStreamIndex) {
            StreamState& stream = streams_[stream_index];
            AddCounter(&stream.counters.rejected_records, 1U);
            AddCounter(
                &stream.counters.rejected_payload_bytes,
                payload_bytes);
        }
    }

    [[nodiscard]] EvictionPlan PlanEvictionLocked(
        const StreamState& stream,
        std::uint64_t candidate_recv_monotonic_ns) const noexcept {
        EvictionPlan plan;
        plan.active_begin = stream.active_begin;
        if (config_.retention_mode ==
            SessionRetentionModeV1::kFullSession) {
            return plan;
        }

        const std::uint64_t cutoff =
            candidate_recv_monotonic_ns >
                    config_.recv_monotonic_window_ns
                ? candidate_recv_monotonic_ns -
                      config_.recv_monotonic_window_ns
                : 0U;

        for (const ChunkSlice& slice : stream.sealed) {
            std::size_t index = slice.begin;
            while (index < slice.end &&
                   slice.chunk->records_[index]
                           .recv_monotonic_ns < cutoff) {
                ++plan.records;
                plan.payload_bytes +=
                    slice.chunk->records_[index]
                        .owned_payload_bytes;
                ++index;
            }
            if (index != slice.end) {
                plan.sealed_front_partial =
                    index != slice.begin;
                plan.sealed_front_begin = index;
                return plan;
            }
            ++plan.sealed_chunks;
        }

        if (stream.active != nullptr) {
            std::size_t index = stream.active_begin;
            while (index < stream.active->records_.size() &&
                   stream.active->records_[index]
                           .recv_monotonic_ns < cutoff) {
                ++plan.records;
                plan.payload_bytes +=
                    stream.active->records_[index]
                        .owned_payload_bytes;
                ++index;
            }
            plan.active_begin = index;
        }
        return plan;
    }

    [[nodiscard]] mutable_chunk_ptr NewActiveChunk(
        record_type first_record) const {
        mutable_chunk_ptr chunk = std::make_shared<chunk_type>();
        chunk->records_.reserve(config_.chunk_record_capacity);
        chunk->records_.push_back(std::move(first_record));
        return chunk;
    }

    void ApplySealedEvictionLocked(
        StreamState& stream,
        const EvictionPlan& eviction) noexcept {
        for (std::size_t count = 0U;
             count < eviction.sealed_chunks;
             ++count) {
            stream.sealed.pop_front();
        }
        if (eviction.sealed_front_partial) {
            stream.sealed.front().begin =
                eviction.sealed_front_begin;
        }
    }

    // Every potentially allocating operation precedes the first logical
    // mutation.  Once a deque insertion or active-vector insertion succeeds,
    // the remainder of the commit is non-throwing.
    void CommitAppendLocked(
        StreamState& stream,
        record_type record,
        const EvictionPlan& eviction) {
        if (stream.active == nullptr) {
            mutable_chunk_ptr replacement =
                NewActiveChunk(std::move(record));
            ApplySealedEvictionLocked(stream, eviction);
            stream.active = std::move(replacement);
            stream.active_begin = 0U;
            return;
        }

        if (stream.active->records_.size() <
            config_.chunk_record_capacity) {
            // reserve(chunk_record_capacity) was completed before this
            // active chunk was published, and record move is noexcept.
            stream.active->records_.push_back(std::move(record));
            ApplySealedEvictionLocked(stream, eviction);
            stream.active_begin = eviction.active_begin;
            return;
        }

        mutable_chunk_ptr replacement =
            NewActiveChunk(std::move(record));
        const std::size_t retained_begin = eviction.active_begin;
        const std::size_t retained_end =
            stream.active->records_.size();
        if (retained_begin < retained_end) {
            // deque::push_back has the strong guarantee for this non-throwing
            // value type.  No store state is changed if allocation fails.
            stream.sealed.push_back(ChunkSlice{
                chunk_ptr(stream.active),
                retained_begin,
                retained_end});
        }
        ApplySealedEvictionLocked(stream, eviction);
        stream.active = std::move(replacement);
        stream.active_begin = 0U;
    }

    void PublishAcceptedLocked(
        StreamState& stream,
        std::uint64_t source_sequence,
        std::uint64_t recv_monotonic_ns,
        std::uint64_t payload_bytes,
        const EvictionPlan& eviction) const noexcept {
        resident_records_ =
            resident_records_ - eviction.records + 1U;
        resident_payload_bytes_ =
            resident_payload_bytes_ - eviction.payload_bytes +
            payload_bytes;
        stream.resident_records =
            stream.resident_records - eviction.records + 1U;
        stream.resident_payload_bytes =
            stream.resident_payload_bytes - eviction.payload_bytes +
            payload_bytes;

        AddCounter(&counters_.accepted_records, 1U);
        AddCounter(
            &counters_.accepted_payload_bytes,
            payload_bytes);
        AddCounter(
            &counters_.evicted_records,
            eviction.records);
        AddCounter(
            &counters_.evicted_payload_bytes,
            eviction.payload_bytes);
        AddCounter(&stream.counters.accepted_records, 1U);
        AddCounter(
            &stream.counters.accepted_payload_bytes,
            payload_bytes);
        AddCounter(
            &stream.counters.evicted_records,
            eviction.records);
        AddCounter(
            &stream.counters.evicted_payload_bytes,
            eviction.payload_bytes);

        if (!stream.has_accepted) {
            stream.has_accepted = true;
            stream.first_source_sequence = source_sequence;
            stream.first_recv_monotonic_ns =
                recv_monotonic_ns;
        }
        stream.latest_source_sequence = source_sequence;
        stream.latest_recv_monotonic_ns =
            recv_monotonic_ns;

        if (resident_records_ > high_water_records_) {
            high_water_records_ = resident_records_;
        }
        if (resident_payload_bytes_ > high_water_payload_bytes_) {
            high_water_payload_bytes_ = resident_payload_bytes_;
        }
        if (stream.resident_records > stream.high_water_records) {
            stream.high_water_records = stream.resident_records;
        }
        if (stream.resident_payload_bytes >
            stream.high_water_payload_bytes) {
            stream.high_water_payload_bytes =
                stream.resident_payload_bytes;
        }
    }

    [[nodiscard]] const record_type* OldestRecordLocked(
        const StreamState& stream) const noexcept {
        if (!stream.sealed.empty()) {
            const ChunkSlice& first = stream.sealed.front();
            return &first.chunk->records_[first.begin];
        }
        if (stream.active != nullptr &&
            stream.active_begin < stream.active->records_.size()) {
            return &stream.active->records_[stream.active_begin];
        }
        return nullptr;
    }

    [[nodiscard]] SessionStreamWatermarkV1 StreamWatermarkLocked(
        std::size_t index) const noexcept {
        const StreamState& stream = streams_[index];
        SessionStreamWatermarkV1 result;
        result.key = config_.streams[index];
        result.counters = stream.counters;
        result.resident_records = stream.resident_records;
        result.resident_payload_bytes =
            stream.resident_payload_bytes;
        result.high_water_records = stream.high_water_records;
        result.high_water_payload_bytes =
            stream.high_water_payload_bytes;
        result.first_accepted_source_sequence =
            stream.first_source_sequence;
        result.first_accepted_recv_monotonic_ns =
            stream.first_recv_monotonic_ns;
        result.latest_accepted_source_sequence =
            stream.latest_source_sequence;
        result.latest_accepted_recv_monotonic_ns =
            stream.latest_recv_monotonic_ns;
        result.has_accepted_record = stream.has_accepted;
        if (config_.retention_mode ==
                SessionRetentionModeV1::kRecvMonotonicWindow &&
            stream.has_accepted &&
            stream.latest_recv_monotonic_ns >
                config_.recv_monotonic_window_ns) {
            result.retention_floor_recv_monotonic_ns =
                stream.latest_recv_monotonic_ns -
                config_.recv_monotonic_window_ns;
        }

        const record_type* oldest = OldestRecordLocked(stream);
        if (oldest != nullptr) {
            result.has_resident_record = true;
            result.oldest_resident_source_sequence =
                oldest->source_sequence;
            result.oldest_resident_recv_monotonic_ns =
                oldest->recv_monotonic_ns;
        }
        return result;
    }

    [[nodiscard]] SessionStoreWatermarkV1 WatermarkLocked()
        const noexcept {
        SessionStoreWatermarkV1 result;
        result.counters = counters_;
        result.resident_records = resident_records_;
        result.resident_payload_bytes = resident_payload_bytes_;
        result.high_water_records = high_water_records_;
        result.high_water_payload_bytes = high_water_payload_bytes_;
        for (std::size_t index = 0U;
             index < streams_.size();
             ++index) {
            result.streams[index] = StreamWatermarkLocked(index);
        }
        return result;
    }

    [[nodiscard]] SessionAppendResultV1 ResultLocked(
        SessionAppendErrorV1 error,
        std::size_t stream_index) const noexcept {
        SessionAppendResultV1 result;
        result.error = error;
        result.store_resident_records = resident_records_;
        result.store_resident_payload_bytes =
            resident_payload_bytes_;
        if (stream_index < streams_.size()) {
            const StreamState& stream = streams_[stream_index];
            result.source_stream_known = true;
            result.stream_resident_records =
                stream.resident_records;
            result.stream_resident_payload_bytes =
                stream.resident_payload_bytes;
            result.stream_latest_source_sequence =
                stream.latest_source_sequence;
            result.stream_latest_recv_monotonic_ns =
                stream.latest_recv_monotonic_ns;
        }
        return result;
    }

    void BuildStreamSnapshotLocked(
        StreamState& source,
        SessionStreamSnapshotV1<OwnedEvent>* output) const {
        const bool have_active =
            source.active != nullptr &&
            source.active_begin < source.active->records_.size();
        if (have_active) {
            source.sealed.push_back(ChunkSlice{
                chunk_ptr(source.active),
                source.active_begin,
                source.active->records_.size()});
        }
        if (source.active != nullptr) {
            source.active.reset();
            source.active_begin = 0U;
        }

        output->chunks_.reserve(source.sealed.size());
        for (const ChunkSlice& slice : source.sealed) {
            output->chunks_.push_back(chunk_view_type{
                slice.chunk, slice.begin, slice.end});
        }
    }

    SessionStoreConfigV1 config_{};
    mutable std::mutex mutex_;
    mutable SessionStoreCountersV1 counters_{};
    mutable std::array<StreamState,
                       kSessionStoreStreamCountV1>
        streams_{};
    mutable std::uint64_t resident_records_ = 0U;
    mutable std::uint64_t resident_payload_bytes_ = 0U;
    mutable std::uint64_t high_water_records_ = 0U;
    mutable std::uint64_t high_water_payload_bytes_ = 0U;
};

}  // namespace l2flow::market
