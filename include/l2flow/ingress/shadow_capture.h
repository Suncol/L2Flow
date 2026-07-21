#ifndef L2FLOW_INGRESS_SHADOW_CAPTURE_H_
#define L2FLOW_INGRESS_SHADOW_CAPTURE_H_

#include "l2flow/ingress/byte_ring.h"
#include "l2flow/ingress/capture_metrics.h"
#include "l2flow/ops/fatal_latch.h"
#include "l2flow/sdk/subscription_manifest.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <type_traits>
#include <vector>

namespace l2flow::ingress {

inline constexpr std::uint32_t kShadowCaptureFormatVersion = 1U;
inline constexpr std::size_t kShadowCaptureRecordAlignment = 8U;
inline constexpr std::array<char, 8U> kShadowCaptureFileMagic{
    'L', '2', 'S', 'H', 'A', 'D', '1', '\0'};
inline constexpr std::array<char, 8U> kShadowCaptureRecordMagic{
    'L', '2', 'S', 'R', 'E', 'C', '1', '\0'};

// Phase-1's deliberately simple sequential-file prologue.  The structure is
// naturally aligned and contains no implicit padding.  Files are local-host
// temporary evidence; the portable, checksummed WAL replaces this format in
// Phase 2.
struct ShadowCaptureFileHeaderV1 final {
    std::array<char, 8U> magic = kShadowCaptureFileMagic;
    std::uint32_t format_version = kShadowCaptureFormatVersion;
    std::uint32_t header_bytes = sizeof(ShadowCaptureFileHeaderV1);
    std::uint32_t record_header_bytes = 0U;
    std::uint32_t capture_meta_bytes = 0U;
    std::uint32_t vendor_head_bytes = 0U;
    std::uint32_t source_stream_id = 0U;
    std::uint32_t max_message_bytes = 0U;
    std::uint32_t required_market_count = 0U;
    std::array<std::uint64_t, 3U> reserved{};
};

// Each record is:
//   ShadowCaptureRecordHeaderV1 + vendor head[23] + body + zero padding.
// record_bytes includes the trailing padding and therefore advances the next
// record to kShadowCaptureRecordAlignment.  CaptureMetaV1 is preserved
// verbatim, while vendor_message_bytes and padding_bytes make a reader able to
// validate and skip every record without decoding a vendor body.
struct ShadowCaptureRecordHeaderV1 final {
    std::array<char, 8U> magic = kShadowCaptureRecordMagic;
    std::uint32_t format_version = kShadowCaptureFormatVersion;
    std::uint32_t header_bytes = sizeof(ShadowCaptureRecordHeaderV1);
    std::uint64_t record_bytes = 0U;
    std::uint64_t sink_record_index = 0U;
    CaptureMetaV1 meta{};
    std::uint32_t vendor_message_bytes = 0U;
    std::uint32_t padding_bytes = 0U;
};

static_assert(std::is_standard_layout_v<ShadowCaptureFileHeaderV1>);
static_assert(std::is_trivially_copyable_v<ShadowCaptureFileHeaderV1>);
static_assert(alignof(ShadowCaptureFileHeaderV1) == 8U);
static_assert(sizeof(ShadowCaptureFileHeaderV1) == 64U);
static_assert(offsetof(ShadowCaptureFileHeaderV1, magic) == 0U);
static_assert(offsetof(ShadowCaptureFileHeaderV1, format_version) == 8U);
static_assert(offsetof(ShadowCaptureFileHeaderV1, header_bytes) == 12U);
static_assert(
    offsetof(ShadowCaptureFileHeaderV1, record_header_bytes) == 16U);
static_assert(
    offsetof(ShadowCaptureFileHeaderV1, required_market_count) == 36U);
static_assert(offsetof(ShadowCaptureFileHeaderV1, reserved) == 40U);

static_assert(std::is_standard_layout_v<ShadowCaptureRecordHeaderV1>);
static_assert(std::is_trivially_copyable_v<ShadowCaptureRecordHeaderV1>);
static_assert(alignof(ShadowCaptureRecordHeaderV1) == 8U);
static_assert(sizeof(ShadowCaptureRecordHeaderV1) == 80U);
static_assert(offsetof(ShadowCaptureRecordHeaderV1, magic) == 0U);
static_assert(
    offsetof(ShadowCaptureRecordHeaderV1, format_version) == 8U);
static_assert(offsetof(ShadowCaptureRecordHeaderV1, header_bytes) == 12U);
static_assert(offsetof(ShadowCaptureRecordHeaderV1, record_bytes) == 16U);
static_assert(
    offsetof(ShadowCaptureRecordHeaderV1, sink_record_index) == 24U);
static_assert(offsetof(ShadowCaptureRecordHeaderV1, meta) == 32U);
static_assert(
    offsetof(ShadowCaptureRecordHeaderV1, vendor_message_bytes) == 72U);
static_assert(offsetof(ShadowCaptureRecordHeaderV1, padding_bytes) == 76U);

struct ShadowCaptureConfig final {
    std::uint32_t source_stream_id = 0U;
    std::uint8_t market_service_id = 0U;
    std::vector<l2flow::sdk::MessageKey> required_market_messages;
};

struct ShadowCaptureStats final {
    std::uint64_t sink_records = 0U;
    std::uint64_t sink_vendor_bytes = 0U;
    std::uint64_t sink_file_bytes = 0U;
    std::uint64_t last_ingress_sequence = 0U;
    std::uint64_t logon_response_headers = 0U;
    std::uint64_t subscribe_response_headers = 0U;
    std::uint64_t logon_ok_responses = 0U;
    std::uint64_t logon_failed_responses = 0U;
    std::uint64_t malformed_control_responses = 0U;
    std::uint64_t required_first_seen_mask = 0U;
    std::uint64_t required_subscription_ok_mask = 0U;
    std::uint64_t required_subscription_failed_mask = 0U;
    std::uint64_t required_subscription_failure_observed_mask = 0U;
    std::uint64_t readiness_generation = 0U;
    bool latest_logon_ok = false;
};

struct ShadowCaptureReconciliation final {
    std::uint64_t callback_records = 0U;
    std::uint64_t sink_records = 0U;
    std::uint64_t callback_vendor_bytes = 0U;
    std::uint64_t sink_vendor_bytes = 0U;

    [[nodiscard]] bool exact() const noexcept {
        return callback_records == sink_records &&
               callback_vendor_bytes == sink_vendor_bytes;
    }
};

// A syscall-shaped output seam keeps short-write/EINTR/error behavior
// injectable without weakening the POSIX implementation.
struct ShadowOutputWriteResult final {
    std::size_t bytes_written = 0U;
    int error_number = 0;
};

class ShadowCaptureOutput {
public:
    virtual ~ShadowCaptureOutput() = default;

    [[nodiscard]] virtual ShadowOutputWriteResult WriteSome(
        std::span<const std::byte> bytes) noexcept = 0;
    // Return 0 on success, otherwise the errno-style failure value.
    [[nodiscard]] virtual int Fdatasync() noexcept = 0;
    [[nodiscard]] virtual int Close() noexcept = 0;
};

// Single-consumer Phase-1 sink.  The owner must stop SDK production and
// quiesce the callback handler before StopAndDrain(); Run() then exits only
// after an acquire-observed stop request and a second EMPTY ring observation.
class ShadowCaptureWriter final {
public:
    ShadowCaptureWriter(ShadowCaptureConfig config,
                        ByteRing& ring,
                        l2flow::ops::FatalLatch& fatal,
                        const std::string& output_path);
    ShadowCaptureWriter(ShadowCaptureConfig config,
                        ByteRing& ring,
                        l2flow::ops::FatalLatch& fatal,
                        std::unique_ptr<ShadowCaptureOutput> output);
    ~ShadowCaptureWriter();

    ShadowCaptureWriter(const ShadowCaptureWriter&) = delete;
    ShadowCaptureWriter& operator=(const ShadowCaptureWriter&) = delete;
    ShadowCaptureWriter(ShadowCaptureWriter&&) = delete;
    ShadowCaptureWriter& operator=(ShadowCaptureWriter&&) = delete;

    // Blocking consume loop.  Exactly one call is allowed.  Returns true only
    // after a clean stop-and-drain, complete sequential writes, fdatasync and
    // close.
    [[nodiscard]] bool Run() noexcept;

    // This is a release publication, not a producer close operation.  Call it
    // only after no callback can publish another ring entry, then join the
    // thread executing Run().
    void StopAndDrain() noexcept;

    [[nodiscard]] bool stop_requested() const noexcept;
    [[nodiscard]] bool startup_complete() const noexcept;
    [[nodiscard]] bool startup_succeeded() const noexcept;
    [[nodiscard]] bool finished() const noexcept;
    [[nodiscard]] ShadowCaptureStats Snapshot() const noexcept;
    [[nodiscard]] ShadowCaptureReconciliation Reconcile(
        const CaptureMetricsSnapshot& callback) const noexcept;
    [[nodiscard]] bool all_required_market_seen() const noexcept;
    [[nodiscard]] bool latest_logon_ok() const noexcept;
    [[nodiscard]] bool
    all_required_subscriptions_ok() const noexcept;
    [[nodiscard]] std::uint64_t required_market_mask() const noexcept;
    // Zero means that the current connection is not ready. A nonzero value
    // identifies the LogonResponse generation whose complete evidence is
    // currently ready; it changes across every accepted replacement logon.
    [[nodiscard]] std::uint64_t readiness_generation() const noexcept;

private:
    void ValidateConfig();
    [[nodiscard]] bool WriteFileHeader() noexcept;
    [[nodiscard]] bool ValidateRecord(
        const ByteRingRecord& record) noexcept;
    [[nodiscard]] bool WriteRecord(
        const ByteRingRecord& record) noexcept;
    [[nodiscard]] bool ObserveRecord(
        const ByteRingRecord& record) noexcept;
    [[nodiscard]] bool ObserveSubscriptionStatuses(
        std::span<const std::byte> body,
        std::size_t services_list_offset,
        std::size_t minimum_services_start,
        bool apply_statuses) noexcept;
    [[nodiscard]] bool WriteAll(
        std::span<const std::byte> bytes) noexcept;
    [[nodiscard]] bool FinalizeOutput() noexcept;
    void RefreshReadinessGeneration() noexcept;
    void TripRingCorruption() noexcept;
    void TripOutputFailure() noexcept;

    ShadowCaptureConfig config_;
    ByteRing& ring_;
    l2flow::ops::FatalLatch& fatal_;
    std::unique_ptr<ShadowCaptureOutput> output_;
    ByteRingRecord record_;
    std::uint64_t required_mask_ = 0U;
    std::uint64_t previous_ingress_sequence_ = 0U;
    std::uint64_t current_logon_generation_ = 0U;

    std::atomic<bool> stop_requested_{false};
    std::atomic<bool> run_started_{false};
    std::atomic<bool> startup_complete_{false};
    std::atomic<bool> startup_succeeded_{false};
    std::atomic<bool> finished_{false};
    bool output_closed_ = false;

    std::atomic<std::uint64_t> sink_records_{0U};
    std::atomic<std::uint64_t> sink_vendor_bytes_{0U};
    std::atomic<std::uint64_t> sink_file_bytes_{0U};
    std::atomic<std::uint64_t> last_ingress_sequence_{0U};
    std::atomic<std::uint64_t> logon_response_headers_{0U};
    std::atomic<std::uint64_t> subscribe_response_headers_{0U};
    std::atomic<std::uint64_t> logon_ok_responses_{0U};
    std::atomic<std::uint64_t> logon_failed_responses_{0U};
    std::atomic<std::uint64_t> malformed_control_responses_{0U};
    std::atomic<std::uint64_t> required_first_seen_mask_{0U};
    std::atomic<std::uint64_t> required_subscription_ok_mask_{0U};
    std::atomic<std::uint64_t> required_subscription_failed_mask_{0U};
    std::atomic<std::uint64_t>
        required_subscription_failure_observed_mask_{0U};
    std::atomic<std::uint64_t> readiness_generation_{0U};
    std::atomic<bool> latest_logon_ok_{false};
};

}  // namespace l2flow::ingress

#endif  // L2FLOW_INGRESS_SHADOW_CAPTURE_H_
