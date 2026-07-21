#ifndef L2FLOW_INGRESS_CAPTURE_META_H_
#define L2FLOW_INGRESS_CAPTURE_META_H_

#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace l2flow::ingress {

inline constexpr std::size_t kVendorMessageHeadBytes = 23U;
inline constexpr std::size_t kEntryCommitLengthBytes =
    sizeof(std::uint32_t);

// Public in-memory schema for the metadata copied ahead of every vendor
// callback.  This structure deliberately uses the platform's natural
// alignment: unlike the vendor ABI, it must never be placed under pack(1).
struct CaptureMetaV1 final {
    std::uint32_t source_stream_id = 0U;
    std::uint32_t connection_epoch_hint = 0U;
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t recv_realtime_ns = 0U;
    std::uint64_t recv_monotonic_ns = 0U;
    std::uint32_t capture_date = 0U;
    std::uint32_t flags = 0U;
};

static_assert(std::is_standard_layout_v<CaptureMetaV1>);
static_assert(std::is_trivially_copyable_v<CaptureMetaV1>);
static_assert(alignof(CaptureMetaV1) == 8U);
static_assert(sizeof(CaptureMetaV1) == 40U);
static_assert(offsetof(CaptureMetaV1, source_stream_id) == 0U);
static_assert(offsetof(CaptureMetaV1, connection_epoch_hint) == 4U);
static_assert(offsetof(CaptureMetaV1, ingress_sequence) == 8U);
static_assert(offsetof(CaptureMetaV1, recv_realtime_ns) == 16U);
static_assert(offsetof(CaptureMetaV1, recv_monotonic_ns) == 24U);
static_assert(offsetof(CaptureMetaV1, capture_date) == 32U);
static_assert(offsetof(CaptureMetaV1, flags) == 36U);

inline constexpr std::size_t kMinimumByteRingEntryBytes =
    sizeof(CaptureMetaV1) + kVendorMessageHeadBytes +
    kEntryCommitLengthBytes;
static_assert(kMinimumByteRingEntryBytes == 67U);

}  // namespace l2flow::ingress

#endif  // L2FLOW_INGRESS_CAPTURE_META_H_
