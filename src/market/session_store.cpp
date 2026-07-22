#include "l2flow/market/session_store.h"

namespace l2flow::market {

std::string_view SessionStoreCreateErrorNameV1(
    SessionStoreCreateErrorV1 error) noexcept {
    switch (error) {
        case SessionStoreCreateErrorV1::kNone:
            return "none";
        case SessionStoreCreateErrorV1::kNullOutput:
            return "null_output";
        case SessionStoreCreateErrorV1::kInvalidStreamKey:
            return "invalid_stream_key";
        case SessionStoreCreateErrorV1::kDuplicateSourceStream:
            return "duplicate_source_stream";
        case SessionStoreCreateErrorV1::kMixedCaptureDate:
            return "mixed_capture_date";
        case SessionStoreCreateErrorV1::kInvalidRetention:
            return "invalid_retention";
        case SessionStoreCreateErrorV1::kInvalidLimits:
            return "invalid_limits";
        case SessionStoreCreateErrorV1::kInvalidChunkCapacity:
            return "invalid_chunk_capacity";
        case SessionStoreCreateErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

std::string_view SessionAppendErrorNameV1(
    SessionAppendErrorV1 error) noexcept {
    switch (error) {
        case SessionAppendErrorV1::kNone:
            return "none";
        case SessionAppendErrorV1::kUnknownSourceStream:
            return "unknown_source_stream";
        case SessionAppendErrorV1::kStreamContextMismatch:
            return "stream_context_mismatch";
        case SessionAppendErrorV1::kInvalidSourceSequence:
            return "invalid_source_sequence";
        case SessionAppendErrorV1::kSourceSequenceNotIncreasing:
            return "source_sequence_not_increasing";
        case SessionAppendErrorV1::kRecvMonotonicRegression:
            return "recv_monotonic_regression";
        case SessionAppendErrorV1::kMaxRecordsExceeded:
            return "max_records_exceeded";
        case SessionAppendErrorV1::kMaxPayloadBytesExceeded:
            return "max_payload_bytes_exceeded";
        case SessionAppendErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case SessionAppendErrorV1::kCount:
            return "count";
    }
    return "unknown";
}

std::string_view SessionSnapshotErrorNameV1(
    SessionSnapshotErrorV1 error) noexcept {
    switch (error) {
        case SessionSnapshotErrorV1::kNone:
            return "none";
        case SessionSnapshotErrorV1::kNullOutput:
            return "null_output";
        case SessionSnapshotErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

}  // namespace l2flow::market
