#pragma once

#include "l2flow/ingress/capture_meta.h"
#include "l2flow/sdk/vendor_head_view.h"

#include <cstddef>
#include <cstdint>
#include <span>

namespace l2flow::ingress {

enum class FastCapturePublishResultV1 : std::uint8_t {
    kPublished = 0U,
    kFull,
    kStopped,
    kFatal,
};

// Optional callback-side copy hook. The callee must be bounded, nonblocking
// and noexcept. It owns any bytes needed after this call returns. Calls for
// one source_stream_id must be externally serialized by exactly one producer;
// different source_stream_id values may publish concurrently. CallbackHandler
// satisfies this contract with its per-source callback admission gate.
struct FastCaptureSinkRefV1 final {
    using TryPublishCopy = FastCapturePublishResultV1 (*)(
        void* context,
        const CaptureMetaV1& metadata,
        std::span<const std::byte, l2flow::sdk::kVendorHeadBytes> head,
        std::span<const std::byte> body) noexcept;
    using InvalidateGeneration = void (*)(
        void* context,
        std::uint32_t source_stream_id,
        std::uint64_t ingress_sequence) noexcept;

    void* context = nullptr;
    TryPublishCopy try_publish_copy = nullptr;
    InvalidateGeneration invalidate_generation = nullptr;

    [[nodiscard]] bool enabled() const noexcept {
        return context != nullptr && try_publish_copy != nullptr;
    }

    [[nodiscard]] bool valid() const noexcept {
        return context == nullptr
            ? try_publish_copy == nullptr &&
                  invalidate_generation == nullptr
            : try_publish_copy != nullptr &&
                  invalidate_generation != nullptr;
    }

    [[nodiscard]] FastCapturePublishResultV1 PublishCopy(
        const CaptureMetaV1& metadata,
        std::span<const std::byte, l2flow::sdk::kVendorHeadBytes> head,
        std::span<const std::byte> body) const noexcept {
        return enabled()
            ? try_publish_copy(context, metadata, head, body)
            : FastCapturePublishResultV1::kStopped;
    }

    void Invalidate(
        std::uint32_t source_stream_id,
        std::uint64_t ingress_sequence) const noexcept {
        if (enabled()) {
            invalidate_generation(
                context, source_stream_id, ingress_sequence);
        }
    }
};

}  // namespace l2flow::ingress
