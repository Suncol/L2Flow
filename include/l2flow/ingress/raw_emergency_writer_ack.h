#pragma once

#include "l2flow/ingress/byte_ring.h"
#include "l2flow/ingress/capture_metrics.h"
#include "l2flow/ingress/raw_capture_worker.h"
#include "l2flow/ingress/raw_ingress_config.h"
#include "l2flow/ingress/raw_wal_writer.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <utility>

namespace l2flow::ingress {

class RawFinalizationContinuationPosixV1;
class RawIngressApp;
class RawEmergencyWriterAckV1;

class RawEmergencyWriterAckControlV1 final {
public:
    RawEmergencyWriterAckControlV1() = default;

private:
    std::atomic<std::uint64_t> active_epoch_{0U};

    friend class RawEmergencyWriterAckV1;
    friend class RawIngressApp;
};

// Immutable, same-process facts frozen after the SDK has returned from
// Shutdown(), the callback handler has quiesced, and the sole Raw writer
// worker has acknowledged its emergency pause at a record boundary.
//
// This is deliberately not a durable wire object. In particular,
// same_process_ring_suffix_retained only describes the live RawIngressApp
// bound to the unconsumed capability; it is never evidence that volatile ring
// bytes survived a process exit.
struct RawEmergencyWriterAckFactsV1 final {
    RawIngressRuntimeState started_runtime{};
    RawWalSinkIdentityV1 writer{};
    CaptureMetricsSnapshot callback{};
    RawCaptureWorkerSnapshot capture{};
    RawWalWriterSnapshot wal{};

    std::uint64_t queued_record_count = 0U;
    std::uint64_t queued_framed_wal_bytes = 0U;
    std::uint64_t ring_published_position = 0U;
    std::uint64_t ring_consumed_position = 0U;
    std::uint64_t ring_used_bytes = 0U;

    bool sdk_shutdown_returned = false;
    bool callback_quiesced = false;
    bool regular_writer_mutation_stopped = false;
    bool same_process_ring_suffix_retained = false;

    [[nodiscard]] bool exact() const noexcept;
};

// An unforgeable, single-use in-process capability. Only a live
// RawIngressApp can construct one. Public users may inspect its immutable
// facts, but only the finalization continuation executor friend can consume
// the capability and obtain the exact paused writer/ring pair from the app.
class RawEmergencyWriterAckV1 final {
public:
    ~RawEmergencyWriterAckV1() = default;

    RawEmergencyWriterAckV1(
        const RawEmergencyWriterAckV1&) = delete;
    RawEmergencyWriterAckV1& operator=(
        const RawEmergencyWriterAckV1&) = delete;
    RawEmergencyWriterAckV1(
        RawEmergencyWriterAckV1&&) = delete;
    RawEmergencyWriterAckV1& operator=(
        RawEmergencyWriterAckV1&&) = delete;

    [[nodiscard]] const RawEmergencyWriterAckFactsV1&
    facts() const noexcept {
        return facts_;
    }

    [[nodiscard]] bool valid() const noexcept {
        return owner_ != nullptr && writer_ != nullptr &&
               ring_ != nullptr && control_ != nullptr &&
               epoch_ != 0U &&
               control_->active_epoch_.load(
                   std::memory_order_acquire) == epoch_ &&
               !consumed_ && facts_.exact();
    }

private:
    RawEmergencyWriterAckV1(
        RawEmergencyWriterAckFactsV1 facts,
        RawIngressApp* owner,
        RawWalSink* writer,
        ByteRing* ring,
        std::shared_ptr<
            RawEmergencyWriterAckControlV1> control,
        std::uint64_t epoch) noexcept
        : facts_(facts),
          owner_(owner),
          writer_(writer),
          ring_(ring),
          control_(std::move(control)),
          epoch_(epoch) {}

    RawEmergencyWriterAckFactsV1 facts_{};
    RawIngressApp* owner_ = nullptr;
    RawWalSink* writer_ = nullptr;
    ByteRing* ring_ = nullptr;
    std::shared_ptr<RawEmergencyWriterAckControlV1>
        control_;
    std::uint64_t epoch_ = 0U;
    bool consumed_ = false;

    friend class RawFinalizationContinuationPosixV1;
    friend class RawIngressApp;
};

}  // namespace l2flow::ingress
