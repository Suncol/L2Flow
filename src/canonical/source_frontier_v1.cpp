#include "l2flow/canonical/source_frontier_v1.h"

#include <algorithm>
#include <bit>
#include <chrono>
#include <cstring>
#include <limits>
#include <thread>

namespace l2flow::canonical {
namespace {

constexpr std::size_t kMagic = 0U;
constexpr std::size_t kVersion = 4U;
constexpr std::size_t kPageBytes = 6U;
constexpr std::size_t kLayoutGeneration = 8U;
constexpr std::size_t kSourceStreamId = 16U;
constexpr std::size_t kCaptureDate = 20U;
constexpr std::size_t kSourceState = 24U;
constexpr std::size_t kClockAlgorithm = 28U;
constexpr std::size_t kStreamDayId = 32U;
constexpr std::size_t kClockDigest = 48U;
constexpr std::size_t kClockLabel = 80U;
constexpr std::size_t kWriterInstance = 88U;
constexpr std::size_t kGeneration = 104U;
constexpr std::size_t kCapturedSequence = 112U;
constexpr std::size_t kAppendSequence = 120U;
constexpr std::size_t kProcessedSequence = 128U;
constexpr std::size_t kAppendWal = 136U;
constexpr std::size_t kProcessedWal = 144U;
constexpr std::size_t kLastRecv = 152U;
constexpr std::size_t kSafeFrontier = 160U;
constexpr std::size_t kCallbackInflight = 168U;
constexpr std::size_t kQualityFlags = 176U;
constexpr std::size_t kCallbackGeneration = 184U;
constexpr std::size_t kProgressGeneration = 192U;
constexpr std::size_t kFatalLatch = 200U;

static_assert(kFatalLatch + sizeof(std::uint64_t) <=
              kSourceFrontierPageBytesV1);

constexpr std::chrono::nanoseconds kProgressAccessTimeout =
    std::chrono::milliseconds{5};
constexpr std::size_t kProgressFastAttempts = 64U;

template <typename Value>
[[nodiscard]] Value AtomicLoad(
    const SourceFrontierPageV1& page,
    std::size_t offset,
    int ordering = __ATOMIC_ACQUIRE) noexcept {
    const auto* pointer = reinterpret_cast<const Value*>(
        page.bytes.data() + static_cast<std::ptrdiff_t>(offset));
    return __atomic_load_n(pointer, ordering);
}

template <typename Value>
void AtomicStore(
    SourceFrontierPageV1* page,
    std::size_t offset,
    Value value,
    int ordering = __ATOMIC_RELEASE) noexcept {
    auto* pointer = reinterpret_cast<Value*>(
        page->bytes.data() + static_cast<std::ptrdiff_t>(offset));
    __atomic_store_n(pointer, value, ordering);
}

template <typename Value>
[[nodiscard]] bool AtomicCompareExchange(
    SourceFrontierPageV1* page,
    std::size_t offset,
    Value* expected,
    Value desired,
    int success_order,
    int failure_order) noexcept {
    auto* pointer = reinterpret_cast<Value*>(
        page->bytes.data() + static_cast<std::ptrdiff_t>(offset));
    return __atomic_compare_exchange_n(
        pointer,
        expected,
        desired,
        false,
        success_order,
        failure_order);
}

[[nodiscard]] SourceFrontierErrorV1 AcquireProgressWrite(
    SourceFrontierPageV1* page,
    std::uint64_t* release_generation) noexcept {
    if (page == nullptr || release_generation == nullptr) {
        return SourceFrontierErrorV1::kNullArgument;
    }
    const auto deadline =
        std::chrono::steady_clock::now() + kProgressAccessTimeout;
    for (std::size_t attempt = 0U;; ++attempt) {
        std::uint64_t current = AtomicLoad<std::uint64_t>(
            *page, kProgressGeneration, __ATOMIC_ACQUIRE);
        if ((current & 1U) == 0U) {
            if (current > std::numeric_limits<std::uint64_t>::max() - 2U) {
                return SourceFrontierErrorV1::kCounterOverflow;
            }
            if (AtomicCompareExchange(
                    page,
                    kProgressGeneration,
                    &current,
                    current + 1U,
                    __ATOMIC_ACQ_REL,
                    __ATOMIC_ACQUIRE)) {
                *release_generation = current + 2U;
                return SourceFrontierErrorV1::kNone;
            }
        }
        if (attempt >= kProgressFastAttempts) {
            if (std::chrono::steady_clock::now() >= deadline) {
                return SourceFrontierErrorV1::kBusy;
            }
            std::this_thread::yield();
        }
    }
}

void ReleaseProgressWrite(
    SourceFrontierPageV1* page,
    std::uint64_t release_generation) noexcept {
    AtomicStore(
        page,
        kProgressGeneration,
        release_generation,
        __ATOMIC_RELEASE);
}

[[nodiscard]] bool ValidBusyTimeout(
    std::chrono::nanoseconds timeout) noexcept {
    return timeout.count() > 0 &&
           timeout <= kSourceFrontierMaximumBusyTimeoutV1;
}

[[nodiscard]] SourceFrontierErrorV1 AcquireProgressWriteWithRetry(
    SourceFrontierPageV1* page,
    std::chrono::nanoseconds timeout,
    std::uint64_t* release_generation) noexcept {
    if (!ValidBusyTimeout(timeout)) {
        return SourceFrontierErrorV1::kInvalidConfiguration;
    }
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        const SourceFrontierErrorV1 error =
            AcquireProgressWrite(page, release_generation);
        if (error != SourceFrontierErrorV1::kBusy ||
            std::chrono::steady_clock::now() >= deadline) {
            return error;
        }
    }
}

template <std::size_t Size>
void StoreAtomicBytes(
    SourceFrontierPageV1* page,
    std::size_t offset,
    const std::array<std::byte, Size>& value) noexcept {
    static_assert(Size % sizeof(std::uint64_t) == 0U);
    for (std::size_t index = 0U; index < Size;
         index += sizeof(std::uint64_t)) {
        std::uint64_t word = 0U;
        std::memcpy(&word, value.data() + index, sizeof(word));
        AtomicStore(page, offset + index, word, __ATOMIC_RELAXED);
    }
}

template <std::size_t Size>
[[nodiscard]] std::array<std::byte, Size> LoadAtomicBytes(
    const SourceFrontierPageV1& page,
    std::size_t offset) noexcept {
    static_assert(Size % sizeof(std::uint64_t) == 0U);
    std::array<std::byte, Size> result{};
    for (std::size_t index = 0U; index < Size;
         index += sizeof(std::uint64_t)) {
        const std::uint64_t word = AtomicLoad<std::uint64_t>(
            page, offset + index, __ATOMIC_ACQUIRE);
        std::memcpy(result.data() + index, &word, sizeof(word));
    }
    return result;
}

[[nodiscard]] bool IdentityArgumentsValid(
    const l2flow::common::Identity128& writer_instance,
    std::uint64_t generation) noexcept {
    return !l2flow::common::IsZeroIdentity(writer_instance) &&
           generation != 0U;
}

[[nodiscard]] bool PageIdentityMatches(
    const SourceFrontierPageV1& page,
    const l2flow::common::Identity128& writer_instance,
    std::uint64_t generation) noexcept {
    return LoadAtomicBytes<16U>(page, kWriterInstance) == writer_instance &&
           AtomicLoad<std::uint64_t>(page, kGeneration) == generation;
}

[[nodiscard]] bool FatalLatched(
    const SourceFrontierPageV1& page) noexcept {
    return AtomicLoad<std::uint64_t>(
               page, kFatalLatch, __ATOMIC_ACQUIRE) != 0U;
}

void LatchFatal(SourceFrontierPageV1* page) noexcept {
    AtomicStore(
        page, kFatalLatch, std::uint64_t{1U}, __ATOMIC_RELEASE);
}

[[nodiscard]] bool DigestNonzero(
    const l2flow::common::Sha256Digest& digest) noexcept {
    return std::any_of(
        digest.begin(), digest.end(),
        [](std::byte value) { return value != std::byte{0}; });
}

[[nodiscard]] bool ValidClock(
    const ClockEpochIdentityV1& value) noexcept {
    return value.algorithm != 0U && DigestNonzero(value.digest);
}

[[nodiscard]] bool ValidState(SourceStateV1 state) noexcept {
    switch (state) {
        case SourceStateV1::kRecovering:
        case SourceStateV1::kHealthy:
        case SourceStateV1::kDisconnected:
        case SourceStateV1::kFatal:
            return true;
    }
    return false;
}

[[nodiscard]] bool ValidTransition(
    SourceStateV1 from,
    SourceStateV1 to) noexcept {
    if (!ValidState(from) || !ValidState(to)) {
        return false;
    }
    if (from == SourceStateV1::kFatal) {
        return to == SourceStateV1::kFatal;
    }
    if (from == SourceStateV1::kDisconnected) {
        return to == SourceStateV1::kDisconnected ||
               to == SourceStateV1::kRecovering ||
               to == SourceStateV1::kFatal;
    }
    return true;
}

[[nodiscard]] bool HeaderValid(
    const SourceFrontierPageV1& page) noexcept {
    return std::endian::native == std::endian::little &&
           AtomicLoad<std::uint32_t>(page, kMagic) ==
               kSourceFrontierPageMagicV1 &&
           AtomicLoad<std::uint16_t>(page, kVersion) ==
               kSourceFrontierPageVersionV1 &&
           AtomicLoad<std::uint16_t>(page, kPageBytes) ==
               kSourceFrontierPageBytesV1 &&
           AtomicLoad<std::uint64_t>(page, kLayoutGeneration) == 2U;
}

[[nodiscard]] bool SameIdentity(
    const SourceFrontierV1& left,
    const SourceFrontierV1& right) noexcept {
    return left.source_stream_id == right.source_stream_id &&
           left.capture_date == right.capture_date &&
           left.stream_day_id == right.stream_day_id &&
           left.clock_epoch == right.clock_epoch &&
           left.writer_instance == right.writer_instance &&
           left.generation == right.generation;
}

[[nodiscard]] bool SameProgress(
    const SourceFrontierV1& left,
    const SourceFrontierV1& right) noexcept {
    return left.captured_ingress_sequence ==
               right.captured_ingress_sequence &&
           left.append_ingress_sequence ==
               right.append_ingress_sequence &&
           left.processed_ingress_sequence ==
               right.processed_ingress_sequence &&
           left.append_global_wal_pos ==
               right.append_global_wal_pos &&
           left.processed_global_wal_pos ==
               right.processed_global_wal_pos &&
           left.last_appended_recv_monotonic_ns ==
               right.last_appended_recv_monotonic_ns &&
           left.callback_generation == right.callback_generation &&
           left.progress_generation == right.progress_generation;
}

void AtomicMaxSafeFrontier(
    SourceFrontierPageV1* page,
    std::int64_t candidate) noexcept {
    std::uint64_t current = AtomicLoad<std::uint64_t>(
        *page, kSafeFrontier, __ATOMIC_ACQUIRE);
    const std::uint64_t desired = static_cast<std::uint64_t>(candidate);
    while (static_cast<std::int64_t>(current) < candidate &&
           !AtomicCompareExchange(
               page,
               kSafeFrontier,
               &current,
               desired,
               __ATOMIC_RELEASE,
               __ATOMIC_ACQUIRE)) {
    }
}

}  // namespace

const char* SourceFrontierErrorNameV1(
    SourceFrontierErrorV1 error) noexcept {
    switch (error) {
        case SourceFrontierErrorV1::kNone:
            return "NONE";
        case SourceFrontierErrorV1::kNullArgument:
            return "NULL_ARGUMENT";
        case SourceFrontierErrorV1::kUnsupportedHost:
            return "UNSUPPORTED_HOST";
        case SourceFrontierErrorV1::kInvalidConfiguration:
            return "INVALID_CONFIGURATION";
        case SourceFrontierErrorV1::kAlreadyInitialized:
            return "ALREADY_INITIALIZED";
        case SourceFrontierErrorV1::kInvalidPage:
            return "INVALID_PAGE";
        case SourceFrontierErrorV1::kIdentityChanged:
            return "IDENTITY_CHANGED";
        case SourceFrontierErrorV1::kInvalidState:
            return "INVALID_STATE";
        case SourceFrontierErrorV1::kCounterOverflow:
            return "COUNTER_OVERFLOW";
        case SourceFrontierErrorV1::kIngressRegression:
            return "INGRESS_REGRESSION";
        case SourceFrontierErrorV1::kWalRegression:
            return "WAL_REGRESSION";
        case SourceFrontierErrorV1::kReceiveTimeRegression:
            return "RECEIVE_TIME_REGRESSION";
        case SourceFrontierErrorV1::kNotCaptured:
            return "NOT_CAPTURED";
        case SourceFrontierErrorV1::kNotAppended:
            return "NOT_APPENDED";
        case SourceFrontierErrorV1::kBusy:
            return "BUSY";
    }
    return "UNKNOWN";
}

bool SourceFrontierAtomicsLockFreeV1() noexcept {
    return std::endian::native == std::endian::little &&
           __atomic_always_lock_free(sizeof(std::uint16_t), nullptr) &&
           __atomic_always_lock_free(sizeof(std::uint32_t), nullptr) &&
           __atomic_always_lock_free(sizeof(std::uint64_t), nullptr);
}

SourceFrontierErrorV1 InitializeSourceFrontierPageV1(
    const SourceFrontierConfigV1& config,
    SourceFrontierPageV1* page) noexcept {
    if (page == nullptr) {
        return SourceFrontierErrorV1::kNullArgument;
    }
    if (!SourceFrontierAtomicsLockFreeV1()) {
        return SourceFrontierErrorV1::kUnsupportedHost;
    }
    if (config.source_stream_id == 0U || config.capture_date == 0U ||
        l2flow::common::IsZeroIdentity(config.stream_day_id) ||
        l2flow::common::IsZeroIdentity(config.writer_instance) ||
        !ValidClock(config.clock_epoch) || config.generation == 0U ||
        !ValidState(config.initial_state) ||
        (config.initial_ingress_sequence != 0U &&
         config.initial_global_wal_pos == 0U) ||
        (config.initial_quality_flags &
         ~kCanonicalQualityFlagsMaskV1) != 0U) {
        return SourceFrontierErrorV1::kInvalidConfiguration;
    }

    // A physical page belongs to exactly one immutable writer generation.
    // Reusing it while a stale callback/publisher still holds a pointer would
    // make generation fencing impossible, so initialization is one-shot.
    if (!std::all_of(
            page->bytes.begin(), page->bytes.end(), [](std::byte value) {
                return value == std::byte{0U};
            })) {
        return SourceFrontierErrorV1::kAlreadyInitialized;
    }
    std::uint64_t expected_layout = 0U;
    if (!AtomicCompareExchange(
            page,
            kLayoutGeneration,
            &expected_layout,
            std::uint64_t{1U},
            __ATOMIC_ACQ_REL,
            __ATOMIC_ACQUIRE)) {
        return SourceFrontierErrorV1::kAlreadyInitialized;
    }
    AtomicStore(page, kMagic, kSourceFrontierPageMagicV1,
                __ATOMIC_RELAXED);
    AtomicStore(page, kVersion, kSourceFrontierPageVersionV1,
                __ATOMIC_RELAXED);
    AtomicStore(
        page,
        kPageBytes,
        static_cast<std::uint16_t>(kSourceFrontierPageBytesV1),
        __ATOMIC_RELAXED);
    AtomicStore(page, kSourceStreamId, config.source_stream_id,
                __ATOMIC_RELAXED);
    AtomicStore(page, kCaptureDate, config.capture_date,
                __ATOMIC_RELAXED);
    AtomicStore(
        page,
        kSourceState,
        static_cast<std::uint32_t>(config.initial_state),
        __ATOMIC_RELAXED);
    AtomicStore(page, kClockAlgorithm, config.clock_epoch.algorithm,
                __ATOMIC_RELAXED);
    StoreAtomicBytes(page, kStreamDayId, config.stream_day_id);
    StoreAtomicBytes(page, kClockDigest, config.clock_epoch.digest);
    AtomicStore(page, kClockLabel, config.clock_epoch.label,
                __ATOMIC_RELAXED);
    StoreAtomicBytes(page, kWriterInstance, config.writer_instance);
    AtomicStore(page, kGeneration, config.generation,
                __ATOMIC_RELAXED);
    AtomicStore(
        page,
        kCapturedSequence,
        config.initial_ingress_sequence,
        __ATOMIC_RELAXED);
    AtomicStore(
        page,
        kAppendSequence,
        config.initial_ingress_sequence,
        __ATOMIC_RELAXED);
    AtomicStore(
        page,
        kProcessedSequence,
        config.initial_ingress_sequence,
        __ATOMIC_RELAXED);
    AtomicStore(
        page,
        kAppendWal,
        config.initial_global_wal_pos,
        __ATOMIC_RELAXED);
    AtomicStore(
        page,
        kProcessedWal,
        config.initial_global_wal_pos,
        __ATOMIC_RELAXED);
    AtomicStore(
        page,
        kQualityFlags,
        config.initial_quality_flags,
        __ATOMIC_RELAXED);
    AtomicStore(
        page,
        kCallbackGeneration,
        std::uint64_t{0U},
        __ATOMIC_RELAXED);
    AtomicStore(
        page,
        kProgressGeneration,
        std::uint64_t{2U},
        __ATOMIC_RELAXED);
    AtomicStore(
        page,
        kFatalLatch,
        config.initial_state == SourceStateV1::kFatal ? 1U : 0U,
        __ATOMIC_RELAXED);
    AtomicStore(page, kLayoutGeneration, std::uint64_t{2U});
    return SourceFrontierErrorV1::kNone;
}

SourceFrontierErrorV1 ReadSourceFrontierV1(
    const SourceFrontierPageV1& page,
    SourceFrontierV1* output) noexcept {
    if (output == nullptr) {
        return SourceFrontierErrorV1::kNullArgument;
    }
    if (!HeaderValid(page)) {
        return SourceFrontierErrorV1::kInvalidPage;
    }
    // Callback entry/completion and append/processed publication are short
    // seqlock writes, but the writer may be descheduled while it owns the odd
    // generation.  Use elapsed monotonic time rather than a CPU-iteration
    // count.  The wait remains bounded so a crashed writer cannot block a
    // reader forever; expiry is reported as retryable BUSY, never as an
    // identity change.
    const auto deadline =
        std::chrono::steady_clock::now() + kProgressAccessTimeout;
    for (std::size_t attempt = 0U;; ++attempt) {
        const std::uint64_t layout_before = AtomicLoad<std::uint64_t>(
            page, kLayoutGeneration, __ATOMIC_ACQUIRE);
        if ((layout_before & 1U) != 0U) {
            if (attempt >= kProgressFastAttempts) {
                if (std::chrono::steady_clock::now() >= deadline) {
                    return SourceFrontierErrorV1::kBusy;
                }
                std::this_thread::yield();
            }
            continue;
        }
        const std::uint64_t callback_generation_before =
            AtomicLoad<std::uint64_t>(
                page, kCallbackGeneration, __ATOMIC_ACQUIRE);
        const std::uint64_t progress_generation_before =
            AtomicLoad<std::uint64_t>(
                page, kProgressGeneration, __ATOMIC_ACQUIRE);
        if ((progress_generation_before & 1U) != 0U) {
            if (attempt >= kProgressFastAttempts) {
                if (std::chrono::steady_clock::now() >= deadline) {
                    return SourceFrontierErrorV1::kBusy;
                }
                std::this_thread::yield();
            }
            continue;
        }
        SourceFrontierV1 candidate;
        candidate.source_stream_id =
            AtomicLoad<std::uint32_t>(page, kSourceStreamId);
        candidate.capture_date =
            AtomicLoad<std::uint32_t>(page, kCaptureDate);
        candidate.stream_day_id =
            LoadAtomicBytes<16U>(page, kStreamDayId);
        candidate.clock_epoch.algorithm =
            AtomicLoad<std::uint32_t>(page, kClockAlgorithm);
        candidate.clock_epoch.digest =
            LoadAtomicBytes<32U>(page, kClockDigest);
        candidate.clock_epoch.label =
            AtomicLoad<std::uint64_t>(page, kClockLabel);
        candidate.writer_instance =
            LoadAtomicBytes<16U>(page, kWriterInstance);
        candidate.generation =
            AtomicLoad<std::uint64_t>(page, kGeneration);
        candidate.captured_ingress_sequence =
            AtomicLoad<std::uint64_t>(page, kCapturedSequence);
        candidate.append_ingress_sequence =
            AtomicLoad<std::uint64_t>(page, kAppendSequence);
        candidate.processed_ingress_sequence =
            AtomicLoad<std::uint64_t>(page, kProcessedSequence);
        candidate.append_global_wal_pos =
            AtomicLoad<std::uint64_t>(page, kAppendWal);
        candidate.processed_global_wal_pos =
            AtomicLoad<std::uint64_t>(page, kProcessedWal);
        candidate.last_appended_recv_monotonic_ns =
            static_cast<std::int64_t>(
                AtomicLoad<std::uint64_t>(page, kLastRecv));
        candidate.safe_processed_frontier_ns =
            static_cast<std::int64_t>(
                AtomicLoad<std::uint64_t>(page, kSafeFrontier));
        candidate.callback_inflight =
            AtomicLoad<std::uint64_t>(page, kCallbackInflight);
        const SourceStateV1 stored_state = static_cast<SourceStateV1>(
            AtomicLoad<std::uint32_t>(page, kSourceState));
        candidate.source_state = stored_state;
        candidate.quality_flags =
            AtomicLoad<std::uint64_t>(page, kQualityFlags);
        const std::uint64_t fatal_latch = AtomicLoad<std::uint64_t>(
            page, kFatalLatch, __ATOMIC_ACQUIRE);
        const std::uint64_t callback_generation_after =
            AtomicLoad<std::uint64_t>(
                page, kCallbackGeneration, __ATOMIC_ACQUIRE);
        candidate.callback_generation = callback_generation_after;
        const std::uint64_t progress_generation_after =
            AtomicLoad<std::uint64_t>(
                page, kProgressGeneration, __ATOMIC_ACQUIRE);
        candidate.progress_generation = progress_generation_after;
        const std::uint64_t layout_after = AtomicLoad<std::uint64_t>(
            page, kLayoutGeneration, __ATOMIC_ACQUIRE);
        if (layout_before != layout_after ||
            callback_generation_before != callback_generation_after ||
            progress_generation_before != progress_generation_after ||
            (progress_generation_after & 1U) != 0U ||
            (layout_after & 1U) != 0U) {
            if (attempt >= kProgressFastAttempts) {
                if (std::chrono::steady_clock::now() >= deadline) {
                    return SourceFrontierErrorV1::kBusy;
                }
                std::this_thread::yield();
            }
            continue;
        }
        if (candidate.source_stream_id == 0U ||
            candidate.capture_date == 0U ||
            l2flow::common::IsZeroIdentity(candidate.stream_day_id) ||
            l2flow::common::IsZeroIdentity(candidate.writer_instance) ||
            !ValidClock(candidate.clock_epoch) ||
            candidate.generation == 0U ||
            !ValidState(stored_state) || fatal_latch > 1U ||
            (candidate.quality_flags &
             ~kCanonicalQualityFlagsMaskV1) != 0U ||
            candidate.last_appended_recv_monotonic_ns < 0 ||
            candidate.safe_processed_frontier_ns < 0 ||
            candidate.captured_ingress_sequence <
                candidate.append_ingress_sequence ||
            candidate.append_ingress_sequence <
                candidate.processed_ingress_sequence ||
            candidate.append_global_wal_pos <
                candidate.processed_global_wal_pos ||
            (candidate.append_ingress_sequence != 0U &&
             candidate.append_global_wal_pos == 0U) ||
            (candidate.processed_ingress_sequence != 0U &&
             candidate.processed_global_wal_pos == 0U) ||
            (candidate.append_ingress_sequence >
                 candidate.processed_ingress_sequence &&
             candidate.append_global_wal_pos <=
                 candidate.processed_global_wal_pos) ||
            (candidate.append_ingress_sequence >
                 candidate.processed_ingress_sequence &&
             candidate.last_appended_recv_monotonic_ns <
                 candidate.safe_processed_frontier_ns)) {
            return SourceFrontierErrorV1::kInvalidPage;
        }
        if (fatal_latch != 0U) {
            candidate.source_state = SourceStateV1::kFatal;
        }
        *output = candidate;
        return SourceFrontierErrorV1::kNone;
    }
}

SourceFrontierErrorV1 PublishSourceStateV1(
    SourceFrontierPageV1* page,
    const l2flow::common::Identity128& expected_writer_instance,
    std::uint64_t expected_generation,
    SourceStateV1 state,
    std::uint64_t quality_flags) noexcept {
    if (page == nullptr) {
        return SourceFrontierErrorV1::kNullArgument;
    }
    if (!HeaderValid(*page)) {
        return SourceFrontierErrorV1::kInvalidPage;
    }
    if (!ValidState(state) ||
        (quality_flags & ~kCanonicalQualityFlagsMaskV1) != 0U) {
        return SourceFrontierErrorV1::kInvalidState;
    }
    if (!IdentityArgumentsValid(
            expected_writer_instance, expected_generation) ||
        !PageIdentityMatches(
            *page, expected_writer_instance, expected_generation)) {
        return SourceFrontierErrorV1::kIdentityChanged;
    }
    // FATAL must never be lost merely because another progress mutation holds
    // the bounded seqlock.  The monotonic latch is visible to every reader and
    // mutator even if the diagnostic state/quality update below cannot lock.
    if (state == SourceStateV1::kFatal) {
        LatchFatal(page);
    }
    std::uint64_t release_generation = 0U;
    const SourceFrontierErrorV1 lock_error = AcquireProgressWrite(
        page, &release_generation);
    if (lock_error != SourceFrontierErrorV1::kNone) {
        return lock_error;
    }
    const auto finish = [&](SourceFrontierErrorV1 error) noexcept {
        ReleaseProgressWrite(page, release_generation);
        return error;
    };
    if (!PageIdentityMatches(
            *page, expected_writer_instance, expected_generation)) {
        return finish(SourceFrontierErrorV1::kIdentityChanged);
    }
    if (FatalLatched(*page) && state != SourceStateV1::kFatal) {
        return finish(SourceFrontierErrorV1::kInvalidState);
    }
    const SourceStateV1 stored = static_cast<SourceStateV1>(
        AtomicLoad<std::uint32_t>(*page, kSourceState));
    if (!ValidState(stored)) {
        LatchFatal(page);
        return finish(SourceFrontierErrorV1::kInvalidPage);
    }
    const SourceStateV1 current = FatalLatched(*page)
        ? SourceStateV1::kFatal
        : stored;
    if (!ValidTransition(current, state)) {
        return finish(SourceFrontierErrorV1::kInvalidState);
    }
    AtomicStore(page, kQualityFlags, quality_flags, __ATOMIC_RELAXED);
    AtomicStore(
        page,
        kSourceState,
        static_cast<std::uint32_t>(state),
        __ATOMIC_RELAXED);
    return finish(SourceFrontierErrorV1::kNone);
}

SourceFrontierCallbackGuardV1::SourceFrontierCallbackGuardV1(
    SourceFrontierPageV1* page,
    const l2flow::common::Identity128& expected_writer_instance,
    std::uint64_t expected_generation,
    std::chrono::nanoseconds busy_timeout) noexcept
    : page_(page),
      expected_writer_instance_(expected_writer_instance),
      expected_generation_(expected_generation),
      busy_timeout_(busy_timeout) {
    if (page_ == nullptr) {
        error_ = SourceFrontierErrorV1::kNullArgument;
        return;
    }
    if (!HeaderValid(*page_)) {
        error_ = SourceFrontierErrorV1::kInvalidPage;
        return;
    }
    if (!IdentityArgumentsValid(
            expected_writer_instance_, expected_generation_)) {
        error_ = SourceFrontierErrorV1::kIdentityChanged;
        return;
    }
    if (!ValidBusyTimeout(busy_timeout_)) {
        error_ = SourceFrontierErrorV1::kInvalidConfiguration;
        return;
    }
    std::uint64_t release_generation = 0U;
    error_ = AcquireProgressWriteWithRetry(
        page_, busy_timeout_, &release_generation);
    if (error_ != SourceFrontierErrorV1::kNone) {
        return;
    }
    const auto finish = [&](SourceFrontierErrorV1 error) noexcept {
        error_ = error;
        ReleaseProgressWrite(page_, release_generation);
    };
    if (!PageIdentityMatches(
            *page_, expected_writer_instance_, expected_generation_)) {
        finish(SourceFrontierErrorV1::kIdentityChanged);
        return;
    }
    const SourceStateV1 state = static_cast<SourceStateV1>(
        AtomicLoad<std::uint32_t>(*page_, kSourceState));
    if (!ValidState(state)) {
        LatchFatal(page_);
        finish(SourceFrontierErrorV1::kInvalidPage);
        return;
    }
    if (FatalLatched(*page_) || state == SourceStateV1::kFatal) {
        finish(SourceFrontierErrorV1::kInvalidState);
        return;
    }
    const std::uint64_t callback_generation =
        AtomicLoad<std::uint64_t>(*page_, kCallbackGeneration);
    const std::uint64_t callback_inflight =
        AtomicLoad<std::uint64_t>(*page_, kCallbackInflight);
    if (callback_generation == std::numeric_limits<std::uint64_t>::max() ||
        callback_inflight == std::numeric_limits<std::uint64_t>::max()) {
        LatchFatal(page_);
        finish(SourceFrontierErrorV1::kCounterOverflow);
        return;
    }
    // Entry generation is release-published before construction returns, so
    // it precedes any receive-time sample made by the callback body.
    AtomicStore(
        page_, kCallbackGeneration, callback_generation + 1U,
        __ATOMIC_RELEASE);
    AtomicStore(
        page_, kCallbackInflight, callback_inflight + 1U,
        __ATOMIC_RELEASE);
    entered_ = true;
    finish(SourceFrontierErrorV1::kNone);
}

SourceFrontierCallbackGuardV1::~SourceFrontierCallbackGuardV1() {
    if (!entered_ || completed_) {
        return;
    }
    if (!PageIdentityMatches(
            *page_, expected_writer_instance_, expected_generation_)) {
        return;
    }
    // This lock-free latch is the safety boundary.  If the bounded progress
    // lock below is unavailable, inflight deliberately remains nonzero as a
    // second fail-closed signal instead of pretending the callback completed.
    LatchFatal(page_);
    std::uint64_t release_generation = 0U;
    if (AcquireProgressWrite(page_, &release_generation) !=
        SourceFrontierErrorV1::kNone) {
        return;
    }
    if (PageIdentityMatches(
            *page_, expected_writer_instance_, expected_generation_)) {
        const std::uint64_t callback_generation =
            AtomicLoad<std::uint64_t>(*page_, kCallbackGeneration);
        const std::uint64_t callback_inflight =
            AtomicLoad<std::uint64_t>(*page_, kCallbackInflight);
        if (callback_generation !=
            std::numeric_limits<std::uint64_t>::max()) {
            AtomicStore(
                page_, kCallbackGeneration, callback_generation + 1U,
                __ATOMIC_RELEASE);
        }
        if (callback_inflight != 0U) {
            AtomicStore(
                page_, kCallbackInflight, callback_inflight - 1U,
                __ATOMIC_RELEASE);
        }
    }
    ReleaseProgressWrite(page_, release_generation);
}

SourceFrontierErrorV1
SourceFrontierCallbackGuardV1::CompleteCaptured(
    std::uint64_t ingress_sequence) noexcept {
    if (!entered_ || completed_ || page_ == nullptr) {
        return SourceFrontierErrorV1::kInvalidState;
    }
    std::uint64_t release_generation = 0U;
    const SourceFrontierErrorV1 lock_error =
        AcquireProgressWriteWithRetry(
            page_, busy_timeout_, &release_generation);
    if (lock_error != SourceFrontierErrorV1::kNone) {
        error_ = lock_error;
        return error_;
    }
    const auto finish = [&](SourceFrontierErrorV1 error) noexcept {
        error_ = error;
        ReleaseProgressWrite(page_, release_generation);
        return error;
    };
    if (!PageIdentityMatches(
            *page_, expected_writer_instance_, expected_generation_)) {
        return finish(SourceFrontierErrorV1::kIdentityChanged);
    }
    const SourceStateV1 state = static_cast<SourceStateV1>(
        AtomicLoad<std::uint32_t>(*page_, kSourceState));
    if (!ValidState(state)) {
        LatchFatal(page_);
        return finish(SourceFrontierErrorV1::kInvalidPage);
    }
    if (FatalLatched(*page_) || state == SourceStateV1::kFatal) {
        return finish(SourceFrontierErrorV1::kInvalidState);
    }
    const std::uint64_t current = AtomicLoad<std::uint64_t>(
        *page_, kCapturedSequence);
    if (current == std::numeric_limits<std::uint64_t>::max() ||
        ingress_sequence != current + 1U) {
        return finish(SourceFrontierErrorV1::kIngressRegression);
    }
    const std::uint64_t callback_generation =
        AtomicLoad<std::uint64_t>(*page_, kCallbackGeneration);
    const std::uint64_t callback_inflight =
        AtomicLoad<std::uint64_t>(*page_, kCallbackInflight);
    if (callback_generation == std::numeric_limits<std::uint64_t>::max() ||
        callback_inflight == 0U) {
        LatchFatal(page_);
        if (callback_inflight != 0U) {
            AtomicStore(
                page_, kCallbackInflight, callback_inflight - 1U,
                __ATOMIC_RELEASE);
        }
        completed_ = true;
        return finish(SourceFrontierErrorV1::kCounterOverflow);
    }
    AtomicStore(
        page_, kCapturedSequence, ingress_sequence, __ATOMIC_RELEASE);
    AtomicStore(
        page_, kCallbackGeneration, callback_generation + 1U,
        __ATOMIC_RELEASE);
    AtomicStore(
        page_, kCallbackInflight, callback_inflight - 1U,
        __ATOMIC_RELEASE);
    completed_ = true;
    return finish(SourceFrontierErrorV1::kNone);
}

SourceFrontierErrorV1 PublishAppendProgressV1(
    SourceFrontierPageV1* page,
    const l2flow::common::Identity128& expected_writer_instance,
    std::uint64_t expected_generation,
    std::uint64_t ingress_sequence,
    std::uint64_t global_wal_pos,
    std::int64_t last_recv_monotonic_ns) noexcept {
    if (page == nullptr) {
        return SourceFrontierErrorV1::kNullArgument;
    }
    if (!HeaderValid(*page)) {
        return SourceFrontierErrorV1::kInvalidPage;
    }
    if (last_recv_monotonic_ns < 0) {
        return SourceFrontierErrorV1::kReceiveTimeRegression;
    }
    if (!IdentityArgumentsValid(
            expected_writer_instance, expected_generation)) {
        return SourceFrontierErrorV1::kIdentityChanged;
    }
    std::uint64_t release_generation = 0U;
    const SourceFrontierErrorV1 lock_error = AcquireProgressWrite(
        page, &release_generation);
    if (lock_error != SourceFrontierErrorV1::kNone) {
        return lock_error;
    }
    const auto finish = [&](SourceFrontierErrorV1 error) noexcept {
        ReleaseProgressWrite(page, release_generation);
        return error;
    };
    if (!PageIdentityMatches(
            *page, expected_writer_instance, expected_generation)) {
        return finish(SourceFrontierErrorV1::kIdentityChanged);
    }
    const SourceStateV1 state = static_cast<SourceStateV1>(
        AtomicLoad<std::uint32_t>(*page, kSourceState));
    if (!ValidState(state)) {
        LatchFatal(page);
        return finish(SourceFrontierErrorV1::kInvalidPage);
    }
    if (FatalLatched(*page) || state == SourceStateV1::kFatal) {
        return finish(SourceFrontierErrorV1::kInvalidState);
    }
    const std::uint64_t captured = AtomicLoad<std::uint64_t>(
        *page, kCapturedSequence, __ATOMIC_ACQUIRE);
    const std::uint64_t previous_sequence =
        AtomicLoad<std::uint64_t>(*page, kAppendSequence);
    const std::uint64_t previous_wal =
        AtomicLoad<std::uint64_t>(*page, kAppendWal);
    const std::int64_t previous_recv = static_cast<std::int64_t>(
        AtomicLoad<std::uint64_t>(*page, kLastRecv));
    const std::int64_t safe_frontier = static_cast<std::int64_t>(
        AtomicLoad<std::uint64_t>(*page, kSafeFrontier));
    if (ingress_sequence > captured) {
        return finish(SourceFrontierErrorV1::kNotCaptured);
    }
    if (ingress_sequence < previous_sequence) {
        return finish(SourceFrontierErrorV1::kIngressRegression);
    }
    if (global_wal_pos < previous_wal ||
        (ingress_sequence > previous_sequence &&
         global_wal_pos <= previous_wal)) {
        return finish(SourceFrontierErrorV1::kWalRegression);
    }
    if (last_recv_monotonic_ns < previous_recv ||
        (ingress_sequence == previous_sequence &&
         last_recv_monotonic_ns != previous_recv) ||
        (ingress_sequence > previous_sequence &&
         last_recv_monotonic_ns < safe_frontier)) {
        return finish(SourceFrontierErrorV1::kReceiveTimeRegression);
    }
    AtomicStore(
        page,
        kLastRecv,
        static_cast<std::uint64_t>(last_recv_monotonic_ns),
        __ATOMIC_RELAXED);
    AtomicStore(page, kAppendWal, global_wal_pos, __ATOMIC_RELAXED);
    AtomicStore(
        page, kAppendSequence, ingress_sequence, __ATOMIC_RELEASE);
    return finish(SourceFrontierErrorV1::kNone);
}

SourceFrontierErrorV1 PublishProcessedProgressV1(
    SourceFrontierPageV1* page,
    const l2flow::common::Identity128& expected_writer_instance,
    std::uint64_t expected_generation,
    std::uint64_t ingress_sequence,
    std::uint64_t global_wal_pos,
    std::int64_t last_processed_recv_monotonic_ns) noexcept {
    if (page == nullptr) {
        return SourceFrontierErrorV1::kNullArgument;
    }
    if (!HeaderValid(*page)) {
        return SourceFrontierErrorV1::kInvalidPage;
    }
    if (last_processed_recv_monotonic_ns < 0) {
        return SourceFrontierErrorV1::kReceiveTimeRegression;
    }
    if (!IdentityArgumentsValid(
            expected_writer_instance, expected_generation)) {
        return SourceFrontierErrorV1::kIdentityChanged;
    }
    std::uint64_t release_generation = 0U;
    const SourceFrontierErrorV1 lock_error = AcquireProgressWrite(
        page, &release_generation);
    if (lock_error != SourceFrontierErrorV1::kNone) {
        return lock_error;
    }
    const auto finish = [&](SourceFrontierErrorV1 error) noexcept {
        ReleaseProgressWrite(page, release_generation);
        return error;
    };
    if (LoadAtomicBytes<16U>(*page, kWriterInstance) !=
            expected_writer_instance ||
        AtomicLoad<std::uint64_t>(*page, kGeneration) !=
            expected_generation) {
        return finish(SourceFrontierErrorV1::kIdentityChanged);
    }
    const SourceStateV1 state = static_cast<SourceStateV1>(
        AtomicLoad<std::uint32_t>(*page, kSourceState));
    if (!ValidState(state)) {
        LatchFatal(page);
        return finish(SourceFrontierErrorV1::kInvalidPage);
    }
    if (FatalLatched(*page) || state == SourceStateV1::kFatal) {
        return finish(SourceFrontierErrorV1::kInvalidState);
    }
    const std::uint64_t append_sequence =
        AtomicLoad<std::uint64_t>(*page, kAppendSequence);
    const std::uint64_t append_wal =
        AtomicLoad<std::uint64_t>(*page, kAppendWal);
    const std::int64_t append_recv = static_cast<std::int64_t>(
        AtomicLoad<std::uint64_t>(*page, kLastRecv));
    const std::uint64_t previous_sequence =
        AtomicLoad<std::uint64_t>(*page, kProcessedSequence);
    const std::uint64_t previous_wal =
        AtomicLoad<std::uint64_t>(*page, kProcessedWal);
    const std::int64_t previous_safe = static_cast<std::int64_t>(
        AtomicLoad<std::uint64_t>(*page, kSafeFrontier));
    if (ingress_sequence > append_sequence ||
        global_wal_pos > append_wal ||
        (ingress_sequence < append_sequence &&
         global_wal_pos >= append_wal)) {
        return finish(SourceFrontierErrorV1::kNotAppended);
    }
    if (ingress_sequence < previous_sequence) {
        return finish(SourceFrontierErrorV1::kIngressRegression);
    }
    if (global_wal_pos < previous_wal ||
        (ingress_sequence > previous_sequence &&
         global_wal_pos <= previous_wal)) {
        return finish(SourceFrontierErrorV1::kWalRegression);
    }
    if (last_processed_recv_monotonic_ns > append_recv ||
        (ingress_sequence > previous_sequence &&
         last_processed_recv_monotonic_ns < previous_safe)) {
        return finish(SourceFrontierErrorV1::kReceiveTimeRegression);
    }
    if (ingress_sequence > previous_sequence) {
        AtomicMaxSafeFrontier(page, last_processed_recv_monotonic_ns);
    }
    AtomicStore(page, kProcessedWal, global_wal_pos, __ATOMIC_RELAXED);
    AtomicStore(
        page, kProcessedSequence, ingress_sequence, __ATOMIC_RELEASE);
    return finish(SourceFrontierErrorV1::kNone);
}

bool SourceFrontierCaughtUpV1(
    const SourceFrontierV1& value) noexcept {
    return value.callback_inflight == 0U &&
           value.captured_ingress_sequence ==
               value.append_ingress_sequence &&
           value.append_ingress_sequence ==
               value.processed_ingress_sequence &&
           value.append_global_wal_pos ==
               value.processed_global_wal_pos;
}

IdleFrontierResultV1 TryPublishIdleFrontierV1(
    SourceFrontierPageV1* page,
    const SourceFrontierV1& first,
    const SourceFrontierV1& second,
    std::int64_t sampled_monotonic_raw_ns) noexcept {
    if (page == nullptr || !HeaderValid(*page)) {
        return IdleFrontierResultV1::kInvalidPage;
    }
    if (first.source_state != SourceStateV1::kHealthy ||
        second.source_state != SourceStateV1::kHealthy) {
        return IdleFrontierResultV1::kSourceUnhealthy;
    }
    if (first.callback_inflight != 0U ||
        second.callback_inflight != 0U) {
        return IdleFrontierResultV1::kCallbackInflight;
    }
    if (!SourceFrontierCaughtUpV1(first) ||
        !SourceFrontierCaughtUpV1(second)) {
        return IdleFrontierResultV1::kNotCaughtUp;
    }
    if (!SameIdentity(first, second) ||
        !SameProgress(first, second)) {
        return IdleFrontierResultV1::kObservationChanged;
    }
    if (sampled_monotonic_raw_ns < 0 ||
        sampled_monotonic_raw_ns <
            second.last_appended_recv_monotonic_ns ||
        sampled_monotonic_raw_ns <
            second.safe_processed_frontier_ns) {
        return IdleFrontierResultV1::kClockRegression;
    }
    SourceFrontierV1 current;
    if (ReadSourceFrontierV1(*page, &current) !=
            SourceFrontierErrorV1::kNone ||
        !SameIdentity(second, current)) {
        return IdleFrontierResultV1::kInvalidPage;
    }
    if (current.source_state != SourceStateV1::kHealthy) {
        return IdleFrontierResultV1::kSourceUnhealthy;
    }
    if (current.safe_processed_frontier_ns >=
        sampled_monotonic_raw_ns) {
        return IdleFrontierResultV1::kNoAdvance;
    }
    std::uint64_t release_generation = 0U;
    if (AcquireProgressWrite(page, &release_generation) !=
        SourceFrontierErrorV1::kNone) {
        return IdleFrontierResultV1::kObservationChanged;
    }
    const auto finish = [&](IdleFrontierResultV1 result) noexcept {
        ReleaseProgressWrite(page, release_generation);
        return result;
    };
    if (!PageIdentityMatches(
            *page, current.writer_instance, current.generation)) {
        return finish(IdleFrontierResultV1::kObservationChanged);
    }
    if (FatalLatched(*page) ||
        static_cast<SourceStateV1>(AtomicLoad<std::uint32_t>(
            *page, kSourceState)) != SourceStateV1::kHealthy) {
        return finish(IdleFrontierResultV1::kSourceUnhealthy);
    }
    if (AtomicLoad<std::uint64_t>(*page, kCapturedSequence) !=
            current.captured_ingress_sequence ||
        AtomicLoad<std::uint64_t>(*page, kAppendSequence) !=
            current.append_ingress_sequence ||
        AtomicLoad<std::uint64_t>(*page, kProcessedSequence) !=
            current.processed_ingress_sequence ||
        AtomicLoad<std::uint64_t>(*page, kAppendWal) !=
            current.append_global_wal_pos ||
        AtomicLoad<std::uint64_t>(*page, kProcessedWal) !=
            current.processed_global_wal_pos ||
        static_cast<std::int64_t>(AtomicLoad<std::uint64_t>(
            *page, kLastRecv)) !=
            current.last_appended_recv_monotonic_ns ||
        static_cast<std::int64_t>(AtomicLoad<std::uint64_t>(
            *page, kSafeFrontier)) !=
            current.safe_processed_frontier_ns) {
        return finish(IdleFrontierResultV1::kObservationChanged);
    }
    AtomicMaxSafeFrontier(page, sampled_monotonic_raw_ns);
    return finish(IdleFrontierResultV1::kPublished);
}

}  // namespace l2flow::canonical
