#include "l2flow/ipc/realtime_certified_reader_v1.h"

#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <sys/mman.h>
#include <unistd.h>

namespace {

namespace ipc = l2flow::ipc;

template <typename Integer>
[[nodiscard]] std::atomic_ref<Integer> Atomic(
    Integer& value) noexcept {
    return std::atomic_ref<Integer>(value);
}

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

[[nodiscard]] ipc::RealtimeCertifiedTickEnvelopeV1 MakeEnvelope(
    std::uint64_t apply_sequence,
    std::uint32_t ordinal) noexcept {
    ipc::RealtimeCertifiedTickEnvelopeV1 envelope{};
    envelope.canonical_apply_sequence = apply_sequence;
    envelope.correction_epoch = 1U;
    envelope.feed_epoch = 1U;
    envelope.certified_monotonic_ns = 1'000U + apply_sequence;
    auto& payload = envelope.payload;
    payload.common.record_schema_version = 2U;
    payload.common.record_bytes = sizeof(payload);
    payload.common.instrument_id = ordinal + 1U;
    payload.common.ordinal = ordinal;
    payload.common.source_sequence = apply_sequence;
    payload.common.ingress_sequence = apply_sequence;
    payload.common.tick_stream_sequence = apply_sequence;
    payload.common.source_stream_id = 101U;
    payload.common.trade_date = 20260730U;
    payload.common.source_slot = 1U;
    payload.common.event_kind = 2U;
    payload.common.market = 1U;
    payload.channel = 1;
    payload.native_event_sequence =
        static_cast<std::int64_t>(100U + apply_sequence);
    return envelope;
}

void PublishSlot(
    ipc::RealtimeCertifiedTickSlotV1* slot,
    const ipc::RealtimeCertifiedTickEnvelopeV1& envelope,
    std::uint64_t stable_tag = 2U) noexcept {
    const auto words = std::bit_cast<
        std::array<
            std::uint64_t,
            ipc::kRealtimeCertifiedTickEnvelopeWordsV1>>(
        envelope);
    slot->publish_tag = stable_tag;
    for (std::size_t index = 0U; index < words.size(); ++index) {
        slot->payload_words[index] = words[index];
    }
}

class Fixture final {
public:
    Fixture() = default;
    Fixture(const Fixture&) = delete;
    Fixture& operator=(const Fixture&) = delete;

    ~Fixture() {
        if (read_only_fd_ >= 0) {
            static_cast<void>(::close(read_only_fd_));
        }
        if (mapping_ != MAP_FAILED) {
            static_cast<void>(::munmap(mapping_, mapping_bytes_));
        }
        if (writable_fd_ >= 0) {
            static_cast<void>(::close(writable_fd_));
        }
    }

    [[nodiscard]] bool Create() noexcept {
        constexpr std::uint32_t latest_capacity = 3U;
        constexpr std::uint32_t ring_capacity = 4U;
        constexpr std::uint32_t channel_capacity = 2U;
        mapping_bytes_ =
            ipc::kRealtimeCertifiedHeaderBytesV1 +
            latest_capacity *
                ipc::kRealtimeCertifiedTickSlotBytesV1 +
            ring_capacity *
                ipc::kRealtimeCertifiedTickSlotBytesV1 +
            channel_capacity *
                ipc::kRealtimeCertifiedChannelStateBytesV1;

        writable_fd_ = ::memfd_create(
            "l2flow-certified-reader-test",
            MFD_CLOEXEC | MFD_ALLOW_SEALING);
        if (writable_fd_ < 0 ||
            ::ftruncate(
                writable_fd_,
                static_cast<off_t>(mapping_bytes_)) != 0) {
            return false;
        }
        mapping_ = ::mmap(
            nullptr,
            mapping_bytes_,
            PROT_READ | PROT_WRITE,
            MAP_SHARED,
            writable_fd_,
            0);
        if (mapping_ == MAP_FAILED) {
            return false;
        }

        auto* const base = static_cast<std::byte*>(mapping_);
        header_ = std::construct_at(
            reinterpret_cast<ipc::RealtimeCertifiedHeaderV1*>(
                base));
        header_->magic = ipc::kRealtimeCertifiedShmMagicV1;
        header_->abi_major = ipc::kRealtimeCertifiedWireMajorV1;
        header_->abi_minor = ipc::kRealtimeCertifiedWireMinorV1;
        header_->header_bytes =
            ipc::kRealtimeCertifiedHeaderBytesV1;
        header_->endian_marker =
            ipc::kRealtimeCertifiedLittleEndianMarkerV1;
        header_->total_mapping_bytes = mapping_bytes_;
        header_->run_id[0U] = 0xA5U;
        header_->session_epoch = 7U;
        header_->trade_date = 20260730U;
        header_->latest_offset =
            ipc::kRealtimeCertifiedHeaderBytesV1;
        header_->certified_ring_offset =
            header_->latest_offset +
            latest_capacity *
                ipc::kRealtimeCertifiedTickSlotBytesV1;
        header_->channel_state_offset =
            header_->certified_ring_offset +
            ring_capacity *
                ipc::kRealtimeCertifiedTickSlotBytesV1;
        header_->latest_capacity = latest_capacity;
        header_->certified_ring_capacity = ring_capacity;
        header_->channel_state_capacity = channel_capacity;
        header_->status_publish_tag = 2U;
        header_->heartbeat_monotonic_ns = 10'000U;
        header_->canonical_apply_frontier = 5U;
        header_->correction_epoch = 1U;
        header_->observed_native_message_count = 5U;
        header_->certified_tick_count = 5U;
        header_->aggregate_state = static_cast<std::uint32_t>(
            ipc::RealtimeCertifiedStateV1::kContiguous);
        header_->channel_state_count = 1U;

        latest_ = reinterpret_cast<
            ipc::RealtimeCertifiedTickSlotV1*>(
                base + header_->latest_offset);
        ring_ = reinterpret_cast<
            ipc::RealtimeCertifiedTickSlotV1*>(
                base + header_->certified_ring_offset);
        channels_ = reinterpret_cast<
            ipc::RealtimeCertifiedChannelStateV1*>(
                base + header_->channel_state_offset);
        for (std::uint32_t index = 0U;
             index < latest_capacity;
             ++index) {
            std::construct_at(&latest_[index]);
        }
        for (std::uint32_t index = 0U;
             index < ring_capacity;
             ++index) {
            std::construct_at(&ring_[index]);
        }
        for (std::uint32_t index = 0U;
             index < channel_capacity;
             ++index) {
            std::construct_at(&channels_[index]);
        }

        for (std::uint64_t sequence = 2U; sequence <= 5U;
             ++sequence) {
            std::uint32_t index = 0U;
            if (!ipc::RealtimeCertifiedRingSlotIndexV1(
                    sequence, ring_capacity, &index)) {
                return false;
            }
            PublishSlot(
                &ring_[index],
                MakeEnvelope(
                    sequence,
                    sequence == 4U ? 1U : 0U));
        }
        PublishSlot(&latest_[0U], MakeEnvelope(5U, 0U));
        PublishSlot(&latest_[1U], MakeEnvelope(4U, 1U));

        auto& channel = channels_[0U];
        channel.publish_tag = 2U;
        channel.feed_epoch = 1U;
        channel.origin_sequence = 101;
        channel.observed_contiguous_frontier = 105;
        channel.certified_published_frontier = 105;
        channel.highest_observed_sequence = 105;
        channel.canonical_apply_frontier = 5U;
        channel.observed_native_message_count = 5U;
        channel.certified_tick_count = 5U;
        channel.state = static_cast<std::uint32_t>(
            ipc::RealtimeCertifiedStateV1::kContiguous);
        channel.trade_date = 20260730U;
        channel.channel = 1;
        channel.market = 1U;

        if (!ipc::RealtimeCertifiedHeaderCanonicalV1(*header_)) {
            return false;
        }

        const std::string descriptor_path =
            "/proc/self/fd/" + std::to_string(writable_fd_);
        read_only_fd_ =
            ::open(descriptor_path.c_str(), O_RDONLY | O_CLOEXEC);
        if (read_only_fd_ < 0) {
            return false;
        }
        constexpr int seals =
            F_SEAL_GROW | F_SEAL_SHRINK |
            F_SEAL_FUTURE_WRITE | F_SEAL_SEAL;
        return ::fcntl(writable_fd_, F_ADD_SEALS, seals) == 0;
    }

    [[nodiscard]] ipc::RealtimeCertifiedExpectedSessionV1
    ExpectedSession() const noexcept {
        ipc::RealtimeCertifiedExpectedSessionV1 expected{};
        expected.run_id = header_->run_id;
        expected.session_epoch = header_->session_epoch;
        expected.trade_date = header_->trade_date;
        return expected;
    }

    void SetGapOpen() noexcept {
        BeginStatusWrite();
        header_->aggregate_state = static_cast<std::uint32_t>(
            ipc::RealtimeCertifiedStateV1::kGapOpen);
        header_->gap_opened_count = 1U;
        header_->pending_token_count = 1U;
        header_->gap_open_channel_count = 1U;
        EndStatusWrite();
    }

    void SetFrozenConflict() noexcept {
        BeginStatusWrite();
        header_->aggregate_state = static_cast<std::uint32_t>(
            ipc::RealtimeCertifiedStateV1::kFrozenConflict);
        header_->conflicting_duplicate_count = 1U;
        header_->gap_open_channel_count = 0U;
        EndStatusWrite();
    }

    void CorruptRingTail(std::uint64_t sequence) noexcept {
        std::uint32_t index = 0U;
        if (!ipc::RealtimeCertifiedRingSlotIndexV1(
                sequence,
                header_->certified_ring_capacity,
                &index)) {
            return;
        }
        auto& slot = ring_[index];
        const std::uint64_t stable = slot.publish_tag;
        Atomic(slot.publish_tag)
            .store(stable + 1U, std::memory_order_release);
        Atomic(
            slot.payload_words[
                ipc::kRealtimeCertifiedTickEnvelopeWordsV1])
            .store(1U, std::memory_order_relaxed);
        Atomic(slot.publish_tag)
            .store(stable + 2U, std::memory_order_release);
    }

    void StageChannelAtNextAggregateEpoch() noexcept {
        auto& channel = channels_[0U];
        Atomic(channel.publish_tag)
            .store(3U, std::memory_order_release);
        Atomic(channel.observed_contiguous_frontier)
            .store(106, std::memory_order_relaxed);
        Atomic(channel.certified_published_frontier)
            .store(106, std::memory_order_relaxed);
        Atomic(channel.highest_observed_sequence)
            .store(106, std::memory_order_relaxed);
        Atomic(channel.observed_native_message_count)
            .store(6U, std::memory_order_relaxed);
        Atomic(channel.publish_tag)
            .store(4U, std::memory_order_release);
    }

    void CommitAggregateEpoch(std::uint64_t stable_tag) noexcept {
        const std::uint64_t current =
            Atomic(header_->status_publish_tag)
                .load(std::memory_order_relaxed);
        Atomic(header_->status_publish_tag)
            .store(current + 1U, std::memory_order_release);
        Atomic(header_->observed_native_message_count)
            .store(6U, std::memory_order_relaxed);
        Atomic(header_->status_publish_tag)
            .store(stable_tag, std::memory_order_release);
    }

    [[nodiscard]] int read_only_fd() const noexcept {
        return read_only_fd_;
    }

    [[nodiscard]] int writable_fd() const noexcept {
        return writable_fd_;
    }

    [[nodiscard]] ipc::RealtimeCertifiedHeaderV1* header()
        noexcept {
        return header_;
    }

private:
    void BeginStatusWrite() noexcept {
        const std::uint64_t stable =
            Atomic(header_->status_publish_tag)
                .load(std::memory_order_relaxed);
        Atomic(header_->status_publish_tag)
            .store(stable + 1U, std::memory_order_release);
    }

    void EndStatusWrite() noexcept {
        const std::uint64_t odd =
            Atomic(header_->status_publish_tag)
                .load(std::memory_order_relaxed);
        Atomic(header_->status_publish_tag)
            .store(odd + 1U, std::memory_order_release);
    }

    int writable_fd_ = -1;
    int read_only_fd_ = -1;
    void* mapping_ = MAP_FAILED;
    std::size_t mapping_bytes_ = 0U;
    ipc::RealtimeCertifiedHeaderV1* header_ = nullptr;
    ipc::RealtimeCertifiedTickSlotV1* latest_ = nullptr;
    ipc::RealtimeCertifiedTickSlotV1* ring_ = nullptr;
    ipc::RealtimeCertifiedChannelStateV1* channels_ = nullptr;
};

bool TestOpenAndReads() {
    Fixture fixture;
    bool ok = true;
    ok &= Expect(fixture.Create(), "create sealed memfd fixture");
    if (!ok) {
        return false;
    }

    std::unique_ptr<ipc::RealtimeCertifiedReaderV1> reader;
    int system_error = -1;
    const auto open_result =
        ipc::RealtimeCertifiedReaderV1::OpenDescriptorForTest(
            fixture.read_only_fd(),
            fixture.ExpectedSession(),
            &reader,
            &system_error);
    ok &= Expect(
        open_result ==
            ipc::RealtimeCertifiedReaderOpenErrorV1::kNone &&
            reader != nullptr && system_error == 0,
        "open production-equivalent read-only descriptor");
    if (!reader) {
        return false;
    }
    ok &= Expect(
        reader->latest_capacity() == 3U &&
            reader->certified_ring_capacity() == 4U &&
            reader->channel_state_capacity() == 2U,
        "reader exposes frozen region capacities");
    ok &= Expect(
        reader->session().run_id ==
                fixture.ExpectedSession().run_id &&
            reader->session().session_epoch == 7U &&
            reader->session().trade_date == 20260730U,
        "reader pins the expected FAST session identity");

    ipc::RealtimeCertifiedStatusSnapshotV1 status{};
    ok &= Expect(
        reader->ReadStatus(&status) ==
                ipc::RealtimeCertifiedReadResultV1::kOk &&
            status.state ==
                ipc::RealtimeCertifiedStateV1::kContiguous &&
            status.canonical_apply_frontier == 5U,
        "read coherent aggregate status");

    ipc::RealtimeCertifiedTickEnvelopeV1 tick{};
    ok &= Expect(
        reader->ReadLatest(0U, &tick) ==
                ipc::RealtimeCertifiedReadResultV1::kOk &&
            tick.canonical_apply_sequence == 5U &&
            tick.payload.common.ordinal == 0U,
        "read latest certified tick by ordinal");
    ok &= Expect(
        reader->ReadLatest(1U, &tick) ==
                ipc::RealtimeCertifiedReadResultV1::kOk &&
            tick.canonical_apply_sequence == 4U &&
            tick.payload.common.ordinal == 1U,
        "latest slots retain independent instruments");

    tick.canonical_apply_sequence = 777U;
    ok &= Expect(
        reader->ReadLatest(2U, &tick) ==
                ipc::RealtimeCertifiedReadResultV1::kNoData &&
            tick.canonical_apply_sequence == 777U,
        "never-published latest leaves output untouched");
    ok &= Expect(
        reader->ReadLatest(3U, &tick) ==
            ipc::RealtimeCertifiedReadResultV1::kOutOfRange,
        "latest ordinal is capacity bounded");

    ok &= Expect(
        reader->ReadCanonical(2U, &tick) ==
                ipc::RealtimeCertifiedReadResultV1::kOk &&
            tick.canonical_apply_sequence == 2U,
        "read oldest retained dense sequence");
    ok &= Expect(
        reader->ReadCanonical(5U, &tick) ==
                ipc::RealtimeCertifiedReadResultV1::kOk &&
            tick.canonical_apply_sequence == 5U,
        "read current dense frontier");
    tick.canonical_apply_sequence = 777U;
    ok &= Expect(
        reader->ReadCanonical(1U, &tick) ==
                ipc::RealtimeCertifiedReadResultV1::kOverwritten &&
            tick.canonical_apply_sequence == 777U,
        "overwritten sequence is distinct and leaves output untouched");
    ok &= Expect(
        reader->ReadCanonical(6U, &tick) ==
                ipc::RealtimeCertifiedReadResultV1::
                    kNotYetPublished &&
            tick.canonical_apply_sequence == 777U,
        "future sequence is distinct from overwrite");
    ok &= Expect(
        reader->ReadCanonical(0U, &tick) ==
            ipc::RealtimeCertifiedReadResultV1::kOutOfRange,
        "canonical sequence zero is reserved");

    ipc::RealtimeCertifiedChannelStateV1 channel{};
    ok &= Expect(
        reader->ReadChannelState(0U, &channel) ==
                ipc::RealtimeCertifiedReadResultV1::kOk &&
            channel.observed_contiguous_frontier == 105 &&
            channel.certified_published_frontier == 105,
        "read coherent per-channel native frontiers");
    ok &= Expect(
        reader->ReadChannelState(1U, &channel) ==
            ipc::RealtimeCertifiedReadResultV1::kNoData,
        "empty channel table row is normal NO_DATA");
    return ok;
}

bool TestGapAndFrozenRemainReadable() {
    Fixture fixture;
    bool ok = true;
    ok &= Expect(fixture.Create(), "create gap/frozen fixture");
    if (!ok) {
        return false;
    }

    std::unique_ptr<ipc::RealtimeCertifiedReaderV1> reader;
    ok &= Expect(
        ipc::RealtimeCertifiedReaderV1::OpenDescriptorForTest(
            fixture.read_only_fd(),
            fixture.ExpectedSession(),
            &reader) ==
            ipc::RealtimeCertifiedReaderOpenErrorV1::kNone,
        "open gap/frozen reader");
    if (!reader) {
        return false;
    }

    fixture.SetGapOpen();
    ipc::RealtimeCertifiedStatusSnapshotV1 status{};
    ipc::RealtimeCertifiedTickEnvelopeV1 tick{};
    ok &= Expect(
        reader->ReadStatus(&status) ==
                ipc::RealtimeCertifiedReadResultV1::kOk &&
            status.state ==
                ipc::RealtimeCertifiedStateV1::kGapOpen,
        "GAP_OPEN is a readable status, not reader failure");
    ok &= Expect(
        reader->ReadLatest(0U, &tick) ==
                ipc::RealtimeCertifiedReadResultV1::kOk &&
            tick.canonical_apply_sequence == 5U,
        "gap keeps last certified latest readable");
    ok &= Expect(
        reader->ReadCanonical(5U, &tick) ==
                ipc::RealtimeCertifiedReadResultV1::kOk &&
            tick.canonical_apply_sequence == 5U,
        "gap keeps last certified dense prefix readable");

    fixture.SetFrozenConflict();
    ok &= Expect(
        reader->ReadStatus(&status) ==
                ipc::RealtimeCertifiedReadResultV1::kOk &&
            status.state ==
                ipc::RealtimeCertifiedStateV1::kFrozenConflict,
        "FROZEN_CONFLICT is a readable status");
    ok &= Expect(
        reader->ReadLatest(0U, &tick) ==
                ipc::RealtimeCertifiedReadResultV1::kOk &&
            tick.canonical_apply_sequence == 5U,
        "frozen state keeps last correct latest readable");
    return ok;
}

bool TestChannelAggregateCommitGate() {
    Fixture fixture;
    bool ok = true;
    ok &= Expect(
        fixture.Create(), "create channel aggregate-gate fixture");
    if (!ok) {
        return false;
    }

    std::unique_ptr<ipc::RealtimeCertifiedReaderV1> reader;
    ok &= Expect(
        ipc::RealtimeCertifiedReaderV1::OpenDescriptorForTest(
            fixture.read_only_fd(),
            fixture.ExpectedSession(),
            &reader) ==
                ipc::RealtimeCertifiedReaderOpenErrorV1::kNone &&
            reader != nullptr,
        "open channel aggregate-gate reader");
    if (!reader) {
        return false;
    }

    fixture.StageChannelAtNextAggregateEpoch();
    ipc::RealtimeCertifiedChannelStateV1 channel{};
    channel.observed_contiguous_frontier = 777;
    ok &= Expect(
        reader->ReadChannelState(0U, &channel) ==
                ipc::RealtimeCertifiedReadResultV1::kInconsistent &&
            channel.observed_contiguous_frontier == 777,
        "future channel row stays hidden before aggregate commit");

    fixture.CommitAggregateEpoch(4U);
    ok &= Expect(
        reader->ReadChannelState(0U, &channel) ==
                ipc::RealtimeCertifiedReadResultV1::kOk &&
            channel.publish_tag == 4U &&
            channel.observed_contiguous_frontier == 106,
        "matching aggregate commit exposes prepared channel row");

    fixture.CommitAggregateEpoch(6U);
    ok &= Expect(
        reader->ReadChannelState(0U, &channel) ==
                ipc::RealtimeCertifiedReadResultV1::kOk &&
            channel.publish_tag == 4U &&
            channel.observed_contiguous_frontier == 106,
        "unchanged older channel row remains visible");
    return ok;
}

bool TestStrictOpenAndCorruption() {
    bool ok = true;

    Fixture fixture;
    ok &= Expect(fixture.Create(), "create strict-open fixture");
    if (!ok) {
        return false;
    }
    auto mismatched = fixture.ExpectedSession();
    ++mismatched.session_epoch;
    std::unique_ptr<ipc::RealtimeCertifiedReaderV1> reader;
    ok &= Expect(
        ipc::RealtimeCertifiedReaderV1::OpenDescriptorForTest(
            fixture.read_only_fd(), mismatched, &reader) ==
                ipc::RealtimeCertifiedReaderOpenErrorV1::
                    kSessionMismatch &&
            reader == nullptr,
        "run/session/date mismatch is rejected");
    ok &= Expect(
        ipc::RealtimeCertifiedReaderV1::OpenDescriptorForTest(
            fixture.writable_fd(),
            fixture.ExpectedSession(),
            &reader) ==
                ipc::RealtimeCertifiedReaderOpenErrorV1::
                    kDescriptorInvalid &&
            reader == nullptr,
        "writable descriptor is rejected");
    ok &= Expect(
        ipc::RealtimeCertifiedReaderV1::OpenDescriptorForTest(
            fixture.read_only_fd(),
            fixture.ExpectedSession(),
            &reader) ==
                ipc::RealtimeCertifiedReaderOpenErrorV1::kNone &&
            reader != nullptr,
        "valid reader opens before slot corruption");
    if (!reader) {
        return false;
    }

    fixture.CorruptRingTail(2U);
    ipc::RealtimeCertifiedTickEnvelopeV1 tick{};
    ok &= Expect(
        reader->ReadCanonical(2U, &tick) ==
            ipc::RealtimeCertifiedReadResultV1::kCorrupt,
        "nonzero reserved ring tail is detected");

    Fixture bad_header;
    ok &= Expect(bad_header.Create(), "create bad-header fixture");
    if (!ok) {
        return false;
    }
    bad_header.header()->reserved[0U] = 1U;
    std::unique_ptr<ipc::RealtimeCertifiedReaderV1> invalid;
    ok &= Expect(
        ipc::RealtimeCertifiedReaderV1::OpenDescriptorForTest(
            bad_header.read_only_fd(),
            bad_header.ExpectedSession(),
            &invalid) ==
                ipc::RealtimeCertifiedReaderOpenErrorV1::
                    kLayoutInvalid &&
            invalid == nullptr,
        "nonzero header reserved byte is rejected");
    return ok;
}

}  // namespace

int main() {
    bool ok = true;
    ok &= TestOpenAndReads();
    ok &= TestGapAndFrozenRemainReadable();
    ok &= TestChannelAggregateCommitGate();
    ok &= TestStrictOpenAndCorruption();
    if (ok) {
        std::cout << "PASS: realtime certified reader v1\n";
        return 0;
    }
    return 1;
}
