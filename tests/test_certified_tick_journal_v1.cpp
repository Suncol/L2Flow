#include "l2flow/ipc/certified_tick_journal_v1.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

namespace {

namespace ipc = l2flow::ipc;

constexpr std::uint32_t kTradeDate = 20260730U;

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

class Descriptor final {
public:
    Descriptor() noexcept = default;
    Descriptor(const Descriptor&) = delete;
    Descriptor& operator=(const Descriptor&) = delete;
    ~Descriptor() {
        if (value_ >= 0) {
            static_cast<void>(::close(value_));
        }
    }

    [[nodiscard]] int get() const noexcept { return value_; }

    [[nodiscard]] int* output() noexcept {
        if (value_ >= 0) {
            static_cast<void>(::close(value_));
        }
        value_ = -1;
        return &value_;
    }

private:
    int value_ = -1;
};

[[nodiscard]] ipc::CertifiedTickJournalConfigV1 JournalConfig(
    std::uint64_t capacity,
    std::uint64_t commit_chunk = 4096U) noexcept {
    ipc::CertifiedTickJournalConfigV1 result{};
    result.run_id[0U] = std::byte{0xA5};
    result.session_epoch = 7U;
    result.trade_date = kTradeDate;
    result.tick_capacity = capacity;
    static_cast<void>(
        ipc::certified_tick_journal_v1_detail::CheckedLayoutBytes(
            capacity, &result.maximum_mapping_bytes));
    result.lazy_commit_chunk_bytes = commit_chunk;
    return result;
}

[[nodiscard]] ipc::RealtimeCertifiedTickEnvelopeV1 Tick(
    std::uint64_t sequence) noexcept {
    ipc::RealtimeCertifiedTickEnvelopeV1 result{};
    result.canonical_apply_sequence = sequence;
    result.correction_epoch = 1U;
    result.feed_epoch = 1U;
    result.certified_monotonic_ns = 1'000U + sequence;
    auto& common = result.payload.common;
    common.record_bytes = sizeof(ipc::RealtimeWireTickPayloadV2);
    common.instrument_id = 1U;
    common.ordinal = 0U;
    common.source_sequence = sequence;
    common.ingress_sequence = sequence;
    common.tick_stream_sequence = sequence;
    common.source_stream_id = 101U;
    common.trade_date = kTradeDate;
    common.source_slot = 1U;
    common.event_kind = 2U;
    common.market = 1U;
    result.payload.channel = 1;
    result.payload.native_event_sequence =
        static_cast<std::int64_t>(sequence);
    return result;
}

[[nodiscard]] bool CreateJournal(
    std::uint64_t capacity,
    std::shared_ptr<ipc::CertifiedTickJournalProducerV1>* output,
    std::uint64_t commit_chunk = 4096U) {
    int system_error = 0;
    return ipc::CertifiedTickJournalProducerV1::Create(
               JournalConfig(capacity, commit_chunk),
               output,
               &system_error) ==
               ipc::CertifiedTickJournalCreateErrorV1::kNone &&
           *output != nullptr && system_error == 0;
}

[[nodiscard]] bool OpenReader(
    const std::shared_ptr<ipc::CertifiedTickJournalProducerV1>&
        producer,
    std::unique_ptr<ipc::CertifiedTickJournalReaderV1>* output) {
    Descriptor descriptor;
    int system_error = 0;
    if (producer == nullptr ||
        !producer->DuplicateReadOnlyDescriptor(
            descriptor.output(), &system_error) ||
        system_error != 0) {
        return false;
    }
    return ipc::CertifiedTickJournalReaderV1::Open(
               descriptor.get(),
               producer->session(),
               output,
               &system_error) ==
               ipc::CertifiedTickJournalOpenErrorV1::kNone &&
           *output != nullptr && system_error == 0;
}

bool TestHistoryToTailAndCompleteLifecycle() {
    bool ok = true;
    std::shared_ptr<ipc::CertifiedTickJournalProducerV1> producer;
    ok &= Expect(
        CreateJournal(8U, &producer),
        "create bounded sparse Tick journal");
    if (producer == nullptr) {
        return false;
    }
    ok &= Expect(
        producer->committed_mapping_bytes() ==
                ipc::kCertifiedTickJournalHeaderBytesV1 &&
            producer->session().total_mapping_bytes == 8192U,
        "creation reserves full VAS but commits only the header");

    std::unique_ptr<ipc::CertifiedTickJournalReaderV1> reader;
    ok &= Expect(
        OpenReader(producer, &reader),
        "open a descriptor-bound read-only journal mapping");
    if (reader == nullptr) {
        return false;
    }

    ipc::CertifiedTickJournalStatusV1 status{};
    ipc::RealtimeCertifiedTickEnvelopeV1 row{};
    ok &= Expect(
        reader->ReadStatus(&status) ==
                ipc::CertifiedTickJournalReadResultV1::kOk &&
            status.state ==
                ipc::CertifiedTickJournalStateV1::kActive &&
            status.canonical_apply_frontier == 0U &&
            status.generation == 0U &&
            status.published_tick_count == 0U,
        "new reader observes the coherent empty ACTIVE cut");
    ok &= Expect(
        reader->ReadOne(1U, &row) ==
            ipc::CertifiedTickJournalReadResultV1::
                kNotYetPublished,
        "tail read distinguishes temporarily unavailable data");
    ok &= Expect(
        reader->ReadOne(0U, &row) ==
                ipc::CertifiedTickJournalReadResultV1::
                    kInvalidArgument &&
            reader->ReadOne(9U, &row) ==
                ipc::CertifiedTickJournalReadResultV1::kOutOfRange,
        "reader rejects sequence zero and addresses beyond capacity");

    const std::array<ipc::RealtimeCertifiedTickEnvelopeV1, 3U>
        prefix{Tick(1U), Tick(2U), Tick(3U)};
    ok &= Expect(
        producer->Append(prefix) ==
            ipc::CertifiedTickJournalAppendErrorV1::kNone,
        "append a dense three-Tick prefix as one publication cut");
    ok &= Expect(
        reader->ReadStatus(&status) ==
                ipc::CertifiedTickJournalReadResultV1::kOk &&
            status.state ==
                ipc::CertifiedTickJournalStateV1::kActive &&
            status.canonical_apply_frontier == 3U &&
            status.generation == 3U &&
            status.published_tick_count == 3U &&
            status.committed_mapping_bytes == 8192U,
        "frontier and lazy backing advance in one coherent status cut");

    std::array<ipc::RealtimeCertifiedTickEnvelopeV1, 4U> rows{};
    ipc::CertifiedTickJournalReadBatchV1 batch{};
    ok &= Expect(
        reader->Read(2U, rows, &batch) ==
                ipc::CertifiedTickJournalReadResultV1::kOk &&
            batch.written == 2U &&
            batch.next_canonical_apply_sequence == 4U &&
            rows[0U].canonical_apply_sequence == 2U &&
            rows[1U].canonical_apply_sequence == 3U,
        "batch reader starts at an arbitrary historical canonical sequence");
    ok &= Expect(
        reader->ReadOne(4U, &row) ==
            ipc::CertifiedTickJournalReadResultV1::
                kNotYetPublished,
        "historical reader transitions to the live tail without ambiguity");

    const auto fourth = Tick(4U);
    ok &= Expect(
        producer->Append(fourth) ==
                ipc::CertifiedTickJournalAppendErrorV1::kNone &&
            reader->ReadOne(4U, &row) ==
                ipc::CertifiedTickJournalReadResultV1::kOk &&
            row.canonical_apply_sequence == 4U &&
            row.payload.native_event_sequence == 4,
        "tail row becomes readable after its frontier publication");
    ok &= Expect(
        producer->Stop() && producer->Stop(),
        "COMPLETE lifecycle transition is monotonic and idempotent");
    ok &= Expect(
        reader->ReadStatus(&status) ==
                ipc::CertifiedTickJournalReadResultV1::kOk &&
            status.state ==
                ipc::CertifiedTickJournalStateV1::kComplete &&
            status.canonical_apply_frontier == 4U &&
            reader->ReadOne(5U, &row) ==
                ipc::CertifiedTickJournalReadResultV1::kEndOfStream,
        "reader distinguishes terminal end-of-stream from live tail lag");
    ok &= Expect(
        producer->Append(Tick(5U)) ==
            ipc::CertifiedTickJournalAppendErrorV1::kStopped,
        "completed journal rejects further writes without changing prefix");

    producer.reset();
    row = {};
    ok &= Expect(
        reader->ReadOne(1U, &row) ==
                ipc::CertifiedTickJournalReadResultV1::kOk &&
            row.canonical_apply_sequence == 1U,
        "read-only mapping retains history after producer destruction");
    return ok;
}

bool TestFailurePreservesPublishedPrefix() {
    bool ok = true;
    std::shared_ptr<ipc::CertifiedTickJournalProducerV1> producer;
    ok &= Expect(
        CreateJournal(4U, &producer),
        "create journal for fail-closed lifecycle test");
    if (producer == nullptr) {
        return false;
    }
    std::unique_ptr<ipc::CertifiedTickJournalReaderV1> reader;
    ok &= Expect(
        OpenReader(producer, &reader),
        "open reader before producer failure");
    if (reader == nullptr) {
        return false;
    }

    ok &= Expect(
        producer->Append(Tick(1U)) ==
                ipc::CertifiedTickJournalAppendErrorV1::kNone &&
            producer->Append(Tick(3U)) ==
                ipc::CertifiedTickJournalAppendErrorV1::
                    kCanonicalSequence &&
            producer->failed() &&
            producer->last_error() ==
                ipc::CertifiedTickJournalAppendErrorV1::
                    kCanonicalSequence,
        "non-dense append fail-closes the writer with an exact cause");

    ipc::CertifiedTickJournalStatusV1 status{};
    ipc::RealtimeCertifiedTickEnvelopeV1 row{};
    ok &= Expect(
        reader->ReadStatus(&status) ==
                ipc::CertifiedTickJournalReadResultV1::kOk &&
            status.state ==
                ipc::CertifiedTickJournalStateV1::kFailed &&
            status.failure ==
                ipc::CertifiedTickJournalAppendErrorV1::
                    kCanonicalSequence &&
            status.canonical_apply_frontier == 1U,
        "FAILED status preserves the last successfully published frontier");
    ok &= Expect(
        reader->ReadOne(1U, &row) ==
                ipc::CertifiedTickJournalReadResultV1::kOk &&
            row.canonical_apply_sequence == 1U &&
            reader->ReadOne(2U, &row) ==
                ipc::CertifiedTickJournalReadResultV1::kProducerFailed,
        "published prefix remains readable while its missing suffix is terminal");
    ok &= Expect(
        producer->Append(Tick(2U)) ==
                ipc::CertifiedTickJournalAppendErrorV1::kFailed &&
            !producer->Stop(),
        "failed journal cannot resume or claim successful completion");
    return ok;
}

bool TestFullCapacityNaturalTailPreservesLifecycle() {
    bool ok = true;
    {
        std::shared_ptr<ipc::CertifiedTickJournalProducerV1> producer;
        ok &= Expect(
            CreateJournal(2U, &producer),
            "create full-capacity COMPLETE boundary journal");
        if (producer == nullptr) {
            return false;
        }
        std::unique_ptr<ipc::CertifiedTickJournalReaderV1> reader;
        ok &= Expect(
            OpenReader(producer, &reader),
            "open COMPLETE boundary reader");
        if (reader == nullptr) {
            return false;
        }

        const std::array<ipc::RealtimeCertifiedTickEnvelopeV1, 2U>
            prefix{Tick(1U), Tick(2U)};
        std::array<ipc::RealtimeCertifiedTickEnvelopeV1, 1U> rows{};
        ipc::CertifiedTickJournalReadBatchV1 batch{};
        ok &= Expect(
            producer->Append(prefix) ==
                    ipc::CertifiedTickJournalAppendErrorV1::kNone &&
                reader->Read(3U, rows, &batch) ==
                    ipc::CertifiedTickJournalReadResultV1::
                        kNotYetPublished &&
                batch.written == 0U &&
                batch.next_canonical_apply_sequence == 3U &&
                batch.status.state ==
                    ipc::CertifiedTickJournalStateV1::kActive &&
                batch.status.canonical_apply_frontier == 2U,
            "natural full-capacity tail remains waitable while ACTIVE");
        ok &= Expect(
            producer->Stop(),
            "complete an exactly full Tick journal");

        batch = {};
        const bool envelope_complete =
            reader->Read(3U, rows, &batch) ==
                ipc::CertifiedTickJournalReadResultV1::kEndOfStream &&
            batch.written == 0U &&
            batch.next_canonical_apply_sequence == 3U &&
            batch.status.state ==
                ipc::CertifiedTickJournalStateV1::kComplete &&
            batch.status.canonical_apply_frontier == 2U;
        std::array<ipc::RealtimeCertifiedTickSlotV1, 1U> slots{};
        batch = {};
        const bool slot_complete =
            reader->ReadSlots(3U, slots, &batch) ==
                ipc::CertifiedTickJournalReadResultV1::kEndOfStream &&
            batch.written == 0U &&
            batch.next_canonical_apply_sequence == 3U &&
            batch.status.state ==
                ipc::CertifiedTickJournalStateV1::kComplete &&
            batch.status.canonical_apply_frontier == 2U;
        std::array<
            std::byte,
            ipc::kCertifiedTickJournalSlotBytesV1>
            slot_bytes{};
        batch = {};
        const bool bytes_complete =
            reader->ReadSlotBytes(3U, slot_bytes, &batch) ==
                ipc::CertifiedTickJournalReadResultV1::kEndOfStream &&
            batch.written == 0U &&
            batch.next_canonical_apply_sequence == 3U &&
            batch.status.state ==
                ipc::CertifiedTickJournalStateV1::kComplete &&
            batch.status.canonical_apply_frontier == 2U;
        ok &= Expect(
            envelope_complete && slot_complete && bytes_complete,
            "Read, ReadSlots, and FFI bytes expose COMPLETE at capacity+1");

        batch = {};
        ok &= Expect(
            reader->Read(4U, rows, &batch) ==
                    ipc::CertifiedTickJournalReadResultV1::kOutOfRange &&
                batch.written == 0U &&
                batch.next_canonical_apply_sequence == 4U,
            "COMPLETE exception is limited to the exact natural tail");
    }

    {
        std::shared_ptr<ipc::CertifiedTickJournalProducerV1> producer;
        ok &= Expect(
            CreateJournal(2U, &producer),
            "create full-capacity FAILED boundary journal");
        if (producer == nullptr) {
            return false;
        }
        std::unique_ptr<ipc::CertifiedTickJournalReaderV1> reader;
        ok &= Expect(
            OpenReader(producer, &reader),
            "open FAILED boundary reader");
        if (reader == nullptr) {
            return false;
        }

        const std::array<ipc::RealtimeCertifiedTickEnvelopeV1, 2U>
            prefix{Tick(1U), Tick(2U)};
        ok &= Expect(
            producer->Append(prefix) ==
                    ipc::CertifiedTickJournalAppendErrorV1::kNone &&
                producer->Append(Tick(3U)) ==
                    ipc::CertifiedTickJournalAppendErrorV1::
                        kTickCapacity &&
                producer->failed(),
            "append beyond a full journal publishes FAILED capacity state");

        std::array<ipc::RealtimeCertifiedTickEnvelopeV1, 1U> rows{};
        ipc::CertifiedTickJournalReadBatchV1 batch{};
        const bool envelope_failed =
            reader->Read(3U, rows, &batch) ==
                ipc::CertifiedTickJournalReadResultV1::kProducerFailed &&
            batch.written == 0U &&
            batch.next_canonical_apply_sequence == 3U &&
            batch.status.state ==
                ipc::CertifiedTickJournalStateV1::kFailed &&
            batch.status.failure ==
                ipc::CertifiedTickJournalAppendErrorV1::kTickCapacity &&
            batch.status.canonical_apply_frontier == 2U;
        std::array<ipc::RealtimeCertifiedTickSlotV1, 1U> slots{};
        batch = {};
        const bool slot_failed =
            reader->ReadSlots(3U, slots, &batch) ==
                ipc::CertifiedTickJournalReadResultV1::kProducerFailed &&
            batch.written == 0U &&
            batch.next_canonical_apply_sequence == 3U &&
            batch.status.state ==
                ipc::CertifiedTickJournalStateV1::kFailed &&
            batch.status.failure ==
                ipc::CertifiedTickJournalAppendErrorV1::kTickCapacity &&
            batch.status.canonical_apply_frontier == 2U;
        std::array<
            std::byte,
            ipc::kCertifiedTickJournalSlotBytesV1>
            slot_bytes{};
        batch = {};
        const bool bytes_failed =
            reader->ReadSlotBytes(3U, slot_bytes, &batch) ==
                ipc::CertifiedTickJournalReadResultV1::kProducerFailed &&
            batch.written == 0U &&
            batch.next_canonical_apply_sequence == 3U &&
            batch.status.state ==
                ipc::CertifiedTickJournalStateV1::kFailed &&
            batch.status.failure ==
                ipc::CertifiedTickJournalAppendErrorV1::kTickCapacity &&
            batch.status.canonical_apply_frontier == 2U;
        ok &= Expect(
            envelope_failed && slot_failed && bytes_failed,
            "Read, ReadSlots, and FFI bytes expose FAILED at capacity+1");

        batch = {};
        ok &= Expect(
            reader->ReadSlots(4U, slots, &batch) ==
                    ipc::CertifiedTickJournalReadResultV1::kOutOfRange &&
                batch.written == 0U &&
                batch.next_canonical_apply_sequence == 4U,
            "FAILED exception is limited to the exact natural tail");
    }
    return ok;
}

bool TestConfigurationAndSessionValidation() {
    bool ok = true;
    auto invalid = JournalConfig(1U);
    invalid.run_id = {};
    std::shared_ptr<ipc::CertifiedTickJournalProducerV1> producer;
    ok &= Expect(
        ipc::CertifiedTickJournalProducerV1::Create(
            invalid, &producer) ==
            ipc::CertifiedTickJournalCreateErrorV1::
                kInvalidConfiguration,
        "producer rejects an all-zero run identity");

    auto overflow = JournalConfig(1U);
    overflow.tick_capacity =
        std::numeric_limits<std::uint64_t>::max();
    overflow.maximum_mapping_bytes = 0U;
    ok &= Expect(
        ipc::CertifiedTickJournalProducerV1::Create(
            overflow, &producer) ==
            ipc::CertifiedTickJournalCreateErrorV1::kLayoutOverflow,
        "producer rejects capacity arithmetic overflow");
    ok &= Expect(
        CreateJournal(2U, &producer),
        "create journal for reader session validation");
    if (producer == nullptr) {
        return false;
    }

    Descriptor descriptor;
    ok &= Expect(
        producer->DuplicateReadOnlyDescriptor(descriptor.output()),
        "duplicate sealed read-only descriptor");
    auto wrong_session = producer->session();
    ++wrong_session.session_epoch;
    std::unique_ptr<ipc::CertifiedTickJournalReaderV1> reader;
    ok &= Expect(
        ipc::CertifiedTickJournalReaderV1::Open(
            descriptor.get(), wrong_session, &reader) ==
                ipc::CertifiedTickJournalOpenErrorV1::
                    kSessionMismatch &&
            reader == nullptr,
        "reader rejects an otherwise valid descriptor with wrong identity");
    ok &= Expect(
        ipc::CertifiedTickJournalReaderV1::Open(
            -1, producer->session(), &reader) ==
            ipc::CertifiedTickJournalOpenErrorV1::kInvalidArgument,
        "reader rejects an invalid descriptor argument");

    const auto session = producer->session();
    const int closed_descriptor = ::dup(descriptor.get());
    const bool closed_descriptor_ready =
        closed_descriptor >= 0 && ::close(closed_descriptor) == 0;
    int system_error = -1;
    errno = EDOM;
    const auto closed_descriptor_error =
        closed_descriptor_ready
            ? ipc::CertifiedTickJournalReaderV1::Open(
                  closed_descriptor,
                  session,
                  &reader,
                  &system_error)
            : ipc::CertifiedTickJournalOpenErrorV1::kUnexpectedFailure;
    ok &= Expect(
        closed_descriptor_ready &&
            closed_descriptor_error ==
                ipc::CertifiedTickJournalOpenErrorV1::
                    kDescriptorInvalid &&
            reader == nullptr && system_error == EBADF,
        "reader reports the failing descriptor syscall errno");

    Descriptor read_write_descriptor;
    *read_write_descriptor.output() = ::memfd_create(
        "l2flow-tick-journal-invalid-access",
        MFD_CLOEXEC | MFD_ALLOW_SEALING);
    const bool read_write_ready =
        read_write_descriptor.get() >= 0 &&
        ::ftruncate(
            read_write_descriptor.get(),
            static_cast<off_t>(session.total_mapping_bytes)) == 0;
    system_error = -1;
    errno = EDOM;
    const auto read_write_error =
        read_write_ready
            ? ipc::CertifiedTickJournalReaderV1::Open(
                  read_write_descriptor.get(),
                  session,
                  &reader,
                  &system_error)
            : ipc::CertifiedTickJournalOpenErrorV1::kUnexpectedFailure;
    ok &= Expect(
        read_write_ready &&
            read_write_error ==
                ipc::CertifiedTickJournalOpenErrorV1::
                    kDescriptorInvalid &&
            reader == nullptr && system_error == EINVAL,
        "reader reports deterministic EINVAL for a writable descriptor");

    Descriptor wrong_size_backing;
    *wrong_size_backing.output() = ::memfd_create(
        "l2flow-tick-journal-invalid-size",
        MFD_CLOEXEC | MFD_ALLOW_SEALING);
    const bool wrong_size_backing_ready =
        wrong_size_backing.get() >= 0 &&
        session.total_mapping_bytes <=
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max()) - 4096U &&
        ::ftruncate(
            wrong_size_backing.get(),
            static_cast<off_t>(
                session.total_mapping_bytes + 4096U)) == 0;
    Descriptor wrong_size_descriptor;
    if (wrong_size_backing_ready) {
        const std::string path =
            "/proc/self/fd/" +
            std::to_string(wrong_size_backing.get());
        *wrong_size_descriptor.output() =
            ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    }
    const bool wrong_size_ready =
        wrong_size_backing_ready && wrong_size_descriptor.get() >= 0;
    system_error = -1;
    errno = EDOM;
    const auto wrong_size_error =
        wrong_size_ready
            ? ipc::CertifiedTickJournalReaderV1::Open(
                  wrong_size_descriptor.get(),
                  session,
                  &reader,
                  &system_error)
            : ipc::CertifiedTickJournalOpenErrorV1::kUnexpectedFailure;
    ok &= Expect(
        wrong_size_ready &&
            wrong_size_error ==
                ipc::CertifiedTickJournalOpenErrorV1::
                    kDescriptorInvalid &&
            reader == nullptr && system_error == EINVAL,
        "reader reports deterministic EINVAL for a wrong-size descriptor");

    ipc::CertifiedTickJournalHeaderV1 header{};
    header.magic = ipc::kCertifiedTickJournalMagicV1;
    header.abi_major = ipc::kCertifiedTickJournalWireMajorV1;
    header.abi_minor = ipc::kCertifiedTickJournalWireMinorV1;
    header.header_bytes = ipc::kCertifiedTickJournalHeaderBytesV1;
    header.endian_marker =
        ipc::kCertifiedTickJournalEndianMarkerV1;
    header.total_mapping_bytes = session.total_mapping_bytes;
    header.run_id[0U] = 0xA5U;
    header.session_epoch = session.session_epoch;
    header.trade_date = session.trade_date;
    header.slots_offset = ipc::kCertifiedTickJournalHeaderBytesV1;
    header.tick_capacity = session.tick_capacity;
    header.slot_stride = ipc::kCertifiedTickJournalSlotBytesV1;
    header.region_alignment = ipc::kCertifiedTickJournalAlignmentV1;
    header.status_publish_tag = 2U;
    header.heartbeat_monotonic_ns = 1U;
    header.committed_mapping_bytes =
        ipc::kCertifiedTickJournalHeaderBytesV1;
    header.state = static_cast<std::uint32_t>(
        ipc::CertifiedTickJournalStateV1::kActive);
    header.coverage_kind = static_cast<std::uint32_t>(
        ipc::CertifiedTickJournalCoverageKindV1::kFromOpen);
    ok &= Expect(
        ipc::CertifiedTickJournalHeaderCanonicalV1(header),
        "stable canonical header accepts the exact checked layout");
    header.coverage_kind = 0U;
    ok &= Expect(
        !ipc::CertifiedTickJournalHeaderCanonicalV1(header),
        "canonical header rejects an unspecified temporal coverage kind");
    header.coverage_kind = static_cast<std::uint32_t>(
        ipc::CertifiedTickJournalCoverageKindV1::kFromOpen);
    header.coverage_start_unix_ns = 1U;
    ok &= Expect(
        !ipc::CertifiedTickJournalHeaderCanonicalV1(header),
        "from-open Tick history rejects a process-start boundary");
    header.coverage_start_unix_ns = 0U;
    header.total_mapping_bytes += 4096U;
    ok &= Expect(
        !ipc::CertifiedTickJournalHeaderCanonicalV1(header),
        "canonical header rejects surplus aliasable mapping space");
    return ok;
}

bool TestConcurrentHistoryToTailNeverTears() {
    constexpr std::uint64_t tick_count = 20'000U;
    constexpr std::size_t writer_batch_size = 23U;
    bool ok = true;
    std::shared_ptr<ipc::CertifiedTickJournalProducerV1> producer;
    ok &= Expect(
        CreateJournal(
            tick_count, &producer, 64U * 1024U),
        "create concurrent history-to-tail journal");
    if (producer == nullptr) {
        return false;
    }
    std::unique_ptr<ipc::CertifiedTickJournalReaderV1> reader;
    ok &= Expect(
        OpenReader(producer, &reader),
        "open concurrent reader before first append");
    if (reader == nullptr) {
        return false;
    }

    std::atomic<bool> writer_done{false};
    std::atomic<bool> writer_failed{false};
    std::thread writer([&] {
        std::array<
            ipc::RealtimeCertifiedTickEnvelopeV1,
            writer_batch_size>
            batch{};
        std::uint64_t next = 1U;
        while (next <= tick_count) {
            const std::uint64_t remaining = tick_count - next + 1U;
            const std::size_t count = static_cast<std::size_t>(
                std::min<std::uint64_t>(
                    remaining,
                    static_cast<std::uint64_t>(
                        writer_batch_size)));
            for (std::size_t index = 0U; index < count; ++index) {
                batch[index] = Tick(
                    next + static_cast<std::uint64_t>(index));
            }
            if (producer->Append(
                    std::span(batch).first(count)) !=
                ipc::CertifiedTickJournalAppendErrorV1::kNone) {
                writer_failed.store(true, std::memory_order_release);
                break;
            }
            next += static_cast<std::uint64_t>(count);
        }
        if (!writer_failed.load(std::memory_order_acquire) &&
            !producer->Stop()) {
            writer_failed.store(true, std::memory_order_release);
        }
        writer_done.store(true, std::memory_order_release);
    });

    bool reader_failed = false;
    std::uint64_t next = 1U;
    std::array<ipc::RealtimeCertifiedTickEnvelopeV1, 64U> rows{};
    while (next <= tick_count) {
        ipc::CertifiedTickJournalReadBatchV1 batch{};
        const auto result = reader->Read(next, rows, &batch);
        if (result ==
                ipc::CertifiedTickJournalReadResultV1::
                    kNotYetPublished ||
            result ==
                ipc::CertifiedTickJournalReadResultV1::kInconsistent) {
            if (writer_done.load(std::memory_order_acquire) &&
                writer_failed.load(std::memory_order_acquire)) {
                reader_failed = true;
                break;
            }
            std::this_thread::yield();
            continue;
        }
        if (result != ipc::CertifiedTickJournalReadResultV1::kOk ||
            batch.written == 0U ||
            batch.next_canonical_apply_sequence !=
                next + static_cast<std::uint64_t>(batch.written) ||
            batch.status.generation !=
                batch.status.canonical_apply_frontier ||
            batch.status.published_tick_count !=
                batch.status.canonical_apply_frontier) {
            reader_failed = true;
            break;
        }
        for (std::size_t index = 0U;
             index < batch.written;
             ++index) {
            const std::uint64_t expected =
                next + static_cast<std::uint64_t>(index);
            if (rows[index].canonical_apply_sequence != expected ||
                rows[index].payload.native_event_sequence !=
                    static_cast<std::int64_t>(expected) ||
                !ipc::RealtimeCertifiedTickEnvelopeCanonicalV1(
                    rows[index])) {
                reader_failed = true;
                break;
            }
        }
        if (reader_failed) {
            break;
        }
        next = batch.next_canonical_apply_sequence;
    }
    writer.join();

    ok &= Expect(
        !writer_failed.load(std::memory_order_acquire),
        "concurrent writer publishes and completes the dense prefix");
    ok &= Expect(
        !reader_failed && next == tick_count + 1U,
        "concurrent reader never accepts a torn status cut or Tick row");
    ipc::CertifiedTickJournalStatusV1 status{};
    ipc::RealtimeCertifiedTickEnvelopeV1 row{};
    ok &= Expect(
        reader->ReadStatus(&status) ==
                ipc::CertifiedTickJournalReadResultV1::kOk &&
            status.state ==
                ipc::CertifiedTickJournalStateV1::kComplete &&
            status.canonical_apply_frontier == tick_count &&
            reader->ReadOne(tick_count + 1U, &row) ==
                ipc::CertifiedTickJournalReadResultV1::kEndOfStream,
        "full-prefix natural cursor preserves COMPLETE end-of-stream");
    return ok;
}

}  // namespace

int main() {
    bool ok = true;
    ok &= TestHistoryToTailAndCompleteLifecycle();
    ok &= TestFailurePreservesPublishedPrefix();
    ok &= TestFullCapacityNaturalTailPreservesLifecycle();
    ok &= TestConfigurationAndSessionValidation();
    ok &= TestConcurrentHistoryToTailNeverTears();
    if (ok) {
        std::cout << "PASS: certified Tick journal v1\n";
        return 0;
    }
    return 1;
}
