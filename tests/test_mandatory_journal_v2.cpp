#include "l2flow/common/crc32c.h"
#include "l2flow/realtime/mandatory_journal_v2.h"
#include "l2flow/realtime/owned_ingress_message_v1.h"
#include "l2flow/sdk/vendor_head_view.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <span>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

namespace mdl = datayes::mdl;
namespace realtime = l2flow::realtime;
namespace sdk = l2flow::sdk;

namespace {

using namespace std::chrono_literals;

class TestContext final {
public:
    void Expect(bool condition, std::string_view description) {
        if (condition) {
            return;
        }
        ++failures_;
        std::cerr << "FAIL: " << description << '\n';
    }

    [[nodiscard]] int failures() const noexcept { return failures_; }

private:
    int failures_ = 0;
};

class FakeMessage final : public mdl::MDLMessage {
public:
    FakeMessage(sdk::MessageKey key, std::vector<std::byte> body)
        : body_(std::move(body)) {
        std::memset(&head_, 0, sizeof(head_));
        head_.HeadSize =
            static_cast<std::uint8_t>(sdk::kVendorHeadBytes);
        head_.MessageSize = static_cast<std::uint32_t>(
            sdk::kVendorHeadBytes + body_.size());
        head_.MessageEncoding =
            static_cast<std::uint8_t>(mdl::MDLEID_BINARY);
        head_.ServiceID = key.service_id;
        head_.ServiceVersion = key.service_version;
        head_.MessageID = key.message_id;
        head_.LocalTime.m_Value = 0x12345678U;
        head_.SequenceID = 9988U;
    }

    void AddRef() override {}
    int ReleaseRef() override { return 1; }
    mdl::MDLMessageHead* GetHead() const override {
        return const_cast<mdl::MDLMessageHead*>(&head_);
    }
    char* GetBody() const override {
        return body_.empty()
                   ? nullptr
                   : reinterpret_cast<char*>(
                         const_cast<std::byte*>(body_.data()));
    }
    mdl::MDLMessage* _Copy() const override { return nullptr; }

private:
    mdl::MDLMessageHead head_{};
    std::vector<std::byte> body_;
};

[[nodiscard]] realtime::OwnedIngressMetadataV1 Metadata(
    std::uint64_t global_sequence,
    std::uint64_t source_sequence,
    std::uint64_t tick_stream_sequence) {
    realtime::OwnedIngressMetadataV1 result{};
    result.run_id[0U] = std::byte{0x51U};
    result.run_id[15U] = std::byte{0xa5U};
    result.global_ingress_sequence = global_sequence;
    result.source_sequence = source_sequence;
    result.tick_stream_sequence = tick_stream_sequence;
    result.recv_realtime_ns =
        1'721'800'000'000'000'000ULL + global_sequence;
    result.recv_monotonic_ns = 123'000U + global_sequence;
    return result;
}

[[nodiscard]] std::unique_ptr<realtime::OwnedIngressMessagePoolV1>
MakePool(TestContext* test, std::size_t capacity = 32U) {
    std::unique_ptr<realtime::OwnedIngressMessagePoolV1> result;
    const realtime::OwnedIngressMessageErrorV1 error =
        realtime::OwnedIngressMessagePoolV1::Create(
            realtime::OwnedIngressMessagePoolConfigV1{4096U, capacity},
            &result);
    test->Expect(
        error == realtime::OwnedIngressMessageErrorV1::kNone &&
            result != nullptr,
        "owned ingress pool creation succeeds");
    return result;
}

[[nodiscard]] realtime::OwnedIngressMessageHandleV1 MakeOwned(
    TestContext* test,
    realtime::OwnedIngressMessagePoolV1* pool,
    sdk::MessageKey key,
    std::vector<std::byte> body,
    std::uint64_t global_sequence,
    std::uint64_t source_sequence,
    std::uint64_t tick_stream_sequence) {
    FakeMessage message(key, std::move(body));
    realtime::OwnedIngressMessageInspectionV1 inspection;
    realtime::OwnedIngressMessageHandleV1 result;
    const realtime::OwnedIngressMessageErrorV1 inspect_error =
        realtime::InspectOwnedIngressMessageV1(
            &message, 4096U, &inspection);
    const realtime::OwnedIngressMessageErrorV1 acquire_error =
        inspect_error == realtime::OwnedIngressMessageErrorV1::kNone
            ? pool->Acquire(
                  inspection,
                  Metadata(
                      global_sequence,
                      source_sequence,
                      tick_stream_sequence),
                  &result)
            : inspect_error;
    test->Expect(
        inspect_error ==
                realtime::OwnedIngressMessageErrorV1::kNone &&
            acquire_error ==
                realtime::OwnedIngressMessageErrorV1::kNone &&
            static_cast<bool>(result),
        "owned ingress message creation succeeds");
    return result;
}

class TemporaryDirectory final {
public:
    TemporaryDirectory() {
        std::array<char, 64U> pattern{};
        const char literal[] = "/tmp/l2flow-mandatory-journal-v2-XXXXXX";
        static_assert(sizeof(literal) <= pattern.size());
        std::memcpy(pattern.data(), literal, sizeof(literal));
        char* const created = ::mkdtemp(pattern.data());
        if (created != nullptr) {
            path_ = created;
        }
    }

    ~TemporaryDirectory() {
        if (!path_.empty()) {
            std::error_code ignored;
            std::filesystem::remove_all(path_, ignored);
        }
    }

    TemporaryDirectory(const TemporaryDirectory&) = delete;
    TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

struct SinkState final {
    std::atomic<bool> block_sync{false};
    std::atomic<bool> entered_sync{false};
    std::atomic<bool> release_sync{false};
    std::atomic<std::uint64_t> durable_sequence{0U};
    std::atomic<std::uint64_t> failure_calls{0U};
    std::atomic<std::uint8_t> failure_kind{0U};
};

void BeforeSyncForTest(void* context) noexcept {
    auto* const state = static_cast<SinkState*>(context);
    if (state == nullptr ||
        !state->block_sync.load(std::memory_order_acquire)) {
        return;
    }
    state->entered_sync.store(true, std::memory_order_release);
    while (!state->release_sync.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
}

void Durable(void* context, std::uint64_t sequence) noexcept {
    auto* const state = static_cast<SinkState*>(context);
    if (state != nullptr) {
        state->durable_sequence.store(
            sequence, std::memory_order_release);
    }
}

void Failure(
    void* context,
    realtime::MandatoryJournalFailureKindV2 kind,
    int) noexcept {
    auto* const state = static_cast<SinkState*>(context);
    if (state != nullptr) {
        state->failure_kind.store(
            static_cast<std::uint8_t>(kind),
            std::memory_order_release);
        state->failure_calls.fetch_add(1U, std::memory_order_acq_rel);
    }
}

[[nodiscard]] realtime::MandatoryJournalConfigV2 Config(
    const std::filesystem::path& path,
    SinkState* state,
    std::size_t queue_capacity,
    std::size_t max_batch_records,
    std::chrono::microseconds max_batch_delay) {
    realtime::MandatoryJournalConfigV2 result{};
    result.path = path.string();
    result.maximum_message_bytes = 4096U;
    result.queue_capacity = queue_capacity;
    result.max_batch_records = max_batch_records;
    result.max_batch_delay = max_batch_delay;
    result.durable = &Durable;
    result.durable_context = state;
    result.failure = &Failure;
    result.failure_context = state;
    result.before_sync_for_test = &BeforeSyncForTest;
    result.before_sync_context_for_test = state;
    return result;
}

[[nodiscard]] bool WaitUntil(
    const std::atomic<bool>& value,
    std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!value.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    return value.load(std::memory_order_acquire);
}

[[nodiscard]] std::uint16_t LoadU16Little(
    std::span<const std::byte> bytes,
    std::size_t offset) {
    return static_cast<std::uint16_t>(
        std::to_integer<std::uint16_t>(bytes[offset]) |
        static_cast<std::uint16_t>(
            std::to_integer<std::uint16_t>(bytes[offset + 1U])
            << 8U));
}

[[nodiscard]] std::uint32_t LoadU32Little(
    std::span<const std::byte> bytes,
    std::size_t offset) {
    std::uint32_t result = 0U;
    for (std::size_t index = 0U; index < 4U; ++index) {
        result |=
            std::to_integer<std::uint32_t>(bytes[offset + index])
            << static_cast<unsigned int>(index * 8U);
    }
    return result;
}

[[nodiscard]] std::uint64_t LoadU64Little(
    std::span<const std::byte> bytes,
    std::size_t offset) {
    std::uint64_t result = 0U;
    for (std::size_t index = 0U; index < 8U; ++index) {
        result |=
            std::to_integer<std::uint64_t>(bytes[offset + index])
            << static_cast<unsigned int>(index * 8U);
    }
    return result;
}

[[nodiscard]] std::vector<std::byte> ReadBytes(
    const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    const std::vector<char> characters{
        std::istreambuf_iterator<char>(input),
        std::istreambuf_iterator<char>()};
    std::vector<std::byte> result(characters.size());
    if (!characters.empty()) {
        std::memcpy(
            result.data(), characters.data(), characters.size());
    }
    return result;
}

void CheckFreshOnlyCreateContract(TestContext* test) {
    TemporaryDirectory temporary;
    if (temporary.path().empty()) {
        test->Expect(false, "temporary journal directory is available");
        return;
    }

    SinkState state;
    std::unique_ptr<realtime::MandatoryJournalV2> journal;
    int system_error = 0;
    test->Expect(
        realtime::MandatoryJournalV2::Create(
            Config(
                temporary.path() / "missing" / "journal.bin",
                &state,
                4U,
                2U,
                200us),
            &journal,
            &system_error) ==
                realtime::MandatoryJournalCreateErrorV2::kOpen &&
            journal == nullptr && system_error != 0,
        "fresh journal fails when its file cannot be opened");

    const std::filesystem::path existing =
        temporary.path() / "existing.bin";
    const int existing_fd =
        ::open(existing.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    test->Expect(existing_fd >= 0, "existing-file fixture is created");
    if (existing_fd >= 0) {
        const std::array<char, 4U> marker{{'k', 'e', 'e', 'p'}};
        test->Expect(
            ::write(existing_fd, marker.data(), marker.size()) ==
                static_cast<ssize_t>(marker.size()),
            "existing-file marker is written");
        static_cast<void>(::close(existing_fd));
    }
    test->Expect(
        realtime::MandatoryJournalV2::Create(
            Config(existing, &state, 4U, 2U, 200us),
            &journal,
            &system_error) ==
                realtime::MandatoryJournalCreateErrorV2::kOpen &&
            journal == nullptr &&
            ReadBytes(existing) ==
                std::vector<std::byte>{
                    std::byte{'k'},
                    std::byte{'e'},
                    std::byte{'e'},
                    std::byte{'p'}},
        "fresh-only journal neither resumes nor truncates an existing path");

    realtime::MandatoryJournalConfigV2 invalid =
        Config(
            temporary.path() / "invalid.bin",
            &state,
            4U,
            2U,
            200us);
    invalid.queue_capacity = 0U;
    test->Expect(
        realtime::MandatoryJournalV2::Create(
            std::move(invalid), &journal) ==
                realtime::MandatoryJournalCreateErrorV2::
                    kInvalidConfiguration &&
            journal == nullptr,
        "invalid bounded queue configuration is rejected");
    test->Expect(
        realtime::MandatoryJournalV2::Create(
            Config(
                temporary.path() / "null-output.bin",
                &state,
                4U,
                2U,
                200us),
            nullptr) ==
            realtime::MandatoryJournalCreateErrorV2::kNullOutput,
        "null create output is rejected without creating a file");
}

void CheckBlockedSyncDoesNotOwnProcessingReference(TestContext* test) {
    TemporaryDirectory temporary;
    auto pool = MakePool(test);
    if (temporary.path().empty() || pool == nullptr) {
        return;
    }

    SinkState state;
    state.block_sync.store(true, std::memory_order_release);
    std::unique_ptr<realtime::MandatoryJournalV2> journal;
    test->Expect(
        realtime::MandatoryJournalV2::Create(
            Config(
                temporary.path() / "blocked-sync.bin",
                &state,
                4U,
                1U,
                10us),
            &journal) ==
                realtime::MandatoryJournalCreateErrorV2::kNone &&
            journal != nullptr,
        "blocked-sync journal starts");
    if (journal == nullptr) {
        return;
    }

    auto owned = MakeOwned(
        test,
        pool.get(),
        sdk::MessageKey{4U, 101U, 24U},
        {std::byte{0x31U}},
        1U,
        1U,
        1U);
    realtime::OwnedIngressMessageHandleV1 processing_message = owned;
    test->Expect(
        journal->TryAppend(std::move(owned)) ==
                realtime::MandatoryJournalAppendResultV2::kAccepted &&
            !owned,
        "callback-side Journal admission is nonblocking and transfers only "
        "one intrusive reference");
    const bool entered = WaitUntil(state.entered_sync, 2s);
    test->Expect(entered, "writer is deterministically blocked before sync");
    if (!entered) {
        state.release_sync.store(true, std::memory_order_release);
        journal->StopAndDrain();
        return;
    }

    const realtime::MandatoryJournalSnapshotV2 blocked =
        journal->Snapshot();
    test->Expect(
        processing_message &&
            processing_message->global_ingress_sequence() == 1U &&
            processing_message->body().size() == 1U &&
            blocked.accepted_sequence == 1U &&
            blocked.written_sequence == 1U &&
            blocked.durable_sequence == 0U &&
            blocked.durability_lag_records == 1U,
        "a separate processing reference remains immediately usable while "
        "durability is stalled");
    processing_message.reset();
    state.release_sync.store(true, std::memory_order_release);
    journal->StopAndDrain();
    const realtime::MandatoryJournalSnapshotV2 stopped =
        journal->Snapshot();
    test->Expect(
        stopped.finished && stopped.stop_requested &&
            !stopped.accepting && !stopped.failed() &&
            stopped.durable_sequence == 1U &&
            stopped.durability_lag_records == 0U &&
            state.durable_sequence.load(std::memory_order_acquire) == 1U,
        "writer independently becomes durable and stop drains without a "
        "clean marker");
}

void CheckRecordFramingWithoutHeader(TestContext* test) {
    TemporaryDirectory temporary;
    auto pool = MakePool(test);
    if (temporary.path().empty() || pool == nullptr) {
        return;
    }

    const std::filesystem::path path =
        temporary.path() / "records.bin";
    SinkState state;
    std::unique_ptr<realtime::MandatoryJournalV2> journal;
    test->Expect(
        realtime::MandatoryJournalV2::Create(
            Config(path, &state, 8U, 3U, 20ms),
            &journal) ==
                realtime::MandatoryJournalCreateErrorV2::kNone &&
            journal != nullptr,
        "framing journal starts");
    if (journal == nullptr) {
        return;
    }

    struct stat status {};
    test->Expect(
        ::stat(path.c_str(), &status) == 0 &&
            S_ISREG(status.st_mode) &&
            (status.st_mode & 0777U) == 0600U,
        "created Journal is one regular 0600 file");

    const std::array<std::vector<std::byte>, 3U> bodies{{
        {std::byte{0x10U}},
        {std::byte{0x20U}, std::byte{0x21U}},
        {std::byte{0x30U}, std::byte{0x31U}, std::byte{0x32U}},
    }};
    for (std::size_t index = 0U; index < bodies.size(); ++index) {
        const std::uint64_t sequence =
            static_cast<std::uint64_t>(index) + 1U;
        auto message = MakeOwned(
            test,
            pool.get(),
            sdk::MessageKey{4U, 101U, 24U},
            bodies[index],
            sequence,
            100U + sequence,
            sequence);
        test->Expect(
            journal->TryAppend(std::move(message)) ==
                realtime::MandatoryJournalAppendResultV2::kAccepted,
            "dense record is admitted");
    }
    journal->StopAndDrain();

    const realtime::MandatoryJournalSnapshotV2 snapshot =
        journal->Snapshot();
    test->Expect(
        snapshot.accepted_records == 3U &&
            snapshot.written_records == 3U &&
            snapshot.rejected_records == 0U &&
            snapshot.accepted_sequence == 3U &&
            snapshot.written_sequence == 3U &&
            snapshot.durable_sequence == 3U &&
            snapshot.durability_lag_records == 0U &&
            !snapshot.failed(),
        "snapshot reports only admission, write, and durability prefixes");

    const std::vector<std::byte> bytes = ReadBytes(path);
    std::size_t offset = 0U;
    for (std::size_t index = 0U; index < bodies.size(); ++index) {
        const std::size_t minimum_record_bytes =
            realtime::kMandatoryJournalRecordPrefixBytesV2 +
            realtime::kMandatoryJournalRecordChecksumBytesV2;
        test->Expect(
            offset <= bytes.size() &&
                bytes.size() - offset >= minimum_record_bytes,
            "record has a complete fixed prefix and checksum");
        if (offset > bytes.size() ||
            bytes.size() - offset < minimum_record_bytes) {
            return;
        }
        const std::span<const std::byte> suffix(
            bytes.data() + offset, bytes.size() - offset);
        const std::uint32_t record_bytes = LoadU32Little(
            suffix,
            realtime::kMandatoryJournalRecordBytesOffsetV2);
        const std::size_t expected_record_bytes =
            realtime::kMandatoryJournalRecordPrefixBytesV2 +
            bodies[index].size() +
            realtime::kMandatoryJournalRecordChecksumBytesV2;
        test->Expect(
            record_bytes == expected_record_bytes &&
                static_cast<std::size_t>(record_bytes) <= suffix.size(),
            "file begins directly with one bounded record frame");
        if (record_bytes != expected_record_bytes ||
            static_cast<std::size_t>(record_bytes) > suffix.size()) {
            return;
        }
        const std::span<const std::byte> frame =
            suffix.first(static_cast<std::size_t>(record_bytes));
        const std::uint64_t sequence =
            static_cast<std::uint64_t>(index) + 1U;
        test->Expect(
            LoadU32Little(
                frame,
                realtime::kMandatoryJournalRecordMagicOffsetV2) ==
                    realtime::kMandatoryJournalRecordMagicV2 &&
                LoadU16Little(
                    frame,
                    realtime::kMandatoryJournalRecordVersionOffsetV2) ==
                    realtime::kMandatoryJournalRecordVersionV2 &&
                LoadU16Little(
                    frame,
                    realtime::
                        kMandatoryJournalRecordPrefixBytesOffsetV2) ==
                    realtime::kMandatoryJournalRecordPrefixBytesV2 &&
                LoadU32Little(
                    frame,
                    realtime::kMandatoryJournalRecordBodyBytesOffsetV2) ==
                    bodies[index].size() &&
                LoadU64Little(
                    frame,
                    realtime::
                        kMandatoryJournalRecordGlobalSequenceOffsetV2) ==
                    sequence &&
                LoadU64Little(
                    frame,
                    realtime::
                        kMandatoryJournalRecordSourceSequenceOffsetV2) ==
                    100U + sequence &&
                LoadU64Little(
                    frame,
                    realtime::
                        kMandatoryJournalRecordTickSequenceOffsetV2) ==
                    sequence,
            "record preserves framing and ingress sequences");
        const std::span<const std::byte> body = frame.subspan(
            realtime::kMandatoryJournalRecordPrefixBytesV2,
            bodies[index].size());
        const std::uint32_t stored_crc = LoadU32Little(
            frame,
            static_cast<std::size_t>(record_bytes) -
                realtime::kMandatoryJournalRecordChecksumBytesV2);
        const std::uint32_t actual_crc =
            l2flow::common::ComputeCrc32c(frame.subspan(
                sizeof(std::uint32_t),
                static_cast<std::size_t>(record_bytes) -
                    sizeof(std::uint32_t) -
                    realtime::
                        kMandatoryJournalRecordChecksumBytesV2));
        test->Expect(
            std::equal(
                body.begin(),
                body.end(),
                bodies[index].begin(),
                bodies[index].end()) &&
                stored_crc == actual_crc,
            "record preserves exact body bytes and their CRC32C");
        offset += static_cast<std::size_t>(record_bytes);
    }
    test->Expect(
        offset == bytes.size(),
        "fresh sink contains records only, without header or stop marker");
}

void CheckQueueOverflowIsNonblockingAndSticky(TestContext* test) {
    TemporaryDirectory temporary;
    auto pool = MakePool(test);
    if (temporary.path().empty() || pool == nullptr) {
        return;
    }

    SinkState state;
    state.block_sync.store(true, std::memory_order_release);
    std::unique_ptr<realtime::MandatoryJournalV2> journal;
    test->Expect(
        realtime::MandatoryJournalV2::Create(
            Config(
                temporary.path() / "queue-full.bin",
                &state,
                1U,
                1U,
                10us),
            &journal) ==
                realtime::MandatoryJournalCreateErrorV2::kNone &&
            journal != nullptr,
        "queue-overflow journal starts");
    if (journal == nullptr) {
        return;
    }

    auto first = MakeOwned(
        test,
        pool.get(),
        sdk::MessageKey{4U, 101U, 24U},
        {std::byte{0x51U}},
        1U,
        1U,
        1U);
    test->Expect(
        journal->TryAppend(std::move(first)) ==
            realtime::MandatoryJournalAppendResultV2::kAccepted,
        "writer accepts first record");
    const bool entered = WaitUntil(state.entered_sync, 2s);
    test->Expect(entered, "writer removes first record before blocking");
    if (!entered) {
        state.release_sync.store(true, std::memory_order_release);
        journal->StopAndDrain();
        return;
    }

    auto second = MakeOwned(
        test,
        pool.get(),
        sdk::MessageKey{4U, 101U, 24U},
        {std::byte{0x52U}},
        2U,
        2U,
        2U);
    auto third = MakeOwned(
        test,
        pool.get(),
        sdk::MessageKey{4U, 101U, 24U},
        {std::byte{0x53U}},
        3U,
        3U,
        3U);
    test->Expect(
        journal->TryAppend(std::move(second)) ==
                realtime::MandatoryJournalAppendResultV2::kAccepted &&
            journal->TryAppend(std::move(third)) ==
                realtime::MandatoryJournalAppendResultV2::kQueueFull,
        "bounded admission reports queue full without waiting for disk");

    auto retry = MakeOwned(
        test,
        pool.get(),
        sdk::MessageKey{4U, 101U, 24U},
        {std::byte{0x54U}},
        3U,
        3U,
        3U);
    test->Expect(
        journal->TryAppend(std::move(retry)) ==
            realtime::MandatoryJournalAppendResultV2::kFailed,
        "queue overflow is sticky and closes Journal admission");

    state.release_sync.store(true, std::memory_order_release);
    journal->StopAndDrain();
    const realtime::MandatoryJournalSnapshotV2 snapshot =
        journal->Snapshot();
    test->Expect(
        snapshot.finished && snapshot.failed() &&
            snapshot.failure_kind ==
                realtime::MandatoryJournalFailureKindV2::kQueueFull &&
            snapshot.accepted_records == 2U &&
            snapshot.rejected_records == 2U &&
            snapshot.durable_sequence == 2U &&
            snapshot.durability_lag_records == 0U &&
            state.failure_calls.load(std::memory_order_acquire) == 0U,
        "producer overflow preserves and drains only its admitted prefix; "
        "writer-failure callback is not misreported");
}

void CheckWriterFailureCallbackIsOnce(TestContext* test) {
    TemporaryDirectory temporary;
    auto pool = MakePool(test);
    if (temporary.path().empty() || pool == nullptr) {
        return;
    }

    SinkState state;
    realtime::MandatoryJournalConfigV2 config = Config(
        temporary.path() / "writer-failure.bin",
        &state,
        2U,
        1U,
        10us);
    config.maximum_message_bytes =
        static_cast<std::uint32_t>(sdk::kVendorHeadBytes);
    std::unique_ptr<realtime::MandatoryJournalV2> journal;
    test->Expect(
        realtime::MandatoryJournalV2::Create(
            std::move(config), &journal) ==
                realtime::MandatoryJournalCreateErrorV2::kNone &&
            journal != nullptr,
        "writer-failure journal starts");
    if (journal == nullptr) {
        return;
    }

    auto oversized = MakeOwned(
        test,
        pool.get(),
        sdk::MessageKey{4U, 101U, 24U},
        {std::byte{0x61U}},
        1U,
        1U,
        1U);
    test->Expect(
        journal->TryAppend(std::move(oversized)) ==
            realtime::MandatoryJournalAppendResultV2::kAccepted,
        "producer admits without doing writer-side record work");
    journal->StopAndDrain();
    const realtime::MandatoryJournalSnapshotV2 snapshot =
        journal->Snapshot();
    test->Expect(
        snapshot.failed() &&
            snapshot.failure_kind ==
                realtime::MandatoryJournalFailureKindV2::kRecordTooLarge &&
            snapshot.accepted_sequence == 1U &&
            snapshot.durable_sequence == 0U &&
            snapshot.durability_lag_records == 1U &&
            state.failure_calls.load(std::memory_order_acquire) == 1U &&
            state.failure_kind.load(std::memory_order_acquire) ==
                static_cast<std::uint8_t>(
                    realtime::MandatoryJournalFailureKindV2::
                        kRecordTooLarge),
        "the first background failure is published and notified exactly "
        "once");
}

}  // namespace

int main() {
    TestContext test;
    CheckFreshOnlyCreateContract(&test);
    CheckBlockedSyncDoesNotOwnProcessingReference(&test);
    CheckRecordFramingWithoutHeader(&test);
    CheckQueueOverflowIsNonblockingAndSticky(&test);
    CheckWriterFailureCallbackIsOnce(&test);
    return test.failures() == 0 ? 0 : 1;
}
