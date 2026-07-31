#include "l2flow/recovery/live_journal_v1.h"

#include "l2flow/realtime/native_sequence_recovery_v1.h"
#include "l2flow/realtime/owned_ingress_message_v1.h"
#include "l2flow/sdk/vendor_head_view.h"

#include "mdl_api.h"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace {

namespace common = l2flow::common;
namespace mdl = datayes::mdl;
namespace realtime = l2flow::realtime;
namespace recovery = l2flow::recovery;
namespace sdk = l2flow::sdk;

using namespace std::chrono_literals;

constexpr std::uint32_t kTradeDate = 20260731U;
constexpr std::size_t kSegmentHeaderBytes = 128U;
constexpr std::size_t kRecordHeaderBytes = 192U;

class TestContext final {
public:
    void Expect(bool condition, std::string_view description) {
        if (!condition) {
            ++failures_;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    [[nodiscard]] int failures() const noexcept { return failures_; }

private:
    int failures_ = 0;
};

class ScopedDirectory final {
public:
    explicit ScopedDirectory(std::string_view label) {
        static std::uint64_t next = 0U;
        ++next;
        path_ = std::filesystem::temp_directory_path() /
                ("l2flow-live-journal-" + std::string(label) + "-" +
                 std::to_string(static_cast<unsigned long long>(::getpid())) +
                 "-" +
                 std::to_string(static_cast<unsigned long long>(next)));
        std::error_code error;
        static_cast<void>(std::filesystem::remove_all(path_, error));
    }

    ScopedDirectory(const ScopedDirectory&) = delete;
    ScopedDirectory& operator=(const ScopedDirectory&) = delete;

    ~ScopedDirectory() {
        std::error_code error;
        static_cast<void>(std::filesystem::remove_all(path_, error));
    }

    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

class WireWriter final {
public:
    explicit WireWriter(std::size_t fixed_bytes)
        : bytes_(fixed_bytes, std::byte{0U}) {}

    void StoreU32(std::size_t offset, std::uint32_t value) {
        StoreUnsigned(offset, value);
    }

    void StoreU64(std::size_t offset, std::uint64_t value) {
        StoreUnsigned(offset, value);
    }

    void StoreString(std::size_t descriptor, std::string_view value) {
        const std::size_t start = bytes_.size();
        StoreUnsigned(
            descriptor, static_cast<std::uint16_t>(value.size()));
        StoreU32(
            descriptor + sizeof(std::uint16_t),
            value.empty()
                ? 0U
                : static_cast<std::uint32_t>(start - descriptor));
        const auto encoded = std::as_bytes(std::span(value));
        bytes_.insert(bytes_.end(), encoded.begin(), encoded.end());
    }

    [[nodiscard]] std::vector<std::byte> Take() && {
        return std::move(bytes_);
    }

private:
    template <typename Unsigned>
    void StoreUnsigned(std::size_t offset, Unsigned value) {
        static_assert(std::is_unsigned_v<Unsigned>);
        for (std::size_t index = 0U; index < sizeof(Unsigned); ++index) {
            bytes_.at(offset + index) = static_cast<std::byte>(
                (value >> static_cast<unsigned int>(index * 8U)) &
                static_cast<Unsigned>(0xffU));
        }
    }

    std::vector<std::byte> bytes_;
};

[[nodiscard]] std::vector<std::byte> ShenzhenTransactionBody(
    std::uint32_t channel,
    std::uint64_t application_sequence,
    std::uint64_t price_raw = 123'456U) {
    WireWriter writer(70U);
    writer.StoreU32(0U, channel);
    writer.StoreU64(4U, application_sequence);
    writer.StoreU64(18U, application_sequence - 1U);
    writer.StoreU64(26U, 0U);
    writer.StoreU64(46U, price_raw);
    writer.StoreU64(54U, 33U);
    writer.StoreU32(62U, 70U);
    writer.StoreU32(66U, 93'000'124U);
    writer.StoreString(12U, "010");
    writer.StoreString(34U, "000001");
    writer.StoreString(40U, "102 ");
    return std::move(writer).Take();
}

class FakeMessage final : public mdl::MDLMessage {
public:
    FakeMessage(
        sdk::MessageKey key,
        std::uint64_t vendor_sequence,
        std::vector<std::byte> body)
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
        head_.LocalTime.m_Value = 93'000'000U;
        head_.SequenceID = vendor_sequence;
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

    [[nodiscard]] std::span<const std::byte> body() const noexcept {
        return body_;
    }

private:
    mdl::MDLMessageHead head_{};
    std::vector<std::byte> body_;
};

[[nodiscard]] common::Identity128 RunId(std::uint8_t first) {
    common::Identity128 result{};
    result[0U] = static_cast<std::byte>(first);
    result[15U] = std::byte{0xa5U};
    return result;
}

[[nodiscard]] recovery::LiveJournalConfigV1 MakeConfig(
    const std::filesystem::path& directory,
    std::uint8_t run_id = 0x31U) {
    recovery::LiveJournalConfigV1 config{};
    config.directory = directory;
    config.run_id = RunId(run_id);
    config.trade_date = kTradeDate;
    config.maximum_message_bytes = 1024U;
    config.segment_maximum_bytes =
        recovery::kLiveJournalMinimumSegmentBytesV1;
    config.maximum_total_bytes = 4ULL * 1024ULL * 1024ULL;
    config.queue_capacity_records = 16U;
    config.sync_batch_records = 8U;
    config.sync_interval = 1ms;
    return config;
}

[[nodiscard]] bool Capture(
    recovery::MdlLiveJournalV1& journal,
    const FakeMessage& message,
    std::uint64_t realtime_ns,
    std::uint64_t monotonic_ns,
    std::uint32_t maximum_message_bytes) {
    realtime::OwnedIngressMessageInspectionV1 inspection{};
    if (realtime::InspectOwnedIngressMessageV1(
            &message, maximum_message_bytes, &inspection) !=
            realtime::OwnedIngressMessageErrorV1::kNone ||
        !inspection) {
        return false;
    }
    realtime::RealtimeIngressCaptureInputV1 input{};
    input.inspection = &inspection;
    input.recv_realtime_ns = realtime_ns;
    input.recv_monotonic_ns = monotonic_ns;
    return journal.Capture(input);
}

[[nodiscard]] bool BodyEquals(
    const recovery::MdlLiveJournalRecordV1& record,
    std::span<const std::byte> expected) {
    if (record.wire_size() != sdk::kVendorHeadBytes + expected.size()) {
        return false;
    }
    const auto* body = reinterpret_cast<const std::byte*>(record.GetBody());
    return body != nullptr &&
           std::equal(expected.begin(), expected.end(), body);
}

void TestCommittedRoundTripAndRotation(TestContext* test) {
    ScopedDirectory directory("roundtrip");
    recovery::LiveJournalConfigV1 config = MakeConfig(directory.path());
    config.maximum_message_bytes = 700U * 1024U;
    config.maximum_total_bytes = 3ULL * 1024ULL * 1024ULL;

    std::shared_ptr<recovery::MdlLiveJournalV1> journal;
    int system_error = 0;
    test->Expect(
        recovery::MdlLiveJournalV1::Create(
            config, &journal, &system_error) ==
                recovery::LiveJournalErrorV1::kNone &&
            journal != nullptr && system_error == 0,
        "create an empty absolute journal directory");
    if (journal == nullptr) {
        return;
    }

    std::unique_ptr<recovery::MdlLiveJournalReaderV1> reader;
    test->Expect(
        journal->CreateReader(&reader) && reader != nullptr,
        "create a committed-frontier reader");
    if (reader != nullptr) {
        const auto empty = reader->ReadNext(
            std::chrono::steady_clock::now());
        test->Expect(
            empty.disposition ==
                    recovery::LiveJournalReadDispositionV1::kTimeout &&
                empty.error == recovery::LiveJournalErrorV1::kNone,
            "reader does not invent a record before durable commit");
    }

    const auto transaction_body =
        ShenzhenTransactionBody(12U, 987'654U);
    const FakeMessage transaction(
        sdk::MessageKey{6U, 101U, 36U},
        100U,
        transaction_body);
    test->Expect(
        Capture(*journal, transaction, 1'000U, 2'000U,
                config.maximum_message_bytes),
        "capture a native-sequenced transaction");

    std::vector<std::byte> first_large(600U * 1024U, std::byte{0x31U});
    std::vector<std::byte> second_large(600U * 1024U, std::byte{0x52U});
    const FakeMessage large_one(
        sdk::MessageKey{4U, 101U, 4U}, 101U, first_large);
    const FakeMessage large_two(
        sdk::MessageKey{4U, 101U, 4U}, 102U, second_large);
    test->Expect(
        Capture(*journal, large_one, 3'000U, 4'000U,
                config.maximum_message_bytes) &&
            Capture(*journal, large_two, 5'000U, 6'000U,
                    config.maximum_message_bytes),
        "capture two records that require segment rotation");

    recovery::LiveJournalTupleFenceV1 transaction_fence{};
    recovery::LiveJournalTupleFenceV1 snapshot_fence{};
    test->Expect(
        journal->CaptureTupleFence(
            sdk::MessageKey{6U, 101U, 36U}, &transaction_fence) &&
            journal->CaptureTupleFence(
                sdk::MessageKey{4U, 101U, 4U}, &snapshot_fence) &&
            transaction_fence.accepted_global_serial == 3U &&
            transaction_fence.accepted_tuple_serial == 1U &&
            snapshot_fence.accepted_global_serial == 3U &&
            snapshot_fence.accepted_tuple_serial == 2U,
        "tuple fences share the global serial but retain tuple-local order");

    test->Expect(
        journal->StopAndFlush(),
        "stop flushes every accepted record");
    const recovery::LiveJournalSnapshotV1 stopped = journal->Snapshot();
    test->Expect(
        stopped.state == recovery::LiveJournalStateV1::kStopped &&
            stopped.error == recovery::LiveJournalErrorV1::kNone &&
            stopped.accepted_serial == 3U &&
            stopped.committed_serial == stopped.accepted_serial &&
            stopped.segment_count == 2U &&
            stopped.accepted_tuple_serials[0U] == 2U &&
            stopped.accepted_tuple_serials[4U] == 1U &&
            stopped.committed_bytes == stopped.reserved_bytes,
        "durable frontier, tuple serials, and rotated byte accounting close");
    test->Expect(
        std::filesystem::is_regular_file(
            directory.path() / "segment-0000000001.wal") &&
            std::filesystem::is_regular_file(
                directory.path() / "segment-0000000002.wal"),
        "rotation creates deterministic append-only segment names");

    if (reader == nullptr) {
        return;
    }
    auto first = reader->ReadNext(
        std::chrono::steady_clock::now() + 2s);
    test->Expect(
        first.has_record() && first.record->global_serial() == 1U &&
            first.record->tuple_serial() == 1U &&
            first.record->recv_realtime_ns() == 1'000U &&
            first.record->recv_monotonic_ns() == 2'000U &&
            first.record->key() == sdk::MessageKey{6U, 101U, 36U} &&
            first.record->GetHead()->SequenceID == 100U &&
            first.record->native_sequence_valid() &&
            first.record->native_market() == static_cast<std::uint8_t>(
                realtime::NativeSequenceMarketV1::kShenzhen) &&
            first.record->native_channel() == 12U &&
            first.record->native_sequence() == 987'654U &&
            BodyEquals(*first.record, transaction.body()),
        "reader reconstructs exact wire bytes, clocks, and native descriptor");

    auto second = reader->ReadNext(
        std::chrono::steady_clock::now() + 2s);
    auto third = reader->ReadNext(
        std::chrono::steady_clock::now() + 2s);
    test->Expect(
        second.has_record() && third.has_record() &&
            second.record->global_serial() == 2U &&
            second.record->tuple_serial() == 1U &&
            !second.record->native_sequence_valid() &&
            third.record->global_serial() == 3U &&
            third.record->tuple_serial() == 2U &&
            !third.record->native_sequence_valid() &&
            BodyEquals(*second.record, large_one.body()) &&
            BodyEquals(*third.record, large_two.body()),
        "reader crosses a segment boundary without changing global order");
    const auto end = reader->ReadNext(
        std::chrono::steady_clock::now() + 2s);
    test->Expect(
        end.disposition ==
                recovery::LiveJournalReadDispositionV1::kEnd &&
            end.error == recovery::LiveJournalErrorV1::kNone &&
            reader->next_serial() == 4U,
        "stopped journal exposes an exact terminal frontier");
}

void TestCapacityAndDirectoryFailures(TestContext* test) {
    {
        ScopedDirectory directory("capture-limit");
        recovery::LiveJournalConfigV1 config = MakeConfig(directory.path());
        constexpr std::size_t kJournalBodyBytes = 32U;
        constexpr std::size_t kActualBodyBytes = 64U;
        config.maximum_message_bytes = static_cast<std::uint32_t>(
            sdk::kVendorHeadBytes + kJournalBodyBytes);
        std::shared_ptr<recovery::MdlLiveJournalV1> journal;
        test->Expect(
            recovery::MdlLiveJournalV1::Create(config, &journal) ==
                    recovery::LiveJournalErrorV1::kNone &&
                journal != nullptr,
            "create a journal with a smaller capture limit");
        if (journal != nullptr) {
            const FakeMessage oversized(
                sdk::MessageKey{4U, 101U, 4U},
                199U,
                std::vector<std::byte>(
                    kActualBodyBytes, std::byte{0x33U}));
            test->Expect(
                !Capture(
                    *journal,
                    oversized,
                    9U,
                    19U,
                    static_cast<std::uint32_t>(
                        sdk::kVendorHeadBytes + kActualBodyBytes)),
                "capture revalidates the journal's own message limit");
            const auto failed = journal->Snapshot();
            test->Expect(
                failed.state == recovery::LiveJournalStateV1::kFailed &&
                    failed.error ==
                        recovery::LiveJournalErrorV1::kInvalidCapture &&
                    failed.accepted_serial == 0U &&
                    !journal->StopAndFlush(),
                "oversized capture fails before serial or capacity commit");
        }
    }

    {
        ScopedDirectory directory("capacity");
        recovery::LiveJournalConfigV1 config = MakeConfig(directory.path());
        constexpr std::size_t kBodyBytes = 64U;
        config.maximum_message_bytes = static_cast<std::uint32_t>(
            sdk::kVendorHeadBytes + kBodyBytes);
        config.maximum_total_bytes =
            kSegmentHeaderBytes + kRecordHeaderBytes + kBodyBytes;
        std::shared_ptr<recovery::MdlLiveJournalV1> journal;
        test->Expect(
            recovery::MdlLiveJournalV1::Create(config, &journal) ==
                    recovery::LiveJournalErrorV1::kNone &&
                journal != nullptr,
            "create an exactly one-record logical journal capacity");
        if (journal != nullptr) {
            const FakeMessage message(
                sdk::MessageKey{4U, 101U, 4U},
                200U,
                std::vector<std::byte>(kBodyBytes, std::byte{0x44U}));
            test->Expect(
                Capture(*journal, message, 10U, 20U,
                        config.maximum_message_bytes),
                "first record reserves the exact remaining logical capacity");
            test->Expect(
                !Capture(*journal, message, 11U, 21U,
                         config.maximum_message_bytes),
                "second record fails closed instead of exceeding capacity");
            const auto failed = journal->Snapshot();
            test->Expect(
                failed.state == recovery::LiveJournalStateV1::kFailed &&
                    failed.error ==
                        recovery::LiveJournalErrorV1::kCapacityExhausted &&
                    failed.accepted_serial == 1U &&
                    failed.reserved_bytes == config.maximum_total_bytes &&
                    !journal->StopAndFlush(),
                "capacity exhaustion preserves the last accepted reservation and is terminal");
        }
    }

    {
        ScopedDirectory directory("nonempty");
        std::error_code error;
        test->Expect(
            std::filesystem::create_directory(directory.path(), error) &&
                !error,
            "prepare a nonempty journal directory");
        const std::filesystem::path marker = directory.path() / "owned";
        const int marker_fd = ::open(
            marker.c_str(),
            O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC,
            S_IRUSR | S_IWUSR);
        if (marker_fd >= 0) {
            static_cast<void>(::close(marker_fd));
        }
        std::shared_ptr<recovery::MdlLiveJournalV1> journal;
        const auto create_error = recovery::MdlLiveJournalV1::Create(
            MakeConfig(directory.path(), 0x41U), &journal);
        test->Expect(
            marker_fd >= 0 &&
                create_error ==
                    recovery::LiveJournalErrorV1::kDirectoryNotEmpty &&
                journal == nullptr,
            "journal refuses to adopt or truncate a nonempty directory");
    }
}

void TestCorruptionFailsWholeJournal(TestContext* test) {
    ScopedDirectory directory("corrupt");
    recovery::LiveJournalConfigV1 config = MakeConfig(directory.path());
    std::shared_ptr<recovery::MdlLiveJournalV1> journal;
    test->Expect(
        recovery::MdlLiveJournalV1::Create(config, &journal) ==
                recovery::LiveJournalErrorV1::kNone &&
            journal != nullptr,
        "create corruption fixture journal");
    if (journal == nullptr) {
        return;
    }
    const FakeMessage message(
        sdk::MessageKey{6U, 101U, 36U},
        300U,
        ShenzhenTransactionBody(7U, 8U));
    test->Expect(
        Capture(*journal, message, 100U, 200U,
                config.maximum_message_bytes) &&
            journal->StopAndFlush(),
        "commit corruption fixture before modifying its bytes");

    const std::filesystem::path segment =
        directory.path() / "segment-0000000001.wal";
    const int fd = ::open(segment.c_str(), O_RDWR | O_CLOEXEC | O_NOFOLLOW);
    std::byte changed{0x7fU};
    const ssize_t changed_bytes =
        fd < 0
            ? -1
            : ::pwrite(
                  fd,
                  &changed,
                  sizeof(changed),
                  static_cast<off_t>(
                      kSegmentHeaderBytes + kRecordHeaderBytes));
    if (fd >= 0) {
        static_cast<void>(::close(fd));
    }
    test->Expect(changed_bytes == 1, "modify one committed body byte");

    std::unique_ptr<recovery::MdlLiveJournalReaderV1> reader;
    test->Expect(
        journal->CreateReader(&reader) && reader != nullptr,
        "create reader for corrupted committed bytes");
    if (reader == nullptr) {
        return;
    }
    const auto read = reader->ReadNext(
        std::chrono::steady_clock::now() + 2s);
    const auto failed = journal->Snapshot();
    test->Expect(
        read.disposition ==
                recovery::LiveJournalReadDispositionV1::kFailed &&
            read.error == recovery::LiveJournalErrorV1::kCorruptRecord &&
            failed.state == recovery::LiveJournalStateV1::kFailed &&
            failed.error == recovery::LiveJournalErrorV1::kCorruptRecord &&
            !failed.healthy(),
        "record CRC/SHA mismatch fails the shared journal closed");
}

}  // namespace

int main() {
    TestContext test;
    TestCommittedRoundTripAndRotation(&test);
    TestCapacityAndDirectoryFailures(&test);
    TestCorruptionFailsWholeJournal(&test);
    if (test.failures() == 0) {
        std::cout << "live journal V1 tests passed\n";
        return 0;
    }
    return 1;
}
