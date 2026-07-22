#include "l2flow/control/control_record_posix_sink.h"
#include "l2flow/control/quality_flags_v1.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace control = l2flow::control;

namespace {

struct TestContext final {
    void Expect(bool condition, std::string_view message) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << message << '\n';
        }
    }
    int failures = 0;
};

class TemporaryDirectory final {
public:
    TemporaryDirectory() {
        std::array<char, 64U> pattern{};
        constexpr std::string_view value =
            "/tmp/l2flow-control-record-sink-XXXXXX";
        std::copy(value.begin(), value.end(), pattern.begin());
        char* const created = ::mkdtemp(pattern.data());
        if (created == nullptr) {
            throw std::runtime_error("mkdtemp failed");
        }
        path_ = created;
        if (::chmod(path_.c_str(), 0700U) != 0) {
            throw std::runtime_error("chmod failed");
        }
        descriptor_ = ::open(
            path_.c_str(),
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        if (descriptor_ < 0) {
            throw std::runtime_error("open directory failed");
        }
    }

    ~TemporaryDirectory() {
        if (descriptor_ >= 0) {
            static_cast<void>(::close(descriptor_));
        }
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] int descriptor() const noexcept {
        return descriptor_;
    }

private:
    std::string path_;
    int descriptor_ = -1;
};

template <std::size_t Size>
void Fill(std::array<std::byte, Size>* value, std::uint8_t seed) {
    for (std::size_t index = 0U; index < value->size(); ++index) {
        (*value)[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(seed + index));
    }
}

control::ControlRecordV1 MakeRecord(
    std::uint64_t ingress_sequence = 7U,
    std::uint64_t record_end_wal_pos = 8192U) {
    control::ControlRecordV1 record;
    record.flags =
        control::kControlRecordResponseManifestHashPresent |
        control::kControlRecordRequiredFailure |
        control::kControlRecordOptionalFailure |
        control::kControlRecordNoncanonicalEmptyOffset;
    record.control_type =
        control::ControlTypeV1::kSubscriptionRejected;
    record.source_stream_id = 1002U;
    record.capture_date = 20260722U;
    Fill(&record.stream_day_id, 0x10U);
    record.vendor_service_id = 2U;
    record.vendor_service_version = 101U;
    record.vendor_message_id = 23U;
    record.connection_epoch = 2U;
    record.subscription_epoch = 3U;
    record.origin_ingress_sequence = ingress_sequence;
    record.origin_record_end_wal_pos = record_end_wal_pos;
    record.quality_flags =
        control::QualityBit(
            control::QualityFlagV1::kNoncanonicalEmptyOffset) |
        control::QualityBit(control::QualityFlagV1::kSchemaUnknown) |
        control::QualityBit(control::QualityFlagV1::kSessionUnknown);
    record.required_count = 3U;
    record.required_ok_count = 1U;
    record.required_failed_count = 2U;
    record.optional_count = 2U;
    record.optional_ok_count = 1U;
    record.optional_failed_count = 1U;
    record.response_entry_count = 5U;
    Fill(&record.response_manifest_sha256, 0x30U);
    Fill(&record.control_state_sha256, 0x90U);
    return record;
}

control::ControlRecordWireV1 Encode(
    const control::ControlRecordV1& record) {
    control::ControlRecordWireV1 wire{};
    if (control::EncodeControlRecordV1(record, &wire) !=
        control::ControlRecordV1Error::kNone) {
        throw std::runtime_error("record fixture did not encode");
    }
    return wire;
}

struct Clock final {
    static std::uint64_t Now(void* context) noexcept {
        return static_cast<Clock*>(context)->now;
    }
    std::uint64_t now = 100U;
};

struct FinalReadbackDeadlineClock final {
    static std::uint64_t Now(void* context) noexcept {
        auto* const clock =
            static_cast<FinalReadbackDeadlineClock*>(context);
        ++clock->calls;
        // A new publication performs seven deadline checks before its final
        // canonical-file readback. Expire exactly at the acknowledgement
        // boundary so durable bytes may exist, but success cannot be returned
        // after the caller's absolute deadline.
        return clock->calls <= 7U ? 100U : 101U;
    }
    std::uint64_t calls = 0U;
};

[[nodiscard]] bool WriteAll(
    int descriptor,
    const control::ControlRecordWireV1& wire,
    std::size_t limit) {
    std::size_t completed = 0U;
    while (completed < limit) {
        const ssize_t result = ::write(
            descriptor, wire.data() + completed, limit - completed);
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            return false;
        }
        completed += static_cast<std::size_t>(result);
    }
    return true;
}

[[nodiscard]] bool CreateTemporary(
    int directory_fd,
    const control::ControlRecordV1& record,
    const control::ControlRecordWireV1& wire,
    std::size_t size) {
    std::string final_name;
    if (!control::ControlRecordV1Filename(record, &final_name)) {
        return false;
    }
    const std::string temporary = "." + final_name + ".tmp";
    const int descriptor = ::openat(
        directory_fd,
        temporary.c_str(),
        O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
        0600U);
    if (descriptor < 0) {
        return false;
    }
    const bool result = ::fchmod(descriptor, 0600U) == 0 &&
        WriteAll(descriptor, wire, size) && ::fsync(descriptor) == 0;
    static_cast<void>(::close(descriptor));
    return result;
}

void TestDurableIdempotencyAndWriterLock(TestContext* test) {
    TemporaryDirectory directory;
    Clock clock;
    control::ControlRecordPosixSinkOptionsV1 options;
    options.monotonic_now = &Clock::Now;
    options.monotonic_clock_context = &clock;
    std::unique_ptr<control::ControlRecordPosixSinkV1> sink;
    std::string diagnostic;
    const auto created = control::CreateControlRecordPosixSinkV1At(
        directory.descriptor(), options, &sink, &diagnostic);
    std::unique_ptr<control::ControlRecordPosixSinkV1> competing;
    const auto busy = control::CreateControlRecordPosixSinkV1At(
        directory.descriptor(), options, &competing, &diagnostic);
    control::ControlRecordV1 record = MakeRecord();
    const control::ControlRecordWireV1 wire = Encode(record);
    const auto published = sink == nullptr
        ? control::ControlRecordPublishResultV1::kFailure
        : sink->Publish(record, wire, 100U);
    const auto identical = sink == nullptr
        ? control::ControlRecordPublishResultV1::kFailure
        : sink->Publish(record, wire, 100U);
    control::ControlRecordV1 conflict_record = record;
    ++conflict_record.connection_epoch;
    const control::ControlRecordWireV1 conflict_wire =
        Encode(conflict_record);
    const auto conflict = sink == nullptr
        ? control::ControlRecordPublishResultV1::kFailure
        : sink->Publish(conflict_record, conflict_wire, 100U);
    std::string final_name;
    const bool named = control::ControlRecordV1Filename(
        record, &final_name);
    struct stat status {};
    const bool safe_final = named &&
        ::fstatat(
            directory.descriptor(),
            final_name.c_str(),
            &status,
            AT_SYMLINK_NOFOLLOW) == 0 &&
        S_ISREG(status.st_mode) &&
        (status.st_mode & static_cast<mode_t>(07777U)) ==
            static_cast<mode_t>(0600U) &&
        status.st_nlink == static_cast<nlink_t>(1U) &&
        status.st_size ==
            static_cast<off_t>(control::kControlRecordV1Bytes);
    test->Expect(
        created == control::ControlRecordPosixSinkCreateErrorV1::kNone &&
            sink != nullptr &&
            busy ==
                control::ControlRecordPosixSinkCreateErrorV1::
                    kDirectoryBusy &&
            competing == nullptr &&
            published ==
                control::ControlRecordPublishResultV1::kPublishedNew &&
            identical ==
                control::ControlRecordPublishResultV1::
                    kAcceptedIdentical &&
            conflict ==
                control::ControlRecordPublishResultV1::kConflict &&
            safe_final,
        "derived sink holds one writer lock, publishes fsync-backed 0600 files, and enforces exact key idempotency");

    sink.reset();
    const auto reopened = control::CreateControlRecordPosixSinkV1At(
        directory.descriptor(), options, &sink, &diagnostic);
    const auto replayed = sink == nullptr
        ? control::ControlRecordPublishResultV1::kFailure
        : sink->Publish(record, wire, 100U);
    test->Expect(
        reopened == control::ControlRecordPosixSinkCreateErrorV1::kNone &&
            replayed ==
                control::ControlRecordPublishResultV1::
                    kAcceptedIdentical,
        "restart reopens the directory and accepts the same retained canonical wire without duplication");
}

void TestCrashTemporaryRecoveryAndDeadline(TestContext* test) {
    TemporaryDirectory complete_directory;
    control::ControlRecordV1 complete_record = MakeRecord(8U, 8320U);
    const control::ControlRecordWireV1 complete_wire =
        Encode(complete_record);
    const bool complete_written = CreateTemporary(
        complete_directory.descriptor(),
        complete_record,
        complete_wire,
        complete_wire.size());
    Clock clock;
    control::ControlRecordPosixSinkOptionsV1 options{
        &Clock::Now, &clock};
    std::unique_ptr<control::ControlRecordPosixSinkV1> complete_sink;
    const auto complete_created =
        control::CreateControlRecordPosixSinkV1At(
            complete_directory.descriptor(),
            options,
            &complete_sink,
            nullptr);
    const auto adopted = complete_sink == nullptr
        ? control::ControlRecordPublishResultV1::kFailure
        : complete_sink->Publish(complete_record, complete_wire, 100U);
    test->Expect(
        complete_written &&
            complete_created ==
                control::ControlRecordPosixSinkCreateErrorV1::kNone &&
            adopted ==
                control::ControlRecordPublishResultV1::kPublishedNew,
        "a complete crash temporary is revalidated, fsynced, no-replace renamed, and directory-fsynced");

    TemporaryDirectory partial_directory;
    control::ControlRecordV1 partial_record = MakeRecord(9U, 8448U);
    const control::ControlRecordWireV1 partial_wire =
        Encode(partial_record);
    const bool partial_written = CreateTemporary(
        partial_directory.descriptor(),
        partial_record,
        partial_wire,
        partial_wire.size() / 2U);
    std::unique_ptr<control::ControlRecordPosixSinkV1> partial_sink;
    const auto partial_created =
        control::CreateControlRecordPosixSinkV1At(
            partial_directory.descriptor(),
            options,
            &partial_sink,
            nullptr);
    const auto partial_result = partial_sink == nullptr
        ? control::ControlRecordPublishResultV1::kPublishedNew
        : partial_sink->Publish(partial_record, partial_wire, 100U);
    test->Expect(
        partial_written &&
            partial_created ==
                control::ControlRecordPosixSinkCreateErrorV1::kNone &&
            partial_result ==
                control::ControlRecordPublishResultV1::kFailure,
        "a partial deterministic crash temporary is retained and fails closed under the single-writer lock");

    TemporaryDirectory deadline_directory;
    std::unique_ptr<control::ControlRecordPosixSinkV1> deadline_sink;
    static_cast<void>(control::CreateControlRecordPosixSinkV1At(
        deadline_directory.descriptor(),
        options,
        &deadline_sink,
        nullptr));
    control::ControlRecordV1 deadline_record = MakeRecord(10U, 8576U);
    const auto deadline_result = deadline_sink == nullptr
        ? control::ControlRecordPublishResultV1::kPublishedNew
        : deadline_sink->Publish(
              deadline_record, Encode(deadline_record), 99U);
    test->Expect(
        deadline_result ==
            control::ControlRecordPublishResultV1::kFailure,
        "an already-expired absolute monotonic deadline performs no acknowledged publication");

    TemporaryDirectory final_readback_directory;
    FinalReadbackDeadlineClock final_readback_clock;
    control::ControlRecordPosixSinkOptionsV1 final_readback_options{
        &FinalReadbackDeadlineClock::Now, &final_readback_clock};
    std::unique_ptr<control::ControlRecordPosixSinkV1>
        final_readback_sink;
    const auto final_readback_created =
        control::CreateControlRecordPosixSinkV1At(
            final_readback_directory.descriptor(),
            final_readback_options,
            &final_readback_sink,
            nullptr);
    control::ControlRecordV1 final_readback_record =
        MakeRecord(11U, 8704U);
    const control::ControlRecordWireV1 final_readback_wire =
        Encode(final_readback_record);
    const auto expired_at_ack = final_readback_sink == nullptr
        ? control::ControlRecordPublishResultV1::kPublishedNew
        : final_readback_sink->Publish(
              final_readback_record, final_readback_wire, 100U);
    final_readback_clock.calls = 0U;
    const auto retry = final_readback_sink == nullptr
        ? control::ControlRecordPublishResultV1::kFailure
        : final_readback_sink->Publish(
              final_readback_record, final_readback_wire, 100U);
    test->Expect(
        final_readback_created ==
                control::ControlRecordPosixSinkCreateErrorV1::kNone &&
            expired_at_ack ==
                control::ControlRecordPublishResultV1::kFailure &&
            retry ==
                control::ControlRecordPublishResultV1::kAcceptedIdentical,
        "a deadline that expires after durable final readback cannot be acknowledged, while an idempotent retry observes the retained canonical record");
}

}  // namespace

int main() {
    TestContext test;
    TestDurableIdempotencyAndWriterLock(&test);
    TestCrashTemporaryRecoveryAndDeadline(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " ControlRecord POSIX sink test(s) failed\n";
        return 1;
    }
    std::cout << "Phase3 ControlRecord POSIX sink tests passed\n";
    return 0;
}
