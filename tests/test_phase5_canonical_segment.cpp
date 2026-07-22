#include "l2flow/canonical/canonical_segment_v1.h"

#include "l2flow/common/sha256.h"

#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace {

using l2flow::canonical::CanonicalEventTypeV1;
using l2flow::canonical::CanonicalMarketV1;
using l2flow::canonical::CanonicalSegmentControlSnapshotV1;
using l2flow::canonical::CanonicalSegmentCreateOptionsV1;
using l2flow::canonical::CanonicalSegmentDescriptorV1;
using l2flow::canonical::CanonicalSegmentErrorV1;
using l2flow::canonical::CanonicalSegmentManifestViewV1;
using l2flow::canonical::CanonicalSegmentReaderV1;
using l2flow::canonical::CanonicalSegmentRecoveryDispositionV1;
using l2flow::canonical::CanonicalSegmentSealOptionsV1;
using l2flow::canonical::CanonicalSegmentSealResultV1;
using l2flow::canonical::CanonicalSegmentWriterV1;
using l2flow::canonical::CanonicalTickActionV1;
using l2flow::canonical::CanonicalTickRecordV1;

[[nodiscard]] bool Check(
    bool condition,
    const char* expression,
    int line) {
    if (!condition) {
        std::cerr << "check failed at line " << line << ": "
                  << expression << '\n';
    }
    return condition;
}

#define CHECK(value)                                      \
    do {                                                  \
        if (!Check((value), #value, __LINE__)) {          \
            return false;                                 \
        }                                                 \
    } while (false)

template <std::size_t Size>
[[nodiscard]] std::array<std::byte, Size> Pattern(
    std::uint8_t seed) {
    std::array<std::byte, Size> value{};
    for (std::size_t index = 0U; index < Size; ++index) {
        value[index] = static_cast<std::byte>(
            static_cast<unsigned int>(seed) +
            static_cast<unsigned int>(index));
    }
    return value;
}

class TemporaryDirectory final {
public:
    TemporaryDirectory() {
        std::array<char, 64U> pattern{};
        constexpr char prefix[] = "/tmp/l2flow-phase5-segment-XXXXXX";
        std::copy(std::begin(prefix), std::end(prefix), pattern.begin());
        char* const created = ::mkdtemp(pattern.data());
        if (created != nullptr) {
            path_ = created;
        }
    }

    ~TemporaryDirectory() {
        if (!path_.empty()) {
            std::error_code ignored;
            static_cast<void>(std::filesystem::remove_all(path_, ignored));
        }
    }

    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_{};
};

[[nodiscard]] CanonicalSegmentDescriptorV1 MakeDescriptor(
    std::uint64_t generation,
    std::uint64_t segment_sequence,
    std::uint64_t capacity) {
    CanonicalSegmentDescriptorV1 descriptor{};
    descriptor.event_type = CanonicalEventTypeV1::kTick;
    descriptor.record_size = static_cast<std::uint32_t>(
        l2flow::canonical::kCanonicalTickRecordBytesV1);
    descriptor.source_stream_id = 1002U;
    descriptor.shard = 3U;
    descriptor.trade_date = 20260722U;
    descriptor.origin_capture_date = 20260722U;
    descriptor.origin_stream_day_id = Pattern<16U>(0x10U);
    descriptor.origin_source_writer_instance = Pattern<16U>(0x28U);
    descriptor.origin_source_generation = 7U;
    descriptor.clock_epoch.algorithm = 1U;
    descriptor.clock_epoch.digest = Pattern<32U>(0x20U);
    descriptor.clock_epoch.label = 0x1122334455667788ULL;
    descriptor.schema_sha256 =
        l2flow::canonical::CanonicalSchemaDescriptorSha256V1();
    descriptor.dtype_sha256 =
        l2flow::canonical::CanonicalDtypeDescriptorSha256V1();
    descriptor.registry_version = 17U;
    descriptor.registry_sha256 = Pattern<32U>(0x40U);
    descriptor.normalizer_build_sha256 = Pattern<32U>(0x60U);
    descriptor.normalizer_config_sha256 = Pattern<32U>(0x80U);
    descriptor.generation = generation;
    descriptor.segment_sequence = segment_sequence;
    descriptor.capacity_records = capacity;
    return descriptor;
}

[[nodiscard]] CanonicalSegmentCreateOptionsV1 MakeOptions(
    const std::filesystem::path& directory,
    std::string stem,
    const CanonicalSegmentDescriptorV1& descriptor,
    std::int64_t created_time) {
    CanonicalSegmentCreateOptionsV1 options{};
    options.segment_path = directory / (stem + ".clog");
    options.manifest_path = directory / (stem + ".manifest");
    options.descriptor = descriptor;
    options.created_realtime_ns = created_time;
    options.created_monotonic_ns = created_time / 2;
    return options;
}

[[nodiscard]] CanonicalTickRecordV1 MakeTick(
    const CanonicalSegmentDescriptorV1& descriptor,
    std::uint64_t event_id,
    std::uint64_t ingress_sequence,
    std::uint64_t wal_end_pos) {
    CanonicalTickRecordV1 record{};
    record.header.event_type = CanonicalEventTypeV1::kTick;
    record.header.record_size = descriptor.record_size;
    record.header.source_stream_id = descriptor.source_stream_id;
    record.header.connection_epoch = 9U;
    record.header.trade_date = descriptor.trade_date;
    record.header.shard_event_id = event_id;
    record.header.origin_ingress_sequence = ingress_sequence;
    record.header.origin_wal_end_pos = wal_end_pos;
    record.header.vendor_sequence_id = ingress_sequence + 1000U;
    record.header.exchange_sequence = ingress_sequence + 2000U;
    record.header.recv_realtime_ns =
        static_cast<std::int64_t>(ingress_sequence * 1000U);
    record.header.recv_monotonic_ns =
        static_cast<std::int64_t>(ingress_sequence * 900U);
    record.header.instrument_id = 19U;
    record.header.channel = 7U;
    record.header.market = CanonicalMarketV1::kShanghai;
    record.header.origin_service_version = 101U;
    record.header.origin_message_id = 24U;
    record.header.origin_service_id = 4U;
    record.payload.action = CanonicalTickActionV1::kStatus;
    record.payload.source_enum_bits = static_cast<std::uint64_t>('S');
    return record;
}

[[nodiscard]] std::span<const std::byte> BytesOf(
    const CanonicalTickRecordV1& record) {
    return std::as_bytes(std::span(&record, std::size_t{1U}));
}

[[nodiscard]] bool PwriteExact(
    const std::filesystem::path& path,
    std::span<const std::byte> bytes,
    std::uint64_t offset) {
    const int descriptor = ::open(
        path.c_str(), O_RDWR | O_CLOEXEC | O_NOFOLLOW);
    if (descriptor < 0) {
        return false;
    }
    std::size_t written = 0U;
    while (written < bytes.size()) {
        const ssize_t count = ::pwrite(
            descriptor,
            bytes.data() + written,
            bytes.size() - written,
            static_cast<off_t>(offset + written));
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            static_cast<void>(::close(descriptor));
            return false;
        }
        written += static_cast<std::size_t>(count);
    }
    const bool synced = ::fsync(descriptor) == 0;
    const bool closed = ::close(descriptor) == 0;
    return synced && closed;
}

[[nodiscard]] bool CheckCreationPublicationSeal(
    const std::filesystem::path& directory) {
    const CanonicalSegmentDescriptorV1 descriptor =
        MakeDescriptor(1U, 1U, 4U);
    const CanonicalSegmentCreateOptionsV1 options = MakeOptions(
        directory, "primary", descriptor, 2000000);
    std::unique_ptr<CanonicalSegmentWriterV1> writer;
    CHECK(CanonicalSegmentWriterV1::Create(options, &writer) ==
          CanonicalSegmentErrorV1::kNone);
    CHECK(writer != nullptr);

    struct stat status {};
    CHECK(::stat(options.segment_path.c_str(), &status) == 0);
    const std::uint64_t expected_size =
        l2flow::canonical::kCanonicalSegmentDataOffsetV1 +
        descriptor.capacity_records * descriptor.record_size;
    CHECK(static_cast<std::uint64_t>(status.st_size) == expected_size);
    CHECK((status.st_mode & 07777) == 0600);

    std::unique_ptr<CanonicalSegmentWriterV1> duplicate;
    CHECK(CanonicalSegmentWriterV1::Create(options, &duplicate) ==
          CanonicalSegmentErrorV1::kAlreadyExists);
    CHECK(duplicate == nullptr);
    const int competing = ::open(
        options.segment_path.c_str(), O_RDWR | O_CLOEXEC | O_NOFOLLOW);
    CHECK(competing >= 0);
    errno = 0;
    CHECK(::flock(competing, LOCK_EX | LOCK_NB) != 0);
    CHECK(errno == EWOULDBLOCK || errno == EAGAIN);
    CHECK(::close(competing) == 0);

    std::unique_ptr<CanonicalSegmentReaderV1> live_reader;
    CHECK(CanonicalSegmentReaderV1::Open(
              options.segment_path, descriptor, &live_reader) ==
          CanonicalSegmentErrorV1::kNone);
    CanonicalSegmentControlSnapshotV1 control{};
    CHECK(live_reader->ReadControl(&control) ==
          CanonicalSegmentErrorV1::kNone);
    CHECK(control.published_records == 0U);
    std::span<const std::byte> exposed;
    CHECK(live_reader->PublishedRecord(0U, &exposed) ==
          CanonicalSegmentErrorV1::kRecordNotPublished);

    const std::array<std::byte, 32U> unpublished = Pattern<32U>(0xa0U);
    CHECK(PwriteExact(
        options.segment_path,
        unpublished,
        l2flow::canonical::kCanonicalSegmentDataOffsetV1));
    CHECK(live_reader->PublishedRecord(0U, &exposed) ==
          CanonicalSegmentErrorV1::kRecordNotPublished);

    const CanonicalTickRecordV1 first = MakeTick(
        descriptor, 50U, 100U, 10000U);
    CHECK(writer->PublishRecord(BytesOf(first)) ==
          CanonicalSegmentErrorV1::kNone);
    const CanonicalTickRecordV1 conflicting_origin = MakeTick(
        descriptor, 51U, 100U, 10050U);
    CHECK(writer->PublishRecord(BytesOf(conflicting_origin)) ==
          CanonicalSegmentErrorV1::kNonMonotonicCursor);
    CHECK(writer->AdvanceProcessedRaw(100U, 10000U) ==
          CanonicalSegmentErrorV1::kNone);
    CHECK(live_reader->ReadControl(&control) ==
          CanonicalSegmentErrorV1::kNone);
    CHECK(control.published_records == 1U);
    CHECK(control.last_shard_event_id == 50U);
    CHECK(live_reader->PublishedRecord(0U, &exposed) ==
          CanonicalSegmentErrorV1::kNone);
    CHECK(exposed.size() == sizeof(first));
    CHECK(std::memcmp(exposed.data(), &first, sizeof(first)) == 0);
    CHECK(live_reader->PublishedRecord(1U, &exposed) ==
          CanonicalSegmentErrorV1::kRecordNotPublished);

    CHECK(writer->AdvanceProcessedRaw(101U, 10100U) ==
          CanonicalSegmentErrorV1::kNone);
    CHECK(live_reader->ReadControl(&control) ==
          CanonicalSegmentErrorV1::kNone);
    CHECK(control.published_records == 1U);
    CHECK(control.processed_raw_ingress_sequence == 101U);
    CHECK(writer->AdvanceProcessedRaw(101U, 10150U) ==
          CanonicalSegmentErrorV1::kNone);

    const CanonicalTickRecordV1 second = MakeTick(
        descriptor, 51U, 102U, 10200U);
    CHECK(writer->PublishRecord(BytesOf(second)) ==
          CanonicalSegmentErrorV1::kNone);
    CHECK(writer->AdvanceProcessedRaw(102U, 10200U) ==
          CanonicalSegmentErrorV1::kNone);
    CHECK(live_reader->ReadControl(&control) ==
          CanonicalSegmentErrorV1::kNone);
    const std::uint64_t observed_notify = control.notify_epoch;
    std::atomic<bool> waiter_started{false};
    std::atomic<bool> waiter_ok{false};
    std::thread waiter([&] {
        waiter_started.store(true, std::memory_order_release);
        waiter_ok.store(
            live_reader->WaitForChange(observed_notify, 2000U) ==
                CanonicalSegmentErrorV1::kNone,
            std::memory_order_release);
    });
    while (!waiter_started.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    const CanonicalTickRecordV1 third = MakeTick(
        descriptor, 52U, 103U, 10300U);
    const CanonicalSegmentErrorV1 third_publish =
        writer->PublishRecord(BytesOf(third));
    const CanonicalSegmentErrorV1 third_progress =
        writer->AdvanceProcessedRaw(103U, 10300U);
    waiter.join();
    CHECK(third_publish == CanonicalSegmentErrorV1::kNone);
    CHECK(third_progress == CanonicalSegmentErrorV1::kNone);
    CHECK(waiter_ok.load(std::memory_order_acquire));

    CanonicalSegmentSealResultV1 seal{};
    CHECK(writer->Seal(CanonicalSegmentSealOptionsV1{
                           .closed_realtime_ns = 3000000,
                           .closed_monotonic_ns = 2000000},
                       &seal) == CanonicalSegmentErrorV1::kNone);
    CHECK(seal.published_records == 3U);
    CHECK(seal.record_stream_sha256 != seal.segment_integrity_sha256);
    CHECK(writer->header().sealed);
    CHECK(writer->PublishRecord(BytesOf(third)) ==
          CanonicalSegmentErrorV1::kSegmentSealed);
    CHECK(live_reader->ReadControl(&control) ==
          CanonicalSegmentErrorV1::kNone);
    CHECK(control.closed);
    CHECK(std::filesystem::file_size(options.manifest_path) ==
          l2flow::canonical::kCanonicalSegmentManifestBytesV1);
    CHECK(!std::filesystem::exists(
        std::filesystem::path(options.manifest_path.string() + ".tmp")));

    CanonicalSegmentManifestViewV1 manifest{};
    CHECK(l2flow::canonical::ReadCanonicalSegmentManifestV1(
              options.manifest_path, writer->header(), &manifest) ==
          CanonicalSegmentErrorV1::kNone);
    CHECK(manifest.segment_header_sha256 == seal.segment_header_sha256);
    CHECK(manifest.manifest_sha256 == seal.manifest_sha256);
    auto mismatched_manifest_header = writer->header();
    mismatched_manifest_header.descriptor.registry_sha256[0U] ^=
        std::byte{0x01U};
    CHECK(l2flow::canonical::ReadCanonicalSegmentManifestV1(
              options.manifest_path,
              mismatched_manifest_header,
              &manifest) == CanonicalSegmentErrorV1::kHashMismatch);
    CanonicalSegmentSealResultV1 repeated_seal{};
    CHECK(writer->Seal(CanonicalSegmentSealOptionsV1{
                           .closed_realtime_ns = 3000000,
                           .closed_monotonic_ns = 2000000},
                       &repeated_seal) ==
          CanonicalSegmentErrorV1::kNone);
    CHECK(repeated_seal.manifest_sha256 == seal.manifest_sha256);

    std::unique_ptr<CanonicalSegmentReaderV1> sealed_reader;
    CHECK(CanonicalSegmentReaderV1::Open(
              options.segment_path, descriptor, &sealed_reader) ==
          CanonicalSegmentErrorV1::kNone);
    CHECK(sealed_reader->header().sealed);
    CHECK(sealed_reader->header().record_stream_sha256 ==
          seal.record_stream_sha256);
    CHECK(sealed_reader->header().segment_integrity_sha256 ==
          seal.segment_integrity_sha256);

    CanonicalSegmentDescriptorV1 display_label_only = descriptor;
    display_label_only.clock_epoch.label ^= 0xffffU;
    std::unique_ptr<CanonicalSegmentReaderV1> label_reader;
    CHECK(CanonicalSegmentReaderV1::Open(
              options.segment_path,
              display_label_only,
              &label_reader) == CanonicalSegmentErrorV1::kNone);

    CanonicalSegmentDescriptorV1 wrong_clock = descriptor;
    wrong_clock.clock_epoch.digest[0U] ^= std::byte{0x01U};
    std::unique_ptr<CanonicalSegmentReaderV1> rejected;
    CHECK(CanonicalSegmentReaderV1::Open(
              options.segment_path, wrong_clock, &rejected) ==
          CanonicalSegmentErrorV1::kHeaderIdentityMismatch);
    CanonicalSegmentDescriptorV1 wrong_registry = descriptor;
    wrong_registry.registry_sha256[0U] ^= std::byte{0x01U};
    CHECK(CanonicalSegmentReaderV1::Open(
              options.segment_path, wrong_registry, &rejected) ==
          CanonicalSegmentErrorV1::kHeaderIdentityMismatch);

    CanonicalSegmentRecoveryDispositionV1 disposition{};
    l2flow::canonical::CanonicalSegmentHeaderViewV1 recovery_header{};
    CHECK(l2flow::canonical::InspectCanonicalSegmentForRecoveryV1(
              options.segment_path,
              descriptor,
              &disposition,
              &recovery_header) == CanonicalSegmentErrorV1::kNone);
    CHECK(disposition ==
          CanonicalSegmentRecoveryDispositionV1::
              kSealedHeaderAndHashesValid);

    writer.reset();
    live_reader.reset();
    sealed_reader.reset();
    label_reader.reset();
    std::byte corrupt = std::byte{0xeeU};
    CHECK(PwriteExact(
        options.segment_path,
        std::span<const std::byte>(&corrupt, std::size_t{1U}),
        l2flow::canonical::kCanonicalSegmentDataOffsetV1 + 3U));
    CHECK(CanonicalSegmentReaderV1::Open(
              options.segment_path, descriptor, &rejected) ==
          CanonicalSegmentErrorV1::kHashMismatch);
    return true;
}

[[nodiscard]] bool CheckRecoveryDoesNotResumeOpen(
    const std::filesystem::path& directory) {
    const CanonicalSegmentDescriptorV1 descriptor =
        MakeDescriptor(5U, 9U, 8U);
    const CanonicalSegmentCreateOptionsV1 options = MakeOptions(
        directory, "open-recovery", descriptor, 4000000);
    std::unique_ptr<CanonicalSegmentWriterV1> writer;
    CHECK(CanonicalSegmentWriterV1::Create(options, &writer) ==
          CanonicalSegmentErrorV1::kNone);
    CHECK(writer->AdvanceProcessedRaw(0U, 4096U) ==
          CanonicalSegmentErrorV1::kNone);
    const CanonicalTickRecordV1 record = MakeTick(
        descriptor, 700U, 900U, 90000U);
    CHECK(writer->PublishRecord(BytesOf(record)) ==
          CanonicalSegmentErrorV1::kNone);
    CHECK(writer->AdvanceProcessedRaw(900U, 90000U) ==
          CanonicalSegmentErrorV1::kNone);
    writer.reset();
    std::array<std::byte, sizeof(std::uint64_t)> odd_seqlock{};
    odd_seqlock[0U] = std::byte{0x01U};
    CHECK(PwriteExact(
        options.segment_path,
        odd_seqlock,
        l2flow::canonical::kCanonicalSegmentHeaderBytesV1));

    CanonicalSegmentRecoveryDispositionV1 disposition{};
    l2flow::canonical::CanonicalSegmentHeaderViewV1 header{};
    CHECK(l2flow::canonical::InspectCanonicalSegmentForRecoveryV1(
              options.segment_path,
              descriptor,
              &disposition,
              &header) == CanonicalSegmentErrorV1::kNone);
    CHECK(disposition == CanonicalSegmentRecoveryDispositionV1::
                             kUnsealedDiscardWholeGeneration);
    CHECK(!header.sealed);
    // The durable open header deliberately contains no resumable prefix even
    // though the volatile control page currently reports one record.
    CHECK(header.published_records == 0U);
    CHECK(std::filesystem::exists(options.segment_path));
    CHECK(!std::filesystem::exists(options.manifest_path));
    return true;
}

[[nodiscard]] bool CheckConcurrentReaderWriter(
    const std::filesystem::path& directory) {
    constexpr std::uint64_t count = 256U;
    const CanonicalSegmentDescriptorV1 descriptor =
        MakeDescriptor(11U, 12U, count);
    const CanonicalSegmentCreateOptionsV1 options = MakeOptions(
        directory, "concurrent", descriptor, 6000000);
    std::unique_ptr<CanonicalSegmentWriterV1> writer;
    CHECK(CanonicalSegmentWriterV1::Create(options, &writer) ==
          CanonicalSegmentErrorV1::kNone);
    std::unique_ptr<CanonicalSegmentReaderV1> reader;
    CHECK(CanonicalSegmentReaderV1::Open(
              options.segment_path, descriptor, &reader) ==
          CanonicalSegmentErrorV1::kNone);

    std::atomic<bool> writer_done{false};
    std::atomic<bool> reader_ok{true};
    std::thread observer([&] {
        std::uint64_t consumed = 0U;
        while (consumed < count) {
            CanonicalSegmentControlSnapshotV1 snapshot{};
            if (reader->ReadControl(&snapshot) !=
                    CanonicalSegmentErrorV1::kNone ||
                snapshot.published_records < consumed ||
                snapshot.published_records > count) {
                reader_ok.store(false, std::memory_order_release);
                return;
            }
            while (consumed < snapshot.published_records) {
                std::span<const std::byte> bytes;
                if (reader->PublishedRecord(consumed, &bytes) !=
                        CanonicalSegmentErrorV1::kNone ||
                    bytes.size() != sizeof(CanonicalTickRecordV1)) {
                    reader_ok.store(false, std::memory_order_release);
                    return;
                }
                CanonicalTickRecordV1 record{};
                std::memcpy(&record, bytes.data(), sizeof(record));
                if (record.header.shard_event_id != 1000U + consumed ||
                    record.header.origin_ingress_sequence !=
                        2000U + consumed) {
                    reader_ok.store(false, std::memory_order_release);
                    return;
                }
                ++consumed;
            }
            if (writer_done.load(std::memory_order_acquire) &&
                consumed < count) {
                CanonicalSegmentControlSnapshotV1 final_snapshot{};
                if (reader->ReadControl(&final_snapshot) !=
                        CanonicalSegmentErrorV1::kNone ||
                    final_snapshot.published_records == consumed) {
                    reader_ok.store(false, std::memory_order_release);
                    return;
                }
            }
            std::this_thread::yield();
        }
    });

    bool publishing_ok = true;
    for (std::uint64_t index = 0U; index < count; ++index) {
        const CanonicalTickRecordV1 record = MakeTick(
            descriptor,
            1000U + index,
            2000U + index,
            300000U + index * 100U);
        if (writer->PublishRecord(BytesOf(record)) !=
                CanonicalSegmentErrorV1::kNone ||
            writer->AdvanceProcessedRaw(
                2000U + index,
                300000U + index * 100U) !=
                CanonicalSegmentErrorV1::kNone) {
            publishing_ok = false;
            break;
        }
    }
    writer_done.store(true, std::memory_order_release);
    observer.join();
    CHECK(publishing_ok);
    CHECK(reader_ok.load(std::memory_order_acquire));
    CanonicalSegmentControlSnapshotV1 final{};
    CHECK(reader->ReadControl(&final) == CanonicalSegmentErrorV1::kNone);
    CHECK(final.published_records == count);
    return true;
}

[[nodiscard]] bool CheckRecordHashDeterminism(
    const std::filesystem::path& directory) {
    const CanonicalSegmentDescriptorV1 first_descriptor =
        MakeDescriptor(20U, 1U, 2U);
    CanonicalSegmentDescriptorV1 second_descriptor =
        MakeDescriptor(21U, 2U, 2U);
    const CanonicalSegmentCreateOptionsV1 first_options = MakeOptions(
        directory, "hash-a", first_descriptor, 7000000);
    const CanonicalSegmentCreateOptionsV1 second_options = MakeOptions(
        directory, "hash-b", second_descriptor, 9000000);
    std::unique_ptr<CanonicalSegmentWriterV1> first_writer;
    std::unique_ptr<CanonicalSegmentWriterV1> second_writer;
    CHECK(CanonicalSegmentWriterV1::Create(
              first_options, &first_writer) ==
          CanonicalSegmentErrorV1::kNone);
    CHECK(CanonicalSegmentWriterV1::Create(
              second_options, &second_writer) ==
          CanonicalSegmentErrorV1::kNone);
    for (std::uint64_t index = 0U; index < 2U; ++index) {
        const CanonicalTickRecordV1 record = MakeTick(
            first_descriptor,
            80U + index,
            90U + index,
            1000U + index * 100U);
        CHECK(first_writer->PublishRecord(BytesOf(record)) ==
              CanonicalSegmentErrorV1::kNone);
        CHECK(first_writer->AdvanceProcessedRaw(
                  90U + index,
                  1000U + index * 100U) ==
              CanonicalSegmentErrorV1::kNone);
        CHECK(second_writer->PublishRecord(BytesOf(record)) ==
              CanonicalSegmentErrorV1::kNone);
        CHECK(second_writer->AdvanceProcessedRaw(
                  90U + index,
                  1000U + index * 100U) ==
              CanonicalSegmentErrorV1::kNone);
    }
    CanonicalSegmentSealResultV1 first_seal{};
    CanonicalSegmentSealResultV1 second_seal{};
    CHECK(first_writer->Seal(
              {.closed_realtime_ns = 8000000,
               .closed_monotonic_ns = 4000000},
              &first_seal) == CanonicalSegmentErrorV1::kNone);
    CHECK(second_writer->Seal(
              {.closed_realtime_ns = 10000000,
               .closed_monotonic_ns = 5000000},
              &second_seal) == CanonicalSegmentErrorV1::kNone);
    CHECK(first_seal.record_stream_sha256 ==
          second_seal.record_stream_sha256);
    CHECK(first_seal.segment_integrity_sha256 !=
          second_seal.segment_integrity_sha256);
    CHECK(first_seal.segment_header_sha256 !=
          second_seal.segment_header_sha256);
    return true;
}

[[nodiscard]] bool CheckRetainedDirectoryPath(
    const std::filesystem::path& directory) {
    const int retained = ::open(
        directory.c_str(),
        O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    CHECK(retained >= 0);
    const std::filesystem::path retained_directory(
        "/proc/self/fd/" + std::to_string(retained) + "/.");
    const CanonicalSegmentDescriptorV1 descriptor =
        MakeDescriptor(31U, 1U, 1U);
    const CanonicalSegmentCreateOptionsV1 options = MakeOptions(
        retained_directory, "retained-directory", descriptor, 12000000);
    std::unique_ptr<CanonicalSegmentWriterV1> writer;
    CHECK(CanonicalSegmentWriterV1::Create(options, &writer) ==
          CanonicalSegmentErrorV1::kNone);
    CHECK(writer->AdvanceProcessedRaw(0U, 4096U) ==
          CanonicalSegmentErrorV1::kNone);
    CanonicalSegmentSealResultV1 seal{};
    CHECK(writer->Seal(
              {.closed_realtime_ns = 13000000,
               .closed_monotonic_ns = 6500000},
              &seal) == CanonicalSegmentErrorV1::kNone);
    writer.reset();
    CHECK(::close(retained) == 0);
    CHECK(std::filesystem::exists(
        directory / "retained-directory.clog"));
    CHECK(std::filesystem::exists(
        directory / "retained-directory.manifest"));
    return true;
}

}  // namespace

int main() {
    TemporaryDirectory directory;
    if (directory.path().empty()) {
        std::cerr << "failed to create temporary directory\n";
        return 1;
    }
    if (!CheckCreationPublicationSeal(directory.path()) ||
        !CheckRecoveryDoesNotResumeOpen(directory.path()) ||
        !CheckConcurrentReaderWriter(directory.path()) ||
        !CheckRecordHashDeterminism(directory.path()) ||
        !CheckRetainedDirectoryPath(directory.path())) {
        return 1;
    }
    std::cout << "phase5 canonical segment tests passed\n";
    return 0;
}
