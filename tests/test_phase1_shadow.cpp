#include "l2flow/ingress/shadow_capture.h"
#include "l2flow/ops/stable_output_prefix.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <sys/stat.h>
#include <unistd.h>

namespace ingress = l2flow::ingress;
namespace ops = l2flow::ops;
namespace sdk = l2flow::sdk;

namespace l2flow::ingress {

class ByteRingTestPeer final {
public:
    static void xor_byte(ByteRing& ring,
                         std::uint64_t absolute_position,
                         std::byte mask) noexcept {
        const std::size_t index = static_cast<std::size_t>(
            absolute_position % ring.capacity_bytes_);
        ring.storage_[index] ^= mask;
    }
};

}  // namespace l2flow::ingress

namespace {

struct TestContext final {
    void Expect(bool condition, const std::string& description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    int failures = 0;
};

void StoreU16(std::uint16_t value,
              std::byte* destination) noexcept {
    destination[0] = static_cast<std::byte>(value & 0xffU);
    destination[1] =
        static_cast<std::byte>((value >> 8U) & 0xffU);
}

void StoreU32(std::uint32_t value,
              std::byte* destination) noexcept {
    for (std::size_t index = 0U; index < 4U; ++index) {
        destination[index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
}

void StoreU64(std::uint64_t value,
              std::byte* destination) noexcept {
    for (std::size_t index = 0U; index < 8U; ++index) {
        destination[index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
}

std::array<std::byte, ingress::kVendorMessageHeadBytes>
MakeHead(std::size_t body_bytes,
         std::uint8_t service_id,
         std::uint16_t service_version,
         std::uint16_t message_id,
         std::uint64_t sequence) {
    std::array<std::byte, ingress::kVendorMessageHeadBytes> head{};
    head[0] =
        static_cast<std::byte>(ingress::kVendorMessageHeadBytes);
    StoreU32(
        static_cast<std::uint32_t>(
            ingress::kVendorMessageHeadBytes + body_bytes),
        head.data() + 1U);
    head[5] = std::byte{1U};
    head[6] = static_cast<std::byte>(service_id);
    StoreU16(service_version, head.data() + 7U);
    StoreU16(message_id, head.data() + 9U);
    StoreU32(static_cast<std::uint32_t>(sequence),
             head.data() + 11U);
    StoreU64(sequence * 17U, head.data() + 15U);
    return head;
}

ingress::CaptureMetaV1 MakeMeta(std::uint32_t source_stream_id,
                                std::uint64_t sequence) {
    ingress::CaptureMetaV1 meta{};
    meta.source_stream_id = source_stream_id;
    meta.connection_epoch_hint = 9U;
    meta.ingress_sequence = sequence;
    meta.recv_realtime_ns = 1'000'000U + sequence;
    meta.recv_monotonic_ns = 2'000'000U + sequence;
    meta.capture_date = 20260717U;
    meta.flags = static_cast<std::uint32_t>(sequence & 7U);
    return meta;
}

std::vector<std::byte> MakeBody(std::size_t size,
                                std::uint64_t sequence) {
    std::vector<std::byte> body(size);
    for (std::size_t index = 0U; index < size; ++index) {
        body[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                sequence * 29U +
                static_cast<std::uint64_t>(index) * 11U));
    }
    return body;
}

enum class DynamicDescriptorKind : std::uint8_t {
    String = 0U,
    List,
};

struct DynamicDescriptor final {
    std::size_t offset = 0U;
    DynamicDescriptorKind kind = DynamicDescriptorKind::String;
};

struct RequiredBodyFixture final {
    std::string name;
    ingress::ShadowCaptureConfig config;
    sdk::MessageKey key;
    std::uint64_t expected_bit = 0U;
    std::size_t fixed_body_bytes = 0U;
    std::vector<std::byte> body;
    std::vector<DynamicDescriptor> descriptors;
};

void AppendStringRange(std::vector<std::byte>* body,
                       std::size_t descriptor_offset,
                       std::byte value,
                       std::vector<DynamicDescriptor>* descriptors) {
    const std::size_t start = body->size();
    body->push_back(value);
    StoreU16(1U, body->data() + descriptor_offset);
    StoreU32(
        static_cast<std::uint32_t>(start - descriptor_offset),
        body->data() + descriptor_offset + 2U);
    descriptors->push_back(
        {descriptor_offset, DynamicDescriptorKind::String});
}

std::size_t AppendListRange(
    std::vector<std::byte>* body,
    std::size_t descriptor_offset,
    std::size_t item_bytes,
    std::vector<DynamicDescriptor>* descriptors) {
    const std::size_t start = body->size();
    body->resize(start + item_bytes);
    StoreU32(1U, body->data() + descriptor_offset);
    StoreU32(
        static_cast<std::uint32_t>(start - descriptor_offset),
        body->data() + descriptor_offset + 4U);
    descriptors->push_back(
        {descriptor_offset, DynamicDescriptorKind::List});
    return start;
}

ingress::ShadowCaptureConfig SingleRequiredConfig(
    std::uint32_t source_stream_id,
    const sdk::MessageKey& key) {
    ingress::ShadowCaptureConfig config;
    config.source_stream_id = source_stream_id;
    config.market_service_id = key.service_id;
    config.required_market_messages = {key};
    return config;
}

RequiredBodyFixture MakeShSnapshotFixture() {
    constexpr sdk::MessageKey key{4U, 101U, 4U};
    RequiredBodyFixture fixture;
    fixture.name = "SH 4.4";
    fixture.config = SingleRequiredConfig(1001U, key);
    fixture.key = key;
    fixture.expected_bit = 0x1U;
    fixture.fixed_body_bytes = 248U;
    fixture.body.resize(fixture.fixed_body_bytes);

    AppendStringRange(
        &fixture.body,
        4U,
        std::byte{'6'},
        &fixture.descriptors);
    AppendStringRange(
        &fixture.body,
        38U,
        std::byte{'T'},
        &fixture.descriptors);
    const std::size_t bid_item = AppendListRange(
        &fixture.body,
        228U,
        28U,
        &fixture.descriptors);
    static_cast<void>(AppendListRange(
        &fixture.body,
        bid_item + 20U,
        16U,
        &fixture.descriptors));
    const std::size_t sell_item = AppendListRange(
        &fixture.body,
        236U,
        28U,
        &fixture.descriptors);
    static_cast<void>(AppendListRange(
        &fixture.body,
        sell_item + 20U,
        16U,
        &fixture.descriptors));
    return fixture;
}

RequiredBodyFixture MakeShTickFixture() {
    constexpr sdk::MessageKey key{4U, 101U, 24U};
    RequiredBodyFixture fixture;
    fixture.name = "SH 4.24";
    fixture.config = SingleRequiredConfig(1002U, key);
    fixture.key = key;
    fixture.expected_bit = 0x1U;
    fixture.fixed_body_bytes = 70U;
    fixture.body.resize(fixture.fixed_body_bytes);

    AppendStringRange(
        &fixture.body,
        12U,
        std::byte{'6'},
        &fixture.descriptors);
    AppendStringRange(
        &fixture.body,
        22U,
        std::byte{'A'},
        &fixture.descriptors);
    AppendStringRange(
        &fixture.body,
        64U,
        std::byte{'B'},
        &fixture.descriptors);
    return fixture;
}

RequiredBodyFixture MakeSzSnapshotFixture() {
    constexpr sdk::MessageKey key{6U, 101U, 28U};
    RequiredBodyFixture fixture;
    fixture.name = "SZ 6.28";
    fixture.config = SingleRequiredConfig(2001U, key);
    fixture.key = key;
    fixture.expected_bit = 0x1U;
    fixture.fixed_body_bytes = 224U;
    fixture.body.resize(fixture.fixed_body_bytes);

    constexpr std::array<std::size_t, 4U> string_offsets{
        8U, 14U, 20U, 26U};
    for (const std::size_t offset : string_offsets) {
        AppendStringRange(
            &fixture.body,
            offset,
            std::byte{'1'},
            &fixture.descriptors);
    }
    const std::size_t bid_item = AppendListRange(
        &fixture.body,
        208U,
        28U,
        &fixture.descriptors);
    static_cast<void>(AppendListRange(
        &fixture.body,
        bid_item + 20U,
        8U,
        &fixture.descriptors));
    const std::size_t ask_item = AppendListRange(
        &fixture.body,
        216U,
        28U,
        &fixture.descriptors);
    static_cast<void>(AppendListRange(
        &fixture.body,
        ask_item + 20U,
        8U,
        &fixture.descriptors));
    return fixture;
}

RequiredBodyFixture MakeSzOrderFixture() {
    constexpr sdk::MessageKey key{6U, 101U, 33U};
    RequiredBodyFixture fixture;
    fixture.name = "SZ 6.33";
    fixture.config = SingleRequiredConfig(2002U, key);
    fixture.key = key;
    fixture.expected_bit = 0x1U;
    fixture.fixed_body_bytes = 58U;
    fixture.body.resize(fixture.fixed_body_bytes);

    constexpr std::array<std::size_t, 3U> string_offsets{
        12U, 18U, 24U};
    for (const std::size_t offset : string_offsets) {
        AppendStringRange(
            &fixture.body,
            offset,
            std::byte{'1'},
            &fixture.descriptors);
    }
    return fixture;
}

RequiredBodyFixture MakeSzTransactionFixture() {
    constexpr sdk::MessageKey key{6U, 101U, 36U};
    RequiredBodyFixture fixture;
    fixture.name = "SZ 6.36";
    fixture.config = SingleRequiredConfig(2002U, key);
    fixture.key = key;
    fixture.expected_bit = 0x1U;
    fixture.fixed_body_bytes = 70U;
    fixture.body.resize(fixture.fixed_body_bytes);

    constexpr std::array<std::size_t, 3U> string_offsets{
        12U, 34U, 40U};
    for (const std::size_t offset : string_offsets) {
        AppendStringRange(
            &fixture.body,
            offset,
            std::byte{'1'},
            &fixture.descriptors);
    }
    return fixture;
}

std::vector<std::byte> MakeSubscriptionStatusBody(
    bool logon_response,
    std::uint32_t return_code,
    std::uint32_t first_status,
    std::uint32_t second_status,
    bool include_second = true) {
    const std::size_t services_list_offset =
        logon_response ? 12U : 0U;
    const std::size_t service_offset =
        logon_response ? 24U : 8U;
    const std::size_t messages_offset = service_offset + 16U;
    const std::size_t message_count =
        include_second ? 2U : 1U;
    std::vector<std::byte> body(
        messages_offset + message_count * 8U);

    StoreU32(1U, body.data() + services_list_offset);
    StoreU32(
        static_cast<std::uint32_t>(
            service_offset - services_list_offset),
        body.data() + services_list_offset + 4U);
    if (logon_response) {
        StoreU32(return_code, body.data() + 20U);
    }
    StoreU32(6U, body.data() + service_offset);
    StoreU32(101U, body.data() + service_offset + 4U);
    StoreU32(
        static_cast<std::uint32_t>(message_count),
        body.data() + service_offset + 8U);
    StoreU32(8U, body.data() + service_offset + 12U);
    StoreU32(33U, body.data() + messages_offset);
    StoreU32(first_status, body.data() + messages_offset + 4U);
    if (include_second) {
        StoreU32(36U, body.data() + messages_offset + 8U);
        StoreU32(
            second_status, body.data() + messages_offset + 12U);
    }
    return body;
}

bool SameMeta(const ingress::CaptureMetaV1& left,
              const ingress::CaptureMetaV1& right) noexcept {
    return left.source_stream_id == right.source_stream_id &&
           left.connection_epoch_hint ==
               right.connection_epoch_hint &&
           left.ingress_sequence == right.ingress_sequence &&
           left.recv_realtime_ns == right.recv_realtime_ns &&
           left.recv_monotonic_ns == right.recv_monotonic_ns &&
           left.capture_date == right.capture_date &&
           left.flags == right.flags;
}

struct ExpectedRecord final {
    ingress::CaptureMetaV1 meta{};
    std::array<std::byte, ingress::kVendorMessageHeadBytes> head{};
    std::vector<std::byte> body;
};

ingress::ShadowCaptureConfig SzTickConfig() {
    ingress::ShadowCaptureConfig config;
    config.source_stream_id = 2002U;
    config.market_service_id = 6U;
    config.required_market_messages = {
        sdk::MessageKey{6U, 101U, 33U},
        sdk::MessageKey{6U, 101U, 36U},
    };
    return config;
}

struct MemoryOutputState final {
    std::vector<std::byte> bytes;
    std::atomic<std::uint64_t> write_calls{0U};
    std::atomic<std::uint64_t> sync_calls{0U};
    std::atomic<std::uint64_t> close_calls{0U};
    std::size_t maximum_chunk =
        std::numeric_limits<std::size_t>::max();
    bool inject_first_eintr = false;
    std::uint64_t fail_write_call = 0U;
    int write_error = EBADF;
    int sync_error = 0;
    int close_error = 0;
};

class MemoryOutput final : public ingress::ShadowCaptureOutput {
public:
    explicit MemoryOutput(
        std::shared_ptr<MemoryOutputState> state)
        : state_(std::move(state)) {}

    ingress::ShadowOutputWriteResult WriteSome(
        std::span<const std::byte> bytes) noexcept override {
        const std::uint64_t call =
            state_->write_calls.fetch_add(
                1U, std::memory_order_relaxed) +
            1U;
        if (state_->inject_first_eintr && call == 1U) {
            return {0U, EINTR};
        }
        if (state_->fail_write_call != 0U &&
            call >= state_->fail_write_call) {
            return {0U, state_->write_error};
        }
        const std::size_t count =
            std::min(bytes.size(), state_->maximum_chunk);
        try {
            state_->bytes.insert(
                state_->bytes.end(),
                bytes.begin(),
                bytes.begin() +
                    static_cast<std::ptrdiff_t>(count));
        } catch (...) {
            return {0U, ENOMEM};
        }
        return {count, 0};
    }

    int Fdatasync() noexcept override {
        state_->sync_calls.fetch_add(
            1U, std::memory_order_relaxed);
        return state_->sync_error;
    }

    int Close() noexcept override {
        state_->close_calls.fetch_add(
            1U, std::memory_order_relaxed);
        return state_->close_error;
    }

private:
    std::shared_ptr<MemoryOutputState> state_;
};

class TempFile final {
public:
    TempFile() {
        std::array<char, 64U> pattern{};
        constexpr char prefix[] =
            "/tmp/l2flow-shadow-dir-XXXXXX";
        static_assert(sizeof(prefix) <= pattern.size());
        std::memcpy(pattern.data(), prefix, sizeof(prefix));
        if (::mkdtemp(pattern.data()) == nullptr) {
            throw std::runtime_error("mkdtemp failed");
        }
        directory_ = pattern.data();
        if (::chmod(directory_.c_str(), 0700) != 0) {
            static_cast<void>(::rmdir(directory_.c_str()));
            throw std::runtime_error(
                "setting temp shadow directory mode failed");
        }
        path_ = directory_ + "/capture";
        const int fd = ::open(
            path_.c_str(),
            O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC,
            0600);
        if (fd < 0) {
            static_cast<void>(::rmdir(directory_.c_str()));
            throw std::runtime_error(
                "creating temp shadow file failed");
        }
        if (::fchmod(fd, 0600) != 0) {
            static_cast<void>(::close(fd));
            static_cast<void>(std::remove(path_.c_str()));
            static_cast<void>(::rmdir(directory_.c_str()));
            throw std::runtime_error(
                "setting temp shadow file mode failed");
        }
        if (::close(fd) != 0) {
            static_cast<void>(std::remove(path_.c_str()));
            static_cast<void>(::rmdir(directory_.c_str()));
            throw std::runtime_error(
                "closing temp shadow fd failed");
        }
    }

    ~TempFile() {
        if (!path_.empty()) {
            static_cast<void>(std::remove(path_.c_str()));
        }
        if (!directory_.empty()) {
            static_cast<void>(::rmdir(directory_.c_str()));
        }
    }

    TempFile(const TempFile&) = delete;
    TempFile& operator=(const TempFile&) = delete;

    const std::string& path() const noexcept { return path_; }
    const std::string& directory() const noexcept {
        return directory_;
    }

private:
    std::string directory_;
    std::string path_;
};

std::vector<std::byte> ReadFile(const std::string& path) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        throw std::runtime_error("open for reread failed");
    }
    std::vector<std::byte> result;
    std::array<std::byte, 17U> buffer{};
    for (;;) {
        const ssize_t count =
            ::read(fd, buffer.data(), buffer.size());
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count < 0) {
            static_cast<void>(::close(fd));
            throw std::runtime_error("file reread failed");
        }
        if (count == 0) {
            break;
        }
        result.insert(
            result.end(),
            buffer.begin(),
            buffer.begin() + count);
    }
    if (::close(fd) != 0) {
        throw std::runtime_error("reread close failed");
    }
    return result;
}

void WriteFile(const std::string& path, std::string_view contents) {
    const int fd =
        ::open(path.c_str(), O_WRONLY | O_TRUNC | O_CLOEXEC);
    if (fd < 0) {
        throw std::runtime_error("open for fixture write failed");
    }
    std::size_t offset = 0U;
    while (offset < contents.size()) {
        const ssize_t count =
            ::write(
                fd,
                contents.data() + offset,
                contents.size() - offset);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            static_cast<void>(::close(fd));
            throw std::runtime_error("fixture write failed");
        }
        offset += static_cast<std::size_t>(count);
    }
    if (::close(fd) != 0) {
        throw std::runtime_error("fixture write close failed");
    }
}

void CreateFixtureFile(
    const std::string& path,
    std::string_view contents,
    mode_t mode) {
    const int fd = ::open(
        path.c_str(),
        O_WRONLY | O_CREAT | O_EXCL |
            O_CLOEXEC | O_NOFOLLOW,
        0600);
    if (fd < 0) {
        throw std::runtime_error(
            "open for fixture creation failed");
    }
    if (::close(fd) != 0) {
        throw std::runtime_error(
            "fixture creation close failed");
    }
    WriteFile(path, contents);
    if (::chmod(path.c_str(), mode) != 0) {
        throw std::runtime_error(
            "setting fixture mode failed");
    }
}

template <typename T>
bool CopyObject(const std::vector<std::byte>& bytes,
                std::size_t offset,
                T* result) noexcept {
    if (offset > bytes.size() ||
        sizeof(T) > bytes.size() - offset) {
        return false;
    }
    std::memcpy(result, bytes.data() + offset, sizeof(T));
    return true;
}

void PushEventually(ingress::ByteRing* ring,
                    const ExpectedRecord& record,
                    ingress::CaptureMetrics* metrics,
                    TestContext* test) {
    constexpr std::size_t maximum_attempts = 1'000'000U;
    for (std::size_t attempt = 0U;
         attempt < maximum_attempts;
         ++attempt) {
        const ingress::ByteRingPushResult result =
            ring->try_push_copy(
                record.meta, record.head, record.body);
        if (result == ingress::ByteRingPushResult::PUBLISHED) {
            metrics->IncrementCaptured(
                static_cast<std::uint32_t>(
                    record.head.size() + record.body.size()),
                record.meta.ingress_sequence);
            return;
        }
        if (result != ingress::ByteRingPushResult::FULL) {
            test->Expect(false, "producer push is valid");
            return;
        }
        std::this_thread::yield();
    }
    test->Expect(false, "producer did not remain permanently full");
}

std::uint64_t ObserveRequiredBody(
    const RequiredBodyFixture& fixture,
    std::span<const std::byte> body,
    TestContext* test,
    const std::string& case_name) {
    ingress::ByteRing ring(4096U, 1024U);
    ops::FatalLatch fatal;
    const auto state = std::make_shared<MemoryOutputState>();
    ingress::ShadowCaptureWriter writer(
        fixture.config,
        ring,
        fatal,
        std::make_unique<MemoryOutput>(state));
    std::atomic<bool> run_result{false};
    std::thread consumer([&] {
        run_result.store(
            writer.Run(), std::memory_order_release);
    });
    while (!writer.startup_complete()) {
        std::this_thread::yield();
    }
    const auto head = MakeHead(
        body.size(),
        fixture.key.service_id,
        fixture.key.service_version,
        fixture.key.message_id,
        1U);
    const bool published =
        ring.try_push_copy(
            MakeMeta(fixture.config.source_stream_id, 1U),
            head,
            body) ==
        ingress::ByteRingPushResult::PUBLISHED;
    // The only producer is quiescent before stop publication.
    writer.StopAndDrain();
    consumer.join();
    const ingress::ShadowCaptureStats stats = writer.Snapshot();
    test->Expect(
        published &&
            run_result.load(std::memory_order_acquire) &&
            writer.startup_succeeded() &&
            !fatal.tripped() &&
            stats.sink_records == 1U,
        fixture.name + " " + case_name +
            " is captured without treating market-body schema "
            "invalidity as ring corruption");
    return stats.required_first_seen_mask;
}

void VerifyFile(const std::vector<std::byte>& bytes,
                const std::vector<ExpectedRecord>& expected,
                TestContext* test) {
    ingress::ShadowCaptureFileHeaderV1 file_header{};
    test->Expect(
        CopyObject(bytes, 0U, &file_header),
        "complete fixed file header is readable");
    test->Expect(
        file_header.magic == ingress::kShadowCaptureFileMagic,
        "file magic matches");
    test->Expect(
        file_header.format_version ==
            ingress::kShadowCaptureFormatVersion &&
            file_header.header_bytes ==
                sizeof(ingress::ShadowCaptureFileHeaderV1) &&
            file_header.record_header_bytes ==
                sizeof(ingress::ShadowCaptureRecordHeaderV1),
        "file format sizes are self-describing");
    test->Expect(
        file_header.capture_meta_bytes ==
                sizeof(ingress::CaptureMetaV1) &&
            file_header.vendor_head_bytes ==
                ingress::kVendorMessageHeadBytes &&
            file_header.source_stream_id == 2002U &&
            file_header.max_message_bytes == 128U &&
            file_header.required_market_count == 2U,
        "file header captures stream configuration");
    test->Expect(
        std::all_of(
            file_header.reserved.begin(),
            file_header.reserved.end(),
            [](std::uint64_t value) { return value == 0U; }),
        "file header reserved bytes are zero");

    std::size_t offset =
        sizeof(ingress::ShadowCaptureFileHeaderV1);
    for (std::size_t index = 0U; index < expected.size();
         ++index) {
        test->Expect(
            offset %
                    ingress::kShadowCaptureRecordAlignment ==
                0U,
            "each record starts naturally aligned");
        ingress::ShadowCaptureRecordHeaderV1 record_header{};
        if (!CopyObject(bytes, offset, &record_header)) {
            test->Expect(false, "complete record header is readable");
            return;
        }
        const ExpectedRecord& wanted = expected[index];
        const std::size_t vendor_bytes =
            wanted.head.size() + wanted.body.size();
        const std::size_t unaligned =
            sizeof(record_header) + vendor_bytes;
        const std::size_t aligned =
            (unaligned +
             ingress::kShadowCaptureRecordAlignment - 1U) &
            ~(ingress::kShadowCaptureRecordAlignment - 1U);
        const std::size_t padding = aligned - unaligned;

        test->Expect(
            record_header.magic ==
                    ingress::kShadowCaptureRecordMagic &&
                record_header.format_version ==
                    ingress::kShadowCaptureFormatVersion &&
                record_header.header_bytes ==
                    sizeof(record_header),
            "record prefix is valid");
        test->Expect(
            record_header.record_bytes == aligned &&
                record_header.sink_record_index == index + 1U &&
                record_header.vendor_message_bytes ==
                    vendor_bytes &&
                record_header.padding_bytes == padding,
            "record lengths and sequential index are exact");
        test->Expect(
            SameMeta(record_header.meta, wanted.meta),
            "CaptureMetaV1 rereads byte-for-byte semantically");

        const std::size_t head_offset =
            offset + sizeof(record_header);
        const bool payload_fits =
            head_offset <= bytes.size() &&
            vendor_bytes <= bytes.size() - head_offset;
        test->Expect(payload_fits, "record payload is complete");
        if (!payload_fits) {
            return;
        }
        test->Expect(
            std::equal(
                wanted.head.begin(),
                wanted.head.end(),
                bytes.begin() +
                    static_cast<std::ptrdiff_t>(head_offset)),
            "vendor head rereads byte exactly");
        test->Expect(
            std::equal(
                wanted.body.begin(),
                wanted.body.end(),
                bytes.begin() +
                    static_cast<std::ptrdiff_t>(
                        head_offset + wanted.head.size())),
            "vendor body rereads byte exactly");
        const std::size_t padding_offset =
            head_offset + vendor_bytes;
        bool padding_zero = true;
        for (std::size_t byte = 0U; byte < padding; ++byte) {
            padding_zero =
                padding_zero &&
                bytes[padding_offset + byte] == std::byte{0U};
        }
        test->Expect(padding_zero, "record alignment padding is zero");
        offset += aligned;
    }
    test->Expect(offset == bytes.size(),
                 "rereader consumes the complete file exactly");
}

void TestRequiredMarketBodyValidation(TestContext* test) {
    std::array<RequiredBodyFixture, 5U> fixtures{
        MakeShSnapshotFixture(),
        MakeShTickFixture(),
        MakeSzSnapshotFixture(),
        MakeSzOrderFixture(),
        MakeSzTransactionFixture(),
    };

    std::size_t checked_descriptors = 0U;
    std::size_t checked_string_topology = 0U;
    std::size_t checked_top_list_topology = 0U;
    std::size_t checked_nested_list_topology = 0U;
    for (const RequiredBodyFixture& fixture : fixtures) {
        test->Expect(
            ObserveRequiredBody(
                fixture,
                fixture.body,
                test,
                "valid dynamic ranges") ==
                fixture.expected_bit,
            fixture.name +
                " valid strings, lists, and nested lists satisfy "
                "first-seen structural readiness");

        std::vector<std::byte> truncated = fixture.body;
        truncated.pop_back();
        test->Expect(
            ObserveRequiredBody(
                fixture,
                truncated,
                test,
                "truncated dynamic payload") == 0U,
            fixture.name +
                " truncated dynamic payload cannot set first_seen");

        for (std::size_t descriptor_index = 0U;
             descriptor_index < fixture.descriptors.size();
             ++descriptor_index) {
            const DynamicDescriptor& descriptor =
                fixture.descriptors[descriptor_index];
            std::vector<std::byte> malformed = fixture.body;
            const std::size_t invalid_start =
                malformed.size() + 1U;
            const std::uint32_t invalid_relative =
                static_cast<std::uint32_t>(
                    invalid_start - descriptor.offset);
            if (descriptor.kind ==
                DynamicDescriptorKind::List) {
                StoreU32(
                    1U,
                    malformed.data() + descriptor.offset);
                StoreU32(
                    invalid_relative,
                    malformed.data() + descriptor.offset + 4U);
            } else {
                StoreU16(
                    1U,
                    malformed.data() + descriptor.offset);
                StoreU32(
                    invalid_relative,
                    malformed.data() + descriptor.offset + 2U);
            }
            test->Expect(
                ObserveRequiredBody(
                    fixture,
                    malformed,
                    test,
                    "out-of-range descriptor " +
                        std::to_string(descriptor_index)) == 0U,
                fixture.name + " dynamic descriptor " +
                    std::to_string(descriptor_index) +
                    " is range-checked before first_seen");

            if (descriptor.kind ==
                DynamicDescriptorKind::List) {
                StoreU32(
                    0U,
                    malformed.data() + descriptor.offset);
            } else {
                StoreU16(
                    0U,
                    malformed.data() + descriptor.offset);
            }
            test->Expect(
                ObserveRequiredBody(
                    fixture,
                    malformed,
                    test,
                    "out-of-range empty descriptor " +
                        std::to_string(descriptor_index)) == 0U,
                fixture.name + " empty dynamic descriptor " +
                    std::to_string(descriptor_index) +
                    " still rejects an unresolvable nonzero offset");

            std::vector<std::byte> resolvable_empty =
                fixture.body;
            const std::uint32_t one_past_end_relative =
                static_cast<std::uint32_t>(
                    resolvable_empty.size() - descriptor.offset);
            if (descriptor.kind ==
                DynamicDescriptorKind::List) {
                StoreU32(
                    0U,
                    resolvable_empty.data() + descriptor.offset);
                StoreU32(
                    one_past_end_relative,
                    resolvable_empty.data() +
                        descriptor.offset + 4U);
            } else {
                StoreU16(
                    0U,
                    resolvable_empty.data() + descriptor.offset);
                StoreU32(
                    one_past_end_relative,
                    resolvable_empty.data() +
                        descriptor.offset + 2U);
            }
            test->Expect(
                ObserveRequiredBody(
                    fixture,
                    resolvable_empty,
                    test,
                    "resolvable noncanonical empty descriptor " +
                        std::to_string(descriptor_index)) ==
                    fixture.expected_bit,
                fixture.name + " empty dynamic descriptor " +
                    std::to_string(descriptor_index) +
                    " accepts a resolvable one-past-end offset");
            ++checked_descriptors;
        }

        const auto first_string =
            std::find_if(
                fixture.descriptors.begin(),
                fixture.descriptors.end(),
                [](const DynamicDescriptor& descriptor) {
                    return descriptor.kind ==
                           DynamicDescriptorKind::String;
                });
        test->Expect(
            first_string != fixture.descriptors.end(),
            fixture.name +
                " exposes a string topology fixture");
        if (first_string != fixture.descriptors.end()) {
            std::vector<std::byte> inside_fixed =
                fixture.body;
            const std::size_t invalid_start =
                fixture.fixed_body_bytes - 1U;
            StoreU16(
                1U,
                inside_fixed.data() +
                    first_string->offset);
            StoreU32(
                static_cast<std::uint32_t>(
                    invalid_start -
                    first_string->offset),
                inside_fixed.data() +
                    first_string->offset + 2U);
            test->Expect(
                ObserveRequiredBody(
                    fixture,
                    inside_fixed,
                    test,
                    "string points into fixed body") == 0U,
                fixture.name +
                    " rejects an in-bounds string that aliases "
                    "the fixed body");
            ++checked_string_topology;
        }

        const auto first_top_list =
            std::find_if(
                fixture.descriptors.begin(),
                fixture.descriptors.end(),
                [&fixture](
                    const DynamicDescriptor& descriptor) {
                    return descriptor.kind ==
                               DynamicDescriptorKind::List &&
                           descriptor.offset <
                               fixture.fixed_body_bytes;
                });
        if (first_top_list != fixture.descriptors.end()) {
            std::vector<std::byte> inside_fixed =
                fixture.body;
            const std::size_t invalid_start =
                fixture.fixed_body_bytes - 1U;
            StoreU32(
                1U,
                inside_fixed.data() +
                    first_top_list->offset);
            StoreU32(
                static_cast<std::uint32_t>(
                    invalid_start -
                    first_top_list->offset),
                inside_fixed.data() +
                    first_top_list->offset + 4U);
            test->Expect(
                ObserveRequiredBody(
                    fixture,
                    inside_fixed,
                    test,
                    "top list points into fixed body") == 0U,
                fixture.name +
                    " rejects an in-bounds top-level list that "
                    "aliases the fixed body");
            ++checked_top_list_topology;

            const auto first_nested_list =
                std::find_if(
                    fixture.descriptors.begin(),
                    fixture.descriptors.end(),
                    [&fixture](
                        const DynamicDescriptor& descriptor) {
                        return descriptor.kind ==
                                   DynamicDescriptorKind::List &&
                               descriptor.offset >=
                                   fixture.fixed_body_bytes;
                    });
            test->Expect(
                first_nested_list !=
                    fixture.descriptors.end(),
                fixture.name +
                    " exposes a nested-list topology fixture");
            if (first_nested_list !=
                fixture.descriptors.end()) {
                std::vector<std::byte> overlaps_top =
                    fixture.body;
                StoreU32(
                    2U,
                    overlaps_top.data() +
                        first_top_list->offset);
                StoreU32(
                    1U,
                    overlaps_top.data() +
                        first_nested_list->offset);
                StoreU32(
                    8U,
                    overlaps_top.data() +
                        first_nested_list->offset + 4U);
                test->Expect(
                    ObserveRequiredBody(
                        fixture,
                        overlaps_top,
                        test,
                        "nested list overlaps top list") ==
                        0U,
                    fixture.name +
                        " rejects an in-bounds nested list that "
                        "overlaps its top-level list");
                ++checked_nested_list_topology;
            }
        }
    }
    test->Expect(
        checked_descriptors == 23U,
        "all 23 instantiated required-message dynamic ranges are "
        "individually rejected when out of bounds");
    test->Expect(
        checked_string_topology == fixtures.size() &&
            checked_top_list_topology == 2U &&
            checked_nested_list_topology == 2U,
        "required schemas reject in-bounds fixed-body and nested-list "
        "topology aliases");
}

void TestFileWrapZeroAndObservation(TestContext* test) {
    constexpr std::uint32_t max_message_bytes = 128U;
    ingress::ByteRing ring(251U, max_message_bytes);
    ops::FatalLatch fatal;
    ingress::CaptureMetrics metrics;
    TempFile file;
    const std::string prior_shadow_magic(
        ingress::kShadowCaptureFileMagic.begin(),
        ingress::kShadowCaptureFileMagic.end());
    WriteFile(file.path(), prior_shadow_magic);
    ingress::ShadowCaptureWriter writer(
        SzTickConfig(), ring, fatal, file.path());
    struct stat shadow_status {};
    test->Expect(
        ::stat(file.path().c_str(), &shadow_status) == 0 &&
            shadow_status.st_uid == ::geteuid() &&
            (shadow_status.st_mode & 07777) == 0600,
        "existing typed owner-only shadow output retains exact 0600 mode");

    const struct Descriptor {
        std::size_t body_bytes;
        std::uint8_t service;
        std::uint16_t version;
        std::uint16_t message;
    } descriptors[] = {
        {56U, 2U, 101U, 2U},
        {58U, 6U, 101U, 33U},
        {40U, 2U, 101U, 23U},
        {70U, 6U, 101U, 36U},
        {1U, 6U, 101U, 33U},
        {16U, 1U, 101U, 3U},
    };

    std::vector<ExpectedRecord> expected;
    expected.reserve(std::size(descriptors));
    for (std::size_t index = 0U;
         index < std::size(descriptors);
         ++index) {
        const std::uint64_t sequence =
            static_cast<std::uint64_t>(index) + 1U;
        const Descriptor& descriptor = descriptors[index];
        ExpectedRecord record;
        record.meta = MakeMeta(2002U, sequence);
        record.head = MakeHead(
            descriptor.body_bytes,
            descriptor.service,
            descriptor.version,
            descriptor.message,
            sequence);
        if (descriptor.service == 2U &&
            descriptor.message == 2U) {
            record.body = MakeSubscriptionStatusBody(
                true, 0U, 0U, 0U);
        } else if (descriptor.service == 2U &&
                   descriptor.message == 23U) {
            record.body = MakeSubscriptionStatusBody(
                false, 0U, 0U, 0U);
        } else if (
            descriptor.service == 6U &&
            (descriptor.message == 33U ||
             descriptor.message == 36U)) {
            record.body =
                std::vector<std::byte>(descriptor.body_bytes);
        } else {
            record.body =
                MakeBody(descriptor.body_bytes, sequence);
        }
        expected.push_back(std::move(record));
    }

    std::atomic<bool> run_result{false};
    std::thread consumer([&] {
        run_result.store(writer.Run(), std::memory_order_release);
    });
    for (const ExpectedRecord& record : expected) {
        PushEventually(&ring, record, &metrics, test);
    }
    writer.StopAndDrain();
    consumer.join();

    test->Expect(
        run_result.load(std::memory_order_acquire),
        "real-file writer drains and closes successfully");
    test->Expect(!fatal.tripped(),
                 "valid shadow capture does not trip fatal");
    test->Expect(writer.finished(), "writer reports completion");
    test->Expect(
        ring.published_position() > ring.capacity_bytes() &&
            ring.consumed_position() ==
                ring.published_position(),
        "small physical ring wrapped and was completely consumed");

    const ingress::ShadowCaptureStats stats = writer.Snapshot();
    std::uint64_t expected_vendor_bytes = 0U;
    for (const ExpectedRecord& record : expected) {
        expected_vendor_bytes +=
            static_cast<std::uint64_t>(
                record.head.size() + record.body.size());
    }
    test->Expect(
        stats.sink_records == expected.size() &&
            stats.sink_vendor_bytes == expected_vendor_bytes &&
            stats.last_ingress_sequence == expected.size(),
        "sink count, bytes and terminal sequence are exact");
    test->Expect(
        stats.logon_response_headers == 1U &&
            stats.subscribe_response_headers == 1U &&
            stats.logon_ok_responses == 1U &&
            stats.logon_failed_responses == 0U &&
            stats.malformed_control_responses == 0U &&
            stats.latest_logon_ok,
        "valid LogonResponse and SubscribeResponse bodies are observed");
    test->Expect(
        stats.required_subscription_ok_mask == 0x3U &&
            stats.required_subscription_failed_mask == 0U &&
            stats.required_subscription_failure_observed_mask == 0U &&
            stats.readiness_generation == 1U &&
            writer.latest_logon_ok() &&
            writer.all_required_subscriptions_ok() &&
            writer.readiness_generation() == 1U,
        "required subscription statuses are safely decoded as successful");
    test->Expect(
        stats.required_first_seen_mask == 0x3U &&
            writer.required_market_mask() == 0x3U &&
            writer.all_required_market_seen(),
        "multiple required market headers set deterministic first-seen bits");

    const auto reconciliation =
        writer.Reconcile(metrics.Snapshot());
    test->Expect(
        reconciliation.exact(),
        "callback and sink record/byte totals reconcile");
    ingress::CaptureMetricsSnapshot mismatch =
        metrics.Snapshot();
    ++mismatch.captured_records;
    test->Expect(
        !writer.Reconcile(mismatch).exact(),
        "one-record discrepancy is visible in reconciliation");

    const std::vector<std::byte> file_bytes =
        ReadFile(file.path());
    test->Expect(
        stats.sink_file_bytes == file_bytes.size(),
        "successful syscall byte count equals file length");
    VerifyFile(file_bytes, expected, test);
}

void TestEmptyDrainAndShortWrite(TestContext* test) {
    ingress::ByteRing ring(256U, 64U);
    ops::FatalLatch fatal;
    const auto state = std::make_shared<MemoryOutputState>();
    state->maximum_chunk = 7U;
    state->inject_first_eintr = true;
    ingress::ShadowCaptureWriter writer(
        SzTickConfig(),
        ring,
        fatal,
        std::make_unique<MemoryOutput>(state));

    writer.StopAndDrain();
    test->Expect(writer.Run(),
                 "empty ring stop-and-drain succeeds");
    const ingress::ShadowCaptureStats stats = writer.Snapshot();
    test->Expect(
        stats.sink_records == 0U &&
            stats.sink_vendor_bytes == 0U &&
            stats.sink_file_bytes ==
                sizeof(ingress::ShadowCaptureFileHeaderV1) &&
            state->bytes.size() ==
                sizeof(ingress::ShadowCaptureFileHeaderV1),
        "zero-record capture contains exactly the file header");
    test->Expect(
        state->write_calls.load(std::memory_order_relaxed) > 2U,
        "WriteAll retries EINTR and completes short writes");
    test->Expect(
        state->sync_calls.load(std::memory_order_relaxed) == 1U &&
            state->close_calls.load(std::memory_order_relaxed) == 1U,
        "clean close performs one fdatasync and one close");
    test->Expect(!fatal.tripped(),
                 "short writes and EINTR are not fatal");
}

void TestConcurrentStopRaceAndAccounting(TestContext* test) {
    constexpr std::size_t record_count = 4000U;
    ingress::ByteRing ring(277U, 48U);
    ops::FatalLatch fatal;
    ingress::CaptureMetrics metrics;
    const auto state = std::make_shared<MemoryOutputState>();
    state->maximum_chunk = 31U;
    ingress::ShadowCaptureConfig config;
    config.source_stream_id = 1002U;
    config.market_service_id = 4U;
    config.required_market_messages = {
        sdk::MessageKey{4U, 101U, 24U},
    };
    ingress::ShadowCaptureWriter writer(
        std::move(config),
        ring,
        fatal,
        std::make_unique<MemoryOutput>(state));

    std::atomic<bool> run_result{false};
    std::thread consumer([&] {
        run_result.store(writer.Run(), std::memory_order_release);
    });
    while (state->write_calls.load(std::memory_order_acquire) ==
           0U) {
        std::this_thread::yield();
    }
    test->Expect(
        !writer.finished(),
        "an EMPTY ring does not stop before stop publication");

    for (std::size_t index = 0U; index < record_count;
         ++index) {
        const std::uint64_t sequence =
            static_cast<std::uint64_t>(index) + 100U;
        ExpectedRecord record;
        record.meta = MakeMeta(1002U, sequence);
        record.head =
            MakeHead(9U, 4U, 101U, 24U, sequence);
        record.body = MakeBody(9U, sequence);
        PushEventually(&ring, record, &metrics, test);
        if ((index & 31U) == 0U) {
            std::this_thread::yield();
        }
    }

    // Producer is closed before this release publication.
    writer.StopAndDrain();
    consumer.join();
    test->Expect(
        run_result.load(std::memory_order_acquire) &&
            !fatal.tripped(),
        "stop racing the final drain loses no published entry");
    const ingress::ShadowCaptureStats stats = writer.Snapshot();
    test->Expect(
        stats.sink_records == record_count &&
            stats.sink_vendor_bytes ==
                record_count *
                    (ingress::kVendorMessageHeadBytes + 9U) &&
            stats.last_ingress_sequence ==
                record_count + 99U,
        "concurrent sink totals cover every produced record");
    test->Expect(
        writer.Reconcile(metrics.Snapshot()).exact(),
        "concurrent callback/sink accounting reconciles");
    test->Expect(
        stats.sink_file_bytes == state->bytes.size() &&
            state->sync_calls.load(std::memory_order_relaxed) == 1U &&
            state->close_calls.load(std::memory_order_relaxed) == 1U,
        "concurrent memory sink byte and close accounting agree");
}

void TestInjectedIoFailures(TestContext* test) {
    {
        ingress::ByteRing ring(256U, 64U);
        ops::FatalLatch fatal;
        const auto state = std::make_shared<MemoryOutputState>();
        state->fail_write_call = 1U;
        ingress::ShadowCaptureWriter writer(
            SzTickConfig(),
            ring,
            fatal,
            std::make_unique<MemoryOutput>(state));
        writer.StopAndDrain();
        test->Expect(!writer.Run(),
                     "EBADF-like first write fails the writer");
        test->Expect(
            fatal.reason() ==
                ops::FatalReason::SHADOW_SINK_IO,
            "write error trips SHADOW_SINK_IO");
        test->Expect(
            state->sync_calls.load(std::memory_order_relaxed) ==
                    1U &&
                state->close_calls.load(
                    std::memory_order_relaxed) == 1U,
            "write failure still finalizes the output");
    }

    {
        ingress::ByteRing ring(256U, 64U);
        ops::FatalLatch fatal;
        const auto state = std::make_shared<MemoryOutputState>();
        state->sync_error = EIO;
        ingress::ShadowCaptureWriter writer(
            SzTickConfig(),
            ring,
            fatal,
            std::make_unique<MemoryOutput>(state));
        writer.StopAndDrain();
        test->Expect(!writer.Run(),
                     "fdatasync failure fails clean shutdown");
        test->Expect(
            fatal.reason() ==
                ops::FatalReason::SHADOW_SINK_IO &&
                state->close_calls.load(
                    std::memory_order_relaxed) == 1U,
            "fdatasync failure is fatal and close is attempted");
    }
}

void TestPosixOutputPolicy(TestContext* test) {
    {
        TempFile reserved_name;
        const std::string target =
            reserved_name.directory() + "/" +
            std::string(
                ops::kSdkLogDirectoryMarkerFilename);
        ingress::ByteRing ring(256U, 64U);
        ops::FatalLatch fatal;
        ingress::ShadowCaptureWriter writer(
            SzTickConfig(),
            ring,
            fatal,
            target);
        struct stat metadata {};
        errno = 0;
        test->Expect(
            fatal.reason() ==
                    ops::FatalReason::SHADOW_SINK_IO &&
                ::lstat(target.c_str(), &metadata) != 0 &&
                errno == ENOENT,
            "shadow cannot create the reserved SDK log marker basename");
    }

    {
        TempFile reserved;
        const std::string marker =
            reserved.directory() + "/" +
            std::string(
                ops::kSdkLogDirectoryMarkerFilename);
        CreateFixtureFile(
            marker,
            ops::kSdkLogDirectoryMarkerContent,
            0444);

        ingress::ByteRing ring(256U, 64U);
        ops::FatalLatch fatal;
        ingress::ShadowCaptureWriter writer(
            SzTickConfig(),
            ring,
            fatal,
            reserved.path());
        const std::vector<std::byte> after =
            ReadFile(reserved.path());
        const int remove_marker =
            ::unlink(marker.c_str());
        test->Expect(
            fatal.reason() ==
                    ops::FatalReason::SHADOW_SINK_IO &&
                after.empty() &&
                remove_marker == 0,
            "a marked SDK log directory rejects shadow capture "
            "without changing the target");
    }

    {
        TempFile foreign;
        constexpr std::string_view sentinel =
            "owner-only-foreign-file";
        WriteFile(foreign.path(), sentinel);

        ingress::ByteRing ring(256U, 64U);
        ops::FatalLatch fatal;
        ingress::ShadowCaptureWriter writer(
            SzTickConfig(),
            ring,
            fatal,
            foreign.path());
        const std::vector<std::byte> after =
            ReadFile(foreign.path());
        test->Expect(
            fatal.reason() ==
                    ops::FatalReason::SHADOW_SINK_IO &&
                after.size() == sentinel.size() &&
                std::memcmp(
                    after.data(),
                    sentinel.data(),
                    sentinel.size()) == 0,
            "an owner-only file without shadow magic is rejected "
            "before truncation");
    }

    {
        TempFile permissive;
        constexpr std::string_view sentinel = "preserve-existing-data";
        WriteFile(permissive.path(), sentinel);
        if (::chmod(permissive.path().c_str(), 0666) != 0) {
            throw std::runtime_error(
                "setting permissive shadow fixture mode failed");
        }

        ingress::ByteRing ring(256U, 64U);
        ops::FatalLatch fatal;
        ingress::ShadowCaptureWriter writer(
            SzTickConfig(), ring, fatal, permissive.path());
        struct stat status {};
        const std::vector<std::byte> after =
            ReadFile(permissive.path());
        test->Expect(
            fatal.reason() ==
                    ops::FatalReason::SHADOW_SINK_IO &&
                ::stat(permissive.path().c_str(), &status) == 0 &&
                (status.st_mode & 07777) == 0666 &&
                after.size() == sentinel.size() &&
                std::memcmp(
                    after.data(),
                    sentinel.data(),
                    sentinel.size()) == 0,
            "pre-existing non-0600 shadow inode is rejected before "
            "chmod, truncation, or reuse");
    }

    {
        TempFile created;
        if (std::remove(created.path().c_str()) != 0) {
            throw std::runtime_error(
                "removing new-shadow fixture failed");
        }
        ingress::ByteRing ring(256U, 64U);
        ops::FatalLatch fatal;
        ingress::ShadowCaptureWriter writer(
            SzTickConfig(), ring, fatal, created.path());
        struct stat status {};
        test->Expect(
            !fatal.tripped() &&
                ::stat(created.path().c_str(), &status) == 0 &&
                status.st_uid == ::geteuid() &&
                status.st_nlink == 1 &&
                (status.st_mode & 07777) == 0600,
            "new shadow inode is created owner-only with one link");
        writer.StopAndDrain();
        test->Expect(writer.Run(),
                     "new owner-only shadow file writes and closes");
    }

    {
        TempFile shared;
        const std::string prior_shadow_magic(
            ingress::kShadowCaptureFileMagic.begin(),
            ingress::kShadowCaptureFileMagic.end());
        WriteFile(
            shared.path(),
            prior_shadow_magic);
        ingress::ByteRing first_ring(256U, 64U);
        ops::FatalLatch first_fatal;
        ingress::ShadowCaptureWriter first(
            SzTickConfig(),
            first_ring,
            first_fatal,
            shared.path());
        std::atomic<bool> first_result{false};
        std::thread first_thread([&] {
            first_result.store(
                first.Run(), std::memory_order_release);
        });
        while (!first.startup_complete()) {
            std::this_thread::yield();
        }
        test->Expect(
            first.startup_succeeded(),
            "first shadow writer owns and initializes the output");

        ingress::ByteRing second_ring(256U, 64U);
        ops::FatalLatch second_fatal;
        ingress::ShadowCaptureWriter second(
            SzTickConfig(),
            second_ring,
            second_fatal,
            shared.path());
        struct stat status {};
        test->Expect(
            second_fatal.reason() ==
                    ops::FatalReason::SHADOW_SINK_IO &&
                ::stat(shared.path().c_str(), &status) == 0 &&
                status.st_size ==
                    static_cast<off_t>(
                        sizeof(
                            ingress::ShadowCaptureFileHeaderV1)),
            "a second writer cannot lock or truncate an active shadow file");

        first.StopAndDrain();
        first_thread.join();
        test->Expect(
            first_result.load(std::memory_order_acquire) &&
                !first_fatal.tripped(),
            "the exclusive first writer remains healthy");
    }

    {
        TempFile original;
        TempFile alias;
        constexpr std::string_view sentinel = "hard-link-sentinel";
        WriteFile(original.path(), sentinel);
        if (std::remove(alias.path().c_str()) != 0 ||
            ::link(
                original.path().c_str(),
                alias.path().c_str()) != 0) {
            throw std::runtime_error(
                "creating shadow hard-link fixture failed");
        }
        ingress::ByteRing ring(256U, 64U);
        ops::FatalLatch fatal;
        ingress::ShadowCaptureWriter writer(
            SzTickConfig(), ring, fatal, alias.path());
        const std::vector<std::byte> after =
            ReadFile(original.path());
        test->Expect(
            fatal.reason() ==
                    ops::FatalReason::SHADOW_SINK_IO &&
                after.size() == sentinel.size() &&
                std::memcmp(
                    after.data(),
                    sentinel.data(),
                    sentinel.size()) == 0,
            "multiply linked shadow inode is rejected before truncation");
    }

    {
        TempFile target;
        TempFile link_holder;
        constexpr std::string_view sentinel =
            "parent-symlink-sentinel";
        WriteFile(target.path(), sentinel);
        const std::string parent_link =
            link_holder.directory() + "/redirect";
        if (::symlink(
                target.directory().c_str(),
                parent_link.c_str()) != 0) {
            throw std::runtime_error(
                "creating parent symlink fixture failed");
        }

        ingress::ByteRing ring(256U, 64U);
        ops::FatalLatch fatal;
        ingress::ShadowCaptureWriter writer(
            SzTickConfig(),
            ring,
            fatal,
            parent_link + "/capture");
        const std::vector<std::byte> after =
            ReadFile(target.path());
        const int unlink_result =
            ::unlink(parent_link.c_str());
        test->Expect(
            fatal.reason() ==
                    ops::FatalReason::SHADOW_SINK_IO &&
                unlink_result == 0 &&
                after.size() == sentinel.size() &&
                std::memcmp(
                    after.data(),
                    sentinel.data(),
                    sentinel.size()) == 0,
            "a symlinked parent component is rejected without "
            "opening or truncating its target file");
    }

    {
        TempFile ancestor;
        const std::string private_directory =
            ancestor.directory() + "/private";
        const std::string nested_capture =
            private_directory + "/capture";
        constexpr std::string_view sentinel =
            "unsafe-ancestor-sentinel";
        if (::mkdir(private_directory.c_str(), 0700) != 0) {
            throw std::runtime_error(
                "creating private descendant directory failed");
        }
        const int fixture_fd = ::open(
            nested_capture.c_str(),
            O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC,
            0600);
        if (fixture_fd < 0 || ::close(fixture_fd) != 0) {
            static_cast<void>(std::remove(
                nested_capture.c_str()));
            static_cast<void>(::rmdir(
                private_directory.c_str()));
            throw std::runtime_error(
                "creating nested shadow fixture failed");
        }
        WriteFile(nested_capture, sentinel);
        if (::chmod(ancestor.directory().c_str(), 0777) != 0) {
            static_cast<void>(std::remove(
                nested_capture.c_str()));
            static_cast<void>(::rmdir(
                private_directory.c_str()));
            throw std::runtime_error(
                "setting unsafe ancestor mode failed");
        }

        ingress::ByteRing ring(256U, 64U);
        ops::FatalLatch fatal;
        ingress::ShadowCaptureWriter writer(
            SzTickConfig(), ring, fatal, nested_capture);
        const std::vector<std::byte> after =
            ReadFile(nested_capture);
        struct stat ancestor_status {};
        const bool status_ok =
            ::stat(
                ancestor.directory().c_str(),
                &ancestor_status) == 0;
        const int restore_result =
            ::chmod(ancestor.directory().c_str(), 0700);
        const int unlink_result =
            std::remove(nested_capture.c_str());
        const int rmdir_result =
            ::rmdir(private_directory.c_str());
        test->Expect(
            fatal.reason() ==
                    ops::FatalReason::SHADOW_SINK_IO &&
                status_ok &&
                (ancestor_status.st_mode & 07777) == 0777 &&
                after.size() == sentinel.size() &&
                std::memcmp(
                    after.data(),
                    sentinel.data(),
                    sentinel.size()) == 0 &&
                restore_result == 0 &&
                unlink_result == 0 &&
                rmdir_result == 0,
            "a non-final world-writable non-sticky ancestor is "
            "rejected even when the final child directory is 0700, "
            "without changing target contents");
    }

    constexpr std::array<mode_t, 2U> unsafe_parent_modes{
        0720,
        0702,
    };
    for (const mode_t unsafe_mode : unsafe_parent_modes) {
        TempFile unsafe_parent;
        constexpr std::string_view sentinel =
            "unsafe-parent-sentinel";
        WriteFile(unsafe_parent.path(), sentinel);
        if (::chmod(
                unsafe_parent.directory().c_str(),
                unsafe_mode) != 0) {
            throw std::runtime_error(
                "setting unsafe parent mode failed");
        }

        ingress::ByteRing ring(256U, 64U);
        ops::FatalLatch fatal;
        ingress::ShadowCaptureWriter writer(
            SzTickConfig(),
            ring,
            fatal,
            unsafe_parent.path());
        const std::vector<std::byte> after =
            ReadFile(unsafe_parent.path());
        struct stat status {};
        const bool status_ok =
            ::stat(
                unsafe_parent.directory().c_str(),
                &status) == 0;
        test->Expect(
            fatal.reason() ==
                    ops::FatalReason::SHADOW_SINK_IO &&
                status_ok &&
                (status.st_mode & 07777) == unsafe_mode &&
                after.size() == sentinel.size() &&
                std::memcmp(
                    after.data(),
                    sentinel.data(),
                    sentinel.size()) == 0,
            "a group- or world-writable final parent is rejected "
            "without changing its mode or file contents");
        if (::chmod(
                unsafe_parent.directory().c_str(),
                0700) != 0) {
            throw std::runtime_error(
                "restoring safe parent mode failed");
        }
    }

    {
        TempFile lexical;
        constexpr std::string_view sentinel =
            "lexical-component-sentinel";
        WriteFile(lexical.path(), sentinel);
        const std::string child =
            lexical.directory() + "/child";
        if (::mkdir(child.c_str(), 0700) != 0) {
            throw std::runtime_error(
                "creating dot-dot fixture directory failed");
        }

        ingress::ByteRing dot_ring(256U, 64U);
        ops::FatalLatch dot_fatal;
        ingress::ShadowCaptureWriter dot_writer(
            SzTickConfig(),
            dot_ring,
            dot_fatal,
            lexical.directory() + "/./capture");
        ingress::ByteRing dot_dot_ring(256U, 64U);
        ops::FatalLatch dot_dot_fatal;
        ingress::ShadowCaptureWriter dot_dot_writer(
            SzTickConfig(),
            dot_dot_ring,
            dot_dot_fatal,
            child + "/../capture");
        const std::vector<std::byte> after =
            ReadFile(lexical.path());
        const int remove_child_result =
            ::rmdir(child.c_str());
        test->Expect(
            dot_fatal.reason() ==
                    ops::FatalReason::SHADOW_SINK_IO &&
                dot_dot_fatal.reason() ==
                    ops::FatalReason::SHADOW_SINK_IO &&
                remove_child_result == 0 &&
                after.size() == sentinel.size() &&
                std::memcmp(
                    after.data(),
                    sentinel.data(),
                    sentinel.size()) == 0,
            "dot and dot-dot path components are rejected before "
            "the existing output can be opened");
    }
}

void TestRingCorruptionFatal(TestContext* test) {
    ingress::ByteRing ring(256U, 64U);
    ops::FatalLatch fatal;
    const auto state = std::make_shared<MemoryOutputState>();
    ingress::ShadowCaptureWriter writer(
        SzTickConfig(),
        ring,
        fatal,
        std::make_unique<MemoryOutput>(state));

    const auto meta = MakeMeta(2002U, 1U);
    const auto head = MakeHead(0U, 6U, 101U, 33U, 1U);
    const std::vector<std::byte> body;
    test->Expect(
        ring.try_push_copy(meta, head, body) ==
            ingress::ByteRingPushResult::PUBLISHED,
        "corruption test publishes a valid entry first");
    ingress::ByteRingTestPeer::xor_byte(
        ring,
        sizeof(ingress::CaptureMetaV1),
        std::byte{1U});
    writer.StopAndDrain();
    test->Expect(!writer.Run(),
                 "corrupt ring entry stops the writer");
    test->Expect(
        fatal.reason() == ops::FatalReason::RING_CORRUPTION &&
            writer.Snapshot().sink_records == 0U,
        "ring corruption has a distinct fatal reason and is not counted");
}

void TestControlStatusAndMalformedBodies(TestContext* test) {
    {
        ingress::ByteRing ring(2048U, 128U);
        ops::FatalLatch fatal;
        const auto state = std::make_shared<MemoryOutputState>();
        ingress::ShadowCaptureWriter writer(
            SzTickConfig(),
            ring,
            fatal,
            std::make_unique<MemoryOutput>(state));
        const std::vector<std::byte> subscribe_body =
            MakeSubscriptionStatusBody(
                false, 0U, 0U, 0U);
        const auto subscribe_head =
            MakeHead(
                subscribe_body.size(), 2U, 101U, 23U, 1U);
        const std::vector<std::byte> old_market_body =
            std::vector<std::byte>(58U);
        const auto old_market_head =
            MakeHead(
                old_market_body.size(), 6U, 101U, 33U, 2U);
        const std::vector<std::byte> next_logon_body =
            MakeSubscriptionStatusBody(
                true, 0U, 0U, 0U, false);
        const auto next_logon_head =
            MakeHead(
                next_logon_body.size(), 2U, 101U, 2U, 3U);
        test->Expect(
            ring.try_push_copy(
                MakeMeta(2002U, 1U),
                subscribe_head,
                subscribe_body) ==
                    ingress::ByteRingPushResult::PUBLISHED &&
                ring.try_push_copy(
                    MakeMeta(2002U, 2U),
                    old_market_head,
                    old_market_body) ==
                    ingress::ByteRingPushResult::PUBLISHED &&
                ring.try_push_copy(
                    MakeMeta(2002U, 3U),
                    next_logon_head,
                    next_logon_body) ==
                    ingress::ByteRingPushResult::PUBLISHED,
            "old complete status and new partial logon are published");
        writer.StopAndDrain();
        test->Expect(
            writer.Run() && !fatal.tripped(),
            "partial new logon is structurally valid evidence");
        const ingress::ShadowCaptureStats stats = writer.Snapshot();
        test->Expect(
            stats.latest_logon_ok &&
                stats.required_subscription_ok_mask == 0x1U &&
                stats.required_subscription_failed_mask == 0U &&
                stats.readiness_generation == 0U &&
                stats.required_first_seen_mask == 0U &&
                !writer.all_required_subscriptions_ok(),
            "new LogonResponse replaces rather than inherits old "
            "connection subscription and market evidence");
    }

    {
        ingress::ByteRing ring(256U, 128U);
        ops::FatalLatch fatal;
        const auto state = std::make_shared<MemoryOutputState>();
        ingress::ShadowCaptureWriter writer(
            SzTickConfig(),
            ring,
            fatal,
            std::make_unique<MemoryOutput>(state));
        const std::vector<std::byte> body =
            MakeBody(57U, 1U);
        const auto head =
            MakeHead(
                body.size(), 6U, 101U, 33U, 1U);
        test->Expect(
            ring.try_push_copy(
                MakeMeta(2002U, 1U), head, body) ==
                ingress::ByteRingPushResult::PUBLISHED,
            "truncated fixed market body fixture is published");
        writer.StopAndDrain();
        test->Expect(
            writer.Run() && !fatal.tripped() &&
                writer.Snapshot().required_first_seen_mask == 0U &&
                !writer.all_required_market_seen(),
            "captured market body below its frozen fixed-size lower bound "
            "does not satisfy first-legal-record readiness");
    }

    {
        ingress::ByteRing ring(256U, 128U);
        ops::FatalLatch fatal;
        const auto state = std::make_shared<MemoryOutputState>();
        ingress::ShadowCaptureWriter writer(
            SzTickConfig(),
            ring,
            fatal,
            std::make_unique<MemoryOutput>(state));
        const std::vector<std::byte> body =
            MakeSubscriptionStatusBody(false, 0U, 0U, 4U);
        const auto head =
            MakeHead(body.size(), 2U, 101U, 23U, 1U);
        test->Expect(
            ring.try_push_copy(MakeMeta(2002U, 1U), head, body) ==
                ingress::ByteRingPushResult::PUBLISHED,
            "partial subscription response fixture is published");
        writer.StopAndDrain();
        test->Expect(writer.Run() && !fatal.tripped(),
                     "a well-formed rejected subscription is evidence, "
                     "not structural corruption");
        const ingress::ShadowCaptureStats stats = writer.Snapshot();
        test->Expect(
            stats.required_subscription_ok_mask == 0x1U &&
                stats.required_subscription_failed_mask == 0x2U &&
                stats.required_subscription_failure_observed_mask == 0x2U &&
                !writer.all_required_subscriptions_ok(),
            "per-message non-OK status prevents readiness");
    }

    {
        ingress::ByteRing ring(2048U, 128U);
        ops::FatalLatch fatal;
        const auto state = std::make_shared<MemoryOutputState>();
        ingress::ShadowCaptureWriter writer(
            SzTickConfig(),
            ring,
            fatal,
            std::make_unique<MemoryOutput>(state));
        const std::vector<std::byte> failed_body =
            MakeSubscriptionStatusBody(false, 0U, 0U, 4U);
        const std::vector<std::byte> recovered_body =
            MakeSubscriptionStatusBody(false, 0U, 0U, 0U);
        const std::vector<std::byte> logon_body =
            MakeSubscriptionStatusBody(true, 0U, 0U, 0U);
        const std::vector<std::byte> first_market =
            std::vector<std::byte>(58U);
        const std::vector<std::byte> second_market =
            std::vector<std::byte>(70U);
        const auto logon_head =
            MakeHead(
                logon_body.size(), 2U, 101U, 2U, 1U);
        const auto first_market_head =
            MakeHead(
                first_market.size(), 6U, 101U, 33U, 2U);
        const auto second_market_head =
            MakeHead(
                second_market.size(), 6U, 101U, 36U, 3U);
        const auto failed_head =
            MakeHead(
                failed_body.size(), 2U, 101U, 23U, 4U);
        const auto recovered_head =
            MakeHead(
                recovered_body.size(), 2U, 101U, 23U, 5U);
        test->Expect(
            ring.try_push_copy(
                MakeMeta(2002U, 1U),
                logon_head,
                logon_body) ==
                    ingress::ByteRingPushResult::PUBLISHED &&
                ring.try_push_copy(
                    MakeMeta(2002U, 2U),
                    first_market_head,
                    first_market) ==
                    ingress::ByteRingPushResult::PUBLISHED &&
                ring.try_push_copy(
                    MakeMeta(2002U, 3U),
                    second_market_head,
                    second_market) ==
                    ingress::ByteRingPushResult::PUBLISHED &&
                ring.try_push_copy(
                    MakeMeta(2002U, 4U),
                    failed_head,
                    failed_body) ==
                    ingress::ByteRingPushResult::PUBLISHED &&
                ring.try_push_copy(
                    MakeMeta(2002U, 5U),
                    recovered_head,
                    recovered_body) ==
                    ingress::ByteRingPushResult::PUBLISHED,
            "failed then recovered subscription statuses are published "
            "within one monitor interval");
        writer.StopAndDrain();
        test->Expect(writer.Run() && !fatal.tripped(),
                     "well-formed status recovery remains capturable");
        const ingress::ShadowCaptureStats stats = writer.Snapshot();
        test->Expect(
            stats.required_subscription_ok_mask == 0x3U &&
                stats.required_subscription_failed_mask == 0U &&
                stats.required_subscription_failure_observed_mask == 0x2U &&
                stats.readiness_generation == 0U &&
                writer.readiness_generation() == 0U,
            "a recovered current status cannot erase cumulative failure "
            "evidence or republish the same READY generation");
    }

    {
        ingress::ByteRing ring(2048U, 128U);
        ops::FatalLatch fatal;
        const auto state = std::make_shared<MemoryOutputState>();
        ingress::ShadowCaptureWriter writer(
            SzTickConfig(),
            ring,
            fatal,
            std::make_unique<MemoryOutput>(state));
        const std::vector<std::byte> logon_body =
            MakeSubscriptionStatusBody(true, 0U, 0U, 0U);
        const std::vector<std::byte> first_market =
            std::vector<std::byte>(58U);
        const std::vector<std::byte> second_market =
            std::vector<std::byte>(70U);
        bool published = true;
        for (std::uint64_t generation = 0U;
             generation < 2U;
             ++generation) {
            const std::uint64_t base = generation * 3U;
            const auto logon_head =
                MakeHead(
                    logon_body.size(),
                    2U,
                    101U,
                    2U,
                    base + 1U);
            const auto first_market_head =
                MakeHead(
                    first_market.size(),
                    6U,
                    101U,
                    33U,
                    base + 2U);
            const auto second_market_head =
                MakeHead(
                    second_market.size(),
                    6U,
                    101U,
                    36U,
                    base + 3U);
            published =
                published &&
                ring.try_push_copy(
                    MakeMeta(2002U, base + 1U),
                    logon_head,
                    logon_body) ==
                    ingress::ByteRingPushResult::PUBLISHED &&
                ring.try_push_copy(
                    MakeMeta(2002U, base + 2U),
                    first_market_head,
                    first_market) ==
                    ingress::ByteRingPushResult::PUBLISHED &&
                ring.try_push_copy(
                    MakeMeta(2002U, base + 3U),
                    second_market_head,
                    second_market) ==
                    ingress::ByteRingPushResult::PUBLISHED;
        }
        test->Expect(
            published,
            "two complete successful logon generations are published");
        writer.StopAndDrain();
        test->Expect(writer.Run() && !fatal.tripped(),
                     "two complete logon generations remain valid capture");
        const ingress::ShadowCaptureStats stats = writer.Snapshot();
        test->Expect(
            stats.logon_response_headers == 2U &&
                stats.logon_ok_responses == 2U &&
                stats.readiness_generation == 2U &&
                writer.readiness_generation() == 2U,
            "replacement logon publishes a distinct ready generation "
            "even when it fully recovers before monitoring");
    }

    {
        ingress::ByteRing ring(256U, 128U);
        ops::FatalLatch fatal;
        const auto state = std::make_shared<MemoryOutputState>();
        ingress::ShadowCaptureWriter writer(
            SzTickConfig(),
            ring,
            fatal,
            std::make_unique<MemoryOutput>(state));
        const std::vector<std::byte> body =
            MakeSubscriptionStatusBody(true, 4U, 0U, 0U);
        const auto head =
            MakeHead(body.size(), 2U, 101U, 2U, 1U);
        test->Expect(
            ring.try_push_copy(MakeMeta(2002U, 1U), head, body) ==
                ingress::ByteRingPushResult::PUBLISHED,
            "failed logon response fixture is published");
        writer.StopAndDrain();
        test->Expect(writer.Run() && !fatal.tripped(),
                     "well-formed failed logon remains capturable evidence");
        const ingress::ShadowCaptureStats stats = writer.Snapshot();
        test->Expect(
            stats.logon_failed_responses == 1U &&
                stats.logon_ok_responses == 0U &&
                !stats.latest_logon_ok &&
                stats.required_subscription_ok_mask == 0U &&
                !writer.latest_logon_ok(),
            "failed logon never applies embedded subscription statuses");
    }

    {
        ingress::ByteRing ring(256U, 128U);
        ops::FatalLatch fatal;
        const auto state = std::make_shared<MemoryOutputState>();
        ingress::ShadowCaptureWriter writer(
            SzTickConfig(),
            ring,
            fatal,
            std::make_unique<MemoryOutput>(state));
        std::vector<std::byte> body =
            MakeSubscriptionStatusBody(
                true, 0U, 0U, 0U);
        // Relative offset 8 begins at ReturnCode instead of after the
        // complete 24-byte LogonResponse fixed portion.
        StoreU32(8U, body.data() + 16U);
        const auto head =
            MakeHead(
                body.size(), 2U, 101U, 2U, 1U);
        test->Expect(
            ring.try_push_copy(
                MakeMeta(2002U, 1U), head, body) ==
                ingress::ByteRingPushResult::PUBLISHED,
            "overlapping logon services fixture is published");
        writer.StopAndDrain();
        test->Expect(
            !writer.Run() &&
                fatal.reason() ==
                    ops::FatalReason::MALFORMED_CONTROL_MESSAGE,
            "LogonResponse list cannot overlap its ReturnCode field");
    }

    {
        ingress::ByteRing ring(256U, 128U);
        ops::FatalLatch fatal;
        const auto state = std::make_shared<MemoryOutputState>();
        ingress::ShadowCaptureWriter writer(
            SzTickConfig(),
            ring,
            fatal,
            std::make_unique<MemoryOutput>(state));
        const std::vector<std::byte> body =
            MakeSubscriptionStatusBody(
                true, 0U, 0U, 0U);
        const auto head =
            MakeHead(
                body.size(), 2U, 102U, 2U, 1U);
        test->Expect(
            ring.try_push_copy(
                MakeMeta(2002U, 1U), head, body) ==
                ingress::ByteRingPushResult::PUBLISHED,
            "wrong-version logon fixture is published");
        writer.StopAndDrain();
        test->Expect(
            !writer.Run() &&
                fatal.reason() ==
                    ops::FatalReason::MALFORMED_CONTROL_MESSAGE &&
                !writer.latest_logon_ok(),
            "a control message ID under an unknown SYS schema "
            "fails closed and clears logon readiness");
    }

    {
        ingress::ByteRing ring(256U, 64U);
        ops::FatalLatch fatal;
        const auto state = std::make_shared<MemoryOutputState>();
        ingress::ShadowCaptureWriter writer(
            SzTickConfig(),
            ring,
            fatal,
            std::make_unique<MemoryOutput>(state));
        const std::vector<std::byte> body(1U, std::byte{0U});
        const auto head =
            MakeHead(body.size(), 2U, 101U, 2U, 1U);
        test->Expect(
            ring.try_push_copy(MakeMeta(2002U, 1U), head, body) ==
                ingress::ByteRingPushResult::PUBLISHED,
            "truncated logon response fixture is published");
        writer.StopAndDrain();
        test->Expect(
            !writer.Run() &&
                fatal.reason() ==
                    ops::FatalReason::MALFORMED_CONTROL_MESSAGE,
            "truncated SYS body is bounded and fail-stop");
        const ingress::ShadowCaptureStats stats = writer.Snapshot();
        test->Expect(
            stats.sink_records == 1U &&
                stats.malformed_control_responses == 1U,
            "fully written malformed evidence is counted before stop");
    }
}

void TestConfigurationGuards(TestContext* test) {
    ingress::ByteRing ring(256U, 64U);
    ops::FatalLatch fatal;
    const auto state = std::make_shared<MemoryOutputState>();
    ingress::ShadowCaptureConfig duplicate = SzTickConfig();
    duplicate.required_market_messages.push_back(
        duplicate.required_market_messages.front());
    bool rejected = false;
    try {
        ingress::ShadowCaptureWriter writer(
            std::move(duplicate),
            ring,
            fatal,
            std::make_unique<MemoryOutput>(state));
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    test->Expect(rejected,
                 "duplicate required mask keys are rejected");

    ingress::ShadowCaptureConfig empty = SzTickConfig();
    empty.required_market_messages.clear();
    rejected = false;
    try {
        ingress::ShadowCaptureWriter writer(
            std::move(empty),
            ring,
            fatal,
            std::make_unique<MemoryOutput>(state));
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    test->Expect(rejected,
                 "an empty required subscription set is rejected");
}

}  // namespace

int main() {
    TestContext test;
    try {
        TestRequiredMarketBodyValidation(&test);
        TestFileWrapZeroAndObservation(&test);
        TestEmptyDrainAndShortWrite(&test);
        TestConcurrentStopRaceAndAccounting(&test);
        TestInjectedIoFailures(&test);
        TestPosixOutputPolicy(&test);
        TestRingCorruptionFatal(&test);
        TestControlStatusAndMalformedBodies(&test);
        TestConfigurationGuards(&test);
    } catch (const std::exception& error) {
        std::cerr << "UNCAUGHT: " << error.what() << '\n';
        return 2;
    }

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " phase-1 shadow test(s) failed\n";
        return 1;
    }
    std::cout << "phase-1 shadow tests passed\n";
    return 0;
}
