#include "l2flow/common/crc32c.h"
#include "l2flow/realtime/optional_wal_sink_v1.h"
#include "l2flow/realtime/owned_ingress_message_v1.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <stdlib.h>
#include <sys/stat.h>

namespace mdl = datayes::mdl;
namespace realtime = l2flow::realtime;
namespace sdk = l2flow::sdk;

namespace {

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
        head_.HeadSize = static_cast<std::uint8_t>(
            sdk::kVendorHeadBytes);
        head_.MessageSize = static_cast<std::uint32_t>(
            sdk::kVendorHeadBytes + body_.size());
        head_.MessageEncoding =
            static_cast<std::uint8_t>(mdl::MDLEID_BINARY);
        head_.ServiceID = key.service_id;
        head_.ServiceVersion = key.service_version;
        head_.MessageID = key.message_id;
        head_.SequenceID = 9988U;
    }

    void AddRef() override {}
    int ReleaseRef() override { return 1; }

    mdl::MDLMessageHead* GetHead() const override {
        head_calls.fetch_add(1U, std::memory_order_relaxed);
        if (throw_head) {
            throw 1;
        }
        return null_head
                   ? nullptr
                   : const_cast<mdl::MDLMessageHead*>(&head_);
    }

    char* GetBody() const override {
        body_calls.fetch_add(1U, std::memory_order_relaxed);
        if (throw_body) {
            throw 1;
        }
        if (null_body || body_.empty()) {
            return nullptr;
        }
        return reinterpret_cast<char*>(
            const_cast<std::byte*>(body_.data()));
    }

    mdl::MDLMessage* _Copy() const override {
        copy_calls.fetch_add(1U, std::memory_order_relaxed);
        return nullptr;
    }

    [[nodiscard]] mdl::MDLMessageHead& head() noexcept { return head_; }
    [[nodiscard]] std::vector<std::byte>& body() noexcept { return body_; }

    mutable std::atomic<std::uint64_t> head_calls{0U};
    mutable std::atomic<std::uint64_t> body_calls{0U};
    mutable std::atomic<std::uint64_t> copy_calls{0U};
    bool null_head = false;
    bool null_body = false;
    bool throw_head = false;
    bool throw_body = false;

private:
    mdl::MDLMessageHead head_{};
    std::vector<std::byte> body_;
};

[[nodiscard]] realtime::OwnedIngressMetadataV1 Metadata(
    std::uint64_t global_sequence,
    std::uint64_t source_sequence) {
    realtime::OwnedIngressMetadataV1 result{};
    result.run_id[0U] = std::byte{0x51U};
    result.run_id[15U] = std::byte{0xa5U};
    result.global_ingress_sequence = global_sequence;
    result.source_sequence = source_sequence;
    result.recv_realtime_ns = 1'721'800'000'000'000'000ULL +
                              global_sequence;
    result.recv_monotonic_ns = 123'000U + global_sequence;
    return result;
}

[[nodiscard]] realtime::OwnedIngressMessageHandleV1 MakeOwned(
    TestContext* test,
    realtime::OwnedIngressMessagePoolV1* pool,
    sdk::MessageKey key,
    std::vector<std::byte> body,
    std::uint64_t global_sequence,
    std::uint64_t source_sequence) {
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
                  Metadata(global_sequence, source_sequence),
                  &result)
            : inspect_error;
    test->Expect(
        inspect_error ==
                realtime::OwnedIngressMessageErrorV1::kNone &&
            acquire_error ==
                realtime::OwnedIngressMessageErrorV1::kNone &&
            static_cast<bool>(result),
        "owned message helper succeeds");
    return result;
}

[[nodiscard]] std::unique_ptr<realtime::OwnedIngressMessagePoolV1>
MakePool(
    TestContext* test,
    std::size_t maximum_inflight_messages = 64U) {
    std::unique_ptr<realtime::OwnedIngressMessagePoolV1> result;
    const realtime::OwnedIngressMessageErrorV1 error =
        realtime::OwnedIngressMessagePoolV1::Create(
            realtime::OwnedIngressMessagePoolConfigV1{
                4096U, maximum_inflight_messages},
            &result);
    test->Expect(
        error == realtime::OwnedIngressMessageErrorV1::kNone &&
            result != nullptr,
        "owned ingress pool helper succeeds");
    return result;
}

[[nodiscard]] realtime::OwnedIngressMessageInspectionV1 Inspect(
    TestContext* test,
    const mdl::MDLMessage* message) {
    realtime::OwnedIngressMessageInspectionV1 result;
    const realtime::OwnedIngressMessageErrorV1 error =
        realtime::InspectOwnedIngressMessageV1(
            message, 4096U, &result);
    test->Expect(
        error == realtime::OwnedIngressMessageErrorV1::kNone &&
            static_cast<bool>(result),
        "owned message inspection helper succeeds");
    return result;
}

class TemporaryDirectory final {
public:
    TemporaryDirectory() {
        std::array<char, 64U> pattern{};
        const char literal[] = "/tmp/l2flow-owned-wal-v1-XXXXXX";
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

[[nodiscard]] std::uint16_t LoadU16Little(
    std::span<const std::byte> bytes,
    std::size_t offset) {
    return static_cast<std::uint16_t>(
        std::to_integer<std::uint16_t>(bytes[offset]) |
        static_cast<std::uint16_t>(
            std::to_integer<std::uint16_t>(bytes[offset + 1U]) << 8U));
}

[[nodiscard]] std::uint32_t LoadU32Little(
    std::span<const std::byte> bytes,
    std::size_t offset) {
    std::uint32_t result = 0U;
    for (std::size_t index = 0U; index < 4U; ++index) {
        result |= std::to_integer<std::uint32_t>(bytes[offset + index])
                  << static_cast<unsigned int>(index * 8U);
    }
    return result;
}

[[nodiscard]] std::uint64_t LoadU64Little(
    std::span<const std::byte> bytes,
    std::size_t offset) {
    std::uint64_t result = 0U;
    for (std::size_t index = 0U; index < 8U; ++index) {
        result |= std::to_integer<std::uint64_t>(bytes[offset + index])
                  << static_cast<unsigned int>(index * 8U);
    }
    return result;
}

void CheckRequiredClassification(TestContext* test) {
    constexpr std::array<realtime::OwnedIngressSourceV1,
                         realtime::kRequiredOwnedIngressMessageCountV1>
        expected = {{
            realtime::OwnedIngressSourceV1::kShanghaiSnapshot,
            realtime::OwnedIngressSourceV1::kShanghaiTick,
            realtime::OwnedIngressSourceV1::kShenzhenSnapshot,
            realtime::OwnedIngressSourceV1::kShenzhenTick,
            realtime::OwnedIngressSourceV1::kShenzhenTick,
        }};
    for (std::size_t index = 0U;
         index < realtime::kRequiredOwnedIngressMessageKeysV1.size();
         ++index) {
        realtime::OwnedIngressSourceV1 source{};
        test->Expect(
            realtime::ClassifyOwnedIngressMessageKeyV1(
                realtime::kRequiredOwnedIngressMessageKeysV1[index],
                &source) == realtime::OwnedIngressKeyErrorV1::kNone &&
                source == expected[index],
            "each of the five required keys has the fixed source");
    }

    realtime::OwnedIngressSourceV1 unchanged =
        realtime::OwnedIngressSourceV1::kShanghaiTick;
    test->Expect(
        realtime::ClassifyOwnedIngressMessageKeyV1(
            realtime::kForbiddenShenzhenCombinedTickKeyV1,
            &unchanged) ==
            realtime::OwnedIngressKeyErrorV1::kForbiddenCombinedTick &&
            unchanged == realtime::OwnedIngressSourceV1::kShanghaiTick,
        "6.101.53 is explicitly forbidden and produces no source");
    test->Expect(
        realtime::ClassifyOwnedIngressMessageKeyV1(
            sdk::MessageKey{6U, 101U, 29U},
            &unchanged) ==
            realtime::OwnedIngressKeyErrorV1::kUnsupported,
        "a sixth market key is not admitted");
}

void CheckOwnedLifetimeAndValidation(TestContext* test) {
    auto pool = MakePool(test, 8U);
    if (pool == nullptr) {
        return;
    }

    FakeMessage message(
        sdk::MessageKey{6U, 101U, 33U},
        {std::byte{0x11U}, std::byte{0x22U}, std::byte{0x33U}});
    const realtime::OwnedIngressMetadataV1 metadata = Metadata(17U, 8U);
    realtime::OwnedIngressMessageInspectionV1 inspection;
    test->Expect(
        realtime::InspectOwnedIngressMessageV1(
            &message, 4096U, &inspection) ==
                realtime::OwnedIngressMessageErrorV1::kNone &&
            static_cast<bool>(inspection) &&
            inspection.source() ==
                realtime::OwnedIngressSourceV1::kShenzhenTick &&
            inspection.wire_size() ==
                sdk::kVendorHeadBytes + message.body().size(),
        "inspection validates and classifies one callback view");
    realtime::OwnedIngressMessageHandleV1 owned;
    test->Expect(
        pool->Acquire(inspection, metadata, &owned) ==
                realtime::OwnedIngressMessageErrorV1::kNone &&
            static_cast<bool>(owned),
        "pool acquire crosses the ownership boundary");
    test->Expect(
        static_cast<bool>(owned) &&
            owned->source() ==
                realtime::OwnedIngressSourceV1::kShenzhenTick &&
            owned->global_ingress_sequence() == 17U &&
            owned->source_sequence() == 8U &&
            owned->recv_realtime_ns() == metadata.recv_realtime_ns &&
            owned->recv_monotonic_ns() == metadata.recv_monotonic_ns,
        "owned message retains process sequence and receive clocks");
    test->Expect(
        message.head_calls.load(std::memory_order_relaxed) == 1U &&
            message.body_calls.load(std::memory_order_relaxed) == 1U &&
            message.copy_calls.load(std::memory_order_relaxed) == 0U,
        "inspect then acquire reads SDK head/body once and never calls Copy");
    test->Expect(
        owned->body().data() ==
            reinterpret_cast<const std::byte*>(owned.get()) +
                sizeof(realtime::OwnedIngressMessageV1),
        "message object and body occupy one contiguous pool block");

    message.body()[0U] = std::byte{0xffU};
    message.head().SequenceID = 1U;
    test->Expect(
        owned->body()[0U] == std::byte{0x11U} &&
            owned->vendor_head().sequence_id() == 9988U,
        "owned bytes do not alias vendor callback lifetime");

    realtime::OwnedIngressMessageHandleV1 copied = owned;
    realtime::OwnedIngressMessageHandleV1 moved = std::move(copied);
    realtime::OwnedIngressMessageHandleV1 assigned;
    assigned = moved;
    test->Expect(
        !copied && moved.get() == owned.get() &&
            assigned.get() == owned.get(),
        "intrusive handle copy and move retain exact object identity");
    test->Expect(
        pool->Snapshot().active_messages == 1U,
        "handle copies do not create additional pooled messages");

    FakeMessage combined(
        realtime::kForbiddenShenzhenCombinedTickKeyV1,
        {std::byte{1U}});
    realtime::OwnedIngressMessageInspectionV1 rejected = inspection;
    test->Expect(
        realtime::InspectOwnedIngressMessageV1(
            &combined, 4096U, &rejected) ==
                realtime::OwnedIngressMessageErrorV1::
                    kForbiddenCombinedTick &&
            !rejected &&
            combined.body_calls.load(std::memory_order_relaxed) == 0U,
        "forbidden combined tick is rejected before body access");

    FakeMessage unsupported(
        sdk::MessageKey{6U, 101U, 29U},
        {std::byte{1U}});
    unsupported.head().HeadSize = 0U;
    unsupported.head().MessageSize = 1U;
    unsupported.head().MessageEncoding =
        static_cast<std::uint8_t>(mdl::MDLEID_FAST);
    test->Expect(
        realtime::InspectOwnedIngressMessageV1(
            &unsupported, 4096U, &rejected) ==
                realtime::OwnedIngressMessageErrorV1::
                    kUnsupportedMessage &&
            unsupported.body_calls.load(std::memory_order_relaxed) == 0U,
        "unsupported tuple is ignored before production schema validation");

    FakeMessage wrong_head(
        sdk::MessageKey{4U, 101U, 24U},
        {std::byte{1U}});
    wrong_head.head().HeadSize = 22U;
    test->Expect(
        realtime::InspectOwnedIngressMessageV1(
            &wrong_head, 4096U, &rejected) ==
            realtime::OwnedIngressMessageErrorV1::kWrongHeadSize,
        "declared vendor HeadSize is validated");

    FakeMessage smaller(
        sdk::MessageKey{4U, 101U, 24U},
        {std::byte{1U}});
    smaller.head().MessageSize = 1U;
    test->Expect(
        realtime::InspectOwnedIngressMessageV1(
            &smaller, 4096U, &rejected) ==
            realtime::OwnedIngressMessageErrorV1::
                kMessageSmallerThanHead,
        "declared MessageSize cannot underflow body length");

    FakeMessage wrong_encoding(
        sdk::MessageKey{4U, 101U, 24U},
        {std::byte{1U}});
    wrong_encoding.head().MessageEncoding =
        static_cast<std::uint8_t>(mdl::MDLEID_FAST);
    test->Expect(
        realtime::InspectOwnedIngressMessageV1(
            &wrong_encoding, 4096U, &rejected) ==
            realtime::OwnedIngressMessageErrorV1::
                kUnexpectedMessageEncoding &&
            wrong_encoding.body_calls.load(std::memory_order_relaxed) == 0U,
        "non-binary vendor encoding is rejected before body decoding");

    FakeMessage null_body(
        sdk::MessageKey{4U, 101U, 24U},
        {std::byte{1U}});
    null_body.null_body = true;
    test->Expect(
        realtime::InspectOwnedIngressMessageV1(
            &null_body, 4096U, &rejected) ==
            realtime::OwnedIngressMessageErrorV1::kNullBody,
        "non-empty declared body requires a vendor body pointer");

    FakeMessage zero_body(
        sdk::MessageKey{4U, 101U, 24U}, {});
    test->Expect(
        realtime::InspectOwnedIngressMessageV1(
            &zero_body, 4096U, &rejected) ==
                realtime::OwnedIngressMessageErrorV1::kNone &&
            rejected.body().empty() &&
            zero_body.body_calls.load(std::memory_order_relaxed) == 0U,
        "empty message body requires no SDK body access");

    FakeMessage too_large(
        sdk::MessageKey{4U, 101U, 24U},
        {std::byte{1U}});
    test->Expect(
        realtime::InspectOwnedIngressMessageV1(
            &too_large,
            static_cast<std::uint32_t>(sdk::kVendorHeadBytes),
            &rejected) ==
                realtime::OwnedIngressMessageErrorV1::kMessageTooLarge &&
            too_large.body_calls.load(std::memory_order_relaxed) == 0U,
        "message limit rejects before SDK body access");

    FakeMessage null_head(
        sdk::MessageKey{4U, 101U, 24U},
        {std::byte{1U}});
    null_head.null_head = true;
    test->Expect(
        realtime::InspectOwnedIngressMessageV1(
            &null_head, 4096U, &rejected) ==
            realtime::OwnedIngressMessageErrorV1::kNullHead,
        "null SDK head is rejected");

    FakeMessage throwing_head(
        sdk::MessageKey{4U, 101U, 24U},
        {std::byte{1U}});
    throwing_head.throw_head = true;
    test->Expect(
        realtime::InspectOwnedIngressMessageV1(
            &throwing_head, 4096U, &rejected) ==
            realtime::OwnedIngressMessageErrorV1::kSdkAccess,
        "SDK head exception cannot cross the callback boundary");

    FakeMessage throwing_body(
        sdk::MessageKey{4U, 101U, 24U},
        {std::byte{1U}});
    throwing_body.throw_body = true;
    test->Expect(
        realtime::InspectOwnedIngressMessageV1(
            &throwing_body, 4096U, &rejected) ==
            realtime::OwnedIngressMessageErrorV1::kSdkAccess,
        "SDK body exception cannot cross the callback boundary");

    test->Expect(
        realtime::InspectOwnedIngressMessageV1(
            nullptr, 4096U, &rejected) ==
                realtime::OwnedIngressMessageErrorV1::kNullMessage &&
            realtime::InspectOwnedIngressMessageV1(
                &message, 4096U, nullptr) ==
                realtime::OwnedIngressMessageErrorV1::kNullOutput,
        "inspection rejects null message and output pointers");

    test->Expect(
        realtime::InspectOwnedIngressMessageV1(
            &message,
            static_cast<std::uint32_t>(
                sdk::kVendorHeadBytes - 1U),
            &rejected) ==
            realtime::OwnedIngressMessageErrorV1::
                kInvalidMaximumMessageBytes,
        "inspection limit cannot be smaller than the vendor head");
    test->Expect(
        realtime::InspectOwnedIngressMessageV1(
            &message,
            realtime::kOwnedIngressMaximumMessageBytesV1 + 1U,
            &rejected) ==
            realtime::OwnedIngressMessageErrorV1::
                kInvalidMaximumMessageBytes,
        "inspection limit has an explicit hard upper bound");

    realtime::OwnedIngressMetadataV1 invalid_metadata = metadata;
    invalid_metadata.run_id = {};
    realtime::OwnedIngressMessageHandleV1 rejected_owned = owned;
    test->Expect(
        pool->Acquire(
            inspection, invalid_metadata, &rejected_owned) ==
                realtime::OwnedIngressMessageErrorV1::kInvalidMetadata &&
            !rejected_owned,
        "zero run identity cannot enter ownership and clears output");

    invalid_metadata = metadata;
    invalid_metadata.global_ingress_sequence =
        std::numeric_limits<std::uint64_t>::max();
    test->Expect(
        pool->Acquire(
            inspection, invalid_metadata, &rejected_owned) ==
            realtime::OwnedIngressMessageErrorV1::kInvalidMetadata,
        "sequence exhaustion sentinel is never published as a message");

    realtime::OwnedIngressMessageInspectionV1 invalid_inspection;
    test->Expect(
        pool->Acquire(
            invalid_inspection, metadata, &rejected_owned) ==
                realtime::OwnedIngressMessageErrorV1::
                    kInvalidInspection &&
            !rejected_owned,
        "pool cannot acquire a default or failed inspection");
}

void CheckPoolBoundsReuseAndRetiredLifetime(TestContext* test) {
    std::unique_ptr<realtime::OwnedIngressMessagePoolV1> invalid_pool;
    test->Expect(
        realtime::OwnedIngressMessagePoolV1::Create(
            realtime::OwnedIngressMessagePoolConfigV1{
                static_cast<std::uint32_t>(
                    sdk::kVendorHeadBytes - 1U),
                1U},
            &invalid_pool) ==
            realtime::OwnedIngressMessageErrorV1::
                kInvalidPoolConfiguration,
        "pool rejects a maximum smaller than the vendor head");
    test->Expect(
        realtime::OwnedIngressMessagePoolV1::Create(
            realtime::OwnedIngressMessagePoolConfigV1{
                realtime::kOwnedIngressMaximumMessageBytesV1 + 1U,
                1U},
            &invalid_pool) ==
            realtime::OwnedIngressMessageErrorV1::
                kInvalidPoolConfiguration,
        "pool rejects a message bound above the hard maximum");
    test->Expect(
        realtime::OwnedIngressMessagePoolV1::Create(
            realtime::OwnedIngressMessagePoolConfigV1{4096U, 0U},
            &invalid_pool) ==
            realtime::OwnedIngressMessageErrorV1::
                kInvalidPoolConfiguration,
        "pool requires a nonzero inflight bound");
    test->Expect(
        realtime::OwnedIngressMessagePoolV1::Create(
            realtime::OwnedIngressMessagePoolConfigV1{
                4096U,
                realtime::kOwnedIngressMaximumInflightMessagesV1 +
                    1U},
            &invalid_pool) ==
            realtime::OwnedIngressMessageErrorV1::
                kInvalidPoolConfiguration,
        "pool inflight count has an explicit hard upper bound");

    auto pool = MakePool(test, 2U);
    if (pool == nullptr) {
        return;
    }
    FakeMessage first_message(
        sdk::MessageKey{4U, 101U, 4U},
        {std::byte{0x31U}});
    FakeMessage second_message(
        sdk::MessageKey{4U, 101U, 24U},
        {std::byte{0x32U}});
    FakeMessage third_message(
        sdk::MessageKey{6U, 101U, 28U},
        {std::byte{0x33U}});
    const auto first_inspection = Inspect(test, &first_message);
    const auto second_inspection = Inspect(test, &second_message);
    const auto third_inspection = Inspect(test, &third_message);

    realtime::OwnedIngressMessageHandleV1 first;
    realtime::OwnedIngressMessageHandleV1 second;
    realtime::OwnedIngressMessageHandleV1 third;
    test->Expect(
        pool->Acquire(first_inspection, Metadata(1U, 1U), &first) ==
                realtime::OwnedIngressMessageErrorV1::kNone &&
            pool->Acquire(second_inspection, Metadata(2U, 1U), &second) ==
                realtime::OwnedIngressMessageErrorV1::kNone,
        "pool admits messages up to its inflight bound");
    if (!first || !second) {
        return;
    }
    test->Expect(
        pool->Acquire(third_inspection, Metadata(3U, 1U), &third) ==
                realtime::OwnedIngressMessageErrorV1::kPoolExhausted &&
            !third,
        "pool deterministically rejects beyond its inflight bound");
    const realtime::OwnedIngressMessagePoolSnapshotV1 full_snapshot =
        pool->Snapshot();
    test->Expect(
        full_snapshot.active_messages == 2U &&
            full_snapshot.allocated_blocks == 2U &&
            full_snapshot.cached_blocks == 0U &&
            full_snapshot.allocated_blocks <=
                full_snapshot.maximum_inflight_messages,
        "pool snapshot proves bounded live and allocated block counts");

    const realtime::OwnedIngressMessageV1* const first_address =
        first.get();
    first.reset();
    test->Expect(
        pool->Snapshot().cached_blocks == 1U,
        "final handle release returns a live pool block to its class");
    test->Expect(
        pool->Acquire(third_inspection, Metadata(3U, 1U), &third) ==
                realtime::OwnedIngressMessageErrorV1::kNone &&
            third.get() == first_address &&
            pool->Snapshot().allocated_blocks == 2U,
        "a matching cached size class is reused without new allocation");
    if (!third) {
        return;
    }

    realtime::OwnedIngressMessageHandleV1 survivor = second;
    second.reset();
    third.reset();
    pool.reset();
    test->Expect(
        survivor->body()[0U] == std::byte{0x32U},
        "live intrusive handles survive destruction of the pool wrapper");
    std::thread final_releaser(
        [owned = std::move(survivor)]() mutable {
            owned.reset();
        });
    final_releaser.join();

    constexpr std::size_t release_thread_count = 16U;
    auto race_pool = MakePool(test, release_thread_count);
    if (race_pool == nullptr) {
        return;
    }
    FakeMessage race_message(
        sdk::MessageKey{6U, 101U, 36U},
        {std::byte{0x71U}});
    const auto race_inspection = Inspect(test, &race_message);
    std::array<realtime::OwnedIngressMessageHandleV1,
               release_thread_count>
        release_handles{};
    for (std::size_t index = 0U;
         index < release_handles.size();
         ++index) {
        test->Expect(
            race_pool->Acquire(
                race_inspection,
                Metadata(index + 10U, index + 1U),
                &release_handles[index]) ==
                realtime::OwnedIngressMessageErrorV1::kNone,
            "retirement race message acquisition succeeds");
    }
    std::atomic<bool> release_now{false};
    std::atomic<std::size_t> completed_releases{0U};
    std::vector<std::thread> releasers;
    releasers.reserve(release_thread_count);
    for (auto& handle : release_handles) {
        releasers.emplace_back(
            [owned = std::move(handle),
             &release_now,
             &completed_releases]() mutable {
                while (!release_now.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                owned.reset();
                completed_releases.fetch_add(
                    1U, std::memory_order_release);
            });
    }
    std::thread retire_pool(
        [&race_pool, &release_now] {
            while (!release_now.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            race_pool.reset();
        });
    release_now.store(true, std::memory_order_release);
    for (std::thread& releaser : releasers) {
        releaser.join();
    }
    retire_pool.join();
    test->Expect(
        completed_releases.load(std::memory_order_acquire) ==
                release_thread_count &&
            race_pool == nullptr,
        "multithread final release safely races pool retirement");
}

void CheckDisabledAndFailedWalAreSideBranches(TestContext* test) {
    auto pool = MakePool(test, 4U);
    if (pool == nullptr) {
        return;
    }
    const auto decoder_handle = MakeOwned(
        test,
        pool.get(),
        sdk::MessageKey{4U, 101U, 4U},
        {std::byte{0x41U}},
        1U,
        1U);

    std::unique_ptr<realtime::OptionalWalSinkV1> disabled;
    test->Expect(
        realtime::OptionalWalSinkV1::Create({}, &disabled) ==
            realtime::OptionalWalCreateErrorV1::kNone &&
            disabled != nullptr,
        "explicitly disabled WAL creates a no-op side branch");
    test->Expect(
        disabled->TryEnqueue(decoder_handle) ==
            realtime::OptionalWalEnqueueResultV1::kDisabled &&
            decoder_handle->body()[0U] == std::byte{0x41U},
        "disabled WAL does not consume or alter decoder ownership");
    disabled->StopAndDrain();
    const realtime::OptionalWalSnapshotV1 disabled_snapshot =
        disabled->Snapshot();
    test->Expect(
        !disabled_snapshot.enabled && disabled_snapshot.finished &&
            !disabled_snapshot.coverage_lost &&
            disabled_snapshot.accepted_records == 0U,
        "disabled WAL is not mislabeled as lost coverage");

    TemporaryDirectory temporary;
    realtime::OptionalWalSinkConfigV1 failed_config{};
    failed_config.enabled = true;
    failed_config.path =
        (temporary.path() / "missing" / "realtime.wal").string();
    failed_config.queue_capacity = 4U;
    std::unique_ptr<realtime::OptionalWalSinkV1> failed;
    test->Expect(
        realtime::OptionalWalSinkV1::Create(
            failed_config, &failed) ==
            realtime::OptionalWalCreateErrorV1::kNone &&
            failed != nullptr,
        "optional WAL path failure does not fail sink construction");
    const realtime::OptionalWalSnapshotV1 initial = failed->Snapshot();
    test->Expect(
        initial.failed() && initial.coverage_lost && initial.finished &&
            initial.failure_kind ==
                realtime::OptionalWalFailureKindV1::kOpen,
        "WAL open failure is sticky observable side-branch state");
    test->Expect(
        failed->TryEnqueue(decoder_handle) ==
            realtime::OptionalWalEnqueueResultV1::kWriterFailed &&
            decoder_handle->body()[0U] == std::byte{0x41U},
        "failed WAL cannot block or invalidate realtime decoder data");
    failed->StopAndDrain();

    const std::filesystem::path fifo_path = temporary.path() / "wal-fifo";
    test->Expect(
        ::mkfifo(fifo_path.c_str(), 0600) == 0,
        "nonregular WAL test creates a FIFO");
    realtime::OptionalWalSinkConfigV1 fifo_config{};
    fifo_config.enabled = true;
    fifo_config.path = fifo_path.string();
    fifo_config.queue_capacity = 2U;
    fifo_config.replace_existing = true;
    std::unique_ptr<realtime::OptionalWalSinkV1> fifo_sink;
    test->Expect(
        realtime::OptionalWalSinkV1::Create(
            fifo_config, &fifo_sink) ==
                realtime::OptionalWalCreateErrorV1::kNone &&
            fifo_sink != nullptr &&
            fifo_sink->Snapshot().failure_kind ==
                realtime::OptionalWalFailureKindV1::kNotRegularFile,
        "replacement mode rejects a FIFO without blocking or truncating it");
    struct stat fifo_status {};
    test->Expect(
        ::stat(fifo_path.c_str(), &fifo_status) == 0 &&
            S_ISFIFO(fifo_status.st_mode),
        "failed WAL open leaves the nonregular target intact");
    if (fifo_sink != nullptr) {
        fifo_sink->StopAndDrain();
    }
}

void CheckWalFramingAndDrain(TestContext* test) {
    TemporaryDirectory temporary;
    test->Expect(
        !temporary.path().empty(),
        "temporary WAL directory is available");
    if (temporary.path().empty()) {
        return;
    }
    auto pool = MakePool(test, 8U);
    if (pool == nullptr) {
        return;
    }

    realtime::OptionalWalSinkConfigV1 config{};
    config.enabled = true;
    config.path = (temporary.path() / "realtime.wal").string();
    config.queue_capacity = 8U;
    std::unique_ptr<realtime::OptionalWalSinkV1> sink;
    test->Expect(
        realtime::OptionalWalSinkV1::Create(config, &sink) ==
            realtime::OptionalWalCreateErrorV1::kNone &&
            sink != nullptr && sink->Snapshot().accepting,
        "enabled optional WAL starts its independent writer");
    if (sink == nullptr || !sink->Snapshot().accepting) {
        return;
    }

    auto first = MakeOwned(
        test,
        pool.get(),
        sdk::MessageKey{4U, 101U, 24U},
        {std::byte{0x10U}, std::byte{0x11U}},
        11U,
        7U);
    auto second = MakeOwned(
        test,
        pool.get(),
        sdk::MessageKey{6U, 101U, 36U},
        {std::byte{0x20U}, std::byte{0x21U}, std::byte{0x22U}},
        12U,
        4U);
    const auto* const first_identity = first.get();
    const auto decoder_copy = first;
    test->Expect(
        sink->TryEnqueue(first) ==
                realtime::OptionalWalEnqueueResultV1::kAccepted &&
            sink->TryEnqueue(second) ==
                realtime::OptionalWalEnqueueResultV1::kAccepted &&
            decoder_copy.get() == first_identity,
        "WAL and decoder share the exact immutable ingress object");
    first.reset();
    second.reset();
    pool.reset();
    test->Expect(
        decoder_copy.get() == first_identity &&
            decoder_copy->body()[0U] == std::byte{0x10U},
        "WAL/decoder handles remain valid after their pool wrapper retires");
    sink->StopAndDrain();

    const realtime::OptionalWalSnapshotV1 snapshot = sink->Snapshot();
    test->Expect(
        snapshot.accepted_records == 2U &&
            snapshot.written_records == 2U &&
            snapshot.rejected_records == 0U &&
            snapshot.abandoned_records == 0U &&
            !snapshot.failed() && !snapshot.coverage_lost &&
            snapshot.finished,
        "clean stop drains and syncs every accepted WAL handle");
    test->Expect(
        sink->TryEnqueue(decoder_copy) ==
            realtime::OptionalWalEnqueueResultV1::kStopped,
        "post-quiescence WAL submission reports stopped");
    const realtime::OptionalWalSnapshotV1 after_stopped =
        sink->Snapshot();
    test->Expect(
        !after_stopped.coverage_lost &&
            after_stopped.rejected_records == 0U,
        "a post-capture kStopped result is not mislabeled as coverage loss");

    std::ifstream input(config.path, std::ios::binary);
    const std::vector<char> chars{
        std::istreambuf_iterator<char>(input),
        std::istreambuf_iterator<char>()};
    std::vector<std::byte> bytes(chars.size());
    if (!chars.empty()) {
        std::memcpy(bytes.data(), chars.data(), chars.size());
    }
    std::size_t offset = 0U;
    const std::array<std::uint64_t, 2U> expected_global{{11U, 12U}};
    const std::array<std::uint64_t, 2U> expected_source{{7U, 4U}};
    const std::array<std::size_t, 2U> expected_body{{2U, 3U}};
    for (std::size_t record = 0U; record < 2U; ++record) {
        test->Expect(
            bytes.size() - offset >=
                realtime::kOptionalWalRecordPrefixBytesV1 +
                    realtime::kOptionalWalRecordChecksumBytesV1,
            "WAL record has its complete fixed framing");
        if (bytes.size() - offset <
            realtime::kOptionalWalRecordPrefixBytesV1 +
                realtime::kOptionalWalRecordChecksumBytesV1) {
            return;
        }
        const std::span<const std::byte> suffix(bytes.data() + offset,
                                                bytes.size() - offset);
        const std::uint32_t record_bytes = LoadU32Little(suffix, 0U);
        test->Expect(
            record_bytes ==
                    realtime::kOptionalWalRecordPrefixBytesV1 +
                        expected_body[record] +
                        realtime::kOptionalWalRecordChecksumBytesV1 &&
                record_bytes <= suffix.size(),
            "WAL length prefix bounds exactly one complete record");
        if (record_bytes > suffix.size()) {
            return;
        }
        const std::span<const std::byte> framed =
            suffix.first(record_bytes);
        test->Expect(
            LoadU32Little(framed, 4U) ==
                    realtime::kOptionalWalRecordMagicV1 &&
                LoadU16Little(framed, 8U) ==
                    realtime::kOptionalWalRecordVersionV1 &&
                LoadU16Little(framed, 10U) ==
                    realtime::kOptionalWalRecordPrefixBytesV1 &&
                LoadU32Little(framed, 12U) == expected_body[record] &&
                LoadU64Little(framed, 36U) == expected_global[record] &&
                LoadU64Little(framed, 44U) == expected_source[record],
            "WAL frame retains version, source, and process sequences");
        const std::uint32_t stored_crc = LoadU32Little(
            framed,
            static_cast<std::size_t>(record_bytes) -
                realtime::kOptionalWalRecordChecksumBytesV1);
        const std::uint32_t actual_crc = l2flow::common::ComputeCrc32c(
            framed.subspan(
                sizeof(std::uint32_t),
                static_cast<std::size_t>(record_bytes) -
                    sizeof(std::uint32_t) -
                    realtime::kOptionalWalRecordChecksumBytesV1));
        test->Expect(
            stored_crc == actual_crc,
            "WAL frame CRC32C covers metadata, vendor head, and body");
        offset += static_cast<std::size_t>(record_bytes);
    }
    test->Expect(offset == bytes.size(), "WAL contains exactly two records");
}

}  // namespace

int main() {
    TestContext test;
    CheckRequiredClassification(&test);
    CheckOwnedLifetimeAndValidation(&test);
    CheckPoolBoundsReuseAndRetiredLifetime(&test);
    CheckDisabledAndFailedWalAreSideBranches(&test);
    CheckWalFramingAndDrain(&test);
    return test.failures() == 0 ? 0 : 1;
}
