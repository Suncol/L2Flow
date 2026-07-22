#include "l2flow/factor/latest_factor_v1.h"

#include "l2flow/common/sha256.h"

#include <array>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string_view>
#include <thread>
#include <vector>

namespace factor = l2flow::factor;
namespace canonical = l2flow::canonical;
namespace common = l2flow::common;

namespace {

static_assert(sizeof(factor::LatestFactorSlotV1) == 4096U);
static_assert(alignof(factor::LatestFactorSlotV1) == 64U);

struct TestContext final {
    int failures = 0;

    void Expect(bool condition, std::string_view message) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << message << '\n';
        }
    }
};

template <std::size_t Size>
std::array<std::byte, Size> Pattern(std::uint8_t seed) {
    std::array<std::byte, Size> value{};
    for (std::size_t index = 0U; index < Size; ++index) {
        value[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(seed + index));
    }
    return value;
}

factor::LatestFactorValueV1 Value(std::int64_t asof) {
    factor::LatestFactorValueV1 value{};
    value.factor_id_sha256 = Pattern<32U>(0x10U);
    value.factor_version_sha256 = Pattern<32U>(0x30U);
    value.instrument_id = 17U;
    value.asof_ns = asof;
    value.value = static_cast<double>(asof);
    value.valid = true;
    value.input_quality_flags = 0U;
    value.watermark_set_id = static_cast<std::uint64_t>(asof);
    value.input_identity_sha256 = Pattern<32U>(
        static_cast<std::uint8_t>(asof & 0xff));
    value.calculation_latency_ns = 99U;
    value.clock_epoch.algorithm = 1U;
    value.clock_epoch.digest = Pattern<32U>(0x70U);
    value.clock_epoch.label = 3U;
    return value;
}

void CheckSchemaAndBasicPublication(TestContext* test) {
    test->Expect(
        factor::LatestFactorAtomicsLockFreeV1(),
        "latest-factor shared uint64 atomics are lock-free");
    const auto schema_hash =
        factor::LatestFactorSchemaDescriptorSha256V1();
    test->Expect(
        common::Sha256Hex(schema_hash) ==
            "7f58f8ec50460eae6b6c62a5a0c1e1d6c6e75aa1af711199e3a2bf35ab3feed2",
        "latest-factor schema descriptor SHA-256 golden");
    std::array<std::uint8_t, 32U> c_hash{};
    test->Expect(
        l2flow_latest_factor_schema_sha256_v1(c_hash.data()) ==
                L2FLOW_LATEST_FACTOR_NONE_V1 &&
            std::memcmp(c_hash.data(), schema_hash.data(), c_hash.size()) == 0,
        "C and C++ schema hash helpers agree");

    factor::LatestFactorSlotV1 slot{};
    test->Expect(
        factor::InitializeLatestFactorSlotV1(&slot) ==
            factor::LatestFactorErrorV1::kNone,
        "all-zero opaque slot initializes");
    factor::LatestFactorValueV1 observed{};
    test->Expect(
        factor::ReadLatestFactorV1(slot, &observed) ==
            factor::LatestFactorErrorV1::kEmpty,
        "initialized slot is empty before first publication");

    const auto first = Value(100U);
    auto published = factor::PublishLatestFactorV1(&slot, first);
    test->Expect(
        published.ok() && published.disposition ==
            factor::LatestFactorPublishDispositionV1::kPublished,
        "first scalar factor value publishes");
    std::uint64_t sequence = 0U;
    test->Expect(
        factor::ReadLatestFactorV1(slot, &observed, &sequence) ==
                factor::LatestFactorErrorV1::kNone &&
            observed.factor_id_sha256 == first.factor_id_sha256 &&
            observed.factor_version_sha256 ==
                first.factor_version_sha256 &&
            observed.instrument_id == first.instrument_id &&
            observed.asof_ns == first.asof_ns &&
            observed.value == first.value && observed.valid &&
            observed.watermark_set_id == first.watermark_set_id &&
            observed.input_identity_sha256 ==
                first.input_identity_sha256 &&
            sequence == 2U,
        "reader obtains one coherent full latest-factor value");
    test->Expect(
        factor::InitializeLatestFactorSlotV1(&slot) ==
            factor::LatestFactorErrorV1::kAlreadyInitialized,
        "initialize cannot clear a live slot");

    auto replay = first;
    replay.watermark_set_id += 1000U;
    replay.calculation_latency_ns += 500U;
    replay.clock_epoch.label += 1U;
    published = factor::PublishLatestFactorV1(&slot, replay);
    std::uint64_t after_idempotent = 0U;
    static_cast<void>(factor::ReadLatestFactorV1(
        slot, &observed, &after_idempotent));
    test->Expect(
        published.ok() && published.disposition ==
            factor::LatestFactorPublishDispositionV1::kIdempotent &&
            after_idempotent == sequence &&
            observed.watermark_set_id == first.watermark_set_id,
        "stable replay identity is idempotent and does not mutate payload/seqlock");

    auto old = Value(99U);
    published = factor::PublishLatestFactorV1(&slot, old);
    std::uint64_t after_reject = 0U;
    static_cast<void>(factor::ReadLatestFactorV1(
        slot, &observed, &after_reject));
    test->Expect(
        published.error == factor::LatestFactorErrorV1::kOldOutput &&
            after_reject == sequence && observed.asof_ns == 100,
        "older output is rejected without changing slot sequence or value");

    auto conflict = first;
    conflict.value += 1.0;
    published = factor::PublishLatestFactorV1(&slot, conflict);
    test->Expect(
        published.error == factor::LatestFactorErrorV1::kOutputConflict,
        "same historical key with a different scalar conflicts");

    auto wrong_identity = Value(101U);
    wrong_identity.factor_id_sha256[0] ^= std::byte{0xffU};
    published = factor::PublishLatestFactorV1(&slot, wrong_identity);
    test->Expect(
        published.error ==
            factor::LatestFactorErrorV1::kSlotIdentityMismatch,
        "one slot cannot be rebound to another factor identity");

    auto newer = Value(101U);
    published = factor::PublishLatestFactorV1(&slot, newer);
    test->Expect(
        published.ok() && published.disposition ==
                factor::LatestFactorPublishDispositionV1::kPublished &&
            factor::ReadLatestFactorV1(slot, &observed, &sequence) ==
                factor::LatestFactorErrorV1::kNone &&
            observed.asof_ns == 101 && sequence == 4U,
        "strictly newer output replaces the slot atomically");

    l2flow_latest_factor_value_v1 noncanonical{};
    std::memcpy(
        noncanonical.factor_id_sha256,
        first.factor_id_sha256.data(), first.factor_id_sha256.size());
    std::memcpy(
        noncanonical.factor_version_sha256,
        first.factor_version_sha256.data(),
        first.factor_version_sha256.size());
    noncanonical.instrument_id = first.instrument_id;
    noncanonical.asof_ns = 102;
    noncanonical.value = 7.0;
    noncanonical.valid = 0U;
    noncanonical.watermark_set_id = 102U;
    std::memcpy(
        noncanonical.input_identity_sha256,
        first.input_identity_sha256.data(),
        first.input_identity_sha256.size());
    noncanonical.clock_epoch_algorithm = first.clock_epoch.algorithm;
    std::memcpy(
        noncanonical.clock_epoch_digest,
        first.clock_epoch.digest.data(), first.clock_epoch.digest.size());
    std::uint8_t disposition = 0U;
    test->Expect(
        l2flow_latest_factor_publish_v1(
            &slot, sizeof(slot), &noncanonical, &disposition) ==
            L2FLOW_LATEST_FACTOR_INVALID_VALUE_V1,
        "C ABI rejects invalid validity with a nonzero scalar instead of normalizing it");
    noncanonical.value = -0.0;
    test->Expect(
        l2flow_latest_factor_publish_v1(
            &slot, sizeof(slot), &noncanonical, &disposition) ==
            L2FLOW_LATEST_FACTOR_INVALID_VALUE_V1,
        "invalid scalar requires canonical positive-zero bit pattern");

    factor::LatestFactorSlotV1 corrupt{};
    test->Expect(
        factor::InitializeLatestFactorSlotV1(&corrupt) ==
                factor::LatestFactorErrorV1::kNone &&
            factor::PublishLatestFactorV1(&corrupt, first).ok(),
        "reserved corruption fixture publishes");
    auto* word = reinterpret_cast<std::uint64_t*>(
        corrupt.bytes.data() + 16U);
    const std::uint64_t original =
        __atomic_load_n(word, __ATOMIC_RELAXED);
    __atomic_store_n(
        word, original | (std::uint64_t{1U} << 32U), __ATOMIC_RELAXED);
    test->Expect(
        factor::ReadLatestFactorV1(corrupt, &observed) ==
            factor::LatestFactorErrorV1::kCorruptSlot,
        "reader rejects nonzero internal reserved bytes");
}

void CheckCAbiOverlapRejection(TestContext* test) {
    factor::LatestFactorSlotV1 slot{};
    const factor::LatestFactorValueV1 value = Value(100U);
    l2flow_latest_factor_value_v1 c_value{};
    std::memcpy(
        c_value.factor_id_sha256,
        value.factor_id_sha256.data(), value.factor_id_sha256.size());
    std::memcpy(
        c_value.factor_version_sha256,
        value.factor_version_sha256.data(),
        value.factor_version_sha256.size());
    c_value.instrument_id = value.instrument_id;
    c_value.asof_ns = value.asof_ns;
    c_value.value = value.value;
    c_value.valid = value.valid ? 1U : 0U;
    c_value.input_quality_flags = value.input_quality_flags;
    c_value.watermark_set_id = value.watermark_set_id;
    std::memcpy(
        c_value.input_identity_sha256,
        value.input_identity_sha256.data(),
        value.input_identity_sha256.size());
    c_value.calculation_latency_ns = value.calculation_latency_ns;
    c_value.clock_epoch_algorithm = value.clock_epoch.algorithm;
    std::memcpy(
        c_value.clock_epoch_digest,
        value.clock_epoch.digest.data(), value.clock_epoch.digest.size());
    c_value.clock_epoch_label = value.clock_epoch.label;

    auto* const slot_bytes = reinterpret_cast<std::uint8_t*>(&slot);
    test->Expect(
        l2flow_latest_factor_publish_v1(
            &slot, sizeof(slot), &c_value, slot_bytes) ==
            L2FLOW_LATEST_FACTOR_INVALID_STORAGE_V1,
        "C publish rejects a disposition byte inside the slot");
    std::uint8_t disposition = 0U;
    test->Expect(
        l2flow_latest_factor_publish_v1(
            &slot, sizeof(slot),
            reinterpret_cast<const l2flow_latest_factor_value_v1*>(
                slot_bytes),
            &disposition) == L2FLOW_LATEST_FACTOR_INVALID_STORAGE_V1,
        "C publish rejects an input object inside the slot");
    test->Expect(
        l2flow_latest_factor_publish_v1(
            &slot, sizeof(slot), &c_value, &c_value.valid) ==
            L2FLOW_LATEST_FACTOR_INVALID_VALUE_V1,
        "C publish rejects overlapping input and disposition objects");
    test->Expect(
        l2flow_latest_factor_read_v1(
            &slot, sizeof(slot),
            reinterpret_cast<l2flow_latest_factor_value_v1*>(slot_bytes),
            nullptr) == L2FLOW_LATEST_FACTOR_INVALID_STORAGE_V1,
        "C read rejects an output object inside the slot");
    test->Expect(
        l2flow_latest_factor_read_v1(
            &slot, sizeof(slot), &c_value,
            reinterpret_cast<std::uint64_t*>(slot_bytes)) ==
            L2FLOW_LATEST_FACTOR_INVALID_STORAGE_V1,
        "C read rejects a sequence output inside the slot");
    test->Expect(
        l2flow_latest_factor_read_v1(
            &slot, sizeof(slot), &c_value,
            reinterpret_cast<std::uint64_t*>(&c_value)) ==
            L2FLOW_LATEST_FACTOR_INVALID_STORAGE_V1,
        "C read rejects overlapping output objects");

    std::uint64_t sequence = 0U;
    test->Expect(
        l2flow_latest_factor_read_v1(
            &slot, sizeof(slot), &c_value, &sequence) ==
            L2FLOW_LATEST_FACTOR_EMPTY_V1,
        "rejected aliases leave the empty slot unchanged");
}

void CheckConcurrentSnapshots(TestContext* test) {
    factor::LatestFactorSlotV1 slot{};
    test->Expect(
        factor::InitializeLatestFactorSlotV1(&slot) ==
            factor::LatestFactorErrorV1::kNone,
        "concurrency slot initializes");
    test->Expect(
        factor::PublishLatestFactorV1(&slot, Value(1000)).ok(),
        "concurrency seed publishes");
    std::atomic<bool> start{false};
    std::atomic<bool> stop{false};
    std::atomic<std::uint64_t> failures{0U};
    std::thread writer([&]() {
        while (!start.load(std::memory_order_acquire)) {
        }
        for (std::int64_t asof = 1001; asof <= 5000; ++asof) {
            if (!factor::PublishLatestFactorV1(&slot, Value(asof)).ok()) {
                failures.fetch_add(1U, std::memory_order_relaxed);
                break;
            }
        }
        stop.store(true, std::memory_order_release);
    });
    std::vector<std::thread> readers;
    for (std::size_t index = 0U; index < 4U; ++index) {
        readers.emplace_back([&]() {
            while (!start.load(std::memory_order_acquire)) {
            }
            do {
                factor::LatestFactorValueV1 value{};
                const auto error =
                    factor::ReadLatestFactorV1(slot, &value);
                if (error == factor::LatestFactorErrorV1::kNone &&
                    (value.value != static_cast<double>(value.asof_ns) ||
                     value.watermark_set_id !=
                         static_cast<std::uint64_t>(value.asof_ns))) {
                    failures.fetch_add(1U, std::memory_order_relaxed);
                } else if (error != factor::LatestFactorErrorV1::kNone &&
                           error != factor::LatestFactorErrorV1::kBusy) {
                    failures.fetch_add(1U, std::memory_order_relaxed);
                }
            } while (!stop.load(std::memory_order_acquire));
        });
    }
    start.store(true, std::memory_order_release);
    writer.join();
    for (auto& reader : readers) {
        reader.join();
    }
    test->Expect(
        failures.load(std::memory_order_relaxed) == 0U,
        "atomic payload words plus seqlock never expose a torn concurrent value");
}

}  // namespace

int main() {
    TestContext test;
    CheckSchemaAndBasicPublication(&test);
    CheckCAbiOverlapRejection(&test);
    CheckConcurrentSnapshots(&test);
    if (test.failures != 0) {
        std::cerr << test.failures << " Phase 7 latest-factor checks failed\n";
        return 1;
    }
    std::cout << "Phase 7 latest-factor checks passed\n";
    return 0;
}
