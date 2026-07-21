#include "l2flow/ingress/byte_ring.h"
#include "l2flow/ops/fatal_latch.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <iostream>
#include <limits>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace ingress = l2flow::ingress;
namespace ops = l2flow::ops;

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

    static void set_empty_cursor(ByteRing& ring,
                                 std::uint64_t position) noexcept {
        ring.producer_position_ = position;
        ring.consumer_position_ = position;
        ring.published_position_.store(position,
                                       std::memory_order_release);
        ring.consumed_position_.store(position,
                                      std::memory_order_release);
    }
};

}  // namespace l2flow::ingress

namespace {

struct TestContext final {
    void expect(bool condition, const std::string& message) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << message << '\n';
        }
    }

    int failures = 0;
};

constexpr std::size_t entry_bytes(std::size_t body_bytes) noexcept {
    return sizeof(ingress::CaptureMetaV1) +
           ingress::kVendorMessageHeadBytes + body_bytes +
           ingress::kEntryCommitLengthBytes;
}

void store_u32_le(
    std::uint32_t value,
    std::array<std::byte, ingress::kVendorMessageHeadBytes>* bytes) {
    (*bytes)[1] = static_cast<std::byte>(value & 0xffU);
    (*bytes)[2] = static_cast<std::byte>((value >> 8U) & 0xffU);
    (*bytes)[3] = static_cast<std::byte>((value >> 16U) & 0xffU);
    (*bytes)[4] = static_cast<std::byte>((value >> 24U) & 0xffU);
}

std::array<std::byte, ingress::kVendorMessageHeadBytes> make_head(
    std::size_t body_bytes,
    std::uint8_t marker = 0U) {
    std::array<std::byte, ingress::kVendorMessageHeadBytes> head{};
    head.fill(static_cast<std::byte>(marker));
    head[0] =
        static_cast<std::byte>(ingress::kVendorMessageHeadBytes);
    const std::size_t message_bytes =
        ingress::kVendorMessageHeadBytes + body_bytes;
    store_u32_le(static_cast<std::uint32_t>(message_bytes), &head);
    return head;
}

ingress::CaptureMetaV1 make_meta(std::uint64_t sequence) {
    ingress::CaptureMetaV1 meta{};
    meta.source_stream_id = 1002U;
    meta.connection_epoch_hint = 7U;
    meta.ingress_sequence = sequence;
    meta.recv_realtime_ns = 1'000'000U + sequence;
    meta.recv_monotonic_ns = 2'000'000U + sequence;
    meta.capture_date = 20260717U;
    meta.flags = static_cast<std::uint32_t>(sequence & 0xffffU);
    return meta;
}

std::vector<std::byte> make_body(std::size_t size,
                                 std::uint64_t sequence) {
    std::vector<std::byte> body(size);
    for (std::size_t index = 0U; index < size; ++index) {
        const std::uint64_t value =
            sequence * 17U + static_cast<std::uint64_t>(index) * 31U;
        body[index] =
            static_cast<std::byte>(static_cast<std::uint8_t>(value));
    }
    return body;
}

bool records_equal(const ingress::ByteRingRecord& actual,
                   const ingress::CaptureMetaV1& meta,
                   const std::array<std::byte,
                                    ingress::kVendorMessageHeadBytes>& head,
                   const std::vector<std::byte>& body) {
    return actual.meta.source_stream_id == meta.source_stream_id &&
           actual.meta.connection_epoch_hint ==
               meta.connection_epoch_hint &&
           actual.meta.ingress_sequence == meta.ingress_sequence &&
           actual.meta.recv_realtime_ns == meta.recv_realtime_ns &&
           actual.meta.recv_monotonic_ns == meta.recv_monotonic_ns &&
           actual.meta.capture_date == meta.capture_date &&
           actual.meta.flags == meta.flags && actual.head == head &&
           actual.body == body;
}

void test_schema_and_fatal_latch(TestContext* test) {
    static_assert(sizeof(ingress::CaptureMetaV1) == 40U);
    static_assert(alignof(ingress::CaptureMetaV1) == 8U);
    static_assert(ingress::kMinimumByteRingEntryBytes == 67U);

    ops::FatalLatch latch;
    test->expect(!latch.tripped(), "fatal latch starts clear");
    test->expect(!latch.trip(ops::FatalReason::NONE),
                 "NONE cannot trip the fatal latch");
    test->expect(
        latch.trip(ops::FatalReason::CALLBACK_REENTRY),
        "the first non-NONE reason wins");
    test->expect(
        !latch.trip(ops::FatalReason::CALLBACK_EXCEPTION),
        "a later fatal reason cannot overwrite the first");
    test->expect(latch.tripped(), "fatal latch reports tripped");
    test->expect(
        latch.reason() == ops::FatalReason::CALLBACK_REENTRY,
        "fatal latch preserves the initiating reason");

    ops::FatalLatch concurrent_latch;
    std::atomic<int> winners{0};
    std::atomic<std::uint32_t> winning_reason{0U};
    std::vector<std::thread> contenders;
    contenders.reserve(12U);
    for (std::uint32_t index = 1U; index <= 12U; ++index) {
        contenders.emplace_back([&, index] {
            const auto reason = static_cast<ops::FatalReason>(index);
            if (concurrent_latch.trip(reason)) {
                winning_reason.store(index, std::memory_order_release);
                winners.fetch_add(1, std::memory_order_acq_rel);
            }
        });
    }
    for (std::thread& contender : contenders) {
        contender.join();
    }
    test->expect(winners.load(std::memory_order_acquire) == 1,
                 "exactly one concurrent fatal reason wins");
    test->expect(
        static_cast<std::uint32_t>(concurrent_latch.reason()) ==
            winning_reason.load(std::memory_order_acquire),
        "concurrent winner and latched reason agree");
}

void test_zero_and_max_message(TestContext* test) {
    constexpr std::size_t max_body = 105U;
    constexpr std::uint32_t max_message =
        static_cast<std::uint32_t>(
            ingress::kVendorMessageHeadBytes + max_body);
    ingress::ByteRing ring(entry_bytes(max_body), max_message);
    ingress::ByteRingRecord output(max_body);
    std::byte* const reserved = output.body.data();

    const ingress::CaptureMetaV1 max_meta = make_meta(1U);
    const auto max_head = make_head(max_body, 0xa1U);
    const auto max_payload = make_body(max_body, 1U);
    test->expect(
        ring.try_push_copy(max_meta, max_head, max_payload) ==
            ingress::ByteRingPushResult::PUBLISHED,
        "maximum configured message is accepted");
    test->expect(ring.used_bytes() == ring.capacity_bytes(),
                 "maximum entry can exactly fill the ring");
    test->expect(
        ring.pressure() == ingress::ByteRingPressure::PROTECT,
        "a full ring is in protect pressure");
    test->expect(
        ring.try_pop(output) == ingress::ByteRingPopResult::RECORD,
        "maximum message is consumed");
    test->expect(
        records_equal(output, max_meta, max_head, max_payload),
        "maximum message round-trips byte exactly");
    test->expect(output.body.data() == reserved,
                 "consumer output does not allocate after reserve");

    const ingress::CaptureMetaV1 zero_meta = make_meta(2U);
    const auto zero_head = make_head(0U, 0x5aU);
    const std::vector<std::byte> empty_body;
    test->expect(
        ring.try_push_copy(zero_meta, zero_head, empty_body) ==
            ingress::ByteRingPushResult::PUBLISHED,
        "zero-body message is accepted");
    test->expect(
        ring.try_pop(output) == ingress::ByteRingPopResult::RECORD,
        "zero-body message is consumed");
    test->expect(
        records_equal(output, zero_meta, zero_head, empty_body),
        "zero-body message round-trips");
    test->expect(ring.used_bytes() == 0U,
                 "ring is empty after zero/max round-trip");
}

void test_invalid_inputs_and_checked_math(TestContext* test) {
    ingress::ByteRing ring(256U, 64U);
    const auto meta = make_meta(1U);
    const auto body = make_body(10U, 1U);

    auto bad_head_size = make_head(body.size());
    bad_head_size[0] = std::byte{22U};
    test->expect(
        ring.try_push_copy(meta, bad_head_size, body) ==
            ingress::ByteRingPushResult::INVALID_ARGUMENT,
        "wrong vendor HeadSize is rejected");

    auto bad_message_size = make_head(body.size());
    store_u32_le(24U, &bad_message_size);
    test->expect(
        ring.try_push_copy(meta, bad_message_size, body) ==
            ingress::ByteRingPushResult::INVALID_ARGUMENT,
        "head/body length mismatch is rejected");

    const auto oversized_body = make_body(42U, 1U);
    const auto oversized_head = make_head(oversized_body.size());
    test->expect(
        ring.try_push_copy(meta, oversized_head, oversized_body) ==
            ingress::ByteRingPushResult::MESSAGE_TOO_LARGE,
        "message larger than configured maximum is rejected");
    test->expect(ring.published_position() == 0U,
                 "invalid pushes never advance published position");

    bool rejected_unrepresentable_commit = false;
    try {
        ingress::ByteRing invalid(
            128U, std::numeric_limits<std::uint32_t>::max());
    } catch (const std::invalid_argument&) {
        rejected_unrepresentable_commit = true;
    }
    test->expect(
        rejected_unrepresentable_commit,
        "constructor rejects a maximum entry that overflows uint32 commit");

    bool rejected_capacity = false;
    try {
        ingress::ByteRing invalid(100U, 100U);
    } catch (const std::invalid_argument&) {
        rejected_capacity = true;
    }
    test->expect(
        rejected_capacity,
        "constructor rejects a ring smaller than its maximum legal entry");

    ingress::ByteRing exhausted(256U, 64U);
    const std::uint64_t near_end =
        std::numeric_limits<std::uint64_t>::max() -
        static_cast<std::uint64_t>(entry_bytes(body.size())) + 1U;
    ingress::ByteRingTestPeer::set_empty_cursor(exhausted, near_end);
    test->expect(
        exhausted.try_push_copy(meta, make_head(body.size()), body) ==
            ingress::ByteRingPushResult::CURSOR_EXHAUSTED,
        "absolute cursor addition is checked before entry copy");
    test->expect(exhausted.published_position() == near_end,
                 "cursor exhaustion does not publish a partial entry");
}

void test_fifo_and_cross_tail_wrap(TestContext* test) {
    constexpr std::uint32_t max_message = 63U;
    ingress::ByteRing ring(256U, max_message);
    ingress::ByteRingRecord output(
        max_message - ingress::kVendorMessageHeadBytes);

    const auto meta1 = make_meta(1U);
    const auto head1 = make_head(20U, 1U);
    const auto body1 = make_body(20U, 1U);
    const auto meta2 = make_meta(2U);
    const auto head2 = make_head(30U, 2U);
    const auto body2 = make_body(30U, 2U);
    const auto meta3 = make_meta(3U);
    const auto head3 = make_head(40U, 3U);
    const auto body3 = make_body(40U, 3U);

    test->expect(
        ring.try_push_copy(meta1, head1, body1) ==
                ingress::ByteRingPushResult::PUBLISHED &&
            ring.try_push_copy(meta2, head2, body2) ==
                ingress::ByteRingPushResult::PUBLISHED,
        "two FIFO entries are published");
    test->expect(
        ring.try_pop(output) == ingress::ByteRingPopResult::RECORD &&
            records_equal(output, meta1, head1, body1),
        "first FIFO entry is consumed");
    test->expect(
        ring.try_push_copy(meta3, head3, body3) ==
            ingress::ByteRingPushResult::PUBLISHED,
        "entry crossing the allocation tail is published");
    test->expect(ring.published_position() == 291U,
                 "absolute producer cursor advances across physical wrap");
    test->expect(
        ring.try_pop(output) == ingress::ByteRingPopResult::RECORD &&
            records_equal(output, meta2, head2, body2),
        "pre-wrap FIFO entry remains intact");
    test->expect(
        ring.try_pop(output) == ingress::ByteRingPopResult::RECORD &&
            records_equal(output, meta3, head3, body3),
        "cross-tail entry round-trips intact");
    test->expect(
        ring.try_pop(output) == ingress::ByteRingPopResult::EMPTY,
        "wrapped FIFO drain ends empty");
    test->expect(ring.consumed_position() == 291U,
                 "absolute consumer cursor follows producer across wrap");

    // Place an empty ring so the final uint32 commit itself occupies physical
    // bytes [126, 127, 0, 1].  This specifically proves that publication does
    // not rely on an aligned in-buffer atomic.
    ingress::ByteRing wrapped_commit(128U, 23U);
    ingress::ByteRingTestPeer::set_empty_cursor(wrapped_commit, 63U);
    const auto commit_meta = make_meta(9U);
    const auto commit_head = make_head(0U, 9U);
    const std::vector<std::byte> empty_body;
    ingress::ByteRingRecord commit_output;
    test->expect(
        wrapped_commit.try_push_copy(
            commit_meta, commit_head, empty_body) ==
            ingress::ByteRingPushResult::PUBLISHED,
        "commit field crossing the physical tail publishes");
    test->expect(wrapped_commit.published_position() == 130U,
                 "wrapped commit advances to its exclusive absolute end");
    test->expect(
        wrapped_commit.try_pop(commit_output) ==
                ingress::ByteRingPopResult::RECORD &&
            records_equal(commit_output,
                          commit_meta,
                          commit_head,
                          empty_body),
        "consumer acquire validates a commit split across the tail");
}

void test_pressure_boundaries(TestContext* test) {
    constexpr std::size_t capacity = 1000U;
    constexpr std::uint32_t max_message = 806U;

    ingress::ByteRing below_warning(capacity, max_message);
    const auto body699 = make_body(632U, 1U);
    test->expect(
        below_warning.try_push_copy(
            make_meta(1U), make_head(body699.size()), body699) ==
            ingress::ByteRingPushResult::PUBLISHED,
        "699-byte entry publishes");
    test->expect(
        below_warning.pressure() == ingress::ByteRingPressure::NORMAL,
        "699/1000 remains below warning");

    ingress::ByteRing at_warning(capacity, max_message);
    const auto body700 = make_body(633U, 1U);
    test->expect(
        at_warning.try_push_copy(
            make_meta(1U), make_head(body700.size()), body700) ==
            ingress::ByteRingPushResult::PUBLISHED,
        "700-byte entry publishes");
    test->expect(at_warning.warning_threshold_bytes() == 700U,
                 "warning threshold is exact ceil(70%)");
    test->expect(
        at_warning.pressure() == ingress::ByteRingPressure::WARNING,
        "warning begins at >=70%");

    ingress::ByteRing below_protect(capacity, max_message);
    const auto body849 = make_body(782U, 1U);
    test->expect(
        below_protect.try_push_copy(
            make_meta(1U), make_head(body849.size()), body849) ==
            ingress::ByteRingPushResult::PUBLISHED,
        "849-byte entry publishes");
    test->expect(
        below_protect.pressure() == ingress::ByteRingPressure::WARNING,
        "849/1000 remains warning");

    ingress::ByteRing at_protect(capacity, max_message);
    const auto body850 = make_body(783U, 1U);
    test->expect(
        at_protect.try_push_copy(
            make_meta(1U), make_head(body850.size()), body850) ==
            ingress::ByteRingPushResult::PUBLISHED,
        "850-byte entry publishes");
    test->expect(at_protect.protect_threshold_bytes() == 850U,
                 "protect threshold is exact ceil(85%)");
    test->expect(
        at_protect.pressure() == ingress::ByteRingPressure::PROTECT,
        "protect begins at >=85%");
}

void test_overflow_does_not_overwrite(TestContext* test) {
    ingress::ByteRing ring(200U, 73U);
    ingress::ByteRingRecord output(50U);
    const auto meta1 = make_meta(1U);
    const auto head1 = make_head(50U, 1U);
    const auto body1 = make_body(50U, 1U);
    const auto meta2 = make_meta(2U);
    const auto head2 = make_head(50U, 2U);
    const auto body2 = make_body(50U, 2U);

    test->expect(
        ring.try_push_copy(meta1, head1, body1) ==
            ingress::ByteRingPushResult::PUBLISHED,
        "first overflow test record publishes");
    const std::uint64_t published = ring.published_position();
    test->expect(
        ring.try_push_copy(meta2, head2, body2) ==
            ingress::ByteRingPushResult::FULL,
        "insufficient free bytes reports FULL");
    test->expect(ring.published_position() == published,
                 "FULL does not advance publication");
    test->expect(ring.used_bytes() == entry_bytes(50U),
                 "FULL does not change occupancy");
    test->expect(
        ring.try_pop(output) == ingress::ByteRingPopResult::RECORD &&
            records_equal(output, meta1, head1, body1),
        "FULL did not overwrite the unread record");
    test->expect(
        ring.try_push_copy(meta2, head2, body2) ==
                ingress::ByteRingPushResult::PUBLISHED &&
            ring.try_pop(output) == ingress::ByteRingPopResult::RECORD &&
            records_equal(output, meta2, head2, body2),
        "space is reusable after consumer release");
}

void test_output_capacity_and_corruption(TestContext* test) {
    ingress::ByteRing ring(256U, 63U);
    const auto meta = make_meta(1U);
    const auto head = make_head(20U, 7U);
    const auto body = make_body(20U, 1U);
    test->expect(
        ring.try_push_copy(meta, head, body) ==
            ingress::ByteRingPushResult::PUBLISHED,
        "record for output-capacity test publishes");

    ingress::ByteRingRecord too_small(19U);
    test->expect(
        ring.try_pop(too_small) ==
            ingress::ByteRingPopResult::OUTPUT_TOO_SMALL,
        "small consumer buffer is reported");
    test->expect(ring.consumed_position() == 0U,
                 "OUTPUT_TOO_SMALL does not consume");
    too_small.body.reserve(20U);
    test->expect(
        ring.try_pop(too_small) == ingress::ByteRingPopResult::RECORD &&
            records_equal(too_small, meta, head, body),
        "record can be retried after consumer reserves enough space");

    const auto corrupt_meta = make_meta(2U);
    const auto corrupt_head = make_head(10U, 8U);
    const auto corrupt_body = make_body(10U, 2U);
    const std::uint64_t start = ring.published_position();
    test->expect(
        ring.try_push_copy(corrupt_meta, corrupt_head, corrupt_body) ==
            ingress::ByteRingPushResult::PUBLISHED,
        "record for commit corruption test publishes");
    const std::uint64_t commit_last =
        ring.published_position() - 1U;
    ingress::ByteRingTestPeer::xor_byte(
        ring, commit_last, std::byte{1U});
    ingress::ByteRingRecord output(20U);
    test->expect(
        ring.try_pop(output) == ingress::ByteRingPopResult::CORRUPT,
        "corrupt trailing commit length is detected");
    test->expect(ring.consumed_position() == start,
                 "corrupt commit does not consume");
    ingress::ByteRingTestPeer::xor_byte(
        ring, commit_last, std::byte{1U});
    test->expect(
        ring.try_pop(output) == ingress::ByteRingPopResult::RECORD &&
            records_equal(
                output, corrupt_meta, corrupt_head, corrupt_body),
        "restored commit makes the entry readable");

    const std::uint64_t head_start =
        ring.published_position() + sizeof(ingress::CaptureMetaV1);
    test->expect(
        ring.try_push_copy(
            make_meta(3U), make_head(10U), make_body(10U, 3U)) ==
            ingress::ByteRingPushResult::PUBLISHED,
        "record for header corruption test publishes");
    ingress::ByteRingTestPeer::xor_byte(
        ring, head_start + 1U, std::byte{1U});
    const std::uint64_t consumed_before = ring.consumed_position();
    test->expect(
        ring.try_pop(output) == ingress::ByteRingPopResult::CORRUPT,
        "corrupt message length is detected");
    test->expect(ring.consumed_position() == consumed_before,
                 "header corruption does not consume");
}

struct ModelRecord final {
    ingress::CaptureMetaV1 meta{};
    std::array<std::byte, ingress::kVendorMessageHeadBytes> head{};
    std::vector<std::byte> body;
};

void test_randomized_fifo_property(TestContext* test) {
    constexpr std::size_t capacity = 1024U;
    constexpr std::size_t max_body = 128U;
    ingress::ByteRing ring(
        capacity,
        static_cast<std::uint32_t>(
            ingress::kVendorMessageHeadBytes + max_body));
    ingress::ByteRingRecord output(max_body);
    std::deque<ModelRecord> model;
    std::size_t model_bytes = 0U;
    std::uint64_t next_sequence = 1U;
    std::mt19937_64 random(0x8e4d'4f11'a290'75c3ULL);

    const auto consume_one = [&] {
        if (model.empty()) {
            test->expect(
                ring.try_pop(output) ==
                    ingress::ByteRingPopResult::EMPTY,
                "model-empty ring reports EMPTY");
            return;
        }
        ModelRecord expected = std::move(model.front());
        model.pop_front();
        const auto result = ring.try_pop(output);
        test->expect(result == ingress::ByteRingPopResult::RECORD,
                     "model record is consumable");
        if (result == ingress::ByteRingPopResult::RECORD) {
            test->expect(
                records_equal(output,
                              expected.meta,
                              expected.head,
                              expected.body),
                "randomized model preserves FIFO bytes");
        }
        model_bytes -= entry_bytes(expected.body.size());
    };

    for (std::size_t step = 0U; step < 20'000U; ++step) {
        const bool should_push =
            model.empty() || (random() % 100U) < 63U;
        if (!should_push) {
            consume_one();
        } else {
            ModelRecord candidate;
            candidate.meta = make_meta(next_sequence);
            const std::size_t body_size =
                static_cast<std::size_t>(random() % (max_body + 1U));
            candidate.head =
                make_head(body_size,
                          static_cast<std::uint8_t>(next_sequence));
            candidate.body =
                make_body(body_size, next_sequence);
            const std::size_t candidate_bytes =
                entry_bytes(body_size);
            const auto result = ring.try_push_copy(
                candidate.meta, candidate.head, candidate.body);
            if (candidate_bytes <= capacity - model_bytes) {
                test->expect(
                    result == ingress::ByteRingPushResult::PUBLISHED,
                    "model-fit randomized entry publishes");
                if (result ==
                    ingress::ByteRingPushResult::PUBLISHED) {
                    model_bytes += candidate_bytes;
                    model.push_back(std::move(candidate));
                    ++next_sequence;
                }
            } else {
                test->expect(
                    result == ingress::ByteRingPushResult::FULL,
                    "model-full randomized entry is rejected");
                consume_one();
            }
        }
        test->expect(ring.used_bytes() == model_bytes,
                     "randomized model occupancy matches ring");
    }

    while (!model.empty()) {
        consume_one();
    }
    test->expect(ring.used_bytes() == 0U,
                 "randomized model drains to zero occupancy");
}

void test_concurrent_spsc_stress(TestContext* test) {
    constexpr std::uint64_t record_count = 100'000U;
    constexpr std::size_t max_body = 256U;
    ingress::ByteRing ring(
        4096U,
        static_cast<std::uint32_t>(
            ingress::kVendorMessageHeadBytes + max_body));
    std::atomic<bool> start{false};
    std::atomic<bool> failed{false};

    std::thread producer([&] {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        for (std::uint64_t sequence = 1U;
             sequence <= record_count;
             ++sequence) {
            const std::size_t body_size =
                static_cast<std::size_t>(
                    (sequence * 37U) % (max_body + 1U));
            const auto meta = make_meta(sequence);
            const auto head =
                make_head(body_size,
                          static_cast<std::uint8_t>(sequence));
            const auto body = make_body(body_size, sequence);
            for (;;) {
                const auto result =
                    ring.try_push_copy(meta, head, body);
                if (result ==
                    ingress::ByteRingPushResult::PUBLISHED) {
                    break;
                }
                if (result != ingress::ByteRingPushResult::FULL) {
                    failed.store(true, std::memory_order_release);
                    return;
                }
                std::this_thread::yield();
            }
        }
    });

    std::thread consumer([&] {
        ingress::ByteRingRecord output(max_body);
        std::byte* const reserved = output.body.data();
        start.store(true, std::memory_order_release);
        for (std::uint64_t expected_sequence = 1U;
             expected_sequence <= record_count;) {
            const auto result = ring.try_pop(output);
            if (result == ingress::ByteRingPopResult::EMPTY) {
                std::this_thread::yield();
                continue;
            }
            if (result != ingress::ByteRingPopResult::RECORD) {
                failed.store(true, std::memory_order_release);
                return;
            }
            const std::size_t expected_size =
                static_cast<std::size_t>(
                    (expected_sequence * 37U) % (max_body + 1U));
            if (output.meta.ingress_sequence != expected_sequence ||
                output.body.size() != expected_size ||
                output.body.data() != reserved) {
                failed.store(true, std::memory_order_release);
                return;
            }
            const auto expected_body =
                make_body(expected_size, expected_sequence);
            if (output.body != expected_body) {
                failed.store(true, std::memory_order_release);
                return;
            }
            ++expected_sequence;
        }
    });

    producer.join();
    consumer.join();
    test->expect(!failed.load(std::memory_order_acquire),
                 "100k concurrent SPSC records preserve order and bytes");
    test->expect(ring.used_bytes() == 0U,
                 "concurrent SPSC stress drains the ring");
    test->expect(
        ring.published_position() == ring.consumed_position(),
        "concurrent publication and consumption cursors converge");
}

}  // namespace

int main() {
    TestContext test;
    test_schema_and_fatal_latch(&test);
    test_zero_and_max_message(&test);
    test_invalid_inputs_and_checked_math(&test);
    test_fifo_and_cross_tail_wrap(&test);
    test_pressure_boundaries(&test);
    test_overflow_does_not_overwrite(&test);
    test_output_capacity_and_corruption(&test);
    test_randomized_fifo_property(&test);
    test_concurrent_spsc_stress(&test);

    if (test.failures != 0) {
        std::cerr << test.failures << " phase-1 ring test(s) failed\n";
        return 1;
    }
    std::cout << "phase-1 capture/ring tests passed\n";
    return 0;
}
