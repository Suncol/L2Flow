#include "l2flow/ipc/partial_order_event_journal_v2.h"

#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string_view>

namespace {

namespace ipc = l2flow::ipc;
namespace market = l2flow::market;

constexpr std::uint32_t kTradeDate = 20260803U;

[[nodiscard]] bool ParseCount(
    std::string_view text,
    std::uint64_t* output) noexcept {
    if (output == nullptr || text.empty()) {
        return false;
    }
    std::uint64_t value = 0U;
    const auto result = std::from_chars(
        text.data(), text.data() + text.size(), value);
    if (result.ec != std::errc{} ||
        result.ptr != text.data() + text.size() || value == 0U ||
        value > 10'000'000U) {
        return false;
    }
    *output = value;
    return true;
}

[[nodiscard]] ipc::InstrumentDerivedEventV1 Revision(
    std::uint64_t sequence) noexcept {
    market::ShanghaiOrderRevisionEventV1 event{};
    event.operation = sequence == 1U
                          ? market::ShanghaiOrderDeltaOperationV1::kInsert
                          : market::ShanghaiOrderDeltaOperationV1::kUpdate;
    event.source_anchor.native_event_sequence =
        static_cast<std::int64_t>(sequence);
    event.source_anchor.source_sequence = sequence;
    event.source_anchor.ingress_sequence = sequence;
    event.source_anchor.tick_stream_sequence = sequence;
    event.order.key.trade_date = kTradeDate;
    event.order.key.instrument_id = 1U;
    event.order.key.channel = 7;
    event.order.key.order_id = 42;
    event.order.side = market::SideV1::kBuy;
    event.order.price_p6 = 10'000'000;
    event.order.price_valid = true;
    event.order.published_quantity = 1'000;
    event.order.published_quantity_valid = true;
    event.order.original_quantity = 1'000;
    event.order.original_quantity_valid = true;
    event.order.remaining_quantity = 1'000;
    event.order.remaining_quantity_valid = true;
    event.order.add_seen = true;
    event.order.apply_to_book = true;
    event.order.revision = sequence;

    ipc::InstrumentDerivedEventV1 result{};
    result.derived_event_sequence = sequence;
    result.payload = event;
    result.source_tick_event_ordinal = 0U;
    result.source_tick_event_ordinal_valid = true;
    return result;
}

[[nodiscard]] ipc::PartialOrderEventStatusUpdateV2 Status(
    std::uint64_t sequence) noexcept {
    ipc::PartialOrderEventStatusUpdateV2 result{};
    result.captured_source_frontier = sequence;
    result.state = ipc::PartialOrderEventServiceStateV2::kContiguous;
    result.stale = false;
    return result;
}

}  // namespace

int main(int argc, char** argv) {
    std::uint64_t measured_commits = 100'000U;
    if (argc > 2 ||
        (argc == 2 &&
         !ParseCount(argv[1] == nullptr ? std::string_view{}
                                        : std::string_view(argv[1]),
                     &measured_commits))) {
        std::cerr << "usage: benchmark_partial_order_event_journal_v2 "
                     "[measured_commits]\n";
        return 2;
    }
    const std::uint64_t warmup_commits =
        std::min<std::uint64_t>(10'000U, measured_commits / 10U);
    if (measured_commits >
        std::numeric_limits<std::uint64_t>::max() - warmup_commits) {
        return 2;
    }
    const std::uint64_t total_commits =
        measured_commits + warmup_commits;

    ipc::PartialOrderEventJournalConfigV2 config{};
    config.run_id[0U] = std::byte{0xB2};
    config.run_id[15U] = std::byte{0x2B};
    config.session_epoch = 1U;
    config.trade_date = kTradeDate;
    config.publication_generation = 1U;
    config.correction_epoch = 1U;
    config.coverage_start_unix_ns = 1U;
    config.ordering_quality =
        ipc::PartialOrderEventOrderingQualityV2::kBoundedReorderedPartial;
    config.event_capacity = total_commits;
    config.affected_channel_capacity = 1U;
    config.order_state_capacity = 1'024U;
    config.maximum_order_state_updates_per_commit = 1U;
    config.lazy_commit_chunk_bytes = 64ULL * 1024ULL * 1024ULL;

    std::shared_ptr<ipc::PartialOrderEventJournalProducerV2> producer;
    int system_error = 0;
    const auto create_error =
        ipc::PartialOrderEventJournalProducerV2::Create(
            config, &producer, &system_error);
    if (create_error != ipc::PartialOrderEventJournalCreateErrorV2::kNone ||
        producer == nullptr) {
        std::cerr << "create_error="
                  << ipc::PartialOrderEventJournalCreateErrorNameV2(
                         create_error)
                  << " errno=" << system_error << '\n';
        return 1;
    }

    for (std::uint64_t sequence = 1U; sequence <= warmup_commits;
         ++sequence) {
        const auto event = Revision(sequence);
        const auto error = producer->PublishCanonicalTick(
            sequence, Status(sequence), std::span(&event, 1U));
        if (error != ipc::PartialOrderEventJournalPublishErrorV2::kNone) {
            std::cerr << "warmup_error="
                      << ipc::PartialOrderEventJournalPublishErrorNameV2(
                             error)
                      << " sequence=" << sequence << '\n';
            return 1;
        }
    }

    const auto begin = std::chrono::steady_clock::now();
    for (std::uint64_t index = 1U; index <= measured_commits; ++index) {
        const std::uint64_t sequence = warmup_commits + index;
        const auto event = Revision(sequence);
        const auto error = producer->PublishCanonicalTick(
            sequence, Status(sequence), std::span(&event, 1U));
        if (error != ipc::PartialOrderEventJournalPublishErrorV2::kNone) {
            std::cerr << "publish_error="
                      << ipc::PartialOrderEventJournalPublishErrorNameV2(
                             error)
                      << " sequence=" << sequence << '\n';
            return 1;
        }
    }
    const auto end = std::chrono::steady_clock::now();
    const auto elapsed =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin)
            .count();
    if (elapsed <= 0) {
        return 1;
    }
    const long double elapsed_ns = static_cast<long double>(elapsed);
    const long double commit_count =
        static_cast<long double>(measured_commits);
    const long double commits_per_second =
        commit_count * 1'000'000'000.0L / elapsed_ns;
    const long double ns_per_commit = elapsed_ns / commit_count;
    const auto resources = producer->ResourceSnapshot();
    std::cout << "partial_event_journal_v2 commits=" << measured_commits
              << " elapsed_ns=" << elapsed
              << " commits_per_second="
              << static_cast<std::uint64_t>(commits_per_second)
              << " ns_per_commit="
              << static_cast<std::uint64_t>(ns_per_commit)
              << " order_state_backed_chunks="
              << resources.order_state_backed_chunk_count
              << " order_state_backing_calls="
              << resources.order_state_backing_allocation_calls << '\n';
    return 0;
}
