#include "l2flow/ipc/order_event_delta_ring_v1.h"

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {

namespace ipc = l2flow::ipc;

ipc::OrderEventDeltaPayloadV1 Order(
    std::uint64_t tick_sequence,
    std::int64_t order_id) {
    ipc::OrderEventDeltaPayloadV1 result{};
    result.record_schema_version = 1U;
    result.record_bytes = sizeof(result);
    result.trade_date = 20260730U;
    result.instrument_id = 1U;
    result.channel = 7;
    result.market =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1;
    result.event_kind =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_REVISION_V1;
    result.order_id = order_id;
    result.revision = 1U;
    result.original_quantity = 100;
    result.original_quantity_valid = 1U;
    result.remaining_quantity = 100;
    result.remaining_quantity_valid = 1U;
    result.tick_stream_sequence = tick_sequence;
    result.native_event_sequence =
        static_cast<std::int64_t>(tick_sequence);
    result.source_sequence = tick_sequence;
    result.ingress_sequence = tick_sequence;
    result.vendor_sequence_id = tick_sequence;
    return result;
}

std::string Hex(const l2flow::common::Identity128& identity) {
    constexpr char digits[] = "0123456789abcdef";
    std::string result;
    result.reserve(identity.size() * 2U);
    for (std::byte value : identity) {
        const std::uint8_t byte = static_cast<std::uint8_t>(value);
        result.push_back(digits[byte >> 4U]);
        result.push_back(digits[byte & 0x0FU]);
    }
    return result;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 4) {
        std::cerr
            << "usage: host <python> <probe.py> <reader-library.so>\n";
        return 2;
    }

    ipc::OrderEventDeltaRingConfigV1 config{};
    for (std::size_t index = 0U; index < config.run_id.size();
         ++index) {
        config.run_id[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(index + 1U));
    }
    config.session_epoch = 123U;
    config.trade_date = 20260730U;
    config.ring_capacity = 8U;
    config.maximum_mapping_bytes = 16ULL * 1024ULL * 1024ULL;
    std::unique_ptr<ipc::OrderEventDeltaRingProducerV1> producer;
    if (ipc::OrderEventDeltaRingProducerV1::Create(
            config, &producer) !=
            ipc::OrderEventDeltaRingCreateErrorV1::kNone ||
        producer == nullptr ||
        producer->PublishSourceTick(1U, {}) !=
            ipc::OrderEventDeltaPublishErrorV1::kNone) {
        std::cerr << "failed to create probe producer\n";
        return 1;
    }
    const std::array<ipc::OrderEventDeltaPayloadV1, 2U> events{
        Order(2U, 7001),
        Order(2U, 7002)};
    if (producer->PublishSourceTick(2U, events) !=
            ipc::OrderEventDeltaPublishErrorV1::kNone ||
        !producer->UpdateHeartbeat(777'777U) ||
        !producer->BeginDraining() || !producer->StopClean()) {
        std::cerr << "failed to publish probe fixture\n";
        return 1;
    }

    int descriptor = -1;
    if (!producer->DuplicateReadOnlyDescriptor(&descriptor) ||
        descriptor < 0) {
        std::cerr << "failed to duplicate probe descriptor\n";
        return 1;
    }
    const int fd_flags = ::fcntl(descriptor, F_GETFD);
    if (fd_flags < 0 ||
        ::fcntl(
            descriptor,
            F_SETFD,
            fd_flags & ~FD_CLOEXEC) != 0) {
        static_cast<void>(::close(descriptor));
        std::cerr << "failed to make probe descriptor inheritable\n";
        return 1;
    }

    const ipc::OrderEventDeltaSessionV1 session =
        producer->session();
    const std::vector<std::string> arguments{
        argv[1],
        argv[2],
        "--library",
        argv[3],
        "--fd",
        std::to_string(descriptor),
        "--run-id",
        Hex(session.run_id),
        "--session-epoch",
        std::to_string(session.session_epoch),
        "--trade-date",
        std::to_string(session.trade_date),
        "--capacity",
        std::to_string(session.ring_capacity),
        "--mapping-bytes",
        std::to_string(session.total_mapping_bytes),
        "--expected-rows",
        "2",
        "--expected-source-tick",
        "2",
        "--expected-heartbeat",
        "777777",
        "--expected-state",
        std::to_string(static_cast<std::uint32_t>(
            ipc::OrderEventDeltaProducerStateV1::kStoppedClean)),
    };
    std::vector<char*> child_argv;
    child_argv.reserve(arguments.size() + 1U);
    for (const std::string& argument : arguments) {
        child_argv.push_back(const_cast<char*>(argument.c_str()));
    }
    child_argv.push_back(nullptr);

    const pid_t child = ::fork();
    if (child < 0) {
        static_cast<void>(::close(descriptor));
        std::cerr << "fork failed\n";
        return 1;
    }
    if (child == 0) {
        ::execvp(child_argv[0], child_argv.data());
        ::_exit(127);
    }
    static_cast<void>(::close(descriptor));

    int status = 0;
    pid_t waited = -1;
    do {
        waited = ::waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    if (waited != child || !WIFEXITED(status)) {
        std::cerr << "probe child did not exit normally\n";
        return 1;
    }
    return WEXITSTATUS(status);
}
