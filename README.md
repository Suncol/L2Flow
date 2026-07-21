# L2Flow

L2Flow provides a local, DataYes C++ SDK-compatible Level-2 market-data mock for
load, concurrency, and failure-path testing when a real DataYes feed is not
available.

The mock deliberately separates two kinds of compatibility:

- **API and payload compatibility:** consumers continue to use
  `IOManager`, `Subscriber`, `MessageHandler`, `MDLMessage`, and the message
  structs from the supplied SDK.
- **Business semantics:** generated identifiers, prices, order flow, trades, and
  code-valued fields are synthetic. They are not exchange-certified data and
  must not be used to validate trading decisions or regulatory logic.

The public API is declared in
[`include/l2mock/l2_mock.h`](include/l2mock/l2_mock.h). The supplied DataYes SDK
version is `2.13.234` (`MDL_VERSION == 213234`).

## Production Phase 0–1 ingress

The repository also contains the first two production phases described in
[`docs/design.md`](docs/design.md):

- an immutable SDK/archive/shared-library baseline, constrained ELF parser,
  compiled ABI probe, runtime wrong-version probe, and reproducible build
  manifest;
- four independent ingress executables, each owning exactly one SDK manager,
  subscriber, callback gate, preallocated SPSC byte ring, and temporary
  sequential shadow sink;
- bounded credential loading, dynamic loading from a sealed immutable SDK
  snapshot, `sd_notify`/watchdog support, metrics, subscription/readiness
  observation, ordered drain, and process fail-stop when opaque SDK callback
  convergence cannot be proved.

The four binaries are:

```text
mdl-ingress-sh-snapshot   required 4.4; optional 4.6
mdl-ingress-sh-tick       required 4.24
mdl-ingress-sz-snapshot   required 6.28; optional 6.29
mdl-ingress-sz-tick       required 6.33 and 6.36 on one Subscriber
```

`6.53 CombinedTick` is explicitly forbidden as a core subscription. The
executables do not have a link-time dependency on `libmdl_api.so`; they open a
regular file with `O_NOFOLLOW|O_NONBLOCK`, require the approved exact size
before copying it into a sealed memfd, and run the frozen
hash/ELF/ABI/runtime gate. Service startup runs the full gate on sealed
snapshot A. The loader then captures fresh snapshot B, repeats the library
component gate on B, and performs final `dlopen` on B, which is retained for
the SDK object lifetime.

The original SDK archive is intentionally required for a full startup gate and
is not synthesized from the extracted directory. Its one-descriptor hash is
bounded by a 1 GiB operational cap before reads; the compiled SHA-256 remains
the identity. The compiled baseline JSON is exact-size captured and verified
from one sealed descriptor without reopening its path:

```bash
./build/mdl_abi_preflight \
  --baseline configs/vendor_baseline.json \
  --archive /approved/mdl_sdk_2_13_234.tar.gz \
  --library /approved/libmdl_api.so

./build/mdl-ingress-sz-tick --help
```

Tokens are accepted only from a named systemd credential or an explicit
root-owned `0400` file; there is no token command-line option. Endpoint
behavior is loaded atomically from `--endpoint-contract` and pinned by
`--endpoint-contract-sha256`; address, encoding, merge, MAC-auth, and server
selection cannot be overridden independently on the production CLI. The
strict version-1 JSON contains exactly:

```json
{
  "schema_version": 1,
  "ingress_kind": "sz-tick",
  "name": "reviewed-endpoint-name",
  "resolved_server_address": "REPLACE_WITH_REVIEWED_RESOLVED_ADDRESS",
  "message_encoding": 7,
  "merge_message": false,
  "send_mac_auth": false,
  "server_select": false
}
```

The expected SHA-256 is over the exact file bytes. `--metrics-path` selects an
absolute Prometheus textfile. Once per second the monitor renders and submits a
snapshot to a bounded latest-pending-wins worker; the worker performs the
atomic filesystem replacement, never the SDK callback or monitor thread.
Construction of the production worker acquires a persistent typed `0600`
sidecar lease derived from the case-folded target basename and retains both
its exclusive `flock` and the destination directory fd. Publications remain
relative to that dirfd even if the named parent is renamed or replaced, and a
second cooperating worker cannot lease the same logical target concurrently.
The default worker constructor throws before accepting submissions if the path
or lease is invalid, causing startup to fail closed. Shutdown submits one final
snapshot and drains the worker.

All production paths are absolute, have pre-existing parents, and use portable
ASCII basenames. Writable outputs may not alias protected inputs or one
another. Each service needs a separate private SDK-log directory containing a
non-symlink, single-link, exact-mode-`0444`
`.l2flow-sdk-log-directory-v1` file whose exact contents are
`l2flow-sdk-log-directory-v1\n`. Startup retains that directory fd and passes
`/proc/self/fd/N/<basename>` to the SDK while holding a nonblocking exclusive
`flock` on the marker; a second cooperating ingress cannot lease that
directory concurrently. Shadow and metrics writers reject a marked SDK-log
directory. The marker declares an operator trust boundary, so services that
could otherwise modify one another's files under the same UID also require
separate service UIDs or mount isolation.

Phase 1 shadow files are local, native-endian, temporary evidence. They are not
the portable checksummed Callback WAL from Phase 2. First-seen readiness checks
the fixed-body minimum and every dynamic string/list range in each required
core record, but does not claim business-semantic decoding. See
[`docs/decisions/phase01.md`](docs/decisions/phase01.md) and the current
[`local acceptance record`](docs/acceptance/phase01-local.md).

## Build and run

Compilation never connects to the network. The default test configuration
includes both the mock suite and the Phase 0–1 gate, so an absent or
non-approved SDK artifact intentionally makes the Phase 0 test fail. To run
only the self-contained mock tests, disable the Phase 0–1 test suite; the mock
uses `L2Flow::l2mock_standalone` and does not require the vendor shared
library:

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DL2FLOW_BUILD_TESTS=OFF
cmake --build build --parallel
ctest --test-dir build --output-on-failure

# Full default workload: 5,000 cash instruments, 320 derivatives,
# all supported streams, target 100,000 generated messages/second.
./build/l2_mock_demo --duration 10
```

The demo accepts `--rate 0` for an unpaced run and `--sh`, `--sz`,
`--futures`, and `--seed` for workload control. Run
`./build/l2_mock_demo --help` for the complete CLI.

## Scope

The default configuration creates a deterministic, full-market-scale workload:

- 2,500 synthetic Shanghai instruments;
- 2,500 synthetic Shenzhen instruments;
- 64 synthetic instruments per supported derivatives market when
  `include_derivatives` is enabled;
- snapshots, orders, and trades for the supported SDK message types;
- configurable callback concurrency, queue capacity, pacing, and
  backpressure.

“Full-market-scale” here means **a market-scale synthetic instrument count over
the 28 core streams returned by `mock::SupportedMessages()`**. It does not mean
that the generated instrument identifiers are a current exchange security
master or that all 109 structs in the seven SDK L2 headers are generated. See
[the exact SDK coverage](docs/sdk_coverage.md) and
[the design document](docs/mock_design.md) before selecting the mock for a
consumer.

## Drop-in integration

The mock replaces the SDK factory call. Subscription, callback, payload casting,
and shutdown continue to use the original SDK interfaces.

```cpp
#include "l2mock/l2_mock.h"
#include "mdl_shl2_msg.h"
#include "mdl_szl2_msg.h"

#include <chrono>
#include <cstdio>
#include <thread>

using namespace datayes::mdl;

class Handler final : public MessageHandler {
public:
    void OnMDLSHL2Message(const MDLMessage* message) override {
        const MDLMessageHead* head = message->GetHead();
        if (head->MessageID == mdl_shl2_msg::SHL2MarketData::MessageID) {
            const auto* body =
                reinterpret_cast<const mdl_shl2_msg::SHL2MarketData*>(
                    message->GetBody());
            std::printf("SH %s last=%.3f\n",
                        body->SecurityID.std_str().c_str(),
                        body->LastPrice.GetFloat());
        }
    }

    void OnMDLSZL2Message(const MDLMessage* message) override {
        const MDLMessageHead* head = message->GetHead();
        if (head->MessageID ==
            mdl_szl2_msg::Snapshot300111_v2::MessageID) {
            const auto* body =
                reinterpret_cast<const mdl_szl2_msg::Snapshot300111_v2*>(
                    message->GetBody());
            std::printf("SZ %s last=%.6f\n",
                        body->SecurityID.std_str().c_str(),
                        body->LastPrice.GetDouble());
        }
    }
};

int main() {
    mock::Config config;
    config.seed = 20260717;
    config.messages_per_second = 100000;
    config.callback_threads = 4;
    config.clock_mode = mock::ClockMode::Realtime;
    config.backpressure = mock::BackpressurePolicy::Block;

    const std::string validation_error = mock::ValidateConfig(config);
    if (!validation_error.empty()) {
        std::fprintf(stderr, "invalid mock config: %s\n",
                     validation_error.c_str());
        return 1;
    }

    Handler handler;  // Must outlive subscribers and the IOManager.
    IOManagerPtr io = mock::CreateIOManager(config);
    if (io.IsNull()) {
        std::fprintf(stderr, "cannot create mock IOManager\n");
        return 2;
    }

    SubscriberPtr subscriber =
        io->CreateSubscriber(&handler, /*multithread_callback=*/true);
    mock::SubscribeAll(subscriber.Get());

    const std::string connect_error = subscriber->Connect();
    if (!connect_error.empty()) {
        std::fprintf(stderr, "mock connect failed: %s\n",
                     connect_error.c_str());
        io->Shutdown();
        return 3;
    }

    std::this_thread::sleep_for(std::chrono::seconds(10));

    const mock::Statistics stats = mock::GetStatistics(io.Get());
    std::printf("generated=%llu delivered=%llu dropped=%llu\n",
                static_cast<unsigned long long>(stats.generated),
                static_cast<unsigned long long>(stats.delivered),
                static_cast<unsigned long long>(stats.dropped));

    io->Shutdown();
    return 0;
}
```

No server address, token, network connection, or market-data permission is
needed in mock mode. `Subscriber::Connect()` activates that subscriber; call
`IOManager::Shutdown()` before destroying the callback handler.

`SubscriberPtr::Reset()` is only a reference-count operation in the vendor API;
there is no per-subscriber `Disconnect()` method. It is not a callback
synchronization boundary. Use `AutoSubscriber::Stop()` for an auto subscriber,
or `IOManager::Shutdown()` for ordinary subscribers, before their handlers go
out of scope. An owner-thread shutdown waits for in-flight callbacks.

For a focused stream, replace `mock::SubscribeAll()` with the normal SDK
template API:

```cpp
subscriber->SubcribeMessage<mdl_shl2_msg::SHL2MarketData>();
subscriber->SubcribeMessage<mdl_shl2_msg::SHL2Transaction2>();
subscriber->SubcribeMessage<mdl_shl2_msg::Order>();
subscriber->SubcribeMessage<mdl_szl2_msg::Snapshot300111_v2>();
subscriber->SubcribeMessage<mdl_szl2_msg::Order300192_v2>();
subscriber->SubcribeMessage<mdl_szl2_msg::Transaction300191_v2>();
```

`mock::SupportedMessages()` is the authoritative runtime list of accepted
SID/version/MID combinations. Do not infer support merely because a message
struct exists in the vendor SDK.

## Switching between real and mock feeds

Keep feed construction at one boundary in the application:

```cpp
IOManagerPtr CreateMarketDataIO(bool use_mock,
                                const mock::Config& mock_config) {
    if (use_mock) {
        return mock::CreateIOManager(mock_config);
    }
    return datayes::mdl::CreateIOManager(/*work_threads=*/4);
}
```

The real-feed branch still needs its normal server address, token, encoding, and
subscriptions. The mock branch does not contact the network. Downstream
handlers should not need a mock-specific payload type.

There are two intentionally different CMake linkage targets:

- A mock-only process links `L2Flow::l2mock_standalone`. This target includes
  local fallbacks for the non-inline refcount and encoding helpers declared by
  the SDK headers, so `libmdl_api.so` is not required.
- A process containing the real/mock factory switch above links
  `L2Flow::l2mock` **and** the real `libmdl_api.so`. The core target deliberately
  contains no fallback `DllConvert*` or `DllInterlocked*` definitions, so the
  vendor's real ANSI/UTF-8 conversion cannot be preempted by the mock.

Do not link `L2Flow::l2mock_standalone` into a process that also loads the
vendor library. A minimal mixed-feed CMake boundary is:

```cmake
target_link_libraries(market_data_app
    PRIVATE
        L2Flow::l2mock
        /path/to/libmdl_api.so)
```

## Configuration

Important defaults:

| Field | Default | Meaning |
| --- | ---: | --- |
| `seed` | `0x4c32464c4f57` | Random stream seed |
| `messages_per_second` | `100000` | Aggregate target rate; `0` means unpaced |
| `shanghai_instruments` | `2500` | Synthetic count when no custom universe is supplied |
| `shenzhen_instruments` | `2500` | Synthetic count when no custom universe is supplied |
| `derivatives_per_market` | `64` | Synthetic count for each derivatives service |
| `include_derivatives` | `true` | Include synthetic derivatives services |
| `book_depth` | `10` | Generated price levels |
| `orders_per_level` | `4` | Per-level order-queue entries where the SDK type has them |
| `callback_threads` | `4` | Worker threads available for callbacks |
| `callback_queue_capacity` | `65536` | Bounded callback queue size |
| `backpressure` | `Block` | Block generation or drop the newest full-queue callback |
| `clock_mode` | `Realtime` | Wall-clock-derived or simulated event time |
| `max_realtime_lag_ms` | `5` | Maximum synthetic lag in realtime mode |
| `simulated_start_date` | `20260105` | Deterministic Gregorian weekday (`yyyymmdd`) used in simulated mode |

Call `mock::ValidateConfig()` before creating the manager. It performs no
filesystem or network access.

For repeatable callback order as well as repeatable generated values, keep
`simulated_start_date`, the universe, and the seed fixed, use
`ClockMode::SimulatedTradingDay`, `callback_threads = 1`, and
`BackpressurePolicy::Block`.
Multi-threaded callbacks can be scheduled in different orders even when the
generated sequence is deterministic.

## Strict and sanitizer builds

Production Phase 0–1 sources compile as C++20 with `-Wall -Wextra -Wpedantic
-Wconversion -Wshadow -Werror`. The mock remains C++17 compatible.

```bash
# ASan + UBSan
cmake -S . -B build-asan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DL2FLOW_ENABLE_ASAN_UBSAN=ON
cmake --build build-asan --parallel
ASAN_OPTIONS=detect_leaks=0 \
  ctest --test-dir build-asan --output-on-failure

# TSan is a separate, mutually exclusive configuration
cmake -S . -B build-tsan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DL2FLOW_ENABLE_TSAN=ON
cmake --build build-tsan --parallel
ctest --test-dir build-tsan --output-on-failure
```

`detect_leaks=0` is only needed in ptrace-managed/container environments where
LeakSanitizer cannot operate; it does not disable AddressSanitizer or UBSan.

## Authoritative universe

The default universe is intentionally synthetic. Applications that need their
real security master must supply it explicitly:

```cpp
mock::Config config;
config.instruments.clear();

for (const SecurityMasterRow& row : dated_security_master) {
    mock::Instrument instrument;
    instrument.service_id = row.mdl_service_id;
    instrument.security_id = row.security_id;
    instrument.reference_price_milli = row.reference_price_milli;
    instrument.tick_size_milli = row.tick_size_milli;
    instrument.lot_size = row.lot_size;
    instrument.option = row.is_option;
    config.instruments.push_back(instrument);
}
```

When `Config::instruments` is non-empty, it replaces the entire synthetic
universe. The caller remains responsible for the source date, listing status,
venue mapping, tick size, lot size, and option classification.

Prices in `Instrument` use integer thousandths: `12.345` is stored as `12345`,
and a `0.010` tick is stored as `10`. Payload builders convert that value to the
decimal placement declared by each SDK field.

## Backpressure and statistics

- `Block` is lossless at the mock queue boundary and intentionally lets a slow
  callback reduce generator throughput. If a callback re-enters
  `AsyncPublish()` while the bounded queue is already full, that newest work is
  rejected instead of blocking a worker on its own queue.
- `DropNewest` keeps the generator moving when the queue is full and increments
  `Statistics::dropped`.
- `generated`, `delivered`, `filtered`, `dropped`, `callback_errors`,
  `queue_high_watermark`, and `active_subscribers` allow a test to distinguish
  generator throughput from consumer throughput.

Callbacks may run concurrently. A handler passed with
`multithread_callback=true` must protect its own mutable state and should not
allow exceptions to cross the SDK callback boundary.

For `multithread_callback=false`, callbacks to that subscriber are serialized,
including callbacks caused by `SyncPublish()`. The serialization gate is shared
by all such subscribers attached to one mock manager, which also makes nested
synchronous publication between them safe. `AsyncPublish()` and
`AsyncResponse()` always enqueue callback work, independently of this flag.
Calling shutdown/stop from inside a callback requests teardown without waiting
on the current callback; make a second `IOManager::Shutdown()` call from the
owner thread before destroying handlers.

Statistics use two different units: `generated` and `filtered` count source
events, while `delivered` and `dropped` count subscriber deliveries. With two
subscribers interested in the same event, one generated event can therefore
produce two deliveries. Stop and shutdown may cancel callback work that was
already queued, so no accounting identity should be assumed while subscription
state is changing.

Field-value filtering uses `SecurityID` for SH/SZ and `InstruID` for the five
derivatives services, with `*` and `?` wildcards. A field name belonging to a
different message family, or any other field name, is retained in the
SDK-compatible subscription description but does not match messages.

## Data-safety boundary

The mock is suitable for:

- parser and callback integration tests;
- throughput, queue, backpressure, and concurrency tests;
- order-book data-structure stress;
- deterministic regression scenarios.

It does not claim:

- a current list of exchange-listed instruments;
- official trading calendars, night sessions, auctions, suspensions, or price
  limits;
- authoritative meanings for exchange business-code fields;
- exchange matching-engine, cancellation, or order-priority behavior;
- realistic price discovery, liquidity, or statistical distributions;
- production market-data entitlements, accuracy, or latency.

See [`docs/mock_design.md`](docs/mock_design.md) before using generated fields
in assertions.
