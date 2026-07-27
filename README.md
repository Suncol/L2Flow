# L2Flow

L2Flow is a C++20 realtime market-data runtime with one production data chain:

```text
operator-selected Vendor SDK shared library
  -> one physical IOManager and one physical Subscriber
  -> serialized subscription callback
  -> immutable owned ingress message
       |-> optional best-effort audit WAL
       `-> source decoder
            -> instrument_id % worker_count router
            -> mandatory complete intraday instrument store
            -> optional event-time multi-window KLine aggregation
            |-> applied latest snapshot/tick read model
            |-> optional read-only Linux memfd IPC projection
            -> generation barrier and ingress-prefix watermark
            -> full-universe factor calculation
            -> one atomic factor-generation publication
```

The production executable is `mdl-production-router`. The live latest read
model is a read-only projection of the same successfully applied Store
records; it is not a second admission, replay, normalization, or retention
route.

## Production contract

### SDK and subscription

The runtime loads exactly the path supplied through `--sdk-library` with:

```text
dlopen(path, RTLD_NOW | RTLD_LOCAL)
dlsym(handle, "DllCreateIOManager")
```

The SDK shared object is not copied, hashed, archived, compared byte-for-byte,
or approved against a repository snapshot. Load failure, a missing
`DllCreateIOManager`, SDK object creation failure, configuration failure, and
connection failure are reported directly.

One physical Subscriber is created with `multithread_callback=false` and owns
exactly these five subscriptions:

```text
4.101.4
4.101.24
6.101.28
6.101.33
6.101.36
```

`6.101.53` is the combined Shenzhen tick feed and is explicitly forbidden. It
must not be mixed with the independent order and transaction subscriptions.

The narrow SDK lifecycle contract requires `IOManager::Shutdown()` to return
only after all callbacks have returned and no queued or new callback can
start. Shutdown order is:

```text
close callback admission
-> IOManager Shutdown / callback quiescence
-> Subscriber ReleaseRef
-> IOManager ReleaseRef
```

The successfully loaded DSO mapping is retained for process lifetime because
vendor process-global teardown is not treated as an in-process reload
boundary. SDK objects are still shut down and released explicitly.

### Owned ingress and WAL isolation

The callback inspects the vendor message once, then copies its 23-byte header
and declared body into one bounded size-class-pool allocation. Vendor callback
memory may be released immediately after the callback returns. When WAL is
disabled the pooled message has one owner from callback to decoder; when WAL
is enabled an intrusive reference shares that same immutable allocation with
the audit writer. Pool exhaustion rejects the callback instead of falling
back to an unbounded allocation.

The decoder queue and optional WAL receive the same immutable owner. WAL is a
side sink only:

- WAL status never grants or rejects instrument-store admission.
- Queue pressure, open/write/sync/close failure, or a stopped writer causes
  sticky audit `coverage_lost` and an operational warning.
- WAL offsets and durability are not part of a store watermark.
- A WAL failure cannot make an otherwise valid store or factor generation
  incomplete.

### Routing and mandatory intraday storage

The callback owns one dense process-wide ingress sequence and four dense
per-source sequences. Rejected and ignored callbacks consume no sequence.
`UINT64_MAX` is reserved for an exclusive cut and is never assigned to a
message.

There are four serial decoder sources:

| Source slot | Message stream |
|---:|---|
| 0 | Shanghai snapshot `4.101.4` |
| 1 | Shanghai tick `4.101.24` |
| 2 | Shenzhen snapshot `6.101.28` |
| 3 | Shenzhen order `6.101.33` and transaction `6.101.36` |

Decoder validity is semantic, not just structural. Known absolute prices must
be non-null and strictly positive before checked p6 normalization; zero or
negative prices remain retained but invalid. Negative quantities likewise
retain raw values while remaining invalid and absent from tick validity bits.
Product-specific SH/SZ fields, including the vendor ETF group, stay raw and
invalid until a versioned capability table proves applicability; a coarse
equity/fund/bond/option label is not enough. SH maximum-duration values keep
their vendor raw unit, with `UINT32_MAX` treated as the observed unavailable
sentinel and no unit guessed.

The registry lookup returns both the stable registry ordinal and instrument
ID. The history runtime resolves those values once to a session-bound route
token, then routes the moved decoded event permanently by:

```text
worker = instrument_id % store_worker_count
```

The production chain always retains every accepted record for the process
trade date in four append-only source lanes per instrument. Cross-source
decoder arrival may be out of order; generation queries merge the captured
lanes by the dense process-wide ingress sequence. The fixed instrument
registry is the generation universe, including instruments with no records.

The owner worker constructs the exact concrete event once in an append-only
per-instrument/source segmented arena. Compact record headers cache ordering
metadata and point to their arena-local payload by relative offset.
Generation cuts capture segment endpoints, visible byte/record counts, and
latest-record locators rather than copying accumulated records, so publication
remains O(the fixed instrument universe) as the session grows. Record and
logical-byte limits are mandatory, and reaching either limit fails the
production pipeline closed; old records are never evicted to regain capacity.

The store is memory-only. It does not replay the optional WAL or feeder CSV;
the feeder's append-only CSV is outside this project's recovery path. After a
mid-session process start or restart, the new process can cover only records
accepted after that start and must not claim `coverage_from_open`.

On a 1 TiB host, the initial envelope is a 600--620 GiB logical store limit,
an 800 GiB process high-water alert, and an 850--860 GiB termination boundary.
Old factors, store generations, and cursors pin their owning session arena, so
consumers must enforce a small fixed limit on retained handles.

### Live latest snapshot/tick reads

The owner worker publishes a Store-owned immutable record to the live latest
read model only after all of these steps succeed:

```text
Store append
-> every enabled KLine update
-> history handoff release
-> release-publish latest pointer
```

The production facade exposes allocation-free point and batch APIs:

```cpp
RealtimeLatestRecordViewV1 snapshot;
pipeline->GetLatestSnapshot(instrument_id, &snapshot);

std::array<std::uint32_t, 2> ids{sh_id, sz_id};
std::array<RealtimeLatestRecordViewV1, 2> snapshots;
pipeline->GetLatestSnapshots(ids, snapshots);
```

`GetLatestTick(s)` uses the same query implementation. Its meaning remains
the Store's existing mixed non-snapshot category: Shanghai tick, Shenzhen
order, or Shenzhen transaction. It must not be interpreted as “latest
trade”. Snapshot and tick slots advance independently, and a candidate only
replaces the current slot when its process `ingress_sequence` is greater.
Every successfully applied event is release-published to its point slot, but
this remains a latest-value cache: a polling reader can miss intermediate
values when writers advance faster than it reads. Lossless per-record
consumption requires a sequence cursor, not a latest API; the optional Linux
IPC service described below provides a bounded mixed-tick ring for that use
case.

Each batch row is one acquire observation and reports `available`,
`not-yet-observed`, `unknown-instrument`, or `invalid-instrument-id`
independently. A batch is not a market-wide atomic cut. Consumers requiring a
repeatable cross-instrument prefix must continue to use an immutable Store
generation. Separate snapshot and tick calls are likewise independent
observations and do not form a joint cut.

Returned records are borrowed from the Store arena. They remain valid while
the pipeline/runtime is alive, including after a clean `StopAndDrain`, but
must not themselves become a cross-process ABI or outlive the runtime. The
optional IPC service projects them into an independent, versioned wire
representation.

### Python/Polars read-only IPC

On Linux, `mdl-production-router` can expose the already-applied latest
snapshot core projection, mixed latest ticks, latest KLines, and a bounded,
sequence-checked mixed-tick cursor through a sealed memfd. An owner-only
Unix-domain control socket passes only a read-only descriptor to same-UID
clients. The core Pipeline remains unaware of sockets, Python, Arrow, or
Polars. Snapshot fields outside the V1 projection—including some normalized
status, time-validity, and market-specific fields—remain available only from
the complete C++ Store event; the linked specification lists the boundary
explicitly.

Enable it with `--ipc-socket`; tune the ring and total mapping cap with
`--ipc-tick-ring-records` and `--ipc-max-mapping-mib`. The socket must be an
absolute path below an operator-owned directory with no group/other
permissions (normally mode `0700`). The Python client uses the native C reader
for acquire/consistent copies, then optionally builds PyArrow or Polars
micro-batches. See
[Realtime Python/Polars IPC V1](docs/realtime-python-polars-ipc-v1.md) for
startup, API, cursor, heartbeat, overrun, ABI, and zero-copy constraints.

### Generation barrier and watermark

A generation cut is serialized with callback admission. The runtime captures
one exclusive global prefix and one exclusive prefix for each source, then
places a marker behind all pre-cut messages in every serial decoder queue.
Every store worker receives all four source fences before it captures its
slice. Only after every worker slice is present and the complete registry
universe and source counts have been verified is one immutable store
generation published.

For every valid watermark:

```text
global_exclusive - 1
  == sum(source_exclusive[i] - 1 for i in 0..3)
```

The watermark proves completeness of the ingress prefix allocated by this
process. It does not prove exchange-event-time completeness, upstream packet
completeness, or WAL durability. `recv_monotonic_cut_ns` is captured at the
actual admission cut, including a civil-date boundary; slow SDK shutdown does
not make the prefix appear fresher.

### Factor publication and reader rule

A calculator receives one immutable, barrier-complete store generation. It
uses the latest-only `SummaryAt(index)` view to transform the fixed universe in
O(instrument count), without scanning the session record stream. Its output
must contain exactly one row per registry instrument in the same order, with
the exact fixed factor schema. The engine rejects missing or reordered rows,
wrong column counts, NaN/infinity, and non-canonical invalid values.

The published factor generation is one atomic shared handle. It retains the
exact input store handle and copies the exact same watermark. Readers that
need a consistent store/factor pair must acquire the factor once and derive
the matching store from it:

```cpp
auto factor = pipeline->AcquireLatestFactorGeneration();
if (factor != nullptr) {
    const auto& matching_store = factor->input_store();
    // factor and matching_store describe the exact same ingress prefix.
}
```

Do not independently acquire the latest store and latest factor and assume
they have the same generation: store generation `N` is necessarily published
before factor generation `N`, so two latest-slot reads may briefly observe
different generations.

Full-session consumers use store cursors: a full-universe drain over I
instrument rows and N records is O(I + N), and each `ReadBatch` uses O(batch)
caller-owned pointer storage rather than allocating a second full-session
result. `maximum_records_per_batch` is only an upper bound; the acceptance
reader keeps that Store limit at 65,536 but defaults the actual `ReadBatch`
span to 1,024 and consumes each page immediately. Large drains can be split
into independent half-open instrument-ordinal ranges with
`OpenUniverseRangeCursor`; one range costs O(I_range + N_range). With an
untruncated `maximum_records` setting, concatenating non-overlapping ranges in
ordinal order is identical to one full-universe cursor.

The acceptance probe exposes `--intraday-scan-batch-records`,
`--intraday-scan-workers`, and `--intraday-reader-cpus`. Single-reader scans
are pinned to one allowed CPU; 4--8 reader scans use record-balanced,
non-overlapping ordinal ranges and one CPU per reader. Store worker count and
segment target size remain separately configurable, for example 4/8 workers
and 64/256 KiB segments in A/B runs. CPU affinity is applied only in the
post-stop reader threads, so the live SDK, decoder, and Store workers do not
inherit a single-core mask.

The default `SnapshotLastPriceProjectionV1` is deliberately literal:

```text
latest valid strictly-positive decoded snapshot last_price.normalized_p6 / 1,000,000
```

It is not an alpha, fair value, microprice, imbalance, trading signal, or any
other financial model. Missing, zero, or negative snapshot prices are
published as `{value=+0.0, valid=false}`; a pre-trade zero is not presented as
a formed market price. A production-specific calculator may be injected,
but it must be a pure, non-reentrant full-generation transformation and must
enforce a strict execution-time bound. The generation timeout is only a shared
wait budget for decoder-marker queue backpressure and the store condition
wait; it is not a wall-clock completion deadline and cannot preempt arbitrary
calculator code.

### One process, one civil trade date

Production pins a process to one fixed UTC+08:00 civil date. Startup rejects a
`--trade-date` different from the current fixed-UTC+08 date. Each callback
checks its receive realtime clock before sequence allocation. On the first
date mismatch, admission closes cleanly without assigning a sequence or
marking the data corrupt.

SIGINT, SIGTERM, or the date boundary uses the terminal publication path:

```text
close admission and capture the final prefix cut
-> quiesce SDK callbacks
-> inject final decoder markers
-> publish final complete store and factor generations
-> join decoder/store workers
-> drain and optionally fdatasync the WAL
-> stopped
```

## Instrument registry

The registry is an immutable fixed-universe identity. Its SHA-256 pin protects
the instrument universe and metadata; it is unrelated to SDK shared-library
selection.

The file format is strict LF-terminated text:

```text
L2FLOW_INSTRUMENT_REGISTRY_V1<TAB>registry_version<LF>
instrument_id<TAB>market<TAB>security_id_source_hex_or_-<TAB>security_id_hex<TAB>quantity_unit<TAB>security_type<TAB>asset_scope<LF>
```

Identifiers are exact opaque bytes in the registry. The production chain adds
these reachability constraints before any SDK connection:

- Shanghai `security_id_source` is empty (`-` in the file).
- Shenzhen `security_id_source` is exactly four bytes `"102 "`, encoded as
  `31303220`; trimming the trailing space is invalid.
- `security_id` is non-empty printable ASCII because that is the set the
  production decoder can accept and match.
- Only Shanghai and Shenzhen entries are accepted by this production router.

`tools/build_live_instrument_registry.py` builds the format from reference
inputs and emits the canonical registry SHA-256 in its JSON report:

```bash
python3 tools/build_live_instrument_registry.py \
  --sh-reference /secure/reference/sh.csv \
  --sz-reference /secure/reference/sz.csv \
  --registry-version 91 \
  --output /secure/l2flow/instruments.v1.tsv \
  --report-json /secure/l2flow/instruments.v1.report.json
```

The loader opens a single non-symlink filename below an already opened secure
directory and checks owner, mode, file type, link count, file stability,
version, and canonical registry digest. A typical deployment uses a private
directory and file, for example modes `0700` and `0600`.

## Build

Requirements:

- Linux or another platform with the equivalent dynamic-loader support used
  by the direct SDK adapter;
- CMake 3.16 or newer;
- a C++20 compiler;
- pthreads;
- vendor SDK headers containing `mdl_api.h`.

This cutover deliberately changes the existing V1 config layouts and
calculator virtual interface in place; there is no binary compatibility
adapter for the retired retention path. Deployment must use a clean rebuild
of the executable, static libraries, tests, and any injected calculator.
Previously compiled objects or plugins must not be mixed with this build.

The repository header location is the default. A different header directory
can be selected without changing runtime SDK selection:

```bash
cmake -S . -B build \
  -DL2FLOW_BUILD_TESTS=ON \
  -DL2MOCK_BUILD_TESTS=OFF \
  -DL2FLOW_SDK_INCLUDE_DIR=/path/to/vendor/include
cmake --build build -j
ctest --test-dir build --output-on-failure
```

The standalone feeder probe is diagnostic-only and is excluded by default:

```bash
cmake -S . -B build-probe -DL2FLOW_BUILD_FEEDER_PROBE=ON
cmake --build build-probe -j --target mdl_sdk_feeder_probe
```

It creates its own SDK objects for an operator-invoked feeder check and does
not publish production store or factor generations. It is not a second
production data chain.

The isolated vendor mock suite is also opt-in:

```bash
cmake -S . -B build-mock \
  -DL2FLOW_BUILD_TESTS=ON \
  -DL2MOCK_BUILD_TESTS=ON
cmake --build build-mock -j
ctest --test-dir build-mock --output-on-failure
```

Sanitizer build:

```bash
cmake -S . -B build-asan \
  -DL2FLOW_BUILD_TESTS=ON \
  -DL2FLOW_ENABLE_ASAN_UBSAN=ON
cmake --build build-asan -j
ASAN_OPTIONS=detect_leaks=0 \
  ctest --test-dir build-asan --output-on-failure
```

The direct-loader test builds a test-only shared library exporting
`DllCreateIOManager` and exercises the real `dlopen`/`dlsym`, object creation,
five subscriptions, and shutdown/release order. The test DSO is never linked
into a production target.

## Run

```bash
./build/mdl-production-router \
  --sdk-library /opt/vendor/lib/libmdl_api.so \
  --registry-directory /secure/l2flow \
  --registry-file instruments.v1.tsv \
  --registry-version 91 \
  --registry-sha256 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
  --trade-date 20260724 \
  --server-address 127.0.0.1:9112 \
  --user-name runtime-token \
  --sdk-log-prefix /var/log/l2flow/mdl \
  --instrument-store-workers 4 \
  --intraday-store-max-records 1000000000 \
  --intraday-store-memory-gib 600 \
  --intraday-store-from-open \
  --kline-windows-ms 1000,5000 \
  --generation-interval-ms 1000 \
  --generation-timeout-ms 10000 \
  --wal-path /var/lib/l2flow/audit.wal
```

`--replace-wal` is required to truncate an existing WAL path. Without it, an
existing path is refused by the optional WAL sink and reported as audit
coverage loss while realtime publication continues.

`--kline-windows-ms` enables one or more exchange-event-time windows. The
duration in milliseconds is also the public `window_id` (for example, `1000`
is the one-second window). Empty bars are not synthesized. Each immutable
`RealtimeKLineGenerationV1` exposes all bars for an instrument/window through
`OpenInstrumentCursor`; `coverage_from_open()` is true only when the matching
store has continuous from-open coverage. The KLine generation retains that
exact store generation, so consumers should acquire the KLine handle and use
`input_store()` rather than combining two independently acquired latest
handles.

Bucket selection and OHLC open/close ordering use the decoded message's
exchange timestamp. Callback receive clocks, SDK `LocalTime`, and the local
server's seconds do not participate. The configured process `trade_date`
provides the calendar date; a later-arriving or out-of-order trade revises the
appropriate bar in the next published generation without mutating older
generations.

The process exits nonzero on a fatal decode, routing, store, barrier, factor,
or SDK lifecycle error. A clean signal or civil-date boundary attempts one
final complete generation before stopping.

### Per-message callback and append latency diagnostics

`accept-realtime-pipeline` accepts the explicit diagnostic flag
`--measure-stage-latency`. A mid-session diagnostic must additionally pass
`--partial-session` instead of falsely asserting `--intraday-store-from-open`.
It enables bounded concurrent histograms on the
same production callback, decoder, router, and append path; it does not create
a second data path. The final JSON adds these per-message distributions:

- `sdk_local_to_callback_success`: callback-success `CLOCK_REALTIME` minus
  `MDLMessageHead::LocalTime`;
- `sdk_local_to_append_complete`: first realtime observation after
  `IntradayInstrumentStoreV1::Append` returns success, minus the same SDK
  header time;
- `callback_entry_to_success`: same-host monotonic callback work;
- `callback_entry_to_append_complete`: same-host monotonic queue/decode/route/
  append latency;
- `append_call`: the monotonic bracket beginning immediately before the
  successful-path input/route checks and ending immediately after the store
  append call returns. It is therefore a tight upper bound for the call, not
  an isolated function-body measurement.

The two `sdk_local_*` values are signed end-to-end observations, not pure
process latency. SDK `LocalTime` is only `hhmmssmmm` (one-millisecond
resolution) and has no date; the diagnostic projects it onto the configured
fixed-UTC+08 trade date. The result therefore also contains upstream feeder
delay and any realtime-clock offset. The three monotonic distributions are
the authoritative same-host stage measurements. Completion clocks are read
before histogram aggregation, but the extra clock reads and atomic updates
can still perturb later messages, so this mode is disabled by default.

## Code map

```text
include/l2flow/sdk, src/sdk
  direct SDK loading and the five-message subscription catalog

include/l2flow/realtime, src/realtime
  immutable ingress ownership and optional WAL side sink

include/l2flow/market, src/market
  decoder, fixed instrument registry, router, intraday store, barrier, watermark

include/l2flow/factor, src/factor
  calculator contract and atomic full-generation factor publication

include/l2flow/runtime, src/runtime
  the single production composition and lifecycle

apps/mdl_production_main.cpp
  production CLI, civil-date owner loop, terminal publication
```

For the detailed concurrency invariants, failure model, and publication
contract, see [docs/realtime-production-chain-v1.md](docs/realtime-production-chain-v1.md).
The complete-session memory, generation, query, sizing, and rollout contract is
documented in
[docs/intraday-instrument-store-v1.md](docs/intraday-instrument-store-v1.md).
