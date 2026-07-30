# L2Flow

L2Flow is a C++20 realtime market-data runtime. The production chain is:

```text
strict premarket daily A-share catalog
  -> exact-key sort/deduplicate
  -> dense session-local IDs
  -> Store/runtime/IPC preallocation
  -> Wire V2.2 ACTIVE

single-threaded vendor SDK callback
  -> inspect and exact-key extraction
  -> A-share classification
  -> immutable catalog lookup
  -> one bounded owned-message copy
  -> direct nonblocking source-decoder admission

four serial source decoder lanes
  -> bounded cross-lane applied gate
  -> full decode and exact-key revalidation
  -> intraday Store, latest IPC, and KLine
  -> contiguous applied watermark
  -> parked all-lane generation fence
```

The four lanes are fixed: Shanghai snapshot, Shanghai NGTSTick, Shenzhen
snapshot, and one shared Shenzhen 6.33/6.36 tick lane. This preserves the
Shanghai phase state machine and Shenzhen order/trade/cancel source order.
The SDK remains configured for one callback thread.

## Daily-catalog contract

Before SDK Connect, production must load an absolute, regular, non-symlink
catalog file that declares the configured trade date, a positive source
version, complete Shanghai+Shenzhen subscription coverage, and exact opaque
`SecurityID`/`SecurityIDSource` bytes. Wire V2.2 exposes:

```text
catalog_scope             = DECLARED_DAILY_A_SHARE
catalog_coverage_complete = true
catalog_generation        = 1
bound_count               = capacity
```

Entries are filtered with the same A-share classifier used by the callback,
sorted by exact `(market, SecurityIDSource bytes, SecurityID bytes)`, and
deduplicated before assigning:

```text
instrument_id = ordinal + 1
ordinal       = instrument_id - 1
```

Identity is immutable for the session. Mutable availability and generation
state live in a preallocated dense array. `CATALOG_ALL` includes catalog
members with no data; `AVAILABLE_ANY` includes only identities with an applied
snapshot or tick. A catalog member with no data returns an empty successful
history result. A key absent from the catalog is an explicit unknown
instrument. IDs and rolling cursors are scoped by run/session/trade date and
catalog digest and cannot cross sessions.

`coverage_complete=true` means only that the premarket source declares the
configured Shanghai+Shenzhen A-share subscription scope complete. It does not
claim complete exchange-wide products, complete-from-open history, or that
every catalog member has received data.

## Latency-sensitive path

Filtering and catalog lookup occur before pool acquisition and all sequence
allocation. A non-A-share message consumes no global, source, or tick
sequence. A structurally valid A-share catalog miss is fatal before sequence
commit. There is no global ProcessingQueue, ProcessingLoop, dynamic
`BindOrGet`, or runtime IPC binding publication.

Each source ring has `Q` logical message slots plus one reserved control slot.
Message admission fully constructs its command, release-publishes all accepted
frontiers, and only then release-publishes the queue tail. Full/closed queues
and pool exhaustion do not commit candidate sequences and fail the session
closed; SDK messages are never silently dropped.

After pop and before full decode, each lane enforces:

```text
D = min(4*Q + 4, completion_tracker_capacity - 1,
        tick_ring_capacity - 1)
0 < global_sequence - applied_sequence <= D
```

Cross-source completion may be out of order, but only the contiguous
completion prefix is published as `applied_sequence`. Backlog remains
source-local until that lane exhausts its own capacity.

The accepted/applied status tuple is coalesced by a background
publisher on a 1 ms cadence. Per-record progress notifications therefore do
not compete continuously with first-availability updates; latest slot
publication and reads do not wait for this status publisher.

Known-ID C/Python latest reads acquire `bound_count`, use direct ordinal
arithmetic, and copy one fixed data slot. A valid published slot is
self-identifying, so the available-data hot path does not copy the mutable
instrument row; only an unpublished type slot takes that cold path to
distinguish `BOUND_NO_DATA` from `TYPE_UNAVAILABLE`. Latest reads do not
resolve keys, rebuild the catalog index, scan the catalog, or contend on the
status seqcount. Exact key lookup uses a separate immutable local index.

A generation cut takes the short admission lock, captures global/source/tick
cuts, and inserts one reserved FIFO fence into every lane. A lane drains its
pre-cut messages, reaches the fence, and parks. Once all four are parked, the
coordinator waits for the contiguous applied cut, snapshots catalog/runtime
availability, begins History generation, asks each source owner to seal, and
releases all lanes together. Post-cut records cannot enter History or mutate
generation availability before the snapshot. Timeout, begin, or seal failure
wakes every lane and fails closed.

The runtime itself does not persist captured messages. It has no startup
replay gate, Raw WAL, intraday replay, or crash-recovery path. Deployments that
need recovery must retain an independent source capture and replay it in a
separate recovery workflow.

## Build and test

The default SDK header path is `mdl_sdk_2_13_234/include`; override
`L2FLOW_SDK_INCLUDE_DIR` when required.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Sanitizer builds are separate:

```bash
cmake -S . -B build-asan \
  -DL2FLOW_ENABLE_ASAN_UBSAN=ON -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build-asan -j
ctest --test-dir build-asan --output-on-failure
```

The production executable is `build/mdl-production-router`. Use
`--help` for the complete validated option set. A minimal invocation shape is:

```bash
build/mdl-production-router \
  --sdk-library /absolute/path/to/vendor.so \
  --session-epoch 1 \
  --trade-date 20260730 \
  --daily-catalog /absolute/path/to/daily.catalog \
  --catalog-version 20260730 \
  --server-address HOST:PORT \
  --user-name USER \
  --ipc-socket /absolute/path/to/l2flow.sock \
  --intraday-store-max-records 100000000 \
  --intraday-store-memory-gib 64 \
  --intraday-store-from-open
```

The control socket must not already exist. Operational code must assert
`--intraday-store-from-open`; this replacement runtime does not offer a
partial-session or recovery startup mode.

The catalog is canonical ASCII with LF endings and lowercase exact-byte hex:

```text
L2FLOW_DAILY_INSTRUMENT_CATALOG_V2	20260730	20260730	sh+sz	complete
sh	-	363030303031	share	equity	documented_core	-
sz	31303220	303030303031	share	equity	documented_core	-
```

The Shenzhen source above decodes to the four bytes `102 `; the loader never
trims or normalizes it.

For the strict live order-event path, add
`--event-aggregator-socket /absolute/private/events.sock` to the router and
start `build/mdl-order-event-aggregator` against the router's source socket:

```bash
build/mdl-order-event-aggregator \
  --source-socket /absolute/private/l2flow.sock \
  --event-socket /absolute/private/events.sock \
  --session-epoch 1 \
  --trade-date 20260730 \
  --shanghai-state-capacity 5000000 \
  --shenzhen-state-capacity 5000000 \
  --event-ring-capacity 1048576 \
  --event-maximum-mapping-bytes 1073741824 \
  --read-batch-records 4096 \
  --poll-ms 1 \
  --timeout-ms 1000
```

Both socket parents must be same-UID, owner-only directories and neither
socket may already exist. With the event socket configured, the router starts
its source IPC service, then waits for an event service with the exact source
run and frozen daily-catalog identity at the zero-prefix origin before
creating the SDK pipeline. Size the source and event rings for measured rates
and maximum reader pauses; an overrun fails closed and this version does not
catch up or recover.

### Default Mainland A-share admission filter

The production callback and premarket catalog builder both apply the same
mandatory, non-configurable classifier. The classifier applies the following
rule:

1. The classification predicate accepts a `SecurityID` only when it is
   exactly six ASCII decimal digits. Signs, spaces, shorter IDs, and longer
   IDs do not match.
2. The exchange comes from the trusted supported message tuple, never from
   the code text alone.
3. The exchange-specific allow-list is:

   | Exchange | Accepted A-share code |
   | --- | --- |
   | Shanghai | `600xxx`, `601xxx`, `603xxx`, `605xxx`, `688xxx` |
   | Shenzhen | `000001-000999`, `001200-004999`, `300000-309799` |
   | Beijing | `920000-920999` |

This deliberately excludes, among other products, Shanghai B shares and
funds, Shenzhen B shares, main-board and ChiNext depositary receipts, and the
non-`920` Beijing codes. Beijing completed the migration of listed stocks to
`920xxx` on 2025-10-09; `83`/`87`/`88` identify National Equities
Exchange and Quotations ordinary shares under the current rule. Historical
replay across the Beijing migration requires the official per-security
mapping and trade date, not a legacy-prefix rule or a mechanical replacement
of the first three digits.

A well-formed supported non-match is returned as
`filtered_non_a_share`, counted separately per source, and discarded before
owned-message acquisition or global/source/mixed-tick sequence allocation.
Unsupported tuples remain `ignored_unsupported`. A structurally malformed
required instrument key (including an invalid descriptor, empty value,
overlap, embedded NUL, or other non-printable/non-ASCII bytes) is fatal and
is counted as rejected, not filtered.
The receive trade-date guard runs before the filter so that a non-A-share
callback on the next civil date still closes the prior-day session.

The classifier includes the Beijing rule, but the current production message
catalog contains only Shanghai and Shenzhen tuples
(`4.101.{4,24}` and `6.101.{28,33,36}`). The Beijing classification rule does
not add a Beijing subscription or decoder.

The rule baseline is 2026-07-30:
[SSE code allocation guide (2026 second revision)](https://www.sse.com.cn/lawandrules/guide/stock/jyglywznylc/zn/c/c_20260713_10825354.shtml),
[SZSE security code ranges (2026-03)](https://www.szse.cn/marketServices/technicalservice/doc/P020260306733846760075.pdf),
and the
[BSE 920 code rule](https://www.bse.cn/uploads/6/file/public/202404/20240419164341_dntowohn65.pdf).
Update the centralized predicate and its boundary tests together if an
exchange revises these allocations.

## Python Wire V2 reader

The Python package is stdlib-only and loads `libl2flow_shm_reader.so` through
`ctypes`.

```bash
PYTHONPATH=python \
L2FLOW_SHM_READER_LIBRARY=build/libl2flow_shm_reader.so \
python3 python/examples/read_latest_snapshots.py \
  --control-socket /absolute/path/to/l2flow.sock \
  --instrument-ids 1,2
```

The main API is:

```python
from l2flow_realtime import L2FlowClient, SelectionScope

with L2FlowClient.connect("/absolute/path/to/l2flow.sock") as client:
    session = client.session_info()
    available = client.select(SelectionScope.AVAILABLE_ANY)
    snapshots = client.latest_snapshots(available.instrument_ids)
```

`select()` returns its IDs and counts in one coherent daily-catalog envelope.
Once a caller has a session-scoped ID, `latest_snapshot()`,
`latest_tick()`, and `latest_kline()` use the direct known-ID path.

The market library also provides deterministic order-analysis cores for the
Shanghai 4.24 combined order/trade stream and the Shenzhen 6.33/6.36 streams.
They preserve raw trades/cancels, publish revisioned order states, keep
source/ingress/tick sequence domains separate, and use integer fixed-point
arithmetic throughout. The Shanghai T-only reconstruction reports fill volume
as a lower bound and BUY-max/SELL-min fill price as an inferred execution
boundary, never as a proven original limit. See
[`docs/order-event-reconstruction-v1.md`](docs/order-event-reconstruction-v1.md)
for phase, quantity, sequence, and quality-flag semantics.

The supported per-instrument raw/normalized event history API provides both an
initial finite read and checkpoint-based rolling updates through one reusable
isolated worker process:

```python
with L2FlowClient.connect("/absolute/path/to/l2flow.sock") as client:
    with client.open_instrument_raw_event_history(
        raw_event_columns=(
            "ingress_sequence",
            "tick_stream_sequence",
            "action",
            "primary_order_id",
            "buy_order_id",
            "sell_order_id",
            "price_p6",
            "quantity_raw",
            "quantity_scale",
        )
    ) as history:
        with history.read_all(1) as initial:
            for batch in initial.batches():
                consume(batch.materialize_all())
            checkpoint = initial.verified_checkpoint

        with history.read_updates(1, checkpoint) as update:
            for batch in update.batches():
                consume(batch.materialize_all())
            checkpoint = update.verified_checkpoint
```

This interface contains Wire V2 source-slot 1/3 Shanghai tick events and
Shenzhen order/transaction events. It is suitable as aggregator replay input;
it contains neither snapshots nor derived canonical order-lifecycle events.
A checkpoint becomes usable only after explicit EOF has been consumed. See
[`docs/instrument-raw-event-history-api.md`](docs/instrument-raw-event-history-api.md)
for lifecycle, coverage, column-selection, and ordering details.

The formal derived history reader is instrument-bound and keeps the same
native Shanghai/Shenzhen order state across the initial replay and rolling
updates:

```python
with client.open_instrument_derived_event_history(1) as history:
    with history.read_all() as initial:
        for batch in initial.batches():
            consume(tuple(batch))
        checkpoint = initial.verified_checkpoint

    with history.read_updates(checkpoint) as update:
        for batch in update.batches():
            consume(tuple(batch))
        checkpoint = update.verified_checkpoint
```

It returns source trades/cancels/status plus revisioned order snapshots; it is
not the raw Wire API above. A generation boundary never finalizes orders, so
T-to-A transitions remain continuous across updates. See
[`docs/instrument-derived-event-history-api.md`](docs/instrument-derived-event-history-api.md).

The standalone `mdl-order-event-aggregator` consumes every record in the
dense global tick ring and publishes a fail-closed event-delta memfd ring. It
assigns a distinct dense derived-event sequence, advances the source-tick
cursor even for zero-event ticks, and publishes each source cursor only after
that tick's complete derived batch. C++, stable C ABI, and Python readers use
an authenticated same-UID control socket and an O_RDONLY sealed mapping:

```python
with client.open_live_order_events(
    "/absolute/private/events.sock"
) as live:
    for batch in live.read_available(maximum_batches=8):
        consume_zero_copy(batch.buffer)
```

A finite reader batch can split an already committed source tick; consumers
which need tick-atomic upserts group by `tick_stream_sequence` and use
`batch.drains_published_prefix` before committing the trailing group. Source
clean-stop drains and clean-stops the event ring, but does not invent a
source-free Shenzhen end-of-day finalization. See
[`docs/order-event-reconstruction-v1.md`](docs/order-event-reconstruction-v1.md)
for the control protocol, capacity invariant, and failure semantics.

The precise runtime, count, and generation semantics are documented in
[`docs/daily-instrument-catalog-runtime-v2.md`](docs/daily-instrument-catalog-runtime-v2.md).
