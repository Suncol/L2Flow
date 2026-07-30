# L2Flow

L2Flow is a C++20 realtime market-data runtime. The production chain is:

```text
vendor SDK callback
  -> bounded owned-message copy
  -> nonblocking ordered-processing-queue admission
  -> callback return

ordered in-memory processing dispatcher
  -> capture-ordered observed-instrument binding
  -> decoder and fixed ordinal worker
  -> intraday Store, latest IPC, and KLine
  -> contiguous applied watermark
  -> immutable Store/Factor generation
```

This branch implements a hard Wire V2 replacement. There is no immutable
startup registry, optional WAL side path, Wire V1 adapter, or compatibility
reader.

## Observed-universe contract

The runtime exposes only identities actually observed from the SDK during the
current session:

```text
catalog_scope     = OBSERVED_ONLY
coverage_complete = false
```

An unknown key means "not observed in this session"; it does not prove that a
security does not exist. APIs that require an authoritative exchange-wide
denominator must not treat this catalog as a complete market universe.

The directory preallocates a fixed number of ordinal slots. Production
defaults to 65,536:

```text
instrument_id = ordinal + 1
ordinal       = instrument_id - 1
```

IDs are scoped by `session_epoch`. A numeric ID cannot be reused in another
session without resolving its exact key again. Capacity and key-arena
exhaustion fail the session closed; this implementation deliberately has no
intraday resize or rollover path.

The live status contains:

- `capacity`, `bound_count`, and `available_count`;
- `snapshot_available_count`, `tick_available_count`, and
  `factor_eligible_count`;
- `catalog_generation`, `data_state_generation`, and `catalog_digest`;
- `accepted_sequence` and `applied_sequence`;
- `processing_lag_records = accepted_sequence - applied_sequence`;

The enforced relations are:

```text
factor_eligible_count <= snapshot_available_count
snapshot_available_count <= available_count <= bound_count <= capacity
tick_available_count <= available_count
applied_sequence <= accepted_sequence
```

Readers become usable while `bound_count == 0`; neither catalog completion nor
processing lag is a startup gate.

## Latency-sensitive path

The SDK callback performs no file I/O and no catalog scan. It copies into a
bounded pool once and nonblockingly transfers the immutable message to the
ordered-processing queue. Queue exhaustion fails the session closed instead
of silently dropping the record.

Before the SDK connects, the pool prewarms the complete bounded in-flight
window for wire messages up to 4,096 bytes and physically touches those
pages. The window includes ordered processing, decoder, batch, and
applied-completion ownership, whose retained sequence ranges need not overlap.
Prewarming is capped at 256 MiB, so unusually large configured windows do not
turn startup into an unbounded reservation. Larger legal messages retain the
bounded size-class fallback instead of being rejected or narrowing the
supported message limit.

The single in-memory dispatcher preserves capture order across all sources
before `BindOrGet`, then routes records to their source decoders. It enforces
an explicit applied-sequence window `W`: a record with capture sequence `s`
is routed only while
`0 < s - applied_sequence <= W`, and the IPC tick ring is required to have at
least `W` slots. This bounds out-of-order completion without making the SDK
callback wait on decoder, Store, or IPC work.

The accepted/applied status tuple is coalesced by a background
publisher on a 1 ms cadence. Per-record progress notifications therefore do
not compete continuously with early-session binding and first-availability
updates; latest slot publication and reads do not wait for this status
publisher.

Known-ID C/Python latest reads acquire `bound_count`, use direct ordinal
arithmetic, and copy one fixed data slot. A valid published slot is
self-identifying, so the available-data hot path does not copy the mutable
instrument row; only an unpublished type slot takes that cold path to
distinguish `BOUND_NO_DATA` from `TYPE_UNAVAILABLE`. Latest reads do not
resolve keys, rebuild the catalog index, scan the bound universe, or contend
on the global catalog status seqcount. Exact key lookup is a separate
operation whose local sorted index is rebuilt lazily only when
`catalog_generation` changes.

An explicit Store/Factor generation cut installs a processing-dispatch barrier
at the accepted sequence under a short admission lock. Callbacks resume queue
admission immediately; the cut waits only for that prefix to become
applied, then freezes its Catalog Snapshot and enqueues Store/Factor markers.
Each Store/KLine owner publishes a constant-size cut token after its source
fences; it does not scan or copy its instrument partition. The first post-cut
update to a row lazily freezes that row's pre-cut endpoint, while a background
builder materializes only the Catalog Snapshot's bound prefix. Unbound capacity
rows are not emitted. The cut is not a gate for live latest reads.

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
its source IPC service, then waits for an exact source run/epoch/day event
service at the zero-prefix origin before creating the SDK pipeline. Size the
source and event rings for measured rates and maximum reader pauses; an
overrun fails closed and this version does not catch up or recover.

### Default Mainland A-share admission filter

`RealtimePipelineConfigV1::enable_mainland_a_share_filter` and the production
option `--enable-mainland-a-share-filter true|false` both default to `true`.
The filter applies the following complete rule:

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
(`4.101.{4,24}` and `6.101.{28,33,36}`). Enabling this filter does not add a
Beijing subscription or decoder. Set
`--enable-mainland-a-share-filter false` only when the operator intentionally
wants the previous all-supported-products behavior.

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
    observed = client.select(SelectionScope.OBSERVED_ANY)
    snapshots = client.latest_snapshots(observed.instrument_ids)
```

`select()` returns its IDs and counts in one coherent observed-universe
envelope. Once a caller has a session-scoped ID, `latest_snapshot()`,
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

The precise runtime, count, generation, and bias semantics are documented in
[`docs/observed-universe-runtime-v2.md`](docs/observed-universe-runtime-v2.md).
