# L2Flow

L2Flow is a C++20 realtime market-data runtime. The production chain is:

```text
strict premarket daily A-share catalog
  -> exact-key sort/deduplicate
  -> dense session-local IDs
  -> Store/runtime/IPC preallocation
  -> Wire V2.5 ACTIVE

single-threaded vendor SDK callback
  -> inspect and exact-key extraction
  -> A-share classification
  -> immutable catalog lookup
  -> one bounded owned-message copy
  -> direct nonblocking source-decoder admission

four source decoder lanes
  -> adaptive inline full decode (legacy latency path), or
  -> Wd stateless parse workers + four source-ordered finalizers
  -> bounded cross-lane applied gate and exact-key revalidation
  -> intraday Store, latest IPC, and KLine
  -> contiguous applied watermark
  -> parked all-lane generation fence
```

The four source lanes are fixed: Shanghai snapshot, Shanghai NGTSTick, Shenzhen
snapshot, and one shared Shenzhen 6.33/6.36 tick lane. This preserves the
Shanghai phase state machine and Shenzhen order/trade/cancel source order.
The SDK remains configured for one callback thread. Parallel decode is
disabled by default. With a positive worker count and the production-default
idle-inline/activation settings, stateless parsing may run on multiple cores
only after a source Pop observes remaining ring occupancy at its bounded local
threshold. The observation reuses the tail acquire already required by Pop;
it does not add a second cross-core depth read. Aggregate pressure across
several sources does not lower that threshold.
This preserves the existing four-core source-owner parallelism for balanced
traffic while no individual source crosses its threshold. Diagnostic
idle-inline-off or zero-threshold configurations enter the farm immediately.
One ordered finalizer per source retains stateful decoder and History
ownership during an active farm interval.

## Daily-catalog contract

Before SDK Connect, production must load an absolute, regular, non-symlink
catalog file that declares the configured trade date, a positive source
version, complete Shanghai+Shenzhen subscription coverage, and exact opaque
`SecurityID`/`SecurityIDSource` bytes. Wire V2.5 exposes:

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

Wire minor 5 retains `LIVE_PARTIAL`, the explicit from-open,
startup-recovered, full-day KLine/Factor, and CERTIFIED-prefix flags, and the
minor-4 process-start KLine metadata. It adds session-wide History temporal
coverage independently of KLine, including the conservative post-connect
boundary for explicitly enabled process-start partial History. The C header
exports stable numeric enums and the Python model exposes the same contract,
but readers still validate the exact supported wire minor. Deploy the
producer, native reader library, and Python package together; an older reader
must reject a V2.5 mapping instead of silently interpreting the new fields.

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
D = min(4 * (Q + 1 + Wd*S), completion_tracker_capacity - 1,
        tick_ring_capacity - 1)
0 < global_sequence - applied_sequence <= D
```

`Wd` is the parallel decode-worker count and `S` is the preallocated lease
count per source/worker; `Wd*S` is zero in the default legacy topology.
Cross-source completion may be out of order, but only the contiguous
completion prefix is published as `applied_sequence`. In parallel mode,
source backlog can also occupy bounded per-worker issue queues, per-source
completion rings, and task leases; exhaustion remains explicit and
fail-closed.

During an active farm interval, an ordered committer may opportunistically
take at most 16 completion records that are already contiguous. It stops at
the first missing source sequence and never waits to fill a batch. Each event
is finalized and submitted to History immediately in source order; the first
record routed to each History worker is signaled immediately, while only
redundant later wake signals are coalesced until the bounded submission ends.
This internal microbatch is unrelated to the user-visible Batch-history
result.

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

The ordinary from-open path does not persist captured callbacks. Same-day CSV
startup recovery is online-only: the single SDK owner writes an asynchronous
disk-backed live journal, publishes a separate `LIVE_PARTIAL` latest-value
preview, and rebuilds a second shadow Store/KLine/Factor/CERTIFIED pipeline
before exposing the recovered socket. The coordinator validates a closed
CSV/live handoff with exact identity matches and immutable tuple cutoffs under
the explicit same-session, lossless-callback operating contract. There is no
synchronous CSV replay or in-memory startup-buffer path in the production
pipeline.

The online journal is a session-local bootstrap source, not an arbitrary
checkpoint or a previous-process restart source: a new run requires an empty
journal directory and this version does not resume existing segments after a
producer crash. The exact input, ordering, WAL, publication, and fail-closed
contracts are documented in
[`docs/csv-startup-recovery-v1.md`](docs/csv-startup-recovery-v1.md).

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

`--parallel-decoder-workers` accepts `0..64` and defaults to `0`. Zero keeps
the original one-full-decoder-owner-per-source behavior and creates no parse
worker or ordered-committer threads. `--parallel-decoder-workers 4` is an
explicit opt-in candidate profile, not a universal default: enable it only
after the target CPU placement, traffic distribution, callback-to-Polars
latency gate, and explicit backlog/pressure guardrails have passed the
validation procedure in
`benchmarks/PARALLEL_DECODER_VALIDATION.md`.

Current parallel issue/completion high-water telemetry cannot prove exact
unused capacity: one field is a conservative upper bound assembled from shard
maxima and the other is a periodically sampled lower bound on the true high
water. Absence of queue-full is therefore not a proof of queue headroom.

The production executable sizes each source decoder queue to 65,536 records,
each source/Store-worker queue to 32,768 records, and the default-on
CERTIFIED handoff queue to 4,194,304 records. These bounded defaults were
selected from the observed 09:30 failure with explicit headroom; they must be
re-certified in a subsequent from-open live session. Deployments may set
`--decoder-queue-records-per-source`,
`--store-queue-records-per-source-worker`, and
`--certified-handoff-queue-records` explicitly from their own peak-rate and
drain-time measurements; the certified handoff capacity must be a power of
two. Queue exhaustion remains explicit and fail-closed for FAST source
admission, while CERTIFIED exhaustion freezes only its last proven prefix.
The online recovery coordinator additionally treats that frozen/dropped
CERTIFIED state as terminal for promotion and the recovered session.

CERTIFIED live order state, derived Event rows, and canonical Tick history
have separate capacity units. Operators may set
`--certified-maximum-live-orders`, `--certified-maximum-events`, and
`--certified-maximum-ticks` independently. For backward-compatible defaults,
live orders and Tick history use `--intraday-store-max-records`, while Event
rows use four times the live-order cap. The live-order limit applies to each
market projector; none of these settings increases queue drain throughput.

Control socket paths must not already exist. Production requires exactly one
startup mode:

- `--intraday-store-from-open` asserts that this process started before the
  first relevant market message and stayed healthy; or
- `--intraday-recovery-csv-dir /absolute/path/to/same-day-csv` asserts that
  the vendor directory contains the current trade day's complete saved prefix
  from market open; or
- `--intraday-live-partial` explicitly starts a process-start-only service
  without CSV recovery and without claiming coverage from market open.

Partial mode uses `--ipc-socket` for FAST and the canonical
`--certified-ipc-socket` Event service. The router starts both workers and
control listeners behind one closed exposure gate before SDK Connect, then
connects the pipeline and opens the gate. FAST serves GET_SESSION plus latest
snapshot/tick reads with `server_state=LIVE_PARTIAL`. Each periodic generation
cut also exposes the complete retained single-instrument History from this
process's start and tick generation delta; before the first cut those opens are
temporarily unavailable. The generation truthfully carries
`record_coverage_complete=true` and `coverage_from_open=false`.
When `--kline-windows-ms` is present, the same cuts publish latest KLine data
using exchange-natural, local-midnight-aligned windows. Those bars contain only
trades received by this process. After SDK Connect succeeds, the router captures
one conservative process-start boundary and finalizes FAST/Event metadata before
opening the shared exposure gate. A materialized bar is marked left-truncated
exactly when its natural bounds satisfy
`window_start < boundary < window_end`; a no-trade window does not synthesize a
bar. The session advertises process-start partial KLine coverage rather than
full-day validity. The public IPC surface remains latest-KLine only; it does
not expose a partial KLine history cursor.
`startup_prefix_recovered`, both full-day validity flags, and
`certified_prefix_valid` remain false for the whole run.
The Event journal is explicitly `PROCESS_START_PARTIAL` and requires
`--native-maximum-backward-displacement D`. Shenzhen 6.33/6.36 share one
per-channel `ApplSeqNum` domain; the worker withholds output until its bounded
origin proof is complete, then publishes exact-next canonical order. A gap or
conflict freezes only that channel and reports `DEGRADED`; global Event
resource failure is also isolated from FAST. No timer flushes native order.
`--disable-native-gap-recovery` selects FAST-only partial. The mode creates no
startup buffer, live journal, CSV source, or shadow pipeline. It also configures
`factor_generation_enabled=false`, so
periodic Store generations remain publishable for History/delta without
creating or invoking the C++ generation Factor engine. The Event socket does
not claim a full-day Event/KLine prefix, and `certified_prefix_valid` remains
false. This is the
factually correct
mode for a mid-session launch that deliberately does not recover the
market-open prefix; using
`--intraday-store-from-open` in that situation remains an invalid operator
assertion.

This behavior applies only to standalone `--intraday-live-partial`. The separate
`LIVE_PARTIAL` socket used while CSV online recovery is running remains
snapshot/tick latest-only, carries no KLine windows, and continues to reject
History/delta, so recovery-side bulk reads cannot be introduced through the
preview endpoint.

CSV recovery is online whenever `--intraday-recovery-csv-dir` is present.
`--intraday-recovery-mode online` remains an optional explicit selector;
`blocking` is rejected. Online recovery requires all of:

```text
--intraday-recovery-csv-dir /absolute/path/to/same-day-csv
--intraday-recovery-journal-dir /absolute/path/to/empty-journal-directory
--live-preview-ipc-socket /absolute/path/to/live-preview.sock
--ipc-socket /absolute/path/to/recovered.sock
```

The callback first makes an independent vendor-head/body copy and reserves its
logical WAL bytes, then admits the original message to the preview pipeline.
That successful capture is an asynchronous retention boundary, not proof that
the record has already passed `fdatasync`. The WAL publishes a separate
`committed_serial` only after the complete batch, including every crossed
segment, is durable; shadow recovery never reads beyond that committed
frontier. Queue exhaustion, logical journal-capacity exhaustion, write/sync
failure, CRC/SHA mismatch, truncated segments, or a final
`committed_serial != accepted_serial` fails the session closed.

The preview and recovered services have different sockets and different
`run_id` values. Before promotion, the preview reports `LIVE_PARTIAL`,
`coverage_from_open=false`, and every full-day/recovered/CERTIFIED validity
flag false. It serves GET_SESSION and latest snapshot/tick reads only;
History/delta are rejected. Internally it currently retains a process-start
partial Store to keep latest-record lifetimes safe—it is not a specialized
physical latest-only container. The recovered mapping stays INITIALIZING and
has no externally usable control plane until shadow replay has reached a fixed
journal frontier, published its first Store/KLine/Factor generation, and
completed the CERTIFIED prefix barrier when that default-on sidecar is enabled.

The final promotion critical section rechecks the journal's health and durable
coverage of that fixed frontier, plus the single SDK/preview owner's accepting,
trade-date, pipeline, and IPC health, before and after starting the recovered
controls. Both controls start behind one false monotonic exposure gate, so
fallible thread/socket activation cannot transfer either descriptor. FAST is
prepared with its final CERTIFIED capability while still INITIALIZING; after a
last health confirmation and the nonzero promotion timestamp, one release
store opens both accept paths. Thus every successfully obtained recovered FAST
descriptor already reports ACTIVE and the final capability flags, and no
CERTIFIED Event descriptor can escape before the same promotion point. A
connection racing the closed gate can be rejected and must retry; it cannot
observe a half-promoted session. Clients still must explicitly switch from the
preview socket/run to the recovered socket/run; there is no in-place Store
replacement and cursors cannot cross the switch.
The preview remains independently queryable while the process runs.
It does not carry an automatic redirect or the recovered `run_id`; deployment
readiness or a client-side probe must discover the recovered GET_SESSION.
After promotion, the journal reader propagates journal failure directly and
its at-most-100-ms tail poll also checks the single preview/SDK owner. A
non-shutdown owner failure marks both mappings FAILED and stops CERTIFIED
control without waiting for the independently configurable generation
interval. Clean shutdown is an explicit one-way tail-drain transition: the
application authorizes expected preview quiescence, publishes the preview's
final generation, closes and durably flushes the journal, then joins the tail
consumer at journal End. The transition carries an absolute deadline derived
from `--intraday-recovery-backpressure-seconds`; permanent preview/shadow/
CERTIFIED pressure therefore fails closed with `BACKPRESSURE_TIMEOUT` instead
of making process shutdown wait forever. It verifies
`tail_consumed_serial == committed_serial == accepted_serial` before publishing
the recovered shadow's final generation. The relaxation covers only the
expected non-accepting/stopped preview state; fatal, invalid-progress, or
trade-date-boundary samples still fail closed.

Online WAL tuning is bounded by
`--intraday-recovery-journal-max-gib` (default 512),
`--intraday-recovery-journal-segment-mib` (default 256), and
`--intraday-recovery-journal-queue-records` (default 65,536). The byte cap is
logical serialized WAL bytes, not filesystem preallocation or a guarantee of
free physical space. The directory must be empty (or absent with an existing
parent so it can be created) and distinct from the CSV directory. The removed
startup-buffer byte/message options are no longer accepted.

The online governor samples preview, shadow, journal, both FAST control planes,
and CERTIFIED data-worker plus socket-accept health before entering any
pressure wait, so sustained backlog cannot hide a failed downstream owner.
CERTIFIED worker-only warmup remains healthy before intentional control
activation; once activated, an unexpected accept-loop exit is terminal. These
additional probes exist
only inside the online-recovery handoff; ordinary from-open and standalone
partial-no-recovery reads do not execute them. Preview backlog reaching 64
records drains through 0;
shadow backlog reaching 1,024 drains through 256. Those hysteresis states
survive bounded journal-tail polls. Bulk CSV/journal publications are grouped
into 64-record quanta: the maximum sub-pause CERTIFIED utilization observed in
a quantum applies one 50-us cooldown at 50-74% or one 500-us cooldown at
75-89%. Any sample at 90% or above instead enters the 50-us pressure-poll loop
until the terminal/pressure gate changes. A low-pressure quantum that observed
new preview admissions cooperatively yields once. Bounded CSV parser
checkpoints sample all terminal and high/low pressure gates without counting
as publications or triggering a quantum cooldown. Permanent post-promotion
journal tailing skips the 50/75% bulk cooldowns, but retains preview/shadow
gates and the 90% CERTIFIED pause. Worker stop, resource freeze/exhaustion,
dropped handoffs, or conflicting duplicates is terminal rather than
recoverable pressure. This version does not expose replay-worker-count,
CPU-percent, or CPU-quota controls. `--event-cpu-set LIST` is the one affinity
control: when explicitly set, Event worker/control threads use that exact
logical-CPU set and all FAST/router threads inherit the exact nonempty startup
affinity complement. When omitted, no affinity syscall is added and existing
placement is preserved. CPU numbering alone cannot prove physical-core, SMT,
NUMA, IRQ, or storage isolation; operators must choose the set accordingly.

The recovered FAST control socket is not made ACTIVE
until replay, the closed live handoff through its selected frontier, the first
immutable Store/Factor generation, and any configured KLine publication all
succeed. A failed or partial recovery is therefore never exposed as a
queryable complete session. Only the explicitly partial preview is available
during online rebuild.

The default-on CERTIFIED sidecar starts its projection worker and an
independent Tick-history writer on the configured Event CPU set during
recovery. The projection worker is part of online promotion: terminal handoff
health or barrier/control failure aborts promotion, and recovered FAST is
exposed only after the barrier. It builds the append-only canonical full-day
Event journal while replaying; `kGetEventHistory` becomes queryable only after
the barrier and supports one History-to-live-tail cursor. The Tick-history
writer batch-copies only already-public bounded CERTIFIED slots into its
append-only journal. Its allocation, `fallocate`, and 512-byte journal stores
do not run before bounded CERTIFIED publication; retention/capacity/I/O loss
fails only Tick History and preserves FAST plus bounded CERTIFIED. An explicit
recovery fence waits for the matching Tick-history frontier on that cold
control path. Disabling native-gap recovery explicitly removes these
canonical services and leaves `certified_prefix_valid=false`.

The default CERTIFIED Event journal is the only production order-event path;
the router has no arrival-order Event fallback. See the
[CSV startup recovery contract](docs/csv-startup-recovery-v1.md) before using
the recovery option.

The catalog is canonical ASCII with LF endings and lowercase exact-byte hex:

```text
L2FLOW_DAILY_INSTRUMENT_CATALOG_V2	20260730	20260730	sh+sz	complete
sh	-	363030303031	share	equity	documented_core	-
sz	31303220	303030303031	share	equity	documented_core	-
```

The Shenzhen source above decodes to the four bytes `102 `; the loader never
trims or normalizes it.

For from-open, promoted online-recovery, and process-start partial sessions,
the native-gap CERTIFIED service is the canonical order-event path. It
reorders/repairs native positions before projection and exposes the entire
append-only history plus future live events on the CERTIFIED socket:

```python
with client.open_certified_order_events(
    "/absolute/private/l2flow.sock.certified"
) as events:
    while True:
        batch = events.read_batch()
        consume_zero_copy(batch.buffer)
```

Online clients may attach only after recovered FAST advertises
`certified_prefix_valid=true`; the Event coverage flags are immutable once the
CERTIFIED control socket is exposed. A partial client instead passes
`coverage_requirement="process_start_partial"`; the default remains strict
from-open.

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

KLine consumers can inspect the session boundary and each bar's coverage
without guessing from its first trade timestamp:

```python
coverage = client.kline_coverage()
bar = client.latest_kline(instrument_id=1, window_id=60_000)

print(coverage.coverage_kind, coverage.coverage_start_unix_ns)
print(bar.temporal_coverage, bar.natural_window_left_truncated)
```

`PROCESS_START_PARTIAL` is never equivalent to full-day validity. A partial
bar carries `KLineCoverageFlag.PROCESS_START_PARTIAL`; the natural window that
strictly contains the process coverage boundary additionally carries
`NATURAL_WINDOW_LEFT_TRUNCATED`.

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

The direct Polars layer adds fixed-schema `DataFrame`/pinned `LazyFrame`
snapshots, immutable chunk manifests, silent background History-to-tail
refresh, strict temporal coverage, and fail-closed reconciliation for FAST
Tick/Event and CERTIFIED Tick/Event products. Returned frames are immutable
snapshots; asking the live handle for its next snapshot observes newly
committed chunks. Product identities are never merged across FAST and
CERTIFIED. See
[`docs/polars-live-history-v1.md`](docs/polars-live-history-v1.md) for the
supported API matrix, partial/from-open rules, stable Event UID, promotion,
spill, and resource limits.

The standalone `mdl-order-event-aggregator` remains an ordered-input replay and
diagnostic tool; the production router does not start, gate, or advertise it.
It must not be attached to live callback-order Wire data because that source
cannot guarantee the shared Shenzhen 6.33/6.36 native order. Given an input
whose native order is already guaranteed, it consumes every record in the
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
