# L2Flow

L2Flow is a C++20 realtime market-data runtime. The production chain is:

```text
strict premarket daily A-share catalog
  -> exact-key sort/deduplicate
  -> dense session-local IDs
  -> Store/runtime/IPC preallocation
  -> Wire V2.3 ACTIVE

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
`SecurityID`/`SecurityIDSource` bytes. Wire V2.3 exposes:

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

Wire minor 3 adds `LIVE_PARTIAL` plus explicit from-open, startup-recovered,
full-day KLine/Factor, and CERTIFIED-prefix flags. The C header exports stable
numeric enums and the Python model exposes the same state/flags, but readers
still validate the exact supported wire minor. Deploy the producer, native
reader library, and Python package together; a V2.2 reader must reject a V2.3
mapping instead of silently interpreting the new state.

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

The ordinary from-open path does not persist captured callbacks. Same-day CSV
startup recovery has two explicit modes. `blocking` retains the original
bounded in-memory handoff and withholds production queries until replay is
complete. `online` opts the single SDK owner into an asynchronous disk-backed
live journal, publishes a separate `LIVE_PARTIAL` latest-value preview, and
rebuilds a second shadow Store/KLine/Factor/CERTIFIED pipeline before exposing
the recovered socket. Both modes validate a closed CSV/live handoff with exact
identity matches and immutable tuple cutoffs under the explicit same-session,
lossless-callback operating contract.

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

Control socket paths must not already exist. Production requires exactly one
startup mode:

- `--intraday-store-from-open` asserts that this process started before the
  first relevant market message and stayed healthy; or
- `--intraday-recovery-csv-dir /absolute/path/to/same-day-csv` asserts that
  the vendor directory contains the current trade day's complete saved prefix
  from market open; or
- `--intraday-live-partial` explicitly starts a process-start-only service
  without CSV recovery and without claiming coverage from market open.

The standalone partial mode uses `--ipc-socket` as its only service socket. It
connects the real SDK immediately and serves GET_SESSION plus latest
snapshot/tick reads with `server_state=LIVE_PARTIAL`; History and delta opens
are rejected. `coverage_from_open`, `startup_prefix_recovered`, both full-day
validity flags, and `certified_prefix_valid` remain false for the whole run.
It creates no startup buffer, live journal, CSV source, shadow pipeline, or
CERTIFIED sidecar. KLine windows, a CERTIFIED socket, and the event aggregator
are therefore rejected rather than silently exposed with provisional
semantics. This is the factually correct mode for a mid-session launch that
deliberately does not recover the market-open prefix; using
`--intraday-store-from-open` in that situation remains an invalid operator
assertion.

CSV recovery selects `--intraday-recovery-mode blocking|online`; `blocking` is
the default. Blocking mode connects the live feed into a bounded startup
buffer while it reconstructs the production Store through the normal decode
path. Its buffer can be sized with
`--intraday-recovery-live-buffer-messages` and
`--intraday-recovery-live-buffer-mib`; warmup and per-admission waits use
`--intraday-recovery-warmup-seconds` and
`--intraday-recovery-backpressure-seconds`. Defaults are 262,144 messages,
512 MiB, 1,800 seconds, and 30 seconds. Exceeding either live-buffer bound
fails startup instead of dropping data.

Online mode requires all of:

```text
--intraday-recovery-mode online
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
has no control worker until shadow replay has reached a fixed journal frontier,
published its first Store/KLine/Factor generation, and completed the
CERTIFIED prefix barrier when that default-on sidecar is enabled.

The final promotion critical section rechecks the journal's health and durable
coverage of that fixed frontier, plus the single SDK/preview owner's accepting,
trade-date, pipeline, and IPC health, both before control exposure and after
FAST/flag publication. A writer or preview failure during the cold generation/
barrier window therefore cannot be logged as a successful promotion.

Promotion starts the recovered control planes and records its realtime
nanosecond only after all required steps and validity-flag publication have
succeeded. The two Unix sockets cannot begin returning queryable sessions by
one CPU instruction: CERTIFIED control is started first and recovered FAST
last. Clients must explicitly switch from the preview socket/run to the
recovered socket/run; there is no in-place Store replacement and cursors
cannot cross the switch.
The preview remains independently queryable while the process runs.
It does not carry an automatic redirect or the recovered `run_id`; deployment
readiness or a client-side probe must discover the recovered GET_SESSION.
After promotion, the journal reader propagates WAL failure directly and its
at-most-100-ms tail poll also checks the single preview/SDK owner. A non-shutdown
owner failure marks both mappings FAILED and stops CERTIFIED control without
waiting for the independently configurable generation interval.

Online WAL tuning is bounded by
`--intraday-recovery-journal-max-gib` (default 512),
`--intraday-recovery-journal-segment-mib` (default 256), and
`--intraday-recovery-journal-queue-records` (default 65,536). The byte cap is
logical serialized WAL bytes, not filesystem preallocation or a guarantee of
free physical space. The directory must be empty (or absent with an existing
parent so it can be created) and distinct from the CSV directory. Explicit
legacy live-buffer byte/message tuning is rejected in online mode.

Online CSV replay samples the CERTIFIED handoff queue before each CSV
publication: below 50% it runs normally, from 50% it sleeps briefly, at the
configurable high watermark (default 75%) it slows further, and at 90% it
pauses. Journal suffix catch-up skips the low/high sleeps but also pauses at
90%. Worker stop, resource freeze/exhaustion, dropped handoffs, or conflicting
duplicates is terminal rather than recoverable pressure. This version does
not expose replay-worker-count, CPU-percent, CPU-quota, or affinity options;
the pressure gate is cooperative per-record throttling.

In blocking mode, the byte bound counts
copied MDL head+body bytes, not allocator/deque/fingerprint overhead, and the
message bound covers every supported subscribed callback before the A-share
filter. Size both bounds from measured peak callback message/byte rates times
the worst measured replay-and-drain duration, with operational headroom; the
defaults are bounded fallbacks, not a throughput guarantee. Replay-tail
fingerprints are retained separately per message tuple by the greatest
SequenceIDs, not by potentially reordered replay publication order. The
Shenzhen cross-file merge also has independent fixed defaults of 2,000,000 pending
messages and 512 MiB of pending body bytes, so process RSS can exceed the live
wire-byte bound.

In both recovery modes the recovered FAST control socket is not made ACTIVE
until replay, the closed live handoff through its selected frontier, the first
immutable Store/Factor generation, and any configured KLine publication all
succeed. A failed or partial recovery is therefore never exposed as a
queryable complete session. Only the explicitly partial preview is available
during online rebuild.

The optional CERTIFIED sidecar starts only its projection worker during
recovery. In blocking mode, FAST can become active first; a later FIFO prefix
barrier must commit `NO_DATA` or `CONTIGUOUS` before CERTIFIED control starts,
and sidecar failure degrades only CERTIFIED. In online mode, the default-on
CERTIFIED worker is part of promotion: terminal handoff health or barrier/
control failure aborts promotion, and recovered FAST is exposed only after the
barrier. Disabling native-gap recovery explicitly removes that sidecar and
leaves `certified_prefix_valid=false`.

CSV recovery cannot be combined with `--event-aggregator-socket` in this
version. The external aggregator has no pre-ACTIVE full-replay handoff and its
bounded ring cannot be assumed to retain an entire intraday replay. See the
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
