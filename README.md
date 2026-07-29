# L2Flow

L2Flow is a C++20 realtime market-data runtime. The production chain is:

```text
vendor SDK callback
  -> bounded owned-message copy
  -> mandatory nonblocking Journal-queue admission
  -> nonblocking ordered-processing-queue admission
  -> callback return

ordered in-memory processing dispatcher
  -> capture-ordered observed-instrument binding
  -> decoder and fixed ordinal worker
  -> intraday Store, latest IPC, and KLine
  -> contiguous applied watermark
  -> immutable Store/Factor generation

independent Journal writer
  -> batch write and fdatasync
  -> durable watermark
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
- `accepted_sequence`, `durable_sequence`, and `applied_sequence`;
- `processing_lag_records = accepted_sequence - applied_sequence`;
- `durability_lag_records = accepted_sequence - durable_sequence`.

The enforced relations are:

```text
factor_eligible_count <= snapshot_available_count
snapshot_available_count <= available_count <= bound_count <= capacity
tick_available_count <= available_count
durable_sequence <= accepted_sequence
applied_sequence <= accepted_sequence
```

Readers become usable while `bound_count == 0`; neither catalog completion nor
either progress lag is a startup gate. There is deliberately no ordering
requirement between `durable_sequence` and `applied_sequence`.

## Latency-sensitive path

The SDK callback performs no file I/O and no catalog scan. It copies into a
bounded pool once, gives the same immutable message two intrusive references,
and nonblockingly admits the Journal reference before the ordered-processing
reference. Journal-queue exhaustion fails the session closed instead of
silently dropping the record; successful admission never waits for `write` or
`fdatasync`.

Before the SDK connects, the pool prewarms the complete bounded in-flight
window for wire messages up to 4,096 bytes and physically touches those
pages. The window includes Journal, ordered processing, decoder, batch, and
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
callback wait on decoder, Store, IPC, or disk work.

The independent Journal writer collects at most 64 records, with a default
maximum batch-collection delay of 50 microseconds, then writes, calls
`fdatasync`, and advances `durable_sequence`. A writer failure is terminal for
the session, but durability never gates Decoder, Store, IPC, or Python
visibility.

The accepted/durable/applied status tuple is coalesced by a background
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
at the accepted sequence under a short admission lock. Callbacks resume both
queue admissions immediately; the cut waits only for that prefix to become
applied, then freezes its Catalog Snapshot and enqueues Store/Factor markers.
Its durability watermark is an independent observation and is not awaited.
Each Store/KLine owner publishes a constant-size cut token after its source
fences; it does not scan or copy its instrument partition. The first post-cut
update to a row lazily freezes that row's pre-cut endpoint, while a background
builder materializes only the Catalog Snapshot's bound prefix. Unbound capacity
rows are not emitted. The cut is not a gate for live latest reads.

The Journal is a fresh per-session asynchronous capture file, not a recovery
engine. This implementation has no startup replay gate, intraday replay,
checkpoint, clean-stop marker, or durability-before-visibility rule. After an
abnormal process loss, records that were visible beyond `durable_sequence`
are not promised to be recoverable.

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
  --journal-path /absolute/path/to/fresh-session.journal \
  --session-epoch 1 \
  --trade-date 20260729 \
  --server-address HOST:PORT \
  --user-name USER \
  --ipc-socket /absolute/path/to/l2flow.sock \
  --intraday-store-max-records 100000000 \
  --intraday-store-memory-gib 64 \
  --intraday-store-from-open
```

The Journal path and control socket must not already exist. Operational code
must assert `--intraday-store-from-open`; this replacement runtime does not
offer a partial-session or recovery startup mode.

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

The precise runtime, count, generation, and bias semantics are documented in
[`docs/observed-universe-runtime-v2.md`](docs/observed-universe-runtime-v2.md).
