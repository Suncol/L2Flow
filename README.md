# L2Flow

L2Flow is a C++20 market-data runtime whose only raw fact source is the
exchange Tick feed. The production subscription is intentionally limited to:

- Shanghai `4.101.24` Tick;
- Shenzhen `6.101.33` Order;
- Shenzhen `6.101.36` Transaction.

Shenzhen `6.101.53` Combined Tick is rejected, and no Snapshot subscription or
Snapshot-derived state exists in the production data path.

## Runtime model

The serialized SDK callback extracts the exact instrument key, resolves its
daily-catalog ordinal, and copies each accepted message directly into the
fixed Tick worker selected by `tick_routes[ordinal]`. Every Tick worker owns a
Shanghai and a Shenzhen decoder. It performs the only full decode, publishes
FAST synchronously, and then sends compact envelopes to the derived planes:

```text
SDK callback
  -> raw queues[source][Tick worker]
  -> Tick worker: full decode -> FAST append/publish
                               +-> compact -> Event worker -> ordered Event root + CDC
                               +-> trade compact -> KLine worker -> mutable KLine root + CDC
```

Raw ownership pools and queues are sharded by source and Tick worker. Derived
SPSC matrices are sharded by producer Tick worker and destination derived
worker. The planes retain distinct immutable route tables, queue capacities,
worker threads, CPU sets, stores, wakeups, and failure state. An Event or KLine
queue failure marks only that instrument for repair. FAST does not wait for
either derived plane. A known-instrument FAST loss fails coverage closed for
that instrument while unrelated instruments continue.

FAST history is append-only in instrument arrival order. Event ordering uses
only `(channel, BizIndex)` for Shanghai and `(channel, ApplSeqNum)` for
Shenzhen; numeric gaps are valid and channels are not compared with each
other. KLine ordering uses exchange event time plus a stable trade tie-break.

Late or skipped derived input is rebuilt from the captured FAST instrument
history. Readers retain the previous immutable root during repair. Event and
KLine replacement CDC uses BEGIN/CHUNK/COMMIT, so clients do not expose a
partially rebuilt view.

The detailed contract is in
[`docs/fast-source-derived-planes-v3.md`](docs/fast-source-derived-planes-v3.md).

## Build and test

GNU builds require GCC 13.x.

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_COMPILER=/path/to/g++-13
cmake --build build -j
ctest --test-dir build --output-on-failure
```

The vendor headers default to `mdl_sdk_2_13_234/include`. Override them with
`-DL2FLOW_SDK_INCLUDE_DIR=/path/to/include`.

Use separate build trees for sanitizers:

```bash
cmake -S . -B build-asan \
  -DCMAKE_CXX_COMPILER=/path/to/g++-13 \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DL2FLOW_ENABLE_ASAN_UBSAN=ON
cmake --build build-asan -j
ctest --test-dir build-asan --output-on-failure

cmake -S . -B build-tsan \
  -DCMAKE_CXX_COMPILER=/path/to/g++-13 \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DL2FLOW_ENABLE_TSAN=ON
cmake --build build-tsan -j
ctest --test-dir build-tsan --output-on-failure
```

## Production entry point

`mdl-production-router --help` lists the current options. Production requires
one nonempty CPU set per Tick, Event, and KLine worker, and all plane CPU sets
must be pairwise disjoint. There are no `--certified-*`, `--generation-*`,
Snapshot, native-gap-recovery, or global-ring options.

FAST capacity is permanently partitioned per instrument. The default is an
even partition of `--fast-max-records`; library callers can provide explicit
`instrument_record_capacities` for a volume-weighted partition.
Event and KLine CDC retention is also bounded per instrument through
`--event-changes-per-instrument` and `--kline-changes-per-instrument`.
Exhaustion fails the affected derived instrument closed without changing the
last published stable root or blocking FAST.

## V3 data API

`InstrumentDataServiceV3` exposes three independent cursor domains:

- `FastTickCursor(session_id, instrument_id, next_arrival_row)`;
- `EventChangeCursor(session_id, instrument_id, next_change_sequence)`;
- `KLineChangeCursor(session_id, instrument_id, next_change_sequence)`.

There is no global market cursor, generation, cross-instrument cut, or
cross-plane atomic read. The C++ service is the transport-neutral composition
boundary; `realtime_wire_v3.h` and `python/l2flow_realtime/wire.py` provide the
fixed 32-byte little-endian cursor/status contract. A cross-process socket or
shared-memory host is not bundled in this repository revision.

The Python package provides validated models, CDC appliers, and immutable
Polars block tables. Activate the repository environment and run its tests:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=python \
  .venv/bin/python -m unittest discover \
  -s tests/python -p 'test_l2flow_realtime_v3.py' -v
```

An arbitrary global Polars expression is not promised to update
incrementally. The block layer incrementally applies source CDC; consumers
must separately define stateful logic for their own unbounded windows or
global aggregations.

## Complexity boundary

FAST acceptance and publication do not sort. Ordered Event input normally
uses an O(1) monotonic append. A late input can change an arbitrarily long
derived suffix, so repair completion cannot have a data-independent O(1)
bound. Repair runs per instrument outside the FAST worker and atomically
publishes only the completed replacement.
