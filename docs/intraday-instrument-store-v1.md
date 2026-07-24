# Intraday instrument store V1

## Scope

The intraday instrument store retains the complete prefix accepted by the
running process for the current trade date. It is an in-memory query view over
the same immutable `RealtimeHistoryRecordV1` owners consumed by bounded
history. It does not decode a second time and does not introduce another
queue, worker pool, sequence authority, or publication clock.

The store intentionally has no disk recovery path. The feeder owns its CSV
append stream independently. Starting or restarting L2Flow after market open
creates a partial in-memory session; it never reads feeder CSV files and cannot
prove open coverage. In that situation the operator must not set
`coverage_from_open`.

## Ownership and append path

The existing history route remains authoritative:

```text
decoded record
-> instrument_id % history_worker_count
-> permanent history worker
     |-> bounded V1 factor compatibility row
     `-> intraday store source lane
```

Each fixed-registry instrument has four append-only source lanes. A lane is a
doubly linked list of fixed-capacity chunks. One slot retains one
`shared_ptr<const RealtimeHistoryRecordV1>`; the event object graph is shared
with bounded history rather than copied.

Chunk capacity is limited to 65,536 records so one append cannot trigger a
pathological multi-GiB allocation even when the session budget is large. The
default is 1,024.

Only the permanent owner worker appends to an instrument. Consequently a lane
has one writer and requires no append mutex. Its source sequence must increase
strictly. Readers never inspect a live tail: they operate on immutable
endpoints captured by a generation fence.

No record is evicted before the process releases the trade-day session.
Reaching a configured record or accounted-byte limit is a coverage failure,
not a request to discard old data.

## Generation semantics

The store reuses all four existing source fences. After a worker has consumed
every record below a cut, it captures, for each owned instrument and source:

- the last visible chunk;
- the number of visible slots in that chunk;
- the visible source count;
- latest snapshot and tick locators.

The slice contains endpoints, not a copy of every retained handle. Building a
generation therefore scales with the fixed instrument universe, not with the
number of records accumulated since open.

Publication validates that all worker slices belong to the requested
generation and that their captured global and per-source counts exactly match
the `RealtimeHistoryWatermarkV1`. A published history generation owns the
exact matching intraday generation. A factor generation owns that history, so
the supported consistent read is:

```cpp
auto factor = pipeline->AcquireLatestFactorGeneration();
if (factor != nullptr) {
    const auto& history = factor->input_history();
    const auto& intraday = factor->input_intraday_store();
    // factor, history, and intraday describe the same ingress prefix.
}
```

Independent `AcquireLatest*` calls are observability conveniences and are not
a transactional multi-slot read.

## Modes

| Mode | Store append/cut failure | Bounded history/factor |
|---|---|---|
| `disabled` | store is not constructed | unchanged |
| `shadow` | sticky `coverage_lost`; no later complete store generation | continues |
| `required` | pipeline fails closed | retained for factor compatibility |
| `primary` | pipeline fails closed | retained as the V1 compatibility view |

`primary` does not yet remove bounded history. It is an operational rollout
label for consumers that treat intraday history as their primary read view.

`coverage_from_open` is an operator assertion. It should be enabled only when
the process started before the first accepted market message and has remained
healthy continuously. Any failed store append clears that claim permanently.

## Memory limits

Enabled modes require both:

- `maximum_session_records`;
- `maximum_session_accounted_bytes`.

The byte limit is one conservative logical budget shared by base index state,
whole chunk allocations, and retained record accounting. The observable
logical total is `accounted_record_bytes + allocated_index_bytes`; chunk
capacity is reserved before allocation, so an oversized chunk cannot bypass
the limit. The two categories intentionally favor a safe upper estimate and
may overlap for a lane-owner slot.

This is still not allocator RSS. It cannot exactly include allocator metadata
and fragmentation, thread stacks, SDK/feeder memory, kernel state, or page
cache. The budget governs the retained session store; bounded-history
compatibility rows and consumer-retained generation/cursor metadata are
outside it. Consumers must not retain an unbounded number of old generations.

On a 1 TiB production host, a reasonable initial envelope is a 600--620 GiB
store hard limit and an independently monitored 800 GiB process high-water
alert, leaving roughly 160--220 GiB for the feeder, allocator variance,
operating system, and page cache. The record cap should be derived from a live
10-minute sample and include a burst multiplier; neither cap should be set to
the full physical-memory size.

This V1 retains the existing exact decoded object graph. It removes the
day-end generation-copy problem, but it is not the final density
optimization. Before relying on a two-times-volume session, measure real RSS,
allocation rate, and cut latency. A later compact POD payload can preserve the
same generation/query contract while reducing object and allocator overhead.

## Queries

All cursors retain their generation and return borrowed record pointers valid
for the cursor lifetime.

- `Find` returns counts and latest snapshot/tick for one instrument.
- `OpenInstrumentCursor` performs a four-way merge by global ingress sequence
  over a half-open sequence range.
- `OpenTailCursor` streams the newest `N` records without materializing or
  reversing the complete history.
- `OpenUniverseCursor` emits ascending instrument ID, then ascending ingress
  sequence within each instrument.

`ReadBatch` writes into caller-owned pointer storage and is bounded by
`maximum_records_per_batch` (at most 1,048,576). Querying the full day is
therefore streaming: the result set itself does not require a second full-day
allocation.

A complete scan is O(records) and can be sharded by instrument across reader
threads. Generation publication remains O(fixed instrument universe).
Oldest-first narrow ranges currently walk each selected source lane from its
captured head until the lower bound; a late-session narrow query is therefore
O(prefix before range), even though a full scan is already optimal. If that
query shape becomes latency-sensitive, the next compatible optimization is
chunk-boundary skipping followed by a sparse per-lane index.

The healthy append path also updates global session record and byte
reservations. At the current measured feed rate this is expected to be small,
but a two-times-volume rollout must verify append throughput and cache-line
contention on the production NUMA topology rather than assuming linear scale.

## Production example

```bash
./build/mdl-production-router \
  ... \
  --intraday-store-mode required \
  --intraday-store-max-records 1000000000 \
  --intraday-store-memory-gib 600 \
  --intraday-store-chunk-records 1024 \
  --intraday-store-batch-records 65536 \
  --intraday-store-from-open
```

The numeric values are deployment starting points, not universal sizing
defaults. First deploy `shadow`, compare retained counts with feeder capture,
and observe peak RSS. Promote to `required` only after the limits and coverage
alarm have been exercised under representative load.

Run `accept-realtime-pipeline` with the same store flags before promotion. For
every enabled mode it rejects lost coverage, drains the final universe cursor
in caller-bounded batches, checks the complete record and four-source counts,
and reports `full_scan_records`, `full_scan_ns`, and scan records per second.
This makes full-read correctness and throughput part of the rollout evidence
instead of inferring them from append-side counters alone.
