# Intraday instrument store V1

## Scope

The intraday instrument store is the only production market-history store. It
retains every record accepted by the running process for one fixed trade date
and supplies the immutable generation consumed by factor calculation. There
is no alternate in-memory retention path and no publication fallback when the
store cannot append or publish a complete cut.

The store owns each decoded `RealtimeHistoryRecordV1` exactly once. It does
not decode a second time and does not introduce another sequence authority or
publication clock.

## Coverage boundary

The store is memory-only and intentionally has no recovery reader. The feeder
client may append raw messages to its own CSV file, but that file is not read,
indexed, replayed, or validated by this project. The optional audit WAL is
also not a store recovery source.

`coverage_from_open` is an operational assertion, not something inferred from
process sequence number 1. It is true only if all of the following remain
true:

- the process was ready before the first market message it was expected to
  accept for the trade date;
- the same process has run continuously since then;
- no append, allocation, capacity, generation, or publication failure has
  occurred;
- the upstream feed itself met the separately managed completeness contract.

The watermark proves completeness only for the dense prefix accepted by this
process. It does not prove absence of loss before the callback or completeness
of the vendor server.

A start after open, process crash, executable restart, or host restart creates
a new partial session beginning at that start. Without recovery, the new
process cannot regain the earlier records and must report
`coverage_from_open=false`. A strict from-open service must remain unready for
the rest of that trade date or continue serving from a still-running process
that began before open.

## Ownership and append path

The production path is:

```text
decoded record
-> instrument_id % store_worker_count
-> permanent instrument-store worker
-> one append-only source lane for that instrument
```

Each fixed-registry instrument has four source lanes. A lane is a doubly
linked list of fixed-capacity chunks, and each slot retains one
`shared_ptr<const RealtimeHistoryRecordV1>`. Only the permanent owner worker
appends to an instrument, so a lane has one writer and does not require an
append mutex.

Source sequence must increase strictly within a lane. Different source
workers can arrive in a different order from callback admission; readers merge
the four captured lanes by the globally dense ingress sequence.

Chunk capacity is at most 65,536 records, with a default of 1,024. This keeps
one append from requesting a pathological allocation even when the session
budget is large.

No record is evicted during the trade-date session. Reaching either capacity
limit is a coverage failure and closes production admission; it is never a
request to discard an old prefix.

## Generation semantics

The store uses the same four source fences that prove the process ingress
prefix. Once a worker has consumed every record below a cut, it captures for
each owned instrument and source:

- the last visible chunk;
- the visible slot count in that chunk;
- the visible source-record count;
- the latest snapshot and tick locators.

The slice contains endpoints and locators, not a copy of all retained record
handles. Building a generation is therefore O(I), where I is the fixed
instrument count, and does not grow with the number of session records.

Publication verifies that every worker slice belongs to the requested
generation and that captured global and per-source totals exactly match the
watermark. The published store generation is the sole input to factor
calculation.

For latest-only factor work, `SummaryAt(index)` exposes the fixed registry
universe in canonical order with record counts and latest snapshot/tick
pointers. Iterating all summaries is O(I); the default last-price projection
does not scan the accumulated session.

The factor generation retains the exact store-generation shared pointer and
copies its watermark:

```cpp
auto factor = pipeline->AcquireLatestFactorGeneration();
if (factor != nullptr) {
    const auto& store = factor->input_store();
    // factor and store describe the exact same ingress prefix.
}
```

The independent latest-store accessor is diagnostic. Reading latest store and
latest factor in separate calls is not a transactional pair because store
generation N is published before factor generation N. Acquire the factor once
and use `input_store()` when an exact pair is needed.

## Query complexity

All cursors retain their generation and return borrowed record pointers valid
for the cursor lifetime.

- `SummaryAt` and `Find` expose latest-only per-instrument state.
- `OpenInstrumentCursor` merges four source lanes over a half-open ingress
  range.
- `OpenTailCursor` streams the newest N records without materializing the
  complete instrument history.
- `OpenUniverseCursor` emits ascending instrument ID and then ascending
  ingress sequence within each instrument.
- `OpenUniverseRangeCursor(begin, end, ...)` applies the same ordering to one
  half-open `SummaryAt` ordinal range. Valid empty ranges are immediately
  terminal; reversed or out-of-universe ranges are rejected.

`ReadBatch` writes into caller-owned pointer storage and rejects a batch above
`maximum_records_per_batch`, whose absolute limit is 1,048,576.

For N visible records, a full universe drain performs O(N) record work and
uses O(batch) caller storage. It does not allocate a second N-record result.
Independent non-overlapping ordinal-range cursors can be drained concurrently;
when no per-cursor `maximum_records` limit truncates a range, concatenating
their outputs in ordinal order is exactly the full-universe ordering. Each
cursor owns only its own traversal state and shares the immutable generation.
The final acceptance probe must continue through an explicit terminal page
with `written == 0` and `done() == true`, and must reconcile global and all
four source counts with the generation watermark.

Oldest-first narrow ranges currently walk each selected source lane from its
captured head to the lower bound. A late-session narrow range can therefore
cost O(the preceding lane prefix). Sparse chunk-boundary indexes are a future
optimization if that query shape becomes latency-sensitive.

## Capacity and 1 TiB deployment boundary

Production requires both:

- `maximum_session_records`;
- `maximum_session_accounted_bytes`.

The byte limit is one conservative logical budget shared by base index state,
whole chunk allocations, and retained-record accounting. The observable
logical total is `accounted_record_bytes + allocated_index_bytes`. Chunk
capacity is reserved before allocation so a new chunk cannot bypass the
limit.

Logical accounting is not allocator RSS. It cannot exactly include allocator
metadata and fragmentation, thread stacks, SDK and feeder memory, consumer
objects, kernel state, or page cache. This version also retains the exact
decoded object graph rather than a compact POD payload.

For a 1 TiB host, the initial operational envelope is:

- 600--620 GiB store logical hard limit;
- an independently monitored process high-water alert near 800 GiB;
- a process termination boundary around 850--860 GiB;
- approximately 160--220 GiB reserved for the feeder, allocator variance,
  operating system, and page cache.

These are rollout starting points, not universal constants. The record cap
must be derived from live rate measurements with an explicit burst and
two-times-volume margin. Neither logical limit should equal physical memory.

### Generation and cursor pinning

A generation owns session state needed by its summaries and chunks. A cursor
owns its generation. Retaining an old factor retains its exact `input_store`,
and retaining a cursor can therefore keep the session and all referenced
record owners alive even after the runtime stops.

Consumers must enforce a small fixed upper bound on retained generations,
factors, and concurrent cursors. Shutdown or trade-date rollover does not
guarantee an immediate RSS drop while any such handle remains live. Monitoring
must include old-generation count, cursor count, process RSS, and time since
the runtime released the session.

## Failure policy

Store construction, append, chunk allocation, record/byte reservation,
worker capture, and generation construction are part of the production
correctness path. Any failure makes coverage sticky-lost, closes admission,
and prevents a later store or factor generation from being advertised as
complete.

The optional WAL remains an independent audit side sink. A WAL failure does
not alter a healthy store prefix, while a store failure cannot be hidden by
WAL or feeder output.

## Production and acceptance

```bash
./build/mdl-production-router \
  ... \
  --instrument-store-workers 4 \
  --intraday-store-max-records 1000000000 \
  --intraday-store-memory-gib 600 \
  --intraday-store-chunk-records 1024 \
  --intraday-store-batch-records 65536 \
  --intraday-store-from-open
```

The process must be deployed before open and kept alive when from-open
coverage is part of the service contract. A same-day restart cannot restore
that claim.

`accept-realtime-pipeline` uses the same capacities, rejects lost or partial
coverage, drains the final universe cursor in caller-owned batches, validates
record ordering and the complete four-source watermark, and reports
`full_scan_records`, `full_scan_ns`, and scan records per second. Promotion
requires an actual full-session run; an earlier short run that measured a
different retention contract is not sufficient evidence.
