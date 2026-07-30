# Daily Instrument Catalog Runtime V2.2

## Session contract

Production freezes one immutable Shanghai+Shenzhen A-share catalog before the
SDK connects. The catalog carries the trade date, positive source version,
complete-coverage declaration, exact opaque keys, dense identities, and a
SHA-256 digest:

```text
catalog_scope             = DECLARED_DAILY_A_SHARE
catalog_coverage_complete = true
catalog_generation        = 1
bound_count               = capacity
instrument_id             = ordinal + 1
```

Exact key order is `(market, SecurityIDSource bytes, SecurityID bytes)`.
Neither the loader nor callback trims, case-folds, or tries alternate source
values. The catalog digest includes the trade date, catalog version, scope,
coverage declaration, ordered identities, metadata, and external IDs.

`coverage_complete` is limited to the premarket source's declared subscribed
Shanghai+Shenzhen A-share scope. It says nothing about other products,
complete-from-open history, or data availability for each identity.

## Startup order

```text
load and validate strict premarket file
  -> classify A shares, sort, deduplicate, assign dense IDs, hash, freeze
  -> allocate dense runtime availability state and Store
  -> create Wire V2.2 mapping
  -> prepublish every BOUND_NO_DATA identity and exact key
  -> IPC ACTIVE
  -> order-event aggregator READY (when configured)
  -> start four decoder owners
  -> SDK Connect with one callback thread
```

The strict loader accepts an absolute regular non-symlink path, opens it with
`O_NOFOLLOW`, bounds it to 64 MiB, checks inode/size/timestamps before and
after the exact read, requires canonical ASCII/LF text and lowercase even
hex, and requires an exact date/version/`sh+sz`/`complete` header.

All catalog IDs are valid point identities from startup. A catalog member with
no data reports `BOUND_NO_DATA`; an ID above capacity reports invalid ID. Key
lookup miss reports unknown instrument. There is no live UNBOUND tail.

## Callback admission and source lanes

The supported SDK callback remains single-threaded. Its successful path is:

```text
inspect tuple/body
  -> extract exact key
  -> mandatory A-share classification
  -> immutable catalog binary lookup
  -> acquire and fill one pooled message
  -> construct source DecoderCommand
  -> commit global/source/tick accepted frontiers
  -> release-publish source queue tail
```

A non-A-share callback is a normal filtered result before pool acquisition or
sequence allocation. A valid A-share missing from the frozen catalog is fatal
before sequence commit. Pool exhaustion or a full/closed source queue is also
fatal without committing the candidate sequence.

There are exactly four serial lanes:

1. Shanghai snapshot;
2. Shanghai NGTSTick, which owns phase state;
3. Shenzhen snapshot;
4. combined Shenzhen 6.33 order and 6.36 transaction/cancel.

The ring for a lane with logical message capacity `Q` has `Q + 2` physical
cells: `Q` message cells, one reserved control/fence cell, and one empty
sentinel. Message publication constructs the complete slot and commits every
accepted counter before the queue tail is release-published. The consumer
acquires the tail, so it cannot complete a record before its accepted frontier
is visible.

There is no global ProcessingQueue, ProcessingLoop, runtime `BindOrGet`, or
dynamic identity IPC sink.

## Applied window

After a decoder pops a message and before full decode/History submission, it
waits for:

```text
D = min(sum(source message capacities) + source_count,
        completion_tracker_capacity - 1,
        tick_ring_capacity - 1)

0 < global_sequence - applied_sequence <= D
```

The tracker and tick ring capacities must both be strictly larger than `D`.
Accepted-minus-applied may be larger because source-queue messages that have
not crossed the gate are not part of the completion window.

Different lanes may complete out of order. `ContiguousSequenceTrackerV2`
retains those completions internally and publishes only the greatest complete
global prefix. The global tick ring therefore never advertises a record beyond
an unapplied gap.

## Parked generation fence

A generation cut is serialized by the cut mutex. Under the short admission
mutex it captures global/source/tick cuts and inserts the same epoch fence in
all four source FIFOs. Later callbacks can only enqueue behind those fences.

Each decoder drains its pre-cut FIFO prefix, reports arrival, and parks without
processing post-cut messages. Once all lanes have arrived, the coordinator:

1. waits until the global cut is contiguously applied;
2. acquires the immutable catalog plus exact runtime availability snapshot;
3. begins History generation;
4. requests `SealSource` from each parked source owner;
5. waits for every source-owner seal;
6. releases all four lanes together and waits for departure, which also makes
   each reserved control slot reusable;
7. waits for the immutable Store/KLine generation;
8. publishes the Store generation and then calculates/publishes Factor.

This proves that every pre-cut record is applied and no post-cut record has
entered History or availability state at snapshot time. Empty lanes still
arrive and seal. Arrival timeout, begin failure, seal failure, publication
failure, stop, or fatal wakes every parked lane and fails closed.

## Runtime state, readers, and history

The dense mutable state records snapshot/tick availability, Factor
eligibility, first/last applied sequences, and snapshot epochs. It never
changes identity or catalog generation.

Reader selections are:

- `CATALOG_ALL`: every dense daily identity, including no-data rows;
- `AVAILABLE_ANY`: at least one applied snapshot or tick;
- `SNAPSHOT_AVAILABLE`;
- `TICK_AVAILABLE`;
- `FACTOR_ELIGIBLE`.

The historical `BOUND` and `OBSERVED_ANY` names remain source aliases only;
Wire V2.2 readers validate the daily scope and reject legacy/partial catalogs.

C++, C ABI, and Python full/rolling raw and derived event readers retain their
existing finite-EOF and verified-checkpoint contracts. Catalog members with
no history return a successful empty result. Unknown keys/IDs are explicit
errors. Run ID, session epoch, trade date, capacity, frozen catalog generation,
catalog digest, and generation endpoint are verified, so a cursor cannot cross
a session, catalog version, or trade date.

The order-event aggregator continues to consume the dense contiguous global
tick ring. Shanghai T-to-A/T-only/phase propagation and the Shenzhen projector
algorithms are unchanged.

## Metrics and excluded scope

When stage measurement is enabled the pipeline exposes callback duration,
callback-to-source-queue publication, source queue dwell, full decode,
decode-to-History-submit, decode-to-applied, callback-to-Store-append,
callback-to-required-IPC visibility, per-source depth/high-water/full counts,
and accepted/applied distance. Queue publication timestamps are sampled
immediately before the irreversible accepted commit; all operations after
commit are non-throwing atomic publication/notification.

This version intentionally does not add Parquet/Arrow output, WAL, ring
catch-up, replay, or producer crash recovery.
