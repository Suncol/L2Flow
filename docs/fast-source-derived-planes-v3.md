# FAST-source derived planes and Wire V3 contract

## Authority and ordering domains

FAST Tick history is the sole original fact domain. It stores every accepted
Tick in physical arrival order under one instrument-local sequence. Late data
is appended; no historical FAST page is moved or sorted.

The process-wide `arrival_id` is retained only as a diagnostic identity and as
a handshake proving that a derived worker cannot publish before the
corresponding FAST append. It is not a public market-order cursor and does not
order Event or KLine history.

Event has one native order per channel:

```text
Shanghai: (channel, BizIndex)
Shenzhen: (channel, ApplSeqNum)
```

Both native values must be positive and strictly increasing on the live fast
path. They need not be consecutive. Cross-channel order is only the
deterministic presentation order
`(channel, business_sequence, source_event_ordinal,
derived_event_ordinal, affected_order_id)`; it is not asserted to be an
exchange total order.

KLine accepts Shanghai trade Tick and Shenzhen Transaction trade only after
the decoder has validated price, quantity, and exchange event time. Its stable
trade order is `(event_time, channel, business_sequence, source_sequence,
arrival_id)`. The final fields are deterministic tie-breaks, not a claim that
arrival order is exchange time.

## FAST store

Each dense catalog ordinal has a permanent Tick route and a permanent record
capacity. Capacity partitions sum exactly to `maximum_session_records`.
Chunks are preallocated during store creation and owned by per-worker pools;
the final chunk may be short, so page fragmentation cannot reduce the
advertised record count.

An append placement-constructs the full decoded Tick, then release-publishes
the chunk count, instrument tail, and latest pointer in that order before it
returns.
Readers acquire a captured instrument tail and never observe an unconstructed
row. A failed append permanently clears that instrument's coverage-complete
bit. Event and KLine for the same instrument then become unrecoverable; other
instruments retain their own coverage state.

## Independent routing and failure isolation

The serialized callback resolves the catalog ordinal and routes the owned raw
message into one of exactly `2 x TickWorkerCount` SPSC queues. Each Tick
worker owns one Shanghai and one Shenzhen decoder, so the complete payload is
decoded once. Shenzhen Order and Transaction share the same source queue and
decoder state. The Tick worker then appends and release-publishes FAST
synchronously before it copies the compact Tick into Event and, for a valid
trade, KLine queues.

Each derived plane uses a `TickWorkerCount x DerivedWorkerCount` SPSC matrix.
The producer dimension must be the Tick worker, rather than the market
source, because distinct Tick workers may concurrently target the same
derived worker. An instrument's permanent Tick route keeps all of its messages
on one producer queue, while no cross-instrument total order is claimed.

Event/KLine `TryPush` is fixed-step and nonblocking. Failure sets the target
instrument's `REPAIR_REQUIRED` state and advances `repair_through`. Event
continues enqueueing later facts while a journal-backed suffix repair is in
progress; a queue gap alone selects the exceptional FAST cold fallback.
KLine retains its full-prefix rebuild behavior. FAST processing for unrelated
instruments continues.

A derived envelope cannot exist before its corresponding FAST append has
published, so Event and KLine workers do not retain a pending envelope or wait
for `PublishedThrough` on the live path. If the FAST append fails, no compact
envelope is emitted and that instrument becomes unrecoverable.

## Event normal path and repair

The Event worker drains a bounded micro-batch and inserts every compact input
into the permanent per-channel journal before projecting any of them. A
sequence greater than the channel maximum appends without sorting; native
numeric gaps are valid. Same-key/same-payload duplicates are idempotent and a
different payload at one key enters `SOURCE_CONFLICT`. A trade/order inversion
contained in one drain batch therefore has no incorrect intermediate stable
publication.

Each channel also retains a configurable bounded mutable tail (128 source
records by default). A smaller unseen key records the earliest `dirty_from`.
The same Event worker then slices a private shadow replay by both record and
CPU-time budgets while the prior immutable root remains readable. New tail
appends remain in the journal and are followed in place; a still-earlier
insertion restarts the private replay from the new dirty key.
Dirty instruments are scheduled through a preallocated, enqueue-once
round-robin ring owned by that Event worker. The monotonic hot path therefore
checks only instruments touched by the current micro-batch; it does not scan
the worker's complete route table after every drain.

Replay starts from the journal's exact predecessor, not `dirty_from - 1`, and
reads only `[dirty_from, current_tail]`. Order state is loaded lazily from an
order-version index. Each version points to the immutable order-revision Event
row that already owns the complete published snapshot and stores only hidden
transition fields. Shenzhen needs `terminal` and `finalization_emitted`;
Shanghai additionally checkpoints its pre-add active-fill accumulator and
execution-price extrema. A Shanghai END message imports the complete
pre-dirty channel range before finalization so untouched orders cannot vanish.
Both live and shadow modes execute the same exchange projector transition
implementation.

On commit, unaffected channels and complete prefix blocks are structurally
shared. At most one boundary block prefix is copied and newly built suffix
blocks are attached. Order versions and live final states are patched before
one stable-root release-store. CDC publishes `kChannelSuffix` BEGIN/CHUNK/
COMMIT with `(channel, dirty_from)` and no artificial end sentinel. A complete
FAST copy/sort/replay remains only for an Event-queue journal gap or damaged
source journal; that cold fallback still uses the adaptive natural-run/radix
sort implementation.

## KLine normal path and repair

Bars use the stable key `(instrument_id, window_id, window_start)` and carry a
positive revision. A new trade updates all configured windows in a private
working transaction. Open and close are selected by the stable trade order;
high, low, volume, and trade count are checked before mutation. Failure rolls
back every touched bar and trade identity before repair is requested.

Duplicate `TradeUid(instrument, channel, business_sequence)` values are
ignored only when their business payload is equal. A conflicting payload
enters `SOURCE_CONFLICT` rather than double-counting volume.

Repair rebuilds the current bar map from a captured FAST prefix, preserves an
unchanged revision, assigns revision 1 to a new bar, and increments a changed
bar exactly once relative to the previously stable version. It computes
UPSERT/DELETE diagnostics, atomically replaces the root, and publishes the
complete bar set as a chunked RANGE_REPLACE transaction. A CDC client retains
its old bar map until COMMIT.

KLine catch-up compares its trade-only `repair_through` watermark with the
latest arrival included in the captured prefix. A later Shenzhen Order or
other non-trade FAST row does not force an identical bar rebuild; any later
valid trade raises the watermark before KLine can return to LIVE.

## Stable roots and CDC cursors

Every instrument has its own atomic immutable Event root and KLine root. A
stable attach acquires one root and derives the next change sequence from the
sequence already included in that root. There is no periodic publication,
global fence, source cut, or coordination with another instrument.

Event point changes are INSERT/UPDATE/DELETE. Ordinary late data uses a
channel-suffix RANGE_REPLACE_BEGIN, zero or more RANGE_REPLACE_CHUNK records,
then RANGE_REPLACE_COMMIT. Only the cold fallback replaces the complete Event
instrument. KLine uses UPSERT/DELETE for live changes and the same three-phase
full replacement for rebuilds. A transaction ID is nonzero only for a range
transaction, and change sequences remain contiguous per instrument.

The Python CDC and Polars layers retain incomplete transaction chunks in
private state. Polars publishes a replacement block directory only when the
CDC applier commits; unaffected point-update blocks are structurally shared.

## Recovery state machine

```text
LIVE -> REPAIR_REQUIRED -> REBUILDING -> CATCHING_UP -> LIVE
                         \-> SOURCE_CONFLICT
                         \-> UNRECOVERABLE
```

During Event suffix replay, later Event inputs continue into the permanent
journal. The replay commits only after its cursor is at the journal tail and
the observed journal generation is unchanged. During an exceptional Event
cold rebuild, or a KLine rebuild, relevant arrivals raise `repair_through` and
the captured FAST tail is checked before returning to LIVE. No other
instrument or plane participates in either decision.

`SOURCE_CONFLICT` and `UNRECOVERABLE` are terminal for the session. A
concurrent coverage loss can move an instrument to a terminal state while a
private rebuild is running, but the rebuild uses compare/exchange transitions
and cannot replace it with a non-terminal state or return the instrument to
LIVE. A later loss of FAST completeness may still refine `SOURCE_CONFLICT` to
the stronger `UNRECOVERABLE` diagnosis.

## Deliberate bounds

The Tick-worker path contains the one complete decode and FAST append, but no
sort, mutex, blocking syscall, Event/KLine calculation, or global ring
publication. Event and KLine data structures may allocate in their own
workers; this cannot execute on a Tick worker.

FAST records and both per-instrument CDC logs have explicit configuration
bounds. A CDC log never reserves beyond
`maximum_change_records_per_instrument`; exhaustion preserves the last stable
root and moves only that derived instrument to `UNRECOVERABLE`. Operators must
size this complete-session retention bound from expected mutation volume.

An early late input may require replay proportional to its channel suffix.
No implementation can promise fixed O(1) completion while producing the exact
derived result, but cooperative slicing bounds one Event-worker repair turn.
The stable-root protocol guarantees consistent visibility, not
data-independent repair completion latency.

The V3 C++ service is currently a transport-neutral, process-local boundary.
Its fixed-width cursor/status codecs are language-neutral, but this revision
does not ship a Unix-socket or shared-memory host. That limitation must not be
described as cross-process IPC availability.
