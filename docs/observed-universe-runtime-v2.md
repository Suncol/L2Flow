# Observed-Universe Runtime V2

## Contract

This runtime does not claim to know the exchange's authoritative instrument
universe. Its catalog is the ordered set of exact SDK identities observed in
supported subscribed market messages during one process session:

```text
catalog_scope     = OBSERVED_ONLY
coverage_complete = false
```

An absent key therefore means "not observed in this session", not "the
security does not exist".

Each session preallocates `capacity` physical ordinal slots. Production
defaults to 65,536. The first observed key receives ordinal 0 and
`instrument_id = 1`; in general:

```text
ordinal       = instrument_id - 1
instrument_id = ordinal + 1
```

IDs are scoped by `session_epoch`. A numeric ID must never be carried into a
different session without resolving its exact opaque key again.

This implementation deliberately fails closed when the fixed capacity or key
arena is exhausted. It does not contain an intraday resize, rollover, or
compatibility path.

## Capture and publication order

The production path is:

```text
SDK callback
  -> bounded owned-message copy
  -> allocate capture_sequence
  -> nonblocking ordered-processing-queue admission
  -> callback return

single ordered in-memory dispatcher
  -> capture-ordered BindOrGet
  -> source decoder
  -> fixed ordinal Store worker
  -> Store/KLine/live IPC publication
  -> directory availability transition
  -> contiguous applied_sequence publication
```

The callback performs neither file I/O nor catalog scanning. The payload is
copied exactly once into a bounded pool. One intrusive handle transfers that
immutable message into the ordered processing queue; there is no second
persistence owner or persistence queue. Processing-queue exhaustion is
terminal and does not commit the candidate capture sequence.

Queue admission is allocation-free and nonblocking. After a slot has been
reserved and populated, `accepted_sequence` is release-published before the
queue tail makes that slot visible to the dispatcher. There is no fallible
operation between those two publications. This ordering prevents downstream
completion from temporarily publishing an `applied_sequence` greater than
`accepted_sequence`.

Pool creation, which occurs before the SDK connects, prewarms and physically
touches the complete bounded in-flight window at a 4,096-byte wire-message
hot-set size (or the configured maximum when it is smaller). This includes the
ordered processing queue, decoder queues, history handoff, and the bounded
applied-completion window. The eager reservation is capped at 256 MiB, so
unusually large configured windows use the normal bounded size-class fallback
after that hot set. Legal messages above the hot-set size remain valid;
prewarming does not change the admitted schema or message-size limit.

Before binding or decoding, the single cross-source processing dispatcher
enforces an explicit applied window `W`:

```text
0 < capture_sequence - applied_sequence <= W
```

The contiguous-completion tracker uses the same `W`, and the IPC tick ring is
required to have at least `W` slots. When the window is full, only the
in-memory dispatcher waits; the SDK callback can continue bounded,
nonblocking admission. Cross-source routing and first binding therefore
follow `capture_sequence` order.

Binding is serialized in accepted capture order. A slot is written once and
is never rebound:

```text
UNBOUND -> BINDING -> BOUND_NO_DATA -> AVAILABLE
```

The first successfully applied snapshot or tick moves the row to `AVAILABLE`.
Snapshot and tick availability remain separate directory flags; Factor
eligibility is an independent reversible state. KLine availability is
published by its own generation rather than folded into either market-data
flag.

## Counts and generations

Every coherent status envelope contains:

- fixed `capacity`;
- monotonic `bound_count`;
- monotonic `available_count` (at least one applied snapshot or tick);
- monotonic `snapshot_available_count`;
- monotonic `tick_available_count`;
- current `factor_eligible_count`;
- `catalog_generation` and rolling `catalog_digest`;
- `data_state_generation`;
- `accepted_sequence` and contiguous `applied_sequence`;
- `processing_lag_records`.

`accepted_sequence` is the greatest dense capture sequence committed by
the ordered processing queue. `applied_sequence` is the greatest dense prefix
for which Store, enabled derived state, live publication, and the required
applied sink have all succeeded.

The runtime coalesces changes to these two header watermarks in a background
publisher on a 1 ms cadence. This status publication is outside the fixed-slot
latest path and never gates Store/IPC data visibility.

The following invariants are enforced:

```text
factor_eligible_count <= snapshot_available_count
snapshot_available_count <= available_count
tick_available_count <= available_count
available_count <= bound_count
bound_count <= capacity

applied_sequence <= accepted_sequence

processing_lag_records = accepted_sequence - applied_sequence
```

`catalog_generation` changes only when a new exact identity is bound.
`data_state_generation` changes when availability or Factor eligibility
changes. The catalog digest describes the ordered observed prefix at that
generation; it is never a final-market hash.

## Reader semantics and latency

The Wire 2.0 mapping is `ACTIVE` even when `bound_count == 0`. Live Readers do
not wait for a catalog-complete milestone or processing lag to reach zero.

Known-ID reads acquire `bound_count`, use direct ordinal arithmetic, and copy
one self-identifying fixed shared-memory data slot. The available-data hot
path does not copy the instrument row, whose ingress bounds change on every
event; only an unpublished type slot reads that row to distinguish
`BOUND_NO_DATA` from `TYPE_UNAVAILABLE`. These reads do not enter the key-index
mutex, inspect `catalog_generation`, scan the catalog, rebuild an index, or
acquire the global catalog status seqcount. Exact key resolution is a
separate, on-demand operation; only that operation lazily rebuilds its sorted
local index when `catalog_generation` changes. Python's throttled health
check reads only fixed epoch, heartbeat, state, and flags fields.

Publishing a `CatalogSnapshot` is O(1) in the number of bound instruments.
The directory records one snapshot epoch and scalar counts; a row preserves
its prior state only on that row's first post-publication mutation. Retained
snapshot epochs drive per-row version recycling, so repeated cuts do not
accumulate one 65,536-row copy per generation.

Global selections validate their IDs against one stable catalog/data-state
cut. The envelope's accepted/applied fields come from one coherent status read
after that structural validation, so progress-only updates do not force a
full-universe retry. Supported scopes are `BOUND`, `OBSERVED_ANY`,
`SNAPSHOT_AVAILABLE`, `TICK_AVAILABLE`, and `FACTOR_ELIGIBLE`.

## Immutable Store and Factor generations

An explicit generation cut installs a processing-dispatch barrier at the
accepted sequence under a short admission lock. Callbacks then resume ordered
queue admission. Post-cut processing records wait before binding while the cut
waits only for its prefix to become applied, freezes one exact
`CatalogSnapshot`, and enqueues the Store/Factor markers. This barrier exists
to make Store/Factor generations immutable and internally consistent; it is
not a startup or live-Reader gate. After its four source fences, each
Store/KLine owner publishes a constant-size cut token and immediately resumes
post-cut work. The first post-cut update to a row lazily freezes that row's
pre-cut endpoint. A background builder performs the O(bound_count)
materialization and emits only the Catalog Snapshot's bound prefix; the unused
fixed-capacity tail is not emitted as instruments.

Factor output contains only rows eligible in that snapshot and carries the
same catalog identity, counts, and processing watermarks. Cross-sectional
results over this set are observed-universe results and may be activity
selected. They must not be presented as statistics over a complete exchange
denominator.

## Persistence and recovery scope

The runtime does not write captured market messages to disk and has no audit,
replay, checkpoint, or intraday recovery path. It targets a fresh process
started before market activity and assumes no mid-session crash recovery. A
process or machine failure can therefore lose records that were already
visible to Python. Restarting creates a new session and can consume only data
the SDK supplies after that restart.
