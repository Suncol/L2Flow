# Realtime production chain V1

## 1. Scope

This document defines the only production market-data chain built by the root
CMake project:

```text
Vendor DSO selected by operator
  -> DllCreateIOManager
  -> one IOManager
  -> one Subscriber, serialized callback mode
  -> callback admission authority
  -> bounded pooled immutable OwnedIngressMessageV1
       |-> OptionalWalSinkV1
       `-> source decoder lane
            -> transferable decoded event + stable registry ordinal
            -> fixed instrument worker
            -> mandatory segmented-arena intraday instrument store
            |-> applied latest snapshot/tick read model
            -> full-universe generation barrier
            -> RealtimeFactorCalculatorV1
            -> atomic RealtimeFactorGenerationV1 publication
```

The V1 scope intentionally has no alternate production admission, replay,
normalization, recovery, or per-instrument factor-publication route. The
latest read model is a read-only point projection over records already owned
by the mandatory Store. The optional feeder probe and vendor mock are manually
enabled diagnostics and are not linked into `mdl-production-router`.

## 2. Component and dependency boundaries

The dependency direction is one-way:

```text
l2flow_sdk
  physical SDK adapter + immutable five-key catalog

l2flow_realtime_ingress
  owned callback bytes + optional WAL side sink
             |
             v
l2flow_market
  decoder + immutable registry + fixed-worker intraday store
  + applied latest read model + generation barrier
             |
             v
l2flow_factor
  pure calculator boundary + whole-generation publication
             |
             v
l2flow_realtime
  lifecycle and production composition
```

Important negative dependencies follow from this graph:

- WAL does not include or call the decoder, store runtime, or factor engine.
- Store records do not carry a WAL offset or durability state.
- The factor engine cannot admit messages or mutate the store.
- The SDK adapter does not know the registry, WAL, router, store, or factor
  policy.
- The registry SHA-256 identifies the fixed market universe; it is not an SDK
  library identity.

## 3. SDK load and lifecycle boundary

`LoadSdkFactoryFromPath()` accepts the operator path and performs only direct
dynamic loading and symbol resolution:

```text
dlopen(path, RTLD_NOW | RTLD_LOCAL)
dlsym("DllCreateIOManager")
```

The path is required to be non-empty and representable as a C string. The
adapter does not validate the DSO by digest or byte comparison and does not
copy it to a repository-controlled location.

The resolved function is called with `MDL_VERSION` and the configured worker
and IO thread counts. Production creates one IOManager and one Subscriber. The
Subscriber is created with `multithread_callback=false`; callback admission is
also protected by `admission_mutex_`, so sequence assignment remains a single
authority even for a nonconforming test implementation.

The five subscription calls and the ingress classifier consume the same
`kProductionMessageKeysV1` array. This prevents the physical subscription set
from silently drifting away from the accepted message set. The catalog is:

```text
4.101.4   Shanghai snapshot
4.101.24  Shanghai tick
6.101.28  Shenzhen snapshot
6.101.33  Shenzhen order
6.101.36  Shenzhen transaction
```

The combined Shenzhen tick tuple `6.101.53` is a distinguished fatal input,
not an optional unsupported message.

### Required vendor shutdown semantics

The narrow `SdkManager::Shutdown()` contract is a full callback-quiescence
barrier:

1. every SDK-started `OnMessage` invocation has returned;
2. no queued invocation can begin after return;
3. no new invocation can begin after return.

The pipeline closes its callback gate and maintains an active-entry counter as
an additional check, but that counter cannot replace the SDK guarantee for a
callback preempted at function entry. A vendor implementation that returns
from Shutdown while a callback can still execute does not satisfy the
production adapter contract and cannot be released safely in-process.

The release order is fixed:

```text
IOManager Shutdown
-> wait active handler entries to reach zero
-> Subscriber ReleaseRef
-> IOManager ReleaseRef
```

The DSO mapping remains loaded for process lifetime. That is a lifecycle
policy for vendor process-global state, not a content-approval mechanism.

## 4. Callback admission and immutable ownership

Unsupported API, SYS, and non-catalog tuples are ignored before sequence
assignment. Required catalog messages proceed under `admission_mutex_`:

1. validate pipeline health and callback admission;
2. read and classify the vendor header;
3. reject the forbidden combined feed;
4. check sequence capacity;
5. read realtime and monotonic receive clocks;
6. enforce the fixed UTC+08 process trade date;
7. form candidate global and source sequences;
8. acquire one bounded size-class-pool block and copy the 23-byte vendor head
   and declared body into it;
9. admit that owner to the corresponding serial decoder queue;
10. commit both sequence counters and the accepted count;
11. offer the same owner to the optional WAL.

Sequence counters advance only after decoder-queue admission succeeds. WAL is
offered after realtime admission and cannot roll it back.

`InspectOwnedIngressMessageV1()` reads and classifies the vendor head/body once
and validates the declared head size, total size, maximum size, binary
encoding, catalog tuple, and body pointer. Pool acquisition validates sequence
metadata and copies the inspected bytes. `OwnedIngressMessageV1` retains no
`MDLMessage*` or callback-scoped body pointer. With WAL disabled, the pooled
allocation moves uniquely into its source SPSC ring. With WAL enabled, an
intrusive reference shares that same immutable allocation with the audit
writer. The pool has a fixed in-flight bound; exhaustion fails admission
closed rather than falling back to unbounded allocation.

## 5. Decoder and registry reachability

One serial decoder lane owns each source slot:

```text
0 = Shanghai snapshot
1 = Shanghai tick
2 = Shenzhen snapshot
3 = Shenzhen order + Shenzhen transaction
```

Each decoder is pinned to one trade date and one source stream ID. It consumes
only the vendor binary body encoding. Decoded strings and arrays are owned;
the transferable event clears its input body span.

The immutable registry maps the decoded byte key to both a stable nonzero
`instrument_id` and a stable instrument-ID-sorted registry ordinal. The
decoder carries both values into a session-bound route token, so the append
path does not repeat a key or ID search. Production validates the complete
registry before starting decoder threads or connecting the SDK:

- the market is Shanghai or Shenzhen;
- Shanghai source bytes are empty;
- Shenzhen source bytes are exactly `31 30 32 20` (`"102 "`);
- SecurityID is non-empty printable ASCII, matching decoder reachability.

This preflight prevents a well-formed registry from containing keys that live
messages can never match.

Decoded fixed-point values preserve the exact signed integer and scale.
Normalization to p6 uses checked integer multiplication. `valid=true` means
that the field is applicable to the decoded action, has a known product/domain
contract, and passed checked normalization; it is not merely evidence that the
wire bytes could be read. Calculators must still impose their own documented
economic constraints.

Known absolute prices are gated before normalization. A non-null snapshot,
book, add/trade, limit-order, or trade-transaction price must be strictly
positive. Zero and negative raw prices remain auditable but invalid with an
explicit domain notice. The gate precedes multiplication so a negative extreme
cannot turn a domain-invalid field into a fatal fixed-point-overflow error.
Price changes, yields, PE values, amounts, and action-inapplicable price
placeholders are not incorrectly subjected to the absolute-price rule.

Product-specific fields are not enabled from a coarse security type. Without a
versioned capability table, SH yield/warrant-exercise/IOPV, the complete vendor
`EtfBuy*`/`EtfSell*` group, and SZ PE/IOPV/open-interest values retain raw and
scale but remain invalid under a product-applicability notice. In particular,
`SecurityType::kFund` does not prove ETF applicability. The vendor fields named
`WarLowerPri` and `WarUpperPri` additionally carry explicit unknown-semantics
notices.

The SDK names `EtfBuy*` and `EtfSell*` are exposed under neutral vendor-group
names rather than being relabeled as subscription/redemption without a field
dictionary. `OptPremiumRatio` is likewise retained as raw/scale with
`valid=false` and an unknown-semantics notice rather than being asserted to be
a warrant or option business factor.

SH `MaxBidDur` and `MaxSellDur` retain their independent unsigned raw values
without guessing a time unit. The observed `UINT32_MAX` unavailable sentinel is
invalid and explicitly noticed; zero and `UINT32_MAX-1` remain valid raw values.
Negative quantities retain raw values but are invalid, never set the tick
quantity-valid bitmap, and carry a quantity-domain notice.

## 6. Fixed-worker routing and mandatory store

The router is deterministic for the lifetime of the process:

```text
worker = instrument_id % store_worker_count
```

The registry, worker count, and instrument IDs are immutable, so an instrument
never migrates between workers. Each worker owns all mutable rows assigned to
it; other workers never mutate those rows.

Each source decoder input is a true SPSC ring: the callback producer and its
one decoder consumer exchange slots without a queue mutex; a condition
variable is only a wait/wakeup aid on empty/full transitions. There is also
one SPSC queue per source and worker. For a given source,
`TrySubmit()` and `SealSource()` must be called by its one serial decoder
owner. The upstream callback authority must provide:

- a globally unique dense `ingress_sequence` across all four sources;
- a dense `source_sequence` within each source;
- exactly one source-sequence increment for each global increment.

The store runtime validates source-local density and monotonicity and
validates the aggregate generation equation. It deliberately does not
create a second global ordering authority.

Ordinary `TrySubmit()` does local source validation, resolves the precomputed
route token, and pushes directly to its target SPSC queue without taking the
global generation mutex. A worker is notified only while its idle gate is
armed and opportunistically drains a small bounded batch without crossing a
generation fence.

The production runtime always constructs the intraday store. Each instrument
has four append-only source lanes made of fixed-target-byte arena segments.
The owner worker constructs the exact event payload in-place and publishes a
compact header containing cached ordering metadata and a self-relative payload
offset; there is no per-record `shared_ptr`. Different source decoder threads
can reach a worker in an order different from callback admission, so readers
merge the captured lanes by cached global ingress sequence. No record is
evicted during the process trade-date session.

Record and logical-byte limits are mandatory. Append, allocation, capacity, or
sequence failure closes production admission and prevents publication of a
later complete store or factor generation. There is no alternate retention
path.

### Applied latest read model

Mutable Store rows already track one `latest_snapshot` and one `latest_tick`
locator, but those ordinary pointers are owner-only state and cannot be read
concurrently. `RealtimeLatestReadModelV1` therefore maintains two separate
atomic borrowed-record slots per fixed registry ordinal. Snapshot and tick
use separate 64-byte-aligned slot arrays; on the target 64-byte cache-line
deployments, a high-rate tick publisher therefore does not invalidate an
unrelated snapshot polling line.

Publication occurs on the permanent instrument owner only after:

```text
Store::Append == success
AND all enabled KLine updates == success
AND handoff release == success
```

The fully constructed Store record is immutable. A release publication of its
pointer followed by an acquire query therefore exposes the complete header
and typed payload without copying a snapshot. Cross-source worker completion
may be out of callback order, so publication compares process
`ingress_sequence` and never replaces a greater current value with an older
candidate.

The snapshot category contains exactly Shanghai snapshot and Shenzhen
snapshot. The tick category deliberately preserves the existing mixed
meaning: Shanghai tick, Shenzhen order, and Shenzhen transaction. The public
single and batch APIs share one selector-based query implementation:

```text
GetLatestSnapshot / GetLatestSnapshots
GetLatestTick     / GetLatestTicks
```

Batch output preserves request order and distinguishes a known but
not-yet-observed instrument from an unknown or zero instrument ID. It is
coherent per row, not one global ingress cut. Exact cross-instrument reads
still require an immutable generation. A fatal Store/history transition
marks latest coverage lost; subsequent latest queries fail closed instead of
advertising the old point state as a healthy live view. A normal
`StopAndDrain` retains the final successfully applied point values until the
runtime is destroyed.

This model is a latest-point cache, not a lossless event stream or contiguous
applied watermark. Every successfully applied record is release-published,
but a slower polling reader can miss intermediate pointer values. Consumers
that must process every retained record need a sequence cursor/stream instead.
Snapshot and tick calls, including two calls for the same instrument, are
independent observations rather than one joint atomic cut.

The returned record pointer is an in-process borrowed handle owned by the
Store session. It may be inspected only while the runtime remains alive and
must be converted to a versioned wire/Arrow/shared-memory value before
crossing a process boundary.

This is a direct, source- and ABI-breaking replacement of the former V1
retention contract. There is intentionally no compatibility adapter. Release
and deployment must clean-rebuild every executable, static library, test, and
injected calculator; an object or plugin compiled against the retired config
layout or calculator vtable is incompatible.

## 7. Generation barrier

### Cut creation

`CutAndPublishGeneration()` serializes cuts with `cut_mutex_`, then acquires
`admission_mutex_`. While admission remains locked it:

1. snapshots global and per-source exclusive cuts;
2. captures `recv_monotonic_cut_ns`;
3. builds and validates the watermark identity;
4. begins one pending store generation;
5. appends one generation marker to every serial decoder queue.

No post-cut callback can overtake a marker because callback admission remains
locked until all four markers are queued.

### Source and worker fences

A decoder handles its marker only after decoding and submitting every earlier
command in that source queue. `SealSource()` verifies its source prefix, then
places a fence into every worker queue for that source.

When a worker consumes a source fence, it parks that source and does not
consume post-fence records from it. Once all four sources are parked at the
same generation, the worker captures each lane's immutable segment endpoint,
visible count, and latest snapshot/tick locator. It does not copy the
accumulated records.

Only when all worker slices and source seals are present, instrument summaries
match the fixed registry universe, and captured global and per-source totals
match the watermark does the runtime publish one store handle. Instruments
not observed in the prefix still have a summary with zero counts. Generation
construction is O(the fixed instrument count), independent of accumulated
session record count. The last slice changes the pending cut to a building
state under the generation mutex, then performs that O(I) construction outside
the mutex. Commit reacquires the mutex and publishes only if the same
generation is still current and the runtime remains healthy and non-stopping.

### Watermark meaning

For each message admitted by this process, the global counter and exactly one
source counter advance together. The watermark builder checked-adds the four
source counts and requires:

```text
ingress_sequence_exclusive - 1
  == sum(source.sequence_exclusive - 1)
```

`UINT64_MAX` may be an exclusive cut but may not be a message sequence. The
watermark identity hashes the run ID, generation, trade date, global cut,
registry identity, and ordered source cuts. The monotonic cut time is metadata
for process-observed staleness; it is not exchange time and is not part of the
identity hash.

What the watermark proves:

```text
all messages admitted by this process before the captured prefix
have reached this complete fixed-universe store generation
```

What it does not prove:

```text
exchange packet completeness
vendor-server completeness
event-time ordering across instruments
absence of upstream loss before the callback
optional WAL durability
```

## 8. Factor calculation and atomic publication

The factor engine receives the just-published immutable store handle. It first
checks that the handle is the exact current healthy generation and matches the
fixed registry universe. Calculation occurs into private staging memory.

Latest-only factor calculation uses `SummaryAt(index)`. Iterating the
canonical summary ordinals is O(I) for I registry instruments. It reads the
latest snapshot/tick locators and does not drain the O(N) session record
stream.

Before publication, the engine revalidates:

- calculator schema identity and order;
- exactly one output row per registry instrument;
- exact instrument order;
- exact value count per row;
- finite values only;
- invalid values use canonical positive zero.

The candidate `RealtimeFactorGenerationV1` retains the exact input store
shared pointer and copies its watermark. A small commit action executes while
the store generation mutex proves that the input handle remains current,
healthy, and not stopping. The final publication is one release-store of a
single shared factor-generation pointer.

This prevents a reader from observing a half-old/half-new factor cross-section.
It does not make the independent latest-store and latest-factor slots a
single atomic pair. The supported pair-read pattern is:

```text
acquire one latest factor handle
-> read factor.input_store()
-> use the factor and its retained store as the matched pair
```

The default calculator is a last-price representation example, not a trading
model. It projects the most recent valid, strictly positive snapshot p6 last
price into a decimal `double`; zero/negative prices remain explicitly invalid.
Custom calculators must document their mathematics, missing-data
policy, numeric bounds, and execution-time bound.

### Store reads and lifetime

`SummaryAt` is the O(I) latest-only path for factor work. Record cursors are
the historical path: a full-universe drain over I instrument rows and N
records is O(I + N), while each `ReadBatch` uses O(batch) caller-owned pointer
storage. Cursor construction and draining must never materialize a second
N-record result. The configured Store batch value is an upper bound; the
acceptance consumer uses 1,024-record pages by default and consumes them
immediately. Large drains can use independent `OpenUniverseRangeCursor`
instances over non-overlapping half-open instrument-ordinal ranges; each costs
O(I_range + N_range). If no range is truncated by its per-cursor
`maximum_records` setting, joining their outputs in ordinal order reproduces
the full-universe ordering exactly. Acceptance reader affinity is applied only
inside post-stop scan threads; 4--8 range readers each require a distinct CPU
from the inherited, operator-selected NUMA-local mask.

A factor retains its exact input store generation, and a cursor retains its
generation. Those handles can pin the session arena after the runtime stops.
Consumers must enforce a small fixed upper limit on retained factors,
generations, and cursors; otherwise rollover cannot reclaim memory even though
production admission has ended.

The configured record and logical-byte limits cover store accounting, not
complete process RSS. On a 1 TiB host, the initial envelope is a 600--620 GiB
store hard limit, a process high-water alert near 800 GiB, and a termination
boundary around 850--860 GiB. The remainder is reserved for the feeder,
allocator variance, SDK, operating system, and page cache. Actual full-session
and two-times-volume measurements remain release gates.

### From-open reachability

This project does not replay the feeder's CSV or the optional WAL. A watermark
proves the complete prefix accepted by the current process, not exchange or
vendor completeness. `coverage_from_open` is valid only for one process that
started before the first expected market message and remained continuously
healthy. Any process or host restart creates a partial session that cannot
recover the earlier prefix during that trade date.

## 9. Terminal final generation

Normal periodic cuts leave admission open after marker insertion. Terminal
publication has an additional requirement: no accepted shutdown-tail message
may appear after the final marker.

`StopAndPublishFinalGeneration()` therefore performs:

```text
lock stop serialization
-> lock cut serialization
-> lock callback admission
-> close admission and capture the final monotonic prefix cut
-> unlock admission
-> full SDK callback quiescence
-> release Subscriber and IOManager
-> begin final generation from the frozen accepted counters
-> insert final decoder markers
-> wait for complete store generation
-> calculate and atomically publish factor generation
-> stop/join decoder workers
-> stop/join store workers
-> drain, sync, close, and join optional WAL
-> publish stopped=true
```

If a callback itself detects the UTC+08 date boundary, it records the existing
message receive monotonic timestamp as the clean admission cut. A later
terminal call reuses that timestamp rather than the later SDK shutdown time.

`StopAndDrain()` is the idempotent terminal path when another generation must
not be created, such as after a fatal error.

## 10. Timeout scope

The generation timeout is a shared wait budget, not a wall-clock API completion
or process-wide cancellation deadline. Decoder-marker queue backpressure and
the store condition wait consume that budget. Setup and allocation may run
past the deadline, and an operation that is already immediately ready can
still complete after the nominal deadline. The timeout does not interrupt:

- waiting to serialize behind another cut or stop;
- vendor `IOManager::Shutdown()`;
- an already entered SDK callback;
- arbitrary `RealtimeFactorCalculatorV1::Calculate()` code;
- thread joins;
- WAL writes or `fdatasync` during terminal drain.

C++ cannot safely preempt those operations inside this in-process design.
Production SDK implementations and custom calculators must provide strict
runtime bounds, and deployment supervision must own a process-level hard
deadline if one is required.

A custom calculator is non-reentrant: it must not call the owning pipeline's
cut, stop, or publication APIs and must not call store lifecycle APIs. Such a
callback would attempt to reacquire lifecycle locks already held by its own
calculation and violates the pure transformation contract.

## 11. Failure containment

| Failure | Realtime admission | Store/factor publication | WAL coverage |
|---|---|---|---|
| Unsupported non-production tuple | ignored, no sequence | unaffected | unaffected |
| Forbidden combined tick | fatal close | prohibited | prior accepted records may drain |
| Owned-copy/decode/registry/routing error | fatal close | prohibited after fatal linearization | independent prior records may drain |
| Decoder/store queue pressure | fatal close | incomplete generation not published | independent |
| Store record/byte/allocation failure | fatal close | no later generation is advertised complete | independent |
| Barrier timeout or inconsistent cut | fatal close | failed generation not published | independent |
| Calculator/schema/output error | fatal close by pipeline | candidate not published | independent |
| SDK lifecycle failure | fatal or terminate when safe release cannot be proved | no successful terminal result | best-effort drain where safe |
| WAL open/queue/write/sync/close error | continues | continues | sticky `coverage_lost` |
| UTC+08 date boundary | clean close, no wrong-day sequence | final prior-day prefix may publish | drains accepted prior-day handles |

The fatal transition closes admission while holding `admission_mutex_`, then
marks the store fatal under its generation mutex before publishing the pipeline
fatal flag. This lock order makes factor commit and fatal transition
linearizable: after fatal becomes observable, a new factor commit cannot pass
the store health guard.

`StoreSnapshot`, current-generation health checks, factor commit, store
generation replacement, and store stop also share the generation mutex. A
store append can discover and mark sticky coverage loss before its worker
obtains that mutex, so both factor preflight and commit additionally check the
store-failure and coverage flags. If a commit already owns the mutex, it is
ordered before external runtime health observation of that failure; otherwise
the commit observes the failure and rejects the candidate.

Current-handle checks require both identical object pointers and an identical
`shared_ptr` owner/control block. A caller-created no-op-deleter pointer to the
same address is not accepted and therefore cannot weaken the generation
lifetime guarantee.

Whichever generation-mutex operation acquires the mutex first defines the
order: a fully verified generation may publish before Stop begins, or Stop
closes generation admission and no later worker slice may publish.

## 12. Verification surface

The test suite covers, among other cases:

- real test-only DSO `dlopen`/`dlsym` success and object reference lifecycle;
- the exact five subscription keys and forbidden combined key;
- callback-memory ownership after vendor bytes are overwritten;
- WAL enabled/disabled identity equivalence and WAL failure isolation;
- decoder body bounds, list descriptors, text validity, fixed-point overflow,
  calendar endpoints, and message encoding;
- exact Shenzhen source bytes `"102 "`;
- unreachable registry SecurityID rejection before SDK connection;
- deterministic fixed-worker routing and cross-source out-of-order insertion;
- append-only segment rollover without record eviction;
- contradictory watermark rejection and `UINT64_MAX` exclusive cuts;
- source/worker generation barriers and full fixed-universe summaries;
- `SummaryAt` latest-only traversal in canonical registry order;
- full-universe cursor ordering, four-source reconciliation, caller-owned
  batches, and explicit terminal empty page;
- factor missing/reordered/wrong-width/NaN/infinity/invalid-zero rejection;
- exact factor-to-store handle retention;
- a real Shenzhen snapshot through callback, decoder, store, projection, and
  publication;
- record/byte/allocation failure closing the production path;
- partial coverage after a simulated process restart;
- final prefix publication after SDK quiescence;
- slow SDK shutdown not shifting the final prefix timestamp;
- date-boundary timestamp preservation and zero wrong-day sequence allocation.
