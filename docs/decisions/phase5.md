# Phase 5 Canonical injection and frontier decisions

Status: implementation contract
Date: 2026-07-22

This record fixes the repository-local Phase 5 boundary. It does not declare a
production cutover or completion of the external Phase 5 exit gates.

## No duplicate feeder persistence

Phase 5 does not write feeder CSV, Raw WAL, callback journals, or feeder
checkpoints. `CanonicalBundleCoordinatorV1::ProcessMarket` and
`ProcessControl` consume caller-fed records only after their cursor is covered
by an acquired `SourceFrontier` append prefix. That frontier proves a high-water
prefix only: it cannot prove that an arbitrary intermediate `(ingress, WAL)`
pair is a record boundary or bind that cursor to supplied decoded content.
Both market and control envelope verifiers are therefore mandatory. They must
authenticate the exact next record and the complete immutable `Process*`
envelope against the caller's already ordered feeder or validated Raw reader;
repeating the component-wise frontier comparison is invalid. Phase 5 produces
only the derived fixed-schema Canonical mmap layer. Raw remains the replay
authority and the existing feeder remains the only owner of its ordered
persistence.

Phase 4 `SessionRetentionModeV1::kFullSession` is the owned “since construction
/ since market open” decoded history. Its nonzero `max_records` and
`max_payload_bytes` are aggregate hard admission limits across the four
configured streams: exhaustion rejects the current admission and poisons that
source without evicting old history. Phase 4 storage is an independent
injection seam; Phase 5 `ProcessMarket` does not populate a `MarketSessionV1`.
A caller that needs both views must explicitly fan out the already ordered
input and own the combined failure policy. Phase 5 sequence guards and SH
phase attribution are also all-day state. Canonical segment mappings expose
their complete physical/debug published prefix while the caller retains them;
only the committed-reader/safe-mux path defines logical consumability. Each
segment has a fixed capacity and Linux may page mappings out; neither mapping
nor logical byte accounting is a physical-DRAM pinning promise.

## One-shot source and Canonical generations

One 4096-byte `SourceFrontierPageV1` belongs to exactly one immutable Raw
writer instance/generation. Initialization is one-shot on an all-zero page;
the page is never rebound in place. Callback gates, source-state updates,
append progress and processed progress all carry the expected writer instance
and generation, so a stale callback or publisher cannot mutate a replacement
generation. The progress seqlock covers state, quality and cursor/time
progress; the callback generation plus inflight count closes the idle-read ABA
window.

FATAL is a monotonic latch, not a transient source status. Once latched, append
and processed progress, callback entry and healthy state recovery are rejected.
The coordinator first publishes the lock-free SourceFrontier FATAL latch as the
global revocation anchor, then fail-stops the normalizer and fans out
`generation_fatal` to every configured Canonical segment. Thus a crash during
fan-out cannot leave another family authorized by a still-healthy frontier.

Repository-local V1 has no normalizer checkpoint codec and no coordinated
segment-rotation protocol. Coordinator creation therefore requires a fresh
normalizer, `processed_ingress_sequence == 0`, and empty, open, non-fatal sinks
from one Canonical generation. Fixed sink capacity must cover the complete
trading day. It is invalid to attach a fresh coordinator at midday, rotate to a
new sink in place, or resume an old generation from a saved local cursor.

## Frozen Canonical V1 semantics

The direct-mmap ABI is little-endian and fixed:

```text
CanonicalHeaderV1          112 bytes
CanonicalTickRecordV1      192 bytes
CanonicalSnapshotRecordV1 2048 bytes
CanonicalQualityRecordV1   192 bytes
CanonicalControlRecordV1   256 bytes
```

The schema and NumPy dtype descriptors have independent SHA-256 identities.
Every segment pins schema, dtype, registry, normalizer build/config, capture
day, stream day, Raw writer instance/source generation, full clock identity,
Canonical generation and segment sequence. Clock display labels are not
identity; algorithm plus all 32 digest bytes are.

Quantities use exact integer arithmetic. Tick native quantities must have
scale zero and be nonnegative. Snapshot p3 quantities are admitted only when
`raw / 10^scale` is exact, nonnegative and representable; no floating-point or
rounding path exists. Validators independently reject negative valid scalar,
depth and queue quantities, unknown bits/enums, and contradictory quality
type/scope pairs. Tick projection is also producer-exact: SH A/D/T/S, SZ 6.33
and SZ 6.36 have separate reachable-field masks and enum/ID relations; SZ price
is valid only for raw limit order type 50, SZ cancel infers primary ID and side
only from exactly one positive order reference, and every tick business
sequence is in the positive signed-64-bit wire domain.

## Sequence scope and transaction

Vendor sequence scope is exactly:

```text
(capture_date, source_stream_id, stream_day_id, ServiceID, MessageID)
```

Service version, encoding, vendor LocalTime and body bytes are exact duplicate
evidence, not scope components. SH exchange scope is trade-date/source/channel;
SZ Order 6.33 and Transaction 6.36 share one trade-date/source/channel guard.
Reconnect and subscription epoch never reset these scopes.

Expected-first policy is keyed per exact scope. `AllowUnknownFirst` forbids a
contradictory expected table and makes unknown-prefix quality sticky.
`RequireConfiguredFirst` fails closed when the encountered scope is absent.
Conflict, backward observation and bounded-history exhaustion poison only that
scope when the normalization transaction commits. Later poison diagnostics
retain the first committed bad Raw WAL cursor; an aborted preparation cannot
move or create that cursor.

`CanonicalNormalizerV1::Prepare` (market) and `PrepareControl` (control)
allocate event IDs and stage all sequence/phase/control output without changing
committed state. The public coordinator entry points are correspondingly
`CanonicalBundleCoordinatorV1::ProcessMarket` and `ProcessControl`.
`CommitPublished` accepts only the independently recomputed ordered
count/SHA-256 receipt. A receipt mismatch or post-publication failure is
generation-fatal. `FailStop` detaches outstanding guard tokens, permanently
rejects new preparations in that normalizer and prevents reuse of physically
visible event IDs.

## Bundle publication and logical visibility

For each append-covered Raw item of one source, the coordinator executes one
logical bundle transaction over its configured, independently owned segment
sinks:

```text
validate immutable frontier/Raw namespace and append coverage
authenticate the exact next Raw record and full immutable envelope
prepare market or control transaction
preflight every configured sink (including an empty route)
publish every routed record in transaction order
independently recompute and commit the receipt
advance every configured sink's processed Raw cursor
publish SourceFrontier.processed_* last
```

Creation requires the complete normalizer route manifest, not merely a subset
that happens to cover an early sample: Snapshot and Tick each have exactly one
sink for every configured shard, while Quality and Control each have exactly
one shard-0 sink. Missing, duplicate and out-of-domain routes are rejected at
day-start attach.

The final SourceFrontier update is this source's only cross-family logical
commit point; it does not create a transaction across sources.
`CanonicalSegmentReaderV1::PublishedRecord` is deliberately a physical/debug
view. `ReadCommittedCanonicalRecordV1` performs two acquire frontier reads,
checks exact Raw writer/generation and clock identity, validates the record,
checks the segment generation-fatal latch, and returns it only if both ingress
and WAL are covered by the global processed prefix. Every required
`SafeMuxInputV1` borrows the live SourceFrontier page; the mux re-reads it at
decision time instead of trusting a saved healthy snapshot, and applies the
same coverage/state rule so a half-published or already-revoked bundle cannot
reach a factor. These reader checks do not replace the envelope verifier at
injection time.

Capacity exhaustion found during preflight is abortable and publishes nothing;
identity/corruption/fatal preflight errors poison the generation. Once a
generation-fatal path is entered, SourceFrontier FATAL is published first,
then the normalizer and every segment are fail-stopped. The global processed
cursor may retain its previous
numeric value, but that value does not keep the old prefix consumable:
committed readers and safe mux reject the entire fatal generation, including
records committed before the fault, and `Seal` refuses a fatal segment. V1
never truncates a plausible local tail or resumes inside that generation.

A successful committed read or mux selection is a point-in-time authorization,
not an irrevocable lease over borrowed mmap bytes. Downstream state must retain
the participating generation identities and be discarded if a live frontier
later becomes FATAL; no C++ API can retroactively erase bytes already consumed
by a caller.

`InspectCanonicalSegmentForRecoveryV1` is read-only: it validates a sealed
segment header and its two hashes (manifest validation is a separate caller
step) or reports `kUnsealedDiscardWholeGeneration`; it does not delete,
truncate, repair, rotate, replay, or switch a generation. Crash or fatal
recovery must allocate a new SourceFrontier page and Canonical generation, then
replay Raw from the trading-day start to reconstruct connection, sequence and
SH phase state when that replay stays within one Raw-writer/clock identity.
Cross-identity or cross-clock replay needs the external generation-chain/state
transition that Local V1 does not implement. A last sealed/processed cursor is
not a V1 normalizer checkpoint. Those actions remain caller-owned recovery
orchestration.

`CanonicalSegmentWriterV1::Seal` is only a local single-segment operation. It
does not prove that SourceFrontier is healthy/caught-up, that the normalizer has
no active transaction, or that sibling family/shard segments share the same
processed cursor. Repository-local V1 therefore does not treat a set of local
Seal results as a production generation cutover certificate. Likewise, an
intraday clock-epoch or Raw-writer generation change has no in-place continuity
path here; it requires a new generation and day-start replay under external
orchestration.

## Formal phase boundary

The repository-local primitives and deterministic tests do not prove a full
trading day, target-host throughput, crash matrix, or live/replay equality.  A
later, independently authorized production-composition change moved
`L2Flow::production` to `l2flow_production` and connected this Canonical
runtime to the fresh live path.  That alias change does not retroactively
declare formal Phase 5 exit complete.  Full-day live/replay equality, a real
process-crash matrix, repeated mux-order hashes, single-writer evidence and
target-host 2×/5× performance artifacts listed in `docs/design.md` remain
required external evidence.
