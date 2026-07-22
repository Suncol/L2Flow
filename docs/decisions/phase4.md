# Phase 4 decoder and in-memory session decisions

Status: implementation contract
Date: 2026-07-22

This record narrows the Phase 4 construction slice in `docs/design.md`.  It
defines safe market-body decoding and an owned in-memory history for later
factor work.  It does **not** replace, wrap, or duplicate the feeder's existing
ordered CSV/Raw persistence, and it does not define the Phase 5 Canonical log.

## Injection boundary and ownership

`MarketSessionV1::Inject` is the synchronous seam used by an existing feeder or
validated Raw consumer.  The caller supplies one `MarketMessageViewV1` with the
exact source context, per-source order, message key, receive clocks, and a
borrowed body span.  Before `Inject` returns successfully:

- the body is bounds-checked and decoded;
- every published string, scalar, depth level, and revealed queue quantity is
  copied into an owned typed event;
- `DecodedMarketCommonV1::origin.body` is cleared;
- the event is admitted to immutable session storage.

The feeder may reuse or destroy its callback buffer immediately after a
successful return.  CSV/WAL writing remains an independent existing sink.  The
two sinks are not described as a transaction: an injection failure is explicit
and fail-stops that source rather than silently pretending memory and persisted
history still agree.

Decoder output uses a convenient by-value variant.  Its footprint is that of
the largest snapshot alternative, so retaining it directly for every tick
would waste memory.  The session converts it to `RetainedMarketEventV1`, a
small tagged owner of an immutable allocation whose size follows the actual
message type.  This retains typed access without reserving a ten-level snapshot
payload for every order/trade.

The optional `InstrumentRegistryV1` is immutable.  A registry pointer in the
decoder/session configuration is borrowed and must outlive the decoder/session
and every call that uses it.

## Session identity, ordering, and retention

The production topology remains four independent sources:

```text
SH snapshot | SH tick | SZ snapshot | SZ order+transaction
```

Each history is scoped by the exact tuple
`(capture_date, source_stream_id, stream_day_id)`.  `source_sequence` is the
caller's callback/Raw order for that one source.  It must be nonzero and
strictly increasing, but gaps are legal because control records may occupy
intervening ingress positions.

The feeder must invoke injection in that authoritative order for each source.
The per-source mutex prevents data races; it does not reorder two concurrently
submitted callbacks.  If a later sequence wins the mutex first, the eventual
backward append is rejected and that source is poisoned rather than silently
creating a false history.

There is no natural total order across four TCP connections.  The session API
therefore publishes four histories and never sorts them by realtime,
exchange-time, vendor sequence, or bare source sequence.  A later factor mux
must use an explicit Phase 5 ordering/frontier contract instead of treating
host arrival timestamps as exchange causality.

`SessionRetentionModeV1::kFullSession` is the default and retains every
accepted record from construction for the exact trading-day contexts.  It is
the intended mode for "all data since market open" factor research.  A new
session must be constructed at the controlled trading-day boundary; the store
does not guess a day rollover from wall time.

Full-session retention still requires explicit nonzero `max_records` and
`max_payload_bytes`.  They are admission budgets, not ring-buffer capacities:
when either is reached, the new record is rejected, old records are not
overwritten, and `MarketSessionV1` poisons that source so a hole cannot be
hidden.  Operators must size both limits from measured message rates and the
retained-event footprint with safety margin.

`kRecvMonotonicWindow` is an opt-in bounded alternative.  Its inclusive lower
bound is advanced separately by each source's nondecreasing
`recv_monotonic_ns`; another stream cannot evict this stream's data.  Exchange
time and realtime never drive retention.

The byte budget is a deterministic logical payload estimate (typed object plus
owned string capacities), not an allocator- or kernel-specific RSS promise.
Chunk/container metadata, allocator metadata, and immutable snapshots retained
by readers are not charged to that logical counter.  Linux may also page heap
pages out.  Thus the contract guarantees logical accessibility of admitted
history, not physical DRAM pinning.  Production capacity tests must measure
RSS/page faults and bound the number and lifetime of snapshots.

## Immutable snapshots and concurrency

Records move into bounded chunks.  `Snapshot` seals the open chunk and copies
chunk descriptors, not the complete history.  A snapshot or record handle owns
its referenced immutable chunk, so its addresses and contents remain valid
after later appends, logical window eviction, or store destruction.  Readers
traverse a completed snapshot without holding the writer mutex.

Injection is serialized per source because SH 4.24 phase attribution is
stateful and source order is authoritative.  Different sources may decode
concurrently; store publication is serialized for aggregate admission and
watermark accounting.  The product-phase map has its own explicit
`maximum_phase_products` cap because it is decoder state rather than a retained
event charged to the session byte budget.  Reaching the cap fails closed and
does not evict older phase state.

## Decoder contract

The supported core keys are exactly:

```text
4.101.4   SHL2MarketData
4.101.24  NGTSTick
6.101.28  Snapshot300111_v2
6.101.33  Order300192_v2
6.101.36  Transaction300191_v2
```

The implementation statically anchors all hard-coded fixed-field offsets,
packed sizes, message keys, nested item layouts, and the easy-to-confuse fixed
point scales against SDK 2.13.234 at compile time.  Runtime access uses
little-endian loads and `CheckedBodyViewV1`; it never dereferences an
unaligned packed vendor object or calls a vendor accessor as a bounds check.

Relative descriptors are based at the descriptor itself.  All addition,
multiplication, body ranges, list limits, overlap checks, and fixed lower bounds
are checked before access.  Structural failures leave the caller's output
unchanged.  Unknown service versions fail closed as schema unknown.

Fixed point conversion never uses floating point.  Raw integers and scales are
retained, null sentinels remain distinct from zero, and p6 multiplication is
checked before publication.  Action-dependent fields are normalized only when
the documented validity matrix says they are meaningful; for example, a
cancel/status placeholder price cannot reject the message merely because that
meaningless raw value would overflow p6.

SH `A/D/T/S`, SZ ASCII side/order type, and SZ trade/cancel rules have explicit
validity bits.  Unknown actions/enums retain raw values but do not publish
action-specific ID/price/quantity validity.  An unknown printable SH status
does not overwrite a previously known product phase.

Text is copied by explicit length.  Each public text field has its own validity
boolean, and invalid identity text is never submitted to the otherwise
opaque-byte instrument registry.  The registry never trims, case-folds, guesses
a market/code prefix, or derives an instrument ID with `std::hash`.

The exchange-time V1 Unix projection uses fixed UTC+08:00 and rejects trade
dates before 1992 instead of misrepresenting historical Asia/Shanghai DST as
fixed offset.  Exchange-time raw and nanoseconds-since-midnight remain
explicit.  The SDK header `LocalTime` is only a time of day: replay, delayed
delivery, and a capture crossing midnight mean its calendar day cannot be
inferred from `trade_date`.  Phase 4 therefore validates and retains its
nanoseconds since midnight but always leaves its Unix projection invalid.

Public names describe the reviewed business meaning rather than copying
misleading legacy member names: SH `EtfBuy*`/`EtfSell*` are exposed as ETF
subscription/redemption fields, the p5 legacy `WarUpperPri` slot is exposed as
high-precision IOPV, and SZ `OptPremiumRatio` is exposed as a warrant premium
ratio.  The product/version-dependent `WarLowerPri` slot intentionally keeps a
neutral public name until applicability policy is pinned.  The packed SDK
member names and offsets remain compile-time ABI anchors.

Exchange/order identifier domains are kept separate from raw retention.  Raw
SH `BizIndex`, SZ `ApplSeqNum`, and order references remain inspectable, but a
nonpositive event sequence or nonpositive primary order ID is never advertised
through a validity bit.  SZ transaction reference zero means absent; a
negative reference is domain-invalid and cannot be used to infer cancellation
side.  Phase-4 notices distinguish event-sequence and order-reference domain
failures without inventing new bits in the frozen Phase-3 quality bitmap.

## Depth, queues, limit sentinels, and Phase 5 boundary

The owned Phase 4 view records actual depth and retains the first ten levels.
It records actual revealed queue length and retains the first 50 quantities.
Depth/queue truncation is explicit.  This is a safe decode representation, not
permission to publish every such event to Phase 5 Canonical storage:

- malformed descriptor/count/body relationships are decoder errors;
- a structurally accessible queue longer than 50 is retained to 50 with
  `QUEUE_TRUNCATED_TO_50` for Phase 4 diagnostics;
- Phase 5 must reject Canonical publication when `actual_revealed_count > 50`,
  as required by `docs/design.md` section 11.3;
- no undocumented equality between a snapshot level's total order count and
  its nested revealed-list count is invented without a reviewed vendor rule
  mapped to these exact fields.

SZ high/low limit prices require a versioned business-sentinel table that is
not present in this repository.  Phase 4 therefore preserves their exact raw
integer and scale, leaves numeric validity false, sets semantics to `kUnknown`,
and emits `kLimitPriceSemanticsUnknown`.  A factor must not treat a large
sentinel as a real price.  Adding reviewed reference policy later may resolve
the semantics; guessing by magnitude is forbidden.

Decoder limits are explicit defensive ceilings.  A caller may deliberately
configure text/depth/queue ceilings below a normal message's size to reject it;
`configuration_valid()` means the limits are internally coherent, not that
every normal core message is guaranteed admission.  Likewise,
`kUnsupportedMessage` only identifies a tuple outside this focused decoder.
The routing layer may ignore it only after classifying it as optional;
otherwise a required unknown tuple must fail closed.

The Phase-4 phase enum is intentionally only the SH 4.24 attribution subset.
SZ snapshot phase text and SH snapshot instrument status remain raw owned text
with per-field validity.  Phase 5 must introduce a reviewed applicability and
normalization policy rather than coercing those richer raw states into this
subset.

## Failure and phase boundary

A body/schema/ordering/context/admission failure poisons only its configured
source in `MarketSessionV1`.  Unknown source IDs do not mutate a known source.
Diagnostics preserve separate typed reasons for decode, retained-event
materialization, and store admission.  Configuration, resource, numeric, and
unexpected failures are not mislabeled as relative-offset corruption merely to
fill a quality bitmap.

This standalone slice deliberately stops before Phase 5 sequence guards,
deduplication, Canonical mmap logs, global/source frontiers, latest state, or
factor runtime.  A later, independently authorized composition change moved
`L2Flow::production` to `l2flow_production` and reuses this decoder through
`ProductionSourcePipelineV1`; it does not use `MarketSessionV1` as the
production history path.  That change does not retroactively complete the
formal Phase 4 exit.  The real per-message corpus and independent field
reconciliation recorded in `docs/design.md` remain external gates which local
golden tests cannot replace.
