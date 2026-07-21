# Phase 3 control-decoder implementation decisions

Status: implementation contract
Date: 2026-07-21

This record narrows `docs/design.md` for Phase 3. It freezes the decisions that
must be shared by live Raw tailing, full Raw replay, checkpoints, readiness, and
tests. It does not weaken the Phase 2 durability contract and does not define
Phase 5 Canonical storage.

## Boundary and ordering

The authoritative input is one validating `RawRecordView` stream in exact Raw
`ingress_sequence` order. API, SYS, and market records are observed in that one
order. The decoder runs after the Raw writer and never parses control messages
in an SDK callback or consumes the callback `ByteRing` directly.

Live and replay use the same transition function. The transition function must
not branch on `is_replay`, `connection_epoch_hint`, wall-clock time, container
iteration order, or process identity. `connection_epoch_hint` remains diagnostic
only.

## `ControlRecordV1`

Phase 3 defines an independent, explicitly encoded `ControlRecordV1`:

- each record is exactly 256 bytes;
- every integer in its wire representation is little-endian;
- encoding and decoding use an explicit codec, not a packed host object;
- reserved bytes are written as zero and rejected when non-zero;
- unknown versions, control types, flags, or enum values are rejected;
- every record carries its Raw namespace and exclusive origin record-end cursor;
- long strings and variable lists remain in Raw; the control record contains
  only normalized codes, counts, and cryptographic hashes.

This is not the Phase 5 `CanonicalHeaderV1`/`ControlRecordV1` record and must not
use the Phase 5 Canonical magic, schema identity, segment, or publication claim.
Phase 5 may adapt this record into a Canonical control event, but it may not
silently reinterpret these 256 bytes as a Canonical record.

The exact magic, field offsets, reserved ranges, schema SHA-256, and golden
bytes are frozen by `schemas/control_record_v1.json`. Its exact source-byte
SHA-256 is
`0a49233912fde159bd238b38b8bf2c3aca921432826f2ea7fa4168aaed74b14b`.
The build invokes `tools/generate_phase3_control_schema.py`, which rejects a
schema that differs from the frozen layout/enum/flag/CRC contract and emits the
C++ constants consumed by the explicit codec. The codec golden test is an
independent byte construction; it does not call the encoder to manufacture its
expected bytes.

The exact source-byte SHA-256 of
`tools/generate_phase3_control_schema.py` is
`0b776cf2ebf9edb40cb24c729da75bb711ebbbe76dc06cbe7a22af3eb228cabc`.
Given the two frozen schema inputs, its generated
`l2flow/control/phase3_schema_v1_generated.h` has exact byte SHA-256
`45653663437581331265c376f620d3572fd3ba9da9be1d0585775ebbf754e53a`.
The generated header is a build-tree artifact; its hash is stated here so a
regeneration check can compare bytes without treating a generated timestamp or
build pathname as schema input.

## Canonical subscription manifests

A message key is the numeric tuple
`(service_id, service_version, message_id)`. Canonical ordering is ascending
lexicographic numeric order over that tuple. Duplicate keys are malformed; an
implementation must not use last-wins behavior.

The requested manifest contains exactly the keys actually requested from the
SDK for this ingress generation: every required key and each explicitly enabled
optional key. Forbidden and disabled optional keys are not requested entries;
they remain part of the stable service configuration and its hash. Each
requested entry also carries its `REQUIRED` or `OPTIONAL` policy.

The requested-manifest hash input is:

```text
ASCII("L2FLOW_PHASE3_REQUESTED_SUBSCRIPTION_MANIFEST_V1") || 0x00
|| entry_count:u32-le
|| for each canonically sorted entry:
     service_id:u8
     || service_version:u16-le
     || message_id:u16-le
     || policy:u8                    # REQUIRED=1, OPTIONAL=2
```

All arithmetic and the entry count are checked. Other policy values are
rejected.

A decoded LogonResponse or SubscribeResponse response manifest contains its
complete, safely resolved list of
`(service_id, service_version, message_id, message_status)` tuples. It is sorted
by the same message-key order before hashing. Duplicate response keys are a
malformed control message. A safely decoded unrequested key remains in the
response hash for audit, but cannot change readiness, the requested manifest,
or the effective requested-key set.

The response-manifest hash input is:

```text
ASCII("L2FLOW_PHASE3_SUBSCRIPTION_RESPONSE_MANIFEST_V1") || 0x00
|| entry_count:u32-le
|| for each canonically sorted entry:
     service_id:u32-le
     || service_version:u32-le
     || message_id:u32-le
     || message_status:u32-le
```

The wider response fields preserve the vendor SYS body widths. Requested and
response hashes therefore have distinct domains and cannot be substituted for
one another even when they describe the same message keys.

## Connection epoch

Epoch state is scoped to exactly one Raw namespace:

```text
(capture_date, source_stream_id, stream_day_id)
```

It starts at zero in each namespace. A bare epoch number is not comparable
across namespaces.

- The first safely decoded `LogonResponse(ReturnCode=MDLEC_OK)` changes the
  connection epoch from 0 to 1.
- Each later successful LogonResponse increments it once with checked
  arithmetic.
- A failed or malformed LogonResponse does not increment it.
- Connecting, ConnectError, and Disconnected API events do not increment it.
- Disconnected belongs to the last successful old epoch.
- A process restart continues the namespace epoch by replaying Raw or by
  loading and validating a checkpoint before replaying its suffix.
- A market record before the first successful logon has epoch 0 and
  `SESSION_UNKNOWN`.

After a Disconnected or ConnectError event, and until the next successful
LogonResponse, market records retain the last successful old epoch and carry
both `SESSION_UNKNOWN` and `SOURCE_DISCONNECTED`. Connecting does not end this
window. No record in the window is assigned prospectively to the next epoch.

## Subscription epoch and incremental responses

`subscription_epoch` also starts at zero in each Raw namespace. Its comparison
value is the effective successful requested-key set: requested keys whose
current safely decoded status is `MDLEC_OK`.

The epoch increments exactly once when, after applying one valid control
response atomically, that set differs from the previously committed set. Both
addition and removal are set changes. A new connection reporting the same
effective successful set does not increment the subscription epoch. Resetting
per-connection readiness evidence must not create a transient empty set or a
pair of artificial epoch increments.

A valid successful LogonResponse establishes the response evidence for its new
connection atomically. An outer failed LogonResponse does not mutate the
committed effective set or subscription epoch, but it clears readiness for the
current connection attempt.

`SubscribeResponse` is an incremental response. Only requested keys present in
that response update their current status; absent keys retain their prior
status. After applying the complete response atomically, the decoder recomputes
the effective set and applies the one-increment rule above. Required-key
failure makes the ingress NOT_READY; optional-key failure is retained in state
and control output but does not by itself make the ingress NOT_READY.

The authoritative subscription state retains at least:

- requested-manifest hash;
- canonically sorted per-requested-key status and policy;
- effective successful requested-key set;
- failed required and failed optional sets;
- response Raw ingress sequence and exclusive record-end cursor;
- connection epoch and subscription epoch.

Counter overflow is fail-closed; epochs never wrap.

## Malformed control input

All relative strings and nested lists are validated against the complete vendor
body with checked arithmetic before access. Relative offsets remain relative to
their descriptor objects. Vendor accessors are not used as bounds checks.

A malformed API/SYS control body poisons the Phase 3 control state for that Raw
namespace and makes readiness false. It must produce bounded diagnostic state
when the fixed Raw metadata is available. It must not:

- stop, truncate, or rewrite Raw capture;
- change a connection or subscription epoch using partially decoded fields;
- allow an exception to cross the decoder boundary;
- restore READY merely because a later control record is well formed.

The decoder may continue scanning subsequent Raw records to advance audit and
diagnostic cursors, but the poison is sticky for that namespace. Recovery from
the condition requires an explicit reviewed policy or a new derived generation;
it is never an implicit last-good-value fallback.

Raw framing, CRC, namespace, or continuity failure remains a Phase 2 Raw error,
not a malformed Phase 3 control-body classification.

## Checkpoint contract

A Phase 3 checkpoint is an optimization. Full Raw replay remains the authority.
Every checkpoint is versioned and binds, at minimum:

- the complete Raw namespace identity;
- an exclusive processed Raw record-end cursor that is revalidated as a real
  record boundary;
- the stable effective configuration hash;
- the requested-subscription-manifest hash;
- the canonical control-state hash;
- the connection and subscription epochs and all sorted state needed to
  recompute that hash.

The state hash uses its own domain-separated, versioned canonical encoding. It
does not contain volatile heartbeat/lag samples, process identity,
`connection_epoch_hint`, or unordered-container iteration order.

On attach, any schema, namespace, cursor, config, requested-manifest, or state
hash mismatch rejects the checkpoint. The decoder then performs full replay or
fails closed; it never skips Raw to preserve availability. Replay of the suffix
from a valid checkpoint must produce the same final state hash and records as a
full replay from the namespace start.

Checkpoint publication must complete its file and actual-parent-directory
durability barrier before it is accepted after restart. A checkpoint never
advances the Raw durable frontier.

The V1 checkpoint wire schema is frozen by
`schemas/control_checkpoint_v1.json`; its exact source-byte SHA-256 is
`90b70205e11edcc9f01ebf5cd7f6c0090e4d03420fde355b15a23305820147a2`.
Publication uses an owner-only retained directory, an `O_EXCL` temporary,
complete write and file `fsync`, no-replace rename, and actual-parent-directory
`fsync`. Its process-local receipt retains the synchronized file and directory
inodes and revalidates exact canonical bytes and the final name-to-inode map.

Restart discovery is read-only. It validates every final in the requested Raw
namespace as a same-filesystem, owner-only, single-link regular file, requires
stable canonical decode/re-encode and an exact model-derived name, rejects any
checkpoint beyond the current durable frontier, and requires all eligible
cursors to form one strictly increasing chain in both ingress sequence and WAL
end. Discovery alone is never an attach authorization: the caller must locate
the exact validating durable Raw boundary record and pass it to
`ControlDecoderV1::Restore`.

## Readiness and `ReSubscribe`

Phase 3 readiness uses the authoritative control state and the same coherent
sampled Raw append-frontier catch-up gate as the Phase 2 observer. It also
retains the required-market first-seen structural check after Raw. It cannot
reuse control or market evidence from an earlier connection to complete the
current connection's readiness.

`ReSubscribe()` is denied by default. It is allowed only when all of the
following are true:

1. an explicit, versioned maintenance window says the operation is outside the
   trading interval;
2. the old and proposed requested-manifest hashes, namespace, actor, reason,
   and intended operation have been published in a durable audit record;
3. the audit record's file and actual-parent-directory barriers have completed;
4. the control state is not poisoned and the caller holds the reviewed
   operational authorization.

Failure to prove any condition is a rejection before calling the SDK. An
environment variable, ad-hoc CLI override, wall-clock guess, or in-memory log is
not a maintenance-window or durable-audit proof. The response is still applied
by the incremental rules above; a required subscription failure keeps READY
false.

Phase-3 V1 has no ordered Raw record that carries the complete proposed
replacement keys and policies. Therefore its guard performs all of the checks
above but still returns `NO_REPLAYABLE_MANIFEST_TRANSITION`; it never authorizes
the SDK call. Returning ALLOWED from only a durable audit receipt would make
live and full Raw replay state diverge.

## Live service bridge

`ControlLiveWorkerV1` is the single-consumer bridge from a validated
`RawLiveTail` to the authoritative decoder and an explicit derived-record sink.
Construction takes a fresh, non-cached Raw control sample. That sample must be
non-fatal, cursor-shape valid, and have its append cursor exactly equal to the
tail attach cursor; append-visible backlog is rejected rather than relabelled
as evidence for a new SDK generation. The decoder cursor must equal the same
attach cursor; a zero decoder may attach only at the stream-day genesis
boundary. A non-genesis attach therefore requires full replay or a checkpoint
restore through the immediately preceding validated Raw record.

The service must quiesce callbacks from the preceding SDK generation before
construction and must not issue the new `Connect` until construction succeeds.
The worker retains the fresh sample's control generation and the attach's first
ingress sequence. A successful logon qualifies this configured
`connect_generation` only when its Raw origin is at or after that first sequence
and its observed control generation is newer than the construction sample. A
control-page generation is an ordering receipt; it is not a substitute for the
callback-quiescence and Connect lifecycle barriers.

Each emitted logical record is explicitly encoded to the canonical 256-byte
wire before publication. The sink call is synchronous and receives an absolute
monotonic deadline. Only `PUBLISHED_NEW` and `ACCEPTED_IDENTICAL` acknowledge
replay-safe retention; conflict, failure, an invalid clock, or a return after
the deadline fail-stops the generation. The worker cannot preempt a blocked
sink, so returning by the deadline is a mandatory sink implementation
property, not a timeout guarantee supplied by the worker thread.

An unhealthy, fatal, pre-start, or publication-in-flight worker cannot expose a
checkpoint model; a zero-record decoder also has no publishable checkpoint.
Even a returned model is only logical state: the caller must bind it to a
freshly sampled durable Raw frontier, publish it with the checkpoint store's
file and parent-directory barriers, and on restart locate the exact validating
Raw boundary before `ControlDecoderV1::Restore`. It never authorizes skipping
Raw validation or advances Raw durability.

Malformed control records are the deliberate exception to a non-ok decoder
result: their Raw cursor and bounded `DECODE_ERROR` record commit, the namespace
poison is sticky, and live tailing continues. Readiness always performs a fresh
Raw control read; a failed read cannot reuse a prior READY sample.

The service owns the blocking `Run` thread. A normal shutdown supplies one
immutable exact terminal cursor with `StopAt` and joins only after catch-up; an
abnormal shutdown calls `Abort`. In both cases it must join `Run` before
destroying the worker or the `RawLiveTailSource` borrowed by the owned tail.
`StopAt` rejects an unaligned cursor, sequence movement without enough Raw
bytes, a rebound segment base, or a segment jump without every intervening
4096-byte header before latching the target. Segment headers may advance the
worker's volatile WAL frontier, but do not advance decoder state.

The bridge samples external Raw health and realtime/calendar evidence rather
than manufacturing either fact. It does not itself create SDK subscribers,
perform Phase-2 recovery, locate a restart checkpoint boundary, or switch the
four production ingress services to `L2Flow::phase3`.

## Completion boundary

These decisions permit implementation and local verification of the Phase 3
control decoder. They do not by themselves:

- complete the Phase 2 production cutover;
- satisfy any target-NVMe, full-trading-day, crash, power-loss, restart, or
  external endpoint acceptance condition;
- turn a Phase 2 control page into durable evidence;
- make a Phase 3 record a Phase 5 Canonical record.

Phase 2 Implementation, Local verification, and external exit evidence remain
separate completion claims under `docs/design.md` and
`docs/acceptance/phase2-local.md`.
