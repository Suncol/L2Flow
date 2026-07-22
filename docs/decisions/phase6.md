# Phase 6 Latest State local contract

Status: repository-local implementation contract
Date: 2026-07-22

This record defines the Phase 6 library boundary. It does not declare the
external Phase 6 exit gates complete and does not change the production alias.

## Slot and generation ownership

`LatestStateSlotV1` is an opaque, 4096-byte, 64-byte-aligned shared-memory ABI.
Callers may place it in a suitably aligned shared mapping, but must not access
its fields directly. Every concurrently accessed 64-bit word is loaded or
stored by the C ABI implementation with `__atomic_*`; the first word is the
seqlock. The implementation refuses unsupported hosts when the required
64-bit atomics are not lock-free. `latest_state_c_api_v1.h` is also compiled by
a strict C11 translation unit; the C ABI does not require a C++ consumer.

One table generation pins the state writer instance, shard topology,
Canonical schema/dtype, registry version/hash and state generation. A slot is
initialized by its first complete snapshot and is never rebound or reset in
place. Phase 5 generation revocation is point-in-time: an external owner must
discard the corresponding generation-tagged state table after a later live
`SourceFrontier` FATAL. The slot bytes cannot retroactively revoke a value
already returned to a reader.

## Authority and publication

Only a validated full `CanonicalSnapshotRecordV1` publishes the authoritative
snapshot payload. The exact capture/source/stream-day, Raw writer/source
generation, Canonical generation, full clock identity, schema/dtype and
registry identity are supplied separately and stored with it. Cursor
regression and same-cursor conflicts fail closed; an exact duplicate is a
no-write idempotent result.

The complete fixed Snapshot record is republished in one seqlock transaction.
Consequently a depth reduction, queue reduction or new null validity cannot
retain an older tail. Tick quality has its own source identity, cursors,
receive time and quality word. It never overwrites snapshot payload or
snapshot quality.

Staleness is a read-time result. The caller supplies one nonnegative threshold
for each frozen trading-phase value and a current monotonic time in the same
algorithm plus full clock digest. The library assigns no exchange schedule or
phase duration. A stale result adds `SNAPSHOT_STALE` only to returned effective
quality; persisted snapshot quality is unchanged.

`LatestStateLocalQueryV1` is a sorted directory over caller-owned slot
mappings and implements single, batch and market scans. The mappings must
outlive it. This local slice does not start a UDS/gRPC server; such a server is
an operational wrapper and must not enter the state-writer thread.

## Checkpoint semantics

The checkpoint codec uses a deterministic little-endian wire image, sorted
instrument IDs, fixed headers, payload SHA-256 and CRC. Logical market-state
identity normalizes the seqlock, output table generation/writer owner and
display-only clock labels while retaining source lineage, clock algorithm and
all digest bytes. The checkpoint payload hash still binds the retained table
identities and labels. Restore requires an exact configuration,
an exact all-zero target set and publishes fresh even slot sequences; it never
overwrites a live table.

By default a checkpoint is explicitly non-durable and may only accelerate
recovery. Marking one `durability_barrier_satisfied` requires
`writer_quiesced=true`: the caller must first stop publication and take the
whole table at one common cut, because independent per-slot seqlock copies do
not by themselves prove a table-wide instant. That same cut also requires
exact barriers for every consumed snapshot and tick-quality Raw namespace and
numerical proof that each required WAL end is no greater than the current Raw
durable position. The quiescence flag, barrier boolean and positions are caller
assertions, not non-forgeable receipts. A production publisher must obtain and
retain the validated Raw journal/control authority independently; Canonical
processed or frontier progress is not durability authority.

The codec and logical-hash comparison prove repository-local serialization
and replay equivalence only. Atomic checkpoint file publication, SHM table
generation cutover, periodic scheduling and crash orchestration remain
caller-owned.

## Formal boundary

The local implementation and tests do not provide a target-host 1000-symbol
batch-read p99 report, a live vendor snapshot shadow comparison, or a real
process crash/recover exercise. Those are the formal Phase 6 exit artifacts in
`docs/design.md` and remain open.
