# Phase 7 consumer and factor-runtime local contract

Status: repository-local implementation contract
Date: 2026-07-22

This record defines the implemented Phase 7 boundary. It deliberately
separates runtime infrastructure from the five named factor placeholders and
does not claim the Phase 7 external exit gates.

## Native consumer boundary

`CanonicalCommittedBatchReaderV1` attaches only when event family, fixed
record size, schema hash, dtype hash and registry identity match the caller's
expectation. The C batch-open path additionally pins the complete expected
segment descriptor, including source/capture/trade day, stream day, Raw writer
and generation, full clock identity and Canonical generation. A mandatory Raw
observer supplies the current exact Raw control snapshot; its durable cursor
is diagnostic batch metadata, not part of stable input identity.

`Peek` returns a zero-copy, read-only span retained by a shared mmap owner and
does not advance the exclusive-next cursor. `Commit` accepts only that reader's
current `[begin,end)` view, calls the reviewed Phase 5 committed-record gate at
the tail, and revalidates live source generation and WAL coverage before
advancing. A plugin exception therefore leaves the batch replayable. A later
FATAL can invalidate authorization even after an earlier successful peek.

The C-compatible consumer header exposes batch handles plus safe-mux and
snapshot-as-of calls. Those calls translate to the Phase 5 proof helpers; they
do not reimplement ordering in Python. The repository-local Python package
ships a fail-closed native-mux seam rather than pretending a native adapter is
installed. Deployments must bind the C ABI explicitly.

## Python view, spec and transaction ownership

Python defines exact little-endian NumPy layouts for the frozen Tick and
Snapshot records and validates full attach/generation/registry identity plus
minimum vectorized header invariants. Public/manual attach accepts only a
bytes-backed immutable ndarray, not a view whose retained owner can still
write. A real zero-copy adapter must instead own a read-only native mapping and
native view handle. `MdlBatchView.records` is read-only and
ordinary access through the leased subclass fails outside its single-use
context. This is an API lifetime guard, not a security or memory-revocation
boundary: an explicit copy intentionally survives, and arbitrary NumPy buffer
escape mechanisms cannot be made unforgeable in pure Python. Native mapping
ownership must therefore remain attached to any real zero-copy ndarray base.

`FactorSpec` is frozen, sorts declarative inputs/windows, separates validity
and quality masks, validates each input mode and hashes canonical JSON.
`LIVE_LATEST` requires a zero-source Latest-State declaration and an explicit
non-deterministic marker. The local V1 Python data view supports Canonical Tick
and Snapshot families; quality/control are not silently exposed under an
incorrect dtype.

Instrument ownership is the fixed rule `instrument_id % 16`. The transaction
runtime represents one logical shard owner; it does not spawn or supervise 16
OS processes. Plugin state, cursor, output identities and generation tags are
staged before native commit authorization. Exceptions restore the prior state.
All allocation-bearing map updates are prepared before the final native
revalidation; after it succeeds, commit is reference replacement. A successful
batch invokes the native commit callback once; replay of its committed cursor
is rejected, and the runtime retains neither transaction objects nor outputs.
Output identities belong to the external sink's idempotency protocol. Each
committed transaction must also carry a strictly increasing run-local
`watermark_set_id`. A denial or explicit FATAL invalidation restores initial
state, clears the generation's derived maps and permanently rejects reuse of
that runtime.

Count and inclusive event-time windows update in O(1). Their versioned
checkpoints store exact float64 accumulator bits and the last timestamp, so a
checkpoint split continues with the same arithmetic state instead of
recomputing a different sum from retained values.

## Watermarks, durability and latest output

The stable input-identity preimage is frozen in C++ and Python as:

```text
"l2flow.factor.input-identity.v1\0"
+ trade_date:u32-le + entry_count:u32-le
+ entries sorted by (source, capture day, stream day, family, shard)
```

Each entry contributes the exclusive Canonical cursor, maximum consumed Raw
WAL end, clock algorithm plus full digest and input quality. Run-local
watermark ID, observed Raw durable position and display-only clock label are
excluded. Durability is checked independently for every exact
`(origin_capture_date, source_stream_id, origin_stream_day_id)` namespace.
The Python mapping of durable positions is trusted caller input; only the C++
path directly validates `RawControlSnapshot` coherence. Neither path treats a
Canonical frontier as Raw durability authority.

Factor checkpoints use strict duplicate-key-free canonical JSON, bounded
lengths, state/config/code/registry hashes, the complete watermark map and an
envelope hash. For the known runtime-state codec, load binds the outer factor
ID/version/config hash and registry identity to the decoded runtime state;
the config hash is the exact `FactorSpec` hash. A generic codec is accepted
only with an explicit caller binding assertion. Publish requires the
per-namespace barrier and performs temp write, file fsync, atomic replace and
directory fsync. Load uses a no-follow descriptor, bounds the regular file
before reading, revalidates identities and rechecks the current barrier. It
does not authenticate the caller's durable-position provider.

`LatestFactorSlotV1` is a 4096-byte, 64-byte-aligned opaque slot with atomic
word access, seqlock snapshots, immutable factor/instrument slot identity,
monotonic as-of publication and idempotent replay. C entry points reject slot,
value and result storage overlap before writing. The reviewed C++ publish path
resolves an already appended exact watermark and rejects an orphan ID or
identity hash; appending the row remains a separate caller step. The lower-level
C slot ABI necessarily treats that relationship as a caller protocol. The
append-only watermark table resolves every process-local Latest reference to
its full map. This local table is in-memory; durable retention/sidecars belong
to later storage integration.

## Five placeholders

The named entries are present exactly as:

```text
book_imbalance
microprice
trade_imbalance
cancel_rate
trade_intensity
```

All five implementations have status `PASSTHROUGH_PLACEHOLDER`. While the
batch context is active they return the exact original leased records and a
provenance digest. They always publish `factor_value=None` and
`factor_value_valid=False`. They do not evaluate a formula, do not constitute
mathematical factor output, and do not make `trade_imbalance` synonymous with
another factor name. The leased records cease ordinary access after context
exit unless the caller explicitly copied them.

Accordingly, mathematical small-sample/extreme/window agreement and the
five-factor full-day equality gate are not claimed. Target-host 2x throughput,
random process-crash output hashes, a real factor-worker deployment,
persistent watermark retention and Python 3.11 deployment evidence also
remain external work. `L2Flow::production` now selects the independent
`l2flow_production` live composition, but that composition stops at Canonical
plus process-local instrument history and does not run these Phase 7 factor
plugins.
