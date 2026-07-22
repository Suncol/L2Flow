# Phase 8 historical-storage local contract

Status: repository-local implementation contract
Date: 2026-07-22

This record defines the local Phase 8 boundary for Parquet publication,
historical lookup, retention planning and recovery orchestration. It does not
claim the external Phase 8 exit gates or a production cutover.

## Authority and visibility

Raw remains the only irreplaceable callback fact. Canonical, factor, Parquet,
indexes, checkpoints and manifests are derived artifacts. A Parquet file is
query-visible only through one immutable, validated manifest generation. A
successful file rename by itself is not publication.

Permanent Canonical visibility additionally requires a sealed and healthy
Canonical generation plus a route-complete common-cut certificate. The local
publication wrapper binds every receipt to one caller-validated certificate
identity; it does not parse that upstream certificate, authenticate a
signature, or independently observe Phase 5 generation health. A later Phase
5 generation FATAL revokes the whole Canonical generation, including a
previously committed prefix; an open all-day V1 generation may therefore be
written only to staging. Phase 8 never interprets one sealed segment as a
generation certificate.

`L2Flow::production` now selects the independently authorized
`l2flow_production` live composition.  This local Python/Parquet package is not
wired into that composition and cannot stop, delay or take locks in ingress.
It is also distinct from the C++ process-local `InstrumentHistoryRuntimeV1`.

## Parquet rows and integrity

Canonical history stores the exact fixed Canonical record bytes and typed
header projections used for partitioning, ordering and lineage. Supporting
Snapshot, Tick, Quality and Control as opaque fixed records avoids inventing a
partial second schema in Python. Market is explicit; global Quality/Control
records are not silently assigned to Shanghai or Shenzhen. Partition bucket
selection uses a frozen domain-separated SHA-256 mapping, not Python's
process-randomized `hash()`.

Factor history pins factor ID/version, code/config/state-schema identities,
registry identity, as-of time, full stable input identity and a persistent
watermark-sidecar reference. A placeholder row has no mathematical value:
its value remains null/invalid and its status explicitly says that the output
is a passthrough placeholder. `FactorHistoryRow` does not contain a factor-group
field, so publication also requires an explicit caller-validated catalog
binding for the exact group, factor/version, code/config/state schema,
implementation status and numeric dtype. Its domain-separated membership hash
is persisted in the manifest entry. This is a content binding to the caller's
evidence identity, not a signature or independent registry lookup. The five
named factors are placeholders in the current Phase 7 producer/catalog; the
storage layer does not treat that list as a universal hard-coded registry. A
production publisher must construct this evidence from an authenticated factor
registry; the generic storage schema alone cannot prove implementation truth.

Rows have a deterministic total sort and a domain-separated logical-row hash.
Writers use real Parquet with ZSTD compression, write to a private temporary
file, fsync it, publish the immutable final name without replacement, fsync
the directory and read the final file back before returning metadata. Row
count, exact schema, exact decoded rows and logical hash are rechecked.

PyArrow is configured to write its standard per-column row-group statistics.
Those library statistics are not the design's complete typed metadata
contract: local V1 does not persist or validate explicit per-column pruning
summaries, instrument/type set hashes, or aggregated quality flags in either
its custom footer keys or `ArtifactEntry`. Consequently the local reader uses
exact row decode, total-order endpoints and the logical-row hash as authority;
production-grade metadata pruning and its evidence remain outside this slice.

The physical SHA-256 covers the complete final Parquet bytes and is stored in
the external manifest. It cannot be placed in the footer of the same file:
doing so would require the file hash to include itself. Logical content hashes
may be stored and checked independently.

## Watermark sidecars and lineage

`watermark_set_id` is unique only within a factor run. Every persistent
reference is therefore scoped by run ID and watermark-table generation. A
sidecar carries the complete sorted `FactorInputWatermarkSetV1` maps and the
stable input-identity hash. The hash includes exact Raw namespace, family,
shard, exclusive Canonical cursor, maximum consumed Raw WAL end, full clock
algorithm/digest and input quality. It excludes observed durable position and
the display-only clock label, while the sidecar still preserves both as
observations.

Lineage ranges remain scoped to an exact
`(origin_capture_date, source_stream_id, origin_stream_day_id)` namespace.
Naked minima or maxima from different Raw namespaces are never compared.
Output min/max alone is not proof of complete input coverage; publication and
rebuild require route coverage and a common-cut certificate.

Publication builds receipt indexes once. Canonical rows first select their
projected source route, then inspect only ingress candidates whose prefix-max
range can contain the row; arbitrary ingress/WAL rectangles mean the remaining
candidate scan is not claimed to be purely logarithmic. Factor watermark routes
must map to exactly one complete `SourceNamespace` before a non-overlapping
`(begin,end]` cursor range is selected by binary search. A sidecar reference and
its full-map coverage validation are cached for repeated rows. If and only if
both the exclusive Canonical cursor and consumed Raw WAL end are zero, the
unique namespace's receipts are retained solely as route identity and, for a
VISIBLE artifact, the caller-validated common-cut identity; the lineage result
has `zero_consumption=true` and no positive receipt. A one-zero/one-nonzero pair
is invalid. The structural hard caps bound accepted
input shape, but publication assembly has no query deadline/work budget and is
not a maximum-size operational latency promise.

## Manifest publication

Manifest and sidecar encodings are bounded, duplicate-key-free canonical JSON
envelopes with SHA-256. Decoders reject unknown fields, type confusion,
non-canonical encodings, unsafe paths and hash mismatches.

Manifest generations form an immutable hash chain. Publication holds the
store lease, compares the expected current generation/hash, creates the next
generation without replacement, fsyncs it and its directory, then atomically
replaces a small `CURRENT` pointer and fsyncs again. A stale writer fails the
compare-and-swap check. Final artifact paths are relative, normalized and
confined beneath the retained store directory. Store construction requires a
normalized absolute non-root path, walks every component with
`O_DIRECTORY|O_NOFOLLOW`, and pins the resulting directory descriptor and
device/inode identity for the object's lifetime; later path replacement cannot
redirect that object. Immutable publication requires
`renameat2(RENAME_NOREPLACE)`. An unavailable syscall/filesystem capability
fails closed: V1 deliberately has no crash-unsafe `link`-then-`unlink`
fallback. The pinned descriptor is an explicit close/context-manager resource;
lifecycle access is lock-coordinated, and store objects reject copying or
serialization so two Python objects cannot own the same raw descriptor.

The manifest store checks an artifact's exact size and whole-file SHA-256 but
deliberately treats its bytes as opaque. The publication assembler is the
bridge that first reopens a real Parquet file and validates its schema, footer
and rows before constructing an entry. Cold-start sidecar loading traverses
from the store dirfd without following symlinks and checks descriptor size,
hash, namespace, set count and canonical-codec round trip.

A successor is append-only and retains all earlier artifact and sidecar
references; neither this store nor the retention planner rotates or compacts a
manifest root. Operationally, one store must therefore be treated as a bounded
publication epoch. Before its artifact/manifest bounds are exhausted, an
external lifecycle must publish a replacement root, fence readers and writers,
atomically redirect the deployed route, and register both roots while old
readers drain. Only after the old root has no current-manifest, query, replay or
audit reference may its objects become retention candidates. This root switch
and retirement protocol is not implemented or claimed by the local package.

## Query semantics

A history query first fixes one manifest snapshot and fixed snapshots of its
hot inputs. Typed hot/current sources hash their frozen row content as well as
their frontier/generation evidence. The supported public cold-read path uses
an adapter that securely traverses the store path and invokes the real
descriptor-aware Parquet reader. Private constructors and module tokens are
only an in-process trusted-caller convention, not an unforgeable capability.
The query may then merge manifest-visible
Parquet, unpublished hot Canonical/factor rows, and optionally a current
Latest value. Latest State and Latest Factor are current-value sources only
and never masquerade as history.

Canonical and factor rows are deduplicated only by their complete frozen
idempotency keys. Equal keys with equal logical content collapse to one row;
equal keys with different content are corruption and fail the query. A
post-scan observer must still report the same fixed snapshot. The merge budget
cumulatively bounds source count, candidate rows, logical row bytes, returned
rows, lineage entries, deterministic lineage-accounting bytes and abstract
lineage work. Factor sidecar maps are fully validated while the fixed catalog
is built; query execution uses that immutable cache, resolves an exact route to
one complete namespace, then uses the namespace's non-overlapping cursor index.
It preflights entry/work capacity and reserves exact deterministic lineage
bytes before allocating nested coverage/resolution objects. Reused immutable
resolutions are cached, while every emitted replay observation and every
output-level lineage occurrence is still charged.

Canonical origin-ingress/WAL receipt rectangles can overlap across complete
namespaces. A prefix-max index narrows the search, but an adversarial route can
still require a linear candidate scan. Every inspected candidate consumes the
explicit lineage-work budget and checks the deadline before evidence is
constructed; the implementation therefore promises a hard work cutoff, not
`O(log R)` lookup for this two-dimensional case. Each public Parquet read
separately gates physical file size and footer-declared decoded bytes;
independent artifact reads do not share a PyArrow/process-memory budget. The
deadline is cooperatively checked and cannot preempt a blocking PyArrow call.
Canonical Raw results are exact locators plus receipt coverage, not proof that
Raw bytes were independently read. The local implementation is an in-process
primitive, not the
isolated/cgroup query service or the 24-hour ingress-SLO evidence required by
the design.

## Retention

Retention produces a deterministic immutable dry-run plan; it does not unlink
files. A Raw candidate is eligible only when all six design gates are present:

1. the applicable retention deadline has passed;
2. complete validated Canonical/Parquet publication covers it;
3. the complete durable-consumer registry has reached the segment's exclusive
   end cursor (`consumer_exclusive_next_cursor >= segment_end`) in the exact
   Raw WAL scope;
4. no replay, audit, query, manifest or watermark-sidecar reference is active;
5. the required backup policy is satisfied; and
6. the configured approval policy is satisfied.

The plan binds policy and inventory hashes and records every failed gate.
The six design conditions are represented by seven explicit checks because
watermark-sidecar reachability is separated from the general active-reference
check. Current-manifest ownership is an active reference, so a currently
reachable Parquet part, sidecar or manifest cannot be proposed. Publication
coverage evidence is bound to the candidate's exact namespace and half-open
cursor range; a bare `complete=true` assertion is insufficient. Approval and
other external evidence remain caller-authenticated inputs, not locally
verified signatures. `observed_at_ns` and each retention deadline must use the
same time basis defined by the bound retention policy; this planner does not
authenticate the host clock.

The planner canonicalizes each complete consumer/active/sidecar snapshot once,
binds its content hash into per-artifact gate evidence, and builds exact-scope
lookup indexes once. The shared-evidence work is therefore
`O(A + C + R + S)` plus `O(A log A)` inventory ordering, rather than rescanning
and re-encoding every complete snapshot for every artifact. It still must emit
seven gates per artifact and inspect each artifact's own bounded publication
and backup evidence; the structural maxima are hard rejection bounds, not a
claim that every maximum-size plan is an operationally sensible batch.
Because a dry-run snapshot cannot eliminate a race with a new reader, a later
destructive executor would still need a deletion lease/fence and immediate
revalidation. No such executor is part of this phase slice.

## Recovery and isolated rebuild

Recovery output is a fail-closed plan/state machine, not an implicit repair.
Ingress recovery is independent of downstream readiness. The repository-local
Canonical V1 recovery path preserves each Raw route's original writer/source
generation and clock identity, while creating fresh recovery ownership, a
fresh Canonical generation, normalizer, zero-progress frontier page and empty
all-day sinks. It then replays from the trading-day Raw start; it cannot resume
from an old processed cursor.

Latest State may use a durable, writer-quiesced checkpoint only with the exact
Phase 6 configuration (including state generation and writer instance) and an
external lease/fence that authorizes that identity. Its tail must cover every
family named by the checkpoint durability barriers: authoritative Snapshot,
and also the separate Tick-quality lineage whenever that family is present in
the state. Since V1 has no checkpoint-rebase API, a different output
configuration requires a full replay of every target family (Snapshot and,
when present, Tick-quality).
Factor recovery validates code/config/state-schema/registry identities and
the complete watermark map, then resumes each input at its exclusive cursor
through the native safe-mux barrier. `LIVE_LATEST` input is not a deterministic
tail-replay source because Phase 7 does not put it in the batch watermark or
generation tags; recovery therefore cannot substitute the current Latest
value for missing history. Missing coverage, a FATAL source, an incompatible
clock epoch, an unresolvable watermark, or a required `LIVE_LATEST` tail fails
closed rather than skipping ahead.

Schema upgrades and repairs build an isolated candidate generation. A cutover
certificate binds the candidate identity, previous generation, schemas,
registry, Raw routes/ranges, artifact hashes and common cut. The local code can
validate and emit a cutover plan/certificate; changing a deployed alias or
service route remains a separate reviewed and authorized operation.

## Evidence boundary

Local tests cover deterministic encoding, corruption rejection, atomic-store
fault points, real Parquet round trips, hot/cold duplicate handling, lineage,
retention gates and recovery/cutover validation. They do not provide a full
trading-day Raw-to-factor run, random historical-row production lineage,
24-hour query/compaction isolation on target hardware, a destructive-retention
race proof, or production service wiring. Those remain external Phase 8 exit
evidence.
