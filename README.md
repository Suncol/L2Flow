# L2Flow

L2Flow contains a DataYes C++ SDK-compatible Level-2 market-data mock and a
Linux production data path for four real market-data sources.  The production
path captures Raw WAL in source order, performs control/market decoding and
Canonical commit, then distributes owned decoded events to fixed instrument
workers for in-process history queries.

The mock deliberately separates two kinds of compatibility:

- **API and payload compatibility:** consumers continue to use
  `IOManager`, `Subscriber`, `MessageHandler`, `MDLMessage`, and the message
  structs from the supplied SDK.
- **Business semantics:** generated identifiers, prices, order flow, trades, and
  code-valued fields are synthetic. They are not exchange-certified data and
  must not be used to validate trading decisions or regulatory logic.

The mock public API is declared in
[`include/l2mock/l2_mock.h`](include/l2mock/l2_mock.h). The supplied DataYes SDK
version is `2.13.234` (`MDL_VERSION == 213234`).

## Legacy Phase 0–1 four-process Shadow ingress

The repository retains the first two ingress phases described in
[`docs/design.md`](docs/design.md):

- an immutable SDK-header archive baseline, bounded sealed shared-library
  snapshots, constrained ELF compatibility parser, compiled ABI probe, runtime
  wrong-version probe, and reproducible build manifest;
- four independent ingress executables, each owning exactly one SDK manager,
  subscriber, callback gate, preallocated SPSC byte ring, and temporary
  sequential shadow sink;
- bounded credential loading, dynamic loading from a sealed immutable SDK
  snapshot, `sd_notify`/watchdog support, metrics, subscription/readiness
  observation, ordered drain, and process fail-stop when opaque SDK callback
  convergence cannot be proved.

The four binaries are:

```text
mdl-ingress-sh-snapshot   required 4.4; optional 4.6
mdl-ingress-sh-tick       required 4.24
mdl-ingress-sz-snapshot   required 6.28; optional 6.29
mdl-ingress-sz-tick       required 6.33 and 6.36 on one Subscriber
```

These `mdl-ingress-*` programs still execute the legacy
`IngressApp -> ShadowCaptureWriter` path.  The formal four-source aggregate
entry is the separate `mdl-production-router` described below.

`6.53 CombinedTick` is explicitly forbidden as a core subscription. The
executables do not have a link-time dependency on `libmdl_api.so`; they open a
regular file with `O_NOFOLLOW|O_NONBLOCK`, enforce a 1 GiB pre-copy bound,
copy it into a sealed memfd, and run the ELF dependency/symbol-version,
compiled ABI, and runtime factory/lifecycle compatibility gate. Service startup
runs the full gate on sealed snapshot A. The loader then captures fresh
snapshot B, repeats the library component gate on B, and performs final
`dlopen` on B, which is retained for the SDK object lifetime. The SDK header
archive remains hash-pinned; compatible 2.13.234 library builds need not have
one identical shared-object hash or Build ID.

The original SDK archive is intentionally required for a full startup gate and
is not synthesized from the extracted directory. Its one-descriptor hash is
bounded by a 1 GiB operational cap before reads; the compiled SHA-256 remains
the identity. The compiled baseline JSON is exact-size captured and verified
from one sealed descriptor without reopening its path:

```bash
./build/mdl_abi_preflight \
  --baseline configs/vendor_baseline.json \
  --archive /approved/mdl_sdk_2_13_234.tar.gz \
  --library /approved/libmdl_api.so

./build/mdl-ingress-sz-tick --help

# Configure optional direct-link probes against a real local artifact when
# the checked-in large-file path is still a Git LFS pointer.
cmake -S . -B build \
  -DL2FLOW_VENDOR_LIBRARY_PATH=/path/to/libmdl_api.so
cmake --build build --target mdl_sdk_feeder_probe

# Exercise L2Flow's sealed loader and adapter against a cascade feeder.
./build/mdl-sdk-feeder-probe \
  --library /path/to/libmdl_api.so \
  --address 127.0.0.1:9112 \
  --timeout-seconds 15 \
  --market-timeout-seconds 60 \
  --minimum-market-messages 1 \
  --monitor-seconds 60 \
  --capture-csv /new/path/feeder-capture.csv
```

`mdl-sdk-feeder-probe` uses binary encoding, a fixed non-secret local client
label, and exactly the five required subscriptions (`4.101.4`, `4.101.24`,
`6.101.28`, `6.101.33`, `6.101.36`). It succeeds only after a `LogonResponse`
reports `MDLEC_OK` for every required subscription, the requested minimum
number of those market-data messages has reached the callback, and
`Shutdown`/reference release completes. The success JSON includes every
subscription status and up to 16 copied `MDLMessageHead` samples. When
`--monitor-seconds` is present, it observes the full duration before shutdown;
`--capture-csv` writes normalized business fields without overwriting an
existing file. Use `--minimum-market-messages 0` only for an explicit
control-plane-only check.
The in-memory capture defaults to 100,000 records and fails closed on
overflow.  Longer explicitly monitored windows can raise that bound with
`--maximum-captured-records` (at most 10,000,000); the JSON result reports the
selected capacity, observed records and dropped records so a truncated window
cannot be reported as complete evidence.
The probe does not accept a token on the command line. Port 9112 is the
deployed cascade publisher used by the current integration; override
`--address` when the feeder's `TCP_SERVER` publisher uses another port.

For an opt-in live Phase 2 capture-path test, build and run the dedicated
probe. Each invocation deliberately covers one of the four production stream
definitions so the real callback topology, source-stream identity, and
required-subscription set stay explicit:

```bash
cmake --build build --target mdl_phase2_live_probe

./build/mdl-phase2-live-probe \
  --library /path/to/libmdl_api.so \
  --output-dir /new/absolute/path/phase2-live-sh-snapshot \
  --ingress-kind sh-snapshot \
  --address 127.0.0.1:9112 \
  --capture-date 20260722 \
  --logon-timeout-seconds 15 \
  --monitor-seconds 60 \
  --minimum-market-messages-per-key 1
```

The output directory must not already exist. The probe drives the real SDK
callback through `CallbackHandler`, `ByteRing`, `RawCaptureWorker`, the POSIX
`RawWalWriter`, and clean shutdown. It then requires exact
callback/append/durable reconciliation, validates the sealed
`segment-00000001.raw` with the Raw reader, and validates
`durable.journal` plus that segment with the recovery analyzer. For `sz-tick`,
the minimum applies independently to both `6.101.33` and `6.101.36`; receiving
only one of them is not a data-plane pass. Use
`--minimum-market-messages-per-key 0` only for an explicit post-close
control-plane and Raw-plumbing check. A nonzero minimum during an active
trading session is required for real market-data evidence.

This probe is intentionally isolated from the production Raw namespace. It
does not provision the reserve coordinator, publish a `ProductionRoute`, or
exercise the four-source aggregate. Its success is therefore live
capture-path evidence, not formal production-router or full-exit evidence.

Tokens are accepted only from a named systemd credential or an explicit
root-owned `0400` file; there is no token command-line option. Endpoint
behavior is loaded atomically from `--endpoint-contract` and pinned by
`--endpoint-contract-sha256`; address, encoding, merge, MAC-auth, and server
selection cannot be overridden independently on the production CLI. The
strict version-1 JSON contains exactly:

```json
{
  "schema_version": 1,
  "ingress_kind": "sz-tick",
  "name": "reviewed-endpoint-name",
  "resolved_server_address": "REPLACE_WITH_REVIEWED_RESOLVED_ADDRESS",
  "message_encoding": 7,
  "merge_message": false,
  "send_mac_auth": false,
  "server_select": false
}
```

The expected SHA-256 is over the exact file bytes. `--metrics-path` selects an
absolute Prometheus textfile. Once per second the monitor renders and submits a
snapshot to a bounded latest-pending-wins worker; the worker performs the
atomic filesystem replacement, never the SDK callback or monitor thread.
Construction of the production worker acquires a persistent typed `0600`
sidecar lease derived from the case-folded target basename and retains both
its exclusive `flock` and the destination directory fd. Publications remain
relative to that dirfd even if the named parent is renamed or replaced, and a
second cooperating worker cannot lease the same logical target concurrently.
The default worker constructor throws before accepting submissions if the path
or lease is invalid, causing startup to fail closed. Shutdown submits one final
snapshot and drains the worker.

All production paths are absolute, have pre-existing parents, and use portable
ASCII basenames. Writable outputs may not alias protected inputs or one
another. Each service needs a separate private SDK-log directory containing a
non-symlink, single-link, exact-mode-`0444`
`.l2flow-sdk-log-directory-v1` file whose exact contents are
`l2flow-sdk-log-directory-v1\n`. Startup retains that directory fd and passes
`/proc/self/fd/N/<basename>` to the SDK while holding a nonblocking exclusive
`flock` on the marker; a second cooperating ingress cannot lease that
directory concurrently. Shadow and metrics writers reject a marked SDK-log
directory. The marker declares an operator trust boundary, so services that
could otherwise modify one another's files under the same UID also require
separate service UIDs or mount isolation.

Phase 1 shadow files are local, native-endian, temporary evidence. They are not
the portable checksummed Callback WAL from Phase 2. First-seen readiness checks
the fixed-body minimum and every dynamic string/list range in each required
core record, but does not claim business-semantic decoding. See
[`docs/decisions/phase01.md`](docs/decisions/phase01.md) and the current
[`local acceptance record`](docs/acceptance/phase01-local.md).

## Formal four-source production router

`L2Flow::production` now aliases `l2flow_production`.  The executable
`mdl-production-router` owns all four fixed sources in one process:

```text
four SDK callbacks -> four Raw WAL writers
  -> four source-order control/market/Canonical pipelines
  -> one fixed-shard InstrumentHistoryRuntimeV1
  -> one owner-liveness-bound ProductionRoute
```

Each source is decoded by one thread before instrument fan-out.  This is
required for the stateful Shanghai 4.24 decoder.  After Canonical commit,
`instrument_id % 16` selects an immutable logical shard; 1–16 physical workers
own those shards.  Per-source dense dispatch tickets allow workers to finish
out of order while queries expose only a continuously acknowledged source
prefix.  `Latest`, `Tail` and `RangeBySourceSequence` are scoped to one
instrument, source and snapshot/tick lane.  They do not claim a four-source
total order.

The in-memory value is the owned Phase-4 decoded event admitted only after its
Canonical bundle commits; it is not a copy of the Canonical record.  In
particular, Canonical-only SH phase attribution, sticky sequence-quality flags
and Canonical event IDs are not injected back into history.  Factors needing
those fields must consume or join the committed Canonical projection.

The V1 route is deliberately fresh-only.  It requires four exact
SCAFFOLDING registrations, next Raw sequence 1, genesis control state, empty
Canonical sinks and empty history.  It does not attach recovered or already
ACTIVE Raw streams, perform exchange-level replay, rotate Canonical capacity,
or run Latest State/factor/Parquet services.

The deployment entry reads only the hash-pinned fixed manifest
`production-v1.tsv` from a private deployment directory:

```bash
./build/mdl-production-router \
  --deployment-dir /absolute/private/deployment \
  --manifest-sha256 <64-lowercase-hex> \
  --check

./build/mdl-production-router \
  --deployment-dir /absolute/private/deployment \
  --manifest-sha256 <64-lowercase-hex>
```

For an opt-in in-memory callback-to-instrument canary, add
`--fast-plane-shadow`. It runs four source-order decoder workers and a private
multi-worker history without waiting for WAL/Canonical, while a Fast failure
remains isolated from the formal route. See
[`Realtime Fast Plane V1`](docs/decisions/realtime-fast-plane-v1.md) for its
provisional query contract, resource budget, live-test command and evidence
criteria.

Start from [`configs/production-v1.example.tsv`](configs/production-v1.example.tsv),
copy it to the deployment directory as `production-v1.tsv`, replace every
placeholder and every sample path, name, hash, identity, date, device,
generation, capacity and timeout, then set the file mode to `0600`.  The checked-in values are a
parser-valid syntax fixture, not production sizing recommendations.  Compute
the command-line pin from the final exact bytes with
`sha256sum production-v1.tsv`; the credential token belongs only in the
separate `0400` credential file.

The production manifest treats `sdk_library_path` as an explicit operator
authorization.  It has no SDK archive path, baseline path or expected SDK
library digest field. `--check` only requires that this path name an existing
regular file. Normal startup passes that exact path directly to `dlopen()` and
resolves `DllCreateIOManager`, because those operations are required to use the
library. It does not copy/snapshot or hash the file and does not run automated
archive, baseline, size, ELF/ABI, lifecycle or expected-digest approval gates.
After successful production composition, the mapping is intentionally retained
until process exit; the formal path does not invoke the vendor DSO's unload
finalizers through an in-process `dlclose()` transition.
The legacy SDK archive/library digest fields in Raw V1 are all-zero to state
that no such identity was computed; they are not acceptance pins.

The formal operator path creates one physical SDK `IOManager` and one physical
`Subscriber`. Four logical ingress facades first prove that their connection
settings agree; the fourth logical `Connect()` installs the union of the five
required market subscriptions and performs the only physical `Connect()`.
API/SYS control callbacks fan out to all four independent Raw handlers. Market
callbacks route by the exact service/version/message key to one owning lane,
and unknown or forbidden keys are consumed without entering any Raw stream.
Shutdown and vendor-object release each occur exactly once, after all four Raw
callback gates have closed; Raw WALs, sequences, clocks, frontiers and decode
pipelines remain independent.

`--check` validates the manifest and non-mutating deployment prerequisites; it
does not load vendor code, acquire SourceFrontier roles, register/mutate Raw
SCAFFOLDING, create Canonical files or publish a route.  Normal mode performs
the fresh composition and publishes Active only after all four control,
durability and history barriers pass.  Cross-process consumers must use
`ReadLiveProductionRouteV1At()` and retain/revalidate its owner guard; a static
Active manifest alone is not liveness evidence.

To close the SDK-log path/occupancy race, preflight temporarily takes the
nonblocking flock on each pre-existing, read-only SDK-log directory marker.
`--check` releases those four leases on exit; normal mode retains and moves the
same leases into the four source lifetimes without reopening their paths.

The check also requires the five fixed route/owner artifacts and every
deterministic target for this generation (four Raw stream directories, four
SourceFrontier files, and every Canonical segment, manifest and manifest
temporary) to be absent at that instant.  This is an early read-only
diagnostic, not an authority claim: each creator still uses its final
exclusive/identity gate, and route publication repeats its absence proof under
the process gate and directory lock immediately before creating owner evidence
and Active.

Canonical capacity is configured independently for the four families with
`canonical.snapshot_capacity_records_per_sink`,
`canonical.tick_capacity_records_per_sink`,
`canonical.quality_capacity_records_per_sink`, and
`canonical.control_capacity_records_per_sink`. Snapshot and tick each have 16
sharded sinks per source; quality and control each have one unsharded sink per
source. Keeping these limits separate is required both for load safety and for
bounded disk preallocation: increasing a high-rate unsharded quality limit
must not multiply the allocation of every 2 KiB snapshot segment. Every field
is a per-sink hard limit, and exhausting any one remains source Fatal.

`history.maximum_records_per_query` is a hard per-call work/output limit for
`Tail` and `RangeBySourceSequence`; an oversized request is rejected before it
takes a shard lock or allocates its result.  Full immutable chunks are pinned
under the lock and traversed after unlocking, so a large permitted query does
not hold the append worker's shard lock for its complete traversal.
`history.maximum_records_per_shard` is an append-time hard ceiling, not an
eager allocation request: each instrument/source/lane store grows lazily in
`history.chunk_record_capacity` chunks and remains independently bounded by
`history.maximum_payload_bytes_per_shard`.

The manifest's four `metrics_path` values currently participate in Raw stable
configuration hashing and path-separation checks only.  This aggregate entry
does not start a `MetricsWorker` or publish those textfiles yet.

The same aggregate entry treats `credential_name` as a validated stable
identity label; the actual SDK token is read from the separately pinned
`credential_path`.  `raw.ring_stall_budget_seconds`,
`raw.reserve_domain_id`, `raw.reserve_coordinator_socket`,
`raw.reserve_ack_timeout_milliseconds`, and `raw.emergency_reserve_bytes` are
also validated and committed to each source's stable Raw configuration hash,
but this entry does not start a socket reserve client or a separate ring-stall
timer from those fields.  Reserve authority is instead attached through the
already-provisioned coordinator rooted at the retained Raw directory.

The four `scaffolding_allocation_cap` and `safe_stop_template_id` values are
deployment identity assertions, not knobs which provision or resize the
reserve coordinator.  Normal startup requires the already-provisioned
coordinator's four fresh SCAFFOLDING entries to match them exactly, including
route, stream-day, writer and recovery-attempt identities.

Raw creation and runtime descendants are anchored to the same retained Raw
root directory fd used by the coordinator; the configured pathname is not
reopened.  Canonical sink creation and manifest sealing are likewise anchored
through the retained Canonical directory fd.  Immediately before fresh
owner/Active publication, the controller
reopens `route_root` and `canonical_root` and requires their owner, mode,
device and inode to match the retained directories.  Deployments must still
prevent later root replacement (normally by stable mounts/service-UID
isolation); a process cannot make an arbitrary pathname immutable.

`activation_timeout` starts only after all four capture `Start()` calls have
returned.  The vendor `Connect()`/Shutdown APIs have no cancellation or
completion bound, so a deployment requiring a hard deadline must use an
external supervisor and eventually SIGKILL.  A forced exit is not a clean
stop and requires a recovery/takeover workflow not implemented by this
fresh-only V1.

SIGINT/SIGTERM received during startup sets the service cancellation latch
immediately, before waiting for a possibly blocked `Connect()`.  If startup
does not first return an authoritative Active result, even a successful local
cleanup does not make the already-created fresh namespace reusable: the
process exits nonzero and requires reprovision or the separate recovery path.

Normal mode creates fresh frontier/Canonical/Raw generation artifacts.  A
failure after that mutation is fail-closed but is not rolled back or retried
in place; the operator must use a newly provisioned generation or the separate
recovery/takeover procedure.  For a fresh generation, prepare static files,
run typed coordinator provisioning, then run the full `--check`; startup is
last. The check requires the already-created coordinator lease marker and
exact four-entry SCAFFOLDING state.

The full ordering, capacity, route-liveness and real-feed test contract is in
[`docs/decisions/production-instrument-runtime-v1.md`](docs/decisions/production-instrument-runtime-v1.md).

## Phase 4 safe decoder and in-memory session slice

The repository now also exposes `L2Flow::phase4` for the five core market
messages.  This is a construction/library slice, not a production-alias
cutover.  It deliberately does no CSV or WAL I/O: an existing feeder supplies
one borrowed head/body view to `MarketSessionV1::Inject`, and the call decodes
and owns all published fields before returning.

The default retention policy keeps the complete configured trading-day
session, subject to the aggregate `max_records` and `max_payload_bytes` hard
admission limits across its four configured streams. Reaching either limit
rejects the new event and poisons the source whose admission failed; old
history is never silently overwritten. Four source histories remain separate
because their TCP deliveries do not define a truthful cross-stream total
order. Immutable snapshots can be read without holding the writer lock, and
retained ticks use an exact-type compact owner instead of reserving the largest
snapshot variant for every record.

This Phase 4 injection seam is independent of Phase 5. `ProcessMarket` does
not populate a `MarketSessionV1`; a feeder that wants both owned decoded
history and Canonical output must fan out each source's already ordered input
to `MarketSessionV1::Inject` and `CanonicalBundleCoordinatorV1::ProcessMarket`
under an explicit caller-owned failure policy.

That statement describes the standalone Phase 4/5 APIs.  The formal
production path does not decode twice or populate `MarketSessionV1`:
`ProductionSourcePipelineV1` decodes once in source order, commits Canonical,
then transfers the owned event to `InstrumentHistoryRuntimeV1`.

```bash
cmake -S . -B build -DL2FLOW_BUILD_TESTS=ON
cmake --build build --target l2flow_phase4
ctest --test-dir build -L phase4 --output-on-failure
```

The detailed injection, ordering, ownership, retention, validity, and
Phase-5-boundary rules are in
[`docs/decisions/phase4.md`](docs/decisions/phase4.md).  The scoped evidence and
remaining real-corpus/production gates are in
[`docs/acceptance/phase4-local.md`](docs/acceptance/phase4-local.md).

## Phase 5 canonical injection and committed memory view

`L2Flow::phase5` accepts caller-fed records that are already ordered and covered
by the feeder's append-visible Raw frontier. A `SourceFrontier` observation is
only an append high-water proof: it does not prove that an intermediate
`(ingress_sequence, WAL end)` pair is a real record boundary or that the bytes
passed to the normalizer are that record. The coordinator therefore requires
market and control envelope verifiers which authenticate the exact next Raw
record and the complete immutable `Process*` input against the caller's feeder
or validated Raw reader. Repeating the frontier comparison is not a valid
verifier. Phase 5 does not tail Raw itself or create another CSV writer, Raw
WAL, callback journal, or feeder checkpoint.
`CanonicalBundleCoordinatorV1::ProcessMarket` decodes a borrowed market view
into fixed Canonical records; `ProcessControl` puts an already decoded API/SYS
control record into the same normalizer-owned event-ID transaction. These APIs
do not implicitly append to Phase 4's independent session store. The caller
also supplies and retains the distinct fixed-capacity memory-mapped Canonical
segment sinks. The repository-local V1 coordinator attaches only at the day
boundary to a fresh normalizer, zero processed frontier and empty sinks; those
fixed capacities must cover the entire trading day. V1 has no in-place segment
rotation chain, normalizer checkpoint codec, or midday continuation inside the
same generation. Owned all-day decoded history, when required, is
provided separately by Phase 4's `SessionRetentionModeV1::kFullSession`, whose
aggregate record/logical-payload hard limits must be sized explicitly. Phase 5
sequence/phase state is also all-day. Neither logical retention nor an mmap
promises physical DRAM pinning.

Vendor sequence identity is exactly `(capture_date, source_stream_id,
stream_day_id, ServiceID, MessageID)`; service version, encoding, vendor local
time and body are duplicate evidence. SH and SZ exchange identities are
market-kind-specific `(trade_date, source_stream_id, channel)` scopes, with SZ
6.33 and 6.36 sharing one unified channel guard. Reconnect and subscription
epochs do not reset these scopes.

Cross-family output uses one bundle protocol: preflight every sink, publish all
records, independently verify the ordered receipt, commit normalizer state,
advance every configured segment (including those with no record for this Raw),
and update `SourceFrontier.processed_*` last.
Coordinator creation requires a complete route manifest: one Snapshot and Tick
sink for every configured shard, plus exactly one shard-0 Quality and Control
sink; missing, duplicate, or illegal routes fail before day-start processing.
Factor and mux readers use the committed-reader gate; a record that is
physically present in one mmap but not covered by the exact Raw
writer/generation processed prefix stays invisible. Any failure after the
first publication first latches SourceFrontier FATAL as the global revocation
anchor, then fail-stops the normalizer and every configured segment. The fatal
latches are sticky: committed readers and safe mux reject the entire Canonical
generation, including records that were committed before the fault, and a
fatal segment cannot be sealed for reuse. Required mux inputs borrow and
re-read the live frontier page; a saved healthy snapshot is not accepted as a
fresh authorization. Returned mmap views and mux selections are point-in-time
proofs, so consumers must generation-tag derived state and discard it on a
later live FATAL.
Recovery creates a new SourceFrontier page and new Canonical generation and
replays Raw from the trading-day start when the replay remains inside one Raw
writer/clock identity. Cross-writer or cross-clock full-day reconstruction
needs an external generation chain/state transition that this local V1 does
not implement. The Phase 5 library does not perform that recovery, rotation,
checkpointing, or generation switch automatically.

Each 4096-byte SourceFrontier page is initialized once for exactly one immutable
Raw writer instance/generation. Callback, append, processed and state mutations
carry the expected writer/generation fence; idle publication double-reads and
rechecks the same immutable identity. A stale owner is rejected, and once FATAL
is latched the page cannot return to a healthy state.

```bash
cmake -S . -B build -DL2FLOW_BUILD_TESTS=ON
cmake --build build --target l2flow_phase5
ctest --test-dir build -L phase5 --output-on-failure
```

The frozen schema, transaction boundaries and no-duplicate-persistence rule
are recorded in [`docs/decisions/phase5.md`](docs/decisions/phase5.md). Local
evidence and the still-external full-day live/replay, real crash matrix,
repeated mux hash, target-host performance and single-writer gates are in
[`docs/acceptance/phase5-local.md`](docs/acceptance/phase5-local.md). The
`L2Flow::production` alias now selects `l2flow_production`, which connects this
Canonical runtime to the fresh live path.  This independent deployment change
does not retroactively complete the Phase 5 formal exit; a local unit-test pass
is not full production qualification.

## Phase 6–7 Latest State and factor-runtime slices

`L2Flow::phase6` adds an opaque 4096-byte/64-aligned Latest State slot. All
concurrent slot words are accessed through `__atomic` helpers and a C ABI
seqlock; a complete validated Canonical Snapshot replaces the full payload in
one publication, while tick quality keeps separate lineage and never changes
snapshot quality. Registry/schema/generation identities are pinned, stale
thresholds are caller-supplied per phase, and single/batch/market local queries
read caller-owned mappings. The deterministic checkpoint codec restores only
into an exact all-zero target generation. A durable marker requires a
writer-quiesced common cut plus exact per-family Raw namespace barriers; both
are caller assertions, not cryptographic Raw receipts.

`L2Flow::phase7` adds the committed zero-copy Canonical batch reader, a
C-compatible batch/mux/snapshot-as-of API, stable multi-input watermark
identity, exact per-Raw-namespace durability checks, an opaque Latest Factor
slot and a process-local append-only watermark table. `Peek` does not advance
the exclusive-next cursor; `Commit` rechecks the live Phase 5 frontier so a
later generation FATAL denies commit.

The installable `python/l2flow_factor` package provides frozen FactorSpec
hashing, exact Tick/Snapshot NumPy layouts, a leased read-only `MdlBatchView`,
16-way instrument ownership, transactional plugin rollback/commit,
incremental windows and bounded atomic checkpoints. Python does not recreate
safe-mux logic: without a reviewed native adapter its mux seam fails closed.

The five first-batch names exist only as explicit
`PASSTHROUGH_PLACEHOLDER` implementations:

```text
book_imbalance
microprice
trade_imbalance
cancel_rate
trade_intensity
```

They return the exact original leased records while the batch context is
active and always expose `factor_value=None` with
`factor_value_valid=False`. They do not calculate or validate any factor
formula and therefore do not satisfy the Phase 7 mathematical or full-day
five-factor exit conditions.

```bash
cmake -S . -B build \
  -DL2MOCK_BUILD_TESTS=OFF \
  -DL2FLOW_BUILD_TESTS=ON
cmake --build build --target l2flow_phase6 l2flow_phase7
ctest --test-dir build -L 'phase6|phase7' --output-on-failure

PYTHONPATH=python python3 -m unittest discover \
  -s tests/python -p 'test_*.py' -v
```

The precise local contracts are in
[`docs/decisions/phase6.md`](docs/decisions/phase6.md) and
[`docs/decisions/phase7.md`](docs/decisions/phase7.md). Scoped evidence and
the unfulfilled external exit gates are recorded in
[`docs/acceptance/phase6-local.md`](docs/acceptance/phase6-local.md) and
[`docs/acceptance/phase7-local.md`](docs/acceptance/phase7-local.md).
Neither slice is wired into the formal production aggregate.  Its current
derived-data endpoint is process-local instrument history, not a Latest State
writer or Phase 7 factor executor.

## Phase 8 historical-storage slice

The optional `python/l2flow_history` package adds a repository-local Phase 8
storage and orchestration slice. It writes actual Apache Parquet through
PyArrow with ZSTD compression, preserves each complete Canonical V1 record as
exact bytes plus checked header projections, and stores float64 factor history
with explicit validity and implementation status. The five Phase 7 first-batch
names in the current producer/catalog remain passthrough placeholders: a
historical placeholder row cannot claim a valid mathematical value. The
history package does not hard-code those five names as a universal registry;
factor publication instead requires an explicit caller-validated group/catalog
binding and persists its content hash. That trust marker is not a signature or
independent catalog attestation; a production publisher must derive it from an
authenticated factor registry rather than accepting arbitrary self-assertion.
Publication indexes receipts once. A factor watermark route must resolve to one
complete SourceNamespace before its non-overlapping `(begin,end]` cursor range
is searched; repeated rows reuse one validated sidecar/coverage result. A
both-zero cursor/WAL input uses receipts from that unique namespace only as
route identity (and, when VISIBLE, as caller-validated common-cut identity); it
does not claim positive range coverage.

Final Parquet parts are immutable no-replace publications. Their complete-file
SHA-256 lives in the external manifest, not recursively in their own footer.
The manifest is a strict canonical-JSON hash chain with a flock-serialized
`CURRENT` compare-and-swap. The local publisher validates real Parquet bytes
and binds visible receipts to one caller-validated upstream common-cut
identity; it does not itself verify a Phase 5 health/route certificate or a
signature. Persistent factor lineage uses `(run_id,
watermark_table_generation, watermark_set_id)` plus the stable input identity
and a complete sidecar map.

The manifest store accepts only a normalized absolute non-root path, resolves
it without following any directory symlink, and pins that directory identity.
Immutable publication requires Linux `renameat2(RENAME_NOREPLACE)` and fails
closed when it is unavailable; there is no crash-unsafe link/unlink fallback.

This V1 manifest is append-only: a successor retains every prior artifact and
sidecar reference. The local package has no manifest-root rotation/compaction
or destructive executor. A deployment must therefore bound each store as one
publication epoch and perform any root switch, reader/writer fencing and old
root retirement through a separately reviewed external lifecycle; until an
old root has no manifest/query/replay/audit reference, retention rejects it.

The query primitives securely read manifest-described files, merge one fixed
cold snapshot with content-bound hot snapshots, and enforce cumulative
row/source/logical-byte/result plus lineage-entry/lineage-byte/lineage-work
budgets during merge. Factor lineage uses prevalidated sidecar maps, exact-route
indexes and exact-namespace cursor lookup; nested evidence is not allocated
until its entry and deterministic-byte reservation succeeds. Canonical
ingress/WAL rectangles may still require a candidate scan, so every inspected
candidate consumes the explicit work budget and checks the cooperative
deadline. This is a hard worst-case cutoff, not a claim of pure logarithmic
Canonical lookup. Each Parquet read also gates that one artifact's physical and
footer-declared decoded bytes; separate reads do not share a PyArrow
process-memory budget. Complete-key duplicate rows with conflicting stable
content fail closed. PyArrow writes ordinary row-group
statistics, but the local V1 contract neither persists nor validates the
design's typed pruning summaries, set hashes or quality aggregations; its
authoritative checks are the exact rows, sort endpoints and logical hash. The
retention component only emits a deterministic,
hashed dry-run plan after the six design conditions (seven explicit evidence
checks, including sidecar reachability); it does not delete data. Recovery and
schema-upgrade APIs similarly validate and emit plans/certificates for fresh
isolated generations. They do not repair Raw, switch a deployed route or
change the production alias.

Retention snapshots are canonicalized and indexed once per plan; exact-scope
consumer and reference checks are linear in total inventory plus evidence,
apart from inventory sorting and each artifact's own bounded evidence. The hard
caps remain rejection bounds, not recommended batch sizes.

Phase 8 tests are opt-in because PyArrow is an optional dependency:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e './python[history]'

cmake -S . -B build-phase8 \
  -DPython3_EXECUTABLE="$PWD/.venv/bin/python" \
  -DL2MOCK_BUILD_TESTS=OFF \
  -DL2FLOW_BUILD_TESTS=ON \
  -DL2FLOW_BUILD_PHASE8_PYTHON_TESTS=ON
ctest --test-dir build-phase8 -L phase8 --output-on-failure
```

The precise contract and local evidence are in
[`docs/decisions/phase8.md`](docs/decisions/phase8.md) and
[`docs/acceptance/phase8-local.md`](docs/acceptance/phase8-local.md). The
formal full-day lineage, 24-hour isolated query/compaction load and approved
retention dry-run gates remain external evidence.  This Python/Parquet history
package is distinct from, and not wired into, the C++ process-local
`InstrumentHistoryRuntimeV1`.

## Build and run

Compilation never connects to the network. The default test configuration
includes both the mock suite and the Phase 0–1 gate, so an absent, Git-LFS
pointer, malformed, or ABI-incompatible SDK artifact intentionally makes the
Phase 0 test fail. A different compatible 2.13.234 library build is accepted by
the component gate. To run only the self-contained mock tests, disable the
Phase 0–1 test suite; the mock uses `L2Flow::l2mock_standalone` and does not
require the vendor shared library:

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DL2FLOW_BUILD_TESTS=OFF
cmake --build build --parallel
ctest --test-dir build --output-on-failure

# Full default workload: 5,000 cash instruments, 320 derivatives,
# all supported streams, target 100,000 generated messages/second.
./build/l2_mock_demo --duration 10
```

The demo accepts `--rate 0` for an unpaced run and `--sh`, `--sz`,
`--futures`, and `--seed` for workload control. Run
`./build/l2_mock_demo --help` for the complete CLI.

## Scope

The default configuration creates a deterministic, full-market-scale workload:

- 2,500 synthetic Shanghai instruments;
- 2,500 synthetic Shenzhen instruments;
- 64 synthetic instruments per supported derivatives market when
  `include_derivatives` is enabled;
- snapshots, orders, and trades for the supported SDK message types;
- configurable callback concurrency, queue capacity, pacing, and
  backpressure.

“Full-market-scale” here means **a market-scale synthetic instrument count over
the 28 core streams returned by `mock::SupportedMessages()`**. It does not mean
that the generated instrument identifiers are a current exchange security
master or that all 109 structs in the seven SDK L2 headers are generated. See
[the exact SDK coverage](docs/sdk_coverage.md) and
[the design document](docs/mock_design.md) before selecting the mock for a
consumer.

## Drop-in integration

The mock replaces the SDK factory call. Subscription, callback, payload casting,
and shutdown continue to use the original SDK interfaces.

```cpp
#include "l2mock/l2_mock.h"
#include "mdl_shl2_msg.h"
#include "mdl_szl2_msg.h"

#include <chrono>
#include <cstdio>
#include <thread>

using namespace datayes::mdl;

class Handler final : public MessageHandler {
public:
    void OnMDLSHL2Message(const MDLMessage* message) override {
        const MDLMessageHead* head = message->GetHead();
        if (head->MessageID == mdl_shl2_msg::SHL2MarketData::MessageID) {
            const auto* body =
                reinterpret_cast<const mdl_shl2_msg::SHL2MarketData*>(
                    message->GetBody());
            std::printf("SH %s last=%.3f\n",
                        body->SecurityID.std_str().c_str(),
                        body->LastPrice.GetFloat());
        }
    }

    void OnMDLSZL2Message(const MDLMessage* message) override {
        const MDLMessageHead* head = message->GetHead();
        if (head->MessageID ==
            mdl_szl2_msg::Snapshot300111_v2::MessageID) {
            const auto* body =
                reinterpret_cast<const mdl_szl2_msg::Snapshot300111_v2*>(
                    message->GetBody());
            std::printf("SZ %s last=%.6f\n",
                        body->SecurityID.std_str().c_str(),
                        body->LastPrice.GetDouble());
        }
    }
};

int main() {
    mock::Config config;
    config.seed = 20260717;
    config.messages_per_second = 100000;
    config.callback_threads = 4;
    config.clock_mode = mock::ClockMode::Realtime;
    config.backpressure = mock::BackpressurePolicy::Block;

    const std::string validation_error = mock::ValidateConfig(config);
    if (!validation_error.empty()) {
        std::fprintf(stderr, "invalid mock config: %s\n",
                     validation_error.c_str());
        return 1;
    }

    Handler handler;  // Must outlive subscribers and the IOManager.
    IOManagerPtr io = mock::CreateIOManager(config);
    if (io.IsNull()) {
        std::fprintf(stderr, "cannot create mock IOManager\n");
        return 2;
    }

    SubscriberPtr subscriber =
        io->CreateSubscriber(&handler, /*multithread_callback=*/true);
    mock::SubscribeAll(subscriber.Get());

    const std::string connect_error = subscriber->Connect();
    if (!connect_error.empty()) {
        std::fprintf(stderr, "mock connect failed: %s\n",
                     connect_error.c_str());
        io->Shutdown();
        return 3;
    }

    std::this_thread::sleep_for(std::chrono::seconds(10));

    const mock::Statistics stats = mock::GetStatistics(io.Get());
    std::printf("generated=%llu delivered=%llu dropped=%llu\n",
                static_cast<unsigned long long>(stats.generated),
                static_cast<unsigned long long>(stats.delivered),
                static_cast<unsigned long long>(stats.dropped));

    io->Shutdown();
    return 0;
}
```

No server address, token, network connection, or market-data permission is
needed in mock mode. `Subscriber::Connect()` activates that subscriber; call
`IOManager::Shutdown()` before destroying the callback handler.

`SubscriberPtr::Reset()` is only a reference-count operation in the vendor API;
there is no per-subscriber `Disconnect()` method. It is not a callback
synchronization boundary. Use `AutoSubscriber::Stop()` for an auto subscriber,
or `IOManager::Shutdown()` for ordinary subscribers, before their handlers go
out of scope. An owner-thread shutdown waits for in-flight callbacks.

For a focused stream, replace `mock::SubscribeAll()` with the normal SDK
template API:

```cpp
subscriber->SubcribeMessage<mdl_shl2_msg::SHL2MarketData>();
subscriber->SubcribeMessage<mdl_shl2_msg::SHL2Transaction2>();
subscriber->SubcribeMessage<mdl_shl2_msg::Order>();
subscriber->SubcribeMessage<mdl_szl2_msg::Snapshot300111_v2>();
subscriber->SubcribeMessage<mdl_szl2_msg::Order300192_v2>();
subscriber->SubcribeMessage<mdl_szl2_msg::Transaction300191_v2>();
```

`mock::SupportedMessages()` is the authoritative runtime list of accepted
SID/version/MID combinations. Do not infer support merely because a message
struct exists in the vendor SDK.

## Switching between real and mock feeds

Keep feed construction at one boundary in the application:

```cpp
IOManagerPtr CreateMarketDataIO(bool use_mock,
                                const mock::Config& mock_config) {
    if (use_mock) {
        return mock::CreateIOManager(mock_config);
    }
    return datayes::mdl::CreateIOManager(/*work_threads=*/4);
}
```

The real-feed branch still needs its normal server address, token, encoding, and
subscriptions. The mock branch does not contact the network. Downstream
handlers should not need a mock-specific payload type.

There are two intentionally different CMake linkage targets:

- A mock-only process links `L2Flow::l2mock_standalone`. This target includes
  local fallbacks for the non-inline refcount and encoding helpers declared by
  the SDK headers, so `libmdl_api.so` is not required.
- A process containing the real/mock factory switch above links
  `L2Flow::l2mock` **and** the real `libmdl_api.so`. The core target deliberately
  contains no fallback `DllConvert*` or `DllInterlocked*` definitions, so the
  vendor's real ANSI/UTF-8 conversion cannot be preempted by the mock.

Do not link `L2Flow::l2mock_standalone` into a process that also loads the
vendor library. A minimal mixed-feed CMake boundary is:

```cmake
target_link_libraries(market_data_app
    PRIVATE
        L2Flow::l2mock
        /path/to/libmdl_api.so)
```

## Configuration

Important defaults:

| Field | Default | Meaning |
| --- | ---: | --- |
| `seed` | `0x4c32464c4f57` | Random stream seed |
| `messages_per_second` | `100000` | Aggregate target rate; `0` means unpaced |
| `shanghai_instruments` | `2500` | Synthetic count when no custom universe is supplied |
| `shenzhen_instruments` | `2500` | Synthetic count when no custom universe is supplied |
| `derivatives_per_market` | `64` | Synthetic count for each derivatives service |
| `include_derivatives` | `true` | Include synthetic derivatives services |
| `book_depth` | `10` | Generated price levels |
| `orders_per_level` | `4` | Per-level order-queue entries where the SDK type has them |
| `callback_threads` | `4` | Worker threads available for callbacks |
| `callback_queue_capacity` | `65536` | Bounded callback queue size |
| `backpressure` | `Block` | Block generation or drop the newest full-queue callback |
| `clock_mode` | `Realtime` | Wall-clock-derived or simulated event time |
| `max_realtime_lag_ms` | `5` | Maximum synthetic lag in realtime mode |
| `simulated_start_date` | `20260105` | Deterministic Gregorian weekday (`yyyymmdd`) used in simulated mode |

Call `mock::ValidateConfig()` before creating the manager. It performs no
filesystem or network access.

For repeatable callback order as well as repeatable generated values, keep
`simulated_start_date`, the universe, and the seed fixed, use
`ClockMode::SimulatedTradingDay`, `callback_threads = 1`, and
`BackpressurePolicy::Block`.
Multi-threaded callbacks can be scheduled in different orders even when the
generated sequence is deterministic.

## Strict and sanitizer builds

Production Phase 0–1 sources compile as C++20 with `-Wall -Wextra -Wpedantic
-Wconversion -Wshadow -Werror`. The mock remains C++17 compatible.

```bash
# ASan + UBSan
cmake -S . -B build-asan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DL2FLOW_ENABLE_ASAN_UBSAN=ON
cmake --build build-asan --parallel
ASAN_OPTIONS=detect_leaks=0 \
  ctest --test-dir build-asan --output-on-failure

# TSan is a separate, mutually exclusive configuration
cmake -S . -B build-tsan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DL2FLOW_ENABLE_TSAN=ON
cmake --build build-tsan --parallel
ctest --test-dir build-tsan --output-on-failure
```

`detect_leaks=0` is only needed in ptrace-managed/container environments where
LeakSanitizer cannot operate; it does not disable AddressSanitizer or UBSan.

## Authoritative universe

The default universe is intentionally synthetic. Applications that need their
real security master must supply it explicitly:

```cpp
mock::Config config;
config.instruments.clear();

for (const SecurityMasterRow& row : dated_security_master) {
    mock::Instrument instrument;
    instrument.service_id = row.mdl_service_id;
    instrument.security_id = row.security_id;
    instrument.reference_price_milli = row.reference_price_milli;
    instrument.tick_size_milli = row.tick_size_milli;
    instrument.lot_size = row.lot_size;
    instrument.option = row.is_option;
    config.instruments.push_back(instrument);
}
```

When `Config::instruments` is non-empty, it replaces the entire synthetic
universe. The caller remains responsible for the source date, listing status,
venue mapping, tick size, lot size, and option classification.

Prices in `Instrument` use integer thousandths: `12.345` is stored as `12345`,
and a `0.010` tick is stored as `10`. Payload builders convert that value to the
decimal placement declared by each SDK field.

## Backpressure and statistics

- `Block` is lossless at the mock queue boundary and intentionally lets a slow
  callback reduce generator throughput. If a callback re-enters
  `AsyncPublish()` while the bounded queue is already full, that newest work is
  rejected instead of blocking a worker on its own queue.
- `DropNewest` keeps the generator moving when the queue is full and increments
  `Statistics::dropped`.
- `generated`, `delivered`, `filtered`, `dropped`, `callback_errors`,
  `queue_high_watermark`, and `active_subscribers` allow a test to distinguish
  generator throughput from consumer throughput.

Callbacks may run concurrently. A handler passed with
`multithread_callback=true` must protect its own mutable state and should not
allow exceptions to cross the SDK callback boundary.

For `multithread_callback=false`, callbacks to that subscriber are serialized,
including callbacks caused by `SyncPublish()`. The serialization gate is shared
by all such subscribers attached to one mock manager, which also makes nested
synchronous publication between them safe. `AsyncPublish()` and
`AsyncResponse()` always enqueue callback work, independently of this flag.
Calling shutdown/stop from inside a callback requests teardown without waiting
on the current callback; make a second `IOManager::Shutdown()` call from the
owner thread before destroying handlers.

Statistics use two different units: `generated` and `filtered` count source
events, while `delivered` and `dropped` count subscriber deliveries. With two
subscribers interested in the same event, one generated event can therefore
produce two deliveries. Stop and shutdown may cancel callback work that was
already queued, so no accounting identity should be assumed while subscription
state is changing.

Field-value filtering uses `SecurityID` for SH/SZ and `InstruID` for the five
derivatives services, with `*` and `?` wildcards. A field name belonging to a
different message family, or any other field name, is retained in the
SDK-compatible subscription description but does not match messages.

## Data-safety boundary

The mock is suitable for:

- parser and callback integration tests;
- throughput, queue, backpressure, and concurrency tests;
- order-book data-structure stress;
- deterministic regression scenarios.

It does not claim:

- a current list of exchange-listed instruments;
- official trading calendars, night sessions, auctions, suspensions, or price
  limits;
- authoritative meanings for exchange business-code fields;
- exchange matching-engine, cancellation, or order-priority behavior;
- realistic price discovery, liquidity, or statistical distributions;
- production market-data entitlements, accuracy, or latency.

See [`docs/mock_design.md`](docs/mock_design.md) before using generated fields
in assertions.
