# Parallel decoder 500k/s optimization and validation report

## Technical summary

This branch implements an opt-in, bounded multi-core stateless decode farm and
keeps `parallel_decoder_worker_count=0` as the production default. The measured
result is a **conditional throughput success, not a universal 500k/s
certification**:

- With four parse workers, four History workers, the `fast` sink, 256
  instruments per market, and all traffic concentrated on the shared Shenzhen
  order/transaction source, a farm-only 10-second target profile offered
  5,000,000 callbacks at 499,999.894 callbacks/s. All 5,000,000 records were
  accepted, parsed, ordered, decoded, applied, and stored; no record was
  rejected or discarded, no explicit queue-full occurred, and the process
  stopped cleanly. Offer-end backlog was 169 records.
- In adaptive mode, the complete W0/W4 matrix passed 59/60 one-second trials:
  W0 passed 30/30 and W4 passed 29/30 across the five requested rates (50k,
  100k, 200k, 300k, and 500k/s) and two distributions. Every W4 trial through
  300k/s passed;
  five-tuple 500k/s passed 3/3 and hot-source 500k/s passed 2/3. The failed
  hot-source trial accepted and eventually drained all 500,000 records without
  queue-full or fatal state, but ended offering with an 8,166-record backlog
  and failed the target/steady-state budgets. A separate four-source-balanced
  500k/s profile passed 3/3.
- This does **not** extend to an arbitrary one-key hotspot. A single-instrument
  profile passed 3/3 at 50k, 100k, and 200k/s, and 0/3 at both 300k and
  500k/s. All six 300k/500k trials kept the OS process alive but entered
  explicit system failure: the three 300k trials saturated the serialized
  instrument-owner downstream path without decoder full, while all three
  500k trials also filled the 65,536-record decoder queue.
- The 500k/s run exercised substantial bounded pressure. The 10-second soak
  sampled completion depth at 128 of 128 slots and recorded 3,027 lease
  waits. Its decoder queue high water was 4,777 of 65,536. This is evidence of
  operation without failure, not proof of spare capacity.
- The literal 0.0 ms callback-to-Polars statistical non-regression gate did
  not pass. In the authoritative default-W0 branch A/B, Batch-history first-
  callback median changed by -5.641 ms, while complete-order first-event
  median changed by -1.416 ms. In the W4 branch A/B those deltas were
  -1.669 ms and -0.494 ms respectively. Despite favorable median point
  estimates, at least one required one-sided 95% bootstrap upper bound remained
  positive in seven of the eight W0/W4 metric gates. This report therefore does
  not claim statistically proven zero latency regression, and multi-core mode
  remains explicit opt-in.

The decision supported by the evidence is: **use W4 only as a candidate for a
distributed hot-source workload after a clean-host deployment qualification;
do not enable it as a universal default, and do not use it as the remedy for a
single-instrument owner hotspot.**

## Exact callback-to-Polars latency

### Metric definitions

All latency values below use `CLOCK_MONOTONIC` and are reported in
milliseconds. “Polars ready” means the Python probe has materialized and
validated an eager Polars DataFrame, not merely received a C++ notification.

- **Batch history, first callback**: Polars-ready timestamp minus callback
  entry for the first of 4,096 raw history records. The DataFrame has 55
  columns.
- **Batch history, last callback**: Polars-ready timestamp minus callback entry
  for record 4,096. This separates record-ingestion span from downstream
  History/IPC/native-reader/Python/Polars work.
- **Complete order sequence, first event**: Polars-ready timestamp minus the
  first raw callback contributing to the filtered five-event order sequence.
  This is the second of four injected raw callbacks; the preceding status
  callback is excluded. The probe validates final revision 3 and zero
  remaining quantity; the derived DataFrame contains six rows in total, one
  of which is outside that filtered five-event sequence.
- **Complete order sequence, last event**: Polars-ready timestamp minus the
  final raw callback contributing to that complete lifecycle.

The primary branch-level comparison used 30 alternating paired runs of the
pre-optimization Release binary at W0 versus this branch's Release binary at
its W0 default. The candidate farm was therefore not active during the
low-load latency measurement.

| Required latency metric | Old branch median (ms) | New branch median (ms) | Old p95 (ms) | New p95 (ms) | Median delta (ms) | p95 delta (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Batch history: first callback → Polars | 57.190018 | **51.548958** | 63.665937 | **59.624953** | -5.641060 | -4.040984 |
| Batch history: last callback → Polars | 50.133151 | **45.258825** | 56.181961 | **53.675917** | -4.874327 | -2.506044 |
| Complete order sequence: first event → Polars | 19.982890 | **18.566510** | 24.033209 | **21.262296** | -1.416380 | -2.770913 |
| Complete order sequence: last event → Polars | 19.979790 | **18.564195** | 24.029483 | **21.258142** | -1.415595 | -2.771341 |

The bold values are the precise default-W0 new-branch latency report requested
by the user. The full-batch latency is the first-callback row; the latency to
obtain the complete five-event order sequence is the first-event row. Values
are shown to six decimal places; the source JSON retains the original decimal
values derived from nanosecond timestamps.

The zero-margin gate requires the one-sided 95% bootstrap upper confidence
bound to be no greater than 0.0 ms for both median and p95. It failed for all
but one of the four required W0 metrics:

| Metric | Median-delta 95% upper bound (ms) | p95-delta 95% upper bound (ms) |
| --- | ---: | ---: |
| Batch history, first callback | -0.647579 | -1.630280 |
| Batch history, last callback | +0.784289 | -0.257532 |
| Complete order, first event | +0.086725 | -0.364940 |
| Complete order, last event | +0.098579 | -0.407024 |

The optimized W4 configuration was also compared directly with the old W0
branch in a separate 30-pair interleaved run. Telemetry confirmed that the
low-load workload remained inline (`farm_messages=0`, zero active parse
workers), which is the intended adaptive routing behavior:

| Required latency metric | Old W0 median (ms) | New W4 median (ms) | Old W0 p95 (ms) | New W4 p95 (ms) | Median delta (ms) | p95 delta (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Batch history: first callback → Polars | 57.477078 | **55.808020** | 64.349573 | **66.356325** | -1.669058 | +2.006752 |
| Batch history: last callback → Polars | 50.381952 | **49.727714** | 57.796252 | **60.024564** | -0.654238 | +2.228312 |
| Complete order sequence: first event → Polars | 19.391652 | **18.897914** | 22.251644 | **23.484813** | -0.493739 | +1.233169 |
| Complete order sequence: last event → Polars | 19.388492 | **18.895824** | 22.248992 | **23.480457** | -0.492669 | +1.231465 |

For W4, the median-delta upper bounds were +0.692887, +2.035097,
+0.594907, and +0.597017 ms in table order; the p95-delta upper bounds were
+7.758638, +5.898432, +3.942713, and +3.942635 ms. Thus all four zero-margin
gates failed despite favorable median point estimates. The current shared
host had confidence intervals too wide to establish a literal
zero-margin guarantee; these data cannot distinguish shared-host variability
from a small real regression.

These are observational performance results, not a mathematical latency
guarantee. The safe operational consequence is to retain W0 by default and
repeat the same gate on an isolated deployment host before enabling W4.

## Throughput and failure results

### Adaptive W4 passed through 300k/s; 500k/s remained distribution-sensitive

Each row below contains three fresh-process, one-second trials per workload.
The callback contract was serialized; decoder queue capacity was 65,536 per
source, History queue capacity 32,768 per source×worker, and Store workers were
fixed at four. `fast` means the FAST applied sink only.

| W4 target rate | Five-tuple uniform | Hot Shenzhen source | Offered-rate range across both workloads (callbacks/s) | Max offer-end backlog | Explicit full/fatal |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 50,000/s | 3/3 PASS | 3/3 PASS | 50,000.453–50,000.639 | 2 | 0 / 0 |
| 100,000/s | 3/3 PASS | 3/3 PASS | 100,000.260–100,000.377 | 2 | 0 / 0 |
| 200,000/s | 3/3 PASS | 3/3 PASS | 199,999.913–200,000.186 | 5 | 0 / 0 |
| 300,000/s | 3/3 PASS | 3/3 PASS | 299,999.323–299,999.920 | 12 | 0 / 0 |
| 500,000/s | 3/3 PASS | 2/3 PASS | 499,997.157–499,999.049 | 8,166 | 0 / 0 |

The five-tuple and most lower-pressure trials stayed inline because each
source owner kept its own FIFO below the source-local activation threshold.
Across the three adaptive hot-source 500k trials, 99,434 records used the farm,
all in the failed run. Two all-inline runs passed. The third accepted and
eventually stored all 500,000 records without fatal/full/reject/discard, but
had 8,166 records outstanding when offering ended, above both the executable's
1,024-record steady-state budget and the runner's 4,096-record budget. It is
therefore a real capacity-gate failure, not a crash or data-loss result.

The separate four-source-balanced 500k/s profile passed 3/3, with all records
remaining on the four inline source owners. This result demonstrates that the
source-local adaptive policy stayed inline; it is not callback-to-Polars
latency evidence and did not use the shared parse farm.

### Farm-only W4 sustained the distributed hot-source target for 10 seconds

The farm-only probe is the direct evidence that multiple CPU cores performed
decode work. It disabled idle-inline routing and required a positive farm
sample.

| Field | Result |
| --- | ---: |
| Target schedule | 500,000 callbacks/s for 10 s |
| Planned / invoked callbacks | 5,000,000 / 5,000,000 |
| Measured offered rate | 499,999.894 callbacks/s |
| Accepted / parsed / completed / committed | 5,000,000 each |
| Applied / Store-appended | 5,000,000 each |
| Per-worker parse counts | 1,250,000 / 1,250,000 / 1,250,000 / 1,250,000 |
| Rejected / discarded / completion-publish failures | 0 / 0 / 0 |
| Decoder full count / final fatal | 0 / 0 |
| Offer-end backlog / post-offer drain | 169 records / 2.264 ms |
| Decoder queue high water | 4,777 of 65,536 |
| Sampled completion high water | 128 of 128 |
| Issue high-water telemetry | 32 (conservative shard-bound sum) |
| Lease waits | 3,027 |
| History multi-record batches | 1,071,710 calls, 3,254,311 records, max 16 |

The benchmark classified capacity as `PRESSURE_OBSERVED_NO_FAILURE`. The issue
high-water field is a conservative upper bound assembled from four shard
bounds that need not be simultaneous; completion high water is periodically
sampled and is therefore a lower bound on the true maximum. Neither proves
unused headroom.

### A single instrument remains a serialized instrument-owner bottleneck

| Target rate | PASS count | Max offer-end backlog | Process survived | Observed failure mode |
| ---: | ---: | ---: | ---: | --- |
| 50,000/s | 3/3 | 1 | 3/3 | None |
| 100,000/s | 3/3 | 2 | 3/3 | None |
| 200,000/s | 3/3 | 14 | 3/3 | None |
| 300,000/s | 0/3 | 53,631 | 3/3 | Three serialized downstream fatal failures; no decoder full |
| 500,000/s | 0/3 | 84,369 | 3/3 | Three decoder-queue-full failures |

All six 300k/500k OS processes survived and reported explicit fatal state
rather than crashing or silently claiming a complete prefix. The 300k logs
show History submission queue-full and latest-projection failure around the
same saturated single-owner downstream; the experiment does not establish
which substage is the independent root bottleneck. This means
“500k/s” cannot be stated without a traffic-distribution qualifier. Parallel
wire decoding cannot parallelize the complete unique instrument-owner path
(Store append, latest projection, and applied sink) for one instrument; these
tests do not isolate one substage as the sole bottleneck.

### CERTIFIED output is not yet qualified at 500k/s

With `fast_certified`, neither distribution reached a 500k/s PASS in this
six-trial rerun. The three five-tuple trials formed clean complete prefixes but
offered only 343,521–349,390 callbacks/s. The three hot-source trials offered
333,868–455,618 callbacks/s and each froze one CERTIFIED channel; none had a
decoder full or Pipeline fatal, and all OS processes survived. This 0/6 result
is insufficient for production certification of the additional CERTIFIED
path.

## What changed in the code

### Stateless parsing now uses bounded multi-core ownership

- `MarketDecoderV1` is split into concurrently callable `DecodeStateless` and
  source-ordered `FinalizeInSourceOrder`; the legacy `Decode` remains a fused
  ordered operation.
- A positive worker count preallocates `(source, worker)` task leases and SPSC
  issue shards. Task assignment is deterministic:
  `(source_sequence - 1 + source_slot) % worker_count`.
- One completion/failure ring per source restores exact source order. For farm
  tasks, only the corresponding ordered committer mutates phase history or
  submits to History; inline intervals use the fused source-owner path.
- Parse workers apply immutable daily identity and merge market notices after
  stateless decoding, then release the copied callback body before completion
  publication and ordered commit.
- Farm threads are created lazily. With the production default W0, no parse
  worker or ordered-committer thread exists.

### The low-load path remains adaptive and source-local

- A source owner performs the original inline full-decode path while its own
  Pop-observed remaining ring occupancy remains below the effective threshold
  `min(8192, Q - max(1, Q/4))`.
- The observation reuses the tail acquire already required by every queue Pop,
  so there is no second cross-core depth read or periodic blind interval. At
  most one counted cell is a generation fence, which can only activate the
  farm one record early. A configured or effective threshold of zero selects
  the farm on the first record. This is not a sustained-duration detector.
- Multi-source aggregate pressure does not activate the shared farm because
  the four owners already decode independently on four cores.
- A source returns from farm to inline only after every issued task has retired
  and source-local pressure has subsided.

### Ordered History submission removes shared per-record fixed cost

- A committer greedily collects at most 16 completion records that are already
  contiguous; it never sleeps or waits to fill a preferred batch size.
- Each event is finalized and enqueued immediately. The first record routed to
  each History worker is signaled immediately, retaining the scalar path's
  no-batch-fill-wait signal semantics; redundant later signals are coalesced
  until `Finish()`. This does not by itself prove an end-to-end latency bound.
- The active History submission gate is finished before the final task is
  retired. This prevents an inline owner or generation fence from overlapping
  non-atomic source-owner state.
- One ready record continues to use the original scalar History path.

### Concurrency defects identified during stress testing were addressed

- Stale wake epochs could return from `atomic::wait` while leaving a waiter
  marked armed, causing `std::terminate` on the next wait. Lease, worker,
  completion, and commit-progress waiters now disarm after every return.
- A worker could observe stop after an earlier empty scan and miss the final
  issued task. Workers now perform a full post-stop rescan before exit.
- Torn issued/retired snapshots could falsely report no outstanding work. The
  single-writer frontiers are sampled in a happens-before-safe order and fail
  closed on an impossible tuple.
- Completion publication failure could leave an unretirable source-sequence
  hole. An exact-sequence failure tombstone ring lets the sole ordered
  committer retire every issued lease.
- Failure retirement now uses the authoritative issue-queue source, preventing
  a corrupted task scalar from publishing a tombstone into the wrong lane.
- A capacity-waiting external ingress now revalidates stop/fatal state under
  the admission mutex before every retry. A separate external-owner mutex
  preserves one sequence authority across the deliberate unlock/sleep gap.
  This closes stop-versus-publication and duplicate-sequence races without
  adding an atomic RMW to the ordinary SDK callback path.

### Pool and telemetry changes are bounded

- The callback-owned message pool has an explicit serialized-acquirer mode,
  matching the callback admission mutex while retaining concurrent recycle.
- Snapshots expose inline/farm/parsed/completed/committed/discarded counts,
  lease waits, per-shard issue pressure, sampled completion pressure, reorder
  wait, and History batch activity.
- The benchmark runner treats imperfect high-water telemetry as
  `HEADROOM_UNPROVEN` or `PRESSURE_OBSERVED_NO_FAILURE`; it never converts a
  sampled value below capacity into a headroom claim.

## Methodology and validation

### Environment

- Branch: `perf/parallel-decoder-500k-v1`
- Base commit: `3bc4f4108786e6df7e37f585fb294c258a02f367`
- Measured Release binary SHA-256:
  `00df775758077f05d248d76e06f4b3bd56f8d69c132ec83899b4a8581cd5ac85`
- Compiler: GCC 13.4.0, C++20 Release
- Host: two AMD EPYC 9354 sockets, 64 physical / 128 logical CPUs, two NUMA
  nodes, Linux 6.8.0-124-generic
- Python: CPython 3.10.12; Polars 1.32.3
- Throughput CPU set:
  `33-37,39,41,44-46,49,52-53,56-57,59,61`
- Latency CPU set:
  `33-34,37-39,41-45,50-51,53-54,57-58,60,62`

The host was not isolated. Representative system samples showed load average
between roughly 65 and 92, with earlier samples at 32–35% aggregate I/O wait.
CPU affinity reduces migration but does not remove shared storage, memory,
interrupt, or sibling-core contention.

### Machine-readable evidence

- [Default-W0 callback-to-Polars A/B](../artifacts/final3_callback_polars_branch_ab_default_w0_batch16/callback_polars_ab.json)
- [Optimized-W4 callback-to-Polars A/B](../artifacts/final3_callback_polars_branch_ab_workers4_batch16/callback_polars_ab.json)
- [W0/W4 adaptive rate matrix](../artifacts/final3_rate_matrix_w0_w4_adaptive/throughput_matrix.json)
- [W4 forced-farm 500k/s 10-second soak](../artifacts/final3_hot_w4_batch16_500k_10s_soak/throughput_matrix.json)
- [W4 single-instrument rate matrix](../artifacts/final3_single_instrument_rate_matrix_w4_adaptive/throughput_matrix.json)
- [W4 balanced 500k/s trials](../artifacts/final3_balanced_500k_w4_adaptive/throughput_matrix.json)
- [W4 FAST+CERTIFIED 500k/s trials](../artifacts/final3_certified_500k_w4_adaptive/throughput_matrix.json)

### Throughput PASS definition

A trial PASS requires all of the following:

1. the synthetic serialized callback schedule achieves at least 98% of its
   requested target;
2. no process timeout/crash, explicit fatal, rejected message, source decoder
   full event, Store append failure, completion publish failure, parse failure,
   or certified drop/freeze occurs;
3. after bounded drain, planned = invoked = accepted = decoded = applied =
   stored and the complete prefix is cleanly stopped;
4. offer-end backlog and second-half backlog growth stay within declared
   budgets.

This is the report-level summary; the benchmark harness is the authoritative
field-by-field classifier and additionally validates result cardinality,
configuration, affinity, topology, worker conservation, publication/discard
counters, and telemetry ranges.

Consequently, a “500k target-profile PASS” is not a claim that the measured
wall-clock rate was at least exactly 500,000.000/s, that queues had exact spare
capacity, or that a real vendor network/DSO/full-session/Python-reader workload
was exercised concurrently.

### Correctness and sanitizer evidence

- After the final production-path change, GCC13 Release passed 45/45 CTest
  targets. A later test-only correction removed an invalid assumption that a
  periodically sampled completion high-water mark must be nonzero; its focused
  target passed, but the environment denied the additional full-suite rerun
  requested at report-finalization time. This is not recorded as a second
  45/45 run.
- A first Pipeline repeat run exposed a test-synchronization defect: the test
  consumed an earlier queue `full_count` sample and could allow two valid
  callers to exchange sequence 7/8. After requiring a new full-count event and
  waiting for the full accepted/decoded/applied/stored snapshot, the Parallel
  Pipeline test passed 100/100 repeats with no duplicate sequence.
- History batch gate/first wake/boundary test: 20/20 repeated PASS.
- Benchmark harness unit tests: 22/22 PASS.
- Current-worktree ASAN/UBSAN build: 42/42 loadable CTest targets PASS.
- Three dynamic-loader targets were not loadable in this GCC13 sanitizer and
  system-runtime combination: `test_sdk_direct_runtime`,
  `test_realtime_shared_service_v2`, and
  `test_order_event_delta_live_python_e2e`. The observed loader failure is an
  unresolved `__cxxabiv1::__vmi_class_type_info` symbol in the sanitized C++
  DSO. These targets are untested under ASAN, not counted as passes.
- TSAN produced `FATAL: ThreadSanitizer: unexpected memory mapping` before all
  three focused tests entered user code. No TSAN result is claimed.

## Limitations and uncertainty

1. **Literal latency non-regression is unproven.** W0 median/p95 point estimates
   all improved, while W4 medians improved and W4 p95 point estimates regressed
   by 1.231–2.228 ms. Seven of eight zero-margin metric gates failed. The
   farm-active 500k callback-to-Polars distribution was not measured by the
   low-load A/B probe.
2. **The 10-second 500k result has pressure, not certified headroom.** Exact
   simultaneous high water is intentionally not measured on every message to
   avoid perturbing the hot path.
3. **Traffic distribution controls the answer.** Distributed Shenzhen order/
   transaction traffic scaled in the tested `fast`-sink profiles; a
   single-instrument owner did not.
4. **The benchmark callback is synthetic and serialized.** It exercises the
   real Pipeline, History, Store, shared-memory service, native reader, and
   Polars path where stated, but it does not reproduce vendor network and DSO
   scheduling.
5. **The host was heavily shared.** High load and I/O wait coincided with wide
   latency confidence intervals and some rate/steady-state misses and may have
   contributed; no isolated-host control proves causation.
6. **CERTIFIED is not qualified.** The primary 10-second success used `fast`;
   `fast_certified` passed 0/6 one-second 500k trials in the final rerun.
7. **No TSAN evidence is available.** Release stress and ASAN/UBSAN evidence
   reduce risk but do not replace a functioning race detector.

## Recommended next steps

1. Keep the production default at `--parallel-decoder-workers 0`.
2. On a CPU- and I/O-isolated deployment-equivalent host, repeat the same
   30-pair latency gate and require a predeclared engineering margin. If the
   requirement remains literally 0.0 ms at the one-sided 95% bound, do not
   enable W4 until that exact gate passes.
3. Qualify `--parallel-decoder-workers 4` with the real traffic distribution,
   real vendor DSO, CPU/NUMA placement, and the required `fast_certified` sink
   for a longer session. Monitor offer-end backlog, decoder full count,
   completion publication failures, fatal state, lease waits, and History
   queue full.
4. Treat single-instrument overload as a separate architecture problem. It
   requires changing instrument-owner/store serialization or workload
   partitioning; adding decode workers cannot solve it.
5. If capacity certification is mandatory, add an exact low-perturbation depth
   sampling design or offline trace before claiming headroom from the current
   HWM fields.
6. Run the focused concurrency suite under a working TSAN environment or
   equivalent race-detection setup before production promotion.

## Further questions

- What fraction of the real 09:30 peak belongs to one source, one instrument,
  and each History worker?
- Is `fast_certified` mandatory at the 500k target, and what backlog budget is
  operationally acceptable during the opening burst?
- What callback-to-Polars regression margin is meaningful operationally if a
  literal statistical bound of 0.0 ms cannot be established on the target
  host?
- Can parse workers, ordered committers, History workers, Python readers, and
  IRQs be placed on separate physical cores and NUMA-local memory?
