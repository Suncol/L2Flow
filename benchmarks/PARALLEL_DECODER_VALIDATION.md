# Parallel decoder validation

These probes are explicit operator benchmarks. They are not registered with
CTest because CPU affinity, machine isolation, build type, and repeated runs
are part of the measurement contract.

## Build

Use GCC 13 and a Release tree with the repository Python environment visible:

```bash
cmake -S . -B build-parallel-gcc13 \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_COMPILER=/home/linuxbrew/.linuxbrew/bin/gcc-13 \
  -DCMAKE_CXX_COMPILER=/home/linuxbrew/.linuxbrew/bin/g++-13
cmake --build build-parallel-gcc13 -j --target \
  test_realtime_shared_service_v2
```

The executable defaults to `parallel_decoder_worker_count=0`, which is the
legacy one-decoder-owner-per-source topology. A positive worker count arms the
adaptive topology; it does not prove that the parse farm ran. Initially only
the four source owners start. They perform the existing full decode inline,
and, while the production idle-inline mode and a nonzero activation depth are
enabled, compare every Pop's already-observed remaining ring occupancy with
the source-local threshold. Pop already requires the tail acquire, so this
adds no second cross-core depth read. A value at or above the threshold lazily
starts the parse workers plus four ordered committers. The configured threshold
defaults to 8,192, while the effective threshold is
`min(8192, Q - max(1, Q/4))`. Each owner checks only its own source FIFO;
aggregate backlog and the number of non-empty sources do not lower it. Once
farm outstanding reaches zero, a low current occupancy permits ownership
to return to the inline source owner. `IDLE_INLINE=0`, a configured activation
depth of zero, or an effective activation depth of zero selects the farm on
the first record instead of using an inline interval.

No test should infer that the farm ran from the branch, binary name, or a
positive worker count. Check the emitted `parallel_enabled`, `parallel_farm`,
per-worker parse counts, and active-worker count. At quiescence,
`parallel_inline + parallel_farm` must equal dispatched records, while summed
worker `parallel_parsed` must equal only `parallel_farm`. Live multi-field
snapshots are eventually consistent; final drained counter identities are
exact. Issue/completion high-water fields retain the conservative-upper-bound
and sampled-lower-bound semantics defined below even after drain.
The emitted active-worker count means workers whose parsed-message count is
non-zero; it is not a count of currently runnable threads.

An ordered farm committer may greedily take at most 16 completion records
that are already contiguous. It stops at the first missing source sequence
and never sleeps or waits to fill a target batch. A single ready record uses
the scalar History path. In a multi-record submission, every event is
finalized and enqueued immediately in source order; the first record routed to
each History worker is signaled immediately and only redundant later signals
are coalesced until `Finish()`. `parallel_history_batch_calls` counts finished
submissions whose admitted prefix exceeds one record,
`parallel_history_batched_messages` counts those admitted records, and
`parallel_history_batch_max` cannot exceed 16.

## Short throughput matrix

The parameterized executable form is:

```text
--throughput-profile-benchmark RATE DURATION_MS INSTRUMENTS_PER_MARKET
  STORE_WORKERS PARALLEL_DECODER_WORKERS IDLE_INLINE DECODER_QUEUE
  STORE_QUEUE SEGMENT_KIB WORKLOAD SINK
```

Workloads are `single_instrument`, `five_tuple_uniform`,
`four_source_balanced`, and `hot_shenzhen_tick_source`. Sinks are `fast` and
`fast_certified`. `single_instrument` is an explicit worst-case compatibility
case; the original `--throughput-stability-benchmark` remains unchanged.

A minimal multi-source command using production queue and segment defaults is:

```bash
taskset -c 8-23 \
  build-parallel-gcc13/test_realtime_shared_service_v2 \
  --throughput-profile-benchmark \
  500000 1000 256 4 4 1 65536 32768 64 \
  five_tuple_uniform fast
```

The equivalent CERTIFIED composition is selected by replacing the final
`fast` with `fast_certified`. This wrapper publishes FAST first and verifies
that the independent CERTIFIED handoff drains without drops or frozen
channels.

`IDLE_INLINE` is mandatory: use `1` for the latency-oriented candidate and
`0` for deterministic farm-only scaling. The matrix runner likewise requires
`--idle-inline 0|1`. Farm-only mode is diagnostic; the callback-to-Polars A/B
test always keeps the idle-inline path enabled.

Run a small topology matrix with machine-readable JSON and CSV evidence:

```bash
.venv/bin/python benchmarks/run_parallel_decoder_matrix.py \
  --binary build-parallel-gcc13/test_realtime_shared_service_v2 \
  --output-dir artifacts/parallel_decoder_matrix \
  --cpu-list 8-23 \
  --rates 50000 100000 200000 300000 500000 \
  --parallel-workers 0 1 2 4 \
  --idle-inline 1 \
  --store-workers 4 8 \
  --workloads five_tuple_uniform four_source_balanced \
              hot_shenzhen_tick_source \
  --sinks fast
```

The driver rejects queue-full/fatal/rejected records, incomplete count
identities, parallel completion failures, CERTIFIED drops/freezes, and
excessive backlog at the instant offering ends. Adaptive all-inline traffic is
valid by default. Add `--require-farm` when the purpose is to prove that at
least two parse workers actually ran. The runner hashes the binary, rejects a
mid-run binary change, and verifies the emitted affinity against `--cpu-list`.

Every trial has two separate classifications. `functional_verdict` is the
full acceptance verdict: it covers process/configuration/affinity/topology
validity, at least 98% of requested offered rate, complete count identities,
explicit failure counters, and the declared offer-end/second-half backlog
budgets. `capacity_pressure_verdict` is one of
`HEADROOM_UNPROVEN`, `PRESSURE_OBSERVED_NO_FAILURE`,
`ACTUAL_OPERATION_FAILURE`, `TELEMETRY_INVALID`, or `NOT_APPLICABLE`.
An issue/completion high-water value equal to its reported capacity, or a
positive lease-wait count, is only classified as observed pressure; without
an enqueue/publish failure it is not classified as an operation failure.

The two high-water fields intentionally have different, non-exact semantics.
`parallel_issue_high_water_max` is the maximum across workers of the sum of
four per-source SPSC-shard high-water bounds. A producer samples the consumer
head before publishing its tail, so a concurrent pop can make each retained
shard value a conservative upper bound rather than an exact peak. The four
shard bounds can also occur at different times, making their sum a
conservative upper bound on simultaneous issue depth.
`parallel_completion_high_water_max` is the maximum of periodically sampled
completion depths, so it is a lower bound on the true completion high-water
mark. Values below capacity therefore do not prove simultaneous capacity
headroom. The JSON metadata records both definitions.

`--require-capacity-headroom` is deliberately fail-closed for every
positive-worker trial and adds `parallel_capacity_headroom_unproven` to the
overall failure reasons. Under the current low-overhead telemetry, an exact
headroom claim cannot be established; actual operation failures and
out-of-range telemetry remain independent hard failures.
`HEADROOM_UNPROVEN` does not mean that headroom exists, and
`PRESSURE_OBSERVED_NO_FAILURE` does not mean that a queue-full failure
occurred.

It deliberately records `drain_elapsed_ns` but does not use post-offer drain
to excuse backlog. The executable's short-probe budget is the larger of 1,024
records or one millisecond of offered traffic, and it emits that value. The
matrix driver's `--max-backlog-records` is a second, explicit acceptance
budget; set it to zero for a literal zero-backlog gate.

A one-second run is a scaling probe, not evidence of sustained 500k/s. A
qualification run must additionally use an operator-sized retained-store
budget, repeat/soak duration suitable for the deployment, and inspect that the
second-half backlog increase (`q100 - q50`) stays within the explicit budget.
This is a bounded guardrail, not a statistical trend test. Repeat the matrix
for balanced, hot-source, and single-instrument distributions; aggregate
500k/s success does not certify an arbitrary one-key hotspot.

A `500k` target-profile PASS means that the synthetic serialized callback
schedule achieved at least 98% of the requested rate, every planned record
formed a complete prefix after drain, no explicit fatal/full/drop condition
occurred, and offer-end backlog stayed within the declared budgets. It is not
evidence of measured throughput at or above exactly 500,000 records/s, exact
queue headroom, a real-vendor full-session workload, or callback-to-Polars
latency while the farm is active. The `fast` result also does not certify the
additional `fast_certified` path.

## Callback-to-Polars A/B gate

The latency workload covers both required products:

- one 4,096-record, 55-column Batch history materialized as an eager Polars
  DataFrame;
- one complete order lifecycle, filtered and sorted as an eager Polars
  DataFrame, with its final revision and remaining quantity validated.

Run the legacy path and a candidate worker count in alternating order:

```bash
.venv/bin/python benchmarks/compare_callback_polars_ab.py \
  --binary build-parallel-gcc13/test_realtime_shared_service_v2 \
  --output-dir artifacts/callback_polars_ab_workers4 \
  --candidate-workers 4 \
  --pairs 30 \
  --cpu-list 8-23
```

Without another option, each pair uses the same executable: workers `0` is a
topology-only baseline and the requested positive count is the candidate.
That isolates the topology switch but cannot detect a regression in code
shared by both paths. For the final branch-level gate, preserve a Release
binary built from the pre-optimization branch with this latency harness and
run:

```bash
.venv/bin/python benchmarks/compare_callback_polars_ab.py \
  --binary build-candidate/test_realtime_shared_service_v2 \
  --baseline-binary build-baseline/test_realtime_shared_service_v2 \
  --baseline-command workers-zero \
  --output-dir artifacts/callback_polars_branch_ab_workers4 \
  --candidate-workers 4 --pairs 30 --cpu-list 8-23
```

The baseline binary in the branch-level gate must be rebuilt from the old
production sources with the same focused callback-to-Polars test driver and
Python probe. Pointing the runner at an untouched legacy executable is not a
valid comparison because it does not emit the exact 4,096-by-55 raw Batch and
complete-order-lifecycle measurement contract. The focused entry point avoids
running the unrelated broad history benchmark during every pair.

The parser additionally requires `CALLBACK_POLARS_TOPOLOGY` evidence that the
candidate is enabled but the low-load measurement remained entirely inline
(`farm_messages=0`, no active parse worker). The pair order reverses on every
other run. The default gate allows
`0.0 ms` regression and requires the one-sided 95% paired-bootstrap upper
bound to be no greater than zero for both median and p95 deltas of every
reported callback-to-Polars metric. The JSON retains all values in
milliseconds, while raw run logs retain nanoseconds.

With a zero margin, passing means the data provide evidence that the upper
confidence bound itself is nonpositive; similar point estimates are not
enough. This is statistical evidence, not a mathematical guarantee that no
future observation can be slower. If the zero-margin gate fails, report the
observed medians, p95 values, and confidence bounds verbatim and keep the
production default at zero. Do not silently replace it with a wider margin.
The default must remain zero until the target machine's interleaved test, full
distribution matrix, correctness suite, and sanitizer suites all pass.
