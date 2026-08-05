# Event dirty-suffix replay benchmark (2026-08-05)

This is a host-local regression measurement for the checkpointed mutable-tail
and dirty-suffix shadow-replay implementation. It is not a portable service
level objective.

## Environment and method

- Release build, GCC 13.4.0.
- AMD EPYC 9354, two sockets, 32 cores per socket, SMT2.
- Process affinity: `taskset -c 0-3`.
- One instrument, 100,000 source records, one Tick/Event/KLine route.
- Three process runs per workload; tables report the median of each metric.
- Stable-root acquire uses 10,000 samples per run. Full `CopyRows` uses 100
  samples per run.
- Every measured root was checked for strict Event-key ordering, contiguous
  business sequences 1 through 100,000, and the exact expected row count.
- Throughput covers publish plus both derived planes reaching their final
  stable roots. Publish latency is synchronous `PublishDecoded` latency
  (FAST publication plus derived-queue admission).

The deterministic workloads are:

- ordered: strictly increasing business sequence;
- adjacent 0.01%: one adjacent pair swapped per 10,000 inputs;
- adjacent 1%: one adjacent pair swapped per 100 inputs;
- earliest late: sequence 1 arrives after sequences 2 through 100,000.

## Throughput and publish latency

| Exchange | Workload | Throughput (records/s) | Publish p50 (us) | p99 (us) | p99.9 (us) |
|---|---|---:|---:|---:|---:|
| Shanghai | ordered | 466,959 | 1.580 | 5.460 | 7.140 |
| Shanghai | adjacent 0.01% | 423,838 | 1.550 | 5.320 | 6.950 |
| Shanghai | adjacent 1% | 322,061 | 1.500 | 5.220 | 6.760 |
| Shanghai | earliest late | 285,608 | 1.550 | 5.361 | 6.860 |
| Shenzhen | ordered | 447,848 | 1.560 | 5.420 | 6.850 |
| Shenzhen | adjacent 0.01% | 426,563 | 1.600 | 5.360 | 6.640 |
| Shenzhen | adjacent 1% | 508,276 | 1.230 | 5.220 | 6.730 |
| Shenzhen | earliest late | 316,104 | 1.560 | 5.420 | 6.830 |

The Shenzhen 1% throughput being above its ordered run is a batching effect,
not a claim that disorder makes projection intrinsically cheaper: many
inversions are absorbed inside a journal-first micro-batch, while suffix
commits coalesce immutable-root publications.

## Event read latency

| Exchange | Workload | Stable acquire p50 (us) | p99 (us) | Full 100k rows p50 (ms) | p99 (ms) |
|---|---|---:|---:|---:|---:|
| Shanghai | ordered | 0.040 | 0.040 | 4.187 | 5.328 |
| Shanghai | adjacent 0.01% | 0.040 | 0.040 | 4.039 | 5.172 |
| Shanghai | adjacent 1% | 0.040 | 0.040 | 3.835 | 4.479 |
| Shanghai | earliest late | 0.040 | 0.040 | 3.520 | 4.442 |
| Shenzhen | ordered | 0.040 | 0.040 | 3.879 | 5.066 |
| Shenzhen | adjacent 0.01% | 0.040 | 0.040 | 4.187 | 5.118 |
| Shenzhen | adjacent 1% | 0.040 | 0.040 | 5.736 | 6.286 |
| Shenzhen | earliest late | 0.040 | 0.040 | 3.513 | 4.575 |

Stable acquire is an atomic immutable-root pin and does not materialize rows.
The full-read column includes traversal and copying of all 100,000 rows.

## Repair work

`Repair slices` is the legacy `event_rebuild_attempts` counter; for normal
late input it counts cooperative dirty-replay turns, not FAST rebuilds.

| Exchange | Workload | Mutable-tail commits | Deep-suffix commits | Replayed inputs | Repair slices |
|---|---|---:|---:|---:|---:|
| Shanghai | ordered | 0 | 0 | 0 | 0 |
| Shanghai | adjacent 0.01% | 9 | 0 | 2,304 | 9 |
| Shanghai | adjacent 1% | 66 | 0 | 420,884 | 924 |
| Shanghai | earliest late | 0 | 1 | 100,000 | 320 |
| Shenzhen | ordered | 0 | 0 | 0 | 0 |
| Shenzhen | adjacent 0.01% | 9 | 0 | 2,304 | 9 |
| Shenzhen | adjacent 1% | 659 | 0 | 62,408 | 659 |
| Shenzhen | earliest late | 0 | 1 | 100,000 | 209 |

Every run reported both `event_cold_fast_rebuilds = 0` and
`event_full_comparison_sort_calls = 0`. Thus even the earliest-late workload
used one exact dirty-suffix replay rather than the cold full-FAST fallback.
