# Single-instrument complete-history stage benchmark

This benchmark measures one Python client reading every record for one
instrument from one immutable Store generation through the real Unix-domain
socket, SCM_RIGHTS, sealed-memfd, and explicit-EOF protocol.

It is an opt-in benchmark, not a CTest target. The observer is process-local
and disabled by default; a null observer adds no benchmark clock reads to the
service path and does not alter the history wire ABI.

## Stage boundaries

- `memfd`: server-side `memfd_create`, truncate, writable mmap, zero-fill,
  unmap, sealing, seal verification, and read-only `/proc/self/fd` reopen.
- `object_decode`: Python descriptor/payload validation, payload byte copy,
  `Snapshot`/`Tick` parsing, and `HistoryRecord` construction. FD validation,
  mmap creation, page-header validation, and mmap close are excluded.
- `column_build`: Python `_history_columns_by_kind` list construction.
  Arrow and Polars materialization are excluded.
- `requested_residual`: complete-scan wall time not covered by those three
  mutually exclusive stages. It includes open/EOF, socket work, server
  projection/send, Python FD and page-header validation, and benchmark
  validation.

`share_full_scan` divides a stage by wall time from cursor open through the
explicit EOF response. `share_requested_mix` divides it by the sum of the
three requested stages. Server stages are nested inside page-read wall time;
the analyzer never adds nested timings to the wall-time denominator.

## Build and run

```bash
cmake -S . -B build-history-bench \
  -DCMAKE_BUILD_TYPE=Release \
  -DL2FLOW_BUILD_TESTS=ON \
  -DL2FLOW_BUILD_BENCHMARKS=ON
cmake --build build-history-bench \
  --target benchmark_single_instrument_history_stages -j2

taskset -c 0 \
  ./build-history-bench/benchmark-single-instrument-history-stages \
  --output-dir artifacts/history-stage/tick-run-1 \
  --records 65536 \
  --page-records 4096 \
  --rounds 5 \
  --warmups 2 \
  --snapshot-every 0 \
  --seed 1 \
  --benchmark-run-id 1

python3 benchmarks/analyze_single_instrument_history_stages.py \
  artifacts/history-stage/tick-run-1
python3 benchmarks/summarize_single_instrument_history_runs.py \
  artifacts/history-stage \
  --expected-runs-per-workload 3
```

Use a fresh benchmark process and distinct seed/run ID for every independent
run. `snapshot_every=0` is tick-only, `1` is snapshot-only, and a value above
one creates a deterministic mixed workload. The harness keeps scans logically
serial and reserves a second service reader slot only for overlap with
asynchronous teardown of the just-completed EOF connection.

The analyzer strictly joins client and server pages by benchmark run, open
request, generation, and page index. It also checks read-request identity,
page layout/counts, source counts, contiguous page indices, ingress sum/xor,
generation totals, all warmup and measured scans, and explicit EOF.

The cross-process summarizer accepts only distinct run IDs with identical
measurement definitions, measured-source hashes, workload configuration,
records, page size, and warmup/measure counts. For every run it independently
reconciles the measured scan count, round/scan IDs, record total, scan-wall
sum, requested-stage totals, residual, and stage shares before pooling scan
wall samples. A zero-duration individual stage or zero residual is valid, but
the sum of the three requested stages must be positive. Global stage
`ns_per_record` and shares use summed nanoseconds and records; the scan p50 is
R-7 over all validated measured scans, while between-run min/mean/max treats
each fresh process as one observation.

`comparison.csv` and `comparison.json` are each written through a same-directory
temporary file, flushed, fsynced, and atomically replaced. CSV is committed
first; JSON is the final completion marker and records the resulting CSV byte
count and SHA-256. Consumers should require the JSON marker and verify that
hash before using the pair.

## 2026-07-28 baseline

Host: AMD EPYC 9354, Linux x86-64, GCC 11.4, Python 3.10.12. The complete
process was pinned to CPU 0. Each workload used three fresh processes, two
warmup scans, and five measured scans per process. The build type was Release.

| Workload | Records per scan | Pages | Scan p50 | memfd/full | decode/full | columns/full | residual/full |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tick-only | 65,536 | 16 | 1,608.1 ms | 1.084% | 75.060% | 19.189% | 4.667% |
| mixed (656 snapshots) | 65,536 | 16 | 1,770.9 ms | 1.054% | 76.205% | 17.936% | 4.805% |
| snapshot-only | 4,096 | 1 | 1,105.5 ms | 0.774% | 86.833% | 9.256% | 3.137% |

| Workload | memfd ns/record | decode ns/record | columns ns/record | decode/requested mix |
| --- | ---: | ---: | ---: | ---: |
| tick-only | 266.0 | 18,419.8 | 4,709.0 | 78.734% |
| mixed | 285.2 | 20,623.1 | 4,854.0 | 80.052% |
| snapshot-only | 2,103.5 | 235,836.5 | 25,137.7 | 89.645% |

The weighted comparison is in
`artifacts/single_instrument_history_stages_20260728_v2/comparison.json` and
`comparison.csv`. The artifact directory is intentionally ignored by Git;
each analysis records input and measured-source SHA-256 hashes.

## Optimization implication

Object materialization is the first optimization target across every tested
payload mix. Python list-column construction is second. A memfd-only
optimization has a measured direct ceiling of about 0.8% to 1.1% of complete
scan wall time on this host.

The highest-value next experiment is a column-oriented decode path that
validates the mapped descriptors and payloads but writes typed column buffers
directly, bypassing the intermediate `Snapshot`/`Tick`/`HistoryRecord`
dataclass graph and the second traversal used by `_history_columns_by_kind`.
The object API should remain available as a compatibility path. This baseline
does not include that optimization.
