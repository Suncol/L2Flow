# Reorder benchmark

Build the opt-in benchmark and run the required disorder matrix:

```bash
cmake -S . -B build-bench \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_COMPILER=/path/to/g++-13 \
  -DL2FLOW_BUILD_BENCHMARKS=ON
cmake --build build-bench -j --target benchmark_realtime_planes

.venv/bin/python benchmarks/run_reorder_matrix.py \
  --binary build-bench/benchmark-realtime-planes \
  --records 100000 \
  --output artifacts/reorder-matrix.json
```

The executable reports FAST-first router p50/p99/p99.9, end-to-end catch-up
throughput, repair attempts, and the Event comparison-sort counter. Run the
ordered case on the same pinned hardware as the current production baseline;
the repository does not encode a hardware-independent latency threshold.

## Callback to Event Polars benchmark

The callback/Polars driver loads a benchmark-only C ABI bridge into the
repository Python environment. Binary Shanghai Tick and Shenzhen Transaction
messages enter `FastTickPipelineV1::IngestForTest`, which shares the production
callback admission/ownership/decoder path. Event reads then pass through
`InstrumentDataServiceV3`, Python `DerivedEvent` validation, immutable Polars
blocks, cumulative DataFrame concatenation, and a tail read.

```bash
cmake -S . -B build-bench \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_COMPILER=/path/to/g++-13 \
  -DL2FLOW_BUILD_TESTS=OFF \
  -DL2FLOW_BUILD_BENCHMARKS=ON
cmake --build build-bench -j --target \
  l2flow_callback_polars_benchmark

taskset -c 0-31 .venv/bin/python \
  benchmarks/run_callback_polars_latency.py \
  --library build-bench/libl2flow_callback_polars_benchmark.so \
  --rates 400000,500000,600000,700000,800000 \
  --duration-seconds 3 \
  --instruments 32 \
  --workers 8 \
  --queue-capacity 65536 \
  --batch-size 1024 \
  --rows-per-block 4096 \
  --repetitions 1 \
  --output artifacts/callback-polars-400k-800k.json
```

The bridge assigns one exclusive CPU to every Tick/Event/KLine route-worker
slot (the parked Event/KLine repair thread shares its plane slot) and reserves
the remaining CPUs in the `taskset` mask for the two decoder lanes, the paced
producer, and Python. A throughput trial passes only when callback
and native all-plane catch-up rates are both at least 98% of the target, every
message reaches the stable Event view, all instruments remain recoverable,
and no queue repair occurs.

The measurement does not include Vendor SDK network/callback dispatch or a
cross-process transport, because Wire V3 currently exposes only the
process-local service boundary. The normal rolling latency test covers INSERT
CDC; late-data range-repair latency is a separate workload.
