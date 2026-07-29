# Realtime Latency Benchmark — 2026-07-29

## Result

The observed-universe runtime was measured with 60,000 bound and
snapshot-available instruments inside the production default physical
capacity of 65,536. Known-ID point reads did not become catalog scans:

- Python public `latest_snapshot(1)` had a run-level median p50 of
  15.009 microseconds.
- Python public `latest_snapshot(60000)` had a run-level median p50 of
  14.940 microseconds.
- A deterministic single-ID access stream covering all 60,000 instruments had
  a run-level median p50 of 15.170 microseconds.
- Strict callback-call-start to Python public latest-return latency had a
  run-level median p50/p95/p99 of 75.940/81.119/88.940 microseconds.

These are measurements from this host, not production SLA guarantees.

## Tested source and build

The benchmark is an opt-in mode of
`test_realtime_shared_service_v2`; it is not part of the normal CTest path:

```bash
L2FLOW_PYTHON_CPU=16 \
taskset -c 8-15 \
./build-release/test_realtime_shared_service_v2 --latency-benchmark
```

The tested build used:

```text
GCC                         11.4.0
C++ flags                   -O3 -DNDEBUG -Wall -Wextra -Wpedantic
                            -Wconversion -Wshadow -Werror -std=c++20
Python                      3.10.12
CPU                         2 x AMD EPYC 9354, 64 physical / 128 logical CPUs
Pipeline affinity           CPUs 8-15, NUMA node 0
Python affinity             CPU 16, NUMA node 0
CPU frequency governor      schedutil
perf_event_paranoid         4
```

The host was shared and its CPUs were not isolated. Dynamic frequency and
scheduler noise remain in the tail. Three independent Release runs were used.
Tables below report the median of each run's nearest-rank quantile followed by
the range across the three run-level quantiles.

## Fixture and invariants

The setup uses one fresh session and one real `RealtimePipelineV1`. It sends
60,000 distinct Shenzhen snapshot messages through the installed SDK
`MessageHandlerBase::OnMessage`:

```text
SecurityID 000001 ... 060000
capture_sequence 1 ... 60000
```

Messages are admitted in batches of 512. Between setup batches the driver
waits for both the applied and durable dense prefixes. This avoids
intentionally overflowing a bounded queue while building the working set.
Before measuring reads, the benchmark requires:

```text
capacity                    = 65536
catalog_generation          = 60000
bound_count                 = 60000
available_count             = 60000
snapshot_available_count    = 60000
tick_available_count        = 0
factor_eligible_count       = 60000

accepted_sequence           = 60000
applied_sequence            = 60000
durable_sequence            = 60000
```

It also reads IDs 1 and 60000 through the C Reader and verifies that their
payload sequences are 1 and 60000 respectively.

The full test sequence is:

```text
1 ... 60000       60k working-set construction
60001 ... 71000   C closed-loop warmup + measurement
71001 ... 82000   Python closed-loop warmup + measurement
82001 ... 85000   open-loop burst
```

The terminal assertion requires
`accepted_sequence == applied_sequence == durable_sequence == 85000`.

The mapping is 343,937,024 bytes, the 60,000 keys consume 600,000 bytes of the
key arena, and GNU `time -v` reported a maximum resident set of 970,752 KiB in
each pinned run. Working-set construction converged to the durable prefix in
5.44-6.13 seconds. After the final setup callback, the complete applied prefix
became visible in 0.861-1.093 milliseconds and durable in
10.946-11.309 milliseconds.

## Measurement definitions

### Static Python reads

Static measurements timestamp immediately before and after the API call;
payload validation happens after the ending timestamp.

- **Head/tail public** repeatedly reads ID 1 or ID 60000.
- **60k working set** uses stride 7,919, which is coprime with 60,000, and
  covers the entire ID range before repeating.
- **Python native wrapper** is not a pure C call. It still includes the Python
  `NativeReader`, ctypes allocation, native payload copy and validation,
  Python bytes copy, parsing, and dataclass construction. It only removes the
  outer `L2FlowClient` layer.
- **C successful latest call** is the duration of the successful fixed-slot C
  Reader call. Bounded seqcount conflicts are retried and counted separately.
- **Connect** includes control-socket discovery, `SCM_RIGHTS`, `mmap`, layout
  validation, and the C Reader's O(`bound_count`) validation of the published
  identity prefix.

Python GC remains enabled with its default thresholds.

### Callback to Python

Closed-loop sampling permits exactly one in-flight update:

```text
Python EXPECT(sequence)
Python validates the preceding value and replies ARMED(sequence)
C++ timestamps immediately before OnMessage
C++ calls the actual installed OnMessage handler
Python busy-polls public latest_snapshot
Python timestamps after the API returns the exact sequence
```

The strict origin is the C++ `CLOCK_MONOTONIC` observation immediately before
the virtual `OnMessage` call. It is a tight lower bound on the actual function
entry timestamp, so origin-to-consumer is a tight upper bound on
entry-to-consumer latency.
The Python `seen` timestamp uses the same Linux `CLOCK_MONOTONIC` clock domain.
The payload's internal receive timestamp is reported separately and is about
0.5 microseconds later in the production-default, stage-disabled path.

There are 1,000 warmup and 10,000 measured samples for both C and Python.
Every 512 samples, and at each phase tail, the driver waits for durability
only after the current consumer has seen the value and its latency has been
recorded. That pacing barrier is between samples. It is not a Reader gate and
does not enter any sample's callback-to-consumer duration.

Production-default stage instrumentation is disabled in the primary mode.
The test-only timed IPC sink still places clock observations around
`PublishApplied`; the slot release occurs inside that interval. Therefore IPC
return is a completion upper bound, not the exact release instruction.

### Open-loop burst

The final 3,000 callbacks have no per-record acknowledgement or durability
barrier. This is within the default 4,096 queue capacity. Since latest is a
one-slot projection, Python is not expected to observe every intermediate
sequence. The benchmark reports:

- how many sequence values survived long enough to be observed;
- how many were overwritten between reads;
- age only for observed survivors;
- per-record callback-to-IPC latency for all 3,000 records.

Survivor age has selection bias and must not be interpreted as the latency of
every offered callback.

## Three-run results

All values are microseconds unless shown otherwise.

| Metric | p50 median [range] | p95 median [range] | p99 median [range] |
|---|---:|---:|---:|
| Python public, ID 1 | 15.009 [14.970, 15.110] | 15.680 [15.550, 15.680] | 20.010 [18.910, 20.640] |
| Python public, ID 60000 | 14.940 [14.940, 15.120] | 15.620 [15.500, 15.690] | 20.050 [19.890, 21.170] |
| Python public, 60k permutation | 15.170 [15.120, 15.491] | 15.820 [15.750, 16.070] | 20.690 [20.470, 21.260] |
| C callback start to latest return | 46.670 [44.930, 47.219] | 53.090 [50.649, 53.500] | 58.669 [58.200, 59.790] |
| C callback start to IPC return | 45.310 [44.039, 45.850] | 51.730 [49.700, 52.190] | 57.020 [56.520, 58.340] |
| Python callback start to latest return | 75.940 [69.290, 77.879] | 81.119 [80.779, 83.670] | 88.940 [88.290, 89.849] |
| Python callback start to IPC return | 54.640 [53.700, 57.179] | 61.040 [60.559, 63.550] | 68.580 [66.920, 69.370] |

Additional medians:

| Metric | p50 | p95 | p99 |
|---|---:|---:|---:|
| Python connect with 60k identity validation | 1.449 ms | 1.527 ms | 1.553 ms |
| C successful latest call | 1.450 | — | — |
| Python native-wrapper, head ID | 13.320 | 13.850 | 18.149 |
| Python native-wrapper, tail ID | 13.360 | 13.850 | 18.160 |
| Public with Python staleness checks disabled, head | 14.740 | 15.389 | 19.870 |
| Callback call duration during Python phase | 4.080 | 4.860 | 7.170 |
| Timed IPC publish call during Python phase | 5.250 | 5.370 | 5.551 |

The public Reader therefore adds about 1.7 microseconds at p50 over the Python
native-wrapper path. Most of the remaining gap to the C Reader is Python
ctypes allocation, payload conversion/parsing, and model construction rather
than an instrument lookup. The head and tail results are indistinguishable at
the precision relevant here, while the full 60k point-read working set adds
only about 0.16 microseconds to the run-level median p50.

### Distinct-ID batches

The ID tuples are created outside the timed region and rotate across all
60,000 IDs. Values below are the batch-call duration divided by batch size:
they are amortized per returned record, not independently timed record
latencies.

| Batch size | p50 median [range] | p95 median [range] | p99 median [range] |
|---:|---:|---:|---:|
| 8 | 10.953 [10.877, 11.099] | 11.703 [11.584, 11.783] | 12.187 [12.145, 13.301] |
| 32 | 10.681 [10.592, 10.782] | 11.014 [10.862, 11.086] | 11.280 [11.248, 12.745] |
| 128 | 10.636 [10.520, 10.727] | 13.799 [13.734, 14.022] | 13.901 [13.894, 14.237] |

Batching amortizes the Python/ctypes call boundary. Size 32 gave the best
observed p95/p99 balance. Size 128 retained a good p50 but showed a repeatable
upper-quantile step while default Python GC was enabled; this benchmark does
not isolate GC as the cause.

### Open-loop burst

The three producer rates were 197k, 218k, and 214k callbacks/second. Python
observed 2,415-2,602 of the 3,000 latest values and skipped 398-585
intermediate sequence values. The median run observed 2,526 values (84.2%).
This is legal latest-slot overwrite, not Pipeline loss: the final assertions
still require every sequence through 85,000 to be applied and durable.

Per-record callback-to-IPC return latency under this overload had run-level
median p50/p95/p99 values of 17.466/28.974/29.534 milliseconds. Successful
Python reads of new survivor values still took about 15-16 microseconds; the
millisecond age came from processing backlog created by a roughly
200k-record/second producer, not from catalog lookup.

## Mandatory Journal queue stress finding

An earlier unpaced 10,000-sample sustained C closed loop used the default
4,096 Journal
queue without the between-window durability barriers. On this host it failed
closed at capture sequence 68,898:

```text
first fatal reason = mandatory_journal_admission
detail             = 3
detail meaning     = MandatoryJournalAppendResultV2::kQueueFull
```

The failure occurred after 8,898 post-fill callbacks had been offered while
real processing was ahead of `fdatasync`. In that run, the observation
demonstrated both intended properties:

1. realtime visibility does not wait for durability; and
2. mandatory admission is bounded and never silently drops on overflow.

It also means the default 4,096 Journal queue cannot absorb that particular
uninterrupted synthetic burst on this storage. Queue sizing must cover the
deployment's maximum arrival-minus-durability backlog. This benchmark does
not establish the required production size because it does not reproduce the
real feed rate, storage device, batching schedule, or callback distribution.
The final latency runs retain the default queue and use explicit 512-record
measurement windows; the separate 3,000-record open-loop test remains
unpaced.

## Interpretation and limits

The 65,536-slot default is not causing valid-ID point-read latency to scale
with the number of bound instruments. The implementation uses
`ordinal = instrument_id - 1`, an acquired `bound_count`, and a fixed slot
copy. The empirical head, tail, and full-working-set results agree with that
O(1) design.

For the closed-loop Python path, the main latency decomposition is roughly:

```text
callback call/admission                 ~4 microseconds p50
callback start -> IPC publish return   ~55 microseconds p50
callback start -> Python return        ~76 microseconds p50
```

The approximately 21-microsecond difference between the two run-level p50
statistics is consistent with a roughly 15-microsecond public read plus
polling alignment and process scheduling. It is not a paired measurement of
IPC-return-to-Python-return latency: the slot may become visible before
`PublishApplied` returns. The realtime path does not synchronously wait on
Journal I/O; background Journal work can still contribute indirect CPU,
cache, or storage contention.

The benchmark deliberately matches the requested fresh early-session case.
It does not test crash recovery, replay, checkpoints, rollover, capacity
exhaustion, or more than 65,536 instruments.

The fake SDK installs and invokes the real production handler and exercises:

```text
OnMessage
-> owned ingress copy
-> mandatory Journal admission
-> ordered processing
-> dynamic BindOrGet
-> decoder
-> Store
-> IPC
-> Python public latest_snapshot
```

It does not include network delay, vendor SDK parsing before `OnMessage`,
vendor callback-thread scheduling, a real Python strategy workload, or a
production-isolated CPU/storage configuration. p99.9 and maxima are retained
in machine output for diagnostics but are intentionally not used as SLA
claims.
