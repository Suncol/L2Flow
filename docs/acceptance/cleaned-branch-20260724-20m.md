# Cleaned branch DecodeMarket / realtime history / performance 20-minute follow-up acceptance

## Verdict

The cleaned realtime branch passed the functional and sustained-live-load
acceptance gates exercised in this 20-minute follow-up:

- the post-window Release CTest run passed all 22 tests;
- the deterministic market-decoder fuzz smoke passed all 50,150 cases;
- all five production market event kinds were decoded during the live window;
- 30,482,755 market messages were accepted and exactly 30,482,755 were
  decoded/submitted before terminal publication;
- no message was rejected, the pipeline did not become fatal, and all 1,200
  periodic generations plus generation 1,201 at terminal shutdown were
  published;
- all 49,947 registry instruments were present in exact registry order in
  every sampled generation;
- every sampled watermark satisfied the global-prefix/source-prefix identity;
- every factor publication retained the exact matching history-generation
  handle and had one row per registry instrument;
- all final bounded-history records passed instrument, source, cut, ordering,
  latest-kind, and capacity checks.

No numerical SLA was supplied. This is therefore a correctness, stability,
and observed-production-load result. It is not a claim that 25,400 messages/s
is the saturation ceiling or that a contractual latency budget was met.

The authoritative same-host freshness result is the monotonic
receive-to-read measurement: the global newest market head had a 10.185 ms
p50 and 20.957 ms p99. The signed exchange-event-to-read measurement was very
different from the earlier 10-minute run: all 1,200 global observations were
positive, with a 2.979 s p50, whereas 545/600 earlier observations were
negative and the earlier p50 was -262.162 ms. The SDK trace independently
changed from roughly -0.2 to -0.4 seconds in the earlier run to roughly +2.95
to +3.2 seconds in this run. This demonstrates a feed/upstream/realtime-clock
domain change outside the history read interval; it does not establish how
much is true transport latency versus clock offset or timestamp convention.
The execution environment could not provide PTP/NTP synchronization evidence.

## Tested identity and configuration

| Item | Value |
|---|---|
| Branch | `refactor/decoupled-realtime-history-factor` |
| Commit | `bf4b04092fbb5f64c526f60c35ea8cfaf29a64cb` |
| Commit subject | `test(decoder): add deterministic fuzz smoke acceptance` |
| Trade date | `20260724` |
| SDK subscription/stop log window | approximately `2026-07-24 14:00:34` to `14:20:35` CST |
| Requested measured window | 1,200 seconds |
| Measured process window | 1,200,093,379,216 ns |
| SDK endpoint | `127.0.0.1:9112` |
| SDK DSO | `/home/sunc/L2Flow/MDL/libmdl_api.so` |
| SDK DSO SHA-256 | `85b69d495e4a9d2e212342c887138599bd913b7d7426f4241d2d12f7c012116a` |
| Registry version | `20260724` |
| Canonical registry SHA-256 | `0f5d99120111308303f3b1d6e907dfb3164a78d160a11a09ad0c77e9ad03bf5a` |
| Registry entries | 49,947 |
| Registry physical-file SHA-256 | `064d6ddacd7f00aab0d6fdcdb6d44d9ecfc9ad2dfbd1be6a3c5cc1af085d919b` |
| History workers | 4 |
| Periodic generation interval | 1,000 ms |
| Generation timeout | 10,000 ms |
| History acquire repetitions | 32 per periodic generation |
| Maximum retained history | 64 records per instrument |
| Optional audit WAL | disabled for the live performance window |
| Probe source SHA-256 | `9f0d91a62905000b3def09ffd44dcf0d5a4aecb89cd4cca70b5e0bf985b99de8` |
| Executed Release probe SHA-256 | `c48ffd06f4e322f139fb0a9b959582da5265a5c229a48ea7d6c00d907e17cad2` |

The credential was read from a file and was not placed in the command line,
JSON, CSV, SDK logs, or this report.

The earlier 10-minute run used commit
`a21adff23f33bf3410e09a676677da345772b0f1`. Between that commit and this run,
only `CMakeLists.txt` and the 10-minute acceptance report changed; no
production runtime source changed. More importantly, the live probe source
and executed binary hashes are identical across the two runs. The performance
observations are therefore directly comparable as two natural-load windows,
while still being subject to different market arrival patterns and upstream
conditions.

## DecodeMarket acceptance

The decoder regression suite covers all five production wire schemas and the
following behavior groups:

- minimum binary sizes and service-version/schema gates;
- Shanghai tick action, side, reference, validity, and phase matrices;
- Shenzhen order enums, price applicability, quantity, and reference domains;
- Shenzhen transaction trade/cancel/unknown-execution matrices;
- Shanghai and Shenzhen identity and registry-resolution gates;
- nested snapshot depth, best-queue parsing, and public retention caps;
- malformed offsets, overlaps, counts, strings, and truncation paths;
- exact fixed-point normalization and overflow failure atomicity;
- null, zero, negative, and extreme price/quantity handling;
- exchange time, vendor local time, calendar bounds, and fixed UTC+08 Unix
  projection;
- maximum-duration unavailable sentinel handling;
- product-specific field applicability;
- stateful Shanghai phase-map capacity and history behavior.

The deterministic decoder fuzz smoke adds all five message selectors,
supported and unknown service-version modes, 15 boundary body lengths, and
50,000 deterministic pseudorandom bodies, for 50,150 cases total. It was
executed both through CTest and directly after the live window.

During the live run the production composition accepted and decoded/submitted
30,482,755 messages. Any core decoder error, unknown registry instrument,
history submission error, invalid retained record, queue-full condition, or
fatal transition fails the run. Terminal accepted/decoded equality,
`rejected=0`, and `fatal=false` are the live no-loss gates for this
composition.

All five decoded variants were observed:

1. Shanghai snapshot;
2. Shanghai tick;
3. Shenzhen snapshot;
4. Shenzhen order;
5. Shenzhen transaction.

Final per-source totals were:

| Source slot | Production stream | Messages | Mean rate | Share |
|---:|---|---:|---:|---:|
| 0 | Shanghai snapshot | 1,004,530 | 837.043/s | 3.295% |
| 1 | Shanghai tick | 12,471,473 | 10,392.085/s | 40.913% |
| 2 | Shenzhen snapshot | 1,144,249 | 953.467/s | 3.754% |
| 3 | Shenzhen order + transaction | 15,862,503 | 13,217.724/s | 52.038% |
| **Total** | five production message keys | **30,482,755** | **25,400.319/s** | **100%** |

Slot 3 is a combined runtime source counter. The acceptance scan observed
both decoded variants, but the runtime does not expose separate cumulative
Shenzhen order and transaction totals.

The runtime also recorded one `ignored_unsupported` callback, the same count
seen in the earlier run. It was not classified as an accepted core market
message or a rejection and did not enter decode/history processing. The
snapshot counter does not expose the ignored callback's message key, so this
test cannot identify it further. This does not break accepted/decoded
conservation for the five subscribed core streams, but it is an observability
boundary worth retaining in the evidence.

## Instrument history acceptance

The unit and live tests together exercised these history contracts:

- permanent routing by `instrument_id % worker_count`;
- one serial owner per source and strictly increasing dense source sequence;
- cross-source row ordering by process-wide ingress sequence;
- bounded suffix retention and correct latest snapshot/tick handles;
- marker/fence behavior when post-cut messages arrive behind only some source
  fences;
- all four source fences required before immutable generation publication;
- exact fixed registry universe, including instruments with no update;
- atomic acquisition of the latest immutable generation;
- factor publication retaining the exact corresponding history handle.

The 20-minute live evidence was:

| Check | Result |
|---|---:|
| Periodic generations sampled | 1,200 |
| Terminal generation | 1,201 |
| Universe rows checked | 59,936,400 = 49,947 x 1,200 |
| Updated instrument heads measured | 4,928,454 |
| Exchange event-time samples measured | 4,928,454 |
| History acquire calls timed | 38,400 = 1,200 x 32 |
| Final retained records fully checked | 474,693 |
| Prefix/watermark violations | 0 |
| Universe identity/order violations | 0 |
| Factor/history generation-handle violations | 0 |
| Final retained ordering/source/cut/capacity violations | 0 |
| Rejected messages | 0 |
| Fatal transitions | 0 |

The retained-record count is much smaller than the decoded count by design:
V1 retains only the newest configured 64 records per instrument. Final
accepted/decoded equality and generation prefixes prove processing of the
live prefix; the final full scan proves correctness of the retained suffix.
This is not an archival all-message-retention claim.

## Throughput

The one-second table uses the probe's percentile rule,
`sorted[ceil(q * (n - 1))]`, on all 1,200 periodic CSV rows:

| Metric | Min | p50 | p90 | p95 | p99 | Max | Mean |
|---|---:|---:|---:|---:|---:|---:|---:|
| Accepted messages/sample | 14,810 | 23,844 | 33,445 | 39,124 | 52,408 | 73,835 | 25,401.58 |
| Decoded/submitted messages/sample | 14,810 | 23,844 | 33,448 | 39,093 | 52,468 | 73,835 | 25,401.58 |
| Shanghai snapshot/sample | 670 | 833 | 906 | 925 | 967 | 1,021 | 837.10 |
| Shanghai tick/sample | 6,039 | 9,717 | 13,728 | 15,327 | 19,579 | 30,489 | 10,392.57 |
| Shenzhen snapshot/sample | 0 | 1,150 | 1,739 | 1,757 | 1,782 | 1,801 | 953.54 |
| Shenzhen order + transaction/sample | 6,824 | 12,204 | 18,045 | 21,633 | 29,728 | 43,797 | 13,218.38 |

The sample intervals averaged 1,000,001,765 ns after the first observation;
their minimum/maximum were 975,399,578/1,024,797,301 ns. The machine-readable
overall rate uses the exact terminal measured duration and is 25,400.319
accepted and decoded messages/s.

The one-second peak was 73,835 accepted messages at generation 1,173. Its
normal cut latency was 8.844 ms. The largest cut latency occurred separately
at generation 868 under 24,094 accepted messages in that sample, so the
single largest latency observation was not coincident with the largest input
burst.

Small accepted/decoded or per-source differences inside an individual
one-second observation are legal because snapshot counters can race callbacks
around the generation's exclusive cut. Across the complete CSV, cumulative
accepted, decoded, and source deltas converge; terminal SDK quiescence gives
exact accepted/decoded and per-source conservation.

The full-universe generation validation rate was 49,943.114 instrument rows/s
over the measured process window, corresponding to one 49,947-row history
generation and one matching factor generation per second. This measures live
processing at the natural feeder rate. No overload was injected, so it is not
a saturation-throughput measurement.

## Latency and realtime history freshness

The table below reports 1,200 periodic cuts. The combined cut metric includes
the history barrier/freeze and full-universe factor publication. The current
probe does not split those two components into separate timers.

| Metric | Count | p50 | p90 | p95 | p99 | p99.9 | Max | Mean |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Normal history cut + full-universe factor publication | 1,200 | 9.912 ms | 12.370 ms | 13.807 ms | 20.532 ms | 32.664 ms | 34.616 ms | 10.315 ms |
| Cut timestamp to completed read | 1,200 | 9.916 ms | 12.374 ms | 13.811 ms | 20.537 ms | 32.668 ms | 34.625 ms | 10.320 ms |
| Atomic history acquire call | 38,400 | 0.049 us | 0.130 us | 0.180 us | 0.420 us | 0.650 us | 12.650 us | 0.069 us |
| Global newest head: local receive to read | 1,200 | 10.185 ms | 12.652 ms | 14.219 ms | 20.957 ms | 32.926 ms | 34.667 ms | 10.609 ms |
| Updated instrument heads: local receive to read | 4,928,454 | 313.518 ms | 802.087 ms | 903.647 ms | 979.747 ms | 1,007.228 ms | 1,032.493 ms | 369.499 ms |

The global-newest-head row answers whether the pipeline as a whole is caught
up. The updated-instrument-head row measures every instrument whose published
head advanced since the preceding one-second read. It intentionally includes
the wait within the configured one-second publication interval, so its p99
near one second is expected and is not the atomic acquire-call cost.

### Read timestamp minus data timestamp

The requested difference has two distinct clock domains:

1. `read CLOCK_MONOTONIC - record.recv_monotonic_ns` is a same-host,
   same-clock-domain freshness metric. It includes local callback/queue,
   decode, history routing, generation barrier/freeze, factor publication,
   and the configured publication cadence. These are the two receive-to-read
   rows above.
2. `read CLOCK_REALTIME - decoded exchange event_time_ns` is a signed
   end-to-end market age. It additionally contains exchange/feeder timestamp
   conventions, upstream transport/replay delay, and realtime-clock offset.

The signed event-time results were:

| Event-to-read metric | Count | p50 | p90 | p95 | p99 | p99.9 | Min | Max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Global newest head | 1,200 | 2,979.155 ms | 3,259.034 ms | 3,979.052 ms | 3,981.856 ms | 3,992.194 ms | 2,967.198 ms | 3,997.465 ms |
| Updated instrument heads | 4,928,454 | 3,449.344 ms | 3,980.896 ms | 4,117.890 ms | 4,980.520 ms | 4,987.082 ms | 2,967.198 ms | 5,002.129 ms |

All 1,200 global-head event-time differences were non-negative. The SDK trace
reported similarly signed and sized latency values throughout this run, while
its earlier-run trace reported negative values matching the earlier probe.
That cross-check strongly indicates that the approximately 3.2-second shift
already exists at or before SDK delivery and was not added by the history
reader. It still cannot distinguish true network/feed delay from clock offset
or timestamp semantics.

`timedatectl` could not access the system bus in the execution environment,
and neither `chronyc` nor `ntpq` was installed. Therefore no PTP/NTP offset or
synchronization claim is made. A one-way exchange-latency SLA needs timestamp
semantics and synchronized-clock evidence captured alongside the run.

## Comparison with the 10-minute baseline

All comparisons below use identical probe code/binary and the same percentile
rule. Rates are natural feeder arrival rates rather than controlled load.

| Metric | 10-minute baseline | 20-minute follow-up | Change |
|---|---:|---:|---:|
| Overall accepted rate | 28,354.769/s | 25,400.319/s | -10.420% |
| One-second accepted peak | 80,610 | 73,835 | -8.405% |
| Normal cut p50 | 9.645 ms | 9.912 ms | +2.770% |
| Normal cut p95 | 15.252 ms | 13.807 ms | -9.474% |
| Normal cut p99 | 20.339 ms | 20.532 ms | +0.950% |
| Normal cut maximum | 24.503 ms | 34.616 ms | +41.272% |
| Global recv-to-read p50 | 9.984 ms | 10.185 ms | +2.006% |
| Global recv-to-read p99 | 20.348 ms | 20.957 ms | +2.997% |
| Global recv-to-read maximum | 24.535 ms | 34.667 ms | +41.295% |
| Updated-head recv-to-read p50 | 269.721 ms | 313.518 ms | +16.238% |
| Updated-head recv-to-read p99 | 989.143 ms | 979.747 ms | -0.950% |
| Atomic acquire p99 | 0.450 us | 0.420 us | -6.667% |
| Global event-to-read p50 | -262.162 ms | 2,979.155 ms | +3,241.317 ms signed shift |

The lower 20-minute throughput is explained by a lower natural arrival rate
in the later market window and is not a processing ceiling. Average combined
cut latency changed from 10.379 ms to 10.315 ms, and global receive-to-read
mean changed from 10.626 ms to 10.609 ms. The p99 values stayed within 3% of
the earlier baseline. Rare maxima increased from roughly 24.5 ms to 34.7 ms
in the doubled observation window and must not be hidden, although they did
not cause message loss, backlog failure, or generation timeout. The updated
head p50 moved by 43.797 ms, while its p90/p95/p99 all improved slightly;
that distribution is also sensitive to when instruments update inside each
one-second generation.

The absolute exchange-event-time shift is qualitatively different: local
monotonic freshness remained stable while SDK/feed event-time age moved by
seconds. It should be investigated as an upstream/time-synchronization issue,
not treated as a cleaned-pipeline latency regression.

## Build, regression, and sanitizer evidence

After the live window, the Release build's complete registered suite was run:

```text
ctest --test-dir build-acceptance --output-on-failure -j2
```

Result: **22/22 passed**, zero failures, exit code 0. The direct deterministic
decoder fuzz smoke then reported **50,150 cases passed**.

The same commit also has earlier acceptance evidence for **13/13 current
production-chain ASan + UBSan tests passed** with
`ASAN_OPTIONS=detect_leaks=0`. LeakSanitizer itself is environment-blocked by
the execution environment's ptrace monitoring and is not claimed as passed.
ThreadSanitizer intermittently fails before `main` with
`ThreadSanitizer: unexpected memory mapping`; non-PIE retries allowed the four
concurrency-relevant tests to complete without race reports. This is
supporting evidence, not a clean full-suite TSan pass.

## Evidence and hashes

- `artifacts/cleaned_acceptance_20260724_20m_report.json` is the
  machine-readable terminal gate and aggregate distribution report. SHA-256:
  `515cc028750477e9ce285e5f340c8b92ea134783731ab9502358a1cbeef2c0e7`.
- `artifacts/cleaned_acceptance_20260724_20m_samples.csv` contains the 1,200
  periodic observations. SHA-256:
  `03b587109415f5d8dbc1e5eb65be4d6116538f59655e6800325a31247d1a397d`.
- [`tools/accept_realtime_pipeline.cpp`](../../tools/accept_realtime_pipeline.cpp)
  is the probe source. SHA-256:
  `9f0d91a62905000b3def09ffd44dcf0d5a4aecb89cd4cca70b5e0bf985b99de8`.
- `build-acceptance/accept-realtime-pipeline` is the executed Release binary.
  SHA-256:
  `c48ffd06f4e322f139fb0a9b959582da5265a5c229a48ea7d6c00d907e17cad2`.
- `/tmp/l2flow-cleaned-acceptance-20m-20260724.log` and
  `/tmp/l2flow-cleaned-acceptance-20m-20260724.trace.log` are the SDK logs.
  The normal log records subscription success and orderly thread shutdown;
  neither log contains a matched fatal/error/reject/drop/queue-full/
  disconnect/timeout/failure warning.

The JSON has `passed=true` and an empty `first_error`; the CSV contains exactly
1,201 lines (one header plus 1,200 samples), sequential generations 1 through
1,200, strictly increasing elapsed timestamps and cumulative counters, and a
final periodic cumulative accepted/decoded count of 30,481,901. The remaining
854 messages arrived before terminal SDK quiescence and are included in the
terminal accepted/decoded total of 30,482,755 and terminal generation 1,201.

## Boundaries and recommended follow-up

- No numerical latency/throughput SLA was supplied, so this report cannot
  convert observed values into a contractual performance pass/fail decision.
- Natural production traffic was used. No overload was injected, and the
  maximum sustainable throughput or queue-saturation point remains unknown.
- The combined cut timer does not separately attribute history freeze,
  factor computation, and factor publication. Per-stage latency requires new
  low-overhead monotonic instrumentation.
- Optional audit WAL was disabled to avoid contaminating the in-memory
  realtime baseline. WAL correctness passed unit tests, but no 20-minute live
  disk-WAL throughput/latency result is claimed.
- History V1 is bounded in-memory suffix retention, not archival storage.
- Absolute exchange-event age remains clock-domain contaminated. Capture
  source/exchange timestamp semantics plus PTP/NTP offset during the next run.
- The current runtime does not export every decoded field for an independent
  per-message comparison against feeder backup data. If field-by-field live
  reconciliation is mandatory, add a bounded side-effect-free decoded audit
  tap or an offline reader for the current owned-ingress WAL.
- The one ignored unsupported callback cannot be identified from current
  snapshot counters. Logging only its bounded message key would close that
  observability gap without changing the hot-path data contract.
