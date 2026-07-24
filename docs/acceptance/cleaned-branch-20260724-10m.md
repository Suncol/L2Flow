# Cleaned branch DecodeMarket / realtime history / performance 10-minute acceptance

## Verdict

The cleaned realtime branch passed the functional and sustained-live-load
acceptance gates exercised here:

- the Release build completed with the repository's strict warnings enabled;
- all 22 Release CTest cases passed;
- all five production market event kinds were decoded during the live window;
- 17,015,310 market messages were accepted and exactly 17,015,310 were
  decoded/submitted before terminal publication;
- no message was rejected, no decoder/history runtime became fatal, and all
  600 periodic generations plus the terminal generation were published;
- all 49,947 registry instruments were present in exact registry order in
  every sampled generation;
- every sampled watermark satisfied the global-prefix/source-prefix identity;
- every factor publication retained the exact history-generation handle and
  had one row per registry instrument;
- final bounded history records passed instrument, source, source-cut,
  ingress-cut, ordering, latest-kind, and capacity checks.

No numerical SLA was supplied. The performance result is therefore an
observed production-load baseline and stability acceptance, not a claim that
the measured rates are the saturation ceiling or that a particular latency
budget has been contractually met.

The signed exchange-event-to-read measurements require an important caveat:
545 of the 600 global-head observations were negative. The local realtime
clock and the feed's exchange event timestamp are not demonstrated to be a
common synchronized one-way-latency clock domain. Those signed differences
are reported exactly as observed, but must not be relabelled as pure process
latency. The monotonic receive-to-read measurements are the authoritative
same-host process-freshness measurements in this report.

## Tested identity and configuration

| Item | Value |
|---|---|
| Branch | `refactor/decoupled-realtime-history-factor` |
| Commit | `a21adff23f33bf3410e09a676677da345772b0f1` |
| Commit subject | `refactor(realtime): replace legacy stack with one production pipeline` |
| Trade date | `20260724` |
| Approximate civil window | `2026-07-24 13:21:10` to `13:31:10` CST |
| Requested live window | 600 seconds |
| Measured process window | 600,086,355,589 ns |
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
| Maximum retained history | 64 records per instrument |
| Optional audit WAL | disabled for the live performance window |

The credential was read by the probe from a file and was not placed in the
command line, JSON, CSV, or this report.

## DecodeMarket acceptance

The cleaned `MarketDecoderV1` test suite exercised the five production wire
schemas and the following semantic/error groups:

- fixed lower bounds and service-version/schema gates;
- Shanghai tick action, side, order-reference, validity, and phase matrices;
- Shenzhen order enums, price applicability, quantity and reference domains;
- Shenzhen transaction trade/cancel/unknown-execution matrices;
- Shanghai and Shenzhen instrument identity/registry resolution gates;
- nested snapshot depth and best-queue parsing and public retention caps;
- malformed offsets, overlaps, counts, text and truncation paths;
- exact fixed-point normalization and overflow failure atomicity;
- null, zero, negative, and extreme absolute-price handling;
- null and negative aggregate/book/queue/tick quantity handling;
- exchange time, vendor local time, calendar bounds and fixed UTC+08 Unix
  projection;
- maximum-duration unavailable sentinel handling;
- product-specific field applicability not inferred from coarse security
  type;
- stateful Shanghai phase-map capacity and history behavior.

The normal regression suite also ran the deterministic decoder fuzz smoke:
all five message selectors, supported/unknown service-version modes, 15 body
length boundaries, and 50,000 deterministic pseudorandom bodies, for 50,150
total cases.

During the live 10-minute run, the production composition accepted and
successfully decoded/submitted 17,015,310 messages. Because any decoder error,
unknown registry instrument, invalid retained record, history submit error, or
queue-full condition closes the pipeline fatally, final equality plus
`fatal=false` and `rejected=0` is the live no-loss gate for this composition.

All five decoded event variants were observed:

1. Shanghai snapshot;
2. Shanghai tick;
3. Shenzhen snapshot;
4. Shenzhen order;
5. Shenzhen transaction.

Per-source totals were:

| Source slot | Production stream | Messages | Mean rate | Share |
|---:|---|---:|---:|---:|
| 0 | Shanghai snapshot | 508,020 | 846.578/s | 2.986% |
| 1 | Shanghai tick | 7,067,958 | 11,778.235/s | 41.539% |
| 2 | Shenzhen snapshot | 581,476 | 968.987/s | 3.417% |
| 3 | Shenzhen order + transaction | 8,857,856 | 14,760.969/s | 52.058% |
| **Total** | five production message keys | **17,015,310** | **28,354.769/s** | **100%** |

The live pipeline currently reports slot 3 as a combined source counter. The
acceptance probe independently observed both decoded variants, but the runtime
does not expose separate cumulative Shenzhen order/transaction totals.

## Instrument history acceptance

The unit and live tests together checked the following history contracts:

- permanent routing by `instrument_id % worker_count`;
- one serial owner per source and strictly increasing dense source sequence;
- cross-source row ordering by the process-wide ingress sequence, rather than
  decoder-worker arrival time;
- bounded suffix retention and correct latest snapshot/tick handles;
- marker/fence behavior when post-cut messages arrive behind only some source
  fences;
- all four source fences required before immutable generation publication;
- exact fixed registry universe, including empty instruments;
- atomic latest generation acquisition;
- factor publication retaining the exact matching history handle.

The 10-minute run provided the following live evidence:

| Check | Result |
|---|---:|
| Periodic generations sampled | 600 |
| Terminal generation | 601 |
| Universe rows checked | 29,968,200 = 49,947 × 600 |
| Updated instrument heads measured | 2,498,767 |
| History acquire calls timed | 19,200 = 600 × 32 |
| Final retained records fully checked | 466,284 |
| Prefix/watermark violations | 0 |
| Universe identity/order violations | 0 |
| Factor/history generation-handle violations | 0 |
| Final retained ordering/source/cut/capacity violations | 0 |
| Rejected messages | 0 |
| Fatal transitions | 0 |

The retained-record count is intentionally much smaller than the decoded
count. V1 keeps only the newest configured 64 records per instrument. The
accepted/decoded equality and generation prefix prove processing of the live
prefix; the final full scan proves correctness of the suffix that the bounded
history contract promises to retain.

## Throughput

Throughput was measured at the one-second generation sampling boundary.

| Metric | Min | p50 | p90 | p95 | p99 | Max | Mean |
|---|---:|---:|---:|---:|---:|---:|---:|
| Accepted messages/s | 17,254 | 26,203 | 39,419 | 46,243 | 57,883 | 80,610 | 28,356.48 |
| Decoded/submitted messages/s | 17,243 | 26,117 | 39,163 | 46,243 | 57,943 | 80,284 | 28,356.48 |
| Shanghai snapshot/s | 708 | 846 | 912 | 935 | 966 | 1,018 | 846.68 |
| Shanghai tick/s | 7,467 | 10,887 | 16,328 | 18,917 | 24,611 | 31,584 | 11,778.89 |
| Shenzhen snapshot/s | 221 | 597 | 2,076 | 2,113 | 2,184 | 2,217 | 969.04 |
| Shenzhen order + transaction/s | 8,365 | 13,388 | 21,412 | 25,486 | 33,345 | 47,803 | 14,761.88 |

Small accepted/decoded differences inside an individual one-second row are
legal because `Snapshot()` can include callbacks accepted immediately after
that generation's exclusive cut. Terminal SDK quiescence removed that
transient observation race: final accepted and decoded counters were exactly
equal.

The full-universe publication/read validation rate was 49,939.812 instrument
rows/s over the measured process window. This corresponds to one 49,947-row
history generation and one matching 49,947-row factor generation per second.

## Latency and realtime history freshness

All values below are nanoseconds in the machine-readable report and converted
to milliseconds or microseconds here. For the 600 normal periodic cuts, the
CSV excludes terminal SDK shutdown. The JSON `generation_cut_and_factor`
distribution contains one additional terminal stop-and-publish observation;
its maximum was 30.260 ms.

| Metric | Count | p50 | p90 | p95 | p99 | p99.9 | Max |
|---|---:|---:|---:|---:|---:|---:|---:|
| Normal history cut + full-universe factor publication | 600 | 9.645 ms | 13.647 ms | 15.252 ms | 20.339 ms | 24.503 ms | 24.503 ms |
| Cut timestamp to completed read | 600 | 9.648 ms | 13.654 ms | 15.258 ms | 20.344 ms | 24.509 ms | 24.509 ms |
| Atomic history acquire call | 19,200 | 0.040 µs | 0.140 µs | 0.210 µs | 0.450 µs | 0.650 µs | 10.109 µs |
| Global newest head: local receive to read | 600 | 9.984 ms | 13.761 ms | 15.393 ms | 20.348 ms | 24.535 ms | 24.535 ms |
| Updated instrument heads: local receive to read | 2,498,767 | 269.721 ms | 805.476 ms | 914.671 ms | 989.143 ms | 1,008.675 ms | 1,024.459 ms |

The two receive-to-read rows answer different questions:

- **Global newest head** chooses the greatest process ingress sequence in the
  generation. It shows whether the pipeline as a whole is currently caught
  up. Its p99 was 20.348 ms.
- **Updated instrument heads** measures every instrument whose published head
  advanced since the preceding one-second read. The distribution includes the
  deliberate publication wait within that one-second interval; a roughly
  uniform arrival pattern therefore naturally produces a p50 near 0.5 seconds
  or lower and a p99 near one second. The observed p50 was 269.721 ms and p99
  was 989.143 ms.

### Read timestamp minus data timestamp

The user-requested difference is reported in two explicitly separate clock
domains:

1. `read CLOCK_MONOTONIC - record.recv_monotonic_ns` is a same-host,
   same-clock-domain measure. It includes decoder queueing, decoding, history
   routing, generation barrier/freeze, factor publication, and the configured
   publication interval. These are the two receive-to-read rows above.
2. `read CLOCK_REALTIME - decoded exchange event_time_ns` is the signed
   end-to-end market-data age. It additionally includes the exchange/feeder
   timestamp convention, upstream transport, and any realtime-clock offset.

The signed exchange-event results were:

| Event-to-read metric | p50 | p90 | p95 | p99 | p99.9 | Min | Max |
|---|---:|---:|---:|---:|---:|---:|---:|
| Global newest head | -262.162 ms | -121.542 ms | 47.734 ms | 1,047.154 ms | 1,048.859 ms | -403.996 ms | 1,048.859 ms |
| Updated instrument heads | 46.813 ms | 1,046.981 ms | 1,048.514 ms | 2,045.765 ms | 2,051.787 ms | -404.001 ms | 2,062.647 ms |

There were 545 negative and 55 non-negative global-head samples. This proves
that the two realtime timestamp authorities cannot be assumed synchronized
from this test alone. The exact signed result is useful diagnostic evidence,
but a production one-way exchange-latency SLA requires separate PTP/NTP clock
offset evidence and a pinned interpretation of the exchange time field.

## Build, regression, and sanitizer evidence

Release build and test command:

```text
cmake -S . -B build-acceptance -DCMAKE_BUILD_TYPE=Release \
  -DL2FLOW_BUILD_TESTS=ON -DL2MOCK_BUILD_TESTS=ON
cmake --build build-acceptance -j2
ctest --test-dir build-acceptance --output-on-failure
```

Result: **22/22 passed**. This includes market decoder, deterministic decoder
fuzz smoke, registry, realtime
history, factor engine, owned ingress/WAL, SDK lifecycle, end-to-end realtime
pipeline, lifecycle concurrency, and isolated vendor-mock tests.

ASan + UBSan result: **13/13 current production-chain tests passed** after
setting `ASAN_OPTIONS=detect_leaks=0`. LeakSanitizer itself could not run under
the execution environment's ptrace monitoring and produced its documented
environment-level fatal error even for the CRC32C test. No LSan pass is
claimed.

TSan result: the current environment intermittently failed before `main` with
`ThreadSanitizer: unexpected memory mapping`. A non-PIE retry allowed the four
concurrency-relevant tests (`test_realtime_history`,
`test_realtime_factor_engine`, `test_realtime_pipeline`, and
`test_realtime_pipeline_lifecycle_concurrency`) to complete under TSan without
a race report. This is supporting evidence, not a clean full-suite TSan pass;
the remaining TSan executions are classified as environment-blocked.

## Evidence and hashes

- `artifacts/cleaned_acceptance_20260724_10m_report.json` is local acceptance
  evidence intentionally excluded from Git. It contains the machine-readable
  gate and percentile report. SHA-256:
  `ec3af689a07472b47285dee49dec09d95836fa45b09f658996a8223d968aa3b9`.
- `artifacts/cleaned_acceptance_20260724_10m_samples.csv` is local acceptance
  evidence intentionally excluded from Git. It contains the 600 one-second
  observations. SHA-256:
  `4dc9a918cf980b6d19d83e8a450ef2500dff43a7d376b06951be910a4f2941cf`.
- [`tools/accept_realtime_pipeline.cpp`](../../tools/accept_realtime_pipeline.cpp)
  contains the production-composition probe. SHA-256 at test time:
  `9f0d91a62905000b3def09ffd44dcf0d5a4aecb89cd4cca70b5e0bf985b99de8`.
- `build-acceptance/accept-realtime-pipeline` was the executed Release binary.
  SHA-256: `c48ffd06f4e322f139fb0a9b959582da5265a5c229a48ea7d6c00d907e17cad2`.

## Boundaries and follow-up risks

- This run tested sustained natural midday production load, including a
  one-second peak of 80,610 accepted messages. It did not inject overload to
  find the maximum sustainable rate or queue-saturation point.
- Optional audit WAL was disabled for the live latency run so storage behavior
  did not contaminate the realtime baseline. WAL ownership, failure isolation,
  and drain behavior passed unit tests, but no 10-minute live disk-WAL result
  is claimed.
- The cleaned runtime deliberately stores bounded in-memory history, not every
  live message forever. Full retained-suffix correctness was checked; this is
  not an archival-history retention test.
- The previously generated five-minute Raw/backup comparison in `artifacts/`
  is useful supplementary evidence, but it used the removed legacy Raw
  adapter/history validation stack and a five-minute window. It is not counted
  as the formal current cleaned-branch 10-minute result.
- The current live runtime does not export every decoded field for an
  independent per-message feeder-backup comparison. Decoder field semantics
  are covered by exhaustive binary-wire unit cases and the live no-error
  stream. If independent field-by-field live reconciliation is a mandatory
  acceptance criterion, the next change should add a bounded, side-effect-free
  decoded audit tap or an offline reader for the current owned-ingress WAL.
