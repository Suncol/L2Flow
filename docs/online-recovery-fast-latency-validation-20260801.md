# Online recovery FAST latency validation (2026-08-01)

## Scope

Validation was performed on branch `perf/online-recovery-fast-isolation`, based
on `6851ce6ec992`. At the time of this archived campaign, the measured patch
was confined to online CSV startup recovery and shared control/status helpers.
The literal
`live_ingress_capture_sink == nullptr` callback branch and the FAST/CERTIFIED
reader implementations were not changed. Native gap/data-loss backfill modules
were not part of that measured patch. Later worktree changes must be rebuilt
and rerun before these numbers are treated as their acceptance result.

The benchmark's mode name `ordinary` means the null-capture/no-recovery data
plane. Its synthetic harness starts the shared service with
`StartLivePartial()` and `coverage_from_open=false`; it does not instantiate
the production from-open `ACTIVE` control state. The callback and latest-reader
comparison remains valid because those measured data-plane branches are the
same null-capture branches, but this mode alone is not an end-to-end test of
from-open startup or control-state transitions.

The two questions measured separately were:

1. Does active CSV/shadow recovery add latency beyond an already-running online
   preview with its mandatory live journal (`active` versus `parked`)?
2. Does the common null-capture/no-online-recovery FAST data plane retain its
   existing behavior, and does the separate CERTIFIED benchmark still pass
   (`ordinary` and `benchmark_realtime_certified_v1 --all`)?

## Environment and method

- CPU: AMD EPYC 9354; node-0 physical CPUs `0-31`, without SMT siblings.
- Affinity: `taskset -c 0-31` for every scored run.
- Governor/boost: `schedutil`, boost enabled.
- Build: GCC 13.4, `Release`.
- Online workload: 128 warmup callbacks, 2,048 measured callbacks, eight pure
  latest reads per measured callback, and 50,000 Shenzhen transaction CSV
  records in recovery modes.
- Run order: one unscored `ordinary/parked/active` warmup, then five scored
  Latin-order rounds:
  `O,P,A | P,A,O | A,O,P | O,A,P | P,O,A`.
- Quantiles: nearest-rank within each run; the tables below report the median
  of the five run-level quantiles. Paired deltas are calculated within each
  round before taking their median.

The opt-in benchmark uses the real `MdlCsvStartupReplaySourceV1`, journal,
online handoff, SDK-less shadow pipeline, `LIVE_PARTIAL` Wire V2 service, and C
latest-tick reader. `active` releases CSV recovery immediately before the live
measurement loop; all 2,048 measured callbacks in every active run overlapped
recovery/promotion.

## FAST results

All values are nanoseconds.

| Mode | Metric | p50 | p95 | p99 | p999 |
| --- | --- | ---: | ---: | ---: | ---: |
| ordinary | callback call | 3,990 | 4,510 | 5,160 | 9,641 |
| parked | callback call | 6,170 | 8,110 | 10,940 | 19,580 |
| active | callback call | 2,990 | 5,510 | 7,420 | 17,410 |
| ordinary | callback origin to preview latest | 17,300 | 23,610 | 26,460 | 36,270 |
| parked | callback origin to preview latest | 17,100 | 23,600 | 31,210 | 52,700 |
| active | callback origin to preview latest | 12,830 | 20,370 | 25,290 | 40,410 |
| ordinary | pure latest read call | 150 | 151 | 190 | 250 |
| parked | pure latest read call | 150 | 180 | 230 | 290 |
| active | pure latest read call | 150 | 151 | 160 | 300 |

The paired `active - parked` median deltas for the directly exposed pure latest
read were `+1/+1/0/0 ns` at p50/p95/p99/p999. Across the five pairs, the p99
delta ranged from `-70` to `+20 ns`, and p999 from `-20` to `+40 ns`. This is no
material read-latency regression at the measured load.

For callback-origin-to-preview, active was non-worse in all five pairs at p50,
p95, and p99; the paired median deltas were `-5,250/-3,960/-8,090 ns`.
Run-level p999 had a paired median of `+2,509 ns` and a wide
`-64,069..+169,051 ns` range. With only 2,048 samples per run this is tail noise,
not evidence for a strict p999 improvement. A hard production p999 guarantee
still requires machine-level CPU/NUMA/I/O isolation and a longer target-host
acceptance run.

`parked - ordinary` isolates the mandatory online live-journal capture cost
between two `LIVE_PARTIAL` harness services with otherwise matching measured
FAST data-plane structure.
It increased callback-side latency, as expected, but its pure read median
deltas were `0/-1/0/+40 ns` at p50/p95/p99/p999. The capture cost is therefore
on the online writer path, not in the exposed latest-reader structure. The
corresponding production null-capture paths do not instantiate that journal.

## Recovery speed and completeness

Every run completed exact sequence and application-sequence validation:

| Mode | Successful runs | History | Promotion overlap | Median recovery | Median throughput |
| --- | ---: | ---: | ---: | ---: | ---: |
| ordinary | 5/5 | 2,176 / 2,176 | n/a | n/a | n/a |
| parked | 5/5 | 52,176 / 52,176 | after measurement | 357.63 ms | 145,891 records/s |
| active | 5/5 | 52,176 / 52,176 | 2,048 / 2,048 | 332.44 ms | 156,950 records/s |

Active recovery ranged from 147,574 to 194,454 records/s. Every recovery run
published 50,000 CSV records, consumed 2,176 journal suffix records, skipped
2,176 provably redundant suffix digests, and executed 256 parser checkpoints.
There were no replay/CERTIFIED/preview pause events at this load; active runs
used a median of 88 cooperative yields. One run observed a shadow high-water
pause. Thus the governor stayed work-conserving while retaining pressure gates.

## Null-capture FAST and separate CERTIFIED validation

`benchmark_realtime_certified_v1 --all` was run three times with the same CPU
set. All three runs passed FAST disabled/enabled, normal CERTIFIED, and gap
suffix contracts for 1, 32, 256, and 1,024 records. During every gap, FAST
latest and CERTIFIED last-good remained readable.

- Enabled FAST latest read p50/p95/p99:
  `150/160/240`, `150/160/200`, and `160/180/210 ns`.
- CERTIFIED latest read p50/p95/p99:
  `620/950/980`, `609/850/930`, and `570/890/910 ns`.

Together with the source-level boundary, these results verify the measured
null-capture callback/latest branch and the separately exercised CERTIFIED
reader. Source inspection shows that a normal pre-open run does not instantiate
the live journal and therefore does not execute the online governor, parser
checkpoints, journal sampling, or recovery status callbacks. The synthetic
`ordinary` mode itself must not be cited as proof of the production from-open
control-state lifecycle, because its service remains `LIVE_PARTIAL`.

## Functional and sanitizer validation

- Native GCC 13 Release full CTest: **45/45 passed**.
- `test_realtime_pipeline_lifecycle_concurrency`,
  `test_online_recovery_v1`, and `test_mdl_csv_startup_replay`: **100 repeated
  runs each passed**.
- ASan/UBSan focused suite for those three tests: **3/3 passed** with leak
  detection disabled for the test environment.
- TSan lifecycle concurrency test passed. The recovery/parser TSan binaries
  could not start in this host due `ThreadSanitizer: unexpected memory mapping`;
  building the full TSan application also encounters the repository's existing
  GCC `-Werror=tsan` diagnostics for `atomic_thread_fence` in IPC sources.

The session-local raw logs and summaries were retained as:

- `/tmp/l2flow_online_recovery_paired_20260801.log`, SHA-256
  `73366d01db5553e39b07a0bbe4a4a8b96fb56e913d0f3d9135f6d8f52050b0cc`;
- `/tmp/l2flow_online_recovery_paired_summary_20260801.tsv`, SHA-256
  `c198eb947fe2220e010c0281bf41ac82403aa23a831f4af206fcfd33f2119e13`;
- `/tmp/l2flow_certified_archival_20260801.log`, SHA-256
  `c461498a5b82bd7db0fe731db11b8e930eda96bb5c2e5462bbeb196fe98a7744`;
- `/tmp/l2flow_certified_archival_summary_20260801.tsv`, SHA-256
  `d0a63300ce2b849406cf47b41420bfeb76794ef4a742368b188f76f306ed67de`.
