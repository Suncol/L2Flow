# Cleaned branch 10-minute rerun: close-boundary failure and post-close diagnostic

## Verdict

This rerun does **not** pass the complete live acceptance gate.

Two evidence-preserving attempts were made with distinct output paths:

1. The intended live window connected at approximately 14:57:13 CST and
   observed all five production event kinds. At the 15:00 close it entered a
   fatal pipeline state after 182.015 seconds, recorded 25,302 rejected
   callbacks, and stopped after only 181 periodic generations. This is a real
   runtime acceptance failure.
2. A diagnostic restart connected at approximately 15:01:44 CST and completed
   the full 600.066-second window without rejection or fatal behavior. Its
   decode/history invariants and terminal retained-history scan completed, but
   the formal gate still failed because no Shenzhen order or transaction
   arrived after the market close. This is useful post-close stability
   evidence, not a replacement passing production-flow acceptance window.

An earlier sandbox-only launch failed before connecting with
`SDK Connect failed: Operation not permitted`. It did not enter the measured
window and is not counted as a product result. The formal attempts were rerun
with permission to connect to the local `127.0.0.1:9112` feeder.

The earlier passing 10-minute and 20-minute reports remain valid evidence for
their own windows, but this rerun adds a new close-boundary fatal defect that
must not be hidden by those earlier passes.

## Tested identity

| Item | Value |
|---|---|
| Branch | `refactor/decoupled-realtime-history-factor` |
| Commit | `bf4b04092fbb5f64c526f60c35ea8cfaf29a64cb` |
| Probe source SHA-256 | `9f0d91a62905000b3def09ffd44dcf0d5a4aecb89cd4cca70b5e0bf985b99de8` |
| Executed probe SHA-256 | `c48ffd06f4e322f139fb0a9b959582da5265a5c229a48ea7d6c00d907e17cad2` |
| SDK DSO SHA-256 | `85b69d495e4a9d2e212342c887138599bd913b7d7426f4241d2d12f7c012116a` |
| Registry canonical SHA-256 | `0f5d99120111308303f3b1d6e907dfb3164a78d160a11a09ad0c77e9ad03bf5a` |
| Registry entries | 49,947 |
| History workers | 4 |
| Generation interval | 1,000 ms |
| Generation timeout | 10,000 ms |
| History acquire repetitions | 32 per periodic generation |
| Maximum retained history | 64 records per instrument |
| Optional audit WAL | disabled |

The probe source and binary are identical to the earlier formal 10-minute and
20-minute runs. The local subscriber token was supplied through a temporary
mode-0600 regular file, was not placed in the process arguments, and was
deleted after both attempts.

## Attempt 1: close-boundary fatal

| Item | Result |
|---|---:|
| SDK connected | approximately 14:57:13 CST |
| Pipeline stopped | approximately 15:00:15 CST |
| Measured duration | 182,014,563,204 ns |
| `passed` | false |
| `first_error` | `pipeline became fatal during the measurement window` |
| Accepted | 1,145,275 |
| Decoded/submitted | 1,140,622 |
| Accepted minus decoded | 4,653 |
| Rejected | 25,302 |
| Ignored unsupported | 1 |
| Periodic generations | 181 |
| Last published generation | 181; no terminal generation 182 |
| Universe rows checked | 9,040,407 = 49,947 x 181 |
| Updated heads measured | 405,486 |
| Final retained records checked | 0; terminal publication/scan did not complete |
| All five event variants observed | yes |

Per-source admitted counts were:

| Source | Count |
|---|---:|
| Shanghai snapshot | 130,367 |
| Shanghai tick | 121,530 |
| Shenzhen snapshot | 59,679 |
| Shenzhen order + transaction | 833,699 |
| Total | 1,145,275 |

The final successful periodic samples show the close transition clearly. The
accepted delta rose from 30,459 at generation 180 to 51,129 at generation
181. At generation 181 the decoded delta was 50,403, already 726 behind the
accepted delta. The SDK trace then reported a roughly 17.5k message/s
five-second input rate immediately before the probe observed the fatal state.

Aggregate message rate alone does not explain the failure: the earlier
passing 10-minute run handled a one-second accepted peak of 80,610 without a
fatal transition. The close window also contained unusual exchange-event age
behavior and SDK-reported source latency increases, but those observations do
not identify the internal fatal cause.

Failure-window latency is diagnostic only because the run is incomplete:

| Metric | p50 | p99 | Max |
|---|---:|---:|---:|
| History cut + factor publication | 12.159 ms | 53.098 ms | 65.323 ms |
| Atomic history acquire | 0.040 us | 0.460 us | 0.929 us |
| Global head local receive-to-read | 14.262 ms | 59.005 ms | 73.938 ms |
| Updated-head local receive-to-read | 374.073 ms | 995.593 ms | 1,061.440 ms |
| Global exchange-event-to-read | 2.985 s | 44.116 s | 169.638 s |

The long exchange-event ages near the close are signed upstream/event-clock
observations, not same-host process latency. They may reflect stale source
events, timestamp semantics, or clock offset and must not be used to assign
the runtime fatal cause.

### Missing fatal attribution

The current machine report serializes only the generic `first_error` above.
`RealtimePipelineSnapshotV1` contains `last_decode_error`, but the acceptance
probe does not serialize it. The internal callback/ingress error is also not
available in the snapshot report, and history-submit/queue failures are
collapsed into the same fatal state. Consequently, the preserved JSON cannot
distinguish among:

- a specific close-event decoder rejection;
- decoder queue full;
- owned-ingress construction failure;
- history submission/queue failure;
- another internal fatal transition.

This is a concrete observability defect. It prevents root-cause attribution
from the acceptance artifact even though the acceptance failure itself is
unambiguous.

## Attempt 2: complete post-close window

| Item | Result |
|---|---:|
| SDK connected | approximately 15:01:44 CST |
| SDK stopped | approximately 15:11:44 CST |
| Measured duration | 600,066,257,495 ns |
| `passed` | false |
| `first_error` | `final duration/count/state/five-kind acceptance gate failed` |
| Accepted | 161,252 |
| Decoded/submitted | 161,252 |
| Rejected | 0 |
| Ignored unsupported | 1 |
| Periodic generations | 600 |
| Terminal generation | 601 |
| Universe rows checked | 29,968,200 = 49,947 x 600 |
| Updated heads measured | 160,620 |
| Final retained records checked | 148,047 |
| CSV samples | 600 sequential periodic rows |

Event coverage was:

| Event | Seen |
|---|---|
| Shanghai snapshot | yes |
| Shanghai tick | yes |
| Shenzhen snapshot | yes |
| Shenzhen order | no |
| Shenzhen transaction | no |

The source counts make the missing coverage explicit:

| Source | Count |
|---|---:|
| Shanghai snapshot | 116,771 |
| Shanghai tick | 3,431 |
| Shenzhen snapshot | 41,050 |
| Shenzhen order + transaction | 0 |

The post-close runtime therefore completed the requested time duration and
history work but cannot satisfy the live five-kind DecodeMarket gate. Its
observed throughput was only 268.724 messages/s and must not be compared with
the earlier intraday natural-load rate as capacity evidence.

Post-close latency was:

| Metric | p50 | p99 | Max |
|---|---:|---:|---:|
| History cut + factor publication | 10.521 ms | 17.363 ms | 47.778 ms |
| Atomic history acquire | 0.050 us | 0.430 us | 6.721 us |
| Global head local receive-to-read | 30.999 ms | 85.566 ms | 185.401 ms |
| Updated-head local receive-to-read | 528.132 ms | 994.178 ms | 1,023.071 ms |
| Global exchange-event-to-read | 3.939 s | 3.946 s | 3.976 s |

All 600 generations had the exact registry universe and matching
factor/history publication contract. Accepted and decoded counters converged
exactly before terminal generation 601; the final retained suffix scan
completed. The SDK normal and trace logs contained no matched
fatal/error/reject/drop/queue-full/disconnect/timeout/failure warning.

## Evidence and hashes

Close-boundary fatal attempt:

- `artifacts/cleaned_acceptance_20260724_10m_rerun_1455_report.json`:
  `9b1da3450b01bc73fd76d7c6612bf3db1e546e94fd1871840a0f5c9d644e6f65`.
- `artifacts/cleaned_acceptance_20260724_10m_rerun_1455_samples.csv`:
  `77528fe58a44608a0c86cee5b096c24458b6e2871add31857cef3a7e8747a687`.
- `/tmp/l2flow-cleaned-acceptance-10m-rerun-20260724-1455.log`:
  `a6eaecd4ce400768891826cf3573cf1d11c8ce83ecfdc2e3acd26d78d9381df5`.
- `/tmp/l2flow-cleaned-acceptance-10m-rerun-20260724-1455.trace.log`:
  `cbb9f4d3ac5ef7e76b972150e3cba901568ceb21c037e4cb426c5191fcbd9410`.

Complete post-close diagnostic attempt:

- `artifacts/cleaned_acceptance_20260724_10m_rerun_1501_report.json`:
  `66fb2349695502ece3a6113615be8578ef08230c38b8a1b510af5176a299dce0`.
- `artifacts/cleaned_acceptance_20260724_10m_rerun_1501_samples.csv`:
  `9ca181502f7037af27994285ed771fe4a2fcb14f83aa8a5c71772d150192e859`.
- `/tmp/l2flow-cleaned-acceptance-10m-rerun-20260724-1501.log`:
  `b4b779ca94aa26995851b842c028c084b86dc7d014b51a14866a3d8233e0e611`.
- `/tmp/l2flow-cleaned-acceptance-10m-rerun-20260724-1501.trace.log`:
  `18f6f9d7c08042f418ab2b7f003383b94c4d4df2128813a7774e4896a56647ab`.

The first CSV contains one header plus 181 sequential samples. The second
contains one header plus 600 sequential samples. Both have strictly
increasing elapsed timestamps and generation numbers.

## Required follow-up

1. Extend the fatal snapshot/report with the exact ingress error,
   `last_decode_error`, history submission error, failing source slot,
   admitted source/global sequence, and a bounded message-key/error ring.
2. Reproduce the 15:00 close path using a captured owned-ingress/WAL prefix or
   an equivalent binary feeder replay. The current CSV feeder backup is not a
   field-complete substitute for the owned binary ingress body.
3. Add a dedicated close-auction acceptance case. Aggregate throughput alone
   is insufficient because the failure appeared at a lower rate than an
   earlier passing peak.
4. Run the next formal 10-minute all-five-kind window during active trading,
   while retaining the dedicated close-boundary run as a separate mandatory
   gate.
5. Continue treating `CLOCK_REALTIME - exchange event_time` as clock-domain
   contaminated until source timestamp semantics and PTP/NTP offsets are
   captured alongside the run.
