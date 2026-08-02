# Startup-mode dataflow benchmark

`run_startup_mode_dataflow.py` compares two startup contracts with the same
FAST-only Pipeline, queues, workers, synthetic five-tuple mix, CPU affinity,
and serialized callback driver:

- `from_open`: `ACTIVE`, `coverage_from_open=true`;
- `live_partial_no_recovery`: `LIVE_PARTIAL`,
  `coverage_from_open=false`, process-start History enabled, and no journal,
  shadow Pipeline, recovery coordinator, Factor generation, KLine, or
  CERTIFIED service. Store-generation cuts remain enabled because History and
  generation delta require them.

The partial workload starts exchange-native tick sequences at `10,000,001` so
it cannot be mistaken for a market-open prefix. Process-local ingress/source
sequences still begin at one, as they do in a newly started process.

## Throughput metrics

The producer records a pacing epoch, then offers callback `i` (zero based) at
the one-based absolute deadline

```text
pacing_epoch + (i + 1) * 1e9 / target_rps
```

This makes `N` callbacks cover `N` target-rate intervals, including the first
interval before callback zero. Therefore

```text
offered_rps = invoked_callbacks * 1e9 / producer_elapsed_ns
```

where `producer_elapsed_ns` starts at the pacing epoch and ends after the last
callback invocation returns. This is a paced sustainable offered rate, not an
unthrottled maximum-capacity estimate. `target_met` requires at least 98% of
the requested rate, in addition to all integrity, health, and steady-state
conditions; it is not a claim that the measured rate is exactly the target.

The benchmark also publishes generations concurrently at the production
default 1,000 ms interval. After offering stops it publishes a final immutable
generation, so

```text
history_ready_rps = planned_callbacks * 1e9 /
                    history_ready_elapsed_ns
```

uses the same pacing epoch and includes the initial pacing interval, callback
offering, joining the periodic-cut thread, final backlog drain, the final
generation fence, and IPC publication. The subsequent integrity validation is
timed separately and is not included in either throughput metric.

`final_pipeline_drain_and_generation_cut_p95_ms` measures only the final
`StopAndPublishFinalGeneration` interval after the periodic-cut thread has
already been joined. It is not the complete offer-end-to-History-ready time.
`history_integrity_validation_p95_ms` measures the complete validation helper,
including local generation/watermark checks, IPC control/open work, cursor
creation, bitmap allocation, full record traversal, and EOF verification; it
is deliberately not labelled as scan time alone.

Every passing trial requires the final immutable generation to satisfy all of
the following:

```text
planned = invoked = accepted = decoded = applied = Store appended
        = immutable-generation records = scanned records
        = unique ingress sequences
```

Rejected/post-cut records, failed appends, queue-full events, duplicate or
out-of-range ingress sequences, invalid source slots, fatal states, and
coverage loss must all be zero. A bitmap over `[1, planned]` proves ingress
uniqueness; cardinality plus range and uniqueness proves there is no missing
ingress value. The five-tuple distribution is derived independently from the
requested workload and planned count; per-source scan counts must equal that
exact distribution. The History endpoint must report record-complete
generations, with the from-open flag present only in the `from_open` scenario.
Periodic cuts are required to publish successfully with contiguous generation
numbers, but the full bitmap traversal is applied to the final generation.

## Callback-to-Polars metrics

The latency workload is the established one-page, 4,096-tick, 55-column raw
generation delta plus a complete four-callback order lifecycle reduced to six
derived events. Each reported callback observation comes from sample zero of
a fresh process; the remaining repeated reads of the same generation are not
treated as independent callback-latency samples.

Throughput and latency are separate fresh-process workloads. No Python/Polars
reader is active during a throughput trial, so the latency distributions must
not be described as callback-to-Polars latency under the configured peak
throughput rate. The default throughput fixture binds 512 instruments
(256 per market), whereas the latency fixture binds 12,000; their results are
not one combined capacity experiment. The scenario comparison intentionally
matches deployed contracts: from-open retains Factor generation and standalone
partial disables it, so it is not a Factor-equal microbenchmark.

The primary strict origin is read immediately before invoking the synthetic
SDK callback. The endpoint publication timestamp and Polars-ready timestamp
use the same host/time namespace and `CLOCK_MONOTONIC` domain:

```text
strict callback-to-Polars
  = (generation publication - pre-callback boundary)
  + (Polars ready - generation publication)
```

For the order lifecycle, the strict-first origin is the first callback that
actually produces an order event (the second callback in the four-callback
lifecycle), not the lifecycle's initial status callback. The first-record
metric includes injection of the whole raw batch. The last-record
metric is the downstream latency for the batch tail. “Polars ready” means the
explicit EOF/checkpoint has been verified and the eager DataFrame, rechunk,
and validation aggregates have completed. The benchmark uses an immediate
forced generation cut to preserve comparability with the earlier
callback-to-Polars test; it does not include the residual wait to the next
production 1,000 ms periodic cut.

The record receive-stamp metric is retained for comparison with older reports,
but it starts inside callback processing after admission/inspection and is not
the primary end-to-end callback boundary.

## Run

```bash
.venv/bin/python benchmarks/run_startup_mode_dataflow.py \
  --binary build-online-final-native-gcc13/test_realtime_shared_service_v2 \
  --output-dir artifacts/startup-mode-dataflow \
  --cpu-list 8-15
```

The runner alternates scenario order, launches every trial in a fresh process,
checks the requested CPU affinity and scenario flags, independently recomputes
both rates from integer counts/times, verifies the binary, Python executable,
native library, and probe hashes remain stable, and writes raw logs plus
JSON/CSV summaries. The output directory must be empty. `summary.json` is
atomically published last and therefore acts as the campaign completion
marker.

## Scope boundary

The synthetic SDK calls the installed production message handler and exercises
the real Pipeline, decoder, Store, IPC, native history worker, Python client,
and Polars materialization. It does not include exchange networking, NIC,
vendor SDK receive/dispatch overhead, or cross-host clocks. Consequently it
proves no loss inside the tested callback-to-History prefix, not that an
external market-data feed cannot lose packets.

At rates where total tick callbacks exceed the 262,144-slot FAST ring, the
bitmap proof still establishes the immutable Store generation's complete
prefix. It does not prove that an independently slow FAST-ring consumer would
avoid an explicit overrun/gap condition.
