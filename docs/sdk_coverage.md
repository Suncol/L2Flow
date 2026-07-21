# DataYes L2 SDK coverage

This document prevents two different claims from being conflated:

- **ABI synchronization:** every generated body is the native packed struct from
  the bundled DataYes C++ SDK 2.13.234, with the SDK's SID, version, MID,
  fixed-point type, and relative string/list layout.
- **Schema coverage:** the mock intentionally generates a reviewed subset of
  the top-level message structs declared by the seven L2 headers.

The seven headers declare 109 unique `(SID, version, MID)` message structs. The
current core-load profile generates 28 (25.69%):

| Header/service | SDK structs | Generated | Coverage |
| --- | ---: | ---: | ---: |
| Shanghai L2 (SID 4) | 32 | 4 | 12.50% |
| Shenzhen L2 (SID 6) | 45 | 8 | 17.78% |
| CFFEX L2 (SID 21) | 2 | 2 | 100% |
| SHFE L2 (SID 22) | 4 | 4 | 100% |
| CZCE L2 (SID 23) | 7 | 2 | 28.57% |
| DCE L2 (SID 24) | 10 | 4 | 40.00% |
| GFEX L2 (SID 26) | 9 | 4 | 44.44% |
| **Total** | **109** | **28** | **25.69%** |

All 109 header declarations use service version 101, and no two declarations
share the same complete `(SID, version, MID)` key. The exact 28 generated keys
are listed in [the support matrix](mock_design.md#9-supported-message-matrix)
and returned at runtime by `mock::SupportedMessages()`.

## What the current profile covers

The generated subset is intended for high-throughput parser, book, callback,
queue, and state-machine tests:

- Shanghai stock snapshot, order, and both transaction layouts;
- Shenzhen legacy and newer stock snapshots, orders, transactions, and
  combined ticks;
- future and option snapshots for all five derivatives services;
- SHFE CTP and crude layouts;
- DCE and GFEX future/option aggregate order queues.

The default universe is therefore a **full-market-scale synthetic workload over
28 supported core streams**, not a claim that all 109 SDK schemas or all
security products are generated.

## Deliberately unsupported header schemas

The remaining 81 structs fall into three field-shape groups:

- 24 additional snapshot/order/transaction/auction shapes;
- 38 index, bond, ETF, option-L1, combination, volume/status-derived, and other
  product-specific real-time shapes;
- 19 parameter, status, reference-data, quota, market-info, and bulletin
  shapes.

Examples include Shanghai bond negotiation/distribution and fixed-income
messages, Shenzhen 300/309 template variants and bond/ETF messages, and
CZCE/DCE/GFEX combination and option-parameter messages.

`RegisterMessage()` returns `false` for these keys, and `SubscribeAll()` does
not silently register or publish a different body under an unsupported MID.
This is intentional: populating an unfamiliar schema with plausible-looking
zeros or invented business codes would be less safe than making the coverage
boundary executable.

Applications that require one of the other 81 schemas should add a
scenario-specific, header-reviewed builder and invariant test before treating
that key as supported. Control-plane/reference messages should normally be
emitted at startup or low frequency rather than mixed uniformly into the
high-rate core-load profile.
