# L2 Mock design and compatibility contract

## 1. Purpose and scope

The L2 mock is an in-process source of high-volume synthetic market data for
applications written against the supplied DataYes C++ SDK. Its primary contract
is that production handlers can consume mock messages through the same public
interfaces and cast them to the same SDK payload structs.

The mock is not an exchange simulator, historical replay service, security
master, or source of investment data. “Full market” describes workload scale
and supported service coverage, not the factual identity of currently listed
instruments.

The SDK headers under `mdl_sdk_2_13_234/include` are authoritative for:

- service ID, service version, and message ID;
- C++ field names and integer widths;
- `#pragma pack(1)` payload layout;
- fixed-point decimal placement;
- `MDLAnsiString`, `MDLUTF8String`, and `MDLListT` offset semantics;
- the `IOManager`, `Subscriber`, `MessageHandler`, and `MDLMessage` interfaces.

`mock::SupportedMessages()` is authoritative for the subset the mock actually
generates.

## 2. Compatibility layers

### 2.1 API compatibility

Only manager construction changes:

```cpp
// Real feed
IOManagerPtr io = datayes::mdl::CreateIOManager(4);

// Local mock
IOManagerPtr io = datayes::mdl::mock::CreateIOManager(config);
```

Both branches then use:

1. `IOManager::CreateSubscriber()`;
2. `Subscriber::SubcribeMessage<T>()` or `mock::SubscribeAll()`;
3. `Subscriber::Connect()`;
4. the appropriate `MessageHandler::OnMDL...Message()` callback;
5. `MDLMessage::GetHead()` and `GetBody()`;
6. `IOManager::Shutdown()`.

The callback handler must outlive its connected callback activity and remain
alive until `AutoSubscriber::Stop()` or manager shutdown has synchronized that
activity. Shutdown is the synchronization boundary that stops generation and
releases ordinary-subscriber callback work.
Resetting an ordinary `SubscriberPtr` alone is not a disconnect operation,
because the SDK interface exposes no subscriber `Disconnect()` method.

Owner-thread `IOManager::Shutdown()` is synchronous and idempotent. If a handler
initiates shutdown from within a callback, teardown is handed to a coordinator
to avoid joining the current engine/worker thread. The owner must call
`Shutdown()` again before releasing callback-owned state.

### 2.2 Payload compatibility

The payload type comes directly from the vendor header. The mock must not
publish parallel “mock DTOs” that force production handlers onto a second code
path.

For example, an SH snapshot delivered through `OnMDLSHL2Message()` has:

```cpp
head->ServiceID      == mdl_shl2_msg::SHL2MarketData::ServiceID
head->ServiceVersion == mdl_shl2_msg::SHL2MarketData::ServiceVer
head->MessageID      == mdl_shl2_msg::SHL2MarketData::MessageID
```

and `GetBody()` points to a packed
`mdl_shl2_msg::SHL2MarketData` payload with valid relative offsets.

### 2.3 Semantic compatibility

The mock preserves structural and arithmetic invariants needed by parsers and
stateful consumers. It does not claim that code-valued fields or event
distributions reproduce an exchange protocol unless the caller supplies and
tests an authoritative mapping.

## 3. Data path and lifecycle

```text
Config
  └─> synthetic or caller-supplied universe
        └─> per-instrument state + seeded random stream
              └─> selected clock and rate limiter
                    └─> SDK-specific packed message builder
                          └─> subscriber filter
                                └─> bounded callback queue
                                      └─> MessageHandler callback
```

The intended lifecycle is:

```text
CreateIOManager
  -> CreateSubscriber
  -> add subscriptions
  -> Connect
  -> receive callbacks
  -> inspect Statistics
  -> Shutdown
```

`Subscriber::Connect()` is local in mock mode. It does not resolve a host,
authenticate a token, or open a socket. Calling `Shutdown()` is required even
when a test fails early.

`AutoSubscriber::Stop()` invalidates queued work from the disconnected
connection generation and waits only for callbacks already running in that
generation. A subsequent `Start()` creates a new generation, so old queued
messages cannot replay into the restarted session and a concurrent Stop does
not wait for new-session callbacks.

`Subscriber::GetSubscription()` follows the SDK 2.13.234 JSON contract:
`null\n` when empty; otherwise an array of `sid`/`mid` objects, with
`fieldname` and sorted, deduplicated `fieldvalues` for a field filter. Explicit
`SyncPublish()` and `AsyncPublish()` calls extract the supported identifier from
the copied payload before applying those filters, just as generated messages
do.

## 4. Universe model

### 4.1 Synthetic full-market-scale universe

When `Config::instruments` is empty, `MakeSyntheticUniverse()` creates a
deterministic stress universe from:

- `shanghai_instruments`;
- `shenzhen_instruments`;
- `derivatives_per_market`;
- `include_derivatives`.

Synthetic Shanghai and Shenzhen identifiers are six-character workload keys.
They are deliberately not documented as listed securities. Derivative
identifiers are likewise synthetic and carry no contract-month, expiry,
underlying, or listing claim.

The same function arguments produce the same universe. `Config::seed` controls
the random event stream and market-state evolution rather than asserting that a
synthetic identifier maps to a real instrument.

### 4.2 Caller-supplied authoritative universe

When `Config::instruments` is non-empty, it replaces all automatically generated
instruments. Synthetic count fields no longer add instruments.

Each input row must provide:

| Field | Unit and responsibility |
| --- | --- |
| `service_id` | The matching DataYes L2 service ID |
| `security_id` | Exact identifier supplied by the caller's dated master |
| `reference_price_milli` | Positive reference price in thousandths |
| `tick_size_milli` | Positive tick size in thousandths |
| `lot_size` | Positive quantity unit |
| `option` | Payload-family selector; not proof of a valid listed option |

The caller owns provenance. The mock does not query a security master, infer
listing status from an identifier prefix, update corporate actions, or validate
an option chain.

## 5. Time model

All SDK time fields use `MDLTime`, whose raw representation is `hhmmssmmm`.
Generated values must pass `MDLTime::IsValid()`; decimal milliseconds are not a
Unix timestamp.

### 5.1 Realtime

`ClockMode::Realtime` derives event time from the local wall clock and subtracts
a random lag in the inclusive range configured by
`max_realtime_lag_ms`. This is a controlled ingestion-lag stimulus, not a claim
about exchange latency.

Different random lags can make adjacent payload event times equal or slightly
out of order. Tests that require strict ordering should use sequence fields, or
use simulated mode with a single callback thread.

The pacing epoch is reset whenever an event has no matching subscriber. Time
spent without a match is therefore not accumulated as pacing credit, so adding
a matching subscription after an idle interval cannot trigger a catch-up
burst.

### 5.2 Simulated trading day

`ClockMode::SimulatedTradingDay` initializes at `simulated_start_time` on the
explicitly configured Gregorian weekday `simulated_start_date`. Before each
event, including the first, it advances by a seeded millisecond step between
`simulated_min_step_ms` and `simulated_max_step_ms`. It skips Saturday and
Sunday when a day rolls over.
The built-in stress timeline covers:

- `09:30:00.000` through `11:30:00.000`;
- `13:00:00.000` through `15:00:00.000`;
- the interval between them is skipped.

These windows are a deterministic A-share-like load-test clock. They are not an
authoritative exchange calendar: it knows Gregorian dates and weekends, but not
exchange holidays. It must not be applied as factual session rules for
derivatives, auctions, holidays, night sessions, or exceptional trading days.

A zero minimum step permits multiple messages to share one timestamp. For full
run reproducibility, keep the seed, configuration, universe, subscriptions, and
callback thread count fixed; use one callback thread when callback order itself
is part of the assertion. Exact random sequences are guaranteed for the same
mock build and C++ standard-library implementation; the C++ standard does not
require different libraries to implement distribution-to-engine mapping
identically.

## 6. Fixed-point numeric representation

`Instrument::reference_price_milli` and `tick_size_milli` use a scale of 1,000.
SDK payload fields use the scale declared in their type:

```text
raw_value(MDLDoubleT<N>) = price_milli × 10^N / 1000
raw_value(MDLFloatT<N>)  = price_milli × 10^N / 1000
```

For a synthetic price of `12.340`:

| SDK field | `m_Value` |
| --- | ---: |
| `MDLFloatT<3>` | `12340` |
| `MDLDoubleT<4>` | `123400` |
| `MDLDoubleT<6>` | `12340000` |

Builders operate on integers. They do not assign a C++ `double` directly to
`m_Value`, and they must reject or safely handle:

- a value that cannot be represented exactly at the destination scale;
- overflow of the destination `int32_t` or `int64_t`;
- the reserved `Null()` sentinel;
- multiplication overflow when computing quantity or turnover.

Price, turnover, ratios, and quantities must each use the decimal placement and
integer width declared by their own SDK field. A raw price value must not be
copied into a differently scaled money field.

Configuration validation also requires at least one whole lot to fit at the
maximum generated price in the tightest supported cumulative-turnover
representation for that service (SH scale 5, SZ scale 4, and the reviewed
derivatives scale-3 layouts).

## 7. Packed strings and lists

The vendor types are packed with `#pragma pack(1)`. Variable-size members do not
own pointers:

- `MDLAnsiString` and `MDLUTF8String` store `Length` and `Offset`;
- `MDLListT<T>` stores `Length` and `Offset`;
- an offset is relative to the address of that string or list field, not to the
  start of the message;
- nested lists apply the same rule from each nested list field.

Conceptually, one message owns a fixed header plus one contiguous body/tail
allocation:

```text
[MDLMessageHead]  ->  [packed fixed body] [level items] [nested orders] [string data]
```

The SDK exposes the two regions separately through `GetHead()` and `GetBody()`;
it does not require the header bytes to be adjacent to the body. Every builder
must calculate body-relative field offsets after final storage placement, set
`HeadSize` and `MessageSize` consistently, and keep both regions alive through
the callback.

Consumers should use `std_str()`, `Length`, and `MDLListT` accessors rather than
caching raw pointers. A pointer returned by `GetBody()`, `c_str()`, or
`operator[]` is callback-scoped. Retain `message->Copy()` when processing must
continue asynchronously after the callback returns.

## 8. Generated-data invariants

The following are the mock's structural correctness contract, independent of
whether values resemble a particular live market:

### 8.1 Instrument and price grid

- reference price, tick size, and lot size are positive;
- non-null prices are positive and representable in their declared SDK type;
- executable, OHLC, limit, order, and book-level prices remain aligned to the
  instrument tick; correctly calculated weighted/average prices may fall
  between ticks;
- quantities are non-negative and use the configured lot unit where the SDK
  field represents tradable quantity.

### 8.2 Order book

- bid levels are strictly descending by price;
- ask levels are strictly ascending by price;
- best bid is strictly lower than best ask;
- list lengths do not exceed configured `book_depth`;
- nested order lists do not exceed `orders_per_level`;
- where a payload exposes both level volume and constituent orders, their
  quantities reconcile;
- total bid/ask quantities reconcile with emitted levels where those totals are
  present.

### 8.3 Snapshot and cumulative fields

- `LowPrice <= OpenPrice <= HighPrice`;
- `LowPrice <= LastPrice <= HighPrice`;
- cumulative trade count, volume, and turnover never decrease within one
  generated trading-day state;
- when another whole lot cannot fit in both SDK cumulative fields, the mock
  stops accepting additional quantity into both volume and turnover; it never
  lets the two totals saturate independently;
- if no further lot is representable, explicit trade payloads for that
  instrument are suppressed until daily rollover; derivative snapshots remain
  available and use zero for a `LastVolume` field when no new lot was accepted;
- simulated date rollover resets daily OHLC/cumulative state while stream and
  order identifiers remain monotonic;
- turnover uses a price-times-quantity calculation at the field's declared
  scale;
- sequence identifiers do not move backward within their defined stream.

### 8.4 Orders and trades

- order and trade quantities are positive;
- side selection and price generation are internally consistent with the
  synthetic book;
- order time, transaction time, update time, and message-local time are valid
  `MDLTime` values from the selected clock;
- `MDLMessageHead::SequenceID` is monotonic for the mock runtime, and generated
  64-bit order/application identifiers are monotonic per instrument;
- legacy 32-bit record/index fields are bounded by their SDK representation and
  are not promised to be globally unique across instruments or channels;
- where an SH or SZ trade payload contains bid/offer order references, generated
  references point to the preceding synthetic buy/sell orders for that
  instrument cycle.

These rules make the feed useful for state-machine and arithmetic tests. They
do not promise exchange matching priority, cancellation semantics, or a
historically realistic order-to-trade ratio.

## 9. Supported-message matrix

All supported L2 service versions are `101`. The current matrix contains 28
SID/version/MID combinations. The exact executable truth is always the vector
returned by `mock::SupportedMessages()`; a consumer should use that function
rather than assuming every struct in an SDK header is generated.

| Market | SID | Snapshot payloads | Order payloads | Trade payloads |
| --- | ---: | --- | --- | --- |
| Shanghai L2 | 4 | `mdl_shl2_msg::SHL2MarketData` (MID 4) | `mdl_shl2_msg::Order` (MID 19) | `mdl_shl2_msg::SHL2Transaction` (MID 3); `SHL2Transaction2` (MID 18) |
| Shenzhen L2 | 6 | `mdl_szl2_msg::MarketData` (MID 4); `Snapshot300111_v2` (MID 28); `Snapshot300111_v3` (MID 39) | `mdl_szl2_msg::Order` (MID 2); `Order300192_v2` (MID 33) | `mdl_szl2_msg::Trade` (MID 1); `Transaction300191_v2` (MID 36); `CombinedTick` (MID 53) |
| CFFEX L2 | 21 | `mdl_cffexl2_msg::Future` (MID 1); `Option` (MID 2) | — | — |
| SHFE L2 | 22 | `mdl_shfel2_msg::CTPFuture` (MID 1); `CTPOption` (MID 2); `CrudeFuture` (MID 3); `CrudeOption` (MID 4) | — | — |
| CZCE L2 | 23 | `mdl_czcel2_msg::CTPFuture` (MID 1); `CTPOption` (MID 2) | — | — |
| DCE L2 | 24 | `mdl_dcel2_msg::Future` (MID 1); `Option` (MID 2) | `mdl_dcel2_msg::FutureOrder` (MID 7); `OptionOrder` (MID 8) | — |
| GFEX L2 | 26 | `mdl_gfexl2_msg::Future` (MID 1); `Option` (MID 2) | `mdl_gfexl2_msg::FutureOrder` (MID 7); `OptionOrder` (MID 8) | — |

`CombinedTick` is structurally supported, but the exchange business meaning of
its `Type` field is not inferred by the mock. Likewise, the DCE/GFEX order
payloads are structurally valid aggregate order queues, not claims about
exchange queue-priority behavior.

The matrix can be inspected without starting generation:

```cpp
for (const mock::SupportedMessage& message : mock::SupportedMessages()) {
    std::printf("SID=%u VER=%u MID=%u TYPE=%s\n",
                static_cast<unsigned>(message.key.service_id),
                static_cast<unsigned>(message.key.service_version),
                static_cast<unsigned>(message.key.message_id),
                message.cpp_type.c_str());
}
```

Messages not returned by this API are not silently approximated with another
payload type. Subscribing to an unsupported SDK message must not cause a body of
the wrong type to be delivered under that message ID.

## 10. Business-code boundary

The supplied SDK structs contain code-valued fields but do not include complete,
versioned exchange code tables for all of them. For that reason:

- `Config::trading_phase_code`, `mock_order_type`, and `mock_exec_type` are
  explicitly mock-local test tokens;
- defaults such as `"T"` and `0` are stable test values, not claims about an
  exchange-defined phase, order type, or execution type;
- caller overrides are passed as test inputs; the mock cannot certify their
  meaning;
- numeric `Side`, `DataStatus`, `ImageStatus`, stream/source identifiers, buy/
  sell flags, and similar fields are only structurally populated unless their
  mapping is explicitly documented by an authoritative source.

Consumers must not use default mock codes to validate:

- continuous-auction versus call-auction state;
- add, cancel, replace, or trade execution semantics;
- aggressor side;
- market, limit, best-price, or special order classification;
- suspension, delisting, price-limit, or end-of-day state;
- option exercise, expiry, margin, or Greek semantics.

If those behaviors are under test, load a dated authoritative code table and
build a scenario-specific adapter or fixture. Do not silently turn a mock-local
token into a production enum.

## 11. Concurrency, backpressure, and observability

The callback queue is bounded by `callback_queue_capacity`.

- `BackpressurePolicy::Block` preserves work at the queue boundary by slowing
  generation when consumers are slow.
- `BackpressurePolicy::DropNewest` rejects the newest callback when the queue is
  full and increments `Statistics::dropped`.

`Statistics` separates generated, filtered, delivered, dropped, and failed
callbacks. `queue_high_watermark` supports capacity tuning, while
`active_subscribers` supports lifecycle assertions.

`generated` and `filtered` count source events. `delivered` and `dropped` count
per-subscriber callback work, so fan-out means `delivered + dropped` can exceed
`generated`. Subscriber stop or runtime shutdown can cancel callback work that
was already queued. Tests should therefore not infer a universal accounting
identity while subscription state is changing.

One liveness exception applies to re-entrant publication: when a callback
publishes asynchronously back into a queue that is already full, even `Block`
rejects that newest callback work and increments `dropped`. Blocking a callback
worker on its own full queue can otherwise deadlock every worker. Normal
generator and owner-thread `Block` enqueue remains lossless at the queue
boundary.

`AsyncPublish()` and `AsyncResponse()` always enter this queue, including for a
subscriber created with `multithread_callback=false`. The flag controls callback
concurrency, not whether an asynchronous API invokes inline.

With multiple callback threads, a handler must synchronize shared state.
Deterministic random generation does not imply deterministic OS thread
scheduling. Tests that compare exact callback sequences should use one callback
thread and blocking backpressure.

With `multithread_callback=false`, one runtime-wide recursive gate serializes
all such subscribers even when generator and explicit publish calls originate
on different threads. The shared gate preserves synchronous re-entry while
preventing two serialized subscribers from deadlocking through cross-published
`SyncPublish()` calls.

## 12. Explicit non-goals

The mock makes no claim of:

- live, licensed, or delayed exchange data;
- a current or complete instrument master;
- authentic exchange sessions or holiday calendars;
- realistic cross-instrument correlation or volatility;
- official tick-size, lot-size, price-limit, or corporate-action rules unless
  supplied by the caller;
- full order-book matching, queue priority, cancel/replace behavior, or auction
  uncrossing;
- realistic derivative night sessions, settlement, open interest, margins, or
  option chains;
- measuring network or exchange latency.

It is an ABI-compatible stress source. Any assertion about market facts must
come from an external authoritative dataset, not from the generator defaults.
