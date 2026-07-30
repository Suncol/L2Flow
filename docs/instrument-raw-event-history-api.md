# Instrument Raw Event History API

## Scope

`InstrumentRawEventHistoryReader` is the supported Python interface for reading
one instrument's retained raw/normalized Wire V2 event history from source
slots 1 and 3:

- Shanghai tick events, including add, cancel, trade, and status actions;
- Shenzhen order and transaction events.

It does **not** return snapshot records or derived canonical
ORDER/TRADE/CANCEL lifecycle rows. It also does not invent orders or reorder
exchange events. Rows preserve the immutable Wire V2 event projection and its
source, ingress, and global tick sequence fields. This makes the API suitable
as deterministic replay input for an aggregator; the aggregator's derived
event history belongs to a separate query interface.

The older low-level `open_instrument_history`,
`open_instrument_tick_delta_session`, and
`open_instrument_tick_delta_worker` APIs remain available for compatibility.

## Native C and C++

The native API is delivered by the existing `l2flow_shm_reader` shared
library. The C ABI is declared in
`l2flow/ipc/realtime_shm_reader_c_v2.h` under the
`l2flow_instrument_raw_event_history_*_v2` prefix. The move-only C++ RAII
facade is declared in
`l2flow/ipc/instrument_raw_event_history_v2.h`.

The native reader maps each sealed service page once and returns a borrowed
contiguous span of `RealtimeWireTickPayloadV2`. It does not allocate one C++
object per row. The borrowed pointer/span is valid only until the next read
on that cursor or cursor close:

```cpp
l2flow_shm_session_info_v2 identity{};
// identity is obtained from l2flow_shm_reader_session_v2(...)

l2flow::ipc::InstrumentRawEventHistorySessionV2 history;
auto error =
    l2flow::ipc::InstrumentRawEventHistorySessionV2::Open(
        "/run/l2flow/control.sock",
        identity,
        0,       // pin the latest immutable generation
        1000,    // socket timeout in milliseconds
        &history);

l2flow::ipc::InstrumentRawEventHistoryCursorV2 read;
error = history.OpenFull(instrument_id, 4096, &read);
while (error ==
       l2flow::ipc::InstrumentRawEventHistoryErrorV2::kNone) {
    l2flow::ipc::InstrumentRawEventHistoryPageViewV2 page;
    error = read.ReadPage(&page);
    if (error !=
            l2flow::ipc::InstrumentRawEventHistoryErrorV2::kNone ||
        page.eof()) {
        break;
    }
    consume_before_next_read(page.records());
}

l2flow::ipc::InstrumentRawEventHistoryCheckpointV2 checkpoint{};
error = read.VerifiedCheckpoint(&checkpoint);
```

To roll forward, open a new session after a later immutable generation is
published and call `OpenUpdate(..., checkpoint, ...)`. Closing a cursor
before EOF fail-closes its stateful session and never verifies a checkpoint.
Session/cursor pairs are not concurrently callable; callers must serialize
open, read, status, reset, and destruction.

## Process and latency model

Opening the public reader starts one isolated CPython worker:

```python
history = client.open_instrument_raw_event_history(...)
```

That process alone receives raw history-page file descriptors and maps the
336-byte Wire V2 tick rows. The client process receives selected fixed-width
numeric columns through a bounded shared-memory result ring. Sequential calls
to `read_all` and `read_updates` reuse the same worker process; they do not
start a process for every generation.

Visibility is generation-bounded rather than per tick. Production
`--generation-interval-ms` currently defaults to `1000` and accepts
`1..60000`. Lowering it reduces the normal rolling-read wait while increasing
generation cut/publication frequency; it does not change this API into the
global live tick ring.

Only one read cursor may be active on a reader at a time. This preserves the
stateful service protocol and avoids hidden interleaving between instruments.
Reader, cursor, and batch methods are serial-only; do not call them
concurrently. Use another reader when scans must run concurrently.

`raw_event_columns` controls what the worker copies into each result slot.
The default is the complete supported numeric event schema. A latency-sensitive
consumer should request only the fields it uses:

```python
EVENT_COLUMNS = (
    "ingress_sequence",
    "tick_stream_sequence",
    "event_kind",
    "action",
    "side",
    "aggressor",
    "primary_order_id",
    "buy_order_id",
    "sell_order_id",
    "price_p6",
    "price_valid",
    "price_is_null",
    "quantity_raw",
    "quantity_scale",
    "quantity_valid",
    "quantity_is_null",
    "matched_quantity_raw",
    "matched_quantity_scale",
    "matched_quantity_valid",
    "matched_quantity_is_null",
)

with client.open_instrument_raw_event_history(
    raw_event_columns=EVENT_COLUMNS,
    ring_slots=4,
    batch_capacity=4096,
) as history:
    ...
```

The result ring is bounded. Each `InstrumentRawEventBatch` leases one slot.
Close the batch promptly, use it as a context manager, or iterate through
`cursor.batches()`, which releases every yielded batch automatically.
`read_columns()` copies selected columns into owned tuples. For the lowest
allocation overhead, `borrow_column()` exposes a zero-copy, context-bounded
sequence which becomes invalid when its `with` block ends.

## Full read

An instrument may be supplied as a session-scoped numeric ID or as an exact
opaque `InstrumentKey`:

```python
from l2flow_realtime import InstrumentKey, L2FlowClient

key = InstrumentKey(
    market=1,
    security_id_source=b"XSHG",
    security_id=b"600010",
)

with L2FlowClient.connect("/run/l2flow/control.sock") as client:
    with client.open_instrument_raw_event_history(
        raw_event_columns=EVENT_COLUMNS
    ) as history:
        with history.read_all(key) as read:
            for batch in read.batches():
                columns = batch.read_columns(
                    "ingress_sequence",
                    "action",
                    "primary_order_id",
                    "price_p6",
                )
                consume(columns)

            checkpoint = read.verified_checkpoint
```

`read_all` is a finite scan from the instrument's retained tick origin to one
pinned immutable generation. “All” means all records retained by this
runtime session, not an assertion that capture began at exchange open. A
stronger coverage claim is valid only after checking the verified
checkpoint's `coverage_from_open`, `record_coverage_complete`, and
`tick_record_coverage_complete` flags.

Numeric IDs are valid only in their `SessionIdentity`. Exact keys are resolved
against the reader's current observed session. An unobserved key raises
`InstrumentRawEventLookupError`; absence means “not observed in this
session,” not “the security does not exist.”

## Rolling updates

Every update is also a finite immutable-generation scan:

```python
with history.read_updates(key, checkpoint) as update:
    for batch in update.batches():
        consume(batch.read_columns(*EVENT_COLUMNS))
    checkpoint = update.verified_checkpoint
```

Call `read_updates` again after a later generation is published. The supplied
checkpoint is the exclusive base boundary; rows at that boundary are not
returned again. An empty update is valid: the first `read_batch()` returns
`None`, and the explicit terminal response still verifies the target
checkpoint.

The checkpoint is available only after the separate explicit EOF response has
reconciled:

- the expected and cumulative event counts;
- per-source event counts;
- instrument/session/day identity;
- target generation and successor relationship.

Reading the last data batch is not sufficient. Exhaust `batches()` or keep
calling `read_batch()` until it returns `None`. Closing early cancels the scan
and does not publish a checkpoint.

A checkpoint can be persisted outside this reader with `to_dict()` and restored
with `InstrumentRawEventCheckpoint.from_dict()`. This persistence records a
verified logical boundary only; it is not a raw-data WAL or crash-recovery
mechanism.

## Ordering

Rows in a read retain generation order and expose three distinct sequence
domains:

- `source_sequence`: per-source Store sequence;
- `ingress_sequence`: accepted local message order;
- `tick_stream_sequence`: dense global tick-ring order.

`vendor_sequence_id` and `native_event_sequence` retain their separate source
meanings when valid. Do not substitute one sequence domain for another.

The full read and every later update concatenate without overlap when each
next read uses the previous read's verified checkpoint. Within one session and
instrument, the checkpoint count difference equals the number of rows emitted
by the update.

## Lifecycle summary

```text
L2FlowClient
  └─ InstrumentRawEventHistoryReader       one reusable worker process
       ├─ read_all(...)                  finite origin → generation read
       │    └─ InstrumentRawEventReadCursor
       │         ├─ InstrumentRawEventBatch ...
       │         └─ explicit EOF → verified_checkpoint
       └─ read_updates(checkpoint, ...)
            finite checkpoint → generation read
            └─ InstrumentRawEventReadCursor
                 ├─ InstrumentRawEventBatch ...
                 └─ explicit EOF → next verified_checkpoint
```

Close batches before cursors, cursors before the reader, and the reader before
the client. Nested context managers implement that ownership order directly.
