"""Run a small, transport-free Event/KLine V3 CDC example."""

from __future__ import annotations

from l2flow_realtime import (
    DerivedEvent,
    EventCdcApplier,
    EventMutation,
    EventMutationKind,
    EventOrderKey,
    EventUid,
    KLineBar,
    KLineBarKey,
    KLineCdcApplier,
    KLineMutation,
    KLineMutationKind,
)


def event(sequence: int) -> DerivedEvent:
    return DerivedEvent(
        EventUid(1, 3, sequence, 1),
        EventOrderKey(3, sequence),
        sequence,
        {"price_p6": 10_000_000 + sequence},
    )


events = EventCdcApplier((event(100), event(102)))
events.apply(
    (
        EventMutation(
            1,
            EventMutationKind.RANGE_REPLACE_BEGIN,
            transaction_id=1,
            replace_entire_instrument=True,
        ),
        EventMutation(
            2,
            EventMutationKind.RANGE_REPLACE_CHUNK,
            transaction_id=1,
            replacement_rows=(event(100), event(101), event(102)),
        ),
    )
)
assert [row.order_key.business_sequence for row in events.rows] == [100, 102]
events.apply(
    (
        EventMutation(
            3,
            EventMutationKind.RANGE_REPLACE_COMMIT,
            transaction_id=1,
        ),
    )
)

bar_key = KLineBarKey(1, 1_000, 34_200_000_000_000)
bars = KLineCdcApplier()
bars.apply(
    (
        KLineMutation(
            1,
            KLineMutationKind.UPSERT,
            bar_key,
            KLineBar(bar_key, 1, {"close_price_p6": 10_000_000}),
        ),
    )
)

print([row.order_key.business_sequence for row in events.rows])
print(bars.bars)
