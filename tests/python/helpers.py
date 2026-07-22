from __future__ import annotations

import numpy as np

from l2flow_factor import (
    BatchMetadata,
    CANONICAL_SNAPSHOT_DTYPE,
    CANONICAL_TICK_DTYPE,
    ConsumerAttachSpec,
    InputFamily,
    MdlBatchView,
)


STREAM_DAY = bytes.fromhex("00112233445566778899aabbccddeeff")
WRITER = bytes.fromhex("102132435465768798a9babcbddcedfe")
CLOCK = bytes.fromhex("11" * 32)
REGISTRY = bytes.fromhex("22" * 32)


def make_batch(
    family: InputFamily = InputFamily.TICK,
    *,
    count: int = 2,
    shard_id: int = 1,
    begin_cursor: int = 10,
    source_stream_id: int = 1002,
    trade_date: int = 20260722,
    origin_capture_date: int = 20260722,
    origin_stream_day_id: bytes = STREAM_DAY,
    clock_epoch_algorithm: int = 1,
    clock_epoch_digest: bytes = CLOCK,
    origin_source_writer_instance: bytes = WRITER,
    origin_source_generation: int = 4,
    canonical_generation: int = 7,
    registry_version: int = 5,
    registry_sha256: bytes = REGISTRY,
    batch_quality_flags: int = 0,
    watermark_set_id: int = 9,
    origin_wal_start: int = 100,
) -> tuple[MdlBatchView, BatchMetadata, ConsumerAttachSpec, np.ndarray]:
    dtype = CANONICAL_TICK_DTYPE if family is InputFamily.TICK else CANONICAL_SNAPSHOT_DTYPE
    records = np.zeros(count, dtype=dtype)
    header = records["header"]
    header["magic"] = 0x3145434D
    header["schema_version"] = 1
    header["event_type"] = family.canonical_event_type
    header["record_size"] = dtype.itemsize
    header["source_stream_id"] = source_stream_id
    header["connection_epoch"] = 3
    header["trade_date"] = trade_date
    header["quality_flags"] = 0
    header["shard_event_id"] = np.arange(1, count + 1, dtype=np.uint64)
    header["origin_ingress_sequence"] = np.arange(20, 20 + count, dtype=np.uint64)
    header["origin_wal_end_pos"] = np.arange(
        origin_wal_start, origin_wal_start + count, dtype=np.uint64
    )
    header["vendor_sequence_id"] = np.arange(30, 30 + count, dtype=np.uint64)
    header["recv_monotonic_ns"] = np.arange(1_000, 1_000 + count, dtype=np.int64)
    instrument_id = shard_id if shard_id else 16
    if instrument_id == 0:
        instrument_id = 16
    header["instrument_id"] = instrument_id
    header["market"] = 1
    max_wal = 0 if count == 0 else origin_wal_start + count - 1
    metadata = BatchMetadata(
        source_stream_id=source_stream_id,
        trade_date=trade_date,
        origin_capture_date=origin_capture_date,
        origin_stream_day_id=origin_stream_day_id,
        family=family,
        shard_id=shard_id,
        begin_canonical_cursor=begin_cursor,
        end_canonical_cursor=begin_cursor + count,
        max_consumed_origin_wal_end_pos=max_wal,
        observed_raw_durable_wal_pos=max_wal + 10,
        clock_epoch_algorithm=clock_epoch_algorithm,
        clock_epoch_digest=clock_epoch_digest,
        clock_epoch_label=99,
        origin_source_writer_instance=origin_source_writer_instance,
        origin_source_generation=origin_source_generation,
        canonical_generation=canonical_generation,
        registry_version=registry_version,
        registry_sha256=registry_sha256,
        batch_quality_flags=batch_quality_flags,
        watermark_set_id=watermark_set_id,
    )
    expected = ConsumerAttachSpec(
        source_stream_id=source_stream_id,
        trade_date=trade_date,
        origin_capture_date=origin_capture_date,
        origin_stream_day_id=origin_stream_day_id,
        family=family,
        shard_id=shard_id,
        clock_epoch_algorithm=clock_epoch_algorithm,
        clock_epoch_digest=clock_epoch_digest,
        origin_source_writer_instance=origin_source_writer_instance,
        origin_source_generation=origin_source_generation,
        canonical_generation=canonical_generation,
        registry_version=registry_version,
        registry_sha256=registry_sha256,
    )
    # Model the native consumer's read-only mmap.  A bytes-backed array cannot
    # be made writeable again by a retained Python alias after attach.
    frozen_records = np.frombuffer(records.tobytes(order="C"), dtype=dtype)
    return (
        MdlBatchView.attach(frozen_records, metadata, expected),
        metadata,
        expected,
        frozen_records,
    )
