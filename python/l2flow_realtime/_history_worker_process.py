"""Executable child process for isolated Wire V2 tick-delta scans."""

from __future__ import annotations

import mmap
import os
import select
import socket
import sys
import time
from typing import Optional

from ._history_worker_protocol import (
    CONTROL_PAYLOAD_BYTES,
    NO_SLOT,
    NO_TIMEOUT_NS,
    SLOT_FLAG_DATA,
    SLOT_FLAG_EOF,
    WorkerOpcode,
    WorkerStatus,
    column_names,
    pack_control,
    pack_error_payload,
    parse_init_payload,
    parse_open_payload,
    publish_slot,
    recv_control,
    send_control,
    validate_ring_header,
)
from .checkpoint import InstrumentTickDeltaCheckpoint
from .instrument_delta import (
    InstrumentTickDeltaCursor,
    InstrumentTickDeltaSession,
    _open_instrument_tick_delta_session,
)
from .models import (
    ProtocolError,
    StaleSessionError,
    UnavailableError,
    WireFormatError,
)


_ZERO_PAYLOAD = b"\x00" * CONTROL_PAYLOAD_BYTES


def _status(error: BaseException) -> WorkerStatus:
    if isinstance(error, StaleSessionError):
        return WorkerStatus.STALE_SESSION
    if isinstance(error, UnavailableError):
        return WorkerStatus.UNAVAILABLE
    if isinstance(error, (ProtocolError, WireFormatError, ValueError)):
        return WorkerStatus.WIRE_FAILURE
    return WorkerStatus.INTERNAL_FAILURE


def _error(
    channel: socket.socket,
    request_id: int,
    error: BaseException,
) -> None:
    try:
        send_control(
            channel,
            pack_control(
                WorkerOpcode.ERROR,
                status=_status(error),
                request_id=request_id,
                payload=pack_error_payload(error),
            ),
        )
    except BaseException:
        pass


def _validate_simple_command(
    packet,
    opcode: WorkerOpcode,
    request_id: int,
) -> None:
    if (
        packet.opcode is not opcode
        or packet.status is not WorkerStatus.OK
        or request_id == 0
        or packet.request_id != request_id
        or packet.transfer_sequence
        or packet.slot_index != NO_SLOT
        or packet.record_count
        or any(packet.args)
        or any(packet.payload)
    ):
        raise ProtocolError(
            f"history worker {opcode.name} command is noncanonical"
        )


class _Scan:
    def __init__(
        self,
        *,
        channel: socket.socket,
        mapping: mmap.mmap,
        layout,
        result_column_mask: int,
        request_id: int,
        cursor: InstrumentTickDeltaCursor,
        session: InstrumentTickDeltaSession,
        transfer_sequence: int,
    ) -> None:
        self.channel = channel
        self.mapping = mapping
        self.layout = layout
        self.result_column_mask = result_column_mask
        self.request_id = request_id
        self.cursor = cursor
        self.session = session
        self.transfer_sequence = transfer_sequence
        self.outstanding: dict[int, int] = {}
        self.cancelled = False

    def _handle_control(self, *, block: bool) -> None:
        if not block:
            readable, _writable, _errors = select.select(
                (self.channel,), (), (), 0
            )
            if not readable:
                return
        packet = recv_control(self.channel)
        if packet.opcode is WorkerOpcode.RELEASE:
            if (
                packet.status is not WorkerStatus.OK
                or packet.request_id != self.request_id
                or packet.transfer_sequence == 0
                or packet.slot_index >= self.layout.slot_count
                or packet.record_count
                or any(packet.args)
                or any(packet.payload)
                or self.outstanding.get(packet.slot_index)
                != packet.transfer_sequence
            ):
                raise ProtocolError(
                    "history worker RELEASE does not own its slot"
                )
            del self.outstanding[packet.slot_index]
            return
        if packet.opcode is WorkerOpcode.CANCEL:
            _validate_simple_command(
                packet, WorkerOpcode.CANCEL, self.request_id
            )
            self.cancelled = True
            return
        raise ProtocolError(
            "history worker received an opcode during an active scan"
        )

    def _acquire_slot(self) -> tuple[int, int]:
        if self.transfer_sequence >= 0xFFFFFFFFFFFFFFFF:
            raise UnavailableError(
                "history worker transfer sequence is exhausted"
            )
        sequence = self.transfer_sequence + 1
        slot_index = (sequence - 1) % self.layout.slot_count
        while slot_index in self.outstanding:
            self._handle_control(block=True)
            if self.cancelled:
                raise InterruptedError
        self.transfer_sequence = sequence
        return sequence, slot_index

    def _publish_data(self, page, read_start: int, read_return: int) -> None:
        sequence, slot_index = self._acquire_slot()
        if self.cancelled:
            raise InterruptedError
        names = column_names(self.result_column_mask)
        columns = page.read_columns(*names)
        publish_slot(
            self.mapping,
            self.layout,
            slot_index=slot_index,
            flags=SLOT_FLAG_DATA,
            request_id=self.request_id,
            transfer_sequence=sequence,
            page_index=page.page_index,
            generation=page.generation.generation,
            record_count=page.record_count,
            cumulative_record_count=page.cumulative_record_count,
            first_ingress_sequence=page.first_ingress_sequence,
            last_ingress_sequence=page.last_ingress_sequence,
            first_tick_stream_sequence=(
                page.first_tick_stream_sequence
            ),
            last_tick_stream_sequence=page.last_tick_stream_sequence,
            cumulative_source_record_counts=(
                page.cumulative_source_record_counts
            ),
            worker_read_start_ns=read_start,
            worker_read_return_ns=read_return,
            worker_publish_begin_ns=0,
            result_column_mask=self.result_column_mask,
            columns=columns,
        )
        ring_publish_return_ns = time.monotonic_ns()
        self.outstanding[slot_index] = sequence
        send_control(
            self.channel,
            pack_control(
                WorkerOpcode.RESULT_READY,
                request_id=self.request_id,
                transfer_sequence=sequence,
                slot_index=slot_index,
                record_count=page.record_count,
                args=(ring_publish_return_ns, 0, 0, 0),
            ),
        )

    def _publish_eof(self, page, read_start: int, read_return: int) -> None:
        sequence, slot_index = self._acquire_slot()
        if self.cancelled:
            raise InterruptedError
        checkpoint = self.cursor.verified_checkpoint
        publish_slot(
            self.mapping,
            self.layout,
            slot_index=slot_index,
            flags=SLOT_FLAG_EOF,
            request_id=self.request_id,
            transfer_sequence=sequence,
            page_index=page.page_index,
            generation=page.generation.generation,
            record_count=0,
            cumulative_record_count=page.cumulative_record_count,
            first_ingress_sequence=0,
            last_ingress_sequence=0,
            first_tick_stream_sequence=0,
            last_tick_stream_sequence=0,
            cumulative_source_record_counts=(
                page.cumulative_source_record_counts
            ),
            worker_read_start_ns=read_start,
            worker_read_return_ns=read_return,
            worker_publish_begin_ns=0,
            result_column_mask=0,
            checkpoint_wire=checkpoint.to_wire(),
        )
        ring_publish_return_ns = time.monotonic_ns()
        self.outstanding[slot_index] = sequence
        send_control(
            self.channel,
            pack_control(
                WorkerOpcode.COMPLETE,
                request_id=self.request_id,
                transfer_sequence=sequence,
                slot_index=slot_index,
                record_count=0,
                args=(ring_publish_return_ns, 0, 0, 0),
            ),
        )

    def run(self) -> int:
        try:
            while not self.cursor.done:
                self._handle_control(block=False)
                if self.cancelled:
                    raise InterruptedError
                read_start = time.monotonic_ns()
                page = self.cursor.read_page()
                read_return = time.monotonic_ns()
                if page is None:
                    raise WireFormatError(
                        "delta cursor ended without an explicit EOF page"
                    )
                if page.eof:
                    self._publish_eof(
                        page, read_start, read_return
                    )
                    break
                self._publish_data(page, read_start, read_return)
        except InterruptedError:
            self.cancelled = True
        finally:
            try:
                self.cursor.close()
            finally:
                self.session.close()

        # A slot remains immutable until its exact RELEASE is received.  This
        # drain is also what makes reuse across consecutive commands safe.
        while self.outstanding:
            self._handle_control(block=True)
        if self.cancelled:
            send_control(
                self.channel,
                pack_control(
                    WorkerOpcode.CANCELED,
                    request_id=self.request_id,
                ),
            )
        return self.transfer_sequence


def _run(control_fd: int, ring_fd: int) -> int:
    channel = socket.socket(fileno=control_fd)
    mapping: Optional[mmap.mmap] = None
    request_id = 0
    try:
        mapping_bytes = os.fstat(ring_fd).st_size
        mapping = mmap.mmap(
            ring_fd, mapping_bytes, access=mmap.ACCESS_WRITE
        )
        layout, ring_column_mask = validate_ring_header(mapping)

        init = recv_control(channel)
        request_id = init.request_id
        if (
            init.opcode is not WorkerOpcode.INIT
            or init.status is not WorkerStatus.OK
            or init.request_id == 0
            or init.transfer_sequence
            or init.slot_index != NO_SLOT
            or init.record_count
            or any(init.args)
        ):
            raise ProtocolError("history worker INIT is noncanonical")
        (
            expected_session,
            timeout_ns,
            result_column_mask,
            control_path,
        ) = parse_init_payload(init.payload)
        if result_column_mask != ring_column_mask:
            raise ProtocolError(
                "history worker INIT/ring projections differ"
            )
        timeout = (
            None
            if timeout_ns == NO_TIMEOUT_NS
            else timeout_ns / 1_000_000_000
        )
        send_control(
            channel,
            pack_control(
                WorkerOpcode.READY,
                request_id=request_id,
                args=(
                    os.getpid(),
                    layout.slot_count,
                    layout.batch_capacity,
                    layout.total_bytes,
                ),
            ),
        )

        transfer_sequence = 0
        while True:
            command = recv_control(channel)
            request_id = command.request_id
            if command.opcode is WorkerOpcode.STOP:
                _validate_simple_command(
                    command, WorkerOpcode.STOP, request_id
                )
                send_control(
                    channel,
                    pack_control(
                        WorkerOpcode.STOPPED,
                        request_id=request_id,
                    ),
                )
                return 0
            if (
                command.opcode is not WorkerOpcode.OPEN
                or command.status is not WorkerStatus.OK
                or command.request_id == 0
                or command.transfer_sequence
                or command.slot_index != NO_SLOT
                or command.record_count
                or any(command.args)
            ):
                raise ProtocolError(
                    "history worker expected one canonical OPEN"
                )
            (
                instrument_id,
                requested_page_records,
                expected_generation,
                checkpoint_wire,
            ) = parse_open_payload(command.payload)
            if requested_page_records > layout.batch_capacity:
                raise ProtocolError(
                    "worker page size exceeds result slot capacity"
                )
            checkpoint = (
                None
                if checkpoint_wire is None
                else InstrumentTickDeltaCheckpoint.from_wire(
                    checkpoint_wire
                )
            )
            session = _open_instrument_tick_delta_session(
                control_path,
                expected_generation=expected_generation,
                expected_session=expected_session,
                timeout=timeout,
            )
            try:
                cursor = session.open_instrument(
                    instrument_id,
                    base_checkpoint=checkpoint,
                    requested_page_records=requested_page_records,
                )
            except BaseException:
                session.close()
                raise
            send_control(
                channel,
                pack_control(
                    WorkerOpcode.OPENED,
                    request_id=request_id,
                    args=(
                        cursor.generation.generation,
                        cursor.expected_record_count,
                        (
                            cursor.metadata.target_checkpoint
                            .instrument_tick_record_count
                        ),
                        (
                            cursor.generation
                            .history_published_monotonic_ns
                        ),
                    ),
                ),
            )
            transfer_sequence = _Scan(
                channel=channel,
                mapping=mapping,
                layout=layout,
                result_column_mask=result_column_mask,
                request_id=request_id,
                cursor=cursor,
                session=session,
                transfer_sequence=transfer_sequence,
            ).run()
    except EOFError:
        return 0
    except BaseException as error:
        _error(channel, request_id, error)
        return 1
    finally:
        if mapping is not None:
            mapping.close()
        channel.close()
        os.close(ring_fd)


def main(argv: list[str] | None = None) -> int:
    values = sys.argv[1:] if argv is None else argv
    if len(values) != 2:
        return 2
    try:
        control_fd = int(values[0], 10)
        ring_fd = int(values[1], 10)
    except ValueError:
        return 2
    if control_fd < 0 or ring_fd < 0 or control_fd == ring_fd:
        return 2
    return _run(control_fd, ring_fd)


if __name__ == "__main__":
    raise SystemExit(main())
