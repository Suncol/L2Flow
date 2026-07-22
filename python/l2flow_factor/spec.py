from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import re
from typing import Any

import numpy as np

from .canonical import InputFamily
from .errors import ValidationError


class InputMode(Enum):
    TICK_ONLY = "TICK_ONLY"
    SNAPSHOT_ONLY = "SNAPSHOT_ONLY"
    RECEIVE_TIME_MUX = "RECEIVE_TIME_MUX"
    SNAPSHOT_ASOF_TICK = "SNAPSHOT_ASOF_TICK"
    LIVE_LATEST = "LIVE_LATEST"


class ClockSemantics(Enum):
    RECEIVE_MONOTONIC = "receive_monotonic"
    EXCHANGE_TIME = "exchange_time"


class GapPolicy(Enum):
    BLOCK = "BLOCK"
    INVALIDATE = "INVALIDATE"
    RESET_AND_WARMUP = "RESET_AND_WARMUP"


class EpochPolicy(Enum):
    BLOCK = "BLOCK"
    INVALIDATE = "INVALIDATE"
    RESET_AND_WARMUP = "RESET_AND_WARMUP"


class OutputCadence(Enum):
    EACH_INPUT = "each_input"
    EACH_TICK = "each_tick"
    EACH_SNAPSHOT = "each_snapshot"
    ON_TIMER = "on_timer"


class WindowKind(Enum):
    EVENT_COUNT = "event_count"
    EVENT_TIME = "event_time"


def _strict_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    if type(value) is not int or value < minimum or value > maximum:
        raise ValidationError(f"{name} must be an integer in [{minimum}, {maximum}]")
    return value


@dataclass(frozen=True, slots=True)
class InputSpec:
    source_stream_id: int
    family: InputFamily
    required: bool = True
    shard_id: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.family, InputFamily):
            raise ValidationError("family must be an InputFamily")
        if self.family is InputFamily.LATEST_STATE:
            _strict_int(self.source_stream_id, "source_stream_id", 0, 0)
        elif self.family not in (InputFamily.TICK, InputFamily.SNAPSHOT):
            raise ValidationError(
                "Phase 7 V1 factor inputs expose only tick, snapshot, or latest_state"
            )
        else:
            _strict_int(self.source_stream_id, "source_stream_id", 1, (1 << 32) - 1)
        if type(self.required) is not bool:
            raise ValidationError("required must be bool")
        _strict_int(self.shard_id, "shard_id", 0, (1 << 32) - 1)

    @property
    def key(self) -> tuple[int, str, int]:
        return (self.source_stream_id, self.family.value, self.shard_id)

    def canonical_object(self) -> dict[str, Any]:
        return {
            "family": self.family.value,
            "required": self.required,
            "shard_id": self.shard_id,
            "source_stream_id": self.source_stream_id,
        }


@dataclass(frozen=True, slots=True)
class WindowSpec:
    name: str
    kind: WindowKind
    size: int

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", self.name):
            raise ValidationError("window name must be a stable lowercase identifier")
        if not isinstance(self.kind, WindowKind):
            raise ValidationError("window kind must be a WindowKind")
        _strict_int(self.size, "window size", 1, (1 << 63) - 1)

    def canonical_object(self) -> dict[str, Any]:
        return {"kind": self.kind.value, "name": self.name, "size": self.size}


@dataclass(frozen=True, slots=True)
class Warmup:
    min_snapshots: int = 0
    min_ticks: int = 0
    min_events: int = 0

    def __post_init__(self) -> None:
        _strict_int(self.min_snapshots, "min_snapshots", 0, (1 << 32) - 1)
        _strict_int(self.min_ticks, "min_ticks", 0, (1 << 32) - 1)
        _strict_int(self.min_events, "min_events", 0, (1 << 32) - 1)

    def canonical_object(self) -> dict[str, int]:
        return {
            "min_events": self.min_events,
            "min_snapshots": self.min_snapshots,
            "min_ticks": self.min_ticks,
        }


WarmupSpec = Warmup


@dataclass(frozen=True, slots=True)
class FactorSpec:
    factor_id: str
    factor_version: str
    state_schema_version: int
    input_mode: InputMode
    inputs: tuple[InputSpec, ...]
    clock_semantics: ClockSemantics
    windows: tuple[WindowSpec, ...] = ()
    required_validity_mask: int = 0
    forbidden_quality_mask: int = 0
    on_gap_policy: GapPolicy = GapPolicy.INVALIDATE
    on_clock_epoch_change: EpochPolicy = EpochPolicy.RESET_AND_WARMUP
    output_cadence: OutputCadence = OutputCadence.EACH_INPUT
    max_batch_events: int = 2048
    max_batch_wait_us: int = 1000
    warmup: Warmup = Warmup()
    numeric_dtype: str = "float64"
    nondeterministic_live_latest: bool = False

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,127}", self.factor_id):
            raise ValidationError("factor_id must be a stable lowercase identifier")
        if not re.fullmatch(r"[0-9A-Za-z][0-9A-Za-z._+-]{0,63}", self.factor_version):
            raise ValidationError("factor_version contains unsupported characters")
        _strict_int(
            self.state_schema_version,
            "state_schema_version",
            1,
            (1 << 32) - 1,
        )
        if not isinstance(self.input_mode, InputMode):
            raise ValidationError("input_mode must be an InputMode")
        if type(self.inputs) is not tuple or not self.inputs:
            raise ValidationError("inputs must be a non-empty tuple")
        if any(not isinstance(item, InputSpec) for item in self.inputs):
            raise ValidationError("every input must be an InputSpec")
        ordered_inputs = tuple(sorted(self.inputs, key=lambda item: item.key))
        object.__setattr__(self, "inputs", ordered_inputs)
        keys = [item.key for item in ordered_inputs]
        if len(keys) != len(set(keys)):
            raise ValidationError("FactorSpec inputs must not contain duplicate keys")
        if not isinstance(self.clock_semantics, ClockSemantics):
            raise ValidationError("clock_semantics must be a ClockSemantics")
        if type(self.windows) is not tuple or any(
            not isinstance(item, WindowSpec) for item in self.windows
        ):
            raise ValidationError("windows must be a tuple of WindowSpec")
        ordered_windows = tuple(sorted(self.windows, key=lambda window: window.name))
        object.__setattr__(self, "windows", ordered_windows)
        window_names = [window.name for window in ordered_windows]
        if len(window_names) != len(set(window_names)):
            raise ValidationError("window names must be unique")
        _strict_int(self.required_validity_mask, "required_validity_mask", 0, (1 << 64) - 1)
        _strict_int(self.forbidden_quality_mask, "forbidden_quality_mask", 0, (1 << 64) - 1)
        if not isinstance(self.on_gap_policy, GapPolicy):
            raise ValidationError("on_gap_policy must be a GapPolicy")
        if not isinstance(self.on_clock_epoch_change, EpochPolicy):
            raise ValidationError("on_clock_epoch_change must be an EpochPolicy")
        if not isinstance(self.output_cadence, OutputCadence):
            raise ValidationError("output_cadence must be an OutputCadence")
        _strict_int(self.max_batch_events, "max_batch_events", 1, 1_000_000)
        _strict_int(self.max_batch_wait_us, "max_batch_wait_us", 0, 60_000_000)
        if not isinstance(self.warmup, Warmup):
            raise ValidationError("warmup must be a Warmup")
        if type(self.nondeterministic_live_latest) is not bool:
            raise ValidationError("nondeterministic_live_latest must be bool")
        try:
            dtype = np.dtype(self.numeric_dtype)
        except (TypeError, ValueError) as error:
            raise ValidationError("numeric_dtype is not a NumPy dtype") from error
        if dtype.fields is not None or dtype.subdtype is not None or dtype.kind not in "iuf":
            raise ValidationError("numeric_dtype must be a scalar integer or real floating dtype")
        object.__setattr__(self, "numeric_dtype", dtype.name)
        self._validate_input_mode()

    def _validate_input_mode(self) -> None:
        families = {item.family for item in self.inputs}
        if self.input_mode is InputMode.TICK_ONLY:
            if families != {InputFamily.TICK}:
                raise ValidationError("TICK_ONLY accepts only tick inputs")
        elif self.input_mode is InputMode.SNAPSHOT_ONLY:
            if families != {InputFamily.SNAPSHOT}:
                raise ValidationError("SNAPSHOT_ONLY accepts only snapshot inputs")
        elif self.input_mode is InputMode.RECEIVE_TIME_MUX:
            if len(self.inputs) < 2 or InputFamily.LATEST_STATE in families:
                raise ValidationError(
                    "RECEIVE_TIME_MUX requires at least two Canonical inputs"
                )
            if self.clock_semantics is not ClockSemantics.RECEIVE_MONOTONIC:
                raise ValidationError("RECEIVE_TIME_MUX requires receive_monotonic")
        elif self.input_mode is InputMode.SNAPSHOT_ASOF_TICK:
            if InputFamily.TICK not in families or InputFamily.SNAPSHOT not in families:
                raise ValidationError("SNAPSHOT_ASOF_TICK requires tick and snapshot inputs")
            if families - {InputFamily.TICK, InputFamily.SNAPSHOT}:
                raise ValidationError("SNAPSHOT_ASOF_TICK accepts only tick/snapshot inputs")
            if self.clock_semantics is not ClockSemantics.RECEIVE_MONOTONIC:
                raise ValidationError("SNAPSHOT_ASOF_TICK requires receive_monotonic")
        elif self.input_mode is InputMode.LIVE_LATEST:
            if InputFamily.TICK not in families or InputFamily.LATEST_STATE not in families:
                raise ValidationError("LIVE_LATEST requires tick and latest_state inputs")
            if families - {InputFamily.TICK, InputFamily.LATEST_STATE}:
                raise ValidationError("LIVE_LATEST accepts only tick/latest_state inputs")
            if not self.nondeterministic_live_latest:
                raise ValidationError(
                    "LIVE_LATEST must explicitly set nondeterministic_live_latest=True"
                )
        if self.input_mode is not InputMode.LIVE_LATEST and self.nondeterministic_live_latest:
            raise ValidationError(
                "nondeterministic_live_latest is reserved for LIVE_LATEST"
            )

    def canonical_object(self) -> dict[str, Any]:
        return {
            "clock_semantics": self.clock_semantics.value,
            "factor_id": self.factor_id,
            "factor_version": self.factor_version,
            "forbidden_quality_mask": self.forbidden_quality_mask,
            "input_mode": self.input_mode.value,
            "inputs": [item.canonical_object() for item in self.inputs],
            "max_batch_events": self.max_batch_events,
            "max_batch_wait_us": self.max_batch_wait_us,
            "nondeterministic_live_latest": self.nondeterministic_live_latest,
            "numeric_dtype": self.numeric_dtype,
            "on_clock_epoch_change": self.on_clock_epoch_change.value,
            "on_gap_policy": self.on_gap_policy.value,
            "output_cadence": self.output_cadence.value,
            "required_validity_mask": self.required_validity_mask,
            "spec_encoding_version": 1,
            "state_schema_version": self.state_schema_version,
            "warmup": self.warmup.canonical_object(),
            "windows": [item.canonical_object() for item in self.windows],
        }

    def canonical_json(self) -> bytes:
        return json.dumps(
            self.canonical_object(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")

    def sha256(self) -> bytes:
        return hashlib.sha256(self.canonical_json()).digest()

    def sha256_hex(self) -> str:
        return self.sha256().hex()
