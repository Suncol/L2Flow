#!/usr/bin/env python3
"""Validate the frozen Phase-3 control schemas and generate C++ constants."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
from typing import Any, Iterable, Mapping, Sequence


_RECORD_FIELDS = (
    ("magic", 0, 4),
    ("version", 4, 2),
    ("header_size", 6, 2),
    ("record_size", 8, 4),
    ("flags", 12, 4),
    ("control_type", 16, 2),
    ("decode_error", 18, 2),
    ("source_stream_id", 20, 4),
    ("capture_date", 24, 4),
    ("stream_day_id", 28, 16),
    ("vendor_service_id", 44, 1),
    ("reserved0", 45, 1),
    ("vendor_service_version", 46, 2),
    ("vendor_message_id", 48, 2),
    ("reserved1", 50, 2),
    ("return_or_error_code", 52, 4),
    ("connection_epoch", 56, 4),
    ("subscription_epoch", 60, 4),
    ("origin_ingress_sequence", 64, 8),
    ("origin_record_end_wal_pos", 72, 8),
    ("quality_flags", 80, 8),
    ("required_count", 88, 4),
    ("required_ok_count", 92, 4),
    ("required_failed_count", 96, 4),
    ("optional_count", 100, 4),
    ("optional_ok_count", 104, 4),
    ("optional_failed_count", 108, 4),
    ("response_entry_count", 112, 4),
    ("reserved2", 116, 4),
    ("response_manifest_sha256", 120, 32),
    ("address_sha256", 152, 32),
    ("error_text_sha256", 184, 32),
    ("control_state_sha256", 216, 32),
    ("record_crc32c", 248, 4),
    ("reserved_tail", 252, 4),
)

_CHECKPOINT_HEADER_FIELDS = (
    ("magic", 0, 4),
    ("version", 4, 2),
    ("header_size", 6, 2),
    ("total_size", 8, 4),
    ("entry_size", 12, 2),
    ("reserved0", 14, 2),
    ("entry_count", 16, 4),
    ("flags", 20, 4),
    ("source_stream_id", 24, 4),
    ("capture_date", 28, 4),
    ("stream_day_id", 32, 16),
    ("stable_config_sha256", 48, 32),
    ("requested_manifest_sha256", 80, 32),
    ("response_manifest_sha256", 112, 32),
    ("current_address_sha256", 144, 32),
    ("last_success_address_sha256", 176, 32),
    ("state_sha256", 208, 32),
    ("connection_epoch", 240, 4),
    ("subscription_epoch", 244, 4),
    ("session_phase", 248, 1),
    ("reserved1", 249, 7),
    ("next_ingress_sequence", 256, 8),
    ("processed_ingress_sequence", 264, 8),
    ("processed_record_start_wal_pos", 272, 8),
    ("processed_record_end_wal_pos", 280, 8),
    ("latest_response_ingress_sequence", 288, 8),
    ("latest_response_record_end_wal_pos", 296, 8),
    ("quality_flags", 304, 8),
    ("required_first_seen_mask", 312, 8),
    ("effective_success_mask", 320, 8),
    ("processed_records", 328, 8),
    ("emitted_control_records", 336, 8),
    ("logon_success", 344, 8),
    ("logon_failure", 352, 8),
    ("disconnect", 360, 8),
    ("subscription_responses", 368, 8),
    ("control_decode_errors", 376, 8),
    ("reserved_tail", 384, 124),
    ("header_crc32c", 508, 4),
)

_CHECKPOINT_ENTRY_FIELDS = (
    ("service_id", 0, 1),
    ("policy", 1, 1),
    ("status_known", 2, 1),
    ("reserved0", 3, 1),
    ("service_version", 4, 2),
    ("message_id", 6, 2),
    ("status", 8, 4),
    ("reserved_tail", 12, 4),
)

_CHECKPOINT_TRAILER_FIELDS = (("checkpoint_sha256", 0, 32),)

_RECORD_CONTROL_TYPES = {
    "CONNECTING": 1,
    "CONNECT_ERROR": 2,
    "DISCONNECTED": 3,
    "LOGON_SUCCESS": 4,
    "LOGON_FAILURE": 5,
    "SUBSCRIPTION_ACCEPTED": 6,
    "SUBSCRIPTION_REJECTED": 7,
    "SERVICE_STATUS": 8,
    "SESSION_STATUS": 9,
    "DECODE_ERROR": 10,
}

_SESSION_PHASES = {
    "INITIAL": 0,
    "CONNECTING": 1,
    "LOGGED_IN": 2,
    "CONNECT_ERROR": 3,
    "DISCONNECTED": 4,
    "LOGON_FAILED": 5,
    "POISONED": 6,
}

_SUBSCRIPTION_POLICIES = {"REQUIRED": 1, "OPTIONAL": 2}

_RECORD_FLAGS = {
    "RESPONSE_MANIFEST_HASH_PRESENT": 1,
    "ADDRESS_HASH_PRESENT": 2,
    "ERROR_TEXT_HASH_PRESENT": 4,
    "REQUIRED_FAILURE": 8,
    "OPTIONAL_FAILURE": 16,
    "NONCANONICAL_EMPTY_OFFSET": 32,
}

_CHECKPOINT_FLAGS = {
    "LOGGED_IN": 1,
    "DISCONNECTED_WINDOW": 2,
    "CONNECTION_SWITCHED": 4,
    "POISONED": 8,
    "CONTROL_READY": 16,
    "DECODER_EVIDENCE_READY": 32,
}

_INTEGER_ENCODINGS = {
    "u8": 1,
    "u16-le": 2,
    "u32-le": 4,
    "u64-le": 8,
}


class SchemaValidationError(ValueError):
    """Raised when an input is JSON but not the frozen schema contract."""


class _Layout:
    def __init__(
        self,
        name: str,
        structure: Mapping[str, Any],
        size: int,
        fields: Sequence[Mapping[str, Any]],
    ) -> None:
        self.name = name
        self.structure = structure
        self.size = size
        self.fields = tuple(fields)
        self.by_name = {str(field["name"]): field for field in fields}


class _LoadedSchema:
    def __init__(self, raw: bytes, document: Mapping[str, Any]) -> None:
        self.raw = raw
        self.document = document
        self.sha256 = hashlib.sha256(raw).digest()


def _fail(where: str, message: str) -> None:
    raise SchemaValidationError(f"{where}: {message}")


def _as_object(value: Any, where: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        _fail(where, "must be a JSON object")
    return value


def _as_array(value: Any, where: str) -> Sequence[Any]:
    if not isinstance(value, list):
        _fail(where, "must be a JSON array")
    return value


def _as_string(value: Any, where: str) -> str:
    if not isinstance(value, str):
        _fail(where, "must be a JSON string")
    return value


def _as_bool(value: Any, where: str) -> bool:
    if type(value) is not bool:
        _fail(where, "must be a JSON boolean")
    return value


def _as_int(value: Any, where: str) -> int:
    if type(value) is not int:
        _fail(where, "must be a JSON integer")
    return value


def _member(obj: Mapping[str, Any], key: str, where: str) -> Any:
    if key not in obj:
        _fail(where, f"missing required member {key!r}")
    return obj[key]


def _object_member(
    obj: Mapping[str, Any], key: str, where: str
) -> Mapping[str, Any]:
    return _as_object(_member(obj, key, where), f"{where}.{key}")


def _array_member(
    obj: Mapping[str, Any], key: str, where: str
) -> Sequence[Any]:
    return _as_array(_member(obj, key, where), f"{where}.{key}")


def _string_member(obj: Mapping[str, Any], key: str, where: str) -> str:
    return _as_string(_member(obj, key, where), f"{where}.{key}")


def _int_member(obj: Mapping[str, Any], key: str, where: str) -> int:
    return _as_int(_member(obj, key, where), f"{where}.{key}")


def _bool_member(obj: Mapping[str, Any], key: str, where: str) -> bool:
    return _as_bool(_member(obj, key, where), f"{where}.{key}")


def _expect_equal(actual: Any, expected: Any, where: str) -> None:
    if actual != expected:
        _fail(where, f"must equal {expected!r}, got {actual!r}")


def _parse_hex(value: Any, where: str) -> int:
    text = _as_string(value, where)
    if re.fullmatch(r"0x[0-9a-fA-F]+", text) is None:
        _fail(where, "must be a 0x-prefixed hexadecimal integer")
    return int(text, 16)


def _reject_json_constant(value: str) -> None:
    raise SchemaValidationError(
        f"JSON contains non-standard numeric constant {value!r}"
    )


def _object_without_duplicate_keys(
    pairs: Iterable[tuple[str, Any]],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise SchemaValidationError(f"duplicate JSON object key {key!r}")
        result[key] = value
    return result


def _validate_unicode(value: Any, where: str) -> None:
    if isinstance(value, str):
        try:
            value.encode("utf-8", errors="strict")
        except UnicodeEncodeError as error:
            _fail(where, f"contains an unpaired Unicode surrogate: {error}")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_unicode(item, f"{where}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            _validate_unicode(key, f"{where}.<key>")
            _validate_unicode(item, f"{where}.{key}")


def _load_schema(path: Path, label: str) -> _LoadedSchema:
    try:
        raw = path.read_bytes()
    except OSError as error:
        _fail(label, f"cannot read {path}: {error}")
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        _fail(label, f"invalid UTF-8 at byte {error.start}: {error.reason}")
    try:
        document = json.loads(
            text,
            object_pairs_hook=_object_without_duplicate_keys,
            parse_constant=_reject_json_constant,
        )
    except json.JSONDecodeError as error:
        _fail(
            label,
            f"invalid JSON at line {error.lineno}, column {error.colno}: "
            f"{error.msg}",
        )
    except SchemaValidationError as error:
        _fail(label, str(error))
    root = _as_object(document, label)
    _validate_unicode(root, label)
    return _LoadedSchema(raw, root)


def _validate_field_metadata(
    field: Mapping[str, Any], where: str, width: int
) -> None:
    encoding = _string_member(field, "encoding", where)
    if encoding == "bytes":
        pass
    elif encoding in _INTEGER_ENCODINGS:
        _expect_equal(_INTEGER_ENCODINGS[encoding], width, f"{where}.width")
    else:
        _fail(where, f"unsupported or non-little-endian encoding {encoding!r}")

    maximum_value = (1 << (8 * width)) - 1
    for key in ("value", "minimum", "maximum"):
        if key in field:
            value = _as_int(field[key], f"{where}.{key}")
            if value < 0 or value > maximum_value:
                _fail(
                    f"{where}.{key}",
                    f"is outside the unsigned {width}-byte field range",
                )
    if "minimum" in field and "maximum" in field:
        if _as_int(field["minimum"], f"{where}.minimum") > _as_int(
            field["maximum"], f"{where}.maximum"
        ):
            _fail(where, "minimum exceeds maximum")

    if "value_hex" in field:
        value = _parse_hex(field["value_hex"], f"{where}.value_hex")
        if value > maximum_value:
            _fail(f"{where}.value_hex", f"does not fit in {width} bytes")
    if "allowed_values" in field:
        values = _as_array(field["allowed_values"], f"{where}.allowed_values")
        seen: set[int] = set()
        for index, item in enumerate(values):
            value = _as_int(item, f"{where}.allowed_values[{index}]")
            if value < 0 or value > maximum_value:
                _fail(
                    f"{where}.allowed_values[{index}]",
                    f"is outside the unsigned {width}-byte field range",
                )
            if value in seen:
                _fail(f"{where}.allowed_values", f"duplicates value {value}")
            seen.add(value)
    for key in (
        "must_be_zero",
        "must_not_be_all_zero",
        "codec_recomputes",
    ):
        if key in field:
            _as_bool(field[key], f"{where}.{key}")


def _is_reserved_name(name: str) -> bool:
    return name == "reserved" or name.startswith("reserved_") or bool(
        re.fullmatch(r"reserved[0-9]+", name)
    )


def _validate_layout(
    structures: Mapping[str, Any], structure_name: str, label: str
) -> _Layout:
    where = f"{label}.structures.{structure_name}"
    structure = _as_object(
        _member(structures, structure_name, f"{label}.structures"), where
    )
    size = _int_member(structure, "size", where)
    if size <= 0:
        _fail(f"{where}.size", "must be positive")
    raw_fields = _array_member(structure, "fields", where)
    if not raw_fields:
        _fail(f"{where}.fields", "must not be empty")

    fields: list[Mapping[str, Any]] = []
    names: set[str] = set()
    cursor = 0
    for index, raw_field in enumerate(raw_fields):
        field_where = f"{where}.fields[{index}]"
        field = _as_object(raw_field, field_where)
        name = _string_member(field, "name", field_where)
        if name in names:
            _fail(field_where, f"duplicates field name {name!r}")
        names.add(name)
        offset = _int_member(field, "offset", field_where)
        width = _int_member(field, "width", field_where)
        if offset < 0:
            _fail(f"{field_where}.offset", "must be nonnegative")
        if width <= 0:
            _fail(f"{field_where}.width", "must be positive")
        if offset > cursor:
            _fail(
                f"{field_where}.offset",
                f"creates a hole [{cursor}, {offset})",
            )
        if offset < cursor:
            _fail(
                f"{field_where}.offset",
                f"overlaps or is out of order; next offset is {cursor}",
            )
        if offset + width > size:
            _fail(field_where, f"extends beyond structure size {size}")

        reserved = _bool_member(field, "reserved", field_where)
        reserved_name = _is_reserved_name(name)
        if reserved != reserved_name:
            _fail(
                f"{field_where}.reserved",
                "must be true exactly for reserved/reservedN/reserved_* fields",
            )
        if reserved:
            _expect_equal(
                _string_member(field, "encoding", field_where),
                "bytes",
                f"{field_where}.encoding",
            )
            if field.get("must_be_zero") is not True:
                _fail(field_where, "reserved bytes must declare must_be_zero=true")
            for forbidden in ("enum", "flags", "value", "value_hex"):
                if forbidden in field:
                    _fail(field_where, f"reserved field cannot declare {forbidden}")
        elif field.get("must_be_zero") is True:
            _fail(field_where, "a non-reserved field cannot be must_be_zero")

        _validate_field_metadata(field, field_where, width)
        fields.append(field)
        cursor = offset + width

    if cursor != size:
        _fail(where, f"fields cover [0, {cursor}), not exact size {size}")
    return _Layout(structure_name, structure, size, fields)


def _expect_frozen_layout(
    layout: _Layout,
    expected: Sequence[tuple[str, int, int]],
    label: str,
) -> None:
    actual = tuple(
        (
            _as_string(field["name"], f"{label}.name"),
            _as_int(field["offset"], f"{label}.offset"),
            _as_int(field["width"], f"{label}.width"),
        )
        for field in layout.fields
    )
    if actual != tuple(expected):
        _fail(label, "does not match the frozen V1 field names/offsets/widths")


def _validate_codec_encodings(
    layout: _Layout, byte_fields: set[str], label: str
) -> None:
    integer_by_width = {1: "u8", 2: "u16-le", 4: "u32-le", 8: "u64-le"}
    for field in layout.fields:
        name = str(field["name"])
        width = int(field["width"])
        reserved = bool(field["reserved"])
        if reserved or name in byte_fields:
            expected = "bytes"
        elif width in integer_by_width:
            expected = integer_by_width[width]
        else:
            _fail(
                f"{label}.{name}",
                f"non-byte codec field has unsupported width {width}",
            )
        _expect_equal(field["encoding"], expected, f"{label}.{name}.encoding")


def _expect_reference(
    field: Mapping[str, Any], key: str, expected: str, where: str
) -> None:
    _expect_equal(_string_member(field, key, where), expected, f"{where}.{key}")


def _validate_opaque_hashes(
    layout: _Layout,
    field_names: Sequence[str],
    nonzero_names: set[str],
    label: str,
) -> None:
    for field_name in field_names:
        field = layout.by_name[field_name]
        where = f"{label}.{field_name}"
        _expect_equal(
            _bool_member(field, "codec_recomputes", where),
            False,
            f"{where}.codec_recomputes",
        )
        if field_name in nonzero_names:
            _expect_equal(
                _bool_member(field, "must_not_be_all_zero", where),
                True,
                f"{where}.must_not_be_all_zero",
            )
        elif "must_not_be_all_zero" in field:
            _fail(
                f"{where}.must_not_be_all_zero",
                "optional hash field must permit the all-zero value",
            )


def _validate_crc_parameters(document: Mapping[str, Any], label: str) -> None:
    crc = _object_member(document, "crc32c", label)
    expected = {
        "algorithm": "CRC-32C/Castagnoli",
        "init_hex": "0xffffffff",
        "polynomial_normal_hex": "0x1edc6f41",
        "polynomial_reflected_hex": "0x82f63b78",
        "refin": True,
        "refout": True,
        "stored_encoding": "u32-le",
        "xorout_hex": "0xffffffff",
    }
    for key, value in expected.items():
        _expect_equal(_member(crc, key, f"{label}.crc32c"), value, f"{label}.crc32c.{key}")


def _validate_crc_domain(
    layout: _Layout,
    field_name: str,
    label: str,
) -> None:
    where = f"{label}.structures.{layout.name}.crc"
    crc = _object_member(layout.structure, "crc", f"{label}.structures.{layout.name}")
    field = layout.by_name[field_name]
    expected = {
        "algorithm": "CRC-32C/Castagnoli",
        "domain_length": layout.size,
        "domain_offset": 0,
        "field_offset": field["offset"],
        "field_width": field["width"],
        "field_zeroed_during_calculation": True,
    }
    for key, value in expected.items():
        _expect_equal(_member(crc, key, where), value, f"{where}.{key}")
    _expect_equal(field["encoding"], "u32-le", f"{where}.field_encoding")
    _expect_equal(field["reserved"], False, f"{where}.field_reserved")


def _validate_named_values(
    container: Mapping[str, Any],
    name: str,
    expected_values: Mapping[str, int] | None,
    is_flags: bool,
    label: str,
) -> Mapping[str, Any]:
    group_name = "flags" if is_flags else "enums"
    where = f"{label}.{group_name}.{name}"
    spec = _as_object(_member(container, name, f"{label}.{group_name}"), where)
    encoding = _string_member(spec, "encoding", where)
    if encoding not in _INTEGER_ENCODINGS:
        _fail(f"{where}.encoding", "must be an unsigned little-endian integer")
    policy_key = "unknown_bits" if is_flags else "unknown_values"
    _expect_equal(
        _string_member(spec, policy_key, where), "reject", f"{where}.{policy_key}"
    )
    values_obj = _object_member(spec, "values", where)
    values: dict[str, int] = {}
    seen: set[int] = set()
    maximum = (1 << (8 * _INTEGER_ENCODINGS[encoding])) - 1
    for key, raw_value in values_obj.items():
        value = _as_int(raw_value, f"{where}.values.{key}")
        if value < 0 or value > maximum:
            _fail(f"{where}.values.{key}", "is outside the encoded range")
        if value in seen:
            _fail(f"{where}.values.{key}", f"duplicates numeric value {value}")
        if is_flags and (value == 0 or (value & (value - 1)) != 0):
            _fail(f"{where}.values.{key}", "must be one nonzero bit")
        values[key] = value
        seen.add(value)
    if expected_values is not None and values != dict(expected_values):
        _fail(f"{where}.values", "does not match the frozen V1 assignments")
    if is_flags:
        mask = _parse_hex(_member(spec, "mask_hex", where), f"{where}.mask_hex")
        combined = 0
        for value in values.values():
            combined |= value
        _expect_equal(mask, combined, f"{where}.mask_hex")
    return spec


def _validate_field_references(
    layouts: Sequence[_Layout],
    enums: Mapping[str, Any],
    flags: Mapping[str, Any],
    label: str,
) -> None:
    for layout in layouts:
        for field in layout.fields:
            name = str(field["name"])
            where = f"{label}.structures.{layout.name}.fields.{name}"
            if "enum" in field:
                enum_name = _as_string(field["enum"], f"{where}.enum")
                enum_spec = _as_object(
                    _member(enums, enum_name, f"{label}.enums"),
                    f"{label}.enums.{enum_name}",
                )
                _expect_equal(
                    field["encoding"], enum_spec["encoding"], f"{where}.encoding"
                )
            if "flags" in field:
                flag_name = _as_string(field["flags"], f"{where}.flags")
                flag_spec = _as_object(
                    _member(flags, flag_name, f"{label}.flags"),
                    f"{label}.flags.{flag_name}",
                )
                _expect_equal(
                    field["encoding"], flag_spec["encoding"], f"{where}.encoding"
                )
                field_mask = _parse_hex(
                    _member(field, "mask_hex", where), f"{where}.mask_hex"
                )
                spec_mask = _parse_hex(
                    _member(flag_spec, "mask_hex", f"{label}.flags.{flag_name}"),
                    f"{label}.flags.{flag_name}.mask_hex",
                )
                _expect_equal(field_mask, spec_mask, f"{where}.mask_hex")


def _validate_magic(
    field: Mapping[str, Any],
    expected_value: int,
    expected_ascii: str,
    where: str,
) -> int:
    _expect_equal(field["encoding"], "u32-le", f"{where}.encoding")
    _expect_equal(field["width"], 4, f"{where}.width")
    value = _parse_hex(_member(field, "value_hex", where), f"{where}.value_hex")
    _expect_equal(value, expected_value, f"{where}.value_hex")
    wire_ascii = _string_member(field, "wire_ascii", where)
    try:
        decoded = value.to_bytes(4, byteorder="little").decode("ascii")
    except (OverflowError, UnicodeDecodeError) as error:
        _fail(where, f"magic is not four little-endian ASCII bytes: {error}")
    _expect_equal(wire_ascii, decoded, f"{where}.wire_ascii")
    _expect_equal(wire_ascii, expected_ascii, f"{where}.wire_ascii")
    return value


def _validate_fixed_value(
    field: Mapping[str, Any], expected: int, where: str
) -> int:
    value = _int_member(field, "value", where)
    _expect_equal(value, expected, f"{where}.value")
    return value


def _validate_common(document: Mapping[str, Any], label: str) -> None:
    _expect_equal(
        _int_member(document, "schema_version", label),
        1,
        f"{label}.schema_version",
    )
    _expect_equal(
        _string_member(document, "byte_order", label),
        "little-endian",
        f"{label}.byte_order",
    )
    _validate_crc_parameters(document, label)


def _validate_record_schema(schema: _LoadedSchema) -> dict[str, Any]:
    document = schema.document
    label = "record schema"
    _validate_common(document, label)
    _expect_equal(
        _string_member(document, "format", label),
        "L2Flow ControlRecordV1",
        f"{label}.format",
    )
    structures = _object_member(document, "structures", label)
    if set(structures) != {"control_record_v1"}:
        _fail(f"{label}.structures", "must contain only control_record_v1")
    layout = _validate_layout(structures, "control_record_v1", label)
    _expect_frozen_layout(layout, _RECORD_FIELDS, label)
    _validate_codec_encodings(
        layout,
        {
            "stream_day_id",
            "response_manifest_sha256",
            "address_sha256",
            "error_text_sha256",
            "control_state_sha256",
        },
        label,
    )

    limits = _object_member(document, "limits", label)
    expected_limits = {
        "version": 1,
        "header_size": 16,
        "record_size": 256,
        "identity128_size": 16,
        "sha256_size": 32,
    }
    for key, expected in expected_limits.items():
        _expect_equal(
            _int_member(limits, key, f"{label}.limits"),
            expected,
            f"{label}.limits.{key}",
        )
    _expect_equal(layout.size, expected_limits["record_size"], f"{label}.size")

    magic = _validate_magic(
        layout.by_name["magic"],
        0x3152434C,
        "LCR1",
        f"{label}.magic",
    )
    version = _validate_fixed_value(
        layout.by_name["version"], expected_limits["version"], f"{label}.version"
    )
    header_size = _validate_fixed_value(
        layout.by_name["header_size"],
        expected_limits["header_size"],
        f"{label}.header_size",
    )
    record_size = _validate_fixed_value(
        layout.by_name["record_size"],
        expected_limits["record_size"],
        f"{label}.record_size",
    )
    _expect_equal(
        layout.by_name["flags"]["offset"] + layout.by_name["flags"]["width"],
        header_size,
        f"{label}.header_size",
    )
    _expect_equal(
        layout.by_name["control_type"]["offset"],
        header_size,
        f"{label}.control_type.offset",
    )

    record_hash_fields = (
        "response_manifest_sha256",
        "address_sha256",
        "error_text_sha256",
        "control_state_sha256",
    )
    for field_name in record_hash_fields:
        _expect_equal(
            layout.by_name[field_name]["width"],
            expected_limits["sha256_size"],
            f"{label}.{field_name}.width",
        )
    _validate_opaque_hashes(
        layout,
        record_hash_fields,
        {"control_state_sha256"},
        label,
    )
    _expect_equal(
        layout.by_name["stream_day_id"]["width"],
        expected_limits["identity128_size"],
        f"{label}.stream_day_id.width",
    )

    enums = _object_member(document, "enums", label)
    flags = _object_member(document, "flags", label)
    if set(enums) != {"control_type_v1"}:
        _fail(f"{label}.enums", "must contain only control_type_v1")
    if set(flags) != {"control_record_flags_v1", "quality_flags_v1"}:
        _fail(f"{label}.flags", "contains unexpected or missing flag groups")
    _validate_named_values(
        enums,
        "control_type_v1",
        _RECORD_CONTROL_TYPES,
        False,
        label,
    )
    record_flags = _validate_named_values(
        flags,
        "control_record_flags_v1",
        _RECORD_FLAGS,
        True,
        label,
    )
    quality_flags = _validate_named_values(
        flags, "quality_flags_v1", None, True, label
    )
    _expect_equal(
        _parse_hex(record_flags["mask_hex"], f"{label}.record_flags.mask_hex"),
        0x3F,
        f"{label}.record_flags.mask_hex",
    )
    _expect_equal(
        _parse_hex(quality_flags["mask_hex"], f"{label}.quality_flags.mask_hex"),
        0xFFFFFFFFF,
        f"{label}.quality_flags.mask_hex",
    )
    _expect_reference(
        layout.by_name["control_type"],
        "enum",
        "control_type_v1",
        f"{label}.control_type",
    )
    _expect_reference(
        layout.by_name["flags"],
        "flags",
        "control_record_flags_v1",
        f"{label}.flags",
    )
    _expect_reference(
        layout.by_name["quality_flags"],
        "flags",
        "quality_flags_v1",
        f"{label}.quality_flags",
    )
    _validate_field_references((layout,), enums, flags, label)
    _validate_crc_domain(layout, "record_crc32c", label)

    return {
        "layout": layout,
        "magic": magic,
        "version": version,
        "header_size": header_size,
        "record_size": record_size,
        "quality_values": _object_member(quality_flags, "values", label),
    }


def _validate_checkpoint_digest(
    document: Mapping[str, Any],
    composite: Mapping[str, Any],
    trailer: _Layout,
    header_size: int,
    entry_size: int,
    trailer_size: int,
    label: str,
) -> None:
    sha = _object_member(document, "sha256", label)
    expected_sha = {
        "algorithm": "SHA-256",
        "digest_size": 32,
        "stored_encoding": "raw-bytes",
    }
    for key, expected in expected_sha.items():
        _expect_equal(_member(sha, key, f"{label}.sha256"), expected, f"{label}.sha256.{key}")
    _expect_equal(trailer.size, expected_sha["digest_size"], f"{label}.trailer.size")
    trailer_field = trailer.by_name["checkpoint_sha256"]
    _expect_equal(trailer_field["offset"], 0, f"{label}.checkpoint_sha256.offset")
    _expect_equal(
        trailer_field["width"], trailer_size, f"{label}.checkpoint_sha256.width"
    )

    digest = _object_member(composite, "digest", f"{label}.control_checkpoint_v1")
    expected_digest = {
        "algorithm": "SHA-256",
        "domain_length_expression": f"total_size - {trailer_size}",
        "domain_offset": 0,
        "field_offset_expression": f"total_size - {trailer_size}",
        "field_width": trailer_size,
        "stored_encoding": "raw-bytes",
        "trailer_excluded_from_calculation": True,
    }
    for key, expected in expected_digest.items():
        _expect_equal(
            _member(digest, key, f"{label}.digest"),
            expected,
            f"{label}.digest.{key}",
        )

    total_expression = f"{header_size} + {entry_size} * entry_count + {trailer_size}"
    _expect_equal(
        _string_member(composite, "total_size_expression", f"{label}.composite"),
        total_expression,
        f"{label}.composite.total_size_expression",
    )
    _expect_equal(
        _string_member(composite, "trailing_bytes", f"{label}.composite"),
        "forbidden",
        f"{label}.composite.trailing_bytes",
    )
    layout = _array_member(composite, "layout", f"{label}.composite")
    if len(layout) != 3:
        _fail(f"{label}.composite.layout", "must have header, subscriptions, trailer")
    expected_layout = (
        {
            "count": 1,
            "name": "header",
            "offset": 0,
            "structure": "control_checkpoint_header_v1",
        },
        {
            "count_expression": "entry_count",
            "name": "subscriptions",
            "offset": header_size,
            "structure": "control_checkpoint_subscription_entry_v1",
        },
        {
            "count": 1,
            "name": "trailer",
            "offset_expression": f"{header_size} + {entry_size} * entry_count",
            "structure": "control_checkpoint_trailer_v1",
        },
    )
    for index, expected in enumerate(expected_layout):
        actual = _as_object(layout[index], f"{label}.composite.layout[{index}]")
        if actual != expected:
            _fail(
                f"{label}.composite.layout[{index}]",
                f"must equal {expected!r}, got {actual!r}",
            )


def _validate_checkpoint_schema(schema: _LoadedSchema) -> dict[str, Any]:
    document = schema.document
    label = "checkpoint schema"
    _validate_common(document, label)
    _expect_equal(
        _string_member(document, "format", label),
        "L2Flow ControlCheckpointV1",
        f"{label}.format",
    )
    structures = _object_member(document, "structures", label)
    expected_structures = {
        "control_checkpoint_header_v1",
        "control_checkpoint_subscription_entry_v1",
        "control_checkpoint_trailer_v1",
        "control_checkpoint_v1",
    }
    if set(structures) != expected_structures:
        _fail(f"{label}.structures", "contains unexpected or missing structures")
    header = _validate_layout(structures, "control_checkpoint_header_v1", label)
    entry = _validate_layout(
        structures, "control_checkpoint_subscription_entry_v1", label
    )
    trailer = _validate_layout(structures, "control_checkpoint_trailer_v1", label)
    _expect_frozen_layout(header, _CHECKPOINT_HEADER_FIELDS, f"{label}.header")
    _expect_frozen_layout(entry, _CHECKPOINT_ENTRY_FIELDS, f"{label}.entry")
    _expect_frozen_layout(trailer, _CHECKPOINT_TRAILER_FIELDS, f"{label}.trailer")
    _validate_codec_encodings(
        header,
        {
            "stream_day_id",
            "stable_config_sha256",
            "requested_manifest_sha256",
            "response_manifest_sha256",
            "current_address_sha256",
            "last_success_address_sha256",
            "state_sha256",
        },
        f"{label}.header",
    )
    _validate_codec_encodings(entry, set(), f"{label}.entry")
    _validate_codec_encodings(
        trailer, {"checkpoint_sha256"}, f"{label}.trailer"
    )

    limits = _object_member(document, "limits", label)
    expected_limits = {
        "version": 1,
        "header_size": 512,
        "entry_size": 16,
        "trailer_size": 32,
        "entry_count_min": 1,
        "entry_count_max": 64,
        "file_size_min": 560,
        "file_size_max": 1568,
        "identity128_size": 16,
        "sha256_size": 32,
    }
    for key, expected in expected_limits.items():
        _expect_equal(
            _int_member(limits, key, f"{label}.limits"),
            expected,
            f"{label}.limits.{key}",
        )
    _expect_equal(header.size, expected_limits["header_size"], f"{label}.header.size")
    _expect_equal(entry.size, expected_limits["entry_size"], f"{label}.entry.size")
    _expect_equal(trailer.size, expected_limits["trailer_size"], f"{label}.trailer.size")
    _expect_equal(
        expected_limits["file_size_min"],
        header.size + entry.size * expected_limits["entry_count_min"] + trailer.size,
        f"{label}.limits.file_size_min",
    )
    _expect_equal(
        expected_limits["file_size_max"],
        header.size + entry.size * expected_limits["entry_count_max"] + trailer.size,
        f"{label}.limits.file_size_max",
    )

    magic = _validate_magic(
        header.by_name["magic"],
        0x3150434C,
        "LCP1",
        f"{label}.magic",
    )
    version = _validate_fixed_value(
        header.by_name["version"], expected_limits["version"], f"{label}.version"
    )
    header_size = _validate_fixed_value(
        header.by_name["header_size"],
        expected_limits["header_size"],
        f"{label}.header_size",
    )
    entry_size = _validate_fixed_value(
        header.by_name["entry_size"],
        expected_limits["entry_size"],
        f"{label}.entry_size",
    )
    total_size_field = header.by_name["total_size"]
    total_expression = f"{header_size} + {entry_size} * entry_count + {trailer.size}"
    _expect_equal(
        _string_member(total_size_field, "value_expression", f"{label}.total_size"),
        total_expression,
        f"{label}.total_size.value_expression",
    )
    entry_count = header.by_name["entry_count"]
    _expect_equal(
        _int_member(entry_count, "minimum", f"{label}.entry_count"),
        expected_limits["entry_count_min"],
        f"{label}.entry_count.minimum",
    )
    _expect_equal(
        _int_member(entry_count, "maximum", f"{label}.entry_count"),
        expected_limits["entry_count_max"],
        f"{label}.entry_count.maximum",
    )
    _expect_equal(
        header.by_name["stream_day_id"]["width"],
        expected_limits["identity128_size"],
        f"{label}.stream_day_id.width",
    )
    checkpoint_hash_fields = (
        "stable_config_sha256",
        "requested_manifest_sha256",
        "response_manifest_sha256",
        "current_address_sha256",
        "last_success_address_sha256",
        "state_sha256",
    )
    for field_name in checkpoint_hash_fields:
        _expect_equal(
            header.by_name[field_name]["width"],
            expected_limits["sha256_size"],
            f"{label}.{field_name}.width",
        )
    _validate_opaque_hashes(
        header,
        checkpoint_hash_fields,
        {
            "stable_config_sha256",
            "requested_manifest_sha256",
            "state_sha256",
        },
        label,
    )

    enums = _object_member(document, "enums", label)
    flags = _object_member(document, "flags", label)
    if set(enums) != {"control_session_phase_v1", "subscription_policy_v1"}:
        _fail(f"{label}.enums", "contains unexpected or missing enum groups")
    if set(flags) != {"control_checkpoint_flags_v1", "quality_flags_v1"}:
        _fail(f"{label}.flags", "contains unexpected or missing flag groups")
    _validate_named_values(
        enums,
        "control_session_phase_v1",
        _SESSION_PHASES,
        False,
        label,
    )
    _validate_named_values(
        enums,
        "subscription_policy_v1",
        _SUBSCRIPTION_POLICIES,
        False,
        label,
    )
    checkpoint_flags = _validate_named_values(
        flags,
        "control_checkpoint_flags_v1",
        _CHECKPOINT_FLAGS,
        True,
        label,
    )
    quality_flags = _validate_named_values(
        flags, "quality_flags_v1", None, True, label
    )
    _expect_equal(
        _parse_hex(
            checkpoint_flags["mask_hex"], f"{label}.checkpoint_flags.mask_hex"
        ),
        0x3F,
        f"{label}.checkpoint_flags.mask_hex",
    )
    _expect_equal(
        _parse_hex(quality_flags["mask_hex"], f"{label}.quality_flags.mask_hex"),
        0xFFFFFFFFF,
        f"{label}.quality_flags.mask_hex",
    )
    _expect_reference(
        header.by_name["session_phase"],
        "enum",
        "control_session_phase_v1",
        f"{label}.session_phase",
    )
    _expect_reference(
        entry.by_name["policy"],
        "enum",
        "subscription_policy_v1",
        f"{label}.entry.policy",
    )
    _expect_reference(
        header.by_name["flags"],
        "flags",
        "control_checkpoint_flags_v1",
        f"{label}.flags",
    )
    _expect_reference(
        header.by_name["quality_flags"],
        "flags",
        "quality_flags_v1",
        f"{label}.quality_flags",
    )
    _validate_field_references((header, entry, trailer), enums, flags, label)
    _validate_crc_domain(header, "header_crc32c", label)

    composite = _as_object(
        _member(structures, "control_checkpoint_v1", f"{label}.structures"),
        f"{label}.structures.control_checkpoint_v1",
    )
    _validate_checkpoint_digest(
        document,
        composite,
        trailer,
        header_size,
        entry_size,
        trailer.size,
        label,
    )

    return {
        "header": header,
        "entry": entry,
        "trailer": trailer,
        "magic": magic,
        "version": version,
        "header_size": header_size,
        "entry_size": entry_size,
        "trailer_size": trailer.size,
        "maximum_entries": expected_limits["entry_count_max"],
        "minimum_bytes": expected_limits["file_size_min"],
        "maximum_bytes": expected_limits["file_size_max"],
        "quality_values": _object_member(quality_flags, "values", label),
    }


def _cpp_field_name(name: str) -> str:
    special = {
        "crc32c": "Crc32c",
        "id": "Id",
        "sha256": "Sha256",
        "wal": "Wal",
    }
    return "k" + "".join(
        special.get(part, part[:1].upper() + part[1:]) for part in name.split("_")
    )


def _render_digest(name: str, digest: bytes) -> list[str]:
    lines = [
        "inline constexpr std::array<std::byte, 32U>",
        f"    {name}{{",
    ]
    for offset in range(0, len(digest), 4):
        chunk = digest[offset : offset + 4]
        rendered = ", ".join(f"std::byte{{0x{value:02x}}}" for value in chunk)
        suffix = "," if offset + 4 < len(digest) else ""
        lines.append(f"        {rendered}{suffix}")
    lines.append("    };")
    return lines


def _render_offsets(namespace_name: str, layout: _Layout) -> list[str]:
    lines = [f"namespace {namespace_name} {{"]
    for field in layout.fields:
        name = _cpp_field_name(str(field["name"]))
        offset = int(field["offset"])
        lines.append(f"inline constexpr std::size_t {name} = {offset}U;")
    lines.append(f"}}  // namespace {namespace_name}")
    return lines


def _render_named_constants(
    namespace_name: str,
    cpp_type: str,
    values: Mapping[str, Any],
) -> list[str]:
    lines = [f"namespace {namespace_name} {{"]
    ordered = sorted(
        ((name, int(value)) for name, value in values.items()),
        key=lambda item: (item[1], item[0]),
    )
    for name, value in ordered:
        cpp_name = _cpp_field_name(name.lower())
        lines.append(
            f"inline constexpr {cpp_type} {cpp_name} = {value}U;"
        )
    lines.append(f"}}  // namespace {namespace_name}")
    return lines


def _render_header(
    record_schema: _LoadedSchema,
    checkpoint_schema: _LoadedSchema,
    record: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
) -> bytes:
    record_hex = record_schema.sha256.hex()
    checkpoint_hex = checkpoint_schema.sha256.hex()
    lines = [
        "// Generated by generate_phase3_control_schema.py. Do not edit.",
        "// Schema hashes cover all exact input bytes, including whitespace.",
        "#pragma once",
        "",
        "#include <array>",
        "#include <cstddef>",
        "#include <cstdint>",
        "#include <string_view>",
        "",
        "namespace l2flow::control::phase3_schema_v1 {",
        "",
    ]
    lines.extend(_render_digest("kControlRecordV1SchemaSha256", record_schema.sha256))
    lines.extend(
        [
            "inline constexpr std::string_view",
            "    kControlRecordV1SchemaSha256Hex =",
            f'        "{record_hex}";',
            "",
        ]
    )
    lines.extend(
        _render_digest(
            "kControlCheckpointV1SchemaSha256", checkpoint_schema.sha256
        )
    )
    lines.extend(
        [
            "inline constexpr std::string_view",
            "    kControlCheckpointV1SchemaSha256Hex =",
            f'        "{checkpoint_hex}";',
            "",
            "inline constexpr std::uint32_t kControlRecordV1Magic =",
            f"    0x{int(record['magic']):08x}U;",
            "inline constexpr std::uint16_t kControlRecordV1Version =",
            f"    {int(record['version'])}U;",
            "inline constexpr std::uint16_t kControlRecordV1HeaderBytes =",
            f"    {int(record['header_size'])}U;",
            "inline constexpr std::size_t kControlRecordV1Bytes =",
            f"    {int(record['record_size'])}U;",
            "",
            "inline constexpr std::uint32_t kControlCheckpointV1Magic =",
            f"    0x{int(checkpoint['magic']):08x}U;",
            "inline constexpr std::uint16_t kControlCheckpointV1Version =",
            f"    {int(checkpoint['version'])}U;",
            "inline constexpr std::size_t kControlCheckpointV1HeaderBytes =",
            f"    {int(checkpoint['header_size'])}U;",
            "inline constexpr std::size_t kControlCheckpointV1EntryBytes =",
            f"    {int(checkpoint['entry_size'])}U;",
            "inline constexpr std::size_t kControlCheckpointV1TrailerBytes =",
            f"    {int(checkpoint['trailer_size'])}U;",
            "inline constexpr std::size_t kControlCheckpointV1MaximumEntries =",
            f"    {int(checkpoint['maximum_entries'])}U;",
            "inline constexpr std::size_t kControlCheckpointV1MinimumBytes =",
            f"    {int(checkpoint['minimum_bytes'])}U;",
            "inline constexpr std::size_t kControlCheckpointV1MaximumBytes =",
            f"    {int(checkpoint['maximum_bytes'])}U;",
            "",
        ]
    )
    lines.extend(
        _render_named_constants(
            "control_type_v1_value", "std::uint16_t", _RECORD_CONTROL_TYPES
        )
    )
    lines.append("")
    lines.extend(
        _render_named_constants(
            "control_record_flag_v1_value", "std::uint32_t", _RECORD_FLAGS
        )
    )
    lines.append("")
    lines.extend(
        _render_named_constants(
            "control_session_phase_v1_value", "std::uint8_t", _SESSION_PHASES
        )
    )
    lines.append("")
    lines.extend(
        _render_named_constants(
            "subscription_policy_v1_value",
            "std::uint8_t",
            _SUBSCRIPTION_POLICIES,
        )
    )
    lines.append("")
    lines.extend(
        _render_named_constants(
            "control_checkpoint_flag_v1_value",
            "std::uint32_t",
            _CHECKPOINT_FLAGS,
        )
    )
    lines.append("")
    lines.extend(
        _render_named_constants(
            "quality_flag_v1_mask", "std::uint64_t", record["quality_values"]
        )
    )
    lines.append("")
    lines.extend(
        _render_offsets("control_record_v1_offset", record["layout"])
    )
    lines.append("")
    lines.extend(
        _render_offsets("control_checkpoint_v1_offset", checkpoint["header"])
    )
    lines.append("")
    lines.extend(
        _render_offsets("control_checkpoint_entry_v1_offset", checkpoint["entry"])
    )
    lines.append("")
    lines.extend(
        _render_offsets(
            "control_checkpoint_trailer_v1_offset", checkpoint["trailer"]
        )
    )
    lines.extend(
        [
            "",
            "}  // namespace l2flow::control::phase3_schema_v1",
            "",
        ]
    )
    return "\n".join(lines).encode("ascii")


def _write_if_changed(path: Path, data: bytes) -> None:
    try:
        if path.exists() and path.read_bytes() == data:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.", dir=path.parent
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as output:
                output.write(data)
                output.flush()
            os.chmod(temporary, 0o644)
            os.replace(temporary, path)
        except BaseException:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
            raise
    except OSError as error:
        _fail("output header", f"cannot write {path}: {error}")


def _parse_arguments(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "validate the frozen Phase-3 control schemas and generate a "
            "deterministic C++ constants header"
        )
    )
    parser.add_argument("record_schema", type=Path)
    parser.add_argument("checkpoint_schema", type=Path)
    parser.add_argument("output_header", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parse_arguments(sys.argv[1:] if argv is None else argv)
    try:
        output_resolved = arguments.output_header.resolve(strict=False)
        for input_path, label in (
            (arguments.record_schema, "record schema"),
            (arguments.checkpoint_schema, "checkpoint schema"),
        ):
            if input_path.resolve(strict=False) == output_resolved:
                _fail("output header", f"must not overwrite the {label}")

        record_schema = _load_schema(arguments.record_schema, "record schema")
        checkpoint_schema = _load_schema(
            arguments.checkpoint_schema, "checkpoint schema"
        )
        record = _validate_record_schema(record_schema)
        checkpoint = _validate_checkpoint_schema(checkpoint_schema)
        if record["quality_values"] != checkpoint["quality_values"]:
            _fail(
                "schemas",
                "quality_flags_v1 assignments differ between record and checkpoint",
            )
        if record_schema.document["crc32c"] != checkpoint_schema.document["crc32c"]:
            _fail("schemas", "CRC-32C parameter blocks differ")
        output = _render_header(
            record_schema, checkpoint_schema, record, checkpoint
        )
        _write_if_changed(arguments.output_header, output)
    except SchemaValidationError as error:
        print(f"{Path(sys.argv[0]).name}: error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
