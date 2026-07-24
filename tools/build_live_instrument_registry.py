#!/usr/bin/env python3
"""Build a deterministic V1 instrument registry from feeder reference data.

This is a deployment aid, not a reference-data classifier.  It preserves the
exact SecurityID keys published by the feeder, assigns deterministic nonzero
instrument IDs, and deliberately records quantity/security/scope as unknown.
The resulting registry is sufficient for production routing and history
identity without inventing unsupported asset metadata.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import struct
from pathlib import Path


DOMAIN = b"L2FLOW_PHASE4_INSTRUMENT_REGISTRY_V1\x00"
MAGIC = b"L2FLOW_INSTRUMENT_REGISTRY_V1"
# The MDL Shenzhen market messages publish the exact four-byte source key
# ``b"102 "``.  InstrumentRegistryV1 deliberately treats this field as an
# opaque byte string, so dropping the trailing space makes every live
# Shenzhen lookup unknown even though the SecurityID itself is present.
SZ_SECURITY_ID_SOURCE = b"102 "


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sh-reference", type=Path, required=True)
    parser.add_argument("--sz-reference", type=Path, required=True)
    parser.add_argument("--capture-csv", type=Path, action="append", default=[])
    parser.add_argument("--registry-version", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report-json", type=Path, required=True)
    args = parser.parse_args()
    if args.registry_version <= 0 or args.registry_version > (1 << 64) - 1:
        parser.error("--registry-version must be in 1..2^64-1")
    if args.output == args.report_json:
        parser.error("registry and report paths must differ")
    return args


def regular_snapshot(path: Path) -> tuple[int, int]:
    stat = path.stat()
    if not path.is_file():
        raise ValueError(f"not a regular file: {path}")
    return stat.st_size, stat.st_mtime_ns


def add_reference_ids(path: Path, target: set[bytes]) -> dict[str, object]:
    size, mtime_ns = regular_snapshot(path)
    before = len(target)
    rows = 0
    with path.open("rb") as source:
        header = source.readline()
        if not header.startswith(b"SecurityID,"):
            raise ValueError(f"unexpected SecurityID reference header: {path}")
        consumed = len(header)
        while consumed < size:
            line = source.readline(size - consumed)
            if not line:
                break
            consumed += len(line)
            if not line.endswith(b"\n"):
                break
            security_id = line.split(b",", 1)[0]
            if not security_id or any(byte < 0x21 or byte > 0x7E for byte in security_id):
                raise ValueError(f"invalid SecurityID at row {rows + 2}: {path}")
            target.add(security_id)
            rows += 1
    return {
        "path": str(path),
        "snapshot_size_bytes": size,
        "snapshot_mtime_ns": mtime_ns,
        "complete_rows": rows,
        "new_unique_keys": len(target) - before,
    }


def add_capture_ids(
    path: Path, sh: set[bytes], sz: set[bytes]
) -> dict[str, object]:
    size, mtime_ns = regular_snapshot(path)
    rows = 0
    added_sh = 0
    added_sz = 0
    with path.open("r", encoding="utf-8", newline="") as source:
        reader = csv.DictReader(source)
        required = {"ServiceID", "SecurityID", "SecurityIDSource"}
        if reader.fieldnames is None or not required.issubset(reader.fieldnames):
            raise ValueError(f"capture lacks instrument identity columns: {path}")
        for row in reader:
            rows += 1
            security_id = row["SecurityID"].encode("ascii")
            if not security_id:
                continue
            if row["ServiceID"] == "4":
                before = len(sh)
                sh.add(security_id)
                added_sh += len(sh) - before
            elif row["ServiceID"] == "6":
                source_id = row["SecurityIDSource"].encode("ascii")
                if source_id != SZ_SECURITY_ID_SOURCE:
                    raise ValueError(
                        f"unsupported Shenzhen SecurityIDSource {source_id!r}: {path}"
                    )
                before = len(sz)
                sz.add(security_id)
                added_sz += len(sz) - before
    return {
        "path": str(path),
        "snapshot_size_bytes": size,
        "snapshot_mtime_ns": mtime_ns,
        "rows": rows,
        "new_shanghai_keys": added_sh,
        "new_shenzhen_keys": added_sz,
    }


def encode_registry(
    version: int, sh: set[bytes], sz: set[bytes]
) -> tuple[bytes, str, list[tuple[int, bytes, bytes, int]]]:
    keys = [(1, b"", value) for value in sh]
    keys.extend((2, SZ_SECURITY_ID_SOURCE, value) for value in sz)
    keys.sort(key=lambda value: (value[0], value[1], value[2]))
    if not keys:
        raise ValueError("registry would be empty")
    if len(keys) > 1_000_000:
        raise ValueError("registry exceeds V1 entry limit")

    entries: list[tuple[int, bytes, bytes, int]] = []
    lines = [MAGIC + b"\t" + str(version).encode("ascii") + b"\n"]
    digest = hashlib.sha256()
    digest.update(DOMAIN)
    digest.update(struct.pack("<QQ", version, len(keys)))
    for instrument_id, (market, source_id, security_id) in enumerate(keys, 1):
        entries.append((market, source_id, security_id, instrument_id))
        digest.update(struct.pack("<B", market))
        digest.update(struct.pack("<Q", len(source_id)))
        digest.update(source_id)
        digest.update(struct.pack("<Q", len(security_id)))
        digest.update(security_id)
        digest.update(struct.pack("<IBBB", instrument_id, 0, 0, 0))
        market_text = b"sh" if market == 1 else b"sz"
        source_hex = b"-" if not source_id else source_id.hex().encode("ascii")
        lines.append(
            str(instrument_id).encode("ascii")
            + b"\t"
            + market_text
            + b"\t"
            + source_hex
            + b"\t"
            + security_id.hex().encode("ascii")
            + b"\tunknown\tunknown\tunknown\n"
        )
    return b"".join(lines), digest.hexdigest(), entries


def write_exclusive(path: Path, data: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        offset = 0
        while offset < len(data):
            offset += os.write(descriptor, data[offset:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def main() -> int:
    args = parse_args()
    sh: set[bytes] = set()
    sz: set[bytes] = set()
    sources = {
        "shanghai_reference": add_reference_ids(args.sh_reference, sh),
        "shenzhen_reference": add_reference_ids(args.sz_reference, sz),
        "captures": [],
    }
    for capture in args.capture_csv:
        sources["captures"].append(add_capture_ids(capture, sh, sz))

    registry, registry_sha256, entries = encode_registry(
        args.registry_version, sh, sz
    )
    args.output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    write_exclusive(args.output, registry)
    report = {
        "schema_version": 1,
        "registry_version": args.registry_version,
        "registry_sha256": registry_sha256,
        "registry_file_sha256": hashlib.sha256(registry).hexdigest(),
        "entry_count": len(entries),
        "shanghai_entry_count": len(sh),
        "shenzhen_entry_count": len(sz),
        "shenzhen_security_id_source_hex": SZ_SECURITY_ID_SOURCE.hex(),
        "metadata_policy": {
            "quantity_unit": "unknown",
            "security_type": "unknown",
            "asset_scope": "unknown",
            "statement": "No asset metadata was inferred from code prefixes.",
        },
        "sources": sources,
        "output": str(args.output),
    }
    write_exclusive(
        args.report_json,
        (json.dumps(report, ensure_ascii=True, indent=2) + "\n").encode("utf-8"),
    )
    print(json.dumps(report, ensure_ascii=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
