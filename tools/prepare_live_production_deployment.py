#!/usr/bin/env python3
"""Prepare one fresh-only formal production deployment generation.

The script fails if the generation root exists. It intentionally provisions
configuration files and directories only. The typed reserve coordinator is a
separate irreversible step and must precede the full
mdl-production-router --check, because that check verifies the coordinator
lease marker and exact four-entry SCAFFOLDING state.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
from pathlib import Path


KINDS = ("sh-snapshot", "sh-tick", "sz-snapshot", "sz-tick")
SLUGS = KINDS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--registry-report", type=Path, required=True)
    parser.add_argument("--sdk-library", type=Path, required=True)
    parser.add_argument("--sdk-log-root", type=Path, required=True)
    parser.add_argument("--capture-date", type=int, required=True)
    parser.add_argument("--address", default="127.0.0.1:9112")
    args = parser.parse_args()
    if not args.root.is_absolute() or args.root != Path(os.path.normpath(args.root)):
        parser.error("--root must be a normalized absolute path")
    if not args.sdk_log_root.is_absolute() or args.sdk_log_root != Path(
        os.path.normpath(args.sdk_log_root)
    ):
        parser.error("--sdk-log-root must be a normalized absolute path")
    if args.sdk_log_root.exists():
        parser.error("--sdk-log-root must not already exist")
    if len(str(args.capture_date)) != 8:
        parser.error("--capture-date must be YYYYMMDD")
    if not args.sdk_library.is_file():
        parser.error("--sdk-library must exist")
    if not args.registry.is_file() or not args.registry_report.is_file():
        parser.error("registry inputs must already exist")
    return args


def mkdir(path: Path, mode: int = 0o700) -> None:
    os.mkdir(path, mode)
    os.chmod(path, mode)


def write_exclusive(path: Path, data: bytes, mode: int) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        os.fchmod(fd, mode)
        offset = 0
        while offset < len(data):
            offset += os.write(fd, data[offset:])
        os.fsync(fd)
    finally:
        os.close(fd)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def identity() -> str:
    value = secrets.token_hex(16)
    if value == "0" * 32:
        return identity()
    return value


def endpoint_bytes(kind: str, address: str) -> bytes:
    value = {
        "schema_version": 1,
        "ingress_kind": kind,
        "name": f"local-feeder-{kind}-production-live",
        "resolved_server_address": address,
        "message_encoding": 1,
        "merge_message": False,
        "send_mac_auth": False,
        "server_select": False,
    }
    return (json.dumps(value, separators=(",", ":"), ensure_ascii=True) + "\n").encode()


def declaration_digest(label: str, root: Path) -> str:
    return sha256_bytes(
        ("L2FLOW_LIVE_DECLARATION_V1\x00" + label + "\x00" + str(root)).encode()
    )


def main() -> int:
    args = parse_args()
    if args.root.exists():
        raise FileExistsError(f"generation root already exists: {args.root}")

    # The registry was built first in a private staging directory because its
    # canonical digest is required by the final exact manifest.
    registry_report = json.loads(args.registry_report.read_text(encoding="utf-8"))
    registry_bytes = args.registry.read_bytes()
    if sha256_bytes(registry_bytes) != registry_report["registry_file_sha256"]:
        raise ValueError("registry file no longer matches its build report")

    mkdir(args.root)
    deployment = args.root / "deployment"
    raw = args.root / "raw"
    frontier = args.root / "frontier"
    canonical = args.root / "canonical"
    route = args.root / "route"
    endpoints = args.root / "endpoints"
    secrets_dir = args.root / "secrets"
    logs = args.sdk_log_root
    metrics = args.root / "metrics"
    for path in (
        deployment,
        raw,
        frontier,
        canonical,
        route,
        endpoints,
        secrets_dir,
        metrics,
    ):
        mkdir(path)
    mkdir(logs)

    registry_name = "instrument-registry-v1.tsv"
    write_exclusive(deployment / registry_name, registry_bytes, 0o600)

    endpoint_paths: list[Path] = []
    endpoint_hashes: list[str] = []
    log_prefixes: list[Path] = []
    metrics_paths: list[Path] = []
    for kind in KINDS:
        data = endpoint_bytes(kind, args.address)
        path = endpoints / f"{kind}.json"
        write_exclusive(path, data, 0o600)
        endpoint_paths.append(path)
        endpoint_hashes.append(sha256_bytes(data))

        log_dir = logs / kind
        metric_dir = metrics / kind
        mkdir(log_dir)
        mkdir(metric_dir)
        write_exclusive(
            log_dir / ".l2flow-sdk-log-directory-v1",
            b"l2flow-sdk-log-directory-v1\n",
            0o444,
        )
        log_prefixes.append(log_dir / "mdl")
        metrics_paths.append(metric_dir / "mdl.prom")

    credential = secrets_dir / "mdl.credential"
    write_exclusive(credential, b"l2flow-production-router\n", 0o400)

    device_id = os.stat(raw, follow_symlinks=False).st_dev
    route_instance = identity()
    coordinator_identity = identity()
    quota_identity = declaration_digest(
        "no-automated-quota-enforcement-proof", args.root
    )
    mount_identity = declaration_digest(
        f"device:{device_id}:path:{raw}", args.root
    )
    stream_day_ids = [identity() for _ in KINDS]
    recovery_ids = [identity() for _ in KINDS]
    writer_ids = [identity() for _ in KINDS]

    rows: list[tuple[str, object]] = [
        ("mode", "fresh"),
        ("sdk_library_path", args.sdk_library),
        ("credential_path", credential),
        ("credential_name", "local-feeder-production-router"),
        ("raw_root", raw),
        ("frontier_root", frontier),
        ("canonical_root", canonical),
        ("route_root", route),
        ("instrument_registry_file", registry_name),
        ("instrument_registry_version", registry_report["registry_version"]),
        ("instrument_registry_sha256", registry_report["registry_sha256"]),
        ("capture_date", args.capture_date),
        ("trade_date", args.capture_date),
        ("route_generation", 1),
        ("route_previous_generation", 0),
        ("route_instance", route_instance),
        ("coordinator_identity", coordinator_identity),
        ("coordinator_device_id", device_id),
        ("coordinator_quota_sha256", quota_identity),
        ("coordinator_mount_sha256", mount_identity),
        ("clock_source_config", "CLOCK_REALTIME+CLOCK_MONOTONIC_RAW:V1"),
        ("raw.heartbeat_interval_seconds", 10),
        ("raw.heartbeat_timeout_seconds", 30),
        ("raw.include_optional_index", "false"),
        ("raw.max_message_bytes", 65536),
        ("raw.ring_capacity_bytes", 268435456),
        ("raw.ring_stall_budget_seconds", 5),
        ("raw.segment_target_bytes", 536870912),
        ("raw.segment_max_age_seconds", 300),
        ("raw.sync_interval_milliseconds", 100),
        ("raw.sync_bytes", 67108864),
        ("raw.sparse_index_every_records", 4096),
        ("raw.sparse_index_every_bytes", 4194304),
        ("raw.reserve_domain_id", f"production-live-{args.capture_date}"),
        ("raw.reserve_coordinator_socket", f"/tmp/l2flow-production-live-{route_instance}.sock"),
        ("raw.reserve_ack_timeout_milliseconds", 1000),
        ("raw.emergency_reserve_bytes", 268435456),
        ("raw.maximum_manifest_bytes", 1048576),
        # Five minutes of the observed high-rate sz-tick stream projects
        # close to four 512 MiB segments.  Keep deterministic headroom for
        # rate variation and a boundary rotation without weakening any byte,
        # age, or manifest bound.
        ("raw.live.max_segments", 8),
        ("raw.live.max_segment_bytes", 1073741824),
        ("raw.live.max_journal_markers", 1048576),
        ("raw.live.max_control_reattach_attempts", 3),
        ("canonical.snapshot_capacity_records_per_sink", 200000),
        ("canonical.tick_capacity_records_per_sink", 1200000),
        ("canonical.quality_capacity_records_per_sink", 15000000),
        ("canonical.control_capacity_records_per_sink", 1024),
        ("history.physical_workers", 16),
        ("history.queue_capacity", 65536),
        ("history.maximum_inflight_per_source", 16384),
        ("history.chunk_record_capacity", 4096),
        ("history.maximum_records_per_query", 65536),
        ("history.maximum_records_per_shard", 1200000),
        ("history.maximum_instruments_per_shard", 65536),
        ("history.maximum_payload_bytes_per_shard", 1073741824),
        ("service.activation_timeout_ms", 120000),
        ("service.drain_timeout_ms", 120000),
        ("service.writer_idle_heartbeat_interval_ms", 1000),
        ("service.writer_heartbeat_timeout_ms", 5000),
    ]
    for index, kind in enumerate(KINDS):
        prefix = f"source.{index}."
        rows.extend(
            (
                (prefix + "kind", "mdl-ingress-" + kind),
                (prefix + "stream_slug", SLUGS[index]),
                (prefix + "endpoint_path", endpoint_paths[index]),
                (prefix + "endpoint_sha256", endpoint_hashes[index]),
                (prefix + "sdk_log_prefix", log_prefixes[index]),
                (prefix + "metrics_path", metrics_paths[index]),
                (prefix + "stream_day_id", stream_day_ids[index]),
                (prefix + "recovery_attempt_id", recovery_ids[index]),
                (prefix + "writer_instance", writer_ids[index]),
                (prefix + "source_generation", 1),
                (prefix + "canonical_generation", 1),
                (prefix + "connect_generation", 1),
                (prefix + "scaffolding_allocation_cap", 67108864),
                (prefix + "safe_stop_template_id", index + 1),
            )
        )

    manifest = b"L2FLOW_PRODUCTION_DEPLOYMENT_V1\n" + b"".join(
        f"{key}\t{value}\n".encode("ascii") for key, value in rows
    )
    manifest_path = deployment / "production-v1.tsv"
    write_exclusive(manifest_path, manifest, 0o600)
    manifest_sha256 = sha256_bytes(manifest)
    preparation = {
        "schema_version": 1,
        "root": str(args.root),
        "deployment_directory": str(deployment),
        "manifest": str(manifest_path),
        "manifest_sha256": manifest_sha256,
        "sdk_library_path": str(args.sdk_library),
        "sdk_log_root": str(logs),
        "sdk_library_selection_policy": "operator_authorized_path",
        "sdk_automated_approval_gate": False,
        "endpoint_address": args.address,
        "capture_date": args.capture_date,
        "registry_version": registry_report["registry_version"],
        "registry_sha256": registry_report["registry_sha256"],
        "registry_entry_count": registry_report["entry_count"],
        "raw_device_id": device_id,
        "quota_identity_note": "Declaration only; no automated quota enforcement proof.",
        "physical_emergency_reserve_inventory_provisioned": False,
    }
    write_exclusive(
        args.root / "preparation.json",
        (json.dumps(preparation, indent=2) + "\n").encode(),
        0o600,
    )
    print(json.dumps(preparation, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
