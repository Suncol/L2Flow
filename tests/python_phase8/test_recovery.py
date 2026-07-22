from __future__ import annotations

import copy
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import struct
import sys
import unittest


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY_ROOT / "python"))
sys.path.insert(0, str(REPOSITORY_ROOT / "tests" / "python"))

from helpers import make_batch  # noqa: E402
from l2flow_factor import (  # noqa: E402
    ClockSemantics,
    FactorCheckpoint,
    FactorSpec,
    FactorTransactionRuntime,
    InputFamily,
    InputMode,
    InputSpec,
    RUNTIME_STATE_CODEC_V1,
    RuntimeOutput,
    encode_checkpoint,
)
from l2flow_history.manifest import SourceNamespace  # noqa: E402
from l2flow_history.recovery import (  # noqa: E402
    ArtifactHashBinding,
    CanonicalColdStartRequest,
    CanonicalGenerationIdentity,
    CanonicalSinkSpec,
    FactorCheckpointInputCoverage,
    FactorInputTarget,
    FactorRecoveryIdentity,
    FactorRecoveryRequest,
    FactorReplaySpan,
    IsolatedRebuildRequest,
    LatestStateCheckpointCursor,
    LatestStateCheckpointEvidence,
    LatestStateCheckpointExpectation,
    LatestStateConfigIdentity,
    LatestStateDurabilityBarrier,
    LatestStateFullReplayReset,
    LatestStateInputFamily,
    LatestStateInputTarget,
    LatestStateRecoveryRequest,
    LatestStateReplaySpan,
    LatestStateWriterReuseLease,
    NativeMuxInputRange,
    NativeSafeMuxDecision,
    NativeSafeMuxEvidence,
    RawReplaySpan,
    RawReplayStage,
    RawRouteIdentity,
    RecoveryConflictError,
    RecoveryError,
    RecoveryIntegrityError,
    RecoveryMode,
    SchemaBinding,
    SourceHealth,
    build_canonical_cold_start_plan,
    build_factor_recovery_plan,
    build_isolated_rebuild_plan,
    build_latest_state_recovery_plan,
    configured_raw_routes_sha256,
    encode_rebuild_cutover_certificate,
    factor_input_routes_sha256,
    latest_input_routes_sha256,
    native_safe_mux_binding_sha256,
    validate_rebuild_cutover_certificate,
)


TRADE_DATE = 20260722


def h(value: int) -> bytes:
    return bytes([value]) * 32


def b16(value: int) -> bytes:
    return bytes([value]) * 16


def raw_route(source_stream_id: int, marker: int) -> RawRouteIdentity:
    return RawRouteIdentity(
        trade_date=TRADE_DATE,
        source_stream_id=source_stream_id,
        origin_capture_date=TRADE_DATE,
        origin_stream_day_id=b16(marker),
        origin_source_writer_instance=b16(marker + 10),
        origin_source_generation=3,
        clock_epoch_algorithm=1,
        clock_epoch_digest=h(7),
        day_begin_ingress_sequence=1,
        day_begin_wal_pos=0,
        replay_end_ingress_sequence=4,
        replay_end_wal_pos=400,
        current_raw_durable_wal_pos=400,
        health=SourceHealth.HEALTHY,
        market="SH" if source_stream_id % 2 else "SZ",
        configured_feed_role=f"feed{source_stream_id}",
    )


def canonical_fixture() -> tuple[CanonicalColdStartRequest, object]:
    routes = (raw_route(10, 1), raw_route(11, 2))
    identity = CanonicalGenerationIdentity(
        trade_date=TRADE_DATE,
        previous_canonical_generation=4,
        candidate_canonical_generation=5,
        previous_recovery_writer_instance=b16(30),
        candidate_recovery_writer_instance=b16(31),
        recovery_attempt_id=b16(32),
        shard_count=1,
        canonical_schema_sha256=h(3),
        canonical_dtype_sha256=h(4),
        registry_version=6,
        registry_sha256=h(6),
        normalizer_build_sha256=h(8),
        normalizer_config_sha256=h(9),
        clock_epoch_algorithm=1,
        clock_epoch_digest=h(7),
    )
    spans: list[RawReplaySpan] = []
    for route in routes:
        for stage, verifier in (
            (RawReplayStage.API_SYS, h(40 + route.source_stream_id)),
            (RawReplayStage.MARKET, h(50 + route.source_stream_id)),
        ):
            spans.append(
                RawReplaySpan(
                    route=route,
                    stage=stage,
                    begin_wal_pos=0,
                    end_wal_pos=400,
                    first_ingress_sequence=1,
                    last_ingress_sequence=4,
                    record_count=4,
                    ordered_record_content_sha256=h(20 + route.source_stream_id),
                    envelope_verifier_sha256=verifier,
                    exact_record_boundaries_verified=True,
                )
            )
    sinks: list[CanonicalSinkSpec] = []
    for route in routes:
        for family in (
            InputFamily.SNAPSHOT,
            InputFamily.TICK,
            InputFamily.QUALITY,
            InputFamily.CONTROL,
        ):
            sinks.append(
                CanonicalSinkSpec(
                    route=route,
                    family=family,
                    shard_id=0,
                    canonical_generation=5,
                    capacity_records=100,
                    required_records_upper_bound=10,
                    descriptor_identity_sha256=h(
                        70 + route.source_stream_id + family.canonical_event_type
                    ),
                    fresh_file=True,
                    empty=True,
                )
            )
    request = CanonicalColdStartRequest(
        identity=identity,
        expected_raw_routes=tuple(reversed(routes)),
        configured_route_manifest_sha256=configured_raw_routes_sha256(routes),
        raw_routes=routes,
        replay_spans=tuple(reversed(spans)),
        sinks=tuple(reversed(sinks)),
        frontier_pages_fresh=True,
        normalizers_fresh=True,
        frontier_initial_processed_ingress_sequence=0,
        frontier_initial_processed_wal_pos=0,
    )
    return request, build_canonical_cold_start_plan(request)


class CanonicalRecoveryTests(unittest.TestCase):
    def test_happy_path_is_order_independent_and_plan_only(self) -> None:
        request, plan = canonical_fixture()
        reordered = replace(
            request,
            expected_raw_routes=tuple(reversed(request.expected_raw_routes)),
            raw_routes=tuple(reversed(request.raw_routes)),
            replay_spans=tuple(reversed(request.replay_spans)),
            sinks=tuple(reversed(request.sinks)),
        )
        other = build_canonical_cold_start_plan(reordered)
        self.assertEqual(plan.plan_sha256, other.plan_sha256)
        self.assertFalse(plan.executes_repair)
        self.assertFalse(plan.deletes_data)
        self.assertFalse(plan.authorizes_alias_switch)
        self.assertEqual(len(plan.sinks), 8)

    def test_full_expected_object_and_per_route_sink_are_required(self) -> None:
        request, _ = canonical_fixture()
        drifted = replace(
            request.expected_raw_routes[0],
            replay_end_wal_pos=401,
            current_raw_durable_wal_pos=401,
        )
        expected = (drifted, request.expected_raw_routes[1])
        with self.assertRaises(RecoveryConflictError):
            build_canonical_cold_start_plan(
                replace(
                    request,
                    expected_raw_routes=expected,
                    configured_route_manifest_sha256=(
                        configured_raw_routes_sha256(expected)
                    ),
                )
            )
        with self.assertRaises(RecoveryConflictError):
            build_canonical_cold_start_plan(
                replace(request, sinks=request.sinks[:-1])
            )

    def test_cross_stage_content_must_match_but_verifiers_may_differ(self) -> None:
        request, plan = canonical_fixture()
        self.assertIsNotNone(plan)
        market_index = next(
            index
            for index, span in enumerate(request.replay_spans)
            if span.stage is RawReplayStage.MARKET
        )
        spans = list(request.replay_spans)
        spans[market_index] = replace(
            spans[market_index], ordered_record_content_sha256=h(99)
        )
        with self.assertRaises(RecoveryConflictError):
            build_canonical_cold_start_plan(
                replace(request, replay_spans=tuple(spans))
            )

    def test_extra_span_with_same_route_key_but_drifted_cut_is_rejected(self) -> None:
        request, _ = canonical_fixture()
        original = request.replay_spans[0]
        drifted_route = replace(
            original.route,
            replay_end_wal_pos=401,
            current_raw_durable_wal_pos=401,
        )
        extra = replace(original, route=drifted_route, end_wal_pos=401)
        with self.assertRaises(RecoveryConflictError):
            build_canonical_cold_start_plan(
                replace(request, replay_spans=request.replay_spans + (extra,))
            )

    def test_old_cursor_and_unhealthy_route_fail_closed(self) -> None:
        request, _ = canonical_fixture()
        with self.assertRaises(RecoveryConflictError):
            build_canonical_cold_start_plan(
                replace(
                    request,
                    resume_from_old_generation_requested=True,
                    old_processed_cursor=7,
                )
            )
        unhealthy_routes = tuple(
            replace(item, health=SourceHealth.FATAL) for item in request.raw_routes
        )
        unhealthy_sinks = tuple(
            replace(
                item,
                route=next(
                    route
                    for route in unhealthy_routes
                    if route.route_key == item.route.route_key
                ),
            )
            for item in request.sinks
        )
        with self.assertRaises(RecoveryConflictError):
            build_canonical_cold_start_plan(
                replace(
                    request,
                    expected_raw_routes=unhealthy_routes,
                    configured_route_manifest_sha256=(
                        configured_raw_routes_sha256(unhealthy_routes)
                    ),
                    raw_routes=unhealthy_routes,
                    sinks=unhealthy_sinks,
                )
            )

    def test_route_shard_cross_product_is_rejected_before_materialization(self) -> None:
        request, _ = canonical_fixture()
        oversized = replace(request.identity, shard_count=65_536)
        with self.assertRaisesRegex(RecoveryConflictError, "cross-product"):
            build_canonical_cold_start_plan(replace(request, identity=oversized))


def latest_namespace(
    family: LatestStateInputFamily,
    source_stream_id: int,
    *,
    shard_id: int = 0,
) -> SourceNamespace:
    canonical_family = (
        InputFamily.SNAPSHOT
        if family is LatestStateInputFamily.SNAPSHOT
        else InputFamily.TICK
    )
    return SourceNamespace(
        trade_date=TRADE_DATE,
        source_stream_id=source_stream_id,
        origin_capture_date=TRADE_DATE,
        origin_stream_day_id=b16(source_stream_id),
        family=canonical_family,
        shard_id=shard_id,
        origin_source_writer_instance=b16(source_stream_id + 20),
        origin_source_generation=2,
        canonical_generation=5,
        clock_epoch_algorithm=1,
        clock_epoch_digest=h(7),
        schema_sha256=h(3),
        dtype_sha256=h(4),
        registry_version=6,
        registry_sha256=h(6),
        normalizer_build_sha256=h(8),
        normalizer_config_sha256=h(9),
    )


def latest_checkpoint_fixture(
    include_tick: bool,
) -> tuple[LatestStateRecoveryRequest, object]:
    config = LatestStateConfigIdentity(
        state_generation=5,
        state_writer_instance=b16(90),
        shard_id=0,
        shard_count=16,
        canonical_schema_sha256=h(3),
        canonical_dtype_sha256=h(4),
        registry_version=6,
        registry_sha256=h(6),
        state_schema_sha256=h(10),
        state_config_sha256=h(11),
    )
    checkpoint_sha = h(12)
    common_cut = h(13)
    route_specs = [
        (LatestStateInputFamily.SNAPSHOT, latest_namespace(
            LatestStateInputFamily.SNAPSHOT, 20
        ), 10, 100)
    ]
    if include_tick:
        route_specs.append(
            (
                LatestStateInputFamily.TICK_QUALITY,
                latest_namespace(LatestStateInputFamily.TICK_QUALITY, 21),
                20,
                200,
            )
        )
    cursors = tuple(
        LatestStateCheckpointCursor(
            family=family,
            namespace=namespace,
            exclusive_canonical_cursor=cursor,
            max_consumed_origin_wal_end_pos=wal,
            external_cursor_receipt_sha256=h(14 + index),
            bound_checkpoint_sha256=checkpoint_sha,
            bound_common_cut_identity_sha256=common_cut,
        )
        for index, (family, namespace, cursor, wal) in enumerate(route_specs)
    )
    barriers = tuple(
        LatestStateDurabilityBarrier(
            family=family,
            namespace=namespace,
            checkpoint_exclusive_canonical_cursor=cursor,
            max_consumed_shard_event_id=1000 + index,
            max_consumed_origin_ingress_sequence=2000 + index,
            max_consumed_origin_wal_end_pos=wal,
            current_raw_durable_wal_pos=wal + 20,
            raw_authority_evidence_sha256=common_cut,
        )
        for index, (family, namespace, cursor, wal) in enumerate(route_specs)
    )
    checkpoint = LatestStateCheckpointEvidence(
        config=config,
        checkpoint_sha256=checkpoint_sha,
        payload_sha256=h(16),
        logical_state_sha256=h(17),
        common_cut_identity_sha256=common_cut,
        cursors=tuple(reversed(cursors)),
        durability_barriers=tuple(reversed(barriers)),
        writer_quiesced=True,
        durability_barrier_satisfied=True,
        checkpoint_codec_validated=True,
        logical_hash_validated=True,
    )
    targets = tuple(
        LatestStateInputTarget(
            family=family,
            namespace=namespace,
            end_exclusive_canonical_cursor=cursor + 2,
            end_max_consumed_origin_wal_end_pos=wal + 2,
            health=SourceHealth.HEALTHY,
        )
        for family, namespace, cursor, wal in route_specs
    )
    spans = tuple(
        LatestStateReplaySpan(
            family=family,
            namespace=namespace,
            begin_exclusive_canonical_cursor=cursor,
            end_exclusive_canonical_cursor=cursor + 2,
            prior_max_consumed_origin_wal_end_pos=wal,
            end_max_consumed_origin_wal_end_pos=wal + 2,
            current_raw_durable_wal_pos=wal + 20,
            record_count=2,
            ordered_content_sha256=h(30 + index),
            raw_authority_evidence_sha256=h(40 + index),
            exact_records_verified=True,
            authoritative_snapshot_payload=(
                family is LatestStateInputFamily.SNAPSHOT
            ),
            health=SourceHealth.HEALTHY,
        )
        for index, (family, namespace, cursor, wal) in enumerate(route_specs)
    )
    request = LatestStateRecoveryRequest(
        trade_date=TRADE_DATE,
        recovery_run_id=b16(91),
        output_config=config,
        expected_input_targets=tuple(reversed(targets)),
        configured_input_route_manifest_sha256=latest_input_routes_sha256(targets),
        input_targets=targets,
        replay_spans=tuple(reversed(spans)),
        targets_all_zero=True,
        expected_final_logical_state_sha256=h(18),
        checkpoint=checkpoint,
        checkpoint_expectation=LatestStateCheckpointExpectation(
            config=config,
            checkpoint_sha256=checkpoint_sha,
            payload_sha256=h(16),
            logical_state_sha256=h(17),
        ),
        writer_reuse_lease=LatestStateWriterReuseLease(
            state_generation=5,
            state_writer_instance=b16(90),
            lease_and_fence_evidence_sha256=h(19),
            exclusive_writer_lease_held=True,
            all_writers_quiesced=True,
            exact_identity_restore_authorized=True,
        ),
    )
    return request, build_latest_state_recovery_plan(request)


class LatestRecoveryTests(unittest.TestCase):
    def test_snapshot_only_checkpoint_is_valid_and_does_not_claim_tick(self) -> None:
        _, plan = latest_checkpoint_fixture(False)
        self.assertEqual(plan.mode, RecoveryMode.LATEST_CHECKPOINT_TAIL)
        self.assertFalse(any("tick_quality" in item for item in plan.steps))
        self.assertFalse(plan.executes_restore)
        self.assertFalse(plan.authorizes_alias_switch)

    def test_optional_tick_checkpoint_is_fully_replayed_without_book_authority(self) -> None:
        request, plan = latest_checkpoint_fixture(True)
        self.assertTrue(any("tick_quality" in item for item in plan.steps))
        tick = next(
            item
            for item in request.replay_spans
            if item.family is LatestStateInputFamily.TICK_QUALITY
        )
        self.assertFalse(tick.authoritative_snapshot_payload)
        reordered = replace(
            request,
            expected_input_targets=tuple(reversed(request.expected_input_targets)),
            input_targets=tuple(reversed(request.input_targets)),
            replay_spans=tuple(reversed(request.replay_spans)),
        )
        self.assertEqual(
            plan.plan_sha256,
            build_latest_state_recovery_plan(reordered).plan_sha256,
        )

    def test_phase6_zero_barrier_and_receipt_binding_fail(self) -> None:
        request, _ = latest_checkpoint_fixture(False)
        barrier = request.checkpoint.durability_barriers[0]
        with self.assertRaises(RecoveryError):
            replace(barrier, max_consumed_shard_event_id=0)
        cursor = request.checkpoint.cursors[0]
        bad_cursor = replace(cursor, bound_checkpoint_sha256=h(99))
        with self.assertRaises(RecoveryConflictError):
            replace(request.checkpoint, cursors=(bad_cursor,))

    def test_wrong_shard_and_checkpoint_config_rebase_fail(self) -> None:
        request, _ = latest_checkpoint_fixture(False)
        target = request.input_targets[0]
        bad_namespace = replace(target.namespace, shard_id=1)
        bad_target = replace(target, namespace=bad_namespace)
        bad_span = replace(request.replay_spans[0], namespace=bad_namespace)
        targets = (bad_target,)
        with self.assertRaises(RecoveryConflictError):
            build_latest_state_recovery_plan(
                replace(
                    request,
                    expected_input_targets=targets,
                    configured_input_route_manifest_sha256=(
                        latest_input_routes_sha256(targets)
                    ),
                    input_targets=targets,
                    replay_spans=(bad_span,),
                )
            )
        rebased = replace(
            request.output_config,
            state_generation=6,
            state_writer_instance=b16(92),
        )
        with self.assertRaises(RecoveryConflictError):
            build_latest_state_recovery_plan(
                replace(request, output_config=rebased)
            )

    def test_request_evidence_binds_final_hash_and_durability_claims(self) -> None:
        request, plan = latest_checkpoint_fixture(False)
        changed = build_latest_state_recovery_plan(
            replace(request, expected_final_logical_state_sha256=h(98))
        )
        self.assertNotEqual(plan.request_evidence_sha256, changed.request_evidence_sha256)
        self.assertNotEqual(plan.plan_sha256, changed.plan_sha256)
        with self.assertRaises(RecoveryConflictError):
            build_latest_state_recovery_plan(
                replace(
                    request,
                    checkpoint=replace(
                        request.checkpoint,
                        durability_barrier_satisfied=False,
                    ),
                )
            )

    def test_full_replay_requires_fresh_reset_and_starts_at_zero(self) -> None:
        namespace = latest_namespace(LatestStateInputFamily.SNAPSHOT, 20)
        config = LatestStateConfigIdentity(
            state_generation=6,
            state_writer_instance=b16(92),
            shard_id=0,
            shard_count=16,
            canonical_schema_sha256=h(3),
            canonical_dtype_sha256=h(4),
            registry_version=6,
            registry_sha256=h(6),
            state_schema_sha256=h(10),
            state_config_sha256=h(20),
        )
        target = LatestStateInputTarget(
            family=LatestStateInputFamily.SNAPSHOT,
            namespace=namespace,
            end_exclusive_canonical_cursor=2,
            end_max_consumed_origin_wal_end_pos=2,
            health=SourceHealth.HEALTHY,
        )
        span = LatestStateReplaySpan(
            family=LatestStateInputFamily.SNAPSHOT,
            namespace=namespace,
            begin_exclusive_canonical_cursor=0,
            end_exclusive_canonical_cursor=2,
            prior_max_consumed_origin_wal_end_pos=0,
            end_max_consumed_origin_wal_end_pos=2,
            current_raw_durable_wal_pos=2,
            record_count=2,
            ordered_content_sha256=h(21),
            raw_authority_evidence_sha256=h(22),
            exact_records_verified=True,
            authoritative_snapshot_payload=True,
            health=SourceHealth.HEALTHY,
        )
        request = LatestStateRecoveryRequest(
            trade_date=TRADE_DATE,
            recovery_run_id=b16(93),
            output_config=config,
            expected_input_targets=(target,),
            configured_input_route_manifest_sha256=latest_input_routes_sha256(
                (target,)
            ),
            input_targets=(target,),
            replay_spans=(span,),
            targets_all_zero=True,
            expected_final_logical_state_sha256=h(23),
            full_replay_reset=LatestStateFullReplayReset(
                previous_state_generation=5,
                previous_state_writer_instance=b16(90),
                candidate_config=config,
                targets_all_zero=True,
                isolated_generation=True,
                reset_and_warmup_required=True,
            ),
        )
        plan = build_latest_state_recovery_plan(request)
        self.assertEqual(plan.mode, RecoveryMode.LATEST_FULL_REPLAY)
        self.assertEqual(plan.start_cursors[0].exclusive_canonical_cursor, 0)
        self.assertIsNone(plan.start_cursors[0].external_cursor_receipt_sha256)


class RuntimePlugin:
    def __init__(self) -> None:
        self.spec = FactorSpec(
            factor_id="recovery_test",
            factor_version="1",
            state_schema_version=1,
            input_mode=InputMode.TICK_ONLY,
            inputs=(InputSpec(1002, InputFamily.TICK, shard_id=1),),
            clock_semantics=ClockSemantics.RECEIVE_MONOTONIC,
        )
        self.count = 0

    def serialize_state(self) -> bytes:
        return str(self.count).encode("ascii")

    def restore_state(self, state: bytes) -> None:
        self.count = int(state.decode("ascii"))

    def on_batch(self, batch):
        self.count += len(batch.records)
        return (
            RuntimeOutput(
                ("recovery_test", str(batch.metadata.end_canonical_cursor)),
                str(self.count).encode("ascii"),
            ),
        )


def factor_fixture() -> tuple[FactorRecoveryRequest, object]:
    plugin = RuntimePlugin()
    runtime = FactorTransactionRuntime(plugin, 1, lambda batch: True)
    batch, metadata, _, _ = make_batch()
    with batch:
        runtime.process_batch(batch)
    watermark = runtime.watermark_set
    entry = watermark.entries[0]
    checkpoint = FactorCheckpoint(
        factor_id=plugin.spec.factor_id,
        factor_version=plugin.spec.factor_version,
        factor_code_sha256=h(50),
        factor_config_sha256=plugin.spec.sha256(),
        state_schema_sha256=h(51),
        registry_version=metadata.registry_version,
        registry_sha256=metadata.registry_sha256,
        watermark_set=watermark,
        state_codec=RUNTIME_STATE_CODEC_V1,
        state_bytes=runtime.checkpoint_bytes(),
    )
    namespace = SourceNamespace(
        trade_date=metadata.trade_date,
        source_stream_id=metadata.source_stream_id,
        origin_capture_date=metadata.origin_capture_date,
        origin_stream_day_id=metadata.origin_stream_day_id,
        family=metadata.family,
        shard_id=metadata.shard_id,
        origin_source_writer_instance=metadata.origin_source_writer_instance,
        origin_source_generation=metadata.origin_source_generation,
        canonical_generation=metadata.canonical_generation,
        clock_epoch_algorithm=metadata.clock_epoch_algorithm,
        clock_epoch_digest=metadata.clock_epoch_digest,
        schema_sha256=h(52),
        dtype_sha256=h(53),
        registry_version=metadata.registry_version,
        registry_sha256=metadata.registry_sha256,
        normalizer_build_sha256=h(54),
        normalizer_config_sha256=h(55),
    )
    coverage = FactorCheckpointInputCoverage(
        namespace=namespace,
        exclusive_canonical_cursor=entry.canonical_cursor,
        max_consumed_origin_wal_end_pos=entry.max_consumed_origin_wal_end_pos,
        current_raw_durable_wal_pos=entry.observed_raw_durable_wal_pos,
        coverage_certificate_sha256=h(56),
    )
    target = FactorInputTarget(
        namespace=namespace,
        end_exclusive_canonical_cursor=entry.canonical_cursor + 2,
        end_max_consumed_origin_wal_end_pos=(
            entry.max_consumed_origin_wal_end_pos + 2
        ),
        health=SourceHealth.HEALTHY,
        clock_compatible=True,
    )
    span = FactorReplaySpan(
        namespace=namespace,
        begin_exclusive_canonical_cursor=entry.canonical_cursor,
        end_exclusive_canonical_cursor=entry.canonical_cursor + 2,
        prior_max_consumed_origin_wal_end_pos=(
            entry.max_consumed_origin_wal_end_pos
        ),
        end_max_consumed_origin_wal_end_pos=(
            entry.max_consumed_origin_wal_end_pos + 2
        ),
        current_raw_durable_wal_pos=entry.max_consumed_origin_wal_end_pos + 10,
        record_count=2,
        ordered_content_sha256=h(57),
        exact_records_verified=True,
        health=SourceHealth.HEALTHY,
        clock_compatible=True,
    )
    ranges = (NativeMuxInputRange.from_span(span),)
    request_hash = h(58)
    result_hash = h(59)
    receipt_hash = h(60)
    mux_binding = native_safe_mux_binding_sha256(
        native_request_sha256=request_hash,
        native_result_sha256=result_hash,
        selection_receipt_sha256=receipt_hash,
        input_ranges=ranges,
        decision=NativeSafeMuxDecision.READY,
        live_frontiers_revalidated=True,
        all_required_inputs_present=True,
        clock_compatible=True,
        fatal_observed=False,
    )
    mux = NativeSafeMuxEvidence(
        native_binding_sha256=mux_binding,
        native_request_sha256=request_hash,
        native_result_sha256=result_hash,
        selection_receipt_sha256=receipt_hash,
        input_ranges=ranges,
        decision=NativeSafeMuxDecision.READY,
        live_frontiers_revalidated=True,
        all_required_inputs_present=True,
        clock_compatible=True,
        fatal_observed=False,
    )
    identity = FactorRecoveryIdentity(
        factor_id=plugin.spec.factor_id,
        factor_version=plugin.spec.factor_version,
        factor_code_sha256=h(50),
        factor_config_sha256=plugin.spec.sha256(),
        state_schema_sha256=h(51),
        registry_version=metadata.registry_version,
        registry_sha256=metadata.registry_sha256,
        factor_spec=plugin.spec,
        state_schema_version=1,
        factor_shard_id=1,
        trade_date=metadata.trade_date,
        clock_epoch_algorithm=metadata.clock_epoch_algorithm,
        clock_epoch_digest=metadata.clock_epoch_digest,
        canonical_schema_sha256=h(52),
        canonical_dtype_sha256=h(53),
        expected_input_namespaces=(namespace,),
        expected_input_route_manifest_sha256=factor_input_routes_sha256(
            (namespace,)
        ),
        expected_native_mux_binding_sha256=mux_binding,
    )
    checkpoint_sha = hashlib.sha256(encode_checkpoint(checkpoint)).digest()
    request = FactorRecoveryRequest(
        expected_identity=identity,
        checkpoint=checkpoint,
        checkpoint_sha256=checkpoint_sha,
        checkpoint_codec_validated=True,
        state_watermark_binding_validated=True,
        checkpoint_input_coverage=(coverage,),
        input_targets=(target,),
        replay_spans=(span,),
        native_safe_mux=mux,
    )
    return request, build_factor_recovery_plan(request)


class FactorRecoveryTests(unittest.TestCase):
    def test_real_runtime_checkpoint_resumes_at_exact_exclusive_cursor(self) -> None:
        request, plan = factor_fixture()
        saved = request.checkpoint.watermark_set.entries[0].canonical_cursor
        self.assertEqual(plan.replay_spans[0].begin_exclusive_canonical_cursor, saved)
        self.assertEqual(plan.watermark_set, request.checkpoint.watermark_set)
        self.assertFalse(plan.executes_checkpoint_restore)
        self.assertFalse(plan.authorizes_alias_switch)

    def test_live_latest_and_skip_to_latest_are_rejected(self) -> None:
        request, _ = factor_fixture()
        live_spec = FactorSpec(
            factor_id="recovery_test",
            factor_version="1",
            state_schema_version=1,
            input_mode=InputMode.LIVE_LATEST,
            inputs=(
                InputSpec(1002, InputFamily.TICK, shard_id=1),
                InputSpec(0, InputFamily.LATEST_STATE, shard_id=1),
            ),
            clock_semantics=ClockSemantics.RECEIVE_MONOTONIC,
            nondeterministic_live_latest=True,
        )
        checkpoint = replace(
            request.checkpoint,
            factor_config_sha256=live_spec.sha256(),
        )
        identity = replace(
            request.expected_identity,
            factor_config_sha256=live_spec.sha256(),
            factor_spec=live_spec,
        )
        with self.assertRaises(RecoveryConflictError):
            build_factor_recovery_plan(
                replace(
                    request,
                    expected_identity=identity,
                    checkpoint=checkpoint,
                    checkpoint_sha256=hashlib.sha256(
                        encode_checkpoint(checkpoint)
                    ).digest(),
                )
            )
        with self.assertRaises(RecoveryConflictError):
            build_factor_recovery_plan(
                replace(request, skip_to_latest_requested=True)
            )

    def test_generation_and_normalizer_mismatch_fail_independent_expectation(self) -> None:
        request, _ = factor_fixture()
        original = request.input_targets[0].namespace
        for bad_namespace in (
            replace(original, origin_source_generation=99),
            replace(original, normalizer_config_sha256=h(99)),
        ):
            coverage = replace(
                request.checkpoint_input_coverage[0], namespace=bad_namespace
            )
            target = replace(request.input_targets[0], namespace=bad_namespace)
            span = replace(request.replay_spans[0], namespace=bad_namespace)
            ranges = (NativeMuxInputRange.from_span(span),)
            binding = native_safe_mux_binding_sha256(
                native_request_sha256=request.native_safe_mux.native_request_sha256,
                native_result_sha256=request.native_safe_mux.native_result_sha256,
                selection_receipt_sha256=(
                    request.native_safe_mux.selection_receipt_sha256
                ),
                input_ranges=ranges,
                decision=NativeSafeMuxDecision.READY,
                live_frontiers_revalidated=True,
                all_required_inputs_present=True,
                clock_compatible=True,
                fatal_observed=False,
            )
            mux = replace(
                request.native_safe_mux,
                native_binding_sha256=binding,
                input_ranges=ranges,
            )
            with self.subTest(normalizer=bad_namespace.normalizer_config_sha256), self.assertRaises(
                RecoveryConflictError
            ):
                build_factor_recovery_plan(
                    replace(
                        request,
                        checkpoint_input_coverage=(coverage,),
                        input_targets=(target,),
                        replay_spans=(span,),
                        native_safe_mux=mux,
                    )
                )

    def test_missing_watermark_route_and_mux_binding_tamper_fail(self) -> None:
        request, _ = factor_fixture()
        with self.assertRaises(RecoveryError):
            replace(request, checkpoint_input_coverage=())
        with self.assertRaises(RecoveryIntegrityError):
            replace(request.native_safe_mux, native_result_sha256=h(99))

    def test_cursor_plus_one_and_unknown_codec_fail(self) -> None:
        request, _ = factor_fixture()
        span = replace(
            request.replay_spans[0],
            begin_exclusive_canonical_cursor=(
                request.replay_spans[0].begin_exclusive_canonical_cursor + 1
            ),
            record_count=1,
        )
        ranges = (NativeMuxInputRange.from_span(span),)
        binding = native_safe_mux_binding_sha256(
            native_request_sha256=request.native_safe_mux.native_request_sha256,
            native_result_sha256=request.native_safe_mux.native_result_sha256,
            selection_receipt_sha256=request.native_safe_mux.selection_receipt_sha256,
            input_ranges=ranges,
            decision=NativeSafeMuxDecision.READY,
            live_frontiers_revalidated=True,
            all_required_inputs_present=True,
            clock_compatible=True,
            fatal_observed=False,
        )
        mux = replace(
            request.native_safe_mux,
            native_binding_sha256=binding,
            input_ranges=ranges,
        )
        with self.assertRaises(RecoveryConflictError):
            build_factor_recovery_plan(
                replace(request, replay_spans=(span,), native_safe_mux=mux)
            )
        checkpoint = replace(request.checkpoint, state_codec="opaque-state-v1")
        with self.assertRaises(RecoveryConflictError):
            build_factor_recovery_plan(
                replace(
                    request,
                    checkpoint=checkpoint,
                    checkpoint_sha256=hashlib.sha256(
                        encode_checkpoint(checkpoint)
                    ).digest(),
                )
            )


def rebuild_fixture():
    _, canonical_plan = canonical_fixture()
    schemas = (
        SchemaBinding("canonical", h(3), h(4)),
        SchemaBinding("factor", h(61), None),
    )
    # Equal file hashes are legal for distinct paths containing equal bytes.
    artifacts = (
        ArtifactHashBinding("candidate/a.parquet", "parquet", 100, h(62), h(63)),
        ArtifactHashBinding("candidate/b.parquet", "parquet", 100, h(62), h(64)),
    )
    request = IsolatedRebuildRequest(
        canonical_plan=canonical_plan,
        previous_generation=4,
        candidate_generation=5,
        candidate_identity_sha256=h(65),
        schemas=tuple(reversed(schemas)),
        registry_version=6,
        registry_sha256=h(6),
        artifacts=tuple(reversed(artifacts)),
        dependent_recovery_plan_sha256s=(h(67), h(66)),
        common_cut_identity_sha256=h(68),
        expected_candidate_content_sha256=h(69),
        candidate_isolated=True,
        artifacts_validated=True,
        common_cut_validated=True,
    )
    return request, build_isolated_rebuild_plan(request)


def repack_tampered_certificate(blob: bytes, mutate):
    length = struct.unpack_from("<Q", blob, 8)[0]
    root = json.loads(blob[16 : 16 + length].decode("ascii"))
    root = copy.deepcopy(root)
    mutate(root)
    identity = dict(root)
    identity.pop("certificate_sha256")
    canonical = lambda value: json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    expected = hashlib.sha256(
        b"l2flow.recovery.rebuild-cutover-certificate.v1\x00"
        + canonical(identity)
    ).digest()
    root["certificate_sha256"] = expected.hex()
    payload = canonical(root)
    prefix = b"L2RCVJ1\x00" + struct.pack("<Q", len(payload))
    return prefix + payload + hashlib.sha256(prefix + payload).digest(), expected


class CutoverCertificateTests(unittest.TestCase):
    def test_round_trip_is_caller_attested_plan_only(self) -> None:
        _, plan = rebuild_fixture()
        certificate = plan.certificate
        blob = encode_rebuild_cutover_certificate(certificate)
        self.assertEqual(
            validate_rebuild_cutover_certificate(
                blob,
                expected_certificate_sha256=certificate.certificate_sha256,
            ),
            certificate.certificate_sha256,
        )
        self.assertEqual(certificate.integrity_kind, "sha256_not_a_signature")
        self.assertTrue(certificate.caller_attested_evidence)
        self.assertFalse(certificate.executes_cutover)
        self.assertFalse(certificate.authorizes_alias_switch)
        self.assertFalse(plan.executes_rebuild)
        self.assertFalse(plan.authorizes_alias_switch)

    def test_recomputed_nested_tampering_still_fails_typed_validation(self) -> None:
        _, plan = rebuild_fixture()
        blob = encode_rebuild_cutover_certificate(plan.certificate)
        mutations = {
            "bad_integer_type": lambda root: root["raw_routes"][0].__setitem__(
                "source_stream_id", True
            ),
            "zero_hash": lambda root: root["artifacts"][0].__setitem__(
                "file_sha256", "00" * 32
            ),
            "unsafe_path": lambda root: root["artifacts"][0].__setitem__(
                "relative_path", "../escape"
            ),
            "route_order": lambda root: root["raw_routes"].reverse(),
            "generation": lambda root: root.__setitem__(
                "candidate_generation", root["previous_generation"]
            ),
            "alias_authority": lambda root: root.__setitem__(
                "authorizes_alias_switch", True
            ),
            "canonical_dtype_missing": lambda root: next(
                item for item in root["schemas"] if item["component"] == "canonical"
            ).__setitem__("dtype_sha256", None),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                tampered, digest = repack_tampered_certificate(blob, mutate)
                with self.assertRaises(RecoveryIntegrityError):
                    validate_rebuild_cutover_certificate(
                        tampered,
                        expected_certificate_sha256=digest,
                    )

    def test_envelope_tamper_fails(self) -> None:
        _, plan = rebuild_fixture()
        blob = bytearray(encode_rebuild_cutover_certificate(plan.certificate))
        blob[-1] ^= 1
        with self.assertRaises(RecoveryIntegrityError):
            validate_rebuild_cutover_certificate(
                bytes(blob),
                expected_certificate_sha256=plan.certificate.certificate_sha256,
            )


if __name__ == "__main__":
    unittest.main()
