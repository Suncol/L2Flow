from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path


_BENCHMARKS = Path(__file__).resolve().parent


def _load(name: str, path: Path):
    specification = importlib.util.spec_from_file_location(name, path)
    if specification is None or specification.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


matrix = _load(
    "run_parallel_decoder_matrix_tested",
    _BENCHMARKS / "run_parallel_decoder_matrix.py",
)
latency = _load(
    "compare_callback_polars_ab_tested",
    _BENCHMARKS / "compare_callback_polars_ab.py",
)


def _healthy_parallel_row() -> dict[str, object]:
    row: dict[str, object] = {
        "exit_code": 0,
        "env_line_count": 1,
        "result_line_count": 1,
        "requested_rate": 100,
        "requested_duration_ms": 1_000,
        "requested_instruments_per_market": 256,
        "requested_workload": "five_tuple_uniform",
        "requested_sink": "fast",
        "requested_store_workers": 4,
        "requested_parallel_workers": 2,
        "requested_idle_inline": 1,
        "requested_decoder_queue": 65_536,
        "requested_store_queue": 32_768,
        "requested_segment_kib": 64,
        "target_rps": "100",
        "duration_ms": "1000",
        "instruments_per_market": "256",
        "workload": "five_tuple_uniform",
        "sink": "fast",
        "store_worker_count": "4",
        "parallel_decoder_workers": "2",
        "env_target_rps": "100",
        "env_duration_ms": "1000",
        "env_planned_callbacks": "100",
        "env_instruments_per_market": "256",
        "env_workload": "five_tuple_uniform",
        "env_sink": "fast",
        "env_store_worker_count": "4",
        "env_parallel_decoder_workers": "2",
        "env_parallel_idle_inline": "1",
        "env_parallel_farm_activation_configured": "8192",
        "env_parallel_farm_activation_effective": "8192",
        "env_decoder_queue_capacity_per_source": "65536",
        "env_store_queue_capacity_per_source_worker": "32768",
        "env_store_segment_kib": "64",
        "env_callback_contract": "serialized",
        "env_pacing": "absolute_deadline_no_batch_wait",
        "env_clock": "CLOCK_MONOTONIC",
        "planned_callbacks": "100",
        "invoked_callbacks": "100",
        "accepted": "100",
        "decoded": "100",
        "applied": "100",
        "store_appended": "100",
        "backlog_before_drain": "0",
        "backlog_q50": "0",
        "backlog_q100": "0",
        "covered_store_worker_count": "4",
        "parallel_enabled": "1",
        "parallel_idle_inline": "1",
        "parallel_dispatched": "100",
        "parallel_completed": "100",
        "parallel_committed": "100",
        "parallel_farm": "40",
        "parallel_inline": "60",
        "parallel_parsed": "40",
        "parallel_worker_parsed_csv": "20,20",
        "parallel_active_worker_count": "2",
        "parallel_slots_per_source_worker": "16",
        "parallel_issue_capacity_per_worker": "64",
        "parallel_completion_capacity_per_source": "32",
        "parallel_issue_high_water_max": "63",
        "parallel_completion_high_water_max": "31",
        "parallel_lease_waits": "0",
    }
    for key in (
        "target_met",
        "process_survived",
        "complete_prefix",
        "stopped_clean",
        "steady_state_met",
        "certified_healthy",
    ):
        row[key] = "1"
    for key in (
        "fatal_during_offer",
        "fatal_final",
        "message_patch_failed",
        "rejected",
        "post_cut",
        "store_failed_appends",
        "decoder_full_count",
        "service_failed",
        "parallel_parse_failures",
        "parallel_completion_publish_failures",
        "parallel_discarded",
        "parallel_farm_outstanding",
        "certified_dropped",
        "certified_frozen_channels",
        "certified_global_frozen",
    ):
        row[key] = "0"
    return row


def _latency_log(workers: int = 8) -> str:
    digest = "a" * 64
    return "\n".join(
        (
            "HISTORY_ENV capacity=12000 bound_instruments=12000 "
            "snapshot_fill=11997 worker_count=4 tick_ring_capacity=262144 "
            "requested_page_records=4096 price_repeats=20 "
            "all_column_repeats=10 raw_polars_records=4096 "
            f"raw_polars_repeats=20 parallel_decoder_workers={workers} "
            "parallel_decoder_farm_activation_queue_depth=8192 "
            "parallel_decoder_farm_activation_effective_depth=6144 "
            "clock=CLOCK_MONOTONIC affinity=0,1;count=2",
            "CALLBACK_POLARS_TOPOLOGY "
            f"enabled={1 if workers else 0} configured_workers={workers} "
            f"reported_workers={workers} "
            f"inline_messages={12345 if workers else 0} "
            "farm_messages=0 active_parse_workers=0 "
            "activation_configured_depth=8192 "
            "activation_effective_depth=6144",
            "PYTHON_READY protocol=wire_v2_history_latency_2 capacity=12000 "
            "bound_count=12000 monotonic_implementation=test "
            "monotonic_resolution_ns=1 worker_ring_slots=4 "
            "worker_result_batch_records=4096 raw_polars_columns=55 "
            "python_version=3.12.3 python_implementation=CPython "
            "python_affinity=0,1;count=2 "
            f"python_executable_sha256={digest} "
            f"native_library_sha256={digest} probe_sha256={digest} "
            "polars_version=1.0.0",
            "PYTHON_RAW_POLARS_SAMPLE sample=0 generation=2 records=4096 "
            "columns=55 batches=1 first_ingress=1 last_ingress=4096 "
            "first_callback_entry_ns=100 last_callback_entry_ns=200 "
            "history_published_monotonic_ns=300 polars_ready_ns=500 "
            "first_callback_to_polars_ns=400 "
            "last_callback_to_polars_ns=300",
            "PYTHON_DERIVED_POLARS_SAMPLE generation=3 records=6 "
            "order_sequence_records=5 columns=20 batches=1 order_id=11001 "
            "order_revision_count=3 first_derived_sequence=10 "
            "last_derived_sequence=14 first_callback_entry_ns=600 "
            "last_callback_entry_ns=700 history_published_monotonic_ns=800 "
            "polars_ready_ns=1000 first_callback_to_polars_ns=400 "
            "last_callback_to_polars_ns=300 final_revision=3 "
            "final_remaining_quantity=0",
            "POLARS_BOUNDARY workload=raw_batch_4096_all_columns "
            "generation=2 records=4096 columns=55 "
            "first_caller_before_callback_ns=90 "
            "last_caller_before_callback_ns=190 "
            "first_callback_entry_ns=100 last_callback_entry_ns=200 "
            "polars_ready_ns=500 strict_first_callback_to_polars_ns=410 "
            "strict_last_callback_to_polars_ns=310",
            "POLARS_BOUNDARY workload=derived_complete_order_lifecycle "
            "generation=3 raw_records=4 derived_events=6 "
            "order_sequence_events=5 order_id=11001 final_revision=3 "
            "final_remaining_quantity=0 first_caller_before_callback_ns=590 "
            "last_caller_before_callback_ns=690 "
            "first_callback_entry_ns=600 last_callback_entry_ns=700 "
            "polars_ready_ns=1000 strict_first_callback_to_polars_ns=410 "
            "strict_last_callback_to_polars_ns=310",
            "PYTHON_BYE commands=1",
        )
    ) + "\n"


class MatrixHarnessTests(unittest.TestCase):
    def test_high_water_metadata_does_not_claim_exact_issue_depth(
        self,
    ) -> None:
        self.assertIn(
            "conservative_high_water_upper_bounds",
            matrix.PARALLEL_ISSUE_HIGH_WATER_SEMANTICS,
        )
        self.assertNotIn(
            "exact_high_waters",
            matrix.PARALLEL_ISSUE_HIGH_WATER_SEMANTICS,
        )
        self.assertIn(
            "true_high_water_lower_bound",
            matrix.PARALLEL_COMPLETION_HIGH_WATER_SEMANTICS,
        )

    def test_duplicate_result_field_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicate"):
            matrix._fields("THROUGHPUT_RESULT target_met=0 target_met=1")

    def test_valid_parallel_trial_passes(self) -> None:
        self.assertEqual(matrix._classify(_healthy_parallel_row(), 1_024), [])

    def test_valid_zero_worker_trial_passes(self) -> None:
        row = _healthy_parallel_row()
        row.update(
            requested_parallel_workers=0,
            parallel_decoder_workers="0",
            env_parallel_decoder_workers="0",
            parallel_enabled="0",
            parallel_idle_inline="0",
            parallel_dispatched="0",
            parallel_completed="0",
            parallel_committed="0",
            parallel_farm="0",
            parallel_inline="0",
            parallel_parsed="0",
            parallel_worker_parsed_csv="",
            parallel_active_worker_count="0",
            parallel_completion_capacity_per_source="0",
            parallel_issue_high_water_max="0",
            parallel_completion_high_water_max="0",
        )
        self.assertEqual(matrix._classify(row, 1_024), [])

    def test_request_result_worker_mismatch_fails(self) -> None:
        row = _healthy_parallel_row()
        row["parallel_decoder_workers"] = "0"
        self.assertIn(
            "parallel_decoder_workers_request_mismatch",
            matrix._classify(row, 1_024),
        )

    def test_adaptive_all_inline_is_functionally_healthy(self) -> None:
        row = _healthy_parallel_row()
        row.update(
            parallel_farm="0",
            parallel_inline="100",
            parallel_parsed="0",
            parallel_worker_parsed_csv="0,0",
            parallel_active_worker_count="0",
        )
        self.assertEqual(matrix._classify(row, 1_024), [])
        failures = matrix._classify(
            row, 1_024, require_farm=True
        )
        self.assertIn("parallel_farm_not_exercised", failures)

    def test_missing_worker_telemetry_fails_closed(self) -> None:
        row = _healthy_parallel_row()
        del row["parallel_worker_parsed_csv"]
        self.assertIn(
            "parallel_worker_telemetry_missing",
            matrix._classify(row, 1_024),
        )

    def test_subcapacity_samples_do_not_claim_headroom(self) -> None:
        row = _healthy_parallel_row()
        self.assertEqual(
            matrix._capacity_pressure(row), "HEADROOM_UNPROVEN"
        )
        self.assertIn(
            "parallel_capacity_headroom_unproven",
            matrix._classify(
                row, 1_024, require_capacity_headroom=True
            ),
        )

    def test_reaching_reported_capacity_is_only_a_pressure_signal(self) -> None:
        row = _healthy_parallel_row()
        row["parallel_completion_high_water_max"] = "32"
        self.assertEqual(matrix._classify(row, 1_024), [])
        self.assertEqual(
            matrix._capacity_pressure(row),
            "PRESSURE_OBSERVED_NO_FAILURE",
        )
        self.assertIn(
            "parallel_capacity_headroom_unproven",
            matrix._classify(
                row, 1_024, require_capacity_headroom=True
            ),
        )

    def test_summed_issue_high_water_is_only_a_pressure_signal(self) -> None:
        row = _healthy_parallel_row()
        row["parallel_issue_high_water_max"] = "64"
        self.assertEqual(matrix._classify(row, 1_024), [])
        self.assertEqual(
            matrix._capacity_pressure(row),
            "PRESSURE_OBSERVED_NO_FAILURE",
        )

    def test_invalid_capacity_telemetry_still_fails(self) -> None:
        row = _healthy_parallel_row()
        row["parallel_completion_high_water_max"] = "33"
        self.assertEqual(
            matrix._capacity_pressure(row), "TELEMETRY_INVALID"
        )
        self.assertIn(
            "parallel_completion_high_water_invalid",
            matrix._classify(row, 1_024),
        )

    def test_negative_lease_wait_telemetry_still_fails(self) -> None:
        row = _healthy_parallel_row()
        row["parallel_lease_waits"] = "-1"
        self.assertEqual(
            matrix._capacity_pressure(row), "TELEMETRY_INVALID"
        )
        self.assertIn(
            "parallel_lease_waits_invalid",
            matrix._classify(row, 1_024),
        )

    def test_actual_capacity_operation_failure_takes_precedence(self) -> None:
        row = _healthy_parallel_row()
        row["parallel_completion_publish_failures"] = "1"
        self.assertEqual(
            matrix._capacity_pressure(row), "ACTUAL_OPERATION_FAILURE"
        )
        self.assertIn(
            "parallel_completion_publish_failures_not_zero",
            matrix._classify(row, 1_024),
        )

    def test_requested_affinity_is_checked(self) -> None:
        row = _healthy_parallel_row()
        row["requested_affinity"] = "0,2,4;count=3"
        row["env_affinity"] = "0,2,5;count=3"
        self.assertIn(
            "affinity_request_mismatch",
            matrix._classify(row, 1_024),
        )

    def test_cpu_list_parser_matches_taskset_form(self) -> None:
        self.assertEqual(
            matrix._requested_cpu_set("0-4:2,7"),
            frozenset((0, 2, 4, 7)),
        )


class LatencyHarnessTests(unittest.TestCase):
    def _parse(self, text: str):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "latency.log"
            path.write_text(text, encoding="utf-8")
            return latency._parse_latency_log(
                path,
                expected_workers=8,
                expected_affinity=frozenset((0, 1)),
            )

    def test_exact_latency_contract_parses(self) -> None:
        metrics, metadata = self._parse(_latency_log())
        self.assertEqual(metrics["batch_first_callback_to_polars_ns"], 400)
        self.assertEqual(metadata["derived_records"], 6)
        self.assertEqual(metadata["order_sequence_records"], 5)

    def test_derived_total_is_exact(self) -> None:
        malformed = _latency_log().replace(
            "PYTHON_DERIVED_POLARS_SAMPLE generation=3 records=6",
            "PYTHON_DERIVED_POLARS_SAMPLE generation=3 records=7",
        )
        with self.assertRaisesRegex(ValueError, "derived records"):
            self._parse(malformed)

    def test_raw_publication_must_follow_last_callback(self) -> None:
        malformed = _latency_log().replace(
            "history_published_monotonic_ns=300 polars_ready_ns=500",
            "history_published_monotonic_ns=199 polars_ready_ns=500",
            1,
        )
        with self.assertRaisesRegex(ValueError, "raw clock ordering"):
            self._parse(malformed)

    def test_worker_topology_is_checked(self) -> None:
        with self.assertRaisesRegex(ValueError, "parallel_decoder_workers"):
            self._parse(_latency_log(workers=4))

    def test_legacy_command_can_supply_zero_worker_evidence(self) -> None:
        text = _latency_log(workers=0).replace(
            "parallel_decoder_workers=0 ", "", 1
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy.log"
            path.write_text(text, encoding="utf-8")
            _, metadata = latency._parse_latency_log(
                path,
                expected_workers=0,
                expected_affinity=frozenset((0, 1)),
                allow_legacy_worker_field_absent=True,
            )
        self.assertEqual(
            metadata["topology_evidence"],
            "legacy_command_without_worker_field",
        )

    def test_duplicate_latency_field_is_rejected(self) -> None:
        malformed = _latency_log().replace(
            "raw_polars_records=4096",
            "raw_polars_records=4096 raw_polars_records=4096",
            1,
        )
        with self.assertRaisesRegex(ValueError, "duplicate"):
            self._parse(malformed)

    def test_cpu_list_parser_matches_taskset_form(self) -> None:
        self.assertEqual(
            latency._requested_cpu_set("0-4:2,7"),
            frozenset((0, 2, 4, 7)),
        )


if __name__ == "__main__":
    unittest.main()
