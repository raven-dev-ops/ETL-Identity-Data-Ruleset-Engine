"""Smoke-test backup, restore, and replay of persisted pipeline state."""

from __future__ import annotations

import argparse
import csv
import gc
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from typing import Any

from etl_identity_engine.generate.synth_generator import PERSON_HEADERS
from etl_identity_engine.storage.sqlite_store import SQLitePipelineStore


REPO_ROOT = Path(__file__).resolve().parents[1]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the persisted-state backup, restore, and replay smoke path."
    )
    parser.add_argument(
        "--temp-root",
        help="Optional temp-root override for the smoke workspace.",
    )
    return parser.parse_args(argv)


def _run_cli(argv: list[str], *, expect_json: bool = False) -> Any:
    completed = subprocess.run(
        [sys.executable, "-m", "etl_identity_engine.cli", *argv],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "unknown error"
        raise SystemExit(
            f"command failed ({completed.returncode}): python -m etl_identity_engine.cli {' '.join(argv)}\n{detail}"
        )
    if not expect_json:
        return completed.stdout
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise SystemExit(
            "expected JSON output from CLI command but received:\n"
            f"{completed.stdout.strip() or '<empty>'}"
        ) from exc


def _write_csv_rows(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _write_parquet_rows(path: Path, rows: list[dict[str, str]]) -> None:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "The recovery smoke path requires parquet support. "
            "Install the project dev environment before running this script."
        ) from exc

    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def _person_row(
    *,
    source_record_id: str,
    person_entity_id: str,
    source_system: str,
    first_name: str,
    last_name: str,
    dob: str,
    address: str,
    phone: str,
) -> dict[str, str]:
    return {
        "source_record_id": source_record_id,
        "person_entity_id": person_entity_id,
        "source_system": source_system,
        "first_name": first_name,
        "last_name": last_name,
        "dob": dob,
        "address": address,
        "city": "Columbus",
        "state": "OH",
        "postal_code": "43004",
        "phone": phone,
        "updated_at": "2025-01-01T00:00:00Z",
        "is_conflict_variant": "false",
        "conflict_types": "",
    }


def _write_manifest(path: Path, *, batch_id: str) -> Path:
    required_columns = "\n".join(f"        - {column}" for column in PERSON_HEADERS)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"""
manifest_version: "1.0"
entity_type: person
batch_id: {batch_id}
landing_zone:
  kind: local_filesystem
  base_path: ./landing
sources:
  - source_id: source_a
    path: agency_a.csv
    format: csv
    schema_version: person-v1
    required_columns:
{required_columns}
  - source_id: source_b
    path: agency_b.parquet
    format: parquet
    schema_version: person-v1
    required_columns:
{required_columns}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return path


def _build_manifest_batch(batch_root: Path, *, batch_id: str) -> tuple[Path, Path, Path]:
    manifest_path = _write_manifest(batch_root / "manifest.yml", batch_id=batch_id)
    landing_dir = batch_root / "landing"
    state_db = batch_root / "state" / "pipeline_state.sqlite"

    _write_csv_rows(
        landing_dir / "agency_a.csv",
        [
            _person_row(
                source_record_id="A-1",
                person_entity_id="P-1",
                source_system="source_a",
                first_name="John",
                last_name="Smith",
                dob="1985-03-12",
                address="123 Main St",
                phone="5551111111",
            )
        ],
    )
    _write_parquet_rows(
        landing_dir / "agency_b.parquet",
        [
            _person_row(
                source_record_id="B-1",
                person_entity_id="P-2",
                source_system="source_b",
                first_name="Jon",
                last_name="Smith",
                dob="1985-03-12",
                address="123 Main St",
                phone="5551111111",
            )
        ],
    )
    return manifest_path, landing_dir, state_db


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    with tempfile.TemporaryDirectory(
        prefix="etl-identity-engine-recovery-",
        dir=args.temp_root,
        ignore_cleanup_errors=True,
    ) as temp_dir:
        workspace = Path(temp_dir)
        original_root = workspace / "original"
        backup_root = workspace / "backup"
        restored_root = workspace / "restored"

        manifest_path, landing_dir, state_db = _build_manifest_batch(
            original_root / "batch",
            batch_id="recovery-smoke-001",
        )
        original_base_dir = original_root / "batch" / "run"

        _run_cli(
            [
                "run-all",
                "--base-dir",
                str(original_base_dir),
                "--manifest",
                str(manifest_path),
                "--state-db",
                str(state_db),
                "--refresh-mode",
                "full",
            ]
        )

        original_store = SQLitePipelineStore(state_db)
        source_run_id = original_store.latest_completed_run_id()
        if source_run_id is None:
            raise SystemExit("expected an initial completed persisted run")

        review_cases = original_store.list_review_cases(run_id=source_run_id)
        if len(review_cases) != 1:
            raise SystemExit(
                f"expected exactly one review case in recovery smoke batch, found {len(review_cases)}"
            )

        decision_payload = _run_cli(
            [
                "apply-review-decision",
                "--state-db",
                str(state_db),
                "--run-id",
                source_run_id,
                "--review-id",
                review_cases[0].review_id,
                "--decision",
                "approved",
                "--assigned-to",
                "recovery.operator",
                "--notes",
                "Approved during recovery smoke path",
            ],
            expect_json=True,
        )
        if decision_payload["action"] != "updated":
            raise SystemExit("expected recovery smoke review decision to update the persisted review case")

        backup_root.mkdir(parents=True, exist_ok=True)
        shutil.copy2(state_db, backup_root / "pipeline_state.sqlite")
        shutil.copy2(manifest_path, backup_root / "manifest.yml")
        shutil.copytree(landing_dir, backup_root / "landing")

        if original_base_dir.exists():
            shutil.rmtree(original_base_dir)
        if manifest_path.exists():
            manifest_path.unlink()
        if landing_dir.exists():
            shutil.rmtree(landing_dir)

        restored_manifest_path = manifest_path
        restored_landing_dir = landing_dir
        restored_manifest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(backup_root / "manifest.yml", restored_manifest_path)
        shutil.copytree(backup_root / "landing", restored_landing_dir)

        restored_state_db = restored_root / "state" / "pipeline_state.sqlite"
        restored_state_db.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(backup_root / "pipeline_state.sqlite", restored_state_db)

        restored_review_cases = _run_cli(
            [
                "review-case-list",
                "--state-db",
                str(restored_state_db),
                "--run-id",
                source_run_id,
            ],
            expect_json=True,
        )
        if len(restored_review_cases) != 1 or restored_review_cases[0]["queue_status"] != "approved":
            raise SystemExit("expected restored review state to preserve the approved manual-review decision")

        rebuilt_report = restored_root / "rebuilt-report" / "run_report.md"
        _run_cli(
            [
                "report",
                "--state-db",
                str(restored_state_db),
                "--run-id",
                source_run_id,
                "--output",
                str(rebuilt_report),
            ]
        )
        rebuilt_summary_path = rebuilt_report.with_name("run_summary.json")
        if not rebuilt_report.exists() or not rebuilt_summary_path.exists():
            raise SystemExit("expected restored persisted state to rebuild report outputs")

        _write_manifest(restored_manifest_path, batch_id="recovery-smoke-002")
        replay_payload = _run_cli(
            [
                "replay-run",
                "--state-db",
                str(restored_state_db),
                "--run-id",
                source_run_id,
                "--base-dir",
                str(restored_root / "replayed-run"),
                "--refresh-mode",
                "incremental",
            ],
            expect_json=True,
        )
        if replay_payload["action"] != "replayed":
            raise SystemExit(
                f"expected replay-run to create a new recovered run, received action={replay_payload['action']!r}"
            )

        replay_run_id = str(replay_payload["result_run_id"])
        if replay_run_id == source_run_id:
            raise SystemExit("expected replay-run to create a distinct recovered run ID")

        restored_store = SQLitePipelineStore(restored_state_db)
        replay_bundle = restored_store.load_run_bundle(replay_run_id)
        if replay_bundle.candidate_pairs[0]["decision"] != "auto_merge":
            raise SystemExit("expected approved review decision to force an auto-merge during recovered replay")
        if "review_case_approved_override" not in replay_bundle.candidate_pairs[0]["reason_trace"]:
            raise SystemExit("expected replayed candidate reason trace to record the approved-review override")
        if len(replay_bundle.golden_rows) != 1:
            raise SystemExit("expected replayed recovery run to produce one merged golden record")
        if replay_bundle.review_rows[0]["queue_status"] != "approved":
            raise SystemExit("expected replayed recovery run to carry forward the approved review state")

        del original_store
        del restored_store
        gc.collect()

        print(
            json.dumps(
                {
                    "status": "ok",
                    "validated_steps": [
                        "backup_sqlite_manifest_and_landing_snapshot",
                        "restore_review_state",
                        "rebuild_report_outputs_from_restored_state",
                        "replay_recovered_run_with_review_override",
                    ],
                    "source_run_id": source_run_id,
                    "replay_run_id": replay_run_id,
                },
                indent=2,
                sort_keys=True,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
