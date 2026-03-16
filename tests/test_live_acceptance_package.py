from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from etl_identity_engine.cli import main
from etl_identity_engine.ingest.landed_batch_custody import capture_live_target_custody
from etl_identity_engine.ingest.live_acceptance_package import package_live_target_acceptance
from etl_identity_engine.ingest.live_target_packs import prepare_live_target_pack


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_package_live_target_acceptance_masks_rows_and_writes_drift_reports(tmp_path: Path) -> None:
    staged_root = tmp_path / "prepared_cad"
    capture_root = tmp_path / "captured_batches"
    acceptance_root = tmp_path / "acceptance_packages"
    prepare_live_target_pack("cad_county_dispatch_v1", staged_root)
    custody_summary = capture_live_target_custody(
        "cad_county_dispatch_v1",
        staged_root,
        capture_root,
        operator_id="dispatch.operator",
        transport_channel="sftp",
    )

    summary = package_live_target_acceptance(
        "cad_county_dispatch_v1",
        Path(custody_summary["immutable_root"]),
        acceptance_root,
    )

    assert summary["status"] == "packaged"
    assert summary["source_custody_manifest_present"] is True
    assert summary["masked_validation"]["status"] == "passed"
    package_dir = Path(summary["acceptance_root"])
    assert (package_dir / "drift_report.json").exists()
    assert (package_dir / "drift_report.md").exists()
    assert (package_dir / "custody_manifest.json").exists() is False
    assert (package_dir / "live_target_pack_summary.json").exists() is False

    manifest_text = (package_dir / "batch_manifest.yml").read_text(encoding="utf-8")
    assert "batch_id: acceptance-cad_county_dispatch_v1" in manifest_text

    masked_rows = _read_csv_rows(package_dir / "cad_county_dispatch_bundle" / "vendor_person_records.csv")
    assert masked_rows[0]["given_name"] != "Taylor"
    assert masked_rows[0]["variant_flag"] == "false"

    drift_report = json.loads((package_dir / "drift_report.json").read_text(encoding="utf-8"))
    assert drift_report["target_id"] == "cad_county_dispatch_v1"
    assert drift_report["bundles"][0]["files"]["person_records"]["diff_report"]["overlay_mode"] == "vendor_profile"


def test_package_live_target_acceptance_cli_fails_for_invalid_source_root(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    staged_root = tmp_path / "prepared_rms"
    prepare_live_target_pack("rms_records_bureau_v1", staged_root)
    (staged_root / "rms_records_bureau_bundle" / "vendor_incident_person_links.csv").unlink()

    with pytest.raises(SystemExit, match="1"):
        main(
            [
                "package-live-target-acceptance",
                "--target-id",
                "rms_records_bureau_v1",
                "--source-root",
                str(staged_root),
                "--output-dir",
                str(tmp_path / "acceptance_packages"),
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "failed"
    assert payload["validation_error"] == "source live target pack failed onboarding validation"
