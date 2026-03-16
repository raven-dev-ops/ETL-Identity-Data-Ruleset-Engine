from __future__ import annotations

import json
from pathlib import Path

import pytest

from etl_identity_engine.cli import main
from etl_identity_engine.ingest.landed_batch_custody import capture_live_target_custody
from etl_identity_engine.ingest.live_target_packs import prepare_live_target_pack


def test_capture_live_target_custody_copies_validated_pack_and_writes_manifest(tmp_path: Path) -> None:
    staged_root = tmp_path / "prepared_cad"
    intake_root = tmp_path / "captured_batches"
    prepare_live_target_pack("cad_county_dispatch_v1", staged_root)

    summary = capture_live_target_custody(
        "cad_county_dispatch_v1",
        staged_root,
        intake_root,
        operator_id="integration.operator",
        transport_channel="sftp",
        tenant_id="tenant-a",
        arrived_at_utc="2026-03-15T12:00:00Z",
    )

    assert summary["status"] == "captured"
    assert summary["arrived_at_utc"] == "2026-03-15T12:00:00Z"
    assert summary["replay_linkage"]["target_id"] == "cad_county_dispatch_v1"
    assert summary["replay_linkage"]["tenant_id"] == "tenant-a"
    assert summary["replay_linkage"]["source_bundle_ids"] == ["cad_county_dispatch_primary"]
    assert Path(summary["immutable_root"]).exists()
    assert Path(summary["custody_manifest_path"]).exists()

    tracked_files = {entry["relative_path"]: entry for entry in summary["tracked_files"]}
    assert "batch_manifest.yml" in tracked_files
    assert "cad_county_dispatch_bundle/vendor_person_records.csv" in tracked_files
    assert tracked_files["batch_manifest.yml"]["role"] == "batch_manifest"
    assert len(tracked_files["batch_manifest.yml"]["sha256"]) == 64
    assert Path(tracked_files["batch_manifest.yml"]["immutable_path"]).exists()


def test_capture_live_target_custody_cli_reports_failed_validation_for_broken_pack(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    staged_root = tmp_path / "prepared_rms"
    intake_root = tmp_path / "captured_batches"
    prepare_live_target_pack("rms_records_bureau_v1", staged_root)
    (staged_root / "rms_records_bureau_bundle" / "vendor_person_records.csv").unlink()

    with pytest.raises(SystemExit, match="1"):
        main(
            [
                "capture-live-target-custody",
                "--target-id",
                "rms_records_bureau_v1",
                "--staged-root",
                str(staged_root),
                "--output-dir",
                str(intake_root),
                "--operator-id",
                "records.operator",
                "--transport-channel",
                "smb_share",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "failed"
    assert payload["validation_error"] == "staged live target pack failed onboarding validation"
