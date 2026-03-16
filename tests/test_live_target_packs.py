from __future__ import annotations

import json
from pathlib import Path

import pytest

from etl_identity_engine.cli import main
from etl_identity_engine.ingest.live_target_packs import (
    LiveTargetPackError,
    check_live_target_pack,
    list_live_target_packs,
    prepare_live_target_pack,
)


@pytest.mark.parametrize(
    ("target_id", "expected_bundle_dir", "source_class"),
    [
        ("cad_county_dispatch_v1", "cad_county_dispatch_bundle", "cad"),
        ("rms_records_bureau_v1", "rms_records_bureau_bundle", "rms"),
    ],
)
def test_prepare_live_target_pack_writes_scaffold_and_self_validates(
    tmp_path: Path,
    target_id: str,
    expected_bundle_dir: str,
    source_class: str,
) -> None:
    output_dir = tmp_path / target_id

    summary = prepare_live_target_pack(
        target_id,
        output_dir,
        variable_overrides={
            "agency_name": "Franklin County Pilot",
            "agency_slug": f"{source_class}-pilot",
            "drop_zone_subpath": f"{source_class}/pilot/inbound",
            "operator_contact": "pilot.operator@example.gov",
        },
    )

    assert summary["target_id"] == target_id
    assert summary["resolved_variables"]["agency_slug"] == f"{source_class}-pilot"
    assert summary["validation"]["status"] == "passed"
    assert (output_dir / "README.md").exists()
    assert (output_dir / "batch_manifest.yml").exists()
    assert (output_dir / expected_bundle_dir / "contract_manifest.yml").exists()
    assert (output_dir / "live_target_pack_summary.json").exists()

    manifest_text = (output_dir / "batch_manifest.yml").read_text(encoding="utf-8")
    assert f"batch_id: live-{source_class}-pilot-{source_class}-001" in manifest_text

    contract_manifest_text = (
        output_dir / expected_bundle_dir / "contract_manifest.yml"
    ).read_text(encoding="utf-8")
    assert f"vendor_profile: {target_id}" in contract_manifest_text


def test_prepare_live_target_pack_rejects_unknown_variables(tmp_path: Path) -> None:
    with pytest.raises(
        LiveTargetPackError,
        match="unsupported variable override",
    ):
        prepare_live_target_pack(
            "cad_county_dispatch_v1",
            tmp_path / "cad_pack",
            variable_overrides={"unknown": "value"},
        )


def test_list_live_target_packs_cli_reports_shipped_targets(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["list-live-target-packs"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert [item["target_id"] for item in payload] == [
        pack.target_id for pack in list_live_target_packs()
    ]


def test_prepare_live_target_pack_cli_writes_summary_json(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output_dir = tmp_path / "prepared_cad_pack"

    assert (
        main(
            [
                "prepare-live-target-pack",
                "--target-id",
                "cad_county_dispatch_v1",
                "--output-dir",
                str(output_dir),
                "--set",
                "agency_name=Franklin County Dispatch Pilot",
                "--set",
                "agency_slug=franklin-cad-pilot",
            ]
        )
        == 0
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["target_id"] == "cad_county_dispatch_v1"
    assert payload["validation"]["status"] == "passed"
    assert payload["resolved_variables"]["agency_slug"] == "franklin-cad-pilot"


def test_check_live_target_pack_reports_failed_validation_when_bundle_is_broken(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output_dir = tmp_path / "prepared_rms_pack"
    prepare_live_target_pack("rms_records_bureau_v1", output_dir)
    (output_dir / "rms_records_bureau_bundle" / "vendor_incident_records.csv").unlink()

    summary = check_live_target_pack("rms_records_bureau_v1", output_dir)
    assert summary["status"] == "failed"

    with pytest.raises(SystemExit, match="1"):
        main(
            [
                "check-live-target-pack",
                "--target-id",
                "rms_records_bureau_v1",
                "--root-dir",
                str(output_dir),
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "failed"
