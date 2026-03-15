from __future__ import annotations

import csv
import json
from pathlib import Path
import shutil

import pytest

from etl_identity_engine.cli import main
from etl_identity_engine.ingest.public_safety_conformance import (
    check_public_safety_onboarding,
)


FIXTURE_ROOT = (
    Path(__file__).resolve().parents[1] / "fixtures" / "public_safety_onboarding"
)


def test_check_public_safety_onboarding_accepts_shipped_example_manifest() -> None:
    summary = check_public_safety_onboarding(
        manifest_path=FIXTURE_ROOT / "example_manifest.yml",
    )

    assert summary["status"] == "passed"
    manifest = summary["manifest"]
    assert isinstance(manifest, dict)
    assert manifest["batch_id"] == "fixture-public-safety-onboarding-001"
    assert manifest["source_bundle_count"] == 2
    source_bundles = manifest["source_bundles"]
    assert isinstance(source_bundles, list)
    assert source_bundles[0]["files"][0]["diff_report"]["overlay_mode"] == "canonical_passthrough"


def test_cli_check_public_safety_onboarding_accepts_shipped_fixture_tree(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert (
        main(
            [
                "check-public-safety-onboarding",
                "--manifest",
                str(FIXTURE_ROOT / "example_manifest.yml"),
                "--bundle-dir",
                str(FIXTURE_ROOT / "cad_bundle"),
                "--bundle-dir",
                str(FIXTURE_ROOT / "rms_bundle"),
            ]
        )
        == 0
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "passed"
    assert payload["bundle_count"] == 2
    assert [bundle["contract_name"] for bundle in payload["bundles"]] == [
        "cad_call_for_service",
        "rms_report_person",
    ]
    assert payload["bundles"][0]["files"]["person_records"]["diff_report"]["mapped_canonical_fields"]


def test_check_public_safety_onboarding_accepts_vendor_overlay_fixture_tree() -> None:
    summary = check_public_safety_onboarding(
        manifest_path=FIXTURE_ROOT / "example_vendor_overlay_manifest.yml",
        bundle_dirs=(FIXTURE_ROOT / "cad_vendor_bundle",),
    )

    assert summary["status"] == "passed"
    bundles = summary["bundles"]
    assert isinstance(bundles, list)
    assert bundles[0]["mapping_overlay_path"]
    bundle_files = bundles[0]["files"]
    assert isinstance(bundle_files, dict)
    assert bundle_files["person_records"]["diff_report"]["overlay_mode"] == "mapping_overlay"
    assert bundle_files["person_records"]["diff_report"]["missing_required_canonical_fields"] == []
    manifest = summary["manifest"]
    assert isinstance(manifest, dict)
    source_bundles = manifest["source_bundles"]
    assert isinstance(source_bundles, list)
    assert source_bundles[0]["mapping_overlay_reference"] is not None
    assert source_bundles[1]["mapping_overlay_reference"] is not None
    assert source_bundles[0]["files"][0]["diff_report"]["overlay_mode"] == "mapping_overlay"


def test_cli_check_public_safety_onboarding_accepts_vendor_overlay_fixture_tree(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert (
        main(
            [
                "check-public-safety-onboarding",
                "--manifest",
                str(FIXTURE_ROOT / "example_vendor_overlay_manifest.yml"),
                "--bundle-dir",
                str(FIXTURE_ROOT / "cad_vendor_bundle"),
                "--bundle-dir",
                str(FIXTURE_ROOT / "rms_vendor_bundle"),
            ]
        )
        == 0
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "passed"
    assert payload["bundles"][1]["files"]["person_records"]["diff_report"]["overlay_mode"] == "mapping_overlay"


def test_check_public_safety_onboarding_reports_unresolved_vendor_columns_on_failure(
    tmp_path: Path,
) -> None:
    broken_bundle_dir = tmp_path / "cad_vendor_bundle"
    shutil.copytree(FIXTURE_ROOT / "cad_vendor_bundle", broken_bundle_dir)
    person_path = broken_bundle_dir / "vendor_person_records.csv"
    with person_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    rewritten_rows = []
    for row in rows:
        row.pop("given_name", None)
        rewritten_rows.append(row)
    with person_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rewritten_rows[0]))
        writer.writeheader()
        writer.writerows(rewritten_rows)

    summary = check_public_safety_onboarding(bundle_dirs=(broken_bundle_dir,))

    assert summary["status"] == "failed"
    bundles = summary["bundles"]
    assert isinstance(bundles, list)
    assert bundles[0]["status"] == "failed"
    person_summary = bundles[0]["files"]["person_records"]
    assert person_summary["status"] == "failed"
    diff_report = person_summary["diff_report"]
    assert diff_report["missing_required_canonical_fields"] == ["first_name"]
    assert diff_report["missing_source_columns"] == ["given_name"]
    assert "missing canonical mappings" in person_summary["validation_error"]


def test_cli_check_public_safety_onboarding_returns_json_then_nonzero_on_failure(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    broken_bundle_dir = tmp_path / "cad_vendor_bundle"
    shutil.copytree(FIXTURE_ROOT / "cad_vendor_bundle", broken_bundle_dir)
    person_path = broken_bundle_dir / "vendor_person_records.csv"
    with person_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    rewritten_rows = []
    for row in rows:
        row.pop("given_name", None)
        rewritten_rows.append(row)
    with person_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rewritten_rows[0]))
        writer.writeheader()
        writer.writerows(rewritten_rows)

    with pytest.raises(SystemExit, match="1"):
        main(
            [
                "check-public-safety-onboarding",
                "--bundle-dir",
                str(broken_bundle_dir),
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "failed"
    bundle = payload["bundles"][0]
    assert bundle["status"] == "failed"
    assert bundle["files"]["person_records"]["diff_report"]["missing_source_columns"] == ["given_name"]


def test_check_public_safety_onboarding_reports_manifest_bundle_diff_on_failure(
    tmp_path: Path,
) -> None:
    copied_root = tmp_path / "public_safety_onboarding"
    shutil.copytree(FIXTURE_ROOT, copied_root)
    person_path = copied_root / "cad_vendor_bundle" / "vendor_person_records.csv"
    with person_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    rewritten_rows = []
    for row in rows:
        row.pop("given_name", None)
        rewritten_rows.append(row)
    with person_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rewritten_rows[0]))
        writer.writeheader()
        writer.writerows(rewritten_rows)

    summary = check_public_safety_onboarding(
        manifest_path=copied_root / "example_vendor_overlay_manifest.yml",
    )

    assert summary["status"] == "failed"
    manifest = summary["manifest"]
    assert isinstance(manifest, dict)
    assert manifest["status"] == "failed"
    source_bundles = manifest["source_bundles"]
    assert isinstance(source_bundles, list)
    cad_bundle = next(bundle for bundle in source_bundles if bundle["bundle_id"] == "cad_vendor_primary")
    person_summary = next(file for file in cad_bundle["files"] if file["logical_name"] == "person_records")
    assert person_summary["diff_report"]["missing_source_columns"] == ["given_name"]
