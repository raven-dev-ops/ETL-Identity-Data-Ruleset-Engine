from __future__ import annotations

import json
from pathlib import Path

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
