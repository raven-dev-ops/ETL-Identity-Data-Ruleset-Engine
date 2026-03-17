from __future__ import annotations

import json
from pathlib import Path

import pytest

from etl_identity_engine.cli import main
from etl_identity_engine.ingest.public_safety_conformance import (
    check_public_safety_onboarding,
)
from etl_identity_engine.ingest.public_safety_contracts import (
    CAD_CALL_FOR_SERVICE_CONTRACT,
    RMS_REPORT_PERSON_CONTRACT,
)
from etl_identity_engine.ingest.public_safety_rehearsal import (
    generate_public_safety_vendor_batches,
)
from etl_identity_engine.ingest.public_safety_vendor_profiles import (
    load_packaged_public_safety_mapping_overlay,
    list_public_safety_vendor_profiles,
)


def test_generate_public_safety_vendor_batches_defaults_to_all_shipped_profiles(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "vendor_rehearsal"

    result = generate_public_safety_vendor_batches(
        output_dir,
        profile="small",
        seed=42,
        person_count_override=8,
    )

    assert result.manifest_path.exists()
    assert result.summary_path.exists()
    assert result.summary["source_bundle_count"] == len(list_public_safety_vendor_profiles())

    onboarding_summary = check_public_safety_onboarding(manifest_path=result.manifest_path)

    assert onboarding_summary["status"] == "passed"
    manifest_summary = onboarding_summary["manifest"]
    assert isinstance(manifest_summary, dict)
    assert manifest_summary["source_bundle_count"] == len(list_public_safety_vendor_profiles())


def test_cli_generate_public_safety_vendor_batches_outputs_json_summary(
    tmp_path: Path,
    capsys,
) -> None:
    output_dir = tmp_path / "vendor_rehearsal"

    assert (
        main(
            [
                "generate-public-safety-vendor-batches",
                "--output-dir",
                str(output_dir),
                "--profile",
                "small",
                "--seed",
                "42",
                "--person-count",
                "8",
                "--cad-profile",
                "cad_county_dispatch_v1",
                "--rms-profile",
                "rms_case_management_v1",
            ]
        )
        == 0
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["cad_profiles"] == ["cad_county_dispatch_v1"]
    assert payload["rms_profiles"] == ["rms_case_management_v1"]
    manifest_path = Path(str(payload["manifest_path"]))
    assert manifest_path.exists()

    onboarding_summary = check_public_safety_onboarding(manifest_path=manifest_path)

    assert onboarding_summary["status"] == "passed"


@pytest.mark.parametrize("profile_name", [profile.name for profile in list_public_safety_vendor_profiles()])
def test_load_packaged_public_safety_mapping_overlay_accepts_all_shipped_profiles(
    profile_name: str,
) -> None:
    contract = (
        CAD_CALL_FOR_SERVICE_CONTRACT
        if profile_name.startswith("cad_")
        else RMS_REPORT_PERSON_CONTRACT
    )
    allowed_fields_by_file = {
        file_spec.logical_name: file_spec.required_columns
        for file_spec in contract.file_specs
    }

    overlay = load_packaged_public_safety_mapping_overlay(
        profile_name,
        contract_name=contract.contract_name,
        contract_version=contract.contract_version,
        allowed_fields_by_file=allowed_fields_by_file,
    )

    assert overlay.vendor_profile == profile_name
    assert overlay.overlay_label == f"vendor-profile:{profile_name}"
    assert set(overlay.files) == {
        "person_records",
        "incident_records",
        "incident_person_links",
    }
