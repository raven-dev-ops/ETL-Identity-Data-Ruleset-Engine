from __future__ import annotations

import json
from pathlib import Path

from etl_identity_engine.cli import main
from etl_identity_engine.ingest.public_safety_conformance import (
    check_public_safety_onboarding,
)
from etl_identity_engine.ingest.public_safety_rehearsal import (
    generate_public_safety_vendor_batches,
)
from etl_identity_engine.ingest.public_safety_vendor_profiles import (
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
