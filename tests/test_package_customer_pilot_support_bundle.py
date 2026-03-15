from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import zipfile


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

PACKAGE_SCRIPT_PATH = SCRIPTS_DIR / "package_customer_pilot_bundle.py"
PACKAGE_SPEC = importlib.util.spec_from_file_location("package_customer_pilot_bundle_script", PACKAGE_SCRIPT_PATH)
assert PACKAGE_SPEC and PACKAGE_SPEC.loader
PACKAGE_MODULE = importlib.util.module_from_spec(PACKAGE_SPEC)
sys.modules[PACKAGE_SPEC.name] = PACKAGE_MODULE
PACKAGE_SPEC.loader.exec_module(PACKAGE_MODULE)

SUPPORT_SCRIPT_PATH = SCRIPTS_DIR / "package_customer_pilot_support_bundle.py"
SUPPORT_SPEC = importlib.util.spec_from_file_location("package_customer_pilot_support_bundle_script", SUPPORT_SCRIPT_PATH)
assert SUPPORT_SPEC and SUPPORT_SPEC.loader
SUPPORT_MODULE = importlib.util.module_from_spec(SUPPORT_SPEC)
sys.modules[SUPPORT_SPEC.name] = SUPPORT_MODULE
SUPPORT_SPEC.loader.exec_module(SUPPORT_MODULE)


def test_package_customer_pilot_support_bundle_collects_redacted_artifacts(tmp_path: Path) -> None:
    pilot_bundle = PACKAGE_MODULE.package_customer_pilot_bundle(
        output_dir=tmp_path / "pilot",
        source_manifest=Path("fixtures/public_safety_regressions/manifest.yml"),
        pilot_name="public-safety-regressions",
        version="1.0.0",
    )
    extracted_root = tmp_path / "extracted"
    with zipfile.ZipFile(pilot_bundle) as archive:
        archive.extractall(extracted_root)

    state_db = extracted_root / "state" / "pipeline_state.sqlite"
    (extracted_root / "runtime").mkdir(parents=True, exist_ok=True)
    (extracted_root / "runtime" / "pilot_bootstrap.json").write_text(
        json.dumps(
            {
                "pilot_name": "public-safety-regressions",
                "state_db": str(state_db),
                "run_id": "RUN-TEST-001",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (extracted_root / "runtime" / "pilot_runtime.env").write_text(
        "\n".join(
            [
                "ETL_IDENTITY_SERVICE_READER_API_KEY=reader-secret",
                "ETL_IDENTITY_SERVICE_OPERATOR_API_KEY=operator-secret",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (extracted_root / "runtime" / "logs").mkdir(parents=True, exist_ok=True)
    (extracted_root / "runtime" / "logs" / "service_api.stderr.log").write_text(
        "Bearer secret-token\npostgresql+psycopg://etl_identity:topsecret@db.internal:5432/identity_state\n",
        encoding="utf-8",
    )

    support_bundle = SUPPORT_MODULE.package_customer_pilot_support_bundle(
        bundle_root=extracted_root,
        output_dir=tmp_path / "support",
        state_db=None,
        audit_event_limit=10,
        run_limit=5,
    )

    with zipfile.ZipFile(support_bundle) as archive:
        members = set(archive.namelist())
        assert SUPPORT_MODULE.SUPPORT_MANIFEST_NAME in members
        assert "runtime/pilot_bootstrap.redacted.json" in members
        assert "runtime/pilot_runtime.redacted.json" in members
        assert "state/state_metadata.json" in members
        assert "state/recent_runs.json" in members
        assert "logs/service_api.stderr.log" in members

        runtime_env = json.loads(archive.read("runtime/pilot_runtime.redacted.json").decode("utf-8"))
        assert runtime_env["ETL_IDENTITY_SERVICE_READER_API_KEY"] == "[REDACTED auth_material]"
        assert runtime_env["ETL_IDENTITY_SERVICE_OPERATOR_API_KEY"] == "[REDACTED auth_material]"

        log_text = archive.read("logs/service_api.stderr.log").decode("utf-8")
        assert "Bearer [REDACTED]" in log_text
        assert ":topsecret@" not in log_text

        manifest = json.loads(archive.read(SUPPORT_MODULE.SUPPORT_MANIFEST_NAME).decode("utf-8"))
        assert manifest["bundle_type"] == "customer_pilot_support"
        assert "state/state_metadata.json" in manifest["artifacts"]
