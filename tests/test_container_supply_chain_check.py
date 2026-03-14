from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "container_supply_chain_check.py"
SPEC = importlib.util.spec_from_file_location("container_supply_chain_check", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def test_extract_python_packages_from_pip_inspect_payload() -> None:
    payload = {
        "installed": [
            {"metadata": {"name": "FastAPI", "version": "0.115.0"}},
            {"metadata": {"name": "PyYAML", "version": "6.0.2"}},
            {"metadata": {"name": "", "version": "ignored"}},
            {"not_metadata": {}},
        ]
    }

    assert MODULE._extract_python_packages(payload) == [
        {"name": "FastAPI", "version": "0.115.0"},
        {"name": "PyYAML", "version": "6.0.2"},
    ]


def test_parse_dpkg_query_output_sorts_packages() -> None:
    output = "zlib1g\t1:1.2.13.dfsg-1\nbash\t5.2.15-2\n\ninvalid-line\n"

    assert MODULE._parse_dpkg_query_output(output) == [
        {"name": "bash", "version": "5.2.15-2"},
        {"name": "zlib1g", "version": "1:1.2.13.dfsg-1"},
    ]


def test_write_requirements_lock_writes_pinned_package_versions(tmp_path: Path) -> None:
    output_path = tmp_path / "container_requirements.txt"

    MODULE._write_requirements_lock(
        output_path,
        [
            {"name": "FastAPI", "version": "0.115.0"},
            {"name": "PyYAML", "version": "6.0.2"},
        ],
    )

    assert output_path.read_text(encoding="utf-8") == "FastAPI==0.115.0\nPyYAML==6.0.2\n"


def test_write_requirements_lock_skips_local_project_package(tmp_path: Path) -> None:
    output_path = tmp_path / "container_requirements.txt"

    MODULE._write_requirements_lock(
        output_path,
        [
            {"name": "etl-identity-engine", "version": "0.6.0"},
            {"name": "FastAPI", "version": "0.115.0"},
        ],
        excluded_package_names={MODULE.PROJECT_NAME},
    )

    assert output_path.read_text(encoding="utf-8") == "FastAPI==0.115.0\n"


def test_build_attestation_includes_hashed_artifact_inventory(tmp_path: Path) -> None:
    sbom_path = tmp_path / "container_sbom.json"
    provenance_path = tmp_path / "container_provenance.json"
    audit_path = tmp_path / "container_dependency_audit.json"
    for path in (sbom_path, provenance_path, audit_path):
        path.write_text(path.name, encoding="utf-8")

    attestation = MODULE._build_attestation(
        image={
            "image_tag": "etl-identity-engine:test",
            "image_id": "sha256:test-image",
            "repo_digests": [],
        },
        output_root=tmp_path,
        artifact_paths=[sbom_path, provenance_path, audit_path],
        generated_at_utc="2026-03-14T20:00:00Z",
        source_commit="abc123",
    )

    assert attestation["predicate_type"] == MODULE.ATTESTATION_PREDICATE_TYPE
    assert attestation["subject"]["image_tag"] == "etl-identity-engine:test"
    assert attestation["subject"]["image_id"] == "sha256:test-image"
    assert [artifact["name"] for artifact in attestation["artifacts"]] == [
        "container_dependency_audit.json",
        "container_provenance.json",
        "container_sbom.json",
    ]
    assert all(len(artifact["sha256"]) == 64 for artifact in attestation["artifacts"])


def test_normalize_image_inspect_uses_expected_fields() -> None:
    payload = [
        {
            "Id": "sha256:image",
            "RepoTags": ["etl-identity-engine:test"],
            "RepoDigests": ["etl-identity-engine@sha256:digest"],
            "Created": "2026-03-14T20:00:00Z",
            "Architecture": "amd64",
            "Os": "linux",
            "RootFS": {"Layers": ["sha256:layer1", "sha256:layer2"]},
            "Config": {"Labels": {"org.opencontainers.image.source": "repo"}},
        }
    ]

    normalized = MODULE._normalize_image_inspect(payload, image_tag="etl-identity-engine:test")

    assert normalized == {
        "image_tag": "etl-identity-engine:test",
        "image_id": "sha256:image",
        "repo_tags": ["etl-identity-engine:test"],
        "repo_digests": ["etl-identity-engine@sha256:digest"],
        "created": "2026-03-14T20:00:00Z",
        "architecture": "amd64",
        "os": "linux",
        "rootfs_layers": ["sha256:layer1", "sha256:layer2"],
        "labels": {"org.opencontainers.image.source": "repo"},
    }


def test_main_writes_supply_chain_outputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    image_tag = "etl-identity-engine:test"
    created_audits: list[Path] = []

    def fake_build_image(tag: str) -> None:
        assert tag == image_tag

    def fake_git_head_commit() -> str:
        return "abc123"

    def fake_capture_json(command: list[str], *, failure_hint: str, env=None):
        del env
        if command[:3] == ["docker", "image", "inspect"]:
            return [
                {
                    "Id": "sha256:image",
                    "RepoTags": [image_tag],
                    "RepoDigests": [],
                    "Created": "2026-03-14T20:00:00Z",
                    "Architecture": "amd64",
                    "Os": "linux",
                    "RootFS": {"Layers": ["sha256:layer1"]},
                    "Config": {"Labels": {}},
                }
            ]
        if command[:6] == ["docker", "run", "--rm", "--entrypoint", "python", image_tag]:
            return {
                "installed": [
                    {"metadata": {"name": "fastapi", "version": "0.115.0"}},
                    {"metadata": {"name": "PyYAML", "version": "6.0.2"}},
                ]
            }
        raise AssertionError(f"unexpected json command for {failure_hint}: {command}")

    def fake_capture_text(command: list[str], *, failure_hint: str, env=None) -> str:
        del command, failure_hint, env
        return "bash\t5.2.15-2\nzlib1g\t1:1.2.13.dfsg-1\n"

    def fake_run_dependency_audit(requirements_lock_path: Path, output_path: Path) -> None:
        assert requirements_lock_path.exists()
        created_audits.append(output_path)
        output_path.write_text(json.dumps({"dependencies": []}) + "\n", encoding="utf-8")

    monkeypatch.setattr(MODULE, "_build_image", fake_build_image)
    monkeypatch.setattr(MODULE, "_git_head_commit", fake_git_head_commit)
    monkeypatch.setattr(MODULE, "_capture_json", fake_capture_json)
    monkeypatch.setattr(MODULE, "_capture_text", fake_capture_text)
    monkeypatch.setattr(MODULE, "_run_dependency_audit", fake_run_dependency_audit)

    assert (
        MODULE.main(
            [
                "--output-dir",
                str(tmp_path),
                "--image-tag",
                image_tag,
            ]
        )
        == 0
    )

    assert created_audits == [tmp_path / "container_dependency_audit.json"]
    assert (tmp_path / "container_provenance.json").exists()
    assert (tmp_path / "container_sbom.json").exists()
    assert (tmp_path / "container_attestation.json").exists()
    assert (tmp_path / "container_supply_chain_summary.json").exists()
    assert (tmp_path / "container_requirements.txt").read_text(encoding="utf-8") == (
        "fastapi==0.115.0\nPyYAML==6.0.2\n"
    )
