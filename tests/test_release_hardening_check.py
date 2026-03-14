from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "release_hardening_check.py"
SPEC = importlib.util.spec_from_file_location("release_hardening_check", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def test_build_artifacts_replaces_stale_release_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    artifact_dir = tmp_path / "artifacts"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "etl_identity_engine-0.1.4.tar.gz").write_bytes(b"old-sdist")
    (artifact_dir / "etl_identity_engine-0.1.4-py3-none-any.whl").write_bytes(b"old-wheel")

    def fake_run_command(
        command: list[str],
        *,
        capture_output: bool = False,
        env: dict[str, str] | None = None,
    ):
        del capture_output, env
        output_dir = Path(command[-1])
        (output_dir / "etl_identity_engine-0.6.0.tar.gz").write_bytes(b"new-sdist")
        (output_dir / "etl_identity_engine-0.6.0-py3-none-any.whl").write_bytes(b"new-wheel")

        class Result:
            returncode = 0

        return Result()

    monkeypatch.setattr(MODULE, "_run_command", fake_run_command)

    built_paths = MODULE._build_artifacts("python", artifact_dir)

    assert [path.name for path in built_paths] == [
        "etl_identity_engine-0.6.0-py3-none-any.whl",
        "etl_identity_engine-0.6.0.tar.gz",
    ]
