from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

import tomllib


PROJECT_NAME = "etl-identity-engine"
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = Path("dist") / "release-samples"
MANIFEST_NAME = "manifest.json"
RELEASE_ARTIFACTS = (
    Path("data/normalized/normalized_person_records.csv"),
    Path("data/matches/candidate_scores.csv"),
    Path("data/matches/blocking_metrics.csv"),
    Path("data/matches/entity_clusters.csv"),
    Path("data/golden/golden_person_records.csv"),
    Path("data/golden/source_to_golden_crosswalk.csv"),
    Path("data/review_queue/manual_review_queue.csv"),
    Path("data/exceptions/invalid_dobs.csv"),
    Path("data/exceptions/malformed_phones.csv"),
    Path("data/exceptions/normalization_failures.csv"),
    Path("data/exceptions/run_report.md"),
    Path("data/exceptions/run_summary.json"),
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Package the documented release sample bundle for a tagged release."
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory where the packaged release sample zip will be written.",
    )
    parser.add_argument("--profile", default="small", choices=["small", "medium", "large"])
    parser.add_argument("--seed", default=42, type=int)
    parser.add_argument(
        "--formats",
        default="csv,parquet",
        help="Comma-separated generate/run-all source formats to request.",
    )
    parser.add_argument(
        "--version",
        default=None,
        help="Release version to embed in the bundle name and manifest. Defaults to pyproject.toml.",
    )
    return parser.parse_args(argv)


def parse_formats(value: str) -> tuple[str, ...]:
    formats = tuple(part.strip().lower() for part in value.split(",") if part.strip())
    if not formats:
        raise ValueError("At least one format must be provided")

    unsupported_formats = sorted({fmt for fmt in formats if fmt not in {"csv", "parquet"}})
    if unsupported_formats:
        raise ValueError(
            f"Unsupported formats for release sample packaging: {', '.join(unsupported_formats)}"
        )
    return formats


def resolve_output_dir(output_dir: str, *, repo_root: Path = REPO_ROOT) -> Path:
    candidate = Path(output_dir)
    if candidate.is_absolute():
        return candidate
    return repo_root / candidate


def read_project_version(pyproject_path: Path = REPO_ROOT / "pyproject.toml") -> str:
    payload = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    version = str(payload.get("project", {}).get("version", "")).strip()
    if not version:
        raise ValueError(f"Unable to resolve project.version from {pyproject_path}")
    return version


def resolve_source_commit(repo_root: Path = REPO_ROOT) -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        return "unknown"
    return completed.stdout.strip() or "unknown"


def build_bundle_name(version: str, profile: str) -> str:
    return f"etl-identity-engine-v{version}-sample-{profile}.zip"


def build_manifest(
    *,
    version: str,
    profile: str,
    seed: int,
    formats: Sequence[str],
    generated_at_utc: str,
    source_commit: str,
    artifacts: Sequence[str],
) -> dict[str, object]:
    return {
        "project": PROJECT_NAME,
        "version": version,
        "profile": profile,
        "seed": seed,
        "formats": list(formats),
        "generated_at_utc": generated_at_utc,
        "source_commit": source_commit,
        "artifacts": list(artifacts),
    }


def _artifact_names() -> tuple[str, ...]:
    return tuple(path.as_posix() for path in RELEASE_ARTIFACTS)


def _build_pythonpath(repo_root: Path) -> str:
    src_path = str(repo_root / "src")
    existing_path = os.environ.get("PYTHONPATH", "")
    if not existing_path:
        return src_path
    return os.pathsep.join((src_path, existing_path))


def _run_pipeline(
    *,
    base_dir: Path,
    profile: str,
    seed: int,
    formats: Sequence[str],
    repo_root: Path = REPO_ROOT,
) -> None:
    env = os.environ.copy()
    env["PYTHONPATH"] = _build_pythonpath(repo_root)
    command = [
        sys.executable,
        "-m",
        "etl_identity_engine.cli",
        "run-all",
        "--base-dir",
        str(base_dir),
        "--profile",
        profile,
        "--seed",
        str(seed),
        "--formats",
        ",".join(formats),
    ]
    completed = subprocess.run(
        command,
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = "\n".join(part for part in (completed.stdout.strip(), completed.stderr.strip()) if part)
        raise RuntimeError(f"release sample pipeline run failed ({completed.returncode})\n{detail}")


def package_release_sample(
    *,
    output_dir: Path,
    profile: str,
    seed: int,
    formats: Sequence[str],
    version: str,
    repo_root: Path = REPO_ROOT,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    bundle_path = output_dir / build_bundle_name(version, profile)
    artifact_names = _artifact_names()
    generated_at_utc = (
        datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )
    manifest = build_manifest(
        version=version,
        profile=profile,
        seed=seed,
        formats=formats,
        generated_at_utc=generated_at_utc,
        source_commit=resolve_source_commit(repo_root),
        artifacts=artifact_names,
    )

    with tempfile.TemporaryDirectory(prefix="etl-release-sample-") as temp_dir:
        base_dir = Path(temp_dir)
        _run_pipeline(
            base_dir=base_dir,
            profile=profile,
            seed=seed,
            formats=formats,
            repo_root=repo_root,
        )

        with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for relative_artifact in RELEASE_ARTIFACTS:
                source_path = base_dir / relative_artifact
                if not source_path.exists():
                    raise FileNotFoundError(f"Missing expected release artifact: {source_path}")
                archive.write(source_path, arcname=relative_artifact.as_posix())
            archive.writestr(
                MANIFEST_NAME,
                json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            )

    return bundle_path


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    formats = parse_formats(args.formats)
    version = args.version or read_project_version()
    bundle_path = package_release_sample(
        output_dir=resolve_output_dir(args.output_dir),
        profile=args.profile,
        seed=args.seed,
        formats=formats,
        version=version,
    )
    print(f"release sample bundle written: {bundle_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
