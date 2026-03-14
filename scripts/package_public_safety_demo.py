from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence
import zipfile

from package_release_sample import (
    PROJECT_NAME,
    REPO_ROOT,
    _run_pipeline,
    _zip_entry_timestamp,
    parse_formats,
    read_project_version,
    resolve_generated_at_utc,
    resolve_output_dir,
    resolve_source_commit,
)


DEFAULT_OUTPUT_DIR = Path("dist") / "public-safety-demo"
MANIFEST_NAME = "demo_manifest.json"
DEMO_ARTIFACTS = (
    Path("data/synthetic_sources/incident_records.csv"),
    Path("data/synthetic_sources/incident_person_links.csv"),
    Path("data/synthetic_sources/generation_summary.json"),
    Path("data/golden/golden_person_records.csv"),
    Path("data/golden/source_to_golden_crosswalk.csv"),
    Path("data/public_safety_demo/incident_identity_view.csv"),
    Path("data/public_safety_demo/golden_person_activity.csv"),
    Path("data/public_safety_demo/public_safety_demo_dashboard.html"),
    Path("data/public_safety_demo/public_safety_demo_report.md"),
    Path("data/public_safety_demo/public_safety_demo_scenarios.json"),
    Path("data/public_safety_demo/public_safety_demo_summary.json"),
    Path("data/public_safety_demo/public_safety_demo_walkthrough.md"),
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Package the synthetic public-safety demo bundle."
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory where the packaged demo zip will be written.",
    )
    parser.add_argument("--profile", default="small", choices=["small", "medium", "large"])
    parser.add_argument("--seed", default=42, type=int)
    parser.add_argument(
        "--formats",
        default="csv,parquet",
        help="Comma-separated generate/run-all source formats to request. CSV is required for the demo bundle.",
    )
    parser.add_argument(
        "--version",
        default=None,
        help="Version to embed in the bundle name and manifest. Defaults to pyproject.toml.",
    )
    return parser.parse_args(argv)


def build_bundle_name(version: str, profile: str) -> str:
    return f"etl-identity-engine-v{version}-public-safety-demo-{profile}.zip"


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
        "bundle_type": "public_safety_demo",
        "version": version,
        "profile": profile,
        "seed": seed,
        "formats": list(formats),
        "generated_at_utc": generated_at_utc,
        "source_commit": source_commit,
        "artifacts": list(artifacts),
    }


def _artifact_names() -> tuple[str, ...]:
    return tuple(path.as_posix() for path in DEMO_ARTIFACTS)


def package_public_safety_demo(
    *,
    output_dir: Path,
    profile: str,
    seed: int,
    formats: Sequence[str],
    version: str,
    repo_root: Path = REPO_ROOT,
    generated_at_utc: str | None = None,
    source_commit: str | None = None,
) -> Path:
    if "csv" not in formats:
        raise ValueError("public safety demo packaging requires csv output in --formats")

    output_dir.mkdir(parents=True, exist_ok=True)
    bundle_path = output_dir / build_bundle_name(version, profile)
    artifact_names = _artifact_names()
    resolved_generated_at_utc = resolve_generated_at_utc(
        repo_root=repo_root,
        explicit_value=generated_at_utc,
    )
    manifest = build_manifest(
        version=version,
        profile=profile,
        seed=seed,
        formats=formats,
        generated_at_utc=resolved_generated_at_utc,
        source_commit=source_commit or resolve_source_commit(repo_root),
        artifacts=artifact_names,
    )
    zip_timestamp = _zip_entry_timestamp(resolved_generated_at_utc)

    import tempfile

    with tempfile.TemporaryDirectory(prefix="etl-public-safety-demo-") as temp_dir:
        base_dir = Path(temp_dir)
        _run_pipeline(
            base_dir=base_dir,
            profile=profile,
            seed=seed,
            formats=formats,
            repo_root=repo_root,
        )

        with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for relative_artifact in DEMO_ARTIFACTS:
                source_path = base_dir / relative_artifact
                if not source_path.exists():
                    raise FileNotFoundError(f"Missing expected demo artifact: {source_path}")
                zip_info = zipfile.ZipInfo(relative_artifact.as_posix(), date_time=zip_timestamp)
                zip_info.compress_type = zipfile.ZIP_DEFLATED
                zip_info.external_attr = 0o100644 << 16
                archive.writestr(zip_info, source_path.read_bytes())

            manifest_info = zipfile.ZipInfo(MANIFEST_NAME, date_time=zip_timestamp)
            manifest_info.compress_type = zipfile.ZIP_DEFLATED
            manifest_info.external_attr = 0o100644 << 16
            archive.writestr(
                manifest_info,
                json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            )

    return bundle_path


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    formats = parse_formats(args.formats)
    version = args.version or read_project_version()
    bundle_path = package_public_safety_demo(
        output_dir=resolve_output_dir(args.output_dir),
        profile=args.profile,
        seed=args.seed,
        formats=formats,
        version=version,
    )
    print(f"public safety demo bundle written: {bundle_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
