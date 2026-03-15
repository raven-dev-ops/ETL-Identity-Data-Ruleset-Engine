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


def _ensure_repo_src_on_path() -> None:
    src_dir = REPO_ROOT / "src"
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))


def _signature_sidecar_name(manifest_name: str) -> str:
    _ensure_repo_src_on_path()
    from etl_identity_engine.handoff_signing import signature_sidecar_name

    return signature_sidecar_name(manifest_name)


def _write_detached_signature(
    *,
    destination: Path,
    manifest_path: str,
    manifest_bytes: bytes,
    private_key_path: Path,
    signer_identity: str | None,
    key_id: str | None,
) -> dict[str, str]:
    _ensure_repo_src_on_path()
    from etl_identity_engine.handoff_signing import write_detached_signature

    return write_detached_signature(
        destination=destination,
        manifest_path=manifest_path,
        manifest_bytes=manifest_bytes,
        private_key_path=private_key_path,
        signer_identity=signer_identity,
        key_id=key_id,
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
    parser.add_argument(
        "--signing-key",
        default=None,
        help="Optional Ed25519 private key PEM used to emit a detached manifest signature.",
    )
    parser.add_argument(
        "--signer-identity",
        default=None,
        help="Optional signer identity to record in the detached signature metadata.",
    )
    parser.add_argument(
        "--key-id",
        default=None,
        help="Optional key identifier to record in the detached signature metadata.",
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


def _normalize_utc_timestamp(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid UTC timestamp: {value}") from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_generated_at_utc(
    *,
    repo_root: Path = REPO_ROOT,
    explicit_value: str | None = None,
    environ: dict[str, str] | None = None,
) -> str:
    if explicit_value is not None:
        return _normalize_utc_timestamp(explicit_value)

    effective_environ = environ if environ is not None else os.environ
    source_date_epoch = effective_environ.get("SOURCE_DATE_EPOCH", "").strip()
    if source_date_epoch:
        try:
            epoch_seconds = int(source_date_epoch)
        except ValueError as exc:
            raise ValueError(
                f"Invalid SOURCE_DATE_EPOCH value: {source_date_epoch}"
            ) from exc
        return (
            datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )

    completed = subprocess.run(
        ["git", "show", "-s", "--format=%cI", "HEAD"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode == 0 and completed.stdout.strip():
        return _normalize_utc_timestamp(completed.stdout.strip())

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_source_commit(repo_root: Path = REPO_ROOT) -> str:
    completed = subprocess.run(
        ["git", "describe", "--always", "--dirty", "--broken"],
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


def _zip_entry_timestamp(generated_at_utc: str) -> tuple[int, int, int, int, int, int]:
    normalized = _normalize_utc_timestamp(generated_at_utc)
    parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    return (
        parsed.year,
        parsed.month,
        parsed.day,
        parsed.hour,
        parsed.minute,
        parsed.second - (parsed.second % 2),
    )


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
    generated_at_utc: str | None = None,
    source_commit: str | None = None,
    signing_key: Path | None = None,
    signer_identity: str | None = None,
    key_id: str | None = None,
) -> Path:
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
    manifest_bytes = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8")
    signature_bytes: bytes | None = None
    signature_name: str | None = None
    if signing_key is not None:
        signature_name = _signature_sidecar_name(MANIFEST_NAME)
        with tempfile.TemporaryDirectory(prefix="etl-release-signature-") as signature_dir:
            signature_path = Path(signature_dir) / signature_name
            _write_detached_signature(
                destination=signature_path,
                manifest_path=MANIFEST_NAME,
                manifest_bytes=manifest_bytes,
                private_key_path=signing_key,
                signer_identity=signer_identity,
                key_id=key_id,
            )
            signature_bytes = signature_path.read_bytes()

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
                zip_info = zipfile.ZipInfo(relative_artifact.as_posix(), date_time=zip_timestamp)
                zip_info.compress_type = zipfile.ZIP_DEFLATED
                zip_info.external_attr = 0o100644 << 16
                archive.writestr(zip_info, source_path.read_bytes())

            manifest_info = zipfile.ZipInfo(MANIFEST_NAME, date_time=zip_timestamp)
            manifest_info.compress_type = zipfile.ZIP_DEFLATED
            manifest_info.external_attr = 0o100644 << 16
            archive.writestr(manifest_info, manifest_bytes)

            if signature_bytes is not None and signature_name is not None:
                signature_info = zipfile.ZipInfo(signature_name, date_time=zip_timestamp)
                signature_info.compress_type = zipfile.ZIP_DEFLATED
                signature_info.external_attr = 0o100644 << 16
                archive.writestr(signature_info, signature_bytes)

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
        signing_key=None if args.signing_key is None else Path(args.signing_key).resolve(),
        signer_identity=args.signer_identity,
        key_id=args.key_id,
    )
    print(f"release sample bundle written: {bundle_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
