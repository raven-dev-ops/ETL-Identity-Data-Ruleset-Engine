from __future__ import annotations

import argparse
import hashlib
import json
import sys
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = Path("dist") / "cjis-evidence-review"
DEFAULT_CADENCE_DAYS = 30
REVIEW_INDEX_NAME = "cjis_evidence_review_index.json"
REVIEW_INDEX_MARKDOWN_NAME = "cjis_evidence_review_index.md"
REPO_SCOPE_BOUNDARY = (
    "This cadence index supports repo-side evidence capture and review tracking only; "
    "agency, CSA, and policy compliance obligations remain outside the repository."
)


def _ensure_repo_paths_on_path() -> None:
    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))
    src_dir = REPO_ROOT / "src"
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))


_ensure_repo_paths_on_path()


class CjisEvidenceCadenceError(ValueError):
    """Raised when the CJIS evidence cadence state is incomplete or inconsistent."""


def _resolve_output_dir(output_dir: str) -> Path:
    from package_release_sample import resolve_output_dir

    return resolve_output_dir(output_dir, repo_root=REPO_ROOT)


def _read_project_version() -> str:
    from package_release_sample import read_project_version

    return read_project_version()


def _package_cjis_evidence_pack(**kwargs) -> Path:
    from package_cjis_evidence_pack import package_cjis_evidence_pack

    return package_cjis_evidence_pack(**kwargs)


def _evidence_manifest_name() -> str:
    from package_cjis_evidence_pack import EVIDENCE_MANIFEST_NAME

    return EVIDENCE_MANIFEST_NAME


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Capture, review, and report the recurring CJIS evidence cadence index."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    capture_parser = subparsers.add_parser(
        "capture",
        help="Generate a new CJIS evidence pack and append it to the cadence index.",
    )
    capture_parser.add_argument("--environment", default="cjis")
    capture_parser.add_argument(
        "--runtime-config",
        default=str(REPO_ROOT / "config" / "runtime_environments.yml"),
    )
    capture_parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    capture_parser.add_argument("--state-db", default=None)
    capture_parser.add_argument("--run-id", default=None)
    capture_parser.add_argument("--audit-limit", default=100, type=int)
    capture_parser.add_argument("--env-file", default=None)
    capture_parser.add_argument("--max-secret-file-age-hours", default=None, type=float)
    capture_parser.add_argument("--version", default=None)
    capture_parser.add_argument("--cadence-days", default=DEFAULT_CADENCE_DAYS, type=int)

    review_parser = subparsers.add_parser(
        "review",
        help="Mark a captured evidence pack as reviewed.",
    )
    review_parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    review_parser.add_argument("--capture-id", default=None, help="Defaults to the latest capture.")
    review_parser.add_argument("--reviewer", required=True)
    review_parser.add_argument("--reviewed-at-utc", default=None)
    review_parser.add_argument("--cadence-days", default=None, type=int)

    status_parser = subparsers.add_parser(
        "status",
        help="Render the current cadence index and overdue state.",
    )
    status_parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    status_parser.add_argument("--evaluated-at-utc", default=None)
    return parser.parse_args(argv)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _format_utc_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_utc_timestamp(value: str | None) -> datetime:
    if value is None or not value.strip():
        return _utc_now()
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise CjisEvidenceCadenceError(
            f"Invalid UTC timestamp {value!r}; expected ISO-8601"
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CjisEvidenceCadenceError(f"Unable to read JSON from {path}") from exc
    if not isinstance(payload, dict):
        raise CjisEvidenceCadenceError(f"Expected a JSON object in {path}")
    return payload


def _load_index(index_path: Path) -> dict[str, object]:
    payload = _read_json(index_path)
    captures = payload.get("captures", [])
    if not isinstance(captures, list):
        raise CjisEvidenceCadenceError(f"{index_path} must contain a captures list")
    return payload


def _read_evidence_manifest(bundle_path: Path) -> dict[str, object]:
    with zipfile.ZipFile(bundle_path) as archive:
        payload = json.loads(archive.read(_evidence_manifest_name()).decode("utf-8"))
    if not isinstance(payload, dict):
        raise CjisEvidenceCadenceError(f"Evidence manifest in {bundle_path} must be a JSON object")
    if payload.get("bundle_type") != "cjis_evidence_pack":
        raise CjisEvidenceCadenceError(f"{bundle_path} is not a CJIS evidence pack")
    return payload


def _capture_id(generated_at_utc: str) -> str:
    return generated_at_utc.replace("-", "").replace(":", "")


def _record_due_at(reference_at_utc: str, cadence_days: int) -> str:
    return _format_utc_timestamp(_parse_utc_timestamp(reference_at_utc) + timedelta(days=cadence_days))


def _record_status(record: dict[str, object], *, evaluated_at: datetime) -> tuple[str, int]:
    due_at = _parse_utc_timestamp(str(record["review_due_at_utc"]))
    days_until_due = int((due_at - evaluated_at).total_seconds() // 86400)
    if evaluated_at > due_at:
        return "overdue", days_until_due
    if record.get("reviewed_at_utc"):
        return "current", days_until_due
    return "pending", days_until_due


def _sorted_captures(captures: list[dict[str, object]]) -> list[dict[str, object]]:
    return sorted(captures, key=lambda item: str(item.get("generated_at_utc", "")))


def _index_output_dir(root: Path) -> tuple[Path, Path]:
    return root / REVIEW_INDEX_NAME, root / REVIEW_INDEX_MARKDOWN_NAME


def _build_index_markdown(index: dict[str, object]) -> str:
    lines = [
        "# CJIS Evidence Review Cadence",
        "",
        f"- environment: `{index['environment']}`",
        f"- cadence_days: `{index['cadence_days']}`",
        f"- status: `{index['status']}`",
        f"- latest_capture_id: `{index.get('latest_capture_id') or 'none'}`",
        f"- next_review_due_at_utc: `{index.get('next_review_due_at_utc') or 'none'}`",
        "",
        "## Captures",
        "",
    ]
    for record in index["captures"]:
        lines.append(
            f"- `{record['capture_id']}`: `{record['cadence_status']}` due `{record['review_due_at_utc']}`"
        )
        lines.append(f"  evidence_pack: `{record['evidence_pack_path']}`")
        if record.get("reviewed_at_utc"):
            lines.append(
                f"  reviewed_at_utc: `{record['reviewed_at_utc']}` by `{record.get('reviewer') or 'unspecified'}`"
            )
    lines.extend(["", "## Scope Boundary", "", f"- {index['scope_boundary']}"])
    return "\n".join(lines).strip() + "\n"


def _hydrate_index(
    *,
    environment: str,
    cadence_days: int,
    captures: list[dict[str, object]],
    evaluated_at: datetime,
) -> dict[str, object]:
    hydrated_captures: list[dict[str, object]] = []
    for record in _sorted_captures(captures):
        cadence_status, days_until_due = _record_status(record, evaluated_at=evaluated_at)
        hydrated_captures.append(
            {
                **record,
                "cadence_status": cadence_status,
                "days_until_due": days_until_due,
            }
        )
    latest_capture = hydrated_captures[-1] if hydrated_captures else None
    overdue_capture_ids = [
        str(record["capture_id"])
        for record in hydrated_captures
        if record["cadence_status"] == "overdue"
    ]
    return {
        "project": "etl-identity-engine",
        "bundle_type": "cjis_evidence_review_index",
        "environment": environment,
        "cadence_days": cadence_days,
        "evaluated_at_utc": _format_utc_timestamp(evaluated_at),
        "status": "missing" if latest_capture is None else latest_capture["cadence_status"],
        "latest_capture_id": None if latest_capture is None else latest_capture["capture_id"],
        "latest_capture_generated_at_utc": (
            None if latest_capture is None else latest_capture["generated_at_utc"]
        ),
        "latest_reviewed_at_utc": None if latest_capture is None else latest_capture.get("reviewed_at_utc"),
        "next_review_due_at_utc": None if latest_capture is None else latest_capture["review_due_at_utc"],
        "overdue_capture_ids": overdue_capture_ids,
        "scope_boundary": REPO_SCOPE_BOUNDARY,
        "captures": hydrated_captures,
    }


def _write_index(root: Path, index: dict[str, object]) -> tuple[Path, Path]:
    index_path, markdown_path = _index_output_dir(root)
    _write_json(index_path, index)
    markdown_path.write_text(_build_index_markdown(index), encoding="utf-8")
    return index_path, markdown_path


def capture_cjis_evidence_cadence(
    *,
    output_dir: Path,
    environment_name: str,
    runtime_config_path: Path,
    state_db: str | Path | None,
    run_id: str | None,
    audit_limit: int,
    env_file: Path | None,
    max_secret_file_age_hours: float | None,
    cadence_days: int,
    version: str,
) -> dict[str, object]:
    if cadence_days <= 0:
        raise CjisEvidenceCadenceError("cadence_days must be greater than 0")
    root = output_dir.resolve()
    root.mkdir(parents=True, exist_ok=True)
    existing_index_path, _ = _index_output_dir(root)
    existing_index = _load_index(existing_index_path) if existing_index_path.exists() else None
    if existing_index is not None and str(existing_index["environment"]) != environment_name:
        raise CjisEvidenceCadenceError(
            "Existing cadence index environment does not match the requested environment"
        )

    capture_root = root / "captures"
    evidence_output_root = capture_root / _format_utc_timestamp(_utc_now()).replace(":", "").replace("-", "")
    evidence_output_root.mkdir(parents=True, exist_ok=False)
    bundle_path = _package_cjis_evidence_pack(
        output_dir=evidence_output_root,
        environment_name=environment_name,
        runtime_config_path=runtime_config_path.resolve(),
        state_db=state_db,
        run_id=run_id,
        audit_limit=audit_limit,
        env_file=None if env_file is None else env_file.resolve(),
        max_secret_file_age_hours=max_secret_file_age_hours,
        version=version,
    )
    evidence_manifest = _read_evidence_manifest(bundle_path)
    generated_at_utc = str(evidence_manifest["generated_at_utc"])
    capture_id = _capture_id(generated_at_utc)

    captures = [] if existing_index is None else list(existing_index["captures"])
    if any(str(record.get("capture_id")) == capture_id for record in captures):
        raise CjisEvidenceCadenceError(f"CJIS evidence capture {capture_id} already exists")
    captures.append(
        {
            "capture_id": capture_id,
            "generated_at_utc": generated_at_utc,
            "evidence_pack_path": str(bundle_path.resolve()),
            "evidence_pack_sha256": _sha256_path(bundle_path),
            "evidence_manifest_path": f"{bundle_path.resolve()}::{_evidence_manifest_name()}",
            "preflight_status": evidence_manifest.get("preflight_status"),
            "selected_run_id": evidence_manifest.get("selected_run_id"),
            "review_due_at_utc": _record_due_at(generated_at_utc, cadence_days),
            "reviewed_at_utc": None,
            "reviewer": None,
        }
    )

    index = _hydrate_index(
        environment=environment_name,
        cadence_days=cadence_days,
        captures=captures,
        evaluated_at=_parse_utc_timestamp(generated_at_utc),
    )
    index_path, markdown_path = _write_index(root, index)
    return {
        "status": index["status"],
        "capture_id": capture_id,
        "evidence_pack_path": str(bundle_path.resolve()),
        "review_due_at_utc": index["next_review_due_at_utc"],
        "index_path": str(index_path),
        "index_markdown_path": str(markdown_path),
        "index": index,
    }


def review_cjis_evidence_capture(
    *,
    output_dir: Path,
    reviewer: str,
    capture_id: str | None = None,
    reviewed_at_utc: str | None = None,
    cadence_days: int | None = None,
) -> dict[str, object]:
    normalized_reviewer = reviewer.strip()
    if not normalized_reviewer:
        raise CjisEvidenceCadenceError("reviewer must be a non-empty string")
    root = output_dir.resolve()
    index_path, _ = _index_output_dir(root)
    if not index_path.exists():
        raise FileNotFoundError(f"CJIS evidence cadence index not found: {index_path}")
    existing_index = _load_index(index_path)
    captures = _sorted_captures(list(existing_index["captures"]))
    if not captures:
        raise CjisEvidenceCadenceError("CJIS evidence cadence index does not contain any captures")

    resolved_cadence_days = cadence_days or int(existing_index["cadence_days"])
    target_capture_id = capture_id or str(captures[-1]["capture_id"])
    review_timestamp = _format_utc_timestamp(_parse_utc_timestamp(reviewed_at_utc))
    updated = False
    updated_captures: list[dict[str, object]] = []
    for record in captures:
        updated_record = dict(record)
        if str(record["capture_id"]) == target_capture_id:
            updated_record["reviewed_at_utc"] = review_timestamp
            updated_record["reviewer"] = normalized_reviewer
            updated_record["review_due_at_utc"] = _record_due_at(review_timestamp, resolved_cadence_days)
            updated = True
        updated_captures.append(updated_record)
    if not updated:
        raise CjisEvidenceCadenceError(f"Unknown CJIS evidence capture_id {target_capture_id!r}")

    index = _hydrate_index(
        environment=str(existing_index["environment"]),
        cadence_days=resolved_cadence_days,
        captures=updated_captures,
        evaluated_at=_parse_utc_timestamp(review_timestamp),
    )
    updated_index_path, markdown_path = _write_index(root, index)
    return {
        "status": index["status"],
        "capture_id": target_capture_id,
        "reviewed_at_utc": review_timestamp,
        "reviewer": normalized_reviewer,
        "index_path": str(updated_index_path),
        "index_markdown_path": str(markdown_path),
        "index": index,
    }


def status_cjis_evidence_cadence(
    *,
    output_dir: Path,
    evaluated_at_utc: str | None = None,
) -> dict[str, object]:
    root = output_dir.resolve()
    index_path, _ = _index_output_dir(root)
    if not index_path.exists():
        return {
            "status": "missing",
            "index_path": str(index_path),
            "scope_boundary": REPO_SCOPE_BOUNDARY,
            "captures": [],
        }
    existing_index = _load_index(index_path)
    evaluated_at = _parse_utc_timestamp(evaluated_at_utc)
    return _hydrate_index(
        environment=str(existing_index["environment"]),
        cadence_days=int(existing_index["cadence_days"]),
        captures=list(existing_index["captures"]),
        evaluated_at=evaluated_at,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "capture":
        payload = capture_cjis_evidence_cadence(
            output_dir=_resolve_output_dir(args.output_dir),
            environment_name=args.environment,
            runtime_config_path=Path(args.runtime_config),
            state_db=args.state_db,
            run_id=args.run_id,
            audit_limit=args.audit_limit,
            env_file=None if args.env_file is None else Path(args.env_file),
            max_secret_file_age_hours=args.max_secret_file_age_hours,
            cadence_days=args.cadence_days,
            version=args.version or _read_project_version(),
        )
    elif args.command == "review":
        payload = review_cjis_evidence_capture(
            output_dir=_resolve_output_dir(args.output_dir),
            reviewer=args.reviewer,
            capture_id=args.capture_id,
            reviewed_at_utc=args.reviewed_at_utc,
            cadence_days=args.cadence_days,
        )
    else:
        payload = status_cjis_evidence_cadence(
            output_dir=_resolve_output_dir(args.output_dir),
            evaluated_at_utc=args.evaluated_at_utc,
        )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
