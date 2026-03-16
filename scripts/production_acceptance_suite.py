from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = Path("dist") / "production-acceptance"
DEFAULT_EVIDENCE_REVIEW_INDEX = Path("dist") / "cjis-evidence-review" / "cjis_evidence_review_index.json"
REPORT_NAME = "production_acceptance_report.json"
REPORT_MARKDOWN_NAME = "production_acceptance_report.md"
READY_STATUSES = {"ready", "current"}
REQUIRED_HA_STEPS = (
    "schema_upgrade_against_writer_endpoint",
    "service_reconnected_after_writer_failover",
    "backup_restored_to_clean_postgresql_target",
    "replay_recovered_run_from_restored_postgresql_state",
)


def _ensure_repo_paths_on_path() -> None:
    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))
    src_dir = REPO_ROOT / "src"
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))


_ensure_repo_paths_on_path()


class ProductionAcceptanceError(ValueError):
    """Raised when the production acceptance suite inputs are invalid."""


def _resolve_output_dir(output_dir: str) -> Path:
    from package_release_sample import resolve_output_dir

    return resolve_output_dir(output_dir, repo_root=REPO_ROOT)


def _status_cjis_evidence_cadence(*, output_dir: Path, evaluated_at_utc: str | None):
    from manage_cjis_evidence_cadence import status_cjis_evidence_cadence

    return status_cjis_evidence_cadence(output_dir=output_dir, evaluated_at_utc=evaluated_at_utc)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate the protected-pilot production acceptance suite and emit a readiness report."
    )
    parser.add_argument("--promotion-manifest", required=True)
    parser.add_argument(
        "--evidence-review-index",
        default=str(DEFAULT_EVIDENCE_REVIEW_INDEX),
        help="CJIS evidence review index created by manage_cjis_evidence_cadence.py.",
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument(
        "--service-base-url",
        default=None,
        help="Optional live service base URL for /healthz and /readyz probes.",
    )
    parser.add_argument(
        "--service-header",
        action="append",
        default=[],
        help="Optional repeated request header in Name=Value form for live service probes.",
    )
    parser.add_argument(
        "--evaluated-at-utc",
        default=None,
        help="Optional ISO-8601 timestamp used when evaluating overdue status.",
    )
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
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _read_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProductionAcceptanceError(f"Unable to read JSON from {path}") from exc
    if not isinstance(payload, dict):
        raise ProductionAcceptanceError(f"Expected a JSON object in {path}")
    return payload


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_evidence_manifest(bundle_path: Path) -> dict[str, object]:
    with zipfile.ZipFile(bundle_path) as archive:
        payload = json.loads(archive.read("evidence_manifest.json").decode("utf-8"))
    if not isinstance(payload, dict):
        raise ProductionAcceptanceError(f"Evidence manifest in {bundle_path} must be a JSON object")
    return payload


def _parse_headers(values: list[str]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for item in values:
        name, separator, value = item.partition("=")
        if not separator or not name.strip():
            raise ProductionAcceptanceError(
                f"Invalid --service-header value {item!r}; expected Name=Value"
            )
        headers[name.strip()] = value.strip()
    return headers


def _probe_service_json(url: str, *, headers: dict[str, str]) -> tuple[int, dict[str, object]]:
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
            if not isinstance(payload, dict):
                raise ProductionAcceptanceError(f"Expected a JSON object from {url}")
            return response.status, payload
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise ProductionAcceptanceError(f"{url} returned HTTP {exc.code}: {detail}") from exc
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise ProductionAcceptanceError(f"Unable to read JSON from {url}: {exc}") from exc


def _build_markdown(report: dict[str, object]) -> str:
    lines = [
        "# Production Acceptance Report",
        "",
        f"- generated_at_utc: `{report['generated_at_utc']}`",
        f"- overall_status: `{report['overall_status']}`",
        f"- promotion_manifest_path: `{report['promotion_manifest_path']}`",
        f"- evidence_review_index_path: `{report['evidence_review_index_path']}`",
        "",
        "## Blocking Findings",
        "",
    ]
    blocking = [entry for entry in report["checks"] if entry["severity"] == "blocking" and entry["status"] != "passed"]
    if not blocking:
        lines.append("- none")
    for finding in blocking:
        lines.append(f"- {finding['check']}: `{finding['status']}`")
    lines.extend(["", "## Advisory Findings", ""])
    advisories = [entry for entry in report["checks"] if entry["severity"] == "advisory" and entry["status"] != "passed"]
    if not advisories:
        lines.append("- none")
    for finding in advisories:
        lines.append(f"- {finding['check']}: `{finding['status']}`")
    return "\n".join(lines).strip() + "\n"


def _check(name: str, *, severity: str, passed: bool, detail: object, status: str | None = None) -> dict[str, object]:
    resolved_status = status or ("passed" if passed else "failed")
    return {
        "check": name,
        "severity": severity,
        "status": resolved_status,
        "detail": detail,
    }


def build_production_acceptance_report(
    *,
    promotion_manifest_path: Path,
    evidence_review_index_path: Path,
    output_dir: Path,
    service_base_url: str | None = None,
    service_headers: list[str] | None = None,
    evaluated_at_utc: str | None = None,
) -> dict[str, object]:
    evaluated_at = _parse_utc_timestamp(evaluated_at_utc)
    promotion_manifest = _read_json(promotion_manifest_path.resolve())
    cadence_root = evidence_review_index_path.resolve().parent
    cadence_status = _status_cjis_evidence_cadence(
        output_dir=cadence_root,
        evaluated_at_utc=_format_utc_timestamp(evaluated_at),
    )

    checks: list[dict[str, object]] = []
    checks.append(
        _check(
            "promotion_manifest_status",
            severity="blocking",
            passed=promotion_manifest.get("status") == "sealed",
            detail=promotion_manifest.get("status"),
        )
    )

    manifest_checks = promotion_manifest.get("checks", [])
    manifest_check_failures = [
        item["check"]
        for item in manifest_checks
        if isinstance(item, dict) and item.get("status") != "ok"
    ]
    checks.append(
        _check(
            "promotion_manifest_checks",
            severity="blocking",
            passed=not manifest_check_failures,
            detail=manifest_check_failures or "all_ok",
        )
    )

    runtime = promotion_manifest.get("runtime", {})
    if not isinstance(runtime, dict):
        raise ProductionAcceptanceError("promotion manifest runtime section must be an object")
    state_store = runtime.get("state_store", {})
    if not isinstance(state_store, dict):
        raise ProductionAcceptanceError("promotion manifest runtime.state_store must be an object")
    checks.append(
        _check(
            "state_store_backend",
            severity="blocking",
            passed=state_store.get("backend") == "postgresql",
            detail=state_store.get("backend"),
        )
    )
    checks.append(
        _check(
            "state_store_revision",
            severity="blocking",
            passed=state_store.get("current_revision") == state_store.get("head_revision"),
            detail={
                "current_revision": state_store.get("current_revision"),
                "head_revision": state_store.get("head_revision"),
            },
        )
    )

    environment_summary = runtime.get("environment_summary", {})
    if not isinstance(environment_summary, dict):
        raise ProductionAcceptanceError("promotion manifest runtime.environment_summary must be an object")
    service_auth = environment_summary.get("service_auth", {})
    if not isinstance(service_auth, dict):
        service_auth = {}
    tenant_isolation_ready = bool(service_auth.get("tenant_claim_path")) or bool(
        service_auth.get("reader_tenant_id") and service_auth.get("operator_tenant_id")
    )
    checks.append(
        _check(
            "tenant_isolation_configuration",
            severity="blocking",
            passed=tenant_isolation_ready,
            detail=service_auth,
        )
    )

    inputs = promotion_manifest.get("inputs", {})
    if not isinstance(inputs, dict):
        raise ProductionAcceptanceError("promotion manifest inputs section must be an object")

    custody_input = inputs.get("custody_manifest", {})
    acceptance_input = inputs.get("acceptance_package_summary", {})
    evidence_input = inputs.get("cjis_evidence_pack", {})
    ha_input = inputs.get("ha_rehearsal_summary", {})
    rollback_input = inputs.get("rollback_bundle", {})
    if not all(isinstance(item, dict) for item in (custody_input, acceptance_input, evidence_input, ha_input, rollback_input)):
        raise ProductionAcceptanceError("promotion manifest input entries must be objects")

    custody_manifest = _read_json(Path(str(custody_input["path"])))
    acceptance_summary = _read_json(Path(str(acceptance_input["path"])))
    ha_summary = _read_json(Path(str(ha_input["path"])))
    evidence_manifest = _read_evidence_manifest(Path(str(evidence_input["path"])))

    checks.append(
        _check(
            "custody_manifest",
            severity="blocking",
            passed=custody_manifest.get("status") == "captured",
            detail=custody_manifest.get("status"),
        )
    )
    checks.append(
        _check(
            "acceptance_package",
            severity="blocking",
            passed=acceptance_summary.get("status") == "packaged"
            and isinstance(acceptance_summary.get("masked_validation"), dict)
            and acceptance_summary["masked_validation"].get("status") == "passed",
            detail=acceptance_summary.get("status"),
        )
    )
    checks.append(
        _check(
            "evidence_pack_preflight",
            severity="blocking",
            passed=evidence_manifest.get("preflight_status") == "ok",
            detail=evidence_manifest.get("preflight_status"),
        )
    )
    checks.append(
        _check(
            "rollback_bundle_present",
            severity="blocking",
            passed=Path(str(rollback_input["path"])).exists(),
            detail=rollback_input.get("path"),
        )
    )

    ha_steps = ha_summary.get("validated_steps", [])
    if not isinstance(ha_steps, list):
        ha_steps = []
    missing_ha_steps = [step for step in REQUIRED_HA_STEPS if step not in ha_steps]
    checks.append(
        _check(
            "ha_rehearsal",
            severity="blocking",
            passed=ha_summary.get("status") == "ok" and not missing_ha_steps,
            detail={"status": ha_summary.get("status"), "missing_steps": missing_ha_steps},
        )
    )

    latest_capture = cadence_status.get("captures", [])[-1] if cadence_status.get("captures") else None
    same_evidence_pack = isinstance(latest_capture, dict) and latest_capture.get("evidence_pack_path") == evidence_input.get("path")
    checks.append(
        _check(
            "evidence_cadence_current",
            severity="blocking",
            passed=cadence_status.get("status") == "current" and same_evidence_pack,
            detail={
                "cadence_status": cadence_status.get("status"),
                "latest_capture_id": cadence_status.get("latest_capture_id"),
                "same_evidence_pack": same_evidence_pack,
            },
        )
    )

    headers = _parse_headers([] if service_headers is None else service_headers)
    if service_base_url is None:
        checks.append(
            _check(
                "service_probes",
                severity="advisory",
                passed=False,
                detail="service_base_url not provided",
                status="skipped",
            )
        )
    else:
        base_url = service_base_url.rstrip("/")
        health_status, health_payload = _probe_service_json(f"{base_url}/healthz", headers=headers)
        ready_status, ready_payload = _probe_service_json(f"{base_url}/readyz", headers=headers)
        checks.append(
            _check(
                "service_health",
                severity="blocking",
                passed=health_status == 200 and health_payload.get("status") == "ok",
                detail=health_payload,
            )
        )
        checks.append(
            _check(
                "service_readiness",
                severity="blocking",
                passed=ready_status == 200 and ready_payload.get("status") == "ready",
                detail=ready_payload,
            )
        )

    blocking_failures = [entry["check"] for entry in checks if entry["severity"] == "blocking" and entry["status"] != "passed"]
    advisory_findings = [entry["check"] for entry in checks if entry["severity"] == "advisory" and entry["status"] != "passed"]
    overall_status = "not_ready" if blocking_failures else ("ready_with_advisories" if advisory_findings else "ready")

    report = {
        "project": "etl-identity-engine",
        "bundle_type": "production_acceptance_report",
        "generated_at_utc": _format_utc_timestamp(evaluated_at),
        "overall_status": overall_status,
        "promotion_manifest_path": str(promotion_manifest_path.resolve()),
        "evidence_review_index_path": str(evidence_review_index_path.resolve()),
        "blocking_failures": blocking_failures,
        "advisory_findings": advisory_findings,
        "checks": checks,
    }
    resolved_output_dir = output_dir.resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(resolved_output_dir / REPORT_NAME, report)
    (resolved_output_dir / REPORT_MARKDOWN_NAME).write_text(_build_markdown(report), encoding="utf-8")
    return {
        "status": overall_status,
        "report_path": str((resolved_output_dir / REPORT_NAME).resolve()),
        "report_markdown_path": str((resolved_output_dir / REPORT_MARKDOWN_NAME).resolve()),
        "report": report,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    payload = build_production_acceptance_report(
        promotion_manifest_path=Path(args.promotion_manifest),
        evidence_review_index_path=Path(args.evidence_review_index),
        output_dir=_resolve_output_dir(args.output_dir),
        service_base_url=args.service_base_url,
        service_headers=args.service_header,
        evaluated_at_utc=args.evaluated_at_utc,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload["status"] in {"ready", "ready_with_advisories"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
