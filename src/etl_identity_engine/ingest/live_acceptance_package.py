"""Masked acceptance-fixture packaging for live onboarding review."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import shutil

import yaml

from etl_identity_engine.ingest.live_target_packs import check_live_target_pack, get_live_target_pack
from etl_identity_engine.io.read import read_dict_fieldnames, read_dict_rows
from etl_identity_engine.io.write import write_csv_dicts, write_markdown


ACCEPTANCE_PACKAGE_SUMMARY_FILENAME = "acceptance_package_summary.json"
DRIFT_REPORT_JSON_FILENAME = "drift_report.json"
DRIFT_REPORT_MARKDOWN_FILENAME = "drift_report.md"


class LiveAcceptancePackageError(ValueError):
    """Raised when acceptance package inputs are incomplete or invalid."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_utc_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _acceptance_output_root(output_dir: Path, *, target_id: str, packaged_at_utc: str) -> Path:
    timestamp_token = packaged_at_utc.replace("-", "").replace(":", "")
    return output_dir / f"{timestamp_token}-{target_id}-acceptance"


def _should_preserve_value(column_name: str, value: str) -> bool:
    normalized_value = value.strip()
    if not normalized_value:
        return True
    lowered_value = normalized_value.lower()
    if lowered_value in {"true", "false", "cad", "rms", "source_a", "source_b"}:
        return True
    if column_name.lower() == "source_system":
        return True
    return False


def _mask_rows(rows: list[dict[str, str]], *, masked_tokens: dict[str, str]) -> list[dict[str, str]]:
    def mask_value(column_name: str, value: str) -> str:
        if _should_preserve_value(column_name, value):
            return value
        normalized = value.strip()
        if normalized not in masked_tokens:
            masked_tokens[normalized] = f"MASK-{len(masked_tokens) + 1:06d}"
        return masked_tokens[normalized]

    return [
        {
            column_name: mask_value(column_name, value)
            for column_name, value in row.items()
        }
        for row in rows
    ]


def _write_parquet_dicts(path: Path, rows: list[dict[str, str]], *, fieldnames: tuple[str, ...]) -> None:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Parquet acceptance packaging requires `pyarrow`. Install project dependencies or use CSV bundles."
        ) from exc

    path.parent.mkdir(parents=True, exist_ok=True)
    ordered_rows = [{field: row.get(field, "") for field in fieldnames} for row in rows]
    pq.write_table(pa.Table.from_pylist(ordered_rows), path)


def _masked_data_copy(source_path: Path, output_path: Path, *, masked_tokens: dict[str, str]) -> None:
    fieldnames = read_dict_fieldnames(source_path)
    rows = read_dict_rows(source_path)
    masked_rows = _mask_rows(rows, masked_tokens=masked_tokens)
    if source_path.suffix.lower() == ".csv":
        write_csv_dicts(output_path, masked_rows, fieldnames=fieldnames)
        return
    if source_path.suffix.lower() == ".parquet":
        _write_parquet_dicts(output_path, masked_rows, fieldnames=fieldnames)
        return
    raise LiveAcceptancePackageError(f"Unsupported acceptance package input format: {source_path.suffix}")


def _sanitized_drift_report(validation_summary: dict[str, object], *, target_id: str) -> dict[str, object]:
    onboarding_summary = validation_summary["validation"]["summary"]
    manifest_summary = onboarding_summary.get("manifest", {})
    bundles = onboarding_summary.get("bundles", [])
    return {
        "target_id": target_id,
        "status": onboarding_summary.get("status"),
        "manifest": {
            "status": manifest_summary.get("status"),
            "source_count": manifest_summary.get("source_count"),
            "source_bundle_count": manifest_summary.get("source_bundle_count"),
        },
        "bundles": [
            {
                "bundle_dir_name": Path(str(bundle.get("bundle_dir", ""))).name,
                "status": bundle.get("status"),
                "contract_name": bundle.get("contract_name"),
                "contract_version": bundle.get("contract_version"),
                "source_system": bundle.get("source_system"),
                "vendor_profile": bundle.get("vendor_profile"),
                "validation_error": bundle.get("validation_error"),
                "files": {
                    logical_name: {
                        "format": file_summary.get("format"),
                        "row_count": file_summary.get("row_count"),
                        "source_fieldnames": file_summary.get("source_fieldnames"),
                        "fieldnames": file_summary.get("fieldnames"),
                        "diff_report": file_summary.get("diff_report"),
                    }
                    for logical_name, file_summary in bundle.get("files", {}).items()
                },
            }
            for bundle in bundles
        ],
    }


def _drift_report_markdown(drift_report: dict[str, object]) -> str:
    lines = [
        "# Live Acceptance Drift Report",
        "",
        f"- target_id: `{drift_report['target_id']}`",
        f"- status: `{drift_report['status']}`",
        f"- manifest.source_count: `{drift_report['manifest']['source_count']}`",
        f"- manifest.source_bundle_count: `{drift_report['manifest']['source_bundle_count']}`",
        "",
    ]
    for bundle in drift_report["bundles"]:
        lines.append(f"## {bundle['bundle_dir_name']}")
        lines.append("")
        lines.append(f"- status: `{bundle['status']}`")
        lines.append(f"- contract: `{bundle['contract_name']}` / `{bundle['contract_version']}`")
        lines.append(f"- vendor_profile: `{bundle['vendor_profile']}`")
        if bundle.get("validation_error"):
            lines.append(f"- validation_error: `{bundle['validation_error']}`")
        lines.append("")
        for logical_name, file_summary in bundle["files"].items():
            diff_report = file_summary["diff_report"]
            lines.append(f"### {logical_name}")
            lines.append("")
            lines.append(f"- row_count: `{file_summary['row_count']}`")
            lines.append(f"- overlay_mode: `{diff_report['overlay_mode']}`")
            lines.append(
                "- missing_required_canonical_fields: "
                f"`{', '.join(diff_report['missing_required_canonical_fields']) or 'none'}`"
            )
            lines.append(
                "- missing_source_columns: "
                f"`{', '.join(diff_report['missing_source_columns']) or 'none'}`"
            )
            lines.append(
                "- unmapped_source_columns: "
                f"`{', '.join(diff_report['unmapped_source_columns']) or 'none'}`"
            )
            lines.append("")
    return "\n".join(lines).strip() + "\n"


def package_live_target_acceptance(
    target_id: str,
    source_root: Path,
    output_dir: Path,
) -> dict[str, object]:
    pack = get_live_target_pack(target_id)
    resolved_source_root = source_root.resolve()
    if not resolved_source_root.exists():
        raise FileNotFoundError(f"Acceptance package source root not found: {resolved_source_root}")
    if not resolved_source_root.is_dir():
        raise NotADirectoryError(f"Acceptance package source root must be a directory: {resolved_source_root}")

    source_validation = check_live_target_pack(target_id, resolved_source_root)
    summary: dict[str, object] = {
        "target_id": target_id,
        "source_root": str(resolved_source_root),
        "source_validation": source_validation,
    }
    if source_validation["status"] != "passed":
        summary["status"] = "failed"
        summary["validation_error"] = "source live target pack failed onboarding validation"
        return summary

    packaged_at_utc = _format_utc_timestamp(_utc_now())
    acceptance_root = _acceptance_output_root(output_dir.resolve(), target_id=target_id, packaged_at_utc=packaged_at_utc)
    if acceptance_root.exists():
        raise LiveAcceptancePackageError(f"Acceptance package output already exists: {acceptance_root}")
    acceptance_root.mkdir(parents=True, exist_ok=False)

    copied_files: list[str] = []
    masked_tokens: dict[str, str] = {}
    for source_path in sorted(path for path in resolved_source_root.rglob("*") if path.is_file()):
        relative_path = source_path.relative_to(resolved_source_root)
        normalized_relative_path = relative_path.as_posix()
        if normalized_relative_path in {
            "README.md",
            "live_target_pack_summary.json",
            "custody_manifest.json",
            "acceptance_package_summary.json",
            "drift_report.json",
            "drift_report.md",
        }:
            continue

        output_path = acceptance_root / relative_path
        if normalized_relative_path == pack.manifest_name:
            manifest = yaml.safe_load(source_path.read_text(encoding="utf-8"))
            manifest["batch_id"] = f"acceptance-{target_id}"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(
                yaml.safe_dump(manifest, sort_keys=False),
                encoding="utf-8",
            )
            copied_files.append(normalized_relative_path)
            continue

        if source_path.name == "contract_manifest.yml":
            output_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, output_path)
            copied_files.append(normalized_relative_path)
            continue

        if source_path.suffix.lower() in {".csv", ".parquet"}:
            _masked_data_copy(source_path, output_path, masked_tokens=masked_tokens)
            copied_files.append(normalized_relative_path)
            continue

    write_markdown(
        acceptance_root / "README.md",
        (
            "# Live Acceptance Package\n\n"
            f"This masked acceptance package was generated for `{target_id}`.\n\n"
            "Use `etl-identity-engine check-live-target-pack --target-id "
            f"{target_id} --root-dir .` from this directory to re-run the onboarding smoke.\n"
        ),
    )
    copied_files.append("README.md")

    drift_report = _sanitized_drift_report(source_validation, target_id=target_id)
    (acceptance_root / DRIFT_REPORT_JSON_FILENAME).write_text(
        json.dumps(drift_report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_markdown(acceptance_root / DRIFT_REPORT_MARKDOWN_FILENAME, _drift_report_markdown(drift_report))
    copied_files.extend([DRIFT_REPORT_JSON_FILENAME, DRIFT_REPORT_MARKDOWN_FILENAME])

    masked_validation = check_live_target_pack(target_id, acceptance_root)
    summary.update(
        {
            "status": "packaged" if masked_validation["status"] == "passed" else "failed",
            "packaged_at_utc": packaged_at_utc,
            "acceptance_root": str(acceptance_root),
            "drift_report_path": str(acceptance_root / DRIFT_REPORT_JSON_FILENAME),
            "drift_report_markdown_path": str(acceptance_root / DRIFT_REPORT_MARKDOWN_FILENAME),
            "masked_validation": masked_validation,
            "files_written": sorted(copied_files),
            "masked_value_count": len(masked_tokens),
        }
    )
    if (resolved_source_root / "custody_manifest.json").exists():
        summary["source_custody_manifest_present"] = True

    (acceptance_root / ACCEPTANCE_PACKAGE_SUMMARY_FILENAME).write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    summary["files_written"] = sorted([*summary["files_written"], ACCEPTANCE_PACKAGE_SUMMARY_FILENAME])
    (acceptance_root / ACCEPTANCE_PACKAGE_SUMMARY_FILENAME).write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return summary
