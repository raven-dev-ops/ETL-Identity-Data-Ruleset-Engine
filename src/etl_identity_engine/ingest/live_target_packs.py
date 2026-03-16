"""Packaged live CAD/RMS onboarding target packs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from importlib.resources import files
import json
from pathlib import Path
import re
import shutil

from etl_identity_engine.ingest.public_safety_conformance import check_public_safety_onboarding


@dataclass(frozen=True)
class LiveTargetPackVariable:
    name: str
    default: str
    description: str


@dataclass(frozen=True)
class LiveTargetPackResource:
    source_path: str
    output_path: str


@dataclass(frozen=True)
class LiveTargetPack:
    target_id: str
    display_name: str
    source_class: str
    contract_name: str
    contract_version: str
    vendor_profile: str
    description: str
    resource_root: str
    bundle_id: str
    bundle_dir: str
    manifest_name: str
    variables: tuple[LiveTargetPackVariable, ...]
    rendered_files: tuple[LiveTargetPackResource, ...]
    sample_files: tuple[LiveTargetPackResource, ...]

    def to_summary(self) -> dict[str, object]:
        return {
            "target_id": self.target_id,
            "display_name": self.display_name,
            "source_class": self.source_class,
            "contract_name": self.contract_name,
            "contract_version": self.contract_version,
            "vendor_profile": self.vendor_profile,
            "description": self.description,
            "bundle_id": self.bundle_id,
            "bundle_dir": self.bundle_dir,
            "manifest_name": self.manifest_name,
            "variables": [
                {
                    "name": variable.name,
                    "default": variable.default,
                    "description": variable.description,
                }
                for variable in self.variables
            ],
        }


class LiveTargetPackError(ValueError):
    """Raised when a packaged live target pack is unknown or malformed."""


_TEMPLATE_PATTERN = re.compile(r"{{\s*([a-zA-Z0-9_]+)\s*}}")
_PREPARED_SUMMARY_FILENAME = "live_target_pack_summary.json"


PACKAGED_LIVE_TARGET_PACKS = {
    "cad_county_dispatch_v1": LiveTargetPack(
        target_id="cad_county_dispatch_v1",
        display_name="County Dispatch CAD Live Target",
        source_class="cad",
        contract_name="cad_call_for_service",
        contract_version="v1",
        vendor_profile="cad_county_dispatch_v1",
        description="Supported county-dispatch CAD onboarding scaffold with packaged vendor profile wiring.",
        resource_root="live_target_packs/cad/cad_county_dispatch_v1",
        bundle_id="cad_county_dispatch_primary",
        bundle_dir="cad_county_dispatch_bundle",
        manifest_name="batch_manifest.yml",
        variables=(
            LiveTargetPackVariable(
                name="agency_name",
                default="Franklin County Dispatch",
                description="Agency name rendered into the onboarding README and summary.",
            ),
            LiveTargetPackVariable(
                name="agency_slug",
                default="franklin-county-dispatch",
                description="Slug used in the sample batch_id emitted into the prepared manifest.",
            ),
            LiveTargetPackVariable(
                name="drop_zone_subpath",
                default="cad/county_dispatch/inbound",
                description="Customer-managed inbound drop-zone path documented in the rendered README.",
            ),
            LiveTargetPackVariable(
                name="operator_contact",
                default="dispatch.integration@example.gov",
                description="Primary onboarding contact rendered into the README.",
            ),
        ),
        rendered_files=(
            LiveTargetPackResource(
                source_path="templates/README.md.template",
                output_path="README.md",
            ),
            LiveTargetPackResource(
                source_path="templates/batch_manifest.yml.template",
                output_path="batch_manifest.yml",
            ),
            LiveTargetPackResource(
                source_path="templates/contract_manifest.yml.template",
                output_path="cad_county_dispatch_bundle/contract_manifest.yml",
            ),
        ),
        sample_files=(
            LiveTargetPackResource(
                source_path="sample/landing/source_a.csv",
                output_path="landing/source_a.csv",
            ),
            LiveTargetPackResource(
                source_path="sample/landing/source_b.csv",
                output_path="landing/source_b.csv",
            ),
            LiveTargetPackResource(
                source_path="sample/cad_county_dispatch_bundle/vendor_person_records.csv",
                output_path="cad_county_dispatch_bundle/vendor_person_records.csv",
            ),
            LiveTargetPackResource(
                source_path="sample/cad_county_dispatch_bundle/vendor_incident_records.csv",
                output_path="cad_county_dispatch_bundle/vendor_incident_records.csv",
            ),
            LiveTargetPackResource(
                source_path="sample/cad_county_dispatch_bundle/vendor_incident_person_links.csv",
                output_path="cad_county_dispatch_bundle/vendor_incident_person_links.csv",
            ),
        ),
    ),
    "rms_records_bureau_v1": LiveTargetPack(
        target_id="rms_records_bureau_v1",
        display_name="Records Bureau RMS Live Target",
        source_class="rms",
        contract_name="rms_report_person",
        contract_version="v1",
        vendor_profile="rms_records_bureau_v1",
        description="Supported records-bureau RMS onboarding scaffold with packaged vendor profile wiring.",
        resource_root="live_target_packs/rms/rms_records_bureau_v1",
        bundle_id="rms_records_bureau_primary",
        bundle_dir="rms_records_bureau_bundle",
        manifest_name="batch_manifest.yml",
        variables=(
            LiveTargetPackVariable(
                name="agency_name",
                default="Franklin County Records Bureau",
                description="Agency name rendered into the onboarding README and summary.",
            ),
            LiveTargetPackVariable(
                name="agency_slug",
                default="franklin-county-records-bureau",
                description="Slug used in the sample batch_id emitted into the prepared manifest.",
            ),
            LiveTargetPackVariable(
                name="drop_zone_subpath",
                default="rms/records_bureau/inbound",
                description="Customer-managed inbound drop-zone path documented in the rendered README.",
            ),
            LiveTargetPackVariable(
                name="operator_contact",
                default="records.integration@example.gov",
                description="Primary onboarding contact rendered into the README.",
            ),
        ),
        rendered_files=(
            LiveTargetPackResource(
                source_path="templates/README.md.template",
                output_path="README.md",
            ),
            LiveTargetPackResource(
                source_path="templates/batch_manifest.yml.template",
                output_path="batch_manifest.yml",
            ),
            LiveTargetPackResource(
                source_path="templates/contract_manifest.yml.template",
                output_path="rms_records_bureau_bundle/contract_manifest.yml",
            ),
        ),
        sample_files=(
            LiveTargetPackResource(
                source_path="sample/landing/source_a.csv",
                output_path="landing/source_a.csv",
            ),
            LiveTargetPackResource(
                source_path="sample/landing/source_b.csv",
                output_path="landing/source_b.csv",
            ),
            LiveTargetPackResource(
                source_path="sample/rms_records_bureau_bundle/vendor_person_records.csv",
                output_path="rms_records_bureau_bundle/vendor_person_records.csv",
            ),
            LiveTargetPackResource(
                source_path="sample/rms_records_bureau_bundle/vendor_incident_records.csv",
                output_path="rms_records_bureau_bundle/vendor_incident_records.csv",
            ),
            LiveTargetPackResource(
                source_path="sample/rms_records_bureau_bundle/vendor_incident_person_links.csv",
                output_path="rms_records_bureau_bundle/vendor_incident_person_links.csv",
            ),
        ),
    ),
}


def get_live_target_pack(target_id: str) -> LiveTargetPack:
    try:
        return PACKAGED_LIVE_TARGET_PACKS[target_id]
    except KeyError as exc:
        supported_names = ", ".join(sorted(PACKAGED_LIVE_TARGET_PACKS))
        raise LiveTargetPackError(
            f"unsupported live target pack {target_id!r}; expected one of: {supported_names}"
        ) from exc


def list_live_target_packs(*, source_class: str | None = None) -> tuple[LiveTargetPack, ...]:
    packs = tuple(PACKAGED_LIVE_TARGET_PACKS.values())
    if source_class is None:
        return packs
    return tuple(pack for pack in packs if pack.source_class == source_class)


def _pack_resource(pack: LiveTargetPack, relative_path: str):
    return files("etl_identity_engine.ingest").joinpath(pack.resource_root).joinpath(relative_path)


def _read_pack_text(pack: LiveTargetPack, relative_path: str) -> str:
    resource = _pack_resource(pack, relative_path)
    if not resource.is_file():
        raise LiveTargetPackError(
            f"live target pack {pack.target_id!r} is missing resource: {pack.resource_root}/{relative_path}"
        )
    return resource.read_text(encoding="utf-8")


def _read_pack_bytes(pack: LiveTargetPack, relative_path: str) -> bytes:
    resource = _pack_resource(pack, relative_path)
    if not resource.is_file():
        raise LiveTargetPackError(
            f"live target pack {pack.target_id!r} is missing resource: {pack.resource_root}/{relative_path}"
        )
    return resource.read_bytes()


def _resolve_variables(pack: LiveTargetPack, overrides: Mapping[str, str] | None) -> dict[str, str]:
    overrides = overrides or {}
    allowed_names = {variable.name for variable in pack.variables}
    unexpected_names = sorted(set(overrides) - allowed_names)
    if unexpected_names:
        raise LiveTargetPackError(
            f"{pack.target_id}: unsupported variable override(s): {', '.join(unexpected_names)}"
        )

    resolved: dict[str, str] = {}
    for variable in pack.variables:
        value = overrides.get(variable.name, variable.default)
        if not isinstance(value, str) or not value.strip():
            raise LiveTargetPackError(
                f"{pack.target_id}: variable {variable.name!r} must be a non-empty string"
            )
        resolved[variable.name] = value.strip()
    return resolved


def _template_context(
    pack: LiveTargetPack,
    *,
    output_dir: Path,
    variables: Mapping[str, str],
) -> dict[str, str]:
    context = {
        "target_id": pack.target_id,
        "display_name": pack.display_name,
        "source_class": pack.source_class,
        "contract_name": pack.contract_name,
        "contract_version": pack.contract_version,
        "vendor_profile": pack.vendor_profile,
        "description": pack.description,
        "bundle_id": pack.bundle_id,
        "bundle_dir": pack.bundle_dir,
        "manifest_name": pack.manifest_name,
        "output_dir": str(output_dir),
    }
    context.update(variables)
    return context


def _render_template(template_text: str, context: Mapping[str, str]) -> str:
    missing_keys: set[str] = set()

    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in context:
            missing_keys.add(key)
            return match.group(0)
        return context[key]

    rendered = _TEMPLATE_PATTERN.sub(replace, template_text)
    if missing_keys:
        raise LiveTargetPackError(
            "template references undefined keys: " + ", ".join(sorted(missing_keys))
        )
    return rendered


def _prepare_output_dir(output_dir: Path, *, force: bool) -> None:
    if output_dir.exists():
        if not output_dir.is_dir():
            raise LiveTargetPackError(f"Output path is not a directory: {output_dir}")
        if any(output_dir.iterdir()):
            if not force:
                raise LiveTargetPackError(
                    f"Output directory must be empty or use force overwrite: {output_dir}"
                )
            shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _validation_summary(pack: LiveTargetPack, output_dir: Path) -> dict[str, object]:
    manifest_path = output_dir / pack.manifest_name
    bundle_dir = output_dir / pack.bundle_dir
    validation = check_public_safety_onboarding(
        bundle_dirs=(bundle_dir,),
        manifest_path=manifest_path,
    )
    return {
        "status": validation["status"],
        "manifest_path": str(manifest_path),
        "bundle_dir": str(bundle_dir),
        "summary": validation,
    }


def prepare_live_target_pack(
    target_id: str,
    output_dir: Path,
    *,
    variable_overrides: Mapping[str, str] | None = None,
    force: bool = False,
) -> dict[str, object]:
    pack = get_live_target_pack(target_id)
    resolved_output_dir = output_dir.resolve()
    variables = _resolve_variables(pack, variable_overrides)
    context = _template_context(pack, output_dir=resolved_output_dir, variables=variables)

    _prepare_output_dir(resolved_output_dir, force=force)

    files_written: list[str] = []
    for resource in pack.rendered_files:
        rendered = _render_template(_read_pack_text(pack, resource.source_path), context)
        output_path = resolved_output_dir / resource.output_path
        _write_text(output_path, rendered)
        files_written.append(resource.output_path.replace("\\", "/"))

    for resource in pack.sample_files:
        output_path = resolved_output_dir / resource.output_path
        _write_bytes(output_path, _read_pack_bytes(pack, resource.source_path))
        files_written.append(resource.output_path.replace("\\", "/"))

    summary = {
        **pack.to_summary(),
        "output_dir": str(resolved_output_dir),
        "resolved_variables": variables,
        "files_written": sorted(files_written),
    }
    summary["validation"] = _validation_summary(pack, resolved_output_dir)

    summary_path = resolved_output_dir / _PREPARED_SUMMARY_FILENAME
    _write_text(summary_path, json.dumps(summary, indent=2, sort_keys=True))
    summary["files_written"] = sorted(
        [*summary["files_written"], _PREPARED_SUMMARY_FILENAME]
    )
    _write_text(summary_path, json.dumps(summary, indent=2, sort_keys=True))
    return summary


def check_live_target_pack(target_id: str, root_dir: Path) -> dict[str, object]:
    pack = get_live_target_pack(target_id)
    resolved_root_dir = root_dir.resolve()
    summary = {
        **pack.to_summary(),
        "root_dir": str(resolved_root_dir),
        "validation": _validation_summary(pack, resolved_root_dir),
    }
    prepared_summary_path = resolved_root_dir / _PREPARED_SUMMARY_FILENAME
    if prepared_summary_path.exists():
        summary["prepared_pack"] = json.loads(prepared_summary_path.read_text(encoding="utf-8"))
    summary["status"] = summary["validation"]["status"]
    return summary
