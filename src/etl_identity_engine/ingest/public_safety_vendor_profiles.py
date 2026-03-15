"""Packaged CAD/RMS vendor profile overlays."""

from __future__ import annotations

from dataclasses import dataclass, replace
from importlib.resources import as_file, files

from etl_identity_engine.ingest.public_safety_mapping import (
    PublicSafetyMappingOverlay,
    PublicSafetyMappingOverlayError,
    load_public_safety_mapping_overlay,
)


@dataclass(frozen=True)
class PublicSafetyVendorProfile:
    name: str
    contract_name: str
    contract_version: str
    source_system: str
    description: str
    resource_path: str


class PublicSafetyVendorProfileError(ValueError):
    """Raised when a packaged vendor profile is unknown or inconsistent."""


PACKAGED_PUBLIC_SAFETY_VENDOR_PROFILES = {
    "cad_county_dispatch_v1": PublicSafetyVendorProfile(
        name="cad_county_dispatch_v1",
        contract_name="cad_call_for_service",
        contract_version="v1",
        source_system="cad",
        description="County CAD export with person, event, and party keys.",
        resource_path="vendor_profiles/cad/cad_county_dispatch_v1.yml",
    ),
    "cad_records_management_v1": PublicSafetyVendorProfile(
        name="cad_records_management_v1",
        contract_name="cad_call_for_service",
        contract_version="v1",
        source_system="cad",
        description="Records-oriented CAD export with call and subject identifiers.",
        resource_path="vendor_profiles/cad/cad_records_management_v1.yml",
    ),
}


def get_public_safety_vendor_profile(profile_name: str) -> PublicSafetyVendorProfile:
    try:
        return PACKAGED_PUBLIC_SAFETY_VENDOR_PROFILES[profile_name]
    except KeyError as exc:
        supported_names = ", ".join(sorted(PACKAGED_PUBLIC_SAFETY_VENDOR_PROFILES))
        raise PublicSafetyVendorProfileError(
            f"unsupported vendor_profile {profile_name!r}; expected one of: {supported_names}"
        ) from exc


def list_public_safety_vendor_profiles(*, source_system: str | None = None) -> tuple[PublicSafetyVendorProfile, ...]:
    profiles = tuple(PACKAGED_PUBLIC_SAFETY_VENDOR_PROFILES.values())
    if source_system is None:
        return profiles
    return tuple(profile for profile in profiles if profile.source_system == source_system)


def load_packaged_public_safety_mapping_overlay(
    profile_name: str,
    *,
    contract_name: str,
    contract_version: str,
    allowed_fields_by_file: dict[str, tuple[str, ...]],
) -> PublicSafetyMappingOverlay:
    profile = get_public_safety_vendor_profile(profile_name)
    if profile.contract_name != contract_name:
        raise PublicSafetyVendorProfileError(
            f"vendor_profile {profile_name!r} is for contract {profile.contract_name!r}, not {contract_name!r}"
        )
    if profile.contract_version != contract_version:
        raise PublicSafetyVendorProfileError(
            f"vendor_profile {profile_name!r} is for contract_version {profile.contract_version!r}, not {contract_version!r}"
        )

    resource = files("etl_identity_engine.ingest").joinpath(profile.resource_path)
    if not resource.is_file():
        raise PublicSafetyVendorProfileError(
            f"vendor_profile {profile_name!r} resource is missing: {profile.resource_path}"
        )

    with as_file(resource) as overlay_path:
        try:
            overlay = load_public_safety_mapping_overlay(
                overlay_path,
                contract_name=contract_name,
                contract_version=contract_version,
                allowed_fields_by_file=allowed_fields_by_file,
            )
        except PublicSafetyMappingOverlayError as exc:
            raise PublicSafetyVendorProfileError(
                f"vendor_profile {profile_name!r} is invalid: {exc}"
            ) from exc

    return replace(
        overlay,
        overlay_label=f"vendor-profile:{profile_name}",
        vendor_profile=profile_name,
    )
