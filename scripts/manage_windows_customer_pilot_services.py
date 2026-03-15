from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]


def _ensure_repo_src_on_path() -> None:
    candidate_paths = (
        REPO_ROOT / "src",
        REPO_ROOT / "runtime" / "src",
    )
    for candidate in candidate_paths:
        if candidate.exists() and str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))
            return


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Install, start, stop, query, or remove the supported Windows customer "
            "pilot services for the demo shell and service API."
        )
    )
    parser.add_argument(
        "--bundle-root",
        default=None,
        help="Extracted customer pilot bundle root. Defaults to the current working tree or extracted bundle root.",
    )
    parser.add_argument(
        "--startup",
        choices=("manual", "auto"),
        default=None,
        help="Optional Windows service startup mode to apply on install.",
    )
    parser.add_argument(
        "--service-kind",
        choices=("demo_shell", "service_api", "all"),
        default="all",
        help="Which pilot service to manage. Defaults to both.",
    )
    parser.add_argument(
        "action",
        choices=(
            "install",
            "start",
            "stop",
            "restart",
            "remove",
            "status",
            "install-and-start",
            "stop-and-remove",
        ),
    )
    return parser.parse_args(argv)


def _resolve_bundle_root(bundle_root: str | None) -> Path:
    resolved = Path(bundle_root).resolve() if bundle_root else Path.cwd().resolve()
    manifest_path = resolved / "pilot_manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(
            "Unable to locate an extracted customer pilot bundle root. "
            "Provide --bundle-root and point it at the extracted bundle directory."
        )
    return resolved


def manage_windows_customer_pilot_services(
    *,
    bundle_root: Path,
    action: str,
    service_kind: str,
    startup: str | None = None,
) -> dict[str, object]:
    _ensure_repo_src_on_path()
    from etl_identity_engine.windows_pilot_services import manage_windows_pilot_services

    return manage_windows_pilot_services(
        bundle_root=bundle_root,
        action=action,
        service_kind=service_kind,
        startup=startup,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    summary = manage_windows_customer_pilot_services(
        bundle_root=_resolve_bundle_root(args.bundle_root),
        action=args.action,
        service_kind=args.service_kind,
        startup=args.startup,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
