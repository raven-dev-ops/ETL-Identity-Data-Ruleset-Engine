from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from package_public_safety_demo import package_public_safety_demo
from package_release_sample import parse_formats, read_project_version, resolve_output_dir

from etl_identity_engine.demo_shell.bootstrap import (
    DEFAULT_OUTPUT_DIR,
    PreparedDemoShell,
    prepare_public_safety_demo_shell,
    run_public_safety_demo_server,
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare and optionally serve the standalone Django public-safety demo shell."
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory where the SQLite database, extracted bundle, and generated bundle will live.",
    )
    parser.add_argument(
        "--bundle",
        default=None,
        help="Existing packaged public-safety demo bundle zip. If omitted, one is built first.",
    )
    parser.add_argument(
        "--state-db",
        default=None,
        help="Persisted state DB or SQLAlchemy URL to load instead of a packaged demo bundle.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Specific persisted run to load from --state-db. Defaults to the latest completed run.",
    )
    parser.add_argument("--profile", default="small", choices=["small", "medium", "large"])
    parser.add_argument("--seed", default=42, type=int)
    parser.add_argument(
        "--formats",
        default="csv,parquet",
        help="Comma-separated generate/run-all source formats to request when building a bundle.",
    )
    parser.add_argument(
        "--version",
        default=None,
        help="Version to embed in the generated bundle name. Defaults to pyproject.toml.",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8000, type=int)
    parser.add_argument(
        "--prepare-only",
        action="store_true",
        help="Prepare the standalone shell without starting Django runserver.",
    )
    return parser.parse_args(argv)


def resolve_demo_bundle(
    *,
    output_dir: Path,
    bundle: str | None,
    profile: str,
    seed: int,
    formats: Sequence[str],
    version: str,
) -> Path:
    if bundle:
        return Path(bundle).resolve()
    bundle_output_dir = output_dir / "bundles"
    return package_public_safety_demo(
        output_dir=bundle_output_dir,
        profile=profile,
        seed=seed,
        formats=formats,
        version=version,
    )


def prepare_public_safety_demo_shell_workspace(
    *,
    output_dir: Path,
    bundle: str | None,
    state_db: str | None,
    run_id: str | None,
    profile: str,
    seed: int,
    formats: Sequence[str],
    version: str,
    host: str,
    port: int,
) -> PreparedDemoShell:
    if bundle and state_db:
        raise ValueError("--bundle and --state-db are mutually exclusive")
    if state_db:
        return prepare_public_safety_demo_shell(
            state_db=state_db,
            run_id=run_id,
            output_dir=output_dir,
            host=host,
            port=port,
        )

    bundle_path = resolve_demo_bundle(
        output_dir=output_dir,
        bundle=bundle,
        profile=profile,
        seed=seed,
        formats=formats,
        version=version,
    )
    return prepare_public_safety_demo_shell(
        bundle_path=bundle_path,
        output_dir=output_dir,
        host=host,
        port=port,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    resolved_output_dir = resolve_output_dir(args.output_dir)
    formats = parse_formats(args.formats)
    version = args.version or read_project_version()

    prepared = prepare_public_safety_demo_shell_workspace(
        output_dir=resolved_output_dir,
        bundle=args.bundle,
        state_db=args.state_db,
        run_id=args.run_id,
        profile=args.profile,
        seed=args.seed,
        formats=formats,
        version=version,
        host=args.host,
        port=args.port,
    )

    if prepared.source_kind == "persisted_state":
        print(f"standalone demo source run: {prepared.source_run_id}")
        print(f"standalone demo state source: {prepared.bundle_path}")
    else:
        print(f"standalone demo bundle: {prepared.bundle_path}")
    print(f"standalone demo database: {prepared.db_path}")
    print(f"standalone extracted artifacts: {prepared.bundle_root}")
    print(f"standalone demo URL: {prepared.base_url}")

    if args.prepare_only:
        return 0

    run_public_safety_demo_server(host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
