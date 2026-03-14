from __future__ import annotations

from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from etl_identity_engine.demo_shell.loader import load_public_safety_demo_bundle
from etl_identity_engine.demo_shell.models import DemoRun


class Command(BaseCommand):
    help = "Load a packaged public-safety demo bundle into the standalone Django shell database."

    def add_arguments(self, parser) -> None:
        parser.add_argument("--bundle", required=True, help="Path to the packaged demo bundle zip.")
        parser.add_argument(
            "--extract-dir",
            required=True,
            help="Directory where the bundle should be extracted for raw artifact links.",
        )
        parser.add_argument(
            "--replace",
            action="store_true",
            help="Replace any existing loaded demo run. This is the default behavior.",
        )

    def handle(self, *args, **options) -> str:
        bundle = Path(options["bundle"])
        extract_dir = Path(options["extract_dir"])
        if not bundle.exists():
            raise CommandError(f"Demo bundle not found: {bundle}")
        if DemoRun.objects.exists() and not options["replace"]:
            raise CommandError("A demo bundle is already loaded. Re-run with --replace to overwrite it.")
        loaded = load_public_safety_demo_bundle(bundle_path=bundle, extract_dir=extract_dir)
        return f"loaded {loaded.bundle_path.name} into {loaded.bundle_root}"
